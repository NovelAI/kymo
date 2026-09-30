"""GPU-to-host reads for ``kymo.log()`` that do not wait for the GPU.

``log()`` gathers a call's CUDA scalars with one kernel per device (or stacks them per dtype where no kernel builds) into a float64 buffer, copies it and each CUDA image to pinned host memory with ``non_blocking=True``, and records one event per device; later calls publish the values once the events have completed.
Everything here runs on the thread that calls ``log()``, ``wait_for_upload()`` or ``finish()``: a CUDA call from a second thread can break a CUDA graph capture in its default "global" mode, and its first call creates a context on GPU 0.
"""

import collections
import ctypes
import functools
import logging
import math
import sys
import time

# The client's logger, without importing the package: chadfusion's tests load this file alone.
_log = logging.getLogger("kymo")

# CUDA objects that are never freed: freeing a pinned host copy or an event aborts the process after a CUDA error or in a fork child. A reference leaked here keeps them alive through interpreter shutdown.
unfreeable: list = []
ctypes.pythonapi.Py_IncRef(ctypes.py_object(unfreeable))

# The dtypes read asynchronously; a gather kernel reads a scalar of DTYPES[code].
DTYPES = (
    "float32",
    "float16",
    "bfloat16",
    "float64",
    "int64",
    "int32",
    "int16",
    "int8",
    "uint8",
    "bool",
)

# A scalar of a null storage (autograd's efficient zero tensors, functional tensors, views of a freed storage) has no memory: its data_ptr() is its offset from address 0, storage_offset() * element_size(), where the kernel would fault.
# The default allocator maps device memory above 2**46, which no offset within a tensor under 64 TiB reaches, so only smaller pointers are tested; expandable segments and cudaMallocAsync map it below.
_NULL_OFFSET_BOUND = 1 << 46

# Gathers n scalars, each addressed by a word (dtype code << 56 | address; addresses fit in 56 bits on x86-64 and arm64), into float64 as float() converts them.
_CUDA_GATHER = r"""
extern "C" __global__ void kymo_gather(const unsigned long long* words, double* out, int n) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= n) return;
    unsigned long long word = words[i];
    const void* p = (const void*)(word & 0x00FFFFFFFFFFFFFFull);
    float half;
    double value;
    switch (word >> 56) {
    case 0: value = *(const float*)p; break;
    case 1:  // NVRTC has no CUDA headers; this is what __half2float compiles to.
        asm("cvt.f32.f16 %0, %1;" : "=f"(half) : "h"(*(const unsigned short*)p));
        value = half;
        break;
    case 2: value = __uint_as_float((unsigned)*(const unsigned short*)p << 16); break;
    case 3: value = *(const double*)p; break;
    case 4: value = *(const long long*)p; break;
    case 5: value = *(const int*)p; break;
    case 6: value = *(const short*)p; break;
    case 7: value = *(const signed char*)p; break;
    case 8: value = *(const unsigned char*)p; break;
    default: value = *(const unsigned char*)p != 0; break;  // 9: bool
    }
    out[i] = value;
}
"""


def cuda_torch():
    """torch once it has initialized CUDA, else None: until then no CUDA tensor exists. Never imports torch.

    Also None inside a torch.func transform, whose tensors are wrappers without memory of their own.
    """
    torch = sys.modules.get("torch")
    try:
        if not torch.cuda.is_initialized():
            return None
    except AttributeError:  # not loaded, a stub, or half imported
        return None
    return torch if torch._C._functorch.peek_interpreter_stack() is None else None


@functools.cache
def classifiers(torch):
    """Return ``(is_scalar, is_image)`` tests for the values read asynchronously.

    A scalar is a dense one-element CUDA tensor of one of DTYPES; a negative view holds the negated value in its bytes, so it keeps the synchronous read. An image is any dense CUDA tensor.
    Both must be a plain tensor or a Parameter: another subclass (a DTensor, say) need not hold its value at data_ptr(), so it keeps the synchronous read.
    """
    kinds = (torch.Tensor, torch.nn.Parameter)
    dtypes = {getattr(torch, name) for name in DTYPES}
    strided = torch.strided

    def is_scalar(value) -> bool:
        return (
            type(value) in kinds
            and value.is_cuda
            and value.layout is strided
            and value.numel() == 1
            and value.dtype in dtypes
            and not value.is_neg()
        )

    def is_image(data) -> bool:
        return type(data) in kinds and data.is_cuda and data.layout is strided

    return is_scalar, is_image


def reads_asynchronously(value) -> bool:
    """Whether ``log()`` copies value to the host without waiting for the GPU, so a caller can hand it over instead of reading it with float()."""
    torch = cuda_torch()
    return torch is not None and classifiers(torch)[0](value)


def nvrtc_gather(torch):
    """The gather kernel compiled with NVRTC through torch, for the current device."""
    kernel = torch.cuda._compile_kernel(_CUDA_GATHER, "kymo_gather")

    def launch(words, out, n: int) -> None:
        # torch passes a Python int as a 32-bit C int, the kernel's int n.
        kernel(grid=((n + 255) // 256, 1, 1), block=(256, 1, 1), args=[words, out, n])

    return launch


def triton_gather(torch):
    """The gather kernel compiled with Triton, for the current device."""
    from kymo._gpu_triton import kymo_gather

    def launch(words, out, n: int) -> None:
        kymo_gather[((n + 1023) // 1024,)](words, out, n, BLOCK=1024)

    return launch


# Values a gather kernel must read exactly as float() does: each dtype, and the conversions a wrong build gets wrong (rounding, subnormals, infinities, NaN, negative zero).
SAMPLES = (
    ("float32", -2.5),
    ("float32", -0.0),
    ("float32", math.nan),
    ("float16", 1 / 3),
    ("float16", 6e-8),
    ("float16", -math.inf),
    ("bfloat16", 1 / 3),
    ("float64", 1e300),
    ("int64", 2**53 + 3),
    ("int32", -7),
    ("int16", -32768),
    ("int8", -128),
    ("uint8", 255),
    ("bool", True),
)


def _same(read: float, expected: float) -> bool:
    if math.isnan(expected):
        return math.isnan(read)
    return read == expected and math.copysign(1, read) == math.copysign(1, expected)


def reads_exactly(torch, launch) -> bool:
    """Whether launch reads SAMPLES as float() does. It runs on a stream of its own, so its synchronizing reads wait for none of the caller's GPU work, with sync debug mode off so they are not reported."""
    mode = torch.cuda.get_sync_debug_mode()
    torch.cuda.set_sync_debug_mode(0)
    try:
        with torch.cuda.stream(torch.cuda.Stream()):
            values, words = [], []
            for name, value in SAMPLES:
                tensor = torch.tensor(value, dtype=getattr(torch, name), device="cuda")
                values.append(tensor)
                words.append((DTYPES.index(name) << 56) | tensor.data_ptr())
            out = torch.empty(len(words), dtype=torch.float64, device="cuda")
            launch(
                torch.tensor(words, dtype=torch.int64, device="cuda"), out, len(words)
            )
            return all(map(_same, out.tolist(), [float(t) for t in values]))
    finally:
        torch.cuda.set_sync_debug_mode(mode)


@functools.cache
def gather(torch, device: int):
    """The gather kernel's launcher on device, which the caller has made current: NVRTC through torch, else Triton. None means neither can be used, and the device's scalars are stacked per dtype.

    A kernel is used only once it has read SAMPLES correctly, so a torch or Triton that builds it to read wrong values falls back instead of corrupting metrics; one that faults loses CUDA for the process, as any faulting kernel does.
    """
    for build in (nvrtc_gather, triton_gather):
        try:
            launch = build(torch)
            if reads_exactly(torch, launch):
                return launch
            _log.warning("%s reads values incorrectly; not using it", build.__name__)
        except (AttributeError, ImportError) as error:
            # No torch.cuda._compile_kernel, or no Triton.
            _log.debug("%s unavailable: %s", build.__name__, error)
        except Exception as error:
            _log.warning("%s failed; not using it: %s", build.__name__, error)
    return None


class Reads:
    """The GPU-to-host copies of one ``log()`` call.

    The copies are enqueued on each device's current stream, so they read the values at the call even if the caller then changes a tensor on that stream.
    Each scalar's memory stays referenced until the call is published, so memory freed on another stream cannot be reused before a copy reads it; a scalar that requires grad is held by its storage, which keeps its bytes but not its autograd graph.
    """

    def __init__(self, torch, scalars: list, images: list):
        self._parts = []
        self.images = []
        self._events = []
        self._sources = [
            tensor.untyped_storage() if tensor.requires_grad else tensor
            for tensor in scalars
        ]
        try:
            groups = _by_device(torch, scalars)
            for device, (indices, tensors) in groups.items():
                with torch.cuda.device(device):
                    launch = gather(torch, device)
                    if launch is None:
                        self._stack(torch, indices, tensors)
                    else:
                        self._gather(torch, device, launch, indices, tensors)
            for data in images:
                # A non-contiguous source would be staged through pageable memory, which waits for the GPU.
                source = data.detach().contiguous()
                self._sources.append(source)
                self.images.append(source.to("cpu", non_blocking=True))
            for device in {*groups, *(data.get_device() for data in images)}:
                # Held before it is recorded: an event that fails to record with a CUDA error must go to unfreeable with Reads.
                self._events.append(torch.cuda.Event())
                self._events[-1].record(torch.cuda.current_stream(device))
        except torch.cuda.OutOfMemoryError:
            # CUDA still works, so what the call made is freed.
            raise
        except BaseException:
            # Any other failure may be a CUDA error, after which freeing a host copy made before it would abort the process.
            unfreeable.append(self)
            raise

    def _gather(self, torch, device: int, launch, indices, tensors) -> None:
        """Copy the scalars of one device with one gather kernel; a scalar of a null storage (see _NULL_OFFSET_BOUND) is read with float()."""
        pointers = [t.data_ptr() for t in tensors]
        if min(pointers) < _NULL_OFFSET_BOUND:
            null = [
                p < _NULL_OFFSET_BOUND and p == t.storage_offset() * t.element_size()
                for t, p in zip(tensors, pointers)
            ]
            if any(null):
                values = [float(t) for t, is_null in zip(tensors, null) if is_null]
                self._parts.append(
                    (
                        [i for i, is_null in zip(indices, null) if is_null],
                        torch.tensor(values, dtype=torch.float64, device="cpu"),
                    )
                )
                indices = [i for i, is_null in zip(indices, null) if not is_null]
                tensors = [t for t, is_null in zip(tensors, null) if not is_null]
                pointers = [p for p, is_null in zip(pointers, null) if not is_null]
                if not tensors:
                    return
        codes = {getattr(torch, name): code for code, name in enumerate(DTYPES)}
        words = torch.tensor(
            [(codes[t.dtype] << 56) | p for t, p in zip(tensors, pointers)],
            dtype=torch.int64,
            device="cpu",
            pin_memory=True,
        )
        # Freed with Reads, not here: freeing pinned memory after a CUDA error aborts the process, and Reads then goes to unfreeable.
        self._sources.append(words)
        out = torch.empty(len(tensors), dtype=torch.float64, device=device)
        launch(words.to(device, non_blocking=True), out, len(tensors))
        self._parts.append((indices, out.to("cpu", non_blocking=True)))

    def _stack(self, torch, indices, tensors) -> None:
        """Copy the scalars of one device with one stack per dtype, for a torch that cannot build the gather kernel; under no_grad, so the host copies keep no autograd graph."""
        groups = collections.defaultdict(lambda: ([], []))
        with torch.no_grad():
            for index, tensor in zip(indices, tensors):
                group_indices, members = groups[tensor.dtype]
                group_indices.append(index)
                # stack needs one shape; a one-element tensor that is not 0-d is rare.
                members.append(tensor.reshape(()) if tensor.dim() else tensor)
            # Across dtypes stack would promote, and int64 65520 next to a float16 becomes inf.
            for group_indices, members in groups.values():
                self._parts.append(
                    (group_indices, torch.stack(members).to("cpu", non_blocking=True))
                )

    def done(self) -> bool:
        """Whether every copy has completed; never waits."""
        return all(event.query() for event in self._events)

    def wait(self, deadline=None) -> bool:
        """Wait until the copies have completed, or until the ``time.monotonic()`` deadline; True once they have."""
        if deadline is None:
            for event in self._events:
                event.synchronize()
            return True
        pause = 0.001
        while not self.done():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(pause, remaining))
            pause = min(pause * 2, 0.05)
        return True

    def scalars(self):
        """Yield ``(index, value)`` for every scalar, the value a Python float; only once the copies have completed."""
        for indices, host in self._parts:
            values = (host if host.is_floating_point() else host.double()).tolist()
            yield from zip(indices, values)


def _by_device(torch, scalars: list) -> dict:
    """``{device index: (indices, tensors)}`` of the scalars, without a per-scalar get_device() in a one-device process."""
    if torch.cuda.device_count() == 1:
        return {0: (range(len(scalars)), scalars)} if scalars else {}
    groups = collections.defaultdict(lambda: ([], []))
    for index, tensor in enumerate(scalars):
        indices, tensors = groups[tensor.get_device()]
        indices.append(index)
        tensors.append(tensor)
    return groups
