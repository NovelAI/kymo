"""GPU-to-host reads for ``kymo.log()`` that do not wait for the GPU.

``log()`` gathers a call's CUDA scalars with one kernel per device into a float64 buffer in pinned host memory (or stacks them per dtype where no kernel builds), copies each CUDA image to pinned host memory with ``non_blocking=True``, and records one event per device; later calls publish the values once the events have completed.
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

# CUDA objects that must not be freed yet, or ever: each Reads, with its pinned buffers, events and source tensors, from just before its first copy until wait() sees its copies complete (torch's allocator does not know a gather kernel uses its pinned buffers), and for good if they never do, since freeing a pinned copy or an event after a CUDA error or in a fork child aborts the process. This leaked reference keeps them through interpreter shutdown.
unfreeable: set = set()
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

# CU_DEVICE_ATTRIBUTE_CAN_USE_HOST_POINTER_FOR_REGISTERED_MEM: the device can use a cudaHostRegister'd host pointer as is.
_CAN_USE_HOST_POINTER_FOR_REGISTERED_MEM = 91
# CUDA_ERROR_INVALID_CONTEXT: the calling thread has no current context, as on one that has not used CUDA through the runtime.
_CUDA_ERROR_INVALID_CONTEXT = 201

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
def dtype_codes(torch) -> dict:
    """Each of DTYPES' torch dtypes mapped to its code in a gather word."""
    return {getattr(torch, name): code for code, name in enumerate(DTYPES)}


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
    """The gather kernel compiled with NVRTC through torch, for the current device; it reads its words from pinned host memory and writes the values there.

    The kernel would fault on a pinned host pointer the device cannot use. Torch pins with cudaHostAlloc or cudaHostRegister, so this checks once that the device uses registered host pointers as they are and that a pinned probe is mapped at its host address.
    It is launched with cuLaunchKernel directly: torch's launcher reloads the driver library and rebuilds its arguments on every call. A thread on which nothing has used CUDA through the runtime has no current context, where the driver's calls fail: one whose pinned buffers torch's allocator served from its cache, say, after another thread's. The kernel is built in the context current then, else the device's primary context, which torch uses, and a launch the driver refuses for want of a context makes that one current and launches again, so a launch costs nothing more.
    Its private torch API (torch.cuda._compile_kernel, torch.cuda._utils._get_cuda_library, the compiled kernel's func) was verified against torch 2.14 (CUDA, on a B200); a torch without it builds the Triton kernel instead (gather).
    """
    if torch.version.hip:
        raise ImportError("the direct launch is CUDA-only")
    driver = torch.cuda._utils._get_cuda_library()
    device, context = ctypes.c_int(), ctypes.c_void_p()
    if driver.cuDeviceGet(ctypes.byref(device), torch.cuda.current_device()):
        raise RuntimeError("the current device has no driver handle")
    if (driver.cuCtxGetCurrent(ctypes.byref(context)) or not context.value) and (
        driver.cuDevicePrimaryCtxRetain(ctypes.byref(context), device)
        or driver.cuCtxSetCurrent(context)
    ):
        raise RuntimeError("the device's primary context cannot be made current")
    kernel = torch.cuda._compile_kernel(_CUDA_GATHER, "kymo_gather")
    usable, mapped = ctypes.c_int(), ctypes.c_void_p()
    probe = torch.empty(1, dtype=torch.float64, device="cpu", pin_memory=True)
    if (
        driver.cuDeviceGetAttribute(
            ctypes.byref(usable), _CAN_USE_HOST_POINTER_FOR_REGISTERED_MEM, device
        )
        or not usable.value
        or driver.cuMemHostGetDevicePointer_v2(
            ctypes.byref(mapped), ctypes.c_void_p(probe.data_ptr()), 0
        )
        or mapped.value != probe.data_ptr()
    ):
        raise RuntimeError("pinned host memory is not usable at its host address")
    launch_kernel = ctypes.CFUNCTYPE(
        ctypes.c_int,
        ctypes.c_void_p,
        *[ctypes.c_uint] * 7,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
    )(("cuLaunchKernel", driver))

    def launch(words, out, n: int) -> None:
        # Parameters per launch: a signal handler that logs can start another launch on this thread between these statements.
        cells = (
            ctypes.c_void_p(words.data_ptr()),
            ctypes.c_void_p(out.data_ptr()),
            ctypes.c_int(n),
        )
        params = (ctypes.c_void_p * 3)(*map(ctypes.addressof, cells))
        stream = torch.cuda.current_stream().cuda_stream
        args = (kernel.func, (n + 255) // 256, 1, 1, 256, 1, 1, 0, stream, params)
        error = launch_kernel(*args, None)
        if error == _CUDA_ERROR_INVALID_CONTEXT and not driver.cuCtxSetCurrent(context):
            error = launch_kernel(*args, None)
        if error:
            raise RuntimeError(f"cuLaunchKernel failed with CUDA error {error}")

    return launch


def triton_gather(torch):
    """The gather kernel compiled with Triton, for the current device; its words and values are copied to and from the device."""
    from kymo._gpu_triton import kymo_gather

    def launch(words, out, n: int) -> None:
        values = torch.empty(n, dtype=torch.float64, device="cuda")
        kymo_gather[((n + 1023) // 1024,)](
            words.to("cuda", non_blocking=True), values, n, BLOCK=1024
        )
        out.copy_(values, non_blocking=True)

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
    """Whether launch, used by Reads as log() uses it, reads SAMPLES as float() does. It runs on a stream of its own, so its synchronizing reads wait for none of the caller's GPU work, with sync debug mode off so they are not reported."""
    mode = torch.cuda.get_sync_debug_mode()
    torch.cuda.set_sync_debug_mode(0)
    try:
        with torch.cuda.stream(torch.cuda.Stream()):
            values = [
                torch.tensor(value, dtype=getattr(torch, name), device="cuda")
                for name, value in SAMPLES
            ]
            reads = Reads(torch, values, [], launch=launch)
            reads.wait()
            # A list, not a generator: Python 3.14 leaves the jump back of all()'s loop over one, where a handler may raise, outside the try (cpython#130279).
            return all([_same(read, float(values[i])) for i, read in reads.scalars()])
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
            # No torch.cuda._compile_kernel, a HIP torch, or no Triton.
            _log.debug("%s unavailable: %s", build.__name__, error)
        except Exception as error:
            _log.warning("%s failed; not using it: %s", build.__name__, error)
    return None


class Reads:
    """The GPU-to-host copies of one ``log()`` call.

    The copies are enqueued on each device's current stream, so they read the values at the call even if the caller then changes a tensor on that stream.
    Each scalar's memory stays referenced until the call is published, so memory freed on another stream cannot be reused before a copy reads it; a scalar that requires grad is held by its storage, which keeps its bytes but not its autograd graph.
    """

    def __init__(self, torch, scalars: list, images: list, launch=None):
        """launch, if given, replaces every device's gather kernel; reads_exactly tests one this way."""
        self._parts = []
        self.images = []
        self._events = []
        self._sources = [
            tensor.untyped_storage() if tensor.requires_grad else tensor
            for tensor in scalars
        ]
        groups = _by_device(torch, scalars)
        # Every device the call reads from: each gets one event, and an out-of-memory error waits on each.
        devices = {*groups, *(data.get_device() for data in images)}
        # Before its first copy, and only then: a failure before it has made nothing to keep.
        unfreeable.add(self)
        try:
            for device, (indices, tensors) in groups.items():
                with torch.cuda.device(device):
                    kernel = gather(torch, device) if launch is None else launch
                    if kernel is None:
                        self._stack(torch, indices, tensors)
                    else:
                        self._gather(torch, kernel, indices, tensors)
            for data in images:
                # A non-contiguous source would be staged through pageable memory, which waits for the GPU.
                source = data.detach().contiguous()
                self._sources.append(source)
                self.images.append(source.to("cpu", non_blocking=True))
            for device in devices:
                # Held before it is recorded: an event that fails to record with a CUDA error must stay unfreeable with Reads.
                self._events.append(torch.cuda.Event())
                self._events[-1].record(torch.cuda.current_stream(device))
        except torch.cuda.OutOfMemoryError:
            # CUDA still works: let every kernel and copy the call started finish, then free what it made. After any other failure, which may be a CUDA error, freeing a host copy would abort the process, so Reads stays unfreeable.
            for device in devices:
                torch.cuda.current_stream(device).synchronize()
            unfreeable.discard(self)
            raise

    def _gather(self, torch, launch, indices, tensors) -> None:
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
        codes = dtype_codes(torch)
        # Each buffer is held by Reads as soon as it exists: torch's allocator does not know the kernel reads words and writes out, and freeing pinned memory after a CUDA error aborts the process.
        words = torch.tensor(
            [(codes[t.dtype] << 56) | p for t, p in zip(tensors, pointers)],
            dtype=torch.int64,
            device="cpu",
            pin_memory=True,
        )
        self._sources.append(words)
        out = torch.empty(
            len(tensors), dtype=torch.float64, device="cpu", pin_memory=True
        )
        self._parts.append((indices, out))
        launch(words, out, len(tensors))

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
        """Wait until the copies have completed, or until the ``time.monotonic()`` deadline; True once they have, and Reads may then be freed."""
        if deadline is None:
            for event in self._events:
                event.synchronize()
        else:
            pause = 0.001
            while not self.done():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                time.sleep(min(pause, remaining))
                pause = min(pause * 2, 0.05)
        unfreeable.discard(self)
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
