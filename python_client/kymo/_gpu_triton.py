"""The gather kernel of ``kymo._gpu`` in Triton, used when NVRTC cannot build it or its build fails the known-answer check.

Imported only then: kymo neither requires Triton nor pays for its import otherwise.
"""

import triton
import triton.language as tl


# The dtype of each word code, in kymo._gpu.DTYPES order.
_DTYPES = tl.constexpr(
    (
        tl.float32,
        tl.float16,
        tl.bfloat16,
        tl.float64,
        tl.int64,
        tl.int32,
        tl.int16,
        tl.int8,
        tl.uint8,
        tl.int1,
    )
)


@triton.jit(do_not_specialize=["n"])  # one compile for every scalar count
def kymo_gather(words, out, n, BLOCK: tl.constexpr):
    """Gathers n scalars, each addressed by a word (dtype code << 56 | address), into float64 as float() converts them."""
    i = tl.program_id(0) * BLOCK + tl.arange(0, BLOCK)
    # A lane past the end reads code -1, which selects no dtype.
    word = tl.load(words + i, mask=i < n, other=-1)
    code = word >> 56
    address = word & 0x00FFFFFFFFFFFFFF
    value = tl.zeros([BLOCK], dtype=tl.float64)
    for dtype_code in tl.static_range(len(_DTYPES)):
        hit = code == dtype_code
        read = tl.load(
            address.to(tl.pointer_type(_DTYPES[dtype_code])), mask=hit, other=0
        )
        value = tl.where(hit, read.to(tl.float64), value)
    tl.store(out + i, value, mask=i < n)
