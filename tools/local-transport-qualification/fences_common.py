"""Small, protobuf-independent helpers for browser qualification fixtures."""

import struct


def request_frame(frame: bytes) -> tuple[int, str, bytes]:
    """Decode the frozen /grpc-ws request envelope; leave protobuf decoding to the caller."""
    if not isinstance(frame, bytes) or len(frame) < 6:
        raise ValueError("invalid RPC request frame")
    request_id, path_len = struct.unpack_from("<IH", frame)
    if path_len == 0 or len(frame) < 6 + path_len:
        raise ValueError("invalid RPC path length")
    return request_id, frame[6 : 6 + path_len].decode(), frame[6 + path_len :]


def encode_response(request_id: int, body: bytes, *, code: int = 0) -> bytes:
    """Wrap an unchanged protobuf body or error message in its response header."""
    return struct.pack("<IB", request_id, code) + body


def render_turn(page) -> None:
    """Wait two animation frames; this is not an RPC-drain or idle barrier."""
    page.evaluate(
        "() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))"
    )
