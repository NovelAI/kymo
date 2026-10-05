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


def drag_handle(page, handle, to_x: float) -> None:
    """Drag `handle` from its middle to `to_x`, in a few moves."""
    box = handle.bounding_box()
    assert box is not None
    y = box["y"] + box["height"] / 2
    page.mouse.move(box["x"] + box["width"] / 2, y)
    page.mouse.down()
    page.mouse.move(to_x, y, steps=6)
    page.mouse.up()


def render_turn(page) -> None:
    """Wait two animation frames; this is not an RPC-drain or idle barrier."""
    page.evaluate(
        "() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))"
    )
