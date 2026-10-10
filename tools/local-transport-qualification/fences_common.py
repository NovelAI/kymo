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


CHART_HEIGHT = "e => getComputedStyle(e).getPropertyValue('--kymo-chart-height')"


def drag_chart_height(page, rect, dy: float) -> str:
    """Drag `rect`'s resize grip `dy` pixels vertically, wait for its section's chart height to change, and return the new height."""
    before = rect.evaluate(CHART_HEIGHT)
    # The section's chart height stays within 100-800 px, so a drag at the edge it heads for would never change it.
    height = float(before.removesuffix("px"))
    if min(800, max(100, height + dy)) == height:
        raise AssertionError(
            f"a {dy:+} px drag cannot move the chart height off {before}"
        )
    rect.hover()
    grip = rect.locator(".rect-resize-handle").bounding_box()
    assert grip is not None
    x, y = grip["x"] + grip["width"] / 2, grip["y"] + grip["height"] / 2
    page.mouse.move(x, y)
    page.mouse.down()
    page.mouse.move(x, y + dy, steps=6)
    page.mouse.up()
    page.wait_for_function(
        f"([e, before]) => ({CHART_HEIGHT})(e) !== before",
        arg=[rect.element_handle(), before],
    )
    return rect.evaluate(CHART_HEIGHT)


def render_turn(page) -> None:
    """Wait two animation frames; this is not an RPC-drain or idle barrier."""
    page.evaluate(
        "() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))"
    )


def fitted_box(node) -> dict:
    """`node`'s box once the chart around it has taken its container's width, which a chart does on its ResizeObserver's next frame after a layout change."""
    chart_id = node.evaluate("n => n.closest('.chart-container').id")
    node.page.wait_for_function(
        "id => Math.abs(window.__kymo_charts[id].width - document.getElementById(id).getBoundingClientRect().width) <= 1",
        arg=chart_id,
    )
    box = node.bounding_box()
    assert box is not None
    return box
