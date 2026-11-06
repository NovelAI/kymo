"""Browser fences for kymo's source-owned chart zoom session.

Most cases isolate event production; two reach ZoomBridge and prove real
bounded and open-ended refetches and shared single-click reset against
``seed_zoom_fixture.py``.
"""

import argparse
import math
import re
import struct
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from kymo._generated import kymo_pb2 as pb
from playwright.sync_api import Page, TimeoutError, sync_playwright

from fences_common import (
    drag_chart_height,
    encode_response,
    fitted_box,
    render_turn as settle,
    request_frame,
)


SOURCE_TITLE = "zoom_a_source"
PEER_TITLE = "zoom_b_peer"
SPARSE_TITLE = "zoom_c_sparse"
GUTTER_TITLE = "zoom_d_gutter"
CLIENT_SOURCE_TITLE = "zoom_client_a"
CLIENT_PEER_TITLE = "zoom_client_b"
RUN_PATH = "/zoom-e2e/zoom-fixture"
VIEWPORT = {"width": 1_600, "height": 900}


def set_single_click_unzoom(page: Page, enabled: bool) -> None:
    page.evaluate(
        "enabled => document.documentElement.setAttribute("
        "'data-kymo-single-click-unzoom', String(enabled))",
        enabled,
    )


@contextmanager
def single_click_enabled(page: Page) -> Iterator[None]:
    """Restore double-click mode and event capture when the case finishes."""
    set_single_click_unzoom(page, True)
    try:
        yield
    finally:
        set_single_click_unzoom(page, False)
        clear_events(page)


def chart_id(page: Page, title: str) -> str:
    candidate = page.locator(".metric-rect").filter(has_text=title)
    if candidate.count() != 1:
        raise AssertionError(
            f"expected one chart titled {title!r}, found {candidate.count()}"
        )
    if candidate.locator(".rect-title-pop").inner_text().strip() != title:
        raise AssertionError(f"chart title selector was not exact for {title!r}")
    identifier = candidate.locator(".chart-container").get_attribute("id")
    if identifier is None:
        raise AssertionError(f"chart {title!r} has no container id")
    return identifier


def install_event_capture(page: Page) -> None:
    page.evaluate(
        """() => {
            if (window.__kymo_zoom_fence) return;
            const state = {events: [], intercept: true, oldChart: null};
            state.handler = event => {
                const detail = event.detail || {};
                const target = event.target instanceof Element
                    ? event.target.closest('.chart-container')
                    : null;
                state.events.push({
                    source: target ? target.id : null,
                    xmin: detail.xmin == null ? null : Number(detail.xmin),
                    xmax: detail.xmax == null ? null : Number(detail.xmax),
                });
                if (state.intercept) event.stopImmediatePropagation();
            };
            document.addEventListener('kymo-zoom', state.handler, true);
            window.__kymo_zoom_fence = state;
        }"""
    )


def clear_events(page: Page, *, intercept: bool = True) -> None:
    page.evaluate(
        "intercept => { window.__kymo_zoom_fence.events.length = 0; "
        "window.__kymo_zoom_fence.intercept = intercept; }",
        intercept,
    )


def events(page: Page) -> list[dict[str, Any]]:
    return page.evaluate("window.__kymo_zoom_fence.events.map(event => ({...event}))")


def chart_snapshot(page: Page, identifier: str) -> dict[str, Any]:
    return page.evaluate(
        """id => {
            const chart = window.__kymo_charts && window.__kymo_charts[id];
            if (!chart) throw new Error(`chart ${id} is not mounted`);
            const zoom = chart.__kymo_zoom;
            const x = chart.data[0];
            const hasXr = !!(zoom && zoom.xrBase != null);
            const xrMin = hasXr ? chart.data[zoom.xrBase] : null;
            const xrMax = hasXr ? chart.data[zoom.xrBase + 1] : null;
            const slots = Array.from(x, (center, index) => ({
                lo: xrMin && Number.isFinite(xrMin[index]) ? xrMin[index] : center,
                hi: xrMax && Number.isFinite(xrMax[index]) ? xrMax[index] : center,
            }));
            if (!slots.length) throw new Error(`chart ${id} has no x slots`);
            const over = chart.over.getBoundingClientRect();
            const container = document.getElementById(id).getBoundingClientRect();
            return {
                id,
                sync_key: chart.cursor.sync.key,
                has_xr: hasXr,
                scale_min: chart.scales.x.min,
                scale_max: chart.scales.x.max,
                newest_x: x[x.length - 1],
                left: over.left,
                top: over.top,
                width: over.width,
                height: over.height,
                container_left: container.left,
                container_bottom: container.bottom,
                coverage_min: slots[0].lo,
                coverage_max: slots[slots.length - 1].hi,
                slots,
            };
        }""",
        identifier,
    )


def mouse_pixel(value: float) -> float:
    """Use the integral CSS coordinate Playwright dispatches to the page."""
    return float(round(value))


def x_at_fraction(chart: dict[str, Any], fraction: float) -> float:
    return mouse_pixel(float(chart["left"]) + fraction * float(chart["width"]))


def y_mid(chart: dict[str, Any]) -> float:
    return mouse_pixel(float(chart["top"]) + float(chart["height"]) * 0.5)


def strip_y(chart: dict[str, Any]) -> float:
    """A page y on the axis strip below the plot."""
    return mouse_pixel(
        min(
            float(chart["container_bottom"]) - 1,
            float(chart["top"]) + float(chart["height"]) + 10,
        )
    )


def assert_close(actual: float | None, expected: float, message: str) -> None:
    if actual is None or not math.isclose(actual, expected, abs_tol=1e-6):
        raise AssertionError(f"{message}: expected {expected}, got {actual}")


def one_event(page: Page, source_id: str) -> dict[str, Any]:
    settle(page)
    captured = events(page)
    if len(captured) != 1:
        raise AssertionError(f"expected one source zoom event, got {captured}")
    if captured[0]["source"] != source_id:
        raise AssertionError(
            f"zoom source changed from {source_id!r} to {captured[0]['source']!r}"
        )
    return captured[0]


def selections(page: Page, identifiers: list[str]) -> list[dict[str, float]]:
    return page.evaluate(
        """ids => ids.map(id => {
            const chart = window.__kymo_charts[id];
            return {
                left: chart.select.left,
                width: chart.select.width,
                plot_width: chart.over.getBoundingClientRect().width,
            };
        })""",
        identifiers,
    )


def assert_no_selection(page: Page) -> None:
    widths = page.evaluate(
        """() => Object.values(window.__kymo_charts || {})
            .map(chart => chart.select.width)"""
    )
    if any(abs(float(width)) > 1e-6 for width in widths):
        raise AssertionError(f"finished zoom left live selections: {widths}")


@contextmanager
def showing_a_closed_window(page: Page, identifier: str) -> Iterator[None]:
    """Mark the step answer chart `identifier` shows as a closed window (no newest step at its right edge), as a fixed step zoom's answer is, so a selection to that edge keeps its end instead of following."""
    page.evaluate(
        "id => { window.__kymo_charts[id].data[0].stepOpen = false; }", identifier
    )
    try:
        yield
    finally:
        page.evaluate(
            "id => { window.__kymo_charts[id].data[0].stepOpen = true; }", identifier
        )


def drag(page: Page, start: tuple[float, float], end: tuple[float, float]) -> None:
    page.mouse.move(*start)
    page.mouse.down()
    try:
        page.mouse.move(*end, steps=12)
    finally:
        page.mouse.up()


def source_over_peer(page: Page, source: dict[str, Any], peer: dict[str, Any]) -> None:
    """A drag from the source over a peer reads the pointer through the source: past its right edge. The unzoomed source shows its newest step there, so the zoom follows it (no xmax); showing a closed window, it extrapolates in source space."""
    traverse_onto_peer(page, source, peer)
    event = one_event(page, source["id"])
    if event["xmax"] is not None:
        raise AssertionError(
            f"peer traversal past the source's newest step did not follow it: {event}"
        )
    with showing_a_closed_window(page, source["id"]):
        traverse_onto_peer(page, source, peer)
    event = one_event(page, source["id"])
    if event["xmax"] is None or event["xmax"] <= source["coverage_max"]:
        raise AssertionError(
            f"peer traversal did not extrapolate in source space: {event}"
        )


def traverse_onto_peer(
    page: Page, source: dict[str, Any], peer: dict[str, Any]
) -> None:
    clear_events(page)
    start = (x_at_fraction(source, 0.35), y_mid(source))
    end = (
        mouse_pixel(float(peer["left"]) + float(peer["width"]) * 0.5),
        y_mid(peer),
    )
    hit = page.evaluate(
        """point => {
            const element = document.elementFromPoint(point.x, point.y);
            return element && element.closest('.chart-container')?.id;
        }""",
        {"x": end[0], "y": end[1]},
    )
    if hit != peer["id"]:
        raise AssertionError(f"trajectory endpoint did not land over peer: {hit!r}")

    page.mouse.move(*start)
    page.mouse.down()
    try:
        page.mouse.move(*end, steps=12)
        settle(page)
        source_box, peer_box = selections(page, [source["id"], peer["id"]])
        for name, box in (("source", source_box), ("peer", peer_box)):
            if box["width"] <= 10:
                raise AssertionError(f"{name} held selection was not painted: {box}")
            if abs(box["left"] + box["width"] - box["plot_width"]) > 1:
                raise AssertionError(
                    f"{name} held selection missed its right edge: {box}"
                )
    finally:
        page.mouse.up()
    assert_no_selection(page)


def true_edge(page: Page, source: dict[str, Any]) -> None:
    """A drag released on the plot's true right edge reaches it: the unzoomed source follows its newest step, and showing a closed window it includes exactly the final bucket."""
    drag_to_true_edge(page, source)
    event = one_event(page, source["id"])
    if event["xmax"] is not None:
        raise AssertionError(
            f"true-edge drag past the newest step did not follow it: {event}"
        )
    with showing_a_closed_window(page, source["id"]):
        drag_to_true_edge(page, source)
    event = one_event(page, source["id"])
    coverage_max = float(source["coverage_max"])
    last_slot = source["slots"][-1]
    bucket_span = max(1.0, float(last_slot["hi"]) - float(last_slot["lo"]))
    if (
        event["xmax"] is None
        or event["xmax"] < coverage_max - 1e-6
        or event["xmax"] >= coverage_max + bucket_span + 1e-6
    ):
        raise AssertionError(
            "rounded edge drag did not include exactly the final bucket: "
            f"coverage={coverage_max}, bucket_span={bucket_span}, event={event}"
        )


def drag_to_true_edge(page: Page, source: dict[str, Any]) -> None:
    clear_events(page)
    start = (x_at_fraction(source, 0.4), y_mid(source))
    end = (mouse_pixel(float(source["left"]) + float(source["width"])), start[1])
    page.mouse.move(*start)
    page.mouse.down()
    try:
        page.mouse.move(*end, steps=10)
        settle(page)
        box = selections(page, [source["id"]])[0]
        if abs(box["left"] + box["width"] - box["plot_width"]) > 1:
            raise AssertionError(f"source held selection missed its true edge: {box}")
    finally:
        page.mouse.up()
    assert_no_selection(page)


def sparse_gap_noop(page: Page, sparse: dict[str, Any]) -> None:
    gaps = [
        (float(right["lo"]) - float(left["hi"]), left, right)
        for left, right in zip(sparse["slots"], sparse["slots"][1:])
        if float(right["lo"]) > float(left["hi"])
    ]
    if not gaps:
        raise AssertionError("sparse fixture produced no data gap")
    _, left, right = max(gaps, key=lambda gap: gap[0])
    first = float(left["hi"]) + 0.35 * (float(right["lo"]) - float(left["hi"]))
    second = float(left["hi"]) + 0.65 * (float(right["lo"]) - float(left["hi"]))
    scale_lo, scale_hi = float(sparse["scale_min"]), float(sparse["scale_max"])
    if not scale_hi > scale_lo:
        raise AssertionError(f"degenerate sparse scale: {scale_lo}..{scale_hi}")
    plot_left, plot_width = float(sparse["left"]), float(sparse["width"])
    scale_span = scale_hi - scale_lo
    start_x = mouse_pixel(plot_left + (first - scale_lo) / scale_span * plot_width)
    end_x = mouse_pixel(plot_left + (second - scale_lo) / scale_span * plot_width)
    actual = sorted(
        (
            scale_lo + (start_x - plot_left) / plot_width * scale_span,
            scale_lo + (end_x - plot_left) / plot_width * scale_span,
        )
    )
    if not (actual[0] > float(left["hi"]) and actual[1] < float(right["lo"])):
        raise AssertionError(
            f"integral mouse coordinates escaped the sparse gap: {actual}"
        )

    clear_events(page)
    page.mouse.move(start_x, y_mid(sparse))
    page.mouse.down()
    try:
        page.mouse.move(end_x, y_mid(sparse), steps=8)
        if not page.evaluate("!!window.__kymo_zg"):
            raise AssertionError("gap-only positive-control gesture never armed")
    finally:
        page.mouse.up()
    settle(page)
    if events(page):
        raise AssertionError(f"gap-only drag emitted a zoom: {events(page)}")
    assert_no_selection(page)


def click_and_doubleclick(page: Page, source: dict[str, Any]) -> None:
    point = (x_at_fraction(source, 0.5), y_mid(source))
    clear_events(page)
    page.mouse.click(*point)
    settle(page)
    if events(page):
        raise AssertionError(f"plain click emitted a zoom: {events(page)}")
    assert_no_selection(page)

    clear_events(page)
    page.mouse.dblclick(*point, delay=50)
    settle(page)
    if events(page) != [{"source": source["id"], "xmin": None, "xmax": None}]:
        raise AssertionError(f"double click reset was not singular: {events(page)}")
    assert_no_selection(page)


def assert_drag_noop(
    page: Page,
    source: dict[str, Any],
    start: tuple[float, float],
    end: tuple[float, float],
    *,
    return_to_start: bool,
) -> None:
    before = chart_snapshot(page, source["id"])
    clear_events(page)
    page.mouse.move(*start)
    page.mouse.down()
    try:
        page.mouse.move(*end, steps=6)
        if return_to_start:
            page.mouse.move(*start, steps=6)
    finally:
        page.mouse.up()
    settle(page)
    if events(page):
        raise AssertionError(f"no-op drag emitted a zoom event: {events(page)}")
    after = chart_snapshot(page, source["id"])
    assert_close(after["scale_min"], before["scale_min"], "no-op drag min")
    assert_close(after["scale_max"], before["scale_max"], "no-op drag max")
    assert_no_selection(page)


def vertical_drags_do_not_reset(page: Page, source: dict[str, Any]) -> None:
    x = x_at_fraction(source, 0.5)
    plot_y = mouse_pixel(float(source["top"]) + float(source["height"]) * 0.25)
    axis_y = strip_y(source)
    end_y = mouse_pixel(float(source["top"]) + float(source["height"]) * 0.75)
    for start_y in (plot_y, axis_y):
        for return_to_start in (False, True):
            assert_drag_noop(
                page, source, (x, start_y), (x, end_y), return_to_start=return_to_start
            )


def single_click_unzoom(page: Page, source: dict[str, Any]) -> None:
    point = (x_at_fraction(source, 0.5), y_mid(source))
    with single_click_enabled(page):
        clear_events(page)
        page.mouse.click(*point)
        event = one_event(page, source["id"])
        if event["xmin"] is not None or event["xmax"] is not None:
            raise AssertionError(f"single click did not reset zoom: {event}")

        # A browser double-click emits click(detail=1), click(detail=2), then
        # dblclick. Enabled mode must reset exactly once through the first.
        clear_events(page)
        page.mouse.dblclick(*point, delay=50)
        event = one_event(page, source["id"])
        if event["xmin"] is not None or event["xmax"] is not None:
            raise AssertionError(f"enabled double click emitted a range: {event}")

        vertical_drags_do_not_reset(page, source)

        clear_events(page)
        start = (x_at_fraction(source, 0.3), y_mid(source))
        end = (x_at_fraction(source, 0.7), y_mid(source))
        drag(page, start, end)
        event = one_event(page, source["id"])
        if event["xmin"] is None or event["xmax"] is None:
            raise AssertionError(f"selection drag was undone by single click: {event}")

        # Crossing the threshold and returning to the start commits no range,
        # but its trailing click must still be suppressed.
        assert_drag_noop(page, source, start, end, return_to_start=True)

        # The axis-pull fence's one run: a pull latches its trailing click from 1 px of travel along the axis, and that click must not reset either.
        axis_pull(page, source)


def gutter_extrapolation(page: Page, source: dict[str, Any]) -> None:
    gutter_x = mouse_pixel(
        (float(source["container_left"]) + float(source["left"])) * 0.5
    )
    if not gutter_x < float(source["left"]):
        raise AssertionError("source chart has no left gutter")
    clear_events(page)
    start, end = (gutter_x, y_mid(source)), (x_at_fraction(source, 0.35), y_mid(source))
    page.mouse.move(*start)
    page.mouse.down()
    try:
        page.mouse.move(*end, steps=10)
        settle(page)
        box = selections(page, [source["id"]])[0]
        if abs(box["left"]) > 1 or box["width"] <= 10:
            raise AssertionError(
                f"gutter selection did not paint from the left edge: {box}"
            )
    finally:
        page.mouse.up()
    event = one_event(page, source["id"])
    if event["xmin"] is None or event["xmin"] >= source["coverage_min"]:
        raise AssertionError(f"gutter did not preserve extrapolation: {event}")
    assert_no_selection(page)


def cancellation(page: Page, source: dict[str, Any]) -> None:
    start = (x_at_fraction(source, 0.25), y_mid(source))
    end = (x_at_fraction(source, 0.75), start[1])

    clear_events(page)
    page.mouse.move(*start)
    page.mouse.down()
    page.mouse.move(*end, steps=8)
    page.evaluate(
        """point => window.dispatchEvent(new MouseEvent('mousemove', {
            clientX: point.x, clientY: point.y, buttons: 0,
        }))""",
        {"x": end[0], "y": end[1]},
    )
    page.mouse.up()
    one_event(page, source["id"])
    page.evaluate(
        """point => window.dispatchEvent(new MouseEvent('mousemove', {
            clientX: point.x, clientY: point.y, buttons: 0,
        }))""",
        {"x": end[0], "y": end[1]},
    )
    settle(page)
    if len(events(page)) != 1:
        raise AssertionError(f"lost mouseup committed more than once: {events(page)}")
    assert_no_selection(page)

    clear_events(page)
    page.mouse.move(*start)
    page.mouse.down()
    page.mouse.move(*end, steps=8)
    page.evaluate("window.dispatchEvent(new Event('blur'))")
    page.mouse.up()
    settle(page)
    if events(page):
        raise AssertionError(f"blur cancellation committed: {events(page)}")
    assert_no_selection(page)

    clear_events(page)
    page.mouse.move(*start)
    page.mouse.down()
    page.mouse.move(*end, steps=8)
    page.keyboard.press("Escape")
    page.mouse.up()
    settle(page)
    if events(page):
        raise AssertionError(f"Escape cancellation committed: {events(page)}")
    assert_no_selection(page)


def real_resize_cancels(page: Page, source: dict[str, Any]) -> None:
    clear_events(page)
    start = (x_at_fraction(source, 0.25), y_mid(source))
    end = (x_at_fraction(source, 0.75), start[1])
    container = page.locator(f"#{source['id']}")
    original_style = container.evaluate("e => e.style.width")

    page.mouse.move(*start)
    page.mouse.down()
    try:
        page.mouse.move(*end, steps=8)
        if not page.evaluate("!!window.__kymo_zg"):
            raise AssertionError("real-resize positive-control gesture never armed")
        container.evaluate(
            "e => { e.style.width = `${Math.max(100, e.getBoundingClientRect().width - 40)}px`; }"
        )
        # The observer that resizes the chart cancels the gesture in the same callback.
        page.wait_for_function("() => window.__kymo_zg == null")
        # A pull's gutter hold must not engage for a selection, whose owner has no hold: the size would be NaN and the canvas blank.
        assert_gutter_fits(page, source["id"], "selection cancelled by a resize")
    finally:
        page.mouse.up()
        container.evaluate("(e, width) => { e.style.width = width; }", original_style)
        fitted_box(container)

    settle(page)
    if events(page):
        raise AssertionError(f"real resize committed a zoom: {events(page)}")
    assert_no_selection(page)


def fractional_width_edge_line(page: Page, source: dict[str, Any]) -> None:
    """uPlot draws the cursor line at a rounded x and hides it once that x passes a fractional plot width, so a chart resized into a container laid out at a fraction of .5 must take a whole width. check_chart_create.mjs pins the floor at creation."""
    container = page.locator(f"#{source['id']}")
    original_style = container.evaluate("e => e.style.width")
    try:
        # Over 1 px narrower, or the observer, which ignores a change within 1 px, keeps the chart's old whole width.
        width, fraction = container.evaluate(
            "e => { const w = window.__kymo_charts[e.id].width; e.style.width = `${Math.floor(w) - 2.5}px`; return [w, e.getBoundingClientRect().width % 1]; }"
        )
        if fraction != 0.5:
            raise AssertionError(
                f"the container did not lay out at a .5 fraction: {fraction}"
            )
        page.wait_for_function(
            "([id, w]) => window.__kymo_charts[id].width < w - 1",
            arg=[source["id"], width],
        )
        chart = chart_snapshot(page, source["id"])
        page.mouse.move(x_at_fraction(chart, 0.5), y_mid(chart))
        # Into the right padding, which plotPosition snaps to the plot's edge.
        page.mouse.move(mouse_pixel(chart["left"] + chart["width"] + 4), y_mid(chart))
        state = page.evaluate(
            """id => {
                const u = window.__kymo_charts[id];
                return {
                    left: u.cursor.left,
                    plot: parseFloat(u.over.style.width),
                    line: !u.root.querySelector('.u-cursor-x').classList.contains('u-off'),
                };
            }""",
            source["id"],
        )
        # The positive control: the hover reached the edge, where a fractional width loses the line.
        if abs(state["left"] - state["plot"]) > 0.01:
            raise AssertionError(f"right-edge hover missed the plot's edge: {state}")
        if not state["line"]:
            raise AssertionError(
                f"a fractional container width lost the cursor line at the plot's edge: {state}"
            )
    finally:
        page.mouse.move(1, 1)
        container.evaluate("(e, width) => { e.style.width = width; }", original_style)
        fitted_box(container)


def idle_sync(page: Page, source: dict[str, Any], peer: dict[str, Any]) -> None:
    if source["sync_key"] != peer["sync_key"]:
        raise AssertionError("fixture charts are not cursor-synced")
    page.mouse.move(x_at_fraction(source, 0.5), y_mid(source))
    settle(page)
    cursors = page.evaluate(
        "ids => ids.map(id => window.__kymo_charts[id].cursor.left)",
        [source["id"], peer["id"]],
    )
    if any(float(value) < 0 for value in cursors):
        raise AssertionError(f"idle cursor sync positive control failed: {cursors}")
    readouts = page.evaluate(
        """ids => ({
            highlight: window.__kymo_hlrun,
            tips: ids.map(id => {
                const tip = window.__kymo_charts[id].__kymo_tip;
                return {
                    shown: !!tip && getComputedStyle(tip).display === 'block',
                    rows: tip ? tip.querySelectorAll('[data-r]').length : 0,
                };
            }),
        })""",
        [source["id"], peer["id"]],
    )
    if readouts["highlight"] != "zoom-fixture" or any(
        not tip["shown"] or tip["rows"] == 0 for tip in readouts["tips"]
    ):
        raise AssertionError(f"idle hover readouts or highlight failed: {readouts}")
    page.mouse.move(1, 1)
    settle(page)
    cursors = page.evaluate(
        "ids => ids.map(id => window.__kymo_charts[id].cursor.left)",
        [source["id"], peer["id"]],
    )
    if any(float(value) >= 0 for value in cursors):
        raise AssertionError(f"idle cursor sync did not clear: {cursors}")


def single_click_reset(
    page: Page, chart: dict[str, Any], expected: list[dict[str, Any]]
) -> None:
    """A single click on `chart` resets the page through the bridge: one event from it with no bounds, and `expected`'s charts back over their full data."""
    with single_click_enabled(page):
        clear_events(page, intercept=False)
        page.mouse.click(x_at_fraction(chart, 0.5), y_mid(chart))
        reset = one_event(page, chart["id"])
        if reset["xmin"] is not None or reset["xmax"] is not None:
            raise AssertionError(f"single click did not dispatch reset: {reset}")
        wait_for_coverage(page, expected)
    assert_no_selection(page)


def wait_for_coverage(page: Page, expected: list[dict[str, Any]]) -> None:
    page.wait_for_function(
        """expected => expected.every(({id, coverage_min, coverage_max}) => {
            const chart = window.__kymo_charts?.[id];
            if (!chart?.data[0].length) return false;
            const xr = chart.__kymo_zoom?.xrBase;
            const lows = xr == null ? chart.data[0] : chart.data[xr];
            const highs = xr == null ? chart.data[0] : chart.data[xr + 1];
            return Math.abs(lows[0] - coverage_min) < 1e-6
                && Math.abs(highs[highs.length - 1] - coverage_max) < 1e-6;
        })""",
        arg=[
            {key: chart[key] for key in ("id", "coverage_min", "coverage_max")}
            for chart in expected
        ],
        timeout=20_000,
    )


def progressive_refetch_and_single_click_reset(
    page: Page, source: dict[str, Any], peer: dict[str, Any]
) -> None:
    page.evaluate(
        "id => { window.__kymo_zoom_fence.oldChart = window.__kymo_charts[id]; }",
        source["id"],
    )
    clear_events(page, intercept=False)
    drag(
        page,
        (x_at_fraction(source, 0.45), y_mid(source)),
        (x_at_fraction(source, 0.55), y_mid(source)),
    )
    event = one_event(page, source["id"])
    if event["xmin"] is None or event["xmax"] is None:
        raise AssertionError(f"progressive zoom emitted no finite range: {event}")
    if not 0 < event["xmax"] - event["xmin"] < 400:
        raise AssertionError(f"progressive range will not refine below target: {event}")

    page.wait_for_function(
        """id => {
            const state = window.__kymo_zoom_fence;
            const chart = window.__kymo_charts && window.__kymo_charts[id];
            return chart && chart !== state.oldChart && chart.__kymo_zoom
                && chart.__kymo_zoom.xrBase == null && chart.data[0].length > 1;
        }""",
        arg=source["id"],
        timeout=20_000,
    )
    refined = chart_snapshot(page, source["id"])
    if page.evaluate("id => window.__kymo_charts[id].data[0].stepOpen", source["id"]):
        raise AssertionError("a closed step zoom's answer was tagged open")
    assert_close(refined["coverage_min"], math.floor(event["xmin"]), "raw refetch min")
    assert_close(refined["coverage_max"], math.ceil(event["xmax"]), "raw refetch max")
    assert_no_selection(page)
    # Prove the peer consumed the shared zoom before testing reset. The
    # selected interval is inside both fixture domains.
    wait_for_coverage(page, [dict(refined, id=peer["id"])])
    for original in (source, peer):
        assert original["coverage_min"] < refined["coverage_min"]
        assert original["coverage_max"] > refined["coverage_max"]

    single_click_reset(page, refined, [source, peer])
    for original in (source, peer):
        restored = chart_snapshot(page, original["id"])
        assert_close(restored["scale_min"], original["scale_min"], "reset scale min")
        assert_close(restored["scale_max"], original["scale_max"], "reset scale max")


def following_step_zoom_through_the_store(
    page: Page, source: dict[str, Any], peer: dict[str, Any]
) -> None:
    """A following left-half pull that narrows the unzoomed source goes through ZoomBridge to the step store as an open window: its answer comes back with no step maximum (stepOpen on its x column) and ends at the newest step, so a second left-half press on it follows too. A single click then resets the page."""
    clear_events(page, intercept=False)
    y = strip_y(source)
    drag(page, (x_at_fraction(source, 0.45), y), (float(source["left"]) + 30, y))
    event = one_event(page, source["id"])
    if event["xmax"] is not None or not event["xmin"] > source["coverage_min"]:
        raise AssertionError(
            f"a narrowing following pull did not send an open window inside the data: {event}"
        )
    page.wait_for_function(
        """([id, lo]) => {
            const chart = window.__kymo_charts && window.__kymo_charts[id];
            return chart && chart.data[0].stepOpen === true && chart.data[0][0] > lo;
        }""",
        arg=[source["id"], source["coverage_min"]],
        timeout=20_000,
    )
    answered = chart_snapshot(page, source["id"])
    assert_close(
        answered["scale_max"],
        answered["newest_x"],
        "the open answer's window ends at its newest step",
    )
    page.mouse.move(x_at_fraction(answered, 0.25), strip_y(answered))
    page.mouse.down()
    try:
        follows = page.evaluate("() => !!(window.__kymo_zg && window.__kymo_zg.follow)")
    finally:
        page.keyboard.press("Escape")
        page.mouse.up()
    if not follows:
        raise AssertionError("a left-half press on the open answer did not follow")
    single_click_reset(page, answered, [source, peer])


def reset_window(page: Page, chart: dict[str, Any]) -> None:
    page.evaluate(
        """state => {
            const chart = window.__kymo_charts[state.id];
            chart.setScale('x', {min: state.min, max: state.max});
            chart.__kymo_userzoom = false;
        }""",
        {"id": chart["id"], "min": chart["scale_min"], "max": chart["scale_max"]},
    )


# A live reply mid-pull, as a fresh data array with one more bucket (each x column a bucket further on, every other column repeating its last value), read back after uPlot's commit (a microtask) and two frames. The original data waits in the event capture's state for restore_data. Returns the chart's x window, newest x and x shift, and where the grabbed value sits against the pointer.
REPLY_ONE_BUCKET = """async ([id, grabbed, px]) => {
    const chart = window.__kymo_charts[id];
    const zoom = chart.__kymo_zoom, xs = chart.data[0], step = xs[xs.length - 1] - xs[xs.length - 2];
    const isX = i => i === 0 || (zoom.xrBase != null && (i === zoom.xrBase || i === zoom.xrBase + 1));
    window.__kymo_zoom_fence.data = chart.data;
    chart.setData(chart.data.map((column, i) => {
        const grown = Array.from(column);
        grown.push(isX(i) ? grown[grown.length - 1] + step : grown[grown.length - 1]);
        // The x column keeps what push_data_to_js puts on it.
        if (i === 0) grown.stepOpen = column.stepOpen;
        return grown;
    }));
    await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
    const x = chart.data[0];
    return {
        min: chart.scales.x.min,
        max: chart.scales.x.max,
        newest: x[x.length - 1],
        off: chart.over.getBoundingClientRect().left + chart.valToPos(grabbed, 'x') - px,
        shift: zoom.readoutXShift,
    };
}"""


def restore_data(page: Page, chart: dict[str, Any]) -> None:
    page.evaluate(
        "id => window.__kymo_charts[id].setData(window.__kymo_zoom_fence.data)",
        chart["id"],
    )
    reset_window(page, chart)


def grabbed_value(page: Page, identifier: str, plot_x: float) -> float:
    """The x value at plot x `plot_x` on chart `identifier`."""
    return page.evaluate(
        "([id, px]) => window.__kymo_charts[id].posToVal(px, 'x')",
        [identifier, plot_x],
    )


def pull_following_reply(
    page: Page, chart: dict[str, Any], what: str
) -> dict[str, Any]:
    """A left-half pull at a quarter of the chart's plot, moved 40 px, a live bucket added under the still hand, then released: the pull must follow the reply, its right edge at the reply's newest x and the grabbed value under the pointer. Returns REPLY_ONE_BUCKET's reading; `what` names the pull in a failure."""
    x, y = x_at_fraction(chart, 0.25), strip_y(chart)
    page.mouse.move(x, y)
    page.mouse.down()
    try:
        # The plot's left edge as the pull pressed on it: an earlier gesture's re-fit may have moved it since the snapshot.
        left = page.evaluate(
            "id => window.__kymo_charts[id].over.getBoundingClientRect().left",
            chart["id"],
        )
        grabbed = grabbed_value(page, chart["id"], x - left)
        page.mouse.move(x + 40, y, steps=8)
        settle(page)
        held = page.evaluate(REPLY_ONE_BUCKET, [chart["id"], grabbed, x + 40])
        if held["max"] != held["newest"] or abs(held["off"]) > 0.5:
            raise AssertionError(
                f"a following {what} did not take a reply's newest x under a still hand: {held}"
            )
    finally:
        page.mouse.up()
    return held


def axis_pull(page: Page, source: dict[str, Any]) -> None:
    """A left-half pull on the unzoomed source, which shows its newest step, follows it: a reply mid-pull moves its right edge to the new newest step with the grabbed value under the pointer, and the release leaves xmax open. Showing a closed window, the pull keeps the pinned edge's coverage."""
    clear_events(page)
    held = pull_following_reply(page, source, "step pull")
    event = one_event(page, source["id"])
    if event["xmax"] is not None:
        raise AssertionError(f"axis pull at the newest step did not follow it: {event}")
    assert_close(
        event["xmin"], held["min"] - held["shift"], "axis hand-owned bound as shown"
    )
    if event["xmin"] >= source["coverage_min"]:
        raise AssertionError(f"axis hand-owned bound did not extrapolate: {event}")
    restore_data(page, source)
    x, y = x_at_fraction(source, 0.25), strip_y(source)
    with showing_a_closed_window(page, source["id"]):
        clear_events(page)
        drag(page, (x, y), (x + 40, y))
        event = one_event(page, source["id"])
    assert_close(event["xmax"], source["coverage_max"], "axis pinned right extent")
    reset_window(page, source)
    assert_gutter_fits(page, source["id"], "step pulls released")


def axis_pull_keeps_window_through_reply(page: Page, source: dict[str, Any]) -> None:
    """AI-1533: a right-half pull, which fixes its window, keeps it through a reply mid-pull, and a release before another move dispatches that window: the pinned min at its coverage, the hand's max as shown."""
    x, y = x_at_fraction(source, 0.75), strip_y(source)
    clear_events(page)
    page.mouse.move(x, y)
    page.mouse.down()
    try:
        page.mouse.move(x + 30, y, steps=6)
        settle(page)
        before = page.evaluate(
            "id => [window.__kymo_charts[id].scales.x.min, window.__kymo_charts[id].scales.x.max]",
            source["id"],
        )
        # A fixed pull reads only the reply's window: no grabbed value or pointer to check.
        held = page.evaluate(REPLY_ONE_BUCKET, [source["id"], 0, 0])
        if [held["min"], held["max"]] != before or held["newest"] <= before[1]:
            raise AssertionError(
                f"a reply mid-pull moved a fixed pull's window: {before}, {held}"
            )
    finally:
        page.mouse.up()
    event = one_event(page, source["id"])
    assert_close(event["xmin"], source["coverage_min"], "axis pinned left extent")
    assert_close(event["xmax"], before[1], "axis hand-owned bound as shown")
    restore_data(page, source)
    assert_gutter_fits(page, source["id"], "fixed step pull released after a reply")


GUTTER_STATE = """id => {
    const chart = window.__kymo_charts[id];
    const ax = chart.axes[1];
    const over = chart.over.getBoundingClientRect();
    // What the labels need, measured apart from the chart's size function (which returns the hold while one exists); with no labels that function gives its floor.
    const ctx = document.createElement('canvas').getContext('2d');
    ctx.font = ax.font[0];
    const widest = Math.max(0, ...ax._values.map(label => ctx.measureText(label).width));
    const labelNeed = Math.ceil(ax.ticks.size + ax.gap + widest / devicePixelRatio);
    return {
        owner: window.__kymo_zg ? window.__kymo_zg.srcId : null,
        left: over.left,
        width: over.width,
        size: ax._size,
        label_need: labelNeed,
        // The 52 is create.js's cap: its 12 px right pad plus the 40 px minimum plot.
        need: Math.min(Math.max(ax.size(chart, [], 1), labelNeed), Math.max(0, chart.width - 52)),
        min: chart.scales.x.min,
        max: chart.scales.x.max,
    };
}"""


def gutter_state(page: Page, identifier: str) -> dict[str, Any]:
    return page.evaluate(GUTTER_STATE, identifier)


def assert_gutter_fits(page: Page, identifier: str, what: str) -> dict[str, Any]:
    settle(page)
    settle(page)
    state = gutter_state(page, identifier)
    if state["owner"] is not None or state["size"] != state["need"]:
        raise AssertionError(f"{what}: the gutter does not fit its labels: {state}")
    return state


def pull_holding_gutter(
    page: Page, chart: dict[str, Any], start: float, end: float
) -> dict[str, Any]:
    """Press the axis strip at plot fraction `start` and move 1 px at a time to page x `end`, checking after each move that the pull owns the chart, the plot's left edge and width are the press's, and, between the pixel clamps, the grabbed value is under the pointer. The hand stays down."""
    x, y = x_at_fraction(chart, start), strip_y(chart)
    page.mouse.move(x, y)
    page.mouse.down()
    press = gutter_state(page, chart["id"])
    grabbed = grabbed_value(page, chart["id"], x - press["left"])
    inset = min(25.0, press["width"] / 4)
    step = 1 if end > x else -1
    for px in range(int(x) + step, int(end) + step, step):
        page.mouse.move(px, y)
        state = page.evaluate(
            """([id, value, px]) => {
                const chart = window.__kymo_charts[id];
                const over = chart.over.getBoundingClientRect();
                return {owner: window.__kymo_zg ? window.__kymo_zg.srcId : null, left: over.left, width: over.width, off: over.left + chart.valToPos(value, 'x') - px};
            }""",
            [chart["id"], grabbed, px],
        )
        if (
            state["owner"] != chart["id"]
            or state["left"] != press["left"]
            or state["width"] != press["width"]
        ):
            raise AssertionError(
                f"the pull let the plot move at x {px}: {state}, pressed {press}"
            )
        unclamped = (
            press["left"] + inset <= px <= press["left"] + press["width"] - inset
        )
        if unclamped and abs(state["off"]) > 0.5:
            raise AssertionError(
                f"the grabbed value left the pointer at x {px}: {state}"
            )
    return press


def axis_pull_holds_gutter(page: Page, chart: dict[str, Any]) -> None:
    """AI-1510: a pull holds the y gutter at its pressed width while the labels change (zoom_d_gutter's need drops below it, then rises past it, as a left-half pull narrows the window past steps 500 and 700: seed_zoom_fixture.py), and every exit re-fits it. A selection holds nothing, so the gutter re-fits under it, and its owner must survive that."""
    left_end = float(chart["left"]) + 30
    right_end = float(chart["left"]) + float(chart["width"]) - 30
    # Pull 1: a left-half pull, which follows the newest step, released: its labels change width, and the release that re-fits them still commits.
    clear_events(page)
    press = pull_holding_gutter(page, chart, 0.45, left_end)
    shown = gutter_state(page, chart["id"])
    page.mouse.up()
    after = assert_gutter_fits(page, chart["id"], "left-half pull released")
    if after["size"] == press["size"]:
        raise AssertionError(
            f"the fixture's labels never changed width: {press}, {after}"
        )
    if [after["min"], after["max"]] != [shown["min"], shown["max"]]:
        raise AssertionError(f"the release moved the pulled window: {shown}, {after}")
    event = one_event(page, chart["id"])
    if event["xmax"] is not None or event["xmin"] is None:
        raise AssertionError(
            f"a following pull's release did not leave xmax open: {event}"
        )
    reset_window(page, chart)
    assert_gutter_fits(page, chart["id"], "reset")
    # Pull 2: the same pull ended by Escape, then by a window blur.
    for end in ("Escape", "blur"):
        clear_events(page)
        pull_holding_gutter(page, chart, 0.45, left_end)
        if end == "Escape":
            page.keyboard.press("Escape")
        else:
            page.evaluate("window.dispatchEvent(new Event('blur'))")
        page.mouse.up()
        state = assert_gutter_fits(page, chart["id"], f"pull ended by {end}")
        assert_close(
            state["min"], chart["scale_min"], f"{end} restores the start window's min"
        )
        assert_close(
            state["max"], chart["scale_max"], f"{end} restores the start window's max"
        )
        if events(page):
            raise AssertionError(f"a pull ended by {end} committed: {events(page)}")
    # Pull 3: a right-half pull, released: the pinned min at its coverage, the hand's max as shown.
    clear_events(page)
    pull_holding_gutter(page, chart, 0.55, right_end)
    shown = gutter_state(page, chart["id"])
    page.mouse.up()
    after = assert_gutter_fits(page, chart["id"], "right-half pull released")
    if [after["min"], after["max"]] != [shown["min"], shown["max"]]:
        raise AssertionError(f"the release moved the pulled window: {shown}, {after}")
    event = one_event(page, chart["id"])
    assert_close(event["xmin"], chart["coverage_min"], "right-half pull's pinned min")
    assert_close(event["xmax"], shown["max"], "right-half pull's hand-owned max")
    reset_window(page, chart)
    # Pull 4: a right-half press (a fixed pull, so x stays put), its labels changed under the still hand (every column after x × 1e-6, which adds leading zeros at any increment uPlot picks, so the labels change width at any plot height): held, then re-fitted by Escape, then by a release without travel.
    scale_columns = """id => {
        const chart = window.__kymo_charts[id];
        window.__kymo_zoom_fence.data = chart.data;
        chart.setData([chart.data[0], ...chart.data.slice(1).map(column => Array.from(column, v => v == null ? v : v * 1e-6))]);
    }"""
    for end in ("Escape", "mouseup"):
        clear_events(page)
        x, y = x_at_fraction(chart, 0.55), strip_y(chart)
        page.mouse.move(x, y)
        page.mouse.down()
        press = gutter_state(page, chart["id"])
        page.evaluate(scale_columns, chart["id"])
        settle(page)
        held = gutter_state(page, chart["id"])
        if [held["size"], held["left"], held["width"]] != [
            press["size"],
            press["left"],
            press["width"],
        ] or held["label_need"] == press["label_need"]:
            raise AssertionError(
                f"labels that changed under a still hand moved the held gutter, or never changed: {press}, {held}"
            )
        if end == "Escape":
            page.keyboard.press("Escape")
        page.mouse.up()
        assert_gutter_fits(page, chart["id"], f"still hand, labels changed, then {end}")
        if events(page):
            raise AssertionError(f"a pull without travel committed: {events(page)}")
        restore_data(page, chart)
        assert_gutter_fits(page, chart["id"], "data restored")
    # A selection's owner holds nothing: the same reply mid-drag re-fits the gutter under it, an internal resize that leaves the selection live.
    clear_events(page)
    x, y = x_at_fraction(chart, 0.3), y_mid(chart)
    page.mouse.move(x, y)
    page.mouse.down()
    try:
        page.mouse.move(x_at_fraction(chart, 0.6), y, steps=6)
        press = gutter_state(page, chart["id"])
        page.evaluate(scale_columns, chart["id"])
        settle(page)
        state = gutter_state(page, chart["id"])
        if (
            state["owner"] != chart["id"]
            or state["size"] != state["need"]
            or state["size"] == press["size"]
        ):
            raise AssertionError(
                f"a selection held the gutter, or lost its owner to an internal resize: {press}, {state}"
            )
    finally:
        page.keyboard.press("Escape")
        page.mouse.up()
    restore_data(page, chart)
    assert_gutter_fits(page, chart["id"], "selection cancelled")
    if events(page):
        raise AssertionError(f"a cancelled selection committed: {events(page)}")


def client_zoom_fences(
    page: Page, source: dict[str, Any], peer: dict[str, Any]
) -> None:
    identifiers = [source["id"], peer["id"]]

    def states() -> list[dict[str, Any]]:
        return page.evaluate(
            """ids => ids.map(id => {
                const chart = window.__kymo_charts[id];
                const x = chart.data[0];
                return {
                    id,
                    userzoom: chart.__kymo_userzoom || false,
                    min: chart.scales.x.min,
                    max: chart.scales.x.max,
                    full_min: x[0],
                    full_max: x[x.length - 1],
                };
            })""",
            identifiers,
        )

    clear_events(page)
    drag(
        page,
        (x_at_fraction(source, 0.25), y_mid(source)),
        (x_at_fraction(source, 0.75), y_mid(source)),
    )
    settle(page)
    if events(page):
        raise AssertionError(f"client-only zoom emitted a server event: {events(page)}")
    zoomed = states()
    for state in zoomed:
        if not state["userzoom"] or not (
            state["min"] > state["full_min"] and state["max"] < state["full_max"]
        ):
            raise AssertionError(f"client sync peer did not zoom: {state}")

    # A section height change resizes the charts in place, so a client zoom, which lives on the chart instance, survives it.
    charts = page.evaluate_handle(
        "ids => ids.map(id => window.__kymo_charts[id])", identifiers
    )
    rect = page.locator(f"#{source['id']}").locator(
        "xpath=ancestor::*[contains(@class, 'metric-rect')][1]"
    )
    heights = charts.evaluate("charts => charts.map(chart => chart.height)")
    drag_chart_height(page, rect, 80)
    try:
        page.wait_for_function(
            """([ids, before]) => ids.every((id, i) => {
                const chart = window.__kymo_charts[id];
                return chart && chart.height !== before[i]
                    && Math.abs(chart.height - chart.root.parentElement.getBoundingClientRect().height) < 1;
            })""",
            arg=[identifiers, heights],
        )
    except TimeoutError as error:
        raise AssertionError(
            "the client-zoomed charts did not take their section's new height"
        ) from error
    if not page.evaluate(
        "([charts, ids]) => charts.every((chart, i) => chart === window.__kymo_charts[ids[i]])",
        [charts, identifiers],
    ):
        raise AssertionError("a section height change rebuilt a client-zoomed chart")
    for before, after in zip(zoomed, states()):
        if not after["userzoom"]:
            raise AssertionError(
                f"a section height change reset a client zoom: {after}"
            )
        assert_close(
            after["min"], before["min"], "client zoom minimum after a height change"
        )
        assert_close(
            after["max"], before["max"], "client zoom maximum after a height change"
        )
    # The click below aims at the chart as it now sits.
    source = chart_snapshot(page, source["id"])

    with single_click_enabled(page):
        vertical_drags_do_not_reset(page, source)
        page.mouse.click(x_at_fraction(source, 0.5), y_mid(source))
        settle(page)
        for state in states():
            if state["userzoom"]:
                raise AssertionError(f"single click left a client peer zoomed: {state}")
            assert_close(state["min"], state["full_min"], "client reset minimum")
            assert_close(state["max"], state["full_max"], "client reset maximum")

    # A left-half axis pull on the unzoomed source follows its newest point: a reply mid-pull moves the right edge with the grabbed value under the pointer, and the release leaves the source and its peer following, each to its own newest x. The taller charts may have put the source's axis strip below the viewport.
    page.locator(f"#{source['id']}").scroll_into_view_if_needed()
    source = chart_snapshot(page, source["id"])
    held = pull_following_reply(page, source, "client pull")
    settle(page)
    for state in states():
        if state["userzoom"] != "follow" or state["max"] != state["full_max"]:
            raise AssertionError(
                f"a following client pull's release did not leave the group following: {states()}"
            )
        assert_close(
            state["min"], held["min"], "the pulled start on every client chart"
        )
    if events(page):
        raise AssertionError(f"a client pull emitted a server event: {events(page)}")
    restore_data(page, source)
    reset_window(page, peer)


def missing_chart_returns_at_an_unchanged_reply(page: Page) -> None:
    """A chart whose uPlot instance went missing comes back at its next reply, even one that changes nothing (UPlotChart's reply check, uplot_chart.rs). A tab of its own routes its socket, destroys the peer chart, and pushes a version of the fixture run above any it has, as the server pushes when a run logs: each chart of the run polls once, and its reply keeps the held model, as the source chart's untouched instance shows."""
    tab = page.context.new_page()
    errors: list[str] = []
    tab.on("pageerror", lambda error: errors.append(str(error)))
    # Each chart's QueryChart requests as [request id, answered].
    polls: dict[str, list[list]] = {SOURCE_TITLE: [], PEER_TITLE: []}
    routes = []

    def connect(ws) -> None:
        server = ws.connect_to_server()
        routes.append(ws)

        def from_page(message) -> None:
            if isinstance(message, bytes):
                request_id, path, body = request_frame(message)
                for title, sent in polls.items():
                    if path == "/kymo.Kymo/QueryChart" and title.encode() in body:
                        sent.append([request_id, False])
            server.send(message)

        def from_server(message) -> None:
            if isinstance(message, bytes):
                request_id = struct.unpack_from("<I", message)[0]
                for sent in polls.values():
                    for poll in sent:
                        poll[1] |= poll[0] == request_id
            ws.send(message)

        ws.on_message(from_page)
        server.on_message(from_server)

    def wait_until(done, what: str, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while not done():
            if time.monotonic() > deadline:
                raise AssertionError(f"timed out waiting for {what}: {polls}")
            tab.wait_for_timeout(50)

    def answered() -> bool:
        return all(done for sent in polls.values() for _, done in sent)

    tab.route_web_socket(
        re.compile(r"/(?:grpc-ws|trash/_kymo-grpc-ws-local-v1)"), connect
    )
    try:
        started = time.monotonic()
        tab.goto(page.url, wait_until="domcontentloaded")
        for title in polls:
            tab.wait_for_selector(
                f".metric-rect:has(.rect-title-pop:text-is('{title}')) .u-over",
                timeout=30_000,
            )
        source, peer = chart_id(tab, SOURCE_TITLE), chart_id(tab, PEER_TITLE)
        # Quiet: every query answered, and none sent for a second.
        sent = -1
        while sent != sum(map(len, polls.values())) or not answered():
            wait_until(answered, "the page's chart queries", 20)
            sent = sum(map(len, polls.values()))
            tab.wait_for_timeout(1_000)
        tab.evaluate(
            """([source, peer]) => {
                const witness = window.__kymo_charts[source];
                witness.__fence_sets = 0;
                const setData = witness.setData;
                witness.setData = function (...data) { this.__fence_sets++; return setData.apply(this, data); };
                window.__kymo_charts[peer].destroy();
                delete window.__kymo_charts[peer];
            }""",
            [source, peer],
        )
        before = {title: len(sent) for title, sent in polls.items()}
        run_id = RUN_PATH.rsplit("/", 1)[1]
        # Far above any version the fixture run reaches, so the page takes it as new.
        push = pb.RunVersionsEvent(run_versions={run_id: 1 << 40}).SerializeToString()
        routes[-1].send(encode_response(0, push))
        wait_until(
            lambda: all(
                len(polls[t]) > before[t] and polls[t][before[t]][1] for t in polls
            ),
            "each chart's poll after the pushed version",
            15,
        )
        try:
            tab.wait_for_function(
                "id => !!window.__kymo_charts[id]?.root?.isConnected",
                arg=peer,
                timeout=5_000,
            )
        except TimeoutError:
            raise AssertionError(
                f"the destroyed chart did not come back at its unchanged reply: {polls}"
            ) from None
        witness = tab.evaluate(
            "id => window.__kymo_charts[id].__fence_sets ?? null", source
        )
        if any(len(polls[t]) != before[t] + 1 for t in polls) or witness != 0:
            raise AssertionError(
                f"expected one poll per chart, answered without new data (source setData: {witness}): {polls}"
            )
        if errors:
            raise AssertionError(f"uncaught browser errors: {errors}")
        print(
            f"missing chart returned at its one unchanged reply, {time.monotonic() - started:.1f} s for the step"
        )
    finally:
        tab.close()


def run_fences(page: Page) -> None:
    page_errors = []

    def record_page_error(error) -> None:
        page_errors.append(str(error))

    page.on("pageerror", record_page_error)
    try:
        for title in (SOURCE_TITLE, PEER_TITLE, SPARSE_TITLE, GUTTER_TITLE):
            page.wait_for_selector(
                f".metric-rect:has(.rect-title-pop:text-is('{title}')) .u-over",
                timeout=30_000,
            )

        set_single_click_unzoom(page, False)
        install_event_capture(page)
        source = chart_snapshot(page, chart_id(page, SOURCE_TITLE))
        if not source["has_xr"]:
            raise AssertionError("zoom fixture must start with bucketed readouts")
        peer = chart_snapshot(page, chart_id(page, PEER_TITLE))
        sparse = chart_snapshot(page, chart_id(page, SPARSE_TITLE))
        gutter = chart_snapshot(page, chart_id(page, GUTTER_TITLE))

        source_over_peer(page, source, peer)
        true_edge(page, source)
        sparse_gap_noop(page, sparse)
        click_and_doubleclick(page, source)
        single_click_unzoom(page, source)
        gutter_extrapolation(page, source)
        cancellation(page, source)
        real_resize_cancels(page, source)
        fractional_width_edge_line(page, source)
        idle_sync(page, source, peer)
        axis_pull_holds_gutter(page, gutter)
        axis_pull_keeps_window_through_reply(page, source)
        progressive_refetch_and_single_click_reset(page, source, peer)
        following_step_zoom_through_the_store(page, source, peer)

        system = page.locator(".section:has(.section-name:text-is('system'))")
        if system.count() != 1:
            raise AssertionError(f"expected one system section, found {system.count()}")
        system.scroll_into_view_if_needed()
        if system.locator(".section-grid").count() == 0:
            system.locator(".section-name").click()
        for title in (CLIENT_SOURCE_TITLE, CLIENT_PEER_TITLE):
            page.wait_for_selector(
                f".metric-rect:has(.rect-title-pop:text-is('{title}')) .u-over",
                timeout=30_000,
            )
        client_source = chart_snapshot(page, chart_id(page, CLIENT_SOURCE_TITLE))
        client_peer = chart_snapshot(page, chart_id(page, CLIENT_PEER_TITLE))
        client_zoom_fences(page, client_source, client_peer)
        settle(page)
        missing_chart_returns_at_an_unchanged_reply(page)
        if page_errors:
            raise AssertionError("uncaught browser errors")
    finally:
        if page_errors:
            print("page errors:", page_errors, file=sys.stderr)
        page.remove_listener("pageerror", record_page_error)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="zoom fixture run URL")
    parser.add_argument(
        "--browser", choices=("chromium", "firefox", "webkit"), default="chromium"
    )
    args = parser.parse_args()

    with sync_playwright() as playwright:
        browser = getattr(playwright, args.browser).launch(headless=True)
        # A context of its own: the missing-chart step opens a second tab, which Playwright refuses in a page's own context.
        page = browser.new_context(viewport=VIEWPORT, device_scale_factor=1).new_page()
        try:
            page.goto(args.url, wait_until="domcontentloaded")
            run_fences(page)
        finally:
            browser.close()
    print("zoom gesture fences passed")


if __name__ == "__main__":
    main()
