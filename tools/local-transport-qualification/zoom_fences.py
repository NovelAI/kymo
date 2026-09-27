"""Browser fences for kymo's source-owned chart zoom session.

Most cases isolate event production; one reaches ZoomBridge and proves a real
envelope-to-raw refetch and shared single-click reset against ``seed_zoom_fixture.py``.
"""

import argparse
import math
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from playwright.sync_api import Page, sync_playwright

from fences_common import render_turn as settle


SOURCE_TITLE = "zoom_a_source"
PEER_TITLE = "zoom_b_peer"
SPARSE_TITLE = "zoom_c_sparse"
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


def drag(page: Page, start: tuple[float, float], end: tuple[float, float]) -> None:
    page.mouse.move(*start)
    page.mouse.down()
    try:
        page.mouse.move(*end, steps=12)
    finally:
        page.mouse.up()


def source_over_peer(page: Page, source: dict[str, Any], peer: dict[str, Any]) -> None:
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

    event = one_event(page, source["id"])
    if event["xmax"] is None or event["xmax"] <= source["coverage_max"]:
        raise AssertionError(
            f"peer traversal did not extrapolate in source space: {event}"
        )
    assert_no_selection(page)


def true_edge(page: Page, source: dict[str, Any]) -> None:
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
    axis_y = mouse_pixel(
        min(
            float(source["container_bottom"]) - 1,
            float(source["top"]) + float(source["height"]) + 10,
        )
    )
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

        # Axis pull owns a separate gesture path and needs the same trailing-
        # click suppression.
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
    original_style = page.evaluate(
        "id => document.getElementById(id).style.width", source["id"]
    )

    page.mouse.move(*start)
    page.mouse.down()
    try:
        page.mouse.move(*end, steps=8)
        if not page.evaluate("!!window.__kymo_zg"):
            raise AssertionError("real-resize positive-control gesture never armed")
        resized_width = page.evaluate(
            """id => {
                const element = document.getElementById(id);
                const width = Math.max(100, element.getBoundingClientRect().width - 40);
                element.style.width = `${width}px`;
                return width;
            }""",
            source["id"],
        )
        page.wait_for_function(
            """state => {
                const chart = window.__kymo_charts[state.id];
                return chart && Math.abs(chart.width - state.width) <= 1
                    && window.__kymo_zg == null;
            }""",
            arg={"id": source["id"], "width": resized_width},
        )
    finally:
        page.mouse.up()
        page.evaluate(
            "state => { document.getElementById(state.id).style.width = state.width; }",
            {"id": source["id"], "width": original_style},
        )
        page.wait_for_function(
            """id => {
                const chart = window.__kymo_charts[id];
                const width = document.getElementById(id).getBoundingClientRect().width;
                return chart && Math.abs(chart.width - width) <= 1;
            }""",
            arg=source["id"],
        )

    settle(page)
    if events(page):
        raise AssertionError(f"real resize committed a zoom: {events(page)}")
    assert_no_selection(page)


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
    assert_close(refined["coverage_min"], math.floor(event["xmin"]), "raw refetch min")
    assert_close(refined["coverage_max"], math.ceil(event["xmax"]), "raw refetch max")
    assert_no_selection(page)
    # Prove the peer consumed the shared zoom before testing reset. The
    # selected interval is inside both fixture domains.
    wait_for_coverage(page, [dict(refined, id=peer["id"])])
    for original in (source, peer):
        assert original["coverage_min"] < refined["coverage_min"]
        assert original["coverage_max"] > refined["coverage_max"]

    with single_click_enabled(page):
        clear_events(page, intercept=False)
        page.mouse.click(x_at_fraction(refined, 0.5), y_mid(refined))
        reset = one_event(page, source["id"])
        if reset["xmin"] is not None or reset["xmax"] is not None:
            raise AssertionError(f"single click did not dispatch reset: {reset}")
        wait_for_coverage(page, [source, peer])
        for original in (source, peer):
            restored = chart_snapshot(page, original["id"])
            assert_close(
                restored["scale_min"], original["scale_min"], "reset scale min"
            )
            assert_close(
                restored["scale_max"], original["scale_max"], "reset scale max"
            )
        assert_no_selection(page)


def axis_pull(page: Page, source: dict[str, Any]) -> None:
    clear_events(page)
    x = x_at_fraction(source, 0.25)
    y = mouse_pixel(
        min(
            float(source["container_bottom"]) - 1,
            float(source["top"]) + float(source["height"]) + 10,
        )
    )
    drag(page, (x, y), (x + 40, y))
    event = one_event(page, source["id"])
    assert_close(event["xmax"], source["coverage_max"], "axis pinned right extent")
    if event["xmin"] is None or event["xmin"] >= source["coverage_min"]:
        raise AssertionError(f"axis hand-owned bound did not extrapolate: {event}")
    page.evaluate(
        """state => {
            const chart = window.__kymo_charts[state.id];
            chart.setScale('x', {min: state.min, max: state.max});
            chart.__kymo_userzoom = false;
        }""",
        {
            "id": source["id"],
            "min": source["scale_min"],
            "max": source["scale_max"],
        },
    )


def axis_internal_reflow_keeps_owner(page: Page, source: dict[str, Any]) -> None:
    clear_events(page)
    # Pulling the right half far enough to widen the visible source values adds
    # one y-label decimal in this fixture. uPlot grows the y axis internally
    # without changing the chart container, so the active owner must survive.
    x = x_at_fraction(source, 0.75)
    y = mouse_pixel(
        min(
            float(source["container_bottom"]) - 1,
            float(source["top"]) + float(source["height"]) + 10,
        )
    )
    page.mouse.move(x, y)
    page.mouse.down()
    try:
        page.mouse.move(x + 700, y, steps=12)
        settle(page)
        state = page.evaluate(
            """id => {
                const chart = window.__kymo_charts[id];
                return {
                    owner: window.__kymo_zg?.srcId ?? null,
                    plot_left: chart.over.getBoundingClientRect().left,
                };
            }""",
            source["id"],
        )
        if state["owner"] != source["id"]:
            raise AssertionError(f"internal axis reflow cancelled its owner: {state}")
        if abs(float(state["plot_left"]) - float(source["left"])) < 1:
            raise AssertionError(f"fixture did not change y-axis width: {state}")
    finally:
        page.keyboard.press("Escape")
        page.mouse.up()
    settle(page)
    if events(page):
        raise AssertionError(f"cancelled axis reflow committed: {events(page)}")
    assert_no_selection(page)


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
                    userzoom: !!chart.__kymo_userzoom,
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

    with single_click_enabled(page):
        vertical_drags_do_not_reset(page, chart_snapshot(page, source["id"]))
        page.mouse.click(x_at_fraction(source, 0.5), y_mid(source))
        settle(page)
        for state in states():
            if state["userzoom"]:
                raise AssertionError(f"single click left a client peer zoomed: {state}")
            assert_close(state["min"], state["full_min"], "client reset minimum")
            assert_close(state["max"], state["full_max"], "client reset maximum")


def run_fences(page: Page) -> None:
    page_errors = []

    def record_page_error(error) -> None:
        page_errors.append(str(error))

    page.on("pageerror", record_page_error)
    try:
        for title in (SOURCE_TITLE, PEER_TITLE, SPARSE_TITLE):
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

        source_over_peer(page, source, peer)
        true_edge(page, source)
        sparse_gap_noop(page, sparse)
        click_and_doubleclick(page, source)
        single_click_unzoom(page, source)
        gutter_extrapolation(page, source)
        cancellation(page, source)
        real_resize_cancels(page, source)
        idle_sync(page, source, peer)
        axis_internal_reflow_keeps_owner(page, source)
        axis_pull(page, source)
        progressive_refetch_and_single_click_reset(page, source, peer)

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
        page = browser.new_page(viewport=VIEWPORT, device_scale_factor=1)
        try:
            page.goto(args.url, wait_until="domcontentloaded")
            run_fences(page)
        finally:
            browser.close()
    print("zoom gesture fences passed")


if __name__ == "__main__":
    main()
