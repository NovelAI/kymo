"""Browser fences for log tabs, virtualization and following, using an intercepted WebSocket RPC fixture (no backend).

Run against one local ``dx serve`` with Playwright >= 1.48 and protobuf installed:
    python text_scroll_fences.py http://127.0.0.1:8080/
"""

import argparse
from contextlib import contextmanager
import importlib.util
import json
import math
from pathlib import Path
import re
import sys
import time
from urllib.parse import urlsplit

from playwright.sync_api import (
    ElementHandle,
    Locator,
    Page,
    WebSocketRoute,
    expect,
    sync_playwright,
)


from fences_common import encode_response, request_frame
from fences_common import render_turn as settle
from user_settings_fences import open_settings, set_font_size


PROJECT = "text-scroll-e2e"
RUNS = ("scroll-run-a", "scroll-run-b", "scroll-run-live")
LIVE = RUNS[2]
PRIMARY = "logs/00-primary"
SIBLING = "logs/01-sibling"
METRICS = [PRIMARY, SIBLING] + [f"logs/filler-{n:02d}" for n in range(30)]
GRID = f'main.main-content [data-slot-id="{PRIMARY}"]'
SIBLING_GRID = f'main.main-content [data-slot-id="{SIBLING}"]'
OVERLAY = f'.maximize-content [data-slot-id="{PRIMARY}"]'
VIEWPORT = {"width": 1_600, "height": 900}
MAX_GRID_ROWS = 400
BAND = 80  # Mirrors OVERSCAN_LINES in text_stream/viewport.rs.

spec = importlib.util.spec_from_file_location(
    "text_scroll_pb2",
    Path(__file__).resolve().parents[2] / "python_client/kymo/_generated/kymo_pb2.py",
)
pb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pb)


class Fixture:
    def __init__(self) -> None:
        self.queries = []
        self.errors = []
        self.hold = False
        self.pending = []
        self.totals = {}
        self.sockets = []
        self.runs = list(RUNS)
        self.versions = {run: 1 for run in RUNS}
        self.reject_next = {}
        self.rejected_lines = {}
        self.text_revisions = {}
        self.wide_rows = {}
        self.rejections = []

    def connect(self, socket: WebSocketRoute) -> None:
        self.sockets.append(socket)
        socket.on_message(lambda message: self.request(socket, message))

    def push(self, run: str) -> None:
        self.versions[run] += 1
        event = pb.RunVersionsEvent(run_versions={run: self.versions[run]})
        for socket in self.sockets:
            socket.send(encode_response(0, event.SerializeToString()))

    def request(self, socket: WebSocketRoute, message: bytes) -> None:
        identifier, path, payload = request_frame(message)
        if path == "/kymo.push/control":
            return
        method = path.rsplit("/", 1)[-1]
        if method == "ListProjects":
            response = pb.ListProjectsResponse(project_ids=[PROJECT])
        elif method == "ListRuns":
            response = pb.ListRunsResponse(
                runs=[
                    pb.RunInfo(
                        project_id=PROJECT,
                        run_id=run,
                        run_name=run,
                        ordinal=len(self.runs) - i,
                        created_at_ms=1_700_000_000_000,
                        status=(
                            pb.RUN_STATUS_RUNNING
                            if run == LIVE
                            else pb.RUN_STATUS_FINISHED
                        ),
                    )
                    for i, run in enumerate(self.runs)
                ]
            )
        elif method in ("ListMetrics", "ListRunSetMetrics"):
            response = pb.ListMetricsResponse(
                metrics=[
                    pb.MetricInfo(
                        metric_name=name, metric_type=pb.MetricInfo.TEXT_STREAM
                    )
                    for name in METRICS
                ]
            )
        elif method == "PollVersions":
            response = pb.PollVersionsResponse(
                global_version=1,
                project_version=1,
                run_versions=self.versions,
            )
        elif method == "QueryTextWindow":
            request = pb.QueryTextWindowRequest.FromString(payload)
            self.queries.append(request)
            key = (request.run_id, request.metric_names[0], request.search)
            rejection = self.reject_next.pop(key, None)
            rejected_line = self.rejected_lines.get(key)
            if (
                rejected_line is not None
                and request.line_offset
                <= rejected_line
                < request.line_offset + request.line_limit
            ):
                rejection = 8  # ResourceExhausted for a permanently oversized line.
            if rejection is not None:
                self.rejections.append((request, rejection))
                socket.send(
                    encode_response(
                        identifier, b"fixture stream rejected", code=rejection
                    )
                )
                return
            if self.hold and PRIMARY in request.metric_names:
                self.pending.append((socket, identifier, request))
                return
            response = self.text_window(request)
        else:
            # Unexpected calls are answered locally and fail the fence. Never
            # forward a request to a real server, including mutation paths.
            self.errors.append(path)
            socket.send(encode_response(identifier, b"Unexpected fixture RPC", code=12))
            return
        socket.send(encode_response(identifier, response.SerializeToString()))

    def text_window(self, request):
        metric = request.metric_names[0]
        key = (request.run_id, metric, request.search)
        revision = self.text_revisions.get(key)
        prefix = f"refresh {revision} " if revision is not None else ""
        wide_rows = self.wide_rows.get(key, ())
        total = self.totals.get(key, 8_000 if metric in (PRIMARY, SIBLING) else 5)
        return pb.QueryTextWindowResponse(
            total_lines=total,
            first_step=1_000,
            lines=[
                pb.TextLine(
                    step=1_000 + line,
                    metric_name=metric,
                    line_index=line,
                    text=(
                        ("wide " + "x" * 4_000 + " " if line in wide_rows else "")
                        + f"{prefix}{request.run_id} {metric} {request.search or 'all'} line {line:05d}"
                    ),
                )
                for line in range(
                    request.line_offset,
                    min(total, request.line_offset + request.line_limit),
                )
            ],
        )

    @contextmanager
    def holding(self):
        self.hold = True
        try:
            yield
        finally:
            self.release()

    def release(self) -> None:
        self.hold = False
        for socket, identifier, request in self.pending:
            response = self.text_window(request)
            socket.send(encode_response(identifier, response.SerializeToString()))
        self.pending.clear()

    def primary_queries(self, since: int = 0, run: str | None = None):
        return [
            query
            for query in self.queries[since:]
            if PRIMARY in query.metric_names and (run is None or query.run_id == run)
        ]


def body(page: Page, panel: str = GRID) -> Locator:
    return page.locator(panel).locator(".text-stream-log")


def tab(page: Page, run: str, panel: str = GRID) -> Locator:
    return page.locator(panel).get_by_role("tab", name=run, exact=True)


def active_run(page: Page, panel: str = GRID) -> str:
    return (
        page.locator(panel).locator('[role="tab"][aria-selected="true"]').inner_text()
    )


def activate(page: Page, run: str, panel: str = GRID) -> None:
    tab(page, run, panel).click()
    expect(tab(page, run, panel)).to_have_attribute("aria-selected", "true")


@contextmanager
def style_tag(page: Page, content: str):
    element = page.add_style_tag(content=content)
    try:
        yield element
    finally:
        element.evaluate("element => element.remove()")


def position(log: Locator | ElementHandle) -> dict:
    return log.evaluate(
        """element => {
            const bounds = element.getBoundingClientRect();
            const padding = parseFloat(getComputedStyle(element).paddingTop);
            const rows = [...element.querySelectorAll('.text-stream-line')];
            const first = rows.find(row => row.getBoundingClientRect().bottom > bounds.top + padding + 0.1);
            return {
                top: element.scrollTop,
                lineHeight: parseFloat(getComputedStyle(element).getPropertyValue('--kymo-text-line-height')),
                text: first?.textContent ?? null,
                rowTop: first ? first.getBoundingClientRect().top - bounds.top : null,
                count: rows.length,
            };
        }"""
    )


def expect_position(page: Page, log: Locator, expected: dict) -> None:
    page.wait_for_function(
        """({element, expected}) => {
            const bounds = element.getBoundingClientRect();
            const row = [...element.querySelectorAll('.text-stream-line')]
                .find(row => row.textContent === expected.text);
            return Math.abs(element.scrollTop - expected.top) < 0.5 && row
                && Math.abs(row.getBoundingClientRect().top - bounds.top - expected.rowTop) < 0.5;
        }""",
        arg={"element": log.element_handle(), "expected": expected},
    )
    assert position(log)["count"] < MAX_GRID_ROWS, (
        "virtualization rendered an unbounded log"
    )


def expect_start(page: Page, log: Locator) -> dict:
    expect(log.locator(".text-stream-line").first).to_contain_text(
        "line 00000", timeout=20_000
    )
    page.wait_for_function(
        "element => element.scrollTop === 0", arg=log.element_handle()
    )
    settle(page)
    return position(log)


def expect_tail(page: Page, log: Locator, total: int) -> None:
    page.wait_for_function(
        """({element, last}) => {
            if (Math.abs(element.scrollTop - (element.scrollHeight - element.clientHeight)) > 1) return false;
            const rows = [...element.querySelectorAll('.text-stream-line')];
            const row = rows[rows.length - 1];
            const bounds = element.getBoundingClientRect();
            return row?.textContent.endsWith(last)
                && row.getBoundingClientRect().bottom <= bounds.top + element.clientTop + element.clientHeight + 0.5;
        }""",
        arg={"element": log.element_handle(), "last": f"line {total - 1:05d}"},
    )
    assert position(log)["count"] < MAX_GRID_ROWS, (
        "virtualization rendered an unbounded log"
    )


def active_last(page: Page, runs, panel: str = GRID) -> list[str]:
    active = active_run(page, panel)
    return [run for run in runs if run != active] + [active]


def expect_runs(page: Page, expected: dict[str, dict], panel: str = GRID) -> None:
    for run in active_last(page, expected, panel):
        activate(page, run, panel)
        expect_position(page, body(page, panel), expected[run])


def scroll_to_line(page: Page, log: Locator, line: int, fractional: int = 5) -> dict:
    log.evaluate(
        """(element, {line, fractional}) => {
            const height = parseFloat(getComputedStyle(element).getPropertyValue('--kymo-text-line-height'));
            element.scrollTop = line * height + fractional;
        }""",
        {"line": line, "fractional": fractional},
    )
    page.wait_for_function(
        """({element, line}) => [...element.querySelectorAll('.text-stream-line')]
            .some(row => row.textContent.endsWith(`line ${String(line).padStart(5, '0')}`))""",
        arg={"element": log.element_handle(), "line": line},
    )
    settle(page)
    result = position(log)
    assert result["text"].endswith(f"line {line:05d}"), result
    return result


def leave_grid(page: Page) -> None:
    page.locator("main.main-content").evaluate(
        "element => { element.scrollTop = element.scrollHeight; }"
    )
    expect(page.locator(GRID).locator(".text-stream-log")).to_have_count(0)
    settle(page)
    assert page.evaluate(
        "[...window.__textScrollObserved].every(element => element.isConnected)"
    ), "an unmounted log body retained its ResizeObserver"


def return_grid(page: Page) -> None:
    page.locator("main.main-content").evaluate("element => { element.scrollTop = 0; }")
    expect(page.locator(GRID).locator(".text-stream-log")).to_have_count(1)


def wait_until(page: Page, predicate, message: str) -> None:
    deadline = time.monotonic() + 20
    while not predicate() and time.monotonic() < deadline:
        settle(page)
    assert predicate(), message


def await_pending(page: Page, fixture: Fixture, count: int) -> None:
    wait_until(
        page,
        lambda: len(fixture.pending) >= count,
        "remount did not request the expected log windows",
    )


def bands(queries) -> list[tuple[int, int]]:
    return [(query.line_offset, query.line_limit) for query in queries]


def expect_saved_bands(fixture: Fixture, since: int, run: str, saved: dict) -> None:
    queries = fixture.primary_queries(since)
    assert {query.run_id for query in queries} == {run}, (
        "only the active tab's log may fetch rows"
    )
    row = int(saved["text"].rsplit("line ", 1)[1])
    for query in queries:
        assert query.line_offset <= row < query.line_offset + query.line_limit, (
            query.run_id,
            row,
            query.line_offset,
            query.line_limit,
        )


def remount(
    page: Page, fixture: Fixture, expected: dict[str, dict], *, interrupt: bool = False
) -> None:
    # Hold responses so a corrective fetch cannot conceal a wrong first band.
    run = active_run(page)
    leave_grid(page)
    start = len(fixture.queries)
    with fixture.holding():
        return_grid(page)
        await_pending(page, fixture, 1)
        if interrupt:
            # Releasing both mount generations probes stale-owner safety.
            leave_grid(page)
            rejections = len(fixture.rejections)
            fixture.reject_next[(run, PRIMARY, "")] = 14  # UNAVAILABLE retries.
            return_grid(page)
            wait_until(
                page,
                lambda: len(fixture.rejections) == rejections + 1,
                "the remount did not receive the transient failure",
            )
            expect_saved_bands(fixture, start, run, expected[run])
            rejected, _ = fixture.rejections[-1]
            body(page).dispatch_event("scroll")
            await_pending(page, fixture, 2)
            assert bands(fixture.primary_queries(start, run)[-1:]) == bands(
                [rejected]
            ), "a scroll during transient retry changed the saved band"
        expect_saved_bands(fixture, start, run, expected[run])
    expect(tab(page, run)).to_have_attribute("aria-selected", "true")
    expect_position(page, body(page), expected[run])
    expect_saved_bands(fixture, start, run, expected[run])


def submit_search(page: Page, value: str) -> None:
    panel = page.locator(GRID)
    panel.locator(".text-stream-search").fill(value)
    panel.locator(".text-stream-search-submit").click()


def font_roundtrip(page: Page, expected: dict[str, dict]) -> None:
    original_font = int(
        page.locator("html").evaluate(
            "element => parseFloat(getComputedStyle(element).fontSize)"
        )
    )
    for pixels in (20 if original_font != 20 else 24, original_font):
        # Client-side navigation keeps the session stores alive while visiting
        # the actual Projects-only settings UI.
        page.locator(".navbar-brand").click()
        open_settings(page)
        set_font_size(page, pixels)
        page.get_by_role("button", name="Save", exact=True).click()
        expect(page.get_by_role("dialog", name="Settings")).to_have_count(0)
        page.locator(".project-card").filter(has_text=PROJECT).click()
        for run in active_last(page, expected):
            activate(page, run)
            saved = expected[run]
            page.wait_for_function(
                """({element, saved}) => {
                    const height = parseFloat(getComputedStyle(element).getPropertyValue('--kymo-text-line-height'));
                    return Math.abs(element.scrollTop / height - saved.top / saved.lineHeight) < 1
                        && [...element.querySelectorAll('.text-stream-line')].some(row => row.textContent === saved.text);
                }""",
                arg={"element": body(page).element_handle(), "saved": saved},
            )
            assert position(body(page))["text"] == saved["text"]
    print(
        "Projects settings font changes retain the logical log row in both directions"
    )


def hidden_restore(page: Page, fixture: Fixture, expected: dict[str, dict]) -> None:
    run = active_run(page)
    leave_grid(page)
    start = len(fixture.queries)
    with fixture.holding():
        return_grid(page)
        await_pending(page, fixture, 1)
        assert body(page).evaluate(
            "element => window.__textScrollObserved.has(element)"
        ), "the geometry fence did not observe its mounted log body"
        initial_limit = fixture.primary_queries(start, run)[-1].line_limit
        with style_tag(
            page, f"{GRID} .text-stream-viewer {{ height: 800px !important; }}"
        ):
            await_pending(page, fixture, 2)
            assert fixture.primary_queries(start, run)[-1].line_limit > initial_limit, (
                "a pending request did not follow the larger viewport"
            )
            with style_tag(
                page, f"{GRID} .text-stream-log {{ display: none !important; }}"
            ) as hidden:
                settle(page)
                assert body(page).evaluate("element => element.clientHeight") == 0
                fixture.release()
                settle(page)
                hidden_start = len(fixture.queries)
                fixture.push(run)
                # The visible sibling is a positive control: wait for the push to reach its version bridge before checking the hidden body.
                wait_until(
                    page,
                    lambda: any(
                        SIBLING in query.metric_names and query.run_id == run
                        for query in fixture.queries[hidden_start:]
                    ),
                    "hidden-phase pushes were not processed",
                )
                settle(page)
                assert not fixture.primary_queries(hidden_start), (
                    "a version push fetched log rows while the body had zero height"
                )
                hidden.evaluate("element => element.remove()")
                expect_position(page, body(page), expected[run])
                expect_saved_bands(fixture, start, run, expected[run])
    # Let the original geometry's request land before later fences count queries.
    wait_until(
        page,
        lambda: fixture.primary_queries(start, run)[-1].line_limit == initial_limit,
        "the restored viewport did not re-plan its window",
    )
    expect_position(page, body(page), expected[run])
    print(
        "Pending restoration follows larger geometry, pauses while hidden, and resumes after a zero-height body becomes visible"
    )


def record_scroll_restores(element: ElementHandle) -> None:
    # Inspect writes before a corrective RPC can conceal missing rows.
    element.evaluate(
        """element => {
            const native = Object.getOwnPropertyDescriptor(Element.prototype, 'scrollTop');
            element.__restores = [];
            Object.defineProperty(element, 'scrollTop', {
                configurable: true,
                get() { return native.get.call(this); },
                set(value) {
                    native.set.call(this, value);
                    const top = native.get.call(this);
                    if (top <= 0) return;
                    const bounds = this.getBoundingClientRect();
                    const style = getComputedStyle(this);
                    const target = bounds.top + parseFloat(style.paddingTop);
                    const height = parseFloat(style.getPropertyValue('--kymo-text-line-height'));
                    const covered = [...this.querySelectorAll('.text-stream-line')].some(row => {
                        const bounds = row.getBoundingClientRect();
                        return bounds.bottom > target && bounds.top <= target + height;
                    });
                    this.__restores.push({top, covered});
                },
            });
        }"""
    )


def await_tall_request(
    page: Page, fixture: Fixture, element: ElementHandle, since: int
):
    """Return the first request planned at the tall height; the overlay can mount at its old height before the debounced maximize bridge resizes it."""

    def planned():
        height, line_height = element.evaluate(
            "element => [element.offsetHeight, parseFloat(getComputedStyle(element).getPropertyValue('--kymo-text-line-height'))]"
        )
        if height <= VIEWPORT["height"]:
            return None
        limit = math.ceil(height / line_height) + 3 * BAND
        return next(
            (
                query
                for query in fixture.primary_queries(since, RUNS[0])
                if query.line_limit == limit
            ),
            None,
        )

    wait_until(
        page,
        lambda: planned() is not None,
        "the maximized log never planned for the taller viewport",
    )
    return planned()


def tall_tail_restore(page: Page, fixture: Fixture, maximized: dict) -> None:
    page.locator(GRID).get_by_title("Maximize", exact=True).click()
    expect_position(page, body(page, OVERLAY), maximized)
    scroll_to_line(page, body(page, OVERLAY), 7_950)
    page.locator(OVERLAY).get_by_title("Close", exact=True).click()
    page.set_viewport_size({**VIEWPORT, "height": 4_800})
    since = len(fixture.queries)
    observed = []
    try:
        with fixture.holding():
            page.locator(GRID).get_by_title("Maximize", exact=True).click()
            element = body(page, OVERLAY).element_handle()
            saved_band = await_tall_request(page, fixture, element, since)
            start = next(
                index
                for index, query in enumerate(fixture.queries)
                if query is saved_band
            )
            record_scroll_restores(element)
            observed.append(element)
            fixture.release()
            fixture.hold = True
            await_pending(page, fixture, 1)
            correction = fixture.primary_queries(start, RUNS[0])[-1]
            assert (
                correction.line_limit == saved_band.line_limit
                and correction.line_offset < saved_band.line_offset
            ), (
                "the taller viewport did not correct into an earlier band",
                bands([saved_band, correction]),
            )
            assert element.evaluate("element => element.__restores") == [], (
                "the viewport scrolled before its corrected band arrived"
            )
            # Interrupt after the logical anchor was corrected but before its
            # rows arrive: the next mount must use that corrected coordinate.
            page.locator(OVERLAY).get_by_title("Close", exact=True).click()
            remount_start = len(fixture.queries)
            pending_count = len(fixture.pending)
            page.locator(GRID).get_by_title("Maximize", exact=True).click()
            await_pending(page, fixture, pending_count + 1)
            remounted = fixture.primary_queries(remount_start, RUNS[0])
            assert len(remounted) == 1
            assert (remounted[0].line_offset, remounted[0].line_limit) == (
                correction.line_offset,
                correction.line_limit,
            ), "interrupted restoration forgot its corrected anchor"
            element = body(page, OVERLAY).element_handle()
            record_scroll_restores(element)
            observed.append(element)
            fixture.release()
            page.wait_for_function(
                """element => element.scrollTop > 0
                    && Math.abs(element.scrollTop - (element.scrollHeight - element.clientHeight)) <= 10
                    && element.__restores.length > 0""",
                arg=element,
            )
            settle(page)
            restores = element.evaluate("element => element.__restores")
            assert all(item["covered"] for item in restores), restores
            max_rows = element.evaluate(
                "(element, overscan) => Math.ceil(element.clientHeight / parseFloat(getComputedStyle(element).getPropertyValue('--kymo-text-line-height'))) + overscan",
                3 * BAND,
            )
            assert position(body(page, OVERLAY))["count"] <= max_rows
            queries = fixture.primary_queries(start, RUNS[0])
            assert 1 < len(queries) <= 4, bands(queries)
    finally:
        for element in observed:
            element.evaluate("element => { delete element.scrollTop; }")
        if page.locator(OVERLAY).count():
            page.locator(OVERLAY).get_by_title("Close", exact=True).click()
        page.set_viewport_size(VIEWPORT)
    print(
        "A taller viewport waits for matching tail rows, including an interrupted corrective fetch"
    )


def horizontal_scrollbar(
    page: Page, fixture: Fixture, expected: dict[str, dict]
) -> None:
    key = (RUNS[0], PRIMARY, "")
    measurements = []
    try:
        for wide in (True, False):
            if wide:
                fixture.wide_rows[key] = set(range(0, 8_000, 97))
            else:
                fixture.wide_rows.pop(key, None)
            start = len(fixture.queries)
            # Empty-search submission refreshes the current unfiltered band in place.
            submit_search(page, "")
            page.wait_for_function(
                """({element, wide}) => (element.scrollWidth > element.clientWidth) === wide""",
                arg={"element": body(page).element_handle(), "wide": wide},
            )
            expect_position(page, body(page), expected[RUNS[0]])
            settle(page)
            measurements.append(
                body(page).evaluate(
                    "element => ({box: element.offsetHeight, viewport: element.clientHeight})"
                )
            )
            queries = fixture.primary_queries(start, RUNS[0])
            assert len(queries) == 1, (
                "horizontal scrollbar changes refetched the same band",
                bands(queries),
            )
            if wide:
                start = len(fixture.queries)
                remount(page, fixture, expected)
                queries = fixture.primary_queries(start)
                assert len(queries) == 1, (
                    "restoring wide rows refetched the same band",
                    bands(queries),
                )
    finally:
        fixture.wide_rows.pop(key, None)
    if page.context.browser.browser_type.name == "chromium":
        # Fail if Chromium's launch hides scrollbars and
        # silently stops exercising the content-dependent clientHeight change.
        assert measurements[0]["box"] == measurements[1]["box"], measurements
        assert measurements[0]["viewport"] < measurements[1]["viewport"], measurements
    print(
        "Horizontal scrollbars do not refetch an unchanged logical window on refresh or remount",
        measurements,
    )


def prepare(page: Page, fixture: Fixture, url: str) -> None:
    storage = {
        f"kymo_selected_runs_v2_{PROJECT}": json.dumps(RUNS),
        f"kymo_layout_diff_{PROJECT}": json.dumps(
            {
                "format_version": 1,
                "section_overrides": [
                    {
                        "key": "logs",
                        "patch": {
                            "collapsed": False,
                            "priority": 100,
                            "max_columns": 2,
                            "rows_per_page": 0,
                            "chart_height": 280,
                        },
                    }
                ],
            }
        ),
    }
    page.add_init_script(
        f"for (const [key, value] of Object.entries({json.dumps(storage)})) localStorage.setItem(key, value);"
    )
    page.add_init_script(
        """(() => {
            const NativeResizeObserver = window.ResizeObserver;
            window.__textScrollObserved = new Set();
            window.ResizeObserver = class extends NativeResizeObserver {
                constructor(callback) { super(callback); this.textTargets = new Set(); }
                observe(element, options) {
                    super.observe(element, options);
                    if (element.matches('.text-stream-log')) {
                        this.textTargets.add(element);
                        window.__textScrollObserved.add(element);
                    }
                }
                unobserve(element) {
                    super.unobserve(element);
                    this.textTargets.delete(element);
                    window.__textScrollObserved.delete(element);
                }
                disconnect() {
                    super.disconnect();
                    for (const element of this.textTargets) window.__textScrollObserved.delete(element);
                    this.textTargets.clear();
                }
            };
        })()"""
    )
    page.route_web_socket(
        re.compile(r"/(?:grpc-ws|trash/_kymo-grpc-ws-local-v1)$"), fixture.connect
    )
    page.route(
        "**/alerts",
        lambda route: route.fulfill(content_type="application/json", body="[]"),
    )
    # dx's development shell may inject an external Inter stylesheet. The
    # dashboard itself uses bundled fonts; keep startup independent of Google.
    page.route(
        "https://fonts.googleapis.com/**",
        lambda route: route.fulfill(content_type="text/css", body=""),
    )
    project_url = f"{url.rstrip('/')}/{PROJECT}/"

    def configure_standalone_dx(route) -> None:
        response = route.fetch(url=url.rstrip("/") + "/")
        html = response.text()
        if not re.search(r"id=[\"\']kymo-runtime-config[\"\']", html):
            # A local-runtime-feature dx bundle needs the same JSON normally
            # embedded by the local dashboard server. Hosted bundles ignore it.
            config = json.dumps(
                {
                    "websocket_path": "/trash/_kymo-grpc-ws-local-v1",
                    "cdn_origin": f"http://127.0.0.1:{urlsplit(url).port or 8080}",
                }
            )
            html = html.replace(
                "<head>",
                f'<head><script id="kymo-runtime-config" type="application/json">{config}</script>',
                1,
            )
        route.fulfill(
            response=response, body=html, content_type="text/html; charset=utf-8"
        )

    page.route(project_url, configure_standalone_dx)
    page.goto(project_url, wait_until="domcontentloaded")
    expect(body(page).locator(".text-stream-line").first).to_contain_text(
        "line 00000", timeout=20_000
    )
    page.wait_for_function(
        """selector => {
            const row = document.querySelector(selector + ' .text-stream-line');
            return row && row.getBoundingClientRect().height
                === parseFloat(getComputedStyle(row).getPropertyValue('--kymo-text-line-height'));
        }""",
        arg=GRID,
    )
    expect(page.locator(GRID).get_by_role("tab")).to_have_count(len(RUNS))
    expect(page.locator(GRID).locator(".text-stream-log")).to_have_count(1)
    assert not fixture.errors, f"unexpected fixture RPCs: {fixture.errors}"


def toggle_sidebar_run(page: Page, run: str) -> None:
    page.locator(f'.sidebar-run[data-run-id="{run}"] .run-marker-input').click()


def tab_switching(page: Page) -> dict[str, dict]:
    tabs = page.locator(GRID).get_by_role("tablist").get_by_role("tab")
    expect(tabs).to_have_text(list(RUNS))
    for run in RUNS:
        expect(tab(page, run)).to_have_attribute(
            "aria-selected", "true" if run == RUNS[0] else "false"
        )
        expect(tab(page, run)).to_have_attribute("title", run)
    expect(page.locator(GRID).locator(".text-stream-count")).to_have_text("8000 lines")
    styles = tabs.evaluate_all(
        """tabs => tabs.map(tab => {
            const style = getComputedStyle(tab);
            return {color: style.color, border: style.borderBottomColor, background: style.backgroundColor};
        })"""
    )
    active, inactive = styles[0], styles[1]
    assert active["color"] != inactive["color"], "tabs lost their run colors"
    assert active["border"] == active["color"], styles
    assert inactive["border"] == "rgba(0, 0, 0, 0)", styles
    assert active["background"] != inactive["background"], (
        "the active tab must differ by more than color",
        styles,
    )

    saved = {RUNS[0]: scroll_to_line(page, body(page), 4_173)}
    first_log = body(page).element_handle()
    first_tab = tab(page, RUNS[0]).element_handle()
    activate(page, RUNS[1])
    assert not first_log.evaluate("element => element.isConnected")
    assert first_tab.evaluate("element => element.isConnected"), (
        "switching tabs replaced the tab strip"
    )
    settle(page)
    assert page.evaluate(
        "[...window.__textScrollObserved].every(element => element.isConnected)"
    ), "a switched-away log retained its ResizeObserver"
    expect_start(page, body(page))
    saved[RUNS[1]] = scroll_to_line(page, body(page), 2_367, 9)
    expect_position(page, body(page), saved[RUNS[1]])
    print("Tabs carry run identity and semantics; mouse activation swaps only the log")

    # Keyboard activation replays onto the button, which keeps focus across the swap.
    for run, key in ((RUNS[0], "Enter"), (RUNS[1], " ")):
        tab(page, run).focus()
        page.keyboard.press(key)
        expect(tab(page, run)).to_have_attribute("aria-selected", "true")
        expect(tab(page, run)).to_be_focused()
        expect_position(page, body(page), saved[run])
    print("Enter and Space activate a focused tab and restore its run's row")

    leave_grid(page)
    return_grid(page)
    expect(tab(page, RUNS[1])).to_have_attribute("aria-selected", "true")
    expect_position(page, body(page), saved[RUNS[1]])
    print("The active tab survives a Far unmount")

    kept = {run: tab(page, run).element_handle() for run in (RUNS[0], LIVE)}
    # The sidebar paints visibility without taking focus, so a focused later tab must survive the removal.
    tab(page, LIVE).focus()
    toggle_sidebar_run(page, RUNS[1])
    expect(tab(page, RUNS[1])).to_have_count(0)
    assert all(
        handle.evaluate("element => element.isConnected") for handle in kept.values()
    ), "a run leaving the panel recreated the remaining tabs"
    expect(tab(page, LIVE)).to_be_focused()
    expect(tab(page, RUNS[0])).to_have_attribute("aria-selected", "true")
    expect_position(page, body(page), saved[RUNS[0]])
    toggle_sidebar_run(page, RUNS[1])
    expect(tab(page, RUNS[1])).to_have_attribute("aria-selected", "true")
    expect_position(page, body(page), saved[RUNS[1]])
    activate(page, RUNS[0])
    expect_position(page, body(page), saved[RUNS[0]])
    print(
        "A departing run keeps the other tabs and focus; its return restores the choice"
    )
    return saved


def near_restore(page: Page, saved: dict[str, dict]) -> None:
    original_log = body(page).element_handle()
    page.locator("main.main-content").evaluate(
        "element => { element.scrollTop = element.clientHeight; }"
    )
    page.wait_for_function(
        """selector => {
            const root = document.querySelector('main.main-content');
            const bottom = document.querySelector(selector).getBoundingClientRect().bottom;
            const top = root.getBoundingClientRect().top;
            return bottom < top && bottom > top - root.clientHeight;
        }""",
        arg=GRID,
    )
    settle(page)
    assert original_log.evaluate("element => element.isConnected"), (
        "a Near panel unexpectedly unmounted"
    )
    expect_position(page, body(page), saved[active_run(page)])
    return_grid(page)
    print("Near retains the same mounted log and scroll coordinates")


def search_restore(page: Page, fixture: Fixture, saved: dict[str, dict]) -> None:
    submit_search(page, "needle")
    expect(body(page).locator(".text-stream-line").first).to_contain_text(
        "needle line 00000"
    )
    assert position(body(page))["top"] == 0
    expect(page.locator(GRID).locator(".text-stream-count")).to_have_text(
        "8000 matching lines"
    )
    searched = {RUNS[0]: scroll_to_line(page, body(page), 1_827)}
    # The committed search applies to whichever tab is active.
    activate(page, RUNS[1])
    expect(body(page).locator(".text-stream-line").first).to_contain_text(
        f"{RUNS[1]} {PRIMARY} needle line 00000"
    )
    searched[RUNS[1]] = scroll_to_line(page, body(page), 2_119, 7)
    activate(page, RUNS[0])
    expect_position(page, body(page), searched[RUNS[0]])
    page.locator(GRID).locator(".text-stream-search").fill("unfinished draft")
    # Firefox needs the edit to settle before the grid scrolls away.
    settle(page)
    remount(page, fixture, searched)
    expect(page.locator(GRID).locator(".text-stream-search")).to_have_value(
        "unfinished draft"
    )
    assert fixture.primary_queries()[-1].search == "needle", (
        "remount committed an unfinished search draft"
    )
    expect_runs(page, searched)
    start = len(fixture.queries)
    previous_log = body(page).element_handle()
    with fixture.holding():
        submit_search(page, "needle")
        await_pending(page, fixture, 1)
        assert previous_log.evaluate("element => element.isConnected"), (
            "resubmitting the same search replaced its log"
        )
        expect_position(page, body(page), searched[RUNS[0]])
        fixture.text_revisions[(RUNS[0], PRIMARY, "needle")] = 1
    expect(body(page).locator(".text-stream-line").first).to_contain_text("refresh 1")
    refreshed = position(body(page))
    assert abs(refreshed["top"] - searched[RUNS[0]]["top"]) < 0.5
    settle(page)
    assert fixture.primary_queries(start), (
        "resubmitting the same search must refresh data"
    )
    assert all(query.line_offset > 0 for query in fixture.primary_queries(start))
    page.locator(GRID).locator(".text-stream-search-clear").click()
    expect_runs(page, saved)
    print(
        "Search follows the active tab, restores per run, and refreshes in place on resubmit"
    )


def maximize_restore(page: Page, saved: dict[str, dict]) -> dict:
    page.locator(GRID).get_by_title("Maximize", exact=True).click()
    expect(body(page, OVERLAY).locator(".text-stream-line").first).to_contain_text(
        "all line 00000"
    )
    assert position(body(page, OVERLAY))["top"] == 0
    maximized = scroll_to_line(page, body(page, OVERLAY), 3_211, 11)
    page.locator(OVERLAY).get_by_title("Close", exact=True).click()
    expect(page.locator(OVERLAY)).to_have_count(0)
    expect_position(page, body(page), saved[RUNS[0]])
    page.locator(GRID).get_by_title("Maximize", exact=True).click()
    expect_position(page, body(page, OVERLAY), maximized)
    page.locator(OVERLAY).get_by_title("Close", exact=True).click()
    expect_position(page, body(page), saved[RUNS[0]])
    print(
        "Grid and maximized viewers keep independent positions through repeated transitions"
    )
    return maximized


def rejected_band(page: Page, fixture: Fixture, saved: dict[str, dict]) -> None:
    key = (RUNS[0], PRIMARY, "")
    for during_remount in (False, True):
        if during_remount:
            scroll_to_line(page, body(page), 5_000)
            leave_grid(page)
        start = len(fixture.queries)
        rejections = len(fixture.rejections)
        fixture.rejected_lines[key] = 5_000
        try:
            if during_remount:
                return_grid(page)
            else:
                body(page).evaluate(
                    """element => {
                        const height = parseFloat(getComputedStyle(element)
                            .getPropertyValue('--kymo-text-line-height'));
                        element.scrollTop = 5000 * height;
                    }"""
                )
            reset = expect_start(page, body(page))
            assert len(fixture.rejections) == rejections + 1
            queries = fixture.primary_queries(start, RUNS[0])
            assert len(queries) == 2 and queries[-1].line_offset == 0, bands(queries)
            for _ in range(2):
                start = len(fixture.queries)
                remount(page, fixture, {RUNS[0]: reset})
                assert all(
                    query.line_offset == 0
                    for query in fixture.primary_queries(start, RUNS[0])
                ), "a remount returned to the permanently rejected band"
            assert len(fixture.rejections) == rejections + 1
        finally:
            fixture.rejected_lines.pop(key, None)
    saved[RUNS[0]] = scroll_to_line(page, body(page), 4_173)
    expect_runs(page, saved)
    print(
        "An oversized band resets to the top on refresh and first mount, including two Far cycles"
    )


def replacement_streams(page: Page, fixture: Fixture, saved: dict[str, dict]) -> None:
    key = (RUNS[0], PRIMARY, "")
    fixture.reject_next[key] = 5  # NotFound clears the DOM but retains the anchor.
    start = len(fixture.queries)
    fixture.push(RUNS[0])
    expect(body(page)).to_have_text("Run no longer available", timeout=20_000)
    assert position(body(page))["top"] == 0, "error state retained phantom log extent"
    expect(page.locator(GRID).locator(".text-stream-count")).to_have_count(0)
    fixture.push(RUNS[0])
    expect_position(page, body(page), saved[RUNS[0]])
    assert all(
        query.line_offset > 0 for query in fixture.primary_queries(start, RUNS[0])
    ), "a terminal refresh discarded the saved anchor before recovery"
    print("A terminal error clears its DOM and restores its anchor after a live push")

    fixture.reject_next[key] = 3  # InvalidArgument resets to the top.
    start = len(fixture.queries)
    fixture.push(RUNS[0])
    expect_start(page, body(page))
    queries = fixture.primary_queries(start, RUNS[0])
    assert len(queries) == 2 and queries[-1].line_offset == 0, bands(queries)
    scroll_to_line(page, body(page), 4_173)
    print("An invalid band forgets its saved anchor and refetches from line zero")

    leave_grid(page)
    fixture.totals[key] = 240
    start = len(fixture.queries)
    return_grid(page)
    shrunk = expect_start(page, body(page))
    expect(body(page).locator(".text-stream-line").last).to_contain_text("line 00239")
    assert shrunk["count"] < MAX_GRID_ROWS, shrunk
    queries = fixture.primary_queries(start, RUNS[0])
    assert len(queries) == 2 and queries[-1].line_offset == 0, bands(queries)
    remount(page, fixture, {RUNS[0]: shrunk})
    print("A saved anchor beyond a shorter stream resets to the top and stays there")

    leave_grid(page)
    fixture.totals[key] = 0
    return_grid(page)
    expect(body(page)).to_have_text("No logs yet")
    assert position(body(page))["top"] == 0
    fixture.totals[key] = 8_000
    start = len(fixture.queries)
    fixture.push(RUNS[0])
    expect_start(page, body(page))
    queries = fixture.primary_queries(start, RUNS[0])
    assert queries and all(query.line_offset == 0 for query in queries), bands(queries)
    assert not fixture.errors, f"unexpected fixture RPCs: {fixture.errors}"
    print(
        "An empty replacement stream clears its stale anchor before a live data push regrows it"
    )


def grow(page: Page, fixture: Fixture, run: str, total: int) -> None:
    """Wait until a pushed stream's new extent is rendered."""
    fixture.totals[(run, PRIMARY, "")] = total
    start = len(fixture.queries)
    fixture.push(run)
    wait_until(
        page,
        lambda: any(query.run_id == run for query in fixture.primary_queries(start)),
        "the push did not refetch the mounted log",
    )
    page.wait_for_function(
        """({element, total}) => {
            const height = parseFloat(getComputedStyle(element).getPropertyValue('--kymo-text-line-height'));
            const padding = parseFloat(getComputedStyle(element).paddingTop) + parseFloat(getComputedStyle(element).paddingBottom);
            return Math.abs(element.scrollHeight - padding - total * height) < 1;
        }""",
        arg={"element": body(page).element_handle(), "total": total},
    )
    settle(page)


def follow_live_tail(page: Page, fixture: Fixture) -> None:
    # The first request only measures the stream; its head rows must never render.
    page.locator(GRID).evaluate(
        """(panel, run) => {
            window.__liveHeadShown = false;
            window.__liveHeadObserver?.disconnect();
            window.__liveHeadObserver = new MutationObserver(() => {
                if ([...panel.querySelectorAll('.text-stream-line')]
                    .some(row => row.textContent === `${run} logs/00-primary all line 00000`)) {
                    window.__liveHeadShown = true;
                }
            });
            window.__liveHeadObserver.observe(panel, {childList: true, subtree: true});
        }""",
        LIVE,
    )
    start = len(fixture.queries)
    activate(page, LIVE)
    expect_tail(page, body(page), 8_000)
    settle(page)
    queries = fixture.primary_queries(start, LIVE)
    assert queries[0].line_offset == 0 and 2 <= len(queries) <= 3, bands(queries)
    assert not page.evaluate("window.__liveHeadShown"), (
        "opening a live run rendered its head before the tail"
    )
    page.evaluate("window.__liveHeadObserver.disconnect()")
    for total in (8_050, 8_330):
        grow(page, fixture, LIVE, total)
        expect_tail(page, body(page), total)
    print("Live tail opens without a head flash and follows pushes")

    line_height = position(body(page))["lineHeight"]
    body(page).evaluate(
        "(element, height) => { element.scrollTop -= 5 * height; }", line_height
    )
    settle(page)
    paused = position(body(page))
    grow(page, fixture, LIVE, 8_400)
    expect_position(page, body(page), paused)
    print("Scrolling up stops following")

    body(page).evaluate("element => { element.scrollTop = element.scrollHeight; }")
    expect_tail(page, body(page), 8_400)
    grow(page, fixture, LIVE, 8_500)
    expect_tail(page, body(page), 8_500)
    print("Returning to the bottom resumes following")

    leave_grid(page)
    fixture.totals[(LIVE, PRIMARY, "")] = 8_700
    return_grid(page)
    expect(tab(page, LIVE)).to_have_attribute("aria-selected", "true")
    expect_tail(page, body(page), 8_700)
    activate(page, RUNS[0])
    fixture.totals[(LIVE, PRIMARY, "")] = 8_800
    activate(page, LIVE)
    expect_tail(page, body(page), 8_800)
    print("A followed end survives Far and tab remounts, resuming at the new bottom")

    mid = scroll_to_line(page, body(page), 5_000)
    activate(page, RUNS[0])
    fixture.totals[(LIVE, PRIMARY, "")] = 8_900
    activate(page, LIVE)
    expect_position(page, body(page), mid)
    grow(page, fixture, LIVE, 9_000)
    expect_position(page, body(page), mid)
    print("A remembered mid-log row of a live run restores without following")

    submit_search(page, "needle")
    expect_tail(page, body(page), 8_000)
    expect(body(page).locator(".text-stream-line").last).to_contain_text(
        f"{LIVE} {PRIMARY} needle line 07999"
    )
    page.locator(GRID).locator(".text-stream-search-clear").click()
    expect_position(page, body(page), mid)
    print("A search on a live run opens at its last match")

    activate(page, RUNS[0])
    expect_start(page, body(page))
    body(page).evaluate("element => { element.scrollTop = element.scrollHeight; }")
    expect_tail(page, body(page), 8_000)
    finished = position(body(page))
    try:
        grow(page, fixture, RUNS[0], 8_100)
        expect_position(page, body(page), finished)
    finally:
        fixture.totals.pop((RUNS[0], PRIMARY, ""), None)
    print("A finished run at its bottom does not follow growth")
    assert not fixture.errors, f"unexpected fixture RPCs: {fixture.errors}"


def expect_settled_queries(
    page: Page, fixture: Fixture, since: int, run: str, most: int
) -> None:
    settle(page)
    count = len(fixture.primary_queries(since, run))
    page.wait_for_timeout(500)
    queries = fixture.primary_queries(since, run)
    assert len(queries) == count and count <= most, bands(queries)


def follow_across_scrollbar_bands(page: Page, fixture: Fixture) -> None:
    key = (LIVE, PRIMARY, "")
    activate(page, LIVE)
    try:
        with style_tag(
            page,
            f"{GRID} .text-stream-log {{ flex: none !important; height: 213px !important; }}",
        ):
            settle(page)
            height, bar, line_height, padding = body(page).evaluate(
                """element => {
                    const probe = element.cloneNode(false);
                    probe.style.cssText = 'position: absolute; left: -9999px; height: 100px; width: 100px; flex: none';
                    const row = document.createElement('div');
                    row.className = 'text-stream-line';
                    row.textContent = 'x'.repeat(500);
                    probe.appendChild(row);
                    element.parentElement.appendChild(probe);
                    const scrollbar = probe.offsetHeight - probe.clientHeight;
                    probe.remove();
                    const style = getComputedStyle(element);
                    return [
                        element.offsetHeight,
                        scrollbar,
                        parseFloat(style.getPropertyValue('--kymo-text-line-height')),
                        parseFloat(style.paddingTop) + parseFloat(style.paddingBottom),
                    ];
                }"""
            )
            if bar == 0:
                print(
                    "Skipped the scrollbar band case: overlay scrollbars take no space"
                )
                return

            def top_row(total: int, client: int) -> int:
                return math.floor(
                    (total * line_height + padding - client) / line_height
                )

            # Pick a length whose bottoms with and without the scrollbar fall in different bands, and put the wide row only in the earlier band.
            total = next(
                total
                for total in range(8_000, 8_400)
                if top_row(total, height) // BAND
                != top_row(total, height - bar) // BAND
            )
            fixture.wide_rows[key] = {
                top_row(total, height - bar) // BAND * BAND - 3 * BAND // 2
            }
            grow(page, fixture, LIVE, total - 1)
            body(page).evaluate(
                "element => { element.scrollTop = element.scrollHeight; }"
            )
            expect_tail(page, body(page), total - 1)
            start = len(fixture.queries)
            grow(page, fixture, LIVE, total)
            expect_tail(page, body(page), total)
            expect_settled_queries(page, fixture, start, LIVE, 3)
            activate(page, RUNS[0])
            start = len(fixture.queries)
            activate(page, LIVE)
            expect_tail(page, body(page), total)
            expect_settled_queries(page, fixture, start, LIVE, 3)
    finally:
        fixture.wide_rows.pop(key, None)
    print("A scrollbar band change settles without a refetch loop")


def dense_tabs(page: Page) -> None:
    with style_tag(
        page,
        f"{GRID} .text-stream-viewer {{ width: 110px !important; height: 130px !important; }}",
    ):
        page.wait_for_function(
            """selector => {
                const panel = document.querySelector(selector);
                const strip = panel.querySelector('[role="tablist"]');
                const log = panel.querySelector('.text-stream-log');
                const line = parseFloat(getComputedStyle(log).getPropertyValue('--kymo-text-line-height'));
                return strip.scrollHeight > strip.clientHeight
                    && log.clientHeight >= line
                    && log.querySelector('.text-stream-line');
            }""",
            arg=GRID,
        )
    print("A crowded tab strip scrolls and leaves the log room to fetch")


def new_run_keeps_unclicked_tab(page: Page, fixture: Fixture) -> None:
    shown = active_run(page, SIBLING_GRID)
    fixture.runs.insert(0, "scroll-run-new")
    fixture.versions["scroll-run-new"] = 1
    # A reconnect makes the dashboard refetch ListRuns; the new run sorts first and is auto-selected.
    for socket in list(fixture.sockets):
        socket.close()
    tabs = page.locator(SIBLING_GRID).get_by_role("tab")
    expect(tabs).to_have_count(len(RUNS) + 1)
    expect(tabs.first).to_have_text("scroll-run-new")
    assert active_run(page, SIBLING_GRID) == shown, (
        "a new run took over a panel whose tab was never clicked"
    )
    print("A newly started run does not take over a panel whose tab was never clicked")


def run(page: Page, fixture: Fixture, url: str) -> None:
    prepare(page, fixture, url)
    saved = tab_switching(page)
    sibling = scroll_to_line(page, body(page, SIBLING_GRID), 1_579, 3)
    near_restore(page, saved)
    remount(page, fixture, saved, interrupt=True)
    expect_position(page, body(page, SIBLING_GRID), sibling)
    expect_runs(page, saved)
    print(
        "Far remount, interruption, and transient retry preserve each tab and panel, including partial-line pixels; no zero-window fetch"
    )
    font_roundtrip(page, saved)
    expect_runs(page, saved)
    hidden_restore(page, fixture, saved)
    horizontal_scrollbar(page, fixture, saved)

    search_restore(page, fixture, saved)
    maximized = maximize_restore(page, saved)
    tall_tail_restore(page, fixture, maximized)

    rejected_band(page, fixture, saved)
    replacement_streams(page, fixture, saved)
    follow_live_tail(page, fixture)
    follow_across_scrollbar_bands(page, fixture)
    dense_tabs(page)
    new_run_keeps_unclicked_tab(page, fixture)


def run_fences(page: Page, base_url: str) -> None:
    """Register fixtures before navigation; Chromium must omit --hide-scrollbars."""
    page.goto("about:blank")
    page.set_viewport_size(VIEWPORT)
    page.set_default_timeout(20_000)
    failures = []
    page.on("pageerror", lambda error: failures.append(str(error)))
    fixture = Fixture()
    try:
        run(page, fixture, base_url)
        assert not failures, f"uncaught browser errors: {failures}"
    except BaseException:
        print(
            f"Fixture errors: {fixture.errors}; browser errors: {failures}",
            file=sys.stderr,
        )
        print(f"Current URL: {page.url}", file=sys.stderr)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", nargs="?", default="http://127.0.0.1:8080/")
    parser.add_argument(
        "--browser", choices=("chromium", "firefox", "webkit"), default="chromium"
    )
    args = parser.parse_args()
    with sync_playwright() as playwright:
        options = (
            {"ignore_default_args": ["--hide-scrollbars"]}
            if args.browser == "chromium"
            else {}
        )
        browser = getattr(playwright, args.browser).launch(**options)
        try:
            run_fences(browser.new_page(viewport=VIEWPORT), args.url)
        finally:
            browser.close()


if __name__ == "__main__":
    main()
