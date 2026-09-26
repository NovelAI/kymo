"""Local browser probe for the compiled frontend; never run this in CI.

See ../../docs/copy-text-validation.md for setup, coverage, and hand tests.
"""

import argparse
import base64
import importlib.util
import json
import struct
import sys
import traceback
from collections import Counter
from pathlib import Path
from urllib.parse import unquote, urlsplit

from playwright.sync_api import Browser, Error, Page, expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]
PROJECT = "copy-probe"
ORIGIN = "http://copy-probe.test:8097"
TEXT = "  alpha bravo charlie\r\ndelta\tEND  "
CAPTION = "  caption bravo charlie\r\ndelta\tEND  "
PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)
RESIZE_OBSERVER_NOTIFICATIONS = {
    "ResizeObserver loop completed with undelivered notifications.",
    "ResizeObserver loop limit exceeded",
}


def protobuf():
    spec = importlib.util.spec_from_file_location(
        "kymo_pb2", ROOT / "python_client/kymo/_generated/kymo_pb2.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Fixture:
    def __init__(self, page: Page, args, *, mode="SelectIndex"):
        self.page, self.args, self.pb = page, args, protobuf()
        self.run_ids = [f"run-{index}" for index in range(2)]
        self.metrics = ["info/run_info", "images/sample"]
        self.blocked = []
        self.errors = []
        page.on("pageerror", lambda error: self.errors.append(str(error)))
        layout = {
            "format_version": 1,
            "project_chart_defaults": {"metadata_diff_only": False},
            "section_overrides": [
                {"key": name, "patch": {"collapsed": False, "max_columns": 1}}
                for name in ("info", "images")
            ],
            "rect_overrides": [
                {
                    "key": "images/sample",
                    "patch": {"options": {"cdn_display_mode": mode}},
                }
            ],
        }
        page.add_init_script(
            f"localStorage.setItem('kymo_layout_diff_{PROJECT}', {json.dumps(json.dumps(layout))});"
            f"localStorage.setItem('kymo_selected_runs_v2_{PROJECT}', {json.dumps(json.dumps(self.run_ids))});"
            "window.copyProbe = {copies: [], commands: [], calls: [], pending: []};"
            "window.addEventListener('copy', event => {"
            "  copyProbe.copies.push(event.clipboardData.getData('text/plain'));"
            "});"
            "const originalExec = document.execCommand.bind(document);"
            "document.execCommand = (...args) => {"
            " const ok = copyProbe.denyExec ? false : originalExec(...args);"
            " copyProbe.commands.push({ok, active: navigator.userActivation?.isActive});"
            " return ok;"
            "};"
        )
        page.route("**/*", self.http)
        page.route_web_socket("**/*", self.websocket)

    def run(self, run_id):
        return self.pb.RunInfo(
            project_id=PROJECT,
            run_id=run_id,
            run_name=f"Copy fixture {run_id}",
            ordinal=self.run_ids.index(run_id) + 1,
            created_at_ms=1_700_000_000_000,
            status=self.pb.RUN_STATUS_FINISHED,
            terminated_at_ms=1_700_000_001_000,
        )

    def websocket(self, socket):
        if urlsplit(socket.url).path != "/grpc-ws":
            # dx hot reload is unnecessary for a fixed-bundle probe.
            return
        socket.on_message(lambda frame: self.rpc(socket, frame))

    def rpc(self, socket, frame):
        request_id, length = struct.unpack_from("<IH", frame)
        method = frame[6 : 6 + length].decode().rsplit("/", 1)[-1]
        payload = frame[6 + length :]
        pb = self.pb
        if method == "control":
            return
        if method == "ListProjects":
            response = pb.ListProjectsResponse(project_ids=[PROJECT])
        elif method == "ListRuns":
            response = pb.ListRunsResponse(runs=[self.run(r) for r in self.run_ids])
        elif method == "GetRun":
            request = pb.GetRunRequest.FromString(payload)
            response = pb.GetRunResponse(
                run=pb.RunRecord(run=self.run(request.run_id), state=1)
            )
        elif method in ("ListMetrics", "ListRunSetMetrics"):
            response = pb.ListMetricsResponse(
                metrics=[
                    pb.MetricInfo(metric_name=m, metric_type=1) for m in self.metrics
                ]
            )
        elif method == "PollVersions":
            response = pb.PollVersionsResponse(
                global_version=1,
                project_version=1,
                run_versions={r: 1 for r in self.run_ids},
            )
        elif method == "QueryCdnKeys":
            request = pb.QueryCdnKeysRequest.FromString(payload)
            response = pb.QueryCdnKeysResponse(
                series=[
                    pb.CdnSeries(
                        project_id=PROJECT,
                        run_id=r.run_id,
                        metric_name=r.metric_name,
                        entries=[
                            pb.CdnEntry(
                                step=0,
                                cdn_key=f"probe/{r.run_id}/{r.metric_name}/1.json",
                            )
                        ],
                    )
                    for r in request.refs
                ]
            )
        else:
            self.blocked.append(f"RPC {method}")
            socket.send(struct.pack("<IB", request_id, 12) + b"not in probe")
            return
        socket.send(struct.pack("<IB", request_id, 0) + response.SerializeToString())

    def http(self, route):
        parsed = urlsplit(route.request.url)
        path = unquote(parsed.path)
        headers = {"access-control-allow-origin": "*"}
        if parsed.hostname in ("fonts.googleapis.com", "fonts.gstatic.com"):
            route.fulfill(body="", content_type="text/css")
        elif path == "/alerts":
            route.fulfill(json={"alerts": []}, headers=headers)
        elif path == "/cdn/probe/pixel.png":
            route.fulfill(body=PNG, content_type="image/png", headers=headers)
        elif path.startswith("/cdn/probe/") and path.endswith(".json"):
            run_id = path.split("/")[3]
            if "/info/run_info/" in path:
                data = {
                    "original": TEXT,
                    "empty": "",
                    "boolean": True,
                    "collision": "true" if run_id == "run-0" else True,
                    "array": [1, "two"],
                }
                manifest = {"class": "metadata", "data": data}
            else:
                manifest = {
                    "class": "image_gallery",
                    "items": [
                        {"resource": "probe/pixel.png", "caption": CAPTION},
                        {"resource": "probe/pixel.png", "caption": "second caption"},
                        {"resource": "probe/pixel.png", "caption": ""},
                    ],
                }
            route.fulfill(json=manifest, headers=headers)
        elif parsed.hostname in ("copy-probe.test", "127.0.0.1", "localhost", "::1"):
            asset_path = (
                path if path.startswith(("/assets/", "/wasm/")) else "/index.html"
            )
            # Stream debug WASM to avoid DevTools' base64 frame-size limit.
            route.continue_(url=self.args.url.rstrip("/") + asset_path)
        else:
            self.blocked.append(route.request.url)
            route.abort()

    def open(self, *, secure=False):
        origin = self.args.url.rstrip("/") if secure else ORIGIN
        self.page.goto(f"{origin}/{PROJECT}", wait_until="domcontentloaded")
        try:
            self.page.locator(".metadata-copy-value").first.wait_for(timeout=20_000)
        except Error as error:
            raise AssertionError(
                f"fixture failed: {self.page.locator('body').inner_text()[:2000]}; errors={self.errors}; blocked={self.blocked}"
            ) from error
        assert self.page.evaluate("isSecureContext") is secure
        if not secure:
            assert self.page.evaluate("typeof navigator.clipboard") == "undefined"

    def assert_clean(self):
        assert not self.blocked, self.blocked
        unexpected = [
            error for error in self.errors if error not in RESIZE_OBSERVER_NOTIFICATIONS
        ]
        assert not unexpected, unexpected
        return dict(Counter(self.errors))


def clear_selection(page):
    page.evaluate("getSelection().removeAllRanges()")


def copies(page):
    return page.evaluate("copyProbe.copies")


def wait_for_copy(page, before):
    page.wait_for_function("count => copyProbe.copies.length > count", arg=before)


def metadata(page, key):
    return (
        page.locator(".metadata-row")
        .filter(has=page.locator(".metadata-key-cell", has_text=key))
        .locator(".metadata-copy-value")
        .first
    )


def select_range(target, start, end):
    target.evaluate(
        """(element, [start, end]) => {
            const range = document.createRange();
            const text = document.createTreeWalker(element, NodeFilter.SHOW_TEXT).nextNode();
            range.setStart(text, start); range.setEnd(text, end);
            getSelection().removeAllRanges(); getSelection().addRange(range);
        }""",
        [start, end],
    )


def char_point(target, offset):
    return target.evaluate(
        """(element, offset) => {
            const range = document.createRange();
            const text = document.createTreeWalker(element, NodeFilter.SHOW_TEXT).nextNode();
            range.setStart(text, offset);
            range.setEnd(text, offset + 1);
            const rect = range.getBoundingClientRect();
            return {x: rect.x + rect.width / 2, y: rect.y + rect.height / 2};
        }""",
        offset,
    )


def selection_checks(page, target):
    page.bring_to_front()
    page.locator(".metadata-key-cell").filter(has_text="original").click()
    target.scroll_into_view_if_needed()
    clear_selection(page)
    previous = copies(page)
    start, end = char_point(target, 3), char_point(target, 14)
    page.mouse.move(**start)
    page.mouse.down()
    page.wait_for_timeout(100)
    page.mouse.move(**end, steps=15)
    page.wait_for_timeout(100)
    page.mouse.up()
    selected = page.evaluate("getSelection().toString()")
    drag = "passed"
    if selected:
        assert copies(page) == previous, "drag selection overwrote clipboard"
    else:
        target.evaluate("""element => {
            const plain = element.cloneNode(true), box = element.getBoundingClientRect();
            const style = getComputedStyle(element);
            for (const property of style) plain.style.setProperty(property, style.getPropertyValue(property));
            for (const attribute of ['role', 'tabindex', 'data-dioxus-id']) plain.removeAttribute(attribute);
            plain.dataset.probeSelectionControl = '';
            Object.assign(plain.style, {position:'fixed', left:box.x+'px', top:box.y+'px',
                width:box.width+'px', height:box.height+'px', zIndex:2147483647});
            document.body.appendChild(plain);
        }""")
        plain = page.locator("[data-probe-selection-control]")
        try:
            clear_selection(page)
            start, end = char_point(plain, 3), char_point(plain, 14)
            for point in (start, end):
                assert plain.evaluate(
                    "(element, point) => element.contains(document.elementFromPoint(point.x, point.y))",
                    point,
                ), "plain-control drag coordinates miss its text"
            page.mouse.move(**start)
            page.mouse.down()
            page.wait_for_timeout(100)
            page.mouse.move(**end, steps=15)
            page.mouse.up()
            assert not page.evaluate("getSelection().toString()"), (
                "copy control blocks native drag that works in the plain control"
            )
        finally:
            plain.evaluate("element => element.remove()")
        drag = "inconclusive: the plain span also failed native drag"
    # A synthetic click checks the guard without relying on native drag support.
    previous = copies(page)
    select_range(target, 3, 14)
    target.dispatch_event("click", {"detail": 1})
    assert copies(page) == previous
    assert page.evaluate("getSelection().toString()")
    before = len(copies(page))
    page.keyboard.press("ControlOrMeta+C")
    assert len(copies(page)) == before + 1, "native selection copy did not fire"
    previous = copies(page)
    page.mouse.click(**char_point(target, 10))
    assert page.evaluate("getSelection().isCollapsed"), (
        "click did not dismiss selection"
    )
    assert copies(page)[len(previous) :] == [target.text_content()], (
        "selection-dismiss click did not copy the whole value"
    )
    select_range(target, 3, 8)
    previous = copies(page)
    page.mouse.click(**char_point(target, 17))
    assert page.evaluate("getSelection().isCollapsed"), (
        "click elsewhere did not dismiss selection"
    )
    assert copies(page)[len(previous) :] == [target.text_content()], (
        "click elsewhere in the value did not copy the whole value"
    )
    # Adjusting a selection is not dismissing it.
    select_range(target, 3, 14)
    previous = copies(page)
    page.keyboard.down("Shift")
    page.mouse.click(**char_point(target, 8))
    page.keyboard.up("Shift")
    assert page.evaluate("getSelection().toString()"), (
        "shift-click cleared the selection"
    )
    assert copies(page) == previous, "shift-click copied the whole value"
    clear_selection(page)
    word = char_point(target, 10)
    page.mouse.dblclick(**word)
    previous = copies(page)
    page.mouse.down(click_count=3)
    page.mouse.up(click_count=3)
    assert page.evaluate("getSelection().toString()"), (
        "triple-click cleared the selection"
    )
    assert copies(page) == previous, "triple-click copied the whole value"
    previous = copies(page)
    # Double-click selects a word; only its first click may copy.
    clear_selection(page)
    word = char_point(target, 10)
    page.mouse.dblclick(**word)
    assert page.evaluate("getSelection().toString()").strip(), (
        "double-click selected nothing"
    )
    assert len(copies(page)) == len(previous) + 1
    clear_selection(page)
    return drag


def pointer_copy(page, target, value):
    clear_selection(page)
    target.scroll_into_view_if_needed()
    before = len(copies(page))
    box = target.bounding_box()
    page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
    page.mouse.down()
    assert len(copies(page)) == before, "copy fired on press before release"
    page.mouse.up()
    wait_for_copy(page, before)
    assert copies(page)[before:] == [value]
    assert page.locator("textarea").count() == 0


def keyboard_checks(page, target, value):
    target.focus()
    # A foreign selection must not disable deliberate keyboard activation.
    page.evaluate(
        """() => {
            const range = document.createRange();
            range.selectNodeContents(document.querySelector('.metadata-key-cell'));
            getSelection().removeAllRanges(); getSelection().addRange(range);
        }"""
    )
    for key in ("Enter", "Space"):
        before = len(copies(page))
        scroll = page.evaluate("scrollY")
        target.press(key)
        wait_for_copy(page, before)
        assert copies(page)[before:] == [value]
        expect(target).to_be_focused()
        assert page.evaluate("scrollY") == scroll
        before = len(copies(page))
        page.keyboard.down(key)
        page.keyboard.down(key)
        page.keyboard.up(key)
        wait_for_copy(page, before)
        assert copies(page)[before:] == [value], "held key copied repeatedly"
    clear_selection(page)


def controlled_clipboard(page):
    page.evaluate(
        """() => Object.defineProperty(navigator, 'clipboard', {
            configurable: true,
            value: {writeText(text) {
                copyProbe.calls.push(text);
                const attempt = {};
                const promise = new Promise((resolve, reject) => Object.assign(attempt, {resolve, reject}));
                attempt.settled = promise.then(() => {}, () => {});
                copyProbe.pending.push(attempt);
                return promise;
            }}
        })"""
    )


def finish_copy(page, index, *, ok):
    page.evaluate(
        """async ({index, ok}) => {
            const attempt = copyProbe.pending[index];
            if (ok) attempt.resolve(); else attempt.reject(new Error('denied'));
            await attempt.settled;
            await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
        }""",
        {"index": index, "ok": ok},
    )


def async_checks(page, target):
    controlled_clipboard(page)
    clear_selection(page)
    newer = metadata(page, "boolean")
    target.click()
    page.wait_for_function("copyProbe.pending.length === 1")
    newer.click()
    page.wait_for_function("copyProbe.pending.length === 2")
    assert page.evaluate("copyProbe.calls") == [TEXT, "true"]
    finish_copy(page, 1, ok=True)
    commands = page.evaluate("copyProbe.commands.length")
    finish_copy(page, 0, ok=False)
    assert page.evaluate("copyProbe.commands.length") == commands, (
        "older rejection overwrote a newer successful copy"
    )

    results = []
    for delay in (100, 5500):
        page.evaluate("copyProbe.pending = []")
        target.click()
        page.wait_for_function("copyProbe.pending.length === 1")
        before = len(copies(page))
        commands = page.evaluate("copyProbe.commands.length")
        page.evaluate(
            "delay => setTimeout(() => copyProbe.pending[0].reject(new Error('late')), delay)",
            delay,
        )
        page.wait_for_function(
            "count => copyProbe.commands.length > count",
            arg=commands,
            timeout=delay + 3000,
        )
        # Report the real fallback outcome after user activation expires.
        assert page.evaluate("copyProbe.commands.length") == commands + 1
        result = page.evaluate("index => copyProbe.commands[index]", commands)
        assert len(copies(page)) - before == int(result["ok"])
        assert page.locator("textarea").count() == 0
        results.append({"delay_ms": delay, **result})
    return results


def sidebar_copy(page, index):
    name = f"Copy fixture run-{index}"
    page.get_by_role("button", name=f"More actions for {name}", exact=True).click()
    page.get_by_role("dialog", name=f"Actions for {name}", exact=True).get_by_role(
        "button", name="Copy run name", exact=True
    ).click()


def sidebar_checks(page):
    controlled_clipboard(page)
    page.evaluate("copyProbe.pending = []; copyProbe.calls = []")
    toast = page.locator(".run-copy-feedback")
    sidebar_copy(page, 0)
    page.wait_for_function("copyProbe.pending.length === 1")
    sidebar_copy(page, 1)
    page.wait_for_function("copyProbe.pending.length === 2")
    assert page.evaluate("copyProbe.calls") == [
        "Copy fixture run-0",
        "Copy fixture run-1",
    ]
    finish_copy(page, 1, ok=True)
    commands = page.evaluate("copyProbe.commands.length")
    finish_copy(page, 0, ok=False)
    assert page.evaluate("copyProbe.commands.length") == commands, (
        "older sidebar rejection overwrote a newer successful copy"
    )
    assert toast.inner_text() == "", "sidebar copy showed a message"


def caption_navigation(page):
    slider = page.get_by_role("slider", name="Image index")
    caption = page.locator(".cdn-caption").first
    caption.focus()
    caption.evaluate("element => window.copyProbe.captionNode = element")
    slider.evaluate(
        "e => {e.value = '1'; e.dispatchEvent(new Event('input', {bubbles: true}));}"
    )
    expect(caption).to_have_text("second caption")
    assert caption.evaluate("e => e === copyProbe.captionNode")
    expect(caption).to_be_focused()
    slider.evaluate(
        "e => {e.value = '0'; e.dispatchEvent(new Event('input', {bubbles: true}));}"
    )


def run_checks(browser: Browser, args):
    results = {}
    for mode in ("SelectIndex", "GroupByRun", "Interleaved"):
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        try:
            fixture = Fixture(page, args, mode=mode)
            fixture.open()
            original = metadata(page, "original")
            expect(original).to_have_attribute("tabindex", "0")
            expect(original).to_have_attribute("role", "button")
            pointer_copy(page, original, TEXT)
            pointer_copy(page, metadata(page, "empty"), "")
            collision = metadata(page, "collision")
            expect(collision).to_have_text('"true"')
            pointer_copy(page, collision, "true")
            metadata_drag = selection_checks(page, original)
            caption = page.locator(".cdn-caption").first
            caption.scroll_into_view_if_needed()
            expect(caption).to_have_attribute("tabindex", "0")
            pointer_copy(page, caption, CAPTION)
            caption_drag = selection_checks(page, caption)
            keyboard_checks(page, caption, CAPTION)
            caption.focus()
            page.keyboard.press("Shift+Tab")
            page.keyboard.press("Tab")
            expect(caption).to_be_focused()
            assert caption.evaluate("e => getComputedStyle(e).outlineStyle") != "none"
            if mode == "SelectIndex":
                caption_navigation(page)
                before = len(copies(page))
                sidebar_copy(page, 0)
                wait_for_copy(page, before)
                assert copies(page)[-1] == "Copy fixture run-0"
                assert page.locator(".run-copy-feedback").inner_text() == ""
                assert page.locator("textarea").count() == 0
            else:
                pointer_copy(
                    page,
                    page.locator('.cdn-caption[aria-label="Copy empty text"]').first,
                    "",
                )
            results[mode] = {
                "checks": "passed",
                "metadata_native_drag": metadata_drag,
                "caption_native_drag": caption_drag,
                "diagnostics": fixture.assert_clean(),
            }
        finally:
            page.close()
    page = browser.new_page(viewport={"width": 1440, "height": 1000})
    try:
        fixture = Fixture(page, args)
        fixture.open(secure=True)
        results["late_rejection"] = async_checks(page, metadata(page, "original"))
        sidebar_checks(page)
        results["sidebar_copy"] = "passed"
        results["secure_diagnostics"] = fixture.assert_clean()
    finally:
        page.close()
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8097")
    parser.add_argument(
        "--engines",
        nargs="+",
        choices=("chromium", "firefox", "webkit"),
        default=["chromium", "firefox", "webkit"],
    )
    parser.add_argument("--headed", action="store_true")
    args = parser.parse_args()
    if urlsplit(args.url).hostname not in ("127.0.0.1", "localhost", "::1"):
        parser.error("--url must point to a local dx server")
    failures = []
    with sync_playwright() as playwright:
        for engine in args.engines:
            try:
                browser = getattr(playwright, engine).launch(
                    headless=not args.headed, timeout=30_000
                )
                try:
                    result = run_checks(browser, args)
                    print(
                        json.dumps(
                            {"engine": engine, "version": browser.version, **result}
                        ),
                        flush=True,
                    )
                finally:
                    browser.close()
            except (Error, AssertionError, OSError, ValueError) as error:
                failures.append(engine)
                print(f"{engine}: {error}", file=sys.stderr, flush=True)
                traceback.print_exc()
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
