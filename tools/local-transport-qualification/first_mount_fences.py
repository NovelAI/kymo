"""Measure first-mount RPCs and fence snapshot/poll races in the actual WASM app.

The isolated protobuf fixture records request counts and response bytes. Held
responses order the races; bounded timeouts detect missing or duplicate work.
"""

import argparse
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from threading import Thread
import time
from urllib.parse import urlsplit

from playwright.sync_api import Browser, Page, WebSocketRoute, sync_playwright
from kymo._generated import kymo_pb2 as pb

from fences_common import encode_response, render_turn, request_frame


PROJECT = "first-mount-fixture"
RUN = "run-0"
MEASURED_RUNS = 1_000
# Deployed local route in kymo-server/src/http_transport.rs.
WS_PATH = "/trash/_kymo-grpc-ws-local-v1"


@dataclass(frozen=True)
class Scenario:
    name: str
    snapshot: int | None
    polled: int
    lists: int
    mutation_after_snapshot: bool = False
    push_before_poll: bool = False
    routes: tuple[bool, ...] = (False,)


SCENARIOS = (
    Scenario("legacy-absent", None, 11, 2),
    Scenario("legacy-absent-zero", None, 0, 2),
    Scenario("covered-equal", 11, 11, 1, routes=(False, True)),
    Scenario("covered-zero", 0, 0, 1),
    Scenario(
        "commit-after-snapshot",
        10,
        11,
        2,
        mutation_after_snapshot=True,
        routes=(False, True),
    ),
    Scenario(
        "covered-push-before-poll",
        11,
        11,
        1,
        push_before_poll=True,
        routes=(False, True),
    ),
    Scenario(
        "uncovered-push-before-poll",
        10,
        11,
        2,
        mutation_after_snapshot=True,
        push_before_poll=True,
        routes=(False, True),
    ),
)


def capture_callback(errors: list[str], callback):
    @wraps(callback)
    def captured(*args):
        try:
            return callback(*args)
        except Exception as error:  # Surfaced immediately by drive/settle.
            errors.append(f"{callback.__name__}: {type(error).__name__}: {error}")
            return None

    return captured


class Peer:
    def __init__(self, scenario: Scenario, runs: int) -> None:
        self.scenario = scenario
        self.list_version = scenario.snapshot
        self.runs = runs
        self.socket: WebSocketRoute | None = None
        self.counts: Counter[str] = Counter()
        self.pending: list[tuple[int, str, bytes]] = []
        self.polls: list[int] = []
        self.responses: Counter[str] = Counter()
        self.response_bytes: Counter[str] = Counter()
        self.errors: list[str] = []
        self.published = False
        self.restore_visible = False
        self.connections = 0
        self.allow_reconnect = False
        self.hold_list_responses = False

    def run(self, index: int) -> pb.RunInfo:
        return pb.RunInfo(
            project_id=PROJECT,
            run_id=f"run-{index}",
            run_name=f"{'after' if self.published else 'before'} snapshot {index:05d}",
            ordinal=self.runs - index,
            created_at_ms=1_700_000_000_000,
            status=pb.RUN_STATUS_FINISHED,
            terminated_at_ms=1_700_000_001_000,
        )

    def connect(self, socket: WebSocketRoute) -> None:
        if self.socket is not None and not self.allow_reconnect:
            self.errors.append("unexpected second WebSocket")
            socket.close()
            return
        self.connections += 1
        self.socket = socket
        socket.on_message(capture_callback(self.errors, self.receive))

    def receive(self, frame: str | bytes) -> None:
        request_id, path, body = request_frame(frame)
        if path == "/kymo.push/control":
            return
        method = path.rsplit("/", 1)[-1]
        self.counts[method] += 1
        if method == "PollVersions":
            request = pb.PollVersionsRequest.FromString(body)
            if request.project_id != PROJECT:
                raise AssertionError(f"unexpected poll scope: {request}")
            self.polls.append(request_id)
            return
        if method == "ListRuns":
            response = pb.ListRunsResponse(
                runs=[self.run(index) for index in range(self.runs)]
            )
            if self.list_version is not None:
                response.project_version = self.list_version
            response = response.SerializeToString()
        elif method == "GetRun":
            response = pb.GetRunResponse(
                run=pb.RunRecord(
                    run=self.run(0),
                    # Return Trash until the restore becomes visible.
                    state=(
                        pb.RUN_LIFECYCLE_STATE_ACTIVE
                        if self.restore_visible
                        else pb.RUN_LIFECYCLE_STATE_TRASHED
                    ),
                    deleted_at_ms=1_700_000_000_000,
                    purge_at_ms=1_700_600_000_000,
                ),
                server_now_ms=1_700_000_002_000,
            ).SerializeToString()
        elif method in ("ListMetrics", "ListRunSetMetrics"):
            response = pb.ListMetricsResponse().SerializeToString()
        elif method == "ListProjects":
            response = pb.ListProjectsResponse(
                project_ids=[PROJECT]
            ).SerializeToString()
        else:
            self.errors.append(f"unexpected RPC: {path}")
            response = b""
        self.pending.append((request_id, method, response))

    def respond(self, request_id: int, method: str, body: bytes) -> None:
        assert self.socket is not None
        self.socket.send(encode_response(request_id, body))
        self.responses[method] += 1
        self.response_bytes[method] += len(body)

    def dispatch(self) -> None:
        ready = []
        parked = []
        for item in self.pending:
            if self.hold_list_responses and item[1] == "ListRuns":
                parked.append(item)
            else:
                ready.append(item)
        self.pending = parked
        for request_id, method, body in ready:
            self.respond(request_id, method, body)

    def release_poll(self) -> None:
        assert len(self.polls) == 1, self.polls
        request_id = self.polls.pop()
        # Simulate a metadata commit after the first list snapshot and before
        # this poll, with the coalesced push deliberately withheld until later.
        self.observe()
        response = pb.PollVersionsResponse(
            project_version=self.scenario.polled,
            run_versions={f"run-{index}": 0 for index in range(self.runs)},
        )
        self.respond(request_id, "PollVersions", response.SerializeToString())

    def observe(self) -> None:
        self.published = self.scenario.mutation_after_snapshot
        self.restore_visible = True
        if self.published and self.list_version is not None:
            self.list_version = self.scenario.polled

    def push(
        self,
        version: int | None = None,
        *,
        global_version: int = 0,
    ) -> None:
        assert self.socket is not None
        event = pb.RunVersionsEvent(
            project_versions={} if version is None else {PROJECT: version},
            global_version=global_version,
            # Registry discovery is a positive witness that this same frame
            # was decoded, even though its equal project version is a no-op.
            metrics_changed_runs=[RUN],
        )
        self.socket.send(encode_response(0, event.SerializeToString()))


def drive(page: Page, peer: Peer, predicate, *, timeout: float = 15) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        peer.dispatch()
        assert not peer.errors, peer.errors
        if predicate():
            return
        page.wait_for_timeout(5)
    raise AssertionError(
        f"fixture timed out: counts={dict(peer.counts)}, pending={peer.pending}, "
        f"polls={peer.polls}, errors={peer.errors}"
    )


def settle(page: Page, peer: Peer) -> None:
    # A bounded quiet observation window detects duplicate requests. Ordering
    # of the race itself uses held poll/push frames, never elapsed-time guesses.
    quiet_since = time.monotonic()
    previous = dict(peer.counts)
    deadline = quiet_since + 5
    while time.monotonic() < deadline:
        peer.dispatch()
        assert not peer.errors, peer.errors
        page.wait_for_timeout(10)
        if dict(peer.counts) != previous or peer.pending:
            quiet_since = time.monotonic()
            previous = dict(peer.counts)
        elif time.monotonic() - quiet_since >= 0.15:
            assert not peer.errors, peer.errors
            return
    raise AssertionError(f"RPCs did not settle: {dict(peer.counts)}")


@contextmanager
def fixture_page(browser: Browser, origin: str, peer: Peer):
    parsed = urlsplit(origin)
    assert parsed.scheme == "http" and parsed.hostname == "127.0.0.1", origin
    page = browser.new_page(viewport={"width": 1_440, "height": 900})

    def route_request(route) -> None:
        if not route.request.url.startswith(origin + "/"):
            peer.errors.append(f"unexpected request: {route.request.url}")
            route.abort()
        elif urlsplit(route.request.url).path == "/alerts":
            route.fulfill(json=[])
        else:
            route.continue_()

    def route_socket(socket: WebSocketRoute) -> None:
        if socket.url.split("?", 1)[0] != origin.replace("http://", "ws://") + WS_PATH:
            peer.errors.append(f"unexpected socket: {socket.url}")
            socket.close()
            return
        peer.connect(socket)

    try:
        page.on("pageerror", lambda error: peer.errors.append(f"page: {error}"))
        page.route("**/*", capture_callback(peer.errors, route_request))
        page.route_web_socket("**/*", capture_callback(peer.errors, route_socket))
        yield page
    finally:
        page.close()


def witnessed_push(
    page: Page, peer: Peer, version: int | None = None, *, global_version: int = 0
) -> None:
    before_metrics = peer.counts["ListRunSetMetrics"]
    peer.push(version, global_version=global_version)
    drive(
        page,
        peer,
        lambda: peer.counts["ListRunSetMetrics"] > before_metrics and not peer.pending,
    )
    # Finish the browser's render turn after the same-frame registry witness.
    render_turn(page)
    drive(page, peer, lambda: not peer.pending)


def run_case(
    browser: Browser,
    origin: str,
    scenario: Scenario,
    *,
    direct: bool,
) -> dict:
    peer = Peer(scenario, MEASURED_RUNS)
    with fixture_page(browser, origin, peer) as page:
        page.goto(origin + f"/{PROJECT}" + (f"/{RUN}" if direct else ""))
        drive(
            page,
            peer,
            lambda: (
                bool(peer.polls)
                and (
                    page.locator('[aria-label="Run is in Trash"]').count() == 1
                    if direct
                    else page.locator(f'.sidebar-run[data-run-id="{RUN}"]').count() == 1
                )
            ),
        )
        settle(page, peer)
        assert peer.counts["ListRuns"] == 1, peer.counts
        direct_before = peer.counts["GetRun"]
        if direct:
            assert direct_before == 1, peer.counts
        if scenario.push_before_poll:
            peer.observe()
            witnessed_push(page, peer, scenario.polled)
            assert peer.counts["ListRuns"] == scenario.lists, peer.counts
            if direct:
                assert peer.counts["GetRun"] == 2, peer.counts
        peer.release_poll()

        def refreshed() -> bool:
            if peer.counts["ListRuns"] < scenario.lists or peer.pending:
                return False
            if direct and (
                peer.counts["GetRun"] <= direct_before
                or page.locator('[aria-label="Run is in Trash"]').count() != 0
            ):
                return False
            name = f"{'after' if scenario.mutation_after_snapshot else 'before'} snapshot 00000"
            if direct:
                return page.title() == name
            row = page.locator(f'.sidebar-run[data-run-id="{RUN}"]')
            return name in row.inner_text()

        drive(
            page,
            peer,
            refreshed,
        )
        # The following push positively witnesses processing after the poll;
        # equal project observations must not launch another list or point read.
        assert peer.counts["ListRuns"] == scenario.lists, peer.counts
        if direct:
            assert peer.counts["GetRun"] == 2, peer.counts
            assert page.locator('[aria-label="Run is in Trash"]').count() == 0
        before_push = dict(peer.counts)
        witnessed_push(page, peer, scenario.polled)
        for method in ("ListRuns", "GetRun", "PollVersions"):
            assert peer.counts[method] == before_push.get(method, 0), peer.counts
        assert not peer.errors, peer.errors
        return {
            "scenario": scenario.name,
            "route": "direct" if direct else "project",
            "runs": peer.runs,
            "rpc_counts": before_push,
            "list_response_bytes": peer.response_bytes["ListRuns"],
        }


def run_fences(browser: Browser, origin: str) -> None:
    results = [
        run_case(browser, origin, scenario, direct=direct)
        for scenario in SCENARIOS
        for direct in scenario.routes
    ]
    results.append(scope_restored_during_poll(browser, origin))
    results.append(warm_direct_and_reconnect(browser, origin))
    results.extend(
        steady_covered_push(browser, origin, direct=direct) for direct in (False, True)
    )
    results.append(empty_project(browser, origin))
    results.append(project_and_global_direct_push(browser, origin))
    print(json.dumps({"first_mount_fixture": results}, sort_keys=True), flush=True)


def steady_covered_push(browser: Browser, origin: str, *, direct: bool) -> dict:
    peer = Peer(Scenario("steady-covered-push", 11, 11, 1), 1)
    with fixture_page(browser, origin, peer) as page:
        page.goto(origin + f"/{PROJECT}" + (f"/{RUN}" if direct else ""))
        drive(page, peer, lambda: bool(peer.polls))
        peer.release_poll()
        drive(page, peer, lambda: not direct or peer.counts["GetRun"] == 2)
        witnessed_push(page, peer, 11)
        assert peer.counts["ListRuns"] == 1, peer.counts
        # The first push observes version 12, but its covering ListRuns can
        # already include version 13. The later push at 13 must reuse those rows.
        peer.list_version = 13
        witnessed_push(page, peer, 12)
        assert peer.counts["ListRuns"] == 2, peer.counts
        if direct:
            assert peer.counts["GetRun"] == 3, peer.counts
        witnessed_push(page, peer, 13)
        assert peer.counts["ListRuns"] == 2, peer.counts
        if direct:
            assert peer.counts["GetRun"] == 4, peer.counts
        return {
            "scenario": "steady-push-after-covered-list",
            "route": "direct" if direct else "project",
            "rpc_counts": dict(peer.counts),
        }


def empty_project(browser: Browser, origin: str) -> dict:
    peer = Peer(Scenario("empty-project", None, 0, 2), 0)
    with fixture_page(browser, origin, peer) as page:
        page.goto(origin + f"/{PROJECT}")
        drive(page, peer, lambda: bool(peer.polls))
        peer.release_poll()
        drive(page, peer, lambda: peer.counts["ListRuns"] == 2 and not peer.pending)
        settle(page, peer)
        assert peer.counts["ListRuns"] == 2, peer.counts
        assert page.locator(".sidebar-run").count() == 0
        assert peer.counts["GetRun"] == 0, peer.counts
        return {"scenario": "empty-project-no-token", "rpc_counts": dict(peer.counts)}


def project_and_global_direct_push(browser: Browser, origin: str) -> dict:
    peer = Peer(Scenario("project-and-global-direct-push", 11, 11, 2), 1)
    with fixture_page(browser, origin, peer) as page:
        page.goto(origin + f"/{PROJECT}/{RUN}")
        drive(
            page,
            peer,
            lambda: (
                bool(peer.polls)
                and page.locator('[aria-label="Run is in Trash"]').count() == 1
            ),
        )
        settle(page, peer)
        before = peer.counts["GetRun"]
        assert before == 1, peer.counts
        # Keep the initial poll held so its lifecycle refresh cannot interfere.
        # Trash must be loaded when the frame arrives: Active skips the global arm.
        assert page.locator('[aria-label="Run is in Trash"]').count() == 1
        witnessed_push(page, peer, 12, global_version=1)
        settle(page, peer)
        assert peer.counts["ListRuns"] == 2, peer.counts
        assert peer.counts["GetRun"] == before + 1, peer.counts
        assert page.locator('[aria-label="Run is in Trash"]').count() == 1
        return {
            "scenario": "project-and-global-direct-push",
            "get_run_before": before,
            "get_run_after": peer.counts["GetRun"],
            "rpc_counts": dict(peer.counts),
        }


def warm_direct_and_reconnect(browser: Browser, origin: str) -> dict:
    peer = Peer(Scenario("warm-direct", 11, 11, 1), 1)
    peer.allow_reconnect = True
    with fixture_page(browser, origin, peer) as page:
        page.goto(origin + f"/{PROJECT}")
        drive(page, peer, lambda: bool(peer.polls))
        peer.release_poll()
        witnessed_push(page, peer, 11)
        assert peer.counts["GetRun"] == 0, peer.counts
        page.locator(f'.sidebar-run[data-run-id="{RUN}"] .run-details-link').click()
        drive(page, peer, lambda: peer.counts["GetRun"] == 1 and not peer.pending)
        settle(page, peer)
        assert urlsplit(page.url).path == f"/{PROJECT}/{RUN}", page.url
        assert page.title() == "before snapshot 00000", page.title()
        assert peer.connections == 1
        assert peer.counts["ListRuns"] == 1, peer.counts
        assert peer.counts["GetRun"] == 1, peer.counts
        assert page.locator('[aria-label="Run is in Trash"]').count() == 0
        assert peer.socket is not None
        peer.hold_list_responses = True
        peer.socket.close(code=1012)
        drive(
            page,
            peer,
            lambda: (
                peer.connections == 2
                and peer.counts["GetRun"] == 2
                and peer.responses["GetRun"] == 2
                and any(item[1] == "ListRuns" for item in peer.pending)
            ),
        )
        # Reconnect GetRun completes while ListRuns is still held.
        # The cold-connection gate must not delay a warm direct lookup.
        peer.hold_list_responses = False
        drive(page, peer, lambda: bool(peer.polls) and not peer.pending)
        peer.release_poll()
        witnessed_push(page, peer, 11)
        assert peer.counts["GetRun"] == 2, peer.counts
        assert peer.counts["ListRuns"] == 2, peer.counts
        assert peer.counts["PollVersions"] == 2, peer.counts
        return {
            "scenario": "warm-direct-navigation-and-reconnect",
            "connections": peer.connections,
            "rpc_counts": dict(peer.counts),
        }


class ScopePeer(Peer):
    def __init__(self):
        super().__init__(Scenario("scope-restored-during-poll", 11, 11, 1), 1)
        self.scope_polls: Counter[str] = Counter()
        self.metadata_reads: Counter[str] = Counter()
        self.hold_next_poll = False
        self.held_poll: tuple[int, str] | None = None

    def receive(self, frame):
        request_id, path, body = request_frame(frame)
        method = path.rsplit("/", 1)[-1]
        if method == "PollVersions":
            request = pb.PollVersionsRequest.FromString(body)
            assert request.project_id in (PROJECT, "other"), request
            self.scope_polls[request.project_id] += 1
            self.counts[method] += 1
            if self.hold_next_poll:
                self.held_poll = (request_id, request.project_id)
                self.hold_next_poll = False
            else:
                self.respond(
                    request_id,
                    method,
                    pb.PollVersionsResponse(project_version=11).SerializeToString(),
                )
        elif method == "GetRun":
            request = pb.GetRunRequest.FromString(body)
            self.metadata_reads[request.project_id] += 1
            self.counts[method] += 1
            response = pb.GetRunResponse(
                run=pb.RunRecord(
                    run=pb.RunInfo(
                        project_id=request.project_id,
                        run_id=request.run_id,
                        run_name="external run",
                        status=pb.RUN_STATUS_FINISHED,
                    ),
                    state=pb.RUN_LIFECYCLE_STATE_ACTIVE,
                ),
                server_now_ms=1_700_000_002_000,
            )
            self.respond(request_id, method, response.SerializeToString())
        elif method == "QueryChart":
            self.respond(
                request_id,
                method,
                pb.ChartResponse().SerializeToString(),
            )
        else:
            super().receive(frame)


EXTERNAL_LAYOUT = {
    "format_version": 1,
    "user_sections": ["external"],
    "added_rects": [
        {
            "section": "external",
            "rect": {
                "id": "external-loss",
                "bindings": [
                    {
                        "project": {"Specific": "other"},
                        "runs": {"Specific": ["external-run"]},
                        "metric_name": "loss",
                    }
                ],
                "display_type": "Numeric",
            },
        }
    ],
}


def mount_scope(page: Page, peer: ScopePeer, origin: str) -> None:
    page.add_init_script(
        f"localStorage.setItem({json.dumps('kymo_layout_diff_' + PROJECT)}, {json.dumps(json.dumps(EXTERNAL_LAYOUT))})"
    )
    page.on("dialog", lambda dialog: dialog.accept())
    page.goto(origin + f"/{PROJECT}")
    drive(page, peer, lambda: peer.scope_polls["other"] >= 1)
    settle(page, peer)


def remove_external_scope(page: Page, peer: ScopePeer) -> dict:
    before = dict(peer.scope_polls)
    page.get_by_title("Delete section", exact=True).click()
    drive(page, peer, lambda: peer.scope_polls[PROJECT] > before[PROJECT])
    settle(page, peer)
    assert peer.scope_polls["other"] == before["other"], peer.scope_polls
    assert page.get_by_title("Delete section", exact=True).count() == 0
    return before


def scope_restored_during_poll(browser: Browser, origin: str) -> dict:
    peer = ScopePeer()
    with fixture_page(browser, origin, peer) as page:
        mount_scope(page, peer, origin)
        peer.hold_next_poll = True
        before = remove_external_scope(page, peer)
        assert peer.held_poll is not None
        # A completed with both projects. Hold B's current-only catch-up, then prune "other" with a witnessed project push. Restoring A must catch up even though B never completed.
        witnessed_push(page, peer, 11)
        before_other = peer.scope_polls["other"]
        before_reads = peer.metadata_reads["other"]
        page.evaluate(
            "([key, value]) => localStorage.setItem(key, JSON.stringify(value))",
            ["kymo_layout_diff_" + PROJECT, EXTERNAL_LAYOUT],
        )
        witnessed_push(page, peer)
        drive(page, peer, lambda: peer.scope_polls["other"] > before_other)
        drive(page, peer, lambda: peer.metadata_reads["other"] > before_reads)
        settle(page, peer)
        assert peer.held_poll is not None, "B's held response was released"
        return {
            "scenario": "scope-restored-before-shrink-poll-completes",
            "poll_scopes_before": before,
            "poll_scopes_after": dict(peer.scope_polls),
            "metadata_reads_before_restore": before_reads,
            "metadata_reads_after_restore": peer.metadata_reads["other"],
        }


@contextmanager
def serve_bundle(bundle: Path):
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(bundle), **kwargs)

        def do_GET(self):
            if urlsplit(self.path).path.startswith("/assets/"):
                return super().do_GET()
            encoded = json.dumps(
                {
                    "websocket_path": WS_PATH,
                    "cdn_origin": f"http://127.0.0.1:{self.server.server_port}",
                }
            )
            html = (
                (bundle / "index.html")
                .read_text()
                .replace(
                    "</head>",
                    f'<script id="kymo-runtime-config" type="application/json">{encoded}</script></head>',
                )
            )
            body = html.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bundle", type=Path, required=True, help="local-runtime public directory"
    )
    parser.add_argument(
        "--browser", choices=("chromium", "firefox", "webkit"), default="chromium"
    )
    args = parser.parse_args()
    with serve_bundle(args.bundle) as origin, sync_playwright() as playwright:
        browser = getattr(playwright, args.browser).launch()
        try:
            run_fences(browser, origin)
        finally:
            browser.close()


if __name__ == "__main__":
    main()
