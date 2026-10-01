"""End-to-end qualification of an installed kymo-local-runtime wheel built with the CI-only `test-idle-timeout` feature.

Run the phases in order against a fresh KYMO_LOCAL_ROOT, with the wheel and the Python client installed (and, for `browser`, Playwright with Chromium):

    python runtime_qualification.py install
    python runtime_qualification.py open-hold
    python runtime_qualification.py recovery
    python runtime_qualification.py integration
    python runtime_qualification.py seed
    python runtime_qualification.py browser

The internal Linux CI and the public repository's macOS release job both run these phases.
"""

import fcntl
import json
import os
import pathlib
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.parse
from urllib.request import urlopen

HERE = pathlib.Path(__file__).resolve().parent
# The kymo tree: the repository root in the public mirror.
KYMO = HERE.parents[1]
ROOT = pathlib.Path(os.environ["KYMO_LOCAL_ROOT"])
MANIFEST = ROOT / "state/runtime.json"
SUPERVISOR_LOG = ROOT / "state/supervisor.log"
# Logged when an idle stop begins, well before runtime.json shows the stop.
IDLE_STOP_MARKER = "local stack reached idle shutdown after "


def manifest():
    return json.loads(MANIFEST.read_text())


def install():
    subprocess.check_call(["kymo", "--version"])
    assert shutil.which("kymo-server"), "kymo-server is not on PATH"
    subprocess.check_call(["kymo", "status", "--json"])
    assert not ROOT.exists(), f"{ROOT} must start fresh"
    subprocess.check_call(["kymo", "install"])
    first = manifest()
    subprocess.check_call(["kymo", "install"])
    second = manifest()
    assert (first["dashboard_port"], first["cdn_port"]) == (
        second["dashboard_port"],
        second["cdn_port"],
    )
    print("compatible reinstall preserved browser ports")
    subprocess.check_call(["kymo", "status", "--json"])
    subprocess.check_call(["kymo", "doctor", "--json"])


def open_hold():
    idle_timeout_ms = 250
    os.environ["KYMO_LOCAL_TEST_IDLE_TIMEOUT_MS"] = str(idle_timeout_ms)
    subprocess.check_call(
        [
            "kymo",
            "open",
            "--expected-installation-uuid",
            manifest()["installation_uuid"],
            "--no-browser",
            "--",
            "browser-e2e",
            "browser-e2e",
        ]
    )
    time.sleep(14)
    # This is the first stack in its KYMO_LOCAL_ROOT, so the whole log is its own.
    log = SUPERVISOR_LOG.read_text()
    # Without the short test idle timeout nothing would stop the stack within the sleep, and the check below would prove nothing.
    assert f"using CI-only local idle timeout of {idle_timeout_ms}ms" in log, log
    # With uninterrupted polling, 14 s outlasts the ten-second ensure hold, the one-second poll interval and a poll's two-second timeout by a second, so only the open hold can have kept an idle stop from starting.
    assert IDLE_STOP_MARKER not in log, "the open hold did not keep the stack up"
    assert manifest()["running"] is not None
    subprocess.check_call(["kymo", "stop"])
    print("the open hold kept the stack up")


def recovery():
    before = MANIFEST.read_bytes()
    with socket.socket() as conflict:
        conflict.bind(("127.0.0.1", json.loads(before)["dashboard_port"]))
        conflict.listen()
        result = subprocess.run(
            ["kymo", "ensure", "--json"], capture_output=True, text=True
        )
    assert result.returncode != 0, result.stdout
    assert "pinned dashboard port" in result.stderr, result.stderr
    assert MANIFEST.read_bytes() == before, "port conflict mutated runtime.json"
    print("pinned browser-port conflict failed without manifest drift")

    first = json.loads(subprocess.check_output(["kymo", "ensure", "--json"]))
    second = json.loads(subprocess.check_output(["kymo", "ensure", "--json"]))
    assert first["endpoint_generation"] == second["endpoint_generation"]
    assert len(first["server_bearer"]) == 43
    print("ensure reused one healthy endpoint generation")

    running = manifest()["running"]
    os.kill(running["supervisor"]["pid"], signal.SIGKILL)

    lock_released = False
    with (ROOT / "state/runtime.lock").open("r+") as lock:
        deadline = time.monotonic() + 10
        while not lock_released and time.monotonic() < deadline:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                lock_released = True
            except BlockingIOError:
                time.sleep(0.05)

    groups = [running[name]["pid"] for name in ("postgresql", "clickhouse", "server")]
    for group in groups:
        try:
            os.killpg(group, signal.SIGKILL)
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + 10
    while groups and time.monotonic() < deadline:
        groups = [group for group in groups if alive(group, process_group=True)]
        time.sleep(0.05)
    assert lock_released, "children inherited the runtime lock"
    assert not groups, groups
    assert (ROOT / "data/postgresql/postmaster.pid").exists()

    recovered = json.loads(subprocess.check_output(["kymo", "ensure", "--json"]))
    assert recovered["endpoint_generation"] != second["endpoint_generation"]
    assert recovered["dashboard_origin"] == second["dashboard_origin"]
    assert recovered["cdn_origin"] == second["cdn_origin"]
    index = urlopen(recovered["dashboard_origin"] + "/", timeout=5).read()
    assert b'id="kymo-runtime-config"' in index
    assert b"/trash/_kymo-grpc-ws-local-v1" in index
    assert recovered["cdn_origin"].encode() in index
    run_url = subprocess.check_output(
        [
            "kymo",
            "open",
            "project/one",
            "run two",
            "--expected-installation-uuid",
            recovered["installation_uuid"],
            "--no-browser",
        ],
        text=True,
    ).strip()
    assert run_url == recovered["dashboard_origin"] + "/project%2Fone/run%20two"
    print("ensure recovered a crash-stale quiescent stack")
    subprocess.check_call(["kymo", "status", "--json"])
    subprocess.check_call(["kymo", "doctor", "--json"])


def integration():
    # Streamed as it runs, so a hung test still leaves its log.
    output = []
    with subprocess.Popen(
        [sys.executable, "test_local_integration.py", "-v"],
        cwd=KYMO / "python_client",
        env={**os.environ, "KYMO_LIVE_LOCAL_TESTS": "1"},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    ) as test:
        for line in test.stdout:
            print(line, end="")
            output.append(line)
    assert test.returncode == 0, "live local-client integration failed"
    # gRPC logs from fork_posix.cc when a process forks with gRPC threads running, which the client must never do.
    assert "fork_posix.cc" not in "".join(output), "the client forked with gRPC running"


def seed():
    # Seeding must not run with a short idle timeout: between one seeded run's finish() and the next run's init hold nothing keeps the stack alive, so the stack could stop there and leave the next run cold-starting PostgreSQL and ClickHouse against a shutdown in progress.
    for fixture in ("seed_browser_fixture.py", "seed_zoom_fixture.py"):
        subprocess.check_call([sys.executable, HERE / fixture])
    # The browser phase must start its own stack, with its own idle timeout.
    subprocess.check_call(["kymo", "stop"])


def alive(pid, *, process_group):
    try:
        (os.killpg if process_group else os.kill)(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # macOS reports EPERM for a group whose members are still being torn down; for a single PID it means another user's process reused the number.
        return process_group
    return True


def offline_browser():
    from playwright.sync_api import sync_playwright

    import gesture_fences
    import legacy_storage_fences
    import user_settings_fences
    import zoom_fences

    idle_timeout_ms = 3000
    os.environ["KYMO_LOCAL_TEST_IDLE_TIMEOUT_MS"] = str(idle_timeout_ms)
    requests = []
    web_sockets = []
    # WebSockets that have exchanged a frame.
    frame_proven = set()
    fence_errors = []

    def route_request(route):
        parsed = urllib.parse.urlsplit(route.request.url)
        if (
            parsed.scheme == "http"
            and parsed.hostname == "127.0.0.1"
            and parsed.port in allowed_ports
        ):
            route.continue_()
        else:
            route.abort()

    def record_web_socket(web_socket):
        web_sockets.append(web_socket)
        web_socket.on("framesent", lambda _: frame_proven.add(web_socket))
        web_socket.on("framereceived", lambda _: frame_proven.add(web_socket))

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        # Start the stack only now that the browser is ready: the ensure's ten-second hold, not the short idle timeout, covers the first page from its shell until its WebSocket connects.
        endpoints = json.loads(subprocess.check_output(["kymo", "ensure", "--json"]))
        ensure_hold_ends = time.monotonic() + 10
        running = manifest()["running"]
        assert running is not None
        identities = [
            (running["supervisor"]["pid"], False),
            (running["postgresql"]["pid"], True),
            (running["clickhouse"]["pid"], True),
            (running["server"]["pid"], True),
        ]
        allowed_ports = {
            urllib.parse.urlsplit(endpoints["dashboard_origin"]).port,
            urllib.parse.urlsplit(endpoints["cdn_origin"]).port,
        }
        page.route("**/*", route_request)
        page.on("request", lambda request: requests.append(request.url))
        page.on("websocket", record_web_socket)
        page.goto(
            endpoints["dashboard_origin"] + "/browser-e2e/browser-e2e",
            wait_until="domcontentloaded",
        )
        assert "kymo" in page.title().lower()
        page.wait_for_selector(".metric-rect canvas", timeout=20_000)
        image = page.wait_for_selector(".cdn-image-item img", timeout=20_000)
        page.wait_for_function(
            "image => image.complete && image.naturalWidth > 0",
            arg=image,
            timeout=20_000,
        )
        deadline = time.monotonic() + 10
        while not frame_proven and time.monotonic() < deadline:
            page.wait_for_timeout(100)
        assert frame_proven, "local frontend WebSocket exchanged no frames"

        # Isolated fence pages preserve this primary frontend socket. Failures are deferred until the network, idle-stop and process-leak audits finish.
        def run_fence_page(path, runner, success, *, viewport=None):
            page = browser.new_page(viewport=viewport, device_scale_factor=1)
            page.route("**/*", route_request)
            page.on("request", lambda request: requests.append(request.url))
            page.on("websocket", record_web_socket)
            try:
                page.goto(
                    endpoints["dashboard_origin"] + path, wait_until="domcontentloaded"
                )
                runner(page)
                print(success)
            except Exception as error:
                fence_errors.append(error)
            finally:
                page.close()

        run_fence_page(
            "/browser-e2e",
            gesture_fences.run_fences,
            "sidebar drag-paint gesture fences passed",
        )
        run_fence_page(
            "/",
            lambda page: user_settings_fences.run_fences(
                page, dashboard_path="/browser-e2e"
            ),
            "user settings fences passed",
            viewport=user_settings_fences.VIEWPORT,
        )
        run_fence_page(
            "/",
            lambda page: legacy_storage_fences.run_fences(
                page, endpoints["dashboard_origin"]
            ),
            "legacy storage fences passed",
            viewport=user_settings_fences.VIEWPORT,
        )
        run_fence_page(
            zoom_fences.RUN_PATH,
            zoom_fences.run_fences,
            "chart zoom gesture fences passed",
            viewport=zoom_fences.VIEWPORT,
        )

        # The last fence page just closed. Wait out the ensure hold and one idle timeout from that close, plus a poll, its two-second timeout and a second: then only the primary page's socket can be holding the stack, and an idle stop that began would have logged its line. A merely attempted upgrade cannot satisfy the frame-proven socket assertion.
        page.wait_for_timeout(
            (max(ensure_hold_ends - time.monotonic(), idle_timeout_ms / 1000) + 4)
            * 1000
        )
        assert any(not web_socket.is_closed() for web_socket in frame_proven), (
            "no frame-proven WebSocket survived the idle timeout"
        )
        assert IDLE_STOP_MARKER not in SUPERVISOR_LOG.read_text(), (
            "the stack began an idle stop while a page was open"
        )
        assert manifest()["running"] is not None

        # All pages are covered: requests and web_sockets accumulate across them, and WebSockets never pass through the request router, so they are only audited here. Requests the router aborted are audited too.
        assert any(
            url.startswith(endpoints["cdn_origin"] + "/cdn/") for url in requests
        ), requests
        for url in requests + [web_socket.url for web_socket in web_sockets]:
            parsed = urllib.parse.urlsplit(url)
            assert parsed.scheme in {"http", "ws"}, url
            assert parsed.hostname == "127.0.0.1", url
            assert parsed.port in allowed_ports, url
        closed_at = time.monotonic()
        browser.close()

    # Closing the browser restarts the idle clock, so the idle stop begins one idle timeout later. The logged idle age can only trail the measured close-to-stop time (the server sees the close, and this loop the log line, a moment late); a clock the close did not reset reads at least the whole wait above longer.
    deadline = time.monotonic() + 30
    while IDLE_STOP_MARKER not in (log := SUPERVISOR_LOG.read_text()):
        assert time.monotonic() < deadline, (
            "local stack did not begin its automatic idle stop"
        )
        time.sleep(0.1)
    stop_began_after = time.monotonic() - closed_at
    idle_ms = int(log.rsplit(IDLE_STOP_MARKER, 1)[1].split("ms", 1)[0])
    assert idle_timeout_ms <= idle_ms <= stop_began_after * 1000, (
        f"idle clock was {idle_ms}ms old when the stop began {stop_began_after:.1f}s after the close; expected the {idle_timeout_ms}ms idle timeout, restarted by the close"
    )
    print(
        f"idle stop began {stop_began_after:.1f}s after the close, with the idle clock at {idle_ms}ms"
    )

    deadline = time.monotonic() + 30
    while manifest()["running"] is not None:
        assert time.monotonic() < deadline, (
            "local stack did not publish its automatic idle stop"
        )
        time.sleep(0.1)

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and any(
        alive(pid, process_group=process_group) for pid, process_group in identities
    ):
        time.sleep(0.1)
    leaked = [
        pid
        for pid, process_group in identities
        if alive(pid, process_group=process_group)
    ]
    assert not leaked, f"idle shutdown leaked process identities: {leaked}"
    print(
        "offline browser stayed local and frontend-aware idle shutdown stopped the stack"
    )
    if fence_errors:
        raise fence_errors[0]


PHASES = {
    "install": install,
    "open-hold": open_hold,
    "recovery": recovery,
    "integration": integration,
    "seed": seed,
    "browser": offline_browser,
}

if __name__ == "__main__":
    PHASES[sys.argv[1]]()
