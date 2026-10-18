"""Synthetic (no-GPU, no-real-server) tests for bounded shutdown + spool + replay.

Covers the failure that motivated them: a 2-GPU DDP trainer reached its final
step, printed `kymo: timeout — 3094606 points still queued`, and then hung
forever — the terminated upload worker left the metric queue's feeder thread
blocked on a full pipe, and Queue._finalize_join joined it at interpreter exit.

Scenarios (each runs the client in a fresh subprocess against an in-process
fake kymo server):
  1. fast server        → everything delivered, no spool, prompt exit
  2. slow server        → finish(flush_timeout) returns within budget, exits
                          promptly, every logged key is delivered or spooled
  3. replay             → python -m kymo.sync delivers the spooled points
                          (timestamps preserved), file renamed .sent
  4. SIGTERM mid-run    → bounded exit, remainder spooled
  5. hung server        → accepts streams but never acks: in-flight send is
                          cancelled at the deadline, everything spools
                          (images pre-encoded as PNG)
  6. large pipelined    → more than one client window cycles without loss
  7. stream break       → unacked suffix is retained and re-fed on a new attempt
  8. no-ack watchdog    → a live stream with an unchanging ACK frontier is
                          cancelled/retried before the shutdown deadline
  9. malformed ack      → an impossible cumulative ACK is rejected; the
                          outcome-unknown cut is re-fed without loss
 10. blackholed attempt → the watchdog cancels the first stream and recovers
                          on a fresh stream without waiting for shutdown
 11. RAM-cap catch-up   → a real gRPC worker seals and replays its disk prefix,
                          then sends a newer suffix live and exits cleanly
 12. finish mid-replay  → shutdown during the last replay still reports the
                          delivered points as delivered
 13. rotated segments   → the owner's shutdown report covers spool segments the
                          worker rotated to and named itself
 14-17. Ctrl-C to the   → during training (uncaught; under spawn too), a slow finish() or the
        process group     atexit flush, and twice: the run ends (TerminateRun
                          130) and the console returns at once; the worker,
                          which carries on, is gone within the 10 s
 18. group SIGTERM      → the worker survives it; every point is delivered or
                          spooled within the signal budget, exit 143
 19. SIGTERM in finish  → bounds the one shutdown by the signal budget
 20. ignored SIGTERM    → a program that ignores SIGTERM carries on from a
                          group SIGTERM, and so does its worker
 21. Ctrl-C mid-group   → a group past a pipe's 64 KiB is being written as the
                          program exits: the worker, which never gets the
                          rest, spools what it received and exits by itself
 22. Ctrl-C, caught     → a program that catches the Ctrl-C and exits with
                          its own status: the run ends at once, handed off
 23. Ctrl-C in the      → the Ctrl-C raises from log()'s wait for a slow
     encoder wait         encoder and hands the run off at once; at a short
                          budget the worker cuts the encode in flight,
                          reports the galleries lost and exits by itself
 24. tracker's mask    → spawn's resource tracker keeps the group signals it
                          does not ignore blocked; the worker blocks none
 25. helper's finish   → a finish() on another thread, or on a
                          ThreadPoolExecutor's worker, as the program exits
                          with sys.exit(7), which joins it before atexit:
                          the run ends with 7
 26. caught sys.exit   → a program that catches a sys.exit(7) and carries
                          on to finish(): the run ends with 0
 27. atexit finish()   → finish() registered with atexit, then an uncaught
                          sys.exit(7): the run ends with 7
 28. caught, SIGTERM   → a caught sys.exit(7), then a SIGTERM: the run ends
                          with 143, as the process does
 29. SIGTERM at exit   → a SIGTERM during concurrent.futures' join of a running
                          job, after an uncaught sys.exit(7): the run ends with 7
 30. pre-init handler  → a SIGTERM handler installed before init() that calls
     at the exit          sys.exit(1), or raises KeyboardInterrupt, its SIGTERM
                          sent from a threading atexit hook after a normal end,
                          where Python ignores what it raises: the run ends
                          with 143, as kymo's own handling would end it

Run:  python3 test_graceful_shutdown.py
"""

import contextlib
import hashlib
import http.server
import json
import multiprocessing
import os
import pickle
import re
import selectors
import signal
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
import unittest
import uuid
from concurrent import futures

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import grpc
from kymo._generated import kymo_pb2, kymo_pb2_grpc

CLIENT_DIR = os.path.dirname(os.path.abspath(__file__))
FAKE_FLUSH_POINTS = 10_000


# ---------------------------------------------------------------------------
# Fake server
# ---------------------------------------------------------------------------


def recorded(p):
    """A point as the fake server records it: (metric_name, step, payload_kind, tag, timestamp_ms)."""
    return (p.metric_name, p.step, p.WhichOneof("payload"), p.tag, p.timestamp_ms)


class FakeKymo(kymo_pb2_grpc.KymoServicer):
    """Counts everything. `call_delay` + `per_point_delay` model an overloaded
    server (fixed per-flush cost + throughput bound). Delays use an Event so
    stop() releases in-flight handlers instead of leaving non-daemon executor
    threads sleeping."""

    def __init__(
        self,
        call_delay: float = 0.0,
        per_point_delay: float = 0.0,
        break_after: int = 0,
        bad_ack_after: int = 0,
        blackhole_first: bool = False,
    ):
        self.call_delay = call_delay
        self.per_point_delay = per_point_delay
        # >0: on the FIRST bidi stream, commit the first flush whose cumulative
        # watermark reaches this value and then abort WITHOUT its ACK. This
        # exercises reconnect + re-feed of an outcome-unknown prefix. Later
        # streams ack normally.
        self.break_after = break_after
        # >0: on the FIRST bidi stream, commit the first cut whose cumulative
        # watermark reaches this value, then send an impossible ACK instead of
        # its real watermark. The client must retain and re-feed that
        # outcome-unknown cut. Later streams ack normally.
        self.bad_ack_after = bad_ack_after
        # The first stream reads requests but neither commits nor ACKs them, and
        # stays open even after request EOF. The client's ACK-progress watchdog
        # must cancel it; later streams behave normally.
        self.blackhole_first = blackhole_first
        self.stop_event = threading.Event()
        # Set as the first point arrives, before any commit or ACK: the client's upload worker has started and read a group.
        self.first_point = threading.Event()
        self.lock = threading.Lock()
        self.points = []  # recorded(p) of each committed point
        self.ingest_calls = 0
        self.bidi_streams = 0
        self.terminates = []
        self.terminated_at = []  # time.monotonic() of each TerminateRun

    def InitRun(self, request, context):
        return kymo_pb2.InitRunResponse(
            run=kymo_pb2.RunInfo(
                project_id=request.project_id,
                run_id=request.run_id,
                run_name=request.run_name,
                ordinal=1,
            )
        )

    def TerminateRun(self, request, context):
        with self.lock:
            self.terminates.append((request.run_id, request.exit_code))
            self.terminated_at.append(time.monotonic())
        return kymo_pb2.TerminateRunResponse()

    def QueryCdnKeys(self, request, context):
        # The replay of a legacy rich record reads its step's key (kymo.sync._current_cdn_key), as for the metadata record a deadline spill can leave in a segment of its own: none is stored here, so every step reads empty.
        return kymo_pb2.QueryCdnKeysResponse()

    def IngestMetrics(self, request_iterator, context):
        n = 0
        recv = []
        for batch in request_iterator:
            for p in batch.points:
                recv.append(recorded(p))
                n += 1
        delay = self.call_delay + n * self.per_point_delay
        if delay:
            self.stop_event.wait(delay)
        if context.is_active():
            with self.lock:
                self.points.extend(recv)
                self.ingest_calls += 1
        return kymo_pb2.IngestResponse(points_received=n)

    def IngestMetricsBidi(self, request_iterator, context):
        """Pipelined path with real-size cuts and cumulative position ACKs.

        The fake combines client messages into 10k cuts and final-flushes a tail
        on request EOF. It deliberately omits the real server's 2s live-stream
        timer: these shutdown scenarios should send EOF once all positions are
        fed, and relying on the timer would hide a client half-close deadlock.
        """
        consumed = 0
        pending = []
        with self.lock:
            self.bidi_streams += 1
            my_stream = self.bidi_streams

        if self.blackhole_first and my_stream == 1:
            try:
                for _ in request_iterator:
                    if not context.is_active():
                        return
            except Exception:
                return
            # Request EOF alone must not rescue this attempt: it models a server
            # that accepted the stream but whose durability frontier is stuck.
            while context.is_active() and not self.stop_event.wait(0.05):
                pass
            return

        def ack_or_inject_fault(cut):
            delay = self.call_delay + len(cut) * self.per_point_delay
            if delay:
                self.stop_event.wait(delay)
            if not context.is_active():
                return "stop", None
            with self.lock:
                self.points.extend(cut)
                self.ingest_calls += 1
            if self.break_after and my_stream == 1 and consumed >= self.break_after:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("injected post-commit/pre-ACK stream break")
                return "stop", None
            if self.bad_ack_after and my_stream == 1 and consumed >= self.bad_ack_after:
                return (
                    "terminal_ack",
                    kymo_pb2.IngestAck(points_acked=consumed + 1_000_000),
                )
            return "ack", kymo_pb2.IngestAck(points_acked=consumed)

        try:
            for batch in request_iterator:
                for p in batch.points:
                    consumed += 1
                    self.first_point.set()
                    pending.append(recorded(p))
                    if len(pending) == FAKE_FLUSH_POINTS:
                        cut, pending = pending, []
                        outcome, ack = ack_or_inject_fault(cut)
                        if outcome == "stop":
                            return
                        yield ack
                        if outcome == "terminal_ack":
                            return
            if pending:
                outcome, ack = ack_or_inject_fault(pending)
                if outcome != "stop":
                    yield ack
        except Exception:
            # Cancellation can make the server-side request iterator raise. Do
            # not hide a real fake-server bug behind a clean end-of-stream.
            if context.is_active():
                raise

    def train_point_count(self):
        with self.lock:
            return sum(1 for p in self.points if p[0].startswith("train/"))

    def train_point_keys(self):
        with self.lock:
            return {
                (p[0], p[1], p[3]) for p in self.points if p[0].startswith("train/")
            }

    @property
    def point_count(self):
        with self.lock:
            return len(self.points)


class FakeKymoProtocolTests(unittest.TestCase):
    class Context:
        def __init__(self):
            self.code = None
            self.details = None

        def is_active(self):
            return True

        def set_code(self, code):
            self.code = code

        def set_details(self, details):
            self.details = details

    @staticmethod
    def batch(n):
        point = kymo_pb2.MetricPoint(metric_name="train/x", step=1, value=1.0)
        return kymo_pb2.MetricsBatch(points=[point] * n)

    def test_tail_is_committed_and_cumulatively_acked_at_request_eof(self):
        servicer = FakeKymo()
        context = self.Context()

        acks = list(servicer.IngestMetricsBidi([self.batch(5_000)], context))

        self.assertEqual([ack.points_acked for ack in acks], [5_000])
        self.assertEqual(servicer.point_count, 5_000)
        self.assertEqual(servicer.ingest_calls, 1)

    def test_injected_break_commits_a_cut_without_acking_it(self):
        servicer = FakeKymo(break_after=12_000)
        context = self.Context()
        batch = self.batch(5_000)

        acks = list(servicer.IngestMetricsBidi([batch] * 4, context))

        self.assertEqual([ack.points_acked for ack in acks], [10_000])
        self.assertEqual(servicer.point_count, 20_000)
        self.assertEqual(servicer.ingest_calls, 2)
        self.assertEqual(context.code, grpc.StatusCode.UNAVAILABLE)
        self.assertIn("pre-ACK", context.details)

    def test_malformed_ack_replaces_the_committed_cuts_real_ack(self):
        servicer = FakeKymo(bad_ack_after=12_000)
        context = self.Context()
        batch = self.batch(5_000)

        acks = list(servicer.IngestMetricsBidi([batch] * 4, context))

        self.assertEqual(
            [ack.points_acked for ack in acks],
            [10_000, 1_020_000],
        )
        self.assertEqual(servicer.point_count, 20_000)
        self.assertEqual(servicer.ingest_calls, 2)


class _CdnHandler(http.server.BaseHTTPRequestHandler):
    store = {}
    lock = threading.Lock()

    def do_POST(self):
        data = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        extension = self.headers.get("X-Extension", "bin").lower()
        rid = f"{hashlib.sha256(data).hexdigest()}.{extension}"
        with self.lock:
            self.store[rid] = data
        body = json.dumps({"resource_id": rid}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


# The stop() of every fake server still running, so main() can stop them after a failed scenario.
_RUNNING_SERVERS: list = []


def start_fake_server(
    call_delay: float = 0.0,
    per_point_delay: float = 0.0,
    break_after: int = 0,
    bad_ack_after: int = 0,
    blackhole_first: bool = False,
):
    servicer = FakeKymo(
        call_delay=call_delay,
        per_point_delay=per_point_delay,
        break_after=break_after,
        bad_ack_after=bad_ack_after,
        blackhole_first=blackhole_first,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    kymo_pb2_grpc.add_KymoServicer_to_server(servicer, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    cdn = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _CdnHandler)
    threading.Thread(target=cdn.serve_forever, daemon=True).start()

    def stop():
        if stop not in _RUNNING_SERVERS:
            return
        _RUNNING_SERVERS.remove(stop)
        stopped = server.stop(grace=0)
        servicer.stop_event.set()
        stopped.wait(timeout=5)
        cdn.shutdown()
        cdn.server_close()

    _RUNNING_SERVERS.append(stop)

    return (
        servicer,
        f"127.0.0.1:{port}",
        f"http://127.0.0.1:{cdn.server_address[1]}",
        stop,
    )


# ---------------------------------------------------------------------------
# Client-side driver (runs in a subprocess so we can observe process exit)
# ---------------------------------------------------------------------------

DRIVER = textwrap.dedent("""
    import json, os, sys, time
    sys.path.insert(0, {client_dir!r})
    import numpy as np
    import kymo

    cfg = json.loads(sys.argv[1])
    if cfg.get("start_method"):
        import multiprocessing
        multiprocessing.set_start_method(cfg["start_method"], force=True)
    if cfg.get("ignore_sigterm"):
        import signal
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if cfg.get("sigterm_exits"):
        import signal
        signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(cfg["sigterm_exits"]))
    if cfg.get("sigterm_interrupts"):
        import signal
        signal.signal(signal.SIGTERM, signal.default_int_handler)  # SIGTERM as a Ctrl-C
    kymo.init(
        server_address=cfg["grpc"], cdn_address=cfg["cdn"],
        project_id="shutdown-test", run_name=cfg["run_name"],
        run_id=cfg["run_id"], system_metrics=False,
        spool_dir=cfg.get("spool_dir"),
    )
    if cfg.get("print_worker_pid"):
        import kymo.client
        print("WORKER_PID", kymo.client._upload_process.pid, flush=True)
    for step in range(cfg["steps"]):
        kymo.log({{f"train/m{{i}}": float(step * 100 + i) for i in range(cfg["metrics"])}}, step=step)
        if cfg.get("step_sleep"):
            time.sleep(cfg["step_sleep"])
    if cfg.get("recovery_wait"):
        time.sleep(cfg["recovery_wait"])
    if cfg.get("tail_metrics"):
        kymo.log(
            {{f"train/tail{{i}}": float(i) for i in range(cfg["tail_metrics"])}},
            step=cfg["steps"],
        )
    if cfg.get("log_image"):
        img = (np.arange(64*64*3, dtype=np.uint8) % 255).reshape(64, 64, 3)
        kymo.log({{"demo/img": kymo.Image(img, caption="spooled image")}}, step=0)
    if cfg.get("log_galleries"):
        # Galleries slow to encode (WebP, about 0.4 s per 4 MP image): from the third call on, log() waits for the encoder.
        ramp = (np.add.outer(np.arange(2048), np.arange(2048)) % 256).astype(np.uint8)
        gallery = [kymo.Image(np.stack([ramp] * 3, axis=-1), format="webp")] * cfg.get("gallery_images", 4)
        for step in range(cfg["log_galleries"]):
            if step == 2:
                print("DRIVER_AWAITING_ENCODER", flush=True)
            kymo.log({{"demo/gallery": gallery}}, step=step)
    print("DRIVER_LOGGED", flush=True)
    if cfg.get("log_until_interrupted"):
        metrics = {{f"train/m{{i}}": float(i) for i in range(cfg["metrics"])}}
        step = cfg["steps"]
        try:
            while True:
                kymo.log(metrics, step=step)
                step += 1
                if cfg.get("step_sleep"):
                    time.sleep(cfg["step_sleep"])
        finally:
            print(f"DRIVER_STEPS {{step}}", flush=True)
    if cfg.get("exit_on_interrupt"):
        try:
            time.sleep(3600)  # wait to be signalled
        except KeyboardInterrupt:
            sys.exit(0)  # a Ctrl-C the program catches and exits on, as click and Lightning do
    if cfg.get("hang_after_log"):
        time.sleep(3600)  # wait to be signalled
    if cfg.get("exit_with_helper_finish"):
        # Another thread's finish() once the program has called sys.exit: the interpreter's exit joins that thread, or concurrent.futures' exit hook joins an executor's worker, before the atexit flush.
        import threading
        import kymo.client
        def finish_after_the_exit():
            while kymo.client._exit_code is None:
                time.sleep(0.001)
            kymo.finish()
        if cfg.get("helper_executor"):
            import concurrent.futures
            concurrent.futures.ThreadPoolExecutor(max_workers=1).submit(finish_after_the_exit)
        else:
            threading.Thread(target=finish_after_the_exit).start()
        sys.exit(cfg["exit_with_helper_finish"])
    if cfg.get("atexit_finish"):
        import atexit
        atexit.register(kymo.finish)
        sys.exit(cfg["atexit_finish"])
    if cfg.get("executor_sigterm"):
        # A job still running as the program exits: concurrent.futures' threading atexit hook joins it, the main thread still alive, as a SIGTERM lands.
        import concurrent.futures, signal, threading
        def job():
            while not getattr(threading, "_SHUTTING_DOWN", False):
                time.sleep(0.001)
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(3)
        concurrent.futures.ThreadPoolExecutor(max_workers=1).submit(job)
        sys.exit(cfg["executor_sigterm"])
    if cfg.get("sigterm_at_threading_exit"):
        # Once the interpreter's exit has begun, which ignores the handler's exit.
        import signal, threading
        threading._register_atexit(os.kill, os.getpid(), signal.SIGTERM)
        raise SystemExit  # ends as a normal end does: no code, status 0
    if cfg.get("caught_exit"):
        try:
            sys.exit(cfg["caught_exit"])
        except SystemExit:
            pass  # caught, as argparse's or a notebook cell's is: the program carries on
    if cfg.get("sigterm_after"):
        import signal
        os.kill(os.getpid(), signal.SIGTERM)
        time.sleep(60)  # kymo's handler ends the run and exits
    if cfg.get("skip_finish"):
        sys.exit()  # the atexit flush ends the run
    print("DRIVER_FINISHING", flush=True)
    t0 = time.monotonic()
    fully = kymo.finish(flush_timeout=cfg["flush_timeout"])
    print(f"DRIVER_FINISH fully_delivered={{fully}} took={{time.monotonic()-t0:.1f}}s", flush=True)
""")

# subprocess.Popen only selects posix_spawn on POSIX when close_fds is false
# and start_new_session is false. The fake server owns gRPC threads, so forking
# this test process to launch a driver is itself unsafe and can hide or recreate
# the shutdown hang under test. Python-created descriptors are non-inheritable
# by default. This clean exec child creates the process group before replacing
# itself with the driver, keeping whole-group timeout cleanup without a fork.
SETSID_EXEC = """
import os
import signal
import sys

os.setsid()
# A shell's background job starts with SIGINT ignored, which the driver would inherit, and a program whose SIGINT is ignored at init() gets no Ctrl-C handling from kymo.
signal.signal(signal.SIGINT, signal.SIG_DFL)
os.execv(sys.executable, [sys.executable, *sys.argv[1:]])
"""


def driver_command(cfg: dict):
    return [
        sys.executable,
        "-c",
        SETSID_EXEC,
        "-c",
        DRIVER.format(client_dir=CLIENT_DIR),
        json.dumps(cfg),
    ]


def kill_driver_group(proc):
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    # macOS refuses (EPERM) a group whose only process has exited unreaped.
    except (ProcessLookupError, PermissionError):
        proc.kill()
    return proc.communicate(timeout=5)


def assert_no_grpc_fork_warning(output):
    assert "fork_posix.cc" not in output, (
        "trainer initialized gRPC before forking its upload worker\n" + output
    )


def load_factor() -> float:
    """The machine's 1-minute load average per CPU, at least 1. The scenarios' wall-time bounds that load stretches scale by it, so they stay as tight as written on an idle machine, as in CI, and a loaded one does not fail them."""
    try:
        return max(1.0, os.getloadavg()[0] / (os.cpu_count() or 1))
    except OSError:
        return 1.0


def run_driver(cfg: dict, timeout: float):
    """Runs the driver subprocess. Returns (returncode, wall_time, output)."""
    t0 = time.monotonic()
    proc = subprocess.Popen(
        driver_command(cfg),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env={**os.environ, "KYMO_VERBOSE": "1", **cfg.get("env", {})},
        close_fds=False,
    )
    try:
        out, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        out, _ = kill_driver_group(proc)
        raise AssertionError(
            f"driver did not exit within {timeout}s — the hang is back!\n{out}"
        )
    # This covers reconnect and replay-recovery scenarios as well as the happy
    # path; a clean exit alone does not prove that the unsafe fork was avoided.
    assert_no_grpc_fork_warning(out)
    return proc.returncode, time.monotonic() - t0, out


def finish_duration(output: str) -> float:
    match = re.search(
        r"DRIVER_FINISH fully_delivered=(?:True|False) took=([0-9.]+)s", output
    )
    assert match, f"driver did not report finish duration\n{output}"
    return float(match.group(1))


def spool_files(spool_dir, run_id, suffix=".mkspool"):
    if not os.path.isdir(spool_dir):
        return []
    matches = []
    for name in os.listdir(spool_dir):
        if not name.endswith(suffix):
            continue
        path = os.path.join(spool_dir, name)
        try:
            with open(path, "rb") as fh:
                header = pickle.load(fh)
        except (OSError, EOFError, pickle.UnpicklingError):
            continue
        if isinstance(header, dict) and header.get("run_id") == run_id:
            matches.append(path)
    return matches


def read_spool_records(path):
    records = []
    with open(path, "rb") as fh:
        pickle.load(fh)  # the header
        while True:
            try:
                records.append(pickle.load(fh))
            except EOFError:
                break
    return records


def spooled_train_records(files):
    """Numeric train/* records across spool files (log() also queues extras like system/log_overhead_ms and info/run_info, which it leaves out)."""
    for f in files:
        for record in read_spool_records(f):
            if record[0].startswith("numeric") and record[1].startswith("train/"):
                yield record


def spooled_train_points(files):
    return sum(1 for _ in spooled_train_records(files))


def spooled_train_point_keys(files):
    """ClickHouse sort keys for numeric train/* records in spool files."""
    return {
        (record[1], record[2], record[4] if record[0] == "numeric_tagged_ts" else "")
        for record in spooled_train_records(files)
    }


def expected_train_point_keys(steps, metrics):
    return {
        (f"train/m{metric}", step, "")
        for step in range(steps)
        for metric in range(metrics)
    }


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


def scenario_fast_server(spool_dir):
    print("=== 1. fast server: full delivery, no spool, prompt exit ===")
    servicer, grpc_addr, cdn_addr, stop = start_fake_server(call_delay=0.0)
    run_id = uuid.uuid4().hex
    total = 200 * 20
    rc, wall, out = run_driver(
        {
            "grpc": grpc_addr,
            "cdn": cdn_addr,
            "run_name": "fast",
            "run_id": run_id,
            "steps": 200,
            "metrics": 20,
            "flush_timeout": 30,
        },
        timeout=60,
    )
    assert rc == 0, out
    assert "fully_delivered=True" in out, out
    # info/run_info cdn point rides along with the numeric points
    assert servicer.point_count >= total, (
        f"server got {servicer.point_count} < {total}\n{out}"
    )
    assert not spool_files(spool_dir, run_id), "unexpected spool file on clean run"
    assert servicer.terminates and servicer.terminates[-1][1] == 0, (
        "TerminateRun(0) not received"
    )
    stop()
    print(
        f"    OK — {servicer.point_count} pts in {servicer.ingest_calls} calls, wall {wall:.1f}s\n"
    )


def scenario_slow_server(spool_dir):
    print("=== 2. slow server: partial delivery, bounded finish, remainder spooled ===")
    # Throughput-bound server: each 10k-point flush costs ~0.85s (0.05 + 10000 ×
    # 0.00008), so within finish()'s ~1.2s send half one flush acks (its points are
    # deleted off the buffer prefix) and the deadline spools the unacked rest —
    # exercising the ack-delete and deadline-spill paths together, as in a real
    # run whose server keeps up only partially before the run ends.
    servicer, grpc_addr, cdn_addr, stop = start_fake_server(
        call_delay=0.05, per_point_delay=0.00008
    )
    run_id = uuid.uuid4().hex
    total = 400 * 50
    rc, wall, out = run_driver(
        {
            "grpc": grpc_addr,
            "cdn": cdn_addr,
            "run_name": "slow",
            "run_id": run_id,
            "steps": 400,
            "metrics": 50,
            "flush_timeout": 3,
        },
        timeout=60,
    )
    assert rc == 0, out
    assert "fully_delivered=False" in out, out
    took = finish_duration(out)
    slack = load_factor()
    assert took <= 3 + slack, (
        f"finish exceeded its 3s budget + scheduler slack: {took:.1f}s\n{out}"
    )
    assert wall < 45 * slack, f"exit took {wall:.0f}s — not bounded\n{out}"
    files = spool_files(spool_dir, run_id)
    delivered = servicer.train_point_count()
    spooled = spooled_train_points(files)
    delivered_keys = servicer.train_point_keys()
    spooled_keys = spooled_train_point_keys(files)
    overlap = len(delivered_keys & spooled_keys)
    print(
        f"    delivered={delivered} spooled={spooled} overlap={overlap} "
        f"total={total} wall={wall:.1f}s files={len(files)}"
    )
    expected_keys = expected_train_point_keys(400, 50)
    assert delivered_keys | spooled_keys == expected_keys, (
        "logged keys are neither committed nor spooled\n"
        f"delivered={len(delivered_keys)} spooled={len(spooled_keys)} "
        f"overlap={overlap} expected={len(expected_keys)}\n{out}"
    )
    assert spooled > 0, (
        f"expected a spool with flush_timeout=3 and a slow server\n{out}"
    )
    assert servicer.terminates, "TerminateRun not received"
    stop()
    # A loaded machine can leave the server no time to ack a flush within the budget: then everything spools, which is as correct, but leaves the acks' deletes untested here (scenario 6 covers them).
    print(
        "    OK\n" if delivered else "    OK, with no flush acked: everything spooled\n"
    )
    return files, run_id, spooled


def scenario_replay(spool_dir, files, run_id, expected_replayed):
    print("=== 3. replay spool via python -m kymo.sync (CPU-only path) ===")
    expected_keys = spooled_train_point_keys(files)
    servicer, grpc_addr, cdn_addr, stop = start_fake_server(call_delay=0.0)
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "kymo.sync",
            "--server",
            grpc_addr,
            "--cdn",
            cdn_addr,
            *files,
        ],
        capture_output=True,
        text=True,
        timeout=120,
        env={**os.environ, "PYTHONPATH": CLIENT_DIR},
        close_fds=False,
    )
    print(textwrap.indent(proc.stdout.strip(), "    | "))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    replayed = servicer.train_point_count()
    assert replayed == expected_replayed, (
        f"replayed {replayed}, expected {expected_replayed}"
    )
    assert servicer.train_point_keys() == expected_keys, (
        "replay changed the spooled key set"
    )
    # timestamps preserved (not re-stamped at replay time)
    ts = [p[4] for p in servicer.points if p[0].startswith("train/")]
    assert all(t > 0 for t in ts), "replayed points lost their timestamps"
    for f in files:
        assert not os.path.exists(f) and os.path.exists(f + ".sent"), (
            "spool not renamed to .sent"
        )
    stop()
    print(f"    OK — {replayed} points replayed, timestamps preserved\n")


def scenario_sigterm(spool_dir):
    print("=== 4. SIGTERM mid-backlog: bounded exit + spool (slurm scancel path) ===")
    sigterm_scenario(
        spool_dir, "sigterm", {"hang_after_log": True}, "DRIVER_LOGGED", group=False
    )


def scenario_hung_server(spool_dir):
    print(
        "=== 5. hung server (accepts, never acks): everything spooled, image encoded ==="
    )
    # InitRun answers instantly; IngestMetrics never acks within any useful
    # time — the worst case for shutdown, since a blocking send call would
    # pin the worker past its deadline.
    servicer, grpc_addr, cdn_addr, stop = start_fake_server(call_delay=3600)
    run_id = uuid.uuid4().hex
    total = 100 * 50
    # explicit spool_dir passed to init() must override $KYMO_SPOOL_DIR —
    # the programmatic path trainer configs use to keep spools next to ckpts
    custom_dir = os.path.join(spool_dir, "next-to-ckpts")
    rc, wall, out = run_driver(
        {
            "grpc": grpc_addr,
            "cdn": cdn_addr,
            "run_name": "hung",
            "run_id": run_id,
            "steps": 100,
            "metrics": 50,
            "flush_timeout": 3,
            "log_image": True,
            "spool_dir": custom_dir,
        },
        timeout=90,
    )
    assert rc == 0, out
    assert "fully_delivered=False" in out, out
    took = finish_duration(out)
    assert took <= 4.0, (
        f"finish exceeded its 3s budget + scheduler slack: {took:.1f}s\n{out}"
    )
    assert not spool_files(spool_dir, run_id), (
        "spool leaked into $KYMO_SPOOL_DIR despite explicit spool_dir"
    )
    files = spool_files(custom_dir, run_id)
    assert files, f"expected spool in explicit spool_dir when server never acks\n{out}"
    spooled_numeric = spooled_train_points(files)
    spooled_imgs = 0
    for f in files:
        for rec in read_spool_records(f):
            if rec[0] == "cdn_batch_encoded":
                spooled_imgs += 1
                assert rec[3][0]["data"][:8] == b"\x89PNG\r\n\x1a\n", (
                    "spooled image is not a PNG"
                )
    print(
        f"    spooled_numeric={spooled_numeric} spooled_images={spooled_imgs} wall={wall:.1f}s"
    )
    assert spooled_numeric == total, (
        f"expected all {total} numeric points spooled, got {spooled_numeric}\n{out}"
    )
    assert spooled_imgs == 1, f"expected the image spooled encoded\n{out}"
    stop()
    print("    OK\n")


def scenario_large_pipelined(spool_dir):
    print(
        "=== 6. large run over a fast server: full delivery across many window rounds ==="
    )
    # More than _MAX_UNACKED_POINTS (100k) so the client must feed, get cumulative
    # acks, delete the buffer prefix, and feed again over MANY rounds — the core
    # pipelining loop, which the small fast-server run above never exercises.
    servicer, grpc_addr, cdn_addr, stop = start_fake_server(call_delay=0.0)
    run_id = uuid.uuid4().hex
    total = 600 * 250  # 150k numeric points
    rc, wall, out = run_driver(
        {
            "grpc": grpc_addr,
            "cdn": cdn_addr,
            "run_name": "large",
            "run_id": run_id,
            "steps": 600,
            "metrics": 250,
            "flush_timeout": 60,
        },
        timeout=180,
    )
    assert rc == 0, out
    assert "fully_delivered=True" in out, out
    assert not spool_files(spool_dir, run_id), "unexpected spool on a fast server"
    delivered = servicer.train_point_count()
    assert delivered == total, f"delivered {delivered} != {total}\n{out}"
    # Multiple ingest calls prove the window actually cycled (one call ⇒ no pipelining).
    assert servicer.ingest_calls > 1, (
        f"expected many flushes over the window, got {servicer.ingest_calls}\n{out}"
    )
    assert servicer.bidi_streams == 1, "a clean run should use exactly one bidi stream"
    stop()
    print(
        f"    OK — {delivered} pts fully delivered in {servicer.ingest_calls} flushes, wall {wall:.1f}s\n"
    )


def scenario_stream_break(spool_dir):
    print("=== 7. mid-stream break: reconnect, re-feed from the front, no loss ===")
    # The server ACKs the first 10k cut, commits the second cut, then aborts
    # before its ACK. The client must reconnect and re-feed that outcome-unknown
    # prefix from the buffer front. The second cut lands twice (acceptable —
    # FINAL dedups), while nothing may be lost or spooled after recovery.
    servicer, grpc_addr, cdn_addr, stop = start_fake_server(break_after=12_000)
    run_id = uuid.uuid4().hex
    steps, metrics = 200, 100
    total = steps * metrics
    rc, wall, out = run_driver(
        {
            "grpc": grpc_addr,
            "cdn": cdn_addr,
            "run_name": "break",
            "run_id": run_id,
            "steps": steps,
            "metrics": metrics,
            "flush_timeout": 60,
        },
        timeout=180,
    )
    assert rc == 0, out
    assert "fully_delivered=True" in out, out
    assert not spool_files(spool_dir, run_id), (
        "no spool expected — the server recovered after the break"
    )
    assert servicer.bidi_streams >= 2, (
        f"expected a reconnect after the injected break, saw {servicer.bidi_streams} stream(s)\n{out}"
    )
    delivered = servicer.train_point_count()
    unique = len(servicer.train_point_keys())
    assert unique == total, (
        f"lost unique points after the stream break: {unique} unique, expected {total}\n{out}"
    )
    # Re-delivery of the committed-but-unacked cut is required here, not merely
    # allowed: equality would mean the test never exercised outcome uncertainty.
    assert delivered > total, (
        f"expected a committed cut to be re-sent: delivered {delivered}, total {total}\n{out}"
    )
    stop()
    print(
        f"    OK — {delivered} pts delivered across {servicer.bidi_streams} streams "
        f"({delivered - total} re-sent), wall {wall:.1f}s\n"
    )


def scenario_ack_watchdog(spool_dir):
    print("=== 8. no-ack watchdog: cancel/retry before shutdown deadline ===")
    servicer, grpc_addr, cdn_addr, stop = start_fake_server(call_delay=3600)
    run_id = uuid.uuid4().hex
    steps, metrics = 25, 20
    total = steps * metrics
    rc, wall, out = run_driver(
        {
            "grpc": grpc_addr,
            "cdn": cdn_addr,
            "run_name": "ack-watchdog",
            "run_id": run_id,
            "steps": steps,
            "metrics": metrics,
            # The watchdog arms only once a feed is in flight: give a loaded machine's worker time to start, connect and feed before finish() ends its delivery.
            "recovery_wait": 3,
            "flush_timeout": 2,
            "env": {"KYMO_ACK_PROGRESS_TIMEOUT": "0.5"},
        },
        timeout=30,
    )
    assert rc == 0, out
    assert "fully_delivered=False" in out, out
    assert "no ack progress for 0.5s" in out, f"watchdog did not fire\n{out}"
    files = spool_files(spool_dir, run_id)
    assert spooled_train_points(files) == total, out
    assert wall < 15, f"watchdog scenario was not bounded: {wall:.1f}s\n{out}"
    stop()
    print(f"    OK — watchdog fired and all {total} points spooled in {wall:.1f}s\n")


def scenario_malformed_ack(spool_dir):
    print("=== 9. malformed cumulative ACK: reject, reconnect, and re-feed ===")
    # Stream 1 commits its second 10k cut but replaces that cut's real ACK with
    # an impossible cumulative value. The client must reject it before touching
    # accounting, reconnect, and re-feed the outcome-unknown suffix. That cut is
    # therefore expected to land twice; FINAL deduplicates it in production.
    servicer, grpc_addr, cdn_addr, stop = start_fake_server(bad_ack_after=12_000)
    run_id = uuid.uuid4().hex
    steps, metrics = 200, 100
    total = steps * metrics
    rc, wall, out = run_driver(
        {
            "grpc": grpc_addr,
            "cdn": cdn_addr,
            "run_name": "malformed-ack",
            "run_id": run_id,
            "steps": steps,
            "metrics": metrics,
            "flush_timeout": 30,
        },
        timeout=45,
    )
    assert rc == 0, out
    assert "fully_delivered=True" in out, out
    assert "invalid cumulative ack" in out, f"malformed ACK was not rejected\n{out}"
    assert not spool_files(spool_dir, run_id), (
        "no spool expected — the second stream should recover"
    )
    assert servicer.bidi_streams >= 2, (
        f"expected a reconnect after the malformed ACK, saw "
        f"{servicer.bidi_streams} stream(s)\n{out}"
    )
    delivered = servicer.train_point_count()
    unique = len(servicer.train_point_keys())
    assert unique == total, (
        f"lost unique points after the malformed ACK: {unique} unique, expected {total}\n{out}"
    )
    assert delivered > total, (
        f"expected the committed-but-unacked cut to be re-sent: "
        f"delivered {delivered}, total {total}\n{out}"
    )
    assert wall < 20, f"malformed-ACK recovery was not bounded: {wall:.1f}s\n{out}"
    stop()
    print(
        f"    OK — rejected + recovered; {delivered} pts across "
        f"{servicer.bidi_streams} streams ({delivered - total} re-sent), "
        f"wall {wall:.1f}s\n"
    )


def scenario_watchdog_recovers_blackholed_attempt(spool_dir):
    print("=== 10. blackholed first stream: watchdog reconnects without loss ===")
    # Stream 1 reads the request but never commits or ACKs and remains open after
    # request EOF. A low progress timeout must cancel it mid-finish; stream 2
    # receives the retained prefix and completes normally without deadline spill.
    servicer, grpc_addr, cdn_addr, stop = start_fake_server(blackhole_first=True)
    run_id = uuid.uuid4().hex
    total = 400 * 100  # 40k numeric points
    rc, wall, out = run_driver(
        {
            "grpc": grpc_addr,
            "cdn": cdn_addr,
            "run_name": "watchdog-recovery",
            "run_id": run_id,
            "steps": 400,
            "metrics": 100,
            "flush_timeout": 30,
            "env": {"KYMO_ACK_PROGRESS_TIMEOUT": "0.5"},
        },
        timeout=45,
    )
    assert rc == 0, out
    assert "fully_delivered=True" in out, out
    assert "no ack progress for 0.5s" in out, f"watchdog did not fire\n{out}"
    assert not spool_files(spool_dir, run_id), (
        "no spool expected — the watchdog should recover before the deadline"
    )
    assert servicer.bidi_streams >= 2, (
        f"expected a reconnect after the blackholed attempt, saw "
        f"{servicer.bidi_streams} stream(s)\n{out}"
    )
    delivered = servicer.train_point_count()
    assert delivered == total, (
        f"blackholed stream committed nothing, so expected exactly {total} "
        f"delivered points; got {delivered}\n{out}"
    )
    assert wall < 20, f"watchdog recovery was not bounded: {wall:.1f}s\n{out}"
    stop()
    print(
        f"    OK — watchdog recovered all {delivered} points across "
        f"{servicer.bidi_streams} streams in {wall:.1f}s\n"
    )


def scenario_ram_cap_catches_up_before_live_suffix(spool_dir):
    print("=== 11. RAM cap: replay disk prefix before a newer live suffix ===")
    servicer, grpc_addr, cdn_addr, stop = start_fake_server(call_delay=0.0)
    run_id = uuid.uuid4().hex
    steps, metrics, tail_metrics = 20, 50, 1
    total = steps * metrics + tail_metrics
    rc, wall, out = run_driver(
        {
            "grpc": grpc_addr,
            "cdn": cdn_addr,
            "run_name": "ram-cap-recovery",
            "run_id": run_id,
            "steps": steps,
            "metrics": metrics,
            # Give the still-running worker time to replay the burst before a
            # final point proves that bidi resumes only after disk catch-up.
            "recovery_wait": 3,
            "tail_metrics": tail_metrics,
            "flush_timeout": 15,
            "env": {"KYMO_MAX_BUFFER_POINTS": "100"},
        },
        timeout=30,
    )
    try:
        assert rc == 0, out
        assert "fully_delivered=True" in out, out
        assert "numeric upload backlog exceeded" in out, (
            f"test did not cross the configured RAM cap\n{out}"
        )
        assert "replayed sealed upload spool" in out, (
            f"test did not exercise automatic disk replay\n{out}"
        )
        assert "recovered after ordered disk catch-up" in out, (
            f"the post-replay live suffix was not ACKed\n{out}"
        )
        assert not spool_files(spool_dir, run_id), (
            f"automatic catch-up left a pending spool\n{out}"
        )
        assert not spool_files(spool_dir, run_id, ".sent"), (
            f"the worker kept a .sent copy of a segment it delivered\n{out}"
        )
        expected = expected_train_point_keys(steps, metrics)
        expected.add(("train/tail0", steps, ""))
        assert servicer.train_point_keys() == expected, (
            f"disk-prefix/live-suffix recovery lost keys: "
            f"{len(servicer.train_point_keys())} delivered, {total} expected\n{out}"
        )
        assert wall < 15, f"RAM-cap recovery did not exit promptly: {wall:.1f}s\n{out}"
    finally:
        stop()
    print(
        f"    OK — caught up and delivered all {total} unique points in {wall:.1f}s\n"
    )


def parent_replay_command(out):
    """The `python -m kymo.sync …` line the OWNER printed at shutdown.

    The worker prints its own single-line variant; only the two-line owner report
    proves what a killed worker's files would still be reported as.
    """
    lines = out.splitlines()
    for index, line in enumerate(lines):
        if "undelivered metrics were spooled to disk" in line:
            return lines[index + 1] if index + 1 < len(lines) else ""
    return ""


def scenario_finish_during_replay(spool_dir):
    print("=== 12. finish() lands mid-replay: delivered data reported delivered ===")
    # A replay slow enough that finish() cannot follow it: shutdown closes the
    # connect gate, so the replay itself has to settle spool accounting.
    servicer, grpc_addr, cdn_addr, stop = start_fake_server(per_point_delay=0.006)
    run_id = uuid.uuid4().hex
    steps, metrics = 20, 50
    total = steps * metrics
    rc, wall, out = run_driver(
        {
            "grpc": grpc_addr,
            "cdn": cdn_addr,
            "run_name": "finish-during-replay",
            "run_id": run_id,
            "steps": steps,
            "metrics": metrics,
            # Long enough for the failover to seal and start replaying, short
            # enough that finish() arrives while that replay is still running.
            "recovery_wait": 1.5,
            "flush_timeout": 30,
            "env": {"KYMO_MAX_BUFFER_POINTS": "100"},
        },
        timeout=90,
    )
    try:
        assert rc == 0, out
        assert "numeric upload backlog exceeded" in out, (
            f"test did not cross the configured RAM cap\n{out}"
        )
        assert "replayed sealed upload spool" in out, (
            f"test did not exercise automatic disk replay\n{out}"
        )
        assert not spool_files(spool_dir, run_id), (
            f"replayed spool was left pending\n{out}"
        )
        expected = expected_train_point_keys(steps, metrics)
        assert servicer.train_point_keys() == expected, (
            f"finish during replay lost keys: "
            f"{len(servicer.train_point_keys())} delivered, {total} expected\n{out}"
        )
        assert "fully_delivered=True" in out, (
            f"every point was delivered and no spool remains, yet finish() "
            f"reported incomplete delivery\n{out}"
        )
    finally:
        stop()
    print(f"    OK — mid-replay shutdown still reported full delivery in {wall:.1f}s\n")


def scenario_rotated_segments_are_reported(spool_dir):
    print("=== 13. rotated segments: the owner reports files it never named ===")
    # Replay slowly enough that producers fill a SECOND segment while the first
    # one is in flight, then kill the server so the rotated segments cannot drain.
    servicer, grpc_addr, cdn_addr, stop = start_fake_server(per_point_delay=0.02)
    run_id = uuid.uuid4().hex
    # The burst crosses the cap early. Recovery seals the first spool, and the
    # producer's next spilled record lazily creates a second worker segment.
    # Observing those two identities proves rotation without waiting for the
    # first replay, whose duration depends on how many points accumulated before
    # the recovery connect. This requires production to still be running when
    # rotation occurs: the first retry is 0.25–0.5s plus a local connect, against
    # at least 3s of production here. recovery_wait keeps the driver alive while
    # the test observes the handoff.
    steps, metrics = 100, 50
    stdout = tempfile.TemporaryFile(mode="w+")
    proc = subprocess.Popen(
        driver_command(
            {
                "grpc": grpc_addr,
                "cdn": cdn_addr,
                "run_name": "rotated-segments",
                "run_id": run_id,
                "steps": steps,
                "metrics": metrics,
                "step_sleep": 0.03,
                "recovery_wait": 20,
                "flush_timeout": 8,
            }
        ),
        stdout=stdout,
        stderr=subprocess.STDOUT,
        text=True,
        env={**os.environ, "KYMO_VERBOSE": "1", "KYMO_MAX_BUFFER_POINTS": "100"},
        close_fds=False,
    )

    def driver_output():
        stdout.seek(0)
        return stdout.read()

    try:
        # Count distinct worker segment names across polls: the worker deletes a segment once it is delivered, so remember every name seen.
        # Requiring a pending worker segment ensures shutdown must report a
        # rotated path that the owner did not create itself.
        deadline = time.monotonic() + 60
        worker_segments = set()
        while True:
            pending_workers = [
                path
                for path in spool_files(spool_dir, run_id)
                if "__worker_" in os.path.basename(path)
            ]
            worker_segments.update(pending_workers)
            if pending_workers and len(worker_segments) >= 2:
                break
            assert proc.poll() is None, (
                f"driver exited before rotating a spool segment\n{driver_output()}"
            )
            assert time.monotonic() < deadline, (
                f"worker never rotated a spool segment\n{driver_output()}"
            )
            time.sleep(0.05)
        stop()
        try:
            proc.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            kill_driver_group(proc)
            raise AssertionError("rotated-segment shutdown hung")
        out = driver_output()
    finally:
        stdout.close()
        stop()

    pending = spool_files(spool_dir, run_id)
    assert_no_grpc_fork_warning(out)
    assert pending, f"a dead server left nothing to replay\n{out}"
    command = parent_replay_command(out)
    unreported = [path for path in pending if path not in command]
    assert not unreported, (
        f"the owner's shutdown report omitted {len(unreported)} of {len(pending)} "
        f"pending spool file(s) — a killed worker would leave them unretired and "
        f"unmentioned:\n  " + "\n  ".join(unreported) + f"\n{out}"
    )
    assert "fully_delivered=False" in out, (
        f"undelivered points remain on disk, yet finish() claimed delivery\n{out}"
    )
    print(f"    OK — all {len(pending)} rotated segment(s) reported by the owner\n")


# A signal scenario's run, unless it says otherwise (signal_driver).
STEPS, METRICS = 300, 50


def signal_driver(
    name,
    cfg,
    wait_for,
    signals,
    *,
    server=None,
    env=None,
    group=True,
    pause_worker=False,
):
    """Run the driver as run name, logging STEPS steps of METRICS metrics unless cfg says otherwise, against a fake server started with server (start_fake_server's arguments); once its output shows wait_for, 0.3 s more have passed and the fake server has its first point, send it signals, 50 ms apart, to its whole process group as a terminal's Ctrl-C or a scheduler's cancel does, or else to it alone. With pause_worker, its upload worker is stopped (SIGSTOP) 0.2 s before them and resumed, if it is still there, once the driver has exited.

    Returns (servicer, run_id, returncode, output, sent, console, gone): the fake server's servicer, the run's id, the driver's exit status and its output, when (time.monotonic()) the first signal went, and the seconds from then to the driver's exit, which gives the console back, and to the end of its output, which every process of its group holds open, the upload worker included. The group must be gone within 60 s, scaled by the machine's load (load_factor).
    """
    servicer, grpc_addr, cdn_addr, stop = start_fake_server(
        **(server or {"call_delay": 0.1, "per_point_delay": 0.0002})
    )
    run_id = uuid.uuid4().hex
    cfg = {
        "grpc": grpc_addr,
        "cdn": cdn_addr,
        "run_name": name,
        "run_id": run_id,
        "steps": STEPS,
        "metrics": METRICS,
        "flush_timeout": 30,
        **cfg,
        "print_worker_pid": pause_worker,
    }
    try:
        proc = subprocess.Popen(
            driver_command(cfg),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env={**os.environ, "KYMO_VERBOSE": "1", **(env or {})},
            close_fds=False,
        )
    except BaseException:
        stop()
        raise
    selector = selectors.DefaultSelector()
    selector.register(proc.stdout, selectors.EVENT_READ)
    output = bytearray()
    paused = None
    try:
        deadline = time.monotonic() + 30
        while wait_for.encode() not in output:
            remaining = deadline - time.monotonic()
            assert remaining > 0 and selector.select(remaining), (
                f"driver never printed {wait_for!r}\n{output.decode(errors='replace')}"
            )
            chunk = os.read(proc.stdout.fileno(), 65536)
            assert chunk, f"driver exited early\n{output.decode(errors='replace')}"
            output.extend(chunk)
        time.sleep(0.3)
        # The scenarios test a running upload worker: one still starting under spawn would leave only the parent's not-started path (_kill_unstarted_worker kills it and the parent spools), and a paused one must already hold a group.
        assert servicer.first_point.wait(30 * load_factor()), (
            f"the upload worker sent nothing\n{output.decode(errors='replace')}"
        )
        if pause_worker:
            paused = int(re.search(rb"WORKER_PID (\d+)", output).group(1))
            os.kill(paused, signal.SIGSTOP)
            time.sleep(0.2)
        sent = time.monotonic()
        for index, signum in enumerate(signals):
            if index:
                time.sleep(0.05)
            if group:
                os.killpg(proc.pid, signum)
            else:
                os.kill(proc.pid, signum)
        console = gone = None
        bound = 60 * load_factor()
        while gone is None:
            assert time.monotonic() - sent < bound, (
                f"driver group still running {bound:.0f}s after the signal\n"
                + output.decode(errors="replace")
            )
            if console is None and proc.poll() is not None:
                console = time.monotonic() - sent
                if paused is not None:
                    with contextlib.suppress(ProcessLookupError):
                        os.kill(paused, signal.SIGCONT)
            if selector.select(0.01):
                chunk = os.read(proc.stdout.fileno(), 65536)
                if not chunk:
                    gone = time.monotonic() - sent
                output.extend(chunk)
        proc.wait(timeout=5)
        if console is None:
            console = gone
    except BaseException:
        kill_driver_group(proc)
        raise
    finally:
        selector.close()
        proc.stdout.close()
        stop()
    out = output.decode(errors="replace")
    assert_no_grpc_fork_warning(out)
    return servicer, run_id, proc.returncode, out, sent, console, gone


def ctrl_c_scenario(
    spool_dir, name, cfg, wait_for, signals, *, budget, exits=None, **driver
):
    """Ctrl-C a run's process group (signal_driver, given driver's keywords) at a signal budget of budget seconds: the run ends with TerminateRun(130) and the console comes back at once, and the upload worker, which carries on through the Ctrl-C, delivers or spools what it can and is gone within budget. Nothing claims a delivery that lost points. The program exits on the Ctrl-C, or with exits, its own status."""
    servicer, run_id, rc, out, sent, console, gone = signal_driver(
        name,
        cfg,
        wait_for,
        signals,
        env={"KYMO_SIGNAL_FLUSH_TIMEOUT": str(budget)},
        **driver,
    )
    # Killed by its uncaught KeyboardInterrupt, as Python exits on one, or by kymo's exit with 130.
    assert rc in ((-signal.SIGINT, 130) if exits is None else (exits,)), (
        f"exit {rc}\n{out}"
    )
    slack = load_factor()
    assert console < 1.5 * slack, (
        f"the console came back {console:.2f}s after Ctrl-C\n{out}"
    )
    assert gone < budget + 0.5 * slack, (
        f"the worker outlived Ctrl-C by {gone:.1f}s\n{out}"
    )
    assert "worker exiting" in out, f"the upload worker died of the Ctrl-C\n{out}"
    assert servicer.terminates == [(run_id, 130)], (
        f"expected one TerminateRun(130), got {servicer.terminates}\n{out}"
    )
    terminated = servicer.terminated_at[0] - sent
    assert terminated < console, (
        f"TerminateRun came {terminated:.2f}s after Ctrl-C, after the console "
        f"({console:.2f}s)\n{out}"
    )
    delivered = servicer.train_point_keys()
    spooled = spooled_train_point_keys(spool_files(spool_dir, run_id))
    # A program that logs until interrupted says how many steps it logged.
    logged = re.search(r"DRIVER_STEPS (\d+)", out)
    steps = int(logged.group(1)) if logged else cfg.get("steps", STEPS)
    expected = expected_train_point_keys(steps, cfg.get("metrics", METRICS))
    assert delivered | spooled <= expected, out
    assert "fully_delivered=True" not in out, (
        f"a run a Ctrl-C ended claimed complete delivery\n{out}"
    )
    print(
        f"    OK — TerminateRun {terminated:.2f}s, console {console:.2f}s, worker "
        f"gone {gone:.1f}s after Ctrl-C; delivered={len(delivered)} "
        f"spooled={len(spooled - delivered)} of {len(expected)}\n"
    )
    return out, run_id


def scenario_ctrl_c_during_training(spool_dir):
    print("=== 14. Ctrl-C mid-training, uncaught: the run ends within 10s ===")
    # The default budget; the server acks a 10k cut in about 2s, so some points spool. Under spawn too where another start method is the default (Linux): the worker then holds the resource tracker's pipe, whose teardown on some Python versions waits for every holder (_disown_worker).
    for method in dict.fromkeys([multiprocessing.get_start_method(), "spawn"]):
        ctrl_c_scenario(
            spool_dir,
            f"ctrl-c-training-{method}",
            {"hang_after_log": True, "start_method": method},
            "DRIVER_LOGGED",
            [signal.SIGINT],
            budget=10,
        )


def scenario_ctrl_c_during_finish(spool_dir):
    print("=== 15. Ctrl-C during a slow finish(): handed off, no restart at exit ===")
    ctrl_c_scenario(
        spool_dir,
        "ctrl-c-finish",
        {},
        "DRIVER_FINISHING",
        [signal.SIGINT],
        budget=3,
    )


def scenario_ctrl_c_during_atexit_flush(spool_dir):
    print("=== 16. Ctrl-C during the atexit flush: handed off, exit status kept ===")
    ctrl_c_scenario(
        spool_dir,
        "ctrl-c-atexit",
        {"skip_finish": True},
        "shutting down — flushing remaining metrics",
        [signal.SIGINT],
        budget=3,
        exits=0,
    )


def exit_code_scenario(spool_dir, name, cfg, exits, code):
    """Run the driver with cfg against a fresh fake server: the program exits with exits, and the run ends with one TerminateRun(code)."""
    servicer, grpc_addr, cdn_addr, stop = start_fake_server()
    run_id = uuid.uuid4().hex
    rc, _wall, out = run_driver(
        {
            "grpc": grpc_addr,
            "cdn": cdn_addr,
            "run_name": name,
            "run_id": run_id,
            "steps": 20,
            "metrics": 5,
            "flush_timeout": 30,
            "spool_dir": spool_dir,
            **cfg,
        },
        timeout=60,
    )
    stop()
    assert rc == exits, f"exit {rc}\n{out}"
    assert [c for rid, c in servicer.terminates if rid == run_id] == [code], (
        f"expected one TerminateRun({code}), got {servicer.terminates}\n{out}"
    )
    print(f"    OK — {name}: exit {exits}, TerminateRun({code})")


# Scenarios 25-30, which the module docstring describes: the driver's cfg, the program's exit status and the code its run ends with.
EXIT_CODE_SCENARIOS = (
    (25, "helper-finish", {"exit_with_helper_finish": 7}, 7, 7),
    (
        25,
        "executor-finish",
        {"exit_with_helper_finish": 7, "helper_executor": True},
        7,
        7,
    ),
    (26, "caught-exit", {"caught_exit": 7}, 0, 0),
    (27, "atexit-finish", {"atexit_finish": 7}, 7, 7),
    (28, "caught-exit-sigterm", {"caught_exit": 7, "sigterm_after": True}, 143, 143),
    (29, "exit-executor-sigterm", {"executor_sigterm": 7}, 7, 7),
    (
        30,
        "handler-exit-at-threading-exit",
        {"sigterm_exits": 1, "sigterm_at_threading_exit": True},
        0,
        143,
    ),
    (
        30,
        "handler-interrupt-at-threading-exit",
        {"sigterm_interrupts": True, "sigterm_at_threading_exit": True},
        0,
        143,
    ),
)


def scenario_exit_codes(spool_dir):
    for number, name, cfg, exits, code in EXIT_CODE_SCENARIOS:
        print(f"=== {number}. {name} ===")
        exit_code_scenario(spool_dir, name, cfg, exits, code)
        print()


def scenario_ctrl_c_caught_then_exit(spool_dir):
    print("=== 22. Ctrl-C the program catches and exits on: handed off ===")
    ctrl_c_scenario(
        spool_dir,
        "ctrl-c-caught",
        {"exit_on_interrupt": True},
        "DRIVER_LOGGED",
        [signal.SIGINT],
        budget=3,
        exits=0,
    )


def scenario_second_ctrl_c(spool_dir):
    print("=== 17. Ctrl-C twice during finish(): the worker survives both ===")
    ctrl_c_scenario(
        spool_dir,
        "ctrl-c-twice",
        {},
        "DRIVER_FINISHING",
        [signal.SIGINT, signal.SIGINT],
        budget=3,
    )


def scenario_ctrl_c_during_the_encoder_wait(spool_dir):
    print("=== 23. Ctrl-C while log() waits for the encoder: handed off at once ===")
    # The wait holds no section that would defer the Ctrl-C, so it raises there, the program's uncaught KeyboardInterrupt hands the run off, and the worker, its encoder still busy, delivers or spools the galleries and exits by itself within the 10 s from the Ctrl-C. With a 3 s budget and galleries far too slow to encode in it (40 images, several seconds each), the worker waits for the encode in flight only until its spill end, then reports the galleries it cut lost and exits by itself.
    for images, budget in ((4, 10), (40, 3)):
        out, run_id = ctrl_c_scenario(
            spool_dir,
            f"ctrl-c-encoder-wait-{images}",
            {
                "steps": 10,
                "log_galleries": 3,
                "gallery_images": images,
                "hang_after_log": True,
            },
            "DRIVER_AWAITING_ENCODER",
            [signal.SIGINT],
            budget=budget,
        )
        assert "in _await_encoder" in out, f"the Ctrl-C landed outside the wait\n{out}"
        assert "DRIVER_LOGGED" not in out, out
        # The two galleries log() published before the third's wait: each delivered, spooled, or reported lost.
        delivered = {
            int(step) for step in re.findall(r"CDN demo/gallery step=(\d+) →", out)
        }
        spooled = {
            record[2]
            for path in spool_files(spool_dir, run_id)
            for record in read_spool_records(path)
            if record[1] == "demo/gallery"
        }
        lost = sum(
            int(count)
            for count in re.findall(r"(\d+) points reached neither the server", out)
        )
        assert len({0, 1} - delivered - spooled) <= lost, (
            f"galleries delivered {delivered}, spooled {spooled}, {lost} lost\n{out}"
        )
        print(
            f"    galleries: delivered {sorted(delivered)}, spooled {sorted(spooled)}, "
            f"{lost} reported lost\n"
        )


# The sitecustomize of scenario 24, which acts in multiprocessing's resource tracker and spawn children only: after each SIG_UNBLOCK, the process records which signals it still blocks, since macOS shows no other process's mask.
MASK_HOOK = textwrap.dedent("""
    import os, signal, sys
    _roles = {"from multiprocessing.resource_tracker import": "tracker", "from multiprocessing.spawn import": "worker"}
    _role = next((r for a in sys.orig_argv for p, r in _roles.items() if a.startswith(p)), None)
    if _role:
        _sigmask = signal.pthread_sigmask

        def _recorded(how, mask):
            old = _sigmask(how, mask)
            if how == signal.SIG_UNBLOCK:
                with open(os.environ["KYMO_TEST_MASKS"], "a") as file:
                    print(_role, *map(int, _sigmask(signal.SIG_BLOCK, [])), file=file)
            return old

        signal.pthread_sigmask = _recorded
""")


def scenario_resource_tracker_mask(spool_dir):
    print("=== 24. spawn's resource tracker keeps the group signals blocked ===")
    # The tracker ignores SIGINT and SIGTERM alone: a group SIGQUIT (Ctrl-\) would kill it with a core dump, and the program's exit would print KeyError tracebacks. init() starts it before its first lock or shared value would, with the group's signals blocked, and after its set-up it keeps blocked all but the two it ignores. The worker's policy unblocks all six.
    servicer, grpc_addr, cdn_addr, stop = start_fake_server()
    with tempfile.TemporaryDirectory() as hook:
        with open(os.path.join(hook, "sitecustomize.py"), "w") as file:
            file.write(MASK_HOOK)
        masks = os.path.join(hook, "masks")
        path = os.pathsep.join(filter(None, [hook, os.environ.get("PYTHONPATH")]))
        rc, _, out = run_driver(
            {
                "grpc": grpc_addr,
                "cdn": cdn_addr,
                "run_name": "tracker-mask",
                "run_id": uuid.uuid4().hex,
                "steps": 10,
                "metrics": 10,
                "flush_timeout": 30,
                "start_method": "spawn",
                "env": {"PYTHONPATH": path, "KYMO_TEST_MASKS": masks},
            },
            timeout=60,
        )
        with open(masks) as file:
            blocked = {
                role: set(map(int, rest)) for role, *rest in map(str.split, file)
            }
    stop()
    assert rc == 0, out
    kept = {signal.SIGQUIT, signal.SIGHUP, signal.SIGUSR1, signal.SIGUSR2}
    group = kept | {signal.SIGINT, signal.SIGTERM}
    # A missing record fails either check.
    assert blocked.get("tracker", set()) & group == kept, f"{blocked}\n{out}"
    assert not blocked.get("worker", group) & group, f"{blocked}\n{out}"
    print("    OK — the tracker keeps those four blocked, the worker none\n")


def sigterm_scenario(spool_dir, name, cfg, wait_for, *, group):
    """SIGTERM a run with the signal budget at 3 s: it exits 143 within the budget, and every logged point is delivered or spooled."""
    servicer, run_id, rc, out, sent, console, gone = signal_driver(
        name,
        cfg,
        wait_for,
        [signal.SIGTERM],
        env={"KYMO_SIGNAL_FLUSH_TIMEOUT": "3"},
        group=group,
    )
    assert rc == 143, f"expected exit 143, got {rc}\n{out}"
    assert console < 3 + 0.5 * load_factor(), (
        f"exit took {console:.1f}s of a 3s signal budget\n{out}"
    )
    delivered = servicer.train_point_keys()
    spooled = spooled_train_point_keys(spool_files(spool_dir, run_id))
    expected = expected_train_point_keys(STEPS, METRICS)
    assert delivered | spooled == expected, (
        f"SIGTERM left logged keys neither committed nor spooled: "
        f"{len(delivered)} delivered, {len(spooled)} spooled, "
        f"{len(expected)} expected\n{out}"
    )
    assert servicer.terminates == [(run_id, 143)], (
        f"expected one TerminateRun(143), got {servicer.terminates}\n{out}"
    )
    terminated = servicer.terminated_at[0] - sent
    print(
        f"    OK — exit {console:.1f}s, TerminateRun {terminated:.1f}s, group gone "
        f"{gone:.1f}s after SIGTERM; delivered={len(delivered)} "
        f"spooled={len(spooled - delivered)} of {len(expected)}\n"
    )
    return out


def scenario_group_sigterm(spool_dir):
    print("=== 18. group SIGTERM mid-training: the worker delivers or spools all ===")
    out = sigterm_scenario(
        spool_dir,
        "group-sigterm",
        {"hang_after_log": True},
        "DRIVER_LOGGED",
        group=True,
    )
    assert "worker exiting" in out, f"the upload worker died of SIGTERM\n{out}"


def scenario_sigterm_during_finish(spool_dir):
    print("=== 19. SIGTERM during finish(): one shutdown, bounded by the budget ===")
    out = sigterm_scenario(
        spool_dir, "sigterm-finish", {}, "DRIVER_FINISHING", group=False
    )
    assert out.count("ending the shutdown within 3s") == 1, out
    assert "flushing metrics before exit" not in out, (
        f"SIGTERM started a second shutdown inside finish()\n{out}"
    )
    assert out.count("queued points (budget") == 1, out


def scenario_ignored_group_sigterm(spool_dir):
    print("=== 20. ignored group SIGTERM: the program and its worker carry on ===")
    # The group SIGTERM lands during finish(), whose delivery takes seconds: a worker that took it as its own would spool the rest after the 1 s budget.
    servicer, run_id, rc, out, sent, console, gone = signal_driver(
        "ignored-sigterm",
        {"ignore_sigterm": True},
        "DRIVER_FINISHING",
        [signal.SIGTERM],
        env={"KYMO_SIGNAL_FLUSH_TIMEOUT": "1"},
    )
    assert rc == 0, f"exit {rc}\n{out}"
    assert "fully_delivered=True" in out, out
    delivered = servicer.train_point_keys()
    assert delivered == expected_train_point_keys(STEPS, METRICS), (
        f"{len(delivered)} delivered\n{out}"
    )
    assert not spool_files(spool_dir, run_id), out
    assert servicer.terminates == [(run_id, 0)], servicer.terminates
    print(f"    OK — all {len(delivered)} points delivered, exit {console:.1f}s\n")


def scenario_ctrl_c_mid_group(spool_dir):
    print("=== 21. Ctrl-C mid-group: the worker spools what it received ===")
    # Groups of about 95 KB, past a pipe's 64 KiB. The worker is stopped (SIGSTOP), as a busy one is not reading, so the program's feeder thread is writing one when the Ctrl-C makes it exit, and the rest of that group never comes: once resumed, the worker must neither wait for it nor lose the groups it received, and must exit by itself before the 3 s cap's SIGALRM. The server never acks, so they spool.
    metrics = 4000
    out, run_id = ctrl_c_scenario(
        spool_dir,
        "ctrl-c-mid-group",
        {
            "steps": 0,
            "metrics": metrics,
            "log_until_interrupted": True,
            "step_sleep": 0.05,
        },
        "DRIVER_LOGGED",
        [signal.SIGINT],
        budget=3,
        server={"call_delay": 3600},
        pause_worker=True,
    )
    assert "arrived only in part" in out, f"no group was cut short\n{out}"
    # Its loss report names the groups it never read, none it cut.
    assert not re.search(r"[1-9]\d* past the spill deadline", out), out
    logged = int(re.search(r"DRIVER_STEPS (\d+)", out).group(1))
    spooled = spooled_train_point_keys(spool_files(spool_dir, run_id))
    received = len({step for _, step, _ in spooled})
    # The worker received the groups in order and spooled every one whole.
    assert received and spooled == expected_train_point_keys(received, metrics), (
        f"spooled {len(spooled)} points of {received} steps\n{out}"
    )
    # The rest is reported: the group the worker received in part, and the program's groups its feeder never wrote.
    left = re.search(r"(\d+) point\(s\) had not yet reached the upload worker", out)
    left = int(left.group(1)) if left else 0
    assert left >= (logged - received - 1) * metrics, (
        f"{logged} steps logged, {received} spooled, {left} points reported left\n{out}"
    )
    print(
        f"    {received} of {logged} steps spooled, one in part, {left} points "
        f"reported left in the program\n"
    )


def main():
    with tempfile.TemporaryDirectory(prefix="kymo-spool-test-") as spool_dir:
        os.environ["KYMO_SPOOL_DIR"] = spool_dir
        try:
            scenario_fast_server(spool_dir)
            files, run_id, spooled = scenario_slow_server(spool_dir)
            scenario_replay(spool_dir, files, run_id, spooled)
            scenario_sigterm(spool_dir)
            scenario_hung_server(spool_dir)
            scenario_large_pipelined(spool_dir)
            scenario_stream_break(spool_dir)
            scenario_ack_watchdog(spool_dir)
            scenario_malformed_ack(spool_dir)
            scenario_watchdog_recovers_blackholed_attempt(spool_dir)
            scenario_ram_cap_catches_up_before_live_suffix(spool_dir)
            scenario_finish_during_replay(spool_dir)
            scenario_rotated_segments_are_reported(spool_dir)
            scenario_ctrl_c_during_training(spool_dir)
            scenario_ctrl_c_during_finish(spool_dir)
            scenario_ctrl_c_during_atexit_flush(spool_dir)
            scenario_second_ctrl_c(spool_dir)
            scenario_group_sigterm(spool_dir)
            scenario_sigterm_during_finish(spool_dir)
            scenario_ignored_group_sigterm(spool_dir)
            scenario_ctrl_c_mid_group(spool_dir)
            scenario_ctrl_c_caught_then_exit(spool_dir)
            scenario_ctrl_c_during_the_encoder_wait(spool_dir)
            scenario_resource_tracker_mask(spool_dir)
            scenario_exit_codes(spool_dir)
        finally:
            # A failed scenario never reaches its own stop(), and a handler parked in a long call_delay would hold the interpreter's exit until it ends.
            for stop in list(_RUNNING_SERVERS):
                stop()
    print("ALL SCENARIOS PASSED")


if __name__ == "__main__":
    main()
