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

Run:  python3 test_graceful_shutdown.py
"""

import hashlib
import http.server
import json
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
        self.lock = threading.Lock()
        self.points = []  # (metric_name, step, payload_kind, tag, timestamp_ms)
        self.ingest_calls = 0
        self.bidi_streams = 0
        self.terminates = []

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
        return kymo_pb2.TerminateRunResponse()

    def IngestMetrics(self, request_iterator, context):
        n = 0
        recv = []
        for batch in request_iterator:
            for p in batch.points:
                recv.append(
                    (
                        p.metric_name,
                        p.step,
                        p.WhichOneof("payload"),
                        p.tag,
                        p.timestamp_ms,
                    )
                )
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

        def commit(cut):
            """Return False when cancellation makes this cut uncommitted."""
            delay = self.call_delay + len(cut) * self.per_point_delay
            if delay:
                self.stop_event.wait(delay)
            if not context.is_active():
                return False
            with self.lock:
                self.points.extend(cut)
                self.ingest_calls += 1
            return True

        def ack_or_inject_fault(cut):
            if not commit(cut):
                return "stop", None
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
                    pending.append(
                        (
                            p.metric_name,
                            p.step,
                            p.WhichOneof("payload"),
                            p.tag,
                            p.timestamp_ms,
                        )
                    )
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
        stopped = server.stop(grace=0)
        servicer.stop_event.set()
        stopped.wait(timeout=5)
        cdn.shutdown()
        cdn.server_close()

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
    kymo.init(
        server_address=cfg["grpc"], cdn_address=cfg["cdn"],
        project_id="shutdown-test", run_name=cfg["run_name"],
        run_id=cfg["run_id"], system_metrics=False,
        spool_dir=cfg.get("spool_dir"),
    )
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
    print("DRIVER_LOGGED", flush=True)
    if cfg.get("hang_after_log"):
        time.sleep(3600)  # wait to be signalled
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
import sys

os.setsid()
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
    except ProcessLookupError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
    return proc.communicate(timeout=5)


def assert_no_grpc_fork_warning(output):
    assert "fork_posix.cc" not in output, (
        "trainer initialized gRPC before forking its upload worker\n" + output
    )


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
        header = pickle.load(fh)
        while True:
            try:
                records.append(pickle.load(fh))
            except EOFError:
                break
    return header, records


def spooled_train_points(files):
    """Numeric train/* points across spool files (log() also queues extras
    like system/log_overhead_ms and info/run_info, which we don't count)."""
    n = 0
    for f in files:
        _, records = read_spool_records(f)
        n += sum(
            1
            for r in records
            if r[0].startswith("numeric") and r[1].startswith("train/")
        )
    return n


def spooled_train_point_keys(files):
    """ClickHouse sort keys for numeric train/* records in spool files."""
    keys = set()
    for f in files:
        _, records = read_spool_records(f)
        for record in records:
            if not record[0].startswith("numeric") or not record[1].startswith(
                "train/"
            ):
                continue
            tag = record[4] if record[0] == "numeric_tagged_ts" else ""
            keys.add((record[1], record[2], tag))
    return keys


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
    assert took <= 4.0, (
        f"finish exceeded its 3s budget + scheduler slack: {took:.1f}s\n{out}"
    )
    assert wall < 45, f"exit took {wall:.0f}s — not bounded\n{out}"
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
    assert delivered > 0, (
        f"expected SOME points acked+delivered within the 3s budget\n{out}"
    )
    assert servicer.terminates, "TerminateRun not received"
    stop()
    print("    OK\n")
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
    servicer, grpc_addr, cdn_addr, stop = start_fake_server(
        call_delay=0.1, per_point_delay=0.001
    )
    run_id = uuid.uuid4().hex
    total = 300 * 50
    proc = subprocess.Popen(
        driver_command(
            {
                "grpc": grpc_addr,
                "cdn": cdn_addr,
                "run_name": "sigterm",
                "run_id": run_id,
                "steps": 300,
                "metrics": 50,
                "flush_timeout": 3,
                "hang_after_log": True,
            }
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env={**os.environ, "KYMO_SIGNAL_FLUSH_TIMEOUT": "3"},
        close_fds=False,
    )
    # Wait for DRIVER_LOGGED without an unbounded readline(): if startup wedges,
    # the test itself must still fail within its advertised bound.
    deadline = time.monotonic() + 30
    selector = selectors.DefaultSelector()
    selector.register(proc.stdout, selectors.EVENT_READ)
    output_prefix = bytearray()
    try:
        while b"DRIVER_LOGGED" not in output_prefix:
            remaining = deadline - time.monotonic()
            assert remaining > 0, "driver never finished logging"
            assert selector.select(remaining), "driver never finished logging"
            chunk = os.read(proc.stdout.fileno(), 4096)
            assert chunk or proc.poll() is None, (
                f"driver exited before finishing logging (rc={proc.returncode})"
            )
            output_prefix.extend(chunk)
    except BaseException:
        kill_driver_group(proc)
        stop()
        raise
    finally:
        selector.close()
    proc.send_signal(signal.SIGTERM)
    t1 = time.monotonic()
    try:
        remainder = proc.communicate(timeout=60)[0]
    except subprocess.TimeoutExpired:
        remainder = kill_driver_group(proc)[0]
        stop()
        out = output_prefix.decode(errors="replace") + remainder
        raise AssertionError(f"SIGTERM path hung\n{out}")
    out = output_prefix.decode(errors="replace") + remainder
    wall = time.monotonic() - t1
    assert_no_grpc_fork_warning(out)
    assert proc.returncode == 143, f"expected exit 143, got {proc.returncode}\n{out}"
    files = spool_files(spool_dir, run_id)
    delivered = servicer.train_point_count()
    spooled = spooled_train_points(files)
    delivered_keys = servicer.train_point_keys()
    spooled_keys = spooled_train_point_keys(files)
    expected_keys = expected_train_point_keys(300, 50)
    assert delivered_keys | spooled_keys == expected_keys, (
        "SIGTERM left logged keys neither committed nor spooled: "
        f"{len(delivered_keys)} delivered, {len(spooled_keys)} spooled, "
        f"{len(delivered_keys & spooled_keys)} overlap, {total} expected\n{out}"
    )
    assert servicer.terminates and servicer.terminates[-1][1] == 143, (
        "TerminateRun(143) not received"
    )
    stop()
    print(
        f"    OK — exit in {wall:.1f}s after SIGTERM, delivered={delivered} spooled={spooled}\n"
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
        _, records = read_spool_records(f)
        for rec in records:
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
        # Count distinct worker segment identities across pending and replayed
        # names. Scan .sent first so a concurrent rename can only undercount for
        # one poll; normalization also prevents one segment from counting twice.
        # Requiring a pending worker segment ensures shutdown must report a
        # rotated path that the owner did not create itself.
        deadline = time.monotonic() + 60
        while True:
            sent_workers = [
                path
                for path in spool_files(spool_dir, run_id, ".sent")
                if "__worker_" in os.path.basename(path)
            ]
            pending_workers = [
                path
                for path in spool_files(spool_dir, run_id)
                if "__worker_" in os.path.basename(path)
            ]
            worker_segments = {
                path.removesuffix(".sent") for path in (*sent_workers, *pending_workers)
            }
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


def main():
    with tempfile.TemporaryDirectory(prefix="kymo-spool-test-") as spool_dir:
        os.environ["KYMO_SPOOL_DIR"] = spool_dir
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
    print("ALL SCENARIOS PASSED")


if __name__ == "__main__":
    main()
