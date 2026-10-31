"""Focused, service-free checks for the bidi upload accounting boundary."""

import base64
import collections
import contextlib
import ctypes
import dis
import gc
import inspect
import io
import itertools
import json
import logging
import math
import multiprocessing
import multiprocessing.queues
import multiprocessing.util
import os
import pickle
import queue
import random
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import types
import unittest
import weakref
from collections import deque
from unittest import mock

import numpy as np
from PIL import Image as PILImage

from kymo import _capture as capture_module
from kymo import _gpu as gpu_module
from kymo import client as client_module
from kymo import spool as spool_module
from kymo import sync as sync_module
from kymo import system_metrics as system_metrics_module
from kymo import _wire as wire_module
from kymo import _worker as worker_module
from kymo._cdn import content_id
from kymo._local_runtime import (
    LocalInstallationMismatch,
    endpoint_from_worker_config,
)
from kymo.spool import SpoolWriter, make_spool_path, read_spool
from kymo.system_metrics import SystemMetricsPoller
from kymo.types import Image, Metadata, Resource
from test_local_runtime import endpoint_payload

try:
    import torch
except ImportError:
    # CI installs no torch: _StubTensor covers the device branches there, and the real-tensor tests skip.
    torch = None

# init()'s first multiprocessing logger set-up in a process (_start_run) unregisters and registers multiprocessing's exit hook again: done here, before any test patches atexit.register, so a test counting init()'s registrations counts kymo's alone, and the hook is not lost to the mock, whichever test runs first.
multiprocessing.util.get_logger()


def numeric(index: int) -> tuple:
    return ("numeric_ts", "train/loss", index, float(index), 1_700_000_000_000)


def _codes_of(*functions):
    """These functions' code objects and those their comprehensions run in: before Python 3.12 a comprehension runs in a frame of its own."""
    codes = {function.__code__ for function in functions}
    for function in functions:
        codes.update(filter(inspect.iscode, function.__code__.co_consts))
    return codes


def _opcode_tracer(codes, k: int, inject, paused=lambda: False):
    """A sys.settrace tracer that calls inject(frame) once, at the k-th opcode run in frames of codes while paused() is false, an except clause's entry and exit aside (_enters_or_leaves_an_except); returned with a list that is non-empty once it has."""
    seen, fired = [0], []

    def local(frame, event, arg):
        # Python 3.13 starts opcode events only once a line event sets this.
        frame.f_trace_opcodes = True
        if (
            event == "opcode"
            and not fired
            and not paused()
            and not _enters_or_leaves_an_except(frame)
        ):
            if seen[0] == k:
                fired.append(True)
                inject(frame)
            seen[0] += 1
        return local

    def tracer(frame, event, arg):
        if frame.f_code in codes and not fired:
            frame.f_trace_opcodes = True
            # Python 3.12 instruments a code's opcodes for a frame already running only when settrace runs again.
            sys.settrace(tracer)
            return local
        return None

    return tracer, fired


def _enters_or_leaves_an_except(frame) -> bool:
    """Whether frame's opcode enters an except clause (PUSH_EXC_INFO) or leaves it (POP_EXCEPT, and the COPY before one in a cleanup), where no signal handler runs: from Python 3.11 a raise there leaves the thread handling an exception for good, which every later exception on it chains to."""
    code, at = frame.f_code.co_code, frame.f_lasti
    name = dis.opname[code[at]]
    return name in ("PUSH_EXC_INFO", "POP_EXCEPT") or (
        name == "COPY" and dis.opname[code[at + 2]] == "POP_EXCEPT"
    )


@contextlib.contextmanager
def _tracing(tracer):
    """Trace the block with tracer (sys.settrace), then as before it: a coverage tool's or a debugger's tracer goes on."""
    before = sys.gettrace()
    sys.settrace(tracer)
    try:
        yield
    finally:
        sys.settrace(before)


def _a_handler_runs_at(frame) -> bool:
    """Whether a signal handler can run at frame's opcode: never at a NOP, which Python 3.13 leaves outside every try's range when it opens a try, nor at a NOT_TAKEN, which 3.14 can leave between two of a try's ranges."""
    return dis.opname[frame.f_code.co_code[frame.f_lasti]] not in ("NOP", "NOT_TAKEN")


def _handlers_run_before(frame) -> bool:
    """Whether Python can run a signal's handler just before frame's opcode: as the function starts, at a loop's head, which its back-edge reaches, or as a call returns, a C function's (pthread_sigmask's, say) inside it; never after a load or a store."""
    instructions = list(dis.get_instructions(frame.f_code))
    heads = {
        i.argval
        for i in instructions
        if i.opname in ("JUMP_BACKWARD", "JUMP_ABSOLUTE") and i.argval < i.offset
    }
    if frame.f_lasti in (instructions[0].offset, *heads):
        return True
    previous = [i for i in instructions if i.offset < frame.f_lasti]
    return previous[-1].opname.startswith("CALL")


def _assert_a_raise_after_any_call_keeps_the_mask(case, functions, call, raises: int):
    """Raise KeyboardInterrupt, as a handler would, wherever Python runs handlers in functions (_handlers_run_before), after pthread_sigmask's own call once it has blocked the shutdown signals included, and check that call() leaves the main thread's signal mask as it was each time, and that at least raises of them reached it."""
    before = signal.pthread_sigmask(signal.SIG_BLOCK, [])
    raised = []

    def interrupt(frame):
        if _handlers_run_before(frame):
            raise KeyboardInterrupt

    for k in itertools.count():
        tracer, fired = _opcode_tracer({f.__code__ for f in functions}, k, interrupt)
        try:
            with _tracing(tracer):
                call()
        except KeyboardInterrupt:
            raised.append(k)
        finally:
            mask = signal.pthread_sigmask(signal.SIG_SETMASK, before)
        case.assertEqual(mask, before, k)
        if not fired:
            break
    case.assertGreaterEqual(len(raised), raises)


def _report_callable_identity(fn, connection) -> None:
    """Spawn target proving that `fn` can be restored by module path."""
    connection.send((fn.__module__, fn.__name__))
    connection.close()


class _Channel:
    def close(self):
        pass


@contextlib.contextmanager
def _channel_stand_ins():
    """Stand-ins for grpc.insecure_channel and KymoStub, so neither the client nor its worker opens a real channel; yields the insecure_channel mock."""
    with (
        mock.patch.object(
            client_module.grpc, "insecure_channel", return_value=_Channel()
        ) as open_channel,
        mock.patch.object(
            client_module.kymo_pb2_grpc, "KymoStub", return_value=object()
        ),
    ):
        yield open_channel


class _ImmediateConnectionAttempt:
    def __init__(self, channel=None, stub=None):
        self.result = channel, stub

    def poll(self, deadline_fn=None):
        del deadline_fn
        return self.result

    def cancel(self):
        pass


def _connected(stream_class):
    """Patch kymo.client so the worker's every connect succeeds at once and its streams are stream_class."""
    return mock.patch.multiple(
        client_module,
        _ConnectionAttempt=mock.Mock(
            return_value=_ImmediateConnectionAttempt(_Channel(), object())
        ),
        _BidiStream=stream_class,
    )


class _ListQueue:
    """A queue that takes every group, unless on_put (called with the group) raises; a shutdown closes it with no group left in a feeder."""

    _thread = None

    def __init__(self, on_put=None):
        self.items = []
        self.on_put = on_put
        self._rlock = threading.Lock()

    def put(self, item, block=True, timeout=None):
        if self.on_put is not None:
            self.on_put(item)
        self.items.append(item)

    def cancel_join_thread(self):
        pass

    def close(self):
        pass


class _FullQueue(_ListQueue):
    """A queue with no room, ever."""

    def put(self, item, block=True, timeout=None):
        raise queue.Full


class _OneShotReducible:
    calls = 0

    def __reduce__(self):
        type(self).calls += 1
        if type(self).calls > 1:
            raise RuntimeError("serialized twice")
        return bytes, (b"frozen",)


class _StepLike:
    calls = 0

    def __index__(self):
        type(self).calls += 1
        return 7


class _StringSubclass(str):
    def __reduce__(self):
        raise RuntimeError("caller-owned string reached Queue serialization")


class _StubTensor:
    """Stands in for torch.Tensor (CI has no torch); refuses to pickle while on a device."""

    def __init__(self, device, values):
        self.device = types.SimpleNamespace(type=device)
        self.values = values

    def detach(self):
        return self

    def cpu(self):
        if self.device.type == "meta":
            raise NotImplementedError("Cannot copy out of meta tensor; no data!")
        return _StubTensor("cpu", list(self.values))

    def numpy(self):
        if self.device.type != "cpu":
            raise TypeError("can't convert a device tensor to numpy")
        return np.array(self.values)

    def untyped_storage(self):
        return types.SimpleNamespace(nbytes=lambda: np.array(self.values).nbytes)

    def __reduce__(self):
        if self.device.type not in ("cpu", "meta"):
            raise RuntimeError("a device tensor reached the snapshot pickle")
        return _StubTensor, (self.device.type, self.values)


class CaptureWriterTests(unittest.TestCase):
    def writer(self):
        original = io.StringIO()
        buffer = deque()
        stats = {"size": 0, "dropped": 0}
        writer = capture_module.TeeWriter(original, buffer, stats, threading.RLock())
        return writer, original, buffer, stats

    def test_writelines_captures_a_lazy_iterable(self):
        writer, original, buffer, stats = self.writer()

        result = writer.writelines(line for line in ("alpha", "\n", "beta"))

        self.assertIsNone(result)
        self.assertEqual(original.getvalue(), "alpha\nbeta")
        self.assertEqual(
            capture_module._drain_one(buffer, stats, writer._lock), "alpha\nbeta"
        )

    def test_a_deferred_handler_that_raises_still_lets_the_text_through(self):
        writer, original, buffer, stats = self.writer()

        def exits():
            raise SystemExit(143)

        with (
            mock.patch.multiple(
                capture_module,
                deferred_signals=[[signal.SIGTERM, None]],
                run_deferred_signals=exits,
            ),
            self.assertRaises(SystemExit),
        ):
            writer.write("last words\n")
        self.assertEqual(original.getvalue(), "last words\n")
        self.assertEqual(
            capture_module._drain_one(buffer, stats, writer._lock), "last words\n"
        )

    def test_invalid_writelines_element_does_not_poison_capture(self):
        writer, original, buffer, stats = self.writer()

        with self.assertRaises(TypeError):
            writer.writelines(iter(("ok", b"bad", "never")))

        self.assertEqual(original.getvalue(), "ok")
        self.assertEqual(capture_module._drain_one(buffer, stats, writer._lock), "ok")
        writer.write("after")
        self.assertEqual(
            capture_module._drain_one(buffer, stats, writer._lock), "after"
        )

    def test_surrogateescaped_output_stays_exact_on_terminal_and_encodes_for_upload(
        self,
    ):
        terminal_bytes = io.BytesIO()
        terminal = io.TextIOWrapper(
            terminal_bytes, encoding="utf-8", errors="surrogateescape"
        )
        buffer = deque()
        stats = {"size": 0, "dropped": 0}
        writer = capture_module.TeeWriter(terminal, buffer, stats, threading.RLock())
        output = b"file-\xff\n".decode("utf-8", errors="surrogateescape")

        writer.write(output)
        writer.flush()
        captured = capture_module._drain_one(buffer, stats, writer._lock)

        self.assertEqual(terminal_bytes.getvalue(), b"file-\xff\n")
        self.assertEqual(captured, "file-?\n")
        captured.encode("utf-8")
        chunks = list(
            wire_module._chunk_tuples(
                [numeric(1), ("text_ts", "logs/std_out", 1, captured, 1)]
            )
        )
        self.assertEqual(len(chunks[0]), 2)
        self.assertEqual(chunks[0][1].text_data.decode("utf-8"), captured)

    def test_capture_nested_on_the_thread_holding_its_lock_never_waits(self):
        # A handler runs on the thread it interrupts, which may be inside a write or a drain: a nested write is kept, and a nested drain leaves it for the next drain.
        def holding(lock, body):
            finished = threading.Event()

            def run():
                with lock:
                    body()
                finished.set()

            threading.Thread(target=run, daemon=True).start()
            self.assertTrue(finished.wait(5), "waited on a capture lock it holds")

        for lock in (capture_module._stdout_lock, capture_module._stderr_lock):
            buffer, stats = deque(), {"size": 0, "dropped": 0}
            writer = capture_module.TeeWriter(io.StringIO(), buffer, stats, lock)
            nested = []

            def write_and_drain(writer=writer, buffer=buffer, stats=stats, lock=lock):
                writer.write("tail")
                nested.append(capture_module._drain_one(buffer, stats, lock))

            holding(lock, write_and_drain)
            self.assertEqual(nested, [""])
            self.assertEqual(capture_module._drain_one(buffer, stats, lock), "tail")
        # The shutdown's last drain takes the drain's lock again: a handler that calls finish() inside a drain on the main thread (a nested log() leaves the text for the next drain).
        got = []
        with (
            mock.patch.object(client_module, "_last_capture_step", None),
            mock.patch.object(
                capture_module, "drain_buffers", return_value=("tail", "")
            ),
            mock.patch.object(client_module, "_on_main_thread", return_value=True),
        ):
            holding(
                client_module._capture_drain_lock,
                lambda: got.extend(client_module._drain_capture_points(1, final=True)),
            )
        self.assertEqual(got, [("text_ts", "logs/std_out", 1, "tail", 1)])

    def test_a_drain_off_the_main_thread_before_the_runs_end_waits_for_no_stream_lock(
        self,
    ):
        # The deadlock this closes: the main thread holds a stream lock inside a print (_keep), a drain on another thread (the poller's) holds _capture_drain_lock and asks for that stream lock, and a handler of a signal kymo does not own drains on the main thread inside the print, waiting for _capture_drain_lock. Both drains now finish, and the busy stream's text goes out at the next drain.
        text = ["held by the print\n"]
        with (
            _captured(text, ["err\n"]),
            mock.patch.object(client_module, "_last_capture_step", None),
            mock.patch.object(
                capture_module, "drain_buffers", wraps=capture_module.drain_buffers
            ) as drained,
        ):
            with capture_module._stdout_lock:
                got, finished = _drain_off_main()
                self.assertTrue(finished.wait(5), "waited for the stream lock")
                # Not even briefly: a wait would hold up the handler's log() at each tick.
                drained.assert_called_once_with(False, 0)
                self.assertEqual(got, [[("text_ts", "logs/std_err", 1, "err\n", 1)]])
                self.assertEqual([*capture_module._stdout_buf], text)
                # The handler's drain on the main thread, inside the print, leaves the text too.
                self.assertEqual(client_module._drain_capture_points(2), [])
            points = client_module._drain_capture_points(3)
        self.assertEqual(points, [("text_ts", "logs/std_out", 3, text[0], 3)])

    def test_a_drain_off_the_main_thread_before_the_runs_end_leaves_an_unfinished_line(
        self,
    ):
        # It can land between the writes of one print on another thread (print("step", 1) writes "step", " ", "1" and "\n"), and a merged stdout and stderr read joins a partial line to whatever chunk follows it, so the line waits for the next drain. A stream with no newline at all (a progress bar, a line written over more than a tick) goes out at once, and the run's last drain takes everything.
        with (
            _captured(["done\nstep", " 1"], ["\r 45%|"]),
            mock.patch.object(client_module, "_last_capture_step", None),
        ):
            got, finished = _drain_off_main()
            self.assertTrue(finished.wait(5))
            self.assertEqual(
                got,
                [
                    [
                        ("text_ts", "logs/std_out", 1, "done\n", 1),
                        ("text_ts", "logs/std_err", 1, "\r 45%|", 1),
                    ]
                ],
            )
            self.assertEqual(
                ([*capture_module._stdout_buf], capture_module._stdout_stats["size"]),
                (["step 1"], len("step 1")),
            )
            last, last_done = _drain_off_main(final=True)
            self.assertTrue(last_done.wait(5))
        self.assertEqual(last, [[("text_ts", "logs/std_out", 2, "step 1", 1)]])

    def test_a_drain_off_the_main_thread_waits_for_a_busy_drain_lock_only_at_the_runs_end(
        self,
    ):
        # The main thread may hold _capture_drain_lock in a drain a handler interrupted, or for good where a raising handler left it held on Python 3.14: a tick passes the console over, and the shutdown's last drain waits at most _FINAL_DRAIN_LOCK_WAIT for it, then says what it left unsent.
        for held_past_the_wait in (False, True):
            with self.subTest(held_past_the_wait=held_past_the_wait):
                messages, lock = [], _AskedLock(client_module._capture_drain_lock)
                wait = 0.05 if held_past_the_wait else 5.0
                with (
                    _captured(["held\n"]),
                    mock.patch.object(client_module, "_capture_drain_lock", lock),
                    mock.patch.object(client_module, "_last_capture_step", None),
                    mock.patch.object(client_module, "_FINAL_DRAIN_LOCK_WAIT", wait),
                    mock.patch.object(
                        client_module._log,
                        "warning",
                        side_effect=lambda message, *args: messages.append(
                            message % args
                        ),
                    ),
                ):
                    with lock.real:
                        ticked, tick_done = _drain_off_main()
                        self.assertTrue(
                            tick_done.wait(5), "a tick waited for the drain lock"
                        )
                        last, last_done = _drain_off_main(final=True)
                        if held_past_the_wait:
                            self.assertTrue(
                                last_done.wait(5),
                                "waited for the drain lock past its bound",
                            )
                        else:
                            # The last drain is now waiting for the drain lock.
                            self.assertFalse(last_done.wait(0.1))
                    self.assertTrue(last_done.wait(5))
                    left = list(capture_module._stdout_buf)
                self.assertEqual(lock.asked, [0, wait])
                self.assertEqual(ticked, [[]])
                if held_past_the_wait:
                    self.assertEqual((last, left), ([[]], ["held\n"]))
                    self.assertEqual(
                        messages,
                        [
                            "captured output left unsent: a capture lock stayed busy as the run ended"
                        ],
                    )
                else:
                    self.assertEqual(
                        (last, left, messages),
                        ([[("text_ts", "logs/std_out", 1, "held\n", 1)]], [], []),
                    )

    def test_a_drain_nested_in_a_drain_on_the_main_thread_leaves_the_text_for_the_next(
        self,
    ):
        # A handler that prints and calls log() inside a drain on its thread, at each opcode of the drain's step code (a superset of where handlers run): both drains would take one step, of which the server keeps one text point. The text goes out once, in order, each point at a step of its own.
        codes = _codes_of(client_module._drain_capture_locked)
        for k in itertools.count():
            published = []

            def nested(frame):
                capture_module._stdout_buf.append("nested\n")
                capture_module._stdout_stats["size"] += len("nested\n")
                published.extend(client_module._drain_capture_points(1))

            tracer, fired = _opcode_tracer(codes, k, nested)
            # Both at time 1, after a step of 0: each would take step 1.
            with (
                _captured(["outer\n"]),
                mock.patch.object(client_module, "_last_capture_step", 0),
            ):
                with _tracing(tracer):
                    published.extend(client_module._drain_capture_points(1))
                if not fired:
                    break
                published.extend(client_module._drain_capture_points(1))
            published.sort(key=lambda point: point[2])
            state = f"nested at opcode {k}"
            self.assertEqual(
                "".join(point[3] for point in published), "outer\nnested\n", state
            )
            self.assertEqual(
                len({point[2] for point in published}), len(published), state
            )
        self.assertGreater(k, 10)

    def test_the_shutdowns_last_drain_off_the_main_thread_waits_for_a_print_but_not_forever(
        self,
    ):
        # None follows it, so it waits for a print to end, but at most _FINAL_DRAIN_LOCK_WAIT: the print may be one a handler interrupted to call log(), which waits for the drain's lock.
        real = capture_module._stdout_lock

        def warn(message, *args):
            # As a handler the program added that writes to the captured stream still held: it waits for the print.
            messages.append(message % args)
            warned.set()
            capture_module.TeeWriter(
                io.StringIO(), deque(), {"size": 0, "dropped": 0}, real
            ).write("warned\n")

        for held_past_the_wait in (False, True):
            with self.subTest(held_past_the_wait=held_past_the_wait):
                lock, warned, messages = _AskedLock(real), threading.Event(), []
                wait = 0.05 if held_past_the_wait else 5.0
                with (
                    _captured(["tail\n"]),
                    mock.patch.object(capture_module, "_stdout_lock", lock),
                    mock.patch.object(client_module, "_last_capture_step", None),
                    mock.patch.object(client_module, "_FINAL_DRAIN_LOCK_WAIT", wait),
                    mock.patch.object(client_module._log, "warning", side_effect=warn),
                ):
                    with real:
                        got, finished = _drain_off_main(final=True)
                        self.assertTrue(lock.reached.wait(5))
                        if held_past_the_wait:
                            # Text left unsent is said so, out of the drain's lock: the handler that interrupted the print can drain.
                            self.assertTrue(
                                warned.wait(5), "the last drain waited past its bound"
                            )
                            self.assertTrue(
                                client_module._capture_drain_lock.acquire(timeout=1),
                                "warned holding the drain's lock",
                            )
                            client_module._capture_drain_lock.release()
                        else:
                            # The drain is now waiting for the print to end.
                            self.assertFalse(finished.wait(0.1))
                    self.assertTrue(finished.wait(5))
                    left = list(capture_module._stdout_buf)
                self.assertEqual(lock.asked, [wait])
                self.assertEqual(
                    messages,
                    [
                        "captured stdout left unsent: a capture lock stayed busy as the run ended"
                    ]
                    if held_past_the_wait
                    else [],
                )
                if held_past_the_wait:
                    self.assertEqual((got, left), ([[]], ["tail\n"]))
                else:
                    self.assertEqual(
                        (got, left),
                        ([[("text_ts", "logs/std_out", 1, "tail\n", 1)]], []),
                    )

    def test_the_shutdowns_last_drain_takes_the_text_of_a_print_it_interrupts(self):
        # A handler installed after init() that calls finish() as it interrupts a print, inside its buffering (the stream's lock held on this thread): a nested drain leaves the text for the next drain, but none follows the shutdown's last, which takes it in place (that no later drain takes it again: test_nested_capture_at_every_opcode_neither_duplicates_nor_loses_text).
        text = ["captured since the last log()\n", "the print a signal interrupts\n"]
        target = _ListQueue()
        with (
            _upload_state(_metric_queue=target),
            _captured(text),
            capture_module._stdout_lock,
        ):
            client_module._publish_queue_items([], capture_ms=1)
            self.assertEqual(target.items, [])
            client_module._publish_queue_items([], capture_ms=2, final=True)
        self.assertEqual(
            target.items, [[("text_ts", "logs/std_out", 2, "".join(text), 2)]]
        )

    def test_a_pending_drop_marker_counts_as_output(self):
        buffer, stats = deque(), {"size": 0, "dropped": 5}
        with mock.patch.multiple(
            capture_module,
            _stdout_buf=buffer,
            _stdout_stats=stats,
            _stderr_buf=deque(),
            _stderr_stats={"size": 0, "dropped": 0},
        ):
            self.assertTrue(capture_module.has_output())
            self.assertEqual(
                capture_module.drain_buffers()[0],
                "[kymo: 5 chars of captured output dropped]\n",
            )
            self.assertFalse(capture_module.has_output())

    def test_nested_capture_at_every_opcode_neither_duplicates_nor_loses_text(self):
        # A handler that prints or calls log() can run between any two bytecodes of a write or a drain on its thread. Inject one at each opcode of the outer operation (a superset of where handlers run) and check: no text published twice, none lost unless counted in a drop marker, no drop while under the cap, and the size never above the buffer's real length, and exact where handlers run (_handlers_run_before). A handler that calls finish(), whose last drain takes the text in place, only there.
        cap = 10
        marker = re.compile(r"\[kymo: (\d+) chars of captured output dropped\]\n")
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        letters = iter(alphabet)

        def unique(length):
            return "".join(next(letters) for _ in range(length))

        def scenarios():
            # (name, pre-filled chunk lengths, outer operation and its write's length, whether a 2-character nested print can pass the cap)
            yield "write into room", (2,), ("write", 4), False
            yield "write past the cap", (8,), ("write", 4), True
            yield "drain", (2, 2, 2), ("drain", None), False
            yield "drain at the cap", (6, 4), ("drain", None), True

        keep_codes = (capture_module.TeeWriter._keep,)
        drain_codes = (capture_module._drain_one, capture_module._take)
        runs = 0
        for name, prefill, (outer, length), over in scenarios():
            for nested_kind in ("print", "log", "finish"):
                codes = _codes_of(*(keep_codes if outer == "write" else drain_codes))
                for k in itertools.count():
                    letters = iter(alphabet)
                    buffer = deque()
                    stats = {"size": 0, "dropped": 0}
                    lock = threading.RLock()
                    writer = capture_module.TeeWriter(
                        io.StringIO(), buffer, stats, lock
                    )
                    written, published = [], []
                    with mock.patch.object(capture_module, "_max_buffered_chars", cap):
                        for size in prefill:
                            chunk = unique(size)
                            written.append(chunk)
                            writer.write(chunk)
                        busy, anywhere = [False], [False]

                        def nested(frame):
                            if not _handlers_run_before(frame):
                                if nested_kind == "finish":
                                    return
                                anywhere[0] = True
                            busy[0] = True
                            try:
                                if nested_kind == "print":
                                    chunk = unique(2)
                                    written.append(chunk)
                                    writer.write(chunk)
                                else:
                                    published.append(
                                        capture_module._drain_one(
                                            buffer, stats, lock, nested_kind == "finish"
                                        )
                                    )
                            finally:
                                busy[0] = False

                        tracer, fired = _opcode_tracer(
                            codes, k, nested, lambda: busy[0]
                        )
                        with _tracing(tracer):
                            if outer == "write":
                                chunk = unique(length)
                                written.append(chunk)
                                writer.write(chunk)
                            else:
                                published.append(
                                    capture_module._drain_one(buffer, stats, lock)
                                )
                        state = f"{name}, nested {nested_kind} at opcode {k}"
                        self.assertLessEqual(
                            stats["size"], sum(map(len, buffer)), state
                        )
                        if not anywhere[0]:
                            self.assertEqual(
                                stats["size"], sum(map(len, buffer)), state
                            )
                        published.append(capture_module._drain_one(buffer, stats, lock))
                    runs += 1
                    dropped = sum(
                        int(n) for text in published for n in marker.findall(text)
                    )
                    text = "".join(marker.sub("", text) for text in published)
                    total = "".join(written)
                    self.assertEqual(len(set(text)), len(text), f"{state}: duplicated")
                    self.assertTrue(set(text) <= set(total), state)
                    self.assertEqual(len(text) + dropped, len(total), f"{state}: lost")
                    if not over:
                        self.assertEqual(dropped, 0, f"{state}: dropped under the cap")
                    self.assertEqual((list(buffer), stats["dropped"]), ([], 0), state)
                    if not fired:
                        break
                self.assertGreater(k, 10, f"{name}: too few opcodes traced")
        self.assertGreater(runs, 100)

    def test_capture_points_share_the_supplied_timestamp(self):
        with (
            mock.patch.object(client_module, "_last_capture_step", None),
            mock.patch.object(
                capture_module, "drain_buffers", return_value=("stdout", "stderr")
            ),
        ):
            points = client_module._drain_capture_points(1234)

        self.assertEqual(
            points,
            [
                ("text_ts", "logs/std_out", 1234, "stdout", 1234),
                ("text_ts", "logs/std_err", 1234, "stderr", 1234),
            ],
        )

    def test_capture_points_use_unique_steps_within_one_millisecond(self):
        with (
            mock.patch.object(client_module, "_last_capture_step", None),
            mock.patch.object(
                capture_module,
                "drain_buffers",
                side_effect=(("first", ""), ("", ""), ("second", "")),
            ),
        ):
            first = client_module._drain_capture_points(1234)
            empty = client_module._drain_capture_points(1234)
            second = client_module._drain_capture_points(1234)

        self.assertEqual(first, [("text_ts", "logs/std_out", 1234, "first", 1234)])
        self.assertEqual(empty, [])
        self.assertEqual(second, [("text_ts", "logs/std_out", 1235, "second", 1234)])

    def test_client_diagnostics_are_not_recaptured_as_user_stderr(self):
        script = r"""
import sys

import kymo
from kymo import _capture
from kymo._log import logger

_capture.drain_buffers()
logger.warning("internal-only-diagnostic")
sys.stderr.write("user-stderr\n")
_stdout, stderr = _capture.drain_buffers()
assert "internal-only-diagnostic" not in stderr, repr(stderr)
assert stderr == "user-stderr\n", repr(stderr)
"""
        result = _run_script_file(script)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("kymo: internal-only-diagnostic", result.stderr)
        self.assertIn("user-stderr", result.stderr)

    def test_import_preserves_missing_standard_streams(self):
        script = r"""
import json
import os
import sys

sys.stdout = None
sys.stderr = None

import kymo
from kymo import _capture
from kymo._log import logger

assert sys.stdout is None
assert sys.stderr is None
print("detached-stdout")
logger.warning("detached-stderr")
assert _capture.drain_buffers() == ("", "")
os.write(1, json.dumps("ok").encode())
"""
        result = _run_script_file(script)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, '"ok"')
        self.assertEqual(result.stderr, "")

    @unittest.skipUnless(hasattr(os, "fork"), "requires os.fork")
    def test_a_fork_child_takes_the_locks_another_thread_held(self):
        # A thread holds kymo's capture and client locks and the accounting lock across the fork: the fork must not wait for them, and the child must find them free and none of the parent's pending capture.
        script = r"""
import multiprocessing
import os
import signal
import sys
import threading
import warnings

import kymo.client as client
from kymo import _capture

# Else the parent's fork warning, printed before os.fork() returns, would wait for the stderr lock the thread holds until then.
warnings.simplefilter("ignore", DeprecationWarning)
held, forked = threading.Event(), threading.Event()
print("parent")
print("parent", file=sys.stderr)
_capture._stdout_stats["dropped"] = _capture._stderr_stats["dropped"] = 3
client._queue_status = multiprocessing.Value("q", 0)


def hold():
    with _capture._stdout_lock, _capture._stderr_lock, client._capture_drain_lock, client._unproven_lock, client._pending_lock, client._gallery_lock, client._queue_status.get_lock():
        held.set()
        forked.wait()


thread = threading.Thread(target=hold)
thread.start()
held.wait()
pid = os.fork()
if pid == 0:
    signal.alarm(15)
    print("out")
    print("err", file=sys.stderr)
    with client._pending_lock, client._gallery_lock, client._unproven_lock:
        pass
    # Shared with the parent: the child waits for the thread's release, and its own must free it.
    client._charge_backlog(1, None)
    texts = [point[3] for point in client._drain_capture_points(1)]
    os._exit(0 if texts == ["out\n", "err\n"] else 1)
forked.set()
thread.join()
_, status = os.waitpid(pid, 0)
assert os.waitstatus_to_exitcode(status) == 0, status
client._charge_backlog(1, None)
assert client._queue_status.get_obj().value == 2
pending = "[kymo: 3 chars of captured output dropped]\nparent\n"
assert _capture.drain_buffers() == (pending, pending)
"""
        result = _run_script_file(script)

        self.assertEqual(result.returncode, 0, result.stderr)

    @unittest.skipUnless(hasattr(os, "fork"), "requires os.fork")
    def test_a_fork_child_releases_the_locks_its_forking_thread_held(self):
        script = r"""
import os

import kymo.client as client
from kymo import _capture

# As a signal handler forking inside a print, or inside a section that takes the pending lock by name (wait_for_upload()), would: the child ends the section, which releases its lock.
client._pending_lock.acquire()
with _capture._stdout_lock:
    pid = os.fork()
client._pending_lock.release()
if pid == 0:
    os._exit(0)
_, status = os.waitpid(pid, 0)
assert os.waitstatus_to_exitcode(status) == 0, status
"""
        result = _run_script_file(script)

        self.assertEqual(result.returncode, 0, result.stderr)

    @unittest.skipUnless(hasattr(os, "fork"), "requires os.fork")
    def test_the_fork_warning_waits_for_a_thread_logging_through_the_fork(self):
        # Python 3.12 prints a threaded process's fork warning before the parent's after-fork hooks run, so only 3.12 can fail this: under logging.captureWarnings the print waits for the handler lock, which a thread logging during the fork holds while it writes to stderr.
        script = r"""
import logging
import os
import sys
import threading
import warnings

# Shown whatever the interpreter's -W options.
warnings.simplefilter("default", DeprecationWarning)
go, holding, forked = threading.Event(), threading.Event(), threading.Event()


def let_it_log():
    go.set()
    holding.wait()


# Before-fork hooks run last registered first, so this one would run after any kymo added (it adds none): the thread would take the handler lock while those held kymo's locks.
os.register_at_fork(before=let_it_log)
import kymo  # noqa: E402

logging.basicConfig()
logging.captureWarnings(True)
handler = logging.getLogger().handlers[0]


def log_through_the_fork():
    go.wait()
    with handler.lock:
        holding.set()
        sys.stderr.write("logged during the fork\n")
    forked.wait()


thread = threading.Thread(target=log_through_the_fork)
thread.start()
pid = os.fork()
if pid == 0:
    os._exit(0)
forked.set()
thread.join()
os.waitpid(pid, 0)
"""
        result = _run_script_file(script)

        self.assertEqual(result.returncode, 0, result.stderr)


class InitApiTests(unittest.TestCase):
    def test_init_run_keys_reject_only_exact_browser_dot_segments(self):
        for project_id, run_id in (
            (".", "run"),
            ("..", "run"),
            ("project", "."),
            ("project", ".."),
        ):
            with self.subTest(project_id=project_id, run_id=run_id):
                with self.assertRaisesRegex(ValueError, "normalize URL path"):
                    client_module._validate_routeable_run_key(project_id, run_id)

        client_module._validate_routeable_run_key(".project", "run")
        client_module._validate_routeable_run_key("project", "run..")

    def test_signal_setup_preserves_ignored_dispositions(self):
        with (
            mock.patch.object(client_module, "_TERMINATION_SIGNALS", (15, 2)),
            mock.patch.object(
                client_module.signal,
                "getsignal",
                side_effect=(
                    client_module.signal.SIG_IGN,
                    client_module.signal.SIG_DFL,
                ),
            ),
            mock.patch.object(client_module.signal, "signal") as install,
            mock.patch.object(client_module, "_original_signals", {}),
        ):
            client_module._setup_signal_handlers()

        install.assert_called_once_with(2, client_module._signal_handler)

    def test_returning_signal_handler_leaves_the_run_active(self):
        original = mock.Mock()
        with (
            mock.patch.object(client_module, "_original_signals", {15: original}),
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_exit_code", None),
            mock.patch.object(client_module, "_drain_and_shutdown") as drain,
        ):
            client_module._signal_handler(15, None)
            self.assertTrue(client_module._is_initialized)
            self.assertIsNone(client_module._exit_code)

        original.assert_called_once_with(15, None)
        drain.assert_not_called()

    def test_post_init_legacy_timeout_names_are_loud_but_exit_paths_still_drain(self):
        with (
            mock.patch.dict(os.environ, {"MKDB2_FLUSH_TIMEOUT": "3"}),
            self.assertRaisesRegex(
                ValueError, "MKDB2_FLUSH_TIMEOUT.*KYMO_FLUSH_TIMEOUT"
            ),
        ):
            client_module._default_flush_timeout()
        for flush_timeout in (None, 7.0):
            with (
                self.subTest(flush_timeout=flush_timeout),
                mock.patch.dict(os.environ, {"MKDB2_FLUSH_TIMEOUT": "3"}),
                mock.patch.object(client_module, "_is_initialized", False),
                mock.patch.object(client_module, "_drain_and_shutdown") as drain,
                self.assertRaisesRegex(
                    ValueError, "MKDB2_FLUSH_TIMEOUT.*KYMO_FLUSH_TIMEOUT"
                ),
            ):
                client_module.finish(flush_timeout=flush_timeout)
            drain.assert_not_called()
        with (
            mock.patch.dict(os.environ, {"MKDB2_SIGNAL_FLUSH_TIMEOUT": "3"}),
            mock.patch.object(client_module, "_original_signals", {}),
            mock.patch.object(client_module, "_exit_code", None),
            mock.patch.object(client_module, "_terminated_at", None),
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_drain_and_shutdown") as drain,
            mock.patch.object(client_module.sys, "exit", side_effect=SystemExit(143)),
            self.assertLogs("kymo", level="ERROR") as logs,
            self.assertRaisesRegex(SystemExit, "143"),
        ):
            client_module._signal_handler(15, None)
        drain.assert_called_once_with(
            flush_timeout=client_module._SIGNAL_FLUSH_TIMEOUT, signum=15
        )
        self.assertIn("MKDB2_SIGNAL_FLUSH_TIMEOUT", "\n".join(logs.output))

        with (
            mock.patch.dict(os.environ, {"MKDB2_FLUSH_TIMEOUT": "3"}),
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.multiple(
                client_module,
                _exit_code=None,
                _interrupted_at=None,
                _terminated_at=None,
            ),
            mock.patch.object(client_module, "_drain_and_shutdown") as drain,
            self.assertLogs("kymo", level="ERROR") as logs,
        ):
            client_module._ensure_metrics_uploaded()
        drain.assert_called_once_with(
            flush_timeout=client_module._DEFAULT_FLUSH_TIMEOUT, at_exit=True
        )
        self.assertIn("MKDB2_FLUSH_TIMEOUT", "\n".join(logs.output))

    def test_an_atexit_flush_aborted_with_the_run_active_ends_it(self):
        # An exception that ends the atexit flush before it claims its shutdown would leave the run active, its worker, which carries on through a Ctrl-C and ignores a SIGTERM the program owns, to multiprocessing's exit hook, which joins it forever. The flush ends the run before the exception propagates: a Ctrl-C's hands it off, and anything else, a SIGTERM handler's exit, say, flushes within the signal budget from that signal. One whose shutdown ended the run raises alone.
        def interrupted():
            raise KeyboardInterrupt

        def handler_exits():
            raise SystemExit(0)

        def ends_then_raises(**kwargs):
            if kwargs.get("flush_timeout") == 60.0:
                client_module._is_initialized = False
                raise KeyboardInterrupt

        now = time.monotonic()
        for name, patches, error, calls in (
            (
                "a Ctrl-C before its claim",
                {"_default_flush_timeout": interrupted},
                KeyboardInterrupt,
                [{"signum": signal.SIGINT, "at_exit": True}],
            ),
            (
                "a SIGTERM handler's exit before its claim",
                {"_default_flush_timeout": handler_exits, "_terminated_at": now - 1},
                SystemExit,
                [{"flush_timeout": 2.0, "at_exit": True}],
            ),
            (
                "after its shutdown",
                {},
                KeyboardInterrupt,
                [{"flush_timeout": 60.0, "at_exit": True}],
            ),
        ):
            drain = mock.Mock(side_effect=ends_then_raises)
            with (
                self.subTest(name),
                mock.patch.dict(
                    os.environ,
                    {"KYMO_FLUSH_TIMEOUT": "60", "KYMO_SIGNAL_FLUSH_TIMEOUT": "3"},
                ),
                mock.patch.multiple(
                    client_module,
                    **{
                        "_is_initialized": True,
                        "_exit_code": None,
                        "_interrupted_at": None,
                        "_worker_interrupted_at": None,
                        "_terminated_at": None,
                        "_drain_and_shutdown": drain,
                        **patches,
                    },
                ),
                self.assertRaises(error),
            ):
                client_module._ensure_metrics_uploaded()
            # To the second: the SIGTERM's budget runs from 1 s before the flush.
            got = [
                {**call.kwargs, "flush_timeout": round(call.kwargs["flush_timeout"])}
                if "flush_timeout" in call.kwargs
                else call.kwargs
                for call in drain.call_args_list
            ]
            self.assertEqual(got, calls)

    def test_terminating_signal_handler_records_its_exit(self):
        def terminate(_signum, _frame):
            raise SystemExit(7)

        with (
            mock.patch.object(client_module, "_original_signals", {15: terminate}),
            mock.patch.object(client_module, "_exit_code", None),
            mock.patch.object(client_module, "_drain_and_shutdown") as drain,
        ):
            with self.assertRaisesRegex(SystemExit, "7"):
                client_module._signal_handler(15, None)

            self.assertEqual(client_module._exit_code, 7)
            drain.assert_not_called()

    def test_a_caught_ctrl_c_leaves_the_run_active(self):
        # A training loop that catches KeyboardInterrupt and carries on: the program keeps Python's KeyboardInterrupt, and the run goes on.
        caught = []
        with (
            mock.patch.multiple(
                client_module,
                _is_initialized=True,
                _exit_code=None,
                _handed_off=False,
                _interrupted_at=None,
            ),
            mock.patch.object(client_module, "_drain_and_shutdown") as shutdown,
            _kymo_handles({signal.SIGINT: signal.default_int_handler}),
        ):
            try:
                signal.raise_signal(signal.SIGINT)
            except KeyboardInterrupt:
                caught.append(True)
            self.assertTrue(client_module._is_initialized)
            self.assertIsNone(client_module._exit_code)
            self.assertFalse(client_module._handed_off)
            self.assertIsNotNone(client_module._interrupted_at)
        self.assertEqual(caught, [True])
        shutdown.assert_not_called()

    def test_uncaught_keyboard_interrupt_records_the_shell_exit_code(self):
        # Not one the interactive interpreter carries on from, as from any Ctrl-C a program carries on from: the run goes on, and no later exit is taken to follow that Ctrl-C.
        with (
            mock.patch.multiple(client_module, _exit_code=None, _interrupted_at=None),
            mock.patch.object(client_module, "_original_excepthook"),
            mock.patch.object(client_module, "_drain_and_shutdown"),
        ):
            client_module._excepthook(KeyboardInterrupt, KeyboardInterrupt(), None)
            self.assertEqual(client_module._exit_code, 130)
            client_module._exit_code = None
            client_module._interrupted_at = time.monotonic()
            with mock.patch.object(client_module.sys, "ps1", ">>> ", create=True):
                client_module._excepthook(KeyboardInterrupt, KeyboardInterrupt(), None)
                # Nor any other exception it carries on from, which recorded 1 for a later finish() to end the run with.
                client_module._excepthook(ValueError, ValueError(), None)
            self.assertIsNone(client_module._exit_code)
            self.assertIsNone(client_module._interrupted_at)
            client_module._excepthook(ValueError, ValueError(), None)
            self.assertEqual(client_module._exit_code, 1)

    def test_python_i_and_pythoninspect_carry_on_into_the_prompt(self):
        # After the script's uncaught exception the interpreter carries on into its prompt under python -i, or with PYTHONINSPECT set, at start or by the program, and a terminal on stdin, as CPython's main decides: a Ctrl-C there is one the program carries on from.
        for ps1, inspecting, interactive, variable, terminal, carries_on in (
            (False, 0, 0, None, True, False),
            (True, 0, 0, None, False, True),
            (False, 1, 1, None, False, True),
            (False, 1, 0, None, True, True),
            (False, 1, 0, None, False, False),
            (False, 0, 0, "1", True, True),
            (False, 0, 0, "1", False, False),
        ):
            interpreter = types.SimpleNamespace(
                flags=types.SimpleNamespace(
                    inspect=inspecting, interactive=interactive, ignore_environment=0
                )
            )
            if ps1:
                interpreter.ps1 = ">>> "
            environ = {k: v for k, v in os.environ.items() if k != "PYTHONINSPECT"}
            if variable is not None:
                environ["PYTHONINSPECT"] = variable
            with (
                self.subTest(ps1=ps1, inspect=inspecting, variable=variable),
                mock.patch.object(client_module, "sys", interpreter),
                mock.patch.dict(os.environ, environ, clear=True),
                mock.patch.object(client_module.os, "isatty", return_value=terminal),
            ):
                self.assertEqual(client_module._carries_on_interactively(), carries_on)

    def test_an_uncaught_ctrl_c_is_handed_off_in_the_exception_hook(self):
        # Before the program's own hook and the atexit hooks, which another library's may hold up, as is an error raised while one was handled or soon after a Ctrl-C kymo saw; not in the interactive interpreter, which carries on after showing it, nor for another exception.
        calls = []
        chained = RuntimeError("Aborted!")
        chained.__cause__ = KeyboardInterrupt()
        with (
            mock.patch.multiple(client_module, _exit_code=None, _interrupted_at=None),
            mock.patch.object(
                client_module,
                "_original_excepthook",
                lambda *args: calls.append("program's hook"),
            ),
            mock.patch.object(
                client_module,
                "_drain_and_shutdown",
                side_effect=lambda **kwargs: calls.append(kwargs),
            ),
        ):
            client_module._excepthook(KeyboardInterrupt, KeyboardInterrupt(), None)
            client_module._excepthook(RuntimeError, chained, None)
            self.assertEqual(calls, [{"signum": signal.SIGINT}, "program's hook"] * 2)
            calls.clear()
            with mock.patch.object(client_module.sys, "ps1", ">>> ", create=True):
                client_module._excepthook(KeyboardInterrupt, KeyboardInterrupt(), None)
            client_module._excepthook(ValueError, ValueError(), None)
            self.assertEqual(calls, ["program's hook", "program's hook"])
            calls.clear()
            client_module._interrupted_at = time.monotonic()
            client_module._excepthook(ValueError, ValueError(), None)
            self.assertEqual(calls, [{"signum": signal.SIGINT}, "program's hook"])

    def test_an_exit_after_a_ctrl_c_hands_the_run_off(self):
        # Lightning's and click's sys.exit(1), or an `except KeyboardInterrupt: sys.exit(0)`: an exit that follows a Ctrl-C the program did not carry on from hands the run off before the atexit hooks, and the process keeps its own code. So does one raised while an exception chained from one is handled (click's Abort), or one soon after a Ctrl-C kymo saw; not one on another thread, which ends only that thread, nor one long after the Ctrl-C.
        shutdowns, exits = [], []

        def exit_handling(error, code):
            try:
                raise error
            except BaseException:
                client_module._exit_wrapper(code)

        def abort():
            try:
                raise KeyboardInterrupt
            except KeyboardInterrupt as error:
                raise RuntimeError("Aborted!") from error

        with (
            mock.patch.multiple(
                client_module,
                _exit_code=None,
                _exit_code_by_sys_exit=False,
                _interrupted_at=None,
                _original_sys_exit=exits.append,
            ),
            mock.patch.object(
                client_module,
                "_drain_and_shutdown",
                side_effect=lambda **kwargs: shutdowns.append(kwargs),
            ),
            mock.patch.dict(os.environ, {"KYMO_SIGNAL_FLUSH_TIMEOUT": "3"}),
        ):
            exit_handling(KeyboardInterrupt(), 1)
            try:
                abort()
            except RuntimeError:
                client_module._exit_wrapper(1)
            client_module._exit_wrapper(0)
            self.assertEqual(shutdowns, [{"signum": signal.SIGINT}] * 2)
            client_module._interrupted_at = time.monotonic() - 2
            client_module._exit_wrapper(0)
            client_module._interrupted_at = time.monotonic() - 4
            client_module._exit_wrapper(0)
            thread = threading.Thread(
                target=exit_handling, args=(KeyboardInterrupt(), 3)
            )
            thread.start()
            thread.join()
            self.assertEqual(shutdowns, [{"signum": signal.SIGINT}] * 3)
            # The main thread's last exit, not the thread's.
            self.assertEqual(client_module._exit_code, 0)
        self.assertEqual(exits, [1, 1, 0, 0, 0, 3])

    @contextlib.contextmanager
    def _finishing_run(self):
        """An active run whose shutdown is mocked to return the exit code it would send."""
        with (
            mock.patch.multiple(
                client_module,
                _is_initialized=True,
                _exit_code=None,
                _exit_code_by_sys_exit=False,
                _interrupted_at=None,
                _terminated_at=None,
                _worker_interrupted_at=None,
                _original_sys_exit=lambda code: None,
                _original_excepthook=lambda *args: None,
            ),
            mock.patch.object(
                client_module,
                "_drain_and_shutdown",
                side_effect=lambda *args, **kwargs: client_module._exit_code,
            ),
        ):
            yield

    def test_a_code_only_sys_exit_recorded_gives_way_to_a_later_exit_a_signal_or_the_hook(
        self,
    ):
        # A later sys.exit's, the exception hook's and a signal's, during a shutdown too: a Ctrl-C's replaces any code. No later sys.exit replaces the hook's or a signal's.
        def signal_during_a_shutdown(signum):
            shutdown = types.SimpleNamespace(
                signum=None,
                exiting=None,
                ended=None,
                bound=lambda end: None,
                hand_off=lambda: None,
            )
            with (
                self.assertLogs("kymo", level="WARNING")
                if signum == signal.SIGTERM
                else contextlib.nullcontext()
            ):
                client_module._signal_during_shutdown(shutdown, signum)

        with self._finishing_run():
            client_module._exit_wrapper(7)
            client_module._exit_wrapper(0)
            self.assertEqual(client_module._exit_code, 0)
            client_module._excepthook(ValueError, ValueError(), None)
            client_module._exit_wrapper(3)
            self.assertEqual(client_module._exit_code, 1)
            for signum in (signal.SIGINT, signal.SIGTERM):
                client_module._exit_code = None
                client_module._exit_wrapper(7)
                signal_during_a_shutdown(signum)
                client_module._exit_wrapper(0)
                self.assertEqual(client_module._exit_code, 128 + signum)
            # Once the interpreter's exit has begun (threading's atexit hooks, where the main thread still reads as alive, then the atexit hooks), the exit that ended the program stays: neither a sys.exit there, which Python ignores, nor a SIGTERM replaces it. After a normal end such an exit records no code.
            for stage in (
                mock.patch.object(threading, "_SHUTTING_DOWN", True),
                mock.patch.object(
                    threading.main_thread(), "is_alive", return_value=False
                ),
            ):
                client_module._exit_code = None
                client_module._exit_wrapper(7)
                with stage:
                    client_module._exit_wrapper(0)
                    signal_during_a_shutdown(signal.SIGTERM)
                    self.assertEqual(client_module._exit_code, 7)
                    client_module._exit_code = None
                    client_module._exit_wrapper(3)
                    self.assertIsNone(client_module._exit_code)

    def test_a_finish_after_a_caught_exit_drops_its_code(self):
        # A sys.exit the program catches (argparse inside a try, a notebook cell), then finish() on the main thread with no exception being handled: the run ends without that code. One called while the SystemExit propagates or is handled, or on another thread (a helper's, or an executor worker's, as the program exits), keeps it, and nothing drops the hook's code.
        codes = []

        def finish_elsewhere():
            thread = threading.Thread(
                target=lambda: codes.append(client_module.finish())
            )
            thread.start()
            thread.join()

        with self._finishing_run():
            client_module._exit_wrapper(7)
            codes.append(client_module.finish())
            client_module._exit_wrapper(7)
            with self.assertRaises(SystemExit):
                try:
                    raise SystemExit(7)
                finally:
                    codes.append(client_module.finish())
            try:
                raise SystemExit(7)
            except SystemExit:
                codes.append(client_module.finish())
            finish_elsewhere()
            # Nor once the interpreter's exit has begun, in threading's atexit hooks or the atexit hooks, where the code may be the exit that ended the program.
            with mock.patch.object(threading, "_SHUTTING_DOWN", True):
                codes.append(client_module.finish())
            with mock.patch.object(
                threading.main_thread(), "is_alive", return_value=False
            ):
                codes.append(client_module.finish())
            # The hook's 1 replaces the caught exit's code, and stays.
            client_module._excepthook(ValueError, ValueError(), None)
            codes.append(client_module.finish())
        self.assertEqual(codes, [None, 7, 7, 7, 7, 7, 1])

    def test_a_ctrl_c_only_the_worker_saw_times_the_runs_end(self):
        # A SIGINT handler the program installed after init() takes the Ctrl-C (one that exits, or SIG_DFL's kill), which kymo's never sees; the upload worker, which the terminal's Ctrl-C reaches with the rest of the group, notes it. An exit soon after it hands the run off, by the cap from the later Ctrl-C; one long after it, or after the interactive interpreter carried on from it, does not.
        noted = types.SimpleNamespace(value=0.0)
        shutdowns, exits = [], []
        with (
            mock.patch.multiple(
                client_module,
                _exit_code=None,
                _interrupted_at=None,
                _worker_interrupted_at=noted,
                _original_sys_exit=exits.append,
                _original_excepthook=lambda *args: None,
            ),
            mock.patch.object(
                client_module,
                "_drain_and_shutdown",
                side_effect=lambda **kwargs: shutdowns.append(kwargs),
            ),
            mock.patch.dict(os.environ, {"KYMO_SIGNAL_FLUSH_TIMEOUT": "3"}),
        ):
            client_module._exit_wrapper(0)
            noted.value = time.monotonic() - 2
            client_module._exit_wrapper(0)
            self.assertEqual(shutdowns, [{"signum": signal.SIGINT}])
            self.assertAlmostEqual(
                client_module._handoff_cap(), noted.value + 3, delta=0.01
            )
            client_module._interrupted_at = noted.value - 0.5
            self.assertAlmostEqual(
                client_module._handoff_cap(), noted.value + 3, delta=0.01
            )
            client_module._interrupted_at = None
            noted.value = time.monotonic() - 4
            client_module._exit_wrapper(0)
            noted.value = time.monotonic()
            with mock.patch.object(client_module.sys, "ps1", ">>> ", create=True):
                client_module._excepthook(KeyboardInterrupt, KeyboardInterrupt(), None)
            self.assertEqual(noted.value, 0.0)
            client_module._exit_wrapper(0)
        self.assertEqual(shutdowns, [{"signum": signal.SIGINT}])
        self.assertEqual(exits, [0, 0, 0, 0])

    def test_an_interactive_session_carries_on_from_every_ctrl_c(self):
        # IPython and Jupyter show a cell's KeyboardInterrupt without sys.excepthook, and the 3.13+ REPL handles one at its prompt itself, so both notes stand (a notebook's interrupt reaches the worker with the kernel's group): finish() runs an ordinary shutdown, and an exit, even one handling the Ctrl-C (an `except KeyboardInterrupt: sys.exit(0)` in a cell), hands nothing off.
        now = time.monotonic()
        shutdowns, exits = [], []
        with (
            mock.patch.multiple(
                client_module,
                _is_initialized=True,
                _exit_code=None,
                _interrupted_at=now,
                _worker_interrupted_at=types.SimpleNamespace(value=now),
                _terminated_at=None,
                _original_sys_exit=exits.append,
            ),
            mock.patch.object(
                client_module,
                "_drain_and_shutdown",
                side_effect=lambda *args, **kwargs: shutdowns.append(kwargs),
            ),
            mock.patch.object(client_module.sys, "ps1", ">>> ", create=True),
        ):
            try:
                raise KeyboardInterrupt
            except KeyboardInterrupt:
                client_module._exit_wrapper(0)
                client_module.finish()
            client_module._ensure_metrics_uploaded()
        self.assertEqual(shutdowns, [{}, {"flush_timeout": mock.ANY, "at_exit": True}])
        self.assertEqual(exits, [0])

    def test_another_threads_exit_never_sets_the_runs_code(self):
        # A thread's sys.exit() ends that thread alone: the run ends with the program's code, its crash's 1 here, not the thread's 5.
        exits = []
        with mock.patch.multiple(
            client_module,
            _exit_code=None,
            _exit_code_by_sys_exit=False,
            _interrupted_at=None,
            _original_sys_exit=exits.append,
            _original_excepthook=lambda *args: None,
        ):
            thread = threading.Thread(target=client_module._exit_wrapper, args=(5,))
            thread.start()
            thread.join()
            self.assertIsNone(client_module._exit_code)
            client_module._excepthook(ValueError, ValueError(), None)
            self.assertEqual(client_module._exit_code, 1)
        self.assertEqual(exits, [5])

    def test_the_atexit_flush_after_a_signal(self):
        # An exit soon after a Ctrl-C kymo saw, whatever ended the program (Lightning's teardown ends in sys.exit(1); a program can raise SystemExit itself): the run is handed off; long after it, an ordinary flush. After a SIGTERM a handler installed before init() took, the flush ends within the signal budget: from that SIGTERM while that budget lasts, since a scheduler's grace period also runs from it, and from the exit after that, since a requeue handler can exit long after it.
        drain = mock.Mock()
        now = time.monotonic()
        with (
            mock.patch.multiple(
                client_module,
                _is_initialized=True,
                _exit_code=None,
                _interrupted_at=None,
                _terminated_at=None,
                _drain_and_shutdown=drain,
            ),
            mock.patch.dict(
                os.environ,
                {"KYMO_SIGNAL_FLUSH_TIMEOUT": "3", "KYMO_FLUSH_TIMEOUT": "60"},
            ),
        ):
            for interrupted, terminated, flush_env, expected in (
                (None, None, None, {"flush_timeout": 60.0, "at_exit": True}),
                (now - 2, None, None, {"signum": signal.SIGINT, "at_exit": True}),
                (now - 4, None, None, {"flush_timeout": 60.0, "at_exit": True}),
                (None, now - 2, None, 1.0),
                (None, now - 4, None, 3.0),
                (None, now - 2, "0.5", 0.5),
            ):
                drain.reset_mock()
                env = {} if flush_env is None else {"KYMO_FLUSH_TIMEOUT": flush_env}
                with (
                    self.subTest(interrupted=interrupted, terminated=terminated),
                    mock.patch.dict(os.environ, env),
                    mock.patch.multiple(
                        client_module,
                        _interrupted_at=interrupted,
                        _terminated_at=terminated,
                    ),
                ):
                    client_module._ensure_metrics_uploaded()
                (call,) = drain.call_args_list
                if isinstance(expected, dict):
                    self.assertEqual(call.kwargs, expected)
                else:
                    self.assertEqual(call.kwargs["at_exit"], True)
                    self.assertAlmostEqual(
                        call.kwargs["flush_timeout"], expected, delta=0.25
                    )

    def test_finish_in_or_after_a_programs_sigterm_handler_ends_within_the_budget(
        self,
    ):
        # A requeue or checkpoint handler installed before init() that calls finish() itself: no worker policy bounds that flush, so finish() without a flush_timeout ends within the signal budget, as the atexit flush does, from the SIGTERM while that budget lasts and from the call after it. A flush_timeout the program passes stays as given.
        drain = mock.Mock()
        now = time.monotonic()
        with (
            mock.patch.multiple(
                client_module,
                _is_initialized=True,
                _interrupted_at=None,
                _worker_interrupted_at=None,
                _terminated_at=None,
                _drain_and_shutdown=drain,
            ),
            mock.patch.dict(
                os.environ,
                {"KYMO_SIGNAL_FLUSH_TIMEOUT": "3", "KYMO_FLUSH_TIMEOUT": "60"},
            ),
        ):
            for terminated, flush_timeout, flush_env, expected in (
                (None, None, None, None),
                (now - 2, None, None, 1.0),
                (now - 4, None, None, 3.0),
                (now - 2, None, "0.5", 0.5),
                (now - 2, 30.0, None, 30.0),
            ):
                drain.reset_mock()
                env = {} if flush_env is None else {"KYMO_FLUSH_TIMEOUT": flush_env}
                with (
                    self.subTest(terminated=terminated, flush_timeout=flush_timeout),
                    mock.patch.dict(os.environ, env),
                    mock.patch.object(client_module, "_terminated_at", terminated),
                ):
                    client_module.finish(flush_timeout)
                ((timeout,),) = (call.args for call in drain.call_args_list)
                if expected is None:
                    self.assertIsNone(timeout)
                else:
                    self.assertAlmostEqual(timeout, expected, delta=0.25)

    def test_handoff_cap_times_the_run_from_the_ctrl_c(self):
        with (
            mock.patch.object(client_module.time, "monotonic", return_value=100.0),
            mock.patch.dict(os.environ, {"KYMO_SIGNAL_FLUSH_TIMEOUT": "3"}),
        ):
            for interrupted, cap in ((99.0, 102.0), (97.0, 100.0), (96.9, 103.0)):
                with mock.patch.object(client_module, "_interrupted_at", interrupted):
                    self.assertEqual(client_module._handoff_cap(), cap)

    def test_the_worker_owns_only_the_signals_kymo_owns(self):
        # Those the program left at their default when kymo first saw them, and whose handler is still kymo's: one the program ignores or handles itself and carries on from must not end the worker.
        def program_handler(signum, frame):
            pass

        for found, owned in (
            (signal.SIG_DFL, True),
            (signal.SIG_IGN, False),
            (program_handler, False),
        ):
            previous = signal.signal(signal.SIGTERM, found)
            try:
                with self.subTest(found=found):
                    with mock.patch.dict(client_module._original_signals, clear=True):
                        self.assertEqual(
                            signal.SIGTERM in client_module._owned_signals(), owned
                        )
                    # Once init() has installed kymo's handler, by what it found.
                    with _kymo_handles({signal.SIGTERM: found}):
                        self.assertEqual(
                            signal.SIGTERM in client_module._owned_signals(), owned
                        )
                    self.assertNotIn(signal.SIGINT, client_module._owned_signals())
            finally:
                signal.signal(signal.SIGTERM, previous)

    def test_a_fork_child_disowns_the_parents_worker(self):
        # A raw os.fork() child's exit runs multiprocessing's exit hook, which would terminate the parent's worker through the children the child inherited.
        worker = object()
        children = {worker}
        with (
            mock.patch.object(multiprocessing.process, "_children", children),
            mock.patch.object(client_module, "_upload_process", worker),
        ):
            client_module._forget_pending_in_child()
        self.assertEqual(children, set())
        with (
            mock.patch.object(multiprocessing.process, "_children", None),
            self.assertLogs("kymo", level="WARNING") as logs,
        ):
            client_module._disown_worker(worker)
        self.assertIn("_children is gone", "\n".join(logs.output))

    def test_upload_worker_is_spawn_picklable_by_module_path(self):
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe(duplex=False)
        process = context.Process(
            target=_report_callable_identity,
            args=(worker_module._upload_worker, child),
        )
        process.start()
        child.close()
        process.join(timeout=10)
        if process.is_alive():
            process.kill()
            process.join(timeout=1)
            self.fail("spawn child did not finish")
        self.assertEqual(process.exitcode, 0)
        self.assertEqual(
            parent.recv(), (worker_module._upload_worker.__module__, "_upload_worker")
        )
        parent.close()

    def test_the_worker_survives_shutdown_signals_sent_before_its_policy(self):
        # A Ctrl-C or SIGTERM to the process group can reach the worker before it installs its policy: under fork and spawn it waits for it, and the policy then notes the Ctrl-C and carries on, and finishes on the SIGTERM within the signal budget. A group SIGHUP, SIGUSR1 or SIGUSR2, which the program handles (a Slurm requeue), neither kills the worker, as their default action did under spawn, nor runs the handler a fork child inherits, before its policy or after. A forkserver, which hands its children its own mask, never gets the blocked one. The script handles SIGINT as Python does by default, wherever it runs from: a shell's background job starts with it ignored, which the worker would keep.
        script = """
import multiprocessing
import os
import signal

import kymo._worker as worker
import kymo.client as client


def report(connection, handoff, interrupted):
    os.kill(os.getpid(), signal.SIGINT)
    os.kill(os.getpid(), signal.SIGTERM)
    for signum in (signal.SIGHUP, signal.SIGUSR1, signal.SIGUSR2):
        os.kill(os.getpid(), signum)
    worker._own_signals(handoff, (signal.SIGTERM,), interrupted)
    for signum in (signal.SIGHUP, signal.SIGUSR1, signal.SIGUSR2):
        os.kill(os.getpid(), signum)
    armed = signal.getitimer(signal.ITIMER_REAL)[0]
    connection.send(
        (worker._signal_deadline < float("inf"), 0 < armed <= 10, interrupted.value > 0)
    )


def blocked(connection):
    connection.send(signal.pthread_sigmask(signal.SIG_BLOCK, []))


def requeue(signum, frame):
    # The program's own handler, which a fork child inherits: it must not run in the worker.
    os._exit(3)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.default_int_handler)
    for signum in (signal.SIGHUP, signal.SIGUSR1, signal.SIGUSR2):
        signal.signal(signum, requeue)
    for method in ("fork", "spawn"):
        # kymo starts its worker from the default context, so the program picks the method.
        multiprocessing.set_start_method(method, force=True)
        receiver, sender = multiprocessing.Pipe(duplex=False)
        handoff = multiprocessing.RawValue("d", 0.0)
        interrupted = multiprocessing.RawValue("d", 0.0)
        process = multiprocessing.Process(
            target=report, args=(sender, handoff, interrupted)
        )
        client._start_worker(process)
        sender.close()
        print(method, receiver.recv() if receiver.poll(30) else None)
        receiver.close()
        process.join(30)
        print(method, process.exitcode)
    multiprocessing.set_start_method("forkserver", force=True)
    receiver, sender = multiprocessing.Pipe(duplex=False)
    for start in (client._start_worker, multiprocessing.Process.start):
        process = multiprocessing.Process(target=blocked, args=(sender,))
        start(process)
        process.join(30)
    print("forkserver", receiver.recv() | receiver.recv())
"""
        result = _run_script_file(script)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.split("\n")[:5],
            [
                "fork (True, True, True)",
                "fork 0",
                "spawn (True, True, True)",
                "spawn 0",
                "forkserver set()",
            ],
            result.stderr,
        )

    def test_a_worker_outliving_its_forkserver_is_seen_until_it_exits(self):
        # Under the forkserver start method a group SIGTERM kills the forkserver while the worker's policy keeps it running, and multiprocessing then reads the worker as exited (255) and signals it no more: kymo sees it run by its pid until it publishes its exit, waits for it, and kills it by its pid.
        if "forkserver" not in multiprocessing.get_all_start_methods():
            self.skipTest("no forkserver start method")
        script = """
import multiprocessing
import os
import signal
import threading
import time
from multiprocessing import forkserver

import kymo.client as client


def run(exited, done):
    # As kymo's policy does where the program owns SIGTERM.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    try:
        done.wait(30)
    finally:
        exited.value = 0


def orphaned(done):
    exited = multiprocessing.RawValue("i", -1)
    process = multiprocessing.Process(target=run, args=(exited, done))
    client._start_worker(process)
    os.kill(forkserver._forkserver._forkserver_pid, signal.SIGTERM)
    while process.is_alive():
        time.sleep(0.01)
    return process, exited


def stop(process, grace):
    now = time.monotonic()
    result = client._stop_upload_process(
        process, graceful_deadline=lambda: now + grace, stop_deadline=lambda: now + 10
    )
    return result, time.monotonic() - now >= min(grace, 0.2)


if __name__ == "__main__":
    multiprocessing.set_start_method("forkserver", force=True)
    client._init_pid = os.getpid()
    done = multiprocessing.Event()
    process, client._worker_exit = orphaned(done)
    client._upload_process = process
    client._check_worker()
    print(process.exitcode, client._worker_alive(process))
    print(stop(process, 0.2))
    try:
        os.kill(process.pid, 0)
        print("pid alive")
    except ProcessLookupError:
        print("pid gone")
    done = multiprocessing.Event()
    process, client._worker_exit = orphaned(done)
    threading.Timer(0.3, done.set).start()
    print(stop(process, 5))
"""
        result = _run_script_file(script)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.split("\n")[:4],
            ["255 True", "((True, False), True)", "pid gone", "((True, True), True)"],
            result.stderr,
        )

    def test_a_ctrl_c_as_the_worker_starts_leaves_the_shutdown_signals_unblocked(self):
        # The parent blocks the shutdown signals while the worker starts: a Ctrl-C's handler raising there leaves the main thread's mask as it was.
        with mock.patch.object(
            client_module.multiprocessing, "get_start_method", return_value="fork"
        ):
            _assert_a_raise_after_any_call_keeps_the_mask(
                self,
                (
                    client_module._start_worker,
                    client_module._with_group_signals_blocked,
                ),
                lambda: client_module._start_worker(mock.Mock()),
                6,
            )

    def test_the_worker_carries_on_through_a_signal_the_program_owns_until_a_handoff(
        self,
    ):
        # A program that ignores or handles SIGTERM itself carries on from a group SIGTERM, and so does its worker; the parent's handoff still bounds the worker by its cap, with a SIGTERM. The worker's backstop, SIGALRM's default action at its deadline, needs the alarm at its default and unblocked: a trainer that ignores or blocks SIGALRM passes that on through fork and spawn, and the policy set the default but left a block in place.
        script = """
import os
import signal
import time
import types

import kymo._worker as worker

signal.signal(signal.SIGALRM, signal.SIG_IGN)
signal.pthread_sigmask(signal.SIG_BLOCK, [signal.SIGALRM])
handoff = types.SimpleNamespace(value=0.0)
worker._own_signals(handoff, (), types.SimpleNamespace(value=0.0))
alarm = (
    signal.getsignal(signal.SIGALRM) is signal.SIG_DFL,
    signal.SIGALRM not in signal.pthread_sigmask(signal.SIG_BLOCK, []),
)
os.kill(os.getpid(), signal.SIGTERM)
ignored = (worker._signal_deadline, signal.getitimer(signal.ITIMER_REAL)[0])
handoff.value = time.monotonic() + 5
os.kill(os.getpid(), signal.SIGTERM)
armed = signal.getitimer(signal.ITIMER_REAL)[0]
signal.setitimer(signal.ITIMER_REAL, 0)
print(
    *alarm,
    ignored == (float("inf"), 0.0),
    worker._signal_deadline < worker._spill_by < handoff.value,
    0 < armed <= 5,
)
"""
        result = _run_script_file(script)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(), "True True True True True", result.stderr
        )

    def test_a_ctrl_c_the_worker_saw_bounds_it_once_its_parent_has_died(self):
        # The worker notes a Ctrl-C and carries on, but keeps ignoring one the program ignores (a shell's background job starts so), for which kymo installs no handler either. Once its parent has died, as SIGINT's default action kills a program that restored it after init(), a Ctrl-C within the handoff's cap hands the worker the run's remaining delivery by that cap, as the parent's handoff would; one older than the cap, which the program carried on from, leaves the orphan watch its 30 s.
        script = """
import os
import signal
import time
import types

import kymo._worker as worker

handoff = types.SimpleNamespace(value=0.0)
interrupted = types.SimpleNamespace(value=0.0)
signal.signal(signal.SIGINT, signal.SIG_IGN)
worker._own_signals(handoff, (), interrupted)
os.kill(os.getpid(), signal.SIGINT)
noted = interrupted.value == 0.0 and signal.getsignal(signal.SIGINT) is signal.SIG_IGN
signal.signal(signal.SIGINT, signal.default_int_handler)
worker._own_signals(handoff, (), interrupted)
os.kill(os.getpid(), signal.SIGINT)
noted = noted and 0 < time.monotonic() - interrupted.value < 1
interrupted.value = time.monotonic() - 20
worker._hand_off_after_ctrl_c()
older = (handoff.value, worker._signal_deadline, signal.getitimer(signal.ITIMER_REAL)[0])
interrupted.value = time.monotonic() - 1
worker._hand_off_after_ctrl_c()
armed = signal.getitimer(signal.ITIMER_REAL)[0]
signal.setitimer(signal.ITIMER_REAL, 0)
print(
    noted,
    older == (0.0, float("inf"), 0.0),
    abs(handoff.value - (interrupted.value + 3)) < 0.01,
    worker._signal_deadline < worker._spill_by < handoff.value,
    0 < armed <= 2.01,
)
"""
        result = _run_script_file(script, KYMO_SIGNAL_FLUSH_TIMEOUT="3")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(), "True True True True True", result.stderr
        )

    def test_the_workers_alarm_only_comes_forward_however_its_signals_nest(self):
        # A SIGTERM pushing a sooner handoff cap lands at each opcode of the worker's handling of another: its alarm and spill end no later than that cap. With a cap set, a budget below hand_off's floor never arms the alarm sooner than the cap.
        script = """
import itertools
import os
import signal
import sys
import time
import types

import kymo._worker as worker
from test_pipelined_upload import _opcode_tracer

handoff = types.SimpleNamespace(value=0.0)
worker._own_signals(handoff, (signal.SIGTERM,), types.SimpleNamespace(value=0.0))
handler = signal.getsignal(signal.SIGTERM)
codes = {handler.__code__} | {
    cell.cell_contents.__code__
    for cell in handler.__closure__
    if getattr(cell.cell_contents, "__name__", "") == "update"
}
late = []
for k in itertools.count():
    worker._signal_deadline = worker._spill_by = float("inf")
    handoff.value = time.monotonic() + 9

    def inject(frame):
        handoff.value = time.monotonic() + 2
        signal.raise_signal(signal.SIGTERM)

    tracer, fired = _opcode_tracer(codes, k, inject)
    sys.settrace(tracer)
    signal.raise_signal(signal.SIGTERM)
    sys.settrace(None)
    armed = signal.getitimer(signal.ITIMER_REAL)[0]
    signal.setitimer(signal.ITIMER_REAL, 0)
    if fired and not (0 < armed <= 2 and worker._spill_by <= time.monotonic() + 2):
        late.append(k)
    if not fired:
        break
worker._signal_deadline = worker._spill_by = float("inf")
handoff.value = time.monotonic() + 1
signal.raise_signal(signal.SIGTERM)
armed = signal.getitimer(signal.ITIMER_REAL)[0]
signal.setitimer(signal.ITIMER_REAL, 0)
print(k > 50, str(late).replace(" ", ""), 0.9 < armed <= 1)
"""
        for budget in ("30", "0.1"):
            result = _run_script_file(script, KYMO_SIGNAL_FLUSH_TIMEOUT=budget)
            self.assertEqual(result.returncode, 0, result.stderr)
            traced, late, capped = result.stdout.strip().split(" ", 2)
            self.assertEqual((traced, late, capped), ("True", "[]", "True"), budget)

    def test_init_rejects_removed_batch_size_keyword(self):
        with self.assertRaises(TypeError):
            client_module.init(batch_size=100)

    def test_init_does_not_rebind_removed_batch_size_position(self):
        with self.assertRaises(TypeError):
            client_module.init(None, "project", "run", "run-id", 100)

    def test_hosted_init_requires_a_server_address(self):
        environ = {
            k: v for k, v in os.environ.items() if k not in ("KYMO_SERVER", "KYMO_MODE")
        }
        with (
            mock.patch.dict(os.environ, environ, clear=True),
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_drain_and_shutdown") as shutdown,
            mock.patch.object(client_module.grpc, "insecure_channel") as connect,
            self.assertRaisesRegex(ValueError, "needs a server address"),
        ):
            client_module.init(project_id="project", run_name="run", run_id="run")

        shutdown.assert_not_called()
        connect.assert_not_called()

    def test_local_mode_rejects_a_url_base_override(self):
        with self.assertRaisesRegex(
            ValueError, "cannot override local runtime endpoints"
        ):
            client_module.init(
                project_id="project",
                run_name="run",
                mode="local",
                url_base="http://dash",
            )

    def test_run_url_requires_a_dashboard_address(self):
        with (
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_url_base", ""),
            self.assertRaisesRegex(RuntimeError, "no dashboard address"),
        ):
            client_module.run_url()

    def test_invalid_config_does_not_stop_an_existing_run(self):
        with (
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_drain_and_shutdown") as shutdown,
            mock.patch.object(client_module.grpc, "insecure_channel") as connect,
            self.assertRaisesRegex(ValueError, "finite JSON-serializable"),
        ):
            client_module.init(
                "localhost:50051",
                project_id="project",
                run_name="run",
                run_id="run",
                config={"loss": float("nan")},
            )

        shutdown.assert_not_called()
        connect.assert_not_called()

    def test_first_init_requires_main_thread_for_signal_handlers(self):
        errors = []

        def initialize():
            try:
                client_module.init(
                    "localhost:50051",
                    project_id="project",
                    run_name="run",
                    run_id="run-id",
                    system_metrics=False,
                )
            except Exception as error:
                errors.append(error)

        with (
            mock.patch.object(client_module, "_atexit_registered", False),
            mock.patch.object(client_module, "_is_initialized", False),
            mock.patch.object(client_module, "_drain_and_shutdown") as shutdown,
            mock.patch.object(client_module.grpc, "insecure_channel") as connect,
        ):
            thread = threading.Thread(target=initialize)
            thread.start()
            thread.join(timeout=1)

        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertIn("main thread of the main Python interpreter", str(errors[0]))
        shutdown.assert_not_called()
        connect.assert_not_called()

    def test_first_init_requires_actual_signal_install_capability(self):
        with (
            mock.patch.object(client_module, "_atexit_registered", False),
            mock.patch.object(client_module, "_is_initialized", False),
            mock.patch.object(client_module, "_drain_and_shutdown") as shutdown,
            mock.patch.object(client_module.grpc, "insecure_channel") as connect,
            mock.patch.object(
                client_module.signal,
                "signal",
                side_effect=ValueError("main interpreter required"),
            ),
            self.assertRaisesRegex(
                RuntimeError, "main thread of the main Python interpreter"
            ),
        ):
            client_module.init(
                "localhost:50051",
                project_id="project",
                run_name="run",
                run_id="run-id",
                system_metrics=False,
            )

        shutdown.assert_not_called()
        connect.assert_not_called()

    @contextlib.contextmanager
    def _init_without_a_server(self, process, **patches):
        """Patch kymo.client so init() runs without a server: a module state no run has touched, patches on top, an InitRun that names the run "canonical run", and mocks for its queue, values, run metadata upload and upload worker, process. Yields the mocks of Process and make_spool_path."""
        response = types.SimpleNamespace(
            run=types.SimpleNamespace(run_name="canonical run", ordinal=1),
            HasField=lambda _name: False,
        )
        with (
            mock.patch.multiple(
                client_module,
                **{
                    "_is_initialized": False,
                    "_exit_code": None,
                    "_interrupted_at": None,
                    "_worker_interrupted_at": None,
                    "_last_log_duration_ms": 0.0,
                    "_project_id": "",
                    "_run_id": "",
                    "_run_name": "",
                    "_server_address": "",
                    "_cdn_address": "",
                    "_mode": "hosted",
                    "_local_installation_uuid": "",
                    "_url_base": "",
                    "_metric_queue": None,
                    "_queue_status": None,
                    "_upload_failure": None,
                    "_upload_spooled": None,
                    "_upload_terminal": None,
                    # The shared values init() makes, restored with the rest: left on the module, they are freed only as the interpreter exits, when multiprocessing's heap can no longer free them and prints an error for each.
                    "_shutdown_deadline": None,
                    "_spill_deadline": None,
                    "_handoff": None,
                    "_worker_started": None,
                    "_worker_exit": None,
                    "_galleries_released": None,
                    "_lanes_on_disk": None,
                    "_spool_dir": None,
                    "_session_id": "",
                    "_upload_process": None,
                    "_system_poller": None,
                    "_init_pid": None,
                    "_atexit_registered": True,
                    "_run_control_rpc": mock.Mock(return_value=response),
                    "_run_metadata": None,
                    "_collect_run_metadata": mock.Mock(return_value={}),
                    "_push_run_metadata": mock.Mock(),
                    **patches,
                },
            ),
            mock.patch.object(
                client_module.multiprocessing, "Queue", return_value=mock.Mock()
            ),
            mock.patch.object(
                client_module.multiprocessing,
                "Value",
                side_effect=lambda *_args: _QueueStatus(),
            ),
            mock.patch.object(
                client_module.multiprocessing, "Process", return_value=process
            ) as process_factory,
            mock.patch.object(
                client_module, "make_spool_path", return_value="worker.mkspool"
            ) as make_spool_path,
            # The poller's thread, which init() starts whatever system_metrics says, would outlive the test and drain the captured output into another test's state.
            mock.patch.object(SystemMetricsPoller, "start"),
        ):
            yield process_factory, make_spool_path

    @staticmethod
    def _terminate_run_codes():
        """The exit codes of the TerminateRuns init() sent through _init_without_a_server's _run_control_rpc."""
        return [
            call.args[1].exit_code
            for call in client_module._run_control_rpc.call_args_list
            if call.args[0] == "TerminateRun"
        ]

    @staticmethod
    def _init(**overrides):
        """init() the hosted run "run" (run-id) of project at 127.0.0.1:50051, without system metrics, spooling to relative-spool, overrides aside."""
        client_module.init(
            **{
                "server_address": "127.0.0.1:50051",
                "url_base": "http://dash:8081",
                "project_id": "project",
                "run_name": "run",
                "run_id": "run-id",
                "mode": "hosted",
                "system_metrics": False,
                "spool_dir": "relative-spool",
                **overrides,
            }
        )

    def test_init_clears_previous_run_state_recorded_between_runs(self):
        process = mock.Mock()
        with self._init_without_a_server(
            process, _exit_code=7, _last_log_duration_ms=12.5
        ) as (process_factory, make_spool_path):
            self._init(url_base="http://dash:8081/", run_name="proposed replacement")
            self.assertTrue(client_module._is_initialized)
            self.assertIsNone(client_module._exit_code)
            self.assertEqual(client_module._last_log_duration_ms, 0.0)
            self.assertEqual(client_module._run_name, "canonical run")
            self.assertEqual(client_module._url_base, "http://dash:8081")
            self.assertEqual(
                client_module._spool_dir,
                os.path.abspath("relative-spool"),
            )

        process.start.assert_called_once_with()
        self.assertEqual(process_factory.call_args.kwargs["args"][8], "canonical run")
        self.assertEqual(
            make_spool_path.call_args.kwargs["spool_dir"],
            os.path.abspath("relative-spool"),
        )

    def test_a_ctrl_c_as_the_poller_starts_ends_the_run(self):
        # The poller's start, NVML's init with system metrics, can take seconds once the worker has started: a Ctrl-C there raises out of init() with the run active and kymo's exit hooks installed, so the exception hook hands it off, rather than leave the worker, which carries on through a Ctrl-C, to multiprocessing's exit, which waits for it. The poller starts last, after the run's metadata.
        started = []

        def start(poller):
            started.append("poller")
            raise KeyboardInterrupt

        with (
            self._init_without_a_server(
                mock.Mock(),
                _atexit_registered=False,
                _original_excepthook=None,
                _push_run_metadata=lambda: started.append("metadata"),
            ),
            mock.patch.object(SystemMetricsPoller, "start", start),
            mock.patch.object(client_module.atexit, "register") as register,
            mock.patch.object(client_module, "_setup_signal_handlers") as handlers,
            mock.patch.object(client_module, "_install_exit_capture") as exit_capture,
            mock.patch.object(client_module, "_drain_and_shutdown") as shutdown,
        ):
            with self.assertRaises(KeyboardInterrupt) as raised:
                self._init(system_metrics=True)
            self.assertTrue(client_module._is_initialized)
            register.assert_called_once_with(client_module._ensure_metrics_uploaded)
            handlers.assert_called_once_with()
            exit_capture.assert_called_once_with()
            self.assertEqual(started, ["metadata", "poller"])
            client_module._excepthook(KeyboardInterrupt, raised.exception, None)
        shutdown.assert_called_once_with(signum=signal.SIGINT)

    def test_init_starts_the_poller_without_system_metrics_after_the_metadata(self):
        # The poller uploads the captured console whatever system_metrics says, which decides only its metrics.
        calls = []
        with (
            self._init_without_a_server(
                mock.Mock(), _push_run_metadata=lambda: calls.append("metadata")
            ),
            mock.patch.object(
                SystemMetricsPoller,
                "start",
                lambda poller: calls.append(("start", poller._system_metrics)),
            ),
        ):
            self._init(system_metrics=False)
        self.assertEqual(calls, ["metadata", ("start", False)])

    def test_a_ctrl_c_as_the_worker_starts_ends_the_run(self):
        # An exception out of the worker's start once the worker has started (a raising handler of a signal kymo does not own, say): init() raises with the run active and kymo's exit hooks installed, so the exception hook hands it off, rather than leave the worker to multiprocessing's exit, which waits for it, forever under a SIGTERM the program owns. A worker that never started leaves the run InitRun registered, which gets a best-effort TerminateRun. kymo's handler is installed before the start either way.
        for pid in (1234, None):

            def start(process, pid=pid):
                process.pid = pid
                raise KeyboardInterrupt

            process = mock.Mock(pid=None)
            with (
                self.subTest(started=pid is not None),
                self._init_without_a_server(
                    process, _atexit_registered=False, _original_excepthook=None
                ),
                mock.patch.object(client_module, "_start_worker", start),
                mock.patch.object(client_module.atexit, "register") as register,
                mock.patch.object(client_module, "_setup_signal_handlers") as handlers,
                mock.patch.object(client_module, "_install_exit_capture"),
            ):
                with self.assertRaises(KeyboardInterrupt):
                    self._init()
                self.assertEqual(client_module._is_initialized, pid is not None)
                self.assertEqual(register.call_count, int(pid is not None))
                handlers.assert_called_once_with()
                self.assertEqual(self._terminate_run_codes(), [] if pid else [130])

    def test_a_ctrl_c_any_thread_takes_as_the_worker_starts_waits_for_the_run(self):
        # The signals are blocked across Process.start() on the main thread alone: a group Ctrl-C the kernel gives another thread, a data loader's say, runs Python's handler on the main thread inside the start. kymo's, installed before the start, defers it until the run is active, and init() then raises its KeyboardInterrupt, which the exception hook hands off.
        def start(process):
            signal.raise_signal(signal.SIGINT)
            process.pid = 1234

        process = mock.Mock(pid=None)
        with (
            self._init_without_a_server(
                process, _atexit_registered=False, _original_excepthook=None
            ),
            mock.patch.object(client_module, "_start_worker", start),
            mock.patch.object(client_module.atexit, "register"),
            mock.patch.object(client_module, "_setup_signal_handlers"),
            mock.patch.object(client_module, "_install_exit_capture"),
            _kymo_handles({signal.SIGINT: signal.default_int_handler}),
        ):
            with self.assertRaises(KeyboardInterrupt):
                self._init()
            self.assertTrue(client_module._is_initialized)
            self.assertEqual(client_module._deferred_signals, [])

    def test_an_init_whose_worker_never_started_puts_back_the_programs_handlers(self):
        # kymo's handlers are installed before the worker starts. A start that leaves no worker raises out of init() with the run never active, after a best-effort TerminateRun of the run InitRun registered, with a SystemExit's own code (143, kymo's exit on a SIGTERM; True as 1), and puts back the handlers it installed, so a program that carries on without kymo keeps its own signal handling.
        def program(signum, frame):
            pass

        saved = {
            signum: signal.getsignal(signum)
            for signum in client_module._TERMINATION_SIGNALS
        }
        try:
            for error, code in (
                (OSError("no worker"), 1),
                (SystemExit(143), 143),
                (SystemExit(True), 1),
            ):
                signal.signal(signal.SIGTERM, program)
                with (
                    self.subTest(error=error),
                    self._init_without_a_server(
                        mock.Mock(pid=None),
                        _atexit_registered=False,
                        _original_excepthook=None,
                    ),
                    mock.patch.object(
                        client_module, "_start_worker", side_effect=error
                    ),
                    mock.patch.dict(client_module._original_signals, clear=True),
                ):
                    with self.assertRaises(type(error)):
                        self._init()
                    self.assertEqual(self._terminate_run_codes(), [code])
                    self.assertEqual(
                        {
                            signum: signal.getsignal(signum)
                            for signum in client_module._TERMINATION_SIGNALS
                        },
                        {**saved, signal.SIGTERM: program},
                    )
        finally:
            for signum, handler in saved.items():
                signal.signal(signum, handler)

    def test_init_starts_the_resource_tracker_before_init_run(self):
        # The tracker's start spawns a process, which can fail: before InitRun, its failure leaves no registered run that nothing would end.
        error = OSError(11, "Resource temporarily unavailable")
        with (
            self._init_without_a_server(mock.Mock()),
            mock.patch.object(
                client_module, "_start_resource_tracker", side_effect=error
            ),
        ):
            with self.assertRaises(OSError):
                self._init()
            client_module._run_control_rpc.assert_not_called()

    def test_the_tracker_start_leaves_the_start_method_unset(self):
        # An init() whose InitRun fails leaves the program free to set the start method, as kymo.api._check_fork_safe does.
        with (
            mock.patch.object(
                multiprocessing.context._default_context, "_actual_context", None
            ),
            mock.patch("multiprocessing.resource_tracker.ensure_running") as ensure,
        ):
            client_module._start_resource_tracker()
            self.assertIsNone(multiprocessing.get_start_method(allow_none=True))
        self.assertEqual(
            ensure.call_count, int(multiprocessing.get_all_start_methods()[0] != "fork")
        )

    def test_a_start_a_deferred_signal_failed_ends_the_run_with_its_code(self):
        # Under forkserver the group's Ctrl-C or SIGTERM kills the forkserver as it imports __main__, and the start fails with EOFError while kymo's handler defers the signal (_start_run): the run InitRun registered ends with the signal's code, not 1, and init() raises the signal's own exception.
        for signum, handler, code, raised in (
            (signal.SIGINT, signal.default_int_handler, 130, KeyboardInterrupt),
            (signal.SIGTERM, signal.SIG_DFL, 143, SystemExit),
        ):

            def start(process, signum=signum):
                signal.raise_signal(signum)
                raise EOFError

            with (
                self.subTest(signal=signal.Signals(signum).name),
                self._init_without_a_server(
                    mock.Mock(pid=None),
                    _atexit_registered=False,
                    _original_excepthook=None,
                ),
                mock.patch.object(client_module, "_start_worker", start),
                mock.patch.object(client_module, "_setup_signal_handlers"),
                _kymo_handles({signum: handler}),
            ):
                with self.assertRaises(raised) as error:
                    self._init()
                self.assertIsInstance(error.exception.__context__, EOFError)
                self.assertEqual(self._terminate_run_codes(), [code])

    def test_a_signal_deferred_as_the_worker_starts_keeps_its_time(self):
        # A Ctrl-C, or a SIGTERM the program's own handler takes, that lands as the worker starts waits for the run to be active (_start_run), and keeps the time it came, which the run's end reads: a program that exits on that Ctrl-C hands the run off at 130, and the atexit flush after that SIGTERM ends within the signal budget from it.
        for signum, handler in (
            (signal.SIGINT, signal.default_int_handler),
            (signal.SIGTERM, lambda signum, frame: None),
        ):

            def start(process, signum=signum):
                signal.raise_signal(signum)
                process.pid = 1234

            process = mock.Mock(pid=None)
            with (
                self.subTest(signal=signal.Signals(signum).name),
                self._init_without_a_server(
                    process, _atexit_registered=False, _original_excepthook=None
                ),
                mock.patch.object(client_module, "_start_worker", start),
                mock.patch.object(client_module.atexit, "register"),
                mock.patch.object(client_module, "_setup_signal_handlers"),
                mock.patch.object(client_module, "_install_exit_capture"),
                _kymo_handles({signum: handler}),
                mock.patch.dict(os.environ, {"KYMO_SIGNAL_FLUSH_TIMEOUT": "3"}),
                mock.patch.object(client_module, "_drain_and_shutdown") as drain,
            ):
                before = time.monotonic()
                with contextlib.suppress(KeyboardInterrupt):
                    self._init()
                self.assertTrue(client_module._is_initialized)
                client_module._ensure_metrics_uploaded()
                at = (
                    client_module._interrupted_at
                    if signum == signal.SIGINT
                    else client_module._terminated_at
                )
            self.assertGreaterEqual(at or 0.0, before)
            (call,) = drain.call_args_list
            if signum == signal.SIGINT:
                self.assertEqual(
                    call.kwargs, {"signum": signal.SIGINT, "at_exit": True}
                )
            else:
                self.assertLessEqual(call.kwargs["flush_timeout"], 3.0)

    def test_multiprocessings_exit_hook_stays_after_the_flush_whatever_sets_up_its_logger(
        self,
    ):
        # multiprocessing's exit hook (multiprocessing.util._exit_function) closes the queue's feeder and terminates and joins the worker. The first set-up of its logger (get_logger(), which log_to_stderr() calls) registers it again, to run first: done after init(), that would run it before the atexit flush, leaving that nothing to deliver, and hang a program that owns SIGTERM, which the worker then ignores, at its exit. The first init() sets the logger up first, as a program's first call would, so a later set-up registers nothing and the flush stays ahead of the hook.
        registered = []

        def unregister(function):
            while function in registered:
                registered.remove(function)

        hooks = types.SimpleNamespace(register=registered.append, unregister=unregister)
        exit_function = multiprocessing.util._exit_function
        logger = logging.getLogger("multiprocessing")

        def start(process):
            process.pid = 1234

        with (
            self._init_without_a_server(
                mock.Mock(pid=None), _atexit_registered=False, _original_excepthook=None
            ),
            mock.patch.object(client_module, "_start_worker", start),
            mock.patch.object(client_module, "_setup_signal_handlers"),
            mock.patch.object(client_module, "_install_exit_capture"),
            mock.patch.object(client_module, "atexit", hooks),
            mock.patch.object(multiprocessing.util, "atexit", hooks),
            mock.patch.object(multiprocessing.util, "_logger", None),
            mock.patch.object(logger, "propagate", True),
        ):
            # As multiprocessing.util's import registers it.
            registered.append(exit_function)
            self._init()
            # The program's own set-up, after init().
            multiprocessing.util.get_logger()
            self.assertIs(multiprocessing.util._logger, logger)
            self.assertFalse(logger.propagate)
        # atexit runs its hooks last registered first.
        self.assertEqual(
            registered, [exit_function, client_module._ensure_metrics_uploaded]
        )


class ConfigSnapshotTests(unittest.TestCase):
    class Label:
        def __str__(self):
            return "label"

    def test_snapshot_owns_nested_values_and_matches_wire_coercion(self):
        nested = {"items": [1, 2]}
        source = {
            "nested": nested,
            "tuple": ("a", "b"),
            "label": self.Label(),
        }

        snapshot = client_module._snapshot_config(source)
        nested["items"].append(3)
        source["new"] = True

        self.assertEqual(
            snapshot,
            {
                "nested": {"items": [1, 2]},
                "tuple": ["a", "b"],
                "label": "label",
            },
        )

    def test_snapshot_rejects_non_dict_nonfinite_and_cyclic_values(self):
        with self.assertRaisesRegex(TypeError, "must be dict"):
            client_module._snapshot_config([("loss", 1.0)])
        for value in (float("nan"), float("inf"), float("-inf")):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(ValueError, "finite JSON-serializable"),
            ):
                client_module._snapshot_config({"value": value})

        cyclic = {}
        cyclic["self"] = cyclic
        with self.assertRaisesRegex(ValueError, "finite JSON-serializable"):
            client_module._snapshot_config(cyclic)

    def test_update_config_owns_patch_without_mutating_public_inputs(self):
        initial = {"optimizer": {"lr": 0.1}}
        metadata = {
            "meta": {"system": "stable"},
            "config": client_module._snapshot_config(initial),
        }
        patch = {"schedule": {"warmup": 5}}

        with (
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_run_metadata", metadata),
            mock.patch.object(client_module, "_push_run_metadata") as push,
        ):
            client_module.update_config(patch)

        initial["optimizer"]["lr"] = 0.2
        patch["schedule"]["warmup"] = 99
        self.assertEqual(
            metadata,
            {
                "meta": {"system": "stable"},
                "config": {
                    "optimizer": {"lr": 0.1},
                    "schedule": {"warmup": 5},
                },
            },
        )
        self.assertNotIn("schedule", initial)
        push.assert_called_once_with()


class RunMetadataTests(unittest.TestCase):
    def test_command_metadata_preserves_argument_boundaries(self):
        argv = ["train.py", "--data", "runs/my set", "", "it's quoted"]
        with mock.patch.object(sys, "argv", argv):
            metadata = client_module._collect_run_metadata(None)

        self.assertEqual(
            shlex.split(metadata["meta"]["system"]["command"]),
            argv,
        )

    def test_slurm_metadata_excludes_the_jwt_bearer_token(self):
        token = "sentinel-slurm-secret"
        with mock.patch.dict(
            os.environ,
            {"SLURM_JOB_ID": "123", "SLURM_JWT": token},
        ):
            metadata = client_module._collect_run_metadata(None)

        self.assertEqual(metadata["meta"]["slurm"]["SLURM_JOB_ID"], "123")
        self.assertNotIn("SLURM_JWT", metadata["meta"]["slurm"])
        self.assertNotIn(token, json.dumps(metadata))

    def test_gpu_discovery_shuts_down_nvml_after_a_query_failure(self):
        calls = []

        def fail_count():
            raise RuntimeError("driver query failed")

        fake_nvml = types.SimpleNamespace(
            nvmlInit=lambda: calls.append("init"),
            nvmlDeviceGetCount=fail_count,
            nvmlShutdown=lambda: calls.append("shutdown"),
        )
        with mock.patch.dict("sys.modules", {"pynvml": fake_nvml}):
            metadata = client_module._collect_run_metadata(None)

        self.assertEqual(calls, ["init", "shutdown"])
        self.assertNotIn("gpu_count", metadata["meta"]["system"])

    def test_a_hung_git_drops_the_git_metadata_within_its_timeout(self):
        # A git that a slow or hung filesystem stalls, first on PATH: one call times out, and the next, which would stall too, is not made. The calls are counted as made, which a child the timeout kills before it runs cannot miss.
        calls = []
        check_output = subprocess.check_output

        def counted(args, **kwargs):
            calls.append(args[3:])
            return check_output(args, **kwargs)

        with tempfile.TemporaryDirectory() as bin_dir:
            git = os.path.join(bin_dir, "git")
            with open(git, "w") as script:
                script.write("#!/bin/sh\nexec sleep 5\n")
            os.chmod(git, 0o755)
            with (
                mock.patch.dict(
                    os.environ, {"PATH": bin_dir + os.pathsep + os.environ["PATH"]}
                ),
                mock.patch.object(client_module, "_GIT_METADATA_TIMEOUT", 0.2),
                mock.patch.object(
                    client_module.subprocess, "check_output", side_effect=counted
                ),
                self.assertLogs("kymo", level="WARNING") as logs,
            ):
                started = time.monotonic()
                metadata = client_module._collect_run_metadata(None)
                elapsed = time.monotonic() - started

        self.assertEqual(calls, [["remote", "get-url", "origin"]])
        self.assertEqual(len(logs.records), 1)
        self.assertIn(
            "git remote get-url origin took longer than 0.2 s",
            logs.records[0].getMessage(),
        )

        self.assertNotIn("git", metadata["meta"])
        self.assertLess(elapsed, 2.0)

    def test_git_remote_userinfo_is_not_persisted(self):
        self.assertEqual(
            client_module._strip_url_userinfo(
                "https://oauth2:TOKEN@gitlab.example:8443/org/repo.git"
            ),
            "https://gitlab.example:8443/org/repo.git",
        )
        self.assertEqual(
            client_module._strip_url_userinfo("https://github.com/org/repo.git"),
            "https://github.com/org/repo.git",
        )
        self.assertEqual(
            client_module._strip_url_userinfo("git@github.com:org/repo.git"),
            "git@github.com:org/repo.git",
        )
        self.assertEqual(
            client_module._strip_url_userinfo("https://TOKEN@[broken/repo.git"),
            "<unparseable URL omitted>",
        )


class RunNameValidationTests(unittest.TestCase):
    def test_run_name_matches_the_server_normalization_contract(self):
        self.assertEqual(
            client_module._normalize_run_name("  named run\n"), "named run"
        )
        self.assertEqual(
            client_module._normalize_run_name("\x1cnamed\x1c"), "\x1cnamed\x1c"
        )
        self.assertEqual(
            client_module._normalize_run_name("界" * 682),
            "界" * 682,
        )

        for invalid in (" \n\t ", "bad\0name", "界" * 683):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                client_module._normalize_run_name(invalid)
        with self.assertRaises(TypeError):
            client_module._normalize_run_name(123)


class BatchBoundTests(unittest.TestCase):
    def test_resource_freezes_mutable_bytes_like_data(self):
        for payload in (bytearray(b"before"), memoryview(b"before")):
            with self.subTest(type=type(payload).__name__):
                resource = Resource(payload, "payload.bin")
                if isinstance(payload, bytearray):
                    payload[:] = b"after!"

                self.assertIs(type(resource.data), bytes)
                self.assertEqual(resource.data, b"before")

    def test_resource_rejects_non_bytes_without_allocating_from_integer(self):
        with self.assertRaisesRegex(TypeError, "data must be bytes-like"):
            Resource(10_000_000_000, "payload.bin")

    def test_resource_extension_uses_the_filename_leaf_on_every_platform(self):
        cases = {
            "report.TXT": "txt",
            "archive.tar.gz": "gz",
            "checkpoint.zip": "bin",
            "events.jsonl": "bin",
            ".env": "bin",
            "report.": "bin",
            "": "bin",
            "/tmp.v1/report": "bin",
            r"C:\tmp.v1\report": "bin",
            r"C:.env": "bin",
            r"C:report.pdf": "pdf",
            "/tmp.v1/report.pdf": "pdf",
            r"C:\tmp.v1\report.pdf": "pdf",
        }
        for filename, expected in cases.items():
            with self.subTest(filename=filename):
                self.assertEqual(
                    client_module._encode_resource(Resource(b"payload", filename)),
                    (b"payload", expected),
                )

    def test_image_encoding_prefers_documented_hwc_before_chw(self):
        cases = (
            (np.zeros((3, 5, 3), dtype=np.uint8), (5, 3), "RGB"),
            (np.zeros((4, 6, 3), dtype=np.uint8), (6, 4), "RGB"),
            (np.zeros((3, 5, 1), dtype=np.uint8), (5, 3), "L"),
            (np.zeros((3, 5, 7), dtype=np.uint8), (7, 5), "RGB"),
        )
        for data, expected_size, expected_mode in cases:
            with self.subTest(shape=data.shape):
                encoded, extension = client_module._encode_image(Image(data))
                with PILImage.open(io.BytesIO(encoded)) as decoded:
                    self.assertEqual(extension, "png")
                    self.assertEqual(decoded.size, expected_size)
                    self.assertEqual(decoded.mode, expected_mode)

    def test_raw_image_bytes_detect_the_default_format_and_keep_overrides(self):
        payloads = {}
        for image_format in ("PNG", "JPEG"):
            output = io.BytesIO()
            PILImage.new("RGB", (2, 1), "red").save(output, format=image_format)
            payloads[image_format] = output.getvalue()

        for image_format, expected_extension in (("PNG", "png"), ("JPEG", "jpeg")):
            with self.subTest(image_format=image_format):
                encoded, extension = client_module._encode_image(
                    Image(payloads[image_format])
                )
                self.assertIs(encoded, payloads[image_format])
                self.assertEqual(extension, expected_extension)

        jpeg = payloads["JPEG"]
        self.assertEqual(
            client_module._encode_image(Image(jpeg, format="jpg")), (jpeg, "jpg")
        )
        svg = b"<svg xmlns='http://www.w3.org/2000/svg'/>"
        self.assertEqual(
            client_module._encode_image(Image(svg, format="svg")), (svg, "svg")
        )
        # Format is case-insensitive: "PNG" is still the detect-the-format default, and explicit overrides come back as lowercase extensions.
        self.assertEqual(
            client_module._encode_image(Image(jpeg, format="PNG")), (jpeg, "jpeg")
        )
        self.assertEqual(
            client_module._encode_image(Image(svg, format="SVG")), (svg, "svg")
        )

    def test_pil_image_encoding_accepts_uppercase_jpg(self):
        encoded, extension = client_module._encode_image(
            Image(PILImage.new("RGB", (2, 1), "red"), format="JPG")
        )

        self.assertEqual(extension, "jpg")
        with PILImage.open(io.BytesIO(encoded)) as decoded:
            self.assertEqual(decoded.format, "JPEG")
            self.assertEqual(decoded.size, (2, 1))

    def test_jpeg_encoding_converts_supported_alpha_inputs_to_rgb(self):
        inputs = (
            np.zeros((2, 3, 4), dtype=np.uint8),
            PILImage.new("RGBA", (3, 2), (255, 0, 0, 128)),
        )
        for data in inputs:
            with self.subTest(type=type(data).__name__):
                encoded, extension = client_module._encode_image(
                    Image(data, format="jpg")
                )
                self.assertEqual(extension, "jpg")
                with PILImage.open(io.BytesIO(encoded)) as decoded:
                    self.assertEqual(decoded.format, "JPEG")
                    self.assertEqual(decoded.size, (3, 2))
                    self.assertEqual(decoded.mode, "RGB")

    def test_spool_image_memory_error_does_not_write_a_reduced_record(self):
        spool = mock.Mock()
        with (
            mock.patch.object(
                client_module, "_encode_image", side_effect=MemoryError("pressure")
            ),
            self.assertLogs("kymo", level="ERROR") as logs,
        ):
            written = client_module._spill_tuple(
                spool,
                ("cdn_batch", "demo/image", 1, [Image(b"valid")]),
            )

        self.assertEqual(written, 0)
        self.assertIn("MemoryError", "\n".join(logs.output))
        spool.write.assert_not_called()

    def test_retained_lane_keeps_items_sizes_and_total_in_sync(self):
        lane = client_module._RetainedLane()
        lane.append(numeric(1), 100)
        lane.append(numeric(2), 200)

        lane.replace(1, numeric(3), 50)
        self.assertEqual(lane.sizes, [100, 50])
        self.assertEqual(lane.total_bytes, 150)

        lane.discard_prefix(1)
        self.assertEqual(lane.items, [numeric(3)])
        self.assertEqual(lane.sizes, [50])
        self.assertEqual(lane.total_bytes, 50)

        lane.clear()
        self.assertFalse(lane)
        self.assertEqual(lane.total_bytes, 0)

    def test_message_build_reuses_cached_byte_estimates(self):
        tuples = [numeric(0), numeric(1)]
        with mock.patch.object(
            wire_module,
            "_estimate_tuple_bytes",
            side_effect=AssertionError("re-encoded a retained point"),
        ):
            chunks = list(wire_module._chunk_tuples(tuples, [100, 100]))

        self.assertEqual([len(chunk) for chunk in chunks], [2])

    def test_message_count_cap_splits_without_dropping_the_edge(self):
        tuples = [numeric(i) for i in range(5_001)]
        first, second = wire_module._chunk_tuples(tuples)
        self.assertEqual(len(first), 5_000)
        self.assertEqual([point.step for point in second], [5_000])

    def test_message_byte_cap_splits_before_large_text(self):
        tuples = [numeric(0), ("text_ts", "logs/std_out", 1, "x" * 2_100_000, 1)]
        chunks = wire_module._chunk_tuples(tuples)
        self.assertEqual([len(chunk) for chunk in chunks], [1, 1])


class AccountingTests(unittest.TestCase):
    def setUp(self):
        # init() resets it per run; puts interrupted here must not leak into later tests' waits.
        patch = mock.patch.object(client_module, "_unproven_points", 0)
        patch.start()
        self.addCleanup(patch.stop)

    def test_writer_epoch_versions_are_allocated_before_rich_queueing(self):
        with (
            mock.patch.object(client_module, "_rich_writer_epoch", 7),
            mock.patch.object(
                client_module, "_rich_mutation_seq", multiprocessing.Value("I", 0)
            ),
        ):
            first = client_module._rich_queue_tuple(
                "metadata_batch", "info/run_info", 0, Metadata({}), 100
            )
            second = client_module._rich_queue_tuple("cdn_batch", "gallery", 1, [], 101)

        self.assertEqual(first[:3], ("metadata_batch_mutation", "info/run_info", 0))
        self.assertEqual(first[4:], (100, (7 << 32) | 1))
        self.assertEqual(second[:3], ("cdn_batch_mutation", "gallery", 1))
        self.assertEqual(second[4:], (101, (7 << 32) | 2, (7 << 32) | 3))

    def test_gallery_primary_and_fallback_are_one_atomic_reservation(self):
        start_contender = threading.Event()
        contender_attempted = threading.Event()
        contender_done = threading.Event()

        class InterleavingLock:
            def __init__(self):
                self._lock = threading.Lock()
                self.gallery_acquires = 0

            def acquire(self, timeout):
                if threading.current_thread().name == "contender":
                    contender_attempted.set()
                elif threading.current_thread().name == "gallery":
                    self.gallery_acquires += 1
                    if self.gallery_acquires > 1:
                        contender_done.wait(1)
                return self._lock.acquire(timeout=timeout)

            def release(self):
                self._lock.release()

        class InterleavingSequence:
            def __init__(self):
                self._value = 0
                self._lock = InterleavingLock()
                self._signaled = False

            def get_lock(self):
                return self._lock

            def get_obj(self):
                return self

            @property
            def value(self):
                if threading.current_thread().name == "gallery" and not self._signaled:
                    self._signaled = True
                    start_contender.set()
                    self.assert_contender_attempted()
                return self._value

            @value.setter
            def value(self, value):
                self._value = value

            @staticmethod
            def assert_contender_attempted():
                if not contender_attempted.wait(1):
                    raise AssertionError(
                        "concurrent mutation never attempted allocation"
                    )

        sequence = InterleavingSequence()
        gallery = []
        contender = []

        def reserve_gallery():
            gallery.append(
                client_module._rich_queue_tuple("cdn_batch", "gallery", 1, [], 101)
            )

        def reserve_contender():
            start_contender.wait(1)
            contender.extend(client_module._reserve_rich_mutation_versions(1))
            contender_done.set()

        with (
            mock.patch.object(client_module, "_rich_writer_epoch", 7),
            mock.patch.object(client_module, "_rich_mutation_seq", sequence),
        ):
            threads = [
                threading.Thread(target=reserve_gallery, name="gallery"),
                threading.Thread(target=reserve_contender, name="contender"),
            ]
            for thread in reversed(threads):
                thread.start()
            for thread in threads:
                thread.join(2)
                self.assertFalse(thread.is_alive())

        self.assertEqual(gallery[0][4:], (101, (7 << 32) | 1, (7 << 32) | 2))
        self.assertEqual(contender, [(7 << 32) | 3])
        self.assertEqual(sequence._lock.gallery_acquires, 1)

    def test_absent_writer_epoch_keeps_the_legacy_rich_tuple(self):
        with mock.patch.object(client_module, "_rich_writer_epoch", None):
            item = client_module._rich_queue_tuple(
                "metadata_batch", "info/run_info", 0, Metadata({}), 100
            )
        self.assertEqual(item[:3], ("metadata_batch", "info/run_info", 0))
        self.assertEqual(len(item), 4)

    def test_rich_mutation_allocator_fails_instead_of_waiting_on_a_dead_owner(self):
        sequence = mock.Mock()
        sequence.get_lock.return_value.acquire.return_value = False
        with (
            mock.patch.object(client_module, "_rich_writer_epoch", 7),
            mock.patch.object(client_module, "_rich_mutation_seq", sequence),
            self.assertRaisesRegex(RuntimeError, "allocator lock is unavailable"),
        ):
            client_module._reserve_rich_mutation_versions(1)
        sequence.get_lock.return_value.acquire.assert_called_once_with(
            timeout=client_module._STATUS_LOCK_TIMEOUT
        )
        sequence.get_lock.return_value.release.assert_not_called()

    def test_a_handler_kymo_chains_to_never_takes_a_reservations_versions(self):
        # A handler kymo chains to logs a gallery at each opcode of a reservation. The allocator's lock is reentrant, so inside it the handler would read the sequence the reservation had read and take the same versions, one gallery's of which the server rejects as DATA_LOSS: the reservation defers it, and every version is reserved once.
        reservation = client_module._reserve_rich_mutation_versions
        traced = {reservation.__code__, client_module._reserve_versions.__code__}
        for k in itertools.count():
            nested = []

            def handler(signum, frame):
                nested.extend(reservation(2))

            tracer, fired = _opcode_tracer(
                traced, k, lambda frame: signal.raise_signal(signal.SIGUSR1)
            )
            with (
                mock.patch.multiple(
                    client_module,
                    _rich_writer_epoch=7,
                    _rich_mutation_seq=multiprocessing.Value("I", 0),
                ),
                _kymo_handles({signal.SIGUSR1: handler}),
            ):
                with _tracing(tracer):
                    outer = reservation(2)
                reserved = client_module._rich_mutation_seq.value
            if not fired:
                break
            state = f"signal at opcode {k}"
            self.assertEqual(len(nested), 2, state)
            self.assertFalse(set(outer) & set(nested), state)
            self.assertEqual(reserved, 4, state)
        self.assertGreater(k, 10)

    def test_run_metadata_uses_one_immutable_ordered_worker_record(self):
        metadata = {"config": {"learning_rate": 0.1}}
        with (
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_run_metadata", metadata),
            mock.patch.object(client_module, "_publish_queue_items") as publish,
        ):
            client_module._push_run_metadata()
            # Empty items are skipped at publication.
            queued = [item for item in publish.call_args.args[0] if item]
            metadata["config"]["learning_rate"] = 0.2

        self.assertEqual(len(queued), 1)
        decoded = client_module._decode_queue_item(queued[0])
        self.assertEqual(len(decoded), 1)
        kind, name, step, snapshot = decoded[0]
        self.assertEqual((kind, name, step), ("metadata_batch", "info/run_info", 0))
        self.assertEqual(snapshot.data["config"]["learning_rate"], 0.1)

    def test_ack_delta_accepts_duplicate_and_advance(self):
        self.assertEqual(client_module._validated_ack_delta(10, 10, 5), 0)
        self.assertEqual(client_module._validated_ack_delta(15, 10, 5), 5)

    def test_ack_delta_rejects_decrease_and_overshoot(self):
        for cumulative in (9, 16):
            with self.subTest(cumulative=cumulative), self.assertRaises(ValueError):
                client_module._validated_ack_delta(cumulative, 10, 5)

    def test_log_cdn_warns_when_its_publication_crosses_the_backlog_threshold(self):
        status = _QueueStatus(client_module._BACKLOG_WARN_THRESHOLD - 1)
        with (
            _upload_state(_metric_queue=mock.Mock(), _queue_status=status),
            mock.patch.object(client_module, "_last_backlog_warn", 0.0),
            mock.patch.object(
                client_module.time,
                "monotonic",
                return_value=client_module._BACKLOG_WARN_INTERVAL + 1,
            ),
            self.assertLogs("kymo", level="WARNING") as captured,
        ):
            client_module.log_cdn({"demo/image": "key.png"}, step=1)

        self.assertEqual(status.value, client_module._BACKLOG_WARN_THRESHOLD)
        self.assertIn(
            f"upload backlog: {client_module._BACKLOG_WARN_THRESHOLD} points queued",
            "\n".join(captured.output),
        )

    def test_nonfinite_upload_timeout_becomes_an_immediate_timeout(self):
        with (
            _upload_state(_queue_status=_QueueStatus(1)),
            self.assertLogs("kymo", level="WARNING"),
        ):
            self.assertFalse(client_module.wait_for_upload(timeout=float("nan")))

    def test_upload_wait_sleep_is_bounded_by_its_deadline(self):
        sleeps = []
        with (
            _upload_state(_queue_status=_QueueStatus(1)),
            mock.patch.object(client_module.time, "sleep", side_effect=sleeps.append),
            self.assertLogs("kymo", level="WARNING"),
        ):
            self.assertFalse(client_module.wait_for_upload(timeout=0.01))

        self.assertTrue(sleeps)
        self.assertLessEqual(max(sleeps), 0.01)

    def test_poisoned_accounting_lock_fails_instead_of_hanging_callers(self):
        class StuckLock:
            @staticmethod
            def acquire(timeout):
                del timeout
                return False

            @staticmethod
            def release():
                raise AssertionError("an unacquired lock was released")

        class StuckStatus:
            value = 3

            @staticmethod
            def get_lock():
                return StuckLock()

        with (
            _upload_state(_queue_status=StuckStatus(), _STATUS_LOCK_TIMEOUT=0.01),
            self.assertRaisesRegex(RuntimeError, "accounting remained locked"),
        ):
            client_module.wait_for_upload()

        with (
            mock.patch.object(client_module, "_queue_status", StuckStatus()),
            mock.patch.object(client_module, "_STATUS_LOCK_TIMEOUT", 0.0),
            self.assertRaisesRegex(RuntimeError, "accounting lock is unavailable"),
        ):
            client_module._publish_queue_items([[numeric(1)]])

    def test_rich_publication_queues_one_immutable_serialized_snapshot(self):
        target = _ListQueue()
        status = _QueueStatus()
        payload = bytearray(b"before")
        _OneShotReducible.calls = 0
        with (
            _upload_state(_metric_queue=target, _queue_status=status),
            mock.patch("kymo._capture.drain_buffers", return_value=("", "")),
        ):
            client_module.log(
                {
                    "demo/mutable": Resource(payload, "mutable.bin"),
                    "demo/one-shot": Image(_OneShotReducible()),
                },
                step=1,
            )

        payload[:] = b"after!"
        decoded = [client_module._decode_queue_item(item)[0] for item in target.items]
        self.assertEqual(decoded[0][3][0].data, b"before")
        self.assertEqual(decoded[1][3][0].data, b"frozen")
        self.assertEqual(_OneShotReducible.calls, 1)
        self.assertEqual(status.value, 2)

    def _log(self, metrics):
        target = mock.Mock()
        with (
            _upload_state(_metric_queue=target, _queue_status=_QueueStatus()),
            mock.patch("kymo._capture.drain_buffers", return_value=("", "")),
            mock.patch.dict(
                sys.modules, {"torch": types.SimpleNamespace(Tensor=_StubTensor)}
            ),
        ):
            client_module.log(metrics, step=1)
        return [call.args[0] for call in target.put.call_args_list]

    def test_rich_snapshot_pickles_tensors_as_host_arrays(self):
        for device in ("cuda", "cpu"):
            with self.subTest(device=device):
                tensor = _StubTensor(device, [1, 2, 3])
                (queued,) = self._log({"demo/gallery": [Image(tensor)]})
                snapshot = client_module._decode_queue_item(queued)[0][3][0].data
                self.assertIsInstance(snapshot, np.ndarray)
                self.assertEqual(snapshot.tolist(), [1, 2, 3])
                self.assertEqual(tensor.device.type, device)
        # A meta tensor has no data; it pickles as itself.
        (queued,) = self._log({"demo/gallery": [Image(_StubTensor("meta", [1]))]})
        snapshot = client_module._decode_queue_item(queued)[0][3][0].data
        self.assertEqual(snapshot.device.type, "meta")

    @staticmethod
    def _host_pickle(obj) -> bytes:
        buffer = io.BytesIO()
        client_module._HostTensorPickler(buffer, protocol=pickle.HIGHEST_PROTOCOL).dump(
            obj
        )
        return buffer.getvalue()

    @unittest.skipUnless(torch is not None, "requires torch")
    def test_rich_snapshot_pickles_a_cpu_view_without_its_storage(self):
        batch = torch.rand(32, 3, 64, 64)
        # One image of the batch, a strided channel across every image, and every image as one item's list (torch writes the whole storage once per view).
        for views in ([batch[5]], [batch[:, 0]], list(batch)):
            with self.subTest(shape=tuple(views[0].shape), views=len(views)):
                own = sum(view.nelement() * view.element_size() for view in views)
                payload = self._host_pickle(views)
                self.assertGreaterEqual(len(payload), own)
                self.assertLess(len(payload), own + 4096)
                restored = pickle.loads(payload)
                for array, view in zip(restored, views):
                    self.assertTrue(np.array_equal(array, view.numpy()))

    @unittest.skipUnless(torch is not None, "requires torch")
    def test_rich_snapshot_pickles_other_tensors_as_host_tensors(self):
        expanded = torch.rand(1, 64, 64).expand(3, 64, 64)
        for name, tensor in (
            ("bfloat16", torch.rand(4, 4).to(torch.bfloat16)[1]),
            ("conjugated", torch.rand(3, dtype=torch.complex64).conj()),
            ("sparse", torch.sparse_coo_tensor([[0, 2], [1, 0]], [1.0, 2.0], (3, 3))),
            ("nested", torch.nested.nested_tensor([torch.rand(2), torch.rand(3)])),
            (
                "per-channel quantized",
                torch.quantize_per_channel(
                    torch.rand(2, 3),
                    torch.tensor([0.1, 0.2]),
                    torch.tensor([0, 1]),
                    0,
                    torch.quint8,
                ),
            ),
            # Its array would repeat the stored row three times.
            ("expanded", expanded),
        ):
            with self.subTest(name):
                payload = self._host_pickle(tensor)
                restored = pickle.loads(payload)
                self.assertIsInstance(restored, torch.Tensor)
                if name == "nested":
                    self.assertTrue(
                        all(map(torch.equal, restored.unbind(), tensor.unbind()))
                    )
                elif name == "per-channel quantized":
                    self.assertTrue(torch.equal(restored.int_repr(), tensor.int_repr()))
                else:
                    self.assertTrue(
                        torch.equal(
                            restored.resolve_conj().to_dense(),
                            tensor.resolve_conj().to_dense(),
                        )
                    )
        self.assertLess(
            len(self._host_pickle(expanded)),
            expanded.nelement() * expanded.element_size(),
        )

    def test_metadata_snapshot_is_frozen_to_its_rendered_json(self):
        data = {"stats": _StubTensor("cuda", [1]), "shape": (2, 3), 5: None}
        manifest = client_module.metadata_manifest(data)
        for batch in (
            ("metadata_batch", "demo/meta", 1, Metadata(data)),
            ("metadata_batch_mutation", "demo/meta", 1, Metadata(data), 7, 9),
        ):
            with self.subTest(kind=batch[0]):
                # No torch in sys.modules: the stub tensor would refuse to pickle if it reached the payload.
                with mock.patch.dict(sys.modules, {"torch": None}):
                    ((_, payload),) = client_module._snapshot_rich_queue_items([batch])
                (decoded,) = client_module._decode_queue_item(
                    (client_module._SERIALIZED_RICH_QUEUE_ITEM, payload)
                )
                self.assertEqual(decoded[:3] + decoded[4:], batch[:3] + batch[4:])
                self.assertEqual(
                    client_module.metadata_manifest(decoded[3].data), manifest
                )

    def test_metadata_snapshot_replaces_lone_surrogates(self):
        # os.fsdecode(b"file-\xff.png"): a POSIX filename, cwd or argument that is not UTF-8. The escaped non-BMP text in "fine" also matches the guard and must come back unchanged.
        name, readable = "file-\udcff.png", "file-?.png"
        fine = {"fine": "\u00fcn\u00efcode \U0001f600"}
        # A key that becomes an existing one after the replacement merges with it, the later value winning.
        data = {readable: "old", "path": name, name: [name, {"inner": name}], **fine}
        (item,) = client_module._snapshot_rich_queue_items(
            [("metadata_batch", "demo/meta", 1, Metadata(data))]
        )
        (decoded,) = client_module._decode_queue_item(item)
        self.assertEqual(
            decoded[3].data,
            {"path": readable, readable: [readable, {"inner": readable}], **fine},
        )

    def test_unpicklable_rich_payload_fails_before_capture_or_publication(self):
        target = _ListQueue()
        status = _QueueStatus()
        capture = mock.Mock(return_value=("captured", ""))
        with (
            _upload_state(_metric_queue=target, _queue_status=status),
            mock.patch("kymo._capture.has_output", return_value=True),
            mock.patch("kymo._capture.drain_buffers", capture),
            self.assertRaisesRegex(TypeError, "multiprocessing-serializable"),
        ):
            client_module.log({"demo/bad": Image(lambda: None)}, step=1)

        capture.assert_not_called()
        self.assertEqual(target.items, [])
        self.assertEqual(status.value, 0)

    def test_nonfinite_metadata_fails_before_capture_or_publication(self):
        # Both queue shapes must reject synchronously: legacy servers (no
        # writer epoch -> metadata_batch) and current servers (negotiated
        # epoch -> metadata_batch_mutation).
        epoch_states = (
            ("legacy", None, None),
            ("versioned", 1, multiprocessing.Value("I", 0)),
        )
        for value in (float("nan"), float("inf"), float("-inf")):
            for label, epoch, seq in epoch_states:
                with self.subTest(value=value, epoch=label):
                    target = mock.Mock()
                    status = _QueueStatus()
                    capture = mock.Mock(return_value=("captured", ""))
                    with (
                        _upload_state(_metric_queue=target, _queue_status=status),
                        mock.patch.object(client_module, "_rich_writer_epoch", epoch),
                        mock.patch.object(client_module, "_rich_mutation_seq", seq),
                        mock.patch("kymo._capture.has_output", return_value=True),
                        mock.patch("kymo._capture.drain_buffers", capture),
                        self.assertRaisesRegex(
                            ValueError,
                            "metadata metric 'demo/meta' must contain finite "
                            "JSON-serializable values",
                        ),
                    ):
                        client_module.log(
                            {"demo/meta": Metadata({"nested": {"value": value}})},
                            step=1,
                        )

                    capture.assert_not_called()
                    target.put.assert_not_called()
                    self.assertEqual(status.value, 0)

    def test_mixed_invalid_rich_list_fails_before_capture_or_publication(self):
        target = mock.Mock()
        status = _QueueStatus()
        capture = mock.Mock(return_value=("captured", ""))
        with (
            _upload_state(_metric_queue=target, _queue_status=status),
            mock.patch("kymo._capture.has_output", return_value=True),
            mock.patch("kymo._capture.drain_buffers", capture),
            self.assertRaisesRegex(
                TypeError,
                "rich metric 'demo/bad' item 1 must be Image or Resource, got object",
            ),
        ):
            client_module.log(
                {"demo/bad": [Image(b"valid"), object(), Resource(b"valid", "x.bin")]},
                step=1,
            )

        capture.assert_not_called()
        target.put.assert_not_called()
        self.assertEqual(status.value, 0)

    def test_tagged_numeric_lists_accept_floatable_scalars_in_any_order(self):
        target = _ListQueue()
        status = _QueueStatus()
        with (
            _upload_state(_metric_queue=target, _queue_status=status),
            mock.patch("kymo._capture.drain_buffers", return_value=("", "")),
        ):
            client_module.log({"demo/tags": [1.0, np.float32(2)]}, step=1)
            client_module._last_log_duration_ms = 0.0
            client_module.log({"demo/tags": [np.float32(2), 1.0]}, step=1)

        decoded = [client_module._decode_queue_item(item) for item in target.items]
        self.assertEqual(
            [[point[:5] for point in batch] for batch in decoded],
            [
                [
                    ("numeric_tagged_ts", "demo/tags", 1, 1.0, "0"),
                    ("numeric_tagged_ts", "demo/tags", 1, 2.0, "1"),
                ],
                [
                    ("numeric_tagged_ts", "demo/tags", 1, 2.0, "0"),
                    ("numeric_tagged_ts", "demo/tags", 1, 1.0, "1"),
                ],
            ],
        )
        self.assertEqual(status.value, 4)

    def test_empty_tagged_lists_do_not_abort_valid_sibling_metrics(self):
        target = _ListQueue()
        status = _QueueStatus()
        with (
            _upload_state(_metric_queue=target, _queue_status=status),
            mock.patch("kymo._capture.drain_buffers", return_value=("", "")),
        ):
            client_module.log(
                {"train/loss": 1.0, "eval/per_class": []},
                step=1,
            )
            client_module._last_log_duration_ms = 0.0
            client_module.log({"eval/per_class": []}, step=2)

        decoded = [client_module._decode_queue_item(item) for item in target.items]
        self.assertEqual(
            [[point[:4] for point in batch] for batch in decoded],
            [[("numeric_ts", "train/loss", 1, 1.0)]],
        )
        self.assertEqual(status.value, 1)

    def test_explicit_log_overhead_metric_is_not_overwritten(self):
        target = _ListQueue()
        status = _QueueStatus()
        with (
            _upload_state(
                _metric_queue=target, _queue_status=status, _last_log_duration_ms=12.5
            ),
            mock.patch("kymo._capture.drain_buffers", return_value=("", "")),
        ):
            client_module.log({"system/log_overhead_ms": 99.0}, step=1)
            client_module._last_log_duration_ms = 12.5
            client_module.log({"train/loss": 1.0}, step=2)

        self.assertEqual(len(target.items), 2)
        decoded = [client_module._decode_queue_item(item) for item in target.items]
        self.assertEqual(len(decoded[0]), 1)
        self.assertEqual(
            decoded[0][0][:4],
            ("numeric_ts", "system/log_overhead_ms", 1, 99.0),
        )
        self.assertEqual(
            [point[:4] for point in decoded[1]],
            [
                ("numeric_ts", "train/loss", 2, 1.0),
                ("numeric_ts", "system/log_overhead_ms", 2, 12.5),
            ],
        )
        self.assertEqual(status.value, 3)

    def test_invalid_tagged_numeric_list_fails_before_publication(self):
        target = mock.Mock()
        status = _QueueStatus()
        capture = mock.Mock(return_value=("captured", ""))
        with (
            _upload_state(_metric_queue=target, _queue_status=status),
            mock.patch("kymo._capture.has_output", return_value=True),
            mock.patch("kymo._capture.drain_buffers", capture),
            self.assertRaises(TypeError),
        ):
            client_module.log(
                {"train/loss": 1.0, "demo/tags": [np.float32(2), object()]},
                step=1,
            )

        capture.assert_not_called()
        target.put.assert_not_called()
        self.assertEqual(status.value, 0)

    def test_finite_values_that_overflow_float32_fail_before_publication(self):
        for value in (3.5e38, -3.5e38):
            for metrics in (
                {"train/loss": value},
                {"train/loss": 1.0, "demo/tags": [2.0, value]},
            ):
                target = mock.Mock()
                capture = mock.Mock(return_value=("captured", ""))
                with (
                    self.subTest(value=value, metrics=metrics),
                    _upload_state(_metric_queue=target, _queue_status=_QueueStatus()),
                    mock.patch("kymo._capture.has_output", return_value=True),
                    mock.patch("kymo._capture.drain_buffers", capture),
                    self.assertRaisesRegex(OverflowError, "protobuf float32 range"),
                ):
                    client_module.log(metrics, step=1)

                capture.assert_not_called()
                target.put.assert_not_called()

    def test_float32_boundary_and_intentional_nonfinite_markers_encode(self):
        values = [
            3.4028234663852886e38,
            -3.4028234663852886e38,
            3.40282355e38,
            float("nan"),
            float("inf"),
            float("-inf"),
        ]
        for value in values:
            with self.subTest(value=value):
                normalized = client_module._normalize_numeric_value(value)
                point = wire_module._tuple_to_point(
                    ("numeric_ts", "train/loss", 1, normalized, 1_700_000_000_000)
                )
                if math.isnan(value):
                    self.assertTrue(math.isnan(point.value))
                elif value == 3.40282355e38:
                    self.assertEqual(point.value, 3.4028234663852886e38)
                else:
                    self.assertEqual(point.value, value)

    def test_log_fast_path_publishes_what_the_full_checks_publish(self):
        class FloatSubclass(float):
            pass

        above = math.nextafter(wire_module._F32_OVERFLOW, 0)
        accepted = [
            {
                "train/loss": 1.0,
                "é/name": 1.0,
                "x" * 2048: 1.0,
                "é" * 1024: 1.0,
            },
            {
                f"v/{index}": value
                for index, value in enumerate(
                    [
                        1.5,
                        -0.0,
                        5e-324,
                        1e-50,
                        3.4028234663852886e38,
                        -3.4028234663852886e38,
                        above,
                        float("nan"),
                        float("inf"),
                        float("-inf"),
                        True,
                        7,
                        np.float32(1.25),
                        np.float64(2.5),
                        FloatSubclass(3.5),
                    ]
                )
            },
        ]
        rejected = [
            ({"x" * 2049: 1.0}, ValueError),
            ({"é" * 1025: 1.0}, ValueError),
            ({"a\x00b": 1.0}, ValueError),
            ({"\udc80": 1.0}, UnicodeEncodeError),
            ({5: 1.0}, TypeError),
            ({"v": wire_module._F32_OVERFLOW}, OverflowError),
            ({"v": -wire_module._F32_OVERFLOW}, OverflowError),
        ]

        def shape(point):
            return point[:3] + (repr(point[3]), type(point[1]), type(point[3]))

        for metrics in accepted:
            (item,) = self._log(metrics)
            batch = client_module._decode_queue_item(item)
            expected = [
                (
                    "numeric_ts",
                    client_module._normalize_metric_name(name),
                    1,
                    client_module._normalize_numeric_value(value),
                )
                for name, value in metrics.items()
            ]
            self.assertEqual(
                [shape(point) for point in batch],
                [shape(point) for point in expected],
            )

        for metrics, error in rejected:
            with self.subTest(metrics=metrics), self.assertRaises(error) as raised:
                self._log(metrics)
            self.assertIs(type(raised.exception), error)

    def test_public_step_and_string_subclasses_are_frozen_to_wire_primitives(self):
        target = _ListQueue()
        status = _QueueStatus()
        _StepLike.calls = 0
        with (
            _upload_state(_metric_queue=target, _queue_status=status),
            mock.patch("kymo._capture.drain_buffers", return_value=("", "")),
        ):
            client_module.log({_StringSubclass("train/loss"): 1.0}, _StepLike())
            client_module.log_cdn(
                {_StringSubclass("demo/image"): _StringSubclass("key.png")},
                _StepLike(),
            )

        numeric_point = client_module._decode_queue_item(target.items[0])[0]
        cdn_point = target.items[1][0]
        self.assertIs(type(numeric_point[1]), str)
        self.assertIs(type(numeric_point[2]), int)
        self.assertIs(type(cdn_point[1]), str)
        self.assertIs(type(cdn_point[2]), int)
        self.assertIs(type(cdn_point[3]), str)
        self.assertEqual(_StepLike.calls, 2)

    def test_invalid_step_fails_before_capture_or_publication(self):
        target = mock.Mock()
        capture = mock.Mock(return_value=("captured", ""))
        with (
            _upload_state(_metric_queue=target),
            mock.patch("kymo._capture.has_output", return_value=True),
            mock.patch("kymo._capture.drain_buffers", capture),
            self.assertRaisesRegex(TypeError, "step must be an integer"),
        ):
            client_module.log({"train/loss": 1.0}, object())

        capture.assert_not_called()
        target.put.assert_not_called()

    def test_step_must_fit_protobuf_int64(self):
        for step in (-(1 << 63) - 1, 1 << 63):
            with (
                self.subTest(step=step),
                self.assertRaisesRegex(OverflowError, "signed 64-bit"),
            ):
                client_module._normalize_step(step)

    def test_a_log_whose_start_came_before_a_poller_tick_drains_at_a_later_text_step(
        self,
    ):
        # A poller tick keys its text at the tick's time; a log() whose start came before it then drains at a strictly later step.
        target = _ListQueue()
        with _upload_state(_metric_queue=target):
            with _captured(["tick\n"]):
                client_module._publish_queue_items([], capture_ms=2000)
            with (
                _captured(["logged\n"]),
                mock.patch.object(client_module.time, "time", return_value=1.0),
            ):
                client_module.log({"loss": 1.0}, step=7)

        text = [
            point
            for item in target.items
            for point in client_module._decode_queue_item(item)
            if point[0] == "text_ts"
        ]
        self.assertEqual(
            [(point[2], point[4]) for point in text], [(2000, 2000), (2001, 1000)]
        )

    def _wait_with_unproven(
        self, backlog, unproven, *, process=None, timeout=None, uncharged=()
    ):
        """wait_for_upload() with _unproven_points at unproven and the groups uncharged held back uncharged, on a clock each sleep advances; backlog is the status, or a function of the seconds waited that returns it. Returns (result or RuntimeError, seconds waited, log, _unproven_points after)."""

        clock = [1000.0]

        class Status(_QueueStatus):
            @property
            def value(self):
                return backlog(clock[0] - 1000.0) if callable(backlog) else backlog

            @value.setter
            def value(self, _value):
                pass

        def sleep(seconds):
            clock[0] += seconds

        with (
            _upload_state(
                _queue_status=Status(),
                _upload_process=process or _Process(),
                _unproven_points=unproven,
                _uncharged=deque(uncharged),
            ),
            mock.patch.object(client_module.time, "sleep", sleep),
            mock.patch.object(client_module.time, "monotonic", lambda: clock[0]),
            self.assertLogs("kymo", level="DEBUG") as logs,
        ):
            client_module._log.debug("start")
            try:
                result = client_module.wait_for_upload(timeout=timeout)
            except RuntimeError as error:
                result = error
            return (
                result,
                clock[0] - 1000.0,
                "\n".join(logs.output),
                client_module._unproven_points,
            )

    def test_an_untimed_wait_reports_points_it_cannot_prove_instead_of_hanging(self):
        # A put an exception interrupted keeps its charge; if the queue never took the group, nothing resolves it. Once the backlog has stayed within it for longer than the server's flush interval, the wait gives up on it.
        result, waited, log, _ = self._wait_with_unproven(1, 1)
        self.assertIs(result, False)
        self.assertGreater(waited, client_module._SERVER_FLUSH_INTERVAL)
        self.assertLess(waited, client_module._SERVER_FLUSH_INTERVAL + 1.1)
        self.assertIn("at most 1 point(s)", log)
        # Delivered after all.
        result, _, _, unproven = self._wait_with_unproven(0, 1)
        self.assertIs(result, True)
        self.assertEqual(unproven, 0)
        # A point beyond the unproven ones is still in flight: the wait goes on until its timeout.
        result, _, log, unproven = self._wait_with_unproven(2, 1, timeout=5)
        self.assertIs(result, False)
        self.assertIn("points still queued", log)
        self.assertEqual(unproven, 1)

    def test_unproven_points_never_exceed_a_backlog_the_wait_saw(self):
        # The backlog goes 3, 2, then stays at 1 as acknowledgements arrive: at most 1 of the 5 points stays unproven, and the wait gives up only once that has held for the flush interval.
        result, waited, log, unproven = self._wait_with_unproven(
            lambda seconds: 3 if seconds < 1 else 2 if seconds < 2 else 1, 5
        )
        self.assertIs(result, False)
        self.assertEqual(unproven, 1)
        self.assertGreater(waited, 2 + client_module._SERVER_FLUSH_INTERVAL)
        self.assertIn("at most 1 point(s)", log)
        # Once a backlog of 0 proved them delivered, a later wait waits for its own points.
        result, waited, _, unproven = self._wait_with_unproven(
            lambda seconds: 1 if seconds < 3 else 0, 0
        )
        self.assertIs(result, True)
        self.assertGreater(waited, 3)

    def test_groups_the_wait_puts_start_its_lowest_backlog_again(self):
        # A wait that puts groups held back uncharged (a charge that met a busy accounting lock left them) read a backlog without their points: it waits for their acknowledgement, which a small batch's flush can hold past the flush interval, rather than take that lower read for unproven points; with one point unproven, it gives up on that one alone, the flush interval after the backlog fell to it.
        for unproven, result, waited in ((0, True, 3), (1, False, 5)):
            got, seconds, log, _ = self._wait_with_unproven(
                lambda seconds: (
                    unproven
                    + 3 * (bool(client_module._metric_queue.items) and seconds < 3)
                ),
                unproven,
                uncharged=[[numeric(1), numeric(2), numeric(3)]],
            )
            with self.subTest(unproven=unproven):
                self.assertIs(got, result)
                self.assertGreater(seconds, waited)
                self.assertNotIn("at most 0 point(s)", log)

    def test_a_dead_worker_is_reported_before_unproven_points(self):
        result, _, _, _ = self._wait_with_unproven(1, 1, process=_Process(1))
        self.assertIsInstance(result, RuntimeError)
        self.assertIn("worker died", str(result))

    def test_the_pollers_publications_never_hold_off_the_unproven_exit(self):
        # The poller publishes a group every 2 s, which raises the backlog until the worker acknowledges it: the backlog never stays unchanged for the flush interval, but it goes no lower than the unproven point, and the wait gives up on it.
        result, waited, log, _ = self._wait_with_unproven(
            lambda seconds: 2 if seconds % 2.0 >= 1.0 else 1, 1, timeout=20
        )
        self.assertIs(result, False)
        self.assertGreater(waited, client_module._SERVER_FLUSH_INTERVAL)
        self.assertLess(waited, client_module._SERVER_FLUSH_INTERVAL + 1.1)
        self.assertIn("at most 1 point(s)", log)

    def test_a_publication_that_cannot_charge_keeps_what_earlier_calls_accepted(self):
        # The drained text and the groups held back uncharged go back, ahead of newer ones, and the next publication charges and puts them first; the call's own groups, whose caller sees the error, do not. An exited worker is reported as such.
        text = [("text_ts", "logs/std_out", 9, "out", 9)]

        for process, error in (
            (_Process(), client_module._AccountingLockUnavailable),
            (_Process(1), client_module._WorkerExited),
        ):
            target, status = _ListQueue(), _QueueStatus()
            with (
                self.subTest(error=error),
                mock.patch.multiple(
                    client_module,
                    _metric_queue=target,
                    _queue_status=status,
                    _held_back=deque([[numeric(1)]]),
                    _uncharged=deque([[numeric(2)]]),
                    _unproven_points=0,
                    _init_pid=os.getpid(),
                    _upload_process=process,
                    _upload_failure=_QueueStatus(),
                    _STATUS_LOCK_TIMEOUT=0.0,
                ),
                mock.patch.object(
                    client_module._capture, "has_output", return_value=True
                ),
                mock.patch.object(
                    client_module, "_drain_capture_points", return_value=text
                ),
            ):
                status.value = 1
                with status.lock, self.assertRaises(error):
                    client_module._publish_queue_items([[numeric(3)]], capture_ms=9)
                self.assertEqual(list(client_module._held_back), [[numeric(1)]])
                self.assertEqual(list(client_module._uncharged), [[numeric(2)], text])
                self.assertEqual((status.value, target.items), (1, []))
                client_module._upload_process = _Process()
                client_module._publish_queue_items([[numeric(4)]])
                self.assertEqual(
                    target.items, [[numeric(1)], [numeric(2)], text, [numeric(4)]]
                )
                self.assertEqual(status.value, 4)
                self.assertEqual(client_module._unproven_points, 0)

    def test_a_handlers_raise_anywhere_in_a_publication_loses_no_accepted_group(self):
        # A handler kymo does not own raises at each opcode of a publication with groups held back, charged and not, and drained text, which joins the call's group when that is a numeric batch, and of its take of the groups held back (_take_held_back), a raise as a popleft() returns included. Every group earlier calls accepted that the publication held then is put once by the next publication, or, the one whose put it interrupted, counted as unproven; every charge is a group put or unproven, so no wait hangs. (Text a raise catches between the drain's return and its store is lost: a documented limit.)
        text = [("text_ts", "logs/std_out", 9, "out", 9)]
        batch = (client_module._NUMERIC_BATCH_QUEUE_ITEM, 3, 9, ["a"], [3.0], [None])

        class Timeout(Exception):
            pass

        def groups(item):
            # A numeric batch the drained text joined holds two groups.
            if item[0] != client_module._NUMERIC_TEXT_BATCH_QUEUE_ITEM:
                return [item]
            return [(client_module._NUMERIC_BATCH_QUEUE_ITEM, *item[1:6]), item[6]]

        for own in ([numeric(3)], batch):
            for k in itertools.count():
                holds_text = []

                def raise_timeout(frame):
                    holds_text.append(bool(frame.f_locals.get("text_points")))
                    if _a_handler_runs_at(frame):
                        raise Timeout

                tracer, fired = _opcode_tracer(
                    {
                        client_module._publish_held.__code__,
                        client_module._take_held_back.__code__,
                    },
                    k,
                    raise_timeout,
                )
                target, status = _ListQueue(), _QueueStatus(1)
                with (
                    mock.patch.multiple(
                        client_module,
                        _metric_queue=target,
                        _queue_status=status,
                        _held_back=deque([[numeric(1)]]),
                        _uncharged=deque([[numeric(2)]]),
                        _unproven_points=0,
                    ),
                    mock.patch.object(
                        client_module._capture, "has_output", return_value=True
                    ),
                    mock.patch.object(
                        client_module, "_drain_capture_points", side_effect=[text, []]
                    ),
                ):
                    with _tracing(tracer), contextlib.suppress(Timeout):
                        client_module._publish_queue_items([own], capture_ms=9)
                    client_module._publish_queue_items([])
                    unproven = client_module._unproven_points
                    stores = list(client_module._held_back) + list(
                        client_module._uncharged
                    )
                state = f"raised at opcode {k}, batch {own is batch}"
                put_groups = [group for item in target.items for group in groups(item)]
                accepted = [[numeric(1)], [numeric(2)]]
                if not fired or holds_text[0] or text in put_groups:
                    accepted.append(text)
                self.assertEqual(stores, [], state)
                for group in [*accepted, own]:
                    self.assertLessEqual(put_groups.count(group), 1, state)
                lost = [group for group in accepted if group not in put_groups]
                self.assertLessEqual(len(lost), 1, state)
                if lost:
                    self.assertGreaterEqual(unproven, 1, state)
                put = sum(map(client_module._queue_item_size, target.items))
                self.assertLessEqual(put, status.value, state)
                self.assertLessEqual(status.value - put, unproven, state)
                if not fired:
                    break
            self.assertGreater(k, 50)

    def test_a_raise_inside_the_charge_counts_it_as_unproven(self):
        # A handler's exception once the accounting lock is taken may come after the charge, with the groups lost: an untimed wait must not wait for them. A charge the lock never allowed is no charge.
        real = client_module._charge_backlog

        def charge_then_raise(points, hard_deadline):
            real(points, hard_deadline)
            raise KeyboardInterrupt

        def lock_busy(points, hard_deadline):
            raise client_module._AccountingLockUnavailable("busy")

        for change, error, unproven in (
            (charge_then_raise, KeyboardInterrupt, 3),
            (lock_busy, client_module._AccountingLockUnavailable, 0),
        ):
            with (
                self.subTest(error=error),
                mock.patch.object(client_module, "_metric_queue", _ListQueue()),
                mock.patch.object(client_module, "_queue_status", _QueueStatus()),
                mock.patch.object(client_module, "_held_back", deque()),
                mock.patch.object(client_module, "_unproven_points", 0),
                mock.patch.object(client_module, "_charge_backlog", change),
            ):
                with self.assertRaises(error):
                    client_module._publish_queue_items(
                        [[numeric(1)], [numeric(2), numeric(3)]]
                    )
                self.assertEqual(client_module._unproven_points, unproven)
                self.assertEqual(client_module._metric_queue.items, [])

    def test_a_wait_never_erases_a_charge_counted_after_its_read(self):
        # Another thread's wait reads the backlog, and an interrupted publication then charges its group and counts it as unproven: lowering the count to the backlog read before would erase it, and the next untimed wait would hang. The read and the lowering are one step, which the count waits for.
        status = _QueueStatus()
        read = client_module._read_delivery_status
        first_read, published, results = threading.Event(), threading.Event(), []

        def read_before_a_charge(timeout):
            snapshot = read(timeout)
            if not first_read.is_set():
                first_read.set()
                published.wait(0.2)
            return snapshot

        class AmbiguousQueue:
            def put(self, item, block=True, timeout=None):
                # A handler's raise, perhaps before the queue took the group.
                raise KeyboardInterrupt

        with _upload_state(
            _metric_queue=AmbiguousQueue(),
            _queue_status=status,
            _upload_process=_Process(),
            _read_delivery_status=read_before_a_charge,
        ):
            waiter = threading.Thread(
                target=lambda: results.append(client_module.wait_for_upload(timeout=5))
            )
            waiter.start()
            self.assertTrue(first_read.wait(5))
            with self.assertRaises(KeyboardInterrupt):
                client_module._publish_queue_items([[numeric(1)]])
            published.set()
            waiter.join(5)
            self.assertEqual(results, [True])
            self.assertEqual((status.value, client_module._unproven_points), (1, 1))

    def test_a_wait_on_another_thread_never_waits_for_groups_held_back(self):
        # Only the main thread's publications put the groups held back, charged (a full queue, a deferred signal) or not (a busy accounting lock): a wait_for_upload() on another thread returns False at once, with or without a timeout.
        for held, timeout in itertools.product(("_held_back", "_uncharged"), (None, 5)):
            results = []
            with (
                self.subTest(held=held, timeout=timeout),
                _upload_state(_queue_status=_QueueStatus(int(held == "_held_back"))),
                self.assertLogs("kymo", level="WARNING") as logs,
            ):
                getattr(client_module, held).append([numeric(1)])
                waiter = threading.Thread(
                    target=lambda: results.append(
                        client_module.wait_for_upload(timeout)
                    ),
                    daemon=True,
                )
                started = time.monotonic()
                waiter.start()
                waiter.join(3)
                self.assertEqual(results, [False])
                self.assertLess(time.monotonic() - started, 1)
                self.assertIn("1 point(s) wait for the main thread", logs.output[-1])

    def test_the_main_threads_wait_puts_the_groups_held_back_while_it_waits(self):
        # Another thread's timed wait holds a pending call's group back, charged, on a queue that is full for a second; the main thread, already waiting, puts it once there is room and returns True, untimed or within its timeout.
        for timeout in (None, 5):
            status, start, result = _QueueStatus(), time.monotonic(), []

            class FullForASecond(_ListQueue):
                def put(self, item, block=True, timeout=None):
                    if time.monotonic() - start < 1.0:
                        if block and timeout:
                            time.sleep(min(timeout, 0.05))
                        raise queue.Full
                    super().put(item)
                    with status.lock:
                        status.value -= len(item)  # delivered at once

            def hold_back():
                time.sleep(0.2)
                client_module._pending.append(
                    client_module._PendingLog([numeric(9)], [])
                )
                client_module.wait_for_upload(timeout=0.1)

            with (
                self.subTest(timeout=timeout),
                _upload_state(
                    _queue_status=status,
                    _metric_queue=FullForASecond(),
                    _upload_process=mock.Mock(),
                ),
                self.assertLogs("kymo", level="WARNING"),
            ):
                status.value = (
                    1  # a point on its way to the worker, so the main thread waits
                )
                other = threading.Thread(target=hold_back, daemon=True)
                other.start()

                def deliver_the_first():
                    other.join()
                    with status.lock:
                        status.value -= 1

                threading.Thread(target=deliver_the_first, daemon=True).start()

                class Hung(Exception):
                    pass

                def hung(signum, frame):
                    raise Hung

                # On this, the main thread; an alarm fails the test rather than hang it.
                previous = signal.signal(signal.SIGALRM, hung)
                signal.setitimer(signal.ITIMER_REAL, 4)
                try:
                    result.append(client_module.wait_for_upload(timeout))
                except Hung:
                    pass
                finally:
                    signal.setitimer(signal.ITIMER_REAL, 0)
                    signal.signal(signal.SIGALRM, previous)
                self.assertEqual(result, [True])
                self.assertEqual(client_module._metric_queue.items, [[numeric(9)]])
                self.assertEqual(status.value, 0)

    def test_a_handler_that_publishes_the_pending_calls_ends_the_budget_loop(self):
        # A signal during the over-budget loop's wait for the encoder runs a handler kymo chains to that publishes every pending call (a wait_for_upload(), log() or finish()): the loop stops rather than index the empty FIFO.
        def entry(step):
            pending = client_module._PendingLog([numeric(step)], [], galleries=True)
            pending.nbytes = 100
            return pending

        nested = []

        def await_encoder():
            if not nested:
                nested.append(True)
                client_module.wait_for_upload(timeout=0)

        target = _ListQueue()
        with (
            _upload_state(
                _metric_queue=target,
                _upload_process=mock.Mock(),
                _pending=deque([entry(1), entry(2)]),
                _PENDING_MAX_BYTES=1,
                _await_encoder=await_encoder,
            ),
            self.assertLogs("kymo", level="WARNING"),
        ):
            client_module._queue_pending_log(None, entry(3), [])
            self.assertEqual(list(client_module._pending), [])
        self.assertEqual(target.items, [[numeric(1)], [numeric(2)], [numeric(3)]])

    def test_no_loop_leaves_its_handler_at_the_back_edge(self):
        """A signal is handled at a loop's back-edge, and on CPython 3.13 and 3.14 some loops' back-edge lies outside the exception table entry of their body (cpython#130279): a handler that raises there skips the enclosing except, finally and with, leaving a lock held or a shutdown unfinished. kymo-sync's flagged loops catch only their own errors or close a directory scan."""
        if not hasattr(dis.Bytecode(numeric), "exception_entries"):
            self.skipTest("Python 3.10 has no exception table")

        def escapes(code):
            entries = dis.Bytecode(code).exception_entries
            opnames = {i.offset: i.opname for i in dis.get_instructions(code)}

            def handler(offset):
                # The innermost handler that catches: comprehension and except-body cleanups reraise outward.
                while (
                    entry := next(
                        (e for e in entries if e.start <= offset < e.end), None
                    )
                ) is not None:
                    if opnames[entry.target] == "PUSH_EXC_INFO":
                        return entry.target
                    offset = entry.target
                return None

            for i in dis.get_instructions(code):
                if i.opname == "JUMP_BACKWARD" and handler(i.argval) not in (
                    None,
                    handler(i.offset),
                ):
                    yield f"{code.co_filename}:{i.positions.lineno} {code.co_qualname}"

        def codes(code):
            yield code
            for const in code.co_consts:
                if isinstance(const, types.CodeType):
                    yield from codes(const)

        package = os.path.dirname(client_module.__file__)
        found = []
        for name in sorted(os.listdir(package)):
            if name.endswith(".py") and name != "sync.py":
                with open(os.path.join(package, name)) as file:
                    for code in codes(compile(file.read(), name, "exec")):
                        found.extend(escapes(code))
        self.assertEqual(found, [])

    def test_a_second_signal_at_any_opcode_of_the_handling_never_waits(self):
        # A handler can run between any two bytecodes. Inject a second signal at each opcode of kymo's signal handling (a superset of where handlers run) while the first is handled, the first deferred by a publication or arriving outside one, and handled by kymo's own flush or by a handler kymo chains to: none waits for a later section. They are handled in the order kymo's handler recorded them, never nested, except that the second interrupts a handler once its call has begun (_call_chained), as it would without kymo; and kymo's own flush runs once.
        functions = (
            client_module._signal_handler,
            client_module._run_deferred_signals,
            client_module._handle_deferred_signals,
            client_module._held_sections,
            client_module._handle_signal,
            client_module._call_chained,
        )
        traced = {function.__code__ for function in functions}
        handle_code = client_module._handle_signal.__code__
        chained_code = client_module._call_chained.__code__
        run_code = client_module._run_deferred_signals.__code__
        for own, deferred in (
            (False, True),
            (False, False),
            (True, True),
            (True, False),
        ):
            for k in itertools.count():
                handled, recorded, exits = [], [], []
                first_recorded, first_ran, late = [True], [None], [False]

                def record(signum, frame):
                    depth, f = 0, sys._getframe()
                    while f is not None:
                        depth += f.f_code is handle_code
                        f = f.f_back
                    handled.append((signum, depth))

                def flush(flush_timeout=None, signum=None):
                    # kymo's own flush, which its handler follows with an exit; the run is over after it.
                    record(signum, None)
                    client_module._is_initialized = False

                def second(frame):
                    first_recorded[0] = signal.SIGUSR1 in recorded
                    if frame.f_code is chained_code:
                        first_ran[0] = signal.SIGUSR1 in dict(handled)
                    late[0] = frame.f_code is run_code and (
                        signal.SIGUSR1 in dict(handled)
                    )
                    signal.raise_signal(signal.SIGUSR2)

                tracer, fired = _opcode_tracer(traced, k, second)

                def put(item):
                    signal.raise_signal(signal.SIGUSR1)
                    sys.settrace(tracer)

                class Recording(list):
                    def append(self, entry):
                        recorded.append(entry[0])
                        super().append(entry)

                waiting = Recording()
                handler = signal.SIG_DFL if own else record
                with (
                    mock.patch.object(client_module, "_deferred_signals", waiting),
                    mock.patch.object(client_module, "_metric_queue", _ListQueue(put)),
                    mock.patch.object(client_module, "_queue_status", _QueueStatus()),
                    mock.patch.multiple(
                        client_module,
                        # Each put raises a signal: only this publication's group may be put.
                        _held_back=deque(),
                        _uncharged=deque(),
                        _drain_and_shutdown=flush,
                        _exit_code=None,
                        _is_initialized=True,
                        _log=mock.Mock(),
                    ),
                    _kymo_handles({signal.SIGUSR1: handler, signal.SIGUSR2: handler}),
                ):
                    try:
                        if deferred:
                            client_module._publish_queue_items([[numeric(1)]])
                        else:
                            sys.settrace(tracer)
                            signal.raise_signal(signal.SIGUSR1)
                    except SystemExit as error:
                        exits.append(error.code)
                    finally:
                        sys.settrace(None)
                state = f"own={own}, deferred={deferred}, second signal at opcode {k}"
                self.assertEqual(list(waiting), [], state)
                if own:
                    # One flush: by the second if it arrived before the first was recorded, or interrupted the first's call before it ran, and exited first; else by the first, after which the second finds the run over. kymo's own handler exits after it, and the first exit propagates; a signal arriving between rounds once the first is handled runs at once, and its exit replaces the one about to be raised, as any handler's exception would.
                    flushed = signal.SIGUSR1
                    if not first_recorded[0] or first_ran[0] is False:
                        flushed = signal.SIGUSR2
                    depth = 2 if first_ran[0] is False else 1
                    self.assertEqual(handled, [(flushed, depth)], state)
                    last = signal.SIGUSR2 if late[0] else flushed
                    self.assertEqual(exits, [128 + last], state)
                else:
                    order = [signal.SIGUSR1, signal.SIGUSR2][: 1 + len(fired)]
                    if not first_recorded[0]:
                        # The second arrived first.
                        order.reverse()
                    expected = [(signum, 1) for signum in order]
                    if first_ran[0] is not None:
                        # The second interrupted the first's chained call, before or after the handler ran.
                        expected = [(signal.SIGUSR2, 2), (signal.SIGUSR1, 1)]
                        if first_ran[0]:
                            expected.reverse()
                    self.assertEqual(handled, expected, state)
                    self.assertEqual(exits, [], state)
                if not fired:
                    break
            self.assertGreater(k, 50)

    def test_a_later_signal_interrupts_a_handler_kymo_chains_to(self):
        # A Ctrl-C during a checkpoint-on-SIGTERM handler installed before init() interrupts it, as it would without kymo.
        events = []

        def checkpoint(signum, frame):
            events.append("checkpoint started")
            try:
                signal.raise_signal(signal.SIGUSR2)
                events.append("checkpoint saved")
            except KeyboardInterrupt:
                events.append("checkpoint interrupted")
                raise

        with (
            _kymo_handles(
                {signal.SIGUSR1: checkpoint, signal.SIGUSR2: signal.default_int_handler}
            ),
            self.assertRaises(KeyboardInterrupt),
        ):
            signal.raise_signal(signal.SIGUSR1)
        self.assertEqual(events, ["checkpoint started", "checkpoint interrupted"])

    def test_many_deferred_signals_run_without_recursion(self):
        handled = []

        def put(item):
            for _ in range(sys.getrecursionlimit() + 100):
                signal.raise_signal(signal.SIGUSR1)

        with (
            mock.patch.object(client_module, "_metric_queue", _ListQueue(put)),
            mock.patch.object(client_module, "_queue_status", _QueueStatus()),
            _kymo_handles({signal.SIGUSR1: lambda signum, frame: handled.append(1)}),
        ):
            client_module._publish_queue_items([[numeric(1)]])
        self.assertEqual(len(handled), sys.getrecursionlimit() + 100)

    def test_a_signal_waits_for_no_timed_step_of_a_full_queue(self):
        # Each group's first try never waits, so a signal deferred by a full queue runs before any timed step.
        calls = []

        class Slow(_ListQueue):
            def put(self, item, block=True, timeout=None):
                calls.append(block)
                if not calls[1:]:
                    signal.raise_signal(signal.SIGUSR1)
                if not block:
                    raise queue.Full
                super().put(item)

        seen = []

        def record(signum, frame):
            seen.append(len(calls))

        with (
            mock.patch.object(client_module, "_metric_queue", Slow()),
            mock.patch.object(client_module, "_queue_status", _QueueStatus()),
            mock.patch.object(client_module, "_held_back", deque()),
            _kymo_handles({signal.SIGUSR1: record}),
        ):
            client_module._publish_queue_items([[numeric(i)] for i in range(6)])
        self.assertEqual(seen, [1])

    def test_a_raising_handler_never_leaves_shutdown_signals_waiting(self):
        # A handler kymo does not own (a per-step timeout, say) raises wherever it interrupts kymo's code, every 30 us. However the publications end, a shutdown signal afterwards must be handled at once.
        class Alarm(Exception):
            pass

        def alarm(signum, frame):
            if frame is not None and frame.f_code.co_filename == client_module.__file__:
                raise Alarm()

        status = _QueueStatus()
        # A raise between its acquire and release leaves it held.
        status.lock = threading.RLock()
        handled = []

        def record(signum, frame):
            handled.append(signum)

        previous = signal.signal(signal.SIGALRM, alarm)
        try:
            with (
                mock.patch.object(client_module, "_metric_queue", _ListQueue()),
                mock.patch.object(client_module, "_queue_status", status),
                # An interrupted publication holds groups back for the next one, which must not be another test's, and on Python 3.14 a raise as a `with` statement takes its lock leaves the lock held.
                mock.patch.multiple(
                    client_module,
                    _held_back=deque(),
                    _uncharged=deque(),
                    _unproven_points=0,
                    _unproven_lock=threading.RLock(),
                    _pending_lock=threading.RLock(),
                    _capture_drain_lock=threading.RLock(),
                ),
                _kymo_handles({signal.SIGUSR1: record}),
            ):
                signal.setitimer(signal.ITIMER_REAL, 30e-6, 30e-6)
                try:
                    deadline = time.monotonic() + 0.5
                    while time.monotonic() < deadline:
                        client_module._metric_queue.items.clear()
                        with contextlib.suppress(Alarm):
                            client_module._publish_queue_items([[numeric(1)]])
                finally:
                    signal.setitimer(signal.ITIMER_REAL, 0)
                signal.raise_signal(signal.SIGUSR1)
                self.assertEqual(handled, [signal.SIGUSR1])
        finally:
            signal.signal(signal.SIGALRM, previous)

    def test_shutdown_signals_wait_for_publication_and_run_in_arrival_order(self):
        handled = []

        def put(item):
            # A handler that drained here would wait on this put's queue lock.
            signal.raise_signal(signal.SIGUSR1)
            signal.raise_signal(signal.SIGUSR2)
            handled.append("put")

        def record(signum, frame):
            handled.append(signum)

        status = _QueueStatus()
        with (
            mock.patch.object(client_module, "_metric_queue", _ListQueue(put)),
            mock.patch.object(client_module, "_queue_status", status),
            _kymo_handles({signal.SIGUSR1: record, signal.SIGUSR2: record}),
        ):
            client_module._publish_queue_items([[numeric(1)]])
            self.assertEqual(client_module._deferred_signals, [])
        self.assertEqual(handled, ["put", signal.SIGUSR1, signal.SIGUSR2])
        self.assertEqual(status.value, 1)

    def test_an_exit_wins_over_a_keyboard_interrupt_deferred_before_it(self):
        # A Ctrl-C and then a SIGTERM wait for one held section: kymo's own handling of the SIGTERM ends the program with its exit, as it would have had the two not waited, not the Ctrl-C's KeyboardInterrupt, which came first.
        with (
            mock.patch.multiple(client_module, _is_initialized=False, _exit_code=None),
            _kymo_handles(
                {
                    signal.SIGINT: signal.default_int_handler,
                    signal.SIGTERM: signal.SIG_DFL,
                }
            ),
        ):
            client_module._deferred_signals.extend(
                [[signal.SIGINT, None], [signal.SIGTERM, None]]
            )
            with self.assertRaises(SystemExit) as exited:
                client_module._run_deferred_signals()
            self.assertEqual(client_module._deferred_signals, [])
        self.assertEqual(exited.exception.code, 128 + signal.SIGTERM)

    def test_a_handler_chained_to_kymos_runs_once_for_a_deferred_signal(self):
        calls = []

        def original(signum, frame):
            calls.append("kymo's original")

        with (
            mock.patch.object(
                client_module,
                "_metric_queue",
                _ListQueue(lambda item: signal.raise_signal(signal.SIGUSR1)),
            ),
            mock.patch.object(client_module, "_queue_status", _QueueStatus()),
            _kymo_handles({signal.SIGUSR1: original}),
        ):
            # Installed after init(), chaining to the handler it replaced, as a framework would.
            kymos = signal.getsignal(signal.SIGUSR1)

            def user_handler(signum, frame):
                calls.append("user handler")
                kymos(signum, frame)

            signal.signal(signal.SIGUSR1, user_handler)
            client_module._publish_queue_items([[numeric(1)]])
        self.assertEqual(calls, ["user handler", "kymo's original"])

    def test_only_the_queues_own_refusals_roll_back_a_group(self):
        class Refusing(_ListQueue):
            # The queue's own put refuses group 2, as a closed multiprocessing.Queue does.
            _closed = False

            def put(self, item, block=True, timeout=None):
                if item[0][2] == 2:
                    self._closed = True
                    raise ValueError("closed")
                super().put(item)

        class Lying(_ListQueue):
            # A put that takes the group and then raises ValueError from its own frame, with the queue still open: not a refusal.
            _closed = False

            def put(self, item, block=True, timeout=None):
                super().put(item)
                if item[0][2] == 2:
                    raise ValueError("taken, then raised")

        def raised_by_a_handler(error):
            # A handler interrupting put() raises from its own frame, perhaps after the queue took the group.
            def handler(item):
                if item[0][2] == 2:
                    raise error

            return _ListQueue(handler)

        # Only the queue's own refusal rolls back its group and those after it; after any other error the group keeps its charge, counted as unproven since the queue may have taken it, and the group after it is held back, still charged.
        for name, target, thread in (
            ("the closed queue's ValueError", Refusing(), False),
            ("an open queue's ValueError", Lying(), False),
            ("a handler's ValueError", raised_by_a_handler(ValueError()), False),
            ("a handler's queue.Full", raised_by_a_handler(queue.Full()), False),
            ("an interrupt", raised_by_a_handler(KeyboardInterrupt()), False),
            ("an error off the main thread", raised_by_a_handler(OSError()), True),
            ("an interrupt off it", raised_by_a_handler(KeyboardInterrupt()), True),
        ):
            with self.subTest(name):
                refused = thread or isinstance(target, Refusing)
                status, raised, held_back = _QueueStatus(), [], deque()

                def publish():
                    try:
                        client_module._publish_queue_items(
                            [[numeric(1)], [numeric(2)], [numeric(3)]]
                        )
                    except BaseException as error:
                        raised.append(error)

                with (
                    mock.patch.object(client_module, "_metric_queue", target),
                    mock.patch.object(client_module, "_queue_status", status),
                    mock.patch.object(client_module, "_held_back", held_back),
                    mock.patch.object(client_module, "_unproven_points", 0),
                ):
                    if thread:
                        worker = threading.Thread(target=publish)
                        worker.start()
                        worker.join()
                    else:
                        publish()
                    unproven = client_module._unproven_points
                self.assertEqual(len(raised), 1)
                self.assertEqual(unproven, 0 if refused else 1)
                self.assertEqual(status.value, 1 if refused else 3)
                self.assertEqual(list(held_back), [] if refused else [[numeric(3)]])

    def test_a_handlers_error_inside_a_real_queues_put_keeps_the_charge(self):
        # The queue has taken the group when the handler raises; a ValueError or queue.Full from it must neither roll the group back nor put it again.
        source, first = inspect.getsourcelines(multiprocessing.queues.Queue.put)
        notify = first + next(
            i for i, line in enumerate(source) if "self._notempty.notify()" in line
        )
        put_code = multiprocessing.queues.Queue.put.__code__
        for error in (ValueError, queue.Full):
            with self.subTest(error=error.__name__):
                target, status, fired = _real_queue(self), _QueueStatus(), []

                def alarm(signum, frame, error=error):
                    raise error("a handler's own error")

                def trace(frame, event, arg):
                    if (
                        event == "line"
                        and frame.f_code is put_code
                        and frame.f_lineno == notify
                        and not fired
                    ):
                        fired.append(True)
                        signal.raise_signal(signal.SIGALRM)
                    return trace

                previous = signal.signal(signal.SIGALRM, alarm)
                try:
                    with (
                        mock.patch.object(client_module, "_metric_queue", target),
                        mock.patch.object(client_module, "_queue_status", status),
                        self.assertRaises(error),
                        _tracing(trace),
                    ):
                        client_module._publish_queue_items([[numeric(1)]])
                    self.assertEqual((fired, status.value), ([True], 1))
                    self.assertEqual(target.get(timeout=5), [numeric(1)])
                    with self.assertRaises(queue.Empty):
                        target.get(timeout=0.2)
                finally:
                    signal.signal(signal.SIGALRM, previous)

    def test_a_closed_queue_rolls_back_only_the_charges_its_publication_made(self):
        # Groups held back before keep their charge and are held back again, and the groups earlier calls accepted are held back uncharged, so a shutdown reports them; only the publication's own group goes with the error, and of a numeric batch the drained text joined, only the batch.
        target = _real_queue(self)
        target.close()
        text = [("text_ts", "logs/std_out", 9, "out", 9)]
        batch = (client_module._NUMERIC_BATCH_QUEUE_ITEM, 3, 9, ["a"], [3.0], [None])
        for own in ([numeric(3)], batch):
            status = _QueueStatus(1)
            with (
                self.subTest(batch=own is batch),
                mock.patch.multiple(
                    client_module,
                    _metric_queue=target,
                    _queue_status=status,
                    _held_back=deque([[numeric(1)]]),
                    _uncharged=deque([[numeric(2)]]),
                ),
                mock.patch.object(
                    client_module._capture, "has_output", return_value=True
                ),
                mock.patch.object(
                    client_module, "_drain_capture_points", return_value=text
                ),
            ):
                with self.assertRaises(ValueError):
                    client_module._publish_queue_items([own], capture_ms=9)
                self.assertEqual(status.value, 1)
                self.assertEqual(list(client_module._held_back), [[numeric(1)]])
                self.assertEqual(list(client_module._uncharged), [[numeric(2)], text])

    def test_a_raise_as_a_full_queue_holds_groups_back_never_puts_one_twice(self):
        # A handler kymo does not own raises at each opcode of a publication, and of its hold-back, once a put on a full queue has ended without putting its group, having given way to a deferred shutdown signal or met the deadline. Every group is then put once by the next publication, or counted as unproven, and every charge is a group put or unproven, so no wait hangs.
        class Timeout(Exception):
            pass

        class FullForTwo(_ListQueue):
            full = True

            def put(self, item, block=True, timeout=None):
                if self.full and item[0][2] == 2:
                    raise queue.Full
                super().put(item)

        codes = {
            client_module._publish_held.__code__,
            client_module._hold_back.__code__,
        }
        real_wait = client_module._wait_to_put
        for ending in ("a deferred signal", "the deadline"):
            for k in itertools.count():
                ended = []

                def wait_to_put(item, hard_deadline):
                    try:
                        return real_wait(item, hard_deadline)
                    finally:
                        ended.append(True)

                def raise_timeout(frame):
                    if _a_handler_runs_at(frame):
                        raise Timeout

                tracer, fired = _opcode_tracer(
                    codes, k, raise_timeout, paused=lambda: not ended
                )
                target, status = FullForTwo(), _QueueStatus()
                signalled = ending == "a deferred signal"
                with mock.patch.multiple(
                    client_module,
                    _metric_queue=target,
                    _queue_status=status,
                    _held_back=deque(),
                    _uncharged=deque(),
                    _unproven_points=0,
                    _deferred_signals=[[signal.SIGUSR1, None]] if signalled else [],
                    _wait_to_put=wait_to_put,
                ):
                    with _tracing(tracer), contextlib.suppress(Timeout, queue.Full):
                        client_module._publish_held(
                            [[numeric(1)], [numeric(2)], [numeric(3)]],
                            hard_deadline=None if signalled else time.monotonic(),
                        )
                    client_module._deferred_signals.clear()
                    target.full = False
                    client_module._publish_held([])
                    unproven = client_module._unproven_points
                    stores = list(client_module._held_back)
                state = f"{ending}: raised at opcode {k}"
                self.assertEqual(stores, [], state)
                for index in (1, 2, 3):
                    self.assertLessEqual(target.items.count([numeric(index)]), 1, state)
                put = sum(map(len, target.items))
                self.assertLessEqual(put, status.value, state)
                self.assertLessEqual(status.value - put, unproven, state)
                if not fired:
                    break
            self.assertGreater(k, 20, ending)

    def test_groups_with_no_room_by_the_deadline_stay_held_back_and_charged(self):
        class FullPastOne(_ListQueue):
            room = False

            def put(self, item, block=True, timeout=None):
                if not self.room and item[0][2] >= 2:
                    raise queue.Full
                super().put(item)

        target, status = FullPastOne(), _QueueStatus()
        with (
            mock.patch.object(client_module, "_metric_queue", target),
            mock.patch.object(client_module, "_queue_status", status),
            mock.patch.object(client_module, "_held_back", deque()),
        ):
            with self.assertRaises(queue.Full):
                client_module._publish_queue_items(
                    [[numeric(1)], [numeric(2)], [numeric(3)]],
                    hard_deadline=time.monotonic(),
                )
            self.assertEqual(
                list(client_module._held_back), [[numeric(2)], [numeric(3)]]
            )
            # Groups held back earlier stay ahead of a later publication's own when that one meets its deadline too.
            with self.assertRaises(queue.Full):
                client_module._publish_queue_items(
                    [[numeric(4)]], hard_deadline=time.monotonic()
                )
            self.assertEqual(
                list(client_module._held_back),
                [[numeric(2)], [numeric(3)], [numeric(4)]],
            )
            self.assertEqual(status.value, 4)
            target.room = True
            client_module._publish_queue_items([])
            self.assertEqual(list(client_module._held_back), [])
        self.assertEqual([group[0][2] for group in target.items], [1, 2, 3, 4])
        self.assertEqual(status.value, 4)

    def test_groups_with_no_room_from_an_exited_worker_stay_held_back_and_charged(self):
        # Only the upload worker makes room, so once it has exited a put on a full queue stops waiting.
        blocks = []

        class Full(_ListQueue):
            def put(self, item, block=True, timeout=None):
                blocks.append(block)
                raise queue.Full

        status = _QueueStatus()
        with _upload_state(
            _metric_queue=Full(), _queue_status=status, _upload_process=_Process(1)
        ):
            with self.assertRaisesRegex(RuntimeError, "upload worker died"):
                client_module._publish_queue_items([[numeric(1)]])
            self.assertEqual(list(client_module._held_back), [[numeric(1)]])
            # wait_for_upload() reports the exited worker as log() does, not as a timeout.
            with self.assertRaisesRegex(RuntimeError, "upload worker died"):
                client_module.wait_for_upload(timeout=5)
            self.assertEqual(list(client_module._held_back), [[numeric(1)]])
        self.assertEqual(blocks, [False, False])
        self.assertEqual(status.value, 1)

    def test_an_exited_worker_is_reported_with_the_failure_it_recorded(self):
        for failure, message in (
            (0, "upload worker died with 1 points still queued"),
            (client_module._WORKER_EXIT_RUN_DELETED, "run is deleted"),
            (client_module._WORKER_EXIT_SPOOL_FAILED, "could not spool"),
            # A worker that cut a gallery short carried on: it died of something else.
            (
                client_module._WORKER_GALLERY_CUT_SHORT,
                "upload worker died with 1 points still queued",
            ),
        ):
            with (
                self.subTest(failure=failure),
                mock.patch.multiple(
                    client_module,
                    _init_pid=os.getpid(),
                    _upload_process=_Process(4),
                    _queue_status=_QueueStatus(1),
                    _upload_failure=_QueueStatus(failure),
                ),
                self.assertRaisesRegex(RuntimeError, message),
            ):
                client_module._check_worker()

    def test_threads_holding_back_groups_never_put_one_twice_or_lose_one(self):
        # A thread that finds the worker exited holds back its group while the main thread takes and puts the held-back groups and a third thread holds back its own. Parked at each opcode of its publication, it neither revives a group the main thread put nor drops one the third thread held back.
        class MainOnlyRoom(_ListQueue):
            def put(self, item, block=True, timeout=None):
                if threading.current_thread() is not threading.main_thread():
                    raise queue.Full
                super().put(item)

        traced = {
            client_module._publish_held.__code__,
            client_module._hold_back.__code__,
        }
        for k in itertools.count():
            parked, release = threading.Event(), threading.Event()

            def park(frame):
                parked.set()
                release.wait(5)

            tracer, _ = _opcode_tracer(traced, k, park)

            def publish(step, trace=None):
                with _tracing(trace), contextlib.suppress(RuntimeError):
                    client_module._publish_queue_items([[numeric(step)]])

            target, status = MainOnlyRoom(), _QueueStatus(2)
            with mock.patch.multiple(
                client_module,
                _metric_queue=target,
                _queue_status=status,
                _held_back=deque([[numeric(1)], [numeric(2)]]),
                _init_pid=os.getpid(),
                _upload_process=_Process(1),
                _upload_failure=_QueueStatus(),
            ):
                first = threading.Thread(target=publish, args=(10, tracer))
                first.start()
                while not parked.wait(0.01) and first.is_alive():
                    pass
                third = threading.Thread(target=publish, args=(20,))
                third.start()
                third.join()
                client_module._publish_queue_items([[numeric(3)]])
                release.set()
                first.join()
                client_module._publish_queue_items([])
                held = list(client_module._held_back)
            state = f"parked at opcode {k}"
            steps = [group[0][2] for group in target.items]
            self.assertEqual(sorted(steps), [1, 2, 3, 10, 20], state)
            self.assertLess(steps.index(1), steps.index(2), state)
            self.assertEqual((held, status.value), ([], 5), state)
            if not parked.is_set():
                break
        self.assertGreater(k, 20)

    def test_groups_a_full_queue_held_back_for_a_signal_are_never_lost(self):
        # A put on a full queue gives way to a waiting shutdown signal. The waiting group and those after it stay charged while the handler runs and go out first with the next publication, whatever the handler does.
        class FullOnce(_ListQueue):
            full = True

            def put(self, item, block=True, timeout=None):
                if self.full:
                    self.full = False
                    signal.raise_signal(signal.SIGUSR1)
                    raise queue.Full
                super().put(item)

        def returns(signum, frame):
            pass

        def flushes_and_exits(signum, frame):
            # As kymo's own handler does: its flush publishes, then it exits.
            client_module._publish_queue_items([])
            raise SystemExit(143)

        def raises(signum, frame):
            raise KeyboardInterrupt

        for handler, raised in (
            (returns, None),
            (flushes_and_exits, SystemExit),
            (raises, KeyboardInterrupt),
        ):
            with self.subTest(handler=handler.__name__):
                status, target, charged = _QueueStatus(), FullOnce(), []

                def record(signum, frame, handler=handler, status=status):
                    charged.append(status.value)
                    handler(signum, frame)

                with (
                    mock.patch.object(client_module, "_metric_queue", target),
                    mock.patch.object(client_module, "_queue_status", status),
                    mock.patch.object(client_module, "_STATUS_LOCK_TIMEOUT", 0.01),
                    mock.patch.object(client_module, "_held_back", deque()),
                    mock.patch.object(client_module, "_exit_code", None),
                    _kymo_handles({signal.SIGUSR1: record}),
                ):
                    try:
                        client_module._publish_queue_items([[numeric(1)], [numeric(2)]])
                    except (SystemExit, KeyboardInterrupt) as error:
                        self.assertIs(type(error), raised)
                    else:
                        self.assertIsNone(raised)
                    client_module._publish_queue_items([[numeric(3)]])
                self.assertEqual(charged, [2])
                self.assertEqual([group[0][2] for group in target.items], [1, 2, 3])
                self.assertEqual(status.value, 3)

    def test_a_full_queue_without_a_waiting_signal_keeps_waiting(self):
        attempts = []

        class FullTwice(_ListQueue):
            def put(self, item, block=True, timeout=None):
                attempts.append(timeout)
                if len(attempts) <= 2:
                    raise queue.Full
                super().put(item)

        target, status = FullTwice(), _QueueStatus()
        with (
            mock.patch.object(client_module, "_metric_queue", target),
            mock.patch.object(client_module, "_queue_status", status),
            mock.patch.object(client_module, "_STATUS_LOCK_TIMEOUT", 0.01),
        ):
            client_module._publish_queue_items([[numeric(1)]])
        # The first try never waits; the steps after it wait one lock timeout each.
        self.assertEqual(attempts, [None, 0.01, 0.01])
        self.assertEqual((status.value, len(target.items)), (1, 1))

    def test_a_shutdown_signal_after_the_capture_drain_finds_the_text_published(self):
        target = _ListQueue()
        seen = []

        def drain(final, timeout=None):
            # The earliest a signal can land after the destructive drain.
            signal.raise_signal(signal.SIGUSR1)
            return ("captured", "")

        def flush(signum, frame):
            seen.extend(target.items)

        with (
            _upload_state(_metric_queue=target, _queue_status=_QueueStatus()),
            mock.patch.multiple(
                capture_module,
                has_output=mock.Mock(return_value=True),
                drain_buffers=mock.Mock(side_effect=drain),
            ),
            _kymo_handles({signal.SIGUSR1: flush}),
        ):
            client_module.log({"train/loss": 1.0}, step=1)
        # A shutdown flush at that point would publish whatever the queue held: the drained text must be there, in the call's numeric batch.
        texts = [
            point[3]
            for group in seen
            for point in client_module._decode_queue_item(group)
            if point[0] == "text_ts"
        ]
        self.assertEqual(texts, ["captured"])

    @unittest.skipUnless(hasattr(os, "fork"), "requires os.fork")
    def test_a_fork_child_handles_shutdown_signals_at_once(self):
        # Forked from another thread while the main thread publishes: the child's main thread is the forking one, in no section.
        script = r"""
import os
import signal
import threading

import kymo.client as client


class Status:
    value = 0
    lock = threading.RLock()

    def get_lock(self):
        return self.lock

    def get_obj(self):
        return self


class Queue:
    entered, release = threading.Event(), threading.Event()

    def put(self, item, block=True, timeout=None):
        self.entered.set()
        self.release.wait()


client._metric_queue, client._queue_status = Queue(), Status()
children = []


def fork():
    client._metric_queue.entered.wait()
    pid = os.fork()
    if pid == 0:
        handled = []
        client._original_signals[signal.SIGUSR1] = lambda s, f: handled.append(s)
        signal.signal(signal.SIGUSR1, client._signal_handler)
        signal.raise_signal(signal.SIGUSR1)
        os._exit(0 if handled else 7)
    children.append(pid)
    client._metric_queue.release.set()


thread = threading.Thread(target=fork)
thread.start()
client._publish_queue_items([[("numeric_ts", "a", 1, 1.0, 0)]])
thread.join()
_, status = os.waitpid(children[0], 0)
print("child", os.waitstatus_to_exitcode(status))
"""
        result = _run_script_file(script)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "child 0", result.stderr)

    def test_a_failed_put_in_log_keeps_the_charge_of_every_group_handed_over(self):
        queue = _patch_log_client(self)
        queue.put.side_effect = [None, OSError("queue closed")]
        with self.assertRaisesRegex(OSError, "queue closed"):
            client_module.log(
                {
                    "train/loss": 1.0,
                    "demo/resource": Resource(b"payload", "demo.bin"),
                },
                step=1,
            )

        first, _ = [call.args[0] for call in queue.put.call_args_list]
        self.assertEqual(client_module._queue_item_size(first), 1)
        # The refused group keeps its charge too: the queue may have taken it.
        self.assertEqual(client_module._queue_status.value, 2)

    def test_a_calls_numeric_values_ride_one_batch_queue_item(self):
        queue = _patch_log_client(self)
        client_module.log(
            {
                "train/loss": 1.5,
                "demo/tags": [2.5, 3.5],
                "train/lr": 0.25,
            },
            step=7,
        )

        (item,) = [call.args[0] for call in queue.put.call_args_list]
        self.assertEqual(item[0], client_module._NUMERIC_BATCH_QUEUE_ITEM)
        self.assertEqual(client_module._queue_item_size(item), 4)
        self.assertEqual(client_module._queue_status.value, 4)
        # One tag per value, as the call tags some; a call that tags none sends none.
        self.assertEqual(item[5], [None, "0", "1", None])
        client_module.log({"train/loss": 1.5}, step=8)
        self.assertEqual(queue.put.call_args.args[0][5], [])
        # The mp.Queue feeder ships the item pickled; the worker sees the expansion.
        shipped = pickle.loads(pickle.dumps(item))
        self.assertEqual(
            client_module._decode_queue_item(shipped),
            [
                ("numeric_ts", "train/loss", 7, 1.5, item[2]),
                ("numeric_tagged_ts", "demo/tags", 7, 2.5, "0", item[2]),
                ("numeric_tagged_ts", "demo/tags", 7, 3.5, "1", item[2]),
                ("numeric_ts", "train/lr", 7, 0.25, item[2]),
            ],
        )
        # Lists out of step are an undecodable item, never a silently shorter one.
        names, values, tags = item[3:]
        with self.assertRaises(ValueError):
            client_module._decode_queue_item((*item[:3], names, values[:-1], tags))

    def test_a_calls_captured_output_rides_its_numeric_batch(self):
        # The text points of the output the call's publication drains join its numeric batch, as its 7th field, a kind of its own: one put, pickle and queue slot.
        queue = _patch_log_client(
            self,
            mock.patch.object(capture_module, "has_output", return_value=True),
            mock.patch.object(
                capture_module, "drain_buffers", return_value=("out", "err")
            ),
        )
        client_module.log({"train/loss": 1.5}, step=7)

        (item,) = [call.args[0] for call in queue.put.call_args_list]
        self.assertEqual(item[0], client_module._NUMERIC_TEXT_BATCH_QUEUE_ITEM)
        self.assertEqual(client_module._queue_item_size(item), 3)
        self.assertEqual(client_module._queue_status.value, 3)
        at = item[2]
        self.assertEqual(
            client_module._decode_queue_item(pickle.loads(pickle.dumps(item))),
            [
                ("numeric_ts", "train/loss", 7, 1.5, at),
                ("text_ts", "logs/std_out", at, "out", at),
                ("text_ts", "logs/std_err", at, "err", at),
            ],
        )


class _GpuScalar:
    """Stands in for a CUDA scalar tensor."""

    def __init__(self, value):
        self.value = value


def _assert_random_calls_publish_a_tag_per_value(case, gpu_values, read, calls):
    """Log calls random dicts of awkward CPU values and of gpu_values (read(value) is a GPU value's float, None a CPU value's), untagged and in lists, through case.queue, and assert that each call publishes, bit for bit, the points a tag per value gives: its CPU values in order, then its GPU values but those beyond float32, which log() drops once read, in an order the read may choose (the stacked read groups them by dtype). A call this model rejects must raise the same error and publish nothing."""
    rng = random.Random(calls)
    f32_max = float(np.finfo(np.float32).max)
    cpu_values = (
        [0.0, -0.0, math.nan, -math.nan, math.inf, -math.inf, 1 / 3, 2.0**53 + 1]
        + [5e-324, -5e-324, 1.17e-38, f32_max, -f32_max, 2.0**128 - 2.0**104]
        + [0, 1, -1, True, False, 2**53 + 1, 2**63 - 1, -(2**63), 2**64 + 7]
        + [np.float16(1 / 3), np.float16(-0.0), np.float32(np.nan)]
        + [np.float32(-0.0), np.float32(1e-45), np.float32(3.4e38), np.float64(-0.0)]
        + [np.int8(-128), np.int32(-7), np.int64(2**63 - 1), np.uint64(2**64 - 1)]
        + [np.bool_(True), np.longdouble(1.5)]
    )
    # Finite beyond float32, or ints float() refuses: log() raises.
    rejected_values = [
        2.0**128 - 2.0**103,
        1e300,
        np.float64(-1e300),
        2**128,
        -(2**1100),
    ]
    names = [f"m/{i}" for i in range(12)] + ["é/0", "  sp  ", "é" * 1024]
    rejected_names = ["a" * 2049, "x\x00y", 5]

    def value():
        k = rng.random()
        if k < 0.03:
            return rng.choice(rejected_values)
        return rng.choice(gpu_values if k < 0.2 else cpu_values)

    def expected(metrics):
        cpu, gpu = [], []
        for name, value in metrics.items():
            name = client_module._normalize_metric_name(name)
            for tag, v in enumerate(value) if type(value) is list else [(None, value)]:
                if read(v) is None:
                    v, points = client_module._normalize_numeric_value(v), cpu
                else:
                    try:
                        v, points = client_module._normalize_numeric_value(read(v)), gpu
                    except OverflowError:
                        continue
                points.append(
                    ("numeric_ts", name, 3, v, 1234)
                    if tag is None
                    else ("numeric_tagged_ts", name, 3, v, str(tag), 1234)
                )
        return cpu, gpu

    def exact(points):
        return [
            tuple(
                (type(x), np.float64(x).tobytes()) if isinstance(x, float) else x
                for x in point
            )
            for point in points
        ]

    with (
        mock.patch.object(client_module.time, "time", return_value=1.234),
        case.assertLogs("kymo", level="WARNING"),
    ):
        for _ in range(calls):
            metrics = {}
            for _ in range(rng.randrange(1, 9)):
                name = rng.choice(rejected_names if rng.random() < 0.02 else names)
                metrics[name] = (
                    [value() for _ in range(rng.randrange(4))]
                    if rng.random() < 0.3
                    else value()
                )
            case.queue.reset_mock()
            client_module._last_log_duration_ms = 0.0
            try:
                cpu, gpu = expected(metrics)
            except (TypeError, ValueError, OverflowError) as error:
                with case.assertRaises(type(error)):
                    client_module.log(metrics, step=3)
                case.assertEqual(case.queue.put.call_count, 0)
                case.assertFalse(client_module._pending)
                continue
            client_module.log(metrics, step=3)
            with client_module._pending_lock:
                client_module._publish_pending(None)
            got = [point for group in _published_groups(case.queue) for point in group]
            case.assertEqual(exact(got[: len(cpu)]), exact(cpu), metrics)
            case.assertEqual(
                collections.Counter(exact(got[len(cpu) :])),
                collections.Counter(exact(gpu)),
                metrics,
            )


class _GpuImage:
    """Stands in for a CUDA image tensor; its host copy is the array."""

    def __init__(self, array):
        self.array = array
        self.nbytes = array.nbytes


class _FakeReads:
    """Stands in for kymo._gpu.Reads: the copies complete when the test says, or when a caller waits without a deadline."""

    def __init__(self, _torch, scalars, images):
        self.values = [scalar.value for scalar in scalars]
        self.images = [image.array.copy() for image in images]
        self.complete = False
        self.error = None

    def done(self):
        if self.error is not None:
            raise self.error
        return self.complete

    def wait(self, deadline=None):
        if deadline is None:
            self.complete = True
        return self.done()

    def scalars(self):
        return enumerate(self.values)


def _gpu_pending(step, gpu, *, complete=True, cpu=None, galleries=False):
    """A pending log() call at step whose GPU scalar "gpu" reads gpu, its copy complete unless complete is false, with cpu's values ({name: value}) logged beside it."""
    cpu = cpu or {}
    pending = client_module._PendingLog(
        (
            client_module._NUMERIC_BATCH_QUEUE_ITEM,
            step,
            1234,
            list(cpu),
            list(cpu.values()),
            [None] * len(cpu),
        ),
        [],
        step=step,
        gpu_keys=[("gpu", None)],
        galleries=galleries,
    )
    pending.reads = _FakeReads(None, [_GpuScalar(gpu)], [])
    pending.reads.complete = complete
    return pending


def _hold(case, lock):
    """Hold lock on another thread until the test ends, or until the returned function, which waits for the thread to let it go, is called."""
    locked, release = threading.Event(), threading.Event()

    def hold():
        with lock:
            locked.set()
            release.wait()

    holder = threading.Thread(target=hold)
    holder.start()
    case.addCleanup(holder.join)
    case.addCleanup(release.set)
    locked.wait()
    return lambda: release.set() or holder.join()


def _patch_log_client(case, *extra) -> mock.Mock:
    """Patch the client state a log() call needs, plus extra patches, for one test; return the queue it publishes to."""
    queue = mock.Mock()
    for patch in (
        _upload_state(
            _metric_queue=queue,
            _last_log_duration_ms=0.0,
            _rich_writer_epoch=None,
            # No encoder pacing unless a test sets it up (_patch_encoder_pacing).
            _galleries_released=None,
        ),
        mock.patch.object(gpu_module, "unfreeable", set()),
        mock.patch.object(capture_module, "drain_buffers", return_value=("", "")),
        *extra,
    ):
        patch.start()
        case.addCleanup(patch.stop)
    return queue


def _patch_encoder_pacing(case) -> types.SimpleNamespace:
    """Pace gallery calls by a fake worker for one test: set released.value as it encodes, on_disk.value while a lane is on disk."""
    pacing = types.SimpleNamespace(
        released=types.SimpleNamespace(value=0),
        on_disk=types.SimpleNamespace(value=0),
        process=mock.Mock(),
    )
    pacing.process.is_alive.return_value = True
    for patch in (
        mock.patch.object(client_module, "_galleries_released", pacing.released),
        mock.patch.object(client_module, "_lanes_on_disk", pacing.on_disk),
        mock.patch.object(client_module, "_upload_terminal", _QueueStatus()),
        mock.patch.object(client_module, "_upload_process", pacing.process),
        mock.patch.object(client_module, "_galleries_published", 0),
        mock.patch.object(client_module, "_previous_galleries", 0),
        mock.patch.object(client_module, "_finishing", False),
        mock.patch.object(client_module, "_terminated_at", None),
    ):
        patch.start()
        case.addCleanup(patch.stop)
    return pacing


def _in_thread(call) -> threading.Event:
    """Run call on a daemon thread; the event is set once it returns."""
    done = threading.Event()

    def run():
        call()
        done.set()

    threading.Thread(target=run, daemon=True).start()
    return done


def _log_promptly(case, *calls) -> None:
    """Make the log(metrics, step) calls on a thread, failing the test instead of hanging it if one waits."""

    def run():
        for metrics, step in calls:
            client_module.log(metrics, step=step)

    case.assertTrue(_in_thread(run).wait(timeout=1))


def _published_groups(queue) -> list:
    """The groups published to queue, decoded, without log()'s own overhead metric."""
    groups = (
        [
            item
            for item in client_module._decode_queue_item(call.args[0])
            if item[1] != "system/log_overhead_ms"
        ]
        for call in queue.put.call_args_list
    )
    return [group for group in groups if group]


def _pending_nbytes() -> int:
    return sum(entry.nbytes for entry in client_module._pending)


class _FakeTensor:
    """Stands in for a torch tensor in the classifier tests."""

    def __init__(
        self, dtype="float32", *, cuda=True, layout="strided", numel=1, negative=False
    ):
        self.dtype, self.is_cuda, self.layout = dtype, cuda, layout
        self._numel, self._negative = numel, negative

    def numel(self):
        return self._numel

    def is_neg(self):
        return self._negative


class _FakeParameter(_FakeTensor):
    pass


def _fake_torch():
    """A torch with the attributes the classifiers read; dtypes are their names."""
    fake = types.SimpleNamespace(
        Tensor=_FakeTensor, nn=types.SimpleNamespace(Parameter=_FakeParameter)
    )
    fake.strided = "strided"
    for name in gpu_module.DTYPES:
        setattr(fake, name, name)
    return fake


class GpuClassifierTests(unittest.TestCase):
    def test_scalars_read_asynchronously(self):
        is_scalar, is_image = gpu_module.classifiers.__wrapped__(_fake_torch())
        for dtype in gpu_module.DTYPES:
            with self.subTest(dtype=dtype):
                self.assertTrue(is_scalar(_FakeTensor(dtype)))
        self.assertTrue(is_scalar(_FakeParameter()))
        for name, value in (
            # Its bytes hold the negated value.
            ("negative view", _FakeTensor(negative=True)),
            ("two elements", _FakeTensor(numel=2)),
            ("on the CPU", _FakeTensor(cuda=False)),
            ("sparse", _FakeTensor(layout="sparse_coo")),
            ("float8", _FakeTensor("float8_e4m3fn")),
            ("another subclass", type("Subclass", (_FakeTensor,), {})()),
            ("a float", 1.0),
        ):
            with self.subTest(name):
                self.assertFalse(is_scalar(value))
        # An image is copied by torch, which resolves a negative view.
        self.assertTrue(is_image(_FakeTensor(numel=12, negative=True)))


def _unavailable(torch):
    raise AttributeError("torch.cuda has no _compile_kernel")


class GatherSelectionTests(unittest.TestCase):
    def setUp(self):
        self.select = gpu_module.gather.__wrapped__
        self.nvrtc, self.triton = object(), object()

    def builders(self, nvrtc, triton, reads_exactly=lambda torch, launch: True):
        return mock.patch.multiple(
            gpu_module,
            nvrtc_gather=nvrtc,
            triton_gather=triton,
            reads_exactly=reads_exactly,
        )

    def test_nvrtc_else_triton_else_the_stack(self):
        for nvrtc, triton, selected in (
            (lambda torch: self.nvrtc, lambda torch: self.triton, self.nvrtc),
            (_unavailable, lambda torch: self.triton, self.triton),
            (_unavailable, _unavailable, None),
        ):
            with self.builders(nvrtc, triton):
                self.assertIs(self.select(None, 0), selected)

    def test_a_kernel_that_reads_values_wrongly_is_not_used(self):
        with (
            self.builders(
                lambda torch: self.nvrtc,
                lambda torch: self.triton,
                lambda torch, launch: launch is self.triton,
            ),
            self.assertLogs("kymo", level="WARNING") as logs,
        ):
            self.assertIs(self.select(None, 0), self.triton)
        self.assertIn("reads values incorrectly", logs.output[0])


class GpuHelperTests(unittest.TestCase):
    def test_only_an_out_of_memory_error_frees_what_a_failed_call_made(self):
        class OutOfMemoryError(RuntimeError):
            pass

        synced = []
        fake = types.SimpleNamespace(
            cuda=types.SimpleNamespace(
                OutOfMemoryError=OutOfMemoryError,
                device_count=lambda: 1,
                device=lambda device: contextlib.nullcontext(),
                current_stream=lambda device: types.SimpleNamespace(
                    synchronize=lambda: synced.append(device)
                ),
            ),
        )
        scalars = [types.SimpleNamespace(requires_grad=False)]
        with (
            mock.patch.object(gpu_module, "unfreeable", set()),
            mock.patch.object(gpu_module, "gather") as gather,
        ):
            gather.side_effect = OutOfMemoryError
            with self.assertRaises(OutOfMemoryError):
                gpu_module.Reads(fake, scalars, [])
            # It first waits for any kernel it launched on the device.
            self.assertEqual((gpu_module.unfreeable, synced), (set(), [0]))
            # And for the copies of images, on their devices.
            synced.clear()
            image = mock.Mock(get_device=lambda: 1)
            image.detach.side_effect = OutOfMemoryError
            with self.assertRaises(OutOfMemoryError):
                gpu_module.Reads(fake, [], [image])
            self.assertEqual((gpu_module.unfreeable, synced), (set(), [1]))
            gather.side_effect = RuntimeError("CUDA error: an illegal memory access")
            with self.assertRaises(RuntimeError):
                gpu_module.Reads(fake, scalars, [])
            self.assertEqual(len(gpu_module.unfreeable), 1)

    def test_reads_are_unfreeable_until_seen_complete(self):
        fake = types.SimpleNamespace(cuda=types.SimpleNamespace(device_count=lambda: 1))
        event = mock.Mock()
        with mock.patch.object(gpu_module, "unfreeable", set()):
            reads = gpu_module.Reads(fake, [], [])
            reads._events = [event]
            # A gather kernel may still write into its pinned buffers.
            event.query.return_value = False
            self.assertFalse(reads.wait(time.monotonic()))
            self.assertEqual(gpu_module.unfreeable, {reads})
            # After a CUDA error it is never seen complete.
            event.query.side_effect = RuntimeError(
                "CUDA error: an illegal memory access"
            )
            with self.assertRaises(RuntimeError):
                reads.wait(time.monotonic())
            self.assertEqual(gpu_module.unfreeable, {reads})
            event.query.side_effect = None
            event.query.return_value = True
            self.assertTrue(reads.wait(time.monotonic()))
            self.assertEqual(gpu_module.unfreeable, set())

    def test_the_known_answer_check_samples_every_dtype(self):
        self.assertEqual(
            {name for name, _ in gpu_module.SAMPLES}, set(gpu_module.DTYPES)
        )

    def test_scalars_group_by_device_only_with_several_devices(self):
        one = types.SimpleNamespace(cuda=types.SimpleNamespace(device_count=lambda: 1))
        two = types.SimpleNamespace(cuda=types.SimpleNamespace(device_count=lambda: 2))
        tensors = [types.SimpleNamespace(get_device=lambda d=d: d) for d in (1, 0, 1)]
        self.assertEqual(gpu_module._by_device(one, tensors), {0: (range(3), tensors)})
        self.assertEqual(gpu_module._by_device(one, []), {})
        self.assertEqual(
            gpu_module._by_device(two, tensors),
            {1: ([0, 2], [tensors[0], tensors[2]]), 0: ([1], [tensors[1]])},
        )

    @unittest.skipUnless(torch is not None, "requires torch")
    def test_no_asynchronous_reads_inside_a_torch_func_transform(self):
        seen = []

        def record(x):
            seen.append(gpu_module.cuda_torch())
            return x.sum()

        with mock.patch.object(torch.cuda, "is_initialized", return_value=True):
            self.assertIs(gpu_module.cuda_torch(), torch)
            torch.func.grad(record)(torch.ones(2))
        self.assertEqual(seen, [None])


class PendingGpuLogTests(unittest.TestCase):
    """log() calls with GPU values, driven through fake copies: publication order, waits, drops and backpressure."""

    def setUp(self):
        patch = mock.patch.object(client_module, "_unproven_points", 0)
        patch.start()
        self.addCleanup(patch.stop)
        self.capturing = False
        self.reads = []
        fake_torch = types.SimpleNamespace(
            cuda=types.SimpleNamespace(
                is_current_stream_capturing=lambda: self.capturing
            )
        )

        def reads(*args):
            self.reads.append(_FakeReads(*args))
            return self.reads[-1]

        self.queue = _patch_log_client(
            self,
            mock.patch.object(client_module.time, "time", return_value=1.234),
            mock.patch.object(gpu_module, "cuda_torch", return_value=fake_torch),
            mock.patch.object(
                gpu_module,
                "classifiers",
                return_value=(
                    lambda value: type(value) is _GpuScalar,
                    lambda data: type(data) is _GpuImage,
                ),
            ),
            mock.patch.object(gpu_module, "Reads", side_effect=reads),
        )

    def published(self):
        return _published_groups(self.queue)

    def test_random_calls_publish_a_tag_per_value(self):
        # A batch's tags are empty until a value is tagged, and cover a pending call's GPU values.
        f32_max = float(np.finfo(np.float32).max)
        _assert_random_calls_publish_a_tag_per_value(
            self,
            [
                _GpuScalar(v)
                for v in (1.5, -0.0, math.nan, -math.inf, 5e-324, f32_max, 1e300)
            ],
            lambda value: value.value if type(value) is _GpuScalar else None,
            1200,
        )

    def test_a_gpu_log_publishes_at_a_later_log_in_call_order(self):
        client_module.log(
            {"gpu": _GpuScalar(1.5), "cpu": 2.0, "tagged": [_GpuScalar(3.0), 4.0]},
            step=1,
        )
        self.assertEqual(self.published(), [])
        # Its copies have not completed: a later CPU-only call waits behind it.
        client_module.log({"later": 5.0}, step=2)
        self.assertEqual(self.published(), [])

        self.reads[0].complete = True
        client_module.log({"last": 6.0}, step=3)

        self.assertEqual(
            self.published(),
            [
                [
                    ("numeric_ts", "cpu", 1, 2.0, 1234),
                    ("numeric_tagged_ts", "tagged", 1, 4.0, "1", 1234),
                    ("numeric_ts", "gpu", 1, 1.5, 1234),
                    ("numeric_tagged_ts", "tagged", 1, 3.0, "0", 1234),
                ],
                [("numeric_ts", "later", 2, 5.0, 1234)],
                [("numeric_ts", "last", 3, 6.0, 1234)],
            ],
        )
        self.assertEqual(len(client_module._pending), 0)

    def test_a_handlers_log_that_publishes_every_pending_call_ends_the_outer_one(self):
        # A handler kymo does not own that logs (a SIGUSR1 requeue handler, say), run as the outer log()'s publication of the oldest pending call begins, publishes every pending call: the outer log() returns rather than index the empty FIFO.
        client_module.log({"gpu": _GpuScalar(1.5)}, step=1)
        self.reads[0].complete = True
        previous = signal.signal(
            signal.SIGUSR1, lambda *_: client_module.log({"handler": 1.0}, step=9)
        )
        self.addCleanup(signal.signal, signal.SIGUSR1, previous)
        tracer, fired = _opcode_tracer(
            {client_module._publish_oldest_pending.__code__},
            0,
            lambda frame: signal.raise_signal(signal.SIGUSR1),
        )
        with _tracing(tracer):
            client_module.log({"later": 2.0}, step=2)
        self.assertTrue(fired)
        self.assertEqual(
            [group[0][1] for group in self.published()], ["gpu", "later", "handler"]
        )
        self.assertEqual(len(client_module._pending), 0)

    def test_captured_text_never_waits_for_gpu_values(self):
        with mock.patch.multiple(
            capture_module,
            has_output=mock.Mock(return_value=True),
            drain_buffers=mock.Mock(return_value=("out", "")),
        ):
            client_module.log({"gpu": _GpuScalar(1.0)}, step=1)

        self.assertEqual(
            self.published(), [[("text_ts", "logs/std_out", 1234, "out", 1234)]]
        )
        self.assertEqual(len(client_module._pending), 1)

    def test_a_pending_call_held_back_at_its_deadline_counts_as_published(self):
        # Its groups stay charged in _held_back for the next publication: neither a failed publication nor incomplete delivery.
        client_module.log({"gpu": _GpuScalar(1.0)}, step=1)
        self.reads[0].complete = True

        with (
            mock.patch.object(client_module, "_metric_queue", _FullQueue()),
            mock.patch.object(client_module, "_held_back", deque()),
            self.assertNoLogs("kymo", level="WARNING"),
        ):
            self.assertTrue(
                client_module._publish_oldest_pending(
                    None, hard_deadline=time.monotonic()
                )
            )
            held = client_module._held_back
            self.assertEqual(len(held), 1)
        self.assertFalse(client_module._pending_incomplete)
        self.assertEqual(
            client_module._queue_status.value, client_module._queue_item_size(held[0])
        )

    def test_wait_for_upload_stops_waiting_for_queue_room_at_its_timeout(self):
        # The pending call's groups stay held back and charged; room coming later does not hold the wait.
        client_module.log({"gpu": _GpuScalar(1.0)}, step=1)
        self.reads[0].complete = True
        room_at = time.monotonic() + 2

        class FullForAWhile(_ListQueue):
            def put(self, item, block=True, timeout=None):
                if time.monotonic() < room_at:
                    if block:
                        time.sleep(min(timeout, room_at - time.monotonic()))
                    raise queue.Full
                super().put(item)

        started = time.monotonic()
        with (
            mock.patch.object(client_module, "_metric_queue", FullForAWhile()),
            mock.patch.object(client_module, "_held_back", deque()),
            self.assertLogs("kymo", level="WARNING") as logs,
        ):
            self.assertFalse(client_module.wait_for_upload(timeout=0.05))
            held = list(client_module._held_back)
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertIn("the upload queue stayed full", logs.output[-1])
        self.assertEqual(len(held), 1)
        self.assertEqual(
            client_module._queue_status.value, client_module._queue_item_size(held[0])
        )
        self.assertFalse(client_module._pending_incomplete)

    def test_a_busy_accounting_lock_holds_a_pending_call_back_uncharged(self):
        # Another thread holds the lock past wait_for_upload()'s timeout: the call it popped keeps its values, uncharged, for the next publication, which charges and puts them first.
        client_module.log({"gpu": _GpuScalar(1.0)}, step=1)
        self.reads[0].complete = True
        release = _hold(self, client_module._queue_status.lock)
        started = time.monotonic()
        with self.assertLogs("kymo", level="WARNING") as logs:
            self.assertFalse(client_module.wait_for_upload(timeout=0.05))
        self.assertLess(time.monotonic() - started, 0.5)
        self.assertIn("timeout waiting for upload accounting", logs.output[-1])
        self.assertNotIn("failed to publish", "\n".join(logs.output))
        self.assertEqual(self.published(), [])
        self.assertEqual(len(client_module._pending), 0)
        self.assertEqual(len(client_module._uncharged), 1)
        self.assertEqual(client_module._queue_status.value, 0)
        self.assertFalse(client_module._pending_incomplete)
        release()
        client_module.log({"cpu": 2.0}, step=2)
        self.assertEqual(
            self.published(),
            [
                [("numeric_ts", "gpu", 1, 1.0, 1234)],
                [("numeric_ts", "cpu", 2, 2.0, 1234)],
            ],
        )
        self.assertEqual(len(client_module._uncharged), 0)
        # Each group charged once, with log()'s overhead metric.
        self.assertEqual(
            client_module._queue_status.value,
            sum(
                client_module._queue_item_size(call.args[0])
                for call in self.queue.put.call_args_list
            ),
        )

    def test_a_deferred_handlers_exception_is_not_a_failed_publication(self):
        class Preempted(Exception):
            pass

        def handler(signum, frame):
            raise Preempted()

        client_module.log({"gpu": _GpuScalar(1.0)}, step=1)
        self.reads[0].complete = True
        self.queue.put.side_effect = lambda *args, **kwargs: signal.raise_signal(
            signal.SIGUSR1
        )
        with (
            _kymo_handles({signal.SIGUSR1: handler}),
            self.assertRaises(Preempted),
        ):
            client_module.log({"cpu": 2.0}, step=2)
        # The pending call was published before the handler ran, and reads as delivered.
        self.assertEqual(self.published(), [[("numeric_ts", "gpu", 1, 1.0, 1234)]])
        self.assertFalse(client_module._pending_incomplete)

    def test_a_value_a_deferred_handler_drops_stays_reported(self):
        # A shutdown signal deferred while the queue is full: the handler kymo chains to logs, publishing the next pending call, which drops a value.
        client_module.log({"fine": _GpuScalar(1.0)}, step=1)
        client_module.log({"huge": _GpuScalar(1e300)}, step=2)
        for reads in self.reads:
            reads.complete = True

        def handler(signum, frame):
            client_module.log({}, step=3)

        class FullOnce(_ListQueue):
            full = True

            def put(self, item, block=True, timeout=None):
                if self.full:
                    self.full = False
                    client_module._deferred_signals.append([signal.SIGUSR1, None])
                    raise queue.Full
                super().put(item)

        target = FullOnce()
        with (
            mock.patch.object(client_module, "_metric_queue", target),
            mock.patch.dict(client_module._original_signals, {signal.SIGUSR1: handler}),
            self.assertLogs("kymo", level="WARNING") as logs,
        ):
            client_module._publish_oldest_pending(None)
        self.assertIn("huge", logs.output[0])
        self.assertEqual(len(target.items), 3)
        self.assertTrue(client_module._pending_incomplete)

    def test_later_cdn_keys_and_config_queue_behind_a_pending_log(self):
        client_module.log({"gpu": _GpuScalar(1.0)}, step=1)
        with mock.patch.object(client_module, "_run_metadata", {"config": {}}):
            client_module.log_cdn({"cdn/key": "abc"}, step=1)
            client_module.update_config({"lr": 0.1})
        self.assertEqual(self.published(), [])

        self.reads[0].complete = True
        client_module.log({}, step=2)

        gpu, cdn, (metadata,) = self.published()
        self.assertEqual(gpu, [("numeric_ts", "gpu", 1, 1.0, 1234)])
        self.assertEqual(cdn, [("cdn_ts", "cdn/key", 1, "abc", 1234)])
        self.assertEqual(metadata[:3], ("metadata_batch", "info/run_info", 0))
        self.assertEqual(metadata[3].data["config"], {"lr": 0.1})

    def test_wait_for_upload_bounds_its_wait_for_the_pending_calls_lock(self):
        _hold(self, client_module._pending_lock)
        started = time.monotonic()
        with self.assertLogs("kymo", level="WARNING") as logs:
            self.assertFalse(client_module.wait_for_upload(timeout=0.1))
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertIn("another thread holds", logs.output[0])

    def test_wait_for_upload_publishes_pending_logs_once_their_copies_complete(self):
        client_module.log({"gpu": _GpuScalar(1.0)}, step=1)
        with mock.patch.object(
            client_module, "_delivery_status_snapshot", return_value=(0, 0, 0)
        ):
            with self.assertLogs("kymo", level="WARNING") as logs:
                self.assertFalse(client_module.wait_for_upload(timeout=0))
            self.assertIn("still wait for their GPU values", logs.output[0])
            self.assertEqual(self.published(), [])

            self.reads[0].complete = True
            self.assertTrue(client_module.wait_for_upload(timeout=0))
        self.assertEqual(self.published(), [[("numeric_ts", "gpu", 1, 1.0, 1234)]])

    def test_a_value_beyond_float32_is_dropped_and_reported(self):
        client_module.log(
            {
                "huge": _GpuScalar(1e300),
                "fine": _GpuScalar(1.0),
                "nan": _GpuScalar(math.nan),
            },
            step=4,
        )
        self.reads[0].complete = True
        with self.assertLogs("kymo", level="WARNING") as logs:
            client_module.log({}, step=5)
        self.assertIn("huge", logs.output[0])

        (group,) = self.published()
        self.assertEqual([point[1] for point in group], ["fine", "nan"])
        self.assertTrue(math.isnan(group[1][3]))
        with mock.patch.object(
            client_module, "_delivery_status_snapshot", return_value=(0, 0, 0)
        ):
            with self.assertLogs("kymo", level="WARNING") as logs:
                self.assertFalse(client_module.wait_for_upload(timeout=0))
        self.assertIn("values were dropped after log() returned", logs.output[0])

    def test_a_publication_failure_is_reported_instead_of_raised(self):
        client_module.log({"gpu": _GpuScalar(1.0), "cpu": 2.0}, step=1)
        self.reads[0].complete = True
        self.queue.put.side_effect = OSError("queue closed")

        with self.assertLogs("kymo", level="WARNING") as logs:
            client_module.log({"next": 3.0}, step=2)

        self.assertIn("failed to publish the values of step 1", logs.output[0])
        self.assertIn("failed to publish the values of step 2", logs.output[1])
        self.assertTrue(client_module._pending_incomplete)
        self.assertEqual(len(client_module._pending), 0)

    def test_an_interrupted_publication_is_reported_and_never_repeated(self):
        client_module.log({"gpu": _GpuScalar(1.0)}, step=1)
        self.reads[0].complete = True
        seen = []

        def interrupted():
            # A shutdown signal handler that runs here must report step 1 incomplete.
            seen.append(client_module._pending_incomplete)
            raise KeyboardInterrupt

        self.reads[0].scalars = interrupted
        with self.assertRaises(KeyboardInterrupt):
            client_module.log({}, step=2)

        self.assertEqual(list(map(bool, seen)), [True])
        # Only step 2's own entry is left: that handler would not publish step 1 again.
        self.assertEqual(len(client_module._pending), 1)
        self.assertIsNone(client_module._pending[0].reads)
        self.assertTrue(client_module._pending_incomplete)

    def test_a_value_a_nested_publication_drops_stays_reported(self):
        # A handler kymo does not own (a per-step SIGALRM, say) logs during a pending call's publication and publishes the next call, which drops a value.
        client_module.log({"fine": _GpuScalar(1.0)}, step=1)
        client_module.log({"huge": _GpuScalar(1e300)}, step=2)
        for reads in self.reads:
            reads.complete = True
        nested = []

        def put(item, block=True, timeout=None):
            if not nested:
                nested.append(True)
                client_module.log({}, step=3)

        self.queue.put.side_effect = put
        with self.assertLogs("kymo", level="WARNING") as logs:
            client_module._publish_oldest_pending(None)
        self.assertIn("huge", logs.output[0])
        self.assertEqual((nested, len(client_module._pending)), ([True], 0))
        self.assertIs(client_module._pending_incomplete, True)

    def test_a_log_nested_anywhere_in_a_pending_calls_hand_over_puts_each_call_once(
        self,
    ):
        # A handler kymo does not own (one installed after init(), a profiler's timer) logs at each opcode of the oldest pending call's hand-over, and its log() publishes the calls whose copies have completed, perhaps this one. Each call reaches the queue once, in call order, the one still copying once its copies complete, and the backlog is what was put. Python 3.10 runs handlers at a taken jump, between a check that the call is still pending and its pop among them.
        take = next(
            code
            for code in client_module._publish_oldest_pending.__code__.co_consts
            if isinstance(code, types.CodeType) and code.co_name == "take"
        )
        for k in itertools.count():
            self.queue.reset_mock()
            client_module._queue_status.value = 0
            client_module.log({"first": _GpuScalar(1.0)}, step=1)
            client_module.log({"second": _GpuScalar(2.0)}, step=2)
            self.reads[-2].complete = True
            tracer, fired = _opcode_tracer(
                {take}, k, lambda frame: client_module.log({"nested": 3.0}, step=3)
            )
            with _tracing(tracer):
                client_module._publish_oldest_pending(None)
            self.reads[-1].complete = True
            client_module._publish_pending(None)
            state = f"log() at opcode {k}"
            self.assertEqual(
                [point[1] for group in self.published() for point in group],
                ["first", "second", "nested"] if fired else ["first", "second"],
                state,
            )
            self.assertEqual(
                client_module._queue_status.value,
                sum(
                    client_module._queue_item_size(call.args[0])
                    for call in self.queue.put.call_args_list
                ),
                state,
            )
            self.assertEqual(len(client_module._pending), 0, state)
            self.assertFalse(client_module._pending_incomplete, state)
            if not fired:
                break
        self.assertGreater(k, 5)

    def test_a_cuda_error_drops_the_gpu_values_and_keeps_later_logs_working(self):
        client_module.log({"gpu": _GpuScalar(1.0), "cpu": 2.0}, step=1)
        client_module.log({"gpu": _GpuScalar(3.0)}, step=2)
        for reads in self.reads:
            reads.error = RuntimeError("CUDA error: an illegal memory access")

        with self.assertLogs("kymo", level="WARNING") as logs:
            client_module.log({"later": 4.0}, step=3)

        self.assertIn("illegal memory access", logs.output[0])
        self.assertEqual(
            self.published(),
            [
                [("numeric_ts", "cpu", 1, 2.0, 1234)],
                [("numeric_ts", "later", 3, 4.0, 1234)],
            ],
        )
        self.assertTrue(client_module._pending_incomplete)

    def test_gpu_images_are_copied_to_the_host_and_pickled_once_copied(self):
        pixels = np.arange(12, dtype=np.uint8).reshape(2, 2, 3)
        image = Image(_GpuImage(pixels), caption="c")
        client_module.log(
            {"single": image, "gallery": [Image(_GpuImage(pixels))]}, step=1
        )
        self.assertEqual(_pending_nbytes(), 2 * pixels.nbytes)
        # The caller's image keeps its data; the queued copy's becomes the host copy.
        self.assertIs(type(image.data), _GpuImage)

        self.reads[0].complete = True
        client_module.log({}, step=2)

        ((single,), (gallery,)) = self.published()
        self.assertEqual(single[:3], ("cdn_batch", "single", 1))
        self.assertEqual(single[3][0].caption, "c")
        np.testing.assert_array_equal(single[3][0].data, pixels)
        np.testing.assert_array_equal(gallery[3][0].data, pixels)
        self.assertEqual(_pending_nbytes(), 0)

    def test_a_pending_gallery_published_by_log_waits_for_the_encoder_unpopped(self):
        pacing = _patch_encoder_pacing(self)
        pixels = np.zeros((2, 2, 3), dtype=np.uint8)
        _log_promptly(
            self,
            ({"cpu": [Image(pixels)]}, 1),
            ({"cpu": [Image(pixels)]}, 2),
            ({"gpu": [Image(_GpuImage(pixels))]}, 3),
        )
        self.reads[0].complete = True
        # The later log() publishes the GPU call, whose gallery is two calls ahead of the encoder.
        done = _in_thread(lambda: client_module.log({"later": 1.0}, step=4))
        self.assertFalse(done.wait(timeout=0.1))
        # It waits at the head of the pending calls, the later one queued behind it.
        self.assertEqual(len(client_module._pending), 2)
        self.assertTrue(client_module._pending[0].gpu_batches)
        pacing.released.value = 1
        self.assertTrue(done.wait(timeout=1))
        self.assertEqual(
            [group[0][1] for group in self.published()],
            ["cpu", "cpu", "gpu", "later"],
        )

    def test_a_published_gpu_gallery_counts_toward_the_encoder_wait(self):
        pacing = _patch_encoder_pacing(self)
        pixels = np.zeros((2, 2, 3), dtype=np.uint8)
        _log_promptly(self, ({"gpu": [Image(_GpuImage(pixels))]}, 1))
        self.reads[0].complete = True
        _log_promptly(self, ({"cpu": [Image(pixels)]}, 2))
        # The GPU call's gallery, published by the second call, is two calls back for the third.
        third = _in_thread(lambda: client_module.log({"next": [Image(pixels)]}, step=3))
        self.assertFalse(third.wait(timeout=0.1))
        pacing.released.value = 1
        self.assertTrue(third.wait(timeout=1))

    def test_a_pending_call_published_during_its_wait_is_not_published_again(self):
        _patch_encoder_pacing(self)
        pixels = np.zeros((2, 2, 3), dtype=np.uint8)
        _log_promptly(self, ({"gpu": [Image(_GpuImage(pixels))]}, 1))
        self.reads[0].complete = True

        def finish_during_the_wait():
            # Like a chained signal handler that runs finish() and returns.
            client_module._publish_pending(None)

        with mock.patch.object(
            client_module, "_await_encoder", side_effect=finish_during_the_wait
        ):
            _log_promptly(self, ({"later": 1.0}, 2))
        self.assertEqual([group[0][1] for group in self.published()], ["gpu", "later"])

    def test_a_call_whose_gpu_copies_failed_does_not_wait_for_the_encoder(self):
        _patch_encoder_pacing(self)
        client_module._galleries_published = 2
        pixels = np.zeros((2, 2, 3), dtype=np.uint8)
        _log_promptly(self, ({"gpu": [Image(_GpuImage(pixels))]}, 1))
        self.reads[0].error = RuntimeError("CUDA error: an illegal memory access")
        # The failed copies drop its only gallery, so it has nothing to wait for.
        _log_promptly(self, ({"later": 1.0}, 2))

    def test_publishing_pending_calls_outside_log_never_waits_for_the_encoder(self):
        _patch_encoder_pacing(self)
        pixels = np.zeros((2, 2, 3), dtype=np.uint8)
        _log_promptly(
            self,
            ({"cpu": [Image(pixels)]}, 1),
            ({"cpu": [Image(pixels)]}, 2),
            ({"gpu": [Image(_GpuImage(pixels))]}, 3),
        )
        self.reads[0].complete = True
        # wait_for_upload() and finish() publish this way.
        published = _in_thread(lambda: client_module._publish_pending(None))
        self.assertTrue(published.wait(timeout=1))
        self.assertEqual(len(self.published()), 3)

    def test_a_gallery_with_any_other_item_is_snapshotted_now(self):
        client_module.log(
            {"gallery": [Image(_GpuImage(np.zeros(2))), Image(np.ones(2))]}, step=1
        )
        self.assertEqual(self.reads, [])
        ((batch,),) = self.published()
        self.assertEqual(batch[1], "gallery")

    def test_log_waits_for_the_oldest_call_when_pending_copies_exceed_the_cap(self):
        with mock.patch.object(client_module, "_PENDING_MAX_BYTES", 100):
            client_module.log(
                {"image": Image(_GpuImage(np.zeros(80, np.uint8)))}, step=1
            )
            client_module.log(
                {"image": Image(_GpuImage(np.ones(80, np.uint8)))}, step=2
            )

        # The second call waited for the first's copies and published it before starting its own.
        self.assertTrue(self.reads[0].complete)
        self.assertFalse(self.reads[1].complete)
        ((batch,),) = self.published()
        self.assertEqual(batch[2], 1)
        np.testing.assert_array_equal(batch[3][0].data, np.zeros(80, np.uint8))
        self.assertEqual(_pending_nbytes(), 80)

    def test_scalar_copies_count_toward_the_cap(self):
        with mock.patch.object(client_module, "_PENDING_MAX_BYTES", 100):
            client_module.log({"t": [_GpuScalar(0.0)] * 10}, step=1)
            client_module.log({"t": [_GpuScalar(1.0)] * 10}, step=2)

        self.assertTrue(self.reads[0].complete)
        self.assertEqual(_pending_nbytes(), 160)

    def test_pickled_rich_values_count_toward_the_cap(self):
        # A pending call's CPU images wait pickled beside its GPU values: their bytes count too.
        with mock.patch.object(client_module, "_PENDING_MAX_BYTES", 100):
            client_module.log(
                {"t": _GpuScalar(0.0), "image": Image(np.zeros(200, np.uint8))},
                step=1,
            )
            (pending,) = client_module._pending
            self.assertGreater(pending.nbytes, 216)
            client_module.log({"t": _GpuScalar(1.0)}, step=2)

        self.assertTrue(self.reads[0].complete)
        self.assertEqual(_pending_nbytes(), 16)

    def test_graph_capture_rejects_gpu_values_and_never_polls(self):
        self.capturing = True
        with self.assertRaisesRegex(RuntimeError, "CUDA graph capture"):
            client_module.log({"gpu": _GpuScalar(1.0)}, step=1)
        self.assertEqual(self.reads, [])

        self.capturing = False
        client_module.log({"gpu": _GpuScalar(1.0)}, step=1)
        self.reads[0].complete = True
        self.capturing = True
        client_module.log({"cpu": 2.0}, step=2)
        self.assertEqual(self.published(), [])
        self.assertEqual(len(client_module._pending), 2)

        self.capturing = False
        client_module.log({}, step=3)
        self.assertEqual(len(self.published()), 2)

    def test_a_fork_child_forgets_the_parents_pending_calls(self):
        client_module.log({"gpu": _GpuScalar(1.0)}, step=1)
        client_module._pending_incomplete = True

        client_module._forget_pending_in_child()

        self.assertEqual(len(client_module._pending), 0)
        self.assertFalse(client_module._pending_incomplete)
        # Freeing the parent's CUDA objects would abort the child.
        self.assertEqual(gpu_module.unfreeable, set(self.reads))


_REQUIRES_CUDA = unittest.skipUnless(
    torch is not None and torch.cuda.is_available(), "requires a CUDA device"
)


class EncoderPacingTests(unittest.TestCase):
    """A log() call that publishes galleries waits while the worker holds raw pixels from before the previous such call, unless anything is on disk."""

    def setUp(self):
        self.queue = _patch_log_client(self)
        self.pacing = _patch_encoder_pacing(self)

    @staticmethod
    def gallery() -> list:
        return [Image(np.zeros((2, 2, 3), dtype=np.uint8))]

    def published_names(self) -> list:
        return [group[0][1] for group in _published_groups(self.queue)]

    def test_a_gallery_call_waits_until_the_one_two_calls_back_is_encoded(self):
        _log_promptly(
            self,
            ({"first": self.gallery()}, 1),
            ({"second": self.gallery(), "third": self.gallery()}, 2),
        )
        # Both of the previous call's galleries may still be raw, but not the first call's.
        done = _in_thread(lambda: client_module.log({"next": self.gallery()}, step=3))
        self.assertFalse(done.wait(timeout=0.1))
        self.assertEqual(self.published_names(), ["first", "second", "third"])
        self.pacing.released.value = 1
        self.assertTrue(done.wait(timeout=1))
        self.assertEqual(self.published_names(), ["first", "second", "third", "next"])

    def assert_never_waits(self):
        _log_promptly(self, *[({"samples": self.gallery()}, step) for step in range(3)])
        self.assertEqual(len(self.published_names()), 3)

    def test_never_waits_within_the_budget_of_a_sigterm_the_program_took(self):
        # A handler installed before init() took the SIGTERM: the program is ending, and the atexit flush gets what is left of the signal budget, which a wait for the encoder would use up.
        with mock.patch.object(client_module, "_terminated_at", time.monotonic()):
            self.assert_never_waits()

    def test_never_waits_while_a_lane_is_on_disk(self):
        self.pacing.on_disk.value = 1
        self.assert_never_waits()

    def test_never_waits_for_a_terminal_run(self):
        client_module._upload_terminal.value = 1
        self.assert_never_waits()

    def test_never_waits_once_shutdown_has_begun(self):
        # The shutdown ends the run the call was for: the call raises at once, as one after the shutdown does, rather than publish to the queue the shutdown closes.
        with (
            mock.patch.object(client_module, "_finishing", True),
            self.assertRaisesRegex(RuntimeError, "not initialised"),
        ):
            client_module.log({"samples": self.gallery()}, step=0)
        self.assertEqual(self.published_names(), [])

    def test_never_waits_for_an_exited_worker(self):
        self.pacing.process.is_alive.return_value = False
        self.assert_never_waits()

    def test_never_waits_in_a_fork_child(self):
        with mock.patch.object(client_module, "_init_pid", os.getpid() + 1):
            self.assert_never_waits()

    def test_concurrent_callers_cannot_pass_one_encoder_check_together(self):
        snapshot = client_module._snapshot_rich_queue_items

        def slow_snapshot(batches):
            # Long enough that unserialized callers would all pass the check before any publishes.
            time.sleep(0.02)
            return snapshot(batches)

        with mock.patch.object(
            client_module, "_snapshot_rich_queue_items", side_effect=slow_snapshot
        ):
            done = [
                _in_thread(
                    lambda: client_module.log({"samples": self.gallery()}, step=1)
                )
                for _ in range(8)
            ]
            time.sleep(0.3)
            # Two calls' galleries may be raw; the other six wait.
            self.assertEqual(client_module._galleries_published, 2)
            self.pacing.released.value = 8
            self.assertTrue(all(event.wait(timeout=2) for event in done))
        self.assertEqual(client_module._galleries_published, 8)

    def test_a_waiting_gallery_call_does_not_hold_other_threads_writes(self):
        _log_promptly(
            self, ({"first": self.gallery()}, 1), ({"second": self.gallery()}, 2)
        )
        waiting = _in_thread(
            lambda: client_module.log({"third": self.gallery()}, step=3)
        )
        self.assertFalse(waiting.wait(timeout=0.1))
        # Another thread's write that takes the pending-call lock goes through while that call waits for the encoder.
        written = _in_thread(lambda: client_module.log_cdn({"key": "abc"}, step=4))
        self.assertTrue(written.wait(timeout=1))
        self.assertIn("key", self.published_names())
        self.assertFalse(waiting.is_set())
        self.pacing.released.value = 2
        self.assertTrue(waiting.wait(timeout=1))

    def test_a_call_without_galleries_never_waits(self):
        _log_promptly(
            self,
            ({"samples": self.gallery()}, 1),
            ({"samples": self.gallery()}, 2),
            ({"loss": 1.0}, 3),
        )

    def test_a_gallery_reaches_the_worker_as_a_plain_tuple(self):
        item = _unversioned_gallery(1, self.gallery())
        self.assertIs(type(item), client_module._GalleryQueueItem)
        copied = multiprocessing.reduction.ForkingPickler.loads(
            multiprocessing.reduction.ForkingPickler.dumps(item)
        )
        self.assertIs(type(copied), tuple)
        self.assertEqual(copied, item)

    def test_a_held_back_gallery_counts_once_a_later_publication_puts_it(self):
        # Charged before its put, counted once its put returns: a gallery a full queue held back counts only when the next publication, here one without galleries of its own, puts it.
        class FullForGalleries(_ListQueue):
            room = False

            def put(self, item, block=True, timeout=None):
                if not self.room and type(item) is client_module._GalleryQueueItem:
                    raise queue.Full
                super().put(item)

        target, gallery = FullForGalleries(), _unversioned_gallery(1, self.gallery())
        with mock.patch.object(client_module, "_metric_queue", target):
            with self.assertRaises(queue.Full):
                client_module._publish_queue_items(
                    [[numeric(1)], gallery],
                    hard_deadline=time.monotonic(),
                    galleries=True,
                )
            self.assertEqual(list(client_module._held_back), [gallery])
            self.assertEqual(client_module._galleries_published, 0)
            target.room = True
            client_module._publish_queue_items([[numeric(2)]])
        self.assertEqual(target.items, [[numeric(1)], gallery, [numeric(2)]])
        self.assertEqual(client_module._galleries_published, 1)
        self.assertEqual(client_module._previous_galleries, 1)

    def test_a_gallery_held_back_uncharged_counts_once_a_later_publication_puts_it(
        self,
    ):
        # A busy accounting lock holds a pending call's groups back uncharged; the next publication charges and puts them, and only then counts its gallery.
        lock_busy = [True]
        change = client_module._charge_backlog

        def busy_once(points, hard_deadline):
            if lock_busy.pop() if lock_busy else False:
                raise client_module._AccountingLockUnavailable("busy")
            change(points, hard_deadline)

        gallery = _unversioned_gallery(1, self.gallery())
        client_module._pending.append(
            client_module._PendingLog([[numeric(1)]], [gallery], galleries=True)
        )
        with mock.patch.object(client_module, "_charge_backlog", side_effect=busy_once):
            client_module._publish_pending(None)
            self.assertEqual(list(client_module._uncharged), [[[numeric(1)]], gallery])
            self.assertEqual(client_module._galleries_published, 0)
            client_module._publish_queue_items([])
        self.assertEqual(list(client_module._uncharged), [])
        self.assertEqual(client_module._galleries_published, 1)

    def test_a_gallery_whose_put_a_handler_interrupted_never_counts(self):
        # The queue may or may not have taken it, so it does not count: an uncounted gallery only lets a later call wait less.
        class Interrupted(Exception):
            pass

        def interrupt(item):
            if type(item) is client_module._GalleryQueueItem:
                raise Interrupted

        galleries = [_unversioned_gallery(step, self.gallery()) for step in (1, 2)]
        with (
            mock.patch.object(
                client_module, "_metric_queue", _ListQueue(on_put=interrupt)
            ),
            self.assertRaises(Interrupted),
        ):
            client_module._publish_queue_items(
                [[numeric(1)], *galleries], galleries=True
            )
        self.assertEqual(client_module._galleries_published, 0)
        # The gallery after it waits, held back and charged, for the next publication.
        self.assertEqual(len(client_module._held_back), 1)


class _CudaLogTests:
    """log() with real CUDA tensors: exact values, snapshots at the call, and no wait for the GPU, copying scalars with the subclass's backend."""

    backend: str

    @classmethod
    def setUpClass(cls):
        cls.launch = None
        if cls.backend != "stack":
            try:
                # A function stored on the class would bind to the test as a method.
                cls.launch = staticmethod(
                    getattr(gpu_module, cls.backend + "_gather")(torch)
                )
            except (AttributeError, ImportError) as error:
                # A torch with _compile_kernel must build the NVRTC kernel, not quietly fall back.
                if (
                    cls.backend == "nvrtc"
                    and hasattr(torch.cuda, "_compile_kernel")
                    and not torch.version.hip
                ):
                    raise
                raise unittest.SkipTest(f"this torch cannot build it: {error}")

    def setUp(self):
        self.queue = _patch_log_client(self)
        torch.cuda.synchronize()
        launch = self.launch
        patches = [
            mock.patch.object(gpu_module, "gather", lambda torch, device: launch)
        ]
        if launch is not None:
            # A kernel never stacks.
            patches.append(
                mock.patch.object(torch, "stack", side_effect=AssertionError("stacked"))
            )
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

    def publish(self):
        """Wait for every pending call and return every published item, decoded."""
        with client_module._pending_lock:
            client_module._publish_pending(None)
        return [item for group in _published_groups(self.queue) for item in group]

    def values(self):
        """publish()'s numeric values by name, or by (name, tag) for a tagged one."""
        return {
            (point[1], point[4])
            if point[0] == "numeric_tagged_ts"
            else point[1]: point[3]
            for point in self.publish()
        }

    def test_random_calls_publish_a_tag_per_value(self):
        # PendingGpuLogTests' random calls, with CUDA scalars of each dtype read by this backend; NaN is left to the test below, which compares it as NaN.
        per_dtype = {
            torch.float16: (1 / 3, -6e-8, math.inf),
            torch.bfloat16: (1 / 3, -0.0),
            torch.float32: (1 / 3, -0.0, 1e-45),
            torch.float64: (1 / 3, 5e-324, 1e300),
            torch.int64: (2**53 + 1, -(2**63)),
            torch.int32: (-7,),
            torch.int8: (-3,),
            torch.uint8: (255,),
            torch.bool: (True,),
        }
        _assert_random_calls_publish_a_tag_per_value(
            self,
            [
                torch.tensor(v, dtype=dtype, device="cuda")
                for dtype, values in per_dtype.items()
                for v in values
            ],
            lambda value: float(value) if isinstance(value, torch.Tensor) else None,
            1200,
        )

    def test_values_equal_float_for_every_dtype(self):
        cuda = torch.device("cuda")
        values = {
            "float16": torch.tensor(1 / 3, dtype=torch.float16, device=cuda),
            "float16/inf": torch.tensor(math.inf, dtype=torch.float16, device=cuda),
            "float16/subnormal": torch.tensor(-6e-8, dtype=torch.float16, device=cuda),
            "bfloat16": torch.tensor(1 / 3, dtype=torch.bfloat16, device=cuda),
            "float32": torch.tensor(1 / 3, device=cuda),
            "float32/nan": torch.tensor(math.nan, device=cuda),
            "float32/negative zero": torch.tensor(-0.0, device=cuda),
            "float64": torch.tensor(1 / 3, dtype=torch.float64, device=cuda),
            "float64/subnormal": torch.tensor(5e-324, dtype=torch.float64, device=cuda),
            "int64": torch.tensor(65520, device=cuda),
            "int64/beyond 2**53": torch.tensor(2**53 + 1, device=cuda),
            "int64/min": torch.tensor(-(2**63), device=cuda),
            "int32": torch.tensor(-7, dtype=torch.int32, device=cuda),
            "int16": torch.tensor(-32768, dtype=torch.int16, device=cuda),
            "int8": torch.tensor(-3, dtype=torch.int8, device=cuda),
            "uint8": torch.tensor(255, dtype=torch.uint8, device=cuda),
            "bool": torch.tensor(True, device=cuda),
            "bool/false": torch.tensor(False, device=cuda),
            "one element, 2-d": torch.tensor([[2.5]], device=cuda),
            "storage offset": torch.arange(10.0, device=cuda)[7],
            # autograd's efficient zero tensor has no memory: data_ptr() is 0.
            "zero tensor": torch._efficientzerotensor((), device=cuda),
            # Its bytes hold 3; it is read synchronously.
            "negative view": torch._neg_view(torch.tensor(3.0, device=cuda)),
            "parameter": torch.nn.Parameter(torch.tensor(4.0, device=cuda)),
            "requires grad": torch.tensor(2.0, device=cuda, requires_grad=True) * 3,
        }
        tagged = [
            torch.tensor(65504, dtype=torch.float16, device=cuda),
            torch.tensor(65520, device=cuda),
            1.5,
        ]
        expected = {name: float(value.detach()) for name, value in values.items()}
        expected.update({("tagged", str(i)): float(v) for i, v in enumerate(tagged)})

        with mock.patch.object(gpu_module, "Reads", wraps=gpu_module.Reads) as reads:
            client_module.log({**values, "tagged": tagged}, step=1)
        reads.assert_called_once()

        logged = self.values()
        self.assertEqual(logged.keys(), expected.keys())
        for key, value in expected.items():
            with self.subTest(key=key):
                self.assertTrue(
                    gpu_module._same(logged[key], value),
                    f"{logged[key]!r} != {value!r}",
                )
                self.assertIs(type(logged[key]), float)

    def test_every_float16_and_bfloat16_bit_pattern_reads_exactly(self):
        patterns = torch.arange(1 << 16, dtype=torch.int32, device="cuda").to(
            torch.int16
        )
        if self.launch is None:
            self.skipTest("the stack converts with torch")
        for dtype in (torch.float16, torch.bfloat16):
            with self.subTest(dtype=dtype):
                values = patterns.view(dtype)
                reads = gpu_module.Reads(torch, list(values), [])
                reads.wait()
                got = [value for _, value in sorted(reads.scalars())]
                for index, (read, want) in enumerate(
                    zip(got, values.double().tolist())
                ):
                    if not gpu_module._same(read, want):
                        self.fail(
                            f"bit pattern {index:#06x}: read {read!r}, want {want!r}"
                        )

    def test_tensors_whose_bytes_are_not_their_value_read_as_float_does(self):
        cuda = torch.device("cuda")
        zeros = torch._efficientzerotensor((4,), device=cuda)
        values = {
            # Autograd's efficient zero tensors: data_ptr() is an offset from a null allocation.
            "zero views": list(zeros),
            "zero tensor": torch._efficientzerotensor((), device=cuda),
            # A functional tensor's data_ptr() is 0.
            "functional": torch._to_functional_tensor(torch.tensor(3.0, device=cuda)),
            "plain": torch.tensor(2.0, device=cuda),
        }
        client_module.log(values, step=1)
        logged = self.values()
        self.assertEqual(
            logged,
            {
                **{("zero views", str(i)): 0.0 for i in range(4)},
                "zero tensor": 0.0,
                "functional": 3.0,
                "plain": 2.0,
            },
        )

    def test_a_view_of_a_freed_storage_raises_as_float_does(self):
        base = torch.arange(4.0, device="cuda")
        view = base[2]
        base.untyped_storage().resize_(0)
        with self.assertRaisesRegex(RuntimeError, "not allocated|invalid"):
            float(view)
        with self.assertRaises(RuntimeError):
            client_module.log({"view": view}, step=1)

    def test_log_inside_a_torch_func_transform_reads_synchronously(self):
        def loss(x):
            total = (x * 2).sum()
            client_module.log({"total": total}, step=1)
            return total

        torch.func.grad(loss)(torch.ones(3, device="cuda"))
        self.assertEqual(
            [point[3] for point in self.publish() if point[1] == "total"], [6.0]
        )

    def test_a_cuda_default_device_does_not_make_log_wait(self):
        value = torch.ones((), device="cuda")
        client_module.log({"value": value}, step=0)
        self.publish()
        self.queue.reset_mock()
        torch.set_default_device("cuda")
        self.addCleanup(torch.set_default_device, None)
        torch.cuda._sleep(2_000_000_000)  # about a second of queued GPU work
        started = time.perf_counter()
        client_module.log({"value": value}, step=1)
        self.assertLess(time.perf_counter() - started, 0.2)
        self.assertEqual(
            [point[3] for point in self.publish() if point[1] == "value"], [1.0]
        )

    def test_a_dropped_call_keeps_its_buffers_from_the_gpu_writes_still_queued(self):
        scalars = [torch.full((), float(i), device="cuda") for i in range(64)]
        client_module.log({"scalars": scalars}, step=0)
        self.publish()
        torch.cuda._sleep(2_000_000_000)  # about a second of queued GPU work
        client_module.log({"scalars": scalars}, step=1)
        (entry,) = client_module._pending
        reads = entry.reads
        with client_module._pending_lock, self.assertLogs("kymo", level="WARNING"):
            client_module._publish_oldest_pending(
                time.monotonic(), drop_unfinished=True
            )
        self.assertIn(reads, gpu_module.unfreeable)
        del entry, reads
        # Pinned blocks the call freed early would be handed out again here, and its gather would then write into them.
        sentinels = [
            torch.full((64,), -1.0, dtype=torch.float64, device="cpu", pin_memory=True)
            for _ in range(32)
        ]
        torch.cuda.synchronize()
        self.assertTrue(all(bool((s == -1.0).all()) for s in sentinels))

    def test_a_float64_beyond_float32_is_dropped(self):
        # log() itself publishes it when its copies complete before it returns.
        with self.assertLogs("kymo", level="WARNING"):
            client_module.log(
                {
                    "huge": torch.tensor(1e300, dtype=torch.float64, device="cuda"),
                    "fine": torch.tensor(1.0, device="cuda"),
                },
                step=1,
            )
            points = self.publish()
        self.assertEqual([point[1] for point in points], ["fine"])
        self.assertTrue(client_module._pending_incomplete)

    def test_log_returns_before_queued_gpu_work_and_reads_the_value_at_the_call(self):
        value = torch.zeros((), device="cuda")
        pixels = torch.randint(0, 256, (3, 64, 48), dtype=torch.uint8, device="cuda")
        scalars = [torch.full((), float(i), device="cuda") for i in range(1000)]
        # CUDA loads a kernel at its first launch, waiting for the GPU; this call loads the ones below.
        client_module.log(
            {
                "value": value,
                "scalars": scalars,
                "image": Image(pixels.permute(1, 2, 0)),
            },
            step=0,
        )
        self.publish()
        self.queue.reset_mock()

        torch.cuda._sleep(2_000_000_000)  # about a second of queued GPU work
        torch.cuda.set_sync_debug_mode("error")
        try:
            started = time.perf_counter()
            client_module.log(
                {
                    "value": value,
                    "scalars": scalars,
                    "image": Image(pixels.permute(1, 2, 0)),
                },
                step=1,
            )
            elapsed = time.perf_counter() - started
            # The caller changes the tensors right after the call.
            value.add_(1)
            pixels.zero_()
        finally:
            torch.cuda.set_sync_debug_mode(0)
        self.assertFalse(client_module._pending[0].reads.done())
        self.assertLess(elapsed, 0.2)

        points = self.publish()
        by_name = {point[1]: point for point in points}
        self.assertEqual(by_name["value"][3], 0.0)
        tagged = sorted(
            (int(point[4]), point[3])
            for point in points
            if point[0] == "numeric_tagged_ts"
        )
        self.assertEqual(tagged, [(i, float(i)) for i in range(1000)])
        image = by_name["image"][3][0].data
        self.assertIsInstance(image, np.ndarray)
        self.assertTrue(image.any())

    def test_images_equal_their_synchronous_snapshot(self):
        pixels = torch.randint(0, 256, (3, 64, 48), dtype=torch.uint8, device="cuda")
        images = {
            "contiguous": pixels,
            "permuted": pixels.permute(1, 2, 0),
            "one channel": pixels[1],
            "float": pixels.float() / 255,
            "bfloat16": pixels.bfloat16(),
        }
        caller = {name: Image(data) for name, data in images.items()}
        client_module.log(dict(caller), step=1)
        for name, image in caller.items():
            self.assertIs(image.data, images[name])

        logged = {point[1]: point[3][0].data for point in self.publish()}
        for name, data in images.items():
            with self.subTest(name=name):
                expected = client_module._decode_queue_item(
                    client_module._snapshot_rich_queue_items(
                        [("cdn_batch", name, 1, [Image(data)])]
                    )[0]
                )[0][3][0].data
                if isinstance(expected, np.ndarray):
                    np.testing.assert_array_equal(logged[name], expected)
                else:
                    self.assertTrue(torch.equal(logged[name], expected))

    def test_backpressure_waits_for_the_oldest_call(self):
        pixels = torch.zeros(1024, dtype=torch.uint8, device="cuda")
        with mock.patch.object(client_module, "_PENDING_MAX_BYTES", 1500):
            for step in range(3):
                torch.cuda._sleep(10_000_000)
                client_module.log({"image": Image(pixels)}, step=step)
        # The third call waited for the second's copies, and the second for the first's.
        self.assertEqual(len(client_module._pending), 1)
        published = [
            item
            for group in _published_groups(self.queue)
            for item in group
            if item[1] == "image"
        ]
        self.assertEqual([batch[2] for batch in published], [0, 1])

    def test_another_tensor_subclass_is_read_synchronously(self):
        class Subclass(torch.Tensor):
            pass

        value = torch.tensor(1.25, device="cuda").as_subclass(Subclass)
        client_module.log({"subclass": value}, step=1)
        self.assertEqual(len(client_module._pending), 0)
        self.assertEqual(
            [point[3] for point in self.publish() if point[1] == "subclass"], [1.25]
        )

    def test_a_retained_source_keeps_no_autograd_graph_alive(self):
        client_module.log({"warm": torch.ones((), device="cuda")}, step=0)
        self.publish()
        weight = torch.ones((), device="cuda", requires_grad=True)
        activations = torch.rand(1 << 22, device="cuda")
        before = torch.cuda.memory_allocated()
        # exp saves its 16 MiB output for a backward pass that never comes.
        metric = torch.exp(activations * weight).sum()
        torch.cuda._sleep(200_000_000)
        client_module.log({"metric": metric}, step=1)
        self.assertEqual(len(client_module._pending), 1)

        del metric
        self.assertLess(torch.cuda.memory_allocated() - before, 1 << 20)
        self.publish()

    _SUBPROCESS_PRELUDE = """
import multiprocessing, os
from unittest import mock
import functools
import torch
from kymo import _gpu, client
# BACKEND, set before this prelude, picks how scalars are copied.
if BACKEND == "stack":
    _gpu.gather = lambda torch, device: None
else:
    build = getattr(_gpu, BACKEND + "_gather")
    _gpu.gather = functools.cache(lambda torch, device: build(torch))
patches = mock.patch.multiple(
    client,
    _is_initialized=True,
    _init_pid=os.getpid(),
    _metric_queue=mock.Mock(),
    _queue_status=multiprocessing.Value("q", 0),
)


def log_pending(metrics):
    # CUDA loads a kernel at its first launch, waiting for the GPU; the first call loads them.
    client.log(metrics, step=0)
    client._publish_pending(None)
    torch.cuda._sleep(500_000_000)
    client.log(metrics, step=1)
    assert client._pending
"""

    def _run_script(self, script: str, **env) -> subprocess.CompletedProcess:
        return _run_script_file(
            f"BACKEND = {self.backend!r}\n" + self._SUBPROCESS_PRELUDE + script, **env
        )

    def test_log_never_waits_where_the_allocator_maps_device_memory_low(self):
        # Expandable segments map device memory below _NULL_OFFSET_BOUND, where each pointer is compared with its storage offset.
        result = self._run_script(
            """
with patches:
    cuda = torch.device("cuda")
    real = [torch.tensor(2.0, device=cuda), torch.arange(4.0, device=cuda)[3]]
    assert max(t.data_ptr() for t in real) < _gpu._NULL_OFFSET_BOUND
    log_pending({"real": real})
    assert not client._pending[0].reads.done(), "log() waited for the GPU"
    null = [
        torch._efficientzerotensor((4,), device=cuda)[2],
        torch._to_functional_tensor(torch.tensor(5.0, device=cuda)),
    ]
    reads = _gpu.Reads(torch, real + null, [])
    reads.wait()
    assert [value for _, value in sorted(reads.scalars())] == [2.0, 3.0, 0.0, 5.0]
print("done")
""",
            PYTORCH_CUDA_ALLOC_CONF="expandable_segments:True",
        )
        self.assertEqual(result.returncode, 0, result.stderr[-2000:])
        self.assertIn("done", result.stdout)

    def test_a_fork_child_never_frees_the_parents_cuda_objects(self):
        # Freeing a pinned host copy or an event in a fork child aborts it; this child runs a full interpreter shutdown.
        result = self._run_script(
            """
with patches:
    image = torch.zeros(8, 8, 3, dtype=torch.uint8, device="cuda")
    log_pending({"x": torch.ones((), device="cuda"), "image": client.Image(image)})
    pid = os.fork()
    if pid == 0:
        raise SystemExit(3)
    _, status = os.waitpid(pid, 0)
    print("child", os.waitstatus_to_exitcode(status))
    torch.cuda.synchronize()
"""
        )
        self.assertEqual(result.returncode, 0, result.stderr[-2000:])
        self.assertIn("child 3", result.stdout)

    def test_a_cuda_error_neither_breaks_later_logs_nor_aborts_the_process(self):
        # Freeing a pinned host copy after a CUDA error aborts the process.
        result = self._run_script(
            """
with patches:
    log_pending({"x": torch.ones((), device="cuda")})
    values = torch.zeros(10, device="cuda")
    values[torch.tensor([10**9], device="cuda")] = 1
    try:
        torch.cuda.synchronize()
    except Exception:
        pass
    client.log({"cpu": 1.0}, step=2)
    assert client._pending_incomplete and not client._pending
print("done")
"""
        )
        self.assertEqual(result.returncode, 0, result.stderr[-2000:])
        self.assertIn("done", result.stdout)

    def test_a_cuda_error_while_starting_the_copies_does_not_abort_the_process(self):
        # The error surfaces once a pinned host copy exists (the first dtype group's stack, or the kernel's word table); freeing it would abort.
        result = self._run_script(
            """
with patches:
    values = {
        "a": torch.ones((), device="cuda"),
        "b": torch.ones((), dtype=torch.int64, device="cuda"),
    }
    client.log(values, step=0)
    client._publish_pending(None)
    def fault():
        index = torch.tensor([10**9], device="cuda")
        torch.zeros(10, device="cuda")[index] = 1
        try:
            torch.cuda.synchronize()
        except Exception:
            pass
    if BACKEND == "stack":
        stack, calls = torch.stack, []
        def failing(tensors):
            calls.append(tensors)
            if len(calls) == 2:
                fault()
            return stack(tensors)
        patch = mock.patch.object(torch, "stack", failing)
    else:
        gather = _gpu.gather
        def failing(torch, device):
            launch = gather(torch, device)
            def launch_after_fault(*args):
                fault()
                launch(*args)
            return launch_after_fault
        patch = mock.patch.object(_gpu, "gather", failing)
    with patch:
        try:
            client.log(values, step=1)
        except Exception:
            print("log raised")
print("done")
"""
        )
        self.assertEqual(result.returncode, 0, result.stderr[-2000:])
        self.assertIn("log raised\ndone", result.stdout)

    def test_a_fresh_thread_logs_after_the_main_thread_has(self):
        # A thread on which nothing has used CUDA through the runtime has no current context, where the driver's calls fail: once the main thread's reads have filled torch's pinned-memory cache, a fresh thread's gather allocates nothing, which would leave the NVRTC kernel's raw cuLaunchKernel failing with CUDA error 201. Such a thread reads the right values, through Reads as log() makes them and through log(), and so does a kernel built on one.
        cuda = torch.device("cuda")
        values = [
            torch.tensor(1.5, device=cuda),
            torch.tensor(7, dtype=torch.int64, device=cuda),
            torch.tensor(2.25, dtype=torch.float16, device=cuda),
        ]
        expected = [(0, 1.5), (1, 7.0), (2, 2.25)]

        def on_a_fresh_thread(function):
            outcome = []

            def run():
                try:
                    outcome.append(function())
                except BaseException as error:
                    outcome.append(error)

            thread = threading.Thread(target=run)
            thread.start()
            thread.join()
            if isinstance(outcome[0], BaseException):
                raise outcome[0]
            return outcome[0]

        def read(launch):
            reads = gpu_module.Reads(torch, values, [], launch=launch)
            reads.wait()
            return sorted(reads.scalars())

        def log():
            client_module.log({"thread": values[1]}, step=2)
            with client_module._pending_lock:
                client_module._publish_pending(None)

        client_module.log({"main": values[0]}, step=1)
        self.assertEqual(self.values()["main"], 1.5)
        # The main thread's buffers go back to torch's cache, which the fresh thread's reads then take from.
        gc.collect()
        torch.cuda.synchronize()
        self.assertEqual(on_a_fresh_thread(lambda: read(self.launch)), expected)
        on_a_fresh_thread(log)
        self.assertEqual(self.values()["thread"], 7.0)
        if self.backend == "nvrtc":
            built = on_a_fresh_thread(lambda: gpu_module.nvrtc_gather(torch))
            self.assertEqual(on_a_fresh_thread(lambda: read(built)), expected)

    def test_graph_capture_rejects_cuda_values_before_touching_the_gpu(self):
        static = torch.zeros((), device="cuda")
        graph = torch.cuda.CUDAGraph()
        with self.assertRaisesRegex(RuntimeError, "CUDA graph capture"):
            with torch.cuda.graph(graph):
                static.add_(1)
                client_module.log({"x": static}, step=1)
        self.assertEqual(len(client_module._pending), 0)


@_REQUIRES_CUDA
class CudaNvrtcLogTests(_CudaLogTests, unittest.TestCase):
    backend = "nvrtc"


@_REQUIRES_CUDA
class CudaTritonLogTests(_CudaLogTests, unittest.TestCase):
    backend = "triton"


@_REQUIRES_CUDA
class CudaStackLogTests(_CudaLogTests, unittest.TestCase):
    backend = "stack"


# The driver entry points nvrtc_gather uses, for FakeDriverGatherTests: it records a launch's arguments and runs the gather kernel's loop on the host for float32, float64 and int64 words.
_FAKE_DRIVER = r"""
#include <stdint.h>
typedef void *CUcontext;
static __thread CUcontext current = 0;
int attribute = 1, last_attribute = -1, refused = 0;
long long offset = 0;
CUcontext made_current = 0;
struct { void *f; unsigned grid[3], block[3], shared; void *stream, *extra; } last;
int cuDeviceGet(int *device, int ordinal) { *device = ordinal; return 0; }
int cuCtxGetCurrent(CUcontext *context) { *context = current; return 0; }
int cuDevicePrimaryCtxRetain(CUcontext *context, int device) { *context = (CUcontext)(uintptr_t)(0x1000 + device); return 0; }
int cuCtxSetCurrent(CUcontext context) { made_current = current = context; return 0; }
int cuDeviceGetAttribute(int *value, int which, int device) { last_attribute = which; *value = attribute; return 0; }
int cuMemHostGetDevicePointer_v2(unsigned long long *mapped, void *host, unsigned flags) { *mapped = (uintptr_t)host + offset; return flags; }
void forget_current(void) { current = 0; }
int cuLaunchKernel(void *f, unsigned gx, unsigned gy, unsigned gz, unsigned bx, unsigned by, unsigned bz, unsigned shared, void *stream, void **params, void **extra) {
    if (!current) { refused++; return 201; }
    last.f = f; last.grid[0] = gx; last.grid[1] = gy; last.grid[2] = gz;
    last.block[0] = bx; last.block[1] = by; last.block[2] = bz; last.shared = shared; last.stream = stream; last.extra = extra;
    const unsigned long long *words = *(const unsigned long long **)params[0];
    double *out = *(double **)params[1];
    int n = *(int *)params[2];
    for (int i = 0; i < n && i < (int)(gx * bx); i++) {
        const void *p = (const void *)(uintptr_t)(words[i] & 0x00FFFFFFFFFFFFFFull);
        switch (words[i] >> 56) {
        case 0: out[i] = *(const float *)p; break;
        case 3: out[i] = *(const double *)p; break;
        case 4: out[i] = (double)*(const long long *)p; break;
        default: out[i] = -1; break;
        }
    }
    return 0;
}
"""


class FakeDriverGatherTests(unittest.TestCase):
    """The NVRTC gather's direct cuLaunchKernel path on the CPU, against a fake driver the system C compiler builds (_FAKE_DRIVER): the launch's prototype and kernel parameters, the retry of a launch refused for want of a context, and the checks that admit pinned host pointers. Skipped without a C compiler; the CUDA classes run the real path on a GPU."""

    class _Launch(ctypes.Structure):
        """The last launch's arguments, as _FAKE_DRIVER records them."""

        _fields_ = [
            ("f", ctypes.c_void_p),
            ("grid", ctypes.c_uint * 3),
            ("block", ctypes.c_uint * 3),
            ("shared", ctypes.c_uint),
            ("stream", ctypes.c_void_p),
            ("extra", ctypes.c_void_p),
        ]

    @classmethod
    def setUpClass(cls):
        compiler = shutil.which("cc")
        if compiler is None:
            raise unittest.SkipTest("no C compiler")
        with tempfile.TemporaryDirectory() as build:
            source, library = (
                os.path.join(build, "fake.c"),
                os.path.join(build, "fake.so"),
            )
            with open(source, "w") as f:
                f.write(_FAKE_DRIVER)
            subprocess.run(
                [compiler, "-shared", "-fPIC", "-o", library, source], check=True
            )
            cls.driver = ctypes.CDLL(library)

    def setUp(self):
        self.driver.forget_current()
        for name, value in (("attribute", 1), ("refused", 0), ("last_attribute", -1)):
            ctypes.c_int.in_dll(self.driver, name).value = value
        ctypes.c_longlong.in_dll(self.driver, "offset").value = 0

    def torch(self):
        class Tensor:
            requires_grad = False

            def __init__(self, values, dtype):
                self.dtype = dtype
                self.array = np.array(values, dtype=getattr(np, dtype))

            def data_ptr(self):
                return self.array.ctypes.data

            def storage_offset(self):
                return 0

            def element_size(self):
                return self.array.itemsize

            def is_floating_point(self):
                return self.dtype != "int64"

            def tolist(self):
                return self.array.tolist()

        event = types.SimpleNamespace(
            record=lambda stream: None, query=lambda: True, synchronize=lambda: None
        )
        torch = types.ModuleType("fake_torch")
        torch.__dict__.update({name: name for name in gpu_module.DTYPES})
        torch.version = types.SimpleNamespace(hip=None)
        torch.tensor = lambda values, dtype, device, pin_memory=False: Tensor(
            values, dtype
        )
        torch.empty = lambda n, dtype, device, pin_memory: Tensor(np.zeros(n), dtype)
        torch.cuda = types.SimpleNamespace(
            _utils=types.SimpleNamespace(_get_cuda_library=lambda: self.driver),
            _compile_kernel=lambda source, name: types.SimpleNamespace(
                func=ctypes.c_void_p(0xF00D00000001)
            ),
            current_device=lambda: 0,
            device_count=lambda: 1,
            device=lambda device: contextlib.nullcontext(),
            current_stream=lambda device=None: types.SimpleNamespace(
                cuda_stream=0x5EA500000001
            ),
            Event=lambda: event,
            OutOfMemoryError=MemoryError,
        )
        return torch, Tensor

    def read(self, torch, Tensor, launch) -> tuple:
        """300 scalars, two blocks' worth, through Reads as log() reads them."""
        dtypes = ("float32", "float64", "int64")
        scalars = [Tensor(i * 1.5 - 100, dtypes[i % 3]) for i in range(300)]
        reads = gpu_module.Reads(torch, scalars, [], launch=launch)
        self.assertTrue(reads.wait())
        return [value for _, value in reads.scalars()], [
            float(t.array) for t in scalars
        ]

    def test_a_launch_passes_its_kernel_and_parameters(self):
        torch, Tensor = self.torch()
        launch = gpu_module.nvrtc_gather(torch)
        self.assertEqual(ctypes.c_int.in_dll(self.driver, "last_attribute").value, 91)
        read, expected = self.read(torch, Tensor, launch)
        self.assertEqual(read, expected)
        last = self._Launch.in_dll(self.driver, "last")
        self.assertEqual(
            (list(last.grid), list(last.block), last.shared),
            ([2, 1, 1], [256, 1, 1], 0),
        )
        self.assertEqual(
            (last.f, last.stream, last.extra), (0xF00D00000001, 0x5EA500000001, None)
        )

    def test_a_launch_without_a_current_context_makes_the_kernels_current(self):
        # A fresh thread has none: the driver refuses the launch, which makes the context the kernel was built in current and launches again.
        torch, Tensor = self.torch()
        launch = gpu_module.nvrtc_gather(torch)
        result = []
        thread = threading.Thread(
            target=lambda: result.append(self.read(torch, Tensor, launch))
        )
        thread.start()
        thread.join()
        ((read, expected),) = result
        self.assertEqual(read, expected)
        self.assertEqual(ctypes.c_int.in_dll(self.driver, "refused").value, 1)
        self.assertEqual(
            ctypes.c_void_p.in_dll(self.driver, "made_current").value, 0x1000
        )

    def test_pinned_memory_unusable_at_its_host_address_is_refused(self):
        torch, _ = self.torch()
        for name, ctype, value in (
            ("attribute", ctypes.c_int, 0),
            ("offset", ctypes.c_longlong, 4096),
        ):
            with self.subTest(name):
                self.setUp()
                ctype.in_dll(self.driver, name).value = value
                with self.assertRaisesRegex(RuntimeError, "pinned host memory"):
                    gpu_module.nvrtc_gather(torch)


@_REQUIRES_CUDA
class GatherBuildTests(unittest.TestCase):
    def test_the_first_kernel_that_builds_is_used_even_under_sync_debug_mode(self):
        torch.cuda.set_sync_debug_mode("error")
        try:
            launch = gpu_module.gather.__wrapped__(torch, 0)
        finally:
            torch.cuda.set_sync_debug_mode(0)
        builds = []
        for build in (gpu_module.nvrtc_gather, gpu_module.triton_gather):
            try:
                build(torch)
                builds.append(build.__name__)
            except (AttributeError, ImportError):
                pass
        if not builds:
            self.assertIsNone(launch)
        else:
            self.assertTrue(launch.__qualname__.startswith(builds[0]))


def _spill_counted(spool, _point_tuple) -> bool:
    """A _spill_tuple stand-in that writes nothing but counts the record, as the salvage reads its spool's count."""
    spool.count += 1
    return True


class QueueDrainTests(unittest.TestCase):
    class ScriptedQueue:
        def __init__(self, events=()):
            self.events = list(events)
            self.puts = []

        def put(self, item, block=True, timeout=None):
            self.puts.append(item)
            self.events.append(item)

        def get(self, timeout=None):
            del timeout
            if not self.events:
                raise queue.Empty
            event = self.events.pop(0)
            if event is queue.Empty:
                raise queue.Empty
            return event

    @staticmethod
    def _salvage(source, held=(), spool=None, within=30.0):
        """The parent's salvage of source, then held, into spool (by default a stand-in that counts nothing), by within seconds from now; returns what it salvaged."""
        with mock.patch.object(client_module, "_init_pid", os.getpid()):
            return client_module._drain_queue_to_spool(
                mock.Mock(count=0) if spool is None else spool,
                source=source,
                held=list(held),
                hard_deadline=time.monotonic() + within,
            )

    def test_the_salvage_keeps_no_group_alive_through_its_next_wait(self):
        # Its next wait can last until its deadline: the group it consumed last, perhaps a large serialized gallery, must not stay alive through it.
        class Group(list):
            pass

        alive = []

        class Source(self.ScriptedQueue):
            def get(self, timeout=None):
                gc.collect()
                alive.append(consumed() is not None)
                return super().get(timeout)

        group = Group([numeric(1)])
        consumed = weakref.ref(group)
        source = Source([group])
        del group
        with mock.patch.object(client_module, "_spill_tuple", return_value=True):
            self._salvage(source)
        self.assertEqual(alive, [True, False])

    def test_owner_drain_uses_a_fresh_fifo_fence_past_empty_reads(self):
        # One numeric batch carries the text points of a call's captured output as its 7th field.
        text = ("text_ts", "logs/std_out", 9, "out", 1_700_000_000_000)
        before_stale_sentinel = (
            client_module._NUMERIC_TEXT_BATCH_QUEUE_ITEM,
            1,
            1_700_000_000_000,
            ["train/loss", "train/tagged"],
            [1.0, 2.0],
            [None, "3"],
            [text],
        )
        after_stale_sentinel = [numeric(2)]
        source = self.ScriptedQueue(
            [
                queue.Empty,
                queue.Empty,
                before_stale_sentinel,
                None,
                after_stale_sentinel,
            ]
        )

        with mock.patch.object(
            client_module, "_spill_tuple", side_effect=_spill_counted
        ) as spill:
            salvaged = self._salvage(source)

        self.assertEqual(salvaged, 4)
        self.assertEqual(
            [call.args[1] for call in spill.call_args_list],
            [
                numeric(1),
                ("numeric_tagged_ts", "train/tagged", 1, 2.0, "3", 1_700_000_000_000),
                text,
                numeric(2),
            ],
        )
        self.assertEqual(len(source.puts), 1)
        self.assertEqual(source.puts[0][0], "__kymo_owner_drain__")
        self.assertEqual(source.events, [])

    def test_owner_drain_skips_unencodable_points_and_stops_at_a_failed_write(self):
        image = Image(b"\x89PNG\r\n\x1a\n", format="webp")
        gallery = ("cdn_batch", "samples", 1, [image])
        (later,) = client_module._snapshot_rich_queue_items(
            [("cdn_batch", "samples", 5, [image])]
        )
        source = self.ScriptedQueue(
            [[numeric(1), gallery, numeric(2), numeric(3), numeric(4)], later]
        )
        spool = mock.Mock()
        # A disk failure partway through the third record, which the writer cuts off.
        spool.write.side_effect = [None, None, OSError("no space left on device")]
        spool.count = 2
        real_decode = client_module._decode_queue_item
        with (
            mock.patch.object(
                client_module, "_encode_image", side_effect=MemoryError("pressure")
            ),
            mock.patch.object(
                client_module, "_decode_queue_item", side_effect=real_decode
            ) as decode,
            self.assertLogs("kymo", level="WARNING") as logs,
        ):
            salvaged = self._salvage(source, spool=spool)

        # The unencodable gallery wrote nothing, so the next point still spooled; after the failed write nothing was written or even decoded, but the drain still reached its fence. The gallery counts as dropped with the three points the failed write cost.
        self.assertEqual(salvaged, 2)
        self.assertEqual(
            [call.args[0][2] for call in spool.write.call_args_list], [1, 2, 3]
        )
        self.assertEqual(decode.call_count, 1)
        self.assertEqual(source.events, [])
        self.assertIn("dropped 4 point(s)", "\n".join(logs.output))

    def test_owner_drain_counts_the_points_it_cannot_decode_or_encode_as_dropped(self):
        # A rich item that will not unpickle and a gallery that cannot be encoded spool nothing: the salvage's report counts them, with what a failed write cost.
        image = Image(b"\x89PNG\r\n\x1a\n", format="webp")
        undecodable = (client_module._SERIALIZED_RICH_QUEUE_ITEM, b"not a pickle")
        spool = mock.Mock(count=0)
        spool.write.side_effect = lambda record: setattr(
            spool, "count", spool.count + 1
        )
        source = self.ScriptedQueue(
            [undecodable, [("cdn_batch", "samples", 1, [image]), numeric(1)]]
        )
        with (
            mock.patch.object(
                client_module, "_encode_image", side_effect=MemoryError("pressure")
            ),
            self.assertLogs("kymo", level="WARNING") as logs,
        ):
            salvaged = self._salvage(source, spool=spool)

        self.assertEqual(salvaged, 1)
        self.assertIn("parent salvage dropped 2 point(s)", "\n".join(logs.output))

    def test_owner_drain_makes_room_for_its_fence_on_a_full_queue(self):
        # A dead worker can leave a bounded queue (every multiprocessing.Queue on macOS) full: salvage takes groups off until the fence fits, then spools the groups the full queue held back.
        source = _real_queue(self, maxsize=2)
        source.put([numeric(1)])
        source.put([numeric(2)])
        with mock.patch.object(
            client_module, "_spill_tuple", side_effect=_spill_counted
        ) as spill:
            salvaged = self._salvage(source, held=[[numeric(3)]])
        self.assertEqual(salvaged, 3)
        self.assertEqual(
            [call.args[1] for call in spill.call_args_list],
            [numeric(1), numeric(2), numeric(3)],
        )

    def test_owner_drain_without_room_for_its_fence_stops_at_its_deadline(self):
        class Full(self.ScriptedQueue):
            def put(self, item, block=True, timeout=None):
                raise queue.Full

        with (
            mock.patch.object(
                client_module, "_spill_tuple", side_effect=_spill_counted
            ),
            self.assertLogs("kymo", level="WARNING") as logs,
        ):
            salvaged = self._salvage(Full(), held=[[numeric(1)]], within=0.05)

        # The groups held back are spooled even so.
        self.assertEqual(salvaged, 1)
        self.assertIn("no room for its owner fence", "\n".join(logs.output))

    def test_owner_drain_reports_a_missing_fence(self):
        source = self.ScriptedQueue()

        def drop_fence(item, block=True, timeout=None):
            source.puts.append(item)

        with (
            mock.patch.object(source, "put", side_effect=drop_fence),
            self.assertLogs("kymo", level="WARNING") as logs,
        ):
            salvaged = self._salvage(source, within=0.01)

        self.assertEqual(salvaged, 0)
        self.assertEqual(len(source.puts), 1)
        self.assertIn("ended before its owner fence", "\n".join(logs.output))

    def test_non_owner_cannot_publish_a_queue_fence(self):
        source = self.ScriptedQueue()
        with (
            mock.patch.object(client_module, "_init_pid", os.getpid() + 1),
            self.assertRaisesRegex(RuntimeError, "initializing process"),
        ):
            client_module._drain_queue_to_spool(
                object(), source=source, held=[], hard_deadline=math.inf
            )

        self.assertEqual(source.puts, [])

    def test_worker_drain_never_puts_and_stops_at_parent_sentinel(self):
        source = self.ScriptedQueue([[numeric(1)], None])
        code, status = _worker_past_its_deadline(source, points=1)

        self.assertEqual(code, 0)
        self.assertEqual(status.value, 0)
        self.assertEqual(source.puts, [])
        self.assertEqual(source.events, [])

    def test_worker_missing_sentinel_uses_bounded_silence_without_a_fence(self):
        # It spools what it read and exits as from a drain cut short, so that the parent's fenced salvage follows.
        source = self.ScriptedQueue([[numeric(1)]])
        started = time.monotonic()
        with mock.patch.object(client_module, "_QUEUE_DRAIN_SILENCE", 0.01):
            code, status = _worker_past_its_deadline(source, points=1)

        self.assertEqual(code, client_module._WORKER_EXIT_DRAIN_INCOMPLETE)
        self.assertEqual(status.value, 0)
        self.assertEqual(source.puts, [])
        self.assertLess(time.monotonic() - started, 0.2)

    def test_worker_drain_ends_by_its_end_however_busy_the_queue(self):
        # A worker a signal ends must release the queue's reader lock and exit by itself before its alarm: neither items arriving nor the silence wait carry the drain past its spill end.
        class BusyQueue:
            def __init__(self):
                self.timeouts = []

            def get(self, timeout=None):
                self.timeouts.append(timeout)
                time.sleep(0.001)
                return [numeric(1)]

        source, spill_end = BusyQueue(), _QueueStatus()
        started = time.monotonic()
        spill_end.value = started + 0.05
        _, status = _worker_past_its_deadline(source, spill_deadline=spill_end)
        self.assertLess(time.monotonic() - started, 0.5)
        # Each group it read and spilled released its (uncharged) point.
        self.assertLess(status.value, 0)
        self.assertLessEqual(max(source.timeouts), 0.05)

    def test_worker_reads_wait_no_longer_than_asked_and_resume_a_message(self):
        # A message arrives in pieces, and the rest of one whose producer died never comes: each read waits only its timeout, holding the queue's reader lock while it has part of one, and the next goes on from there.
        source = _real_queue(self)
        reads = worker_module._QueueReads(source)
        self.assertIsNotNone(reads.reader)
        # Within a pipe's buffer, so this thread's writes never wait for its reads.
        group = [numeric(index) for index in range(300)]
        frame = pickle.dumps(group, protocol=pickle.DEFAULT_PROTOCOL)
        frame = len(frame).to_bytes(4, "big") + frame
        self.assertLess(len(frame), 16_384)
        writer = source._writer.fileno()
        source._sem.acquire()
        os.write(writer, frame[:1000])
        started = time.monotonic()
        with self.assertRaises(queue.Empty):
            reads.get(0.05)
        self.assertLess(time.monotonic() - started, 0.5)
        self.assertEqual(len(reads.frame), 1000)
        self.assertFalse(source._rlock.acquire(False))
        with self.assertRaises(queue.Empty):
            reads.get()
        os.write(writer, frame[1000:])
        self.assertEqual(reads.get(1.0), group)
        self.assertEqual(reads.frame, bytearray())
        self.assertTrue(source._rlock.acquire(False))
        source._rlock.release()
        # Queue.put's own writes, past a pipe's buffer, read whole.
        large = group * 300
        source.put(large)
        source.put(None)
        self.assertEqual(reads.get(5.0), large)
        self.assertIsNone(reads.get(5.0))
        with self.assertRaises(queue.Empty):
            reads.get(0.01)

    def test_a_read_ends_by_its_timeout_while_a_message_keeps_arriving(self):
        # A pipe that stays readable never makes the read's poll wait, so a read checks its timeout after each part it reads: it stops there, keeping the part of the message it holds and the queue's reader lock.
        source = _real_queue(self)
        reads = worker_module._QueueReads(source)
        body = b"x" * (1 << 20)
        source.put(body)
        self.assertTrue(source._reader.poll(5))
        read = os.read

        # At most 4 KiB a read, so data always waits and the poll never does: only the check after each read ends it.
        def slow_read(descriptor, size):
            time.sleep(0.01)
            return read(descriptor, min(size, 4096))

        started = time.monotonic()
        with (
            mock.patch.object(worker_module.os, "read", slow_read),
            self.assertRaises(queue.Empty),
        ):
            reads.get(0.02)
        self.assertLess(time.monotonic() - started, 0.5)
        self.assertTrue(reads.frame)
        self.assertFalse(source._rlock.acquire(False))
        self.assertEqual(reads.get(5.0), body)
        self.assertTrue(source._rlock.acquire(False))
        source._rlock.release()

    def test_a_read_waits_for_a_message_without_the_reader_lock(self):
        # With no message begun, a read waits for one without the queue's reader lock, which a worker killed as it waits would leave held, the queue unreadable to the parent's salvage; it takes the lock once one arrives.
        source = _real_queue(self)
        reads = worker_module._QueueReads(source)
        read = []
        reader = threading.Thread(target=lambda: read.append(reads.get(5.0)))
        reader.start()
        time.sleep(0.1)
        free = source._rlock.acquire(False)
        if free:
            source._rlock.release()
        source.put([numeric(1)])
        reader.join(5)
        self.assertTrue(free)
        self.assertEqual(read, [[numeric(1)]])
        self.assertTrue(source._rlock.acquire(False))
        source._rlock.release()

    def test_worker_drain_waits_out_a_message_still_arriving_and_its_consumption(self):
        # Silence is no data for the silence interval: a message that keeps arriving, or a group whose consumption (a spill that encodes a gallery) takes longer than the interval, is not.
        class Trickling:
            def __init__(self):
                self.frame = bytearray()
                self.events = [None, [numeric(1)]]

            def get(self, timeout=None):
                if len(self.frame) < 10:
                    time.sleep(0.02)
                    self.frame += b"x"
                    raise queue.Empty
                self.frame = bytearray()
                return self.events.pop()

        consumed = []
        spill_tuple = client_module._spill_tuple

        def slow_spill(spool, point):
            time.sleep(0.1)
            consumed.append(point)
            return spill_tuple(spool, point)

        with (
            mock.patch.object(client_module, "_QUEUE_DRAIN_SILENCE", 0.05),
            mock.patch.object(client_module, "_spill_tuple", slow_spill),
            # The worker reads the queue's frame as Trickling grows it.
            mock.patch.object(worker_module, "_QueueReads", lambda source: source),
        ):
            code, _ = _worker_past_its_deadline(Trickling(), points=1)
        self.assertEqual(code, 0)
        self.assertEqual(consumed, [numeric(1)])

    def test_consumed_parent_sentinel_is_authoritative(self):
        deadline = _QueueStatus()

        class SentinelThenSilence:
            reads = 0

            def get(self, timeout=None):
                # The deadline drain must not read once the sentinel is in.
                self.reads += 1
                if deadline.value:
                    raise AssertionError(f"unexpected get({timeout})")
                if self.reads == 1:
                    return None
                deadline.value = time.monotonic() - 1
                raise queue.Empty

            @staticmethod
            def put(_item, block=True, timeout=None):
                raise AssertionError("worker published a fence")

        source = SentinelThenSilence()
        with tempfile.TemporaryDirectory() as spool_dir:
            worker_module._upload_worker(
                "unused",
                "p",
                "r",
                source,
                _QueueStatus(),
                shutdown_deadline=deadline,
                spool_path=os.path.join(spool_dir, "w.mkspool"),
            )
        self.assertEqual(source.reads, 2)


class ShutdownProofTests(unittest.TestCase):
    def setUp(self):
        # A shutdown leaves these set for the rest of its run, until the next init(): they must not reach later tests.
        patch = mock.patch.multiple(
            client_module,
            _finishing=False,
            _exit_code=None,
            _unproven_points=0,
            _last_capture_step=None,
        )
        patch.start()
        self.addCleanup(patch.stop)

    class _AckingQueue(_ListQueue):
        """A queue whose worker acknowledges each group as it is put, in status."""

        def __init__(self, status):
            super().__init__()
            self.status = status

        def put(self, item, block=True, timeout=None):
            super().put(item)
            if item is not None:
                self.status.value -= client_module._queue_item_size(item)

    def _run_shutdown(
        self,
        *,
        backlog: int = 0,
        poller_quiesced: bool = True,
        target=None,
        status=None,
        poller=None,
        capture=("", ""),
        flush_timeout: float = 0.1,
        process=None,
        run=None,
        server_address="",
    ) -> bool:
        """Shut down against these fakes, or with run, call it under them instead; with a server_address, the run's TerminateRun goes out (_run_control_rpc)."""
        target = target or _ListQueue()
        status = status or _QueueStatus(backlog)
        if poller is None:
            poller = mock.Mock()
            poller.stop.return_value = poller_quiesced
        # The publication probes the buffers before it drains them; the mocked drain decides what was captured, whatever this test process printed.
        capture_patch = mock.patch.multiple(
            capture_module,
            has_output=mock.Mock(return_value=True),
            drain_buffers=mock.Mock(side_effect=capture)
            if callable(capture)
            else mock.Mock(return_value=capture),
        )
        with (
            tempfile.TemporaryDirectory() as spool_dir,
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_init_pid", os.getpid()),
            mock.patch.object(client_module, "_metric_queue", target),
            mock.patch.object(client_module, "_queue_status", status),
            mock.patch.object(client_module, "_upload_spooled", _QueueStatus()),
            mock.patch.object(client_module, "_upload_terminal", _QueueStatus()),
            mock.patch.object(client_module, "_upload_process", process or _Process(0)),
            mock.patch.object(client_module, "_system_poller", poller),
            mock.patch.multiple(
                client_module,
                _shutdown_deadline=_QueueStatus(),
                _spill_deadline=_QueueStatus(),
                _worker_started=types.SimpleNamespace(value=1),
            ),
            mock.patch.object(client_module, "_spool_dir", spool_dir),
            mock.patch.object(client_module, "_server_address", server_address),
            mock.patch.object(client_module, "_project_id", "p"),
            mock.patch.object(client_module, "_run_id", "r"),
            mock.patch.multiple(
                client_module,
                _session_id="session-a",
                _last_capture_step=None,
                # A test's own failure code, else none.
                _upload_failure=client_module._upload_failure or _QueueStatus(),
            ),
            mock.patch.object(client_module, "SpoolWriter"),
            mock.patch.object(client_module, "_drain_queue_to_spool", return_value=0),
            mock.patch.object(client_module.time, "time", return_value=1.234),
            capture_patch,
        ):
            if run is not None:
                return run()
            return client_module._drain_and_shutdown(flush_timeout=flush_timeout)

    def test_shutdown_releases_a_log_call_waiting_for_the_encoder_first(self):
        holding = threading.Event()

        def waiting_log():
            # Stands in for a log() that waits for the encoder while it holds the pending calls.
            with client_module._pending_lock:
                holding.set()
                deadline = time.monotonic() + 5
                while not client_module._finishing and time.monotonic() < deadline:
                    time.sleep(0.001)

        with mock.patch.object(client_module, "_finishing", False):
            waiter = threading.Thread(target=waiting_log, daemon=True)
            waiter.start()
            self.assertTrue(holding.wait(timeout=1))
            started = time.monotonic()
            self._run_shutdown(flush_timeout=2.0)
            # Not the second the pending calls' lock would take to time out.
            self.assertLess(time.monotonic() - started, 0.5)
        waiter.join(timeout=1)
        self.assertFalse(waiter.is_alive())

    def test_a_shutdown_refused_its_flush_timeout_keeps_the_encoder_wait(self):
        with mock.patch.object(client_module, "_finishing", False):
            with self.assertRaises(ValueError):
                self._run_shutdown(flush_timeout="invalid")
            self.assertFalse(client_module._finishing)

    def _encoder_behind(self):
        """Pace a gallery call by a worker that holds the previous two galleries raw, the previous call's among them; return a queue, that worker, which exits on taking its shutdown marker, and the pacing (_patch_encoder_pacing)."""
        _patch_log_client(self)
        pacing = _patch_encoder_pacing(self)
        client_module._galleries_published = 2
        client_module._previous_galleries = 1
        target, worker = _ListQueue(), _Process()
        put = target.put

        def take_marker(item, block=True, timeout=None):
            if item is None:
                worker.exitcode = 0
            put(item, block, timeout)

        target.put = take_marker
        return target, worker, pacing

    def _gallery_call_waiting(self, signum):
        """For a log() of a gallery that waits for the encoder (_encoder_behind), return its queue and worker, and a run for _run_shutdown that makes that call, signals this process 0.1 s into its wait, and returns (the exception log() raised, when the signal was sent, when log() returned)."""
        target, worker, pacing = self._encoder_behind()

        def run():
            sent = []

            def send():
                sent.append(time.monotonic())
                os.kill(os.getpid(), signum)

            timer = threading.Timer(0.1, send)
            # Should the signal wait for the encoder, the encoder catches up, and the test fails rather than hangs.
            encoded = threading.Timer(3, setattr, (pacing.released, "value", 2))
            timer.start()
            encoded.start()
            try:
                client_module.log(
                    {"samples": [Image(np.zeros((2, 2, 3), dtype=np.uint8))]}, step=3
                )
            except BaseException as error:
                raised = error
            else:
                raised = None
            returned = time.monotonic()
            timer.join()
            encoded.cancel()
            return raised, sent[0], returned

        return target, worker, run

    def test_a_ctrl_c_during_the_encoder_wait_hands_the_run_off_at_once(self):
        # log() waits for the encoder outside every held section, so a Ctrl-C there raises at once, from the wait, with nothing of the call published and the gallery call's lock free again. The uncaught interrupt hands the run off in kymo's exception hook: TerminateRun(130), the console back at once, and the worker armed with the handoff's cap, 10 s after the Ctrl-C.
        target, worker, run = self._gallery_call_waiting(signal.SIGINT)
        ended, hooked = [], []
        with (
            self._signal_state(
                _terminate_run=lambda code, timeout, **_: ended.append(code),
                _original_excepthook=lambda *args: hooked.append(args[0]),
                _worker_started=None,
            ),
            _kymo_handles({signal.SIGINT: signal.default_int_handler}),
            mock.patch.dict(os.environ, {"KYMO_SIGNAL_FLUSH_TIMEOUT": "30"}),
        ):

            def interrupted():
                error, sent, returned = run()
                self.assertIsInstance(error, KeyboardInterrupt)
                frames = traceback.extract_tb(error.__traceback__)
                self.assertIn("_await_encoder", [frame.name for frame in frames])
                self.assertLess(returned - sent, 0.5)
                self.assertEqual(target.items, [])
                # The gallery call's lock is free again, and so is the pending calls' lock, which the handoff takes only if it is free.
                locked = []

                def check():
                    for lock in (
                        client_module._gallery_lock,
                        client_module._pending_lock,
                    ):
                        if lock.acquire(blocking=False):
                            locked.append(True)
                            lock.release()

                checker = threading.Thread(target=check)
                checker.start()
                checker.join()
                self.assertEqual(locked, [True, True])
                client_module._excepthook(KeyboardInterrupt, error, error.__traceback__)
                return sent, time.monotonic()

            sent, console = self._run_shutdown(
                target=target, process=worker, run=interrupted
            )
            self.assertLess(console - sent, 1.5)
            self.assertEqual(ended, [130])
            self.assertEqual(hooked, [KeyboardInterrupt])
            self.assertTrue(client_module._handed_off)
            self.assertEqual(worker.terminated, 1)
            self.assertLessEqual(client_module._handoff.value, sent + 10.05)
            self.assertIsNone(target.items[-1])

    def test_a_sigterm_during_the_encoder_wait_is_handled_at_once_within_its_budget(
        self,
    ):
        # A SIGTERM (kymo's own; SIGUSR1 here) in log()'s wait for the encoder is handled there and then, never deferred: kymo's flush runs inside the wait, where the gallery call holds only the gallery call's lock, takes the pending calls' lock, ends the run with the signal's code and exits within the signal budget, the call published by none of it.
        sigterm = signal.SIGUSR1
        target, worker, run = self._gallery_call_waiting(sigterm)
        ended, marked = [], []
        put = target.put

        def mark(item, block=True, timeout=None):
            if item is None:
                marked.append(time.monotonic())
            put(item, block, timeout)

        target.put = mark
        with (
            self._signal_state(
                _terminate_run=lambda code, timeout, **_: ended.append(code),
            ),
            _kymo_handles({sigterm: signal.SIG_DFL}),
            mock.patch.dict(os.environ, {"KYMO_SIGNAL_FLUSH_TIMEOUT": "1"}),
        ):
            error, sent, returned = self._run_shutdown(
                target=target, process=worker, run=run
            )
        self.assertIsInstance(error, SystemExit)
        self.assertEqual(error.code, 128 + sigterm)
        frames = traceback.extract_tb(error.__traceback__)
        self.assertIn("_await_encoder", [frame.name for frame in frames])
        # The flush reached the shutdown marker at once, and the exit came within the budget.
        self.assertLess(marked[0] - sent, 0.5)
        self.assertLess(returned - sent, 1.5)
        self.assertEqual(ended, [128 + sigterm])
        self.assertEqual(target.items, [None])

    def test_shutdown_publishes_captured_tail_before_the_sentinel(self):
        status = _QueueStatus()
        events = []

        class AckingQueue(self._AckingQueue):
            def put(queue_self, item, block=True, timeout=None):
                events.append(item)
                super().put(item)

        target = AckingQueue(status)
        poller = mock.Mock()
        poller.stop.side_effect = lambda **_kwargs: events.append("poller") or True
        complete = self._run_shutdown(
            target=target,
            status=status,
            poller=poller,
            capture=lambda final, timeout=None: (
                events.append(("drain", final)) or ("tail", "error")
            ),
        )

        self.assertTrue(complete)
        self.assertEqual(
            events,
            [
                "poller",
                ("drain", True),
                [
                    ("text_ts", "logs/std_out", 1234, "tail", 1234),
                    ("text_ts", "logs/std_err", 1234, "error", 1234),
                ],
                None,
            ],
        )
        self.assertEqual(status.value, 0)

    # The shutdown's paths a signal can land in, the waits it steps through included.
    @staticmethod
    def _shutdown_codes():
        functions = [
            client_module._drain_and_shutdown,
            client_module._shut_down,
            client_module._complete_handoff,
            client_module._feeder_left,
            client_module._signal_during_shutdown,
            client_module._shut_down_on_signal,
            client_module._disown_worker,
            client_module._exit_now,
            client_module._handoff_cap,
            client_module._await,
            client_module._joiner,
            client_module._put_shutdown_marker,
            client_module._stop_upload_process,
            client_module._terminate_run,
            *(
                member
                for member in vars(client_module._Shutdown).values()
                if inspect.isfunction(member)
            ),
        ]
        codes = {function.__code__ for function in functions}
        # Their lambdas and nested functions too.
        return codes | {
            const
            for code in codes
            for const in code.co_consts
            if isinstance(const, types.CodeType)
        }

    class _Worker:
        """A worker the shutdown waits for a few steps, which a Ctrl-C hands the run (_Shutdown.hand_off)."""

        exitcode = 0

        def __init__(self):
            self.steps = 3

        def is_alive(self):
            return self.steps > 0

        def join(self, timeout=None):
            self.steps -= 1

        def terminate(self):
            pass

        def kill(self):
            self.steps = 0

    def _signal_state(self, ended=None, **patches):
        """Patch kymo.client to a run p/r, its worker running, that no signal or shutdown has touched, which the patch restores even if an exit (here an exception) leaves a shutdown claimed; its TerminateRun codes (_run_control_rpc) go to ended, and patches go on top."""
        codes = [] if ended is None else ended
        return mock.patch.multiple(
            client_module,
            **{
                "_project_id": "p",
                "_run_id": "r",
                "_upload_terminal": _QueueStatus(),
                "_upload_process": _Process(),
                "_shutdown_claim": {},
                "_handoff": _QueueStatus(),
                "_handed_off": False,
                "_exit_code": None,
                "_interrupted_at": None,
                "_worker_interrupted_at": None,
                "_terminated_at": None,
                "_left_control": None,
                "_run_control_rpc": lambda method, request, *_, **__: codes.append(
                    request.exit_code
                ),
                "_log": mock.Mock(),
                **patches,
            },
        )

    # A run whose TerminateRun goes out (_Shutdown.run), but for its flag that the run was deleted.
    _TARGET = ("127.0.0.1:1", "", "p", "r")

    @contextlib.contextmanager
    def _control_processes(self, communicate):
        """Patch kymo.client to _signal_state's run, whose TerminateRun the real _run_control_rpc sends through stand-ins for its control processes, yielded in a list as they start, their pids from 1000: communicate(process, timeout) answers each wait but its reaper's, which returns at once."""
        started = []

        class Control:
            returncode = 0
            killed = False

            def __init__(self, args, **kwargs):
                self.pid = 1000 + len(started)
                self.stdin = self.sent = _ControlStdin()
                started.append(self)

            def communicate(self, input=None, timeout=None):
                return (b"", b"") if timeout is None else communicate(self, timeout)

            def poll(self):
                return None

            def kill(self):
                self.killed = True

        with (
            self._signal_state(
                _run_control_rpc=client_module._run_control_rpc,
                _server_address="127.0.0.1:1",
                _local_installation_uuid="",
            ),
            mock.patch.object(client_module.subprocess, "Popen", Control),
        ):
            yield started

    def test_a_signal_at_any_opcode_of_a_shutdown_never_nests_one(self):
        # A Ctrl-C (chained to Python's handler) or a SIGTERM (kymo's own; SIGUSR1 here, its budget 1s) injected at every opcode of a shutdown's paths: at most one shutdown runs, never one inside another. A signal during it hands it off, or bounds it by the signal budget, and every deadline the shutdown plans once it has seen the bound is no later, the worker's included. Either way the run ends with the signal's code, once, even after the shutdown's own TerminateRun(0) or during it. The signal's own handling, a KeyboardInterrupt or an exit, comes once, after the shutdown has returned. Before the shutdown starts or once it has ended, the signal is handled as if none ran.
        traced = self._shutdown_codes()
        shut_down = client_module._shut_down
        for signum, original, outcome in (
            (signal.SIGINT, signal.default_int_handler, KeyboardInterrupt),
            (signal.SIGUSR1, signal.SIG_DFL, SystemExit),
        ):
            replanned = 0
            for k in itertools.count():
                # A loaded machine can preempt the process between the signal and its handler: an opcode whose bound came late runs again, and a handler that is late for a reason of kymo's is late every time.
                for _ in range(3):
                    runs, active, events, raised, ended = [], [], [], [], []

                    def counted(shutdown):
                        self.assertEqual(active, [], "a shutdown nested in another")
                        runs.append(shutdown)
                        active.append(shutdown)
                        try:
                            return shut_down(shutdown)
                        finally:
                            active.pop()

                    def inject(frame):
                        events.append("signal")
                        sent.append(time.monotonic())
                        signal.raise_signal(signum)

                    sent = []
                    tracer, fired = _opcode_tracer(traced, k, inject)
                    handoff = _QueueStatus()

                    def run():
                        try:
                            with _tracing(tracer):
                                client_module._drain_and_shutdown(flush_timeout=5)
                        except (KeyboardInterrupt, SystemExit) as error:
                            raised.append(type(error))
                        finally:
                            ended.append(time.monotonic())
                            ended.append(client_module._shutdown_deadline.value)

                    with (
                        self._signal_state(
                            events,
                            _pending_lock=threading.RLock(),
                            _shut_down=counted,
                            _handoff=handoff,
                        ),
                        mock.patch.dict(os.environ, {"KYMO_SIGNAL_FLUSH_TIMEOUT": "1"}),
                        _kymo_handles({signum: original}),
                    ):
                        self._run_shutdown(
                            run=run,
                            process=self._Worker(),
                            server_address="127.0.0.1:1",
                        )
                        self.assertEqual(client_module._shutdown_claim, {})
                        handed_off = client_module._handed_off
                    if not (
                        runs
                        and runs[0].signum == signum
                        and signum != signal.SIGINT
                        and runs[0].hard >= sent[0] + 1.05
                    ):
                        break
                state = f"signal {signum!r} at opcode {k}"
                self.assertLessEqual(len(runs), 1, state)
                self.assertEqual(raised, [outcome] if fired else [], state)
                if runs and runs[0].signum == signum:
                    shutdown = runs[0]
                    self.assertEqual(handed_off, signum == signal.SIGINT, state)
                    self.assertIn(
                        [event for event in events if event != "signal"],
                        ([128 + signum], [0, 128 + signum]),
                        state,
                    )
                    if signum == signal.SIGINT:
                        self.assertGreater(handoff.value, 0, state)
                    else:
                        # Its handler runs a moment after it arrives.
                        self.assertLess(shutdown.hard, sent[0] + 1.05, state)
                        if shutdown.planned_hard == shutdown.hard:
                            # A deadline planned once the end had passed is now.
                            limit = max(shutdown.hard, ended[0])
                            planned = [
                                deadline
                                for deadline in (
                                    shutdown.pending_by,
                                    shutdown.deliver_by,
                                    shutdown.graceful,
                                    shutdown.stop_by,
                                    shutdown.pre_cleanup,
                                    ended[1] or math.inf,
                                )
                                if deadline != math.inf
                            ]
                            replanned += bool(planned)
                            for deadline in planned:
                                self.assertLessEqual(deadline, limit, state)
                if not fired:
                    break
            self.assertGreater(k, 300)
            if signum != signal.SIGINT:
                self.assertGreater(replanned, 100)

    def test_a_further_signal_during_a_bounded_or_handed_off_shutdown_exits_at_once(
        self,
    ):
        # Injected at every opcode of a shutdown that a signal started, bounded by the signal budget (SIGUSR1 here) or handed off (a Ctrl-C): a further Ctrl-C exits at once with 130, after handing the worker what is left of the shutdown and ending the run with 130, once per code, never starting another. A Ctrl-C landing in the shutdown's own TerminateRun neither sends another inside it nor cuts it short: the exit follows it. Before the shutdown starts or once it has ended, the Ctrl-C is handled as if none ran.
        traced = self._shutdown_codes()

        class Exited(BaseException):
            pass

        for first in (signal.SIGUSR1, signal.SIGINT):
            for k in itertools.count():
                exits, raised, ended = [], [], []

                def exit_now(code):
                    exits.append(code)
                    raise Exited

                tracer, fired = _opcode_tracer(
                    traced, k, lambda frame: signal.raise_signal(signal.SIGINT)
                )
                handoff = _QueueStatus()

                def run():
                    try:
                        with _tracing(tracer):
                            client_module._drain_and_shutdown(
                                flush_timeout=5, signum=first
                            )
                    except (KeyboardInterrupt, Exited) as error:
                        raised.append(type(error))

                with (
                    self._signal_state(
                        ended, _pending_lock=threading.RLock(), _handoff=handoff
                    ),
                    mock.patch.object(client_module.os, "_exit", side_effect=exit_now),
                    _kymo_handles({signal.SIGINT: signal.default_int_handler}),
                ):
                    self._run_shutdown(
                        run=run, process=self._Worker(), server_address="127.0.0.1:1"
                    )
                    # The process would be gone after an exit.
                    left = client_module._shutdown_claim.get("shutdown")
                state = f"after {first!r}, a Ctrl-C at opcode {k}"
                self.assertTrue(exits or left is None, state)
                if exits:
                    self.assertEqual((exits, raised), ([130], [Exited]), state)
                    # The worker was handed the rest of the delivery, by the Ctrl-C that started the shutdown or before the exit, and the parent ended the run with 130, after the first signal's code if that had gone out.
                    self.assertGreater(handoff.value, 0, state)
                    self.assertIn(ended, ([130], [128 + first, 130]), state)
                else:
                    self.assertEqual(
                        raised, [KeyboardInterrupt] if fired else [], state
                    )
                    self.assertIn(ended, ([], [128 + first]), state)
                if not fired:
                    break
            self.assertGreater(k, 100)

    def test_a_signal_during_the_shutdown_is_handled_once_it_returns(self):
        events = []

        class SignalingQueue(_ListQueue):
            def put(queue_self, item, block=True, timeout=None):
                if item is None:
                    # It bounds the shutdown at once, and its handler runs once the shutdown returns, never inside it.
                    signal.raise_signal(signal.SIGUSR1)
                    events.append("marker")
                queue_self.items.append(item)

        def handler(signum, frame):
            events.append(signum)

        with _kymo_handles({signal.SIGUSR1: handler}):
            self._run_shutdown(target=SignalingQueue())
        self.assertEqual(events, ["marker", signal.SIGUSR1])

    def test_a_ctrl_c_as_the_shutdown_ends_never_loses_a_signal_that_arrived(self):
        # A SIGTERM arrives during a plain shutdown, its handler (installed before init()) waiting for the shutdown to return, and a Ctrl-C, Python's handler, lands at each opcode of _drain_and_shutdown: the SIGTERM's handler runs unless the Ctrl-C exits at once.
        class Exited(BaseException):
            pass

        def shut_down(shutdown):
            client_module._signal_handler(signal.SIGTERM, None)
            events.append("arrived")
            return True

        def exit_now(code):
            events.append("exit")
            raise Exited

        for k in itertools.count():
            events = []
            tracer, fired = _opcode_tracer(
                {client_module._drain_and_shutdown.__code__},
                k,
                lambda frame: client_module._signal_handler(signal.SIGINT, frame),
            )
            with (
                self._signal_state(
                    _is_initialized=True,
                    _init_pid=os.getpid(),
                    _original_signals={
                        signal.SIGTERM: lambda signum, frame: events.append(signum),
                        signal.SIGINT: signal.default_int_handler,
                    },
                    _shut_down=shut_down,
                    _exit_now=exit_now,
                    _complete_handoff=lambda shutdown: None,
                    _finishing=False,
                ),
                contextlib.suppress(KeyboardInterrupt, Exited),
                _tracing(tracer),
            ):
                client_module._drain_and_shutdown(flush_timeout=1.0)
            client_module._deferred_signals.clear()
            if "arrived" in events and "exit" not in events:
                self.assertIn(signal.SIGTERM, events, f"a Ctrl-C at opcode {k}")
            if not fired:
                break

    def test_concurrent_shutdowns_claim_one(self):
        # Two threads' finish() at once, each past its look for a running shutdown before either has made one (where both ran a shutdown): one claims it, and the other waits for it and returns its result.
        made, waiting, runs, results = threading.Barrier(2), threading.Event(), [], []
        real_shutdown, real_await = client_module._Shutdown, client_module._await

        def make(*args, **kwargs):
            shutdown = real_shutdown(*args, **kwargs)
            made.wait(5)
            return shutdown

        def await_(wait, deadline):
            waiting.set()
            return real_await(wait, deadline)

        def shut_down(shutdown):
            runs.append(shutdown)
            # Until the other caller waits for this one.
            waiting.wait(5)
            return True

        def run():
            callers = [
                threading.Thread(
                    target=lambda: results.append(
                        client_module._drain_and_shutdown(flush_timeout=1)
                    )
                )
                for _ in range(2)
            ]
            for caller in callers:
                caller.start()
            for caller in callers:
                caller.join(5)

        with mock.patch.multiple(
            client_module,
            _shutdown_claim={},
            _Shutdown=make,
            _await=await_,
            _shut_down=shut_down,
        ):
            self._run_shutdown(run=run)
            self.assertEqual(client_module._shutdown_claim, {})
        self.assertEqual(len(runs), 1)
        self.assertEqual(results, [True, True])

    def test_a_signal_finds_another_threads_shutdown_once_it_is_claimed(self):
        # Another thread's finish() claims the shutdown, and a SIGTERM (kymo's own) is handled on the main thread at each opcode of that claim: once the claim is made, the signal bounds that shutdown (_signal_during_shutdown), where kymo's own flush would wait for its whole end.
        claimed = 0
        for k in itertools.count():
            paused, resume, during, flushed = (
                threading.Event(),
                threading.Event(),
                [],
                [],
            )
            tracer, fired = _opcode_tracer(
                {client_module._claim_shutdown.__code__},
                k,
                lambda frame: paused.set() or resume.wait(5),
            )
            made = []

            def claim():
                made.append(client_module._Shutdown(60.0))
                try:
                    with _tracing(tracer):
                        client_module._claim_shutdown(made[0])
                finally:
                    paused.set()

            with self._signal_state(
                _is_initialized=True,
                _original_signals={},
                _signal_during_shutdown=lambda shutdown, signum: during.append(signum),
                _drain_and_shutdown=lambda **kwargs: flushed.append(kwargs),
            ):
                thread = threading.Thread(target=claim)
                thread.start()
                self.assertTrue(paused.wait(5))
                seen = bool(client_module._shutdown_claim)
                with self.assertRaises(SystemExit):
                    client_module._handle_signal(signal.SIGUSR1, None)
                resume.set()
                thread.join(5)
                client_module._release_shutdown(made[0])
            state = f"a signal at opcode {k} of the claim"
            claimed += seen
            self.assertEqual(
                (during, bool(flushed)),
                ([signal.SIGUSR1], False) if seen else ([], True),
                state,
            )
            if not fired:
                break
        self.assertGreater(claimed, 1)

    def test_a_ctrl_c_kymo_never_saw_hands_another_threads_shutdown_off(self):
        # finish() runs on another thread when a SIGINT handler the program installed after init() keeps a Ctrl-C from kymo's: its KeyboardInterrupt reaches the exception hook, or the program exits after the Ctrl-C the worker noted. Either hands that thread's shutdown off, as kymo's handler would, the run ending with 130, and waits for the handoff alone, not for that shutdown's flush.
        class Running(_Process):
            # The worker the other thread's shutdown waits for.
            def join(self, timeout=None):
                if timeout is not None:
                    waiting.set()
                    time.sleep(timeout)

        for mode in ("exception hook", "exit"):
            ended, results, waiting = [], [], threading.Event()

            def run(mode=mode):
                finishing = threading.Thread(
                    target=lambda: results.append(
                        client_module._drain_and_shutdown(flush_timeout=5.0)
                    )
                )
                finishing.start()
                self.assertTrue(waiting.wait(5))
                began = time.monotonic()
                if mode == "exit":
                    client_module._worker_interrupted_at.value = began
                    client_module._exit_wrapper(1)
                else:
                    error = KeyboardInterrupt()
                    client_module._excepthook(KeyboardInterrupt, error, None)
                took = time.monotonic() - began
                finishing.join(5)
                return took

            with (
                self.subTest(mode=mode),
                self._signal_state(
                    ended,
                    _worker_interrupted_at=_QueueStatus(0.0),
                    _original_excepthook=mock.Mock(),
                    _original_sys_exit=mock.Mock(),
                ),
            ):
                took = self._run_shutdown(
                    run=run, process=Running(), server_address="127.0.0.1:1"
                )
                self.assertTrue(client_module._handed_off)
            self.assertLess(took, 1.0)
            self.assertEqual((ended, results), ([130], [False]))

    def test_a_handoff_kills_a_worker_that_has_not_started_and_spools_its_groups(
        self,
    ):
        # Under spawn the worker imports __main__ before it reads the queue, which can take seconds: a Ctrl-C hands off to a worker that holds nothing, and that would outlive the handoff's cap, or die opening the semaphores the program's exit unlinks, delivering and spooling none of it. The handoff kills it, holding the queue's reader lock, and spools the queue's groups and those held back from the parent, and says so; so it does for a worker that has exited, unless the run was deleted or the shutdown salvaged its queue already. A worker that has started, or is reading, keeps the run. Whether the worker runs is checked before the thread that reaps it starts: while a thread joins a process that has exited, multiprocessing's is_alive() can read it as running (the join reaps it before it records the exit code), so a worker that exits as the handoff's TerminateRun goes out (an OOM kill, say) would have its queue closed rather than spooled.
        salvaged = threading.Thread(target=lambda: None)
        salvaged.start()
        salvaged.join()

        def shut_down_after_its_salvage(shutdown):
            shutdown.stopped, shutdown.salvage = True, (salvaged, None, None)
            raise client_module._HandedOff

        class Joined(_Process):
            joining = threading.Event()

            def join(self, timeout=None):
                self.joining.set()

            def is_alive(self):
                return self.joining.is_set() or self.exitcode is None

        def exits_during_the_send(*args, **kwargs):
            worker.exitcode = 1

        for state in (
            "unstarted",
            "started",
            "reading",
            "exited",
            "exits during the send",
            "deleted",
            "salvaged",
        ):
            source = _real_queue(self)
            exited = state in ("exited", "deleted", "salvaged")
            worker = (Joined if state == "exits during the send" else _Process)(
                exitcode=1 if exited else None
            )
            started = types.SimpleNamespace(value=int(state != "unstarted"))
            status = _QueueStatus(3)
            with (
                self.subTest(state=state),
                tempfile.TemporaryDirectory() as spool_dir,
                mock.patch.multiple(
                    client_module,
                    _is_initialized=True,
                    _init_pid=os.getpid(),
                    _metric_queue=source,
                    _queue_status=status,
                    _upload_spooled=_QueueStatus(),
                    _upload_terminal=_QueueStatus(int(state == "deleted")),
                    _upload_process=worker,
                    _worker_started=started,
                    _system_poller=None,
                    _shutdown_deadline=_QueueStatus(),
                    _handoff=_QueueStatus(),
                    _handed_off=False,
                    _exit_code=None,
                    _interrupted_at=None,
                    _shutdown_claim={},
                    _spool_dir=spool_dir,
                    _server_address="",
                    _project_id="p",
                    _run_id="r",
                    _session_id="session-a",
                    _pending=deque(),
                    _pending_incomplete=False,
                    _held_back=deque([[numeric(3)]]),
                    _uncharged=deque([[numeric(4)]]),
                    # The handoff's own publication is best effort: here it puts nothing, as on a full queue.
                    _publish_queue_items=lambda *args, **kwargs: None,
                    # Room for a busy runner, which can take longer than the 0.2 s grace to spool four points; the salvage's join returns as soon as its thread ends.
                    _HANDOFF_FEEDER_GRACE=5.0,
                    **(
                        {"_terminate_run": exits_during_the_send}
                        if state == "exits during the send"
                        else {}
                    ),
                ),
                mock.patch.object(multiprocessing.process, "_children", set()),
                mock.patch.object(
                    client_module,
                    "_shut_down",
                    shut_down_after_its_salvage
                    if state == "salvaged"
                    else client_module._shut_down,
                ),
                self.assertLogs("kymo", level="WARNING") as logs,
            ):
                source.put([numeric(1)])
                source.put([numeric(2)])
                if state == "reading":
                    self.assertTrue(source._rlock.acquire(False))
                try:
                    self.assertFalse(
                        client_module._drain_and_shutdown(signum=signal.SIGINT)
                    )
                finally:
                    if state == "reading":
                        source._rlock.release()
                spooled = [
                    record[2]
                    for path in spool_module.run_spool_files(
                        "p", "r", spool_dir, session="session-a"
                    )
                    for record in spool_module.read_spool(path)[1]
                ]
            output = "\n".join(logs.output)
            if state in ("unstarted", "exited", "exits during the send"):
                self.assertEqual(worker.killed, int(state == "unstarted"))
                self.assertEqual(spooled, [1, 2, 3, 4])
                self.assertIn(
                    "had not started; spooled 4 of its 4 point(s)"
                    if state == "unstarted"
                    else "had exited (exitcode 1); spooled 4 of its 4 point(s)",
                    output,
                )
                self.assertIn("python -m kymo.sync", output)
                if state == "exits during the send":
                    self.assertTrue(worker.joining.wait(5))
            else:
                self.assertEqual((worker.killed, spooled), (0, []))
                # The groups held back that the handoff could not put are dropped, and counted.
                self.assertIn("dropped 2 point(s)", output)

    def test_an_unstarted_workers_spool_cuts_off_a_record_written_past_its_deadline(
        self,
    ):
        # The process may exit as soon as the handoff's spooling for a worker that never started is over: its writer cuts off whole a record still written at the deadline, here a large text point, rather than let the exit tear it partway, which costs the whole file. The point is reported, not spooled.
        source = _real_queue(self)
        with (
            tempfile.TemporaryDirectory() as spool_dir,
            mock.patch.multiple(
                client_module,
                _init_pid=os.getpid(),
                _metric_queue=source,
                _queue_status=_QueueStatus(1),
                _spool_dir=spool_dir,
                _server_address="",
                _project_id="p",
                _run_id="r",
                _session_id="session-a",
                _held_back=deque(
                    [[("text_ts", "logs/std_out", 1, "x" * (4 << 20), 1)]]
                ),
                _uncharged=deque(),
                _HANDOFF_FEEDER_GRACE=0.0,
            ),
            self.assertLogs("kymo", level="WARNING") as logs,
        ):
            client_module._spool_for_stopped_worker("had not started")
            spooled = [
                record
                for path in spool_module.run_spool_files(
                    "p", "r", spool_dir, session="session-a"
                )
                for record in spool_module.read_spool(path)[1]
            ]
        self.assertEqual(spooled, [])
        self.assertIn("spooled 0 of its 1 point(s)", "\n".join(logs.output))

    def test_a_handed_off_runs_end_is_never_killed(self):
        # The control process that sends a handed-off run's TerminateRun can take much of a second to start: the parent waits 1 s for it at most, then leaves it to finish by itself, by its RPC's deadline, never killing it, which would leave the run RUNNING.
        class SlowControl:
            killed = False
            reaped = threading.Event()

            def __init__(self, args, **kwargs):
                self.returncode = None
                self.calls = []
                self.stdin = _ControlStdin()

            def communicate(self, input=None, timeout=None):
                self.calls.append(timeout)
                if timeout is not None:
                    raise subprocess.TimeoutExpired("kymo._control_rpc", timeout)
                SlowControl.reaped.set()
                return b"", b""

            def kill(self):
                SlowControl.killed = True

        payloads = []
        with (
            mock.patch.object(client_module.subprocess, "Popen", SlowControl),
            mock.patch.object(client_module, "_left_control", None),
            mock.patch.object(
                client_module.json,
                "dumps",
                side_effect=lambda payload, **kwargs: payloads.append(payload) or "{}",
            ),
            self.assertLogs("kymo", level="WARNING") as logs,
        ):
            client_module._terminate_run(
                130, 9.0, run=(*self._TARGET, _QueueStatus()), wait=1.0
            )
            self.assertTrue(SlowControl.reaped.wait(5))
        self.assertFalse(SlowControl.killed)
        self.assertEqual(payloads[0]["timeout"], 5.0)
        self.assertIn("TerminateRun is still on its way", logs.output[-1])

    def test_a_ctrl_c_during_the_runs_end_leaves_it_and_sends_130_after_it(self):
        # A Ctrl-C during a plain shutdown's TerminateRun(0), which a slow server holds, hands the run off: the parent stops waiting for that send within a step, leaving its control process to finish by itself, never killed; and the TerminateRun(130) that follows sends once that one has ended, so the server takes 130 last.
        def communicate(process, timeout):
            if process.sent.request()[0] == 0 and not client_module._handed_off:
                client_module._signal_during_shutdown(shutdown, signal.SIGINT)
            raise subprocess.TimeoutExpired("kymo control", timeout)

        with self._control_processes(communicate) as started:
            shutdown = client_module._Shutdown(5.0)
            began = time.monotonic()
            shutdown.end_run()
            took = time.monotonic() - began
            self.assertTrue(client_module._handed_off)
        self.assertLess(took, client_module._HANDOFF_TERMINATE_TIMEOUT + 1.0)
        self.assertEqual(
            [process.sent.request() for process in started],
            [(0, None), (130, started[0].pid)],
        )
        self.assertEqual(shutdown.ended, 130)
        self.assertFalse(any(process.killed for process in started))

    def test_a_raise_out_of_the_runs_end_leaves_the_next_send_after_it(self):
        # A handler kymo does not own raises as finish()'s TerminateRun(0) waits for a slow server, leaving the 0's control process running: a SIGINT handler the program installed after init() aborts that shutdown, and the TerminateRun(130) of the shutdown that follows (the exception hook's) sends once that one has ended, so the server takes 130 last; so does the same shutdown's next send, after an Exception (a per-step timeout's) the send swallows or a KeyboardInterrupt.
        for error, same in (
            (KeyboardInterrupt, False),
            (KeyboardInterrupt, True),
            (TimeoutError, True),
        ):

            def communicate(process, timeout, error=error):
                if process is started[0]:
                    raise error
                return b"", b""

            with (
                self.subTest(error=error.__name__, same_shutdown=same),
                self._control_processes(communicate) as started,
            ):
                shutdown = client_module._Shutdown(5.0)
                with contextlib.suppress(KeyboardInterrupt):
                    shutdown.end_run()
                client_module._exit_code = 130
                if not same:
                    shutdown = client_module._Shutdown(0.0, signal.SIGINT)
                shutdown.end_run()
                self.assertEqual(
                    [process.sent.request() for process in started],
                    [(0, None), (130, started[0].pid)],
                )

    def test_a_send_that_never_starts_keeps_the_one_left_running_to_wait_for(self):
        # A SIGTERM supersedes the shutdown's TerminateRun(0) as its end passes, leaving the 0's control process running; the 143 that follows has no time left and starts no process. The handoff's 130 after it still sends once the 0's process has ended, so the server takes 130 last.
        def communicate(process, timeout):
            if process is started[0]:
                client_module._exit_code = 143
                shutdown.hard = time.monotonic() - 1
                raise subprocess.TimeoutExpired("kymo control", timeout)
            return b"", b""

        with self._control_processes(communicate) as started:
            shutdown = client_module._Shutdown(5.0)
            shutdown.end_run()
            client_module._exit_code = 130
            shutdown.hand_off()
            shutdown.end_run()
        self.assertEqual(
            [process.sent.request() for process in started],
            [(0, None), (130, started[0].pid)],
        )

    def test_a_signal_during_a_slow_ensure_ends_the_run_at_once(self):
        # A local run's endpoint ensure before the shutdown's own TerminateRun(0), slow to answer (here a launcher that sleeps 30 s), and a Ctrl-C or SIGTERM during it: the ensure gives up within a step of the code being superseded, the 0 never goes out, and the signal's code follows at once.
        installation = endpoint_payload()["installation_uuid"]
        for signum, original, code in (
            (signal.SIGINT, signal.default_int_handler, 130),
            (signal.SIGTERM, signal.SIG_DFL, 143),
        ):
            sent = []
            with (
                self.subTest(signal=signum),
                tempfile.TemporaryDirectory() as directory,
                self._signal_state(
                    _run_control_rpc=lambda method, request, *_, **__: sent.append(
                        (request.exit_code, time.monotonic())
                    ),
                    _server_address="unix:/local/grpc.sock",
                    _local_installation_uuid=installation,
                ),
                mock.patch.dict(os.environ, {"KYMO_SIGNAL_FLUSH_TIMEOUT": "3"}),
                _kymo_handles({signum: original}),
            ):
                launcher, slept = (
                    os.path.join(directory, name) for name in ("kymo", "slept")
                )
                with open(launcher, "w") as file:
                    file.write(
                        f"#!{sys.executable}\n"
                        "import os, time\n"
                        f"if not os.path.exists({slept!r}):\n"
                        f"    os.mkdir({slept!r})\n"
                        "    time.sleep(30)\n"
                        f"print({json.dumps(endpoint_payload())!r})\n"
                    )
                os.chmod(launcher, 0o700)
                shutdown = client_module._Shutdown(5.0)
                client_module._claim_shutdown(shutdown)
                signalled = []

                def signal_once_asleep():
                    # Before then, the signal's own ensure would start the sleep.
                    _wait_for(lambda: os.path.exists(slept), timeout=10)
                    signalled.append(time.monotonic())
                    os.kill(os.getpid(), signum)

                signaller = threading.Thread(target=signal_once_asleep)
                with mock.patch(
                    "kymo._local_runtime._launcher_path", return_value=launcher
                ):
                    signaller.start()
                    shutdown.end_run()
                    returned = time.monotonic()
                signaller.join()
                self.assertEqual([sent_code for sent_code, _ in sent], [code])
                # Its own ensure starts the launcher again, which answers at once.
                self.assertLess(sent[0][1] - signalled[0], 1.0)
                self.assertLess(returned - signalled[0], 1.0)
                self.assertEqual(shutdown.ended, code)

    def test_a_ctrl_c_during_a_sigterms_end_of_the_run_leaves_it(self):
        # A SIGTERM once the shutdown's own TerminateRun(0) has gone out sends 143 in a held round, which defers a Ctrl-C during it: the send leaves the 143's control process within a step of the Ctrl-C, which hands the run off: the 130 goes out behind it, and the exit follows.
        class Exited(BaseException):
            pass

        def communicate(process, timeout):
            if process.sent.request()[0] != 143:
                # The 130's.
                return b"", b""
            if not interrupted:
                interrupted.append(time.monotonic())
                client_module._signal_handler(signal.SIGINT, None)
            raise subprocess.TimeoutExpired("kymo control", timeout)

        interrupted, exits = [], []

        def exit_now(code):
            exits.append((code, time.monotonic()))
            raise Exited

        with (
            self._control_processes(communicate) as started,
            mock.patch.dict(os.environ, {"KYMO_SIGNAL_FLUSH_TIMEOUT": "2"}),
            mock.patch.object(client_module.os, "_exit", side_effect=exit_now),
            _kymo_handles(
                {
                    signal.SIGINT: signal.default_int_handler,
                    signal.SIGTERM: signal.SIG_DFL,
                }
            ),
        ):
            shutdown = client_module._Shutdown(30.0)
            shutdown.ended = 0
            client_module._claim_shutdown(shutdown)
            with self.assertRaises(Exited):
                client_module._signal_handler(signal.SIGTERM, None)
        self.assertEqual(
            [process.sent.request() for process in started],
            [(143, None), (130, started[0].pid)],
        )
        self.assertEqual(exits[0][0], 130)
        # The console comes back within a step or so of the Ctrl-C, not at the 143's RPC deadline 2 s later.
        self.assertLess(exits[0][1] - interrupted[0], 0.5)

    def test_a_ctrl_c_once_the_shutdown_stopped_the_worker_publishes_nothing(self):
        # A Ctrl-C during the shutdown's own TerminateRun(0), once the worker has stopped and the shutdown has dealt with its queue: the handoff only ends the run with 130, publishing nothing more to a queue nothing reads and spooling nothing again.
        codes, published = [], []

        def control_rpc(method, request, *_, **__):
            codes.append(request.exit_code)
            if request.exit_code == 0:
                client_module._signal_handler(signal.SIGINT, None)

        def run():
            with self.assertRaises(KeyboardInterrupt):
                client_module._drain_and_shutdown(flush_timeout=1)
            published.extend(client_module._metric_queue.items)

        with (
            self._signal_state(codes, _run_control_rpc=control_rpc),
            _kymo_handles({signal.SIGINT: signal.default_int_handler}),
        ):
            self._run_shutdown(run=run, server_address="127.0.0.1:1")
            warnings = " ".join(
                str(call.args) for call in client_module._log.warning.call_args_list
            )
        self.assertEqual(codes, [0, 130])
        self.assertEqual(published, [None])
        self.assertNotIn("spooled", warnings)
        self.assertNotIn("fence", warnings)

    def test_a_handoff_warns_where_the_program_is_its_namespaces_pid_1(self):
        # The trainer's exit, as PID 1 of its namespace (docker run -it, kubectl run -it), kills the upload worker at once: the handoff says so once the run's end has gone out, and changes nothing else.
        for pid in (1, 4321):
            with (
                self.subTest(pid=pid),
                self._signal_state(_terminate_run=lambda *_, **__: None),
                mock.patch.object(client_module.os, "getpid", return_value=pid),
            ):
                shutdown = client_module._Shutdown(0.0, signal.SIGINT)
                shutdown.hand_off()
                shutdown.end_run()
                warnings = [
                    call.args[0] for call in client_module._log.warning.call_args_list
                ]
            self.assertEqual(
                any("PID 1" in warning for warning in warnings), pid == 1, warnings
            )

    def test_the_runs_end_goes_out_with_the_shutdown_signals_blocked(self):
        # A group Ctrl-C, SIGTERM or SIGUSR1 also reaches the control process that sends TerminateRun: it starts with the group's signals blocked, so it never dies mid-send, and the caller's mask comes back once it has started, so a Ctrl-C during the wait for it reaches kymo's handler, which can hand the run off. A local run's endpoint ensure before it runs in a session of its own instead, which no group signal reaches, with the mask unblocked, since a local stack it starts would keep a blocked one.
        before = signal.pthread_sigmask(signal.SIG_BLOCK, [])
        spawned, waited, ensured = [], [], []

        class Control:
            returncode = 0

            def __init__(self, args, **kwargs):
                spawned.append(signal.pthread_sigmask(signal.SIG_BLOCK, []))
                self.stdin = _ControlStdin()

            def communicate(self, input=None, timeout=None):
                waited.append(signal.pthread_sigmask(signal.SIG_BLOCK, []))
                return b"", b""

        def ensure(**kwargs):
            ensured.append(
                (kwargs["own_session"], signal.pthread_sigmask(signal.SIG_BLOCK, []))
            )

        with (
            mock.patch.object(client_module.subprocess, "Popen", Control),
            mock.patch.object(client_module, "_left_control", None),
            mock.patch("kymo._local_runtime.ensure_local_endpoint", ensure),
        ):
            client_module._terminate_run(130, 1, run=(*self._TARGET, _QueueStatus()))
            client_module._terminate_run(
                130, 1, run=("", "installation", "p", "r", _QueueStatus())
            )
        group = {*client_module._TERMINATION_SIGNALS, *client_module._GROUP_SIGNALS}
        self.assertEqual(len(spawned), 2)
        for mask in spawned:
            self.assertLessEqual(group, mask)
        self.assertEqual(waited, [before, before])
        self.assertEqual(ensured, [(True, before)])
        self.assertEqual(signal.pthread_sigmask(signal.SIG_BLOCK, []), before)

    def test_a_handler_raising_as_the_runs_end_goes_out_leaves_the_signals_unblocked(
        self,
    ):
        # A handler of a signal kymo does not own (a per-step timeout, say) that raises as the shutdown signals are blocked for the control process leaves the main thread's mask as it was; and one that raises once the control process exists, as the mask comes back, kills and reaps it before its request is written.
        controls = []

        class Control:
            returncode = 0

            def __init__(self, args, **kwargs):
                self.stdin = self.sent = _ControlStdin()
                self.killed = self.reaped = False
                controls.append(self)

            def communicate(self, input=None, timeout=None):
                return b"", b""

            def kill(self):
                self.killed = True

            def __enter__(self):
                return self

            def __exit__(self, *exc_info):
                self.reaped = True

        with (
            mock.patch.object(client_module.subprocess, "Popen", Control),
            mock.patch.object(client_module, "_left_control", None),
        ):
            _assert_a_raise_after_any_call_keeps_the_mask(
                self,
                (client_module._with_group_signals_blocked,),
                lambda: client_module._terminate_run(
                    130, 1.0, run=(*self._TARGET, _QueueStatus())
                ),
                4,
            )
        # Each got its request, or was killed and reaped unwritten: twice, the raise as the spawn returns and the one as the mask comes back.
        ends = [(bool(c.sent.written), c.killed, c.reaped) for c in controls]
        self.assertLessEqual(set(ends), {(True, False, False), (False, True, True)})
        self.assertEqual(ends.count((False, True, True)), 2)

    def test_the_run_ends_before_the_salvage_and_the_spool_listing(self):
        # Once the worker is gone the run ends, so a salvage or spool listing that takes what is left of a bounded shutdown never leaves TerminateRun without time: the server takes points after a run has ended.
        order = []
        with (
            self._signal_state(
                _run_control_rpc=lambda method, request, *_, **__: order.append(
                    f"TerminateRun({request.exit_code})"
                )
            ),
            mock.patch.object(
                client_module,
                "_salvage_queue_before",
                side_effect=lambda deadline, held: order.append("salvage") or True,
            ),
            mock.patch.object(
                client_module,
                "_resolve_run_spools",
                side_effect=lambda **kwargs: order.append("spool listing") or False,
            ),
        ):
            self._run_shutdown(
                status=_QueueStatus(1),
                process=_Process(1),
                flush_timeout=1,
                server_address="127.0.0.1:1",
            )
        self.assertEqual(order, ["TerminateRun(0)", "salvage", "spool listing"])

    def test_a_signal_once_the_shutdown_forgot_its_run_still_ends_it_through_its_endpoint(
        self,
    ):
        # The shutdown forgets the run's local installation before it releases its claim; a Ctrl-C then still ends the run with 130 through the run's authenticated endpoint, which the shutdown kept.
        endpoint, sends = object(), []

        def control_rpc(method, request, response_type, **kwargs):
            sends.append((request.exit_code, kwargs["local_endpoint"] is endpoint))

        take_held_back, signalled = client_module._take_held_back, []

        def take_after_the_run_is_forgotten(*args):
            if not client_module._local_installation_uuid and not signalled:
                signalled.append(True)
                client_module._signal_handler(signal.SIGINT, None)
            return take_held_back(*args)

        def run():
            with mock.patch.multiple(
                client_module,
                _server_address="unix:/local/grpc.sock",
                _local_installation_uuid="installation",
            ):
                with self.assertRaises(KeyboardInterrupt):
                    client_module._drain_and_shutdown(flush_timeout=1)

        with (
            self._signal_state(_run_control_rpc=control_rpc),
            mock.patch.object(
                client_module,
                "_take_held_back",
                side_effect=take_after_the_run_is_forgotten,
            ),
            mock.patch(
                "kymo._local_runtime.ensure_local_endpoint", return_value=endpoint
            ),
            _kymo_handles({signal.SIGINT: signal.default_int_handler}),
        ):
            self._run_shutdown(run=run)
        self.assertTrue(signalled)
        self.assertEqual(sends, [(0, True), (130, True)])

    def test_the_atexit_flush_calls_no_handler_for_a_signal_deferred_as_it_ends(self):
        # A SIGTERM (SIGUSR1 here) deferred as the atexit flush releases its claim is dropped with those that interrupted the flush: the process keeps its exit status, and no handler installed before init() runs after the program finished.
        handler = mock.Mock()
        release = client_module._release_shutdown

        def deferred_as_it_releases(shutdown, arrived=()):
            client_module._deferred_signals.append([signal.SIGUSR1, None])
            release(shutdown, arrived)

        with (
            self._signal_state(),
            mock.patch.object(client_module, "_deferred_signals", []),
            mock.patch.object(
                client_module, "_release_shutdown", side_effect=deferred_as_it_releases
            ),
            _kymo_handles({signal.SIGUSR1: handler}),
        ):
            self._run_shutdown(
                run=lambda: client_module._drain_and_shutdown(
                    flush_timeout=1, at_exit=True
                )
            )
            self.assertEqual(client_module._deferred_signals, [])
        handler.assert_not_called()

    def test_a_control_process_left_running_is_reaped_after_an_interrupt(self):
        # With leave, an interrupt during the wait leaves the control process running, as a timeout does, and a daemon thread reaps it once it ends.
        class Interrupted:
            reaped = threading.Event()
            killed = False

            def __init__(self, args, **kwargs):
                self.calls = 0
                self.stdin = _ControlStdin()
                Interrupted.made = self

            def communicate(self, input=None, timeout=None):
                self.calls += 1
                if self.calls == 1:
                    raise KeyboardInterrupt
                Interrupted.reaped.set()
                return b"", b""

            def kill(self):
                Interrupted.killed = True

        with (
            mock.patch.object(client_module.subprocess, "Popen", Interrupted),
            mock.patch.object(client_module, "_left_control", None),
        ):
            with self.assertRaises(KeyboardInterrupt):
                client_module._run_control_rpc(
                    "TerminateRun",
                    client_module.kymo_pb2.TerminateRunRequest(),
                    client_module.kymo_pb2.TerminateRunResponse,
                    server_address="127.0.0.1:1",
                    timeout=5.0,
                    leave=lambda: False,
                )
            # The run's next TerminateRun goes out after it.
            self.assertIs(client_module._left_control, Interrupted.made)
        self.assertTrue(Interrupted.reaped.wait(5))
        self.assertFalse(Interrupted.killed)

    def test_a_control_process_gets_its_request_though_the_first_step_times_out(self):
        # The wait for a TerminateRun's control process goes in steps (leave), and the first can time out before it writes the request, as a stall of the main thread there does (a lost GIL, a descheduled process). Before Python 3.13 a later step never writes it, nor closes stdin, so the request goes whole before the wait: this one reaches its RPC at once, which a closed port refuses.
        select, stalled = subprocess._PopenSelector.select, []

        def stall(selector, timeout=None):
            ready = select(selector, timeout)
            if not stalled:
                stalled.append(True)
                time.sleep(2 * client_module._SHUTDOWN_STEP)
            return ready

        started = time.monotonic()
        with (
            mock.patch.object(subprocess._PopenSelector, "select", stall),
            self.assertRaisesRegex(RuntimeError, "UNAVAILABLE") as raised,
        ):
            left = client_module._run_control_rpc(
                "TerminateRun",
                client_module.kymo_pb2.TerminateRunRequest(
                    project_id="p", run_id="r", exit_code=130
                ),
                client_module.kymo_pb2.TerminateRunResponse,
                server_address="127.0.0.1:1",
                timeout=3.0,
                leave=lambda: False,
            )
            left.kill()
            left.wait()
        self.assertTrue(stalled)
        self.assertLess(time.monotonic() - started, 3.0, raised.exception)

    def test_a_queue_whose_reader_lock_a_dead_worker_kept_is_not_salvaged(self):
        # A worker that exits holding part of a message, or is killed inside a read, keeps the queue's reader lock: the salvage reports the queue's points lost at once rather than wait out its deadline reading nothing, and spools the groups held back.
        source = _real_queue(self)
        self.assertTrue(source._rlock.acquire(False))
        self.addCleanup(source._rlock.release)
        with (
            tempfile.TemporaryDirectory() as spool_dir,
            mock.patch.multiple(
                client_module,
                _metric_queue=source,
                _queue_status=_QueueStatus(3),
                _init_pid=os.getpid(),
                _spool_dir=spool_dir,
                _project_id="p",
                _run_id="r",
                _session_id="session",
            ),
            self.assertLogs("kymo", level="ERROR") as logs,
        ):
            started = time.monotonic()
            finished = client_module._salvage_queue_before(
                types.SimpleNamespace(pre_cleanup=time.monotonic() + 5),
                [[numeric(1)]],
            )
            elapsed = time.monotonic() - started
            (path,) = (os.path.join(spool_dir, name) for name in os.listdir(spool_dir))
            records = list(read_spool(path)[1])
        self.assertTrue(finished)
        self.assertLess(elapsed, 1.0)
        self.assertEqual(records, [numeric(1)])
        self.assertIn("at most 3 queued point(s) are LOST", "\n".join(logs.output))

    def test_a_shutdown_tells_the_worker_when_to_stop_spilling(self):
        # The worker's spill ends a margin before the shutdown's kill, half the shorter of its spill share and the cleanup tail, as a signal's does, so it reports what it lost and exits by itself; a signal's bound brings it forward.
        spill_end = _QueueStatus()
        with (
            mock.patch.object(client_module, "_spill_deadline", spill_end),
            mock.patch.object(client_module, "_shutdown_deadline", _QueueStatus()),
        ):
            shutdown = client_module._Shutdown(10.0)
            now = time.monotonic()
            shutdown.plan_worker(now)
            # The cleanup tail, a fifth of the 10 s, is the shorter: its half.
            self.assertAlmostEqual(
                spill_end.value, shutdown.graceful - (shutdown.hard - now) / 10
            )
            self.assertLess(shutdown.deliver_by, spill_end.value)
            self.assertLess(spill_end.value, shutdown.graceful)
            planned = spill_end.value
            shutdown.bound(time.monotonic() + 3.0)
            shutdown.check()
            self.assertLess(spill_end.value, planned)
            self.assertLess(spill_end.value, shutdown.graceful)

    def test_a_signal_during_the_atexit_flush(self):
        # A SIGTERM (SIGUSR1 here, kymo's own) bounds the flush, and a second one, as torchrun or Slurm can send milliseconds later, only bounds it again; a Ctrl-C hands the run off. Either way the run ends with the signal's code (a SIGTERM's unless the program's was already a failure, a Ctrl-C's whatever ended the program: an exception's 1 or a sys.exit(7)), but the process keeps the program's own exit status, and its later atexit hooks run: a 143 could make a scheduler retry a job that had finished. Only a second Ctrl-C exits at once, with 130.
        sigterm = signal.SIGUSR1
        for signum, original, prior, times, code, exits_expected in (
            (sigterm, signal.SIG_DFL, None, 1, 128 + sigterm, []),
            (sigterm, signal.SIG_DFL, 0, 1, 128 + sigterm, []),
            (sigterm, signal.SIG_DFL, 7, 1, 7, []),
            (sigterm, signal.SIG_DFL, 0, 2, 128 + sigterm, []),
            (signal.SIGINT, signal.default_int_handler, None, 1, 130, []),
            (signal.SIGINT, signal.default_int_handler, 1, 1, 130, []),
            (signal.SIGINT, signal.default_int_handler, 7, 1, 130, []),
            (signal.SIGINT, signal.default_int_handler, 0, 2, 130, [130]),
        ):
            exits, ended = [], []

            class SignalingQueue(_ListQueue):
                def put(queue_self, item, block=True, timeout=None):
                    if item is None and not queue_self.items.count(None):
                        for _ in range(times):
                            signal.raise_signal(signum)
                    queue_self.items.append(item)

            class Exited(BaseException):
                pass

            def exit_now(code):
                exits.append(code)
                raise Exited

            def run():
                try:
                    return client_module._drain_and_shutdown(
                        flush_timeout=5, at_exit=True
                    )
                except Exited:
                    return "exited"

            with (
                self.subTest(signum=signum, prior=prior, times=times),
                self._signal_state(ended, _exit_code=prior),
                mock.patch.object(client_module.os, "_exit", side_effect=exit_now),
                _kymo_handles({signum: original}),
            ):
                result = self._run_shutdown(
                    target=SignalingQueue(), run=run, server_address="127.0.0.1:1"
                )
            self.assertEqual(exits, exits_expected)
            # The bounded flush delivered everything; a handed-off one cannot prove it.
            self.assertEqual(result, "exited" if exits else signum != signal.SIGINT)
            self.assertEqual(ended, [code])

    def test_kymos_own_handling_exits_with_the_signals_code(self):
        # kymo's own handling of a Ctrl-C or a SIGTERM (SIGUSR1 here) exits with the signal's status, and the run ends with a Ctrl-C's 130 whatever the program's code, and with a SIGTERM's in place of none or 0 only: by kymo's flush when no shutdown runs, or by another thread's shutdown (_signal_during_shutdown). Without a run, the first code set stays.
        sigterm = signal.SIGUSR1
        for signum, prior, code, first in (
            (signal.SIGINT, None, 130, 130),
            (signal.SIGINT, 0, 130, 0),
            (signal.SIGINT, 7, 130, 7),
            (sigterm, None, 128 + sigterm, 128 + sigterm),
            (sigterm, 0, 128 + sigterm, 0),
            (sigterm, 7, 7, 7),
        ):
            for state in ("flush", "another thread's shutdown", "no run"):
                ended = []
                shutdown = client_module._Shutdown(5.0)
                shutdown.thread = None

                def signaled(**patches):
                    with (
                        mock.patch.multiple(client_module, **patches),
                        self.assertRaises(SystemExit) as raised,
                    ):
                        signal.raise_signal(signum)
                    self.assertEqual(raised.exception.code, 128 + signum)

                with (
                    self.subTest(signum=signum, prior=prior, state=state),
                    self._signal_state(ended, _exit_code=prior),
                    _kymo_handles({signum: signal.SIG_DFL}),
                ):
                    if state == "flush":
                        self._run_shutdown(
                            run=lambda: signaled(_server_address="127.0.0.1:1")
                        )
                        self.assertEqual(ended, [code])
                    elif state == "no run":
                        signaled(_is_initialized=False)
                    else:
                        signaled(
                            _is_initialized=True,
                            _upload_process=_Process(),
                            _shutdown_claim={"shutdown": shutdown},
                        )
                        self.assertEqual(shutdown.signum, signum)
                    self.assertEqual(
                        client_module._exit_code, first if state == "no run" else code
                    )
                if state != "flush":
                    self.assertEqual(ended, [])

    def test_a_ctrl_c_ends_a_bounded_or_handed_off_shutdown_by_its_own_cap(self):
        # With a 30 s signal budget, a Ctrl-C during a SIGTERM's shutdown, or after a Ctrl-C handed it off, gives the worker 10 s from the latest Ctrl-C at most, and a SIGTERM that arms it; it ends the run with 130, once, and exits. A later SIGTERM never gives the worker longer, and neither exits nor ends the run again.
        for first, code in ((signal.SIGTERM, 143), (signal.SIGINT, 130)):
            worker, handoff, ended, exits = _Process(), _QueueStatus(), [], []
            with (
                self.subTest(first=first),
                self._signal_state(
                    _init_pid=os.getpid(),
                    _upload_process=worker,
                    _handoff=handoff,
                    _exit_code=code,
                    _terminate_run=lambda code, timeout, **_: ended.append(code),
                    _exit_now=exits.append,
                ),
                mock.patch.dict(os.environ, {"KYMO_SIGNAL_FLUSH_TIMEOUT": "30"}),
            ):
                shutdown = client_module._Shutdown(30.0, first)
                if first == signal.SIGINT:
                    shutdown.hand_off()
                    handed = handoff.value
                    self.assertGreater(handed - time.monotonic(), 9)
                interrupted = client_module._interrupted_at = time.monotonic()
                client_module._signal_during_shutdown(shutdown, signal.SIGINT)
                if first == signal.SIGINT:
                    self.assertEqual(handoff.value, handed)
                else:
                    self.assertAlmostEqual(handoff.value - interrupted, 10, delta=0.5)
                self.assertEqual(worker.terminated, 1)
                self.assertEqual(client_module._exit_code, 130)
                cap = handoff.value
                client_module._signal_during_shutdown(shutdown, signal.SIGTERM)
                self.assertEqual((handoff.value, worker.terminated), (cap, 1))
            self.assertEqual(ended, [130])
            self.assertEqual(exits, [130])

    def test_a_second_sigterm_only_bounds_the_shutdown_again(self):
        # torchrun and Slurm can send two SIGTERMs milliseconds apart: the second neither hands the run off, nor exits, nor ends the run again.
        ended, exits = [], []
        with (
            self._signal_state(
                _init_pid=os.getpid(),
                _upload_process=None,
                _terminate_run=lambda code, timeout, **_: ended.append(code),
                _exit_now=exits.append,
            ),
            mock.patch.dict(os.environ, {"KYMO_SIGNAL_FLUSH_TIMEOUT": "3"}),
        ):
            shutdown = client_module._Shutdown(60.0)
            for signum in (signal.SIGTERM, signal.SIGTERM, signal.SIGQUIT):
                client_module._signal_during_shutdown(shutdown, signum)
                self.assertLess(shutdown.hard, time.monotonic() + 3.01)
            shutdown.end_run()
            self.assertFalse(client_module._handed_off)
            self.assertEqual(client_module._exit_code, 143)
        self.assertEqual((ended, exits), ([143], []))

    def test_finish_while_a_keyboard_interrupt_propagates_hands_the_run_off(self):
        # finish() in a finally: or except: block of a Ctrl-C, or of an error raised from another cause while one was handled, ends the run with 130, as the Ctrl-C's exception hook would, never FINISHED, and so does finish() within the handoff's cap of a Ctrl-C the program caught; finish() after a KeyboardInterrupt no Ctrl-C raised, once handled, is an ordinary one.
        def in_finally():
            try:
                raise KeyboardInterrupt
            finally:
                results.append(client_module.finish(flush_timeout=1))

        def in_except():
            try:
                raise KeyboardInterrupt
            except KeyboardInterrupt:
                results.append(client_module.finish(flush_timeout=1))

        def after_handling():
            try:
                raise KeyboardInterrupt
            except KeyboardInterrupt:
                pass
            results.append(client_module.finish(flush_timeout=1))

        def from_another_cause():
            # Its context alone leads back to the Ctrl-C.
            try:
                raise KeyboardInterrupt
            except KeyboardInterrupt:
                try:
                    raise RuntimeError("aborted") from OSError("unrelated")
                except RuntimeError:
                    results.append(client_module.finish(flush_timeout=1))

        def soon_after_a_caught_one():
            # A Ctrl-C kymo's handler saw, which the program caught and left its loop on.
            client_module._interrupted_at = time.monotonic()
            after_handling()

        for body, code in (
            (in_finally, 130),
            (in_except, 130),
            (after_handling, 0),
            (from_another_cause, 130),
            (soon_after_a_caught_one, 130),
        ):
            results, ended = [], []

            def run():
                try:
                    body()
                except KeyboardInterrupt:
                    pass

            with (
                self.subTest(body=body.__name__),
                self._signal_state(ended),
            ):
                self._run_shutdown(run=run, server_address="127.0.0.1:1")
                self.assertEqual(client_module._handed_off, code == 130)
            self.assertEqual((results, ended), ([code == 0], [code]))

    def test_a_handoff_ends_the_run_before_it_writes_to_stderr(self):
        # A stderr whose reader has stalled (a full pipe) holds any write to it, so a Ctrl-C's handoff sends the run's end first: before the handoff's warnings, the poller's stop and the publications, any of which can write.
        events, poller, log = [], mock.Mock(), mock.Mock()
        poller.stop.side_effect = lambda timeout=None: events.append("poller") or True
        for level in ("info", "warning", "error"):
            getattr(log, level).side_effect = lambda *_, level=level: events.append(
                level
            )

        with self._signal_state(
            events,
            _pending=deque([_gpu_pending(1, 1.0, complete=False)]),
            _pending_incomplete=False,
            _held_back=deque(),
            _log=log,
        ):
            self._run_shutdown(
                poller=poller,
                run=lambda: client_module._drain_and_shutdown(signum=signal.SIGINT),
                server_address="127.0.0.1:1",
            )
        self.assertEqual(events[0], 130)
        self.assertIn("poller", events)
        self.assertIn("warning", events)

    def test_a_handoff_publishes_the_ready_pending_calls_without_waiting(self):
        # The pending calls whose GPU values are ready reach the queue, in order, once the run's end has gone out; those still copying are dropped, and counted. Then the captured output's tail, in the shutdown's last drain.
        target, log, drains = _ListQueue(), mock.Mock(), []
        with (
            self._signal_state(
                _pending=deque(
                    [
                        _gpu_pending(1, 1.0),
                        _gpu_pending(2, 2.0, complete=False),
                        _gpu_pending(3, 3.0),
                    ]
                ),
                _pending_incomplete=False,
                _held_back=deque(),
                _log=log,
            ),
        ):
            self._run_shutdown(
                target=target,
                run=lambda: client_module._drain_and_shutdown(signum=signal.SIGINT),
                capture=lambda final, timeout=None: drains.append(final) or ("", ""),
            )
            self.assertEqual(len(client_module._pending), 2)
        self.assertEqual(drains, [True])
        published = [
            point[1:4]
            for group in target.items
            if group is not None
            for point in client_module._decode_queue_item(group)
            if point[1] == "gpu"
        ]
        self.assertEqual(published, [("gpu", 1, 1.0)])
        self.assertIn(
            mock.call(
                "dropped %d log() call(s) %s", 2, "still waiting for their GPU values"
            ),
            log.warning.call_args_list,
        )

    def test_a_handoff_publishes_the_pending_calls_another_threads_encoder_wait_holds(
        self,
    ):
        # A gallery call behind pending calls waits for the encoder holding the pending calls' lock. A Ctrl-C's handoff on the main thread ends that wait by its claim, so the call publishes and lets the lock go, and the handoff, which waits for the lock within its budget, publishes the rest in order before the shutdown marker.
        target, worker, pacing = self._encoder_behind()
        log = mock.Mock()
        waiting = threading.Event()
        await_encoder = client_module._await_encoder

        def entered():
            waiting.set()
            await_encoder()

        def interrupted():
            caller = threading.Thread(
                target=client_module._queue_pending_log,
                args=(None, _gpu_pending(3, 3.0, galleries=True), []),
            )
            caller.start()
            # Should the handoff not end its wait, the encoder catches up, and the test fails rather than hangs.
            encoded = threading.Timer(3, setattr, (pacing.released, "value", 2))
            encoded.start()
            try:
                self.assertTrue(waiting.wait(timeout=5))
                started = time.monotonic()
                client_module._drain_and_shutdown(signum=signal.SIGINT)
                returned = time.monotonic()
                caller.join(timeout=5)
            finally:
                encoded.cancel()
            self.assertFalse(caller.is_alive())
            return returned - started

        with (
            self._signal_state(
                _pending=deque(
                    [
                        _gpu_pending(1, 1.0, galleries=True),
                        _gpu_pending(2, 2.0, galleries=True),
                    ]
                ),
                _pending_incomplete=False,
                _held_back=deque(),
                _log=log,
            ),
            mock.patch.object(client_module, "_await_encoder", entered),
        ):
            took = self._run_shutdown(target=target, process=worker, run=interrupted)
        self.assertLess(took, 0.5)
        published = [
            point[2]
            for group in target.items[: target.items.index(None)]
            for point in client_module._decode_queue_item(group)
            if point[1] == "gpu"
        ]
        self.assertEqual(published, [1, 2, 3])
        self.assertNotIn(
            "dropped %d log() call(s) %s",
            [call.args[0] for call in log.warning.call_args_list],
        )

    def test_a_ctrl_c_during_a_dead_workers_salvage_leaves_it_the_queue(self):
        # A Ctrl-C hands the shutdown off while the parent salvages a dead worker's queue: the queue stays open to the salvage, nothing more is put past its fence, and from then on its writer cuts off whole a record still being written, since the process may exit as soon as the console is back.
        target, released, seen, ended = _ListQueue(), threading.Event(), {}, []
        target.close = lambda: seen.setdefault("closed", True)

        class Spool:
            count, path = 1, "salvage.mkspool"

            def __init__(self, path, header, until=None):
                seen["until"] = until

            def close(self):
                pass

        def salvage(spool, *, source, held, hard_deadline):
            seen["before"] = seen["until"]()
            os.kill(os.getpid(), signal.SIGINT)
            released.wait(5)
            seen["after"] = seen["until"]()
            seen["closed while salvaging"] = "closed" in seen
            return 1

        def run():
            with (
                mock.patch.multiple(
                    client_module, SpoolWriter=Spool, _drain_queue_to_spool=salvage
                ),
                self.assertRaises(KeyboardInterrupt),
            ):
                client_module._drain_and_shutdown(flush_timeout=10)
            put = list(target.items)
            released.set()
            for thread in threading.enumerate():
                if thread.name == "kymo-parent-salvage":
                    thread.join(5)
            return put

        with (
            self._signal_state(
                _terminate_run=lambda code, timeout, **_: ended.append(code)
            ),
            _kymo_handles({signal.SIGINT: signal.default_int_handler}),
        ):
            put = self._run_shutdown(
                target=target, backlog=3, process=_Process(-9), run=run
            )
        self.assertEqual(put, [None])
        self.assertEqual((seen["before"], seen["after"]), (math.inf, -math.inf))
        self.assertFalse(seen["closed while salvaging"])
        self.assertEqual(ended, [0, 130])

    def test_a_shutdown_signal_anywhere_in_a_pending_calls_handoff_loses_nothing(self):
        # kymo's own handler flushes and exits wherever its signal lands while the oldest pending call is handed over to the queue, its pop, flag and assembly included: the call reaches the queue once, before the shutdown marker.
        publication = client_module._publish_oldest_pending.__code__
        traced = {client_module._publish_pending.__code__, publication}
        traced.update(c for c in publication.co_consts if isinstance(c, types.CodeType))
        for k in itertools.count():
            reports = []
            tracer, fired = _opcode_tracer(
                traced, k, lambda frame: signal.raise_signal(signal.SIGUSR1)
            )

            status = _QueueStatus()
            entry = _gpu_pending(1, 2.0, cpu={"cpu": 1.0})
            shutdown = client_module._drain_and_shutdown

            def report(**kwargs):
                reports.append(shutdown(**kwargs))

            def publish():
                with _tracing(tracer), contextlib.suppress(SystemExit):
                    client_module._publish_pending(None)

            target = self._AckingQueue(status)
            with (
                mock.patch.multiple(
                    client_module,
                    _pending=deque([entry]),
                    _pending_incomplete=False,
                    _held_back=deque(),
                    _exit_code=None,
                    _drain_and_shutdown=report,
                    _log=mock.Mock(),
                ),
                _kymo_handles({signal.SIGUSR1: signal.SIG_DFL}),
            ):
                self._run_shutdown(target=target, status=status, run=publish)
            state = f"signal at opcode {k}"
            published = [
                point[1]
                for group in target.items
                if group is not None
                for point in client_module._decode_queue_item(group)
            ]
            self.assertEqual(published, ["cpu", "gpu"], state)
            self.assertEqual(len(reports), len(fired), state)
            if not fired:
                break
            self.assertIsNone(target.items[-1], state)
        self.assertGreater(k, 50)

    def test_a_dead_workers_salvage_takes_the_held_back_groups(self):
        # The salvage spools the held-back groups, and finish() still reports them undelivered. A shutdown entered again meanwhile, by a handler kymo does not own, say, runs no second one.
        salvaged = []

        def salvage(shutdown, held):
            salvaged.append((held, list(client_module._held_back)))
            self.assertFalse(client_module._drain_and_shutdown(flush_timeout=0.1))
            return True

        with (
            mock.patch.object(client_module, "_held_back", deque([[numeric(7)]])),
            mock.patch.object(
                client_module, "_salvage_queue_before", side_effect=salvage
            ),
            self.assertLogs("kymo", level="WARNING"),
        ):
            complete = self._run_shutdown(
                target=_FullQueue(), status=_QueueStatus(1), process=_Process(1)
            )
            self.assertEqual(list(client_module._held_back), [])
        self.assertFalse(complete)
        self.assertEqual(salvaged, [([[numeric(7)]], [])])

    def test_the_salvage_takes_the_groups_no_charge_let_out(self):
        # The accounting lock stays busy through the shutdown: the groups held back uncharged, and the captured tail, which joins them, never reach the queue and add nothing to the backlog, yet the salvage spools them, even after a clean exit of the worker, and finish() reports them undelivered.
        salvaged = []

        def salvage(shutdown, held):
            salvaged.append(held)
            return True

        def lock_busy(points, hard_deadline):
            raise client_module._AccountingLockUnavailable("busy")

        with (
            mock.patch.object(client_module, "_held_back", deque()),
            mock.patch.object(client_module, "_uncharged", deque([[numeric(8)]])),
            mock.patch.object(client_module, "_charge_backlog", lock_busy),
            mock.patch.object(
                client_module, "_salvage_queue_before", side_effect=salvage
            ),
            self.assertLogs("kymo", level="WARNING"),
        ):
            complete = self._run_shutdown(capture=("tail", ""))
            self.assertEqual(list(client_module._uncharged), [])
        self.assertFalse(complete)
        self.assertEqual(
            salvaged,
            [[[numeric(8)], [("text_ts", "logs/std_out", 1234, "tail", 1234)]]],
        )

    def test_the_groups_held_back_that_no_salvage_takes_are_dropped_and_counted(self):
        # A worker still alive after SIGKILL is never salvaged: the groups a full queue held back are dropped and counted.
        class Unkillable(_Process):
            def kill(self):
                self.killed += 1

        with (
            mock.patch.object(client_module, "_held_back", deque([[numeric(7)]])),
            mock.patch.object(client_module, "_uncharged", deque([[numeric(8)]])),
            self.assertLogs("kymo", level="WARNING") as logs,
        ):
            complete = self._run_shutdown(target=_FullQueue(), process=Unkillable())
        self.assertFalse(complete)
        self.assertIn(
            "dropped 2 point(s) that never reached the upload queue",
            "\n".join(logs.output),
        )

    def test_shutdown_puts_groups_held_back_for_a_signal_first(self):
        status = _QueueStatus(1)

        target = self._AckingQueue(status)
        with mock.patch.object(client_module, "_held_back", deque([[numeric(7)]])):
            complete = self._run_shutdown(target=target, status=status)
            self.assertEqual(list(client_module._held_back), [])
        self.assertTrue(complete)
        self.assertEqual(target.items, [[numeric(7)], None])

    def test_shutdown_publishes_pending_logs_without_values_never_copied(self):
        for copied in (True, False):
            with self.subTest(copied=copied):
                status = _QueueStatus()

                entry = client_module._PendingLog(
                    (
                        client_module._NUMERIC_BATCH_QUEUE_ITEM,
                        1,
                        1,
                        ["cpu"],
                        [1.0],
                        [None],
                    ),
                    [],
                    step=1,
                    gpu_keys=[("gpu", None)],
                )
                entry.reads = mock.Mock()
                entry.reads.wait.return_value = copied
                entry.reads.scalars.return_value = [(0, 2.0)]
                target = self._AckingQueue(status)
                with (
                    mock.patch.object(client_module, "_pending", deque([entry])),
                    mock.patch.object(client_module, "_pending_incomplete", False),
                    contextlib.nullcontext()
                    if copied
                    else self.assertLogs("kymo", level="WARNING"),
                ):
                    complete = self._run_shutdown(target=target, status=status)

                self.assertEqual(complete, copied)
                points = [("numeric_ts", "cpu", 1, 1.0, 1)]
                if copied:
                    points.append(("numeric_ts", "gpu", 1, 2.0, 1))
                self.assertEqual(
                    [client_module._decode_queue_item(item) for item in target.items],
                    [points, None],
                )

    def test_shutdown_fails_when_a_log_queues_behind_it(self):
        status = _QueueStatus()
        pending = deque()

        class LateLogQueue(_ListQueue):
            def put(queue_self, item, block=True, timeout=None):
                queue_self.items.append(item)
                if item is not None:
                    status.value -= client_module._queue_item_size(item)
                    # Another thread's log() queues after the pending calls were drained.
                    pending.append(client_module._PendingLog([], []))

        with (
            mock.patch.object(client_module, "_pending", pending),
            mock.patch.object(client_module, "_pending_incomplete", False),
        ):
            complete = self._run_shutdown(
                target=LateLogQueue(), status=status, capture=("tail", "")
            )

        self.assertFalse(complete)

    def test_shutdown_bounds_its_wait_for_the_pending_calls_lock(self):
        _hold(self, client_module._pending_lock)
        started = time.monotonic()
        with (
            mock.patch.object(client_module, "_pending", deque()),
            self.assertLogs("kymo", level="ERROR"),
        ):
            complete = self._run_shutdown(flush_timeout=0.2)

        self.assertLess(time.monotonic() - started, 1.0)
        self.assertTrue(complete)

    def test_shutdown_continues_but_fails_if_captured_tail_cannot_publish(self):
        status = _QueueStatus()

        class FailingTailQueue(_ListQueue):
            def put(queue_self, item, block=True, timeout=None):
                queue_self.items.append(item)
                if item is not None:
                    raise OSError("queue closed")

        target = FailingTailQueue()
        with self.assertLogs("kymo", level="WARNING"):
            complete = self._run_shutdown(
                target=target, status=status, capture=("tail", "")
            )

        self.assertFalse(complete)
        self.assertEqual(len(target.items), 2)
        self.assertIsNone(target.items[-1])
        # The refused tail keeps its charge: the queue may have taken it.
        self.assertEqual(status.value, 1)

    def test_final_capture_accounting_obeys_the_shutdown_deadline(self):
        status = _QueueStatus()
        status.lock.acquire()
        self.addCleanup(status.lock.release)
        target = _ListQueue()

        started = time.monotonic()
        with self.assertLogs("kymo", level="WARNING"):
            complete = self._run_shutdown(
                target=target,
                status=status,
                capture=("tail", ""),
                flush_timeout=0.02,
            )

        self.assertFalse(complete)
        self.assertEqual(target.items, [None])
        self.assertLess(time.monotonic() - started, 0.25)

    def test_finish_fails_once_a_point_was_spooled(self):
        def run():
            with mock.patch.object(client_module, "_upload_spooled", _QueueStatus(1)):
                return client_module._drain_and_shutdown(flush_timeout=0.1)

        self.assertTrue(self._run_shutdown())
        self.assertFalse(self._run_shutdown(run=run))

    def test_shutdown_budget_keeps_equal_delivery_and_worker_spill_halves(self):
        # When the worker stops delivering and spilling, is killed, and the cleanup tail: delivery and spill take equal halves before the tail, the spill ending half the shorter of its half and the tail before the kill.
        for total, expected in (
            (3.0, (1.2, 2.1, 2.4, 0.6)),
            (10.0, (4.0, 7.0, 8.0, 2.0)),
            (60.0, (25.0, 45.0, 50.0, 10.0)),
        ):
            with self.subTest(total=total):
                actual = client_module._worker_window(0.0, total)
                for value, want in zip(actual, expected):
                    self.assertAlmostEqual(value, want)

    def test_finish_fails_on_unquiesced_poller(self):
        self.assertFalse(self._run_shutdown(backlog=0, poller_quiesced=False))

    def test_finish_fails_when_clean_worker_leaves_positive_status(self):
        self.assertFalse(self._run_shutdown(backlog=1, poller_quiesced=True))

    def test_finish_fails_after_a_gallery_went_out_cut_short(self):
        # The worker exits cleanly after publishing a gallery without items its encode dropped or the CDN rejected, but delivery is incomplete.
        cut_short = _QueueStatus(client_module._WORKER_GALLERY_CUT_SHORT)
        with mock.patch.object(client_module, "_upload_failure", cut_short):
            self.assertFalse(self._run_shutdown())

    def test_terminal_shutdown_retires_every_worker_spool_segment(self):
        target = _ListQueue()
        with tempfile.TemporaryDirectory() as spool_dir:
            # The pre-created worker path plus a segment the worker rotated to and
            # named itself: a force-killed worker retires neither, so the owner
            # must find both by this init's session prefix.
            spool_path = make_spool_path(
                "p", "r", "worker", spool_dir, session="session-a"
            )
            rotated_path = make_spool_path(
                "p", "r", "worker", spool_dir, session="session-a"
            )
            other_run = make_spool_path(
                "p", "other", "worker", spool_dir, session="session-a"
            )
            # Same project and run_id, but not this init()'s files. A rejection
            # says nothing about another deployment, and a restored generation or
            # a sibling rank on THIS server is still someone else's data.
            other_server = make_spool_path(
                "p", "r", "worker", spool_dir, session="session-b"
            )
            other_generation = make_spool_path(
                "p", "r", "worker", spool_dir, session="session-c"
            )
            for path, server, session in (
                (spool_path, "server", "session-a"),
                (rotated_path, "server", "session-a"),
                (other_run, "server", "session-a"),
                (other_server, "elsewhere", "session-b"),
                (other_generation, "server", "session-c"),
            ):
                writer = SpoolWriter(
                    path,
                    client_module._spool_header(server, "p", "r", "run", "", session),
                )
                writer.write(("numeric_ts", "train/loss", 1, 1.0, 1_700_000_000_000))
                writer.close()

            with (
                mock.patch.object(client_module, "_spool_dir", spool_dir),
                mock.patch.object(client_module, "_session_id", "session-a"),
                mock.patch.object(client_module, "_is_initialized", True),
                mock.patch.object(client_module, "_init_pid", os.getpid()),
                mock.patch.object(client_module, "_metric_queue", target),
                mock.patch.object(client_module, "_queue_status", _QueueStatus(2)),
                mock.patch.object(client_module, "_upload_terminal", _QueueStatus(1)),
                mock.patch.multiple(
                    client_module,
                    _upload_spooled=_QueueStatus(0),
                    _upload_failure=_QueueStatus(),
                ),
                mock.patch.object(client_module, "_upload_process", _Process(0)),
                mock.patch.object(client_module, "_system_poller", None),
                mock.patch.object(client_module, "_shutdown_deadline", _QueueStatus()),
                mock.patch.object(client_module, "_spill_deadline", _QueueStatus()),
                mock.patch.object(client_module, "_server_address", "server"),
                mock.patch.object(client_module, "_project_id", "p"),
                mock.patch.object(client_module, "_run_id", "r"),
                mock.patch.object(client_module, "_salvage_queue_before") as salvage,
                mock.patch.object(client_module, "_run_control_rpc") as terminate,
                self.assertLogs("kymo", level="ERROR"),
            ):
                complete = client_module._drain_and_shutdown(flush_timeout=0.1)

            self.assertFalse(complete)
            salvage.assert_not_called()
            terminate.assert_not_called()
            for path in (spool_path, rotated_path):
                self.assertFalse(os.path.exists(path))
                self.assertTrue(os.path.exists(path + ".deleted"))
            self.assertTrue(os.path.exists(other_run), "retired another run's spool")
            self.assertTrue(
                os.path.exists(other_server), "retired another server's spool"
            )
            self.assertTrue(
                os.path.exists(other_generation),
                "retired a restored generation's spool",
            )

    def _hanging_scan(self):
        """A spool scan stuck the way a hung mount sticks, plus its release."""
        entered = threading.Event()
        release = threading.Event()
        self.addCleanup(release.set)

        def scan(*_args, **_kwargs):
            entered.set()
            release.wait(timeout=30)
            return []

        return scan, entered, release

    def test_stalled_spool_cleanup_cannot_outlive_the_finish_deadline(self):
        """finish() promises a bounded return, so listing, a terminal lock check,
        and rename — all on a mount that can hang — run off this thread, and the
        join honours the deadline it was given rather than extending it."""
        scan, entered, _release = self._hanging_scan()
        deadline_in = 0.05

        started = time.monotonic()
        with (
            mock.patch.object(client_module, "run_spool_files", scan),
            mock.patch.object(client_module, "_project_id", "p"),
            mock.patch.object(client_module, "_run_id", "r"),
            mock.patch.object(client_module, "_spool_dir", "/nonexistent"),
            mock.patch.object(client_module, "_session_id", "session-a"),
            self.assertLogs("kymo", level="ERROR") as logs,
        ):
            had_spool = client_module._resolve_run_spools(
                terminal_rejected=False,
                deadline=_at(time.monotonic() + deadline_in),
            )
        elapsed = time.monotonic() - started

        self.assertTrue(entered.is_set())
        self.assertTrue(had_spool, "an unproven spool state must not claim delivery")
        # The budget it was handed, plus scheduling slack — NOT a floor added on top.
        self.assertLess(elapsed, deadline_in + 0.25)
        self.assertIn("exceeded the finish deadline", "\n".join(logs.output))
        self.assertIn("python -m kymo.sync", "\n".join(logs.output))

    def test_finish_reserves_its_cleanup_slice_inside_the_flush_budget(self):
        """Cleanup time is carved out of the caller's budget, not added to it, so
        every earlier phase must be handed the earlier deadline."""
        captured = []

        def salvage(shutdown, held):
            captured.append(shutdown.pre_cleanup)
            return True

        flush_timeout = 10.0
        # Taken from the forced-cleanup tail, capped at the reserve.
        cleanup_reserve = client_module._worker_window(0.0, flush_timeout)[3]
        slice_seconds = min(client_module._SPOOL_CLEANUP_RESERVE, cleanup_reserve / 2)
        self.assertGreater(slice_seconds, 0)
        started = time.monotonic()
        with (
            mock.patch.object(client_module, "run_spool_files", return_value=[]),
            mock.patch.object(
                client_module, "_salvage_queue_before", side_effect=salvage
            ),
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_init_pid", os.getpid()),
            mock.patch.object(client_module, "_metric_queue", _ListQueue()),
            # A nonzero backlog on a cleanly exited worker forces owner salvage.
            mock.patch.object(client_module, "_queue_status", _QueueStatus(3)),
            mock.patch.multiple(
                client_module,
                _upload_terminal=_QueueStatus(),
                _upload_failure=_QueueStatus(),
                _upload_spooled=_QueueStatus(),
            ),
            mock.patch.object(client_module, "_upload_process", _Process(0)),
            mock.patch.object(client_module, "_system_poller", None),
            mock.patch.object(client_module, "_shutdown_deadline", _QueueStatus()),
            mock.patch.object(client_module, "_spill_deadline", _QueueStatus()),
            mock.patch.object(client_module, "_spool_dir", "/nonexistent"),
            mock.patch.object(client_module, "_server_address", ""),
            mock.patch.object(client_module, "_project_id", "p"),
            mock.patch.object(client_module, "_run_id", "r"),
            mock.patch.object(client_module, "_session_id", "session-a"),
            mock.patch.object(client_module, "_run_control_rpc"),
            self.assertLogs("kymo", level="ERROR"),
        ):
            client_module._drain_and_shutdown(flush_timeout=flush_timeout)

        self.assertEqual(len(captured), 1)
        self.assertLessEqual(
            captured[0], started + flush_timeout - slice_seconds + 0.05
        )

    def test_timed_out_cleanup_acts_for_the_run_it_started_for(self):
        """A cleanup that outlives finish() overlaps the next init(). It must keep
        deciding by the filename identity captured at launch, never globals that
        a later init() has replaced."""
        gate = threading.Event()
        self.addCleanup(gate.set)
        real_active = spool_module.writer_active

        def stalling_active(path):
            # Stall at the destructive writer check after session-scoped listing.
            gate.wait(10)
            return real_active(path)

        with tempfile.TemporaryDirectory() as spool_dir:
            paths = {}
            for session, server in (("mine", "server-a"), ("theirs", "server-b")):
                writer = SpoolWriter(
                    make_spool_path("p", "r", "worker", spool_dir, session=session),
                    client_module._spool_header(server, "p", "r", "run", "", session),
                )
                writer.write(("numeric_ts", "train/loss", 1, 1.0, 1_700_000_000_000))
                paths[session] = writer.close()

            with (
                mock.patch.object(client_module, "writer_active", stalling_active),
                mock.patch.object(client_module, "_project_id", "p"),
                mock.patch.object(client_module, "_run_id", "r"),
                mock.patch.object(client_module, "_spool_dir", spool_dir),
                mock.patch.object(client_module, "_session_id", "mine"),
                mock.patch.object(client_module, "_server_address", "server-a"),
                self.assertLogs("kymo", level="ERROR"),
            ):
                self.assertTrue(
                    client_module._resolve_run_spools(
                        terminal_rejected=True, deadline=_at(time.monotonic())
                    )
                )
                # finish() has returned; a later init() re-points every global.
                client_module._project_id = "p2"
                client_module._run_id = "r2"
                client_module._session_id = "theirs"
                client_module._server_address = "server-b"
                gate.set()
                deadline = time.monotonic() + 5
                while not os.path.exists(paths["mine"] + ".deleted"):
                    self.assertLess(time.monotonic(), deadline, "cleanup never ran")
                    time.sleep(0.01)

            self.assertTrue(
                os.path.exists(paths["theirs"]),
                "the late cleanup retired the NEXT run's spool",
            )

    def test_terminal_cleanup_timeout_does_not_advise_replay(self):
        """A rejected run's leftovers must never be advertised as replayable: the
        daemon can die at interpreter exit before it renames them. Session-scoped
        names make deletion advice precise even if listing itself stalls."""
        scan, _entered, _release = self._hanging_scan()

        with (
            mock.patch.object(client_module, "run_spool_files", scan),
            mock.patch.object(client_module, "_project_id", "p"),
            mock.patch.object(client_module, "_run_id", "r"),
            mock.patch.object(client_module, "_spool_dir", "/nonexistent"),
            mock.patch.object(client_module, "_session_id", "mine"),
            self.assertLogs("kymo", level="ERROR") as logs,
        ):
            had_spool = client_module._resolve_run_spools(
                terminal_rejected=True, deadline=_at(time.monotonic() + 0.05)
            )

        output = "\n".join(logs.output)
        self.assertTrue(had_spool)
        self.assertNotIn("python -m kymo.sync", output)
        self.assertIn(spool_module.spool_name_prefix("p", "r", "mine"), output)

    def test_terminal_cleanup_timeout_names_only_its_session_prefix(self):
        """A stalled rename names the captured session prefix, never a sibling's."""
        gate = threading.Event()
        self.addCleanup(gate.set)

        with tempfile.TemporaryDirectory() as spool_dir:
            for session in ("mine", "theirs"):
                writer = SpoolWriter(
                    make_spool_path("p", "r", "worker", spool_dir, session=session),
                    client_module._spool_header("srv", "p", "r", "run", "", session),
                )
                writer.write(("numeric_ts", "train/loss", 1, 1.0, 1_700_000_000_000))
                writer.close()

            with (
                # Classification finishes; the rename is what hangs.
                mock.patch.object(
                    client_module,
                    "retire_deleted_spool",
                    side_effect=lambda _path: gate.wait(10) or "",
                ),
                mock.patch.object(client_module, "_project_id", "p"),
                mock.patch.object(client_module, "_run_id", "r"),
                mock.patch.object(client_module, "_spool_dir", spool_dir),
                mock.patch.object(client_module, "_session_id", "mine"),
                self.assertLogs("kymo", level="ERROR") as logs,
            ):
                had_spool = client_module._resolve_run_spools(
                    terminal_rejected=True, deadline=_at(time.monotonic() + 0.2)
                )

            output = "\n".join(logs.output)
            self.assertTrue(had_spool)
            self.assertIn(spool_module.spool_name_prefix("p", "r", "mine"), output)
            self.assertNotIn(spool_module.spool_name_prefix("p", "r", "theirs"), output)
            self.assertNotIn("python -m kymo.sync", output)

    def test_failed_terminal_retirement_is_not_offered_for_replay(self):
        """A rename that fails leaves the *.mkspool name on inadmissible points.
        Reporting it through the ordinary replay path would tell an operator to
        push a deleted run's data back at the server."""
        with tempfile.TemporaryDirectory() as spool_dir:
            writer = SpoolWriter(
                make_spool_path("p", "r", "worker", spool_dir, session="mine"),
                client_module._spool_header("srv", "p", "r", "run", "", "mine"),
            )
            writer.write(("numeric_ts", "train/loss", 1, 1.0, 1_700_000_000_000))
            stranded = writer.close()

            with (
                mock.patch.object(
                    client_module,
                    "retire_deleted_spool",
                    side_effect=OSError("read-only file system"),
                ),
                mock.patch.object(client_module, "_project_id", "p"),
                mock.patch.object(client_module, "_run_id", "r"),
                mock.patch.object(client_module, "_spool_dir", spool_dir),
                mock.patch.object(client_module, "_session_id", "mine"),
                self.assertLogs("kymo", level="ERROR") as logs,
            ):
                had_spool = client_module._resolve_run_spools(
                    terminal_rejected=True, deadline=_at(time.monotonic() + 5)
                )

            output = "\n".join(logs.output)
            self.assertTrue(
                had_spool, "a stranded rejected spool is not proof of delivery"
            )
            self.assertTrue(os.path.exists(stranded), "evidence was discarded")
            self.assertIn(stranded, output)
            self.assertIn("do not replay", output)
            self.assertNotIn("python -m kymo.sync", output)

    def test_another_sessions_spool_does_not_fail_this_runs_delivery(self):
        """had_spool is this invocation's claim. A file sharing the ids but written
        by another session belongs to that session's report, not this result."""
        with tempfile.TemporaryDirectory() as spool_dir:
            writer = SpoolWriter(
                make_spool_path("p", "r", "worker", spool_dir, session="other-session"),
                client_module._spool_header(
                    "elsewhere", "p", "r", "run", "", "other-session"
                ),
            )
            writer.write(("numeric_ts", "train/loss", 1, 1.0, 1_700_000_000_000))
            other = writer.close()

            with (
                mock.patch.object(client_module, "_project_id", "p"),
                mock.patch.object(client_module, "_run_id", "r"),
                mock.patch.object(client_module, "_spool_dir", spool_dir),
                mock.patch.object(client_module, "_session_id", "this-session"),
            ):
                had_spool = client_module._resolve_run_spools(
                    terminal_rejected=False, deadline=_at(time.monotonic() + 5)
                )

            self.assertFalse(had_spool, "another session's spool failed this run")
            self.assertTrue(os.path.exists(other))

    def test_empty_session_cleanup_fails_closed_before_starting_a_daemon(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            paths = [
                make_spool_path("p", "r", "worker", spool_dir, session=session)
                for session in ("mine", "theirs")
            ]
            for path in paths:
                with open(path, "wb") as fh:
                    fh.write(b"spooled")

            with (
                mock.patch.object(client_module, "_project_id", "p"),
                mock.patch.object(client_module, "_run_id", "r"),
                mock.patch.object(client_module, "_spool_dir", spool_dir),
                mock.patch.object(client_module, "_session_id", ""),
                mock.patch.object(client_module, "retire_deleted_spool") as retire,
                self.assertLogs("kymo", level="ERROR") as logs,
            ):
                had_spool = client_module._resolve_run_spools(
                    terminal_rejected=True, deadline=_at(time.monotonic())
                )

            output = "\n".join(logs.output)
            self.assertTrue(had_spool)
            self.assertIn("session identity is missing", output)
            self.assertNotIn("do not replay any remaining", output)
            retire.assert_not_called()
            for path in paths:
                self.assertTrue(os.path.exists(path))

    def test_nonterminal_cleanup_never_reads_spool_headers_for_ownership(self):
        """Filename ownership must remain the shutdown fast path."""
        with tempfile.TemporaryDirectory() as spool_dir:
            writer = SpoolWriter(
                make_spool_path("p", "r", "worker", spool_dir, session="mine"),
                client_module._spool_header("srv", "p", "r", "run", "", "mine"),
            )
            writer.write(("numeric_ts", "train/loss", 1, 1.0, 1_700_000_000_000))
            owned = writer.close()

            with (
                mock.patch.object(client_module, "_project_id", "p"),
                mock.patch.object(client_module, "_run_id", "r"),
                mock.patch.object(client_module, "_spool_dir", spool_dir),
                mock.patch.object(client_module, "_session_id", "mine"),
                mock.patch.object(
                    spool_module,
                    "read_spool_header",
                    wraps=spool_module.read_spool_header,
                ) as read_header,
                mock.patch.object(
                    os.path,
                    "exists",
                    side_effect=AssertionError(
                        "non-terminal reporting restatted a listed spool"
                    ),
                ) as exists,
                mock.patch(
                    "builtins.open",
                    side_effect=AssertionError(
                        "non-terminal ownership resolution opened a spool file"
                    ),
                ) as open_file,
                self.assertLogs("kymo", level="ERROR") as logs,
            ):
                had_spool = client_module._resolve_run_spools(
                    terminal_rejected=False, deadline=_at(time.monotonic() + 5)
                )

            self.assertTrue(had_spool)
            self.assertIn(owned, "\n".join(logs.output))
            read_header.assert_not_called()
            exists.assert_not_called()
            open_file.assert_not_called()

    def test_worker_terminal_retirement_never_advises_replay_after_an_error(self):
        paths = ["gone.mkspool", "stuck.mkspool"]
        with (
            mock.patch.object(
                client_module,
                "retire_deleted_spool",
                side_effect=[
                    FileNotFoundError(paths[0]),
                    OSError("stale NFS handle"),
                ],
            ) as retire,
            self.assertLogs("kymo", level="ERROR") as logs,
        ):
            client_module._retire_rejected_worker_spools(paths)

        output = "\n".join(logs.output)
        self.assertEqual(paths, [])
        self.assertEqual(retire.call_count, 2)
        self.assertIn("stuck.mkspool", output)
        self.assertIn("do not replay", output)
        self.assertNotIn("python -m kymo.sync", output)

    def test_terminal_retirement_treats_a_vanished_file_as_resolved(self):
        """A concurrent sync rename after listdir is not stranded data."""
        with tempfile.TemporaryDirectory() as spool_dir:
            path = make_spool_path("p", "r", "worker", spool_dir, session="mine")
            with open(path, "wb") as fh:
                fh.write(b"spooled")

            with (
                mock.patch.object(client_module, "writer_active", return_value=False),
                mock.patch.object(
                    client_module,
                    "retire_deleted_spool",
                    side_effect=FileNotFoundError(path),
                ),
            ):
                had_spool = client_module._retire_or_report_run_spools(
                    project_id="p",
                    run_id="r",
                    spool_dir=spool_dir,
                    session="mine",
                    terminal_rejected=True,
                )

            self.assertFalse(had_spool)

    def test_terminal_retirement_does_not_rename_an_active_owned_file(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = make_spool_path("p", "r", "worker", spool_dir, session="mine")
            with open(path, "wb") as fh:
                fh.write(b"spooled")

            with (
                mock.patch.object(client_module, "writer_active", return_value=True),
                mock.patch.object(client_module, "retire_deleted_spool") as retire,
                self.assertLogs("kymo", level="ERROR") as logs,
            ):
                had_spool = client_module._retire_or_report_run_spools(
                    project_id="p",
                    run_id="r",
                    spool_dir=spool_dir,
                    session="mine",
                    terminal_rejected=True,
                )

            self.assertTrue(had_spool)
            self.assertTrue(os.path.exists(path))
            retire.assert_not_called()
            self.assertIn("locked by its writer or a replay", "\n".join(logs.output))

    def test_unreadable_spool_directory_is_not_reported_as_empty(self):
        """A listing error must not read as proof that nothing is waiting — that
        would silently leave terminal-rejected files replayable."""
        with (
            mock.patch.object(
                client_module,
                "run_spool_files",
                side_effect=PermissionError("stale NFS handle"),
            ),
            mock.patch.object(client_module, "_project_id", "p"),
            mock.patch.object(client_module, "_run_id", "r"),
            mock.patch.object(client_module, "_session_id", "session-a"),
            self.assertLogs("kymo", level="ERROR") as logs,
        ):
            had_spool = client_module._resolve_run_spools(
                terminal_rejected=True, deadline=_at(time.monotonic() + 5)
            )

        self.assertTrue(had_spool)
        self.assertIn("failed to resolve this run's spool", "\n".join(logs.output))

    def test_sigkill_fallback_makes_worker_dead_before_salvage_is_safe(self):
        class Stuck:
            def __init__(self):
                self.alive = True
                self.exitcode = None
                self.events = []

            def is_alive(self):
                return self.alive

            def join(self, timeout):
                self.events.append(("join", timeout))

            def kill(self):
                self.events.append(("kill", None))
                self.alive = False
                self.exitcode = -9

        process = Stuck()
        now = time.monotonic()
        dead, clean = client_module._stop_upload_process(
            process,
            graceful_deadline=_at(now),
            stop_deadline=_at(now + 1),
        )

        self.assertTrue(dead)
        self.assertFalse(clean)
        # SIGTERM only bounds the worker by the signal budget (_own_signals): it is killed.
        self.assertEqual([kind for kind, _ in process.events], ["join", "kill"])

    @unittest.skipUnless(
        os.path.exists("/proc/self/stat"), "a zombie reads as running without /proc"
    )
    def test_a_zombie_worker_its_forkserver_left_is_salvaged(self):
        # Under forkserver, once a group SIGTERM has killed the forkserver, a worker killed before it published its exit (an OOM kill, say) is a zombie for good under a PID 1 that never reaps: os.kill(pid, 0) still finds it, and the shutdown took it for running and skipped the salvage of its queue. Its state in /proc reads as exited.
        zombie = subprocess.Popen([sys.executable, "-c", "pass"])
        try:
            # Exited, and left unreaped.
            os.waitid(os.P_PID, zombie.pid, os.WEXITED | os.WNOWAIT)
            worker = types.SimpleNamespace(
                pid=zombie.pid,
                exitcode=255,
                _popen=types.SimpleNamespace(method="forkserver"),
                is_alive=lambda: False,
                join=lambda timeout=None: None,
            )
            with (
                self._signal_state(_worker_exit=_QueueStatus(-1)),
                mock.patch.object(
                    client_module, "_salvage_queue_before", return_value=True
                ) as salvage,
            ):
                self.assertFalse(self._run_shutdown(process=worker, flush_timeout=5.0))
            salvage.assert_called_once()
        finally:
            zombie.wait()

    def test_shutdown_status_read_bypasses_a_poisoned_process_lock(self):
        # SIGKILL can strand the Value's SemLock in the worker's accounting update: the shutdown reads the backlog raw, never through the lock or the synchronized property.
        touched = []

        class PoisonedStatus:
            @property
            def value(self):
                touched.append("value")

            @staticmethod
            def get_lock():
                touched.append("lock")

            @staticmethod
            def get_obj():
                return types.SimpleNamespace(value=0)

        self.assertTrue(self._run_shutdown(status=PoisonedStatus()))
        self.assertEqual(touched, [])

    def test_parent_salvage_returns_at_deadline_if_fsync_path_stalls(self):
        release = threading.Event()
        closed = threading.Event()

        class SlowSpool:
            path = "/tmp/salvage.mkspool"

            @staticmethod
            def close():
                closed.set()
                return SlowSpool.path

        old_queue = types.SimpleNamespace(_rlock=threading.Lock())
        captured_sources = []

        def blocked_drain(_spool, *, source, held, hard_deadline):
            captured_sources.append(source)
            self.assertEqual(held, [])
            self.assertGreater(hard_deadline, 0)
            release.wait(timeout=1)
            return 1

        started = time.monotonic()
        with (
            mock.patch.object(client_module, "SpoolWriter", return_value=SlowSpool()),
            mock.patch.object(
                client_module, "make_spool_path", return_value=SlowSpool.path
            ),
            mock.patch.object(
                client_module, "_drain_queue_to_spool", side_effect=blocked_drain
            ),
            mock.patch.object(client_module, "_metric_queue", old_queue),
            self.assertLogs("kymo", level="ERROR") as logs,
        ):
            finished = client_module._salvage_queue_before(
                types.SimpleNamespace(pre_cleanup=time.monotonic() + 0.02), []
            )
        elapsed = time.monotonic() - started
        release.set()
        self.assertTrue(closed.wait(timeout=1))

        self.assertFalse(finished)
        self.assertEqual(captured_sources, [old_queue])
        self.assertLess(elapsed, 0.2)
        self.assertIn("exceeded the finish deadline", "\n".join(logs.output))


class PollerPublicationTests(unittest.TestCase):
    @staticmethod
    def _poller(publish):
        poller = SystemMetricsPoller(publish=publish)
        poller._psutil_ok = True
        poller._poll_cpu_mem = lambda metrics: metrics.update({"system/cpu": 1.0})
        poller._poll_disk = lambda _metrics: None
        poller._poll_network = lambda _metrics: None
        return poller

    def test_publication_failure_propagates(self):
        class FailingPublisher:
            def __call__(self, _groups, capture_ms=None):
                raise OSError("queue closed")

        poller = self._poller(FailingPublisher())
        with self.assertRaises(OSError):
            poller._poll_once()

    def test_polling_stops_once_the_worker_has_exited(self):
        publish = mock.Mock(side_effect=client_module._WorkerExited("worker gone"))
        poller = self._poller(publish)
        with (
            self.assertLogs("kymo", level="WARNING") as logs,
            mock.patch.object(poller._stop, "wait", side_effect=[False]),
        ):
            poller._poll_loop()
        self.assertEqual(publish.call_count, 1)
        self.assertIn("upload worker has exited", "\n".join(logs.output))

    def test_polling_survives_failed_publications(self):
        # A failure after a success still waits out the minute.
        busy = RuntimeError("accounting lock busy")
        publish = mock.Mock(side_effect=[busy, None, busy, None])
        poller = self._poller(publish)
        with (
            self.assertLogs("kymo", level="WARNING") as logs,
            mock.patch.object(poller._stop, "wait", side_effect=[False] * 4 + [True]),
        ):
            poller._poll_loop()
        self.assertEqual(publish.call_count, 4)
        failures = [line for line in logs.output if "poll failed" in line]
        self.assertEqual(len(failures), 1)

    def test_polling_warns_again_once_the_minute_has_passed(self):
        now = [0.0]

        def publish(_groups, capture_ms=None):
            # Failures 30 s apart: the first warns, the second is within its minute, the third is not.
            now[0] += 30.0
            raise RuntimeError("accounting lock busy")

        poller = self._poller(publish)
        with (
            self.assertLogs("kymo", level="WARNING") as logs,
            mock.patch.object(poller._stop, "wait", side_effect=[False] * 3 + [True]),
            mock.patch.object(
                system_metrics_module.time, "monotonic", side_effect=lambda: now[0]
            ),
        ):
            poller._poll_loop()
        failures = [line for line in logs.output if "poll failed" in line]
        self.assertEqual(len(failures), 2)

    @staticmethod
    def _fake_nvml(fails=(), *, limit_mw=700_000, total=80e9):
        """A pynvml stand-in whose readings fail for a call named in fails, or for a (call, GPU index) pair in it; the healthy GPU is idle and error-free, so its zeros are true readings."""

        def reading(call, value):
            def read(handle, *_args):
                if call in fails or (call, handle) in fails:
                    raise RuntimeError("NVML_ERROR_NOT_SUPPORTED")
                return value

            return read

        names = (
            "NVML_TEMPERATURE_GPU",
            "NVML_CLOCK_SM",
            "NVML_CLOCK_MEM",
            "NVML_MEMORY_ERROR_TYPE_UNCORRECTED",
            "NVML_MEMORY_ERROR_TYPE_CORRECTED",
            "NVML_VOLATILE_ECC",
            "NVML_MEMORY_LOCATION_DEVICE_MEMORY",
        )
        return types.SimpleNamespace(
            **{name: name for name in names},
            nvmlDeviceGetUtilizationRates=reading(
                "util", types.SimpleNamespace(gpu=0, memory=0)
            ),
            nvmlDeviceGetTemperature=reading("temp", 71),
            nvmlDeviceGetPowerUsage=reading("power", 350_000),
            nvmlDeviceGetEnforcedPowerLimit=reading("limit", limit_mw),
            nvmlDeviceGetMemoryInfo=reading(
                "mem", types.SimpleNamespace(used=60e9, total=total)
            ),
            nvmlDeviceGetClockInfo=reading("clock", 1980),
            nvmlDeviceGetMemoryErrorCounter=reading("ecc", 0),
        )

    def test_a_failed_gpu_reading_is_a_nan_marker_at_its_gpus_tag(self):
        nan = "nan"
        healthy = {
            "system/gpu_util_pct": 0.0,
            "system/gpu_mem_util_pct": 0.0,
            "system/gpu_temp_c": 71.0,
            "system/gpu_power_w": 350.0,
            "system/gpu_power_pct": 50.0,
            "system/gpu_power_limit_w": 700.0,
            "system/gpu_mem_used_bytes": 60e9,
            "system/gpu_mem_used_pct": 75.0,
            "system/gpu_sm_clock_mhz": 1980.0,
            "system/gpu_mem_clock_mhz": 1980.0,
            "system/gpu_ecc_uncorrected": 0.0,
            "system/gpu_ecc_corrected": 0.0,
        }
        unread = dict.fromkeys(healthy, nan)
        ecc = ("system/gpu_ecc_uncorrected", "system/gpu_ecc_corrected")
        every_call = ("util", "temp", "power", "limit", "mem", "clock", "ecc")
        for name, nvml, expected in (
            ("every reading fails", self._fake_nvml(every_call), [unread, unread]),
            (
                "GPU 1 is lost",
                self._fake_nvml([(call, 1) for call in every_call]),
                [healthy, unread],
            ),
            (
                "ECC is unsupported",
                self._fake_nvml(["ecc"]),
                [{**healthy, **dict.fromkeys(ecc, nan)}] * 2,
            ),
            (
                "the power limit fails",
                self._fake_nvml(["limit"]),
                [
                    {
                        **healthy,
                        "system/gpu_power_limit_w": nan,
                        "system/gpu_power_pct": nan,
                    }
                ]
                * 2,
            ),
            (
                "the totals are zero",
                self._fake_nvml(limit_mw=0, total=0),
                [
                    {
                        **healthy,
                        "system/gpu_power_limit_w": 0.0,
                        "system/gpu_power_pct": nan,
                        "system/gpu_mem_used_pct": nan,
                    }
                ]
                * 2,
            ),
        ):
            published = []
            poller = SystemMetricsPoller(
                publish=lambda groups, capture_ms: published.extend(groups)
            )
            poller._nvml_ok = True
            poller._gpu_handles = [0, 1]
            with (
                self.subTest(name),
                mock.patch.object(system_metrics_module, "pynvml", nvml, create=True),
            ):
                poller._poll_once()
                (points,) = published
                got = {}
                for point in points:
                    value = point[3]
                    if math.isnan(value):
                        # Encoded as the worker sends it, the marker reaches the wire.
                        self.assertTrue(
                            math.isnan(wire_module._tuple_to_point(point).value)
                        )
                        value = nan
                    got[point[1], point[4]] = value
                # One point per field and GPU, at that GPU's tag.
                self.assertEqual(
                    got,
                    {
                        (field, str(index)): value
                        for index, fields in enumerate(expected)
                        for field, value in fields.items()
                    },
                )

    def test_nvml_discovery_failure_balances_successful_initialization(self):
        calls = []

        def fail_count():
            calls.append("count")
            raise RuntimeError("driver query failed")

        fake_nvml = types.SimpleNamespace(
            nvmlInit=lambda: calls.append("init"),
            nvmlDeviceGetCount=fail_count,
            nvmlShutdown=lambda: calls.append("shutdown"),
        )
        poller = SystemMetricsPoller(publish=mock.Mock())
        with (
            mock.patch.object(system_metrics_module, "_HAS_NVML", True),
            mock.patch.object(system_metrics_module, "pynvml", fake_nvml, create=True),
            self.assertLogs("kymo", level="WARNING"),
        ):
            poller._init_nvml()

        self.assertEqual(calls, ["init", "count", "shutdown"])
        self.assertFalse(poller._nvml_ok)

    def test_stop_waits_for_an_in_progress_publication(self):
        started = threading.Event()
        release = threading.Event()

        class BlockingPublisher:
            def __init__(self):
                self.values = []

            def __call__(self, groups, capture_ms=None):
                started.set()
                release.wait(timeout=1)
                self.values.append(groups)

        target = BlockingPublisher()
        poller = self._poller(target)
        publish = threading.Thread(target=poller._poll_once)
        poller._thread = publish
        publish.start()
        self.assertTrue(started.wait(timeout=1))

        stopped = threading.Event()
        stop_results = []

        def stop_poller():
            stop_results.append(poller.stop())
            stopped.set()

        stopper = threading.Thread(target=stop_poller)
        stopper.start()
        self.assertFalse(stopped.wait(timeout=0.05))
        release.set()
        publish.join(timeout=1)
        stopper.join(timeout=1)

        self.assertTrue(stopped.is_set())
        self.assertEqual(stop_results, [True])
        self.assertEqual(len(target.values), 1)

    def test_stop_gate_prevents_publication_behind_owner_sentinel(self):
        reached_build = threading.Event()
        release_build = threading.Event()

        class Target:
            def __init__(self):
                self.values = []

            def __call__(self, value, capture_ms=None):
                self.values.append(value)

        def blocked_wall_time():
            reached_build.set()
            release_build.wait(timeout=1)
            return 1.0

        target = Target()
        poller = self._poller(target)
        with mock.patch.object(system_metrics_module.time, "time", blocked_wall_time):
            polling = threading.Thread(target=poller._poll_once)
            poller._thread = polling
            polling.start()
            self.assertTrue(reached_build.wait(timeout=1))

            self.assertTrue(poller.stop(timeout=0.01))
            target(None)  # the owner's shutdown marker
            release_build.set()
            polling.join(timeout=1)

        self.assertEqual(target.values, [None])

    def test_stop_reports_unquiesced_publication_at_its_deadline(self):
        publication_started = threading.Event()
        release_publication = threading.Event()

        class BlockingPublisher:
            def __call__(self, _groups, capture_ms=None):
                publication_started.set()
                release_publication.wait(timeout=1)

        poller = self._poller(BlockingPublisher())
        polling = threading.Thread(target=poller._poll_once)
        poller._thread = polling
        polling.start()
        self.assertTrue(publication_started.wait(timeout=1))
        try:
            with self.assertLogs("kymo", level="WARNING") as logs:
                quiesced = poller.stop(timeout=0.01)
        finally:
            release_publication.set()
            polling.join(timeout=1)

        self.assertFalse(quiesced)
        self.assertIn("publication did not quiesce", "\n".join(logs.output))

    @contextlib.contextmanager
    def _gpu_poll_in_flight(self):
        """A poller with NVML whose GPU poll runs on its thread until release is set or 1 s passes; yields (poller, release, the NVML shutdowns it made)."""
        poll_started, release, shutdown_calls = threading.Event(), threading.Event(), []
        poller = SystemMetricsPoller(publish=mock.Mock())
        poller._nvml_ok = True

        def poll_gpu(_metrics):
            poll_started.set()
            release.wait(timeout=1)

        poller._poll_gpu = poll_gpu
        fake_nvml = types.SimpleNamespace(
            nvmlShutdown=lambda: shutdown_calls.append("shutdown")
        )
        with mock.patch.object(system_metrics_module, "pynvml", fake_nvml, create=True):
            polling = threading.Thread(target=poller._poll_once)
            poller._thread = polling
            polling.start()
            self.assertTrue(poll_started.wait(timeout=1))
            try:
                yield poller, release, shutdown_calls
            finally:
                release.set()
                polling.join(timeout=1)

    def test_stop_waits_for_backend_poll_before_nvml_shutdown(self):
        with self._gpu_poll_in_flight() as (poller, release, shutdown_calls):
            stopped = threading.Event()
            stopper = threading.Thread(target=lambda: (poller.stop(), stopped.set()))
            stopper.start()
            self.assertFalse(stopped.wait(timeout=0.05))
            self.assertEqual(shutdown_calls, [])
            release.set()
            stopper.join(timeout=1)

        self.assertTrue(stopped.is_set())
        self.assertEqual(shutdown_calls, ["shutdown"])

    def test_stop_is_bounded_when_backend_poll_is_wedged(self):
        with (
            mock.patch.object(system_metrics_module, "_STOP_BARRIER_TIMEOUT", 0.05),
            self._gpu_poll_in_flight() as (poller, _, shutdown_calls),
        ):
            started = time.monotonic()
            quiesced = poller.stop()
            elapsed = time.monotonic() - started

        self.assertLess(elapsed, 0.5)
        self.assertTrue(quiesced)
        self.assertEqual(shutdown_calls, [])

    def test_disk_rate_uses_its_own_sample_clock(self):
        counters = [
            types.SimpleNamespace(read_bytes=2_000_000, write_bytes=4_000_000),
            types.SimpleNamespace(read_bytes=4_000_000, write_bytes=8_000_000),
        ]
        poller = SystemMetricsPoller(publish=mock.Mock())
        poller._prev_disk = types.SimpleNamespace(read_bytes=0, write_bytes=0)
        poller._prev_disk_time = 0.0
        metrics = {}
        fake_psutil = types.SimpleNamespace(
            disk_usage=mock.Mock(side_effect=OSError("unavailable")),
            disk_io_counters=mock.Mock(side_effect=counters),
        )
        with (
            mock.patch.object(
                system_metrics_module, "psutil", fake_psutil, create=True
            ),
            mock.patch.object(
                system_metrics_module.time, "monotonic", side_effect=[2.0, 4.0]
            ),
        ):
            poller._poll_disk(metrics)
            poller._poll_disk(metrics)

        self.assertEqual(metrics["system/disk_read_mbps"], 1.0)
        self.assertEqual(metrics["system/disk_write_mbps"], 2.0)

    def test_io_counter_resets_skip_negative_rates_and_rebase(self):
        disk_counters = [
            types.SimpleNamespace(read_bytes=1_000_000, write_bytes=6_000_000),
            types.SimpleNamespace(read_bytes=3_000_000, write_bytes=10_000_000),
        ]
        net_counters = [
            types.SimpleNamespace(bytes_sent=1_000_000, bytes_recv=6_000_000),
            types.SimpleNamespace(bytes_sent=5_000_000, bytes_recv=10_000_000),
        ]
        poller = SystemMetricsPoller(publish=mock.Mock())
        poller._prev_disk = types.SimpleNamespace(
            read_bytes=2_000_000, write_bytes=4_000_000
        )
        poller._prev_disk_time = 0.0
        poller._prev_net = types.SimpleNamespace(
            bytes_sent=2_000_000, bytes_recv=4_000_000
        )
        poller._prev_net_time = 0.0
        fake_psutil = types.SimpleNamespace(
            disk_usage=mock.Mock(side_effect=OSError("unavailable")),
            disk_io_counters=mock.Mock(side_effect=disk_counters),
            net_io_counters=mock.Mock(side_effect=net_counters),
        )

        with (
            mock.patch.object(
                system_metrics_module, "psutil", fake_psutil, create=True
            ),
            mock.patch.object(
                system_metrics_module.time,
                "monotonic",
                side_effect=[2.0, 2.0, 4.0, 4.0],
            ),
        ):
            reset_metrics = {}
            poller._poll_disk(reset_metrics)
            poller._poll_network(reset_metrics)
            self.assertNotIn("system/disk_read_mbps", reset_metrics)
            self.assertEqual(reset_metrics["system/disk_write_mbps"], 1.0)
            self.assertNotIn("system/net_up_mbps", reset_metrics)
            self.assertEqual(reset_metrics["system/net_down_mbps"], 1.0)

            recovered_metrics = {}
            poller._poll_disk(recovered_metrics)
            poller._poll_network(recovered_metrics)

        self.assertEqual(recovered_metrics["system/disk_read_mbps"], 1.0)
        self.assertEqual(recovered_metrics["system/disk_write_mbps"], 2.0)
        self.assertEqual(recovered_metrics["system/net_up_mbps"], 2.0)
        self.assertEqual(recovered_metrics["system/net_down_mbps"], 2.0)

    def test_a_tick_publishes_its_metrics_and_the_captured_console(self):
        # Through the real _publish_queue_items: the metric group, then the text, both at the tick's time.
        target = _ListQueue()
        poller = self._poller(client_module._publish_queue_items)
        with (
            _upload_state(_metric_queue=target),
            _captured(["printed\n"]),
            mock.patch.object(system_metrics_module.time, "time", return_value=1234.5),
        ):
            poller._poll_once()

        self.assertEqual(
            target.items,
            [
                [("numeric_ts", "system/cpu", 1234500, 1.0, 1234500)],
                [("text_ts", "logs/std_out", 1234500, "printed\n", 1234500)],
            ],
        )

    def test_start_starts_the_thread_without_a_metric_backend(self):
        # init() starts the thread whatever system_metrics says, which decides only the metric backends: off, none is initialized or mentioned; on with none available (pynvml and psutil are optional), the console still goes out.
        for metrics in (False, True):
            with self.subTest(metrics=metrics):
                poller = SystemMetricsPoller(
                    publish=mock.Mock(), system_metrics=metrics
                )
                with (
                    mock.patch.multiple(
                        system_metrics_module, _HAS_NVML=False, _HAS_PSUTIL=False
                    ),
                    mock.patch.object(
                        poller, "_init_nvml", wraps=poller._init_nvml
                    ) as nvml,
                    mock.patch.object(
                        poller, "_init_psutil", wraps=poller._init_psutil
                    ) as psutil,
                    mock.patch.object(
                        system_metrics_module.threading, "Thread"
                    ) as thread,
                    self.assertLogs("kymo", level="INFO")
                    if metrics
                    else self.assertNoLogs("kymo", level="INFO") as logs,
                ):
                    poller.start()
                if metrics:
                    self.assertIn(
                        "the console is still uploaded", "\n".join(logs.output)
                    )
                self.assertEqual((nvml.called, psutil.called), (metrics, metrics))
                thread.assert_called_once_with(
                    target=poller._poll_loop, name="kymo-poller", daemon=True
                )
                thread.return_value.start.assert_called_once_with()

    def test_a_tick_without_metrics_publishes_only_the_captured_text(self):
        # A tick publishes just the text, and with no output it puts, charges and drains nothing.
        target = _ListQueue()
        poller = SystemMetricsPoller(
            publish=client_module._publish_queue_items, system_metrics=False
        )
        with (
            _upload_state(_metric_queue=target),
            _captured(["printed\n"]),
            mock.patch.object(system_metrics_module.time, "time", return_value=5.0),
        ):
            poller._poll_once()
        self.assertEqual(
            target.items, [[("text_ts", "logs/std_out", 5000, "printed\n", 5000)]]
        )
        target, status = _ListQueue(), _QueueStatus()
        with (
            _upload_state(_metric_queue=target, _queue_status=status),
            _captured(),
            mock.patch.object(
                capture_module, "drain_buffers", wraps=capture_module.drain_buffers
            ) as drained,
        ):
            poller._poll_once()
        self.assertEqual((target.items, status.value), ([], 0))
        drained.assert_not_called()

    def test_a_started_poller_uploads_the_console_without_metrics(self):
        # The thread itself, not a hand-called tick: started without system metrics, it publishes what was printed.
        target = _ListQueue()
        poller = SystemMetricsPoller(
            publish=client_module._publish_queue_items,
            poll_interval=0.01,
            system_metrics=False,
        )
        with _upload_state(_metric_queue=target), _captured(["printed\n"]):
            poller.start()
            try:
                deadline = time.monotonic() + 5
                while not target.items and time.monotonic() < deadline:
                    time.sleep(0.01)
            finally:
                self.assertTrue(poller.stop())
        self.assertEqual(
            [point[3] for item in target.items for point in item], ["printed\n"]
        )

    def test_a_tick_after_stop_leaves_the_text_to_the_final_drain(self):
        # No publication begins once stop() has; the shutdown's last drain then takes all the text the tick left.
        target = _ListQueue()
        poller = self._poller(client_module._publish_queue_items)
        text = ["tail one\n", "tail two\n"]
        with (
            _upload_state(_metric_queue=target),
            _captured(text),
        ):
            self.assertTrue(poller.stop())
            poller._poll_once()
            self.assertEqual(target.items, [])
            self.assertEqual(list(capture_module._stdout_buf), text)
            client_module._publish_queue_items([], capture_ms=2000, final=True)

        self.assertEqual(
            target.items, [[("text_ts", "logs/std_out", 2000, "".join(text), 2000)]]
        )

    def test_stop_waits_for_a_drain_in_progress(self):
        # The tick drains inside the publication gate, so stop() waits for the drain as for any publication. Its 1 s budget and the drain's wait are raised so a stalled runner cannot end either first.
        started, release, stopped = (
            threading.Event(),
            threading.Event(),
            threading.Event(),
        )
        results = []

        def slow_drain(final=False, timeout=None):
            started.set()
            release.wait(timeout=10)
            return ("drained\n", "")

        target = _ListQueue()
        poller = self._poller(client_module._publish_queue_items)
        with (
            _upload_state(_metric_queue=target),
            _captured(["x"]),
            mock.patch.object(capture_module, "drain_buffers", side_effect=slow_drain),
            mock.patch.object(system_metrics_module.time, "time", return_value=3.0),
            mock.patch.object(system_metrics_module, "_STOP_BARRIER_TIMEOUT", 10.0),
        ):
            polling = threading.Thread(target=poller._poll_once)
            poller._thread = polling
            polling.start()
            self.assertTrue(started.wait(timeout=1))

            def stop_poller():
                results.append(poller.stop())
                stopped.set()

            stopper = threading.Thread(target=stop_poller)
            stopper.start()
            # stop() has begun, and waits for the drain.
            self.assertTrue(poller._stop.wait(timeout=1))
            self.assertFalse(stopped.wait(timeout=0.05))
            release.set()
            polling.join(timeout=10)
            stopper.join(timeout=10)

        self.assertTrue(stopped.is_set())
        self.assertEqual(results, [True])
        self.assertEqual(
            target.items,
            [
                [("numeric_ts", "system/cpu", 3000, 1.0, 3000)],
                [("text_ts", "logs/std_out", 3000, "drained\n", 3000)],
            ],
        )

    def test_a_charge_failure_on_the_poller_thread_leaves_the_text_at_the_end_of_uncharged(
        self,
    ):
        # The tick's text that no charge let out waits behind what _uncharged already holds, the loop carries on, and the main thread's next publication puts it first.
        old = [numeric(1)]
        text = [("text_ts", "logs/std_out", 4000, "printed\n", 4000)]
        target, status = _ListQueue(), _QueueStatus()
        poller = SystemMetricsPoller(
            publish=client_module._publish_queue_items, system_metrics=False
        )
        with (
            _upload_state(
                _metric_queue=target,
                _queue_status=status,
                _uncharged=deque([old]),
            ),
            mock.patch.object(client_module, "_STATUS_LOCK_TIMEOUT", 0.0),
            _captured(["printed\n"]),
            mock.patch.object(system_metrics_module.time, "time", return_value=4.0),
            mock.patch.object(
                poller._stop, "wait", side_effect=[False, False, True]
            ) as ticked,
            self.assertLogs("kymo", level="WARNING"),
        ):
            with status.lock:
                looping = threading.Thread(target=poller._poll_loop)
                looping.start()
                looping.join(timeout=5)
            self.assertFalse(looping.is_alive())
            self.assertEqual(ticked.call_count, 3)
            self.assertEqual(list(client_module._uncharged), [old, text])
            self.assertEqual(target.items, [])
            client_module._publish_queue_items([[numeric(2)]])

        self.assertEqual(target.items, [old, text, [numeric(2)]])
        self.assertEqual(status.value, 3)
        self.assertEqual(list(client_module._uncharged), [])


class _FakeCall:
    def __init__(self, acks=()):
        self.acks = list(acks)
        self.cancelled = False

    def __iter__(self):
        for cumulative in self.acks:
            yield types.SimpleNamespace(points_acked=cumulative)

    def cancel(self):
        self.cancelled = True


class _FakeStub:
    def __init__(self, call):
        self.call = call
        self.requests = None

    def IngestMetricsBidi(self, requests, timeout=None):
        self.requests = requests
        return self.call


class AttemptIsolationTests(unittest.TestCase):
    def test_ack_pump_preserves_ack_then_terminal_order(self):
        stream = client_module._BidiStream(_FakeStub(_FakeCall([3, 7])), 2)
        stream._pump.join(timeout=1)
        self.assertFalse(stream._pump.is_alive())
        self.assertEqual(
            stream.poll_acks(),
            [("ack", 3), ("ack", 7), ("done", None)],
        )

    def test_cancel_marks_transport_and_attempt_queues_are_fresh(self):
        first_call = _FakeCall()
        first = client_module._BidiStream(_FakeStub(first_call), 2)
        second = client_module._BidiStream(_FakeStub(_FakeCall()), 2)
        self.assertIsNot(first._feed_q, second._feed_q)
        self.assertIsNot(first._ack_q, second._ack_q)

        first.cancel()
        self.assertTrue(_wait_for(lambda: first_call.cancelled, timeout=1))

    def test_cancelled_generator_drops_already_queued_batches(self):
        stub = _FakeStub(_FakeCall())
        stream = client_module._BidiStream(stub, 2)
        self.assertTrue(stream.feed(object()))
        stream.cancel()
        with self.assertRaises(StopIteration):
            next(stub.requests)

    def test_cancel_wakes_generator_blocked_on_empty_queue(self):
        stub = _FakeStub(_FakeCall())
        stream = client_module._BidiStream(stub, 2)
        stopped = threading.Event()

        def consume():
            try:
                next(stub.requests)
            except StopIteration:
                stopped.set()

        consumer = threading.Thread(target=consume)
        consumer.start()
        self.assertTrue(consumer.is_alive())
        stream.cancel()
        consumer.join(timeout=1)
        self.assertTrue(stopped.is_set())

    def test_half_close_retries_without_dropping_a_full_feed_queue(self):
        stub = _FakeStub(_FakeCall())
        stream = client_module._BidiStream(stub, 1)
        batch = object()
        self.assertTrue(stream.feed(batch))
        self.assertFalse(stream.half_close())
        self.assertFalse(stream._dead.is_set())
        self.assertIs(next(stub.requests), batch)
        self.assertTrue(stream.half_close())
        with self.assertRaises(StopIteration):
            next(stub.requests)


class ConnectionTests(unittest.TestCase):
    def test_owner_connect_retry_delay_is_stateful_and_capped(self):
        delay = client_module._CONNECT_INITIAL_DELAY
        observed = []
        for _ in range(6):
            observed.append(delay)
            delay = client_module._next_connect_retry_delay(delay)

        self.assertEqual(observed, [0.5, 1.0, 2.0, 4.0, 5.0, 5.0])

    def test_disk_recovery_backoff_is_jittered_without_a_cap_cliff(self):
        low_rng = mock.Mock(random=mock.Mock(return_value=0.0))
        high_rng = mock.Mock(random=mock.Mock(return_value=1.0))

        self.assertEqual(client_module._recovery_retry_delay(1, low_rng), 0.25)
        self.assertEqual(client_module._recovery_retry_delay(1, high_rng), 0.5)
        self.assertEqual(client_module._recovery_retry_delay(20, low_rng), 30.0)
        self.assertEqual(client_module._recovery_retry_delay(20, high_rng), 60.0)

    def test_ordinary_retry_jitter_spreads_the_five_second_cap(self):
        low_rng = mock.Mock(random=mock.Mock(return_value=0.0))
        high_rng = mock.Mock(random=mock.Mock(return_value=1.0))

        self.assertEqual(client_module._equal_jitter_delay(5.0, low_rng), 2.5)
        self.assertEqual(client_module._equal_jitter_delay(5.0, high_rng), 5.0)

    def test_connect_attempt_keeps_its_full_window_across_nonblocking_polls(self):
        channel = mock.Mock()
        ready = mock.Mock()
        ready.done.side_effect = [False, True]
        stub = object()

        with (
            mock.patch.object(
                client_module.grpc, "insecure_channel", return_value=channel
            ),
            mock.patch.object(
                client_module.grpc, "channel_ready_future", return_value=ready
            ),
            mock.patch.object(
                client_module.kymo_pb2_grpc, "KymoStub", return_value=stub
            ),
        ):
            attempt = client_module._ConnectionAttempt("unused")
            self.assertIs(attempt.poll(), client_module._CONNECT_PENDING)
            result = attempt.poll()

        self.assertEqual(result, (channel, stub))
        ready.result.assert_called_once_with()
        channel.close.assert_not_called()

    def test_connect_timeout_closes_channel(self):
        channel = mock.Mock()
        ready = mock.Mock()
        with (
            mock.patch.object(
                client_module.grpc, "insecure_channel", return_value=channel
            ),
            mock.patch.object(
                client_module.grpc, "channel_ready_future", return_value=ready
            ),
            mock.patch.object(client_module.time, "monotonic", side_effect=[0.0, 5.0]),
        ):
            attempt = client_module._ConnectionAttempt("unused")
            result = attempt.poll()

        self.assertEqual(result, (None, None))
        ready.done.assert_not_called()
        channel.close.assert_called_once_with()

    def test_connect_observes_shutdown_deadline_between_polls(self):
        channel = mock.Mock()
        ready = mock.Mock()
        with (
            mock.patch.object(
                client_module.grpc, "insecure_channel", return_value=channel
            ),
            mock.patch.object(
                client_module.grpc, "channel_ready_future", return_value=ready
            ),
        ):
            attempt = client_module._ConnectionAttempt("unused")
            result = attempt.poll(deadline_fn=lambda: True)

        self.assertEqual(result, (None, None))
        ready.done.assert_not_called()
        channel.close.assert_called_once_with()


class _Process:
    """An upload worker process, alive or, with exitcode, exited; it counts the SIGTERMs and SIGKILLs it gets, and a SIGKILL ends it."""

    def __init__(self, exitcode=None):
        self.exitcode = exitcode
        self.terminated = self.killed = 0

    def is_alive(self):
        return self.exitcode is None

    def join(self, timeout=None):
        pass

    def terminate(self):
        self.terminated += 1

    def kill(self):
        self.killed += 1
        self.exitcode = -9


class _ControlStdin(io.BytesIO):
    """A control process's stdin, for a Popen stand-in (_run_control_rpc): what was written, kept once it is closed."""

    written = b""

    def close(self):
        if not self.closed:
            self.written = self.getvalue()
        super().close()

    def request(self) -> tuple:
        """The exit code of the TerminateRun written to it, and the pid of the control process it was to send after."""
        payload = json.loads(self.written)
        request = client_module.kymo_pb2.TerminateRunRequest.FromString(
            base64.b64decode(payload["request"])
        )
        return request.exit_code, payload["after"]


class _QueueStatus:
    def __init__(self, value=0):
        self.value = value
        self.lock = threading.Lock()

    def get_lock(self):
        return self.lock

    def get_obj(self):
        return self


def _upload_state(**patches):
    """Patch kymo.client to a run this process initialized, its worker running, with nothing queued, held back or pending, nor log() overhead recorded, patches on top: what log(), wait_for_upload() and a publication read."""
    return mock.patch.multiple(
        client_module,
        **{
            "_is_initialized": True,
            "_init_pid": os.getpid(),
            "_metric_queue": _ListQueue(),
            "_queue_status": _QueueStatus(),
            "_upload_failure": _QueueStatus(),
            "_upload_spooled": _QueueStatus(),
            "_upload_process": _Process(),
            "_held_back": deque(),
            "_uncharged": deque(),
            "_pending": deque(),
            "_pending_incomplete": False,
            "_handed_off": False,
            "_unproven_points": 0,
            "_last_log_duration_ms": 0.0,
            "_last_capture_step": None,
            **patches,
        },
    )


def _drain_off_main(final=False):
    """Run client._drain_capture_points(1, final) on another thread: (a list that gets its points, an Event set once it returns)."""
    got, finished = [], threading.Event()

    def drain():
        got.append(client_module._drain_capture_points(1, final=final))
        finished.set()

    threading.Thread(target=drain, daemon=True).start()
    return got, finished


class _AskedLock:
    """A capture lock's stand-in that records the timeouts drains ask for it with (asked) and sets reached when one does."""

    def __init__(self, real):
        self.real, self.asked, self.reached = real, [], threading.Event()

    def _is_owned(self):
        return self.real._is_owned()

    def acquire(self, timeout=-1):
        self.asked.append(timeout)
        self.reached.set()
        return self.real.acquire(timeout=timeout)

    def release(self):
        self.real.release()


def _captured(stdout=(), stderr=()):
    """Patch kymo._capture to hold these chunks captured, none dropped."""
    return mock.patch.multiple(
        capture_module,
        _stdout_buf=deque(stdout),
        _stdout_stats={"size": sum(map(len, stdout)), "dropped": 0},
        _stderr_buf=deque(stderr),
        _stderr_stats={"size": sum(map(len, stderr)), "dropped": 0},
    )


def _run_script_file(script: str, **env) -> subprocess.CompletedProcess:
    """Run script from a file, which spawn children import as __main__, with env added to the environment; it and its children import this checkout's kymo, installed or not."""
    path_entries = [os.path.dirname(os.path.abspath(__file__))]
    if os.environ.get("PYTHONPATH"):
        path_entries.append(os.environ["PYTHONPATH"])
    with tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, "script.py")
        with open(path, "w") as file:
            file.write(script)
        return subprocess.run(
            [sys.executable, path],
            env={**os.environ, "PYTHONPATH": os.pathsep.join(path_entries), **env},
            capture_output=True,
            text=True,
            timeout=120,
        )


@contextlib.contextmanager
def _kymo_handles(originals: dict):
    """Install kymo's signal handler for each signal in originals, mapped to the handler kymo found at init; restore everything after."""
    installed = client_module._signal_handler
    previous = {signum: signal.signal(signum, installed) for signum in originals}
    try:
        # The handler records when it saw a Ctrl-C or a SIGTERM, which later tests' exits must not take for theirs.
        with (
            mock.patch.dict(client_module._original_signals, originals),
            mock.patch.multiple(
                client_module,
                _interrupted_at=client_module._interrupted_at,
                _terminated_at=client_module._terminated_at,
            ),
        ):
            yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        client_module._deferred_signals.clear()


def _unversioned_gallery(step: int, gallery: list) -> tuple:
    """A gallery as an old server's writer queues it, without logical versions."""
    (item,) = client_module._snapshot_rich_queue_items(
        [("cdn_batch", "samples", step, gallery)]
    )
    return item


def _versioned_gallery(step: int, gallery: list) -> tuple:
    """A gallery as a current server's writer queues it, its two versions' sequences 2 * step - 1 and 2 * step: only versioned rich records replay while a lane goes live again."""
    version = (1 << 32) | (2 * step - 1)
    (item,) = client_module._snapshot_rich_queue_items(
        [("cdn_batch_mutation", "samples", step, gallery, step, version, version + 1)]
    )
    return item


def _charged_queue(*items):
    """A queue.Queue holding items, and a backlog charged with their points, as log() charges them."""
    source, status = queue.Queue(), _QueueStatus()
    for item in items:
        if item is not None:
            status.value += client_module._queue_item_size(item)
        source.put(item)
    return source, status


def _real_queue(case, maxsize: int = 0) -> multiprocessing.queues.Queue:
    """A multiprocessing.Queue, closed and its feeder thread joined once case ends."""
    source = multiprocessing.Queue(maxsize)
    case.addCleanup(source.join_thread)
    case.addCleanup(source.close)
    return source


def _at(deadline: float):
    """A fixed time.monotonic() deadline, given as the shutdown's are, as a function."""
    return lambda: deadline


def _wait_for(condition, timeout=5.0):
    """Poll condition until it holds or timeout seconds pass; return it."""
    deadline = time.monotonic() + timeout
    while not condition() and time.monotonic() < deadline:
        time.sleep(0.005)
    return condition()


def _worker_past_its_deadline(source, points=0, **kwargs):
    """Run the upload worker of run p/r over source, holding points, with its deadline already past, so it reads source only in its deadline drain; return its exit code (0 if it returned) and its accounting."""
    deadline = _QueueStatus(time.monotonic() - 1)
    status = _QueueStatus(points)
    with tempfile.TemporaryDirectory() as spool_dir:
        try:
            worker_module._upload_worker(
                "unused",
                "p",
                "r",
                source,
                status,
                shutdown_deadline=deadline,
                spool_path=os.path.join(spool_dir, "w.mkspool"),
                **kwargs,
            )
        except SystemExit as error:
            return error.code, status
    return 0, status


def _worker_thread(case, source, status, spool_path, *, name=None, **kwargs):
    """The upload worker of run p/r over source, with no server, spooling to spool_path, started on a daemon thread; the thread's code is the worker's exit code once it has exited by itself. A worker still running 10 s after case ends fails it, so no hang goes unseen."""

    def run():
        try:
            worker_module._upload_worker(
                "unused", "p", "r", source, status, spool_path=spool_path, **kwargs
            )
            thread.code = 0
        except SystemExit as error:
            thread.code = error.code

    def ended():
        thread.join(timeout=10)
        if thread.is_alive():
            case.fail(f"the upload worker on {thread.name} never exited")

    thread = threading.Thread(target=run, name=name, daemon=True)
    thread.code = None
    thread.start()
    case.addCleanup(ended)
    return thread


def _held_encode():
    """An _encode_image stand-in that sets .encoding and returns once .release is set."""

    def encode(image):
        encode.encoding.set()
        encode.release.wait(timeout=10)
        return (b"encoded", "png")

    encode.encoding, encode.release = threading.Event(), threading.Event()
    return encode


class WorkerResponsivenessTests(unittest.TestCase):
    class _HttpClient:
        def close(self):
            pass

    @staticmethod
    def _immediate_stream_class(on_feed):
        class ImmediateStream:
            def __init__(self, _stub, _feed_cap):
                self.events = queue.Queue()
                self.cumulative = 0
                self.half_closed = False

            def feed_has_room(self):
                return not self.half_closed

            def feed(self, batch):
                if self.half_closed:
                    return False
                self.cumulative += len(batch.points)
                on_feed(batch)
                self.events.put(("ack", self.cumulative))
                return True

            def poll_acks(self):
                events = []
                while True:
                    try:
                        events.append(self.events.get_nowait())
                    except queue.Empty:
                        return events

            def half_close(self):
                if not self.half_closed:
                    self.half_closed = True
                    self.events.put(("done", None))
                return True

            def cancel(self):
                self.half_closed = True

        return ImmediateStream

    class _NeverConnects:
        @staticmethod
        def poll(deadline_fn=None):
            return client_module._CONNECT_PENDING

        @staticmethod
        def cancel():
            pass

    def test_a_gallery_encoding_at_the_spill_end_is_cut_and_reported(self):
        # The deadline spill waits for the encode in flight only until the spill end, read again as it waits, and not at all once that has passed: the worker reports the gallery lost and exits by itself, its spool readable, rather than wait for an encode that outlives its kill.
        for ahead in (-1.0, 0.15):
            with self.subTest(spill_end_in=ahead):
                encode = _held_encode()
                image = Image(np.zeros((2, 2, 3), dtype=np.uint8))
                source, status = _charged_queue(
                    [numeric(1)], _unversioned_gallery(2, [image])
                )
                try:
                    with (
                        tempfile.TemporaryDirectory() as spool_dir,
                        mock.patch.object(worker_module, "_signal_deadline", math.inf),
                        mock.patch.object(worker_module, "_spill_by", math.inf),
                        mock.patch.object(client_module, "_encode_image", encode),
                        mock.patch.object(
                            client_module,
                            "_ConnectionAttempt",
                            return_value=self._NeverConnects(),
                        ),
                        self.assertLogs("kymo", level="ERROR") as logs,
                    ):
                        worker = _worker_thread(
                            self, source, status, os.path.join(spool_dir, "w.mkspool")
                        )
                        self.assertTrue(encode.encoding.wait(timeout=5))
                        started = time.monotonic()
                        worker_module._spill_by = started + ahead
                        worker_module._signal_deadline = started - 1
                        worker.join(timeout=5)
                        elapsed = time.monotonic() - started
                        records = (
                            list(read_spool(os.path.join(spool_dir, "w.mkspool"))[1])
                            if ahead > 0
                            else []
                        )
                finally:
                    encode.release.set()
                self.assertFalse(worker.is_alive())
                self.assertLess(elapsed, max(ahead, 0) + 0.5)
                self.assertEqual(worker.code, client_module._WORKER_EXIT_SPOOL_FAILED)
                cut = 2 if ahead < 0 else 1
                self.assertEqual(
                    [record[2] for record in records], [] if cut == 2 else [1]
                )
                self.assertEqual(status.value, cut)
                self.assertIn(f"{cut} past the spill deadline", "\n".join(logs.output))

    def test_a_raw_gallery_spilled_after_the_delivery_end_is_encoded_until_the_spill_end(
        self,
    ):
        # A gallery the deadline drain reads raw is encoded on a helper until the spill end the parent's shutdown published (spill_deadline), which cuts it and the rest, so the worker exits by itself with its report, the spool readable; what it never read is the parent's salvage's.
        encode = _held_encode()
        image = Image(np.zeros((2, 2, 3), dtype=np.uint8))
        source, status = _charged_queue(
            [numeric(1)], _unversioned_gallery(2, [image]), [numeric(3)], None
        )
        delivered_by = _QueueStatus(time.monotonic() - 1)
        spill_by = _QueueStatus(time.monotonic() + 0.3)
        try:
            with (
                tempfile.TemporaryDirectory() as spool_dir,
                mock.patch.object(client_module, "_encode_image", encode),
                self.assertLogs("kymo", level="ERROR") as logs,
            ):
                worker = _worker_thread(
                    self,
                    source,
                    status,
                    os.path.join(spool_dir, "w.mkspool"),
                    shutdown_deadline=delivered_by,
                    spill_deadline=spill_by,
                )
                worker.join(timeout=5)
                ended = time.monotonic()
                records = list(read_spool(os.path.join(spool_dir, "w.mkspool"))[1])
        finally:
            encode.release.set()
        self.assertTrue(encode.encoding.is_set())
        self.assertFalse(worker.is_alive())
        self.assertLess(ended - spill_by.value, 0.5)
        self.assertEqual(worker.code, client_module._WORKER_EXIT_SPOOL_FAILED)
        self.assertEqual([record[2] for record in records], [1])
        output = "\n".join(logs.output)
        self.assertIn("1 past the spill deadline, 0 never read", output)
        self.assertIn("with 1 point(s) unread; parent salvage is required", output)
        self.assertEqual(status.value, 2)

    def test_what_a_spill_wrote_is_on_disk_while_a_gallery_after_it_encodes(self):
        # A raw gallery the deadline spill encodes comes after a flush: the records the spill wrote before it are on disk while the encode runs, so a kill during a slow encode loses none of them with the writer's buffer.
        encode = _held_encode()
        image = Image(np.zeros((2, 2, 3), dtype=np.uint8))
        source, status = _charged_queue(
            [numeric(1), ("cdn_batch", "samples", 2, [image])], None
        )
        delivered_by = _QueueStatus(time.monotonic() - 1)
        spill_by = _QueueStatus(time.monotonic() + 30)
        try:
            with (
                tempfile.TemporaryDirectory() as spool_dir,
                mock.patch.object(client_module, "_encode_image", encode),
                self.assertLogs("kymo", level="WARNING"),
            ):
                path = os.path.join(spool_dir, "w.mkspool")
                worker = _worker_thread(
                    self,
                    source,
                    status,
                    path,
                    shutdown_deadline=delivered_by,
                    spill_deadline=spill_by,
                )
                self.assertTrue(encode.encoding.wait(timeout=5))
                while_encoding = [record[2] for record in read_spool(path)[1]]
                encode.release.set()
                worker.join(timeout=5)
                records = [record[2] for record in read_spool(path)[1]]
        finally:
            encode.release.set()
        self.assertEqual(while_encoding, [1])
        self.assertEqual((worker.code, records, status.value), (0, [1, 2], 0))

    def test_points_never_read_are_reported_lost_when_no_salvage_follows(self):
        # The drain stops at the spill end with groups still queued. Once the parent has exited, or has handed the run off, no salvage follows: the worker reports them lost with those it cut.
        # An owner that has exited.
        owner = subprocess.Popen([sys.executable, "-c", ""], close_fds=False)
        owner.wait()
        source, status = _charged_queue(
            *[[numeric(index)] * 1000 for index in range(5)]
        )
        written = []
        spill_tuple = client_module._spill_tuple

        def spill_until_the_end_passes(spool, point):
            written.append(point)
            if len(written) == 1500:
                worker_module._spill_by = 1.0
            return spill_tuple(spool, point)

        with (
            tempfile.TemporaryDirectory() as spool_dir,
            mock.patch.object(worker_module, "_signal_deadline", 1.0),
            mock.patch.object(worker_module, "_spill_by", time.monotonic() + 3600),
            mock.patch.object(
                client_module, "_spill_tuple", spill_until_the_end_passes
            ),
            self.assertLogs("kymo", level="ERROR") as logs,
        ):
            worker = _worker_thread(
                self,
                source,
                status,
                os.path.join(spool_dir, "w.mkspool"),
                owner_pid=owner.pid,
            )
            worker.join(timeout=10)
            spooled = len(list(read_spool(os.path.join(spool_dir, "w.mkspool"))[1]))
        self.assertEqual(worker.code, client_module._WORKER_EXIT_SPOOL_FAILED)
        # The first group spilled whole; the second, whose spill end passed as its 500th point was written, cut at the next check of that end, at its 512th point; the last three never read. The spool and the report account for every point.
        self.assertEqual((spooled, status.value), (1512, 3488))
        self.assertIn(
            "3488 points reached neither the server nor the spool (0 unspoolable "
            "(disk full?), 0 unencodable, 0 undecodable, 488 past the spill "
            "deadline, 3000 never read) — LOST",
            "\n".join(logs.output),
        )

    def test_a_deadline_spill_writes_the_rich_lane_to_a_segment_of_its_own(self):
        # A record torn by a kill the spill end cannot foresee then costs only the rich segment, never the numeric records sealed before it, which kymo.sync would quarantine with it: whether the gallery is in the rich lane at the deadline or the spill reads it from the producer queue behind numeric points.
        for in_lane in (True, False):
            source, status = _charged_queue(
                [numeric(1), numeric(2)],
                _unversioned_gallery(3, [Resource(b"x" * 1000, "a.bin")]),
                *([] if in_lane else [None]),
            )
            released = _QueueStatus()
            deadline = _QueueStatus(0.0 if in_lane else time.monotonic())
            with (
                self.subTest(in_lane=in_lane),
                tempfile.TemporaryDirectory() as spool_dir,
                mock.patch.object(
                    client_module,
                    "_ConnectionAttempt",
                    return_value=self._NeverConnects(),
                ),
                self.assertLogs("kymo", level="ERROR"),
            ):
                worker = _worker_thread(
                    self,
                    source,
                    status,
                    os.path.join(spool_dir, "w.mkspool"),
                    shutdown_deadline=deadline,
                    galleries_released=released,
                )
                if in_lane:
                    # Once the gallery is encoded, both lanes hold work in RAM.
                    self.assertTrue(_wait_for(lambda: released.value >= 1))
                    deadline.value = time.monotonic()
                    source.put(None)
                worker.join(timeout=5)
                # In the spool's own sequence: the worker's first segment, then those it rotated to, named by time_ns (mtimes can tie or invert).
                first = os.path.join(spool_dir, "w.mkspool")
                segments = sorted(
                    (os.path.join(spool_dir, name) for name in os.listdir(spool_dir)),
                    key=lambda path: (path != first, path),
                )
                kinds = [
                    [record[0] for record in read_spool(path)[1]] for path in segments
                ]
                self.assertEqual(worker.code, 0)
                self.assertEqual(status.value, 0)
                self.assertEqual(
                    kinds, [["numeric_ts", "numeric_ts"], ["cdn_batch_encoded"]]
                )

    def test_a_queue_that_can_no_longer_be_read_ends_the_worker_as_a_drain_cut_short(
        self,
    ):
        # A message's frame is malformed, or a whole message will not unpickle (a MemoryError under RLIMIT_AS, say): the worker reads the queue no more, delivers what it holds and exits as from a drain cut short, leaving the queue to the parent's salvage, rather than let the read's error unwind it past its spool and report; it reports a message it got in part.
        whole = pickle.dumps([numeric(1)], protocol=pickle.DEFAULT_PROTOCOL)
        unpicklable = b"\x80\x05not a pickle."
        for after, in_part in (
            ((-2).to_bytes(4, "big", signed=True), ["4"]),
            (len(unpicklable).to_bytes(4, "big") + unpicklable, []),
        ):
            source = _real_queue(self)
            for frame in (len(whole).to_bytes(4, "big") + whole, after):
                source._sem.acquire()
                os.write(source._writer.fileno(), frame)
            status = _QueueStatus(2)
            delivered = []
            immediate = self._immediate_stream_class(
                lambda batch: delivered.extend(point.step for point in batch.points)
            )
            with (
                self.subTest(in_part=bool(in_part)),
                tempfile.TemporaryDirectory() as spool_dir,
                _connected(immediate),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.001),
                self.assertLogs("kymo", level="ERROR") as logs,
                self.assertRaises(SystemExit) as exited,
            ):
                worker_module._upload_worker(
                    "unused",
                    "p",
                    "r",
                    source,
                    status,
                    spool_path=os.path.join(spool_dir, "lost-queue.mkspool"),
                )
            self.assertEqual(
                exited.exception.code, client_module._WORKER_EXIT_DRAIN_INCOMPLETE
            )
            self.assertEqual((delivered, status.value), ([1], 1))
            output = "\n".join(logs.output)
            self.assertIn("upload queue can no longer be read", output)
            self.assertIn("1 point(s) unread; parent salvage is required", output)
            self.assertEqual(
                re.findall(
                    r"a queued group arrived only in part \((\d+) bytes\)", output
                ),
                in_part,
            )

    def test_quiet_local_worker_exits_without_connecting_or_ensuring(self):
        source, status = _charged_queue(None)

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                mock.patch.object(
                    client_module,
                    "_ConnectionAttempt",
                    side_effect=AssertionError("empty local worker connected"),
                ) as connect,
                mock.patch(
                    "kymo._local_runtime.ensure_local_endpoint",
                    side_effect=AssertionError("empty local worker invoked ensure"),
                ) as ensure,
            ):
                worker_module._upload_worker(
                    "unused",
                    "p",
                    "r",
                    source,
                    status,
                    spool_path=os.path.join(spool_dir, "quiet-local.mkspool"),
                    local_endpoint_config=endpoint_payload(),
                )

        connect.assert_not_called()
        ensure.assert_not_called()

    def test_pending_local_item_opens_the_authenticated_connector(self):
        source, status = _charged_queue([numeric(1)], None)
        seen_endpoints = []

        def connect(_address, *, local_endpoint):
            seen_endpoints.append(local_endpoint)
            return _ImmediateConnectionAttempt(_Channel(), object())

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                mock.patch.object(
                    client_module, "_ConnectionAttempt", side_effect=connect
                ),
                mock.patch.object(
                    client_module,
                    "_BidiStream",
                    self._immediate_stream_class(lambda _batch: None),
                ),
            ):
                worker_module._upload_worker(
                    "unused",
                    "p",
                    "r",
                    source,
                    status,
                    spool_path=os.path.join(spool_dir, "pending-local.mkspool"),
                    local_endpoint_config=endpoint_payload(),
                )

        self.assertEqual(
            seen_endpoints, [endpoint_from_worker_config(endpoint_payload())]
        )
        self.assertEqual(status.value, 0)

    def test_broken_quiet_local_stream_waits_for_new_delivery_before_ensure(self):
        source = queue.Queue()
        source.put([numeric(1)])
        status = _QueueStatus(2)
        first_fed = threading.Event()
        second_fed = threading.Event()
        ensure_called = threading.Event()
        stream_count = 0

        class LocalStream(self._immediate_stream_class(lambda _batch: None)):
            def __init__(self, stub, feed_cap):
                nonlocal stream_count
                super().__init__(stub, feed_cap)
                stream_count += 1
                self.number = stream_count

            def feed(self, batch):
                self.events.put(("ack", len(batch.points)))
                if self.number == 1:
                    first_fed.set()
                    self.events.put(("done", None))
                else:
                    second_fed.set()
                return True

        endpoint = endpoint_from_worker_config(endpoint_payload())

        def ensure(**_kwargs):
            ensure_called.set()
            return endpoint

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                _connected(LocalStream),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.005),
                mock.patch(
                    "kymo._local_runtime.ensure_local_endpoint",
                    side_effect=ensure,
                ),
            ):
                worker = _worker_thread(
                    self,
                    source,
                    status,
                    os.path.join(spool_dir, "broken-quiet-local.mkspool"),
                    local_endpoint_config=endpoint_payload(),
                )
                self.assertTrue(first_fed.wait(timeout=1))
                time.sleep(0.05)
                self.assertFalse(ensure_called.is_set())

                source.put([numeric(2)])
                self.assertTrue(ensure_called.wait(timeout=1))
                self.assertTrue(second_fed.wait(timeout=1))
                source.put(None)
                worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(status.value, 0)

    def test_rich_only_spool_wakes_local_runtime_to_replay_it(self):
        source = queue.Queue()
        source.put(
            [
                (
                    "metadata_batch_mutation",
                    "info/run_info",
                    0,
                    Metadata({"model": "test"}),
                    1,
                    (1 << 32) | 1,
                )
            ]
        )
        status = _QueueStatus(1)
        spooled = threading.Event()
        original_spill_tuple = client_module._spill_tuple

        def fail_upload(*_args, **_kwargs):
            raise RuntimeError("local upload unavailable")

        def spill_tuple(writer, item):
            result = original_spill_tuple(writer, item)
            spooled.set()
            return result

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                _connected(self._immediate_stream_class(lambda _batch: None)),
                mock.patch.object(client_module, "_CDN_MAX_ATTEMPTS", 1),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.005),
                mock.patch.object(
                    client_module, "_upload_to_cdn", side_effect=fail_upload
                ),
                mock.patch.object(
                    client_module, "_spill_tuple", side_effect=spill_tuple
                ),
                mock.patch(
                    "kymo._local_runtime.grpc_channel",
                    return_value=_Channel(),
                ),
                mock.patch.object(
                    client_module.kymo_pb2_grpc,
                    "KymoStub",
                    return_value=object(),
                ),
                mock.patch(
                    "kymo._local_runtime.http_client",
                    return_value=self._HttpClient(),
                ),
                mock.patch(
                    "kymo._local_runtime.ensure_local_endpoint",
                    side_effect=RuntimeError("local runtime stopped"),
                ) as ensure,
            ):
                worker = _worker_thread(
                    self,
                    source,
                    status,
                    os.path.join(spool_dir, "rich-only-local.mkspool"),
                    local_endpoint_config=endpoint_payload(),
                )
                self.assertTrue(spooled.wait(timeout=1))
                # The worker replays a rich-only spool itself, so it wakes the stopped stack for it.
                self.assertTrue(_wait_for(lambda: ensure.called, timeout=2))
                self.assertTrue(worker.is_alive())
                source.put(None)
                worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(status.value, 0)

    def test_replacement_local_installation_spools_once_without_retrying(self):
        source, status = _charged_queue([numeric(1)], None)
        spooled = _QueueStatus()

        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "old-installation.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_ConnectionAttempt",
                    return_value=_ImmediateConnectionAttempt(),
                ),
                mock.patch.object(client_module, "_equal_jitter_delay", return_value=0),
                mock.patch(
                    "kymo._local_runtime.ensure_local_endpoint",
                    side_effect=LocalInstallationMismatch(
                        "installation identity changed"
                    ),
                ) as ensure,
            ):
                worker_module._upload_worker(
                    "unused",
                    "p",
                    "r",
                    source,
                    status,
                    spool_path=spool_path,
                    upload_spooled=spooled,
                    local_endpoint_config=endpoint_payload(),
                )

            ensure.assert_called_once()
            _, records = read_spool(spool_path)
            self.assertEqual(list(records), [numeric(1)])

        self.assertEqual(status.value, 0)
        self.assertEqual(spooled.value, 1)

    def test_worker_retries_terminal_connect_results_until_success(self):
        attempts = []
        outcomes = [
            (None, None),
            (None, None),
            (_Channel(), object()),
        ]

        def connect(_address, *, local_endpoint):
            self.assertIsNone(local_endpoint)
            attempts.append(True)
            return _ImmediateConnectionAttempt(*outcomes.pop(0))

        source, status = _charged_queue([numeric(1)], None)
        immediate = self._immediate_stream_class(lambda _batch: None)

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                mock.patch.object(
                    client_module, "_ConnectionAttempt", side_effect=connect
                ),
                mock.patch.object(client_module, "_BidiStream", immediate),
                mock.patch.object(client_module, "_CONNECT_INITIAL_DELAY", 0.0),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.001),
                _channel_stand_ins() as open_channel,
            ):
                worker = _worker_thread(
                    self, source, status, os.path.join(spool_dir, "connect.mkspool")
                )
                worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(attempts, [True, True, True])
        self.assertEqual(status.value, 0)
        open_channel.assert_not_called()

    def test_short_deadline_interrupts_connect_and_spools_private_buffer(self):
        ready_started = threading.Event()

        class NeverReady:
            @staticmethod
            def done():
                ready_started.set()
                return False

            @staticmethod
            def cancel():
                return True

        source, status = _charged_queue([numeric(1)], None)
        spooled = _QueueStatus()
        deadline = _QueueStatus()

        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "connect-deadline.mkspool")
            with (
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.001),
                mock.patch.object(client_module, "_ACTIVE_POLL_TIMEOUT", 0.001),
                _channel_stand_ins(),
                mock.patch.object(
                    client_module.grpc,
                    "channel_ready_future",
                    return_value=NeverReady(),
                ),
            ):
                worker = _worker_thread(
                    self,
                    source,
                    status,
                    spool_path,
                    shutdown_deadline=deadline,
                    upload_spooled=spooled,
                )
                self.assertTrue(ready_started.wait(timeout=1))
                deadline.value = time.monotonic() + 0.005
                worker.join(timeout=1)

            self.assertFalse(worker.is_alive())
            self.assertTrue(os.path.exists(spool_path))
            self.assertEqual(status.value, 0)
            self.assertEqual(spooled.value, 1)

    def test_worker_waits_out_a_live_owner_stalled_on_the_accounting_lock(self):
        class StalledLock:
            """The accounting lock of a live owner that keeps it across several lock steps."""

            def __init__(self, steps):
                self.steps = steps
                self.lock = threading.Lock()

            def acquire(self, timeout=-1):
                if self.steps:
                    self.steps -= 1
                    return False
                return self.lock.acquire(timeout=timeout)

            def release(self):
                self.lock.release()

        source = queue.Queue()
        source.put([numeric(1)])
        source.put([numeric(2)])
        status = _QueueStatus(2)
        status.lock = StalledLock(3)
        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "stalled.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_ConnectionAttempt",
                    return_value=self._NeverConnects(),
                ),
                mock.patch.object(client_module, "_CONNECT_POLL_TIMEOUT", 0.001),
                mock.patch.dict(os.environ, {"KYMO_MAX_BUFFER_POINTS": "1"}),
            ):
                # The RAM cap fails the lane over to disk, whose spill releases accounting before any deadline.
                worker = _worker_thread(self, source, status, spool_path)
                self.assertTrue(_wait_for(lambda: status.value == 0))
                source.put(None)
                worker.join(timeout=2)
            records = list(read_spool(spool_path)[1])

        self.assertEqual(worker.code, 0)
        self.assertEqual(status.lock.steps, 0)
        self.assertEqual([record[2] for record in records], [1, 2])

    def test_never_ready_connection_keeps_draining_until_ram_cap_failover(self):
        attempt_started = threading.Event()

        class NeverReadyAttempt:
            cancelled = False

            @staticmethod
            def poll(deadline_fn=None):
                self.assertFalse(deadline_fn())
                attempt_started.set()
                return client_module._CONNECT_PENDING

            def cancel(self):
                self.cancelled = True

        attempt = NeverReadyAttempt()
        source = queue.Queue()
        source.put([numeric(1)])
        status = _QueueStatus(2)

        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "never-ready.mkspool")
            with (
                mock.patch.object(
                    client_module, "_ConnectionAttempt", return_value=attempt
                ),
                mock.patch.object(client_module, "_CONNECT_POLL_TIMEOUT", 0.001),
                mock.patch.dict(os.environ, {"KYMO_MAX_BUFFER_POINTS": "1"}),
            ):
                worker = _worker_thread(self, source, status, spool_path)
                self.assertTrue(attempt_started.wait(timeout=1))
                source.put([numeric(2)])
                source.put(None)
                worker.join(timeout=2)

            self.assertTrue(os.path.exists(spool_path))

        self.assertFalse(worker.is_alive())
        self.assertTrue(attempt.cancelled)
        self.assertEqual(status.value, 0)

    def test_ram_cap_failover_recovers_future_live_sends(self):
        """Recovery replays each disk prefix before a newer live suffix."""
        first_replay_started = threading.Event()
        release_first_replay = threading.Event()
        live_stream_opened = threading.Event()
        live_point_acked = threading.Event()
        replayed_steps = []
        replay_had_writer_lock = []
        live_steps = []

        def replay(path, **kwargs):
            self.assertTrue(kwargs["_writer_lock_held"])
            replay_had_writer_lock.append(spool_module.writer_active(path))
            _header, records = read_spool(path)
            steps = [record[2] for record in records]
            replayed_steps.append(steps)
            if len(replayed_steps) == 1:
                first_replay_started.set()
                self.assertTrue(release_first_replay.wait(timeout=1))
                return False
            os.remove(path)
            return True

        def observe_live(batch):
            live_steps.extend(point.step for point in batch.points)
            live_point_acked.set()

        LiveStream = self._immediate_stream_class(observe_live)

        class ObservedLiveStream(LiveStream):
            def __init__(self, stub, feed_cap):
                super().__init__(stub, feed_cap)
                live_stream_opened.set()

        source = queue.Queue()
        # The second point crosses the one-point test cap and moves this whole
        # ordered prefix to disk before any connection attempt is made.
        source.put([numeric(1), numeric(2)])
        status = _QueueStatus(4)
        spooled = _QueueStatus()
        on_disk = types.SimpleNamespace(value=0)

        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "cap-recovery.mkspool")
            with (
                _connected(ObservedLiveStream),
                mock.patch.object(
                    client_module, "_recovery_retry_delay", return_value=0
                ),
                mock.patch.object(sync_module, "replay_file", side_effect=replay),
                mock.patch.dict(os.environ, {"KYMO_MAX_BUFFER_POINTS": "1"}),
            ):
                worker = _worker_thread(
                    self,
                    source,
                    status,
                    spool_path,
                    upload_spooled=spooled,
                    lanes_on_disk=on_disk,
                )
                self.assertTrue(first_replay_started.wait(timeout=1))
                on_disk_during_replay = on_disk.value
                source.put([numeric(3)])
                self.assertTrue(_wait_for(lambda: status.value == 1, timeout=1))
                release_first_replay.set()
                self.assertTrue(live_stream_opened.wait(timeout=1))
                on_disk_once_live = on_disk.value
                source.put([numeric(4)])
                self.assertTrue(live_point_acked.wait(timeout=1))
                source.put(None)
                worker.join(timeout=2)

            self.assertFalse(worker.is_alive())
            self.assertFalse(
                [name for name in os.listdir(spool_dir) if name.endswith(".mkspool")]
            )

        self.assertEqual(replayed_steps, [[1, 2], [1, 2], [3]])
        self.assertEqual(replay_had_writer_lock, [True, True, True])
        # log() stops pacing galleries while the lane is on disk, and resumes once both lanes are live.
        self.assertEqual((on_disk_during_replay, on_disk_once_live), (1, 0))
        self.assertEqual(live_steps, [4])
        self.assertEqual(spooled.value, 0)
        self.assertEqual(status.value, 0)

    @staticmethod
    def _replay_stand_in(gates=(), outcomes=(), delay=0.0):
        """A kymo.sync.replay_file stand-in: call n waits for gates[n] and returns outcomes[n], if given, else True; a successful one records its steps in .numeric and .rich and deletes the segment, "quarantine" quarantines it as authoritative DATA_LOSS would, "terminal" reports the run deleted and then fails to retire the file, and "vanish" removes it as another process might."""

        def replay(path, **kwargs):
            index = len(replay.calls)
            replay.calls.append(path)
            if index < len(gates):
                gates[index].wait(timeout=5)
            time.sleep(delay)
            succeeded = outcomes[index] if index < len(outcomes) else True
            if succeeded == "terminal":
                kwargs["terminal_runs"].add(("unused", "p", "r"))
                raise PermissionError(13, "Permission denied", path)
            if succeeded == "vanish":
                os.remove(path)
                raise FileNotFoundError(2, "No such file or directory", path)
            if succeeded == "quarantine":
                os.rename(path, path + ".rejected")
                kwargs["quarantined_files"].add(path + ".rejected")
                return False
            if succeeded:
                _header, records = read_spool(path)
                for record in records:
                    lane = (
                        replay.rich if record[0].startswith("cdn") else replay.numeric
                    )
                    lane.append(record[2])
                assert kwargs["delete"], "the worker keeps no .sent copy"
                os.remove(path)
            return succeeded

        replay.calls, replay.numeric, replay.rich, replay.gates = [], [], [], gates
        return replay

    @contextlib.contextmanager
    def _numeric_outage(
        self,
        replay,
        *,
        env=None,
        stream=None,
        connect=None,
        retry_delay=None,
        ensure=None,
        jitter=0.01,
        limit=None,
    ):
        """Run the worker through a numeric outage: connects fail until lane.server_up is set (then connect(lane) makes them, if given), and a 200-point cap, or limit seconds without an ACK if given, fails the lane over. With an ensure_local_endpoint stand-in, the worker runs against a local endpoint and lane.ensure is that mock; jitter is every jittered backoff (stream breaks, local ensures).

        Yields a namespace with put(step), put_item(item), shut_down(), the live steps, the opens event, on_disk, status, spooled and spool_dir; on exit it shuts the worker down and checks that it left.
        """
        lane = types.SimpleNamespace(
            server_up=threading.Event(),
            live=[],
            opens=threading.Event(),
            on_disk=types.SimpleNamespace(value=0),
            status=_QueueStatus(),
            spooled=_QueueStatus(),
            source=queue.Queue(),
        )

        def put_item(item):
            with lane.status.get_lock():
                lane.status.value += client_module._queue_item_size(item)
            lane.source.put(item)

        lane.put_item = put_item
        lane.put = lambda step: put_item([numeric(step)])
        lane.shut_down = lambda: lane.source.put(None)
        LiveStream = stream or self._immediate_stream_class(
            lambda batch: lane.live.extend(point.step for point in batch.points)
        )

        class ObservedLiveStream(LiveStream):
            def __init__(self, stub, feed_cap):
                super().__init__(stub, feed_cap)
                lane.opens.set()

        def connect_when_up(*_args, **_kwargs):
            if not lane.server_up.is_set():
                return _ImmediateConnectionAttempt()
            if connect is not None:
                return connect(lane)
            return _ImmediateConnectionAttempt(_Channel(), object())

        patches = (
            mock.patch.object(
                client_module, "_ConnectionAttempt", side_effect=connect_when_up
            ),
            mock.patch.object(client_module, "_BidiStream", ObservedLiveStream),
            mock.patch.object(
                client_module,
                "_recovery_retry_delay",
                side_effect=retry_delay,
                return_value=0,
            ),
            mock.patch.object(
                client_module, "_equal_jitter_delay", return_value=jitter
            ),
            mock.patch.object(sync_module, "replay_file", side_effect=replay),
            mock.patch.dict(
                os.environ, {"KYMO_MAX_BUFFER_POINTS": "200", **(env or {})}
            ),
            # Short loop turns: a worker waiting on nothing still notices a recovery, a limit or a deadline at once.
            mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.02),
        )
        if limit is not None:
            patches += (
                mock.patch.object(client_module, "_NUMERIC_OUTAGE_FAILOVER", limit),
            )
        local = {}
        if ensure is not None:
            local["local_endpoint_config"] = endpoint_payload()
        with (
            tempfile.TemporaryDirectory() as spool_dir,
            contextlib.ExitStack() as stack,
        ):
            for patch in patches:
                stack.enter_context(patch)
            if ensure is not None:
                lane.ensure = stack.enter_context(
                    mock.patch(
                        "kymo._local_runtime.ensure_local_endpoint", side_effect=ensure
                    )
                )
            lane.spool_dir = spool_dir
            worker = _worker_thread(
                self,
                lane.source,
                lane.status,
                os.path.join(spool_dir, "numeric.mkspool"),
                lanes_on_disk=lane.on_disk,
                upload_spooled=lane.spooled,
                **local,
            )
            try:
                yield lane
            finally:
                lane.shut_down()
                worker.join(timeout=5)
        self.assertFalse(worker.is_alive())

    @contextlib.contextmanager
    def _logging_points(self, lane):
        """Log a point every 2 ms from step 1 until the block exits, then set lane.last_step."""
        producing = threading.Event()
        producing.set()

        def log_points():
            step = 1
            while producing.is_set():
                lane.put(step)
                step += 1
                time.sleep(0.002)
            lane.last_step = step - 1

        producer = threading.Thread(target=log_points, daemon=True)
        producer.start()
        try:
            yield
        finally:
            producing.clear()
            producer.join(timeout=1)

    def _drain_behind_second_replay(self, lane, replay, *, gallery=None):
        """Fail the numeric lane over (and the rich one, given a gallery over its cap), recover, and spool point 202 behind the first replay, which replay.gates[0] holds: the second replay then drains the lanes."""
        for step in range(1, 202):
            lane.put(step)
        if gallery is not None:
            lane.put_item(gallery)
        self.assertTrue(_wait_for(lambda: lane.status.value == 0))
        lane.server_up.set()
        self.assertTrue(_wait_for(lambda: replay.calls))
        lane.put(202)
        time.sleep(0.05)
        replay.gates[0].set()
        self.assertTrue(_wait_for(lambda: lane.on_disk.value == 0))

    @staticmethod
    def _resource_gallery(step, data) -> tuple:
        """A versioned one-resource gallery, charged len(data) bytes on arrival."""
        return _versioned_gallery(step, [client_module.Resource(data, "item.bin")])

    @staticmethod
    def _steps_on_disk(spool_dir) -> list:
        """The steps of every record in spool_dir's pending (not yet replayed) spool files, as far as the worker, which may be writing one as this reads, has written whole records."""
        steps = []
        for name in sorted(os.listdir(spool_dir)):
            if name.endswith(".mkspool"):
                try:
                    _header, records = read_spool(os.path.join(spool_dir, name))
                    steps.extend(record[2] for record in records)
                except (
                    EOFError,
                    FileNotFoundError,
                    spool_module.SpoolCorruptionError,
                ):
                    # Created and not yet given its header, replayed and deleted since the listing, or its last record written in part so far.
                    continue
        return steps

    def _local_cdn(self, stack_down):
        """Patches for a local worker's rich transport: uploads fail while stack_down is set."""

        def upload(_http_client, _cdn_url, data, ext):
            if stack_down.is_set():
                raise OSError("the local stack is down")
            return content_id(data, ext)

        stack = contextlib.ExitStack()
        for patch in (
            mock.patch.object(client_module, "_upload_to_cdn", side_effect=upload),
            mock.patch.object(
                client_module, "_publish_rich_mutation", return_value=True
            ),
            mock.patch.object(client_module, "_cdn_retry_delay", return_value=0.0),
            mock.patch("kymo._local_runtime.grpc_channel", return_value=_Channel()),
            mock.patch.object(
                client_module.kymo_pb2_grpc, "KymoStub", return_value=object()
            ),
        ):
            stack.enter_context(patch)
        return stack

    def test_a_segment_that_vanishes_before_its_replay_is_skipped(self):
        replay = self._replay_stand_in(outcomes=("vanish",))
        with (
            self.assertLogs("kymo", level="WARNING") as logs,
            self._numeric_outage(replay) as lane,
        ):
            for step in range(1, 202):
                lane.put(step)
            self.assertTrue(_wait_for(lambda: lane.status.value == 0))
            # Another process takes the sealed segment, so the lane goes on past it and live.
            lane.server_up.set()
            self.assertTrue(lane.opens.wait(timeout=2))
            lane.put(202)
            self.assertTrue(_wait_for(lambda: lane.live == [202]))
        self.assertEqual(len(replay.calls), 1)
        self.assertTrue(
            any("disappeared before its replay" in line for line in logs.output)
        )
        # Whether another process delivered it is unknown, so the run still reports spooled data.
        self.assertEqual(lane.spooled.value, 1)

    def test_a_later_replay_does_not_clear_the_report_of_a_vanished_segment(self):
        replay = self._replay_stand_in(gates=(threading.Event(),), outcomes=("vanish",))
        with (
            self.assertLogs("kymo", level="WARNING"),
            self._numeric_outage(replay) as lane,
        ):
            try:
                for step in range(1, 202):
                    lane.put(step)
                self.assertTrue(_wait_for(lambda: lane.status.value == 0))
                lane.server_up.set()
                self.assertTrue(_wait_for(lambda: replay.calls))
                # Spooled behind the segment that vanishes, then delivered by the next replay.
                lane.put(202)
                time.sleep(0.05)
            finally:
                replay.gates[0].set()
            self.assertTrue(lane.opens.wait(timeout=2))
            self.assertEqual(replay.numeric, [202])
            # The vanished segment's delivery is still unknown while the run goes on.
            self.assertEqual(lane.spooled.value, 1)

    def _break_once_stream(self):
        """A live stream double that records each stream it opens in .opened, and breaks once, the first time it is fed step 999, recording that in .breaks."""

        class BreakOnce(self._immediate_stream_class(lambda _batch: None)):
            opened, breaks = [], []

            def __init__(self, stub, feed_cap):
                super().__init__(stub, feed_cap)
                BreakOnce.opened.append(time.monotonic())

            def feed(self, batch):
                if not BreakOnce.breaks and any(
                    point.step == 999 for point in batch.points
                ):
                    BreakOnce.breaks.append(time.monotonic())
                    self.events.put(("error", RuntimeError("stream broke")))
                    return True
                return super().feed(batch)

        return BreakOnce

    def test_a_delivered_rich_replay_keeps_a_live_stream_backing_off(self):
        replay = self._replay_stand_in(gates=(threading.Event(),))
        stream = self._break_once_stream()
        opened, breaks = stream.opened, stream.breaks
        env = {"KYMO_MAX_RICH_BUFFER_BYTES": "1024"}
        with self._numeric_outage(replay, env=env, stream=stream, jitter=1.0) as lane:
            try:
                lane.server_up.set()
                self.assertTrue(lane.opens.wait(timeout=2))
                # The rich lane fails over and replays while numeric stays live.
                lane.put_item(self._resource_gallery(1001, b"x" * 2048))
                self.assertTrue(_wait_for(lambda: replay.calls))
                # The live stream breaks, and backs off for a second once the worker has seen the break.
                lane.put(999)
                self.assertTrue(_wait_for(lambda: breaks))
                time.sleep(0.2)
                streams = len(opened)
            finally:
                replay.gates[0].set()
            self.assertTrue(_wait_for(lambda: lane.on_disk.value == 0))
            time.sleep(0.3)
            # The rich circuit's success does not cut the numeric backoff short.
            self.assertEqual(len(opened), streams)

    def test_a_terminal_answer_stands_when_retiring_the_segment_fails(self):
        replay = self._replay_stand_in(outcomes=("terminal",))
        with (
            self.assertLogs("kymo", level="ERROR"),
            self._numeric_outage(replay) as lane,
        ):
            for step in range(1, 202):
                lane.put(step)
            self.assertTrue(_wait_for(lambda: lane.status.value == 0))
            lane.server_up.set()
            self.assertTrue(_wait_for(lambda: replay.calls))
            time.sleep(0.2)
            # The run is gone: the segment is not replayed again, and a later point is rejected, neither sent nor spooled.
            lane.put(202)
            self.assertTrue(_wait_for(lambda: lane.status.value == 0))
            time.sleep(0.1)
            self.assertEqual(len(replay.calls), 1)
            self.assertNotIn(202, lane.live)
            self.assertNotIn(202, self._steps_on_disk(lane.spool_dir))

    def test_points_with_no_ack_for_the_outage_limit_go_to_disk(self):
        replay = self._replay_stand_in()
        with (
            self.assertLogs("kymo", level="WARNING") as logs,
            self._numeric_outage(replay, limit=0.3) as lane,
        ):
            for step in range(1, 6):
                lane.put(step)
            # Far below the 200-point cap: only the outage limit moves them to disk.
            self.assertTrue(_wait_for(lambda: lane.status.value == 0))
            self.assertEqual(self._steps_on_disk(lane.spool_dir), [1, 2, 3, 4, 5])
            lane.server_up.set()
            self.assertTrue(lane.opens.wait(timeout=2))
            lane.put(6)
            self.assertTrue(_wait_for(lambda: lane.live == [6]))
        self.assertEqual(replay.numeric, [1, 2, 3, 4, 5])
        self.assertTrue(any("no numeric ACK for 0.3s" in line for line in logs.output))

    def _slow_ack_stream(self, delay):
        """A live stream double that ACKs each batch, in order, delay seconds after it is fed, records the fed steps in .fed, holds its ACKs while .muted is set, and closes once a half-close has every ACK."""

        class SlowAcks(
            self._immediate_stream_class(
                lambda batch: SlowAcks.fed.extend(point.step for point in batch.points)
            )
        ):
            fed = []
            muted = threading.Event()

            def __init__(self, stub, feed_cap):
                super().__init__(stub, feed_cap)
                self.unacked = deque()

            def feed(self, batch):
                if not super().feed(batch):
                    return False
                # Hold back the ACK the parent queued until it is due.
                self.unacked.append(
                    (time.monotonic() + delay, self.events.get_nowait())
                )
                return True

            def poll_acks(self):
                acks = []
                while (
                    self.unacked
                    and self.unacked[0][0] <= time.monotonic()
                    and not SlowAcks.muted.is_set()
                ):
                    acks.append(self.unacked.popleft()[1])
                # A half-close's EOF waits in the parent's queue until every ACK is out.
                return acks + ([] if self.unacked else super().poll_acks())

        return SlowAcks

    def test_acks_slower_than_logging_keep_the_lane_live(self):
        SlowAcks = self._slow_ack_stream(0.1)
        replay = self._replay_stand_in()
        with (
            self.assertLogs("kymo", level="WARNING"),
            # A cap the backlog cannot reach, even if the loop stalls under load.
            self._numeric_outage(
                replay,
                limit=0.5,
                env={"KYMO_MAX_BUFFER_POINTS": "1000000"},
                stream=SlowAcks,
            ) as lane,
        ):
            lane.server_up.set()
            self.assertTrue(lane.opens.wait(timeout=2))
            # Points always wait for an ACK, but each ACK restarts the clock.
            with self._logging_points(lane):
                time.sleep(1.5)
            self.assertTrue(_wait_for(lambda: SlowAcks.fed[-1:] == [lane.last_step]))
            self.assertEqual(replay.calls, [])
            self.assertEqual(SlowAcks.fed, list(range(1, lane.last_step + 1)))
            # Then the open stream stops acknowledging, and the lane fails over.
            SlowAcks.muted.set()
            lane.put(lane.last_step + 1)
            self.assertTrue(_wait_for(lambda: lane.last_step + 1 in replay.numeric))

    def test_silent_streams_the_ack_watchdog_breaks_do_not_restart_the_clock(self):
        class Silent(self._immediate_stream_class(lambda _batch: None)):
            opened = []

            def __init__(self, stub, feed_cap):
                super().__init__(stub, feed_cap)
                Silent.opened.append(self)

            def feed(self, batch):
                # Accepted and never acknowledged.
                return not self.half_closed

        replay = self._replay_stand_in()
        with (
            self.assertLogs("kymo", level="WARNING"),
            self._numeric_outage(
                replay,
                limit=0.6,
                env={"KYMO_ACK_PROGRESS_TIMEOUT": "0.1"},
                stream=Silent,
            ) as lane,
        ):
            lane.server_up.set()
            for step in range(1, 6):
                lane.put(step)
            # The watchdog breaks each silent stream and the worker opens another, while the clock runs on across them.
            self.assertTrue(_wait_for(lambda: replay.numeric == [1, 2, 3, 4, 5]))
            self.assertGreater(len(Silent.opened), 3)

    def test_a_live_lane_with_nothing_pending_does_not_fail_over(self):
        replay = self._replay_stand_in()
        with self._numeric_outage(replay, limit=0.1) as lane:
            lane.server_up.set()
            lane.put(1)
            self.assertTrue(_wait_for(lambda: lane.live == [1]))
            # Acked and idle, the lane has nothing waiting, so no clock runs.
            with self.assertNoLogs("kymo", level="WARNING"):
                time.sleep(0.5)

    def test_after_a_spool_failure_the_limit_leaves_points_in_ram(self):
        disk_full = threading.Event()
        real_spill = client_module._spill_tuple

        def spill(spool, point):
            if disk_full.is_set():
                raise OSError(28, "No space left on device")
            return real_spill(spool, point)

        SlowAcks = self._slow_ack_stream(0.0)
        replay = self._replay_stand_in()
        with (
            mock.patch.object(client_module, "_spill_tuple", side_effect=spill),
            self.assertLogs("kymo", level="WARNING"),
            self._numeric_outage(replay, limit=0.3, stream=SlowAcks) as lane,
        ):
            # A first outage finds the disk full, and its point is lost.
            disk_full.set()
            lane.put(1)
            self.assertTrue(_wait_for(lambda: lane.status.value == 0))
            disk_full.clear()
            lane.server_up.set()
            self.assertTrue(lane.opens.wait(timeout=2))
            # The failed spool would lose a later outage's points too, so they wait in RAM for the server instead.
            SlowAcks.muted.set()
            for step in range(2, 6):
                lane.put(step)
            time.sleep(1.0)
            self.assertEqual(lane.status.value, 4)
            SlowAcks.muted.clear()
            self.assertTrue(_wait_for(lambda: lane.status.value == 0))
        self.assertEqual(SlowAcks.fed, [2, 3, 4, 5])
        self.assertEqual(replay.calls, [])

    def test_a_draining_lane_waits_out_a_slow_replay_in_ram(self):
        replay = self._replay_stand_in(gates=(threading.Event(), threading.Event()))
        with self._numeric_outage(replay, limit=0.3) as lane:
            try:
                self._drain_behind_second_replay(lane, replay)
                # Behind a replay slower than the limit, the drained point stays in RAM, so the catch-up still converges.
                lane.put(203)
                time.sleep(0.6)
                self.assertNotIn(203, self._steps_on_disk(lane.spool_dir))
            finally:
                replay.gates[1].set()
            self.assertTrue(_wait_for(lambda: lane.live == [203]))
        self.assertEqual(len(replay.calls), 2)

    def test_a_gallery_log_did_not_pace_is_charged_whole(self):
        pixels = [Image(np.zeros((512, 512, 3), np.uint8))]
        paced = _versioned_gallery(1001, pixels)
        # Published while a lane wrote to disk, so log() did not wait for the encoder.
        with mock.patch.object(
            client_module, "_lanes_on_disk", types.SimpleNamespace(value=1)
        ):
            unpaced = _versioned_gallery(1002, pixels)
        replay = self._replay_stand_in()
        with (
            self.assertLogs("kymo", level="WARNING"),
            self._numeric_outage(
                replay, env={"KYMO_MAX_RICH_BUFFER_BYTES": str(512 * 1024)}
            ) as lane,
        ):
            lane.server_up.set()
            # A paced gallery's arrays are not charged before they are encoded.
            lane.put_item(paced)
            self.assertFalse(_wait_for(lambda: lane.on_disk.value == 1, timeout=0.5))
            # The unpaced one's 768 KiB of raw pixels count against the 512 KiB rich cap at once, whatever the lanes do now, so the rich lane fails over (and the server being up, its catch-up replays it).
            lane.put_item(unpaced)
            self.assertTrue(
                _wait_for(
                    lambda: 1002 in self._steps_on_disk(lane.spool_dir) + replay.rich
                )
            )

    def test_log_stops_pacing_galleries_before_the_limit_opens_the_spool(self):
        opening, release = threading.Event(), threading.Event()
        real_open = spool_module.SpoolWriter.open

        def slow_open(writer):
            opening.set()
            release.wait(timeout=5)
            real_open(writer)

        replay = self._replay_stand_in()
        with (
            mock.patch.object(spool_module.SpoolWriter, "open", slow_open),
            self.assertLogs("kymo", level="WARNING"),
            self._numeric_outage(replay, limit=0.3) as lane,
        ):
            try:
                for step in range(1, 6):
                    lane.put(step)
                self.assertTrue(opening.wait(timeout=5))
                # The open may stall on a wedged mount, so log() has already stopped waiting for the encoder.
                self.assertEqual(lane.on_disk.value, 1)
            finally:
                release.set()
            self.assertTrue(
                _wait_for(
                    lambda: self._steps_on_disk(lane.spool_dir) == [1, 2, 3, 4, 5]
                )
            )

    def test_points_go_to_disk_while_the_local_stack_stays_down(self):
        replay = self._replay_stand_in()
        endpoint = endpoint_from_worker_config(endpoint_payload())
        stack_down = threading.Event()
        stack_down.set()

        def ensure(**_kwargs):
            if stack_down.is_set():
                raise RuntimeError("the local stack does not start")
            return endpoint

        with (
            self.assertLogs("kymo", level="WARNING"),
            self._local_cdn(stack_down),
            self._numeric_outage(replay, limit=0.3, ensure=ensure) as lane,
        ):
            # Connects fail and every endpoint refresh fails with them, so the points wait for the limit.
            for step in range(1, 6):
                lane.put(step)
            self.assertTrue(
                _wait_for(
                    lambda: self._steps_on_disk(lane.spool_dir) == [1, 2, 3, 4, 5]
                )
            )
            stack_down.clear()
            lane.server_up.set()
            self.assertTrue(_wait_for(lambda: replay.numeric == [1, 2, 3, 4, 5]))

    def test_a_spool_that_cannot_be_opened_leaves_points_in_ram(self):
        broken = threading.Event()
        broken.set()
        fed = []

        class BreakingStream(
            self._immediate_stream_class(
                lambda batch: fed.extend(point.step for point in batch.points)
            )
        ):
            def feed(self, batch):
                if broken.is_set():
                    self.events.put(("error", RuntimeError("ingest is broken")))
                    return True
                return super().feed(batch)

        def read_only(_writer):
            raise OSError(30, "Read-only file system")

        replay = self._replay_stand_in()
        with (
            mock.patch.object(spool_module.SpoolWriter, "open", read_only),
            self.assertLogs("kymo", level="WARNING") as logs,
            self._numeric_outage(replay, limit=0.3, stream=BreakingStream) as lane,
        ):
            lane.server_up.set()
            for step in range(1, 6):
                lane.put(step)
            # The spool could only lose the points, so they wait in RAM, and the streams' own break warnings do not hide why.
            self.assertTrue(
                _wait_for(
                    lambda: any("cannot be opened" in line for line in logs.output)
                )
            )
            self.assertEqual(lane.status.value, 5)
            # Nothing writes to disk after all, so log() paces galleries again.
            self.assertEqual(lane.on_disk.value, 0)
            broken.clear()
            self.assertTrue(_wait_for(lambda: lane.status.value == 0))
        self.assertEqual(fed, [1, 2, 3, 4, 5])
        self.assertEqual(replay.calls, [])

    def test_points_the_limit_spooled_are_reported_once_at_exit(self):
        replay = self._replay_stand_in()
        with (
            self.assertLogs("kymo", level="WARNING") as logs,
            self._numeric_outage(replay, limit=0.3) as lane,
        ):
            for step in range(1, 6):
                lane.put(step)
            self.assertTrue(_wait_for(lambda: lane.status.value == 0))
        # The server never came back, so the worker exits with them on disk.
        self.assertTrue(any("0 points sent, 5 spooled" in line for line in logs.output))

    def test_a_shutdown_waits_for_the_server_without_the_outage_limit(self):
        replay = self._replay_stand_in()
        with self._numeric_outage(replay, limit=0.1) as lane:
            for step in range(1, 6):
                lane.put(step)
            lane.shut_down()
            # The shutdown deadline, not the outage limit, bounds how long a finishing run holds its points.
            time.sleep(0.6)
            self.assertEqual(self._steps_on_disk(lane.spool_dir), [])
            lane.server_up.set()
            self.assertTrue(_wait_for(lambda: lane.live == [1, 2, 3, 4, 5]))

    def test_a_spill_end_holds_off_the_outage_limit(self):
        # A signal, a handoff or the parent's shutdown sets the worker's spill end before any shutdown sentinel arrives: that shutdown's deadlines bound the points from then on, not the outage limit, so the limit opens no spool past that end.
        replay = self._replay_stand_in()
        with (
            mock.patch.object(worker_module, "_signal_deadline", math.inf),
            mock.patch.object(worker_module, "_spill_by", time.monotonic() + 3600),
            self._numeric_outage(replay, limit=0.1) as lane,
        ):
            for step in range(1, 6):
                lane.put(step)
            time.sleep(0.6)
            self.assertEqual(self._steps_on_disk(lane.spool_dir), [])
            lane.server_up.set()
            self.assertTrue(_wait_for(lambda: lane.live == [1, 2, 3, 4, 5]))

    def test_galleries_log_did_not_pace_are_marked_for_the_worker(self):
        gallery = ("cdn_batch", "samples", 1, [Image(np.zeros((2, 2, 3), np.uint8))])

        def items(pid, lanes_on_disk):
            with (
                mock.patch.object(client_module, "_init_pid", pid),
                mock.patch.object(
                    client_module,
                    "_lanes_on_disk",
                    types.SimpleNamespace(value=lanes_on_disk),
                ),
            ):
                (item,) = client_module._snapshot_rich_queue_items([gallery])
            return client_module._decode_queue_item(item)[0][3]

        unpaced, child = (
            client_module._UnpacedGalleryItems,
            client_module._ChildGalleryItems,
        )
        self.assertNotIsInstance(items(os.getpid(), 0), unpaced)
        # Published while a lane writes to disk, when log() does not wait for the encoder.
        self.assertIsInstance(items(os.getpid(), 1), unpaced)
        self.assertNotIsInstance(items(os.getpid(), 1), child)
        # A fork child's gallery is never paced, nor released against the parent's count.
        self.assertIsInstance(items(os.getpid() + 1, 0), child)

    def test_a_spooling_lane_reconnects_only_at_its_recovery_deadline(self):
        replay = self._replay_stand_in()
        with self._numeric_outage(replay, retry_delay=lambda failed, rng: 5.0) as lane:
            for step in range(1, 202):
                lane.put(step)
            self.assertTrue(_wait_for(lambda: lane.on_disk.value == 1))
            # The server is back, but the circuit's backoff has not run out: no replay starts within several idle polls.
            lane.server_up.set()
            self.assertFalse(_wait_for(lambda: replay.calls, timeout=1.5))

    def test_a_live_rich_failover_backs_off_a_draining_numeric_lane(self):
        replay = self._replay_stand_in(gates=(threading.Event(), threading.Event()))
        slow = threading.Event()
        env = {"KYMO_MAX_RICH_BUFFER_BYTES": "1024"}
        with self._numeric_outage(
            replay, env=env, retry_delay=lambda failed, rng: 5.0 if slow.is_set() else 0
        ) as lane:
            try:
                self._drain_behind_second_replay(lane, replay)
                # The live rich lane trips its cap while numeric drains: evidence against the circuit, so it backs off.
                slow.set()
                lane.put_item(self._resource_gallery(1001, b"x" * 2048))
                self.assertTrue(
                    _wait_for(lambda: 1001 in self._steps_on_disk(lane.spool_dir))
                )
                replay.gates[1].set()
                self.assertFalse(_wait_for(lambda: len(replay.calls) > 2, timeout=1.5))
            finally:
                replay.gates[1].set()

    def test_a_failover_clears_a_live_streams_break_backoff(self):
        replay = self._replay_stand_in()
        stream = self._break_once_stream()
        with self._numeric_outage(replay, stream=stream, jitter=2.0) as lane:
            lane.server_up.set()
            self.assertTrue(lane.opens.wait(timeout=5))
            # The live stream breaks and backs off for two seconds, then the lane trips its cap and fails over.
            lane.put(999)
            self.assertTrue(_wait_for(lambda: stream.breaks))
            time.sleep(0.1)
            lane.put_item([numeric(step) for step in range(1, 202)])
            self.assertTrue(_wait_for(lambda: replay.numeric))
            # The first stream after recovery opens at once, not when the old backoff ends.
            self.assertTrue(_wait_for(lambda: len(stream.opened) >= 2, timeout=1.0))

    def test_a_lane_whose_replay_empties_the_disk_goes_live_and_sends_live(self):
        replay = self._replay_stand_in()
        with self._numeric_outage(replay) as lane:
            for step in range(1, 202):
                lane.put(step)
            self.assertTrue(_wait_for(lambda: lane.on_disk.value == 1))
            # Nothing arrives during the replay, so the lane goes live straight from writing to disk.
            lane.server_up.set()
            self.assertTrue(lane.opens.wait(timeout=2))
            lane.put(202)
            self.assertTrue(_wait_for(lambda: lane.live == [202]))
            self.assertEqual(lane.on_disk.value, 0)
        self.assertEqual(sorted(replay.numeric), list(range(1, 202)))

    def test_sustained_logging_returns_a_recovered_numeric_lane_to_live(self):
        # Slower than points arrive, so points always wait behind each replay.
        replay = self._replay_stand_in(delay=0.02)
        with (
            # A drained lane's backlog exceeds this half-open bound, which must not fail it over again.
            mock.patch.object(client_module, "_RECOVERY_MAX_BUFFER_POINTS", 5),
            self._numeric_outage(replay) as lane,
            self._logging_points(lane),
        ):
            self.assertTrue(_wait_for(lambda: lane.on_disk.value == 1))
            lane.server_up.set()
            # Live again while points keep arriving.
            self.assertTrue(lane.opens.wait(timeout=2))
            time.sleep(0.2)
            self.assertEqual(lane.on_disk.value, 0)
        self.assertEqual(
            sorted(replay.numeric + lane.live), list(range(1, lane.last_step + 1))
        )
        # Nothing sent live overtook a point on disk.
        self.assertLess(max(replay.numeric), min(lane.live))

    def test_a_drained_lane_behind_a_dead_stream_still_delivers_by_replay(self):
        replay = self._replay_stand_in(delay=0.02)

        class DeadStream(self._immediate_stream_class(lambda _batch: None)):
            # Unary replays work, but every bidi stream breaks before an ACK (a broken ingest path).
            def feed(self, batch):
                self.events.put(("error", RuntimeError("bidi ingest is broken")))
                return True

        with self._numeric_outage(replay, stream=DeadStream) as lane:
            with self._logging_points(lane):
                self.assertTrue(_wait_for(lambda: lane.on_disk.value == 1))
                lane.server_up.set()
                self.assertTrue(lane.opens.wait(timeout=2))
                time.sleep(0.3)
            # Each broken stream puts its suffix back on disk, so the replays deliver every point.
            every_point = list(range(1, lane.last_step + 1))
            self.assertTrue(_wait_for(lambda: sorted(replay.numeric) == every_point))

    def test_shutdown_spools_rich_work_draining_behind_a_spooling_numeric_lane(self):
        replay = self._replay_stand_in(gates=(threading.Event(), threading.Event()))
        env = {"KYMO_MAX_RICH_BUFFER_BYTES": "1024"}
        with self._numeric_outage(replay, env=env) as lane:
            try:
                # Both lanes fail over (rich on its 1 KiB cap) and then drain.
                over_cap = self._resource_gallery(1, b"x" * 2048)
                self._drain_behind_second_replay(lane, replay, gallery=over_cap)
                # A numeric cap trip then ends only numeric's drain.
                lane.put_item(self._resource_gallery(2, b"y" * 16))
                for step in range(203, 405):
                    lane.put(step)
                self.assertTrue(_wait_for(lambda: lane.on_disk.value == 1))
                lane.shut_down()
            finally:
                replay.gates[1].set()
        # The draining gallery went to disk at shutdown, and the exit tail replayed it.
        self.assertIn(2, replay.rich)
        self.assertEqual(lane.status.value, 0)

    def test_shutdown_after_the_last_replay_still_replays_the_drained_work(self):
        replay = self._replay_stand_in(gates=(threading.Event(),))
        connects = []

        class HeldConnection:
            # Stays pending, holding both lanes between their last replay and going live.
            def poll(self, deadline_fn=None):
                return client_module._CONNECT_PENDING

            def cancel(self):
                pass

        def connect(lane):
            connects.append(None)
            if len(connects) <= 2:
                return _ImmediateConnectionAttempt(_Channel(), object())
            return HeldConnection()

        env = {"KYMO_MAX_RICH_BUFFER_BYTES": "1024"}
        with self._numeric_outage(replay, env=env, connect=connect) as lane:
            try:
                # Both lanes fail over and drain behind the second replay; the connect after it stays pending.
                over_cap = self._resource_gallery(1001, b"x" * 2048)
                self._drain_behind_second_replay(lane, replay, gallery=over_cap)
                self.assertTrue(_wait_for(lambda: len(connects) >= 3))
                lane.put(203)
                lane.put_item(self._resource_gallery(1002, b"y" * 16))
                time.sleep(0.05)
            finally:
                replay.gates[0].set()
        # Shutdown sent the drained work to disk with the circuit still proven, so the exit tail replayed it.
        self.assertEqual(sorted(replay.numeric), list(range(1, 204)))
        self.assertEqual(sorted(replay.rich), [1001, 1002])
        self.assertEqual(lane.status.value, 0)

    def test_a_failed_replay_while_numeric_drains_moves_its_points_back_to_disk(self):
        # The first replay proves the circuit, the second fails, and the third waits for the test's end.
        replay = self._replay_stand_in(
            gates=tuple(threading.Event() for _ in range(3)), outcomes=(True, False)
        )
        failures = []

        def retry_delay(failed, rng):
            failures.append(failed)
            return 0

        with self._numeric_outage(replay, retry_delay=retry_delay) as lane:
            try:
                self._drain_behind_second_replay(lane, replay)
                # Draining: held in RAM, not sent.
                lane.put(203)
                time.sleep(0.05)
                self.assertEqual(lane.status.value, 1)
                replay.gates[1].set()
                self.assertTrue(_wait_for(lambda: lane.status.value == 0))
                self.assertEqual(lane.on_disk.value, 1)
                self.assertEqual(lane.live, [])
                self.assertIn(203, self._steps_on_disk(lane.spool_dir))
                # The failed replay is one step of the backoff; leaving the drain schedules no extra retry.
                self.assertEqual(failures[-1], 1)
            finally:
                replay.gates[2].set()

    def test_a_failed_reconnect_while_numeric_drains_moves_its_points_back_to_disk(
        self,
    ):
        replay = self._replay_stand_in(gates=(threading.Event(), threading.Event()))
        with self._numeric_outage(replay) as lane:
            try:
                self._drain_behind_second_replay(lane, replay)
                lane.put(203)
                time.sleep(0.05)
                # The server goes away again before the lane is live: its next connect fails.
                lane.server_up.clear()
                replay.gates[1].set()
                self.assertTrue(_wait_for(lambda: lane.status.value == 0))
                self.assertEqual(lane.on_disk.value, 1)
                self.assertEqual(lane.live, [])
                self.assertIn(203, self._steps_on_disk(lane.spool_dir))
            finally:
                replay.gates[1].set()
        # The failed connect voided the circuit proof, so shutdown did not replay against a server that had just gone away.
        self.assertEqual(len(replay.calls), 2)

    def test_shutdown_while_numeric_drains_replays_its_points_from_disk(self):
        replay = self._replay_stand_in(gates=(threading.Event(), threading.Event()))
        with self._numeric_outage(replay) as lane:
            try:
                self._drain_behind_second_replay(lane, replay)
                # Draining, behind the second replay: shutdown cannot go live, so this point goes to disk and the exit tail replays it.
                lane.put(203)
                time.sleep(0.05)
                lane.shut_down()
            finally:
                replay.gates[1].set()
        self.assertEqual(sorted(replay.numeric), list(range(1, 204)))
        self.assertEqual(lane.status.value, 0)

    def test_shutdown_after_a_quarantine_ends_the_drain_replays_the_drained_points(
        self,
    ):
        replay = self._replay_stand_in(
            gates=(threading.Event(), threading.Event()),
            outcomes=(True, "quarantine"),
        )
        with (
            self.assertLogs("kymo", level="ERROR"),
            self._numeric_outage(replay) as lane,
        ):
            try:
                self._drain_behind_second_replay(lane, replay)
                # Shutdown sends the drained point to disk, and then the server quarantines the second replay's segment.
                lane.put(203)
                lane.shut_down()
                self.assertTrue(_wait_for(lambda: lane.on_disk.value == 1))
            finally:
                replay.gates[1].set()
        # A quarantine is the server's answer, so the circuit stayed proven and the exit tail replayed the drained point.
        self.assertEqual(sorted(replay.numeric), [*range(1, 202), 203])

    def test_a_quarantine_after_a_failed_replay_leaves_the_circuit_unproven(self):
        # The first replay fails, so its segment replays again, and the server quarantines it.
        replay = self._replay_stand_in(
            gates=(threading.Event(), threading.Event()),
            outcomes=(False, "quarantine"),
        )
        with (
            self.assertLogs("kymo", level="ERROR"),
            self._numeric_outage(replay) as lane,
        ):
            try:
                for step in range(1, 202):
                    lane.put(step)
                self.assertTrue(_wait_for(lambda: lane.status.value == 0))
                lane.server_up.set()
                self.assertTrue(_wait_for(lambda: replay.calls))
                lane.put(202)
                replay.gates[0].set()
                self.assertTrue(_wait_for(lambda: len(replay.calls) == 2))
                # Shutdown begins before the quarantine.
                lane.put(203)
                lane.shut_down()
                self.assertTrue(_wait_for(lane.source.empty))
            finally:
                replay.gates[1].set()
        # A quarantine says nothing about the circuit (it can come from a check that never reached the server), so after the failure the exit tail left what waited behind it on disk.
        self.assertEqual(replay.numeric, [])
        self.assertEqual(len(replay.calls), 2)

    def test_a_live_failover_during_a_replay_leaves_the_circuit_unproven(self):
        replay = self._replay_stand_in(gates=(threading.Event(),))
        env = {"KYMO_MAX_RICH_BUFFER_BYTES": "1024"}
        with self._numeric_outage(replay, env=env) as lane:
            try:
                for step in range(1, 202):
                    lane.put(step)
                self.assertTrue(_wait_for(lambda: lane.status.value == 0))
                lane.server_up.set()
                self.assertTrue(_wait_for(lambda: replay.calls))
                # The live rich lane trips its cap while the numeric replay runs, and shutdown begins before that replay succeeds.
                lane.put_item(self._resource_gallery(1001, b"x" * 2048))
                self.assertTrue(
                    _wait_for(lambda: 1001 in self._steps_on_disk(lane.spool_dir))
                )
                lane.shut_down()
                self.assertTrue(_wait_for(lane.source.empty))
            finally:
                replay.gates[0].set()
        # The failover is newer evidence than the replay's success, so the exit tail left the gallery on disk.
        self.assertEqual(len(replay.calls), 1)

    def test_a_numeric_failover_during_a_rich_replay_leaves_the_circuit_unproven(
        self,
    ):
        replay = self._replay_stand_in(gates=(threading.Event(),))
        env = {"KYMO_MAX_RICH_BUFFER_BYTES": "1024"}
        with self._numeric_outage(replay, env=env) as lane:
            try:
                lane.server_up.set()
                # The rich lane fails over on its cap and replays its spool while numeric stays live.
                lane.put_item(self._resource_gallery(1001, b"x" * 2048))
                self.assertTrue(_wait_for(lambda: replay.calls))
                # Then the live numeric lane trips its cap, and shutdown begins before the rich replay succeeds.
                lane.put_item([numeric(step) for step in range(1, 202)])
                self.assertTrue(
                    _wait_for(lambda: 201 in self._steps_on_disk(lane.spool_dir))
                )
                lane.shut_down()
                self.assertTrue(_wait_for(lane.source.empty))
            finally:
                replay.gates[0].set()
        # The numeric failover is newer evidence than the replay's success, so the exit tail left the points on disk.
        self.assertEqual(len(replay.calls), 1)

    def test_a_failed_local_refresh_while_numeric_drains_moves_its_points_back_to_disk(
        self,
    ):
        replay = self._replay_stand_in(gates=(threading.Event(), threading.Event()))
        endpoint = endpoint_from_worker_config(endpoint_payload())
        stack_down = threading.Event()

        def ensure(**_kwargs):
            if stack_down.is_set():
                raise RuntimeError("the local stack does not start")
            return endpoint

        with (
            self.assertLogs("kymo", level="WARNING"),
            self._local_cdn(stack_down),
            self._numeric_outage(replay, ensure=ensure) as lane,
        ):
            try:
                self._drain_behind_second_replay(lane, replay)
                lane.put(203)
                # The stack goes down: a live gallery's upload fails, so the endpoint needs a refresh, which keeps failing.
                stack_down.set()
                lane.put_item(self._resource_gallery(1001, b"y" * 16))
                self.assertTrue(
                    _wait_for(lambda: 203 in self._steps_on_disk(lane.spool_dir))
                )
                self.assertEqual(lane.on_disk.value, 1)
            finally:
                stack_down.clear()
                replay.gates[1].set()
        self.assertEqual(sorted(replay.numeric), list(range(1, 204)))

    def test_a_connect_that_lands_during_a_local_refresh_does_not_replay(self):
        replay = self._replay_stand_in()
        endpoint = endpoint_from_worker_config(endpoint_payload())
        stack_down, refreshed, connected, landed = (threading.Event() for _ in range(4))
        held_ensures = []

        def ensure(**_kwargs):
            if stack_down.is_set():
                held_ensures.append(None)
                refreshed.wait(timeout=5)
            return endpoint

        channel = _Channel()

        class HeldConnection:
            def poll(self, deadline_fn=None):
                if not connected.is_set():
                    return client_module._CONNECT_PENDING
                landed.set()
                return channel, object()

            def cancel(self):
                pass

        connects = []

        def connect(_lane):
            connects.append(None)
            if len(connects) == 1:
                return HeldConnection()
            return _ImmediateConnectionAttempt(_Channel(), object())

        with (
            self.assertLogs("kymo", level="WARNING"),
            self._local_cdn(stack_down),
            self._numeric_outage(replay, connect=connect, ensure=ensure) as lane,
        ):
            try:
                for step in range(1, 202):
                    lane.put(step)
                self.assertTrue(_wait_for(lambda: lane.status.value == 0))
                lane.server_up.set()
                self.assertTrue(_wait_for(lambda: connects))
                # A live gallery's upload fails, so the endpoint needs a refresh, which is still running when the connect lands.
                stack_down.set()
                lane.put_item(self._resource_gallery(1001, b"y" * 16))
                self.assertTrue(_wait_for(lambda: held_ensures))
                connected.set()
                self.assertTrue(landed.wait(timeout=5))
                time.sleep(0.05)
                self.assertEqual(replay.calls, [])
                # Once the endpoint is refreshed, a fresh connect replays the prefix.
                stack_down.clear()
                refreshed.set()
                self.assertTrue(
                    _wait_for(lambda: sorted(replay.numeric) == list(range(1, 202)))
                )
            finally:
                stack_down.clear()
                refreshed.set()
                connected.set()

    def test_shutdown_while_a_local_lane_drains_starts_no_endpoint_refresh(self):
        replay = self._replay_stand_in(gates=(threading.Event(), threading.Event()))
        endpoint = endpoint_from_worker_config(endpoint_payload())
        with self._numeric_outage(replay, ensure=lambda **_kwargs: endpoint) as lane:
            try:
                self._drain_behind_second_replay(lane, replay)
                ensures = lane.ensure.call_count
                lane.put(203)
                lane.shut_down()
                self.assertTrue(_wait_for(lambda: lane.on_disk.value == 1))
                time.sleep(0.05)
                # A draining lane holds no transport, so spilling it leaves the endpoint as it is.
                self.assertEqual(lane.ensure.call_count, ensures)
            finally:
                replay.gates[1].set()
        self.assertEqual(sorted(replay.numeric), list(range(1, 204)))

    def test_numeric_lane_connects_beside_a_rich_only_replay(self):
        replaying = threading.Event()
        release = threading.Event()
        live_steps = []
        live_point = threading.Event()

        def replay(path, **kwargs):
            replaying.set()
            self.assertTrue(release.wait(timeout=5))
            os.remove(path)
            return True

        def observe_live(batch):
            live_steps.extend(point.step for point in batch.points)
            live_point.set()

        # Its 2 KiB resource is charged on arrival, over the 1 KiB cap, so the rich lane fails over and its replay holds before the first connect.
        gallery = _versioned_gallery(
            1, [client_module.Resource(b"x" * 2048, "overflow.bin")]
        )
        source, status = _charged_queue(gallery, [numeric(7)])
        spooled = _QueueStatus()
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                _connected(self._immediate_stream_class(observe_live)),
                mock.patch.object(
                    client_module, "_recovery_retry_delay", return_value=0
                ),
                mock.patch.object(sync_module, "replay_file", side_effect=replay),
                mock.patch.dict(os.environ, {"KYMO_MAX_RICH_BUFFER_BYTES": "1024"}),
            ):
                worker = _worker_thread(
                    self,
                    source,
                    status,
                    os.path.join(spool_dir, "rich-replay.mkspool"),
                    upload_spooled=spooled,
                )
                self.assertTrue(replaying.wait(timeout=1))
                # The held replay is rich-only, so it must not keep the live numeric lane from connecting.
                self.assertTrue(live_point.wait(timeout=1))
                release.set()
                source.put(None)
                worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(live_steps, [7])
        self.assertEqual(spooled.value, 0)
        self.assertEqual(status.value, 0)

    def test_local_replay_refreshes_generation_after_auth_failure(self):
        replayed_endpoints = []
        replay_done = threading.Event()
        initial_endpoint = endpoint_from_worker_config(endpoint_payload())
        refreshed_config = endpoint_payload()
        refreshed_config.update(
            {
                "endpoint_generation": "33333333-3333-4333-8333-333333333333",
                "server_bearer": "B" * 42 + "A",
            }
        )
        refreshed_endpoint = endpoint_from_worker_config(refreshed_config)

        def replay(path, **kwargs):
            replayed_endpoints.append(kwargs["_local_endpoint"])
            if len(replayed_endpoints) == 1:
                return False
            os.remove(path)
            replay_done.set()
            return True

        source, status = _charged_queue([numeric(1), numeric(2)])

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                mock.patch.object(
                    client_module,
                    "_ConnectionAttempt",
                    return_value=_ImmediateConnectionAttempt(_Channel(), object()),
                ),
                mock.patch.object(
                    client_module, "_recovery_retry_delay", return_value=0
                ),
                mock.patch.object(sync_module, "replay_file", side_effect=replay),
                mock.patch.dict(os.environ, {"KYMO_MAX_BUFFER_POINTS": "1"}),
                mock.patch(
                    "kymo._local_runtime.ensure_local_endpoint",
                    side_effect=[initial_endpoint, refreshed_endpoint],
                ) as ensure,
            ):
                worker = _worker_thread(
                    self,
                    source,
                    status,
                    os.path.join(spool_dir, "rotated-bearer.mkspool"),
                    local_endpoint_config=endpoint_payload(),
                )
                self.assertTrue(replay_done.wait(timeout=2))
                source.put(None)
                worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(ensure.call_count, 2)
        self.assertEqual(replayed_endpoints, [initial_endpoint, refreshed_endpoint])

    def test_data_loss_quarantine_advances_automatic_spool_replay(self):
        replay_calls = []
        quarantined = threading.Event()
        replayed_later = threading.Event()

        def quarantine_replay(path, **kwargs):
            replay_calls.append(path)
            if len(replay_calls) > 1:
                os.remove(path)
                replayed_later.set()
                return True
            rejected = path + ".rejected"
            os.rename(path, rejected)
            kwargs["quarantined_files"].add(rejected)
            quarantined.set()
            return False

        source, status = _charged_queue([numeric(1), numeric(2)])

        def enqueue_later_segment():
            quarantined.wait(timeout=2)
            with status.get_lock():
                status.value += 1
            source.put([numeric(3)])
            replayed_later.wait(timeout=2)
            source.put(None)

        threading.Thread(target=enqueue_later_segment, daemon=True).start()
        failure = _QueueStatus()
        spooled = _QueueStatus()

        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "data-loss.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_ConnectionAttempt",
                    return_value=_ImmediateConnectionAttempt(_Channel(), object()),
                ),
                mock.patch.object(
                    client_module, "_recovery_retry_delay", return_value=0
                ),
                mock.patch.object(
                    sync_module, "replay_file", side_effect=quarantine_replay
                ),
                mock.patch.dict(os.environ, {"KYMO_MAX_BUFFER_POINTS": "1"}),
            ):
                worker_module._upload_worker(
                    "unused",
                    "p",
                    "r",
                    source,
                    status,
                    spool_path=spool_path,
                    upload_failure=failure,
                    upload_spooled=spooled,
                )

            self.assertTrue(replayed_later.is_set())
            self.assertEqual(len(replay_calls), 2)
            self.assertFalse(
                [name for name in os.listdir(spool_dir) if name.endswith(".mkspool")]
            )
            self.assertTrue(
                [name for name in os.listdir(spool_dir) if ".mkspool.rejected" in name]
            )
            self.assertEqual(status.value, 0)
            self.assertEqual(failure.value, client_module._WORKER_EXIT_DATA_LOSS)
            # The rejected evidence keeps the parent's spooled-data warning
            # set even though no automatic replay segment remains.
            self.assertEqual(spooled.value, 1)
        self.assertEqual(status.value, 0)

    def test_shutdown_during_the_final_replay_reports_delivered(self):
        """Shutdown closes the connect gate, so replay itself must clear
        accounting: the caller cannot report spooled data that was delivered."""
        replay_started = threading.Event()
        release_replay = threading.Event()

        def replay(path, **_kwargs):
            replay_started.set()
            self.assertTrue(release_replay.wait(timeout=5))
            os.remove(path)
            return True

        source, status = _charged_queue([numeric(1), numeric(2)])
        spooled = _QueueStatus()

        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "final-replay.mkspool")
            with (
                _connected(self._immediate_stream_class(lambda _batch: None)),
                mock.patch.object(
                    client_module, "_recovery_retry_delay", return_value=0
                ),
                mock.patch.object(sync_module, "replay_file", side_effect=replay),
                mock.patch.dict(os.environ, {"KYMO_MAX_BUFFER_POINTS": "1"}),
            ):
                worker = _worker_thread(
                    self, source, status, spool_path, upload_spooled=spooled
                )
                self.assertTrue(replay_started.wait(timeout=5))
                # The loop drains the queue before it collects the replay result,
                # so the sentinel is observed no later than the replay's success.
                source.put(None)
                release_replay.set()
                worker.join(timeout=5)

            self.assertFalse(worker.is_alive())
            self.assertFalse(
                [name for name in os.listdir(spool_dir) if name.endswith(".mkspool")]
            )
        self.assertEqual(spooled.value, 0)
        self.assertEqual(status.value, 0)

    def test_unsealable_spool_keeps_draining_behind_its_disk_prefix(self):
        """A spool mount that fails the seal must not kill the worker, and must
        not let live sends jump ahead of the undelivered disk prefix."""
        live_steps = []
        seal_attempted = threading.Event()

        def failing_seal(*_args):
            seal_attempted.set()
            raise OSError("stale file handle")

        source, status = _charged_queue([numeric(1), numeric(2)])
        spooled = _QueueStatus()
        failure = _QueueStatus()

        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "unsealable.mkspool")
            with (
                _connected(
                    self._immediate_stream_class(
                        lambda batch: live_steps.extend(
                            point.step for point in batch.points
                        )
                    )
                ),
                mock.patch.object(
                    client_module, "_recovery_retry_delay", return_value=0
                ),
                mock.patch.object(
                    client_module.SpoolWriter, "seal", side_effect=failing_seal
                ),
                mock.patch.object(
                    sync_module, "replay_file", side_effect=AssertionError
                ),
                mock.patch.dict(os.environ, {"KYMO_MAX_BUFFER_POINTS": "1"}),
                self.assertLogs("kymo", level="ERROR") as logs,
            ):
                worker = _worker_thread(
                    self,
                    source,
                    status,
                    spool_path,
                    upload_spooled=spooled,
                    upload_failure=failure,
                )
                self.assertTrue(seal_attempted.wait(timeout=5))
                source.put(None)
                worker.join(timeout=5)

            self.assertFalse(worker.is_alive())
            self.assertTrue(os.path.exists(spool_path))
            _header, records = read_spool(spool_path)
            self.assertEqual([record[2] for record in records], [1, 2])

        self.assertIn(
            "failed to seal upload spool",
            "\n".join(logs.output),
        )
        self.assertEqual(live_steps, [])
        self.assertEqual(spooled.value, 1)
        self.assertEqual(failure.value, client_module._WORKER_EXIT_SPOOL_FAILED)
        self.assertEqual(status.value, 0)

    def test_replay_keeps_every_fsync_off_the_queue_consumer_thread(self):
        """The whole point of moving the barrier: no fsync may run on the loop that drains the producer queue — not the close() after replay, and not a retry of a barrier that failed, which is precisely when the mount is the thing that would block.

        Delivering the records is a stronger guarantee than persisting them, so a failed barrier only warns."""
        real_fsync = os.fsync
        for label, barrier_fails in (("barrier ok", False), ("barrier fails", True)):
            fsync_threads, replayed = [], []
            replay_done = threading.Event()

            def spy(fd, barrier_fails=barrier_fails):
                fsync_threads.append(threading.current_thread().name)
                if barrier_fails:
                    raise OSError("input/output error")
                return real_fsync(fd)

            def replay(path, replayed=replayed, replay_done=replay_done, **_kwargs):
                replayed.append(path)
                os.remove(path)
                replay_done.set()
                return True

            source, status = _charged_queue([numeric(1), numeric(2)])
            spooled = _QueueStatus()

            with self.subTest(label), tempfile.TemporaryDirectory() as spool_dir:
                spool_path = os.path.join(spool_dir, "barrier.mkspool")
                with (
                    _connected(self._immediate_stream_class(lambda _batch: None)),
                    mock.patch.object(
                        client_module, "_recovery_retry_delay", return_value=0
                    ),
                    mock.patch.object(sync_module, "replay_file", side_effect=replay),
                    mock.patch.object(os, "fsync", spy),
                    mock.patch.dict(os.environ, {"KYMO_MAX_BUFFER_POINTS": "1"}),
                ):
                    worker = _worker_thread(
                        self,
                        source,
                        status,
                        spool_path,
                        upload_spooled=spooled,
                        name="kymo-test-owner",
                    )
                    # Shutdown suppresses new connects, so the replay has to be under way before the sentinel, as it is in the live outage this fix is about.
                    self.assertTrue(replay_done.wait(timeout=5))
                    source.put(None)
                    worker.join(timeout=5)

                self.assertFalse(worker.is_alive())
                self.assertEqual(replayed, [spool_path])
                # Every barrier (the records and, once they succeed, the directory entry) runs off the loop.
                self.assertEqual(set(fsync_threads), {"kymo-spool-replay"})
                self.assertEqual((spooled.value, status.value), (0, 0))

    def test_expired_monotonic_deadline_ignores_wall_clock_and_spools_in_order(self):
        spilled_steps = []
        test_case = self

        class DeadlineQueue:
            def __init__(self):
                self.items = [[numeric(1)], [numeric(2)], None]

            def get(self, timeout=None):
                del timeout
                if len(self.items) == 2:
                    test_case.assertEqual(spilled_steps, [1])
                return self.items.pop(0)

        real_spill = client_module._spill_tuple

        def record_spill(spool, point):
            spilled_steps.append(point[2])
            return real_spill(spool, point)

        with (
            mock.patch.object(client_module, "_spill_tuple", side_effect=record_spill),
            # A backward wall-clock correction must not extend shutdown.
            mock.patch.object(worker_module.time, "time", return_value=0.0),
        ):
            code, status = _worker_past_its_deadline(DeadlineQueue(), points=2)

        self.assertEqual(spilled_steps, [1, 2])
        self.assertEqual((code, status.value), (0, 0))

    def test_blocked_cdn_head_does_not_stall_numeric_and_preserves_cdn_order(self):
        first_cdn_started = threading.Event()
        release_first_cdn = threading.Event()
        second_numeric_fed = threading.Event()
        cdn_calls = []

        def process_cdn(
            _stub,
            _http_client,
            _cdn_url,
            _project_id,
            _run_id,
            name,
            _step,
            _items,
            **_kwargs,
        ):
            cdn_calls.append(name)
            if name == "demo/first":
                first_cdn_started.set()
                release_first_cdn.wait(timeout=3)
            return True

        def on_feed(batch):
            if any(point.step == 2 for point in batch.points):
                second_numeric_fed.set()

        source = queue.Queue()
        source.put(
            [
                numeric(1),
                ("cdn_batch", "demo/first", 1, [Resource(b"one", "bin")]),
            ]
        )
        status = _QueueStatus(4)
        deadline = _QueueStatus()

        with tempfile.TemporaryDirectory() as spool_dir:
            with contextlib.ExitStack() as stack:
                stack.enter_context(_connected(self._immediate_stream_class(on_feed)))
                stack.enter_context(_channel_stand_ins())
                stack.enter_context(
                    mock.patch("httpx.Client", return_value=self._HttpClient())
                )
                stack.enter_context(
                    mock.patch.object(
                        client_module, "_process_cdn_batch", side_effect=process_cdn
                    )
                )
                worker = _worker_thread(
                    self,
                    source,
                    status,
                    os.path.join(spool_dir, "cdn.mkspool"),
                    cdn_url="http://unused",
                    shutdown_deadline=deadline,
                )
                self.assertTrue(first_cdn_started.wait(timeout=1))
                source.put(
                    [
                        numeric(2),
                        (
                            "cdn_batch",
                            "demo/second",
                            2,
                            [Resource(b"two", "bin")],
                        ),
                    ]
                )
                source.put(None)
                try:
                    numeric_progressed = second_numeric_fed.wait(timeout=0.5)
                finally:
                    release_first_cdn.set()
                    worker.join(timeout=3)

        self.assertTrue(numeric_progressed)
        self.assertFalse(worker.is_alive())
        self.assertEqual(cdn_calls, ["demo/first", "demo/second"])
        self.assertEqual(status.value, 0)

    def test_outage_retries_cdn_and_metadata_until_real_points_commit(self):
        class TransientRpcError(client_module.grpc.RpcError):
            def details(self):
                return "ingest unavailable"

        source = queue.Queue()
        source.put(
            [
                ("cdn_batch", "demo/gallery", 1, [Resource(b"image", "bin")]),
                (
                    "metadata_batch",
                    "info/run_info",
                    0,
                    Metadata({"model": "test"}),
                ),
            ]
        )
        source.put(None)
        status = _QueueStatus(2)

        # The first gallery attempt sees the ingest side of a server rollout:
        # both its best-effort placeholder and its real point fail. Metadata's
        # real point then fails independently. The content-addressed uploads
        # succeed throughout, so only retaining each queue head can make its
        # already-uploaded content visible after ingest recovers.
        upload_results = [
            "resource.bin",
            "manifest.json",
            "resource.bin",
            "manifest.json",
            "metadata.json",
            "metadata.json",
        ]
        point_results = [
            TransientRpcError(),
            TransientRpcError(),
            True,
            TransientRpcError(),
            True,
        ]

        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "cdn-outage.mkspool")
            deadline = _QueueStatus()
            with (
                _channel_stand_ins(),
                mock.patch("httpx.Client", return_value=self._HttpClient()),
                mock.patch.object(
                    client_module,
                    "_upload_to_cdn",
                    side_effect=upload_results,
                ) as upload,
                mock.patch.object(
                    client_module,
                    "_send_unary_point",
                    side_effect=point_results,
                ) as send_point,
                mock.patch.object(client_module, "_cdn_retry_delay", return_value=0.0),
            ):
                worker = _worker_thread(
                    self,
                    source,
                    status,
                    spool_path,
                    cdn_url="http://unused",
                    shutdown_deadline=deadline,
                )
                try:
                    worker.join(timeout=3)
                    timed_out = worker.is_alive()
                finally:
                    if worker.is_alive():
                        deadline.value = time.monotonic() - 1
                        worker.join(timeout=1)

            self.assertFalse(timed_out)
            self.assertFalse(worker.is_alive())
            self.assertFalse(os.path.exists(spool_path))

        self.assertEqual(upload.call_count, 6)
        self.assertEqual(send_point.call_count, 5)
        sent_keys = [call.args[3].cdn_key for call in send_point.call_args_list]
        self.assertTrue(sent_keys[0].startswith("pending:"))
        self.assertEqual(
            sent_keys[1:],
            ["manifest.json", "manifest.json", "metadata.json", "metadata.json"],
        )
        self.assertEqual(status.value, 0)

    def test_undecodable_queue_item_is_dropped_as_lost_and_later_items_deliver(self):
        # A rich snapshot that will not unpickle, and a numeric batch whose lists are out of step: each is lost and reported, and only the snapshot, which may be a gallery's, counts as a gallery no longer raw.
        fed = []
        undecodable = (
            client_module._SERIALIZED_RICH_QUEUE_ITEM,
            b"not a pickle",
        )
        out_of_step = (
            client_module._NUMERIC_BATCH_QUEUE_ITEM,
            3,
            1234,
            ["a", "b"],
            [1.0],
            [None, None],
        )
        source = queue.Queue()
        for item in ([numeric(1)], undecodable, out_of_step, [numeric(2)], None):
            source.put(item)
        status = _QueueStatus(5)
        failure = _QueueStatus()
        released = _QueueStatus()
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                _connected(
                    self._immediate_stream_class(
                        lambda batch: fed.extend(point.step for point in batch.points)
                    )
                ),
                self.assertLogs("kymo", level="ERROR") as logs,
                self.assertRaises(SystemExit) as exited,
            ):
                worker_module._upload_worker(
                    "unused",
                    "p",
                    "r",
                    source,
                    status,
                    spool_path=os.path.join(spool_dir, "undecodable.mkspool"),
                    upload_failure=failure,
                    galleries_released=released,
                )
            self.assertEqual(os.listdir(spool_dir), [])

        self.assertEqual(fed, [1, 2])
        self.assertEqual(status.value, 0)
        self.assertEqual(released.value, 1)
        self.assertEqual(failure.value, client_module._WORKER_EXIT_SPOOL_FAILED)
        self.assertEqual(exited.exception.code, client_module._WORKER_EXIT_SPOOL_FAILED)
        self.assertIn("failed to decode", "\n".join(logs.output))

    def test_stream_construction_failure_retries_without_losing_buffer(self):
        fed = []
        stream_attempts = 0
        immediate = self._immediate_stream_class(lambda batch: fed.extend(batch.points))

        def open_stream(stub, feed_cap):
            nonlocal stream_attempts
            stream_attempts += 1
            if stream_attempts == 1:
                raise RuntimeError("call construction failed")
            return immediate(stub, feed_cap)

        source, status = _charged_queue([numeric(1)], None)
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                _connected(mock.Mock(side_effect=open_stream)),
                mock.patch.object(wire_module, "_SEND_INITIAL_DELAY", 0.0),
            ):
                worker = _worker_thread(
                    self, source, status, os.path.join(spool_dir, "stream-open.mkspool")
                )
                worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(stream_attempts, 2)
        self.assertEqual([point.step for point in fed], [1])
        self.assertEqual(status.value, 0)

    def test_a_batchs_captured_output_is_delivered_after_its_values(self):
        # A numeric batch carries the text points of the output its publication drained as its 7th field, which the worker expands after the values.
        fed = []
        text = [("text_ts", "logs/std_out", 5, "out", 1234)]
        source = queue.Queue()
        source.put(
            (
                client_module._NUMERIC_TEXT_BATCH_QUEUE_ITEM,
                1,
                1234,
                ["a"],
                [1.0],
                [None],
                text,
            )
        )
        source.put(None)
        status = _QueueStatus(2)
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                mock.patch.object(
                    client_module,
                    "_ConnectionAttempt",
                    return_value=_ImmediateConnectionAttempt(mock.Mock(), object()),
                ),
                mock.patch.object(
                    client_module,
                    "_BidiStream",
                    self._immediate_stream_class(
                        lambda batch: fed.extend(batch.points)
                    ),
                ),
            ):
                worker = _worker_thread(
                    self, source, status, os.path.join(spool_dir, "text.mkspool")
                )
                worker.join(timeout=5)

        self.assertFalse(worker.is_alive())
        self.assertEqual(
            [(point.metric_name, point.step, point.text_data) for point in fed],
            [("a", 1, b""), ("logs/std_out", 5, b"out")],
        )
        self.assertEqual(status.value, 0)

    def test_partial_ack_then_break_refeeds_only_unacked_suffix(self):
        stream_attempts = 0
        fed_steps = []

        class ScriptedStream(self._immediate_stream_class(lambda _batch: None)):
            def __init__(self, stub, feed_cap):
                nonlocal stream_attempts
                super().__init__(stub, feed_cap)
                stream_attempts += 1
                self.attempt = stream_attempts

            def feed(self, batch):
                steps = [point.step for point in batch.points]
                fed_steps.append(steps)
                if self.attempt == 1:
                    self.events.put(("ack", 1))
                    self.events.put(("error", RuntimeError("injected break")))
                else:
                    self.events.put(("ack", len(steps)))
                return True

        source, status = _charged_queue([numeric(1), numeric(2), numeric(3)], None)
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                _connected(ScriptedStream),
                mock.patch.object(wire_module, "_SEND_INITIAL_DELAY", 0.0),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.01),
            ):
                worker = _worker_thread(
                    self, source, status, os.path.join(spool_dir, "partial-ack.mkspool")
                )
                worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(fed_steps, [[1, 2, 3], [2, 3]])
        self.assertEqual(status.value, 0)

    def test_ack_watchdog_cancels_and_retries_before_deadline_spill(self):
        stream_attempts = 0
        cancel_times = []

        class SilentStream:
            def __init__(self, _stub, _feed_cap):
                nonlocal stream_attempts
                stream_attempts += 1
                self.fed = False

            def feed_has_room(self):
                return not self.fed

            def feed(self, _batch):
                self.fed = True
                return True

            @staticmethod
            def poll_acks():
                return []

            @staticmethod
            def half_close():
                # Request EOF is correct even with unacked work; this fake then
                # hangs forever without the final ACK/response EOF.
                return True

            def cancel(self):
                cancel_times.append(time.monotonic())

        source, status = _charged_queue([numeric(1)], None)
        deadline = _QueueStatus(time.monotonic() + 0.2)
        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "watchdog.mkspool")
            with (
                _connected(SilentStream),
                mock.patch.object(wire_module, "_SEND_INITIAL_DELAY", 0.01),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.005),
                mock.patch.object(client_module, "_ACTIVE_POLL_TIMEOUT", 0.005),
                mock.patch.dict(os.environ, {"KYMO_ACK_PROGRESS_TIMEOUT": "0.03"}),
            ):
                worker = _worker_thread(
                    self, source, status, spool_path, shutdown_deadline=deadline
                )
                worker.join(timeout=2)

            self.assertTrue(os.path.exists(spool_path))

        self.assertFalse(worker.is_alive())
        self.assertGreaterEqual(stream_attempts, 2)
        self.assertTrue(cancel_times)
        self.assertEqual(status.value, 0)

    def test_shutdown_eof_flushes_a_partial_unacked_tail(self):
        class EofFlushStream(self._immediate_stream_class(lambda _batch: None)):
            def feed(self, batch):
                self.cumulative += len(batch.points)
                return True

            def half_close(self):
                self.events.put(("ack", self.cumulative))
                return super().half_close()

        source, status = _charged_queue([numeric(1), numeric(2), numeric(3)], None)
        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "eof-tail.mkspool")
            with (
                _connected(EofFlushStream),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.01),
            ):
                worker = _worker_thread(self, source, status, spool_path)
                worker.join(timeout=1)

            self.assertFalse(os.path.exists(spool_path))

        self.assertFalse(worker.is_alive())
        self.assertEqual(status.value, 0)

    def test_shutdown_eof_survives_a_full_feed_window(self):
        release_requests = threading.Event()
        stream_instances = []

        class FinalFlushCall:
            def __init__(self):
                self.requests = None
                self.cancelled = False

            def __iter__(self):
                release_requests.wait(timeout=5)
                total = sum(len(batch.points) for batch in self.requests)
                if not self.cancelled:
                    yield types.SimpleNamespace(points_acked=total)

            def cancel(self):
                self.cancelled = True

        call = FinalFlushCall()

        class FinalFlushStub:
            @staticmethod
            def IngestMetricsBidi(requests, timeout=None):
                call.requests = requests
                return call

        real_stream = client_module._BidiStream

        class RecordingStream(real_stream):
            def __init__(self, stub, feed_cap):
                self.half_close_attempts = 0
                super().__init__(stub, feed_cap)
                stream_instances.append(self)

            def half_close(self):
                self.half_close_attempts += 1
                return super().half_close()

        source, status = _charged_queue([numeric(i) for i in range(4)], None)
        deadline = _QueueStatus(time.monotonic() + 10)
        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "full-window.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_ConnectionAttempt",
                    return_value=_ImmediateConnectionAttempt(
                        _Channel(), FinalFlushStub()
                    ),
                ),
                mock.patch.object(client_module, "_BidiStream", RecordingStream),
                mock.patch.object(client_module, "_MAX_UNACKED_POINTS", 4),
                mock.patch.object(wire_module, "_MAX_POINTS_PER_MSG", 2),
                mock.patch.object(client_module, "_FEED_Q_CAP", 2),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.005),
                mock.patch.object(client_module, "_ACTIVE_POLL_TIMEOUT", 0.005),
            ):
                worker = _worker_thread(
                    self, source, status, spool_path, shutdown_deadline=deadline
                )
                _wait_for(
                    lambda: (
                        stream_instances
                        and stream_instances[0]._feed_q.full()
                        and stream_instances[0].half_close_attempts
                    )
                )
                self.assertTrue(stream_instances)
                stream = stream_instances[0]
                self.assertTrue(stream._feed_q.full())
                self.assertGreater(stream.half_close_attempts, 0)
                self.assertFalse(stream._dead.is_set())
                release_requests.set()
                worker.join(timeout=5)
                if worker.is_alive():
                    deadline.value = time.monotonic() - 1
                    worker.join(timeout=2)

            self.assertFalse(os.path.exists(spool_path))

        self.assertFalse(worker.is_alive())
        self.assertEqual(status.value, 0)

    def test_blocked_rich_write_does_not_stall_direct_cdn_bidi_point(self):
        rich_started = threading.Event()
        release_rich = threading.Event()
        direct_fed = threading.Event()
        commits = []

        def process_rich(
            _stub,
            _http_client,
            _cdn_url,
            _project_id,
            _run_id,
            _name,
            _step,
            _items,
            **_kwargs,
        ):
            rich_started.set()
            release_rich.wait(timeout=3)
            commits.append("rich-manifest")
            return True

        def record_bidi_leak(batch):
            for point in batch.points:
                if point.cdn_key == "new-direct-key":
                    commits.append("new-direct-key")
                    direct_fed.set()

        source = queue.Queue()
        source.put(
            [
                (
                    "cdn_batch",
                    "demo/gallery",
                    7,
                    [Resource(b"old-rich", "bin")],
                )
            ]
        )
        status = _QueueStatus(2)

        with tempfile.TemporaryDirectory() as spool_dir:
            with contextlib.ExitStack() as stack:
                stack.enter_context(
                    _connected(self._immediate_stream_class(record_bidi_leak))
                )
                stack.enter_context(_channel_stand_ins())
                stack.enter_context(
                    mock.patch("httpx.Client", return_value=self._HttpClient())
                )
                stack.enter_context(
                    mock.patch.object(
                        client_module, "_process_cdn_batch", side_effect=process_rich
                    )
                )
                worker = _worker_thread(
                    self,
                    source,
                    status,
                    os.path.join(spool_dir, "cdn-order.mkspool"),
                    cdn_url="http://unused",
                )
                self.assertTrue(rich_started.wait(timeout=1))
                source.put(
                    [
                        (
                            "cdn_ts",
                            "demo/gallery",
                            7,
                            "new-direct-key",
                            1_700_000_000_001,
                        )
                    ]
                )
                source.put(None)
                try:
                    direct_progressed = direct_fed.wait(timeout=0.5)
                finally:
                    release_rich.set()
                    worker.join(timeout=3)

        self.assertTrue(direct_progressed)
        self.assertFalse(worker.is_alive())
        self.assertEqual(commits, ["new-direct-key", "rich-manifest"])
        self.assertEqual(status.value, 0)

    def test_first_spool_failure_breaks_the_write_loop_and_accounts_suffix(self):
        source = queue.Queue()
        source.put([numeric(1), numeric(2), numeric(3)])
        source.put(None)

        with mock.patch.object(
            client_module, "_spill_tuple", side_effect=OSError("disk full")
        ) as spill:
            code, status = _worker_past_its_deadline(source, points=3)

        self.assertEqual(code, client_module._WORKER_EXIT_SPOOL_FAILED)
        self.assertEqual(spill.call_count, 1)
        self.assertEqual(status.value, 0)

    def test_wait_does_not_report_success_after_spool_failure_before_worker_exit(self):
        source = queue.Queue()
        source.put([numeric(1), numeric(2), numeric(3)])
        status = _QueueStatus(3)
        failure = _QueueStatus()
        deadline = _QueueStatus()
        spill_failed = threading.Event()

        def fail_spill(_spool, _point):
            spill_failed.set()
            raise OSError("disk full")

        with tempfile.TemporaryDirectory() as spool_dir:
            with contextlib.ExitStack() as stack:
                stack.enter_context(
                    mock.patch.object(
                        client_module,
                        "_ConnectionAttempt",
                        return_value=_ImmediateConnectionAttempt(),
                    )
                )
                stack.enter_context(
                    mock.patch.object(
                        client_module, "_spill_tuple", side_effect=fail_spill
                    )
                )
                stack.enter_context(
                    mock.patch.dict(os.environ, {"KYMO_MAX_BUFFER_POINTS": "0"})
                )
                # This deliberately omits the parent sentinel so the worker's
                # bounded missing-sentinel fallback is part of the scenario.
                stack.enter_context(
                    mock.patch.object(client_module, "_QUEUE_DRAIN_SILENCE", 0.01)
                )
                worker = _worker_thread(
                    self,
                    source,
                    status,
                    os.path.join(spool_dir, "wait-race.mkspool"),
                    shutdown_deadline=deadline,
                    upload_failure=failure,
                )
                self.assertTrue(spill_failed.wait(timeout=1))

                _wait_for(lambda: failure.value, timeout=1)
                self.assertEqual(failure.value, client_module._WORKER_EXIT_SPOOL_FAILED)

                with (
                    _upload_state(_queue_status=status, _upload_failure=failure),
                    self.assertRaisesRegex(RuntimeError, "could not spool"),
                ):
                    client_module.wait_for_upload(timeout=0)

                deadline.value = time.monotonic() - 1
                worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(worker.code, client_module._WORKER_EXIT_SPOOL_FAILED)
        with status.get_lock():
            self.assertEqual(
                (status.value, failure.value),
                (0, client_module._WORKER_EXIT_SPOOL_FAILED),
            )


def tearDownModule():
    """A poller thread that outlives its test keeps publishing into the next one's state: fail the module run if any still runs."""
    left = [thread for thread in threading.enumerate() if thread.name == "kymo-poller"]
    if left:
        raise AssertionError(f"{len(left)} poller thread(s) outlived their test")


if __name__ == "__main__":
    unittest.main()
