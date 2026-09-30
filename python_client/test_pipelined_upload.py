"""Focused, service-free checks for the bidi upload accounting boundary."""

import contextlib
import io
import json
import math
import multiprocessing
import os
import pickle
import queue
import shlex
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
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
from kymo._local_runtime import (
    LocalInstallationMismatch,
    endpoint_from_worker_config,
)
from kymo.spool import SpoolWriter, make_spool_path, read_spool
from kymo.system_metrics import SystemMetricsPoller
from kymo.types import Image, Metadata, Resource

try:
    import torch
except ImportError:
    # CI installs no torch: _StubTensor covers the device branches there, and the real-tensor tests skip.
    torch = None


def numeric(index: int) -> tuple:
    return ("numeric_ts", "train/loss", index, float(index), 1_700_000_000_000)


def local_endpoint_config() -> dict:
    return {
        "protocol_min": 2,
        "protocol_max": 2,
        "installation_uuid": "11111111-1111-4111-8111-111111111111",
        "endpoint_generation": "22222222-2222-4222-8222-222222222222",
        "native_socket": "/private/tmp/mkdb2/native.sock",
        "upload_socket": "/private/tmp/mkdb2/upload.sock",
        "dashboard_origin": "http://127.0.0.1:49152",
        "cdn_origin": "http://127.0.0.1:49153",
        "server_bearer": "A" * 43,
    }


def _report_callable_identity(fn, connection) -> None:
    """Spawn target proving that `fn` can be restored by module path."""
    connection.send((fn.__module__, fn.__name__))
    connection.close()


class _ImmediateConnectionAttempt:
    def __init__(self, channel=None, stub=None):
        self.result = channel, stub

    def poll(self, deadline_fn=None):
        del deadline_fn
        return self.result

    def cancel(self):
        pass


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
        writer = capture_module.TeeWriter(original, buffer, stats, threading.Lock())
        return writer, original, buffer, stats

    def test_writelines_captures_a_lazy_iterable(self):
        writer, original, buffer, stats = self.writer()

        result = writer.writelines(line for line in ("alpha", "\n", "beta"))

        self.assertIsNone(result)
        self.assertEqual(original.getvalue(), "alpha\nbeta")
        self.assertEqual(
            capture_module._drain_one(buffer, stats, writer._lock), "alpha\nbeta"
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
        writer = capture_module.TeeWriter(terminal, buffer, stats, threading.Lock())
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
        self.assertEqual(chunks[0][1], 2)
        self.assertEqual(chunks[0][0][1].text_data.decode("utf-8"), captured)

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
        result = subprocess.run(
            [sys.executable, "-c", script],
            check=False,
            capture_output=True,
            text=True,
        )

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
        result = subprocess.run(
            [sys.executable, "-c", script],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, '"ok"')
        self.assertEqual(result.stderr, "")

    @unittest.skipUnless(hasattr(os, "fork"), "requires os.fork")
    def test_fork_child_drops_the_parents_pending_capture(self):
        script = r"""
import json
import os

import kymo
from kymo import _capture

_capture.drain_buffers()
with _capture._stdout_lock:
    _capture._stdout_buf.append("parent-out")
    _capture._stdout_stats.update(size=10, dropped=3)
with _capture._stderr_lock:
    _capture._stderr_buf.append("parent-err")
    _capture._stderr_stats.update(size=10, dropped=4)

read_fd, write_fd = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(read_fd)
    payload = json.dumps(_capture.drain_buffers()).encode()
    os.write(write_fd, payload)
    os.close(write_fd)
    os._exit(0)

os.close(write_fd)
payload = bytearray()
while True:
    chunk = os.read(read_fd, 4096)
    if not chunk:
        break
    payload.extend(chunk)
os.close(read_fd)
_, status = os.waitpid(pid, 0)
assert os.waitstatus_to_exitcode(status) == 0, status
assert json.loads(payload) == ["", ""], payload
assert _capture.drain_buffers() == (
    "[kymo: 3 chars of captured output dropped]\nparent-out",
    "[kymo: 4 chars of captured output dropped]\nparent-err",
)
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            check=False,
            capture_output=True,
            text=True,
        )

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

    def test_termination_signals_skip_platform_constants_that_do_not_exist(self):
        signal_module = types.SimpleNamespace(SIGTERM=15, SIGINT=2)
        self.assertEqual(client_module._termination_signals(signal_module), (15, 2))

    def test_signal_setup_preserves_ignored_dispositions(self):
        with (
            mock.patch.object(
                client_module, "_termination_signals", return_value=(15, 2)
            ),
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
            mock.patch.object(client_module, "_drain_and_shutdown") as drain,
            mock.patch.object(client_module.sys, "exit", side_effect=SystemExit(143)),
            self.assertLogs("kymo", level="ERROR") as logs,
            self.assertRaisesRegex(SystemExit, "143"),
        ):
            client_module._signal_handler(15, None)
        drain.assert_called_once_with(flush_timeout=client_module._SIGNAL_FLUSH_TIMEOUT)
        self.assertIn("MKDB2_SIGNAL_FLUSH_TIMEOUT", "\n".join(logs.output))

        with (
            mock.patch.dict(os.environ, {"MKDB2_FLUSH_TIMEOUT": "3"}),
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_drain_and_shutdown") as drain,
            self.assertLogs("kymo", level="ERROR") as logs,
        ):
            client_module._ensure_metrics_uploaded()
        drain.assert_called_once_with(
            flush_timeout=client_module._DEFAULT_FLUSH_TIMEOUT
        )
        self.assertIn("MKDB2_FLUSH_TIMEOUT", "\n".join(logs.output))

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

    def test_caught_keyboard_interrupt_does_not_poison_a_clean_finish(self):
        def interrupt(_signum, _frame):
            raise KeyboardInterrupt

        with (
            mock.patch.object(client_module, "_original_signals", {2: interrupt}),
            mock.patch.object(client_module, "_exit_code", None),
            self.assertRaises(KeyboardInterrupt),
        ):
            client_module._signal_handler(2, None)

        self.assertIsNone(client_module._exit_code)

    def test_uncaught_keyboard_interrupt_records_the_shell_exit_code(self):
        with (
            mock.patch.object(client_module, "_exit_code", None),
            mock.patch.object(client_module, "_original_excepthook"),
        ):
            client_module._excepthook(KeyboardInterrupt, KeyboardInterrupt(), None)
            self.assertEqual(client_module._exit_code, 130)

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

    def test_init_clears_previous_run_state_recorded_between_runs(self):
        run_info = types.SimpleNamespace(run_name="canonical run", ordinal=1)
        process = mock.Mock()

        with (
            mock.patch.multiple(
                client_module,
                _is_initialized=False,
                _exit_code=7,
                _last_log_duration_ms=12.5,
                _project_id="",
                _run_id="",
                _run_name="",
                _server_address="",
                _cdn_address="",
                _mode="hosted",
                _local_installation_uuid="",
                _url_base="",
                _metric_queue=None,
                _queue_status=None,
                _upload_failure=None,
                _upload_spooled=None,
                _upload_terminal=None,
                _shutdown_deadline=None,
                _spool_dir=None,
                _session_id="",
                _spool_path="",
                _upload_process=None,
                _system_poller=None,
                _init_pid=None,
                _atexit_registered=True,
            ),
            mock.patch.object(
                client_module,
                "_run_control_rpc",
                return_value=types.SimpleNamespace(
                    run=run_info, HasField=lambda _name: False
                ),
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
            mock.patch.object(client_module, "_upload_run_metadata"),
        ):
            client_module.init(
                server_address="127.0.0.1:50051",
                url_base="http://dash:8081/",
                project_id="project",
                run_name="proposed replacement",
                run_id="run-id",
                mode="hosted",
                system_metrics=False,
                spool_dir="relative-spool",
            )
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

    def test_live_image_memory_error_does_not_commit_a_reduced_manifest(self):
        stub = mock.Mock()
        upload = mock.Mock()
        with (
            mock.patch.object(
                client_module, "_encode_image", side_effect=MemoryError("pressure")
            ),
            mock.patch.object(client_module, "_upload_to_cdn", upload),
            self.assertRaises(MemoryError),
        ):
            client_module._process_cdn_batch(
                stub,
                object(),
                "unused",
                "project",
                "run",
                "demo/image",
                1,
                [Image(b"valid")],
                send_placeholder=False,
            )

        upload.assert_not_called()
        stub.IngestMetrics.assert_not_called()

    def test_spool_image_memory_error_does_not_write_a_reduced_record(self):
        spool = mock.Mock()
        with (
            mock.patch.object(
                client_module, "_encode_image", side_effect=MemoryError("pressure")
            ),
            self.assertRaises(MemoryError),
        ):
            client_module._spill_tuple(
                spool,
                ("cdn_batch", "demo/image", 1, [Image(b"valid")]),
            )

        spool.write.assert_not_called()

    def test_retained_lane_keeps_items_sizes_and_total_in_sync(self):
        lane = client_module._RetainedLane()
        lane.append(numeric(1), 100)
        lane.append(numeric(2), 200)

        lane.discard_prefix(1)
        self.assertEqual(lane.items, [numeric(2)])
        self.assertEqual(lane.sizes, [200])
        self.assertEqual(lane.total_bytes, 200)

        lane.clear()
        self.assertFalse(lane)
        self.assertEqual(lane.total_bytes, 0)

    def test_message_build_reuses_cached_byte_estimates(self):
        tuples = [numeric(0), numeric(1)]
        with mock.patch.object(
            client_module,
            "_estimate_tuple_bytes",
            side_effect=AssertionError("re-encoded a retained point"),
        ):
            batch, consumed = client_module._next_batch("p", "r", tuples, [100, 100])

        self.assertEqual(consumed, 2)
        self.assertEqual(len(batch.points), 2)

    def test_message_count_cap_splits_without_dropping_the_edge(self):
        tuples = [numeric(i) for i in range(5_001)]
        first, consumed = client_module._next_batch("p", "r", tuples)
        self.assertEqual(consumed, 5_000)
        self.assertEqual(len(first.points), 5_000)

        second, consumed = client_module._next_batch("p", "r", tuples[consumed:])
        self.assertEqual(consumed, 1)
        self.assertEqual([point.step for point in second.points], [5_000])

    def test_message_byte_cap_splits_before_large_text(self):
        tuples = [numeric(0), ("text_ts", "logs/std_out", 1, "x" * 2_100_000, 1)]
        first, consumed = client_module._next_batch("p", "r", tuples)
        self.assertEqual(consumed, 1)
        self.assertEqual(len(first.points), 1)

        second, consumed = client_module._next_batch("p", "r", tuples[1:])
        self.assertEqual(consumed, 1)
        self.assertEqual(len(second.points), 1)


class AccountingTests(unittest.TestCase):
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
            contender.append(client_module._next_rich_mutation_version())
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
            client_module._next_rich_mutation_version()
        sequence.get_lock.return_value.acquire.assert_called_once_with(
            timeout=client_module._STATUS_LOCK_TIMEOUT
        )
        sequence.get_lock.return_value.release.assert_not_called()

    def test_run_metadata_uses_one_immutable_ordered_worker_record(self):
        metadata = {"config": {"learning_rate": 0.1}}
        with (
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_run_metadata", metadata),
            mock.patch.object(client_module, "_publish_queue_items") as publish,
        ):
            client_module._push_run_metadata()
            queued = publish.call_args.args[0]
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
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_metric_queue", mock.Mock()),
            mock.patch.object(client_module, "_queue_status", status),
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
        class AliveProcess:
            @staticmethod
            def is_alive():
                return True

        with (
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_queue_status", _QueueStatus(1)),
            mock.patch.object(client_module, "_upload_failure", _QueueStatus()),
            mock.patch.object(client_module, "_upload_spooled", _QueueStatus()),
            mock.patch.object(client_module, "_upload_process", AliveProcess()),
            self.assertLogs("kymo", level="WARNING"),
        ):
            self.assertFalse(client_module.wait_for_upload(timeout=float("nan")))

    def test_upload_wait_sleep_is_bounded_by_its_deadline(self):
        class AliveProcess:
            @staticmethod
            def is_alive():
                return True

        sleeps = []
        with (
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_queue_status", _QueueStatus(1)),
            mock.patch.object(client_module, "_upload_failure", _QueueStatus()),
            mock.patch.object(client_module, "_upload_spooled", _QueueStatus()),
            mock.patch.object(client_module, "_upload_process", AliveProcess()),
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

        class AliveProcess:
            @staticmethod
            def is_alive():
                return True

        with (
            mock.patch.object(client_module, "_queue_status", StuckStatus()),
            mock.patch.object(client_module, "_upload_failure", _QueueStatus()),
            mock.patch.object(client_module, "_upload_process", AliveProcess()),
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_STATUS_LOCK_TIMEOUT", 0.01),
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
        class Target:
            def __init__(self):
                self.items = []

            def put(self, item):
                self.items.append(item)

        target = Target()
        status = _QueueStatus()
        payload = bytearray(b"before")
        _OneShotReducible.calls = 0
        with (
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_metric_queue", target),
            mock.patch.object(client_module, "_queue_status", status),
            mock.patch.object(client_module, "_last_log_duration_ms", 0.0),
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
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_metric_queue", target),
            mock.patch.object(client_module, "_queue_status", _QueueStatus()),
            mock.patch.object(client_module, "_last_log_duration_ms", 0.0),
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
                    ((_, _, payload),) = client_module._snapshot_rich_queue_items(
                        [batch]
                    )
                (decoded,) = client_module._decode_queue_item(
                    (client_module._SERIALIZED_RICH_QUEUE_ITEM, 1, payload)
                )
                self.assertEqual(decoded[:3] + decoded[4:], batch[:3] + batch[4:])
                self.assertEqual(
                    client_module.metadata_manifest(decoded[3].data), manifest
                )

    def test_unpicklable_rich_payload_fails_before_capture_or_publication(self):
        class Target:
            items = []

            def put(self, item):
                self.items.append(item)

        target = Target()
        status = _QueueStatus()
        capture = mock.Mock(return_value=("captured", ""))
        with (
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_metric_queue", target),
            mock.patch.object(client_module, "_queue_status", status),
            mock.patch.object(client_module, "_last_log_duration_ms", 0.0),
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
                        mock.patch.object(client_module, "_is_initialized", True),
                        mock.patch.object(client_module, "_metric_queue", target),
                        mock.patch.object(client_module, "_queue_status", status),
                        mock.patch.object(client_module, "_last_log_duration_ms", 0.0),
                        mock.patch.object(client_module, "_rich_writer_epoch", epoch),
                        mock.patch.object(client_module, "_rich_mutation_seq", seq),
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
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_metric_queue", target),
            mock.patch.object(client_module, "_queue_status", status),
            mock.patch.object(client_module, "_last_log_duration_ms", 0.0),
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
        class Target:
            def __init__(self):
                self.items = []

            def put(self, item):
                self.items.append(item)

        target = Target()
        status = _QueueStatus()
        with (
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_metric_queue", target),
            mock.patch.object(client_module, "_queue_status", status),
            mock.patch.object(client_module, "_last_log_duration_ms", 0.0),
            mock.patch("kymo._capture.drain_buffers", return_value=("", "")),
        ):
            client_module.log({"demo/tags": [1.0, np.float32(2)]}, step=1)
            client_module._last_log_duration_ms = 0.0
            client_module.log({"demo/tags": [np.float32(2), 1.0]}, step=1)

        self.assertEqual(
            [[point[:5] for point in batch] for batch in target.items],
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
        class Target:
            def __init__(self):
                self.items = []

            def put(self, item):
                self.items.append(item)

        target = Target()
        status = _QueueStatus()
        with (
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_metric_queue", target),
            mock.patch.object(client_module, "_queue_status", status),
            mock.patch.object(client_module, "_last_log_duration_ms", 0.0),
            mock.patch("kymo._capture.drain_buffers", return_value=("", "")),
        ):
            client_module.log(
                {"train/loss": 1.0, "eval/per_class": []},
                step=1,
            )
            client_module._last_log_duration_ms = 0.0
            client_module.log({"eval/per_class": []}, step=2)

        self.assertEqual(
            [[point[:4] for point in batch] for batch in target.items],
            [[("numeric_ts", "train/loss", 1, 1.0)]],
        )
        self.assertEqual(status.value, 1)

    def test_explicit_log_overhead_metric_is_not_overwritten(self):
        class Target:
            def __init__(self):
                self.items = []

            def put(self, item):
                self.items.append(item)

        target = Target()
        status = _QueueStatus()
        with (
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_metric_queue", target),
            mock.patch.object(client_module, "_queue_status", status),
            mock.patch.object(client_module, "_last_log_duration_ms", 12.5),
            mock.patch("kymo._capture.drain_buffers", return_value=("", "")),
        ):
            client_module.log({"system/log_overhead_ms": 99.0}, step=1)
            client_module._last_log_duration_ms = 12.5
            client_module.log({"train/loss": 1.0}, step=2)

        self.assertEqual(len(target.items), 2)
        self.assertEqual(len(target.items[0]), 1)
        self.assertEqual(
            target.items[0][0][:4],
            ("numeric_ts", "system/log_overhead_ms", 1, 99.0),
        )
        self.assertEqual(
            [point[:4] for point in target.items[1]],
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
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_metric_queue", target),
            mock.patch.object(client_module, "_queue_status", status),
            mock.patch.object(client_module, "_last_log_duration_ms", 0.0),
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
                    mock.patch.object(client_module, "_is_initialized", True),
                    mock.patch.object(client_module, "_metric_queue", target),
                    mock.patch.object(client_module, "_queue_status", _QueueStatus()),
                    mock.patch.object(client_module, "_last_log_duration_ms", 0.0),
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
            (batch,) = self._log(metrics)
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
        class Target:
            def __init__(self):
                self.items = []

            def put(self, item):
                self.items.append(item)

        target = Target()
        status = _QueueStatus()
        _StepLike.calls = 0
        with (
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_metric_queue", target),
            mock.patch.object(client_module, "_queue_status", status),
            mock.patch.object(client_module, "_last_log_duration_ms", 0.0),
            mock.patch("kymo._capture.drain_buffers", return_value=("", "")),
        ):
            client_module.log({_StringSubclass("train/loss"): 1.0}, _StepLike())
            client_module.log_cdn(
                {_StringSubclass("demo/image"): _StringSubclass("key.png")},
                _StepLike(),
            )

        numeric_point = target.items[0][0]
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
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_metric_queue", target),
            mock.patch.object(client_module, "_queue_status", _QueueStatus()),
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

    def test_partial_queue_publication_rolls_back_only_unpublished_suffix(self):
        class FailingSecondPut:
            def __init__(self):
                self.items = []

            def put(self, item):
                if self.items:
                    raise OSError("queue closed")
                self.items.append(item)

        target = FailingSecondPut()
        status = _QueueStatus()
        with (
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_metric_queue", target),
            mock.patch.object(client_module, "_queue_status", status),
            mock.patch.object(client_module, "_last_log_duration_ms", 0.0),
            mock.patch("kymo._capture.drain_buffers", return_value=("", "")),
        ):
            with self.assertRaisesRegex(OSError, "queue closed"):
                client_module.log(
                    {
                        "train/loss": 1.0,
                        "demo/resource": Resource(b"payload", "demo.bin"),
                    },
                    step=1,
                )

        self.assertEqual(len(target.items), 1)
        self.assertEqual(len(target.items[0]), 1)
        self.assertEqual(status.value, 1)


class _GpuScalar:
    """Stands in for a CUDA scalar tensor."""

    def __init__(self, value):
        self.value = value


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


def _hold_pending_lock(case) -> None:
    """Hold client._pending_lock on another thread until the test ends."""
    locked, release = threading.Event(), threading.Event()

    def hold():
        with client_module._pending_lock:
            locked.set()
            release.wait()

    holder = threading.Thread(target=hold)
    holder.start()
    case.addCleanup(holder.join)
    case.addCleanup(release.set)
    locked.wait()


def _patch_log_client(case, *extra) -> mock.Mock:
    """Patch the client state a log() call needs, plus extra patches, for one test; return the queue it publishes to."""
    queue = mock.Mock()
    for patch in (
        mock.patch.object(client_module, "_is_initialized", True),
        mock.patch.object(client_module, "_init_pid", os.getpid()),
        mock.patch.object(client_module, "_metric_queue", queue),
        mock.patch.object(client_module, "_queue_status", _QueueStatus()),
        mock.patch.object(client_module, "_last_log_duration_ms", 0.0),
        mock.patch.object(client_module, "_pending", deque()),
        mock.patch.object(client_module, "_pending_incomplete", False),
        mock.patch.object(gpu_module, "unfreeable", []),
        mock.patch.object(client_module, "_rich_writer_epoch", None),
        mock.patch.object(capture_module, "drain_buffers", return_value=("", "")),
        *extra,
    ):
        patch.start()
        case.addCleanup(patch.stop)
    return queue


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

        fake = types.SimpleNamespace(
            cuda=types.SimpleNamespace(
                OutOfMemoryError=OutOfMemoryError,
                device_count=lambda: 1,
                device=lambda device: contextlib.nullcontext(),
            ),
        )
        scalars = [types.SimpleNamespace(requires_grad=False)]
        with (
            mock.patch.object(gpu_module, "unfreeable", []),
            mock.patch.object(gpu_module, "gather") as gather,
        ):
            gather.side_effect = OutOfMemoryError
            with self.assertRaises(OutOfMemoryError):
                gpu_module.Reads(fake, scalars, [])
            self.assertEqual(gpu_module.unfreeable, [])
            gather.side_effect = RuntimeError("CUDA error: an illegal memory access")
            with self.assertRaises(RuntimeError):
                gpu_module.Reads(fake, scalars, [])
            self.assertEqual(len(gpu_module.unfreeable), 1)

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

    def test_captured_text_never_waits_for_gpu_values(self):
        with mock.patch.object(
            capture_module, "drain_buffers", return_value=("out", "")
        ):
            client_module.log({"gpu": _GpuScalar(1.0)}, step=1)

        self.assertEqual(
            self.published(), [[("text_ts", "logs/std_out", 1234, "out", 1234)]]
        )
        self.assertEqual(len(client_module._pending), 1)

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
        _hold_pending_lock(self)
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

        self.assertEqual(seen, [True])
        # Only step 2's own entry is left: that handler would not publish step 1 again.
        self.assertEqual(len(client_module._pending), 1)
        self.assertIsNone(client_module._pending[0].reads)
        self.assertTrue(client_module._pending_incomplete)

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
        # Freeing their pinned host copies would now abort the process.
        self.assertEqual(gpu_module.unfreeable, self.reads)
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
        parent_lock = client_module._pending_lock
        # Restore the parent's lock with the rest of the patched state.
        self.addCleanup(setattr, client_module, "_pending_lock", parent_lock)

        client_module._pending_incomplete = True

        client_module._forget_pending_in_child()

        self.assertEqual(len(client_module._pending), 0)
        self.assertFalse(client_module._pending_incomplete)
        self.assertIsNot(client_module._pending_lock, parent_lock)
        # Freeing the parent's CUDA objects would abort the child.
        self.assertEqual(gpu_module.unfreeable, self.reads)


_REQUIRES_CUDA = unittest.skipUnless(
    torch is not None and torch.cuda.is_available(), "requires a CUDA device"
)


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

    def _run_script(
        self, script: str, env: dict | None = None
    ) -> subprocess.CompletedProcess:
        # On Python 3.12, os.fork() in a threaded process warns before the parent's at-fork handlers release kymo's capture locks, and printing that warning deadlocks (AI-1518).
        return subprocess.run(
            [
                sys.executable,
                "-W",
                "ignore::DeprecationWarning",
                "-c",
                f"BACKEND = {self.backend!r}\n" + self._SUBPROCESS_PRELUDE + script,
            ],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            env={**os.environ, **(env or {})},
            capture_output=True,
            text=True,
            timeout=120,
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
            env={"PYTORCH_CUDA_ALLOC_CONF": "expandable_segments:True"},
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


class QueueDrainTests(unittest.TestCase):
    class ScriptedQueue:
        def __init__(self, events=()):
            self.events = list(events)
            self.puts = []

        def put(self, item):
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

    def test_owner_drain_uses_a_fresh_fifo_fence_past_empty_reads(self):
        before_stale_sentinel = [numeric(1)]
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

        with (
            mock.patch.object(client_module, "_init_pid", os.getpid()),
            mock.patch.object(client_module, "_metric_queue", source),
            mock.patch.object(client_module, "_spill_tuple", return_value=1) as spill,
        ):
            salvaged = client_module._drain_queue_to_spool(object())

        self.assertEqual(salvaged, 2)
        self.assertEqual(spill.call_count, 2)
        self.assertEqual(len(source.puts), 1)
        self.assertEqual(source.puts[0][0], "__kymo_owner_drain__")
        self.assertEqual(source.events, [])

    def test_owner_drain_reports_a_missing_fence(self):
        source = self.ScriptedQueue()
        hard_deadline = time.monotonic() + 0.01

        def drop_fence(item):
            source.puts.append(item)

        with (
            mock.patch.object(client_module, "_init_pid", os.getpid()),
            mock.patch.object(client_module, "_metric_queue", source),
            mock.patch.object(source, "put", side_effect=drop_fence),
            self.assertLogs("kymo", level="WARNING") as logs,
        ):
            salvaged = client_module._drain_queue_to_spool(
                object(), hard_deadline=hard_deadline
            )

        self.assertEqual(salvaged, 0)
        self.assertEqual(len(source.puts), 1)
        self.assertIn("ended before its owner fence", "\n".join(logs.output))

    def test_non_owner_cannot_publish_a_queue_fence(self):
        source = self.ScriptedQueue()
        with (
            mock.patch.object(client_module, "_init_pid", os.getpid() + 1),
            mock.patch.object(client_module, "_metric_queue", source),
            self.assertRaisesRegex(RuntimeError, "initializing process"),
        ):
            client_module._drain_queue_to_spool(object())

        self.assertEqual(source.puts, [])

    def test_worker_drain_never_puts_and_stops_at_parent_sentinel(self):
        expected = [numeric(1)]
        source = self.ScriptedQueue([expected, None])
        drained = []
        complete = client_module._drain_worker_queue(
            source, drained.append, input_closed=False
        )

        self.assertTrue(complete)
        self.assertEqual(drained, [expected])
        self.assertEqual(source.puts, [])
        self.assertEqual(source.events, [])

    def test_worker_missing_sentinel_uses_bounded_silence_without_a_fence(self):
        source = self.ScriptedQueue()
        started = time.monotonic()
        with mock.patch.object(client_module, "_QUEUE_DRAIN_SILENCE", 0.01):
            complete = client_module._drain_worker_queue(
                source, lambda _item: None, input_closed=False
            )

        self.assertFalse(complete)
        self.assertEqual(source.puts, [])
        self.assertLess(time.monotonic() - started, 0.2)

    def test_consumed_parent_sentinel_is_authoritative(self):
        class NoTouchQueue:
            @staticmethod
            def get(timeout=None):
                raise AssertionError(f"unexpected get({timeout})")

            @staticmethod
            def put(_item):
                raise AssertionError("worker published a fence")

        complete = client_module._drain_worker_queue(
            NoTouchQueue(), lambda _item: None, input_closed=True
        )
        self.assertTrue(complete)


class ShutdownProofTests(unittest.TestCase):
    class _Queue:
        def __init__(self):
            self.items = []

        def put(self, item):
            self.items.append(item)

        def cancel_join_thread(self):
            pass

        def close(self):
            pass

    class _CleanProcess:
        exitcode = 0

        @staticmethod
        def is_alive():
            return False

    class _Spool:
        def __init__(self, *_args, **_kwargs):
            pass

        @staticmethod
        def close():
            return None

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
    ) -> bool:
        target = target or self._Queue()
        status = status or _QueueStatus(backlog)
        if poller is None:
            poller = mock.Mock()
            poller.stop.return_value = poller_quiesced
        capture_patch = (
            mock.patch.object(capture_module, "drain_buffers", side_effect=capture)
            if callable(capture)
            else mock.patch.object(
                capture_module, "drain_buffers", return_value=capture
            )
        )
        with (
            tempfile.TemporaryDirectory() as spool_dir,
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_init_pid", os.getpid()),
            mock.patch.object(client_module, "_metric_queue", target),
            mock.patch.object(client_module, "_queue_status", status),
            mock.patch.object(client_module, "_upload_spooled", _QueueStatus()),
            mock.patch.object(client_module, "_upload_terminal", _QueueStatus()),
            mock.patch.object(client_module, "_upload_process", self._CleanProcess()),
            mock.patch.object(client_module, "_system_poller", poller),
            mock.patch.object(client_module, "_shutdown_deadline", _DeadlineValue()),
            mock.patch.object(client_module, "_spool_path", ""),
            mock.patch.object(client_module, "_spool_dir", spool_dir),
            mock.patch.object(client_module, "_server_address", ""),
            mock.patch.object(client_module, "_project_id", "p"),
            mock.patch.object(client_module, "_run_id", "r"),
            mock.patch.multiple(
                client_module, _session_id="session-a", _last_capture_step=None
            ),
            mock.patch.object(client_module, "SpoolWriter", self._Spool),
            mock.patch.object(client_module, "_drain_queue_to_spool", return_value=0),
            mock.patch.object(client_module.time, "time", return_value=1.234),
            capture_patch,
        ):
            return client_module._drain_and_shutdown(flush_timeout=flush_timeout)

    def test_shutdown_publishes_captured_tail_before_the_sentinel(self):
        status = _QueueStatus()
        events = []

        class AckingQueue(self._Queue):
            def put(queue_self, item):
                events.append(item)
                queue_self.items.append(item)
                if item is not None:
                    status.value -= client_module._queue_item_size(item)

        target = AckingQueue()
        poller = mock.Mock()
        poller.stop.side_effect = lambda **_kwargs: events.append("poller") or True
        complete = self._run_shutdown(
            target=target,
            status=status,
            poller=poller,
            capture=lambda: events.append("drain") or ("tail", "error"),
        )

        self.assertTrue(complete)
        self.assertEqual(
            events,
            [
                "poller",
                "drain",
                [
                    ("text_ts", "logs/std_out", 1234, "tail", 1234),
                    ("text_ts", "logs/std_err", 1234, "error", 1234),
                ],
                None,
            ],
        )
        self.assertEqual(status.value, 0)

    def test_shutdown_publishes_pending_logs_without_values_never_copied(self):
        for copied in (True, False):
            with self.subTest(copied=copied):
                status = _QueueStatus()

                class AckingQueue(self._Queue):
                    def put(queue_self, item):
                        queue_self.items.append(item)
                        if item is not None:
                            status.value -= client_module._queue_item_size(item)

                entry = client_module._PendingLog(
                    [("numeric_ts", "cpu", 1, 1.0, 1)],
                    [],
                    step=1,
                    timestamp_ms=1,
                    gpu_keys=[("gpu", None)],
                )
                entry.reads = mock.Mock()
                entry.reads.wait.return_value = copied
                entry.reads.scalars.return_value = [(0, 2.0)]
                target = AckingQueue()
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
                self.assertEqual(target.items, [points, None])

    def test_shutdown_fails_when_a_log_queues_behind_it(self):
        status = _QueueStatus()
        pending = deque()

        class LateLogQueue(self._Queue):
            def put(queue_self, item):
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
        _hold_pending_lock(self)
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

        class FailingTailQueue(self._Queue):
            def put(queue_self, item):
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
        self.assertEqual(status.value, 0)

    def test_final_capture_accounting_obeys_the_shutdown_deadline(self):
        status = _QueueStatus()
        status.lock.acquire()
        self.addCleanup(status.lock.release)
        target = self._Queue()

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

    def test_success_requires_zero_backlog_and_quiesced_poller(self):
        common = {"worker_clean": True, "had_spool": False}
        self.assertTrue(
            client_module._shutdown_succeeded(
                **common, poller_quiesced=True, remaining=0
            )
        )
        self.assertFalse(
            client_module._shutdown_succeeded(
                **common, poller_quiesced=True, remaining=1
            )
        )
        self.assertFalse(
            client_module._shutdown_succeeded(
                **common, poller_quiesced=False, remaining=0
            )
        )
        self.assertFalse(
            client_module._shutdown_succeeded(
                **common, poller_quiesced=True, remaining=0, spooled=True
            )
        )

    def test_shutdown_budget_keeps_equal_delivery_and_worker_spill_halves(self):
        for total, expected in (
            (3.0, (1.2, 1.2, 0.6)),
            (10.0, (4.0, 4.0, 2.0)),
            (60.0, (25.0, 25.0, 10.0)),
        ):
            with self.subTest(total=total):
                actual = client_module._shutdown_budget_split(total)
                for value, want in zip(actual, expected):
                    self.assertAlmostEqual(value, want)

    def test_finish_fails_on_unquiesced_poller(self):
        self.assertFalse(self._run_shutdown(backlog=0, poller_quiesced=False))

    def test_finish_fails_when_clean_worker_leaves_positive_status(self):
        self.assertFalse(self._run_shutdown(backlog=1, poller_quiesced=True))

    def test_terminal_shutdown_retires_every_worker_spool_segment(self):
        target = self._Queue()
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
                mock.patch.object(client_module, "_upload_spooled", _QueueStatus(0)),
                mock.patch.object(
                    client_module, "_upload_process", self._CleanProcess()
                ),
                mock.patch.object(client_module, "_system_poller", None),
                mock.patch.object(
                    client_module, "_shutdown_deadline", _DeadlineValue()
                ),
                mock.patch.object(client_module, "_spool_path", spool_path),
                mock.patch.object(client_module, "_server_address", "server"),
                mock.patch.object(client_module, "_project_id", "p"),
                mock.patch.object(client_module, "_run_id", "r"),
                mock.patch.object(client_module, "_salvage_queue_before") as salvage,
                mock.patch.object(client_module, "_send_terminate") as terminate,
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
                terminal_rejected=False, hard_deadline=time.monotonic() + deadline_in
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

        def salvage(deadline):
            captured.append(deadline)
            return "", True

        flush_timeout = 10.0
        # Taken from the forced-cleanup tail, capped at the reserve.
        cleanup_reserve = client_module._shutdown_budget_split(flush_timeout)[2]
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
            mock.patch.object(client_module, "_metric_queue", self._Queue()),
            # A nonzero backlog on a cleanly exited worker forces owner salvage,
            # the phase that used to be allowed to consume the whole budget.
            mock.patch.object(client_module, "_queue_status", _QueueStatus(3)),
            mock.patch.object(client_module, "_upload_process", self._CleanProcess()),
            mock.patch.object(client_module, "_system_poller", None),
            mock.patch.object(client_module, "_shutdown_deadline", _DeadlineValue()),
            mock.patch.object(client_module, "_spool_path", ""),
            mock.patch.object(client_module, "_spool_dir", "/nonexistent"),
            mock.patch.object(client_module, "_server_address", ""),
            mock.patch.object(client_module, "_project_id", "p"),
            mock.patch.object(client_module, "_run_id", "r"),
            mock.patch.object(client_module, "_session_id", "session-a"),
            mock.patch.object(client_module, "_send_terminate"),
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
                        terminal_rejected=True, hard_deadline=time.monotonic()
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
                terminal_rejected=True, hard_deadline=time.monotonic() + 0.05
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
                    terminal_rejected=True, hard_deadline=time.monotonic() + 0.2
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
                    terminal_rejected=True, hard_deadline=time.monotonic() + 5
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
                    terminal_rejected=False, hard_deadline=time.monotonic() + 5
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
                    terminal_rejected=True, hard_deadline=time.monotonic()
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
                    terminal_rejected=False, hard_deadline=time.monotonic() + 5
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
            self.assertIn("still open", "\n".join(logs.output))

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
                terminal_rejected=True, hard_deadline=time.monotonic() + 5
            )

        self.assertTrue(had_spool)
        self.assertIn("failed to resolve this run's spool", "\n".join(logs.output))

    def test_sigkill_fallback_makes_worker_dead_before_salvage_is_safe(self):
        class IgnoresTerminate:
            def __init__(self):
                self.alive = True
                self.exitcode = None
                self.events = []

            def is_alive(self):
                return self.alive

            def join(self, timeout):
                self.events.append(("join", timeout))

            def terminate(self):
                self.events.append(("terminate", None))

            def kill(self):
                self.events.append(("kill", None))
                self.alive = False
                self.exitcode = -9

        process = IgnoresTerminate()
        now = time.monotonic()
        dead, clean = client_module._stop_upload_process(
            process,
            graceful_deadline=now,
            stop_deadline=now + 1,
        )

        self.assertTrue(dead)
        self.assertFalse(clean)
        self.assertEqual(
            [kind for kind, _ in process.events],
            ["join", "terminate", "join", "kill", "join"],
        )

    def test_shutdown_status_read_bypasses_a_poisoned_process_lock(self):
        class PoisonedStatus:
            @property
            def value(self):
                raise AssertionError("the synchronized property must not be read")

            @staticmethod
            def get_lock():
                raise AssertionError("the poisoned SemLock must not be acquired")

            @staticmethod
            def get_obj():
                return types.SimpleNamespace(value=17)

        self.assertEqual(client_module._read_shutdown_status(PoisonedStatus()), 17)

    def test_unreadable_shutdown_status_cannot_prove_success(self):
        class UnreadableStatus:
            @staticmethod
            def get_obj():
                return types.SimpleNamespace(value=object())

        with self.assertLogs("kymo", level="ERROR"):
            remaining = client_module._read_shutdown_status(UnreadableStatus())
        self.assertIsNone(remaining)
        self.assertFalse(
            client_module._shutdown_succeeded(
                worker_clean=True,
                poller_quiesced=True,
                had_spool=False,
                remaining=remaining,
            )
        )

    def test_parent_salvage_returns_at_deadline_if_fsync_path_stalls(self):
        release = threading.Event()
        closed = threading.Event()

        class SlowSpool:
            path = "/tmp/salvage.mkspool"

            @staticmethod
            def close():
                closed.set()
                return SlowSpool.path

        old_queue = object()
        captured_sources = []

        def blocked_drain(_spool, *, source, hard_deadline):
            captured_sources.append(source)
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
            path, finished = client_module._salvage_queue_before(
                time.monotonic() + 0.02
            )
        elapsed = time.monotonic() - started
        release.set()
        self.assertTrue(closed.wait(timeout=1))

        self.assertEqual(path, "")
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
            def __call__(self, _groups):
                raise OSError("queue closed")

        poller = self._poller(FailingPublisher())
        with self.assertRaises(OSError):
            poller._poll_once()

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
        poller = SystemMetricsPoller()
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

            def __call__(self, groups):
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

            def __call__(self, value):
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
            def __call__(self, _groups):
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

    def test_stop_waits_for_backend_poll_before_nvml_shutdown(self):
        poll_started = threading.Event()
        release_poll = threading.Event()
        shutdown_calls = []
        poller = SystemMetricsPoller()
        poller._nvml_ok = True

        def poll_gpu(_metrics):
            poll_started.set()
            release_poll.wait(timeout=1)

        poller._poll_gpu = poll_gpu
        fake_nvml = types.SimpleNamespace(
            nvmlShutdown=lambda: shutdown_calls.append("shutdown")
        )
        with mock.patch.object(system_metrics_module, "pynvml", fake_nvml, create=True):
            polling = threading.Thread(target=poller._poll_once)
            poller._thread = polling
            polling.start()
            self.assertTrue(poll_started.wait(timeout=1))

            stopped = threading.Event()
            stopper = threading.Thread(target=lambda: (poller.stop(), stopped.set()))
            stopper.start()
            self.assertFalse(stopped.wait(timeout=0.05))
            self.assertEqual(shutdown_calls, [])
            release_poll.set()
            polling.join(timeout=1)
            stopper.join(timeout=1)

        self.assertTrue(stopped.is_set())
        self.assertEqual(shutdown_calls, ["shutdown"])

    def test_stop_is_bounded_when_backend_poll_is_wedged(self):
        poll_started = threading.Event()
        release_poll = threading.Event()
        shutdown_calls = []
        poller = SystemMetricsPoller()
        poller._nvml_ok = True

        def poll_gpu(_metrics):
            poll_started.set()
            release_poll.wait(timeout=1)

        poller._poll_gpu = poll_gpu
        fake_nvml = types.SimpleNamespace(
            nvmlShutdown=lambda: shutdown_calls.append("shutdown")
        )
        with (
            mock.patch.object(system_metrics_module, "pynvml", fake_nvml, create=True),
            mock.patch.object(system_metrics_module, "_STOP_BARRIER_TIMEOUT", 0.05),
        ):
            polling = threading.Thread(target=poller._poll_once)
            poller._thread = polling
            polling.start()
            self.assertTrue(poll_started.wait(timeout=1))
            started = time.monotonic()
            quiesced = poller.stop()
            elapsed = time.monotonic() - started
            release_poll.set()
            polling.join(timeout=1)

        self.assertLess(elapsed, 0.5)
        self.assertTrue(quiesced)
        self.assertEqual(shutdown_calls, [])

    def test_disk_rate_uses_its_own_sample_clock(self):
        counters = [
            types.SimpleNamespace(read_bytes=2_000_000, write_bytes=4_000_000),
            types.SimpleNamespace(read_bytes=4_000_000, write_bytes=8_000_000),
        ]
        poller = SystemMetricsPoller()
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
        poller = SystemMetricsPoller()
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
        deadline = time.monotonic() + 1
        while not first_call.cancelled and time.monotonic() < deadline:
            time.sleep(0.001)
        self.assertTrue(first_call.cancelled)

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
            attempt = client_module._connect("unused")
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
            attempt = client_module._connect("unused")
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
            attempt = client_module._connect("unused")
            result = attempt.poll(deadline_fn=lambda: True)

        self.assertEqual(result, (None, None))
        ready.done.assert_not_called()
        channel.close.assert_called_once_with()

    def test_connect_skips_attempt_after_shutdown_deadline(self):
        with mock.patch.object(client_module.grpc, "insecure_channel") as connect:
            attempt = client_module._connect("unused", deadline_fn=lambda: True)
            self.assertIsInstance(attempt, client_module._ConnectionAttempt)
            self.assertEqual(attempt.poll(), (None, None))
        connect.assert_not_called()


class _QueueStatus:
    def __init__(self, value=0):
        self.value = value
        self.lock = threading.Lock()

    def get_lock(self):
        return self.lock


class _DeadlineValue:
    value = 0.0


class WorkerResponsivenessTests(unittest.TestCase):
    class _Channel:
        def close(self):
            pass

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

    def test_quiet_local_worker_exits_without_connecting_or_ensuring(self):
        source = queue.Queue()
        source.put(None)
        status = _QueueStatus()

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    side_effect=AssertionError("empty local worker connected"),
                ) as connect,
                mock.patch(
                    "kymo._local_runtime.ensure_local_endpoint",
                    side_effect=AssertionError("empty local worker invoked ensure"),
                ) as ensure,
            ):
                client_module._upload_worker(
                    "unused",
                    "p",
                    "r",
                    source,
                    status,
                    shutdown_deadline=_DeadlineValue(),
                    spool_path=os.path.join(spool_dir, "quiet-local.mkspool"),
                    local_endpoint_config=local_endpoint_config(),
                )

        connect.assert_not_called()
        ensure.assert_not_called()

    def test_pending_local_item_opens_the_authenticated_connector(self):
        source = queue.Queue()
        source.put([numeric(1)])
        source.put(None)
        status = _QueueStatus(1)
        seen_endpoints = []

        def connect(_address, *, deadline_fn, local_endpoint):
            self.assertFalse(deadline_fn())
            seen_endpoints.append(local_endpoint)
            return _ImmediateConnectionAttempt(self._Channel(), object())

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                mock.patch.object(client_module, "_connect", side_effect=connect),
                mock.patch.object(
                    client_module,
                    "_BidiStream",
                    self._immediate_stream_class(lambda _batch: None),
                ),
            ):
                client_module._upload_worker(
                    "unused",
                    "p",
                    "r",
                    source,
                    status,
                    shutdown_deadline=_DeadlineValue(),
                    spool_path=os.path.join(spool_dir, "pending-local.mkspool"),
                    local_endpoint_config=local_endpoint_config(),
                )

        self.assertEqual(
            seen_endpoints, [endpoint_from_worker_config(local_endpoint_config())]
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

        class LocalStream:
            def __init__(self, _stub, _feed_cap):
                nonlocal stream_count
                stream_count += 1
                self.number = stream_count
                self.events = queue.Queue()
                self.half_closed = False

            def feed_has_room(self):
                return not self.half_closed

            def feed(self, batch):
                self.events.put(("ack", len(batch.points)))
                if self.number == 1:
                    first_fed.set()
                    self.events.put(("done", None))
                else:
                    second_fed.set()
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

        endpoint = endpoint_from_worker_config(local_endpoint_config())

        def ensure(**_kwargs):
            ensure_called.set()
            return endpoint

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(self._Channel(), object()),
                ),
                mock.patch.object(client_module, "_BidiStream", LocalStream),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.005),
                mock.patch(
                    "kymo._local_runtime.ensure_local_endpoint",
                    side_effect=ensure,
                ),
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "shutdown_deadline": _DeadlineValue(),
                        "spool_path": os.path.join(
                            spool_dir, "broken-quiet-local.mkspool"
                        ),
                        "local_endpoint_config": local_endpoint_config(),
                    },
                )
                worker.start()
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

    def test_rich_only_spool_handoff_does_not_wake_local_runtime(self):
        source = queue.Queue()
        source.put(
            [
                (
                    "metadata_batch",
                    "info/run_info",
                    0,
                    Metadata({"model": "test"}),
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
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(self._Channel(), object()),
                ),
                mock.patch.object(
                    client_module,
                    "_BidiStream",
                    self._immediate_stream_class(lambda _batch: None),
                ),
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
                    return_value=self._Channel(),
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
                    side_effect=AssertionError("rich handoff invoked ensure"),
                ) as ensure,
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "shutdown_deadline": _DeadlineValue(),
                        "spool_path": os.path.join(
                            spool_dir, "rich-only-local.mkspool"
                        ),
                        "local_endpoint_config": local_endpoint_config(),
                    },
                )
                worker.start()
                self.assertTrue(spooled.wait(timeout=1))
                time.sleep(0.05)
                ensure.assert_not_called()
                self.assertTrue(worker.is_alive())
                source.put(None)
                worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(status.value, 0)

    def test_replacement_local_installation_spools_once_without_retrying(self):
        source = queue.Queue()
        source.put([numeric(1)])
        source.put(None)
        status = _QueueStatus(1)
        spooled = _QueueStatus()

        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "old-installation.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
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
                client_module._upload_worker(
                    "unused",
                    "p",
                    "r",
                    source,
                    status,
                    shutdown_deadline=_DeadlineValue(),
                    spool_path=spool_path,
                    upload_spooled=spooled,
                    local_endpoint_config=local_endpoint_config(),
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
            (self._Channel(), object()),
        ]

        def connect(_address, *, deadline_fn):
            self.assertFalse(deadline_fn())
            attempts.append(True)
            return _ImmediateConnectionAttempt(*outcomes.pop(0))

        source = queue.Queue()
        source.put([numeric(1)])
        source.put(None)
        status = _QueueStatus(1)
        immediate = self._immediate_stream_class(lambda _batch: None)

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                mock.patch.object(client_module, "_connect", side_effect=connect),
                mock.patch.object(client_module, "_BidiStream", immediate),
                mock.patch.object(client_module, "_CONNECT_INITIAL_DELAY", 0.0),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.001),
                mock.patch.object(
                    client_module.grpc,
                    "insecure_channel",
                    return_value=self._Channel(),
                ) as open_channel,
                mock.patch.object(
                    client_module.kymo_pb2_grpc,
                    "KymoStub",
                    return_value=object(),
                ),
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "shutdown_deadline": _DeadlineValue(),
                        "spool_path": os.path.join(spool_dir, "connect.mkspool"),
                    },
                )
                worker.start()
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

        source = queue.Queue()
        source.put([numeric(1)])
        source.put(None)
        status = _QueueStatus(1)
        spooled = _QueueStatus()
        deadline = _DeadlineValue()

        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "connect-deadline.mkspool")
            with (
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.001),
                mock.patch.object(client_module, "_ACTIVE_POLL_TIMEOUT", 0.001),
                mock.patch.object(
                    client_module.grpc,
                    "insecure_channel",
                    return_value=self._Channel(),
                ),
                mock.patch.object(
                    client_module.grpc,
                    "channel_ready_future",
                    return_value=NeverReady(),
                ),
                mock.patch.object(
                    client_module.kymo_pb2_grpc,
                    "KymoStub",
                    return_value=object(),
                ),
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "shutdown_deadline": deadline,
                        "spool_path": spool_path,
                        "upload_spooled": spooled,
                    },
                )
                worker.start()
                self.assertTrue(ready_started.wait(timeout=1))
                deadline.value = time.monotonic() + 0.005
                worker.join(timeout=1)

            self.assertFalse(worker.is_alive())
            self.assertTrue(os.path.exists(spool_path))
            self.assertEqual(status.value, 0)
            self.assertEqual(spooled.value, 1)

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
                mock.patch.object(client_module, "_connect", return_value=attempt),
                mock.patch.object(client_module, "_CONNECT_POLL_TIMEOUT", 0.001),
                mock.patch.dict(os.environ, {"KYMO_MAX_BUFFER_POINTS": "1"}),
                mock.patch.object(
                    client_module.grpc,
                    "insecure_channel",
                    return_value=self._Channel(),
                ),
                mock.patch.object(
                    client_module.kymo_pb2_grpc,
                    "KymoStub",
                    return_value=object(),
                ),
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "shutdown_deadline": _DeadlineValue(),
                        "spool_path": spool_path,
                    },
                )
                worker.start()
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
            replay_had_writer_lock.append(sync_module._writer_active(path))
            _header, records = read_spool(path)
            steps = [record[2] for record in records]
            replayed_steps.append(steps)
            if len(replayed_steps) == 1:
                first_replay_started.set()
                self.assertTrue(release_first_replay.wait(timeout=1))
                return False
            os.rename(path, path + ".sent")
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

        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "cap-recovery.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(self._Channel(), object()),
                ),
                mock.patch.object(client_module, "_BidiStream", ObservedLiveStream),
                mock.patch.object(
                    client_module, "_recovery_retry_delay", return_value=0
                ),
                mock.patch.object(sync_module, "replay_file", side_effect=replay),
                mock.patch.dict(os.environ, {"KYMO_MAX_BUFFER_POINTS": "1"}),
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "shutdown_deadline": _DeadlineValue(),
                        "spool_path": spool_path,
                        "upload_spooled": spooled,
                    },
                )
                worker.start()
                self.assertTrue(first_replay_started.wait(timeout=1))
                source.put([numeric(3)])
                deadline = time.monotonic() + 1
                while status.value != 1 and time.monotonic() < deadline:
                    time.sleep(0.001)
                self.assertEqual(status.value, 1)
                release_first_replay.set()
                self.assertTrue(live_stream_opened.wait(timeout=1))
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
        self.assertEqual(live_steps, [4])
        self.assertEqual(spooled.value, 0)
        self.assertEqual(status.value, 0)

    def test_local_replay_refreshes_generation_after_auth_failure(self):
        replayed_endpoints = []
        replay_done = threading.Event()
        initial_endpoint = endpoint_from_worker_config(local_endpoint_config())
        refreshed_config = local_endpoint_config()
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
            os.rename(path, path + ".sent")
            replay_done.set()
            return True

        source = queue.Queue()
        source.put([numeric(1), numeric(2)])
        status = _QueueStatus(2)

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(self._Channel(), object()),
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
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "shutdown_deadline": _DeadlineValue(),
                        "spool_path": os.path.join(spool_dir, "rotated-bearer.mkspool"),
                        "local_endpoint_config": local_endpoint_config(),
                    },
                )
                worker.start()
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
                os.rename(path, path + ".sent")
                replayed_later.set()
                return True
            rejected = path + ".rejected"
            os.rename(path, rejected)
            kwargs["quarantined_files"].add(rejected)
            quarantined.set()
            return False

        source = queue.Queue()
        source.put([numeric(1), numeric(2)])
        status = _QueueStatus(2)

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
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(self._Channel(), object()),
                ),
                mock.patch.object(
                    client_module, "_recovery_retry_delay", return_value=0
                ),
                mock.patch.object(
                    sync_module, "replay_file", side_effect=quarantine_replay
                ),
                mock.patch.dict(os.environ, {"KYMO_MAX_BUFFER_POINTS": "1"}),
            ):
                client_module._upload_worker(
                    "unused",
                    "p",
                    "r",
                    source,
                    status,
                    shutdown_deadline=_DeadlineValue(),
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
            os.rename(path, path + ".sent")
            return True

        source = queue.Queue()
        source.put([numeric(1), numeric(2)])
        status = _QueueStatus(2)
        spooled = _QueueStatus()

        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "final-replay.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(self._Channel(), object()),
                ),
                mock.patch.object(
                    client_module,
                    "_BidiStream",
                    self._immediate_stream_class(lambda _batch: None),
                ),
                mock.patch.object(
                    client_module, "_recovery_retry_delay", return_value=0
                ),
                mock.patch.object(sync_module, "replay_file", side_effect=replay),
                mock.patch.dict(os.environ, {"KYMO_MAX_BUFFER_POINTS": "1"}),
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "shutdown_deadline": _DeadlineValue(),
                        "spool_path": spool_path,
                        "upload_spooled": spooled,
                    },
                )
                worker.start()
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

        source = queue.Queue()
        source.put([numeric(1), numeric(2)])
        status = _QueueStatus(2)
        spooled = _QueueStatus()
        failure = _QueueStatus()

        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "unsealable.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(self._Channel(), object()),
                ),
                mock.patch.object(
                    client_module,
                    "_BidiStream",
                    self._immediate_stream_class(
                        lambda batch: live_steps.extend(
                            point.step for point in batch.points
                        )
                    ),
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
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "shutdown_deadline": _DeadlineValue(),
                        "spool_path": spool_path,
                        "upload_spooled": spooled,
                        "upload_failure": failure,
                    },
                )
                worker.start()
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
        """The whole point of moving the barrier: no fsync may run on the loop
        that drains the producer queue — not the close() after replay, and not a
        retry of a barrier that failed, which is precisely when the mount is the
        thing that would block."""
        for label, barrier_fails in (("barrier ok", False), ("barrier fails", True)):
            with self.subTest(label):
                self._assert_replay_fsyncs_off_owner_thread(barrier_fails)

    def _assert_replay_fsyncs_off_owner_thread(self, barrier_fails):
        fsync_threads = []
        real_fsync = os.fsync
        replay_done = threading.Event()

        def spy(fd):
            fsync_threads.append(threading.current_thread().name)
            if barrier_fails:
                raise OSError("input/output error")
            return real_fsync(fd)

        def replay(path, **_kwargs):
            os.rename(path, path + ".sent")
            replay_done.set()
            return True

        source = queue.Queue()
        source.put([numeric(1), numeric(2)])
        status = _QueueStatus(2)

        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "barrier.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(self._Channel(), object()),
                ),
                mock.patch.object(
                    client_module,
                    "_BidiStream",
                    self._immediate_stream_class(lambda _batch: None),
                ),
                mock.patch.object(
                    client_module, "_recovery_retry_delay", return_value=0
                ),
                mock.patch.object(sync_module, "replay_file", side_effect=replay),
                mock.patch.object(os, "fsync", spy),
                mock.patch.dict(os.environ, {"KYMO_MAX_BUFFER_POINTS": "1"}),
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    name="kymo-test-owner",
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "shutdown_deadline": _DeadlineValue(),
                        "spool_path": spool_path,
                        "upload_spooled": _QueueStatus(),
                    },
                )
                worker.start()
                self.assertTrue(replay_done.wait(timeout=5))
                source.put(None)
                worker.join(timeout=5)

            self.assertFalse(worker.is_alive())

        # Every barrier (the records and, once they succeed, the directory entry) runs off the loop.
        self.assertEqual(set(fsync_threads), {"kymo-spool-replay"})

    def test_replay_survives_a_failed_durability_barrier(self):
        """fsync moved off the queue-consumer thread; delivering the records is a
        stronger guarantee than persisting them, so a failed sync only warns."""
        replayed = []
        replay_done = threading.Event()

        def replay(path, **_kwargs):
            replayed.append(path)
            os.rename(path, path + ".sent")
            replay_done.set()
            return True

        source = queue.Queue()
        source.put([numeric(1), numeric(2)])
        status = _QueueStatus(2)
        spooled = _QueueStatus()

        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "unsyncable.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(self._Channel(), object()),
                ),
                mock.patch.object(
                    client_module,
                    "_BidiStream",
                    self._immediate_stream_class(lambda _batch: None),
                ),
                mock.patch.object(
                    client_module, "_recovery_retry_delay", return_value=0
                ),
                mock.patch.object(
                    client_module.SpoolWriter,
                    "sync",
                    side_effect=OSError("input/output error"),
                ),
                mock.patch.object(sync_module, "replay_file", side_effect=replay),
                mock.patch.dict(os.environ, {"KYMO_MAX_BUFFER_POINTS": "1"}),
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "shutdown_deadline": _DeadlineValue(),
                        "spool_path": spool_path,
                        "upload_spooled": spooled,
                    },
                )
                worker.start()
                # Shutdown suppresses new connects, so the replay has to be under
                # way before the sentinel — as it is in the live outage this fix
                # is about.
                self.assertTrue(replay_done.wait(timeout=5))
                source.put(None)
                worker.join(timeout=5)

            self.assertFalse(worker.is_alive())
            self.assertEqual(replayed, [spool_path])
        self.assertEqual(spooled.value, 0)
        self.assertEqual(status.value, 0)

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

        source = DeadlineQueue()
        status = _QueueStatus(2)
        deadline = _DeadlineValue()
        deadline.value = time.monotonic() - 1

        def record_spill(_spool, point):
            spilled_steps.append(point[2])
            return 1

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                mock.patch.object(
                    client_module, "_spill_tuple", side_effect=record_spill
                ),
                mock.patch.object(
                    client_module.grpc,
                    "insecure_channel",
                    return_value=self._Channel(),
                ),
                mock.patch.object(
                    client_module.kymo_pb2_grpc,
                    "KymoStub",
                    return_value=object(),
                ),
                # A backward wall-clock correction must not extend shutdown.
                mock.patch.object(worker_module.time, "time", return_value=0.0),
            ):
                client_module._upload_worker(
                    "unused",
                    "p",
                    "r",
                    source,
                    status,
                    shutdown_deadline=deadline,
                    spool_path=os.path.join(spool_dir, "direct-spill.mkspool"),
                )

        self.assertEqual(spilled_steps, [1, 2])
        self.assertEqual(status.value, 0)

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
        deadline = _DeadlineValue()

        with tempfile.TemporaryDirectory() as spool_dir:
            with contextlib.ExitStack() as stack:
                stack.enter_context(
                    mock.patch.object(
                        client_module,
                        "_connect",
                        return_value=_ImmediateConnectionAttempt(
                            self._Channel(), object()
                        ),
                    )
                )
                stack.enter_context(
                    mock.patch.object(
                        client_module.grpc,
                        "insecure_channel",
                        return_value=self._Channel(),
                    )
                )
                stack.enter_context(
                    mock.patch.object(
                        client_module.kymo_pb2_grpc,
                        "KymoStub",
                        return_value=object(),
                    )
                )
                stack.enter_context(
                    mock.patch.object(
                        client_module,
                        "_BidiStream",
                        self._immediate_stream_class(on_feed),
                    )
                )
                stack.enter_context(
                    mock.patch("httpx.Client", return_value=self._HttpClient())
                )
                stack.enter_context(
                    mock.patch.object(
                        client_module, "_process_cdn_batch", side_effect=process_cdn
                    )
                )
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "cdn_url": "http://unused",
                        "shutdown_deadline": deadline,
                        "spool_path": os.path.join(spool_dir, "cdn.mkspool"),
                    },
                )
                worker.start()
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
            deadline = _DeadlineValue()
            with (
                mock.patch.object(
                    client_module.grpc,
                    "insecure_channel",
                    return_value=self._Channel(),
                ),
                mock.patch.object(
                    client_module.kymo_pb2_grpc,
                    "KymoStub",
                    return_value=object(),
                ),
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
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "cdn_url": "http://unused",
                        "shutdown_deadline": deadline,
                        "spool_path": spool_path,
                    },
                )
                worker.start()
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
        fed = []
        undecodable = (
            client_module._SERIALIZED_RICH_QUEUE_ITEM,
            1,
            b"not a pickle",
        )
        source = queue.Queue()
        for item in ([numeric(1)], undecodable, [numeric(2)], None):
            source.put(item)
        status = _QueueStatus(3)
        failure = _QueueStatus()
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(self._Channel(), object()),
                ),
                mock.patch.object(
                    client_module,
                    "_BidiStream",
                    self._immediate_stream_class(
                        lambda batch: fed.extend(point.step for point in batch.points)
                    ),
                ),
                self.assertLogs("kymo", level="ERROR") as logs,
                self.assertRaises(SystemExit) as exited,
            ):
                client_module._upload_worker(
                    "unused",
                    "p",
                    "r",
                    source,
                    status,
                    shutdown_deadline=_DeadlineValue(),
                    spool_path=os.path.join(spool_dir, "undecodable.mkspool"),
                    upload_failure=failure,
                )
            self.assertEqual(os.listdir(spool_dir), [])

        self.assertEqual(fed, [1, 2])
        self.assertEqual(status.value, 0)
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

        source = queue.Queue()
        source.put([numeric(1)])
        source.put(None)
        status = _QueueStatus(1)
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(self._Channel(), object()),
                ),
                mock.patch.object(
                    client_module, "_BidiStream", side_effect=open_stream
                ),
                mock.patch.object(wire_module, "_SEND_INITIAL_DELAY", 0.0),
                mock.patch.object(
                    client_module.grpc,
                    "insecure_channel",
                    return_value=self._Channel(),
                ),
                mock.patch.object(
                    client_module.kymo_pb2_grpc,
                    "KymoStub",
                    return_value=object(),
                ),
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "shutdown_deadline": _DeadlineValue(),
                        "spool_path": os.path.join(spool_dir, "stream-open.mkspool"),
                    },
                )
                worker.start()
                worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(stream_attempts, 2)
        self.assertEqual([point.step for point in fed], [1])
        self.assertEqual(status.value, 0)

    def test_partial_ack_then_break_refeeds_only_unacked_suffix(self):
        stream_attempts = 0
        fed_steps = []

        class ScriptedStream:
            def __init__(self, _stub, _feed_cap):
                nonlocal stream_attempts
                stream_attempts += 1
                self.attempt = stream_attempts
                self.events = queue.Queue()
                self.closed = False

            def feed_has_room(self):
                return not self.closed

            def feed(self, batch):
                steps = [point.step for point in batch.points]
                fed_steps.append(steps)
                if self.attempt == 1:
                    self.events.put(("ack", 1))
                    self.events.put(("error", RuntimeError("injected break")))
                else:
                    self.events.put(("ack", len(steps)))
                return True

            def poll_acks(self):
                events = []
                while True:
                    try:
                        events.append(self.events.get_nowait())
                    except queue.Empty:
                        return events

            def half_close(self):
                self.closed = True
                self.events.put(("done", None))
                return True

            def cancel(self):
                self.closed = True

        source = queue.Queue()
        source.put([numeric(1), numeric(2), numeric(3)])
        source.put(None)
        status = _QueueStatus(3)
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(self._Channel(), object()),
                ),
                mock.patch.object(client_module, "_BidiStream", ScriptedStream),
                mock.patch.object(wire_module, "_SEND_INITIAL_DELAY", 0.0),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.01),
                mock.patch.object(
                    client_module.grpc,
                    "insecure_channel",
                    return_value=self._Channel(),
                ),
                mock.patch.object(
                    client_module.kymo_pb2_grpc,
                    "KymoStub",
                    return_value=object(),
                ),
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "shutdown_deadline": _DeadlineValue(),
                        "spool_path": os.path.join(spool_dir, "partial-ack.mkspool"),
                    },
                )
                worker.start()
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

        source = queue.Queue()
        source.put([numeric(1)])
        source.put(None)
        status = _QueueStatus(1)
        deadline = _DeadlineValue()
        deadline.value = time.monotonic() + 0.2
        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "watchdog.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(self._Channel(), object()),
                ),
                mock.patch.object(client_module, "_BidiStream", SilentStream),
                mock.patch.object(wire_module, "_SEND_INITIAL_DELAY", 0.01),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.005),
                mock.patch.object(client_module, "_ACTIVE_POLL_TIMEOUT", 0.005),
                mock.patch.dict(os.environ, {"KYMO_ACK_PROGRESS_TIMEOUT": "0.03"}),
                mock.patch.object(
                    client_module.grpc,
                    "insecure_channel",
                    return_value=self._Channel(),
                ),
                mock.patch.object(
                    client_module.kymo_pb2_grpc,
                    "KymoStub",
                    return_value=object(),
                ),
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "shutdown_deadline": deadline,
                        "spool_path": spool_path,
                    },
                )
                worker.start()
                worker.join(timeout=2)

            self.assertTrue(os.path.exists(spool_path))

        self.assertFalse(worker.is_alive())
        self.assertGreaterEqual(stream_attempts, 2)
        self.assertTrue(cancel_times)
        self.assertEqual(status.value, 0)

    def test_shutdown_eof_flushes_a_partial_unacked_tail(self):
        class EofFlushStream:
            def __init__(self, _stub, _feed_cap):
                self.events = queue.Queue()
                self.fed = 0
                self.closed = False

            def feed_has_room(self):
                return not self.closed

            def feed(self, batch):
                self.fed += len(batch.points)
                return True

            def poll_acks(self):
                events = []
                while True:
                    try:
                        events.append(self.events.get_nowait())
                    except queue.Empty:
                        return events

            def half_close(self):
                self.closed = True
                self.events.put(("ack", self.fed))
                self.events.put(("done", None))
                return True

            def cancel(self):
                self.closed = True

        source = queue.Queue()
        source.put([numeric(1), numeric(2), numeric(3)])
        source.put(None)
        status = _QueueStatus(3)
        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "eof-tail.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(self._Channel(), object()),
                ),
                mock.patch.object(client_module, "_BidiStream", EofFlushStream),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.01),
                mock.patch.object(
                    client_module.grpc,
                    "insecure_channel",
                    return_value=self._Channel(),
                ),
                mock.patch.object(
                    client_module.kymo_pb2_grpc,
                    "KymoStub",
                    return_value=object(),
                ),
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "shutdown_deadline": _DeadlineValue(),
                        "spool_path": spool_path,
                    },
                )
                worker.start()
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

        source = queue.Queue()
        source.put([numeric(i) for i in range(4)])
        source.put(None)
        status = _QueueStatus(4)
        deadline = _DeadlineValue()
        deadline.value = time.monotonic() + 10
        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "full-window.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(
                        self._Channel(), FinalFlushStub()
                    ),
                ),
                mock.patch.object(client_module, "_BidiStream", RecordingStream),
                mock.patch.object(client_module, "_MAX_UNACKED_POINTS", 4),
                mock.patch.object(wire_module, "_MAX_POINTS_PER_MSG", 2),
                mock.patch.object(client_module, "_FEED_Q_CAP", 2),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.005),
                mock.patch.object(client_module, "_ACTIVE_POLL_TIMEOUT", 0.005),
                mock.patch.object(
                    client_module.grpc,
                    "insecure_channel",
                    return_value=self._Channel(),
                ),
                mock.patch.object(
                    client_module.kymo_pb2_grpc,
                    "KymoStub",
                    return_value=object(),
                ),
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "shutdown_deadline": deadline,
                        "spool_path": spool_path,
                    },
                )
                worker.start()
                wait_until = time.monotonic() + 5
                while (
                    not stream_instances
                    or not stream_instances[0]._feed_q.full()
                    or stream_instances[0].half_close_attempts == 0
                ) and time.monotonic() < wait_until:
                    time.sleep(0.001)
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
                    mock.patch.object(
                        client_module,
                        "_connect",
                        return_value=_ImmediateConnectionAttempt(
                            self._Channel(), object()
                        ),
                    )
                )
                stack.enter_context(
                    mock.patch.object(
                        client_module.grpc,
                        "insecure_channel",
                        return_value=self._Channel(),
                    )
                )
                stack.enter_context(
                    mock.patch.object(
                        client_module.kymo_pb2_grpc,
                        "KymoStub",
                        return_value=object(),
                    )
                )
                stack.enter_context(
                    mock.patch.object(
                        client_module,
                        "_BidiStream",
                        self._immediate_stream_class(record_bidi_leak),
                    )
                )
                stack.enter_context(
                    mock.patch("httpx.Client", return_value=self._HttpClient())
                )
                stack.enter_context(
                    mock.patch.object(
                        client_module, "_process_cdn_batch", side_effect=process_rich
                    )
                )
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "p", "r", source, status),
                    kwargs={
                        "cdn_url": "http://unused",
                        "shutdown_deadline": _DeadlineValue(),
                        "spool_path": os.path.join(spool_dir, "cdn-order.mkspool"),
                    },
                )
                worker.start()
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
        status = _QueueStatus(3)
        deadline = _DeadlineValue()
        deadline.value = time.monotonic() - 1

        with tempfile.TemporaryDirectory() as spool_dir:
            with mock.patch.object(
                client_module, "_spill_tuple", side_effect=OSError("disk full")
            ) as spill:
                with self.assertRaises(SystemExit) as exited:
                    client_module._upload_worker(
                        "unused",
                        "p",
                        "r",
                        source,
                        status,
                        shutdown_deadline=deadline,
                        spool_path=os.path.join(spool_dir, "broken.mkspool"),
                    )

        self.assertEqual(exited.exception.code, client_module._WORKER_EXIT_SPOOL_FAILED)
        self.assertEqual(spill.call_count, 1)
        self.assertEqual(status.value, 0)

    def test_missing_parent_sentinel_forces_fenced_parent_salvage(self):
        source = queue.Queue()
        source.put([numeric(1)])
        status = _QueueStatus(1)
        deadline = _DeadlineValue()
        deadline.value = time.monotonic() - 1

        with tempfile.TemporaryDirectory() as spool_dir:
            spool_path = os.path.join(spool_dir, "incomplete-drain.mkspool")
            with mock.patch.object(client_module, "_QUEUE_DRAIN_SILENCE", 0.01):
                with self.assertRaises(SystemExit) as exited:
                    client_module._upload_worker(
                        "unused",
                        "p",
                        "r",
                        source,
                        status,
                        shutdown_deadline=deadline,
                        spool_path=spool_path,
                    )

            self.assertTrue(os.path.exists(spool_path))

        self.assertEqual(
            exited.exception.code, client_module._WORKER_EXIT_DRAIN_INCOMPLETE
        )
        self.assertEqual(status.value, 0)

    def test_wait_does_not_report_success_after_spool_failure_before_worker_exit(self):
        source = queue.Queue()
        source.put([numeric(1), numeric(2), numeric(3)])
        status = _QueueStatus(3)
        failure = _QueueStatus()
        deadline = _DeadlineValue()
        spill_failed = threading.Event()
        worker_exit = []

        def fail_spill(_spool, _point):
            spill_failed.set()
            raise OSError("disk full")

        def run_worker():
            try:
                client_module._upload_worker(
                    "unused",
                    "p",
                    "r",
                    source,
                    status,
                    shutdown_deadline=deadline,
                    spool_path=os.path.join(spool_dir, "wait-race.mkspool"),
                    upload_failure=failure,
                )
            except SystemExit as error:
                worker_exit.append(error.code)

        class AliveWorker:
            exitcode = None

            @staticmethod
            def is_alive():
                return True

        with tempfile.TemporaryDirectory() as spool_dir:
            with contextlib.ExitStack() as stack:
                stack.enter_context(
                    mock.patch.object(
                        client_module,
                        "_connect",
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
                worker = threading.Thread(target=run_worker)
                worker.start()
                self.assertTrue(spill_failed.wait(timeout=1))

                deadline_at = time.monotonic() + 1
                while failure.value == 0 and time.monotonic() < deadline_at:
                    time.sleep(0.001)
                self.assertEqual(failure.value, client_module._WORKER_EXIT_SPOOL_FAILED)

                with (
                    mock.patch.object(client_module, "_is_initialized", True),
                    mock.patch.object(client_module, "_queue_status", status),
                    mock.patch.object(client_module, "_upload_failure", failure),
                    mock.patch.object(client_module, "_upload_process", AliveWorker()),
                ):
                    with self.assertRaisesRegex(RuntimeError, "could not spool"):
                        client_module.wait_for_upload(timeout=0)

                deadline.value = time.monotonic() - 1
                worker.join(timeout=2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(worker_exit, [client_module._WORKER_EXIT_SPOOL_FAILED])
        with status.get_lock():
            self.assertEqual(
                (status.value, failure.value),
                (0, client_module._WORKER_EXIT_SPOOL_FAILED),
            )


if __name__ == "__main__":
    unittest.main()
