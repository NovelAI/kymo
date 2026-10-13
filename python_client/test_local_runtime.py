"""Local launcher contract and private-transport tests."""

import base64
import json
import os
import pickle
import shlex
import signal
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
import uuid
from unittest import mock

from kymo import _control_rpc as control_rpc
from kymo import _local_runtime as local_runtime
from kymo import client as client_module
from kymo import sync as sync_module
from kymo.spool import SpoolWriter


INSTALLATION_UUID = "11111111-1111-4111-8111-111111111111"
GENERATION_UUID = "22222222-2222-4222-8222-222222222222"
INIT_HOLD_UUID = "33333333-3333-4333-8333-333333333333"
SERVER_BEARER = "A" * 43


def endpoint_payload(**overrides) -> dict:
    payload = {
        "protocol_min": 2,
        "protocol_max": 2,
        "installation_uuid": INSTALLATION_UUID,
        "endpoint_generation": GENERATION_UUID,
        "native_socket": "/private/tmp/mkdb2/native.sock",
        "upload_socket": "/private/tmp/mkdb2/upload.sock",
        "dashboard_origin": "http://127.0.0.1:49152",
        "cdn_origin": "http://127.0.0.1:49153",
        "server_bearer": SERVER_BEARER,
    }
    payload.update(overrides)
    return payload


class LocalEndpointTests(unittest.TestCase):
    def test_launcher_beside_the_interpreter_needs_no_path(self):
        with tempfile.TemporaryDirectory() as scripts:
            launcher = os.path.join(scripts, "kymo")
            with open(launcher, "w") as file:
                file.write("#!/bin/sh\n")
            os.chmod(launcher, 0o755)
            with (
                mock.patch.object(
                    local_runtime.sysconfig, "get_path", return_value=scripts
                ),
                mock.patch.dict(os.environ, {"PATH": ""}),
            ):
                self.assertEqual(local_runtime._launcher_path(), launcher)

    def test_missing_launcher_names_the_local_extra(self):
        with (
            tempfile.TemporaryDirectory() as scripts,
            mock.patch.object(
                local_runtime.sysconfig, "get_path", return_value=scripts
            ),
            mock.patch.dict(os.environ, {"PATH": ""}),
            self.assertRaisesRegex(RuntimeError, r"kymo\[local\]"),
        ):
            local_runtime._launcher_path()

    def test_control_rpc_handoff_keeps_local_bearer_out_of_argv(self):
        endpoint = local_runtime.endpoint_from_worker_config(endpoint_payload())
        expected = client_module.kymo_pb2.InitRunResponse(
            run=client_module.kymo_pb2.RunInfo(
                project_id="project",
                run_id="run",
                run_name="name",
                ordinal=7,
            )
        )
        stdin = mock.Mock()
        process = mock.Mock(returncode=0, stdin=stdin)
        process.communicate.return_value = (expected.SerializeToString(), b"")
        request = client_module.kymo_pb2.InitRunRequest(
            project_id="project", run_id="run", run_name="name"
        )
        with mock.patch.object(
            client_module.subprocess, "Popen", return_value=process
        ) as popen:
            actual = client_module._run_control_rpc(
                "InitRun",
                request,
                client_module.kymo_pb2.InitRunResponse,
                server_address=endpoint.grpc_target,
                timeout=30,
                local_endpoint=endpoint,
            )

        command = popen.call_args.args[0]
        # Written whole to its stdin before the wait.
        payload = json.loads(stdin.write.call_args.args[0])
        stdin.close.assert_called_once()
        process.communicate.assert_called_once_with(timeout=30)
        self.assertNotIn(SERVER_BEARER, repr(command))
        self.assertEqual(payload["local_endpoint"]["server_bearer"], SERVER_BEARER)
        self.assertFalse(popen.call_args.kwargs["close_fds"])
        self.assertEqual(actual, expected)

    def test_control_child_dispatches_only_the_allowlisted_rpc(self):
        request = client_module.kymo_pb2.TerminateRunRequest(
            project_id="project", run_id="run", exit_code=143
        )
        payload = {
            "method": "TerminateRun",
            "request": base64.b64encode(request.SerializeToString()).decode("ascii"),
            "server_address": "127.0.0.1:50051",
            "timeout": 5,
            "local_endpoint": None,
        }
        channel = mock.MagicMock()
        response = client_module.kymo_pb2.TerminateRunResponse()
        stub = mock.Mock()
        stub.TerminateRun.return_value = response
        with (
            mock.patch.object(
                control_rpc.grpc, "insecure_channel", return_value=channel
            ),
            mock.patch.object(control_rpc.kymo_pb2_grpc, "KymoStub", return_value=stub),
        ):
            encoded = control_rpc._dispatch(payload)

        self.assertEqual(
            client_module.kymo_pb2.TerminateRunResponse.FromString(encoded), response
        )
        stub.TerminateRun.assert_called_once_with(request, timeout=5.0)
        with self.assertRaisesRegex(ValueError, "unsupported"):
            control_rpc._dispatch({**payload, "method": "DeleteProject"})
        # With after, the control process an earlier end of the run left running, it sends once that one has ended, within the same deadline.
        earlier = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(0.3)"]
        )
        threading.Thread(target=earlier.wait, daemon=True).start()
        stub.TerminateRun.reset_mock()
        sent_after = []

        def gone(pid):
            # Not earlier.poll(), which reads None while the waiting thread, having reaped the process, has yet to record its status.
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return True
            return False

        stub.TerminateRun.side_effect = lambda request, timeout: (
            sent_after.append((gone(earlier.pid), timeout)) or response
        )
        with (
            mock.patch.object(
                control_rpc.grpc, "insecure_channel", return_value=channel
            ),
            mock.patch.object(control_rpc.kymo_pb2_grpc, "KymoStub", return_value=stub),
        ):
            control_rpc._dispatch({**payload, "after": earlier.pid})
        ((ended, timeout),) = sent_after
        self.assertTrue(ended)
        self.assertLess(timeout, 4.8)
        with self.assertRaisesRegex(ValueError, "process id"):
            control_rpc._dispatch({**payload, "after": True})

    @unittest.skipUnless(
        os.path.exists("/proc/self/stat"), "a zombie reads as running without /proc"
    )
    def test_a_control_process_sends_after_an_earlier_one_that_is_a_zombie(self):
        # The earlier control process that the next waits for is a zombie once it has ended until its parent reaps it, for good where that parent has exited under a PID 1 that never reaps: os.kill(pid, 0) still finds it, which would hold the next until its alarm, unsent. A zombie's state in /proc reads as ended.
        payload = {
            "method": "TerminateRun",
            "request": base64.b64encode(
                client_module.kymo_pb2.TerminateRunRequest(
                    project_id="project", run_id="run", exit_code=130
                ).SerializeToString()
            ).decode("ascii"),
            "server_address": "127.0.0.1:50051",
            "timeout": 5,
            "local_endpoint": None,
        }
        sent = threading.Event()
        stub = mock.Mock()
        stub.TerminateRun.side_effect = lambda request, timeout: (
            sent.set() or client_module.kymo_pb2.TerminateRunResponse()
        )
        earlier = subprocess.Popen([sys.executable, "-c", "pass"])
        waiting = threading.Thread(
            target=control_rpc._dispatch,
            args=({**payload, "after": earlier.pid},),
            daemon=True,
        )
        with (
            mock.patch.object(
                control_rpc.grpc, "insecure_channel", return_value=mock.MagicMock()
            ),
            mock.patch.object(control_rpc.kymo_pb2_grpc, "KymoStub", return_value=stub),
        ):
            try:
                # Exited, and left unreaped.
                os.waitid(os.P_PID, earlier.pid, os.WEXITED | os.WNOWAIT)
                waiting.start()
                self.assertTrue(sent.wait(5))
            finally:
                # Reaped, a wait that missed the zombie ends, sending through the stand-ins.
                earlier.wait()
                if waiting.is_alive():
                    waiting.join(5)

    def test_a_control_process_whose_start_hangs_dies_by_its_alarm(self):
        # A run's TerminateRun goes out with the group's signals blocked, so no Ctrl-C or SIGTERM ends its control process: its alarm does, 2 s past its timeout, set before site's processing (a sitecustomize here) and its imports of kymo and grpc (a grpc that hangs here), at SIGALRM's default action and unblocked though the program ignores or blocks it.
        hangs = "import time\ntime.sleep(60)\n"
        left = []
        with (
            tempfile.TemporaryDirectory() as site,
            tempfile.TemporaryDirectory() as imports,
        ):
            with open(os.path.join(site, "sitecustomize.py"), "w") as file:
                file.write(hangs)
            with open(os.path.join(imports, "grpc.py"), "w") as file:
                file.write(hangs)
            mask = signal.pthread_sigmask(signal.SIG_BLOCK, [])
            alarm = signal.getsignal(signal.SIGALRM)
            started = time.monotonic()
            try:
                for path, program in ((site, "ignores"), (imports, "blocks")):
                    if program == "ignores":
                        signal.signal(signal.SIGALRM, signal.SIG_IGN)
                    else:
                        signal.pthread_sigmask(signal.SIG_BLOCK, [signal.SIGALRM])
                    with mock.patch.dict(os.environ, {"PYTHONPATH": path}):
                        left.append(
                            client_module._run_control_rpc(
                                "TerminateRun",
                                client_module.kymo_pb2.TerminateRunRequest(),
                                client_module.kymo_pb2.TerminateRunResponse,
                                server_address="127.0.0.1:1",
                                timeout=0.5,
                                leave=lambda: False,
                            )
                        )
                    signal.signal(signal.SIGALRM, alarm)
                    signal.pthread_sigmask(signal.SIG_SETMASK, mask)
                for process in left:
                    process.wait(timeout=30)
            finally:
                signal.signal(signal.SIGALRM, alarm)
                signal.pthread_sigmask(signal.SIG_SETMASK, mask)
                for process in left:
                    if process.poll() is None:
                        process.kill()
                        process.wait()
        self.assertEqual(
            [process.returncode for process in left], [-signal.SIGALRM] * 2
        )
        self.assertLess(time.monotonic() - started, 10)

    def test_ensure_validates_and_normalizes_the_launcher_contract(self):
        completed = subprocess.CompletedProcess(
            ["/runtime/bin/kymo", "ensure", "--json"],
            0,
            stdout=json.dumps(endpoint_payload()),
        )
        with (
            mock.patch.object(
                local_runtime.shutil, "which", return_value="/runtime/bin/kymo"
            ),
            mock.patch.object(
                local_runtime.subprocess, "run", return_value=completed
            ) as run,
        ):
            endpoint = local_runtime.ensure_local_endpoint(timeout=17)

        self.assertEqual(endpoint.installation_uuid, INSTALLATION_UUID)
        self.assertEqual(endpoint.grpc_target, "unix:///private/tmp/mkdb2/native.sock")
        self.assertEqual(endpoint.upload_origin, "http://localhost")
        run.assert_called_once_with(
            ["/runtime/bin/kymo", "ensure", "--json"],
            stdout=subprocess.PIPE,
            text=True,
            timeout=17,
            check=False,
            close_fds=False,
        )

    def test_an_ensure_in_its_own_session_keeps_the_default_mask(self):
        # The ensure before a run's TerminateRun runs as a session leader, which a group Ctrl-C or SIGTERM never reaches, with no signal blocked though its caller blocks some, since a local stack it starts keeps its mask; past its timeout it is killed and reaped, as subprocess.run would. Where posix_spawn has no setsid (CPython built against glibc older than 2.26), it leads a process group of its own instead.
        real_spawn = os.posix_spawn

        def without_setsid(*args, setsid=False, **kwargs):
            if setsid:
                raise NotImplementedError("posix_spawn: setsid unavailable")
            return real_spawn(*args, **kwargs)

        with tempfile.TemporaryDirectory() as directory:
            launcher = os.path.join(directory, "kymo")
            facts = os.path.join(directory, "facts.json")
            with open(launcher, "w") as file:
                file.write(
                    f"#!{sys.executable}\n"
                    "import json, os, signal, time\n"
                    "blocked = signal.pthread_sigmask(signal.SIG_BLOCK, [])\n"
                    "facts = {'leader': os.getsid(0) == os.getpid(), 'group': os.getpgid(0) == os.getpid(), 'pid': os.getpid(), 'blocked': sorted(map(int, blocked))}\n"
                    f"with open({facts!r}, 'w') as handle: handle.write(json.dumps(facts))\n"
                    "time.sleep(float(os.environ['KYMO_TEST_LAUNCHER_SLEEP']))\n"
                    f"print({json.dumps(endpoint_payload())!r})\n"
                )
            os.chmod(launcher, 0o700)
            before = signal.pthread_sigmask(
                signal.SIG_BLOCK, [signal.SIGINT, signal.SIGTERM]
            )
            try:
                with mock.patch.object(
                    local_runtime, "_launcher_path", return_value=launcher
                ):
                    for spawn, leader in ((real_spawn, True), (without_setsid, False)):
                        with (
                            mock.patch.dict(
                                os.environ, {"KYMO_TEST_LAUNCHER_SLEEP": "0"}
                            ),
                            mock.patch.object(local_runtime.os, "posix_spawn", spawn),
                        ):
                            endpoint = local_runtime.ensure_local_endpoint(
                                timeout=30, own_session=True
                            )
                        with open(facts) as file:
                            ran = json.load(file)
                        self.assertEqual(endpoint.installation_uuid, INSTALLATION_UUID)
                        self.assertEqual(
                            (ran["leader"], ran["group"], ran["blocked"]),
                            (leader, True, []),
                        )
                    with (
                        mock.patch.dict(os.environ, {"KYMO_TEST_LAUNCHER_SLEEP": "30"}),
                        self.assertRaisesRegex(RuntimeError, "did not become ready"),
                    ):
                        started = time.monotonic()
                        local_runtime.ensure_local_endpoint(timeout=1, own_session=True)
                self.assertLess(time.monotonic() - started, 5)
                with open(facts) as file:
                    pid = json.load(file)["pid"]
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, before)

    def test_an_ensure_in_its_own_session_starts_with_sigpipe_at_its_default(self):
        # Python ignores SIGPIPE and SIGXFSZ at its start, and exec keeps an ignored disposition: the ensure's spawn sets them to their default actions, as subprocess does, so a shell launcher's pipeline ends as it would anywhere else (its writer killed by SIGPIPE, 141).
        with tempfile.TemporaryDirectory() as directory:
            launcher = os.path.join(directory, "kymo")
            status = os.path.join(directory, "status")
            with open(launcher, "w") as file:
                file.write(
                    "#!/bin/sh\n"
                    f"(yes; echo $? > {shlex.quote(status)}) 2>/dev/null | head -n 1 > /dev/null\n"
                    f"printf '%s\\n' {shlex.quote(json.dumps(endpoint_payload()))}\n"
                )
            os.chmod(launcher, 0o700)
            with mock.patch.object(
                local_runtime, "_launcher_path", return_value=launcher
            ):
                endpoint = local_runtime.ensure_local_endpoint(
                    timeout=30, own_session=True
                )
            with open(status) as file:
                self.assertEqual(file.read().strip(), str(128 + signal.SIGPIPE))
        self.assertEqual(endpoint.installation_uuid, INSTALLATION_UUID)

    def test_ensure_passes_a_canonical_nonsecret_init_hold(self):
        completed = subprocess.CompletedProcess(
            ["/runtime/bin/kymo", "ensure", "--json"],
            0,
            stdout=json.dumps(endpoint_payload()),
        )
        with (
            mock.patch.object(
                local_runtime.shutil, "which", return_value="/runtime/bin/kymo"
            ),
            mock.patch.object(
                local_runtime.subprocess, "run", return_value=completed
            ) as run,
        ):
            local_runtime.ensure_local_endpoint(
                timeout=17,
                init_hold_id=INIT_HOLD_UUID.replace("-", ""),
            )

        command = run.call_args.args[0]
        self.assertEqual(
            command,
            [
                "/runtime/bin/kymo",
                "ensure",
                "--json",
                "--init-hold-id",
                INIT_HOLD_UUID,
            ],
        )
        self.assertNotIn(SERVER_BEARER, repr(command))

    def test_open_uses_stable_nonsecret_url_handoff(self):
        url = "http://127.0.0.1:20001/project/run"
        completed = subprocess.CompletedProcess(
            ["/runtime/bin/kymo", "open"],
            0,
            stdout=f"{url}\n",
            stderr="warning: could not open a browser\n",
        )
        with (
            mock.patch.object(
                local_runtime.shutil, "which", return_value="/runtime/bin/kymo"
            ),
            mock.patch.object(
                local_runtime.subprocess, "run", return_value=completed
            ) as run,
        ):
            with self.assertLogs("kymo", "WARNING") as logs:
                opened = local_runtime.open_local_run(
                    "project",
                    "run",
                    expected_installation_uuid=INSTALLATION_UUID,
                    timeout=17,
                )

        self.assertEqual(opened, url)
        self.assertIn("could not open a browser", logs.output[0])
        command = run.call_args.args[0]
        self.assertEqual(
            command,
            [
                "/runtime/bin/kymo",
                "open",
                "--expected-installation-uuid",
                INSTALLATION_UUID,
                "--",
                "project",
                "run",
            ],
        )
        self.assertNotIn(SERVER_BEARER, repr(command))
        self.assertFalse(run.call_args.kwargs["close_fds"])

    def test_open_disambiguates_ids_equal_to_launcher_options(self):
        completed = subprocess.CompletedProcess(
            ["/runtime/bin/kymo", "open"], 0, stdout="", stderr=""
        )
        with (
            mock.patch.object(
                local_runtime.shutil, "which", return_value="/runtime/bin/kymo"
            ),
            mock.patch.object(
                local_runtime.subprocess, "run", return_value=completed
            ) as run,
        ):
            local_runtime.open_local_run(
                "--no-browser",
                "--expected-installation-uuid",
                expected_installation_uuid=INSTALLATION_UUID,
            )

        self.assertEqual(
            run.call_args.args[0],
            [
                "/runtime/bin/kymo",
                "open",
                "--expected-installation-uuid",
                INSTALLATION_UUID,
                "--",
                "--no-browser",
                "--expected-installation-uuid",
            ],
        )

    def test_local_run_url_is_stable_and_open_checks_installation(self):
        restored = {
            name: getattr(client_module, name)
            for name in (
                "_is_initialized",
                "_mode",
                "_project_id",
                "_run_id",
                "_local_installation_uuid",
                "_url_base",
            )
        }
        try:
            client_module._is_initialized = True
            client_module._mode = "local"
            client_module._project_id = "project/one"
            client_module._run_id = "run two"
            client_module._local_installation_uuid = INSTALLATION_UUID
            client_module._url_base = "http://127.0.0.1:49152"
            self.assertEqual(
                client_module.run_url(),
                "http://127.0.0.1:49152/project%2Fone/run%20two",
            )
            with mock.patch(
                "kymo._local_runtime.open_local_run",
                return_value="http://127.0.0.1:49152/project%2Fone/run%20two",
            ) as open_local_run:
                self.assertEqual(client_module.open_run(), client_module.run_url())
            open_local_run.assert_called_once_with(
                "project/one",
                "run two",
                expected_installation_uuid=INSTALLATION_UUID,
            )
        finally:
            for name, value in restored.items():
                setattr(client_module, name, value)

    def test_ensure_rejects_an_invalid_init_hold_before_launch(self):
        with (
            mock.patch.object(
                local_runtime.shutil,
                "which",
                side_effect=AssertionError("invalid hold launched kymo"),
            ) as which,
            self.assertRaisesRegex(ValueError, "badly formed hexadecimal UUID"),
        ):
            local_runtime.ensure_local_endpoint(init_hold_id="not-a-uuid")

        which.assert_not_called()

    def test_local_init_acknowledges_the_exact_launcher_hold(self):
        endpoint = local_runtime.endpoint_from_worker_config(endpoint_payload())
        captured = {}

        class StopAfterInitRun(Exception):
            pass

        def control_rpc(method, request, *_args, **_kwargs):
            captured["method"] = method
            captured["request"] = request
            raise StopAfterInitRun

        restored_globals = {
            name: getattr(client_module, name)
            for name in (
                "_project_id",
                "_run_id",
                "_run_name",
                "_server_address",
                "_mode",
                "_local_installation_uuid",
                "_url_base",
                "_cdn_address",
            )
        }
        try:
            with (
                mock.patch.object(
                    client_module.uuid,
                    "uuid4",
                    return_value=uuid.UUID(INIT_HOLD_UUID),
                ),
                mock.patch(
                    "kymo._local_runtime.ensure_local_endpoint",
                    return_value=endpoint,
                ) as ensure,
                mock.patch.object(
                    client_module, "_run_control_rpc", side_effect=control_rpc
                ),
                self.assertRaises(StopAfterInitRun),
            ):
                client_module.init(
                    mode="local",
                    project_id="project",
                    run_id="run",
                    run_name="name",
                    system_metrics=False,
                )
            self.assertEqual(client_module._url_base, endpoint.dashboard_origin)
        finally:
            for name, value in restored_globals.items():
                setattr(client_module, name, value)

        ensure.assert_called_once_with(init_hold_id=INIT_HOLD_UUID)
        self.assertEqual(captured["method"], "InitRun")
        self.assertEqual(captured["request"].local_hold_id, INIT_HOLD_UUID)

    def test_ensure_refuses_a_replacement_installation(self):
        completed = subprocess.CompletedProcess(
            ["kymo", "ensure", "--json"],
            0,
            stdout=json.dumps(endpoint_payload()),
        )
        with (
            mock.patch.object(local_runtime.shutil, "which", return_value="kymo"),
            mock.patch.object(local_runtime.subprocess, "run", return_value=completed),
            self.assertRaisesRegex(RuntimeError, "installation identity changed"),
        ):
            local_runtime.ensure_local_endpoint(
                expected_installation_uuid="33333333-3333-4333-8333-333333333333"
            )

    def test_endpoint_validation_rejects_unsafe_or_ambiguous_values(self):
        invalid = (
            {"protocol_min": 3},
            {"endpoint_generation": 42},
            {"native_socket": "relative.sock"},
            {"upload_socket": "/private/tmp/mkdb2/native.sock"},
            {"dashboard_origin": "http://localhost:49152"},
            {"cdn_origin": "http://127.0.0.1:49152"},
            {"server_bearer": "A" * 42},
            {"server_bearer": ("A" * 42) + "B"},
        )
        for override in invalid:
            with (
                self.subTest(override=override),
                self.assertRaisesRegex(RuntimeError, "invalid local endpoint"),
            ):
                local_runtime.endpoint_from_worker_config(endpoint_payload(**override))

    def test_grpc_interceptor_adds_one_bearer_and_rejects_duplicates(self):
        details = types.SimpleNamespace(
            method="/kymo.Kymo/InitRun",
            timeout=5,
            metadata=(("x-test", "yes"),),
            credentials=None,
            wait_for_ready=False,
            compression=None,
        )
        interceptor = local_runtime._BearerInterceptor(SERVER_BEARER)
        updated = interceptor._details(details)
        self.assertEqual(
            tuple(updated.metadata),
            (("x-test", "yes"), ("authorization", f"Bearer {SERVER_BEARER}")),
        )

        details.metadata = (("Authorization", "Bearer duplicate"),)
        with self.assertRaisesRegex(RuntimeError, "already contains"):
            interceptor._details(details)

    def test_grpc_channel_uses_the_qualified_tonic_authority(self):
        endpoint = local_runtime.endpoint_from_worker_config(endpoint_payload())
        base = mock.Mock()
        with (
            mock.patch.object(
                local_runtime.grpc, "insecure_channel", return_value=base
            ) as open_channel,
            mock.patch.object(
                local_runtime.grpc, "intercept_channel", return_value="channel"
            ),
        ):
            self.assertEqual(local_runtime.grpc_channel(endpoint), "channel")

        open_channel.assert_called_once_with(
            endpoint.grpc_target,
            options=(("grpc.default_authority", "localhost"),),
        )

    def test_local_spool_identity_is_stable_and_contains_no_endpoint_or_secret(self):
        header = client_module._spool_header(
            "unix:///private/native.sock",
            "project",
            "run",
            "name",
            "http://localhost",
            "session",
            INSTALLATION_UUID,
        )

        self.assertEqual(header["target_kind"], "local")
        self.assertEqual(header["installation_uuid"], INSTALLATION_UUID)
        self.assertEqual(header["server_address"], "")
        self.assertEqual(header["cdn_address"], "")
        self.assertNotIn("bearer", repr(header).lower())
        self.assertEqual(
            sync_module._effective_server(header), f"local:{INSTALLATION_UUID}"
        )

    def test_local_spool_identity_is_validated_before_ensure(self):
        with self.assertRaisesRegex(ValueError, "invalid installation UUID"):
            sync_module._effective_server(
                {"target_kind": "local", "installation_uuid": "not-a-uuid"}
            )

    def test_invalid_local_spool_identity_is_quarantined_without_ensure(self):
        header = client_module._spool_header(
            "", "project", "run", "name", "", "session", INSTALLATION_UUID
        )
        header["installation_uuid"] = "not-a-uuid"
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "bad-identity.mkspool")
            writer = SpoolWriter(path, header)
            writer.write(("numeric_ts", "loss", 1, 1.25, 1_700_000_000_000))
            writer.close()

            with mock.patch(
                "kymo._local_runtime.ensure_local_endpoint",
                side_effect=AssertionError("invalid spool invoked ensure"),
            ) as ensure:
                self.assertFalse(sync_module.replay_file(path))

            ensure.assert_not_called()
            self.assertFalse(os.path.exists(path))
            self.assertTrue(os.path.exists(path + ".rejected"))

    def test_header_only_local_spool_is_retired_without_waking_the_stack(self):
        header = client_module._spool_header(
            "",
            "project",
            "run",
            "name",
            "",
            "session",
            INSTALLATION_UUID,
        )
        header.update({"v": 1, "kind": "mkdb2-spool"})
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "empty.mkspool")
            with open(path, "wb") as spool:
                pickle.dump(header, spool, protocol=pickle.HIGHEST_PROTOCOL)

            with (
                mock.patch(
                    "kymo._local_runtime.ensure_local_endpoint",
                    side_effect=AssertionError("empty spool invoked ensure"),
                ) as ensure,
                mock.patch.object(
                    sync_module,
                    "_connect",
                    side_effect=AssertionError("empty spool connected"),
                ) as connect,
            ):
                self.assertTrue(sync_module.replay_file(path))

            ensure.assert_not_called()
            connect.assert_not_called()
            self.assertFalse(os.path.exists(path))
            self.assertTrue(os.path.exists(path + ".sent"))

    def test_nonempty_local_spool_resolves_current_authenticated_endpoint(self):
        endpoint = local_runtime.endpoint_from_worker_config(endpoint_payload())
        header = client_module._spool_header(
            "",
            "project",
            "run",
            "name",
            "",
            "session",
            INSTALLATION_UUID,
        )
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "numeric.mkspool")
            writer = SpoolWriter(path, header)
            writer.write(("numeric_ts", "loss", 1, 1.25, 1_700_000_000_000))
            writer.close()
            channel = mock.Mock()

            with (
                mock.patch(
                    "kymo._local_runtime.ensure_local_endpoint",
                    return_value=endpoint,
                ) as ensure,
                mock.patch.object(
                    sync_module, "_connect", return_value=(channel, object())
                ) as connect,
                mock.patch.object(sync_module, "_send_tuples", return_value=True),
            ):
                self.assertTrue(sync_module.replay_file(path))

            ensure.assert_called_once_with(expected_installation_uuid=INSTALLATION_UUID)
            connect.assert_called_once_with(endpoint.grpc_target, endpoint)
            channel.close.assert_called_once_with()
            self.assertTrue(os.path.exists(path + ".sent"))

    def test_replacement_installation_quarantines_spool_without_connecting(self):
        header = client_module._spool_header(
            "", "project", "run", "name", "", "session", INSTALLATION_UUID
        )
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "old-installation.mkspool")
            writer = SpoolWriter(path, header)
            writer.write(("numeric_ts", "loss", 1, 1.25, 1_700_000_000_000))
            writer.close()

            with (
                mock.patch(
                    "kymo._local_runtime.ensure_local_endpoint",
                    side_effect=local_runtime.LocalInstallationMismatch(
                        "installation identity changed"
                    ),
                ),
                mock.patch.object(
                    sync_module,
                    "_connect",
                    side_effect=AssertionError("mismatched spool connected"),
                ) as connect,
            ):
                self.assertFalse(sync_module.replay_file(path))

            connect.assert_not_called()
            self.assertTrue(os.path.exists(path + ".rejected"))


if __name__ == "__main__":
    unittest.main()
