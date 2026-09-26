"""Local launcher contract and private-transport tests."""

import base64
import json
import os
import pickle
import subprocess
import tempfile
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
        completed = subprocess.CompletedProcess(
            ["python", "-m", "kymo._control_rpc"],
            0,
            stdout=expected.SerializeToString(),
            stderr=b"",
        )
        request = client_module.kymo_pb2.InitRunRequest(
            project_id="project", run_id="run", run_name="name"
        )
        with mock.patch.object(
            client_module.subprocess, "run", return_value=completed
        ) as run:
            actual = client_module._run_control_rpc(
                "InitRun",
                request,
                client_module.kymo_pb2.InitRunResponse,
                server_address=endpoint.grpc_target,
                timeout=30,
                local_endpoint=endpoint,
            )

        command = run.call_args.args[0]
        payload = json.loads(run.call_args.kwargs["input"])
        self.assertNotIn(SERVER_BEARER, repr(command))
        self.assertEqual(payload["local_endpoint"]["server_bearer"], SERVER_BEARER)
        self.assertFalse(run.call_args.kwargs["close_fds"])
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
