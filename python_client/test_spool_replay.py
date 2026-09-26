"""Focused checks for frame validation and causal spool replay."""

import contextlib
import hashlib
import io
import json
import os
import pickle
import queue
import shlex
import stat
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

from kymo import _wire as wire_module
from kymo import client as client_module
from kymo import sync as sync_module
from kymo._cdn import content_id, gallery_item, gallery_manifest, metadata_manifest
from kymo.spool import (
    LEGACY_SPOOL_KIND,
    SPOOL_KIND,
    SpoolCorruptionError,
    SpoolWriter,
    default_replay_dirs,
    default_spool_dir,
    make_spool_path,
    read_spool,
    read_spool_header,
    replay_command,
    run_spool_files,
    spool_name_prefix,
)
from kymo.types import Metadata


def numeric(value: float, *, step: int = 7) -> tuple:
    return ("numeric_ts", "train/loss", step, value, 1_700_000_000_000)


def spool_header(*, created: int = 1) -> dict:
    return {
        "server_address": "unused:1",
        "project_id": "project",
        "run_id": "run",
        "run_name": "test",
        "created_unix": created,
    }


class _Status:
    def __init__(self, value=0):
        self.value = value
        self._lock = threading.RLock()

    def get_lock(self):
        return self._lock


class _Channel:
    def close(self):
        pass


class _ImmediateConnectionAttempt:
    @staticmethod
    def poll(deadline_fn=None):
        del deadline_fn
        return None, None

    @staticmethod
    def cancel():
        pass


class _ReadyConnectionAttempt:
    @staticmethod
    def poll(deadline_fn=None):
        del deadline_fn
        return _Channel(), object()

    @staticmethod
    def cancel():
        pass


class _RunRejectedRpcError(sync_module.grpc.RpcError):
    def __init__(self, code=sync_module.grpc.StatusCode.FAILED_PRECONDITION):
        self._code = code

    def code(self):
        return self._code

    @staticmethod
    def details():
        return "run is deleted"


class _DataLossRpcError(_RunRejectedRpcError):
    def __init__(self):
        super().__init__(sync_module.grpc.StatusCode.DATA_LOSS)

    @staticmethod
    def details():
        return "same mutation version names different content"


class _PermanentUploadError(Exception):
    response = types.SimpleNamespace(status_code=400)


class FrameValidationTests(unittest.TestCase):
    def test_rich_mutation_accepts_only_consistent_final_dispositions(self):
        version = (1 << 32) | 11
        stub = mock.Mock()
        stub.PublishRichMutation.return_value = kymo_response = (
            sync_module.kymo_pb2.PublishRichMutationResponse(
                disposition=sync_module.kymo_pb2.RICH_MUTATION_SUPERSEDED,
                stored_version=version + 1,
            )
        )
        with self.assertLogs(wire_module._log, level="WARNING") as logs:
            self.assertTrue(
                wire_module._publish_rich_mutation(
                    stub, "project", "run", "metric", 1, "value.json", 100, version
                )
            )
        self.assertIn("was superseded", "\n".join(logs.output))
        request = stub.PublishRichMutation.call_args.args[0]
        self.assertEqual(request.mutation_version, version)
        self.assertEqual(request.cdn_key, "value.json")

        kymo_response.stored_version = version
        with self.assertRaisesRegex(RuntimeError, "invalid superseding"):
            wire_module._publish_rich_mutation(
                stub, "project", "run", "metric", 1, "value.json", 100, version
            )

    def test_manifest_bytes_are_frozen(self):
        # Replay must reproduce what an earlier client published under the same mutation version, so these bytes and content ids may never change; a new format needs a new "v".
        gallery = gallery_manifest(
            [
                gallery_item(
                    "a" * 64 + ".png", extension="png", caption="step 9 — ünïcode"
                ),
                gallery_item(
                    "b" * 64 + ".bin",
                    content_type="application/x-npy",
                    filename="weights.npy",
                ),
            ]
        )
        self.assertEqual(
            gallery,
            b'{"v": 1, "class": "image_gallery", "items": [{"content_type": "image/png", '
            b'"caption": "step 9 \\u2014 \\u00fcn\\u00efcode", "resource": "'
            + b"a"
            * 64
            + b'.png"}, '
            b'{"content_type": "application/x-npy", "filename": "weights.npy", "resource": "'
            + b"b" * 64
            + b'.bin"}]}',
        )
        self.assertEqual(
            content_id(gallery, "json"),
            "840d6dba1aee8ec2274b5744595aa988893c33a3c3ec679691964c9f4a3a1505.json",
        )
        # The caller's key order is kept, not sorted.
        metadata = metadata_manifest(
            {"zeta": 1, "alpha": [1.5, "x"], "nested": {"b": None, "a": True}}
        )
        self.assertEqual(
            metadata,
            b'{\n  "v": 1,\n  "class": "metadata",\n  "data": {\n    "zeta": 1,\n'
            b'    "alpha": [\n      1.5,\n      "x"\n    ],\n    "nested": {\n'
            b'      "b": null,\n      "a": true\n    }\n  }\n}',
        )
        self.assertEqual(
            content_id(metadata, "json"),
            "a8c998a6bafafe65d6b0d81f2e434bf09be03992abbc52a51fdbe30bbe27ad2a.json",
        )

    def test_gallery_manifest_text_replaces_lone_surrogates(self):
        items = [
            gallery_item("image.png", extension="png", caption="preview-\ud800"),
            gallery_item(
                "resource.bin",
                filename="model-\udcff.bin",
                content_type="application/x-\ud800",
            ),
            gallery_item(
                "unicode.bin",
                filename="modèle-雪.bin",
                content_type="application/octet-stream",
            ),
        ]

        decoded = json.loads(gallery_manifest(items).decode("utf-8"))
        self.assertEqual(decoded["items"][0]["caption"], "preview-?")
        self.assertEqual(decoded["items"][1]["filename"], "model-?.bin")
        self.assertEqual(decoded["items"][1]["content_type"], "application/x-?")
        self.assertEqual(decoded["items"][2]["filename"], "modèle-雪.bin")
        for item in decoded["items"]:
            for value in item.values():
                if isinstance(value, str):
                    value.encode("utf-8")

    def test_rich_mutation_maps_data_loss_to_permanent_conflict(self):
        stub = mock.Mock()
        stub.PublishRichMutation.side_effect = _DataLossRpcError()

        with self.assertRaisesRegex(
            wire_module._RichMutationDataLoss, "different content"
        ):
            wire_module._publish_rich_mutation(
                stub,
                "project",
                "run",
                "metric",
                1,
                "value.json",
                100,
                (1 << 32) | 1,
            )

    def test_reduced_versioned_manifest_gets_a_fresh_mutation_identity(self):
        original_version = (3 << 32) | 7
        replacement_version = original_version + 1
        record = (
            "cdn_batch_encoded_mutation_reserved",
            "gallery",
            4,
            [
                {"kind": "image", "data": b"bad", "ext": "png"},
                {"kind": "image", "data": b"good", "ext": "png"},
            ],
            1_700_000_000_000,
            original_version,
            replacement_version,
        )
        stub = mock.Mock()
        stub.PublishRichMutation.return_value = (
            sync_module.kymo_pb2.PublishRichMutationResponse(
                disposition=sync_module.kymo_pb2.RICH_MUTATION_ACCEPTED,
                stored_version=replacement_version,
            )
        )

        def upload(_client, _url, data, _ext, *, expected_id=None):
            if data == b"bad":
                raise _PermanentUploadError("rejected")
            return expected_id

        with mock.patch.object(sync_module, "_upload_to_cdn", side_effect=upload):
            self.assertTrue(
                sync_module._replay_cdn_record(
                    stub,
                    object(),
                    "unused",
                    "project",
                    "run",
                    record,
                    {},
                )
            )

        request = stub.PublishRichMutation.call_args.args[0]
        self.assertEqual(request.mutation_version, replacement_version)
        self.assertNotEqual(request.mutation_version, original_version)
        stub.InitRun.assert_not_called()

    def test_reserved_successor_uses_a_forward_safe_record_kind(self):
        record = (
            "cdn_batch_encoded_mutation_reserved",
            "gallery",
            4,
            [],
            1_700_000_000_000,
            (3 << 32) | 7,
            (3 << 32) | 8,
        )
        legacy_known_kinds = set(sync_module._VERSIONED_RICH_KINDS) - {
            "cdn_batch_encoded_mutation_reserved"
        }
        self.assertEqual(
            sync_module._validate_spool_record(record, legacy_known_kinds),
            "cdn_batch_encoded_mutation_reserved",
        )
        self.assertEqual(
            sync_module._validate_spool_record(
                record, set(sync_module._VERSIONED_RICH_KINDS)
            ),
            "cdn_batch_encoded_mutation_reserved",
        )

    def test_reduced_legacy_versioned_manifest_fails_safe_without_reservation(self):
        original_version = (3 << 32) | 7
        record = (
            "cdn_batch_encoded_mutation",
            "gallery",
            4,
            [{"kind": "image", "data": b"bad", "ext": "png"}],
            1_700_000_000_000,
            original_version,
        )

        def upload(_client, _url, data, _ext, *, expected_id=None):
            if data == b"bad":
                raise _PermanentUploadError("rejected")
            return expected_id

        with (
            mock.patch.object(
                sync_module,
                "_upload_to_cdn",
                side_effect=upload,
            ),
            self.assertRaisesRegex(
                client_module._RichMutationDataLoss,
                "causally reserved mutation identity",
            ),
        ):
            sync_module._replay_cdn_record(
                mock.Mock(), object(), "unused", "project", "run", record, {}
            )

    def test_spool_failure_outranks_a_later_terminal_rejection(self):
        self.assertEqual(
            client_module._merged_worker_failure(
                client_module._WORKER_EXIT_SPOOL_FAILED,
                client_module._WORKER_EXIT_RUN_DELETED,
            ),
            client_module._WORKER_EXIT_SPOOL_FAILED,
        )
        self.assertEqual(
            client_module._merged_worker_failure(
                client_module._WORKER_EXIT_RUN_DELETED,
                client_module._WORKER_EXIT_SPOOL_FAILED,
            ),
            client_module._WORKER_EXIT_SPOOL_FAILED,
        )

    def test_spool_failure_outranks_a_quarantine_which_outranks_other_failures(self):
        merged = client_module._merged_worker_failure
        spool_failed = client_module._WORKER_EXIT_SPOOL_FAILED
        data_loss = client_module._WORKER_EXIT_DATA_LOSS
        deleted = client_module._WORKER_EXIT_RUN_DELETED
        self.assertEqual(merged(deleted, data_loss), data_loss)
        self.assertEqual(merged(data_loss, deleted), data_loss)
        self.assertEqual(merged(data_loss, spool_failed), spool_failed)
        self.assertEqual(merged(spool_failed, data_loss), spool_failed)
        self.assertEqual(merged(0, data_loss), data_loss)
        self.assertEqual(merged(data_loss, 0), data_loss)

    def test_trash_project_id_is_reserved_exactly(self):
        with self.assertRaisesRegex(ValueError, "reserved"):
            client_module._validate_project_id("trash")
        client_module._validate_project_id("Trash")
        client_module._validate_project_id("trash-run")

    def test_live_cdn_upload_rejects_a_non_content_addressed_response(self):
        response = mock.Mock()
        response.json.return_value = {"resource_id": "wrong.json"}
        http_client = mock.Mock()
        http_client.post.return_value = response

        with self.assertRaisesRegex(RuntimeError, "CDN returned"):
            client_module._upload_to_cdn(
                http_client, "https://cdn.invalid", b"manifest", "json"
            )

        response.raise_for_status.assert_called_once_with()

    def test_cdn_upload_reuses_a_precomputed_content_id(self):
        response = mock.Mock()
        response.json.return_value = {"resource_id": "known.json"}
        http_client = mock.Mock()
        http_client.post.return_value = response

        with mock.patch.object(
            wire_module,
            "content_id",
            side_effect=AssertionError("payload was hashed twice"),
        ):
            resource_id = client_module._upload_to_cdn(
                http_client,
                "https://cdn.invalid///",
                b"large payload",
                "json",
                expected_id="known.json",
            )

        self.assertEqual(resource_id, "known.json")
        self.assertEqual(
            http_client.post.call_args.args[0],
            "https://cdn.invalid/cdn/upload",
        )
        self.assertNotIn("timeout", http_client.post.call_args.kwargs)

    def test_log_cdn_rejects_bad_keys_before_publication(self):
        bad_keys = [object(), "\ud800", "x" * client_module._MAX_POINT_BYTES]
        for key in bad_keys:
            with (
                self.subTest(key_type=type(key).__name__),
                mock.patch.object(client_module, "_is_initialized", True),
                mock.patch.object(client_module, "_publish_queue_items") as publish,
                self.assertRaises((TypeError, ValueError)),
            ):
                client_module.log_cdn({"demo/key": key}, step=1)
            publish.assert_not_called()

    def test_versioned_log_cdn_enforces_the_rich_resource_id_bound(self):
        with (
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_rich_writer_epoch", 1),
            mock.patch.object(client_module, "_publish_queue_items") as publish,
            self.assertRaisesRegex(ValueError, "versioned rich-write limit"),
        ):
            client_module.log_cdn(
                {"demo/key": "x" * (wire_module._MAX_RICH_RESOURCE_ID_BYTES + 1)},
                step=1,
            )
        publish.assert_not_called()

    def test_oversized_non_text_record_is_permanently_invalid(self):
        record = (
            "cdn_ts",
            "demo/key",
            1,
            "x" * client_module._MAX_POINT_BYTES,
            1_700_000_000_000,
        )
        with self.assertRaisesRegex(
            client_module._PermanentPointError, "too large to ingest"
        ):
            list(client_module._chunk_tuples([record]))

    def test_finite_numeric_overflow_is_permanently_invalid_for_replay(self):
        with self.assertRaisesRegex(
            client_module._PermanentPointError, "protobuf float32 range"
        ):
            list(client_module._chunk_tuples([numeric(3.5e38)]))

    def test_unary_replay_rejects_a_short_server_response(self):
        stub = mock.Mock()
        stub.IngestMetrics.return_value = sync_module.kymo_pb2.IngestResponse(
            points_received=0
        )

        self.assertFalse(
            client_module._send_tuples(stub, "project", "run", [numeric(1.0)])
        )
        self.assertEqual(
            stub.IngestMetrics.call_args.kwargs["timeout"],
            client_module._SEND_CALL_TIMEOUT,
        )

    def test_unary_replay_does_not_retry_a_deleted_run(self):
        stub = mock.Mock()
        stub.IngestMetrics.side_effect = _RunRejectedRpcError()

        with self.assertRaisesRegex(client_module._TerminalRunError, "deleted"):
            client_module._send_tuples(stub, "project", "run", [numeric(1.0)])

        stub.IngestMetrics.assert_called_once()

    def test_live_rich_upload_rejects_a_short_server_response(self):
        stub = mock.Mock()
        stub.IngestMetrics.return_value = sync_module.kymo_pb2.IngestResponse(
            points_received=0
        )
        with (
            mock.patch.object(
                client_module,
                "_upload_to_cdn",
                side_effect=["payload.bin", "manifest.json"],
            ),
            self.assertLogs("kymo", level="WARNING"),
        ):
            self.assertFalse(
                client_module._process_cdn_batch(
                    stub,
                    object(),
                    "unused",
                    "project",
                    "run",
                    "gallery",
                    7,
                    [client_module.Resource(b"payload", "payload.bin")],
                    send_placeholder=False,
                )
            )

    def test_sync_connect_closes_a_channel_that_never_becomes_ready(self):
        channel = mock.Mock()
        ready = mock.Mock()
        ready.result.side_effect = RuntimeError("not ready")
        with (
            mock.patch.object(
                sync_module.grpc, "insecure_channel", return_value=channel
            ),
            mock.patch.object(
                sync_module.grpc, "channel_ready_future", return_value=ready
            ),
            self.assertRaisesRegex(RuntimeError, "not ready"),
        ):
            sync_module._connect("unused")

        channel.close.assert_called_once_with()


class LaneFailoverTests(unittest.TestCase):
    def assert_wait_reports_spooled(self, status, spooled):
        with (
            mock.patch.object(client_module, "_is_initialized", True),
            mock.patch.object(client_module, "_queue_status", status),
            mock.patch.object(client_module, "_upload_failure", _Status()),
            mock.patch.object(client_module, "_upload_spooled", spooled),
            self.assertLogs("kymo", level="WARNING"),
        ):
            self.assertFalse(client_module.wait_for_upload(timeout=0))

    def test_numeric_byte_cap_spools_whole_lane_and_future_points_in_order(self):
        source = queue.Queue()
        source.put([numeric(1.0), numeric(2.0)])
        status = _Status(3)
        spooled = _Status()
        first_spill = threading.Event()
        real_spill = client_module._spill_tuple

        def observe_spill(spool, point):
            result = real_spill(spool, point)
            first_spill.set()
            return result

        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "numeric.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(),
                ),
                mock.patch.object(
                    client_module.grpc,
                    "insecure_channel",
                    return_value=_Channel(),
                ),
                mock.patch.object(
                    client_module.kymo_pb2_grpc,
                    "KymoStub",
                    return_value=object(),
                ),
                mock.patch.object(
                    client_module, "_spill_tuple", side_effect=observe_spill
                ),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.001),
                mock.patch.dict(
                    os.environ,
                    {
                        "KYMO_MAX_BUFFER_POINTS": "1000",
                        "KYMO_MAX_BUFFER_BYTES": "1",
                    },
                ),
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "project", "run", source, status),
                    kwargs={"spool_path": path, "upload_spooled": spooled},
                )
                worker.start()
                self.assertTrue(first_spill.wait(timeout=1))
                source.put([numeric(3.0)])
                source.put(None)
                worker.join(timeout=2)

            self.assertFalse(worker.is_alive())
            _, records = read_spool(path)
            self.assertEqual([record[3] for record in records], [1.0, 2.0, 3.0])
            self.assertEqual(status.value, 0)
            self.assertEqual(spooled.value, 1)
            self.assert_wait_reports_spooled(status, spooled)

    def test_rich_byte_cap_spools_whole_lane_and_future_entries_in_order(self):
        def metadata(value: int) -> tuple:
            return ("metadata_batch", "info/run_info", 0, Metadata({"v": value}))

        source = queue.Queue()
        source.put([metadata(1), metadata(2)])
        status = _Status(3)
        spooled = _Status()
        first_spill = threading.Event()
        real_spill = client_module._spill_tuple

        def observe_spill(spool, point):
            result = real_spill(spool, point)
            first_spill.set()
            return result

        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "rich.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(),
                ),
                mock.patch.object(
                    client_module.grpc,
                    "insecure_channel",
                    return_value=_Channel(),
                ),
                mock.patch.object(
                    client_module.kymo_pb2_grpc,
                    "KymoStub",
                    return_value=object(),
                ),
                mock.patch.object(
                    client_module, "_spill_tuple", side_effect=observe_spill
                ),
                mock.patch.object(client_module, "_MAX_CDN_QUEUE", 1000),
                mock.patch.object(client_module, "_MAX_CDN_QUEUE_BYTES", 1),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.001),
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "project", "run", source, status),
                    kwargs={"spool_path": path, "upload_spooled": spooled},
                )
                worker.start()
                self.assertTrue(first_spill.wait(timeout=1))
                source.put([metadata(3)])
                source.put(None)
                worker.join(timeout=2)

            self.assertFalse(worker.is_alive())
            _, records = read_spool(path)
            self.assertEqual([record[3]["v"] for record in records], [1, 2, 3])
            self.assertEqual(status.value, 0)
            self.assertEqual(spooled.value, 1)
            self.assert_wait_reports_spooled(status, spooled)

    def test_versioned_direct_cdn_key_counts_its_retained_bytes(self):
        version = (3 << 32) | 1
        direct = (
            "cdn_key_mutation",
            "gallery",
            0,
            "x" * 200,
            1_700_000_000_000,
            version,
        )
        source = queue.Queue()
        source.put([direct])
        status = _Status(1)
        spooled = _Status()
        first_spill = threading.Event()
        real_spill = client_module._spill_tuple

        def observe_spill(spool, point):
            result = real_spill(spool, point)
            first_spill.set()
            return result

        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "direct-rich.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ImmediateConnectionAttempt(),
                ),
                mock.patch.object(
                    client_module.grpc,
                    "insecure_channel",
                    return_value=_Channel(),
                ),
                mock.patch.object(
                    client_module.kymo_pb2_grpc,
                    "KymoStub",
                    return_value=object(),
                ),
                mock.patch.object(
                    client_module, "_spill_tuple", side_effect=observe_spill
                ),
                mock.patch.dict(os.environ, {"KYMO_MAX_RICH_BUFFER_BYTES": "100"}),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.001),
            ):
                worker = threading.Thread(
                    target=client_module._upload_worker,
                    args=("unused", "project", "run", source, status),
                    kwargs={"spool_path": path, "upload_spooled": spooled},
                )
                worker.start()
                self.assertTrue(first_spill.wait(timeout=1))
                source.put(None)
                worker.join(timeout=2)

            self.assertFalse(worker.is_alive())
            _, records = read_spool(path)
            try:
                self.assertEqual(list(records), [direct])
            finally:
                records.close()
            self.assertEqual(status.value, 0)
            self.assertEqual(spooled.value, 1)


class SpoolDurabilityTests(unittest.TestCase):
    def test_replay_command_round_trips_arbitrary_posix_paths(self):
        paths = [
            "/shared/my spools/run 1.mkspool",
            "/shared/it's;$(unsafe).mkspool",
            "--server.mkspool",
        ]
        self.assertEqual(
            shlex.split(replay_command(paths)),
            ["python", "-m", "kymo.sync", "--", *paths],
        )

    def test_bidi_terminal_rejection_fails_queue_without_retry_or_spool(self):
        source = queue.Queue()
        source.put([numeric(1.0)])
        source.put(None)
        status = _Status(1)
        spooled = _Status()
        failure = _Status()
        terminal = _Status()

        class RejectedStream:
            def __init__(self, _stub, _capacity):
                self.cancelled = False

            @staticmethod
            def poll_acks():
                return [("error", _RunRejectedRpcError())]

            def cancel(self):
                # Parent-side shutdown must learn terminality before any
                # potentially blocking transport teardown begins.
                assert terminal.value == 1
                self.cancelled = True

        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "terminal.mkspool")
            with (
                mock.patch.object(
                    client_module,
                    "_connect",
                    return_value=_ReadyConnectionAttempt(),
                ) as connect,
                mock.patch.object(client_module, "_BidiStream", RejectedStream),
                mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.001),
                self.assertRaises(SystemExit) as exit_error,
            ):
                client_module._upload_worker(
                    "unused",
                    "project",
                    "run",
                    source,
                    status,
                    spool_path=path,
                    upload_failure=failure,
                    upload_spooled=spooled,
                    upload_terminal=terminal,
                )

            self.assertEqual(
                exit_error.exception.code, client_module._WORKER_EXIT_RUN_DELETED
            )
            self.assertEqual(status.value, 0)
            self.assertEqual(failure.value, client_module._WORKER_EXIT_RUN_DELETED)
            self.assertEqual(spooled.value, 0)
            self.assertEqual(terminal.value, 1)
            self.assertFalse(os.path.exists(path))
            connect.assert_called_once()

    def test_flush_exposes_accounted_records_before_writer_close(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "worker.mkspool")
            writer = SpoolWriter(path, spool_header())
            point = numeric(1.0)
            writer.write(point)

            writer.flush()
            _header, records = read_spool(path)
            self.assertEqual(list(records), [point])

            writer.close()
            self.assertEqual(os.stat(path).st_mode & 0o077, 0)

    def test_spool_in_shared_dir_is_readable_by_replay_users(self):
        # A writer uid that nobody replays as (e.g. --container-remap-root on NFS) must not produce owner-only spools in a deliberately shared spool dir, or sync from a login node needs root.
        with tempfile.TemporaryDirectory() as spool_dir:
            os.chmod(spool_dir, 0o777)
            path = os.path.join(spool_dir, "worker.mkspool")
            writer = SpoolWriter(path, spool_header())
            old_umask = os.umask(0o077)
            try:
                writer.write(numeric(1.0))
            finally:
                os.umask(old_umask)
            writer.close()
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o644)

    @unittest.skipUnless(hasattr(os, "fchmod"), "fchmod is Unix-only")
    def test_spool_closes_raw_descriptor_when_mode_application_fails(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "worker.mkspool")
            writer = SpoolWriter(path, spool_header())
            opened_fd = None
            real_open = os.open

            def tracked_open(*args, **kwargs):
                nonlocal opened_fd
                opened_fd = real_open(*args, **kwargs)
                return opened_fd

            with (
                mock.patch("kymo.spool.os.open", side_effect=tracked_open),
                mock.patch("kymo.spool.os.fchmod", side_effect=OSError("read-only fs")),
                self.assertRaisesRegex(OSError, "read-only fs"),
            ):
                writer.write(numeric(1.0))

            self.assertIsNone(writer._fh)
            with self.assertRaises(OSError):
                os.fstat(opened_fd)

    def test_sealed_writer_is_immutable_and_excludes_external_sync(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "worker.mkspool")
            point = numeric(1.0)
            writer = SpoolWriter(path, spool_header())
            writer.write(point)

            self.assertEqual(writer.seal(), path)
            self.assertTrue(sync_module._writer_active(path))
            with self.assertRaisesRegex(RuntimeError, "sealed spool"):
                writer.write(numeric(2.0))

            channel = _Channel()
            with (
                mock.patch.object(sync_module, "_connect") as connect,
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertFalse(sync_module.replay_file(path))
            connect.assert_not_called()

            with (
                mock.patch.object(
                    sync_module, "_connect", return_value=(channel, object())
                ),
                mock.patch.object(sync_module, "_send_tuples", return_value=True),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertTrue(sync_module.replay_file(path, _writer_lock_held=True))

            self.assertFalse(os.path.exists(path))
            self.assertTrue(os.path.exists(path + ".sent"))
            writer.close()

    def test_seal_leaves_the_durability_barrier_to_the_replay_thread(self):
        """seal() runs on the worker's only queue-consumer thread, so its fsync
        moved to sync() — a stalled mount must not stop the queue draining. The
        owner's later close() of an already-synced segment must not re-fsync
        there either, or the barrier is back on that thread."""
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "worker.mkspool")
            writer = SpoolWriter(path, spool_header())
            writer.write(numeric(1.0))

            with mock.patch("os.fsync") as fsync:
                writer.seal()
                self.assertEqual(fsync.call_count, 0)
                # The records, then the new file's entry and the spool directory's own entry.
                writer.sync()
                self.assertEqual(fsync.call_count, 3)
                writer.sync()
                writer.close()
                self.assertEqual(fsync.call_count, 3)

        with tempfile.TemporaryDirectory() as spool_dir:
            # Unsynced records still get their barrier: only a redundant one goes.
            path = os.path.join(spool_dir, "active.mkspool")
            writer = SpoolWriter(path, spool_header())
            writer.write(numeric(1.0))
            with mock.patch("os.fsync") as fsync:
                writer.close()
                self.assertEqual(fsync.call_count, 3)

    def test_directory_sync_surfaces_storage_failures_but_not_missing_support(self):
        import errno

        from kymo import spool as spool_module

        def failing_fsync(code):
            def fsync(fd):
                if stat.S_ISDIR(os.fstat(fd).st_mode):
                    raise OSError(code, os.strerror(code))

            return fsync

        with tempfile.TemporaryDirectory() as spool_dir:
            with mock.patch("os.fsync", failing_fsync(errno.EINVAL)):
                spool_module.sync_directory(spool_dir)
            with mock.patch("os.fsync", failing_fsync(errno.EIO)):
                with self.assertRaises(OSError):
                    spool_module.sync_directory(spool_dir)

                # A new segment whose entry cannot be made durable is not reported as synced.
                path = os.path.join(spool_dir, "worker.mkspool")
                writer = SpoolWriter(path, spool_header())
                writer.write(numeric(1.0))
                with self.assertRaises(OSError):
                    writer.sync()

                # A finished retirement only warns.
                retired_source = os.path.join(spool_dir, "retire.mkspool")
                open(retired_source, "wb").close()
                with self.assertLogs("kymo", "WARNING"):
                    retired = spool_module.quarantine_spool(retired_source)
                self.assertTrue(os.path.exists(retired))
            writer.close()

    def test_run_spool_files_finds_segments_the_owner_never_named(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            owned = make_spool_path(
                "project", "run", "worker", spool_dir, session="mine"
            )
            rotated = make_spool_path(
                "project", "run", "worker", spool_dir, session="mine"
            )
            salvaged = make_spool_path(
                "project", "run", "salvage", spool_dir, session="mine"
            )
            sibling = make_spool_path(
                "project", "run", "worker", spool_dir, session="theirs"
            )
            legacy = make_spool_path("project", "run", "worker", spool_dir)
            other_run = make_spool_path(
                "project", "other", "worker", spool_dir, session="mine"
            )
            for path in (owned, rotated, salvaged, sibling, legacy, other_run):
                with open(path, "wb") as fh:
                    fh.write(b"spooled")
            # Neither state is replayable: one is delivered, one is withdrawn.
            for suffix in (".sent", ".deleted"):
                with open(
                    make_spool_path(
                        "project", "run", "worker", spool_dir, session="mine"
                    )
                    + suffix,
                    "wb",
                ) as fh:
                    fh.write(b"spooled")
            self.assertEqual(
                run_spool_files("project", "run", spool_dir, session="mine"),
                sorted([owned, rotated, salvaged]),
            )
            self.assertNotIn(
                sibling, run_spool_files("project", "run", spool_dir, session="mine")
            )
            self.assertNotIn(
                legacy, run_spool_files("project", "run", spool_dir, session="mine")
            )

    def test_run_spool_files_rejects_an_empty_ownership_session(self):
        with self.assertRaisesRegex(ValueError, "nonempty session"):
            run_spool_files("project", "run", session="")

    def test_run_spool_files_tolerates_a_missing_spool_directory(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            self.assertEqual(
                run_spool_files(
                    "project",
                    "run",
                    os.path.join(spool_dir, "absent"),
                    session="mine",
                ),
                [],
            )

    def test_worker_final_spool_accounting_does_not_restat_and_keeps_session_header(
        self,
    ):
        source = queue.Queue()
        point = numeric(1.0)
        source.put([point])
        source.put(None)
        status = _Status(1)
        spooled = _Status()
        deadline = _Status(time.monotonic() - 1)
        session = "session-a"
        real_exists = os.path.exists
        real_spill = client_module._spill_tuple
        spilled = False

        def observe_spill(spool, record):
            nonlocal spilled
            result = real_spill(spool, record)
            spilled = True
            return result

        def forbid_final_restat(candidate):
            if spilled:
                raise AssertionError("worker restatted a known spool path")
            return real_exists(candidate)

        with tempfile.TemporaryDirectory() as spool_dir:
            path = make_spool_path(
                "project", "run", "worker", spool_dir, session=session
            )
            with (
                mock.patch.object(
                    client_module, "_spill_tuple", side_effect=observe_spill
                ),
                mock.patch.object(os.path, "exists", side_effect=forbid_final_restat),
            ):
                client_module._upload_worker(
                    "unused",
                    "project",
                    "run",
                    source,
                    status,
                    shutdown_deadline=deadline,
                    spool_path=path,
                    upload_spooled=spooled,
                    session=session,
                )

            header, records = read_spool(path)
            self.assertEqual(header["session"], session)
            self.assertEqual(list(records), [point])
            self.assertEqual(status.value, 0)
            self.assertEqual(spooled.value, 1)

    def test_worker_flushes_before_releasing_spool_accounting(self):
        source = queue.Queue()
        source.put([numeric(1.0)])
        source.put(None)
        status = _Status(1)
        spooled = _Status()
        failure = _Status()
        observed = []

        deadline = _Status(0.0)
        deadline.value = time.monotonic() - 1
        with tempfile.TemporaryDirectory() as spool_dir:
            # A real path: the worker's exit accounting reports the spool files
            # that actually survive, so a fake that never lands one would claim
            # delivery.
            spool_path = os.path.join(spool_dir, "observed.mkspool")
            with open(spool_path, "wb") as fh:
                fh.write(b"spooled")

            class ObservedSpool:
                path = spool_path

                @staticmethod
                def write(_record):
                    pass

                @staticmethod
                def flush():
                    observed.append(status.value)

                @staticmethod
                def close():
                    return spool_path

            with mock.patch.object(
                client_module, "SpoolWriter", return_value=ObservedSpool()
            ):
                client_module._upload_worker(
                    "unused",
                    "project",
                    "run",
                    source,
                    status,
                    shutdown_deadline=deadline,
                    upload_failure=failure,
                    upload_spooled=spooled,
                )

        self.assertEqual(observed, [1])
        self.assertEqual(status.value, 0)
        self.assertEqual(spooled.value, 1)
        self.assertEqual(failure.value, 0)

    def test_worker_reports_a_flush_failure_before_releasing_accounting(self):
        source = queue.Queue()
        source.put([numeric(1.0)])
        source.put(None)
        status = _Status(1)
        spooled = _Status()
        failure = _Status()

        class FailingSpool:
            path = "failed.mkspool"

            @staticmethod
            def write(_record):
                pass

            @staticmethod
            def flush():
                raise OSError("flush failed")

            @staticmethod
            def close():
                return None

        deadline = _Status(0.0)
        deadline.value = time.monotonic() - 1
        with (
            mock.patch.object(
                client_module, "SpoolWriter", return_value=FailingSpool()
            ),
            self.assertRaises(SystemExit) as exit_error,
        ):
            client_module._upload_worker(
                "unused",
                "project",
                "run",
                source,
                status,
                shutdown_deadline=deadline,
                upload_failure=failure,
                upload_spooled=spooled,
            )

        self.assertEqual(
            exit_error.exception.code, client_module._WORKER_EXIT_SPOOL_FAILED
        )
        self.assertEqual(status.value, 0)
        self.assertEqual(spooled.value, 0)
        self.assertEqual(failure.value, client_module._WORKER_EXIT_SPOOL_FAILED)

    def test_worker_accounting_lock_wait_is_bounded(self):
        source = queue.Queue()
        source.put([numeric(1.0)])
        source.put(None)

        class LockedStatus:
            def __init__(self):
                self.value = 1
                self.lock = threading.Lock()
                self.lock.acquire()

            def get_lock(self):
                return self.lock

        class MemorySpool:
            path = "memory.mkspool"

            @staticmethod
            def write(_record):
                pass

            @staticmethod
            def flush():
                pass

            @staticmethod
            def close():
                return None

        status = LockedStatus()
        deadline = _Status(time.monotonic() - 1)
        started = time.monotonic()
        try:
            with (
                mock.patch.object(
                    client_module, "SpoolWriter", return_value=MemorySpool()
                ),
                mock.patch.object(client_module, "_STATUS_LOCK_TIMEOUT", 0.01),
                self.assertRaisesRegex(RuntimeError, "accounting lock"),
            ):
                client_module._upload_worker(
                    "unused",
                    "project",
                    "run",
                    source,
                    status,
                    shutdown_deadline=deadline,
                )
        finally:
            status.lock.release()

        self.assertLess(time.monotonic() - started, 0.5)
        self.assertEqual(status.value, 1)


class ReplayTests(unittest.TestCase):
    def _write(
        self,
        path: str,
        records: list[tuple],
        *,
        created: int = 1,
        server: str = "unused:1",
    ) -> None:
        header = spool_header(created=created)
        header["server_address"] = server
        writer = SpoolWriter(path, header)
        for record in records:
            writer.write(record)
        writer.close()

    def _write_legacy(
        self,
        path: str,
        records: list[tuple],
        *,
        created: int = 1,
        server: str = "unused:1",
    ) -> None:
        header = spool_header(created=created)
        header.update(
            server_address=server,
            v=1,
            kind=LEGACY_SPOOL_KIND,
        )
        with open(path, "wb") as spool:
            pickle.dump(header, spool, protocol=pickle.HIGHEST_PROTOCOL)
            for record in records:
                pickle.dump(record, spool, protocol=pickle.HIGHEST_PROTOCOL)

    def test_new_writer_and_legacy_reader_use_their_respective_headers(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            current = os.path.join(spool_dir, "current.mkspool")
            legacy = os.path.join(spool_dir, "legacy.mkspool")
            self._write(current, [numeric(1.0)])
            self._write_legacy(legacy, [numeric(2.0)])

            self.assertEqual(read_spool_header(current)["kind"], SPOOL_KIND)
            self.assertEqual(read_spool_header(legacy)["kind"], LEGACY_SPOOL_KIND)
            self.assertEqual(len(list(read_spool(legacy)[1])), 1)

    def test_default_writer_and_replay_directories_use_kymo_then_legacy(self):
        with (
            tempfile.TemporaryDirectory() as home,
            mock.patch.dict(os.environ, {"HOME": home}, clear=True),
        ):
            current = os.path.join(home, ".cache", "kymo", "spool")
            legacy = os.path.join(home, ".cache", "pymkdb2", "spool")
            self.assertEqual(default_spool_dir(), current)
            self.assertEqual(default_replay_dirs(), [current, legacy])

    def test_custom_writer_directory_still_scans_the_legacy_default(self):
        with (
            tempfile.TemporaryDirectory() as home,
            tempfile.TemporaryDirectory() as custom,
            mock.patch.dict(
                os.environ,
                {"HOME": home, "KYMO_SPOOL_DIR": custom},
                clear=True,
            ),
        ):
            self.assertEqual(
                default_replay_dirs(),
                [custom, os.path.join(home, ".cache", "pymkdb2", "spool")],
            )

    def test_superseded_versioned_spool_is_retired_only_after_server_ack(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "versioned.mkspool")
            version = (3 << 32) | 4
            self._write(
                path,
                [
                    (
                        "metadata_json_mutation",
                        "info/run_info",
                        0,
                        {"config": {"old": True}},
                        1_700_000_000_000,
                        version,
                    )
                ],
            )
            stub = mock.Mock()
            stub.PublishRichMutation.return_value = (
                sync_module.kymo_pb2.PublishRichMutationResponse(
                    disposition=sync_module.kymo_pb2.RICH_MUTATION_SUPERSEDED,
                    stored_version=version + 1,
                )
            )
            with (
                mock.patch.object(
                    sync_module, "_connect", return_value=(_Channel(), stub)
                ),
                mock.patch("httpx.Client", return_value=mock.Mock()),
                mock.patch.object(
                    sync_module,
                    "_upload_to_cdn",
                    side_effect=lambda *_args, expected_id=None, **_kwargs: expected_id,
                ),
            ):
                self.assertTrue(sync_module.replay_file(path, cdn="http://unused"))

            self.assertFalse(os.path.exists(path))
            self.assertTrue(os.path.exists(path + ".sent"))
            stub.PublishRichMutation.assert_called_once()

    def test_data_loss_quarantines_and_advances_to_later_run_spools(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            first = os.path.join(spool_dir, "first.mkspool")
            second = os.path.join(spool_dir, "second.mkspool")
            version = (3 << 32) | 4
            self._write(
                first,
                [
                    (
                        "metadata_json_mutation",
                        "info/run_info",
                        0,
                        {"config": {"old": True}},
                        1_700_000_000_000,
                        version,
                    )
                ],
                created=1,
            )
            self._write(second, [numeric(1.0)], created=2)
            stub = mock.Mock()
            stub.PublishRichMutation.side_effect = _DataLossRpcError()
            quarantined = set()
            with (
                mock.patch.object(
                    sync_module, "_connect", return_value=(_Channel(), stub)
                ) as connect,
                mock.patch("httpx.Client", return_value=mock.Mock()),
                mock.patch.object(
                    sync_module,
                    "_upload_to_cdn",
                    side_effect=lambda *_args, expected_id=None, **_kwargs: expected_id,
                ),
                mock.patch.object(sync_module, "_send_tuples", return_value=True),
            ):
                self.assertFalse(
                    sync_module.replay_file(
                        first,
                        cdn="http://unused",
                        quarantined_files=quarantined,
                    )
                )
                connect.reset_mock()
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    self.assertEqual(sync_module.main([spool_dir]), 1)

            self.assertFalse(os.path.exists(first))
            self.assertEqual(quarantined, {first + ".rejected"})
            self.assertTrue(os.path.exists(first + ".rejected"))
            self.assertFalse(os.path.exists(second))
            self.assertTrue(os.path.exists(second + ".sent"))
            connect.assert_called_once()
            self.assertIn("later files remain eligible", output.getvalue())

    def test_unexpected_replay_error_closes_both_transports(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "rich.mkspool")
            self._write(
                path,
                [("metadata_json", "info/run_info", 0, {"version": 1})],
            )
            channel = mock.Mock()
            http_client = mock.Mock()
            with (
                mock.patch.object(
                    sync_module, "_connect", return_value=(channel, object())
                ),
                mock.patch("httpx.Client", return_value=http_client),
                mock.patch.object(
                    sync_module,
                    "_replay_cdn_record",
                    side_effect=RuntimeError("delivery failed"),
                ),
                self.assertRaisesRegex(RuntimeError, "delivery failed"),
            ):
                sync_module.replay_file(path, cdn="http://unused")

            http_client.close.assert_called_once_with()
            channel.close.assert_called_once_with()

    def test_invalid_latest_same_key_is_quarantined_without_replaying_stale_value(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "bad.mkspool")
            oversized = (
                "cdn_ts",
                "train/loss",
                7,
                "x" * client_module._MAX_POINT_BYTES,
                1_700_000_000_001,
            )
            self._write(path, [numeric(1.0), oversized])
            existing_rejection = path + ".rejected"
            with open(existing_rejection, "wb") as fh:
                fh.write(b"earlier rejection")

            output = io.StringIO()
            with (
                mock.patch.object(
                    sync_module, "_connect", return_value=(_Channel(), object())
                ),
                mock.patch.object(sync_module, "_send_tuples") as send,
                contextlib.redirect_stdout(output),
            ):
                ok = sync_module.replay_file(path)

            self.assertFalse(ok)
            send.assert_not_called()
            self.assertFalse(os.path.exists(path))
            with open(existing_rejection, "rb") as fh:
                self.assertEqual(fh.read(), b"earlier rejection")
            self.assertTrue(os.path.exists(path + ".rejected.1"))
            self.assertIn("permanently invalid — quarantined", output.getvalue())

    def test_truncated_tail_is_quarantined_without_sending_its_prefix(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "truncated.mkspool")
            self._write(path, [numeric(1.0)])
            with open(path, "ab") as fh:
                fh.write(b"\x80\x05\x95")

            with (
                mock.patch.object(sync_module, "_connect") as connect,
                mock.patch.object(sync_module, "_send_tuples") as send,
            ):
                self.assertFalse(sync_module.replay_file(path))

            connect.assert_not_called()
            send.assert_not_called()
            self.assertFalse(os.path.exists(path))
            self.assertTrue(os.path.exists(path + ".rejected"))

    def test_malformed_header_is_quarantined_by_directory_sync(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            paths = [
                os.path.join(spool_dir, f"bad-header-{index}.mkspool")
                for index in range(2)
            ]
            for path in paths:
                with open(path, "wb") as fh:
                    fh.write(b"not a pickle")

            self.assertEqual(sync_module.main([spool_dir]), 1)

            for path in paths:
                self.assertFalse(os.path.exists(path))
                self.assertTrue(os.path.exists(path + ".rejected"))

    def test_invalid_header_project_ids_are_quarantined_before_connecting(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            for index, project_id in enumerate(("bad\0id", "trash")):
                path = os.path.join(spool_dir, f"bad-id-{index}.mkspool")
                writer = SpoolWriter(path, dict(spool_header(), project_id=project_id))
                writer.write(numeric(1.0))
                writer.close()

                with mock.patch.object(sync_module, "_connect") as connect:
                    self.assertFalse(sync_module.replay_file(path))

                connect.assert_not_called()
                self.assertTrue(os.path.exists(path + ".rejected"))

    def test_unknown_spool_kind_is_quarantined_without_connecting(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "unknown.mkspool")
            header = dict(spool_header(), v=1, kind="other-spool")
            with open(path, "wb") as spool:
                pickle.dump(header, spool, protocol=pickle.HIGHEST_PROTOCOL)
                pickle.dump(numeric(1.0), spool, protocol=pickle.HIGHEST_PROTOCOL)

            with mock.patch.object(sync_module, "_connect") as connect:
                self.assertFalse(sync_module.replay_file(path))

            connect.assert_not_called()
            self.assertTrue(os.path.exists(path + ".rejected"))

    def test_future_spool_version_is_kept_and_blocks_the_run_suffix(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            future = os.path.join(spool_dir, "x__worker_1_1.mkspool")
            later = os.path.join(spool_dir, "x__salvage_1_2.mkspool")
            header = dict(
                spool_header(created=1),
                kind="mkdb2-spool",
                v=2,
                created_unix_ns=1,
            )
            with open(future, "wb") as fh:
                pickle.dump(header, fh)
                pickle.dump(numeric(1.0), fh)
            self._write(later, [numeric(2.0)], created=2)

            with mock.patch.object(sync_module, "_connect") as connect:
                self.assertEqual(sync_module.main([spool_dir]), 1)

            connect.assert_not_called()
            self.assertTrue(os.path.exists(future))
            self.assertTrue(os.path.exists(later))
            self.assertFalse(os.path.exists(future + ".rejected"))

    def test_transient_header_read_failure_is_not_quarantined(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "retry.mkspool")
            self._write(path, [numeric(1.0)])

            with (
                mock.patch.object(
                    sync_module, "read_spool", side_effect=OSError("NFS")
                ),
                self.assertRaisesRegex(OSError, "NFS"),
            ):
                sync_module.replay_file(path)

            self.assertTrue(os.path.exists(path))
            self.assertFalse(os.path.exists(path + ".rejected"))

    def test_validation_memory_exhaustion_is_not_quarantined(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "large.mkspool")
            self._write(path, [numeric(1.0)])

            with (
                mock.patch.object(
                    sync_module,
                    "_validate_spool_record",
                    side_effect=MemoryError("retry elsewhere"),
                ),
                self.assertRaisesRegex(MemoryError, "retry elsewhere"),
            ):
                sync_module.replay_file(path)

            self.assertTrue(os.path.exists(path))
            self.assertFalse(os.path.exists(path + ".rejected"))

    def test_malformed_rich_record_is_quarantined_before_connecting(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            malformed_records = [
                (
                    "cdn_batch_encoded",
                    "gallery",
                    1,
                    [{"kind": "resource", "data": "not bytes", "ext": "bin"}],
                ),
                (
                    "cdn_batch_encoded",
                    "gallery",
                    2,
                    [
                        {
                            "kind": "image",
                            "data": b"image",
                            "ext": "png",
                            "caption": b"not JSON",
                        }
                    ],
                ),
                ("metadata_json", "bad\0name", 3, {"value": 1}),
                ("metadata_json", "metadata", 3, {"value": float("nan")}),
                (
                    "cdn_batch_encoded",
                    "gallery",
                    4,
                    [
                        {
                            "kind": "resource",
                            "data": b"data",
                            "ext": "bin\n",
                        }
                    ],
                ),
                (
                    "numeric_ts",
                    "train/loss",
                    5,
                    1.0,
                    sync_module._MAX_TIMESTAMP_MS + 1,
                ),
                numeric(3.5e38),
                (
                    "cdn_batch_encoded",
                    "gallery",
                    6,
                    [
                        {
                            "kind": "image",
                            "data": b"image",
                            "ext": "png",
                            "caption": 123,
                        }
                    ],
                ),
            ]
            for index, malformed in enumerate(malformed_records):
                with self.subTest(index=index):
                    path = os.path.join(spool_dir, f"malformed-rich-{index}.mkspool")
                    self._write(path, [malformed])
                    with mock.patch.object(sync_module, "_connect") as connect:
                        self.assertFalse(sync_module.replay_file(path))

                    connect.assert_not_called()
                    self.assertTrue(os.path.exists(path + ".rejected"))

    def test_data_only_reader_rejects_executable_pickle_globals(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "unsafe.mkspool")
            marker = os.path.join(spool_dir, "executed")
            self._write(path, [numeric(1.0)])
            command = f"touch {marker}"
            with open(path, "ab") as fh:
                fh.write(f"cos\nsystem\n(S'{command}'\ntR.".encode())

            _header, records = read_spool(path)
            with self.assertRaises(SpoolCorruptionError):
                list(records)
            self.assertFalse(os.path.exists(marker))

    def test_replay_keeps_only_last_record_for_a_full_sort_key(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "same-key.mkspool")
            self._write(path, [numeric(1.0), numeric(2.0)])
            delivered = []

            def send(_stub, _project, _run, records):
                delivered.extend(records)
                return True

            with (
                mock.patch.object(
                    sync_module, "_connect", return_value=(_Channel(), object())
                ),
                mock.patch.object(sync_module, "_send_tuples", side_effect=send),
            ):
                self.assertTrue(sync_module.replay_file(path))

            self.assertEqual([record[3] for record in delivered], [2.0])
            self.assertTrue(os.path.exists(path + ".sent"))

    def test_deleted_run_spool_is_retired_instead_of_retried(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "deleted.mkspool")
            self._write(path, [numeric(1.0)])
            stub = mock.Mock()
            stub.IngestMetrics.side_effect = _RunRejectedRpcError()

            with mock.patch.object(
                sync_module, "_connect", return_value=(_Channel(), stub)
            ):
                self.assertFalse(sync_module.replay_file(path))

            self.assertFalse(os.path.exists(path))
            self.assertTrue(os.path.exists(path + ".deleted"))
            stub.IngestMetrics.assert_called_once()

    def test_terminal_rejection_retires_every_later_spool_for_the_run(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            first = os.path.join(spool_dir, "first.mkspool")
            second = os.path.join(spool_dir, "second.mkspool")
            self._write(first, [numeric(1.0)], created=1)
            self._write(second, [numeric(2.0)], created=2)
            stub = mock.Mock()
            stub.IngestMetrics.side_effect = _RunRejectedRpcError()

            with mock.patch.object(
                sync_module, "_connect", return_value=(_Channel(), stub)
            ):
                self.assertEqual(sync_module.main([first, second]), 1)

            self.assertTrue(os.path.exists(first + ".deleted"))
            self.assertTrue(os.path.exists(second + ".deleted"))
            stub.IngestMetrics.assert_called_once()

    def test_replay_windows_are_byte_bounded_and_stop_after_failure(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "bounded.mkspool")
            self._write(path, [numeric(float(i), step=i) for i in range(8)])

            with (
                mock.patch.object(
                    sync_module, "_connect", return_value=(_Channel(), object())
                ),
                mock.patch.object(sync_module, "_BATCH_BYTES", 250),
                mock.patch.object(
                    sync_module, "_send_tuples", side_effect=[True, False]
                ) as send,
            ):
                self.assertFalse(sync_module.replay_file(path))

            self.assertEqual(send.call_count, 2)
            self.assertTrue(os.path.exists(path))

    def test_rich_replay_recognizes_an_exact_unknown_commit_then_advances(self):
        old = ("metadata_json", "info/run_info", 0, {"version": 1})
        new = ("metadata_json", "info/run_info", 0, {"version": 2})

        def upload(_client, _url, _data, _ext, *, expected_id=None):
            self.assertIsNotNone(expected_id)
            return expected_id

        old_manifest = {"v": 1, "class": "metadata", "data": old[3]}
        new_manifest = {"v": 1, "class": "metadata", "data": new[3]}
        new_id = (
            hashlib.sha256(
                json.dumps(new_manifest, indent=2, default=str).encode("utf-8")
            ).hexdigest()
            + ".json"
        )
        state = {
            "current": (
                hashlib.sha256(
                    json.dumps(old_manifest, indent=2, default=str).encode("utf-8")
                ).hexdigest()
                + ".json"
            )
        }
        writes = []

        class Stub:
            @staticmethod
            def IngestMetrics(batches, *, timeout=None):
                self.assertEqual(timeout, client_module._CDN_RPC_TIMEOUT)
                point = next(iter(batches)).points[0]
                state["current"] = point.cdn_key
                writes.append(point.cdn_key)
                return sync_module.kymo_pb2.IngestResponse(points_received=1)

        replayed = {}
        with (
            mock.patch.object(sync_module, "_upload_to_cdn", side_effect=upload),
            mock.patch.object(
                sync_module,
                "_current_cdn_key",
                side_effect=lambda *_args: state["current"],
            ),
        ):
            self.assertTrue(
                sync_module._replay_cdn_record(
                    Stub(), object(), "unused", "project", "run", old, replayed
                )
            )
            self.assertEqual(writes, [])
            self.assertTrue(
                sync_module._replay_cdn_record(
                    Stub(), object(), "unused", "project", "run", new, replayed
                )
            )

        self.assertEqual(writes, [new_id])
        self.assertEqual(state["current"], new_id)
        self.assertEqual(replayed[("project", "run", "info/run_info", 0)], new_id)

    def test_versioned_rich_replay_uses_server_cas_without_recency_probe(self):
        version = (9 << 32) | 3
        metadata = {"version": 2}
        record = (
            "metadata_json_mutation",
            "info/run_info",
            0,
            metadata,
            1_700_000_000_000,
            version,
        )
        # Live metadata publication uses the pretty form. Replay after an
        # ambiguous ACK must reproduce its exact content id or the same logical
        # version would correctly conflict with the resource already in PG.
        expected_id = sync_module.content_id(
            sync_module.metadata_manifest(metadata), "json"
        )
        stub = mock.Mock()
        stub.PublishRichMutation.return_value = (
            sync_module.kymo_pb2.PublishRichMutationResponse(
                disposition=sync_module.kymo_pb2.RICH_MUTATION_ACCEPTED,
                stored_version=version,
            )
        )
        with (
            mock.patch.object(
                sync_module,
                "_upload_to_cdn",
                side_effect=lambda *_args, expected_id=None, **_kwargs: expected_id,
            ),
            mock.patch.object(
                sync_module,
                "_current_cdn_key",
                side_effect=AssertionError(
                    "versioned replay used the legacy recency probe"
                ),
            ),
        ):
            self.assertTrue(
                sync_module._replay_cdn_record(
                    stub, object(), "unused", "project", "run", record, {}
                )
            )

        request = stub.PublishRichMutation.call_args.args[0]
        self.assertEqual(request.mutation_version, version)
        self.assertEqual(request.timestamp_ms, record[4])
        self.assertEqual(request.cdn_key, expected_id)
        stub.IngestMetrics.assert_not_called()

    def test_rich_replay_rechecks_recency_after_manifest_upload(self):
        record = ("metadata_json", "info/run_info", 0, {"version": 2})
        stub = mock.Mock()
        with (
            mock.patch.object(
                sync_module,
                "_current_cdn_key",
                side_effect=["", "newer.json"],
            ) as current,
            mock.patch.object(
                sync_module,
                "_upload_to_cdn",
                side_effect=lambda *_args, expected_id=None, **_kwargs: expected_id,
            ) as upload,
        ):
            self.assertTrue(
                sync_module._replay_cdn_record(
                    stub, object(), "unused", "project", "run", record, {}
                )
            )

        self.assertEqual(current.call_count, 2)
        upload.assert_called_once()
        stub.IngestMetrics.assert_not_called()

    def test_rich_recency_query_has_an_rpc_timeout(self):
        response = mock.Mock()
        response.series = []
        stub = mock.Mock()
        stub.QueryCdnKeys.return_value = response

        self.assertEqual(
            sync_module._current_cdn_key(stub, "project", "run", "metric", 1), ""
        )
        self.assertEqual(
            stub.QueryCdnKeys.call_args.kwargs["timeout"],
            client_module._CDN_RPC_TIMEOUT,
        )

    def test_rich_replay_does_not_accept_a_short_server_response(self):
        record = ("metadata_json", "info/run_info", 0, {"version": 2})
        stub = mock.Mock()
        stub.IngestMetrics.return_value = sync_module.kymo_pb2.IngestResponse(
            points_received=0
        )
        with (
            mock.patch.object(sync_module, "_current_cdn_key", return_value=""),
            mock.patch.object(
                sync_module,
                "_upload_to_cdn",
                side_effect=lambda *_args, expected_id=None, **_kwargs: expected_id,
            ),
        ):
            self.assertFalse(
                sync_module._replay_cdn_record(
                    stub, object(), "unused", "project", "run", record, {}
                )
            )

    def test_superseded_rich_replay_does_not_upload_content(self):
        entries = [
            {
                "kind": "resource",
                "data": f"payload-{index}".encode(),
                "ext": "bin",
                "content_type": "application/octet-stream",
                "filename": f"item-{index}.bin",
            }
            for index in range(100)
        ]
        record = ("cdn_batch_encoded", "gallery", 7, entries)
        upload = mock.Mock()
        stub = mock.Mock()

        with (
            mock.patch.object(sync_module, "_upload_to_cdn", upload),
            mock.patch.object(
                sync_module, "_current_cdn_key", return_value="newer.json"
            ),
        ):
            self.assertTrue(
                sync_module._replay_cdn_record(
                    stub, object(), "unused", "project", "run", record, {}
                )
            )

        upload.assert_not_called()
        stub.IngestMetrics.assert_not_called()

    def test_directory_expansion_orders_worker_before_salvage(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            # A user id containing "__worker_" must not make the salvage file
            # look like the worker phase.
            salvage = os.path.join(
                spool_dir, "p__worker_name__r__x__salvage_1_1.mkspool"
            )
            worker = os.path.join(spool_dir, "p__worker_name__r__x__worker_1_1.mkspool")
            self._write(salvage, [numeric(2.0)], created=1)
            self._write(worker, [numeric(1.0)], created=1)

            for args in ([], [spool_dir]):
                seen = []
                with (
                    self.subTest(args=args),
                    mock.patch.object(
                        sync_module, "default_replay_dirs", return_value=[spool_dir]
                    ),
                    mock.patch.object(
                        sync_module,
                        "replay_file",
                        side_effect=lambda path, **_kwargs: seen.append(path) or True,
                    ),
                ):
                    self.assertEqual(sync_module.main(args), 0)

            self.assertEqual(seen, [worker, salvage])

    def test_directory_expansion_treats_the_directory_as_a_literal_path(self):
        with tempfile.TemporaryDirectory() as root:
            literal_dir = os.path.join(root, "spool[1]")
            sibling_dir = os.path.join(root, "spool1")
            os.mkdir(literal_dir)
            os.mkdir(sibling_dir)
            literal = os.path.join(literal_dir, "literal.mkspool")
            sibling = os.path.join(sibling_dir, "sibling.mkspool")
            self._write(literal, [numeric(1.0)], created=1)
            self._write(sibling, [numeric(2.0)], created=2)

            seen = []
            with mock.patch.object(
                sync_module,
                "replay_file",
                side_effect=lambda path, **_kwargs: seen.append(path) or True,
            ):
                self.assertEqual(sync_module.main([literal_dir]), 0)

            self.assertEqual(seen, [literal])

    def test_session_name_segment_preserves_worker_before_salvage_order(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            salvage = make_spool_path(
                "project", "run", "salvage", spool_dir, session="session-a"
            )
            worker = make_spool_path(
                "project", "run", "worker", spool_dir, session="session-a"
            )
            self._write(salvage, [numeric(2.0)], created=1)
            self._write(worker, [numeric(1.0)], created=1)

            seen = []
            with mock.patch.object(
                sync_module,
                "replay_file",
                side_effect=lambda path, **_kwargs: seen.append(path) or True,
            ):
                self.assertEqual(sync_module.main([spool_dir]), 0)

            self.assertEqual(seen, [worker, salvage])

    def test_causal_sort_spans_multiple_directories(self):
        with (
            tempfile.TemporaryDirectory() as first_dir,
            tempfile.TemporaryDirectory() as second_dir,
        ):
            newer = os.path.join(first_dir, "x__salvage_1_2.mkspool")
            older = os.path.join(second_dir, "x__worker_1_1.mkspool")
            self._write(newer, [numeric(2.0)], created=2)
            self._write(older, [numeric(1.0)], created=1)

            seen = []
            with mock.patch.object(
                sync_module,
                "replay_file",
                side_effect=lambda path, **_kwargs: seen.append(path) or True,
            ):
                self.assertEqual(sync_module.main([first_dir, second_dir]), 0)

            self.assertEqual(seen, [older, newer])

    def test_no_arg_sync_scans_both_default_directories_in_causal_order(self):
        with (
            tempfile.TemporaryDirectory() as current_dir,
            tempfile.TemporaryDirectory() as legacy_dir,
        ):
            newer = os.path.join(current_dir, "x__salvage_1_2.mkspool")
            older = os.path.join(legacy_dir, "x__worker_1_1.mkspool")
            self._write(newer, [numeric(2.0)], created=2)
            self._write_legacy(older, [numeric(1.0)], created=1)

            seen = []
            with (
                mock.patch.object(
                    sync_module,
                    "default_replay_dirs",
                    return_value=[current_dir, legacy_dir],
                ),
                mock.patch.object(
                    sync_module,
                    "replay_file",
                    side_effect=lambda path, **_kwargs: seen.append(path) or True,
                ),
            ):
                self.assertEqual(sync_module.main([]), 0)

            self.assertEqual(seen, [older, newer])

    def test_no_arg_sync_skips_missing_defaults_but_explicit_missing_fails(self):
        with tempfile.TemporaryDirectory() as root:
            missing_current = os.path.join(root, "current")
            missing_legacy = os.path.join(root, "legacy")
            with mock.patch.object(
                sync_module,
                "default_replay_dirs",
                return_value=[missing_current, missing_legacy],
            ):
                self.assertEqual(sync_module.main([]), 0)
            self.assertEqual(sync_module.main([missing_current]), 1)

    def test_unreadable_default_directory_is_not_reported_empty(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            os.chmod(spool_dir, 0)
            try:
                with mock.patch.object(
                    sync_module, "default_replay_dirs", return_value=[spool_dir]
                ):
                    self.assertEqual(sync_module.main([]), 1)
            finally:
                os.chmod(spool_dir, 0o700)

    def test_explicit_sync_inputs_do_not_mask_a_legacy_environment_name(self):
        with (
            tempfile.TemporaryDirectory() as spool_dir,
            mock.patch.dict(os.environ, {"MKDB2_SERVER": "legacy:50051"}),
            mock.patch.object(sync_module, "replay_file") as replay,
            self.assertRaisesRegex(ValueError, "MKDB2_SERVER.*KYMO_SERVER"),
        ):
            sync_module.main([spool_dir, "--server", "canonical:50051"])
        replay.assert_not_called()

    def test_directory_candidate_changed_before_replay_is_not_followed(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "candidate.mkspool")
            self._write(path, [numeric(1.0)])
            with (
                mock.patch.object(
                    sync_module,
                    "_directory_candidate_unchanged",
                    side_effect=[True, False],
                ),
                mock.patch.object(sync_module, "replay_file") as replay,
            ):
                self.assertEqual(sync_module.main([spool_dir]), 1)
            replay.assert_not_called()

    def test_changed_earlier_directory_candidate_blocks_the_remaining_suffix(self):
        with (
            tempfile.TemporaryDirectory() as first_dir,
            tempfile.TemporaryDirectory() as second_dir,
        ):
            first = os.path.join(first_dir, "x__worker_1_1.mkspool")
            second = os.path.join(second_dir, "x__salvage_1_2.mkspool")
            self._write(first, [numeric(1.0)], created=1)
            self._write(second, [numeric(2.0)], created=2)
            with (
                mock.patch.object(
                    sync_module,
                    "_directory_candidate_unchanged",
                    side_effect=[True, True, False],
                ),
                mock.patch.object(sync_module, "replay_file") as replay,
            ):
                self.assertEqual(sync_module.main([first_dir, second_dir]), 1)
            replay.assert_not_called()

    def test_overlapping_directory_and_file_inputs_replay_once(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "one.mkspool")
            alternate_path = os.path.join(spool_dir, ".", "one.mkspool")
            self._write(path, [numeric(1.0)], created=1)

            seen = []

            def replay(item, **_kwargs):
                seen.append(item)
                os.rename(item, item + ".sent")
                return True

            with mock.patch.object(
                sync_module,
                "replay_file",
                side_effect=replay,
            ):
                self.assertEqual(sync_module.main([spool_dir, alternate_path]), 0)

            self.assertEqual(seen, [path])
            self.assertTrue(os.path.exists(path + ".sent"))

    def test_symlink_input_retires_the_target_instead_of_replaying_it_later(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "one.mkspool")
            alias = os.path.join(spool_dir, "alias.mkspool")
            self._write(path, [numeric(1.0)], created=1)
            try:
                os.symlink(path, alias)
            except OSError as error:
                self.skipTest(f"symlinks unavailable: {error}")

            seen = []

            def replay(item, **_kwargs):
                seen.append(item)
                os.rename(item, item + ".sent")
                return True

            with mock.patch.object(sync_module, "replay_file", side_effect=replay):
                # Alias first is the old failure case: it used to retire only the link, leaving the still-live target for this directory input. The directory scan must also ignore its leaf alias.
                self.assertEqual(sync_module.main([alias, spool_dir]), 0)
                # The dangling alias left after target retirement must not make every later directory scan fail.
                self.assertEqual(sync_module.main([spool_dir]), 0)

            self.assertEqual(seen, [path])
            self.assertTrue(os.path.exists(path + ".sent"))
            self.assertFalse(os.path.exists(path))

    def test_hardlinked_spool_aliases_abort_before_any_replay(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "one.mkspool")
            alias = os.path.join(spool_dir, "alias.mkspool")
            self._write(path, [numeric(1.0)], created=1)
            try:
                os.link(path, alias)
            except OSError as error:
                self.skipTest(f"hardlinks unavailable: {error}")

            seen = []

            def replay(item, **_kwargs):
                seen.append(item)
                os.rename(item, item + ".sent")
                return True

            with mock.patch.object(sync_module, "replay_file", side_effect=replay):
                # Retiring one name would leave the other live, so a later run would replay the stale spool over newer values. Refuse before sending anything.
                self.assertEqual(sync_module.main([spool_dir]), 1)

            self.assertEqual(seen, [])
            self.assertTrue(os.path.exists(path))
            self.assertTrue(os.path.exists(alias))

    def test_directory_replay_stops_a_runs_suffix_after_failure(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            first = os.path.join(spool_dir, "p__r__x__worker_1_1.mkspool")
            second = os.path.join(spool_dir, "p__r__x__salvage_1_2.mkspool")
            self._write(first, [numeric(1.0)], created=1)
            self._write(second, [numeric(2.0)], created=2)

            seen = []

            def replay(path, **_kwargs):
                seen.append(path)
                return False

            with mock.patch.object(sync_module, "replay_file", side_effect=replay):
                self.assertEqual(sync_module.main([spool_dir]), 1)

            self.assertEqual(seen, [first])

    def test_spool_paths_do_not_alias_rapid_reinitializations(self):
        with (
            tempfile.TemporaryDirectory() as spool_dir,
            mock.patch("kymo.spool.time.time_ns", side_effect=[100, 101]),
        ):
            first = make_spool_path("project", "run", "worker", spool_dir)
            second = make_spool_path("project", "run", "worker", spool_dir)

        self.assertNotEqual(first, second)

    def test_session_spool_name_is_scoped_without_changing_legacy_names(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            scoped = make_spool_path(
                "project", "run", "worker", spool_dir, session="session-a"
            )
            legacy = make_spool_path("project", "run", "worker", spool_dir)

        scoped_name = os.path.basename(scoped)
        self.assertTrue(
            scoped_name.startswith(spool_name_prefix("project", "run", "session-a"))
        )
        self.assertTrue(
            os.path.basename(legacy).startswith(spool_name_prefix("project", "run"))
        )
        self.assertNotIn("session-a", os.path.basename(legacy))

    def test_spool_path_is_linux_name_max_safe_for_multibyte_ids(self):
        project_id = "界" * 5 + "a" * 43
        run_id = "界" * 48
        with tempfile.TemporaryDirectory() as spool_dir:
            path = make_spool_path(
                project_id, run_id, "worker", spool_dir, session="f" * 32
            )
            self.assertLessEqual(len(os.path.basename(path).encode("utf-8")), 255)
            writer = SpoolWriter(path, spool_header())
            writer.write(numeric(1.0))
            writer.close()
            self.assertTrue(os.path.exists(path))

    def test_explicit_files_are_causally_sorted(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            older = os.path.join(spool_dir, "older.mkspool")
            newer = os.path.join(spool_dir, "newer.mkspool")
            self._write(older, [numeric(1.0)], created=1)
            self._write(newer, [numeric(2.0)], created=2)

            seen = []
            with mock.patch.object(
                sync_module,
                "replay_file",
                side_effect=lambda path, **_kwargs: seen.append(path) or True,
            ):
                self.assertEqual(sync_module.main([newer, older]), 0)

            self.assertEqual(seen, [older, newer])

    def test_replay_state_is_scoped_to_the_destination_server(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            first = os.path.join(spool_dir, "first.mkspool")
            second = os.path.join(spool_dir, "second.mkspool")
            self._write(first, [numeric(1.0)], created=1, server="server-a:1")
            self._write(second, [numeric(2.0)], created=2, server="server-b:1")

            seen = []

            def replay(path, **kwargs):
                seen.append((path, kwargs["replayed_keys"]))
                return path == second

            with mock.patch.object(sync_module, "replay_file", side_effect=replay):
                self.assertEqual(sync_module.main([second, first]), 1)

            self.assertEqual([path for path, _state in seen], [first, second])
            self.assertIsNot(seen[0][1], seen[1][1])


if __name__ == "__main__":
    unittest.main()
