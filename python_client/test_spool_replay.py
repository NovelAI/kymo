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
from kymo import spool as spool_module
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
from kymo.types import Image, Metadata
from PIL import Image as PILImage


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

    def test_a_gallery_the_encoder_reduced_spools_under_its_reserved_version(self):
        version = (3 << 32) | 7
        gallery = [Image(object()), client_module.Resource(b"x", "item.bin")]
        point = ("cdn_batch_mutation", "gallery", 4, gallery, 1, version, version + 1)
        with self.assertLogs("kymo", level="WARNING"):
            record = client_module._spool_record(point)
        # The reduced gallery is a different mutation, so it takes the reserved successor, which leaves no reserve.
        self.assertEqual(record[0], "cdn_batch_encoded_mutation")
        self.assertEqual(record[5], version + 1)
        self.assertEqual(len(record[3]), 1)

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
                    client_module._encode_cdn_items(
                        "gallery",
                        7,
                        [client_module.Resource(b"payload", "payload.bin")],
                    ),
                    send_placeholder=False,
                )
            )

    def test_a_live_gallery_that_lost_an_item_publishes_its_reduced_version(self):
        encoded = client_module._encode_cdn_items(
            "gallery", 7, [client_module.Resource(b"payload", "payload.bin")]
        )
        with (
            mock.patch.object(
                client_module,
                "_upload_to_cdn",
                side_effect=["payload.bin", "manifest.json"],
            ),
            mock.patch.object(
                client_module, "_publish_rich_mutation", return_value=True
            ) as publish,
        ):
            self.assertTrue(
                client_module._process_cdn_batch(
                    mock.Mock(),
                    object(),
                    "unused",
                    "project",
                    "run",
                    "gallery",
                    7,
                    client_module._EncodedItems(encoded.entries, dropped=True),
                    send_placeholder=False,
                    timestamp_ms=1,
                    mutation_version=10,
                    reduced_mutation_version=11,
                )
            )
        self.assertEqual(publish.call_args.args[-1], 11)

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

    @contextlib.contextmanager
    def _lane_worker(
        self,
        spool_dir: str,
        env: dict,
        upload=None,
        encode=None,
        spill=None,
        expected_failure=0,
        replay=None,
        retry_delay=None,
    ):
        """Run the worker over mocked transports until the block exits, then check that it drained with expected_failure as its failure code; yields the lane namespace below, whose put() queues a group of points.

        Without a replay stand-in for kymo.sync.replay_file, a failed-over lane retries its disk catch-up only after an hour, so the spool stays as written (and the block fails if a replay ran); retry_delay(failures, rng) replaces the recovery backoff."""
        source = queue.Queue()
        lane = types.SimpleNamespace(
            path=os.path.join(spool_dir, "lane.mkspool"),
            first_spill=threading.Event(),
            status=_Status(),
            spooled=_Status(),
            failure=_Status(),
            released=_Status(),
            on_disk=_Status(),
            on_disk_at_first_spill=None,
        )
        real_spill = spill or client_module._spill_tuple

        def put(item):
            with lane.status.get_lock():
                lane.status.value += client_module._queue_item_size(item)
            source.put(item)

        def observe_spill(spool, point):
            if lane.on_disk_at_first_spill is None:
                lane.on_disk_at_first_spill = lane.on_disk.value
            result = real_spill(spool, point)
            lane.first_spill.set()
            return result

        lane.put = put
        lane.shut_down = lambda: source.put(None)
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
            mock.patch.object(client_module, "_upload_to_cdn", side_effect=upload),
            mock.patch.object(client_module, "_send_unary_point", return_value=True),
            mock.patch.object(
                client_module,
                "_encode_image",
                side_effect=encode or client_module._encode_image,
            ) as lane.encode,
            mock.patch.object(client_module, "_spill_tuple", side_effect=observe_spill),
            mock.patch.object(client_module, "_cdn_retry_delay", return_value=0.0),
            mock.patch.object(
                client_module,
                "_recovery_retry_delay",
                side_effect=retry_delay
                or (lambda failures, rng: 0.0 if replay else 3600.0),
            ),
            mock.patch("kymo.sync.replay_file", side_effect=replay) as replay_file,
            mock.patch.object(
                client_module, "_publish_rich_mutation", return_value=True
            ) as lane.publish,
            mock.patch.dict(os.environ, env),
            mock.patch.object(client_module, "_IDLE_POLL_TIMEOUT", 0.001),
        ):
            worker = threading.Thread(
                target=client_module._upload_worker,
                args=("unused", "project", "run", source, lane.status),
                kwargs={
                    "spool_path": lane.path,
                    "upload_spooled": lane.spooled,
                    "upload_failure": lane.failure,
                    "galleries_released": lane.released,
                    "lanes_on_disk": lane.on_disk,
                    # Without an upload there is no CDN, so only the caps can move rich entries.
                    "cdn_url": "http://unused" if upload else "",
                },
                # A worker stuck by a regression fails the join assertion instead of hanging the suite.
                daemon=True,
            )
            worker.start()
            try:
                yield lane
            finally:
                source.put(None)
                worker.join(timeout=5)
        self.assertFalse(worker.is_alive())
        if replay is None:
            replay_file.assert_not_called()
        self.assertEqual(lane.status.value, 0)
        # A zero backlog alone also holds when the worker reported points lost.
        self.assertEqual(lane.failure.value, expected_failure)

    @staticmethod
    def _gallery(step: int, gallery: list) -> tuple:
        """A gallery as an old server's writer queues it, without logical versions."""
        (item,) = client_module._snapshot_rich_queue_items(
            [("cdn_batch", "samples", step, gallery)]
        )
        return item

    @staticmethod
    def _versioned_gallery(step: int, gallery: list) -> tuple:
        """A gallery as a current server's writer queues it; only versioned rich records replay while the lane goes live again."""
        version = (1 << 32) | (2 * step - 1)
        (item,) = client_module._snapshot_rich_queue_items(
            [
                (
                    "cdn_batch_mutation",
                    "samples",
                    step,
                    gallery,
                    step,
                    version,
                    version + 1,
                )
            ]
        )
        return item

    @staticmethod
    def _metadata(value: int) -> tuple:
        return ("metadata_batch", "info/run_info", 0, Metadata({"v": value}))

    @staticmethod
    def _upload_recorder(uploads: list):
        def upload(http_client, cdn_url, data, ext):
            uploads.append(ext)
            return content_id(data, ext)

        return upload

    def test_numeric_byte_cap_spools_whole_lane_and_future_points_in_order(self):
        env = {"KYMO_MAX_BUFFER_POINTS": "1000", "KYMO_MAX_BUFFER_BYTES": "1"}
        with tempfile.TemporaryDirectory() as spool_dir:
            with self._lane_worker(spool_dir, env) as lane:
                lane.put([numeric(1.0), numeric(2.0)])
                self.assertTrue(lane.first_spill.wait(timeout=1))
                lane.put([numeric(3.0)])
            _, records = read_spool(lane.path)
            self.assertEqual([record[3] for record in records], [1.0, 2.0, 3.0])
        # log() stops pacing galleries before the first write: disk, not the encoder, would set the pace.
        self.assertEqual(lane.on_disk_at_first_spill, 1)
        self.assertEqual(lane.spooled.value, 1)
        self.assert_wait_reports_spooled(lane.status, lane.spooled)

    def test_a_failed_spill_keeps_the_records_already_flushed_replayable(self):
        env = {"KYMO_MAX_RICH_BUFFER_BYTES": "1"}
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                _failing_spool_writes() as failing,
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(
                    spool_dir,
                    env,
                    expected_failure=client_module._WORKER_EXIT_SPOOL_FAILED,
                ) as lane,
            ):
                lane.put([self._metadata(1), self._metadata(2)])
                # The two entries spill separately: wait until both are on disk.
                deadline = time.monotonic() + 5
                while lane.status.value and time.monotonic() < deadline:
                    time.sleep(0.005)
                self.assertEqual(lane.status.value, 0)
                # The disk fills: the next spill's flush tears its record.
                failing.set()
                lane.put([self._metadata(3)])
            _, records = read_spool(lane.path)
            self.assertEqual([record[3]["v"] for record in records], [1, 2])

    @staticmethod
    def _wait_for(condition, timeout=5.0):
        deadline = time.monotonic() + timeout
        while not condition() and time.monotonic() < deadline:
            time.sleep(0.005)
        return condition()

    # A rich byte cap that _overflowing() exceeds on arrival, before any encode, and that an encoded _small() gallery fits under.
    _RICH_CAP_1K = {"KYMO_MAX_RICH_BUFFER_BYTES": "1024"}
    _OVERFLOW = [client_module.Resource(b"x" * 2048, "overflow.bin")]

    @classmethod
    def _overflowing(cls, step: int) -> tuple:
        return cls._versioned_gallery(step, cls._OVERFLOW)

    @classmethod
    def _small(cls, step: int) -> tuple:
        return cls._versioned_gallery(step, [Image(PILImage.new("RGB", (8, 8)))])

    @staticmethod
    def _held_replay():
        """A kymo.sync.replay_file stand-in that records each segment's steps, once release is set."""
        held = types.SimpleNamespace(
            replaying=threading.Event(), release=threading.Event(), replayed=[]
        )

        def replay(path, **kwargs):
            held.replaying.set()
            held.release.wait(timeout=5)
            _, records = read_spool(path)
            held.replayed.extend(record[2] for record in records)
            return True

        held.replay = replay
        return held

    def test_rich_lane_replays_its_spool_and_goes_live_again(self):
        held = self._held_replay()
        held.release.set()
        uploads = []
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(
                    spool_dir,
                    self._RICH_CAP_1K,
                    self._upload_recorder(uploads),
                    replay=held.replay,
                ) as lane,
            ):
                lane.put(self._overflowing(1))
                self.assertTrue(self._wait_for(lambda: held.replayed == [1]))
                # The replay emptied the disk prefix, so nothing is reported spooled any more.
                self.assertTrue(self._wait_for(lambda: lane.spooled.value == 0))
                lane.put(self._small(2))
                self.assertTrue(self._wait_for(lambda: "json" in uploads))
        self.assertEqual(held.replayed, [1])
        self.assertEqual(uploads, ["png", "json"])

    def test_worker_publishes_galleries_no_longer_raw_and_lanes_on_disk(self):
        held = self._held_replay()
        held.release.set()
        uploads = []
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(
                    spool_dir,
                    self._RICH_CAP_1K,
                    self._upload_recorder(uploads),
                    replay=held.replay,
                    expected_failure=client_module._WORKER_EXIT_SPOOL_FAILED,
                ) as lane,
            ):
                lane.put(self._small(1))
                # Delivered, so the failover below finds it gone from the lane.
                self.assertTrue(self._wait_for(lambda: lane.status.value == 0))
                # Encoded by the encoder helper.
                self.assertEqual(lane.released.value, 1)
                self.assertEqual(lane.on_disk.value, 0)
                lane.put(self._overflowing(2))
                # Encoded and written by the spill; the flag rose before that write.
                self.assertTrue(self._wait_for(lambda: lane.released.value == 2))
                self.assertEqual(lane.on_disk_at_first_spill, 1)
                # The replay empties the disk, so both lanes are live again.
                self.assertTrue(self._wait_for(lambda: held.replayed == [2]))
                self.assertTrue(self._wait_for(lambda: lane.on_disk.value == 0))
                lane.put(
                    (client_module._SERIALIZED_RICH_QUEUE_ITEM, 1, b"not a pickle")
                )
                self.assertTrue(self._wait_for(lambda: lane.released.value == 3))

    def test_the_encoder_publishes_a_gallery_before_the_loop_wakes(self):
        uploads = []
        with (
            tempfile.TemporaryDirectory() as spool_dir,
            # The loop sleeps through the encode: nothing else arrives to wake it.
            mock.patch.object(client_module, "_ACTIVE_POLL_TIMEOUT", 1.0),
            self._lane_worker(
                spool_dir, self._RICH_CAP_1K, self._upload_recorder(uploads)
            ) as lane,
        ):
            lane.put(self._small(1))
            self.assertTrue(self._wait_for(lambda: lane.released.value == 1, 0.5))

    def test_failover_counts_a_gallery_encoding_in_flight_once(self):
        encoding = threading.Event()
        release = threading.Event()

        def encode(image):
            encoding.set()
            release.wait(timeout=5)
            return client_module._encode_image(image)

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(spool_dir, self._RICH_CAP_1K, encode=encode) as lane,
            ):
                lane.put(self._small(1))
                self.assertTrue(encoding.wait(timeout=5))
                # Its failover waits for the encode in flight, then spills both.
                lane.put(self._overflowing(2))
                release.set()
                self.assertTrue(self._wait_for(lambda: lane.status.value == 0))
            _, records = read_spool(lane.path)
        self.assertEqual([record[2] for record in records], [1, 2])
        self.assertEqual(lane.released.value, 2)

    def test_failover_does_not_count_an_encoded_gallery_again(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(spool_dir, self._RICH_CAP_1K) as lane,
            ):
                # No CDN, so the encoded gallery stays queued until the failover spills it.
                lane.put(self._small(1))
                self.assertTrue(self._wait_for(lambda: lane.released.value == 1))
                lane.put(self._overflowing(2))
                self.assertTrue(self._wait_for(lambda: lane.status.value == 0))
        self.assertEqual(lane.released.value, 2)

    def test_sustained_logging_returns_a_recovered_rich_lane_to_live(self):
        replayed = []

        def replay(path, **kwargs):
            # Slower than galleries arrive, so a record always waits behind each replay.
            time.sleep(0.02)
            _, records = read_spool(path)
            replayed.extend(record[2] for record in records)
            return True

        producing = threading.Event()
        producing.set()
        env = {"KYMO_MAX_RICH_BUFFER_BYTES": str(1 << 20)}
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(
                    spool_dir, env, self._upload_recorder([]), replay=replay
                ) as lane,
            ):

                def log_galleries():
                    step = 2
                    while producing.is_set():
                        lane.put(self._small(step))
                        step += 1
                        time.sleep(0.002)
                    lane.last_step = step - 1

                big = [client_module.Resource(b"x" * (2 << 20), "big.bin")]
                lane.put(self._versioned_gallery(1, big))
                producer = threading.Thread(target=log_galleries, daemon=True)
                producer.start()
                try:
                    # Live again while galleries keep arriving, and pacing with it.
                    self.assertTrue(self._wait_for(lambda: lane.publish.called, 3.0))
                    self.assertEqual(lane.on_disk.value, 0)
                finally:
                    producing.clear()
                    producer.join(timeout=1)
        published = [call.args[4] for call in lane.publish.call_args_list]
        # Every gallery arrived once, and nothing published live overtook a record on disk.
        self.assertEqual(
            sorted(replayed + published), list(range(1, lane.last_step + 1))
        )
        self.assertLess(max(replayed), min(published))

    def test_a_failed_replay_while_draining_moves_the_rich_work_back_to_disk(self):
        gates = [threading.Event() for _ in range(3)]
        outcomes = iter([True, False, True])

        def replay(path, **kwargs):
            # The first replay proves the circuit, the second fails, and any later one waits for the test's end.
            gate = gates[min(len(replay.calls), 2)]
            replay.calls.append(path)
            gate.wait(timeout=5)
            return next(outcomes, True)

        replay.calls = []
        failures = []

        def retry_delay(failed, rng):
            failures.append(failed)
            return 0.0

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(
                    spool_dir,
                    self._RICH_CAP_1K,
                    self._upload_recorder([]),
                    replay=replay,
                    retry_delay=retry_delay,
                ) as lane,
            ):
                try:
                    lane.put(self._overflowing(1))
                    self.assertTrue(self._wait_for(lambda: len(replay.calls) == 1))
                    # Spooled behind the first replay, then sealed for the second.
                    lane.put(self._small(2))
                    self.assertTrue(self._wait_for(lambda: lane.status.value == 0))
                    gates[0].set()
                    self.assertTrue(self._wait_for(lambda: lane.on_disk.value == 0))
                    # Draining: held in RAM, encoded but not uploaded.
                    lane.put(self._small(3))
                    self.assertTrue(self._wait_for(lambda: lane.released.value == 3))
                    self.assertEqual(lane.status.value, 1)
                    gates[1].set()
                    self.assertTrue(self._wait_for(lambda: lane.status.value == 0))
                    self.assertEqual(lane.on_disk.value, 1)
                    self.assertFalse(lane.publish.called)
                    # The first failover, then the failed replay; leaving the drain schedules no extra retry.
                    self.assertEqual(failures, [1, 1])
                finally:
                    gates[2].set()

    def test_a_live_metadata_conflict_is_data_loss_not_a_retry(self):
        version = (1 << 32) | 1
        (item,) = client_module._snapshot_rich_queue_items(
            [
                (
                    "metadata_batch_mutation",
                    "info/run_info",
                    0,
                    Metadata({"v": 1}),
                    1,
                    version,
                )
            ]
        )
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="ERROR"),
                self._lane_worker(
                    spool_dir,
                    {},
                    self._upload_recorder([]),
                    expected_failure=client_module._WORKER_EXIT_DATA_LOSS,
                ) as lane,
            ):
                lane.publish.side_effect = client_module._RichMutationDataLoss(
                    "different content"
                )
                lane.put(item)
                self.assertTrue(self._wait_for(lambda: lane.failure.value != 0))
        # The server's conflict answer is final: the entry goes to the spool for replay to quarantine, not back to the publish.
        self.assertEqual(lane.publish.call_count, 1)

    def test_a_failover_after_the_lane_went_live_needs_a_new_proof_to_drain(self):
        gates = [threading.Event(), threading.Event()]
        calls = []

        def replay(path, quarantined_files, **kwargs):
            gate = gates[min(len(calls), 1)]
            calls.append(path)
            gate.wait(timeout=5)
            return True

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(
                    spool_dir,
                    self._RICH_CAP_1K,
                    self._upload_recorder([]),
                    replay=replay,
                ) as lane,
            ):
                try:
                    lane.put(self._overflowing(1))
                    self.assertTrue(self._wait_for(lambda: len(calls) == 1))
                    gates[0].set()
                    # The delivered replay proves the circuit, and the empty prefix lets the lane go live.
                    self.assertTrue(self._wait_for(lambda: lane.on_disk.value == 0))
                    lane.put(self._overflowing(2))
                    self.assertTrue(self._wait_for(lambda: len(calls) == 2))
                    # The failover from live voided that proof, so the second replay does not drain the lane: its new work still goes to disk.
                    self.assertFalse(
                        self._wait_for(lambda: lane.on_disk.value == 0, timeout=0.3)
                    )
                finally:
                    gates[1].set()

    def test_shutdown_after_a_quarantine_ends_the_drain_goes_live(self):
        gates = [threading.Event(), threading.Event()]
        calls = []

        def replay(path, quarantined_files, **kwargs):
            gate = gates[min(len(calls), 1)]
            calls.append(path)
            gate.wait(timeout=5)
            if len(calls) == 2:
                # The server rejects the drained prefix's last segment for good.
                quarantined_files.add(path)
            return True

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(
                    spool_dir,
                    self._RICH_CAP_1K,
                    self._upload_recorder([]),
                    replay=replay,
                    expected_failure=client_module._WORKER_EXIT_DATA_LOSS,
                ) as lane,
            ):
                try:
                    lane.put(self._overflowing(1))
                    self.assertTrue(self._wait_for(lambda: len(calls) == 1))
                    lane.put(self._small(2))
                    self.assertTrue(self._wait_for(lambda: lane.status.value == 0))
                    gates[0].set()
                    self.assertTrue(self._wait_for(lambda: lane.on_disk.value == 0))
                    # Draining behind the second replay, which the server quarantines after shutdown begins.
                    lane.put(self._small(3))
                    self.assertTrue(self._wait_for(lambda: lane.released.value == 3))
                    lane.shut_down()
                finally:
                    gates[1].set()
        # The quarantine left no prefix, so shutdown went live and uploaded the drained gallery.
        self.assertEqual([call.args[4] for call in lane.publish.call_args_list], [3])

    def test_no_upload_starts_while_one_a_failover_abandoned_still_runs(self):
        released = threading.Event()
        uploads = []

        def upload(_http_client, _cdn_url, data, ext):
            uploads.append(ext)
            if len(uploads) == 1:
                # The first upload hangs, as against a stalled CDN, until the test releases it.
                released.wait(timeout=5)
            return content_id(data, ext)

        def replay(path, **kwargs):
            replay.calls += 1
            return True

        replay.calls = 0
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(
                    spool_dir, self._RICH_CAP_1K, upload, replay=replay
                ) as lane,
            ):
                try:
                    lane.put(self._small(1))
                    self.assertTrue(self._wait_for(lambda: uploads))
                    # The cap trips behind the hung upload: the failover abandons it, and the replay brings the lane back live.
                    lane.put(self._overflowing(2))
                    self.assertTrue(self._wait_for(lambda: replay.calls))
                    self.assertTrue(self._wait_for(lambda: lane.on_disk.value == 0))
                    lane.put(self._small(3))
                    time.sleep(0.2)
                    self.assertEqual(len(uploads), 1)
                finally:
                    released.set()
                # Once the abandoned upload ends, the live gallery uploads.
                published = lane.publish.call_args_list
                self.assertTrue(
                    self._wait_for(lambda: 3 in [call.args[4] for call in published])
                )

    def test_a_fork_childs_gallery_is_charged_whole_and_never_released(self):
        # A fork child of the initializing process snapshots these galleries.
        with mock.patch.object(client_module, "_init_pid", os.getpid() + 1):
            small = self._small(1)
            large = self._versioned_gallery(2, [Image(PILImage.new("RGB", (32, 32)))])
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(spool_dir, self._RICH_CAP_1K) as lane,
            ):
                lane.put(small)
                self.assertTrue(self._wait_for(lambda: lane.encode.called))
                # Its raw pixels sit in the worker until encoded, so its 3 KiB arrays trip the 1 KiB cap on arrival.
                lane.put(large)
                self.assertTrue(lane.first_spill.wait(timeout=2))
                self.assertTrue(self._wait_for(lambda: lane.status.value == 0))
        # The parent never counted them, so neither the encode nor the spill releases them.
        self.assertEqual(lane.released.value, 0)

    def test_rich_lane_recovers_after_a_cdn_outage(self):
        outage = threading.Event()
        outage.set()
        uploads = []
        record_upload = self._upload_recorder(uploads)

        def upload(*args):
            if outage.is_set():
                raise OSError("CDN unreachable")
            return record_upload(*args)

        replay_attempts = []
        replayed = []

        def replay(path, **kwargs):
            replay_attempts.append(path)
            if outage.is_set():
                return False
            _, records = read_spool(path)
            replayed.extend(record[2] for record in records)
            return True

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(
                    spool_dir, self._RICH_CAP_1K, upload, replay=replay
                ) as lane,
            ):
                # The upload gives up, the lane fails over, and replays keep failing while the outage lasts.
                lane.put(self._small(1))
                self.assertTrue(self._wait_for(lambda: len(replay_attempts) >= 2))
                self.assertEqual(replayed, [])
                outage.clear()
                self.assertTrue(self._wait_for(lambda: replayed == [1]))
                self.assertTrue(self._wait_for(lambda: lane.spooled.value == 0))
                lane.put(self._small(2))
                self.assertTrue(self._wait_for(lambda: "json" in uploads))
        self.assertEqual(replayed, [1])
        self.assertEqual(uploads, ["png", "json"])

    def test_rich_work_logged_during_a_replay_waits_behind_it_on_disk(self):
        held = self._held_replay()
        uploads = []
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(
                    spool_dir,
                    self._RICH_CAP_1K,
                    self._upload_recorder(uploads),
                    replay=held.replay,
                ) as lane,
            ):
                lane.put(self._overflowing(1))
                self.assertTrue(held.replaying.wait(timeout=5))
                # Logged while the first segment replays: it must reach the server after it, so it waits on disk too.
                lane.put(self._small(2))
                self.assertTrue(self._wait_for(lambda: lane.status.value == 0))
                held.release.set()
                self.assertTrue(self._wait_for(lambda: held.replayed == [1, 2]))
                self.assertTrue(self._wait_for(lambda: lane.spooled.value == 0))
                self.assertEqual(uploads, [])
                lane.put(self._small(3))
                self.assertTrue(self._wait_for(lambda: "json" in uploads))
        self.assertEqual(held.replayed, [1, 2])

    def test_shutdown_replays_the_rich_tail_behind_a_replay_that_succeeded(self):
        held = self._held_replay()
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(
                    spool_dir, self._RICH_CAP_1K, replay=held.replay
                ) as lane,
            ):
                lane.put(self._overflowing(1))
                self.assertTrue(held.replaying.wait(timeout=5))
                lane.put(self._small(2))
                self.assertTrue(self._wait_for(lambda: lane.status.value == 0))
                lane.shut_down()
                held.release.set()
        # The first replay proved the circuit, so shutdown delivers the record spooled behind it too.
        self.assertEqual(held.replayed, [1, 2])
        self.assertEqual(lane.spooled.value, 0)

    def test_shutdown_stops_retrying_a_failing_rich_replay(self):
        attempts = threading.Semaphore(0)

        def replay(path, **kwargs):
            attempts.release()
            return False

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(spool_dir, self._RICH_CAP_1K, replay=replay) as lane,
            ):
                lane.put(self._overflowing(1))
                self.assertTrue(attempts.acquire(timeout=5))
        # The harness's join proves the worker left instead of retrying an unproven circuit until a deadline it does not have.
        self.assertEqual(lane.spooled.value, 1)

    def test_an_unversioned_rich_lane_stays_on_disk(self):
        replay = mock.Mock(return_value=True)
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(spool_dir, self._RICH_CAP_1K, replay=replay) as lane,
            ):
                # An old server's records carry no logical versions, so a replay could skip one behind a predecessor delivered live and still report success.
                lane.put(self._gallery(1, self._OVERFLOW))
                self.assertTrue(self._wait_for(lambda: lane.spooled.value == 1))
                time.sleep(0.2)
        replay.assert_not_called()
        self.assertEqual(lane.spooled.value, 1)

    def test_a_failing_rich_seal_backs_off_the_rich_catch_up(self):
        def delay(failures, rng):
            # The failover's first catch-up runs at once; any later retry waits.
            return 0.0 if failures == 1 else 3600.0

        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(
                    spool_dir,
                    self._RICH_CAP_1K,
                    replay=mock.Mock(return_value=True),
                    expected_failure=client_module._WORKER_EXIT_SPOOL_FAILED,
                    retry_delay=delay,
                ) as lane,
                mock.patch.object(
                    spool_module.SpoolWriter,
                    "seal",
                    autospec=True,
                    side_effect=OSError("spool mount is gone"),
                ) as seal,
            ):
                lane.put(self._overflowing(1))
                self.assertTrue(self._wait_for(lambda: seal.called))
                time.sleep(0.2)
                self.assertEqual(seal.call_count, 1)

    def test_rich_byte_cap_spools_whole_lane_and_future_entries_in_order(self):
        env = {"KYMO_MAX_RICH_BUFFER_BYTES": "1"}
        with tempfile.TemporaryDirectory() as spool_dir:
            with self._lane_worker(spool_dir, env) as lane:
                lane.put([self._metadata(1), self._metadata(2)])
                self.assertTrue(lane.first_spill.wait(timeout=1))
                lane.put([self._metadata(3)])
            _, records = read_spool(lane.path)
            self.assertEqual([record[3]["v"] for record in records], [1, 2, 3])
        self.assertEqual(lane.spooled.value, 1)
        self.assert_wait_reports_spooled(lane.status, lane.spooled)

    def test_rich_byte_cap_counts_encoded_gallery_bytes_not_raw_pixels(self):
        gallery = [Image(PILImage.new("RGB", (128, 128))) for _ in range(4)]
        cap = 100_000
        self.assertGreater(len(self._gallery(1, gallery)[2]), cap)
        uploads = []
        env = {"KYMO_MAX_RICH_BUFFER_BYTES": str(cap)}
        with tempfile.TemporaryDirectory() as spool_dir:
            with self._lane_worker(
                spool_dir, env, self._upload_recorder(uploads)
            ) as lane:
                lane.put(self._gallery(1, gallery))
                lane.put(self._gallery(2, gallery))
            self.assertFalse(os.path.exists(lane.path))
        self.assertEqual(lane.spooled.value, 0)
        self.assertEqual(uploads, (["png"] * 4 + ["json"]) * 2)
        # Each image is encoded once: the upload reuses the encoder's bytes.
        self.assertEqual(lane.encode.call_count, 8)

    def test_failed_encode_is_retried_from_the_item_that_failed(self):
        images = [Image(bytes([index]) * 8, format="webp") for index in range(3)]
        encoded = [client_module._encode_image(image) for image in images]
        # The second item fails once; the retry encodes it and the third, not the first again.
        encode = [encoded[0], MemoryError("pressure"), encoded[1], encoded[2]]
        uploads = []
        env = {"KYMO_MAX_RICH_BUFFER_BYTES": str(1 << 20)}
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(
                    spool_dir, env, self._upload_recorder(uploads), encode
                ) as lane,
            ):
                lane.put(self._gallery(1, images))
        self.assertEqual(lane.spooled.value, 0)
        self.assertEqual(uploads, ["webp"] * 3 + ["json"])
        self.assertEqual(lane.encode.call_count, 4)

    def test_a_gallery_the_spool_cannot_encode_loses_only_itself(self):
        env = {"KYMO_MAX_RICH_BUFFER_BYTES": str(1 << 20)}
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING") as logs,
                self._lane_worker(
                    spool_dir,
                    env,
                    encode=MemoryError("pressure"),
                    expected_failure=client_module._WORKER_EXIT_SPOOL_FAILED,
                ) as lane,
            ):
                # The encoder gives up on the gallery and fails the lane over; the spool cannot encode it either.
                lane.put(self._gallery(1, [Image(PILImage.new("RGB", (8, 8)))]))
                lane.put([self._metadata(1)])
                self.assertTrue(lane.first_spill.wait(timeout=5))
                lane.put([self._metadata(2)])
            records = list(read_spool(lane.path)[1])
        self.assertEqual([record[3]["v"] for record in records], [1, 2])
        self.assertEqual(lane.spooled.value, 1)
        self.assertIn("1 unencodable", "\n".join(logs.output))

    def test_galleries_arriving_while_encodes_fail_are_charged_whole(self):
        real_encode = client_module._encode_image
        retrying = threading.Event()
        release = threading.Event()
        calls = []

        def encode(image):
            calls.append(image)
            if len(calls) == 1:
                raise MemoryError("pressure")
            if len(calls) == 2:
                retrying.set()
                release.wait(timeout=5)
            return real_encode(image)

        gallery = [Image(PILImage.new("RGB", (128, 128)))]
        env = {"KYMO_MAX_RICH_BUFFER_BYTES": "10000"}
        uploads = []
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(
                    spool_dir, env, self._upload_recorder(uploads), encode
                ) as lane,
            ):
                lane.put(self._gallery(1, gallery))
                self.assertTrue(retrying.wait(timeout=1))
                # Its arrays alone exceed the cap; charged nothing, it would wait behind the failing encode.
                lane.put(self._gallery(2, gallery))
                release_later = threading.Timer(0.2, release.set)
                release_later.start()
            release_later.join()
            self.assertEqual(lane.spooled.value, 1)
            records = list(read_spool(lane.path)[1])
        self.assertEqual(uploads, [])
        self.assertEqual([record[2] for record in records], [1, 2])

    def test_failover_after_a_spool_failure_does_not_wait_for_the_encoder(self):
        started = threading.Event()
        release = threading.Event()
        encoded = client_module._encode_image(Image(b"x" * 8, format="webp"))

        def encode(image):
            if image.data == b"b" * 8:
                started.set()
                release.wait()
            return encoded

        def failing_spill(spool, point):
            raise OSError("spool unavailable")

        def gallery(step, byte):
            return self._gallery(step, [Image(byte * 8, format="webp")])

        env = {"KYMO_MAX_RICH_BUFFER_BYTES": str(1 << 20)}
        # The encoder stays blocked until after the harness has checked that the worker exited.
        try:
            with tempfile.TemporaryDirectory() as spool_dir:
                with (
                    mock.patch.object(client_module, "_MAX_CDN_QUEUE", 2),
                    self.assertLogs("kymo", level="WARNING"),
                    self._lane_worker(
                        spool_dir,
                        env,
                        encode=encode,
                        spill=failing_spill,
                        expected_failure=client_module._WORKER_EXIT_SPOOL_FAILED,
                    ) as lane,
                ):
                    lane.put(gallery(1, b"a"))
                    lane.put(gallery(2, b"b"))
                    self.assertTrue(started.wait(timeout=1))
                    # The third entry trips the two-entry cap while the second gallery is still encoding; writing the first fails, so waiting could not help.
                    lane.put(gallery(3, b"c"))
        finally:
            release.set()

    def test_encoded_gallery_over_the_rich_byte_cap_spools_its_encoded_bytes(self):
        noise = PILImage.frombytes("RGB", (64, 64), os.urandom(64 * 64 * 3))
        gallery = [Image(noise), Image(noise)]
        encoded = client_module._encode_image(gallery[0])[0]
        upload = mock.Mock(side_effect=AssertionError("must spool, not upload"))
        env = {"KYMO_MAX_RICH_BUFFER_BYTES": str(len(encoded))}
        with tempfile.TemporaryDirectory() as spool_dir:
            with self._lane_worker(spool_dir, env, upload) as lane:
                lane.put(self._gallery(1, gallery))
            records = list(read_spool(lane.path)[1])
        upload.assert_not_called()
        self.assertEqual(lane.spooled.value, 1)
        self.assertEqual(
            [(record[0], record[2]) for record in records], [("cdn_batch_encoded", 1)]
        )
        self.assertEqual([entry["data"] for entry in records[0][3]], [encoded] * 2)
        # The spool wrote the encoder's bytes rather than encoding the gallery again.
        self.assertEqual(lane.encode.call_count, 2)

    def test_failover_during_an_encode_waits_for_it_instead_of_encoding_again(self):
        real_encode = client_module._encode_image
        started = threading.Event()
        release = threading.Event()

        def blocked_encode(image):
            started.set()
            release.wait(timeout=5)
            return real_encode(image)

        gallery = [Image(PILImage.new("RGB", (16, 16))) for _ in range(3)]
        env = {"KYMO_MAX_RICH_BUFFER_BYTES": str(1 << 20)}
        with tempfile.TemporaryDirectory() as spool_dir:
            with (
                mock.patch.object(client_module, "_MAX_CDN_QUEUE", 1),
                self.assertLogs("kymo", level="WARNING"),
                self._lane_worker(spool_dir, env, encode=blocked_encode) as lane,
            ):
                lane.put(self._gallery(1, gallery))
                self.assertTrue(started.wait(timeout=1))
                # A second entry exceeds the one-entry cap while the first gallery is still encoding.
                lane.put(self._gallery(2, gallery))
                release_later = threading.Timer(0.2, release.set)
                release_later.start()
            release_later.join()
            records = list(read_spool(lane.path)[1])
        self.assertEqual([record[2] for record in records], [1, 2])
        # Three by the encoder for the first gallery and three by the spool for the second.
        self.assertEqual(lane.encode.call_count, 6)

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
        with tempfile.TemporaryDirectory() as spool_dir:
            with self._lane_worker(
                spool_dir, {"KYMO_MAX_RICH_BUFFER_BYTES": "100"}
            ) as lane:
                lane.put([direct])
                self.assertTrue(lane.first_spill.wait(timeout=1))
            self.assertEqual(list(read_spool(lane.path)[1]), [direct])
        self.assertEqual(lane.spooled.value, 1)


class _Unpicklable:
    def __reduce__(self):
        raise ValueError("cannot pickle")


@contextlib.contextmanager
def _failing_spool_writes(already_full: bool = False):
    """Once the yielded event is set, the disk fills up: the next spool file write lands only half its bytes, as write(2) does (none if already_full), and every write after it fails without writing."""
    failing = threading.Event()
    real_write = spool_module._SpoolFile.write
    short_written = []

    def write(raw, b):
        if not failing.is_set() or raw.discarding:
            return real_write(raw, b)
        if short_written or already_full:
            raise OSError(28, "No space left on device")
        data = bytes(b)
        short_written.append(real_write(raw, data[: len(data) // 2]))
        return short_written[0]

    with mock.patch.object(spool_module._SpoolFile, "write", write):
        yield failing


class SpoolDurabilityTests(unittest.TestCase):
    def test_a_record_that_fails_partway_is_cut_off_and_the_file_stays_readable(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "torn.mkspool")
            spool = SpoolWriter(path, header={})
            spool.write(numeric(1.0, step=1))
            spool.write(numeric(2.0, step=2))
            # A megabyte reaches the file before the record fails to pickle.
            with self.assertRaisesRegex(ValueError, "cannot pickle"):
                spool.write(("text_ts", "logs", 3, b"x" * (1 << 20), _Unpicklable()))
            self.assertEqual(spool.count, 2)
            spool.flush()
            _, records = read_spool(path)
            self.assertEqual([record[2] for record in records], [1, 2])
            spool.write(numeric(4.0, step=4))
            spool.close()
            _, records = read_spool(path)
            self.assertEqual([record[2] for record in records], [1, 2, 4])

    def test_a_failed_flush_drops_the_unflushed_records_and_cuts_the_file_back(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "full.mkspool")
            spool = SpoolWriter(path, header={})
            spool.write(numeric(1.0, step=1))
            spool.flush()
            flushed_size = os.path.getsize(path)
            spool.write(numeric(2.0, step=2))
            with _failing_spool_writes() as failing:
                failing.set()
                with self.assertRaises(OSError):
                    spool.flush()
            self.assertEqual(spool.count, 1)
            self.assertEqual(os.path.getsize(path), flushed_size)
            # Nothing stale from the dropped buffer reaches the file when it closes.
            spool.close()
            self.assertEqual(os.path.getsize(path), flushed_size)
            _, records = read_spool(path)
            self.assertEqual([record[2] for record in records], [1])

    def test_a_failed_flush_keeps_the_records_that_already_reached_the_file(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "partial.mkspool")
            spool = SpoolWriter(path, header={})
            spool.write(numeric(1.0, step=1))
            spool.flush()
            # Too large to buffer, so it reaches the file before the next flush.
            spool.write(("text_ts", "logs", 2, "x" * (1 << 20), 1_700_000_000_000))
            spool.write(numeric(3.0, step=3))
            with _failing_spool_writes() as failing:
                failing.set()
                with self.assertRaises(OSError):
                    spool.flush()
            self.assertEqual(spool.count, 2)
            spool.close()
            _, records = read_spool(path)
            self.assertEqual([record[2] for record in records], [1, 2])

    def test_a_failed_write_whose_earlier_records_cannot_be_flushed_still_ends_whole(
        self,
    ):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "full.mkspool")
            spool = SpoolWriter(path, header={})
            spool.write(numeric(1.0, step=1))
            spool.flush()
            spool.write(numeric(2.0, step=2))
            with _failing_spool_writes() as failing:
                failing.set()
                with self.assertRaises(OSError):
                    spool.write(("text_ts", "logs", 3, "x" * (1 << 20), 0))
            spool.close()
            _, records = read_spool(path)
            steps = [record[2] for record in records]
            self.assertEqual(steps, [1, 2][: spool.count])

    def test_a_failed_first_record_keeps_the_header(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "first.mkspool")
            spool = SpoolWriter(path, header={})
            spool.write(numeric(1.0, step=1))
            with _failing_spool_writes() as failing:
                failing.set()
                with self.assertRaises(OSError):
                    spool.flush()
            self.assertEqual(spool.count, 0)
            spool.write(numeric(2.0, step=2))
            spool.close()
            _, records = read_spool(path)
            self.assertEqual([record[2] for record in records], [2])

    def test_a_torn_header_leaves_an_empty_file_that_the_next_write_reopens(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "header.mkspool")
            spool = SpoolWriter(path, header={})
            with _failing_spool_writes() as failing:
                failing.set()
                with self.assertRaises(OSError):
                    spool.write(numeric(1.0, step=1))
            self.assertEqual(os.path.getsize(path), 0)
            spool.write(numeric(2.0, step=2))
            spool.close()
            _, records = read_spool(path)
            self.assertEqual([record[2] for record in records], [2])

    def test_record_ends_stay_bounded_between_flushes(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "long.mkspool")
            spool = SpoolWriter(path, header={})
            # A small-block file system's buffer, so records reach the file long before 8192 ends pile up.
            with mock.patch.object(
                spool_module.os,
                "fstat",
                return_value=types.SimpleNamespace(st_blksize=4096),
            ):
                spool.write(numeric(1.0, step=0))
            # Write until the list is pruned, then fail at once: the pruned list must still hold the end of the last record on disk.
            step = 1
            pruned = False
            while not pruned:
                before = len(spool._record_ends)
                spool.write(numeric(1.0, step=step))
                step += 1
                pruned = len(spool._record_ends) < before
                self.assertLessEqual(len(spool._record_ends), 8193)
            with _failing_spool_writes(already_full=True) as failing:
                failing.set()
                with self.assertRaises(OSError):
                    spool.flush()
            spool.close()
            _, records = read_spool(path)
            self.assertEqual(
                [record[2] for record in records], list(range(spool.count))
            )

    def test_a_torn_record_that_cannot_be_cut_off_stops_the_writer(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "stuck.mkspool")
            spool = SpoolWriter(path, header={})
            spool.write(numeric(1.0, step=1))
            spool.flush()
            spool.write(numeric(2.0, step=2))
            with (
                _failing_spool_writes() as failing,
                mock.patch.object(
                    spool_module.os, "ftruncate", side_effect=OSError(5, "EIO")
                ),
            ):
                failing.set()
                with self.assertRaises(OSError):
                    spool.flush()
            # The unflushed record is not counted, and nothing may follow the torn one.
            self.assertEqual(spool.count, 1)
            with self.assertRaisesRegex(OSError, "torn record"):
                spool.write(numeric(3.0, step=3))
            # It still seals, so replay can quarantine it and the worker go on to newer segments.
            spool.seal()
            with self.assertRaisesRegex(OSError, "torn record"):
                spool.close()

    def test_the_write_buffer_takes_the_file_system_block_size(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "blocks.mkspool")
            spool = SpoolWriter(path, header={})
            with mock.patch.object(
                spool_module.os,
                "fstat",
                return_value=types.SimpleNamespace(st_blksize=4 << 20),
            ):
                spool.write(numeric(0.0, step=0))
            opened = os.path.getsize(path)
            for step in range(1, 5000):
                spool.write(numeric(float(step), step=step))
            # Hundreds of KiB of records still fit the 4 MiB buffer of a large-block file system.
            self.assertEqual(os.path.getsize(path), opened)
            spool.close()
            self.assertGreater(os.path.getsize(path), opened)

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

            writer.seal()
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
                count = 0

                def write(self, _record):
                    self.count += 1

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
            count = 0  # a writer whose flush failed holds none of the records

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
            count = 0

            def write(self, _record):
                self.count += 1

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

    def test_static_quarantine_is_reported_and_advances_to_later_run_spools(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            paths = [
                os.path.join(spool_dir, f"{name}.mkspool")
                for name in ("first", "second", "third")
            ]
            for created, path in enumerate(paths, 1):
                self._write(path, [numeric(float(created))], created=created)
            # The first and third end in a torn record, which the pre-scan rejects for good.
            for path in paths[0], paths[2]:
                with open(path, "ab") as fh:
                    fh.write(b"\x80\x05\x95")
            quarantined = set()
            with (
                mock.patch.object(
                    sync_module, "_connect", return_value=(_Channel(), object())
                ),
                mock.patch.object(sync_module, "_send_tuples", return_value=True),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertFalse(
                    sync_module.replay_file(paths[2], quarantined_files=quarantined)
                )
                self.assertEqual(sync_module.main([spool_dir]), 1)

            # Reported as a DATA_LOSS quarantine is, so the worker and directory sync both move on to the run's later spools.
            self.assertEqual(quarantined, {paths[2] + ".rejected"})
            self.assertTrue(os.path.exists(paths[0] + ".rejected"))
            self.assertTrue(os.path.exists(paths[1] + ".sent"))

    def test_delete_removes_a_replayed_file_instead_of_keeping_it_sent(self):
        with tempfile.TemporaryDirectory() as spool_dir:
            path = os.path.join(spool_dir, "delivered.mkspool")
            self._write(path, [numeric(1.0)])
            with (
                mock.patch.object(
                    sync_module, "_connect", return_value=(_Channel(), object())
                ),
                mock.patch.object(sync_module, "_send_tuples", return_value=True),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertTrue(sync_module.replay_file(path, delete=True))
            self.assertEqual(os.listdir(spool_dir), [])

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
