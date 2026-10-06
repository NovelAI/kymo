"""Background upload process for kymo."""

import multiprocessing
import os
import queue
import random
import sys
import threading
import time
from typing import Optional

import grpc

from kymo._cdn import metadata_manifest
from kymo._env import integer as _env_integer
from kymo._env import number as _env_number
from kymo._generated import kymo_pb2_grpc
from kymo._log import logger as _log
from kymo.spool import replay_command


class _ReplayQuarantined(RuntimeError):
    """A sealed spool left automatic replay: the server reported authoritative DATA_LOSS, or its records can never be delivered."""

    def __init__(self, path: str):
        super().__init__(f"quarantined {path}")
        self.path = path


class _SegmentGone(RuntimeError):
    """A sealed spool vanished before its replay: another process took it (a kymo.sync where flock does not work, or an operator)."""

    def __init__(self, path: str):
        super().__init__(f"sealed upload spool {path} disappeared before its replay")
        self.path = path


def _encoded_entry(entry: tuple, items) -> tuple:
    """The queued gallery entry with its items replaced by their encoded form."""
    return (*entry[:3], items, *entry[4:])


# An upload lane is live, or spooling while its recovery circuit is open.
# A spooling lane writes its new work to disk until a replay starts on a proven circuit.
# It then drains: new work waits in RAM behind the sealed prefix instead of growing it, so the catch-up converges under sustained logging.
# It goes live only once the prefix is gone.
_LIVE, _TO_DISK, _DRAINING = "live", "to disk", "draining"


def _upload_worker(
    server_address: str,
    project_id: str,
    run_id: str,
    metric_queue: multiprocessing.Queue,
    queue_status: multiprocessing.Value,
    cdn_url: str = "",
    shutdown_deadline: Optional[multiprocessing.Value] = None,
    spool_path: str = "",
    run_name: str = "",
    upload_failure: Optional[multiprocessing.Value] = None,
    upload_spooled: Optional[multiprocessing.Value] = None,
    owner_pid: Optional[int] = None,
    upload_terminal: Optional[multiprocessing.Value] = None,
    session: str = "",
    local_endpoint_config: Optional[dict] = None,
    galleries_released=None,
    lanes_on_disk=None,
):
    """Runs in a child process, streaming metrics over one long-lived
    IngestMetricsBidi call: feed a bounded in-flight window, free points as the
    server's cumulative acks arrive (see _BidiStream).
    Rich CDN, metadata, and version-aware direct keys share one ordered unary
    lane. Legacy direct CDN keys remain ordinary bidi points.

    Degradation ladder when the server can't keep up:
    1. in-flight window — ≤ _MAX_UNACKED_POINTS fed-but-unacked; feeding stops
       there and the buffer absorbs the rest;
    2. RAM cap or ACK wait — the overflowing ordered lane fails over to disk, as
       does a live numeric lane that holds points and gets no ACK for 3 minutes;
       sealed spool segments replay FIFO while producers append to a newer
       segment, and a lane resumes live only after the disk prefix is empty;
    3. shutdown deadline — past it, the stream is cancelled and everything
       undelivered spills, bounding exit latency;
    4. orphan watch — a parent that dies without draining us becomes a shutdown
       with a 30s deadline instead of retrying forever.
    """
    # Import owner-side machinery only when the child begins executing. Keeping
    # this module importable without importing client.py is what lets the spawn
    # start method restore this top-level function by module path.
    from kymo.client import (
        _ACK_PROGRESS_TIMEOUT,
        _ACTIVE_POLL_TIMEOUT,
        _BidiStream,
        _CDN_MAX_ATTEMPTS,
        _ChildGalleryItems,
        _CONNECT_INITIAL_DELAY,
        _CONNECT_PENDING,
        _CONNECT_POLL_TIMEOUT,
        _ConnectionAttempt,
        _DEFAULT_MAX_BUFFER_BYTES,
        _DEFAULT_MAX_BUFFER_POINTS,
        _DRAIN_MAX_ITEMS_PER_CYCLE,
        _EncodedItems,
        _FEED_Q_CAP,
        _IDLE_POLL_TIMEOUT,
        _MAX_CDN_QUEUE,
        _DEFAULT_MAX_RICH_BUFFER_BYTES,
        _MAX_UNACKED_POINTS,
        _NUMERIC_OUTAGE_FAILOVER,
        _ORDERED_CDN_KINDS,
        _PermanentPointError,
        _RECOVERY_MAX_BUFFER_BYTES,
        _RECOVERY_MAX_BUFFER_POINTS,
        _RetainedLane,
        _RichMutationDataLoss,
        _SERIALIZED_RICH_QUEUE_ITEM,
        _STATUS_LOCK_TIMEOUT,
        _TerminalRunError,
        _ThreadAttempt,
        _WORKER_EXIT_DATA_LOSS,
        _WORKER_EXIT_DRAIN_INCOMPLETE,
        _WORKER_EXIT_RUN_DELETED,
        _WORKER_EXIT_SPOOL_FAILED,
        _cdn_retry_delay,
        _connect,
        _decode_queue_item,
        _drain_worker_queue,
        _encode_cdn_items,
        _equal_jitter_delay,
        _estimate_tuple_bytes,
        _is_terminal_run_error,
        _merged_worker_failure,
        _next_batch,
        _next_connect_retry_delay,
        _process_cdn_batch,
        _publish_manifest_point,
        _publish_rich_mutation,
        _queue_item_size,
        _raw_shared,
        _recovery_retry_delay,
        _retire_rejected_worker_spools,
        _spill_tuple,
        _spool_header,
        _terminal_run_error,
        _upload_to_cdn,
        _validated_ack_delta,
        SpoolWriter,
        make_spool_path,
    )
    from kymo._wire import (
        _MAX_POINTS_PER_MSG,
        _SEND_INITIAL_DELAY,
        _SEND_MAX_DELAY,
        _SEND_MAX_RETRIES,
    )

    if local_endpoint_config is not None:
        from kymo._local_runtime import endpoint_from_worker_config

        local_endpoint = endpoint_from_worker_config(local_endpoint_config)
        server_address = local_endpoint.grpc_target
        cdn_url = local_endpoint.upload_origin
        local_installation_uuid = local_endpoint.installation_uuid
    else:
        local_endpoint = None
        local_installation_uuid = ""

    max_buffer_points = max(
        0,
        _env_integer(
            "KYMO_MAX_BUFFER_POINTS",
            "MKDB2_MAX_BUFFER_POINTS",
            _DEFAULT_MAX_BUFFER_POINTS,
        ),
    )
    max_buffer_bytes = max(
        0,
        _env_integer(
            "KYMO_MAX_BUFFER_BYTES", "MKDB2_MAX_BUFFER_BYTES", _DEFAULT_MAX_BUFFER_BYTES
        ),
    )
    max_rich_buffer_bytes = max(
        0,
        _env_integer(
            "KYMO_MAX_RICH_BUFFER_BYTES",
            "MKDB2_MAX_RICH_BUFFER_BYTES",
            _DEFAULT_MAX_RICH_BUFFER_BYTES,
        ),
    )
    configured = _env_number(
        "KYMO_ACK_PROGRESS_TIMEOUT", "MKDB2_ACK_PROGRESS_TIMEOUT", _ACK_PROGRESS_TIMEOUT
    )
    # A non-positive setting is ignored, so a typo cannot turn the watchdog off.
    ack_progress_timeout = configured if configured > 0 else _ACK_PROGRESS_TIMEOUT

    spool = SpoolWriter(
        spool_path or make_spool_path(project_id, run_id, "worker", session=session),
        header=_spool_header(
            server_address,
            project_id,
            run_id,
            run_name,
            cdn_url,
            session,
            local_installation_uuid,
        ),
    )
    spool_directory = os.path.dirname(os.path.abspath(spool.path))
    recovery_rng = random.Random((os.getpid() << 64) ^ time.time_ns())
    orphan_deadline = 0.0
    creator_pid = owner_pid if owner_pid is not None else os.getppid()
    process_parent = multiprocessing.parent_process()

    def owner_alive() -> bool:
        # multiprocessing preserves the creator's death sentinel even when a
        # forkserver is the OS parent. It also closes the startup race where
        # getppid() has already changed before this target begins executing.
        if process_parent is not None and process_parent.pid == creator_pid:
            try:
                return process_parent.is_alive()
            except (OSError, ValueError):
                pass
        if os.getppid() != creator_pid:
            return False
        try:
            os.kill(creator_pid, 0)
            return True
        except ProcessLookupError:
            return False
        except OSError:
            # Permission errors still prove that the pid exists.
            return True

    def past_deadline() -> bool:
        # A lifecycle rejection is permanent. Keep draining the producer queue
        # to a failed state; never turn those points into replayable spool data.
        if terminal_rejected:
            return False
        if orphan_deadline and time.monotonic() >= orphan_deadline:
            return True
        if shutdown_deadline is None:
            return False
        d = shutdown_deadline.value
        return d > 0.0 and time.monotonic() >= d

    # Readiness attempts span owner-loop cycles, but each poll is non-blocking.
    # The sole mp.Queue consumer therefore keeps draining and bounding its
    # worker-retained lanes while an outage consumes the connector's full
    # five-second attempt window.
    channel = None
    stub = None
    # Numeric reconnects may close their channel. Keep the blocking unary CDN
    # lane isolated so a stream reset cannot cancel its in-progress request.
    # Construct it lazily: most runs only send numeric/text points.
    cdn_channel = None
    cdn_stub = None
    http_client = None
    # Undelivered items, oldest first. Numeric sizes are also their encoded
    # wire estimates, so batching can reuse them instead of UTF-8 encoding each
    # string again. Rich sizes reflect retained serialized memory.
    buffer = _RetainedLane()
    cdn_queue = _RetainedLane()
    numeric_lane = rich_lane = _LIVE
    # The (points, bytes) RAM caps of the half-open numeric lane, None outside it.
    # A fresh channel became ready after numeric disk failover, but no positive ACK has proved the ingest path yet, so the lane is bounded more tightly than ordinary connected operation.
    # A drained lane's caps count on top of the RAM backlog it held as it went live, so going live does not fail it over again.
    numeric_probe_caps: Optional[tuple[int, int]] = None
    # Consecutive failed catch-ups of whichever circuit owns the disk FIFO.
    recovery_failures = 0
    replay_segment: Optional[SpoolWriter] = None
    replay_attempt: Optional[_ThreadAttempt] = None
    # The newest disk-circuit event was a delivered sealed-segment replay: set then, cleared by a failed replay or numeric reconnect and by a lane failing over from live (a broken live circuit is not proof).
    # Lanes drain only while it holds, and the shutdown tail replays only on it.
    replay_proved_circuit = False
    # Whether the replay in flight may set that proof: a lane failing over from live while it runs is newer evidence.
    replay_can_prove = False
    replayed_cdn_keys: dict[tuple, str] = {}
    quarantined_spool_paths: list[str] = []
    # Sealed segments another process took before their replay: whether they were delivered is unknown, so they keep the owner reporting spooled data.
    vanished_spool_paths: list[str] = []
    # The disk circuit's next catch-up, whichever lane owns it; stream_retry_at times only the live numeric stream's reopens.
    recovery_retry_at = 0.0
    # Rich records without logical versions (an old server) keep rich-only failover one-way: their replay guard cannot tell a predecessor delivered live from a newer write, so it would skip the newer record and report it delivered.
    unversioned_rich = False
    shutdown = False
    input_closed = False  # the explicit queue sentinel was consumed
    total_sent = 0
    spilled = 0
    spool_failed = 0  # points lost to a failed spool write; nonzero also stops spill() from writing again
    undecodable = 0  # queued points dropped because their snapshot would not unpickle; kept out of spool_failed, which also stops spill() from writing
    unencodable = 0  # points dropped because their spool record could not be built (a gallery too large to encode); the spool itself is still sound
    drain_incomplete = False
    terminal_rejected = False
    terminal_reason = ""
    # Consecutive failures on cdn_queue's HEAD entry, and the earliest time to retry it. Transient CDN/RPC failures keep the entry queued (popping it was silent data loss); after _CDN_MAX_ATTEMPTS the rich lane fails over to the spool instead.
    cdn_fails = 0
    cdn_backoff_until = 0.0
    last_progress_log = time.monotonic()

    # --- bidi ingest stream state (numeric/text points) ---
    stream: Optional[_BidiStream] = None
    connect_attempt: Optional[_ConnectionAttempt] = None
    cdn_attempt: Optional[_ThreadAttempt] = None
    # An upload a rich failover left running still holds its gallery, so no other upload starts until it ends: repeated outages cannot pile abandoned galleries up past the rich cap.
    abandoned_upload: Optional[_ThreadAttempt] = None
    # Encodes the oldest queued gallery that is still Image/Resource objects, so the rich cap counts encoded bytes.
    encode_attempt: Optional[_ThreadAttempt] = None
    # Consecutive failures encoding that gallery, and the earliest time to retry it, as for the CDN head.
    encode_fails = 0
    encode_backoff_until = 0.0
    inflight_n = 0  # points fed to the current stream, unacked (== buffer[:inflight_n])
    last_acked_cum = 0  # last cumulative points_acked seen on the current stream
    ack_progress_at: Optional[float] = None
    # Since when the live numeric lane has held points without an ACK advancing it; None while no limit applies.
    numeric_waiting_since: Optional[float] = None
    half_closed = False  # feed sentinel sent (shutdown half-close)
    # Earliest time to reopen after a break; backoff runs on the loop cadence.
    stream_retry_at = 0.0
    connect_retry_delay = _CONNECT_INITIAL_DELAY
    # Rebuild the channel after this many consecutive breaks.
    stream_breaks = 0
    last_warned_at = 0.0  # throttle the retry logging
    accounting_bypass_warned = False
    local_refresh_required = False
    local_ensure_attempt: Optional[_ThreadAttempt] = None
    local_ensure_retry_at = 0.0
    local_ensure_retry_delay = _CONNECT_INITIAL_DELAY
    local_identity_mismatch = ""

    release_lock = threading.Lock()

    def release_raw_galleries(count: int) -> None:
        """Publish galleries that no longer hold raw pixels (encoded, spooled or dropped); log() paces the calls that publish galleries by this count."""
        if count and galleries_released is not None:
            # The encoder helper publishes too, so this loop is not its only writer.
            with release_lock:
                galleries_released.value += count

    def publish_lanes_on_disk() -> None:
        """Tell log() whether either lane writes new work to disk; it then never waits for the encoder, since disk writes would set the pace."""
        if lanes_on_disk is not None:
            lanes_on_disk.value = int(_TO_DISK in (numeric_lane, rich_lane))

    def spool_draining_lanes() -> None:
        """Send the draining lanes' RAM work to disk behind the prefix, leaving the retry and the circuit proof as they are."""
        if numeric_lane == _DRAINING:
            failover_numeric()
        if rich_lane == _DRAINING:
            failover_rich()

    def has_pending_delivery() -> bool:
        """One local wake predicate: anything retained or spooled. In-flight points stay in buffer, an upload attempt's head in cdn_queue, and a replay's segment in replay_segment."""
        return bool(buffer or cdn_queue or replay_segment is not None or spool.count)

    def may_start_transport() -> bool:
        """New delivery work may start: the run still accepts data, the deadline has not passed, and no local endpoint refresh, identity mismatch or ensure is pending."""
        return (
            not terminal_rejected
            and not past_deadline()
            and not local_refresh_required
            and not local_identity_mismatch
            and local_ensure_attempt is None
        )

    def close_numeric_channel() -> None:
        """Close the bidi channel, so the next stream needs a fresh connect."""
        nonlocal channel, stub
        if channel is not None:
            channel.close()
        channel = stub = None

    def drop_numeric_transport() -> None:
        """Cancel the stream and any connect, close the bidi channel, and forget the in-flight window."""
        nonlocal stream, connect_attempt
        nonlocal inflight_n, last_acked_cum, ack_progress_at, half_closed
        # Cancel before releasing the in-flight prefix's accounting. A racing
        # commit may duplicate on replay, but cannot ACK and decrement twice.
        if stream is not None:
            stream.cancel()
            stream = None
        if connect_attempt is not None:
            connect_attempt.cancel()
            connect_attempt = None
        # A channel can remain READY briefly while its dead HTTP/2 transport is
        # being torn down. Recovery must not immediately reuse that stale
        # channel and declare the circuit half-open without a real reconnect.
        close_numeric_channel()
        inflight_n = 0
        last_acked_cum = 0
        ack_progress_at = None
        half_closed = False

    def install_local_endpoint(endpoint) -> None:
        nonlocal local_endpoint, server_address, cdn_url
        generation_changed = (
            local_endpoint is None
            or endpoint.endpoint_generation != local_endpoint.endpoint_generation
        )
        if generation_changed:
            drop_numeric_transport()
            close_rich_transport()
        local_endpoint = endpoint
        server_address = endpoint.grpc_target
        cdn_url = endpoint.upload_origin

    def mark_local_transport_stale() -> None:
        nonlocal local_refresh_required
        if local_endpoint is not None:
            local_refresh_required = True

    def resolve_points(
        n: int,
        *,
        spooled: Optional[bool] = None,
        failure: int = 0,
        terminal: bool = False,
    ) -> None:
        """Release delivery accounting without hanging on a dead owner."""
        nonlocal accounting_bypass_warned
        lock = queue_status.get_lock()
        acquired = lock.acquire(timeout=_STATUS_LOCK_TIMEOUT)
        if not acquired and owner_alive():
            raise RuntimeError("kymo upload accounting lock is unavailable")
        if not acquired:
            # The creator is gone, so no producer can race this worker. A lock
            # it died while holding must not defeat orphan spill/exit bounds.
            if not accounting_bypass_warned:
                accounting_bypass_warned = True
                _log.warning(
                    "parent died holding upload accounting; updating raw state"
                )
        try:
            if spooled is not None and upload_spooled is not None:
                _raw_shared(upload_spooled).value = int(spooled)
            if failure and upload_failure is not None:
                raw_failure = _raw_shared(upload_failure)
                raw_failure.value = _merged_worker_failure(raw_failure.value, failure)
            if terminal and upload_terminal is not None:
                _raw_shared(upload_terminal).value = 1
            _raw_shared(queue_status).value -= n
        finally:
            if acquired:
                lock.release()

    def ensure_rich_transport() -> None:
        nonlocal cdn_channel, cdn_stub, http_client
        if http_client is not None:
            return
        if local_endpoint is None:
            import httpx

            cdn_channel = grpc.insecure_channel(server_address)
            http_client = httpx.Client(timeout=60.0)
        else:
            from kymo._local_runtime import grpc_channel, http_client as local_http

            cdn_channel = grpc_channel(local_endpoint)
            http_client = local_http(local_endpoint, timeout=60.0)
        cdn_stub = kymo_pb2_grpc.KymoStub(cdn_channel)

    def close_rich_transport() -> None:
        nonlocal cdn_channel, cdn_stub, http_client
        if http_client is not None:
            http_client.close()
            http_client = None
        if cdn_channel is not None:
            cdn_channel.close()
            cdn_channel = None
        cdn_stub = None

    def spill(tuples: list[tuple]) -> int:
        nonlocal spilled, spool_failed, unencodable
        if not tuples:
            return 0
        # Written, unencodable or lost: none of them stays raw in this worker.
        raw_galleries = sum(map(counted_raw, tuples))
        spooled_now = 0
        if spool_failed:
            spool_failed += len(tuples)
        else:
            held = spool.count
            unwritten = 0
            error = None
            for t in tuples:
                try:
                    if not _spill_tuple(spool, t):
                        unwritten += 1
                except Exception as e:
                    # A write failure is normally disk-wide (full, read-only, broken mount): stop touching it after the first failure; the removed suffix is still resolved below.
                    error = e
                    break
            try:
                # Accounting is a delivery proof. Flush Python's file buffer
                # before decrementing it so a later SIGTERM/SIGKILL cannot
                # erase records that the shared counter already released.
                spool.flush()
            except Exception as e:
                error = error or e
            # A failed write or flush cuts the file back to its last whole record, so the writer's count says what spooled.
            spooled_now = spool.count - held
            unencodable += unwritten
            lost = len(tuples) - spooled_now - unwritten
            if lost:
                _log.warning("failed to spool %d point(s): %s", lost, error)
                spool_failed += lost
        resolve_points(
            len(tuples),
            spooled=True if spooled_now else None,
            failure=_WORKER_EXIT_SPOOL_FAILED if spool_failed or unencodable else 0,
        )
        release_raw_galleries(raw_galleries)
        spilled += spooled_now
        # Only points written: callers log it as spooled, and losses are counted apart.
        return spooled_now

    def close_spool(writer: SpoolWriter) -> Optional[str]:
        """Release a spool's descriptor, including its durability barrier.

        A broken mount must not kill the worker here. Report the path as pending
        instead: whatever records reached the file are still replayable, and the
        owner's own spool scan will find them.
        """
        try:
            return writer.close()
        except OSError as error:
            _log.error("failed to close upload spool %s: %s", writer.path, error)
            return writer.path

    def start_spool_replay() -> bool:
        """Replay the oldest sealed segment in a daemon helper while producers spool, first rotating the active spool into the single FIFO replay slot if that slot is empty.

        False means nothing started: delivery may not start (see may_start_transport), nothing is spooled, or sealing failed and the next attempt waits out a backoff.
        """
        nonlocal spool, replay_segment, replay_attempt, replay_can_prove
        nonlocal numeric_lane, rich_lane
        if not may_start_transport():
            return False
        if replay_segment is None:
            if spool.count == 0:
                return False
            try:
                spool.seal()
            except OSError as error:
                # The unflushed tail is gone, and its accounting was already released
                # as spooled — record the loss, but keep this worker draining its
                # queue instead of dying on a broken mount.
                _log.error("failed to seal upload spool %s: %s", spool.path, error)
                resolve_points(0, failure=_WORKER_EXIT_SPOOL_FAILED)
                schedule_recovery_retry()
                return False
            replay_segment = spool
            spool = SpoolWriter(
                make_spool_path(
                    project_id,
                    run_id,
                    "worker",
                    spool_dir=spool_directory,
                    session=session,
                ),
                header=_spool_header(
                    server_address,
                    project_id,
                    run_id,
                    run_name,
                    cdn_url,
                    session,
                    local_installation_uuid,
                ),
            )
        segment = replay_segment

        def replay() -> bool:
            # Lazy import avoids client <-> sync module initialization recursion.
            from kymo.sync import replay_file

            try:
                # The durability barrier belongs here, off the queue-consumer
                # thread. Delivering the records is a stronger guarantee than
                # persisting them, so a failed fsync only warns.
                segment.sync()
            except OSError as error:
                _log.warning(
                    "failed to fsync %s before replay: %s", segment.path, error
                )
            terminal_runs: set[tuple[str, str, str]] = set()
            quarantined_files: set[str] = set()
            try:
                # Delete consumed segments: their records were delivered, superseded, or on an old server skipped as older than the server's.
                delivered = replay_file(
                    segment.path,
                    delete=True,
                    replayed_keys=replayed_cdn_keys,
                    terminal_runs=terminal_runs,
                    quarantined_files=quarantined_files,
                    _writer_lock_held=True,
                    _local_endpoint=local_endpoint,
                )
            except Exception as error:
                # The server's terminal answer stands even if retiring the segment then failed.
                if terminal_runs:
                    delivered = False
                elif isinstance(error, FileNotFoundError) and not os.path.exists(
                    segment.path
                ):
                    segment.release()
                    raise _SegmentGone(segment.path) from None
                else:
                    raise
            if delivered or terminal_runs or quarantined_files:
                # Done with a segment whose path is gone or renamed: release it without a barrier, since persisting it buys nothing, and here, since closing a deleted file frees its blocks (milliseconds per GiB) and the queue-consumer thread should not pay that.
                segment.release()
            if terminal_runs:
                raise _TerminalRunError("run is no longer writable during spool replay")
            if quarantined_files:
                raise _ReplayQuarantined(next(iter(quarantined_files)))
            return delivered

        replay_attempt = _ThreadAttempt(segment, replay, name="kymo-spool-replay")
        replay_can_prove = True
        if replay_proved_circuit:
            # A proven circuit retains no failed segment, so this replay sealed every record left on disk: the spooling lanes drain, their new work waiting in RAM behind it.
            if numeric_lane == _TO_DISK:
                numeric_lane = _DRAINING
            if rich_lane == _TO_DISK:
                rich_lane = _DRAINING
            publish_lanes_on_disk()
        return True

    def awaits_encoding(entry: tuple) -> bool:
        return entry[0] in ("cdn_batch", "cdn_batch_mutation") and not isinstance(
            entry[3], _EncodedItems
        )

    def counted_raw(entry: tuple) -> bool:
        """A gallery still raw that the initializing process counted: one log()'s encoder wait waits on, unlike a fork child's."""
        return awaits_encoding(entry) and not isinstance(entry[3], _ChildGalleryItems)

    def enforce_rich_caps() -> None:
        if (
            len(cdn_queue) <= _MAX_CDN_QUEUE
            and cdn_queue.total_bytes <= max_rich_buffer_bytes
        ):
            return
        count = failover_rich()
        _log.warning(
            "rich upload backlog exceeded its %d-entry/%d-byte cap "
            "— failed over %d point(s) to %s",
            _MAX_CDN_QUEUE,
            max_rich_buffer_bytes,
            count,
            spool.path,
        )

    def poll_encoder() -> None:
        """Apply a finished encode, then start one on the oldest gallery still awaiting it."""
        nonlocal encode_attempt, encode_fails, encode_backoff_until
        if encode_attempt is not None:
            try:
                succeeded, result = encode_attempt.events.get_nowait()
            except queue.Empty:
                return
            entry = encode_attempt.item
            encode_attempt = None
            if succeeded:
                encode_fails = 0
                index = next(i for i, e in enumerate(cdn_queue.items) if e is entry)
                encoded = _encoded_entry(entry, result)
                cdn_queue.replace(
                    index, encoded, _estimate_tuple_bytes(encoded) + result.nbytes
                )
                enforce_rich_caps()
            else:
                # Items that fail to encode are dropped inside the encode, so this is a MemoryError or a bug: retry it like a failed CDN attempt.
                _log.warning(
                    "encoding CDN entry %s step %d failed: %s",
                    entry[1],
                    entry[2],
                    result,
                )
                encode_fails += 1
                if encode_fails < _CDN_MAX_ATTEMPTS:
                    encode_backoff_until = time.monotonic() + _cdn_retry_delay(
                        encode_fails
                    )
                else:
                    _log.error(
                        "giving up encoding CDN entry %s step %d after %d attempts — failing the rich lane over to the spool",
                        entry[1],
                        entry[2],
                        encode_fails,
                    )
                    failover_rich()
        if time.monotonic() < encode_backoff_until:
            return
        # A rejected rich lane, or one writing to disk, is empty, so it has nothing to encode; a draining one holds new work for it.
        entry = next((e for e in cdn_queue.items if awaits_encoding(e)), None)
        if entry is not None:
            counted = counted_raw(entry)

            def encode():
                result = _encode_cdn_items(entry[1], entry[2], entry[3])
                # Its arrays are gone, so a waiting log() need not wait for this loop to wake and apply the result: the loop wakes when that log() publishes.
                release_raw_galleries(int(counted))
                return result

            encode_attempt = _ThreadAttempt(entry, encode, name="kymo-cdn-encode")

    def retain_queue_item(item) -> None:
        nonlocal undecodable, unversioned_rich
        if terminal_rejected:
            resolve_points(
                _queue_item_size(item),
                failure=_WORKER_EXIT_RUN_DELETED,
            )
            return
        try:
            point_tuples = _decode_queue_item(item)
        except Exception as error:
            # Like parent salvage, drop just this item: a snapshot that only unpickles in the trainer (a non-tensor CUDA object, or a class the worker cannot import) must not stop every later delivery. It is lost, so it fails delivery with the spool-failure code.
            count = _queue_item_size(item)
            if not undecodable:
                _log.error(
                    "failed to decode a queued rich item — dropping %d point(s); later decode losses are summarized at exit: %s",
                    count,
                    error,
                )
            undecodable += count
            resolve_points(count, failure=_WORKER_EXIT_SPOOL_FAILED)
            # Only a rich snapshot can fail to decode, and only a gallery's can hold a tensor. If it was metadata or a fork child's gallery, this release was never counted, which only lets a later log() wait less.
            release_raw_galleries(1)
            return
        pending_spill: list[tuple] = []
        # A serialized envelope holds exactly one rich point, which is charged its size.
        serialized_bytes = (
            len(item[2])
            if isinstance(item, tuple)
            and len(item) == 3
            and item[0] == _SERIALIZED_RICH_QUEUE_ITEM
            else 0
        )
        for point_tuple in point_tuples:
            is_rich = point_tuple[0] in _ORDERED_CDN_KINDS
            unversioned_rich |= point_tuple[0] in ("cdn_batch", "metadata_batch")
            if (rich_lane if is_rich else numeric_lane) == _TO_DISK:
                pending_spill.append(point_tuple)
                continue
            if is_rich:
                # A gallery waiting for the encoder is charged only its bytes-backed items, since log()'s encoder wait bounds how many do; its arrays are charged their encoded bytes once encoded.
                # A gallery no log() paced is charged whole until then: a fork child's, or any while the numeric lane writes to disk.
                # So is any gallery while encodes are failing (memory pressure), when nothing will shrink it soon.
                if (
                    not encode_fails
                    and numeric_lane != _TO_DISK
                    and counted_raw(point_tuple)
                ):
                    retained_bytes = sum(
                        len(element.data)
                        for element in point_tuple[3]
                        if isinstance(element.data, bytes)
                    )
                else:
                    # Galleries and metadata arrive in one serialized envelope; direct versioned CDN keys use raw tuples and must retain their actual string bytes or large keys bypass the rich RAM cap.
                    retained_bytes = serialized_bytes or _estimate_tuple_bytes(
                        point_tuple
                    )
                cdn_queue.append(point_tuple, retained_bytes)
                enforce_rich_caps()
            else:
                retained_bytes = _estimate_tuple_bytes(point_tuple)
                buffer.append(point_tuple, retained_bytes)
                point_cap, byte_cap = numeric_probe_caps or (
                    max_buffer_points,
                    max_buffer_bytes,
                )
                if len(buffer) > point_cap or buffer.total_bytes > byte_cap:
                    count = failover_numeric()
                    _log.warning(
                        "numeric upload backlog exceeded its %d-point/%d-byte "
                        "cap — failed over %d point(s) to %s",
                        point_cap,
                        byte_cap,
                        count,
                        spool.path,
                    )
        # One queue publication often contains hundreds of metrics. Preserve
        # their order but flush/account them as one group rather than issuing a
        # file-buffer flush for every point during a prolonged outage.
        if pending_spill:
            spill(pending_spill)

    def process_cdn_entry(entry: tuple, *, first_attempt: bool) -> bool:
        """Complete one CDN head entry; the owner loop applies its result."""
        kind, name, step, payload, *versions = entry
        # Legacy entries carry no versions; a direct key and metadata carry no reduced one.
        timestamp_ms, mutation_version, reduced_mutation_version = (
            versions + [None] * 3
        )[:3]
        if kind == "cdn_key_mutation":
            return _publish_rich_mutation(
                cdn_stub,
                project_id,
                run_id,
                name,
                step,
                payload,
                timestamp_ms,
                mutation_version,
            )
        if kind in ("cdn_batch", "cdn_batch_mutation"):
            return _process_cdn_batch(
                cdn_stub,
                http_client,
                cdn_url,
                project_id,
                run_id,
                name,
                step,
                payload,
                deadline_fn=past_deadline,
                send_placeholder=first_attempt,
                timestamp_ms=timestamp_ms,
                mutation_version=mutation_version,
                reduced_mutation_version=reduced_mutation_version,
            )
        try:
            encoded_manifest = metadata_manifest(payload.data)
            resource_id = _upload_to_cdn(http_client, cdn_url, encoded_manifest, "json")
            if not _publish_manifest_point(
                cdn_stub,
                project_id,
                run_id,
                name,
                step,
                resource_id,
                timestamp_ms,
                mutation_version,
            ):
                return False
            _log.info("metadata %s step=%d → %s", name, step, resource_id)
            return True
        except (_TerminalRunError, _RichMutationDataLoss):
            raise
        except Exception as error:
            _log.warning("failed to upload metadata: %s", error)
            return False

    def reject_deleted_run(error: BaseException) -> None:
        """Stop every transport and fail queued work without creating a spool."""
        nonlocal terminal_rejected, terminal_reason, cdn_attempt, encode_attempt
        if terminal_rejected:
            return
        terminal_rejected = True
        terminal_reason = str(error) or "run is no longer writable"
        # Publish the irreversible lifecycle observation before transport
        # teardown: channel close/cancellation may block until the parent
        # reaches its force-kill deadline, and parent salvage must already know
        # that every remaining queue item is permanently inadmissible.
        resolve_points(0, failure=_WORKER_EXIT_RUN_DELETED, terminal=True)
        drop_numeric_transport()
        # Closing the rich transport cancels a concurrent rich publication. It
        # may have uploaded content-addressed bytes, but cannot publish a row.
        close_rich_transport()
        cdn_attempt = None
        encode_attempt = None
        rejected = len(buffer) + len(cdn_queue)
        buffer.clear()
        cdn_queue.clear()
        resolve_points(rejected, failure=_WORKER_EXIT_RUN_DELETED)
        _log.error(
            "run %s/%s no longer accepts data — rejected %d queued point(s): %s",
            project_id,
            run_id,
            rejected,
            terminal_reason,
        )

    def schedule_recovery_retry() -> float:
        """Advance and publish the disk circuit's jittered retry deadline."""
        nonlocal recovery_failures, recovery_retry_at
        recovery_failures += 1
        delay = _recovery_retry_delay(recovery_failures, recovery_rng)
        recovery_retry_at = time.monotonic() + delay
        return delay

    def warn_throttled(message: str, *args) -> None:
        """Warn at most every 30 s: these warnings repeat at a broken server's retry cadence."""
        nonlocal last_warned_at
        now = time.monotonic()
        if now - last_warned_at > 30.0:
            last_warned_at = now
            _log.warning(message, *args)

    def handle_break(err) -> None:
        """The stream ended other than by our clean half-close (an RpcError, or —
        `err is None` — a server close we didn't ask for). Unacked points stay in
        buffer[:inflight_n]; reset the fed cursor and re-feed from the front next
        attempt. Back off on loop cadence (no time.sleep); rebuild the channel after
        _SEND_MAX_RETRIES consecutive breaks (it may be wedged), and after every break
        on a local endpoint, behind a refresh. Logging throttled."""
        nonlocal stream, inflight_n, ack_progress_at, half_closed
        nonlocal stream_breaks, stream_retry_at
        if err is not None and _is_terminal_run_error(err):
            reject_deleted_run(_terminal_run_error(err))
            return
        detail = (
            "server closed the stream"
            if err is None
            else (err.details() if hasattr(err, "details") else str(err))
        )
        if numeric_probe_caps is not None:
            # Half-open means exactly one transport attempt: if it ends before
            # proving progress, put its retained probe suffix back on disk and
            # require another fresh-channel attempt. This prevents a READY TCP
            # endpoint with a broken ingest path from accumulating the normal
            # 2M-point window or spinning on a stale channel.
            count = failover_numeric()
            warn_throttled(
                "numeric ingest recovery probe failed (%s) — spooled %d "
                "point(s); retrying on a fresh channel",
                detail,
                count,
            )
            return
        if stream is not None:
            stream.cancel()
        stream = None
        inflight_n = 0
        ack_progress_at = None
        half_closed = False
        stream_breaks += 1
        retry_base = min(
            _SEND_INITIAL_DELAY * (2 ** (stream_breaks - 1)), _SEND_MAX_DELAY
        )
        stream_retry_at = time.monotonic() + _equal_jitter_delay(
            retry_base, recovery_rng
        )
        warn_throttled("ingest stream broke (%s) — retrying", detail)
        if stream_breaks >= _SEND_MAX_RETRIES or local_endpoint is not None:
            close_numeric_channel()
            stream_breaks = 0
            mark_local_transport_stale()

    def void_circuit_proof() -> None:
        """Only a lane failing over from live is evidence against the circuit: it voids the proof (and a running replay's claim to it) and backs off. A draining lane returning to disk leaves both as they are."""
        nonlocal replay_proved_circuit, replay_can_prove
        replay_proved_circuit = replay_can_prove = False
        schedule_recovery_retry()

    def failover_numeric() -> int:
        """Move the retained numeric lane behind an ordered disk prefix.

        While the circuit is open, newly dequeued points append to the active
        spool. Recovery seals and replays segments FIFO; live bidi cannot reopen
        until every older segment has committed, so offline replay can never
        overwrite a newer live suffix from this worker.
        """
        nonlocal numeric_lane, numeric_probe_caps
        nonlocal connect_retry_delay, stream_retry_at
        if numeric_lane == _TO_DISK:
            return 0
        opens_circuit = numeric_lane == _LIVE
        numeric_lane = _TO_DISK
        publish_lanes_on_disk()
        numeric_probe_caps = None
        if opens_circuit:
            void_circuit_proof()
            mark_local_transport_stale()
        drop_numeric_transport()
        connect_retry_delay = _CONNECT_INITIAL_DELAY
        # A live stream's break backoff ends with the live lane: the first stream after recovery opens at once.
        stream_retry_at = 0.0
        count = spill(buffer.items)
        buffer.clear()
        return count

    def failover_rich() -> int:
        """Move the whole ordered rich lane to disk; versioned records return to live delivery once the disk FIFO has been replayed."""
        nonlocal rich_lane, cdn_attempt, cdn_fails, cdn_backoff_until
        nonlocal abandoned_upload
        nonlocal encode_attempt, encode_fails, encode_backoff_until
        if rich_lane == _TO_DISK:
            return 0
        if rich_lane == _LIVE:
            void_circuit_proof()
        rich_lane = _TO_DISK
        publish_lanes_on_disk()
        # The daemon helper owns no accounting; ignore a late result once its head is on disk. Its publish may still land after a replay's: a versioned server rejects it as older, but an old server keeps arrival order, so there it can replace a newer record that a numeric-circuit replay delivered first.
        if cdn_attempt is not None:
            abandoned_upload = cdn_attempt
        cdn_attempt = None
        cdn_fails = 0
        cdn_backoff_until = 0.0
        encode_fails = 0
        encode_backoff_until = 0.0
        # Every entry ahead of an encode in flight is encoded, so write those first. Then wait for the encoder rather than encode its gallery again on this thread (never longer than that encode), unless the spool has failed and would write nothing. The encoder encodes in place, so spilling that gallery only resumes an encode that raised.
        if encode_attempt is None:
            count = spill(cdn_queue.items)
        else:
            index = next(
                i for i, e in enumerate(cdn_queue.items) if e is encode_attempt.item
            )
            count = spill(cdn_queue.items[:index])
            remaining = cdn_queue.items[index:]
            if not spool_failed:
                succeeded, result = encode_attempt.events.get()
                # Its helper counted it as no longer raw, so spill the encoded form, which spill does not count again. With a failed spool it is not waited for, and its helper's late count is a second one: a later log() only waits less.
                if succeeded:
                    remaining[0] = _encoded_entry(remaining[0], result)
            encode_attempt = None
            count += spill(remaining)
        cdn_queue.clear()
        return count

    def advance_after_data_loss(
        error: BaseException, *, quarantined_path: str = ""
    ) -> None:
        """Quarantine one bad segment and keep later delivery eligible."""
        if quarantined_path:
            quarantined_spool_paths.append(quarantined_path)
        else:
            # A live rich head has no spool to rename yet. Move both lanes into
            # the existing FIFO circuit; replay will quarantine the segment
            # containing this head, then advance to newer segments.
            failover_numeric()
            failover_rich()
            close_rich_transport()
        resolve_points(0, failure=_WORKER_EXIT_DATA_LOSS)
        if quarantined_path:
            _log.error(
                "spool replay %s (authoritative DATA_LOSS, or records that can "
                "never be delivered). Later segments remain eligible for delivery",
                error,
            )
        else:
            _log.error(
                "authoritative DATA_LOSS moved the live ordered head to the "
                "spool quarantine path: %s",
                error,
            )

    def accept_next(timeout: float) -> bool:
        """Accept one queue item, waiting up to timeout seconds (0: not at all); False if none came. Only this frame holds the item, so a quiet queue keeps no large serialized gallery alive."""
        nonlocal shutdown, input_closed
        try:
            item = (
                metric_queue.get(timeout=timeout)
                if timeout
                else metric_queue.get_nowait()
            )
        except queue.Empty:
            return False
        if item is None:
            shutdown = input_closed = True
        else:
            retain_queue_item(item)
        return True

    def drain_to_spool_after_deadline() -> None:
        """Move owned work to disk, then drain every new queue item there."""
        nonlocal drain_incomplete
        # Put both lanes into their one-way spool mode before touching the
        # producer queue. Each subsequently dequeued group is flushed before
        # its accounting is released, minimizing worker-private state that
        # owner-fenced salvage cannot reach.
        failover_numeric()
        failover_rich()
        saw_parent_close = _drain_worker_queue(
            metric_queue, retain_queue_item, input_closed=input_closed
        )
        # Silence is an expected close only after the parent has died. In a
        # normal shutdown, missing None means delivery is unproven; exit
        # nonzero so the parent performs its exact fenced salvage.
        if not saw_parent_close and not orphan_deadline:
            drain_incomplete = True

    while True:
        if past_deadline():
            drain_to_spool_after_deadline()
            break

        # ---- drain the queue greedily (block briefly, then take what's ready) ----
        drained_to_empty = False
        drained_items = 0
        if (
            inflight_n > 0
            or cdn_attempt is not None
            or encode_attempt is not None
            or replay_attempt is not None
            or half_closed
        ):
            poll_timeout = _ACTIVE_POLL_TIMEOUT
        elif connect_attempt is not None:
            # Buffered data is active transport work even before the channel
            # becomes ready. Keep shutdown tails and low-rate sends responsive;
            # use the slower connector cadence only when there is nothing to
            # ship yet.
            poll_timeout = _ACTIVE_POLL_TIMEOUT if buffer else _CONNECT_POLL_TIMEOUT
        else:
            poll_timeout = _IDLE_POLL_TIMEOUT
        if accept_next(poll_timeout):
            drained_items = 1
        else:
            drained_to_empty = True
        if past_deadline():
            drain_to_spool_after_deadline()
            break
        while drained_items < _DRAIN_MAX_ITEMS_PER_CYCLE and not past_deadline():
            if not accept_next(0.0):
                drained_to_empty = True
                break
            drained_items += 1
        if past_deadline():
            drain_to_spool_after_deadline()
            break

        # A lane drains only while a proven circuit could take it live. Once the proof is gone, or shutdown has closed the connect gate while numeric owns the disk FIFO, its RAM work goes to disk rather than wait out an outage there; at shutdown the exit tail replays it on the proven circuit.
        if not replay_proved_circuit or (shutdown and numeric_lane != _LIVE):
            spool_draining_lanes()

        # ---- orphan watch: parent died without draining us ----
        if not orphan_deadline and not owner_alive():
            _log.warning(
                "parent process died — flushing for 30s, then spooling the rest"
            )
            orphan_deadline = time.monotonic() + 30.0
            shutdown = True

        # A local worker wakes the stack only for delivery it currently owns.
        # Launcher work runs in a daemon helper so a slow first-use ensure never
        # stops this sole queue consumer from draining and bounding new input.
        if (
            local_endpoint is not None
            and local_refresh_required
            and not local_identity_mismatch
            and has_pending_delivery()
            and local_ensure_attempt is None
            and time.monotonic() >= local_ensure_retry_at
            and not past_deadline()
        ):
            from kymo._local_runtime import ensure_local_endpoint

            local_ensure_attempt = _ThreadAttempt(
                None,
                lambda: ensure_local_endpoint(
                    expected_installation_uuid=local_installation_uuid
                ),
                name="kymo-local-ensure",
            )
        if local_ensure_attempt is not None:
            try:
                succeeded, result = local_ensure_attempt.events.get_nowait()
            except queue.Empty:
                pass
            else:
                local_ensure_attempt = None
                if succeeded:
                    install_local_endpoint(result)
                    local_refresh_required = False
                    local_ensure_retry_delay = _CONNECT_INITIAL_DELAY
                    local_ensure_retry_at = time.monotonic()
                else:
                    from kymo._local_runtime import LocalInstallationMismatch

                    if isinstance(result, LocalInstallationMismatch):
                        local_identity_mismatch = str(result)
                        failover_numeric()
                        failover_rich()
                        local_refresh_required = False
                        _log.error(
                            "%s; retaining new points in the old installation's spool",
                            result,
                        )
                    else:
                        # Work held in RAM behind the prefix goes to disk rather than wait out a failing local stack there.
                        spool_draining_lanes()
                        local_ensure_retry_at = time.monotonic() + _equal_jitter_delay(
                            local_ensure_retry_delay, recovery_rng
                        )
                        local_ensure_retry_delay = _next_connect_retry_delay(
                            local_ensure_retry_delay
                        )
                        warn_throttled("local runtime ensure failed: %s", result)

        # ---- ordered disk catch-up ----
        # A sealed segment retains its writer lock while the helper replays it;
        # producers append to the newer active segment. Never open live bidi
        # until every segment has succeeded in FIFO order.
        if replay_attempt is not None:
            try:
                replay_succeeded, replay_result = replay_attempt.events.get_nowait()
            except queue.Empty:
                pass
            else:
                completed_segment = replay_attempt.item
                replay_attempt = None
                error = None
                if not replay_succeeded:
                    error = replay_result
                elif not replay_result:
                    error = RuntimeError("spool replay did not complete")
                # The helper has released a segment it finished with; only a failed replay keeps it for the retry.
                if isinstance(error, _TerminalRunError):
                    replay_segment = None
                    reject_deleted_run(error)
                elif error is None or isinstance(error, _ReplayQuarantined):
                    # Delivered, or quarantined so that later segments stay eligible.
                    replay_segment = None
                    # The segment is done, so the backoff starts over, unless a lane failed over from live while this replay ran (newer evidence). Only a delivery proves the circuit: a quarantine can come from a check that never reached the server.
                    if replay_can_prove:
                        recovery_failures = 0
                        if error is None:
                            replay_proved_circuit = True
                    if error is None:
                        _log.info(
                            "replayed sealed upload spool %s; checking for a newer segment",
                            completed_segment.path,
                        )
                    else:
                        advance_after_data_loss(error, quarantined_path=error.path)
                    # Publish an empty disk prefix here, not on the live-resume
                    # path below: a shutdown that arrives during this replay
                    # closes the connect gate, and the owner would otherwise
                    # report spooled data that has in fact been delivered.
                    if spool.count == 0 and not vanished_spool_paths:
                        resolve_points(0, spooled=False)
                elif isinstance(error, _SegmentGone):
                    # Nothing is left to replay, so go on to newer segments.
                    replay_segment = None
                    vanished_spool_paths.append(error.path)
                    _log.warning("%s; going on to newer segments", error)
                else:
                    replay_proved_circuit = False
                    if local_endpoint is not None:
                        # A restarted local server reuses its socket path but
                        # rotates the bearer. A READY UDS channel therefore
                        # does not prove that replay authenticated; force an
                        # endpoint refresh before retrying this disk prefix.
                        close_numeric_channel()
                        close_rich_transport()
                        mark_local_transport_stale()
                    delay = schedule_recovery_retry()
                    warn_throttled(
                        "ordered spool replay failed (%s) — retrying in %.1fs",
                        error,
                        delay,
                    )

        # ---- rich-only disk catch-up ----
        # With numeric live, the active spool holds only rich records. Replay it as the numeric circuit would, and resume live rich delivery once no disk prefix remains; until then new rich work waits behind it, so nothing overtakes an older record. Shutdown starts a replay only on a proven circuit, as the numeric tail does, but goes live on an empty prefix (a quarantine can leave one) regardless.
        if (
            rich_lane != _LIVE
            and numeric_lane == _LIVE
            and not unversioned_rich
            and replay_attempt is None
            and time.monotonic() >= recovery_retry_at
            and may_start_transport()
        ):
            if replay_segment is None and spool.count == 0:
                rich_lane = _LIVE
                publish_lanes_on_disk()
                recovery_failures = 0
                _log.info("rich uploads are live again; the spool has been replayed")
            elif not shutdown or replay_proved_circuit:
                start_spool_replay()

        # ---- bidi ingest: (re)open a stream, then apply acks ----
        # Reconnect when there is no channel, so a stream can open.
        if connect_attempt is not None and (
            (shutdown and (numeric_lane != _LIVE or not buffer))
            or (local_endpoint is not None and not has_pending_delivery())
        ):
            connect_attempt.cancel()
            connect_attempt = None
        if (
            stub is None
            and connect_attempt is None
            # A replay in flight belongs to an open numeric circuit, or else to the rich-only catch-up, beside which the live numeric lane still reconnects.
            and (replay_attempt is None or numeric_lane == _LIVE)
            # An open numeric circuit still connects in the background while
            # an empty active spool proves there is no older disk prefix.
            and (not shutdown or (numeric_lane == _LIVE and buffer))
            and (local_endpoint is None or has_pending_delivery())
            and may_start_transport()
            and time.monotonic()
            >= (stream_retry_at if numeric_lane == _LIVE else recovery_retry_at)
        ):
            if local_endpoint is None:
                # Preserve the hosted call contract exactly; local endpoint
                # material is meaningful only to the private UDS connector.
                connect_attempt = _connect(server_address, deadline_fn=past_deadline)
            else:
                connect_attempt = _connect(
                    server_address,
                    deadline_fn=past_deadline,
                    local_endpoint=local_endpoint,
                )
        connect_result = (
            connect_attempt.poll(deadline_fn=past_deadline)
            if connect_attempt is not None
            else _CONNECT_PENDING
        )
        if connect_result is not _CONNECT_PENDING:
            connect_attempt = None
            channel, stub = connect_result
            if stub is None:
                mark_local_transport_stale()
                if numeric_lane != _LIVE:
                    replay_proved_circuit = False
                    schedule_recovery_retry()
                else:
                    stream_retry_at = time.monotonic() + _equal_jitter_delay(
                        connect_retry_delay, recovery_rng
                    )
                    connect_retry_delay = _next_connect_retry_delay(connect_retry_delay)
            else:
                connect_retry_delay = _CONNECT_INITIAL_DELAY
                if numeric_lane != _LIVE:
                    # Queue draining precedes this poll, so no sealed segment and
                    # an empty active spool together prove the disk prefix is
                    # gone. Anything else replays first — including a rotation
                    # that could not be sealed, which must not be stepped over.
                    if replay_segment is not None or spool.count:
                        close_numeric_channel()
                        start_spool_replay()
                    else:
                        numeric_probe_caps = (
                            min(
                                max_buffer_points,
                                len(buffer) + _RECOVERY_MAX_BUFFER_POINTS,
                            ),
                            min(
                                max_buffer_bytes,
                                buffer.total_bytes + _RECOVERY_MAX_BUFFER_BYTES,
                            ),
                        )
                        numeric_lane = rich_lane = _LIVE
                        publish_lanes_on_disk()
                        recovery_failures = 0
        if (
            stream is None
            and numeric_lane == _LIVE
            and stub is not None
            and (
                (buffer or not shutdown)
                if local_endpoint is None
                else has_pending_delivery()
            )
            and not past_deadline()
            and time.monotonic() >= stream_retry_at
        ):
            try:
                stream = _BidiStream(stub, _FEED_Q_CAP)
            except Exception as error:
                # Call construction can fail before a pump exists (closed/bad
                # channel, incompatible stub). Treat it like every other stream
                # break so private buffered points remain retryable.
                handle_break(error)
            else:
                # Fresh attempt: the server's cumulative counter restarts at 0, so
                # last_acked_cum MUST restart with it or the first delta is wild.
                inflight_n = 0
                last_acked_cum = 0
                ack_progress_at = None
                half_closed = False
        if stream is not None:
            for kind, payload in stream.poll_acks():
                if kind == "ack":
                    # Cumulative: delta = cum - last. Delete that many off the buffer
                    # front, shrink the prefix, decrement queue_status ONCE (spill is
                    # the only other decrementer).
                    cumulative = int(payload)
                    try:
                        delta = _validated_ack_delta(
                            cumulative, last_acked_cum, inflight_n
                        )
                    except ValueError as error:
                        handle_break(error)
                        break
                    if delta > 0:
                        buffer.discard_prefix(delta)
                        inflight_n -= delta
                        total_sent += delta
                        last_acked_cum = cumulative
                        stream_breaks = 0  # a live ack clears the break streak
                        numeric_waiting_since = None
                        resolve_points(delta)
                        if numeric_probe_caps is not None:
                            numeric_probe_caps = None
                            _log.warning(
                                "numeric ingest recovered after ordered disk "
                                "catch-up; new points are sending live again"
                            )
                        now = time.monotonic()
                        ack_progress_at = now if inflight_n > 0 else None
                        if now - last_progress_log > 5.0:
                            last_progress_log = now
                            _log.info(
                                "%d points sent (backlog %d)", total_sent, len(buffer)
                            )
                elif kind in ("done", "error"):
                    # A clean "done" after OUR half-close = success. Anything else
                    # (error, or an unrequested "done" — a proxy, not our server) is
                    # an unexpected end → reconnect, re-feed from the front.
                    if kind == "done" and half_closed and inflight_n == 0:
                        stream = None
                        half_closed = False
                    else:
                        if kind == "error":
                            error = payload
                        elif half_closed:
                            error = RuntimeError(
                                f"ingest stream closed with {inflight_n} points unacked"
                            )
                        else:
                            error = None
                        handle_break(error)
                    break  # stream marker is terminal; nothing follows it

        # The long-lived stream has no whole-call deadline. Preserve hung-server
        # protection as a watchdog only while unacknowledged work exists.
        now = time.monotonic()
        if (
            stream is not None
            and inflight_n > 0
            and ack_progress_at is not None
            and now - ack_progress_at >= ack_progress_timeout
        ):
            handle_break(RuntimeError(f"no ack progress for {ack_progress_timeout:g}s"))
        # A live lane that holds points and gets no ACK for this long goes to disk, as over the RAM cap.
        # A draining lane is exempt: its points wait for the replay ahead of them, so the catch-up converges.
        # So are shutdown, which has its own deadline, and a failed spool, where spill() would only count the points lost.
        if numeric_lane != _LIVE or shutdown or spool_failed or not buffer:
            numeric_waiting_since = None
        elif numeric_waiting_since is None:
            numeric_waiting_since = now
        elif now - numeric_waiting_since >= _NUMERIC_OUTAGE_FAILOVER:
            try:
                # A spool that cannot even be opened (a read-only or full home, say) would only lose the points: keep them in RAM and try again after another wait.
                spool.open()
            except OSError as error:
                # Restarting the clock spaces this warning out, so a throttle shared with stream breaks must not hide it.
                numeric_waiting_since = now
                _log.warning(
                    "no numeric ACK for %gs, but the spool cannot be opened (%s) — keeping the points in RAM",
                    _NUMERIC_OUTAGE_FAILOVER,
                    error,
                )
            else:
                count = failover_numeric()
                _log.warning(
                    "no numeric ACK for %gs — failed over %d point(s) to %s",
                    _NUMERIC_OUTAGE_FAILOVER,
                    count,
                    spool.path,
                )

        # ---- shutdown deadline: spool everything undelivered and exit ----
        if past_deadline():
            drain_to_spool_after_deadline()
            break

        # ---- bidi ingest: feed the in-flight window from buffer[inflight_n:] ----
        # Ship everything behind the prefix in ≤_MAX_POINTS_PER_MSG batches, up to the
        # window. Each batch is one cycle's output — a fast run ships full 5k batches,
        # a slow one a small tail — so no accumulate-to-5k latency for low-rate runs,
        # and no 1-point spam. The server coalesces arrivals into efficient inserts.
        if (
            stream is not None
            and numeric_lane == _LIVE
            and not half_closed
            and not past_deadline()
        ):
            while stream.feed_has_room() and inflight_n < _MAX_UNACKED_POINTS:
                take = min(
                    _MAX_POINTS_PER_MSG,
                    len(buffer) - inflight_n,
                    _MAX_UNACKED_POINTS - inflight_n,
                )
                if take <= 0:
                    break
                try:
                    batch, n = _next_batch(
                        project_id,
                        run_id,
                        buffer.items[inflight_n : inflight_n + take],
                        buffer.sizes[inflight_n : inflight_n + take],
                    )
                except _PermanentPointError as error:
                    # Unreachable for live points — log_cdn size-checks keys, text truncates, numerics are tiny — but if a future buffered kind is ever permanently unencodable, degrade instead of crashing: fail the whole numeric lane over to disk (as the RAM cap does) so sync quarantines the offending record, rather than the worker dying with its buffer unspilled.
                    _log.error(
                        "unencodable buffered point — spooling the numeric lane: %s",
                        error,
                    )
                    failover_numeric()
                    break
                # A full feed queue: retry next cycle.
                if not stream.feed(batch):
                    break
                if inflight_n == 0:
                    ack_progress_at = time.monotonic()
                inflight_n += n

        # Request EOF is what makes the server final-flush a partial tail. Do
        # not wait for ACKs (buffer empty) first: an EOF-flush server would then
        # wait for us while we wait for it. The sentinel follows every fed batch
        # in feed_q; a full queue is retried next cycle without dropping data.
        if (
            shutdown
            and stream is not None
            and not half_closed
            # Explicit EOF proves the producer queue is closed. The orphan path
            # has no sentinel, so require an actual empty observation and never
            # half-close merely because the per-cycle drain cap was reached.
            and (input_closed or drained_to_empty)
            and inflight_n == len(buffer)
            and stream.half_close()
        ):
            half_closed = True

        # ---- encode queued galleries, oldest first ----
        # Pillow releases the GIL while encoding, so this daemon helper does not stall the loop. It counts the galleries it encodes for log(), but owns no delivery accounting: the loop swaps the encoded entry in and charges its bytes.
        poll_encoder()

        # ---- process CDN batches (uploads + manifests) ----
        # A daemon helper may block in httpx/unary gRPC, but owns no
        # delivery accounting. This loop alone retains and mutates the ordered
        # queue head, so numeric feeding and ACK harvesting remain independent.
        if cdn_attempt is not None:
            try:
                succeeded, result = cdn_attempt.events.get_nowait()
            except queue.Empty:
                pass
            else:
                # Its name and step only: holding the entry would keep the gallery alive after it leaves cdn_queue.
                head_name, head_step = cdn_attempt.item[1:3]
                cdn_attempt = None
                if succeeded:
                    ok = bool(result)
                else:
                    if isinstance(result, _TerminalRunError):
                        reject_deleted_run(result)
                        continue
                    if isinstance(result, _RichMutationDataLoss):
                        advance_after_data_loss(result)
                        continue
                    _log.warning(
                        "CDN entry %s step %d failed: %s",
                        head_name,
                        head_step,
                        result,
                    )
                    ok = False

                if ok:
                    cdn_fails = 0
                    cdn_backoff_until = 0.0
                    cdn_queue.discard_prefix(1)
                    resolve_points(1)
                elif not past_deadline():
                    if local_endpoint is not None:
                        close_rich_transport()
                        mark_local_transport_stale()
                    cdn_fails += 1
                    if cdn_fails < _CDN_MAX_ATTEMPTS:
                        cdn_backoff_until = time.monotonic() + _cdn_retry_delay(
                            cdn_fails
                        )
                    else:
                        _log.error(
                            "giving up on CDN entry %s step %d after %d attempts "
                            "— failing the rich lane over to the spool",
                            head_name,
                            head_step,
                            cdn_fails,
                        )
                        failover_rich()

        if abandoned_upload is not None and not abandoned_upload.events.empty():
            abandoned_upload = None
        if (
            cdn_attempt is None
            and abandoned_upload is None
            and rich_lane == _LIVE
            and cdn_queue
            and not awaits_encoding(cdn_queue.items[0])
            and cdn_url
            and time.monotonic() >= cdn_backoff_until
            and may_start_transport()
        ):
            ensure_rich_transport()
            # Bound only in the helper's arguments, so a delivered gallery is not kept alive by this frame.
            cdn_attempt = _ThreadAttempt(
                cdn_queue.items[0],
                lambda entry=cdn_queue.items[0], first=cdn_fails == 0: (
                    process_cdn_entry(entry, first_attempt=first)
                ),
                name="kymo-cdn-attempt",
            )

        # ---- shutdown: exit after numeric EOF/ACK and rich work complete ----
        if (
            shutdown
            and not buffer
            and not cdn_queue
            and cdn_attempt is None
            and replay_attempt is None
            and stream is None
            and (input_closed or drained_to_empty)
        ):
            # Shutdown closed the connect gate, so no connect probe can resume the disk FIFO for records spilled behind an in-flight replay (e.g. the owner's final captured-output flush). A delivered replay is the circuit proof instead: keep sealing and replaying the tail until the disk is empty, a replay fails, or delivery may not start (past the deadline, say, or with a local endpoint to refresh) — never exit stranding a tail the proven circuit could deliver as a fresh pending spool.
            if numeric_lane != _LIVE and replay_proved_circuit and start_spool_replay():
                continue
            break

    # Close the bidi channel and anything else still open; a clean exit has already ended the stream.
    drop_numeric_transport()
    close_rich_transport()
    pending_spool_files = []
    spool_file = close_spool(spool)
    if spool_file:
        pending_spool_files.append(spool_file)
    if replay_segment is not None:
        # A running daemon replay owns the sealed descriptor until this worker
        # process exits. If it already finished, release the lock explicitly.
        if replay_attempt is None:
            close_spool(replay_segment)
        pending_spool_files.insert(0, replay_segment.path)
    # Known pending paths are conservative delivery truth, whatever the circuit
    # state was when shutdown interrupted it. Do not restat: disappearance alone
    # cannot prove delivery. Publish before retirement so a rejected run still
    # reports its dropped points as undelivered.
    try:
        resolve_points(
            0,
            spooled=bool(
                pending_spool_files or quarantined_spool_paths or vanished_spool_paths
            ),
        )
    except RuntimeError as error:
        _log.warning("failed to publish final spool state: %s", error)
    if terminal_rejected and pending_spool_files:
        # Earlier transient failures may already have created these files. Keep
        # the evidence, but remove it from automatic *.mkspool replay.
        _retire_rejected_worker_spools(pending_spool_files)
    if pending_spool_files:
        paths = ", ".join(pending_spool_files)
        _log.error(
            "worker exiting: %d points sent, %d spooled to %s (replay with: %s)",
            total_sent,
            spilled,
            paths,
            replay_command(pending_spool_files),
        )
    else:
        _log.info("worker exiting, %d points sent total", total_sent)
    if quarantined_spool_paths:
        _log.error(
            "worker quarantined spool segment(s) (authoritative DATA_LOSS, or "
            "records that can never be delivered) while later segments "
            "remained eligible for delivery: %s",
            ", ".join(quarantined_spool_paths),
        )
    if vanished_spool_paths:
        _log.error(
            "spool segment(s) disappeared before the worker replayed them, so "
            "their delivery is unknown: %s",
            ", ".join(vanished_spool_paths),
        )
    if spool_failed or undecodable or unencodable:
        _log.error(
            "%d points reached neither the server nor the spool (%d unspoolable "
            "(disk full?), %d unencodable, %d undecodable) — LOST",
            spool_failed + unencodable + undecodable,
            spool_failed,
            unencodable,
            undecodable,
        )
        sys.exit(_WORKER_EXIT_SPOOL_FAILED)
    if terminal_rejected:
        sys.exit(_WORKER_EXIT_RUN_DELETED)
    if drain_incomplete:
        _log.error(
            "upload queue drain ended before the parent shutdown sentinel; "
            "parent salvage is required"
        )
        sys.exit(_WORKER_EXIT_DRAIN_INCOMPLETE)
