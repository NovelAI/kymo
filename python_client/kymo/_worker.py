"""Background upload process for kymo."""

import multiprocessing
import os
import queue
import random
import sys
import time
from typing import Optional

import grpc

from kymo._cdn import metadata_manifest
from kymo._env import integer as _env_integer
from kymo._env import number as _env_number
from kymo._generated import kymo_pb2, kymo_pb2_grpc
from kymo._log import logger as _log
from kymo.spool import replay_command


class _ReplayQuarantined(RuntimeError):
    """A sealed spool left automatic replay after authoritative DATA_LOSS."""

    def __init__(self, path: str):
        super().__init__(f"authoritative DATA_LOSS quarantined {path}")
        self.path = path


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
):
    """Runs in a child process, streaming metrics over one long-lived
    IngestMetricsBidi call: feed a bounded in-flight window, free points as the
    server's cumulative acks arrive (see _BidiStream).
    Rich CDN, metadata, and version-aware direct keys share one ordered unary
    lane. Legacy direct CDN keys remain ordinary bidi points.

    Degradation ladder when the server can't keep up:
    1. in-flight window — ≤ _MAX_UNACKED_POINTS fed-but-unacked; feeding stops
       there and the buffer absorbs the rest;
    2. RAM cap — the overflowing ordered lane fails over to disk; sealed spool
       segments replay FIFO while producers append to a newer segment, and the
       numeric lane resumes live only after the disk prefix is empty;
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
        _CONNECT_INITIAL_DELAY,
        _CONNECT_PENDING,
        _CONNECT_POLL_TIMEOUT,
        _ConnectionAttempt,
        _DEFAULT_MAX_BUFFER_BYTES,
        _DEFAULT_MAX_BUFFER_POINTS,
        _DRAIN_MAX_ITEMS_PER_CYCLE,
        _FEED_Q_CAP,
        _IDLE_POLL_TIMEOUT,
        _MAX_CDN_QUEUE,
        _MAX_CDN_QUEUE_BYTES,
        _MAX_UNACKED_POINTS,
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
        _equal_jitter_delay,
        _estimate_tuple_bytes,
        _is_terminal_run_error,
        _merged_worker_failure,
        _next_batch,
        _next_connect_retry_delay,
        _process_cdn_batch,
        _publish_rich_mutation,
        _queue_item_size,
        _recovery_retry_delay,
        _retire_rejected_worker_spools,
        _send_unary_point,
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

    max_buffer_points = _DEFAULT_MAX_BUFFER_POINTS
    max_buffer_points = max(
        0,
        _env_integer(
            "KYMO_MAX_BUFFER_POINTS", "MKDB2_MAX_BUFFER_POINTS", max_buffer_points
        ),
    )
    max_buffer_bytes = _DEFAULT_MAX_BUFFER_BYTES
    max_buffer_bytes = max(
        0,
        _env_integer(
            "KYMO_MAX_BUFFER_BYTES", "MKDB2_MAX_BUFFER_BYTES", max_buffer_bytes
        ),
    )
    max_rich_buffer_bytes = _MAX_CDN_QUEUE_BYTES
    max_rich_buffer_bytes = max(
        0,
        _env_integer(
            "KYMO_MAX_RICH_BUFFER_BYTES",
            "MKDB2_MAX_RICH_BUFFER_BYTES",
            max_rich_buffer_bytes,
        ),
    )
    ack_progress_timeout = _ACK_PROGRESS_TIMEOUT
    configured = _env_number(
        "KYMO_ACK_PROGRESS_TIMEOUT",
        "MKDB2_ACK_PROGRESS_TIMEOUT",
        ack_progress_timeout,
    )
    if configured > 0:
        ack_progress_timeout = configured

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
    numeric_spooling = False
    # A fresh channel became ready after numeric disk failover, but no positive
    # ACK has proved the ingest path yet. Bound this half-open state more tightly
    # than ordinary connected operation.
    numeric_recovery_probe = False
    numeric_failovers = 0
    replay_segment: Optional[SpoolWriter] = None
    replay_attempt: Optional[_ThreadAttempt] = None
    # The newest disk-circuit event was a successful sealed-segment replay: set on replay success, cleared on replay failure and on failover (a broken live circuit is not proof). Shutdown may not open connects, so this proof is what lets the disk FIFO keep replaying a tail spilled behind an in-flight replay.
    replay_proved_circuit = False
    replayed_cdn_keys: dict[tuple, str] = {}
    quarantined_spool_paths: list[str] = []
    rich_spooling = False
    shutdown = False
    input_closed = False  # the explicit queue sentinel was consumed
    total_sent = 0
    spilled = 0
    spool_failed = 0  # points that reached neither the server nor the spool
    undecodable = 0  # queued points dropped because their snapshot would not unpickle; kept out of spool_failed, which also stops spill() from writing
    drain_incomplete = False
    terminal_rejected = False
    terminal_reason = ""
    # Consecutive failures on cdn_queue's HEAD entry, and the earliest time to retry it. Transient CDN/RPC failures keep the entry queued (popping it was silent data loss); after _CDN_MAX_ATTEMPTS it goes to the spool for kymo.sync instead.
    cdn_fails = 0
    cdn_backoff_until = 0.0
    last_progress_log = time.monotonic()

    # --- bidi ingest stream state (numeric/text points) ---
    stream: Optional[_BidiStream] = None
    connect_attempt: Optional[_ConnectionAttempt] = None
    cdn_attempt: Optional[_ThreadAttempt] = None
    inflight_n = 0  # points fed to the current stream, unacked (== buffer[:inflight_n])
    last_acked_cum = 0  # last cumulative points_acked seen on the current stream
    ack_progress_at: Optional[float] = None
    half_closed = False  # feed sentinel sent (shutdown half-close)
    # Earliest time to reopen after a break; backoff runs on the loop cadence.
    stream_retry_at = 0.0
    connect_retry_delay = _CONNECT_INITIAL_DELAY
    # Rebuild the channel after this many consecutive breaks.
    stream_breaks = 0
    last_break_warn = 0.0  # throttle the retry/version-mismatch logging
    accounting_bypass_warned = False
    local_refresh_required = False
    local_ensure_attempt: Optional[_ThreadAttempt] = None
    local_ensure_retry_at = 0.0
    local_ensure_retry_delay = _CONNECT_INITIAL_DELAY
    local_identity_mismatch = ""

    def has_pending_delivery() -> bool:
        """One local wake predicate for every retained delivery owner."""
        return bool(
            buffer
            or cdn_queue
            or inflight_n
            or cdn_attempt is not None
            or replay_segment is not None
            or replay_attempt is not None
            # Rich-only failover hands the active segment to kymo.sync; this
            # worker does not replay it until numeric failover owns the shared
            # spool circuit. Do not wake a stopped local stack for work this
            # process has deliberately handed off.
            or (numeric_spooling and spool.count)
        )

    def install_local_endpoint(endpoint) -> None:
        nonlocal local_endpoint
        nonlocal server_address, cdn_url, channel, stub, stream, connect_attempt
        nonlocal inflight_n, last_acked_cum, ack_progress_at, half_closed
        generation_changed = (
            local_endpoint is None
            or endpoint.endpoint_generation != local_endpoint.endpoint_generation
        )
        if generation_changed:
            if stream is not None:
                stream.cancel()
                stream = None
            if connect_attempt is not None:
                connect_attempt.cancel()
                connect_attempt = None
            if channel is not None:
                channel.close()
                channel = None
                stub = None
            close_rich_transport()
            inflight_n = 0
            last_acked_cum = 0
            ack_progress_at = None
            half_closed = False
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
                raw_spooled = (
                    upload_spooled.get_obj()
                    if hasattr(upload_spooled, "get_obj")
                    else upload_spooled
                )
                raw_spooled.value = int(spooled)
            if failure and upload_failure is not None:
                raw_failure = (
                    upload_failure.get_obj()
                    if hasattr(upload_failure, "get_obj")
                    else upload_failure
                )
                raw_failure.value = _merged_worker_failure(raw_failure.value, failure)
            if terminal and upload_terminal is not None:
                raw_terminal = (
                    upload_terminal.get_obj()
                    if hasattr(upload_terminal, "get_obj")
                    else upload_terminal
                )
                raw_terminal.value = 1
            raw_status = (
                queue_status.get_obj()
                if hasattr(queue_status, "get_obj")
                else queue_status
            )
            raw_status.value -= n
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
        nonlocal spool_failed
        if not tuples:
            return 0
        n = 0
        spooled_now = 0
        if spool_failed:
            n = len(tuples)
            spool_failed += n
        else:
            for index, t in enumerate(tuples):
                try:
                    written = _spill_tuple(spool, t)
                    n += written
                    spooled_now += written
                except Exception as e:
                    # A write failure is normally disk-wide (full, read-only,
                    # broken mount). Stop touching it after the first failure,
                    # but resolve the removed suffix so accounting has no owner.
                    lost = len(tuples) - index
                    _log.warning("failed to spool %d point(s): %s", lost, e)
                    n += lost
                    spool_failed += lost
                    break
        if spooled_now:
            try:
                # Accounting is a delivery proof. Flush Python's file buffer
                # before decrementing it so a later SIGTERM/SIGKILL cannot
                # erase records that the shared counter already released.
                spool.flush()
            except Exception as error:
                _log.warning(
                    "failed to flush %d spooled point(s): %s",
                    spooled_now,
                    error,
                )
                spool_failed += spooled_now
                spooled_now = 0
        resolve_points(
            n,
            spooled=True if spooled_now else None,
            failure=_WORKER_EXIT_SPOOL_FAILED if spool_failed else 0,
        )
        return n

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

    def seal_spool_for_replay() -> bool:
        """Rotate the current spool into the single FIFO replay slot.

        False means no segment is ready to replay: either nothing is spooled, or
        sealing failed and the caller must retry the rotation after a backoff.
        """
        nonlocal spool, replay_segment
        if replay_segment is not None:
            return True
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
        return True

    def start_spool_replay() -> None:
        """Start replay of the oldest sealed segment while producers spool."""
        nonlocal replay_attempt
        segment = replay_segment
        if segment is None or replay_attempt is not None:
            return

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
            delivered = replay_file(
                segment.path,
                replayed_keys=replayed_cdn_keys,
                terminal_runs=terminal_runs,
                quarantined_files=quarantined_files,
                _writer_lock_held=True,
                _local_endpoint=local_endpoint,
            )
            if terminal_runs:
                raise _TerminalRunError("run is no longer writable during spool replay")
            if quarantined_files:
                raise _ReplayQuarantined(next(iter(quarantined_files)))
            return delivered

        replay_attempt = _ThreadAttempt(segment, replay, name="kymo-spool-replay")

    def retain_queue_item(item) -> None:
        nonlocal spilled, undecodable
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
            return
        pending_spill: list[tuple] = []
        serialized_bytes = (
            len(item[2])
            if isinstance(item, tuple)
            and len(item) == 3
            and item[0] == _SERIALIZED_RICH_QUEUE_ITEM
            else 0
        )
        per_point_serialized = (
            max(1, serialized_bytes // len(point_tuples))
            if serialized_bytes and point_tuples
            else 0
        )
        for point_tuple in point_tuples:
            is_rich = point_tuple[0] in _ORDERED_CDN_KINDS
            if rich_spooling if is_rich else numeric_spooling:
                pending_spill.append(point_tuple)
                continue
            if is_rich:
                # Public galleries/metadata arrive in one serialized envelope;
                # direct versioned CDN keys use raw tuples and must retain their
                # actual string bytes or large keys bypass the rich RAM cap.
                retained_bytes = per_point_serialized or _estimate_tuple_bytes(
                    point_tuple
                )
                cdn_queue.append(point_tuple, retained_bytes)
                if (
                    len(cdn_queue) > _MAX_CDN_QUEUE
                    or cdn_queue.total_bytes > max_rich_buffer_bytes
                ):
                    count = failover_rich()
                    spilled += count
                    _log.warning(
                        "rich upload backlog exceeded its %d-entry/%d-byte cap "
                        "— failed over %d point(s) to %s",
                        _MAX_CDN_QUEUE,
                        max_rich_buffer_bytes,
                        count,
                        spool.path,
                    )
            else:
                retained_bytes = _estimate_tuple_bytes(point_tuple)
                buffer.append(point_tuple, retained_bytes)
                point_cap = (
                    min(max_buffer_points, _RECOVERY_MAX_BUFFER_POINTS)
                    if numeric_recovery_probe
                    else max_buffer_points
                )
                byte_cap = (
                    min(max_buffer_bytes, _RECOVERY_MAX_BUFFER_BYTES)
                    if numeric_recovery_probe
                    else max_buffer_bytes
                )
                if len(buffer) > point_cap or buffer.total_bytes > byte_cap:
                    count = failover_numeric()
                    spilled += count
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
            spilled += spill(pending_spill)

    def process_cdn_entry(entry: tuple, *, first_attempt: bool) -> bool:
        """Complete one CDN head entry; the owner loop applies its result."""
        kind = entry[0]
        if kind == "cdn_key_mutation":
            _, name, step, resource_id, timestamp_ms, mutation_version = entry
            return _publish_rich_mutation(
                cdn_stub,
                project_id,
                run_id,
                name,
                step,
                resource_id,
                timestamp_ms,
                mutation_version,
            )
        if kind in ("cdn_batch", "cdn_batch_mutation"):
            if kind == "cdn_batch_mutation":
                (
                    _,
                    name,
                    step,
                    items_,
                    timestamp_ms,
                    mutation_version,
                    reduced_mutation_version,
                ) = entry
            else:
                _, name, step, items_ = entry
                timestamp_ms = None
                mutation_version = None
                reduced_mutation_version = None
            return _process_cdn_batch(
                cdn_stub,
                http_client,
                cdn_url,
                project_id,
                run_id,
                name,
                step,
                items_,
                deadline_fn=past_deadline,
                send_placeholder=first_attempt,
                timestamp_ms=timestamp_ms,
                mutation_version=mutation_version,
                reduced_mutation_version=reduced_mutation_version,
            )

        if kind == "metadata_batch_mutation":
            _, name, step, metadata_obj, timestamp_ms, mutation_version = entry
        else:
            _, name, step, metadata_obj = entry
            timestamp_ms = int(time.time() * 1000)
            mutation_version = None
        try:
            encoded_manifest = metadata_manifest(metadata_obj.data)
            resource_id = _upload_to_cdn(http_client, cdn_url, encoded_manifest, "json")
            if mutation_version is None:
                point = kymo_pb2.MetricPoint(
                    metric_name=name,
                    step=step,
                    cdn_key=resource_id,
                    timestamp_ms=timestamp_ms,
                )
                if not _send_unary_point(cdn_stub, project_id, run_id, point):
                    return False
            elif not _publish_rich_mutation(
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
        except _TerminalRunError:
            raise
        except Exception as error:
            _log.warning("failed to upload metadata: %s", error)
            return False

    def reject_deleted_run(error: BaseException) -> None:
        """Stop every transport and fail queued work without creating a spool."""
        nonlocal terminal_rejected, terminal_reason
        nonlocal stream, inflight_n, last_acked_cum, ack_progress_at, half_closed
        nonlocal connect_attempt, cdn_attempt, channel, stub, cdn_channel, cdn_stub
        if terminal_rejected:
            return
        terminal_rejected = True
        terminal_reason = str(error) or "run is no longer writable"
        # Publish the irreversible lifecycle observation before transport
        # teardown: channel close/cancellation may block until the parent
        # reaches its force-kill deadline, and parent salvage must already know
        # that every remaining queue item is permanently inadmissible.
        resolve_points(0, failure=_WORKER_EXIT_RUN_DELETED, terminal=True)
        if stream is not None:
            stream.cancel()
            stream = None
        if connect_attempt is not None:
            connect_attempt.cancel()
            connect_attempt = None
        # Closing the unary channel cancels a concurrent rich publication. It
        # may have uploaded content-addressed bytes, but cannot publish a row.
        if cdn_channel is not None:
            cdn_channel.close()
            cdn_channel = None
            cdn_stub = None
        cdn_attempt = None
        if channel is not None:
            channel.close()
            channel = None
            stub = None
        inflight_n = 0
        last_acked_cum = 0
        ack_progress_at = None
        half_closed = False
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
        """Advance and publish the jittered disk-catch-up retry deadline."""
        nonlocal numeric_failovers, stream_retry_at
        numeric_failovers += 1
        delay = _recovery_retry_delay(numeric_failovers, recovery_rng)
        stream_retry_at = time.monotonic() + delay
        return delay

    def handle_break(err) -> None:
        """The stream ended other than by our clean half-close (an RpcError, or —
        `err is None` — a server close we didn't ask for). Unacked points stay in
        buffer[:inflight_n]; reset the fed cursor and re-feed from the front next
        attempt. Back off on loop cadence (no time.sleep); rebuild the channel after
        _SEND_MAX_RETRIES consecutive breaks (it may be wedged). Logging throttled."""
        nonlocal stream, inflight_n, ack_progress_at, half_closed, spilled
        nonlocal stream_breaks, stream_retry_at
        nonlocal channel, stub, last_break_warn
        if err is not None and _is_terminal_run_error(err):
            reject_deleted_run(_terminal_run_error(err))
            return
        if numeric_recovery_probe:
            # Half-open means exactly one transport attempt: if it ends before
            # proving progress, put its retained probe suffix back on disk and
            # require another fresh-channel attempt. This prevents a READY TCP
            # endpoint with a broken ingest path from accumulating the normal
            # 2M-point window or spinning on a stale channel.
            count = failover_numeric()
            spilled += count
            now = time.monotonic()
            if now - last_break_warn > 30.0:
                last_break_warn = now
                detail = (
                    "server closed the stream"
                    if err is None
                    else (err.details() if hasattr(err, "details") else str(err))
                )
                _log.warning(
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
        now = time.monotonic()
        if now - last_break_warn > 30.0:
            last_break_warn = now
            code = err.code() if hasattr(err, "code") else None
            if code == grpc.StatusCode.UNIMPLEMENTED:
                # A version mismatch must degrade like an outage, never masquerade as a flake.
                _log.error(
                    "the kymo server does not implement IngestMetricsBidi — it predates the "
                    "pipelined-ingest server revision. Upgrade the server. Until then points "
                    "accumulate and spool at shutdown (replay: python -m kymo.sync). No data is lost."
                )
            else:
                detail = (
                    "server closed the stream"
                    if err is None
                    else (err.details() if hasattr(err, "details") else str(err))
                )
                _log.warning("ingest stream broke (%s) — retrying", detail)
        # After enough consecutive breaks the channel itself may be dead — rebuild it.
        if stream_breaks >= _SEND_MAX_RETRIES:
            if channel:
                channel.close()
            channel = None
            stub = None
            stream_breaks = 0
        if local_endpoint is not None:
            if channel is not None:
                channel.close()
            channel = None
            stub = None
            stream_breaks = 0
            mark_local_transport_stale()

    def failover_numeric() -> int:
        """Move the retained numeric lane behind an ordered disk prefix.

        While the circuit is open, newly dequeued points append to the active
        spool. Recovery seals and replays segments FIFO; live bidi cannot reopen
        until every older segment has committed, so offline replay can never
        overwrite a newer live suffix from this worker.
        """
        nonlocal numeric_spooling, numeric_recovery_probe, replay_proved_circuit
        nonlocal stream, inflight_n, last_acked_cum
        nonlocal ack_progress_at, half_closed, connect_attempt
        nonlocal channel, stub, stream_retry_at, connect_retry_delay
        if numeric_spooling:
            return 0
        numeric_spooling = True
        numeric_recovery_probe = False
        replay_proved_circuit = False
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
        if channel is not None:
            channel.close()
            channel = None
            stub = None
        mark_local_transport_stale()
        inflight_n = 0
        last_acked_cum = 0
        ack_progress_at = None
        half_closed = False
        connect_retry_delay = _CONNECT_INITIAL_DELAY
        schedule_recovery_retry()
        count = spill(buffer.items)
        buffer.clear()
        return count

    def failover_rich() -> int:
        """Move the whole ordered rich lane to disk and keep it there."""
        nonlocal rich_spooling, cdn_attempt, cdn_fails, cdn_backoff_until
        if rich_spooling:
            return 0
        rich_spooling = True
        # The daemon helper owns no accounting; ignore a late result after its
        # retained head has moved to the spool.
        cdn_attempt = None
        cdn_fails = 0
        cdn_backoff_until = 0.0
        count = spill(cdn_queue.items)
        cdn_queue.clear()
        return count

    def advance_after_data_loss(
        error: BaseException, *, quarantined_path: str = ""
    ) -> None:
        """Quarantine one bad segment and keep later delivery eligible."""
        nonlocal spilled
        if quarantined_path:
            quarantined_spool_paths.append(quarantined_path)
        else:
            # A live rich head has no spool to rename yet. Move both lanes into
            # the existing FIFO circuit; replay will quarantine the segment
            # containing this head, then advance to newer segments.
            spilled += failover_numeric() + failover_rich()
            close_rich_transport()
        resolve_points(0, failure=_WORKER_EXIT_DATA_LOSS)
        if quarantined_path:
            _log.error(
                "authoritative DATA_LOSS quarantined one ordered spool segment: "
                "%s. Later segments remain eligible for delivery",
                error,
            )
        else:
            _log.error(
                "authoritative DATA_LOSS moved the live ordered head to the "
                "spool quarantine path: %s",
                error,
            )

    def accept_queue_item(item) -> None:
        nonlocal shutdown, input_closed
        if item is None:
            shutdown = True
            input_closed = True
        else:
            retain_queue_item(item)

    def drain_to_spool_after_deadline() -> None:
        """Move owned work to disk, then drain every new queue item there."""
        nonlocal spilled, drain_incomplete
        # Put both lanes into their one-way spool mode before touching the
        # producer queue. Each subsequently dequeued group is flushed before
        # its accounting is released, minimizing worker-private state that
        # owner-fenced salvage cannot reach.
        spilled += failover_numeric() + failover_rich()
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
        try:
            item = metric_queue.get(timeout=poll_timeout)
        except queue.Empty:
            drained_to_empty = True
        else:
            accept_queue_item(item)
            drained_items = 1
        if past_deadline():
            drain_to_spool_after_deadline()
            break
        while drained_items < _DRAIN_MAX_ITEMS_PER_CYCLE and not past_deadline():
            try:
                item = metric_queue.get_nowait()
            except queue.Empty:
                drained_to_empty = True
                break
            accept_queue_item(item)
            drained_items += 1
        if past_deadline():
            drain_to_spool_after_deadline()
            break

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
                        spilled += failover_numeric() + failover_rich()
                        local_refresh_required = False
                        _log.error(
                            "%s; retaining new points in the old installation's spool",
                            result,
                        )
                    else:
                        local_ensure_retry_at = time.monotonic() + _equal_jitter_delay(
                            local_ensure_retry_delay, recovery_rng
                        )
                        local_ensure_retry_delay = _next_connect_retry_delay(
                            local_ensure_retry_delay
                        )
                        now = time.monotonic()
                        if now - last_break_warn > 30.0:
                            last_break_warn = now
                            _log.warning("local runtime ensure failed: %s", result)

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
                if replay_succeeded and bool(replay_result):
                    # Delivered: release the descriptor, never re-attempt its
                    # barrier here. This loop is the queue's only consumer.
                    completed_segment.release()
                    replay_segment = None
                    numeric_failovers = 0
                    replay_proved_circuit = True
                    stream_retry_at = time.monotonic()
                    # Publish an empty disk prefix here, not on the live-resume
                    # path below: a shutdown that arrives during this replay
                    # closes the connect gate, and the owner would otherwise
                    # report spooled data that has in fact been delivered.
                    if spool.count == 0:
                        resolve_points(0, spooled=False)
                    _log.info(
                        "replayed sealed upload spool %s; checking for a newer segment",
                        completed_segment.path,
                    )
                else:
                    replay_proved_circuit = False
                    error = (
                        replay_result
                        if not replay_succeeded
                        else RuntimeError("spool replay did not complete")
                    )
                    if isinstance(error, _TerminalRunError):
                        # Headed for retirement; durability would buy nothing.
                        completed_segment.release()
                        replay_segment = None
                        reject_deleted_run(error)
                    elif isinstance(error, _ReplayQuarantined):
                        completed_segment.release()
                        replay_segment = None
                        numeric_failovers = 0
                        stream_retry_at = time.monotonic()
                        advance_after_data_loss(error, quarantined_path=error.path)
                        if spool.count == 0:
                            resolve_points(0, spooled=False)
                    else:
                        if local_endpoint is not None:
                            # A restarted local server reuses its socket path but
                            # rotates the bearer. A READY UDS channel therefore
                            # does not prove that replay authenticated; force an
                            # endpoint refresh before retrying this disk prefix.
                            if channel is not None:
                                channel.close()
                            channel = None
                            stub = None
                            close_rich_transport()
                            mark_local_transport_stale()
                        delay = schedule_recovery_retry()
                        now = time.monotonic()
                        if now - last_break_warn > 30.0:
                            last_break_warn = now
                            _log.warning(
                                "ordered spool replay failed (%s) — retrying in %.1fs",
                                error,
                                delay,
                            )

        # ---- bidi ingest: (re)open a stream, then apply acks ----
        # Reconnect a dead channel first so a stream can open. `stub` is None only
        # after a channel rebuild failed (initial connect, or _SEND_MAX_RETRIES breaks).
        if connect_attempt is not None and (
            (shutdown and (numeric_spooling or not buffer))
            or (local_endpoint is not None and not has_pending_delivery())
        ):
            connect_attempt.cancel()
            connect_attempt = None
        if (
            stub is None
            and connect_attempt is None
            and not terminal_rejected
            and replay_attempt is None
            # An open numeric circuit still connects in the background while
            # an empty active spool proves there is no older disk prefix.
            and (not shutdown or (not numeric_spooling and buffer))
            and (local_endpoint is None or has_pending_delivery())
            and not local_refresh_required
            and not local_identity_mismatch
            and local_ensure_attempt is None
            and not past_deadline()
            and time.monotonic() >= stream_retry_at
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
            connect_result = connect_attempt.poll(deadline_fn=past_deadline)
        else:
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
                if numeric_spooling:
                    schedule_recovery_retry()
                else:
                    stream_retry_at = time.monotonic() + _equal_jitter_delay(
                        connect_retry_delay, recovery_rng
                    )
                    connect_retry_delay = _next_connect_retry_delay(connect_retry_delay)
            else:
                connect_retry_delay = _CONNECT_INITIAL_DELAY
                if numeric_spooling:
                    # Queue draining precedes this poll, so no sealed segment and
                    # an empty active spool together prove the disk prefix is
                    # gone. Anything else replays first — including a rotation
                    # that could not be sealed, which must not be stepped over.
                    if replay_segment is not None or spool.count:
                        channel.close()
                        channel = None
                        stub = None
                        if seal_spool_for_replay():
                            start_spool_replay()
                    else:
                        numeric_spooling = False
                        numeric_recovery_probe = True
                        rich_spooling = False
                        numeric_failovers = 0
        if (
            stream is None
            and not numeric_spooling
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
                        resolve_points(delta)
                        if numeric_recovery_probe:
                            numeric_recovery_probe = False
                            numeric_failovers = 0
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
            and not numeric_spooling
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
                    spilled += failover_numeric()
                    break
                # n == 0 is a defensive no-op (a non-empty slice yields n≥1 or raises above); the live break reason is a full feed queue — retry next cycle.
                if n == 0 or not stream.feed(batch):
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

        # ---- process CDN batches (uploads + manifests) ----
        # A daemon helper may block in encoding/httpx/unary gRPC, but owns no
        # delivery accounting. This loop alone retains and mutates the ordered
        # queue head, so numeric feeding and ACK harvesting remain independent.
        if cdn_attempt is not None:
            try:
                succeeded, result = cdn_attempt.events.get_nowait()
            except queue.Empty:
                pass
            else:
                entry = cdn_attempt.item
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
                        entry[1],
                        entry[2],
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
                            "— spooling for kymo.sync",
                            entry[1],
                            entry[2],
                            cdn_fails,
                        )
                        spilled += failover_rich()

        if (
            cdn_attempt is None
            and not terminal_rejected
            and not rich_spooling
            and cdn_queue
            and cdn_url
            and not local_refresh_required
            and not local_identity_mismatch
            and local_ensure_attempt is None
            and time.monotonic() >= cdn_backoff_until
            and not past_deadline()
        ):
            ensure_rich_transport()
            entry = cdn_queue.items[0]
            first_attempt = cdn_fails == 0
            cdn_attempt = _ThreadAttempt(
                entry,
                lambda entry=entry, first_attempt=first_attempt: process_cdn_entry(
                    entry, first_attempt=first_attempt
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
            # Shutdown closed the connect gate, so no connect probe can resume the disk FIFO for records spilled behind an in-flight replay (e.g. the owner's final captured-output flush). A replay that just succeeded is the circuit proof instead: keep sealing and replaying the tail until the disk is empty, a replay fails, or the deadline passes — never exit stranding a deliverable tail as a fresh pending spool.
            if (
                numeric_spooling
                and spool.count
                and replay_proved_circuit
                and not terminal_rejected
                and not past_deadline()
                and seal_spool_for_replay()
            ):
                start_spool_replay()
                continue
            break

    if stream is not None:
        stream.cancel()  # idempotent; releases the gen/pump threads if we exited on a clean done
    if connect_attempt is not None:
        connect_attempt.cancel()
    if http_client:
        http_client.close()
    if cdn_channel:
        cdn_channel.close()
    if channel:
        channel.close()
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
            spooled=bool(pending_spool_files or quarantined_spool_paths),
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
            "worker quarantined spool segment(s) after DATA_LOSS while later "
            "segments remained eligible for delivery: %s",
            ", ".join(quarantined_spool_paths),
        )
    if spool_failed or undecodable:
        _log.error(
            "%d points reached neither the server nor the spool (%d unspoolable "
            "— disk full? — and %d undecodable) — LOST",
            spool_failed + undecodable,
            spool_failed,
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
