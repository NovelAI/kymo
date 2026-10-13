"""Background upload process for kymo."""

import math
import multiprocessing
import multiprocessing.queues
import os
import queue
import random
import select
import signal
import struct
import sys
import threading
import time
from multiprocessing.reduction import ForkingPickler
from typing import Optional

import grpc

from kymo._cdn import metadata_manifest
from kymo._env import integer as _env_integer
from kymo._env import number as _env_number
from kymo._generated import kymo_pb2, kymo_pb2_grpc
from kymo._log import logger as _log
from kymo.spool import SpoolWriteCut, replay_command


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


# When the worker stops delivering, and when it stops spilling to close its spool and exit by itself before SIGALRM's default action kills it (time.monotonic(), inf = never). A signal kymo owns or the parent's handoff sets them (_own_signals); they only come forward, and the parent's writes cannot move them.
_signal_deadline = math.inf
_spill_by = math.inf
# The parent's handoff cap (kymo.client._handoff) as _own_signals got it: once set, no salvage of the parent's follows the worker.
_handoff = None
# Once the parent has died, a Ctrl-C the worker saw may bound it as a handoff would (_own_signals); None without the worker's signal policy.
_hand_off_after_ctrl_c = None


class _QueueReads:
    """The worker's reads of its producer queue, each bounded by the time it is given and resumed across calls.

    Queue.get can block forever once the parent's feeder dies partway through a message. So a message's frame (a 4-byte length, 8 more past 2 GiB, then the pickle) is read as far as it has arrived, and unpickled only once whole. The reader lock stays held while a frame is partial, even after a read fails: if the worker exits then, the parent's salvage finds it held and skips the queue rather than read the rest as a new frame. An idle queue is polled without the lock, so a worker killed there cannot strand it. A source without that framing (a test's queue.Queue) is read with its own get.
    """

    def __init__(self, source):
        self.source = source
        self.reader = None
        if isinstance(source, multiprocessing.queues.Queue):
            self.reader = source._reader
            self.poller = select.poll()
            self.poller.register(self.reader.fileno(), select.POLLIN)
        # The message read so far, frame header included; this holds the queue's reader lock while it is not empty.
        self.frame = bytearray()

    def get(self, timeout: float = 0.0):
        """The next item, waiting at most timeout seconds; queue.Empty if no whole message has arrived by then."""
        if self.reader is None:
            return self.source.get(timeout=timeout)
        source = self.source
        end = time.monotonic() + timeout
        if not self.frame and not (
            self.poller.poll(timeout * 1000)
            and source._rlock.acquire(timeout > 0, max(0.0, end - time.monotonic()))
        ):
            raise queue.Empty
        try:
            payload = self._read(end)
            source._sem.release()
        finally:
            # A message read in part, by a read that timed out or failed, keeps its frame and the lock: the next read goes on from there, and the worker reports it if it exits with it.
            if not self.frame:
                source._rlock.release()
        try:
            return ForkingPickler.loads(payload)
        except Exception as error:
            raise OSError("malformed upload queue message") from error

    def _read(self, end: float) -> memoryview:
        """Read the current message's frame until it is whole, returning its pickle, or until end (time.monotonic()) passes (queue.Empty), never reading past it."""
        self._fill(4, end)
        header, (size,) = 4, struct.unpack_from("!i", self.frame)
        if size == -1:
            header = 12
            self._fill(header, end)
            (size,) = struct.unpack_from("!Q", self.frame, 4)
        elif size < 0:
            raise OSError(f"malformed upload queue message length {size}")
        self._fill(header + size, end)
        frame, self.frame = self.frame, bytearray()
        return memoryview(frame)[header:]

    def _fill(self, length: int, end: float) -> None:
        """Read until the frame holds length bytes, or until end passes (queue.Empty): checked after each read, since a pipe that stays readable never makes the poll wait."""
        while len(self.frame) < length:
            if not self.poller.poll(max(0.0, end - time.monotonic()) * 1000):
                raise queue.Empty
            # Never at end of file: the worker holds a write end of the pipe itself.
            self.frame += os.read(self.reader.fileno(), length - len(self.frame))
            if len(self.frame) < length and time.monotonic() >= end:
                raise queue.Empty


def _worker_process(*args, handoff, owned, started, interrupted, exited):
    """The upload worker process: it marks itself started, before it reads the queue (kymo.client._kill_unstarted_worker), then kymo's signal policy (_own_signals), then the worker (_upload_worker, with args). As it ends it publishes the status it exits with in exited, which the parent reads where multiprocessing no longer sees it (kymo.client._worker_alive)."""
    started.value = 1
    status = 1
    try:
        _own_signals(handoff, owned, interrupted)
        _upload_worker(*args)
        status = 0
    except SystemExit as error:
        # As multiprocessing's bootstrap turns it into the exit code.
        status = (
            error.code if isinstance(error.code, int) else int(error.code is not None)
        )
        raise
    finally:
        exited.value = status


def _own_signals(handoff, owned, interrupted) -> None:
    """Install the worker's signal policy, then unblock the shutdown signals and the other group signals (kymo.client._GROUP_SIGNALS), which the parent blocks across Process.start() under fork and spawn so none arrives first; the worker ignores the latter.

    A Ctrl-C, which reaches the whole group and the parent handles, the worker only notes in interrupted (time.monotonic()) and carries on, unless the program ignores SIGINT (as a shell's background job starts), when kymo installs no handler either and the worker ignores it too. The parent reads the note for a Ctrl-C a handler installed after init() kept from kymo's (kymo.client._ctrl_c_at); once the parent has died (SIGINT's default action, say), a Ctrl-C within the handoff's cap bounds the worker by that cap, as a handoff would (_hand_off_after_ctrl_c). A SIGTERM or SIGQUIT kymo owns (owned, kymo.client._owned_signals) finishes it within the signal budget (finish_by): it delivers until _signal_deadline, spills until _spill_by and exits, SIGALRM's default action killing it at the budget's end only if a system call wedged it; one the program ignores or handles leaves it running, and the program's exit stops it as usual. A handoff puts the worker's end in handoff, which replaces the budget's, then sends a SIGTERM, which the worker obeys whoever owns SIGTERM (kymo.client._Shutdown.hand_off).

    A signal can land while finish_by handles another, on the main thread between its bytecodes even with the signals blocked there, since another thread then receives them: the updates run one at a time (updating), and one that lands during another has that one run again, so the deadlines and the alarm only ever come forward.
    """
    from kymo.client import (
        _CLEANUP_RESERVE_MIN,
        _GROUP_SIGNALS,
        _INTERRUPT_FLUSH_TIMEOUT,
        _TERMINATION_SIGNALS,
        _signal_budget,
        _worker_window,
    )

    global _handoff, _hand_off_after_ctrl_c
    _handoff = handoff
    budget = _signal_budget()
    updating = threading.Lock()
    # Non-empty while an update is owed: a signal landing while update() runs has it run again.
    owed = []

    def finish_by(signum, frame) -> None:
        if not handoff.value and signum not in owned:
            return
        owed.append(signum)
        while owed and updating.acquire(blocking=False):
            try:
                owed.clear()
                update()
            finally:
                updating.release()

    def update() -> None:
        global _signal_deadline, _spill_by
        now = time.monotonic()
        end = handoff.value or now + budget
        # As the parent's shutdown plans it (kymo.client._Shutdown.plan_worker).
        deliver_by, spill_by, *_ = _worker_window(now, end)
        _signal_deadline = min(_signal_deadline, deliver_by)
        _spill_by = min(_spill_by, spill_by)
        armed = signal.getitimer(signal.ITIMER_REAL)[0]
        if not 0.0 < armed <= end - now:
            signal.setitimer(signal.ITIMER_REAL, max(end - now, 1e-3))

    def note_ctrl_c(signum, frame) -> None:
        interrupted.value = time.monotonic()

    def hand_off_after_ctrl_c() -> None:
        """Once the parent has died: if a Ctrl-C came within the handoff's cap (kymo.client._interrupt_budget), hand the worker the run's remaining delivery as the parent's handoff would, by that cap but never one already past, and arm it."""
        cap = min(_INTERRUPT_FLUSH_TIMEOUT, budget)
        now = time.monotonic()
        if interrupted.value and interrupted.value + cap > now:
            end = max(interrupted.value + cap, now + _CLEANUP_RESERVE_MIN)
            if not handoff.value or end < handoff.value:
                handoff.value = end
            finish_by(signal.SIGTERM, None)
            _log.warning(
                "parent process exited after a Ctrl-C — delivering what it can "
                "within %.1fs, then spooling the rest",
                max(0.0, handoff.value - time.monotonic()),
            )

    _hand_off_after_ctrl_c = hand_off_after_ctrl_c
    for signum in _TERMINATION_SIGNALS:
        if signum != signal.SIGINT:
            signal.signal(signum, finish_by)
        elif signal.getsignal(signum) is not signal.SIG_IGN:
            signal.signal(signum, note_ctrl_c)
    # A handler a fork inherited from the trainer must not catch the alarm, nor a mask the trainer blocked it in, which fork and spawn pass on, hold it.
    signal.signal(signal.SIGALRM, signal.SIG_DFL)
    # Nor may a group's SIGUSR1, SIGUSR2 or SIGHUP, which the trainer may handle, kill the worker or run that handler here; one sent before this policy is discarded.
    for signum in _GROUP_SIGNALS:
        signal.signal(signum, signal.SIG_IGN)
    signal.pthread_sigmask(
        signal.SIG_UNBLOCK, (*_TERMINATION_SIGNALS, *_GROUP_SIGNALS, signal.SIGALRM)
    )


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
    spill_deadline=None,
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
       with a 30s deadline instead of retrying forever, or by the handoff's cap
       of a Ctrl-C the worker saw (_hand_off_after_ctrl_c).
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
        _UnpacedGalleryItems,
        _CONNECT_INITIAL_DELAY,
        _CONNECT_PENDING,
        _CONNECT_POLL_TIMEOUT,
        _ConnectionAttempt,
        _DEFAULT_MAX_BUFFER_BYTES,
        _DEFAULT_MAX_BUFFER_POINTS,
        _DRAIN_MAX_ITEMS_PER_CYCLE,
        _QUEUE_DRAIN_SILENCE,
        _EncodedItems,
        _FEED_Q_CAP,
        _IDLE_POLL_TIMEOUT,
        _MAX_CDN_QUEUE,
        _DEFAULT_MAX_RICH_BUFFER_BYTES,
        _MAX_UNACKED_POINTS,
        _NUMERIC_OUTAGE_FAILOVER,
        _ORDERED_CDN_KINDS,
        _GALLERY_KINDS,
        _RECOVERY_MAX_BUFFER_BYTES,
        _RECOVERY_MAX_BUFFER_POINTS,
        _RetainedLane,
        _SERIALIZED_RICH_QUEUE_ITEM,
        _STATUS_LOCK_TIMEOUT,
        _ThreadAttempt,
        _WORKER_EXIT_DATA_LOSS,
        _WORKER_GALLERY_CUT_SHORT,
        _WORKER_EXIT_DRAIN_INCOMPLETE,
        _WORKER_EXIT_RUN_DELETED,
        _WORKER_EXIT_SPOOL_FAILED,
        _cdn_retry_delay,
        _decode_queue_item,
        _encode_cdn_items,
        _equal_jitter_delay,
        _estimate_tuple_bytes,
        _merged_worker_failure,
        _next_connect_retry_delay,
        _process_cdn_batch,
        _publish_manifest_point,
        _publish_rich_mutation,
        _queue_item_size,
        _recovery_retry_delay,
        _retire_rejected_worker_spools,
        _spill_tuple,
        _spool_header,
        _upload_to_cdn,
        _validated_ack_delta,
        SpoolWriter,
        make_spool_path,
    )
    from kymo._wire import (
        _MAX_POINTS_PER_MSG,
        _PermanentPointError,
        _RichMutationDataLoss,
        _SEND_INITIAL_DELAY,
        _SEND_MAX_DELAY,
        _SEND_MAX_RETRIES,
        _TerminalRunError,
        _chunk_tuples,
        _is_terminal_run_error,
        _terminal_run_error,
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

    def spill_end() -> float:
        """When spilling stops, to close the spool and exit by itself before it is killed: a signal's or the handoff's end (_spill_by), or the parent's shutdown's (spill_deadline, 0.0 = none), whichever is sooner. Either can come forward, so it is read again at every step."""
        parent = spill_deadline.value if spill_deadline is not None else 0.0
        return min(_spill_by, parent) if parent > 0.0 else _spill_by

    def new_spool(path: str) -> SpoolWriter:
        """A spool segment at path, whose writer cuts off whole a record the spill end catches being written."""
        return SpoolWriter(
            path,
            header=_spool_header(
                server_address,
                project_id,
                run_id,
                run_name,
                cdn_url,
                session,
                local_installation_uuid,
            ),
            until=spill_end,
        )

    spool = new_spool(
        spool_path or make_spool_path(project_id, run_id, "worker", session=session)
    )
    spool_directory = os.path.dirname(os.path.abspath(spool.path))
    recovery_rng = random.Random((os.getpid() << 64) ^ time.time_ns())
    orphan_deadline = 0.0
    creator_pid = owner_pid if owner_pid is not None else os.getppid()
    queue_reads = _QueueReads(metric_queue)
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
        if _signal_deadline <= time.monotonic():
            return True
        if shutdown_deadline is None:
            return False
        d = shutdown_deadline.value
        return d > 0.0 and time.monotonic() >= d

    def await_attempt(attempt: _ThreadAttempt) -> Optional[tuple]:
        """A helper's outcome (succeeded, result), waited for in steps until the spill end (spill_end()); None once that has passed."""
        while True:
            remaining = spill_end() - time.monotonic()
            if remaining <= 0:
                return None
            try:
                return attempt.events.get(timeout=min(remaining, _ACTIVE_POLL_TIMEOUT))
            except queue.Empty:
                pass

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
    # The segment the deadline spill sealed ahead of its first rich record, which gets one of its own (spill), closed and reported at exit.
    sealed_spools: list[SpoolWriter] = []
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
    queue_lost = False  # the queue can no longer be read (read_queue)
    total_sent = 0
    spilled = 0
    spool_failed = 0  # points lost to a failed spool write; nonzero also stops spill() from writing again
    cut = 0  # points left unspilled once the spill end passed (spill_end())
    undecodable = 0  # queued points dropped because their snapshot would not unpickle; kept out of spool_failed, which also stops spill() from writing
    unencodable = 0  # points dropped because their spool record could not be built (a gallery too large to encode); the spool itself is still sound
    # Galleries published without items their encode dropped or the CDN rejected.
    cut_short = 0
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
    # Encodes a rich failover did not wait for (failover_rich): one that fails counts its gallery as no longer raw only once this loop sees it fail.
    abandoned_encodes: list[_ThreadAttempt] = []
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

    def publish_lanes_on_disk(writing: bool = False) -> None:
        """Tell log() whether either lane writes new work to disk, or this loop is about to write (writing); it then never waits for the encoder, since disk writes would set the pace."""
        if lanes_on_disk is not None:
            lanes_on_disk.value = int(writing or _TO_DISK in (numeric_lane, rich_lane))

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

    def end_stream() -> None:
        """Cancel the stream, if any, and forget its in-flight window: its unacked points stay at the front of buffer for the next stream."""
        nonlocal stream, inflight_n, last_acked_cum, ack_progress_at, half_closed
        if stream is not None:
            stream.cancel()
            stream = None
        inflight_n = 0
        # The server's cumulative counter restarts at 0 on every stream, so last_acked_cum MUST restart with it or the next stream's first delta is wild.
        last_acked_cum = 0
        ack_progress_at = None
        half_closed = False

    def drop_numeric_transport() -> None:
        """End the stream, cancel any connect, and close the bidi channel."""
        nonlocal connect_attempt
        # Cancel before releasing the in-flight prefix's accounting. A racing
        # commit may duplicate on replay, but cannot ACK and decrement twice.
        end_stream()
        if connect_attempt is not None:
            connect_attempt.cancel()
            connect_attempt = None
        # A channel can remain READY briefly while its dead HTTP/2 transport is
        # being torn down. Recovery must not immediately reuse that stale
        # channel and declare the circuit half-open without a real reconnect.
        close_numeric_channel()

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
                upload_spooled.get_obj().value = int(spooled)
            if failure and upload_failure is not None:
                raw_failure = upload_failure.get_obj()
                raw_failure.value = _merged_worker_failure(raw_failure.value, failure)
            if terminal and upload_terminal is not None:
                upload_terminal.get_obj().value = 1
            queue_status.get_obj().value -= n
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

    def spill(tuples: list[tuple], encoding: Optional[tuple] = None) -> int:
        """Spool tuples, returning how many it wrote, which it adds to spilled. encoding is a gallery the encoder helper still encodes, which counts itself as no longer raw once its encode ends. A gallery it writes without items its encode dropped is cut short, as one published so would be."""
        nonlocal spilled, spool_failed, unencodable, cut, cut_short
        if not tuples:
            return 0
        if (
            tuples[0][0] in _ORDERED_CDN_KINDS
            and spool.count
            and not sealed_spools
            and past_deadline()
        ):
            # The deadline spill's first rich record behind others, from the rich lane or the producer queue, starts a segment of its own: a kill the spill end cannot foresee (the container's, an OOM kill, SIGALRM behind a wedged system call) that tears one then costs only that segment, never the records sealed before it, which kymo.sync would quarantine with it.
            try:
                sealed_spools.append(rotate_spool())
            except OSError as error:
                _log.error("failed to seal upload spool %s: %s", spool.path, error)
        # Written, unencodable, cut or lost: none of them stays raw in this worker.
        raw_galleries = sum(map(counted_raw, tuples)) - (
            encoding is not None and counted_raw(encoding)
        )
        spooled_now = cut_now = reduced = 0
        if spool_failed:
            spool_failed += len(tuples)
        else:
            held = spool.count
            unwritten = 0
            error = None
            for index, t in enumerate(tuples):
                gallery = t[0] in _GALLERY_KINDS
                # Checked before each gallery and every 512th point, and only a gallery is asked whether it awaits encoding or was cut short: those checks on every point made a numeric spill take 17% longer. The writer still cuts a record the end catches it sending (SpoolWriter.until), so what goes out past the end stays within the megabyte it buffers.
                if (gallery or not index & 511) and spill_end() <= time.monotonic():
                    # The spill end has passed: leave the rest charged and lost, and exit by itself.
                    cut_now = len(tuples) - index
                    break
                try:
                    if gallery and awaits_encoding(t):
                        if index:
                            # What this spill wrote reaches the file before a slow encode, so a kill during it loses none of it.
                            spool.flush()
                        if spill_end() < math.inf:
                            # Encoded on a helper, waited for until the spill end, so that end cuts this gallery and the rest rather than a kill cutting a record short.
                            outcome = await_attempt(
                                _ThreadAttempt(
                                    t,
                                    lambda t=t: _encode_cdn_items(t[1], t[2], t[3]),
                                    name="kymo-spill-encode",
                                )
                            )
                            if outcome is None:
                                cut_now = len(tuples) - index
                                break
                            succeeded, result = outcome
                            if not succeeded:
                                _log.error(
                                    "could not encode %r for the spool — 1 point LOST: %r",
                                    t[:3],
                                    result,
                                )
                                unwritten += 1
                                continue
                            t = _encoded_entry(t, result)
                    if not _spill_tuple(spool, t):
                        unwritten += 1
                    elif gallery and spooled_cut_short(t):
                        reduced += 1
                except SpoolWriteCut:
                    # The spill end passed as it wrote this record, which the writer cut off.
                    cut_now = len(tuples) - index
                    break
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
            cut += cut_now
            cut_short += reduced
            lost = len(tuples) - cut_now - spooled_now - unwritten
            if lost:
                _log.warning("failed to spool %d point(s): %s", lost, error)
                spool_failed += lost
        # Those cut stay charged: the worker exits with them reported lost.
        resolve_points(
            len(tuples) - cut_now,
            spooled=True if spooled_now else None,
            failure=_WORKER_EXIT_SPOOL_FAILED
            if spool_failed or unencodable
            else _WORKER_GALLERY_CUT_SHORT
            if reduced
            else 0,
        )
        release_raw_galleries(raw_galleries)
        spilled += spooled_now
        # Only points written, which spilled counts and the failover warnings report; losses are counted apart.
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

    def rotate_spool() -> SpoolWriter:
        """Seal the active spool, keeping its writer lock, and write on to a new segment; return the sealed one. OSError if sealing failed, writing on to the active spool."""
        nonlocal spool
        spool.seal()
        sealed, spool = (
            spool,
            new_spool(
                make_spool_path(
                    project_id,
                    run_id,
                    "worker",
                    spool_dir=spool_directory,
                    session=session,
                )
            ),
        )
        return sealed

    def start_spool_replay() -> bool:
        """Replay the oldest sealed segment in a daemon helper while producers spool, first rotating the active spool into the single FIFO replay slot if that slot is empty.

        False means nothing started: delivery may not start (see may_start_transport), nothing is spooled, or sealing failed and the next attempt waits out a backoff.
        """
        nonlocal replay_segment, replay_attempt, replay_can_prove
        nonlocal numeric_lane, rich_lane
        if not may_start_transport():
            return False
        if replay_segment is None:
            if spool.count == 0:
                return False
            try:
                replay_segment = rotate_spool()
            except OSError as error:
                # The unflushed tail is gone, and its accounting was already released
                # as spooled — record the loss, but keep this worker draining its
                # queue instead of dying on a broken mount.
                _log.error("failed to seal upload spool %s: %s", spool.path, error)
                resolve_points(0, failure=_WORKER_EXIT_SPOOL_FAILED)
                schedule_recovery_retry()
                return False
        segment = replay_segment

        def replay() -> tuple[bool, int]:
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
            reduced_galleries: list[tuple[str, int]] = []
            try:
                # Delete consumed segments: their records were delivered, superseded, or on an old server skipped as older than the server's.
                delivered = replay_file(
                    segment.path,
                    delete=True,
                    replayed_keys=replayed_cdn_keys,
                    terminal_runs=terminal_runs,
                    quarantined_files=quarantined_files,
                    reduced_galleries=reduced_galleries,
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
            # Whether it delivered the segment, and how many galleries it published without items the CDN rejected.
            return delivered, len(reduced_galleries)

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
        return entry[0] in _GALLERY_KINDS and not isinstance(entry[3], _EncodedItems)

    def counted_raw(entry: tuple) -> bool:
        """A gallery still raw that the initializing process counted: one log()'s encoder wait waits on, unlike a fork child's."""
        return awaits_encoding(entry) and not isinstance(entry[3], _ChildGalleryItems)

    def spooled_cut_short(entry: tuple) -> bool:
        """Whether entry, a gallery just spooled, was written without items its encode dropped: the encoder's _EncodedItems says so, and a spool record's encode replaces a dropped item with None in place (_encode_cdn_items)."""
        items = entry[3]
        return items.dropped if isinstance(items, _EncodedItems) else None in items

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
        # A serialized envelope holds exactly one rich point, which is charged its size.
        serialized = isinstance(item, tuple) and item[0] == _SERIALIZED_RICH_QUEUE_ITEM
        try:
            point_tuples = _decode_queue_item(item)
        except Exception as error:
            # Like parent salvage, drop just this item: a snapshot that only unpickles in the trainer (a non-tensor CUDA object, or a class the worker cannot import) must not stop every later delivery. It is lost, so it fails delivery with the spool-failure code.
            count = _queue_item_size(item)
            if not undecodable:
                _log.error(
                    "failed to decode a queued item — dropping %d point(s); later decode losses are summarized at exit: %s",
                    count,
                    error,
                )
            undecodable += count
            resolve_points(count, failure=_WORKER_EXIT_SPOOL_FAILED)
            if serialized:
                # A rich snapshot that will not unpickle may be a gallery's, the only kind that holds a tensor. If it was metadata or a fork child's gallery, this release was never counted, which only lets a later log() wait less. A numeric batch whose lists are out of step (zip strict) holds none.
                release_raw_galleries(1)
            return
        pending_spill: list[tuple] = []
        serialized_bytes = len(item[1]) if serialized else 0
        for point_tuple in point_tuples:
            is_rich = point_tuple[0] in _ORDERED_CDN_KINDS
            unversioned_rich |= point_tuple[0] in ("cdn_batch", "metadata_batch")
            if (rich_lane if is_rich else numeric_lane) == _TO_DISK:
                pending_spill.append(point_tuple)
                continue
            if is_rich:
                # A gallery waiting for the encoder is charged only its bytes-backed items, since log()'s encoder wait bounds how many do; its arrays are charged their encoded bytes once encoded.
                # A gallery log() did not pace (marked _UnpacedGalleryItems: a fork child's, or one published while a lane wrote to disk) is charged whole until then, as is any while encodes are failing (memory pressure), when nothing will shrink it soon.
                if (
                    not encode_fails
                    and awaits_encoding(point_tuple)
                    and not isinstance(point_tuple[3], _UnpacedGalleryItems)
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

    def process_cdn_entry(entry: tuple, *, first_attempt: bool) -> bool | int:
        """Complete one CDN head entry; the owner loop applies its result (_process_cdn_batch's for a gallery)."""
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
        if kind in _GALLERY_KINDS:
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
        # The owner loop handles what this raises: a terminal answer, DATA_LOSS, or a failure it logs and retries.
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
        end_stream()
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
        # Spool the encoded entries ahead of an encode in flight, then wait for that encode rather than repeat it on this thread, never past the spill end, unless the spool has failed and would write nothing.
        # The encoder encodes in place, so spilling that gallery only resumes an encode that raised.
        if encode_attempt is None:
            count = spill(cdn_queue.items)
        else:
            index = next(
                i for i, e in enumerate(cdn_queue.items) if e is encode_attempt.item
            )
            count = spill(cdn_queue.items[:index])
            remaining = cdn_queue.items[index:]
            outcome = None if spool_failed else await_attempt(encode_attempt)
            if outcome is None:
                # Still encoding, it counts itself once its encode succeeds, and the loop counts it once its encode fails, which its helper never counts.
                abandoned_encodes.append(encode_attempt)
            elif outcome[0]:
                # Its helper counted it as no longer raw, so spill the encoded form, which spill does not count again.
                remaining[0] = _encoded_entry(remaining[0], outcome[1])
            encode_attempt = None
            count += spill(
                remaining, encoding=remaining[0] if outcome is None else None
            )
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

    def read_queue(timeout: float) -> bool:
        """Take the next queue item, waiting at most timeout; False if none came. Only this frame holds the item, so a quiet queue keeps no large serialized gallery alive. Once the queue can no longer be read (a message malformed or that will not unpickle), the worker reads it no more, delivers what it holds and exits as from a drain cut short, leaving the queue to the parent's salvage, and reports a message it holds in part (_QueueReads)."""
        nonlocal shutdown, input_closed, queue_lost, drain_incomplete
        if queue_lost:
            time.sleep(timeout)
            return False
        try:
            item = queue_reads.get(timeout)
        except queue.Empty:
            return False
        except OSError as error:
            _log.error("upload queue can no longer be read: %r", error)
            queue_lost = shutdown = True
            if not orphan_deadline:
                drain_incomplete = True
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
        # Drain through the parent's shutdown sentinel, never publishing a marker of the worker's own, until the queue stays silent for _QUEUE_DRAIN_SILENCE or the spill end passes, which bounds that silence too: the worker must release the queue's reader lock and exit by itself. A message still arriving (queue_reads.frame grows) is no silence, nor is the time a group's spill takes.
        silent_by = time.monotonic() + _QUEUE_DRAIN_SILENCE
        while not input_closed and not queue_lost:
            remaining = min(silent_by, spill_end()) - time.monotonic()
            if remaining <= 0:
                break
            arrived = len(queue_reads.frame)
            if read_queue(min(0.25, remaining)) or len(queue_reads.frame) > arrived:
                silent_by = time.monotonic() + _QUEUE_DRAIN_SILENCE
        # Silence is an expected close only after the parent has died. In a normal shutdown, missing None means delivery is unproven; exit nonzero so the parent performs its exact fenced salvage.
        if not input_closed and not orphan_deadline:
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
        if read_queue(poll_timeout):
            drained_items = 1
        else:
            drained_to_empty = True
        while drained_items < _DRAIN_MAX_ITEMS_PER_CYCLE and not past_deadline():
            if not read_queue(0.0):
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
            # After a handoff or a signal, which bound the worker sooner, the parent's exit is expected, as it is after a Ctrl-C, which bounds it now.
            if _hand_off_after_ctrl_c is not None:
                _hand_off_after_ctrl_c()
            if _signal_deadline == math.inf:
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
                else:
                    delivered, reduced = replay_result
                    if reduced:
                        # Recorded before an empty disk prefix clears the spooled flag below, so delivery never reads complete without it.
                        resolve_points(0, failure=_WORKER_GALLERY_CUT_SHORT)
                    if delivered:
                        # Counted by the replay that delivers the segment: one that fails is retried, and cuts the same galleries again.
                        cut_short += reduced
                    else:
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
            connect_attempt = _ConnectionAttempt(
                server_address, local_endpoint=local_endpoint
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
            # Every stream that ended reset the in-flight window (end_stream), so this one starts fresh.
            try:
                stream = _BidiStream(stub, _FEED_Q_CAP)
            except Exception as error:
                # Call construction can fail before a pump exists (closed/bad
                # channel, incompatible stub). Treat it like every other stream
                # break so private buffered points remain retryable.
                handle_break(error)
        if stream is not None:
            for kind, payload in stream.poll_acks():
                if kind == "ack":
                    # Cumulative: the delta = cum - last points at the buffer's front are delivered, each resolved once.
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
                        end_stream()
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
        # So are shutdown, which has its own deadline, from the parent's sentinel or once a signal, the handoff or the parent's shutdown has set the spill end, and a failed spool, where spill() would only count the points lost.
        if (
            numeric_lane != _LIVE
            or shutdown
            or spool_failed
            or not buffer
            or spill_end() < math.inf
        ):
            numeric_waiting_since = None
        elif numeric_waiting_since is None:
            numeric_waiting_since = now
        elif now - numeric_waiting_since >= _NUMERIC_OUTAGE_FAILOVER:
            # The open touches the disk, which may stall, so log() stops waiting for the encoder first.
            publish_lanes_on_disk(writing=True)
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
            finally:
                # The lanes' own state again: still live after a failed open, on disk after a failover.
                publish_lanes_on_disk()

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
                    # The first chunk: fewer than take points when a text point trips the byte cap.
                    points = next(
                        _chunk_tuples(
                            buffer.items[inflight_n : inflight_n + take],
                            buffer.sizes[inflight_n : inflight_n + take],
                        )
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
                if not stream.feed(
                    kymo_pb2.MetricsBatch(
                        project_id=project_id, run_id=run_id, points=points
                    )
                ):
                    break
                if inflight_n == 0:
                    ack_progress_at = time.monotonic()
                inflight_n += len(points)

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
                    if result == _WORKER_GALLERY_CUT_SHORT:
                        # Delivered, but not whole: delivery is incomplete, its failure recorded with the point's resolution.
                        cut_short += 1
                        resolve_points(1, failure=_WORKER_GALLERY_CUT_SHORT)
                    else:
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
        for attempt in [a for a in abandoned_encodes if not a.events.empty()]:
            abandoned_encodes.remove(attempt)
            if not attempt.events.get()[0]:
                release_raw_galleries(int(counted_raw(attempt.item)))
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
    pending_spool_files = [
        path for path in map(close_spool, [*sealed_spools, spool]) if path
    ]
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
            "worker exiting: %d points sent, %d spooled during the run; still on disk in %s (replay with: %s)",
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
    if queue_reads.frame:
        # Its producer died writing it (_QueueReads): the rest never comes.
        _log.error(
            "a queued group arrived only in part (%d bytes) and could not be "
            "decoded — its points are LOST",
            len(queue_reads.frame),
        )
        if not orphan_deadline:
            drain_incomplete = True
    # Points still charged but those cut: groups the worker never read (queued, held in part, or in the parent's feeder) or that the parent holds back. No salvage follows a parent that has exited or handed the run off; otherwise the parent's salvage spools what the queue holds.
    unread = max(0, queue_status.get_obj().value - cut)
    handed_off = _handoff is not None and _handoff.value > 0
    salvaged = not (orphan_deadline or handed_off) and owner_alive()
    unread_lost = 0 if salvaged else unread
    if drain_incomplete:
        _log.error(
            "upload queue drain ended before the parent shutdown sentinel with %d "
            "point(s) unread; %s",
            unread,
            "parent salvage is required" if salvaged else "no salvage follows",
        )
    if cut_short:
        _log.error(
            "%d gallery(ies) were published or spooled without items that could "
            "not be encoded or that the CDN rejected",
            cut_short,
        )
    if spool_failed or undecodable or unencodable or cut or unread_lost:
        _log.error(
            "%d points reached neither the server nor the spool (%d unspoolable "
            "(disk full?), %d unencodable, %d undecodable, %d past the spill "
            "deadline, %d never read) — LOST",
            spool_failed + unencodable + undecodable + cut + unread_lost,
            spool_failed,
            unencodable,
            undecodable,
            cut,
            unread_lost,
        )
        sys.exit(_WORKER_EXIT_SPOOL_FAILED)
    if terminal_rejected:
        sys.exit(_WORKER_EXIT_RUN_DELETED)
    if drain_incomplete:
        sys.exit(_WORKER_EXIT_DRAIN_INCOMPLETE)
