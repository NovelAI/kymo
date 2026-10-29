"""
kymo client — asynchronous metrics logger for kymo.

Architecture: the main process queues metrics via a multiprocessing.Queue.
A background worker drains it into a long-lived bidi ingest stream; blocking
rich-resource uploads use a separate ordered unary lane. Points the server can't
accept in time are spooled to disk (kymo.spool) and delivered later by
`python -m kymo.sync`, so shutdown never blocks on a slow server.
"""

import atexit
import base64
import collections
import contextlib
import copy
import io
import json
import math
import multiprocessing
import multiprocessing.synchronize
import multiprocessing.util
import ntpath
import operator
import os
import pickle
import queue
import random
import shlex
import signal
import stat
import subprocess
import sys
import threading
import time
import uuid
from typing import Optional
from urllib.parse import quote, urlsplit

import grpc

from kymo._env import number as _env_number
from kymo._env import reject_legacy_client_env
from kymo._env import string as _env_string
from kymo._env import validate_client_settings

# Generated stubs ship with kymo and must match the installed grpc/protobuf
# runtime.
try:
    from kymo._generated import kymo_pb2, kymo_pb2_grpc
except (ImportError, RuntimeError) as e:
    raise ImportError(
        "kymo's generated proto stubs are unavailable or incompatible with "
        f"the installed grpc/protobuf runtime (original error: {e})"
    ) from e
from kymo import _capture, _gpu
from kymo._cdn import (
    encoded_gallery_item,
    gallery_manifest,
    metadata_manifest,
    readable_metadata,
)
from kymo._control_rpc import BOOTSTRAP as _CONTROL_BOOTSTRAP, _ended
from kymo._log import logger as _log
from kymo.api import _check_fork_safe
from kymo._wire import (
    _F32_OVERFLOW,
    _MAX_ID_BYTES,
    _MAX_METRIC_NAME_BYTES,
    _MAX_POINTS_PER_MSG,
    _MAX_POINT_BYTES,
    _MAX_RICH_RESOURCE_ID_BYTES,
    _SEND_CALL_TIMEOUT,
    _SEND_MAX_DELAY,
    _estimate_tuple_bytes,
    _is_permanent_upload_error,
    _normalize_numeric_value,
    _publish_rich_mutation,
    _reduced_mutation_version,
    _send_unary_point,
    _upload_to_cdn,
    _validate_ident,
    _validate_project_id,
)
from kymo.spool import (
    SPOOL_SUFFIX,
    SpoolWriter,
    default_spool_dir,
    make_spool_path,
    replay_command,
    retire_deleted_spool,
    run_spool_files,
    spool_name_prefix,
    writer_active,
)
from kymo.types import Image, Metadata, Resource
from kymo._worker import _worker_process

_MAX_RUN_NAME_BYTES = 2048
# Client-side mirror of ALLOWED_EXTENSIONS in kymo-server/src/cdn.rs (keep in sync): the server 400s any other extension, which would drop the item as a permanent upload error.
_CDN_EXTENSIONS = frozenset(
    {
        "png",
        "jpg",
        "jpeg",
        "gif",
        "webp",
        "bmp",
        "svg",
        "ico",
        "tiff",
        "json",
        "txt",
        "csv",
        "wav",
        "mp3",
        "mp4",
        "webm",
        "ogg",
        "pdf",
        "gz",
        "bin",
    }
)
# Unicode White_Space code points used by Rust str::trim; Python str.strip() additionally removes U+001C..U+001F.
_RUN_NAME_TRIM_CHARS = "\t\n\v\f\r \u0085\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000"


def _normalize_metric_name(name) -> str:
    # log() inlines a fast path that skips this; it must admit only names this returns unchanged.
    if not isinstance(name, str):
        raise TypeError(f"metric name must be str, got {type(name).__name__}")
    normalized = str(name)
    _validate_ident("metric name", normalized, _MAX_METRIC_NAME_BYTES)
    return normalized


def _validate_routeable_run_key(project_id: str, run_id: str) -> None:
    if project_id in (".", "..") or run_id in (".", ".."):
        raise ValueError(
            "kymo.init: project_id and run_id cannot be '.' or '..' because "
            "browsers normalize URL path segments"
        )


def _run_control_rpc(
    method: str,
    request,
    response_type,
    *,
    server_address: str,
    timeout: float,
    local_endpoint=None,
    leave=None,
    after: Optional[int] = None,
):
    """Run a lifecycle RPC in a clean interpreter, outside the trainer.

    Under fork, initializing gRPC in the trainer makes the upload worker's fork unsafe and has caused shutdown hangs. Exec children keep control RPCs out of the trainer without requiring training scripts to use spawn.

    timeout is the RPC's deadline; SIGALRM kills the process 2 s past it (kymo._control_rpc.BOOTSTRAP), and the caller kills it at the deadline. With leave, a function that says when to stop waiting, asked at each _SHUTDOWN_STEP, the process starts with the group's signals blocked, as the upload worker does, so no Ctrl-C or SIGTERM to the group kills it mid-send, and once it has its request it is never killed: once leave() says so or past the deadline the caller returns it, and on an interrupt re-raises, leaving it to end by itself (its interpreter's start alone can take much of a second). One interrupted before its request was written, by a handler raising as the spawn restores the signal mask, say, is killed. With after, the pid of such a process an earlier call left, it sends once that one has ended.
    """
    global _left_control
    payload = {
        "method": method,
        "request": base64.b64encode(request.SerializeToString()).decode("ascii"),
        "server_address": server_address,
        "timeout": timeout,
        "local_endpoint": (
            local_endpoint.worker_config() if local_endpoint is not None else None
        ),
        "after": after,
    }
    payload_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    # Where the handler below finds the process from the moment it exists: a signal blocked across the spawn is delivered as the mask comes back, where a handler raising would otherwise lose it, its pipes leaking and its request never written.
    process = None

    def spawn():
        nonlocal process
        process = subprocess.Popen(
            [sys.executable, "-S", "-c", _CONTROL_BOOTSTRAP, repr(float(timeout))],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            # Force subprocess's posix_spawn path on POSIX. Training programs are commonly multithreaded; fork-before-exec is unsafe there for the same reason that initializing gRPC before the worker fork is. Python-created descriptors are non-inheritable by default.
            close_fds=False,
        )

    try:
        if leave is None:
            spawn()
        else:
            # Blocked only across the spawn, whose child keeps the mask: a Ctrl-C during the wait reaches kymo's handler, which can hand the run off.
            _with_group_signals_blocked(spawn)
        end = time.monotonic() + timeout
        # The payload, a few KB that a pipe takes at once, goes whole before the wait, and stdin closes with it: on Python 3.10 and 3.11 a communicate() after a step that timed out never writes what that step left unwritten, nor closes stdin, so the process would wait for its request until its alarm. communicate() gets no stdin, since there it fails on a closed one; taking it marks the request written.
        stdin, process.stdin = process.stdin, None
        # Closed in a finally, not by `with`, whose exit on Python 3.12 jumps back from inside this try to past it, which the back-edge test rejects.
        try:
            try:
                stdin.write(payload_bytes)
            finally:
                stdin.close()
        except BrokenPipeError:
            pass  # It exited unread, which its status and stderr say, as communicate() would.
        # Not `while <condition>:`, whose back-edge on Python 3.13 lies outside this try (cpython#130279).
        while True:
            try:
                stdout, stderr = process.communicate(
                    timeout=timeout if leave is None else _SHUTDOWN_STEP
                )
                break
            except subprocess.TimeoutExpired:
                if leave is None or time.monotonic() >= end or leave():
                    raise
    except BaseException as error:
        if process is None:
            raise
        # Timed out or interrupted. Left running once its request is written, and reaped once it ends, by a thread interpreter exit does not wait for; or else killed, its pipes closed and reaped, as subprocess.run does.
        if leave is not None and process.stdin is None:
            # Recorded first, since an interrupt re-raises below: the run's next TerminateRun goes out after it.
            _left_control = process
            threading.Thread(
                target=process.communicate, name="kymo-control-rpc", daemon=True
            ).start()
        else:
            with process:
                process.kill()
        if not isinstance(error, subprocess.TimeoutExpired):
            raise
        if leave is not None:
            return process
        raise RuntimeError(f"{method} timed out after {timeout:g}s") from error
    if process.returncode != 0:
        detail = stderr.decode("utf-8", errors="replace").strip()[-1000:]
        raise RuntimeError(
            f"{method} failed in control process: {detail or 'unknown error'}"
        )
    try:
        return response_type.FromString(stdout)
    except Exception as error:
        raise RuntimeError(f"{method} returned an invalid response") from error


def _normalize_run_name(name) -> str:
    if not isinstance(name, str):
        raise TypeError(f"run_name must be str, got {type(name).__name__}")
    normalized = name.strip(_RUN_NAME_TRIM_CHARS)
    if not normalized:
        raise ValueError("kymo.init: run_name is required")
    _validate_ident("run_name", normalized, _MAX_RUN_NAME_BYTES)
    return normalized


def _normalize_step(step) -> int:
    """Freeze a public step as the signed int64 carried by protobuf."""
    try:
        normalized = operator.index(step)
    except TypeError as error:
        raise TypeError("step must be an integer") from error
    if not -(1 << 63) <= normalized < (1 << 63):
        raise OverflowError("step must fit in a signed 64-bit integer")
    return normalized


def _resolve_server(
    caller: str, mode: Optional[str], server_address: Optional[str], **endpoints
) -> Optional[str]:
    """The hosted gRPC address (argument, then ``$KYMO_SERVER``), or None in local mode, whose runtime owns every endpoint."""
    value = _env_string("KYMO_MODE", "MKDB2_MODE", "hosted") if mode is None else mode
    if not isinstance(value, str):
        raise TypeError(f"{caller}: mode must be a string")
    value = value.strip().lower()
    if value not in ("hosted", "local"):
        raise ValueError(f"{caller}: mode must be 'hosted' or 'local'")
    if value == "local":
        if server_address is not None or any(v is not None for v in endpoints.values()):
            names = "/".join(["server_address", *endpoints])
            raise ValueError(
                f"{caller}: {names} cannot override local runtime endpoints"
            )
        return None
    server_address = server_address or _env_string("KYMO_SERVER", "MKDB2_SERVER")
    if not server_address:
        raise ValueError(
            f"{caller}: hosted mode needs a server address; pass server_address, "
            "set KYMO_SERVER, or use mode='local'"
        )
    return server_address


def _cdn_address_for(server_address: str) -> str:
    """The hosted CDN origin: port 8080 on the gRPC server's host."""
    return f"http://{server_address.rsplit(':', 1)[0]}:8080"


# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

_metric_queue: Optional[multiprocessing.Queue] = None
_upload_process: Optional[multiprocessing.Process] = None
_queue_status: Optional[multiprocessing.Value] = None
# Non-zero as soon as the worker proves that a point reached neither the server
# nor the spool.  This is separate from the eventual process exit code because
# wait_for_upload() may observe a zero backlog while the worker is still alive.
_upload_failure: Optional[multiprocessing.Value] = None
# Non-zero once any point is durably spooled instead of reaching the server; unlike _upload_failure this is recoverable, but wait_for_upload() must not call it an upload success.
_upload_spooled: Optional[multiprocessing.Value] = None
# Set as soon as the worker learns that the run lifecycle rejects all future
# writes.  Keep this separate from _upload_failure: a prior disk failure wins
# that error code, but parent-side crash salvage must still never create a
# replayable spool for a deleted run.
_upload_terminal: Optional[multiprocessing.Value] = None
# Monotonic deadline for the upload worker, 0.0 = none. Once passed, the worker stops sending and spools the rest — bounded shutdown. Shared memory without a lock, like _handoff: only the parent writes it, and only the worker reads it.
_shutdown_deadline = None
# When the worker stops spilling, 0.0 = none: a margin before the shutdown's kill, so the worker reports what it lost and exits by itself (_Shutdown.plan_worker). Shared memory like _shutdown_deadline.
_spill_deadline = None
# The time.monotonic() by which the worker must be gone once a handoff has left it the run's remaining delivery (_Shutdown.hand_off), 0.0 until then: shared memory without a lock, which the worker reads in its signal handler (kymo._worker._own_signals).
_handoff = None
# Set by the worker as it begins (kymo._worker._worker_process), before it reads the queue; shared memory without a lock. A handoff kills a worker that has not begun (_kill_unstarted_worker).
_worker_started = None
# When the upload worker last saw a Ctrl-C (time.monotonic(), 0.0 = never), which reaches it with the rest of the process group: its SIGINT handler notes it (kymo._worker._own_signals), so one that a handler the program installed after init() took, which kymo's never saw, still times the run's end (_ctrl_c_at). Shared memory without a lock.
_worker_interrupted_at = None  # multiprocessing.RawValue("d")
# The status the upload worker exits with, -1 until it publishes it as it ends (kymo._worker._worker_process): whether it runs where multiprocessing cannot tell (_worker_alive). Shared memory without a lock.
_worker_exit = None  # multiprocessing.RawValue("i")
# _await_encoder paces gallery calls by these: the galleries the worker no longer holds as raw pixels (encoded, spooled or dropped), and whether either upload lane writes new work to disk.
_galleries_released = None  # multiprocessing.RawValue("q")
_lanes_on_disk = None  # multiprocessing.RawValue("i")
# Galleries this process published; only the initializing process waits on it. The worker never releases a fork child's galleries against it (see _ChildGalleryItems): a child's queue puts can be lost (a raw os.fork() child has no feeder thread, and a killed child loses what its feeder held), and a count the worker never released would hold every later gallery call. Unlocked between the direct and the pending path: an increment lost to a race only lets a later call wait less.
_galleries_published = 0
# How many this process's latest gallery-publishing call published: the depth left raw without waiting.
_previous_galleries = 0
# Set when a shutdown is claimed (_drain_and_shutdown), so a log() waiting for the encoder lets it proceed.
_finishing = False
_spool_dir: Optional[str] = None
# Identifies the spool files this init() is answerable for. Written into every
# basename and header this run produces — the pre-created path, each segment the
# worker rotates to, and the parent's salvage. Shutdown classifies ownership by
# basename without opening files; replay and rollback retain the header copy.
_session_id: str = ""
_last_backlog_warn: float = 0.0
_is_initialized: bool = False
# PID that called init(). A raw os.fork() child inherits _is_initialized, the shared queue, AND the parent's atexit/signal hooks — its exit would inject the shutdown sentinel and kill the parent's worker mid-run (multiprocessing children are immune: they exit via os._exit, skipping atexit). Shutdown paths no-op unless they run in this pid.
_init_pid: Optional[int] = None
_project_id: str = ""
_run_id: str = ""
_run_name: str = ""
_server_address: str = ""
_cdn_address: str = ""
# Dashboard origin for run_url(); not derived like _cdn_address because a browser may not reach the gRPC host.
_url_base: str = ""
_mode: str = "hosted"
_local_installation_uuid: str = ""
_original_signals: dict = {}
_atexit_registered: bool = False
_system_poller = None  # SystemMetricsPoller instance
_last_log_duration_ms: float = 0.0  # overhead of previous log() call
# Reentrant, like _capture's stream locks: a handler of a signal kymo does not own may call finish() or fork inside a drain on the thread it interrupts, taking this lock again (a log() there leaves the text for the next drain: _drain_capture_points). kymo's own shutdown handler waits for the drain's held section to end.
_capture_drain_lock = threading.RLock()
# The shutdown signals waiting for a held section (_HELD_SECTIONS) to end, in arrival order, each as [signum, the frame it interrupted, or None once deferred].
_deferred_signals: list[list] = []
# Queue groups the queue has not taken (_publish_held): still charged, so delivery reads incomplete until the main thread's next publication (the shutdown handler's flush among them) puts them first. One deque changed in place by single calls (_hold_back, _take_held_back), so no thread acts on a copy another has replaced.
_held_back: collections.deque = collections.deque()
# Queue groups a publication took over but could not charge (_publish_held): those of a pending log() call and drained captured output, which earlier calls accepted. The main thread's next publication charges and puts them, after _held_back's; the shutdown salvages them, and a handoff drops them with a warning. Changed like _held_back.
_uncharged: collections.deque = collections.deque()
_last_capture_step: Optional[int] = None
# Captured exit code (None = not yet observed; resolved at TerminateRun time), set by sys.excepthook, the wrapped sys.exit and shutdown signals: the first writer wins (_record_exit_code), except that a Ctrl-C's 130 replaces any code and a SIGTERM's or SIGQUIT's 128 + signum None or 0 (_record_signal_code), and that, while the main thread runs the program (_main_thread_runs), a code only sys.exit recorded gives way to a later sys.exit's, a shutdown signal's and the exception hook's, and is dropped by a finish() the program calls after catching it.
_exit_code: Optional[int] = None
# Whether only the wrapped sys.exit recorded _exit_code, so the program may have caught that SystemExit.
_exit_code_by_sys_exit = False
# The shutdown in progress (_Shutdown) as "shutdown", claimed by one dict.setdefault, which no other thread can interleave (_claim_shutdown); kymo's signal handler reads it, perhaps while another thread runs it.
_shutdown_claim: dict = {}
# The control process of the run's last TerminateRun, if it was left running (_run_control_rpc), after which the run's next one goes out (_terminate_run), even when a handler kymo does not own raised out of that send, aborting its shutdown or swallowed by it. None for a new run (_start_run).
_left_control = None
# When kymo's handler last saw a Ctrl-C (time.monotonic()), from which, or from the worker's if later (_ctrl_c_at), its run's end is timed (_handoff_cap), and the program's exit taken to follow it (_follows_ctrl_c).
_interrupted_at: Optional[float] = None
# When kymo's handler last saw a SIGTERM or SIGQUIT, by which the atexit flush is bounded (_ensure_metrics_uploaded).
_terminated_at: Optional[float] = None
# The last run was handed off to its worker (_Shutdown.hand_off): finish() and wait_for_upload() cannot prove its delivery.
_handed_off = False
_original_excepthook = None
_original_sys_exit = None
# The metadata blob last uploaded, whose config field update_config() changes before uploading it again.
_run_metadata: Optional[dict] = None
# Presence is negotiated by InitRunResponse. An older server leaves this None,
# preserving the existing hosted rich-write path byte-for-byte.
_rich_writer_epoch: Optional[int] = None
_rich_mutation_seq = None  # multiprocessing.Value("I") when versioning is negotiated
# log() calls whose GPU values are still being copied to the host (kymo._gpu), oldest first; log_cdn() and update_config() writes queue behind them. All are published in that order. System metrics and captured text never wait here: the poller publishes them directly, and log() publishes the captured text at once even when its call waits.
_pending: collections.deque = collections.deque()
# Reentrant: the shutdown signal handler runs on the main thread, possibly inside log().
_pending_lock = threading.RLock()
# Held by a log() call that publishes galleries directly, from its encoder check to its publication, so concurrent callers cannot all pass one check. It is never held while taking _pending_lock, which other threads' writes need while such a call waits; reentrant for a signal handler that logs inside log().
_gallery_lock = threading.RLock()
# What log() holds instead when it publishes no gallery directly.
_NO_LOCK = contextlib.nullcontext()
# Set when a value was dropped after its log() returned; wait_for_upload() and finish() then report incomplete delivery. While a pending call is handed over, the call itself, until it loses a value (_publish_oldest_pending).
_pending_incomplete = False
# At most how many charged points nothing may ever resolve: those of groups whose put or charge an exception interrupted (_publish_held). A charge the queue never took stays in the backlog, so each backlog read lowers this to it (_delivery_status_snapshot), and wait_for_upload() reports delivery unproven once the backlog stays within it.
_unproven_points = 0
# Guards _unproven_points, which the main thread raises and any thread's wait_for_upload() lowers. Reentrant: a handler may wait inside the main thread's update.
_unproven_lock = threading.RLock()
# Host copies and pickled rich items the pending calls may hold before log() waits for the oldest.
_PENDING_MAX_BYTES = 256 << 20


def _reserve_rich_mutation_versions(count: int) -> Optional[tuple[int, ...]]:
    """Atomically reserve persisted logical versions, or select legacy behavior."""
    if _rich_writer_epoch is None:
        return None
    if _rich_mutation_seq is None:
        raise RuntimeError("kymo rich mutation allocator is not initialized")
    try:
        return _reserve_versions(count)
    finally:
        if _deferred_signals:
            _run_deferred_signals()


def _reserve_versions(count: int) -> tuple[int, ...]:
    """The reservation for _reserve_rich_mutation_versions, a held section (_HELD_SECTIONS): the allocator's lock is reentrant, so a handler kymo chains to that logs a gallery from inside it would reserve the same versions, and the server would reject one gallery as DATA_LOSS."""
    lock = _rich_mutation_seq.get_lock()
    if not lock.acquire(timeout=_STATUS_LOCK_TIMEOUT):
        raise RuntimeError("kymo rich mutation allocator lock is unavailable")
    try:
        raw_seq = _rich_mutation_seq.get_obj()
        if raw_seq.value > (1 << 32) - 1 - count:
            raise RuntimeError(
                "kymo rich mutation sequence exhausted for this run writer"
            )
        first = raw_seq.value + 1
        raw_seq.value += count
        return tuple(
            (_rich_writer_epoch << 32) | sequence
            for sequence in range(first, first + count)
        )
    finally:
        lock.release()


def _forget_pending() -> None:
    """Drop the pending calls unpublished, keeping their CUDA objects in _gpu.unfreeable."""
    global _pending_incomplete
    _gpu.unfreeable.update(entry.reads for entry in _pending if entry.reads is not None)
    _pending.clear()
    _pending_incomplete = False


def _forget_pending_in_child() -> None:
    """A fork child cannot use the parent's CUDA context, so it never publishes the parent's pending calls. It resets its locks as _capture does its own (_forget_capture_in_child).

    Nor does it handle the parent's deferred shutdown signals, put its held-back groups, or run its shutdown. Its main thread, the forking one, runs its handlers and holds no section another thread was in. Nor does it count the parent's upload worker among its own children, which multiprocessing's exit hook would terminate at a raw os.fork() child's exit.
    """
    _forget_pending()
    for lock in (_pending_lock, _gallery_lock, _capture_drain_lock, _unproven_lock):
        if not lock._is_owned():
            lock._at_fork_reinit()
    # multiprocessing resets a SemLock's owner count only in its own children; after a raw fork while another thread held one, the child's release would leave the shared semaphore taken for good. Where the forking thread held it, the child's release then fails instead of freeing the semaphore under the parent. A test's stand-in status has a threading lock, hence the isinstance check.
    for shared in (_queue_status, _rich_mutation_seq):
        lock = None if shared is None else shared.get_lock()
        if isinstance(lock, multiprocessing.synchronize.SemLock):
            lock._semlock._after_fork()
    # Limit: the queue is not reset. Once the parent's feeder has started, a raw os.fork() child's copy still names it and starts none, so the child's log() puts are never sent though charged to the backlog: the parent's wait_for_upload() waits for them until its timeout, or for good without one. A put also hangs if another parent thread held the queue's lock at the fork. Resetting the queue, as multiprocessing does in its own children, would give the child a feeder of its own, and a child that ends mid-write (os._exit(), a kill) would leave the shared write lock taken and a partial message in the pipe, cutting off the parent's uploads for good.
    _deferred_signals.clear()
    _uncharged.clear()
    _held_back.clear()
    _shutdown_claim.clear()
    if _upload_process is not None:
        _disown_worker(_upload_process)


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_forget_pending_in_child)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def init(
    server_address: Optional[str] = None,
    project_id: Optional[str] = None,
    run_name: Optional[str] = None,
    run_id: Optional[str] = None,
    *,
    mode: Optional[str] = None,
    cdn_address: Optional[str] = None,
    url_base: Optional[str] = None,
    system_metrics: bool = True,
    config: Optional[dict] = None,
    spool_dir: Optional[str] = None,
) -> None:
    """Initialise the kymo client.

    Can be called multiple times — shuts down the previous worker first.
    Under the ``fork`` start method it raises RuntimeError once a ``kymo.Api`` has opened a gRPC channel in this process, since it forks its upload worker.

    Args:
        server_address: gRPC server address (``host:port``). If None, falls
            back to ``$KYMO_SERVER``; hosted mode needs one of the two.
        project_id: Project identifier. Required.
        run_name: Display name for the run (shown in the UI). Required.
            Duplicates are allowed — use ``run_id`` to pin identity across re-runs.
        run_id: Globally unique run identifier. Auto-generated via
            ``uuid.uuid4().hex`` if not provided. Pass explicitly to resume an
            existing run, but do not reuse that ID in another project. The
            server is idempotent and keeps the *first* run_name set.
        mode: ``"hosted"`` (default) or ``"local"``. If omitted, uses
            ``$KYMO_MODE``. Local mode invokes ``kymo ensure --json`` and
            uses its authenticated private Unix sockets.
        cdn_address: HTTP CDN address (``http://host:port``). If None,
            derived from server_address by swapping port to 8080.
        url_base: Dashboard origin that ``run_url()`` builds on in hosted mode
            (``http://host:port``). If None, falls back to ``$KYMO_URL_BASE``.
        system_metrics: Automatically log GPU/CPU/memory/disk/network metrics.
            Captured stdout/stderr is uploaded every 2 s either way.
        config: Optional run configuration dict (hyperparams, etc.).
        spool_dir: Where undeliverable points are spooled for later replay
            (``python -m kymo.sync``). Falls back to ``$KYMO_SPOOL_DIR`` or
            ``~/.cache/kymo/spool``. Point it somewhere shared (e.g.
            next to your checkpoint dir) so a CPU node can replay it.
    """
    global _metric_queue, _upload_process
    global _queue_status, _project_id, _run_id, _run_name, _cdn_address, _url_base
    global _system_poller, _server_address, _shutdown_deadline, _spool_dir
    global _spill_deadline
    global _session_id
    global _upload_failure, _upload_spooled, _upload_terminal, _unproven_points
    global _galleries_released, _lanes_on_disk, _galleries_published
    global _previous_galleries, _finishing
    global _mode, _local_installation_uuid
    global _rich_writer_epoch, _rich_mutation_seq
    global _handoff, _worker_started
    global _worker_interrupted_at, _worker_exit
    global _run_metadata

    # The upload worker is a multiprocessing child; with the spawn start
    # method (macOS default) that child re-imports __main__. If the calling
    # script has no `if __name__ == "__main__"` guard, this very function
    # re-executes inside the child's bootstrap — registering a DUPLICATE
    # run and then dying, leaving the parent queueing into the void. Fail
    # here, before the InitRun RPC, with the actual fix. (`_inheriting` is
    # the same flag the stdlib's own bootstrap check uses; it is False by
    # the time legitimate child code — e.g. a torch.multiprocessing rank
    # worker calling init() itself — runs.)
    if getattr(multiprocessing.current_process(), "_inheriting", False):
        raise RuntimeError(
            "kymo.init() was re-executed while a worker process was importing "
            "your script (multiprocessing spawn re-imports __main__). Wrap your "
            'script body in `if __name__ == "__main__":` and run again.'
        )

    validate_client_settings()
    if url_base is not None and not isinstance(url_base, str):
        raise TypeError("kymo.init: url_base must be a string")
    server_address = _resolve_server(
        "kymo.init", mode, server_address, cdn_address=cdn_address, url_base=url_base
    )
    mode = "local" if server_address is None else "hosted"
    if mode == "hosted" and url_base is None:
        url_base = _env_string("KYMO_URL_BASE", "MKDB2_URL_BASE")
    if not project_id:
        raise ValueError("kymo.init: project_id is required")
    run_name = _normalize_run_name(run_name)
    if run_id is None:
        run_id = uuid.uuid4().hex
    # Validate before the re-init shutdown below: a bad id must raise before killing the previous worker.
    if not run_id:
        raise ValueError(
            "kymo.init: run_id must be non-empty (or None to auto-generate)"
        )
    _validate_project_id(project_id)
    _validate_ident("run_id", run_id, _MAX_ID_BYTES)
    _validate_routeable_run_key(project_id, run_id)
    # Own and validate caller config before a re-init can stop the current run.
    if config is not None:
        config = _snapshot_config(config)
    _check_fork_safe()
    # The upload child inherits this cwd, while the parent can change cwd before shutdown inventory/salvage. Freeze one shared location now.
    spool_dir = os.path.abspath(spool_dir or default_spool_dir())

    if not _atexit_registered:
        _require_shutdown_signal_capability()

    # Shut down previous worker if re-initialising
    if _is_initialized:
        _drain_and_shutdown()
    # Drop what a log() on another thread queued or held back behind the previous run's shutdown, so it does not reach this run (a log() still racing init() can, as its direct publications always could).
    with _pending_lock:
        _forget_pending()
    dropped = sum(
        map(_queue_item_size, _take_held_back() + _take_held_back(_uncharged))
    )
    if dropped:
        _log.warning(
            "dropped %d point(s) the previous run held back, never queued", dropped
        )

    local_endpoint = None
    local_init_hold_id = None
    if mode == "local":
        from kymo._local_runtime import ensure_local_endpoint

        local_init_hold_id = str(uuid.uuid4())
        local_endpoint = ensure_local_endpoint(init_hold_id=local_init_hold_id)
        server_address = local_endpoint.grpc_target
        cdn_address = local_endpoint.upload_origin
        url_base = local_endpoint.dashboard_origin

    assert server_address is not None

    _project_id = project_id
    _run_id = run_id
    _server_address = server_address
    _url_base = url_base.rstrip("/")
    _mode = mode
    _local_installation_uuid = (
        local_endpoint.installation_uuid if local_endpoint is not None else ""
    )

    _cdn_address = (
        _cdn_address_for(server_address) if cdn_address is None else cdn_address
    )

    # Before the first lock or shared value, whose semaphore would start the tracker with the group's signals unblocked, and before InitRun, so that a start that fails (a process spawn) leaves no registered run unended.
    _start_resource_tracker()
    # Synchronously register the run before starting the upload worker. The
    # clean control process is load-bearing on Linux: forking after this RPC ran
    # in the trainer would inherit gRPC's background state nondeterministically.
    init_request = kymo_pb2.InitRunRequest(
        project_id=project_id,
        run_id=run_id,
        run_name=run_name,
    )
    if local_init_hold_id is not None:
        init_request.local_hold_id = local_init_hold_id
    resp = _run_control_rpc(
        "InitRun",
        init_request,
        kymo_pb2.InitRunResponse,
        server_address=server_address,
        timeout=30.0,
        local_endpoint=local_endpoint,
    )
    run_info = resp.run
    _rich_writer_epoch = resp.writer_epoch if resp.HasField("writer_epoch") else None
    if _rich_writer_epoch is not None and not 1 <= _rich_writer_epoch < (1 << 32):
        raise RuntimeError("kymo returned an invalid rich writer epoch")
    _rich_mutation_seq = (
        multiprocessing.Value("I", 0) if _rich_writer_epoch is not None else None
    )
    # Existing run IDs are first-write-wins on name. Recovery metadata must use the server's canonical value rather than this caller's rejected proposal.
    run_name = run_info.run_name
    _run_name = run_name

    _metric_queue = multiprocessing.Queue()
    # A busy long-running job can exceed a signed 32-bit lifetime point count.
    _queue_status = multiprocessing.Value("q", 0)
    _unproven_points = 0
    _upload_failure = multiprocessing.Value("i", 0)
    _upload_spooled = multiprocessing.Value("i", 0)
    _upload_terminal = multiprocessing.Value("i", 0)
    _shutdown_deadline = multiprocessing.RawValue("d", 0.0)
    _spill_deadline = multiprocessing.RawValue("d", 0.0)
    _handoff = multiprocessing.RawValue("d", 0.0)
    _worker_started = multiprocessing.RawValue("b", 0)
    _worker_interrupted_at = multiprocessing.RawValue("d", 0.0)
    _worker_exit = multiprocessing.RawValue("i", -1)
    _galleries_released = multiprocessing.RawValue("q", 0)
    _lanes_on_disk = multiprocessing.RawValue("i", 0)
    _galleries_published = 0
    _previous_galleries = 0
    _finishing = False
    _spool_dir = spool_dir
    _session_id = uuid.uuid4().hex
    spool_path = make_spool_path(
        project_id,
        run_id,
        "worker",
        spool_dir=spool_dir,
        session=_session_id,
    )

    # daemon=True: graceful drain is atexit's job; the daemon flag is the
    # backstop so the interpreter never waits on this process.
    _upload_process = multiprocessing.Process(
        target=_worker_process,
        args=(
            server_address,
            project_id,
            run_id,
            _metric_queue,
            _queue_status,
            _cdn_address,
            _shutdown_deadline,
            spool_path,
            run_name,
            _upload_failure,
            _upload_spooled,
            os.getpid(),
            _upload_terminal,
            _session_id,
            local_endpoint.worker_config() if local_endpoint is not None else None,
            _galleries_released,
            _lanes_on_disk,
            _spill_deadline,
        ),
        kwargs={
            "handoff": _handoff,
            "owned": _owned_signals(),
            "started": _worker_started,
            "interrupted": _worker_interrupted_at,
            "exited": _worker_exit,
        },
        daemon=True,
    )
    try:
        if not _atexit_registered:
            # Before the worker starts, so a Ctrl-C as it starts waits for the run to be active (_start_run).
            _setup_signal_handlers()
        _start_run(_upload_process)
    except BaseException as error:
        if _upload_process.pid is None:
            if not _atexit_registered:
                # Installed above for this run: a program that carries on without kymo gets the signal handling it had.
                _restore_signal_handlers()
            code = (
                130
                if isinstance(error, KeyboardInterrupt)
                else _system_exit_code(error.code)
                if isinstance(error, SystemExit)
                else 1
            )
            if _deferred_signals:
                # Deferred as the worker started (_start_run, which timed them afresh), and waiting (finally, below): a group Ctrl-C or SIGTERM can fail the start itself, under forkserver by killing the forkserver as it imports __main__ (EOFError), and the run ends with its code, a Ctrl-C's over any other.
                code = (
                    130
                    if _interrupted_at is not None
                    else 128 + _deferred_signals[0][0]
                )
            # InitRun registered the run, which nothing else would end: best effort, as a handoff sends it.
            _terminate_run(
                code,
                _INTERRUPT_FLUSH_TIMEOUT,
                run=(
                    _server_address,
                    _local_installation_uuid,
                    project_id,
                    run_id,
                    _upload_terminal,
                ),
                wait=_HANDOFF_TERMINATE_TIMEOUT,
            )
        raise
    finally:
        if _deferred_signals:
            _run_deferred_signals()
    _log.info(
        "init project=%s run=%r (id=%s..., ordinal=%d) server=%s cdn=%s",
        project_id,
        run_info.run_name,
        run_id[:8],
        run_info.ordinal,
        server_address,
        _cdn_address,
    )

    # Collected once: update_config() changes only its config field, so the time, system and git fields never drift between uploads.
    if _is_initialized:
        _run_metadata = _collect_run_metadata(config)
        _push_run_metadata()

    # Last, since with system_metrics its start can take seconds (NVML's init): the poller, which every 2 s queues the captured console and, with system_metrics, the GPU/CPU/memory/disk/network metrics, directly to the upload queue with time-based steps. Imported here: kymo.system_metrics imports this module back (_WorkerExited).
    from kymo.system_metrics import SystemMetricsPoller

    _system_poller = SystemMetricsPoller(
        poll_interval=2.0,
        publish=_publish_queue_items,
        system_metrics=system_metrics,
    )
    _system_poller.start()


def _start_run(process: multiprocessing.Process) -> None:
    """Start the upload worker (_start_worker) and, once it has a pid, make the run active and install the exit hooks, so a Ctrl-C or an error before init() returns ends the run as a later one would, rather than leave the worker, which carries on through a Ctrl-C, to multiprocessing's exit, which waits for it (forever under a SIGTERM the program owns). A held section (_HELD_SECTIONS): the signals are blocked across Process.start() on the main thread alone, so one the kernel gives another thread runs kymo's handler on the main thread inside the start, which defers it until the run is active. What a previous run recorded for the exit hooks and log-overhead tracking, which span the process, is cleared first."""
    global _exit_code, _handed_off, _interrupted_at, _terminated_at, _left_control
    global _last_log_duration_ms, _system_poller, _is_initialized, _init_pid
    global _atexit_registered
    # Before the start: a signal deferred during it keeps the time its handling and the program's exit read, and a failed start's TerminateRun waits for no other run's.
    _interrupted_at = _terminated_at = _left_control = None
    try:
        _start_worker(process)
    finally:
        if process.pid is not None:
            _exit_code = None
            _handed_off = False
            _last_log_duration_ms = 0.0
            _system_poller = None
            _is_initialized = True
            _init_pid = os.getpid()
            if not _atexit_registered:
                # multiprocessing's logger first: its first set-up (get_logger(), which log_to_stderr() calls) registers multiprocessing's exit hook again, to run first, which after this registration would close the queue's feeder and terminate and join the worker before the flush (forever where the program owns SIGTERM, which the worker then ignores). As after a program's own first call, the logger shows nothing until given a handler, and does not propagate.
                multiprocessing.util.get_logger()
                atexit.register(_ensure_metrics_uploaded)
                _install_exit_capture()
                _atexit_registered = True


def _start_worker(process: multiprocessing.Process) -> None:
    """Start the upload worker with the shutdown signals and the other group signals (_GROUP_SIGNALS) blocked: a terminal's Ctrl-C and a scheduler's SIGTERM or SIGUSR1 reach every process of the group, and the worker must not die of one, or run a handler a fork inherited, before its policy unblocks them (kymo._worker._own_signals). Fork and spawn children inherit the mask. The resource tracker is started first (_start_resource_tracker), since before CPython 3.12.9 and 3.13.2 its start unblocks SIGINT and SIGTERM, then a forkserver, unblocked, since it would hand the mask to every later child of its own. The queue init() made has already fixed the start method."""
    _start_resource_tracker()
    if multiprocessing.get_start_method() == "forkserver":
        from multiprocessing import forkserver

        forkserver.ensure_running()
    _with_group_signals_blocked(process.start)


def _start_resource_tracker() -> None:
    """Start multiprocessing's resource tracker, which spawn and forkserver use, with the group's signals blocked (_with_group_signals_blocked), unless it runs already.

    The tracker ignores and unblocks only SIGINT and SIGTERM. A group SIGQUIT (a terminal's Ctrl-\\), or a SIGHUP, SIGUSR1 or SIGUSR2 the program handles, then stays pending in it rather than kill it (SIGQUIT with a core dump) and make the program's exit print KeyError tracebacks and a warning that resources might leak. It runs no handler and starts no process.

    One the program starts itself, before init() or through a non-fork context of its own (a fork program's get_context("spawn")), keeps its own mask.
    """
    # Read without fixing it, as _check_fork_safe does: an init() whose InitRun fails leaves the start method to the program.
    method = (
        multiprocessing.get_start_method(allow_none=True)
        or multiprocessing.get_all_start_methods()[0]
    )
    if method != "fork":
        from multiprocessing import resource_tracker

        _with_group_signals_blocked(resource_tracker.ensure_running)


def _with_group_signals_blocked(call) -> None:
    """call() with the shutdown and other group signals (_GROUP_SIGNALS) blocked, the mask the upload worker, the TerminateRun control process and the resource tracker start with, then the caller's mask restored.

    The mask is read unchanged and the block made inside the try: a handler raising as pthread_sigmask returns having blocked them would otherwise skip the restore, as would one at the opcode after a `return call()` there, outside the try from Python 3.11.

    A signal blocked meanwhile is handled as the restore returns, or, before CPython 3.12.9 and 3.13.2, a SIGINT or SIGTERM inside call() as the tracker's start unblocks them (resource_tracker.ensure_running), and a handler may raise there. So call stores what its caller must not lose (_run_control_rpc), init() starts the tracker before InitRun registers the run, and _start_worker runs inside _start_run's held section.
    """
    mask = signal.pthread_sigmask(signal.SIG_BLOCK, [])
    try:
        signal.pthread_sigmask(signal.SIG_BLOCK, _TERMINATION_SIGNALS + _GROUP_SIGNALS)
        call()
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, mask)


def is_initialized() -> bool:
    """Returns True if kymo.init() has been called and not yet shut down."""
    return _is_initialized


def run_url() -> str:
    """Browsable dashboard URL for the current run, for the training script to
    print at startup (wandb-style ``url: http://...``).

    Hosted mode builds on the ``url_base`` given to ``init()`` or
    ``$KYMO_URL_BASE``. Local mode returns the non-secret dashboard origin
    from the installation's pinned dashboard port, so it remains valid across
    idle restarts without waking a stopped stack. Use ``open_run()`` to wake it.
    """
    if not _is_initialized:
        raise RuntimeError("kymo not initialised — call kymo.init() first")
    if not _url_base:
        raise RuntimeError(
            "kymo.run_url: no dashboard address; pass url_base to kymo.init() "
            "or set KYMO_URL_BASE before calling it"
        )
    # safe="" so a `/` in a project id can't splinter the route path
    return f"{_url_base}/{quote(_project_id, safe='')}/{quote(_run_id, safe='')}"


def open_run() -> str:
    """Open the current run in the dashboard and return its URL.

    Local mode asks the launcher to wake the stack and hold it during browser
    startup. Its pinned loopback URL contains no credential; on a headless
    server no browser opens, and the returned URL is the one to forward.
    """
    if not _is_initialized:
        raise RuntimeError("kymo not initialised — call kymo.init() first")
    if _mode == "local":
        from kymo._local_runtime import open_local_run

        return open_local_run(
            _project_id,
            _run_id,
            expected_installation_uuid=_local_installation_uuid,
        )

    import webbrowser

    url = run_url()
    if not webbrowser.open(url):
        raise RuntimeError("failed to open the kymo dashboard in a browser")
    return url


def log(metrics: dict, step: int) -> None:
    """Queue metrics for upload.

    Values can be:
    - float/int: numeric metric (sent via gRPC)
    - list[int/float]: tagged (bundled) metric (one trace per index)
    - Image or list[Image]: image gallery (uploaded to CDN)
    - Resource or list[Resource]: generic resource (uploaded to CDN)

    Numeric and text values take the queue fast path. Rich values are pickled
    synchronously to freeze caller-owned data before background delivery, so
    logging a large rich payload may copy it on the calling thread.

    CUDA tensors, as scalar values and in a value whose items are all Images
    holding CUDA tensors, are copied to the host without waiting for the GPU;
    log() raises if it would read one during a CUDA graph capture. The call's
    points are then published, in call order, by a later log(),
    wait_for_upload() or finish() once the copies have completed. A value that
    can no longer be sent (for example a float64 beyond float32 range) is
    dropped with a warning, and wait_for_upload() and finish() report
    incomplete delivery.
    """
    global _last_log_duration_ms

    if not _is_initialized:
        raise RuntimeError("kymo not initialised — call kymo.init() first")

    step = _normalize_step(step)
    t0 = time.perf_counter()
    now_ms = int(time.time() * 1000)

    # Include overhead of *previous* log() call
    all_metrics = dict(metrics)
    if _last_log_duration_ms > 0:
        all_metrics.setdefault("system/log_overhead_ms", _last_log_duration_ms)

    numeric_names = []
    numeric_values = []
    numeric_tags = []  # empty until the first list (or the CUDA scalars below), then one per value (_NUMERIC_BATCH_QUEUE_ITEM)
    cdn_batches = []
    galleries = False  # whether cdn_batches holds one
    # CUDA values, read asynchronously once CUDA is initialized.
    torch = _gpu.cuda_torch()
    if torch is not None:
        is_gpu_scalar, is_gpu_image = _gpu.classifiers(torch)
    gpu_keys = []  # (name, tag) of each of gpu_scalars
    gpu_scalars = []
    gpu_batches = []  # rich batches whose images are all on the GPU, pickled once copied

    for name, value in all_metrics.items():
        # Per-point validation dominates a large log(): exact ASCII str names and exact floats below ±_F32_OVERFLOW skip it, since the full checks return them unchanged.
        if not (
            type(name) is str
            and name.isascii()
            and len(name) <= _MAX_METRIC_NAME_BYTES
            and "\x00" not in name
        ):
            name = _normalize_metric_name(name)
        if type(value) is float and -_F32_OVERFLOW < value < _F32_OVERFLOW:
            numeric_names.append(name)
            numeric_values.append(value)
            if numeric_tags:
                numeric_tags.append(None)
        elif torch is not None and is_gpu_scalar(value):
            gpu_keys.append((name, None))
            gpu_scalars.append(value)
        elif isinstance(value, (int, float)):
            numeric_names.append(name)
            numeric_values.append(_normalize_numeric_value(value))
            if numeric_tags:
                numeric_tags.append(None)
        elif isinstance(value, Metadata):
            cdn_batches.append(
                _rich_queue_tuple("metadata_batch", name, step, value, now_ms)
            )
        elif isinstance(value, (Image, Resource)) or (
            isinstance(value, list)
            and value
            and isinstance(value[0], (Image, Resource))
        ):
            items = value if isinstance(value, list) else [value]
            for index, item in enumerate(items):
                if not isinstance(item, (Image, Resource)):
                    raise TypeError(
                        f"rich metric {name!r} item {index} must be Image or "
                        f"Resource, got {type(item).__name__}"
                    )
            batches = cdn_batches
            if torch is not None and all(
                isinstance(item, Image) and is_gpu_image(item.data) for item in items
            ):
                # The queued copies' data becomes the host copies.
                items = [copy.copy(item) for item in items]
                batches = gpu_batches
            else:
                galleries = True
            batches.append(_rich_queue_tuple("cdn_batch", name, step, items, now_ms))
        elif isinstance(value, list):
            if not numeric_tags:
                numeric_tags = [None] * len(numeric_names)
            for i, v in enumerate(value):
                if torch is not None and is_gpu_scalar(v):
                    gpu_keys.append((name, str(i)))
                    gpu_scalars.append(v)
                    continue
                numeric_names.append(name)
                numeric_values.append(_normalize_numeric_value(v))
                numeric_tags.append(str(i))
        else:
            numeric_names.append(name)
            numeric_values.append(_normalize_numeric_value(value))
            if numeric_tags:
                numeric_tags.append(None)

    if gpu_keys and not numeric_tags:
        # The GPU values join the batch once copied, each appending its tag (_publish_oldest_pending), after one per CPU value.
        numeric_tags = [None] * len(numeric_names)

    numeric = (
        _NUMERIC_BATCH_QUEUE_ITEM,
        step,
        now_ms,
        numeric_names,
        numeric_values,
        numeric_tags,
    )
    # The publication drains the captured stdout/stderr (keyed by timestamp, independent of step) inside its held section, after everything that can reject this call (bad name, un-floatable value), so a shutdown signal finds the text on the queue or held back, charged (_held_back) or not (_uncharged).
    if not (cdn_batches or gpu_scalars or gpu_batches or _pending):
        # Numeric values only, with nothing to publish before them.
        _publish_queue_items([numeric], capture_ms=now_ms)
    else:
        pending = gpu_scalars or gpu_batches or _pending
        paced = galleries and not pending
        with _gallery_lock if paced else _NO_LOCK:
            if paced:
                # Before the snapshot, so a waiting call holds no copy of its images yet, and outside every held section, so a shutdown signal during the wait is handled at once.
                _await_encoder()
                if _finishing:
                    # A shutdown claimed meanwhile ends the run this call was for, as for a call that starts after it.
                    raise RuntimeError("kymo not initialised — call kymo.init() first")
            # Freeze public rich work before draining captured text: Queue.put() returns
            # before its feeder pickles, so passing the live object could fail later or
            # observe a caller mutation after log() returned. Numeric/text values contain
            # primitives and stay on the no-synchronous-snapshot fast path. Rich log()
            # latency is intentionally proportional to the snapshot size.
            rich_queue_items = _snapshot_rich_queue_items(cdn_batches)
            if pending:
                _queue_pending_log(
                    torch,
                    _PendingLog(
                        numeric,
                        rich_queue_items,
                        step=step,
                        gpu_keys=gpu_keys,
                        gpu_batches=gpu_batches,
                        galleries=galleries,
                    ),
                    gpu_scalars,
                )
                # Published in call order, now or later; the captured text below never waits.
                numeric, rich_queue_items, galleries = [], [], False
            _publish_queue_items(
                [numeric, *rich_queue_items], capture_ms=now_ms, galleries=galleries
            )

    _warn_if_backlogged()
    _last_log_duration_ms = (time.perf_counter() - t0) * 1000.0


# How long the shutdown's last drain off the main thread waits for each busy capture lock before it leaves that lock's text unsent (_drain_capture_points).
_FINAL_DRAIN_LOCK_WAIT = 0.1


def _drain_capture_points(now_ms: int, final: bool = False) -> list[tuple]:
    """Drain the captured output into text points for a publication (_publish_held), with final the shutdown's last drain (_capture.drain_buffers)."""
    if _on_main_thread():
        if not final and _capture_drain_lock._is_owned():
            # A handler's log() nested in a drain on this thread could take the outer drain's step, and the server keeps one text point per stream and step: leave the text for the next drain, as _capture._drain_one does.
            return []
        with _capture_drain_lock:
            points, _ = _drain_capture_locked(now_ms, final, None)
        return points
    # Off the main thread (the poller's drain, another thread's log()) a drain waits for no capture lock; the shutdown's last waits at most _FINAL_DRAIN_LOCK_WAIT for each. A handler of a signal kymo does not own can call log() inside a print on the main thread, which holds its stream's lock, so waiting for that lock while holding the drain lock would deadlock with the handler's drain; and on Python 3.14 a raising handler can leave a lock held for good as a `with` takes it. No handler runs off the main thread between the acquire and the try.
    timeout = _FINAL_DRAIN_LOCK_WAIT if final else 0
    if _capture_drain_lock.acquire(timeout=timeout):
        try:
            points, unsent = _drain_capture_locked(now_ms, final, timeout)
        finally:
            _capture_drain_lock.release()
    else:
        points, unsent = [], (["output"] if _capture.has_output() else [])
    if final and unsent:
        # Out of the drain's lock: a handler the program added may write this to the captured stderr, perhaps the stream passed over.
        _log.warning(
            "captured %s left unsent: a capture lock stayed busy as the run ended",
            " and ".join(unsent),
        )
    return points


def _drain_capture_locked(
    now_ms: int, final: bool, timeout: Optional[float]
) -> tuple[list[tuple], list[str]]:
    """_drain_capture_points' drain, _capture_drain_lock held: its text points, and the streams it passed over (_capture.drain_buffers' timeout)."""
    global _last_capture_step
    stdout_text, stderr_text = _capture.drain_buffers(final, timeout)
    points = []
    if stdout_text or stderr_text:
        step = (
            now_ms
            if _last_capture_step is None
            else max(now_ms, _last_capture_step + 1)
        )
        _last_capture_step = step
        if stdout_text:
            points.append(("text_ts", "logs/std_out", step, stdout_text, now_ms))
        if stderr_text:
            points.append(("text_ts", "logs/std_err", step, stderr_text, now_ms))
    return points, [
        name
        for name, text in (("stdout", stdout_text), ("stderr", stderr_text))
        if text is None
    ]


_BACKLOG_WARN_THRESHOLD = 100_000
_BACKLOG_WARN_INTERVAL = 60.0
_STATUS_LOCK_TIMEOUT = 1.0
# A healthy server acknowledges a stream's fed points within this even when they fill no cut (FLUSH_INTERVAL in kymo-server/src/ingest.rs).
_SERVER_FLUSH_INTERVAL = 2.0


_SERIALIZED_RICH_QUEUE_ITEM = "__kymo_serialized_rich_v1__"
# A log() call's numeric values as one queue item: (kind, step, timestamp_ms, names, values, tags), parallel lists, except that tags may be empty, meaning no value is tagged: a call that logs no list and no CUDA scalar appends no tag; otherwise None tags an untagged value. The mp.Queue feeder pickles in the training process, and one batch pickles far cheaper than a tuple per point; _decode_queue_item expands it.
_NUMERIC_BATCH_QUEUE_ITEM = "__kymo_numeric_batch_v1__"
# Such a batch with a 7th field, the text points of the captured output its publication drained (_publish_held), so the call makes one put; a kind of its own, so sizing the plain batch most log() calls publish takes no extra check.
_NUMERIC_TEXT_BATCH_QUEUE_ITEM = "__kymo_numeric_text_batch_v1__"
_NUMERIC_BATCH_KINDS = (_NUMERIC_BATCH_QUEUE_ITEM, _NUMERIC_TEXT_BATCH_QUEUE_ITEM)
_GALLERY_KINDS = ("cdn_batch", "cdn_batch_mutation")
_METADATA_KINDS = ("metadata_batch", "metadata_batch_mutation")


class _HostTensorPickler(pickle.Pickler):
    """Snapshot pickler that pickles a tensor as its host NumPy array, or as a host tensor when that is smaller or NumPy cannot hold it.

    Torch pickles a tensor with its device and its whole storage.
    A forked upload worker cannot initialize CUDA to unpickle a device tensor and would drop the item as lost, and one that can opens its own context.
    A view, such as one image of a batch, would carry the whole batch.
    A NumPy array pickles only its own elements, and the worker converts an image tensor to NumPy first anyway.
    Other objects use their normal pickle reducers.
    """

    def reducer_override(self, obj):
        # Never imports torch: a tensor can only exist once it is loaded.
        # A stub or half-imported torch has no Tensor type; pickle normally then.
        tensor = getattr(sys.modules.get("torch"), "Tensor", None)
        if not (isinstance(tensor, type) and isinstance(obj, tensor)):
            return NotImplemented
        # A meta tensor has no data to copy; torch pickles it and the worker drops it.
        if obj.device.type == "meta":
            return NotImplemented
        # For an accelerator tensor this waits for the value: log() snapshots it.
        host = obj.detach().cpu()
        try:
            array = host.numpy()
            # An expanded (stride-0) view's array holds every element it repeats, more than its storage.
            if array.nbytes <= host.untyped_storage().nbytes():
                return array.__reduce_ex__(pickle.HIGHEST_PROTOCOL)
        except (TypeError, RuntimeError, AttributeError):
            # NumPy has no bfloat16 or float8, conjugated, sparse, nested and quantized tensors refuse, and torch 1.x has no untyped_storage.
            pass
        # Torch's own pickle, embedded as bytes so this override cannot turn the tensors a nested or quantized tensor rebuilds from into arrays.
        return pickle.loads, (pickle.dumps(host, protocol=pickle.HIGHEST_PROTOCOL),)


class _UnpacedGalleryItems(list):
    """A gallery log() published without waiting for the encoder: the worker charges it whole until encoded, since no wait bounds how many there are."""


class _ChildGalleryItems(_UnpacedGalleryItems):
    """A fork child's gallery, which no log() paces: the worker also never releases it against the parent's gallery count."""


def _snapshot_rich_queue_items(cdn_batches: list[tuple]) -> list[tuple]:
    """Return queue-safe immutable snapshots of public rich metric batches."""
    snapshots = []
    in_child = _init_pid is not None and os.getpid() != _init_pid
    # While a lane writes to disk, log() does not wait for the encoder (see _await_encoder).
    unpaced = in_child or bool(_lanes_on_disk is not None and _lanes_on_disk.value)
    marker = _ChildGalleryItems if in_child else _UnpacedGalleryItems
    for batch in cdn_batches:
        if unpaced and batch[0] in ("cdn_batch", "cdn_batch_mutation"):
            batch = (*batch[:3], marker(batch[3]), *batch[4:])
        if batch[0] in ("metadata_batch", "metadata_batch_mutation"):
            try:
                manifest = metadata_manifest(batch[3].data)
            except (TypeError, ValueError) as error:
                raise ValueError(
                    f"metadata metric {batch[1]!r} must contain finite "
                    "JSON-serializable values"
                ) from error
            # Freeze metadata to its rendered JSON, as _snapshot_config does, so the payload never carries tensors or other objects the worker might fail to unpickle.
            data = json.loads(manifest)["data"]
            # A lone surrogate (an os.fsdecode()d filename, cwd or argument) renders as a \ud escape that the dashboard's JSON reader rejects. Replace it as captions and filenames are, here rather than in the frozen metadata_manifest, so a spool written by an older client still replays the bytes it published. Escaped non-BMP text also matches and comes back unchanged.
            if b"\\ud" in manifest:
                data = readable_metadata(data)
            batch = (*batch[:3], Metadata(data), *batch[4:])
        try:
            # Standard pickle copies array/tensor storage into this byte string;
            # multiprocessing reducers may instead publish shared storage whose
            # later mutation would violate log()'s snapshot semantics.
            buffer = io.BytesIO()
            _HostTensorPickler(buffer, protocol=pickle.HIGHEST_PROTOCOL).dump(batch)
            payload = buffer.getvalue()
        except Exception as error:
            raise TypeError(
                f"rich metric {batch[1]!r} cannot be sent to the upload worker: "
                "its payload is not multiprocessing-serializable"
            ) from error
        snapshot = (_SERIALIZED_RICH_QUEUE_ITEM, payload)
        if batch[0] in ("cdn_batch", "cdn_batch_mutation"):
            snapshot = _GalleryQueueItem(snapshot)
        snapshots.append(snapshot)
    return snapshots


class _GalleryQueueItem(tuple):
    """A gallery's rich snapshot, which the parent counts as published once its put returns (_count_published_galleries), wherever it is held back meanwhile; the worker gets a plain tuple."""

    __slots__ = ()

    def __reduce__(self):
        return tuple, (tuple(self),)


def _rich_queue_tuple(
    kind: str, name: str, step: int, payload, timestamp_ms: int
) -> tuple:
    # A gallery also reserves its successor, before any later logical mutation: a replay may have to omit a child that the CDN now rejects permanently, and the reduced rebuild must stay in this writer's causal order.
    versions = _reserve_rich_mutation_versions(2 if kind == "cdn_batch" else 1)
    if versions is None:
        return (kind, name, step, payload)
    if kind != "cdn_batch":
        return (f"{kind}_mutation", name, step, payload, timestamp_ms, versions[0])
    return (f"{kind}_mutation", name, step, payload, timestamp_ms) + versions


def _queue_item_size(item) -> int:
    if isinstance(item, tuple):
        if item[0] == _NUMERIC_BATCH_QUEUE_ITEM:
            return len(item[3])
        if item[0] == _SERIALIZED_RICH_QUEUE_ITEM:
            return 1
        if item[0] == _NUMERIC_TEXT_BATCH_QUEUE_ITEM:
            return len(item[3]) + len(item[6])
    return len(item)


def _decode_queue_item(item):
    """Decode a numeric batch, its text points after its values, or a rich snapshot; accept raw queue groups (lists of point tuples) unchanged."""
    if isinstance(item, tuple) and item[0] in _NUMERIC_BATCH_KINDS:
        _, step, timestamp_ms, names, values, tags, *text = item
        points = [
            ("numeric_ts", name, step, value, timestamp_ms)
            if tag is None
            else ("numeric_tagged_ts", name, step, value, tag, timestamp_ms)
            for name, value, tag in zip(
                names, values, tags or [None] * len(names), strict=True
            )
        ]
        return points + text[0] if text else points
    if isinstance(item, tuple) and item[0] == _SERIALIZED_RICH_QUEUE_ITEM:
        return [pickle.loads(item[1])]
    return item


def _on_main_thread() -> bool:
    """Whether this is the thread signal handlers run on (in a fork child, the thread that forked)."""
    return threading.get_ident() == threading.main_thread().ident


def _main_thread_runs() -> bool:
    """Whether the main thread still runs the program: alive, and the interpreter's exit not begun. threading._shutdown sets the private threading._SHUTTING_DOWN (3.10 to 3.14) before it runs threading's atexit hooks (concurrent.futures' join among them), while the main thread still reads as alive; without that attribute this falls back to is_alive alone."""
    return threading.main_thread().is_alive() and not getattr(
        threading, "_SHUTTING_DOWN", False
    )


def _held_sections(frame) -> int:
    """How many held sections (_HELD_SECTIONS) run in frame and the frames that called it, down to a call of a handler kymo chains to (_call_chained)."""
    count = 0
    while frame is not None and frame.f_code is not _call_chained.__code__:
        # By identity: hashing a code object costs about 20 us for a large function on Python 3.13 and 3.14.
        if id(frame.f_code) in _HELD_SECTIONS:
            count += 1
        frame = frame.f_back
    return count


def _run_deferred_signals() -> None:
    """On the main thread, outside every held section, handle the waiting shutdown signals in arrival order through _handle_signal, never by raising them again, so a handler installed after init() that chained to kymo's at arrival does not run twice. They run in held rounds (_handle_deferred_signals), every one even if another raises; a signal arriving during a round waits for it instead of recursing (but see _call_chained). This frame is no held section and looks again after each round, so none arriving as a round ends, or here, is left for a later section."""
    if not _on_main_thread() or _held_sections(sys._getframe().f_back):
        return
    first = None
    while _deferred_signals:
        first = _handle_deferred_signals(first)
    if first is not None:
        raise first


def _handle_deferred_signals(first: Optional[BaseException]) -> Optional[BaseException]:
    """A round of _run_deferred_signals, a held section (_HELD_SECTIONS): handle the waiting signals until none is left; return first, or else the first exception one raised, but an exit over any other: a SIGTERM's after a Ctrl-C's KeyboardInterrupt ends the program, as it would have had the two not waited."""
    while True:
        try:
            # A handler that forks empties the list in the child (_forget_pending_in_child).
            signum, frame = _deferred_signals[0]
        except IndexError:
            return first
        # Taken off by a statement that calls nothing, so no handler runs between the read and it: one raising as a pop() returned would lose the signal. One raising as _handle_signal is entered still loses it, a narrower window.
        del _deferred_signals[0]
        try:
            _handle_signal(signum, frame)
        except BaseException as error:
            if first is None or (
                isinstance(error, SystemExit) and not isinstance(first, SystemExit)
            ):
                first = error


class _QueueStillFull(queue.Full):
    """No room on the queue by the publication's deadline (_wait_to_put): the group was not put."""


class _WorkerExited(RuntimeError):
    """The upload worker, which alone drains the queue, has exited (_check_worker)."""


def _check_worker() -> None:
    """Raise _WorkerExited once the upload worker has exited, with the failure it recorded, if any; only the initializing process, its parent, can tell."""
    if os.getpid() != _init_pid or _worker_alive(_upload_process):
        return
    failure = _upload_failure.get_obj().value
    # A gallery cut short is the one failure the worker records and carries on after, so it is never the cause of its exit.
    if failure and failure != _WORKER_GALLERY_CUT_SHORT:
        raise _WorkerExited(_upload_failure_message(failure))
    raise _WorkerExited(
        f"kymo upload worker died with {_queue_status.get_obj().value} "
        f"points still queued (exitcode {_worker_exitcode(_upload_process)}). If it crashed "
        "at startup on macOS, your script is likely missing an "
        '`if __name__ == "__main__":` guard.'
    )


def _worker_alive(process, signum: int = 0) -> bool:
    """Whether the upload worker still runs, having sent it signum if it does (0: none); every check and kill of the worker comes here.

    Under the forkserver start method a group SIGTERM or SIGQUIT (Slurm's scancel, tini -g, dumb-init) kills the forkserver, while kymo's policy keeps the worker running (kymo._worker._own_signals): the worker's Popen then reads its server's death as the worker's exit, returncode 255, and signals it no more. There the exit the worker publishes (_worker_exit) and its pid tell whether it runs, and the signal goes to the pid. One killed before it published its exit is a zombie until the process it was reparented to reaps it, for good under a PID 1 that never reaps (a container's trainer), and on Linux reads as ended (_ended).
    """
    if process.is_alive():
        if signum:
            try:
                (process.kill if signum == signal.SIGKILL else process.terminate)()
            except Exception as error:
                _log.warning("failed to signal the upload worker: %s", error)
        return True
    if (
        process.exitcode != 255
        or getattr(process._popen, "method", None) != "forkserver"
        or _worker_exit.value >= 0
    ):
        return False
    try:
        os.kill(process.pid, signum)
    except OSError:
        return False
    return not _ended(process.pid)


def _worker_exitcode(process) -> Optional[int]:
    """The upload worker's exit code: the one it published (_worker_exit) where its forkserver's death had its Popen read 255 (_worker_alive)."""
    code = process.exitcode
    if code == 255 and _worker_exit.value >= 0:
        return _worker_exit.value
    return code


def _upload_failure_message(failure: int) -> str:
    """What a failure the upload worker recorded in _upload_failure means for delivery."""
    if failure == _WORKER_EXIT_RUN_DELETED:
        return (
            "kymo run is deleted or no longer exists; queued points "
            "were rejected and will not be retried or spooled"
        )
    if failure == _WORKER_GALLERY_CUT_SHORT:
        return (
            "kymo upload worker published or spooled a gallery without items "
            "that could not be encoded or that the CDN rejected (see its log)"
        )
    if failure == _WORKER_EXIT_DATA_LOSS:
        return (
            "an upload spool segment was quarantined as *.rejected: "
            "the kymo server rejected a rich-media update in it with "
            "DATA_LOSS, or it held records that can never be delivered "
            "(the worker log names the file and reason); later data "
            "remains eligible for delivery"
        )
    return (
        "kymo upload worker could not spool one or more undelivered "
        "points; delivery failed (disk full, spool unavailable, a "
        "gallery it could not encode, or a queued item it could not "
        "decode; see its log)"
    )


def _from_the_queue(error: BaseException) -> bool:
    """Whether the queue itself refused the group with error, so certainly did not take it: off the main thread, where no handler runs, any error; on it, a queue.Full, or a ValueError once the queue is closed, from the queue's own put() (a handler raising inside put(), perhaps after the queue took the group, is the traceback's innermost frame instead)."""
    if not _on_main_thread():
        return True
    if not isinstance(error, (ValueError, queue.Full)):
        return False
    tb = error.__traceback__
    while tb.tb_next is not None:
        tb = tb.tb_next
    if tb.tb_frame.f_code is not type(_metric_queue).put.__code__:
        return False
    return isinstance(error, queue.Full) or _metric_queue._closed


class _AccountingLockUnavailable(RuntimeError):
    """The accounting lock stayed busy (_charge_backlog): the backlog was not changed."""


def _charge_backlog(points: int, hard_deadline: Optional[float]) -> None:
    """Charge points to the backlog, waiting for the accounting lock at most one step (_lock_timeout), never forever on a dead owner."""
    lock = _queue_status.get_lock()
    if not lock.acquire(timeout=_lock_timeout(hard_deadline)):
        raise _AccountingLockUnavailable("kymo upload accounting lock is unavailable")
    try:
        _queue_status.get_obj().value += points
    finally:
        lock.release()


def _delivery_status_snapshot(timeout: float) -> tuple[int, int, int] | None:
    """Read backlog/failure/spool state under the accounting lock, and lower _unproven_points to the backlog read in the same step, under _unproven_lock: lowering it from a read that came before a charge another thread then counted as unproven would erase that count."""
    global _unproven_points
    try:
        with _unproven_lock:
            snapshot = _read_delivery_status(timeout)
            if snapshot is not None:
                _unproven_points = min(_unproven_points, max(0, snapshot[0]))
            return snapshot
    finally:
        if _deferred_signals:
            _run_deferred_signals()


def _read_delivery_status(timeout: float) -> tuple[int, int, int] | None:
    """The read for _delivery_status_snapshot, a held section (_HELD_SECTIONS)."""
    lock = _queue_status.get_lock()
    if not lock.acquire(timeout=timeout):
        return None
    try:
        remaining = _queue_status.get_obj().value
        failure = _upload_failure.get_obj().value
        spooled = _upload_spooled.get_obj().value
        return remaining, failure, spooled
    finally:
        lock.release()


def _publish_queue_items(
    items: list,
    *,
    capture_ms: Optional[int] = None,
    hard_deadline: Optional[float] = None,
    galleries: bool = False,
    final: bool = False,
) -> None:
    """Publish queue groups with backlog accounting (_publish_held), then handle the shutdown signals it deferred, and publish the groups a full queue held back while one waited once its handler has returned. galleries tells whether items holds a gallery (_count_published_galleries); final, whether its drain of the captured output is the shutdown's last (_capture.drain_buffers)."""
    while True:
        try:
            held_back = _publish_held(
                items,
                capture_ms=capture_ms,
                hard_deadline=hard_deadline,
                galleries=galleries,
                final=final,
            )
        finally:
            if _deferred_signals:
                _run_deferred_signals()
        if not held_back:
            return
        items, capture_ms, galleries = [], None, False


def _publish_held(
    items: list,
    *,
    capture_ms: Optional[int] = None,
    hard_deadline: Optional[float] = None,
    take=None,
    galleries: bool = False,
    final: bool = False,
) -> bool:
    """Publish queue groups, skipping empty ones, as a held section (_HELD_SECTIONS): with take, the groups take() hands over inside it (_publish_oldest_pending); with capture_ms, the captured output, drained inside it (with final, by the shutdown's last drain: _capture.drain_buffers), as text points in the first group if that is a numeric batch (_NUMERIC_TEXT_BATCH_QUEUE_ITEM), else after it. On the main thread the groups held back earlier go first: those charged (_held_back), then those not (_uncharged).

    galleries tells whether items holds a gallery to count once its put returns (_count_published_galleries); take()'s groups and those held back are always counted.

    The backlog is charged before any put, since the worker can consume a group at once. If the charge fails, the groups earlier calls accepted that this publication took over (take()'s, the drained text, _uncharged's) go back to _uncharged, and it raises _WorkerExited once the worker has exited, else the lock's _AccountingLockUnavailable, or a handler's exception, which may have followed the charge and so counts it as unproven (_unproven_points). Only the queue's own refusal (_from_the_queue) rolls back a charge, this publication's for the refused group and those after it; of these groups, the ones held back before go back charged and the ones earlier calls accepted uncharged, so a shutdown reports what it drops. After any other exception a group whose put it interrupted, which the queue may have taken, keeps its charge, counted as unproven, and the groups after it (all of them if no put was under way) go back charged.

    On a full queue a put waits in steps (_wait_to_put); the waiting group and those after it are held back, still charged, when a step ends with a shutdown signal deferred (this then returns True), past hard_deadline (_QueueStillFull), or once the worker has exited (_WorkerExited). hard_deadline also bounds the accounting lock's waits.
    """
    taken = []
    if take is not None:
        items = taken = take()
        # A pending call's galleries, its GPU ones snapshotted only now.
        galleries = True
    text_points = None
    held, uncharged, total = [], [], 0
    # Whether the backlog is charged, and how many groups' puts have begun and returned: each changes in one store, so an exception anywhere finds what the publication holds.
    charged = False
    started = done = 0
    try:
        if capture_ms is not None and _capture.has_output():
            text_points = _drain_capture_points(capture_ms, final)
            if text_points:
                first = items[0] if items else None
                if type(first) is tuple and first[0] == _NUMERIC_BATCH_QUEUE_ITEM:
                    # One group: the text costs log() no second put, pickle or queue slot.
                    items = [
                        (_NUMERIC_TEXT_BATCH_QUEUE_ITEM, *first[1:], text_points),
                        *items[1:],
                    ]
                else:
                    items = [*items[:1], text_points, *items[1:]]
        if (_held_back or _uncharged) and _on_main_thread():
            _take_held_back(_held_back, held)
            _take_held_back(_uncharged, uncharged)
            items = [*held, *uncharged, *items]
            galleries = True
        sizes = [_queue_item_size(item) for item in items]
        total = sum(sizes[len(held) :])
        if total:
            _charge_backlog(total, hard_deadline)
        charged = True
        # A for loop: Python 3.13 leaves a conditional while loop's jump back, where a handler may raise, outside the try.
        for item, size in zip(items, sizes):
            started = done + 1
            if size:
                try:
                    _metric_queue.put(item, block=False)
                except queue.Full as error:
                    if not _from_the_queue(error):
                        raise
                    try:
                        put = _wait_to_put(item, hard_deadline)
                    except (_QueueStillFull, _WorkerExited):
                        # Not put: no put is under way from here, so a handler's raise below finds the groups held back or holds them back itself, never twice.
                        started = done
                        _hold_back(items[done:])
                        raise
                    if not put:
                        # It gave way to a deferred shutdown signal, as above.
                        started = done
                        _hold_back(items[done:])
                        return True
            done += 1
    except (_QueueStillFull, _WorkerExited):
        # Held back as they were raised.
        raise
    except BaseException as error:
        if not charged:
            _hold_back(held)
            if text_points:
                taken = [text_points]
            _hold_back([*uncharged, *taken], _uncharged)
            if isinstance(error, _AccountingLockUnavailable):
                _check_worker()
            else:
                # It may have come after the charge.
                _add_unproven(total)
        elif _from_the_queue(error):
            # The queue's refusal took none of the groups from done on.
            first = max(done, len(held))
            try:
                _charge_backlog(-sum(sizes[first:]), hard_deadline)
            except Exception as e:
                _log.error("failed to roll back upload accounting: %s", e)
            _hold_back(items[done:first])
            accepted = set(map(id, [*uncharged, *taken, text_points]))
            # Of the batch the text joined, the caller's own (take() hands over no captured output), only the text.
            rest = [
                group[6]
                if type(group) is tuple and group[0] == _NUMERIC_TEXT_BATCH_QUEUE_ITEM
                else group
                for group in items[first:]
            ]
            _hold_back(
                list(filter(lambda group: id(group) in accepted, rest)), _uncharged
            )
        elif started > done:
            # It may have come after the queue took this group, which keeps its charge (_unproven_points); the next publication puts those after it.
            _add_unproven(sizes[done])
            _hold_back(items[done + 1 :])
        else:
            # Not a group the publication held back before the raise. Only the main thread, where handlers raise, takes the groups held back.
            held_now = set(map(id, _held_back))
            _hold_back(
                list(filter(lambda group: id(group) not in held_now, items[done:]))
            )
        raise
    finally:
        if galleries and done:
            _count_published_galleries(items[:done])
    return False


def _hold_back(groups: list, held: Optional[collections.deque] = None) -> None:
    """Hold back groups the queue has not taken: charged ones (_held_back), or with held, uncharged ones (_uncharged). On the main thread ahead of the rest, since its publication took the earlier ones first (_take_held_back) and any held back meanwhile are newer; on another thread, whose publications never take any, after them."""
    if held is None:
        held = _held_back
    if _on_main_thread():
        held.extendleft(reversed(groups))
    else:
        held.extend(groups)


def _take_held_back(
    groups: Optional[collections.deque] = None, into: Optional[list] = None
) -> list:
    """Take the groups held back (_held_back, or groups), oldest first, one at a time, so a group another thread, a nested publication or a shutdown takes meanwhile is never taken twice; into a list given (into), so that a caller an exception interrupts finds them there."""
    if groups is None:
        groups = _held_back
    taken = [] if into is None else into
    try:
        # Popped and appended in C, where no handler runs: one raising as a popleft() returned, before its append, would lose that group.
        taken.extend(iter(groups.popleft, None))
    except IndexError:
        pass
    return taken


def _lock_timeout(hard_deadline: Optional[float]) -> float:
    """How long a step may wait on the accounting lock or for queue room: the lock timeout, within hard_deadline."""
    if hard_deadline is None:
        return _STATUS_LOCK_TIMEOUT
    return min(_STATUS_LOCK_TIMEOUT, _remaining_seconds(hard_deadline))


def _wait_to_put(item, hard_deadline: Optional[float]) -> bool:
    """Put an item the queue had no room for in steps of the lock timeout: False, not put, once a step ends on the main thread with a shutdown signal deferred and no other held section running; _QueueStillFull past hard_deadline, or in the shutdown in progress on this thread past its own deadline, which a signal can bring forward, or once it is handed off (_Shutdown); _WorkerExited once the upload worker, which alone makes room, has exited."""
    while True:
        _check_worker()
        shutdown = _shutdown_claim.get("shutdown")
        if shutdown is not None and shutdown.thread == threading.get_ident():
            if _handed_off:
                raise _QueueStillFull
            if hard_deadline is None or shutdown.hard < hard_deadline:
                hard_deadline = shutdown.hard
        if hard_deadline is not None and _remaining_seconds(hard_deadline) <= 0:
            raise _QueueStillFull
        if (
            _deferred_signals
            and _on_main_thread()
            and _held_sections(sys._getframe().f_back) == 1
        ):
            return False
        try:
            _metric_queue.put(item, timeout=_lock_timeout(hard_deadline))
            return True
        except queue.Full as error:
            if not _from_the_queue(error):
                raise


def _add_unproven(points: int) -> None:
    """Count charged points nothing may ever resolve (_unproven_points)."""
    global _unproven_points
    with _unproven_lock:
        _unproven_points += points


def _count_published_galleries(groups: list) -> None:
    """Count the galleries among groups whose puts have returned (_GalleryQueueItem) as published, for _await_encoder; they become the depth the next gallery-publishing call leaves raw. Unlocked: an increment a race loses only lets a later call wait less."""
    global _previous_galleries, _galleries_published
    galleries = sum(type(group) is _GalleryQueueItem for group in groups)
    if galleries:
        _galleries_published += galleries
        _previous_galleries = galleries


_ENCODER_POLL_INTERVAL = 0.001


def _await_encoder() -> None:
    """Wait while a gallery published before the previous gallery-publishing call is still raw in the worker: one call encoding and one queued keep the encoder busy, and later calls wait rather than pile raw copies up. It never waits while either lane writes new work to disk, so a server outage never delays training.

    Callers run it before their publication, outside every held section (_HELD_SECTIONS), so a shutdown signal during the wait is handled at once: a Ctrl-C raises its KeyboardInterrupt here, and kymo's own SIGTERM flush runs here, its shutdown ending the wait (_finishing). Nor does it wait within the signal budget of a SIGTERM or SIGQUIT a handler installed before init() took (_terminated_at), leaving what is left of that budget to the atexit flush.
    """
    if _galleries_released is None or os.getpid() != _init_pid:
        return
    budget = None
    while (
        _galleries_published - _galleries_released.value > _previous_galleries
        and not _lanes_on_disk.value
        and not _upload_terminal.get_obj().value
        and not _finishing
        and _worker_alive(_upload_process)
    ):
        if _terminated_at is not None:
            # Read once, and only then: each read scans the environment.
            if budget is None:
                budget = _signal_budget()
            if _terminated_at >= time.monotonic() - budget:
                return
        time.sleep(_ENCODER_POLL_INTERVAL)


def _warn_if_backlogged() -> None:
    """Warn (throttled) when the upload backlog grows, so a struggling server
    is visible during the run rather than at the end-of-run flush."""
    global _last_backlog_warn
    # This warning is advisory; a racy raw read is preferable to letting a
    # worker killed inside an accounting update freeze the logging thread.
    backlog = _queue_status.get_obj().value
    if backlog < _BACKLOG_WARN_THRESHOLD:
        return
    now = time.monotonic()
    if now - _last_backlog_warn < _BACKLOG_WARN_INTERVAL:
        return
    _last_backlog_warn = now
    # Don't blame the server (a backlog means it's down and the worker is retrying, or the run exceeds the upload ceiling) — state the fact, leave diagnosis to the surrounding logs.
    _log.warning(
        "upload backlog: %d points queued. Undelivered points spool to disk at "
        "shutdown (replayable via `python -m kymo.sync`).",
        backlog,
    )


class _PendingLog:
    """A log() call whose GPU values are being copied to the host, or a later write queued behind one.

    item is the write's queue item, published before rich: for a log() call its numeric batch, which its GPU values join once copied.
    """

    def __init__(
        self,
        item,
        rich: list,
        *,
        step: int = 0,
        gpu_keys: list = (),
        gpu_batches: list = (),
        galleries: bool = False,
    ):
        self.item = item
        self.rich = rich
        # Whether rich holds a gallery, for which a publication from log() waits for the encoder, as it does for the GPU batches.
        self.galleries = galleries
        self.step = step
        self.gpu_keys = gpu_keys
        self.gpu_batches = gpu_batches
        self.reads = None
        # Budget 16 host bytes per scalar, its 8-byte gather word and 8-byte result; the images' data is still on the GPU.
        self.nbytes = (
            16 * len(gpu_keys)
            + sum(image.data.nbytes for batch in gpu_batches for image in batch[3])
            + sum(len(queued[1]) for queued in rich)
        )


def _queue_pending_log(torch, entry: _PendingLog, gpu_scalars: list) -> None:
    """Start a log() call's GPU-to-host copies and queue it behind the pending calls, publishing every call whose copies have completed."""
    with _pending_lock:
        # Querying or waiting on an event during a capture can invalidate it. After a CUDA error the check raises, and nothing can be capturing.
        try:
            capturing = torch is not None and torch.cuda.is_current_stream_capturing()
        except Exception:
            capturing = False
        if capturing:
            if gpu_scalars or entry.gpu_batches:
                raise RuntimeError(
                    "kymo.log() cannot read CUDA tensors during CUDA graph capture; "
                    "log them outside the captured region"
                )
            _pending.append(entry)
            return
        held = entry.nbytes + sum(pending.nbytes for pending in _pending)
        # A for loop: Python 3.13 leaves a conditional while loop's jump back, where a handler may raise, outside the with (cpython#130279; test_no_loop_leaves_its_handler_at_the_back_edge).
        for oldest in list(_pending):
            # A handler kymo chains to may have published the pending calls meanwhile.
            if not _pending or held <= _PENDING_MAX_BYTES:
                break
            held -= oldest.nbytes
            _publish_oldest_pending(None, wait_for_encoder=True)
        images = [image for batch in entry.gpu_batches for image in batch[3]]
        if gpu_scalars or images:
            entry.reads = _gpu.Reads(
                torch, gpu_scalars, [image.data for image in images]
            )
            for image, host in zip(images, entry.reads.images):
                image.data = host
        _pending.append(entry)
        # A past deadline publishes only the calls whose copies have completed.
        _publish_pending(0.0, wait_for_encoder=True)


def _publish_pending(
    deadline: Optional[float],
    hard_deadline: Optional[float] = None,
    *,
    wait_for_encoder: bool = False,
) -> bool:
    """Publish the pending calls, oldest first, as their copies complete, waiting until the time.monotonic() deadline (None: as long as they take), and until hard_deadline for the accounting lock and room on the queue (_publish_oldest_pending); True once none remain."""
    while _pending:
        if not _publish_oldest_pending(
            deadline, hard_deadline=hard_deadline, wait_for_encoder=wait_for_encoder
        ):
            return False
    return True


def _publish_oldest_pending(
    deadline: Optional[float],
    *,
    drop_unfinished: bool = False,
    hard_deadline: Optional[float] = None,
    wait_for_encoder: bool = False,
) -> bool:
    """Publish the oldest pending call once its copies complete, waiting until the time.monotonic() deadline (None: as long as they take).

    If they have not completed by then, return False and keep it, or with drop_unfinished publish it without its GPU values; a CUDA error does the latter. The call counts as published even with groups its publication held back (_publish_held: a queue full by hard_deadline or while a shutdown signal waits, an exited worker, a busy accounting lock), which the caller's next publication puts first.
    A call published from log() (wait_for_encoder) that has galleries first waits for the encoder, as log() does, outside the publication's held section.
    """
    global _pending_incomplete
    try:
        entry = _pending[0]
    except IndexError:
        # A handler's log(), run as this call began, published every pending call since the caller found one.
        return True
    failure = None
    if entry.reads is not None:
        try:
            if not entry.reads.wait(deadline):
                if not drop_unfinished:
                    return False
                failure = "their copies to the host did not complete"
        except Exception as error:
            # A CUDA error is sticky: the copies never complete, so Reads stays in _gpu.unfreeable.
            failure = f"reading them failed: {error}"
    if wait_for_encoder and (entry.galleries or (entry.gpu_batches and not failure)):
        # Before the held section, so a shutdown signal during the wait is handled at once; take() checks that the call is still pending.
        _await_encoder()
    # _pending_incomplete before take().
    before = None

    def take() -> list:
        # Inside the publication's held section, so kymo's shutdown handler finds the call pending, put or held back, never none of these. The flag holds the call itself before its claim, so a handler kymo does not own that interrupts neither publishes it again nor sees delivery complete; a lost value, here or in such a handler's log() nested in the hand-over, sets it True, and after the publication it is restored only if it still holds the call.
        nonlocal before
        global _pending_incomplete
        before, _pending_incomplete = _pending_incomplete, entry
        # Claimed in one call, unless a handler's log() or finish() published it during the waits above or here: a check before a pop leaves a point between them where Python 3.10 runs handlers (a taken jump), and a handler's log() there would have this publish it again and pop the next call.
        try:
            _pending.remove(entry)
        except ValueError:
            return []
        rich, dropped = entry.rich, []
        if failure is not None:
            _log.warning(
                "dropped %d GPU value(s) of step %d: %s",
                len(entry.gpu_keys) + len(entry.gpu_batches),
                entry.step,
                failure,
            )
            _pending_incomplete = True
        elif entry.reads is not None:
            # Only log() starts reads, so item is its numeric batch.
            names, values, tags = entry.item[3:]
            for index, value in entry.reads.scalars():
                name, tag = entry.gpu_keys[index]
                if not -_F32_OVERFLOW < value < _F32_OVERFLOW:
                    try:
                        value = _normalize_numeric_value(value)
                    except OverflowError:
                        dropped.append(name)
                        continue
                names.append(name)
                values.append(value)
                tags.append(tag)
            for batch in entry.gpu_batches:
                # A handoff's publications end by hard_deadline, and a large gallery's pickling would hold the console past it.
                if _handed_off and time.monotonic() >= (hard_deadline or math.inf):
                    dropped.append(batch[1])
                    continue
                try:
                    rich.extend(_snapshot_rich_queue_items([batch]))
                except TypeError:
                    dropped.append(batch[1])
        if dropped:
            _log.warning(
                "dropped %d value(s) of step %d that cannot be sent: %s",
                len(dropped),
                entry.step,
                ", ".join(sorted(set(dropped))),
            )
            _pending_incomplete = True
        return [entry.item, *rich]

    try:
        try:
            _publish_held([], take=take, hard_deadline=hard_deadline)
        except (_QueueStillFull, _WorkerExited, _AccountingLockUnavailable):
            pass
        except Exception as error:
            _log.warning(
                "failed to publish the values of step %d: %s", entry.step, error
            )
            _pending_incomplete = True
        if _pending_incomplete is entry:
            _pending_incomplete = before
    finally:
        # Once the flag is restored and outside the except: an exception a deferred handler raises is the caller's, never a failed publication.
        if _deferred_signals:
            _run_deferred_signals()
    return True


def _publish_in_log_order(entry: _PendingLog) -> None:
    """Publish a write now, or behind the pending log() calls, so it never overtakes one; never touches the GPU."""
    with _pending_lock:
        if _pending:
            _pending.append(entry)
            return
        _publish_queue_items([entry.item, *entry.rich])


def log_cdn(cdn_keys: dict[str, str], step: int) -> None:
    """Queue CDN key metrics for upload."""
    if not _is_initialized:
        raise RuntimeError("kymo not initialised — call kymo.init() first")

    step = _normalize_step(step)
    # cdn_ts, not the legacy timestampless "cdn": a zero timestamp never advances the run's heartbeat, so a CDN-only run read as STUCK/presumed-dead while actively logging.
    now_ms = int(time.time() * 1000)
    points = []
    for name, key in cdn_keys.items():
        name = _normalize_metric_name(name)
        if not isinstance(key, str):
            raise TypeError(
                f"CDN key for {name!r} must be str, got {type(key).__name__}"
            )
        key = str(key)
        try:
            point_bytes = _estimate_tuple_bytes(("cdn_ts", name, step, key, now_ms))
        except UnicodeEncodeError as error:
            raise ValueError(f"CDN key for {name!r} is not valid UTF-8") from error
        if point_bytes > _MAX_POINT_BYTES:
            raise ValueError(
                f"CDN key for {name!r} is too large to ingest "
                f"({point_bytes} encoded bytes; limit {_MAX_POINT_BYTES})"
            )
        if (
            _rich_writer_epoch is not None
            and len(key.encode("utf-8")) > _MAX_RICH_RESOURCE_ID_BYTES
        ):
            raise ValueError(
                f"CDN key for {name!r} exceeds the versioned rich-write limit "
                f"of {_MAX_RICH_RESOURCE_ID_BYTES} UTF-8 bytes"
            )
        versions = _reserve_rich_mutation_versions(1)
        points.append(
            ("cdn_ts", name, step, key, now_ms)
            if versions is None
            else ("cdn_key_mutation", name, step, key, now_ms, versions[0])
        )

    _publish_in_log_order(_PendingLog(points, [], step=step))
    _warn_if_backlogged()


def _normalize_timeout_budget(value: float, name: str) -> float:
    value = float(value)
    if not math.isfinite(value) or value < 0:
        _log.warning("invalid %s timeout %r; using a zero-second budget", name, value)
        return 0.0
    return value


def wait_for_upload(timeout: Optional[float] = None) -> bool:
    """Block until all queued metrics are uploaded.

    It first waits for the GPU values of pending log() calls, so on the
    calling thread it waits for the GPU work queued before them.

    Returns True only if everything reached the server. Returns False on a
    timeout, once any point has failed over to the replayable disk spool, or
    once a value was dropped after its log() returned.

    A log() that an exception from a signal handler interrupted while it put
    its values keeps their charge, since the queue may have taken them. Once
    the backlog has gone no lower than that many points for longer than the
    server's 2 s flush interval, it returns False: their delivery cannot be
    proven. So does a run a Ctrl-C handed off (see finish()).

    Raises:
        RuntimeError: if the upload worker has died with points still queued,
            or if it could not spool an undeliverable point. In either case
            successful delivery can no longer be reported.
    """
    if not _is_initialized:
        return not _handed_off
    if timeout is not None:
        timeout = _normalize_timeout_budget(timeout, "upload")
    deadline = None if timeout is None else time.monotonic() + timeout
    # Another thread may hold the pending calls while it waits for the GPU.
    if not _pending_lock.acquire(
        timeout=-1 if deadline is None else _remaining_seconds(deadline)
    ):
        _log.warning("timeout — another thread holds the pending log() calls")
        return False
    try:
        # The timeout bounds the waits for the GPU, the accounting lock and queue room: a pending call's groups the lock or the queue does not take by then stay held back.
        if not _publish_pending(deadline, deadline):
            _log.warning(
                "timeout — %d log() call(s) still wait for their GPU values",
                len(_pending),
            )
            return False
    finally:
        _pending_lock.release()
    wait_time = 0.05
    accounting_unavailable_since = None
    # The lowest backlog seen, and since when it has not gone lower: the poller's publications, when it has something to publish, raise and lower it every 2 s.
    lowest, steady_since = None, 0.0

    while True:
        # The worker publishes a spill failure before its matching backlog
        # decrement under this same lock.  Read the pair atomically so zero can
        # never win the race against a not-yet-visible eventual exit code.
        snapshot = _delivery_status_snapshot(
            0.1 if deadline is None else min(0.1, _remaining_seconds(deadline))
        )
        now = time.monotonic()
        if snapshot is None:
            if accounting_unavailable_since is None:
                accounting_unavailable_since = now
            _check_worker()
            if now - accounting_unavailable_since >= _STATUS_LOCK_TIMEOUT:
                raise RuntimeError(
                    "kymo upload accounting remained locked; the worker "
                    "may have died during an update"
                )
            if deadline is not None and now >= deadline:
                _log.warning("timeout waiting for upload accounting")
                return False
            continue
        accounting_unavailable_since = None
        remaining, failure, spooled = snapshot
        if lowest is None or remaining < lowest:
            lowest, steady_since = remaining, now
        if failure and failure != _WORKER_GALLERY_CUT_SHORT:
            raise RuntimeError(_upload_failure_message(failure))
        if spooled:
            _log.warning(
                "one or more points were spooled for later replay instead of uploaded"
            )
            return False
        if remaining <= 0 and (_pending_incomplete or failure):
            if _pending_incomplete:
                _log.warning(
                    "values were dropped after log() returned; see the warnings above"
                )
            if failure:
                _log.warning("%s", _upload_failure_message(failure))
            return False
        if _held_back or _uncharged:
            # Only the main thread's publications put the groups held back, charged or not, so a wait on another thread never sees them delivered; on the main thread it puts them, within the timeout.
            if not _on_main_thread():
                _log.warning(
                    "%d point(s) wait for the main thread to queue them",
                    sum(map(_queue_item_size, [*_held_back, *_uncharged])),
                )
                return False
            try:
                _publish_queue_items([], hard_deadline=deadline)
            except _QueueStillFull:
                _log.warning("timeout — the upload queue stayed full")
                return False
            except _AccountingLockUnavailable:
                if deadline is None:
                    raise
                _log.warning("timeout waiting for upload accounting")
                return False
            # Charged only now, groups held back uncharged were in no backlog read before: the lowest starts again.
            lowest = None
            continue
        if remaining < 0:
            # A point resolved twice: delivery cannot be proven.
            _log.error("upload accounting went below zero (%d)", remaining)
            return False
        if remaining == 0:
            return not _handed_off
        # A dead worker can't drain the queue: fail with the count instead of waiting forever.
        _check_worker()
        # Unproven points may yet be acknowledged, once fed, within the server's flush interval.
        if lowest <= _unproven_points and now - steady_since > _SERVER_FLUSH_INTERVAL:
            _log.warning(
                "at most %d point(s), whose queueing a signal handler "
                "interrupted, cannot be proven delivered",
                lowest,
            )
            return False
        if deadline is not None and now >= deadline:
            _log.warning("timeout — %d points still queued", remaining)
            return False
        sleep_for = min(wait_time, 1.0)
        if deadline is not None:
            sleep_for = min(sleep_for, deadline - now)
        time.sleep(sleep_for)
        wait_time *= 1.5


def finish(flush_timeout: Optional[float] = None) -> bool:
    """Flush against one ``flush_timeout`` deadline (default
    ``$KYMO_FLUSH_TIMEOUT`` or 60), spool the rest to disk, and shut down.

    The recommended end-of-run call: unlike ``wait_for_upload``, every client
    wait shares that deadline. A kernel-level filesystem or Queue operation can
    still outlive a Python timeout; parent salvage therefore runs in a daemon
    thread and an unquiesced console/system-metrics publication makes the result false.
    Spooled points are delivered later from any CPU-only machine with
    ``python -m kymo.sync``.

    log() calls whose GPU values are still being copied get half the budget;
    values not copied by then are dropped.

    A Ctrl-C during it ends the run at once and hands its remaining delivery
    to the upload worker, which delivers or spools what it can within 10
    seconds (or ``$KYMO_SIGNAL_FLUSH_TIMEOUT``, if shorter) in the background,
    and finish() then raises KeyboardInterrupt (or, under a SIGINT handler
    installed before init(), calls it and returns False). So does finish()
    called while a KeyboardInterrupt propagates (in a ``finally:`` or an
    ``except:`` block), or an exception raised while one was handled, or
    within the handoff's 10 seconds of a Ctrl-C (after an ``except
    KeyboardInterrupt:`` block, say), which then returns False. A SIGTERM or
    SIGQUIT bounds it by ``$KYMO_SIGNAL_FLUSH_TIMEOUT`` (default 10), and the
    process then exits as on that signal. Without a ``flush_timeout``, finish()
    called in or after a handler installed before init() that took one ends
    within that budget of the signal, or of the call once the budget has
    passed.

    A ``sys.exit()`` sets the run's exit code as it is called. Called on the
    main thread with no exception being handled, while that thread runs the
    program (not once the interpreter's exit has begun, as in an atexit hook),
    finish() drops a code only ``sys.exit()`` set, taking that ``SystemExit``
    for caught. So does a finish() in the teardown before that exit begins,
    with nothing being handled: a ``__del__`` or ``weakref.finalize``
    callback of an object an uncaught ``sys.exit()`` frees, or the printing
    of a ``sys.exit()`` message. That run ends with no code.
    A caught ``sys.exit()`` that the run outlives without such a finish() (a
    normal end, or a finish() on another thread) still ends it with that code,
    and a ``raise SystemExit`` (the ``exit()`` builtin's too) sets none. A
    crash through ``try: ... finally: kymo.finish()`` ends the run as finished:
    the exception hook's code comes after the run has ended.

    Returns True only if delivery was proven complete. Returns False if points
    were spooled or dropped, or the worker otherwise could not prove delivery.
    """
    global _exit_code, _exit_code_by_sys_exit
    reject_legacy_client_env()
    if not _is_initialized:
        return not _handed_off
    if _follows_ctrl_c(sys.exc_info()[1]):
        # The program is ending, or handling, a Ctrl-C, or ends soon after one: its run is, too.
        return _drain_and_shutdown(signum=signal.SIGINT)
    if (
        _on_main_thread()
        and sys.exc_info()[1] is None
        and _main_thread_runs()
        and _exit_code_by_sys_exit
    ):
        # Taken for caught: the program carried on from the SystemExit whose code this is. Not once the interpreter's exit has begun, where it may be the exit that ended the program.
        _exit_code, _exit_code_by_sys_exit = None, False
    if flush_timeout is None and _terminated_at is not None:
        # In or after a handler installed before init() that took a SIGTERM or SIGQUIT, within the signal budget, as the atexit flush (_ensure_metrics_uploaded): no worker policy bounds it, and the scheduler's grace period runs.
        flush_timeout = min(
            _default_flush_timeout(),
            _remaining_seconds(_signal_end(_terminated_at, _signal_budget())),
        )
    return _drain_and_shutdown(flush_timeout)


# ---------------------------------------------------------------------------
# Run metadata collection & upload
# ---------------------------------------------------------------------------


def _snapshot_config(config: dict) -> dict:
    """Own the config tree exactly as the metadata JSON will represent it."""
    if not isinstance(config, dict):
        raise TypeError(f"kymo config must be dict, got {type(config).__name__}")
    try:
        encoded = json.dumps(config, default=str, allow_nan=False)
        return json.loads(encoded)
    except (TypeError, ValueError) as error:
        raise ValueError(
            "kymo config must contain finite JSON-serializable values"
        ) from error


def _strip_url_userinfo(value: str) -> str:
    """Remove standardized URL credentials without rewriting SCP-style remotes."""
    try:
        parsed = urlsplit(value)
    except ValueError:
        return "<unparseable URL omitted>"
    if "@" not in parsed.netloc:
        return value
    return parsed._replace(netloc=parsed.netloc.rsplit("@", 1)[1]).geturl()


# Bounds the git calls of the run metadata, which init() collects on the caller's thread: a git that a slow or hung filesystem stalls must not hold init() forever. A timeout drops the git fields from that call on, with a warning; any other git failure drops its own field quietly.
_GIT_METADATA_TIMEOUT = 5.0


def _collect_run_metadata(config: Optional[dict]) -> dict:
    """Collect system info, git, SLURM env, and user config into a structured dict."""
    import datetime
    import getpass
    import platform
    import socket

    meta = {}

    # Time
    now = datetime.datetime.now()
    meta["time"] = {
        "start": now.strftime("%B %d, %Y %I:%M:%S %p"),
        "start_unix": int(now.timestamp()),
    }

    # System
    system = {
        "hostname": socket.gethostname(),
        "os": platform.system(),
        "username": getpass.getuser(),
        "cwd": os.getcwd(),
        "command": shlex.join(sys.argv),
        "python_version": platform.python_version(),
        "python_executable": sys.executable,
        "cpu_count": os.cpu_count(),
    }
    try:
        system["logical_cpu_count"] = len(os.sched_getaffinity(0))
    except Exception:
        pass
    try:
        import grp

        system["group"] = grp.getgrgid(os.getgid()).gr_name
    except Exception:
        pass
    try:
        import pynvml

        pynvml.nvmlInit()
        try:
            count = pynvml.nvmlDeviceGetCount()
            system["gpu_count"] = count
            if count > 0:
                h = pynvml.nvmlDeviceGetHandleByIndex(0)
                system["gpu_type"] = pynvml.nvmlDeviceGetName(h)
        finally:
            pynvml.nvmlShutdown()
    except Exception:
        pass
    meta["system"] = system

    # Git at the launched training script's directory, not cwd. `git -C` walks up to the repo root from there.
    git = {}
    repo_dir = (
        os.path.dirname(os.path.abspath(sys.argv[0]))
        if sys.argv and os.path.isfile(sys.argv[0])
        else os.getcwd()
    )
    for key, args in (
        ("remote", ["remote", "get-url", "origin"]),
        ("commit", ["rev-parse", "HEAD"]),
    ):
        try:
            value = (
                subprocess.check_output(
                    ["git", "-C", repo_dir, *args],
                    stderr=subprocess.DEVNULL,
                    timeout=_GIT_METADATA_TIMEOUT,
                )
                .decode()
                .strip()
            )
            git[key] = _strip_url_userinfo(value) if key == "remote" else value
        except subprocess.TimeoutExpired:
            _log.warning(
                "git %s took longer than %g s in %s: the run's metadata goes without the git fields from that call on",
                " ".join(args),
                _GIT_METADATA_TIMEOUT,
                repo_dir,
            )
            # A file system that stalls one git call stalls the next.
            break
        except Exception:
            pass
    if git:
        meta["git"] = git

    # SLURM env vars
    # SLURM_JWT is a bearer token used by Slurm's REST/JWT authentication, not scheduler context. Persisting it in info/run_info would expose the credential anywhere run metadata is viewed or downloaded.
    slurm = {
        k: v
        for k, v in os.environ.items()
        if k.startswith("SLURM") and k != "SLURM_JWT"
    }
    if slurm:
        meta["slurm"] = slurm

    result = {"meta": meta}
    if config:
        result["config"] = config
    return result


def _push_run_metadata():
    """Queue one immutable metadata snapshot on the ordered rich lane."""
    if not _is_initialized or _run_metadata is None:
        return
    _publish_in_log_order(
        _PendingLog(
            [],
            _snapshot_rich_queue_items(
                [
                    _rich_queue_tuple(
                        "metadata_batch",
                        "info/run_info",
                        0,
                        Metadata(_run_metadata),
                        int(time.time() * 1000),
                    )
                ]
            ),
        )
    )


def update_config(config: dict) -> None:
    """Merge new entries into the run's config and re-upload the metadata blob.

    Keys present in ``config`` overwrite existing keys (shallow merge); other
    fields (time, system, git, slurm) carry over bit-identical from the
    initial upload — only the ``config`` field is mutated.
    """
    global _run_metadata
    if not _is_initialized:
        raise RuntimeError("kymo not initialised — call kymo.init() first")
    config = _snapshot_config(config)
    if _run_metadata is None:
        _run_metadata = _collect_run_metadata(config)
    else:
        existing = _run_metadata.get("config", {})
        if not isinstance(existing, dict):
            existing = {}
        existing.update(config)
        _run_metadata["config"] = existing
    _push_run_metadata()


# ---------------------------------------------------------------------------
# Signal and exit handling
# ---------------------------------------------------------------------------


# The shutdown signals the platform has (Windows has no SIGQUIT).
_TERMINATION_SIGNALS = tuple(
    getattr(signal, name)
    for name in ("SIGTERM", "SIGINT", "SIGQUIT")
    if hasattr(signal, name)
)
# Other signals that reach the whole process group (Slurm's --signal=USR1 for a requeue, submitit's USR2, a terminal's hangup), which the program may handle: the upload worker ignores them (kymo._worker._own_signals), blocked with the shutdown signals across its start, as the control process that sends TerminateRun does.
_GROUP_SIGNALS = tuple(
    getattr(signal, name)
    for name in ("SIGHUP", "SIGUSR1", "SIGUSR2")
    if hasattr(signal, name)
)


def _owned_signals() -> tuple:
    """The SIGTERM and SIGQUIT kymo handles itself, as the upload worker's policy (kymo._worker._own_signals): those the program left at their default when kymo first saw them (_setup_signal_handlers, which installs kymo's handler then), whose handler is still kymo's. One the program ignores or handles itself is the program's to act on, and its exit stops the worker as usual. The worker reads this once, as it starts: a handler the program installs later does not change its policy."""
    owned = []
    for signum in _TERMINATION_SIGNALS:
        if signum == signal.SIGINT:
            continue
        handler = signal.getsignal(signum)
        if handler is _signal_handler:
            handler = _original_signals.get(signum)
        # None: a handler installed from C, which kymo replaces and does not chain to (_handle_signal).
        if handler is signal.SIG_DFL or handler is None:
            owned.append(signum)
    return tuple(owned)


def _require_shutdown_signal_capability() -> None:
    """Prove SIGTERM can be wrapped before init performs any side effect."""
    sigterm = getattr(signal, "SIGTERM", None)
    if sigterm is None:
        return
    try:
        original = signal.getsignal(sigterm)
        if original is None:
            # Handler installed from C; re-setting None would TypeError, and
            # installing a Python handler over it still works in the main thread.
            return
        signal.signal(sigterm, original)
    except (ValueError, OSError) as error:
        raise RuntimeError(
            "kymo.init() must first be called from the main thread of the "
            "main Python interpreter so shutdown signal handlers can be installed"
        ) from error


def _setup_signal_handlers():
    for sig in _TERMINATION_SIGNALS:
        try:
            original = signal.getsignal(sig)
            if original is _signal_handler:
                # Left by an init() whose worker never started, where a raise cut its restore short (_restore_signal_handlers).
                continue
            _original_signals[sig] = original
            if original is signal.SIG_IGN:
                continue
            signal.signal(sig, _signal_handler)
        except (ValueError, OSError):
            pass


def _restore_signal_handlers() -> None:
    """Put back the handlers kymo's replaced (_setup_signal_handlers), after an init() whose worker never started: a SIGTERM or SIGQUIT then takes the program's own action again, not kymo's exit. One installed from C (None) cannot be put back."""
    for sig, original in _original_signals.items():
        if original is not None and signal.getsignal(sig) is _signal_handler:
            signal.signal(sig, original)


def _record_exit_code(code: int, by_sys_exit: bool = False) -> None:
    """First writer wins — a signal handler shouldn't be overridden by a
    later sys.exit cascade fired during cleanup. A code only sys.exit
    recorded (by_sys_exit, the wrapped sys.exit's) gives way while the main
    thread runs the program (_main_thread_runs), since the program may have
    caught that SystemExit. Once the interpreter's exit has begun no exit's
    code is recorded: the code stays, and a sys.exit then, which Python
    ignores, sets none."""
    global _exit_code, _exit_code_by_sys_exit
    code = int(code)
    if not _main_thread_runs():
        return
    if _exit_code is None or _exit_code_by_sys_exit:
        _exit_code, _exit_code_by_sys_exit = code, by_sys_exit


def _record_signal_code(signum: int) -> None:
    """Record a shutdown signal's code, 128 + signum, after which the run never reads as finished, not even after a sys.exit(0). A Ctrl-C's 130 replaces any code, whatever ended the program; a SIGTERM's or SIGQUIT's replaces none or 0, and replaces a code only sys.exit recorded as a later sys.exit would (_record_exit_code): while the main thread runs the program, not once the interpreter's exit has begun."""
    global _exit_code, _exit_code_by_sys_exit
    if signum == signal.SIGINT or not _exit_code:
        _exit_code, _exit_code_by_sys_exit = 128 + signum, False
    else:
        _record_exit_code(128 + signum)


def _excepthook(exc_type, exc_value, exc_tb):
    global _interrupted_at
    try:
        if _carries_on_interactively():
            # The interactive interpreter shows a statement's exception here and carries on: the run goes on, with no exit code recorded, and after a KeyboardInterrupt, as after any Ctrl-C a program carries on from, no later exit is taken to follow this Ctrl-C (_follows_ctrl_c).
            if issubclass(exc_type, KeyboardInterrupt):
                _interrupted_at = None
                if _worker_interrupted_at is not None:
                    _worker_interrupted_at.value = 0.0
            return
        # SystemExit is captured by wrapped sys.exit/direct signal delegation below, so don't double-set. KeyboardInterrupt has the conventional shell code; other uncaught exceptions exit the interpreter with status 1.
        if not issubclass(exc_type, SystemExit):
            _record_exit_code(130 if issubclass(exc_type, KeyboardInterrupt) else 1)
        # An uncaught Ctrl-C, or an error after one, ends the program: hand the run off now, before the atexit hooks, which another library's may hold up (the atexit flush stays the fallback, _ensure_metrics_uploaded).
        if _follows_ctrl_c(exc_value):
            _drain_and_shutdown(signum=signal.SIGINT)
    finally:
        if _original_excepthook is not None:
            _original_excepthook(exc_type, exc_value, exc_tb)


def _carries_on_interactively() -> bool:
    """Whether the interpreter carries on into its interactive prompt after an uncaught exception, as CPython's main decides: it is at the prompt (sys.ps1), or the script ends into it, under python -i, or with PYTHONINSPECT set and a terminal on stdin."""
    if hasattr(sys, "ps1"):
        return True
    inspect = sys.flags.inspect or (
        not sys.flags.ignore_environment and os.environ.get("PYTHONINSPECT")
    )
    return bool(inspect) and (bool(sys.flags.interactive) or os.isatty(0))


def _exit_wrapper(code=0):
    # Only the main thread's exit ends the program: another thread's sys.exit ends that thread alone, so it neither sets the run's code nor hands the run off.
    if _on_main_thread():
        _record_exit_code(_system_exit_code(code), by_sys_exit=True)
        # An exit that follows a Ctrl-C the program did not carry on from (Lightning's and click's sys.exit(1), an `except KeyboardInterrupt: sys.exit(0)`) hands the run off before the atexit hooks, and the process keeps its code.
        if _follows_ctrl_c(sys.exc_info()[1]):
            _drain_and_shutdown(signum=signal.SIGINT)
    _original_sys_exit(code)


def _follows_ctrl_c(error: Optional[BaseException]) -> bool:
    """Whether the program's exit or finish(), with or while handling error, follows a Ctrl-C it did not carry on from: error is a KeyboardInterrupt or was raised while one was handled, through its __cause__ or its __context__ (an error raised from another cause has the KeyboardInterrupt as its context alone), or a Ctrl-C came no longer ago than the handoff's cap allows (_ctrl_c_at, _handoff_cap). Never in an interactive session, which carries on from every Ctrl-C: IPython and Jupyter show a cell's KeyboardInterrupt without sys.excepthook, and the 3.13+ REPL handles one at its prompt itself, so their notes stay (_excepthook clears them)."""
    if _carries_on_interactively():
        return False
    seen, chain = set(), [error]
    while chain:
        error = chain.pop()
        if error is None or id(error) in seen:
            continue
        if isinstance(error, KeyboardInterrupt):
            return True
        seen.add(id(error))
        chain += (error.__cause__, error.__context__)
    at = _ctrl_c_at()
    return at is not None and at >= time.monotonic() - _interrupt_budget()


def _ctrl_c_at() -> Optional[float]:
    """When the last Ctrl-C came (time.monotonic()), or None: the later of kymo's handler's (_interrupted_at) and the upload worker's (_worker_interrupted_at)."""
    at = _worker_interrupted_at.value if _worker_interrupted_at is not None else 0.0
    return max(at, _interrupted_at or 0.0) or None


def _system_exit_code(code) -> int:
    # sys.exit(None) is 0, integers pass through, and other values exit 1.
    return code if isinstance(code, int) else int(code is not None)


def _install_exit_capture():
    """Hook the three places an exit code can come from: uncaught exceptions
    (sys.excepthook), explicit sys.exit calls, and signals (already wired in
    _setup_signal_handlers). Installed once during init()."""
    global _original_excepthook, _original_sys_exit
    _original_excepthook = sys.excepthook
    sys.excepthook = _excepthook
    _original_sys_exit = sys.exit
    sys.exit = _exit_wrapper


def _signal_handler(signum, frame):
    """The handler kymo installs. A signal joins the waiting ones and is handled with them unless it interrupted a held section (_HELD_SECTIONS), which handles them as it ends. A Ctrl-C's arrival times the end of its run (_handoff_cap), and a SIGTERM's or SIGQUIT's the atexit flush's (_ensure_metrics_uploaded)."""
    global _interrupted_at, _terminated_at
    if signum == signal.SIGINT:
        _interrupted_at = time.monotonic()
    else:
        _terminated_at = time.monotonic()
    entry = [signum, frame]
    _deferred_signals.append(entry)
    _run_deferred_signals()
    # Still waiting if a held section deferred it, when its handler gets frame None.
    entry[1] = None


def _handle_signal(signum, frame):
    """Handle a shutdown signal. During a shutdown, which no signal nests in or aborts, kymo's policy acts at once (_signal_during_shutdown), and the rest waits until it returns if this interrupted it. Then kymo chains to a callable handler installed before init() (frame None for a deferred signal, as Python's handler contract allows), else handles the signal itself (_shut_down_on_signal). SIGINT's default disposition is such a handler, Python's own, whose KeyboardInterrupt, uncaught, hands the run off in kymo's exception hook (_excepthook)."""
    shutdown = _shutdown_claim.get("shutdown")
    if shutdown is not None:
        _signal_during_shutdown(shutdown, signum)
        if shutdown.thread == threading.get_ident():
            shutdown.arrived.append(signum)
            return
    handler = _original_signals.get(signum)
    if not callable(handler):
        handler = _shut_down_on_signal
    try:
        _call_chained(handler, signum, frame)
    except BaseException as error:
        if not _main_thread_runs():
            # Python ignores whatever the handler raises once the interpreter's exit has begun, an exit among them: the signal ends the run, as kymo's own handling would.
            _record_signal_code(signum)
        elif isinstance(error, SystemExit):
            _record_exit_code(_system_exit_code(error.code))
        raise


def _shut_down_on_signal(signum, frame):
    """kymo's own handling of a shutdown signal no handler installed before init() takes: a SIGINT hands the run off to the upload worker, a SIGTERM or SIGQUIT flushes within the signal budget, and kymo exits."""
    try:
        if not _shutdown_claim and _is_initialized:
            if signum == signal.SIGINT:
                _drain_and_shutdown(signum=signum)
            else:
                _log.warning("signal %d — flushing metrics before exit…", signum)
                _drain_and_shutdown(flush_timeout=_signal_budget(), signum=signum)
    finally:
        sys.exit(128 + signum)


def _signal_during_shutdown(shutdown: "_Shutdown", signum: int) -> None:
    """kymo's policy for a shutdown signal during a shutdown: a Ctrl-C ends it by the handoff's cap (_handoff_cap) and hands it off, and the shutdown returns at once (_Shutdown.hand_off); a SIGTERM or SIGQUIT bounds it by the signal budget (_Shutdown.bound). Either ends the run again with its code if the shutdown's own TerminateRun has gone out (_Shutdown.end_run), as _record_signal_code records it.

    Once a signal has bounded or handed off the shutdown, a SIGTERM or SIGQUIT only bounds it again: torchrun and Slurm can send two SIGTERMs milliseconds apart. A Ctrl-C escalates: it hands the worker what is left of the shutdown, by the handoff's cap at the latest, even once handed off, ends the run with 130, and exits at once. It runs in a held round (_handle_deferred_signals), so a further signal waits for it to return.
    """
    if shutdown.signum is not None and signum != signal.SIGINT:
        shutdown.bound(time.monotonic() + _signal_budget())
        return
    _record_signal_code(signum)
    if signum == signal.SIGINT:
        shutdown.bound(_handoff_cap())
        shutdown.hand_off()
        if shutdown.signum is not None:
            shutdown.exiting = 128 + signum
    else:
        budget = _signal_budget()
        shutdown.bound(time.monotonic() + budget)
        _log.warning("signal %d — ending the shutdown within %.0fs", signum, budget)
    shutdown.signum = signum
    if shutdown.exiting is not None or shutdown.ended is not None:
        shutdown.end_run()


def _signal_budget() -> float:
    """How long a shutdown signal leaves the flush: $KYMO_SIGNAL_FLUSH_TIMEOUT, else _SIGNAL_FLUSH_TIMEOUT."""
    try:
        reject_legacy_client_env()
    except ValueError as error:
        _log.error("%s; using the fixed emergency signal flush budget", error)
        return _SIGNAL_FLUSH_TIMEOUT
    try:
        budget = _env_number(
            "KYMO_SIGNAL_FLUSH_TIMEOUT",
            "MKDB2_SIGNAL_FLUSH_TIMEOUT",
            _SIGNAL_FLUSH_TIMEOUT,
        )
    except ValueError:
        return _SIGNAL_FLUSH_TIMEOUT
    return _normalize_timeout_budget(budget, "signal flush")


def _interrupt_budget() -> float:
    """How long a run a Ctrl-C ends may take to: _INTERRUPT_FLUSH_TIMEOUT, or the signal budget if shorter."""
    return min(_INTERRUPT_FLUSH_TIMEOUT, _signal_budget())


def _signal_end(at: Optional[float], budget: float) -> float:
    """budget after a signal kymo's handler saw at `at` (time.monotonic()), or after now if it saw none within that time."""
    now = time.monotonic()
    if at is None or at < now - budget:
        at = now
    return at + budget


def _handoff_cap() -> float:
    """When the run a Ctrl-C hands off must have ended (time.monotonic()): _interrupt_budget() after the last Ctrl-C (_ctrl_c_at), or after now if none came within that time (a KeyboardInterrupt the program raised itself, say)."""
    return _signal_end(_ctrl_c_at(), _interrupt_budget())


def _call_chained(handler, signum, frame):
    """Call the handler of a signal: one kymo chains to, or kymo's own (_shut_down_on_signal). _held_sections stops at this frame: the held round that called it only keeps signals from recursing, so a later signal interrupts the handler as it would without kymo. A Ctrl-C interrupts a checkpoint-on-SIGTERM handler, say, and any signal ends kymo's own flush at once (_signal_during_shutdown)."""
    handler(signum, frame)


def _ensure_metrics_uploaded():
    """The atexit flush. An exit that follows a Ctrl-C the program did not carry on from (_follows_ctrl_c), or with a Ctrl-C's status, 130, hands the run off instead (_Shutdown.hand_off), as kymo's exception hook and sys.exit already have for an uncaught KeyboardInterrupt or a sys.exit while handling one (_excepthook, _exit_wrapper).

    After a SIGTERM or SIGQUIT a handler installed before init() took, the flush ends within the signal budget, as kymo's own flush would, from that signal while the budget from it lasts: the scheduler's grace period runs from it, and a flush past it would be killed partway. After that, from the exit: a requeue or checkpoint handler can exit long after its SIGTERM, and a full flush would then outlast any grace period.

    An exception that ends the flush before its claim, the run still active, ends the run before it propagates, rather than leave the worker, which carries on through a Ctrl-C, to multiprocessing's exit hook, which waits for it (forever where the program owns SIGTERM, which the worker then ignores): one that follows a Ctrl-C hands it off, as above; any other (a SIGTERM handler's exit, say) flushes within the signal budget from that signal, or from now.
    """
    if not _is_initialized:
        return
    try:
        if _exit_code == 128 + signal.SIGINT or _follows_ctrl_c(None):
            _drain_and_shutdown(signum=signal.SIGINT, at_exit=True)
            return
        try:
            flush_timeout = _default_flush_timeout()
        except ValueError as error:
            _log.error("%s; using the fixed emergency exit flush budget", error)
            flush_timeout = _DEFAULT_FLUSH_TIMEOUT
        if _terminated_at is not None:
            end = _signal_end(_terminated_at, _signal_budget())
            flush_timeout = min(flush_timeout, _remaining_seconds(end))
        _drain_and_shutdown(flush_timeout=flush_timeout, at_exit=True)
    except BaseException as error:
        if _is_initialized:
            if _exit_code == 128 + signal.SIGINT or _follows_ctrl_c(error):
                _drain_and_shutdown(signum=signal.SIGINT, at_exit=True)
            else:
                end = _signal_end(_terminated_at, _signal_budget())
                _drain_and_shutdown(flush_timeout=_remaining_seconds(end), at_exit=True)
        raise


# Maximum share of the ONE finish() budget reserved for the worker to spill its
# remainder (pickling a few million tuples + encoding pending images).
_SPILL_GRACE = 30.0
# Keep a distinct tail for SIGKILL and owner-fenced parent salvage when the worker crashes. Twenty percent is material under Slurm's 10-second path; the cap leaves most of a normal 60-second finish for delivery/worker spill.
_CLEANUP_RESERVE_FRACTION = 0.2
_CLEANUP_RESERVE_MIN = 0.5
_CLEANUP_RESERVE_MAX = 10.0
_PROCESS_STOP_GRACE = 1.0
# Spool reporting/retirement runs after the worker window, on a mount that can stall. The kill/salvage steps end this much before the hard deadline (or half the forced-cleanup tail, whichever is smaller) so the last phase has room inside the same budget rather than overrunning it. That room comes from the forced-cleanup tail, never from the worker's delivery+spill window, which on a 2 s budget decides how many points reach disk at all (_Shutdown.plan_worker).
_SPOOL_CLEANUP_RESERVE = 0.5
# Quiet-time fallback for an orphaned worker whose parent can no longer publish the original shutdown sentinel.
_QUEUE_DRAIN_SILENCE = 2.0
_DEFAULT_FLUSH_TIMEOUT = 60.0
# Shorter budget on signals: slurm follows SIGTERM with SIGKILL after KillWait
# (often 30s) — leave room for the spill itself.
_SIGNAL_FLUSH_TIMEOUT = 10.0
# A Ctrl-C that ends the program ends its run at once, and the upload worker, handed the run's remaining delivery, delivers and spills within this, or the signal budget if shorter, in the background (_Shutdown.hand_off).
_INTERRUPT_FLUSH_TIMEOUT = 10.0
# How long the parent may take to send a handed-off run's TerminateRun, before it gives the console back (_Shutdown.end_run).
_HANDOFF_TERMINATE_TIMEOUT = 1.0
# How long a handoff's publications may wait for the pending calls' lock and the accounting lock, in all (_complete_handoff).
_HANDOFF_DRAIN_TIMEOUT = 0.1
# How long, once the run has ended, the parent lets its queue's feeder thread pass the groups it still holds to the upload worker before it gives the console back (_complete_handoff).
_HANDOFF_FEEDER_GRACE = 0.2
# The steps of a shutdown's waits (_await): how soon it sees a signal's bound or handoff.
_SHUTDOWN_STEP = 0.05


def _default_flush_timeout() -> float:
    reject_legacy_client_env()
    try:
        return _env_number(
            "KYMO_FLUSH_TIMEOUT", "MKDB2_FLUSH_TIMEOUT", _DEFAULT_FLUSH_TIMEOUT
        )
    except ValueError:
        return _DEFAULT_FLUSH_TIMEOUT


def _remaining_seconds(deadline: float) -> float:
    return max(0.0, deadline - time.monotonic())


def _worker_window(now: float, end: float) -> tuple[float, float, float, float]:
    """The upload worker's part of a shutdown that must end by end (time.monotonic()): when it stops delivering, when it stops spilling, when it is killed, and the cleanup tail after that. Its spill ends a margin before the kill, half the shorter of its spill share and the tail, so it reports what it lost and exits by itself. A group signal reaches both the parent (_Shutdown.plan_worker) and the worker (kymo._worker._own_signals), which plan from the same budget, and the parent may see it first."""
    remaining = max(0.0, end - now)
    cleanup = min(
        remaining,
        _CLEANUP_RESERVE_MAX,
        max(_CLEANUP_RESERVE_MIN, remaining * _CLEANUP_RESERVE_FRACTION),
    )
    worker = max(0.0, remaining - cleanup)
    spill = min(_SPILL_GRACE, worker / 2)
    kill = end - cleanup
    return now + (worker - spill), kill - min(spill, cleanup) / 2, kill, cleanup


def _drain_queue_to_spool(
    spool: SpoolWriter, *, source, held: list, hard_deadline: float
) -> int:
    """Parent-side salvage after the worker died, of source through an owner FIFO fence for _SPILL_GRACE at most and until hard_deadline (time.monotonic()), finish()'s shared one, unless source is None, then of held, the held-back groups, which were never put."""
    if _init_pid is None or os.getpid() != _init_pid:
        raise RuntimeError("only the initializing process may fence the upload queue")

    # Queue ordering is producer-local, so only the initializing process may publish this marker; a worker marker could overtake objects still being serialized by the parent's feeder.
    fence = ("__kymo_owner_drain__", uuid.uuid4().hex)
    # Points not spooled: undecodable, unencodable, or left unwritten once a write failed.
    dropped = 0
    write_failed = False

    def consume(item) -> None:
        nonlocal dropped, write_failed
        if item is None:
            return
        if write_failed:
            dropped += _queue_item_size(item)
            return
        try:
            point_tuples = _decode_queue_item(item)
        except Exception as error:
            dropped += _queue_item_size(item)
            _log.warning("failed to decode a queued item during salvage: %s", error)
            return
        for index, point_tuple in enumerate(point_tuples):
            written = spool.count
            try:
                if not _spill_tuple(spool, point_tuple):
                    dropped += 1
            except Exception as e:
                # A failed write is normally disk-wide, and the writer has cut its record off, with any earlier records that had not reached the file: stop writing, but keep draining to the fence.
                write_failed = True
                # Lost: the failed point and the rest of its group, plus any counted records the cut removed.
                dropped += len(point_tuples) - index + written - spool.count
                _log.warning(
                    "failed to spool during salvage; dropping the rest of the queue: %s",
                    e,
                )
                return

    deadline = min(time.monotonic() + _SPILL_GRACE, hard_deadline)
    # A dead worker can leave the queue full (macOS bounds it at 32,767 groups): each group salvaged first makes room for the fence.
    fenced = reached = source is None
    while not reached:
        if not fenced:
            try:
                source.put(fence, block=False)
                fenced = True
            except queue.Full:
                pass
            except (OSError, ValueError) as error:
                _log.warning(
                    "failed to publish the upload queue salvage fence: %s", error
                )
                break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            if not fenced:
                _log.warning("upload queue salvage found no room for its owner fence")
            break
        try:
            item = source.get(timeout=min(0.25, remaining))
        except queue.Empty:
            continue
        except (OSError, ValueError) as error:
            _log.warning("upload queue drain failed: %s", error)
            break
        reached = item == fence
        if not reached:
            consume(item)
            # The next wait may last seconds; it must not keep a large serialized gallery alive.
            item = None
    if fenced and not reached:
        _log.warning("upload queue salvage ended before its owner fence")
    for group in held:
        consume(group)
    if dropped:
        _log.error("parent salvage dropped %d point(s)", dropped)
    # The writer's count leaves out the records a failed write cut off.
    return spool.count


def _report_spool(paths: list[str]) -> bool:
    # The session-scoped listdir is the delivery evidence. Do not restat: disappearance cannot distinguish delivery (the worker deletes what it delivers, sync renames it .sent) from quarantine, retirement, or manual removal.
    if not paths:
        return False
    _log.error(
        "undelivered metrics were spooled to disk. Replay them later from any "
        "CPU machine with:\n    %s",
        replay_command(paths),
    )
    return True


def _retire_rejected_worker_spools(paths: list[str]) -> None:
    """Remove a rejected worker's closed spools from automatic replay.

    A missing source has already left the replayable namespace. Other rename
    failures remain delivery evidence, but must never fall through to the
    ordinary replay advice printed for admissible runs.
    """
    for path in paths:
        try:
            retire_deleted_spool(path)
        except FileNotFoundError:
            continue
        except OSError as error:
            _log.error(
                "failed to retire lifecycle-rejected spool %s: %s. Its points "
                "are inadmissible — do not replay it; delete it after any writer "
                "exits",
                path,
                error,
            )
    paths.clear()


def _retire_or_report_run_spools(
    *,
    project_id: str,
    run_id: str,
    spool_dir: Optional[str],
    session: str,
    terminal_rejected: bool,
) -> bool:
    """Retire or report the spool files this run can still replay.

    Every input is passed in, never read from a module global: this runs in a
    daemon thread that may outlive `finish()` and overlap a later `init()` — the
    same reason parent salvage captures its queue up front.

    The worker rotates through as many segments as recovery needs and names them
    itself, so the path init() pre-creates is not a complete inventory. Every
    path this init creates carries the same filename session prefix, making this
    one listing complete without opening headers belonging to sibling ranks,
    restarts, restored generations, or another deployment.
    """
    owned = run_spool_files(
        project_id,
        run_id,
        spool_dir,
        session=session,
    )
    if terminal_rejected:
        stranded = []
        for path in owned:
            if writer_active(path):
                stranded.append(path)
                _log.error(
                    "lifecycle-rejected spool is locked by its writer or a replay; "
                    "not retiring %s",
                    path,
                )
                continue
            try:
                retired = retire_deleted_spool(path)
                _log.error(
                    "retired lifecycle-rejected spool as %s; it will not be replayed",
                    retired,
                )
            except FileNotFoundError:
                # A concurrent sync/cleanup already renamed it after listdir.
                continue
            except OSError as error:
                stranded.append(path)
                _log.error(
                    "failed to retire lifecycle-rejected spool %s: %s", path, error
                )
        if stranded:
            # Keep the evidence and the unproven-delivery answer, but never route
            # a rejected run's points through the replay instructions below.
            _log.error(
                "%d lifecycle-rejected spool file(s) kept the replayable *.mkspool "
                "name because retirement failed. Their run is deleted and their "
                "points are inadmissible — do not replay them; delete them after "
                "any writer exits: %s",
                len(stranded),
                " ".join(stranded),
            )
        return bool(stranded)
    return _report_spool(owned)


def _resolve_run_spools(*, terminal_rejected: bool, deadline) -> bool:
    """Run spool cleanup off the caller's thread, like the parent's own salvage, until deadline (_await).

    Listing, the terminal writer-lock check, and rename all touch a shared
    cluster mount that can stall indefinitely, and `finish()` promises a bounded
    return. A cleanup that outlives the deadline may continue in the background;
    its run cannot claim complete delivery, because nothing proved the spool
    empty.
    """
    # Bind this run's identity BEFORE starting the thread: a timed-out cleanup
    # runs on into a later init() and must keep acting for the run it began for.
    context = {
        "project_id": _project_id,
        "run_id": _run_id,
        "spool_dir": _spool_dir,
        "session": _session_id,
        "terminal_rejected": terminal_rejected,
    }
    if not context["session"]:
        # Validate before launching the daemon. With an exhausted deadline the
        # caller may otherwise time out before the thread rejects the session and
        # print a run-wide deletion pattern for an empty session.
        _log.error(
            "cannot resolve this run's spool files: session identity is missing; "
            "delivery remains unproven"
        )
        return True
    outcome: dict = {}
    owned_prefix = spool_name_prefix(
        context["project_id"], context["run_id"], context["session"]
    )

    def resolve() -> None:
        try:
            outcome["had_spool"] = _retire_or_report_run_spools(**context)
        except BaseException as error:
            outcome["error"] = error

    thread = threading.Thread(target=resolve, name="kymo-spool-cleanup", daemon=True)
    thread.start()
    # Strictly inside finish()'s budget: the shutdown split ends the worker and
    # salvage phases _SPOOL_CLEANUP_RESERVE early to leave room for this.
    if not _await(_joiner(thread), deadline):
        if terminal_rejected:
            # Never point an operator at a rejected run's spool: this daemon can
            # be killed at interpreter exit before it renames anything, and those
            # points are inadmissible — replaying them into a restored run is the
            # exact outcome retirement exists to prevent. This session owns the
            # complete filename prefix, so no header reads are needed to give
            # specific advice without reaching into a sibling/restored session.
            _log.error(
                "spool cleanup exceeded the finish deadline. This run is deleted; "
                "do not replay any remaining %s*%s file in the configured spool "
                "directory — delete it after its writer exits",
                owned_prefix,
                SPOOL_SUFFIX,
            )
        else:
            _log.error(
                "spool cleanup exceeded the finish deadline; replay any leftovers "
                "with: python -m kymo.sync"
            )
        return True
    if "error" in outcome:
        _log.error("failed to resolve this run's spool files: %s", outcome["error"])
        return True
    return outcome["had_spool"]


def _stop_upload_process(
    process: multiprocessing.Process, *, graceful_deadline, stop_deadline
) -> tuple[bool, bool]:
    """Stop one worker within the shared shutdown budget: wait for it until graceful_deadline, then kill it and wait until stop_deadline (_await).

    Returns ``(dead, clean_exit)``.  Salvage is safe only when ``dead`` is true;
    the worker finishes on SIGTERM within the signal budget rather than at once
    (_own_signals), so only SIGKILL stops it in time.
    """

    def stopped(timeout: float) -> bool:
        if process.is_alive():
            process.join(timeout)
        elif _worker_alive(process):
            # Its Popen, which no longer sees it, returns at once (_worker_alive).
            time.sleep(timeout)
        return not _worker_alive(process)

    if not _await(stopped, graceful_deadline):
        _log.warning("worker did not exit within its deadline, killing")
        _worker_alive(process, signal.SIGKILL)
        _await(stopped, stop_deadline)

    dead = not _worker_alive(process)
    return dead, dead and _worker_exitcode(process) == 0


def _salvage_queue_before(shutdown: "_Shutdown", held: list) -> bool:
    """Run potentially blocking encode/write/fsync work outside finish's thread until shutdown.pre_cleanup (_await): spool the queue's groups, then held, the groups held back, charged or not. The salvage is the shutdown's (shutdown.salvage): a Ctrl-C's handoff leaves it the queue (_complete_handoff), and its writer then cuts off whole a record still being written, since the process may exit as soon as the console is back."""
    shutdown.salvage = _start_salvage(
        held,
        shutdown.pre_cleanup,
        until=lambda: -math.inf if _handed_off else math.inf,
    )
    thread, outcome, spool = shutdown.salvage
    if not _await(_joiner(thread), lambda: shutdown.pre_cleanup):
        _log.error(
            "parent queue salvage exceeded the finish deadline; it may continue "
            "in the background at %s",
            spool.path,
        )
        return False
    if outcome["error"] is not None:
        _log.error("parent queue salvage failed: %s", outcome["error"])
    elif outcome["salvaged"]:
        _log.warning(
            "salvaged %d undelivered points from the queue", outcome["salvaged"]
        )
    return True


def _start_salvage(held: list, deadline: float, *, until):
    """Start the parent's salvage in a daemon thread, which a stalled filesystem holds instead of the caller: spool the queue's groups through an owner fence until deadline (time.monotonic()), then held (_drain_queue_to_spool), and close the spool, whose writer cuts off whole a record still written at until() (SpoolWriter.until). Return the thread, its outcome ({"salvaged": points spooled, "error": its exception}) and the spool.

    A dead worker that held the queue's reader lock, part of a message read (kymo._worker._QueueReads) or killed inside a read, leaves the queue unreadable: then it reports the queue's points, at most the backlog, lost at once, and spools held alone.
    """
    # Capture the old queue before starting the daemon. A deadline-expired salvage may overlap a later init(), which replaces it.
    source = _metric_queue
    if source._rlock.acquire(False):
        source._rlock.release()
    else:
        _log.error(
            "the upload worker left the upload queue unreadable: at most %d "
            "queued point(s) are LOST",
            max(0, _queue_status.get_obj().value),
        )
        source = None
    spool = SpoolWriter(
        make_spool_path(
            _project_id,
            _run_id,
            "salvage",
            spool_dir=_spool_dir,
            session=_session_id,
        ),
        header=_spool_header(
            _server_address,
            _project_id,
            _run_id,
            _run_name,
            _cdn_address,
            _session_id,
            _local_installation_uuid,
        ),
        until=until,
    )
    outcome = {"salvaged": 0, "error": None}

    def salvage() -> None:
        try:
            outcome["salvaged"] = _drain_queue_to_spool(
                spool,
                source=source,
                held=held,
                hard_deadline=deadline,
            )
        except BaseException as error:
            outcome["error"] = error
        finally:
            try:
                spool.close()
            except BaseException as error:
                if outcome["error"] is None:
                    outcome["error"] = error

    thread = threading.Thread(
        target=salvage,
        name="kymo-parent-salvage",
        daemon=True,
    )
    thread.start()
    return thread, outcome, spool


def _put_shutdown_marker(timeout: float) -> bool:
    """A step of putting the worker's shutdown marker (_await): True once it is put, False if the queue had no room within timeout; _WorkerExited once the upload worker, which alone makes room, has exited."""
    try:
        _metric_queue.put(None, timeout=timeout)
        return True
    except queue.Full as error:
        if not _from_the_queue(error):
            raise
    _check_worker()
    return False


def _claim_shutdown(shutdown: "_Shutdown") -> Optional["_Shutdown"]:
    """Make shutdown the one in progress (_shutdown_claim) unless another is: return that one, else None. One dict.setdefault claims it, so two threads never both do. A held section (_HELD_SECTIONS): a signal arriving as it is claimed is handled once the shutdown has set the run's code (_drain_and_shutdown)."""
    running = _shutdown_claim.setdefault("shutdown", shutdown)
    return None if running is shutdown else running


def _release_shutdown(shutdown: "_Shutdown", arrived=()) -> None:
    """End the claim of shutdown, if it made one (_claim_shutdown), and wake those waiting for it, the signals that arrived during it (arrived) waiting first, ahead of any deferred during the release, which came later. A held section (_HELD_SECTIONS), so no handler kymo chains to runs partway: one that raised there would leave it claimed with no shutdown running, or the arrived signals lost."""
    if _shutdown_claim.get("shutdown") is shutdown:
        _deferred_signals[:0] = [[signum, None] for signum in arrived]
        _shutdown_claim.clear()
        shutdown.done.set()


# Held sections: kymo's handler defers its shutdown signals while one runs on the main thread (_signal_handler), where it would act before a shutdown set the run's code, its flush could deadlock or time out on the section's locks or lose captured text or a pending call, a chained handler's gallery log() could reserve the same rich versions, and a Ctrl-C as the upload worker starts would raise before the run is active (_start_run).
# Handling the deferred signals is one, so a later signal waits rather than nests (but see _call_chained), and so is the stack walk, whose caller handles a signal interrupting it. Frames on the stack, not a count: a raising handler can skip a count's restore, even one first in a `finally` on Python 3.10, which runs pending handlers as an exception handler starts. A shutdown's waits hold nothing: the handler never flushes then (_signal_during_shutdown).
_HELD_SECTIONS = frozenset(
    # Their code objects' ids, which the functions keep alive (_held_sections).
    id(function.__code__)
    for function in (
        _reserve_versions,
        _start_run,
        _publish_held,
        _read_delivery_status,
        _capture.TeeWriter._keep,
        _handle_deferred_signals,
        _held_sections,
        _claim_shutdown,
        _release_shutdown,
    )
)
# A print's buffering (_capture.TeeWriter._keep) handles what it deferred as it ends.
_capture.deferred_signals = _deferred_signals
_capture.run_deferred_signals = _run_deferred_signals


class _HandedOff(Exception):
    """The shutdown in progress was handed off (_Shutdown.hand_off), so it waits no longer (_await)."""


class _Shutdown:
    """The shutdown in progress (_shutdown_claim), which kymo's signal handler reads and changes (_signal_during_shutdown), perhaps while another thread runs it: a signal brings its end forward (bound), and a Ctrl-C hands the run off (hand_off)."""

    def __init__(self, flush_timeout: float, signum: Optional[int] = None):
        self.thread = threading.get_ident()
        # Its flush budget, and when it must have ended; a Ctrl-C that starts it hands it off at once, with the handoff's cap.
        self.budget = flush_timeout
        self.hard = (
            _handoff_cap()
            if signum == signal.SIGINT
            else time.monotonic() + flush_timeout
        )
        # The hard the deadlines below were last planned from (check).
        self.planned_hard = self.hard
        # The signal that started, bounded or handed off the shutdown: a later Ctrl-C exits at once (_signal_during_shutdown).
        self.signum = signum
        # The signals that interrupted the shutdown, in arrival order, which it handles as it returns (_drain_and_shutdown).
        self.arrived: list[int] = []
        # The exit code the run's TerminateRun went out with, None until a send claims the run's end (end_run); from then on a signal that changes the code ends the run again.
        self.ended: Optional[int] = None
        # Held while a TerminateRun is in flight, which no other send nests in (end_run).
        self.sending = threading.Lock()
        # The run it ends, which every TerminateRun of its addresses (end_run): the shutdown forgets the module's copy before its claim is released, and a signal then still ends the run.
        self.run = (
            _server_address,
            _local_installation_uuid,
            _project_id,
            _run_id,
            _upload_terminal,
        )
        # The status a further signal exits with, once the TerminateRun in flight returns (end_run).
        self.exiting: Optional[int] = None
        # Whether end_run has warned of the handoff.
        self.announced = False
        # When the pending log() calls give up (plan_pending); when the worker stops delivering and spilling, its window ends and it is killed, the wait for it ends, and the forced cleanup's salvage ends (plan_worker).
        self.pending_by = self.deliver_by = self.spill_by = self.graceful = math.inf
        self.stop_by = self.pre_cleanup = math.inf
        # The parent's salvage of a dead worker's queue, its thread, outcome and spool (_salvage_queue_before), once started.
        self.salvage = None
        # Whether the shutdown has stopped waiting for the worker, and so dealt with its queue (_stop_upload_process).
        self.stopped = False
        self.result = False
        self.done = threading.Event()

    def check(self) -> None:
        """A step of the shutdown: _HandedOff once it is handed off; the deadlines it has planned are planned again from now once a signal has brought hard forward, and only then: planning again from the same hard would only shrink the kill's grace as time passes (plan_worker)."""
        if _handed_off:
            raise _HandedOff
        hard = self.hard
        if hard != self.planned_hard:
            # Read before planning: a bound that lands during it moves hard again, which the next step sees.
            self.planned_hard = hard
            now = time.monotonic()
            if self.pending_by != math.inf:
                self.plan_pending(now)
            if self.graceful != math.inf:
                self.plan_worker(now)

    def plan_pending(self, now: float) -> None:
        """Give the log() calls still copying GPU values half the time left, never more than before."""
        self.pending_by = min(self.pending_by, now + max(0.0, self.hard - now) / 2)

    def plan_worker(self, now: float) -> None:
        """Divide the time left roughly 50/50 between the worker's delivery and its own spill, capped at _SPILL_GRACE, before a separate tail for SIGKILL and owner-fenced salvage after a crash (_worker_window, as a signal's handling in the worker does), never later than before, and tell the worker when to stop delivering and spilling."""
        deliver_by, spill_by, kill, cleanup = _worker_window(now, self.hard)
        self.deliver_by = min(self.deliver_by, deliver_by)
        self.graceful = min(self.graceful, kill)
        self.spill_by = min(self.spill_by, spill_by)
        pre_cleanup = self.hard - min(_SPOOL_CLEANUP_RESERVE, cleanup / 2)
        self.pre_cleanup = min(self.pre_cleanup, pre_cleanup)
        self.stop_by = min(
            self.stop_by,
            pre_cleanup,
            self.graceful + min(_PROCESS_STOP_GRACE, cleanup / 2),
        )
        _shutdown_deadline.value = self.deliver_by
        _spill_deadline.value = self.spill_by

    def bound(self, end: float) -> None:
        """A signal: end by end (time.monotonic()), if that is sooner. It runs in a handler, perhaps inside the shutdown's own planning, so it only brings hard forward, a single store; the shutdown plans again at its next step (check)."""
        if end < self.hard:
            self.hard = end

    def hand_off(self) -> None:
        """Leave the run's remaining delivery to the upload worker, which delivers, spills and exits by itself by hard: it gets that end in _handoff, never one already past (at least _CLEANUP_RESERVE_MIN, to close its spool), and a SIGTERM that arms it at once, even where the program owns SIGTERM (kymo._worker._own_signals). The parent ends the run (end_run), which then says so, and the shutdown waits no longer. finish() and wait_for_upload() cannot prove delivery from then on. Once handed off, it pushes a sooner end to the worker the same way."""
        global _handed_off
        process, handoff = _upload_process, _handoff
        end = max(self.hard, time.monotonic() + _CLEANUP_RESERVE_MIN)
        if not handoff.value or end < handoff.value:
            handoff.value = end
            _worker_alive(process, signal.SIGTERM)
        _handed_off = True

    def end_run(self) -> None:
        """Send the run's TerminateRun with its exit code (_exit_code, else 0) unless that code has gone out, by hard. Once handed off, it waits _HANDOFF_TERMINATE_TIMEOUT at most and leaves a control process still running then to finish by itself, never killed, by hard but at least that long from now (_run_control_rpc). Then, with no send in flight, warn of a handoff, once: after the send, so a stderr whose reader has stalled (a full pipe) cannot hold it up. Then exit if a further signal asked to. The parent sends it, never a handed-off worker, which the cap or a crash can kill, or the program's container or process group take down; the server takes the worker's points after the run has ended.

        A send claims the run's end first, and none nests in another, since the two could reach the server in either order: a signal during one only changes the code or asks for the exit, which the sender acts on once its send returns, or, once the send is superseded, within a step (_terminate_run), the next control process waiting for that one to end. A handed-off run keeps its code, so its send, which may go on, is the last.
        """
        while self.ended != (_exit_code or 0) and self.sending.acquire(blocking=False):
            try:
                code = _exit_code or 0
                if code != self.ended:
                    self.ended = code
                    timeout, wait = _remaining_seconds(self.hard), None
                    if _handed_off:
                        timeout = max(_HANDOFF_TERMINATE_TIMEOUT, timeout)
                        wait = _HANDOFF_TERMINATE_TIMEOUT
                    _terminate_run(
                        code, timeout, run=self.run, wait=wait, end=lambda: self.hard
                    )
            finally:
                self.sending.release()
        if self.sending.locked():
            return
        if _handed_off and not self.announced:
            self.announced = True
            _log.warning(
                "ending the run; the upload worker delivers what it can within "
                "%.1fs in the background",
                _remaining_seconds(self.hard),
            )
            if os.getpid() == 1:
                _log.warning(
                    "this process is PID 1 of its namespace, as in a container: "
                    "its exit kills the upload worker at once, losing what it has "
                    "not delivered, and any TerminateRun control process still "
                    "running, whose end of the run then never goes out"
                )
        if self.exiting is not None:
            _exit_now(self.exiting)


def _await(wait, deadline) -> bool:
    """Call wait(timeout) in steps of _SHUTDOWN_STEP until it returns True (True) or deadline() has passed (False), read at every step, since a signal can bring a shutdown's deadlines forward (_Shutdown.bound); _HandedOff once the shutdown in progress on this thread is handed off."""
    shutdown = _shutdown_claim.get("shutdown")
    if shutdown is not None and shutdown.thread != threading.get_ident():
        shutdown = None
    while True:
        if shutdown is not None:
            shutdown.check()
        remaining = _remaining_seconds(deadline())
        if wait(min(remaining, _SHUTDOWN_STEP)):
            return True
        if remaining <= 0:
            return False


def _joiner(handle):
    """A wait for _await that joins handle, a thread: True once it has ended."""

    def joined(timeout: float) -> bool:
        if handle.is_alive():
            handle.join(timeout)
        return not handle.is_alive()

    return joined


def _disown_worker(process) -> None:
    """Take the upload worker out of multiprocessing's children, so interpreter exit neither waits for it nor terminates it (multiprocessing.util._exit_function). They are the private multiprocessing.process._children, a set on CPython 3.10 to 3.14; without one, this only logs.

    Nor may the exit wait for multiprocessing's resource tracker, which exits only once every holder of its pipe has, the worker among them under spawn and forkserver: on CPython 3.12 from 3.12.10, 3.13.3-3.13.13 and 3.14.0-3.14.4 its __del__ closes the pipe at interpreter teardown and waits for it. Clearing its private _pid makes that a no-op, as CPython's at-fork hook does in a fork child from 3.13.14 and 3.14.5 (gh-146313); the tracker still unlinks what is left registered once the worker exits, and a later init() still finds it running."""
    children = getattr(multiprocessing.process, "_children", None)
    if isinstance(children, set):
        children.discard(process)
    else:
        _log.warning(
            "multiprocessing.process._children is gone: interpreter exit may wait "
            "for the upload worker or terminate it"
        )
    tracker = getattr(
        sys.modules.get("multiprocessing.resource_tracker"), "_resource_tracker", None
    )
    if tracker is not None:
        tracker._pid = None


def _exit_now(code: int) -> None:
    """Exit with code at once, past the atexit hooks, once it has warned of the pending log() calls and held-back groups the exit drops and what the program printed to a file (a Slurm or redirect log) is flushed: never to a pipe, socket or terminal, whose reader may have stalled."""
    held = sum(map(_queue_item_size, [*_held_back, *_uncharged]))
    if _pending or held:
        _log.warning(
            "exiting at once: dropped %d log() call(s) still pending and %d "
            "point(s) that never reached the upload queue",
            len(_pending),
            held,
        )
    for stream in (sys.stdout, sys.stderr):
        try:
            if stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                stream.flush()
        except Exception:
            pass
    os._exit(code)


def _terminate_run(
    code: int,
    timeout: float,
    *,
    run: tuple,
    wait: Optional[float] = None,
    end=None,
) -> None:
    """Send run's TerminateRun (_Shutdown.run: server address, local installation, project, run, and the worker's run-deleted flag) with code within timeout, best effort: a failure is logged, never raised into the program's exit. After a control process an earlier send left running (_left_control), this one sends once that one has ended, so the server takes its code last, its RPC timeout then not capped at 5 s.

    The waits for a local run's endpoint ensure (kymo._local_runtime) and for the control process (_run_control_rpc) go in steps of _SHUTDOWN_STEP and stop once leave() says so, killing the ensure, or leaving the control process running, never killed: wait seconds after the send began (a handoff's, which is the last); or, without wait, once end() has passed (the shutdown's hard, which a signal brings forward; else timeout from now), or once the send is superseded: its code is no longer the run's (_exit_code), or a Ctrl-C has handed the run off or waits in the held round this send may run in, whose handoff sends 130 behind it. A superseded send logs nothing, so a stalled stderr cannot hold up the one that follows. Each step of a send in flight scans the few deferred signals.

    Not once its worker observed the run deleted: if the run is restored before this old process exits, a stale TerminateRun must not mark the restored run finished.

    A control process it leaves running becomes _left_control (_run_control_rpc), even when an exception ends the send; one whose send went out clears it.
    """
    global _left_control
    server_address, installation, project_id, run_id, terminal = run
    if (
        terminal.get_obj().value
        or not (server_address or installation)
        or not (project_id and run_id)
        or timeout <= 0
    ):
        return
    started = time.monotonic()

    def superseded() -> bool:
        return wait is None and (
            (_exit_code or 0) != code
            or _handed_off
            or any(entry[0] == signal.SIGINT for entry in _deferred_signals)
        )

    def leave() -> bool:
        if wait is not None:
            return time.monotonic() >= started + wait
        return superseded() or time.monotonic() >= (
            started + timeout if end is None else end()
        )

    try:
        endpoint = None
        if installation:
            from kymo._local_runtime import ensure_local_endpoint

            # In a session of its own, which the group's Ctrl-C or SIGTERM never reaches, with the default signal mask, which a local stack it starts takes (ensure_local_endpoint).
            endpoint = ensure_local_endpoint(
                expected_installation_uuid=installation,
                timeout=timeout if wait is None else min(timeout, wait),
                own_session=True,
                leave=leave,
            )
        rpc_timeout = max(0.001, timeout - (time.monotonic() - started))
        after = _left_control
        if after is not None and after.poll() is not None:
            after = None
        if after is None:
            rpc_timeout = min(5.0, rpc_timeout)
        sent = _run_control_rpc(
            "TerminateRun",
            kymo_pb2.TerminateRunRequest(
                project_id=project_id, run_id=run_id, exit_code=code
            ),
            kymo_pb2.TerminateRunResponse,
            server_address=server_address,
            timeout=rpc_timeout,
            local_endpoint=endpoint,
            leave=leave,
            after=None if after is None else after.pid,
        )
        if isinstance(sent, subprocess.Popen):
            if not superseded():
                _log.warning(
                    "TerminateRun is still on its way; its control process goes "
                    "on for up to %.1fs",
                    rpc_timeout,
                )
            return
    except Exception as e:
        if not superseded():
            _log.warning("failed to send TerminateRun: %s", e)
        return
    _left_control = None


def _drain_and_shutdown(
    flush_timeout: Optional[float] = None,
    *,
    signum: Optional[int] = None,
    at_exit: bool = False,
) -> bool:
    """Flush and shut the run down within flush_timeout, as the one shutdown in progress (_Shutdown), which a signal never nests in nor aborts (_signal_during_shutdown); True only if delivery was proven complete.

    signum is the signal that starts it: a SIGINT hands the run off at once, any other bounds it from the start; the run ends with its code. A signal that interrupts it is handled once it returns, in arrival order (_handle_deferred_signals), except in the atexit flush (at_exit), where a raise cannot set the exit status: there a Ctrl-C, which handed the run off, and a SIGTERM or SIGQUIT, which bounded the flush, leave the program's own exit status, and its later atexit hooks run; nor is a handler installed before init() called, since a 143 could make a scheduler retry a job that had finished its work, and a requeue or checkpoint handler could redo it. Only a further Ctrl-C exits at once (_signal_during_shutdown).

    The shutdown is claimed at once (_claim_shutdown): a caller that finds one running, another thread's or this thread's re-entered by a handler kymo does not own, waits for it, never running a second one.
    """
    global \
        _is_initialized, \
        _run_metadata, \
        _mode, \
        _local_installation_uuid, \
        _url_base, \
        _rich_writer_epoch, \
        _rich_mutation_seq, \
        _finishing

    if not _is_initialized:
        return True
    # Raw os.fork() children (NOT multiprocessing children — those skip atexit) inherit this module's state and hooks; see _init_pid. Only the initializing process may shut the shared worker down.
    if _init_pid is not None and os.getpid() != _init_pid:
        return True
    if signum == signal.SIGINT:
        # Handed off at once, with the handoff's time (_handoff_cap).
        flush_timeout = 0.0
    elif flush_timeout is None:
        flush_timeout = _default_flush_timeout()
    flush_timeout = _normalize_timeout_budget(flush_timeout, "flush")
    shutdown = _Shutdown(flush_timeout, signum)
    try:
        running = _claim_shutdown(shutdown)
        if running is not None:
            if _deferred_signals:
                _run_deferred_signals()
            if running.thread != threading.get_ident():
                if signum == signal.SIGINT and not _handed_off:
                    # A Ctrl-C kymo's handler never saw (a SIGINT handler the program installed after init() took it) hands another thread's shutdown off as that handler would (_signal_during_shutdown), but for its escalation, so the wait below is for the handoff alone.
                    _record_signal_code(signum)
                    running.bound(_handoff_cap())
                    running.hand_off()
                    if running.ended is not None:
                        running.end_run()
                _await(running.done.wait, lambda: running.hard + _PROCESS_STOP_GRACE)
            return running.result
        # Every shutdown is claimed here: a log() waiting for the encoder (_await_encoder) goes on, and releases the pending calls' lock for this one.
        _finishing = True
        if not _is_initialized:
            # Another shutdown ended the run as this one was claimed.
            shutdown.result = not _handed_off
            return shutdown.result
        if signum is not None:
            _record_signal_code(signum)
        if at_exit and signum is None:
            # Claimed first: a signal while it logs, which a stalled stderr can hold up, bounds or hands off the flush.
            _log.info("shutting down — flushing remaining metrics…")
        # Those that arrived during the claim, now handled as during this shutdown (_handle_signal).
        if _deferred_signals:
            _run_deferred_signals()
        try:
            if signum == signal.SIGINT:
                shutdown.hand_off()
            result = _shut_down(shutdown)
        except _HandedOff:
            result = False
        if _handed_off:
            _complete_handoff(shutdown)
            result = False
        _is_initialized = False
        _run_metadata = None
        _mode = "hosted"
        _local_installation_uuid = ""
        _url_base = ""
        _rich_writer_epoch = None
        _rich_mutation_seq = None
        # Groups still held back now were never put, and the next init() must not put them: after a handoff, those the queue had no room for or the accounting lock did not let it charge; else those no salvage took (a deleted run, a worker alive after SIGKILL, no time left) and those a log() on another thread held back behind the drain.
        dropped = sum(
            map(_queue_item_size, _take_held_back() + _take_held_back(_uncharged))
        )
        if dropped:
            _log.warning(
                "dropped %d point(s) that never reached the upload queue", dropped
            )
            result = False
        shutdown.result = result
    finally:
        _release_shutdown(shutdown, () if at_exit else shutdown.arrived)
    if at_exit:
        # The flush is over and the process exits with its own status: neither the signals that interrupted it nor those deferred as it was released call a handler installed before init().
        _deferred_signals.clear()
    elif _deferred_signals:
        _run_deferred_signals()
    return shutdown.result


def _complete_handoff(shutdown: _Shutdown) -> None:
    """The parent's part of a handoff, waiting for no worker: it ends the run (_Shutdown.end_run), publishes what is ready, and leaves the worker to deliver or spill the rest and exit by itself."""
    global _system_poller
    process = _upload_process
    # First, before anything that can write to stderr (the poller's stop, the publications), whose stalled reader (a full pipe) would hold it up.
    shutdown.end_run()
    # Interpreter exit must neither wait for the worker nor terminate it, as multiprocessing's exit hook would; it exits by the handoff's cap by itself (kymo._worker._own_signals), and a program that carries on reaps it then. Nor may it wait for the queue's feeder to write to a queue nothing may read.
    _disown_worker(process)
    _metric_queue.cancel_join_thread()
    try:
        if _system_poller is not None:
            _system_poller.stop(timeout=_SHUTDOWN_STEP)
            _system_poller = None
        if shutdown.stopped:
            # The shutdown had stopped the worker and dealt with its queue: only the run was left to end. The salvage of a dead worker's queue may still read it (_salvage_queue_before), so nothing is published past its fence nor the queue closed under it; its writer now cuts off whole a record still being written, so an exit that follows tears none.
            if shutdown.salvage is not None and shutdown.salvage[0].is_alive():
                _log.warning(
                    "the Ctrl-C cut short the parent's salvage of the upload queue; "
                    "the points it had not spooled are LOST"
                )
                if shutdown.salvage[2].count:
                    _report_spool([shutdown.salvage[2].path])
            return
        # Its publications wait for nothing but the pending calls' lock and the accounting lock, within one small budget, by the shutdown's end.
        by = min(time.monotonic() + _HANDOFF_DRAIN_TIMEOUT, shutdown.hard)
        # The pending log() calls whose GPU values are ready, oldest first, waiting for neither the GPU nor room on the queue (_wait_to_put): a gallery call on another thread that holds _pending_lock while it waits for the encoder (_queue_pending_log) stops waiting at this shutdown's claim (_finishing), publishes and lets it go.
        locked = _pending_lock.acquire(timeout=_remaining_seconds(by))
        if locked:
            try:
                _publish_pending(0.0, by)
            finally:
                _pending_lock.release()
        if _pending:
            _log.warning(
                "dropped %d log() call(s) %s",
                len(_pending),
                "still waiting for their GPU values"
                if locked
                else "that another thread held as the run ended",
            )
        # The captured output's tail, and the marker that spares the worker waiting for the queue to go silent: best effort. What the queue or the lock does not take stays held back, and _drain_and_shutdown warns of it.
        try:
            _publish_queue_items(
                [], capture_ms=int(time.time() * 1000), hard_deadline=by, final=True
            )
        except Exception:
            pass
        try:
            _metric_queue.put(None, block=False)
        except Exception:
            pass
        if _kill_unstarted_worker(process, _metric_queue):
            _spool_for_stopped_worker("had not started")
            return
        if not (_worker_alive(process) or _upload_terminal.get_obj().value):
            # One that has exited reads no more: unless the run was deleted, which rejects them, the queue's groups are spooled the same way.
            _spool_for_stopped_worker(
                f"had exited (exitcode {_worker_exitcode(process)})"
            )
            return
        # Once closed, the feeder writes what it holds and exits (_feeder_left).
        _metric_queue.close()
        left = _feeder_left(_metric_queue, time.monotonic() + _HANDOFF_FEEDER_GRACE)
        if left:
            _log.warning(
                "%d point(s) had not yet reached the upload worker; they are "
                "lost if this process exits now",
                left,
            )
    finally:
        # Only once the checks above of whether the worker runs are over, however they end: while a thread joins an exited worker, is_alive() can read it as running (the join reaps it before recording its exit code), and the queue of a worker that died after hand_off() found it running would be closed rather than spooled.
        threading.Thread(
            target=process.join, name="kymo-worker-reaper", daemon=True
        ).start()


def _kill_unstarted_worker(process, source) -> bool:
    """At a handoff, kill an upload worker that has not begun (_worker_started), which holds nothing, and return True. Under spawn it first imports __main__, which can take seconds: it would outlive the handoff's cap, or die opening the semaphores the parent's exit unlinks, delivering and spooling nothing. Checked holding the queue's reader lock, without which the worker reads nothing, so it cannot take a group meanwhile: its flag is set before its first read."""
    lock = source._rlock
    if _worker_started.value or not lock.acquire(False):
        return False
    try:
        if _worker_started.value:
            return False
        _worker_alive(process, signal.SIGKILL)
        # SIGKILL leaves it no more user code to run, even one it has yet to take.
        process.join(_SHUTDOWN_STEP)
    finally:
        lock.release()
    return True


def _spool_for_stopped_worker(state: str) -> None:
    """Spool, from the parent, what a worker a handoff killed before it began (_kill_unstarted_worker), or found exited, was to read: the queue's groups and those held back, within _HANDOFF_FEEDER_GRACE, as the console's budget allows (_start_salvage). Report how many points it spooled and how many it could not; state says what became of the worker."""
    held = _take_held_back()
    uncharged = _take_held_back(_uncharged)
    total = max(0, _queue_status.get_obj().value) + sum(
        map(_queue_item_size, uncharged)
    )
    deadline = time.monotonic() + _HANDOFF_FEEDER_GRACE
    # The process may exit at the deadline: a record still written then is cut off whole rather than torn, which would cost the whole file.
    thread, outcome, spool = _start_salvage(
        held + uncharged, deadline, until=lambda: deadline
    )
    # Its spool's close comes after its deadline.
    thread.join(_remaining_seconds(deadline) + _SHUTDOWN_STEP)
    if thread.is_alive():
        _log.error(
            "the upload worker %s; spooling its %d point(s) goes on in the "
            "background at %s, and what is not spooled is lost if this process "
            "exits now",
            state,
            total,
            spool.path,
        )
        return
    if outcome["error"] is not None:
        _log.error("spooling for the upload worker failed: %s", outcome["error"])
    salvaged = outcome["salvaged"]
    _log.warning(
        "the upload worker %s; spooled %d of its %d point(s)%s",
        state,
        salvaged,
        total,
        "" if salvaged >= total else ", the rest are lost if this process exits now",
    )
    if salvaged:
        _report_spool([spool.path])


def _feeder_left(source, deadline: float) -> int:
    """Wait until deadline (time.monotonic()) for the feeder thread of a closed multiprocessing.Queue, which writes the groups it holds and exits, and return how many points the groups it has not taken by then hold (the queue's private _thread and _buffer). A group it is writing then, cut short if the process exits, the upload worker reports (kymo._worker._QueueReads)."""
    thread = source._thread
    if thread is None:
        return 0
    thread.join(_remaining_seconds(deadline))
    if not thread.is_alive():
        return 0
    return sum(
        _queue_item_size(item)
        for item in list(source._buffer)
        if isinstance(item, (tuple, list))
    )


def _shut_down(shutdown: _Shutdown) -> bool:
    """The phases of a shutdown (_drain_and_shutdown); _HandedOff once a Ctrl-C hands it off."""
    global _system_poller
    shutdown.check()
    poller_quiesced = True
    if _system_poller is not None:
        poller_quiesced = _system_poller.stop(timeout=_remaining_seconds(shutdown.hard))
        _system_poller = None

    # log() calls still copying GPU values get half the budget, including any wait for another thread that holds them while it waits for the GPU; any left then are published without those values.
    shutdown.plan_pending(time.monotonic())
    if _await(
        lambda timeout: _pending_lock.acquire(timeout=timeout),
        lambda: shutdown.pending_by,
    ):
        try:
            # Not `while _pending:`: Python 3.13 leaves a conditional while loop's jump back, where a handler may raise, outside the try (cpython#130279).
            while True:
                if not _pending:
                    break
                shutdown.check()
                step = min(time.monotonic() + _SHUTDOWN_STEP, shutdown.pending_by)
                _publish_oldest_pending(
                    step,
                    drop_unfinished=step >= shutdown.pending_by,
                    hard_deadline=shutdown.hard,
                )
        finally:
            _pending_lock.release()
    else:
        _log.error("the pending log() calls stayed locked by another thread")

    # Shared accounting is read without its lock (get_obj): SIGKILL can strand the Value's SemLock in the worker's accounting update, and the worker is the only concurrent decrementer, so a raw snapshot suffices.
    backlog = _queue_status.get_obj().value
    if backlog > 0:
        _log.info("flushing %d queued points (budget %.0fs)…", backlog, shutdown.budget)

    capture_tail_published = True
    try:
        _publish_queue_items(
            [],
            capture_ms=int(time.time() * 1000),
            hard_deadline=shutdown.hard,
            final=True,
        )
    except (OSError, RuntimeError, ValueError, queue.Full) as error:
        capture_tail_published = False
        shutdown.check()
        _log.warning("failed to publish final captured output: %s", error)

    shutdown.plan_worker(time.monotonic())
    shutdown_marker_published = False
    try:
        shutdown_marker_published = _await(
            _put_shutdown_marker, lambda: shutdown.graceful
        )
    except (OSError, ValueError, queue.Full, _WorkerExited) as error:
        _log.warning("failed to publish upload shutdown marker: %s", error)
    else:
        if not shutdown_marker_published:
            _log.warning("upload queue stayed full; no shutdown marker published")

    worker_dead, worker_clean = _stop_upload_process(
        _upload_process,
        graceful_deadline=lambda: shutdown.graceful,
        stop_deadline=lambda: shutdown.stop_by,
    )
    shutdown.stopped = True

    # The worker has delivered what it will, so the run's last metrics are persisted before it flips to CRASHED/FINISHED. End it now: the salvage and the spool listing below only write and read the disk, and the server takes points after a run has ended, so they must not use up TerminateRun's time.
    shutdown.check()
    shutdown.end_run()

    salvage_finished = True
    current_backlog = _queue_status.get_obj().value
    terminal_rejected = bool(_upload_terminal.get_obj().value)
    # Groups still held back, charged or not, were never put, and with the worker stopped never will be: they leave delivery unproven, and a salvage spools them; any it does not take the shutdown drops and counts (_drain_and_shutdown).
    held_back = bool(_held_back or _uncharged)
    needs_salvage = not worker_clean or current_backlog != 0 or held_back
    if terminal_rejected:
        # The worker can be force-killed while draining a very large producer
        # queue after a lifecycle rejection. Those remaining points are known
        # to be permanently inadmissible, so deliberately drop them instead of
        # turning them into an immortal replay queue.
        if needs_salvage:
            _log.error(
                "run is deleted or no longer exists; discarding %d queued "
                "point(s) instead of creating a replayable salvage spool",
                max(0, current_backlog),
            )
    elif needs_salvage and worker_dead and _remaining_seconds(shutdown.pre_cleanup) > 0:
        # Worker was killed or crashed; the owner's fresh unique fence remains ordered after every earlier owner publication, even if its feeder is still pickling one.
        salvage_finished = _salvage_queue_before(
            shutdown, _take_held_back() + _take_held_back(_uncharged)
        )
    elif needs_salvage and not worker_dead:
        _log.error(
            "upload worker is still alive after SIGKILL; skipping unsafe salvage"
        )

    # A killed worker leaves the queue's feeder thread wedged on a full
    # pipe, and Queue._finalize_join would join it forever at interpreter
    # exit — the original "trainer finished but never exited" hang.
    _metric_queue.cancel_join_thread()
    # A deadline-expired daemon salvage may still be reading this queue.
    if salvage_finished:
        _metric_queue.close()

    # Covers the pre-created worker path, every segment the worker rotated to,
    # and the salvage spool: they all carry this init's session prefix.
    had_spool = _resolve_run_spools(
        terminal_rejected=terminal_rejected, deadline=lambda: shutdown.hard
    )

    final_backlog = _queue_status.get_obj().value
    final_spooled = _upload_spooled.get_obj().value
    if final_backlog != 0:
        _log.error(
            "shutdown accounting incomplete: %d point(s) remain unacknowledged",
            final_backlog,
        )

    # A gallery the worker published or spooled cut short, the one failure it records and exits cleanly after, leaves delivery incomplete.
    values_complete = (
        not (_pending_incomplete or _pending or held_back)
        and not _upload_failure.get_obj().value
    )
    # Complete delivery takes every proof. A terminated worker takes its private in-RAM buffers with it, so queue salvage alone cannot prove it.
    return (
        worker_clean
        and shutdown_marker_published
        and capture_tail_published
        and values_complete
        and poller_quiesced
        and not had_spool
        and final_backlog == 0
        and final_spooled == 0
    )


# ---------------------------------------------------------------------------
# CDN helpers (run in worker process)
# ---------------------------------------------------------------------------


def _encode_image(img: "Image") -> tuple[bytes, str]:
    """Encode an Image to bytes. Returns (encoded_bytes, extension)."""
    data = img.data
    fmt = img.format.lower()
    from PIL import Image as PILImage

    # The default format is only a fallback for array/PIL encoding, so detect the actual container when raw bytes use it; a non-default format is kept as an override, checked against the CDN's list below.
    if isinstance(data, bytes) and fmt == "png":
        with PILImage.open(io.BytesIO(data)) as encoded:
            if encoded.format is None:
                raise ValueError("encoded image format could not be detected")
            fmt = encoded.format.lower()
    # A camera's multi-picture JPEG is a JPEG stream; browsers show its first picture.
    if fmt == "mpo":
        fmt = "jpeg"
    # The CDN refuses any other extension; one that is not even a token (empty, spaced) would also fail replay's pre-scan of a spooled segment and quarantine the segment whole.
    if fmt not in _CDN_EXTENSIONS:
        raise ValueError(f"unsupported image format {fmt!r}")
    if isinstance(data, bytes):
        # Raw bytes stay byte-identical.
        return data, fmt

    if isinstance(data, PILImage.Image):
        pil_img = data
    else:
        import numpy as np

        if not isinstance(data, np.ndarray):
            try:
                import torch
            except ImportError:
                pass
            else:
                if isinstance(data, torch.Tensor):
                    data = data.detach().cpu().numpy()
        if not isinstance(data, np.ndarray):
            raise TypeError(f"Unsupported image data type: {type(data)}")

        # Handle float [0,1] or [0,255]
        if data.dtype in (np.float32, np.float64, np.float16):
            if data.max() <= 1.0:
                data = (data * 255).clip(0, 255).astype(np.uint8)
            else:
                data = data.clip(0, 255).astype(np.uint8)
        # HWC is the documented form, preferred when both ends look like channel axes (a three-pixel-tall RGB image, say); CHW is transposed to it, and one channel is grayscale.
        if (
            data.ndim == 3
            and data.shape[2] not in (1, 3, 4)
            and data.shape[0] in (1, 3, 4)
        ):
            data = np.transpose(data, (1, 2, 0))
        if data.ndim == 3 and data.shape[2] == 1:
            data = data[:, :, 0]
        pil_img = (
            PILImage.fromarray(data, mode="L")
            if data.ndim == 2
            else PILImage.fromarray(data)
        )

    buf = io.BytesIO()
    save_fmt = {"jpg": "JPEG", "jpeg": "JPEG"}.get(fmt, fmt.upper())
    # JPEG has no alpha/palette representation. The caller explicitly chose this lossy format, so preserve the image instead of letting Pillow reject an otherwise supported RGBA/LA/P input and dropping it from the gallery.
    if save_fmt == "JPEG" and pil_img.mode not in {
        "1",
        "L",
        "RGB",
        "RGBX",
        "CMYK",
        "YCbCr",
    }:
        pil_img = pil_img.convert("RGB")
    pil_img.save(buf, format=save_fmt)
    return buf.getvalue(), fmt


def _encode_resource(res: "Resource") -> tuple[bytes, str]:
    """Encode a Resource to bytes. Returns (data, extension)."""
    ext = ntpath.splitext(ntpath.basename(res.filename))[1].removeprefix(".").lower()
    # Stored and served as octet-stream; the manifest keeps the real filename.
    if ext not in _CDN_EXTENSIONS:
        ext = "bin"
    return res.data, ext


class _EncodedItems:
    """A gallery's items encoded once, in the spool's entry form, replacing the Image/Resource list in a worker's queued cdn_batch tuple."""

    __slots__ = ("entries", "dropped", "nbytes")

    def __init__(self, entries: list[dict], dropped: bool):
        self.entries = entries
        # An item failed to encode, so the manifest must publish under the reduced mutation version.
        self.dropped = dropped
        # Captions and filenames keep lone surrogates until display, so a strict encode would raise.
        self.nbytes = sum(
            len(value.encode("utf-8", "surrogatepass"))
            if isinstance(value, str)
            else len(value)
            for entry in entries
            for value in entry.values()
            if isinstance(value, (str, bytes))
        )


def _encode_cdn_items(name: str, step: int, items: list) -> _EncodedItems:
    """Encode a gallery's items in place, each into its spool entry, or None when it cannot be encoded (dropped with a warning, since a retry can never fix it).

    Replacing each item as it is encoded releases its arrays at once, so a gallery is never held both whole and encoded, and a retry after a MemoryError resumes at the item that raised. Callers pass a decoded snapshot, never a list given to log().
    """
    for index, item in enumerate(items):
        try:
            if isinstance(item, Image):
                data, ext = _encode_image(item)
                entry = {
                    "kind": "image",
                    "data": data,
                    "ext": ext,
                    "caption": item.caption,
                }
            elif isinstance(item, Resource):
                data, ext = _encode_resource(item)
                entry = {
                    "kind": "resource",
                    "data": data,
                    "ext": ext,
                    "content_type": item.content_type,
                    "filename": item.filename,
                }
            else:
                # Encoded or dropped by an earlier attempt.
                continue
        except MemoryError:
            raise
        except Exception as e:
            entry = None
            _log.warning(
                "failed to encode CDN item (%s step %d) — dropping it: %s",
                name,
                step,
                e,
            )
        items[index] = entry
    entries = [entry for entry in items if entry is not None]
    return _EncodedItems(entries, dropped=len(entries) < len(items))


def _publish_manifest_point(
    stub,
    project_id: str,
    run_id: str,
    metric_name: str,
    step: int,
    cdn_key: str,
    timestamp_ms: Optional[int],
    mutation_version: Optional[int],
) -> bool:
    """Publish a rich record's manifest key: as its versioned mutation, or to an old server as a point stamped at publication."""
    if mutation_version is not None:
        return _publish_rich_mutation(
            stub,
            project_id,
            run_id,
            metric_name,
            step,
            cdn_key,
            timestamp_ms,
            mutation_version,
        )
    point = kymo_pb2.MetricPoint(
        metric_name=metric_name,
        step=step,
        cdn_key=cdn_key,
        timestamp_ms=int(time.time() * 1000),
    )
    return _send_unary_point(stub, project_id, run_id, point)


def _process_cdn_batch(
    stub,
    http_client,
    cdn_url: str,
    project_id: str,
    run_id: str,
    metric_name: str,
    step: int,
    items: _EncodedItems,
    deadline_fn=None,
    send_placeholder: bool = True,
    timestamp_ms: Optional[int] = None,
    mutation_version: Optional[int] = None,
    reduced_mutation_version: Optional[int] = None,
) -> bool | int:
    """Upload encoded items and publish their gallery's manifest.

    Return True for a complete gallery, _WORKER_GALLERY_CUT_SHORT once the manifest went out without items its encode dropped or the CDN rejected, and False for a failure the caller retries, keeping the batch queued: running this again is safe, since re-uploads dedup by content hash and a manifest's re-send replaces itself. A legacy retry must pass send_placeholder=False: those rows take the server's arrival order, so its placeholder could overwrite a real point whose commit succeeded but whose ack was lost. Versioned writers publish no placeholders.

    Return False as soon as deadline_fn fires, so a shutdown spools the batch instead of finishing slow uploads.
    """
    # 1. Send placeholder (first attempt only, see docstring)
    if send_placeholder and mutation_version is None:
        placeholder_key = f"pending:{uuid.uuid4().hex[:16]}"
        placeholder_point = kymo_pb2.MetricPoint(
            metric_name=metric_name,
            step=step,
            cdn_key=placeholder_key,
            # Without a timestamp the server's heartbeat sees epoch 0 and an image-only run reads as STUCK/dead.
            timestamp_ms=int(time.time() * 1000),
        )
        try:
            _send_unary_point(stub, project_id, run_id, placeholder_point)
        except grpc.RpcError as e:
            _log.error("failed to send placeholder: %s", e.details())

    # 2. Upload the encoded items. A CDN rejection drops its item, like an encode failure; any other upload failure is transient and fails the whole batch for retry — committing a manifest missing the failed items would silently thin the gallery. A retry re-uploads the already-uploaded prefix (the CDN dedups by content hash). Past the deadline the caller spools the batch, so finishing uploads here is pure shutdown latency against the parent's kill grace.
    manifest_items = []
    manifest_changed = items.dropped
    for entry in items.entries:
        if deadline_fn is not None and deadline_fn():
            return False
        try:
            resource_id = _upload_to_cdn(
                http_client, cdn_url, entry["data"], entry["ext"]
            )
        except Exception as e:
            if _is_permanent_upload_error(e):
                manifest_changed = True
                _log.warning(
                    "CDN rejected item (%s step %d) — dropping it: %s",
                    metric_name,
                    step,
                    e,
                )
                continue
            _log.warning("failed to upload resource: %s", e)
            return False
        manifest_items.append(encoded_gallery_item(entry, resource_id))

    if not manifest_items:
        # Fall through and log an EMPTY manifest rather than returning early: the placeholder is already in the store, and consuming the batch without replacing it left the gallery cell "Uploading…" forever (a re-logged step even masked its previous valid manifest with an eternal placeholder).
        _log.error(
            "no encodable/accepted items in CDN batch %s step %d — logging an empty gallery",
            metric_name,
            step,
        )

    # 3. Build and upload manifest
    encoded_manifest = gallery_manifest(manifest_items)
    try:
        manifest_id = _upload_to_cdn(http_client, cdn_url, encoded_manifest, "json")
    except Exception as e:
        _log.error("failed to upload manifest: %s", e)
        return False

    # 4. Publish the immutable manifest. New servers compare the parent-issued
    # logical version before inserting, so a delayed retry cannot overwrite a
    # newer same-step value. Legacy servers retain arrival-order replacement.
    if mutation_version is not None:
        assert timestamp_ms is not None
        if manifest_changed:
            mutation_version = _reduced_mutation_version(reduced_mutation_version)
    try:
        published = _publish_manifest_point(
            stub,
            project_id,
            run_id,
            metric_name,
            step,
            manifest_id,
            timestamp_ms,
            mutation_version,
        )
    except grpc.RpcError as e:
        _log.error("failed to publish manifest %s: %s", manifest_id, e.details())
        return False
    if not published:
        return False
    _log.info(
        "CDN %s step=%d → %s (%d items)",
        metric_name,
        step,
        manifest_id,
        len(manifest_items),
    )
    return _WORKER_GALLERY_CUT_SHORT if manifest_changed else True


_CONNECT_INITIAL_DELAY = 0.5
_CONNECT_READY_TIMEOUT = 5.0
_CONNECT_POLL_TIMEOUT = 0.25
# Disk catch-up is a fleet-wide outage path. Its longer, jittered ceiling keeps
# workers that observed the same restart from hammering the replacement pod in
# lockstep. Ordinary in-memory stream retries retain the low-latency 5s cap.
_RECOVERY_MAX_DELAY = 60.0
# Attempts on one CDN batch before the rich lane fails over to the spool (exponential backoff, 2s..60s — a few minutes of outage total). Permanent rejections (server 400s the content) also land here rather than wedging the queue head forever.
_CDN_MAX_ATTEMPTS = 8
# Worker exit code: some points reached neither the server nor the spool (a spool write failed, e.g. disk full, a gallery could not be encoded for the spool, a queued snapshot would not unpickle in the worker, its spill end passed before it spilled them, or it never read them and no salvage follows). finish() must not report success.
_WORKER_EXIT_SPOOL_FAILED = 2
# The worker stopped without observing the owner's shutdown marker. The parent
# must run its fenced salvage path before delivery can be claimed.
_WORKER_EXIT_DRAIN_INCOMPLETE = 3
# The identity is deleted/purged. Retrying or spooling would create an
# immortal backlog, so the worker rejects queued points and reports failure.
_WORKER_EXIT_RUN_DELETED = 4
# A spool segment was quarantined as *.rejected (the server answered DATA_LOSS for a rich mutation in it, or it held records that can never be delivered), and later data kept flowing.
_WORKER_EXIT_DATA_LOSS = 5
# Not an exit code, and what _process_cdn_batch returns for it: the worker published a gallery without items its encode dropped or the CDN rejected, so delivery is incomplete, as for a value dropped after its log() returned.
_WORKER_GALLERY_CUT_SHORT = 6
# Loss evidence outranks other failures, and an outright spool failure outranks a quarantine; any failure outranks a gallery cut short.
_FAILURE_RANK = {
    _WORKER_GALLERY_CUT_SHORT: -1,
    _WORKER_EXIT_DATA_LOSS: 1,
    _WORKER_EXIT_SPOOL_FAILED: 2,
}


def _merged_worker_failure(current: int, new: int) -> int:
    """Keep the strongest data-loss evidence; otherwise preserve the first terminal failure."""
    if current == 0 or _FAILURE_RANK.get(new, 0) > _FAILURE_RANK.get(current, 0):
        return new
    return current


# Queue poll cadence while no transport work is active.
_IDLE_POLL_TIMEOUT = 0.5
# While transport work is active, 50ms polling preserves measured throughput while cutting empty-producer wakeups about 5x versus 10ms. A busy producer's get() returns immediately.
_ACTIVE_POLL_TIMEOUT = 0.05

# Bidi in-flight window: fed-but-unacked points stay within this bound. Matching feed-queue capacity gives the stream up to one window of runway between active-loop polls.
_MAX_UNACKED_POINTS = 100_000
# Built MetricsBatch protos the generator may hold ahead of gRPC (window /
# _MAX_POINTS_PER_MSG). A text burst that byte-caps into more just feeds later.
_FEED_Q_CAP = _MAX_UNACKED_POINTS // _MAX_POINTS_PER_MSG
_ACK_PROGRESS_TIMEOUT = _SEND_CALL_TIMEOUT
# Max queue items (one per log() call) pulled per loop iteration — keeps the
# loop responsive to the shutdown deadline.
_DRAIN_MAX_ITEMS_PER_CYCLE = 2_000
# RAM cap on points held in the worker; crossing it fails that ordered lane
# over to disk so a dead/slow server cannot grow worker memory unboundedly.
# Numeric uploads periodically half-open again on a fresh ready channel. Until
# the first ACK proves ingest progress, use a much smaller cap so a server that
# accepts TCP but still cannot ingest cannot consume another full RAM window.
_DEFAULT_MAX_BUFFER_POINTS = 2_000_000
_DEFAULT_MAX_BUFFER_BYTES = 256 * 1024 * 1024
_RECOVERY_MAX_BUFFER_POINTS = _MAX_UNACKED_POINTS
_RECOVERY_MAX_BUFFER_BYTES = 32 * 1024 * 1024
# A live numeric lane that holds points and gets no ACK for this long fails over to disk, so an outage's points do not sit only in RAM until the cap.
_NUMERIC_OUTAGE_FAILOVER = 180.0
# Bound ordered CDN entries; retain_queue_item in _upload_worker says what a queued gallery is charged.
_MAX_CDN_QUEUE = 1_000
_DEFAULT_MAX_RICH_BUFFER_BYTES = 2 * 1024 * 1024 * 1024
_ORDERED_CDN_KINDS = (
    "cdn_batch",
    "metadata_batch",
    "cdn_batch_mutation",
    "metadata_batch_mutation",
    "cdn_key_mutation",
)

_CONNECT_PENDING = object()


class _RetainedLane:
    """Ordered retained items and their cached memory/wire-size estimates."""

    __slots__ = ("items", "sizes", "total_bytes")

    def __init__(self) -> None:
        self.items: list[tuple] = []
        self.sizes: list[int] = []
        self.total_bytes = 0

    def __len__(self) -> int:
        return len(self.items)

    def append(self, item: tuple, size: int) -> None:
        self.items.append(item)
        self.sizes.append(size)
        self.total_bytes += size

    def replace(self, index: int, item: tuple, size: int) -> None:
        self.items[index] = item
        self.total_bytes += size - self.sizes[index]
        self.sizes[index] = size

    def discard_prefix(self, count: int) -> None:
        self.total_bytes -= sum(self.sizes[:count])
        del self.items[:count]
        del self.sizes[:count]

    def clear(self) -> None:
        self.items.clear()
        self.sizes.clear()
        self.total_bytes = 0


class _ConnectionAttempt:
    """One channel-readiness attempt, which the worker loop polls without blocking."""

    def __init__(self, server_address: str, *, local_endpoint=None):
        self._server_address = server_address
        self._deadline = time.monotonic() + _CONNECT_READY_TIMEOUT
        if local_endpoint is None:
            self._channel = grpc.insecure_channel(server_address)
        else:
            from kymo._local_runtime import grpc_channel

            self._channel = grpc_channel(local_endpoint)
        self._ready = grpc.channel_ready_future(self._channel)

    def poll(self, deadline_fn=None):
        """Return a connected pair, a failed pair, or _CONNECT_PENDING."""
        if (
            deadline_fn is not None and deadline_fn()
        ) or time.monotonic() >= self._deadline:
            self.cancel()
            return None, None
        if not self._ready.done():
            return _CONNECT_PENDING
        try:
            self._ready.result()
        except Exception as error:
            _log.info("connect attempt failed: %s", error)
            self.cancel()
            return None, None
        channel = self._channel
        self._channel = None
        _log.info("connected to %s", self._server_address)
        return channel, kymo_pb2_grpc.KymoStub(channel)

    def cancel(self) -> None:
        if self._channel is None:
            return
        try:
            self._ready.cancel()
        except Exception:
            pass
        self._channel.close()
        self._channel = None


def _next_connect_retry_delay(current: float) -> float:
    """Advance the owner-loop readiness retry delay, capped at five seconds."""
    return min(current * 2, _SEND_MAX_DELAY)


def _equal_jitter_delay(base: float, rng: random.Random) -> float:
    """Spread a retry over the upper half of its exponential-backoff slot."""
    return base * (0.5 + 0.5 * rng.random())


def _recovery_retry_delay(failures: int, rng: random.Random) -> float:
    """Equal-jitter exponential backoff for outage recovery.

    The random half prevents a fleet of workers that failed over to disk at
    roughly the same time from all probing a restarted backend together. Keep
    the whole interval below the cap rather than clamping randomized values to
    it: clamping would create a large point mass at exactly 60 seconds.
    """
    base = min(
        _CONNECT_INITIAL_DELAY * (2 ** min(max(0, failures - 1), 16)),
        _RECOVERY_MAX_DELAY,
    )
    return _equal_jitter_delay(base, rng)


def _cdn_retry_delay(failures: int) -> float:
    """Exponential backoff for the retained cdn_queue head, capped at 60s."""
    return min(2.0**failures, 60.0)


def _spool_header(
    server_address: str,
    project_id: str,
    run_id: str,
    run_name: str,
    cdn_address: str,
    session: str = "",
    local_installation_uuid: str = "",
) -> dict:
    header = {
        "server_address": "" if local_installation_uuid else server_address,
        "cdn_address": "" if local_installation_uuid else cdn_address,
        "project_id": project_id,
        "run_id": run_id,
        "run_name": run_name,
        # One init()'s identity, retained in the header for replay validation and
        # rollback compatibility. The filename carries the same value so bounded
        # shutdown cleanup does not need to open the file to classify ownership.
        "session": session,
        "created_unix": int(time.time()),
        "created_unix_ns": time.time_ns(),
    }
    if local_installation_uuid:
        header.update(
            {
                "target_kind": "local",
                "installation_uuid": local_installation_uuid,
            }
        )
    return header


def _spool_record(point_tuple: tuple) -> tuple:
    """The spool record for one queue tuple, encoding CDN payloads not yet encoded so the file is self-contained."""
    kind = point_tuple[0]
    if kind in _METADATA_KINDS:
        # Its data is already JSON, frozen as it was queued (_snapshot_rich_queue_items).
        _, name, step, metadata, *versions = point_tuple
        record_kind = "metadata_json_mutation" if versions else "metadata_json"
        return (record_kind, name, step, metadata.data, *versions)
    if kind not in _GALLERY_KINDS:
        return point_tuple
    _, name, step, items, *versions = point_tuple
    # The worker's encoder may already have encoded this gallery.
    if not isinstance(items, _EncodedItems):
        items = _encode_cdn_items(name, step, items)
    # Recorded even when every item failed to encode. Legacy batches may
    # need an empty manifest to clear their pending:* placeholder;
    # versioned batches still represent a real empty-gallery mutation.
    if not versions:
        return ("cdn_batch_encoded", name, step, items.entries)
    timestamp_ms, _, reduced_mutation_version = versions
    if items.dropped:
        # Under the version reserved for the reduced gallery, which leaves none in reserve.
        return (
            "cdn_batch_encoded_mutation",
            name,
            step,
            items.entries,
            timestamp_ms,
            reduced_mutation_version,
        )
    # A distinct kind makes the extra field forward-safe: older replay tools preserve unknown records instead of rejecting a valid newer tuple as malformed.
    return ("cdn_batch_encoded_mutation_reserved", name, step, items.entries, *versions)


def _spill_tuple(spool, point_tuple: tuple) -> bool:
    """Write one queue tuple to the spool, returning whether it was written.

    False means its record could not be built, such as a gallery too large to encode in the memory left (never reduced to the images that did encode): the point is lost, but nothing was written, so later points still spool. An exception comes from the write, after the writer has cut the failed record off.
    """
    try:
        record = _spool_record(point_tuple)
    except Exception as error:
        _log.error(
            "could not encode %r for the spool — 1 point LOST: %r",
            point_tuple[:3],
            error,
        )
        return False
    spool.write(record)
    return True


def _validated_ack_delta(cumulative: int, last_cumulative: int, inflight_n: int) -> int:
    """Validate one cumulative ACK before it can mutate delivery accounting."""
    upper = last_cumulative + inflight_n
    if cumulative < last_cumulative or cumulative > upper:
        raise ValueError(
            f"invalid cumulative ack {cumulative} (expected {last_cumulative}..{upper})"
        )
    return cumulative - last_cumulative


class _ThreadAttempt:
    """Run one blocking operation without stalling the upload owner loop."""

    def __init__(self, item, operation, *, name: str):
        self.item = item
        self.events: queue.Queue = queue.Queue(maxsize=1)
        thread = threading.Thread(
            target=self._run,
            args=(operation,),
            daemon=True,
            name=name,
        )
        thread.start()

    def _run(self, operation) -> None:
        try:
            outcome = (True, operation())
        except BaseException as error:
            outcome = (False, error)
        self.events.put(outcome)


class _BidiStream:
    """Owns ONE bidi-ingest attempt's transport, nothing else: a bounded
    feed queue the worker fills with immutable protos + a generator that yields
    them to gRPC, and a daemon pump thread moving cumulative acks off the
    response stream into a thread-safe queue.

    Holds NONE of the worker's accounting (buffer, inflight_n, last_acked_cum,
    queue_status) — the worker stays the sole mutator of point state. The two
    threads touch only immutable protos and thread-safe queues, so no race with
    the loop can lose or double-count a point."""

    def __init__(self, stub, feed_cap: int, rpc=None):
        self._feed_q: queue.Queue = queue.Queue(maxsize=feed_cap)
        self._ack_q: queue.Queue = queue.Queue()
        # Hard teardown (cancel) only: frees the generator parked in feed_q.get(),
        # which call.cancel() can't unblock (gRPC abandons the thread → leak). A
        # clean half-close feeds the sentinel instead.
        self._dead = threading.Event()
        self._sentinel = object()
        # No lifetime deadline: the stream lives for the whole run. The worker's
        # ACK-progress watchdog and shutdown deadline cancel hung attempts.
        # `rpc` selects a sibling bidi method with the same wire contract (the
        # bulk importer's ImportMetricsBidi); the default is the live lane.
        self._call = (rpc or stub.IngestMetricsBidi)(self._request_gen(), timeout=None)
        self._pump = threading.Thread(target=self._pump_acks, daemon=True)
        self._pump.start()

    def _request_gen(self):
        while True:
            batch = self._feed_q.get()
            if batch is self._sentinel or self._dead.is_set():
                return  # half-close: end of the request stream
            yield batch

    def _pump_acks(self):
        try:
            for ack in self._call:
                self._ack_q.put(("ack", ack.points_acked))
        # Never let the pump die silently; surface every failure as a stream break.
        except Exception as e:
            self._ack_q.put(("error", e))
        else:
            self._ack_q.put(("done", None))

    def feed_has_room(self) -> bool:
        return not self._feed_q.full()

    def feed(self, batch) -> bool:
        """Non-blocking. False if the feed queue is full (the caller keeps the
        points in its buffer and retries next cycle)."""
        try:
            self._feed_q.put_nowait(batch)
            return True
        except queue.Full:
            return False

    def poll_acks(self) -> list:
        """Drain all pending ack markers without blocking. Each is
        ("ack", cumulative_points_acked) | ("done", None) | ("error", RpcError)."""
        markers = []
        while True:
            try:
                markers.append(self._ack_q.get_nowait())
            except queue.Empty:
                return markers

    def half_close(self) -> bool:
        """Try to append ordered request EOF after every already-fed batch.

        False means the bounded feed queue is full; the owner must retry after
        the gRPC generator consumes a batch. Never set ``_dead`` here: doing so
        would let cancellation discard queued batches already counted in
        ``inflight_n`` instead of letting EOF final-flush them.
        """
        return self.feed(self._sentinel)

    def cancel(self):
        """Hard teardown (deadline or stream break): cancel the RPC and release
        both threads. _dead frees a generator parked in feed_q.get() that
        cancel() alone can't."""
        self._dead.set()
        try:
            self._feed_q.put_nowait(self._sentinel)
        except queue.Full:
            pass
        try:
            self._call.cancel()
        except Exception:
            pass
