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
import io
import json
import math
import multiprocessing
import ntpath
import operator
import os
import pickle
import queue
import random
import shlex
import signal
import subprocess
import sys
import threading
import time
import uuid
from typing import Optional
from urllib.parse import urlsplit

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
from kymo._cdn import gallery_item, gallery_manifest, metadata_manifest
from kymo._log import logger as _log
from kymo._wire import (
    _CDN_RPC_TIMEOUT,
    _F32_OVERFLOW,
    _MAX_ID_BYTES,
    _MAX_METRIC_NAME_BYTES,
    _MAX_POINTS_PER_MSG,
    _MAX_POINT_BYTES,
    _MAX_RICH_RESOURCE_ID_BYTES,
    _PermanentPointError as _PermanentPointError,
    _SEND_CALL_TIMEOUT,
    _SEND_MAX_DELAY,
    _RichMutationDataLoss as _RichMutationDataLoss,
    _TerminalRunError,
    _chunk_tuples,
    _estimate_tuple_bytes,
    _is_permanent_upload_error,
    _is_terminal_run_error,
    _normalize_numeric_value,
    _publish_rich_mutation,
    _send_tuples as _send_tuples,
    _terminal_run_error,
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
    quarantine_spool as quarantine_spool,
    retire_deleted_spool,
    run_spool_files,
    spool_name_prefix,
    writer_active,
)
from kymo.types import Image, Metadata, Resource

_MAX_RUN_NAME_BYTES = 2048
# Client-side mirror of ALLOWED_EXTENSIONS in kymo-server/src/cdn.rs (keep in sync): the server 400s any other extension and the item would be dropped as a permanent upload error, so unsupported extensions are coerced to "bin" (stored/served as octet-stream; the manifest keeps the real filename).
_RESOURCE_EXTENSIONS = frozenset(
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
):
    """Run a lifecycle RPC in a clean interpreter, outside the trainer.

    Linux's default multiprocessing start method forks. Initializing gRPC in
    the trainer before starting the upload worker makes that fork unsafe and
    has produced nondeterministic shutdown hangs. Keeping synchronous control
    RPCs in a short-lived exec child leaves the trainer gRPC-thread-free; the
    long-lived worker can then fork safely without imposing spawn semantics on
    existing hosted training scripts.
    """
    payload = {
        "method": method,
        "request": base64.b64encode(request.SerializeToString()).decode("ascii"),
        "server_address": server_address,
        "timeout": timeout,
        "local_endpoint": (
            local_endpoint.worker_config() if local_endpoint is not None else None
        ),
    }
    try:
        completed = subprocess.run(
            [sys.executable, "-m", "kymo._control_rpc"],
            input=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
            # Force subprocess's posix_spawn path on POSIX. Training programs
            # are commonly multithreaded; fork-before-exec is unsafe there for
            # the same reason that initializing gRPC before the worker fork is.
            # Python-created descriptors are non-inheritable by default.
            close_fds=False,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(f"{method} timed out after {timeout:g}s") from error
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        if len(detail) > 1000:
            detail = detail[-1000:]
        raise RuntimeError(
            f"{method} failed in control process: {detail or 'unknown error'}"
        )
    try:
        return response_type.FromString(completed.stdout)
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
    return int(normalized)


def _resolve_mode(mode: Optional[str]) -> str:
    value = _env_string("KYMO_MODE", "MKDB2_MODE", "hosted") if mode is None else mode
    if not isinstance(value, str):
        raise TypeError("kymo.init: mode must be a string")
    value = value.strip().lower()
    if value not in ("hosted", "local"):
        raise ValueError("kymo.init: mode must be 'hosted' or 'local'")
    return value


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
# Monotonic deadline for the upload worker, 0.0 = none. Once passed, the worker
# stops sending and spools the rest — bounded shutdown.
_shutdown_deadline: Optional[multiprocessing.Value] = None
# Spool path chosen at init so the parent can report it even if the worker
# was killed before logging it.
_spool_path: str = ""
_spool_dir: Optional[str] = None
# Identifies the spool files this init() is answerable for. Written into every
# basename and header this run produces — the pre-created path, each segment the
# worker rotates to, and the parent's salvage. Shutdown classifies ownership by
# basename without opening files; replay and rollback retain the header copy.
_session_id: str = ""
_last_backlog_warn: float = 0.0
# Whether the last _drain_and_shutdown left a spool file OR delivery could not be proven (finish() returns the inverse).
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
_capture_drain_lock = threading.Lock()
_last_capture_step: Optional[int] = None
# Captured exit code (None = not yet observed; resolved at TerminateRun time).
# Set by signal handler, sys.excepthook, or wrapped sys.exit — first writer wins.
_exit_code: Optional[int] = None
_original_excepthook = None
_original_sys_exit = None
# Snapshot of the exact metadata blob we last uploaded. update_config() mutates
# the `config` field and re-uploads, leaving meta/system/git/slurm bit-identical
# to the original — guarantees only the config field changes between uploads.
_run_metadata: Optional[dict] = None
# Presence is negotiated by InitRunResponse. An older server leaves this None,
# preserving the existing hosted rich-write path byte-for-byte.
_rich_writer_epoch: Optional[int] = None
_rich_mutation_seq = None  # multiprocessing.Value("I") when versioning is negotiated


def _reserve_rich_mutation_versions(count: int) -> Optional[tuple[int, ...]]:
    """Atomically reserve persisted logical versions, or select legacy behavior."""
    if _rich_writer_epoch is None:
        return None
    if count < 1:
        raise ValueError("rich mutation reservation must be nonempty")
    if _rich_mutation_seq is None:
        raise RuntimeError("kymo rich mutation allocator is not initialized")
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


if hasattr(os, "register_at_fork"):
    os.register_at_fork(
        before=_capture_drain_lock.acquire,
        after_in_parent=_capture_drain_lock.release,
        after_in_child=_capture_drain_lock.release,
    )


def _next_rich_mutation_version() -> Optional[int]:
    """Allocate the next persisted logical version, or select legacy behavior."""
    versions = _reserve_rich_mutation_versions(1)
    return versions[0] if versions is not None else None


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
        config: Optional run configuration dict (hyperparams, etc.).
        spool_dir: Where undeliverable points are spooled for later replay
            (``python -m kymo.sync``). Falls back to ``$KYMO_SPOOL_DIR`` or
            ``~/.cache/kymo/spool``. Point it somewhere shared (e.g.
            next to your checkpoint dir) so a CPU node can replay it.
    """
    global _metric_queue, _upload_process, _is_initialized, _init_pid
    global \
        _queue_status, \
        _project_id, \
        _run_id, \
        _run_name, \
        _cdn_address, \
        _url_base, \
        _atexit_registered
    global _system_poller, _server_address, _shutdown_deadline, _spool_path, _spool_dir
    global _session_id
    global _upload_failure, _upload_spooled, _upload_terminal
    global _mode, _local_installation_uuid
    global _rich_writer_epoch, _rich_mutation_seq
    global _exit_code, _last_log_duration_ms

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
    mode = _resolve_mode(mode)
    if url_base is not None and not isinstance(url_base, str):
        raise TypeError("kymo.init: url_base must be a string")
    if mode == "local" and (
        server_address is not None or cdn_address is not None or url_base is not None
    ):
        raise ValueError(
            "kymo.init: server_address/cdn_address/url_base cannot override local runtime endpoints"
        )
    if mode == "hosted":
        server_address = server_address or _env_string("KYMO_SERVER", "MKDB2_SERVER")
        if not server_address:
            raise ValueError(
                "kymo.init: hosted mode needs a server address; pass server_address, "
                "set KYMO_SERVER, or use mode='local'"
            )
        if url_base is None:
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
    # The upload child inherits this cwd, while the parent can change cwd before shutdown inventory/salvage. Freeze one shared location now.
    spool_dir = os.path.abspath(spool_dir or default_spool_dir())

    if not _atexit_registered:
        _require_shutdown_signal_capability()

    # Shut down previous worker if re-initialising
    if _is_initialized:
        _drain_and_shutdown()

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
    _run_name = run_name
    _server_address = server_address
    _url_base = url_base.rstrip("/")
    _mode = mode
    _local_installation_uuid = (
        local_endpoint.installation_uuid if local_endpoint is not None else ""
    )

    # Derive CDN address from gRPC address if not provided
    if cdn_address is None:
        host = server_address.rsplit(":", 1)[0]
        _cdn_address = f"http://{host}:8080"
    else:
        _cdn_address = cdn_address

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
    _upload_failure = multiprocessing.Value("i", 0)
    _upload_spooled = multiprocessing.Value("i", 0)
    _upload_terminal = multiprocessing.Value("i", 0)
    _shutdown_deadline = multiprocessing.Value("d", 0.0)
    _spool_dir = spool_dir
    _session_id = uuid.uuid4().hex
    _spool_path = make_spool_path(
        project_id,
        run_id,
        "worker",
        spool_dir=spool_dir,
        session=_session_id,
    )

    # daemon=True: graceful drain is atexit's job; the daemon flag is the
    # backstop so the interpreter never waits on this process.
    from kymo._worker import _upload_worker as worker_target

    _upload_process = multiprocessing.Process(
        target=worker_target,
        args=(
            server_address,
            project_id,
            run_id,
            _metric_queue,
            _queue_status,
            _cdn_address,
            _shutdown_deadline,
            _spool_path,
            run_name,
            _upload_failure,
            _upload_spooled,
            os.getpid(),
            _upload_terminal,
            _session_id,
            local_endpoint.worker_config() if local_endpoint is not None else None,
        ),
        daemon=True,
    )
    _upload_process.start()

    # Start system metrics poller (GPU/CPU/memory/disk/network)
    # Poller queues directly to the upload queue with time-based steps
    if system_metrics:
        from kymo.system_metrics import SystemMetricsPoller

        _system_poller = SystemMetricsPoller(
            poll_interval=2.0,
            publish=_publish_queue_items,
        )
        _system_poller.start()
    else:
        _system_poller = None

    if not _atexit_registered:
        atexit.register(_ensure_metrics_uploaded)
        _setup_signal_handlers()
        _install_exit_capture()
        _atexit_registered = True

    # Exit hooks and log-overhead tracking span the process lifetime. Clear anything recorded by a previous run at the last possible point before this run becomes active, after registration and worker startup have completed.
    _exit_code = None
    _last_log_duration_ms = 0.0
    _is_initialized = True
    _init_pid = os.getpid()
    _log.info(
        "init project=%s run=%r (id=%s..., ordinal=%d) server=%s cdn=%s",
        project_id,
        run_info.run_name,
        run_id[:8],
        run_info.ordinal,
        server_address,
        _cdn_address,
    )

    # Upload run metadata + config to CDN
    _upload_run_metadata(config)


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
    from urllib.parse import quote

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

    numeric_points = []
    cdn_batches = []

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
            numeric_points.append(("numeric_ts", name, step, value, now_ms))
        elif isinstance(value, (int, float)):
            numeric_points.append(
                ("numeric_ts", name, step, _normalize_numeric_value(value), now_ms)
            )
        elif isinstance(value, Metadata):
            cdn_batches.append(
                _rich_queue_tuple("metadata_batch", name, step, value, now_ms)
            )
        elif isinstance(value, (Image, Resource)):
            cdn_batches.append(
                _rich_queue_tuple("cdn_batch", name, step, [value], now_ms)
            )
        elif (
            isinstance(value, list)
            and value
            and isinstance(value[0], (Image, Resource))
        ):
            for index, item in enumerate(value):
                if not isinstance(item, (Image, Resource)):
                    raise TypeError(
                        f"rich metric {name!r} item {index} must be Image or "
                        f"Resource, got {type(item).__name__}"
                    )
            cdn_batches.append(
                _rich_queue_tuple("cdn_batch", name, step, value, now_ms)
            )
        elif isinstance(value, list):
            for i, v in enumerate(value):
                numeric_points.append(
                    (
                        "numeric_tagged_ts",
                        name,
                        step,
                        _normalize_numeric_value(v),
                        str(i),
                        now_ms,
                    )
                )
        else:
            numeric_points.append(
                ("numeric_ts", name, step, _normalize_numeric_value(value), now_ms)
            )

    # Freeze public rich work before draining captured text: Queue.put() returns
    # before its feeder pickles, so passing the live object could fail later or
    # observe a caller mutation after log() returned. Numeric/text tuples contain
    # primitives and stay on the no-synchronous-snapshot fast path. Rich log()
    # latency is intentionally proportional to the snapshot size.
    rich_queue_items = _snapshot_rich_queue_items(cdn_batches)

    # Drain stdout/stderr buffers (keyed by timestamp, independent of step). Destructive — keep below anything that can raise (bad name, un-floatable value), or a failed log() discards the captured text.
    text_points = _drain_capture_points(now_ms)

    total = len(numeric_points) + len(cdn_batches) + len(text_points)
    if total > 0:
        queue_items = []
        all_points = numeric_points + text_points
        if all_points:
            queue_items.append(all_points)
        queue_items.extend(rich_queue_items)
        _publish_queue_items(queue_items)

    _warn_if_backlogged()
    _last_log_duration_ms = (time.perf_counter() - t0) * 1000.0


def _drain_capture_points(now_ms: int) -> list[tuple]:
    """Drain captured output into the text points used by log and finish."""
    from kymo._capture import drain_buffers

    global _last_capture_step
    with _capture_drain_lock:
        stdout_text, stderr_text = drain_buffers()
        if not stdout_text and not stderr_text:
            return []

        step = (
            now_ms
            if _last_capture_step is None
            else max(now_ms, _last_capture_step + 1)
        )
        _last_capture_step = step

        points = []
        if stdout_text:
            points.append(("text_ts", "logs/std_out", step, stdout_text, now_ms))
        if stderr_text:
            points.append(("text_ts", "logs/std_err", step, stderr_text, now_ms))
        return points


_BACKLOG_WARN_THRESHOLD = 100_000
_BACKLOG_WARN_INTERVAL = 60.0
_STATUS_LOCK_TIMEOUT = 1.0


_SERIALIZED_RICH_QUEUE_ITEM = "__kymo_serialized_rich_v1__"


class _HostTensorPickler(pickle.Pickler):
    """Snapshot pickler that copies accelerator tensors to host memory.

    Torch pickles a device tensor with its device, so the upload worker would unpickle it onto the accelerator: a forked worker cannot initialize CUDA and would drop the item as lost, and one that can opens its own context. Pickling copies the data to host anyway. Everything else, CPU tensors included, pickles exactly as pickle.dumps would.
    """

    def reducer_override(self, obj):
        # Never imports torch: a tensor can only exist once it is loaded.
        # A stub or half-imported torch has no Tensor type; pickle normally then.
        tensor = getattr(sys.modules.get("torch"), "Tensor", None)
        if (
            isinstance(tensor, type)
            and isinstance(obj, tensor)
            # A meta tensor has no data to copy; it pickles as before and the worker drops it.
            and obj.device.type not in ("cpu", "meta")
        ):
            return obj.detach().cpu().__reduce_ex__(pickle.HIGHEST_PROTOCOL)
        return NotImplemented


def _snapshot_rich_queue_items(cdn_batches: list[tuple]) -> list[tuple]:
    """Return queue-safe immutable snapshots of public rich metric batches."""
    snapshots = []
    for batch in cdn_batches:
        if batch[0] in ("metadata_batch", "metadata_batch_mutation"):
            try:
                manifest = metadata_manifest(batch[3].data)
            except (TypeError, ValueError) as error:
                raise ValueError(
                    f"metadata metric {batch[1]!r} must contain finite "
                    "JSON-serializable values"
                ) from error
            # Freeze metadata to its rendered JSON, as _snapshot_config does, so the payload never carries tensors or other objects the worker might fail to unpickle.
            batch = (*batch[:3], Metadata(json.loads(manifest)["data"]), *batch[4:])
        try:
            # Standard pickle copies array/tensor storage into this byte string;
            # multiprocessing reducers may instead publish shared storage whose
            # later mutation would violate log()'s snapshot semantics.
            buffer = io.BytesIO()
            _HostTensorPickler(buffer, protocol=pickle.HIGHEST_PROTOCOL).dump([batch])
            payload = buffer.getvalue()
        except Exception as error:
            raise TypeError(
                f"rich metric {batch[1]!r} cannot be sent to the upload worker: "
                "its payload is not multiprocessing-serializable"
            ) from error
        snapshots.append((_SERIALIZED_RICH_QUEUE_ITEM, 1, payload))
    return snapshots


def _rich_queue_tuple(
    kind: str, name: str, step: int, payload, timestamp_ms: int
) -> tuple:
    versions = _reserve_rich_mutation_versions(2 if kind == "cdn_batch" else 1)
    if versions is None:
        return (kind, name, step, payload)
    mutation_version = versions[0]
    record = (
        f"{kind}_mutation",
        name,
        step,
        payload,
        timestamp_ms,
        mutation_version,
    )
    if kind != "cdn_batch":
        return record
    # A replay may have to omit a child that the CDN now rejects permanently; reserve the successor before any later logical mutation so the reduced rebuild stays in this writer's causal order.
    return (*record, versions[1])


def _queue_item_size(item) -> int:
    if (
        isinstance(item, tuple)
        and len(item) == 3
        and item[0] == _SERIALIZED_RICH_QUEUE_ITEM
    ):
        return item[1]
    return len(item)


def _decode_queue_item(item):
    """Decode a rich snapshot; accept legacy/raw queue groups unchanged."""
    if not (
        isinstance(item, tuple)
        and len(item) == 3
        and item[0] == _SERIALIZED_RICH_QUEUE_ITEM
    ):
        return item
    _, expected, payload = item
    decoded = pickle.loads(payload)
    if not isinstance(decoded, list) or len(decoded) != expected:
        raise ValueError("invalid serialized rich queue item")
    return decoded


def _raw_shared_value(shared, default=0):
    if shared is None:
        return default
    raw = shared.get_obj() if hasattr(shared, "get_obj") else shared
    return raw.value


def _change_shared_value(shared, delta: int, *, timeout: float) -> None:
    """Change a synchronized value without waiting forever on a dead owner."""
    lock = shared.get_lock()
    if not lock.acquire(timeout=max(0.0, timeout)):
        raise RuntimeError("kymo upload accounting lock is unavailable")
    try:
        raw = shared.get_obj() if hasattr(shared, "get_obj") else shared
        raw.value += delta
    finally:
        lock.release()


def _delivery_status_snapshot(timeout: float) -> tuple[int, int, int] | None:
    """Read backlog/failure/spool state under the accounting lock."""
    if _queue_status is None:
        return 0, 0, 0
    lock = _queue_status.get_lock()
    if not lock.acquire(timeout=max(0.0, timeout)):
        return None
    try:
        remaining = int(_raw_shared_value(_queue_status))
        failure = int(_raw_shared_value(_upload_failure))
        spooled = int(_raw_shared_value(_upload_spooled))
        return remaining, failure, spooled
    finally:
        lock.release()


def _publish_queue_items(items: list, *, hard_deadline: Optional[float] = None) -> None:
    """Publish queue groups with exact producer-side backlog accounting.

    Status must increase before publication because the worker can consume a
    group immediately.  If a later put fails synchronously, retain ownership of
    groups whose puts returned and roll back only the unpublished suffix.
    """
    total = sum(_queue_item_size(item) for item in items)
    if total == 0:
        return

    def lock_timeout() -> float:
        if hard_deadline is None:
            return _STATUS_LOCK_TIMEOUT
        return min(_STATUS_LOCK_TIMEOUT, _remaining_seconds(hard_deadline))

    _change_shared_value(_queue_status, total, timeout=lock_timeout())
    published = 0
    try:
        for item in items:
            _metric_queue.put(item)
            published += _queue_item_size(item)
    except BaseException:
        try:
            _change_shared_value(
                _queue_status,
                -(total - published),
                timeout=lock_timeout(),
            )
        except Exception as accounting_error:
            _log.error("failed to roll back upload accounting: %s", accounting_error)
        raise


def _warn_if_backlogged() -> None:
    """Warn (throttled) when the upload backlog grows, so a struggling server
    is visible during the run rather than at the end-of-run flush."""
    global _last_backlog_warn
    # This warning is advisory; a racy raw read is preferable to letting a
    # worker killed inside an accounting update freeze the logging thread.
    try:
        backlog = int(_raw_shared_value(_queue_status))
    except Exception:
        return
    if backlog < _BACKLOG_WARN_THRESHOLD:
        return
    now = time.monotonic()
    if now - _last_backlog_warn < _BACKLOG_WARN_INTERVAL:
        return
    _last_backlog_warn = now
    # Don't blame the server (post-pipelining a backlog means it's down and the
    # worker is retrying, or the run exceeds the upload ceiling) — state the fact,
    # leave diagnosis to the surrounding logs.
    _log.warning(
        "upload backlog: %d points queued. Undelivered points spool to disk at "
        "shutdown (replayable via `python -m kymo.sync`).",
        backlog,
    )


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
        mutation_version = _next_rich_mutation_version()
        point = (
            ("cdn_ts", name, step, key, now_ms)
            if mutation_version is None
            else ("cdn_key_mutation", name, step, key, now_ms, mutation_version)
        )
        points.append(point)

    _publish_queue_items([points])
    _warn_if_backlogged()


def _normalize_timeout_budget(value: float, name: str) -> float:
    value = float(value)
    if not math.isfinite(value) or value < 0:
        _log.warning("invalid %s timeout %r; using a zero-second budget", name, value)
        return 0.0
    return value


def wait_for_upload(timeout: Optional[float] = None) -> bool:
    """Block until all queued metrics are uploaded.

    Returns True only if everything reached the server. Returns False on a
    timeout or once any point has failed over to the replayable disk spool.

    Raises:
        RuntimeError: if the upload worker has died with points still queued,
            or if it could not spool an undeliverable point. In either case
            successful delivery can no longer be reported.
    """
    if not _is_initialized:
        return True
    if timeout is not None:
        timeout = _normalize_timeout_budget(timeout, "upload")
    deadline = None if timeout is None else time.monotonic() + timeout
    wait_time = 0.05
    accounting_unavailable_since = None

    while True:
        # The worker publishes a spill failure before its matching backlog
        # decrement under this same lock.  Read the pair atomically so zero can
        # never win the race against a not-yet-visible eventual exit code.
        lock_wait = 0.1
        if deadline is not None:
            lock_wait = min(lock_wait, max(0.0, deadline - time.monotonic()))
        snapshot = _delivery_status_snapshot(lock_wait)
        if snapshot is None:
            now = time.monotonic()
            if accounting_unavailable_since is None:
                accounting_unavailable_since = now
            if _upload_process is not None and not _upload_process.is_alive():
                remaining = int(_raw_shared_value(_queue_status, -1))
                raise RuntimeError(
                    "kymo upload worker died while updating delivery "
                    f"accounting (raw backlog {remaining})"
                )
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
        if failure:
            if failure == _WORKER_EXIT_RUN_DELETED:
                raise RuntimeError(
                    "kymo run is deleted or no longer exists; queued points "
                    "were rejected and will not be retried or spooled"
                )
            if failure == _WORKER_EXIT_DATA_LOSS:
                raise RuntimeError(
                    "kymo server rejected a rich-media update with DATA_LOSS; "
                    "its spool segment was quarantined as *.rejected (the "
                    "worker log names the file and reason); later data remains "
                    "eligible for delivery"
                )
            raise RuntimeError(
                "kymo upload worker could not spool one or more undelivered "
                "points; delivery failed (disk full, spool unavailable, or a "
                "queued item the worker could not decode; see its log)"
            )
        if spooled:
            _log.warning(
                "one or more points were spooled for later replay instead of uploaded"
            )
            return False
        if remaining <= 0:
            return True
        # A dead worker can't drain the queue; sleeping on it is a silent
        # forever-hang (the way a missing __main__ guard on macOS used to
        # present). Fail loudly with the count instead.
        if _upload_process is not None and not _upload_process.is_alive():
            raise RuntimeError(
                f"kymo upload worker died with {remaining} points still queued "
                f"(exitcode {_upload_process.exitcode}). If it crashed at startup "
                "on macOS, your script is likely missing an `if __name__ == "
                '"__main__":` guard.'
            )
        now = time.monotonic()
        if deadline is not None and now >= deadline:
            _log.warning("timeout — %d points still queued", remaining)
            return False
        sleep_for = min(wait_time, 1.0)
        if deadline is not None:
            sleep_for = min(sleep_for, max(0.0, deadline - now))
        time.sleep(sleep_for)
        wait_time *= 1.5


def finish(flush_timeout: Optional[float] = None) -> bool:
    """Flush and shut down against one ``flush_timeout`` deadline (default
    ``$KYMO_FLUSH_TIMEOUT`` or 60), spool the rest to disk, and shut down.

    The recommended end-of-run call: unlike ``wait_for_upload``, every client
    wait shares that deadline. A kernel-level filesystem or Queue operation can
    still outlive a Python timeout; parent salvage therefore runs in a daemon
    thread and an unquiesced system-metrics publisher makes the result false.
    Spooled points are delivered later from any CPU-only machine with
    ``python -m kymo.sync``.

    Returns True only if delivery was proven complete. Returns False if points
    were spooled or the worker otherwise could not prove delivery.
    """
    reject_legacy_client_env()
    if not _is_initialized:
        return True
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


def _collect_run_metadata(config: Optional[dict]) -> dict:
    """Collect system info, git, SLURM env, and user config into a structured dict."""
    import datetime
    import getpass
    import os
    import platform
    import socket
    import subprocess
    import sys

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
    try:
        git["remote"] = _strip_url_userinfo(
            subprocess.check_output(
                ["git", "-C", repo_dir, "remote", "get-url", "origin"],
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
    except Exception:
        pass
    try:
        git["commit"] = (
            subprocess.check_output(
                ["git", "-C", repo_dir, "rev-parse", "HEAD"], stderr=subprocess.DEVNULL
            )
            .decode()
            .strip()
        )
    except Exception:
        pass
    if git:
        meta["git"] = git

    # SLURM env vars
    import os as _os

    # SLURM_JWT is a bearer token used by Slurm's REST/JWT authentication, not scheduler context. Persisting it in info/run_info would expose the credential anywhere run metadata is viewed or downloaded.
    slurm = {
        k: v
        for k, v in _os.environ.items()
        if k.startswith("SLURM") and k != "SLURM_JWT"
    }
    if slurm:
        meta["slurm"] = slurm

    result = {"meta": meta}
    if config:
        result["config"] = config
    return result


def _upload_run_metadata(config: Optional[dict]):
    """Upload run metadata + config to CDN and log as info/run_info metric.

    Stores the resulting blob in ``_run_metadata`` so subsequent
    ``update_config`` calls can mutate just the config field without
    re-collecting time/system/git fields (which could otherwise drift
    between uploads)."""
    global _run_metadata
    if not _is_initialized:
        return

    _run_metadata = _collect_run_metadata(config)
    _push_run_metadata()


def _push_run_metadata():
    """Queue one immutable metadata snapshot on the ordered rich lane."""
    if not _is_initialized or _run_metadata is None:
        return
    _publish_queue_items(
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
# Signal / exit handling (guarantees delivery)
# ---------------------------------------------------------------------------


def _termination_signals(signal_module=signal) -> tuple:
    return tuple(
        sig
        for name in ("SIGTERM", "SIGINT", "SIGQUIT")
        if (sig := getattr(signal_module, name, None)) is not None
    )


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
    global _original_signals
    for sig in _termination_signals():
        try:
            original = signal.getsignal(sig)
            _original_signals[sig] = original
            if original is signal.SIG_IGN:
                continue
            signal.signal(sig, _signal_handler)
        except (ValueError, OSError):
            pass


def _record_exit_code(code: int) -> None:
    """First writer wins — a signal handler shouldn't be overridden by a
    later sys.exit cascade fired during cleanup."""
    global _exit_code
    if _exit_code is None:
        _exit_code = int(code)


def _excepthook(exc_type, exc_value, exc_tb):
    # SystemExit is captured by wrapped sys.exit/direct signal delegation below,
    # so don't double-set. KeyboardInterrupt has the conventional shell code;
    # other uncaught exceptions exit the interpreter with status 1.
    if not issubclass(exc_type, SystemExit):
        _record_exit_code(130 if issubclass(exc_type, KeyboardInterrupt) else 1)
    if _original_excepthook is not None:
        _original_excepthook(exc_type, exc_value, exc_tb)


def _exit_wrapper(code=0):
    _record_exit_code(_system_exit_code(code))
    _original_sys_exit(code)


def _system_exit_code(code) -> int:
    if code is None:
        return 0
    if isinstance(code, int):
        return code
    # sys.exit("message") — Python uses status 1 for non-int args.
    return 1


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
    original = _original_signals.get(signum)
    if callable(original) and original not in (signal.SIG_IGN, signal.SIG_DFL):
        try:
            original(signum, frame)
        except SystemExit as error:
            _record_exit_code(_system_exit_code(error.code))
            raise
        return

    _record_exit_code(128 + signum)
    _log.warning("signal %d — flushing metrics before exit…", signum)
    try:
        reject_legacy_client_env()
    except ValueError as error:
        _log.error("%s; using the fixed emergency signal flush budget", error)
        signal_budget = _SIGNAL_FLUSH_TIMEOUT
    else:
        try:
            signal_budget = _env_number(
                "KYMO_SIGNAL_FLUSH_TIMEOUT",
                "MKDB2_SIGNAL_FLUSH_TIMEOUT",
                _SIGNAL_FLUSH_TIMEOUT,
            )
        except ValueError:
            signal_budget = _SIGNAL_FLUSH_TIMEOUT
    try:
        _drain_and_shutdown(flush_timeout=signal_budget)
    finally:
        sys.exit(128 + signum)


def _ensure_metrics_uploaded():
    global _is_initialized
    if not _is_initialized:
        return
    _log.info("shutting down — flushing remaining metrics…")
    try:
        reject_legacy_client_env()
    except ValueError as error:
        _log.error("%s; using the fixed emergency exit flush budget", error)
        _drain_and_shutdown(flush_timeout=_DEFAULT_FLUSH_TIMEOUT)
    else:
        _drain_and_shutdown()


def _send_terminate(
    server_address: str,
    project_id: str,
    run_id: str,
    exit_code: int,
    *,
    timeout: float = 5.0,
    local_installation_uuid: str = "",
) -> None:
    """Best-effort terminal status delivery. Failures are logged but never
    raised — we don't want a server hiccup to crash the user's exit path."""
    try:
        started = time.monotonic()
        endpoint = None
        if local_installation_uuid:
            from kymo._local_runtime import ensure_local_endpoint

            endpoint = ensure_local_endpoint(
                expected_installation_uuid=local_installation_uuid,
                timeout=timeout,
            )
        rpc_timeout = max(0.001, timeout - (time.monotonic() - started))
        _run_control_rpc(
            "TerminateRun",
            kymo_pb2.TerminateRunRequest(
                project_id=project_id,
                run_id=run_id,
                exit_code=exit_code,
            ),
            kymo_pb2.TerminateRunResponse,
            server_address=server_address,
            timeout=min(5.0, rpc_timeout),
            local_endpoint=endpoint,
        )
    except Exception as e:
        _log.warning("failed to send TerminateRun: %s", e)


# Maximum share of the ONE finish() budget reserved for the worker to spill its
# remainder (pickling a few million tuples + encoding pending images).
_SPILL_GRACE = 30.0
# Keep a distinct tail for SIGTERM/SIGKILL and owner-fenced parent salvage when
# the worker crashes. Twenty percent is material under Slurm's 10-second path;
# the cap leaves most of a normal 60-second finish for delivery/worker spill.
_CLEANUP_RESERVE_FRACTION = 0.2
_CLEANUP_RESERVE_MIN = 0.5
_CLEANUP_RESERVE_MAX = 10.0
_PROCESS_STOP_GRACE = 1.0
_TERMINATE_GRACE = 0.1
# Spool reporting/retirement runs after the worker window, on a mount that can
# stall. The kill/salvage steps end this much before the hard deadline (or half
# the forced-cleanup tail, whichever is smaller) so the last phase has room
# inside the same budget rather than overrunning it.
_SPOOL_CLEANUP_RESERVE = 0.5
# Quiet-time fallback for an orphaned worker whose parent can no longer publish
# the original shutdown sentinel. Owner salvage waits at most _SPILL_GRACE and
# is also bounded by finish()'s shared hard deadline.
_QUEUE_DRAIN_SILENCE = 2.0
_DEFAULT_FLUSH_TIMEOUT = 60.0
# Shorter budget on signals: slurm follows SIGTERM with SIGKILL after KillWait
# (often 30s) — leave room for the spill itself.
_SIGNAL_FLUSH_TIMEOUT = 10.0


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


def _read_shutdown_status(status) -> Optional[int]:
    """Read shared accounting without its possibly poisoned process lock.

    SIGKILL can strand ``multiprocessing.Value``'s SemLock if the worker dies
    inside an accounting update. The worker is the only concurrent decrementer,
    so a racy raw snapshot is sufficient for shutdown decisions; an unreadable
    snapshot can never support a successful-delivery claim.
    """
    if status is None:
        return 0
    try:
        raw = status.get_obj() if hasattr(status, "get_obj") else status
        return int(raw.value)
    except Exception as error:
        _log.error("shutdown accounting could not be read: %s", error)
        return None


def _set_shutdown_deadline(deadline, value: float) -> None:
    """Publish the worker deadline without waiting on a shared-value lock."""
    try:
        raw = deadline.get_obj() if hasattr(deadline, "get_obj") else deadline
        raw.value = value
    except Exception as error:
        # The parent's process deadline still force-stops the worker.
        _log.error("worker shutdown deadline could not be published: %s", error)


def _shutdown_budget_split(remaining: float) -> tuple[float, float, float]:
    """Return delivery, worker-spill, and forced-cleanup budgets."""
    cleanup = min(
        remaining,
        _CLEANUP_RESERVE_MAX,
        max(_CLEANUP_RESERVE_MIN, remaining * _CLEANUP_RESERVE_FRACTION),
    )
    worker = max(0.0, remaining - cleanup)
    spill = min(_SPILL_GRACE, worker / 2)
    return worker - spill, spill, cleanup


def _drain_queue_until(
    source,
    consume,
    is_fence,
    *,
    timeout: float,
    extend_on_item: bool,
    hard_deadline: Optional[float] = None,
) -> bool:
    """Consume queue items until a producer-owned fence or a bounded timeout."""
    deadline = time.monotonic() + timeout
    if hard_deadline is not None:
        deadline = min(deadline, hard_deadline)
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        try:
            item = source.get(timeout=min(0.25, remaining))
        except queue.Empty:
            continue
        except (OSError, ValueError) as error:
            _log.warning("upload queue drain failed: %s", error)
            return False
        if extend_on_item:
            deadline = time.monotonic() + timeout
            if hard_deadline is not None:
                deadline = min(deadline, hard_deadline)
        if is_fence(item):
            return True
        consume(item)


def _drain_worker_queue(metric_queue, consume, *, input_closed: bool) -> bool:
    """Drain through the parent's shutdown sentinel without publishing from the worker."""
    if input_closed:
        return True
    return _drain_queue_until(
        metric_queue,
        consume,
        lambda item: item is None,
        timeout=_QUEUE_DRAIN_SILENCE,
        extend_on_item=True,
    )


def _drain_queue_to_spool(
    spool: SpoolWriter,
    *,
    source=None,
    hard_deadline: Optional[float] = None,
) -> int:
    """Parent-side salvage after the worker died, through an owner FIFO fence."""
    if _init_pid is None or os.getpid() != _init_pid:
        raise RuntimeError("only the initializing process may fence the upload queue")
    if source is None:
        source = _metric_queue

    # Queue ordering is producer-local, so only the initializing process may publish this marker; a worker marker could overtake objects still being serialized by the parent's feeder.
    fence = ("__kymo_owner_drain__", uuid.uuid4().hex)
    try:
        source.put(fence)
    except (OSError, ValueError) as error:
        _log.warning("failed to publish the upload queue salvage fence: %s", error)
        return 0

    salvaged = 0

    def consume(item) -> None:
        nonlocal salvaged
        if item is None:
            return
        try:
            point_tuples = _decode_queue_item(item)
        except Exception as error:
            _log.warning("failed to decode a rich queue item during salvage: %s", error)
            return
        for point_tuple in point_tuples:
            try:
                salvaged += _spill_tuple(spool, point_tuple)
            except Exception as e:
                _log.warning("failed to spool a point during salvage: %s", e)

    complete = _drain_queue_until(
        source,
        consume,
        lambda item: item == fence,
        timeout=_SPILL_GRACE,
        extend_on_item=False,
        hard_deadline=hard_deadline,
    )
    if not complete:
        _log.warning("upload queue salvage ended before its owner fence")
    return salvaged


def _report_spool(paths: list[str]) -> bool:
    # The session-scoped listdir is the delivery evidence. Restatting here would
    # make a vanished name ambiguous: sync may have delivered it as .sent, but it
    # may instead have quarantined/retired it or an operator may have removed it.
    # Conservatively report the listed path rather than infer delivery.
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
    itself, so the pre-created ``_spool_path`` is not a complete inventory. Every
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
                    "lifecycle-rejected spool is still open by its writer; "
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


def _resolve_run_spools(*, terminal_rejected: bool, hard_deadline: float) -> bool:
    """Run spool cleanup off the caller's thread, like the parent's own salvage.

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
    thread.join(timeout=_remaining_seconds(hard_deadline))
    if thread.is_alive():
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
    process: multiprocessing.Process,
    *,
    graceful_deadline: float,
    stop_deadline: float,
) -> tuple[bool, bool]:
    """Stop one worker within the shared shutdown budget.

    Returns ``(dead, clean_exit)``.  Salvage is safe only when ``dead`` is true;
    SIGTERM is catchable, so a bounded SIGKILL fallback is mandatory.
    """
    if process.is_alive():
        process.join(timeout=_remaining_seconds(graceful_deadline))
    if process.is_alive():
        _log.warning("worker did not exit within its deadline, terminating")
        try:
            process.terminate()
        except Exception as error:
            _log.warning("failed to terminate upload worker: %s", error)
        process.join(
            timeout=min(
                _TERMINATE_GRACE,
                _remaining_seconds(stop_deadline) / 2,
            )
        )
    if process.is_alive():
        _log.warning("upload worker ignored SIGTERM, killing")
        try:
            process.kill()
        except Exception as error:
            _log.warning("failed to kill upload worker: %s", error)
        process.join(timeout=_remaining_seconds(stop_deadline))

    dead = not process.is_alive()
    return dead, dead and process.exitcode == 0


def _shutdown_succeeded(
    *,
    worker_clean: bool,
    poller_quiesced: bool,
    had_spool: bool,
    remaining: Optional[int],
    spooled: bool = False,
) -> bool:
    """The complete-delivery claim requires every accounting proof."""
    return (
        worker_clean
        and poller_quiesced
        and not had_spool
        and not spooled
        and remaining == 0
    )


def _salvage_queue_before(hard_deadline: float) -> tuple[str, bool]:
    """Run potentially blocking encode/write/fsync work outside finish's thread."""
    # Capture the old queue before starting the daemon. A deadline-expired
    # salvage may overlap a later init(), which replaces the module global.
    source = _metric_queue
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
    )
    outcome = {"path": "", "salvaged": 0, "error": None}

    def salvage() -> None:
        try:
            outcome["salvaged"] = _drain_queue_to_spool(
                spool,
                source=source,
                hard_deadline=hard_deadline,
            )
        except BaseException as error:
            outcome["error"] = error
        finally:
            try:
                outcome["path"] = spool.close() or ""
            except BaseException as error:
                if outcome["error"] is None:
                    outcome["error"] = error

    thread = threading.Thread(
        target=salvage,
        name="kymo-parent-salvage",
        daemon=True,
    )
    thread.start()
    thread.join(timeout=_remaining_seconds(hard_deadline))
    if thread.is_alive():
        _log.error(
            "parent queue salvage exceeded the finish deadline; it may continue "
            "in the background at %s",
            spool.path,
        )
        return "", False
    if outcome["error"] is not None:
        _log.error("parent queue salvage failed: %s", outcome["error"])
        return outcome["path"], True
    if outcome["salvaged"]:
        _log.warning(
            "salvaged %d undelivered points from the queue", outcome["salvaged"]
        )
    return outcome["path"], True


def _drain_and_shutdown(flush_timeout: Optional[float] = None) -> bool:
    global \
        _is_initialized, \
        _upload_process, \
        _metric_queue, \
        _queue_status, \
        _system_poller, \
        _exit_code, \
        _run_metadata, \
        _shutdown_deadline, \
        _spool_path, \
        _mode, \
        _local_installation_uuid, \
        _url_base, \
        _rich_writer_epoch, \
        _rich_mutation_seq

    if not _is_initialized:
        return True
    # Raw os.fork() children (NOT multiprocessing children — those skip atexit) inherit this module's state and hooks; see _init_pid. Only the initializing process may shut the shared worker down.
    if _init_pid is not None and os.getpid() != _init_pid:
        return True

    if flush_timeout is None:
        flush_timeout = _default_flush_timeout()
    flush_timeout = _normalize_timeout_budget(flush_timeout, "flush")
    hard_deadline = time.monotonic() + flush_timeout

    poller_quiesced = True
    if _system_poller is not None:
        poller_quiesced = _system_poller.stop(timeout=_remaining_seconds(hard_deadline))
        _system_poller = None

    backlog = _read_shutdown_status(_queue_status)
    if backlog is not None and backlog > 0:
        _log.info("flushing %d queued points (budget %.0fs)…", backlog, flush_timeout)

    capture_tail_published = True
    capture_points = _drain_capture_points(int(time.time() * 1000))
    if capture_points:
        try:
            _publish_queue_items([capture_points], hard_deadline=hard_deadline)
        except (OSError, RuntimeError, ValueError) as error:
            capture_tail_published = False
            _log.warning("failed to publish final captured output: %s", error)

    # Divide the remaining worker window roughly 50/50 between delivery and its
    # own spill, capped at 30 seconds. A separate cleanup tail remains after the
    # worker deadline for SIGKILL and owner-fenced salvage after a crash.
    remaining_budget = _remaining_seconds(hard_deadline)
    send_budget, spill_reserve, cleanup_reserve = _shutdown_budget_split(
        remaining_budget
    )
    # Spool reporting/retirement is the last phase and also touches the shared
    # mount, so it needs a slice of that same forced-cleanup tail — at most half,
    # taken from the kill/salvage steps that could otherwise consume all of it.
    # Never from the worker's own delivery+spill window: on a 2s budget that
    # window is what decides how many points reach disk at all.
    pre_cleanup_deadline = hard_deadline - min(
        _SPOOL_CLEANUP_RESERVE, cleanup_reserve / 2
    )
    graceful_deadline = min(
        hard_deadline - cleanup_reserve,
        time.monotonic() + send_budget + spill_reserve,
    )
    if _shutdown_deadline is not None:
        _set_shutdown_deadline(_shutdown_deadline, time.monotonic() + send_budget)
    shutdown_marker_published = False
    if _metric_queue:
        try:
            _metric_queue.put(None)
            shutdown_marker_published = True
        except (OSError, ValueError) as error:
            _log.warning("failed to publish upload shutdown marker: %s", error)

    worker_dead = _upload_process is None
    worker_clean = False
    if _upload_process:
        stop_deadline = min(
            pre_cleanup_deadline,
            graceful_deadline + min(_PROCESS_STOP_GRACE, cleanup_reserve / 2),
        )
        worker_dead, worker_clean = _stop_upload_process(
            _upload_process,
            graceful_deadline=graceful_deadline,
            stop_deadline=stop_deadline,
        )

    salvage_finished = True
    current_backlog = _read_shutdown_status(_queue_status)
    terminal_rejected = _read_shutdown_status(_upload_terminal) not in (None, 0)
    needs_salvage = not worker_clean or current_backlog != 0
    if terminal_rejected:
        # The worker can be force-killed while draining a very large producer
        # queue after a lifecycle rejection. Those remaining points are known
        # to be permanently inadmissible, so deliberately drop them instead of
        # turning them into an immortal replay queue.
        if needs_salvage:
            _log.error(
                "run is deleted or no longer exists; discarding %s queued "
                "point(s) instead of creating a replayable salvage spool",
                "an unknown number of"
                if current_backlog is None
                else str(max(0, current_backlog)),
            )
    elif (
        needs_salvage
        and worker_dead
        and _metric_queue is not None
        and _remaining_seconds(pre_cleanup_deadline) > 0
    ):
        # Worker was killed or crashed; the owner's fresh unique fence remains ordered after every earlier owner publication, even if its feeder is still pickling one.
        # The returned path is unused: the spool scan below enumerates this file too.
        _salvage_path, salvage_finished = _salvage_queue_before(pre_cleanup_deadline)
    elif needs_salvage and not worker_dead:
        _log.error(
            "upload worker is still alive after SIGKILL; skipping unsafe salvage"
        )

    if _metric_queue is not None:
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
        terminal_rejected=terminal_rejected, hard_deadline=hard_deadline
    )

    final_backlog = _read_shutdown_status(_queue_status)
    final_spooled = _read_shutdown_status(_upload_spooled)
    if final_backlog is None:
        _log.error("shutdown accounting is unavailable")
    elif final_backlog != 0:
        _log.error(
            "shutdown accounting incomplete: %d point(s) remain unacknowledged",
            final_backlog,
        )

    # Send terminal status AFTER worker drains, so the run's last metrics are
    # persisted before it flips to CRASHED/FINISHED on the dashboard.
    final_exit = _exit_code if _exit_code is not None else 0
    terminate_budget = _remaining_seconds(hard_deadline)
    # A worker that observed a terminal lifecycle rejection belongs to the
    # deleted generation. If the run is restored before this old process
    # exits, its stale TerminateRun must not mark the restored run finished.
    if (
        not terminal_rejected
        and (_server_address or _local_installation_uuid)
        and _project_id
        and _run_id
        and terminate_budget > 0
    ):
        _send_terminate(
            _server_address,
            _project_id,
            _run_id,
            final_exit,
            timeout=terminate_budget,
            local_installation_uuid=_local_installation_uuid,
        )

    _is_initialized = False
    _exit_code = None  # reset so a subsequent init() starts fresh
    _run_metadata = None
    _spool_path = ""
    _mode = "hosted"
    _local_installation_uuid = ""
    _url_base = ""
    _rich_writer_epoch = None
    _rich_mutation_seq = None
    # A terminated worker takes its private in-RAM buffers with it, so queue salvage alone cannot prove complete delivery.
    return _shutdown_succeeded(
        worker_clean=(
            worker_clean and shutdown_marker_published and capture_tail_published
        ),
        poller_quiesced=poller_quiesced,
        had_spool=had_spool,
        remaining=final_backlog,
        spooled=final_spooled is None or final_spooled != 0,
    )


# ---------------------------------------------------------------------------
# Background worker
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# CDN helpers (run in worker process)
# ---------------------------------------------------------------------------


def _encode_image(img: "Image") -> tuple[bytes, str]:
    """Encode an Image to bytes. Returns (encoded_bytes, extension)."""
    data = img.data
    fmt = img.format.lower()
    from PIL import Image as PILImage

    # Raw bytes stay byte-identical. The default format is only a fallback for
    # array/PIL encoding, so detect the actual container when bytes use it;
    # preserve non-default overrides for formats Pillow cannot identify.
    if isinstance(data, bytes):
        if fmt == "png":
            with PILImage.open(io.BytesIO(data)) as encoded:
                if encoded.format is None:
                    raise ValueError("encoded image format could not be detected")
                fmt = encoded.format.lower()
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
        # Handle single channel → RGB
        if data.ndim == 2:
            pil_img = PILImage.fromarray(data, mode="L")
        elif data.ndim == 3 and data.shape[2] in (1, 3, 4):
            # HWC is the documented form. Prefer it when both ends look like
            # channel axes (for example a three-pixel-tall RGB image).
            if data.shape[2] == 1:
                data = data[:, :, 0]
                pil_img = PILImage.fromarray(data, mode="L")
            else:
                pil_img = PILImage.fromarray(data)
        elif data.ndim == 3 and data.shape[0] in (1, 3, 4):
            # CHW → HWC
            data = np.transpose(data, (1, 2, 0))
            if data.shape[2] == 1:
                data = data[:, :, 0]
                pil_img = PILImage.fromarray(data, mode="L")
            else:
                pil_img = PILImage.fromarray(data)
        else:
            pil_img = PILImage.fromarray(data)

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
    if ext not in _RESOURCE_EXTENSIONS:
        ext = "bin"
    return res.data, ext


def _send_unary_point(stub, project_id: str, run_id: str, point) -> bool:
    batch = kymo_pb2.MetricsBatch(project_id=project_id, run_id=run_id, points=[point])
    try:
        response = stub.IngestMetrics(iter([batch]), timeout=_CDN_RPC_TIMEOUT)
    except grpc.RpcError as error:
        if _is_terminal_run_error(error):
            raise _terminal_run_error(error) from error
        raise
    if response.points_received != 1:
        _log.warning("server accepted %d of 1 point", response.points_received)
        return False
    return True


def _process_cdn_batch(
    stub,
    http_client,
    cdn_url: str,
    project_id: str,
    run_id: str,
    metric_name: str,
    step: int,
    items: list,
    deadline_fn=None,
    send_placeholder: bool = True,
    timestamp_ms: Optional[int] = None,
    mutation_version: Optional[int] = None,
    reduced_mutation_version: Optional[int] = None,
) -> bool:
    """Upload CDN items, create manifest, log to gRPC. Returns True on success
    (the batch is fully dealt with) — False means RETRYABLE: the caller keeps
    the batch queued, and re-running this whole function is safe (manifest
    re-sends replace themselves, re-uploads dedup by content hash) PROVIDED
    the legacy retry passes ``send_placeholder=False``: those rows use
    server-side arrival order, so a retry's placeholder could overwrite a real
    point whose commit succeeded but whose ack was lost. Versioned writers do
    not publish placeholders. Aborts (returns False) as soon as
    ``deadline_fn`` fires, so a shutdown can reroute the batch to the spool
    instead of finishing slow uploads."""
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
        except _TerminalRunError:
            raise

    # 2. Encode and upload ONE ITEM AT A TIME — peak memory is the raw gallery plus a single encoded image (an encode-all-then-upload split holds every encoded byte simultaneously, an OOM risk for giant galleries). The error classes stay distinct: encode failures (bad array shape, unsupported type) are PERMANENT — a retry can never fix them, so the item is dropped with a log and the rest of the batch survives; upload failures are transient (network/server) and fail the whole batch for retry — committing a manifest missing the failed items would silently thin the gallery. A retry re-encodes the already-uploaded prefix (the CDN dedups by content hash). The deadline check covers the encodes too: past it the caller spools the batch anyway (re-encoding there), so finishing encodes here is pure shutdown latency against the parent's kill grace.
    manifest_items = []
    manifest_changed = False
    for item in items:
        data = None
        try:
            if deadline_fn is not None and deadline_fn():
                return False
            try:
                if isinstance(item, Image):
                    data, ext = _encode_image(item)
                    entry_args = {
                        "extension": ext,
                        "caption": item.caption,
                    }
                elif isinstance(item, Resource):
                    data, ext = _encode_resource(item)
                    entry_args = {
                        "content_type": item.content_type,
                        "filename": item.filename,
                    }
                else:
                    continue
            except MemoryError:
                raise
            except Exception as e:
                manifest_changed = True
                _log.warning(
                    "failed to encode CDN item (%s step %d) — dropping it: %s",
                    metric_name,
                    step,
                    e,
                )
                continue
            try:
                resource_id = _upload_to_cdn(http_client, cdn_url, data, ext)
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
            manifest_items.append(gallery_item(resource_id, **entry_args))
        finally:
            # Release this potentially multi-MiB payload on every path before
            # encoding the next item or building/uploading the manifest.
            data = None

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
            if reduced_mutation_version is None:
                raise _RichMutationDataLoss(
                    "reduced gallery has no causally reserved mutation identity"
                )
            mutation_version = reduced_mutation_version
        try:
            if not _publish_rich_mutation(
                stub,
                project_id,
                run_id,
                metric_name,
                step,
                manifest_id,
                timestamp_ms,
                mutation_version,
            ):
                return False
            _log.info(
                "CDN %s step=%d → %s (%d items)",
                metric_name,
                step,
                manifest_id,
                len(manifest_items),
            )
            return True
        except grpc.RpcError as e:
            _log.error("failed to publish rich mutation: %s", e.details())
            return False
        except _TerminalRunError:
            raise

    real_point = kymo_pb2.MetricPoint(
        metric_name=metric_name,
        step=step,
        cdn_key=manifest_id,
        timestamp_ms=int(time.time() * 1000),
    )
    try:
        if not _send_unary_point(stub, project_id, run_id, real_point):
            return False
        _log.info(
            "CDN %s step=%d → %s (%d items)",
            metric_name,
            step,
            manifest_id,
            len(manifest_items),
        )
        return True
    except grpc.RpcError as e:
        _log.error("failed to send real cdn_key: %s", e.details())
        return False
    except _TerminalRunError:
        raise


_CONNECT_INITIAL_DELAY = 0.5
_CONNECT_READY_TIMEOUT = 5.0
_CONNECT_POLL_TIMEOUT = 0.25
# Disk catch-up is a fleet-wide outage path. Its longer, jittered ceiling keeps
# workers that observed the same restart from hammering the replacement pod in
# lockstep. Ordinary in-memory stream retries retain the low-latency 5s cap.
_RECOVERY_MAX_DELAY = 60.0
# Attempts on one CDN batch before it's spooled for kymo.sync (exponential backoff, 2s..60s — a few minutes of outage total). Permanent rejections (server 400s the content) also land here rather than wedging the queue head forever.
_CDN_MAX_ATTEMPTS = 8
# Worker exit code: some points reached neither the server nor the spool (a spool write failed, e.g. disk full, or a queued snapshot would not unpickle in the worker). finish() must not report success.
_WORKER_EXIT_SPOOL_FAILED = 2
# The worker stopped without observing the owner's shutdown marker. The parent
# must run its fenced salvage path before delivery can be claimed.
_WORKER_EXIT_DRAIN_INCOMPLETE = 3
# The identity is deleted/purged. Retrying or spooling would create an
# immortal backlog, so the worker rejects queued points and reports failure.
_WORKER_EXIT_RUN_DELETED = 4
# The server answered DATA_LOSS for a rich mutation: its spool segment was quarantined as *.rejected and later data kept flowing.
_WORKER_EXIT_DATA_LOSS = 5
# Loss evidence outranks other failures, and an outright spool failure outranks a quarantine.
_FAILURE_RANK = {_WORKER_EXIT_DATA_LOSS: 1, _WORKER_EXIT_SPOOL_FAILED: 2}


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
# Bound ordered CDN entries; rich entries may retain raw image arrays.
_MAX_CDN_QUEUE = 1_000
_MAX_CDN_QUEUE_BYTES = 256 * 1024 * 1024
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

    def __bool__(self) -> bool:
        return bool(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def append(self, item: tuple, size: int) -> None:
        self.items.append(item)
        self.sizes.append(size)
        self.total_bytes += size

    def discard_prefix(self, count: int) -> None:
        if count <= 0:
            return
        self.total_bytes -= sum(self.sizes[:count])
        del self.items[:count]
        del self.sizes[:count]

    def clear(self) -> None:
        self.items.clear()
        self.sizes.clear()
        self.total_bytes = 0


class _ConnectionAttempt:
    """One non-blocking channel-readiness attempt owned by the worker loop."""

    def __init__(
        self,
        server_address: str,
        *,
        start: bool = True,
        local_endpoint=None,
    ):
        self._server_address = server_address
        self._channel = None
        self._ready = None
        self._deadline = time.monotonic() + _CONNECT_READY_TIMEOUT
        if not start:
            return
        if local_endpoint is None:
            self._channel = grpc.insecure_channel(server_address)
        else:
            from kymo._local_runtime import grpc_channel

            self._channel = grpc_channel(local_endpoint)
        self._ready = grpc.channel_ready_future(self._channel)

    def poll(self, deadline_fn=None):
        """Return a connected pair, a failed pair, or _CONNECT_PENDING."""
        if self._channel is None:
            return None, None
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
            if self._ready is not None:
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

    The random half prevents a fleet of workers that crossed the RAM cap at
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


def _spill_tuple(spool, point_tuple: tuple) -> int:
    """Write one queue tuple to the spool, pre-encoding CDN payloads so the
    file is self-contained. Returns 1 (queue_status units) always."""
    kind = point_tuple[0]
    if kind in ("metadata_batch", "metadata_batch_mutation"):
        import json

        if kind == "metadata_batch_mutation":
            _, name, step, metadata_obj, timestamp_ms, mutation_version = point_tuple
        else:
            _, name, step, metadata_obj = point_tuple
        data = json.loads(json.dumps(metadata_obj.data, default=str, allow_nan=False))
        if kind == "metadata_batch_mutation":
            spool.write(
                (
                    "metadata_json_mutation",
                    name,
                    step,
                    data,
                    timestamp_ms,
                    mutation_version,
                )
            )
        else:
            spool.write(("metadata_json", name, step, data))
    elif kind in ("cdn_batch", "cdn_batch_mutation"):
        if kind == "cdn_batch_mutation":
            (
                _,
                name,
                step,
                items,
                timestamp_ms,
                mutation_version,
                reduced_mutation_version,
            ) = point_tuple
        else:
            _, name, step, items = point_tuple
        encoded = []
        manifest_changed = False
        for item in items:
            try:
                if isinstance(item, Image):
                    data, ext = _encode_image(item)
                    encoded.append(
                        {
                            "kind": "image",
                            "data": data,
                            "ext": ext,
                            "caption": item.caption,
                        }
                    )
                elif isinstance(item, Resource):
                    data, ext = _encode_resource(item)
                    encoded.append(
                        {
                            "kind": "resource",
                            "data": data,
                            "ext": ext,
                            "content_type": item.content_type,
                            "filename": item.filename,
                        }
                    )
            except MemoryError:
                raise
            except Exception as e:
                manifest_changed = True
                _log.warning(
                    "failed to encode CDN item for spool (%s step %d): %s",
                    name,
                    step,
                    e,
                )
        # Written even when every item failed to encode. Legacy batches may
        # need an empty manifest to clear their pending:* placeholder;
        # versioned batches still represent a real empty-gallery mutation.
        if kind == "cdn_batch_mutation":
            if manifest_changed:
                mutation_version = reduced_mutation_version
                reduced_mutation_version = None
            if reduced_mutation_version is None:
                spool.write(
                    (
                        "cdn_batch_encoded_mutation",
                        name,
                        step,
                        encoded,
                        timestamp_ms,
                        mutation_version,
                    )
                )
            else:
                # A distinct kind makes the extra field forward-safe: older
                # replay tools preserve unknown records instead of rejecting a
                # valid newer tuple as malformed.
                spool.write(
                    (
                        "cdn_batch_encoded_mutation_reserved",
                        name,
                        step,
                        encoded,
                        timestamp_ms,
                        mutation_version,
                        reduced_mutation_version,
                    )
                )
        else:
            spool.write(("cdn_batch_encoded", name, step, encoded))
    else:
        spool.write(point_tuple)
    return 1


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
        try:
            self._feed_q.put_nowait(self._sentinel)
            return True
        except queue.Full:
            return False

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


def _upload_worker(*args, **kwargs):
    """Compatibility shim for tests and callers of this private helper."""
    from kymo._worker import _upload_worker as worker_target

    return worker_target(*args, **kwargs)


def _connect(
    server_address: str,
    deadline_fn=None,
    *,
    local_endpoint=None,
):
    """Start one readiness attempt; the owner loop polls it without blocking."""
    expired = deadline_fn is not None and deadline_fn()
    return _ConnectionAttempt(
        server_address,
        start=not expired,
        local_endpoint=local_endpoint,
    )


def _next_batch(
    project_id: str,
    run_id: str,
    tuples: list[tuple],
    tuple_sizes: Optional[list[int]] = None,
):
    """Build the FIRST count/byte-capped MetricsBatch from `tuples`; return
    (batch, n_consumed) — n < len(tuples) when a text point trips the byte cap, so
    the caller advances by n and re-slices. (None, 0) if empty."""
    for pts, n in _chunk_tuples(tuples, tuple_sizes):
        return (
            kymo_pb2.MetricsBatch(project_id=project_id, run_id=run_id, points=pts),
            n,
        )
    return None, 0
