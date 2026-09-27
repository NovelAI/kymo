#!/usr/bin/env python
"""Replay archived wandb runs (written by export_wandb.py) into kymo.

Dry-run by default: walks the archive and estimates what would be sent
(point counts, media items/bytes, unsupported leftovers) without touching any
server. Pass --execute together with an explicit --server to actually import.

Usage (needs pyarrow and this checkout's kymo client, installed from its
python_client/ directory, whose internals the script uses):
    python import_to_kymo.py --archive A --entity E --projects P  # dry-run
    python import_to_kymo.py --archive A --entity E --all         # everything
    python import_to_kymo.py --archive A --entity E --projects P \
        --execute --server HOST:50051
    ... --execute --server HOST:50051 --limit-runs 1              # pilot

Mapping (wandb -> kymo):
    project "foo bar"        -> project_id "wandb_foo_bar" (archive dir slug)
    run  <id> "name"         -> run_id "wandb_<proj_slug>_<id>", run_name "name"
    history numeric cell     -> numeric_ts point (step=_step, ts=_timestamp)
    history string column    -> text_ts stream under the metric name
    images/separated, image-file -> CDN image_gallery manifest at the step
    table-/plotly-/html-file -> CDN gallery of file resources (download-only)
    histogram                -> skipped (counted; no kymo representation)
    {Min,Max,Sum,Count,...} struct -> expanded to <name>/<field> numeric points
    events parquet           -> system/* metrics, per-GPU columns as tags,
                                step = timestamp_ms (kymo system convention)
    files/output.log         -> text_ts chunks on logs/std_out (the complete log)
    files/console_log.jsonl.gz -> fallback when output.log was never uploaded
                                (crashed runs): wandb's streamed console log,
                                per-line time + stdout/stderr split, chunks on
                                logs/std_out + logs/std_err; capped by wandb
                                for some runs
    config+summary+tags+notes+wandb metadata -> info/run_info metadata manifest,
                                redacted like kymo's own run metadata: git
                                remote credentials and the Slurm JWT dropped
    state                    -> FinalizeImportRun (finished=0, else nonzero)

Data flows over the server's bulk-import lane (ImportRun / ImportMetricsBidi /
FinalizeImportRun, requires KYMO_IMPORT_ENABLED=1 server-side): backdated
created_at/terminated_at, big synchronous inserts with a private admission
budget, no liveness churn. There is no fallback for servers without those
RPCs — a server that returns UNIMPLEMENTED must be updated.

Retry is per run: a failed stream fails only its run (its bookkeeping stays
incomplete) and the next invocation replays that run from the start —
ClickHouse dedups on (project,run,metric,tag,step), the CDN is
content-addressed, ImportRun and FinalizeImportRun converge. Per-run
bookkeeping (_mkdb2_import.json, keyed by importer version + server + project
+ run) skips completed runs unless --force. It records the export's
exported_at, and a completed run exported again since is reported
(export_changed), not re-imported: a replay cannot remove rows it no longer
writes (see below), so a re-import is deliberate — delete the run's
_mkdb2_import.json, or --force the whole selection.

Runs whose export has not settled are deferred (counted as deferred_<reason>)
unless --allow-partial-export: no or an incomplete _export.json, recorded
export errors (such as a deferred history parquet), sections a --skip-files
or --skip-history pass left out, or a run not stopped in _export.json or
run.json (a run wandb leaves pending never settles). Import from a quiescent
archive: while the exporter runs, run.json can be newer than the data beside
it.

Known limitations:
- --allow-partial-export imports an unsettled export as it is, a running run
  with exit code 1 at its last heartbeat.
- A run deferred on one pass and imported on a later one is numbered after
  the runs imported before it, so the dashboard lists it above older runs.
- A re-import overwrites only rows with the same (metric, tag, step). When a
  run's log source changed (the streamed console log, later output.log) or
  --max-log-bytes changed, the earlier log chunks stay beside the new ones.
  The first import's run name and created_at are kept.
- Media a history cell names but the archive lacks are counted
  (media_missing), not damage, so a repair needs a deliberate re-import.
  Files over the server's upload limit (256 MiB) are counted
  (media_too_large) and skipped; a lower limit in front of the server, such
  as a proxy's, fails the run instead.
- String cells over 63,999 characters are truncated. history_scan.jsonl.gz
  rows without _step all land on step 0, keeping one value per metric.
- Run timestamps parse only as YYYY-MM-DDTHH:MM:SS followed by "Z",
  ".ffffffZ", or a numeric offset; console lines only as
  YYYY-MM-DDTHH:MM:SS[.ffffff], with or without the Z. Any other shape falls
  back silently: created_at to the import time, a console line to an earlier
  line's time (0 at the start of the log).
- --prefix renames projects, not runs: run ids stay wandb_<slug>_<id>, which
  keeps imported runs apart from a server's own, and run ids are global, so
  the same runs cannot be imported under two prefixes.
- A worker holds up to --media-workers whole media files and a run's whole
  console log in memory. If the OS kills one (out of memory), the invocation
  stops, and a rerun resumes.
"""

import argparse
import collections
import datetime
import gzip
import json
import logging
import math
import multiprocessing
import os
import re
import shlex
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path, PurePosixPath

import grpc
import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from kymo._cdn import content_id, gallery_item, gallery_manifest, metadata_manifest
from kymo._generated import kymo_pb2, kymo_pb2_grpc
from kymo._wire import (
    _MAX_POINTS_PER_MSG,
    _chunk_tuples,
    _is_permanent_upload_error,
    _upload_to_cdn,
)
from kymo.client import (
    _MAX_RUN_NAME_BYTES,
    _RESOURCE_EXTENSIONS,
    _BidiStream,
    _strip_url_userinfo,
)

log = logging.getLogger("import")

# Bump whenever what gets sent changes in a way that should invalidate the
# per-run bookkeeping of earlier importers.
IMPORTER_VERSION = 2
F32_MAX = 3.4028234663852886e38
TEXT_CHUNK_CHARS = 64_000
# wandb's states for a run that has stopped (export_wandb.py's TERMINAL_STATES); a preempted run received SIGTERM.
STATE_EXIT_CODES = {
    "finished": 0,
    "failed": 1,
    "crashed": 255,
    "killed": 137,
    "preempted": 143,
}
IMAGE_EXTS = {"png", "jpg", "jpeg", "gif", "webp", "bmp", "svg", "ico", "tiff"}
# Struct cells of this shape (old wandb summary aggregations) expand to
# <metric>/<field> numeric points instead of being dropped.
STATS_FIELDS = {"-Inf", "Inf", "Nan", "Count", "Max", "Min", "Sum"}
STATS_RENAME = {"-Inf": "neg_inf", "Inf": "inf", "Nan": "nan"}
# kymo-server's upload body limit (cdn.rs ENVELOPE_BYTES).
CDN_UPLOAD_MAX_BYTES = 256 * 1024 * 1024
POINT_KINDS = ("numeric_ts", "numeric_tagged_ts", "text_ts", "cdn_ts")


# ---------------------------------------------------------------- name mapping


def snake(s: str) -> str:
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", s).lower()


GPU_KEYMAP = {
    "gpu": "gpu_util_pct",
    "memory": "gpu_mem_util_pct",
    "memoryAllocated": "gpu_mem_used_pct",
    "memoryAllocatedBytes": "gpu_mem_used_bytes",
    "temp": "gpu_temp_c",
    "powerWatts": "gpu_power_w",
    "powerPercent": "gpu_power_pct",
    "enforcedPowerLimitWatts": "gpu_power_limit_w",
    "smClock": "gpu_sm_clock_mhz",
    "memoryClock": "gpu_mem_clock_mhz",
    "correctedMemoryErrors": "gpu_ecc_corrected",
    "uncorrectedMemoryErrors": "gpu_ecc_uncorrected",
}
SCALAR_MAP = {
    "cpu": "system/proc_cpu_pct",
    "memory": "system/sys_mem_pct",
    "proc.memory.rssMB": "system/proc_mem_used_mb",
    "proc.memory.percent": "system/proc_mem_pct",
    "proc.memory.availableMB": "system/sys_mem_avail_mb",
    "proc.cpu.threads": "system/proc_threads",
    "network.sent": "system/net_sent_bytes",
    "network.recv": "system/net_recv_bytes",
}
_GPU_RE = re.compile(r"^gpu\.(\d+)\.(.+)$")
_GPU_PROC_RE = re.compile(r"^gpu\.process\.(\d+)\.(.+)$")
_CPU_CORE_RE = re.compile(r"^cpu\.(\d+)\.(.+)$")
_DISK_IO_RE = re.compile(r"^disk\.(.+)\.(in|out)$")
_DISK_USAGE_RE = re.compile(r"^disk\.(.+)\.(usagePercent|usageGB)$")


def map_system_column(col: str):
    """wandb events column -> (kymo metric name, tag) or None to skip."""
    if not col.startswith("system."):
        return None
    rest = col[len("system.") :]
    if m := _GPU_PROC_RE.match(rest):
        idx, key = m.groups()
        base = GPU_KEYMAP.get(key, "gpu_" + snake(key))
        return "system/" + base.replace("gpu_", "gpu_proc_", 1), idx
    if m := _GPU_RE.match(rest):
        idx, key = m.groups()
        return "system/" + GPU_KEYMAP.get(key, "gpu_" + snake(key)), idx
    if m := _CPU_CORE_RE.match(rest):
        idx, key = m.groups()
        name = "cpu_core_pct" if key == "cpu_percent" else "cpu_core_" + snake(key)
        return "system/" + name, idx
    if rest in SCALAR_MAP:
        return SCALAR_MAP[rest], ""
    if m := _DISK_IO_RE.match(rest):
        dev, direction = m.groups()
        return f"system/disk_{direction}_mb", dev
    if m := _DISK_USAGE_RE.match(rest):
        path, kind = m.groups()
        name = "disk_used_pct" if kind == "usagePercent" else "disk_used_gb"
        return "system/" + name, path
    return "system/" + snake(rest).replace(".", "_"), ""


# ------------------------------------------------------------------- utilities


def read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def write_json(path, obj):
    path = Path(path)
    tmp = path.parent / (path.name + ".tmp")
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=1, default=str)
    os.replace(tmp, path)


def sanitize_json(value):
    """Make a tree safe for allow_nan=False manifest encoding."""
    if isinstance(value, dict):
        return {str(k): sanitize_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize_json(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def parse_iso_ms(s):
    """wandb ISO timestamp -> epoch ms (None on failure)."""
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    return None


def text_line(text: str) -> str:
    """One wandb string cell -> one line of the metric's text stream. Cells
    carry no terminator, and the viewer joins consecutive chunks verbatim, so
    without one every cell of a run would render as a single line. The
    terminator is inside the chunk budget, not added on top of it."""
    text = text[: TEXT_CHUNK_CHARS - 1]
    return text if text.endswith("\n") else text + "\n"


def point_total(counts):
    return sum(counts[k] for k in POINT_KINDS)


def fmt_points(n):
    return (
        f"{n / 1e9:.2f}B" if n >= 1e9 else (f"{n / 1e6:.1f}M" if n >= 1e6 else f"{n:,}")
    )


def utf8_truncate(text: str, max_bytes: int) -> str:
    return text.encode("utf-8")[:max_bytes].decode("utf-8", "ignore")


def run_id_for(proj_slug: str, orig_id: str) -> str:
    return f"wandb_{proj_slug}_{orig_id}"


class ImportAborted(RuntimeError):
    """The server cannot take imports at all; every run would fail the same way."""


class Counters(collections.Counter):
    def add_errors(self, run_errors, msg):
        run_errors.append(msg)
        self["errors"] += 1
        log.error("%s", msg)


class BulkStream:
    """One ImportMetricsBidi stream: kymo's `_BidiStream` transport (bounded
    feed queue + ack pump thread) under a blocking unacked-point window.

    The server only acks when a cut commits, and it only cuts after
    KYMO_IMPORT_CUT_ROWS rows — so the window MUST exceed the server's cut
    size or the pipeline stalls (`_wait` detects that and says so). The one
    failure clock is `stall_seconds` without ack progress: the server never
    errors under load, so this is the operator's trade between catching a
    misconfigured window and riding out a slow ClickHouse.
    """

    POLL_SECONDS = 0.05

    def __init__(self, stub, project_id, run_id, window_points, stall_seconds):
        self.project_id = project_id
        self.run_id = run_id
        self.window = window_points
        self.stall_seconds = stall_seconds
        self.sent = 0
        self.acked = 0
        self.error = None
        self.ended = False
        self.stream = _BidiStream(stub, 64, rpc=stub.ImportMetricsBidi)

    def _poll(self):
        for kind, value in self.stream.poll_acks():
            if kind == "ack":
                self.acked = max(self.acked, value)
            elif kind == "error":
                self.error = value
            else:
                self.ended = True

    def _wait(self, ready):
        """Poll acks until ready(); cancel and raise on a stream error or on no
        ack progress for `stall_seconds`."""
        progress_at = time.monotonic()
        acked = self.acked
        while True:
            self._poll()
            if self.error is None and ready():
                return
            now = time.monotonic()
            if self.acked != acked:
                acked, progress_at = self.acked, now
            state = f"({self.acked}/{self.sent} points acked)"
            failure = None
            if self.error is not None:
                failure = f"bulk stream failed {state}"
            elif self.ended:
                failure = f"server ended the bulk stream early {state}"
            elif now - progress_at > self.stall_seconds:
                failure = (
                    f"bulk stream stalled with {self.sent - self.acked} points "
                    f"unacked for {self.stall_seconds}s — if this repeats "
                    "immediately, the --bulk-window is probably smaller than the "
                    "server's KYMO_IMPORT_CUT_ROWS (it only acks committed cuts); "
                    "otherwise ClickHouse is slow: raise --stall-seconds"
                )
            if failure is not None:
                self.stream.cancel()
                raise RuntimeError(failure) from self.error
            time.sleep(self.POLL_SECONDS)

    def feed(self, points, count: int):
        self._wait(
            lambda: self.sent - self.acked < self.window and self.stream.feed_has_room()
        )
        self.stream.feed(
            kymo_pb2.MetricsBatch(
                project_id=self.project_id, run_id=self.run_id, points=points
            )
        )
        self.sent += count

    def finish(self):
        """Half-close and wait for the final ack covering every sent point."""
        self._wait(self.stream.feed_has_room)
        if not self.stream.half_close():  # single feeder: room only grows
            raise RuntimeError("bulk stream feed queue refused EOF")
        self._wait(lambda: self.acked >= self.sent)

    def cancel(self):
        self.stream.cancel()


class RateLimiter:
    """Token bucket over points/sec, sized for multi-thousand-point batches."""

    def __init__(self, rate: float):
        self.rate = max(1.0, rate)
        self.allowance = self.rate
        self.last = time.monotonic()

    def consume(self, n: int):
        while True:
            now = time.monotonic()
            self.allowance = min(
                self.rate, self.allowance + (now - self.last) * self.rate
            )
            self.last = now
            if self.allowance >= n or self.allowance >= self.rate:  # never stall huge n
                self.allowance -= n
                return
            time.sleep(min(1.0, (n - self.allowance) / self.rate))


# ------------------------------------------------------------------ the sender


class Sender:
    """All kymo I/O behind one object so --dry-run swaps in pure counting."""

    def __init__(self, cfg, counters: Counters):
        self.cfg = cfg
        self.c = counters
        self.dry = not cfg.execute
        # Optional operator throttle (--rate). Default pacing is the server's:
        # the ack window against synchronous cuts on the private admission
        # budget already bounds a stream, so no client-side ceiling applies.
        self._limiter = RateLimiter(cfg.rate) if cfg.rate else None
        self._pending = []
        self.bulk = None  # open BulkStream for the run being imported
        # Galleries in flight for the current run, oldest first: (future, ctx,
        # metric, step, ts_ms). Reaped on the main thread so counters and the
        # bulk stream stay single-threaded.
        self._galleries = collections.deque()
        self._max_inflight = 2 * cfg.media_workers
        if not self.dry:
            self.channel = grpc.insecure_channel(cfg.server)
            self.stub = kymo_pb2_grpc.KymoStub(self.channel)
            limits = httpx.Limits(
                max_connections=cfg.media_workers + 4,
                max_keepalive_connections=cfg.media_workers + 4,
            )
            self.http = httpx.Client(timeout=120, limits=limits)
            self.cdn_url = cfg.cdn or f"http://{cfg.server.rsplit(':', 1)[0]}:8080"
            self.pool = ThreadPoolExecutor(cfg.media_workers)

    # ---- runs
    def init_run(self, project_id, run_id, run_name, created_at_ms, attempts=4):
        """`attempts` is the transient-error budget: the parent's registration
        pass uses a large one so a server roll (minutes of UNAVAILABLE) pauses
        the import instead of ending it."""
        if self.dry:
            return
        req = kymo_pb2.ImportRunRequest(
            project_id=project_id,
            run_id=run_id,
            run_name=run_name,
            created_at_ms=created_at_ms,
        )
        try:
            self._retry(
                lambda: self.stub.ImportRun(req, timeout=30), "ImportRun", attempts
            )
        except grpc.RpcError as e:
            # FAILED_PRECONDITION is also a trashed/purged run (per-run failure,
            # left to the caller); only the lane gate aborts the invocation.
            if (
                e.code() == grpc.StatusCode.FAILED_PRECONDITION
                and "bulk import is disabled" in (e.details() or "")
            ):
                raise ImportAborted(
                    "the server has bulk import disabled — start kymo-server "
                    "with KYMO_IMPORT_ENABLED=1 and retry"
                ) from e
            if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                raise ImportAborted(
                    "this server predates the bulk-import RPCs — update it; "
                    "there is no fallback path"
                ) from e
            raise

    def finish_data(self):
        """Close the run's bulk stream and wait until everything is acked."""
        if self.bulk is not None:
            stream, self.bulk = self.bulk, None
            stream.finish()

    def abort_data(self):
        """Drop a failed run's stream and its unsent points so neither leaks
        into the next run (the run is replayed whole on the next invocation)."""
        self._pending = []
        for fut, *_ in self._galleries:
            fut.cancel()
        self._galleries.clear()
        if self.bulk is not None:
            stream, self.bulk = self.bulk, None
            stream.cancel()

    def finalize_run(self, project_id, run_id, exit_code, terminated_at_ms):
        if self.dry:
            return
        # No metric list: the server derives the run's registry from the
        # ClickHouse outbox its own inserts populated.
        req = kymo_pb2.FinalizeImportRunRequest(
            project_id=project_id,
            run_id=run_id,
            exit_code=exit_code,
            terminated_at_ms=terminated_at_ms,
        )
        self._retry(
            lambda: self.stub.FinalizeImportRun(req, timeout=120),
            "FinalizeImportRun",
        )

    _NO_RETRY_CODES = (
        "UNIMPLEMENTED",
        "FAILED_PRECONDITION",
        "INVALID_ARGUMENT",
        "NOT_FOUND",
        "ALREADY_EXISTS",
        "PERMISSION_DENIED",
    )

    def _retry(self, fn, what, attempts=4):
        for i in range(attempts):
            try:
                return fn()
            except Exception as e:
                code = getattr(e, "code", lambda: None)()
                if code is not None and code.name in self._NO_RETRY_CODES:
                    raise
                if _is_permanent_upload_error(e):
                    raise
                if i == attempts - 1:
                    raise
                log.warning("retrying %s after: %r", what, e)
                time.sleep(min(30, 2 * (i + 1)))

    # ---- points
    def queue(self, project_id, run_id, tuples):
        self._pending.extend(tuples)
        if len(self._pending) >= _MAX_POINTS_PER_MSG:
            self.flush(project_id, run_id)

    def flush(self, project_id, run_id):
        if not self._pending:
            return
        batch, self._pending = self._pending, []
        self.c.update(t[0] for t in batch)
        if self.dry:
            return
        if self.bulk is None:
            self.bulk = BulkStream(
                self.stub,
                project_id,
                run_id,
                self.cfg.bulk_window,
                self.cfg.stall_seconds,
            )
        if (self.bulk.project_id, self.bulk.run_id) != (project_id, run_id):
            raise RuntimeError("bulk stream crossed runs without finish_data()")
        for points, n in _chunk_tuples(batch):
            if self._limiter:
                self._limiter.consume(n)
            self.bulk.feed(points, n)

    # ---- CDN
    def _upload_bytes(self, data: bytes, ext: str) -> str:
        """Thread-safe upload (httpx.Client is shareable); no counting."""
        if self.dry:
            return content_id(b"", ext)  # placeholder id; content untouched in dry-run
        return self._retry(
            lambda: _upload_to_cdn(self.http, self.cdn_url, data, ext), "cdn upload"
        )

    def upload(self, data: bytes, ext: str) -> str:
        """Main-thread upload with counting (run_info manifests)."""
        self.c["cdn_bytes"] += len(data)
        self.c["cdn_uploads"] += 1
        return self._upload_bytes(data, ext)

    def _gallery_work(self, files, is_image):
        """Upload one gallery's files, then its manifest. Sequential within the
        gallery — parallelism is across galleries in flight, so throughput is
        `media_workers` concurrent uploads whatever the gallery sizes are (an
        upload is a ~200 ms server→bucket round trip, not bandwidth). Runs in a
        pool thread in execute mode: counts are returned, not applied.
        Returns (manifest_id | None, Counter)."""
        c = collections.Counter()
        entries = []
        for rel, caption, path, ext in files:
            try:
                size = path.stat().st_size
                if size > CDN_UPLOAD_MAX_BYTES:
                    # It can never import: a counted skip like unknown media, since run damage would replay the run on every rerun.
                    c["media_too_large"] += 1
                    continue
                data = None if self.dry else path.read_bytes()
            except OSError:
                c["media_missing"] += 1
                continue
            rid = self._upload_bytes(data, ext)
            c["cdn_bytes"] += size
            c["cdn_uploads"] += 1
            if is_image and ext in IMAGE_EXTS:
                entries.append(gallery_item(rid, extension=ext, caption=caption))
            else:
                ctype = {
                    "json": "application/json",
                    "bin": "text/html"
                    if rel.endswith(".html")
                    else "application/octet-stream",
                }.get(ext, "application/octet-stream")
                entries.append(
                    gallery_item(
                        rid, content_type=ctype, filename=PurePosixPath(rel).name
                    )
                )
            c["gallery_items"] += 1
        if not entries:
            c["media_cell_all_missing"] += 1
            return None, c
        manifest = gallery_manifest(entries)
        c["cdn_bytes"] += len(manifest)
        c["cdn_uploads"] += 1
        manifest_id = self._upload_bytes(manifest, "json")
        c["galleries"] += 1
        return manifest_id, c

    def submit_gallery(self, ctx, name, step, ts_ms, files, is_image):
        """Queue one gallery for upload; its cdn_ts point is emitted when the
        gallery is reaped (bounded in-flight count, oldest first). Dry-run
        counts inline. An upload failure surfaces from the reap — inside the
        caller's phase or the run's final `drain_media`."""
        if self.dry:
            self._finish_gallery(
                ctx, name, step, ts_ms, *self._gallery_work(files, is_image)
            )
            return
        fut = self.pool.submit(self._gallery_work, files, is_image)
        self._galleries.append((fut, ctx, name, step, ts_ms))
        while len(self._galleries) >= self._max_inflight:
            self._reap_oldest()

    def _reap_oldest(self):
        fut, ctx, name, step, ts_ms = self._galleries.popleft()
        manifest_id, counts = fut.result()
        self._finish_gallery(ctx, name, step, ts_ms, manifest_id, counts)

    def _finish_gallery(self, ctx, name, step, ts_ms, manifest_id, counts):
        self.c.update(counts)
        if manifest_id is not None:
            self.queue(
                ctx.project_id, ctx.run_id, [("cdn_ts", name, step, manifest_id, ts_ms)]
            )

    def drain_media(self):
        """Reap every in-flight gallery of the current run. Always empties the
        queue (nothing may leak into the next run's stream), then re-raises the
        first failure so the run's bookkeeping does not say complete."""
        first = None
        while self._galleries:
            try:
                self._reap_oldest()
            except Exception as e:
                first = first or e
        if first is not None:
            raise first

    def close(self):
        if not self.dry:
            self.pool.shutdown(wait=True)
            self.http.close()
            self.channel.close()


# ------------------------------------------------------------- media handling


def media_ext(filename: str, fmt: str | None) -> str:
    ext = PurePosixPath(filename).suffix.lstrip(".").lower() or (fmt or "").lower()
    return ext if ext in _RESOURCE_EXTENSIONS else "bin"


def coerce_caption(v) -> str:
    """wandb captions may be str, bool, None, or (in parquet) nested
    type-tagged union structs like {_type_utf8, _type_bool} inside lists."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        for key in ("_type_utf8", "_type_bool"):
            if v.get(key) is not None:
                return coerce_caption(v[key])
        return ""
    if isinstance(v, (list, tuple)):
        return " ".join(filter(None, (coerce_caption(x) for x in v))).strip()
    return str(v)


def synth_image_items(cell, name, step, files_dir):
    """Old images/separated cells carry no filenames; wandb derived
    media/images/{key}_{step}_{index}.{format} at logging time."""
    count = int(cell.get("count") or 0)
    fmt = (cell.get("format") or "png").lower()
    captions = cell.get("captions") or []
    if not count or name is None or step is None:
        return []
    keys = [name, name.replace("/", "_")]
    key = next(
        (
            k
            for k in keys
            if (files_dir / "media" / "images" / f"{k}_{int(step)}_0.{fmt}").is_file()
        ),
        keys[0],
    )
    return [
        (
            f"media/images/{key}_{int(step)}_{i}.{fmt}",
            coerce_caption(captions[i]) if i < len(captions) else "",
        )
        for i in range(count)
    ]


def media_cell_items(cell: dict):
    """Extract (relpath, caption) pairs from a wandb media history cell."""
    if isinstance(cell.get("filenames"), list):
        captions = cell.get("captions") or []
        return [
            (fn, coerce_caption(captions[i]) if i < len(captions) else "")
            for i, fn in enumerate(cell["filenames"])
            if fn and isinstance(fn, str)
        ]
    if cell.get("path") and isinstance(cell["path"], str):
        return [(cell["path"], coerce_caption(cell.get("caption")))]
    # List-of-media containers: {"_type": "html", "count": N, "html": [cells]}
    # (same shape for other media kinds logged as lists).
    for value in cell.values():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            items = []
            for child in value:
                if isinstance(child, dict):
                    items.extend(media_cell_items(child))
            if items:
                return items
    return []


def safe_media_path(files_dir: Path, rel: str):
    p = PurePosixPath(rel)
    if p.is_absolute() or ".." in p.parts:
        return None
    return files_dir.joinpath(*p.parts)


def union_scalar(cell):
    """Parquet type-union structs ({_type_float64, _type_utf8, ...}) encode a
    scalar column whose type varied across rows — wandb stores non-finite
    floats as JSON strings ("Infinity", "NaN"), which forces the union."""
    for v in cell.values():
        if isinstance(v, (int, float)):  # bool is an int
            return float(v)
        if isinstance(v, str):
            try:
                return float(v)
            except ValueError:
                return v
    return None


def process_media_cell(sender, ctx, name, step, ts_ms, cell):
    """One history media cell -> one CDN gallery point (or counted skip)."""
    c = sender.c
    if cell and all(k.startswith("_type_") for k in cell):
        v = union_scalar(cell)
        if isinstance(v, float):
            sender.queue(
                ctx.project_id,
                ctx.run_id,
                [("numeric_ts", name, step, clamp_f32(v, c), ts_ms)],
            )
        elif v is None:
            pass
        else:
            c["union_string_skipped"] += 1
        return
    mtype = cell.get("_type") or ""
    if mtype == "histogram":
        c["histogram_skipped"] += 1
        return
    if not mtype and STATS_FIELDS.issuperset(cell.keys()):
        pts = []
        for field, v in cell.items():
            if isinstance(v, (int, float)) and math.isfinite(float(v)):
                sub = STATS_RENAME.get(field, field.lower())
                pts.append(
                    ("numeric_ts", f"{name}/{sub}", step, clamp_f32(v, c), ts_ms)
                )
        if pts:
            sender.queue(ctx.project_id, ctx.run_id, pts)
            c["stats_expanded"] += 1
        return
    items = media_cell_items(cell)
    if not items and mtype.startswith("images"):
        items = synth_image_items(cell, name, step, ctx.files_dir)
    if not items:
        c["media_unknown"] += 1
        ctx.unknown_media[mtype or "<no _type>"] += 1
        return

    files = []
    for rel, caption in items:
        path = safe_media_path(ctx.files_dir, rel)
        if path is None or not path.is_file():
            c["media_missing"] += 1
            continue
        files.append((rel, caption, path, media_ext(rel, cell.get("format"))))
    if not files:
        c["media_cell_all_missing"] += 1
        return
    sender.submit_gallery(ctx, name, step, ts_ms, files, "image" in mtype)


# ------------------------------------------------------------ value handling


def clamp_f32(v, c: Counters) -> float:
    v = float(v)
    if math.isfinite(v) and abs(v) > F32_MAX:
        c["clamped_f32"] += 1
        return math.copysign(F32_MAX, v)
    return v


class RunContext:
    def __init__(self, project_id, run_id, files_dir):
        self.project_id = project_id
        self.run_id = run_id
        self.files_dir = files_dir
        self.last_text_step = {}
        self.unknown_media = collections.Counter()
        self.history_rows = 0  # run-wide row ordinal for rows without a _step

    def text_step(self, metric, want):
        step = max(want, self.last_text_step.get(metric, want - 1) + 1)
        self.last_text_step[metric] = step
        return step


# --------------------------------------------------------------- run import


def _nleaves(t) -> int:
    if pa.types.is_struct(t):
        return sum(_nleaves(f.type) for f in t)
    if (
        pa.types.is_list(t)
        or pa.types.is_large_list(t)
        or pa.types.is_fixed_size_list(t)
    ):
        return _nleaves(t.value_type)
    if pa.types.is_map(t):
        return _nleaves(t.key_type) + _nleaves(t.item_type)
    return 1


def leaf_owners(schema_arrow):
    """Top-level field name owning each parquet leaf column, in leaf order.

    Row-group column chunks are depth-first leaves of the schema; resolving
    ownership positionally is the only unambiguous way, because
    path_in_schema joins path parts with '.' and metric names themselves
    contain dots (system.gpu.0.gpu, size/up.0.layers...).
    """
    owners = []
    for field in schema_arrow:
        owners.extend([field.name] * _nleaves(field.type))
    return owners


def footer_nonnull_by_field(pf):
    """Exact per-top-level-field non-null cell counts from footer statistics.
    Returns None if leaf alignment can't be established (caller falls back)."""
    md = pf.metadata
    schema = pf.schema_arrow
    owners = leaf_owners(schema)
    if md.num_row_groups and md.row_group(0).num_columns != len(owners):
        return None
    counts = collections.Counter()
    struct_names = {
        n
        for i, n in enumerate(schema.names)
        if str(schema.field(i).type).startswith("struct")
    }
    any_stats = False
    for rg in range(md.num_row_groups):
        rgm = md.row_group(rg)
        for ci in range(rgm.num_columns):
            top = owners[ci]
            if top in struct_names:
                continue  # media cells are counted by reading their columns
            st = rgm.column(ci).statistics
            if st is not None and st.has_null_count:
                any_stats = True
                counts[top] += rgm.num_rows - st.null_count
            else:
                counts[top] += rgm.num_rows
    if not any_stats and md.num_rows:
        return None  # stats-less file (e.g. events): rows*cols would overcount
    return counts


def column_kind(field):
    """How a history column imports: "_" (axes and wandb internals, skipped), "m" (struct cells: media, stats, type unions), "s" (text), or "n" (numeric)."""
    t = str(field.type)
    if field.name.startswith("_"):
        return "_"
    if t.startswith("struct"):
        return "m"
    return "s" if t in ("string", "large_string", "binary") else "n"


def iter_parquet_rows_meta(files):
    """Yield (batch, colname->index) per record batch across ordered parts."""
    for path in files:
        pf = pq.ParquetFile(path)
        for batch in pf.iter_batches(batch_size=2048):
            yield batch, {n: i for i, n in enumerate(batch.schema.names)}


def history_row_axes(batch, idx, created_ms, ctx, c):
    """Per-row (step, ts) axes. A row without a _step takes its run-wide row
    ordinal — batch-local indices would collide across record batches."""
    n = batch.num_rows
    base = ctx.history_rows
    ctx.history_rows += n
    if "_step" in idx:
        steps = [
            int(v) if v is not None else base + i
            for i, v in enumerate(batch.column(idx["_step"]).to_pylist())
        ]
    else:
        c["missing_step_axis"] += 1
        steps = list(range(base, base + n))
    if "_timestamp" in idx:
        tss = [
            int(v * 1000) if v is not None else created_ms
            for v in batch.column(idx["_timestamp"]).to_pylist()
        ]
    else:
        tss = [created_ms] * n
    return steps, tss


def import_history(sender, ctx, run_dir, created_ms, deep):
    c = sender.c
    files = sorted(run_dir.glob("history/*.parquet"))
    scan = run_dir / "history_scan.jsonl.gz"
    if not files and scan.exists():
        import_scan_history(sender, ctx, scan, created_ms)
        return
    if not files:
        return
    if not deep:
        count_history_dry(sender, ctx, files, created_ms)
        return
    for batch, idx in iter_parquet_rows_meta(files):
        steps, tss = history_row_axes(batch, idx, created_ms, ctx, c)
        for name, col_i in idx.items():
            kind = column_kind(batch.schema.field(col_i))
            col = batch.column(col_i)
            if kind == "m":
                for row, cell in enumerate(col.to_pylist()):
                    if cell is not None:
                        process_media_cell(
                            sender, ctx, name, steps[row], tss[row], cell
                        )
            elif kind == "s":
                for row, v in enumerate(col.to_pylist()):
                    if v is None or v == "":
                        continue
                    text = v.decode("utf-8", "replace") if isinstance(v, bytes) else v
                    step = ctx.text_step(name, tss[row])
                    sender.queue(
                        ctx.project_id,
                        ctx.run_id,
                        [("text_ts", name, step, text_line(text), tss[row])],
                    )
                    c["string_cells"] += 1
            elif kind == "n":
                pts = []
                for row, v in enumerate(col.to_pylist()):
                    if v is None:
                        continue
                    try:
                        pts.append(
                            ("numeric_ts", name, steps[row], clamp_f32(v, c), tss[row])
                        )
                    except (TypeError, ValueError):
                        c["unconvertible_values"] += 1
                sender.queue(ctx.project_id, ctx.run_id, pts)


def count_history_dry(sender, ctx, files, created_ms):
    """Dry-run: numeric/string counts from footers, media columns read fully."""
    c = sender.c
    for path in files:
        pf = pq.ParquetFile(path)
        kinds = {f.name: column_kind(f) for f in pf.schema_arrow}
        nonnull = footer_nonnull_by_field(pf)
        if nonnull is None:  # exotic schema; count by reading (slow, rare)
            t = pf.read()
            nonnull = collections.Counter(
                {
                    n: t.num_rows - t.column(i).null_count
                    for i, n in enumerate(t.column_names)
                }
            )
        c["numeric_ts"] += sum(
            nn for top, nn in nonnull.items() if kinds.get(top) == "n"
        )
        string_cols = [n for n, k in kinds.items() if k == "s"]
        media_cols = [n for n, k in kinds.items() if k == "m"]
        if not string_cols and not media_cols:
            ctx.history_rows += pf.metadata.num_rows  # keep ordinals = execute's
            continue
        take = (
            media_cols
            + string_cols
            + [x for x in ("_step", "_timestamp") if x in kinds]
        )
        for batch in pf.iter_batches(batch_size=2048, columns=take):
            idx = {n: i for i, n in enumerate(batch.schema.names)}
            steps, tss = history_row_axes(batch, idx, created_ms, ctx, c)
            for name in media_cols:
                for row, cell in enumerate(batch.column(idx[name]).to_pylist()):
                    if cell is not None:
                        process_media_cell(
                            sender, ctx, name, steps[row], tss[row], cell
                        )
            for name in string_cols:
                vals = batch.column(idx[name]).to_pylist()
                got = sum(1 for v in vals if v not in (None, ""))
                c["string_cells"] += got
                c["text_ts"] += got


def import_scan_history(sender, ctx, scan_path, created_ms):
    c = sender.c
    with gzip.open(scan_path, "rt") as f:
        for line in f:
            try:
                row = json.loads(line)
            except ValueError:
                continue
            step = int(row.get("_step", 0) or 0)
            ts = int(float(row.get("_timestamp", created_ms / 1000)) * 1000)
            pts = []
            for name, v in row.items():
                if name.startswith("_") or v is None:
                    continue
                if isinstance(v, dict):
                    process_media_cell(sender, ctx, name, step, ts, v)
                elif isinstance(v, str):
                    if v:
                        tstep = ctx.text_step(name, ts)
                        sender.queue(
                            ctx.project_id,
                            ctx.run_id,
                            [("text_ts", name, tstep, text_line(v), ts)],
                        )
                        c["string_cells"] += 1
                elif isinstance(v, (int, float)):
                    pts.append(("numeric_ts", name, step, clamp_f32(v, c), ts))
            sender.queue(ctx.project_id, ctx.run_id, pts)


def import_events(sender, ctx, run_dir):
    """Events parquets carry no column statistics, so both dry-run and
    execute count/send by reading them (they are tiny next to history)."""
    c = sender.c
    files = sorted(run_dir.glob("events/*.parquet"))
    if not files:
        return
    for batch, idx in iter_parquet_rows_meta(files):
        if "_timestamp" not in idx:
            c["events_no_timestamp"] += 1
            continue
        tss = [
            int(v * 1000) if v is not None else None
            for v in batch.column(idx["_timestamp"]).to_pylist()
        ]
        pts = []
        for name, col_i in idx.items():
            mapping = map_system_column(name)
            if mapping is None:
                continue
            metric, tag = mapping
            for row, v in enumerate(batch.column(col_i).to_pylist()):
                if v is None or tss[row] is None:
                    continue
                try:
                    val = clamp_f32(v, c)
                except (TypeError, ValueError):
                    c["unconvertible_values"] += 1
                    continue
                ts = tss[row]
                if tag:
                    pts.append(("numeric_tagged_ts", metric, ts, val, tag, ts))
                else:
                    pts.append(("numeric_ts", metric, ts, val, ts))
        sender.queue(ctx.project_id, ctx.run_id, pts)


def import_console_log(sender, ctx, run_dir, max_bytes):
    """files/console_log.jsonl.gz: the console log wandb streamed while the run
    was alive ({"t": iso, "level": "info"|"error", "line": str} per line), with
    per-line timestamps and the stdout/stderr split. Consecutive same-stream
    lines are chunked (newline-joined, <= TEXT_CHUNK_CHARS) into text_ts points
    stepped by the chunk's first timestamp, matching how kymo's own capture
    batches lines."""
    c = sender.c
    path = run_dir / "files" / "console_log.jsonl.gz"
    if not path.is_file():
        return
    entries = []
    total = 0
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for raw in f:
            try:
                e = json.loads(raw)
            except ValueError:
                continue
            line = e.get("line") or ""
            # Older wandb stored each line with its terminator; newer ones do
            # not. The chunk join supplies the terminator, so drop exactly one.
            if line.endswith("\n"):
                line = line[:-1]
                if line.endswith("\r"):
                    line = line[:-1]
            t = e.get("t") or ""
            ts = parse_iso_ms(t if t.endswith("Z") else t + "Z")  # wandb omits the Z
            entries.append((ts, e.get("level"), line))
            total += len(line) + 1
    if not entries:
        return
    dropped = 0
    while total > max_bytes and dropped < len(entries) - 1:
        total -= len(entries[dropped][2]) + 1
        dropped += 1
    c["log_bytes"] += total
    if dropped:
        note = f"[wandb import: first {dropped} console lines omitted]"
        entries = [(entries[dropped][0], "info", note)] + entries[dropped:]
    chunk, chunk_metric, chunk_ts, chunk_len = [], None, None, 0
    pts = []

    def flush_chunk():
        if chunk:
            step = ctx.text_step(chunk_metric, chunk_ts)
            # Terminated: the viewer joins consecutive chunks verbatim (see
            # text_line), so an unterminated chunk would fuse its last line
            # with the next chunk's first.
            text = "\n".join(chunk) + "\n"
            pts.append(("text_ts", chunk_metric, step, text, chunk_ts))

    for ts, level, line in entries:
        metric = "logs/std_err" if level == "error" else "logs/std_out"
        if ts is None:
            ts = chunk_ts or 0
        if metric != chunk_metric or chunk_len + len(line) + 1 > TEXT_CHUNK_CHARS:
            flush_chunk()
            chunk, chunk_metric, chunk_ts, chunk_len = [], metric, ts, 0
        chunk.append(line)
        chunk_len += len(line) + 1
    flush_chunk()
    sender.queue(ctx.project_id, ctx.run_id, pts)


def import_output_log(sender, ctx, run_dir, created_ms, max_bytes):
    """Console log: the uploaded output.log when the archive has it — it is the
    complete record, whereas wandb's stream is truncated for some runs (100k
    of 1.7M lines seen) — otherwise the streamed console_log.jsonl.gz, which
    exists for runs that never finalized."""
    c = sender.c
    path = run_dir / "files" / "output.log"
    if not path.is_file():
        import_console_log(sender, ctx, run_dir, max_bytes)
        return
    size = path.stat().st_size
    dropped = max(0, size - max_bytes)
    c["log_bytes"] += size - dropped
    if sender.dry:
        c["text_ts"] += max(1, (size - dropped) // (TEXT_CHUNK_CHARS // 2))
        return
    with open(path, "rb") as f:
        f.seek(dropped)
        data = f.read()
    text = data.decode("utf-8", "replace")
    if dropped:
        text = f"[wandb import: first {dropped} bytes of output.log omitted]\n" + text
    pos = 0
    while pos < len(text):
        end = min(pos + TEXT_CHUNK_CHARS, len(text))
        cut = text.rfind("\n", pos, end)
        end = cut + 1 if cut > pos and end < len(text) else end
        step = ctx.text_step("logs/std_out", created_ms)
        sender.queue(
            ctx.project_id,
            ctx.run_id,
            [("text_ts", "logs/std_out", step, text[pos:end], created_ms)],
        )
        pos = end


def build_run_info(run_meta, wb_meta, summary, artifacts, created_ms):
    start = datetime.datetime.fromtimestamp(created_ms / 1000)
    meta = {
        "time": {
            "start": start.strftime("%B %d, %Y %I:%M:%S %p"),
            "start_unix": created_ms // 1000,
        }
    }
    wb_meta = wb_meta or {}
    system = {}
    for src, dst in [
        ("host", "hostname"),
        ("os", "os"),
        ("username", "username"),
        ("root", "cwd"),
        ("python", "python_version"),
        ("executable", "python_executable"),
        ("cpu_count", "cpu_count"),
        ("cpu_count_logical", "logical_cpu_count"),
        ("gpu_count", "gpu_count"),
        ("gpu", "gpu_type"),
        ("cudaVersion", "cuda_version"),
    ]:
        if wb_meta.get(src) is not None:
            system[dst] = wb_meta[src]
    if wb_meta.get("program"):
        args = wb_meta.get("args") or []
        system["command"] = shlex.join(
            [str(wb_meta["program"])] + [str(a) for a in args]
        )
    meta["system"] = system
    git = wb_meta.get("git") or {}
    git_out = {}
    if git.get("remote"):
        git_out["remote"] = _strip_url_userinfo(git["remote"])
    if git.get("commit") or run_meta.get("commit"):
        git_out["commit"] = git.get("commit") or run_meta.get("commit")
    if git_out:
        meta["git"] = git_out
    if isinstance(wb_meta.get("slurm"), dict) and wb_meta["slurm"]:
        # wandb-core stores the SLURM_JWT bearer token under "jwt".
        meta["slurm"] = {k: v for k, v in wb_meta["slurm"].items() if k != "jwt"}
    meta["wandb_import"] = {
        "url": run_meta.get("url"),
        "entity": run_meta.get("entity"),
        "project": run_meta.get("project"),
        "run_id": run_meta.get("id"),
        "state": run_meta.get("state"),
        "tags": run_meta.get("tags") or [],
        "notes": run_meta.get("notes"),
        "group": run_meta.get("group"),
        "job_type": run_meta.get("job_type"),
        "sweep": run_meta.get("sweep"),
        "user": run_meta.get("user"),
        "email": wb_meta.get("email"),
        "created_at": run_meta.get("created_at"),
        "heartbeat_at": run_meta.get("heartbeat_at"),
        "history_line_count": run_meta.get("history_line_count"),
        "imported_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "summary": summary,
        "artifacts": artifacts,
    }
    doc = {"meta": meta}
    if run_meta.get("config"):
        doc["config"] = run_meta["config"]
    return sanitize_json(doc)


def import_run_info(sender, ctx, run_dir, run_meta, created_ms):
    summary = run_meta.get("summary")
    if summary is None:
        summary = read_json(run_dir / "files" / "wandb-summary.json")
    wb_meta = read_json(run_dir / "files" / "wandb-metadata.json")
    artifacts = read_json(run_dir / "artifacts.json")
    doc = build_run_info(run_meta, wb_meta, summary, artifacts, created_ms)
    manifest_id = sender.upload(metadata_manifest(doc), "json")
    sender.queue(
        ctx.project_id,
        ctx.run_id,
        [("cdn_ts", "info/run_info", 0, manifest_id, created_ms)],
    )


def export_hold(exp, run_meta):
    """Why a run's export (its _export.json, or None, beside run.json's run_meta) cannot be imported yet, else None. The exporter marks an export complete even when a section failed or was skipped, so completeness alone does not settle it; run.json's state is checked too because older exporters rewrote run.json before a refresh without clearing complete."""
    if exp is None:
        return "no_export"
    if not exp.get("complete"):
        return "export_incomplete"
    if exp.get("errors"):
        return "export_errors"
    if exp.get("skipped"):
        return "export_skipped"
    if (
        exp.get("state") not in STATE_EXIT_CODES
        or run_meta.get("state") not in STATE_EXIT_CODES
    ):
        return "run_not_finished"
    return None


def run_identity(proj_slug, run_meta):
    """(run_id, run_name, created_ms, terminated_ms) as sent to the server."""
    orig_id = run_meta["id"]
    run_id = run_id_for(proj_slug, orig_id)
    run_name = (run_meta.get("name") or orig_id).strip() or orig_id
    run_name = utf8_truncate(run_name, _MAX_RUN_NAME_BYTES)
    created_ms = parse_iso_ms(run_meta.get("created_at")) or int(time.time() * 1000)
    # wandb's last heartbeat is the closest thing the archive has to an end time.
    terminated_ms = parse_iso_ms(run_meta.get("heartbeat_at")) or created_ms
    return run_id, run_name, created_ms, terminated_ms


def describe_error(e: BaseException) -> str:
    """repr(e) plus its cause chain — a stream failure's gRPC status and
    details live on the cause, and that is what an operator needs to see."""
    parts = [repr(e)]
    seen = {id(e)}
    cause = e.__cause__ or e.__context__
    while cause is not None and id(cause) not in seen:
        seen.add(id(cause))
        code = getattr(cause, "code", None)
        details = getattr(cause, "details", None)
        if callable(code) and callable(details):
            parts.append(f"{type(cause).__name__}({code().name}: {details()!r})")
        else:
            parts.append(repr(cause))
        cause = cause.__cause__ or cause.__context__
    return " <- ".join(parts)


def import_run(sender, project_id, proj_slug, run_dir, run_meta, exported_at):
    c, cfg = sender.c, sender.cfg
    run_id, run_name, created_ms, terminated_ms = run_identity(proj_slug, run_meta)
    ctx = RunContext(project_id, run_id, run_dir / "files")
    errors = []
    # Phases that raised: the run's bookkeeping must not say complete, or an
    # ordinary rerun would skip a run with silently missing data. (Finalize
    # still runs — it converges on re-import.) Unknown-media leftovers are
    # deliberately NOT completeness failures: they can never import, so
    # counting them would make reruns retry forever.
    damaged = []

    def phase(name, fn):
        try:
            fn()
        except Exception as e:
            damaged.append(name)
            c.add_errors(errors, f"{name}: {describe_error(e)}")

    sender.init_run(project_id, run_id, run_name, created_ms)
    if cfg.execute:
        # A re-import killed midway must not leave the previous complete marker standing; removed only now that ImportRun accepted the run, since a refusal changes nothing.
        (run_dir / "_mkdb2_import.json").unlink(missing_ok=True)
    phase(
        "run_info",
        lambda: import_run_info(sender, ctx, run_dir, run_meta, created_ms),
    )
    phase(
        "history",
        lambda: import_history(
            sender, ctx, run_dir, created_ms, deep=cfg.execute or cfg.deep
        ),
    )
    phase("events", lambda: import_events(sender, ctx, run_dir))
    phase(
        "output.log",
        lambda: import_output_log(sender, ctx, run_dir, created_ms, cfg.max_log_bytes),
    )
    # Galleries still uploading belong to this run; their points must be in
    # the stream before it closes, and a failed upload must damage the run.
    phase("media", sender.drain_media)
    sender.flush(project_id, run_id)
    fatal = None
    try:
        sender.finish_data()
    except Exception as e:
        fatal = f"bulk stream: {describe_error(e)}"
        c.add_errors(errors, fatal)

    exit_code = STATE_EXIT_CODES.get(run_meta.get("state"), 1)
    if fatal is None:
        sender.finalize_run(project_id, run_id, exit_code, terminated_ms)
    c["runs"] += 1

    if ctx.unknown_media:
        c.add_errors(errors, f"unknown media types skipped: {dict(ctx.unknown_media)}")

    if cfg.execute:
        write_json(
            run_dir / "_mkdb2_import.json",
            {
                "importer_version": IMPORTER_VERSION,
                "imported_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "exported_at": exported_at,
                "server": cfg.server,
                "project_id": project_id,
                "run_id": run_id,
                "exit_code": exit_code,
                "created_ms": created_ms,
                "terminated_ms": terminated_ms,
                "errors": errors,
                "complete": fatal is None and not damaged,
            },
        )
    return errors


# ------------------------------------------------------------------- driver


def project_runs_sorted(proj_dir):
    runs = []
    for run_dir in sorted((proj_dir / "runs").glob("*")):
        meta = read_json(run_dir / "run.json")
        if not meta or not meta.get("id"):
            log.warning("skipping %s: no readable run.json", run_dir.name)
            continue
        runs.append((run_dir, meta))
    runs.sort(key=lambda r: r[1].get("created_at") or "")
    return runs


# Runs shard across worker processes: a run's stream, media pool, and bookkeeping live in the process that imports it. The parent registers each project's runs chronologically before any data work, so within a pass ordinals follow created_at whatever order workers finish in.
_WORKER = None  # this process's Sender, created by _worker_init


def _worker_cfg(cfg):
    """Per-process view of the CLI settings. The optional rate cap is a global
    figure split across workers; media concurrency is per process, so a
    project with a single fat run still gets the full upload parallelism."""
    if cfg.workers <= 1 or not cfg.rate:
        return cfg
    c = argparse.Namespace(**vars(cfg))
    c.rate = cfg.rate / cfg.workers
    return c


def _worker_init(cfg):
    global _WORKER
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(process)d] %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    _WORKER = Sender(_worker_cfg(cfg), Counters())


def import_one(task):
    """Import one run in this process. task = (project_id, dirname, run_dir,
    exported_at): run.json is read here rather than carried, because queued
    tasks that fill the executor's call-queue pipe hang a broken executor's
    shutdown before Python 3.11.5 (CPython gh-94777). Returns (run_dir.name,
    counter delta, errors); ImportAborted, the server refusing imports
    outright, propagates and ends the invocation."""
    sender = _WORKER
    before = sender.c.copy()
    project_id, dirname, run_dir, exported_at = task
    try:
        run_meta = read_json(run_dir / "run.json")
        errs = import_run(sender, project_id, dirname, run_dir, run_meta, exported_at)
    except ImportAborted:
        raise
    except Exception as e:
        errs = []
        sender.c.add_errors(errs, f"run {run_dir.name} FAILED: {describe_error(e)}")
    finally:
        # A finished run leaves nothing to drop; a failed or interrupted one must not leak into this process's next run (an executor worker survives an interrupt) or keep its queued uploads for close() to wait on.
        sender.abort_data()
    return run_dir.name, sender.c - before, errs


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--archive", type=Path, required=True, help="export_wandb.py output root"
    )
    ap.add_argument(
        "--entity", required=True, help="wandb entity (a directory under --archive)"
    )
    ap.add_argument(
        "--projects",
        nargs="*",
        default=None,
        help="original wandb names or archive dir slugs",
    )
    ap.add_argument("--all", action="store_true", help="process every project")
    ap.add_argument(
        "--exclude",
        nargs="*",
        default=[],
        help="projects --all skips (original names or dir slugs)",
    )
    ap.add_argument("--prefix", default="wandb_", help="kymo project name prefix")
    ap.add_argument(
        "--execute",
        action="store_true",
        help="actually write to kymo (default: dry-run)",
    )
    ap.add_argument(
        "--server",
        default=None,
        help="kymo gRPC host:port (REQUIRED with --execute; no default "
        "on purpose so no server is written by accident)",
    )
    ap.add_argument(
        "--cdn", default=None, help="CDN base URL (default: server host :8080)"
    )
    ap.add_argument(
        "--rate",
        type=float,
        default=None,
        help="optional max points/sec throttle across all workers (e.g. for "
        "throughput characterization); by default pacing comes from the "
        "server's ack window and private admission budget",
    )
    ap.add_argument(
        "--bulk-window",
        type=int,
        default=400_000,
        help="max unacked points in flight on the bulk stream; MUST "
        "exceed the server's KYMO_IMPORT_CUT_ROWS (default 250k) "
        "because the server only acks committed cuts",
    )
    ap.add_argument(
        "--stall-seconds",
        type=int,
        default=600,
        help="fail a run's stream after this long without ack progress "
        "(acks arrive once per committed server cut; raise it for a slow "
        "ClickHouse — the server itself never errors under load)",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=4,
        help="processes importing runs concurrently (one run per process at a "
        "time, each on its own bulk stream). One process moves ~100k pts/s; "
        "the server's KYMO_IMPORT_CONCURRENCY (default 2) bounds concurrent "
        "inserts, so beyond a few workers streams just queue on the server. "
        "1 = everything in this process",
    )
    ap.add_argument(
        "--media-workers",
        type=int,
        default=32,
        help="concurrent CDN uploads per worker process (total in flight is "
        "up to workers x this)",
    )
    ap.add_argument(
        "--max-log-bytes",
        type=int,
        default=64 * 1024 * 1024,
        help="tail cap per output.log",
    )
    ap.add_argument(
        "--limit-runs",
        type=int,
        default=None,
        help="import at most N runs per project, the oldest still to import",
    )
    ap.add_argument("--force", action="store_true", help="redo runs already imported")
    ap.add_argument(
        "--allow-partial-export",
        action="store_true",
        help="import runs whose export is not settled as they are, instead of "
        "deferring them (see the docstring)",
    )
    ap.add_argument(
        "--deep",
        action="store_true",
        help="dry-run reads full data instead of parquet footers",
    )
    cfg = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if cfg.execute and not cfg.server:
        ap.error("--execute requires an explicit --server")
    if not cfg.projects and not cfg.all:
        ap.error("pass --projects ... or --all")
    if cfg.workers < 1:
        ap.error("--workers must be >= 1")
    if cfg.limit_runs is not None and cfg.limit_runs < 0:
        ap.error("--limit-runs must be >= 0")
    if cfg.exclude and not cfg.all:
        ap.error("--exclude applies only with --all")

    entity_root = cfg.archive / cfg.entity
    pmap = read_json(entity_root / "projects_map.json") or {}
    slug_to_orig = {v: k for k, v in pmap.items()}
    if cfg.all:
        excluded = set(cfg.exclude)
        if unknown := excluded - set(pmap) - set(pmap.values()):
            ap.error(
                f"--exclude names no project in {entity_root}/projects_map.json: "
                f"{sorted(unknown)}"
            )
        selected = sorted(
            (
                (k, v)
                for k, v in pmap.items()
                if k not in excluded and v not in excluded
            ),
            key=lambda kv: kv[1],
        )
        if excluded:
            log.info(
                "--all: excluding %s", sorted(set(pmap) - {k for k, _ in selected})
            )
    else:
        selected = []
        for token in cfg.projects:
            if token in pmap:
                selected.append((token, pmap[token]))
            elif token in slug_to_orig:
                selected.append((slug_to_orig[token], token))
            else:
                ap.error(
                    f"project {token!r} not found in {entity_root}/projects_map.json"
                )

    counters = Counters()
    pool = None
    if cfg.workers > 1:
        # spawn, not fork: gRPC channels and thread pools do not survive fork. An executor rather than multiprocessing.Pool, which waits forever for the run of a worker the OS killed; the executor breaks instead (BrokenProcessPool).
        pool = ProcessPoolExecutor(
            cfg.workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_worker_init,
            initargs=(cfg,),
        )
    # This process's Sender: it imports the runs itself, or with a pool only registers them.
    _worker_init(cfg)
    # Dry-run ETA ballpark only; actual pacing is the server's admission.
    est_rate = cfg.rate or 200_000
    mode = "EXECUTE" if cfg.execute else "DRY-RUN"
    grand_errors = 0
    clean_exit = interrupted = False
    t0 = time.time()
    try:
        for orig_name, dirname in selected:
            proj_dir = entity_root / dirname
            if not proj_dir.is_dir():
                log.warning("archive dir missing for %s", orig_name)
                continue
            project_id = cfg.prefix + dirname
            runs = project_runs_sorted(proj_dir)
            before = counters.copy()
            log.info("[%s] %s -> %s (%d runs)", mode, orig_name, project_id, len(runs))
            tasks = []
            for run_dir, run_meta in runs:
                prev = read_json(run_dir / "_mkdb2_import.json") or {}
                if (
                    prev.get("server"),
                    prev.get("project_id"),
                    prev.get("run_id"),
                ) != (cfg.server, project_id, run_id_for(dirname, run_meta["id"])):
                    prev = {}  # another target's marker says nothing about this one
                exp = read_json(run_dir / "_export.json")
                exported_at = (exp or {}).get("exported_at")
                if (
                    cfg.execute
                    and not cfg.force
                    and prev.get("complete")
                    and prev.get("importer_version") == IMPORTER_VERSION
                ):
                    # A marker written before exported_at was recorded keeps skipping.
                    if prev.get("exported_at", exported_at) == exported_at:
                        counters["runs_skipped"] += 1
                    else:
                        counters["export_changed"] += 1
                        log.warning(
                            "%s: export changed since its import (now %s); "
                            "delete its _mkdb2_import.json to re-import it",
                            run_dir.name,
                            export_hold(exp, run_meta) or "settled",
                        )
                    continue
                if not cfg.allow_partial_export and (
                    hold := export_hold(exp, run_meta)
                ):
                    counters[f"deferred_{hold}"] += 1
                    log.info("%s: deferred (%s)", run_dir.name, hold)
                    continue
                if len(tasks) == cfg.limit_runs:
                    continue
                if pool is not None:
                    # Chronological, before any worker touches the project. ~40 attempts with the capped backoff is ~17 minutes of patience, enough to ride out a server roll. A per-run refusal (a trashed run, an invalid name) is left to the run's own ImportRun in its worker, which fails only that run.
                    run_id, run_name, created_ms, _ = run_identity(dirname, run_meta)
                    try:
                        _WORKER.init_run(
                            project_id, run_id, run_name, created_ms, attempts=40
                        )
                    except grpc.RpcError as e:
                        if e.code().name not in Sender._NO_RETRY_CODES:
                            raise
                tasks.append((project_id, dirname, run_dir, exported_at))
            results = (
                (
                    f.result()
                    for f in as_completed(pool.submit(import_one, t) for t in tasks)
                )
                if pool is not None
                else map(import_one, tasks)
            )
            for name, delta, errs in results:
                counters.update(delta)
                grand_errors += len(errs)
                if cfg.execute:
                    log.info(
                        "  %s: %s points, %d uploads%s",
                        name,
                        fmt_points(point_total(delta)),
                        delta["cdn_uploads"],
                        f", {len(errs)} error(s)" if errs else "",
                    )
            delta = counters - before
            pts = point_total(delta)
            log.info(
                "[%s] %s: ~%s points, %s galleries, %.1f MB media%s",
                mode,
                orig_name,
                fmt_points(pts),
                delta.get("galleries", 0),
                delta.get("cdn_bytes", 0) / 1e6,
                f", est {pts / est_rate / 3600:.1f}h at {est_rate:.0f}/s"
                if not cfg.execute
                else "",
            )
        clean_exit = True
    except KeyboardInterrupt:
        interrupted = True
        log.warning(
            "interrupted — bookkeeping for finished runs is on disk; re-run to resume"
        )
    except ImportAborted as e:
        log.error("aborting the whole import: %s", e)
        grand_errors += 1
    except BrokenProcessPool:
        log.error(
            "aborting the whole import: a worker process died (killed by the OS, "
            "perhaps out of memory, or failed to start: see any traceback above), "
            "which also stops the other workers' runs in flight; finished runs keep "
            "their bookkeeping, so a rerun resumes (with fewer --workers or "
            "--media-workers if memory ran out)"
        )
        grand_errors += 1
    finally:
        if pool is not None:
            # Ending a worker is a sufficient close: between runs it holds no unflushed state (each run drains its media and closes its stream before returning), and a run cut short sends no data before removing its marker. The executor has no public terminate before Python 3.14.
            if not clean_exit:
                for proc in multiprocessing.active_children():
                    proc.terminate()
            pool.shutdown(cancel_futures=True)
        _WORKER.close()

    total_pts = point_total(counters)
    log.info("=== %s summary (%.1f min) ===", mode, (time.time() - t0) / 60)
    for key in sorted(counters):
        log.info("  %-24s %s", key, fmt_points(counters[key]))
    log.info("  %-24s %s", "TOTAL points", fmt_points(total_pts))
    if not cfg.execute:
        log.info(
            "estimated import time at %.0f pts/s: %.1f h "
            "(media upload time extra: %.1f GB)",
            est_rate,
            total_pts / est_rate / 3600,
            counters.get("cdn_bytes", 0) / 1e9,
        )
    return 130 if interrupted else (1 if grand_errors else 0)


if __name__ == "__main__":
    sys.exit(main())
