#!/usr/bin/env python
"""Export an entire wandb entity to a local archive folder tree. Resumable.

import_to_kymo.py reads the archive. Needs wandb and requests, plus
wandb-workspaces for saved views, and authenticates the way the wandb client
does (`wandb login` or WANDB_API_KEY; WANDB_BASE_URL for a self-hosted server).

Usage:
    python export_wandb.py --archive A --entity E                 # export everything
    python export_wandb.py --archive A --entity E --projects P1 P2
    python export_wandb.py --archive A --entity E --retry-errors  # also redo runs that had download errors
    python export_wandb.py --archive A --entity E --force --projects P1

Exit code 3 means the stall watchdog fired (--stall-timeout); rerun to resume:
    while python export_wandb.py --archive A --entity E; [ $? -eq 3 ]; do sleep 15; done

Archive layout:
    <archive>/<entity>/
      entity.json                    export summary (updated each invocation)
      projects_map.json              original project name -> directory name
      <project_dir>/
        project.json
        reports/<slug>__<id>.json    report specs
        views/<slug>__<id>.json      saved workspace/view specs
        artifacts/<type>/<collection>/<vN>/
          _manifest.json             artifact metadata + file entries (always)
          _complete.json             marker: version fully exported
          files/...                  artifact content (unless --manifest-only-artifacts)
        runs/<run_id>__<name_slug>/
          run.json                   config, summary, tags, notes, state, user, ...
          _export.json               exporter bookkeeping (resume state; exported_at: the run's last export, or a later metadata change)
          history/*.parquet          full metric history (latest run-<id>-history artifact)
          events/*.parquet           system metrics (latest run-<id>-events artifact)
          history_scan.jsonl.gz      fallback when no history parquet artifact exists
          files/...                  run files (logs, media, code, ...)
          artifacts.json             references to logged/used artifacts

Re-running catches up: new runs are exported, terminal-and-unchanged runs are
skipped cheaply (run.json is rewritten when their metadata changed), and runs
that were active during the previous export, or that it left unfinished or
partial (--skip-files, --skip-history), are re-exported (latest history
parquet, new/changed files).

Known limitations:
- The stall watchdog waits for finished tasks: a run's whole export, or all
  of a project's artifact versions. A task longer than --stall-timeout ends
  the process with exit code 3, and the restart lists the entity again and
  redoes that task (artifact versions resume from their markers). Set
  --stall-timeout above the longest task.
- A run file that downloads shorter than wandb's listed size is kept.
- A run's console log is held whole in memory before it is written.
- A run resumed after it finished can keep its older history parquet when
  the re-export runs before wandb has generated the new one.
- A run whose id is another's plus "__..." (abc and abc__x) can take over
  that run's directory.
- wandb identity-token (OIDC) logins carry no API key, so console-log fetches
  fail; log in with an API key.
- Reports stop at 500 per project (logged).
"""

import argparse
import gzip
import hashlib
import json
import logging
import os
import queue
import re
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath

os.environ.setdefault("WANDB_SILENT", "true")

import requests
import wandb

log = logging.getLogger("export")

# The states import_to_kymo.py's STATE_EXIT_CODES maps; a state missing from either side never settles.
TERMINAL_STATES = {"finished", "crashed", "failed", "killed", "preempted"}
STREAM_ARTIFACT_TYPES = {"wandb-history", "wandb-events"}

VIEWS_QUERY = """
query Views($entityName: String, $name: String, $viewType: String) {
  project(name: $name, entityName: $entityName) {
    allViews(viewType: $viewType) {
      edges { node { id name displayName updatedAt spec user { username } } }
    }
  }
}
"""

_tl = threading.local()


def get_api():
    if not hasattr(_tl, "api"):
        _tl.api = wandb.Api(timeout=120)
    return _tl.api


def get_session():
    if not hasattr(_tl, "sess"):
        _tl.sess = requests.Session()
    return _tl.sess


def utcnow():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def slug(s, maxlen=50):
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", str(s or "")).strip("._")
    return s[:maxlen] or "unnamed"


def write_json(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / (path.name + ".tmp")
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=1, default=str)
    os.replace(tmp, path)


def read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def canonical(obj):
    """obj as JSON with sorted keys, as write_json stores values, so a stored and a fresh copy compare equal (NaN included)."""
    return json.dumps(obj, sort_keys=True, default=str)


def parse_maybe_json(v):
    if isinstance(v, str):
        try:
            return json.loads(v)
        except ValueError:
            return v
    return v


def retry(fn, attempts=4, what="call"):
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            if i == attempts - 1:
                raise
            log.warning("retrying %s after error: %r", what, e)
            time.sleep(3 * (i + 1))


class Watchdog:
    """Detects a wedged export: a worker stuck in a network call that never
    times out (deep inside the wandb client, or on a connection wandb's API
    tarpits, which trickles and never completes). If tasks are in flight but
    nothing has completed for --stall-timeout seconds, log the in-flight tasks
    and exit with code 3 so a wrapper can restart us; resume logic redoes the
    aborted tasks, and finished ones are on disk."""

    def __init__(self, stall_secs):
        self.stall_secs = stall_secs
        self.lock = threading.Lock()
        self.inflight = {}
        self.last_progress = time.monotonic()

    def start(self, desc):
        with self.lock:
            self.inflight[threading.current_thread().name] = (desc, time.monotonic())

    def done(self):
        with self.lock:
            self.inflight.pop(threading.current_thread().name, None)
            self.last_progress = time.monotonic()

    def loop(self):
        while True:
            time.sleep(60)
            now = time.monotonic()
            with self.lock:
                idle = now - self.last_progress
                inflight = list(self.inflight.values())
            if inflight and idle > self.stall_secs:
                log.error(
                    "STALL: no task completed in %.0f min; in-flight: %s — "
                    "exiting with code 3 for restart",
                    idle / 60,
                    [f"{d} ({(now - t) / 60:.0f} min)" for d, t in inflight],
                )
                os._exit(3)


class Stats:
    """Thread-safe per-project counters."""

    def __init__(self):
        self.lock = threading.Lock()
        self.projects = {}

    def proj(self, name):
        with self.lock:
            return self.projects.setdefault(
                name,
                {
                    "runs_total": 0,
                    "runs_exported": 0,
                    "runs_skipped": 0,
                    "runs_failed": 0,
                    "bytes_downloaded": 0,
                    "artifact_versions": 0,
                    "reports": 0,
                    "views": 0,
                    "errors": [],
                },
            )

    def add(self, name, key, val=1):
        p = self.proj(name)
        with self.lock:
            p[key] += val

    def error(self, name, msg):
        p = self.proj(name)
        with self.lock:
            p["errors"].append(msg)
        log.error("[%s] %s", name, msg)


# ---------------------------------------------------------------- run export


def run_metadata(run):
    a = run._attrs
    return {
        "id": run.id,
        "name": run.name,
        "entity": run.entity,
        "project": run.project,
        "url": run.url,
        "state": run.state,
        "tags": run.tags,
        "group": run.group or None,
        "job_type": run.job_type or None,
        "notes": run.notes or None,
        "created_at": a.get("createdAt"),
        "heartbeat_at": a.get("heartbeatAt"),
        "commit": a.get("commit"),
        "sweep": a.get("sweepName"),
        "history_line_count": a.get("historyLineCount"),
        "user": a.get("user"),
        "config": run.config,
        "summary": parse_maybe_json(a.get("summaryMetrics")),
        "system_metrics": parse_maybe_json(a.get("systemMetrics")),
        "history_keys": parse_maybe_json(a.get("historyKeys")),
    }


def resolve_run_dir(runs_root, run_id, run_name):
    """Find or create the directory for a run, renaming if the display name changed."""
    want = runs_root / f"{run_id}__{slug(run_name)}"
    existing = sorted(runs_root.glob(f"{run_id}__*"))  # empty when runs_root is missing
    if existing and existing[0] != want:
        try:
            existing[0].rename(want)
        except OSError:
            return existing[0]
    return want


def run_needs_heavy(prev, state, heartbeat, hlc, cfg):
    return (
        cfg.force
        or not prev
        or not prev.get("complete")
        or (cfg.retry_errors and bool(prev.get("errors")))
        or prev.get("state") not in TERMINAL_STATES
        or state not in TERMINAL_STATES
        or prev.get("heartbeat_at") != heartbeat
        or prev.get("history_line_count") != hlc
        or any(not getattr(cfg, f"skip_{k}") for k in prev.get("skipped", []))
    )


def download_run_file(f, files_dir):
    """Download one run file; returns bytes downloaded (0 if already present)."""
    rel = PurePosixPath(f.name)
    if rel.is_absolute() or ".." in rel.parts:
        raise ValueError(f"suspicious file path: {f.name}")
    dest = files_dir.joinpath(*rel.parts)
    size = f.size or 0
    if dest.exists() and dest.stat().st_size == size:
        return 0
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.parent / (dest.name + ".part")
    url = f._attrs.get("directUrl")
    last_err = None
    for attempt in range(3):
        try:
            if not url:
                raise RuntimeError("no directUrl")
            with get_session().get(url, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(tmp, "wb") as out:
                    for chunk in r.iter_content(1 << 20):
                        out.write(chunk)
            os.replace(tmp, dest)
            got = dest.stat().st_size
            if size and got != size:
                log.warning(
                    "size mismatch for %s: listed %d, got %d", f.name, size, got
                )
            return got
        except Exception as e:
            last_err = e
            time.sleep(2 * (attempt + 1))
    # fallback: wandb's own downloader (fresh signed URL, handles auth)
    try:
        f.download(root=str(files_dir), replace=True)
        return dest.stat().st_size if dest.exists() else 0
    except Exception:
        raise last_err


def download_stream_artifact(art, dest_dir, prev_qname):
    """Download latest history/events parquet artifact; skip if version unchanged."""
    if (
        prev_qname == art.qualified_name
        and dest_dir.is_dir()
        and any(dest_dir.iterdir())
    ):
        return 0
    if (art.size or 0) == 0:
        # empty artifact (some 2022-era events artifacts have no contents at all;
        # their manifest also predates the format the current client can parse)
        return 0
    if dest_dir.exists():
        shutil.rmtree(dest_dir)
    dest_dir.mkdir(parents=True)
    retry(
        lambda: art.download(root=str(dest_dir), skip_cache=True),
        what=f"download {art.qualified_name}",
    )
    return art.size or 0


def artifact_ref(a):
    return {
        "qualified_name": a.qualified_name,
        "type": a.type,
        "version": a.version,
        "digest": a.digest,
        "size": a.size,
        "aliases": list(a.aliases or []),
    }


def scan_history_fallback(run, run_dir):
    path = run_dir / "history_scan.jsonl.gz"
    tmp = run_dir / "history_scan.jsonl.gz.tmp"
    n = 0
    with gzip.open(tmp, "wt") as f:
        for row in run.scan_history():
            f.write(json.dumps(row, default=str) + "\n")
            n += 1
    os.replace(tmp, path)
    return n


# useImprovedPagination is load-bearing: without it the server ignores `after`
# and serves the first 500 lines for every cursor while hasNextPage never
# clears, so a naive loop spins forever and archives one page repeated.
CONSOLE_LOG_QUERY = """query ConsoleLog($entity: String!, $project: String!, $name: String!,
                                 $first: Int, $after: String) {
  project(name: $project, entityName: $entity) {
    run(name: $name) {
      logLineCount
      logLines(first: $first, after: $after, useImprovedPagination: true) {
        pageInfo { hasNextPage endCursor }
        edges { node { number line level timestamp } }
      }
    }
  }
}"""


CONSOLE_GRAPHQL = {}  # "url" and "key" of the wandb client's server, set by main()


def fetch_console_log(entity, project, run_id, page=1000):
    """The console log wandb streamed while the run was alive — what the UI's
    Logs tab shows. `output.log` in the files list is only uploaded when a run
    finalizes, so killed/OOM'd/preempted ("crashed") runs have no file, while
    this stream exists for every run and carries a per-line timestamp and a
    stdout/stderr level. Returns [{"t", "level", "line"}, ...], or None when
    wandb no longer has the run.

    Plain HTTPS GraphQL (CONSOLE_GRAPHQL), not the wandb.Api client: its
    calls funnel through the single wandb-core service process, which
    serializes and eventually stalls concurrent fetches. One session per
    run, replaced on any failure: connections have been seen to stall in the
    response read for minutes while a fresh connection answers at once, and
    the pages of one run are the only thing worth keeping a connection for."""
    state = {"sess": None}

    def session():
        if state["sess"] is None:
            state["sess"] = requests.Session()
            state["sess"].auth = ("api", CONSOLE_GRAPHQL["key"])
        return state["sess"]

    after, out = None, []
    while True:
        variables = {
            "entity": entity,
            "project": project,
            "name": run_id,
            "first": page,
            "after": after,
        }

        def call():
            try:
                resp = session().post(
                    CONSOLE_GRAPHQL["url"],
                    json={"query": CONSOLE_LOG_QUERY, "variables": variables},
                    timeout=30,
                )
            except Exception:
                state["sess"].close()
                state["sess"] = None
                raise
            resp.raise_for_status()
            body = resp.json()
            if body.get("errors"):
                raise RuntimeError(f"graphql: {body['errors']}")
            return body["data"]

        r = retry(call, attempts=6, what=f"logLines {run_id}")
        run = (r.get("project") or {}).get("run")
        if run is None:
            return None
        nodes = [e["node"] for e in run["logLines"]["edges"]]
        if out and nodes and nodes[0]["number"] <= out[-1]["n"]:
            raise RuntimeError(
                f"logLines pagination did not advance for {run_id} "
                f"(line {nodes[0]['number']} after {out[-1]['n']})"
            )
        out.extend(
            {
                "n": n["number"],
                "t": n["timestamp"],
                "level": n["level"],
                "line": n["line"],
            }
            for n in nodes
        )
        info = run["logLines"]["pageInfo"]
        if not info["hasNextPage"] or not nodes:
            return out
        after = info["endCursor"]


def export_console_log(entity, project, run_id, run_dir):
    """Write files/console_log.jsonl.gz; returns the line count (None: run gone)."""
    lines = fetch_console_log(entity, project, run_id)
    if lines is None:
        return None
    dest = run_dir / "files" / "console_log.jsonl.gz"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        for line in lines:
            f.write(json.dumps(line, ensure_ascii=False) + "\n")
    os.replace(tmp, dest)
    return len(lines)


def export_run_heavy(entity, project, run_id, run_dir, cfg, stats):
    """Full export of one run's files/history/artifact references."""
    api = get_api()
    run = retry(
        lambda: api.run(f"{entity}/{project}/{run_id}"), what=f"fetch run {run_id}"
    )
    a = run._attrs
    errors = []
    bytes_dl = 0

    write_json(run_dir / "run.json", run_metadata(run))

    # ---- logged/used artifacts: pick latest history/events, record the rest
    hist_art = events_art = None
    logged_refs, used_refs = [], []
    try:
        for art in retry(
            lambda: list(run.logged_artifacts()), what=f"logged_artifacts {run_id}"
        ):
            if art.type == "wandb-history":
                if hist_art is None or int(art.version[1:]) > int(hist_art.version[1:]):
                    hist_art = art
            elif art.type == "wandb-events":
                if events_art is None or int(art.version[1:]) > int(
                    events_art.version[1:]
                ):
                    events_art = art
            else:
                logged_refs.append(artifact_ref(art))
    except Exception as e:
        errors.append(f"logged_artifacts: {e!r}")
    try:
        for art in retry(
            lambda: list(run.used_artifacts()), what=f"used_artifacts {run_id}"
        ):
            used_refs.append(artifact_ref(art))
    except Exception as e:
        errors.append(f"used_artifacts: {e!r}")
    write_json(run_dir / "artifacts.json", {"logged": logged_refs, "used": used_refs})

    prev = read_json(run_dir / "_export.json") or {}
    # ---- metric history / system metrics (parquet artifacts, latest version)
    hist_qname = events_qname = None
    if not cfg.skip_history:
        hlc = a.get("historyLineCount") or 0
        try:
            if hist_art is not None:
                bytes_dl += download_stream_artifact(
                    hist_art, run_dir / "history", prev.get("history_artifact")
                )
                hist_qname = hist_art.qualified_name
            elif hlc > cfg.scan_limit:
                # The backend generates history parquet artifacts with a delay of
                # days for fresh runs, and its live-history scan endpoint times out
                # on histories this large. Record an error so --retry-errors picks
                # the run up once the parquet exists.
                errors.append(
                    f"history: parquet artifact not generated yet and "
                    f"history too large to scan ({hlc} rows) — retry later"
                )
            elif hlc > 0:
                n = scan_history_fallback(run, run_dir)
                log.info(
                    "[%s] %s: no history artifact, scanned %d rows", project, run_id, n
                )
        except Exception as e:
            errors.append(f"history: {e!r}")
        try:
            if events_art is not None:
                bytes_dl += download_stream_artifact(
                    events_art, run_dir / "events", prev.get("events_artifact")
                )
                events_qname = events_art.qualified_name
        except Exception as e:
            errors.append(f"events: {e!r}")

    # ---- run files (logs, media, code, ...) and the streamed console log (the fallback for runs without output.log; wandb caps it for some runs)
    files_count = 0
    console_lines = None
    if not cfg.skip_files:
        try:
            files = [
                f
                for f in retry(
                    lambda: list(run.files(per_page=1000)),
                    what=f"files listing {run_id}",
                )
                if not f.name.startswith("artifact/")
            ]
            files_count = len(files)
            if files:
                files_dir = run_dir / "files"
                with ThreadPoolExecutor(cfg.file_threads) as pool:
                    futs = {
                        pool.submit(download_run_file, f, files_dir): f for f in files
                    }
                    for fut, f in futs.items():
                        try:
                            bytes_dl += fut.result()
                        except Exception as e:
                            errors.append(f"file {f.name}: {e!r}")
        except Exception as e:
            errors.append(f"files: {e!r}")
        try:
            console_lines = export_console_log(entity, project, run_id, run_dir)
        except Exception as e:
            errors.append(f"console_log: {e!r}")

    write_json(
        run_dir / "_export.json",
        {
            "exported_at": utcnow(),
            "state": run.state,
            "heartbeat_at": a.get("heartbeatAt"),
            "history_line_count": a.get("historyLineCount"),
            "history_artifact": hist_qname,
            "events_artifact": events_qname,
            "files_count": files_count,
            "console_log_lines": console_lines,
            "bytes_downloaded": bytes_dl,
            "errors": errors,
            # Sections this pass left out; a later pass without the --skip-* flag exports the run again.
            "skipped": [k for k in ("files", "history") if getattr(cfg, f"skip_{k}")],
            "complete": True,
        },
    )
    stats.add(project, "bytes_downloaded", bytes_dl)
    stats.add(project, "runs_exported")
    if errors:
        stats.error(project, f"run {run_id}: {len(errors)} errors (first: {errors[0]})")
    log.info(
        "[%s] run %s exported (%d files, %.1f MB%s)",
        project,
        run_id,
        files_count,
        bytes_dl / 1e6,
        f", {len(errors)} ERRORS" if errors else "",
    )


# ------------------------------------------------------------ project-level


def export_project_artifacts(entity, project, proj_dir, cfg, stats):
    api = get_api()
    for t in api.artifact_types(f"{entity}/{project}"):
        if t.name in STREAM_ARTIFACT_TYPES:
            continue
        for coll in t.collections():
            for art in coll.artifacts():
                vdir = (
                    proj_dir
                    / "artifacts"
                    / slug(t.name, 80)
                    / slug(coll.name, 120)
                    / art.version
                )
                skip_files = f"{project}/{t.name}" in cfg.manifest_only_artifacts
                done = read_json(vdir / "_complete.json")
                if done and (done.get("files_downloaded") or skip_files):
                    continue
                try:
                    entries = [
                        {"path": p, "digest": e.digest, "size": e.size, "ref": e.ref}
                        for p, e in art.manifest.entries.items()
                    ]
                    write_json(
                        vdir / "_manifest.json",
                        {
                            "artifact": {
                                "qualified_name": art.qualified_name,
                                "name": art.name,
                                "type": art.type,
                                "version": art.version,
                                "digest": art.digest,
                                "size": art.size,
                                "state": str(art.state),
                                "aliases": list(art.aliases or []),
                                "metadata": art.metadata,
                                "description": art.description,
                                "created_at": art.created_at,
                                "updated_at": art.updated_at,
                            },
                            "entries": entries,
                        },
                    )
                    if not skip_files:
                        retry(
                            lambda: art.download(
                                root=str(vdir / "files"),
                                skip_cache=True,
                                allow_missing_references=True,
                            ),
                            what=f"download artifact {art.qualified_name}",
                        )
                        stats.add(project, "bytes_downloaded", art.size or 0)
                    write_json(
                        vdir / "_complete.json",
                        {"exported_at": utcnow(), "files_downloaded": not skip_files},
                    )
                    stats.add(project, "artifact_versions")
                    log.info(
                        "[%s] artifact %s (%.1f MB%s)",
                        project,
                        art.qualified_name,
                        (art.size or 0) / 1e6,
                        ", manifest only" if skip_files else "",
                    )
                except Exception as e:
                    stats.error(project, f"artifact {art.qualified_name}: {e!r}")


def export_reports(entity, project, proj_dir, cfg, stats):
    api = get_api()
    seen = set()
    for r in api.reports(f"{entity}/{project}"):
        if r.id in seen:
            break
        if len(seen) >= 500:
            log.warning("[%s] reports: stopped at the 500-report cap", project)
            break
        seen.add(r.id)
        try:
            spec = r.spec if isinstance(r.spec, dict) else json.loads(r.spec or "{}")
            out = {
                "id": r.id,
                "name": getattr(r, "name", None),
                "display_name": getattr(r, "display_name", None),
                "description": getattr(r, "description", None),
                "created_at": str(getattr(r, "created_at", "") or ""),
                "updated_at": str(getattr(r, "updated_at", "") or ""),
                "url": getattr(r, "url", None),
                "spec": spec,
            }
            fname = f"{slug(out['display_name'] or 'report')}__{slug(r.id, 30)}.json"
            write_json(proj_dir / "reports" / fname, out)
            stats.add(project, "reports")
        except Exception as e:
            stats.error(project, f"report {getattr(r, 'id', '?')}: {e!r}")


def export_views(entity, project, proj_dir, cfg, stats):
    from wandb_workspaces.workspaces.internal import execute_graphql

    api = get_api()
    res = execute_graphql(
        api,
        VIEWS_QUERY,
        {"entityName": entity, "name": project, "viewType": "project-view"},
    )
    edges = ((res.get("project") or {}).get("allViews") or {}).get("edges") or []
    for e in edges:
        n = e["node"]
        out = {
            "id": n["id"],
            "name": n.get("name"),
            "display_name": n.get("displayName"),
            "updated_at": n.get("updatedAt"),
            "user": (n.get("user") or {}).get("username"),
            "spec": parse_maybe_json(n.get("spec")),
        }
        fname = f"{slug(out['display_name'] or out['name'] or 'view')}__{slug(n['id'], 30)}.json"
        write_json(proj_dir / "views" / fname, out)
        stats.add(project, "views")


# ------------------------------------------------------------- orchestration

TASK_HANDLERS = {
    "run": export_run_heavy,
    "artifacts": export_project_artifacts,
    "reports": export_reports,
    "views": export_views,
}


def consumer(q, stop, cfg, stats, wd):
    while not stop.is_set():
        try:
            kind, project, args = q.get(timeout=1)
        except queue.Empty:
            continue
        wd.start(f"{kind} {project}" + (f"/{args[0]}" if kind == "run" else ""))
        try:
            TASK_HANDLERS[kind](cfg.entity, project, *args, cfg, stats)
        except Exception as e:
            if kind == "run":
                stats.add(project, "runs_failed")
                stats.error(project, f"run {args[0]} FAILED: {e!r}")
            else:
                stats.error(project, f"{kind} FAILED: {e!r}")
        finally:
            wd.done()
            q.task_done()


def project_dir_for(entity_root, pmap, name):
    if name in pmap:
        return entity_root / pmap[name]
    s = slug(name, 80)
    if s in pmap.values():
        s = f"{s}-{hashlib.sha1(name.encode()).hexdigest()[:6]}"
    pmap[name] = s
    write_json(entity_root / "projects_map.json", pmap)
    return entity_root / s


def produce(q, cfg, stats):
    api = get_api()
    entity_root = cfg.archive / cfg.entity
    entity_root.mkdir(parents=True, exist_ok=True)
    pmap = read_json(entity_root / "projects_map.json") or {}

    projects = list(api.projects(cfg.entity))
    if cfg.projects:
        wanted = set(cfg.projects)
        missing = wanted - {p.name for p in projects}
        if missing:
            log.warning("projects not found on server: %s", sorted(missing))
        projects = [p for p in projects if p.name in wanted]
    log.info("exporting %d project(s) of entity %s", len(projects), cfg.entity)

    for p in projects:
        proj_dir = project_dir_for(entity_root, pmap, p.name)
        write_json(
            proj_dir / "project.json",
            {
                "name": p.name,
                "entity": cfg.entity,
                "url": p.url,
                "attrs": dict(p._attrs),
                "refreshed_at": utcnow(),
            },
        )
        q.put(("reports", p.name, (proj_dir,)))
        q.put(("views", p.name, (proj_dir,)))
        if not cfg.skip_artifacts:
            q.put(("artifacts", p.name, (proj_dir,)))

        runs_root = proj_dir / "runs"
        seen = set()
        n_heavy = 0
        try:
            runs = api.runs(f"{cfg.entity}/{p.name}", per_page=100)
            # wandb >= 0.22.3 lists runs lazily, without config and summaries, and loading them later replaces run._attrs under run_metadata; list in full, as older clients do.
            if hasattr(runs, "upgrade_to_full"):
                runs.upgrade_to_full()
            for run in runs:
                if run.id in seen:
                    continue
                seen.add(run.id)
                stats.add(p.name, "runs_total")
                run_dir = resolve_run_dir(runs_root, run.id, run.name)
                a = run._attrs
                prev = read_json(run_dir / "_export.json")
                heavy = run_needs_heavy(
                    prev,
                    run.state,
                    a.get("heartbeatAt"),
                    a.get("historyLineCount"),
                    cfg,
                )
                meta = run_metadata(run)
                meta_changed = canonical(read_json(run_dir / "run.json")) != canonical(
                    meta
                )
                if heavy and prev and prev.get("complete"):
                    # The run's data is about to be replaced, starting with run.json below (the refresh also removes history before its new version downloads), so a kill before the refresh finishes must not leave the old completion standing.
                    write_json(run_dir / "_export.json", {**prev, "complete": False})
                elif not heavy and prev and meta_changed:
                    # Metadata changed without a refresh (tags, notes, or config edited after the run stopped); a new exported_at lets the importer report it.
                    write_json(
                        run_dir / "_export.json", {**prev, "exported_at": utcnow()}
                    )
                if heavy or meta_changed:
                    write_json(run_dir / "run.json", meta)
                if heavy:
                    n_heavy += 1
                    q.put(("run", p.name, (run.id, run_dir)))
                else:
                    stats.add(p.name, "runs_skipped")
        except Exception as e:
            stats.error(p.name, f"run listing FAILED: {e!r}")
        log.info(
            "[%s] listed %d runs, %d queued for export", p.name, len(seen), n_heavy
        )


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--archive", type=Path, required=True, help="archive root")
    ap.add_argument("--entity", required=True, help="wandb entity to export")
    ap.add_argument(
        "--projects",
        nargs="*",
        default=None,
        help="restrict to these project names (exact match)",
    )
    ap.add_argument("--workers", type=int, default=8, help="parallel export workers")
    ap.add_argument(
        "--file-threads", type=int, default=6, help="parallel file downloads per run"
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="re-export all encountered runs even if unchanged",
    )
    ap.add_argument(
        "--retry-errors",
        action="store_true",
        help="re-export runs whose previous export recorded errors",
    )
    ap.add_argument(
        "--skip-artifacts",
        action="store_true",
        help="skip project-level artifact export",
    )
    ap.add_argument("--skip-files", action="store_true", help="skip run file downloads")
    ap.add_argument(
        "--skip-history", action="store_true", help="skip history/events export"
    )
    ap.add_argument(
        "--manifest-only-artifacts",
        nargs="*",
        default=[],
        metavar="PROJECT/TYPE",
        help="artifact types to export as manifests and metadata only, without "
        "downloading their files (e.g. large model checkpoints)",
    )
    ap.add_argument(
        "--scan-limit",
        type=int,
        default=3000,
        help="max history rows to export via the scan_history fallback; "
        "larger histories without a parquet artifact are deferred",
    )
    ap.add_argument(
        "--stall-timeout",
        type=int,
        default=3600,
        help="exit with code 3 if tasks are in flight but none "
        "completes for this many seconds",
    )
    cfg = ap.parse_args()

    logdir = cfg.archive / "_logs"
    logdir.mkdir(parents=True, exist_ok=True)
    logfile = logdir / time.strftime("export-%Y%m%d-%H%M%S.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stderr), logging.FileHandler(logfile)],
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    log.info("archive: %s | log: %s", cfg.archive, logfile)

    # Once, on this thread: constructing wandb.Api verifies the login (through wandb-core, in recent clients) and may prompt for one.
    api = get_api()
    CONSOLE_GRAPHQL.update(url=f"{api.settings['base_url']}/graphql", key=api.api_key)

    stats = Stats()
    q = queue.Queue(maxsize=2000)
    stop = threading.Event()
    wd = Watchdog(cfg.stall_timeout)
    workers = [
        threading.Thread(target=consumer, args=(q, stop, cfg, stats, wd), daemon=True)
        for _ in range(cfg.workers)
    ]
    for w in workers:
        w.start()
    threading.Thread(target=wd.loop, daemon=True).start()

    interrupted = False
    try:
        produce(q, cfg, stats)
        q.join()
    except KeyboardInterrupt:
        interrupted = True
        log.warning("interrupted — draining queue; safe to re-run to resume")
        stop.set()
        try:
            while True:
                q.get_nowait()
                q.task_done()
        except queue.Empty:
            pass
    finally:
        stop.set()
        for w in workers:
            w.join(timeout=60)

    # ---- summary
    entity_root = cfg.archive / cfg.entity
    prev_entity = read_json(entity_root / "entity.json") or {}
    all_projects = prev_entity.get("projects", {})
    totals = {
        "runs_total": 0,
        "runs_exported": 0,
        "runs_skipped": 0,
        "runs_failed": 0,
        "bytes_downloaded": 0,
        "errors": 0,
    }
    with stats.lock:
        for name, p in stats.projects.items():
            all_projects[name] = {**p, "last_export": utcnow()}
            for k in totals:
                totals[k] += p.get(k, 0) if k != "errors" else len(p.get("errors", []))
    write_json(
        entity_root / "entity.json",
        {
            "entity": cfg.entity,
            "updated_at": utcnow(),
            "interrupted": interrupted,
            "excluded_artifact_files": sorted(cfg.manifest_only_artifacts),
            "last_session_totals": totals,
            "projects": all_projects,
        },
    )
    log.info(
        "DONE%s: %d exported, %d skipped, %d failed, %.2f GB downloaded, %d errors",
        " (interrupted)" if interrupted else "",
        totals["runs_exported"],
        totals["runs_skipped"],
        totals["runs_failed"],
        totals["bytes_downloaded"] / 1e9,
        totals["errors"],
    )

    if totals["errors"]:
        log.warning(
            "%d errors this session — see %s and per-run _export.json; "
            "re-run with --retry-errors to retry",
            totals["errors"],
            logfile,
        )
    return 130 if interrupted else (1 if totals["errors"] else 0)


if __name__ == "__main__":
    sys.exit(main())
