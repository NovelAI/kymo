"""Read access to logged runs: ``kymo.Api``.

Uses only read RPCs and CDN GETs, so it is safe to point at runs that are still logging.
"""

import json
import math
import multiprocessing
import os
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote

import grpc

from kymo._generated import kymo_pb2 as pb
from kymo._generated import kymo_pb2_grpc

__all__ = ["Api", "LogLine", "Logs", "Run"]

# Raw series of long runs exceed grpc-python's 4 MB default.
_CHANNEL_OPTIONS = (("grpc.max_receive_message_length", 256 * 1024 * 1024),)
# The server's text window cap (query.rs MAX_TEXT_WINDOW_LINES); asking for no more per page also keeps a huge limit inside line_limit's uint32.
_TEXT_WINDOW_LINES = 2000
# Steps ride the chart's f64 axis, which is exact only below 2**53 in magnitude: from there up, neighbouring integers round to the same double.
_MAX_EXACT_STEP = 2**53
# Marker kinds 1-3 are logged non-finite values. Kind 4 annotates an unplottable custom x, which these reads never request.
_NONFINITE = {1: math.nan, 2: math.inf, 3: -math.inf}

# Set once this process opens an Api channel; kymo.init() reads it (_check_fork_safe).
_channel_opened = False


def _check_fork_safe() -> None:
    """Refuse a forked upload worker after this process started gRPC.

    Forking after gRPC starts has hung worker shutdown; client.py _run_control_rpc keeps the trainer gRPC-free for the same reason. allow_none=False would pin the process-wide start method, so fall back to the platform default instead.
    """
    if not _channel_opened:
        return
    method = (
        multiprocessing.get_start_method(allow_none=True)
        or multiprocessing.get_all_start_methods()[0]
    )
    if method == "fork":
        raise RuntimeError(
            "kymo.init() forks its upload worker, which is unsafe after kymo.Api "
            "opened a gRPC channel in this process. Read from a separate process, "
            "call kymo.init() before the first read, or select the 'spawn' or "
            "'forkserver' start method with multiprocessing.set_start_method() "
            "before the first kymo.init()."
        )


@dataclass(frozen=True)
class Run:
    run_id: str
    name: str
    ordinal: int  # per-project, stable; drives the dashboard's colour and order
    status: str  # running, stuck, unresponsive, presumed_dead, crashed, finished, or unknown (unset, or a state newer than this client)
    created_at_ms: int
    last_ingested_at_ms: Optional[int]  # server clock of the last stored point
    terminated_at_ms: Optional[int]


@dataclass(frozen=True)
class LogLine:
    index: int  # position in the (search-filtered) stream
    stream: str
    step: int  # capture time (epoch ms) for the logs/std_* streams kymo writes
    text: str


@dataclass(frozen=True)
class Logs:
    total_lines: int
    # Can hold fewer lines than requested, with gaps or repeats in index: the server reads its index and payload non-atomically, so lines can vanish or shift mid-read while a stream is rewritten.
    lines: list[LogLine]


class Api:
    """Read-only client for a kymo server or the local stack.

    Arguments resolve as in ``kymo.init``: ``mode`` falls back to ``$KYMO_MODE``, hosted mode needs ``server_address`` or ``$KYMO_SERVER``, and the CDN defaults to port 8080 on the server's host. ``timeout`` bounds every RPC and CDN fetch.

    An Api works only in the process that created it, and not after ``close()``.
    """

    def __init__(
        self,
        server_address: Optional[str] = None,
        *,
        mode: Optional[str] = None,
        cdn_address: Optional[str] = None,
        timeout: float = 60.0,
    ):
        # client.py imports this module before defining these helpers, so they cannot be imported at module scope.
        from kymo.client import _cdn_address_for, _resolve_server

        self._timeout = timeout
        self._server_address = _resolve_server(
            "kymo.Api", mode, server_address, cdn_address=cdn_address
        )
        self._local = self._server_address is None
        if not self._local:
            self._cdn = (cdn_address or _cdn_address_for(self._server_address)).rstrip(
                "/"
            )
        # Opened on first use: creating a channel starts gRPC's threads, which _check_fork_safe guards init() against.
        self._channel = self._stub = None
        self._pid = os.getpid()
        import httpx  # not at module scope: every `import kymo` loads this module

        # Content keys are immutable, so a redirecting ``cdn_address`` gateway is safe to follow. The local CDN is loopback, which proxy settings must not intercept (httpx proxies 127.0.0.1 even under NO_PROXY=localhost).
        self._http = httpx.Client(
            timeout=timeout, follow_redirects=True, trust_env=not self._local
        )

    def _connect(self) -> None:
        global _channel_opened
        if self._local:
            from kymo._local_runtime import ensure_local_endpoint, grpc_channel

            endpoint = ensure_local_endpoint()
            channel = grpc_channel(endpoint, _CHANNEL_OPTIONS)
            self._cdn = endpoint.cdn_origin
        else:
            channel = grpc.insecure_channel(
                self._server_address, options=_CHANNEL_OPTIONS
            )
        _channel_opened = True
        # The old channel closes only once its replacement exists: a stub left on a closed channel crashes the process on its next call when the channel is intercepted (local mode, grpcio 1.84).
        if self._channel is not None:
            self._channel.close()
        self._channel, self._stub = channel, kymo_pb2_grpc.KymoStub(channel)

    def _require_usable(self) -> None:
        if self._http.is_closed:
            raise RuntimeError("kymo.Api: used after close()")
        # A forked child shares its parent's pooled sockets and channel: interleaved responses hand a process another key's bytes, and gRPC is unsafe after fork.
        if os.getpid() != self._pid:
            raise RuntimeError(
                "kymo.Api: an Api cannot be used across fork; create one in each process"
            )

    def _rpc(self, method: str, request):
        self._require_usable()
        if self._stub is None:
            self._connect()
        try:
            return getattr(self._stub, method)(request, timeout=self._timeout)
        except grpc.RpcError as error:
            # The local stack stops after an idle hour (UNAVAILABLE), and a restarted stack keeps its socket path but rotates the bearer (UNAUTHENTICATED); ensure_local_endpoint starts or finds it.
            if not self._local or error.code() not in (
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.UNAUTHENTICATED,
            ):
                raise
        self._connect()
        return getattr(self._stub, method)(request, timeout=self._timeout)

    def close(self) -> None:
        if self._channel is not None:
            self._channel.close()
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def projects(self) -> list[str]:
        """Project ids, including projects whose runs are all in Trash."""
        return list(self._rpc("ListProjects", pb.ListProjectsRequest()).project_ids)

    def runs(self, project_id: str) -> list[Run]:
        """The project's runs, newest first."""
        resp = self._rpc("ListRuns", pb.ListRunsRequest(project_id=project_id))
        return [
            Run(
                run_id=r.run_id,
                name=r.run_name,
                ordinal=r.ordinal,
                status=_enum_name(pb.RunStatus, r.status).removeprefix("run_status_"),
                created_at_ms=r.created_at_ms,
                last_ingested_at_ms=(
                    r.last_ingested_at_ms if r.HasField("last_ingested_at_ms") else None
                ),
                terminated_at_ms=(
                    r.terminated_at_ms if r.HasField("terminated_at_ms") else None
                ),
            )
            for r in resp.runs
        ]

    def metrics(self, project_id: str, run_id: str) -> dict[str, str]:
        """Metric name to kind: ``numeric``, ``cdn`` (images, files, metadata) or ``text_stream``.

        The server registers a name a couple of seconds after its first point arrives, so a brand-new metric can be readable before it is listed.
        """
        resp = self._rpc(
            "ListMetrics", pb.ListMetricsRequest(project_id=project_id, run_id=run_id)
        )
        return {
            m.metric_name: _enum_name(pb.MetricInfo.MetricType, m.metric_type)
            for m in sorted(resp.metrics, key=lambda m: m.metric_name)
        }

    def history(
        self,
        project_id: str,
        run_id: str,
        metric_name: str,
        *,
        step_min: Optional[int] = None,
        step_max: Optional[int] = None,
    ) -> dict[str, list[tuple[int, float]]]:
        """Every logged point of a numeric metric: tag to ``[(step, value), ...]``.

        An untagged metric has the single tag ``""``; a metric logged as a list has tags ``"0"``, ``"1"``, .... Values are f32-exact floats, and logged NaN/inf come back as ``nan``/``inf``. A step re-logged later holds its latest value.

        Server limits: a metric with list entries in the requested range hides its scalar entries there, a list metric with more than 64 tags is refused, and steps must stay below 2**53 in magnitude.
        """

        def query(tags):
            ref = pb.SeriesRef(
                project_id=project_id, run_id=run_id, metric_name=metric_name, tags=tags
            )
            req = pb.ChartRequest(y_series=[ref], step_min=step_min, step_max=step_max)
            return self._rpc("QueryChart", req)

        resp = query([""])
        tagged = not resp.series
        if tagged:
            # A metric with tagged rows answers the untagged filter with nothing (query.rs keeps only its tags), so ask for every tag; a single-ref chart labels each series by its tag.
            resp = query([])
            if len(resp.series) == 1 and resp.series[0].label == run_id:
                # A lone series labelled with the run id is either a tag equal to it or an untagged series whose first point arrived between the two requests; the untagged filter tells them apart.
                untagged = query([""])
                if untagged.series:
                    resp, tagged = untagged, False
        if resp.x_values and (
            resp.x_values[0] <= -_MAX_EXACT_STEP or resp.x_values[-1] >= _MAX_EXACT_STEP
        ):
            raise ValueError(
                f"kymo.Api: {metric_name!r} has steps of magnitude 2**53 or more, which the chart axis cannot return exactly"
            )
        if resp.banded:
            # Step is the storage key, so an unsampled step chart holds one value per column. A band would put bucket means in `values`, which must never pass for samples.
            raise RuntimeError(
                f"kymo.Api: server returned a banded chart for {metric_name!r}"
            )
        return {
            s.label if tagged else "": [
                (int(resp.x_values[c]), v) for c, v in _decode_points(s)
            ]
            for s in resp.series
        }

    def media(
        self,
        project_id: str,
        run_id: str,
        metric_name: str,
        *,
        step_min: Optional[int] = None,
        step_max: Optional[int] = None,
    ) -> list[tuple[int, Optional[str]]]:
        """A cdn metric's ``(step, key)`` entries, ascending; ``key`` is None while that step's upload is still in flight.

        Fetch a key with ``fetch``. Image and metadata metrics store JSON manifests: ``{"class": "image_gallery", "items": [{"resource": key, ...}]}`` or ``{"class": "metadata", "data": {...}}``.
        """
        resp = self._rpc(
            "QueryCdnKeys",
            pb.QueryCdnKeysRequest(
                refs=[
                    pb.SeriesRef(
                        project_id=project_id, run_id=run_id, metric_name=metric_name
                    )
                ],
                step_min=step_min,
                step_max=step_max,
            ),
        )
        return [
            (e.step, None if e.cdn_key.startswith("pending:") else e.cdn_key)
            for s in resp.series
            for e in s.entries
        ]

    def fetch(self, key: str) -> bytes:
        """The bytes of one CDN object."""
        import httpx

        self._require_usable()
        if self._local and self._stub is None:
            self._connect()  # the local CDN origin comes from the endpoint
        # Encoded whole, as the dashboard requests it: a raw `?`, `#`, `%` or `/` would reach some other object.
        path = f"/cdn/{quote(key, safe='')}"
        try:
            resp = self._http.get(self._cdn + path)
        except httpx.ConnectError:
            if not self._local:
                raise
            # The local stack stops after an idle hour; ensure_local_endpoint restarts it, on new ports if `kymo ports` changed them.
            self._connect()
            resp = self._http.get(self._cdn + path)
        resp.raise_for_status()
        return resp.content

    def run_info(self, project_id: str, run_id: str) -> Optional[dict]:
        """The run's metadata as logged by ``kymo.init``/``update_config`` (``meta`` and ``config``), or None until it has been uploaded."""
        keys = [k for _, k in self.media(project_id, run_id, "info/run_info") if k]
        if not keys:
            return None
        return json.loads(self.fetch(keys[-1])).get("data", {})

    def logs(
        self,
        project_id: str,
        run_id: str,
        *,
        streams=("logs/std_out", "logs/std_err"),
        search: str = "",
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Logs:
        """Captured console lines from the named streams, merged as the server orders them: by step (capture time), then stream name.

        ``offset=None`` reads the tail; ``limit=None`` reads to the end. ``search`` is a case-insensitive substring filter applied by the server, and indexes then count filtered lines; the server scans the whole stream for every page of a search, so bound ``limit`` on long runs.
        """
        if isinstance(streams, str):
            raise TypeError("kymo.Api.logs: streams must be a sequence of metric names")
        if (offset is not None and offset < 0) or (limit is not None and limit < 0):
            raise ValueError("kymo.Api.logs: offset and limit must be >= 0")
        # Any offset past the end returns the total with no payload read; an in-range probe could trip the window byte cap on an oversized chunk.
        req = pb.QueryTextWindowRequest(
            project_id=project_id,
            run_id=run_id,
            metric_names=streams,
            search=search,
            line_offset=2**63,
            line_limit=1,
        )
        total = self._rpc("QueryTextWindow", req).total_lines
        if limit is None:
            limit = total
        if offset is None:
            offset = max(0, total - limit)
        lines = []
        page = _TEXT_WINDOW_LINES
        while len(lines) < limit:
            req.line_offset = offset
            req.line_limit = min(limit - len(lines), page)
            try:
                resp = self._rpc("QueryTextWindow", req)
            except grpc.RpcError as error:
                # The server also caps a window's bytes (8 MiB, counting every chunk it overlaps), which long lines reach before 2000 of them.
                if (
                    error.code() != grpc.StatusCode.RESOURCE_EXHAUSTED
                    or req.line_limit == 1
                ):
                    raise
                page = req.line_limit // 2
                continue
            lines.extend(
                LogLine(ln.line_index, ln.metric_name, ln.step, ln.text)
                for ln in resp.lines
            )
            # A short page is not the end: line_index is the cursor, and only the response's total, or a cursor that fails to advance, ends the stream.
            cursor = resp.lines[-1].line_index + 1 if resp.lines else offset
            if cursor >= resp.total_lines or cursor <= offset:
                break
            offset = cursor
        return Logs(total_lines=total, lines=lines)


def _enum_name(enum, value: int) -> str:
    # Proto enums are open: a newer server can send a value this client has no name for.
    return enum.Name(value).lower() if value in enum.values() else "unknown"


def _expand(seg_starts, seg_lens):
    return (i for s, n in zip(seg_starts, seg_lens) for i in range(s, s + n))


def _decode_points(series) -> list[tuple[int, float]]:
    """``(column, value)`` pairs of one ChartSeries, ascending, with logged NaN/inf restored from the marker channel.

    A marker column usually has no value entry, but a duplicate-x collision keeps both, so the channels are decoded separately and such a column yields two pairs.
    """
    points = list(zip(_expand(series.seg_starts, series.seg_lens), series.values))
    kinds = series.nan_kinds or [1] * len(series.nan_indices)
    points += [
        (c, _NONFINITE[k]) for c, k in zip(series.nan_indices, kinds) if k in _NONFINITE
    ]
    # Stable: a collision's finite value stays ahead of its marker.
    points.sort(key=lambda p: p[0])
    return points


def _decode_band(series) -> dict[int, tuple[float, float]]:
    """Column to ``(min, max)`` where the envelope differs from the value."""
    return {
        c: (lo, hi)
        for c, lo, hi in zip(
            _expand(series.band_seg_starts, series.band_seg_lens),
            series.band_min,
            series.band_max,
        )
    }
