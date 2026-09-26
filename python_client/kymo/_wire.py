"""Shared validation and wire helpers for live upload and spool replay."""

import math
import struct
import time
from typing import Optional

import grpc

from kymo._cdn import content_id
from kymo._generated import kymo_pb2
from kymo._log import logger as _log

# Mirror the server's ingest-boundary limits. The server can ACK a stream while
# skipping an unstorable metric name, so the client rejects it before queuing.
_MAX_METRIC_NAME_BYTES = 2048
_MAX_ID_BYTES = 256
_MAX_RICH_RESOURCE_ID_BYTES = 2048
_RESERVED_PROJECT_ID = "trash"

_SEND_MAX_RETRIES = 5
_SEND_INITIAL_DELAY = 0.5
_SEND_MAX_DELAY = 5.0
_SEND_CALL_TIMEOUT = 120.0
_CDN_RPC_TIMEOUT = 30.0
_MAX_POINTS_PER_MSG = 5_000
_MAX_BYTES_PER_MSG = 2 * 1024 * 1024
_MAX_POINT_BYTES = 4 * 1024 * 1024 - 64 * 1024


class _PermanentPointError(ValueError):
    """A point that retrying cannot make encodable or fit on the wire."""


class _TerminalRunError(RuntimeError):
    """The server terminally rejected this session's queued run data."""


class _RichMutationDataLoss(RuntimeError):
    """A versioned rich mutation conflicts with authoritative server state."""


def _is_terminal_run_error(error: BaseException) -> bool:
    """Return whether queued data must never be retried for this run."""
    code = error.code() if hasattr(error, "code") else None
    return code in (
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.NOT_FOUND,
    )


def _terminal_run_error(error: BaseException) -> _TerminalRunError:
    details = error.details() if hasattr(error, "details") else str(error)
    return _TerminalRunError(details or "run is no longer writable")


def _publish_rich_mutation(
    stub,
    project_id: str,
    run_id: str,
    metric_name: str,
    step: int,
    cdn_key: str,
    timestamp_ms: int,
    mutation_version: int,
) -> bool:
    """Publish one versioned rich head; every successful disposition is final."""
    if (
        type(mutation_version) is not int
        or not 0 < mutation_version < (1 << 64)
        or mutation_version >> 32 == 0
        or mutation_version & 0xFFFFFFFF == 0
    ):
        raise ValueError(
            "mutation version must contain nonzero uint32 epoch and sequence fields"
        )
    try:
        response = stub.PublishRichMutation(
            kymo_pb2.PublishRichMutationRequest(
                project_id=project_id,
                run_id=run_id,
                metric_name=metric_name,
                step=step,
                cdn_key=cdn_key,
                timestamp_ms=timestamp_ms,
                mutation_version=mutation_version,
            ),
            timeout=_CDN_RPC_TIMEOUT,
        )
    except grpc.RpcError as error:
        if _is_terminal_run_error(error):
            raise _terminal_run_error(error) from error
        if error.code() == grpc.StatusCode.DATA_LOSS:
            details = error.details() if hasattr(error, "details") else str(error)
            raise _RichMutationDataLoss(
                details or "rich mutation conflicts with authoritative state"
            ) from error
        raise
    if response.disposition not in (
        kymo_pb2.RICH_MUTATION_ACCEPTED,
        kymo_pb2.RICH_MUTATION_IDEMPOTENT,
        kymo_pb2.RICH_MUTATION_SUPERSEDED,
    ):
        raise RuntimeError(
            f"server returned unknown rich mutation disposition {response.disposition}"
        )
    if not response.HasField("stored_version"):
        raise RuntimeError("server omitted the authoritative rich mutation version")
    if response.disposition == kymo_pb2.RICH_MUTATION_SUPERSEDED:
        if response.stored_version <= mutation_version:
            raise RuntimeError(
                "server returned an invalid superseding mutation version"
            )
        _log.warning(
            "rich mutation %s step %s version %s was superseded by version %s",
            metric_name,
            step,
            mutation_version,
            response.stored_version,
        )
    elif response.stored_version != mutation_version:
        raise RuntimeError("server returned an inconsistent rich mutation version")
    return True


def _validate_ident(what: str, value: str, max_bytes: int) -> None:
    if "\x00" in value:
        raise ValueError(f"{what} contains a NUL byte: {value!r}")
    if len(value.encode("utf-8")) > max_bytes:
        raise ValueError(f"{what} exceeds {max_bytes} bytes: {value[:80]!r}…")


def _validate_project_id(project_id: str) -> None:
    _validate_ident("project_id", project_id, _MAX_ID_BYTES)
    if project_id == _RESERVED_PROJECT_ID:
        raise ValueError(f"project_id {_RESERVED_PROJECT_ID!r} is reserved")


def _upload_to_cdn(
    http_client,
    cdn_url: str,
    data: bytes,
    ext: str,
    *,
    expected_id: Optional[str] = None,
) -> str:
    """Upload bytes to CDN and verify the returned content identity."""
    response = http_client.post(
        f"{cdn_url.rstrip('/')}/cdn/upload",
        content=data,
        headers={"X-Extension": ext, "Content-Type": "application/octet-stream"},
    )
    response.raise_for_status()
    resource_id = response.json()["resource_id"]
    if expected_id is None:
        expected_id = content_id(data, ext)
    if resource_id != expected_id:
        raise RuntimeError(f"CDN returned {resource_id!r}, expected {expected_id!r}")
    return resource_id


def _is_permanent_upload_error(error: Exception) -> bool:
    """Return whether a CDN validation rejection is permanently unretryable."""
    status = getattr(getattr(error, "response", None), "status_code", None)
    return status is not None and 400 <= status < 500 and status not in (408, 425, 429)


def _normalize_numeric_value(value) -> float:
    """Preserve intentional markers, but reject finite values f32 would overflow."""
    numeric = float(value)
    try:
        encoded = struct.unpack("<f", struct.pack("<f", numeric))[0]
    except OverflowError:
        if math.isfinite(numeric):
            raise OverflowError(
                f"finite numeric metric value {numeric!r} exceeds protobuf float32 range"
            ) from None
        raise
    if math.isfinite(numeric) and not math.isfinite(encoded):
        raise OverflowError(
            f"finite numeric metric value {numeric!r} exceeds protobuf float32 range"
        )
    return numeric


def _tuple_to_point(point_tuple: tuple) -> kymo_pb2.MetricPoint:
    kind = point_tuple[0]
    if kind == "numeric_ts":
        _, name, step, value, timestamp_ms = point_tuple
        return kymo_pb2.MetricPoint(
            metric_name=name,
            step=step,
            value=_normalize_numeric_value(value),
            timestamp_ms=timestamp_ms,
        )
    if kind == "numeric_tagged_ts":
        _, name, step, value, tag, timestamp_ms = point_tuple
        return kymo_pb2.MetricPoint(
            metric_name=name,
            step=step,
            value=_normalize_numeric_value(value),
            tag=tag,
            timestamp_ms=timestamp_ms,
        )
    if kind == "text_ts":
        _, name, step, value, timestamp_ms = point_tuple
        return kymo_pb2.MetricPoint(
            metric_name=name,
            step=step,
            text_data=value.encode("utf-8"),
            timestamp_ms=timestamp_ms,
        )
    if kind == "cdn_ts":
        _, name, step, value, timestamp_ms = point_tuple
        return kymo_pb2.MetricPoint(
            metric_name=name, step=step, cdn_key=value, timestamp_ms=timestamp_ms
        )
    _, name, step, value = point_tuple
    return kymo_pb2.MetricPoint(metric_name=name, step=step, cdn_key=value)


def _point_sort_key(point_tuple: tuple) -> tuple:
    """Point components of the ClickHouse key; project/run are file-scoped."""
    tag = point_tuple[4] if point_tuple[0] == "numeric_tagged_ts" else ""
    return point_tuple[1], tag, point_tuple[2]


def _estimate_tuple_bytes(point_tuple: tuple) -> int:
    """Conservative encoded size of one point tuple, including wire slack."""
    return 64 + sum(
        len(value.encode("utf-8")) if isinstance(value, str) else len(value)
        for value in point_tuple
        if isinstance(value, (str, bytes, bytearray))
    )


def _truncate_text_tuple(point_tuple: tuple) -> tuple[tuple, int]:
    """Trim an old oversized text spool record to a wire-safe tail."""
    _, name, step, value, timestamp_ms = point_tuple
    data = value.encode("utf-8")
    fixed_bytes = _estimate_tuple_bytes(("text_ts", name, step, "", timestamp_ms))
    budget = max(0, _MAX_POINT_BYTES - fixed_bytes - 128)
    if len(data) > budget:
        tail = data[-budget:].decode("utf-8", errors="ignore") if budget else ""
        value = (
            f"[kymo: {len(value) - len(tail)} chars dropped to fit the "
            f"server's message limit]\n{tail}"
        )
        point_tuple = ("text_ts", name, step, value, timestamp_ms)
    return point_tuple, _estimate_tuple_bytes(point_tuple)


def _chunk_tuples(tuples: list[tuple], tuple_sizes: Optional[list[int]] = None):
    """Yield count- and byte-capped encoded point chunks."""
    if tuple_sizes is not None and len(tuple_sizes) != len(tuples):
        raise ValueError("tuple_sizes must correspond one-to-one with tuples")

    points: list = []
    chunk_bytes = 0
    count = 0
    for index, point_tuple in enumerate(tuples):
        try:
            point_bytes = (
                tuple_sizes[index]
                if tuple_sizes is not None
                else _estimate_tuple_bytes(point_tuple)
            )
            kind = point_tuple[0]
            if point_bytes > _MAX_POINT_BYTES:
                if kind == "text_ts":
                    point_tuple, point_bytes = _truncate_text_tuple(point_tuple)
                else:
                    raise _PermanentPointError(
                        f"{kind!r} record is too large to ingest "
                        f"({point_bytes} encoded bytes; limit {_MAX_POINT_BYTES})"
                    )
            point = _tuple_to_point(point_tuple)
        except _PermanentPointError:
            raise
        except (
            IndexError,
            TypeError,
            ValueError,
            OverflowError,
            UnicodeError,
        ) as error:
            kind = point_tuple[0] if point_tuple else "<empty>"
            raise _PermanentPointError(
                f"{kind!r} record cannot be encoded: {error}"
            ) from error
        if points and (
            len(points) >= _MAX_POINTS_PER_MSG
            or chunk_bytes + point_bytes > _MAX_BYTES_PER_MSG
        ):
            yield points, count
            points, chunk_bytes, count = [], 0, 0
        points.append(point)
        chunk_bytes += point_bytes
        count += 1
    if points:
        yield points, count


def _send_tuples(stub, project_id: str, run_id: str, tuples: list[tuple]) -> bool:
    """Send tuple batches over one unary IngestMetrics call."""
    if not stub or not tuples:
        return False

    batches = [
        kymo_pb2.MetricsBatch(project_id=project_id, run_id=run_id, points=points)
        for points, _ in _chunk_tuples(tuples)
    ]
    delay = _SEND_INITIAL_DELAY
    for attempt in range(1, _SEND_MAX_RETRIES + 1):
        try:
            response = stub.IngestMetrics(iter(batches), timeout=_SEND_CALL_TIMEOUT)
        except grpc.RpcError as error:
            if _is_terminal_run_error(error):
                raise _terminal_run_error(error) from error
            details = error.details() if hasattr(error, "details") else str(error)
            _log.info(
                "send attempt %d/%d failed: %s",
                attempt,
                _SEND_MAX_RETRIES,
                details,
            )
        else:
            if response.points_received != len(tuples):
                _log.warning(
                    "server accepted %d of %d points — the rest were "
                    "unstorable (metric name or timestamp) and were dropped "
                    "(see server logs)",
                    response.points_received,
                    len(tuples),
                )
                return False
            return True
        if attempt < _SEND_MAX_RETRIES:
            time.sleep(delay)
            delay = min(delay * 2, _SEND_MAX_DELAY)

    _log.error(
        "failed to send %d points after %d retries", len(tuples), _SEND_MAX_RETRIES
    )
    return False
