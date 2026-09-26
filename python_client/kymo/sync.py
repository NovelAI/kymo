"""Replay spooled kymo metrics from any (CPU-only) machine.

When a training run exits with points the server never accepted, the client
spools them to disk (see kymo.spool). This tool delivers them later —
from a login node or any box that can reach the server — so a finished GPU
job never has to stay alive just to finish uploading.

Usage:
    python -m kymo.sync                    # replay every *.mkspool in the spool dir
    python -m kymo.sync FILE [FILE ...]    # replay specific spool files
    python -m kymo.sync --server HOST:PORT --cdn http://HOST:8080 FILE

Successfully replayed files are renamed to <name>.sent (use --delete to
remove them instead). A file that partially fails is kept as-is; replaying
it again is safe — kymo's storage deduplicates identical points. Legacy
CDN/metadata records conservatively keep an existing real value; versioned
records use the server's persisted logical ordering. Permanently unencodable
files are preserved as <name>.rejected and removed from automatic replay. The
exit code is 0 only when nothing remains pending: kept, skipped, or quarantined
files exit 1.

Spool pickle globals are disabled, but the legacy stream is not
resource-bounded. Older records containing datetime, NumPy, or custom objects
require explicit trusted migration; normal sync quarantines them. Replay only
files from a trusted spool directory.

Runtime floor: the committed protobuf stubs need protobuf>=6.33.5 and
grpcio>=1.81.0 (see pyproject.toml). Docker builds enforce this, but this
replay tool is meant to run from any CPU/login node where the interpreter may
be installed ad hoc — a stale protobuf there fails at import, not at build. Fix
by upgrading (`pip install -U 'protobuf>=6.33.5' 'grpcio>=1.81.0'`); the
protobuf runtime check can be bypassed in a pinch with
TEMPORARILY_DISABLE_PROTOBUF_VERSION_CHECK=true.
"""

import argparse
import os
import stat
import sys
import time
import uuid

import grpc

from kymo._cdn import content_id, gallery_item, gallery_manifest, metadata_manifest
from kymo._env import reject_legacy_client_env, string as _env_string
from kymo._generated import kymo_pb2, kymo_pb2_grpc
from kymo._log import logger as _log
from kymo._wire import (
    _PermanentPointError,
    _TerminalRunError,
    _CDN_RPC_TIMEOUT,
    _MAX_ID_BYTES,
    _MAX_METRIC_NAME_BYTES,
    _MAX_RICH_RESOURCE_ID_BYTES,
    _RichMutationDataLoss,
    _chunk_tuples,
    _estimate_tuple_bytes,
    _is_permanent_upload_error,
    _is_terminal_run_error,
    _publish_rich_mutation,
    _terminal_run_error,
    _point_sort_key,
    _send_tuples,
    _upload_to_cdn,
    _validate_ident,
    _validate_project_id,
)
from kymo.spool import (
    SPOOL_SUFFIX,
    SpoolCorruptionError,
    UnsupportedSpoolVersion,
    default_replay_dirs,
    read_spool,
    read_spool_header,
    read_spool_header_file,
    quarantine_spool,
    retire_deleted_spool,
    retire_sent_spool,
    sync_after_retirement,
    writer_active as _writer_active,
)

_POINT_KINDS = ("numeric_ts", "numeric_tagged_ts", "text_ts", "cdn_ts", "cdn")
_VERSIONED_RICH_KINDS = (
    "cdn_key_mutation",
    "cdn_batch_encoded_mutation",
    "cdn_batch_encoded_mutation_reserved",
    "metadata_json_mutation",
)
_BATCH_POINTS = 100_000  # tuples accumulated before each IngestMetrics call
_BATCH_BYTES = 32 * 1024 * 1024
_MIN_TIMESTAMP_MS = -200_000_000_000_000
_MAX_TIMESTAMP_MS = 9_000_000_000_000_000


class _DirectoryCandidateChanged(RuntimeError):
    pass


def _causal_spool_key(
    path: str, directory_identity: tuple[int, int] | None = None
) -> tuple:
    """Order files within a run by creation, with worker before salvage ties."""
    try:
        if directory_identity is None:
            header = read_spool_header(path)
        else:
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            try:
                fd = os.open(path, flags)
            except OSError as error:
                raise _DirectoryCandidateChanged(path) from error
            try:
                metadata = os.fstat(fd)
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or (
                        metadata.st_dev,
                        metadata.st_ino,
                    )
                    != directory_identity
                ):
                    raise _DirectoryCandidateChanged(path)
                with os.fdopen(fd, "rb", closefd=False) as fh:
                    header = read_spool_header_file(fh, path=path)
            finally:
                os.close(fd)
        created_ns = int(
            header.get("created_unix_ns", int(header.get("created_unix", 0)) * 10**9)
        )
        name = os.path.basename(path)
        label = name.rsplit("__", 1)[-1]
        phase = (
            0
            if label.startswith("worker_")
            else 1
            if label.startswith("salvage_")
            else 2
        )
        return (
            0,
            str(header.get("project_id", "")),
            str(header.get("run_id", "")),
            created_ns,
            phase,
            name,
        )
    except _DirectoryCandidateChanged:
        raise
    except Exception:
        # replay_file reports the useful parse error; keep bad files last here.
        return (1, "", "", 0, 0, os.path.basename(path))


def _effective_server(header: dict, override: str = "") -> str:
    target_kind = header.get("target_kind")
    if target_kind not in (None, "local"):
        raise ValueError(f"unsupported spool target kind {target_kind!r}")
    if target_kind == "local":
        if override:
            raise ValueError("--server cannot override a local-runtime spool")
        installation_uuid = header.get("installation_uuid")
        if not isinstance(installation_uuid, str):
            raise TypeError("local spool has no installation UUID")
        try:
            installation_uuid = str(uuid.UUID(installation_uuid))
        except ValueError as error:
            raise ValueError("local spool has an invalid installation UUID") from error
        return f"local:{installation_uuid}"
    server = (
        override
        or header.get("server_address")
        or _env_string("KYMO_SERVER", "MKDB2_SERVER")
    )
    if not isinstance(server, str):
        raise TypeError("server address must be a string")
    return server


def _spool_run_key(path: str, server_override: str = "") -> tuple[str, str, str]:
    """Return the causal replay identity recorded by one spool file."""
    header = read_spool_header(path)
    return (
        _effective_server(header, server_override),
        str(header.get("project_id", "")),
        str(header.get("run_id", "")),
    )


def _quarantine_if_unchanged(path: str, size_before: int, reason: str) -> bool:
    """Quarantine static bad input; retain a file that may still be growing."""
    if os.stat(path).st_size != size_before:
        print(f"    invalid input ({reason}), but file grew — kept (re-sync is safe)")
        return False
    print(f"    permanently invalid — quarantined: {reason}")
    rejected_path = quarantine_spool(path)
    print(
        f"    source quarantined as {os.path.basename(rejected_path)} "
        "without sending any records"
    )
    return False


def _quarantine_after_data_loss(path: str, size_before: int, reason: str) -> str | None:
    """Quarantine one static spool after an authoritative mutation conflict."""
    if os.stat(path).st_size != size_before:
        print(
            f"    DATA_LOSS ({reason}), but file grew — kept and blocked "
            "(stop its writer before investigating)"
        )
        return None
    rejected_path = quarantine_spool(path)
    print(f"    DATA_LOSS — quarantined as {os.path.basename(rejected_path)}: {reason}")
    print(
        "    earlier records may already be delivered; the remainder of this "
        "file needs investigation, while later spool files remain eligible"
    )
    return rejected_path


def _retire_deleted(path: str, size_before: int, reason: str) -> bool:
    """Retire data rejected for this run lifecycle from automatic replay."""
    grew = os.stat(path).st_size != size_before
    deleted_path = retire_deleted_spool(path)
    print(
        f"    run is no longer writable — retired as "
        f"{os.path.basename(deleted_path)}: {reason}"
        + (" (the file grew during this attempt)" if grew else "")
    )
    return False


def _retire_sent(path: str, size_before: int, *, delete: bool) -> bool:
    """Retire one fully consumed snapshot unless a live writer extended it."""
    if os.stat(path).st_size != size_before:
        print("    file grew during replay — kept (re-sync is safe)")
        return False
    if delete:
        os.remove(path)
        sync_after_retirement(path)
    else:
        retire_sent_spool(path)
    return True


def _directory_candidate_unchanged(path: str, identity: tuple[int, int]) -> bool:
    try:
        metadata = os.lstat(path)
    except OSError:
        return False
    return (
        stat.S_ISREG(metadata.st_mode)
        and (
            metadata.st_dev,
            metadata.st_ino,
        )
        == identity
    )


def _connect(server_address: str, local_endpoint=None):
    if local_endpoint is None:
        channel = grpc.insecure_channel(server_address)
    else:
        from kymo._local_runtime import grpc_channel

        channel = grpc_channel(local_endpoint)
    try:
        grpc.channel_ready_future(channel).result(timeout=15)
        stub = kymo_pb2_grpc.KymoStub(channel)
    except BaseException:
        channel.close()
        raise
    return channel, stub


def _current_cdn_key(stub, project_id, run_id, metric_name, step):
    """Current cdn_key at (metric, step): the key, "" if the step is empty, None if the read failed (unknown)."""
    try:
        resp = stub.QueryCdnKeys(
            kymo_pb2.QueryCdnKeysRequest(
                refs=[
                    kymo_pb2.SeriesRef(
                        project_id=project_id, run_id=run_id, metric_name=metric_name
                    )
                ],
                step_min=step,
                step_max=step,
            ),
            timeout=_CDN_RPC_TIMEOUT,
        )
        for series in resp.series:
            for entry in series.entries:
                if entry.step == step:
                    return entry.cdn_key
        return ""
    except grpc.RpcError as e:
        if _is_terminal_run_error(e):
            raise _terminal_run_error(e) from e
        _log.warning("QueryCdnKeys failed for %s step %s: %s", metric_name, step, e)
        return None


def _gallery_item(entry: dict, resource_id: str) -> dict:
    if entry["kind"] == "image":
        return gallery_item(
            resource_id,
            extension=entry["ext"],
            caption=entry.get("caption"),
        )
    return gallery_item(
        resource_id,
        content_type=entry.get("content_type", "application/octet-stream"),
        filename=entry.get("filename", ""),
    )


def _replay_cdn_record(
    stub,
    http_client,
    cdn_url,
    project_id,
    run_id,
    record,
    replayed_keys: dict[tuple, str],
) -> bool:
    kind, name, step = record[0], record[1], record[2]
    versioned = kind in _VERSIONED_RICH_KINDS
    if kind == "cdn_key_mutation":
        _, _, _, resource_id, timestamp_ms, mutation_version = record
        try:
            return _publish_rich_mutation(
                stub,
                project_id,
                run_id,
                name,
                step,
                resource_id,
                timestamp_ms,
                mutation_version,
            )
        except (_TerminalRunError, _RichMutationDataLoss):
            raise
        except grpc.RpcError as error:
            if _is_terminal_run_error(error):
                raise _terminal_run_error(error) from error
            _log.error(
                "failed to replay rich mutation %s step %s: %s", name, step, error
            )
            return False
        except Exception as error:
            _log.error(
                "failed to replay rich mutation %s step %s: %s", name, step, error
            )
            return False

    storage_key = (project_id, run_id, name, step)
    # Legacy records lack logical versions, so retain their conservative
    # server-wins guard and its known recency limitation. Versioned records
    # bypass both probes below and let the authoritative server CAS decide.
    try:
        if kind in ("metadata_json", "metadata_json_mutation"):
            prospective_bytes = metadata_manifest(record[3])
            expected_resources = []
        else:
            expected_resources = [
                (entry, content_id(entry["data"], entry["ext"])) for entry in record[3]
            ]
            prospective_bytes = gallery_manifest(
                [
                    _gallery_item(entry, expected_id)
                    for entry, expected_id in expected_resources
                ]
            )
        prospective_id = content_id(prospective_bytes, "json")
        if not versioned:
            current = _current_cdn_key(stub, project_id, run_id, name, step)
            if current is None:
                return False
            if (
                current
                and not current.startswith("pending:")
                and replayed_keys.get(storage_key) != current
            ):
                if current == prospective_id:
                    replayed_keys[storage_key] = current
                    return True
                print(
                    f"    {name} step {step}: server already has newer data — "
                    "record skipped"
                )
                return True

        manifest_changed = False
        if kind not in ("metadata_json", "metadata_json_mutation"):
            items = []
            for entry, expected_id in expected_resources:
                try:
                    resource_id = _upload_to_cdn(
                        http_client,
                        cdn_url,
                        entry["data"],
                        entry["ext"],
                        expected_id=expected_id,
                    )
                except Exception as e:
                    # 4xx = the CDN will reject this item on every future replay too — dropping it is the only way the rest of the record (and file) can ever finish.
                    if _is_permanent_upload_error(e):
                        _log.warning(
                            "CDN rejected spooled item (%s step %s) — dropping it: %s",
                            name,
                            step,
                            e,
                        )
                        manifest_changed = True
                        continue
                    raise
                items.append(_gallery_item(entry, resource_id))
        if manifest_changed:
            # Even with zero usable items, log an EMPTY manifest: the original run may have left a pending:* placeholder at this step, and only a real manifest clears it (see _process_cdn_batch).
            encoded_manifest = gallery_manifest(items)
            manifest_id = content_id(encoded_manifest, "json")
        else:
            encoded_manifest = prospective_bytes
            manifest_id = prospective_id
        # Uploading a content-addressed manifest is harmless until its key is
        # published. Do that potentially slow work first, then recheck every
        # rich kind immediately before the final point write.
        _upload_to_cdn(
            http_client,
            cdn_url,
            encoded_manifest,
            "json",
            expected_id=manifest_id,
        )
        if versioned:
            timestamp_ms, mutation_version = record[4], record[5]
            if manifest_changed:
                reduced_mutation_version = (
                    record[6] if kind == "cdn_batch_encoded_mutation_reserved" else None
                )
                if reduced_mutation_version is None:
                    raise _RichMutationDataLoss(
                        "reduced gallery has no causally reserved mutation identity"
                    )
                mutation_version = reduced_mutation_version
            return _publish_rich_mutation(
                stub,
                project_id,
                run_id,
                name,
                step,
                manifest_id,
                timestamp_ms,
                mutation_version,
            )

        current = _current_cdn_key(stub, project_id, run_id, name, step)
        if current is None:
            return False
        if current and not current.startswith("pending:"):
            if current == manifest_id:
                replayed_keys[storage_key] = current
                return True
            if replayed_keys.get(storage_key) != current:
                print(
                    f"    {name} step {step}: server already has newer data — "
                    "record skipped"
                )
                return True
        point = kymo_pb2.MetricPoint(
            metric_name=name,
            step=step,
            cdn_key=manifest_id,
            timestamp_ms=int(time.time() * 1000),
        )
        response = stub.IngestMetrics(
            iter(
                [
                    kymo_pb2.MetricsBatch(
                        project_id=project_id, run_id=run_id, points=[point]
                    )
                ]
            ),
            timeout=_CDN_RPC_TIMEOUT,
        )
        if response.points_received != 1:
            _log.error(
                "server accepted %d of 1 rich replay point; file kept",
                response.points_received,
            )
            return False
        replayed_keys[storage_key] = manifest_id
        return True
    except (_TerminalRunError, _RichMutationDataLoss):
        raise
    except grpc.RpcError as e:
        if _is_terminal_run_error(e):
            raise _terminal_run_error(e) from e
        _log.error("failed to replay CDN record %s step %s: %s", name, step, e)
        return False
    except Exception as e:
        _log.error("failed to replay CDN record %s step %s: %s", name, step, e)
        return False


def _validate_spool_record(record, known_kinds: set[str]) -> str:
    """Return a record kind or raise for a permanently malformed record."""
    if not isinstance(record, tuple) or not record or not isinstance(record[0], str):
        raise _PermanentPointError(
            "record must be a non-empty tuple with a string kind"
        )
    kind = record[0]
    if kind not in known_kinds:
        return kind
    try:
        name = record[1]
        if not isinstance(name, str):
            raise TypeError("metric name must be a string")
        _validate_ident("metric name", name, _MAX_METRIC_NAME_BYTES)
        if kind in _POINT_KINDS:
            next(_chunk_tuples([record]), None)
            if kind != "cdn" and not (
                _MIN_TIMESTAMP_MS <= record[-1] <= _MAX_TIMESTAMP_MS
            ):
                raise ValueError("timestamp is outside the server's storable range")
            return kind
        if kind in _VERSIONED_RICH_KINDS:
            expected_length = 7 if kind == "cdn_batch_encoded_mutation_reserved" else 6
            if len(record) != expected_length:
                raise ValueError(
                    f"expected {expected_length} fields, got {len(record)}"
                )
            _, name, step, payload, timestamp_ms, mutation_version = record[:6]
            if not (_MIN_TIMESTAMP_MS <= timestamp_ms <= _MAX_TIMESTAMP_MS):
                raise ValueError("timestamp is outside the server's storable range")
            if (
                type(mutation_version) is not int
                or not 0 < mutation_version < (1 << 64)
                or mutation_version >> 32 == 0
                or mutation_version & 0xFFFFFFFF == 0
            ):
                raise ValueError(
                    "mutation version must contain nonzero uint32 epoch and sequence fields"
                )
            if kind == "cdn_batch_encoded_mutation_reserved":
                reduced_mutation_version = record[6]
                if (
                    type(reduced_mutation_version) is not int
                    or not (0 < reduced_mutation_version < 1 << 64)
                    or reduced_mutation_version != mutation_version + 1
                    or reduced_mutation_version >> 32 != mutation_version >> 32
                ):
                    raise ValueError(
                        "reduced mutation version must be the reserved successor"
                    )
        else:
            if len(record) != 4:
                raise ValueError(f"expected 4 fields, got {len(record)}")
            _, name, step, payload = record
        # Reuse protobuf conversion to validate the shared name/step fields.
        next(_chunk_tuples([("cdn_ts", name, step, "validation", 0)]), None)
        if kind == "cdn_key_mutation":
            if not isinstance(payload, str):
                raise TypeError("CDN resource id must be a string")
            _validate_ident("CDN resource id", payload, _MAX_RICH_RESOURCE_ID_BYTES)
            return kind
        if kind in ("metadata_json", "metadata_json_mutation"):
            if not isinstance(payload, dict):
                raise TypeError("metadata payload must be a dict")
            metadata_manifest(payload)
            return kind
        if not isinstance(payload, list):
            raise TypeError("encoded CDN payload must be a list")
        for entry in payload:
            if not isinstance(entry, dict):
                raise TypeError("encoded CDN entries must be dicts")
            entry_kind = entry.get("kind")
            if entry_kind not in ("image", "resource"):
                raise ValueError(f"unsupported encoded CDN kind {entry_kind!r}")
            data = entry["data"]
            extension = entry["ext"]
            if not isinstance(data, bytes) or not isinstance(extension, str):
                raise TypeError("encoded CDN data/ext must be bytes/str")
            token_chars = "!#$%&'*+-.^_`|~"
            if (
                not extension
                or not extension.isascii()
                or not all(char.isalnum() or char in token_chars for char in extension)
            ):
                raise ValueError("encoded CDN extension must be an ASCII token")
            if entry_kind == "image":
                caption = entry.get("caption")
                if caption is not None and not isinstance(caption, str):
                    raise TypeError("encoded image caption must be a string or null")
            elif not isinstance(
                entry.get("content_type", "application/octet-stream"), str
            ) or not isinstance(entry.get("filename", ""), str):
                raise TypeError(
                    "encoded resource content_type/filename must be strings"
                )
    except _PermanentPointError:
        raise
    except MemoryError:
        raise
    except Exception as error:
        raise _PermanentPointError(
            f"{kind!r} record cannot be encoded: {error}"
        ) from error
    return kind


def replay_file(
    path: str,
    server: str = "",
    cdn: str = "",
    delete: bool = False,
    replayed_keys: dict[tuple, str] | None = None,
    terminal_runs: set[tuple[str, str, str]] | None = None,
    quarantined_files: set[str] | None = None,
    *,
    _writer_lock_held: bool = False,
    _local_endpoint=None,
) -> bool:
    """Replay one spool file; return False for pending or quarantined input.

    ``_writer_lock_held`` is for the live worker's sealed-spool handoff only:
    that worker owns the still-open ``SpoolWriter`` descriptor, so external sync
    processes see the file as active while this replay safely reads its immutable
    contents.
    """
    # A run's worker keeps its spool open (and appends to it) for the run's whole lifetime. Replaying to the current EOF and renaming to .sent would strand every later append in the .sent inode where no sync looks — skip live files entirely.
    if not _writer_lock_held and _writer_active(path):
        print(
            f"  {os.path.basename(path)}: still open by a live run — skipped (sync again after it exits)"
        )
        return False
    size_before = os.stat(path).st_size
    header = None
    try:
        header = read_spool_header(path, require_supported=True)
        if header.get("target_kind") == "local" and server:
            raise ValueError("--server cannot override a local-runtime spool")
        project_id = header["project_id"]
        run_id = header["run_id"]
        if not isinstance(project_id, str) or not isinstance(run_id, str):
            raise TypeError("project_id/run_id must be strings")
        _validate_project_id(project_id)
        _validate_ident("run_id", run_id, _MAX_ID_BYTES)
        destination = _effective_server(header, server)
    except UnsupportedSpoolVersion as error:
        print(f"    {error} — upgrade kymo; file kept")
        return False
    except (MemoryError, OSError):
        raise
    except Exception as error:
        if header is not None and header.get("target_kind") == "local" and server:
            raise
        return _quarantine_if_unchanged(path, size_before, f"invalid header: {error}")
    local_installation_uuid = (
        destination.removeprefix("local:")
        if header.get("target_kind") == "local"
        else ""
    )
    cdn_url = cdn or header.get("cdn_address", "")
    if not destination:
        print(f"  {path}: no server address in header — pass --server", file=sys.stderr)
        return False

    try:
        _, records = read_spool(path)
    except UnsupportedSpoolVersion as error:
        print(f"    {error} — upgrade kymo; file kept")
        return False
    except (MemoryError, OSError):
        raise
    except Exception as error:
        return _quarantine_if_unchanged(path, size_before, f"invalid header: {error}")

    print(
        f"  {os.path.basename(path)}: run {header.get('run_name', '?')!r} ({run_id[:8]}…) → {destination}"
    )

    # Pre-scan (replay is all-or-nothing; costs a second pass — sync is an offline tool):
    # 1. Reject record kinds this sync doesn't know (a newer kymo wrote the file): partially replaying and keeping the file for its unknown records would re-deliver the known ones on EVERY later sync, each re-insert taking a fresh server-side inserted_at — and last-insert-wins storage would let those stale points overwrite same-step values a resumed run has since re-logged.
    # 2. Reject permanently unencodable point records before sending anything.
    # Ordinary point rows are deduplicated only inside each bounded unary
    # window; windows replay serially. Rich records remain in causal order: a
    # content-address match lets a later record supersede one this replay owns.
    known_kinds = (
        set(_POINT_KINDS)
        | {"cdn_batch_encoded", "metadata_json"}
        | set(_VERSIONED_RICH_KINDS)
    )
    unknown_kinds = set()
    first_permanent_error: tuple[str, str] | None = None
    permanent_error_count = 0
    record_count = 0
    try:
        for r in records:
            record_count += 1
            try:
                kind = _validate_spool_record(r, known_kinds)
            except _PermanentPointError as error:
                kind = r[0] if isinstance(r, tuple) and r else "record"
                permanent_error_count += 1
                if first_permanent_error is None:
                    first_permanent_error = str(kind), str(error)
                continue
            if kind not in known_kinds:
                unknown_kinds.add(kind)
    except SpoolCorruptionError as error:
        permanent_error_count += 1
        if first_permanent_error is None:
            first_permanent_error = "spool", str(error)
    if first_permanent_error is not None:
        kind, error = first_permanent_error
        reason = f"{kind}: {error}"
        if permanent_error_count > 1:
            reason += f" ({permanent_error_count - 1} additional invalid record(s))"
        return _quarantine_if_unchanged(
            path,
            size_before,
            reason,
        )
    if unknown_kinds:
        print(
            f"    unknown record kinds {sorted(unknown_kinds)} — upgrade kymo to replay; nothing sent, file kept"
        )
        return False
    if record_count == 0:
        # A crash between the lazy writer's header and first record can leave a
        # valid empty segment. Retire it without waking a stopped local stack.
        print("    0 points delivered")
        return _retire_sent(path, size_before, delete=delete)
    local_endpoint = None
    if local_installation_uuid:
        if cdn:
            raise ValueError("--cdn cannot override a local-runtime spool")
        from kymo._local_runtime import (
            LocalInstallationMismatch,
            ensure_local_endpoint,
        )

        if _local_endpoint is not None:
            if _local_endpoint.installation_uuid != local_installation_uuid:
                raise RuntimeError(
                    "worker supplied an endpoint for the wrong installation"
                )
            local_endpoint = _local_endpoint
        else:
            try:
                local_endpoint = ensure_local_endpoint(
                    expected_installation_uuid=local_installation_uuid
                )
            except LocalInstallationMismatch as error:
                return _quarantine_if_unchanged(path, size_before, str(error))
        server_address = local_endpoint.grpc_target
        cdn_url = local_endpoint.upload_origin
    else:
        server_address = destination
    # The loop variable can retain a whole encoded gallery through the second
    # pass, doubling that record's footprint. Drop it before reopening the file.
    r = None
    channel, stub = _connect(server_address, local_endpoint)
    try:
        _, records = read_spool(path)  # second pass for delivery
    except BaseException:
        channel.close()
        raise
    http_client = None
    if replayed_keys is None:
        replayed_keys = {}

    sent = 0
    failed = 0
    superseded = 0
    pending: list[tuple] = []
    pending_bytes = 0

    def flush_pending() -> bool:
        nonlocal sent, failed, superseded, pending_bytes
        if not pending:
            return True
        seen = set()
        deduped = []
        for record in reversed(pending):
            key = _point_sort_key(record)
            if key not in seen:
                seen.add(key)
                deduped.append(record)
        deduped.reverse()
        superseded += len(pending) - len(deduped)
        if _send_tuples(stub, project_id, run_id, deduped):
            sent += len(deduped)
        else:
            failed += len(deduped)
        pending.clear()
        pending_bytes = 0
        return failed == 0

    ok = True
    terminal_error: str | None = None
    data_loss_error: str | None = None
    try:
        for record in records:
            kind = record[0]
            if kind in _POINT_KINDS:
                record_bytes = _estimate_tuple_bytes(record)
                if pending and (
                    len(pending) >= _BATCH_POINTS
                    or pending_bytes + record_bytes > _BATCH_BYTES
                ):
                    if not flush_pending():
                        ok = False
                        break
                pending.append(record)
                pending_bytes += record_bytes
            elif kind in (
                "cdn_batch_encoded",
                "metadata_json",
                *_VERSIONED_RICH_KINDS,
            ):
                if not flush_pending():
                    ok = False
                    break
                if kind != "cdn_key_mutation" and not cdn_url:
                    _log.error("spool has CDN records but no CDN address — pass --cdn")
                    failed += 1
                    ok = False
                    break
                if kind != "cdn_key_mutation" and http_client is None:
                    if local_endpoint is None:
                        import httpx

                        http_client = httpx.Client(timeout=60.0)
                    else:
                        from kymo._local_runtime import http_client as local_http

                        http_client = local_http(local_endpoint, timeout=60.0)
                if _replay_cdn_record(
                    stub,
                    http_client,
                    cdn_url,
                    project_id,
                    run_id,
                    record,
                    replayed_keys,
                ):
                    sent += 1
                else:
                    failed += 1
                    ok = False
                    break
            else:
                # The pre-scan rejects unknown kinds, but a flock-less live writer can append NEW records between the two passes — keep the file rather than .sent-ing records this pass never judged.
                failed += 1
                ok = False
                break
        if ok:
            ok = flush_pending()
    except _TerminalRunError as error:
        terminal_error = str(error)
        failed += len(pending) or 1
        ok = False
    except _RichMutationDataLoss as error:
        data_loss_error = str(error)
        failed += len(pending) or 1
        ok = False
    except SpoolCorruptionError as error:
        _log.error("spool changed or became corrupt during replay: %s", error)
        failed += 1
        ok = False
    finally:
        try:
            if http_client is not None:
                http_client.close()
        finally:
            try:
                records.close()
            finally:
                channel.close()

    if terminal_error is not None:
        if terminal_runs is not None:
            terminal_runs.add((destination, project_id, run_id))
        return _retire_deleted(path, size_before, terminal_error)

    if data_loss_error is not None:
        rejected_path = _quarantine_after_data_loss(path, size_before, data_loss_error)
        if rejected_path is not None and quarantined_files is not None:
            quarantined_files.add(rejected_path)
        return False

    print(
        f"    {sent} points delivered"
        + (
            f", {superseded} superseded by later same-step records"
            if superseded
            else ""
        )
        + (f", {failed} FAILED (file kept for retry)" if failed else "")
    )
    if ok:
        # Belt to the flock suspenders: a writer on a filesystem without flock
        # support that appended during replay shows up as growth.
        ok = _retire_sent(path, size_before, delete=delete)
    return ok


def main(argv=None) -> int:
    reject_legacy_client_env()
    parser = argparse.ArgumentParser(
        prog="python -m kymo.sync",
        description="Replay spooled kymo metrics that a training run could not deliver before exit.",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help=(
            "spool files or directories (default: "
            + ", ".join(default_replay_dirs())
            + ")"
        ),
    )
    parser.add_argument(
        "--server",
        default="",
        help="override the gRPC server address from the spool header",
    )
    parser.add_argument(
        "--cdn", default="", help="override the CDN address from the spool header"
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="delete replayed files instead of renaming to .sent",
    )
    args = parser.parse_args(argv)

    implicit_defaults = not args.paths
    paths = args.paths or default_replay_dirs()
    files: list[tuple[str, tuple[int, int] | None]] = []
    rejected_files: set[str] = set()
    for p in paths:
        if implicit_defaults:
            try:
                mode = os.stat(p).st_mode
            except FileNotFoundError:
                continue
            except OSError as error:
                print(f"cannot inspect default spool directory {p}: {error}")
                return 1
            if not stat.S_ISDIR(mode):
                print(f"default spool path is not a directory: {p}")
                return 1
        if os.path.isdir(p):
            try:
                with os.scandir(p) as entries:
                    entries = [
                        (entry, entry.stat(follow_symlinks=False))
                        for entry in entries
                        if entry.is_file(follow_symlinks=False)
                    ]
            except OSError as error:
                print(f"cannot scan spool directory {p}: {error}")
                return 1
            # A directory scan owns its regular spool entries, not arbitrary leaf symlinks. Following and retiring one could rename a target outside the directory; retiring only the alias leaves the real spool live. Explicit symlink arguments below remain supported.
            files.extend(
                (entry.path, (metadata.st_dev, metadata.st_ino))
                for entry, metadata in entries
                if entry.name.endswith(SPOOL_SUFFIX)
            )
            rejected_files.update(
                os.path.normcase(os.path.abspath(entry.path))
                for entry, _metadata in entries
                if f"{SPOOL_SUFFIX}.rejected" in entry.name
            )
        else:
            if f"{SPOOL_SUFFIX}.rejected" in os.path.basename(p):
                rejected_files.add(p)
            else:
                files.append((p, None))
    # Replay the object behind a symlink, not the link itself: replay_file retires the path it receives, and renaming only an alias would leave the delivered target ending in .mkspool for a later duplicate replay. Canonical paths also collapse overlapping directory/explicit aliases.
    unique_files: dict[tuple, tuple[str, bool, tuple[int, int] | None]] = {}
    # Distinct resolved names per inode: symlink+target collapse to one, hardlinks do not.
    alias_names: dict[tuple, set[str]] = {}
    for path, directory_identity in files:
        if directory_identity is not None and not _directory_candidate_unchanged(
            path, directory_identity
        ):
            print(f"directory spool entry changed after scan — aborting: {path}")
            return 1
        if directory_identity is not None:
            replay_path = os.path.abspath(path)
            is_leaf_symlink = False
            key = ("inode", *directory_identity)
        else:
            canonical = os.path.realpath(path)
            is_leaf_symlink = os.path.islink(path)
            replay_path = canonical if is_leaf_symlink else os.path.abspath(path)
            try:
                metadata = os.stat(canonical)
                key = ("inode", metadata.st_dev, metadata.st_ino)
            except OSError:
                key = ("path", os.path.normcase(canonical))
        alias_names.setdefault(key, set()).add(
            os.path.normcase(os.path.realpath(replay_path))
        )
        previous = unique_files.get(key)
        # Prefer a direct spelling when one was supplied. A symlink-only input still uses the resolved target so retirement cannot rename just the link and leave the delivered spool live.
        if previous is None or (previous[1] and not is_leaf_symlink):
            unique_files[key] = (replay_path, is_leaf_symlink, directory_identity)
    # Hardlinks are separate names for one spool: retiring the replayed name leaves the other live, and its later replay would overwrite newer values (ReplacingMergeTree keeps the latest insert). Refuse until only one name remains.
    for names in alias_names.values():
        if len(names) > 1:
            print(
                "hardlinked spool aliases would replay again after one name is retired — "
                "remove all but one and rerun: " + ", ".join(sorted(names))
            )
            return 1
    files = [
        (path, directory_identity)
        for path, _is_leaf_symlink, directory_identity in unique_files.values()
    ]
    # Causal order is global across directories and explicit paths alike.
    try:
        keyed_files = [
            (_causal_spool_key(path, directory_identity), path, directory_identity)
            for path, directory_identity in files
        ]
    except _DirectoryCandidateChanged as error:
        print(
            f"directory spool entry changed while reading its header — aborting: {error}"
        )
        return 1
    keyed_files.sort(key=lambda item: item[0])
    files = [
        (path, directory_identity) for _key, path, directory_identity in keyed_files
    ]
    for path, directory_identity in files:
        if directory_identity is not None and not _directory_candidate_unchanged(
            path, directory_identity
        ):
            print(f"directory spool entry changed after ordering — aborting: {path}")
            return 1

    if not files:
        if rejected_files:
            print(
                f"nothing replayable; {len(rejected_files)} quarantined spool file(s) "
                "retained for investigation"
            )
            return 1
        print(f"nothing to sync (searched: {', '.join(paths)})")
        return 0

    print(f"syncing {len(files)} spool file(s)…")
    all_ok = not rejected_files
    if rejected_files:
        print(
            f"retaining {len(rejected_files)} quarantined spool file(s) for "
            "investigation; later files remain eligible"
        )
    blocked_runs: set[tuple[str, str, str]] = set()
    terminal_runs: set[tuple[str, str, str]] = set()
    replayed_keys_by_server: dict[str, dict[tuple, str]] = {}
    for f, directory_identity in files:
        if directory_identity is not None and not _directory_candidate_unchanged(
            f, directory_identity
        ):
            print(f"  {f}: directory entry changed before replay — aborting")
            all_ok = False
            break
        run_key = None
        replayed_keys = None
        try:
            try:
                run_key = _spool_run_key(f, args.server)
                server_address = run_key[0]
                replayed_keys = replayed_keys_by_server.setdefault(server_address, {})
            except (MemoryError, OSError):
                raise
            except Exception:
                # replay_file diagnoses/quarantines a static malformed header.
                pass
            if run_key is not None and run_key in terminal_runs:
                if _writer_active(f):
                    print(
                        f"  {f}: still open by a live run; an earlier spool "
                        "for this identity was rejected, so it was not replayed"
                    )
                else:
                    _retire_deleted(
                        f,
                        os.stat(f).st_size,
                        "an earlier spool for this run was rejected",
                    )
                all_ok = False
                continue
            if run_key is not None and run_key in blocked_runs:
                print(
                    f"  {f}: skipped because an earlier spool for this run "
                    "is still pending"
                )
                all_ok = False
                continue
            quarantined_by_attempt: set[str] = set()
            delivered = replay_file(
                f,
                server=args.server,
                cdn=args.cdn,
                delete=args.delete,
                replayed_keys=replayed_keys,
                terminal_runs=terminal_runs,
                quarantined_files=quarantined_by_attempt,
            )
            if not delivered:
                if quarantined_by_attempt:
                    all_ok = False
                    continue
                if run_key is not None and run_key not in terminal_runs:
                    blocked_runs.add(run_key)
                all_ok = False
        except Exception as e:
            print(f"  {f}: FAILED — {e}", file=sys.stderr)
            if run_key is not None:
                blocked_runs.add(run_key)
            all_ok = False
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
