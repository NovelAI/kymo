"""On-disk spool for metrics that could not be delivered to the kymo server.

When the upload worker must exit before the server has accepted everything
(shutdown deadline hit, server down, RAM cap exceeded), the undelivered
points go to a spool file instead of being dropped: a pickle stream of one
header dict followed by one record per point. Records reuse the upload
queue's tuple kinds, with CDN payloads (images/resources) pre-encoded to
bytes so the file is self-contained — replayable later from a CPU-only
machine via ``python -m kymo.sync``, so a finished GPU job never has to
stay alive just to finish uploading.

Record kinds:
    ("numeric_ts",        name, step, value, timestamp_ms)
    ("numeric_tagged_ts", name, step, value, tag, timestamp_ms)
    ("text_ts",           name, step, text, timestamp_ms)
    ("cdn_ts",            name, step, cdn_key, timestamp_ms)
    ("cdn",               name, step, cdn_key)
    ("metadata_json",     name, step, dict)
    ("cdn_batch_encoded", name, step, [ {kind, data: bytes, ext, ...}, ... ])
    ("cdn_key_mutation", name, step, cdn_key, timestamp_ms, mutation_version)
    ("metadata_json_mutation", name, step, dict, timestamp_ms, mutation_version)
    ("cdn_batch_encoded_mutation", name, step, entries, timestamp_ms,
     mutation_version)
    ("cdn_batch_encoded_mutation_reserved", name, step, entries, timestamp_ms,
     mutation_version, reduced_mutation_version)
"""

import bisect
import errno
import hashlib
import io
import logging
import os
import pickle
import shlex
import time
from typing import Iterator, Optional

from kymo._env import optional_string as _env_optional_string

_log = logging.getLogger("kymo")
# A filesystem that cannot sync (or open) a directory reports one of these; anything else is a real storage failure.
_DIRECTORY_SYNC_UNSUPPORTED = frozenset(
    {errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP, errno.EACCES, errno.EPERM}
)

SPOOL_SUFFIX = ".mkspool"
HEADER_VERSION = 1
SPOOL_KIND = "kymo-spool"
LEGACY_SPOOL_KIND = "mkdb2-spool"


def replay_command(paths: list[str]) -> str:
    """POSIX-shell command for replaying exact spool paths safely."""
    return shlex.join(["python", "-m", "kymo.sync", "--", *paths])


class SpoolCorruptionError(ValueError):
    """A spool ended with bytes that are not a complete pickle record."""


class UnsupportedSpoolVersion(ValueError):
    """A valid spool requires a newer replay implementation."""


class _DataOnlyUnpickler(pickle.Unpickler):
    """Read the legacy tuple stream without allowing executable globals."""

    def find_class(self, module, name):
        raise SpoolCorruptionError(
            f"spool pickle references forbidden global {module}.{name}"
        )

    def persistent_load(self, pid):
        raise SpoolCorruptionError(f"spool pickle has forbidden persistent id {pid!r}")


def _load_data(fh):
    return _DataOnlyUnpickler(fh).load()


def _validate_header(path: str, header: object, *, require_supported: bool) -> dict:
    if not (
        isinstance(header, dict)
        and header.get("kind") in (SPOOL_KIND, LEGACY_SPOOL_KIND)
    ):
        raise ValueError(f"{path}: not a kymo spool file")
    if require_supported and header.get("v") != HEADER_VERSION:
        raise UnsupportedSpoolVersion(
            f"{path}: spool version {header.get('v')!r} is unsupported "
            f"(this client reads version {HEADER_VERSION})"
        )
    return header


def default_spool_dir() -> str:
    """$KYMO_SPOOL_DIR, else ~/.cache/kymo/spool. On clusters with a shared
    home this makes spool files reachable from CPU-only login/replay nodes."""
    env = _env_optional_string("KYMO_SPOOL_DIR", "MKDB2_SPOOL_DIR")
    if env:
        return env
    return os.path.join(os.path.expanduser("~"), ".cache", "kymo", "spool")


def legacy_default_spool_dir() -> str:
    return os.path.join(os.path.expanduser("~"), ".cache", "pymkdb2", "spool")


def default_replay_dirs() -> list[str]:
    """Current writer directory plus the pre-cutover backlog directory."""
    paths = [default_spool_dir(), legacy_default_spool_dir()]
    unique = []
    seen = set()
    for path in paths:
        key = os.path.normcase(os.path.realpath(path))
        if key not in seen:
            unique.append(path)
            seen.add(key)
    return unique


def _sanitize(s: str) -> str:
    return "".join(c if (c.isalnum() or c in "-_.") else "_" for c in s)


def spool_ident(project_id: str, run_id: str) -> str:
    """Stable basename identity shared by every spool file of one run.

    IDs live in the header; using their full hash here keeps every basename below
    Linux NAME_MAX even when valid ids are multibyte UTF-8. A session segment is
    appended for ownership-scoped shutdown discovery.
    """
    return hashlib.sha256(f"{project_id}\x00{run_id}".encode()).hexdigest()


def spool_name_prefix(project_id: str, run_id: str, session: str = "") -> str:
    """Basename prefix for one run, optionally narrowed to one init session.

    Session ids are minted by the client as filename-safe UUID hex. Keeping the
    legacy run-wide form when no session is supplied preserves discovery of old
    files, while shutdown passes its session and never has to open a header to
    decide which rank/restart owns a file.
    """
    prefix = f"{spool_ident(project_id, run_id)}__"
    if session:
        prefix += f"{_sanitize(session)}__"
    return prefix


def make_spool_path(
    project_id: str,
    run_id: str,
    label: str,
    spool_dir: Optional[str] = None,
    *,
    session: str = "",
) -> str:
    spool_dir = spool_dir or default_spool_dir()
    safe_label = (
        _sanitize(label).encode("utf-8")[:32].decode("utf-8", errors="ignore")
        or "spool"
    )
    prefix = spool_name_prefix(project_id, run_id, session)
    fname = f"{prefix}{safe_label}_{os.getpid()}_{time.time_ns()}{SPOOL_SUFFIX}"
    return os.path.join(spool_dir, fname)


def writer_active(path: str) -> bool:
    """True if a live SpoolWriter still holds its advisory lock on the file (see SpoolWriter._open). Errs toward False where flock is unsupported or node-local — replay_file's size-growth check backs this up for active writers, but an idle live writer on such a mount goes undetected (accepted residual, see SpoolWriter._open)."""
    try:
        import errno
        import fcntl
    except ImportError:
        return False
    try:
        with open(path, "rb") as fh:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
            except OSError as e:
                # Only "someone holds it" counts. ENOLCK/EOPNOTSUPP (filesystem can't flock) must fall through to False, or every file on such a mount would be skipped forever.
                return e.errno in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES)
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass
    return False


def run_spool_files(
    project_id: str,
    run_id: str,
    spool_dir: Optional[str] = None,
    *,
    session: str,
) -> list[str]:
    """Replayable spool names owned by one exact init session.

    The upload worker rotates through as many segments as its recovery needs, so
    the owner discovers them by their shared session prefix. Replayed (``.sent``)
    and retired (``.deleted``) files are excluded by the suffix check.

    Only a missing directory reads as "nothing spooled". Every other OSError
    propagates: an unreadable spool directory must not be reported as proof that
    no file is waiting. This intentionally performs only one directory listing;
    callers that destructively rename a result must separately check writer
    liveness and tolerate a file concurrently disappearing.
    """
    if not session:
        raise ValueError("spool ownership discovery requires a nonempty session")
    spool_dir = spool_dir or default_spool_dir()
    prefix = spool_name_prefix(project_id, run_id, session)
    try:
        names = os.listdir(spool_dir)
    except FileNotFoundError:
        return []
    return [
        os.path.join(spool_dir, name)
        for name in sorted(names)
        if name.startswith(prefix) and name.endswith(SPOOL_SUFFIX)
    ]


def sync_directory(path: str) -> None:
    """Make a create, rename, or unlink inside ``path`` survive power loss.

    Raises ``OSError`` unless the filesystem simply cannot sync directories.
    """
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError as error:
        if error.errno not in _DIRECTORY_SYNC_UNSUPPORTED:
            raise


def sync_after_retirement(path: str) -> None:
    """Sync a completed retirement; failing here only risks replaying already-accepted data after power loss, so it warns instead of undoing the retirement."""
    try:
        sync_directory(os.path.dirname(os.path.abspath(path)))
    except OSError as error:
        _log.warning("could not make spool retirement of %s durable: %s", path, error)


def _retire_spool(path: str, suffix: str) -> str:
    """Rename a spool out of automatic replay under the first free ``suffix`` name, durably and without overwriting."""
    retired = path + suffix
    counter = 1
    while os.path.exists(retired):
        retired = f"{path}{suffix}.{counter}"
        counter += 1
    os.rename(path, retired)
    sync_after_retirement(retired)
    return retired


def retire_sent_spool(path: str) -> str:
    """Remove a fully delivered spool from automatic replay."""
    return _retire_spool(path, ".sent")


def retire_deleted_spool(path: str) -> str:
    """Remove a lifecycle-rejected spool from automatic replay."""
    return _retire_spool(path, ".deleted")


def quarantine_spool(path: str) -> str:
    """Remove a corrupt spool from automatic replay, keeping it as evidence."""
    return _retire_spool(path, ".rejected")


class _SpoolFile(io.FileIO):
    """The spool's raw file: while discarding, writes are dropped, so a failed writer can empty its buffer without its stale bytes reaching the file.

    It keeps its own offset, which counts the bytes that reached the file: the writer asks for every record's end, and FileIO's tell() would cost a syscall each time. The writer's lock makes it the file's only appender.
    """

    discarding = False

    def __init__(self, fd: int, mode: str):
        super().__init__(fd, mode)
        self._offset = super().tell()

    def write(self, b) -> int:
        if self.discarding:
            return memoryview(b).nbytes
        written = super().write(b)
        self._offset += written
        return written

    def seek(self, pos: int, whence: int = os.SEEK_SET) -> int:
        self._offset = super().seek(pos, whence)
        return self._offset

    def tell(self) -> int:
        return self._offset


class SpoolWriter:
    """Append-only spool writer. Opens the file (and writes the header) lazily
    on the first record, so no file appears on runs that flush cleanly.

    A failed write or flush cuts the file back to its last whole record, since a reader rejects a file whose last record is torn. ``count`` is the records written, less any a cut removed; those still buffered reach the file at the next flush."""

    def __init__(self, path: str, header: dict):
        self._path = path
        self._header = dict(header, v=HEADER_VERSION, kind=SPOOL_KIND)
        self._fh = None
        self._sealed = False
        self._unsynced = False  # bytes written since the last durability barrier
        self._entry_synced = False  # the file's directory entry is durable
        self.count = 0
        # The end of the last flushed record, then the end of each record written since.
        self._record_ends = [0]
        # A torn record could not be cut off, so nothing more may follow it.
        self._torn = False

    @property
    def path(self) -> str:
        return self._path

    def _open(self):
        spool_dir = os.path.dirname(self._path)
        os.makedirs(spool_dir, mode=0o700, exist_ok=True)
        # Replay often runs as another uid (e.g. --container-remap-root jobs), so mirror the directory's group/other read bits; writes remain owner-only. The default 0700 directory widens nothing.
        mode = 0o600 | (os.stat(spool_dir).st_mode & 0o044)
        fd = os.open(self._path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, mode)
        try:
            # os.open's mode is still filtered through the process umask; the directory bits above are the operator's explicit sharing policy, so reapply them.
            fchmod = getattr(os, "fchmod", None)
            if fchmod is not None:
                fchmod(fd, mode)
            raw = _SpoolFile(fd, "a")
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            raise
        self._fh = io.BufferedWriter(raw)
        # Advisory writer lock, held while the file is open: sync renames replayed files to .sent, and renaming a file a live writer still appends to would strand every later record in the .sent inode. Best-effort — where flock is unsupported or node-local (NFSv3/nolock mounts) sync's only backstop is its size-growth check during the replay, so an IDLE live writer there can still be renamed out from under; accepted residual.
        try:
            import fcntl

            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (ImportError, OSError):
            pass
        self._record_ends = [self._fh.tell()]
        self._unsynced = True
        if self._fh.tell() == 0:
            try:
                pickle.dump(self._header, self._fh, protocol=pickle.HIGHEST_PROTOCOL)
                # On disk at once, so a failed first record cannot take the header with it.
                self._fh.flush()
                self._record_ends = [self._fh.tell()]
            except BaseException:
                # Leave an empty file rather than a torn header; the next write opens it again.
                try:
                    self._discard()
                finally:
                    fh, self._fh = self._fh, None
                    fh.close()
                raise

    def write(self, record: tuple) -> None:
        if self._sealed:
            raise RuntimeError(f"cannot append to sealed spool {self._path}")
        self._raise_if_torn()
        if self._fh is None:
            self._open()
        try:
            pickle.dump(record, self._fh, protocol=pickle.HIGHEST_PROTOCOL)
        except BaseException:
            # Keep the whole records buffered before the failed one if they can still reach the file; the cut removes the failed one either way.
            try:
                self._fh.flush()
            finally:
                self._discard()
            raise
        self._record_ends.append(self._fh.tell())
        if len(self._record_ends) > 8192:
            # Records already wholly on disk survive any cut, so only the last of their ends is needed.
            del self._record_ends[
                : bisect.bisect_right(self._record_ends, self._fh.raw.tell()) - 1
            ]
        self._unsynced = True
        self.count += 1

    def flush(self) -> None:
        """Push completed records out of Python's userspace buffer.

        The upload worker calls this before it releases delivery accounting, so
        terminating the worker cannot discard an already-accounted buffer tail.
        The final close still performs the more expensive durability fsync.
        A failed flush drops the records that had not wholly reached the file, which ``count`` then omits.
        """
        if self._fh is None:
            return
        self._raise_if_torn()
        try:
            self._fh.flush()
        except BaseException:
            self._discard()
            raise
        self._record_ends = [self._fh.tell()]

    def _raise_if_torn(self) -> None:
        if self._torn:
            raise OSError(
                errno.EIO,
                f"spool {self._path} ends in a torn record it could not cut off",
            )

    def _discard(self) -> None:
        """Drop the buffered bytes unwritten and cut the file back to the last whole record it holds."""
        raw = self._fh.raw
        raw.discarding = True
        try:
            self._fh.flush()
        finally:
            raw.discarding = False
        # Records whose end reached the file are whole; what reached it past the last of them is torn.
        whole = bisect.bisect_right(self._record_ends, raw.tell())
        # Adjusted before the truncation can fail: a torn cut must never leave the dropped records counted as spooled.
        self.count -= len(self._record_ends) - whole
        size = self._record_ends[whole - 1]
        try:
            os.ftruncate(raw.fileno(), size)
            self._fh.seek(size)
        except BaseException:
            self._torn = True
            raise
        self._record_ends = [size]
        self._unsynced = True

    def seal(self) -> Optional[str]:
        """Make a nonempty spool immutable while retaining its writer lock.

        The live upload worker hands sealed segments to its replay helper. The
        open descriptor keeps external ``kymo.sync`` processes from racing
        that replay; a worker crash releases the lock while leaving the original
        ``*.mkspool`` path available for ordinary recovery.

        Deliberately no fsync: this runs on the worker's only queue-consumer
        thread, and a stalled spool mount must not stop the queue draining.
        ``sync()`` performs the durability barrier from the replay helper.
        """
        if self._sealed:
            return self._path if self.count else None
        if self._fh is None:
            self._sealed = True
            return None
        self.flush()
        self._sealed = True
        return self._path if self.count else None

    def sync(self) -> None:
        """Make the written records durable. Safe to call from the thread that
        replays a sealed segment; the owner performs no concurrent writes.

        A no-op once everything written has been synced, so the owner's own
        ``close()`` of a segment its replay helper already synced costs no second
        fsync — on a stalled mount that fsync would block the queue-consumer
        thread this barrier was moved off.
        """
        if self._fh is None or not self._unsynced:
            return
        self.flush()
        os.fsync(self._fh.fileno())
        if not self._entry_synced:
            # A synced file whose directory entry is not durable can vanish on power loss, and so can a spool directory this writer just created.
            spool_dir = os.path.dirname(os.path.abspath(self._path))
            sync_directory(spool_dir)
            sync_directory(os.path.dirname(spool_dir))
            self._entry_synced = True
        self._unsynced = False

    def release(self) -> None:
        """Drop the descriptor without a durability barrier.

        For a segment whose records the server has accepted — or that is being
        retired — persistence is moot, and a barrier that already failed must not
        be retried by the worker's queue-consumer thread: that retry is exactly
        the stall this split was made to remove. Sealing already emptied the
        userspace buffer, so nothing is dropped here.
        """
        if self._fh is None:
            return
        fh, self._fh = self._fh, None
        self._sealed = True
        try:
            fh.close()
        except OSError:
            # Nothing left to report: the records are delivered, and the lock
            # goes with the descriptor at process exit either way.
            pass

    def close(self) -> Optional[str]:
        """Flush and close. Returns the path if any records were written."""
        if self._fh is None:
            return None
        fh = self._fh
        try:
            self.seal()
            self.sync()
        finally:
            try:
                fh.close()
            finally:
                self._fh = None
        return self._path if self.count else None


def read_spool(path: str) -> tuple[dict, Iterator[tuple]]:
    """Return ``(header, records)`` and distinguish clean EOF from corruption.

    A replay must never treat a truncated final pickle as success: doing so
    would deliver only the valid prefix and then rename the source ``.sent``.
    """
    fh = open(path, "rb")
    try:
        header = _load_data(fh)
    except Exception:
        fh.close()
        raise
    try:
        _validate_header(path, header, require_supported=True)
    except Exception:
        fh.close()
        raise

    def _records():
        try:
            while True:
                offset = fh.tell()
                if not fh.read(1):
                    return
                fh.seek(offset)
                try:
                    yield _load_data(fh)
                except (MemoryError, OSError):
                    raise
                except Exception as e:
                    raise SpoolCorruptionError(
                        f"{path}: corrupt spool record at byte {offset}: {e}"
                    ) from e
        finally:
            fh.close()

    return header, _records()


def read_spool_header(path: str, *, require_supported: bool = False) -> dict:
    """Read and validate only the header, without opening a record iterator."""
    with open(path, "rb") as fh:
        return read_spool_header_file(
            fh, path=path, require_supported=require_supported
        )


def read_spool_header_file(fh, *, path: str, require_supported: bool = False) -> dict:
    """Validate a header through an already-open, identity-pinned file."""
    header = _load_data(fh)
    return _validate_header(path, header, require_supported=require_supported)
