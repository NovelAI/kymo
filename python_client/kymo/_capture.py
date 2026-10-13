"""
Import-time stdout/stderr capture for kymo.

Wraps sys.stdout and sys.stderr at import time so that output
produced before init() is not lost. Buffers are drained on each
log() call and sent as text stream metrics.
"""

import os
import sys
import threading
from collections import deque

_stdout_buf: deque[str] = deque()
_stderr_buf: deque[str] = deque()
_stdout_stats = {"size": 0, "dropped": 0}
_stderr_stats = {"size": 0, "dropped": 0}
_installed = False

# Continuous cap per stream BETWEEN drains, oldest dropped first (the tail is what you want when reading a stalled run). Two reasons, both real incidents waiting to happen: a chatty process that stops calling log() grows these buffers without bound, and one drain becomes ONE text point — past the server's 4MiB gRPC frame cap it can never be delivered or replayed (tqdm over a slow epoch is enough).
_max_buffered_chars = 1_000_000

# One lock per stream guards its buffer and stats: prints come from any thread (data loaders, tqdm monitors) while log() drains, and unsynchronized, write() indexed a deque a drain had just cleared (IndexError out of print()). The passthrough write stays outside it: no user I/O under the lock.
# Reentrant: a signal handler runs on the thread it interrupts, perhaps inside a write or a drain, and may print or call log() there. Only the outermost operation removes or trims chunks: a nested write appends and counts its chunk (TeeWriter._keep), and a nested drain takes nothing but for the shutdown's last, which takes the text in place and blanks the chunks, so neither the operation it interrupted nor a later drain publishes it again. A drain takes only the chunks held as it started (_drain_one).
_stdout_lock = threading.RLock()
_stderr_lock = threading.RLock()

# Set by kymo.client: the shutdown signals its handler deferred, and what handles them once a held section, here a write's buffering, has ended.
deferred_signals: list = []
run_deferred_signals = None


def _locks_before_fork():
    _stdout_lock.acquire()
    _stderr_lock.acquire()


def _locks_after_fork():
    _stdout_lock.release()
    _stderr_lock.release()


def _capture_after_fork_child():
    # A fork child did not produce the parent's pending output. Clear the
    # copied state in place so its inherited TeeWriters keep valid references
    # and cannot upload parent-only text/drop markers into a child-owned run.
    for buffer, stats in (
        (_stdout_buf, _stdout_stats),
        (_stderr_buf, _stderr_stats),
    ):
        buffer.clear()
        stats["size"] = 0
        stats["dropped"] = 0
    _locks_after_fork()


# Same discipline as the logging module: a fork (init() spawns the upload worker via fork on Linux) while another thread holds a capture lock would leave the child's copy locked forever, deadlocking its first print.
if hasattr(os, "register_at_fork"):
    os.register_at_fork(
        before=_locks_before_fork,
        after_in_parent=_locks_after_fork,
        after_in_child=_capture_after_fork_child,
    )


class TeeWriter:
    """Wraps a stream, passes through all writes while buffering a bounded copy."""

    def __init__(self, original, buffer: deque, stats: dict, lock: threading.RLock):
        self._original = original
        self._buffer = buffer
        self._stats = stats
        self._lock = lock

    def write(self, s):
        if isinstance(s, str) and s:
            self._keep(s)
            if deferred_signals:
                try:
                    run_deferred_signals()
                except BaseException:
                    # A deferred handler that raises, as kymo's own exits, still lets the terminal see the text.
                    self._original.write(s)
                    raise
        return self._original.write(s)

    def _keep(self, s: str) -> None:
        """Buffer a copy of s, evicting the oldest text past the cap unless nested in another capture operation on this thread."""
        length = len(s)
        nested = self._lock._is_owned()
        with self._lock:
            self._stats["size"] += length
            self._buffer.append(s)
            if nested:
                return
            # Evict oldest-first down to the cap, trimming the boundary chunk instead of dropping it whole — dropping whole chunks under-retains (a 900k chunk followed by a 200k write would keep only the 200k, not the newest 1M). Not `while <condition>:`: Python 3.13 leaves a conditional while loop's jump back, where a handler may raise, outside the with (cpython#130279).
            while True:
                excess = self._stats["size"] - _max_buffered_chars
                if excess <= 0:
                    break
                head = self._buffer[0]
                head_length = len(head)
                if self._buffer[0] is not head:
                    # The shutdown's last drain, nested as len() returned, took the head and blanked it.
                    continue
                if head_length <= excess:
                    self._stats["size"] -= head_length
                    self._stats["dropped"] += head_length
                    self._buffer.popleft()
                else:
                    self._stats["size"] -= excess
                    self._stats["dropped"] += excess
                    self._buffer[0] = head[excess:]

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def __getattr__(self, name):
        return getattr(self._original, name)


def install_capture():
    """Wrap available stdout/stderr streams. Called at import time."""
    global _installed
    if _installed:
        return
    if sys.stdout is not None:
        sys.stdout = TeeWriter(sys.stdout, _stdout_buf, _stdout_stats, _stdout_lock)
    if sys.stderr is not None:
        sys.stderr = TeeWriter(sys.stderr, _stderr_buf, _stderr_stats, _stderr_lock)
    _installed = True


def _drain_one(buffer: deque, stats: dict, lock: threading.RLock, final=False) -> str:
    if lock._is_owned():
        # Nested in a write or a drain on this thread (a handler that called log()): leave the text for the next drain, but the shutdown's last (final), which none follows, takes it in place, blanking the chunks the operation nested in may still hold.
        if not final:
            return ""
        chunks = list(buffer)
        for index in range(len(chunks)):
            buffer[index] = ""
        # Summed first, here and below: a handler that runs as sum() returns (a print, or the run's end) then changes the size before the read, not between it and the store, which would undo that change.
        taken = sum(map(len, chunks))
        stats["size"] -= taken
        dropped = stats["dropped"]
        stats["dropped"] = 0
    else:
        with lock:
            chunks = [buffer.popleft() for _ in range(len(buffer))]
            taken = sum(map(len, chunks))
            stats["size"] -= taken
            dropped = stats["dropped"]
            stats["dropped"] = 0
    # Unix filenames decoded with surrogateescape can be printed normally, but protobuf's string-to-bytes boundary rejects their lone surrogates. Keep terminal passthrough exact and make only the captured copy safe.
    text = "".join(chunks).encode("utf-8", errors="replace").decode("utf-8")
    if dropped:
        # The marker rides inside the stream so the dashboard shows the gap where it happened.
        return f"[kymo: {dropped} chars of captured output dropped]\n{text}"
    return text


def has_output() -> bool:
    """Whether either stream holds captured text or a pending drop marker, read without the stream locks, so an empty log() takes none; a write in progress on another thread goes out at the next drain."""
    return bool(
        _stdout_buf
        or _stderr_buf
        or _stdout_stats["dropped"]
        or _stderr_stats["dropped"]
    )


def drain_buffers(final=False) -> tuple[str, str]:
    """Drain and return (stdout, stderr) text since last drain; has_output() tells whether there is any. A stream with nothing captured is passed over without its lock, as has_output() reads it, so a call that printed to one stream pays for one drain. final: the shutdown's last drain, which also takes the text of a stream whose write or drain it is nested in (_drain_one)."""
    return (
        _drain_one(_stdout_buf, _stdout_stats, _stdout_lock, final)
        if _stdout_buf or _stdout_stats["dropped"]
        else "",
        _drain_one(_stderr_buf, _stderr_stats, _stderr_lock, final)
        if _stderr_buf or _stderr_stats["dropped"]
        else "",
    )
