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

# One lock per stream, guarding the buffer + stats pair: prints come from any thread (data loaders, tqdm monitors) while log() drains from the training thread, and the unsynchronized read-modify sequences let write() index a just-cleared deque (reproduced: IndexError out of print()) and corrupt the size accounting. The passthrough write stays OUTSIDE the lock — no user I/O under it.
_stdout_lock = threading.Lock()
_stderr_lock = threading.Lock()


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

    def __init__(self, original, buffer: deque, stats: dict, lock: threading.Lock):
        self._original = original
        self._buffer = buffer
        self._stats = stats
        self._lock = lock

    def write(self, s):
        if not isinstance(s, str):
            return self._original.write(s)
        if s:
            with self._lock:
                self._buffer.append(s)
                self._stats["size"] += len(s)
                # Evict oldest-first down to the cap, trimming the boundary chunk instead of dropping it whole — dropping whole chunks under-retains (a 900k chunk followed by a 200k write would keep only the 200k, not the newest 1M).
                while self._stats["size"] > _max_buffered_chars:
                    excess = self._stats["size"] - _max_buffered_chars
                    head = self._buffer[0]
                    if len(head) <= excess:
                        self._buffer.popleft()
                        self._stats["size"] -= len(head)
                        self._stats["dropped"] += len(head)
                    else:
                        self._buffer[0] = head[excess:]
                        self._stats["size"] -= excess
                        self._stats["dropped"] += excess
        return self._original.write(s)

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


def _drain_one(buffer: deque, stats: dict, lock: threading.Lock) -> str:
    with lock:
        # Unix filenames decoded with surrogateescape can be printed normally, but protobuf's string-to-bytes boundary rejects their lone surrogates. Keep terminal passthrough exact and make only the captured copy safe.
        text = "".join(buffer).encode("utf-8", errors="replace").decode("utf-8")
        buffer.clear()
        dropped = stats["dropped"]
        stats["size"] = 0
        stats["dropped"] = 0
    if dropped:
        # The marker rides inside the stream so the dashboard shows the gap where it happened.
        return f"[kymo: {dropped} chars of captured output dropped]\n{text}"
    return text


def drain_buffers() -> tuple[str, str]:
    """Drain and return (stdout, stderr) text since last drain."""
    return (
        _drain_one(_stdout_buf, _stdout_stats, _stdout_lock),
        _drain_one(_stderr_buf, _stderr_stats, _stderr_lock),
    )
