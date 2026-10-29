"""
Background poller for kymo.

Every N seconds publishes, through the client publisher, the captured
stdout/stderr and, unless disabled, timestamped GPU (via pynvml), CPU,
memory, disk, and network (via psutil) metrics.
"""

import math
import os
import threading
import time

from kymo._log import logger as log
from kymo.client import _WorkerExited

_STOP_BARRIER_TIMEOUT = 1.0
_FAILURE_WARN_INTERVAL = 60.0

# Optional imports — gracefully degrade if not available
try:
    import pynvml

    _HAS_NVML = True
except ImportError:
    _HAS_NVML = False

try:
    import psutil

    _HAS_PSUTIL = True
except ImportError:
    _HAS_PSUTIL = False


def _nvml(read) -> float:
    """One value's NVML reading through its own lambda, read, or a NaN marker (a hole in the chart, never an invented 0) wherever read raises: where NVML cannot give the value (an unsupported field, a lost GPU), and where a percentage's total is zero. Guarding each field alone costs about 1 µs of Python per GPU per poll."""
    try:
        return float(read())
    except Exception:
        return math.nan


class SystemMetricsPoller:
    """Background thread that publishes the captured console and, with system_metrics, polls and publishes system metrics."""

    def __init__(self, *, publish, poll_interval: float = 2.0, system_metrics: bool = True):
        self._interval = poll_interval
        self._publish = publish
        self._system_metrics = system_metrics
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        # Linearizes the final stop check with queue publication. stop() may
        # return while a backend call is wedged, but after crossing this gate a
        # later backend return cannot publish behind the owner's sentinel.
        self._publish_lock = threading.Lock()
        # GPU state
        self._nvml_ok = False
        self._gpu_count = 0
        self._gpu_handles = []

        # psutil state
        self._psutil_ok = False
        self._process: "psutil.Process | None" = None

        # Delta tracking for rates
        self._prev_net = None
        self._prev_disk = None
        self._prev_net_time = None
        self._prev_disk_time = None

    def start(self):
        """With system_metrics, initialize the metric backends; either way start the polling thread, which uploads the captured console."""
        if self._system_metrics:
            self._init_nvml()
            self._init_psutil()
            available = []
            if self._nvml_ok:
                available.append(f"gpu×{self._gpu_count}")
            if self._psutil_ok:
                available.append("cpu/mem/disk/net")
            if available:
                log.info("system metrics enabled (%s)", ", ".join(available))
            else:
                log.info("system metrics disabled (neither psutil nor a working NVML); the console is still uploaded")

        self._thread = threading.Thread(target=self._poll_loop, name="kymo-poller", daemon=True)
        self._thread.start()

    def stop(self, timeout: float | None = None) -> bool:
        """Stop publishing within ``timeout``; return whether that was proven."""
        budget = (
            _STOP_BARRIER_TIMEOUT
            if timeout is None
            else min(_STOP_BARRIER_TIMEOUT, max(0.0, timeout))
        )
        deadline = time.monotonic() + budget
        self._stop.set()

        publication_quiesced = self._publish_lock.acquire(
            timeout=max(0.0, deadline - time.monotonic())
        )
        if publication_quiesced:
            self._publish_lock.release()
        else:
            log.warning("console/system-metrics publication did not quiesce before shutdown")

        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        thread_stopped = thread is None or not thread.is_alive()
        if self._nvml_ok and not thread_stopped:
            # A wedged driver or queue call must not make shutdown unbounded. Leaving NVML initialized is safer than racing its active poll.
            log.warning("console/system-metrics poll did not stop; skipping NVML shutdown")
        elif self._nvml_ok:
            try:
                pynvml.nvmlShutdown()
            except Exception:
                pass
            self._nvml_ok = False
        return publication_quiesced

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def _init_nvml(self):
        if not _HAS_NVML:
            return
        initialized = False
        try:
            pynvml.nvmlInit()
            initialized = True
            self._gpu_count = pynvml.nvmlDeviceGetCount()
            self._gpu_handles = [
                pynvml.nvmlDeviceGetHandleByIndex(i) for i in range(self._gpu_count)
            ]
            self._nvml_ok = True
        except Exception as e:
            if initialized:
                try:
                    pynvml.nvmlShutdown()
                except Exception:
                    pass
            log.warning("pynvml init failed (%s), GPU metrics disabled", e)

    def _init_psutil(self):
        if not _HAS_PSUTIL:
            return
        try:
            self._process = psutil.Process(os.getpid())
            # Prime cpu_percent (first call always returns 0)
            self._process.cpu_percent()
            self._psutil_ok = True
        except Exception as e:
            log.warning("psutil init failed (%s), CPU/mem/disk/net metrics disabled", e)

    # ------------------------------------------------------------------
    # Polling loop
    # ------------------------------------------------------------------

    def _poll_loop(self):
        """Poll until stopped. A failed poll or publication is retried, with a warning at most once a minute: the console text and system metrics a tick publishes are what show a run alive between rare log() calls. Of failures, only the upload worker's exit ends it: nothing restarts the worker (only init() starts one), so no later point could be delivered."""
        warned_at = float("-inf")
        while not self._stop.wait(self._interval):
            try:
                self._poll_once()
            except _WorkerExited:
                log.warning("console/system-metrics uploads stopped: the upload worker has exited")
                return
            except Exception as error:
                now = time.monotonic()
                if now - warned_at >= _FAILURE_WARN_INTERVAL:
                    log.warning("console/system-metrics poll failed; retrying: %s", error)
                    warned_at = now

    def _poll_once(self):
        if self._stop.is_set():
            return
        metrics = {}
        if self._nvml_ok:
            self._poll_gpu(metrics)
        if self._psutil_ok:
            self._poll_cpu_mem(metrics)
            self._poll_disk(metrics)
            self._poll_network(metrics)

        # Use timestamp as both step and timestamp_ms
        ts = int(time.time() * 1000)
        points = []
        for name, value in metrics.items():
            if isinstance(value, list):
                for i, v in enumerate(value):
                    points.append(("numeric_tagged_ts", name, ts, float(v), str(i), ts))
            else:
                points.append(("numeric_ts", name, ts, float(value), ts))

        with self._publish_lock:
            if self._stop.is_set():
                return
            # The captured console goes with them at their time, or alone without metrics.
            self._publish([points] if points else [], capture_ms=ts)

    # ------------------------------------------------------------------
    # GPU metrics (tagged — lists of per-GPU values)
    # ------------------------------------------------------------------

    def _poll_gpu(self, metrics: dict):
        gpu_util = []
        gpu_mem_util = []
        gpu_temp = []
        gpu_power = []
        gpu_power_pct = []
        gpu_power_limit = []
        gpu_mem_used = []
        gpu_mem_used_pct = []
        gpu_sm_clock = []
        gpu_mem_clock = []
        gpu_ecc_uncorrected = []
        gpu_ecc_corrected = []

        for handle in self._gpu_handles:
            # One query for both utilizations, and one for both memory figures.
            try:
                util = pynvml.nvmlDeviceGetUtilizationRates(handle)
            except Exception:
                util = None
            gpu_util.append(_nvml(lambda: util.gpu))
            gpu_mem_util.append(_nvml(lambda: util.memory))

            gpu_temp.append(
                _nvml(
                    lambda: pynvml.nvmlDeviceGetTemperature(
                        handle, pynvml.NVML_TEMPERATURE_GPU
                    )
                )
            )

            # Power, kept when only the limit fails
            power_w = _nvml(lambda: pynvml.nvmlDeviceGetPowerUsage(handle)) / 1000.0
            limit_w = (
                _nvml(lambda: pynvml.nvmlDeviceGetEnforcedPowerLimit(handle)) / 1000.0
            )
            gpu_power.append(power_w)
            gpu_power_limit.append(limit_w)
            gpu_power_pct.append(_nvml(lambda: power_w / limit_w * 100.0))

            try:
                mem = pynvml.nvmlDeviceGetMemoryInfo(handle)
            except Exception:
                mem = None
            gpu_mem_used.append(_nvml(lambda: mem.used))
            gpu_mem_used_pct.append(_nvml(lambda: mem.used / mem.total * 100.0))

            gpu_sm_clock.append(
                _nvml(
                    lambda: pynvml.nvmlDeviceGetClockInfo(handle, pynvml.NVML_CLOCK_SM)
                )
            )
            gpu_mem_clock.append(
                _nvml(
                    lambda: pynvml.nvmlDeviceGetClockInfo(handle, pynvml.NVML_CLOCK_MEM)
                )
            )

            # ECC errors (may not be supported on consumer GPUs)
            gpu_ecc_uncorrected.append(
                _nvml(
                    lambda: pynvml.nvmlDeviceGetMemoryErrorCounter(
                        handle,
                        pynvml.NVML_MEMORY_ERROR_TYPE_UNCORRECTED,
                        pynvml.NVML_VOLATILE_ECC,
                        pynvml.NVML_MEMORY_LOCATION_DEVICE_MEMORY,
                    )
                )
            )
            gpu_ecc_corrected.append(
                _nvml(
                    lambda: pynvml.nvmlDeviceGetMemoryErrorCounter(
                        handle,
                        pynvml.NVML_MEMORY_ERROR_TYPE_CORRECTED,
                        pynvml.NVML_VOLATILE_ECC,
                        pynvml.NVML_MEMORY_LOCATION_DEVICE_MEMORY,
                    )
                )
            )

        metrics["system/gpu_util_pct"] = gpu_util
        metrics["system/gpu_mem_util_pct"] = gpu_mem_util
        metrics["system/gpu_temp_c"] = gpu_temp
        metrics["system/gpu_power_w"] = gpu_power
        metrics["system/gpu_power_pct"] = gpu_power_pct
        metrics["system/gpu_power_limit_w"] = gpu_power_limit
        metrics["system/gpu_mem_used_bytes"] = gpu_mem_used
        metrics["system/gpu_mem_used_pct"] = gpu_mem_used_pct
        metrics["system/gpu_sm_clock_mhz"] = gpu_sm_clock
        metrics["system/gpu_mem_clock_mhz"] = gpu_mem_clock
        metrics["system/gpu_ecc_uncorrected"] = gpu_ecc_uncorrected
        metrics["system/gpu_ecc_corrected"] = gpu_ecc_corrected

    # ------------------------------------------------------------------
    # CPU / Memory metrics (scalar)
    # ------------------------------------------------------------------

    def _poll_cpu_mem(self, metrics: dict):
        try:
            metrics["system/proc_threads"] = float(self._process.num_threads())
        except Exception:
            pass

        try:
            metrics["system/proc_cpu_pct"] = self._process.cpu_percent()
        except Exception:
            pass

        try:
            mem_info = self._process.memory_info()
            metrics["system/proc_mem_used_mb"] = mem_info.rss / 1e6
        except Exception:
            pass

        try:
            metrics["system/proc_mem_pct"] = self._process.memory_percent()
        except Exception:
            pass

        try:
            vm = psutil.virtual_memory()
            metrics["system/sys_mem_avail_mb"] = vm.available / 1e6
            metrics["system/sys_mem_pct"] = vm.percent
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Disk metrics (scalar, rates via delta)
    # ------------------------------------------------------------------

    def _poll_disk(self, metrics: dict):
        try:
            usage = psutil.disk_usage("/")
            metrics["system/disk_used_gb"] = usage.used / 1e9
            metrics["system/disk_used_pct"] = usage.percent
        except Exception:
            pass

        try:
            now = time.monotonic()
            counters = psutil.disk_io_counters()
            if self._prev_disk is not None and self._prev_disk_time is not None:
                dt = now - self._prev_disk_time
                if dt > 0:
                    read_delta = counters.read_bytes - self._prev_disk.read_bytes
                    write_delta = counters.write_bytes - self._prev_disk.write_bytes
                    if read_delta >= 0:
                        metrics["system/disk_read_mbps"] = read_delta / dt / 1e6
                    if write_delta >= 0:
                        metrics["system/disk_write_mbps"] = write_delta / dt / 1e6
            self._prev_disk = counters
            self._prev_disk_time = now
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Network metrics (scalar, rates via delta)
    # ------------------------------------------------------------------

    def _poll_network(self, metrics: dict):
        try:
            now = time.monotonic()
            counters = psutil.net_io_counters()
            if self._prev_net is not None and self._prev_net_time is not None:
                dt = now - self._prev_net_time
                if dt > 0:
                    sent_delta = counters.bytes_sent - self._prev_net.bytes_sent
                    recv_delta = counters.bytes_recv - self._prev_net.bytes_recv
                    if sent_delta >= 0:
                        metrics["system/net_up_mbps"] = sent_delta / dt / 1e6
                    if recv_delta >= 0:
                        metrics["system/net_down_mbps"] = recv_delta / dt / 1e6
            self._prev_net = counters
            self._prev_net_time = now
        except Exception:
            pass
