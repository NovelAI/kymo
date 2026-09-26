//! Host-facing resource metrics for the server process.

use std::path::{Path, PathBuf};

const OUTBOX_STATS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Publish storage and container pressure every 30 seconds.
pub fn spawn_metrics(cdn_root: Option<PathBuf>, ch: std::sync::Arc<crate::clickhouse::ChClient>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
        let mut outbox_stats_failing = false;
        loop {
            tick.tick().await;
            if let Some((used, total)) = cdn_root.as_deref().and_then(cdn_fs_stats) {
                metrics::gauge!("mkdb2_cdn_disk_used_bytes").set(used as f64);
                metrics::gauge!("mkdb2_cdn_disk_total_bytes").set(total as f64);
            }
            if let Some(cpu) = read_cgroup_cpu() {
                // Cumulative seconds are gauges so rate() retains sub-second
                // precision; nr_* are exact integer counters.
                metrics::gauge!("mkdb2_cgroup_cpu_usage_seconds_total").set(cpu.usage_seconds);
                metrics::gauge!("mkdb2_cgroup_cpu_throttled_seconds_total")
                    .set(cpu.throttled_seconds);
                metrics::counter!("mkdb2_cgroup_cpu_nr_periods_total").absolute(cpu.nr_periods);
                metrics::counter!("mkdb2_cgroup_cpu_nr_throttled_total").absolute(cpu.nr_throttled);
                metrics::gauge!("mkdb2_cgroup_cpu_quota_cores").set(cpu.quota_cores);
            }
            if let Some(mem) = read_cgroup_memory() {
                metrics::gauge!("mkdb2_cgroup_memory_usage_bytes").set(mem.usage_bytes);
                metrics::gauge!("mkdb2_cgroup_memory_rss_bytes").set(mem.rss_bytes);
                metrics::gauge!("mkdb2_cgroup_memory_limit_bytes").set(mem.limit_bytes);
            }
            let outbox_storage =
                tokio::time::timeout(OUTBOX_STATS_TIMEOUT, ch.metric_registry_outbox_storage())
                    .await
                    .unwrap_or_else(|_| {
                        Err(anyhow::anyhow!(
                            "timed out after {}s",
                            OUTBOX_STATS_TIMEOUT.as_secs()
                        ))
                    });
            match outbox_storage {
                Ok(storage) => {
                    if std::mem::replace(&mut outbox_stats_failing, false) {
                        tracing::info!("ClickHouse metric-registry outbox stats recovered");
                    }
                    // These retain their last successful sample while the
                    // query fails; the error counter and transition log mark
                    // that stale interval.
                    metrics::gauge!("mkdb2_registry_outbox_rows").set(storage.rows as f64);
                    metrics::gauge!("mkdb2_registry_outbox_bytes").set(storage.bytes as f64);
                    metrics::gauge!("mkdb2_registry_outbox_parts").set(storage.parts as f64);
                }
                Err(error) => {
                    if !std::mem::replace(&mut outbox_stats_failing, true) {
                        tracing::warn!(
                            %error,
                            "Failed to read ClickHouse metric-registry outbox stats"
                        );
                    }
                    metrics::counter!("mkdb2_registry_outbox_stats_errors_total").increment(1);
                }
            }
        }
    });
}

/// statvfs the CDN filesystem, returning (used_bytes, total_bytes).
fn cdn_fs_stats(path: &Path) -> Option<(u64, u64)> {
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) } != 0 {
        return None;
    }
    let frsize = stat.f_frsize as u64;
    let total = stat.f_blocks as u64 * frsize;
    // Used follows df/POSIX semantics (f_blocks - f_bfree); f_bavail would count blocks reserved for privileged use as used.
    let free = stat.f_bfree as u64 * frsize;
    Some((total.saturating_sub(free), total))
}

/// CFS CPU accounting for this container's own cgroup. Reads the cluster's
/// cgroup v1 layout and falls back to v2 for future migrations.
struct CgroupCpu {
    usage_seconds: f64,
    throttled_seconds: f64,
    nr_periods: u64,
    nr_throttled: u64,
    quota_cores: f64,
}

fn cgroup_read_trim(path: &Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_string())
}

fn cgroup_v2_dir(mount_root: &Path, self_cgroup: &Path) -> Option<PathBuf> {
    let contents = std::fs::read_to_string(self_cgroup).ok()?;
    let relative = contents.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        let hierarchy = fields.next()?;
        let controllers = fields.next()?;
        let path = fields.next()?;
        (hierarchy == "0" && controllers.is_empty()).then_some(path)
    })?;
    let relative = Path::new(relative).strip_prefix("/").ok()?;
    if !relative
        .components()
        .all(|component| matches!(component, std::path::Component::Normal(_)))
    {
        return None;
    }
    // A host-level cgroup mount exposes the service below its own path, while
    // a private cgroup namespace reports `/` and therefore keeps the root.
    Some(mount_root.join(relative))
}

fn cgroup_stat_field(stat: &str, key: &str) -> Option<u64> {
    stat.lines().find_map(|line| {
        line.strip_prefix(key)?
            .strip_prefix(' ')?
            .trim()
            .parse()
            .ok()
    })
}

fn read_cgroup_cpu() -> Option<CgroupCpu> {
    read_cgroup_cpu_at(Path::new("/sys/fs/cgroup"), Path::new("/proc/self/cgroup"))
}

fn read_cgroup_cpu_at(mount_root: &Path, self_cgroup: &Path) -> Option<CgroupCpu> {
    let v1 = mount_root.join("cpu,cpuacct");
    if let Some(stat) = cgroup_read_trim(&v1.join("cpu.stat")) {
        let usage_ns: u64 = cgroup_read_trim(&v1.join("cpuacct.usage"))?.parse().ok()?;
        let quota: i64 = cgroup_read_trim(&v1.join("cpu.cfs_quota_us"))
            .and_then(|value| value.parse().ok())
            .unwrap_or(-1);
        let period: i64 = cgroup_read_trim(&v1.join("cpu.cfs_period_us"))
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        return Some(CgroupCpu {
            usage_seconds: usage_ns as f64 / 1e9,
            throttled_seconds: cgroup_stat_field(&stat, "throttled_time").unwrap_or(0) as f64 / 1e9,
            nr_periods: cgroup_stat_field(&stat, "nr_periods").unwrap_or(0),
            nr_throttled: cgroup_stat_field(&stat, "nr_throttled").unwrap_or(0),
            quota_cores: if quota > 0 && period > 0 {
                quota as f64 / period as f64
            } else {
                0.0
            },
        });
    }

    let v2 = cgroup_v2_dir(mount_root, self_cgroup)?;
    let stat = cgroup_read_trim(&v2.join("cpu.stat"))?;
    let quota_cores = cgroup_read_trim(&v2.join("cpu.max"))
        .and_then(|value| {
            let mut fields = value.split_whitespace();
            let quota = fields.next()?;
            let period: f64 = fields.next()?.parse().ok()?;
            if quota == "max" || period <= 0.0 {
                Some(0.0)
            } else {
                Some(quota.parse::<f64>().ok()? / period)
            }
        })
        .unwrap_or(0.0);
    Some(CgroupCpu {
        usage_seconds: cgroup_stat_field(&stat, "usage_usec").unwrap_or(0) as f64 / 1e6,
        throttled_seconds: cgroup_stat_field(&stat, "throttled_usec").unwrap_or(0) as f64 / 1e6,
        nr_periods: cgroup_stat_field(&stat, "nr_periods").unwrap_or(0),
        nr_throttled: cgroup_stat_field(&stat, "nr_throttled").unwrap_or(0),
        quota_cores,
    })
}

/// Memory accounting for this container's own cgroup. RSS/anon is the
/// non-reclaimable footprint that drives OOMs; usage also includes page cache.
struct CgroupMemory {
    usage_bytes: f64,
    rss_bytes: f64,
    limit_bytes: f64,
}

fn read_cgroup_memory() -> Option<CgroupMemory> {
    read_cgroup_memory_at(Path::new("/sys/fs/cgroup"), Path::new("/proc/self/cgroup"))
}

fn read_cgroup_memory_at(mount_root: &Path, self_cgroup: &Path) -> Option<CgroupMemory> {
    let sane_limit = |value: u64| {
        if value >= 1 << 60 {
            0.0
        } else {
            value as f64
        }
    };

    let v1 = mount_root.join("memory");
    if let Some(usage) = cgroup_read_trim(&v1.join("memory.usage_in_bytes")) {
        let stat = cgroup_read_trim(&v1.join("memory.stat")).unwrap_or_default();
        let limit: u64 = cgroup_read_trim(&v1.join("memory.limit_in_bytes"))
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        return Some(CgroupMemory {
            usage_bytes: usage.parse::<u64>().ok()? as f64,
            rss_bytes: cgroup_stat_field(&stat, "rss").unwrap_or(0) as f64,
            limit_bytes: sane_limit(limit),
        });
    }

    let v2 = cgroup_v2_dir(mount_root, self_cgroup)?;
    let usage: u64 = cgroup_read_trim(&v2.join("memory.current"))?.parse().ok()?;
    let stat = cgroup_read_trim(&v2.join("memory.stat")).unwrap_or_default();
    let limit = cgroup_read_trim(&v2.join("memory.max"))
        .map(|value| {
            if value == "max" {
                0.0
            } else {
                value.parse::<u64>().map(sane_limit).unwrap_or(0.0)
            }
        })
        .unwrap_or(0.0);
    Some(CgroupMemory {
        usage_bytes: usage as f64,
        rss_bytes: cgroup_stat_field(&stat, "anon").unwrap_or(0) as f64,
        limit_bytes: limit,
    })
}

#[cfg(test)]
mod tests {
    use super::{cgroup_v2_dir, read_cgroup_cpu_at, read_cgroup_memory_at};

    #[test]
    fn cgroup_v2_stats_follow_the_process_path_and_preserve_namespace_root() {
        let temp = tempfile::tempdir().unwrap();
        let mount = temp.path().join("cgroup");
        let nested = mount.join("system.slice/mkdb2.service");
        std::fs::create_dir_all(&nested).unwrap();

        std::fs::write(mount.join("cpu.stat"), "usage_usec 9000000\n").unwrap();
        std::fs::write(mount.join("cpu.max"), "max 100000\n").unwrap();
        std::fs::write(mount.join("memory.current"), "900\n").unwrap();
        std::fs::write(mount.join("memory.stat"), "anon 800\n").unwrap();
        std::fs::write(mount.join("memory.max"), "1000\n").unwrap();

        std::fs::write(nested.join("cpu.stat"), "usage_usec 1000000\n").unwrap();
        std::fs::write(nested.join("cpu.max"), "50000 100000\n").unwrap();
        std::fs::write(nested.join("memory.current"), "100\n").unwrap();
        std::fs::write(nested.join("memory.stat"), "anon 80\n").unwrap();
        std::fs::write(nested.join("memory.max"), "200\n").unwrap();

        let proc_file = temp.path().join("self.cgroup");
        std::fs::write(&proc_file, "0::/system.slice/mkdb2.service\n").unwrap();
        let cpu = read_cgroup_cpu_at(&mount, &proc_file).unwrap();
        let memory = read_cgroup_memory_at(&mount, &proc_file).unwrap();
        assert_eq!(cpu.usage_seconds, 1.0);
        assert_eq!(cpu.quota_cores, 0.5);
        assert_eq!(memory.usage_bytes, 100.0);
        assert_eq!(memory.rss_bytes, 80.0);
        assert_eq!(memory.limit_bytes, 200.0);

        std::fs::write(&proc_file, "0::/\n").unwrap();
        assert_eq!(
            read_cgroup_cpu_at(&mount, &proc_file)
                .unwrap()
                .usage_seconds,
            9.0
        );
        assert_eq!(
            read_cgroup_memory_at(&mount, &proc_file)
                .unwrap()
                .usage_bytes,
            900.0
        );

        std::fs::write(&proc_file, "0::/../escape\n").unwrap();
        assert!(cgroup_v2_dir(&mount, &proc_file).is_none());
    }
}
