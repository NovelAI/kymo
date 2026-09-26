//! Run-liveness durations shared by server components and the local lifecycle runtime.

// Each path-including crate intentionally consumes a different subset.
#![allow(dead_code)]

use std::time::Duration;

const RUNNING_WINDOW_SECS: u64 = 10;
const PRESUMED_DEAD_WINDOW_SECS: u64 = 10 * 60;
const STATUS_WATCH_MARGIN_SECS: u64 = 60;

pub const RUNNING_WINDOW: Duration = Duration::from_secs(RUNNING_WINDOW_SECS);
pub const PRESUMED_DEAD_WINDOW: Duration = Duration::from_secs(PRESUMED_DEAD_WINDOW_SECS);
pub const STATUS_WATCH_MARGIN: Duration = Duration::from_secs(STATUS_WATCH_MARGIN_SECS);
pub const STATUS_WATCH_WINDOW: Duration =
    Duration::from_secs(PRESUMED_DEAD_WINDOW.as_secs() + STATUS_WATCH_MARGIN.as_secs());
pub const LOCAL_IDLE_TIMEOUT: Duration = Duration::from_secs(60 * 60);

pub const RUNNING_WINDOW_MS: i64 = duration_millis(RUNNING_WINDOW);
pub const PRESUMED_DEAD_WINDOW_MS: i64 = duration_millis(PRESUMED_DEAD_WINDOW);

const fn duration_millis(duration: Duration) -> i64 {
    (duration.as_secs() * 1_000 + duration.subsec_millis() as u64) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_windows_preserve_existing_status_thresholds() {
        assert_eq!(RUNNING_WINDOW, Duration::from_secs(10));
        assert_eq!(PRESUMED_DEAD_WINDOW, Duration::from_secs(10 * 60));
        assert_eq!(STATUS_WATCH_MARGIN, Duration::from_secs(60));
        assert_eq!(STATUS_WATCH_WINDOW, Duration::from_secs(11 * 60));
        assert_eq!(LOCAL_IDLE_TIMEOUT, Duration::from_secs(60 * 60));
    }
}
