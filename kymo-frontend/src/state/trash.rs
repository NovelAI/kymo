#[cfg(not(target_arch = "wasm32"))]
use std::sync::OnceLock;

use crate::grpc::proto::{ListTrashRequest, RunLifecycleState, RunRecord};
use crate::grpc::GrpcClient;

const IDENTITY_LOOKUP_CHUNK: usize = 1_024;

/// Elapsed-time source for server-clock extrapolation and local deadlines.
/// Unlike Date.now, this cannot jump when the user or NTP changes wall time.
#[cfg(target_arch = "wasm32")]
pub fn monotonic_now_ms() -> f64 {
    web_sys::window()
        .and_then(|window| window.performance())
        .map(|performance| performance.now())
        .unwrap_or_else(js_sys::Date::now)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn monotonic_now_ms() -> f64 {
    static START: OnceLock<std::time::Instant> = OnceLock::new();
    START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_secs_f64()
        * 1_000.0
}

#[cfg(target_arch = "wasm32")]
fn wall_now_ms() -> i64 {
    js_sys::Date::now() as i64
}

#[cfg(not(target_arch = "wasm32"))]
fn wall_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

pub(crate) fn extrapolated_now_ms(server_now: i64, observed_monotonic_ms: f64) -> i64 {
    if server_now <= 0 || observed_monotonic_ms <= 0.0 {
        wall_now_ms()
    } else {
        server_now.saturating_add(
            (monotonic_now_ms() - observed_monotonic_ms)
                .max(0.0)
                .min(i64::MAX as f64) as i64,
        )
    }
}

/// Authoritative lookups for a mutation whose response was ambiguous or lost.
/// Successful typed mutation replies never pay these extra round trips. The
/// chunks are a transport guard only; every supplied identity is checked.
pub async fn lookup_trashed_runs(
    project_id: &str,
    run_ids: &[String],
) -> Result<Vec<RunRecord>, String> {
    if run_ids.is_empty() {
        return Ok(Vec::new());
    }
    let grpc = GrpcClient::new();
    let mut records = Vec::new();
    for chunk in run_ids.chunks(IDENTITY_LOOKUP_CHUNK) {
        crate::grpc::wait_until_page_visible().await;
        let response = grpc
            .reconcile_trash(ListTrashRequest {
                project_id: project_id.to_string(),
                run_ids: chunk.to_vec(),
                ..Default::default()
            })
            .await
            .map_err(|status| status.message().to_string())?;
        records.extend(response.runs);
    }
    Ok(records)
}

pub fn effective_lifecycle(record: &RunRecord, now_ms: i64) -> RunLifecycleState {
    let state = record.state();
    if state == RunLifecycleState::Trashed
        && record
            .purge_at_ms
            .is_some_and(|purge_at| purge_at <= now_ms)
    {
        RunLifecycleState::Expired
    } else {
        state
    }
}

/// Milliseconds until the next expiry-driven render. Ignore deadlines that have already passed, bound active waits so the UI neither spins nor overshoots, and poll faster until an authoritative snapshot is loaded.
pub fn clock_wait_ms(
    now_ms: i64,
    deadlines: impl IntoIterator<Item = i64>,
    has_snapshot: bool,
) -> u64 {
    deadlines
        .into_iter()
        .filter(|deadline| *deadline > now_ms)
        .min()
        .map(|deadline| deadline.saturating_sub(now_ms).clamp(100, 60_000) as u64)
        .unwrap_or(if has_snapshot { 60_000 } else { 1_000 })
}

pub fn compact_duration(mut millis: i64) -> String {
    millis = millis.max(0);
    let seconds = millis / 1_000;
    if seconds < 1 {
        return "<1s".to_string();
    }
    if seconds < 60 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    if minutes < 60 {
        let remainder = seconds % 60;
        return if remainder == 0 {
            format!("{minutes}m")
        } else {
            format!("{minutes}m {remainder}s")
        };
    }
    let hours = minutes / 60;
    if hours < 24 {
        let remainder = minutes % 60;
        return if remainder == 0 {
            format!("{hours}h")
        } else {
            format!("{hours}h {remainder}m")
        };
    }
    let days = hours / 24;
    let remainder = hours % 24;
    if remainder == 0 {
        format!("{days}d")
    } else {
        format!("{days}d {remainder}h")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_keeps_the_useful_two_largest_units() {
        assert_eq!(compact_duration(0), "<1s");
        assert_eq!(compact_duration(999), "<1s");
        assert_eq!(compact_duration(1_000), "1s");
        assert_eq!(compact_duration(59_999), "59s");
        assert_eq!(compact_duration(65_000), "1m 5s");
        assert_eq!(compact_duration(18 * 3_600_000), "18h");
        assert_eq!(compact_duration((6 * 24 + 22) * 3_600_000), "6d 22h");
        assert_eq!(compact_duration(75 * 60_000), "1h 15m");
    }

    #[test]
    fn client_clock_turns_a_trashed_record_expired_at_the_deadline() {
        let record = RunRecord {
            state: RunLifecycleState::Trashed as i32,
            purge_at_ms: Some(10),
            ..Default::default()
        };
        assert_eq!(effective_lifecycle(&record, 9), RunLifecycleState::Trashed);
        assert_eq!(effective_lifecycle(&record, 10), RunLifecycleState::Expired);
    }

    #[test]
    fn clock_wait_uses_the_nearest_future_deadline_and_idle_fallbacks() {
        assert_eq!(clock_wait_ms(1_000, None, false), 1_000);
        assert_eq!(clock_wait_ms(1_000, None, true), 60_000);
        assert_eq!(clock_wait_ms(1_000, [900, 1_000], true), 60_000);
        assert_eq!(clock_wait_ms(1_000, [900, 1_000, 1_001, 2_000], true), 100);
        assert_eq!(clock_wait_ms(1_000, [2_000, 1_500], true), 500);
        assert_eq!(clock_wait_ms(1_000, [100_000], true), 60_000);
    }
}
