//! Local server activity owned on one monotonic clock.
//!
//! The supervisor consumes only relative durations from an authenticated RPC; absolute `Instant` values never cross the process boundary. Hosted mode constructs the same cheap tracker but has no supervisor polling it.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Instant;

const MAX_FULFILLED_HOLDS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActivitySnapshot {
    pub(crate) keepalive_idle_for_ms: u64,
    pub(crate) last_committed_ingest_ago_ms: Option<u64>,
    pub(crate) frontend_connections: u32,
    pub(crate) in_flight_work: u32,
    pub(crate) fulfilled_hold_ids: Vec<String>,
}

struct ActivityState {
    last_keepalive: Instant,
    last_committed_ingest: Option<Instant>,
    frontend_connections: u32,
    in_flight_work: u32,
    fulfilled_hold_ids: VecDeque<String>,
}

pub(crate) struct ActivityTracker {
    enabled: bool,
    state: Mutex<ActivityState>,
}

impl ActivityTracker {
    pub(crate) fn new_local() -> Arc<Self> {
        Self::new(true)
    }

    pub(crate) fn disabled() -> Arc<Self> {
        Self::new(false)
    }

    fn new(enabled: bool) -> Arc<Self> {
        let now = Instant::now();
        Arc::new(Self {
            enabled,
            state: Mutex::new(ActivityState {
                last_keepalive: now,
                last_committed_ingest: None,
                frontend_connections: 0,
                in_flight_work: 0,
                fulfilled_hold_ids: VecDeque::new(),
            }),
        })
    }

    pub(crate) fn restart_idle_clock(&self) {
        if !self.enabled {
            return;
        }
        self.state().last_keepalive = Instant::now();
    }

    pub(crate) fn record_committed_ingest(&self) {
        if !self.enabled {
            return;
        }
        let now = Instant::now();
        let mut state = self.state();
        state.last_keepalive = now;
        state.last_committed_ingest = Some(now);
    }

    pub(crate) fn record_lifecycle_mutation(&self, fulfilled_hold_id: Option<&str>) {
        if !self.enabled {
            return;
        }
        let mut state = self.state();
        state.last_keepalive = Instant::now();
        let Some(id) = fulfilled_hold_id.filter(|id| valid_hold_id(id)) else {
            return;
        };
        if !state
            .fulfilled_hold_ids
            .iter()
            .any(|existing| existing == id)
        {
            if state.fulfilled_hold_ids.len() == MAX_FULFILLED_HOLDS {
                state.fulfilled_hold_ids.pop_front();
            }
            state.fulfilled_hold_ids.push_back(id.to_owned());
        }
    }

    pub(crate) fn begin_work(self: &Arc<Self>) -> WorkGuard {
        if !self.enabled {
            return WorkGuard { tracker: None };
        }
        let mut state = self.state();
        state.in_flight_work = state.in_flight_work.saturating_add(1);
        drop(state);
        WorkGuard {
            tracker: Some(self.clone()),
        }
    }

    pub(crate) fn frontend_connected(self: &Arc<Self>) -> FrontendGuard {
        debug_assert!(self.enabled, "hosted mode must not track frontend sockets");
        let mut state = self.state();
        state.frontend_connections = state.frontend_connections.saturating_add(1);
        drop(state);
        FrontendGuard {
            tracker: self.clone(),
        }
    }

    pub(crate) fn snapshot(&self) -> ActivitySnapshot {
        debug_assert!(self.enabled, "hosted mode has no activity consumer");
        let now = Instant::now();
        let state = self.state();
        ActivitySnapshot {
            keepalive_idle_for_ms: elapsed_ms(now, state.last_keepalive),
            last_committed_ingest_ago_ms: state
                .last_committed_ingest
                .map(|instant| elapsed_ms(now, instant)),
            frontend_connections: state.frontend_connections,
            in_flight_work: state.in_flight_work,
            fulfilled_hold_ids: state.fulfilled_hold_ids.iter().cloned().collect(),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, ActivityState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

pub(crate) struct WorkGuard {
    tracker: Option<Arc<ActivityTracker>>,
}

impl Drop for WorkGuard {
    fn drop(&mut self) {
        let Some(tracker) = &self.tracker else {
            return;
        };
        let mut state = tracker.state();
        debug_assert!(state.in_flight_work > 0);
        state.in_flight_work = state.in_flight_work.saturating_sub(1);
    }
}

/// Held by each connected dashboard socket, blocking idle shutdown; dropping it restarts the idle clock.
pub(crate) struct FrontendGuard {
    tracker: Arc<ActivityTracker>,
}

impl Drop for FrontendGuard {
    fn drop(&mut self) {
        let mut state = self.tracker.state();
        debug_assert!(state.frontend_connections > 0);
        state.frontend_connections = state.frontend_connections.saturating_sub(1);
        // Under the same lock as the decrement: no snapshot sees the last viewer gone with a stale clock.
        state.last_keepalive = Instant::now();
    }
}

fn valid_hold_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

fn elapsed_ms(now: Instant, then: Instant) -> u64 {
    now.saturating_duration_since(then)
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn work_and_frontend_guards_are_relative_and_drop_owned() {
        let activity = ActivityTracker::new_local();
        let work = activity.begin_work();
        let frontend = activity.frontend_connected();
        let active = activity.snapshot();
        assert_eq!(active.in_flight_work, 1);
        assert_eq!(active.frontend_connections, 1);
        drop(work);
        activity.state().last_keepalive -= Duration::from_secs(10);
        drop(frontend);
        let idle = activity.snapshot();
        assert_eq!(idle.in_flight_work, 0);
        assert_eq!(idle.frontend_connections, 0);
        // The departing viewer restarted the idle clock.
        assert!(idle.keepalive_idle_for_ms < 10_000);
    }

    #[test]
    fn lifecycle_activity_resets_keepalive_and_reports_exact_hold() {
        let activity = ActivityTracker::new_local();
        activity.record_lifecycle_mutation(Some("init-123"));
        activity.record_lifecycle_mutation(Some("init-123"));
        activity.record_lifecycle_mutation(Some("invalid hold"));
        let snapshot = activity.snapshot();
        assert!(snapshot.keepalive_idle_for_ms < 100);
        assert_eq!(snapshot.fulfilled_hold_ids, ["init-123"]);
    }

    #[test]
    fn committed_ingest_resets_keepalive_without_leaving_work() {
        let activity = ActivityTracker::new_local();
        let work = activity.begin_work();
        assert_eq!(activity.snapshot().in_flight_work, 1);
        activity.record_committed_ingest();
        drop(work);
        let snapshot = activity.snapshot();
        assert_eq!(snapshot.in_flight_work, 0);
        assert!(snapshot.last_committed_ingest_ago_ms.is_some());
    }

    #[test]
    fn hosted_tracker_does_not_lock_or_retain_work() {
        let activity = ActivityTracker::disabled();
        let work = activity.begin_work();
        assert!(work.tracker.is_none());
        activity.restart_idle_clock();
        activity.record_committed_ingest();
        activity.record_lifecycle_mutation(None);
    }
}
