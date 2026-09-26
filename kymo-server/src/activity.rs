//! Local server activity owned on one monotonic clock.
//!
//! The supervisor consumes only relative durations from an authenticated RPC; absolute `Instant` values never cross the process boundary. Hosted mode constructs the same cheap tracker but has no supervisor polling it.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const FRONTEND_RECONNECT_GRACE: Duration = Duration::from_secs(10);
// Covers a slow (e.g. SSH-forwarded) page load from its shell to its WebSocket: asset bodies stream after their request's work guard is released, and the page connects only after the WASM compiles. The connection ends it.
const PAGE_LOAD_GRACE: Duration = Duration::from_secs(60);
const MAX_FULFILLED_HOLDS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActivitySnapshot {
    pub(crate) keepalive_idle_for_ms: u64,
    pub(crate) last_committed_ingest_ago_ms: Option<u64>,
    pub(crate) frontend_reconnect_grace_remaining_ms: u64,
    pub(crate) frontend_connections: u32,
    pub(crate) in_flight_work: u32,
    pub(crate) fulfilled_hold_ids: Vec<String>,
}

struct ActivityState {
    last_keepalive: Instant,
    last_committed_ingest: Option<Instant>,
    frontend_reconnect_until: Option<Instant>,
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
                frontend_reconnect_until: None,
                frontend_connections: 0,
                in_flight_work: 0,
                fulfilled_hold_ids: VecDeque::new(),
            }),
        })
    }

    /// Seed the idle clock only after schema/reconciliation startup is ready to expose listeners; constructor time can precede readiness by minutes.
    pub(crate) fn mark_ready(&self) {
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
        state.frontend_reconnect_until = None;
        drop(state);
        FrontendGuard {
            tracker: self.clone(),
        }
    }

    /// A dashboard page shell was served: hold the stack until the page can connect its WebSocket.
    pub(crate) fn dashboard_page_served(&self) {
        if !self.enabled {
            return;
        }
        let mut state = self.state();
        extend_grace(&mut state, Instant::now() + PAGE_LOAD_GRACE);
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
            frontend_reconnect_grace_remaining_ms: state
                .frontend_reconnect_until
                .map_or(0, |deadline| remaining_ms(now, deadline)),
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

pub(crate) struct FrontendGuard {
    tracker: Arc<ActivityTracker>,
}

impl Drop for FrontendGuard {
    fn drop(&mut self) {
        let mut state = self.tracker.state();
        debug_assert!(state.frontend_connections > 0);
        state.frontend_connections = state.frontend_connections.saturating_sub(1);
        if state.frontend_connections == 0 {
            extend_grace(&mut state, Instant::now() + FRONTEND_RECONNECT_GRACE);
        }
    }
}

fn extend_grace(state: &mut ActivityState, until: Instant) {
    // `None < Some`, so this also starts a grace.
    state.frontend_reconnect_until = state.frontend_reconnect_until.max(Some(until));
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

fn remaining_ms(now: Instant, deadline: Instant) -> u64 {
    deadline
        .saturating_duration_since(now)
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_and_frontend_guards_are_relative_and_drop_owned() {
        let activity = ActivityTracker::new_local();
        let work = activity.begin_work();
        let frontend = activity.frontend_connected();
        let active = activity.snapshot();
        assert_eq!(active.in_flight_work, 1);
        assert_eq!(active.frontend_connections, 1);
        drop(work);
        drop(frontend);
        let idle = activity.snapshot();
        assert_eq!(idle.in_flight_work, 0);
        assert_eq!(idle.frontend_connections, 0);
        assert!(idle.frontend_reconnect_grace_remaining_ms > 0);
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
    fn a_served_page_holds_the_stack_until_it_connects() {
        let activity = ActivityTracker::new_local();
        activity.dashboard_page_served();
        let remaining = activity.snapshot().frontend_reconnect_grace_remaining_ms;
        assert!(remaining > FRONTEND_RECONNECT_GRACE.as_millis() as u64);
        // Once the page connects, the socket itself holds the stack.
        let frontend = activity.frontend_connected();
        let connected = activity.snapshot();
        assert_eq!(connected.frontend_connections, 1);
        assert_eq!(connected.frontend_reconnect_grace_remaining_ms, 0);
        drop(frontend);
    }

    #[test]
    fn hosted_tracker_does_not_lock_or_retain_work() {
        let activity = ActivityTracker::disabled();
        let work = activity.begin_work();
        assert!(work.tracker.is_none());
        activity.mark_ready();
        activity.record_committed_ingest();
        activity.record_lifecycle_mutation(None);
        activity.dashboard_page_served();
    }
}
