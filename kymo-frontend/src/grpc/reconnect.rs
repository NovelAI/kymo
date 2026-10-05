//! When a disconnected tab may (re)open the dashboard socket.
//!
//! Firefox (netwerk/protocol/websocket/WebSocketChannel.cpp) delays a WebSocket to an endpoint that failed recently, by up to 60 s; it forgets the failures when a connection succeeds or when an attempt starts 60 s plus the current delay after the last one. Every failed attempt, and every drop without a close handshake, in any tab counts; the server closes its sockets with that handshake when it shuts down, so a planned stop adds none for the tabs it closes. The delayed socket holds the single connecting slot Firefox allows per IP and site. Tabs retrying every second through an outage drive that delay to a minute, so once the server is back a fresh load or reload can sit on "Connecting to the server…" for up to 60 s.
//!
//! So an origin's tabs share one schedule in localStorage: retry 1, 2, 4, 8 and 16 s after successive failures, then every 125 s, which outlasts Firefox's memory however many failures it recorded (tabs can't count them: their writes race). Once the schedule reaches its 125 s waits, Firefox holds no failures against a fresh load, unless a user-intent attempt failed since. An attempt's claim holds the other tabs until its outcome, and jitter spreads them, so each attempt is usually one tab's; a success clears the record, and the waiting tabs follow one after another. User intent makes a tab attempt at once: loading the page or coming back online in a shown tab; focusing the window, which switching to the tab does; issuing a mutation or its verifying read. No probe other than the socket itself is sent; the price is that after an outage outlasting the fast retries, a tab can take up to 125 s to notice the server came back, even one in use: clicks and navigation in a window that already has focus aren't intent, while reloading or switching windows retries at once.

use std::sync::LazyLock;
use std::time::Duration;

use futures::{select, FutureExt};
use serde::{Deserialize, Serialize};
use wasm_bindgen::{closure::Closure, JsCast};

use super::ws::{page_hidden, Watch};
use crate::util::local_storage;

const KEY: &str = "kymo_ws_reconnect";
/// The waits after the 1st to 5th consecutive failure.
const FAST_MS: [f64; 5] = [1_000.0, 2_000.0, 4_000.0, 8_000.0, 16_000.0];
/// Firefox's 60 s of memory after a failure plus its longest delay, and a margin.
/// A claimed attempt holds other tabs back this long too, in case its tab dies before the outcome: longer than Firefox's longest delay plus its 20 s handshake timeout.
const SLOW_MS: f64 = 125_000.0;
/// Spreads tabs that wake for the same scheduled attempt.
const JITTER_MS: f64 = 1_000.0;

/// Retry state shared by the origin's tabs, whose bundles may differ: only ever add fields, and let them go missing, since an older bundle rewrites the value without them.
#[derive(Clone, Copy, Default, Serialize, Deserialize)]
#[serde(default)]
struct Shared {
    /// Attempts that never opened since the last success, plus one for the drop that began the outage.
    failures: u32,
    /// `Date.now()` milliseconds before which a tab attempts only on intent: the wait after the latest failure, or the claim of an attempt in flight. A success clears it with the count.
    due: f64,
}

impl Shared {
    fn fail(&mut self, now: f64) {
        self.failures = self.failures.saturating_add(1);
        self.due = now + self.backoff();
    }

    /// An open socket closed. Every tab loses its socket at once, and the first to notice starts the schedule over, unless a live one (a claim or a wait) already holds the tabs.
    fn drop_socket(&mut self, now: f64) {
        if self.stale(now) {
            self.failures = 0;
            self.fail(now);
        }
    }

    /// A `due` more than `SLOW_MS + JITTER_MS` ahead or behind is stale: claims last `SLOW_MS`, attempts settle within 90 s, and the jitter tolerates another tab reading a slightly earlier clock (Firefox, by a millisecond). A cleared record is stale too.
    /// Behind, the record survived an earlier outage (every tab closed, or a failure landed after a success), so Firefox has forgotten it; ahead, the clock moved back. Either way a waiting tab attempts at once, and its claim starts the count over.
    fn stale(&self, now: f64) -> bool {
        (now - self.due).abs() > SLOW_MS + JITTER_MS
    }

    /// Hold other tabs back until this attempt's outcome replaces `due`.
    fn claim(&mut self, now: f64) {
        if self.stale(now) {
            self.failures = 0;
        }
        self.due = now + SLOW_MS;
    }

    fn backoff(&self) -> f64 {
        match self.failures {
            n @ 1..=5 => FAST_MS[n as usize - 1],
            _ => SLOW_MS,
        }
    }

    /// When a disconnected tab may attempt; `None` means now: on intent, or when nothing holds the tabs.
    fn next_attempt(&self, intent: bool, now: f64) -> Option<f64> {
        if intent || self.stale(now) {
            return None;
        }
        (now < self.due).then_some(self.due)
    }
}

/// Pending user intent; a storage change only wakes the waiting task.
static WAKE: LazyLock<Watch> = LazyLock::new(Default::default);

/// Re-evaluate a waiting tab's turn; `intent` makes it attempt at once.
pub(super) fn wake(intent: bool) {
    WAKE.update(|pending| {
        *pending |= intent;
        true
    });
}

/// This tab's side of the schedule, owned by the connection task.
pub(super) struct Reconnect {
    /// This tab's copy, used while storage is unavailable or holds nothing readable.
    local: Shared,
}

impl Reconnect {
    pub(super) fn new() -> Self {
        if let Some(window) = web_sys::window() {
            let listen = |event: &str, on_event: fn()| {
                let listener = Closure::<dyn Fn()>::new(on_event);
                let _ = window
                    .add_event_listener_with_callback(event, listener.as_ref().unchecked_ref());
                // The connection task, and so the schedule, lives as long as the page.
                listener.forget();
            };
            listen("storage", || wake(false));
            // Switching to a tab focuses its window too, so focus is the one intent for it: also taking the visibility flip, which arrives separately, would retry twice.
            listen("focus", || wake(true));
            // `online` reaches every tab, so only a shown one takes it as intent; the rest would each add a failure if the server is still away.
            listen("online", || wake(!page_hidden()));
        }
        // Loading the page is intent, unless it loads in a background tab: during an outage each would add a failure, and switching to one focuses it.
        wake(!page_hidden());
        Self {
            local: Shared::default(),
        }
    }

    fn load(&mut self) -> Shared {
        if let Some(stored) = local_storage::get(KEY).and_then(|v| serde_json::from_str(&v).ok()) {
            self.local = stored;
        }
        self.local
    }

    fn update(&mut self, outcome: impl FnOnce(&mut Shared, f64)) {
        self.load();
        outcome(&mut self.local, js_sys::Date::now());
        let json = serde_json::to_string(&self.local).expect("plain numbers serialize");
        // A refused write must not leave older state to be read back over this tab's.
        if !local_storage::set(KEY, &json) {
            local_storage::remove(KEY);
        }
    }

    /// Wait until this tab may attempt, then claim the attempt.
    pub(super) async fn turn(&mut self) {
        loop {
            let (seen, intent) = WAKE.get();
            let now = js_sys::Date::now();
            let Some(at) = self.load().next_attempt(intent, now) else {
                break;
            };
            let ms = at - now + js_sys::Math::random() * JITTER_MS;
            select! {
                _ = gloo_timers::future::sleep(Duration::from_millis(ms as u64)).fuse() => {}
                _ = WAKE.changed(seen).fuse() => {}
            }
        }
        // This claim answers the intent shown so far; intent shown while its attempt runs stays pending for the next one.
        WAKE.update(std::mem::take);
        self.update(Shared::claim);
    }

    pub(super) fn failed(&mut self) {
        self.update(Shared::fail);
    }

    pub(super) fn opened(&mut self) {
        self.update(|shared, _| *shared = Shared::default());
    }

    pub(super) fn dropped(&mut self) {
        // Intent shown while connected doesn't skip the first retry.
        WAKE.update(std::mem::take);
        self.update(Shared::drop_socket);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: f64 = 1_700_000_000_000.0;

    fn after_failures(times: &[f64]) -> Shared {
        let mut shared = Shared::default();
        for &at in times {
            shared.fail(at);
        }
        shared
    }

    #[test]
    fn intent_attempts_at_once() {
        assert_eq!(Shared::default().next_attempt(true, T), None);
        // Even mid-outage, with another tab's attempt in flight.
        let mut shared = after_failures(&[T - 500.0]);
        shared.claim(T - 100.0);
        assert_eq!(shared.next_attempt(true, T), None);
    }

    #[test]
    fn tabs_dropping_together_retry_once_after_a_second() {
        let mut shared = Shared::default();
        for i in 0..4 {
            shared.drop_socket(T + i as f64);
        }
        assert_eq!(shared.failures, 1);
        assert_eq!(shared.next_attempt(false, T + 3.0), Some(T + 1_000.0));
        assert_eq!(shared.next_attempt(false, T + 1_000.0), None);
    }

    #[test]
    fn waits_double_from_one_to_sixteen_seconds_then_outlast_firefox() {
        let mut shared = Shared::default();
        let mut now = T;
        for expected in FAST_MS {
            shared.fail(now);
            assert_eq!(shared.backoff(), expected);
            now += expected;
        }
        shared.fail(now);
        // Firefox forgets a failure at most 60 s of memory plus 60 s of delay later.
        assert!(shared.backoff() > 120_000.0);
    }

    #[test]
    fn another_tabs_success_makes_waiting_tabs_attempt_at_once() {
        let mut shared = after_failures(&[T, T + 1_000.0, T + 3_000.0]);
        assert!(shared.next_attempt(false, T + 3_500.0).is_some());
        shared = Shared::default();
        assert_eq!(shared.next_attempt(false, T + 3_700.0), None);
        // If the server went away again, the waiting tab's failed attempt restarts the schedule.
        shared.fail(T + 3_800.0);
        assert_eq!(shared.next_attempt(false, T + 3_900.0), Some(T + 4_800.0));
    }

    #[test]
    fn a_first_attempt_holds_other_tabs_until_its_outcome() {
        // A fresh or cleared record lets any tab attempt, and its claim then holds the rest, so tabs loading into an unrecorded outage don't all fail at once.
        let mut shared = Shared::default();
        assert_eq!(shared.next_attempt(false, T), None);
        shared.claim(T);
        assert_eq!(shared.next_attempt(false, T + 1.0), Some(T + SLOW_MS));
    }

    #[test]
    fn an_attempt_in_flight_holds_other_tabs_until_its_outcome_or_claim_expiry() {
        let mut shared = after_failures(&[T]);
        shared.claim(T + 1_000.0);
        let expiry = T + 1_000.0 + SLOW_MS;
        assert_eq!(shared.next_attempt(false, T + 2_000.0), Some(expiry));
        assert_eq!(shared.next_attempt(false, expiry), None);
        // The outcome replaces the claim.
        shared.fail(T + 1_500.0);
        assert_eq!(shared.next_attempt(false, T + 1_600.0), Some(T + 3_500.0));
    }

    #[test]
    fn a_count_left_by_an_earlier_outage_starts_over() {
        let mut shared = Shared::default();
        for _ in 0..6 {
            shared.fail(T);
        }
        assert_eq!(shared.backoff(), SLOW_MS);
        let later = shared.due + SLOW_MS + JITTER_MS + 1.0;
        let mut dropped = shared;
        dropped.drop_socket(later);
        assert_eq!(dropped.next_attempt(false, later), Some(later + 1_000.0));
        // An attempt on the stale count holds the other tabs while in flight, and the next failure starts over, even in another tab after the claiming one closed.
        let mut claimed = shared;
        claimed.claim(later);
        assert_eq!(
            claimed.next_attempt(false, later + 1.0),
            Some(later + SLOW_MS)
        );
        claimed.claim(later + SLOW_MS);
        claimed.fail(later + SLOW_MS + 2.0);
        assert_eq!(claimed.backoff(), 1_000.0);
    }

    #[test]
    fn a_clock_set_back_attempts_at_once() {
        let shared = after_failures(&[T]);
        assert_eq!(shared.next_attempt(false, T - 3_600_000.0), None);
    }

    #[test]
    fn a_fresh_claim_read_a_millisecond_early_still_holds() {
        let mut shared = after_failures(&[T]);
        shared.claim(T);
        assert_eq!(shared.next_attempt(false, T - 1.0), Some(T + SLOW_MS));
    }
}
