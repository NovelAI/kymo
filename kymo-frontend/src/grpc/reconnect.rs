//! When a disconnected tab may (re)open the dashboard socket.
//!
//! Firefox (netwerk/protocol/websocket/WebSocketChannel.cpp) delays a WebSocket to an endpoint that failed recently, by up to 60 s; it forgets the failures when a connection succeeds or when an attempt starts 60 s plus the current delay after the last one. Every failed attempt, and every drop without a close handshake, in any tab counts; the server closes its sockets with that handshake when it shuts down, so a planned stop adds none for the tabs it closes. The delayed socket holds the single connecting slot Firefox allows per IP and site. Tabs retrying every second through an outage drive that delay to a minute, so once the server is back a fresh load or reload can sit on "Connecting to the server…" for up to 60 s.
//!
//! So an origin's tabs share one schedule in localStorage: retry 1, 2, 4, 8 and 16 s after successive failures, then every 125 s, which outlasts Firefox's memory however many failures it recorded (tabs can't count them: their writes race). Once the schedule reaches its 125 s waits, Firefox holds no failures against a fresh load, unless a user-intent attempt failed since. An attempt's claim holds the other tabs until its outcome, and jitter spreads them, so each attempt is usually one tab's; a success clears the record, and the waiting tabs follow at once. User intent makes a tab attempt at once: loading the page in a shown tab; coming back online in the focused window; focusing the window, which switching to the tab does; issuing a mutation or its verifying read. No probe other than the socket itself is sent; the price is that after an outage outlasting the fast retries, a tab can take up to about 126 s to notice the server came back (longer while hidden, if the browser throttles its timers), even one in use, since clicks and navigation in a window that already has focus aren't intent.

use std::sync::LazyLock;
use std::time::Duration;

use futures::{select, FutureExt};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use wasm_bindgen::{closure::Closure, JsCast};

use super::ws::page_hidden;
use crate::util::local_storage;

const KEY: &str = "kymo_ws_reconnect";
/// The waits after the 1st to 5th consecutive failure.
const FAST_MS: [f64; 5] = [1_000.0, 2_000.0, 4_000.0, 8_000.0, 16_000.0];
/// Firefox's 60 s of memory after a failure plus its longest delay, and a margin.
/// A claimed attempt holds other tabs back this long too, in case its tab dies before the outcome: longer than Firefox's longest delay plus its 20 s handshake timeout.
const SLOW_MS: f64 = 125_000.0;
/// Spreads tabs that find the same attempt due; also the second to which a hidden tab's browser holds shorter timers, which the windows and plans below lean on.
const JITTER_MS: f64 = 1_000.0;

/// Pending user intent; another tab's write to the record only wakes the waiting task.
static WAKE: LazyLock<watch::Sender<bool>> = LazyLock::new(|| watch::Sender::new(false));

/// Re-evaluate a waiting tab's turn; `intent` makes it attempt at once. Wakes without intent must stay rare, as record writes are: each one restarts the plan of a tab the record doesn't hold.
pub(super) fn wake(intent: bool) {
    WAKE.send_modify(|pending| *pending |= intent);
}

/// This tab's copy of the retry state its origin's tabs share in localStorage, owned by the connection task; it stands in while storage is unavailable or holds nothing readable.
/// The tabs' bundles may differ: only ever add fields, and let them go missing, since an older bundle rewrites the value without them.
#[derive(Clone, Copy, Default, Serialize, Deserialize)]
#[serde(default)]
pub(super) struct Reconnect {
    /// Attempts that never opened since the last success, plus one for the drop that began the outage.
    failures: u32,
    /// `Date.now()` milliseconds before which a tab attempts only on intent: the wait after the latest failure, or the claim of an attempt in flight. A success clears it with the count, so a zero `due` marks a record cleared (or never written).
    due: f64,
}

impl Reconnect {
    pub(super) fn new() -> Self {
        if let Some(window) = web_sys::window() {
            let listen = |event: &str, on_event: fn(web_sys::Event)| {
                let listener = Closure::<dyn Fn(web_sys::Event)>::new(on_event);
                let _ = window
                    .add_event_listener_with_callback(event, listener.as_ref().unchecked_ref());
                // The connection task, and so the schedule, lives as long as the page.
                listener.forget();
            };
            // Only writes of the record: its removal or a clear() leaves every tab its own copy (see load), and wakes without intent must stay rare (see wake).
            listen("storage", |event| {
                let event = event.unchecked_into::<web_sys::StorageEvent>();
                if event.key().as_deref() == Some(KEY) && event.new_value().is_some() {
                    wake(false);
                }
            });
            // Switching to a tab focuses its window too, so focus is the one intent for it: also taking the visibility flip, which arrives separately, would retry twice.
            listen("focus", |_| wake(true));
            // `online` reaches every tab, so only the focused window's takes it, as intent; the rest would each add a failure if the server is still away.
            listen("online", |_| {
                if web_sys::window()
                    .and_then(|window| window.document())
                    .is_some_and(|document| document.has_focus().unwrap_or(false))
                {
                    wake(true);
                }
            });
        }
        // Loading the page is intent, unless it loads in a background tab: during an outage each would add a failure, and switching to one focuses it.
        wake(!page_hidden());
        Self::default()
    }

    /// Wait until this tab may attempt, then claim the attempt.
    pub(super) async fn turn(&mut self) {
        // Drawn once per turn and kept across wakes: another tab's write wakes every tab at once.
        let jitter = js_sys::Math::random() * JITTER_MS;
        let mut planned = false;
        let mut fired = false;
        let mut wakes = WAKE.subscribe();
        loop {
            let intent = *wakes.borrow_and_update();
            let now = js_sys::Date::now();
            self.load();
            let Some(at) =
                self.next_attempt(intent, fired, page_hidden(), now, jitter, &mut planned)
            else {
                break;
            };
            // A millisecond over: Firefox can fire a timer just before Date.now() reaches its time.
            let ms = (at - now).ceil() as u64 + 1;
            fired = select! {
                _ = gloo_timers::future::sleep(Duration::from_millis(ms)).fuse() => true,
                _ = wakes.changed().fuse() => false,
            };
        }
        // This claim answers the intent shown so far; intent shown while its attempt runs stays pending for the next one.
        WAKE.send_replace(false);
        self.update(Self::claim);
    }

    pub(super) fn failed(&mut self) {
        self.update(Self::fail);
    }

    pub(super) fn opened(&mut self) {
        self.update(|record, _| *record = Self::default());
    }

    pub(super) fn dropped(&mut self) {
        // Intent shown while connected doesn't skip the first retry.
        WAKE.send_replace(false);
        self.update(Self::drop_socket);
    }

    fn load(&mut self) {
        if let Some(stored) = local_storage::get(KEY).and_then(|v| serde_json::from_str(&v).ok()) {
            *self = stored;
        }
    }

    fn update(&mut self, outcome: impl FnOnce(&mut Self, f64)) {
        self.load();
        outcome(self, js_sys::Date::now());
        let json = serde_json::to_string(self).expect("plain numbers serialize");
        // A refused write must not leave older state to be read back over this tab's.
        if !local_storage::set(KEY, &json) {
            local_storage::remove(KEY);
        }
    }

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

    /// A `due` more than `SLOW_MS + JITTER_MS` ahead or behind is stale: claims last `SLOW_MS`, attempts settle within 90 s, and the jitter tolerates another tab reading a slightly earlier clock (Firefox, by a millisecond). A cleared record is stale too, and attempts at once.
    /// Behind, the record survived an earlier outage (every tab closed, or a failure landed after a success), so Firefox has forgotten it; ahead, the clock moved back. Either way it holds no tab, and the next claim starts the count over.
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

    /// When a disconnected tab may attempt; `None` means now. Intent, and a record a success cleared, attempt at once. Otherwise a tab waits for a live `due` plus its `jitter` in one timer, and attempts if that timer fires (`fired`) within a second of its time: a hidden tab's browser holds shorter timers to a second (Firefox) or to whole seconds (Chromium), so a separate jitter wait would collapse, and a hidden tab arms no timer under a second.
    /// A tab that finds the record stale, that time passed without its own timer within the second (a first look, or a write that reached it late), its own timer over a second late (as after a sleep), or, if hidden, that time under a second ahead (a write that reached it late), plans a second plus its jitter from then, so tabs that find it together still spread. `planned` marks that timer, which attempts whenever it fires, however late (Chromium aligns a long-hidden tab's chained timers to the minute); a wake before then plans again.
    fn next_attempt(
        &self,
        intent: bool,
        fired: bool,
        hidden: bool,
        now: f64,
        jitter: f64,
        planned: &mut bool,
    ) -> Option<f64> {
        if intent || self.due == 0.0 {
            return None;
        }
        let at = self.due + jitter;
        // A hidden tab's browser would hold a timer for `at` to a second.
        let held = hidden && now < at && at - now < JITTER_MS;
        if !self.stale(now) && !held && (now < at || fired && now <= at + JITTER_MS) {
            *planned = false;
            return (now < at).then_some(at);
        }
        if fired && *planned {
            return None;
        }
        *planned = true;
        Some(now + JITTER_MS + jitter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: f64 = 1_700_000_000_000.0;

    /// `next_attempt` for the cases that set nothing: no intent, the tab's timer fired, a shown tab, no jitter, no plan.
    fn attempt(record: &Reconnect, now: f64) -> Option<f64> {
        record.next_attempt(false, true, false, now, 0.0, &mut false)
    }

    fn after_failures(times: &[f64]) -> Reconnect {
        let mut shared = Reconnect::default();
        for &at in times {
            shared.fail(at);
        }
        shared
    }

    #[test]
    fn intent_attempts_at_once() {
        assert_eq!(
            Reconnect::default().next_attempt(true, true, false, T, 0.0, &mut false),
            None
        );
        // Even mid-outage, with another tab's attempt in flight.
        let mut shared = after_failures(&[T - 500.0]);
        shared.claim(T - 100.0);
        assert_eq!(
            shared.next_attempt(true, true, false, T, 0.0, &mut false),
            None
        );
    }

    #[test]
    fn tabs_dropping_together_retry_once_after_a_second() {
        let mut shared = Reconnect::default();
        for i in 0..4 {
            shared.drop_socket(T + i as f64);
        }
        assert_eq!(shared.failures, 1);
        assert_eq!(attempt(&shared, T + 3.0), Some(T + 1_000.0));
        assert_eq!(attempt(&shared, T + 1_000.0), None);
    }

    #[test]
    fn waits_double_from_one_to_sixteen_seconds_then_outlast_firefox() {
        let mut shared = Reconnect::default();
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
        assert!(attempt(&shared, T + 3_500.0).is_some());
        shared = Reconnect::default();
        assert_eq!(attempt(&shared, T + 3_700.0), None);
        // If the server went away again, the waiting tab's failed attempt restarts the schedule.
        shared.fail(T + 3_800.0);
        assert_eq!(attempt(&shared, T + 3_900.0), Some(T + 4_800.0));
    }

    #[test]
    fn a_first_attempt_holds_other_tabs_until_its_outcome() {
        // A fresh or cleared record lets any tab attempt, and its claim then holds the rest, so tabs loading into an unrecorded outage don't all fail at once.
        let mut shared = Reconnect::default();
        assert_eq!(attempt(&shared, T), None);
        shared.claim(T);
        assert_eq!(attempt(&shared, T + 1.0), Some(T + SLOW_MS));
    }

    #[test]
    fn an_attempt_in_flight_holds_other_tabs_until_its_outcome_or_claim_expiry() {
        let mut shared = after_failures(&[T]);
        shared.claim(T + 1_000.0);
        let expiry = T + 1_000.0 + SLOW_MS;
        assert_eq!(attempt(&shared, T + 2_000.0), Some(expiry));
        assert_eq!(attempt(&shared, expiry), None);
        // The outcome replaces the claim.
        shared.fail(T + 1_500.0);
        assert_eq!(attempt(&shared, T + 1_600.0), Some(T + 3_500.0));
    }

    #[test]
    fn a_count_left_by_an_earlier_outage_starts_over() {
        let mut shared = Reconnect::default();
        for _ in 0..6 {
            shared.fail(T);
        }
        assert_eq!(shared.backoff(), SLOW_MS);
        let later = shared.due + SLOW_MS + JITTER_MS + 1.0;
        let mut dropped = shared;
        dropped.drop_socket(later);
        assert_eq!(attempt(&dropped, later), Some(later + 1_000.0));
        // An attempt on the stale count holds the other tabs while in flight, and the next failure starts over, even in another tab after the claiming one closed.
        let mut claimed = shared;
        claimed.claim(later);
        assert_eq!(attempt(&claimed, later + 1.0), Some(later + SLOW_MS));
        claimed.claim(later + SLOW_MS);
        claimed.fail(later + SLOW_MS + 2.0);
        assert_eq!(claimed.backoff(), 1_000.0);
    }

    #[test]
    fn a_tab_waits_for_a_live_due_plus_its_jitter_in_one_timer() {
        let shared = after_failures(&[T]);
        let due = T + 1_000.0;
        let mut planned = false;
        assert_eq!(
            shared.next_attempt(false, false, false, T + 10.0, 800.0, &mut planned),
            Some(due + 800.0)
        );
        assert_eq!(
            shared.next_attempt(false, true, false, due + 800.0, 800.0, &mut planned),
            None
        );
        // A timer held to the next whole second still attempts.
        assert_eq!(
            shared.next_attempt(false, true, false, due + 1_500.0, 800.0, &mut false),
            None
        );
        // The same time found passed by another look, as when a write reached the tabs late, spreads instead.
        assert_eq!(
            shared.next_attempt(false, false, false, due + 1_500.0, 800.0, &mut false),
            Some(due + 3_300.0)
        );
    }

    #[test]
    fn a_hidden_tab_arms_no_timer_under_a_second() {
        let shared = after_failures(&[T]);
        let due = T + 1_000.0;
        // A write that reached the tab 600 ms late: a visible tab waits for its time exactly, a hidden one, whose browser would hold that timer to a second, spreads from now.
        assert_eq!(
            shared.next_attempt(false, false, false, T + 600.0, 200.0, &mut false),
            Some(due + 200.0)
        );
        let mut planned = false;
        assert_eq!(
            shared.next_attempt(false, false, true, T + 600.0, 200.0, &mut planned),
            Some(T + 1_800.0)
        );
        // A wake plans again, and the plan's own timer attempts.
        assert_eq!(
            shared.next_attempt(false, false, true, T + 1_000.0, 200.0, &mut planned),
            Some(T + 2_200.0)
        );
        assert_eq!(
            shared.next_attempt(false, true, true, T + 2_201.0, 200.0, &mut planned),
            None
        );
    }

    #[test]
    fn a_late_or_stale_tab_spreads_from_when_it_looked() {
        let shared = after_failures(&[T]);
        let due = T + 1_000.0;
        // Overslept, as after a sleep: a second plus the jitter from when this tab looked.
        let mut planned = false;
        assert_eq!(
            shared.next_attempt(false, true, false, due + 60_000.0, 800.0, &mut planned),
            Some(due + 61_800.0)
        );
        // A wake plans again, and the plan's own timer attempts however late it fires (Chromium aligns a long-hidden tab's chained timers to the minute).
        assert_eq!(
            shared.next_attempt(false, false, false, due + 60_300.0, 800.0, &mut planned),
            Some(due + 62_100.0)
        );
        assert_eq!(
            shared.next_attempt(false, true, false, due + 122_100.0, 800.0, &mut planned),
            None
        );
        // A claim seen meanwhile holds the tab and drops its plan.
        let mut claimed = shared;
        claimed.claim(due + 60_100.0);
        let mut planned = true;
        let held = Some(due + 60_100.0 + SLOW_MS + 800.0);
        assert_eq!(
            claimed.next_attempt(false, false, false, due + 60_200.0, 800.0, &mut planned),
            held
        );
        assert!(!planned);
        // A stale record spreads too, but one a success cleared attempts at once.
        let stale = due + 1_000_000.0;
        let spread = Some(stale + JITTER_MS + 800.0);
        assert_eq!(
            shared.next_attempt(false, false, false, stale, 800.0, &mut false),
            spread
        );
        assert_eq!(
            Reconnect::default().next_attempt(false, true, false, due, 800.0, &mut false),
            None
        );
    }

    #[test]
    fn a_dead_tabs_stale_claim_spreads_instead_of_bursting() {
        // A first claim zeroes the count; if its tab dies, the record goes stale with no count but is not a cleared one.
        let mut claimed = Reconnect::default();
        claimed.claim(T);
        let later = T + SLOW_MS + SLOW_MS + JITTER_MS + 1.0;
        let at = Some(later + JITTER_MS + 800.0);
        assert_eq!(
            claimed.next_attempt(false, false, false, later, 800.0, &mut false),
            at
        );
    }

    #[test]
    fn a_clock_set_back_stops_holding_the_tabs() {
        let shared = after_failures(&[T]);
        let now = T - 3_600_000.0;
        assert_eq!(attempt(&shared, now), Some(now + JITTER_MS));
    }

    #[test]
    fn a_fresh_claim_read_a_millisecond_early_still_holds() {
        let mut shared = after_failures(&[T]);
        shared.claim(T);
        assert_eq!(attempt(&shared, T - 1.0), Some(T + SLOW_MS));
    }
}
