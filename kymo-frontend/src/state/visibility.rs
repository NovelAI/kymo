//! Viewport-visibility zones and the chart-fetch priority gate.
//!
//! Every dashboard panel is classified by the shared observers in
//! zones.rs into a [`Zone`]. Data fetching and rendering key off it:
//!
//! - `Visible`: fetches immediately (first paints counted in the
//!   high-priority gate) and renders its chart.
//! - `Near` (within one scroll-container height, or covered by the maximize overlay — see zones.rs): data is PREFETCHED so scrolling into view renders instantly, deferring to visible first paints. Not rendered.
//! - `Far`: completely free. No fetch, no uPlot instance, and version
//!   bumps don't touch it; it catches up when it re-enters the band.

use std::cell::Cell;
use std::time::Duration;

use gloo_timers::future::sleep;

use crate::state::trash::monotonic_now_ms;

/// Where a panel sits relative to the viewport.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Zone {
    Visible,
    Near,
    /// The default: a panel is fetch-gated until its observer reports in
    /// (one frame after mount), exactly like the width-measurement gate.
    #[default]
    Far,
}

thread_local! {
    /// In-flight fetches for VISIBLE panels (wasm is single-threaded, so a
    /// plain Cell suffices). Near-zone prefetches hold off while this is
    /// non-zero.
    static HI_IN_FLIGHT: Cell<u32> = const { Cell::new(0) };
}

/// RAII count of one in-flight visible-panel fetch. Dropping decrements —
/// including when the owning future is cancelled mid-fetch by a resource
/// restart.
pub struct HiFetchToken(());

impl HiFetchToken {
    pub fn new() -> Self {
        HI_IN_FLIGHT.with(|c| c.set(c.get() + 1));
        HiFetchToken(())
    }
}

impl Drop for HiFetchToken {
    fn drop(&mut self) {
        HI_IN_FLIGHT.with(|c| c.set(c.get().saturating_sub(1)));
    }
}

fn hi_idle() -> bool {
    HI_IN_FLIGHT.with(|c| c.get() == 0)
}

/// Hold until every visible FIRST PAINT settles, `keep_waiting` turns false, or the monotonic `deadline_ms` passes. The initial grace beat lets a same-flush burst of visible fetches take their tokens before the first idle check.
pub(crate) async fn wait_for_visible_idle(deadline_ms: f64, keep_waiting: impl Fn() -> bool) {
    let grace_ms = (deadline_ms - monotonic_now_ms()).clamp(0.0, 150.0);
    sleep(Duration::from_millis(grace_ms as u64)).await;
    while !hi_idle() && keep_waiting() && monotonic_now_ms() < deadline_ms {
        sleep(Duration::from_millis(50)).await;
    }
}

/// Admit one panel fetch under the visibility rules — THE preamble for
/// every zone-gated request: park while the tab is hidden, and hold
/// Near-zone prefetches until visible first paints settle. Hold the
/// returned token across the request.
///
/// Only FIRST paints (`first_paint`: the panel has nothing to show yet)
/// take a gate token: a visible panel refreshing data it already renders
/// proceeds immediately but must not hold the gate — live runs refresh
/// every ~2s, and counting those kept the gate busy forever, starving
/// every prefetch.
pub async fn admit_fetch(zone: impl Fn() -> Zone, first_paint: bool) -> Option<HiFetchToken> {
    crate::grpc::wait_until_page_visible().await;
    if zone() == Zone::Near {
        // A Near-zone prefetch waits for the visible first paints (or its promotion), capped: with slow queries and constant live-run churn the gate can stay busy for a long time, and a prefetch delayed past the cap helps nobody — by then it's what the user is about to scroll to.
        wait_for_visible_idle(monotonic_now_ms() + 2_000.0, || zone() == Zone::Near).await;
        crate::grpc::wait_until_page_visible().await;
    }
    (first_paint && zone() == Zone::Visible).then(HiFetchToken::new)
}

/// How long a failed request waits before it is retried.
pub const RETRY_WAIT: Duration = Duration::from_secs(5);

/// Retry `op` until it succeeds, parked while the tab is hidden — retries
/// must not turn a hidden tab into traffic. For error recovery only;
/// steady-state re-runs stay event-driven (callers are cancelled by their
/// resource restarting).
pub async fn retry_visible<T, E: std::fmt::Display>(
    what: &str,
    op: impl AsyncFnMut() -> Result<T, E>,
) -> T {
    match retry_visible_while(what, op, |_| true).await {
        Ok(value) => value,
        Err(_) => unreachable!("an unconditional retry loop cannot return an error"),
    }
}

/// A run read becomes permanently unavailable at expiry. Unlike transient transport/server failures, retrying these statuses cannot heal the request.
pub fn is_terminal_run_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::FailedPrecondition | tonic::Code::NotFound
    )
}

/// Retry transient gRPC failures under the normal visibility gate, but return a terminal run-lifecycle status to the caller so it can clear stale state.
pub async fn retry_visible_run<T>(
    what: &str,
    op: impl AsyncFnMut() -> Result<T, tonic::Status>,
) -> Result<T, tonic::Status> {
    retry_visible_while(what, op, |status| !is_terminal_run_status(status)).await
}

/// Whether no retry of the same request can heal `status`: the run's terminal lifecycle states, or InvalidArgument, the request itself being unaskable (a chart the server rejects, or a saved run key it never accepts). The caller settles instead of retrying the same rejected request every five seconds forever. Not folded into [`is_terminal_run_status`]: an unaskable request is not a lifecycle state.
pub fn retry_cannot_heal(status: &tonic::Status) -> bool {
    is_terminal_run_status(status) || status.code() == tonic::Code::InvalidArgument
}

/// [`retry_visible_run`] that also settles on InvalidArgument ([`retry_cannot_heal`]), so the panel renders the server's explanation.
pub async fn retry_visible_chart<T>(
    what: &str,
    op: impl AsyncFnMut() -> Result<T, tonic::Status>,
) -> Result<T, tonic::Status> {
    retry_visible_while(what, op, |status| !retry_cannot_heal(status)).await
}

async fn retry_visible_while<T, E: std::fmt::Display>(
    what: &str,
    mut op: impl AsyncFnMut() -> Result<T, E>,
    should_retry: impl Fn(&E) -> bool,
) -> Result<T, E> {
    loop {
        crate::grpc::wait_until_page_visible().await;
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if !should_retry(&e) => return Err(e),
            Err(e) => {
                crate::util::warn(&format!("[{what}] failed: {e}; retrying"));
                sleep(RETRY_WAIT).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{is_terminal_run_status, retry_cannot_heal};

    #[test]
    fn terminal_runs_and_unaskable_requests_settle_and_transient_failures_retry() {
        use tonic::Status;
        for (status, terminal, settles) in [
            (Status::failed_precondition("expired"), true, true),
            (Status::not_found("purged"), true, true),
            (Status::invalid_argument("too wide"), false, true),
            (Status::resource_exhausted("pressure"), false, false),
            (Status::unavailable("retry"), false, false),
            (Status::internal("retry"), false, false),
        ] {
            assert_eq!(is_terminal_run_status(&status), terminal, "{status}");
            assert_eq!(retry_cannot_heal(&status), settles, "{status}");
        }
    }
}
