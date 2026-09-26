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

/// Hold a Near-zone prefetch until every visible FIRST PAINT settles (or
/// the panel is promoted mid-wait). The initial grace beat lets a
/// same-flush burst of visible fetches take their tokens before the first
/// idle check. Capped: with slow queries and constant live-run churn the
/// gate can stay busy for a long time, and a prefetch delayed past the cap
/// helps nobody — by then it's what the user is about to scroll to.
async fn wait_for_visible_idle(is_still_near: impl Fn() -> bool) {
    let mut waited_ms = 0u32;
    sleep(Duration::from_millis(150)).await;
    while !hi_idle() && is_still_near() && waited_ms < 2_000 {
        sleep(Duration::from_millis(50)).await;
        waited_ms += 50;
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
        wait_for_visible_idle(|| zone() == Zone::Near).await;
        crate::grpc::wait_until_page_visible().await;
    }
    (first_paint && zone() == Zone::Visible).then(HiFetchToken::new)
}

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

/// Chart validation errors cannot heal on a retry of the same request. Keep
/// lifecycle terminal handling, and additionally return InvalidArgument so
/// the panel can render the server's explanation instead of issuing the same
/// rejected chart every five seconds forever.
pub fn is_terminal_chart_status(status: &tonic::Status) -> bool {
    is_terminal_run_status(status) || status.code() == tonic::Code::InvalidArgument
}

pub async fn retry_visible_chart<T>(
    what: &str,
    op: impl AsyncFnMut() -> Result<T, tonic::Status>,
) -> Result<T, tonic::Status> {
    retry_visible_while(what, op, |status| !is_terminal_chart_status(status)).await
}

/// One attempt behind the visibility gate — the non-retrying counterpart of
/// [`retry_visible_run`], for callers that schedule their own retries and so
/// need the gate on each individual attempt.
///
/// Gating a whole fan-out once is not equivalent: `buffer_unordered` admits
/// later requests as slots free, long after the pass started, so a request
/// admitted after the tab was hidden would fire unparked.
pub async fn visible_attempt<T, E>(op: impl AsyncFnOnce() -> Result<T, E>) -> Result<T, E> {
    crate::grpc::wait_until_page_visible().await;
    op().await
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
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{is_terminal_chart_status, is_terminal_run_status};

    #[test]
    fn only_terminal_run_lifecycle_codes_stop_retries() {
        assert!(is_terminal_run_status(&tonic::Status::failed_precondition(
            "expired"
        )));
        assert!(is_terminal_run_status(&tonic::Status::not_found("purged")));
        assert!(!is_terminal_run_status(&tonic::Status::unavailable(
            "retry"
        )));
        assert!(!is_terminal_run_status(&tonic::Status::internal("retry")));
    }

    #[test]
    fn invalid_chart_shapes_stop_but_transient_capacity_retries() {
        assert!(is_terminal_chart_status(&tonic::Status::invalid_argument(
            "too wide"
        )));
        assert!(is_terminal_chart_status(
            &tonic::Status::failed_precondition("trashed")
        ));
        assert!(!is_terminal_chart_status(
            &tonic::Status::resource_exhausted("temporary pressure")
        ));
        assert!(!is_terminal_chart_status(&tonic::Status::unavailable(
            "retry"
        )));
    }
}
