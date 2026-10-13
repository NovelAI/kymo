//! Stage 2 of docs/chart-request-latency.md: chart requests are admitted by the bytes they pin and build, and build on a few blocking threads.
//!
//! A request waits in three places, in this order: for its bytes ([`ByteBudget::reserve`], holding nothing), for its scan units (holding only its byte charge), and for a build slot ([`BuildSlots::run`], holding only its charge and pinned snapshots). Byte and unit holders never wait on either budget again, except a poll that starts over ([`ChartCharge::try_recharge`], [`ChartCharge::overrun`]), which gives back everything it holds first ([`ChartCharge::recharge`]). Builds never wait, so no wait cycle exists.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use tokio::sync::{oneshot, Semaphore};
use tonic::Status;

/// Build transients per prepared row, charged over every row a build prepares and again over its largest series' (query.rs `ChartPlan::output_bytes`): `prepare` holds each series' rows as 32-byte points until it turns them into columns, one series at a time, beside that series' working copies, a time axis's sort buffer or a custom-x series' sorted points, and a smoothed series' finite points, collected into a vector grown by doubling, which holds both buffers while one moves. The allocator probe, counting a moved reallocation's old and new buffers as both live, measures at most 67, 47, 39 and 34 bytes per row for 1, 2, 4 and 8 equal series of 20,000 rows (query.rs `build_charges_cover_measured_peaks`); just past a power of two in rows (8,193 or 16,385), a smoothed single series whose finite points move as they grow peaks up to 12% over its charge.
pub(super) const BUILD_BYTES_PER_ROW: u64 = 34;
/// A series the cache has never seen is charged as the prod-average series until its rows arrive (2026-09-30: 26k rows, about 0.85 MB cached).
pub(super) const MISS_ESTIMATE_ROWS: u64 = 26_000;
/// A custom-x point: its decoded (step, x) pair and its entry in the run's x map.
pub(super) const X_BYTES_PER_ROW: u64 = 56;
/// A build's output per dense cell (one series × one shared-axis column): the model's value, envelope and raw columns, the per-run accumulators behind them and the wire copy built from them. The allocator probe measured 21-34 bytes (query.rs `build_charges_cover_measured_peaks`; the most with smoothing on a banded passthrough axis), rounded up for allocator size classes.
pub(super) const DENSE_BYTES_PER_CELL: u64 = 48;

/// A budget lock, recovered after a panic under it: what it guards are byte counts and waiters, and the drops that return bytes ([`WaitGuard`], [`ChartCharge`], [`SplitCharge`]) must never panic, even while unwinding.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One process-wide byte budget, granted first come, first served. Corrections never wait, so `in_use` can exceed the limit (an overdraft); later requests then wait it out.
pub(super) struct ByteBudget {
    limit: u64,
    state: Mutex<BudgetState>,
}

#[derive(Default)]
struct BudgetState {
    in_use: u64,
    queue: VecDeque<Waiter>,
    next_id: u64,
}

struct Waiter {
    id: u64,
    bytes: u64,
    grant: oneshot::Sender<()>,
}

impl ByteBudget {
    pub(super) fn new(limit: u64) -> Arc<Self> {
        metrics::gauge!("mkdb2_chart_budget_bytes_limit").set(limit as f64);
        metrics::gauge!("mkdb2_chart_budget_bytes_in_use").set(0.0);
        Arc::new(Self {
            limit,
            state: Mutex::default(),
        })
    }

    /// Wait until `items` (one charge per series, custom-X run and the build's output, as [`super::ChartPlan`] lists them; summed) fit, behind every earlier request ([`Self::wait`]).
    pub(super) async fn reserve(self: &Arc<Self>, items: Vec<u64>) -> Arc<ChartCharge> {
        let bytes = items.iter().sum();
        self.wait(bytes).await;
        Arc::new(ChartCharge {
            budget: self.clone(),
            items: Mutex::new(items),
            granted: AtomicU64::new(bytes),
        })
    }

    /// Wait until `bytes` fit behind every earlier waiter, then count them in use, for the caller's charge to own. A total above the whole budget waits until nothing else is charged and then runs alone, counted in `mkdb2_chart_budget_alone_total`: while it holds more than the limit, nothing else fits.
    async fn wait(&self, bytes: u64) {
        let (grant, granted) = oneshot::channel();
        let id = {
            let mut state = lock(&self.state);
            let id = state.next_id;
            state.next_id += 1;
            state.queue.push_back(Waiter { id, bytes, grant });
            self.dispatch(&mut state);
            id
        };
        // Created before the first await: dropping this future always runs the guard, which either leaves the queue or returns a grant nobody observed.
        let waiting = WaitGuard {
            budget: self,
            id,
            bytes,
        };
        let started = Instant::now();
        granted
            .await
            .expect("a queued sender is dequeued only by a grant or by this guard");
        // Observed: the bytes are the charge's now. The guard holds no resources of its own.
        std::mem::forget(waiting);
        metrics::histogram!("mkdb2_chart_budget_wait_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        if bytes > self.limit {
            metrics::counter!("mkdb2_chart_budget_alone_total").increment(1);
        }
    }

    /// Whether `bytes` fit beside `in_use` others; anything fits an idle budget, so a request above the whole limit runs alone.
    fn fits(&self, in_use: u64, bytes: u64) -> bool {
        in_use == 0 || in_use + bytes <= self.limit
    }

    /// Grant queued requests in order while the head fits.
    fn dispatch(&self, state: &mut BudgetState) {
        while state
            .queue
            .front()
            .is_some_and(|head| self.fits(state.in_use, head.bytes))
        {
            let waiter = state.queue.pop_front().unwrap();
            state.in_use += waiter.bytes;
            // A closed receiver means its future is being dropped; its guard returns the grant.
            let _ = waiter.grant.send(());
        }
        Self::publish(state);
    }

    fn publish(state: &BudgetState) {
        metrics::gauge!("mkdb2_chart_budget_bytes_in_use").set(state.in_use as f64);
        metrics::gauge!("mkdb2_chart_budget_waiters").set(state.queue.len() as f64);
    }

    /// Recharge `old` bytes as `new` (a release is `new == 0`).
    fn adjust(&self, old: u64, new: u64) {
        let mut state = lock(&self.state);
        state.in_use = state.in_use - old + new;
        self.dispatch(&mut state);
    }

    /// Recharge a whole charge of `old` bytes as `new` if `new` fits now beside the others' charges and nobody waits, first come, first served as [`Self::wait`] grants, or, `ahead` of the waiters, if it fits now. If so, record `new` as what the charge was `granted` and return true; else change nothing and return false. A charge above the whole budget fits only an otherwise idle one and runs alone, counted as [`Self::wait`] counts it when its last grant was within the budget.
    fn try_adjust(&self, granted: &AtomicU64, old: u64, new: u64, ahead: bool) -> bool {
        let mut state = lock(&self.state);
        let others = state.in_use - old;
        if !self.fits(others, new) || !(ahead || state.queue.is_empty()) {
            return false;
        }
        state.in_use = others + new;
        Self::publish(&state);
        if granted.swap(new, Relaxed) <= self.limit && new > self.limit {
            metrics::counter!("mkdb2_chart_budget_alone_total").increment(1);
        }
        true
    }

    #[cfg(test)]
    pub(super) fn in_use(&self) -> u64 {
        lock(&self.state).in_use
    }

    #[cfg(test)]
    pub(super) fn waiters(&self) -> usize {
        lock(&self.state).queue.len()
    }
}

struct WaitGuard<'a> {
    budget: &'a ByteBudget,
    id: u64,
    bytes: u64,
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        let mut state = lock(&self.budget.state);
        match state.queue.iter().position(|waiter| waiter.id == self.id) {
            Some(position) => {
                state.queue.remove(position);
            }
            // Granted, but the future was dropped before it observed the grant.
            None => state.in_use -= self.bytes,
        }
        self.budget.dispatch(&mut state);
    }
}

/// One request's bytes, itemized as [`super::ChartPlan`] lists them: series, custom-X runs, then the build's output. The request and its build share it, so the bytes stay charged until the build ends, even when the request is cancelled; a detached read holds its series' snapshot bytes apart ([`Self::split`]), so a cancelled request's other bytes go back at once.
pub(super) struct ChartCharge {
    budget: Arc<ByteBudget>,
    items: Mutex<Vec<u64>>,
    /// What the budget last granted it whole: what it waited for ([`ByteBudget::reserve`], [`Self::recharge`]) or took in place ([`Self::try_recharge`], a kept [`Self::overrun`]). Corrections move it past that without asking; it counts in `mkdb2_chart_budget_alone_total` once per grant past the budget.
    granted: AtomicU64,
}

impl ChartCharge {
    /// Charge item `index` at its real size from now on. Never waits: a result larger than its estimate overdraws the budget.
    pub(super) fn correct(&self, index: usize, bytes: u64) {
        let mut items = lock(&self.items);
        let old = std::mem::replace(&mut items[index], bytes);
        if old != bytes {
            self.budget.adjust(old, bytes);
        }
    }

    /// Move up to `bytes` of item `index` into a charge of their own, which a detached read of that series holds until it ends: a cancelled request's other bytes, its transients included, go back at once instead of riding the read for up to its whole timeout.
    pub(super) fn split(&self, index: usize, bytes: u64) -> SplitCharge {
        let mut items = lock(&self.items);
        let bytes = bytes.min(items[index]);
        items[index] -= bytes;
        SplitCharge {
            budget: self.budget.clone(),
            bytes,
        }
    }

    /// Charge `items` (as [`Self::recharge`] takes them) in place of what the charge holds if they fit now and nobody waits, and return true; else change nothing and return false. The caller is a poll that skipped its build transients at step 1 and builds in full after all. Without room it starts over ([`Self::recharge`]) instead of waiting here: holding its bytes, it could wait behind a request waiting for them, and its pins, of snapshots the series cache may no longer hold, would be counted by nothing while it waited.
    pub(super) fn try_recharge(&self, items: &[u64]) -> bool {
        let mut held = lock(&self.items);
        debug_assert_eq!(held.len(), items.len());
        let fits =
            self.budget
                .try_adjust(&self.granted, held.iter().sum(), items.iter().sum(), false);
        if fits {
            held.copy_from_slice(items);
        }
        fits
    }

    /// Whether the charge may keep what corrections took it to. No more than it was granted, it keeps it; more, it keeps it, granted in place, if it fits now and nobody waits, as [`Self::try_recharge`] decides, or, when it may go `ahead` of the waiters, if it fits now. Either way this returns `None`. Otherwise it changes nothing and returns what it holds: the caller, a poll whose reads came out larger than step 1 priced them, starts over with that ([`Self::recharge`]) rather than hold the overrun and its pins while it waits for a build slot.
    pub(super) fn overrun(&self, ahead: bool) -> Option<Vec<u64>> {
        let held = lock(&self.items);
        let bytes = held.iter().sum();
        let granted = &self.granted;
        (bytes > granted.load(Relaxed) && !self.budget.try_adjust(granted, bytes, bytes, ahead))
            .then(|| held.clone())
    }

    /// Charge `items` (one per series, custom-X run and the build's output, as [`ByteBudget::reserve`] takes them) in place of what the charge holds, waiting behind every earlier request as `reserve` does. The caller is a poll starting over, with the transients step 1 skipped ([`Self::try_recharge`]) or at the sizes its reads came to ([`Self::overrun`]). The charge's bytes go back first, so while it waits it holds nothing. Nothing else corrects the charge meanwhile: the poll's reads are done, and it starts its next ones once this returns.
    pub(super) async fn recharge(&self, items: Vec<u64>) {
        let held = std::mem::replace(&mut *lock(&self.items), vec![0; items.len()]);
        self.budget.adjust(held.iter().sum(), 0);
        let bytes = items.iter().sum();
        // Cancelled while it waits, the charge holds nothing.
        self.budget.wait(bytes).await;
        self.granted.store(bytes, Relaxed);
        let mut held = lock(&self.items);
        debug_assert!(
            held.iter().all(|&bytes| bytes == 0),
            "nothing corrects a charge while it is recharged"
        );
        *held = items;
    }

    #[cfg(test)]
    pub(super) fn items(&self) -> Vec<u64> {
        lock(&self.items).clone()
    }
}

/// Bytes split off a [`ChartCharge`] for a detached read ([`ChartCharge::split`]), returned when it ends.
pub(super) struct SplitCharge {
    budget: Arc<ByteBudget>,
    bytes: u64,
}

impl Drop for SplitCharge {
    fn drop(&mut self) {
        self.budget.adjust(self.bytes, 0);
    }
}

impl Drop for ChartCharge {
    fn drop(&mut self) {
        let bytes = self
            .items
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .sum();
        self.budget.adjust(bytes, 0);
    }
}

/// Raises a gauge by one while held.
pub(super) struct GaugeHold(&'static str);

impl GaugeHold {
    pub(super) fn new(gauge: &'static str) -> Self {
        metrics::gauge!(gauge).increment(1.0);
        Self(gauge)
    }
}

impl Drop for GaugeHold {
    fn drop(&mut self) {
        metrics::gauge!(self.0).decrement(1.0);
    }
}

/// N concurrent chart builds, each on the blocking pool.
pub(super) struct BuildSlots {
    slots: Arc<Semaphore>,
}

impl BuildSlots {
    pub(super) fn new(limit: usize) -> Self {
        metrics::gauge!("mkdb2_chart_build_slots_limit").set(limit as f64);
        Self {
            slots: Arc::new(Semaphore::new(limit)),
        }
    }

    /// Wait for a slot, then run `build` on the blocking pool. The task owns the slot and everything `build` captures (the byte charge and pinned snapshots) until it returns, even if the caller stops waiting; builds never wait on anything.
    pub(super) async fn run<T: Send + 'static>(
        &self,
        build: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, Status> {
        let started = Instant::now();
        let waiting = GaugeHold::new("mkdb2_chart_build_waiters");
        let permit = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .expect("the build slots' semaphore is never closed");
        drop(waiting);
        metrics::histogram!("mkdb2_chart_build_wait_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        // The gauge drops first, so it never counts more slots than exist.
        let slot = (GaugeHold::new("mkdb2_chart_build_slots_in_use"), permit);
        // The build logs under the request's span.
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();
            let _slot = slot;
            let started = Instant::now();
            let out = build();
            metrics::histogram!("mkdb2_chart_build_duration_seconds")
                .record(started.elapsed().as_secs_f64());
            out
        })
        .await
        .map_err(|error| Status::internal(format!("chart build failed: {error}")))
    }

    #[cfg(test)]
    pub(super) fn available(&self) -> usize {
        self.slots.available_permits()
    }

    #[cfg(test)]
    pub(super) async fn hold(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.slots.clone().acquire_owned().await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn pending<T>(fut: &mut (impl std::future::Future<Output = T> + Unpin)) -> bool {
        futures::FutureExt::now_or_never(fut).is_none()
    }

    #[tokio::test]
    async fn reservations_wait_in_order_until_their_bytes_fit() {
        let budget = ByteBudget::new(100);
        let first = budget.reserve(vec![30, 30]).await;
        assert_eq!(budget.in_use(), 60);
        let mut second = Box::pin(budget.reserve(vec![50]));
        assert!(pending(&mut second));
        // First come, first served: a request that would fit waits behind the head.
        let mut third = Box::pin(budget.reserve(vec![10]));
        assert!(pending(&mut third));
        drop(first);
        let second = second.await;
        let third = third.await;
        assert_eq!(budget.in_use(), 60);
        drop((second, third));
        assert_eq!(budget.in_use(), 0);
    }

    #[test]
    fn a_charge_returns_its_bytes_after_a_panic_under_its_locks() {
        let budget = ByteBudget::new(100);
        let charge = futures::FutureExt::now_or_never(budget.reserve(vec![30, 20])).unwrap();
        let poison = |mutex: &dyn Fn()| {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(mutex));
        };
        poison(&|| {
            let _items = charge.items.lock().unwrap();
            panic!("injected panic under a charge's lock");
        });
        poison(&|| {
            let _state = budget.state.lock().unwrap();
            panic!("injected panic under the budget's lock");
        });
        charge.correct(0, 10);
        assert_eq!(budget.in_use(), 30);
        drop(charge);
        assert_eq!(budget.in_use(), 0);
        let again = futures::FutureExt::now_or_never(budget.reserve(vec![100]));
        assert!(again.is_some(), "the budget still admits");
    }

    #[tokio::test]
    async fn corrections_overdraw_without_waiting_and_later_requests_wait_it_out() {
        let budget = ByteBudget::new(100);
        let charge = budget.reserve(vec![40, 40]).await;
        charge.correct(0, 250);
        assert_eq!(budget.in_use(), 290, "a correction never waits");
        assert_eq!(charge.items(), vec![250, 40]);
        let mut next = Box::pin(budget.reserve(vec![1]));
        assert!(pending(&mut next));
        charge.correct(0, 20);
        assert_eq!(budget.in_use(), 61, "a correction down admits the waiter");
        let next = next.await;
        assert_eq!(budget.in_use(), 61);
        drop((charge, next));
        assert_eq!(budget.in_use(), 0);
    }

    #[tokio::test]
    async fn a_request_above_the_whole_budget_drains_it_and_runs_alone() {
        let budget = ByteBudget::new(100);
        let small = budget.reserve(vec![30]).await;
        let mut huge = Box::pin(budget.reserve(vec![300, 200]));
        assert!(pending(&mut huge), "waits for the budget to drain");
        let mut later = Box::pin(budget.reserve(vec![10]));
        assert!(pending(&mut later));
        drop(small);
        let huge = huge.await;
        assert_eq!(budget.in_use(), 500);
        assert!(pending(&mut later), "nothing runs beside it");
        drop(huge);
        let later = later.await;
        assert_eq!(budget.in_use(), 10);
        drop(later);
    }

    #[tokio::test]
    async fn cancelled_waiters_leave_the_queue_or_return_their_grant() {
        let budget = ByteBudget::new(100);
        let held = budget.reserve(vec![90]).await;
        // Cancelled while queued: the request behind it is admitted at once.
        let mut blocked = Box::pin(budget.reserve(vec![50]));
        assert!(pending(&mut blocked));
        let mut behind = Box::pin(budget.reserve(vec![10]));
        assert!(pending(&mut behind));
        drop(blocked);
        let behind = behind.await;
        assert_eq!(budget.in_use(), 100);
        drop(behind);
        // Granted after its last poll, then cancelled before observing it: the grant goes back.
        let mut granted = Box::pin(budget.reserve(vec![60]));
        assert!(pending(&mut granted));
        drop(held);
        assert_eq!(budget.in_use(), 60, "granted while nobody polls it");
        drop(granted);
        assert_eq!(budget.in_use(), 0);
        let _again = budget.reserve(vec![100]).await;
    }

    #[tokio::test]
    async fn a_detached_read_keeps_only_its_split_after_its_request_ends() {
        let budget = ByteBudget::new(100);
        let charge = budget.reserve(vec![60, 30, 10]).await;
        let read = charge.split(0, 25);
        // At most what the item holds.
        let ended = charge.split(2, 40);
        assert_eq!(charge.items(), vec![35, 30, 0]);
        assert_eq!(budget.in_use(), 100, "a split moves bytes, never adds them");
        drop(ended);
        assert_eq!(budget.in_use(), 90, "a read that ends returns its split");
        drop(charge);
        assert_eq!(
            budget.in_use(),
            25,
            "the cancelled request's other bytes go back"
        );
        drop(read);
        assert_eq!(budget.in_use(), 0);
    }

    #[tokio::test]
    async fn a_recharge_charges_in_place_only_with_room_and_nobody_waiting() {
        let budget = ByteBudget::new(100);
        let poll = budget.reserve(vec![30, 0]).await;
        assert!(poll.try_recharge(&[40, 20]), "room, nobody waiting");
        assert_eq!((poll.items(), budget.in_use()), (vec![40, 20], 60));
        let other = budget.reserve(vec![30]).await;
        assert!(!poll.try_recharge(&[40, 40]), "no room");
        let mut earlier = Box::pin(budget.reserve(vec![20]));
        assert!(pending(&mut earlier));
        assert!(
            !poll.try_recharge(&[40, 25]),
            "room, but an earlier request waits"
        );
        assert_eq!(
            (poll.items(), budget.in_use()),
            (vec![40, 20], 90),
            "a refusal changes nothing"
        );
        drop(other);
        let earlier = earlier.await;
        assert!(
            !poll.try_recharge(&[40, 150]),
            "over the budget beside others"
        );
        drop(earlier);
        assert!(poll.try_recharge(&[40, 150]), "over the budget, alone");
        assert_eq!(budget.in_use(), 190);
        drop(poll);
        assert_eq!(budget.in_use(), 0);
    }

    #[tokio::test]
    async fn an_overrun_is_kept_only_with_room_and_nobody_waiting() {
        let budget = ByteBudget::new(100);
        let poll = budget.reserve(vec![30, 10]).await;
        poll.correct(0, 25);
        assert_eq!(poll.overrun(false), None, "within its reservation");
        poll.correct(0, 50);
        assert_eq!(poll.overrun(false), None, "room, nobody waiting");
        let other = budget.reserve(vec![30]).await;
        let mut earlier = Box::pin(budget.reserve(vec![20]));
        assert!(pending(&mut earlier));
        assert_eq!(poll.overrun(false), None, "kept, it was granted in place");
        poll.correct(0, 55);
        assert_eq!(
            poll.overrun(false),
            Some(vec![55, 10]),
            "room, but an earlier request waits"
        );
        drop(other);
        let earlier = earlier.await;
        poll.correct(0, 95);
        assert_eq!(poll.overrun(false), Some(vec![95, 10]), "no room");
        assert_eq!(
            (poll.items(), budget.in_use()),
            (vec![95, 10], 125),
            "a refusal changes nothing"
        );
        drop(earlier);
        assert_eq!(poll.overrun(false), None, "over the budget, alone");
        drop(poll);
        assert_eq!(budget.in_use(), 0);
    }

    /// An overrun allowed `ahead` of the waiters is kept whenever it fits, within the limit or alone, an earlier request waiting or not; past the limit beside others it starts over as any other.
    #[tokio::test]
    async fn an_overrun_ahead_of_waiters_is_kept_when_it_fits() {
        let budget = ByteBudget::new(100);
        let poll = budget.reserve(vec![30, 10]).await;
        let other = budget.reserve(vec![30]).await;
        let mut earlier = Box::pin(budget.reserve(vec![50]));
        assert!(pending(&mut earlier));
        poll.correct(0, 55);
        assert_eq!(poll.overrun(false), Some(vec![55, 10]), "not ahead");
        assert_eq!(poll.overrun(true), None, "ahead, within the limit");
        poll.correct(0, 65);
        assert_eq!(poll.overrun(true), Some(vec![65, 10]), "past the limit");
        assert_eq!((budget.in_use(), budget.waiters()), (105, 1));
        drop(other);
        poll.correct(0, 95);
        assert_eq!(poll.overrun(false), Some(vec![95, 10]), "alone, not ahead");
        assert_eq!(poll.overrun(true), None, "alone past the limit, ahead");
        assert_eq!((budget.in_use(), budget.waiters()), (105, 1));
        drop(poll);
        drop(earlier.await);
        assert_eq!(budget.in_use(), 0);
    }

    /// A request counts as running alone once per grant past the budget: at its wait when it waited for more than the budget, else when an in-place recharge or a kept overrun first takes it past the budget.
    #[test]
    fn a_request_past_the_budget_counts_alone_once_per_grant() {
        let budget = ByteBudget::new(100);
        let admit = |items| futures::FutureExt::now_or_never(budget.reserve(items)).unwrap();
        let ((), rendered) = crate::with_metrics(|| {
            // Within the budget: not counted.
            drop(admit(vec![60, 40]));
            // Admitted alone, then larger, kept alone twice: counted at its wait.
            let poll = admit(vec![150, 0]);
            poll.correct(0, 200);
            assert_eq!(poll.overrun(false), None);
            assert!(poll.try_recharge(&[200, 40]));
            drop(poll);
            // Admitted within the budget, recharged in place within it, then past it: counted there.
            let poll = admit(vec![50, 0]);
            assert!(poll.try_recharge(&[50, 40]));
            assert!(poll.try_recharge(&[50, 70]), "alone on an idle budget");
            drop(poll);
            // Admitted within the budget, its read past it, kept alone twice: counted once, where it was first kept.
            let poll = admit(vec![50, 0]);
            poll.correct(0, 150);
            assert_eq!(poll.overrun(false), None);
            assert!(poll.try_recharge(&[150, 40]));
        });
        assert!(
            rendered
                .lines()
                .any(|line| line == "mkdb2_chart_budget_alone_total 3"),
            "{rendered}"
        );
    }

    #[tokio::test]
    async fn a_recharge_gives_its_bytes_back_and_waits_its_turn() {
        let budget = ByteBudget::new(100);
        let poll = budget.reserve(vec![60, 0]).await;
        // An earlier request waits for bytes the poll holds: a recharge holding them would wait behind it for ever.
        let mut earlier = Box::pin(budget.reserve(vec![50]));
        assert!(pending(&mut earlier));
        let mut recharge = Box::pin(poll.recharge(vec![60, 30]));
        assert!(pending(&mut recharge));
        let earlier = earlier.await;
        assert_eq!(
            budget.in_use(),
            50,
            "the poll's bytes went back while it waits"
        );
        assert_eq!(poll.items(), vec![0, 0]);
        drop(earlier);
        recharge.await;
        assert_eq!(poll.items(), vec![60, 30]);
        assert_eq!(budget.in_use(), 90);
        // Cancelled while it waits, it holds nothing.
        let blocker = budget.reserve(vec![10]).await;
        let mut cancelled = Box::pin(poll.recharge(vec![60, 50]));
        assert!(pending(&mut cancelled));
        drop(cancelled);
        drop(poll);
        assert_eq!(budget.in_use(), 10);
        drop(blocker);
        assert_eq!(budget.in_use(), 0);
    }

    #[tokio::test]
    async fn a_build_keeps_its_slot_snapshots_and_bytes_after_its_requester_is_cancelled() {
        let slots = Arc::new(BuildSlots::new(1));
        let budget = ByteBudget::new(1_000);
        let charge = budget.reserve(vec![400]).await;
        let snapshot = Arc::new(crate::series_cache::SeriesSnapshot::full(Vec::new()));
        let pinned = Arc::downgrade(&snapshot);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel::<()>();
        let requester = tokio::spawn({
            let slots = slots.clone();
            async move {
                slots
                    .run(move || {
                        let (_charge, _snapshot) = (charge, snapshot);
                        started_tx.send(()).unwrap();
                        finish_rx.recv().unwrap();
                    })
                    .await
            }
        });
        tokio::task::spawn_blocking(move || started_rx.recv().unwrap())
            .await
            .unwrap();
        requester.abort();
        assert!(requester.await.unwrap_err().is_cancelled());
        assert_eq!(slots.available(), 0, "the build still owns its slot");
        assert_eq!(budget.in_use(), 400, "and its bytes");
        assert!(pinned.upgrade().is_some(), "and its snapshots");
        let mut waiter = Box::pin(slots.run(|| ()));
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut waiter)
                .await
                .is_err(),
            "the next build waits for the slot"
        );
        finish_tx.send(()).unwrap();
        waiter.await.unwrap();
        crate::wait_until(|| slots.available() == 1 && budget.in_use() == 0).await;
        assert!(pinned.upgrade().is_none());
    }

    #[tokio::test]
    async fn builds_run_off_the_async_workers() {
        let slots = BuildSlots::new(2);
        // A current-thread runtime's one async worker is the test's own thread.
        let thread = slots.run(|| std::thread::current().id()).await.unwrap();
        assert_ne!(thread, std::thread::current().id());
        assert_eq!(slots.available(), 2);
        let _held = slots.hold().await;
        assert_eq!(slots.available(), 1);
    }
}
