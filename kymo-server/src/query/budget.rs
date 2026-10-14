//! Stage 2 of docs/chart-request-latency.md: chart requests are admitted by the bytes they pin and build, and build on a few blocking threads.
//!
//! A request waits in three places, in this order: for its bytes ([`ByteBudget::reserve`], holding nothing), for its scan units (holding only its byte charge), and for a build slot ([`BuildSlots::run`], holding only its charge and pinned snapshots). Byte and unit holders never wait on either budget again, except a poll that starts over ([`ChartCharge::overrun`]), which gives back everything it holds first ([`ChartCharge::recharge`]). Builds never wait, so no wait cycle exists.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
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
    /// What the charges were granted, summed, kept to state and check the invariant admission rests on (`count_grant`): within the limit unless one charge runs alone, since `in_use`, which also counts growth past a grant, credit and splits, never falls below it.
    granted: u64,
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
            held: Mutex::new(Held {
                items,
                credit: Some(0),
                granted: bytes,
            }),
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

    /// Grant queued requests in order while the head fits, then publish the budget's gauges.
    fn dispatch(&self, state: &mut BudgetState) {
        while state
            .queue
            .front()
            .is_some_and(|head| self.fits(state.in_use, head.bytes))
        {
            let waiter = state.queue.pop_front().unwrap();
            state.in_use += waiter.bytes;
            self.count_grant(state, waiter.bytes);
            // A closed receiver means its future is being dropped; its guard returns the grant.
            let _ = waiter.grant.send(());
        }
        metrics::gauge!("mkdb2_chart_budget_bytes_in_use").set(state.in_use as f64);
        metrics::gauge!("mkdb2_chart_budget_waiters").set(state.queue.len() as f64);
    }

    /// Add `bytes` granted to the sum of grants, which never passes the limit beside another charge.
    fn count_grant(&self, state: &mut BudgetState, bytes: u64) {
        state.granted += bytes;
        debug_assert!(state.granted <= self.limit || state.granted == bytes);
    }

    /// Recharge `old` bytes as `new` (a release is `new == 0`).
    fn adjust(&self, old: u64, new: u64) {
        let mut state = lock(&self.state);
        state.in_use = state.in_use - old + new;
        self.dispatch(&mut state);
    }

    /// Move a charge's count from `counted` to `bytes`, what its items come to, and grant the waiters the room that frees. With `grant`, bytes past the charge's grant must fit now beside the others' charges, ahead of any waiter: the charge was granted before every waiter, and keeping what fits delays one at most until the charges granted before it end, since no later request is granted before it. If so they become its grant; else nothing changes and this returns false. Without `grant`, the charge holds them ungranted. Its grant never stays above what it holds. A charge above the whole budget fits only an otherwise idle one and runs alone, counted as [`Self::wait`] counts it when its last grant was within the budget.
    fn settle(&self, granted: &mut u64, counted: u64, bytes: u64, grant: bool) -> bool {
        // No credit to hand over and no grant to move: nothing the budget tracks changes, and its queue's head, which did not fit before, still does not.
        if counted == bytes && (bytes == *granted || (bytes > *granted && !grant)) {
            return true;
        }
        let mut state = lock(&self.state);
        let others = state.in_use - counted;
        let had = *granted;
        if bytes > had && grant {
            if !self.fits(others, bytes) {
                return false;
            }
            if had <= self.limit && bytes > self.limit {
                metrics::counter!("mkdb2_chart_budget_alone_total").increment(1);
            }
            *granted = bytes;
            state.granted -= had;
            self.count_grant(&mut state, bytes);
        } else if bytes < had {
            *granted = bytes;
            state.granted -= had - bytes;
        }
        state.in_use = others + bytes;
        self.dispatch(&mut state);
        true
    }

    /// Give back a charge counted at `counted` and its grant.
    fn release(&self, granted: &mut u64, counted: u64) {
        let mut state = lock(&self.state);
        state.in_use -= counted;
        state.granted -= std::mem::take(granted);
        self.dispatch(&mut state);
    }

    /// Record `bytes`, what a charge holds once its wait ends, as its grant: the `waited` its wait granted, and any split that landed on it meanwhile.
    fn claim(&self, granted: &mut u64, waited: u64, bytes: u64) {
        lock(&self.state).granted += bytes - waited;
        *granted = bytes;
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
            None => {
                state.in_use -= self.bytes;
                state.granted -= self.bytes;
            }
        }
        self.budget.dispatch(&mut state);
    }
}

/// One request's bytes, itemized as [`super::ChartPlan`] lists them: series, custom-X runs, then the build's output. The request and its build share it, so the bytes stay charged until the build ends, even when the request is cancelled; a detached read holds its series' snapshot bytes apart ([`Self::split`]), so a cancelled request's other bytes go back at once.
pub(super) struct ChartCharge {
    budget: Arc<ByteBudget>,
    held: Mutex<Held>,
}

/// What a charge holds.
struct Held {
    items: Vec<u64>,
    /// While a pass reads: what its corrections took below the prices it was charged, still counted, so no waiter is admitted on bytes the pass's other corrections may take back within its grant; growth draws on it first. `None` once the pass settles ([`ChartCharge::overrun`], [`ChartCharge::settle`]): corrections then move the budget at once.
    credit: Option<u64>,
    /// What the budget last granted it whole: what it waited for ([`ByteBudget::reserve`], [`ChartCharge::recharge`]) or took in place ([`ByteBudget::settle`]). It never stays above what the charge holds once its pass settles; growth past it is an overdraft until a first pass decides ([`ChartCharge::overrun`]), and counts in `mkdb2_chart_budget_alone_total` once per grant past the budget.
    granted: u64,
}

impl Held {
    /// What the budget counts for the charge.
    fn counted(&self) -> u64 {
        self.items.iter().sum::<u64>() + self.credit.unwrap_or(0)
    }

    /// Account items that came to `old` coming to `new`: while the pass reads, what they dropped is credit, and what they grew draws on it first.
    fn absorb(&mut self, old: u64, new: u64) {
        if let Some(credit) = &mut self.credit {
            *credit = (*credit + old).saturating_sub(new);
        }
    }
}

impl ChartCharge {
    /// Charge item `index` at its real size from now on. Never waits: a result larger than its estimate overdraws the budget. While the pass reads, a smaller one is credit, handed to the waiters only once the pass settles.
    pub(super) fn correct(&self, index: usize, bytes: u64) {
        let mut held = lock(&self.held);
        let counted = held.counted();
        let old = std::mem::replace(&mut held.items[index], bytes);
        held.absorb(old, bytes);
        self.recount(&mut held, counted);
    }

    /// Move the budget's count of the charge from `counted`, before its items changed, to what it now holds. While its pass reads, only growth moves it, and credit takes the rest; once settled, every change does, and its grant follows down.
    fn recount(&self, held: &mut Held, counted: u64) {
        let now = held.counted();
        if now == counted {
            return;
        }
        if held.credit.is_some() {
            self.budget.adjust(counted, now);
        } else {
            self.budget.settle(&mut held.granted, counted, now, false);
        }
    }

    /// Move up to `bytes` of item `index` into a charge of their own, which a detached read of that series holds until it ends: a cancelled request's other bytes, its transients included, go back at once instead of riding the read for up to its whole timeout. A read that ends while the request lives returns them to the item ([`SplitCharge`]'s drop).
    pub(super) fn split(self: &Arc<Self>, index: usize, bytes: u64) -> SplitCharge {
        let mut held = lock(&self.held);
        let bytes = bytes.min(held.items[index]);
        held.items[index] -= bytes;
        SplitCharge {
            budget: self.budget.clone(),
            bytes,
            owner: Arc::downgrade(self),
            index,
        }
    }

    /// Charge `items` (as [`Self::recharge`] takes them) in place of what the charge holds, as corrections would. The caller is a poll that skipped its build transients at step 1 and builds in full after all; it then decides as any first pass does ([`Self::overrun`]).
    pub(super) fn correct_all(&self, items: &[u64]) {
        let mut held = lock(&self.held);
        let (counted, old) = (held.counted(), held.items.iter().sum::<u64>());
        held.items.copy_from_slice(items);
        held.absorb(old, items.iter().sum());
        self.recount(&mut held, counted);
    }

    /// Whether a first pass may keep what its corrections took it to: `None` if so, else what it holds. No more than it was granted, it keeps it; more, it keeps it, granted in place, if it fits now, ahead of any waiter ([`ByteBudget::settle`]). Kept, the pass settles: its credit goes to the waiters. Otherwise nothing changes and this returns what it holds: the caller, a poll whose reads or skipped transients came to more than it was granted, starts over with that ([`Self::recharge`]), which gives it back before awaiting anything. It does not wait here: holding its bytes, it could wait behind a request waiting for them, and its pins, of snapshots the series cache may no longer hold, would be counted by nothing while it waited.
    pub(super) fn overrun(&self) -> Option<Vec<u64>> {
        let mut held = lock(&self.held);
        let (counted, bytes) = (held.counted(), held.items.iter().sum());
        if !self.budget.settle(&mut held.granted, counted, bytes, true) {
            return Some(held.items.clone());
        }
        held.credit = None;
        None
    }

    /// Settle a pass that waits for a build slot without deciding (a pass that started over, or an advance tried first): its credit goes to the waiters, and its grant shrinks to what it holds, so growth past that counts as an overrun.
    pub(super) fn settle(&self) {
        let mut held = lock(&self.held);
        let counted = held.counted();
        if held.credit.take().is_some() {
            let bytes = held.items.iter().sum();
            self.budget.settle(&mut held.granted, counted, bytes, false);
        }
    }

    /// Charge `items` (one per series, custom-X run and the build's output, as [`ByteBudget::reserve`] takes them) in place of what the charge holds, waiting behind every earlier request as `reserve` does, with a fresh credit for the next pass's reads. The caller is a poll starting over ([`Self::overrun`]), with the transients step 1 skipped or at the sizes its reads came to. The charge's bytes go back first, so while it waits it holds nothing. Nothing should correct the charge meanwhile, since the poll's reads are done and it starts its next ones once this returns; bytes that did land on it (a split a late read returned, say) stay counted, added to `items` rather than overwritten, and granted with them.
    pub(super) async fn recharge(&self, items: Vec<u64>) {
        {
            let mut held = lock(&self.held);
            let counted = held.counted();
            self.budget.release(&mut held.granted, counted);
            held.items = vec![0; items.len()];
            held.credit = Some(0);
        }
        let waited = items.iter().sum();
        self.budget.wait(waited).await;
        let mut held = lock(&self.held);
        for (item, landed) in items.into_iter().zip(held.items.iter_mut()) {
            *landed += item;
        }
        let bytes = held.items.iter().sum();
        self.budget.claim(&mut held.granted, waited, bytes);
    }

    #[cfg(test)]
    pub(super) fn items(&self) -> Vec<u64> {
        lock(&self.held).items.clone()
    }
}

/// Bytes split off a [`ChartCharge`] for a detached read ([`ChartCharge::split`]), returned when it ends: to the budget if the request has gone, else to the request's item. A request that lives awaits the read before anything else (its correction, an overrun check, a restart), and holds the rows it gets until that correction prices them, so its bytes stay counted, never given to a waiter for the moment in between.
pub(super) struct SplitCharge {
    budget: Arc<ByteBudget>,
    bytes: u64,
    owner: Weak<ChartCharge>,
    index: usize,
}

impl Drop for SplitCharge {
    fn drop(&mut self) {
        match self.owner.upgrade() {
            Some(owner) => lock(&owner.held).items[self.index] += self.bytes,
            None => self.budget.adjust(self.bytes, 0),
        }
    }
}

impl Drop for ChartCharge {
    fn drop(&mut self) {
        let held = self.held.get_mut().unwrap_or_else(PoisonError::into_inner);
        let counted = held.counted();
        self.budget.release(&mut held.granted, counted);
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

    /// Charge `items` in place, as a poll that skipped its transients does, and decide as its first pass does: whether it keeps them.
    fn recharged(poll: &ChartCharge, items: &[u64]) -> bool {
        poll.correct_all(items);
        poll.overrun().is_none()
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
            let _held = charge.held.lock().unwrap();
            panic!("injected panic under a charge's lock");
        });
        poison(&|| {
            let _state = budget.state.lock().unwrap();
            panic!("injected panic under the budget's lock");
        });
        charge.settle();
        charge.correct(0, 10);
        assert_eq!(budget.in_use(), 30);
        drop(charge);
        assert_eq!(budget.in_use(), 0);
        let again = futures::FutureExt::now_or_never(budget.reserve(vec![100]));
        assert!(again.is_some(), "the budget still admits");
    }

    /// A first pass holds back what its corrections take below their prices until it decides: a waiter admitted on them would leave the budget past its limit once another of its corrections took them back within its grant, as the pass waits for a build slot.
    #[tokio::test]
    async fn a_pass_holds_back_its_corrections_down_until_it_decides() {
        let budget = ByteBudget::new(100);
        let poll = budget.reserve(vec![60, 10]).await;
        let mut waiter = Box::pin(budget.reserve(vec![80]));
        assert!(pending(&mut waiter));
        poll.correct(0, 10);
        assert!(pending(&mut waiter), "not admitted on the pass's credit");
        poll.correct(1, 50);
        assert_eq!(poll.overrun(), None, "within its grant");
        assert_eq!(
            (budget.in_use(), budget.waiters()),
            (60, 1),
            "at the build boundary, within the limit"
        );
        drop(poll);
        let waiter = waiter.await;
        assert_eq!(budget.in_use(), 80);
        drop(waiter);
        assert_eq!(budget.in_use(), 0);
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
        assert_eq!(
            budget.in_use(),
            290,
            "while the pass reads, it holds back a correction down"
        );
        assert!(pending(&mut next));
        charge.settle();
        assert_eq!(budget.in_use(), 61, "settled, it admits the waiter");
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

    /// A detached read that ends while its request lives returns its split to the request, which holds the rows it read until its correction prices them, so a waiter is not admitted on those bytes; a cancelled request's split goes back to the budget when its read ends.
    #[tokio::test]
    async fn a_detached_read_returns_its_split_to_its_request_or_once_it_is_gone_to_the_budget() {
        let budget = ByteBudget::new(100);
        let charge = budget.reserve(vec![60, 30, 10]).await;
        let read = charge.split(0, 25);
        // At most what the item holds.
        let ended = charge.split(2, 40);
        assert_eq!(charge.items(), vec![35, 30, 0]);
        assert_eq!(budget.in_use(), 100, "a split moves bytes, never adds them");
        let mut waiter = Box::pin(budget.reserve(vec![10]));
        assert!(pending(&mut waiter));
        drop(ended);
        assert_eq!(charge.items(), vec![35, 30, 10], "back to the request");
        assert_eq!(budget.in_use(), 100);
        assert!(
            pending(&mut waiter),
            "the request still holds the rows it read"
        );
        // Its correction prices them.
        charge.correct(2, 12);
        assert_eq!(budget.in_use(), 102);
        drop(charge);
        let waiter = waiter.await;
        assert_eq!(
            budget.in_use(),
            35,
            "the cancelled request's other bytes go back, the waiter is admitted"
        );
        drop(read);
        assert_eq!(
            budget.in_use(),
            10,
            "the running read's split goes back as it ends"
        );
        drop(waiter);
        assert_eq!(budget.in_use(), 0);
    }

    /// A recharge in place is taken whenever it fits: within the limit, an earlier request waiting or not (that request came after the charge), or alone in an otherwise idle budget; without room the charge holds it until the poll starts over.
    #[tokio::test]
    async fn a_recharge_charges_in_place_when_it_fits() {
        let budget = ByteBudget::new(100);
        let poll = budget.reserve(vec![30, 0]).await;
        assert!(recharged(&poll, &[40, 20]), "room, nobody waiting");
        assert_eq!((poll.items(), budget.in_use()), (vec![40, 20], 60));
        let other = budget.reserve(vec![30]).await;
        assert!(!recharged(&poll, &[40, 40]), "no room");
        assert_eq!(
            (poll.items(), budget.in_use()),
            (vec![40, 40], 110),
            "refused, it holds them until it starts over"
        );
        poll.correct(1, 20);
        let mut earlier = Box::pin(budget.reserve(vec![20]));
        assert!(pending(&mut earlier));
        assert!(
            recharged(&poll, &[40, 25]),
            "room, ahead of an earlier request"
        );
        assert_eq!((poll.items(), budget.in_use()), (vec![40, 25], 95));
        assert!(pending(&mut earlier));
        drop(other);
        let earlier = earlier.await;
        assert!(
            !recharged(&poll, &[40, 150]),
            "over the budget beside others"
        );
        drop(earlier);
        assert!(recharged(&poll, &[40, 150]), "over the budget, alone");
        assert_eq!(budget.in_use(), 190);
        drop(poll);
        assert_eq!(budget.in_use(), 0);
    }

    /// A recharge to no more than the charge was granted is kept, as an overrun within its grant is, even with the budget past its limit beside it.
    #[tokio::test]
    async fn a_recharge_within_its_grant_is_kept_as_an_overrun_is() {
        let budget = ByteBudget::new(100);
        let poll = budget.reserve(vec![30, 10]).await;
        let other = budget.reserve(vec![60]).await;
        other.correct(0, 80);
        assert!(recharged(&poll, &[25, 10]));
        assert_eq!((poll.items(), budget.in_use()), (vec![25, 10], 115));
        drop((poll, other));
        assert_eq!(budget.in_use(), 0);
    }

    /// A recharge to less than the charge held hands what it frees to the waiters at once.
    #[tokio::test]
    async fn a_smaller_recharge_grants_the_room_it_frees() {
        let budget = ByteBudget::new(100);
        let poll = budget.reserve(vec![50, 20]).await;
        let mut waiter = Box::pin(budget.reserve(vec![60]));
        assert!(pending(&mut waiter));
        assert!(recharged(&poll, &[30, 10]));
        let waiter = futures::FutureExt::now_or_never(waiter).expect("granted at once");
        assert_eq!(budget.in_use(), 100);
        drop((poll, waiter));
        assert_eq!(budget.in_use(), 0);
    }

    /// An overrun is kept, granted in place, whenever it fits: within the limit, an earlier request waiting or not, or alone in an otherwise idle budget; past the limit beside others it starts over, changing nothing.
    #[tokio::test]
    async fn an_overrun_is_kept_when_it_fits() {
        let budget = ByteBudget::new(100);
        let poll = budget.reserve(vec![30, 10]).await;
        poll.correct(0, 25);
        assert_eq!(poll.overrun(), None, "within its reservation");
        poll.correct(0, 50);
        assert_eq!(poll.overrun(), None, "room, nobody waiting");
        let other = budget.reserve(vec![30]).await;
        let mut earlier = Box::pin(budget.reserve(vec![50]));
        assert!(pending(&mut earlier));
        assert_eq!(poll.overrun(), None, "kept, it was granted in place");
        poll.correct(0, 55);
        assert_eq!(poll.overrun(), None, "room, ahead of an earlier request");
        poll.correct(0, 65);
        assert_eq!(poll.overrun(), Some(vec![65, 10]), "past the limit");
        assert_eq!(
            (poll.items(), budget.in_use(), budget.waiters()),
            (vec![65, 10], 105, 1),
            "a refusal changes nothing"
        );
        drop(other);
        poll.correct(0, 95);
        assert_eq!(poll.overrun(), None, "alone past the limit");
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
            assert_eq!(poll.overrun(), None);
            assert!(recharged(&poll, &[200, 40]));
            drop(poll);
            // Admitted within the budget, recharged in place within it, then past it: counted there.
            let poll = admit(vec![50, 0]);
            assert!(recharged(&poll, &[50, 40]));
            assert!(recharged(&poll, &[50, 70]), "alone on an idle budget");
            drop(poll);
            // Admitted within the budget, its read past it, kept alone twice: counted once, where it was first kept.
            let poll = admit(vec![50, 0]);
            poll.correct(0, 150);
            assert_eq!(poll.overrun(), None);
            assert!(recharged(&poll, &[150, 40]));
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

    /// A split returned while its request is recharged stays counted: the recharge adds its items to what the charge holds then, and every byte goes back when the charge ends.
    #[tokio::test]
    async fn a_split_returned_during_a_recharge_stays_counted() {
        let budget = ByteBudget::new(100);
        let poll = budget.reserve(vec![40, 10]).await;
        let read = poll.split(0, 30);
        let other = budget.reserve(vec![50]).await;
        // It starts over, waiting for 60 with 50 held by another.
        let mut recharge = Box::pin(poll.recharge(vec![50, 10]));
        assert!(pending(&mut recharge));
        assert_eq!((poll.items(), budget.in_use()), (vec![0, 0], 80));
        drop(read);
        assert_eq!((poll.items(), budget.in_use()), (vec![30, 0], 80));
        drop(other);
        recharge.await;
        assert_eq!((poll.items(), budget.in_use()), (vec![80, 10], 90));
        drop(poll);
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
