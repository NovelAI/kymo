//! In-memory cache of full raw series, kept fresh by append-only
//! incremental ClickHouse reads.
//!
//! The chart path used to re-read every point of every series from
//! ClickHouse on every refetch — during training that's a full scan per
//! chart per poll tick. Metrics are append-only in the common case, so
//! each entry holds a series' complete (tag, step)-sorted rows plus an
//! `inserted_at` watermark; a refresh reads only rows newer than the
//! watermark (ClickHouse skips the granules of already-cached history via
//! the `idx_inserted_at` minmax index) and splices them in. A resumed run
//! that re-logs an
//! existing step is detected by the merge and triggers a full rebuild of
//! that one entry. Rows are handed out as `Arc` clones — refreshes build
//! a new vector instead of mutating, so readers never block on a splice.
//!
//! INVARIANT: only RAW points are cached; every request recomputes smoothing, bucketing, and resampling from scratch.
//! The derived mean/envelope aggregates are cell-local; a smoothed envelope's center value additionally depends on the adjacent finite curve segment. An append can therefore change: the buckets its points land in (including a still-filling bucket's center), the preceding occupied bucket when a finite interpolation endpoint is added or changes, held points within the smoother's bounded reach, and the whole grid when its span outgrows stable_grid's width. The delta planner bounds each dependency, including markers on unplottable absolute log-x values, and answers full when the bound reaches column zero.

use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use crate::clickhouse::VersionedRawPoint;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LineageOrigin {
    Miss,
    Rewrite,
    Late,
}

fn row_content(row: &VersionedRawPoint) -> u64 {
    static KEY: OnceLock<std::collections::hash_map::RandomState> = OnceLock::new();
    let VersionedRawPoint {
        tag,
        step,
        timestamp_ms,
        value,
        is_value,
        inserted_ms,
    } = row;
    KEY.get_or_init(std::collections::hash_map::RandomState::new)
        .hash_one((
            tag,
            step,
            timestamp_ms,
            value.to_bits(),
            is_value,
            inserted_ms,
        ))
}

/// Immutable rows and their append lineage travel together through retained and pass-through refreshes.
/// Within a lineage, every addition has a stamp strictly above the preceding snapshot's actual maximum.
/// Filtering at a previously authenticated maximum therefore recovers exactly that held snapshot.
#[derive(Debug)]
pub struct SeriesSnapshot {
    rows: Vec<VersionedRawPoint>,
    lineage: u64,
    maximum: Option<i64>,
    origin: LineageOrigin,
    content: u64,
}

impl std::ops::Deref for SeriesSnapshot {
    type Target = [VersionedRawPoint];

    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}

impl SeriesSnapshot {
    fn next_lineage() -> u64 {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        // The process-keyed chart stamp separately invalidates tokens across restarts.
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn full(rows: Vec<VersionedRawPoint>) -> Self {
        Self::full_with_origin(rows, LineageOrigin::Miss)
    }

    pub(crate) fn full_with_origin(rows: Vec<VersionedRawPoint>, origin: LineageOrigin) -> Self {
        Self::reload(rows, origin, None)
    }

    fn reload(
        rows: Vec<VersionedRawPoint>,
        origin: LineageOrigin,
        evicted: Option<&EvictedLineage>,
    ) -> Self {
        let (mut maximum, mut content) = (None, 0u64);
        let (mut held_count, mut held_content) = (0, 0u64);
        let (mut tag, mut seen_new, mut prefix) = (None, false, true);
        for row in &rows {
            let hash = row_content(row);
            content = content.wrapping_add(hash);
            maximum = Some(maximum.map_or(row.inserted_ms, |max: i64| max.max(row.inserted_ms)));
            if let Some(old) = evicted {
                if tag != Some(row.tag.as_str()) {
                    tag = Some(row.tag.as_str());
                    seen_new = false;
                }
                if old.maximum.is_some_and(|max| row.inserted_ms <= max) {
                    held_count += 1;
                    held_content = held_content.wrapping_add(hash);
                    prefix &= !seen_new;
                } else {
                    seen_new = true;
                }
            }
        }
        // Full reads are (tag, step)-sorted. Count and sum prove the held set; the prefix check also rejects newly stamped lower-step backfills, preserving the cache merge's append contract.
        let restored =
            evicted.filter(|old| prefix && old.count == held_count && old.content == held_content);
        Self {
            rows,
            lineage: restored.map_or_else(Self::next_lineage, |old| old.lineage),
            maximum,
            origin: restored.map_or(origin, |old| old.origin),
            content,
        }
    }

    pub(crate) fn lineage(&self) -> u64 {
        self.lineage
    }
    pub(crate) fn maximum(&self) -> Option<i64> {
        self.maximum
    }
    pub(crate) fn origin(&self) -> LineageOrigin {
        self.origin
    }

    fn merge(&self, increment: &[VersionedRawPoint]) -> Option<Self> {
        let merged = merge_increment_tracked(self, increment)?;
        let late = merged
            .added_min
            .is_some_and(|min| self.maximum.is_some_and(|max| min <= max));
        Some(Self {
            rows: merged.rows,
            lineage: if late {
                Self::next_lineage()
            } else {
                self.lineage
            },
            maximum: self.maximum.into_iter().chain(merged.added_max).max(),
            origin: if late {
                LineageOrigin::Late
            } else {
                self.origin
            },
            content: self.content.wrapping_add(merged.added_content),
        })
    }

    /// Fixture source snapshots pass through the same merge proof. Missing/replaced rows model an authoritative refetch.
    #[cfg(test)]
    pub(crate) fn refreshed_fixture(&self, rows: &[VersionedRawPoint]) -> Self {
        if let Some(merged) = self.merge(rows) {
            if merged.len() == rows.len()
                && merged.iter().zip(rows).all(|(a, b)| {
                    a.tag == b.tag
                        && a.step == b.step
                        && a.timestamp_ms == b.timestamp_ms
                        && a.value.to_bits() == b.value.to_bits()
                        && a.is_value == b.is_value
                        && a.inserted_ms == b.inserted_ms
                })
            {
                return merged;
            }
        }
        Self::full_with_origin(rows.to_vec(), LineageOrigin::Rewrite)
    }
}

/// One cached series: a metric of a run.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SeriesKey {
    pub project_id: String,
    pub run_id: String,
    pub metric_name: String,
}

impl SeriesKey {
    pub fn new(
        project_id: impl Into<String>,
        run_id: impl Into<String>,
        metric_name: impl Into<String>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            run_id: run_id.into(),
            metric_name: metric_name.into(),
        }
    }
}

/// Bounded, process-local history of LRU victims; neither rows nor tags are retained, and a restart discards it.
const EVICTED_LINEAGE_LIMIT: usize = 4096;

struct EvictedLineage {
    lineage: u64,
    maximum: Option<i64>,
    origin: LineageOrigin,
    content: u64,
    count: usize,
    evicted: Instant,
}

/// Re-read this much of the watermark's past on each incremental fetch.
/// `inserted_at` has millisecond resolution, so rows landing in the same
/// millisecond as the watermark AFTER our read would be missed by a
/// strict `>` — the overlap re-reads them and the merge drops exact
/// duplicates.
pub const WATERMARK_OVERLAP_MS: i64 = 1_000;

/// Watermarks never advance past the read's START minus this ([`watermark_cap`]). A row is STAMPED
/// (`inserted_at DEFAULT now64(3)`) when ClickHouse starts processing its
/// INSERT but only becomes VISIBLE when the insert commits — so a slow
/// flush can surface rows stamped seconds in the past. If a faster
/// concurrent flush to the same series had meanwhile pushed the watermark
/// beyond those stamps, no later incremental read would ever see them
/// (they sit below the high-water mark of an already-seen tag, so even
/// rewrite detection can't notice). Capping the watermark at start − margin
/// means a row is only ever missed if its insert takes longer than
/// margin + overlap to commit — flushes run well under a second (see the
/// mkdb2_ch_insert_duration_seconds histogram). The cost is re-reading
/// an active series from ~16s before the previous read's start each refresh; the merge drops the
/// duplicates. Compares this process' clock against ClickHouse's: both
/// run in one NTP-synced cluster, skew ≪ margin.
/// Writes that can exceed margin + overlap: an insert that commits after its client timed out (the client's retry restamps its rows), and bulk imports, whose caches FinalizeImportRun purges (docs/bulk-import.md).
pub const VISIBILITY_MARGIN_MS: i64 = 15_000;

fn unix_ms_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The highest watermark a read that started at `fetch_started` may record ([`VISIBILITY_MARGIN_MS`]), on the wall clock ClickHouse stamps rows with. The start, not the completion: a read's snapshot is no earlier than its start, so every row it lacks committed after it, while a cap at completion would let a read slower than the margin record a watermark past rows still committing at its snapshot.
pub(crate) fn watermark_cap(fetch_started: Instant) -> i64 {
    unix_ms_now() - fetch_started.elapsed().as_millis() as i64 - VISIBILITY_MARGIN_MS
}

/// How long an entry serves without even the incremental query, counted from its fetch START. Long because each expiry costs a ClickHouse round trip per series, held under the chart request's admission units.
/// Safety comes from the bump gate ([`SeriesCache::note_bumps`]), not the window. The window alone bounds writes nothing notes that stamp a fresh `inserted_at` (the incremental read's key): a manual INSERT, a failed insert that commits after its late re-note, or a legacy unversioned rich publish cancelled mid-insert. One that keeps old stamps (an in-place ALTER UPDATE/DELETE, an INSERT copying `inserted_at`) reaches a retained entry only through a full read: eviction or a restart.
const FRESH_WINDOW: Duration = Duration::from_secs(30 * 60);

/// How long a note must survive: LONGER THAN ANY ROWS FETCHED BEFORE IT CAN STILL SERVE, which [`is_fresh`] bounds at [`FRESH_WINDOW`] after their fetch start; pruning the note sooner would serve rows that may predate it.
const BUMP_RETENTION: Duration = Duration::from_secs(3600);
const _: () = assert!(FRESH_WINDOW.as_secs() < BUMP_RETENTION.as_secs());

/// Pruning scans every note and every insert notes, so it runs at most this often; retention is only a lower bound.
const PRUNE_INTERVAL: Duration = Duration::from_secs(60);

/// Whole-task bound for a detached chart refresh (docs/admission-control.md Stage R). Detached tasks have no enclosing request deadline, so this is what frees a hung scan's admission unit and refresh slot; it sits inside [`FRESH_WINDOW`] so a finished scan's rows can still serve.
const DETACHED_REFRESH_TIMEOUT: Duration = Duration::from_secs(300);
const _: () = assert!(DETACHED_REFRESH_TIMEOUT.as_secs() < FRESH_WINDOW.as_secs());

/// A failure is never published: the caller that receives it surfaces the error, and later arrivals elect a fresh attempt.
#[derive(Debug)]
pub(crate) enum RefreshError {
    Source(anyhow::Error),
    Timeout,
    Died,
}

pub(crate) type RefreshOutcome = Result<Arc<SeriesSnapshot>, RefreshError>;

/// The one freshness rule for cached and shared rows: their fetch STARTED within [`FRESH_WINDOW`] and at or after the run's last note.
pub(crate) fn is_fresh(last_bump: Option<Instant>, started: Instant) -> bool {
    started.elapsed() < FRESH_WINDOW && last_bump.is_none_or(|bump| started >= bump)
}

impl std::fmt::Display for RefreshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefreshError::Source(e) => e.fmt(f),
            RefreshError::Timeout => write!(
                f,
                "detached series refresh exceeded {}s",
                DETACHED_REFRESH_TIMEOUT.as_secs()
            ),
            RefreshError::Died => write!(
                f,
                "detached series refresh ended without a result (task panic or shutdown)"
            ),
        }
    }
}

impl From<anyhow::Error> for RefreshError {
    fn from(e: anyhow::Error) -> Self {
        RefreshError::Source(e)
    }
}

fn default_budget_bytes() -> usize {
    crate::env::required_mebibytes("KYMO_SERIES_CACHE_MB", 256)
        .expect("invalid kymo series-cache environment")
}

fn entry_bytes(rows: &[VersionedRawPoint]) -> usize {
    std::mem::size_of::<SeriesSnapshot>()
        + rows.len() * (std::mem::size_of::<VersionedRawPoint>() + 16)
}

pub struct Entry {
    pub rows: Arc<SeriesSnapshot>,
    pub max_inserted_ms: i64,
    /// When the fetch that produced these rows STARTED — the bump-gate stamp (a note after this instant means the rows may predate a write) and the start of the [`FRESH_WINDOW`]. Start, not completion, so query duration can't fake safety.
    started: Instant,
    /// Identity of this store, unique per insert_full. An incremental fetch is computed against ONE base's watermark; if a concurrent full fetch replaces the entry before the increment applies, merging into the replacement can leave rows missing BELOW the merged watermark — a hole no later incremental re-reads. apply_increment refuses a stale generation and the caller full-rebuilds.
    gen: u64,
    last_access: Instant,
    bytes: usize,
}

#[derive(Default)]
struct Inner {
    map: HashMap<SeriesKey, Entry>,
    evicted: HashMap<SeriesKey, EvictedLineage>,
    total_bytes: usize,
    /// run_id → the run's latest note ([`SeriesCache::note_bumps`]). Notes must outlive any fetch that started before them (see [`BUMP_RETENTION`]); pruned every [`PRUNE_INTERVAL`], the map self-bounds at "runs written in the last hour".
    bumps: HashMap<String, Instant>,
    last_prune: Option<Instant>,
    /// Source of [`Entry::gen`] values.
    next_gen: u64,
}

pub struct SeriesCache {
    inner: std::sync::Mutex<Inner>,
    budget_bytes: usize,
}

/// Per-series refresh serialization. The cache deliberately does not put
/// in-flight reads in `Inner`: a ClickHouse read may take seconds, and holding
/// the cache mutex across it would block every other series. A weak registry
/// gives each key its own refresh slot without retaining request-derived keys
/// after the last leader/waiter is gone. The slot briefly publishes a leader's
/// successful Arc to the waiters that were already in flight. That sharing is
/// independent of durable LRU admission, so a series larger than the cache
/// budget is still fetched once per concurrent burst.
#[derive(Clone, Default)]
pub struct SeriesRefreshLocks {
    locks: crate::refresh_locks::RefreshLocks<SeriesKey, SlotState>,
}

#[derive(Clone)]
struct PublishedRefresh {
    rows: Arc<SeriesSnapshot>,
    started: Instant,
}

type RunningOutcome = Result<PublishedRefresh, RefreshError>;

#[derive(Default)]
struct SlotState {
    published: Option<PublishedRefresh>,
    /// A detached refresh in flight for this key; waiters attach here instead
    /// of electing a second leader.
    running: Option<tokio::task::JoinHandle<RunningOutcome>>,
}

impl SeriesRefreshLocks {
    fn lease_for(&self, key: &SeriesKey) -> SeriesRefreshLease {
        self.locks.lease_for(key)
    }

    #[cfg(test)]
    fn registry_len(&self) -> usize {
        self.locks.registry_len()
    }

    #[cfg(test)]
    fn lease_count(&self, key: &SeriesKey) -> usize {
        self.locks.lease_count(key)
    }

    /// Return a fresh entry or elect exactly one refresher for this key. Every
    /// waiter repeats the cache lookup after acquiring the key lock: it can use
    /// the leader's newly stored Arc, while a bump that arrived during the
    /// leader's read leaves that result stale and elects the waiter for another
    /// refresh. Errors and cancellation drop the guard, allowing the next
    /// waiter to retry.
    pub async fn get_or_refresh<F, Fut>(
        &self,
        cache: &SeriesCache,
        key: &SeriesKey,
        detach: impl FnOnce() -> Option<crate::clickhouse::RefreshDetach>,
        refresh: F,
    ) -> RefreshOutcome
    where
        F: FnOnce(Lookup) -> Fut,
        Fut: std::future::Future<Output = RefreshOutcome> + Send + 'static,
    {
        if let Lookup::Fresh(rows) = cache.lookup_untracked(key) {
            SeriesCache::record_lookup_result("fresh");
            return Ok(rows);
        }
        let lease = self.lease_for(key);
        let wait_started = Instant::now();
        let mut state = lease.lock().await;
        metrics::histogram!("mkdb2_series_refresh_wait_duration_seconds")
            .record(wait_started.elapsed().as_secs_f64());
        if state.running.is_some() {
            let done = Self::await_running(&mut state).await?;
            // Attach deliveries are bump-gated (docs/admission-control.md
            // goal 4): a snapshot whose fetch predates a bump this caller's
            // consult postdates falls through to a fresh election instead.
            if is_fresh(cache.last_bump(&key.run_id), done.started) {
                state.published = Some(done.clone());
                SeriesCache::record_lookup_result("fresh");
                SeriesCache::record_shared("running");
                return Ok(done.rows);
            }
        }
        match cache.lookup_untracked(key) {
            Lookup::Fresh(rows) => {
                // Another caller refreshed while we waited; this request did
                // no ClickHouse work and is therefore a fresh service for the
                // cache-work metrics.
                SeriesCache::record_lookup_result("fresh");
                SeriesCache::record_shared("retained");
                Ok(rows)
            }
            needs_refresh => {
                // An oversized result is deliberately absent from the LRU.
                // Existing waiters can nevertheless use the leader's Arc while it is fresh.
                if matches!(needs_refresh, Lookup::Miss) {
                    if let Some(rows) = state
                        .published
                        .as_ref()
                        .filter(|result| is_fresh(cache.last_bump(&key.run_id), result.started))
                        .map(|result| result.rows.clone())
                    {
                        SeriesCache::record_lookup_result("fresh");
                        SeriesCache::record_shared("published");
                        return Ok(rows);
                    }
                }
                SeriesCache::record_lookup(&needs_refresh);
                // Conservatively predate the loader invocation. A bump after
                // this point makes a pass-through result ineligible for
                // sharing, just as Entry::started gates retained rows.
                let refresh_started = Instant::now();
                let fut = refresh(needs_refresh);
                let done = match detach() {
                    None => fut.await.map(|rows| PublishedRefresh {
                        rows,
                        started: refresh_started,
                    }),
                    Some(ctx) => {
                        state.running = Some(self.spawn_detached(key, ctx, fut, refresh_started));
                        // The leader's consult predates its task's fetch
                        // start, so its delivery is never bump-gated.
                        Self::await_running(&mut state).await
                    }
                };
                if let Ok(published) = &done {
                    state.published = Some(published.clone());
                }
                done.map(|published| published.rows)
            }
        }
    }

    /// Stage R's detached refresh: the task owns `ctx` (the admission units and run guards backing its read) and its own lease, so the refresh slot, the attach point for re-polls, outlives every cancelled requester for the task's bounded lifetime.
    fn spawn_detached<D: Send + 'static>(
        &self,
        key: &SeriesKey,
        ctx: D,
        refresh: impl std::future::Future<Output = RefreshOutcome> + Send + 'static,
        started: Instant,
    ) -> tokio::task::JoinHandle<RunningOutcome> {
        let task_lease = self.lease_for(key);
        tokio::spawn(async move {
            // Declaration order is load-bearing: locals drop in reverse, so the lease (the slot's last owner) is declared AFTER the detach RAII and retires before the run guard is released — a writer taking the gate in between (finalize/purge evicting the run) must not find an attachable slot still holding a pre-eviction result.
            let _detach = ctx;
            let _lease = task_lease;
            let outcome = tokio::time::timeout(DETACHED_REFRESH_TIMEOUT, refresh)
                .await
                .unwrap_or(Err(RefreshError::Timeout));
            metrics::counter!(
                "mkdb2_series_refresh_detached_total",
                "outcome" => match &outcome {
                    Ok(_) => "ok",
                    Err(RefreshError::Timeout) => "timeout",
                    Err(_) => "error",
                }
            )
            .increment(1);
            outcome.map(|rows| PublishedRefresh { rows, started })
        })
    }

    /// Elect this caller as the full-read refresher of up to `limit` of `keys`, taking only slots that are free right now, so electing never waits.
    /// A key that is not a miss, whose slot is busy, or that a running or still-fresh published refresh already covers is left for [`Self::get_or_refresh`], which waits or attaches as usual.
    /// No deadlock: the future holding the elected slots ([`Self::spawn_batch`]) awaits only its own tasks, which take no locks.
    pub(crate) fn elect_misses(
        &self,
        cache: &SeriesCache,
        keys: &[SeriesKey],
        limit: usize,
    ) -> Vec<ElectedMiss> {
        let mut elected = Vec::new();
        for key in keys {
            if elected.len() == limit {
                break;
            }
            // Keeps fresh series, every poll's common case, off the refresh registry.
            if !matches!(cache.lookup_untracked(key), Lookup::Miss) {
                continue;
            }
            let lease = self.lease_for(key);
            let Some(state) = lease.try_lock_owned() else {
                continue;
            };
            // The checks get_or_refresh makes once it holds the lock.
            let covered = state.running.is_some()
                || state
                    .published
                    .as_ref()
                    .is_some_and(|result| is_fresh(cache.last_bump(&key.run_id), result.started));
            if covered || !matches!(cache.lookup_untracked(key), Lookup::Miss) {
                continue;
            }
            SeriesCache::record_lookup(&Lookup::Miss);
            elected.push(ElectedMiss {
                key: key.clone(),
                state,
                _lease: lease,
            });
        }
        elected
    }

    /// Refresh each of `elected` in its own detached task running `refresh(its position in elected, its key)`, as [`Self::get_or_refresh`] would.
    /// Every task is spawned, and its slot's `running` set, before this returns: cancelling the caller cannot strand an elected slot or leave the shared read running on released units.
    /// Every task keeps a clone of `ctx`, so it is released only when the last task ends.
    /// Its published stamp is taken here, before any task can start the read, as a leader's is.
    /// The returned future awaits each task under its slot lock and publishes, as a leader does, up to the first failure; the tasks it leaves stay attachable, as a cancelled leader's do.
    pub(crate) fn spawn_batch<D, Fut>(
        &self,
        elected: Vec<ElectedMiss>,
        ctx: D,
        mut refresh: impl FnMut(usize, &SeriesKey) -> Fut,
    ) -> impl std::future::Future<Output = Result<Vec<(SeriesKey, Arc<SeriesSnapshot>)>, RefreshError>>
    where
        D: Clone + Send + 'static,
        Fut: std::future::Future<Output = RefreshOutcome> + Send + 'static,
    {
        let started = Instant::now();
        let running: Vec<ElectedMiss> = elected
            .into_iter()
            .enumerate()
            .map(|(index, mut miss)| {
                let fut = refresh(index, &miss.key);
                miss.state.running =
                    Some(self.spawn_detached(&miss.key, ctx.clone(), fut, started));
                miss
            })
            .collect();
        async move {
            let mut rows = Vec::with_capacity(running.len());
            for mut miss in running {
                let published = Self::await_running(&mut miss.state).await?;
                miss.state.published = Some(published.clone());
                rows.push((miss.key, published.rows));
            }
            Ok(rows)
        }
    }

    /// Await a detached refresh's result while holding the key lock (later
    /// callers queue on the lock exactly as they do behind an inline leader).
    /// Awaiting `&mut JoinHandle` is cancel-safe: cancellation releases the
    /// lock and leaves the task and its pending result in place for the next
    /// caller.
    async fn await_running(state: &mut SlotState) -> RunningOutcome {
        let handle = state.running.as_mut().expect("caller checked running");
        let done = match handle.await {
            Ok(outcome) => outcome,
            // The task died without a result (panic or runtime shutdown).
            Err(_) => Err(RefreshError::Died),
        };
        state.running = None;
        done
    }
}

type SeriesRefreshLease = crate::refresh_locks::RefreshLease<SeriesKey, SlotState>;

/// A key [`SeriesRefreshLocks::elect_misses`] elected; its slot stays locked until the batch that refreshes it publishes.
pub(crate) struct ElectedMiss {
    pub(crate) key: SeriesKey,
    // Declared before the lease so it drops first: the lease's exact registry removal needs the slot's last Arc.
    state: tokio::sync::OwnedMutexGuard<SlotState>,
    _lease: SeriesRefreshLease,
}

/// What the cache knows when a query starts.
pub enum Lookup {
    /// Entry is fresh enough to serve as-is.
    Fresh(Arc<SeriesSnapshot>),
    /// Entry exists; fetch rows with `inserted_at > watermark - overlap`
    /// and call [`SeriesCache::apply_increment`] with this `gen` — it names
    /// the base the watermark came from (see [`Entry::gen`]).
    Stale { watermark_ms: i64, gen: u64 },
    /// No entry; fetch everything and call [`SeriesCache::insert_full_with_origin`].
    Miss,
}

impl SeriesCache {
    pub fn new() -> Self {
        Self::with_budget(default_budget_bytes())
    }

    pub(crate) fn with_budget(budget_bytes: usize) -> Self {
        Self {
            inner: std::sync::Mutex::new(Inner::default()),
            budget_bytes,
        }
    }

    /// Record that these runs just gained rows. [`crate::clickhouse::ChClient::insert_batch`] calls this when an insert returns, before its ack, so the note predates EVERY channel that can reveal a version including the rows (the push frame, PollVersions/resync polls, TerminateRun): entries whose fetch STARTED before it may predate the rows and must not serve Fresh. Without that, a run's final flush would permanently truncate its chart. The first lookup after a note goes Stale (one cheap incremental), re-stamps past it, and the rest of the burst serves Fresh.
    pub fn note_bumps<'a>(&self, run_ids: impl Iterator<Item = &'a str>) {
        let mut inner = self.inner.lock().unwrap();
        if inner
            .last_prune
            .is_none_or(|pruned| pruned.elapsed() >= PRUNE_INTERVAL)
        {
            inner.bumps.retain(|_, t| t.elapsed() < BUMP_RETENTION);
            inner.last_prune = Some(Instant::now());
        }
        // Stamped under the lock, so a run's note only ever moves later.
        let now = Instant::now();
        for run_id in run_ids {
            inner.bumps.insert(run_id.to_string(), now);
        }
    }

    /// Latest note for a run. Other caches use the same
    /// start-before-bump gate as the raw-series cache so a response triggered
    /// by a pushed version cannot reuse metadata from before that insert.
    pub fn last_bump(&self, run_id: &str) -> Option<Instant> {
        self.inner.lock().unwrap().bumps.get(run_id).copied()
    }

    /// Probe without emitting a lookup disposition. Refresh coordination uses
    /// this before and after waiting, then records exactly one final work
    /// disposition: only the elected loader reports miss/stale (including a
    /// failed attempt); followers that avoid ClickHouse report fresh.
    fn lookup_untracked(&self, key: &SeriesKey) -> Lookup {
        let mut inner = self.inner.lock().unwrap();
        let last_bump = inner.bumps.get(&key.run_id).copied();
        match inner.map.get_mut(key) {
            None => Lookup::Miss,
            Some(e) => {
                e.last_access = Instant::now();
                if is_fresh(last_bump, e.started) {
                    Lookup::Fresh(e.rows.clone())
                } else {
                    Lookup::Stale {
                        watermark_ms: e.max_inserted_ms,
                        gen: e.gen,
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub fn lookup(&self, key: &SeriesKey) -> Lookup {
        let result = self.lookup_untracked(key);
        Self::record_lookup(&result);
        result
    }

    fn record_lookup(result: &Lookup) {
        let label = match result {
            Lookup::Fresh(_) => "fresh",
            Lookup::Stale { .. } => "stale",
            Lookup::Miss => "miss",
        };
        Self::record_lookup_result(label);
    }

    fn record_shared(source: &'static str) {
        metrics::counter!("mkdb2_series_refresh_shared_total", "source" => source).increment(1);
    }

    fn record_lookup_result(label: &'static str) {
        metrics::counter!("mkdb2_series_cache_lookups_total", "result" => label).increment(1);
    }

    #[cfg(test)]
    pub fn insert_full(
        &self,
        key: SeriesKey,
        rows: Vec<VersionedRawPoint>,
        fetch_started: Instant,
    ) -> Arc<SeriesSnapshot> {
        self.insert_full_with_origin(key, rows, fetch_started, LineageOrigin::Miss)
    }

    /// Store a full fetch. `fetch_started` anchors the watermark cap, feeds the bump gate and starts the fresh window. An LRU victim can recover its lineage only from a matching, single-use eviction record.
    pub(crate) fn insert_full_with_origin(
        &self,
        key: SeriesKey,
        rows: Vec<VersionedRawPoint>,
        fetch_started: Instant,
        origin: LineageOrigin,
    ) -> Arc<SeriesSnapshot> {
        let bytes = entry_bytes(&rows);
        // Consume before hashing outside the mutex: exactly one full fetch can own this lineage while no cache entry holds it. Never restore a claimed record, even if the fetched contents do not match.
        let evicted = self.inner.lock().unwrap().evicted.remove(&key);
        let rows = Arc::new(SeriesSnapshot::reload(rows, origin, evicted.as_ref()));
        let max_inserted_ms = rows.maximum.unwrap_or(0).min(watermark_cap(fetch_started));
        let mut inner = self.inner.lock().unwrap();
        // A concurrent fetch may have published and evicted a different lineage while this one was hashing. That record must not survive this replacement.
        inner.evicted.remove(&key);
        // Concurrent full fetches race their stores; last writer wins, even if its snapshot is older (local start order can't order ClickHouse snapshots anyway). Safe without an ordering guard: a noted insert is protected by the bump gate in lookup regardless of which racer won, and a regression past a write nothing notes heals on the first incremental after FRESH_WINDOW.
        if let Some(old) = inner.map.remove(&key) {
            inner.total_bytes -= old.bytes;
        }
        // An entry larger than the whole budget is never retained — eviction can't save a sole entry, so once stored it would sit over budget for as long as it's read. Serving it pass-through makes the budget a hard bound on retained memory, at ablated-mode read cost for just this series.
        if bytes > self.budget_bytes {
            metrics::counter!("mkdb2_series_cache_oversized_total").increment(1);
            Self::settle(&mut inner, self.budget_bytes);
            return rows;
        }
        inner.total_bytes += bytes;
        let gen = inner.next_gen;
        inner.next_gen += 1;
        inner.map.insert(
            key,
            Entry {
                rows: rows.clone(),
                max_inserted_ms,
                started: fetch_started,
                gen,
                last_access: Instant::now(),
                bytes,
            },
        );
        Self::settle(&mut inner, self.budget_bytes);
        rows
    }

    /// Fold an incremental fetch into the cached entry. `Ok(rows)` is the
    /// merged series; `Err(())` means the increment rewrites history and
    /// the caller must do a full fetch + [`Self::insert_full_with_origin`].
    /// `fetch_started` is the fetch START, as in [`Self::insert_full_with_origin`]; it only ever advances `started` (a concurrent later-started query may have stamped first), which restarts the fresh window: the rows are verified current as of that fetch. `gen` is the [`Lookup::Stale`] generation the increment's watermark came from.
    pub fn apply_increment(
        &self,
        key: &SeriesKey,
        increment: Vec<VersionedRawPoint>,
        fetch_started: Instant,
        gen: u64,
    ) -> Result<Arc<SeriesSnapshot>, ()> {
        let mut inner = self.inner.lock().unwrap();
        let Some(e) = inner.map.get_mut(key) else {
            // Evicted between lookup and apply — treat as rewrite so the
            // caller re-inserts from a full fetch.
            return Err(());
        };
        if e.gen != gen {
            // The base was replaced while the increment was in flight (concurrent full fetch): the watermark this increment was computed against no longer describes the entry, and merging could bury a hole below the merged watermark where no later incremental looks. Same remedy as eviction: full rebuild.
            return Err(());
        }
        if increment.is_empty() {
            e.started = e.started.max(fetch_started);
            return Ok(e.rows.clone());
        }
        let Some(merged) = e.rows.merge(&increment) else {
            metrics::counter!("mkdb2_series_cache_rewrites_total").increment(1);
            return Err(());
        };
        let new_bytes = entry_bytes(&merged);
        // The merge grew the entry past the whole budget: hand out the merged rows but stop retaining the entry (see the full-store path) — the next poll misses, full-fetches, and stays pass-through while the series is this large.
        if new_bytes > self.budget_bytes {
            metrics::counter!("mkdb2_series_cache_oversized_total").increment(1);
            let e = inner.map.remove(key).unwrap();
            inner.total_bytes -= e.bytes;
            Self::settle(&mut inner, self.budget_bytes);
            return Ok(Arc::new(merged));
        }
        // Capped like the full-store watermark; never regresses below the old mark (it
        // was capped the same way when set).
        let new_wm = increment
            .iter()
            .map(|r| r.inserted_ms)
            .max()
            .unwrap_or(e.max_inserted_ms)
            .min(watermark_cap(fetch_started))
            .max(e.max_inserted_ms);
        inner.total_bytes = inner.total_bytes + new_bytes - inner.map[key].bytes;
        let e = inner.map.get_mut(key).unwrap();
        e.rows = Arc::new(merged);
        e.max_inserted_ms = new_wm;
        e.bytes = new_bytes;
        e.started = e.started.max(fetch_started);
        let rows = e.rows.clone();
        Self::settle(&mut inner, self.budget_bytes);
        Ok(rows)
    }

    /// Remove every cached metric for the exact `(project_id, run_id)` identities. The full identity matches the cache/storage key and keeps this eviction defensively project-scoped, even though admission now permanently reserves each run ID to one project.
    ///
    /// Bump notes follow a separate safety lifetime: they must outlive any fetch that started before the insert they note (see [`BUMP_RETENTION`]). They hold no series rows and age out on their existing retention schedule.
    pub fn purge_runs<'a>(&self, runs: impl IntoIterator<Item = (&'a str, &'a str)>) -> usize {
        let runs = runs.into_iter().collect::<HashSet<_>>();
        let mut inner = self.inner.lock().unwrap();
        let mut removed = 0usize;
        let mut removed_bytes = 0usize;
        inner.map.retain(|key, entry| {
            if runs.contains(&(key.project_id.as_str(), key.run_id.as_str())) {
                removed += 1;
                removed_bytes += entry.bytes;
                false
            } else {
                true
            }
        });
        inner
            .evicted
            .retain(|key, _| !runs.contains(&(key.project_id.as_str(), key.run_id.as_str())));
        inner.total_bytes -= removed_bytes;
        Self::settle(&mut inner, self.budget_bytes);
        removed
    }

    /// Drop least-recently-accessed entries until under budget, then publish the size gauges. Call before returning from any mutation. Every retained entry is individually within budget (oversized ones are rejected at both insert sites), so the loop reaches the budget before it could empty the map.
    fn settle(inner: &mut Inner, budget: usize) {
        while inner.total_bytes > budget {
            let oldest = inner
                .map
                .iter()
                .min_by_key(|(_, e)| e.last_access)
                .map(|(k, _)| k.clone());
            let Some(k) = oldest else { break };
            if let Some(e) = inner.map.remove(&k) {
                inner.total_bytes -= e.bytes;
                inner.evicted.insert(
                    k,
                    EvictedLineage {
                        lineage: e.rows.lineage,
                        maximum: e.rows.maximum,
                        origin: e.rows.origin,
                        content: e.rows.content,
                        count: e.rows.len(),
                        evicted: Instant::now(),
                    },
                );
                if inner.evicted.len() > EVICTED_LINEAGE_LIMIT {
                    let oldest = inner
                        .evicted
                        .iter()
                        .min_by_key(|(_, record)| record.evicted)
                        .map(|(key, _)| key.clone())
                        .unwrap();
                    inner.evicted.remove(&oldest);
                }
                metrics::counter!("mkdb2_series_cache_evictions_total").increment(1);
            }
        }
        metrics::gauge!("mkdb2_series_cache_bytes").set(inner.total_bytes as f64);
        metrics::gauge!("mkdb2_series_cache_entries").set(inner.map.len() as f64);
    }
}

/// Merge incremental rows into a cached (tag, step)-sorted list. Returns
/// None when the increment rewrites history — a row whose (tag, step)
/// already exists with different content (a resumed run re-logging), or
/// lands before that tag's high-water step. Exact duplicates (same step,
/// same bits — the watermark-overlap re-read) are dropped.
#[cfg(test)]
pub fn merge_increment(
    cached: &[VersionedRawPoint],
    increment: &[VersionedRawPoint],
) -> Option<Vec<VersionedRawPoint>> {
    merge_increment_tracked(cached, increment).map(|merged| merged.rows)
}

struct MergedRows {
    rows: Vec<VersionedRawPoint>,
    added_min: Option<i64>,
    added_max: Option<i64>,
    added_content: u64,
}

fn merge_increment_tracked(
    cached: &[VersionedRawPoint],
    increment: &[VersionedRawPoint],
) -> Option<MergedRows> {
    // The increment is read WITHOUT FINAL (see `fetch_increment`), so one
    // (tag, step) can appear as several versions — unmerged duplicate
    // inserts, or a re-log inside the watermark window. ORDER BY (tag,
    // step) leaves versions adjacent but unordered; keep the one FINAL
    // would have kept (max `inserted_ms` — the ReplacingMergeTree version
    // column) before any rewrite comparison, so a superseded sibling never
    // masquerades as a conflict.
    let mut versions: Vec<&VersionedRawPoint> = Vec::with_capacity(increment.len());
    let mut maximum_version_conflicts = false;
    for r in increment {
        match versions.last_mut() {
            Some(last) if last.tag == r.tag && last.step == r.step => {
                if r.inserted_ms > last.inserted_ms {
                    *last = r;
                    maximum_version_conflicts = false;
                } else if r.inserted_ms == last.inserted_ms
                    && (r.is_value != last.is_value
                        || (r.is_value != 0
                            && (r.value.to_bits() != last.value.to_bits()
                                || r.timestamp_ms != last.timestamp_ms)))
                {
                    // ReplacingMergeTree cannot deterministically choose between disagreeing equal-version siblings from this non-FINAL result. Let an authoritative FINAL read settle the key instead of depending on physical row order.
                    maximum_version_conflicts = true;
                }
            }
            _ => {
                if maximum_version_conflicts {
                    return None;
                }
                versions.push(r);
                maximum_version_conflicts = false;
            }
        }
    }
    if maximum_version_conflicts {
        return None;
    }

    // A newer nonnumeric payload at an existing numeric key is a deletion from this series. Fall back to the authoritative FINAL read. Tombstones for keys the numeric cache never held are irrelevant but still advance the fetch watermark in apply_increment.
    let mut tombstone_rewrite = false;
    versions.retain(|row| {
        if row.is_value != 0 {
            return true;
        }
        tombstone_rewrite |= cached
            .binary_search_by(|cached| {
                (cached.tag.as_str(), cached.step).cmp(&(row.tag.as_str(), row.step))
            })
            .is_ok_and(|index| row.inserted_ms >= cached[index].inserted_ms);
        false
    });
    if tombstone_rewrite {
        return None;
    }
    let inc = versions;

    // Per-tag high-water steps of the cached rows (one pass — sorted by tag).
    let mut tag_max: HashMap<&str, i64> = HashMap::new();
    for r in cached {
        tag_max.insert(r.tag.as_str(), r.step); // sorted: last write wins
    }
    // Existing (tag, step) pairs near each tag's tail can re-appear via the
    // overlap window; identical rows are dropped, differing ones are rewrites.
    let mut out = Vec::with_capacity(cached.len() + inc.len());
    let (mut added_min, mut added_max) = (None, None);
    let mut added_content = 0u64;
    let mut note_added = |row: &VersionedRawPoint| {
        let stamp = row.inserted_ms;
        added_min = Some(added_min.map_or(stamp, |old: i64| old.min(stamp)));
        added_max = Some(added_max.map_or(stamp, |old: i64| old.max(stamp)));
        added_content = added_content.wrapping_add(row_content(row));
    };
    let (mut a, mut b) = (cached.iter().peekable(), inc.iter().peekable());
    loop {
        match (a.peek(), b.peek()) {
            (Some(x), Some(y)) => {
                let kx = (x.tag.as_str(), x.step);
                let ky = (y.tag.as_str(), y.step);
                if kx < ky {
                    out.push((*x).clone());
                    a.next();
                } else if kx > ky {
                    // A genuinely new row must extend its tag past the
                    // cached high-water step; anything else is a rewrite.
                    if tag_max.get(ky.0).is_some_and(|&m| ky.1 <= m) {
                        return None;
                    }
                    note_added(y);
                    out.push((**y).clone());
                    b.next();
                } else {
                    // Same (tag, step): retain the cached row and its ORIGINAL insertion stamp on an identical-content overlap. The append-lineage proof depends on that stamp staying unchanged even when a re-log supplies a newer one.
                    if x.value.to_bits() != y.value.to_bits() || x.timestamp_ms != y.timestamp_ms {
                        return None;
                    }
                    out.push((*x).clone());
                    a.next();
                    b.next();
                }
            }
            (Some(x), None) => {
                out.push((*x).clone());
                a.next();
            }
            (None, Some(y)) => {
                if tag_max.get(y.tag.as_str()).is_some_and(|&m| y.step <= m) {
                    return None;
                }
                note_added(y);
                out.push((**y).clone());
                b.next();
            }
            (None, None) => break,
        }
    }
    Some(MergedRows {
        rows: out,
        added_min,
        added_max,
        added_content,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(tag: &str, step: i64, value: f32, ins: i64) -> VersionedRawPoint {
        VersionedRawPoint {
            tag: tag.to_string(),
            step,
            timestamp_ms: step * 10,
            value,
            is_value: 1,
            inserted_ms: ins,
        }
    }

    fn tombstone(tag: &str, step: i64, ins: i64) -> VersionedRawPoint {
        VersionedRawPoint {
            tag: tag.to_string(),
            step,
            timestamp_ms: step * 10,
            value: 0.0,
            is_value: 0,
            inserted_ms: ins,
        }
    }

    /// Current generation of the cached entry (what Lookup::Stale would hand a caller).
    fn gen_of(cache: &SeriesCache, k: &SeriesKey) -> u64 {
        cache.inner.lock().unwrap().map[k].gen
    }

    #[test]
    fn snapshot_lineage_tracks_additions_not_fetch_watermarks_or_overlap_restamps() {
        let cache = SeriesCache::new();
        let key = SeriesKey::new("p", "r", "m");
        let now = unix_ms_now();
        let first = cache.insert_full(key.clone(), vec![row("", 0, 1.0, now)], Instant::now());
        assert_eq!(first.maximum(), Some(now));
        assert!(cache.inner.lock().unwrap().map[&key].max_inserted_ms < now);
        let generation = gen_of(&cache, &key);
        let overlap = cache
            .apply_increment(
                &key,
                vec![row("", 0, 1.0, now + 1)],
                Instant::now(),
                generation,
            )
            .unwrap();
        assert_eq!(overlap.lineage(), first.lineage());
        assert_eq!(overlap[0].inserted_ms, now);
        assert_eq!(overlap.maximum(), Some(now));
        let appended = cache
            .apply_increment(
                &key,
                vec![row("", 1, 2.0, now + 2)],
                Instant::now(),
                generation,
            )
            .unwrap();
        assert_eq!(appended.lineage(), first.lineage());
        assert_eq!(appended.maximum(), Some(now + 2));
        let equal_stamp = cache
            .apply_increment(
                &key,
                vec![row("", 2, 3.0, now + 2)],
                Instant::now(),
                generation,
            )
            .unwrap();
        assert_ne!(equal_stamp.lineage(), appended.lineage());
        assert_eq!(equal_stamp.origin(), LineageOrigin::Late);
        let older_new_tag = cache
            .apply_increment(
                &key,
                vec![row("tag", 0, 4.0, now - 1)],
                Instant::now(),
                generation,
            )
            .unwrap();
        assert_ne!(older_new_tag.lineage(), equal_stamp.lineage());
        assert_eq!(older_new_tag.maximum(), Some(now + 2));
        assert_eq!(first.origin(), LineageOrigin::Miss);
        for snapshot in [&first, &overlap, &appended, &equal_stamp, &older_new_tag] {
            assert_eq!(
                snapshot.content,
                snapshot
                    .iter()
                    .fold(0u64, |sum, row| sum.wrapping_add(row_content(row))),
                "the cached sum includes additions exactly once and retains overlap stamps"
            );
        }
        assert_eq!(first.len(), 1, "published snapshots remain immutable");
    }

    #[test]
    fn irrelevant_tombstone_advances_fetch_watermark_without_forging_numeric_membership() {
        let cache = SeriesCache::new();
        let key = SeriesKey::new("p", "r", "m");
        let first = cache.insert_full(key.clone(), vec![row("", 0, 1.0, 100)], Instant::now());
        let next = cache
            .apply_increment(
                &key,
                vec![tombstone("", 5, 500)],
                Instant::now(),
                gen_of(&cache, &key),
            )
            .unwrap();
        assert_eq!(next.lineage(), first.lineage());
        assert_eq!(next.maximum(), Some(100));
        assert_eq!(cache.inner.lock().unwrap().map[&key].max_inserted_ms, 500);
    }

    #[test]
    fn replacement_purge_and_unrecorded_reloads_start_new_lineages() {
        let source = vec![row("", 0, 1.0, 100)];
        let cache = SeriesCache::with_budget(entry_bytes(&source));
        let key = SeriesKey::new("p", "r", "m");
        let first = cache.insert_full(key.clone(), source.clone(), Instant::now());
        let stale_generation = gen_of(&cache, &key);
        let replacement = cache.insert_full(key.clone(), source.clone(), Instant::now());
        assert_ne!(first.lineage(), replacement.lineage());
        assert!(cache
            .apply_increment(
                &key,
                vec![row("", 1, 2.0, 200)],
                Instant::now(),
                stale_generation
            )
            .is_err());
        let oversized = cache
            .apply_increment(
                &key,
                vec![row("", 1, 2.0, 200)],
                Instant::now(),
                gen_of(&cache, &key),
            )
            .unwrap();
        assert_eq!(oversized.lineage(), replacement.lineage());
        assert!(matches!(cache.lookup(&key), Lookup::Miss));
        assert!(!cache.inner.lock().unwrap().evicted.contains_key(&key));
        let reloaded = cache.insert_full(key.clone(), oversized.to_vec(), Instant::now());
        assert_ne!(reloaded.lineage(), oversized.lineage());
        assert!(matches!(cache.lookup(&key), Lookup::Miss));
        assert!(!cache.inner.lock().unwrap().evicted.contains_key(&key));
        let another_cache = SeriesCache::new();
        let other = another_cache.insert_full(key.clone(), source.clone(), Instant::now());
        assert_ne!(other.lineage(), first.lineage());
        another_cache.purge_runs([("p", "r")]);
        let after_purge = another_cache.insert_full(key, source.clone(), Instant::now());
        assert_ne!(after_purge.lineage(), other.lineage());
        assert_ne!(
            SeriesSnapshot::full(source).lineage(),
            after_purge.lineage()
        );
    }

    fn eviction_cache(row_count: usize) -> SeriesCache {
        SeriesCache::with_budget(entry_bytes(&vec![row("", 0, 0.0, 0); row_count]))
    }

    fn evict(cache: &SeriesCache, key: &SeriesKey, row_count: usize) {
        let other = SeriesKey::new("p", "eviction-pressure", "m");
        cache.insert_full(
            other,
            (0..row_count)
                .map(|step| row("", step as i64, 0.0, 1))
                .collect(),
            Instant::now(),
        );
        assert!(matches!(cache.lookup(key), Lookup::Miss));
        assert!(cache.inner.lock().unwrap().evicted.contains_key(key));
    }

    #[test]
    fn lru_reload_restores_lineage_and_origin_through_repeated_evictions() {
        let cache = eviction_cache(4);
        let key = SeriesKey::new("p", "r", "m");
        let first = cache.insert_full_with_origin(
            key.clone(),
            vec![row("a", 2, 1.0, 100)],
            Instant::now(),
            LineageOrigin::Rewrite,
        );
        evict(&cache, &key, 4);
        let second = cache.insert_full(
            key.clone(),
            vec![row("a", 2, 1.0, 100), row("a", 4, 2.0, 200)],
            Instant::now(),
        );
        assert_eq!(second.lineage(), first.lineage());
        assert_eq!(second.origin(), LineageOrigin::Rewrite);
        evict(&cache, &key, 4);
        let third = cache.insert_full(key.clone(), second.to_vec(), Instant::now());
        assert_eq!(third.lineage(), first.lineage());
        evict(&cache, &key, 4);
        let regressed = cache.insert_full(key, first.to_vec(), Instant::now());
        assert_ne!(
            regressed.lineage(),
            first.lineage(),
            "the newest eviction maximum cannot regress"
        );
        assert_eq!(regressed.origin(), LineageOrigin::Miss);
    }

    #[test]
    fn lru_reload_rejects_changed_held_rows_late_rows_and_step_backfills() {
        let original = vec![row("a", 2, 1.0, 100), row("a", 6, 2.0, 200)];
        let mut timestamp_rewrite = original.clone();
        timestamp_rewrite[0].timestamp_ms += 1;
        let cases = [
            vec![row("a", 2, 9.0, 100), original[1].clone()],
            timestamp_rewrite,
            vec![original[0].clone()],
            vec![row("a", 2, 1.0, 300), original[1].clone()],
            vec![
                original[0].clone(),
                original[1].clone(),
                row("a", 8, 3.0, 200),
            ],
            vec![
                original[0].clone(),
                row("a", 4, 3.0, 300),
                original[1].clone(),
            ],
        ];
        for (case, rows) in cases.into_iter().enumerate() {
            let cache = eviction_cache(4);
            let key = SeriesKey::new("p", "r", "m");
            let first = cache.insert_full(key.clone(), original.clone(), Instant::now());
            evict(&cache, &key, 4);
            let reloaded = cache.insert_full(key, rows, Instant::now());
            assert_ne!(reloaded.lineage(), first.lineage(), "case {case}");
        }
    }

    #[test]
    fn empty_lineage_can_be_reloaded_and_then_extended() {
        let cache = eviction_cache(2);
        let key = SeriesKey::new("p", "r", "m");
        let first = cache.insert_full(key.clone(), vec![], Instant::now());
        evict(&cache, &key, 2);
        let empty = cache.insert_full(key.clone(), vec![], Instant::now());
        assert_eq!(empty.lineage(), first.lineage());
        evict(&cache, &key, 2);
        let populated = cache.insert_full(key, vec![row("new", 0, 1.0, -5)], Instant::now());
        assert_eq!(populated.lineage(), first.lineage());
        assert_eq!(populated.maximum(), Some(-5));
    }

    #[test]
    fn oversized_reload_consumes_recovery_without_recording_its_returned_snapshot() {
        let cache = eviction_cache(1);
        let key = SeriesKey::new("p", "r", "m");
        let first = cache.insert_full(key.clone(), vec![row("", 0, 1.0, 100)], Instant::now());
        evict(&cache, &key, 1);
        let full = vec![row("", 0, 1.0, 100), row("", 1, 2.0, 200)];
        let oversized = cache.insert_full(key.clone(), full.clone(), Instant::now());
        assert_eq!(oversized.lineage(), first.lineage());
        assert!(matches!(cache.lookup(&key), Lookup::Miss));
        assert!(!cache.inner.lock().unwrap().evicted.contains_key(&key));
        let repeated = cache.insert_full(key.clone(), full, Instant::now());
        assert_ne!(repeated.lineage(), oversized.lineage());
        assert!(!cache.inner.lock().unwrap().evicted.contains_key(&key));
    }

    #[test]
    fn concurrent_full_reloads_cannot_fork_an_evicted_lineage() {
        let cache = eviction_cache(3);
        let key = SeriesKey::new("p", "r", "m");
        let first = cache.insert_full(key.clone(), vec![row("", 0, 1.0, 100)], Instant::now());
        evict(&cache, &key, 3);
        let barrier = std::sync::Barrier::new(2);
        let returned = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for step in [1, 2] {
                let (cache, key, barrier) = (&cache, &key, &barrier);
                handles.push(scope.spawn(move || {
                    barrier.wait();
                    cache.insert_full(
                        key.clone(),
                        vec![row("", 0, 1.0, 100), row("", step, 2.0, 200)],
                        Instant::now(),
                    )
                }));
            }
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(
            returned
                .iter()
                .filter(|rows| rows.lineage() == first.lineage())
                .count(),
            1
        );
        assert_ne!(returned[0].lineage(), returned[1].lineage());
    }

    #[test]
    fn eviction_records_are_single_use_bounded_and_purged_by_exact_run() {
        let cache = eviction_cache(1);
        let key = SeriesKey::new("p", "r", "m");
        let first = cache.insert_full(key.clone(), vec![row("", 0, 1.0, 100)], Instant::now());
        evict(&cache, &key, 1);
        let claimed = cache.inner.lock().unwrap().evicted.remove(&key).unwrap();
        let concurrent = cache.insert_full(key.clone(), first.to_vec(), Instant::now());
        assert_ne!(concurrent.lineage(), claimed.lineage);
        evict(&cache, &key, 1);
        let other_project = SeriesKey::new("other-project", "r", "m");
        let other = cache.insert_full(other_project.clone(), first.to_vec(), Instant::now());
        evict(&cache, &other_project, 1);
        cache.purge_runs([("p", "r")]);
        assert!(!cache.inner.lock().unwrap().evicted.contains_key(&key));
        let reloaded = cache.insert_full(other_project, first.to_vec(), Instant::now());
        assert_eq!(reloaded.lineage(), other.lineage());
        for n in 0..=EVICTED_LINEAGE_LIMIT + 1 {
            cache.insert_full(
                SeriesKey::new("p", format!("bounded-{n}"), "m"),
                first.to_vec(),
                Instant::now(),
            );
        }
        let evicted = &cache.inner.lock().unwrap().evicted;
        assert_eq!(evicted.len(), EVICTED_LINEAGE_LIMIT);
        assert!(!evicted.contains_key(&SeriesKey::new("p", "bounded-0", "m")));
        let newest = SeriesKey::new("p", format!("bounded-{EVICTED_LINEAGE_LIMIT}"), "m");
        assert!(evicted.contains_key(&newest));
    }

    #[test]
    fn cache_lineage_retains_every_prior_held_set_as_a_per_tag_step_prefix() {
        let mut comparisons = 0;
        let mut restored = 0;
        let mut refused = 0;
        let mut late = 0;
        for seed in 1..=8u64 {
            let cache = eviction_cache(128);
            let key = SeriesKey::new("p", "r", "m");
            let mut current =
                cache.insert_full(key.clone(), vec![row("", 2, 1.0, 100)], Instant::now());
            let mut history = vec![current.clone()];
            let mut random = seed;
            for tick in 1..=120 {
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                let tag = ["", "a", "b"][(random as usize >> 8) % 3];
                let step = current
                    .iter()
                    .filter(|row| row.tag == tag)
                    .map(|row| row.step)
                    .max()
                    .unwrap_or(0)
                    + 2;
                let stamp = 100 + tick * 5;
                let old_lineage = current.lineage();
                if random % 9 == 8 {
                    let mut reloaded = current.to_vec();
                    if random & 32 != 0 {
                        let step = if random & 64 != 0 { -tick } else { step };
                        reloaded.push(row(tag, step, tick as f32, stamp));
                        reloaded.sort_by(|a, b| (&a.tag, a.step).cmp(&(&b.tag, b.step)));
                    }
                    evict(&cache, &key, 128);
                    current = cache.insert_full(key.clone(), reloaded, Instant::now());
                    restored += usize::from(current.lineage() == old_lineage);
                } else {
                    let mut changed = row(tag, step, tick as f32, stamp);
                    if !current.is_empty() {
                        match random % 9 {
                            3 => changed.inserted_ms = current.maximum().unwrap(),
                            4 | 5 | 7 => {
                                changed = current[(random as usize >> 16) % current.len()].clone();
                                changed.inserted_ms = stamp;
                                if random % 9 == 5 {
                                    changed.value += 1.0;
                                } else if random % 9 == 7 {
                                    changed.is_value = 0;
                                }
                            }
                            6 => changed.step = -tick,
                            _ => {}
                        }
                    }
                    current = match cache.apply_increment(
                        &key,
                        vec![changed.clone()],
                        Instant::now(),
                        gen_of(&cache, &key),
                    ) {
                        Ok(rows) => rows,
                        Err(()) => {
                            refused += 1;
                            let mut full = current.to_vec();
                            full.retain(|row| row.tag != changed.tag || row.step != changed.step);
                            if changed.is_value != 0 {
                                full.push(changed);
                            }
                            full.sort_by(|a, b| (&a.tag, a.step).cmp(&(&b.tag, b.step)));
                            cache.insert_full_with_origin(
                                key.clone(),
                                full,
                                Instant::now(),
                                LineageOrigin::Rewrite,
                            )
                        }
                    };
                    late += usize::from(
                        current.lineage() != old_lineage && current.origin() == LineageOrigin::Late,
                    );
                }
                for held in history
                    .iter()
                    .filter(|held| held.lineage() == current.lineage())
                {
                    comparisons += 1;
                    let mut recovered = Vec::new();
                    let (mut last_tag, mut seen_new) = (None, false);
                    for row in current.iter() {
                        if last_tag != Some(row.tag.as_str()) {
                            last_tag = Some(row.tag.as_str());
                            seen_new = false;
                        }
                        if held
                            .maximum()
                            .is_some_and(|maximum| row.inserted_ms <= maximum)
                        {
                            assert!(
                                !seen_new,
                                "seed {seed}, tick {tick}: new step precedes held row"
                            );
                            recovered.push(row);
                        } else {
                            seen_new = true;
                        }
                    }
                    assert_eq!(recovered.len(), held.len(), "seed {seed}, tick {tick}");
                    for (actual, expected) in recovered.into_iter().zip(held.iter()) {
                        assert_eq!(
                            (
                                &actual.tag,
                                actual.step,
                                actual.timestamp_ms,
                                actual.value.to_bits(),
                                actual.is_value,
                                actual.inserted_ms
                            ),
                            (
                                &expected.tag,
                                expected.step,
                                expected.timestamp_ms,
                                expected.value.to_bits(),
                                expected.is_value,
                                expected.inserted_ms
                            ),
                            "seed {seed}, tick {tick}"
                        );
                    }
                }
                history.push(current.clone());
            }
        }
        assert!(
            comparisons > 500 && restored > 20 && refused > 100 && late > 20,
            "comparisons={comparisons}, restored={restored}, refused={refused}, late={late}"
        );
    }

    #[test]
    fn append_extends_tail() {
        let cached = vec![row("", 1, 1.0, 100), row("", 2, 2.0, 200)];
        let inc = vec![row("", 3, 3.0, 300), row("", 4, 4.0, 400)];
        let merged = merge_increment(&cached, &inc).unwrap();
        assert_eq!(merged.len(), 4);
        assert!(merged.windows(2).all(|w| w[0].step < w[1].step));
    }

    #[test]
    fn overlap_duplicates_are_dropped() {
        let cached = vec![row("", 1, 1.0, 100), row("", 2, 2.0, 200)];
        let inc = vec![row("", 2, 2.0, 200), row("", 3, 3.0, 300)];
        let merged = merge_increment(&cached, &inc).unwrap();
        assert_eq!(merged.len(), 3);
    }

    #[test]
    fn rewrite_is_detected() {
        let cached = vec![row("", 1, 1.0, 100), row("", 5, 5.0, 200)];
        // value changed at an existing step
        assert!(merge_increment(&cached, &[row("", 5, 9.9, 300)]).is_none());
        // new row before the high-water step
        assert!(merge_increment(&cached, &[row("", 3, 3.0, 300)]).is_none());
    }

    #[test]
    fn new_tags_interleave_sorted() {
        let cached = vec![row("0", 1, 1.0, 100), row("2", 1, 1.0, 100)];
        let inc = vec![row("0", 2, 2.0, 200), row("1", 1, 1.0, 200)];
        let merged = merge_increment(&cached, &inc).unwrap();
        let keys: Vec<(String, i64)> = merged.iter().map(|r| (r.tag.clone(), r.step)).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
        assert_eq!(merged.len(), 4);
    }

    #[test]
    fn nan_values_compare_by_bits() {
        let cached = vec![row("", 1, f32::NAN, 100)];
        let inc = vec![row("", 1, f32::NAN, 100)];
        // NaN != NaN must not read as a rewrite
        assert!(merge_increment(&cached, &inc).is_some());
    }

    #[test]
    fn increment_versions_keep_latest() {
        // No-FINAL reads can return several versions of one (tag, step);
        // the max-inserted_ms one wins regardless of adjacency order.
        let cached = vec![row("", 1, 1.0, 100)];
        for inc in [
            vec![row("", 2, 2.0, 300), row("", 2, 2.5, 350)],
            vec![row("", 2, 2.5, 350), row("", 2, 2.0, 300)],
        ] {
            let merged = merge_increment(&cached, &inc).unwrap();
            assert_eq!(merged.len(), 2);
            assert_eq!(merged[1].value, 2.5);
        }
        // A superseded sibling of a row the cache already holds must not
        // read as a rewrite when the surviving version matches the cache.
        let inc = vec![row("", 1, 0.5, 50), row("", 1, 1.0, 100)];
        assert_eq!(merge_increment(&cached, &inc).unwrap().len(), 1);
        // ...but a NEWER conflicting version of a cached step still is one.
        let inc = vec![row("", 1, 1.0, 100), row("", 1, 9.9, 400)];
        assert!(merge_increment(&cached, &inc).is_none());
    }

    #[test]
    fn nonnumeric_replacements_invalidate_only_cached_numeric_keys() {
        let cached = vec![row("", 1, 1.0, 100)];

        assert!(merge_increment(&cached, &[tombstone("", 1, 200)]).is_none());
        assert_eq!(
            merge_increment(&cached, &[tombstone("", 2, 200)])
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            merge_increment(&cached, &[tombstone("", 2, 200), row("", 2, 2.0, 300)])
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            merge_increment(&cached, &[tombstone("", 1, 50)])
                .unwrap()
                .len(),
            1
        );

        for conflict in [
            vec![tombstone("", 2, 200), row("", 2, 2.0, 200)],
            vec![row("", 2, 2.0, 200), tombstone("", 2, 200)],
            vec![row("", 2, 2.0, 200), row("", 2, 3.0, 200)],
        ] {
            assert!(merge_increment(&cached, &conflict).is_none());
        }

        // A strictly newer version resolves an older equal-version conflict.
        assert_eq!(
            merge_increment(
                &cached,
                &[
                    tombstone("", 2, 200),
                    row("", 2, 2.0, 200),
                    row("", 2, 3.0, 300),
                ],
            )
            .unwrap()
            .len(),
            2
        );
    }

    #[test]
    fn slow_reads_cap_the_watermark_at_their_start() {
        let cache = SeriesCache::with_budget(usize::MAX);
        let k = SeriesKey::new("p", "r", "m");
        // A row stamped "now" must not advance the watermark past
        // its read's START − margin, or rows from a still-committing concurrent flush
        // (stamped earlier, visible later) would be skipped forever. This read ran 20 s.
        let slow = Duration::from_secs(20);
        let started = Instant::now() - slow;
        let now = unix_ms_now();
        let expected = now - slow.as_millis() as i64 - VISIBILITY_MARGIN_MS;
        cache.insert_full(k.clone(), vec![row("", 1, 1.0, now)], started);
        let wm = cache.inner.lock().unwrap().map[&k].max_inserted_ms;
        assert!(wm.abs_diff(expected) < 1_000, "{wm} vs {expected}");
        // Ancient stamps are below the cap already: watermark = the stamp
        // (idle series keep their cheap empty increments).
        cache.insert_full(k.clone(), vec![row("", 1, 1.0, 100)], started);
        let wm = cache.inner.lock().unwrap().map[&k].max_inserted_ms;
        assert_eq!(wm, 100);
        cache
            .apply_increment(&k, vec![row("", 2, 2.0, now)], started, gen_of(&cache, &k))
            .unwrap();
        let wm = cache.inner.lock().unwrap().map[&k].max_inserted_ms;
        assert!(wm.abs_diff(expected) < 1_000, "{wm} vs {expected}");
    }

    #[test]
    fn bump_gate_forces_stale_inside_the_fresh_window() {
        let cache = SeriesCache::with_budget(entry_bytes(&vec![row("", 0, 0.0, 0); 8]));
        let k = SeriesKey::new("p", "r", "m");
        cache.insert_full(k.clone(), vec![row("", 1, 1.0, 100)], Instant::now());
        assert!(matches!(cache.lookup(&k), Lookup::Fresh(_)));
        // A note for the run invalidates entries stamped before
        // it, even though they are well inside FRESH_WINDOW...
        cache.note_bumps(std::iter::once("r"));
        assert!(matches!(cache.lookup(&k), Lookup::Stale { .. }));
        // ...while other runs' entries are untouched.
        let k2 = SeriesKey::new("p", "r2", "m");
        cache.insert_full(k2.clone(), vec![row("", 1, 1.0, 100)], Instant::now());
        assert!(matches!(cache.lookup(&k2), Lookup::Fresh(_)));
        // The refetch the bump triggers re-stamps past the note; the rest of
        // its burst dedups as Fresh again.
        cache
            .apply_increment(&k, vec![], Instant::now(), gen_of(&cache, &k))
            .expect("empty increment ok");
        assert!(matches!(cache.lookup(&k), Lookup::Fresh(_)));
    }

    #[test]
    fn increment_against_a_replaced_base_forces_full_rebuild() {
        let cache = SeriesCache::with_budget(entry_bytes(&vec![row("", 0, 0.0, 0); 8]));
        let k = SeriesKey::new("p", "r", "m");
        cache.insert_full(k.clone(), vec![row("", 1, 1.0, 100)], Instant::now());
        let stale_gen = gen_of(&cache, &k);
        // A concurrent full fetch replaces the entry while the increment
        // (computed against stale_gen's watermark) is still in flight...
        cache.insert_full(k.clone(), vec![row("", 1, 1.0, 100)], Instant::now());
        // ...so applying it must refuse: merging into the replacement could
        // bury a hole below the merged watermark that no incremental
        // re-reads.
        assert!(cache
            .apply_increment(&k, vec![row("", 2, 2.0, 300)], Instant::now(), stale_gen)
            .is_err());
        // With the CURRENT generation the same increment applies fine.
        let rows = cache
            .apply_increment(
                &k,
                vec![row("", 2, 2.0, 300)],
                Instant::now(),
                gen_of(&cache, &k),
            )
            .expect("current gen applies");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn bump_notes_survive_pruning_while_a_fetch_is_in_flight() {
        let cache = SeriesCache::with_budget(entry_bytes(&vec![row("", 0, 0.0, 0); 8]));
        let k = SeriesKey::new("p", "r", "m");
        // A bump noted seconds ago while a slow
        // fetch is still in flight. Backdate it directly; note_bumps always
        // stamps now. (10s, not minutes: Instant subtraction panics if it
        // would predate the platform's Instant epoch on a fresh boot.)
        let bump_at = Instant::now() - Duration::from_secs(10);
        cache
            .inner
            .lock()
            .unwrap()
            .bumps
            .insert("r".to_string(), bump_at);
        // The first note of a fresh cache prunes.
        cache.note_bumps(std::iter::once("other"));
        // The slow fetch (started before the bump) finally stores, well inside its window — only the surviving note may keep the gate closed.
        let started = bump_at - Duration::from_secs(1);
        cache.insert_full(k.clone(), vec![row("", 1, 1.0, 100)], started);
        assert!(matches!(cache.lookup(&k), Lookup::Stale { .. }));
        // Truly ancient notes (older than any possible in-flight read) do
        // get pruned.
        if let Some(ancient) = Instant::now().checked_sub(BUMP_RETENTION + Duration::from_secs(60))
        {
            let mut inner = cache.inner.lock().unwrap();
            inner.bumps.insert("ancient".to_string(), ancient);
            inner.last_prune = None;
            drop(inner);
            cache.note_bumps(std::iter::once("trigger"));
            assert!(!cache.inner.lock().unwrap().bumps.contains_key("ancient"));
        }
    }

    #[test]
    fn fresh_window_counts_from_fetch_start() {
        let cache = SeriesCache::with_budget(entry_bytes(&vec![row("", 0, 0.0, 0); 8]));
        // A fetch that started a whole window ago is stale on arrival, however recently it stored.
        if let Some(started) = Instant::now().checked_sub(FRESH_WINDOW + Duration::from_secs(1)) {
            let k = SeriesKey::new("p", "r", "m");
            cache.insert_full(k.clone(), vec![row("", 1, 1.0, 100)], started);
            assert!(matches!(cache.lookup(&k), Lookup::Stale { .. }));
        }
    }

    #[test]
    fn cache_insert_lookup_increment_evict() {
        let cache = SeriesCache::with_budget(entry_bytes(&vec![row("", 0, 0.0, 0); 3]) * 2);
        let k1 = SeriesKey::new("p", "r1", "m");
        let k2 = SeriesKey::new("p", "r2", "m");

        assert!(matches!(cache.lookup(&k1), Lookup::Miss));
        cache.insert_full(k1.clone(), vec![row("", 1, 1.0, 100)], Instant::now());
        // Fresh right after insert
        assert!(matches!(cache.lookup(&k1), Lookup::Fresh(_)));

        // Empty increment refreshes the clock and keeps rows
        let rows = cache
            .apply_increment(&k1, vec![], Instant::now(), gen_of(&cache, &k1))
            .expect("empty increment ok");
        assert_eq!(rows.len(), 1);

        // Real increment extends the rows and the watermark
        let rows = cache
            .apply_increment(
                &k1,
                vec![row("", 2, 2.0, 250)],
                Instant::now(),
                gen_of(&cache, &k1),
            )
            .expect("append ok");
        assert_eq!(rows.len(), 2);
        {
            let inner = cache.inner.lock().unwrap();
            assert_eq!(inner.map[&k1].max_inserted_ms, 250);
        }

        // Second entry blows the (tiny) budget: LRU evicts k1
        cache.insert_full(k2.clone(), vec![row("", 1, 1.0, 100); 3], Instant::now());
        let inner = cache.inner.lock().unwrap();
        assert!(inner.total_bytes <= cache.budget_bytes || inner.map.len() == 1);
    }

    #[test]
    fn oversized_entries_are_never_retained() {
        // Budget fits exactly two rows. Eviction can't touch a sole entry, so a series over the whole budget must be served without being stored — the budget is a hard bound on retained memory.
        let cache = SeriesCache::with_budget(entry_bytes(&vec![row("", 0, 0.0, 0); 2]));
        let k = SeriesKey::new("p", "r", "m");

        // Full insert over budget: rows come back, nothing is retained.
        let rows = cache.insert_full(k.clone(), vec![row("", 1, 1.0, 100); 3], Instant::now());
        assert_eq!(rows.len(), 3);
        assert!(matches!(cache.lookup(&k), Lookup::Miss));
        assert_eq!(cache.inner.lock().unwrap().total_bytes, 0);

        // An increment that grows a retained entry past the budget serves the merged rows but drops the entry.
        cache.insert_full(
            k.clone(),
            vec![row("", 1, 1.0, 100), row("", 2, 2.0, 200)],
            Instant::now(),
        );
        assert!(matches!(cache.lookup(&k), Lookup::Fresh(_)));
        let rows = cache
            .apply_increment(
                &k,
                vec![row("", 3, 3.0, 300)],
                Instant::now(),
                gen_of(&cache, &k),
            )
            .expect("append ok");
        assert_eq!(rows.len(), 3);
        assert!(matches!(cache.lookup(&k), Lookup::Miss));
        assert_eq!(cache.inner.lock().unwrap().total_bytes, 0);
    }

    #[test]
    fn purge_runs_is_exact_and_removes_every_metric() {
        let cache = SeriesCache::with_budget(usize::MAX);
        let target_a = SeriesKey::new("project-a", "same-run", "loss");
        let target_b = SeriesKey::new("project-a", "same-run", "lr");
        let other_project = SeriesKey::new("project-b", "same-run", "loss");
        let target_c = SeriesKey::new("project-a", "other-run", "loss");
        let kept_run = SeriesKey::new("project-a", "kept-run", "loss");

        for key in [&target_a, &target_b, &target_c, &other_project, &kept_run] {
            cache.insert_full(key.clone(), vec![row("", 1, 1.0, 100)], Instant::now());
        }
        cache.note_bumps(std::iter::once("same-run"));

        assert_eq!(
            cache.purge_runs([("project-a", "same-run"), ("project-a", "other-run"),]),
            3
        );
        assert!(matches!(cache.lookup(&target_a), Lookup::Miss));
        assert!(matches!(cache.lookup(&target_b), Lookup::Miss));
        assert!(matches!(cache.lookup(&target_c), Lookup::Miss));
        assert!(!matches!(cache.lookup(&other_project), Lookup::Miss));
        assert!(!matches!(cache.lookup(&kept_run), Lookup::Miss));

        let inner = cache.inner.lock().unwrap();
        assert_eq!(inner.map.len(), 2);
        assert_eq!(
            inner.total_bytes,
            inner.map.values().map(|entry| entry.bytes).sum::<usize>()
        );
        assert!(inner.bumps.contains_key("same-run"));
    }

    fn no_detach() -> Option<crate::clickhouse::RefreshDetach> {
        None
    }

    fn detach_ctx(
        sem: &Arc<tokio::sync::Semaphore>,
    ) -> impl FnOnce() -> Option<crate::clickhouse::RefreshDetach> {
        let sem = sem.clone();
        move || {
            Some(crate::clickhouse::RefreshDetach {
                _permit: sem.try_acquire_owned().unwrap(),
                _run_guard: std::sync::Arc::new(
                    std::sync::Arc::new(tokio::sync::RwLock::new(()))
                        .try_read_owned()
                        .unwrap(),
                ),
            })
        }
    }

    /// Poll `ready` between yields, bounded so a pinned regression fails the
    /// test instead of hanging it.
    async fn wait_until(mut ready: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("condition not reached within 1s");
    }

    fn plain_cache() -> Arc<SeriesCache> {
        Arc::new(SeriesCache::with_budget(1024 * 1024))
    }

    #[tokio::test]
    async fn detached_refresh_survives_requester_cancellation() {
        let cache = plain_cache();
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let sem = Arc::new(tokio::sync::Semaphore::new(2));
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let leader = {
            let (cache, locks, key, sem) = (cache.clone(), locks.clone(), key.clone(), sem.clone());
            let (refresh_cache, refresh_key, calls) = (cache.clone(), key.clone(), calls.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, detach_ctx(&sem), move |_| async move {
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let _ = gate_rx.await;
                        Ok(refresh_cache.insert_full(
                            refresh_key,
                            vec![row("", 1, 1.0, 1)],
                            Instant::now(),
                        ))
                    })
                    .await
            })
        };
        // The task holds one of the two permits once spawned.
        wait_until(|| sem.available_permits() != 2).await;

        let waiter = {
            let (cache, locks, key) = (cache.clone(), locks.clone(), key.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, no_detach, |_| async {
                        unreachable!("waiter must attach, not elect")
                    })
                    .await
            })
        };
        // Leader + task already hold two leases; the third is the waiter's,
        // so this proves the waiter joined before the leader dies.
        wait_until(|| locks.lease_count(&key) >= 3).await;
        leader.abort();
        assert!(leader.await.unwrap_err().is_cancelled());

        gate_tx.send(()).unwrap();
        let rows = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap()
            .expect("waiter shares the detached result");
        assert_eq!(rows.len(), 1);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        // The task's split permit is returned once it completes.
        wait_until(|| sem.available_permits() == 2).await;
        // The cache is populated even though the original requester died.
        assert!(matches!(
            cache.lookup_untracked(&key),
            Lookup::Fresh(_) | Lookup::Stale { .. }
        ));
    }

    #[tokio::test]
    async fn repoll_after_all_requesters_died_attaches_to_the_running_task() {
        let cache = plain_cache();
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let sem = Arc::new(tokio::sync::Semaphore::new(2));
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let leader = {
            let (cache, locks, key, sem) = (cache.clone(), locks.clone(), key.clone(), sem.clone());
            let (refresh_cache, refresh_key, calls) = (cache.clone(), key.clone(), calls.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, detach_ctx(&sem), move |_| async move {
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let _ = gate_rx.await;
                        Ok(refresh_cache.insert_full(
                            refresh_key,
                            vec![row("", 1, 1.0, 1)],
                            Instant::now(),
                        ))
                    })
                    .await
            })
        };
        wait_until(|| sem.available_permits() != 2).await;

        // The requester dies with no waiter in flight.
        leader.abort();
        assert!(leader.await.unwrap_err().is_cancelled());
        tokio::task::yield_now().await;

        // A later poll attaches; the scan must not run twice.
        let repoll = {
            let (cache, locks, key) = (cache.clone(), locks.clone(), key.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, no_detach, |_| async {
                        panic!("re-poll must attach to the running task, not elect")
                    })
                    .await
            })
        };
        wait_until(|| locks.lease_count(&key) >= 2).await;
        gate_tx.send(()).unwrap();
        let rows = tokio::time::timeout(Duration::from_secs(1), repoll)
            .await
            .unwrap()
            .unwrap()
            .expect("re-poll shares the detached result");
        assert_eq!(rows.len(), 1);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn attacher_after_a_bump_is_gated_off_the_running_snapshot() {
        let cache = plain_cache();
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let sem = Arc::new(tokio::sync::Semaphore::new(2));
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();

        let leader = {
            let (cache, locks, key, sem) = (cache.clone(), locks.clone(), key.clone(), sem.clone());
            let (refresh_cache, refresh_key) = (cache.clone(), key.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, detach_ctx(&sem), move |_| async move {
                        // Stamp fetch START, as production does: the gate
                        // compares bumps against when the read began.
                        let fetch_started = Instant::now();
                        let _ = gate_rx.await;
                        Ok(refresh_cache.insert_full(
                            refresh_key,
                            vec![row("", 1, 1.0, 1)],
                            fetch_started,
                        ))
                    })
                    .await
            })
        };
        wait_until(|| sem.available_permits() != 2).await;
        // The requester dies so the attacher (not the leader) receives the
        // task's outcome directly through the running slot.
        leader.abort();
        assert!(leader.await.unwrap_err().is_cancelled());

        let attacher_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attacher = {
            let (cache, locks, key) = (cache.clone(), locks.clone(), key.clone());
            let (refresh_cache, refresh_key, calls) =
                (cache.clone(), key.clone(), attacher_calls.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, no_detach, move |_| async move {
                        // Gated off the snapshot: the attacher re-elects with
                        // its own loader and serves post-bump rows.
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(refresh_cache.insert_full(
                            refresh_key,
                            vec![row("", 1, 1.0, 1)],
                            Instant::now(),
                        ))
                    })
                    .await
            })
        };
        wait_until(|| locks.lease_count(&key) >= 2).await;
        // The bump lands after the task's fetch started and after the
        // attacher's consult; the snapshot must not be served to it.
        cache.note_bumps(std::iter::once(key.run_id.as_str()));
        gate_tx.send(()).unwrap();
        let rows = tokio::time::timeout(Duration::from_secs(1), attacher)
            .await
            .unwrap()
            .unwrap()
            .expect("gated attacher re-elects and succeeds");
        assert_eq!(rows.len(), 1);
        assert_eq!(attacher_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn detached_failure_surfaces_once_and_later_waiters_reelect() {
        let cache = plain_cache();
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let leader = {
            let (cache, locks, key, sem) = (cache.clone(), locks.clone(), key.clone(), sem.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, detach_ctx(&sem), move |_| async move {
                        let _ = gate_rx.await;
                        Err(anyhow::anyhow!("scan failed").into())
                    })
                    .await
            })
        };
        let waiter = {
            let (cache, locks, key) = (cache.clone(), locks.clone(), key.clone());
            let (refresh_cache, refresh_key) = (cache.clone(), key.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, no_detach, move |_| async move {
                        Ok(refresh_cache.insert_full(
                            refresh_key,
                            vec![row("", 1, 1.0, 1)],
                            Instant::now(),
                        ))
                    })
                    .await
            })
        };
        wait_until(|| locks.lease_count(&key) >= 3).await;
        gate_tx.send(()).unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), leader)
            .await
            .unwrap()
            .unwrap()
            .expect_err("the attached leader receives the failure");
        assert_eq!(error.to_string(), "scan failed");
        let rows = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap()
            .expect("a later waiter re-elects past the failure");
        assert_eq!(rows.len(), 1);
        assert_eq!(sem.available_permits(), 1);
        assert_eq!(locks.registry_len(), 0);
    }

    #[tokio::test]
    async fn task_death_without_a_result_reports_failed_not_timeout() {
        let cache = plain_cache();
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let error = locks
            .get_or_refresh(&cache, &key, detach_ctx(&sem), |_| async {
                panic!("loader died")
            })
            .await
            .expect_err("a dead task must surface as a failure");
        assert!(matches!(error, RefreshError::Died));
        wait_until(|| sem.available_permits() == 1).await;
    }

    #[tokio::test(start_paused = true)]
    async fn detached_refresh_is_bounded_by_the_task_deadline() {
        let cache = plain_cache();
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let error = locks
            .get_or_refresh(&cache, &key, detach_ctx(&sem), |_| {
                std::future::pending::<RefreshOutcome>()
            })
            .await
            .expect_err("a stuck scan must hit the whole-task deadline");
        assert!(matches!(error, RefreshError::Timeout));
        assert_eq!(sem.available_permits(), 1);
    }

    #[tokio::test]
    async fn concurrent_oversized_misses_share_one_loader_result() {
        // Every non-empty result is pass-through rather than retained in
        // the LRU. Singleflight must still share it with current waiters.
        let cache = Arc::new(SeriesCache::with_budget(0));
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let start = Arc::new(tokio::sync::Barrier::new(9));
        let mut tasks = Vec::new();

        for _ in 0..8 {
            let cache = cache.clone();
            let refresh_cache = cache.clone();
            let locks = locks.clone();
            let key = key.clone();
            let refresh_key = key.clone();
            let calls = calls.clone();
            let start = start.clone();
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                locks
                    .get_or_refresh(&cache, &key, no_detach, move |lookup| async move {
                        assert!(matches!(lookup, Lookup::Miss));
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Ok::<_, RefreshError>(refresh_cache.insert_full(
                            refresh_key,
                            vec![row("", 1, 1.0, 100)],
                            Instant::now(),
                        ))
                    })
                    .await
                    .unwrap()
            }));
        }
        start.wait().await;
        let mut results = Vec::new();
        for task in tasks {
            results.push(task.await.unwrap());
        }
        assert!(results.iter().all(|rows| rows.len() == 1));
        assert!(results[1..]
            .iter()
            .all(|rows| Arc::ptr_eq(&results[0], rows)));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(matches!(cache.lookup(&key), Lookup::Miss));
        assert_eq!(locks.registry_len(), 0);
    }

    #[tokio::test]
    async fn bump_during_the_leader_read_elects_a_waiter_to_refresh_again() {
        let cache = Arc::new(SeriesCache::with_budget(usize::MAX));
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (leader_started_tx, leader_started_rx) = tokio::sync::oneshot::channel();
        let (release_leader_tx, release_leader_rx) = tokio::sync::oneshot::channel();

        let leader_cache = cache.clone();
        let leader_refresh_cache = cache.clone();
        let leader_locks = locks.clone();
        let leader_key = key.clone();
        let leader_refresh_key = key.clone();
        let leader_calls = calls.clone();
        let leader = tokio::spawn(async move {
            leader_locks
                .get_or_refresh(
                    &leader_cache,
                    &leader_key,
                    no_detach,
                    move |lookup| async move {
                        assert!(matches!(lookup, Lookup::Miss));
                        leader_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let fetch_started = Instant::now();
                        leader_started_tx.send(()).unwrap();
                        release_leader_rx.await.unwrap();
                        Ok::<_, RefreshError>(leader_refresh_cache.insert_full(
                            leader_refresh_key,
                            vec![row("", 1, 1.0, 100)],
                            fetch_started,
                        ))
                    },
                )
                .await
                .unwrap()
        });
        leader_started_rx.await.unwrap();

        let follower_cache = cache.clone();
        let follower_refresh_cache = cache.clone();
        let follower_locks = locks.clone();
        let follower_key = key.clone();
        let follower_refresh_key = key.clone();
        let follower_calls = calls.clone();
        let (follower_started_tx, follower_started_rx) = tokio::sync::oneshot::channel();
        let follower = tokio::spawn(async move {
            follower_started_tx.send(()).unwrap();
            follower_locks
                .get_or_refresh(
                    &follower_cache,
                    &follower_key,
                    no_detach,
                    move |lookup| async move {
                        follower_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let Lookup::Stale { gen, .. } = lookup else {
                            panic!("the leader's pre-bump snapshot must remain stale");
                        };
                        Ok::<_, RefreshError>(
                            follower_refresh_cache
                                .apply_increment(
                                    &follower_refresh_key,
                                    Vec::new(),
                                    Instant::now(),
                                    gen,
                                )
                                .unwrap(),
                        )
                    },
                )
                .await
                .unwrap()
        });
        follower_started_rx.await.unwrap();
        cache.note_bumps(std::iter::once("r"));
        release_leader_tx.send(()).unwrap();

        assert_eq!(leader.await.unwrap().len(), 1);
        assert_eq!(follower.await.unwrap().len(), 1);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(matches!(cache.lookup(&key), Lookup::Fresh(_)));
    }

    #[tokio::test]
    async fn bump_invalidates_an_oversized_published_result_for_all_waiters() {
        let cache = Arc::new(SeriesCache::with_budget(0));
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (leader_started_tx, leader_started_rx) = tokio::sync::oneshot::channel();
        let (release_leader_tx, release_leader_rx) = tokio::sync::oneshot::channel();

        let leader_cache = cache.clone();
        let leader_refresh_cache = cache.clone();
        let leader_locks = locks.clone();
        let leader_key = key.clone();
        let leader_refresh_key = key.clone();
        let leader_calls = calls.clone();
        let leader = tokio::spawn(async move {
            leader_locks
                .get_or_refresh(
                    &leader_cache,
                    &leader_key,
                    no_detach,
                    move |lookup| async move {
                        assert!(matches!(lookup, Lookup::Miss));
                        leader_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let fetch_started = Instant::now();
                        leader_started_tx.send(()).unwrap();
                        release_leader_rx.await.unwrap();
                        Ok::<_, RefreshError>(leader_refresh_cache.insert_full(
                            leader_refresh_key,
                            vec![row("", 1, 1.0, 100)],
                            fetch_started,
                        ))
                    },
                )
                .await
                .unwrap()
        });
        leader_started_rx.await.unwrap();

        let mut followers = Vec::new();
        for step in 2..=3 {
            let follower_cache = cache.clone();
            let follower_refresh_cache = cache.clone();
            let follower_locks = locks.clone();
            let follower_key = key.clone();
            let follower_refresh_key = key.clone();
            let follower_calls = calls.clone();
            followers.push(tokio::spawn(async move {
                follower_locks
                    .get_or_refresh(
                        &follower_cache,
                        &follower_key,
                        no_detach,
                        move |lookup| async move {
                            assert!(matches!(lookup, Lookup::Miss));
                            follower_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            Ok::<_, RefreshError>(follower_refresh_cache.insert_full(
                                follower_refresh_key,
                                vec![row("", step, step as f32, step * 100)],
                                Instant::now(),
                            ))
                        },
                    )
                    .await
                    .unwrap()
            }));
        }

        // Prove both followers hold this slot before releasing the leader, so
        // both exercise publication rather than starting a later burst.
        wait_until(|| locks.lease_count(&key) >= 3).await;
        cache.note_bumps(std::iter::once("r"));
        release_leader_tx.send(()).unwrap();

        let stale_leader_rows = leader.await.unwrap();
        let first_follower_rows = followers.remove(0).await.unwrap();
        let second_follower_rows = followers.remove(0).await.unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(!Arc::ptr_eq(&stale_leader_rows, &first_follower_rows));
        assert!(Arc::ptr_eq(&first_follower_rows, &second_follower_rows));
        assert!(matches!(cache.lookup(&key), Lookup::Miss));
        assert_eq!(locks.registry_len(), 0);
    }

    #[tokio::test]
    async fn cancellation_and_loader_errors_release_refresh_election() {
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let leader_lease = locks.lease_for(&key);
        let leader_guard = leader_lease.lock().await;
        let (waiter_ready_tx, waiter_ready_rx) = tokio::sync::oneshot::channel();
        let waiter_locks = locks.clone();
        let waiter_key = key.clone();
        let waiter = tokio::spawn(async move {
            let lease = waiter_locks.lease_for(&waiter_key);
            waiter_ready_tx.send(()).unwrap();
            let _guard = lease.lock().await;
        });
        waiter_ready_rx.await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert_eq!(locks.registry_len(), 1);
        drop(leader_guard);
        drop(leader_lease);
        assert_eq!(locks.registry_len(), 0);

        let cache = SeriesCache::with_budget(usize::MAX);
        let error = locks
            .get_or_refresh(&cache, &key, no_detach, |_| async {
                Err::<Arc<SeriesSnapshot>, _>(anyhow::anyhow!("load failed").into())
            })
            .await
            .expect_err("loader error must be returned");
        assert_eq!(error.to_string(), "load failed");
        assert_eq!(locks.registry_len(), 0);

        let (leader_ready_tx, leader_ready_rx) = tokio::sync::oneshot::channel();
        let cancelled_locks = locks.clone();
        let cancelled_key = key.clone();
        let cancelled_leader = tokio::spawn(async move {
            let lease = cancelled_locks.lease_for(&cancelled_key);
            let _guard = lease.lock().await;
            leader_ready_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        leader_ready_rx.await.unwrap();
        cancelled_leader.abort();
        assert!(cancelled_leader.await.unwrap_err().is_cancelled());
        assert_eq!(locks.registry_len(), 0);
    }

    fn series_key(run: &str) -> SeriesKey {
        SeriesKey::new("p", run, "m")
    }

    #[tokio::test]
    async fn batch_election_takes_only_free_misses_up_to_the_limit() {
        let cache = plain_cache();
        let locks = SeriesRefreshLocks::default();
        let keys: Vec<_> = ["fresh", "busy", "a", "b"]
            .into_iter()
            .map(series_key)
            .collect();
        cache.insert_full(keys[0].clone(), vec![row("", 1, 1.0, 1)], Instant::now());
        let busy = locks.lease_for(&keys[1]);
        let _busy_guard = busy.lock().await;

        let elected = locks.elect_misses(&cache, &keys, 1);
        assert_eq!(
            elected.iter().map(|miss| &miss.key).collect::<Vec<_>>(),
            [&keys[2]]
        );
        // An elected slot stays locked until its batch publishes, so a second election skips it.
        let second = locks.elect_misses(&cache, &keys, usize::MAX);
        assert_eq!(
            second.iter().map(|miss| &miss.key).collect::<Vec<_>>(),
            [&keys[3]]
        );
        drop((elected, second));
        assert_eq!(locks.elect_misses(&cache, &keys, usize::MAX).len(), 2);
    }

    #[tokio::test]
    async fn a_batch_reads_once_and_outlives_its_cancelled_requester() {
        use futures::FutureExt;
        let cache = plain_cache();
        let locks = Arc::new(SeriesRefreshLocks::default());
        let keys: Vec<_> = ["a", "b"].into_iter().map(series_key).collect();
        let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let read = {
            let reads = reads.clone();
            async move {
                reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let _ = gate_rx.await;
            }
            .boxed()
            .shared()
        };
        // The batch's units, shared by its tasks.
        let sem = Arc::new(tokio::sync::Semaphore::new(2));
        let units = Arc::new(sem.clone().try_acquire_many_owned(2).unwrap());

        let elected = locks.elect_misses(&cache, &keys, usize::MAX);
        let batch = locks.spawn_batch(elected, units, |index, key| {
            let (read, cache, key) = (read.clone(), cache.clone(), key.clone());
            async move {
                // The first task ends before the read completes, as one timing out would.
                if index == 1 {
                    read.await;
                }
                Ok(cache.insert_full(key, vec![row("", 1, index as f32, 1)], Instant::now()))
            }
        });
        // Cancelled before it is ever polled: the tasks were already spawned.
        drop(batch);

        let attach = |key: SeriesKey| {
            let (cache, locks) = (cache.clone(), locks.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, no_detach, |_| async {
                        unreachable!("waiters must attach to the batch, not elect")
                    })
                    .await
            })
        };
        let first = attach(keys[0].clone()).await.unwrap().unwrap();
        assert_eq!(first[0].value, 0.0);
        wait_until(|| reads.load(std::sync::atomic::Ordering::SeqCst) == 1).await;
        // The finished task released nothing: the read still runs.
        assert_eq!(sem.available_permits(), 0);
        let second = attach(keys[1].clone());
        gate_tx.send(()).unwrap();
        let rows = tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap()
            .expect("the waiter shares the batch's result");
        assert_eq!(rows[0].value, 1.0);
        wait_until(|| sem.available_permits() == 2).await;
        assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(locks.registry_len(), 0);
    }

    #[tokio::test]
    async fn a_re_poll_after_a_cancelled_batch_attaches_instead_of_electing() {
        use futures::FutureExt;
        let cache = plain_cache();
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = series_key("a");
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let read = async move {
            let _ = gate_rx.await;
        }
        .boxed()
        .shared();
        let elected = locks.elect_misses(&cache, std::slice::from_ref(&key), 1);
        drop(locks.spawn_batch(elected, (), |_, key| {
            let (read, cache, key) = (read.clone(), cache.clone(), key.clone());
            async move {
                read.await;
                Ok(cache.insert_full(key, vec![row("", 1, 1.0, 1)], Instant::now()))
            }
        }));

        // The cancelled request's task still runs: a re-poll must attach to it, not start a second read.
        assert!(locks
            .elect_misses(&cache, std::slice::from_ref(&key), 1)
            .is_empty());
        let waiter = {
            let (cache, locks, key) = (cache.clone(), locks.clone(), key.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, no_detach, |_| async {
                        unreachable!("the re-poll must attach to the running batch task")
                    })
                    .await
            })
        };
        gate_tx.send(()).unwrap();
        let rows = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap()
            .expect("the re-poll shares the batch's result");
        assert_eq!(rows[0].value, 1.0);
    }

    #[tokio::test]
    async fn a_batch_task_publishes_the_batch_stamp() {
        let cache = plain_cache();
        let locks = SeriesRefreshLocks::default();
        let key = series_key("a");
        let elected = locks.elect_misses(&cache, std::slice::from_ref(&key), 1);
        // A pass-through result, as under cache ablation, reaches others only through the slot, stamped by the task.
        drop(locks.spawn_batch(elected, (), |_, _| async {
            Ok(Arc::new(SeriesSnapshot::full_with_origin(
                vec![row("", 1, 1.0, 1)],
                LineageOrigin::Miss,
            )))
        }));
        // The single-threaded test runtime hasn't run the task yet: this bump lands after the batch's stamp.
        cache.note_bumps(std::iter::once(key.run_id.as_str()));
        let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = reads.clone();
        locks
            .get_or_refresh(&cache, &key, no_detach, |_| async move {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(Arc::new(SeriesSnapshot::full_with_origin(
                    Vec::new(),
                    LineageOrigin::Miss,
                )))
            })
            .await
            .unwrap();
        // The batch started before the bump, so its result must not satisfy a later consult.
        assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
