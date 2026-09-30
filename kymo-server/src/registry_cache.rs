//! In-memory copy of each run's metric registry (`run_metrics`), serving ListRunSetMetrics.
//!
//! A dashboard lists the metrics of every visible run to lay itself out, on every page load and every visibility toggle.
//! The cache keeps each run's (name, type) list and merges the set in memory.
//!
//! Coherence rests on two facts, both enforced in `PgStore`:
//! - `register_run_metrics` is the only writer, and it brackets every statement with a [`Registration`], which marks its runs when the statement starts and again when it ends, fails or is cancelled.
//! - Rows are deleted only by the cascade from purging a run, and `finalize_purged_runs` brackets its transaction the same way, as a registration that attempts no rows.
//!
//! A fetched list is installed only if, for its run, no mark came after the fetch began, no registration is in flight, and no row attempted for it has an unknown outcome.
//! A statement that errors or is cancelled can still commit.
//! Registration only ever upgrades a row's type, so a later success for the same name at a type at least as high makes that commit a no-op, and resolves it.
//! Until then the run is read from Postgres on every request.
//!
//! Marks last for the life of the process, one per run ever registered or purged.
//! Hand edits to `run_metrics` show after a server restart.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::refresh_locks::{RefreshLease, RefreshLocks};

/// Registry types in collapse order: a name logged as several types lists as the highest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum MetricKind {
    Cdn,
    Numeric,
    TextStream,
}

impl MetricKind {
    pub(crate) fn parse(metric_type: &str) -> Self {
        match metric_type {
            "TEXT_STREAM" => Self::TextStream,
            "NUMERIC" => Self::Numeric,
            _ => Self::Cdn,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Cdn => "CDN",
            Self::Numeric => "NUMERIC",
            Self::TextStream => "TEXT_STREAM",
        }
    }
}

pub(crate) type MetricList = Arc<[(Arc<str>, MetricKind)]>;

/// Registry rows read from Postgres, grouped by run; the runs share one copy of each name.
#[derive(Default)]
pub(crate) struct Fetched {
    names: HashSet<Arc<str>>,
    runs: HashMap<String, Vec<(Arc<str>, MetricKind)>>,
    /// The interned names and their overhead, each counted once.
    name_bytes: usize,
    /// The runs' entries and vectors; vectors grow by doubling, so their capacity, not their length, is what the fill costs.
    list_bytes: usize,
}

impl Fetched {
    pub(crate) fn push(&mut self, run_id: String, metric_name: &str, kind: MetricKind) {
        let name = match self.names.get(metric_name) {
            Some(name) => name.clone(),
            None => {
                let name: Arc<str> = metric_name.into();
                self.name_bytes += name.len() + NAME_OVERHEAD_BYTES;
                self.names.insert(name.clone());
                name
            }
        };
        let list = self.list(run_id);
        let capacity = list.capacity();
        list.push((name, kind));
        let grown = list.capacity() - capacity;
        self.list_bytes += grown * PAIR_BYTES;
    }

    /// A run with no rows still gets a list, so its emptiness is cached too.
    pub(crate) fn include(&mut self, run_id: &str) {
        self.list(run_id.to_string());
    }

    fn list(&mut self, run_id: String) -> &mut Vec<(Arc<str>, MetricKind)> {
        self.runs.entry(run_id).or_insert_with_key(|run_id| {
            self.list_bytes += ENTRY_OVERHEAD_BYTES + run_id.len();
            Vec::new()
        })
    }

    /// What the fill holds; installing charges no more than this.
    pub(crate) fn bytes(&self) -> usize {
        self.list_bytes + self.name_bytes
    }
}

/// A run set's registry, merged: each name once, with its highest type.
#[derive(Default)]
pub(crate) struct Merger(HashMap<String, MetricKind>);

impl Merger {
    pub(crate) fn add(&mut self, name: &str, kind: MetricKind) {
        match self.0.get_mut(name) {
            Some(slot) => *slot = (*slot).max(kind),
            None => {
                self.0.insert(name.to_string(), kind);
            }
        }
    }

    pub(crate) fn add_lists(&mut self, lists: Vec<MetricList>) {
        for (name, kind) in lists.iter().flat_map(|list| list.iter()) {
            self.add(name, *kind);
        }
    }

    /// (name, type) rows in byte order of name; clients sort for display.
    pub(crate) fn into_rows(self) -> Vec<(String, String)> {
        let mut rows: Vec<(String, String)> = self
            .0
            .into_iter()
            .map(|(name, kind)| (name, kind.as_str().to_string()))
            .collect();
        rows.sort_unstable();
        rows
    }
}

pub(crate) struct Lookup {
    pub hits: Vec<MetricList>,
    pub misses: Vec<String>,
    /// Pass to [`RegistryCache::install`] with lists read after this lookup.
    pub since: u64,
}

const BUDGET_BYTES: usize = 64 << 20;
const PAIR_BYTES: usize = std::mem::size_of::<(Arc<str>, MetricKind)>();
/// Map slots, the run id key and the list header.
const ENTRY_OVERHEAD_BYTES: usize = 128;
/// A shared name's allocation header and set slot, beyond its text.
const NAME_OVERHEAD_BYTES: usize = 64;

pub(crate) struct RegistryCache {
    inner: Mutex<Inner>,
    /// What fills in flight hold, across projects: together at most the fill limit (half the budget).
    filling: AtomicUsize,
    /// One fill per project at a time: misses that arrive together (every open tab resyncing after a restart) wait for it, then hit.
    fill_locks: RefreshLocks<String, ()>,
    budget_bytes: usize,
}

#[derive(Default)]
struct Inner {
    entries: HashMap<String, HashMap<String, Entry>>,
    marks: HashMap<String, HashMap<String, Mark>>,
    /// Ticks on every lookup and mark: orders marks against fetches, and entries by last use.
    clock: u64,
    bytes: usize,
    /// Projects whose fill alone outgrew the fill limit.
    /// Registrations only add rows, so only a purge clears it.
    oversized: HashSet<String>,
}

struct Entry {
    list: MetricList,
    /// The list and its key.
    bytes: usize,
    /// The bytes of the names its fill's lists share, charged once until the last of those lists leaves.
    name_bytes: Arc<usize>,
    used: u64,
}

#[derive(Default)]
struct Mark {
    in_flight: u32,
    /// Metric names whose attempt ended without proof of its outcome, with the highest type attempted.
    unresolved: HashMap<String, MetricKind>,
    /// Clock of the run's last mark.
    at: u64,
}

/// A fill's share of the fill limit, released when dropped.
pub(crate) struct FillShare<'a> {
    cache: &'a RegistryCache,
    project_id: &'a str,
    held: usize,
}

impl FillShare<'_> {
    /// Reserve `bytes`, the fill's total so far, within the fill limit.
    /// A refusal leaves the previous reservation unchanged.
    pub(crate) fn hold(&mut self, bytes: usize) -> bool {
        let grown = bytes - self.held;
        let limit = self.cache.budget_bytes / 2;
        let reserved = self
            .cache
            .filling
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |filling| {
                (filling + grown <= limit).then_some(filling + grown)
            })
            .is_ok();
        if reserved {
            self.held = bytes;
            return true;
        }
        metrics::counter!("mkdb2_registry_cache_fill_overflows_total").increment(1);
        // Outgrowing the fill limit alone, not just beside other fills, marks the project too large to cache.
        if bytes > limit {
            self.cache
                .inner
                .lock()
                .unwrap()
                .oversized
                .insert(self.project_id.to_string());
        }
        false
    }
}

impl Drop for FillShare<'_> {
    fn drop(&mut self) {
        self.cache.filling.fetch_sub(self.held, Ordering::Relaxed);
    }
}

/// Each attempted (project, run) with the metric names and types attempted for it.
pub(crate) type Attempted<'a> = HashMap<(&'a str, &'a str), Vec<(&'a str, MetricKind)>>;

/// Brackets one registry statement.
/// Dropped without [`Registration::succeeded`] (an error or a cancellation), its outcome is unknown: the connection can fail after Postgres committed.
pub(crate) struct Registration<'a> {
    cache: &'a RegistryCache,
    runs: Attempted<'a>,
    committed: bool,
}

impl Registration<'_> {
    /// The statement returned Ok.
    pub(crate) fn succeeded(mut self) {
        self.committed = true;
    }
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        self.cache.end_registration(&self.runs, self.committed);
    }
}

impl Inner {
    fn remove(&mut self, project_id: &str, run_id: &str) {
        let Some(runs) = self.entries.get_mut(project_id) else {
            return;
        };
        let Some(entry) = runs.remove(run_id) else {
            return;
        };
        if runs.is_empty() {
            self.entries.remove(project_id);
        }
        self.bytes -= entry.bytes;
        if let Some(name_bytes) = Arc::into_inner(entry.name_bytes) {
            self.bytes -= name_bytes;
        }
    }

    /// Remove a run's list and stamp its mark with a new tick, so no fetch that began earlier installs over it.
    fn mark(&mut self, project_id: &str, run_id: &str) -> &mut Mark {
        self.remove(project_id, run_id);
        self.clock += 1;
        let at = self.clock;
        let mark = self
            .marks
            .entry(project_id.to_string())
            .or_default()
            .entry(run_id.to_string())
            .or_default();
        mark.at = at;
        mark
    }

    fn installable(&self, project_id: &str, run_id: &str, since: u64) -> bool {
        self.marks
            .get(project_id)
            .and_then(|runs| runs.get(run_id))
            .is_none_or(|mark| {
                mark.in_flight == 0 && mark.unresolved.is_empty() && mark.at <= since
            })
    }

    /// Once over budget, evict least recently used lists down to 7/8 of it, so a full cache sorts its entries once per several fills rather than on each.
    fn evict(&mut self, budget_bytes: usize) -> usize {
        if self.bytes <= budget_bytes {
            return 0;
        }
        let target = budget_bytes / 8 * 7;
        let mut order: Vec<(u64, &str, &str, usize)> = self
            .entries
            .iter()
            .flat_map(|(project_id, runs)| {
                runs.iter().map(|(run_id, entry)| {
                    (
                        entry.used,
                        project_id.as_str(),
                        run_id.as_str(),
                        entry.bytes,
                    )
                })
            })
            .collect();
        order.sort_unstable_by_key(|&(used, ..)| used);
        // Lists alone this large bring the cache under target; freeing a fill's names on its last list only brings it lower.
        let mut excess = self.bytes - target;
        let mut victims = Vec::new();
        for (_, project_id, run_id, bytes) in order {
            if excess == 0 {
                break;
            }
            excess = excess.saturating_sub(bytes);
            victims.push((project_id.to_string(), run_id.to_string()));
        }
        let mut evicted = 0;
        for (project_id, run_id) in victims {
            if self.bytes <= target {
                break;
            }
            self.remove(&project_id, &run_id);
            evicted += 1;
        }
        evicted
    }

    fn publish(&self) {
        metrics::gauge!("mkdb2_registry_cache_bytes").set(self.bytes as f64);
        metrics::gauge!("mkdb2_registry_cache_runs")
            .set(self.entries.values().map(HashMap::len).sum::<usize>() as f64);
    }
}

impl RegistryCache {
    pub(crate) fn new() -> Self {
        Self::with_budget(BUDGET_BYTES)
    }

    pub(crate) fn with_budget(budget_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            filling: AtomicUsize::new(0),
            fill_locks: RefreshLocks::default(),
            budget_bytes,
        }
    }

    pub(crate) fn lookup(&self, project_id: &str, run_ids: &[String]) -> Lookup {
        let mut hits = Vec::with_capacity(run_ids.len());
        let mut misses = Vec::new();
        let mut inner = self.inner.lock().unwrap();
        inner.clock += 1;
        let since = inner.clock;
        let mut runs = inner.entries.get_mut(project_id);
        for run_id in run_ids {
            match runs.as_mut().and_then(|runs| runs.get_mut(run_id)) {
                Some(entry) => {
                    entry.used = since;
                    hits.push(entry.list.clone());
                }
                None => misses.push(run_id.clone()),
            }
        }
        Lookup {
            hits,
            misses,
            since,
        }
    }

    pub(crate) fn fill_share<'a>(&'a self, project_id: &'a str) -> FillShare<'a> {
        FillShare {
            cache: self,
            project_id,
            held: 0,
        }
    }

    /// Whether a fill of the project alone outgrew the fill limit, with no run of it purged since.
    pub(crate) fn oversized(&self, project_id: &str) -> bool {
        self.inner.lock().unwrap().oversized.contains(project_id)
    }

    pub(crate) fn fill_lock(&self, project_id: &str) -> RefreshLease<String, ()> {
        self.fill_locks.lease_for(&project_id.to_string())
    }

    pub(crate) fn begin_registration<'a>(&'a self, runs: Attempted<'a>) -> Registration<'a> {
        let mut inner = self.inner.lock().unwrap();
        for (project_id, run_id) in runs.keys() {
            inner.mark(project_id, run_id).in_flight += 1;
        }
        inner.publish();
        Registration {
            cache: self,
            runs,
            committed: false,
        }
    }

    fn end_registration(&self, runs: &Attempted, committed: bool) {
        let mut inner = self.inner.lock().unwrap();
        for ((project_id, run_id), names) in runs {
            let mark = inner.mark(project_id, run_id);
            mark.in_flight -= 1;
            for &(name, kind) in names {
                if !committed {
                    let unknown = mark.unresolved.entry(name.to_string()).or_insert(kind);
                    *unknown = (*unknown).max(kind);
                } else if mark
                    .unresolved
                    .get(name)
                    .is_some_and(|unknown| kind >= *unknown)
                {
                    mark.unresolved.remove(name);
                }
            }
        }
        inner.publish();
    }

    /// Bracket a purge like a registration that attempts no rows.
    /// The purge removes rows, so the project may fit a fill again.
    pub(crate) fn begin_purge<'a>(
        &'a self,
        project_id: &'a str,
        run_ids: &[&'a str],
    ) -> Registration<'a> {
        self.inner.lock().unwrap().oversized.remove(project_id);
        let runs = run_ids
            .iter()
            .map(|&run_id| ((project_id, run_id), Vec::new()));
        self.begin_registration(runs.collect())
    }

    /// Install lists read from a snapshot taken after `since` (a [`Lookup::since`]).
    /// Runs fetched without being `requested` install as least recently used.
    /// Lists already cached stay as they are: every mark removes a list, so a cached one is current.
    pub(crate) fn install(
        &self,
        project_id: &str,
        fetched: Fetched,
        since: u64,
        requested: &HashSet<&str>,
    ) {
        let name_bytes = Arc::new(fetched.name_bytes);
        // Converted before taking the cache mutex.
        let lists: Vec<(String, MetricList)> = fetched
            .runs
            .into_iter()
            .map(|(run_id, list)| (run_id, list.into()))
            .collect();
        let mut inner = self.inner.lock().unwrap();
        let clock = inner.clock;
        let mut skipped = 0;
        for (run_id, list) in lists {
            if inner
                .entries
                .get(project_id)
                .is_some_and(|runs| runs.contains_key(&run_id))
            {
                continue;
            }
            if !inner.installable(project_id, &run_id, since) {
                skipped += 1;
                continue;
            }
            let bytes = ENTRY_OVERHEAD_BYTES + run_id.len() + list.len() * PAIR_BYTES;
            inner.bytes += bytes;
            let used = if requested.contains(run_id.as_str()) {
                clock
            } else {
                0
            };
            let entry = Entry {
                list,
                bytes,
                name_bytes: name_bytes.clone(),
                used,
            };
            inner
                .entries
                .entry(project_id.to_string())
                .or_default()
                .insert(run_id, entry);
        }
        if Arc::strong_count(&name_bytes) > 1 {
            inner.bytes += *name_bytes;
        }
        // The installed lists alone hold the names now, so evicting the last of them frees their bytes.
        drop(name_bytes);
        let evicted = inner.evict(self.budget_bytes);
        inner.publish();
        drop(inner);
        metrics::counter!("mkdb2_registry_cache_skipped_total").increment(skipped);
        metrics::counter!("mkdb2_registry_cache_evictions_total").increment(evicted as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fetched<N: AsRef<str>>(runs: &[(&str, &[(N, MetricKind)])]) -> Fetched {
        let mut out = Fetched::default();
        for (run_id, pairs) in runs {
            out.include(run_id);
            for (name, kind) in *pairs {
                out.push(run_id.to_string(), name.as_ref(), *kind);
            }
        }
        out
    }

    fn ids(run_ids: &[&str]) -> Vec<String> {
        run_ids.iter().map(|run_id| run_id.to_string()).collect()
    }

    fn set<'a>(run_ids: &[&'a str]) -> HashSet<&'a str> {
        run_ids.iter().copied().collect()
    }

    /// Registration rows: each run with the metric names attempted for it, as numeric.
    fn attempt<'a>(runs: &[(&'a str, &[&'a str])]) -> Attempted<'a> {
        runs.iter()
            .map(|&(run_id, names)| {
                let names = names
                    .iter()
                    .map(|&name| (name, MetricKind::Numeric))
                    .collect();
                (("p", run_id), names)
            })
            .collect()
    }

    fn misses(cache: &RegistryCache, run_ids: &[&str]) -> Vec<String> {
        cache.lookup("p", &ids(run_ids)).misses
    }

    /// Look the `requested` runs up, then install `fetched` as their fill.
    fn fill(cache: &RegistryCache, requested: &[&str], fetched: Fetched) {
        let since = cache.lookup("p", &ids(requested)).since;
        cache.install("p", fetched, since, &set(requested));
    }

    #[test]
    fn merged_lists_collapse_types_and_sort_names() {
        let cache = RegistryCache::new();
        fill(
            &cache,
            &["a", "b"],
            fetched(&[
                (
                    "a",
                    &[("loss", MetricKind::Numeric), ("img", MetricKind::Cdn)],
                ),
                (
                    "b",
                    &[("img", MetricKind::TextStream), ("Acc", MetricKind::Cdn)],
                ),
            ]),
        );
        let lookup = cache.lookup("p", &ids(&["a", "b"]));
        assert!(lookup.misses.is_empty());
        let mut merger = Merger::default();
        merger.add_lists(lookup.hits);
        assert_eq!(
            merger.into_rows(),
            vec![
                ("Acc".to_string(), "CDN".to_string()),
                ("img".to_string(), "TEXT_STREAM".to_string()),
                ("loss".to_string(), "NUMERIC".to_string()),
            ]
        );
    }

    #[test]
    fn a_registration_marked_during_the_fetch_blocks_its_install() {
        let cache = RegistryCache::new();
        let since = cache.lookup("p", &ids(&["a"])).since;
        // The snapshot may predate this commit.
        cache
            .begin_registration(attempt(&[("a", &["new"])]))
            .succeeded();
        cache.install(
            "p",
            fetched(&[("a", &[("old", MetricKind::Cdn)])]),
            since,
            &set(&["a"]),
        );
        assert_eq!(misses(&cache, &["a"]), ids(&["a"]));
    }

    #[test]
    fn a_registration_in_flight_blocks_installs_that_began_after_it() {
        let cache = RegistryCache::new();
        let registration = cache.begin_registration(attempt(&[("a", &["new"])]));
        // The fetch starts after the mark but may read before the statement commits.
        fill(
            &cache,
            &["a"],
            fetched(&[("a", &[("old", MetricKind::Cdn)])]),
        );
        assert_eq!(misses(&cache, &["a"]), ids(&["a"]));
        registration.succeeded();
        fill(
            &cache,
            &["a"],
            fetched(&[("a", &[("new", MetricKind::Cdn)])]),
        );
        assert!(misses(&cache, &["a"]).is_empty());
    }

    #[test]
    fn a_later_success_resolves_only_the_rows_it_attempted() {
        let cache = RegistryCache::new();
        let refill = || fill(&cache, &["a"], fetched(&[("a", &[("y", MetricKind::Cdn)])]));
        // A cancelled batch for "x" can still commit after a later batch for the same run succeeds with other rows.
        drop(cache.begin_registration(attempt(&[("a", &["x"])])));
        cache
            .begin_registration(attempt(&[("a", &["y"])]))
            .succeeded();
        refill();
        assert_eq!(misses(&cache, &["a"]), ids(&["a"]));
        cache
            .begin_registration(attempt(&[("a", &["x", "z"])]))
            .succeeded();
        refill();
        assert!(misses(&cache, &["a"]).is_empty());
    }

    #[test]
    fn registration_and_purge_drop_cached_lists_and_leave_other_runs() {
        let cache = RegistryCache::new();
        let run = &[("m", MetricKind::Numeric)][..];
        fill(
            &cache,
            &["a", "b", "c"],
            fetched(&[("a", run), ("b", run), ("c", run)]),
        );
        cache
            .begin_registration(attempt(&[("a", &["m"])]))
            .succeeded();
        drop(cache.begin_purge("p", &["b"]));
        assert_eq!(misses(&cache, &["a", "b", "c"]), ids(&["a", "b"]));
    }

    fn run_pairs(prefix: &str) -> Vec<(String, MetricKind)> {
        (0..100)
            .map(|m| (format!("{prefix}/metric-{m:03}"), MetricKind::Numeric))
            .collect()
    }

    /// What a run installed by [`install_run`] charges.
    fn one_run_bytes() -> usize {
        ENTRY_OVERHEAD_BYTES
            + "a".len()
            + run_pairs("a")
                .iter()
                .map(|(name, _)| PAIR_BYTES + name.len() + NAME_OVERHEAD_BYTES)
                .sum::<usize>()
    }

    fn install_run(cache: &RegistryCache, run_id: &str) {
        fill(cache, &[run_id], fetched(&[(run_id, &run_pairs(run_id))]));
    }

    #[test]
    fn installing_a_fill_charges_no_more_than_it_held() {
        let cache = RegistryCache::new();
        let pairs: Vec<Vec<(String, MetricKind)>> =
            ["a", "b", "c"].iter().map(|run| run_pairs(run)).collect();
        let mut held = fetched(&[("a", &pairs[0]), ("b", &pairs[1]), ("c", &pairs[2])]);
        // Names shared across runs are counted once.
        held.push("c".to_string(), "a/metric-000", MetricKind::Cdn);
        held.include("empty");
        let counted = held.bytes();
        fill(&cache, &["a", "b", "c", "empty"], held);
        let charged = cache.inner.lock().unwrap().bytes;
        assert!(charged <= counted, "{charged} vs {counted}");
    }

    #[test]
    fn fills_in_flight_share_the_fill_limit() {
        let cache = RegistryCache::with_budget(1_000);
        let mut first = cache.fill_share("p");
        let mut second = cache.fill_share("q");
        assert!(first.hold(300));
        assert!(!second.hold(300), "together they would hold 600 of 500");
        assert!(
            !cache.oversized("q"),
            "it outgrew the limit only beside another fill"
        );
        assert!(first.hold(300), "a refused hold reserved nothing");
        assert!(second.hold(200));
        drop((first, second));
        assert_eq!(cache.filling.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn only_a_success_at_a_type_as_high_resolves_an_unknown_outcome() {
        let cache = RegistryCache::new();
        let attempt = |kind| Attempted::from([(("p", "a"), vec![("x", kind)])]);
        let refill = || fill(&cache, &["a"], fetched(&[("a", &[("x", MetricKind::Cdn)])]));
        // A cancelled upgrade can still commit after a lower-typed success, which cannot stop it.
        drop(cache.begin_registration(attempt(MetricKind::TextStream)));
        cache
            .begin_registration(attempt(MetricKind::Cdn))
            .succeeded();
        refill();
        assert_eq!(misses(&cache, &["a"]), ids(&["a"]));
        cache
            .begin_registration(attempt(MetricKind::TextStream))
            .succeeded();
        refill();
        assert!(misses(&cache, &["a"]).is_empty());
    }

    #[test]
    fn a_fills_names_stay_charged_until_its_last_list_leaves() {
        let cache = RegistryCache::new();
        let run = &[("shared", MetricKind::Numeric)][..];
        fill(
            &cache,
            &["a", "b", "c"],
            fetched(&[("a", run), ("b", run), ("c", run)]),
        );
        let names = "shared".len() + NAME_OVERHEAD_BYTES;
        drop(cache.begin_purge("p", &["a", "b"]));
        assert!(cache.inner.lock().unwrap().bytes >= names);
        drop(cache.begin_purge("p", &["c"]));
        let inner = cache.inner.lock().unwrap();
        assert_eq!(inner.bytes, 0);
        assert!(inner.entries.is_empty());
    }

    #[test]
    fn a_fill_evicted_by_its_own_install_leaves_nothing_charged() {
        let cache = RegistryCache::with_budget(one_run_bytes() / 2);
        install_run(&cache, "a");
        assert_eq!(misses(&cache, &["a"]), ids(&["a"]));
        assert_eq!(cache.inner.lock().unwrap().bytes, 0);
    }

    #[test]
    fn a_fill_keeps_the_lists_already_cached_and_their_recency() {
        let cache = RegistryCache::new();
        install_run(&cache, "a");
        // The request hits "a" and misses "c"; its fill reads both.
        let since = cache.lookup("p", &ids(&["a", "c"])).since;
        let (a, c) = (run_pairs("a"), run_pairs("c"));
        cache.install("p", fetched(&[("a", &a), ("c", &c)]), since, &set(&["c"]));
        assert_eq!(cache.inner.lock().unwrap().entries["p"]["a"].used, since);
    }

    #[test]
    fn runs_fetched_without_being_requested_are_evicted_first() {
        let cache = RegistryCache::with_budget(3 * one_run_bytes() - 16);
        let (a, b) = (run_pairs("a"), run_pairs("b"));
        fill(&cache, &["a"], fetched(&[("a", &a), ("b", &b)]));
        install_run(&cache, "c");
        assert_eq!(misses(&cache, &["b"]), ids(&["b"]));
        assert!(misses(&cache, &["c"]).is_empty());
    }

    #[test]
    fn a_fill_that_outgrows_the_limit_alone_marks_its_project_until_a_purge() {
        let cache = RegistryCache::with_budget(1_000);
        assert!(!cache.fill_share("p").hold(600));
        assert!(cache.oversized("p"));
        cache
            .begin_registration(attempt(&[("a", &["m"])]))
            .succeeded();
        assert!(cache.oversized("p"), "registrations only add rows");
        drop(cache.begin_purge("p", &["a"]));
        assert!(!cache.oversized("p"));
    }

    #[test]
    fn eviction_drops_least_recently_used_runs_down_to_seven_eighths() {
        // Three runs overflow it; eviction down to 7/8 of it keeps two.
        let budget = 3 * one_run_bytes() - 16;
        let cache = RegistryCache::with_budget(budget);
        install_run(&cache, "a");
        install_run(&cache, "b");
        // "a" is used again after "b" was installed, so "b" is the oldest when "c" arrives.
        cache.lookup("p", &ids(&["a"]));
        install_run(&cache, "c");
        assert_eq!(misses(&cache, &["a", "b", "c"]), ids(&["b"]));
        assert!(cache.inner.lock().unwrap().bytes <= budget / 8 * 7);
    }
}
