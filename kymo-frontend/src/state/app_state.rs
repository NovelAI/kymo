use std::collections::{BTreeMap, HashSet};

use dioxus::prelude::*;

use crate::grpc::proto::{RunInfo, RunRecord};
use crate::grpc::GrpcClient;
use crate::route::Route;
use crate::state::layout_config::{
    cascade_options, LayoutDiff, LoadResult, RectConfig, RectOptions, SectionConfig,
};
use crate::state::section_order::SectionGap;
use crate::state::LayoutConfig;
use crate::util::warn;

const DISPLAY_RUN_CACHE_CAP: usize = 2_048;

fn merge_display_runs(cache: &mut Vec<RunInfo>, seen: &[RunInfo]) {
    let seen_keys = seen
        .iter()
        .map(|run| (run.project_id.as_str(), run.run_id.as_str()))
        .collect::<HashSet<_>>();
    cache.retain(|cached| {
        !seen_keys.contains(&(cached.project_id.as_str(), cached.run_id.as_str()))
    });
    for run in seen[..seen.len().min(DISPLAY_RUN_CACHE_CAP)].iter().rev() {
        cache.push(run.clone());
    }
    let excess = cache.len().saturating_sub(DISPLAY_RUN_CACHE_CAP);
    if excess > 0 {
        cache.drain(..excess);
    }
}

/// Load the saved diff, treating Missing as an empty diff. On a corrupt
/// store: warn, surface the recovery page, and return None so the caller
/// aborts its operation — overwriting the store from here would destroy
/// every customization (which a newer build might still parse) to salvage
/// one gesture. `replace`, not `push`: the redirect re-fires while the
/// corrupt key persists, so a pushed history entry turns Back into a loop.
pub fn load_diff_or_route(project_id: &str, context: &str) -> Option<LayoutDiff> {
    match LayoutDiff::load_strict(project_id) {
        LoadResult::Loaded(diff) => Some(diff),
        LoadResult::Missing => Some(LayoutDiff::default()),
        LoadResult::Corrupt(msg) => {
            warn(&format!(
                "[layout] corrupt saved diff for {project_id} ({context}): {msg}"
            ));
            navigator().replace(Route::SettingsErrorPage {
                project_id: project_id.to_string(),
            });
            None
        }
    }
}

/// Snapshot driving the full-screen maximize overlay. We keep the full
/// `RectConfig` rather than just its id so the overlay can render even
/// across edits — the layout signal is the source of truth on save.
#[derive(Clone, Debug, PartialEq)]
pub struct MaximizedRect {
    pub config: RectConfig,
    pub max_columns: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectRunView {
    pub record: RunRecord,
    pub server_now_ms: i64,
    pub observed_monotonic_ms: f64,
}

impl DirectRunView {
    pub fn new(record: RunRecord, server_now_ms: i64) -> Self {
        Self {
            record,
            server_now_ms,
            observed_monotonic_ms: crate::state::trash::monotonic_now_ms(),
        }
    }

    pub fn authoritative_now_ms(&self) -> i64 {
        crate::state::trash::extrapolated_now_ms(self.server_now_ms, self.observed_monotonic_ms)
    }

    pub fn matches(&self, project_id: &str, run_id: &str) -> bool {
        self.record
            .run
            .as_ref()
            .is_some_and(|run| run.project_id == project_id && run.run_id == run_id)
    }
}

/// Identity of a saved `Specific` binding: (project_id, run_id).
pub type ExplicitRunKey = (String, String);

/// Settled outcome of one explicit-binding metadata lookup, valid for the
/// project generation it was read at. A MISSING entry means "not yet known"
/// — either never attempted or the attempt failed transiently — and is what
/// makes the fetch pass retryable without a second bookkeeping structure.
#[derive(Clone, Debug, PartialEq)]
pub enum ExplicitRunMetadata {
    Present {
        run: RunInfo,
        generation: u64,
    },
    /// The run does not exist, is permanently unavailable, or the binding
    /// itself is unaskable. Settled until the project generation moves.
    Absent {
        generation: u64,
    },
}

impl ExplicitRunMetadata {
    pub fn generation(&self) -> u64 {
        match self {
            Self::Present { generation, .. } | Self::Absent { generation } => *generation,
        }
    }
}

/// Append `run` unless an earlier — higher-precedence — source already
/// supplied that identity. Every display source funnels through this, so
/// merge order alone decides precedence.
///
/// `seen` carries the identities already taken. Rescanning the output instead
/// would make the merge quadratic in the FIFO's 2,048-row capacity, on a
/// function several consumers call per render.
fn push_unseen(runs: &mut Vec<RunInfo>, seen: &mut HashSet<ExplicitRunKey>, run: &RunInfo) {
    if seen.insert((run.project_id.clone(), run.run_id.clone())) {
        runs.push(run.clone());
    }
}

/// Merge the display-metadata sources in precedence order. Freshness, not
/// convenience, sets the order: the active list is re-read whole on every
/// project-version bump; explicit metadata is a point lookup keyed to that
/// same generation; the direct-run record is a point lookup refreshed on its
/// own route triggers; the FIFO cache is stale by construction — it only
/// remembers rows that have already left a fresher source.
///
/// The FIFO must stay last. It remembers runs as they leave the active list,
/// so ranking it above the direct-run record let a remembered pre-rename row
/// shadow the fresh one on that run's own page.
fn merge_display_sources(
    active: &[RunInfo],
    explicit: impl IntoIterator<Item = RunInfo>,
    direct: Option<&RunInfo>,
    cached: &[RunInfo],
) -> Vec<RunInfo> {
    let mut runs = Vec::with_capacity(active.len() + cached.len());
    let mut seen = HashSet::with_capacity(runs.capacity());
    for run in active {
        push_unseen(&mut runs, &mut seen, run);
    }
    for run in explicit {
        push_unseen(&mut runs, &mut seen, &run);
    }
    if let Some(run) = direct {
        push_unseen(&mut runs, &mut seen, run);
    }
    for run in cached {
        push_unseen(&mut runs, &mut seen, run);
    }
    runs
}

#[derive(Clone, Debug, Default, PartialEq)]
pub enum DirectRunLoad {
    #[default]
    Idle,
    Loading,
    Loaded(DirectRunView),
    NotFound,
    Error(String),
}

#[derive(Clone, Copy)]
pub struct DashboardState {
    pub project_id: Signal<String>,
    pub runs: Signal<Vec<RunInfo>>,
    /// Bounded, display-only metadata remembered INCIDENTALLY: rows observed
    /// leaving the active list or a Trash view. It names saved Specific
    /// panels after a run leaves discovery, but never participates in binding
    /// resolution. Saved `Specific` bindings have their own store below — this
    /// one is best-effort and evicts under pressure.
    pub display_run_cache: Signal<Vec<RunInfo>>,
    /// Display metadata for saved `Specific` bindings whose runs are not in
    /// this project's active list — cross-project bindings and runs sitting in
    /// Trash. Keyed to the binding identity rather than remembered by
    /// observation, so it is bounded by the LAYOUT (pruned as bindings
    /// disappear) and cannot be evicted out from under a panel that still
    /// needs the name. BTreeMap, not HashMap: it feeds `display_runs`, and a
    /// randomized iteration order would make that merge non-deterministic.
    pub explicit_run_metadata: Signal<BTreeMap<ExplicitRunKey, ExplicitRunMetadata>>,
    /// False until the first successful `list_runs` — before that, an empty
    /// `runs` means "still loading", not "this project has no runs".
    pub runs_loaded: Signal<bool>,
    /// Project version from the same database snapshot as the published `runs`. None means the server cannot certify coverage. This is independent of `project_versions`: list coverage says nothing about a concurrent direct GetRun or cross-project metadata lookup.
    pub runs_project_version: CopyValue<Option<u64>>,
    /// Latest connection/event-loss resync. Zero means the first connection is not ready. Run lists, direct lookups, metric discovery and chart caches share this invalidation generation.
    pub resync_gen: Signal<u64>,
    pub selected_runs: Signal<HashSet<String>>, // keyed by run_id (UUID)
    pub current_run: Signal<Option<String>>,    // run_id (UUID)
    /// Point lookup for a direct run route. Deleted records live here, never
    /// in `runs`, so they can label a view-only page without leaking into
    /// project discovery or All-run bindings.
    pub direct_run: Signal<DirectRunLoad>,
    pub direct_run_refresh: Signal<u64>,
    /// Bumped when someone asks for a fresh run list: an uncovered project-version
    /// observation (`push.rs`) or a UI mutation. The list fetch keys on this and
    /// on `resync_gen`.
    pub runs_refresh: Signal<u64>,
    /// Per-run data versions, fed primarily by server push and backed by the
    /// visible-tab minute poll plus reconnect resync. Charts key their refetch
    /// off the versions of the runs THEY render, so one run logging does not
    /// refetch every chart.
    pub run_versions: Signal<std::collections::HashMap<String, u64>>,
    /// Latest observed run-list/metadata version for every project in this
    /// dashboard's binding scope. Cross-project Specific bindings key their
    /// display-metadata lookups on this without treating a rename as chart
    /// data that needs refetching.
    pub project_versions: Signal<std::collections::HashMap<String, u64>>,
    /// Per-run counters of pushed metric-discovery invalidations. Registry
    /// changes and Restore both make affected panels re-list metric types;
    /// data landing on known metrics never touches this.
    pub metrics_gen: Signal<std::collections::HashMap<String, u64>>,
    pub layout_config: Signal<Option<LayoutConfig>>,
    /// The pristine auto-generated layout for the current metric set.
    /// `layout_config` = this + the user's persisted `LayoutDiff`.
    pub base_layout: Signal<Option<LayoutConfig>>,
    pub layout_generation: Signal<u64>,
    pub color_version: Signal<u64>,
    pub maximized: Signal<Option<MaximizedRect>>,
    /// Immutable `name` of the section whose config dialog is open. Lives
    /// here (like `maximized`) so the editor renders OUTSIDE the sorted
    /// section list: live edits re-sort that list (rename is a sort key),
    /// and a dialog mounted inside a list item dies when its item moves.
    pub editing_section: Signal<Option<String>>,
    /// THE x-axis zoom for step-axis charts, as (step_min, step_max) —
    /// the single pathway linking them. Every zoom gesture on any chart
    /// (drag, reset click, axis pan, pinch) bubbles to the document-level
    /// ZoomBridge (uplot_chart.rs) which writes it here, and every
    /// step-axis chart's fetch subscribes to it, so zoom-out follows
    /// to the whole group and panels mounting later (collapsed
    /// sections, other pages, the maximize overlay) come up in the same
    /// window. Time/custom-x charts zoom client-side and ignore it.
    pub step_zoom: Signal<Option<(i64, i64)>>,
    /// Navbar "filter panels" text; empty = no filter. Transient view state like `step_zoom`/`maximized` — never persisted to the layout diff, but survives navigation. The grid hides panels whose label/metric names don't match.
    pub panel_filter: Signal<String>,
    /// Whether a `MetricGrid` is mounted. A run page that can't show its run renders none while `layout_config` keeps the last layout, so the navbar's collapse toggle checks this.
    pub grid_mounted: Signal<bool>,
    /// CopyValue (untracked, never written) rather than a bare GrpcClient,
    /// so the whole state struct is Copy and handlers can capture it
    /// without a clone per closure.
    pub grpc: CopyValue<GrpcClient>,
}

impl DashboardState {
    pub fn new(project_id: String) -> Self {
        Self {
            project_id: Signal::new(project_id),
            runs: Signal::new(Vec::new()),
            display_run_cache: Signal::new(Vec::new()),
            explicit_run_metadata: Signal::new(BTreeMap::new()),
            runs_loaded: Signal::new(false),
            runs_project_version: CopyValue::new(None),
            resync_gen: Signal::new(0),
            selected_runs: Signal::new(HashSet::new()),
            current_run: Signal::new(None),
            direct_run: Signal::new(DirectRunLoad::Idle),
            direct_run_refresh: Signal::new(0),
            runs_refresh: Signal::new(0),
            run_versions: Signal::new(std::collections::HashMap::new()),
            project_versions: Signal::new(std::collections::HashMap::new()),
            metrics_gen: Signal::new(std::collections::HashMap::new()),
            layout_config: Signal::new(None),
            base_layout: Signal::new(None),
            layout_generation: Signal::new(0),
            color_version: Signal::new(0),
            maximized: Signal::new(None),
            editing_section: Signal::new(None),
            step_zoom: Signal::new(None),
            panel_filter: Signal::new(String::new()),
            grid_mounted: Signal::new(false),
            grpc: CopyValue::new(GrpcClient::new()),
        }
    }

    pub fn request_runs_refresh(&self) {
        let mut refresh = self.runs_refresh;
        let next = refresh.peek().wrapping_add(1);
        refresh.set(next);
    }

    pub fn remember_display_runs(&self, runs: &[RunInfo]) {
        let mut next = self.display_run_cache.peek().clone();
        merge_display_runs(&mut next, runs);
        let mut cache = self.display_run_cache;
        if *cache.peek() != next {
            cache.set(next);
        }
    }

    /// Record one settled explicit-binding lookup. Published per completion
    /// rather than per pass: this signal is only ever PEEKED by the fetch
    /// resource, so a write cannot cancel the lookups still in flight
    /// alongside it.
    pub fn record_explicit_metadata(&self, key: ExplicitRunKey, entry: ExplicitRunMetadata) {
        let mut store = self.explicit_run_metadata;
        // Guard first, then mutate in place: `Signal::set` notifies even for an
        // equal value, and publishing per completion makes a clone-per-write
        // quadratic in the plan.
        if store.peek().get(&key) == Some(&entry) {
            return;
        }
        store.write().insert(key, entry);
    }

    /// Drop entries whose binding no longer exists. This is what bounds the
    /// store by layout size instead of by a capacity cap.
    pub fn prune_explicit_metadata(&self, live: &HashSet<ExplicitRunKey>) {
        let mut store = self.explicit_run_metadata;
        if store.peek().keys().all(|key| live.contains(key)) {
            return;
        }
        store.write().retain(|key, _| live.contains(key));
    }

    /// Metadata for display only. Explicit, cached, and direct records may be deleted and must never feed discovery or binding resolution, so callers use this only for presentation: names, ordinals, and colors, plus status and lifecycle timestamps for the `info/run_info` panel's server timing (`add_server_timing`), the gallery's pending wording (`decorate_cdn_series`), and the log panel's tail-follow.
    /// See [`merge_display_sources`] for why the sources rank the way they do.
    pub fn display_runs(&self) -> Vec<RunInfo> {
        let project_id = self.project_id.read();
        let direct = match &*self.direct_run.read() {
            DirectRunLoad::Loaded(view) => view
                .record
                .run
                .as_ref()
                .filter(|run| run.project_id == *project_id)
                .cloned(),
            _ => None,
        };
        merge_display_sources(
            &self.runs.read(),
            self.explicit_run_metadata
                .read()
                .values()
                .filter_map(|entry| match entry {
                    ExplicitRunMetadata::Present { run, .. } => Some(run.clone()),
                    ExplicitRunMetadata::Absent { .. } => None,
                }),
            direct.as_ref(),
            &self.display_run_cache.read(),
        )
    }

    /// The panel filter in the form panels match it against: trimmed, lowercased, empty for no filter.
    pub fn panel_needle(&self) -> String {
        self.panel_filter.read().trim().to_lowercase()
    }

    /// The single write funnel for layout edits: re-read the saved diff,
    /// apply one intent `f` to it, persist, and refresh the displayed layout
    /// as `diff.apply(base)`.
    ///
    /// Re-reading per edit picks up edits other tabs saved before this
    /// gesture instead of overwriting them with this tab's stale picture.
    /// The read-modify-write itself is not atomic — two tabs saving in the
    /// same instant still lose one whole gesture (accepted: localStorage
    /// has no transactions, and a lost gesture is redoable). Displaying the
    /// round-trip means anything the diff can't represent snaps back
    /// immediately instead of silently reverting on the next load. Intents
    /// preserve unrelated settings; ordering additionally protects hidden
    /// sections and anchors while minimizing visible placements.
    /// All reads in here and in the public edit methods are `peek`, never
    /// `read`: these are imperative commands, and `read()` would subscribe
    /// whatever reactive context the caller happens to run in (e.g. the
    /// binding editor's live-apply effect) to the very signal this method
    /// writes. Since `Signal::set` notifies even for equal values, that
    /// turns one edit into an infinite effect → set → effect loop.
    fn record_edit(&self, f: impl FnOnce(&mut LayoutDiff, &LayoutConfig)) {
        self.record_edit_if_changed(|diff, base| {
            f(diff, base);
            true
        });
    }

    fn record_edit_if_changed(&self, f: impl FnOnce(&mut LayoutDiff, &LayoutConfig) -> bool) {
        let project_id = self.project_id.peek().clone();
        let base = self.base_layout.peek().clone();
        let Some(base) = base else {
            // base/layout are set together on load, so this is unreachable
            // today — but an edit silently not persisting must be
            // diagnosable if a future code path shows a layout without one.
            warn(&format!(
                "[layout] no base layout for {project_id}; edit dropped"
            ));
            return;
        };
        // Mid-session corruption (another tab running a different build?):
        // drop this one edit and surface the recovery page.
        let Some(mut diff) = load_diff_or_route(&project_id, "edit dropped") else {
            return;
        };
        if f(&mut diff, &base) {
            diff.save(&project_id);
        }
        let shown = diff.apply(&base);
        let mut layout_signal = self.layout_config;
        layout_signal.set(Some(shown));
    }

    /// Record an edit to one section's SETTINGS; rect edits arrive as
    /// explicit [`Self::update_rect`] intents, never inferred from a section
    /// payload. That's load-bearing: `apply` materializes the defaults
    /// cascade into displayed rect options, so payloads rebuilt from an
    /// open-time snapshot (the section config dialog) carry pre-edit rect
    /// options — inferring rect edits from them pinned every chart in the
    /// section to its old values as rect overrides.
    pub fn update_section_settings(&self, new_section: SectionConfig) {
        self.record_edit(|diff, base| diff.upsert_section_settings(base, &new_section));
    }

    /// Collapse or expand the named sections as one edit (see [`LayoutDiff::set_sections_collapsed`]).
    pub fn set_sections_collapsed(
        &self,
        names: &[String],
        collapsed: bool,
        sections_visible: bool,
    ) {
        self.record_edit_if_changed(|diff, base| {
            diff.set_sections_collapsed(base, names, collapsed, sections_visible)
        });
    }

    pub fn delete_section(&self, name: &str) {
        self.record_edit(|diff, base| diff.delete_section(base, name));
    }

    pub fn add_section(&self, section: SectionConfig) {
        self.record_edit(|diff, base| diff.add_section(base, &section));
    }

    /// One ordering gesture, validated inside the fresh-storage edit funnel.
    pub fn reorder_section(&self, source: String, gap: SectionGap) {
        self.record_edit_if_changed(|diff, base| diff.reorder_section(base, &source, &gap));
    }

    /// Cancellation refreshes stale discovery/storage without writing anything.
    pub fn cancel_section_reorder(&self) {
        self.record_edit_if_changed(|_, _| false);
    }

    /// Record an edit to one rect. Returns false (recording nothing) when
    /// the rect is no longer in the displayed layout — its metric vanished
    /// from a regenerated base — so callers don't pretend the edit applied.
    pub fn update_rect(&self, new_rect: RectConfig) -> bool {
        let exists = self
            .layout_config
            .peek()
            .as_ref()
            .is_some_and(|l| l.find_rect(&new_rect.id).is_some());
        if !exists {
            warn(&format!(
                "[layout] rect {} no longer in layout; edit not saved",
                new_rect.id
            ));
            return false;
        }
        self.record_edit(|diff, base| diff.update_rect(base, &new_rect));
        true
    }

    /// Record a user-added chart in `section`. Like every rect intent, adds
    /// are explicit — nothing infers them from whole-section payloads.
    pub fn add_rect(&self, section: &str, rect: RectConfig) {
        self.record_edit(|diff, base| diff.add_rect(base, section, &rect));
    }

    /// The saved layout diff, or empty if missing/corrupt — read-only helper for the cascade resolutions below and the chart-link local resolve (edits go through `record_edit`, which re-reads storage itself).
    pub fn peek_diff(&self) -> LayoutDiff {
        let project_id = self.project_id.peek().clone();
        match LayoutDiff::load_strict(&project_id) {
            LoadResult::Loaded(diff) => diff,
            _ => LayoutDiff::default(),
        }
    }

    /// Chart options resolved at the PROJECT level: library defaults plus
    /// the saved project patch. The anchor the section-defaults editor
    /// diffs against, and the initial values of the project-defaults form.
    pub fn project_level_options(&self) -> RectOptions {
        let diff = self.peek_diff();
        cascade_options(&RectOptions::default(), &[&diff.project_chart_defaults])
    }

    /// Chart options resolved at the SECTION level: the project level plus
    /// `section`'s defaults patch. Initial values of the section-defaults
    /// form; rects inherit these before their own edits.
    pub fn section_level_options(&self, section: &str) -> RectOptions {
        let diff = self.peek_diff();
        let Some(base) = self.base_layout.peek().clone() else {
            return self.project_level_options();
        };
        diff.inherited_options(&base, section, &RectOptions::default())
    }

    /// Record the project-level chart defaults (the coarsest cascade level).
    pub fn set_project_chart_defaults(&self, opts: RectOptions) {
        self.record_edit(move |diff, _base| diff.set_project_chart_defaults(&opts));
    }

    /// Clear one field of `section`'s chart-defaults patch so it re-inherits from the project level (override chips fire this).
    pub fn clear_section_chart_default(&self, section: &str, field: &str) {
        let section = section.to_string();
        let field = field.to_string();
        self.record_edit(move |diff, base| {
            diff.clear_section_chart_default(base, &section, &field)
        });
    }

    /// Clear one options field of rect `id`'s override patch so it re-inherits from the section/project cascade.
    pub fn clear_rect_option(&self, id: &str, field: &str) {
        let id = id.to_string();
        let field = field.to_string();
        self.record_edit(move |diff, base| diff.clear_rect_option(base, &id, &field));
    }

    /// The options rect `id` would have with no rect-level edits — the
    /// cascade resolution at its section. Anchor for the chart editor's
    /// "this field is overridden here" highlights.
    pub fn inherited_options_for_rect(&self, id: &str) -> RectOptions {
        let diff = self.peek_diff();
        let Some(base) = self.base_layout.peek().clone() else {
            return RectOptions::default();
        };
        diff.inherited_options_for_rect(&base, id)
            .unwrap_or_default()
    }

    pub fn delete_rect(&self, id: &str) {
        self.record_edit(|diff, base| diff.delete_rect(base, id));
    }

    /// Reset the project to pure auto-generation: clear the saved diff and
    /// both layout signals together, and bump the generation so the loader
    /// regenerates.
    pub fn reset_layout(&self) {
        let project_id = self.project_id.peek().clone();
        LayoutDiff::clear(&project_id);
        let mut layout = self.layout_config;
        layout.set(None);
        let mut base = self.base_layout;
        base.set(None);
        let gen = *self.layout_generation.peek();
        let mut generation = self.layout_generation;
        generation.set(gen + 1);
    }
}

#[cfg(test)]
mod display_run_tests {
    use super::*;

    fn run(project_id: &str, run_id: &str, run_name: &str) -> RunInfo {
        RunInfo {
            project_id: project_id.to_string(),
            run_id: run_id.to_string(),
            run_name: run_name.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn display_cache_updates_metadata_without_duplicate_identities() {
        let mut cache = vec![run("p", "r", "old")];
        merge_display_runs(&mut cache, &[run("p", "r", "new")]);

        assert_eq!(cache.len(), 1);
        assert_eq!(cache[0].run_name, "new");
    }

    #[test]
    fn display_cache_is_bounded_and_keeps_newest_observations() {
        let seen = (0..=DISPLAY_RUN_CACHE_CAP)
            .map(|i| run("p", &format!("r{i}"), "run"))
            .collect::<Vec<_>>();
        let mut cache = Vec::new();
        merge_display_runs(&mut cache, &seen);

        assert_eq!(cache.len(), DISPLAY_RUN_CACHE_CAP);
        assert!(cache.iter().any(|run| run.run_id == "r0"));
        assert!(!cache
            .iter()
            .any(|run| run.run_id == format!("r{DISPLAY_RUN_CACHE_CAP}")));
    }
}

/// Fingerprint of `map`'s values for a set of ids (order-insensitive;
/// missing ids count as 0), mixed with `seed` — the one hash behind every
/// "refetch when one of MY runs' counters moved" memo.
pub fn versions_key<'a>(
    seed: u64,
    ids: impl IntoIterator<Item = &'a str>,
    map: &std::collections::HashMap<String, u64>,
) -> u64 {
    let mut ids: Vec<&str> = ids.into_iter().collect();
    ids.sort_unstable();
    ids.dedup();
    let mut h: u64 = seed ^ ids.len() as u64;
    for id in ids {
        for b in id.bytes() {
            h = h.wrapping_mul(31).wrapping_add(b as u64);
        }
        h = h
            .wrapping_mul(0x100000001b3)
            .wrapping_add(map.get(id).copied().unwrap_or(0));
    }
    h
}

/// Floor between push-driven refetches of one panel. Version bumps arrive
/// per ingest flush (~2s per live run) but a warm refresh still costs the
/// server real work (incremental read + smoothing/bucketing recompute), and
/// the pre-push dashboard refreshed at 5s — don't exceed that.
const MIN_REFRESH_MS: f64 = 5_000.0;

fn refresh_floor_wait_ms(now_ms: f64, last_ms: f64) -> Option<u64> {
    let since = now_ms - last_ms;
    (since < MIN_REFRESH_MS).then(|| (MIN_REFRESH_MS - since).max(50.0) as u64)
}

#[cfg(test)]
mod refresh_floor_tests {
    use super::*;

    #[test]
    fn refresh_floor_is_immediate_then_preserves_the_five_second_gap() {
        let now = 100.0;
        assert_eq!(refresh_floor_wait_ms(now, now - MIN_REFRESH_MS), None);
        assert_eq!(refresh_floor_wait_ms(now + 2_000.0, now), Some(3_000));
        assert_eq!(refresh_floor_wait_ms(now + MIN_REFRESH_MS, now), None);
    }
}

/// Cancel-safe fetch keying: `version` reaches the returned signal only
/// while `allowed` holds and no fetch is in flight, catching up after — so
/// a pushed bump can't cancel a slow query, and an offscreen panel's
/// resource doesn't restart per event. Propagations are also floored to
/// MIN_REFRESH_MS apart; a bump landing inside the floor schedules ONE
/// trailing propagation for when the floor expires, because the bump it
/// batches into may never come — a finishing run's FINAL flush is often
/// exactly the one that lands inside the floor, and waiting for "the next
/// bump" would leave the chart missing its last points until reload.
/// Callers subscribe their resource to the RETURNED signal, flip `loading`
/// around the network call, and heal it at body start (a restart can drop
/// the old body with the flag true).
pub fn use_version_bridge(
    version: Memo<u64>,
    loading: Signal<bool>,
    allowed: Memo<bool>,
) -> Signal<u64> {
    let mut data_seq = use_signal(|| *version.peek());
    let mut last_propagated_ms =
        use_signal(|| crate::state::trash::monotonic_now_ms() - MIN_REFRESH_MS);
    // Bumped by the trailing timer purely to re-run the effect; the effect re-reads the real state, so late/duplicate timers are no-ops.
    let mut trail_gen = use_signal(|| 0u64);
    let mut trail_pending = use_signal(|| false);
    use_effect(move || {
        let cur = *version.read();
        let busy = *loading.read();
        let allowed = *allowed.read();
        let _ = *trail_gen.read();
        if allowed && !busy && cur != *data_seq.peek() {
            let now = crate::state::trash::monotonic_now_ms();
            let last = *last_propagated_ms.peek();
            if let Some(wait) = refresh_floor_wait_ms(now, last) {
                if !*trail_pending.peek() {
                    trail_pending.set(true);
                    spawn(async move {
                        gloo_timers::future::sleep(std::time::Duration::from_millis(wait)).await;
                        trail_pending.set(false);
                        let g = *trail_gen.peek();
                        trail_gen.set(g + 1);
                    });
                }
            } else {
                trail_pending.set(false);
                last_propagated_ms.set(now);
                data_seq.set(cur);
            }
        }
    });
    data_seq
}

/// Find a RunInfo by run_id (UUID).
pub fn find_run<'a>(runs: &'a [RunInfo], run_id: &str) -> Option<&'a RunInfo> {
    runs.iter().find(|r| r.run_id == run_id)
}

/// Resolve a run_id to its display name. Falls back to the UUID if unknown
/// (e.g. for runs that haven't been fetched yet).
pub fn run_name_for(runs: &[RunInfo], run_id: &str) -> String {
    find_run(runs, run_id)
        .map(|r| r.run_name.clone())
        .unwrap_or_else(|| run_id.to_string())
}

/// Resolve a run_id to its ordinal (0 if unknown).
pub fn run_ordinal_for(runs: &[RunInfo], run_id: &str) -> u64 {
    find_run(runs, run_id).map(|r| r.ordinal).unwrap_or(0)
}

/// Rewrite a ChartSeries label from the server by replacing its owning run_id with the corresponding run_name.
pub fn rewrite_label_with_run_name(label: &str, series_run_id: &str, runs: &[RunInfo]) -> String {
    let run = runs.iter().find(|run| run.run_id == series_run_id);
    match (run, label.strip_prefix(series_run_id)) {
        (Some(run), Some(rest)) if rest.is_empty() || rest.starts_with('/') => {
            format!("{}{rest}", run.run_name)
        }
        _ => label.to_string(),
    }
}

#[cfg(test)]
mod display_precedence_tests {
    use super::*;

    fn named(run_name: &str) -> RunInfo {
        RunInfo {
            project_id: "p".to_string(),
            run_id: "r".to_string(),
            run_name: run_name.to_string(),
            ordinal: 1,
            created_at_ms: 0,
            status: 0,
            last_ingested_at_ms: None,
            terminated_at_ms: None,
        }
    }

    #[test]
    fn chart_labels_use_the_explicit_run_identity() {
        let mut short = named("Short");
        short.run_id = "team".to_string();
        let mut nested = named("Nested");
        nested.run_id = "team/run".to_string();
        let runs = vec![short, nested];

        assert_eq!(
            rewrite_label_with_run_name("team/run/loss", "team/run", &runs),
            "Nested/loss",
        );
        assert_eq!(
            rewrite_label_with_run_name("team/run", "team/run", &runs),
            "Nested",
        );
        assert_eq!(
            rewrite_label_with_run_name("team", "team/run", &runs),
            "team",
        );
    }

    #[test]
    fn fresher_display_sources_outrank_staler_ones_for_the_same_identity() {
        let active = named("from-list");
        let explicit = named("from-explicit");
        let direct = named("from-direct");
        let cached = named("from-cache");

        let names = |runs: Vec<RunInfo>| {
            runs.into_iter()
                .map(|run| run.run_name)
                .collect::<Vec<String>>()
        };

        assert_eq!(
            names(merge_display_sources(
                std::slice::from_ref(&active),
                [explicit.clone()],
                Some(&direct),
                std::slice::from_ref(&cached),
            )),
            vec!["from-list"]
        );
        assert_eq!(
            names(merge_display_sources(
                &[],
                [explicit.clone()],
                Some(&direct),
                std::slice::from_ref(&cached),
            )),
            vec!["from-explicit"]
        );
        // The regression this ordering fixes: a run remembered into the FIFO
        // as it left the active list must not shadow the fresh point lookup
        // backing its own direct-run page.
        assert_eq!(
            names(merge_display_sources(
                &[],
                [],
                Some(&direct),
                std::slice::from_ref(&cached),
            )),
            vec!["from-direct"]
        );
        assert_eq!(
            names(merge_display_sources(&[], [], None, &[cached])),
            vec!["from-cache"]
        );
    }

    #[test]
    fn distinct_identities_from_every_source_all_survive_the_merge() {
        let mut active = named("active");
        active.run_id = "a".to_string();
        let mut explicit = named("explicit");
        explicit.run_id = "e".to_string();
        let mut direct = named("direct");
        direct.run_id = "d".to_string();
        let mut cached = named("cached");
        cached.run_id = "c".to_string();

        let merged = merge_display_sources(&[active], [explicit], Some(&direct), &[cached]);
        assert_eq!(
            merged
                .iter()
                .map(|run| run.run_id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "e", "d", "c"]
        );
    }
}
