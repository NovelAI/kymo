//! Stage 3b of docs/chart-request-latency.md: held chart state.
//!
//! After answering a continuation poll in full, the server keeps the chart's lean state: the dense model as emitted, its axis plan, each series' running aggregates for its last envelope cell, causal smoother checkpoints every [`chart::CAUSAL_CHECKPOINT_STRIDE`] samples and at the tail, the gap counts proving a median plan, and an index of long runs of non-finite plot values. The next poll whose client holds exactly that model, on the same lineages, advances it by the rows appended since: only their slots, the slots whose shared center moved, the interpolation and smoother dependencies the planner defines are recomputed, from the snapshot's rows with the same functions in the same order; everything else is copied. The response is then assembled exactly as [`super::build_chart`] assembles it, its `from_col` from the planner's own marking rules ([`chart::incremental::planner_from_col`]). Anything the advance does not cover answers with the full build, counted by reason ([`Fallback`]).
//!
//! The state is keyed by the canonical request, holding at most [`ENTRIES_PER_KEY`] entries, each at its refs' lineages and maxima (tabs holding different responses; an entry on a lineage since rotated, which no poll takes, is released by its viewer's next answer); entries are shared, so viewers at the same maxima all advance from one. Within a lineage, rows stamped at or below a maximum are a step prefix of each tag (series_cache.rs), so a tag's rows past the held count are exactly the rows appended since.
//!
//! `KYMO_CHART_HELD_STATE` selects `off` (the default: nothing held), `shadow` (build both, serve the full build, count divergences) or `on` (serve the advance; the sampled 1-in-67 audit builds in full as well and compares, and a mismatch answers in full, drops every entry and turns the advance off until restart). `KYMO_CHART_HELD_STATE_MB` bounds the held bytes (default 128).
//!
//! Metrics: `mkdb2_chart_held_total{outcome}` (`served`, `fallback`, `ineligible` for custom-x charts and first paints with nothing held, `shadow_match`, `shadow_divergence`, `audit_ok`, `audit_failed`, and `audit_skipped` when the full build failed its own checks or could not be held), `mkdb2_chart_held_fallback_total{reason}` ([`Fallback::label`]), `mkdb2_chart_held_uncaptured_total{reason}` (states that cannot be held: a full build's, by [`Fallback::label`], and `budget` for any the store could hold only past its budget, its own containers beside it), `mkdb2_chart_held_evicted_total{cause}` (entries with a viewer on record, evicted by the per-key `cap` or the byte `budget`: that viewer's next poll answers in full; a viewer that closed its tab or changed its request stays on record, so this bounds those polls from above), and the gauges `mkdb2_chart_held_entries`, `mkdb2_chart_held_bytes` and `mkdb2_chart_held_mode{mode}`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use super::budget::{BUILD_BYTES_PER_ROW, DENSE_BYTES_PER_CELL};
use super::lineage::LineageState;
use super::{
    audit_failed_full, inspect_lineages, stamp_lineages, stamp_smoothing_state, BuiltChart,
    ChartParams, DenseShape, PreparedSeries,
};
pub(super) use crate::chart::incremental::Fallback;
use crate::chart::incremental::{self as inc, CellAgg, HeldAxis, Samples as _};
use crate::chart::{self, CausalSmoother, CausalState, SmoothingPlan, CAUSAL_CHECKPOINT_STRIDE};
use crate::chart_delta::DenseChart;
use crate::proto;
use crate::proto::smoothing_config::Algorithm;
use crate::series_cache::{
    buffer_bytes, hash_table_bytes, shrink_table, vec_bytes, CachedRow, SeriesSnapshot, TagRows,
    ARC_HEADER,
};
use tonic::Status;

/// Default bound on held bytes (`KYMO_CHART_HELD_STATE_MB`).
pub(super) const DEFAULT_HELD_BUDGET_MB: usize = 128;
/// Models of one chart held at once, for tabs holding different responses.
const ENTRIES_PER_KEY: usize = 4;
/// Runs of at least this many non-finite plot values are indexed, so a walk to the nearest finite value skips them.
const HOLE_MIN: usize = 64;
/// Causal checkpoints per shared block: an advance copies at most one partial block.
const CHECKPOINT_BLOCK: usize = 64;
/// Integers at most this far from zero are exact in f64; the advance holds no chart whose x leaves that range (beyond it, distinct steps collide as f64 and appends need not keep a chart downsampled).
const EXACT_X: u64 = 1 << 53;
/// What one entry of the store's LRU order costs (a `BTreeMap` slot of tick and key, in nodes at least half full).
const ORDER_ENTRY_BYTES: usize = 96;
/// An irregular Savitzky-Golay advance's cost per sample in the windows of the outputs it recomputes, in units of the full build's per sample in the windows it fits: the advance walks each window from its output through the snapshot, where the full build's sweep moves a window's ends over a slice. Past the full build's cost, it answers in full ([`Fallback::Dense`]).
const ADVANCE_WINDOW_COST: usize = 3;
/// The rest of a full build per fetched row (prepare, the axis, the reply), in the same unit.
const FULL_BUILD_ROW_COST: usize = 10;
/// Shadow divergences remembered as logged ([`HeldStore::first_divergence`]).
const LOGGED_MAX: usize = 1_024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum HeldMode {
    Off,
    Shadow,
    On,
}

impl HeldMode {
    /// Every mode, indexed by its `u8`.
    const ALL: [Self; 3] = [Self::Off, Self::Shadow, Self::On];

    pub(super) fn parse(raw: &str) -> Option<Self> {
        let raw = raw.trim().to_ascii_lowercase();
        Self::ALL.into_iter().find(|mode| mode.label() == raw)
    }

    fn label(self) -> &'static str {
        ["off", "shadow", "on"][self as usize]
    }
}

fn count(outcome: &'static str) {
    metrics::counter!("mkdb2_chart_held_total", "outcome" => outcome).increment(1);
    #[cfg(test)]
    OUTCOMES.with_borrow_mut(|outcomes| *outcomes.entry(outcome).or_default() += 1);
}

#[cfg(test)]
thread_local! {
    /// What `mkdb2_chart_held_total` counted on this thread, for tests.
    static OUTCOMES: std::cell::RefCell<BTreeMap<&'static str, usize>> = const { std::cell::RefCell::new(BTreeMap::new()) };
    /// Make the next advance on this thread panic, for tests.
    pub(super) static PANIC_NEXT_ADVANCE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Make the next capture on this thread panic, for tests.
    pub(super) static PANIC_NEXT_CAPTURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Make a store operation on this thread panic under the lock, after skipping this many, for tests.
    pub(super) static PANIC_STORE_AFTER: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    /// Turn held state off as the next advance on this thread starts, as another poll's audit failure can, for tests.
    pub(super) static DISABLE_AT_NEXT_ADVANCE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Outcomes counted on this thread since the last call.
#[cfg(test)]
pub(super) fn take_outcomes() -> BTreeMap<&'static str, usize> {
    OUTCOMES.take()
}

/// The request without its echoed cache state, hashed ([`super::memo::request_hash`]): what entries are held under, and what stage 2's step-1 hint looks up.
pub(super) type RequestKey = [u8; 32];

#[cfg(test)]
pub(super) fn request_key(req: &proto::ChartRequest) -> RequestKey {
    sha2::Digest::finalize(super::memo::request_hash(req)).into()
}

/// Whether a series' fetched keys, ascending from `first` to `last`, are exact in f64.
fn exact_range(first: i64, last: i64) -> bool {
    first.unsigned_abs() <= EXACT_X && last.unsigned_abs() <= EXACT_X
}

/// One tag of a ref at the held maxima.
#[derive(Clone, PartialEq, Eq)]
struct HeldTag {
    name: Box<str>,
    /// Rows in the tag.
    len: usize,
    /// The tag's rows in the fetch range: [r0, r1).
    r0: usize,
    r1: usize,
    /// Time axes: the latest timestamp among the fetched rows (`i64::MIN` without one).
    max_ts: i64,
}

/// What proves an append leaves a median plan in place: how many of the series' `total` positive gaps lie below its median `m` ([`chart::median_dx`]) and equal it.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct MedianGaps {
    below: usize,
    equal: usize,
    total: usize,
}

impl MedianGaps {
    /// These counts with the gaps between consecutive `keys` added, if `m` is still the median positive gap (1 with none).
    fn extended(mut self, m: f64, keys: impl Iterator<Item = f64>) -> Result<Self, Fallback> {
        let mut previous: Option<f64> = None;
        for key in keys {
            let gap = previous.map_or(0.0, |previous| key - previous);
            if gap > 0.0 {
                self.total += 1;
                self.below += usize::from(gap < m);
                self.equal += usize::from(gap == m);
            }
            previous = Some(key);
        }
        let holds = if self.total == 0 {
            m == 1.0
        } else {
            self.below <= self.total / 2 && self.total / 2 < self.below + self.equal
        };
        holds.then_some(self).ok_or(Fallback::PlanShift)
    }
}

/// A causal smoother's recorded states, without their indices: the k-th before sample k·stride, for every multiple of the stride below the series' count, in shared blocks of [`CHECKPOINT_BLOCK`] that later states reuse; and the tail, after its last sample.
#[derive(Clone, Default)]
struct Checkpoints {
    blocks: Vec<Arc<[CausalState]>>,
    tail: Option<CausalState>,
}

impl Checkpoints {
    /// The recorded state at or before `index`, below the tail, and its index: the stride checkpoint at `index` rounded down.
    fn at_or_before(&self, index: usize) -> (usize, CausalState) {
        let k = index / CAUSAL_CHECKPOINT_STRIDE;
        let state = self.blocks[k / CHECKPOINT_BLOCK][k % CHECKPOINT_BLOCK];
        (k * CAUSAL_CHECKPOINT_STRIDE, state)
    }

    /// These checkpoints, extended by `states`: the stride checkpoints recorded past the tail, then a new tail (none extends nothing), as [`chart::smooth_run_with_plan`] records them; full blocks are shared.
    fn extended(&self, states: &[(usize, CausalState)]) -> Self {
        let Some((&(_, tail), mut fresh)) = states.split_last() else {
            return self.clone();
        };
        let state = |&(_, state): &(usize, CausalState)| state;
        let mut blocks = self.blocks.clone();
        // A partial last block is rebuilt only when a fresh state joins it; else it stays shared.
        if let Some(partial) =
            blocks.pop_if(|last| !fresh.is_empty() && last.len() < CHECKPOINT_BLOCK)
        {
            let head;
            (head, fresh) = fresh.split_at(fresh.len().min(CHECKPOINT_BLOCK - partial.len()));
            blocks.push(
                partial
                    .iter()
                    .copied()
                    .chain(head.iter().map(state))
                    .collect(),
            );
        }
        // Each state's list is cloned from the last, so growth slack buys nothing.
        blocks.reserve_exact(fresh.len().div_ceil(CHECKPOINT_BLOCK));
        blocks.extend(
            fresh
                .chunks(CHECKPOINT_BLOCK)
                .map(|block| block.iter().map(state).collect()),
        );
        Self {
            blocks,
            tail: Some(tail),
        }
    }

    fn entries(&self) -> impl Iterator<Item = &CausalState> {
        self.blocks
            .iter()
            .flat_map(|block| block.iter())
            .chain(&self.tail)
    }

    fn same_as(&self, other: &Self) -> bool {
        self.entries()
            .map(CausalState::bits)
            .eq(other.entries().map(CausalState::bits))
    }

    /// Each block at its allocation (shared blocks are counted by every state holding them), and the block list.
    fn heap_bytes(&self) -> usize {
        vec_bytes(&self.blocks)
            + self
                .blocks
                .iter()
                .map(|block| {
                    buffer_bytes(ARC_HEADER + block.len() * std::mem::size_of::<CausalState>())
                })
                .sum::<usize>()
    }
}

#[derive(Clone)]
struct HeldSeries {
    request_index: usize,
    /// Index into the ref's tags.
    tag: usize,
    /// Emitted samples: fetched indices [emit_from, emit_to).
    emit_from: usize,
    emit_to: usize,
    /// Relative time: the series' first timestamp.
    x_offset: i64,
    plan: SmoothingPlan,
    /// Median plans: the gaps proving the median.
    gaps: MedianGaps,
    /// Irregular Savitzky-Golay: the samples in the windows of all its fetched outputs, which the full build's fits read ([`Fallback::Dense`]); 0 otherwise.
    windows: usize,
    /// Causal smoothers' recorded states.
    checkpoints: Checkpoints,
    open: Option<CellAgg>,
    /// Smoothed charts (a series without finite output too, so walks over it stay short): runs [start, end) of at least [`HOLE_MIN`] emitted samples whose plot values are all non-finite.
    holes: Vec<(usize, usize)>,
    /// Smoothed charts: the first emitted sample with a finite logged value (`flag_lerp_dependency`'s endpoint of last resort; a finite plot value implies one), `Some` exactly when prepare's `has_finite_output` holds.
    first_endpoint: Option<usize>,
}

impl HeldSeries {
    /// Which field differs from another series' state, floats by bits.
    fn difference(&self, other: &Self) -> Option<&'static str> {
        let open = |s: &Self| s.open.map(|agg| agg.bits());
        [
            (
                "identity",
                (self.request_index, self.tag) == (other.request_index, other.tag),
            ),
            (
                "emitted",
                (self.emit_from, self.emit_to) == (other.emit_from, other.emit_to),
            ),
            ("offset", self.x_offset == other.x_offset),
            ("plan", self.plan == other.plan),
            ("gaps", self.gaps == other.gaps),
            ("windows", self.windows == other.windows),
            ("checkpoints", self.checkpoints.same_as(&other.checkpoints)),
            ("open cell", open(self) == open(other)),
            ("holes", self.holes == other.holes),
            ("endpoint", self.first_endpoint == other.first_endpoint),
        ]
        .into_iter()
        .find(|(_, same)| !same)
        .map(|(field, _)| field)
    }
}

/// One chart's held state at its per-ref maxima.
#[derive(Clone)]
pub(super) struct HeldChart {
    /// Each ref's lineage: only a poll reading these takes the entry.
    lineages: Vec<u64>,
    maxima: Vec<Option<i64>>,
    model: DenseChart,
    axis: HeldAxis,
    /// Per ref, the tags its series read, ascending by name; on a relative axis every tag (relative_offsets_stable checks each one).
    refs: Vec<Vec<HeldTag>>,
    series: Vec<HeldSeries>,
    /// Every allocation the state holds at its capacity, its own included ([`Self::measure`]).
    bytes: usize,
    /// The model's share of `bytes`.
    model_bytes: usize,
}

impl HeldChart {
    /// Charge every allocation at its capacity and the allocator's rounding: the model's columns, markers and strings, the axis plan, the tags, each series' checkpoint blocks and hole index, and the state's own `Arc` allocation in the store.
    fn measure(&mut self) {
        let model = &self.model;
        let series_model: usize = model
            .series
            .iter()
            .map(|s| {
                vec_bytes(&s.values)
                    + vec_bytes(&s.raw_values)
                    + vec_bytes(&s.min_values)
                    + vec_bytes(&s.max_values)
                    + vec_bytes(&s.nan_indices)
                    + vec_bytes(&s.nan_kinds)
                    + buffer_bytes(s.label.capacity())
                    + buffer_bytes(s.run_id.capacity())
            })
            .sum();
        let model_bytes = series_model
            + vec_bytes(&model.series)
            + vec_bytes(&model.x_values)
            + vec_bytes(&model.xr_min)
            + vec_bytes(&model.xr_max);
        let refs = vec_bytes(&self.refs)
            + self
                .refs
                .iter()
                .map(|tags| {
                    vec_bytes(tags)
                        + tags
                            .iter()
                            .map(|t| buffer_bytes(t.name.len()))
                            .sum::<usize>()
                })
                .sum::<usize>();
        let series = vec_bytes(&self.series)
            + self
                .series
                .iter()
                .map(|s| s.checkpoints.heap_bytes() + vec_bytes(&s.holes))
                .sum::<usize>();
        self.model_bytes = model_bytes;
        self.bytes = buffer_bytes(ARC_HEADER + std::mem::size_of::<Self>())
            + model_bytes
            + self.axis.heap_bytes()
            + refs
            + series
            + vec_bytes(&self.maxima)
            + vec_bytes(&self.lineages);
    }

    /// Whether the state was built on these snapshots' lineages.
    fn reads(&self, snapshots: &[Arc<SeriesSnapshot>]) -> bool {
        self.lineages
            .iter()
            .copied()
            .eq(snapshots.iter().map(|rows| rows.lineage()))
    }

    #[cfg(test)]
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    /// What a full build of this state's inputs costs, in the unit [`Fallback::Dense`] prices an advance in.
    #[cfg(test)]
    pub(super) fn full_build_cost(&self) -> usize {
        self.series
            .iter()
            .map(|s| {
                let tag = &self.refs[s.request_index][s.tag];
                s.windows + FULL_BUILD_ROW_COST * (tag.r1 - tag.r0)
            })
            .sum()
    }

    /// The per-ref maxima the state was built at.
    #[cfg(test)]
    pub(super) fn maxima(&self) -> &[Option<i64>] {
        &self.maxima
    }

    /// What the state holds beyond its model, which stage 2's charge per dense cell already covers.
    fn state_bytes(&self) -> u64 {
        (self.bytes - self.model_bytes) as u64
    }

    /// What an advance to this state holds beyond its model and working copies: the state's own bytes, and its axis step beside the model (the advanced axis and its slot maps, two cells' worth per column).
    fn beside_model(&self) -> u64 {
        self.state_bytes() + 2 * self.model.x_values.len() as u64 * DENSE_BYTES_PER_CELL
    }

    #[cfg(test)]
    pub(super) fn model(&self) -> &DenseChart {
        &self.model
    }

    /// The axis shape and each series' plan, for the differential test's coverage.
    #[cfg(test)]
    pub(super) fn shape(&self) -> (&'static str, Vec<SmoothingPlan>) {
        let axis = match &self.axis {
            HeldAxis::Passthrough => "passthrough",
            axis if axis.passthrough_shaped() => "shaped",
            _ => "mixed",
        };
        (axis, self.series.iter().map(|s| s.plan).collect())
    }

    /// The longest non-finite plot run any series holds in its hole index, for the differential test's coverage.
    #[cfg(test)]
    pub(super) fn longest_hole(&self) -> usize {
        self.series
            .iter()
            .flat_map(|s| s.holes.iter().map(|&(from, to)| to - from))
            .max()
            .unwrap_or(0)
    }

    /// Where this state differs from another of the same inputs (an advanced state against the one a full build captured), floats by bits.
    pub(super) fn state_difference(&self, other: &Self) -> Option<String> {
        let parts = [
            ("maxima", self.maxima == other.maxima),
            ("model", same_model(&self.model, &other.model)),
            ("axis", self.axis == other.axis),
            ("tags", self.refs == other.refs),
            ("series count", self.series.len() == other.series.len()),
        ];
        if let Some((part, _)) = parts.into_iter().find(|(_, same)| !same) {
            return Some(part.into());
        }
        self.series
            .iter()
            .zip(&other.series)
            .enumerate()
            .find_map(|(i, (a, b))| a.difference(b).map(|field| format!("series {i} {field}")))
    }
}

/// A tag's rows, addressable by index across its chunks.
struct TagView<'a> {
    chunks: Vec<&'a [CachedRow]>,
    ends: Vec<usize>,
}

impl<'a> TagView<'a> {
    fn new(tag: &TagRows<'a>) -> Self {
        let chunks: Vec<&'a [CachedRow]> = tag.chunks().collect();
        let mut end = 0;
        let ends = chunks
            .iter()
            .map(|chunk| {
                end += chunk.len();
                end
            })
            .collect();
        Self { chunks, ends }
    }

    fn len(&self) -> usize {
        self.ends.last().copied().unwrap_or(0)
    }

    fn row(&self, i: usize) -> &'a CachedRow {
        let c = self.ends.partition_point(|&end| end <= i);
        let start = if c == 0 { 0 } else { self.ends[c - 1] };
        &self.chunks[c][i - start]
    }

    /// Rows [lo, hi), walking the chunks in order; an inverted range (`lo > hi`, as a zoomed fetch can ask for) is empty.
    fn rows(&self, lo: usize, hi: usize) -> impl Iterator<Item = &'a CachedRow> + '_ {
        let hi = hi.max(lo);
        let first = self.ends.partition_point(|&end| end <= lo);
        let mut start = if first == 0 { 0 } else { self.ends[first - 1] };
        self.chunks[first..].iter().flat_map(move |chunk| {
            let from = lo.saturating_sub(start).min(chunk.len());
            let to = hi.saturating_sub(start).min(chunk.len());
            start += chunk.len();
            &chunk[from..to]
        })
    }

    /// The tag's rows in the request's fetch range (rows are step-sorted).
    fn fetched(&self, p: &ChartParams) -> (usize, usize) {
        let r0 = inc::partition(0, self.len(), |i| self.row(i).step < p.fetch_min);
        let r1 = inc::partition(r0, self.len(), |i| self.row(i).step <= p.fetch_max);
        (r0, r1)
    }
}

/// How a series' plot values are produced at the current state.
enum Smoother<'a> {
    /// The logged values (unsmoothed, or smoothed without a finite output).
    Raw,
    /// Values below `dense_from` (the held count) are replayed from `recorded`.
    Causal {
        smoother: CausalSmoother,
        recorded: &'a Checkpoints,
    },
    SgUniform {
        window: usize,
        order: u32,
    },
    SgIrregular {
        h_x: f64,
        deg: usize,
    },
}

/// One series' samples at the current state, read from the snapshot's rows, with exact plot values computed on demand and cached.
struct Source<'a> {
    tag: &'a TagView<'a>,
    /// First fetched row in the tag.
    r0: usize,
    /// Fetched samples.
    n: usize,
    /// Emitted samples: fetched [a, a + len).
    a: usize,
    len: usize,
    use_time: bool,
    offset: i64,
    smoother: Smoother<'a>,
    dense_from: usize,
    dense: Vec<f64>,
    sparse: HashMap<usize, f64>,
    /// The held index of long non-finite runs (emitted indices), valid below `changed_from`.
    holes: &'a [(usize, usize)],
    /// The first emitted sample whose plot value may differ from the held one (the held emitted count when none does).
    changed_from: usize,
    rows_read: std::cell::Cell<usize>,
    smoothed: usize,
    replays: usize,
}

impl<'a> Source<'a> {
    /// `n` fetched rows from row `r0` of the tag; the emitted range (`a`, `len`) is set once known.
    fn new(
        tag: &'a TagView<'a>,
        r0: usize,
        n: usize,
        use_time: bool,
        offset: i64,
        holes: &'a [(usize, usize)],
    ) -> Self {
        Self {
            tag,
            r0,
            n,
            a: 0,
            len: 0,
            use_time,
            offset,
            smoother: Smoother::Raw,
            dense_from: n,
            dense: Vec::new(),
            sparse: HashMap::new(),
            holes,
            changed_from: 0,
            rows_read: std::cell::Cell::new(0),
            smoothed: 0,
            replays: 0,
        }
    }

    fn row(&self, i: usize) -> &'a CachedRow {
        self.rows_read.set(self.rows_read.get() + 1);
        self.tag.row(self.r0 + i)
    }

    fn key(&self, i: usize) -> i64 {
        self.key_of(self.row(i))
    }

    /// The x key of one of the series' rows.
    fn key_of(&self, row: &CachedRow) -> i64 {
        if self.use_time {
            row.timestamp_ms - self.offset
        } else {
            row.step
        }
    }

    fn xkey(&self, i: usize) -> f64 {
        self.key(i) as f64
    }

    fn value(&self, i: usize) -> f64 {
        self.row(i).value as f64
    }

    /// The smoothers' input: non-finite values as NaN.
    fn y(&self, i: usize) -> f64 {
        let v = self.value(i);
        if v.is_finite() {
            v
        } else {
            f64::NAN
        }
    }

    /// smooth_run_with_plan's mask: outputs at non-finite inputs are holes (causal smoothers leave them NaN themselves).
    fn masked(&self, i: usize, v: f64) -> f64 {
        if self.value(i).is_finite() {
            v
        } else {
            f64::NAN
        }
    }

    /// Exact plot values for fetched [from, to), computed as the full build computes them.
    fn compute(&mut self, from: usize, to: usize) -> Vec<f64> {
        let n = self.n;
        match self.smoother {
            Smoother::Raw => (from..to).map(|i| self.value(i)).collect(),
            Smoother::Causal { smoother, recorded } => {
                debug_assert!(to <= self.dense_from, "the new tail is computed eagerly");
                let (start, mut state) = recorded.at_or_before(from);
                self.replays += 1;
                let xs: Vec<f64> = (start..to).map(|i| self.xkey(i)).collect();
                let y: Vec<f64> = (start..to).map(|i| self.y(i)).collect();
                self.smoothed += to - start;
                smoother
                    .run(&xs, &y, &mut state, start, None)
                    .split_off(from - start)
            }
            Smoother::SgUniform { window, order } => {
                let (lo, hi) = chart::savgol_inputs(n, window, from, to);
                let yw: Vec<f64> = (lo..hi).map(|i| self.y(i)).collect();
                self.smoothed += to - from;
                let out = chart::savitzky_golay_range(&yw, lo, n, from, to, window, order);
                (from..to).map(|i| self.masked(i, out[i - from])).collect()
            }
            Smoother::SgIrregular { h_x, deg } => {
                self.smoothed += to - from;
                (from..to)
                    .map(|i| {
                        let v =
                            chart::savgol_x_at(&|j| self.xkey(j), &|j| self.y(j), n, i, h_x, deg);
                        self.masked(i, v)
                    })
                    .collect()
            }
        }
    }

    fn plot_at(&mut self, i: usize) -> f64 {
        if i >= self.dense_from && i < self.dense_from + self.dense.len() {
            return self.dense[i - self.dense_from];
        }
        if let Some(&v) = self.sparse.get(&i) {
            return v;
        }
        // A few neighbors at once: lookups cluster (interpolation scans walk outward from a slot).
        let (from, to) = match self.smoother {
            Smoother::Raw => return self.value(i),
            Smoother::Causal { .. } => {
                // Replayed from the checkpoint at or before i, whatever the range.
                let start = i / CAUSAL_CHECKPOINT_STRIDE * CAUSAL_CHECKPOINT_STRIDE;
                (start, (i + 8).min(self.dense_from).max(i + 1))
            }
            Smoother::SgUniform { .. } => (i.saturating_sub(8), (i + 8).min(self.n)),
            Smoother::SgIrregular { .. } => (i.saturating_sub(1), (i + 2).min(self.n)),
        };
        let values = self.compute(from, to);
        for (j, v) in (from..to).zip(values) {
            self.sparse.insert(j, v);
        }
        self.sparse[&i]
    }

    /// The held hole holding emitted sample `e`, if it lies where held plot values still hold.
    fn hole_at(&self, e: usize) -> Option<(usize, usize)> {
        if e >= self.changed_from {
            return None;
        }
        let at = self.holes.partition_point(|&(_, end)| end <= e);
        self.holes
            .get(at)
            .filter(|&&(start, _)| start <= e)
            .map(|&(start, end)| (start, end.min(self.changed_from)))
    }
}

impl inc::Samples for Source<'_> {
    fn len(&self) -> usize {
        self.len
    }
    fn x(&mut self, e: usize) -> f64 {
        self.xkey(self.a + e)
    }
    fn raw(&mut self, e: usize) -> f64 {
        self.value(self.a + e)
    }
    fn plot(&mut self, e: usize) -> f64 {
        self.plot_at(self.a + e)
    }
    fn hole_below(&mut self, e: usize) -> usize {
        e.checked_sub(1)
            .and_then(|below| self.hole_at(below))
            .map_or(e, |(start, _)| start)
    }
    fn hole_from(&mut self, e: usize) -> usize {
        self.hole_at(e).map_or(e, |(_, end)| end)
    }
}

/// The emitted samples of a series' fetched rows: those in the step range and, on a log axis, at a nonnegative x (prepare's `is_output_position`). Fetched x ascend, so they are one range.
fn emitted_range(source: &Source<'_>, p: &ChartParams) -> (usize, usize) {
    let n = source.n;
    let lo = inc::partition(0, n, |i| source.row(i).step < p.step_min);
    let hi = inc::partition(0, n, |i| source.row(i).step <= p.step_max).max(lo);
    let lo = if p.spec.log_buckets {
        lo.max(inc::partition(0, n, |i| source.key(i) < 0)).min(hi)
    } else {
        lo
    };
    (lo, hi)
}

/// Irregular Savitzky-Golay window sizes over a series' ascending keys. `keys` are its keys from sample `from` on, and `lagging` its keys from sample `lo` on, the first sample within `h_x` below `from`. Returns two counts: the samples the windows of `outputs` (from `from` on) hold; and the samples that samples `new..` (`new` at least `from`) add to the windows of all outputs, each one's own window up to itself and itself in each earlier one's. A window holds the samples within `h_x` of its output, itself included, as [`chart::savgol_x_at`] finds them.
fn window_samples(
    keys: impl Iterator<Item = f64>,
    mut lagging: impl Iterator<Item = f64>,
    mut lo: usize,
    from: usize,
    outputs: std::ops::Range<usize>,
    new: usize,
    h_x: f64,
) -> (usize, usize) {
    let (mut windows, mut added) = (0, 0);
    let Some(mut x_lo) = lagging.next() else {
        return (0, 0);
    };
    for (i, x) in (from..).zip(keys) {
        while x - x_lo > h_x {
            lo += 1;
            x_lo = lagging.next().expect("each sample is within h_x of itself");
        }
        // Samples lo..=i in i's window if it is one of `outputs`, and i in the windows of those of them from lo up to i.
        if outputs.contains(&i) {
            windows += i + 1 - lo;
        }
        windows += outputs.end.min(i).saturating_sub(lo.max(outputs.start));
        if i >= new {
            added += 2 * (i - lo) + 1;
        }
    }
    (windows, added)
}

/// Push onto `holes` the runs of at least [`HOLE_MIN`] non-finite plot values over samples `from..to`, the first continuing `run` (the start of a non-finite run reaching `from`).
fn holes_in(
    holes: &mut Vec<(usize, usize)>,
    mut run: Option<usize>,
    from: usize,
    to: usize,
    mut plot: impl FnMut(usize) -> f64,
) {
    for e in from..to {
        if plot(e).is_finite() {
            if let Some(start) = run.take() {
                if e - start >= HOLE_MIN {
                    holes.push((start, e));
                }
            }
        } else {
            run.get_or_insert(e);
        }
    }
    if let Some(start) = run {
        if to - start >= HOLE_MIN {
            holes.push((start, to));
        }
    }
}

/// Each ref's tags at these snapshots that `keep` names (by ref and tag), with views of their rows: row counts and fetched ranges, `max_ts` unset.
fn read_tags<'a>(
    snapshots: &'a [Arc<SeriesSnapshot>],
    p: &ChartParams,
    keep: impl Fn(usize, &str) -> bool,
) -> (Vec<Vec<HeldTag>>, Vec<Vec<TagView<'a>>>) {
    let mut refs = Vec::with_capacity(snapshots.len());
    let mut views = Vec::with_capacity(snapshots.len());
    for (index, rows) in snapshots.iter().enumerate() {
        let mut tags = Vec::new();
        let mut ref_views = Vec::new();
        for tag in rows.tags().filter(|tag| keep(index, tag.tag())) {
            let view = TagView::new(&tag);
            let (r0, r1) = view.fetched(p);
            tags.push(HeldTag {
                name: tag.tag().into(),
                len: view.len(),
                r0,
                r1,
                max_ts: i64::MIN,
            });
            ref_views.push(view);
        }
        refs.push(tags);
        views.push(ref_views);
    }
    (refs, views)
}

/// Capture a full build's held state ([`super::build_chart`]), taking its finished model.
pub(super) fn capture(
    req: &proto::ChartRequest,
    p: &ChartParams,
    snapshots: &[Arc<SeriesSnapshot>],
    prepared: &mut [PreparedSeries],
    model: DenseChart,
    axis: Option<HeldAxis>,
) -> Result<HeldChart, Fallback> {
    #[cfg(test)]
    if PANIC_NEXT_CAPTURE.replace(false) {
        panic!("injected capture panic");
    }
    // Custom-x charts never continue, so the service never captures them (`build` answers them before).
    debug_assert!(req.x_series.is_none());
    // Without a plottable point there is no axis; with one, the model has a column.
    let axis = axis.ok_or(Fallback::Empty)?;
    debug_assert!(!model.x_values.is_empty());
    if axis.passthrough_shaped()
        && prepared
            .iter()
            .any(|ps| ps.xs.windows(2).any(|w| w[0] == w[1]))
    {
        return Err(Fallback::Duplicates);
    }
    // The tags held ([`HeldChart::refs`]): those a series reads, and on a relative axis every tag. `prepare` lists each ref's series in tag order, which the binary search below needs.
    let mut reads: Vec<Vec<&str>> = vec![Vec::new(); snapshots.len()];
    for ps in prepared.iter() {
        reads[ps.identity.request_index].push(&ps.identity.tag);
    }
    debug_assert!(reads.iter().all(|tags| tags.is_sorted()));
    let (mut tags_of, views) = read_tags(snapshots, p, |index, tag| {
        p.relative || reads[index].binary_search(&tag).is_ok()
    });
    // Which tag of which ref each series reads.
    let positions = prepared
        .iter()
        .map(|ps| {
            let request_index = ps.identity.request_index;
            tags_of[request_index]
                .iter()
                .position(|t| *t.name == *ps.identity.tag)
                .map(|tag| (request_index, tag))
                .ok_or(Fallback::Lineage)
        })
        .collect::<Result<Vec<_>, _>>()?;
    // On time axes, one sequential pass over the fetched rows of each held tag: its latest timestamp, and whether its rows ascend in time.
    let mut sorted: Vec<Vec<bool>> = tags_of.iter().map(|tags| vec![true; tags.len()]).collect();
    if p.use_time {
        for (ri, tags) in tags_of.iter_mut().enumerate() {
            for (t, tag) in tags.iter_mut().enumerate() {
                let (mut last, mut ascending) = (i64::MIN, true);
                for row in views[ri][t].rows(tag.r0, tag.r1) {
                    ascending &= row.timestamp_ms >= last;
                    last = last.max(row.timestamp_ms);
                }
                tag.max_ts = last;
                sorted[ri][t] = ascending;
            }
        }
    }
    let mut series = Vec::with_capacity(prepared.len());
    for (ps, (request_index, tag)) in prepared.iter_mut().zip(positions) {
        if !sorted[request_index][tag] {
            return Err(Fallback::TimeOrder);
        }
        let HeldTag { r0, r1, .. } = tags_of[request_index][tag];
        let view = &views[request_index][tag];
        let n = r1 - r0;
        let x_offset = if p.relative && n > 0 {
            view.row(r0).timestamp_ms
        } else {
            0
        };
        let source = Source::new(view, r0, n, p.use_time, x_offset, &[]);
        if n > 0 && !exact_range(source.key(0), source.key(n - 1)) {
            return Err(Fallback::InexactX);
        }
        let (emit_from, emit_to) = emitted_range(&source, p);
        if emit_to - emit_from != ps.xs.len() {
            return Err(Fallback::Lineage);
        }
        let keys = || view.rows(r0, r1).map(|row| source.key_of(row) as f64);
        let plan = ps.smoothing_plan;
        let (gaps, windows) = match plan {
            SmoothingPlan::Median(bits) => {
                let gaps = MedianGaps::default().extended(f64::from_bits(bits), keys())?;
                let windows = if p.algo == Algorithm::SavitzkyGolay {
                    let h_x = chart::savgol_x_half_width(p.window_size as usize, bits);
                    window_samples(keys(), keys(), 0, 0, 0..0, 0, h_x).1
                } else {
                    0
                };
                (gaps, windows)
            }
            _ => (MedianGaps::default(), 0),
        };
        // Causal smoothers' recorded states (prepare records them for a causal series with a finite output), the tail at the fetched count.
        let states = std::mem::take(&mut ps.checkpoints);
        if states.last().is_some_and(|&(index, _)| index != n) {
            return Err(Fallback::Lineage);
        }
        let env_src = if p.is_smoothed { &ps.raw } else { &ps.plot };
        series.push(HeldSeries {
            request_index,
            tag,
            emit_from,
            emit_to,
            x_offset,
            plan,
            gaps,
            windows,
            checkpoints: Checkpoints::default().extended(&states),
            open: axis.open_cell(&ps.xs, env_src, &ps.kinds),
            holes: {
                let mut holes = Vec::new();
                if p.is_smoothed {
                    holes_in(&mut holes, None, 0, ps.plot.len(), |e| ps.plot[e]);
                }
                holes
            },
            first_endpoint: if p.is_smoothed {
                ps.kinds.iter().position(|&kind| kind == 0)
            } else {
                None
            },
        });
    }
    let mut held = HeldChart {
        lineages: snapshots.iter().map(|rows| rows.lineage()).collect(),
        maxima: snapshots.iter().map(|rows| rows.maximum()).collect(),
        model,
        axis,
        refs: tags_of,
        series,
        bytes: 0,
        model_bytes: 0,
    };
    held.measure();
    Ok(held)
}

/// What [`advance`] produced: the answer and the next held state, whose model is the answered one.
pub(super) struct Advanced {
    pub(super) response: proto::ChartResponse,
    pub(super) kind: Option<&'static str>,
    pub(super) next: HeldChart,
    /// What it visited, which the differential test bounds.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) work: Work,
    /// Its working copies' charge: the samples it computed (appended samples and plot values, checkpoint replays included) at the transient rate, and the most one series computed again, as a full build is charged over its rows and largest series, at most that full build's transients.
    transients: u64,
}

impl Advanced {
    /// What the advance held beyond its model: the next state's own bytes, its axis step ([`HeldChart::beside_model`]) and its working copies.
    fn extra_bytes(&self) -> u64 {
        self.next.beside_model() + self.transients
    }
}

/// What an advance visited, for the differential test's work bounds.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Work {
    /// Row reads from the snapshots (binary-search probes included).
    pub(super) rows: usize,
    /// Smoother outputs computed (checkpoint replays included).
    pub(super) smoothed: usize,
    /// What the axis and model steps did ([`inc::RebuildWork`]).
    pub(super) rebuild: inc::RebuildWork,
    /// Causal replays from a held checkpoint.
    pub(super) replays: usize,
    /// Samples in the windows of the irregular Savitzky-Golay outputs it recomputed, as [`Fallback::Dense`] priced them.
    pub(super) windows: usize,
}

/// Per series, what the advance learned before rebuilding.
struct Step {
    /// First emitted sample that is new (held emitted count).
    first_new: usize,
    /// Fetched rows appended since the held state.
    appended: usize,
    gaps: MedianGaps,
    windows: usize,
    fresh_checkpoints: Vec<(usize, CausalState)>,
}

/// [`advance`], with a panic caught rather than unwinding into the build: it answers [`Fallback::Panic`] and turns held state off until restart, in shadow mode as in `on`, since the same chart would panic again on its next poll.
fn advance_guarded(
    store: &HeldStore,
    key: &RequestKey,
    held: &HeldChart,
    req: &proto::ChartRequest,
    p: &ChartParams,
    snapshots: &[Arc<SeriesSnapshot>],
    lineages: &LineageState,
) -> Result<Advanced, Fallback> {
    #[cfg(test)]
    if DISABLE_AT_NEXT_ADVANCE.replace(false) {
        store.disable();
    }
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        advance(held, req, p, snapshots, lineages)
    }))
    .unwrap_or_else(|_| {
        tracing::error!(key = %short(key), "held chart advance panicked; answering in full and turning held state off until restart");
        store.disable();
        Err(Fallback::Panic)
    })
}

/// Advance held state to the current snapshots and answer the poll as the full build would. `held` matched the poll's key and, for a verified continuation, the client's cutoffs; it is left as it was, for other viewers at its maxima.
pub(super) fn advance(
    held: &HeldChart,
    req: &proto::ChartRequest,
    p: &ChartParams,
    snapshots: &[Arc<SeriesSnapshot>],
    lineages: &LineageState,
) -> Result<Advanced, Fallback> {
    #[cfg(test)]
    if PANIC_NEXT_ADVANCE.replace(false) {
        panic!("injected advance panic");
    }
    let mut work = Work::default();
    let verified = lineages.verified();
    // The store hands a verified continuation only the entry at its cutoffs ([`HeldStore::take`]).
    debug_assert!(verified.is_none_or(|verified| verified.cutoffs() == held.maxima));

    // Tags: each held tag's rows past its held count are the appended ones, and on time axes it tracks its latest timestamp. A tag new since the capture backs no series (one that would falls back as `tags` below), so only a relative axis holds it next.
    let (mut refs, views) = read_tags(snapshots, p, |index, tag| {
        p.relative
            || held.refs[index]
                .binary_search_by(|t| (*t.name).cmp(tag))
                .is_ok()
    });
    let mut relative_ok = req.step_min.is_none() && req.step_max.is_none();
    for (index, (tags, ref_views)) in refs.iter_mut().zip(&views).enumerate() {
        let maximum = held.maxima[index];
        // Held tags and the snapshot's both ascend by name, so one walk pairs them.
        let mut held_tags = held.refs[index].iter().peekable();
        for (tag, view) in tags.iter_mut().zip(ref_views) {
            while held_tags.next_if(|t| *t.name < *tag.name).is_some() {}
            let held_tag = held_tags.next_if(|t| *t.name == *tag.name);
            let held_len = held_tag.map_or(0, |t| t.len);
            // Rows at or below the held maximum are exactly the held count, a prefix.
            if tag.len < held_len
                || (held_len > 0 && maximum.is_none_or(|m| view.row(held_len - 1).inserted_ms > m))
                || (tag.len > held_len
                    && maximum.is_some_and(|m| view.row(held_len).inserted_ms <= m))
            {
                return Err(Fallback::Lineage);
            }
            let fresh_from = tag.r0.max(held_len);
            let held_max_ts = held_tag.map_or(i64::MIN, |t| t.max_ts);
            tag.max_ts = held_max_ts;
            if p.use_time {
                for row in view.rows(fresh_from, tag.r1) {
                    // relative_offsets_stable: a new row before the latest held one of its tag moves the offsets (a ref held without rows holds no tags).
                    relative_ok &= row.timestamp_ms >= held_max_ts;
                    tag.max_ts = tag.max_ts.max(row.timestamp_ms);
                }
                work.rows += tag.r1.saturating_sub(fresh_from);
            }
        }
    }

    // The series prepare would form, in its order, must be the held ones: each ref's filter picks them among its tag groups, the tags with rows in the fetch range. A tag the entry holds has its range from the walk above; of any other only whether it has rows there is looked up.
    let mut identities: Vec<(usize, usize)> = Vec::new();
    for (request_index, rows) in snapshots.iter().enumerate() {
        let mut kept = refs[request_index].iter().enumerate().peekable();
        let groups: Vec<(&str, Option<usize>)> = rows
            .tags()
            .filter_map(|tag| {
                let kept = kept.next_if(|(_, t)| *t.name == *tag.tag());
                let in_range = match kept {
                    Some((_, t)) => t.r1 > t.r0,
                    None => tag
                        .between(p.fetch_min, p.fetch_max)
                        .any(|rows| !rows.is_empty()),
                };
                in_range.then(|| (tag.tag(), kept.map(|(t, _)| t)))
            })
            .collect();
        // A ref past 64 tags (which the full build refuses) pairs with no held series below.
        let (picked, _) =
            super::selected_groups(groups.iter().map(|&(tag, _)| tag), p.admits(request_index));
        for i in picked {
            // A group the entry does not hold backs no held series.
            let t = groups[i].1.ok_or(Fallback::Tags)?;
            identities.push((request_index, t));
        }
    }
    if identities.len() != held.series.len()
        || identities.iter().zip(&held.series).any(|(&(ri, t), hs)| {
            ri != hs.request_index || *refs[ri][t].name != *held.refs[ri][hs.tag].name
        })
    {
        return Err(Fallback::Tags);
    }

    // Irregular Savitzky-Golay ([`Fallback::Dense`]): the full build's cost, the samples in the windows it fits (each series' held ones, then those its appended samples add) and its other work per fetched row, against the advance's, the samples in the windows of the outputs it recomputes.
    let mut full_cost: usize = held.series.iter().map(|hs| hs.windows).sum::<usize>()
        + FULL_BUILD_ROW_COST
            * identities
                .iter()
                .map(|&(ri, t)| refs[ri][t].r1 - refs[ri][t].r0)
                .sum::<usize>();
    let mut advance_cost = 0;

    // Per series: the appended rows, their emitted samples, the plan, and the plot values they change.
    let mut sources: Vec<Source<'_>> = Vec::with_capacity(held.series.len());
    let mut steps: Vec<Step> = Vec::with_capacity(held.series.len());
    for (&(ri, t), hs) in identities.iter().zip(&held.series) {
        let tag = &refs[ri][t];
        let held_tag = &held.refs[ri][hs.tag];
        let view = &views[ri][t];
        if tag.r0 != held_tag.r0 {
            return Err(Fallback::Lineage);
        }
        let (held_n, n) = (held_tag.r1 - held_tag.r0, tag.r1 - tag.r0);
        let mut source = Source::new(view, tag.r0, n, p.use_time, hs.x_offset, &hs.holes);
        // Keys ascend (step order, and time order checked below), so the last bounds them all.
        if n > held_n && !exact_range(source.key(held_n), source.key(n - 1)) {
            return Err(Fallback::InexactX);
        }
        if p.use_time && n > held_n {
            // Held fetched rows ascend in time (captured so), the latest last; appended ones must continue them.
            let mut last = held_tag.max_ts;
            for i in held_n..n {
                let ts = source.row(i).timestamp_ms;
                if ts < last {
                    return Err(Fallback::TimeOrder);
                }
                last = ts;
            }
        }
        // Emitted samples: appended rows extend the held range only when it reached the held fetched end.
        let (emit_from, emit_to) = if n == held_n {
            (hs.emit_from, hs.emit_to)
        } else {
            let (lo, hi) = emitted_range(&source, p);
            if hs.emit_to < held_n || hs.emit_from == hs.emit_to {
                if hi - lo != hs.emit_to - hs.emit_from {
                    return Err(Fallback::Membership);
                }
            } else if lo != hs.emit_from || hi < hs.emit_to {
                return Err(Fallback::Lineage);
            }
            (lo, hi)
        };
        // A new in-range sample with an unplottable x (log axes) would add a kind-4 marker.
        if p.spec.log_buckets && n > held_n {
            for i in held_n..n {
                let step = source.row(i).step;
                if step >= p.step_min && step <= p.step_max && source.key(i) < 0 {
                    return Err(Fallback::Unplottable);
                }
            }
        }
        source.a = emit_from;
        source.len = emit_to - emit_from;
        // The held emitted count: new samples follow the held ones.
        let first_new = hs.emit_to - hs.emit_from;

        // The plan, held while the appended gaps keep it; on smoothed charts a first finite logged value changes it.
        let finite_output = hs.first_endpoint.is_some();
        if p.is_smoothed
            && !finite_output
            && (emit_from + first_new..emit_to).any(|i| source.value(i).is_finite())
        {
            return Err(Fallback::PlanShift);
        }
        let mut gaps = hs.gaps;
        let mut windows = hs.windows;
        if finite_output && n > held_n {
            match hs.plan {
                SmoothingPlan::NoState => {}
                SmoothingPlan::Uniform => {
                    // is_uniform's first gap, between held samples (a finite output means one is held).
                    let d0 = source.xkey(1) - source.xkey(0);
                    let gaps = (held_n.max(1)..n).map(|i| source.xkey(i) - source.xkey(i - 1));
                    if !chart::same_gaps(d0, gaps) {
                        return Err(Fallback::PlanShift);
                    }
                }
                SmoothingPlan::Median(bits) => {
                    let keys = (held_n.saturating_sub(1)..n).map(|i| source.xkey(i));
                    gaps = gaps.extended(f64::from_bits(bits), keys)?;
                }
            }
        }

        // Plot values that change, from `reach_from` on, computed now; the rest on demand.
        let mut reach_from = held_n;
        let mut fresh_checkpoints = Vec::new();
        if finite_output {
            source.smoother = match (p.algo, hs.plan) {
                (Algorithm::Ema | Algorithm::Triangular, plan) => Smoother::Causal {
                    smoother: chart::causal_smoother(p.algo, p.time_constant, p.poly_order, plan),
                    recorded: &hs.checkpoints,
                },
                (Algorithm::SavitzkyGolay, SmoothingPlan::Uniform) => Smoother::SgUniform {
                    window: p.window_size as usize,
                    order: p.poly_order,
                },
                (Algorithm::SavitzkyGolay, SmoothingPlan::Median(bits)) => Smoother::SgIrregular {
                    h_x: chart::savgol_x_half_width(p.window_size as usize, bits),
                    deg: p.poly_order.min(2) as usize,
                },
                _ => return Err(Fallback::PlanShift),
            };
            if n > held_n {
                // prepare's flag_reach (OLD within the centered reach of the first NEW) and the dependency suffix (causal: from the first NEW; uniform SG: from its block origin).
                if p.reach > 0 {
                    reach_from = held_n.saturating_sub(p.reach);
                    if let SmoothingPlan::Median(bits) = hs.plan {
                        let span = f64::from_bits(bits) * p.reach as f64;
                        let first = source.xkey(held_n);
                        // Keys ascend to `first`, so the samples within its span are a suffix: searched for, however densely they sit.
                        reach_from =
                            inc::partition(0, reach_from, |i| first - source.xkey(i) > span);
                    }
                }
                match source.smoother {
                    Smoother::Causal { smoother, recorded } => {
                        // Captured with every causal series' finite output, at its fetched count; a broken invariant panics into the advance's guard (answered in full, held state off).
                        let mut state = recorded.tail.expect("a causal series' tail state");
                        let xs: Vec<f64> = (held_n..n).map(|i| source.xkey(i)).collect();
                        let y: Vec<f64> = (held_n..n).map(|i| source.y(i)).collect();
                        source.smoothed += n - held_n;
                        source.dense =
                            smoother.run(&xs, &y, &mut state, held_n, Some(&mut fresh_checkpoints));
                        source.dense_from = held_n;
                    }
                    Smoother::SgUniform { .. } | Smoother::SgIrregular { .. } => {
                        // Uniform: also from the first prefix-sum block the new sample shifts.
                        if let Smoother::SgUniform { window, .. } = source.smoother {
                            reach_from =
                                reach_from.min(chart::savgol_dependency_start(window, held_n));
                        }
                        // Only emitted outputs are read: those from `reach_from` on are computed up to the emitted end, not over a zoomed chart's fetch margins (each output's bits do not depend on the range computed).
                        let start = reach_from.max(emit_from);
                        let outputs = start..emit_to.max(start);
                        // Irregular: answers in full once it would cost more than the full build ([`Fallback::Dense`]).
                        if let Smoother::SgIrregular { h_x, .. } = source.smoother {
                            let x0 = source.xkey(reach_from);
                            let lo = inc::partition(0, reach_from, |i| x0 - source.xkey(i) > h_x);
                            let keys = |from| (from..n).map(|i| source.xkey(i));
                            let (recomputed, added) = window_samples(
                                keys(reach_from),
                                keys(lo),
                                lo,
                                reach_from,
                                outputs.clone(),
                                held_n,
                                h_x,
                            );
                            windows += added;
                            work.windows += recomputed;
                            full_cost += added;
                            advance_cost += ADVANCE_WINDOW_COST * recomputed;
                            if advance_cost > full_cost {
                                return Err(Fallback::Dense);
                            }
                        }
                        source.dense = source.compute(outputs.start, outputs.end);
                        source.dense_from = outputs.start;
                    }
                    Smoother::Raw => {}
                }
            }
        }
        // Plot values may differ from the held ones from `reach_from` on: held samples there are the planner's REACH, then the new ones follow.
        source.changed_from = reach_from.clamp(emit_from, emit_to) - emit_from;
        debug_assert!(source.changed_from <= first_new);
        sources.push(source);
        steps.push(Step {
            first_new,
            appended: n.saturating_sub(held_n),
            gaps,
            windows,
            fresh_checkpoints,
        });
    }

    // The axis, advanced by every series' new emitted x.
    let new_xs: Vec<Vec<f64>> = sources
        .iter_mut()
        .zip(&steps)
        .map(|(source, step)| (step.first_new..source.len).map(|e| source.x(e)).collect())
        .collect();
    if held.axis.passthrough_shaped() {
        for (source, (step, xs)) in sources.iter_mut().zip(steps.iter().zip(&new_xs)) {
            let previous = (step.first_new > 0).then(|| source.x(step.first_new - 1));
            if previous.zip(xs.first()).is_some_and(|(a, &b)| a == b)
                || xs.windows(2).any(|w| w[0] == w[1])
            {
                return Err(Fallback::Duplicates);
            }
        }
    }
    let new_refs: Vec<&[f64]> = new_xs.iter().map(Vec::as_slice).collect();
    let axis_step = inc::advance_axis(&held.axis, &held.model.x_values, &new_refs, p.spec)?;

    // The model. Interpolation dependencies: envelope slots past the last finite point before the first change re-interpolate. Below the first change, plot values are the held ones, so the held hole index applies there.
    let (model, open) = {
        let mut series_steps: Vec<inc::SeriesStep<'_>> = Vec::with_capacity(sources.len());
        for ((source, step), hs) in sources.iter_mut().zip(&steps).zip(&held.series) {
            let changed_from = source.changed_from;
            let mut lerp_after = f64::INFINITY;
            if p.is_smoothed && changed_from < source.len {
                let finite = inc::finite_below(source, changed_from);
                lerp_after = if finite.len() == 2 {
                    source.x(finite[0].0)
                } else {
                    f64::NEG_INFINITY
                };
            }
            series_steps.push(inc::SeriesStep {
                first_new: step.first_new,
                changed_from,
                lerp_after,
                open: hs.open,
                samples: source,
            });
        }
        inc::rebuild(
            &held.model,
            &axis_step,
            &mut series_steps,
            p.is_smoothed,
            &mut work.rebuild,
        )
    };

    let plans = || held.series.iter().map(|hs| hs.plan);
    let mut kind = lineages.rejection().map(super::lineage::Rejection::label);
    let deltable = verified.is_some()
        && (!p.relative || relative_ok)
        && super::continuing_series_match(req, p, plans(), held.series.len());
    let from_col = deltable.then(|| planner_from_col(held, &axis_step, &mut sources, &steps, p));
    // Every held series continues: no series appeared, and a series without held emitted samples gained none.
    let continues = std::iter::repeat_n(true, held.series.len());
    let mut response = super::reply(&model, req, from_col, continues, &mut kind);
    stamp_lineages(&mut response.frontiers, lineages);
    stamp_smoothing_state(&mut response.frontiers, p, plans());

    // The next state's hole index: held below the first change, rescanned above it (over the samples the poll recomputed). Its endpoint is the held one: emitted samples keep their logged values, and a first finite one falls back (`plan_shift`).
    let mut series = Vec::with_capacity(held.series.len());
    for ((((hs, step), source), open), &(_, t)) in held
        .series
        .iter()
        .zip(&steps)
        .zip(sources.iter_mut())
        .zip(open)
        .zip(&identities)
    {
        let holes = if p.is_smoothed {
            let below = source.changed_from;
            let len = source.len;
            let mut holes: Vec<(usize, usize)> = hs
                .holes
                .iter()
                .filter(|&&(_, end)| end < below)
                .copied()
                .collect();
            // A hole reaching the first change runs on while the rescanned values stay non-finite; so does a shorter run of held values below it (under [`HOLE_MIN`] long, or the index would hold it), which only rescanned values can make a hole: without any, it is not looked for.
            let run = hs
                .holes
                .iter()
                .find(|&&(start, end)| start < below && end >= below)
                .map(|&(start, _)| start)
                .or_else(|| {
                    if below >= len {
                        return None;
                    }
                    let mut start = below;
                    while start > 0 && !source.plot(start - 1).is_finite() {
                        start -= 1;
                    }
                    (start < below).then_some(start)
                });
            holes_in(&mut holes, run, below, len, |e| source.plot(e));
            holes
        } else {
            Vec::new()
        };
        series.push(HeldSeries {
            request_index: hs.request_index,
            tag: t,
            emit_from: source.a,
            emit_to: source.a + source.len,
            x_offset: hs.x_offset,
            plan: hs.plan,
            gaps: step.gaps,
            windows: step.windows,
            checkpoints: hs.checkpoints.extended(&step.fresh_checkpoints),
            open,
            holes,
            first_endpoint: hs.first_endpoint,
        });
    }

    let (mut computed, mut most, mut fetched, mut longest) = (0, 0, 0, 0);
    for (source, step) in sources.iter().zip(&steps) {
        work.rows += source.rows_read.get();
        work.smoothed += source.smoothed;
        work.replays += source.replays;
        let samples = step.appended + source.smoothed;
        (computed, most) = (computed + samples, most.max(samples));
        (fetched, longest) = (fetched + source.n, longest.max(source.n));
    }
    let transients =
        ((computed + most) as u64).min((fetched + longest) as u64) * BUILD_BYTES_PER_ROW;

    let mut next = HeldChart {
        lineages: held.lineages.clone(),
        maxima: snapshots.iter().map(|rows| rows.maximum()).collect(),
        model,
        axis: axis_step.axis,
        refs,
        series,
        bytes: 0,
        model_bytes: 0,
    };
    next.measure();
    Ok(Advanced {
        response,
        kind,
        next,
        work,
        transients,
    })
}

/// [`super::plan_delta_from_col`] on the samples it can mark: each continuing series' REACH (prepare's centered reach and dependency suffix, then flag_lerp_dependency's one interpolation endpoint) and NEW samples.
fn planner_from_col(
    held: &HeldChart,
    axis_step: &inc::AxisStep,
    sources: &mut [Source<'_>],
    steps: &[Step],
    p: &ChartParams,
) -> usize {
    let mut xs: Vec<Vec<f64>> = Vec::with_capacity(sources.len());
    let mut ages: Vec<Vec<u8>> = Vec::with_capacity(sources.len());
    let mut continuing = Vec::with_capacity(sources.len());
    for ((source, step), hs) in sources.iter_mut().zip(steps).zip(&held.series) {
        // numeric_delta_from_col's continuing: some held emitted sample.
        continuing.push(step.first_new > 0);
        let hot = source.changed_from..source.len;
        let mut samples = Vec::with_capacity(hot.len() + 1);
        if p.is_smoothed {
            // flag_lerp_dependency: the first changing sample, then the nearest preceding finite endpoint, or, with fewer than two, the first possible one (held below the first change, since every hot sample before `changing` is not one).
            if let Some(changing) = hot.clone().find(|&e| source.raw(e).is_finite()) {
                let finite = inc::finite_below(source, changing);
                let dependency = if finite.len() == 2 {
                    Some(finite[0].0)
                } else {
                    hs.first_endpoint
                };
                samples.extend(dependency.filter(|&d| d < hot.start));
            }
        }
        samples.extend(hot);
        // Ages: REACH from the first change (the endpoint too), NEW from the first new sample.
        xs.push(samples.iter().map(|&e| source.x(e)).collect());
        ages.push(
            samples
                .iter()
                .map(|&e| {
                    if e >= step.first_new {
                        chart::AGE_NEW
                    } else {
                        chart::AGE_REACH
                    }
                })
                .collect(),
        );
    }
    let xs_r: Vec<&[f64]> = xs.iter().map(Vec::as_slice).collect();
    let ages_r: Vec<&[u8]> = ages.iter().map(Vec::as_slice).collect();
    inc::planner_from_col(
        &held.axis,
        &held.model.x_values,
        axis_step,
        &xs_r,
        &ages_r,
        &continuing,
    )
}

/// Held state of every chart, LRU-bounded by bytes.
pub(super) struct HeldStore {
    /// [`HeldMode`] as its `u8`; an audit failure sets it to off.
    mode: AtomicU8,
    budget: usize,
    inner: Mutex<StoreInner>,
}

/// One held entry.
struct Held {
    /// Its place in the LRU order.
    tick: u64,
    /// Answers stored at its maxima, less the polls since answered from them: the viewers that may still continue from it.
    viewers: usize,
    chart: Arc<HeldChart>,
}

#[derive(Default)]
struct StoreInner {
    map: HashMap<RequestKey, Vec<Held>>,
    /// Least recently used first.
    order: BTreeMap<u64, RequestKey>,
    next_tick: u64,
    /// Every entry's [`HeldChart::bytes`].
    entries: usize,
    /// `entries` and the store's own containers ([`Self::overhead`]).
    bytes: usize,
    /// The largest capacity `map` has reported since it last shrank ([`hash_table_bytes`]).
    map_capacity: usize,
    /// Shadow divergences already logged, as hashes of their key and part ([`HeldStore::first_divergence`]).
    logged: HashSet<u64>,
}

impl StoreInner {
    fn recount(&mut self) {
        self.map_capacity = self.map_capacity.max(self.map.capacity());
        self.bytes = self.entries + self.overhead(self.map.len(), self.order.len(), 0);
    }

    /// What the store counts beside its entries' own bytes with `keys` keys and `entries` entries, its table holding `table` keys or its charged capacity if more: the per-key entry lists (each allocated once, at [`ENTRIES_PER_KEY`]), the table ([`hash_table_bytes`], as hashbrown grows a full one), the LRU order and the logged divergences.
    fn overhead(&self, keys: usize, entries: usize, table: usize) -> usize {
        keys * buffer_bytes(ENTRIES_PER_KEY * std::mem::size_of::<Held>())
            + hash_table_bytes(
                self.map_capacity.max(table),
                std::mem::size_of::<(RequestKey, Vec<Held>)>(),
            )
            + entries * ORDER_ENTRY_BYTES
            + hash_table_bytes(self.logged.capacity(), std::mem::size_of::<u64>())
    }

    /// Remove the entry; its viewers.
    fn remove_at(&mut self, key: &RequestKey, at: usize) -> usize {
        let list = self.map.get_mut(key).unwrap();
        let Held {
            tick,
            chart,
            viewers,
        } = list.swap_remove(at);
        if list.is_empty() {
            self.map.remove(key);
        }
        self.order.remove(&tick);
        self.entries -= chart.bytes;
        viewers
    }

    /// Evict the entry, counting it if a viewer is on record (it may still continue from it, unless it closed its tab or changed its request).
    fn evict(&mut self, key: &RequestKey, at: usize, cause: &'static str) {
        if self.remove_at(key, at) > 0 {
            metrics::counter!("mkdb2_chart_held_evicted_total", "cause" => cause).increment(1);
        }
    }

    fn publish(&mut self) {
        self.recount();
        metrics::gauge!("mkdb2_chart_held_entries").set(self.order.len() as f64);
        metrics::gauge!("mkdb2_chart_held_bytes").set(self.bytes as f64);
    }
}

impl HeldStore {
    pub(super) fn new(mode: HeldMode, budget: usize) -> Self {
        let store = Self {
            mode: AtomicU8::new(0),
            budget,
            inner: Mutex::default(),
        };
        store.set_mode(mode);
        store
    }

    /// The store's state. A panic while it was locked leaves that state unknown: recover the lock, hold nothing and advance nothing until restart, as an audit failure does.
    fn lock(&self) -> std::sync::MutexGuard<'_, StoreInner> {
        self.inner.lock().unwrap_or_else(|poisoned| {
            let mut inner = poisoned.into_inner();
            self.inner.clear_poison();
            tracing::error!(
                "held chart store poisoned by a panic; turning held state off until restart"
            );
            self.set_mode(HeldMode::Off);
            *inner = StoreInner::default();
            inner.publish();
            inner
        })
    }

    /// `op` on the store under its lock. A panic in it answers `fallback` rather than failing the poll, which off mode would have answered: recovering the lock it poisoned turns held state off until restart ([`Self::lock`]).
    fn guarded<T>(&self, fallback: T, op: impl FnOnce(&mut StoreInner) -> T) -> T {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut inner = self.lock();
            #[cfg(test)]
            if PANIC_STORE_AFTER.replace(PANIC_STORE_AFTER.get().and_then(|n| n.checked_sub(1)))
                == Some(0)
            {
                panic!("injected panic in the held chart store");
            }
            op(&mut inner)
        }))
        .unwrap_or_else(|_| {
            drop(self.lock());
            fallback
        })
    }

    /// Whether this is the first shadow divergence of this chart in this part (since the store forgot, at [`LOGGED_MAX`] of them): a diverging chart keeps its full build's state and diverges again on its next poll, so only the first is logged, and every one counted.
    pub(super) fn first_divergence(&self, key: &RequestKey, part: &str) -> bool {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        (key, part).hash(&mut hasher);
        let mut inner = self.lock();
        if inner.logged.len() >= LOGGED_MAX {
            inner.logged.clear();
        }
        let first = inner.logged.insert(hasher.finish());
        inner.publish();
        first
    }

    fn set_mode(&self, mode: HeldMode) {
        self.mode.store(mode as u8, Ordering::Relaxed);
        for candidate in HeldMode::ALL {
            metrics::gauge!("mkdb2_chart_held_mode", "mode" => candidate.label())
                .set(if candidate == mode { 1.0 } else { 0.0 });
        }
    }

    pub(super) fn mode(&self) -> HeldMode {
        HeldMode::ALL[usize::from(self.mode.load(Ordering::Relaxed))]
    }

    /// Stage 2's step-1 hint for an unaudited `on`-mode continuation poll: whether it may be advanced (some entry of the request is held), so it need not reserve a full build's transients up front; if so, what its advance holds beside its working copies, which step 1 reserves instead: the largest entry's model at the output's rate per cell, and beside it ([`HeldChart::beside_model`]).
    pub(super) fn hints(&self, request: &RequestKey) -> Option<u64> {
        self.guarded(None, |inner| {
            inner
                .map
                .get(request)?
                .iter()
                .map(|held| {
                    let model = &held.chart.model;
                    let cells = model.series.len() * model.x_values.len();
                    cells as u64 * DENSE_BYTES_PER_CELL + held.chart.beside_model()
                })
                .max()
        })
    }

    /// Whether any entry of this request is held: only then can an advance run beside a full build of it (in `shadow` mode, or audited in `on` mode), whose second model stage 2's step 1 then reserves.
    pub(super) fn holds(&self, request: &RequestKey) -> bool {
        self.lock().map.contains_key(request)
    }

    /// The entry on these snapshots' lineages the client holds (its verified cutoffs), shared with any other viewer; or, for a poll without them, the newest such entry its snapshots extend. Marks it recently used. Also whether any entry of `key` is held on these lineages.
    pub(super) fn take(
        &self,
        key: &RequestKey,
        snapshots: &[Arc<SeriesSnapshot>],
        cutoffs: Option<&[Option<i64>]>,
    ) -> (Result<Arc<HeldChart>, Fallback>, bool) {
        self.guarded((Err(Fallback::Panic), false), |inner| {
            let Some(list) = inner
                .map
                .get(key)
                .filter(|list| list.iter().any(|held| held.chart.reads(snapshots)))
            else {
                return (Err(Fallback::Absent), false);
            };
            let at = match cutoffs {
                Some(cutoffs) => list
                    .iter()
                    .position(|held| held.chart.maxima == cutoffs && held.chart.reads(snapshots))
                    .ok_or(Fallback::Cutoff),
                None => list
                    .iter()
                    .enumerate()
                    .filter(|(_, held)| {
                        held.chart.reads(snapshots)
                            && held
                                .chart
                                .maxima
                                .iter()
                                .zip(snapshots)
                                .all(|(held, rows)| *held <= rows.maximum())
                    })
                    .max_by_key(|(_, held)| held.tick)
                    .map(|(at, _)| at)
                    .ok_or(Fallback::Absent),
            };
            let at = match at {
                Ok(at) => at,
                Err(reason) => return (Err(reason), true),
            };
            let tick = inner.next_tick;
            inner.next_tick += 1;
            let list = inner.map.get_mut(key).unwrap();
            let old = std::mem::replace(&mut list[at].tick, tick);
            let chart = Arc::clone(&list[at].chart);
            inner.order.remove(&old);
            inner.order.insert(tick, *key);
            (Ok(chart), true)
        })
    }

    /// Hold `chart`, unless the store holding it alone would exceed the budget: then it is never held, and counted (`budget` in `mkdb2_chart_held_uncaptured_total`), since its chart answers in full on every poll, paying its capture and its stage-2 charge each time.
    pub(super) fn insert(&self, key: RequestKey, chart: HeldChart) {
        self.guarded((), |inner| self.insert_locked(inner, key, chart));
    }

    fn insert_locked(&self, inner: &mut StoreInner, key: RequestKey, chart: HeldChart) {
        if self.mode() == HeldMode::Off {
            return;
        }
        // Alone, it would be counted beside its list, its LRU slot, the table (grown by the insert if its key is new) and the logged divergences: past the budget, the eviction loop below would evict every other entry and then it.
        let table = inner.map.len() + usize::from(!inner.map.contains_key(&key));
        if chart.bytes + inner.overhead(1, 1, table) > self.budget {
            metrics::counter!("mkdb2_chart_held_uncaptured_total", "reason" => "budget")
                .increment(1);
            return;
        }
        // An answer at lineages and maxima already held replaces that entry and adds its viewer.
        let mut viewers = 1;
        if let Some(at) = inner.map.get(&key).and_then(|list| {
            list.iter().position(|held| {
                held.chart.maxima == chart.maxima && held.chart.lineages == chart.lineages
            })
        }) {
            viewers += inner.remove_at(&key, at);
        }
        if let Some(at) = inner
            .map
            .get(&key)
            .filter(|list| list.len() >= ENTRIES_PER_KEY)
            .and_then(|list| list.iter().enumerate().min_by_key(|(_, held)| held.tick))
            .map(|(at, _)| at)
        {
            inner.evict(&key, at, "cap");
        }
        let tick = inner.next_tick;
        inner.next_tick += 1;
        inner.entries += chart.bytes;
        inner.order.insert(tick, key);
        // Never more than ENTRIES_PER_KEY (the eviction above), so the list never grows past its one allocation.
        inner
            .map
            .entry(key)
            .or_insert_with(|| Vec::with_capacity(ENTRIES_PER_KEY))
            .push(Held {
                tick,
                viewers,
                chart: Arc::new(chart),
            });
        inner.recount();
        while inner.bytes > self.budget {
            let Some((&tick, &oldest)) = inner.order.iter().next() else {
                break;
            };
            let at = inner.map[&oldest]
                .iter()
                .position(|held| held.tick == tick)
                .unwrap();
            inner.evict(&oldest, at, "budget");
            inner.recount();
        }
        shrink_table(&mut inner.map, &mut inner.map_capacity);
        inner.publish();
    }

    /// A poll whose client held the entry at `maxima`, on these snapshots' lineages (or, not `on_lineages`, on the lineages they rotated from), was answered: that viewer moved on, and an entry no viewer still holds is dropped (one viewer keeps one entry).
    fn release(
        &self,
        key: &RequestKey,
        snapshots: &[Arc<SeriesSnapshot>],
        maxima: &[Option<i64>],
        on_lineages: bool,
    ) {
        self.guarded((), |inner| {
            let Some(list) = inner.map.get_mut(key) else {
                return;
            };
            let Some(at) = list.iter().position(|held| {
                held.chart.maxima == maxima && held.chart.reads(snapshots) == on_lineages
            }) else {
                return;
            };
            let held = &mut list[at];
            held.viewers = held.viewers.saturating_sub(1);
            if held.viewers == 0 {
                inner.remove_at(key, at);
                shrink_table(&mut inner.map, &mut inner.map_capacity);
                inner.publish();
            }
        });
    }

    /// A memo hit answered a poll whose client held the entry at `from` with a reply at these snapshots' maxima (stage 1; see `BuiltChart::moved_from`): that viewer moves to the entry there, if one is held, as an answer stored there would add it, and off the one at `from`.
    pub(super) fn moved(
        &self,
        key: &RequestKey,
        snapshots: &[Arc<SeriesSnapshot>],
        from: &[Option<i64>],
    ) {
        self.guarded((), |inner| {
            if let Some(held) = inner.map.get_mut(key).and_then(|list| {
                list.iter_mut().find(|held| {
                    held.chart.reads(snapshots)
                        && held
                            .chart
                            .maxima
                            .iter()
                            .copied()
                            .eq(snapshots.iter().map(|rows| rows.maximum()))
                })
            }) {
                held.viewers += 1;
            }
        });
        self.release(key, snapshots, from, true);
    }

    /// An audit found the advance wrong: hold nothing and advance nothing until restart.
    fn disable(&self) {
        let mut inner = self.lock();
        self.set_mode(HeldMode::Off);
        *inner = StoreInner::default();
        inner.publish();
    }

    /// Corrupt every held model, for the audit-failure test.
    #[cfg(test)]
    pub(super) fn tamper_for_test(&self) {
        let mut inner = self.lock();
        for list in inner.map.values_mut() {
            for held in list {
                let chart = Arc::get_mut(&mut held.chart).expect("no advance in flight");
                for s in &mut chart.model.series {
                    for v in s.values.iter_mut().filter(|v| v.is_finite()) {
                        *v += 1.0;
                    }
                }
            }
        }
    }

    /// The map's charged capacity and what `capacity()` reports now.
    #[cfg(test)]
    pub(super) fn map_capacity_for_test(&self) -> (usize, usize) {
        let inner = self.lock();
        (inner.map_capacity, inner.map.capacity())
    }

    #[cfg(test)]
    pub(super) fn len_and_bytes(&self) -> (usize, usize) {
        let inner = self.lock();
        (inner.order.len(), inner.bytes)
    }
}

/// Whole-model bit equality, labels and counts included.
pub(super) fn same_model(a: &DenseChart, b: &DenseChart) -> bool {
    let bits = |a: &[f64], b: &[f64]| {
        a.iter()
            .map(|x| x.to_bits())
            .eq(b.iter().map(|x| x.to_bits()))
    };
    bits(&a.x_values, &b.x_values)
        && bits(&a.xr_min, &b.xr_min)
        && bits(&a.xr_max, &b.xr_max)
        && a.series.len() == b.series.len()
        && a.series.iter().zip(&b.series).all(|(s, t)| {
            s.label == t.label
                && s.run_id == t.run_id
                && bits(&s.values, &t.values)
                && bits(&s.raw_values, &t.raw_values)
                && bits(&s.min_values, &t.min_values)
                && bits(&s.max_values, &t.max_values)
                && s.nan_indices == t.nan_indices
                && s.nan_kinds == t.nan_kinds
                && s.xnan_count == t.xnan_count
        })
}

/// What [`build`] does with an unaudited `on`-mode poll.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Advance {
    /// Advance the client's entry, else build in full.
    Try,
    /// Advance it, else answer `None` without building: the poll reserved no transients for a full build (stage 2's step-1 hint), so its caller charges them outside the build slot (or starts over with them) and calls again with `Skip`.
    Only,
    /// Build in full: an `Only` call fell back and counted why.
    Skip,
}

/// The service's build step with held state. An unaudited poll in `on` mode answers by advancing the client's entry (as `attempt` says), else by a full build; every other poll builds in full: with held state off, in `shadow` mode, and on every audited poll, every memo-checked one included. It keeps the state of every continuation poll's answer, and of a fresh tab's on a chart already held. It answers `None` only for [`Advance::Only`]. `p` holds the request's parameters as its poll planned them.
#[allow(clippy::too_many_arguments)]
pub(super) fn build(
    store: &HeldStore,
    req: &proto::ChartRequest,
    p: &ChartParams,
    request: Option<RequestKey>,
    snapshots: &[Arc<SeriesSnapshot>],
    x_maps: Option<&HashMap<String, HashMap<i64, f64>>>,
    audit: bool,
    memo: Option<&proto::ChartResponse>,
    on_dense: &dyn Fn(DenseShape),
    attempt: Advance,
) -> Result<Option<BuiltChart>, Status> {
    let mode = store.mode();
    // Nothing advances here: held state turned off since the poll was planned (or the poll is audited).
    if attempt == Advance::Only && (mode != HeldMode::On || audit) {
        return Ok(None);
    }
    // No key: a custom-x chart, or held state was off when the poll was planned (so it is off now).
    let Some(key) = request.filter(|_| mode != HeldMode::Off) else {
        if mode != HeldMode::Off {
            count("ineligible");
        }
        return super::build_chart(req, p, snapshots, x_maps, audit, memo, on_dense, false)
            .map(Some);
    };
    let frontiers = super::echoed_frontiers(req);
    let continuation = frontiers.is_some();
    let lineages = inspect_lineages(req, snapshots, frontiers);
    let cutoffs = lineages.verified().map(|verified| verified.cutoffs());
    let (entry, held_here) = store.take(&key, snapshots, cutoffs);
    // The entry this poll's client holds, taken by its cutoffs: an answer moves that viewer on, from it to whatever the poll stores next, an `audit_failed` answer too (the client replaces its model with it); a failed build leaves it in place. A client whose lineage rotated held the entry at its echoed (unverified) maxima on the old lineages, which no poll takes: its answer moves it off that one.
    let taken = cutoffs.filter(|_| entry.is_ok());
    let rotated = lineages.rotated_cutoffs();
    let answered = || {
        if let Some(cutoffs) = taken {
            store.release(&key, snapshots, cutoffs, true);
        } else if let Some(cutoffs) = rotated {
            store.release(&key, snapshots, cutoffs, false);
        }
    };
    // A chart is live when its client continues it or another tab's state is held: only then is its full answer worth holding, so only then captured.
    let live = continuation || held_here;
    let keep = |held: Option<Result<HeldChart, Fallback>>| match held {
        Some(Ok(held)) => store.insert(key, held),
        Some(Err(reason)) => {
            metrics::counter!("mkdb2_chart_held_uncaptured_total", "reason" => reason.label())
                .increment(1);
        }
        None => {}
    };
    let miss = |reason: Fallback| {
        if reason == Fallback::Absent && !continuation {
            count("ineligible");
        } else {
            count("fallback");
            metrics::counter!("mkdb2_chart_held_fallback_total", "reason" => reason.label())
                .increment(1);
        }
    };
    // The full build's model shape, charged with an advance's model beside it when one follows (shadow mode, or an audited poll in `on` mode, that took an entry), before that model exists. Step 1 reserved it only if the store held an entry of the request when the poll was planned; one held since is charged here, a correction that never waits.
    let models = 1 + u64::from(entry.is_ok() && (mode != HeldMode::On || audit));
    let reported = std::cell::Cell::new(None);
    let record = |shape: DenseShape| {
        let shape = DenseShape { models, ..shape };
        reported.set(Some(shape));
        on_dense(shape);
    };
    // A captured state, or an advance's state and working copies, adds to what the build holds.
    let charge_held = |extra: u64| {
        if let Some(shape) = reported.get() {
            on_dense(DenseShape { extra, ..shape });
        }
    };
    // A panic while capturing (in the capture or the build's capture-only work) answers with the build without capture and turns held state off until restart, as a panicking advance does; a panic that build repeats is the build's own and unwinds as with held state off.
    let full = || {
        let build =
            |capture| super::build_chart(req, p, snapshots, x_maps, audit, memo, &record, capture);
        let mut built = if live {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| build(true))) {
                Ok(built) => built?,
                Err(_) => {
                    let mut built = build(false)?;
                    tracing::error!(key = %short(&key), "held chart capture panicked; answering in full and turning held state off until restart");
                    store.disable();
                    built.held = Some(Err(Fallback::Panic));
                    built
                }
            }
        } else {
            build(false)?
        };
        if let Some(Ok(captured)) = &built.held {
            charge_held(captured.state_bytes());
        }
        if built.kind == Some("unchanged") {
            built.moved_from = cutoffs
                .filter(|cutoffs| {
                    !cutoffs
                        .iter()
                        .copied()
                        .eq(snapshots.iter().map(|rows| rows.maximum()))
                })
                .map(Arc::from);
        }
        Ok::<_, Status>(built)
    };

    if mode == HeldMode::On && !audit {
        if attempt != Advance::Skip {
            match entry
                .and_then(|held| advance_guarded(store, &key, &held, req, p, snapshots, &lineages))
            {
                // Held state turned off while it advanced (another poll's audit failure or panic): the advance is not served, and the poll builds in full.
                Ok(_) if store.mode() != HeldMode::On => {}
                Ok(advanced) => {
                    on_dense(DenseShape {
                        series: advanced.next.model.series.len(),
                        columns: advanced.next.model.x_values.len(),
                        prepared_rows: 0,
                        largest_series: 0,
                        models: 1,
                        extra: advanced.extra_bytes(),
                    });
                    count("served");
                    answered();
                    store.insert(key, advanced.next);
                    return Ok(Some(BuiltChart {
                        response: advanced.response,
                        kind: advanced.kind,
                        memo_audit: None,
                        held: None,
                        advanced: true,
                        moved_from: None,
                    }));
                }
                Err(reason) => miss(reason),
            }
            if attempt == Advance::Only {
                return Ok(None);
            }
        }
        let mut built = full()?;
        answered();
        keep(built.held.take());
        return Ok(Some(built));
    }

    // Shadow mode, or an audited poll in `on` mode: the full build answers; an advance from the same entry must equal it.
    let mut built = full()?;
    let captured = built.held.take();
    // Its capture panicked and turned held state off (or something else did): nothing to compare the advance with, and nothing to hold.
    if store.mode() == HeldMode::Off {
        answered();
        keep(captured);
        return Ok(Some(built));
    }
    // The client's entry stays in the store, charged, while the advance reads it; the answer releases it after.
    let advanced =
        entry.and_then(|held| advance_guarded(store, &key, &held, req, p, snapshots, &lineages));
    answered();
    let advanced = match advanced {
        Ok(advanced) => advanced,
        Err(reason) => {
            miss(reason);
            keep(captured);
            return Ok(Some(built));
        }
    };
    // A full build that failed its own checks (the stage-1 memo's, or the planner's prefix audit) already answered in full with `audit_failed`, and one whose state could not be held is no reference for the advance.
    let reference = match &captured {
        Some(Ok(reference)) if !built.response.audit_failed => reference,
        _ => {
            count("audit_skipped");
            keep(captured);
            return Ok(Some(built));
        }
    };
    // Both models and both replies are live now.
    charge_held(reference.state_bytes() + advanced.extra_bytes());
    let difference = if advanced.response != built.response {
        Some("reply".to_string())
    } else if advanced.kind != built.kind {
        Some("kind".to_string())
    } else {
        advanced.next.state_difference(reference)
    };
    match (mode, difference) {
        (HeldMode::Shadow, None) => {
            count("shadow_match");
            store.insert(key, advanced.next);
        }
        (HeldMode::Shadow, Some(part)) => {
            // Counted (the chart's first in each part logged) and answered by the full build, whose state is kept.
            count("shadow_divergence");
            if store.first_divergence(&key, &part) {
                tracing::error!(key = %short(&key), %part, "held chart state diverged from the full build in shadow mode; this chart's later divergences in this part are only counted");
            }
            keep(captured);
        }
        (_, None) => {
            // An audited poll keeps the full build's state, the ground truth.
            count("audit_ok");
            keep(captured);
        }
        (_, Some(part)) => {
            count("audit_failed");
            tracing::error!(key = %short(&key), %part, "held chart state disagreed with the full build under audit; answering in full and turning held state off until restart");
            store.disable();
            // The full build's answer in full, flagged.
            built.response = audit_failed_full(
                &reference.model,
                std::mem::take(&mut built.response.frontiers),
            );
            built.kind = Some("audit_failed");
        }
    }
    Ok(Some(built))
}

/// A key's first bytes, for logs.
fn short(key: &RequestKey) -> String {
    key[..6].iter().map(|b| format!("{b:02x}")).collect()
}
