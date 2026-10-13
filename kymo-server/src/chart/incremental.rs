//! Stage 3b of docs/chart-request-latency.md, the chart side: advance a model [`shared_chart`] emitted by samples appended to its series. The axis advances by the rules [`decide_axis`], [`make_grid`] and [`plan_axis`] apply, the slots the new samples can change are recomputed by [`bucket_run`]'s and [`shared_passthrough`]'s rules over the same inputs in the same order, and every other slot is copied from the held model. The caller (query/held.rs) supplies sample access and says which samples changed; the differential fuzz (query/frontier_delta_tests/held_tests.rs) holds the result to the full build bit for bit.

use super::*;
use crate::chart_delta::round_y;

/// Why a poll was not answered by advancing held state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Fallback {
    /// No entry for the chart and lineages.
    Absent,
    /// The client holds a different response than any entry.
    Cutoff,
    /// A tag's rows do not extend the held ones as the lineage promises.
    Lineage,
    /// A series appeared or disappeared: a tag group, a run's first rows in the fetch range, or the scalar group yielding to tags.
    Tags,
    /// A time-axis row before its series' held maximum, or held rows out of timestamp order.
    TimeOrder,
    /// A series gained its first in-range samples.
    Membership,
    /// A new sample with an unplottable x (a kind-4 marker).
    Unplottable,
    /// The smoothing plan changes: uniform spacing broken, the median moved, or a first finite output.
    PlanShift,
    /// The grid's tier or anchor moved, or a passthrough chart started downsampling.
    Regrid,
    /// An envelope cell became occupied on a chart that shipped passthrough-shaped.
    EnvelopeFlip,
    /// A passthrough-shaped chart with a repeated x in a series.
    Duplicates,
    /// An x (step, or timestamp as the axis uses it) beyond the integers f64 holds exactly.
    InexactX,
    /// Irregular Savitzky-Golay outputs to recompute whose windows cost more than the full build's (repeated or clustered timestamps, an appended stall, a window wide against its series), which the full build computes faster.
    Dense,
    /// No plottable point.
    Empty,
    /// The advance panicked.
    Panic,
}

impl Fallback {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Cutoff => "cutoff",
            Self::Lineage => "lineage",
            Self::Tags => "tags",
            Self::TimeOrder => "time_order",
            Self::Membership => "membership",
            Self::Unplottable => "unplottable",
            Self::PlanShift => "plan_shift",
            Self::Regrid => "regrid",
            Self::EnvelopeFlip => "envelope_flip",
            Self::Duplicates => "duplicates",
            Self::InexactX => "inexact_x",
            Self::Dense => "dense",
            Self::Empty => "empty",
            Self::Panic => "panic",
        }
    }
}

/// The axis a chart with a plottable point was emitted on ([`shared_chart`]). Its floats (sample x, grid bounds, ±∞ sentinels) are never NaN or −0, so the derived `==` is bit equality.
#[derive(Clone, PartialEq)]
pub(crate) enum HeldAxis {
    /// The passthrough decision: the axis is the model's x values, every distinct x, so its ends are the chart's bounds.
    Passthrough,
    /// The downsample decision's plan, and the floor its grid was made from ([`log_ladder_floor`]). With no occupied envelope cell the chart ships passthrough-shaped columns over `plan.raw_xs`.
    Grid {
        plan: AxisPlan,
        p_lo: f64,
        g_first: f64,
        g_last: f64,
    },
}

impl HeldAxis {
    /// Whether the chart ships [`shared_passthrough`]'s columns: raw slots only, raw values when smoothed, bands only under a repeated x.
    pub(crate) fn passthrough_shaped(&self) -> bool {
        match self {
            Self::Passthrough => true,
            Self::Grid { plan, .. } => !plan.any_env_occupied(),
        }
    }

    /// Heap bytes held beyond the value itself: each buffer at its capacity and the allocator's rounding.
    pub(crate) fn heap_bytes(&self) -> usize {
        use crate::series_cache::vec_bytes;
        match self {
            Self::Passthrough => 0,
            Self::Grid { plan, .. } => {
                vec_bytes(&plan.env) + vec_bytes(&plan.ext) + vec_bytes(&plan.raw_xs)
            }
        }
    }

    /// The running aggregates [`bucket_run`] holds for a series' last occupied envelope cell (the one appends extend), from the series' emitted samples: `env_src` is the raw value when smoothing, else the plot value, as there.
    pub(crate) fn open_cell(&self, xs: &[f64], env_src: &[f64], kinds: &[u8]) -> Option<CellAgg> {
        let Self::Grid { plan, .. } = self else {
            return None;
        };
        let &last = xs.last()?;
        let cell = plan.grid.index(last).filter(|&c| plan.env[c])?;
        let first = xs.partition_point(|&x| plan.grid.index(x) != Some(cell));
        let mut agg = CellAgg::new(cell);
        for j in first..xs.len() {
            agg.fold(env_src[j], kinds[j]);
        }
        Some(agg)
    }
}

/// One series' running aggregates over one envelope cell, folded in sweep order: what [`bucket_run`] accumulates there.
#[derive(Clone, Copy)]
pub(crate) struct CellAgg {
    cell: usize,
    cnt: u32,
    sum: f64,
    ymin: f64,
    ymax: f64,
    nfkind: u8,
}

impl CellAgg {
    /// The aggregates' bits, for exact comparison.
    pub(crate) fn bits(&self) -> (usize, u32, [u64; 3], u8) {
        (
            self.cell,
            self.cnt,
            [self.sum.to_bits(), self.ymin.to_bits(), self.ymax.to_bits()],
            self.nfkind,
        )
    }

    fn new(cell: usize) -> Self {
        Self {
            cell,
            cnt: 0,
            sum: 0.0,
            ymin: f64::INFINITY,
            ymax: f64::NEG_INFINITY,
            nfkind: 0,
        }
    }

    fn fold(&mut self, v: f64, kind: u8) {
        if kind != 0 {
            if self.nfkind == 0 {
                self.nfkind = kind;
            }
            return;
        }
        self.cnt += 1;
        self.sum += v;
        self.ymin = self.ymin.min(v);
        self.ymax = self.ymax.max(v);
    }
}

#[derive(Clone, Copy)]
enum SlotKind {
    Raw,
    Env(usize),
}

/// The advanced axis and how its slots relate to the held ones.
pub(crate) struct AxisStep {
    pub(crate) axis: HeldAxis,
    pub(crate) x_values: Vec<f64>,
    pub(crate) xr_min: Vec<f64>,
    pub(crate) xr_max: Vec<f64>,
    slots: Vec<SlotKind>,
    /// Per slot: the held slot of the same identity (the same raw x, or the same occupied envelope cell), if any.
    old: Vec<Option<usize>>,
    /// Cells that became envelopes in this step (non-step axes); every series re-aggregates them.
    flipped: Vec<usize>,
    /// Occupied envelope cells → their slot.
    slot_of_cell: Vec<usize>,
}

impl AxisStep {
    fn grid_plan(&self) -> Option<&AxisPlan> {
        match &self.axis {
            HeldAxis::Grid { plan, .. } => Some(plan),
            HeldAxis::Passthrough => None,
        }
    }

    /// Where a kind-4 marker anchored at `x` sits ([`MixedAxis::anchor_slot`], or the passthrough slot).
    fn anchor_slot(&self, x: f64) -> usize {
        match self
            .grid_plan()
            .and_then(|plan| plan.grid.index(x).filter(|&c| plan.env[c]))
        {
            Some(c) => self.slot_of_cell[c],
            None => self.x_values.partition_point(|&v| v < x),
        }
    }
}

/// Merge `fresh` x values (any order, repeats and held x included) into the ascending distinct axis `old_x`: the merged axis and, per merged slot, its index in `old_x`.
fn merge_raw(old_x: &[f64], mut fresh: Vec<f64>) -> (Vec<f64>, Vec<Option<usize>>) {
    fresh.sort_by(f64::total_cmp);
    fresh.dedup();
    fresh.retain(|x| old_x.binary_search_by(|p| p.total_cmp(x)).is_err());
    let mut x = Vec::with_capacity(old_x.len() + fresh.len());
    let mut old = Vec::with_capacity(old_x.len() + fresh.len());
    let (mut i, mut j) = (0, 0);
    while i < old_x.len() || j < fresh.len() {
        if j == fresh.len() || (i < old_x.len() && old_x[i] < fresh[j]) {
            x.push(old_x[i]);
            old.push(Some(i));
            i += 1;
        } else {
            x.push(fresh[j]);
            old.push(None);
            j += 1;
        }
    }
    (x, old)
}

/// The identity of the slot at `x`, ordered as slots are: below-grid raw x first (cell −1), then cells ascending, an envelope cell's one slot (its center lies in the cell) or a raw cell's x ascending. Axis x are never NaN or −0 ([`HeldAxis`]), so `<` and `==` order keys totally.
fn slot_key(plan: &AxisPlan, x: f64) -> (i64, u8, f64) {
    match plan.grid.index(x) {
        None => (-1, 1, x),
        Some(c) if plan.env[c] => (c as i64, 0, 0.0),
        Some(c) => (c as i64, 1, x),
    }
}

/// Advance a held axis by each series' new emitted x values (ascending, each past its series' held samples). `old_x` is the held model's axis.
pub(crate) fn advance_axis(
    held: &HeldAxis,
    old_x: &[f64],
    new_xs: &[&[f64]],
    spec: GridSpec,
) -> Result<AxisStep, Fallback> {
    let (g_first, mut g_last) = match *held {
        HeldAxis::Passthrough => (old_x[0], old_x[old_x.len() - 1]),
        HeldAxis::Grid {
            g_first, g_last, ..
        } => (g_first, g_last),
    };
    for xs in new_xs {
        if let (Some(&first), Some(&last)) = (xs.first(), xs.last()) {
            // New x follow held samples of their series, so the axis only grows past its held end.
            debug_assert!(first >= g_first, "new x below the held axis");
            g_last = g_last.max(last);
        }
    }
    match held {
        HeldAxis::Passthrough => {
            let fresh = new_xs.iter().flat_map(|xs| xs.iter().copied()).collect();
            let (x_values, old) = merge_raw(old_x, fresh);
            // decide_axis passes through exactly while distinct ≤ cap (decision_cap).
            if spec.target != 0
                && g_last > g_first
                && x_values.len() > decision_cap(g_first, g_last, spec)
            {
                return Err(Fallback::Regrid);
            }
            let n = x_values.len();
            Ok(AxisStep {
                axis: HeldAxis::Passthrough,
                xr_min: vec![f64::NAN; n],
                xr_max: vec![f64::NAN; n],
                slots: vec![SlotKind::Raw; n],
                old,
                x_values,
                flipped: Vec::new(),
                slot_of_cell: Vec::new(),
            })
        }
        HeldAxis::Grid { plan, p_lo, .. } => {
            // The decision stays Downsample: distinct x only grow, and decision_cap only shrinks as the span grows (a linear step grid's width only grows, so its cap stays the target).
            let grid = make_grid_from(*p_lo, g_first, g_last, spec);
            if grid.kind != plan.grid.kind || grid.base != plan.grid.base {
                return Err(Fallback::Regrid);
            }
            let held_shaped = !plan.any_env_occupied();
            let mut env = plan.env.clone();
            let mut ext = plan.ext.clone();
            for c in plan.grid.cells..grid.cells {
                env.push(spec.is_step_axis && grid.width(c) >= ENV_MIN_POINTS);
                ext.push((f64::INFINITY, f64::NEG_INFINITY));
            }
            let raw_hi = match grid.kind {
                GridKind::Linear { x_lo, .. } => x_lo,
                GridKind::Log { m, shift } => pow2(m + 2) - if shift { 1.0 } else { 0.0 },
            };
            let mut raw_new: Vec<f64> = Vec::new();
            // Non-step raw cells receiving new x values.
            let mut sparse: Vec<(usize, f64)> = Vec::new();
            for &x in new_xs.iter().flat_map(|xs| xs.iter()) {
                let cell = grid.index(x);
                if spec.is_step_axis {
                    if x < raw_hi {
                        raw_new.push(x);
                    } else if let Some(c) = cell {
                        let e = &mut ext[c];
                        e.0 = e.0.min(x);
                        e.1 = e.1.max(x);
                    }
                    continue;
                }
                match cell {
                    None => raw_new.push(x),
                    Some(c) => {
                        let e = &mut ext[c];
                        e.0 = e.0.min(x);
                        e.1 = e.1.max(x);
                        if !env[c] {
                            sparse.push((c, x));
                        }
                    }
                }
            }
            let mut raw_xs = plan.raw_xs.clone();
            let mut flipped = Vec::new();
            sparse.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
            sparse.dedup_by(|a, b| a.0 == b.0 && a.1.to_bits() == b.1.to_bits());
            for group in sparse.chunk_by(|a, b| a.0 == b.0) {
                let c = group[0].0;
                // plan_axis settles a non-step cell as an envelope at its ENV_MIN_POINTS-th distinct x.
                let lo = raw_xs.partition_point(|&x| grid.index(x).is_none_or(|i| i < c));
                let hi = raw_xs.partition_point(|&x| grid.index(x).is_none_or(|i| i <= c));
                let fresh: Vec<f64> = group
                    .iter()
                    .map(|&(_, x)| x)
                    .filter(|x| raw_xs[lo..hi].binary_search_by(|p| p.total_cmp(x)).is_err())
                    .collect();
                if ((hi - lo + fresh.len()) as f64) < ENV_MIN_POINTS {
                    raw_new.extend(fresh);
                } else {
                    env[c] = true;
                    flipped.push(c);
                    raw_xs.drain(lo..hi);
                }
            }
            let (raw_xs, _) = merge_raw(&raw_xs, raw_new);
            let new_plan = AxisPlan {
                grid,
                env,
                ext,
                raw_xs,
            };
            // A passthrough-shaped chart stays one: its columns are its raw slots, which is what `mixed_axis` lays out without an occupied envelope.
            if held_shaped && new_plan.any_env_occupied() {
                return Err(Fallback::EnvelopeFlip);
            }
            let ma = mixed_axis(new_plan);
            // Each slot's held slot of the same identity (`old_x` holds the held plan's slots, keys ascending).
            let mut held_keys = old_x
                .iter()
                .map(|&x| slot_key(plan, x))
                .enumerate()
                .peekable();
            let mut old = Vec::with_capacity(ma.x.len());
            let mut slots = Vec::with_capacity(ma.x.len());
            for &x in &ma.x {
                let key = slot_key(&ma.plan, x);
                let mut held_at = None;
                while let Some((i, k)) = held_keys.next_if(|(_, k)| *k <= key) {
                    held_at = (k == key).then_some(i);
                }
                old.push(held_at);
                slots.push(match key {
                    (c, 0, _) => SlotKind::Env(c as usize),
                    _ => SlotKind::Raw,
                });
            }
            Ok(AxisStep {
                axis: HeldAxis::Grid {
                    plan: ma.plan,
                    p_lo: *p_lo,
                    g_first,
                    g_last,
                },
                x_values: ma.x,
                xr_min: ma.xr_min,
                xr_max: ma.xr_max,
                slots,
                old,
                flipped,
                slot_of_cell: ma.slot_of_cell,
            })
        }
    }
}

/// [`numeric_delta_from_col`]'s arms, given the held and advanced axes it would derive and only the samples that can dirty a column (non-OLD ones). The caller has proved no series repeats an x, which passthrough arms would otherwise answer in full.
pub(crate) fn planner_from_col(
    held: &HeldAxis,
    held_x: &[f64],
    step: &AxisStep,
    xs: &[&[f64]],
    age: &[&[u8]],
    continuing: &[bool],
) -> usize {
    // [`advance_axis`] refuses a change of decision, a re-grid and an envelope flip, the arms that answer in full: both axes are passthrough, both passthrough-shaped grids (whose columns are the held raw slots, `held_x`), or two grids of one tier and anchor with an occupied envelope.
    match (held, &step.axis) {
        (HeldAxis::Grid { plan: old, .. }, HeldAxis::Grid { plan: new, .. })
            if old.any_env_occupied() =>
        {
            debug_assert!(
                new.any_env_occupied()
                    && new.grid.kind == old.grid.kind
                    && new.grid.base == old.grid.base
            );
            mixed_from_col(old, new, xs, age, continuing)
        }
        _ => {
            debug_assert!(match held {
                HeldAxis::Grid { plan, .. } => plan.raw_xs == held_x,
                HeldAxis::Passthrough => true,
            });
            passthrough_dirty_from_col(held_x, xs, age, continuing)
        }
    }
}

/// One series' emitted samples at the current state, in emitted (ascending-x) order.
pub(crate) trait Samples {
    fn len(&self) -> usize;
    fn x(&mut self, e: usize) -> f64;
    /// The logged value.
    fn raw(&mut self, e: usize) -> f64;
    /// The plotted value: exactly the smoothed one when smoothing, else the logged one.
    fn plot(&mut self, e: usize) -> f64;
    /// For a walk down from `e`: the start of a known run of non-finite plot values holding sample `e - 1`, else `e`. Lets a walk to the nearest finite values skip long holes.
    fn hole_below(&mut self, e: usize) -> usize;
    /// For a walk up from `e`: the end of a known run of non-finite plot values holding sample `e`, else `e`.
    fn hole_from(&mut self, e: usize) -> usize;
}

/// First `e` in `lo..hi` where `pred` turns false (pred monotone true-then-false).
pub(crate) fn partition(lo: usize, hi: usize, mut pred: impl FnMut(usize) -> bool) -> usize {
    let (mut lo, mut hi) = (lo, hi);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if pred(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// The nearest finite plot values below sample `e`, nearest first, at most two, as (sample, value); walks skip known holes.
pub(crate) fn finite_below(s: &mut dyn Samples, mut e: usize) -> Vec<(usize, f64)> {
    let mut found = Vec::with_capacity(2);
    while e > 0 && found.len() < 2 {
        let below = s.hole_below(e);
        if below < e {
            e = below;
            continue;
        }
        e -= 1;
        let v = s.plot(e);
        if v.is_finite() {
            found.push((e, v));
        }
    }
    found
}

/// The smoothed line at `xq` as [`bucket_run`] interpolates it ([`lerp_at`] over the series' finite plot points): from the two finite points either side of `xq`, which select the same segment and arithmetic as the whole list.
fn lerp_value(s: &mut dyn Samples, xq: f64) -> f64 {
    let n = s.len();
    let p = partition(0, n, |e| s.x(e) < xq);
    let mut fin: Vec<(f64, f64)> = Vec::with_capacity(4);
    for (e, v) in finite_below(s, p).into_iter().rev() {
        fin.push((s.x(e), v));
    }
    let before = fin.len();
    let mut e = p;
    while e < n && fin.len() < before + 2 {
        let past = s.hole_from(e);
        if past > e {
            e = past;
            continue;
        }
        let v = s.plot(e);
        if v.is_finite() {
            fin.push((s.x(e), v));
        }
        e += 1;
    }
    lerp_at(&fin, &mut 0, xq)
}

/// What the advance knows about one series.
pub(crate) struct SeriesStep<'a> {
    pub(crate) samples: &'a mut dyn Samples,
    /// First new emitted sample (the sample count when none is new).
    pub(crate) first_new: usize,
    /// First emitted sample that is new or whose plot value may have changed: raw slots from its x on are recomputed.
    pub(crate) changed_from: usize,
    /// Smoothed charts: envelope slots whose x exceeds this re-interpolate (−∞ for every one, +∞ for none beyond moved or re-aggregated cells).
    pub(crate) lerp_after: f64,
    /// The held aggregates of the series' last occupied envelope cell.
    pub(crate) open: Option<CellAgg>,
}

/// Work an advance did, for the differential test's bounds and coverage.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RebuildWork {
    /// (series, slot) pairs recomputed rather than copied.
    pub(crate) slots: usize,
    /// Slots new to the axis (appended or inserted).
    pub(crate) fresh_slots: usize,
    /// Slots inserted before a held one (shifting the columns after them).
    pub(crate) inserted_slots: usize,
    /// Cells that became envelopes.
    pub(crate) flipped_cells: usize,
    /// (series, envelope slot) values re-interpolated because the shared center moved.
    pub(crate) moved: usize,
    /// (series, envelope slot) values re-interpolated for an interpolation dependency.
    pub(crate) relerped: usize,
    /// Held samples of recomputed raw slots carried by their held columns rather than visited.
    pub(crate) folded: usize,
}

/// The advanced model: every slot recomputed where `series` says it may have changed, copied from `held` elsewhere. Also each series' new open-cell aggregates.
pub(crate) fn rebuild(
    held: &DenseChart,
    step: &AxisStep,
    series: &mut [SeriesStep<'_>],
    is_smoothed: bool,
    work: &mut RebuildWork,
) -> (DenseChart, Vec<Option<CellAgg>>) {
    work.fresh_slots += step.old.iter().filter(|old| old.is_none()).count();
    let last_held = step.old.iter().rposition(Option::is_some);
    work.inserted_slots += last_held.map_or(0, |last| {
        step.old[..last].iter().filter(|old| old.is_none()).count()
    });
    work.flipped_cells += step.flipped.len();
    let mut out = Vec::with_capacity(series.len());
    let mut open = Vec::with_capacity(series.len());
    for (old, s) in held.series.iter().zip(series.iter_mut()) {
        let (dense, cell) = rebuild_series(&held.x_values, old, step, s, is_smoothed, work);
        out.push(dense);
        open.push(cell);
    }
    (
        DenseChart {
            x_values: step.x_values.clone(),
            xr_min: step.xr_min.clone(),
            xr_max: step.xr_max.clone(),
            series: out,
        },
        open,
    )
}

fn rebuild_series(
    held_x: &[f64],
    old: &DenseSeries,
    step: &AxisStep,
    s: &mut SeriesStep<'_>,
    is_smoothed: bool,
    work: &mut RebuildWork,
) -> (DenseSeries, Option<CellAgg>) {
    let nax = step.x_values.len();
    let len = s.samples.len();
    let shaped = step.axis.passthrough_shaped();
    let changed_x = if s.changed_from < len {
        s.samples.x(s.changed_from)
    } else {
        f64::INFINITY
    };
    let had_band = !old.min_values.is_empty();
    let had_raw = !old.raw_values.is_empty();
    let mut values = vec![f64::NAN; nax];
    let mut raw_values = if shaped && is_smoothed {
        vec![f64::NAN; nax]
    } else {
        Vec::new()
    };
    let mut mins = vec![f64::NAN; nax];
    let mut maxs = vec![f64::NAN; nax];
    let mut any_band = had_band;
    let mut markers: Vec<(u32, u32)> = Vec::new();
    let old_marker = |k0: usize| -> Option<u32> {
        let i = old.nan_indices.binary_search(&(k0 as u32)).ok()?;
        Some(old.nan_kinds[i]).filter(|&kind| kind != 4)
    };

    // Envelope aggregates this step changes: cells that became envelopes (every sample there), then new samples in envelope cells, continuing the held open cell where they extend it.
    let plan = step.grid_plan().filter(|_| !shaped);
    let mut aggs: Vec<CellAgg> = Vec::new();
    if let Some(plan) = plan {
        for &c in &step.flipped {
            let lo = partition(0, len, |e| {
                plan.grid.index(s.samples.x(e)).is_none_or(|i| i < c)
            });
            let hi = partition(lo, len, |e| {
                plan.grid.index(s.samples.x(e)).is_none_or(|i| i <= c)
            });
            if lo < hi {
                let mut agg = CellAgg::new(c);
                for e in lo..hi {
                    let v = s.samples.raw(e);
                    agg.fold(v, nan_kind(v));
                }
                aggs.push(agg);
            }
        }
        let mut cur: Option<CellAgg> = None;
        for e in s.first_new..len {
            let x = s.samples.x(e);
            let Some(c) = plan.grid.index(x).filter(|&c| plan.env[c]) else {
                continue;
            };
            if step.flipped.binary_search(&c).is_ok() {
                continue;
            }
            if cur.is_none_or(|agg| agg.cell != c) {
                aggs.extend(cur);
                cur = Some(match s.open {
                    Some(open) if open.cell == c => open,
                    _ => CellAgg::new(c),
                });
            }
            let v = s.samples.raw(e);
            cur.as_mut().unwrap().fold(v, nan_kind(v));
        }
        aggs.extend(cur);
        aggs.sort_by_key(|agg| agg.cell);
    }

    // Recomputed raw slots ascend, and no sample below `cursor` lies at or past the next one's x: a slot holding none of the series' samples costs one look at the sample there, however many slots lie past the series' first change.
    let mut cursor = 0;
    let mut cursor_x = if len > 0 {
        s.samples.x(0)
    } else {
        f64::INFINITY
    };
    for k in 0..nax {
        let x = step.x_values[k];
        match step.slots[k] {
            SlotKind::Raw => match step.old[k] {
                Some(k0) if x < changed_x => {
                    values[k] = old.values[k0];
                    if had_band {
                        mins[k] = old.min_values[k0];
                        maxs[k] = old.max_values[k0];
                    }
                    if had_raw {
                        raw_values[k] = old.raw_values[k0];
                    }
                    if let Some(kind) = old_marker(k0) {
                        markers.push((k as u32, kind));
                    }
                }
                _ => {
                    work.slots += 1;
                    let (lo, hi) = if cursor_x > x {
                        (cursor, cursor)
                    } else {
                        let lo = partition(cursor, len, |e| s.samples.x(e) < x);
                        (lo, partition(lo, len, |e| s.samples.x(e) <= x))
                    };
                    if hi > cursor {
                        cursor = hi;
                        cursor_x = if hi < len {
                            s.samples.x(hi)
                        } else {
                            f64::INFINITY
                        };
                    }
                    let (mut v, mut lo_y, mut hi_y, mut raw) =
                        (f64::NAN, f64::NAN, f64::NAN, f64::NAN);
                    // The slot's value is its last finite sample's plot value. A held slot's held samples keep their logged values, so its band, markers and last finite raw value continue from its held columns (the band's min and max fold on in sample order, as the full build's do), and its value is held unless a sample from `changed_from` replaces it: however many samples share its x, only the new ones and those whose plot values changed are visited.
                    let mut from = lo;
                    if let Some(k0) = step.old[k] {
                        from = hi.min(s.first_new).max(lo);
                        work.folded += from - lo;
                        if had_band {
                            lo_y = old.min_values[k0];
                            hi_y = old.max_values[k0];
                        }
                        if had_raw {
                            raw = old.raw_values[k0];
                        }
                        if let Some(kind) = old_marker(k0) {
                            markers.push((k as u32, kind));
                        }
                        v = old.values[k0];
                        let mut e = from;
                        while e > lo.max(s.changed_from) {
                            e -= 1;
                            if nan_kind(s.samples.raw(e)) == 0 {
                                v = s.samples.plot(e);
                                break;
                            }
                        }
                    }
                    let mut last = None;
                    for e in from..hi {
                        let r = s.samples.raw(e);
                        let kind = nan_kind(r);
                        if kind != 0 {
                            markers.push((k as u32, kind as u32));
                            continue;
                        }
                        last = Some(e);
                        raw = r;
                        lo_y = if lo_y.is_finite() { lo_y.min(r) } else { r };
                        hi_y = if hi_y.is_finite() { hi_y.max(r) } else { r };
                    }
                    if let Some(e) = last {
                        v = s.samples.plot(e);
                    }
                    values[k] = round_y(v);
                    if shaped {
                        if is_smoothed {
                            raw_values[k] = round_y(raw);
                        }
                    } else if lo_y.is_finite() {
                        mins[k] = round_y(lo_y);
                        maxs[k] = round_y(hi_y);
                        any_band = true;
                    }
                }
            },
            SlotKind::Env(c) => {
                if let Some(agg) = aggs
                    .binary_search_by_key(&c, |agg| agg.cell)
                    .ok()
                    .map(|i| aggs[i])
                {
                    work.slots += 1;
                    if agg.nfkind != 0 {
                        markers.push((k as u32, agg.nfkind as u32));
                    } else if agg.cnt >= 1 {
                        values[k] = round_y(if is_smoothed {
                            lerp_value(s.samples, x)
                        } else {
                            agg.sum / agg.cnt as f64
                        });
                    }
                    if agg.cnt >= 1 {
                        mins[k] = round_y(agg.ymin);
                        maxs[k] = round_y(agg.ymax);
                        any_band = true;
                    }
                } else if let Some(k0) = step.old[k] {
                    let y_marker = old_marker(k0);
                    if had_band {
                        mins[k] = old.min_values[k0];
                        maxs[k] = old.max_values[k0];
                    }
                    if let Some(kind) = y_marker {
                        markers.push((k as u32, kind));
                    }
                    let has_value = had_band && !old.min_values[k0].is_nan() && y_marker.is_none();
                    let moved = x.to_bits() != held_x[k0].to_bits();
                    values[k] = if is_smoothed && has_value && (moved || x > s.lerp_after) {
                        work.slots += 1;
                        if moved {
                            work.moved += 1;
                        } else {
                            work.relerped += 1;
                        }
                        round_y(lerp_value(s.samples, x))
                    } else {
                        old.values[k0]
                    };
                }
            }
        }
    }

    // Unplottable samples carry one kind-4 marker to the first plottable x (column 0 without one); appends add none.
    if old.xnan_count > 0 {
        let slot = if len > 0 {
            step.anchor_slot(s.samples.x(0))
        } else {
            0
        };
        markers.push((slot as u32, 4));
    }
    markers.sort_unstable();
    markers.dedup_by_key(|&mut (slot, _)| slot);
    let (nan_indices, nan_kinds) = markers.into_iter().unzip();
    // Empty columns hold no allocation, as the full build's do: held state charges each at its capacity.
    if !any_band {
        mins = Vec::new();
        maxs = Vec::new();
    }
    if !raw_values.iter().any(|v| v.is_finite()) {
        raw_values = Vec::new();
    }

    // The series' last occupied envelope cell, whose aggregates the next step continues: re-aggregated this step, or else the held open cell (a cell no new or flipped sample touched keeps its aggregates).
    let open = plan.and_then(|plan| {
        let last = len.checked_sub(1)?;
        let cell = plan
            .grid
            .index(s.samples.x(last))
            .filter(|&c| plan.env[c])?;
        match aggs.binary_search_by_key(&cell, |agg| agg.cell) {
            Ok(i) => Some(aggs[i]),
            Err(_) => s.open.filter(|open| open.cell == cell),
        }
    });

    (
        DenseSeries {
            label: old.label.clone(),
            run_id: old.run_id.clone(),
            values,
            raw_values,
            min_values: mins,
            max_values: maxs,
            nan_indices,
            nan_kinds,
            xnan_count: old.xnan_count,
        },
        open,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(n: usize, seed: u64, holes: bool) -> (Vec<f64>, Vec<f64>) {
        let mut rng = crate::Rng(seed | 1);
        let mut x = 0.0;
        (0..n)
            .map(|_| {
                let r = rng.next();
                x += [1.0, 1.0, 1.0, 2.0, 0.0, 7.0][(r % 6) as usize];
                let y = if holes && r.is_multiple_of(23) {
                    f64::NAN
                } else {
                    ((r >> 16) % 100_000) as f64 / 37.0 - 900.0
                };
                (x, y)
            })
            .unzip()
    }

    /// Any output range of the Savitzky–Golay sweep, from just the inputs [`savgol_inputs`] names, is bit-identical to the whole-series call.
    #[test]
    fn savgol_ranges_match_the_whole_series() {
        for (n, window) in [
            (1usize, 3usize),
            (5, 9),
            (40, 3),
            (61, 10),
            (300, 21),
            (900, 130),
            (2000, 64),
        ] {
            for holes in [false, true] {
                let (_, y) = series(n, n as u64 * 31 + window as u64, holes);
                for order in 0..=2 {
                    let whole = savitzky_golay(&y, window, order);
                    for (from, to) in [
                        (0, n),
                        (0, 1),
                        (n / 3, n / 2 + 1),
                        (n.saturating_sub(5), n),
                        (n / 2, n),
                    ] {
                        let (lo, hi) = savgol_inputs(n, window, from, to);
                        let part = savitzky_golay_range(&y[lo..hi], lo, n, from, to, window, order);
                        let want: Vec<u64> = whole[from.min(n)..to.min(n)]
                            .iter()
                            .map(|v| v.to_bits())
                            .collect();
                        let got: Vec<u64> = part.iter().map(|v| v.to_bits()).collect();
                        assert_eq!(
                            got, want,
                            "n={n} window={window} order={order} [{from}, {to})"
                        );
                    }
                }
            }
        }
    }

    /// A causal smoother resumed from any recorded checkpoint reproduces the uninterrupted run bit for bit.
    #[test]
    fn causal_runs_resume_from_checkpoints_bit_for_bit() {
        let (x, y) = series(3_000, 7, true);
        for smoother in [
            CausalSmoother::Ema {
                tau: 3.0,
                dx_ref: 1.0,
                deg: 2,
            },
            CausalSmoother::Ema {
                tau: 250.0,
                dx_ref: 2.5,
                deg: 0,
            },
            CausalSmoother::Triangular {
                dx_ref: 1.0,
                deg: 1,
            },
            CausalSmoother::Triangular {
                dx_ref: 4.0,
                deg: 2,
            },
        ] {
            let mut checkpoints = Vec::new();
            let mut state = CausalState::START;
            let whole = smoother.run(&x, &y, &mut state, 0, Some(&mut checkpoints));
            assert_eq!(
                checkpoints.len(),
                3_000usize.div_ceil(CAUSAL_CHECKPOINT_STRIDE) + 1
            );
            for &(index, recorded) in &checkpoints {
                let mut state = recorded;
                let tail = smoother.run(&x[index..], &y[index..], &mut state, index, None);
                for (i, v) in tail.iter().enumerate() {
                    assert_eq!(
                        v.to_bits(),
                        whole[index + i].to_bits(),
                        "{smoother:?} from {index}"
                    );
                }
            }
        }
    }

    /// One irregular Savitzky–Golay output equals the whole-series sweep's.
    #[test]
    fn savgol_x_outputs_match_the_sweep() {
        for holes in [false, true] {
            let (x, y) = series(1_500, 11, holes);
            for (h_x, deg) in [(1.5, 2), (6.0, 1), (40.0, 0), (130.0, 2)] {
                let whole = savgol_x(&x, &y, h_x, deg);
                for i in (0..x.len()).step_by(7) {
                    let one = savgol_x_at(&|j| x[j], &|j| y[j], x.len(), i, h_x, deg);
                    assert_eq!(one.to_bits(), whole[i].to_bits(), "h_x={h_x} i={i}");
                }
            }
        }
    }
}
