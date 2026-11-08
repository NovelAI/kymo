//! Stage 3b of docs/chart-request-latency.md, the chart side: advance a model [`shared_chart`] emitted by samples appended to its series. The axis advances by the rules [`decide_axis`], [`make_grid_from`] and [`plan_axis`] apply, the slots the new samples can change are recomputed by [`bucket_run`]'s and [`shared_passthrough`]'s rules over the same inputs in the same order, and every other slot is copied from the held model. The caller (query/held.rs) supplies sample access and says which samples changed; the differential fuzz (query/frontier_delta_tests/held_tests.rs) holds the result to the full build bit for bit.

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
    /// The smoothing plan changes: the median moved, or a first finite output.
    PlanShift,
    /// The grid's tier or anchor moved, or a passthrough chart started downsampling.
    Regrid,
    /// An envelope cell became occupied on a chart that shipped passthrough-shaped.
    EnvelopeFlip,
    /// A passthrough-shaped chart with a repeated x in a series.
    Duplicates,
    /// An x (step, or timestamp as the axis uses it) beyond the integers f64 holds exactly.
    InexactX,
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
    /// The downsample decision's plan, and the log ladder's lowest x its grid was made from ([`log_ladder_lo`]). With no occupied envelope cell the chart ships passthrough-shaped columns over `plan.raw_xs`.
    Grid {
        plan: AxisPlan,
        p_lo: f64,
        g_first: f64,
        g_last: f64,
    },
}

impl HeldAxis {
    fn plan(&self) -> Option<&AxisPlan> {
        match self {
            Self::Grid { plan, .. } => Some(plan),
            Self::Passthrough => None,
        }
    }

    /// The chart's smoothing [`Floor`], from its grid's tier (none passing through).
    pub(crate) fn floor(&self) -> Floor {
        self.plan()
            .map_or_else(Floor::default, |plan| Floor::of(plan.grid.kind))
    }

    /// Whether the chart ships [`shared_passthrough`]'s columns: raw slots only, raw values when smoothed, bands only under a repeated x.
    pub(crate) fn passthrough_shaped(&self) -> bool {
        self.plan().is_none_or(|plan| !plan.any_env_occupied())
    }

    /// Heap bytes held beyond the value itself: each buffer at its capacity and the allocator's rounding.
    pub(crate) fn heap_bytes(&self) -> usize {
        use crate::series_cache::vec_bytes;
        self.plan().map_or(0, |plan| {
            vec_bytes(&plan.env) + vec_bytes(&plan.ext) + vec_bytes(&plan.raw_xs)
        })
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
            agg.fold(xs[j], env_src[j], kinds[j]);
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
    /// The x of its first finite sample, which a causal slot must have read ([`SlotSmoother::shows`]).
    first: f64,
}

impl CellAgg {
    /// The aggregates' bits, for exact comparison.
    pub(crate) fn bits(&self) -> (usize, u32, [u64; 4], u8) {
        (
            self.cell,
            self.cnt,
            [
                self.sum.to_bits(),
                self.ymin.to_bits(),
                self.ymax.to_bits(),
                self.first.to_bits(),
            ],
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
            first: f64::NAN,
        }
    }

    fn fold(&mut self, x: f64, v: f64, kind: u8) {
        if kind != 0 {
            if self.nfkind == 0 {
                self.nfkind = kind;
            }
            return;
        }
        if self.cnt == 0 {
            self.first = x;
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
    /// The advanced axis's raw slots, ascending: every column passing through, else its grid's raw cells' x.
    pub(crate) fn raw_slots(&self) -> &[f64] {
        self.axis.plan().map_or(&self.x_values, |plan| &plan.raw_xs)
    }
}

/// Merge `fresh` x values (any order, repeats and held x included) into the ascending distinct axis `old_x`.
fn merge_raw(old_x: &[f64], mut fresh: Vec<f64>) -> Vec<f64> {
    fresh.sort_by(f64::total_cmp);
    fresh.dedup();
    fresh.retain(|x| old_x.binary_search_by(|p| p.total_cmp(x)).is_err());
    let mut x = Vec::with_capacity(old_x.len() + fresh.len());
    let (mut i, mut j) = (0, 0);
    while i < old_x.len() || j < fresh.len() {
        if j == fresh.len() || (i < old_x.len() && old_x[i] < fresh[j]) {
            x.push(old_x[i]);
            i += 1;
        } else {
            x.push(fresh[j]);
            j += 1;
        }
    }
    x
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
            let x_values = merge_raw(old_x, fresh);
            // decide_axis passes through exactly while distinct ≤ cap (decision_cap).
            if x_values.len() > decision_cap(g_first, g_last, spec) {
                return Err(Fallback::Regrid);
            }
            // Each slot's index in `old_x`, whose x the merge keeps in order.
            let mut held = old_x.iter().enumerate().peekable();
            let old = x_values
                .iter()
                .map(|x| held.next_if(|&(_, h)| h == x).map(|(i, _)| i))
                .collect();
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
            let mut raw_new: Vec<f64> = Vec::new();
            // Non-step raw cells receiving new x values.
            let mut sparse: Vec<(usize, f64)> = Vec::new();
            for &x in new_xs.iter().flat_map(|xs| xs.iter()) {
                match grid.index(x) {
                    // A step axis's raw cells, narrower than ENV_MIN_POINTS steps, stay raw without extents, as plan_axis leaves them.
                    Some(c) if env[c] || !spec.is_step_axis => {
                        let e = &mut ext[c];
                        e.0 = e.0.min(x);
                        e.1 = e.1.max(x);
                        if !env[c] {
                            sparse.push((c, x));
                        }
                    }
                    _ => raw_new.push(x),
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
            let raw_xs = merge_raw(&raw_xs, raw_new);
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

/// One series' emitted samples at the current state, in emitted (ascending-x) order.
pub(crate) trait Samples {
    fn len(&self) -> usize;
    fn x(&self, e: usize) -> f64;
    /// The logged value.
    fn raw(&self, e: usize) -> f64;
    /// Smoothed charts: the smoother evaluated at `x`, as the full build's slots evaluate it ([`SlotEvaluator`]), `first` being the x of the series' first sample in the slot's cell; NaN where it does not show ([`SlotSmoother::shows`]).
    fn slot_value(&mut self, x: f64, first: f64) -> f64;
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

/// What the advance knows about one series.
pub(crate) struct SeriesStep<'a> {
    pub(crate) samples: &'a mut dyn Samples,
    /// First new emitted sample (the sample count when none is new): raw slots from its x on are recomputed.
    pub(crate) first_new: usize,
    /// Smoothed charts: slots from this x on re-evaluate, their smoother reading a changed sample (+∞ where only new, moved or re-aggregated slots do).
    pub(crate) slot_dirty_from: f64,
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
    /// (series, envelope slot) values re-evaluated because the shared center moved.
    pub(crate) moved: usize,
    /// (series, slot) values re-evaluated because their smoother reads a changed sample.
    pub(crate) reached: usize,
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

/// The first of the series' emitted samples at or past grid cell `c` (below-grid samples lead).
fn cell_start(samples: &dyn Samples, step: &AxisStep, c: usize) -> usize {
    let plan = step.axis.plan().expect("envelope cells lie on a grid");
    partition(0, samples.len(), |e| {
        plan.grid.index(samples.x(e)).is_none_or(|i| i < c)
    })
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
    let new_x = if s.first_new < len {
        s.samples.x(s.first_new)
    } else {
        f64::INFINITY
    };
    let had_band = !old.min_values.is_empty();
    let had_raw = !old.raw_values.is_empty();
    let mut values = vec![f64::NAN; nax];
    let mut raw_values = if had_raw {
        vec![f64::NAN; nax]
    } else {
        Vec::new()
    };
    // Shaped advances cannot gain repeated x or envelopes, so they keep no band.
    let band_len = if shaped { 0 } else { nax };
    let mut mins = vec![f64::NAN; band_len];
    let mut maxs = vec![f64::NAN; band_len];
    let mut any_band = had_band;
    let mut markers: Vec<(u32, u32)> = Vec::new();
    let old_marker = |k0: usize| -> Option<u32> {
        let i = old.nan_indices.binary_search(&(k0 as u32)).ok()?;
        Some(old.nan_kinds[i]).filter(|&kind| kind != 4)
    };

    // Envelope aggregates this step changes: cells that became envelopes (every sample there), then new samples in envelope cells, continuing the held open cell where they extend it.
    let plan = step.axis.plan();
    let mut aggs: Vec<CellAgg> = Vec::new();
    if let Some(plan) = plan {
        for &c in &step.flipped {
            let lo = cell_start(s.samples, step, c);
            let hi = cell_start(s.samples, step, c + 1);
            if lo < hi {
                let mut agg = CellAgg::new(c);
                for e in lo..hi {
                    let v = s.samples.raw(e);
                    agg.fold(s.samples.x(e), v, nan_kind(v));
                }
                aggs.push(agg);
            }
        }
        for e in s.first_new..len {
            let x = s.samples.x(e);
            let Some(c) = plan.grid.index(x).filter(|&c| plan.env[c]) else {
                continue;
            };
            if step.flipped.binary_search(&c).is_ok() {
                continue;
            }
            if aggs.last().is_none_or(|agg| agg.cell != c) {
                aggs.push(match s.open {
                    Some(open) if open.cell == c => open,
                    _ => CellAgg::new(c),
                });
            }
            let v = s.samples.raw(e);
            aggs.last_mut().unwrap().fold(x, v, nan_kind(v));
        }
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
                Some(k0) if x < new_x => {
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
                    // A smoothed slot has a value exactly when it holds a finite sample, which its smoother reads.
                    if is_smoothed && x >= s.slot_dirty_from && old.values[k0].is_finite() {
                        work.slots += 1;
                        work.reached += 1;
                        values[k] = round_y(s.samples.slot_value(x, x));
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
                    // The slot's value is its last finite sample's logged value, or on a smoothed chart the smoother at its x when it holds one. A held slot's held samples keep their logged values, so its band, markers, last finite raw value and value continue from its held columns (the band's min and max fold on in sample order, as the full build's do): however many samples share its x, only the new ones are visited.
                    let mut from = lo;
                    if let Some(k0) = step.old[k] {
                        from = s.first_new.max(lo);
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
                    }
                    for e in from..hi {
                        let r = s.samples.raw(e);
                        let kind = nan_kind(r);
                        if kind != 0 {
                            markers.push((k as u32, kind as u32));
                            continue;
                        }
                        (v, raw) = (r, r);
                        (lo_y, hi_y) = (lo_y.min(r), hi_y.max(r));
                    }
                    if is_smoothed && v.is_finite() {
                        v = s.samples.slot_value(x, x);
                    }
                    values[k] = round_y(v);
                    if had_raw {
                        raw_values[k] = round_y(raw);
                    } else if !shaped && lo_y.is_finite() {
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
                    } else {
                        values[k] = round_y(if is_smoothed {
                            s.samples.slot_value(x, agg.first)
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
                    // The cell holds samples, all finite: where its smoother shows, the slot has a value.
                    let all_finite = had_band && !old.min_values[k0].is_nan() && y_marker.is_none();
                    let moved = x.to_bits() != held_x[k0].to_bits();
                    values[k] = if is_smoothed && all_finite && (moved || x >= s.slot_dirty_from) {
                        work.slots += 1;
                        if moved {
                            work.moved += 1;
                        } else {
                            work.reached += 1;
                        }
                        // Its first sample, finite: the cell has no marker (bucket_run's `first_x` is its first finite one).
                        let first = s.samples.x(cell_start(s.samples, step, c));
                        round_y(s.samples.slot_value(x, first))
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
            super::anchor_slot(
                step.axis.plan(),
                &step.slot_of_cell,
                &step.x_values,
                s.samples.x(0),
            )
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

    /// A causal smoother resumed from any state its fold records reproduces the uninterrupted run bit for bit.
    #[test]
    fn causal_runs_resume_from_checkpoints_bit_for_bit() {
        let (x, y) = series(3_000, 7, true);
        for smoother in [
            CausalSmoother::Ema {
                tau: Width::fixed(3.0),
                dx_ref: 1.0,
                deg: 2,
            },
            CausalSmoother::Ema {
                tau: Width::fixed(250.0),
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
            let (mut checkpoints, mut state) = (Vec::new(), CausalState::START);
            smoother.fold_recording(&[(&x, &y)], &mut state, 0, Some(&mut checkpoints));
            checkpoints.push(state);
            let mut uninterrupted = CausalState::START;
            let whole = smoother.run(&x, &y, &mut uninterrupted);
            assert_eq!(checkpoints.last().unwrap().bits(), uninterrupted.bits());
            assert_eq!(
                checkpoints.len(),
                3_000usize.div_ceil(CAUSAL_CHECKPOINT_STRIDE) + 1
            );
            for (k, &recorded) in checkpoints.iter().enumerate() {
                let index = (k * CAUSAL_CHECKPOINT_STRIDE).min(x.len());
                let mut state = recorded;
                let tail = smoother.run(&x[index..], &y[index..], &mut state);
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
}
