//! Chart processing pipeline: smoothing, alignment, and min/max-envelope downsampling.

use crate::proto::smoothing_config::Algorithm;

/// Server-side cap on the smoothing window: chart_params clamps the wire value to it, and the smoothers rely on that bound.
pub(crate) const MAX_SMOOTHING_WINDOW: u32 = 4096;

/// The x gap when every gap is equal (1 under two samples): a median without a sort ([`median_dx`]), and on custom x Savitzky–Golay's index-space kernel. Irregular grids, and a series at one x (index distance would set apart samples at the same time), have none and take the x-aware fit.
fn uniform_gap(xs: &[f64]) -> Option<f64> {
    #[cfg(test)]
    SPACING_DERIVATIONS.with(|counts| counts.set((counts.get().0 + 1, counts.get().1)));
    // A smoothed series' x ascend (the smoothers' contract), so a zero first gap is no spacing. They are whole steps or milliseconds (query.rs), whose gaps f64 holds exactly below 2^53, so equal spacing needs no tolerance.
    let d0 = xs.get(1).map_or(1.0, |&x1| x1 - xs[0]);
    (d0 > 0.0 && xs.windows(2).all(|w| w[1] - w[0] == d0)).then_some(d0)
}

/// The median gap between consecutive distinct x, the x unit smoothing widths are quoted in (median, not mean: a handful of logging gaps must not stretch every window), with its counts ([`MedianGaps`]). Zero gaps are skipped: a majority of them would collapse the unit, overflowing Triangular's weights, resetting EMA at every new x and shrinking Savitzky–Golay windows to one x. Without two distinct x every unit smooths alike, so the unit is 1. An evenly spaced series takes one scan and no sort; otherwise the counts are two binary searches in the sorted gaps.
fn median_dx(xs: &[f64]) -> (f64, MedianGaps) {
    if let Some(gap) = uniform_gap(xs) {
        let total = xs.len().saturating_sub(1);
        let gaps = MedianGaps {
            below: 0,
            equal: total,
            total,
        };
        return (gap, gaps);
    }
    #[cfg(test)]
    SPACING_DERIVATIONS.with(|counts| counts.set((counts.get().0, counts.get().1 + 1)));
    let mut gaps: Vec<f64> = xs
        .windows(2)
        .map(|w| w[1] - w[0])
        .filter(|&gap| gap > 0.0)
        .collect();
    if gaps.is_empty() {
        return (1.0, MedianGaps::default());
    }
    gaps.sort_by(f64::total_cmp);
    let m = gaps[gaps.len() / 2];
    let below = gaps.partition_point(|&gap| gap < m);
    let equal = gaps[below..].partition_point(|&gap| gap == m);
    let gaps = MedianGaps {
        below,
        equal,
        total: gaps.len(),
    };
    (m, gaps)
}

/// What proves an append leaves a median plan in place: how many of the series' `total` positive gaps lie below its median ([`median_dx`]) and equal it. Held chart state (query/held.rs) keeps them and extends them by each append's gaps.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MedianGaps {
    below: usize,
    equal: usize,
    total: usize,
}

impl MedianGaps {
    /// These counts with the gaps between consecutive `keys` added, if `m` is still the median positive gap, as it is while there is none (`m` is then [`median_dx`]'s unit 1).
    pub(crate) fn extended(mut self, m: f64, keys: impl Iterator<Item = f64>) -> Option<Self> {
        let mut previous = f64::NAN;
        for key in keys {
            let gap = key - previous;
            if gap > 0.0 {
                self.total += 1;
                self.below += usize::from(gap < m);
                self.equal += usize::from(gap == m);
            }
            previous = key;
        }
        let holds = self.total == 0
            || (self.below <= self.total / 2 && self.total / 2 < self.below + self.equal);
        holds.then_some(self)
    }

    /// Whether every gap between a series' `n` samples is the median: an evenly spaced series (the held-state fuzz's coverage).
    #[cfg(test)]
    pub(crate) fn even(self, n: usize) -> bool {
        self.equal + 1 == n
    }
}

/// The only whole-series, data-derived state a smoother consumes. `NoState` means appends cannot rescale a held prefix; `Median` is time EMA/Triangular's decay unit or Savitzky–Golay's x scale.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SmoothingPlan {
    NoState,
    /// f64 bits of a gap between whole steps or milliseconds, at least 1, so the smoothers divide by it unguarded.
    Median(u64),
}

impl SmoothingPlan {
    /// The x unit a smoother measures distances in: the median gap, or 1 without one (step EMA and Triangular).
    fn unit(self) -> f64 {
        match self {
            Self::NoState => 1.0,
            Self::Median(bits) => f64::from_bits(bits),
        }
    }
}

/// Derive exactly the state the selected algorithm consumes, and a median plan's counts: no spacing work for step EMA/Triangular, otherwise the median gap ([`median_dx`]).
pub(crate) fn smoothing_plan(
    xs: &[f64],
    algo: Algorithm,
    step_sized_x: bool,
) -> (SmoothingPlan, MedianGaps) {
    match algo {
        Algorithm::None => (SmoothingPlan::NoState, MedianGaps::default()),
        Algorithm::Ema | Algorithm::Triangular if step_sized_x => {
            (SmoothingPlan::NoState, MedianGaps::default())
        }
        Algorithm::Ema | Algorithm::Triangular | Algorithm::SavitzkyGolay => {
            let (m, gaps) = median_dx(xs);
            (SmoothingPlan::Median(m.to_bits()), gaps)
        }
    }
}

#[cfg(test)]
thread_local! {
    static SPACING_DERIVATIONS: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

#[cfg(test)]
pub(crate) fn take_spacing_derivations() -> (usize, usize) {
    SPACING_DERIVATIONS.with(|counts| counts.replace((0, 0)))
}

/// What a causal smoother carries from one sample to the next: its moments, the last finite sample's x, and its `weight` and `base` words. A fit resumed from a recorded state takes the same arithmetic as the uninterrupted one, so held chart state (query/held.rs) replays from checkpoints bit for bit.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CausalState {
    mw: [f64; 5],
    sw: [f64; 3],
    prev_x: f64,
    /// Triangular's 1 + elapsed x since the first sample, in dx_ref units. EMA's count of distinct x, up to 3, a past at its u² mass's rounding counted as the last x: its fit's degree stays below it, as a Savitzky-Golay fit's does ([`biweight_fit_at`]).
    weight: f64,
    /// The value `sw` takes values relative to and the fit adds back: EMA's newest value folded, Triangular's first. A constant run's `sw` stays exactly zero, leaving no curvature of rounding for a read past its samples to amplify, and EMA's level cancels wherever values drift. Triangular sets it once, for its build cost: values that settle far from its first lose digits there, below a pixel (chart-request-latency.md, 2026-11-06).
    base: f64,
}

impl CausalState {
    pub(crate) const START: Self = Self {
        mw: [0.0; 5],
        sw: [0.0; 3],
        prev_x: f64::NAN,
        weight: 1.0,
        base: 0.0,
    };

    /// Re-base the past's `sw` to `yv` before a sample of value `yv` deposits its weight at the origin, which then adds nothing to `sw`: the shift is a change from sample to sample, so its rounding stays at the values' range, not their level.
    #[inline(always)]
    fn rebase(&mut self, yv: f64, terms: usize) {
        let shift = yv - self.base;
        for k in 0..terms {
            self.sw[k] -= shift * self.mw[k];
        }
        self.base = yv;
    }

    /// The state's bits, for exact comparison (a fresh state's `prev_x` is NaN).
    pub(crate) fn bits(&self) -> [u64; 11] {
        let ([a, b, c, d, e], [f, g, h]) = (self.mw, self.sw);
        let (x, w, base) = (self.prev_x, self.weight, self.base);
        [a, b, c, d, e, f, g, h, x, w, base].map(f64::to_bits)
    }
}

/// Held chart state keeps a causal smoother's state before every sample whose index is a multiple of this.
pub(crate) const CAUSAL_CHECKPOINT_STRIDE: usize = 512;

/// A smoothing width that may grow with x: `base`, or `growth`·(x + `shift`) where that is wider ([`Floor`]).
#[derive(Clone, Copy, Debug)]
pub(crate) struct Width {
    base: f64,
    growth: f64,
    shift: f64,
}

impl Width {
    const fn fixed(base: f64) -> Self {
        Self {
            base,
            growth: 0.0,
            shift: 0.0,
        }
    }

    pub(crate) fn at(self, x: f64) -> f64 {
        if self.growth == 0.0 {
            return self.base;
        }
        let grown = self.growth * (x + self.shift);
        #[cfg(test)]
        if grown > self.base {
            FLOOR_GROWN.with(|grown| grown.set(grown.get() + 1));
        }
        self.base.max(grown)
    }

    /// ∫ dt / (`unit`·width(t)) from `a` to `b` (growth > 0; none when b ≤ a): the e-folds an EMA with this time constant decays between them. Below the knee the width is `base`; above it growth·t, whose reciprocal integrates to a log.
    fn folds(self, a: f64, b: f64, unit: f64) -> f64 {
        let (a, b) = (a + self.shift, b + self.shift);
        let knee = self.base / self.growth;
        let flat = (b.min(knee) - a).max(0.0) / self.base;
        let lo = a.max(knee);
        let grown = if b > lo {
            ((b - lo) / lo).ln_1p() / self.growth
        } else {
            0.0
        };
        (flat + grown) / unit
    }
}

#[cfg(test)]
thread_local! {
    /// Widths read past where a log ladder's floor outgrew the user's: the differential test's coverage.
    pub(crate) static FLOOR_GROWN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The narrowest smoothing a chart that downsamples allows (NovelAI/kymo#16): a bucket width w(x), and a smoother's width at x reaches R·w(x), R = ∫K² of its kernel there, so each slot is about as smooth as its bucket's mean (`floored_slots_vary_as_their_bucket_means_do`). On a linear grid w is the bucket width; on the log ladder (x + shift)/s, s = 2^m its buckets per octave, which equals the ladder's bucket at each octave's start, is never below it and never steps at an octave. Both depend only on the grid's tier, so appends keep the floor until a re-tier, which answers in full. The default, zero, widens nothing ([`AxisDecision::floor`]).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Floor {
    width: f64,
    growth: f64,
    shift: f64,
}

impl Floor {
    fn of(grid: GridKind) -> Self {
        match grid {
            GridKind::Linear { width, .. } => Self {
                width,
                ..Self::default()
            },
            GridKind::Log { m, shift } => Self {
                width: 0.0,
                growth: pow2(-m),
                shift: if shift { 1.0 } else { 0.0 },
            },
        }
    }

    /// w(x): a floored smoother's width at x is at least R times this.
    pub(crate) fn at(self, x: f64) -> f64 {
        self.width.max(self.growth * (x + self.shift))
    }

    /// A smoother's width `user` (in `unit` x), widened to `r` times this floor.
    fn widen(self, user: f64, r: f64, unit: f64) -> Width {
        Width {
            base: user.max(r * self.width / unit),
            growth: r * self.growth / unit,
            shift: self.shift,
        }
    }
}

/// R = ∫K² of each Savitzky-Golay order's kernel (the biweight's equivalent kernel) at its center, which [`Floor`] widens its half-width by.
pub(crate) const SAVGOL_FLOOR_R: [f64; 3] = [5.0 / 7.0, 5.0 / 7.0, 805.0 / 572.0];
/// R = ∫K² of each EMA order's kernel at its current x, which [`Floor`] widens its time constant by. Triangular has no width.
pub(crate) const EMA_FLOOR_R: [f64; 3] = [0.5, 1.25, 33.0 / 16.0];

/// The widest floor of a step chart whose emitted x lie in `lo..=hi`: a narrower span only refines the grid (a finer linear width, a deeper log ladder), so this bounds the fetch margins a zoom's smoothers need. Zero at target 0, which never downsamples, and over a range that can hold no emitted x.
pub(crate) fn zoom_floor(lo: f64, hi: f64, spec: GridSpec) -> Floor {
    debug_assert!(spec.is_step_axis, "a step axis's ladder (shifted at 0)");
    // A log axis emits no x below 0, so the range is clamped there; one reaching 0 may hold it, and the ladder then runs in log(x + 1) from 0's cell, the widest floor such a zoom can need.
    let lo = if spec.log_buckets { lo.max(0.0) } else { lo };
    if spec.target == 0 || hi < lo {
        return Floor::default();
    }
    Floor::of(make_grid_from(lo, hi, spec).kind)
}

/// The axis decision over each series' emitted x, which [`shared_chart`] builds on and the delta planner compares. Prepare makes it once, for a smoothed step or time chart's [`Floor`] too.
pub(crate) enum AxisDecision {
    /// No plottable point.
    Empty,
    /// Every distinct x, each a slot.
    Passthrough(Vec<f64>),
    /// The grid and the chart's first and last x.
    Downsample(Grid, f64, f64),
}

impl AxisDecision {
    pub(crate) fn of(xs: &[&[f64]], spec: GridSpec) -> Self {
        let Some((g_first, g_last)) = axis_bounds(xs) else {
            return Self::Empty;
        };
        match decide_axis(xs, g_first, g_last, spec) {
            Decision::Passthrough(axis) => Self::Passthrough(axis),
            Decision::Downsample => {
                Self::Downsample(make_grid_from(g_first, g_last, spec), g_first, g_last)
            }
        }
    }

    /// The chart's floor: none when it passes every x through, its grid's when it downsamples (even should every cell ship raw).
    pub(crate) fn floor(&self) -> Floor {
        match self {
            Self::Downsample(grid, ..) => Floor::of(grid.kind),
            _ => Floor::default(),
        }
    }
}

/// A causal smoother with its parameters, folded sample by sample: [`Self::run`] fits at each sample (custom x), [`Self::fold_recording`] folds without fits for slots to read (step and time charts).
#[derive(Clone, Copy, Debug)]
pub(crate) enum CausalSmoother {
    /// One-sided exponentially weighted polynomial regression, evaluated at each current x. Moments are kept in coordinates whose origin moves with the current sample; translating the five scalar moments and solving the at-most 3x3 normal equations is constant work per point. The coordinate unit is the time constant τ (in dx_ref units), which keeps the moment matrix well-scaled at any τ. A τ that grows with x (a log ladder's [`Floor`]) weights a sample by exp(−∫dt/τ) to the current x, its unit following τ from sample to sample.
    Ema { tau: Width, dx_ref: f64, deg: usize },
    /// Causal triangular-weighted polynomial regression, evaluated at each
    /// current x. Where [`Self::Ema`] weights the past by a geometric decay in
    /// x, this ramps the weight up LINEARLY in x: a sample's weight is one plus
    /// its x-distance from the run's first sample, in dx_ref units. On a
    /// contiguous step grid that is 1, 2, 3, … (the newest point counts most),
    /// but a logging gap lifts the weight across it by the gap's width rather
    /// than by one. Every earlier point keeps a linearly-shrinking share instead
    /// of decaying away, so the window is the whole history so far and there is
    /// no bandwidth parameter.
    ///
    /// As in the EMA fit, moments are carried in coordinates whose origin tracks
    /// the current sample; here the coordinate UNIT also grows with the elapsed
    /// x, so the oldest point sits near u = −1 whatever the run length and the
    /// moment matrix stays well-scaled even at order 2. Rescaling the unit
    /// (u ↦ r·u) maps moment_k ↦ rᵏ·moment_k, folded into the same binomial
    /// origin shift [`Self::Ema`] uses. The fitted value is read at the current
    /// point (u = 0), which is unit-invariant, so the rescale changes only
    /// conditioning, not the result.
    ///
    /// Within a fixed dx_ref the output at a sample depends only on the samples up to it, so appending later points never moves an earlier value, as for EMA (the causal slot rule, [`SlotSmoother::dirty_from`]). Step/custom-x smoothing fixes dx_ref at one step; time smoothing uses the whole series' median interval, so an append that shifts the median rescales every output and the exact [`SmoothingPlan`] gate answers full.
    Triangular { dx_ref: f64, deg: usize },
}

impl CausalSmoother {
    /// Fold one finite sample at `x` into `st` and return the fitted value there with `FIT` (NaN without): one arithmetic for both, and each arm keeps its own fit (one fit after the match would make Triangular's run 60% longer). Inlined at each caller ([`Self::run`]'s loop among them), where a call would slow Triangular's builds by 8-28%.
    #[inline(always)]
    fn fold<const FIT: bool>(self, st: &mut CausalState, x: f64, yv: f64) -> f64 {
        let fit = |st: &CausalState, deg| {
            if FIT {
                st.base + fit_from_moments_at(&st.mw, &st.sw, deg, 0.0)
            } else {
                f64::NAN
            }
        };
        match self {
            Self::Ema { tau, dx_ref, deg } => {
                if st.prev_x.is_finite() {
                    let t = tau.at(x);
                    // In units of τ, old u becomes u − h at the new origin; past where τ grows beyond its base, r·u − h, r rescaling the unit from τ at the last sample to τ here. Each arm keeps its own call, so the plain one's r stays the literal 1 its powers fold away for.
                    let (r, h) = (tau.at(st.prev_x) / t, (x - st.prev_x) / dx_ref / t);
                    let decay = if t == tau.base {
                        (-h).exp()
                    } else {
                        (-tau.folds(st.prev_x, x, dx_ref)).exp()
                    };
                    if x != st.prev_x {
                        // At order 2, a past whose spread about the last x (its u² mass) is under 16ε of what the last x's samples put at the new origin sits at mw[2]'s rounding: it counts as that one x. That gives up quadratics mw[3] and mw[4] might still resolve, so that the rounding a heavy last x leaves in d2 does not pass as curvature (16 is a measured trade-off, not a bound).
                        let spread = st.mw[2] * r * r;
                        st.weight = (st.weight + 1.0).min(3.0);
                        if deg == 2 && spread < 16.0 * f64::EPSILON * st.mw[0] * h * h {
                            st.weight = 2.0;
                        }
                    }
                    if decay < f64::MIN_POSITIVE {
                        // Drop a weight decayed below the normal range: its moments, rounded to subnormals, lose the digits the degree cut counts on, and one underflowed to zero would make 0 * inf.
                        st.mw = [0.0; 5];
                        st.sw = [0.0; 3];
                        st.weight = 1.0;
                    } else if t == tau.base {
                        translate_moments(&mut st.mw, 2 * deg + 1, 1.0, h, decay);
                        translate_moments(&mut st.sw, deg + 1, 1.0, h, decay);
                    } else {
                        translate_moments(&mut st.mw, 2 * deg + 1, r, h, decay);
                        translate_moments(&mut st.sw, deg + 1, r, h, decay);
                    }
                }
                st.rebase(yv, deg + 1);
                st.mw[0] += 1.0;
                st.prev_x = x;
                fit(st, deg.min(st.weight as usize - 1))
            }
            Self::Triangular { dx_ref, deg } => {
                if st.prev_x.is_finite() {
                    let prev_weight = st.weight;
                    // Weight accumulates the x-distance covered (in dx_ref units), so
                    // a gap raises it by the gap's width, not by one.
                    st.weight += (x - st.prev_x) / dx_ref;
                    // One change of variables u ↦ r·u − h that both moves the origin
                    // to this sample and rescales the unit from prev_weight·dx_ref to
                    // weight·dx_ref, so u stays bounded as the elapsed x grows.
                    let r = prev_weight / st.weight;
                    let h = (x - st.prev_x) / (st.weight * dx_ref);
                    translate_moments(&mut st.mw, 2 * deg + 1, r, h, 1.0);
                    translate_moments(&mut st.sw, deg + 1, r, h, 1.0);
                } else {
                    // The first sample's value is the base.
                    st.base = yv;
                }
                st.mw[0] += st.weight;
                st.sw[0] += st.weight * (yv - st.base);
                st.prev_x = x;
                fit(st, deg)
            }
        }
    }

    /// Fold the finite samples of `parts` (consecutive runs of x and logged values, the first at index `from`) into `state` without their fits, recording the state before every index that is a multiple of [`CAUSAL_CHECKPOINT_STRIDE`] into `checkpoints`, if given. [`Self::run`] takes the same arithmetic.
    pub(crate) fn fold_recording(
        self,
        parts: &[(&[f64], &[f64])],
        state: &mut CausalState,
        mut from: usize,
        mut checkpoints: Option<&mut Vec<CausalState>>,
    ) {
        for &(xs, ys) in parts {
            for (&x, &yv) in xs.iter().zip(ys) {
                if from.is_multiple_of(CAUSAL_CHECKPOINT_STRIDE) {
                    if let Some(checkpoints) = checkpoints.as_mut() {
                        checkpoints.push(*state);
                    }
                }
                if yv.is_finite() {
                    self.fold::<false>(state, x, yv);
                }
                from += 1;
            }
        }
    }

    /// Fold the finite samples `from..to` into `st`, as [`Self::run`] does without its fits.
    pub(crate) fn fold_range(
        self,
        st: &mut CausalState,
        x: impl Fn(usize) -> f64,
        y: impl Fn(usize) -> f64,
        from: usize,
        to: usize,
    ) {
        for i in from..to {
            let yv = y(i);
            if yv.is_finite() {
                self.fold::<false>(st, x(i), yv);
            }
        }
    }

    /// The fit `st` holds, read at `x` (at or past its last sample) without folding a sample: its last fitted polynomial evaluated there, at `x`'s offset from that sample in the moments' unit (weights between samples keep their ratios, so neither EMA's decay nor Triangular's growing unit changes the fit). NaN before the first finite sample, which leaves no weight.
    pub(crate) fn value_at(self, st: &CausalState, x: f64) -> f64 {
        let (at, deg) = match self {
            Self::Ema { tau, dx_ref, deg } => (
                (x - st.prev_x) / dx_ref / tau.at(st.prev_x),
                deg.min(st.weight as usize - 1),
            ),
            Self::Triangular { dx_ref, deg } => ((x - st.prev_x) / (st.weight * dx_ref), deg),
        };
        st.base + fit_from_moments_at(&st.mw, &st.sw, deg, at)
    }

    /// Run the smoother over samples from `state` (the state before the first), leaving the state after the last: each finite sample's fit, NaN at the others.
    fn run(self, xs: &[f64], y: &[f64], state: &mut CausalState) -> Vec<f64> {
        let mut out = vec![f64::NAN; y.len()];
        for (i, &yv) in y.iter().enumerate() {
            if yv.is_finite() {
                out[i] = self.fold::<true>(state, xs[i], yv);
            }
        }
        out
    }
}

/// The first `terms` moments under the change of variables u ↦ r·u − h, scaled: moment_k ↦ scale · Σ_l C(k, l)·rˡ·(−h)^(k−l)·moment_l. With r = 1 the rescale factors are exactly 1.
#[inline(always)]
fn translate_moments<const N: usize>(
    moments: &mut [f64; N],
    terms: usize,
    r: f64,
    h: f64,
    scale: f64,
) {
    let old = *moments;
    for (k, moment) in moments.iter_mut().enumerate().take(terms) {
        let mut acc = 0.0;
        for (l, &m) in old.iter().enumerate().take(k + 1) {
            acc += binomial(k, l) * r.powi(l as i32) * (-h).powi((k - l) as i32) * m;
        }
        *moment = scale * acc;
    }
}

fn binomial(n: usize, k: usize) -> f64 {
    const C: [[f64; 5]; 5] = [
        [1.0, 0.0, 0.0, 0.0, 0.0],
        [1.0, 1.0, 0.0, 0.0, 0.0],
        [1.0, 2.0, 1.0, 0.0, 0.0],
        [1.0, 3.0, 3.0, 1.0, 0.0],
        [1.0, 4.0, 6.0, 4.0, 1.0],
    ];
    C[n][k]
}

/// Savitzky-Golay smoothing, biweight-windowed: each output point is the
/// center value of a local least-squares polynomial fit (degree =
/// poly_order, ≤ 2) over its surrounding window, weighted by the biweight
/// window w(x) = (1−x²)², x = offset/half-width — the n = 2 Landau kernel
/// (in kernel-smoothing terms, the quartic/biweight). Windows clip at the data
/// edges with the weights still centered on the output point, LOESS-style:
/// truncated boundary windows get real fits, so the fit's slope and
/// curvature terms keep boundary values anchored to the local trend
/// instead of smearing it like a plain weighted mean would.
///
/// Why this window: a uniform window (classical SG) lets the hard window
/// edge telegraph into the output — big points step in and out of the
/// sliding fit at full say, kinking the curve, and the boundary needs a
/// special regime whose seam shows on cliff-shaped data. The biweight
/// window tapers to zero with zero slope at the edge, so window entry and
/// exit are seamless and no boundary regime exists. And unlike a Gaussian
/// window — which admits no finite moment decomposition and would force
/// O(n·window) direct sums — it is a polynomial: weighted moments
/// are combinations of plain moments (Σw·xᵏ = mₖ − 2mₖ₊₂ + mₖ₊₄), so the
/// finite interior runs in O(n) off block-local prefix sums.
///
/// Numerically there is no sliding state to drift: every output is either a fresh direct fit or a difference of prefix sums rebuilt for each block of 2·half outputs. A window's center lies within half of its block's origin and its inputs within 2·half, so recentering amplifies rounding by roughly 3⁶ whatever n or the absolute step: about 2e-12 of the series' magnitude, far below f32 rounding except for values near zero. Longer blocks save little time for much more error (4·half: 12% faster, ~40× the error).
///
/// Non-finite values are excluded from every fit. The fit still produces a value
/// at a non-finite input position, but [`smooth_run_with_plan`] (custom x's entry
/// point) masks those positions back to NaN, so a hole in the input is
/// never filled in what reaches the wire.
#[allow(clippy::needless_range_loop)] // An output's index is its sample's, which its fit reads too.
fn savitzky_golay(y: &[f64], window: usize, poly_order: u32) -> Vec<f64> {
    let n = y.len();
    let deg = poly_order.min(2) as usize;
    let mut out = vec![f64::NAN; n];
    if n == 0 {
        return out;
    }
    let (half, h_s) = savgol_geometry(window);

    // Interior shortcut: on a full, finite window the odd weighted
    // moments vanish, so the fitted center value is a = α·S₀ʷ + β·S₂ʷ
    // with α = M₄ʷ/(M₀ʷM₄ʷ − M₂ʷ²), β = −M₂ʷ/(M₀ʷM₄ʷ − M₂ʷ²) constants of
    // the grid. For degree 0/1 the center value is the weighted mean.
    let (alpha, beta) = {
        let (mut mw0, mut mw2, mut mw4) = (1.0f64, 0.0f64, 0.0f64); // x = 0 term
        for d in 1..=half {
            let x2 = (d as f64 / h_s) * (d as f64 / h_s);
            let w = (1.0 - x2) * (1.0 - x2);
            mw0 += 2.0 * w;
            mw2 += 2.0 * w * x2;
            mw4 += 2.0 * w * x2 * x2;
        }
        if deg == 2 {
            let det = mw0 * mw4 - mw2 * mw2;
            (mw4 / det, -mw2 / det)
        } else {
            (1.0 / mw0, 0.0)
        }
    };

    // Non-finite prefix counts: O(1) per-window cleanliness check.
    let mut nonfinite_pfx = vec![0u32; n + 1];
    for (i, v) in y.iter().enumerate() {
        nonfinite_pfx[i + 1] = nonfinite_pfx[i] + u32::from(!v.is_finite());
    }

    // Clipped windows at the data edges: direct weighted fits.
    for c in 0..half.min(n) {
        out[c] = sg_fit_window(y, 0, (c + half + 1).min(n), c, h_s, deg);
    }
    for c in n.saturating_sub(half)..n {
        if c >= half {
            out[c] = sg_fit_window(y, c - half, n, c, h_s, deg);
        }
    }

    // Full-window interior: the even centered data moments Σxᵏy (k ≤ 6)
    // come from block prefix sums of u^k·y recentered onto each window
    // (binomials in the center offset d), then fold in the window:
    // Sₖʷ = Sₖ − 2Sₖ₊₂ + Sₖ₊₄ in half-width-scaled x. Windows containing
    // non-finite values divert to the direct path.
    let lo_int = half;
    let hi_int = n.saturating_sub(half);
    if lo_int >= hi_int {
        return out;
    }
    let cap = 4 * half + 1;
    let mut p0 = vec![0.0f64; cap];
    let mut p1 = vec![0.0f64; cap];
    let mut p2 = vec![0.0f64; cap];
    let mut p3 = vec![0.0f64; cap];
    let mut p4 = vec![0.0f64; cap];
    let mut p5 = vec![0.0f64; cap];
    let mut p6 = vec![0.0f64; cap];
    let h2 = h_s * h_s;
    let h4 = h2 * h2;
    let h6 = h4 * h2;
    // Blocks of 2·half outputs from lo_int.
    let mut block_start = lo_int;
    while block_start < hi_int {
        let block_end = (block_start + 2 * half).min(hi_int);
        let lo = block_start - half;
        let len = block_end + half - lo;
        let origin = (len / 2) as f64;
        for u in 0..len {
            let v = y[lo + u];
            let v = if v.is_finite() { v } else { 0.0 };
            let x = u as f64 - origin;
            let x2 = x * x;
            let x3 = x2 * x;
            p0[u + 1] = p0[u] + v;
            p1[u + 1] = p1[u] + x * v;
            p2[u + 1] = p2[u] + x2 * v;
            p3[u + 1] = p3[u] + x3 * v;
            p4[u + 1] = p4[u] + x2 * x2 * v;
            p5[u + 1] = p5[u] + x3 * x2 * v;
            p6[u + 1] = p6[u] + x3 * x3 * v;
        }
        for c in block_start..block_end {
            if nonfinite_pfx[c + half + 1] != nonfinite_pfx[c - half] {
                out[c] = sg_fit_window(y, c - half, c + half + 1, c, h_s, deg);
                continue;
            }
            let a = c - half - lo;
            let b = a + 2 * half + 1;
            let q0 = p0[b] - p0[a];
            let q1 = p1[b] - p1[a];
            let q2 = p2[b] - p2[a];
            let q3 = p3[b] - p3[a];
            let q4 = p4[b] - p4[a];
            let q5 = p5[b] - p5[a];
            let q6 = p6[b] - p6[a];
            // Even centered moments at offset d from the block origin.
            let d = (c - lo) as f64 - origin;
            let d2 = d * d;
            let d3 = d2 * d;
            let t2 = q2 - 2.0 * d * q1 + d2 * q0;
            let t4 = q4 - 4.0 * d * q3 + 6.0 * d2 * q2 - 4.0 * d3 * q1 + d2 * d2 * q0;
            let t6 = q6 - 6.0 * d * q5 + 15.0 * d2 * q4 - 20.0 * d3 * q3 + 15.0 * d2 * d2 * q2
                - 6.0 * d3 * d2 * q1
                + d3 * d3 * q0;
            let sw0 = q0 - 2.0 * t2 / h2 + t4 / h4;
            let sw2 = t2 / h2 - 2.0 * t4 / h4 + t6 / h6;
            out[c] = alpha * sw0 + beta * sw2;
        }
        block_start = block_end;
    }
    out
}

/// (half, taper scale) for a window, shared by [`savitzky_golay`] and [`savgol_half_width`]. Windows floor at 3, the smallest whose taper weights any neighbor (window 3: half 1 at scale 1.5).
///
/// The taper scale is window/2 in floating point, so windows 2k and 2k+1 differ; the support stays the integer `half`, so odd windows give their support-edge points a small positive weight.
fn savgol_geometry(window: usize) -> (usize, f64) {
    let window = window.max(3);
    (window / 2, window as f64 / 2.0)
}

/// Biweight-weighted least-squares fit over (u, x, y) points, x ascending and non-finite y excluded: u the offset from the weights' center in taper half-widths and x the coordinate the polynomial is fitted in, read at x = `at`, its values relative to `base`, which the fit adds back (a sample's value: a constant run's moments of y are then exactly zero, leaving no curvature of rounding for a read far from its samples to amplify). The degree stays below the weighted points' distinct x, past which a pivot is rounding noise that [`fit_from_moments_at`]'s cut can miss.
fn biweight_fit_at(
    points: impl Iterator<Item = (f64, f64, f64)> + Clone,
    deg: usize,
    at: f64,
    base: f64,
) -> f64 {
    let points = points.filter(|&(_, _, yv)| yv.is_finite());
    // The weighted points' distinct x, counted to one past the degree in a pass of its own, which keeps the count's branch out of the moments' loop.
    let (mut distinct, mut last) = (0usize, f64::NAN);
    for (u, x, _) in points.clone() {
        let w = 1.0 - u * u;
        if w * w > 0.0 && x != last {
            (distinct, last) = (distinct + 1, x);
            if distinct > deg {
                break;
            }
        }
    }
    // Weighted moments Σw·xᵏ (k ≤ 4) and Σw·xᵏ·(y − base) (k ≤ 2).
    let mut mw = [0.0f64; 5];
    let mut sw = [0.0f64; 3];
    for (u, x, yv) in points {
        let w = 1.0 - u * u;
        let mut pw = w * w;
        for k in 0..5 {
            mw[k] += pw;
            if k < 3 {
                sw[k] += pw * (yv - base);
            }
            pw *= x;
        }
    }
    base + fit_from_moments_at(&mw, &sw, deg.min(distinct.saturating_sub(1)), at)
}

/// Direct fit over y[lo..hi] with weights centered at `c`, returning the fitted value at `c`.
fn sg_fit_window(y: &[f64], lo: usize, hi: usize, c: usize, h_s: f64, deg: usize) -> f64 {
    let points = (lo..hi).map(|j| {
        let u = (j as f64 - c as f64) / h_s;
        (u, u, y[j])
    });
    biweight_fit_at(points, deg, 0.0, 0.0)
}

/// Fitted value at x = `at` of the weighted least-squares polynomial of degree ≤ `deg`, from `mw[k]` = Σw·xᵏ and `sw[k]` = Σw·xᵏ·y. It sums qₖ(at)·⟨qₖ, y⟩/Dₖ over the weight-orthogonal monic polynomials qₖ, Dₖ = ⟨qₖ, qₖ⟩ (an unpivoted LDLᵀ of the moment matrix), so each degree adds one term to the lower degree's arithmetic. Reading a fit away from the moments' origin keeps them where the samples are: a fit read far past its samples loses no digits to recentering the moments there.
///
/// Degrees are added while the pivot Dₖ exceeds 4ε·`mw[2k]`. Every term subtracted in forming Dₖ is at most `mw[2k]` (Cauchy–Schwarz), so below that Dₖ is rounding noise at any scale of x or the weights: the points numerically lie on k distinct x, which degree k − 1 already interpolates, so its value is the answer double precision can resolve. A small pivot above the cut is kept: its correction still carries digits. The cut does not see rounding accumulated before the pivot (in the moments, or carried in from D₁), so a noise pivot can occasionally pass. NaN without positive weight.
#[allow(clippy::neg_cmp_op_on_partial_ord)] // The negated comparisons also reject NaN pivots.
#[inline(always)]
fn fit_from_moments_at(mw: &[f64; 5], sw: &[f64; 3], deg: usize, at: f64) -> f64 {
    const TOL: f64 = 4.0 * f64::EPSILON;
    if !(mw[0] > 0.0) {
        return f64::NAN;
    }
    let mut fit = sw[0] / mw[0];
    if deg == 0 {
        return fit;
    }
    // q₁ = x − μ
    let mu = mw[1] / mw[0];
    let d1 = mw[2] - mu * mw[1];
    if !(d1 > TOL * mw[2]) {
        return fit;
    }
    let t1 = sw[1] - mu * sw[0];
    let q1 = at - mu;
    fit += q1 * t1 / d1;
    if deg == 1 {
        return fit;
    }
    // q₂ = x² − β·q₁ − m₂₀
    let m20 = mw[2] / mw[0];
    let g = mw[3] - mu * mw[2]; // ⟨x², q₁⟩
    let beta = g / d1;
    let d2 = mw[4] - m20 * mw[2] - beta * g;
    if !(d2 > TOL * mw[4]) {
        return fit;
    }
    let t2 = sw[2] - m20 * sw[0] - beta * t1;
    fit + (at * at - beta * q1 - m20) * t2 / d2
}

#[cfg(test)]
thread_local! {
    /// Samples the fits of [`savgol_at`] have read.
    pub(crate) static SAVGOL_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// A Savitzky–Golay window's half-width in x, at `gap` x per sample.
fn savgol_half_width(window: usize, gap: f64) -> f64 {
    savgol_geometry(window).1 * gap
}

/// What a series' Savitzky–Golay slots carry from one to the next ([`savgol_at`]): the last window's bounds, which ascending slots' windows search from, and the weights of an evenly spaced window symmetric about its slot (on its middle sample, or between its middle two). Its fit is linear in their values, with weights that depend only on the spacing, the half-width, the count and the degree, so it is one dot product, its weights computed again only when those change (a half-width growing with x would change them at every slot, so its windows fit directly). The cursor never changes a fit's bits: which way a window fits, and with which weights, depends on the window alone.
#[derive(Default)]
pub(crate) struct SavgolCursor {
    bounds: (usize, usize),
    /// Samples known evenly spaced: the spacing's bits and their index range.
    even: (u64, usize, usize),
    /// The spacing's and half-width's bits, the count and the degree `weights` are for.
    key: Option<(u64, u64, usize, usize)>,
    weights: Vec<f64>,
}

impl SavgolCursor {
    /// Whether the samples `lo..hi` are evenly spaced by `gap`, reading only the x past the run already known so, each once.
    fn evenly_spaced(&mut self, x: &impl Fn(usize) -> f64, lo: usize, hi: usize, gap: f64) -> bool {
        let (bits, mut start, mut end) = self.even;
        if bits != gap.to_bits() || lo < start || lo >= end {
            (start, end) = (lo, lo + 1);
        }
        let mut before = x(end - 1);
        let mut spaced = true;
        while end < hi {
            let at = x(end);
            if at - before != gap {
                spaced = false;
                break;
            }
            (before, end) = (at, end + 1);
        }
        self.even = (gap.to_bits(), start, end);
        spaced
    }

    fn weights(&mut self, gap: f64, h_x: f64, count: usize, deg: usize) -> &[f64] {
        let key = Some((gap.to_bits(), h_x.to_bits(), count, deg));
        if self.key != key {
            let middle = (count - 1) as f64 * 0.5;
            let u = |j: usize| (j as f64 - middle) * gap / h_x;
            let taper = |u: f64| (1.0 - u * u) * (1.0 - u * u);
            let mut mw = [0.0f64; 5];
            for u in (0..count).map(u) {
                let mut pw = taper(u);
                for m in &mut mw {
                    *m += pw;
                    pw *= u;
                }
            }
            // The fit is linear in Σw·uᵏ·y, through coefficients its moments alone decide (the degree cut included).
            let [a0, a1, a2] = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]
                .map(|unit| fit_from_moments_at(&mw, &unit, deg, 0.0));
            let weight = |u: f64| taper(u) * (a0 + u * (a1 + u * a2));
            self.weights.clear();
            self.weights.extend((0..count).map(u).map(weight));
            self.key = key;
        }
        &self.weights
    }
}

/// [`incremental::partition`] over `lo..hi`, searched outward from `from` in `lo..=hi`: a few probes when the answer lies near it.
fn partition_near(lo: usize, hi: usize, from: usize, pred: impl Fn(usize) -> bool) -> usize {
    if from > lo && !pred(from - 1) {
        return incremental::partition(lo, from - 1, pred);
    }
    let (mut a, mut b, mut step) = (from, from, 1);
    while b < hi && pred(b) {
        a = b + 1;
        b = (b + step).min(hi);
        step *= 2;
    }
    incremental::partition(a, b, pred)
}

/// The Savitzky–Golay fit centered at `xq`, which need not be a sample's x: the biweight fit of [`savitzky_golay`] with offsets in x, over the samples within the half-width `width` gives there (x ascending, read through `x`, their values through `y`; non-finite values excluded). NaN when no finite sample weighs (none in the window, or only at its edges). Past a series' end the window holds samples on one side only, then none. The polynomial is fitted about the middle of the samples that weigh more than about 1e-8 and read at `xq`, so a window whose samples sit far to one side keeps its digits. An evenly spaced window of finite samples symmetric about `xq` at a fixed half-width, as a passthrough chart's are (on a sample) and a linear grid's envelope slots' (between two), fits by `cursor`'s weights; one of 4 samples or fewer between two, whose edges may weigh nothing, fits directly.
pub(crate) fn savgol_at(
    x: &impl Fn(usize) -> f64,
    y: &impl Fn(usize) -> f64,
    n: usize,
    xq: f64,
    width: Width,
    deg: usize,
    cursor: &mut SavgolCursor,
) -> f64 {
    let h_x = width.at(xq);
    let lo = partition_near(0, n, cursor.bounds.0, |j| xq - x(j) > h_x);
    let hi = partition_near(lo, n, cursor.bounds.1.max(lo), |j| x(j) - xq <= h_x);
    cursor.bounds = (lo, hi);
    #[cfg(test)]
    SAVGOL_READS.with(|reads| reads.set(reads.get() + hi - lo));
    if lo == hi {
        return f64::NAN;
    }
    let count = hi - lo;
    let (first, last) = (x(lo), x(hi - 1));
    // `first < last`: samples all at one x (a stalled clock) take the direct fit, their mean.
    let centered = xq - first == last - xq && first < last && (count > 4 || count % 2 == 1);
    if h_x == width.base && centered {
        let gap = (last - first) / (count - 1) as f64;
        if cursor.evenly_spaced(x, lo, hi, gap) {
            // Sample j adds to sum j % 4, which keeps four adds in flight where one sum would wait on each.
            let mut sums = [0.0f64; 4];
            let mut add = |j: usize, w: f64, lane: usize| sums[lane] += w * y(lo + j);
            let (chunks, rest) = cursor.weights(gap, h_x, count, deg).as_chunks::<4>();
            for (c, weights) in chunks.iter().enumerate() {
                for (lane, &w) in weights.iter().enumerate() {
                    add(4 * c + lane, w, lane);
                }
            }
            for (lane, &w) in rest.iter().enumerate() {
                add(4 * chunks.len() + lane, w, lane);
            }
            let fit = (sums[0] + sums[1]) + (sums[2] + sums[3]);
            // The weights do not exclude a non-finite value; the direct fit below does.
            if fit.is_finite() {
                return fit;
            }
        }
    }
    // The first and last finite samples nearer `xq` than `within`.
    let ends = |within: f64| {
        let inside = |j: usize| (x(j) - xq).abs() < within && y(j).is_finite();
        let first = (lo..hi).find(|&j| inside(j))?;
        Some((first, (first..hi).rfind(|&j| inside(j))?))
    };
    // The direct fit runs over the samples that weigh: a sample at the window's edge weighs nothing (through the reciprocal it could keep a weight the degree cut would count), and a hole none. Its middle is that of the samples weighing more than about 1e-8, within 0.99995 of the half-width: one weighing less, first or last, would set it apart from the samples that carry the fit, where an order-2 fit loses its digits.
    let Some((a, b)) = ends(h_x) else {
        return f64::NAN;
    };
    let (ma, mb) = ends(0.99995 * h_x).unwrap_or((a, b));
    let (mid, scale) = ((x(ma) + x(mb)) * 0.5, 1.0 / h_x);
    // Values relative to the finite sample nearest `xq`, holes passed over, which weighs the most: a base that weighs next to nothing can swamp the rest (1e16 at the window's edge, beside values of 1, reads 0 where the fit reads 1.008).
    let near = incremental::partition(ma, mb + 1, |j| x(j) < xq);
    let finite = |j: usize| y(j).is_finite();
    let nearest = [
        (ma..near).rev().find(|&j| finite(j)),
        (near..=mb).find(|&j| finite(j)),
    ]
    .into_iter()
    .flatten()
    .min_by(|&i, &j| (x(i) - xq).abs().total_cmp(&(x(j) - xq).abs()))
    .unwrap_or(ma);
    let base = y(nearest);
    biweight_fit_at(
        (a..b + 1).map(|j| {
            let xj = x(j);
            ((xj - xq) * scale, (xj - mid) * scale, y(j))
        }),
        deg,
        (xq - mid) * scale,
        base,
    )
}

/// A smoothed series' smoother as its slots evaluate it: at the slot's x, over the fetched samples ([`bucket_run`], [`shared_passthrough`]).
#[derive(Clone, Copy, Debug)]
pub(crate) enum SlotSmoother {
    /// [`savgol_at`]'s half-width at the slot x, and degree.
    SavitzkyGolay { h_x: Width, deg: usize },
    /// The state after the last finite sample at or before the slot x, read there ([`CausalSmoother::value_at`]).
    Causal(CausalSmoother),
}

impl SlotSmoother {
    /// The slot smoother of a series smoothed under `plan`, at the user's widths; custom x builds it too, for its parameters ([`smooth_run_with_plan`]).
    pub(crate) fn new(
        algo: Algorithm,
        window: u32,
        time_constant: f64,
        poly_order: u32,
        plan: SmoothingPlan,
    ) -> Self {
        let (dx_ref, deg) = (plan.unit(), poly_order.min(2) as usize);
        match algo {
            Algorithm::None => unreachable!("an unsmoothed series has no smoother"),
            Algorithm::Ema => Self::Causal(CausalSmoother::Ema {
                tau: Width::fixed(time_constant),
                dx_ref,
                deg,
            }),
            Algorithm::Triangular => Self::Causal(CausalSmoother::Triangular { dx_ref, deg }),
            Algorithm::SavitzkyGolay => Self::SavitzkyGolay {
                h_x: Width::fixed(savgol_half_width(window as usize, dx_ref)),
                deg,
            },
        }
    }

    /// This smoother widened to a chart's [`Floor`]: Savitzky-Golay's half-width ([`SAVGOL_FLOOR_R`]) and EMA's time constant ([`EMA_FLOOR_R`]). Triangular has no width.
    pub(crate) fn floored(self, floor: Floor) -> Self {
        match self {
            Self::SavitzkyGolay { h_x, deg } => Self::SavitzkyGolay {
                h_x: floor.widen(h_x.base, SAVGOL_FLOOR_R[deg], 1.0),
                deg,
            },
            Self::Causal(CausalSmoother::Ema { tau, dx_ref, deg }) => {
                Self::Causal(CausalSmoother::Ema {
                    tau: floor.widen(tau.base, EMA_FLOOR_R[deg], dx_ref),
                    dx_ref,
                    deg,
                })
            }
            triangular => triangular,
        }
    }

    /// Whether the slot at `xq` has a value, `first` being the x of the series' first sample in the slot's cell: a causal smoother needs that sample at or before `xq`, else it has read none of the samples the slot shows (a series' first cell, or where it resumes after a gap, when other series center the cell earlier). A floored Savitzky-Golay window always reaches them.
    pub(crate) fn shows(self, xq: f64, first: f64) -> bool {
        !matches!(self, Self::Causal(_)) || first <= xq
    }

    /// The planners' slot rule ([`numeric_delta_from_col`]'s `slot_dirty`): the x from which slots may read a changed sample, given the first new fetched sample's x (+∞ without one). Causal: that x. Savitzky-Golay: that x less its half-width there, which never shrinks with x, so no slot below reaches the sample. The half-width scales with the plan's spacing, which the caller proves unchanged.
    pub(crate) fn dirty_from(self, first_new: Option<f64>) -> f64 {
        match (self, first_new) {
            (_, None) => f64::INFINITY,
            (Self::Causal(_), Some(x)) => x,
            (Self::SavitzkyGolay { h_x, .. }, Some(x)) => x - h_x.at(x),
        }
    }
}

/// One series' [`SlotSmoother`] over its fetched samples, evaluated at ascending slot x. The samples (x and logged value, x ascending, in fetched order) come in up to three runs read as one: fetched samples before a range of the series' emitted ones, that range, and those after it. A causal smoother folds them once, forward, from slot to slot, recording the checkpoints held state keeps when `record` asks ([`Self::finish`]).
pub(crate) struct SlotEvaluator<'a> {
    smoother: SlotSmoother,
    parts: [(&'a [f64], &'a [f64]); 3],
    /// A causal smoother's state before a fetched index, which the next slot continues.
    cursor: (usize, CausalState),
    recorded: Option<Vec<CausalState>>,
    savgol: SavgolCursor,
}

impl<'a> SlotEvaluator<'a> {
    pub(crate) fn new(
        smoother: SlotSmoother,
        parts: [(&'a [f64], &'a [f64]); 3],
        record: bool,
    ) -> Self {
        let n: usize = parts.iter().map(|part| part.0.len()).sum();
        let causal = matches!(smoother, SlotSmoother::Causal(_));
        Self {
            smoother,
            parts,
            cursor: (0, CausalState::START),
            // Sized to the checkpoints it records, which a vector grown by doubling would hold up to twice over.
            recorded: (record && causal)
                .then(|| Vec::with_capacity(n / CAUSAL_CHECKPOINT_STRIDE + 2)),
            savgol: SavgolCursor::default(),
        }
    }

    /// Over one run of samples, as the tests build them.
    #[cfg(test)]
    pub(crate) fn over(smoother: SlotSmoother, x: &'a [f64], y: &'a [f64]) -> Self {
        Self::new(smoother, [(&[], &[]), (x, y), (&[], &[])], false)
    }

    /// A recording causal smoother's checkpoints, its fold carried through the last fetched sample (a read past every sample), and last the state after that sample (at the fetched count); empty otherwise.
    pub(crate) fn finish(mut self) -> Vec<CausalState> {
        if self.recorded.is_none() {
            return Vec::new();
        }
        self.at(f64::INFINITY);
        let mut recorded = self.recorded.expect("recording, checked above");
        recorded.push(self.cursor.1);
        recorded
    }

    /// The smoother at `xq`.
    pub(crate) fn at(&mut self, xq: f64) -> f64 {
        let parts = self.parts;
        // Most series read one run (no fetched samples kept): index it directly.
        let mut runs = parts.iter().filter(|(x, _)| !x.is_empty());
        if let (Some(&(x, y)), None) = (runs.next(), runs.next()) {
            return self.at_in(xq, x.len(), |j| x[j], |j| y[j]);
        }
        let [before, own, after] = parts;
        let (b, o) = (before.0.len(), before.0.len() + own.0.len());
        let sample = |j: usize| {
            if j < b {
                (before.0[j], before.1[j])
            } else if j < o {
                (own.0[j - b], own.1[j - b])
            } else {
                (after.0[j - o], after.1[j - o])
            }
        };
        self.at_in(xq, o + after.0.len(), |j| sample(j).0, |j| sample(j).1)
    }

    /// [`Self::at`] over `n` samples read through `x` and `y`.
    #[inline(always)]
    fn at_in(
        &mut self,
        xq: f64,
        n: usize,
        x: impl Fn(usize) -> f64,
        y: impl Fn(usize) -> f64,
    ) -> f64 {
        match self.smoother {
            SlotSmoother::SavitzkyGolay { h_x, deg } => {
                savgol_at(&x, &y, n, xq, h_x, deg, &mut self.savgol)
            }
            SlotSmoother::Causal(smoother) => {
                let (from, state) = &mut self.cursor;
                debug_assert!(*from == 0 || x(*from - 1) <= xq, "slots ascend");
                let past = |j: usize| j == n || x(j) > xq;
                if past(*from) {
                    // No sample since the last slot.
                } else if past(*from + 1) {
                    // One, as on a passthrough chart: folded as `fold_recording` would, without the search and slicing that would double such a chart's cost.
                    if from.is_multiple_of(CAUSAL_CHECKPOINT_STRIDE) {
                        if let Some(recorded) = self.recorded.as_mut() {
                            recorded.push(*state);
                        }
                    }
                    let yv = y(*from);
                    if yv.is_finite() {
                        smoother.fold::<false>(state, x(*from), yv);
                    }
                    *from += 1;
                } else {
                    let to = partition_near(*from, n, *from, |j| x(j) <= xq);
                    let parts = parts_between(self.parts, *from, to);
                    smoother.fold_recording(&parts, state, *from, self.recorded.as_mut());
                    *from = to;
                }
                smoother.value_at(state, xq)
            }
        }
    }
}

/// The samples `from..to` of runs read as one sequence.
fn parts_between<'a>(
    parts: [(&'a [f64], &'a [f64]); 3],
    from: usize,
    to: usize,
) -> [(&'a [f64], &'a [f64]); 3] {
    let mut start = 0;
    parts.map(|(x, y)| {
        let at = |i: usize| i.clamp(start, start + x.len()) - start;
        let (lo, hi) = (at(from), at(to));
        start += x.len();
        (&x[lo..hi], &y[lo..hi])
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One window's fit through the direct path, as ground truth for the
    /// prefix-sum interior.
    fn direct(y: &[f64], c: usize, window: usize) -> f64 {
        let (half, h_s) = savgol_geometry(window);
        let lo = c.saturating_sub(half);
        let hi = (c + half + 1).min(y.len());
        sg_fit_window(y, lo, hi, c, h_s, 2)
    }

    /// Reference values from an independent implementation (numpy weighted
    /// lstsq: biweight (1−x²)² weights, degree 2, clipped windows), for
    /// window 11 → half 5.
    #[test]
    fn sg_matches_weighted_lstsq_reference() {
        let y = [
            101.7,
            79.5730753078,
            67.9320046036,
            53.7811636094,
            47.7328964117,
            36.1879441171,
            31.3194211912,
            22.7596963942,
            20.5896517995,
            18.6298888222,
            12.1335283237,
            11.8803158362,
            6.4717953289,
            8.9273578214,
            5.7810062625,
            7.3787068368,
            2.2762203978,
            4.037326996,
            1.5323722447,
            4.1370771856,
        ];
        let want = [
            100.9599164269,
            82.0610707712,
            67.3408877938,
            55.4505408249,
            45.4620877355,
            37.1582920283,
            30.2796251719,
            24.7073096893,
            20.2824090532,
            16.6320984363,
            13.4545848889,
            10.7722752802,
            8.7697423503,
            7.4168256312,
            6.4323994197,
            5.3117879273,
            4.2145420455,
            3.1659912603,
            2.820336942,
            3.8074687553,
        ];
        let out = savitzky_golay(&y, 11, 2);
        for i in 0..y.len() {
            assert!(
                (out[i] - want[i]).abs() < 1e-6,
                "i={i}: {} vs {}",
                out[i],
                want[i]
            );
        }
    }

    /// SG of an exact quadratic must return it unchanged everywhere —
    /// truncated boundary windows included (a weighted least-squares fit
    /// reproduces polynomials up to its degree exactly, whatever the
    /// weights).
    #[test]
    fn sg_reproduces_quadratic_exactly_including_boundaries() {
        let y: Vec<f64> = (0..30)
            .map(|i| {
                let x = i as f64;
                3.0 - 2.0 * x + 0.5 * x * x
            })
            .collect();
        let out = savitzky_golay(&y, 11, 2);
        for (i, (&a, &b)) in out.iter().zip(&y).enumerate() {
            assert!((a - b).abs() < 1e-7, "i={i}: {a} vs {b}");
        }
    }

    /// The prefix-sum interior must agree with the direct fit across a
    /// long series, including NaN windows (which divert to the direct
    /// path), block seams, and the clipped boundaries. NaNs fill only the first half, so every window size also runs NaN-free prefix sums.
    #[test]
    fn sg_prefix_matches_direct_fits() {
        let n = 5000usize;
        let mut y: Vec<f64> = (0..n)
            .map(|i| {
                let x = i as f64;
                (x * 0.01).sin() * 50.0 + ((i * 7919) % 100) as f64 / 7.0
            })
            .collect();
        for i in (37..n / 2).step_by(97) {
            y[i] = f64::NAN;
        }
        for window in [3, 4, 5, 8, 16, 30, 64, 130] {
            let out = savitzky_golay(&y, window, 2);
            for (c, &actual) in out.iter().enumerate() {
                let want = direct(&y, c, window);
                assert!(
                    (actual - want).abs() < 1e-9,
                    "window={window}, c={c}: {actual} vs {want}"
                );
            }
        }
    }

    /// Windows 3 and 4 have different taper weights, so they differ at orders 0–1 (at order 2 both interpolate their three weighted points).
    #[test]
    fn sg_windows_3_and_4() {
        let y: Vec<f64> = (0..200).map(|i| ((i * 7919) % 1000) as f64).collect();
        for order in 0..=1 {
            let w3 = savitzky_golay(&y, 3, order);
            let w4 = savitzky_golay(&y, 4, order);
            assert!(w3.iter().zip(&w4).any(|(a, b)| a != b), "order {order}");
        }
    }

    /// Two weighted points fit a line, so order 2 returns exactly order 1's result: the direct SG fit and the EMA by their distinct x ([`biweight_fit_at`], [`CausalState`]'s count), Triangular by the pivot cut (its quadratic's pivot at rounding level, below it in this fixture).
    #[test]
    fn underdetermined_order_2_matches_order_1() {
        let (xs, y) = ([0.0, 1.0], [7.0, 9.0]);
        let fits = |order| {
            [
                savitzky_golay(&y, 4, order),
                CausalSmoother::Ema {
                    tau: Width::fixed(10.0),
                    dx_ref: 1.0,
                    deg: order as usize,
                }
                .run(&xs, &y, &mut { CausalState::START }),
                CausalSmoother::Triangular {
                    dx_ref: 1.0,
                    deg: order as usize,
                }
                .run(&xs, &y, &mut { CausalState::START }),
            ]
        };
        assert_eq!(fits(2), fits(1));
    }

    /// The degree switch is scale-free: three early samples determine a quadratic, which interpolates the newest one, at each tested time constant up to τ = 1e6, where they span a millionth of the EMA's unit.
    #[test]
    fn early_ema_samples_fit_full_degree() {
        let (xs, y) = ([0.0, 1.0, 2.0], [0.0, 0.0, 1.0]);
        for tau in [1e6, 1e3, 2.0] {
            let out = CausalSmoother::Ema {
                tau: Width::fixed(tau),
                dx_ref: 1.0,
                deg: 2,
            }
            .run(&xs, &y, &mut { CausalState::START });
            assert!((out[2] - 1.0).abs() < 1e-9, "tau={tau}: {}", out[2]);
        }
    }

    /// A window wider than the series stays well-defined: every window is
    /// clipped, every output matches its direct fit.
    #[test]
    fn sg_window_longer_than_data() {
        let y: Vec<f64> = (0..15)
            .map(|i| 100.0 * (-(i as f64) / 4.0).exp() + (i % 3) as f64)
            .collect();
        let out = savitzky_golay(&y, 100, 2);
        for (c, &v) in out.iter().enumerate() {
            let want = direct(&y, c, 100);
            assert!((v - want).abs() < 1e-9, "c={c}: {v} vs {want}");
        }
    }

    /// NaN inputs are excluded from fits and the fit stays accurate around
    /// them. The values produced AT the NaN positions are internal only —
    /// smooth_run masks them back to NaN before anything reaches the wire.
    #[test]
    fn sg_fills_nan_holes() {
        let mut y: Vec<f64> = (0..30)
            .map(|i| {
                let x = i as f64;
                3.0 - 2.0 * x + 0.5 * x * x
            })
            .collect();
        let exact = y.clone();
        y[0] = f64::NAN; // boundary hole
        y[14] = f64::NAN; // interior hole
        let out = savitzky_golay(&y, 11, 2);
        for (i, &v) in out.iter().enumerate() {
            assert!(v.is_finite(), "i={i} not filled");
            assert!((v - exact[i]).abs() < 1e-7, "i={i}: {v} vs {}", exact[i]);
        }
    }

    /// Stress the prefix path's cancellation: a huge common offset is the
    /// worst case for moment differences. Parity with the direct fit must
    /// hold to ~1e-9 relative.
    #[test]
    fn sg_stable_with_large_offset() {
        let n = 3000usize;
        let y: Vec<f64> = (0..n)
            .map(|i| 1e9 + (i as f64 * 0.05).sin() * 50.0)
            .collect();
        let out = savitzky_golay(&y, 40, 2);
        for (c, &actual) in out.iter().enumerate() {
            let want = direct(&y, c, 40);
            let rel = ((actual - want) / want).abs();
            assert!(rel < 1e-9, "c={c}: {actual} vs {want}");
        }
    }

    #[test]
    fn sg_handles_tiny_inputs() {
        assert!(savitzky_golay(&[], 5, 2).is_empty());
        let one = savitzky_golay(&[7.0], 5, 2);
        assert_eq!(one.len(), 1);
        assert!((one[0] - 7.0).abs() < 1e-12);
        let two = savitzky_golay(&[7.0, 9.0], 5, 2);
        assert_eq!(two.len(), 2);
        assert!(two.iter().all(|v| v.is_finite()));
        let all_nan = savitzky_golay(&[f64::NAN, f64::NAN, f64::NAN, f64::NAN], 5, 2);
        assert!(all_nan.iter().all(|v| v.is_nan()));
    }
}

/// Bucket grid for a chart span: width from the power-of-two ladder
/// (smallest 2^k with span/2^k ≤ target), boundaries anchored at x = 0.
/// Returns the grid-aligned origin, exact power-of-two width, and bucket count.
/// The last bucket always owns the chart's maximum x, including when it lies
/// exactly on a boundary. Keeping width explicit matters when the absolute
/// origin is so large relative to the span that `origin + width` rounds back
/// to `origin`; an f64 upper boundary cannot represent that half-open range,
/// but local `(x - origin) / width` cell offsets still can.
///
/// The grid is a pure function of (span tier, target) with absolutely
/// anchored boundaries, so it is stable in every direction that used to
/// invalidate everything: a run appending points extends the grid with
/// new buckets instead of shifting all boundaries; a short run joining
/// the chart can't drag x_lo under everyone else's buckets; and a zoom
/// reverted to a previous range reproduces the identical grid. Unchanged
/// series therefore produce byte-identical responses, which the client's
/// fingerprint check turns into skipped redraws. The power-of-two ladder
/// (rather than an arbitrary multiple) is what makes tiers canonical:
/// the width for a span is history-independent, every tier's boundaries
/// nest inside the next coarser tier's, and the widths are exact in f64.
///
/// Bucket count lands in (target/2, target] plus up to 2 from edge
/// alignment — never more than the chart has pixels to show.
#[allow(clippy::neg_cmp_op_on_partial_ord)] // `!(span > 0)` intentionally rejects NaN spans too.
fn stable_grid(x_lo: f64, x_hi: f64, target: usize) -> (f64, f64, usize) {
    if target == 0 {
        return (x_lo, 0.0, 0);
    }
    let span = x_hi - x_lo;
    if !(span > 0.0) || !x_lo.is_finite() {
        // Empty chart or a single x position: bucket math degenerates to
        // one bucket downstream (width 0); pass through unchanged.
        return (x_lo, 0.0, target);
    }
    let width = (span / target as f64).log2().ceil().exp2();
    let lo = (x_lo / width).floor() * width;
    // Subtract first: x_hi and lo are close enough for exact cancellation
    // (Sterbenz), whereas floor(x_hi / width) can exceed 2^53 and lose the
    // `+1` that makes an exact-boundary maximum own the following cell.
    let n = ((x_hi - lo) / width).floor() as usize + 1;
    debug_assert!(n <= target + 2, "aligned grid exceeded its bucket bound");
    (lo, width, n)
}

#[cfg(test)]
mod stable_grid_tests {
    use super::stable_grid;

    #[test]
    fn boundaries_are_anchored_multiples_of_width() {
        let (lo, w, n) = stable_grid(137.0, 9731.0, 1000);
        assert!(w.log2().fract() == 0.0, "width {w} not a power of two");
        assert_eq!(lo / w, (lo / w).round());
        let hi = lo + w * n as f64;
        assert_eq!(hi / w, (hi / w).round());
        assert!(n <= 1002 && n > 500, "n={n}");
        assert!(lo <= 137.0 && hi > 9731.0);
    }

    #[test]
    fn upper_boundary_gets_its_own_cell() {
        let (lo, width, n) = stable_grid(0.0, 1200.0, 400);
        assert_eq!(width, 4.0);
        assert_eq!(lo + width * n as f64, 1204.0);
        assert_eq!(((1200.0 - lo) / width) as usize, n - 1);
    }

    #[test]
    fn huge_origin_tiny_span_still_owns_its_maximum() {
        let x_lo = 1_000_000_000.0f64;
        // 420 representable steps makes width = 2^-24 and x_hi / width >
        // 2^53. In the old upper-bound formula, quotient + 1 rounded back to
        // quotient and omitted x_hi's cell.
        let x_hi = f64::from_bits(x_lo.to_bits() + 420);
        let (lo, width, n) = stable_grid(x_lo, x_hi, 1000);
        assert!(x_hi / width >= 2f64.powi(53));
        assert_eq!(width, 2f64.powi(-24));
        assert_eq!(
            ((x_hi / width).floor() + 1.0) * width,
            x_hi,
            "the absolute-quotient upper bound cannot advance one cell"
        );
        assert!((((x_hi - lo) / width) as usize) < n);
    }

    #[test]
    fn appends_extend_without_shifting() {
        // Growing x_hi within the same tier must keep lo and width fixed.
        let (lo1, w1, n1) = stable_grid(0.0, 6000.0, 1000);
        let (lo2, w2, n2) = stable_grid(0.0, 6500.0, 1000);
        assert_eq!(w1, w2, "same tier, same width");
        assert_eq!(lo1, lo2, "anchor must not move");
        assert!(n2 >= n1, "grid extends right");
        // Crossing the tier (span > target * width) doubles the width.
        let (lo3, w3, _) = stable_grid(0.0, 9000.0, 1000);
        assert_eq!(w3, 2.0 * w1);
        assert_eq!(lo3, 0.0);
    }

    #[test]
    fn zoom_revert_reproduces_identical_grid() {
        let full = stable_grid(0.0, 100_000.0, 1000);
        let zoomed = stable_grid(20_480.0, 24_576.0, 1000);
        let reverted = stable_grid(0.0, 100_000.0, 1000);
        assert_eq!(full, reverted);
        // and the zoom tier's boundaries nest inside the full tier's
        let wz = zoomed.1;
        let wf = full.1;
        assert_eq!((wf / wz).log2().fract(), 0.0, "tiers must nest");
    }

    #[test]
    fn degenerate_inputs_pass_through() {
        assert_eq!(stable_grid(5.0, 5.0, 1000), (5.0, 0.0, 1000));
        assert_eq!(stable_grid(0.0, 100.0, 0), (0.0, 0.0, 0));
        let (lo, width, n) = stable_grid(f64::INFINITY, f64::NEG_INFINITY, 1000);
        assert!(lo.is_infinite() && width == 0.0 && n == 1000);
    }

    #[test]
    fn sub_unit_spans_get_fractional_power_widths() {
        // custom-x axes (e.g. learning rate) live well below 1.0
        let (lo, w, n) = stable_grid(1e-4, 5e-4, 100);
        assert!(w.log2().fract() == 0.0);
        assert!(n > 0 && lo <= 1e-4 && lo + w * n as f64 >= 5e-4);
    }
}

// === Shared-axis bucketing ===
//
// Implements docs/chart-shared-axis.md + docs/log-scale-buckets.md. Every run — step, timestamp, or custom-x — emits on ONE shared slot sequence. Slots mix two kinds: an ENVELOPE slot is a downsampling bucket (x = average of its first and last point across runs; y = per-run min/max envelope, dense, min == max behind a single point), a RAW slot is one exact logged x shared by every run that logged it. A bucket downsamples only when it EXPECTS at least [`ENV_MIN_POINTS`] points — below that its points ship raw (two exact points render better than one envelope). Few enough distinct x skips the grid entirely (passthrough: every slot raw); a grid with no occupied envelope bucket emits the same shape. Buckets are disjoint and every point, including the chart's first and last, follows its cell's ordinary raw-or-envelope rule. Works on any ascending f64 x, shifted-positive on the log ladder ([`GridKind::Log`]) — custom-x is just a non-step x metric (its unplottable-x markers ride in via `xnan`).

use std::borrow::Cow;
use std::ops::ControlFlow;

use crate::chart_delta::{DenseChart, DenseSeries};

pub(crate) mod incremental;

/// A bucket downsamples only if it EXPECTS at least this many points; fewer ship raw (docs/log-scale-buckets.md: "a bucket with only 2 points should just be two non-downsampled points"). Step axes expect one point per integer step, so the test is width >= 4 — grid-pure, deliberately blind to missing points; non-step axes use the bucket's distinct-x count instead. Both are monotone against a FIXED threshold, which lets the delta planner compare the exact verified held structure with the current structure.
const ENV_MIN_POINTS: f64 = 4.0;

/// Octaves below this clamp share the bottom log-ladder cell (degenerate custom-x under 2^-500 merges — never drops), keeping every cell width an exact power of two clear of the subnormal range.
const OCTAVE_MIN: i32 = -500;

/// floor(log2 x) of a positive x, from the exponent bits — exact at the 2^k boundaries, where a floating log2().floor() can land on either side.
fn octave(x: f64) -> i32 {
    debug_assert!(x > 0.0);
    let e = ((x.to_bits() >> 52) & 0x7ff) as i32;
    (e - 1023).max(OCTAVE_MIN)
}

/// 2^k by exponent construction, exact and ~6x cheaper than powi — [`Grid::id`] runs twice per SAMPLE during planning and bucketing. Valid for normal exponents only; the grid's k stays within [OCTAVE_MIN - m, 1023].
fn pow2(k: i32) -> f64 {
    debug_assert!((-1022..=1023).contains(&k));
    f64::from_bits(((k + 1023) as u64) << 52)
}

/// Bucket grid of a downsampling chart. Cell ids are ABSOLUTE and consecutive integers, so appends extend the id range without moving any boundary — the stability the delta planner needs — and the instantiated window [base, base+cells) indexes plain vectors.
#[derive(Debug, Clone, Copy, PartialEq)]
enum GridKind {
    /// Uniform power-of-two width anchored at x = 0 ([`stable_grid`]); id = the bucket ordinal from the anchor.
    Linear { x_lo: f64, width: f64 },
    /// The log ladder: octave [2^k, 2^(k+1)) splits into s = 2^m buckets of width 2^(k−m) — [16,20),[20,24),[24,28),[28,32),[32,40) is m = 2 (docs/log-scale-buckets.md). id = k·s + j, consecutive across octave boundaries. `shift` = the ladder runs in log(x+1) (step/timestamp axes, where step 0 must render; the +1 is exact on their integer x). Every x reaching the ladder is shifted-positive: query.rs makes an unplottable x an exceptional-point marker, and [`make_grid_from`] asserts it of the chart's first x.
    Log { m: i32, shift: bool },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Grid {
    kind: GridKind,
    base: i64,
    cells: usize,
}

impl Grid {
    /// Absolute id of the cell containing `x`, which on the log ladder must be shifted-positive ([`GridKind::Log`]). Exact: within an octave x − 2^k is exact (Sterbenz), and every width is a power of two, so j < s; only an x below [`OCTAVE_MIN`]'s octave gives j < 0, joining its bottom cell.
    fn id(kind: GridKind, x: f64) -> i64 {
        match kind {
            GridKind::Linear { x_lo, width } => ((x - x_lo) / width) as i64,
            GridKind::Log { m, shift } => {
                let xe = if shift { x + 1.0 } else { x };
                let k = octave(xe);
                let s = 1i64 << m;
                let j = (((xe - pow2(k)) / pow2(k - m)) as i64).max(0);
                k as i64 * s + j
            }
        }
    }

    /// Instantiated index of the cell containing `x`. An `x` outside the instantiated cells is a caller bug, which debug builds assert.
    fn index(&self, x: f64) -> usize {
        let id = Self::id(self.kind, x);
        let offset = id - self.base;
        debug_assert!(
            (0..self.cells as i64).contains(&offset),
            "point x={x} mapped outside its instantiated grid: id={id}, base={}, cells={}",
            self.base,
            self.cells
        );
        offset as usize
    }

    /// Bucket width of an instantiated cell — the step-axis point expectation (one point per integer).
    fn width(&self, i: usize) -> f64 {
        match self.kind {
            GridKind::Linear { width, .. } => width,
            GridKind::Log { m, .. } => {
                let k = (self.base + i as i64).div_euclid(1i64 << m) as i32;
                pow2(k - m)
            }
        }
    }
}

/// Request-derived axis geometry, shared by the chart build ([`shared_chart`]) and the delta planner ([`numeric_delta_from_col`]) so they cannot diverge.
#[derive(Clone, Copy, Debug)]
pub struct GridSpec {
    /// Downsample resolution (0 = never downsample).
    pub target: usize,
    /// x values are integer steps (strictly ascending, unique per run) — unlocks the cheaper decision paths and the width-based bucket-mode rule.
    pub is_step_axis: bool,
    /// The panel renders a log x axis: bucket on the log ladder.
    pub log_buckets: bool,
    /// The log ladder MAY run in log(x+1): step/timestamp axes only (the shift is exact on their integer x; custom-x keeps plain log(x) — +1 would crush sub-1 domains like learning rates). Engaged only when the chart actually contains x = 0 (docs/log-scale-buckets.md: "when step 0 is present") — g_first == 0, encoded in the grid kind the delta planner compares and read from the same model by the client, so all sides agree. Always false when !log_buckets.
    pub shift_one: bool,
}

/// The grid for a chart that decided to downsample, its x spanning `g_first..=g_last`. Linear: [`stable_grid`]. Log ladder: the largest subdivision s = 2^m whose cell count over the span stays within target — ids are consecutive, so the count is closed-form, no data scanned; a sub-octave zoom therefore scales s up to match linear slot density.
fn make_grid_from(g_first: f64, g_last: f64, spec: GridSpec) -> Grid {
    if spec.log_buckets {
        let shift = spec.shift_one && g_first == 0.0;
        // An x at or below the shifted zero would anchor the ladder at its magnitude, bucketing the whole chart wrong: it fails the build instead (no chart sends one, [`GridKind::Log`]).
        assert!(
            g_first > 0.0 || shift,
            "log ladder from x = {g_first}, at or below its shifted zero"
        );
        let grid_for = |m: i32| {
            let kind = GridKind::Log { m, shift };
            let base = Grid::id(kind, g_first);
            let cells = (Grid::id(kind, g_last) - base + 1) as usize;
            Grid { kind, base, cells }
        };
        // m caps at 40: far past any pixel target, and cell counts stay in i64. m = 0 can still exceed target (a span of more octaves than pixels) — the octave count bounds the excess at ~1500 cells.
        let mut m = 0;
        while m < 40 && grid_for(m + 1).cells <= spec.target {
            m += 1;
        }
        return grid_for(m);
    }
    let (x_lo, width, buckets) = stable_grid(g_first, g_last, spec.target);
    Grid {
        kind: GridKind::Linear { x_lo, width },
        base: 0,
        cells: buckets,
    }
}

/// Linear interpolation over ascending finite (x, y) pairs, extrapolating past either end along the nearest segment. Custom x only: it smooths in step order, so a slot has no smoother to evaluate. Ascending queries carry just their current segment, reading each point once.
fn lerp_from(points: impl Iterator<Item = (f64, f64)>) -> impl FnMut(f64) -> f64 {
    let mut points = points.fuse();
    let mut segment = points
        .next()
        .map(|first| (first, points.next().unwrap_or(first)));
    move |xq| {
        let Some((left, right)) = &mut segment else {
            return f64::NAN;
        };
        while right.0 < xq {
            let Some(next) = points.next() else {
                break;
            };
            (*left, *right) = (*right, next);
        }
        let ((x0, y0), (x1, y1)) = (*left, *right);
        if x1 > x0 {
            y0 + (y1 - y0) * (xq - x0) / (x1 - x0)
        } else {
            y0
        }
    }
}

/// Whether the chart downsamples, and — when it doesn't — the distinct-x axis
/// every series aligns to. `Passthrough` carries the axis so it's never built
/// twice: it IS the decision's work, completed.
enum Decision {
    Downsample,
    Passthrough(Vec<f64>),
}

/// Global finite x bounds across ascending runs; empty when no run has points.
fn axis_bounds(xs: &[&[f64]]) -> Option<(f64, f64)> {
    let (mut first, mut last) = (f64::INFINITY, f64::NEG_INFINITY);
    for run in xs {
        if let Some(&x) = run.first() {
            first = first.min(x);
        }
        if let Some(&x) = run.last() {
            last = last.max(x);
        }
    }
    (first.is_finite() && last.is_finite()).then_some((first, last))
}

/// All distinct x positions, ascending — the plain sort+dedup of every run's
/// points. Only for a total within the cap (any total at target 0); the
/// size-bounded paths below avoid it.
fn sorted_distinct(xs: &[&[f64]]) -> Vec<f64> {
    let mut u: Vec<f64> = xs.iter().flat_map(|x| x.iter().copied()).collect();
    u.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    u.dedup();
    u
}

/// Decide downsample vs passthrough WITHOUT sorting all N points, and on
/// passthrough return the axis as a by-product. The threshold is the raw
/// TARGET — fixed per request, never the span-derived bucket count: fixed
/// thresholds keep the decision monotone in the sample set, the property the
/// delta planner's held/current structural comparison uses. Paths:
///   - target 0 (no downsampling asked) has no cap, so every path below ships
///     every distinct point;
///   - a linear STEP grid whose width falls under ENV_MIN_POINTS also passes
///     through: every bucket would ship its points raw anyway ("2 points beat
///     1 envelope bucket"), and that IS the distinct axis — at most ~2×target
///     slots (width < 4 means span ≤ 2·target);
///   - step axis with contiguous runs decides from interval endpoints alone
///     ([`decide_step`], O(runs)) — no points touched;
///   - everything else counts distinct with an early-exit at `target+1`
///     ([`decide_general`]) — large N means distinct ≫ target, so it bails fast.
fn decide_axis(xs: &[&[f64]], g_first: f64, g_last: f64, spec: GridSpec) -> Decision {
    let cap = decision_cap(g_first, g_last, spec);
    if spec.is_step_axis {
        if let Some(d) = decide_step(xs, cap) {
            return d;
        }
    }
    decide_general(xs, cap)
}

/// The distinct-x count above which [`decide_axis`] downsamples a chart spanning [g_first, g_last]: every path decides exactly `distinct > cap`.
fn decision_cap(g_first: f64, g_last: f64, spec: GridSpec) -> usize {
    // target 0 = "never downsample". A single x position (one distinct x) passes every other cap.
    if spec.target == 0 {
        return usize::MAX;
    }
    if spec.is_step_axis && !spec.log_buckets {
        let (_, width, _) = stable_grid(g_first, g_last, spec.target);
        if width >= ENV_MIN_POINTS {
            spec.target
        } else {
            usize::MAX // all-raw grid: pass every distinct step through
        }
    } else {
        spec.target
    }
}

/// Step-axis fast path: each run is strictly-ascending unique integers, so a
/// run is a contiguous integer interval `[first, last]` iff `last-first+1 ==
/// len`, and then its distinct count is exactly `len`. With every run an
/// interval the union's distinct count is an O(runs) interval-merge — no points.
/// Returns `None` (defer to [`decide_general`]) if any run is gappy/strided, so
/// it can't be one interval.
fn decide_step(xs: &[&[f64]], cap: usize) -> Option<Decision> {
    let mut intervals: Vec<(f64, f64)> = Vec::with_capacity(xs.len());
    let mut max_len = 0usize;
    for r in xs {
        let Some((&first, &last)) = r.first().zip(r.last()) else {
            continue; // empty run contributes nothing
        };
        // Valid ONLY because step x is strictly-ascending unique integers
        // (guaranteed by query.rs `is_step_axis` + ClickHouse per-step dedup):
        // then `last-first+1 == len` ⟺ contiguous, and distinct count == len.
        // A duplicate (e.g. [0,1,1,3]) would satisfy this yet over-count
        // distinct, flipping the decision — defended upstream, not here.
        if last - first + 1.0 != r.len() as f64 {
            return None; // not a single contiguous interval
        }
        max_len = max_len.max(r.len());
        intervals.push((first, last));
    }
    // Exact lower bound: a contiguous run owns `len` distinct steps. The
    // dominant case — one long run logged every step — exits here, O(runs),
    // having looked at no individual point.
    if max_len > cap {
        return Some(Decision::Downsample);
    }
    // Otherwise merge the intervals (O(runs log runs)); `covered` is then the
    // exact distinct count.
    let segments = merge_intervals(intervals);
    let covered: usize = segments.iter().map(|&(s, e)| (e - s) as usize + 1).sum();
    if covered > cap {
        Some(Decision::Downsample)
    } else {
        Some(Decision::Passthrough(expand_intervals(&segments)))
    }
}

/// Sort intervals by start and merge overlapping/adjacent ones (integer
/// adjacency: `next.start ≤ cur.end + 1` joins, leaving no gap) into disjoint
/// ascending segments.
fn merge_intervals(mut iv: Vec<(f64, f64)>) -> Vec<(f64, f64)> {
    iv.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let mut out: Vec<(f64, f64)> = Vec::with_capacity(iv.len());
    for (s, e) in iv {
        match out.last_mut() {
            Some(last) if s <= last.1 + 1.0 => last.1 = last.1.max(e),
            _ => out.push((s, e)),
        }
    }
    out
}

/// Expand disjoint ascending integer segments to the explicit ascending axis.
/// Equals the sort+dedup of the same steps, value-for-value. Bounded: only
/// called on passthrough, where the segments hold ≤ cap integers total.
fn expand_intervals(segs: &[(f64, f64)]) -> Vec<f64> {
    segs.iter()
        .flat_map(|&(s, e)| (s as i64..=e as i64).map(|i| i as f64))
        .collect()
}

/// One run's current head in the k-way merge, ordered so a `BinaryHeap`
/// (a max-heap) yields the SMALLEST x first.
struct MergeHead<'a> {
    x: f64,
    rest: &'a [f64],
}
impl PartialEq for MergeHead<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.x == other.x
    }
}
impl Eq for MergeHead<'_> {}
impl Ord for MergeHead<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.x.total_cmp(&self.x) // reversed: smaller x = higher priority
    }
}
impl PartialOrd for MergeHead<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// K-way merge of already-ascending runs, visiting each distinct x once, ascending, until it drains or the visitor breaks. Streamed — the union is never materialized (on a wall-time chart distinct is nearly every point, and the axis plan runs on every build and once more per delta, over the held x).
fn merge_distinct(xs: &[&[f64]], mut visit: impl FnMut(f64) -> ControlFlow<()>) {
    use std::collections::BinaryHeap;
    let mut heap: BinaryHeap<MergeHead> = BinaryHeap::with_capacity(xs.len());
    for r in xs {
        if let Some((&x, rest)) = r.split_first() {
            heap.push(MergeHead { x, rest });
        }
    }
    let mut last = f64::NAN; // never equals a data x (all-finite here)
    while let Some(MergeHead { x, rest }) = heap.pop() {
        if last != x {
            if visit(x).is_break() {
                return;
            }
            last = x;
        }
        if let Some((&x, rest)) = rest.split_first() {
            heap.push(MergeHead { x, rest });
        }
    }
}

/// General path (timestamp, custom-x, or gappy step): count distinct x with an
/// early-exit. `total` is an upper bound on distinct, so `total ≤ cap`
/// passes through immediately. Otherwise [`merge_distinct`] streams the
/// distinct sequence, stopping the instant it passes `cap` (→ downsample);
/// if it drains, the merged sequence IS the passthrough axis. x is all-finite
/// here (non-finite custom-x rides the `xnan` channel, never `xs`).
fn decide_general(xs: &[&[f64]], cap: usize) -> Decision {
    let total: usize = xs.iter().map(|r| r.len()).sum();
    if total <= cap {
        return Decision::Passthrough(sorted_distinct(xs));
    }
    let mut axis: Vec<f64> = Vec::with_capacity(total.min(cap.saturating_add(1)));
    merge_distinct(xs, |x| {
        axis.push(x);
        if axis.len() > cap {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    });
    if axis.len() > cap {
        return Decision::Downsample;
    }
    Decision::Passthrough(axis)
}

/// A downsampling chart's slot structure, derived once from the data and shared
/// verbatim by the chart build ([`shared_chart`]) and the delta planner
/// ([`numeric_delta_from_col`]) so they cannot diverge. Each instantiated cell is either
/// ENVELOPE (one slot at the midpoint of its point extent) or RAW (each distinct
/// x in it is its own exact slot).
#[derive(Clone, PartialEq)]
pub(crate) struct AxisPlan {
    grid: Grid,
    /// Per instantiated cell: does it downsample?
    env: Vec<bool>,
    /// Per cell, the (min, max) x of its points: an envelope cell's extent, and a raw non-step cell's, which held state's advance reads when appends flip it to an envelope. (INF, -INF) = unoccupied, or a raw step cell, which keeps none.
    ext: Vec<(f64, f64)>,
    /// Every raw slot x: ascending and distinct.
    raw_xs: Vec<f64>,
}

impl AxisPlan {
    /// Whether any envelope bucket actually stands — the emission-shape switch:
    /// none, and the chart ships passthrough-shaped (band-less) columns.
    fn any_env_occupied(&self) -> bool {
        self.env
            .iter()
            .zip(&self.ext)
            .any(|(&e, x)| e && x.0.is_finite())
    }
}

/// Classify every cell and collect extents + raw slots. Step axes: mode is
/// width-pure (no data pass decides it); extents come from one per-run sweep,
/// and only the raw region — log ladders below 2^(m+2), where widths drop under
/// ENV_MIN_POINTS — needs a distinct-merge, small by construction
/// (a linear step grid in this arm has width ≥ ENV_MIN_POINTS, so its raw
/// region is empty). Non-step axes: one streamed distinct-merge visits every x
/// in order; each cell's mode comes from its own distinct count.
fn plan_axis(xs: &[&[f64]], grid: Grid, spec: GridSpec) -> AxisPlan {
    let mut env = vec![false; grid.cells];
    let mut ext = vec![(f64::INFINITY, f64::NEG_INFINITY); grid.cells];
    let mut raw_xs: Vec<f64> = Vec::new();
    if spec.is_step_axis {
        for (c, e) in env.iter_mut().enumerate() {
            *e = grid.width(c) >= ENV_MIN_POINTS;
        }
        let mut raw_slices: Vec<&[f64]> = Vec::with_capacity(xs.len());
        for r in xs {
            // Widths only grow with x, so a run's raw samples lead it.
            let cut = r.partition_point(|&v| !env[grid.index(v)]);
            raw_slices.push(&r[..cut]);
            for &x in &r[cut..] {
                let e = &mut ext[grid.index(x)];
                e.0 = e.0.min(x);
                e.1 = e.1.max(x);
            }
        }
        merge_distinct(&raw_slices, |x| {
            raw_xs.push(x);
            ControlFlow::Continue(())
        });
    } else {
        // Distinct x ascend, so each cell's x arrive consecutively, its first (meeting the cell unoccupied) and last giving its extent, a raw cell's too ([`AxisPlan::ext`]). A still-raw cell buffers its ≤ ENV_MIN_POINTS-1 x, which it may yet dump to the raw list: the ENV_MIN_POINTS-th distinct x settles it as envelope and drops the buffer; a cell that ends short ships it raw.
        let mut buf: Vec<f64> = Vec::with_capacity(ENV_MIN_POINTS as usize - 1);
        merge_distinct(xs, |x| {
            let c = grid.index(x);
            if !ext[c].0.is_finite() {
                raw_xs.append(&mut buf); // the previous cell ended short of ENV_MIN_POINTS
                ext[c].0 = x;
            }
            if !env[c] && ((buf.len() + 1) as f64) < ENV_MIN_POINTS {
                buf.push(x);
            } else {
                env[c] = true;
                buf.clear();
            }
            ext[c].1 = x;
            ControlFlow::Continue(())
        });
        raw_xs.append(&mut buf);
    }
    AxisPlan {
        grid,
        env,
        ext,
        raw_xs,
    }
}

/// The slots of a downsampling chart in axis order, plus the per-slot bucket x
/// extents. Raw slots (exact x) and envelope slots (extent midpoint) interleave
/// in x order. `xr_min`/`xr_max` are the chart-level bucket extents (union
/// across runs), finite only where a bucket aggregated a real x spread.
struct MixedAxis {
    plan: AxisPlan,
    x: Vec<f64>,
    /// Envelope-occupied cells → their slot; usize::MAX otherwise.
    slot_of_cell: Vec<usize>,
    xr_min: Vec<f64>,
    xr_max: Vec<f64>,
}

/// The slot holding a run's own sample at `x`, where [`bucket_run`] put it and an advance finds it: its envelope cell's slot (`plan`'s cells, none passing through), else its exact raw slot among `x_values`.
pub(crate) fn anchor_slot(
    plan: Option<&AxisPlan>,
    slot_of_cell: &[usize],
    x_values: &[f64],
    x: f64,
) -> usize {
    match plan.and_then(|plan| Some(plan.grid.index(x)).filter(|&c| plan.env[c])) {
        Some(c) => slot_of_cell[c],
        None => x_values.partition_point(|&v| v < x),
    }
}

fn mixed_axis(plan: AxisPlan) -> MixedAxis {
    let mut x = Vec::with_capacity(plan.raw_xs.len() + plan.grid.cells);
    let mut slot_of_cell = vec![usize::MAX; plan.grid.cells];
    let mut rp = 0usize;
    for (c, slot) in slot_of_cell.iter_mut().enumerate() {
        if plan.env[c] {
            if plan.ext[c].0.is_finite() {
                *slot = x.len();
                x.push((plan.ext[c].0 + plan.ext[c].1) * 0.5);
            }
        } else {
            while rp < plan.raw_xs.len() && plan.grid.index(plan.raw_xs[rp]) == c {
                x.push(plan.raw_xs[rp]);
                rp += 1;
            }
        }
    }
    debug_assert_eq!(rp, plan.raw_xs.len(), "every raw slot placed");
    let mut xr_min = vec![f64::NAN; x.len()];
    let mut xr_max = vec![f64::NAN; x.len()];
    for (c, &s) in slot_of_cell.iter().enumerate() {
        if s != usize::MAX && plan.ext[c].1 > plan.ext[c].0 {
            xr_min[s] = plan.ext[c].0;
            xr_max[s] = plan.ext[c].1;
        }
    }
    MixedAxis {
        plan,
        x,
        slot_of_cell,
        xr_min,
        xr_max,
    }
}

// === Frontier deltas ===
//
// Per-sample age relative to the verified held response: OLD = held input with no explicit dirty flag; ANCHOR = held input that dirties its cell (a kind-4 marker's anchor); NEW = input absent from that response. Cache lineage proves membership before query.rs marks dependencies. Only the prefix before the first dirty cell is retained.
pub const AGE_OLD: u8 = 0;
pub const AGE_ANCHOR: u8 = 1;
pub const AGE_NEW: u8 = 2;

/// Prove a prefix for numeric columns and logged-y markers: axis and every CONTINUING series (any held sample; all-new series ship complete and constrain nothing). Callers must first flag kind-4 marker dependencies before using this as a splice point. Zero means answer in full.
///
/// The non-NEW reconstruction is exactly the held input; `axis` is the one [`shared_chart`] emitted over `xs`.
///
/// The caller proves the exact [`SmoothingPlan`] unchanged when one exists. A column is dirty when its structure changes or a continuing series has a non-OLD sample there, and from its series' `slot_dirty` x on ([`SlotSmoother::dirty_from`]), where its smoother at the slot x may read a changed sample: an envelope column by its center, whichever runs fill it (a timestamp-axis fetch-margin sample can change a cell the series has no sample near), and a raw column where the series has a sample. Sampled audits reconstruct the held response; client hashes verify every splice.
pub fn numeric_delta_from_col(
    xs: &[&[f64]],
    age: &[&[u8]],
    slot_dirty: &[f64],
    spec: GridSpec,
    axis: &incremental::HeldAxis,
) -> usize {
    let continuing: Vec<bool> = age
        .iter()
        .map(|a| a.iter().any(|&k| k != AGE_NEW))
        .collect();
    let old_xs: Vec<Cow<'_, [f64]>> = xs.iter().zip(age).map(|(x, a)| held_xs(x, a)).collect();
    let old_refs: Vec<&[f64]> = old_xs.iter().map(|c| c.as_ref()).collect();
    let (held, held_x) = match AxisDecision::of(&old_refs, spec) {
        AxisDecision::Empty => return 0, // nothing held
        AxisDecision::Passthrough(held_x) => (incremental::HeldAxis::Passthrough, held_x),
        AxisDecision::Downsample(grid, g_first, g_last) => {
            let plan = plan_axis(&old_refs, grid, spec);
            let held = incremental::HeldAxis::Grid {
                plan,
                g_first,
                g_last,
            };
            (held, Vec::new())
        }
    };
    axes_from_col(&held, &held_x, axis, xs, age, slot_dirty, &continuing)
}

/// The prefix two axes share for the continuing series ([`numeric_delta_from_col`]; held state's advance compares its held axis with [`incremental::advance_axis`]'s): `held_x` holds the held columns when `held` passes every x through. Only non-OLD samples and those at or past their series' `slot_dirty` x mark a column, so a caller that has proved no series repeats an x may pass just those, and of the latter each series' first at a raw slot.
pub(crate) fn axes_from_col(
    held: &incremental::HeldAxis,
    held_x: &[f64],
    axis: &incremental::HeldAxis,
    xs: &[&[f64]],
    age: &[&[u8]],
    slot_dirty: &[f64],
    continuing: &[bool],
) -> usize {
    use incremental::HeldAxis::{Grid, Passthrough};
    let held_x = match (held, axis) {
        (Passthrough, Passthrough) => held_x,
        (Grid { plan: old, .. }, Grid { plan: new, .. }) => {
            if new.grid.kind != old.grid.kind || new.grid.base != old.grid.base {
                return 0; // tier or anchor change: every boundary moved
            }
            match (old.any_env_occupied(), new.any_env_occupied()) {
                // No envelope bucket stands on either side: both ship passthrough-SHAPED columns over their distinct axes.
                (false, false) => &old.raw_xs,
                (true, true) => return mixed_from_col(old, new, xs, age, slot_dirty, continuing),
                // Envelope emission springing into existence (or vanishing) reshapes every series' families chart-wide — band-less held columns can't continue a banded tail.
                _ => return 0,
            }
        }
        // Passthrough <-> grid flip: structurally different axes.
        _ => return 0,
    };
    passthrough_from_col(held_x, xs, age, slot_dirty, continuing)
}

/// Borrow a contiguous held (non-NEW) x prefix, or filter into an owned buffer when timestamp ordering interleaves new rows. Cache lineage preserves a held step prefix within each tag, including after eviction recovery.
fn held_xs<'a>(x: &'a [f64], a: &[u8]) -> Cow<'a, [f64]> {
    let split = a.iter().position(|&k| k == AGE_NEW).unwrap_or(a.len());
    if a[split..].iter().all(|&k| k == AGE_NEW) {
        Cow::Borrowed(&x[..split])
    } else {
        Cow::Owned(
            x.iter()
                .zip(a)
                .filter(|(_, &k)| k != AGE_NEW)
                .map(|(&v, _)| v)
                .collect(),
        )
    }
}

/// The first x at which a continuing series changes: each one's first non-OLD sample (NEW, or a marker's ANCHOR), and from its `slot_dirty` x on its first sample at a raw slot (`raw`), whose smoothed value may read a changed sample. Slots ascend with x, so the planners stop at the first slot from there on.
fn first_change(
    xs: &[&[f64]],
    age: &[&[u8]],
    slot_dirty: &[f64],
    continuing: &[bool],
    raw: impl Fn(f64) -> bool,
) -> f64 {
    let mut first = f64::INFINITY;
    for (((x, a), &reads), &cont) in xs.iter().zip(age).zip(slot_dirty).zip(continuing) {
        if !cont {
            continue;
        }
        if let Some(e) = a.iter().position(|&k| k != AGE_OLD) {
            first = first.min(x[e]);
        }
        let from = x.partition_point(|&v| v < reads);
        if let Some(&v) = x[from..].iter().find(|&&v| raw(v)) {
            first = first.min(v);
        }
    }
    first
}

/// Passthrough arm: the first slot of `old_axis` whose x or content changed — a continuing series' change ([`first_change`]) or a new series' sample at an x the held axis lacks (an insertion). A new series' samples at already-charted x change neither the axis nor any continuing column.
fn passthrough_from_col(
    old_axis: &[f64],
    xs: &[&[f64]],
    age: &[&[u8]],
    slot_dirty: &[f64],
    continuing: &[bool],
) -> usize {
    // Any duplicate x flips every series' band presence chart-wide (see shared_passthrough), changing columns before it too — only full is safe. xs ascend per series, so duplicates are adjacent.
    if xs.iter().any(|x| x.windows(2).any(|w| w[0] == w[1])) {
        return 0;
    }
    let mut first_dirty_x = first_change(xs, age, slot_dirty, continuing, |_| true);
    for (x, _) in xs.iter().zip(continuing).filter(|&(_, &cont)| !cont) {
        if let Some(&v) = x
            .iter()
            .take_while(|&&v| v < first_dirty_x)
            .find(|&&v| old_axis.binary_search_by(|p| p.total_cmp(&v)).is_err())
        {
            first_dirty_x = v;
        }
    }
    old_axis.partition_point(|&v| v < first_dirty_x)
}

/// Grid arm: walk the shared cell layout in slot order, counting the OLD axis's slots; the first structural or content divergence bounds the provable prefix. Old raw x sets are per-cell subsets of new ones (samples only accumulate), so a pairwise walk either matches bit-for-bit or stops at the first inserted slot; envelope cells compare extents bitwise and mode (a bucket's 4th distinct x restructures its slots).
fn mixed_from_col(
    old_plan: &AxisPlan,
    new_plan: &AxisPlan,
    xs: &[&[f64]],
    age: &[&[u8]],
    slot_dirty: &[f64],
    continuing: &[bool],
) -> usize {
    // Envelope slots from this x on may read a changed sample ([`SlotSmoother::dirty_from`]).
    let slot_dirty_x = slot_dirty
        .iter()
        .zip(continuing)
        .filter(|&(_, &cont)| cont)
        .fold(f64::INFINITY, |x, (&d, _)| x.min(d));
    // The current grid holds every held and current x: the held grid's cells (kind and base agree, checked by the caller), then any appended cells, whose slots all sit past the old prefix.
    let cell_of = |x| new_plan.grid.index(x);

    // A continuing series' first change ([`first_change`]): a NEW sample at an existing raw x is a duplicate or first-sample content change, an ANCHOR sample's marker anchor moves, and a raw slot from its series' slot rule's x on reads a changed sample. The walk stops at the raw slot at or past it, or the envelope cell whose x reach it.
    let first_dirty = first_change(xs, age, slot_dirty, continuing, |v| {
        !new_plan.env[cell_of(v)]
    });

    let (old_raw, new_raw) = (&old_plan.raw_xs, &new_plan.raw_xs);
    let mut r = 0usize; // raw slots walked, the same in both lists
    let mut from_col = 0usize;
    for c in 0..old_plan.grid.cells {
        if old_plan.env[c] != new_plan.env[c] {
            return from_col; // mode flip: the cell's slots restructure
        }
        if old_plan.env[c] {
            let (lo, hi) = old_plan.ext[c];
            if lo.to_bits() != new_plan.ext[c].0.to_bits()
                || hi.to_bits() != new_plan.ext[c].1.to_bits()
                || first_dirty <= hi
                || (lo + hi) * 0.5 >= slot_dirty_x
            {
                return from_col;
            }
            if lo.is_finite() {
                from_col += 1;
            }
        } else {
            // A raw cell: its held slots recur bit for bit at the same places, and no current slot follows them in the cell.
            while let Some(&x) = old_raw.get(r).filter(|&&x| cell_of(x) == c) {
                if x >= first_dirty || new_raw.get(r).is_none_or(|v| v.to_bits() != x.to_bits()) {
                    return from_col;
                }
                r += 1;
                from_col += 1;
            }
            if new_raw.get(r).is_some_and(|&v| cell_of(v) == c) {
                return from_col;
            }
        }
    }
    from_col
}

#[cfg(test)]
mod delta_planner_tests {
    use super::*;

    fn spec(target: usize) -> GridSpec {
        GridSpec {
            target,
            is_step_axis: true,
            log_buckets: false,
            shift_one: false,
        }
    }
    fn log_spec(target: usize) -> GridSpec {
        GridSpec {
            target,
            is_step_axis: true,
            log_buckets: true,
            shift_one: true,
        }
    }

    /// The exact response for a set of series — the brute-force oracle — and its axis. With `w`, each run's values are its window-`w` Savitzky-Golay smoother, unfloored, at every slot.
    fn respond(
        xs: &[Vec<f64>],
        ys: &[Vec<f64>],
        w: usize,
        spec: GridSpec,
    ) -> (DenseChart, Option<incremental::HeldAxis>) {
        let xr: Vec<&[f64]> = xs.iter().map(Vec::as_slice).collect();
        let rr: Vec<&[f64]> = ys.iter().map(Vec::as_slice).collect();
        let kinds: Vec<Vec<u8>> = xs.iter().map(|x| vec![0u8; x.len()]).collect();
        let kr: Vec<&[u8]> = kinds.iter().map(Vec::as_slice).collect();
        let xnan: Vec<&[f64]> = xs.iter().map(|_| [].as_slice()).collect();
        let mut slots: Vec<_> = xs
            .iter()
            .zip(ys)
            .map(|(x, y)| slot_smoother(x, w).map(|s| SlotEvaluator::over(s, x, y)))
            .collect();
        shared_chart(
            &xr,
            &rr,
            &rr,
            &kr,
            &xnan,
            w > 0,
            &mut slots,
            spec,
            AxisDecision::of(&xr, spec),
        )
    }

    /// The window-`w` Savitzky-Golay slot smoother of a series, if `w` smooths.
    fn slot_smoother(x: &[f64], w: usize) -> Option<SlotSmoother> {
        let plan = smoothing_plan(x, Algorithm::SavitzkyGolay, true).0;
        (w > 0).then(|| SlotSmoother::new(Algorithm::SavitzkyGolay, w as u32, 1.0, 1, plan))
    }

    /// Age flags for a series holding its first `old_n` samples.
    fn ages(len: usize, old_n: usize) -> Vec<u8> {
        (0..len)
            .map(|i| if i >= old_n { AGE_NEW } else { AGE_OLD })
            .collect()
    }

    /// Assert numeric_delta_from_col's claim against the oracle: the first from_col
    /// columns of the old and new responses are bit-identical for the axis,
    /// the chart-level bucket extents, and every continuing series. Returns
    /// from_col.
    fn check(
        xs: &[Vec<f64>],
        ys: &[Vec<f64>],
        old_ns: &[usize],
        w: usize,
        spec: GridSpec,
    ) -> usize {
        let age: Vec<Vec<u8>> = xs
            .iter()
            .zip(old_ns)
            .map(|(x, &n)| ages(x.len(), n))
            .collect();
        let xr: Vec<&[f64]> = xs.iter().map(Vec::as_slice).collect();
        let ar: Vec<&[u8]> = age.iter().map(Vec::as_slice).collect();

        let old_xs: Vec<Vec<f64>> = xs
            .iter()
            .zip(old_ns)
            .filter(|(_, &n)| n > 0)
            .map(|(x, &n)| x[..n].to_vec())
            .collect();
        let old_ys: Vec<Vec<f64>> = ys
            .iter()
            .zip(old_ns)
            .filter(|(_, &n)| n > 0)
            .map(|(y, &n)| y[..n].to_vec())
            .collect();
        let old = respond(&old_xs, &old_ys, w, spec).0;
        let (new, axis) = respond(xs, ys, w, spec);
        let slot_dirty: Vec<f64> = xs
            .iter()
            .zip(old_ns)
            .map(|(x, &n)| {
                let first_new = x.get(n).copied();
                slot_smoother(x, w).map_or(f64::INFINITY, |s| s.dirty_from(first_new))
            })
            .collect();
        let from_col = numeric_delta_from_col(&xr, &ar, &slot_dirty, spec, &axis.unwrap());

        assert!(
            from_col <= old.x_values.len(),
            "from_col past the held axis"
        );
        assert!(from_col <= new.x_values.len(), "from_col past the new axis");
        let bits = |v: &[f64]| v.iter().map(|f| f.to_bits()).collect::<Vec<_>>();
        assert_eq!(
            bits(&old.x_values[..from_col]),
            bits(&new.x_values[..from_col]),
            "axis prefix diverged before from_col={from_col}"
        );
        let pfx = |v: &[f64]| bits(&v[..from_col.min(v.len())]);
        assert_eq!(pfx(&old.xr_min), pfx(&new.xr_min), "xr_min prefix");
        assert_eq!(pfx(&old.xr_max), pfx(&new.xr_max), "xr_max prefix");
        for (c, (o, n)) in old
            .series
            .iter()
            .zip(
                new.series
                    .iter()
                    .zip(old_ns)
                    .filter(|(_, &n)| n > 0)
                    .map(|(s, _)| s),
            )
            .enumerate()
        {
            assert_eq!(pfx(&o.values), pfx(&n.values), "series {c} values");
            assert_eq!(pfx(&o.raw_values), pfx(&n.raw_values), "series {c} raw");
            assert_eq!(pfx(&o.min_values), pfx(&n.min_values), "series {c} min");
            assert_eq!(pfx(&o.max_values), pfx(&n.max_values), "series {c} max");
        }
        from_col
    }

    fn series(n: usize, seed: u64) -> (Vec<f64>, Vec<f64>) {
        let xs: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let ys: Vec<f64> = (0..n)
            .map(|i| ((i as u64).wrapping_mul(seed + 3) % 997) as f64 / 7.0)
            .collect();
        (xs, ys)
    }

    /// The planner's held-x input: append-only ages borrow the prefix, an
    /// interleaved (time-mode) shape falls back to the owned filter, and both
    /// yield the same values.
    #[test]
    fn held_xs_borrows_prefixes_and_filters_interleaves() {
        let x = [1.0, 2.0, 3.0, 4.0];
        let h = held_xs(&x, &[AGE_OLD, AGE_ANCHOR, AGE_NEW, AGE_NEW]);
        assert!(matches!(h, Cow::Borrowed(_)));
        assert_eq!(h.as_ref(), [1.0, 2.0]);
        let h = held_xs(&x, &[AGE_OLD, AGE_NEW, AGE_ANCHOR, AGE_NEW]);
        assert!(matches!(h, Cow::Owned(_)));
        assert_eq!(h.as_ref(), [1.0, 3.0]);
        assert_eq!(held_xs(&x, &[AGE_NEW; 4]).as_ref(), [0f64; 0]);
        assert!(matches!(held_xs(&x, &[AGE_OLD; 4]), Cow::Borrowed(s) if s == x));
    }

    /// A new series filling a cell the held plan left empty flips it from raw to envelope, which only `mixed_from_col`'s mode check sees (`first_change` reads continuing series, and the raw walk steps over an empty cell): the prefix stops before it. Cells of width 256 from 0: an envelope at 1..=4, a new one at 300..=303, raw x at 1000..=1002.
    #[test]
    fn a_new_series_filling_an_empty_cell_bounds_the_prefix() {
        let a = vec![1.0, 2.0, 3.0, 4.0, 1000.0, 1001.0, 1002.0];
        let b = vec![300.0, 301.0, 302.0, 303.0];
        let ya: Vec<f64> = a.iter().map(|x| x * 2.0).collect();
        let yb: Vec<f64> = b.iter().map(|x| x * 3.0).collect();
        let spec = GridSpec {
            is_step_axis: false,
            ..spec(4)
        };
        assert_eq!(check(&[a, b], &[ya, yb], &[7, 0], 0, spec), 1);
    }

    /// A new series' x in a held raw cell, past its held raw slots, inserts a slot before every later column, which only the raw walk's insertion check sees (the envelope cell after it is unchanged): the prefix stops at it. Cells of width 256 from 0: raw x at 1 and 2, the new series' 3, an envelope at 300..=303, raw x at 1000..=1002.
    #[test]
    fn a_new_series_inserting_a_raw_slot_bounds_the_prefix() {
        let a = vec![1.0, 2.0, 300.0, 301.0, 302.0, 303.0, 1000.0, 1001.0, 1002.0];
        let ya: Vec<f64> = a.iter().map(|x| x * 2.0).collect();
        let spec = GridSpec {
            is_step_axis: false,
            ..spec(4)
        };
        assert_eq!(
            check(&[a, vec![3.0]], &[ya, vec![9.0]], &[9, 0], 0, spec),
            2
        );
    }

    #[test]
    fn tail_appends_keep_most_of_a_downsampled_chart() {
        let (xa, ya) = series(3000, 1);
        let (xb, yb) = series(2400, 8);
        let from_col = check(&[xa, xb], &[ya, yb], &[2900, 2300], 12, spec(500));
        assert!(from_col > 200, "salvaged only {from_col} columns");
    }

    #[test]
    fn exact_upper_bound_cells_are_invalidated_across_grid_extension() {
        // At width 4, each held maximum below lands exactly on a cell's lower
        // boundary. Appends first fill that existing cell, then extend the grid
        // by another cell. The planner must keep the earlier cells but resend
        // the boundary cell whose center/envelope changed.
        for target in [8usize, 16, 32, 120, 400] {
            let old_last = 3 * target; // divisible by 4; span/target = 3 => width 4
            let (x, y) = series(old_last + 6, target as u64);
            let from_col = check(
                std::slice::from_ref(&x),
                std::slice::from_ref(&y),
                &[old_last + 1],
                0,
                spec(target),
            );
            assert_eq!(
                from_col,
                old_last / 4,
                "target={target}: step boundary cell must start the delta"
            );

            // Non-step mode adds a structural transition: the held boundary
            // cell is one raw slot, then its fourth distinct x turns it into
            // an envelope. It must invalidate at the same position.
            let nonstep = GridSpec {
                is_step_axis: false,
                ..spec(target)
            };
            let from_col = check(&[x], &[y], &[old_last + 1], 0, nonstep);
            assert_eq!(
                from_col,
                old_last / 4,
                "target={target}: non-step boundary mode flip must start the delta"
            );
        }
    }

    #[test]
    fn mid_axis_append_bounds_the_prefix_but_salvages_the_head() {
        // Run B is far shorter: its appends land mid-axis.
        let (xa, ya) = series(4000, 2);
        let (xb, yb) = series(1000, 9);
        let from_col = check(&[xa, xb], &[ya, yb], &[4000, 900], 0, spec(500));
        assert!(from_col > 0 && from_col < 300, "from_col={from_col}");
    }

    #[test]
    fn same_step_new_run_disturbs_nothing() {
        // A new run logging already-charted steps: the axis and both held
        // series are fully salvageable (only the new series ships).
        let (xa, ya) = series(2000, 3);
        let (xb, yb) = series(2000, 5);
        let (xc, yc) = series(1500, 7); // new, steps within the charted range
        let from_col = check(&[xa, xb, xc], &[ya, yb, yc], &[2000, 2000, 0], 0, spec(500));
        let new_len = respond(
            &[series(2000, 3).0, series(2000, 5).0, series(1500, 7).0],
            &[series(2000, 3).1, series(2000, 5).1, series(1500, 7).1],
            0,
            spec(500),
        )
        .0
        .x_values
        .len();
        assert_eq!(from_col, new_len, "growth should salvage everything");
    }

    #[test]
    fn unchanged_salvages_everything_and_flips_answer_full() {
        let (xa, ya) = series(1000, 4);
        let from_col = check(
            std::slice::from_ref(&xa),
            std::slice::from_ref(&ya),
            &[1000],
            6,
            spec(300),
        );
        assert_eq!(
            from_col,
            respond(&[xa], &[ya], 6, spec(300)).0.x_values.len()
        );
        // Tier crossing: the appended span doubles the bucket width.
        let (xl, yl) = series(9000, 4);
        assert_eq!(check(&[xl], &[yl], &[5000], 0, spec(1000)), 0);
    }

    #[test]
    fn passthrough_paths_hold_and_flips_bail() {
        // Sparse chart stays passthrough across the append.
        let (xa, ya) = series(300, 6);
        let (xb, yb) = series(200, 11);
        let from_col = check(&[xa, xb], &[ya, yb], &[290, 200], 4, spec(1000));
        assert!(from_col > 200, "from_col={from_col}");
        // New run on the same sparse steps: everything salvages.
        let (xc, yc) = series(250, 13);
        let (xd, yd) = series(300, 6);
        let from_col = check(&[xd, xc], &[series(300, 6).1, yc], &[300, 0], 0, spec(1000));
        assert_eq!(
            from_col,
            respond(
                &[series(300, 6).0, series(250, 13).0],
                &[yd, series(250, 13).1],
                0,
                spec(1000)
            )
            .0
            .x_values
            .len()
        );
        // Passthrough -> downsample flip: answer full. (2100 steps at target
        // 500 give width 8 ≥ 4, a real grid; 400 held steps passed through.)
        let (xe, ye) = series(2100, 15);
        assert_eq!(check(&[xe], &[ye], &[400], 0, spec(500)), 0);
    }

    /// A new run's sample at an x the held passthrough axis lacks is an insertion that bounds the prefix though no continuing series changes before it: run a's new sample at 8 changes nothing before 8, but the new run's 3 lands between the held 2 and 4, so the delta starts at column 2.
    #[test]
    fn a_new_runs_inserted_x_bounds_a_passthrough_prefix() {
        let a = vec![0.0, 2.0, 4.0, 6.0, 8.0];
        let ya = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let from = check(&[a, vec![3.0]], &[ya, vec![7.0]], &[4, 0], 0, spec(1000));
        assert_eq!(from, 2);
    }

    /// On a grid that buckets the other series, series 1's three samples are raw slots of one cell: its new sample at 120 is a new slot, and the held slot at 110, fitted over 100-120, reads it, so the delta starts there (column 2, after cell 0's envelope and the slot at 100).
    #[test]
    fn raw_slots_reading_a_new_sample_are_resent() {
        let stable: Vec<f64> = (0..8).chain(1000..1008).map(f64::from).collect();
        let ys: Vec<f64> = stable.iter().map(|v| v * 0.01).collect();
        let changed = vec![100.0, 110.0, 120.0];
        let spec = GridSpec {
            is_step_axis: false,
            ..spec(16)
        };
        let from = check(
            &[stable, changed],
            &[ys, vec![1.0, 8.0, 2.0]],
            &[16, 2],
            3,
            spec,
        );
        assert_eq!(from, 2);
    }

    /// The slot rule is inclusive: an envelope whose center sits exactly at the rule's x is dirty. Window 4 has half-width 2, so run a's new sample at 12 dirties from 10, the center of cell [8, 12), whose extent is a's 9 to 11: its window now reaches 12, at zero weight, which can still change its bits (the window's count moves, which can switch its fit to or from the even window's dot product), so the delta starts at its column, before the new sample's own cell.
    #[test]
    fn an_envelope_centered_at_the_slot_rules_x_is_resent() {
        let a = vec![9.0, 10.0, 11.0, 12.0];
        let ya = vec![1.0, 2.0, 4.0, 8.0];
        let b = vec![0.0, 1.0, 2.0];
        let from = check(&[a, b], &[ya, vec![5.0; 3]], &[3, 3], 4, spec(4));
        assert_eq!(from, 1);
    }

    /// An envelope cell holding a continuing series' first change at its extent's high edge is resent: run a fills width-4 cells over 0 to 15, and run b's new sample at 7, already a's, leaves cell [4, 8)'s extent 4 to 7 as it was but changes b's envelope there, so the delta starts at its column, after cell 0's.
    #[test]
    fn an_envelope_whose_high_edge_holds_a_change_is_resent() {
        let a: Vec<f64> = (0..16).map(f64::from).collect();
        let ya: Vec<f64> = (0..16).map(|i| f64::from(i % 5)).collect();
        let b = vec![4.0, 5.0, 7.0];
        let from = check(&[a, b], &[ya, vec![1.0, 2.0, 9.0]], &[16, 2], 0, spec(4));
        assert_eq!(from, 1);
    }

    /// The slot rule is inclusive for raw slots too: run b's held slot at 1,100 is exactly the rule's x (the new 1,300 less half-width 200: window 4 at spacing 100), so its window reaches 1,300 at zero weight and the delta starts at its column, after cell 0's envelope and the slot at 1,000.
    #[test]
    fn a_raw_slot_at_the_slot_rules_x_is_resent() {
        let a: Vec<f64> = (0..=40).map(f64::from).collect();
        let ya: Vec<f64> = (0..=40).map(|i| (i % 7) as f64).collect();
        let b = vec![1000.0, 1100.0, 1200.0, 1300.0];
        let yb: Vec<f64> = (0..4)
            .map(|i| ((i * 8 + 1) % 11) as f64 * 0.37 + 0.1)
            .collect();
        let spec = GridSpec {
            is_step_axis: false,
            ..spec(8)
        };
        assert_eq!(check(&[a, b], &[ya, yb], &[41, 3], 4, spec), 2);
    }

    #[test]
    fn linear_sub_envelope_widths_stay_passthrough_across_appends() {
        // Width 2 < ENV_MIN_POINTS: the whole chart is raw ("2 points beat 1
        // envelope bucket") on both sides of the append, and the delta is the
        // plain passthrough tail.
        let (xa, ya) = series(1500, 3);
        let from_col = check(&[xa], &[ya], &[1400], 0, spec(1000));
        assert!(from_col >= 1400, "from_col={from_col}");
    }

    #[test]
    fn log_appends_keep_most_of_the_chart() {
        let (xa, ya) = series(3000, 1);
        let (xb, yb) = series(2400, 8);
        let from_col = check(&[xa, xb], &[ya, yb], &[2900, 2300], 0, log_spec(300));
        assert!(from_col > 100, "salvaged only {from_col} columns");
        // Smoothed variant: the slot rule dirties trailing buckets only.
        let (xc, yc) = series(3000, 5);
        let from_col = check(&[xc], &[yc], &[2950], 10, log_spec(300));
        assert!(from_col > 100, "smoothed log salvaged only {from_col}");
    }

    #[test]
    fn log_subdivision_tier_change_answers_full() {
        // 601 held steps span ~10 octaves (subdivision 2^5 fits target 300);
        // growing to 40k steps spans ~16, forcing 2^4 — every boundary moves,
        // so the planner must answer full.
        let (xa, ya) = series(40_000, 4);
        assert_eq!(check(&[xa], &[ya], &[601], 0, log_spec(300)), 0);
    }

    #[test]
    fn nonstep_bucket_mode_flip_bounds_the_prefix() {
        // Time-like axis, deliberately clumped: cluster A (8 xs in one cell)
        // downsamples; cluster B holds 3 distinct xs (raw) until a new run
        // lands its 4th — that bucket restructures, everything before it holds.
        // check()'s oracle verifies the claimed prefix bit-for-bit either way.
        let xs: Vec<f64> = vec![
            // cluster far left, dense: envelope material
            1000.0, 1001.0, 1002.0, 1003.0, 1004.0, 1005.0, 1006.0, 1007.0,
            // mid cluster: 3 distinct — raw until the 4th arrives
            5000.0, 5001.0, 5002.0,
            // right tail, so the span (and grid) stays put while a new run adds its sample
            9000.0, 9001.0, 9002.0, 9003.0, 9004.0, 9005.0, 9006.0, 9007.0,
            9008.0,
            // the mid cluster's 4th x arrives from a new run below
        ];
        let ys: Vec<f64> = xs.iter().map(|x| x * 0.25).collect();
        // A new run whose only sample lands in the mid cluster, flipping that
        // bucket from 3 raw slots to one envelope slot.
        let xs2 = vec![5003.0];
        let ys2 = vec![7.0];
        let spec2 = GridSpec {
            is_step_axis: false,
            ..spec(4)
        };
        let from_col = check(
            &[xs.clone(), xs2],
            &[ys.clone(), ys2],
            &[xs.len(), 0],
            0,
            spec2,
        );
        let old_len = respond(&[xs], &[ys], 0, spec2).0.x_values.len();
        assert!(
            from_col > 0 && from_col < old_len,
            "from_col={from_col} of {old_len}"
        );
    }
}

/// Bucket every run onto one shared slot axis, by the chart's axis decision over `xs` (`decided`) and its geometry `spec`. Per run, `xs` and `kinds` are parallel (x ascending), as are `plot` and `raw` where not empty: `plot` holds the logged values (custom x's smoothed ones) and is empty for a run with a slot evaluator, `raw` holds the logged values when `is_smoothed`. `xnan` is each run's kind-4 anchors: the plottable x its unplottable samples (non-finite custom x, or on a log axis a negative x or a custom x of 0) are carried to, or −∞ for a run with no plottable x (column 0). A run with a slot evaluator (`slot_evaluators`: smoothed step and time charts) takes every value from its smoother at the slot's x ([`SlotEvaluator`]); a smoothed run without one takes the samples in `plot` at raw slots and interpolates them at envelope slots. Labels and run ids are left for the caller to fill.
/// Also returns the axis layout, which the delta planner and held chart state read ([`incremental`]); `None` with no plottable point.
#[allow(clippy::too_many_arguments)]
pub(crate) fn shared_chart(
    xs: &[&[f64]],
    plot: &[&[f64]],
    raw: &[&[f64]],
    kinds: &[&[u8]],
    xnan: &[&[f64]],
    is_smoothed: bool,
    slot_evaluators: &mut [Option<SlotEvaluator<'_>>],
    spec: GridSpec,
    decided: AxisDecision,
) -> (DenseChart, Option<incremental::HeldAxis>) {
    let n = xs.len();
    debug_assert_eq!(slot_evaluators.len(), n);
    match decided {
        AxisDecision::Empty => {
            let chart = DenseChart {
                series: (0..n).map(|_| DenseSeries::default()).collect(),
                ..Default::default()
            };
            (chart, None)
        }
        AxisDecision::Passthrough(axis) => (
            shared_passthrough(
                axis,
                xs,
                plot,
                raw,
                kinds,
                xnan,
                is_smoothed,
                slot_evaluators,
            ),
            Some(incremental::HeldAxis::Passthrough),
        ),
        AxisDecision::Downsample(grid, g_first, g_last) => {
            let plan = plan_axis(xs, grid, spec);
            let (chart, plan) = if !plan.any_env_occupied() {
                // Every cell came out raw (a log ladder over sparse-enough data): the distinct axis IS the chart, and the emission shape is passthrough's — band only under duplicate-x, exactly what the delta planner mirrors.
                let chart = shared_passthrough(
                    plan.raw_xs.clone(),
                    xs,
                    plot,
                    raw,
                    kinds,
                    xnan,
                    is_smoothed,
                    slot_evaluators,
                );
                (chart, plan)
            } else {
                let ma = mixed_axis(plan);
                let mut series = Vec::with_capacity(n);
                for i in 0..n {
                    let mut a = bucket_run(
                        xs[i],
                        plot[i],
                        if is_smoothed { raw[i] } else { plot[i] },
                        kinds[i],
                        is_smoothed,
                        slot_evaluators[i].as_mut(),
                        &ma,
                    );
                    fold_xnan(&mut a, xnan[i], |x| {
                        anchor_slot(Some(&ma.plan), &ma.slot_of_cell, &ma.x, x)
                    });
                    series.push(a);
                }
                let chart = DenseChart {
                    x_values: ma.x,
                    xr_min: ma.xr_min,
                    xr_max: ma.xr_max,
                    series,
                };
                (chart, ma.plan)
            };
            let held = incremental::HeldAxis::Grid {
                plan,
                g_first,
                g_last,
            };
            (chart, Some(held))
        }
    }
}

/// Merge "unplottable x" annotations (kind 4: non-finite custom-x, negative x
/// on a log axis, or a custom x of 0 there) into a run's markers, each on the slot holding its anchor
/// sample (`slot_of`). A y-kind already at that slot wins (it says strictly
/// more). Also the single place markers are sorted and de-duplicated by slot.
fn fold_xnan(a: &mut DenseSeries, xnan: &[f64], slot_of: impl Fn(f64) -> usize) {
    let mut pairs: Vec<(u32, u32)> = a
        .nan_indices
        .iter()
        .copied()
        .zip(a.nan_kinds.iter().copied())
        .collect();
    for &x in xnan {
        // −∞ (a run with no plottable x) is column 0 on any axis.
        let slot = if x == f64::NEG_INFINITY {
            0
        } else {
            slot_of(x)
        };
        pairs.push((slot as u32, 4));
    }
    pairs.sort_unstable();
    pairs.dedup_by_key(|&mut (p, _)| p);
    (a.nan_indices, a.nan_kinds) = pairs.into_iter().unzip();
}

/// No-downsample axis = the union of distinct x. Each run drops its values onto
/// the slots it actually logged; everything else is a gap.
///
/// Duplicate x: a non-injective custom-x metric can log two y at the exact same x, which a shared distinct-x axis can't keep as two slots. Rather than drop a sample, the second+ collapse onto the slot and the spread surfaces as a y band. Any collision then makes EVERY series ship a dense envelope (min == max at single-sample slots) — the envelope contract is chart-wide, and a run without one would blank under the client's envelope rendering. A logged non-finite duplicate at the same x stays a plain marker; non-finite-wins isn't applied in this (rare) collision.
#[allow(clippy::too_many_arguments)]
fn shared_passthrough(
    axis: Vec<f64>,
    xs: &[&[f64]],
    plot: &[&[f64]],
    raw: &[&[f64]],
    kinds: &[&[u8]],
    xnan: &[&[f64]],
    is_smoothed: bool,
    slot_evaluators: &mut [Option<SlotEvaluator<'_>>],
) -> DenseChart {
    let nax = axis.len();
    let mut series = Vec::with_capacity(xs.len());
    let mut any_dup = false;
    for i in 0..xs.len() {
        let mut s = DenseSeries {
            values: vec![f64::NAN; nax],
            ..Default::default()
        };
        if is_smoothed {
            s.raw_values = vec![f64::NAN; nax];
        }
        let mut p = 0usize;
        for k in 0..xs[i].len() {
            let x = xs[i][k];
            while p < nax && axis[p] < x {
                p += 1;
            }
            if kinds[i][k] == 0 {
                let placed = if is_smoothed {
                    s.raw_values[p]
                } else {
                    s.values[p]
                };
                any_dup |= placed.is_finite();
                // A smoothed step or time run's values come from its smoother below; its samples supply the raw values.
                s.values[p] = plot[i].get(k).copied().unwrap_or(f64::NAN);
                if is_smoothed {
                    s.raw_values[p] = raw[i][k];
                }
            } else {
                s.nan_indices.push(p as u32);
                s.nan_kinds.push(kinds[i][k] as u32);
            }
        }
        // A smoothed step or time run: its smoother at each slot holding a finite sample of it.
        if let Some(evaluator) = slot_evaluators[i].as_mut() {
            for (p, &x) in axis.iter().enumerate() {
                if s.raw_values[p].is_finite() {
                    s.values[p] = evaluator.at(x);
                }
            }
        }
        // A raw column with no finite entry normalizes to ABSENT, like the envelope, holding no allocation: the wire cannot express an all-gap family for a value-less series, so the model must not either — the client's inflation would otherwise hash-diverge on every delta touching an all-marker series.
        if !s.raw_values.iter().any(|v| v.is_finite()) {
            s.raw_values = Vec::new();
        }
        fold_xnan(&mut s, xnan[i], |x| axis.partition_point(|&v| v < x));
        series.push(s);
    }
    // Any collision anywhere: every series ships its band, the RAW spread at each slot (as in the downsample path; unsmoothed, raw IS the plotted value), a lone sample its own min and max. Runs with no finite sample keep an empty envelope (all-NaN columns are never shipped).
    if any_dup {
        for (i, s) in series.iter_mut().enumerate() {
            let src = if is_smoothed { raw[i] } else { plot[i] };
            let (mut y_min, mut y_max) = (vec![f64::NAN; nax], vec![f64::NAN; nax]);
            let (mut p, mut any) = (0usize, false);
            for (k, &x) in xs[i].iter().enumerate() {
                while p < nax && axis[p] < x {
                    p += 1;
                }
                if kinds[i][k] == 0 {
                    // From NaN, which `min` and `max` pass over.
                    y_min[p] = y_min[p].min(src[k]);
                    y_max[p] = y_max[p].max(src[k]);
                    any = true;
                }
            }
            if any {
                s.min_values = y_min;
                s.max_values = y_max;
            }
        }
    }
    DenseChart {
        x_values: axis,
        xr_min: vec![f64::NAN; nax],
        xr_max: vec![f64::NAN; nax],
        series,
    }
}

/// One run onto the mixed axis. `env_src` is the value source for the bucket
/// mean + envelope and the raw slots' band (raw when smoothing, else the plot
/// itself). With a `slot_evaluator` (smoothed step and time charts) every value is the run's smoother at its slot's x and `rp` is empty; without one, raw slots take `rp`, a custom-x run's smoothed samples or an unsmoothed run's logged ones.
fn bucket_run(
    rx: &[f64],
    rp: &[f64],
    env_src: &[f64],
    rk: &[u8],
    is_smoothed: bool,
    slot_evaluator: Option<&mut SlotEvaluator<'_>>,
    ma: &MixedAxis,
) -> DenseSeries {
    let nax = ma.x.len();
    let mut out = DenseSeries {
        values: vec![f64::NAN; nax],
        ..Default::default()
    };
    let m = rx.len();
    if m == 0 {
        return out;
    }

    let grid = &ma.plan.grid;
    // Per-cell aggregates over this run's envelope-cell points — finite for mean/envelope, first non-finite kind for markers (non-finite samples never enter the envelope; the marker circle carries them). Every aggregate is LOCAL to its cell. A smoothed slot value also depends on the samples its smoother reads at the slot x, which the delta planners' slot rule covers.
    let mut cnt = vec![0u32; grid.cells];
    let mut sum_y = vec![0.0f64; grid.cells];
    let mut ymin = vec![f64::INFINITY; grid.cells];
    let mut ymax = vec![f64::NEG_INFINITY; grid.cells];
    let mut nfkind = vec![0u8; grid.cells];
    // The x of each cell's first finite sample, which a causal slot must have read ([`SlotSmoother::shows`]).
    let mut first_x = vec![f64::NAN; grid.cells];

    let mut y_min = vec![f64::NAN; nax];
    let mut y_max = vec![f64::NAN; nax];
    let mut any_range = false;
    let mut markers: Vec<(u32, u32)> = Vec::new();

    // Envelope cells aggregate; raw slots take the exact sample. Points and
    // slots both ascend, so raw lookups ride one shared forward pointer. A raw
    // slot's band is the sample itself (min == max, widened by duplicate-x),
    // pinching the dense envelope to the line through the raw region.
    let mut p = 0usize;
    for j in 0..m {
        let x = rx[j];
        let c = grid.index(x);
        if ma.plan.env[c] {
            if rk[j] != 0 {
                if nfkind[c] == 0 {
                    nfkind[c] = rk[j];
                }
                continue;
            }
            let v = env_src[j];
            if cnt[c] == 0 {
                first_x[c] = x;
            }
            cnt[c] += 1;
            sum_y[c] += v;
            ymin[c] = ymin[c].min(v);
            ymax[c] = ymax[c].max(v);
        } else {
            while p < nax && ma.x[p] < x {
                p += 1;
            }
            debug_assert!(p < nax && ma.x[p] == x, "raw point must own a slot");
            if rk[j] != 0 {
                markers.push((p as u32, rk[j] as u32));
            } else {
                if slot_evaluator.is_none() {
                    out.values[p] = rp[j];
                }
                // From NaN, which `min` and `max` pass over.
                y_min[p] = y_min[p].min(env_src[j]);
                y_max[p] = y_max[p].max(env_src[j]);
                any_range = true;
            }
        }
    }

    // Custom x interpolates its finite smoothed points at ascending envelope slots.
    let mut lerp = (is_smoothed && slot_evaluator.is_none()).then(|| {
        lerp_from(
            rx.iter()
                .copied()
                .zip(rp.iter().copied())
                .filter(|&(_, y)| y.is_finite()),
        )
    });
    for c in 0..grid.cells {
        let slot = ma.slot_of_cell[c];
        if slot == usize::MAX {
            continue;
        }
        // A logged non-finite sample wins the slot: value gaps, the bucket's
        // finite evidence still rides as a range (rule: marker keeps its band).
        if nfkind[c] != 0 {
            markers.push((slot as u32, nfkind[c] as u32));
        } else if cnt[c] == 0 {
            continue; // run has no data in this bucket — gap
        } else if is_smoothed {
            if let Some(lerp) = lerp.as_mut() {
                out.values[slot] = lerp(ma.x[slot]);
            }
        } else {
            // Bucket mean — a tooltip/hover/y-ranging summary, never stroked; the envelope is the rendering.
            out.values[slot] = sum_y[c] / cnt[c] as f64;
        }
        // Dense envelope: every occupied bucket ships min/max, min == max behind a single sample.
        if cnt[c] >= 1 {
            y_min[slot] = ymin[c];
            y_max[slot] = ymax[c];
            any_range = true;
        }
    }

    // Smoothed values, one ascending pass over every slot (a causal smoother's state only moves forward): the smoother at the slot's x, at each raw slot holding a finite sample of the run and each envelope slot whose cell holds only finite ones, where it shows ([`SlotSmoother::shows`]).
    if let Some(evaluator) = slot_evaluator {
        let mut cells = (0..grid.cells)
            .filter(|&c| ma.slot_of_cell[c] != usize::MAX)
            .peekable();
        for (k, &x) in ma.x.iter().enumerate() {
            let shown = match cells.next_if(|&c| ma.slot_of_cell[c] == k) {
                Some(c) => nfkind[c] == 0 && cnt[c] > 0 && evaluator.smoother.shows(x, first_x[c]),
                None => y_min[k].is_finite(),
            };
            if shown {
                out.values[k] = evaluator.at(x);
            }
        }
    }

    // Markers are finalized (sorted + de-duped by slot) in fold_xnan.
    (out.nan_indices, out.nan_kinds) = markers.into_iter().unzip();
    if any_range {
        out.min_values = y_min;
        out.max_values = y_max;
    }
    out
}

/// Wire kind of a sample's logged value: 0 = finite (never shipped as a
/// marker), 1 = NaN, 2 = +∞, 3 = -∞. Kind 4 — an unplottable x, carried to
/// the preceding plottable custom-x (or the first for a leading gap), the
/// first plottable log-axis x, or column 0 for a run with no plottable x — is
/// assigned in query.rs; it never describes
/// a y value. THE single
/// classification: emission derives both row eviction and the wire
/// `nan_kinds` from it, so a row cannot be marked one thing and ship another.
pub fn nan_kind(v: f64) -> u8 {
    if v.is_finite() {
        0
    } else if v.is_nan() {
        1
    } else if v > 0.0 {
        2
    } else {
        3
    }
}

/// Test helper: derive a plan and smooth one run's samples; see [`smooth_run_with_plan`].
#[cfg(test)]
pub fn smooth_run(
    xs: &[f64],
    y: &[f64],
    algo: Algorithm,
    window: u32,
    time_constant: f64,
    poly_order: u32,
    step_sized_x: bool,
) -> Vec<f64> {
    let plan = smoothing_plan(xs, algo, step_sized_x).0;
    smooth_run_with_plan(xs, y, algo, window, time_constant, poly_order, plan)
}

/// Run a smoother over a series' samples with its already-derived semantic plan: each sample's smoothed value, as custom x plots them (step and time charts evaluate the smoother at their slots instead, [`SlotEvaluator`]).
///
/// It takes a run's own consecutive samples, not a union-aligned vector, so the window means N of this run's samples whatever other runs share the chart, and smoothing cannot extend a line past the run's first or last logged step. `xs` are the run's steps or timestamps, ascending (custom x plots each at its own x). Savitzky–Golay over a uniform grid takes the index-space kernel, over an irregular one the x-aware fit, its window quoted in median sample intervals, so the same setting spans the same x range either way and a logging gap narrows a window instead of stretching it. Non-finite values are excluded from every window and their outputs stay NaN; showing them is the markers' job.
pub(crate) fn smooth_run_with_plan(
    xs: &[f64],
    y: &[f64],
    algo: Algorithm,
    window: u32,
    time_constant: f64,
    poly_order: u32,
    plan: SmoothingPlan,
) -> Vec<f64> {
    debug_assert_eq!(xs.len(), y.len());
    debug_assert!(window <= MAX_SMOOTHING_WINDOW);
    let mut out = match SlotSmoother::new(algo, window, time_constant, poly_order, plan) {
        SlotSmoother::Causal(smoother) => smoother.run(xs, y, &mut { CausalState::START }),
        SlotSmoother::SavitzkyGolay { .. } if uniform_gap(xs).is_some() => {
            savitzky_golay(y, window as usize, poly_order)
        }
        SlotSmoother::SavitzkyGolay { h_x, deg } => {
            let (x, v, mut cursor) = (|j: usize| xs[j], |j: usize| y[j], SavgolCursor::default());
            // Samples at one x share its fit (holes are masked below): a stalled clock costs one fit, not one per sample over a window that holds them all.
            let mut fit = (f64::NAN, f64::NAN);
            xs.iter()
                .map(|&xq| {
                    if xq != fit.0 {
                        fit = (xq, savgol_at(&x, &v, xs.len(), xq, h_x, deg, &mut cursor));
                    }
                    fit.1
                })
                .collect()
        }
    };
    for (o, v) in out.iter_mut().zip(y) {
        if !v.is_finite() {
            *o = f64::NAN;
        }
    }
    out
}

#[cfg(test)]
mod smooth_run_tests {
    use super::{
        median_dx, savgol_at, savgol_half_width, smooth_run, smoothing_plan, MedianGaps,
        SavgolCursor, SmoothingPlan, Width, SAVGOL_READS,
    };
    use crate::proto::smoothing_config::Algorithm;

    const ALGOS: [Algorithm; 3] = [
        Algorithm::Ema,
        Algorithm::SavitzkyGolay,
        Algorithm::Triangular,
    ];

    fn step_xs(n: usize) -> Vec<f64> {
        (0..n).map(|i| i as f64).collect()
    }

    #[test]
    fn non_finite_inputs_stay_holes() {
        let y = [1.0, 2.0, f64::NAN, 4.0, f64::INFINITY, 6.0, 7.0];
        let xs = step_xs(y.len());
        for algo in ALGOS {
            let out = smooth_run(&xs, &y, algo, 4, 0.5, 1, true);
            assert_eq!(out.len(), y.len());
            assert!(out[2].is_nan(), "{algo:?}: NaN input must stay a hole");
            assert!(out[4].is_nan(), "{algo:?}: Inf input must stay a hole");
            for (i, v) in out.iter().enumerate() {
                if y[i].is_finite() {
                    assert!(
                        v.is_finite(),
                        "{algo:?}: finite input produced non-finite output at {i}"
                    );
                }
            }
        }
    }

    #[test]
    fn inf_does_not_poison_neighbors() {
        let y = [5.0, 5.0, f64::INFINITY, 5.0, 5.0];
        let xs = step_xs(y.len());
        for algo in ALGOS {
            let out = smooth_run(&xs, &y, algo, 4, 0.5, 1, true);
            for (i, v) in out.iter().enumerate() {
                if y[i].is_finite() {
                    assert!((v - 5.0).abs() < 1e-9, "{algo:?}: neighbor at {i} got {v}");
                }
            }
        }
    }

    #[test]
    fn uniform_grid_takes_index_fast_path() {
        let y: Vec<f64> = (0..50).map(|i| (i as f64 * 0.3).sin()).collect();
        let xs = step_xs(y.len());
        let a = smooth_run(&xs, &y, Algorithm::SavitzkyGolay, 8, 0.3, 2, true);
        let b = super::savitzky_golay(&y, 8, 2);
        assert_eq!(a, b, "uniform grid must give the exact index-space result");
    }

    /// EMA output must equal the explicit exponential kernel in x, out_i = Σ_j e^(−(xᵢ−xⱼ)/τ)·v_j / Σ_j e^(−(xᵢ−xⱼ)/τ), on uniform AND irregular grids (gaps decay by their width).
    #[test]
    fn ema_matches_explicit_kernel() {
        let tau = 4.5f64;
        let cases: Vec<Vec<f64>> = vec![
            (0..40).map(|i| i as f64).collect(), // contiguous steps
            vec![0.0, 1.0, 2.0, 10.0, 11.0, 50.0, 51.0, 52.0], // gappy steps
        ];
        for xs in cases {
            let y: Vec<f64> = xs.iter().map(|x| (x * 0.7).sin() * 10.0 + 3.0).collect();
            let out = smooth_run(&xs, &y, Algorithm::Ema, 0, tau, 0, true);
            for i in 0..y.len() {
                let (mut num, mut den) = (0.0, 0.0);
                for j in 0..=i {
                    let w = (-(xs[i] - xs[j]) / tau).exp();
                    num += w * y[j];
                    den += w;
                }
                let want = num / den;
                assert!((out[i] - want).abs() < 1e-9, "i={i}: {} vs {want}", out[i]);
            }
        }
    }

    /// Debiasing: early output is the weighted mean of what's been seen,
    /// not dragged toward the seed sample like classic EMA.
    #[test]
    fn ema_is_debiased() {
        let y = [0.0, 1.0, 1.0, 1.0];
        let xs = step_xs(y.len());
        let out = smooth_run(&xs, &y, Algorithm::Ema, 0, -1.0 / 0.9f64.ln(), 0, true);
        assert_eq!(out[0], 0.0);
        // classic seeded EMA gives 0.1 here; the debiased weighted mean of
        // {0, 1} with weights {0.9, 1} is 1/1.9
        assert!((out[1] - 1.0 / 1.9).abs() < 1e-12, "{}", out[1]);
    }

    #[test]
    fn ema_polyfit_reproduces_polynomials_after_warmup() {
        let xs = step_xs(100);
        for order in 1..=2 {
            let y: Vec<f64> = xs
                .iter()
                .map(|x| {
                    if order == 1 {
                        3.0 * x - 4.0
                    } else {
                        0.2 * x * x - 3.0 * x + 7.0
                    }
                })
                .collect();
            let out = smooth_run(&xs, &y, Algorithm::Ema, 0, 10.0, order, true);
            for i in order as usize..xs.len() {
                assert!(
                    (out[i] - y[i]).abs() < 1e-7 * (1.0 + y[i].abs()),
                    "order={order} i={i}: {} vs {}",
                    out[i],
                    y[i]
                );
            }
        }
    }

    /// Order 0 is the x-weighted running mean: a sample's weight is 1 plus
    /// its x-distance from the first sample (dx_ref units), so a gap lifts
    /// weight by its width — not by one, the way arrival rank would.
    #[test]
    fn triangular_order_zero_weights_by_x_distance() {
        let xs = [0.0, 1.0, 2.0, 8.0, 9.0, 12.0]; // gaps of 6 and 3
        let y = [3.0, -1.0, 4.0, 2.0, 5.0, -2.0];
        let out = smooth_run(&xs, &y, Algorithm::Triangular, 0, 0.0, 0, true); // step mode: dx_ref = 1
        let (mut num, mut den) = (0.0f64, 0.0f64);
        for n in 0..xs.len() {
            let w = 1.0 + (xs[n] - xs[0]); // dx_ref = 1
            num += w * y[n];
            den += w;
            assert!(
                (out[n] - num / den).abs() < 1e-9,
                "n={n}: {} vs {}",
                out[n],
                num / den
            );
        }
    }

    /// A local fit of degree ≥ the data's degree reproduces it exactly at
    /// every point past warmup, whatever the weights — the conditioning
    /// rescale must not spoil that even at order 2 over a long run.
    #[test]
    fn triangular_reproduces_polynomials() {
        let xs = step_xs(2000);
        for order in 1..=2 {
            let y: Vec<f64> = xs
                .iter()
                .map(|x| {
                    if order == 1 {
                        2.0 * x - 3.0
                    } else {
                        0.1 * x * x - 2.0 * x + 5.0
                    }
                })
                .collect();
            let out = smooth_run(&xs, &y, Algorithm::Triangular, 0, 0.0, order, true);
            for i in order as usize..xs.len() {
                assert!(
                    (out[i] - y[i]).abs() < 1e-6 * (1.0 + y[i].abs()),
                    "order={order} i={i}: {} vs {}",
                    out[i],
                    y[i]
                );
            }
        }
    }

    /// The fit is genuinely in x: on an irregular grid (dx_ref = median
    /// interval, origin shifted by real gaps) a low-degree polynomial is
    /// still reproduced exactly past warmup, whatever the weights.
    #[test]
    fn triangular_reproduces_polynomials_irregular_grid() {
        let mut xs = vec![0.0f64];
        for (i, gap) in [1.0, 3.0, 1.0, 1.0, 7.0, 2.0, 1.0, 4.0]
            .iter()
            .cycle()
            .take(80)
            .enumerate()
        {
            xs.push(xs[i] + gap);
        }
        for order in 1..=2 {
            let y: Vec<f64> = xs
                .iter()
                .map(|&x| {
                    if order == 1 {
                        -1.5 * x + 2.0
                    } else {
                        0.3 * x * x + x - 4.0
                    }
                })
                .collect();
            let out = smooth_run(&xs, &y, Algorithm::Triangular, 0, 0.0, order, false);
            for i in order as usize..xs.len() {
                assert!(
                    (out[i] - y[i]).abs() < 1e-6 * (1.0 + y[i].abs()),
                    "order={order} i={i}: {} vs {}",
                    out[i],
                    y[i]
                );
            }
        }
    }

    /// Causal and prefix-stable: the value at each point is a pure function
    /// of the samples up to it, so a longer run leaves earlier values
    /// bit-for-bit unchanged, which the causal slot rule relies on.
    #[test]
    fn triangular_is_prefix_stable() {
        let xs = step_xs(64);
        let y: Vec<f64> = (0..64)
            .map(|i| ((i * 11 % 17) as f64).sin() * 3.0 + 1.0)
            .collect();
        let full = smooth_run(&xs, &y, Algorithm::Triangular, 0, 0.0, 2, true);
        for cut in [3usize, 25, 63] {
            let pre = smooth_run(
                &xs[..=cut],
                &y[..=cut],
                Algorithm::Triangular,
                0,
                0.0,
                2,
                true,
            );
            for i in 0..=cut {
                assert_eq!(
                    pre[i].to_bits(),
                    full[i].to_bits(),
                    "cut={cut} i={i}: {} vs {}",
                    pre[i],
                    full[i]
                );
            }
        }
    }

    /// Prefix-stability holds on a TIME axis too when the median interval is unchanged: a uniform grid keeps it fixed under appends, so earlier values stay bit-exact. A median shift rescales outputs and is gated by the exact semantic smoothing plan.
    #[test]
    fn triangular_time_axis_prefix_stable_under_fixed_median() {
        let xs: Vec<f64> = (0..64).map(|i| i as f64 * 10.0).collect(); // uniform gaps ⇒ median stays 10
        let y: Vec<f64> = (0..64)
            .map(|i| ((i * 11 % 17) as f64).cos() * 2.0 - 1.0)
            .collect();
        let full = smooth_run(&xs, &y, Algorithm::Triangular, 0, 0.0, 2, false); // time mode
        for cut in [4usize, 30, 63] {
            let pre = smooth_run(
                &xs[..=cut],
                &y[..=cut],
                Algorithm::Triangular,
                0,
                0.0,
                2,
                false,
            );
            for i in 0..=cut {
                assert_eq!(
                    pre[i].to_bits(),
                    full[i].to_bits(),
                    "cut={cut} i={i}: {} vs {}",
                    pre[i],
                    full[i]
                );
            }
        }
    }

    /// The x-aware savgol path is a real fit in x: a quadratic sampled on
    /// an irregular grid is reproduced exactly, edges included.
    #[test]
    fn savgol_exact_quadratic_on_irregular_grid() {
        let mut xs = vec![0.0f64];
        for (i, gap) in [1.0, 3.0, 1.0, 1.0, 7.0, 2.0, 1.0, 1.0, 1.0, 4.0]
            .iter()
            .cycle()
            .take(60)
            .enumerate()
        {
            xs.push(xs[i] + gap);
        }
        let q = |x: f64| 0.5 * x * x - 3.0 * x + 7.0;
        let y: Vec<f64> = xs.iter().map(|&x| q(x)).collect();
        let out = smooth_run(&xs, &y, Algorithm::SavitzkyGolay, 12, 0.0, 2, true);
        for (i, &x) in xs.iter().enumerate() {
            assert!(
                (out[i] - q(x)).abs() < 1e-6 * (1.0 + q(x).abs()),
                "i={i} x={x}: {} vs {}",
                out[i],
                q(x)
            );
        }
    }

    /// Custom x's irregular Savitzky-Golay is the slot fit at each sample, its half-width the window's in median gaps (10 here).
    #[test]
    fn custom_x_savgol_is_the_slot_fit_at_each_sample() {
        let xs = [
            0.0, 10.0, 30.0, 40.0, 50.0, 80.0, 90.0, 100.0, 140.0, 150.0, 160.0,
        ];
        let y: Vec<f64> = (0..xs.len()).map(|i| ((i * 37) % 11) as f64).collect();
        assert_eq!(median_dx(&xs).0, 10.0);
        let out = smooth_run(&xs, &y, Algorithm::SavitzkyGolay, 4, 0.0, 1, false);
        let width = Width::fixed(savgol_half_width(4, 10.0));
        for (i, &xq) in xs.iter().enumerate() {
            let mut cursor = SavgolCursor::default();
            let want = savgol_at(&|j| xs[j], &|j| y[j], xs.len(), xq, width, 1, &mut cursor);
            assert_eq!(out[i].to_bits(), want.to_bits(), "at {xq}");
        }
    }

    /// Repeated timestamps (fast logging, importer rows without _timestamp) keep the unit at the median gap between distinct timestamps, so every smoother stays finite and smooths across them (AI-1508).
    #[test]
    fn repeated_timestamps_keep_the_smoothing_unit() {
        // Gaps 0, 0, 1, 5, 9: the positive gaps' median is 5; counting the zeros would give 1.
        assert_eq!(median_dx(&[0.0, 0.0, 0.0, 1.0, 6.0, 15.0]).0, 5.0);
        let xs = [0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2.0];
        let y = [1.0, 2.0, 3.0, 4.0, 5.0, 4.0, -4.0];
        for algo in ALGOS {
            let out = smooth_run(&xs, &y, algo, 5, 0.5, 0, false);
            assert!(out.iter().all(|v| v.is_finite()), "{algo:?}: {out:?}");
            assert_ne!(out[6], y[6], "{algo:?} returned the raw last sample");
        }
    }

    /// Savitzky–Golay over a series at one x weighs its samples by x distance, which is zero for all of them, not by their order: every output is the mean of the finite samples, at any degree, and holes stay holes.
    #[test]
    fn savgol_at_one_x_is_the_x_aware_fit() {
        let xs = [1000.0; 3];
        assert_eq!(
            smoothing_plan(&xs, Algorithm::SavitzkyGolay, false).0,
            SmoothingPlan::Median(1f64.to_bits())
        );
        for order in 0..=2 {
            let out = smooth_run(
                &xs,
                &[0.0, 0.0, 10.0],
                Algorithm::SavitzkyGolay,
                3,
                0.0,
                order,
                false,
            );
            assert!(
                out.iter().all(|v| (v - 10.0 / 3.0).abs() < 1e-12),
                "{order}: {out:?}"
            );
        }
        let out = smooth_run(
            &[5.0; 5],
            &[f64::NAN, 0.0, f64::NAN, 10.0, 2.0],
            Algorithm::SavitzkyGolay,
            3,
            0.0,
            2,
            false,
        );
        assert!(out[0].is_nan() && out[2].is_nan(), "{out:?}");
        assert!(
            [1, 3, 4].iter().all(|&i| (out[i] - 4.0).abs() < 1e-12),
            "{out:?}"
        );
        // A positive uniform spacing is its gap, one sample's 1, with no median sort.
        for (xs, gap) in [(&[0.0, 2.0, 4.0][..], 2.0), (&[7.0], 1.0)] {
            super::take_spacing_derivations();
            assert_eq!(
                smoothing_plan(xs, Algorithm::SavitzkyGolay, false).0,
                SmoothingPlan::Median(f64::to_bits(gap))
            );
            assert_eq!(super::take_spacing_derivations(), (1, 0));
        }
    }

    /// A median plan comes with the counts held state would take from every gap of its series (`MedianGaps::extended` from none), uniform or not; gaps that move the median off the plan's, up or down, extend to none.
    #[test]
    fn a_median_plan_comes_with_its_gap_counts() {
        let irregular: Vec<f64> = (0..200)
            .scan(0.0, |x, i| {
                *x += ((i * 7) % 5) as f64;
                Some(*x)
            })
            .collect();
        let uniform: Vec<f64> = (0..50).map(|i| 1e12 + 1000.0 * i as f64).collect();
        let cases: [&[f64]; 8] = [
            &[],
            &[7.0],
            &[7.0, 7.0],
            &[0.0, 1.0, 3.0],
            &[0.0, 0.0, 0.0, 1.0, 6.0, 15.0],
            // Gaps 1, 1, 2, 3, 3, 3: half of them below the median 3, `extended`'s lower bound.
            &[0.0, 1.0, 2.0, 4.0, 7.0, 10.0, 13.0],
            &irregular,
            &uniform,
        ];
        for xs in cases {
            for algo in ALGOS {
                let (plan, gaps) = smoothing_plan(xs, algo, false);
                let SmoothingPlan::Median(m) = plan else {
                    unreachable!("a median plan");
                };
                let counted = MedianGaps::default().extended(f64::from_bits(m), xs.iter().copied());
                assert_eq!(counted, Some(gaps), "{algo:?} over {xs:?}");
            }
        }
        // Gaps 5, 9, 9 and 5, 1, 1.
        for keys in [[0.0, 5.0, 14.0, 23.0], [0.0, 5.0, 6.0, 7.0]] {
            assert_eq!(MedianGaps::default().extended(5.0, keys.into_iter()), None);
        }
    }

    /// An append keeps a median plan exactly when the whole series' median is unchanged: [`MedianGaps::extended`] against [`median_dx`] over the joined keys, for random keys with repeated x and colliding gaps, split anywhere.
    #[test]
    fn extended_counts_keep_exactly_the_median() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = |m: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % m
        };
        for _ in 0..5_000 {
            let n = 2 + next(40) as usize;
            let mut keys = vec![0.0];
            for _ in 1..n {
                let gap = [0.0, 1.0, 1.0, 2.0, 3.0, 5.0][next(6) as usize];
                keys.push(keys[keys.len() - 1] + gap);
            }
            let split = 1 + next(n as u64 - 1) as usize;
            let (m, gaps) = median_dx(&keys[..split]);
            let kept = gaps.extended(m, keys[split - 1..].iter().copied());
            let fresh = median_dx(&keys).0.to_bits() == m.to_bits();
            assert_eq!(kept.is_some(), fresh, "{keys:?} split at {split}");
        }
    }

    /// Samples at one x share one fit, so a stalled clock's run costs one window, not one per sample.
    #[test]
    fn savgol_fits_a_stall_once() {
        let n = 2_000;
        let y: Vec<f64> = (0..n).map(|i| (i % 7) as f64).collect();
        SAVGOL_READS.with(|reads| reads.set(0));
        smooth_run(
            &vec![0.0; n],
            &y,
            Algorithm::SavitzkyGolay,
            20,
            0.0,
            1,
            false,
        );
        assert_eq!(SAVGOL_READS.with(|reads| reads.get()), n);
    }
}

#[cfg(test)]
mod shared_chart_tests {
    use super::*;

    fn spec_of(target: usize, step: bool) -> GridSpec {
        GridSpec {
            target,
            is_step_axis: step,
            log_buckets: false,
            shift_one: false,
        }
    }

    fn log_step_spec(target: usize) -> GridSpec {
        GridSpec {
            target,
            is_step_axis: true,
            log_buckets: true,
            shift_one: true,
        }
    }

    /// A custom-x log chart's: its ladder never shifts.
    fn log_custom_spec(target: usize) -> GridSpec {
        GridSpec {
            target,
            is_step_axis: false,
            log_buckets: true,
            shift_one: false,
        }
    }

    /// Call helper: builds slice views; `raw` mirrors `plot` when smoothed.
    fn run_spec(
        xs: &[Vec<f64>],
        plot: &[Vec<f64>],
        kinds: &[Vec<u8>],
        smoothed: bool,
        spec: GridSpec,
    ) -> DenseChart {
        let empty: Vec<Vec<f64>> = xs.iter().map(|_| Vec::new()).collect();
        run_xnan(xs, plot, kinds, &empty, smoothed, spec)
    }

    fn run_xnan(
        xs: &[Vec<f64>],
        plot: &[Vec<f64>],
        kinds: &[Vec<u8>],
        xnan: &[Vec<f64>],
        smoothed: bool,
        spec: GridSpec,
    ) -> DenseChart {
        let raw: Vec<Vec<f64>> = if smoothed {
            plot.to_vec()
        } else {
            xs.iter().map(|_| Vec::new()).collect()
        };
        let xs_s: Vec<&[f64]> = xs.iter().map(|v| v.as_slice()).collect();
        let plot_s: Vec<&[f64]> = plot.iter().map(|v| v.as_slice()).collect();
        let raw_s: Vec<&[f64]> = raw.iter().map(|v| v.as_slice()).collect();
        let kinds_s: Vec<&[u8]> = kinds.iter().map(|v| v.as_slice()).collect();
        let xnan_s: Vec<&[f64]> = xnan.iter().map(|v| v.as_slice()).collect();
        // Smoothed runs evaluate a window-3 local-linear fit at their slots, exact on linear data.
        let mut slot_smoothers: Vec<_> = if smoothed {
            xs.iter()
                .zip(&raw)
                .map(|(x, y)| {
                    let plan = smoothing_plan(x, Algorithm::SavitzkyGolay, true).0;
                    let smoother = SlotSmoother::new(Algorithm::SavitzkyGolay, 3, 1.0, 1, plan);
                    Some(SlotEvaluator::over(smoother, x, y))
                })
                .collect()
        } else {
            xs.iter().map(|_| None).collect()
        };
        shared_chart(
            &xs_s,
            &plot_s,
            &raw_s,
            &kinds_s,
            &xnan_s,
            smoothed,
            &mut slot_smoothers,
            spec,
            AxisDecision::of(&xs_s, spec),
        )
        .0
    }

    fn run_axis(
        xs: &[Vec<f64>],
        plot: &[Vec<f64>],
        kinds: &[Vec<u8>],
        smoothed: bool,
        target: usize,
        step: bool,
    ) -> DenseChart {
        run_spec(xs, plot, kinds, smoothed, spec_of(target, step))
    }

    /// Step-axis shorthand — the default for these tests.
    fn run(
        xs: &[Vec<f64>],
        plot: &[Vec<f64>],
        kinds: &[Vec<u8>],
        smoothed: bool,
        target: usize,
    ) -> DenseChart {
        run_axis(xs, plot, kinds, smoothed, target, true)
    }

    /// A bucket's x is the average of the first and last point in it (doc:
    /// steps 0..=3 -> 1.5), and the chart endpoints obey the same bucket rule
    /// as every other point. The bucket's x extent rides at chart level (xr).
    #[test]
    fn bucket_center_is_endpoint_average() {
        let xs = [vec![0.0, 1.0, 2.0, 3.0]];
        let plot = [vec![10.0, 20.0, 30.0, 40.0]];
        let kinds = vec![vec![0u8; 4]];
        let sc = run(&xs, &plot, &kinds, false, 1);
        assert_eq!(sc.x_values, vec![1.5], "0 through 3 share one bucket");
        let s = &sc.series[0];
        assert_eq!((sc.xr_min[0], sc.xr_max[0]), (0.0, 3.0));
        assert_eq!((s.min_values[0], s.max_values[0]), (10.0, 40.0));
        assert_eq!(s.values[0], 25.0, "bucket mean, not a sample pick");
    }

    /// Two runs of different length and density emit on ONE identical axis,
    /// and both carry ranges — the original bug was a short-dense run getting
    /// none while a long run did.
    #[test]
    fn runs_share_one_axis_and_both_get_ranges() {
        let a_x: Vec<f64> = (0..=1000).map(|i| i as f64).collect();
        let b_x: Vec<f64> = (0..=200).map(|i| i as f64).collect();
        let a_y: Vec<f64> = a_x.iter().map(|x| (x * 0.05).sin()).collect();
        let b_y: Vec<f64> = b_x.iter().map(|x| (x * 0.2).cos()).collect();
        let xs = vec![a_x.clone(), b_x.clone()];
        let plot = vec![a_y, b_y];
        let kinds = vec![vec![0u8; a_x.len()], vec![0u8; b_x.len()]];
        let sc = run(&xs, &plot, &kinds, false, 64);
        let n = sc.x_values.len();
        assert!(
            sc.x_values.windows(2).all(|w| w[0] < w[1]),
            "axis ascending & unique"
        );
        for s in &sc.series {
            assert_eq!(s.values.len(), n, "every series aligned to the shared axis");
            assert!(!s.min_values.is_empty(), "both runs carry a min/max band");
            assert_eq!(s.min_values.len(), n);
        }
        // The short run is present only over its own extent (NaN past step 200).
        let b = &sc.series[1];
        let last_b = sc.x_values.iter().rposition(|&x| x <= 200.0).unwrap();
        assert!(b.values[last_b].is_finite());
        assert!(b.values[n - 1].is_nan(), "short run is a gap past its end");
    }

    /// Interior runs share the same slots a longer run defines.
    #[test]
    fn short_run_uses_long_runs_slots() {
        let a = vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0];
        let b = vec![0.0, 1.0, 2.0, 3.0];
        let v = |xs: &[f64]| xs.iter().map(|x| x * 10.0).collect::<Vec<_>>();
        let xs = vec![a.clone(), b.clone()];
        let plot = vec![v(&a), v(&b)];
        let kinds = vec![vec![0u8; a.len()], vec![0u8; b.len()]];
        let sc = run(&xs, &plot, &kinds, false, 2);
        assert_eq!(sc.x_values, vec![1.5, 5.5]);
        let (sa, sb) = (&sc.series[0], &sc.series[1]);
        assert!(
            sa.values.iter().all(|v| v.is_finite()),
            "long run fills every slot"
        );
        assert!(sb.values[1].is_nan(), "short run gaps past its end");
        assert_eq!(sb.values[0], 15.0, "short run's bucket mean of steps 0..=3");
        assert_eq!(
            (sb.min_values[0], sb.max_values[0]),
            (0.0, 30.0),
            "its band still reaches its true last value"
        );
    }

    /// Few enough distinct positions: no downsampling, axis is the union and
    /// no envelope is produced.
    #[test]
    fn sparse_chart_passes_through_on_union_axis() {
        let xs = vec![vec![0.0, 5.0, 10.0], vec![0.0, 5.0, 10.0]];
        let plot = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]];
        let kinds = vec![vec![0u8; 3], vec![0u8; 3]];
        let sc = run(&xs, &plot, &kinds, false, 100);
        assert_eq!(sc.x_values, vec![0.0, 5.0, 10.0]);
        for s in &sc.series {
            assert_eq!(s.values.len(), 3);
            assert!(s.min_values.is_empty(), "no ranges without downsampling");
            assert!(s.values.iter().all(|v| v.is_finite()));
        }
        assert!(
            sc.xr_min.iter().all(|v| v.is_nan()),
            "no bucket, no x extent"
        );
    }

    /// Smoothed value rides the bucket center (the smoother evaluated there); the band comes
    /// from the raw points in the bucket.
    #[test]
    fn smoothed_value_is_at_center_band_from_raw() {
        let xs = vec![vec![0.0, 1.0, 2.0, 3.0]];
        let plot = vec![vec![10.0, 20.0, 30.0, 40.0]];
        let kinds = vec![vec![0u8; 4]];
        let sc = run(&xs, &plot, &kinds, true, 1);
        assert_eq!(sc.x_values, vec![1.5]);
        let s = &sc.series[0];
        assert_eq!(s.values[0], 25.0, "smoothed line evaluated at x=1.5");
        assert_eq!(
            (s.min_values[0], s.max_values[0]),
            (10.0, 40.0),
            "band from every raw point in the bucket"
        );
        assert!(
            s.raw_values.is_empty(),
            "downsampled charts ship no raw column — the envelope IS the raw representation"
        );
    }

    /// A mid-chart run's smoothed value at its first/last bucket is the smoother
    /// evaluated at the bucket center — NOT the run's endpoint
    /// value. The raw evidence rides exclusively in the envelope.
    #[test]
    fn smoothed_midchart_endpoint_uses_center_not_endpoint_value() {
        // Run A stretches the grid; run B (steps 1..=7, value = step) shares
        // the first bucket with A's step 0, centering that bucket at 3.5.
        let xs = vec![vec![0.0, 12.0], vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]];
        let plot = vec![vec![0.0, 12.0], vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]];
        let kinds = vec![vec![0u8; 2], vec![0u8; 7]];
        let sc = run(&xs, &plot, &kinds, true, 2);
        assert_eq!(sc.x_values, vec![3.5, 12.0]);
        let b = &sc.series[1];
        // Smoothed line: evaluated at center 3.5, not B's endpoint value.
        assert_eq!(b.values[0], 3.5);
        assert!(b.values[1].is_nan(), "B has no sample in the second bucket");
        // No raw column; the raw spread is the envelope.
        assert!(b.raw_values.is_empty());
        assert_eq!((b.min_values[0], b.max_values[0]), (1.0, 7.0));
    }

    /// A logged non-finite sample wins its slot (value gaps, marker recorded);
    /// the slot still carries the bucket's finite range when >=1 point stands.
    #[test]
    fn logged_nan_becomes_a_marker_slot() {
        let xs = vec![vec![0.0, 1.0, 2.0, 3.0]];
        let plot = vec![vec![10.0, f64::NAN, 30.0, 40.0]];
        let kinds = vec![vec![0u8, 1, 0, 0]];
        let sc = run(&xs, &plot, &kinds, false, 1);
        assert_eq!(sc.x_values, vec![1.5]);
        let s = &sc.series[0];
        assert_eq!(s.nan_indices, vec![0]);
        assert_eq!(s.nan_kinds, vec![1]);
        assert!(s.values[0].is_nan(), "marker slot has no plotted value");
        // The marker's bucket keeps all finite evidence in its dense band.
        assert_eq!((s.min_values[0], s.max_values[0]), (10.0, 40.0));
    }

    /// Downsampled values are the bucket MEAN and the envelope is DENSE:
    /// min == max stands behind single-point buckets, so the band alone is a
    /// complete raw representation — no NaN placeholders for the client to
    /// paper over.
    #[test]
    fn downsampled_mean_and_dense_envelope() {
        let a: Vec<f64> = (0..=10).map(|i| i as f64).collect();
        let av: Vec<f64> = a.iter().map(|x| x * 10.0).collect();
        // B logs a single interior point.
        let xs = vec![a.clone(), vec![5.0]];
        let plot = vec![av, vec![55.5]];
        let kinds = vec![vec![0u8; a.len()], vec![0u8; 1]];
        let sc = run(&xs, &plot, &kinds, false, 4);
        assert!(sc.x_values.len() < a.len(), "downsampled");
        let s = &sc.series[0];
        for i in 0..sc.x_values.len() {
            assert!(s.values[i].is_finite(), "dense run occupies every slot");
            assert!(
                s.min_values[i] <= s.values[i] && s.values[i] <= s.max_values[i],
                "mean sits inside its own envelope at slot {i}"
            );
        }
        let b = &sc.series[1];
        assert!(
            !b.min_values.is_empty(),
            "a run with any finite sample ships an envelope"
        );
        let slot = (0..sc.x_values.len())
            .find(|&i| b.values[i].is_finite())
            .unwrap();
        assert_eq!(b.values[slot], 55.5);
        assert_eq!(
            (b.min_values[slot], b.max_values[slot]),
            (55.5, 55.5),
            "single sample: min == max"
        );
        // The bucket's x extent is chart-level and unioned across runs: A's
        // steps span the bucket even though B logged only x=5.
        assert!(sc.xr_min[slot] <= 5.0 && sc.xr_max[slot] >= 5.0);
    }

    /// A logged ±inf is marker-only, exactly like NaN: it wins the slot's value but never enters the envelope — the wire carries no non-finite value other than NaN, and the client's border circle is the inf's rendering. Its x still counts toward the bucket extent (it is real data at a real x).
    #[test]
    fn inf_is_marker_only() {
        let xs = vec![vec![0.0, 1.0, 2.0, 3.0]];
        let plot = vec![vec![10.0, f64::INFINITY, 30.0, 40.0]];
        let kinds = vec![vec![0u8, 2, 0, 0]];
        let sc = run(&xs, &plot, &kinds, false, 1);
        assert_eq!(sc.x_values, vec![1.5]);
        let s = &sc.series[0];
        assert_eq!(
            (s.nan_indices.clone(), s.nan_kinds.clone()),
            (vec![0], vec![2])
        );
        assert!(s.values[0].is_nan(), "marker wins the value");
        assert_eq!(
            (s.min_values[0], s.max_values[0]),
            (10.0, 40.0),
            "envelope from the finite samples only"
        );
        assert_eq!(
            (sc.xr_min[0], sc.xr_max[0]),
            (0.0, 3.0),
            "x extent counts every real x"
        );
    }

    /// Custom-x can be non-injective (two y at the same x). On a shared
    /// distinct-x axis they collapse to one slot and surface as a y-range
    /// rather than dropping a sample — and one collision makes the whole
    /// chart's envelopes ship dense.
    #[test]
    fn duplicate_x_collapses_to_a_y_range() {
        // Four samples at x = 1, the band's extremes in the first two: the fold carries them past the last two.
        let xs = vec![vec![0.0, 1.0, 1.0, 1.0, 1.0, 2.0], vec![0.0, 2.0]];
        let plot = vec![vec![10.0, 20.0, 30.0, 24.0, 26.0, 40.0], vec![7.0, 8.0]];
        let kinds = vec![vec![0u8; 6], vec![0u8; 2]];
        // custom-x (non-injective): not a step axis. sparse -> passthrough.
        let sc = run_axis(&xs, &plot, &kinds, false, 100, false);
        assert_eq!(sc.x_values, vec![0.0, 1.0, 2.0]);
        let s = &sc.series[0];
        assert_eq!(
            (s.min_values[1], s.max_values[1]),
            (20.0, 30.0),
            "spread shown as a band"
        );
        assert_eq!(
            (s.min_values[0], s.max_values[0]),
            (10.0, 10.0),
            "dense: min == max off-collision"
        );
        assert_eq!(s.values[0], 10.0);
        assert_eq!(s.values[2], 40.0);
        // The collision-free run ships a dense band too — the contract is chart-wide.
        let b = &sc.series[1];
        assert_eq!((b.min_values[0], b.max_values[0]), (7.0, 7.0));
        assert_eq!((b.min_values[2], b.max_values[2]), (8.0, 8.0));
        assert!(b.min_values[1].is_nan(), "gap slots stay gaps");
    }

    /// A smoothed custom-x run (no slot evaluator) with a repeated x bands its raw spread there, not its smoothed one.
    #[test]
    fn custom_x_smoothed_duplicate_bands_raw() {
        let xs: Vec<&[f64]> = vec![&[0.0, 1.0, 1.0, 2.0]];
        let plot: Vec<&[f64]> = vec![&[5.0, 6.0, 7.0, 8.0]];
        let raw: Vec<&[f64]> = vec![&[10.0, 20.0, 30.0, 40.0]];
        let kinds: Vec<&[u8]> = vec![&[0, 0, 0, 0]];
        let xnan: Vec<&[f64]> = vec![&[]];
        let spec = spec_of(100, false);
        let decision = AxisDecision::of(&xs, spec);
        let mut none = vec![None];
        let chart = shared_chart(
            &xs, &plot, &raw, &kinds, &xnan, true, &mut none, spec, decision,
        )
        .0;
        let s = &chart.series[0];
        assert_eq!((s.min_values[1], s.max_values[1]), (20.0, 30.0));
        assert_eq!((s.min_values[0], s.max_values[0]), (10.0, 10.0));
        assert_eq!(s.values[1], 7.0);
    }

    /// Beside another run's repeated x, a smoothed run with no finite sample gets neither a band nor a raw column, nor an allocation for one: all-NaN columns are never shipped, and held state charges a column's capacity.
    #[test]
    fn an_all_marker_run_holds_no_band_or_raw_column() {
        let xs: Vec<&[f64]> = vec![&[0.0, 1.0, 1.0], &[0.0, 2.0]];
        let plot: Vec<&[f64]> = vec![&[5.0, 6.0, 7.0], &[f64::NAN, f64::NAN]];
        let raw: Vec<&[f64]> = vec![&[10.0, 20.0, 30.0], &[f64::NAN, f64::NAN]];
        let kinds: Vec<&[u8]> = vec![&[0, 0, 0], &[1, 1]];
        let xnan: Vec<&[f64]> = vec![&[], &[]];
        let spec = spec_of(100, false);
        let decision = AxisDecision::of(&xs, spec);
        let mut none = vec![None, None];
        let chart = shared_chart(
            &xs, &plot, &raw, &kinds, &xnan, true, &mut none, spec, decision,
        )
        .0;
        let (banded, gap) = (&chart.series[0], &chart.series[1]);
        assert_eq!((banded.min_values[1], banded.max_values[1]), (20.0, 30.0));
        assert!(gap.min_values.is_empty() && gap.max_values.is_empty());
        assert_eq!(gap.raw_values.capacity(), 0);
    }

    /// A kind-4 marker takes the slot holding its anchor sample even when another slot is nearer in x: anchor 8 opens the [8,16) envelope centred at 11.5, while the [0,8) envelope centre 5.5 is nearer. A raw anchor keeps its exact slot, where a logged y-kind wins.
    #[test]
    fn kind4_marks_its_anchor_cell_not_the_nearest_slot() {
        let xs = [
            vec![8.0, 20.0],
            vec![4.0, 5.0, 6.0, 7.0, 13.0, 14.0, 15.0, 17.0],
        ];
        let plot = [vec![1.0, f64::NAN], vec![2.0; 8]];
        let kinds = [vec![0u8, 1], vec![0u8; 8]];
        let xnan = [vec![8.0f64, 20.0], vec![]];
        let sc = run_xnan(&xs, &plot, &kinds, &xnan, false, spec_of(2, false));
        assert_eq!(sc.x_values, vec![5.5, 11.5, 17.0, 20.0]);
        let s = &sc.series[0];
        assert_eq!(s.nan_indices, vec![1, 3]);
        assert_eq!(
            s.nan_kinds,
            vec![4, 1],
            "the logged NaN at x=20 wins its slot"
        );
        assert!(s.values[1].is_finite(), "kind 4 annotates, does not evict");
    }

    /// Custom-x sample whose X was logged non-finite becomes a kind-4 marker
    /// on its anchor's slot, without evicting that slot's value.
    #[test]
    fn custom_x_nonfinite_x_becomes_kind4_marker() {
        let xs = [vec![0.0, 1.0, 2.0, 3.0]];
        let plot = [vec![10.0, 20.0, 30.0, 40.0]];
        let kinds = [vec![0u8; 4]];
        let xnan = [vec![2.0f64]];
        let sc = run_xnan(&xs, &plot, &kinds, &xnan, false, spec_of(100, false));
        let s = &sc.series[0];
        assert_eq!(s.nan_indices, vec![2]);
        assert_eq!(s.nan_kinds, vec![4], "kind 4 = unplottable x");
        assert!(
            s.values.iter().all(|v| v.is_finite()),
            "kind 4 annotates, does not evict"
        );
    }

    /// Global endpoint values follow the same rule as every other sample: they
    /// aggregate into their regular grid cells, including duplicate-x spreads.
    #[test]
    fn endpoint_duplicates_follow_regular_bucket_rules() {
        let a: Vec<f64> = (0..=10).map(|i| i as f64).collect();
        // B logged only at x=0 (twice); C only at x=10 (twice).
        let xs = vec![a.clone(), vec![0.0, 0.0], vec![10.0, 10.0]];
        let plot = vec![a.clone(), vec![5.0, 77.0], vec![100.0, 200.0]];
        let kinds = vec![vec![0u8; a.len()], vec![0u8; 2], vec![0u8; 2]];
        let sc = run(&xs, &plot, &kinds, false, 4);

        assert!(
            sc.x_values.windows(2).all(|w| w[0] < w[1]),
            "axis ascending & unique"
        );
        assert_eq!(*sc.x_values.first().unwrap(), 1.5);
        assert_eq!(*sc.x_values.last().unwrap(), 9.0);
        assert!(
            !sc.x_values.contains(&0.0) && !sc.x_values.contains(&10.0),
            "endpoints must not get special slots"
        );
        let last = sc.x_values.len() - 1;

        // B joins the first regular bucket and its duplicate values aggregate.
        let b = &sc.series[1];
        assert_eq!(b.values[0], 41.0, "B's bucket mean");
        assert!(
            b.values[1..].iter().all(|v| v.is_nan()),
            "B must not occupy another bucket"
        );
        assert_eq!(
            (b.min_values[0], b.max_values[0]),
            (5.0, 77.0),
            "B's spread is the first bucket's band"
        );

        // C joins the last regular bucket under the identical rule.
        let c = &sc.series[2];
        assert_eq!(c.values[last], 150.0, "C's bucket mean");
        assert!(
            c.values[..last].iter().all(|v| v.is_nan()),
            "C must not occupy another bucket"
        );
        assert_eq!(
            (c.min_values[last], c.max_values[last]),
            (100.0, 200.0),
            "C's spread is the last bucket's band"
        );
    }

    // === Decision (decide_step / decide_general) ===

    /// Two contiguous runs that overlap but neither spans the whole range: the
    /// interval-merge unions them to one segment, distinct ≤ target, so it
    /// passes through on the merged integer axis.
    #[test]
    fn step_contiguous_multirun_union_passthrough() {
        let a: Vec<f64> = (0..=6).map(|i| i as f64).collect();
        let b: Vec<f64> = (4..=10).map(|i| i as f64).collect();
        let v = |xs: &[f64]| xs.iter().map(|x| x * 10.0).collect::<Vec<_>>();
        let xs = vec![a.clone(), b.clone()];
        let plot = vec![v(&a), v(&b)];
        let kinds = vec![vec![0u8; a.len()], vec![0u8; b.len()]];
        let sc = run(&xs, &plot, &kinds, false, 100);
        // Union of [0,6] and [4,10] is the single contiguous segment [0,10].
        assert_eq!(sc.x_values, (0..=10).map(|i| i as f64).collect::<Vec<_>>());
        for s in &sc.series {
            assert!(s.min_values.is_empty(), "passthrough carries no envelope");
        }
        let (sa, sb) = (&sc.series[0], &sc.series[1]);
        assert!(
            sa.values[..=6].iter().all(|v| v.is_finite())
                && sa.values[7..].iter().all(|v| v.is_nan())
        );
        assert!(
            sb.values[..4].iter().all(|v| v.is_nan())
                && sb.values[4..=10].iter().all(|v| v.is_finite())
        );
    }

    /// Disjoint contiguous runs: the merge yields TWO segments, so the axis is
    /// exactly the logged steps with NO phantom slots spanning the gap (the
    /// failure mode of a blind "distinct == span" assumption).
    #[test]
    fn step_disjoint_runs_have_no_phantom_slots() {
        let a: Vec<f64> = (0..=3).map(|i| i as f64).collect();
        let b: Vec<f64> = (100..=103).map(|i| i as f64).collect();
        let xs = vec![a.clone(), b.clone()];
        let plot = vec![a.clone(), b.clone()];
        let kinds = vec![vec![0u8; 4], vec![0u8; 4]];
        let sc = run(&xs, &plot, &kinds, false, 100);
        assert_eq!(
            sc.x_values,
            vec![0.0, 1.0, 2.0, 3.0, 100.0, 101.0, 102.0, 103.0]
        );
        assert!(
            !sc.x_values.iter().any(|&x| (4.0..100.0).contains(&x)),
            "no slot in the gap"
        );
    }

    /// The dominant fast path: one long contiguous run with more points than
    /// the target short-circuits to downsample on `max_len` alone — no merge,
    /// no points scanned.
    #[test]
    fn step_long_run_downsamples_via_max_len() {
        let a: Vec<f64> = (0..=199).map(|i| i as f64).collect();
        let plot = a.iter().map(|x| (x * 0.1).sin()).collect::<Vec<_>>();
        let xs = vec![a.clone()];
        let kinds = vec![vec![0u8; a.len()]];
        let sc = run(&xs, &[plot], &kinds, false, 50);
        assert!(
            sc.x_values.len() <= 52,
            "downsampled to ~target slots, not 200"
        );
        assert!(sc.x_values.len() < a.len());
        assert!(
            sc.series.iter().any(|s| !s.min_values.is_empty()),
            "downsampling builds envelopes"
        );
    }

    /// The step decision under the expectation rule: a grid whose width holds
    /// ENV_MIN_POINTS downsamples; one whose width falls under it passes every
    /// distinct step through instead ("2 points beat 1 envelope bucket"), even
    /// though distinct exceeds the target.
    #[test]
    fn step_decision_branches() {
        let a: Vec<f64> = (0..=40).map(|i| i as f64).collect();
        let b: Vec<f64> = (40..=80).map(|i| i as f64).collect();
        let xs_s: Vec<&[f64]> = vec![&a, &b];
        // target 30: width 4 ≥ ENV_MIN_POINTS and distinct 81 > 30 → grid.
        assert!(matches!(
            decide_axis(&xs_s, 0.0, 80.0, spec_of(30, true)),
            Decision::Downsample
        ));
        // target 60: distinct 81 > 60 BUT width 2 < ENV_MIN_POINTS → every
        // bucket would ship raw; the chart passes through in full.
        match decide_axis(&xs_s, 0.0, 80.0, spec_of(60, true)) {
            Decision::Passthrough(axis) => {
                assert_eq!(axis, (0..=80).map(|i| i as f64).collect::<Vec<_>>());
            }
            Decision::Downsample => panic!("sub-envelope widths must pass through"),
        }
        // target 200 ≥ distinct: plain passthrough.
        assert!(matches!(
            decide_axis(&xs_s, 0.0, 80.0, spec_of(200, true)),
            Decision::Passthrough(_)
        ));
    }

    /// A gappy (strided) step run isn't one interval, so `decide_step` defers
    /// to the general path, which still decides correctly.
    #[test]
    fn step_gappy_run_falls_through_to_general() {
        let a: Vec<f64> = (0..200).map(|i| (i * 3) as f64).collect(); // stride 3
        let plot = a.iter().map(|x| x * 0.5).collect::<Vec<_>>();
        let xs = vec![a.clone()];
        let kinds = vec![vec![0u8; a.len()]];
        let sc = run(&xs, &[plot], &kinds, false, 50);
        // 200 strided points, 50 targets -> general path early-exits, downsamples.
        assert!(sc.x_values.len() <= 52 && sc.x_values.len() < a.len());
        assert!(sc.series.iter().any(|s| !s.min_values.is_empty()));
    }

    /// The general path's distinct axis equals the plain sort+dedup (a run on
    /// the timestamp/custom-x axis with few enough points to pass through).
    #[test]
    fn general_passthrough_axis_matches_sort_dedup() {
        // Two non-step runs sharing some x; distinct union small -> passthrough.
        let xs = vec![vec![0.5, 2.5, 9.0], vec![2.5, 4.0, 9.0]];
        let plot = vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]];
        let kinds = vec![vec![0u8; 3], vec![0u8; 3]];
        let sc = run_axis(&xs, &plot, &kinds, false, 100, false);
        assert_eq!(
            sc.x_values,
            vec![0.5, 2.5, 4.0, 9.0],
            "merged, deduped, ascending"
        );
    }

    /// Behavior lock: `decide_axis` implements exactly "distinct > target
    /// downsamples, except a linear step grid under ENV_MIN_POINTS width",
    /// and on passthrough the axis equals a plain sort+dedup — across step,
    /// gappy-step, custom-x, and degenerate inputs at every target tier.
    #[test]
    fn decide_axis_matches_reference_decision() {
        fn reference(
            xs: &[&[f64]],
            g_first: f64,
            g_last: f64,
            target: usize,
            step: bool,
        ) -> (bool, Vec<f64>) {
            let mut u: Vec<f64> = xs.iter().flat_map(|x| x.iter().copied()).collect();
            u.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            u.dedup();
            let mut downsample = target > 0 && g_last > g_first && u.len() > target;
            if downsample && step {
                let (_, width, _) = stable_grid(g_first, g_last, target);
                if width < ENV_MIN_POINTS {
                    downsample = false;
                }
            }
            (downsample, u)
        }
        let cases: Vec<(Vec<Vec<f64>>, bool)> = vec![
            (vec![(0..=100).map(|i| i as f64).collect()], true), // one long contiguous run
            (
                vec![
                    (0..=50).map(|i| i as f64).collect(),
                    (20..=120).map(|i| i as f64).collect(),
                ],
                true,
            ), // overlap
            (
                vec![
                    (0..=10).map(|i| i as f64).collect(),
                    (1000..=1010).map(|i| i as f64).collect(),
                ],
                true,
            ), // disjoint
            (vec![(0..200).map(|i| (i * 3) as f64).collect()], true), // gappy step -> general
            (vec![vec![0.5, 2.5, 9.0], vec![2.5, 4.0, 9.0]], false), // custom-x
            (vec![vec![5.0], vec![5.0]], true),                  // single x position
            (vec![vec![5.0, 5.0, 5.0]], false),                  // single x position, repeated
        ];
        for (runs, is_step) in cases {
            let xs_s: Vec<&[f64]> = runs.iter().map(|r| r.as_slice()).collect();
            let (g_first, g_last) = axis_bounds(&xs_s).unwrap();
            for &target in &[0usize, 8, 64, 1000] {
                let (want_down, want_axis) = reference(&xs_s, g_first, g_last, target, is_step);
                match decide_axis(&xs_s, g_first, g_last, spec_of(target, is_step)) {
                    Decision::Downsample => {
                        assert!(want_down, "downsampled but reference passed through (step={is_step}, target={target})");
                    }
                    Decision::Passthrough(axis) => {
                        assert!(!want_down, "passed through but reference downsampled (step={is_step}, target={target})");
                        assert_eq!(
                            axis, want_axis,
                            "axis must equal sort+dedup (step={is_step}, target={target})"
                        );
                    }
                }
            }
        }
    }

    // === Log ladder ===

    /// pow2 must be bit-identical to powi over every exponent the grid can
    /// produce (k down to OCTAVE_MIN - m, up to the top octave).
    #[test]
    fn pow2_matches_powi_over_the_grid_range() {
        for k in -560..=1023 {
            assert_eq!(pow2(k).to_bits(), 2f64.powi(k).to_bits(), "k={k}");
        }
    }

    /// The doc's example, literally: with 4 buckets per octave (m = 2) the
    /// boundaries around 16..40 are [16,20),[20,24),[24,28),[28,32),[32,40) —
    /// consecutive cell ids, power-of-two widths aligned on powers of two.
    #[test]
    fn log_ladder_matches_doc_example() {
        let kind = GridKind::Log { m: 2, shift: false };
        let id = |x: f64| Grid::id(kind, x);
        assert_eq!(id(16.0), id(19.9));
        assert_eq!(id(20.0), id(16.0) + 1);
        assert_eq!(id(24.0), id(16.0) + 2);
        assert_eq!(id(28.0), id(16.0) + 3);
        assert_eq!(
            id(32.0),
            id(16.0) + 4,
            "octave boundary: ids stay consecutive"
        );
        assert_eq!(id(32.0), id(39.9), "[32,40) is one bucket of width 8");
        assert_eq!(id(40.0), id(32.0) + 1);
        // Widths double per octave.
        let g = Grid {
            kind,
            base: id(16.0),
            cells: 8,
        };
        assert_eq!(g.width(0), 4.0);
        assert_eq!(g.width(4), 8.0);
    }

    /// An x below [`OCTAVE_MIN`]'s octave joins that octave's bottom cell.
    #[test]
    fn log_ladder_merges_the_octaves_below_its_floor() {
        let kind = GridKind::Log { m: 2, shift: false };
        let bottom = Grid::id(kind, pow2(OCTAVE_MIN));
        assert_eq!(Grid::id(kind, pow2(OCTAVE_MIN - 100)), bottom);
        assert_eq!(Grid::id(kind, f64::MIN_POSITIVE / 2.0), bottom);
    }

    /// The ladder takes the finest subdivision whose cells over the span fit the target: 16..=256 spans octaves 4 to 8, 5 cells at m = 0 and 9 at m = 1.
    #[test]
    fn the_ladder_takes_the_finest_tier_within_target() {
        let grid = |target| {
            let g = make_grid_from(16.0, 256.0, log_custom_spec(target));
            (g.kind, g.cells)
        };
        assert_eq!(grid(8), (GridKind::Log { m: 0, shift: false }, 5));
        assert_eq!(grid(9), (GridKind::Log { m: 1, shift: false }, 9));
    }

    /// A ladder from an x at or below its shifted zero fails the build rather than anchor at that x's magnitude: here 0 on a custom-x ladder, which never shifts.
    #[test]
    #[should_panic(expected = "at or below its shifted zero")]
    fn a_ladder_from_its_shifted_zero_fails() {
        make_grid_from(0.0, 10.0, log_custom_spec(8));
    }

    /// A log step chart is a MIXTURE: exact raw slots where buckets would
    /// expect under ENV_MIN_POINTS points (small steps), envelope buckets
    /// above — and with the +1 shift, step 0 is on the chart. Every logged
    /// step stays represented: raw slots and bucket x extents tile the range.
    #[test]
    fn log_step_chart_mixes_raw_and_buckets() {
        let a: Vec<f64> = (0..=1000).map(|i| i as f64).collect();
        let av: Vec<f64> = a.iter().map(|x| x * 10.0).collect();
        let xs = vec![a.clone()];
        let kinds = vec![vec![0u8; a.len()]];
        let sc = run_spec(&xs, &[av], &kinds, false, log_step_spec(100));

        // Slot budget respected; axis ascending and unique.
        assert!(sc.x_values.len() <= 102, "{} slots", sc.x_values.len());
        assert!(sc.x_values.windows(2).all(|w| w[0] < w[1]));
        // m = 3 (8 buckets/octave over ~10 octaves = 80 ≤ 100): the raw region
        // is shifted x < 4·8 = 32, i.e. steps 0..=30, all exact raw slots.
        assert_eq!(sc.x_values[0], 0.0, "step 0 renders (log(x+1) shift)");
        assert_eq!(
            &sc.x_values[1..=30],
            (1..=30).map(|i| i as f64).collect::<Vec<_>>().as_slice()
        );
        // First envelope bucket: shifted [32,36) = steps 31..=34, center 32.5.
        assert_eq!(sc.x_values[31], 32.5);
        assert_eq!((sc.xr_min[31], sc.xr_max[31]), (31.0, 34.0));
        let s = &sc.series[0];
        assert_eq!((s.min_values[31], s.max_values[31]), (310.0, 340.0));
        assert_eq!(s.values[31], 325.0, "bucket mean");
        // Raw slots carry the exact sample, and the dense-envelope contract
        // pinches the band onto it.
        assert_eq!(s.values[7], 70.0);
        assert_eq!((s.min_values[7], s.max_values[7]), (70.0, 70.0));
        // No step goes unrepresented: raw slots and bucket extents together
        // tile 0..=1000.
        let mut covered = vec![false; 1001];
        for i in 0..sc.x_values.len() {
            if sc.xr_min[i].is_nan() {
                covered[sc.x_values[i] as usize] = true;
            } else {
                for covered in covered
                    .iter_mut()
                    .take(sc.xr_max[i] as usize + 1)
                    .skip(sc.xr_min[i] as usize)
                {
                    *covered = true;
                }
            }
        }
        assert!(covered.iter().all(|&c| c), "every logged step represented");
    }

    /// Without a zero in the chart the ladder stays plain log(x): the same
    /// tail of steps gives different bucket boundaries than the
    /// zero-containing chart, whose ladder runs in log(x+1).
    #[test]
    fn log_without_zero_keeps_plain_log() {
        let mk = |lo: i64| {
            let a: Vec<f64> = (lo..=1000).map(|i| i as f64).collect();
            let av: Vec<f64> = a.iter().map(|x| x * 10.0).collect();
            let kinds = vec![vec![0u8; a.len()]];
            run_spec(&[a], &[av], &kinds, false, log_step_spec(100))
        };
        let zero = mk(0);
        let one = mk(1);
        // Zero present: ladder in x+1, first envelope bucket = steps 31..=34 (shifted [32,36)), center 32.5.
        assert_eq!(zero.x_values[0], 0.0);
        assert_eq!(zero.x_values[31], 32.5);
        // No zero: plain log(x), first envelope bucket = steps 32..=35, center 33.5.
        assert_eq!(one.x_values[0], 1.0);
        assert_eq!(one.x_values[31], 33.5);
        assert_eq!((one.xr_min[31], one.xr_max[31]), (32.0, 35.0));
    }

    /// Deep sub-octave zooms scale the subdivision up until the ladder is as
    /// fine as a linear grid would be — a log axis zoomed into a narrow range
    /// must not collapse to a handful of buckets.
    #[test]
    fn log_zoom_scales_subdivision_up() {
        let a: Vec<f64> = (100_000..=101_000).map(|i| i as f64).collect();
        let av: Vec<f64> = a.iter().map(|x| x * 0.5).collect();
        let xs = vec![a.clone()];
        let kinds = vec![vec![0u8; a.len()]];
        let sc = run_spec(&xs, &[av], &kinds, false, log_step_spec(250));
        assert!(
            sc.x_values.len() > 120,
            "only {} slots for a 1000-step window",
            sc.x_values.len()
        );
        assert!(sc.x_values.len() <= 260);
    }

    /// Non-step axes decide each bucket from its own distinct count: sparse
    /// buckets (< 4 distinct x) ship raw points, dense ones an envelope —
    /// the mixture on one axis.
    #[test]
    fn nonstep_sparse_buckets_stay_raw() {
        // target 4 → linear width 4096 over [0, 12000]: a dense cluster in
        // bucket [4096,8192), an isolated pair in bucket [8192,12288).
        let xs = vec![vec![
            0.0, // sparse first cell: raw
            4200.0, 4201.0, 4202.0, 4203.0, 4204.0, // dense: envelope
            9000.0, 9001.0,  // sparse: raw
            12000.0, // same sparse cell as 9000/9001: raw
        ]];
        let ys: Vec<f64> = xs[0].iter().map(|x| x * 2.0).collect();
        let kinds = vec![vec![0u8; xs[0].len()]];
        let sc = run_axis(&xs, &[ys], &kinds, false, 4, false);
        // Axis: 0, envelope center (4202), 9000, 9001, 12000.
        assert_eq!(sc.x_values, vec![0.0, 4202.0, 9000.0, 9001.0, 12000.0]);
        let s = &sc.series[0];
        assert_eq!((sc.xr_min[1], sc.xr_max[1]), (4200.0, 4204.0));
        assert_eq!((s.min_values[1], s.max_values[1]), (8400.0, 8408.0));
        assert_eq!(s.values[2], 18000.0, "raw slot keeps the exact sample");
        assert_eq!(
            (s.min_values[2], s.max_values[2]),
            (18000.0, 18000.0),
            "band pinches to the line at raw slots"
        );
        assert!(sc.xr_min[2].is_nan(), "raw slots carry no x extent");
    }
}

#[cfg(test)]
mod slot_smoother_tests {
    use super::*;

    /// The weighted least-squares polynomial of degree `deg` through `(x, y, weight)`, solved by elimination on its normal equations in raw offsets from `at` and read there: a reference independent of [`fit_from_moments_at`].
    #[allow(clippy::needless_range_loop)] // Matrix rows and columns read clearer by index.
    fn wls_at(points: &[(f64, f64, f64)], deg: usize, at: f64) -> f64 {
        let k = deg + 1;
        let mut a = [[0.0f64; 4]; 3];
        for &(x, y, w) in points {
            let u = x - at;
            for r in 0..k {
                for c in 0..k {
                    a[r][c] += w * u.powi((r + c) as i32);
                }
                a[r][k] += w * y * u.powi(r as i32);
            }
        }
        for col in 0..k {
            let pivot = (col..k)
                .max_by(|&i, &j| a[i][col].abs().total_cmp(&a[j][col].abs()))
                .unwrap();
            a.swap(col, pivot);
            for r in 0..k {
                if r != col {
                    let f = a[r][col] / a[col][col];
                    for c in col..=k {
                        a[r][c] -= f * a[col][c];
                    }
                }
            }
        }
        a[0][k] / a[0][0]
    }

    fn close(got: f64, want: f64, what: &str) {
        assert!(
            (got - want).abs() <= 1e-9 * want.abs().max(1.0),
            "{what}: {got} vs {want}"
        );
    }

    /// An EMA whose time constant grows with x (a log ladder's floor, past the knee where it overtakes the user's) weights each sample by exp(−∫dt/T) from it to the slot: the slot value is that weighted fit, here against weights integrated numerically. A constant time constant is the plain EMA.
    #[test]
    fn an_ema_with_a_growing_time_constant_weights_by_its_integral() {
        let xs: Vec<f64> = (0..300).map(|i| (i * 7 + i % 5) as f64 + 40.0).collect();
        let ys: Vec<f64> = xs
            .iter()
            .map(|&x| (x * 0.05).sin() * 3.0 + x * 0.01)
            .collect();
        let dx_ref = 2.0;
        for (base, growth, shift) in [(5.0, 0.0, 0.0), (5.0, 0.04, 1.0), (40.0, 0.03, 0.0)] {
            let tau = Width {
                base,
                growth,
                shift,
            };
            // ∫ dt / (dx_ref·τ(t)) by Simpson's rule, independent of `Width::folds`.
            let folds = |a: f64, b: f64| {
                let steps = 4_000;
                let h = (b - a) / steps as f64;
                let f = |t: f64| 1.0 / (dx_ref * tau.at(t));
                let inner: f64 = (1..steps)
                    .map(|i| f(a + i as f64 * h) * if i % 2 == 1 { 4.0 } else { 2.0 })
                    .sum();
                (f(a) + inner + f(b)) * h / 3.0
            };
            for deg in 0..=2 {
                let smoother = SlotSmoother::Causal(CausalSmoother::Ema { tau, dx_ref, deg });
                let mut evaluator = SlotEvaluator::over(smoother, &xs, &ys);
                for xq in [100.5, 900.0, 1_500.25, 2_200.0] {
                    let points: Vec<(f64, f64, f64)> = xs
                        .iter()
                        .zip(&ys)
                        .filter(|(&x, _)| x <= xq)
                        .map(|(&x, &y)| (x, y, (-folds(x, xq)).exp()))
                        .collect();
                    let want = wls_at(&points, deg, xq);
                    let got = evaluator.at(xq);
                    assert!(
                        (got - want).abs() <= 1e-6 * want.abs().max(1.0),
                        "τ {base}+{growth}(x+{shift}), order {deg} at {xq}: {got} vs {want}"
                    );
                }
            }
        }
    }

    /// The log ladder's floor width, (x + shift)/s, covers the ladder's bucket at every x without stepping at an octave: it equals the bucket's width at the octave's start and stays under twice it.
    #[test]
    fn the_log_floor_covers_each_ladder_bucket() {
        for (m, shift) in [(0, false), (3, true), (6, false)] {
            let kind = GridKind::Log { m, shift };
            let width = Floor::of(kind);
            let mut x = 1.0f64;
            while x < 1e6 {
                let id = Grid::id(kind, x);
                let bucket = pow2((id.div_euclid(1i64 << m)) as i32 - m);
                let w = width.at(x);
                assert!(
                    w >= bucket && w < 2.0 * bucket,
                    "m={m} x={x}: {w} for {bucket}"
                );
                x *= 1.013;
            }
            let start = if shift { 63.0 } else { 64.0 };
            assert_eq!(width.at(start), pow2(6 - m), "m={m}: an octave's start");
        }
    }

    /// A zoom range that can hold no emitted x, inverted or on a log axis below 0, bounds no floor (`zoom_floor`).
    #[test]
    fn a_zoom_holding_no_emitted_x_has_no_floor() {
        let spec = |log_buckets| GridSpec {
            target: 100,
            is_step_axis: true,
            log_buckets,
            shift_one: log_buckets,
        };
        for (lo, hi, log) in [
            (-100.0, -50.0, true),
            (500.0, 100.0, true),
            (500.0, 100.0, false),
        ] {
            let floor = zoom_floor(lo, hi, spec(log));
            assert_eq!(floor.at(1e6), 0.0, "{lo}..={hi} log={log}");
        }
    }

    /// `floored` widens each smoother by its kernel's R: a Savitzky-Golay half-width to R·w and an EMA's time constant (in its dx_ref units) to R·w/dx_ref, on a linear grid of width w and along the log ladder's (x + 1)/s; Triangular keeps its weights.
    #[test]
    fn floors_widen_each_smoother_by_its_kernels_r() {
        let ladder = Floor::of(GridKind::Log { m: 2, shift: true });
        for floor in [
            Floor::of(GridKind::Linear {
                x_lo: 0.0,
                width: 64.0,
            }),
            ladder,
        ] {
            let w = |x: f64| floor.at(x);
            for (deg, r) in [(0, 5.0 / 7.0), (1, 5.0 / 7.0), (2, 805.0 / 572.0)] {
                let sg = SlotSmoother::SavitzkyGolay {
                    h_x: Width::fixed(0.5),
                    deg,
                };
                let SlotSmoother::SavitzkyGolay { h_x, .. } = sg.floored(floor) else {
                    unreachable!()
                };
                assert_eq!(h_x.at(1023.0), r * w(1023.0), "Savitzky-Golay order {deg}");
            }
            for (deg, r) in [(0, 0.5), (1, 1.25), (2, 33.0 / 16.0)] {
                let ema = SlotSmoother::Causal(CausalSmoother::Ema {
                    tau: Width::fixed(0.5),
                    dx_ref: 4.0,
                    deg,
                });
                let SlotSmoother::Causal(CausalSmoother::Ema { tau, .. }) = ema.floored(floor)
                else {
                    unreachable!()
                };
                assert_eq!(tau.at(1023.0), r * w(1023.0) / 4.0, "EMA order {deg}");
            }
        }
        let triangular = SlotSmoother::Causal(CausalSmoother::Triangular {
            dx_ref: 4.0,
            deg: 1,
        });
        assert!(matches!(
            triangular.floored(ladder),
            SlotSmoother::Causal(CausalSmoother::Triangular {
                dx_ref: 4.0,
                deg: 1
            })
        ));
    }

    /// The window centered at any x, between samples, across a logging gap, and past either end, is the biweight fit over the samples within the half-width; with none, there is no value.
    #[test]
    fn savgol_at_fits_the_window_centered_at_any_x() {
        let xs: [f64; 10] = [0.0, 1.0, 2.5, 3.0, 4.0, 7.0, 7.5, 8.0, 9.0, 11.0];
        let ys: Vec<f64> = xs
            .iter()
            .enumerate()
            .map(|(i, &x)| {
                if i == 6 {
                    f64::NAN
                } else {
                    (0.7 * x).sin() * 3.0 + x
                }
            })
            .collect();
        let h_x = 3.0;
        for deg in 0..=2 {
            for q in [-2.5, 0.3, 2.5, 5.5, 8.75, 11.0, 12.9] {
                let points: Vec<(f64, f64, f64)> = xs
                    .iter()
                    .zip(&ys)
                    .filter(|&(&x, y)| (x - q).abs() < h_x && !y.is_nan())
                    .map(|(&x, &y)| {
                        let u = (x - q) / h_x;
                        (x, y, (1.0 - u * u).powi(2))
                    })
                    .collect();
                // Fewer distinct points than coefficients step the degree down.
                let deg = deg.min(points.len() - 1);
                let got = savgol_at(
                    &|j| xs[j],
                    &|j| ys[j],
                    xs.len(),
                    q,
                    Width::fixed(h_x),
                    deg,
                    &mut SavgolCursor::default(),
                );
                close(got, wls_at(&points, deg, q), &format!("deg {deg} at {q}"));
            }
        }
        assert!(savgol_at(
            &|j| xs[j],
            &|j| ys[j],
            xs.len(),
            14.5,
            Width::fixed(h_x),
            1,
            &mut SavgolCursor::default()
        )
        .is_nan());
    }

    /// On a uniform grid, the slot fit at a sample's x is the index-space output there.
    #[test]
    fn savgol_at_matches_the_uniform_sweep_at_samples() {
        let xs: Vec<f64> = (0..120).map(|i| 5.0 * i as f64).collect();
        let ys: Vec<f64> = (0..120)
            .map(|i| ((i * 37) % 101) as f64 / 7.0 - (i as f64 * 0.05).cos())
            .collect();
        for (window, deg) in [(5u32, 0u32), (3, 1), (20, 1), (21, 2), (64, 2)] {
            let sweep = smooth_run(&xs, &ys, Algorithm::SavitzkyGolay, window, 1.0, deg, true);
            let SlotSmoother::SavitzkyGolay { h_x, deg } = SlotSmoother::new(
                Algorithm::SavitzkyGolay,
                window,
                1.0,
                deg,
                smoothing_plan(&xs, Algorithm::SavitzkyGolay, true).0,
            ) else {
                unreachable!()
            };
            for i in 0..xs.len() {
                let got = savgol_at(
                    &|j| xs[j],
                    &|j| ys[j],
                    xs.len(),
                    xs[i],
                    h_x,
                    deg,
                    &mut SavgolCursor::default(),
                );
                close(got, sweep[i], &format!("window {window} sample {i}"));
            }
        }
    }

    /// An evenly spaced window symmetric about a slot between two samples, as a linear grid's envelope slot is, fits by the cursor's weights: six samples about 10.5 at half-width 3 give the biweight fit as a dot product. Four samples about it (half-width 2) fit directly: their edges might weigh nothing, leaving a quadratic two x.
    #[test]
    fn an_even_window_between_samples_fits_as_a_dot_product() {
        let xs: Vec<f64> = (0..21).map(f64::from).collect();
        let ys: Vec<f64> = (0..21).map(|i| ((i * 37) % 23) as f64).collect();
        for (h, dot) in [(3.0, true), (2.0, false)] {
            let mut cursor = SavgolCursor::default();
            let (x, y) = (|j: usize| xs[j], |j: usize| ys[j]);
            let got = savgol_at(&x, &y, 21, 10.5, Width::fixed(h), 2, &mut cursor);
            assert_eq!(cursor.key.is_some(), dot, "half-width {h}");
            let window: Vec<_> = (0..21)
                .filter(|&j| (xs[j] - 10.5_f64).abs() < h)
                .map(|j| (xs[j], ys[j], (1.0 - ((xs[j] - 10.5) / h).powi(2)).powi(2)))
                .collect();
            close(got, wls_at(&window, 2, 10.5), "the biweight fit");
        }
    }

    /// A window whose ends sit evenly about its slot, with its middle sample there, but whose samples are not evenly spaced fits directly, not by the even window's weights: steps divisible by 2 or 3 put 27, 28, 30, 32 and 33 in window 6's half-width 3 about 30.
    #[test]
    fn a_centered_uneven_window_fits_directly() {
        let xs: Vec<f64> = (0..60)
            .filter(|s| s % 2 == 0 || s % 3 == 0)
            .map(f64::from)
            .collect();
        let ys: Vec<f64> = (0..xs.len())
            .map(|i| ((i * 37) % 23) as f64 - 0.3 * i as f64)
            .collect();
        let plan = smoothing_plan(&xs, Algorithm::SavitzkyGolay, true).0;
        for deg in 0..=1usize {
            let SlotSmoother::SavitzkyGolay { h_x, .. } =
                SlotSmoother::new(Algorithm::SavitzkyGolay, 6, 1.0, deg as u32, plan)
            else {
                unreachable!()
            };
            let (xq, h) = (30.0, h_x.at(30.0));
            let mut cursor = SavgolCursor::default();
            let got = savgol_at(&|j| xs[j], &|j| ys[j], xs.len(), xq, h_x, deg, &mut cursor);
            let window: Vec<usize> = (0..xs.len()).filter(|&j| (xs[j] - xq).abs() <= h).collect();
            let window_x: Vec<f64> = window.iter().map(|&j| xs[j]).collect();
            assert_eq!(window_x, [27.0, 28.0, 30.0, 32.0, 33.0]);
            let points = window
                .iter()
                .map(|&j| ((xs[j] - xq) / h, (xs[j] - xq) / h, ys[j]));
            // Relative to the sample at the slot, the nearest.
            let want = biweight_fit_at(points, deg, 0.0, ys[window[2]]);
            assert_eq!(got.to_bits(), want.to_bits(), "deg {deg}");
        }
    }

    /// An order-2 EMA over two distinct x reads the line through them ahead of the last: two x fit a line, so its fit's degree stays below its count of distinct x. Without the count, the degree cut drops the order-2 pivot of rounding noise over one sample at each (with the cut at 0 too, the fit is off by up to 12.9 times the line) but not over many (a clock logging many samples per tick): 255 samples at one millisecond and five at the next draw a quadratic 11% off the line, alone or after a past (75 or 209 time constants back) whose spread the moments no longer resolve, which the count leaves out. A weight decayed below the normal range is dropped: kept, three samples 741.9 time constants before a fourth leave subnormal moments whose noise passes the cut, and the fit draws 299.3 for values of 2.3.
    #[test]
    fn an_order_2_ema_over_two_x_reads_their_line() {
        let (xs, ys) = ([10.0, 11.0], [3.0, 7.0]);
        for i in 0..2000 {
            let tau = 0.5 + i as f64 * 0.37;
            let smoother = CausalSmoother::Ema {
                tau: Width::fixed(tau),
                dx_ref: 1.0,
                deg: 2,
            };
            let mut st = CausalState::START;
            smoother.fold_range(&mut st, |i| xs[i], |i| ys[i], 0, 2);
            for ahead in [0.25, 0.5, 2.0] {
                let line = 7.0 + 4.0 * ahead;
                let got = smoother.value_at(&st, 11.0 + ahead);
                assert!(
                    (got - line).abs() <= 1e-12 * line,
                    "tau {tau} ahead {ahead}: {got}"
                );
            }
        }
        let y = |i: usize| (3.0 + (i % 255 * 73 % 101) as f64 * 0.31) as f32 as f64;
        let at = |deg, history: &[f64]| {
            let smoother = CausalSmoother::Ema {
                tau: Width::fixed(239.0),
                dx_ref: 1.0,
                deg,
            };
            let mut st = CausalState::START;
            let m = history.len();
            smoother.fold_range(&mut st, |i| history[i], |_| 3.0, 0, m);
            let x = |i: usize| if i < 255 { 100_008.0 } else { 100_009.0 };
            smoother.fold_range(&mut st, x, y, 0, 260);
            smoother.value_at(&st, 100_009.5)
        };
        // Alone, and after histories 209 and 75 time constants back, whose spread the moments no longer resolve.
        let history: Vec<f64> = (0..8).map(f64::from).chain([50_000.0]).collect();
        for history in [&[][..], &history, &[100_008.0 - 75.0 * 239.0]] {
            assert_eq!(at(2, history).to_bits(), at(1, history).to_bits());
        }
        let smoother = CausalSmoother::Ema {
            tau: Width::fixed(100.0),
            dx_ref: 1.0,
            deg: 2,
        };
        let mut st = CausalState::START;
        let (xs, ys) = ([0.0, 1.0, 2.0, 74_192.0], [2.31, 2.35, 2.28, 2.29]);
        smoother.fold_range(&mut st, |i| xs[i], |i| ys[i], 0, 4);
        assert!((smoother.value_at(&st, 74_216.0) - 2.29).abs() < 1e-3);
        let mut fresh = CausalState::START;
        smoother.fold_range(&mut fresh, |i| xs[i], |i| ys[i], 3, 4);
        assert_eq!(st.bits(), fresh.bits(), "a dropped past");
        // A past whose u² mass is 34.6 times the cut's keeps the count at 3: the quadratic through the three x, whatever their weights.
        let (xs, ys) = ([0.0, 3_700.0, 4_090.0], [0.0, 0.0, 1.0]);
        let mut st = CausalState::START;
        smoother.fold_range(&mut st, |i| xs[i], |i| ys[i], 0, 3);
        let quadratic = 4_100.0 * 400.0 / (4_090.0 * 390.0);
        assert!((smoother.value_at(&st, 4_100.0) - quadratic).abs() < 1e-4);
    }

    /// An order-2 EMA's slot keeps its digits when the values sit far above their range: the fold takes them relative to the newest, so their level cancels before the fit. Folded at their level, a loss of 2.300 ± 0.002 logged at steps 0 to 49, then at 3,800 and 6,625, read 48 steps on, draws a fifth of its range off the same fit on values less 2.3.
    #[test]
    fn an_ema_slot_keeps_its_digits_over_a_level() {
        let xs: Vec<f64> = (0..50).map(f64::from).chain([3_800.0, 6_625.0]).collect();
        let y = |i: usize| (2.3 + 0.002 * ((i * 37 % 101) as f64 / 50.0 - 1.0)) as f32 as f64;
        let smoother = CausalSmoother::Ema {
            tau: Width::fixed(100.0),
            dx_ref: 1.0,
            deg: 2,
        };
        let read = |level: f64| {
            let mut st = CausalState::START;
            smoother.fold_range(&mut st, |i| xs[i], |i| y(i) - level, 0, xs.len());
            smoother.value_at(&st, 6_673.0) + level
        };
        let (got, want) = (read(0.0), read(2.3));
        assert!((got - want).abs() < 1e-3 * 0.004, "{got} against {want}");
    }

    /// A sample that weighs next to nothing cannot swamp the rest: 1e16 just inside a half-width of 1e9 (a weight of 4e-18), beside five samples of 1 at the slot, fits their weighted mean, exactly 1.0080000002100513 (its weight's own rounding leaves about 1e-9). Taken relative to the first sample that weighs, the light one, the five's digits cancelled and the fit read 0.
    #[test]
    fn a_light_sample_leaves_the_fit_its_digits() {
        let mut xs = vec![11_000_001.0];
        xs.extend((0..5).map(|i| 1_011_000_000.0 + f64::from(i)));
        let ys = [1e16f32 as f64, 1.0, 1.0, 1.0, 1.0, 1.0];
        let (x, y) = (|j: usize| xs[j], |j: usize| ys[j]);
        let width = Width::fixed(1e9);
        let got = savgol_at(&x, &y, 6, xs[1], width, 0, &mut SavgolCursor::default());
        assert!((got - 1.0080000002100513).abs() < 1e-8, "{got}");
    }

    /// The base is the finite sample nearest the slot, holes passed over on either side: at 1,000, a window of half-width 1,000 reaching back to 1e16 at 1, with the other values chosen so the weighted mean is about 1, fits that mean to within the rounding of the far sample's weight (terms of 1e10 cancel to 1 here: under 1e-2), whether the nearest finite samples sit at the slot past holes beside it, only left of it or only right of it past holes; the far 1e16 taken as the base reads 2.
    #[test]
    fn the_base_is_found_past_holes() {
        let big = f32::from_bits(0x5a0e_1c95) as f64;
        let w = |x: f64| (1.0 - ((x - 1000.0) / 1000.0).powi(2)).powi(2);
        let layouts = [
            (vec![1.0, 998.0, 999.0, 1000.0, 1000.0, 1001.0], "fvhhvv"),
            (vec![1.0, 998.0, 999.0, 1000.0, 1000.0, 1001.0], "fvvhhh"),
            (vec![1.0, 999.0, 1000.0, 1000.0, 1001.0], "fhhvv"),
        ];
        for (xs, kinds) in layouts {
            let weighs = xs.iter().zip(kinds.chars()).filter(|&(_, k)| k != 'h');
            let (w0, s) = (w(1.0), weighs.map(|(&x, _)| w(x)).sum::<f64>());
            let v = ((1.0 - w0 * big / s) / (1.0 - w0 / s)).round();
            let ys: Vec<f64> = kinds
                .chars()
                .map(|k| match k {
                    'f' => big,
                    'v' => v,
                    _ => f64::NAN,
                })
                .collect();
            let (x, y, n) = (|j: usize| xs[j], |j: usize| ys[j], xs.len());
            let width = Width::fixed(1000.0);
            let got = savgol_at(&x, &y, n, 1000.0, width, 0, &mut SavgolCursor::default());
            let want = v + w0 * (big - v) / s;
            assert!((got - want).abs() < 1e-2, "{kinds}: {got} against {want}");
        }
    }

    /// A Savitzky-Golay window's edge sample weighs nothing and is left out of its fit, first or last: beside one sample that weighs (5, 300 before the slot), an edge sample of 100 at either edge of a half-width of 729 leaves the fit 5 at every order. Through the reciprocal of 729 the edge would weigh 5e-32, count as a distinct x and draw the line through it.
    #[test]
    fn a_savgol_windows_edge_sample_weighs_nothing() {
        let xq = 948_184.0;
        let sides = [
            ([xq - 729.0, xq - 300.0], [100.0, 5.0]),
            ([xq - 300.0, xq + 729.0], [5.0, 100.0]),
        ];
        for (xs, ys) in sides {
            for deg in 0..=2 {
                let (x, y) = (|i: usize| xs[i], |i: usize| ys[i]);
                let width = Width::fixed(729.0);
                let got = savgol_at(&x, &y, 2, xq, width, deg, &mut SavgolCursor::default());
                assert_eq!(got, 5.0, "{xs:?} at order {deg}");
            }
        }
    }

    /// A series' cursor, carried across ascending slots as a build carries it, gives every slot the bits a fresh one does, and each is the biweight fit: over evenly spaced windows of two spacings and (past x = 200, from slot to slot) two half-widths, whose dot product's weights it caches, a NaN value's, a symmetric uneven one's (around x = 82, whose samples 76 and 88 sit between 75, 80 and 85, 90), and a gap's edges, past which a run of the same spacing resumes (its first centered window starts where the last one before the gap ended).
    #[test]
    fn a_carried_savgol_cursor_keeps_every_fits_bits() {
        let mut xs: Vec<f64> = (0..80)
            .map(|i| 2.5 * i as f64)
            .chain([76.0, 88.0])
            .chain((0..30).map(|i| 250.0 + 3.0 * i as f64))
            .chain((0..30).map(|i| 400.0 + 3.0 * i as f64))
            .collect();
        xs.sort_by(f64::total_cmp);
        let ys: Vec<f64> = (0..xs.len())
            .map(|i| {
                if i == 12 {
                    f64::NAN
                } else {
                    ((i * 37) % 23) as f64 - 0.3 * i as f64
                }
            })
            .collect();
        let h_x = |q: f64| {
            if q > 200.0 && q as i64 % 2 == 1 {
                7.0
            } else {
                6.0
            }
        };
        for deg in 0..=2 {
            let mut carried = SavgolCursor::default();
            let mut slots: Vec<f64> = xs.iter().flat_map(|&x| [x, x + 1.25]).collect();
            slots.push(82.0);
            slots.sort_by(f64::total_cmp);
            for q in slots {
                let h_x = h_x(q);
                let fit = |cursor: &mut SavgolCursor| {
                    savgol_at(
                        &|j| xs[j],
                        &|j| ys[j],
                        xs.len(),
                        q,
                        Width::fixed(h_x),
                        deg,
                        cursor,
                    )
                };
                let got = fit(&mut carried);
                assert_eq!(
                    got.to_bits(),
                    fit(&mut SavgolCursor::default()).to_bits(),
                    "{q}"
                );
                let points: Vec<(f64, f64, f64)> = xs
                    .iter()
                    .zip(&ys)
                    .filter(|&(&x, y)| (x - q).abs() < h_x && !y.is_nan())
                    .map(|(&x, &y)| (x, y, (1.0 - ((x - q) / h_x).powi(2)).powi(2)))
                    .collect();
                if points.is_empty() {
                    assert!(got.is_nan(), "{q}");
                    continue;
                }
                let deg = deg.min(points.len() - 1);
                close(got, wls_at(&points, deg, q), &format!("deg {deg} at {q}"));
            }
        }
        // A cursor skipping slots past a spacing break gives the same bits (an advance's starts at its first dirty slot and skips the clean ones): about 30, then about 130, past 76 and 88.
        let mut sparse = SavgolCursor::default();
        for q in [30.0, 130.0] {
            let fit = |cursor: &mut SavgolCursor| {
                savgol_at(
                    &|j| xs[j],
                    &|j| ys[j],
                    xs.len(),
                    q,
                    Width::fixed(20.0),
                    2,
                    cursor,
                )
            };
            let got = fit(&mut sparse);
            assert_eq!(
                got.to_bits(),
                fit(&mut SavgolCursor::default()).to_bits(),
                "{q}"
            );
        }
        // A window reaching before the run a cursor knows evenly spaced is checked again: about 10, samples 6-14 two apart, then about 4, whose 0, 2, 3, 6 and 8 are symmetric with the same mean spacing but 3 is off its grid.
        let xs = [0.0, 2.0, 3.0, 6.0, 8.0, 10.0, 12.0, 14.0];
        let ys = [1.0, 4.0, -2.0, 7.0, 3.0, 5.0, 0.0, 2.0];
        let mut carried = SavgolCursor::default();
        for q in [10.0, 4.0] {
            let fit = |cursor: &mut SavgolCursor| {
                savgol_at(
                    &|j| xs[j],
                    &|j| ys[j],
                    xs.len(),
                    q,
                    Width::fixed(4.0),
                    1,
                    cursor,
                )
            };
            let got = fit(&mut carried);
            assert_eq!(
                got.to_bits(),
                fit(&mut SavgolCursor::default()).to_bits(),
                "{q}"
            );
        }
    }

    /// A causal smoother read at x: the state's own fit at its last sample, the same fit carried to any later x without a sample (order 0 holds the value bit for bit; orders 1 and 2 continue the fitted polynomial), and nothing before the first sample.
    #[test]
    fn causal_value_at_continues_the_last_fit() {
        let xs: Vec<f64> = (0..40).map(|i| (3 * i + i % 4) as f64).collect();
        let ys: Vec<f64> = xs
            .iter()
            .enumerate()
            .map(|(i, &x)| {
                if i % 13 == 5 {
                    f64::NAN
                } else {
                    (0.1 * x).cos() * 5.0 + 0.2 * x
                }
            })
            .collect();
        for algo in [Algorithm::Ema, Algorithm::Triangular] {
            for deg in 0..=2u32 {
                let SlotSmoother::Causal(smoother) =
                    SlotSmoother::new(algo, 0, 4.0, deg, SmoothingPlan::NoState)
                else {
                    unreachable!("a causal algorithm")
                };
                let mut st = CausalState::START;
                assert!(smoother.value_at(&st, 0.0).is_nan(), "no sample yet");
                let mut points: Vec<(f64, f64, f64)> = Vec::new();
                for (i, (&x, &y)) in xs.iter().zip(&ys).enumerate() {
                    if y.is_nan() {
                        continue;
                    }
                    let out = smoother.fold::<true>(&mut st, x, y);
                    assert_eq!(
                        smoother.value_at(&st, x).to_bits(),
                        out.to_bits(),
                        "{algo:?} {deg} at sample {i}"
                    );
                    // Each sample's deposit weight, as the smoother sees it at x (EMA's decay is common to every point, so it is dropped).
                    let weight = match algo {
                        Algorithm::Ema => (x / 4.0).exp(),
                        _ => 1.0 + (x - xs[0]),
                    };
                    points.push((x, y, weight));
                    let later = x + 2.0;
                    let carried = smoother.value_at(&st, later);
                    if deg == 0 {
                        assert_eq!(carried.to_bits(), out.to_bits(), "{algo:?} order 0 holds");
                    } else if points.len() > deg as usize {
                        close(
                            carried,
                            wls_at(&points, deg as usize, later),
                            &format!("{algo:?} {deg} past sample {i}"),
                        );
                    }
                }
            }
        }
    }

    /// A causal evaluator folds its samples once, forward, across its runs and from slot to slot: its slots take the bits of one over a single run, and the checkpoints it records as it goes, finished at the tail, are a whole fold's.
    #[test]
    fn a_recording_evaluator_folds_once_across_runs() {
        let xs: Vec<f64> = (0..3000).map(|i| (2 * i + i % 3) as f64).collect();
        let ys: Vec<f64> = (0..3000)
            .map(|i| {
                if i % 97 == 3 {
                    f64::INFINITY
                } else {
                    ((i * 7919) % 1009) as f64 / 3.0
                }
            })
            .collect();
        let bits = |cps: &[CausalState]| -> Vec<_> { cps.iter().map(CausalState::bits).collect() };
        for algo in [Algorithm::Ema, Algorithm::Triangular] {
            for deg in 0..=2u32 {
                let plan = smoothing_plan(&xs, algo, true).0;
                let smoother = SlotSmoother::new(algo, 0, 50.0, deg, plan);
                let SlotSmoother::Causal(causal) = smoother else {
                    unreachable!("a causal algorithm");
                };
                let (mut whole, mut state) = (Vec::new(), CausalState::START);
                causal.fold_recording(&[(&xs, &ys)], &mut state, 0, Some(&mut whole));
                whole.push(state);
                let (a, b) = (700, 2100);
                let mut walking = SlotEvaluator::over(smoother, &xs, &ys);
                let mut parted = SlotEvaluator::new(
                    smoother,
                    [
                        (&xs[..a], &ys[..a]),
                        (&xs[a..b], &ys[a..b]),
                        (&xs[b..], &ys[b..]),
                    ],
                    true,
                );
                for q in [-1.0, 0.0, 0.5, 7.0, 1500.25, 1501.0, 3000.0, 5998.0, 5999.5] {
                    let want = walking.at(q).to_bits();
                    assert_eq!(parted.at(q).to_bits(), want, "{algo:?} {deg} parts at {q}");
                }
                assert_eq!(bits(&parted.finish()), bits(&whole), "{algo:?} {deg}");
                assert!(walking.finish().is_empty(), "records only when asked");
            }
        }
    }

    /// A constant run read far past its samples stays constant, by every smoother: its values fit relative to one of them (Savitzky-Golay's nearest the slot, EMA's newest, Triangular's first), so no curvature of rounding is left for the distance to amplify (fitted at their level, five samples of 1 read 134 million steps on draw -0.48 by Savitzky-Golay at order 2 and -3.86 by EMA, and thirteen draw 1.56 by Triangular).
    #[test]
    fn a_constant_run_read_far_past_its_samples_stays_constant() {
        let (xs, ys) = ([0.0, 1.0, 2.0, 3.0, 4.0], [1.0; 5]);
        let causal = |smoother: CausalSmoother, n: usize| {
            let mut st = CausalState::START;
            smoother.fold_range(&mut st, |i| i as f64, |_| 1.0, 0, n);
            smoother.value_at(&st, 134_217_728.0)
        };
        for deg in 0..=2 {
            let tau = Width::fixed(EMA_FLOOR_R[2] * 268_435_456.0);
            let ema = CausalSmoother::Ema {
                tau,
                dx_ref: 1.0,
                deg,
            };
            assert_eq!(causal(ema, 5), 1.0, "EMA order {deg}");
            let triangular = CausalSmoother::Triangular { dx_ref: 1.0, deg };
            assert_eq!(causal(triangular, 13), 1.0, "Triangular order {deg}");
            let got = savgol_at(
                &|j| xs[j],
                &|j| ys[j],
                5,
                134_217_727.5,
                Width::fixed(SAVGOL_FLOOR_R[2] * 268_435_456.0),
                deg,
                &mut SavgolCursor::default(),
            );
            assert_eq!(got, 1.0, "order {deg}");
        }
    }

    /// Triangular sets its base once, at the first finite sample, so a second sample at the first x folds relative to it like any other: samples 1 and 3 at x 0, then 2 at 1 and 2, give order 0's weighted means 1, 2, 2, 2 (weights 1, 1, 2, 3), and a slot at 0.5 reads 2.
    #[test]
    fn triangular_keeps_its_first_samples_base_across_a_repeated_first_x() {
        let (xs, ys) = ([0.0, 0.0, 1.0, 2.0], [1.0, 3.0, 2.0, 2.0]);
        let triangular = CausalSmoother::Triangular {
            dx_ref: 1.0,
            deg: 0,
        };
        let mut st = CausalState::START;
        let out = triangular.run(&xs, &ys, &mut st);
        assert_eq!(out, [1.0, 2.0, 2.0, 2.0]);
        let mut evaluator = SlotEvaluator::over(SlotSmoother::Causal(triangular), &xs, &ys);
        assert_eq!(evaluator.at(0.5), 2.0);
    }

    /// A window whose only sample on one side sits at its edge, which weighs nothing, fits about the samples that weigh: a run logging every 20 steps up to 100,002, five steps from 120,000 and every 20 again from 140,010, at window 2,000 (half-width 20,000), puts a step chart's slot at 120,002 with 100,002 at its edge. Fitted about the middle between that sample and the five, order 2 reads 0.4 where the five's fit reads -3.46. A hole first in the window, at 100,003, is left out alike, and so are both mirrored about the slot, last in it; a sample weighing 1e-18, just inside the edge, is fitted but sets no middle.
    #[test]
    fn an_edge_sample_leaves_the_fit_on_the_samples_that_weigh() {
        let base = [3.0, -1.0, 4.0, 1.0, -5.0, 9.0, 2.0, -6.0, 5.0, 3.0];
        let mut xs: Vec<f64> = (0..20)
            .rev()
            .map(|i| 100_002.0 - 20.0 * f64::from(i))
            .collect();
        xs.extend((0..5).map(|i| 120_000.0 + f64::from(i)));
        xs.extend((0..20).map(|i| 140_010.0 + 20.0 * f64::from(i)));
        let mut ys: Vec<f64> = [base, base].concat();
        ys.extend([10.0, -10.0, 7.0, -7.0, 2.0]);
        ys.extend([base, base].concat());
        let spec = GridSpec {
            target: 1000,
            is_step_axis: true,
            log_buckets: false,
            shift_one: false,
        };
        let plan = smoothing_plan(&xs, Algorithm::SavitzkyGolay, true).0;
        let floor = AxisDecision::of(&[&xs], spec).floor();
        let smoother =
            SlotSmoother::new(Algorithm::SavitzkyGolay, 2000, 1.0, 2, plan).floored(floor);
        let SlotSmoother::SavitzkyGolay { h_x, .. } = smoother else {
            unreachable!()
        };
        let xq = 120_002.0;
        assert_eq!(
            h_x.at(xq),
            20_000.0,
            "the user's half-width, above the floor"
        );
        let kinds = vec![0u8; xs.len()];
        let (chart, _) = shared_chart(
            &[&xs],
            &[&[]],
            &[&ys],
            &[&kinds],
            &[&[]],
            true,
            &mut [Some(SlotEvaluator::over(smoother, &xs, &ys))],
            spec,
            AxisDecision::of(&[&xs], spec),
        );
        let k = chart.x_values.iter().position(|&x| x == xq).unwrap();
        let five: Vec<(f64, f64, f64)> = (20..25)
            .map(|j| {
                (
                    xs[j],
                    ys[j],
                    (1.0 - ((xs[j] - xq) / 20_000.0).powi(2)).powi(2),
                )
            })
            .collect();
        close(
            chart.series[0].values[k],
            wls_at(&five, 2, xq),
            "the slot at 120,002",
        );
        let fit_within = |xs: &[f64], ys: &[f64], h: f64| {
            let (x, y, mut cursor) = (|j: usize| xs[j], |j: usize| ys[j], SavgolCursor::default());
            savgol_at(&x, &y, xs.len(), xq, Width::fixed(h), 2, &mut cursor)
        };
        let fit = |xs: &[f64], ys: &[f64]| fit_within(xs, ys, 20_000.0);
        // Just inside the edge, 100,002 weighs 1e-18: too little to set the middle, though the fit, ill-conditioned, keeps fewer digits.
        let h: f64 = 20_000.0 + 1e-5;
        let light = (1.0 - (20_000.0 / h).powi(2)).powi(2);
        let six = [&five[..], &[(100_002.0, 3.0, light)]].concat();
        let (got, want) = (fit_within(&xs, &ys, h), wls_at(&six, 2, xq));
        assert!((got - want).abs() < 1e-6, "light first: {got} vs {want}");
        // Mirrored about the slot, the edge sample, then the hole, come last.
        let mirror = |xs: &[f64], ys: &[f64]| -> (Vec<f64>, Vec<f64>) {
            let xs = xs.iter().rev().map(|x| 2.0 * xq - x).collect();
            (xs, ys.iter().rev().copied().collect())
        };
        let mirrored: Vec<_> = five.iter().map(|&(x, y, w)| (2.0 * xq - x, y, w)).collect();
        let (mx, my) = mirror(&xs, &ys);
        close(fit(&mx, &my), wls_at(&mirrored, 2, xq), "edge last");
        let (mut xs, mut ys) = (xs, ys);
        (xs[19], ys[19]) = (100_003.0, f64::NAN);
        close(fit(&xs, &ys), wls_at(&five, 2, xq), "hole first");
        let (mx, my) = mirror(&xs, &ys);
        close(fit(&mx, &my), wls_at(&mirrored, 2, xq), "hole last");
    }

    /// Custom x's interpolation reads each segment as it reaches it: past either end the nearest segment's line, and at a repeated x the first of its points (the segment ending there).
    #[test]
    fn lerp_from_takes_the_first_point_at_a_repeated_x() {
        let points = [(0.0, 0.0), (1.0, 3.0), (1.0, 5.0), (2.0, 7.0)];
        let mut lerp = lerp_from(points.into_iter());
        for (xq, want) in [(-1.0, -3.0), (0.5, 1.5), (1.0, 3.0), (1.5, 6.0), (3.0, 9.0)] {
            assert_eq!(lerp(xq), want, "at {xq}");
        }
        assert!(lerp_from(std::iter::empty())(1.0).is_nan());
        assert_eq!(lerp_from([(4.0, 2.0)].into_iter())(9.0), 2.0);
    }

    /// A lagging run's last slot, centered past its last sample by another run, takes its smoother's value there rather than a line run out along its last segment.
    #[test]
    fn lagging_run_slot_evaluates_its_smoother_past_its_end() {
        let xa: Vec<f64> = (0..64).map(f64::from).collect();
        let xb: Vec<f64> = (0..35).map(f64::from).collect();
        let ya: Vec<f64> = xa.iter().map(|x| (x * 0.3).sin()).collect();
        let yb: Vec<f64> = xb.iter().map(|x| ((x * 1.7).sin() * 5.0).round()).collect();
        let spec = GridSpec {
            target: 4,
            is_step_axis: true,
            log_buckets: false,
            shift_one: false,
        };
        for (algo, window, deg) in [
            (Algorithm::SavitzkyGolay, 20u32, 1u32),
            (Algorithm::Ema, 0, 0),
            (Algorithm::Ema, 0, 1),
        ] {
            let pb = smooth_run(&xb, &yb, algo, window, 5.0, deg, true);
            let smoother = |x: &[f64]| {
                SlotSmoother::new(algo, window, 5.0, deg, smoothing_plan(x, algo, true).0)
            };
            let mut slots = vec![
                Some(SlotEvaluator::over(smoother(&xa), &xa, &ya)),
                Some(SlotEvaluator::over(smoother(&xb), &xb, &yb)),
            ];
            let kinds = [vec![0u8; xa.len()], vec![0u8; xb.len()]];
            let (chart, _) = shared_chart(
                &[&xa, &xb],
                &[&[], &[]],
                &[&ya, &yb],
                &[&kinds[0], &kinds[1]],
                &[&[], &[]],
                true,
                &mut slots,
                spec,
                AxisDecision::of(&[&xa, &xb], spec),
            );
            assert_eq!(
                chart.x_values,
                vec![7.5, 23.5, 39.5, 55.5],
                "width-16 cells"
            );
            let b = chart.series[1].values[2];
            let want = SlotEvaluator::over(smoother(&xb), &xb, &yb).at(39.5);
            assert_eq!(
                b.to_bits(),
                want.to_bits(),
                "{algo:?} {deg}: B's smoother at 39.5"
            );
            let lerp = lerp_from(xb.iter().copied().zip(pb.iter().copied()))(39.5);
            assert_ne!(
                b.to_bits(),
                lerp.to_bits(),
                "{algo:?} {deg}: not run out along the last segment"
            );
            if algo == Algorithm::Ema && deg == 0 {
                assert_eq!(
                    b.to_bits(),
                    pb[34].to_bits(),
                    "an EMA holds its last value past its samples"
                );
            }
        }
    }
}
