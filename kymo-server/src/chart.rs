//! Chart processing pipeline: smoothing, alignment, and min/max-envelope downsampling.

use crate::proto::smoothing_config::Algorithm;

/// Server-side cap on the smoothing window: chart_params clamps the wire value to it, and the smoothers and delta planner rely on that bound.
pub(crate) const MAX_SMOOTHING_WINDOW: u32 = 4096;

/// Spacing of an x grid if it is uniform (within float tolerance), else
/// None. Step grids are integers cast to f64, so contiguous logging — the
/// overwhelmingly common case — compares exactly and takes the fast
/// index-space smoothers; anything irregular gets the x-aware path.
fn uniform_spacing(xs: &[f64]) -> Option<f64> {
    #[cfg(test)]
    SPACING_DERIVATIONS.with(|counts| counts.set((counts.get().0 + 1, counts.get().1)));
    if xs.len() < 2 {
        return Some(1.0);
    }
    let d0 = xs[1] - xs[0];
    let tol = d0.abs() * 1e-9 + 1e-12;
    for w in xs.windows(2) {
        if ((w[1] - w[0]) - d0).abs() > tol {
            return None;
        }
    }
    Some(d0)
}

/// Median interval between consecutive distinct x values — the x-unit the window scales are quoted in
/// on irregular grids. Median, not mean: a handful of logging gaps must
/// not stretch every window.
/// Zero gaps are skipped: a majority of them would collapse the unit, overflowing Triangular's weights, resetting EMA at every new x and shrinking Savitzky–Golay windows to one x.
/// Without two distinct x values every unit smooths alike, so the unit is 1.
/// The MIN_POSITIVE floor keeps every result decodable by decode_smoothing_plan (query.rs).
pub(crate) fn median_dx(xs: &[f64]) -> f64 {
    #[cfg(test)]
    SPACING_DERIVATIONS.with(|counts| counts.set((counts.get().0, counts.get().1 + 1)));
    let mut gaps: Vec<f64> = xs
        .windows(2)
        .map(|w| w[1] - w[0])
        .filter(|&gap| gap > 0.0)
        .collect();
    if gaps.is_empty() {
        return 1.0;
    }
    gaps.sort_by(f64::total_cmp);
    gaps[gaps.len() / 2].max(f64::MIN_POSITIVE)
}

/// The only whole-series, data-derived state a smoother consumes. `NoState` means appends cannot rescale a held prefix; `Uniform` selects the index-space Savitzky–Golay kernel; `Median` is time EMA/Triangular's decay unit or an irregular Savitzky–Golay x scale.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SmoothingPlan {
    NoState,
    Uniform,
    /// f64 bits of a median of at least f64::MIN_POSITIVE (median_dx floors it; decode_smoothing_plan rejects anything smaller), so the smoothers divide by it unguarded.
    Median(u64),
}

impl SmoothingPlan {
    fn causal_dx_ref(self) -> f64 {
        match self {
            Self::NoState => 1.0,
            Self::Median(bits) => f64::from_bits(bits),
            Self::Uniform => panic!("causal smoother cannot use a uniform-branch plan"),
        }
    }
}

/// Derive exactly the state the selected algorithm consumes: no spacing work for step EMA/Triangular, one median for time EMA/Triangular, and one uniformity scan plus a median only on the irregular Savitzky–Golay branch.
pub(crate) fn smoothing_plan(xs: &[f64], algo: Algorithm, step_sized_x: bool) -> SmoothingPlan {
    match algo {
        Algorithm::None => SmoothingPlan::NoState,
        Algorithm::Ema | Algorithm::Triangular if step_sized_x => SmoothingPlan::NoState,
        Algorithm::Ema | Algorithm::Triangular => SmoothingPlan::Median(median_dx(xs).to_bits()),
        Algorithm::SavitzkyGolay => {
            if uniform_spacing(xs).is_some() {
                SmoothingPlan::Uniform
            } else {
                SmoothingPlan::Median(median_dx(xs).to_bits())
            }
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

/// One-sided exponentially weighted polynomial regression, evaluated at each current x. Moments are kept in coordinates whose origin moves with the current sample; translating the five scalar moments and solving the at-most 3x3 normal equations is constant work per point. The coordinate unit is the time constant τ, which keeps the moment matrix well-scaled at any τ.
fn ema_polyfit(xs: &[f64], y: &[f64], tau: f64, dx_ref: f64, poly_order: u32) -> Vec<f64> {
    let mut out = vec![f64::NAN; y.len()];
    let deg = poly_order.min(2) as usize;
    let mut mw = [0.0f64; 5];
    let mut sw = [0.0f64; 3];
    let mut prev_x = f64::NAN;

    for (i, &yv) in y.iter().enumerate() {
        if !yv.is_finite() {
            continue;
        }
        if prev_x.is_finite() {
            // In units of τ, old u becomes u − h at the new origin.
            let h = ((xs[i] - prev_x) / dx_ref).max(0.0) / tau;
            let decay = (-h).exp();
            if decay == 0.0 {
                // Avoid 0 * inf when an enormous x gap underflows the old
                // state's weight to exactly zero.
                mw = [0.0; 5];
                sw = [0.0; 3];
            } else {
                let old_mw = mw;
                let old_sw = sw;
                for (k, moment) in mw.iter_mut().enumerate().take(2 * deg + 1) {
                    let mut translated = 0.0;
                    for (l, &moment) in old_mw.iter().enumerate().take(k + 1) {
                        translated += binomial(k, l) * (-h).powi((k - l) as i32) * moment;
                    }
                    *moment = decay * translated;
                }
                for (k, moment) in sw.iter_mut().enumerate().take(deg + 1) {
                    let mut translated = 0.0;
                    for (l, &moment) in old_sw.iter().enumerate().take(k + 1) {
                        translated += binomial(k, l) * (-h).powi((k - l) as i32) * moment;
                    }
                    *moment = decay * translated;
                }
            }
        }
        mw[0] += 1.0;
        sw[0] += yv;
        prev_x = xs[i];
        out[i] = fit_from_moments(&mw, &sw, deg);
    }
    out
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

/// Causal triangular-weighted polynomial regression, evaluated at each
/// current x. Where [`ema_polyfit`] weights the past by a geometric decay in
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
/// origin shift [`ema_polyfit`] uses. The fitted value is read at the current
/// point (u = 0), which is unit-invariant, so the rescale changes only
/// conditioning, not the result.
///
/// Within a fixed dx_ref the output at a sample depends only on the samples up to it, so appending later points never moves an earlier value — the delta reach is 0, exactly as for EMA. Step/custom-x smoothing fixes dx_ref at one step; time smoothing uses the whole series' median interval, so an append that shifts the median rescales every output and the exact [`SmoothingPlan`] gate answers full. Reach 0 is therefore the behavior across a delta continuation, not a promise that a time-axis median shift can never move history.
fn triangular_polyfit(xs: &[f64], y: &[f64], dx_ref: f64, poly_order: u32) -> Vec<f64> {
    let mut out = vec![f64::NAN; y.len()];
    let deg = poly_order.min(2) as usize;
    let mut mw = [0.0f64; 5];
    let mut sw = [0.0f64; 3];
    let mut prev_x = f64::NAN;
    let mut weight = 1.0f64; // 1 + elapsed x since the first sample, in dx_ref units
    for (i, &yv) in y.iter().enumerate() {
        if !yv.is_finite() {
            continue;
        }
        if prev_x.is_finite() {
            let prev_weight = weight;
            // Weight accumulates the x-distance covered (in dx_ref units), so
            // a gap raises it by the gap's width, not by one.
            weight += (xs[i] - prev_x) / dx_ref;
            // One change of variables u ↦ r·u − h that both moves the origin
            // to this sample and rescales the unit from prev_weight·dx_ref to
            // weight·dx_ref, so u stays bounded as the elapsed x grows.
            let r = prev_weight / weight;
            let h = (xs[i] - prev_x) / (weight * dx_ref);
            let old_mw = mw;
            let old_sw = sw;
            for (k, moment) in mw.iter_mut().enumerate().take(2 * deg + 1) {
                let mut acc = 0.0;
                for (l, &m) in old_mw.iter().enumerate().take(k + 1) {
                    acc += binomial(k, l) * r.powi(l as i32) * (-h).powi((k - l) as i32) * m;
                }
                *moment = acc;
            }
            for (k, moment) in sw.iter_mut().enumerate().take(deg + 1) {
                let mut acc = 0.0;
                for (l, &m) in old_sw.iter().enumerate().take(k + 1) {
                    acc += binomial(k, l) * r.powi(l as i32) * (-h).powi((k - l) as i32) * m;
                }
                *moment = acc;
            }
        }
        mw[0] += weight;
        sw[0] += weight * yv;
        prev_x = xs[i];
        out[i] = fit_from_moments(&mw, &sw, deg);
    }
    out
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
/// NaN-free interior runs in O(n) off block-local prefix sums.
///
/// Numerically there is no sliding state to drift: every output is either a fresh direct fit or a difference of prefix sums rebuilt for each block of 2·half outputs. A window's center lies within half of its block's origin and its inputs within 2·half, so recentering amplifies rounding by roughly 3⁶ whatever n or the absolute step: about 2e-12 of the series' magnitude, far below f32 rounding except for values near zero. Longer blocks save little time for much more error (4·half: 12% faster, ~40× the error).
///
/// NaN values are excluded from every fit. The fit still produces a value
/// at a NaN input position, but [`smooth_run_with_plan`] — the pipeline's entry
/// point — masks those positions back to NaN, so a hole in the input is
/// never filled in what reaches the wire.
fn savitzky_golay(y: &[f64], window: usize, poly_order: u32) -> Vec<f64> {
    let n = y.len();
    let deg = poly_order.min(2) as usize;
    let mut out = vec![f64::NAN; n];
    if n == 0 {
        return out;
    }
    let (half, h_s) = savgol_geometry(window);

    // Interior shortcut: on a full, NaN-free window the odd weighted
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

    // NaN prefix counts: O(1) per-window cleanliness check.
    let mut nan_pfx = vec![0u32; n + 1];
    for (i, v) in y.iter().enumerate() {
        nan_pfx[i + 1] = nan_pfx[i] + u32::from(v.is_nan());
    }

    // Clipped windows at the data edges: direct weighted fits.
    for (c, output) in out.iter_mut().enumerate().take(half.min(n)) {
        *output = sg_fit_window(y, 0, (c + half + 1).min(n), c, h_s, deg);
    }
    for (c, output) in out.iter_mut().enumerate().skip(n.saturating_sub(half)) {
        if c >= half {
            *output = sg_fit_window(y, c - half, n, c, h_s, deg);
        }
    }

    // Full-window interior: the even centered data moments Σxᵏy (k ≤ 6)
    // come from block prefix sums of u^k·y recentered onto each window
    // (binomials in the center offset d), then fold in the window:
    // Sₖʷ = Sₖ − 2Sₖ₊₂ + Sₖ₊₄ in half-width-scaled x. Windows containing
    // NaN divert to the direct path.
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
    let mut block_start = lo_int;
    while block_start < hi_int {
        let block_end = (block_start + 2 * half).min(hi_int);
        let lo = block_start - half;
        let len = block_end + half - lo;
        let origin = (len / 2) as f64;
        for u in 0..len {
            let v = y[lo + u];
            let v = if v.is_nan() { 0.0 } else { v };
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
            if nan_pfx[c + half + 1] != nan_pfx[c - half] {
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

/// (half, taper scale) for a window, shared by [`savitzky_golay`], [`savgol_dependency_start`] and the irregular-x scale. Windows floor at 3, the smallest whose taper weights any neighbor (window 3: half 1 at scale 1.5). Prefix-sum blocks cover 2·half outputs.
///
/// The taper scale is window/2 in floating point, so windows 2k and 2k+1 differ; the support stays the integer `half`, so odd windows give their support-edge points a small positive weight.
fn savgol_geometry(window: usize) -> (usize, f64) {
    let window = window.max(3);
    (window / 2, window as f64 / 2.0)
}

/// Conservative starting index for outputs an insertion can change in the index-space smoother, including wire-visible rounding. Inserting a row shifts every later prefix-sum block; extending the final block can also move its origin. Even an unchanged mathematical window can then round differently. Blocks whose expanded input ranges end before `first_new` use identical inputs and arithmetic in every held snapshot; from the first intersecting block to the array end we must re-emit, including when the new row itself is later trimmed away.
pub(crate) fn savgol_dependency_start(window: usize, first_new: usize) -> usize {
    let (half, _) = savgol_geometry(window);
    let block = 2 * half;
    if first_new < block {
        0
    } else {
        half + (first_new - block) / block * block
    }
}

/// Biweight-weighted least-squares fit at x = 0 over (x, y) points, x in taper half-widths and NaN y excluded; too few weighted points step the degree down ([`fit_from_moments`]).
fn biweight_fit(points: impl Iterator<Item = (f64, f64)>, deg: usize) -> f64 {
    // Weighted moments Σw·xᵏ (k ≤ 4) and Σw·xᵏ·y (k ≤ 2).
    let mut mw = [0.0f64; 5];
    let mut sw = [0.0f64; 3];
    for (x, yv) in points.filter(|(_, yv)| !yv.is_nan()) {
        let u = 1.0 - x * x;
        let mut pw = u * u;
        for k in 0..5 {
            mw[k] += pw;
            if k < 3 {
                sw[k] += pw * yv;
            }
            pw *= x;
        }
    }
    fit_from_moments(&mw, &sw, deg)
}

/// Direct fit over y[lo..hi] with weights centered at `c`, returning the fitted value at `c`.
fn sg_fit_window(y: &[f64], lo: usize, hi: usize, c: usize, h_s: f64, deg: usize) -> f64 {
    biweight_fit((lo..hi).map(|j| ((j as f64 - c as f64) / h_s, y[j])), deg)
}

/// Fitted value at x = 0 of the weighted least-squares polynomial of degree ≤ `deg`, from mw[k] = Σw·xᵏ and sw[k] = Σw·xᵏ·y. It sums qₖ(0)·⟨qₖ, y⟩/Dₖ over the weight-orthogonal monic polynomials qₖ, Dₖ = ⟨qₖ, qₖ⟩ (an unpivoted LDLᵀ of the moment matrix), so each degree adds one term to the lower degree's arithmetic.
///
/// Degrees are added while the pivot Dₖ exceeds 4ε·mw[2k]. Every term subtracted in forming Dₖ is at most mw[2k] (Cauchy–Schwarz), so below that Dₖ is rounding noise at any scale of x or the weights: the points numerically lie on k distinct x, which degree k − 1 already interpolates, so its value at the weighted point x = 0 is the answer. A small pivot above the cut is kept: its correction still carries digits. The cut does not see rounding accumulated before the pivot (in the moments, or carried in from D₁), so a noise pivot can occasionally pass. NaN without positive weight.
#[allow(clippy::neg_cmp_op_on_partial_ord)] // The negated comparisons also reject NaN pivots.
fn fit_from_moments(mw: &[f64; 5], sw: &[f64; 3], deg: usize) -> f64 {
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
    fit -= mu * t1 / d1;
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
    fit + (beta * mu - m20) * t2 / d2
}

/// Savitzky-Golay for an irregular x grid: the same biweight WLS fit as
/// [`savitzky_golay`], but with offsets measured in real x. Window
/// half-width `h_x` is in x units; membership and weights come from
/// x-distance, so a logging gap narrows the effective window instead of
/// silently widening it like index counting would. O(n·k) direct fits — the
/// price of irregularity; uniform grids take the O(n) prefix-sum path.
fn savgol_x(xs: &[f64], y: &[f64], h_x: f64, deg: usize) -> Vec<f64> {
    let n = y.len();
    let mut out = vec![f64::NAN; n];
    let (mut lo, mut hi) = (0usize, 0usize);
    for i in 0..n {
        while xs[i] - xs[lo] > h_x {
            lo += 1;
        }
        if hi < i {
            hi = i;
        }
        while hi + 1 < n && xs[hi + 1] - xs[i] <= h_x {
            hi += 1;
        }
        out[i] = biweight_fit((lo..=hi).map(|j| ((xs[j] - xs[i]) / h_x, y[j])), deg);
    }
    out
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

    /// Two weighted points leave the quadratic's pivot at rounding level, below the cut in these fixtures, so order 2 returns exactly order 1's result in both the direct SG fit and the causal fits.
    #[test]
    fn underdetermined_order_2_matches_order_1() {
        let (xs, y) = ([0.0, 1.0], [7.0, 9.0]);
        let fits = |order| {
            [
                savitzky_golay(&y, 4, order),
                ema_polyfit(&xs, &y, 10.0, 1.0, order),
                triangular_polyfit(&xs, &y, 1.0, order),
            ]
        };
        assert_eq!(fits(2), fits(1));
    }

    /// The degree switch is scale-free: three early samples determine a quadratic, which interpolates the newest one, at each tested time constant up to τ = 1e6, where they span a millionth of the EMA's unit.
    #[test]
    fn early_ema_samples_fit_full_degree() {
        let (xs, y) = ([0.0, 1.0, 2.0], [0.0, 0.0, 1.0]);
        for tau in [1e6, 1e3, 2.0] {
            let out = ema_polyfit(&xs, &y, tau, 1.0, 2);
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
// Implements docs/chart-shared-axis.md + docs/log-scale-buckets.md. Every run — step, timestamp, or custom-x — emits on ONE shared slot sequence. Slots mix two kinds: an ENVELOPE slot is a downsampling bucket (x = average of its first and last point across runs; y = per-run min/max envelope, dense, min == max behind a single point), a RAW slot is one exact logged x shared by every run that logged it. A bucket downsamples only when it EXPECTS at least [`ENV_MIN_POINTS`] points — below that its points ship raw (two exact points render better than one envelope). Few enough distinct x skips the grid entirely (passthrough: every slot raw); a grid with no occupied envelope bucket emits the same shape. Buckets are disjoint and every point, including the chart's first and last, follows its cell's ordinary raw-or-envelope rule. Works on any ascending f64 x — custom-x is just a non-step x metric (its non-finite-x markers ride in via `xnan`).

use std::borrow::Cow;
use std::ops::ControlFlow;

use crate::chart_delta::{DenseChart, DenseSeries};

/// A bucket downsamples only if it EXPECTS at least this many points; fewer ship raw (docs/log-scale-buckets.md: "a bucket with only 2 points should just be two non-downsampled points"). Step axes expect one point per integer step, so the test is width >= 4 — grid-pure, deliberately blind to missing points; non-step axes use the bucket's distinct-x count instead. Both are monotone against a FIXED threshold, which lets the delta planner compare the exact verified held structure with the current structure.
const ENV_MIN_POINTS: f64 = 4.0;

/// Octaves below this clamp share the bottom log-ladder cell (degenerate custom-x under 2^-500 merges — never drops), keeping every cell width an exact power of two clear of the subnormal range.
const OCTAVE_MIN: i32 = -500;

/// floor(log2 x) of a positive x, from the exponent bits — exact at the 2^k boundaries, where a floating log2().floor() can land on either side.
fn octave(x: f64) -> i32 {
    debug_assert!(x > 0.0);
    let e = ((x.to_bits() >> 52) & 0x7ff) as i32;
    (if e == 0 { -1023 } else { e - 1023 }).max(OCTAVE_MIN)
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
    /// The log ladder: octave [2^k, 2^(k+1)) splits into s = 2^m buckets of width 2^(k−m) — [16,20),[20,24),[24,28),[28,32),[32,40) is m = 2 (docs/log-scale-buckets.md). id = k·s + j, consecutive across octave boundaries. `shift` = the ladder runs in log(x+1) (step/timestamp axes, where step 0 must render; the +1 is exact on their integer x). Only shifted-positive x has a cell; anything below is raw by fiat — and normally never reaches the grid, because unplottable x becomes an exceptional-point marker upstream (query.rs).
    Log { m: i32, shift: bool },
}

#[derive(Debug, Clone, Copy)]
struct Grid {
    kind: GridKind,
    base: i64,
    cells: usize,
}

impl Grid {
    /// Absolute id of the cell containing `x`; None = below the grid (log ladder, x <= 0). Exact: within an octave x − 2^k is exact (Sterbenz), and every width is a power of two.
    #[allow(clippy::neg_cmp_op_on_partial_ord)] // `!(xe > 0)` intentionally rejects NaN coordinates too.
    fn id(kind: GridKind, x: f64) -> Option<i64> {
        match kind {
            GridKind::Linear { x_lo, width } => Some(((x - x_lo) / width) as i64),
            GridKind::Log { m, shift } => {
                let xe = if shift { x + 1.0 } else { x };
                if !(xe > 0.0) {
                    return None;
                }
                let k = octave(xe);
                let s = 1i64 << m;
                let j = (((xe - pow2(k)) / pow2(k - m)) as i64).clamp(0, s - 1);
                Some(k as i64 * s + j)
            }
        }
    }

    /// Instantiated index of the cell containing `x`; None = below the grid, raw by fiat.
    fn index(&self, x: f64) -> Option<usize> {
        Self::id(self.kind, x).map(|id| {
            let offset = id - self.base;
            debug_assert!(
                (0..self.cells as i64).contains(&offset),
                "point x={x} mapped outside its instantiated grid: id={id}, base={}, cells={}",
                self.base,
                self.cells
            );
            offset.clamp(0, self.cells as i64 - 1) as usize
        })
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

/// The grid for a chart that decided to downsample. Linear: [`stable_grid`], as ever. Log ladder: the largest subdivision s = 2^m whose cell count over the chart's positive span stays within target — ids are consecutive, so the count is closed-form, no data scanned; a sub-octave zoom therefore scales s up to match linear slot density. `p_lo` (the smallest positive x) clamps to 1 on step axes — sub-integer octaves cannot hold a step. No positive x at all falls back to the linear grid: a bounded grid beats shipping every distinct nonpositive x raw.
fn make_grid(xs: &[&[f64]], g_first: f64, g_last: f64, spec: GridSpec) -> Grid {
    if spec.log_buckets {
        // log(x+1) only when the chart contains x = 0 — otherwise plain log(x).
        let shift_one = spec.shift_one && g_first == 0.0;
        let shift = if shift_one { 1.0 } else { 0.0 };
        let mut p_lo = f64::INFINITY;
        for r in xs {
            let i = r.partition_point(|&v| v + shift <= 0.0);
            if let Some(&v) = r.get(i) {
                p_lo = p_lo.min(v);
            }
        }
        if spec.is_step_axis {
            p_lo = p_lo.max(1.0 - shift);
        }
        if p_lo.is_finite() && p_lo <= g_last {
            let cells_for = |m: i32| {
                let k = GridKind::Log {
                    m,
                    shift: shift_one,
                };
                (Grid::id(k, g_last).unwrap() - Grid::id(k, p_lo).unwrap() + 1) as usize
            };
            // m caps at 40: far past any pixel target, and cell counts stay in i64. m = 0 can still exceed target (a span of more octaves than pixels) — the octave count bounds the excess at ~1500 cells.
            let mut m = 0;
            while m < 40 && cells_for(m + 1) <= spec.target {
                m += 1;
            }
            let kind = GridKind::Log {
                m,
                shift: shift_one,
            };
            let base = Grid::id(kind, p_lo).unwrap();
            let cells = (Grid::id(kind, g_last).unwrap() - base + 1) as usize;
            return Grid { kind, base, cells };
        }
    }
    let (x_lo, width, buckets) = stable_grid(g_first, g_last, spec.target);
    Grid {
        kind: GridKind::Linear { x_lo, width },
        base: 0,
        cells: buckets,
    }
}

/// Linear value of a smoothed curve (finite (x, y) pairs, ascending) at `xq`,
/// EXTRAPOLATING past either end along the nearest segment's slope rather than
/// clamping. A bucket center can sit beyond a run's own data when other runs
/// stretch the shared center: this run has a point in the bucket and the center
/// is the average of every run's points in it, so the gap is under one bucket
/// width (e.g. this run's lone point at the bucket's edge, the rest filling it).
/// Letting the line run out to it on the local slope is fine — the invention is
/// sub-bucket and keeps the line reaching every center it occupies. `ptr` is
/// carried across ascending `xq` calls so a run's slots cost O(points + slots)
/// total.
fn lerp_at(fin: &[(f64, f64)], ptr: &mut usize, xq: f64) -> f64 {
    match fin.len() {
        0 => return f64::NAN,
        1 => return fin[0].1, // no slope to extrapolate along
        _ => {}
    }
    // Land on the segment [ptr, ptr+1] covering xq, capped at the last segment
    // so xq beyond the data extrapolates from it (rather than clamping).
    while *ptr + 2 < fin.len() && fin[*ptr + 1].0 < xq {
        *ptr += 1;
    }
    let (x0, y0) = fin[*ptr];
    let (x1, y1) = fin[*ptr + 1];
    if x1 > x0 {
        y0 + (y1 - y0) * (xq - x0) / (x1 - x0)
    } else {
        y0
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
/// points. Only for the cheap/degenerate passthrough cases (target 0, a single
/// x position, or a small total); the size-bounded paths below avoid it.
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
///   - degenerate (no downsampling asked, or a single x) ships every distinct
///     point — the only case that still sorts, and it's cheap or explicit;
///   - a linear STEP grid whose width falls under ENV_MIN_POINTS also passes
///     through: every bucket would ship its points raw anyway ("2 points beat
///     1 envelope bucket"), and that IS the distinct axis — at most ~2×target
///     slots (width < 4 means span ≤ 2·target);
///   - step axis with contiguous runs decides from interval endpoints alone
///     ([`decide_step`], O(runs)) — no points touched;
///   - everything else counts distinct with an early-exit at `target+1`
///     ([`decide_general`]) — large N means distinct ≫ target, so it bails fast.
fn decide_axis(xs: &[&[f64]], g_first: f64, g_last: f64, spec: GridSpec) -> Decision {
    // target 0 = "never downsample"; a single x position has nothing to thin.
    // Either way ship every distinct point.
    if spec.target == 0 || g_last <= g_first {
        return Decision::Passthrough(sorted_distinct(xs));
    }
    let cap = if spec.is_step_axis && !spec.log_buckets {
        let (_, width, _) = stable_grid(g_first, g_last, spec.target);
        if width >= ENV_MIN_POINTS {
            spec.target
        } else {
            usize::MAX // all-raw grid: pass every distinct step through
        }
    } else {
        spec.target
    };
    if spec.is_step_axis {
        if let Some(d) = decide_step(xs, cap) {
            return d;
        }
    }
    decide_general(xs, cap)
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
struct MergeHead {
    x: f64,
    run: usize,
}
impl PartialEq for MergeHead {
    fn eq(&self, other: &Self) -> bool {
        self.x == other.x
    }
}
impl Eq for MergeHead {}
impl Ord for MergeHead {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.x.total_cmp(&self.x) // reversed: smaller x = higher priority
    }
}
impl PartialOrd for MergeHead {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// K-way merge of already-ascending runs, visiting each distinct x once, ascending, until it drains or the visitor breaks. Streamed — the union is never materialized (on a wall-time chart distinct is nearly every point, and the axis plan runs on every build and twice more per delta).
fn merge_distinct(xs: &[&[f64]], mut visit: impl FnMut(f64) -> ControlFlow<()>) {
    use std::collections::BinaryHeap;
    let mut heap: BinaryHeap<MergeHead> = BinaryHeap::with_capacity(xs.len());
    let mut cursor = vec![0usize; xs.len()];
    for (run, r) in xs.iter().enumerate() {
        if let Some(&x) = r.first() {
            heap.push(MergeHead { x, run });
        }
    }
    let mut last = f64::NAN; // never equals a data x (all-finite here)
    while let Some(MergeHead { x, run }) = heap.pop() {
        if last != x {
            if visit(x).is_break() {
                return;
            }
            last = x;
        }
        cursor[run] += 1;
        if let Some(&nx) = xs[run].get(cursor[run]) {
            heap.push(MergeHead { x: nx, run });
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
struct AxisPlan {
    grid: Grid,
    /// Per instantiated cell: does it downsample?
    env: Vec<bool>,
    /// Envelope cells: (min, max) x of their points; (INF, -INF) = unoccupied.
    ext: Vec<(f64, f64)>,
    /// Every raw slot x: ascending and distinct. Log-ladder x <= 0 (below the
    /// grid) leads the list.
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
/// ENV_MIN_POINTS, plus x <= 0 — needs a distinct-merge, small by construction
/// (a linear step grid in this arm is guaranteed, and asserted, to have width
/// ≥ ENV_MIN_POINTS, so its raw region is empty). Non-step axes: one streamed
/// distinct-merge visits every x in order; each cell's mode comes from its own
/// distinct count.
fn plan_axis(xs: &[&[f64]], grid: Grid, spec: GridSpec) -> AxisPlan {
    let mut env = vec![false; grid.cells];
    let mut ext = vec![(f64::INFINITY, f64::NEG_INFINITY); grid.cells];
    let mut raw_xs: Vec<f64> = Vec::new();
    if spec.is_step_axis {
        for (c, e) in env.iter_mut().enumerate() {
            *e = grid.width(c) >= ENV_MIN_POINTS;
        }
        let raw_hi = match grid.kind {
            GridKind::Linear { x_lo, width } => {
                // No raw region — sub-envelope linear step grids can't get here: !log_buckets ones pass through in decide_axis, and the log ladder's linear fallback needs a chart with no positive x, which query.rs never sends a log chart (unplottable x becomes a marker). Asserted because a violation would drop every interior point of the all-raw grid silently.
                debug_assert!(
                    width >= ENV_MIN_POINTS,
                    "all-raw linear step grid: points have no raw region to land in"
                );
                x_lo
            }
            GridKind::Log { m, shift } => pow2(m + 2) - if shift { 1.0 } else { 0.0 },
        };
        let mut raw_slices: Vec<&[f64]> = Vec::with_capacity(xs.len());
        for r in xs {
            let cut = r.partition_point(|&v| v < raw_hi);
            if cut > 0 {
                raw_slices.push(&r[..cut]);
            }
            for &x in &r[cut..] {
                if let Some(c) = grid.index(x) {
                    let e = &mut ext[c];
                    e.0 = e.0.min(x);
                    e.1 = e.1.max(x);
                }
            }
        }
        merge_distinct(&raw_slices, |x| {
            raw_xs.push(x);
            ControlFlow::Continue(())
        });
    } else {
        // Distinct x ascend, so each cell's x arrive consecutively: count them on the fly, buffering the ≤ ENV_MIN_POINTS-1 x a still-raw cell may yet dump to the raw list (the ENV_MIN_POINTS-th distinct x settles it as envelope and drops the buffer; a cell that ends short ships it raw). Below-grid x (log ladder, x <= 0) streams straight to the raw list. ext accumulates for raw cells too — harmless, nothing reads it there.
        let mut cur: Option<Option<usize>> = None;
        let mut cnt = 0usize;
        let mut buf: Vec<f64> = Vec::with_capacity(ENV_MIN_POINTS as usize - 1);
        merge_distinct(xs, |x| {
            let cell = grid.index(x);
            if cur != Some(cell) {
                raw_xs.append(&mut buf); // the previous cell ended short of ENV_MIN_POINTS
                cnt = 0;
                cur = Some(cell);
            }
            match cell {
                None => raw_xs.push(x),
                Some(c) => {
                    cnt += 1;
                    if (cnt as f64) < ENV_MIN_POINTS {
                        buf.push(x);
                    } else {
                        env[c] = true;
                        buf.clear();
                    }
                    let e = &mut ext[c];
                    e.0 = e.0.min(x);
                    e.1 = e.1.max(x);
                }
            }
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

fn mixed_axis(plan: AxisPlan) -> MixedAxis {
    let mut x = Vec::with_capacity(plan.raw_xs.len() + plan.grid.cells);
    let mut slot_of_cell = vec![usize::MAX; plan.grid.cells];
    let mut rp = 0usize;
    while rp < plan.raw_xs.len() && Grid::id(plan.grid.kind, plan.raw_xs[rp]).is_none() {
        x.push(plan.raw_xs[rp]);
        rp += 1;
    }
    for (c, slot) in slot_of_cell.iter_mut().enumerate() {
        if plan.env[c] {
            if plan.ext[c].0.is_finite() {
                *slot = x.len();
                x.push((plan.ext[c].0 + plan.ext[c].1) * 0.5);
            }
        } else {
            while rp < plan.raw_xs.len() && plan.grid.index(plan.raw_xs[rp]) == Some(c) {
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
// Per-sample age relative to the verified held response: OLD = held input with no explicit dirty flag; REACH = held input whose smoothing or interpolation dependency dirties its cell; NEW = input absent from that response. Cache lineage proves membership before query.rs marks dependencies. Later OLD samples may still change after an earlier dirty sample; only the prefix before the first dirty cell is retained.
pub const AGE_OLD: u8 = 0;
pub const AGE_REACH: u8 = 1;
pub const AGE_NEW: u8 = 2;

/// Prove a prefix for numeric columns and logged-y markers: axis and every CONTINUING series (any held sample; all-new series ship complete and constrain nothing). Callers must first flag interpolation dependencies and then apply [`bound_xnan_from_col`] for carried kind-4 markers before using this as a splice point. Zero means answer in full.
///
/// The non-NEW reconstruction is exactly the held input.
///
/// The caller marks smoother support, numerical prefix-sum blocks and preceding interpolation endpoints, and proves the exact [`SmoothingPlan`] unchanged when one exists. A column is dirty when its structure changes or a continuing series has a non-OLD sample there. Sampled audits reconstruct the held response; client hashes verify every splice.
pub fn numeric_delta_from_col(xs: &[&[f64]], age: &[&[u8]], spec: GridSpec) -> usize {
    let Some((g_first, g_last)) = axis_bounds(xs) else {
        return 0;
    };
    let continuing: Vec<bool> = age
        .iter()
        .map(|a| a.iter().any(|&k| k != AGE_NEW))
        .collect();
    let old_xs: Vec<Cow<'_, [f64]>> = xs.iter().zip(age).map(|(x, a)| held_xs(x, a)).collect();
    let old_refs: Vec<&[f64]> = old_xs.iter().map(|c| c.as_ref()).collect();
    let Some((og_first, og_last)) = axis_bounds(&old_refs) else {
        return 0; // nothing held
    };

    let new_dec = decide_axis(xs, g_first, g_last, spec);
    let old_dec = decide_axis(&old_refs, og_first, og_last, spec);
    match (&old_dec, &new_dec) {
        (Decision::Passthrough(old_axis), Decision::Passthrough(_)) => {
            passthrough_from_col(old_axis, xs, age, &continuing)
        }
        (Decision::Downsample, Decision::Downsample) => {
            let new_grid = make_grid(xs, g_first, g_last, spec);
            let old_grid = make_grid(&old_refs, og_first, og_last, spec);
            if new_grid.kind != old_grid.kind || new_grid.base != old_grid.base {
                return 0; // tier or anchor change: every boundary moved
            }
            let new_plan = plan_axis(xs, new_grid, spec);
            let old_plan = plan_axis(&old_refs, old_grid, spec);
            match (old_plan.any_env_occupied(), new_plan.any_env_occupied()) {
                // No envelope bucket stands on either side: both ship passthrough-SHAPED columns over their distinct axes.
                (false, false) => passthrough_from_col(&old_plan.raw_xs, xs, age, &continuing),
                // Envelope emission springing into existence (or vanishing) reshapes every series' families chart-wide — band-less held columns can't continue a banded tail.
                (a, b) if a != b => 0,
                _ => mixed_from_col(&old_plan, &new_plan, xs, age, &continuing),
            }
        }
        // Passthrough <-> grid flip: structurally different axes.
        _ => 0,
    }
}

/// Bound the current and verified held kind-4 marker anchors of a continuing series.
///
/// With verified input history, held rows are retained in the current rows. On absolute log axes min_x is the first current plottable x and max_x is the first verified-held plottable x.
///
/// held records whether the verified-held rows contained a marker with a plottable anchor. The upper bound proves stable placement only when held is true. With no held plottable x, use max_x = INFINITY and held = false.
#[derive(Clone, Copy, Debug)]
pub struct XnanDependency {
    pub min_x: f64,
    pub max_x: f64,
    pub held: bool,
}

/// Restrict a proven numeric prefix for kind-4 nearest-slot placement.
///
/// Every held axis has the leading slots proved by numeric_delta_from_col. A dirty slot can move a marker onto or off the preceding slot. A marker is stable only when both anchors choose the same prefix slot and a fixed right neighbor, or the anchor itself, excludes competition from the suffix. Otherwise the earliest possible affected prefix slot starts the delta.
///
/// This needs no held-chart reconstruction and examines O(log from_col) axis entries per dependency.
pub fn bound_xnan_from_col(axis: &[f64], from_col: usize, deps: &[XnanDependency]) -> usize {
    if from_col == 0 || from_col > axis.len() {
        return 0;
    }
    let prefix = &axis[..from_col];
    let mut bound = from_col;
    for dep in deps {
        if !dep.min_x.is_finite() || dep.max_x.is_nan() || dep.max_x < dep.min_x {
            return 0;
        }
        let first = nearest_axis_slot(prefix, dep.min_x);
        // A complete numeric prefix leaves no competing suffix: retained rows cannot supply an extra held column, and any new plottable sample of a continuing series would already dirty its column.
        let stable = dep.held
            && first == nearest_axis_slot(prefix, dep.max_x)
            && (first + 1 < from_col || dep.max_x <= prefix[first] || from_col == axis.len());
        if !stable {
            bound = bound.min(first);
        }
    }
    bound
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

/// Passthrough arm: the first slot of `old_axis` whose x or content changed — a continuing series' non-OLD sample, or any sample at an x the held axis lacks (an insertion). A new series' samples at already-charted x change neither the axis nor any continuing column.
fn passthrough_from_col(
    old_axis: &[f64],
    xs: &[&[f64]],
    age: &[&[u8]],
    continuing: &[bool],
) -> usize {
    // Any duplicate x flips every series' band presence chart-wide (see shared_passthrough), changing columns before it too — only full is safe. xs ascend per series, so duplicates are adjacent.
    if xs.iter().any(|x| x.windows(2).any(|w| w[0] == w[1])) {
        return 0;
    }
    let mut first_dirty_x = f64::INFINITY;
    for ((x, a), &cont) in xs.iter().zip(age).zip(continuing) {
        for (&v, &k) in x.iter().zip(a.iter()) {
            if k != AGE_OLD
                && v < first_dirty_x
                && (cont || old_axis.binary_search_by(|p| p.total_cmp(&v)).is_err())
            {
                first_dirty_x = v;
            }
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
    continuing: &[bool],
) -> usize {
    let cells = old_plan.grid.cells;
    // UNclamped cell offset from the shared base (the layouts agree on kind and base — checked by the caller): None = below the grid (log ladder, x <= 0); offsets at or past `cells` are appended cells, whose slots all sit past the old prefix. Grid::index would clamp those into range and alias them onto real cells.
    let kind = old_plan.grid.kind;
    let base = old_plan.grid.base;
    let cell_of = move |x: f64| Grid::id(kind, x).map(|id| id - base);

    // Content dirt from continuing series' non-OLD samples: envelope cells
    // flag; raw slots collect their exact x (a NEW sample at an existing raw x
    // is a duplicate/first-sample content change; a REACH sample's smoothed
    // value moves).
    let mut dirty_env = vec![false; cells];
    let mut dirty_raw: Vec<f64> = Vec::new();
    for ((x, a), &cont) in xs.iter().zip(age).zip(continuing) {
        for (&v, &k) in x.iter().zip(a.iter()) {
            let hot = (k == AGE_NEW && cont) || k == AGE_REACH;
            if !hot {
                continue;
            }
            match cell_of(v) {
                Some(off) if (0..cells as i64).contains(&off) => {
                    if new_plan.env[off as usize] {
                        dirty_env[off as usize] = true;
                    } else {
                        dirty_raw.push(v);
                    }
                }
                None => dirty_raw.push(v), // below-grid raw slot
                Some(_) => {}              // appended cell: past the old prefix
            }
        }
    }
    dirty_raw.sort_by(|a, b| a.total_cmp(b));
    let raw_is_dirty = |x: f64| dirty_raw.binary_search_by(|p| p.total_cmp(&x)).is_ok();

    let (old_raw, new_raw) = (&old_plan.raw_xs, &new_plan.raw_xs);
    let (mut op, mut np) = (0usize, 0usize);
    let mut from_col = 0usize;

    // Pairwise walk of one cell's raw slots (`cell` None = the below-grid group that leads the list). Some(col) = divergence at that column.
    let walk_raw = |cell: Option<i64>,
                    op: &mut usize,
                    np: &mut usize,
                    from_col: &mut usize|
     -> Option<usize> {
        loop {
            let o_here = *op < old_raw.len() && cell_of(old_raw[*op]) == cell;
            let n_here = *np < new_raw.len() && cell_of(new_raw[*np]) == cell;
            match (o_here, n_here) {
                (false, false) => return None,
                (true, true) => {
                    if old_raw[*op].to_bits() != new_raw[*np].to_bits()
                        || raw_is_dirty(old_raw[*op])
                    {
                        return Some(*from_col);
                    }
                    *op += 1;
                    *np += 1;
                    *from_col += 1;
                }
                // An inserted slot shifts every later column; a vanished one is impossible while samples only accumulate — refuse it the same way rather than trust it.
                _ => return Some(*from_col),
            }
        }
    };

    if let Some(col) = walk_raw(None, &mut op, &mut np, &mut from_col) {
        return col;
    }
    for (c, &dirty) in dirty_env.iter().enumerate().take(cells) {
        if old_plan.env[c] != new_plan.env[c] {
            return from_col; // mode flip: the cell's slots restructure
        }
        if old_plan.env[c] {
            if old_plan.ext[c].0.to_bits() != new_plan.ext[c].0.to_bits()
                || old_plan.ext[c].1.to_bits() != new_plan.ext[c].1.to_bits()
                || dirty
            {
                return from_col;
            }
            if old_plan.ext[c].0.is_finite() {
                from_col += 1;
            }
        } else if let Some(col) = walk_raw(Some(c as i64), &mut op, &mut np, &mut from_col) {
            return col;
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

    /// Centered moving average, window `w` samples — a stand-in smoother
    /// whose reach is exactly w (the real smoothers' reach is bounded by
    /// the window the caller derives flags from).
    fn smooth(y: &[f64], w: usize) -> Vec<f64> {
        (0..y.len())
            .map(|i| {
                let lo = i.saturating_sub(w);
                let hi = (i + w + 1).min(y.len());
                y[lo..hi].iter().sum::<f64>() / (hi - lo) as f64
            })
            .collect()
    }

    /// The exact response for a set of series — the brute-force oracle.
    fn respond(xs: &[Vec<f64>], ys: &[Vec<f64>], w: usize, spec: GridSpec) -> DenseChart {
        let plot: Vec<Vec<f64>> = ys
            .iter()
            .map(|y| if w > 0 { smooth(y, w) } else { y.clone() })
            .collect();
        let xr: Vec<&[f64]> = xs.iter().map(Vec::as_slice).collect();
        let pr: Vec<&[f64]> = plot.iter().map(Vec::as_slice).collect();
        let rr: Vec<&[f64]> = ys.iter().map(Vec::as_slice).collect();
        let kinds: Vec<Vec<u8>> = xs.iter().map(|x| vec![0u8; x.len()]).collect();
        let kr: Vec<&[u8]> = kinds.iter().map(Vec::as_slice).collect();
        let xnan: Vec<&[f64]> = xs.iter().map(|_| [].as_slice()).collect();
        shared_chart(&xr, &pr, &rr, &kr, &xnan, w > 0, spec)
    }

    /// Age flags for a series holding its first `old_n` samples: the last
    /// `w` held samples are REACH (a new sample's window can touch them).
    fn ages(len: usize, old_n: usize, w: usize) -> Vec<u8> {
        (0..len)
            .map(|i| {
                if i >= old_n {
                    AGE_NEW
                } else if old_n < len && i + w >= old_n {
                    AGE_REACH
                } else {
                    AGE_OLD
                }
            })
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
            .map(|(x, &n)| ages(x.len(), n, w))
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
        let old = respond(&old_xs, &old_ys, w, spec);
        let new = respond(xs, ys, w, spec);
        let from_col = numeric_delta_from_col(&xr, &ar, spec);

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
        let h = held_xs(&x, &[AGE_OLD, AGE_REACH, AGE_NEW, AGE_NEW]);
        assert!(matches!(h, Cow::Borrowed(_)));
        assert_eq!(h.as_ref(), [1.0, 2.0]);
        let h = held_xs(&x, &[AGE_OLD, AGE_NEW, AGE_REACH, AGE_NEW]);
        assert!(matches!(h, Cow::Owned(_)));
        assert_eq!(h.as_ref(), [1.0, 3.0]);
        assert_eq!(held_xs(&x, &[AGE_NEW; 4]).as_ref(), [0f64; 0]);
        assert!(matches!(held_xs(&x, &[AGE_OLD; 4]), Cow::Borrowed(s) if s == x));
    }

    #[test]
    fn kind4_stable_placement_keeps_the_numeric_prefix() {
        let axis = [0.0, 10.0, 20.0, 30.0];
        for (min_x, max_x) in [(5.0, 5.0), (11.0, 14.0), (20.0, 20.0)] {
            assert_eq!(
                bound_xnan_from_col(
                    &axis,
                    3,
                    &[XnanDependency {
                        min_x,
                        max_x,
                        held: true,
                    }],
                ),
                3,
                "stable marker anchor range [{min_x}, {max_x}]"
            );
        }
    }

    #[test]
    fn kind4_dirty_center_or_mode_can_move_marker_to_preceding_slot() {
        // The first two numeric columns are identical. The changing envelope center, or a raw-to-envelope cell flip, can nevertheless move the marker into column 1, which the numeric planner would retain.
        for (old_axis, new_axis, anchor) in [
            (
                vec![0.0, 10.0, 18.0, 40.0],
                vec![0.0, 10.0, 28.0, 40.0],
                18.0,
            ),
            (
                vec![0.0, 10.0, 16.0, 20.0, 24.0, 40.0],
                vec![0.0, 10.0, 20.0, 40.0],
                14.0,
            ),
        ] {
            let mut old = DenseSeries::default();
            let mut new = DenseSeries::default();
            fold_xnan(&mut old, &[anchor], &old_axis);
            fold_xnan(&mut new, &[anchor], &new_axis);
            assert_eq!(old.nan_indices, [2]);
            assert_eq!(new.nan_indices, [1]);
            assert_eq!(
                bound_xnan_from_col(
                    &new_axis,
                    2,
                    &[XnanDependency {
                        min_x: anchor,
                        max_x: anchor,
                        held: true,
                    }],
                ),
                1
            );
        }
    }

    #[test]
    fn kind4_bound_includes_uncertain_presence_and_carried_anchor() {
        let axis = [0.0, 10.0, 20.0, 30.0];
        for (min_x, max_x, held, expected) in [
            (1.0, 19.0, true, 0),
            (10.0, 10.0, false, 1),
            (20.0, f64::INFINITY, false, 2),
        ] {
            assert_eq!(
                bound_xnan_from_col(&axis, axis.len(), &[XnanDependency { min_x, max_x, held }]),
                expected
            );
        }
    }

    #[test]
    fn kind4_bound_covers_intermediate_held_axes_and_anchors() {
        let current_axis = [0.0, 10.0, 20.0, 30.0, 40.0];
        let prefix_markers = |s: &DenseSeries, bound: usize| {
            s.nan_indices
                .iter()
                .copied()
                .filter(|&i| (i as usize) < bound)
                .collect::<Vec<_>>()
        };
        // The first three columns are guaranteed; any held snapshot may have a different tail and a carried anchor anywhere within the bounds.
        for min_x in (0..=40).step_by(5) {
            for max_x in (min_x..=45).step_by(5) {
                for held in [false, true] {
                    let bound = bound_xnan_from_col(
                        &current_axis,
                        3,
                        &[XnanDependency {
                            min_x: min_x as f64,
                            max_x: max_x as f64,
                            held,
                        }],
                    );
                    let mut current = DenseSeries::default();
                    fold_xnan(&mut current, &[min_x as f64], &current_axis);
                    let tails: &[&[f64]] = &[
                        &[],
                        &[21.0],
                        &[25.0],
                        &[30.0],
                        &[40.0],
                        &[50.0],
                        &[21.0, 25.0, 50.0],
                    ];
                    for tail in tails {
                        // A held axis can end at the fixed prefix: no right neighbor from the current suffix may be assumed to have existed then.
                        let old_axis: Vec<f64> = current_axis[..3]
                            .iter()
                            .chain(tail.iter())
                            .copied()
                            .collect();
                        for old_x in min_x..=max_x {
                            let mut old = DenseSeries::default();
                            fold_xnan(&mut old, &[old_x as f64], &old_axis);
                            assert_eq!(
                                prefix_markers(&old, bound),
                                prefix_markers(&current, bound),
                                "anchors=[{min_x}, {max_x}] old_x={old_x} tail={tail:?} held={held}"
                            );
                        }
                        if !held {
                            assert!(prefix_markers(&current, bound).is_empty());
                        }
                    }
                }
            }
        }
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
        assert_eq!(from_col, respond(&[xa], &[ya], 6, spec(300)).x_values.len());
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
            .x_values
            .len()
        );
        // Passthrough -> downsample flip: answer full. (2100 steps at target
        // 500 give width 8 ≥ 4, a real grid; 400 held steps passed through.)
        let (xe, ye) = series(2100, 15);
        assert_eq!(check(&[xe], &[ye], &[400], 0, spec(500)), 0);
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
        // Smoothed variant: the REACH window dirties trailing buckets only.
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
        // Time-like axis, deliberately clumped: cluster A (5 xs in one cell)
        // downsamples; cluster B holds 3 distinct xs (raw) until the append
        // lands its 4th — that bucket restructures, everything before it holds.
        // check()'s oracle verifies the claimed prefix bit-for-bit either way.
        let xs: Vec<f64> = vec![
            // cluster far left, dense: envelope material
            1000.0, 1001.0, 1002.0, 1003.0, 1004.0, 1005.0, 1006.0, 1007.0,
            // mid cluster: 3 distinct — raw until the 4th arrives
            5000.0, 5001.0, 5002.0,
            // right tail so the span (and grid) stays put, plus the append
            9000.0, 9001.0, 9002.0, 9003.0, 9004.0, 9005.0, 9006.0, 9007.0,
            9008.0,
            // the appended sample: 4th distinct x of the mid cluster? No —
            // appends must ascend; instead the 4th mid-cluster x arrives as a
            // NEW SERIES below.
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
        let old_len = respond(&[xs], &[ys], 0, spec2).x_values.len();
        assert!(
            from_col > 0 && from_col < old_len,
            "from_col={from_col} of {old_len}"
        );
    }
}

/// Bucket every run onto one shared slot axis. `xs`/`plot`/`kinds` are parallel
/// per run (ascending x); `raw` is the pre-smoothing values, used per run only
/// when `is_smoothed` (else pass empty slices and `plot` is the value source).
/// `xnan` is the per-run "unplottable x" positions (kind-4 markers: non-finite
/// custom-x, or negative x on a log axis; empty otherwise). `spec` is the
/// request's axis geometry — labels/run_ids on the result are left empty for
/// the caller to fill.
pub fn shared_chart(
    xs: &[&[f64]],
    plot: &[&[f64]],
    raw: &[&[f64]],
    kinds: &[&[u8]],
    xnan: &[&[f64]],
    is_smoothed: bool,
    spec: GridSpec,
) -> DenseChart {
    let n = xs.len();
    let Some((g_first, g_last)) = axis_bounds(xs) else {
        return DenseChart {
            series: (0..n).map(|_| DenseSeries::default()).collect(),
            ..Default::default()
        };
    };

    // One chart-wide decision, the same for every run: bucket on a grid, or
    // pass every distinct x through. The distinct axis is built ONLY when we
    // pass through — never to feed the decision (see decide_axis).
    match decide_axis(xs, g_first, g_last, spec) {
        Decision::Passthrough(axis) => {
            shared_passthrough(axis, xs, plot, raw, kinds, xnan, is_smoothed)
        }
        Decision::Downsample => {
            let grid = make_grid(xs, g_first, g_last, spec);
            let plan = plan_axis(xs, grid, spec);
            if !plan.any_env_occupied() {
                // Every cell came out raw (a log ladder over sparse-enough data): the distinct axis IS the chart, and the emission shape is passthrough's — band only under duplicate-x, exactly what the delta planner mirrors.
                return shared_passthrough(plan.raw_xs, xs, plot, raw, kinds, xnan, is_smoothed);
            }
            let ma = mixed_axis(plan);
            let mut series = Vec::with_capacity(n);
            for i in 0..n {
                let mut a = bucket_run(
                    xs[i],
                    plot[i],
                    if is_smoothed { raw[i] } else { plot[i] },
                    kinds[i],
                    is_smoothed,
                    &ma,
                );
                fold_xnan(&mut a, xnan[i], &ma.x);
                series.push(a);
            }
            DenseChart {
                x_values: ma.x,
                xr_min: ma.xr_min,
                xr_max: ma.xr_max,
                series,
            }
        }
    }
}

/// Merge "unplottable x" annotations (kind 4: non-finite custom-x, negative x
/// on a log axis) into a run's markers, each pinned to the nearest axis slot. A
/// y-kind already at that slot wins (it says strictly more). Also the single
/// place markers are sorted and de-duplicated by slot. No work when `xnan` is
/// empty and the run produced no markers.
fn fold_xnan(a: &mut DenseSeries, xnan: &[f64], axis: &[f64]) {
    if a.nan_indices.is_empty() && xnan.is_empty() {
        return;
    }
    let mut pairs: Vec<(u32, u32)> = a
        .nan_indices
        .iter()
        .copied()
        .zip(a.nan_kinds.iter().copied())
        .collect();
    if !axis.is_empty() {
        for &x in xnan {
            let slot = nearest_axis_slot(axis, x);
            pairs.push((slot as u32, 4));
        }
    }
    pairs.sort_unstable();
    pairs.dedup_by_key(|&mut (p, _)| p);
    (a.nan_indices, a.nan_kinds) = pairs.into_iter().unzip();
}

/// Nearest slot on a nonempty ascending axis. Ties go left, identically for marker emission and the delta dependency proof.
fn nearest_axis_slot(axis: &[f64], x: f64) -> usize {
    let p = axis.partition_point(|&v| v < x);
    if p == 0 {
        0
    } else if p >= axis.len() {
        axis.len() - 1
    } else if (x - axis[p - 1]).abs() <= (axis[p] - x).abs() {
        p - 1
    } else {
        p
    }
}

/// No-downsample axis = the union of distinct x. Each run drops its values onto
/// the slots it actually logged; everything else is a gap.
///
/// Duplicate x: a non-injective custom-x metric can log two y at the exact same x, which a shared distinct-x axis can't keep as two slots. Rather than drop a sample, the second+ collapse onto the slot and the spread surfaces as a y band. Any collision then makes EVERY series ship a dense envelope (min == max at single-sample slots) — the envelope contract is chart-wide, and a run without one would blank under the client's envelope rendering. A logged non-finite duplicate at the same x stays a plain marker; non-finite-wins isn't applied in this (rare) collision.
fn shared_passthrough(
    axis: Vec<f64>,
    xs: &[&[f64]],
    plot: &[&[f64]],
    raw: &[&[f64]],
    kinds: &[&[u8]],
    xnan: &[&[f64]],
    is_smoothed: bool,
) -> DenseChart {
    let nax = axis.len();
    let mut series = Vec::with_capacity(xs.len());
    let mut bands: Vec<(Vec<f64>, Vec<f64>)> = Vec::with_capacity(xs.len());
    let mut any_dup = false;
    for i in 0..xs.len() {
        let mut s = DenseSeries {
            values: vec![f64::NAN; nax],
            ..Default::default()
        };
        if is_smoothed {
            s.raw_values = vec![f64::NAN; nax];
        }
        let mut y_min = vec![f64::NAN; nax];
        let mut y_max = vec![f64::NAN; nax];
        let mut p = 0usize;
        for k in 0..xs[i].len() {
            let x = xs[i][k];
            while p < nax && axis[p] < x {
                p += 1;
            }
            if p >= nax {
                break;
            }
            if kinds[i][k] == 0 {
                let v = plot[i][k];
                // The band is the RAW spread (as in the downsample path), not
                // the smoothed line's; unsmoothed, raw IS the plotted value.
                let band = if is_smoothed { raw[i][k] } else { v };
                if s.values[p].is_finite() {
                    // Duplicate x at this slot — widen the band to the raw spread.
                    let prev = if is_smoothed {
                        s.raw_values[p]
                    } else {
                        s.values[p]
                    };
                    let lo = if y_min[p].is_finite() { y_min[p] } else { prev };
                    let hi = if y_max[p].is_finite() { y_max[p] } else { prev };
                    y_min[p] = lo.min(band);
                    y_max[p] = hi.max(band);
                    any_dup = true;
                }
                s.values[p] = v;
                if is_smoothed {
                    s.raw_values[p] = raw[i][k];
                }
            } else {
                s.nan_indices.push(p as u32);
                s.nan_kinds.push(kinds[i][k] as u32);
            }
        }
        fold_xnan(&mut s, xnan[i], &axis);
        bands.push((y_min, y_max));
        series.push(s);
    }
    // Any collision anywhere: every series ships its band, densified — a slot's lone sample is its own min and max. Runs with no finite sample keep an empty envelope (all-NaN columns are never shipped).
    if any_dup {
        for (s, (mut y_min, mut y_max)) in series.iter_mut().zip(bands) {
            let mut any_finite = false;
            for p in 0..nax {
                if y_min[p].is_finite() {
                    any_finite = true;
                    continue;
                }
                let v = if is_smoothed {
                    s.raw_values[p]
                } else {
                    s.values[p]
                };
                if v.is_finite() {
                    y_min[p] = v;
                    y_max[p] = v;
                    any_finite = true;
                }
            }
            if any_finite {
                s.min_values = y_min;
                s.max_values = y_max;
            }
        }
    }
    // A raw column with no finite entry normalizes to ABSENT, like the envelope: the wire cannot express an all-gap family for a value-less series, so the model must not either — the client's inflation would otherwise hash-diverge on every delta touching an all-marker series.
    for s in series.iter_mut() {
        if !s.raw_values.iter().any(|v| v.is_finite()) {
            s.raw_values.clear();
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
/// itself).
fn bucket_run(
    rx: &[f64],
    rp: &[f64],
    env_src: &[f64],
    rk: &[u8],
    is_smoothed: bool,
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
    // Per-cell aggregates over this run's envelope-cell points — finite for mean/envelope, first non-finite kind for markers (non-finite samples never enter the envelope; the marker circle carries them). Every aggregate is LOCAL to its cell. A smoothed slot value has the additional run-wide interpolation dependency handled by query.rs age flags.
    let mut cnt = vec![0u32; grid.cells];
    let mut sum_y = vec![0.0f64; grid.cells];
    let mut ymin = vec![f64::INFINITY; grid.cells];
    let mut ymax = vec![f64::NEG_INFINITY; grid.cells];
    let mut nfkind = vec![0u8; grid.cells];

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
        let env_cell = grid.index(x).filter(|&c| ma.plan.env[c]);
        if let Some(c) = env_cell {
            if rk[j] != 0 {
                if nfkind[c] == 0 {
                    nfkind[c] = rk[j];
                }
                continue;
            }
            let v = env_src[j];
            cnt[c] += 1;
            sum_y[c] += v;
            ymin[c] = ymin[c].min(v);
            ymax[c] = ymax[c].max(v);
        } else {
            while p < nax && ma.x[p] < x {
                p += 1;
            }
            debug_assert!(p < nax && ma.x[p] == x, "raw point must own a slot");
            if p >= nax {
                break;
            }
            if rk[j] != 0 {
                markers.push((p as u32, rk[j] as u32));
            } else {
                out.values[p] = rp[j];
                let b = env_src[j];
                y_min[p] = if y_min[p].is_finite() {
                    y_min[p].min(b)
                } else {
                    b
                };
                y_max[p] = if y_max[p].is_finite() {
                    y_max[p].max(b)
                } else {
                    b
                };
                any_range = true;
            }
        }
    }

    // Envelope slots, ascending — when smoothing, carry a lerp pointer over the
    // run's finite smoothed points for the slot-x value. Edge buckets can center
    // beyond a run's own extent, so the nearest segment extrapolates there.
    // Unsmoothed runs never lerp, so the finite-point list isn't built.
    let fin: Vec<(f64, f64)> = if is_smoothed {
        (0..m)
            .filter(|&j| rp[j].is_finite())
            .map(|j| (rx[j], rp[j]))
            .collect()
    } else {
        Vec::new()
    };
    let mut lptr = 0usize;
    for c in 0..grid.cells {
        let slot = ma.slot_of_cell[c];
        if slot == usize::MAX {
            continue;
        }
        // A logged non-finite sample wins the slot: value gaps, the bucket's
        // finite evidence still rides as a range (rule: marker keeps its band).
        if nfkind[c] != 0 {
            markers.push((slot as u32, nfkind[c] as u32));
            if cnt[c] >= 1 {
                y_min[slot] = ymin[c];
                y_max[slot] = ymax[c];
                any_range = true;
            }
            continue;
        }
        if cnt[c] == 0 {
            continue; // run has no data in this bucket — gap
        }
        if is_smoothed {
            // The smoothed line is the smoothed value interpolated at the slot x.
            out.values[slot] = lerp_at(&fin, &mut lptr, ma.x[slot]);
        } else {
            // Bucket mean — a tooltip/hover/y-ranging summary, never stroked; the envelope is the rendering.
            out.values[slot] = sum_y[c] / cnt[c] as f64;
        }
        // Dense envelope: every occupied bucket ships min/max, min == max behind a single sample.
        y_min[slot] = ymin[c];
        y_max[slot] = ymax[c];
        any_range = true;
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
/// the preceding plottable custom-x (or the first for a leading gap) or the
/// first plottable absolute-log x — is assigned in query.rs; it never describes
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

/// Smooth a single run's own sample sequence.
///
/// Call with a run's consecutive samples — NOT a union-aligned vector. That
/// makes the window width mean "N of this run's samples" regardless of
/// which other runs share the chart, and makes it impossible for smoothing
/// to extend a line past the run's first/last logged step.
///
/// `xs` are the run's x positions (steps or timestamps), ascending, same length as `y`. A uniform grid — contiguous step logging, the overwhelmingly common case — takes the fast index-space smoothers (sample distance IS x distance there, checked exactly since step grids are integer-valued). Irregular grids take the O(n·window) x-aware paths with the window scale quoted in median sample intervals, so the same UI setting spans the same x range either way and a logging gap narrows a window instead of silently stretching it. `step_sized_x` declares that one x unit is one step: EMA's τ stays in steps even for every-k-step loggers; on time axes it is in median sample intervals instead.
///
/// Non-finite values (logged NaN/Inf) get the least-effort policy: they
/// are excluded from every window (an Inf would poison sums), their output
/// positions stay NaN, and nothing else handles them — making them visible
/// is the NaN markers' job.
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
    let plan = smoothing_plan(xs, algo, step_sized_x);
    smooth_run_with_plan(xs, y, algo, window, time_constant, poly_order, plan)
}

/// Run a smoother with its already-derived semantic plan. Production preparation calls this after deriving the live plan once; audit reconstruction supplies the proven current plan and derives nothing from its conservative old snapshot.
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
    let sanitized: Vec<f64> = y
        .iter()
        .map(|v| if v.is_finite() { *v } else { f64::NAN })
        .collect();
    let mut out = match algo {
        Algorithm::None => sanitized.clone(),
        Algorithm::Ema => ema_polyfit(
            xs,
            &sanitized,
            time_constant,
            plan.causal_dx_ref(),
            poly_order,
        ),
        Algorithm::Triangular => {
            let dx_ref = plan.causal_dx_ref();
            triangular_polyfit(xs, &sanitized, dx_ref, poly_order)
        }
        Algorithm::SavitzkyGolay => {
            let w = window as usize;
            match plan {
                SmoothingPlan::Uniform => savitzky_golay(&sanitized, w, poly_order),
                SmoothingPlan::Median(bits) => {
                    let (_, h_s) = savgol_geometry(w);
                    savgol_x(
                        xs,
                        &sanitized,
                        h_s * f64::from_bits(bits),
                        poly_order.min(2) as usize,
                    )
                }
                SmoothingPlan::NoState => panic!("Savitzky-Golay smoother requires a spacing plan"),
            }
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
    use super::{median_dx, savgol_dependency_start, savgol_geometry, smooth_run};
    use crate::proto::smoothing_config::Algorithm;

    const ALGOS: [Algorithm; 3] = [
        Algorithm::Ema,
        Algorithm::SavitzkyGolay,
        Algorithm::Triangular,
    ];

    fn step_xs(n: usize) -> Vec<f64> {
        (0..n).map(|i| i as f64).collect()
    }

    /// Compares f64 bits, which the bound keeps identical (so wire values match too); block placement moves outputs too little to show reliably after f32 rounding. Inputs use full f64 mantissas, since f32-valued inputs make small windows' prefix sums exact and hide block placement; the lengths leave both full and partial final blocks.
    #[test]
    fn savgol_block_bound_preserves_the_bitwise_prefix() {
        let mut changes_outside_support = 0usize;
        for window in [1usize, 3, 5, 10, 20, 50, 100, 130] {
            let mut retained = 0usize;
            let (half, _) = savgol_geometry(window);
            let block_size = 2 * half;
            let lengths = if window >= 100 {
                vec![
                    2 * half + block_size - 1,
                    2 * half + block_size,
                    2 * half + block_size + 1,
                    4 * block_size + 7,
                ]
            } else {
                vec![60, 99, 181, 2 * half + block_size]
            };
            for n in lengths {
                for order in 0..=2 {
                    for seed in [0u64, 1, 9, 17, 42, 99, 233] {
                        for hole in [false, true] {
                            let mut state = seed + 1;
                            let y: Vec<f64> = (0..n)
                                .map(|i| {
                                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                                    if hole && i == n / 3 {
                                        f64::NAN
                                    } else {
                                        (((state >> 32) % 20_001) as i64 - 10_000) as f64 / 97.0
                                    }
                                })
                                .collect();
                            let old = smooth_run(
                                &step_xs(n),
                                &y,
                                Algorithm::SavitzkyGolay,
                                window as u32,
                                0.5,
                                order,
                                true,
                            );
                            // Prepending shifts block placement; appending one or two rows exercises both parities of the final block's origin.
                            let growths = if window >= 100 {
                                vec![0, 1, 2, 3, block_size + 1]
                            } else {
                                vec![0, 1, 2]
                            };
                            for growth in growths {
                                let (new_y, new_x, first_new, offset) = if growth == 0 {
                                    (
                                        std::iter::once(3.0).chain(y.iter().copied()).collect(),
                                        (-1..n as i64).map(|x| x as f64).collect(),
                                        0,
                                        1,
                                    )
                                } else {
                                    (
                                        y.iter()
                                            .copied()
                                            .chain((0..growth).map(|i| i as f64 + 3.0))
                                            .collect::<Vec<_>>(),
                                        step_xs(n + growth),
                                        n,
                                        0,
                                    )
                                };
                                let new = smooth_run(
                                    &new_x,
                                    &new_y,
                                    Algorithm::SavitzkyGolay,
                                    window as u32,
                                    0.5,
                                    order,
                                    true,
                                );
                                let from = savgol_dependency_start(window, first_new);
                                if growth == 0 {
                                    assert_eq!(from, 0, "a prepend shifts every block");
                                } else {
                                    assert!(from > 0 && from <= old.len(), "window={window}, n={n}: append fixture must retain a real prefix");
                                }
                                for (i, value) in old.iter().enumerate() {
                                    let a = value.to_bits();
                                    let b = new[i + offset].to_bits();
                                    if i + offset < from {
                                        assert_eq!(a, b, "window={window}, n={n}, order={order}, seed={seed}, growth={growth}, index={i}");
                                        retained += 1;
                                    }
                                    let outside_support = if growth == 0 {
                                        i + 1 > half
                                    } else {
                                        i + half < n
                                    };
                                    changes_outside_support +=
                                        usize::from(outside_support && a != b);
                                }
                            }
                        }
                    }
                }
            }
            assert!(
                retained > 1_000,
                "window={window}: each block geometry must retain real kernel outputs"
            );
        }
        assert!(
            changes_outside_support > 0,
            "fixtures must demonstrate why mathematical support alone is insufficient"
        );
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
    /// bit-for-bit unchanged. This is what lets the delta planner treat its
    /// reach as 0.
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

    /// Repeated timestamps (fast logging, importer rows without _timestamp) keep the unit at the median gap between distinct timestamps, so every smoother stays finite and smooths across them (AI-1508).
    #[test]
    fn repeated_timestamps_keep_the_smoothing_unit() {
        // Gaps 0, 0, 1, 5, 9: the positive gaps' median is 5; counting the zeros would give 1.
        assert_eq!(median_dx(&[0.0, 0.0, 0.0, 1.0, 6.0, 15.0]), 5.0);
        let xs = [0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2.0];
        let y = [1.0, 2.0, 3.0, 4.0, 5.0, 4.0, -4.0];
        for algo in ALGOS {
            let out = smooth_run(&xs, &y, algo, 5, 0.5, 0, false);
            assert!(out.iter().all(|v| v.is_finite()), "{algo:?}: {out:?}");
            assert_ne!(out[6], y[6], "{algo:?} returned the raw last sample");
        }
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

    /// Call helper: builds slice views; `raw` mirrors `plot` when smoothed.
    fn run_spec(
        xs: &[Vec<f64>],
        plot: &[Vec<f64>],
        kinds: &[Vec<u8>],
        smoothed: bool,
        spec: GridSpec,
    ) -> DenseChart {
        let raw: Vec<Vec<f64>> = if smoothed {
            plot.to_vec()
        } else {
            xs.iter().map(|_| Vec::new()).collect()
        };
        let empty: Vec<Vec<f64>> = xs.iter().map(|_| Vec::new()).collect();
        let xs_s: Vec<&[f64]> = xs.iter().map(|v| v.as_slice()).collect();
        let plot_s: Vec<&[f64]> = plot.iter().map(|v| v.as_slice()).collect();
        let raw_s: Vec<&[f64]> = raw.iter().map(|v| v.as_slice()).collect();
        let kinds_s: Vec<&[u8]> = kinds.iter().map(|v| v.as_slice()).collect();
        let xnan_s: Vec<&[f64]> = empty.iter().map(|v| v.as_slice()).collect();
        shared_chart(&xs_s, &plot_s, &raw_s, &kinds_s, &xnan_s, smoothed, spec)
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

    /// Smoothed value rides the bucket center (interpolated); the band comes
    /// from the raw points in the bucket.
    #[test]
    fn smoothed_value_is_at_center_band_from_raw() {
        let xs = vec![vec![0.0, 1.0, 2.0, 3.0]];
        let plot = vec![vec![10.0, 20.0, 30.0, 40.0]];
        let kinds = vec![vec![0u8; 4]];
        let sc = run(&xs, &plot, &kinds, true, 1);
        assert_eq!(sc.x_values, vec![1.5]);
        let s = &sc.series[0];
        assert_eq!(s.values[0], 25.0, "smoothed line interpolated at x=1.5");
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

    /// A mid-chart run's smoothed value at its first/last bucket is the value
    /// interpolated at the bucket center (rule 2) — NOT the run's endpoint
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
        // Smoothed line: interpolated at center 3.5, not B's endpoint value.
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
        let xs = vec![vec![0.0, 1.0, 1.0, 2.0], vec![0.0, 2.0]]; // two samples at x = 1
        let plot = vec![vec![10.0, 20.0, 30.0, 40.0], vec![7.0, 8.0]];
        let kinds = vec![vec![0u8; 4], vec![0u8; 2]];
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

    /// Custom-x sample whose X was logged non-finite becomes a kind-4 marker
    /// at the nearest slot, without evicting that slot's value.
    #[test]
    fn custom_x_nonfinite_x_becomes_kind4_marker() {
        let xs = [vec![0.0, 1.0, 2.0, 3.0]];
        let plot = [vec![10.0, 20.0, 30.0, 40.0]];
        let raw: [Vec<f64>; 1] = [vec![]];
        let kinds = [vec![0u8; 4]];
        let xnan = [vec![2.0f64]];
        let xs_s: Vec<&[f64]> = xs.iter().map(|v| v.as_slice()).collect();
        let plot_s: Vec<&[f64]> = plot.iter().map(|v| v.as_slice()).collect();
        let raw_s: Vec<&[f64]> = raw.iter().map(|v| v.as_slice()).collect();
        let kinds_s: Vec<&[u8]> = kinds.iter().map(|v| v.as_slice()).collect();
        let xnan_s: Vec<&[f64]> = xnan.iter().map(|v| v.as_slice()).collect();
        let sc = shared_chart(
            &xs_s,
            &plot_s,
            &raw_s,
            &kinds_s,
            &xnan_s,
            false,
            spec_of(100, false),
        );
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
            (vec![vec![5.0, 5.0, 5.0]], true),                   // single x position
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
        let id = |x: f64| Grid::id(kind, x).unwrap();
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

    /// Same defense-in-depth on the DOWNSAMPLE arm: nonpositive interior x on
    /// a custom-x log grid sit below the ladder and must lead the axis as
    /// exact raw slots — never bucketed, never dropped — while the positive
    /// range buckets normally around them.
    #[test]
    fn log_below_grid_interior_x_leads_as_raw_slots() {
        // -5, -4, -3 are all below-grid raw slots.
        // 100..=119 spans one octave: at target 8 the ladder picks m = 4
        // (width-4 cells), so every four-point cell is an envelope, including
        // the final [116,120) cell.
        let mut x = vec![-5.0, -4.0, -3.0];
        x.extend((100..=119).map(|i| i as f64));
        let ys: Vec<f64> = x.iter().map(|v| v * 2.0).collect();
        let kinds = vec![vec![0u8; x.len()]];
        let spec = GridSpec {
            target: 8,
            is_step_axis: false,
            log_buckets: true,
            shift_one: false,
        };
        let sc = run_spec(&[x], &[ys], &kinds, false, spec);
        assert_eq!(
            sc.x_values,
            vec![-5.0, -4.0, -3.0, 101.5, 105.5, 109.5, 113.5, 117.5]
        );
        let s = &sc.series[0];
        assert_eq!(
            s.values[1], -8.0,
            "below-grid raw slot keeps the exact sample"
        );
        assert_eq!(
            (s.min_values[1], s.max_values[1]),
            (-8.0, -8.0),
            "band pinches to the line there"
        );
        assert_eq!(
            (sc.xr_min[3], sc.xr_max[3]),
            (100.0, 103.0),
            "first envelope bucket extent"
        );
        assert_eq!(
            (*sc.xr_min.last().unwrap(), *sc.xr_max.last().unwrap()),
            (116.0, 119.0),
            "last cell follows the same envelope rule"
        );
        assert!(sc.xr_min[1].is_nan(), "raw slots carry no x extent");
    }

    /// Negative x arrives pre-filtered into the xnan channel on log charts
    /// (query.rs) and becomes a kind-4 exceptional marker — but if one ever
    /// reaches the grid, it must land raw, never bucketed or dropped.
    #[test]
    fn log_stray_nonpositive_x_stays_raw() {
        let xs = vec![vec![-5.0, 0.0, 1.0, 2.0, 3.0]];
        let ys = vec![vec![1.0, 2.0, 3.0, 4.0, 5.0]];
        let kinds = vec![vec![0u8; 5]];
        // Custom-x log (no shift): -5 and 0 are below the ladder.
        let spec = GridSpec {
            target: 100,
            is_step_axis: false,
            log_buckets: true,
            shift_one: false,
        };
        let sc = run_spec(&xs, &ys, &kinds, false, spec);
        assert_eq!(
            sc.x_values,
            vec![-5.0, 0.0, 1.0, 2.0, 3.0],
            "distinct ≤ target: passthrough keeps them all"
        );
    }
}
