//! The dense chart model, its wire codec, and the delta slice/splice — shared by server and frontend.
//!
//! Lives next to kymo.proto because it IS wire contract: both crates include this file verbatim (`#[path]` module) and must agree across independent deploys. The MODEL ([`DenseChart`]) is what both sides compute over — axis-length columns, NaN = no data in that slot — and what the result hashes pin. The WIRE (proto ChartSeries/ChartResponse) carries no filler: occupancy ships as ranges of dense column indices with strictly finite f32 values ([`emit_series`]); the client re-inflates to the model at receipt ([`inflate_series`]/[`inflate_chart`]). The emit/inflate pair lives here so it cannot drift — each side only moves fields into/out of its generated types. Deciding what to ship is server-side frontier reconstruction (ChartCacheState, query.rs); this module only defines what delta columns mean: a continuation carries positions [from_col..) with markers and segment starts kept absolute ([`slice_series`]); the client keeps its first from_col columns and appends ([`splice_series`]).

/// Private frontier-map wire ABI for exact semantic smoothing state. Version 2 deliberately supersedes the initial v1 two-word bandwidth tuple so any cached v1 response fails closed to a full answer.
pub const SMOOTHING_STATE_VERSION_KEY: &str = "\0kymo:smoothing-version";
pub const SMOOTHING_STATE_VERSION: i64 = 2;
pub const SMOOTHING_STATE_SERIES_KEY: &str = "\0kymo:smoothing-series";

/// One run's columns, aligned to the chart's shared axis (same length, NaN where the run has no data in a slot).
/// `values`: the exact sample at raw slots, the bucket mean at envelope slots (never stroked unsmoothed), or the smoothed curve. `raw_values`: pre-smoothing samples, smoothed passthrough charts only. `min_values`/`max_values`: the envelope — DENSE over the run's occupied slots on any banded chart (min == max behind a single sample, so the band pinches to the line through raw slots), absent entirely on band-less charts and for runs with no finite sample. `nan_indices`/`nan_kinds`: slots where the run logged a non-finite value (1 NaN, 2 +inf, 3 -inf, 4 unplottable x). `xnan_count`: how many samples sit behind the kind-4 markers (tooltip "×N"); whole-series display metadata like `label` — ships complete, adopted on splice, unhashed.
#[derive(Default, Debug, Clone)]
pub struct DenseSeries {
    pub label: String,
    pub run_id: String,
    pub values: Vec<f64>,
    pub raw_values: Vec<f64>,
    pub min_values: Vec<f64>,
    pub max_values: Vec<f64>,
    pub nan_indices: Vec<u32>,
    pub nan_kinds: Vec<u32>,
    pub xnan_count: u32,
}

/// A whole chart in model form: THE shared x axis (ascending, unique), the chart-level x extent of each slot's bucket (`xr_min`/`xr_max`, axis-length, finite only where a bucket aggregated a real x spread — the union across runs, the only form clients ever used), and one [`DenseSeries`] per run.
#[derive(Default, Debug, Clone)]
pub struct DenseChart {
    pub x_values: Vec<f64>,
    pub xr_min: Vec<f64>,
    pub xr_max: Vec<f64>,
    pub series: Vec<DenseSeries>,
}

/// Presentation rounding: y ships as f32 (metrics ingest as f32; ~7 digits is the meaningful precision), so the server rounds every y column through this BEFORE hashing/slicing — the model both sides hash is then bit-identical to what the wire reconstructs. Idempotent; the clamp keeps an overshooting smoother from producing an inf the strictly-finite wire cannot carry.
pub fn round_y(v: f64) -> f64 {
    (v.clamp(-f64::from(f32::MAX), f64::from(f32::MAX)) as f32) as f64
}

/// Occupancy of a dense column as (start, len) runs of finite entries — "ranges of dense column indices". NaN slots are simply not listed: the wire carries no filler.
pub fn segment_spans(dense: &[f64]) -> (Vec<u32>, Vec<u32>) {
    let mut starts = Vec::new();
    let mut lens = Vec::new();
    let mut run_open = false;
    for (i, &v) in dense.iter().enumerate() {
        if v.is_finite() {
            if !run_open {
                starts.push(i as u32);
                lens.push(0u32);
                run_open = true;
            }
            *lens.last_mut().unwrap() += 1;
        } else {
            run_open = false;
        }
    }
    (starts, lens)
}

/// The finite entries of a dense column, in order, as wire f32 — parallel to [`segment_spans`]. The caller guarantees [`round_y`]-rounded input, so the cast is exact.
pub fn finite_f32(dense: &[f64]) -> Vec<f32> {
    dense
        .iter()
        .filter(|v| v.is_finite())
        .map(|&v| v as f32)
        .collect()
}

/// Occupancy-compress a dense column: [`segment_spans`] + [`finite_f32`] in one call.
pub fn compress_segments(dense: &[f64]) -> (Vec<u32>, Vec<u32>, Vec<f32>) {
    let (starts, lens) = segment_spans(dense);
    (starts, lens, finite_f32(dense))
}

/// Inflate segments back to a dense column of length `n`, NaN elsewhere. `offset` shifts the absolute starts (a delta tail's columns are absolute; the tail vector is not) — any out-of-range or non-parallel shape is a protocol violation and returns None so the caller refuses instead of rendering a misaligned chart. Generic over the wire float width (f32 series values, f64 x extents).
pub fn expand_segments<T: Into<f64> + Copy>(
    starts: &[u32],
    lens: &[u32],
    vals: &[T],
    n: usize,
    offset: usize,
) -> Option<Vec<f64>> {
    let mut out = vec![f64::NAN; n];
    let mut k = 0usize;
    if starts.len() != lens.len() {
        return None;
    }
    let mut prev_end = 0usize;
    for (&s, &l) in starts.iter().zip(lens) {
        let s = (s as usize).checked_sub(offset)?;
        if s < prev_end {
            return None; // overlapping/unordered segments
        }
        let (l, end) = (l as usize, s.checked_add(l as usize)?);
        if end > n || k + l > vals.len() {
            return None;
        }
        for (slot, &v) in out[s..end].iter_mut().zip(&vals[k..k + l]) {
            *slot = v.into();
        }
        k += l;
        prev_end = end;
    }
    if k != vals.len() {
        return None;
    }
    Some(out)
}

/// Borrowed view of one wire series — each side adapts its generated ChartSeries (structurally identical prost types this module can't name).
pub struct WireSeries<'a> {
    pub label: &'a str,
    pub run_id: &'a str,
    pub seg_starts: &'a [u32],
    pub seg_lens: &'a [u32],
    pub values: &'a [f32],
    pub raw_values: &'a [f32],
    pub band_seg_starts: &'a [u32],
    pub band_seg_lens: &'a [u32],
    pub band_min: &'a [f32],
    pub band_max: &'a [f32],
    pub nan_indices: &'a [u32],
    pub nan_kinds: &'a [u32],
    pub xnan_count: u32,
}

/// Inflate one wire series into dense model columns spanning `n` slots at `offset` (0/axis-length for a full response or a delta's COMPLETE series; from_col/tail-length for a continuation — markers stay absolute either way, the splice expects that). `env_dense`/`raw_dense` say whether those families exist for this series — an all-gap tail carries no entries, so the wire alone cannot answer; the caller derives them from the chart's `banded` bit and, for continuations, the held series. None = protocol violation; refuse, never render a misaligned chart.
pub fn inflate_series(
    w: &WireSeries,
    n: usize,
    offset: usize,
    env_dense: bool,
    raw_dense: bool,
) -> Option<DenseSeries> {
    let values = expand_segments(w.seg_starts, w.seg_lens, w.values, n, offset)?;
    let raw_values = if raw_dense {
        if w.raw_values.is_empty() {
            vec![f64::NAN; n]
        } else {
            if w.raw_values.len() != w.values.len() {
                return None; // raw is parallel to the value occupancy
            }
            expand_segments(w.seg_starts, w.seg_lens, w.raw_values, n, offset)?
        }
    } else if w.raw_values.is_empty() {
        Vec::new()
    } else {
        return None; // raw entries on a raw-less chart
    };
    let (min_values, max_values) = if env_dense {
        // The dense-envelope contract: min == max == value at every occupied slot the wire's band entries skip, band entries overriding — including marker-won slots, where the value is NaN but the bucket's finite evidence still bands.
        let mut min_values = values.clone();
        let mut max_values = values.clone();
        let bmin = expand_segments(w.band_seg_starts, w.band_seg_lens, w.band_min, n, offset)?;
        let bmax = expand_segments(w.band_seg_starts, w.band_seg_lens, w.band_max, n, offset)?;
        for i in 0..n {
            if !bmin[i].is_nan() || !bmax[i].is_nan() {
                min_values[i] = bmin[i];
                max_values[i] = bmax[i];
            }
        }
        (min_values, max_values)
    } else if w.band_seg_starts.is_empty()
        && w.band_seg_lens.is_empty()
        && w.band_min.is_empty()
        && w.band_max.is_empty()
    {
        (Vec::new(), Vec::new())
    } else {
        return None; // band entries on a band-less series
    };
    Some(DenseSeries {
        label: w.label.to_string(),
        run_id: w.run_id.to_string(),
        values,
        raw_values,
        min_values,
        max_values,
        nan_indices: w.nan_indices.to_vec(),
        nan_kinds: w.nan_kinds.to_vec(),
        xnan_count: w.xnan_count,
    })
}

/// Whether a wire series carries anything at all in a family's tail — the client's half of the family-existence rule for continuations (see [`inflate_series`]).
pub fn wire_has_content(w: &WireSeries) -> bool {
    !w.values.is_empty() || !w.band_min.is_empty()
}

/// [`emit_series`]' output: owned wire vectors, because this module cannot name either side's generated ChartSeries — the caller moves the fields across, no logic of its own.
pub struct WireSeriesOwned {
    pub label: String,
    pub run_id: String,
    pub seg_starts: Vec<u32>,
    pub seg_lens: Vec<u32>,
    pub values: Vec<f32>,
    pub raw_values: Vec<f32>,
    pub band_seg_starts: Vec<u32>,
    pub band_seg_lens: Vec<u32>,
    pub band_min: Vec<f32>,
    pub band_max: Vec<f32>,
    pub nan_indices: Vec<u32>,
    pub nan_kinds: Vec<u32>,
    pub xnan_count: u32,
}

impl WireSeriesOwned {
    /// Borrow as the inflater's view — the codec pair meets in this module's tests without either side's proto types.
    pub fn view(&self) -> WireSeries<'_> {
        WireSeries {
            label: &self.label,
            run_id: &self.run_id,
            seg_starts: &self.seg_starts,
            seg_lens: &self.seg_lens,
            values: &self.values,
            raw_values: &self.raw_values,
            band_seg_starts: &self.band_seg_starts,
            band_seg_lens: &self.band_seg_lens,
            band_min: &self.band_min,
            band_max: &self.band_max,
            nan_indices: &self.nan_indices,
            nan_kinds: &self.nan_kinds,
            xnan_count: self.xnan_count,
        }
    }
}

/// Wire form of one dense series over columns [c..] (0 = the whole series, from_col for a continuation's tail): occupancy segments with ABSOLUTE column starts, strictly finite f32 values, markers absolute, band entries only where the envelope says more than the value — [`inflate_series`] re-derives min == max == value everywhere else. The inverse of that inflation, kept beside it so the codec pair cannot drift.
pub fn emit_series(s: &DenseSeries, c: usize) -> WireSeriesOwned {
    let t = slice_series(s, c);
    let (mut seg_starts, seg_lens, values) = compress_segments(&t.values);
    for st in &mut seg_starts {
        *st += c as u32;
    }
    // Raw rides parallel to the value occupancy (they exist at exactly the same slots: both are the run's finite samples).
    let raw_values: Vec<f32> = if t.raw_values.is_empty() {
        Vec::new()
    } else {
        t.values
            .iter()
            .zip(&t.raw_values)
            .filter(|(v, _)| v.is_finite())
            .map(|(_, &r)| r as f32)
            .collect()
    };
    let (band_seg_starts, band_seg_lens, band_min, band_max) = if t.min_values.is_empty() {
        (Vec::new(), Vec::new(), Vec::new(), Vec::new())
    } else {
        // Mask the derivable entries (band == value, bitwise on the rounded model): what remains is the real information — bucket spreads, duplicate-x spreads, and marker-slot bands (value NaN there, so they always differ).
        let mut mn = vec![f64::NAN; t.values.len()];
        let mut mx = vec![f64::NAN; t.values.len()];
        for i in 0..t.values.len() {
            let (lo, hi, v) = (t.min_values[i], t.max_values[i], t.values[i]);
            if !lo.is_nan() && (lo.to_bits() != v.to_bits() || hi.to_bits() != v.to_bits()) {
                mn[i] = lo;
                mx[i] = hi;
            }
        }
        let (mut bs, bl, bmn) = compress_segments(&mn);
        let bmx = finite_f32(&mx);
        for st in &mut bs {
            *st += c as u32;
        }
        (bs, bl, bmn, bmx)
    };
    WireSeriesOwned {
        label: t.label,
        run_id: t.run_id,
        seg_starts,
        seg_lens,
        values,
        raw_values,
        band_seg_starts,
        band_seg_lens,
        band_min,
        band_max,
        nan_indices: t.nan_indices,
        nan_kinds: t.nan_kinds,
        xnan_count: t.xnan_count,
    }
}

/// Chart-level bucket x extents over columns [c..]: the same segment scheme, but f64 — time-axis extents are epoch milliseconds, past f32. Returns (starts, lens, min, max) for the caller's ChartResponse.
pub fn emit_xr(chart: &DenseChart, c: usize) -> (Vec<u32>, Vec<u32>, Vec<f64>, Vec<f64>) {
    let tail_min = &chart.xr_min[c.min(chart.xr_min.len())..];
    let tail_max = &chart.xr_max[c.min(chart.xr_max.len())..];
    let (mut starts, lens) = segment_spans(tail_min);
    for st in &mut starts {
        *st += c as u32;
    }
    let xr_min: Vec<f64> = tail_min.iter().copied().filter(|v| v.is_finite()).collect();
    let xr_max: Vec<f64> = tail_max.iter().copied().filter(|v| v.is_finite()).collect();
    (starts, lens, xr_min, xr_max)
}

/// Borrowed view of a full (non-delta) wire response's chart-level fields — [`WireSeries`] one level up.
pub struct WireChart<'a> {
    pub x_values: &'a [f64],
    pub xr_seg_starts: &'a [u32],
    pub xr_seg_lens: &'a [u32],
    pub xr_min: &'a [f64],
    pub xr_max: &'a [f64],
    pub banded: bool,
}

/// Inflate a FULL (non-delta) response into the dense model. Family existence is wire-driven here: the envelope exists for every series with any content on a banded chart (the dense-envelope contract), raw wherever entries ship. None = malformed segments (a protocol violation): refuse, never render a misaligned chart. Deltas go through the splice instead (chart_sync splice_response) — their family existence needs the held series.
pub fn inflate_chart<'a>(
    c: &WireChart<'_>,
    series: impl IntoIterator<Item = WireSeries<'a>>,
) -> Option<DenseChart> {
    let n = c.x_values.len();
    let series = series
        .into_iter()
        .map(|w| {
            let env_dense = c.banded && wire_has_content(&w);
            let raw_dense = !w.raw_values.is_empty();
            inflate_series(&w, n, 0, env_dense, raw_dense)
        })
        .collect::<Option<Vec<_>>>()?;
    Some(DenseChart {
        x_values: c.x_values.to_vec(),
        xr_min: expand_segments(c.xr_seg_starts, c.xr_seg_lens, c.xr_min, n, 0)?,
        xr_max: expand_segments(c.xr_seg_starts, c.xr_seg_lens, c.xr_max, n, 0)?,
        series,
    })
}

/// Columns `[c..]` of one series, markers kept absolute — what a delta response carries per continuing series. An absent vector stays absent: `[c.min(len)..]` of nothing is nothing.
pub fn slice_series(s: &DenseSeries, c: usize) -> DenseSeries {
    let tail = |v: &[f64]| v[c.min(v.len())..].to_vec();
    let (nan_indices, nan_kinds) = markers(s, c as u32, false);
    DenseSeries {
        label: s.label.clone(),
        run_id: s.run_id.clone(),
        values: tail(&s.values),
        raw_values: tail(&s.raw_values),
        min_values: tail(&s.min_values),
        max_values: tail(&s.max_values),
        nan_indices,
        nan_kinds,
        xnan_count: s.xnan_count, // whole-series scalar: a tail carries the full count
    }
}

/// Splice a delta series onto the cached one at column `c`: the cached prefix `[..c]`, then the delta's columns. Two absent vectors join to absent. A family ABSENT in the cache but present in the delta materializes as NaN over the kept prefix: envelope families are absent exactly while a series has no finite sample, so one springing into existence means its first finite samples arrived — new, at or past `c` by the dirty-column rule — and the prefix is provably all-NaN. The reverse (cached present, delta absent alongside a non-empty tail) is a protocol violation the caller refuses (chart_sync splice_response). Labels/run_ids adopt the delta's — they can legitimately change.
pub fn splice_series(cached: &DenseSeries, delta: &DenseSeries, c: usize) -> DenseSeries {
    let join = |a: &[f64], b: &[f64]| {
        if a.is_empty() && !b.is_empty() {
            let mut v = vec![f64::NAN; c];
            v.extend_from_slice(b);
            return v;
        }
        let mut v = a[..c.min(a.len())].to_vec();
        v.extend_from_slice(b);
        v
    };
    let (mut nan_indices, mut nan_kinds) = markers(cached, c as u32, true);
    let (di, dk) = markers(delta, c as u32, false);
    nan_indices.extend(di);
    nan_kinds.extend(dk);
    DenseSeries {
        label: delta.label.clone(),
        run_id: delta.run_id.clone(),
        values: join(&cached.values, &delta.values),
        raw_values: join(&cached.raw_values, &delta.raw_values),
        min_values: join(&cached.min_values, &delta.min_values),
        max_values: join(&cached.max_values, &delta.max_values),
        nan_indices,
        nan_kinds,
        xnan_count: delta.xnan_count, // ships complete, like the label
    }
}

/// A series' (index, kind) markers on one side of column `c`, kinds defaulted to 1 where absent (the client renderer's rule).
fn markers(s: &DenseSeries, c: u32, below: bool) -> (Vec<u32>, Vec<u32>) {
    s.nan_indices
        .iter()
        .enumerate()
        .filter(|(_, &idx)| (idx < c) == below)
        .map(|(j, &idx)| (idx, s.nan_kinds.get(j).copied().unwrap_or(1)))
        .unzip()
}

/// FNV-1a over 64-bit words. Not cryptographic — the result hashes guard against bugs, not adversaries; what matters is that any divergent bit, length, or family assignment moves the hash, deterministically and identically in both crates.
fn fnv64(h: u64, word: u64) -> u64 {
    (h ^ word).wrapping_mul(0x0000_0100_0000_01b3)
}

/// Hash of the chart-level columns (ChartResponse.result_x_hash): the shared axis and the bucket x extents, lengths included.
pub fn hash_axis(c: &DenseChart) -> u64 {
    let mut h = fnv64(0xcbf2_9ce4_8422_2325, c.x_values.len() as u64);
    for col in [&c.x_values, &c.xr_min, &c.xr_max] {
        h = fnv64(h, col.len() as u64);
        for v in col.iter() {
            h = fnv64(h, v.to_bits());
        }
    }
    h
}

/// Content hash of one series' columns and markers (ChartResponse.result_series_hashes), family-tagged so equal values can't slide between families. Labels, run_ids, and xnan_count are EXCLUDED — they ship complete on every series and can legitimately differ between the request the held response answered and the one the delta answers. The server stamps every delta with hashes of the FULL model it reconstructs to (query.rs to_delta); the client hashes its spliced rebuild and refuses a mismatch BEFORE rendering (chart_sync splice_response) — the end-to-end check on every delta, catching whatever diverged: a planner bug the sampled audit missed, a membership edge, drift accumulated by an earlier splice (the cache is the accumulated splices) — none of which the server can see from its side. Marker kinds default to 1 where absent, the same rule [`markers`] applies, so a series hashes the same before and after a splice materializes them. Y columns must already be [`round_y`]-rounded (the server rounds at build; the client's columns arrive f32).
pub fn hash_series(s: &DenseSeries) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325;
    for (fi, col) in [&s.values, &s.raw_values, &s.min_values, &s.max_values]
        .into_iter()
        .enumerate()
    {
        h = fnv64(h, ((fi as u64) << 32) | col.len() as u64);
        for v in col.iter() {
            h = fnv64(h, v.to_bits());
        }
    }
    h = fnv64(h, s.nan_indices.len() as u64);
    for (j, &idx) in s.nan_indices.iter().enumerate() {
        let kind = s.nan_kinds.get(j).copied().unwrap_or(1);
        h = fnv64(h, ((idx as u64) << 32) | kind as u64);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A synthetic series over `n` columns, seeded and deterministic; envelope/raw presence flags cover every shipping shape chart.rs produces. Values pre-rounded through round_y, as the real pipeline guarantees.
    fn series(n: usize, seed: u64, envelope: bool, raw: bool) -> DenseSeries {
        let col = |k: u64| -> Vec<f64> {
            (0..n)
                .map(|i| {
                    if (i as u64 + seed).is_multiple_of(7) {
                        f64::NAN
                    } else {
                        round_y(((i as u64).wrapping_mul(seed + k + 1) % 1000) as f64 / 3.0)
                    }
                })
                .collect()
        };
        let e = |on: bool, k: u64| if on { col(k) } else { Vec::new() };
        DenseSeries {
            label: format!("s{seed}"),
            run_id: format!("r{seed}"),
            values: col(0),
            raw_values: e(raw, 1),
            min_values: e(envelope, 2),
            max_values: e(envelope, 3),
            nan_indices: (0..n as u32)
                .filter(|i| (*i as u64 + seed).is_multiple_of(11))
                .collect(),
            nan_kinds: (0..n as u32)
                .filter(|i| (*i as u64 + seed).is_multiple_of(11))
                .map(|i| 1 + i % 4)
                .collect(),
            xnan_count: (seed % 3) as u32,
        }
    }

    /// The first `c` columns — what an older, shorter chart shipped.
    fn truncated(s: &DenseSeries, c: usize) -> DenseSeries {
        let t = |v: &Vec<f64>| {
            if v.is_empty() {
                Vec::new()
            } else {
                v[..c].to_vec()
            }
        };
        let keep: Vec<usize> = (0..s.nan_indices.len())
            .filter(|&j| s.nan_indices[j] < c as u32)
            .collect();
        DenseSeries {
            label: s.label.clone(),
            run_id: s.run_id.clone(),
            values: t(&s.values),
            raw_values: t(&s.raw_values),
            min_values: t(&s.min_values),
            max_values: t(&s.max_values),
            nan_indices: keep.iter().map(|&j| s.nan_indices[j]).collect(),
            nan_kinds: keep.iter().map(|&j| s.nan_kinds[j]).collect(),
            xnan_count: s.xnan_count,
        }
    }

    /// Bitwise column equality — Vec<f64> PartialEq is false at NaN, which every real chart contains.
    fn cols_eq(a: &DenseSeries, b: &DenseSeries) -> bool {
        let f = |x: &[f64], y: &[f64]| {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| p.to_bits() == q.to_bits())
        };
        f(&a.values, &b.values)
            && f(&a.raw_values, &b.raw_values)
            && f(&a.min_values, &b.min_values)
            && f(&a.max_values, &b.max_values)
            && a.nan_indices == b.nan_indices
            && a.nan_kinds == b.nan_kinds
            && a.xnan_count == b.xnan_count
    }

    #[test]
    fn segments_roundtrip_without_filler() {
        // Gaps at the front, interior, and tail; a column that is all-gap; a dense one.
        let cases: Vec<Vec<f64>> = vec![
            vec![f64::NAN, 1.5, 2.5, f64::NAN, f64::NAN, 4.0, f64::NAN],
            vec![f64::NAN; 5],
            (0..9).map(|i| i as f64).collect(),
            vec![],
        ];
        for dense in cases {
            let rounded: Vec<f64> = dense.iter().map(|&v| round_y(v)).collect();
            let (starts, lens, vals) = compress_segments(&rounded);
            assert!(
                vals.iter().all(|v| v.is_finite()),
                "wire values must be finite"
            );
            let back = expand_segments(&starts, &lens, &vals, rounded.len(), 0).unwrap();
            assert_eq!(
                back.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                rounded.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn segments_offset_and_violations() {
        let dense = vec![f64::NAN, 1.0, 2.0, f64::NAN, 3.0];
        let (starts, lens, vals) = compress_segments(&dense);
        // A delta tail: same segments, absolute starts, expanded into a shorter tail vector.
        let shifted: Vec<u32> = starts.iter().map(|s| s + 10).collect();
        let tail = expand_segments(&shifted, &lens, &vals, dense.len(), 10).unwrap();
        assert_eq!(
            tail.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            dense.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
        );
        // Out-of-range, mismatched-parallel, and short-value shapes refuse instead of misaligning.
        assert!(
            expand_segments(&shifted, &lens, &vals, dense.len(), 12).is_none(),
            "start below offset"
        );
        assert!(
            expand_segments(&starts, &lens, &vals, 2, 0).is_none(),
            "segment past the axis"
        );
        assert!(
            expand_segments(&starts, &lens[..1], &vals, dense.len(), 0).is_none(),
            "starts/lens not parallel"
        );
        assert!(
            expand_segments(&starts, &lens, &vals[..1], dense.len(), 0).is_none(),
            "values shorter than segments"
        );
        let mut long = vals.clone();
        long.push(9.0);
        assert!(
            expand_segments(&starts, &lens, &long, dense.len(), 0).is_none(),
            "values longer than segments"
        );
    }

    #[test]
    fn emit_matches_inflate_bit_for_bit() {
        // The codec pair at series level: emit's band masking (entries only where the envelope says more) must inflate back bit-for-bit under the dense-envelope derivation, for full series and delta tails alike.
        for (envelope, raw) in [(true, false), (false, true), (false, false), (true, true)] {
            let mut s = series(90, 13, envelope, raw);
            if envelope {
                // A derivable stretch (band == value): emit must drop those entries and inflate must re-derive them.
                for i in 30..50 {
                    s.min_values[i] = s.values[i];
                    s.max_values[i] = s.values[i];
                }
            }
            for c in [0usize, 45] {
                let w = emit_series(&s, c);
                assert!(
                    w.values.iter().all(|v| v.is_finite()),
                    "wire values must be finite"
                );
                if envelope && c == 0 {
                    assert!(
                        w.band_min.len() < w.values.len(),
                        "derivable band entries must not ship"
                    );
                }
                let n = s.values.len() - c;
                let tail = inflate_series(&w.view(), n, c, envelope, raw).unwrap();
                assert!(
                    cols_eq(&tail, &slice_series(&s, c)),
                    "envelope={envelope} raw={raw} c={c}"
                );
            }
        }
    }

    #[test]
    fn stray_band_values_without_segments_refuse() {
        let s = series(20, 5, false, false);
        let mut w = emit_series(&s, 0);
        w.band_min = vec![1.0];
        w.band_max = vec![1.0];
        assert!(
            inflate_series(&w.view(), 20, 0, false, false).is_none(),
            "band values on a band-less series must refuse even without segments"
        );
    }

    #[test]
    fn slice_then_splice_roundtrips() {
        for (envelope, raw) in [(true, false), (false, true), (false, false), (true, true)] {
            let new = series(90, 13, envelope, raw);
            for c in [1usize, 45, 89, 90] {
                let old = truncated(&new, c); // cached holds >= from_col columns
                let sliced = slice_series(&new, c);
                let rejoined = splice_series(&old, &sliced, c);
                assert!(
                    cols_eq(&rejoined, &new),
                    "envelope={envelope} raw={raw} c={c}"
                );
            }
        }
    }

    #[test]
    fn not_modified_is_an_empty_slice() {
        let s = series(30, 2, true, false);
        let sliced = slice_series(&s, 30);
        assert!(sliced.values.is_empty() && sliced.nan_indices.is_empty());
    }

    #[test]
    fn absent_families_materialize_as_nan_prefix() {
        // A series whose first finite samples arrive in the delta: the cached response shipped no envelope (no finite sample yet), the new one ships it full-length with an all-NaN prefix. The splice must materialize that prefix, not shorten the vectors.
        let mut new = series(40, 3, true, false);
        for col in [&mut new.min_values, &mut new.max_values] {
            for v in col[..25].iter_mut() {
                *v = f64::NAN;
            }
        }
        let mut old = truncated(&new, 25);
        old.min_values.clear();
        old.max_values.clear();
        let sliced = slice_series(&new, 25);
        let rejoined = splice_series(&old, &sliced, 25);
        assert!(cols_eq(&rejoined, &new));
    }

    #[test]
    fn hashes_pin_every_bit_family_and_marker() {
        let a = series(60, 9, true, true);
        assert_eq!(hash_series(&a), hash_series(&a), "deterministic");
        // One flipped bit in one family moves the hash.
        let mut b = series(60, 9, true, true);
        b.min_values[17] = f64::from_bits(b.min_values[17].to_bits() ^ 1);
        assert_ne!(hash_series(&a), hash_series(&b));
        // The same values in a different family are a different series.
        let fam = |values: Vec<f64>, min_values: Vec<f64>| DenseSeries {
            values,
            min_values,
            ..Default::default()
        };
        assert_ne!(
            hash_series(&fam(vec![1.5, 2.5], vec![])),
            hash_series(&fam(vec![], vec![1.5, 2.5]))
        );
        // A marker kind moves it; labels don't (they legitimately change between requests).
        let mut e = series(60, 9, true, true);
        e.nan_kinds[0] += 1;
        assert_ne!(hash_series(&a), hash_series(&e));
        let mut l = series(60, 9, true, true);
        l.label = "renamed".into();
        assert_eq!(hash_series(&a), hash_series(&l));
        // The axis hash pins x, the bucket extents, and lengths.
        let chart = DenseChart {
            x_values: a.values.clone(),
            xr_min: vec![f64::NAN; 60],
            xr_max: vec![f64::NAN; 60],
            series: vec![],
        };
        let mut chart2 = chart.clone();
        chart2.xr_min[5] = 1.0;
        assert_ne!(hash_axis(&chart), hash_axis(&chart2));
        let mut chart3 = chart.clone();
        chart3.x_values.pop();
        assert_ne!(hash_axis(&chart), hash_axis(&chart3));
    }

    #[test]
    fn splice_roundtrip_preserves_the_hash() {
        // What the verification relies on: the client's accumulated splices hash identically to the response computed from scratch.
        let new = series(90, 13, true, false);
        let old = truncated(&new, 45);
        let sliced = slice_series(&new, 45);
        let rejoined = splice_series(&old, &sliced, 45);
        assert_eq!(hash_series(&rejoined), hash_series(&new));
    }
}
