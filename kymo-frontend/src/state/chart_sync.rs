//! Client half of the incremental chart protocol (AI-1421), plus the local-only cache transforms that avoid queries entirely.
//!
//! The WIRE carries no filler (occupancy segments, strictly finite f32 values — kymo.proto); the client inflates every response into the dense MODEL ([`chart_delta::DenseChart`]: axis-length columns, NaN = no data) at receipt ([`inflate_response`]), and everything downstream — cache, splice, filter, render — works on the model. Three mechanisms keep a chart's traffic proportional to what changed:
//!  - a run-SUBSET request (deselection) is answered from the cached superset model by [`filter_response`] — no query, and the superset entry stays so reselecting is also free;
//!  - real queries echo the held response's opaque continuation state ([`echo_state`]); the server reconstructs what the client holds and answers with a delta that [`splice_response`] rebuilds — appends arrive as a small tail, "nothing changed" as an empty one, a newly-selected run's series complete while held series continue (full only when the bucket layout shifted or append-only didn't hold);
//!  - runs that contributed no series to a complete linear response without custom X are dropped from the panel's version key ([`panel_version_key`]), so their steady ingest bumps stop reaching it (re-armed by that run's registry events or a resync epoch; range trims, log-axis filtering, and exact X/Y joins cannot prove a run silent).

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use prost::Message as _;

use crate::grpc::chart_delta::{self, DenseChart, DenseSeries};
use crate::grpc::proto::{ChartCacheState, ChartRequest, ChartResponse, ChartSeries, SeriesRef};
use crate::state::versions_key;

/// One panel's cached chart, module-level so it survives body unmounts (see metric_rect.rs). `response` is always the FULL dense model — deltas splice before storing — so it serves subset filters, delta splices, and instant remount renders alike.
#[derive(Clone)]
pub struct ChartCacheEntry {
    /// The request answered, `cache_state` stripped (transport detail, not query identity).
    pub request: ChartRequest,
    pub response: Rc<DenseChart>,
    /// Identity stamp of `response` (metric_rect NEXT_DATA_SEQ): uplot_chart keys data-change detection on this, so it must change exactly when `response` is a different model.
    pub data_seq: u64,
    /// Pre-send snapshots of the change signals this response depended on, for [`Self::fresh_for`]: per-run data versions and registry counters (request runs), and the resync epoch.
    pub versions: Rc<HashMap<String, u64>>,
    pub metrics_gen: Rc<HashMap<String, u64>>,
    pub epoch: u64,
    /// ChartResponse.frontiers of the freshest response folded in — opaque server state, including versioned lineage proofs, echoed verbatim on the next query ([`echo_state`]).
    pub frontiers: Rc<HashMap<String, i64>>,
    /// Requested runs that produced no series — their version bumps are noise to this panel. Derived only from complete linear responses without custom X (else empty): a range-trimmed answer can't prove a run silent outside the range, while log-axis filtering and an all-empty exact X/Y join can lose every series identity on the wire.
    pub noncontrib: Rc<HashSet<String>>,
}

impl ChartCacheEntry {
    /// Conservative retained-heap estimate used by the session cache's byte
    /// budget. Dense f64 columns dominate; request and map allocations are
    /// included so tag-heavy and many-run entries are not treated as free.
    pub fn estimated_heap_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.request.encoded_len())
            .saturating_add(dense_chart_heap_bytes(&self.response))
            .saturating_add(string_map_heap_bytes(&self.versions))
            .saturating_add(string_map_heap_bytes(&self.metrics_gen))
            .saturating_add(string_map_heap_bytes(&self.frontiers))
            .saturating_add(string_set_heap_bytes(&self.noncontrib))
    }

    /// Whether this entry still answers a query over `runs` — the one freshness rule behind both the exact-request and run-subset serves in metric_rect.rs. A contributing run's data version must be unchanged; a known non-contributing run has no data here, so only what could make it START contributing (its metrics_gen counter, the resync epoch) must be. Snapshot-vs-current comparison, absent-vs-present included: a version learned only after the fetch means the response may predate its data.
    pub fn fresh_for<'a>(
        &self,
        runs: impl IntoIterator<Item = &'a str>,
        run_versions: &HashMap<String, u64>,
        metrics_gen: &HashMap<String, u64>,
        epoch: u64,
    ) -> bool {
        let mut any_noncontrib = false;
        for r in runs {
            let ok = if self.noncontrib.contains(r) {
                any_noncontrib = true;
                self.metrics_gen.get(r) == metrics_gen.get(r)
            } else {
                self.versions.get(r) == run_versions.get(r)
            };
            if !ok {
                return false;
            }
        }
        !any_noncontrib || self.epoch == epoch
    }
}

fn vec_heap_bytes<T>(values: &Vec<T>) -> usize {
    values.capacity().saturating_mul(std::mem::size_of::<T>())
}

fn dense_chart_heap_bytes(chart: &DenseChart) -> usize {
    let mut bytes = std::mem::size_of::<DenseChart>()
        .saturating_add(2 * std::mem::size_of::<usize>()) // Rc allocation header
        .saturating_add(vec_heap_bytes(&chart.x_values))
        .saturating_add(vec_heap_bytes(&chart.xr_min))
        .saturating_add(vec_heap_bytes(&chart.xr_max))
        .saturating_add(vec_heap_bytes(&chart.series));
    for series in &chart.series {
        bytes = bytes
            .saturating_add(series.label.capacity())
            .saturating_add(series.run_id.capacity())
            .saturating_add(vec_heap_bytes(&series.values))
            .saturating_add(vec_heap_bytes(&series.raw_values))
            .saturating_add(vec_heap_bytes(&series.min_values))
            .saturating_add(vec_heap_bytes(&series.max_values))
            .saturating_add(vec_heap_bytes(&series.nan_indices))
            .saturating_add(vec_heap_bytes(&series.nan_kinds));
    }
    bytes
}

fn string_map_heap_bytes<V>(map: &HashMap<String, V>) -> usize {
    let bucket_bytes = std::mem::size_of::<(String, V)>().saturating_add(1);
    map.capacity()
        .saturating_mul(bucket_bytes)
        .saturating_add(map.keys().map(|key| key.capacity()).sum::<usize>())
        .saturating_add(2 * std::mem::size_of::<usize>()) // Rc allocation header
}

fn string_set_heap_bytes(set: &HashSet<String>) -> usize {
    let bucket_bytes = std::mem::size_of::<String>().saturating_add(1);
    set.capacity()
        .saturating_mul(bucket_bytes)
        .saturating_add(set.iter().map(|key| key.capacity()).sum::<usize>())
        .saturating_add(2 * std::mem::size_of::<usize>()) // Rc allocation header
}

/// Borrow a wire series for the shared inflater (structurally identical prost types chart_delta can't name).
fn wire(s: &ChartSeries) -> chart_delta::WireSeries<'_> {
    chart_delta::WireSeries {
        label: &s.label,
        run_id: &s.run_id,
        seg_starts: &s.seg_starts,
        seg_lens: &s.seg_lens,
        values: &s.values,
        raw_values: &s.raw_values,
        band_seg_starts: &s.band_seg_starts,
        band_seg_lens: &s.band_seg_lens,
        band_min: &s.band_min,
        band_max: &s.band_max,
        nan_indices: &s.nan_indices,
        nan_kinds: &s.nan_kinds,
        xnan_count: s.xnan_count,
    }
}

/// Inflate a FULL (non-delta) wire response into the dense model — chart_delta::inflate_chart, where the family-existence rules live. None = malformed segments (a protocol violation): the caller alerts and renders nothing rather than a misaligned chart.
pub fn inflate_response(resp: &ChartResponse) -> Option<DenseChart> {
    if resp.delta {
        return None;
    }
    chart_delta::inflate_chart(
        &chart_delta::WireChart {
            x_values: &resp.x_values,
            xr_seg_starts: &resp.xr_seg_starts,
            xr_seg_lens: &resp.xr_seg_lens,
            xr_min: &resp.xr_min,
            xr_max: &resp.xr_max,
            banded: resp.banded,
        },
        resp.series.iter().map(wire),
    )
}

/// Query-parameter equality — everything but the y-series set. The shared gate for subset serves and frontier echoes: different smoothing/resolution/range/axis is a different chart, and nothing held transfers.
fn params_match(a: &ChartRequest, b: &ChartRequest) -> bool {
    // The x metric alone identifies the x series — the server maps each run through its OWN x metric and ignores the ref's run_id, which shifts with selection (it's refs.first()).
    let x_matches = match (&a.x_series, &b.x_series) {
        (None, None) => true,
        (Some(xa), Some(xb)) => xa.metric_name == xb.metric_name,
        _ => false,
    };
    x_matches
        && a.smoothing == b.smoothing
        && a.target_resolution == b.target_resolution
        && a.step_min == b.step_min
        && a.step_max == b.step_max
        && a.use_timestamp_axis == b.use_timestamp_axis
        && a.relative_time == b.relative_time
        && a.log_buckets == b.log_buckets
}

/// The cache state to send with `request`: the held frontiers, echoed verbatim — but only when the entry answers the same chart (params match; the y-series set may differ, that's the growth case), the shared series pair positionally ([`splice_pairable`]), the connection/resync epoch still matches, and there is anything to continue from. An epoch change can reconnect this cache to a different server build, whose chart semantics must not splice onto the old model. None = ask for full.
pub fn echo_state(
    entry: &ChartCacheEntry,
    request: &ChartRequest,
    current_epoch: u64,
) -> Option<ChartCacheState> {
    (request.x_series.is_none()
        && entry.epoch == current_epoch
        && params_match(&entry.request, request)
        && splice_pairable(&entry.request, request)
        && !entry.frontiers.is_empty()
        && !entry.response.x_values.is_empty())
    .then(|| ChartCacheState {
        frontiers: (*entry.frontiers).clone(),
        // The held series count — the membership gate's ground truth (kymo.proto held_series): the server only ships a delta when every one of these continues, so the positional splice can never be asked to pair against a series it doesn't have.
        held_series: Some(entry.response.series.len() as u32),
    })
}

/// Client-side eligibility for positional splicing: shared (run, metric) refs must retain their complete identity, including tags, and relative order. Refs present on only one side may interleave freely; new series ship complete. The server also verifies ref identity/order in its opaque state, while this gate avoids sending unusable state.
fn splice_pairable(held: &ChartRequest, new: &ChartRequest) -> bool {
    fn key(s: &SeriesRef) -> (&str, &str) {
        (&s.run_id, &s.metric_name)
    }
    let held_keys: HashSet<_> = held.y_series.iter().map(key).collect();
    let new_keys: HashSet<_> = new.y_series.iter().map(key).collect();
    held.y_series
        .iter()
        .filter(|s| new_keys.contains(&key(s)))
        .eq(new.y_series.iter().filter(|s| held_keys.contains(&key(s))))
}

/// Rebuild the full model a delta continues: paired series splice onto cached ones at `from_col`; complete series (splice_from_cached -1) are adopted as shipped. None when the delta can't apply to `cached` — the caller refetches in full; a splice is never rendered on guesswork.
pub fn splice_response(cached: &DenseChart, delta: &ChartResponse) -> Option<DenseChart> {
    let c = delta.from_col as usize;
    if !delta.delta
        || c == 0
        || c > cached.x_values.len()
        || delta.splice_from_cached.len() != delta.series.len()
    {
        return None;
    }
    // The banded contract is chart-wide and only flips alongside a full answer — a delta whose bit disagrees with the held chart cannot continue it.
    let cached_banded = cached.series.iter().any(|s| !s.min_values.is_empty());
    if delta.banded != cached_banded {
        return None;
    }
    // The seam must ascend — a valid continuation's first new x exceeds the kept prefix's last. The one content invariant checkable without an oracle; a violation means the delta doesn't continue THIS response.
    if delta
        .x_values
        .first()
        .is_some_and(|&x| x <= cached.x_values[c - 1])
    {
        return None;
    }
    let tail_len = delta.x_values.len();
    let new_len = c + tail_len;
    let mut x_values = cached.x_values[..c].to_vec();
    x_values.extend_from_slice(&delta.x_values);
    let mut xr_min = cached.xr_min[..c.min(cached.xr_min.len())].to_vec();
    xr_min.extend(chart_delta::expand_segments(
        &delta.xr_seg_starts,
        &delta.xr_seg_lens,
        &delta.xr_min,
        tail_len,
        c,
    )?);
    let mut xr_max = cached.xr_max[..c.min(cached.xr_max.len())].to_vec();
    xr_max.extend(chart_delta::expand_segments(
        &delta.xr_seg_starts,
        &delta.xr_seg_lens,
        &delta.xr_max,
        tail_len,
        c,
    )?);
    let mut series = Vec::with_capacity(delta.series.len());
    let mut prev: i32 = -1;
    for (new, &idx) in delta.series.iter().zip(&delta.splice_from_cached) {
        let w = wire(new);
        if idx < 0 {
            // A complete series spans the whole spliced axis.
            let env_dense = delta.banded && chart_delta::wire_has_content(&w);
            series.push(chart_delta::inflate_series(
                &w,
                new_len,
                0,
                env_dense,
                !new.raw_values.is_empty(),
            )?);
            continue;
        }
        // The >= 0 entries are exactly 0,1,2,... — every held series continues in a valid delta (dropped refs and undecidable membership answer full instead, kymo.proto); anything else is a protocol violation.
        let old = (idx == prev + 1)
            .then(|| cached.series.get(idx as usize))
            .flatten()?;
        prev = idx;
        // A raw family the cached series carries cannot vanish from a tail that has values — refuse. (The envelope has no such check: band entries are legitimately absent wherever they equal the value, and the held series says the family exists.)
        if !old.raw_values.is_empty() && !w.values.is_empty() && w.raw_values.is_empty() {
            return None;
        }
        // Family existence for an all-gap tail comes from the held series; a family springing into existence (an all-non-finite series' first finite samples arrived) materializes its NaN prefix in splice_series — those samples are new, so the prefix is provably all-NaN.
        let env_dense =
            delta.banded && (!old.min_values.is_empty() || chart_delta::wire_has_content(&w));
        let raw_dense = !old.raw_values.is_empty() || !new.raw_values.is_empty();
        let tail = chart_delta::inflate_series(&w, tail_len, c, env_dense, raw_dense)?;
        series.push(chart_delta::splice_series(old, &tail, c));
    }
    // Every cached series consumed — a skipped one would silently drop a series the server believes this client still shows.
    if (prev + 1) as usize != cached.series.len() {
        return None;
    }
    let out = DenseChart {
        x_values,
        xr_min,
        xr_max,
        series,
    };
    // The end-to-end content check, on every delta: the spliced result must hash to the full model the server computed (to_delta's result_x_hash/result_series_hashes — chart_delta.rs hash fns, shared verbatim). A mismatch means this rebuild does NOT reproduce the server's chart, whatever the cause — a planner bug the sampled audit missed, a membership edge, drift accumulated by an earlier splice — so it is never rendered: refuse, refetch in full, alert.
    if delta.result_x_hash? != chart_delta::hash_axis(&out) {
        return None;
    }
    if delta.result_series_hashes.len() != out.series.len()
        || out
            .series
            .iter()
            .zip(&delta.result_series_hashes)
            .any(|(s, &h)| chart_delta::hash_series(s) != h)
    {
        return None;
    }
    Some(out)
}

fn ref_key(s: &SeriesRef) -> (&str, &str, &str, &[String]) {
    (&s.project_id, &s.run_id, &s.metric_name, &s.tags)
}

/// If `new` asks for a run-subset of what `cached` answered — same params, y_series missing only WHOLE runs — return the kept run_ids for [`filter_response`]. Matches a server answer except sub-bucket x-center drift (centers and x extents averaged the removed run's points; per-series envelopes stay exact) and grid tier (the filtered chart keeps the superset's bucketing until the next real fetch). Series separate by run_id alone, so a run keeping some refs while losing others is not filterable. Equal sets pass (order/dup jitter).
pub fn subset_keep(cached: &ChartRequest, new: &ChartRequest) -> Option<HashSet<String>> {
    if !params_match(cached, new) {
        return None;
    }
    let cached_set: HashSet<_> = cached.y_series.iter().map(ref_key).collect();
    let new_set: HashSet<_> = new.y_series.iter().map(ref_key).collect();
    if new_set.is_empty() || !new_set.is_subset(&cached_set) {
        return None;
    }
    let kept: HashSet<String> = new_set.iter().map(|k| k.1.to_string()).collect();
    if cached_set.difference(&new_set).any(|k| kept.contains(k.1)) {
        return None;
    }
    Some(kept)
}

/// The cached model restricted to `keep`'s runs: other series drop, axis slots no kept series occupies drop with them, markers remap to the compacted axis, and the chart-level x extents compact alongside (they may still include removed runs' points — accepted drift, like the centers). One more accepted drift on log charts: dropping the only run that owned x = 0 compacts the zero slot away, so the renderer's zero-present rule (uplot_chart log_shift) flips this view to plain log(x) while the bucket GEOMETRY was computed on the log(x+1) ladder — sub-bucket misalignment confined to x ≲ 16, replaced by a correctly bucketed fetch on the next kept-run bump. The transform is always derived from the rendered model, never copied from the superset, so it cannot go stale.
pub fn filter_response(resp: &DenseChart, keep: &HashSet<String>) -> DenseChart {
    let kept: Vec<&DenseSeries> = resp
        .series
        .iter()
        .filter(|s| keep.contains(&s.run_id))
        .collect();
    let n = resp.x_values.len();
    // Wire default: a marker without a kind is kind 1.
    let kind = |s: &DenseSeries, j: usize| s.nan_kinds.get(j).copied().unwrap_or(1);
    let mut occupied = vec![false; n];
    for s in &kept {
        // Any finite column marks the slot: values covers unsmoothed charts, raw/envelope cover smoothed and marker-evicted ones. Kinds 1-3 occupy their slot by definition. Kind 4 claims none: it rides its run's own finite column, except for a run with no plottable x, whose column-0 marker re-lands below.
        for col in [&s.values, &s.raw_values, &s.min_values] {
            for (i, v) in col.iter().enumerate() {
                if !v.is_nan() {
                    occupied[i] = true;
                }
            }
        }
        for (j, &i) in s.nan_indices.iter().enumerate() {
            if kind(s, j) != 4 {
                if let Some(o) = occupied.get_mut(i as usize) {
                    *o = true;
                }
            }
        }
    }
    let mut slot_map = vec![u32::MAX; n];
    let mut x_values = Vec::new();
    for i in 0..n {
        if occupied[i] {
            slot_map[i] = x_values.len() as u32;
            x_values.push(resp.x_values[i]);
        }
    }
    let compact = |col: &[f64]| -> Vec<f64> {
        col.iter()
            .enumerate()
            .filter(|(i, _)| occupied[*i])
            .map(|(_, v)| *v)
            .collect()
    };
    let series = kept
        .into_iter()
        .map(|s| {
            // Markers remap through the compacted axis. A kind-4 marker whose slot left is the no-plottable-x marker: it re-lands on the first kept column, or leaves only its count on an empty axis.
            let (nan_indices, nan_kinds) = s
                .nan_indices
                .iter()
                .enumerate()
                .filter_map(|(j, &i)| {
                    let k = kind(s, j);
                    match slot_map.get(i as usize) {
                        Some(&m) if m != u32::MAX => Some((m, k)),
                        _ if k == 4 && !x_values.is_empty() => Some((0, 4)),
                        _ => None,
                    }
                })
                .unzip();
            DenseSeries {
                label: s.label.clone(),
                run_id: s.run_id.clone(),
                values: compact(&s.values),
                raw_values: compact(&s.raw_values),
                min_values: compact(&s.min_values),
                max_values: compact(&s.max_values),
                nan_indices,
                nan_kinds,
                xnan_count: s.xnan_count,
            }
        })
        .collect();
    DenseChart {
        x_values,
        xr_min: compact(&resp.xr_min),
        xr_max: compact(&resp.xr_max),
        series,
    }
}

/// The version key a chart panel refetches on: the bound runs' versions its data actually depends on. Known non-contributors are excluded — their ingest bumps can't change this chart — with their registry counters and the resync epoch mixed into the seed instead, so a first registry event (the run might have just logged this very metric) or a reconnect gap re-probes. With nothing excluded this reduces exactly to the plain bound-runs key (epoch bumps don't refetch settled panels).
pub fn panel_version_key(
    epoch: u64,
    bound: &[String],
    metrics_gen: &HashMap<String, u64>,
    versions: &HashMap<String, u64>,
    noncontrib: Option<&HashSet<String>>,
) -> u64 {
    let excluded = |id: &str| noncontrib.is_some_and(|nc| nc.contains(id));
    if !bound.iter().any(|id| excluded(id)) {
        return versions_key(0, bound.iter().map(String::as_str), versions);
    }
    let seed = versions_key(
        epoch,
        bound.iter().map(String::as_str).filter(|id| excluded(id)),
        metrics_gen,
    );
    versions_key(
        seed,
        bound.iter().map(String::as_str).filter(|id| !excluded(id)),
        versions,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn representative_heavy_chart_has_a_multi_megabyte_cache_weight() {
        let axis_len = 2_000;
        let series = (0..64)
            .map(|index| DenseSeries {
                label: format!("run-{index}"),
                run_id: format!("run-{index}"),
                values: vec![1.0; axis_len],
                raw_values: vec![1.0; axis_len],
                min_values: vec![0.5; axis_len],
                max_values: vec![1.5; axis_len],
                ..Default::default()
            })
            .collect();
        let entry = ChartCacheEntry {
            request: ChartRequest::default(),
            response: Rc::new(DenseChart {
                x_values: vec![0.0; axis_len],
                xr_min: vec![0.0; axis_len],
                xr_max: vec![0.0; axis_len],
                series,
            }),
            data_seq: 1,
            versions: Rc::new(HashMap::new()),
            metrics_gen: Rc::new(HashMap::new()),
            epoch: 0,
            frontiers: Rc::new(HashMap::new()),
            noncontrib: Rc::new(HashSet::new()),
        };

        let bytes = entry.estimated_heap_bytes();
        assert!(
            (3_900_000..5_000_000).contains(&bytes),
            "representative 64-series chart weighed {bytes} bytes"
        );
    }

    fn req(runs: &[&str], resolution: u32) -> ChartRequest {
        ChartRequest {
            y_series: runs
                .iter()
                .map(|r| SeriesRef {
                    project_id: "p".into(),
                    run_id: (*r).into(),
                    metric_name: "loss".into(),
                    tags: vec![],
                })
                .collect(),
            x_series: None,
            smoothing: None,
            target_resolution: resolution,
            step_min: None,
            step_max: None,
            use_timestamp_axis: false,
            relative_time: false,
            cache_state: None,
            log_buckets: false,
        }
    }

    fn keep_of(cached: &ChartRequest, new: &ChartRequest) -> Option<Vec<String>> {
        subset_keep(cached, new).map(|k| {
            let mut v: Vec<String> = k.into_iter().collect();
            v.sort();
            v
        })
    }

    fn dense(run: &str, values: Vec<f64>, nis: Vec<u32>, nks: Vec<u32>) -> DenseSeries {
        DenseSeries {
            label: run.into(),
            run_id: run.into(),
            values,
            nan_indices: nis,
            nan_kinds: nks,
            ..Default::default()
        }
    }

    /// Wire-encode a dense tail the way the server does — the same chart_delta::emit_series, moved into the frontend's generated type.
    fn wire_series(s: &DenseSeries, c: usize) -> ChartSeries {
        let w = chart_delta::emit_series(s, c);
        ChartSeries {
            label: w.label,
            run_id: w.run_id,
            seg_starts: w.seg_starts,
            seg_lens: w.seg_lens,
            values: w.values,
            raw_values: w.raw_values,
            band_seg_starts: w.band_seg_starts,
            band_seg_lens: w.band_seg_lens,
            band_min: w.band_min,
            band_max: w.band_max,
            nan_indices: w.nan_indices,
            nan_kinds: w.nan_kinds,
            xnan_count: w.xnan_count,
        }
    }

    /// A wire delta continuing `full` from column `c`, with correct result hashes; `complete` marks series that ship full-axis.
    fn delta_of(full: &DenseChart, c: usize, complete: &[bool]) -> ChartResponse {
        let mut next = 0i32;
        let splice_from_cached: Vec<i32> = complete
            .iter()
            .map(|&comp| {
                if comp {
                    -1
                } else {
                    let v = next;
                    next += 1;
                    v
                }
            })
            .collect();
        let series: Vec<ChartSeries> = full
            .series
            .iter()
            .zip(complete)
            .map(|(s, &comp)| wire_series(s, if comp { 0 } else { c }))
            .collect();
        let (xr_seg_starts, xr_seg_lens, xr_min, xr_max) = chart_delta::emit_xr(full, c);
        ChartResponse {
            x_values: full.x_values[c..].to_vec(),
            series,
            delta: true,
            from_col: c as u32,
            splice_from_cached,
            banded: full.series.iter().any(|s| !s.min_values.is_empty()),
            xr_seg_starts,
            xr_seg_lens,
            xr_min,
            xr_max,
            result_x_hash: Some(chart_delta::hash_axis(full)),
            result_series_hashes: full.series.iter().map(chart_delta::hash_series).collect(),
            ..Default::default()
        }
    }

    fn bits_eq(a: &DenseChart, b: &DenseChart) -> bool {
        let bits = |v: &[f64]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
        bits(&a.x_values) == bits(&b.x_values)
            && bits(&a.xr_min) == bits(&b.xr_min)
            && bits(&a.xr_max) == bits(&b.xr_max)
            && a.series.len() == b.series.len()
            && a.series.iter().zip(&b.series).all(|(s, t)| {
                bits(&s.values) == bits(&t.values)
                    && bits(&s.raw_values) == bits(&t.raw_values)
                    && bits(&s.min_values) == bits(&t.min_values)
                    && bits(&s.max_values) == bits(&t.max_values)
                    && s.nan_indices == t.nan_indices
                    && s.nan_kinds == t.nan_kinds
                    && s.xnan_count == t.xnan_count
            })
    }

    #[test]
    fn subset_keep_accepts_whole_run_removal_only() {
        let cached = req(&["a", "b", "c"], 1000);
        assert_eq!(
            keep_of(&cached, &req(&["a", "c"], 1000)),
            Some(vec!["a".into(), "c".into()])
        );
        // equal sets (order jitter) filter to everything
        assert_eq!(
            keep_of(&cached, &req(&["c", "b", "a"], 1000)),
            Some(vec!["a".into(), "b".into(), "c".into()])
        );
        // superset, empty, changed params: no
        assert_eq!(keep_of(&cached, &req(&["a", "b", "c", "d"], 1000)), None);
        assert_eq!(keep_of(&cached, &req(&[], 1000)), None);
        assert_eq!(keep_of(&cached, &req(&["a"], 1250)), None);
        // a log-bucket toggle is a different chart entirely
        let mut logd = req(&["a", "b", "c"], 1000);
        logd.log_buckets = true;
        assert_eq!(keep_of(&cached, &logd), None);
        // a run losing one metric but keeping another is not run-separable
        let mut two_metrics = req(&["a", "b"], 1000);
        two_metrics.y_series.push(SeriesRef {
            project_id: "p".into(),
            run_id: "a".into(),
            metric_name: "acc".into(),
            tags: vec![],
        });
        assert_eq!(keep_of(&two_metrics, &req(&["a", "b"], 1000)), None);
        // x_series identity is the metric, not the ref's run
        let mut cx = req(&["a", "b"], 1000);
        cx.x_series = Some(SeriesRef {
            project_id: "p".into(),
            run_id: "a".into(),
            metric_name: "epoch".into(),
            tags: vec![],
        });
        let mut cx_new = req(&["b"], 1000);
        cx_new.x_series = Some(SeriesRef {
            project_id: "p".into(),
            run_id: "b".into(),
            metric_name: "epoch".into(),
            tags: vec![],
        });
        assert_eq!(keep_of(&cx, &cx_new), Some(vec!["b".into()]));
    }

    #[test]
    fn filter_drops_series_and_empty_slots() {
        // Axis slots: 0 shared, 1 b-only, 2 a-only, 3 b-marker-only, 4 b-only.
        let resp = DenseChart {
            x_values: vec![0.0, 1.0, 2.0, 3.0, 4.0],
            xr_min: vec![f64::NAN, f64::NAN, 1.8, f64::NAN, f64::NAN],
            xr_max: vec![f64::NAN, f64::NAN, 2.2, f64::NAN, f64::NAN],
            series: vec![
                DenseSeries {
                    label: "a".into(),
                    run_id: "a".into(),
                    values: vec![1.0, f64::NAN, 3.0, f64::NAN, f64::NAN],
                    min_values: vec![1.0, f64::NAN, 2.5, f64::NAN, f64::NAN],
                    max_values: vec![1.0, f64::NAN, 3.5, f64::NAN, f64::NAN],
                    nan_indices: vec![2],
                    nan_kinds: vec![4],
                    ..Default::default()
                },
                DenseSeries {
                    label: "b".into(),
                    run_id: "b".into(),
                    values: vec![5.0, 6.0, f64::NAN, f64::NAN, 8.0],
                    nan_indices: vec![3],
                    nan_kinds: vec![1],
                    ..Default::default()
                },
            ],
        };
        let keep: HashSet<String> = ["a".to_string()].into();
        let out = filter_response(&resp, &keep);
        assert_eq!(out.x_values, vec![0.0, 2.0]);
        assert_eq!(out.series.len(), 1);
        assert_eq!(out.series[0].run_id, "a");
        assert_eq!(out.series[0].values.len(), 2);
        assert_eq!(out.series[0].values[0], 1.0);
        assert_eq!(out.series[0].values[1], 3.0);
        assert_eq!(out.series[0].min_values, vec![1.0, 2.5]);
        // chart-level x extents compact with the axis
        assert!(out.xr_min[0].is_nan());
        assert_eq!((out.xr_min[1], out.xr_max[1]), (1.8, 2.2));
        // marker remapped from slot 2 to compacted slot 1, kind preserved
        assert_eq!(out.series[0].nan_indices, vec![1]);
        assert_eq!(out.series[0].nan_kinds, vec![4]);
    }

    /// Filtering away the only run that owns x = 0 must also drop the zero
    /// slot, flipping the renderer's zero-present rule (log(x+1) -> log(x))
    /// in the same view — the transform is derived from the rendered model,
    /// never copied from the superset. A kept run whose step-0 sample is a
    /// MARKER still owns the slot, so the shift correctly survives there.
    #[test]
    fn filter_drops_zero_slot_with_its_owner() {
        let resp = DenseChart {
            x_values: vec![0.0, 1.0, 2.0],
            xr_min: vec![f64::NAN; 3],
            xr_max: vec![f64::NAN; 3],
            series: vec![
                dense("z", vec![5.0, f64::NAN, f64::NAN], vec![], vec![]), // owns step 0
                dense("a", vec![f64::NAN, 1.0, 2.0], vec![], vec![]),
            ],
        };
        let out = filter_response(&resp, &["a".to_string()].into());
        assert_eq!(
            out.x_values,
            vec![1.0, 2.0],
            "zero slot leaves with its owner; positions stay real x"
        );
        assert_ne!(
            out.x_values.first(),
            Some(&0.0),
            "the renderer's zero-present rule flips to plain log"
        );

        // Marker ownership: run a logged a non-finite value AT step 0 — the
        // slot stays, and so does the shifted rendering (matching the shifted
        // bucket geometry).
        let resp2 = DenseChart {
            x_values: vec![0.0, 1.0, 2.0],
            xr_min: vec![f64::NAN; 3],
            xr_max: vec![f64::NAN; 3],
            series: vec![
                dense("z", vec![5.0, f64::NAN, f64::NAN], vec![], vec![]),
                dense("a", vec![f64::NAN, 1.0, 2.0], vec![0], vec![1]),
            ],
        };
        let out2 = filter_response(&resp2, &["a".to_string()].into());
        assert_eq!(
            out2.x_values.first(),
            Some(&0.0),
            "a marker at step 0 keeps the slot and the shift"
        );
        assert_eq!(out2.series[0].nan_indices, vec![0]);
    }

    /// A run with no plottable x marks column 0 without owning it: the filter keeps only its kept neighbours' columns and re-lands the marker on the first of them, or leaves just its count when none remain (the server's all-unplottable answer).
    #[test]
    fn filter_relands_an_anchorless_marker_on_the_first_kept_column() {
        let mut anchorless = dense("n", vec![f64::NAN; 3], vec![0], vec![4]);
        anchorless.xnan_count = 3;
        let resp = DenseChart {
            x_values: vec![1.0, 2.0, 3.0],
            xr_min: vec![f64::NAN; 3],
            xr_max: vec![f64::NAN; 3],
            series: vec![
                anchorless,
                dense("b", vec![5.0, f64::NAN, f64::NAN], vec![], vec![]),
                dense("c", vec![f64::NAN, 6.0, 7.0], vec![], vec![]),
            ],
        };
        let out = filter_response(&resp, &["n".to_string(), "c".to_string()].into());
        assert_eq!(out.x_values, vec![2.0, 3.0], "b's column leaves with b");
        assert_eq!(out.series[0].nan_indices, vec![0]);
        assert_eq!(out.series[0].nan_kinds, vec![4]);

        let alone = filter_response(&resp, &["n".to_string()].into());
        assert!(alone.x_values.is_empty());
        assert!(alone.series[0].nan_indices.is_empty());
        assert_eq!(alone.series[0].xnan_count, 3, "the panel notice's count");

        // A kind-4 marker on a run's own finite column is an ordinary slot owner.
        let anchored = DenseChart {
            series: vec![
                dense("b", vec![5.0, f64::NAN, f64::NAN], vec![0], vec![4]),
                dense("c", vec![f64::NAN, 6.0, 7.0], vec![], vec![]),
            ],
            ..resp.clone()
        };
        let out = filter_response(&anchored, &["b".to_string()].into());
        assert_eq!(out.x_values, vec![1.0]);
        assert_eq!(out.series[0].nan_indices, vec![0]);
    }

    #[test]
    fn splice_rebuilds_the_full_model_and_refuses_violations() {
        let cached = DenseChart {
            x_values: vec![0.0, 1.0, 2.0],
            xr_min: vec![f64::NAN; 3],
            xr_max: vec![f64::NAN; 3],
            series: vec![
                dense("a", vec![1.0, 2.0, 3.0], vec![0], vec![1]),
                dense("b", vec![4.0, 5.0, 6.0], vec![], vec![]),
            ],
        };
        // The truth the delta reconstructs to: appended slot 3, a new complete series c interleaved in request order.
        let truth = DenseChart {
            x_values: vec![0.0, 1.0, 2.0, 2.5],
            xr_min: vec![f64::NAN; 4],
            xr_max: vec![f64::NAN; 4],
            series: vec![
                dense("a", vec![1.0, 2.0, 3.0, 9.0], vec![0, 3], vec![1, 4]),
                dense("c", vec![7.0, 7.0, 7.0, 7.0], vec![], vec![]),
                dense("b", vec![4.0, 5.0, 6.0, 8.0], vec![], vec![]),
            ],
        };
        // The wire pairing is positional over CONTINUING series: a (cached 0), c (complete), b (cached 1).
        let delta = {
            let mut d = delta_of(&truth, 3, &[false, true, false]);
            d.splice_from_cached = vec![0, -1, 1];
            d
        };
        let out = splice_response(&cached, &delta).unwrap();
        assert!(bits_eq(&out, &truth));
        // held marker below from_col kept, the delta's absolute marker appended
        assert_eq!(out.series[0].nan_indices, vec![0, 3]);
        assert_eq!(out.series[0].nan_kinds, vec![1, 4]);

        // Protocol violations refuse — None means the caller refetches in full.
        for breaker in [
            (|d: &mut ChartResponse| d.from_col = 0) as fn(&mut ChartResponse),
            |d| d.from_col = 4,
            |d| d.splice_from_cached = vec![1, -1, 0],
            |d| d.splice_from_cached = vec![0, -1, 5],
            |d| d.splice_from_cached = vec![0, -1],
            |d| d.x_values = vec![1.0], // seam not ascending: tail ≤ kept prefix's last x
            |d| d.result_x_hash = d.result_x_hash.map(|h| h ^ 1),
            |d| {
                d.result_series_hashes[1] ^= 1;
            },
            |d| d.result_x_hash = None, // hashes are mandatory now
            // A held series nothing continues: every cached series must be consumed.
            |d| {
                d.series.pop();
                d.splice_from_cached = vec![0, -1];
                d.result_series_hashes.pop();
            },
            // Malformed segments: a start below from_col.
            |d| d.series[0].seg_starts[0] = 1,
        ] {
            let mut d = delta.clone();
            breaker(&mut d);
            assert!(splice_response(&cached, &d).is_none());
        }
    }

    #[test]
    fn splice_materializes_new_families_and_derives_bands() {
        // The cached series has no envelope (no finite sample yet) on a banded chart (the OTHER series is enveloped); the delta's tail brings its first finite samples with a band entry. The spliced prefix materializes as NaN, and slots without band entries derive min == max == value.
        let cached = DenseChart {
            x_values: vec![0.0, 1.0, 2.0],
            xr_min: vec![f64::NAN; 3],
            xr_max: vec![f64::NAN; 3],
            series: vec![
                DenseSeries {
                    label: "a".into(),
                    run_id: "a".into(),
                    values: vec![f64::NAN, f64::NAN, f64::NAN],
                    ..Default::default()
                },
                DenseSeries {
                    label: "e".into(),
                    run_id: "e".into(),
                    values: vec![1.0, 1.5, 2.0],
                    min_values: vec![0.5, 1.5, 2.0],
                    max_values: vec![1.5, 1.5, 2.0],
                    ..Default::default()
                },
            ],
        };
        let truth = DenseChart {
            x_values: vec![0.0, 1.0, 2.0, 3.0],
            xr_min: vec![f64::NAN, f64::NAN, f64::NAN, 2.75],
            xr_max: vec![f64::NAN, f64::NAN, f64::NAN, 3.25],
            series: vec![
                DenseSeries {
                    label: "a".into(),
                    run_id: "a".into(),
                    values: vec![f64::NAN, f64::NAN, f64::NAN, 5.0],
                    min_values: vec![f64::NAN, f64::NAN, f64::NAN, 4.0],
                    max_values: vec![f64::NAN, f64::NAN, f64::NAN, 6.0],
                    ..Default::default()
                },
                DenseSeries {
                    label: "e".into(),
                    run_id: "e".into(),
                    values: vec![1.0, 1.5, 2.0, 7.0],
                    min_values: vec![0.5, 1.5, 2.0, 7.0],
                    max_values: vec![1.5, 1.5, 2.0, 7.0],
                    ..Default::default()
                },
            ],
        };
        let delta = delta_of(&truth, 3, &[false, false]);
        let out = splice_response(&cached, &delta).unwrap();
        assert!(bits_eq(&out, &truth));
        assert!(out.series[0].min_values[..3].iter().all(|v| v.is_nan()));
        assert_eq!(out.series[0].min_values[3], 4.0);
        assert_eq!(
            (out.series[1].min_values[3], out.series[1].max_values[3]),
            (7.0, 7.0),
            "derived band pinches to the value"
        );

        // A raw family the cached series carries vanishing from a tail that has values is a violation: refuse, refetch.
        let mut cached2 = cached.clone();
        cached2.series[0].values = vec![1.0, 2.0, 3.0];
        cached2.series[0].raw_values = vec![1.0, 2.0, 3.0];
        let mut d2 = delta_of(&truth, 3, &[false, false]);
        d2.series[0].raw_values = vec![];
        assert!(splice_response(&cached2, &d2).is_none());
    }

    #[test]
    fn inflate_roundtrips_a_full_response() {
        let truth = DenseChart {
            x_values: vec![0.0, 1.5, 3.0],
            xr_min: vec![f64::NAN, 1.0, f64::NAN],
            xr_max: vec![f64::NAN, 2.0, f64::NAN],
            series: vec![DenseSeries {
                label: "a".into(),
                run_id: "a".into(),
                values: vec![10.0, 25.0, 40.0],
                min_values: vec![10.0, 20.0, 40.0],
                max_values: vec![10.0, 30.0, 40.0],
                nan_indices: vec![2],
                nan_kinds: vec![2],
                ..Default::default()
            }],
        };
        let full = ChartResponse {
            x_values: truth.x_values.clone(),
            series: truth.series.iter().map(|s| wire_series(s, 0)).collect(),
            banded: true,
            xr_seg_starts: vec![1],
            xr_seg_lens: vec![1],
            xr_min: vec![1.0],
            xr_max: vec![2.0],
            ..Default::default()
        };
        // The wire is lean: only the mid slot's band differs from the value.
        assert_eq!(full.series[0].band_seg_starts, vec![1]);
        assert_eq!(full.series[0].values.len(), 3);
        let out = inflate_response(&full).unwrap();
        assert!(bits_eq(&out, &truth));
        // Deltas and malformed segments refuse.
        let mut d = full.clone();
        d.delta = true;
        assert!(inflate_response(&d).is_none());
        let mut m = full.clone();
        m.series[0].seg_lens[0] = 9;
        assert!(inflate_response(&m).is_none());
        // Stray band values on a band-less chart refuse too, even without segments.
        let mut b = full.clone();
        b.banded = false;
        b.series[0].band_seg_starts = vec![];
        b.series[0].band_seg_lens = vec![];
        assert!(
            inflate_response(&b).is_none(),
            "band values without segments on a band-less series"
        );
    }

    /// A chart with no plottable point ships its series' unplottable counts over an empty axis: it inflates to a column-less model and never offers continuation state.
    #[test]
    fn unplottable_counts_inflate_without_columns_and_never_echo() {
        let full = ChartResponse {
            series: vec![
                ChartSeries {
                    label: "a".into(),
                    run_id: "a".into(),
                    xnan_count: 3,
                    ..Default::default()
                },
                ChartSeries {
                    label: "b".into(),
                    run_id: "b".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let out = inflate_response(&full).unwrap();
        assert!(out.x_values.is_empty());
        let counts: Vec<u32> = out.series.iter().map(|s| s.xnan_count).collect();
        assert_eq!(counts, vec![3, 0]);
        assert!(out.series.iter().all(|s| s.nan_indices.is_empty()));
        let entry = ChartCacheEntry {
            request: req(&["a", "b"], 500),
            response: Rc::new(out),
            data_seq: 1,
            versions: Rc::new(HashMap::new()),
            metrics_gen: Rc::new(HashMap::new()),
            epoch: 0,
            frontiers: Rc::new([("a\u{1f}loss".to_string(), 5i64)].into()),
            noncontrib: Rc::new(HashSet::new()),
        };
        assert!(echo_state(&entry, &req(&["a", "b"], 500), 0).is_none());
    }

    #[test]
    fn echo_requires_shared_refs_in_shared_order() {
        let entry = ChartCacheEntry {
            request: req(&["a", "b"], 500),
            response: Rc::new(DenseChart {
                x_values: vec![0.0, 1.0],
                ..Default::default()
            }),
            data_seq: 1,
            versions: Rc::new(HashMap::new()),
            metrics_gen: Rc::new(HashMap::new()),
            epoch: 0,
            frontiers: Rc::new([("a\u{1f}loss".to_string(), 5i64)].into()),
            noncontrib: Rc::new(HashSet::new()),
        };
        // Same set, growth (appended or interleaved new refs): echo, held series count riding along.
        let cs = echo_state(&entry, &req(&["a", "b"], 500), 0).unwrap();
        assert_eq!(
            cs.held_series,
            Some(0),
            "the membership gate's ground truth rides every echo"
        );
        assert!(echo_state(&entry, &req(&["a", "b", "c"], 500), 0).is_some());
        assert!(echo_state(&entry, &req(&["a", "c", "b"], 500), 0).is_some());
        // Shared refs reordered: the positional pairing would mispair.
        assert!(echo_state(&entry, &req(&["b", "a"], 500), 0).is_none());
        // A shared (run, metric) whose tags changed: same frontier key, different series.
        let mut tagged = req(&["a", "b"], 500);
        tagged.y_series[0].tags = vec!["t".into()];
        assert!(echo_state(&entry, &tagged, 0).is_none());
        // Different params never echo — the log toggle included.
        assert!(echo_state(&entry, &req(&["a", "b"], 800), 0).is_none());
        let mut logd = req(&["a", "b"], 500);
        logd.log_buckets = true;
        assert!(echo_state(&entry, &logd, 0).is_none());
        // Reconnect/resync may land on a different server build. Keep painting the cache, but make its first subsequent query full.
        assert!(echo_state(&entry, &req(&["a", "b"], 500), 1).is_none());
    }

    #[test]
    fn version_key_ignores_excluded_runs_until_rearmed() {
        let bound = vec!["a".to_string(), "b".to_string()];
        let mut versions: HashMap<String, u64> = [("a".into(), 3u64), ("b".into(), 7u64)].into();
        let mg: HashMap<String, u64> = HashMap::new();
        let nc: HashSet<String> = ["b".to_string()].into();

        let base = panel_version_key(1, &bound, &mg, &versions, Some(&nc));
        // The excluded run bumping is invisible…
        versions.insert("b".into(), 8);
        assert_eq!(
            panel_version_key(1, &bound, &mg, &versions, Some(&nc)),
            base
        );
        // …a kept run bumping is not…
        versions.insert("a".into(), 4);
        assert_ne!(
            panel_version_key(1, &bound, &mg, &versions, Some(&nc)),
            base
        );
        versions.insert("a".into(), 3);
        // …and the excluded run's registry event or a resync epoch re-arm.
        let mg2: HashMap<String, u64> = [("b".into(), 1u64)].into();
        assert_ne!(
            panel_version_key(1, &bound, &mg2, &versions, Some(&nc)),
            base
        );
        assert_ne!(
            panel_version_key(2, &bound, &mg, &versions, Some(&nc)),
            base
        );

        // With nothing excluded the key is the plain bound-runs key: the
        // epoch must NOT leak in (a reconnect can't refetch settled panels).
        let plain = versions_key(0, bound.iter().map(String::as_str), &versions);
        assert_eq!(panel_version_key(1, &bound, &mg, &versions, None), plain);
        assert_eq!(
            panel_version_key(2, &bound, &mg, &versions, Some(&HashSet::new())),
            plain
        );
    }

    #[test]
    fn fresh_for_distinguishes_contributing_from_noncontributing() {
        let entry = ChartCacheEntry {
            request: req(&["a", "b"], 500),
            response: Rc::new(DenseChart::default()),
            data_seq: 1,
            versions: Rc::new([("a".into(), 3u64)].into()),
            metrics_gen: Rc::new([("b".into(), 2u64)].into()),
            epoch: 7,
            frontiers: Rc::new(HashMap::new()),
            noncontrib: Rc::new(["b".to_string()].into()),
        };
        let versions: HashMap<String, u64> = [("a".into(), 3u64), ("b".into(), 99u64)].into();
        let mg: HashMap<String, u64> = [("b".into(), 2u64)].into();
        let runs = || ["a", "b"].into_iter();

        // The non-contributing run's version is irrelevant; its registry
        // counter and the epoch are what re-arm it.
        assert!(entry.fresh_for(runs(), &versions, &mg, 7));
        assert!(!entry.fresh_for(runs(), &versions, &HashMap::new(), 7));
        assert!(!entry.fresh_for(runs(), &versions, &mg, 8));
        // A contributing run's version moving (or vanishing) invalidates.
        let moved: HashMap<String, u64> = [("a".into(), 4u64)].into();
        assert!(!entry.fresh_for(runs(), &moved, &mg, 7));
        // Epoch only matters when a non-contributing run is in scope.
        let moved_b_only = || ["a"].into_iter();
        assert!(entry.fresh_for(moved_b_only(), &versions, &HashMap::new(), 12));
    }
}
