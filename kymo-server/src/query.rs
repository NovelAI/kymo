use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tonic::{Request, Response, Status};
use tracing::instrument;

use crate::chart;
use crate::chart_delta::{
    self, SMOOTHING_STATE_SERIES_KEY, SMOOTHING_STATE_VERSION, SMOOTHING_STATE_VERSION_KEY,
};
#[cfg(test)]
use crate::clickhouse::VersionedRawPoint;
use crate::clickhouse::{CdnKeyBatchRow, ChClient, RefreshDetach};
use crate::ingest::{
    is_reserved_project_id, storable_ident, BumpCoalescer, MAX_ID_BYTES, MAX_METRIC_NAME_BYTES,
    RESERVED_PROJECT_ID,
};
use crate::lifecycle::{LifecycleGates, ReadGuards, RunKey};
use crate::liveness::{PRESUMED_DEAD_WINDOW_MS, RUNNING_WINDOW_MS};
use crate::pg::{
    InitRunError, PgStore, RestoreMutationKind, RunAccessError, RunInfoRow, RunLifecycleClass,
    RunRecordRow, TimedRunRows, TrashListQuery, TrashMutationKind, TrashPageCursor,
    TRASH_RETENTION_MS,
};
use crate::proto;
use crate::proto::smoothing_config::Algorithm;
use crate::series_cache::{EvictedSize, SeriesKey, SeriesSnapshot};

#[cfg(test)]
mod alloc_probe;
mod budget;
mod held;
mod lineage;
mod memo;
use lineage::{inspect_lineages, stamp_lineages, VerifiedLineages};

const LIFECYCLE_GATE_TIMEOUT: Duration = Duration::from_secs(15);
const LIFECYCLE_DB_TIMEOUT: Duration = Duration::from_secs(30);
const LIFECYCLE_OPERATION_TIMEOUT: Duration = Duration::from_secs(120);
const LIST_TRASH_DEFAULT_PAGE_SIZE: usize = 100;
const LIST_TRASH_MAX_PAGE_SIZE: usize = 250;
const LIST_TRASH_MAX_FILTER_IDENTITIES: usize = 1_024;
/// Most run ids (PollVersions, ListRunSetMetrics, TrashRuns), series refs (QueryCdnKeys) or metric names (QueryTextWindow) one request may list, checked before any other work on the list.
/// Production's largest dashboard sends about 10k run ids per request, a tenth of this; a 4 MiB message holds about 110k 36-byte UUID run ids, so a dashboard of kymo-generated ids reaches the message cap first; the browser trashes 256 runs per request.
/// Past it, 16 PollVersions of 2 million empty run ids in flight (4 KB each compressed) held about 85 MiB each through the query, and QueryCdnKeys of 2 million empty refs about 400 MiB each; at the bound, a PollVersions of short distinct ids holds about 6 MiB.
const MAX_REQUEST_RUN_IDS: usize = crate::ws_rpc::MAX_REQUEST_RUN_IDS;
const MAX_RUN_NAME_BYTES: usize = 2_048;
const MAX_TEXT_WINDOW_LINES: u32 = 2_000;
const MAX_TEXT_WINDOW_METRICS: usize = 256;
const MAX_TEXT_SEARCH_BYTES: usize = 512;
/// The process-wide count of concurrent chart scans (KYMO_CHART_INFLIGHT_SERIES). A request takes a unit per Y ref (runs × bindings after frontend resolution) and per distinct custom-X run, or every unit when it is wider; docs/chart-request-latency.md (stage 2, step 2) says when it returns them.
const DEFAULT_CHART_INFLIGHT_SERIES: usize = 32;
/// The per-request bound ([`ChartAdmission::check_width`]), independent of KYMO_CHART_INFLIGHT_SERIES, since a request wider than that count takes every unit and retains every result at once. 128 covers two editor-max 64-run Y bindings, or one plus custom X; more bindings, and persisted max_runs=0, are rejected. It counts raw reads, not bytes: the byte budget bounds a pathological series.
const MAX_CHART_REQUEST_RAW_SERIES: usize = 128;
/// A tagged ref forms at most this many series; [`prepare`] refuses a request whose ref selects more tag groups, so stage 2 prices such a ref at nothing ([`ChartPlan::prepared_rows`]).
const MAX_TAG_GROUPS: usize = 64;
/// The cells a log-ladder grid can hold beyond the resolution: its coarsest ladder has one cell per octave, of which f64 x span at most 1,524 (chart.rs `make_grid_from`; octaves from 2^-500 up).
const LOG_GRID_OCTAVES: usize = 1_524;
/// `acquire_many_owned` accepts a u32 count while Tokio's semaphore has its own
/// platform limit. This is a primitive boundary, not a product safety cap.
const MAX_CHART_INFLIGHT_SERIES: usize = if Semaphore::MAX_PERMITS < u32::MAX as usize {
    Semaphore::MAX_PERMITS
} else {
    u32::MAX as usize
};
const MAX_CHART_FETCH_CONCURRENCY: usize = 16;
/// Below the frontend's 60-second request deadline. Covers admission waiting, async database work and the wait for a build, so a stalled SELECT cannot leave an unread dispatch monopolizing the process-wide gate. A build that has started finishes on its blocking thread even after the deadline; the stable per-request ceiling bounds its fan-out.
const CHART_QUERY_TIMEOUT: Duration = Duration::from_secs(45);
/// Stage 2's byte budget (KYMO_CHART_BYTE_BUDGET_MB): the pod's read-and-build envelope (about 1.75 GiB) less a 256 MiB reserve, divided by allocator slack of 2.
const DEFAULT_CHART_BYTE_BUDGET_MB: usize = 768;
/// Concurrent chart builds (KYMO_CHART_BUILD_SLOTS), each on a blocking thread.
const DEFAULT_CHART_BUILD_SLOTS: usize = 4;
const MAX_CHART_BUILD_SLOTS: usize = 64;

fn canonicalize_chart_request(request: &mut proto::ChartRequest) -> Result<(), Status> {
    if request.use_timestamp_axis {
        request.x_series = None;
    }
    if let Some(smoothing) = request.smoothing.as_mut() {
        // prost's getter reads unknown values, retired ones included, as NONE, which would silently answer an unsmoothed chart.
        if Algorithm::try_from(smoothing.algorithm).is_err() {
            return Err(Status::invalid_argument(format!(
                "unknown smoothing algorithm {}",
                smoothing.algorithm
            )));
        }
        let tau = smoothing.time_constant;
        if smoothing.algorithm() == Algorithm::Ema && !(tau.is_finite() && tau > 0.0) {
            return Err(Status::invalid_argument(
                "EMA smoothing needs a finite, positive time_constant",
            ));
        }
    }
    Ok(())
}

fn custom_x_runs(request: &proto::ChartRequest) -> Option<(&proto::SeriesRef, Vec<(&str, &str)>)> {
    let x_series = request
        .x_series
        .as_ref()
        .filter(|_| !request.use_timestamp_axis)?;
    let mut seen = std::collections::HashSet::new();
    let runs = request
        .y_series
        .iter()
        .filter_map(|series| {
            seen.insert(series.run_id.as_str())
                .then_some((series.project_id.as_str(), series.run_id.as_str()))
        })
        .collect();
    Some((x_series, runs))
}

/// Distinct series `keys` grouped by (project, metric), groups and members in first-appearance order.
fn series_groups<'a>(keys: impl IntoIterator<Item = &'a SeriesKey>) -> Vec<Vec<SeriesKey>> {
    let mut groups: Vec<Vec<SeriesKey>> = Vec::new();
    for key in keys {
        match groups.iter_mut().find(|group| {
            group[0].project_id == key.project_id && group[0].metric_name == key.metric_name
        }) {
            Some(group) => group.push(key.clone()),
            None => groups.push(vec![key.clone()]),
        }
    }
    groups
}

fn chart_admission_weight(request: &proto::ChartRequest) -> usize {
    // Its Y refs alone put the request past the per-chart ceiling, which refuses it: skip the distinct-run pass over an arbitrarily long list (the refusal then counts the Y refs alone).
    if request.y_series.len() > MAX_CHART_REQUEST_RAW_SERIES {
        return request.y_series.len();
    }
    let x_series = custom_x_runs(request).map_or(0, |(_, runs)| runs.len());
    request.y_series.len() + x_series
}

struct ChartAdmission {
    slots: Arc<Semaphore>,
    limit: usize,
    bytes: Arc<budget::ByteBudget>,
    builds: budget::BuildSlots,
}

impl ChartAdmission {
    fn new(limit: usize, budget_bytes: u64, build_slots: usize) -> Self {
        metrics::gauge!("mkdb2_chart_inflight_series_limit").set(limit as f64);
        Self {
            slots: Arc::new(Semaphore::new(limit)),
            limit,
            bytes: budget::ByteBudget::new(budget_bytes),
            builds: budget::BuildSlots::new(build_slots),
        }
    }

    /// The per-request shape bound, decided at the request boundary before anything is planned or waited on: a plan costs a key per ref and a cache lookup per series.
    fn check_width(requested_weight: usize) -> Result<(), Status> {
        if requested_weight > MAX_CHART_REQUEST_RAW_SERIES {
            metrics::counter!(
                "mkdb2_chart_admission_rejections_total",
                "reason" => "request_too_wide"
            )
            .increment(1);
            return Err(Status::invalid_argument(format!(
                "chart requires {requested_weight} raw-series reads; maximum per chart is {MAX_CHART_REQUEST_RAW_SERIES}"
            )));
        }
        Ok(())
    }

    async fn acquire(&self, requested_weight: usize) -> ChartAdmissionPermit {
        let weight = requested_weight.min(self.limit) as u32;
        let wait_started = std::time::Instant::now();
        let waiting = budget::GaugeHold::new("mkdb2_chart_admission_waiters");
        let permit = self
            .slots
            .clone()
            .acquire_many_owned(weight)
            .await
            .expect("the scan units' semaphore is never closed");
        drop(waiting);
        metrics::histogram!("mkdb2_chart_admission_wait_duration_seconds")
            .record(wait_started.elapsed().as_secs_f64());
        let raw_series = requested_weight as u32;
        metrics::gauge!("mkdb2_chart_inflight_series").increment(f64::from(raw_series));
        let inline_floor = if requested_weight > weight as usize {
            self.fetch_concurrency()
        } else {
            0
        };
        ChartAdmissionPermit {
            permit: std::sync::Mutex::new(permit),
            inline_floor,
            raw_series,
        }
    }

    fn fetch_concurrency(&self) -> usize {
        self.limit.min(MAX_CHART_FETCH_CONCURRENCY)
    }

    /// Admit a request (its bytes, then its scan units) and run its work, all under `deadline`. The width check ran before it ([`Self::check_width`]).
    async fn run_with_timeout<T, W, F>(
        &self,
        requested_weight: usize,
        charge: Vec<u64>,
        deadline: Duration,
        work: W,
    ) -> Result<T, Status>
    where
        W: FnOnce(Arc<budget::ChartCharge>, ChartAdmissionPermit) -> F,
        F: std::future::Future<Output = Result<T, Status>>,
    {
        match tokio::time::timeout(deadline, async {
            // Bytes before units, and neither while holding a lease, a run guard or a pinned snapshot. The work owns its units until its reads finish (detached refreshes split units off them) and its charge until its build does; while the future is awaiting IO, a timeout drops the outstanding SELECTs before releasing them.
            let charge = self.bytes.reserve(charge).await;
            let permit = self.acquire(requested_weight).await;
            work(charge, permit).await
        })
        .await
        {
            Ok(result) => result,
            Err(_) => {
                metrics::counter!("mkdb2_chart_admission_rejections_total", "reason" => "deadline")
                    .increment(1);
                Err(Status::deadline_exceeded(format!(
                    "chart query exceeded its {}ms server deadline",
                    deadline.as_millis()
                )))
            }
        }
    }
}

/// A parked task-deadline outcome means the scan is still not done;
/// deadline_exceeded is the accepted loading form and the next poll re-elects.
fn chart_fetch_status(e: crate::series_cache::RefreshError) -> Status {
    let message = format!("metric query failed: {e}");
    match e {
        crate::series_cache::RefreshError::Timeout => Status::deadline_exceeded(message),
        _ => Status::internal(message),
    }
}

struct ChartAdmissionPermit {
    /// Mutex so detached refreshes can split units off mid-request.
    permit: std::sync::Mutex<OwnedSemaphorePermit>,
    /// Units the permit must retain rather than split away (see
    /// [`Self::split_refresh_unit`]; full argument in
    /// docs/admission-control.md Stage R (a)).
    inline_floor: usize,
    /// What the request still counts in `mkdb2_chart_inflight_series`: its requested weight, less the units [`Self::keep_for_reads`] returned.
    raw_series: u32,
}

impl ChartAdmissionPermit {
    /// One unit of this request's admitted weight for a detached series refresh. Completed tasks recycle their units to the shared gate, not back to this permit, so splitting below `inline_floor` would leave the request's remaining scans unweighted; `None` keeps the refresh inline, request-bound. The inflight gauge counts a split unit until the permit drops — detached tails run on transferred units, outside it.
    fn split_refresh_unit(&self) -> Option<OwnedSemaphorePermit> {
        let mut permit = self.permit.lock().unwrap();
        if permit.num_permits() <= self.inline_floor {
            return None;
        }
        permit.split(1)
    }

    /// Units [`Self::split_refresh_unit`] can still hand out.
    fn spare_refresh_units(&self) -> usize {
        self.permit
            .lock()
            .unwrap()
            .num_permits()
            .saturating_sub(self.inline_floor)
    }

    /// After the cache lookups: keep one unit for each read the request still dispatches (a distinct series that was not a fresh hit, and each custom-X run) and return the others, the fresh hits', to the gate, the inflight gauge dropping by them. A permit with a unit for every read lets each of them split its own, so it drops the inline floor; a request clamped below its reads keeps every unit and its floor (Stage R (a)).
    fn keep_for_reads(&mut self, reads: usize) {
        let permit = self.permit.get_mut().unwrap();
        let held = permit.num_permits();
        if reads <= held {
            if held > reads {
                drop(permit.split(held - reads));
                let returned = (held - reads) as u32;
                self.raw_series -= returned;
                metrics::gauge!("mkdb2_chart_inflight_series").decrement(f64::from(returned));
            }
            self.inline_floor = 0;
        }
    }

    #[cfg(test)]
    fn units(&self) -> usize {
        self.permit.lock().unwrap().num_permits()
    }
}

impl Drop for ChartAdmissionPermit {
    fn drop(&mut self) {
        metrics::gauge!("mkdb2_chart_inflight_series").decrement(f64::from(self.raw_series));
    }
}

/// What a chart request reads, itemized as its byte charge is: each distinct Y series in first-appearance order, then each distinct custom-X run, then the build's output.
struct ChartPlan {
    series: Vec<SeriesKey>,
    /// Per series, the request indices of the refs naming it, whose tag filters ([`ChartParams::admits`]) and the step range `prepare` keeps decide the rows its transients are charged over ([`Self::prepared_rows`]).
    refs: Vec<Vec<usize>>,
    /// The request's parameters, its filter sets and axis geometry among them, which its builds take as planned ([`build_chart`]).
    params: ChartParams,
    x_runs: usize,
    /// Stage 1's key for this poll, when it could be memo-served.
    memo_key: Option<memo::MemoKey>,
    /// Held state's key for this poll, unless held state is off or the chart has custom x (stage 3b). It and `memo_key` come from one hash of the request.
    held_key: Option<held::RequestKey>,
    /// Whether held state's advance may run beside a full build, holding a second model ([`Self::output_bound`]): in `shadow` mode, or for an audited poll in `on` mode, when the store held an entry of the request as the poll was planned. One held since is charged once the build reports its model ([`held::build`]).
    beside: bool,
    /// The poll's 1-in-67 audit draw ([`QueryService::draw_audit`]), taken when it is planned, only by a poll that can audit, and kept if it starts over: an audited poll always builds in full, so step 1 reserves its transients, and prices its output with the audit's model and second prepare if it has a prefix to check.
    audit: bool,
    /// Whether the poll's build holds the audit's reconstructed model and second prepare: an audited poll that echoes cache state on a standard axis, the only kind whose build has a prefix to check ([`build_chart`]'s `from_col`; custom x never continues).
    audits_prefix: bool,
    /// Whether step 1 reserves build transients: not when, unaudited, a memo is stored for the key, or (held state `on`) held state is stored for the request, since either usually answers without building; a poll that does build charges them first if they fit now, else starts over with them ([`Self::restart`]).
    transients: bool,
    /// Step 1 skipped the transients for held state (stage 3b), reserving what its advance holds beside its model's working copies ([`held::HeldStore::hints`]).
    held_hint: Option<u64>,
    /// Per series, its snapshot's bytes alone as step 1 priced it ([`Self::estimate`]): what a detached read of it keeps charged ([`budget::ChartCharge::split`]).
    snapshots: Vec<u64>,
    /// What a build that may build in full attempts ([`held::build`]): [`held::Advance::Try`], or [`held::Advance::Skip`] once the poll started over ([`Self::restart`]) after its expected advance fell back (and counted why).
    attempt: held::Advance,
    /// Once the poll starts over, the charge its first pass came to in full: step 1's floor ([`Self::estimate`]), so its next pass reserves the sizes it now knows. Empty on the first pass, the only one that checks its overrun ([`budget::ChartCharge::overrun`]), so a poll starts over at most once.
    known: Vec<u64>,
}

impl ChartPlan {
    fn new(
        req: &proto::ChartRequest,
        chart_memo: &memo::ChartMemo,
        chart_held: &held::HeldStore,
        draw: impl FnOnce() -> bool,
    ) -> Self {
        let mut series: Vec<SeriesKey> = Vec::new();
        let mut refs: Vec<Vec<usize>> = Vec::new();
        let mut index: std::collections::HashMap<SeriesKey, usize> =
            std::collections::HashMap::new();
        for (request_index, s) in req.y_series.iter().enumerate() {
            let key = SeriesKey::new(&s.project_id, &s.run_id, &s.metric_name);
            match index.get(&key) {
                Some(&i) => refs[i].push(request_index),
                None => {
                    index.insert(key.clone(), series.len());
                    series.push(key);
                    refs.push(vec![request_index]);
                }
            }
        }
        let memo_on = chart_memo.serves(req);
        let held_mode = chart_held.mode();
        let held_on = held_mode != held::HeldMode::Off && req.x_series.is_none();
        let hash = (memo_on || held_on).then(|| memo::request_hash(req));
        let memo_key = hash
            .as_ref()
            .filter(|_| memo_on)
            .map(|hash| memo::memo_key(hash, req));
        let held_key: Option<held::RequestKey> = hash
            .filter(|_| held_on)
            .map(|hash| sha2::Digest::finalize(hash).into());
        let prefix = req.x_series.is_none() && echoed_frontiers(req).is_some();
        let on = held_mode == held::HeldMode::On;
        // Only a poll that can audit draws: one with a prefix, or in `on` mode one whose advance the audit can check, from an entry the store holds.
        let audit = (prefix || held_key.is_some_and(|key| on && chart_held.holds(&key))) && draw();
        let held_hint = held_key
            .filter(|_| on && prefix && !audit)
            .and_then(|key| chart_held.hints(&key));
        let beside = held_key.is_some_and(|key| {
            (held_mode == held::HeldMode::Shadow || audit) && chart_held.holds(&key)
        });
        Self {
            series,
            refs,
            params: chart_params(req),
            x_runs: custom_x_runs(req).map_or(0, |(_, runs)| runs.len()),
            audit,
            audits_prefix: audit && prefix,
            transients: audit
                || (!memo_key.is_some_and(|key| chart_memo.holds(&key)) && held_hint.is_none()),
            held_hint,
            memo_key,
            held_key,
            beside,
            snapshots: Vec::new(),
            attempt: held::Advance::Try,
            known: Vec::new(),
        }
    }

    /// The poll starts over ([`budget::ChartCharge::overrun`] says when and why), building with `attempt` next, its first pass having come to `known`. Its plan keeps its audit draw; step 1 reserves its transients this time, at no less than `known`, and it expects no advance.
    fn restart(&mut self, attempt: held::Advance, known: Vec<u64>) {
        metrics::counter!("mkdb2_chart_budget_restarts_total").increment(1);
        #[cfg(test)]
        chart_admission_tests::RESTARTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.transients = true;
        self.held_hint = None;
        self.attempt = attempt;
        self.known = known;
    }

    /// What a build holds beyond its series' transients ([`Self::transient_bytes`] over their prepared rows) once its dense model exists: the model and its wire copy, at a measured cost per cell (series × shared-axis columns), and its largest series' transients again (prepare's working copies, before the model); with the audit, a reconstructed model and a second prepare's transients alongside; and held state's further models and bytes.
    fn output_bytes(shape: DenseShape, audit: bool) -> u64 {
        let cells = shape.series as u64 * shape.columns as u64;
        let model = cells * budget::DENSE_BYTES_PER_CELL;
        let mut build = model + Self::transient_bytes(shape.largest_series);
        if audit {
            build += model + Self::transient_bytes(shape.prepared_rows + shape.largest_series);
        }
        build + model * (shape.models - 1) + shape.extra
    }

    /// Build transients over `rows` prepared rows.
    fn transient_bytes(rows: usize) -> u64 {
        rows as u64 * budget::BUILD_BYTES_PER_ROW
    }

    fn index_of(&self, key: &SeriesKey) -> usize {
        self.series
            .iter()
            .position(|k| k == key)
            .expect("every read series is planned")
    }

    /// The most a build of what it keeps holds beyond its series' transients before its model exists, reserved so: [`Self::output_bytes`] over the most columns its chart can have ([`columns_bound`]), corrected to the model's real shape once it exists.
    fn output_bound(&self, kept: Kept) -> u64 {
        Self::output_bytes(
            DenseShape {
                series: kept.series,
                columns: columns_bound(self.params.spec, kept),
                prepared_rows: kept.rows,
                largest_series: kept.largest,
                models: 1 + u64::from(self.beside),
                extra: 0,
            },
            self.audits_prefix,
        )
    }

    /// The bytes series `index`, in hand, pins and builds: its whole snapshot at the cache's accounting (a pin holds all of it) and, with `transients`, transients over the rows the request keeps of it ([`Self::prepared_rows`]), which it also returns.
    fn pinned(&self, index: usize, rows: &SeriesSnapshot, transients: bool) -> (u64, Kept) {
        let kept = if transients {
            self.prepared_rows(index, rows)
        } else {
            Kept::NONE
        };
        (rows.bytes() as u64 + Self::transient_bytes(kept.rows), kept)
    }

    /// What `prepare` keeps of series `index` over every ref naming it: for each ref, the rows in the fetch range of the tag groups it selects ([`selected_groups`]), each group one series, and nothing of a ref selecting more than [`MAX_TAG_GROUPS`], since `prepare` refuses it before collecting any. Each tag's rows in range are found once, by a binary search over its chunks.
    fn prepared_rows(&self, index: usize, rows: &SeriesSnapshot) -> Kept {
        let (lo, hi) = (self.params.fetch_min, self.params.fetch_max);
        let groups: Vec<(&str, Kept)> = rows
            .tags()
            .filter_map(|tag| {
                let mut group = Kept {
                    series: 1,
                    ..Kept::NONE
                };
                for chunk in tag.between(lo, hi) {
                    if let (Some(first), Some(last)) = (chunk.first(), chunk.last()) {
                        group.rows += chunk.len();
                        group.steps = (group.steps.0.min(first.step), last.step);
                    }
                }
                group.largest = group.rows;
                (group.rows > 0).then_some((tag.tag(), group))
            })
            .collect();
        self.refs[index]
            .iter()
            .map(|&r| selected_groups(groups.iter().map(|&(tag, _)| tag), self.params.admits(r)).0)
            .filter(|picked| picked.len() <= MAX_TAG_GROUPS)
            .flatten()
            .fold(Kept::NONE, |kept, i| kept.join(groups[i].1))
    }

    /// What `prepare` keeps of evicted series `index`, priced from its recovery `record` ([`EvictedSize`]): the share of its rows the fetch range spans at their mean density (all of them for an unzoomed chart), spread evenly over its groups. Each ref naming it keeps every group, or as many as the tags it lists, and nothing where that is more than [`MAX_TAG_GROUPS`], which `prepare` refuses, as [`Self::prepared_rows`] does; each group it keeps forms a series. A series that grew since, a range over denser steps than its average, a listed tag it never held, or groups larger than their mean, adds what its read corrects.
    fn evicted_rows(&self, index: usize, record: EvictedSize) -> Kept {
        let EvictedSize {
            rows,
            steps,
            groups,
            ..
        } = record;
        let (lo, hi) = (
            self.params.fetch_min.max(steps.0),
            self.params.fetch_max.min(steps.1),
        );
        // A record that held nothing in the range (an empty one's steps are an empty range) keeps no series: priced as any, it would add groups that take the chart's columns ([`Self::output_bound`]).
        if lo > hi {
            return Kept::NONE;
        }
        // u128: a span of every i64 step is 2^64 steps.
        let span = u128::from(steps.1.abs_diff(steps.0)) + 1;
        let share = u128::from(hi.abs_diff(lo)) + 1;
        let kept = (rows as u128 * share).div_ceil(span);
        // A record with rows in range has a group; an inconsistent one is priced wrong, not divided by zero in a poll.
        let groups = groups.max(1);
        self.refs[index]
            .iter()
            .map(|&r| {
                let listed = self.params.filters.as_ref().map_or(0, |f| f[r].len());
                let picked = if listed == 0 {
                    groups
                } else {
                    listed.min(groups)
                };
                if picked > MAX_TAG_GROUPS {
                    return Kept::NONE;
                }
                Kept {
                    rows: (kept * picked as u128).div_ceil(groups as u128) as usize,
                    largest: kept.div_ceil(groups as u128) as usize,
                    series: picked,
                    steps: (lo, hi),
                }
            })
            .fold(Kept::NONE, Kept::join)
    }

    /// Step 1's charge, reserved before any lookup pins anything and priced outside the cache's lock from the snapshots `sizes` pinned (dropped before step 1 waits): a retained series at [`Self::pinned`]; an evicted one at the bytes its recovery record's snapshot charged and, with transients, at what its record says the request keeps of it ([`Self::evicted_rows`]); any other miss as the prod-average series, with transients over all its rows for every ref naming it (an upper bound on the rows a range keeps).
    ///
    /// Custom-X runs at that series' points; the output at its bound over the priced series ([`Self::output_bound`]), or for a held hint at what the advance holds beside its working copies.
    ///
    /// Once the poll started over, each item at no less than its first pass came to ([`Self::known`]). Records each series' snapshot bytes ([`Self::snapshots`]).
    fn estimate(&mut self, sizes: &[crate::series_cache::CachedSize]) -> Vec<u64> {
        use crate::series_cache::{snapshot_bytes, CachedSize};
        self.snapshots = sizes
            .iter()
            .map(|size| match size {
                CachedSize::Cached(rows) => rows.bytes() as u64,
                CachedSize::Evicted(record) => record.bytes as u64,
                CachedSize::Unknown => snapshot_bytes(budget::MISS_ESTIMATE_ROWS as usize) as u64,
            })
            .collect();
        let mut kept = Kept::NONE;
        let mut items: Vec<u64> = sizes
            .iter()
            .zip(&self.snapshots)
            .enumerate()
            .map(|(index, (size, &snapshot))| {
                let series = if self.transients {
                    match size {
                        CachedSize::Cached(rows) => self.prepared_rows(index, rows),
                        CachedSize::Evicted(record) => self.evicted_rows(index, *record),
                        CachedSize::Unknown => {
                            let rows = budget::MISS_ESTIMATE_ROWS as usize;
                            let refs = self.refs[index].len();
                            Kept {
                                rows: rows * refs,
                                largest: rows,
                                series: refs,
                                steps: (i64::MIN, i64::MAX),
                            }
                        }
                    }
                } else {
                    Kept::NONE
                };
                kept = kept.join(series);
                snapshot + Self::transient_bytes(series.rows)
            })
            .collect();
        items.extend(std::iter::repeat_n(
            budget::MISS_ESTIMATE_ROWS * budget::X_BYTES_PER_ROW,
            self.x_runs,
        ));
        items.push(if self.transients {
            self.output_bound(kept)
        } else {
            self.held_hint.unwrap_or(0)
        });
        for (item, &known) in items.iter_mut().zip(&self.known) {
            *item = (*item).max(known);
        }
        items
    }
}

/// What `prepare` keeps of one or more series as stage 2 prices it ([`ChartPlan::prepared_rows`]): their rows, the most one series keeps, the series formed and the lowest and highest of their steps.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Kept {
    rows: usize,
    largest: usize,
    series: usize,
    steps: (i64, i64),
}

impl Kept {
    const NONE: Self = Self {
        rows: 0,
        largest: 0,
        series: 0,
        steps: (i64::MAX, i64::MIN),
    };

    fn join(self, other: Self) -> Self {
        Self {
            rows: self.rows + other.rows,
            largest: self.largest.max(other.largest),
            series: self.series + other.series,
            steps: (
                self.steps.0.min(other.steps.0),
                self.steps.1.max(other.steps.1),
            ),
        }
    }
}

/// The most columns a chart can have over what its build keeps (chart.rs `shared_chart`): one per distinct x at most, so per row and, on a step axis, per step its rows span; and with a resolution, a chart that passes through holds at most the resolution's x, or on a linear step axis whose cells would be under four steps wide the steps of a span of at most twice the resolution, and one that downsamples at most three slots per cell (an envelope's one, or the up to three x of a cell short of an envelope), of at most the resolution's cells and two more at its edges, or on a log axis one cell per octave when that is more ([`LOG_GRID_OCTAVES`]).
fn columns_bound(spec: chart::GridSpec, kept: Kept) -> usize {
    let mut columns = kept.rows;
    if spec.is_step_axis && kept.steps.0 <= kept.steps.1 {
        let span = usize::try_from(kept.steps.1.abs_diff(kept.steps.0)).unwrap_or(usize::MAX);
        columns = columns.min(span.saturating_add(1));
    }
    if spec.target > 0 {
        let resolution = if spec.is_step_axis && !spec.log_buckets {
            2 * spec.target + 2
        } else if spec.log_buckets {
            3 * (spec.target.max(LOG_GRID_OCTAVES) + 2)
        } else {
            3 * (spec.target + 2)
        };
        columns = columns.min(resolution);
    }
    columns
}

/// The tag groups that become a ref's series, given the tags of the groups with rows in range, ascending: with any tagged group, the tagged ones (they win over stray scalar rows) the ref's filter `admits`; else the scalar group. Their positions, and whether they are tagged.
pub(super) fn selected_groups<'a>(
    tags: impl DoubleEndedIterator<Item = &'a str> + ExactSizeIterator + Clone,
    admits: impl Fn(&str) -> bool,
) -> (Vec<usize>, bool) {
    if tags.clone().next_back().is_some_and(|tag| !tag.is_empty()) {
        let picked = tags
            .enumerate()
            .filter_map(|(i, tag)| (!tag.is_empty() && admits(tag)).then_some(i))
            .collect();
        (picked, true)
    } else {
        (tags.len().checked_sub(1).into_iter().collect(), false)
    }
}

/// Liveness state machine. Pure function of timestamps + the explicit exit
/// signal — same inputs always produce the same status, so polling clients
/// (frontend, slack bot) see consistent state across pulls.
///
/// Fallbacks:
/// - No metrics ever logged → use `created_at_ms` so a brand-new run reads as
///   RUNNING for the first 10 seconds.
/// - System metrics never logged (user disabled the poller) → fall back to the
///   main metric timestamp so the run doesn't get stuck in PRESUMED_DEAD; STUCK
///   becomes unreachable in this mode, which is the correct degenerate behavior.
fn compute_status(
    now_ms: i64,
    created_at_ms: i64,
    last_main_metric_at_ms: Option<i64>,
    last_system_metric_at_ms: Option<i64>,
    exit_code: Option<i32>,
) -> proto::RunStatus {
    if let Some(code) = exit_code {
        return if code == 0 {
            proto::RunStatus::Finished
        } else {
            proto::RunStatus::Crashed
        };
    }

    let main_at = last_main_metric_at_ms.unwrap_or(created_at_ms);
    let sys_at = last_system_metric_at_ms.unwrap_or(main_at);

    let main_age = (now_ms - main_at).max(0);
    let sys_age = (now_ms - sys_at).max(0);

    if main_age < RUNNING_WINDOW_MS && sys_age < RUNNING_WINDOW_MS {
        proto::RunStatus::Running
    } else if sys_age < RUNNING_WINDOW_MS {
        proto::RunStatus::Stuck
    } else if sys_age < PRESUMED_DEAD_WINDOW_MS {
        proto::RunStatus::Unresponsive
    } else {
        proto::RunStatus::PresumedDead
    }
}

#[cfg(test)]
mod liveness_status_tests {
    use super::*;

    #[test]
    fn resumed_main_baseline_provides_grace_without_requiring_system_metrics() {
        let old_created = 1_000;
        let resumed_at = old_created + PRESUMED_DEAD_WINDOW_MS + 1;
        assert_eq!(
            compute_status(resumed_at, old_created, None, None, None),
            proto::RunStatus::PresumedDead
        );
        assert_eq!(
            compute_status(resumed_at, old_created, Some(resumed_at), None, None),
            proto::RunStatus::Running
        );

        let later_main = resumed_at + RUNNING_WINDOW_MS;
        assert_eq!(
            compute_status(later_main, old_created, Some(later_main), None, None),
            proto::RunStatus::Running
        );
    }

    #[test]
    fn run_lists_use_the_database_clock_that_authored_liveness_timestamps() {
        let database_now_ms = 1_000;
        let row = RunInfoRow {
            project_id: "project".into(),
            run_id: "run".into(),
            run_name: "run".into(),
            ordinal: 1,
            created_at_ms: database_now_ms,
            last_main_metric_at_ms: None,
            last_system_metric_at_ms: None,
            last_ingested_at_ms: None,
            terminated_at_ms: None,
            exit_code: None,
        };

        let app_clock_ahead = database_now_ms + RUNNING_WINDOW_MS + 1;
        assert_eq!(
            run_info_row_to_proto(row.clone(), app_clock_ahead).status,
            proto::RunStatus::Unresponsive as i32
        );
        let runs = timed_run_rows_to_proto(TimedRunRows {
            rows: vec![row],
            server_now_ms: database_now_ms,
        });
        assert_eq!(runs[0].status, proto::RunStatus::Running as i32);
    }
}

fn run_info_row_to_proto(row: RunInfoRow, now_ms: i64) -> proto::RunInfo {
    let status = compute_status(
        now_ms,
        row.created_at_ms,
        row.last_main_metric_at_ms,
        row.last_system_metric_at_ms,
        row.exit_code,
    );
    proto::RunInfo {
        project_id: row.project_id,
        run_id: row.run_id,
        run_name: row.run_name,
        ordinal: row.ordinal as u64,
        created_at_ms: row.created_at_ms,
        status: status as i32,
        last_ingested_at_ms: row.last_ingested_at_ms,
        terminated_at_ms: row.terminated_at_ms,
    }
}

fn timed_run_rows_to_proto(timed_rows: TimedRunRows) -> Vec<proto::RunInfo> {
    timed_rows
        .rows
        .into_iter()
        .map(|row| run_info_row_to_proto(row, timed_rows.server_now_ms))
        .collect()
}

#[cfg(test)]
mod list_runs_wire_tests {
    use super::proto;
    use prost::Message;

    #[test]
    fn list_snapshot_wire_vectors_pin_legacy_rows_and_optional_version_tags() {
        // Historical response: runs (tag 1) contains one RunInfo whose run_id (tag 2) is "run". The optional project version must remain tag 2 of the outer response.
        let legacy = b"\x0a\x05\x12\x03run";
        let mut response = proto::ListRunsResponse::decode(legacy.as_slice()).unwrap();
        assert_eq!(response.runs.len(), 1);
        assert_eq!(response.runs[0].run_id, "run");
        assert_eq!(response.project_version, None);
        for (version, expected) in [
            (0, b"\x0a\x05\x12\x03run\x10\x00"),
            (7, b"\x0a\x05\x12\x03run\x10\x07"),
        ] {
            response.project_version = Some(version);
            assert_eq!(response.encode_to_vec(), expected);
        }
    }
}

fn run_record_row_to_proto(row: RunRecordRow, now_ms: i64) -> proto::RunRecord {
    let state = match row.lifecycle_at(now_ms) {
        RunLifecycleClass::Active => proto::RunLifecycleState::Active,
        RunLifecycleClass::Trashed => proto::RunLifecycleState::Trashed,
        RunLifecycleClass::Expired => proto::RunLifecycleState::Expired,
        RunLifecycleClass::Purging => proto::RunLifecycleState::Purging,
        RunLifecycleClass::Purged | RunLifecycleClass::Missing => proto::RunLifecycleState::Unknown,
    };
    let status = compute_status(
        now_ms,
        row.created_at_ms,
        row.last_main_metric_at_ms,
        row.last_system_metric_at_ms,
        row.exit_code,
    );
    let purge_at_ms = row
        .deleted_at_ms
        .map(|deleted| deleted.saturating_add(TRASH_RETENTION_MS));
    proto::RunRecord {
        run: Some(proto::RunInfo {
            project_id: row.project_id,
            run_id: row.run_id,
            run_name: row.run_name,
            ordinal: row.ordinal as u64,
            created_at_ms: row.created_at_ms,
            status: status as i32,
            last_ingested_at_ms: row.last_ingested_at_ms,
            terminated_at_ms: row.terminated_at_ms,
        }),
        state: state as i32,
        deleted_at_ms: row.deleted_at_ms,
        purge_at_ms,
    }
}

fn lifecycle_access_status(error: RunAccessError) -> Status {
    match error {
        RunAccessError::Store(error) => {
            if error
                .downcast_ref::<sqlx::Error>()
                .is_some_and(pg_lifecycle_store_unavailable)
            {
                Status::unavailable(format!("run lifecycle lookup failed: {error}"))
            } else {
                Status::internal(format!("run lifecycle lookup failed: {error}"))
            }
        }
        RunAccessError::NotActive { key, state } | RunAccessError::NotReadable { key, state } => {
            match state {
                RunLifecycleClass::Missing | RunLifecycleClass::Purged => Status::not_found(
                    format!("run {}/{} was not found", key.project_id, key.run_id),
                ),
                _ => Status::failed_precondition(format!(
                    "run {}/{} is unavailable ({state:?})",
                    key.project_id, key.run_id
                )),
            }
        }
    }
}

fn pg_lifecycle_store_unavailable(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed => true,
        sqlx::Error::Database(error) => error
            .code()
            .is_some_and(|code| pg_availability_code(code.as_ref())),
        _ => false,
    }
}

fn pg_availability_code(code: &str) -> bool {
    // PostgreSQL emits these FATAL responses during an orderly shutdown, crash recovery, and startup before it can serve ordinary queries.
    matches!(code, "57P01" | "57P02" | "57P03")
}

#[cfg(test)]
mod lifecycle_access_status_tests {
    use std::io;

    use super::*;

    #[test]
    fn postgres_transport_and_pool_outages_are_unavailable() {
        for error in [
            sqlx::Error::PoolTimedOut,
            sqlx::Error::PoolClosed,
            sqlx::Error::Io(io::Error::new(io::ErrorKind::ConnectionReset, "reset")),
        ] {
            let status = lifecycle_access_status(RunAccessError::Store(error.into()));
            assert_eq!(status.code(), tonic::Code::Unavailable);
            assert!(status.message().starts_with("run lifecycle lookup failed:"));
        }
    }

    #[test]
    fn postgres_protocol_and_query_defects_remain_internal() {
        for error in [
            sqlx::Error::Protocol("invalid frame".to_string()),
            sqlx::Error::ColumnNotFound("lifecycle".to_string()),
        ] {
            assert_eq!(
                lifecycle_access_status(RunAccessError::Store(error.into())).code(),
                tonic::Code::Internal
            );
        }
    }

    #[test]
    fn postgres_shutdown_codes_are_narrowly_retryable() {
        for code in ["57P01", "57P02", "57P03"] {
            assert!(pg_availability_code(code));
        }
        for code in ["08P01", "28P01", "42P01"] {
            assert!(!pg_availability_code(code));
        }
    }
}

fn normalize_run_name(run_name: &str) -> Result<&str, Status> {
    let normalized = run_name.trim();
    if normalized.is_empty() || !storable_ident(normalized, MAX_RUN_NAME_BYTES) {
        return Err(Status::invalid_argument(format!(
            "run_name must be 1..={MAX_RUN_NAME_BYTES} UTF-8 bytes after trimming, with no NUL bytes"
        )));
    }
    Ok(normalized)
}

fn init_run_status(error: InitRunError) -> Status {
    match error {
        InitRunError::Store(error) => Status::internal(format!("InitRun failed: {error}")),
        InitRunError::WriterEpochExhausted(key) => Status::resource_exhausted(format!(
            "run {}/{} exhausted its rich writer epochs",
            key.project_id, key.run_id
        )),
        InitRunError::RunIdOwned {
            run_id,
            requested_project_id,
        } => Status::already_exists(format!(
            "run id {run_id:?} already belongs to another project and cannot be initialized in {requested_project_id:?}"
        )),
        InitRunError::NotInitializable { key, state } => Status::failed_precondition(format!(
            "run {}/{} cannot be initialized ({state:?})",
            key.project_id, key.run_id
        )),
    }
}

#[cfg(test)]
mod import_validation_tests {
    use super::*;

    #[test]
    fn import_timestamps_reject_zero_and_unstorable_values() {
        assert!(require_import_timestamp("created_at_ms", 1_680_000_000_000).is_ok());
        // Negative (pre-1970) values are Postgres-storable and allowed.
        assert!(require_import_timestamp("created_at_ms", -1).is_ok());
        for bad in [0, i64::MAX, i64::MIN] {
            assert_eq!(
                require_import_timestamp("created_at_ms", bad)
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument
            );
        }
    }
}

#[cfg(test)]
mod run_key_validation_tests {
    use super::*;

    #[test]
    fn cross_project_run_id_owner_is_an_already_exists_error() {
        let status = init_run_status(InitRunError::RunIdOwned {
            run_id: "shared".to_string(),
            requested_project_id: "other".to_string(),
        });
        assert_eq!(status.code(), tonic::Code::AlreadyExists);
        assert!(status.message().contains("shared"));
        assert!(status.message().contains("other"));
    }

    #[test]
    fn new_run_keys_use_the_ingest_storage_boundary() {
        assert!(require_storable_run_key("project", "run").is_ok());

        for (project_id, run_id) in [
            ("", "run"),
            ("project", ""),
            ("project\0suffix", "run"),
            ("project", "run\0suffix"),
        ] {
            assert_eq!(
                require_storable_run_key(project_id, run_id)
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument
            );
        }

        let oversized = "x".repeat(MAX_ID_BYTES + 1);
        assert_eq!(
            require_storable_run_key(&oversized, "run")
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            require_storable_run_key("project", &oversized)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );

        // TerminateRun must still reach rows created before the storage bound
        // existed; PostgreSQL already excludes the only poison (NUL).
        assert!(require_run_key("project", &oversized).is_ok());
    }

    #[test]
    fn new_run_keys_reject_browser_dot_segments_only_at_admission() {
        for (project_id, run_id) in [
            (".", "run"),
            ("..", "run"),
            ("project", "."),
            ("project", ".."),
        ] {
            assert_eq!(
                require_routeable_run_key(project_id, run_id)
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument,
            );
            assert!(require_storable_run_key(project_id, run_id).is_ok());
        }
        for (project_id, run_id) in [(".project", "run"), ("project", "run..")] {
            assert!(require_routeable_run_key(project_id, run_id).is_ok());
        }
    }
}

fn require_run_key(project_id: &str, run_id: &str) -> Result<(), Status> {
    if project_id.is_empty() || run_id.is_empty() {
        return Err(Status::invalid_argument(
            "project_id and run_id are required",
        ));
    }
    Ok(())
}

/// Match ingest's storage boundary so a run accepted by a lifecycle RPC can
/// also be written without poisoning PostgreSQL or leaving the client spooling.
fn require_storable_run_key(project_id: &str, run_id: &str) -> Result<(), Status> {
    require_run_key(project_id, run_id)?;
    if !storable_ident(project_id, MAX_ID_BYTES) || !storable_ident(run_id, MAX_ID_BYTES) {
        return Err(Status::invalid_argument(format!(
            "project_id/run_id must be at most {MAX_ID_BYTES} bytes with no NUL bytes"
        )));
    }
    Ok(())
}

/// New run identities must also survive browser URL path normalization.
/// Keep this narrower than the read/replay validators so historical rows
/// remain reachable through the API even if the dashboard cannot route them.
fn require_routeable_run_key(project_id: &str, run_id: &str) -> Result<(), Status> {
    require_storable_run_key(project_id, run_id)?;
    if [project_id, run_id]
        .iter()
        .any(|value| matches!(*value, "." | ".."))
    {
        return Err(Status::invalid_argument(
            "project_id and run_id cannot be '.' or '..' because browsers normalize URL path segments",
        ));
    }
    Ok(())
}

/// Import timestamps are required and must be storable (see ingest's
/// storable_timestamp: outside the Postgres range one bad value poisons the
/// UPDATE). Zero is rejected as well — it is proto3's missing-field default,
/// and an importer that forgot the field must hear about it rather than
/// backdate a run to 1970.
fn require_import_timestamp(field: &'static str, value_ms: i64) -> Result<(), Status> {
    if value_ms == 0 || !crate::ingest::storable_timestamp(value_ms) {
        return Err(Status::invalid_argument(format!(
            "{field} must be a nonzero epoch-milliseconds timestamp inside the storable range"
        )));
    }
    Ok(())
}

fn normalize_text_metric_names(mut metric_names: Vec<String>) -> Result<Vec<String>, Status> {
    // Before the checks, sort and dedup below, whose cost grows with the list: the 256-metric cap counts distinct names.
    require_run_id_count("metric_names", metric_names.len())?;
    if metric_names
        .iter()
        .any(|name| !storable_ident(name, MAX_METRIC_NAME_BYTES))
    {
        return Err(Status::invalid_argument(format!(
            "metric names must be at most {MAX_METRIC_NAME_BYTES} bytes with no NUL bytes"
        )));
    }
    metric_names.sort_unstable();
    metric_names.dedup();
    if metric_names.len() > MAX_TEXT_WINDOW_METRICS {
        return Err(Status::invalid_argument(format!(
            "text windows are limited to {MAX_TEXT_WINDOW_METRICS} metrics"
        )));
    }
    Ok(metric_names)
}

fn require_run_id_count(field: &str, count: usize) -> Result<(), Status> {
    if count > MAX_REQUEST_RUN_IDS {
        return Err(Status::invalid_argument(format!(
            "{field} lists {count} entries; a request may list at most {MAX_REQUEST_RUN_IDS}"
        )));
    }
    Ok(())
}

fn validate_text_search(search: &str) -> Result<&str, Status> {
    if search.len() > MAX_TEXT_SEARCH_BYTES {
        return Err(Status::invalid_argument(format!(
            "text search is limited to {MAX_TEXT_SEARCH_BYTES} bytes"
        )));
    }
    Ok(search)
}

fn extend_trash_errors(results: &mut Vec<proto::TrashRunResult>, run_ids: &[String], error: &str) {
    results.extend(run_ids.iter().map(|run_id| proto::TrashRunResult {
        run_id: run_id.clone(),
        outcome: proto::TrashRunOutcome::Error as i32,
        error: error.to_string(),
    }));
}

fn restore_metric_discovery_runs(kind: RestoreMutationKind, run_id: &str) -> Vec<String> {
    matches!(
        kind,
        RestoreMutationKind::Restored | RestoreMutationKind::AlreadyActive
    )
    .then(|| vec![run_id.to_string()])
    .unwrap_or_default()
}

/// The single source of truth for whether a ListTrash request is an identity
/// lookup (filtered) rather than a page scan. main.rs gates the lifecycle
/// snapshot barrier on this and `list_trash` selects its query shape from it;
/// both must agree, or a filtered lookup could bypass the reconciliation fence.
pub(crate) fn list_trash_is_filtered(req: &proto::ListTrashRequest) -> bool {
    !req.project_id.is_empty() || !req.run_ids.is_empty()
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

use crate::chart_delta::{DenseChart, DenseSeries};

/// Wire form of one dense series over columns [c..] — chart_delta::emit_series (the codec pair lives there), moved into the generated type.
fn emit_series(s: &DenseSeries, c: usize) -> proto::ChartSeries {
    let w = chart_delta::emit_series(s, c);
    proto::ChartSeries {
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

/// Whether the chart carries envelopes at all — the wire's `banded` bit; chart-wide, because the dense-envelope contract is (see kymo.proto).
fn is_banded(chart: &DenseChart) -> bool {
    chart.series.iter().any(|s| !s.min_values.is_empty())
}

/// The full wire response for a dense model (no delta).
fn emit_full(chart: &DenseChart) -> proto::ChartResponse {
    let (xr_seg_starts, xr_seg_lens, xr_min, xr_max) = chart_delta::emit_xr(chart, 0);
    proto::ChartResponse {
        x_values: chart.x_values.clone(),
        series: chart.series.iter().map(|s| emit_series(s, 0)).collect(),
        banded: is_banded(chart),
        xr_seg_starts,
        xr_seg_lens,
        xr_min,
        xr_max,
        ..Default::default()
    }
}

/// The full answer for a dense model, flagged `audit_failed` (a sampled check found the answer it checked wrong), with `frontiers`, the stamps that answer carried: every reply starts with no frontiers, so they are exactly a full answer's.
fn audit_failed_full(
    chart: &DenseChart,
    frontiers: std::collections::HashMap<String, i64>,
) -> proto::ChartResponse {
    proto::ChartResponse {
        audit_failed: true,
        frontiers,
        ..emit_full(chart)
    }
}

/// Exact output-determining smoothing state rides inside the otherwise opaque frontier map so even an already-deployed frontend echoes it verbatim. Real identifiers cannot contain NUL (ingest::storable_ident), which reserves this namespace. Each output series is one word: 0 = no global state, 1 = uniform index-space branch, otherwise positive-finite median f64 bits.
fn smoothing_state_key(index: usize) -> String {
    format!("\0kymo:s:{index}")
}

fn encode_smoothing_plan(plan: chart::SmoothingPlan) -> i64 {
    match plan {
        chart::SmoothingPlan::NoState => 0,
        chart::SmoothingPlan::Uniform => 1,
        chart::SmoothingPlan::Median(bits) => bits as i64,
    }
}

fn decode_smoothing_plan(word: i64) -> Option<chart::SmoothingPlan> {
    match word {
        0 => Some(chart::SmoothingPlan::NoState),
        1 => Some(chart::SmoothingPlan::Uniform),
        _ => {
            let bits = word as u64;
            let median = f64::from_bits(bits);
            (median.is_finite() && median >= f64::MIN_POSITIVE)
                .then_some(chart::SmoothingPlan::Median(bits))
        }
    }
}

fn read_smoothing_state(
    frontiers: &std::collections::HashMap<String, i64>,
    series: usize,
) -> Option<Vec<chart::SmoothingPlan>> {
    (frontiers.get(SMOOTHING_STATE_VERSION_KEY) == Some(&SMOOTHING_STATE_VERSION)).then_some(())?;
    (usize::try_from(*frontiers.get(SMOOTHING_STATE_SERIES_KEY)?).ok()? == series).then_some(())?;
    (0..series)
        .map(|index| decode_smoothing_plan(*frontiers.get(&smoothing_state_key(index))?))
        .collect()
}

/// Request-derived knobs shared by the live build, the delta planner, and the audit reconstruction.
#[derive(Clone)]
struct ChartParams {
    /// Each ref's tag filter as a set, by request index, when any ref lists tags ([`Self::admits`]): built once per poll ([`ChartPlan::new`]), so a ref costs one lookup per tag group whatever its filter's length, and shared with its builds.
    filters: Option<Arc<[std::collections::HashSet<String>]>>,
    step_min: i64,
    step_max: i64,
    // With smoothing + a zoom range, fetch a margin past both zoom edges so the smoothed curve keeps its unzoomed shape (clipping the fetch at the zoom edge made windows clip there like a data edge), then trim back to [step_min, step_max] after smoothing. Warmup bounds, not exactness guarantees: they're in steps while windows are in samples, so a sparser-than-4x logger's windows clip early — same artifact as a real data edge, vanishingly small under the biweight taper. EMA needs left history only; 8 time constants decay the truncated mass to e⁻⁸.
    fetch_min: i64,
    fetch_max: i64,
    use_time: bool,
    relative: bool,
    is_smoothed: bool,
    algo: Algorithm,
    window_size: u32,
    time_constant: f64,
    poly_order: u32,
    /// Whether this delta-capable request has a whole-series data-derived smoothing plan that must round-trip exactly. Step EMA/Triangular have no such state; custom-x never deltas.
    exact_smoothing_state: bool,
    all_same_metric: bool,
    all_same_run: bool,
    /// Axis geometry for the bucketing pipeline and the delta planner.
    spec: chart::GridSpec,
    /// Conservative centered-smoother reach in samples; irregular plans also use the median-scaled x span. Causal influence and uniform SG block arithmetic are marked separately as suffixes before plotting filters.
    reach: usize,
}

impl ChartParams {
    /// Whether the tag filter of the ref at `request_index` admits `tag`: every tag when it names none.
    fn admits(&self, request_index: usize) -> impl Fn(&str) -> bool + '_ {
        let filter = self.filters.as_ref().map(|filters| &filters[request_index]);
        move |tag| filter.is_none_or(|filter| filter.is_empty() || filter.contains(tag))
    }
}

fn chart_params(req: &proto::ChartRequest) -> ChartParams {
    let step_min = req.step_min.unwrap_or(i64::MIN);
    let step_max = req.step_max.unwrap_or(i64::MAX);
    let smoothing = req.smoothing.unwrap_or_default();
    let algo = smoothing.algorithm();
    let is_smoothed = algo != Algorithm::None;
    // Keep fetch bounds and delta invalidation on the same window the
    // smoother executes. The wire value is untrusted and can span all u32.
    let window_size = smoothing.window_size.min(chart::MAX_SMOOTHING_WINDOW);
    let (margin_l, margin_r): (i64, i64) = match algo {
        Algorithm::None => (0, 0),
        Algorithm::Ema => (
            ((8.0 * smoothing.time_constant).ceil() as i64).clamp(1, 1_000_000),
            0,
        ),
        // Causal like EMA (nothing ahead), but its weights never decay, so a
        // correct value needs the whole run behind it — an unbounded left
        // extent that no finite margin expresses, pinned in fetch_min below.
        Algorithm::Triangular => (0, 0),
        _ => {
            let w = window_size.max(3) as i64 * 4;
            (w, w)
        }
    };
    ChartParams {
        filters: req.y_series.iter().any(|s| !s.tags.is_empty()).then(|| {
            req.y_series
                .iter()
                .map(|s| s.tags.iter().cloned().collect())
                .collect()
        }),
        step_min,
        step_max,
        fetch_min: if is_smoothed && req.step_min.is_some() {
            // Triangular reaches back to the run's first step (any earlier
            // step, negative included); the bounded smoothers extend left by
            // their warmup margin.
            if matches!(algo, Algorithm::Triangular) {
                i64::MIN
            } else {
                step_min.saturating_sub(margin_l)
            }
        } else {
            step_min
        },
        fetch_max: if is_smoothed && req.step_max.is_some() {
            step_max.saturating_add(margin_r)
        } else {
            step_max
        },
        use_time: req.use_timestamp_axis,
        relative: req.use_timestamp_axis && req.relative_time,
        is_smoothed,
        algo,
        window_size,
        time_constant: smoothing.time_constant,
        poly_order: smoothing.poly_order,
        exact_smoothing_state: req.x_series.is_none()
            && match algo {
                Algorithm::None => false,
                Algorithm::Ema | Algorithm::Triangular => req.use_timestamp_axis,
                Algorithm::SavitzkyGolay => true,
            },
        all_same_metric: req
            .y_series
            .iter()
            .all(|s| s.metric_name == req.y_series[0].metric_name),
        all_same_run: req
            .y_series
            .iter()
            .all(|s| s.run_id == req.y_series[0].run_id),
        spec: chart::GridSpec {
            target: req.target_resolution as usize,
            is_step_axis: !req.use_timestamp_axis && req.x_series.is_none(),
            log_buckets: req.log_buckets,
            // Step/timestamp log ladders run in log(x+1) so step 0 renders; custom-x keeps plain log(x) (a +1 shift would crush sub-1 domains).
            shift_one: req.log_buckets && req.x_series.is_none(),
        },
        reach: match algo {
            Algorithm::None | Algorithm::Ema | Algorithm::Triangular => 0,
            _ => window_size.max(3) as usize,
        },
    }
}

/// Stable identity of one request/tag expansion. Request position is part of the identity because duplicate `SeriesRef`s are valid and can otherwise produce the same run/metric/tag/label.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SeriesIdentity {
    request_index: usize,
    tag: String,
}

/// One series' samples ready for the shared axis, plus delta provenance.
struct PreparedSeries {
    identity: SeriesIdentity,
    label: String,
    run_id: String,
    xs: Vec<f64>,
    plot: Vec<f64>, // smoothed when smoothing is on, else raw
    raw: Vec<f64>,  // empty unless smoothing is on
    /// Per-sample [`chart::nan_kind`] of the LOGGED value (0 = finite).
    /// The single source of truth for non-finiteness: row eviction, marker
    /// emission, and the wire nan_kinds all derive from it.
    kinds: Vec<u8>,
    /// Carried positions for samples whose x is unplottable: non-finite
    /// custom-x, or negative x on an absolute log step/time axis. They become
    /// kind-4 annotations on the row holding their anchor sample; they never own a row and
    /// never evict one. Custom-x carries each gap from its last plottable
    /// position (ascending, deduped); negative log-x attaches to the first.
    /// A run with no plottable x carries them to −∞, the chart's first column.
    xnan_xs: Vec<f64>,
    /// How many samples had an unplottable x (the samples behind `xnan_xs`,
    /// pre-dedup) — the tooltip's "×N" (DenseSeries::xnan_count).
    xnan_count: u32,
    /// A verified-held unplottable log-x sample proves the held chart already carried this run's kind-4 marker (on column 0 when the run has no plottable x).
    xnan_held: bool,
    /// Per-sample [`chart::AGE_OLD`]/REACH/NEW relative to verified cutoffs (all NEW when none apply), parallel to `xs`.
    age: Vec<u8>,
    /// Whether a delta may CONTINUE this series (name the held one it extends) rather than ship it complete: some in-range plottable sample is verified-held, or no plottable sample is in range and some fetched row (smoothing warmup, unplottable x) is. The second case keeps such all-gap series from failing the held-count gate; their only possible change is a column-0 kind-4 marker appearing, which the planner answers in full. A series whose in-range plottable samples are ALL new ships complete even when held rows exist: its held columns were empty, and the held-count gate answers full for that one poll. The echoed held-series count still guards pairing after refs or tag groups change.
    continues: bool,
    /// The exact output-determining whole-series smoothing plan used for this response. `NoState` is explicit so margin-only series preserve positional alignment without computing or gating on discarded samples.
    smoothing_plan: chart::SmoothingPlan,
    /// With [`prepare`]'s `checkpoints`: a causal smoother's recorded states (chart::smooth_run_with_plan), empty otherwise.
    checkpoints: Vec<(usize, chart::CausalState)>,
}

/// A plan override is safe to consume only when the old-only reconstruction expands to exactly the identities that supplied it. A mismatch is client-controlled cache state, so it is a normal full-answer gate rather than an error.
struct Preparation {
    series: Vec<PreparedSeries>,
    audit_plans_aligned: bool,
    /// The rows collected, and those of the largest series: what stage 2 charges this build's transients over ([`DenseShape`]).
    rows: usize,
    largest: usize,
}

/// Whether the client holds exactly the `continuing` series. Verified lineages and shared-ref order prove that continuing groups were held; the client's echoed series count still detects removed refs/tag groups and held groups that no longer continue, such as a margin-only group gaining in-range samples, and positional splicing requires that every held series is paired. With exact smoothing state, their plans must also equal the ones stamped on the response the client holds (new complete series do not participate); missing state safely forces one full answer, which seeds plans for subsequent deltas.
fn continuing_series_match(
    req: &proto::ChartRequest,
    p: &ChartParams,
    continuing_plans: impl Iterator<Item = chart::SmoothingPlan>,
    continuing: usize,
) -> bool {
    let Some(cache) = req.cache_state.as_ref() else {
        return false;
    };
    cache
        .held_series
        .is_some_and(|held| held as usize == continuing)
        && (!p.exact_smoothing_state
            || read_smoothing_state(&cache.frontiers, continuing)
                .is_some_and(|held| continuing_plans.eq(held)))
}

/// Stamp every output series' plan.
fn stamp_smoothing_state(
    frontiers: &mut std::collections::HashMap<String, i64>,
    p: &ChartParams,
    plans: impl ExactSizeIterator<Item = chart::SmoothingPlan>,
) {
    if !p.exact_smoothing_state {
        return;
    }
    frontiers.insert(
        SMOOTHING_STATE_VERSION_KEY.to_string(),
        SMOOTHING_STATE_VERSION,
    );
    frontiers.insert(SMOOTHING_STATE_SERIES_KEY.to_string(), plans.len() as i64);
    for (index, plan) in plans.enumerate() {
        frontiers.insert(smoothing_state_key(index), encode_smoothing_plan(plan));
    }
}

/// Flag held samples whose smoothed value a new sample can reach, reusing the already-derived plan: uniform smoothers need index distance only; irregular smoothers also use their one median-scaled x span. Full/no-cache requests have no OLD samples and do no reach work.
fn flag_reach(xs: &[f64], age: &mut [u8], reach: usize, plan: chart::SmoothingPlan) {
    if reach == 0
        || matches!(plan, chart::SmoothingPlan::NoState)
        || !age.contains(&chart::AGE_NEW)
        || !age.contains(&chart::AGE_OLD)
    {
        return;
    }
    let reach_x = match plan {
        chart::SmoothingPlan::Median(bits) => Some(f64::from_bits(bits) * reach as f64),
        chart::SmoothingPlan::Uniform => None,
        chart::SmoothingPlan::NoState => unreachable!(),
    };
    let within = |i: usize, j: usize| {
        i.abs_diff(j) <= reach || reach_x.is_some_and(|span| (xs[i] - xs[j]).abs() <= span)
    };
    let mut last_new: Option<usize> = None;
    for (i, sample_age) in age.iter_mut().enumerate() {
        match *sample_age {
            chart::AGE_NEW => last_new = Some(i),
            chart::AGE_OLD if last_new.is_some_and(|j| within(i, j)) => {
                *sample_age = chart::AGE_REACH;
            }
            _ => {}
        }
    }
    let mut next_new: Option<usize> = None;
    for (i, sample_age) in age.iter_mut().enumerate().rev() {
        match *sample_age {
            chart::AGE_NEW => next_new = Some(i),
            chart::AGE_OLD if next_new.is_some_and(|j| within(i, j)) => {
                *sample_age = chart::AGE_REACH;
            }
            _ => {}
        }
    }
}

/// Mark the earliest interpolation dependency for a suffix replacement.
///
/// Call once on the original smoothing-reach ages. A second call would treat the REACH flag written here as another changed endpoint. Only OLD becomes REACH, preserving held membership and marker provenance.
///
/// OLD smoother-only holes do not force a splice. Treating a NEW smoother-only hole as changing is deliberate conservatism for interpolation/extrapolation. REACH/NEW finite raw points can add, remove, or change endpoints even if their current smoothed output is non-finite. With two preceding finite endpoints, the earliest affected cell contains the nearer endpoint; with fewer, leading occupied holes also depend on the first segment. Later dependencies are already covered by the suffix. Logged non-finite samples own markers and cannot be endpoints.
fn flag_lerp_dependency(plot: &[f64], kinds: &[u8], age: &mut [u8]) {
    debug_assert_eq!(plot.len(), age.len());
    debug_assert_eq!(kinds.len(), age.len());
    let Some(changing) = (0..plot.len()).find(|&i| age[i] != chart::AGE_OLD && kinds[i] == 0)
    else {
        return;
    };
    let mut preceding = (0..changing).rev().filter(|&i| plot[i].is_finite());
    let nearest = preceding.next();
    let dependency = if preceding.next().is_some() {
        nearest
    } else {
        // chart::lerp_at samples adjacent finite pairs for both interpolation and extrapolation. Its first pair supplies the slope for occupied smoother-only holes before either endpoint.
        (0..changing).find(|&i| kinds[i] == 0)
    };
    if let Some(i) = dependency {
        // Every possible endpoint before `changing` is OLD by construction. Preserve NEW: it records absence from the held snapshot.
        debug_assert_eq!(age[i], chart::AGE_OLD);
        age[i] = chart::AGE_REACH;
    }
}

/// Prepare chart inputs, smoothing and provenance from fetched rows. The caller supplies verified cutoffs from the cache-lineage proof; those cutoffs drive ages (an absent ref is all NEW). old_only keeps precisely the verified held versions for the audit oracle. With `checkpoints`, also records each causal smoother's checkpoints (held chart state, query/held.rs).
#[allow(clippy::too_many_arguments)]
fn prepare(
    req: &proto::ChartRequest,
    p: &ChartParams,
    all_rows: &[Arc<SeriesSnapshot>],
    x_maps: Option<&std::collections::HashMap<String, std::collections::HashMap<i64, f64>>>,
    held_rows: Option<&VerifiedLineages>,
    old_only: bool,
    smoothing_override: Option<&[(SeriesIdentity, chart::SmoothingPlan)]>,
    checkpoints: bool,
) -> Result<Preparation, Status> {
    // (step, x_key, value, age) — x_key is the plot key (the step, or
    // timestamp_ms in time mode). The step rides along so range trims and
    // x-metric lookups always work in step space, whatever the chart plots.
    type RawPoint = (i64, i64, f64, u8);
    struct RawSeries {
        identity: SeriesIdentity,
        label: String,
        run_id: String,
        points: Vec<RawPoint>,
    }
    let mut raw_series: Vec<RawSeries> = Vec::new();
    for (request_index, (s, rows)) in req.y_series.iter().zip(all_rows).enumerate() {
        let wm = held_rows.and_then(|held| held.cutoff(request_index));
        let (fetch_min, fetch_max, use_time) = (p.fetch_min, p.fetch_max, p.use_time);
        let age = move |row: &crate::series_cache::CachedRow| match wm {
            Some(w) if row.inserted_ms <= w => chart::AGE_OLD,
            _ => chart::AGE_NEW,
        };
        // Rows are the FULL series from the cache, in tag order, so the scalar group ('') is first if present. A tag forms a group when it keeps a row: one in the fetch range, and under old_only a verified held one. Only the chosen groups are collected: a build allocates the rows it keeps, what stage 2 charges ([`ChartPlan::prepared_rows`]), never those of a group it drops.
        let formed: Vec<_> = rows
            .tags()
            .filter(|tag| {
                tag.between(fetch_min, fetch_max)
                    .flatten()
                    .any(|row| !(old_only && age(row) == chart::AGE_NEW))
            })
            .collect();
        let (picked, tagged) =
            selected_groups(formed.iter().map(|tag| tag.tag()), p.admits(request_index));
        let mut groups = picked.iter().map(move |&i| {
            let tag = &formed[i];
            // Sized at the tag's rows in range (those it keeps, or under old_only at most those), so the build holds no growth slack.
            let mut points: Vec<RawPoint> =
                Vec::with_capacity(tag.rows_between(fetch_min, fetch_max));
            // Extended chunk by chunk: this visits every row a build prepares, and a per-row `for` over these slices measured about 5% slower in full-range builds.
            for rows in tag.between(fetch_min, fetch_max) {
                points.extend(rows.iter().filter_map(|row| {
                    let age = age(row);
                    let xk = if use_time { row.timestamp_ms } else { row.step };
                    let kept = !(old_only && age == chart::AGE_NEW);
                    kept.then_some((row.step, xk, row.value as f64, age))
                }));
            }
            (tag.tag().to_owned(), points)
        });
        if tagged {
            if picked.len() > MAX_TAG_GROUPS {
                return Err(Status::invalid_argument(format!(
                    "Tagged metric '{}' has {} tags, max {MAX_TAG_GROUPS} per chart",
                    s.metric_name,
                    picked.len()
                )));
            }
            for (tag, points) in groups {
                let label = if p.all_same_run && p.all_same_metric {
                    tag.clone()
                } else if p.all_same_metric {
                    format!("{}/{}", s.run_id, tag)
                } else if p.all_same_run {
                    format!("{}_{}", s.metric_name, tag)
                } else {
                    format!("{}/{}_{}", s.run_id, s.metric_name, tag)
                };
                raw_series.push(RawSeries {
                    identity: SeriesIdentity { request_index, tag },
                    label,
                    run_id: s.run_id.clone(),
                    points,
                });
            }
        } else if let Some((tag, points)) = groups.next() {
            // Scalar metric (tag = '')
            let label = if p.all_same_metric {
                s.run_id.clone()
            } else if p.all_same_run {
                s.metric_name.clone()
            } else {
                format!("{}/{}", s.run_id, s.metric_name)
            };
            raw_series.push(RawSeries {
                identity: SeriesIdentity { request_index, tag },
                label,
                run_id: s.run_id.clone(),
                points,
            });
        }
    }

    let rows = raw_series.iter().map(|rs| rs.points.len()).sum();
    let largest = raw_series
        .iter()
        .map(|rs| rs.points.len())
        .max()
        .unwrap_or(0);
    // `held_series` and smoothing-state counts are untrusted client bytes. Prove the whole positional override before smoothing: an extra old-only group could otherwise shift NoState onto Savitzky-Golay and panic, or overrun the plan slice and return a 500.
    if smoothing_override.is_some_and(|plans| {
        !raw_series
            .iter()
            .map(|rs| &rs.identity)
            .eq(plans.iter().map(|(identity, _)| identity))
    }) {
        return Ok(Preparation {
            series: Vec::new(),
            audit_plans_aligned: false,
            rows,
            largest,
        });
    }

    // Time mode plots client-set timestamps, which nothing guarantees
    // monotone (clock steps, multi-host runs). Sort each series by its
    // plot key so every downstream ascending-x assumption — smoothing
    // windows, resampling scans, the response's sorted-x invariant —
    // holds. Nearly sorted in practice, so the adaptive sort is ~O(n).
    if p.use_time {
        for rs in &mut raw_series {
            rs.points.sort_by_key(|pt| pt.1);
        }
    }
    // For relative time mode: offset each series so it starts from 0
    // (first == min thanks to the sort above).
    if p.relative {
        for rs in &mut raw_series {
            if let Some(&(_, min_x, ..)) = rs.points.first() {
                for pt in &mut rs.points {
                    pt.1 -= min_x;
                }
            }
        }
    }

    let mut prepared: Vec<PreparedSeries> = Vec::with_capacity(raw_series.len());
    for (series_index, mut rs) in raw_series.into_iter().enumerate() {
        let mut series_checkpoints = Vec::new();
        let steps_v: Vec<i64> = rs.points.iter().map(|&(s, ..)| s).collect();
        let xkeys_v: Vec<i64> = rs.points.iter().map(|&(_, x, ..)| x).collect();
        let raw_v: Vec<f64> = rs.points.iter().map(|&(_, _, v, ..)| v).collect();
        let mut age_v: Vec<u8> = rs.points.iter().map(|&(_, _, _, a)| a).collect();
        // Nothing reads the points again: free them before this series' working copies are built.
        drop(std::mem::take(&mut rs.points));
        // Trim the smoothing warmup back to the requested STEP range whatever the plot key is. The precheck exactly matches the later axis filter, so a margin-only/unplottable series can skip plan derivation and smoothing entirely.
        let in_range = |i: usize| steps_v[i] >= p.step_min && steps_v[i] <= p.step_max;
        // Resolve this run's custom-x map once for both the output precheck and emission.
        let x_map = x_maps.map(|maps| maps.get(&rs.run_id));
        let is_output_position = |i: usize| match x_map {
            Some(xm) => {
                in_range(i)
                    && xm
                        .and_then(|m| m.get(&steps_v[i]))
                        .is_some_and(|&x| x.is_finite() && !(p.spec.log_buckets && x <= 0.0))
            }
            None => in_range(i) && !(p.spec.log_buckets && xkeys_v[i] < 0),
        };
        let (has_output_position, has_finite_output) = if p.is_smoothed {
            let mut has_output_position = false;
            let has_finite_output = (0..steps_v.len()).any(|i| {
                let output_position = is_output_position(i);
                has_output_position |= output_position;
                output_position && raw_v[i].is_finite()
            });
            (has_output_position, has_finite_output)
        } else {
            ((0..steps_v.len()).any(is_output_position), false)
        };
        let override_plan = smoothing_override.map(|plans| plans[series_index].1);
        let (plot_v, smoothing_plan) = if p.is_smoothed && has_finite_output {
            // One f64 x vector and one semantic plan feed reach marking, smoothing, exact echo state, and audit. An audit override bypasses derivation completely.
            let x_keys: Vec<f64> = xkeys_v.iter().map(|&x| x as f64).collect();
            let plan = override_plan
                .unwrap_or_else(|| chart::smoothing_plan(&x_keys, p.algo, !p.use_time));
            flag_reach(
                &x_keys,
                &mut age_v,
                if req.x_series.is_none() { p.reach } else { 0 },
                plan,
            );
            if held_rows.is_some() && !old_only {
                if let Some(first_new) = age_v.iter().position(|&age| age == chart::AGE_NEW) {
                    let from = match p.algo {
                        Algorithm::Ema | Algorithm::Triangular => Some(first_new),
                        Algorithm::SavitzkyGolay if plan == chart::SmoothingPlan::Uniform => Some(
                            chart::savgol_dependency_start(p.window_size as usize, first_new),
                        ),
                        _ => None,
                    };
                    // Verified lineages preserve held positions. Causal influence and SG block arithmetic can change the whole suffix, even if the new input itself is later trimmed/unplottable. Preserve NEW membership.
                    if let Some(from) = from {
                        for age in &mut age_v[from..] {
                            if *age == chart::AGE_OLD {
                                *age = chart::AGE_REACH;
                            }
                        }
                    }
                }
            }
            (
                chart::smooth_run_with_plan(
                    &x_keys,
                    &raw_v,
                    p.algo,
                    p.window_size,
                    p.time_constant,
                    p.poly_order,
                    plan,
                    checkpoints.then_some(&mut series_checkpoints),
                ),
                plan,
            )
        } else if has_output_position {
            (raw_v.clone(), chart::SmoothingPlan::NoState)
        } else {
            (Vec::new(), chart::SmoothingPlan::NoState)
        };

        // The output columns, allocated once at their length: the fetched rows' on step and time axes, the plottable points' on custom x.
        let columns = |n: usize| {
            (
                Vec::with_capacity(n),
                Vec::with_capacity(n),
                Vec::with_capacity(if p.is_smoothed { n } else { 0 }),
                Vec::with_capacity(n),
                Vec::with_capacity(n),
            )
        };
        let (mut xs, mut plot, mut raw, mut kinds, mut age);
        let mut xnan_xs: Vec<f64> = Vec::new();
        let mut xnan_count = 0u32;
        let mut xnan_held = false;
        match x_map {
            Some(xm) => {
                // (x, plot, raw, kind) per plottable point. A step whose x
                // metric is ABSENT is normal sparse logging and stays
                // hidden; a step whose x metric was LOGGED non-finite has
                // no position but is real data — carry it to the last
                // plottable x seen in step order (leading ones attach to
                // the first), as a kind-4 annotation. Custom-x charts never
                // take the delta path, so ages need not survive this arm.
                // Beyond its prepared rows, a series holds at most these points and their sort's buffer, which stage 2 charges as any series' working copies: the key copy is freed before they are collected (at most one per row in range), the other copies before the sort.
                drop(xkeys_v);
                let mut pts: Vec<(f64, f64, f64, u8)> =
                    Vec::with_capacity((0..steps_v.len()).filter(|&i| in_range(i)).count());
                let mut leading_gap = false;
                for i in (0..steps_v.len()).filter(|&i| in_range(i)) {
                    let Some(&x) = xm.and_then(|m| m.get(&steps_v[i])) else {
                        continue;
                    };
                    // A non-finite x never plots; on a log axis a nonpositive x can't either — both become exceptional kind-4 markers carried to the run's last plottable x (docs/log-scale-buckets.md).
                    let plottable = x.is_finite() && !(p.spec.log_buckets && x <= 0.0);
                    if plottable {
                        pts.push((x, plot_v[i], raw_v[i], chart::nan_kind(raw_v[i])));
                    } else {
                        xnan_count += 1;
                        match pts.last() {
                            Some(&(cx, ..)) => xnan_xs.push(cx),
                            None => leading_gap = true,
                        }
                    }
                }
                if leading_gap {
                    // First plottable x in step order = first pts entry
                    // (pts is still in step order here). A run whose x
                    // metric was never plottable anchors at the left edge.
                    xnan_xs.push(pts.first().map_or(f64::NEG_INFINITY, |&(fx, ..)| fx));
                }
                xnan_xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                xnan_xs.dedup();
                drop((steps_v, raw_v, plot_v));
                if !pts.is_empty() {
                    // `continues` reads the fetched ages only when no point plots.
                    age_v = Vec::new();
                }
                // uPlot needs ascending x; an x metric need not be monotonic
                pts.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                (xs, plot, raw, kinds, age) = columns(pts.len());
                for (x, pv, rv, k) in pts {
                    xs.push(x);
                    plot.push(pv);
                    if p.is_smoothed {
                        raw.push(rv);
                    }
                    kinds.push(k);
                    age.push(chart::AGE_NEW);
                }
            }
            None => {
                (xs, plot, raw, kinds, age) = columns(steps_v.len());
                // On a log axis (ladder in log(x+1) here) a negative x can't render: it becomes an exceptional kind-4 marker, like a non-finite custom-x (docs/log-scale-buckets.md). Negatives lead the ascending keys, so they all carry to the first plottable x.
                for i in 0..steps_v.len() {
                    if !in_range(i) {
                        continue;
                    }
                    let xk = xkeys_v[i] as f64;
                    if p.spec.log_buckets && xk < 0.0 {
                        xnan_count += 1;
                        xnan_held |= age_v[i] != chart::AGE_NEW;
                        continue;
                    }
                    xs.push(xk);
                    plot.push(plot_v[i]);
                    if p.is_smoothed {
                        raw.push(raw_v[i]);
                    }
                    kinds.push(chart::nan_kind(raw_v[i]));
                    age.push(age_v[i]);
                }
                if xnan_count > 0 {
                    // A run with no plottable x anchors its marker at the left edge.
                    xnan_xs.push(xs.first().copied().unwrap_or(f64::NEG_INFINITY));
                }
            }
        }
        let held = |ages: &[u8]| ages.iter().any(|&k| k != chart::AGE_NEW);
        let continues = held(&age) || (xs.is_empty() && held(&age_v));
        prepared.push(PreparedSeries {
            identity: rs.identity,
            label: rs.label,
            run_id: rs.run_id,
            xs,
            plot,
            raw,
            kinds,
            xnan_xs,
            xnan_count,
            xnan_held,
            age,
            continues,
            smoothing_plan,
            checkpoints: series_checkpoints,
        });
    }
    Ok(Preparation {
        series: prepared,
        audit_plans_aligned: true,
        rows,
        largest,
    })
}

/// The FULL dense model for a prepared set — every chart, delta or not, is computed through here (the audit oracle reconstructs the held one). Every chart — step, timestamp, AND custom-x — is bucketed on ONE slot grid shared by all runs (docs/chart-shared-axis.md); `shared_chart` owns the grid, slot mixture, envelopes, and the marker channel. Y columns are rounded to wire precision HERE (chart_delta::round_y), so the model, the result hashes, and the client's inflated copy are bit-identical — and a smoother's non-finite overshoot is clamped back into the finite range the wire carries.
///
/// With `capture`, also returns the axis layout (held chart state, query/held.rs).
fn respond(
    prepared: &[PreparedSeries],
    p: &ChartParams,
    capture: bool,
) -> (DenseChart, Option<chart::incremental::HeldAxis>) {
    let xs_s: Vec<&[f64]> = prepared.iter().map(|q| q.xs.as_slice()).collect();
    let plot_s: Vec<&[f64]> = prepared.iter().map(|q| q.plot.as_slice()).collect();
    let raw_s: Vec<&[f64]> = prepared.iter().map(|q| q.raw.as_slice()).collect();
    let kinds_s: Vec<&[u8]> = prepared.iter().map(|q| q.kinds.as_slice()).collect();
    let xnan_s: Vec<&[f64]> = prepared.iter().map(|q| q.xnan_xs.as_slice()).collect();
    let (mut chart, axis) = chart::shared_chart(
        &xs_s,
        &plot_s,
        &raw_s,
        &kinds_s,
        &xnan_s,
        p.is_smoothed,
        p.spec,
        capture,
    );
    for (s, ps) in chart.series.iter_mut().zip(prepared) {
        s.label = ps.label.clone();
        s.run_id = ps.run_id.clone();
        s.xnan_count = ps.xnan_count;
        for col in [
            &mut s.values,
            &mut s.raw_values,
            &mut s.min_values,
            &mut s.max_values,
        ] {
            for v in col.iter_mut() {
                *v = chart_delta::round_y(*v);
            }
        }
    }
    (chart, axis)
}

/// Relative-time offsets require an untrimmed request and timestamps monotone across the held boundary.
/// Verified lineages preserve a held step prefix within each tag, including after eviction recovery; non-relative charts need no additional row scan.
fn relative_offsets_stable(
    req: &proto::ChartRequest,
    p: &ChartParams,
    all_rows: &[Arc<SeriesSnapshot>],
    held_rows: &VerifiedLineages,
) -> bool {
    if req.step_min.is_some() || req.step_max.is_some() {
        return false;
    }
    for (index, rows) in all_rows.iter().enumerate() {
        let Some(wm) = held_rows.cutoff(index) else {
            continue; // not held: the whole series ships complete
        };
        for tag in rows.tags() {
            let mut old_max_ts = i64::MIN;
            for row in tag.between(p.fetch_min, p.fetch_max).flatten() {
                if row.inserted_ms > wm {
                    if row.timestamp_ms < old_max_ts {
                        return false;
                    }
                } else {
                    old_max_ts = old_max_ts.max(row.timestamp_ms);
                }
            }
        }
    }
    true
}

/// The audit oracle's verdict: the first `from_col` columns of the reconstructed held model must be bit-identical to the new one — the axis, the chart-level bucket extents, and every continuing series (paired in order, see to_delta).
fn prefix_matches(
    old: &DenseChart,
    new: &DenseChart,
    from_col: usize,
    prepared: &[PreparedSeries],
) -> bool {
    let continuing: Vec<usize> = (0..prepared.len())
        .filter(|&i| prepared[i].continues)
        .collect();
    if from_col > old.x_values.len() || old.series.len() != continuing.len() {
        return false;
    }
    fn bits(v: &[f64], n: usize) -> impl Iterator<Item = u64> + '_ {
        v.iter().take(n).map(|f| f.to_bits())
    }
    if !bits(&old.x_values, from_col).eq(bits(&new.x_values, from_col))
        || !bits(&old.xr_min, from_col).eq(bits(&new.xr_min, from_col))
        || !bits(&old.xr_max, from_col).eq(bits(&new.xr_max, from_col))
    {
        return false;
    }
    let markers = |s: &DenseSeries| {
        s.nan_indices
            .iter()
            .zip(&s.nan_kinds)
            .take_while(|(&i, _)| (i as usize) < from_col)
            .map(|(&i, &k)| (i, k))
            .collect::<Vec<_>>()
    };
    // A family absent held but present now splices as a NaN prefix (chart_delta.rs splice_series) — hold the audit to exactly that claim.
    let fam = |o: &[f64], n: &[f64]| {
        if o.is_empty() && !n.is_empty() {
            n.iter().take(from_col).all(|v| v.is_nan())
        } else {
            bits(o, from_col).eq(bits(n, from_col))
        }
    };
    continuing.iter().zip(&old.series).all(|(&ni, o)| {
        let n = &new.series[ni];
        fam(&o.values, &n.values)
            && fam(&o.raw_values, &n.raw_values)
            && fam(&o.min_values, &n.min_values)
            && fam(&o.max_values, &n.max_values)
            && markers(o) == markers(n)
    })
}

/// Shrink the full model to the delta the client needs: continuing series (any held sample) become tails past from_col and name the held series they extend — held order is the request's refs expanded in order, a subsequence both sides share — while all-new series ship complete. Every delta carries content hashes of the full model it reconstructs to (result_x_hash/result_series_hashes): the client hashes its spliced result and refuses a mismatch before rendering — the end-to-end check only the client can run, on every delta. It checks the splice, not the model: a memo-served reply replays the hashes its build stored, and held state's advance (stage 3b, `on`) hashes the advanced model, whose prefix it copied from the entry the client also holds, so a wrong advance or lineage hashes consistently, and a fresh tab's full answer from an advance carries no hashes; only the sampled audit checks those models.
fn to_delta(
    full: &DenseChart,
    from_col: usize,
    continues: impl Iterator<Item = bool>,
) -> proto::ChartResponse {
    let result_x_hash = chart_delta::hash_axis(full);
    let result_series_hashes: Vec<u64> = full.series.iter().map(chart_delta::hash_series).collect();
    let mut splice_from_cached = Vec::with_capacity(full.series.len());
    let mut next = 0i32;
    let series = full
        .series
        .iter()
        .zip(continues)
        .map(|(s, continues)| {
            if !continues {
                splice_from_cached.push(-1);
                emit_series(s, 0) // complete: full-axis columns, as computed
            } else {
                splice_from_cached.push(next);
                next += 1;
                emit_series(s, from_col)
            }
        })
        .collect();
    let (xr_seg_starts, xr_seg_lens, xr_min, xr_max) = chart_delta::emit_xr(full, from_col);
    proto::ChartResponse {
        x_values: full.x_values[from_col..].to_vec(),
        series,
        delta: true,
        from_col: from_col as u32,
        splice_from_cached,
        banded: is_banded(full),
        xr_seg_starts,
        xr_seg_lens,
        xr_min,
        xr_max,
        result_x_hash: Some(result_x_hash),
        result_series_hashes,
        ..Default::default()
    }
}

/// Flag interpolation and kind-4 appearance dependencies for verified input history, then prove the numeric prefix.
///
/// Flags turn OLD ages into REACH in place, without changing continuation facts. Initial full builds and audit reconstruction skip this step.
fn plan_delta_from_col(prepared: &mut [PreparedSeries], p: &ChartParams) -> usize {
    for ps in prepared.iter_mut() {
        if p.is_smoothed {
            flag_lerp_dependency(&ps.plot, &ps.kinds, &mut ps.age);
        }
        // A kind-4 marker's appearance (the held rows had no unplottable sample) is the one change no numeric column shows. A run with no plottable x has its marker on column 0, so no prefix survives. Otherwise the marker sits on its anchor sample's slot, the run's first plottable x, and moves only with that sample's cell: dirty the anchor.
        if ps.xnan_count == 0 || ps.xnan_held {
            continue;
        }
        if ps.xs.is_empty() {
            if ps.continues {
                return 0;
            }
        } else if ps.age[0] == chart::AGE_OLD {
            // Held rows exist only on standard axes, where every unplottable sample is carried to the first plottable x; custom-x ages are all NEW.
            debug_assert_eq!(ps.xnan_xs.as_slice(), &ps.xs[..1]);
            ps.age[0] = chart::AGE_REACH;
        }
    }
    let xs: Vec<&[f64]> = prepared.iter().map(|ps| ps.xs.as_slice()).collect();
    let ages: Vec<&[u8]> = prepared.iter().map(|ps| ps.age.as_slice()).collect();
    chart::numeric_delta_from_col(&xs, &ages, p.spec)
}

#[cfg(test)]
fn build_response(
    req: &proto::ChartRequest,
    snapshots: &[std::sync::Arc<SeriesSnapshot>],
    x_maps: Option<&std::collections::HashMap<String, std::collections::HashMap<i64, f64>>>,
    audit: bool,
) -> Result<proto::ChartResponse, Status> {
    let built = build_chart(
        req,
        &chart_params(req),
        snapshots,
        x_maps,
        audit,
        None,
        &|_| {},
        false,
    )?;
    count_delta(built.kind);
    Ok(built.response)
}

/// A build's dense model, reported as soon as it exists so stage 2 charges what it and the rest of the build hold ([`ChartPlan::output_bytes`]) in place of the bound reserved before it ([`ChartPlan::output_bound`]).
#[derive(Clone, Copy, Debug)]
struct DenseShape {
    series: usize,
    columns: usize,
    /// The rows `prepare` kept ([`ChartPlan::prepared_rows`]), and those of its largest series.
    prepared_rows: usize,
    largest_series: usize,
    /// Dense models live at once with their replies: 1 for a build; 2 while held state's advance sits beside a full build's (query/held.rs).
    models: u64,
    /// Held state's bytes beyond its models: a captured or advanced state's own, and an advance's axis step and working copies (query/held.rs).
    extra: u64,
}

/// The frontier map a continuation poll echoes; none for a first paint.
fn echoed_frontiers(req: &proto::ChartRequest) -> Option<&std::collections::HashMap<String, i64>> {
    req.cache_state
        .as_ref()
        .map(|cs| &cs.frontiers)
        .filter(|f| !f.is_empty())
}

/// `mkdb2_chart_delta_total`: one count per answered chart poll, by how its answer was classified.
fn count_delta(kind: Option<&'static str>) {
    if let Some(kind) = kind {
        metrics::counter!("mkdb2_chart_delta_total", "kind" => kind).increment(1);
    }
}

/// A build's answer (a full build's, or held state's advance) and its [`count_delta`] classification, counted once the poll is answered.
struct BuiltChart {
    response: proto::ChartResponse,
    kind: Option<&'static str>,
    /// With a memoized reply under audit (stage 1): whether the build reproduced it; when not, it answered in full instead.
    memo_audit: Option<bool>,
    /// With [`build_chart`]'s `capture`: the held state of the answered chart (stage 3b), or why it has none.
    held: Option<Result<held::HeldChart, held::Fallback>>,
    /// Held state's advance answered, not a full build: stage 1 memoizes only full builds' replies, so an advance's fault trips held state's audit, never the memo's.
    advanced: bool,
    /// A full build's `unchanged` reply at maxima past the client's verified cutoffs, which rows its chart does not show moved: those cutoffs, which stage 1 keeps with its memo, so each tab the memo answers moves its viewer off held state's entry there ([`held::HeldStore::moved`]).
    moved_from: Option<memo::MovedFrom>,
}

/// The reply to `req` over its full `model`, classified in `kind`: a delta from the planner's `from_col` (`None` when the poll cannot continue or the audit refused it) extending the series `continues` names, else in full. The bound turns a planner bug into a full answer, never a panic in to_delta's slice. Held state's advance replies through here too.
fn reply(
    model: &DenseChart,
    req: &proto::ChartRequest,
    from_col: Option<usize>,
    continues: impl Iterator<Item = bool> + Clone,
    kind: &mut Option<&'static str>,
) -> proto::ChartResponse {
    match from_col {
        Some(from_col) if from_col > 0 && from_col <= model.x_values.len() => {
            let unchanged = from_col == model.x_values.len() && continues.clone().all(|c| c);
            *kind = Some(if unchanged { "unchanged" } else { "delta" });
            to_delta(model, from_col, continues)
        }
        Some(from_col) => {
            if from_col == 0 {
                *kind = Some("miss");
            }
            emit_full(model)
        }
        None => {
            if echoed_frontiers(req).is_some() {
                kind.get_or_insert("gated");
            }
            emit_full(model)
        }
    }
}

/// Everything after the row fetches: the full response, shrunk to a frontier delta when the echoed cache state proves the client holds a prefix (anything unprovable answers in full). `audit`, the sampled self-check, reconstructs the held response and verifies the claimed prefix bit for bit ([`prefix_matches`]); `memo` is a memoized reply under audit (stage 1) that the build must reproduce. Either failing answers in full with `audit_failed` stamped, and classified so, which the client alerts on. `on_dense` hears the full model's shape as soon as it exists. End-to-end checks: the client's result hashes on every delta (to_delta), and wire tests of audit-off responses against independent full models and hashes.
///
/// With `capture`, also captures the answered chart's held state (stage 3b, query/held.rs). `p` holds the request's parameters as its poll planned them ([`ChartPlan::params`]).
#[allow(clippy::too_many_arguments)]
fn build_chart(
    req: &proto::ChartRequest,
    p: &ChartParams,
    snapshots: &[std::sync::Arc<SeriesSnapshot>],
    x_maps: Option<&std::collections::HashMap<String, std::collections::HashMap<i64, f64>>>,
    audit: bool,
    memo: Option<&proto::ChartResponse>,
    on_dense: &dyn Fn(DenseShape),
    capture: bool,
) -> Result<BuiltChart, Status> {
    let lineages = inspect_lineages(req, snapshots, echoed_frontiers(req));
    let held_rows = lineages.verified();
    let preparation = prepare(req, p, snapshots, x_maps, held_rows, false, None, capture)?;
    let mut prepared = preparation.series;
    let mut kind = lineages.rejection().map(lineage::Rejection::label);
    let (full, axis) = respond(&prepared, p, capture);
    on_dense(DenseShape {
        series: full.series.len(),
        columns: full.x_values.len(),
        prepared_rows: preparation.rows,
        largest_series: preparation.largest,
        models: 1,
        extra: 0,
    });
    let mut resp = if full.x_values.is_empty() {
        // No plottable point anywhere: series ship their unplottable counts over an empty axis, so the panel can say why it has nothing to draw, and nothing is stamped to continue from.
        emit_full(&full)
    } else {
        let continuing = prepared.iter().filter(|ps| ps.continues).count();
        // Appends can change a global smoothing plan even with verified held inputs; the audit reuses the exact matched plan.
        let deltable = held_rows
            .is_some_and(|f| !p.relative || relative_offsets_stable(req, p, snapshots, f))
            && continuing_series_match(
                req,
                p,
                prepared
                    .iter()
                    .filter(|ps| ps.continues)
                    .map(|ps| ps.smoothing_plan),
                continuing,
            );
        let mut from_col = deltable.then(|| plan_delta_from_col(&mut prepared, p));
        let mut audit_bad = false;
        if let Some(col) = from_col.filter(|&col| audit && col > 0 && col <= full.x_values.len()) {
            // The verified held reconstruction must not derive spacing because the exact gate proved the held/current plans equal. Pair each override with stable request/tag identity: `held_series` is client-controlled, so its count alone cannot prove that old-only and continuing groups align.
            let audit_plans: Vec<(SeriesIdentity, chart::SmoothingPlan)> = prepared
                .iter()
                .filter(|ps| ps.continues)
                .map(|ps| (ps.identity.clone(), ps.smoothing_plan))
                .collect();
            let old = prepare(
                req,
                p,
                snapshots,
                x_maps,
                held_rows,
                true,
                p.exact_smoothing_state.then_some(audit_plans.as_slice()),
                false,
            )?;
            if !old.audit_plans_aligned {
                kind = Some("gated");
                from_col = None;
            } else if !prefix_matches(&respond(&old.series, p, false).0, &full, col, &prepared) {
                tracing::error!(
                    from_col = col,
                    "chart delta audit failed; answering in full"
                );
                kind = Some("audit_failed");
                audit_bad = true;
                from_col = None;
            }
        }
        let continues = prepared.iter().map(|ps| ps.continues);
        let mut resp = reply(&full, req, from_col, continues, &mut kind);
        resp.audit_failed = audit_bad;
        stamp_lineages(&mut resp.frontiers, &lineages);
        stamp_smoothing_state(
            &mut resp.frontiers,
            p,
            prepared.iter().map(|ps| ps.smoothing_plan),
        );
        resp
    };
    // The memo is compared with the reply as it would ship; a mismatch answers in full with the same stamps.
    let memo_audit = memo.map(|memo| *memo == resp);
    if memo_audit == Some(false) {
        // `built` is how the full build classified its own answer: `gated` when the audit's old-only reconstruction did not align with the request, a gate rather than the memo's reply.
        tracing::error!(
            empty = full.x_values.is_empty(),
            built = kind,
            "memoized unchanged chart reply disagreed with the full build; answering in full and disabling the memo"
        );
        kind = Some("audit_failed");
        resp = audit_failed_full(&full, std::mem::take(&mut resp.frontiers));
    }
    let held = capture.then(|| held::capture(req, p, snapshots, &mut prepared, full, axis));
    Ok(BuiltChart {
        response: resp,
        kind,
        memo_audit,
        held,
        advanced: false,
        moved_from: None,
    })
}

/// Looks up the poll's memo key ([`ChartPlan::memo_key`]) on the snapshots its reads produced. Break with a served reply, else continue to a full build with the matching memo for its audit, if any ([`settle_memo`]). `held` is held state's store and the poll's key there ([`ChartPlan::held_key`]), unless held state is off.
fn probe_memo(
    chart_memo: &memo::ChartMemo,
    key: Option<memo::MemoKey>,
    snapshots: &[Arc<SeriesSnapshot>],
    audit: bool,
    held: Option<(&held::HeldStore, held::RequestKey)>,
) -> ControlFlow<proto::ChartResponse, Option<proto::ChartResponse>> {
    let Some(key) = key else {
        return ControlFlow::Continue(None);
    };
    match chart_memo.lookup(&key, snapshots) {
        Some((reply, moved_from)) if !audit => {
            memo::count("hit");
            // What the full build would have counted.
            count_delta(Some("unchanged"));
            // The reply moved the client past its cutoffs: so does its viewer in held state, as a built answer would have.
            if let (Some((store, request)), Some(from)) = (held, moved_from) {
                store.moved(&request, snapshots, &from);
            }
            ControlFlow::Break(reply)
        }
        // A hit the audit builds behind, or a miss: counted once the poll builds ([`settle_memo`]), so a poll that starts over counts once.
        hit => ControlFlow::Continue(hit.map(|(reply, _)| reply)),
    }
}

/// Stage 1 after a build: counts the poll's outcome (an audited hit, or a candidate's miss); a memo the audit's build disagreed with turns stage 1 off until restart; a build classified `unchanged` is memoized.
fn settle_memo(
    chart_memo: &memo::ChartMemo,
    key: Option<memo::MemoKey>,
    snapshots: &[Arc<SeriesSnapshot>],
    built: &BuiltChart,
) {
    match built.memo_audit {
        Some(false) => {
            memo::count("audit_failed");
            chart_memo.disable();
            return;
        }
        Some(true) => memo::count("hit"),
        None if key.is_some() => memo::count("miss"),
        None => {}
    }
    // Only a full build's reply is recorded (an advance's fault must trip held state's audit, not replay from the memo), and never an audit_failed one, which is classified so, never `unchanged`.
    if let (Some(key), Some("unchanged"), false) = (key, built.kind, built.advanced) {
        chart_memo.record(key, snapshots, &built.response, built.moved_from.clone());
    }
}

/// Registry row (name, type-string) → wire MetricInfo. One string→enum
/// mapping shared by the per-run and run-set discovery paths.
fn metric_info_from_row((metric_name, metric_type): (String, String)) -> proto::MetricInfo {
    proto::MetricInfo {
        metric_name,
        metric_type: match metric_type.as_str() {
            "NUMERIC" => proto::metric_info::MetricType::Numeric as i32,
            "TEXT_STREAM" => proto::metric_info::MetricType::TextStream as i32,
            _ => proto::metric_info::MetricType::Cdn as i32,
        },
    }
}

fn assemble_cdn_series(
    refs: Vec<(String, String, String)>,
    rows: Vec<CdnKeyBatchRow>,
) -> Vec<proto::CdnSeries> {
    // Bucket interleaved rows by their (project, run, metric) key.
    let mut by_key: std::collections::HashMap<(String, String, String), Vec<proto::CdnEntry>> =
        std::collections::HashMap::new();
    for r in rows {
        by_key
            .entry((r.project_id, r.run_id, r.metric_name))
            .or_default()
            .push(proto::CdnEntry {
                step: r.step,
                cdn_key: r.cdn_key,
            });
    }
    refs.into_iter()
        .map(|key| {
            // `get` + clone, not `remove`: duplicate refs must each receive the full entry list.
            let entries = by_key.get(&key).cloned().unwrap_or_default();
            let (project_id, run_id, metric_name) = key;
            proto::CdnSeries {
                project_id,
                run_id,
                metric_name,
                entries,
            }
        })
        .collect()
}

/// Watch for derived liveness transitions (RUNNING → STUCK → UNRESPONSIVE →
/// PRESUMED_DEAD) and publish them as project version bumps on the push bus.
///
/// These transitions are caused by the ABSENCE of events — a run going silent — so the server must watch the clock: one 5s scan, bounded to runs still inside the transition window (`status_watch_candidates`). Push is the prompt signal; a visible tab's one-minute PollVersions loop backs up missed delivery.
///
/// Transitions bump `projects.version` first, then push the returned value (pushed versions are real versions). A run APPEARING in the candidate set also counts as a transition: a revival re-enters the window with no lifecycle RPC (for a new run this occasionally doubles InitRun's bump — one redundant ListRuns). A clean first scan seeds silently: a restart already forces reconnect resyncs. After a failed one, the first healthy scan heals instead, since transitions since those resyncs went unannounced.
pub fn spawn_status_watcher(pg: Arc<PgStore>, events: crate::events::EventSender) {
    tokio::spawn(async move {
        let mut watch = StatusWatch::default();
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let bump = async |ids: Vec<String>| pg.bump_unlocked_project_versions(&ids).await;
            match watch.tick(pg.status_watch_candidates(), bump).await {
                Ok(projects) if projects.is_empty() => {}
                Ok(projects) => {
                    let _ = events.send(crate::events::VersionEvent {
                        projects,
                        ..Default::default()
                    });
                }
                Err(e) => tracing::warn!("{e:#}"),
            }
        }
    });
}

/// Deadline on each status-watcher store call, connecting included, for a connection that died without a reset and never answers. It exceeds PostgreSQL's 11 s statement deadline, so a slow statement on an open connection fails there first. Neither is 60 s or a multiple of the 2, 4, 5, 10 or 12 s clocks.
const STATUS_WATCH_STORE_TIMEOUT: Duration = Duration::from_secs(13);
const _: () = assert!(
    STATUS_WATCH_STORE_TIMEOUT.as_secs() > crate::pg::STATUS_WATCH_STATEMENT_TIMEOUT.as_secs()
);

/// What the status watcher carries from one tick to the next.
#[derive(Default)]
struct StatusWatch {
    prev: std::collections::HashMap<(String, String), proto::RunStatus>,
    seeded: bool,
    /// A failed tick (scan or bump) may have lost transitions — including runs that aged out of the window during the gap. The next healthy tick heals by bumping every project seen on either side of the gap once.
    degraded: bool,
    /// Projects whose rows the last bump skipped as held ([`PgStore::bump_unlocked_project_versions`]), bumped again each tick until one takes.
    owed: std::collections::BTreeSet<String>,
}

impl StatusWatch {
    /// One tick over the scanned candidates: bump the projects whose runs changed status, returning the versions to push. A store call that fails or times out leaves `prev` and `seeded` as they were and the watch degraded, so the next healthy tick detects its transitions again and re-bumps any that a timed-out bump committed.
    async fn tick(
        &mut self,
        scan: impl std::future::Future<Output = anyhow::Result<TimedRunRows>>,
        bump: impl AsyncFnOnce(Vec<String>) -> anyhow::Result<Vec<(String, Option<u64>)>>,
    ) -> anyhow::Result<Vec<(String, u64)>> {
        use anyhow::Context as _;
        // Degraded until the tick completes.
        let degraded = std::mem::replace(&mut self.degraded, true);
        let secs = STATUS_WATCH_STORE_TIMEOUT.as_secs();
        let timed_rows = tokio::time::timeout(STATUS_WATCH_STORE_TIMEOUT, scan)
            .await
            .map_err(|_| anyhow::anyhow!("status watcher scan: no answer in {secs}s"))?
            .context("status watcher scan")?;
        let mut cur = std::collections::HashMap::with_capacity(timed_rows.rows.len());
        let mut changed: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for row in timed_rows.rows {
            let status = compute_status(
                timed_rows.server_now_ms,
                row.created_at_ms,
                row.last_main_metric_at_ms,
                row.last_system_metric_at_ms,
                row.exit_code,
            );
            if self.prev.get(&(row.project_id.clone(), row.run_id.clone())) != Some(&status) {
                changed.insert(row.project_id.clone());
            }
            cur.insert((row.project_id, row.run_id), status);
        }
        let mut announced = Vec::new();
        // A clean first scan seeds silently; one after a failed scan heals. Dropping runs that left the window loses nothing on a healthy tick: they transitioned to their pinned state while still inside it (the 1-minute margin covers a missed tick). (Empty while degraded = no watched runs on either side of the gap.)
        if self.seeded || degraded {
            if degraded {
                changed.extend(self.prev.keys().chain(cur.keys()).map(|(p, _)| p.clone()));
            }
            changed.extend(self.owed.iter().cloned());
            if !changed.is_empty() {
                let bump = bump(changed.into_iter().collect());
                let projects = tokio::time::timeout(STATUS_WATCH_STORE_TIMEOUT, bump)
                    .await
                    .map_err(|_| {
                        anyhow::anyhow!(
                            "status watcher project bump: no answer in {secs}s, outcome unknown"
                        )
                    })?
                    .context("status watcher project bump")?;
                self.owed.clear();
                for (project, version) in projects {
                    match version {
                        Some(version) => announced.push((project, version)),
                        None => {
                            self.owed.insert(project);
                        }
                    }
                }
            }
        }
        self.seeded = true;
        self.prev = cur;
        self.degraded = false;
        Ok(announced)
    }
}

#[cfg(test)]
mod status_watch_tests {
    use super::*;

    type Bumped = anyhow::Result<Vec<(String, Option<u64>)>>;

    /// A scan at `server_now_ms` of runs that logged a main metric at 0.
    async fn scan(server_now_ms: i64, runs: &[(&str, &str)]) -> anyhow::Result<TimedRunRows> {
        let rows = runs
            .iter()
            .map(|&(project_id, run_id)| RunInfoRow {
                project_id: project_id.into(),
                run_id: run_id.into(),
                run_name: String::new(),
                ordinal: 1,
                created_at_ms: 0,
                last_main_metric_at_ms: Some(0),
                last_system_metric_at_ms: None,
                last_ingested_at_ms: None,
                terminated_at_ms: None,
                exit_code: None,
            })
            .collect();
        Ok(TimedRunRows {
            rows,
            server_now_ms,
        })
    }

    fn no_bump(ids: Vec<String>) -> std::future::Ready<Bumped> {
        panic!("unexpected bump of {ids:?}")
    }

    /// A tick with one stalled store call, the other answering at once: it must end by that call's deadline. (A tick whose scan answers late and whose bump then stalls can take both deadlines.)
    async fn ended<T>(tick: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(STATUS_WATCH_STORE_TIMEOUT + Duration::from_secs(1), tick)
            .await
            .expect("the tick outlived its store deadline")
    }

    /// A store call that never answers fails its tick at the deadline, keeping what the watch had seen; the next healthy tick, the first scan included, re-bumps every project seen, a skipped project stays owed until a bump takes it (a tick that skips every project pushes nothing), and nothing is bumped once all are announced.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_store_call_fails_its_tick_and_the_next_heals() {
        let mut watch = StatusWatch::default();
        let state = |watch: &StatusWatch| (watch.seeded, watch.degraded, watch.prev.len());
        let started = tokio::time::Instant::now();
        assert!(ended(watch.tick(std::future::pending(), no_bump))
            .await
            .is_err());
        assert_eq!(started.elapsed(), STATUS_WATCH_STORE_TIMEOUT);
        assert_eq!(state(&watch), (false, true, 0));
        // After a failed first scan the first healthy one heals: the restart's resyncs predate its transitions.
        let healed = watch.tick(scan(20_000, &[("p", "r")]), |ids| {
            assert_eq!(ids, ["p"]);
            std::future::ready(Ok(vec![("p".into(), Some(1))]))
        });
        assert_eq!(healed.await.unwrap(), [("p".into(), 1)]);
        assert_eq!(state(&watch), (true, false, 1));
        let quiet = watch.tick(scan(20_000, &[("p", "r")]), no_bump);
        assert!(quiet.await.unwrap().is_empty());
        // A clean first scan seeds silently.
        let mut clean = StatusWatch::default();
        let seeded = clean.tick(scan(0, &[("p", "r")]), no_bump);
        assert!(seeded.await.unwrap().is_empty());
        assert_eq!(state(&clean), (true, false, 1));

        assert!(ended(watch.tick(std::future::pending(), no_bump))
            .await
            .is_err());
        assert_eq!(state(&watch), (true, true, 1));
        let mut asked = Vec::new();
        let hung = watch.tick(scan(0, &[("p", "r")]), |ids| {
            asked = ids;
            std::future::pending::<Bumped>()
        });
        assert!(ended(hung).await.is_err());
        assert_eq!(asked, ["p"]);
        assert_eq!(state(&watch), (true, true, 1));

        let pushed = watch.tick(scan(20_000, &[("p", "r"), ("q", "s")]), |ids| {
            assert_eq!(ids, ["p", "q"]);
            std::future::ready(Ok(vec![("p".into(), Some(4)), ("q".into(), None)]))
        });
        assert_eq!(pushed.await.unwrap(), [("p".into(), 4)]);
        let skipped = watch.tick(scan(20_000, &[("p", "r"), ("q", "s")]), |ids| {
            assert_eq!(ids, ["q"]);
            std::future::ready(Ok(vec![("q".into(), None)]))
        });
        assert!(skipped.await.unwrap().is_empty());
        assert_eq!(state(&watch), (true, false, 2));
        let pushed = watch.tick(scan(20_000, &[("p", "r"), ("q", "s")]), |ids| {
            assert_eq!(ids, ["q"]);
            std::future::ready(Ok(vec![("q".into(), Some(9))]))
        });
        assert_eq!(pushed.await.unwrap(), [("q".into(), 9)]);
        let quiet = watch.tick(scan(20_000, &[("p", "r"), ("q", "s")]), no_bump);
        assert!(quiet.await.unwrap().is_empty());
    }

    /// A bump that fails leaves owed what it owed, a project no longer watched included.
    #[tokio::test(start_paused = true)]
    async fn a_failed_bump_keeps_what_it_owed() {
        let mut watch = StatusWatch::default();
        let seeded = watch.tick(scan(0, &[("p", "r"), ("q", "s")]), no_bump);
        assert!(seeded.await.unwrap().is_empty());
        assert!(ended(watch.tick(std::future::pending(), no_bump))
            .await
            .is_err());
        // q's run has left the window; the heal bumps p and q, and q's row is held.
        let healed = watch.tick(scan(0, &[("p", "r")]), |ids| {
            assert_eq!(ids, ["p", "q"]);
            std::future::ready(Ok(vec![("p".into(), Some(1)), ("q".into(), None)]))
        });
        assert_eq!(healed.await.unwrap(), [("p".into(), 1)]);
        let failed = watch.tick(scan(0, &[("p", "r")]), |ids| {
            assert_eq!(ids, ["q"]);
            std::future::ready(Err(anyhow::anyhow!("lost")))
        });
        assert!(failed.await.is_err());
        let healed = watch.tick(scan(0, &[("p", "r")]), |ids| {
            assert_eq!(ids, ["p", "q"]);
            std::future::ready(Ok(vec![("p".into(), Some(2)), ("q".into(), Some(5))]))
        });
        assert_eq!(healed.await.unwrap(), [("p".into(), 2), ("q".into(), 5)]);
    }
}

#[cfg(test)]
mod frontier_delta_tests {
    use super::*;
    use held::same_model as eq;
    use lineage::{lineage_id, parse_lineages, LINEAGE_PREFIX, LINEAGE_VERSION_KEY};

    use crate::series_cache::LineageOrigin;
    use crate::Rng;
    use lineage::Rejection;
    use std::sync::Arc;

    /// [`super::build_chart`] over the request's parameters as its poll plans them ([`chart_params`]).
    pub(super) fn build_chart(
        req: &proto::ChartRequest,
        snapshots: &[Arc<SeriesSnapshot>],
        x_maps: Option<&std::collections::HashMap<String, std::collections::HashMap<i64, f64>>>,
        audit: bool,
        memo: Option<&proto::ChartResponse>,
        on_dense: &dyn Fn(DenseShape),
        capture: bool,
    ) -> Result<BuiltChart, Status> {
        let p = chart_params(req);
        super::build_chart(req, &p, snapshots, x_maps, audit, memo, on_dense, capture)
    }

    mod bench;
    mod held_tests;
    mod lineage_tests;
    mod marker_delta_tests;
    mod memo_tests;
    mod smoother_hole_tests;

    const ALGORITHMS: [Algorithm; 4] = [
        Algorithm::None,
        Algorithm::Ema,
        Algorithm::Triangular,
        Algorithm::SavitzkyGolay,
    ];

    fn verified_inputs(
        request: &proto::ChartRequest,
        rows: &[Arc<Vec<VersionedRawPoint>>],
        state: &std::collections::HashMap<String, i64>,
    ) -> Option<VerifiedLineages> {
        let mut echoed = request.clone();
        echoed.cache_state = Some(proto::ChartCacheState {
            frontiers: state.clone(),
            ..Default::default()
        });
        inspect_lineages(request, &fixture_snapshots(&echoed, rows), Some(state))
            .verification
            .and_then(Result::ok)
    }
    fn stamped_cutoff(
        request: &proto::ChartRequest,
        index: usize,
        state: &std::collections::HashMap<String, i64>,
    ) -> Option<i64> {
        parse_lineages(state)?
            .get(lineage_id(request, index).as_str())
            .map(|row| row.maximum)
    }

    fn scalar_row(step: i64, value: f32) -> VersionedRawPoint {
        VersionedRawPoint {
            tag: String::new(),
            step,
            timestamp_ms: 1_000_000 + step,
            value,
            is_value: 1,
            inserted_ms: step * 10_000_000,
        }
    }

    /// `rows` in (tag, step) order, as ClickHouse returns them.
    fn sorted(rows: &[VersionedRawPoint]) -> Vec<VersionedRawPoint> {
        let mut rows = rows.to_vec();
        rows.sort_by(|a, b| (&a.tag, a.step).cmp(&(&b.tag, b.step)));
        rows
    }

    /// Rows for one scalar series: steps 0..n, inserted far enough apart that the verified cutoffs distinguish successive appends. `f(step)` gives the value, so tests can rewrite history.
    fn rows_with(n: i64, f: impl Fn(i64) -> f32) -> Arc<Vec<VersionedRawPoint>> {
        Arc::new((0..n).map(|step| scalar_row(step, f(step))).collect())
    }

    fn rows(n: i64, seed: i64) -> Arc<Vec<VersionedRawPoint>> {
        rows_with(n, move |s| ((s * seed) % 97) as f32)
    }

    fn rows_at(points: &[(i64, f32)]) -> Arc<Vec<VersionedRawPoint>> {
        Arc::new(
            points
                .iter()
                .map(|&(step, value)| scalar_row(step, value))
                .collect(),
        )
    }

    /// Refresh a cached series by `rows`, as a poll after its run's bump does.
    pub(super) fn cache_append(
        cache: &crate::series_cache::SeriesCache,
        key: &crate::series_cache::SeriesKey,
        rows: Vec<VersionedRawPoint>,
    ) -> Arc<SeriesSnapshot> {
        cache.note_bumps(std::iter::once(key.run_id.as_str()));
        let crate::series_cache::Lookup::Stale { base, .. } = cache.lookup(key) else {
            panic!("bumped fixture must refresh its retained snapshot");
        };
        cache
            .apply_increment(key, rows, std::time::Instant::now(), &base)
            .unwrap()
    }

    #[test]
    fn lineage_identities_preserve_distinct_valid_identifier_pairs() {
        let first = proto::SeriesRef {
            run_id: "a\u{1f}b".to_string(),
            metric_name: "c".to_string(),
            ..Default::default()
        };
        let second = proto::SeriesRef {
            run_id: "a".to_string(),
            metric_name: "b\u{1f}c".to_string(),
            ..Default::default()
        };
        let request = proto::ChartRequest {
            y_series: vec![first.clone(), second.clone()],
            ..Default::default()
        };
        let rows = [rows_at(&[(1, 1.0)]), rows_at(&[(2, 2.0)])];

        let lineages = inspect_lineages(&request, &fixture_snapshots(&request, &rows), None);
        let mut frontiers = std::collections::HashMap::new();
        stamp_lineages(&mut frontiers, &lineages);
        assert_eq!(frontiers.len(), 3);
        assert_ne!(lineage_id(&request, 0), lineage_id(&request, 1));
        assert!(
            frontiers.keys().all(|key| key.starts_with('\0')),
            "older watermark-only servers must find no data frontier keys"
        );
    }

    /// `algorithm` with these parameters; none for [`Algorithm::None`].
    fn smoothing(
        algorithm: Algorithm,
        window_size: u32,
        time_constant: f64,
        poly_order: u32,
    ) -> Option<proto::SmoothingConfig> {
        (algorithm != Algorithm::None).then_some(proto::SmoothingConfig {
            algorithm: algorithm as i32,
            window_size,
            time_constant,
            poly_order,
        })
    }

    fn req(runs: &[&str], target: u32) -> proto::ChartRequest {
        proto::ChartRequest {
            y_series: runs
                .iter()
                .map(|r| proto::SeriesRef {
                    project_id: "p".into(),
                    run_id: (*r).into(),
                    metric_name: "loss".into(),
                    tags: vec![],
                })
                .collect(),
            target_resolution: target,
            ..Default::default()
        }
    }

    /// A tag forms a group only when it keeps a row: under `old_only`, a tag holding only new rows forms none, so the scalar rows its group suppresses in the full build are the held reconstruction's series.
    #[test]
    fn old_only_groups_form_from_held_rows_alone() {
        let request = req(&["a"], 300);
        let held = build(&request, &[rows(20, 3)]);
        let tagged = (20..25).map(|step| VersionedRawPoint {
            tag: "t".into(),
            ..scalar_row(step, step as f32)
        });
        let grown = Arc::new(rows(20, 3).iter().cloned().chain(tagged).collect());
        let grown = std::slice::from_ref(&grown);
        let continued = continuation(&request, &held);
        let verified = verified_inputs(&continued, grown, &held.frontiers).expect("held rows");
        let p = chart_params(&continued);
        let snapshots = fixture_snapshots(&continued, grown);
        let groups = |old_only| -> Vec<String> {
            prepare(
                &continued,
                &p,
                &snapshots,
                None,
                Some(&verified),
                old_only,
                None,
                false,
            )
            .unwrap()
            .series
            .into_iter()
            .map(|ps| ps.identity.tag)
            .collect()
        };
        assert_eq!(groups(false), ["t"]);
        assert_eq!(groups(true), [""]);
    }

    #[test]
    fn timestamp_axis_discards_retained_custom_x_before_chart_planning() {
        let mut request = req(&["a"], 300);
        request.use_timestamp_axis = true;
        request.relative_time = true;
        request.log_buckets = true;
        request.smoothing = Some(proto::SmoothingConfig {
            algorithm: Algorithm::SavitzkyGolay as i32,
            window_size: 20,
            ..Default::default()
        });
        let custom_x = proto::SeriesRef {
            metric_name: "train/epoch".into(),
            ..Default::default()
        };
        request.x_series = Some(custom_x.clone());

        canonicalize_chart_request(&mut request).unwrap();
        assert!(request.x_series.is_none());

        let held_rows = rows_shaped(3_000, |i| i, |i| 1_000_000 + 10 * i);
        let held = build(&request, std::slice::from_ref(&held_rows));
        let grown_rows = rows_shaped(3_080, |i| i, |i| 1_000_000 + 10 * i);
        let continued = continuation(&request, &held);
        let delta = build(&continued, std::slice::from_ref(&grown_rows));
        assert!(delta.delta);
        assert!(eq(
            &splice(&inflate_full(&held), &delta),
            &inflate_full(&build(&request, std::slice::from_ref(&grown_rows)))
        ));

        let mut step_request = req(&["a"], 300);
        step_request.x_series = Some(custom_x.clone());
        canonicalize_chart_request(&mut step_request).unwrap();
        assert_eq!(step_request.x_series, Some(custom_x));
    }

    #[test]
    fn ema_rejects_a_time_constant_that_is_not_finite_and_positive() {
        let ema = |time_constant: f64| {
            let mut request = req(&["a"], 300);
            request.smoothing = Some(proto::SmoothingConfig {
                algorithm: Algorithm::Ema as i32,
                time_constant,
                ..Default::default()
            });
            canonicalize_chart_request(&mut request)
        };
        assert!(ema(7.0).is_ok());
        for time_constant in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert_eq!(
                ema(time_constant).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    /// Retired enum numbers must fail loudly instead of reading as NONE and shipping an unsmoothed chart.
    #[test]
    fn retired_smoothing_algorithms_are_invalid_arguments() {
        for algorithm in [1, 3] {
            let mut request = req(&["a"], 300);
            request.smoothing = Some(proto::SmoothingConfig {
                algorithm,
                window_size: 20,
                ..Default::default()
            });
            let err = canonicalize_chart_request(&mut request).unwrap_err();
            assert_eq!(err.code(), tonic::Code::InvalidArgument, "{algorithm}");
            assert!(err.message().contains(&algorithm.to_string()), "{err:?}");
        }
    }

    #[test]
    fn oversized_smoothing_windows_use_the_execution_cap_everywhere() {
        let mut request = req(&["a"], 300);
        request.step_min = Some(100_000);
        request.step_max = Some(200_000);
        request.smoothing = Some(proto::SmoothingConfig {
            algorithm: Algorithm::SavitzkyGolay as i32,
            window_size: u32::MAX,
            ..Default::default()
        });

        let params = chart_params(&request);
        assert_eq!(params.window_size, chart::MAX_SMOOTHING_WINDOW);
        assert_eq!(params.reach, chart::MAX_SMOOTHING_WINDOW as usize);
        let margin = i64::from(chart::MAX_SMOOTHING_WINDOW) * 4;
        assert_eq!(params.fetch_min, 100_000 - margin);
        assert_eq!(params.fetch_max, 200_000 + margin);

        let mut ages = vec![chart::AGE_OLD; chart::MAX_SMOOTHING_WINDOW as usize + 2];
        *ages.last_mut().unwrap() = chart::AGE_NEW;
        let xs = (0..ages.len())
            .map(|index| index as f64)
            .collect::<Vec<_>>();
        flag_reach(&xs, &mut ages, params.reach, chart::SmoothingPlan::Uniform);
        assert_eq!(ages[0], chart::AGE_OLD);
        assert!(ages[1..ages.len() - 1]
            .iter()
            .all(|age| *age == chart::AGE_REACH));
    }

    /// `request` continued from `held`, as the client polls it next.
    pub(super) fn continuation(
        request: &proto::ChartRequest,
        held: &proto::ChartResponse,
    ) -> proto::ChartRequest {
        proto::ChartRequest {
            cache_state: echo(held),
            ..request.clone()
        }
    }

    /// What the client does: echo the held response's frontiers and series count (chart_sync::echo_state).
    pub(super) fn echo(held: &proto::ChartResponse) -> Option<proto::ChartCacheState> {
        (!held.frontiers.is_empty()).then(|| proto::ChartCacheState {
            frontiers: held.frontiers.clone(),
            held_series: Some(held.series.len() as u32),
        })
    }

    use super::frontier_delta_tests_support::{inflate_full, wire};

    /// What the client does with a delta (chart_sync::splice_response): inflate each tail against the held model, splice, and verify the result hashes.
    fn splice(held: &DenseChart, delta: &proto::ChartResponse) -> DenseChart {
        let c = delta.from_col as usize;
        assert!(delta.delta && c > 0 && c <= held.x_values.len());
        assert_eq!(delta.splice_from_cached.len(), delta.series.len());
        let tail_len = delta.x_values.len();
        let new_len = c + tail_len;
        let mut x_values = held.x_values[..c].to_vec();
        x_values.extend_from_slice(&delta.x_values);
        let mut xr_min = held.xr_min[..c].to_vec();
        xr_min.extend(
            chart_delta::expand_segments(
                &delta.xr_seg_starts,
                &delta.xr_seg_lens,
                &delta.xr_min,
                tail_len,
                c,
            )
            .unwrap(),
        );
        let mut xr_max = held.xr_max[..c].to_vec();
        xr_max.extend(
            chart_delta::expand_segments(
                &delta.xr_seg_starts,
                &delta.xr_seg_lens,
                &delta.xr_max,
                tail_len,
                c,
            )
            .unwrap(),
        );
        let series: Vec<DenseSeries> = delta
            .series
            .iter()
            .zip(&delta.splice_from_cached)
            .map(|(s, &idx)| {
                let w = wire(s);
                if idx < 0 {
                    // complete series: full-axis columns, adopt as-is
                    let env_dense = delta.banded && chart_delta::wire_has_content(&w);
                    return chart_delta::inflate_series(
                        &w,
                        new_len,
                        0,
                        env_dense,
                        !s.raw_values.is_empty(),
                    )
                    .unwrap();
                }
                let old = &held.series[idx as usize];
                // Family existence for an all-gap tail comes from the held series; a family springing into existence (first finite samples) materializes its NaN prefix in splice_series.
                let env_dense = delta.banded
                    && (!old.min_values.is_empty() || chart_delta::wire_has_content(&w));
                let raw_dense = !old.raw_values.is_empty() || !s.raw_values.is_empty();
                let tail =
                    chart_delta::inflate_series(&w, tail_len, c, env_dense, raw_dense).unwrap();
                chart_delta::splice_series(old, &tail, c)
            })
            .collect();
        let out = DenseChart {
            x_values,
            xr_min,
            xr_max,
            series,
        };
        assert_eq!(
            delta.result_x_hash,
            Some(chart_delta::hash_axis(&out)),
            "spliced axis must hash to the server's full model"
        );
        assert_eq!(
            delta.result_series_hashes,
            out.series
                .iter()
                .map(chart_delta::hash_series)
                .collect::<Vec<_>>(),
            "spliced series must hash to the server's full model"
        );
        out
    }

    /// The slot whose raw x or bucket extent holds `x`.
    fn slot_holding(chart: &DenseChart, x: f64) -> usize {
        (0..chart.x_values.len())
            .find(|&i| {
                if chart.xr_min[i].is_nan() {
                    chart.x_values[i] == x
                } else {
                    (chart.xr_min[i]..=chart.xr_max[i]).contains(&x)
                }
            })
            .unwrap()
    }

    /// Verify the actual client splice and its result hashes even when the server's sampled reconstruction audit is disabled.
    fn checked_delta(
        request: &proto::ChartRequest,
        held: &proto::ChartResponse,
        current: &[Arc<Vec<VersionedRawPoint>>],
        audit: bool,
    ) -> FixtureResponse {
        let continued = continuation(request, held);
        let out = build_response(&continued, current, None, audit).unwrap();
        assert!(!out.audit_failed, "audit={audit}: planner lost a prefix");
        assert!(
            out.delta,
            "audit={audit}: the proven prefix must remain reusable"
        );
        let truth = inflate_full(&build(request, current));
        assert!(eq(&splice(&inflate_full(held), &out), &truth));
        out
    }

    fn assert_full_response(
        request: &proto::ChartRequest,
        held: &proto::ChartResponse,
        current: &[Arc<Vec<VersionedRawPoint>>],
        expected_rejection: Option<Rejection>,
    ) -> [FixtureResponse; 2] {
        let continued = continuation(request, held);
        let proof = inspect_lineages(
            &continued,
            &fixture_snapshots(&continued, current),
            continued.cache_state.as_ref().map(|state| &state.frontiers),
        );
        assert_eq!(
            proof.rejection(),
            expected_rejection,
            "full response must exercise its intended gate"
        );
        if expected_rejection.is_none() {
            assert!(matches!(proof.verification, Some(Ok(_))));
        }
        let truth = inflate_full(&build(request, current));
        [false, true].map(|audit| {
            let out = build_response(&continued, current, None, audit).unwrap();
            assert!(!out.delta, "an unprovable continuation must answer in full");
            assert!(
                !out.audit_failed,
                "a gated full response is not an audit failure"
            );
            assert!(eq(&inflate_full(&out), &truth));
            out
        })
    }

    /// Own source snapshots while a fixture response can be echoed. The weak index lets row fixtures
    /// exercise the actual cache merge/lineage transitions without retaining unrelated test history.
    #[derive(Clone, Debug)]
    struct FixtureResponse {
        response: proto::ChartResponse,
        snapshots: Vec<Arc<SeriesSnapshot>>,
    }
    impl std::ops::Deref for FixtureResponse {
        type Target = proto::ChartResponse;
        fn deref(&self) -> &Self::Target {
            &self.response
        }
    }
    impl std::ops::DerefMut for FixtureResponse {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.response
        }
    }
    thread_local! {
        static FIXTURE_SNAPSHOTS: std::cell::RefCell<std::collections::HashMap<(u64, i64), std::sync::Weak<SeriesSnapshot>>> = Default::default();
    }
    fn fixture_snapshots(
        req: &proto::ChartRequest,
        rows: &[Arc<Vec<VersionedRawPoint>>],
    ) -> Vec<Arc<SeriesSnapshot>> {
        let held = req
            .cache_state
            .as_ref()
            .and_then(|state| parse_lineages(&state.frontiers));
        FIXTURE_SNAPSHOTS.with(|index| {
            let mut index = index.borrow_mut();
            index.retain(|_, rows| rows.strong_count() > 0);
            rows.iter()
                .enumerate()
                .map(|(i, rows)| {
                    let old = held
                        .as_ref()
                        .and_then(|held| held.get(lineage_id(req, i).as_str()))
                        .map(|stamp| {
                            index
                                .get(&(stamp.digest, stamp.maximum))
                                .and_then(std::sync::Weak::upgrade)
                                .expect(
                                    "echoed fixture lineage must remain owned by a FixtureResponse",
                                )
                        });
                    match old {
                        Some(old) => old.refreshed_fixture(rows),
                        None => Arc::new(SeriesSnapshot::full(rows.as_ref().clone())),
                    }
                })
                .collect()
        })
    }
    fn build_response(
        req: &proto::ChartRequest,
        rows: &[Arc<Vec<VersionedRawPoint>>],
        x_maps: Option<&std::collections::HashMap<String, std::collections::HashMap<i64, f64>>>,
        audit: bool,
    ) -> Result<FixtureResponse, Status> {
        respond_with(req, rows, |snapshots| {
            super::build_response(req, snapshots, x_maps, audit)
        })
    }
    /// Answer `req` over fixture snapshots with `answer`, keeping the snapshots its stamps name echoable.
    fn respond_with(
        req: &proto::ChartRequest,
        rows: &[Arc<Vec<VersionedRawPoint>>],
        answer: impl FnOnce(&[Arc<SeriesSnapshot>]) -> Result<proto::ChartResponse, Status>,
    ) -> Result<FixtureResponse, Status> {
        let mut snapshots = fixture_snapshots(req, rows);
        let response = answer(&snapshots)?;
        FIXTURE_SNAPSHOTS.with(|index| {
            let mut index = index.borrow_mut();
            if let Some(stamps) = parse_lineages(&response.frontiers) {
                for stamp in stamps.values() {
                    let key = (stamp.digest, stamp.maximum);
                    // Identical authenticated stamps name identical inputs. Share the Arc so either response keeps the weak fixture entry alive.
                    if let Some(existing) = index.get(&key).and_then(std::sync::Weak::upgrade) {
                        snapshots[stamp.order] = existing;
                    } else {
                        index.insert(key, Arc::downgrade(&snapshots[stamp.order]));
                    }
                }
            }
        });
        Ok(FixtureResponse {
            response,
            snapshots,
        })
    }
    /// The production response builder and cache-lineage proof, with the audit oracle forced on.
    fn build(r: &proto::ChartRequest, all_rows: &[Arc<Vec<VersionedRawPoint>>]) -> FixtureResponse {
        build_response(r, all_rows, None, true).unwrap()
    }

    #[test]
    fn downsampled_step_bucket_extents_drive_progressive_zoom() {
        let rows = rows_with(90_000, |step| step as f32);
        let request = req(&["a"], 500);
        let full = inflate_full(&build(&request, std::slice::from_ref(&rows)));

        // 0..=89_999 at target 500 uses width-256 buckets. Their rendered
        // representatives are extent midpoints, so the outer representatives
        // are deliberately not the true data bounds that a zoom must retain.
        assert_eq!(full.x_values.len(), 352);
        assert_eq!(full.x_values.first(), Some(&127.5));
        assert_eq!(full.x_values.last(), Some(&89_927.5));
        assert_eq!(full.xr_min.first(), Some(&0.0));
        assert_eq!(full.xr_max.last(), Some(&89_999.0));

        // Pin one ordinary interior bucket too: selecting this rendered slot
        // must request its whole source extent, not its 35_199.5 midpoint.
        assert_eq!(full.x_values[137], 35_199.5);
        assert_eq!((full.xr_min[137], full.xr_max[137]), (35_072.0, 35_327.0));

        let mut zoomed = request;
        zoomed.step_min = Some(35_072);
        zoomed.step_max = Some(35_327);
        let refined = inflate_full(&build(&zoomed, &[rows]));

        // The inclusive 256-step bucket extent is now below the target and
        // refines to exact raw slots. A midpoint-based request would instead
        // collapse to one step and make progressive zoom impossible.
        assert_eq!(refined.x_values.len(), 256);
        assert!(refined
            .x_values
            .iter()
            .copied()
            .eq((35_072..=35_327).map(|step| step as f64)));
        assert!(refined.xr_min.iter().all(|value| value.is_nan()));
        assert!(refined.xr_max.iter().all(|value| value.is_nan()));
    }

    /// Assert the number of whole-series spacing derivations, not elapsed time: production work must match the algorithm's semantic minimum exactly.
    fn counted<T>(expected: (usize, usize), f: impl FnOnce() -> T) -> T {
        chart::take_spacing_derivations();
        let out = f();
        assert_eq!(
            chart::take_spacing_derivations(),
            expected,
            "(uniform scans, median sorts)"
        );
        out
    }

    #[test]
    fn appends_ship_a_small_tail_that_splices_back() {
        let r = req(&["a", "b"], 300);
        let held = build(&r, &[rows(2000, 3), rows(1600, 7)]);
        assert!(!held.delta && !held.frontiers.is_empty());

        let grown = [rows(2100, 3), rows(1700, 7)];
        let r2 = continuation(&r, &held);
        let out = build(&r2, &grown);
        assert!(out.delta);
        let truth = build(&r, &grown);
        assert!(eq(
            &splice(&inflate_full(&held), &out),
            &inflate_full(&truth)
        ));
        assert!(
            out.x_values.len() < truth.x_values.len() / 2,
            "tail not small: {} of {}",
            out.x_values.len(),
            truth.x_values.len()
        );
    }

    /// The series cache merges raw rows before charting. If the held maximum
    /// lies exactly on a linear grid boundary, an overlapping incremental read
    /// plus tail append must dirty that ordinary boundary cell and splice from
    /// it — never keep the old cell under a newly extended grid.
    #[test]
    fn cached_append_at_linear_upper_boundary_splices_exactly() {
        let r = req(&["a"], 400);
        let cached = rows(1201, 3); // held steps 0..=1200; 1200 is a width-4 boundary
        let held = build(&r, std::slice::from_ref(&cached));
        let held_model = inflate_full(&held);
        assert_eq!(held_model.x_values.len(), 301);
        assert_eq!(held_model.x_values.last(), Some(&1200.0));

        // Model the cache's watermark-overlap read: step 1200 is re-read, then
        // 1201..=1205 append. merge_increment drops the exact overlap.
        let source = rows(1206, 3);
        let merged = Arc::new(
            crate::series_cache::merge_increment(&cached, &source[1200..])
                .expect("tail append must merge into the cached raw series"),
        );
        assert_eq!(merged.len(), source.len());

        let continued = continuation(&r, &held);
        let out = build(&continued, std::slice::from_ref(&merged));
        assert!(out.delta, "boundary append should remain incremental");
        assert_eq!(out.from_col, 300, "the boundary cell itself is dirty");

        let truth = inflate_full(&build(&r, std::slice::from_ref(&merged)));
        assert!(eq(&splice(&held_model, &out), &truth));
    }

    #[test]
    fn smoothed_appends_still_splice_exactly() {
        let mut r = req(&["a"], 250);
        r.smoothing = Some(proto::SmoothingConfig {
            algorithm: proto::smoothing_config::Algorithm::SavitzkyGolay as i32,
            window_size: 20,
            ..Default::default()
        });
        let held = build(&r, &[rows(3000, 5)]);
        let grown = [rows(3080, 5)];
        let r2 = continuation(&r, &held);
        let out = build(&r2, &grown);
        assert!(out.delta, "smoothed append should still delta");
        assert!(eq(
            &splice(&inflate_full(&held), &out),
            &inflate_full(&build(&r, &grown))
        ));
    }

    /// Smoothed envelope values are evaluated at the shared bucket center. A
    /// gappy finite append supplies a new interpolation endpoint and can
    /// therefore change the preceding occupied bucket even when intervening
    /// samples are non-finite. The delta must begin at that finite
    /// predecessor, not merely at the append/marker buckets.
    #[test]
    fn gappy_causal_append_invalidates_preceding_interpolation_bucket() {
        let held_a = rows_at(&[(8, 1.0), (9, 4.0), (16, f32::NAN)]);
        let grown_a = rows_at(&[(8, 1.0), (9, 4.0), (16, f32::NAN), (24, 20.0)]);
        let b = rows(32, 7); // fixes four width-8 shared envelope cells

        for algorithm in [Algorithm::Ema, Algorithm::Triangular] {
            let mut r = req(&["a", "b"], 4);
            r.smoothing = Some(proto::SmoothingConfig {
                algorithm: algorithm as i32,
                window_size: 10,
                time_constant: std::f64::consts::LOG2_E,
                poly_order: 1,
            });
            let held = build(&r, &[held_a.clone(), b.clone()]);
            assert_eq!(inflate_full(&held).x_values, vec![3.5, 11.5, 19.5, 27.5]);

            let continued = continuation(&r, &held);
            let out = build(&continued, &[grown_a.clone(), b.clone()]);
            assert!(!out.audit_failed, "{algorithm:?}: planner lost a prefix");
            assert!(out.delta, "{algorithm:?}: stable grid should still delta");
            assert_eq!(out.from_col, 1, "{algorithm:?}: predecessor bucket");

            let truth = inflate_full(&build(&r, &[grown_a.clone(), b.clone()]));
            assert!(eq(&splice(&inflate_full(&held), &out), &truth));
        }
    }

    /// A centered smoother can change an already-held finite endpoint near a
    /// NEW sample. If non-finite samples separate it from the preceding finite
    /// endpoint, interpolation moves an earlier bucket beyond the smoother's
    /// ordinary index reach; that preceding finite bucket must join the tail.
    #[test]
    fn changed_smoothing_endpoint_invalidates_preceding_finite_bucket() {
        let mut held_points = vec![(9, 1.0)];
        held_points.extend((16..32).map(|step| (step, f32::NAN)));
        held_points.extend([(32, 4.0), (33, f32::NAN)]);
        let mut grown_points = held_points.clone();
        grown_points.push((34, 20.0));
        let held_a = rows_at(&held_points);
        let grown_a = rows_at(&grown_points);
        // Keep five width-8 shared cells while making the anchor's smoother use the irregular, direct-fit path. Its prefix-sum block rounding is a separate dependency covered by the block regressions.
        let b = rows_at(
            &(0..40)
                .filter(|&step| step != 17)
                .map(|step| (step, ((step * 7) % 97) as f32))
                .collect::<Vec<_>>(),
        );

        let mut r = req(&["a", "b"], 5);
        r.smoothing = Some(proto::SmoothingConfig {
            algorithm: Algorithm::SavitzkyGolay as i32,
            window_size: 10,
            poly_order: 1,
            ..Default::default()
        });
        let held = build(&r, &[held_a.clone(), b.clone()]);
        let continued = continuation(&r, &held);
        let out = build(&continued, &[grown_a.clone(), b.clone()]);
        assert!(!out.audit_failed, "planner lost a prefix");
        assert!(out.delta, "stable grid should still delta");
        assert_eq!(out.from_col, 1, "finite predecessor");

        let truth = inflate_full(&build(&r, &[grown_a.clone(), b.clone()]));
        assert!(eq(&splice(&inflate_full(&held), &out), &truth));
    }

    /// A verified new negative timestamp can reuse earlier shared columns: causal influence is marked before filtering, and the new kind-4 marker dirties its anchor's cell. Here the centered smoothers answer in full because the timestamp changes their exact plan from Uniform to Median.
    #[test]
    fn verified_new_unplottable_timestamp_bounds_causal_influence_and_dirties_its_anchor() {
        let held_a = rows_shaped(10, |i| i, |i| 64 + 10 * i);
        let grown_a = rows_shaped(11, |i| i, |i| if i == 10 { -10 } else { 64 + 10 * i });
        let b = rows_shaped(301, |i| i, |i| i);

        for algorithm in ALGORITHMS {
            let mut r = req(&["a", "b"], 4);
            r.use_timestamp_axis = true;
            r.log_buckets = true;
            r.smoothing = smoothing(algorithm, 10, std::f64::consts::LOG2_E, 1);
            let held = build(&r, &[held_a.clone(), b.clone()]);
            let held_model = inflate_full(&held);
            assert!(held_model.series[0].nan_indices.is_empty());

            let continued = continuation(&r, &held);
            let out = build(&continued, &[grown_a.clone(), b.clone()]);
            let truth = inflate_full(&build(&r, &[grown_a.clone(), b.clone()]));
            assert_eq!(
                out.delta,
                matches!(
                    algorithm,
                    Algorithm::None | Algorithm::Ema | Algorithm::Triangular
                ),
                "{algorithm:?}"
            );
            assert!(
                !out.audit_failed,
                "{algorithm:?}: a gate is not an audit failure"
            );
            let rebuilt = if out.delta {
                splice(&held_model, &out)
            } else {
                inflate_full(&out)
            };
            assert!(eq(&rebuilt, &truth));
            assert_eq!(truth.series[0].nan_kinds, vec![4]);
            assert_eq!(
                truth.series[0].nan_indices,
                vec![slot_holding(&truth, 64.0) as u32],
                "the marker sits on the slot holding A's first plotted x"
            );
        }
    }

    #[test]
    fn negative_log_step_in_a_continuing_run_reuses_prefix() {
        let held_rows = rows_shaped(6, |i| i - 1, |i| 1_000_000 + i);
        let grown_rows = rows_shaped(7, |i| i - 1, |i| 1_000_000 + i);
        let mut r = req(&["a"], 1_000);
        r.log_buckets = true;

        let held = build(&r, std::slice::from_ref(&held_rows));
        let held_model = inflate_full(&held);
        assert_eq!(held_model.x_values.first(), Some(&0.0));
        assert_eq!(held_model.series[0].nan_kinds, vec![4]);
        assert_eq!(held_model.series[0].nan_indices, vec![0]);

        let continued = continuation(&r, &held);
        let out = build(&continued, std::slice::from_ref(&grown_rows));
        let truth = inflate_full(&build(&r, std::slice::from_ref(&grown_rows)));
        assert!(out.delta, "the settled negative step marker is stable");
        assert!(!out.audit_failed);
        assert!(eq(&splice(&held_model, &out), &truth));
    }

    #[test]
    fn relative_negative_timestamps_remain_incremental() {
        let held_rows = rows_shaped(10, |i| i, |i| -100 + 10 * i);
        let grown_rows = rows_shaped(11, |i| i, |i| if i == 10 { -5 } else { -100 + 10 * i });
        let mut r = req(&["a"], 1_000);
        r.use_timestamp_axis = true;
        r.relative_time = true;
        r.log_buckets = true;

        let held = build(&r, std::slice::from_ref(&held_rows));
        let continued = continuation(&r, &held);
        let out = build(&continued, std::slice::from_ref(&grown_rows));
        assert!(
            out.delta,
            "relative offsets make every plotted x nonnegative"
        );

        let truth = inflate_full(&build(&r, std::slice::from_ref(&grown_rows)));
        assert!(eq(&splice(&inflate_full(&held), &out), &truth));
        assert!(truth.series[0].nan_indices.is_empty());
    }

    #[test]
    fn added_run_with_same_steps_ships_only_its_series() {
        let r2runs = req(&["a", "b"], 300);
        let held = build(&r2runs, &[rows(2000, 3), rows(2000, 7)]);

        let mut r3runs = req(&["a", "b", "c"], 300);
        r3runs.cache_state = echo(&held);
        let grown = [rows(2000, 3), rows(2000, 7), rows(1500, 11)];
        let out = build(&r3runs, &grown);
        assert!(out.delta);
        assert_eq!(out.splice_from_cached, vec![0, 1, -1]);
        // Added runs ship complete over the unchanged axis; continuing runs have no tail.
        assert!(out.x_values.is_empty());
        let from = out.from_col as usize;
        assert!(
            out.series[0].seg_starts.iter().all(|&s| s as usize >= from),
            "continuing series carry only the tail"
        );
        assert!(out.series[1].seg_starts.iter().all(|&s| s as usize >= from));
        let truth = build(&req(&["a", "b", "c"], 300), &grown);
        assert_eq!(
            out.series[2].seg_starts.first(),
            Some(&0),
            "the new run ships complete, full-axis indexed"
        );
        assert!(eq(
            &splice(&inflate_full(&held), &out),
            &inflate_full(&truth)
        ));
    }

    #[test]
    fn rewritten_cached_rows_answer_full() {
        let r = req(&["a"], 300);
        let held = build(&r, &[rows(2000, 3)]);
        // Step 100 re-logged with a fresh insert time: history rewritten.
        let rewritten = Arc::new(
            rows(2000, 3)
                .iter()
                .cloned()
                .map(|mut row| {
                    if row.step == 100 {
                        row.value = 999.0;
                        row.inserted_ms = 2000 * 10_000_000;
                    }
                    row
                })
                .collect::<Vec<_>>(),
        );
        let r2 = continuation(&r, &held);
        let out = build(&r2, std::slice::from_ref(&rewritten));
        assert!(!out.delta, "a rewrite must gate to a full answer");
        assert!(eq(
            &inflate_full(&out),
            &inflate_full(&build(&r, &[rewritten]))
        ));
    }

    #[test]
    fn removed_refs_and_missing_held_count_fail_closed_after_lineage_verification() {
        let request = req(&["a", "b"], 300);
        let current = [rows(2000, 3), rows(1600, 7)];
        let held = build(&request, &current);
        // Positive control: both refs, their proof and the ordinary count permit continuation.
        for audit in [false, true] {
            checked_delta(&request, &held, &current, audit);
        }
        let removed = req(&["a"], 300);
        assert_full_response(&removed, &held, &current[..1], None);
        // Keep every ref so missing count, rather than membership loss, is the only refusal.
        let mut continued = continuation(&request, &held);
        continued.cache_state.as_mut().unwrap().held_series = None;
        assert!(verified_inputs(&continued, &current, &held.frontiers).is_some());
        let truth = inflate_full(&held);
        for audit in [false, true] {
            let out = build_response(&continued, &current, None, audit).unwrap();
            assert!(!out.delta && !out.audit_failed);
            assert!(eq(&inflate_full(&out), &truth));
        }
    }

    #[test]
    fn grid_tier_crossing_answers_full() {
        // The appended span doubles the bucket width: every boundary moved.
        let r = req(&["a"], 500);
        let held = build(&r, &[rows(3000, 3)]);
        let r2 = continuation(&r, &held);
        let out = build(&r2, &[rows(9000, 3)]);
        assert!(!out.delta);
        assert!(eq(
            &inflate_full(&out),
            &inflate_full(&build(&r, &[rows(9000, 3)]))
        ));
    }

    /// Rows with caller-chosen plot geometry: `step(i)` and `ts(i)` per row,
    /// inserted far apart so held cutoffs distinguish appends by index.
    fn rows_shaped(
        n: i64,
        step: impl Fn(i64) -> i64,
        ts: impl Fn(i64) -> i64,
    ) -> Arc<Vec<VersionedRawPoint>> {
        Arc::new(
            (0..n)
                .map(|i| VersionedRawPoint {
                    tag: String::new(),
                    step: step(i),
                    timestamp_ms: ts(i),
                    value: ((i * 5) % 97) as f32,
                    is_value: 1,
                    inserted_ms: i * 10_000_000,
                })
                .collect(),
        )
    }

    #[test]
    fn smoothing_derives_only_the_semantic_minimum() {
        let shaped = |uniform: bool| {
            rows_shaped(
                24,
                |i| i + i / 5,
                |i| 1_000_000 + 10 * i + if uniform { 0 } else { i / 4 },
            )
        };
        let cases = [
            (Algorithm::None, false, true, (0, 0)),
            (Algorithm::Ema, false, true, (0, 0)),
            (Algorithm::Triangular, false, true, (0, 0)),
            (Algorithm::Ema, true, false, (0, 1)),
            (Algorithm::Triangular, true, false, (0, 1)),
            (Algorithm::SavitzkyGolay, true, true, (1, 0)),
            (Algorithm::SavitzkyGolay, true, false, (1, 1)),
        ];
        for (algorithm, use_time, uniform, expected) in cases {
            let mut r = req(&["a"], 100);
            r.use_timestamp_axis = use_time;
            r.smoothing = smoothing(algorithm, 9, std::f64::consts::LOG2_E, 1);
            let out = counted(expected, || {
                build_response(&r, &[shaped(uniform)], None, false).unwrap()
            });
            let has_exact =
                out.frontiers.get(SMOOTHING_STATE_VERSION_KEY) == Some(&SMOOTHING_STATE_VERSION);
            assert_eq!(
                has_exact,
                use_time || algorithm == Algorithm::SavitzkyGolay,
                "{algorithm:?} use_time={use_time}"
            );
        }
    }

    #[test]
    fn irrelevant_spacing_state_does_not_gate_causal_smoothers() {
        for algorithm in [Algorithm::Ema, Algorithm::Triangular] {
            let mut step_req = req(&["a"], 100);
            step_req.smoothing = Some(proto::SmoothingConfig {
                algorithm: algorithm as i32,
                time_constant: std::f64::consts::LOG2_E,
                poly_order: 1,
                ..Default::default()
            });
            let step_shape = |n: i64| {
                rows_shaped(
                    n,
                    |i| {
                        if i < 8 {
                            2 * i
                        } else {
                            16 + (i - 8)
                        }
                    },
                    |i| 1_000_000 + i,
                )
            };
            let held = build(&step_req, &[step_shape(8)]);
            assert!(!held.frontiers.contains_key(SMOOTHING_STATE_VERSION_KEY));
            let grown = [step_shape(20)];
            let continued = continuation(&step_req, &held);
            let out = counted((0, 0), || build(&continued, &grown));
            assert!(out.delta, "{algorithm:?}: step cadence has no global state");
            assert!(eq(
                &splice(&inflate_full(&held), &out),
                &inflate_full(&build(&step_req, &grown))
            ));

            // Time EMA/Triangular do need the median, but not the uniformity branch. Appending 12ms then 8ms gaps flips uniformity while keeping the upper median exactly 10ms.
            let mut time_req = step_req.clone();
            time_req.use_timestamp_axis = true;
            let time_shape = |n: i64| {
                rows_shaped(
                    n,
                    |i| i,
                    |i| match i {
                        0..=19 => 1_000_000 + 10 * i,
                        20 => 1_000_202,
                        _ => 1_000_210 + 10 * (i - 21),
                    },
                )
            };
            let held = build(&time_req, &[time_shape(20)]);
            let grown = [time_shape(22)];
            let continued = continuation(&time_req, &held);
            let out = counted((0, 1), || build(&continued, &grown));
            assert!(
                out.delta,
                "{algorithm:?}: stable median must ignore uniformity flip"
            );
            assert!(eq(
                &splice(&inflate_full(&held), &out),
                &inflate_full(&build(&time_req, &grown))
            ));
        }
    }

    fn savgol_req(runs: &[&str], target: u32) -> proto::ChartRequest {
        let mut r = req(runs, target);
        r.smoothing = Some(proto::SmoothingConfig {
            algorithm: proto::smoothing_config::Algorithm::SavitzkyGolay as i32,
            window_size: 20,
            ..Default::default()
        });
        r
    }

    /// A zoomed triangular request must fetch from the run's very start — its
    /// weights never decay, so any earlier step (a positive step_min above a
    /// negative-stepped history included) still affects the in-range fit. It
    /// stays causal: nothing is fetched past step_max, and its delta reach is 0.
    #[test]
    fn triangular_fetch_spans_whole_run() {
        let mut r = req(&["a"], 100);
        r.step_min = Some(5_000);
        r.step_max = Some(6_000);
        r.smoothing = Some(proto::SmoothingConfig {
            algorithm: proto::smoothing_config::Algorithm::Triangular as i32,
            poly_order: 1,
            ..Default::default()
        });
        let p = chart_params(&r);
        assert_eq!(
            p.fetch_min,
            i64::MIN,
            "must reach back to the first step, not step_min − margin"
        );
        assert_eq!(p.fetch_max, 6_000, "causal: no look-ahead past step_max");
        assert_eq!(p.reach, 0);
    }

    #[test]
    fn smoothing_semantic_plan_shift_answers_full() {
        // Appended intervals change the exact time median from 12 to 10, rescaling every Savitzky–Golay output. The semantic-plan gate must reject before the sampled audit.
        let ts = |i: i64| {
            1_000_000 + 10 * i.min(1000) + 12 * (i - 1000).clamp(0, 1000) + 10 * (i - 2000).max(0)
        };
        let mut r = savgol_req(&["a"], 300);
        r.use_timestamp_axis = true;
        let held = build(&r, &[rows_shaped(2001, |i| i, ts)]);
        let grown = [rows_shaped(2041, |i| i, ts)];
        let r2 = continuation(&r, &held);
        for audit in [false, true] {
            let out = build_response(&r2, &grown, None, audit).unwrap();
            assert!(!out.delta, "median shift must gate to full (audit={audit})");
            assert!(eq(
                &inflate_full(&out),
                &inflate_full(&build_response(&r, &grown, None, false).unwrap())
            ));
        }

        // Step axis, Savitzky–Golay: held logged every step (uniform spacing), the
        // appends log every 3rd step — smooth_run switches from the
        // index-space smoother to the x-aware one, moving every output.
        let r = savgol_req(&["a"], 300);
        let held = build(&r, &[rows_shaped(2000, |i| i, |i| 1_000_000 + i)]);
        let stride = |i: i64| i.min(2000) + 3 * (i - 2000).max(0);
        let grown = [rows_shaped(2060, stride, |i| 1_000_000 + i)];
        let r2 = continuation(&r, &held);
        let out = build(&r2, &grown);
        assert!(!out.delta, "a uniform-spacing flip must gate to full");
        assert!(eq(&inflate_full(&out), &inflate_full(&build(&r, &grown))));
    }

    /// Historical jitter fixture: median spacing can change globally after an append even though every held row is still present. Lineage verification proves the rows, but the exact smoothing-plan gate must still reject a changed median.
    #[test]
    fn changed_exact_time_median_answers_full() {
        fn shaped(ts: &[i64], inserted: &[i64], tags: &[&str]) -> Arc<Vec<VersionedRawPoint>> {
            Arc::new(
                tags.iter()
                    .enumerate()
                    .flat_map(|(tag_i, &tag)| {
                        ts.iter()
                            .zip(inserted)
                            .enumerate()
                            .map(move |(i, (&t, &inserted_ms))| VersionedRawPoint {
                                tag: tag.to_string(),
                                step: i as i64,
                                timestamp_ms: 1_700_000_000_000 + t,
                                value: ((i * 7 + tag_i) % 13) as f32,
                                is_value: 1,
                                inserted_ms,
                            })
                    })
                    .collect(),
            )
        }

        for algorithm in [
            proto::smoothing_config::Algorithm::Ema,
            proto::smoothing_config::Algorithm::Triangular,
            proto::smoothing_config::Algorithm::SavitzkyGolay,
        ] {
            let mut r = req(&["a", "a"], 100);
            r.y_series[0].metric_name = "system/gpu_mem_used_bytes".into();
            r.y_series[1].metric_name = "system/disk_write_mbps".into();
            r.use_timestamp_axis = true;
            r.relative_time = true;
            r.smoothing = Some(proto::SmoothingConfig {
                algorithm: algorithm as i32,
                window_size: 3,
                time_constant: 100.0,
                poly_order: 1,
            });
            let held_rows = [
                shaped(
                    &[0, 1, 2, 3, 5, 7, 9],
                    &[0, 1000, 2000, 3000, 4000, 4500, 5000],
                    &["0", "1"],
                ),
                shaped(
                    &[0, 1, 2, 3, 5, 7, 9],
                    &[0, 1000, 2000, 3000, 4000, 4500, 5000],
                    &[""],
                ),
            ];
            let held = build(&r, &held_rows);
            assert_eq!(held.series.len(), 3, "tagged GPU + scalar disk");
            assert_eq!(stamped_cutoff(&r, 0, &held.frontiers), Some(5000));
            assert_eq!(stamped_cutoff(&r, 1, &held.frontiers), Some(5000));

            let grown = [
                shaped(
                    &[0, 1, 2, 3, 5, 7, 9, 10],
                    &[0, 1000, 2000, 3000, 4000, 4500, 5000, 5500],
                    &["0", "1"],
                ),
                shaped(
                    &[0, 1, 2, 3, 5, 7, 9, 10],
                    &[0, 1000, 2000, 3000, 4000, 4500, 5000, 5500],
                    &[""],
                ),
            ];
            let continued = continuation(&r, &held);
            let out = build(&continued, &grown); // sampled audit forced on
            assert!(
                !out.delta,
                "{algorithm:?}: the exact held smoothing plan differs; answer in full"
            );
            assert!(
                !out.audit_failed,
                "the exact-state gate should fire before the audit"
            );
            assert!(eq(&inflate_full(&out), &inflate_full(&build(&r, &grown))));
        }
    }

    #[test]
    fn stable_semantic_plan_smoothed_charts_still_delta() {
        // Steady 10ms cadence (uniform in time), and an irregular-but-steady
        // pattern (every 5th gap 12ms, median solidly 10): the semantic plan
        // is unchanged by same-shape appends, so smoothed time-axis charts
        // keep their deltas. Audit forced on: the splice must be bit-exact.
        for ts in [(|i: i64| 1_000_000 + 10 * i) as fn(i64) -> i64, |i: i64| {
            1_000_000 + 10 * i + 2 * (i / 5)
        }] {
            let mut r = savgol_req(&["a"], 300);
            r.use_timestamp_axis = true;
            let held = build(&r, &[rows_shaped(3000, |i| i, ts)]);
            let grown = [rows_shaped(3080, |i| i, ts)];
            let r2 = continuation(&r, &held);
            let out = build(&r2, &grown);
            assert!(
                out.delta,
                "stable semantic smoothing plan should still delta"
            );
            let truth = build(&r, &grown);
            assert!(eq(
                &splice(&inflate_full(&held), &out),
                &inflate_full(&truth)
            ));
            assert!(out.x_values.len() < truth.x_values.len() / 2);

            // Rolling out the server against a response created before the exact keys existed costs one full answer, then reseeds them.
            let mut legacy_state = echo(&held).unwrap();
            legacy_state
                .frontiers
                .retain(|key, _| !key.starts_with('\0'));
            let mut legacy_req = r.clone();
            legacy_req.cache_state = Some(legacy_state);
            assert!(!build(&legacy_req, &grown).delta);

            // Every v2 position is required and must decode to NoState, Uniform, or a median that median_dx can produce.
            let mut malformed_state = echo(&held).unwrap();
            malformed_state.frontiers.insert(smoothing_state_key(0), 2);
            let mut malformed_req = r.clone();
            malformed_req.cache_state = Some(malformed_state);
            assert!(!build(&malformed_req, &grown).delta);
        }
    }

    /// The audit reuses the exact matched smoothing plan rather than sorting the verified held inputs to derive it a second time.
    #[test]
    fn smoothed_audit_uses_the_exact_held_plan() {
        fn shaped(ts: &[i64], inserted: &[i64]) -> Arc<Vec<VersionedRawPoint>> {
            Arc::new(
                ts.iter()
                    .zip(inserted)
                    .enumerate()
                    .map(|(i, (&t, &inserted_ms))| VersionedRawPoint {
                        tag: String::new(),
                        step: i as i64,
                        timestamp_ms: 1_700_000_000_000 + t,
                        value: ((i * 11) % 17) as f32,
                        is_value: 1,
                        inserted_ms,
                    })
                    .collect(),
            )
        }

        for algorithm in [
            proto::smoothing_config::Algorithm::Ema,
            proto::smoothing_config::Algorithm::Triangular,
            proto::smoothing_config::Algorithm::SavitzkyGolay,
        ] {
            let mut r = req(&["a"], 100);
            r.use_timestamp_axis = true;
            r.relative_time = true;
            r.smoothing = Some(proto::SmoothingConfig {
                algorithm: algorithm as i32,
                window_size: 3,
                time_constant: std::f64::consts::LOG2_E,
                poly_order: 1,
            });
            let held_rows = [shaped(
                &[0, 2, 4, 6, 7, 8, 9, 10],
                &[0, 1000, 2000, 3000, 4000, 4500, 4750, 5000],
            )];
            let held = build(&r, &held_rows);
            assert_eq!(stamped_cutoff(&r, 0, &held.frontiers), Some(5000));

            let grown = [shaped(
                &[0, 2, 4, 6, 7, 8, 9, 10, 11],
                &[0, 1000, 2000, 3000, 4000, 4500, 4750, 5000, 5500],
            )];
            let continued = continuation(&r, &held);
            let truth = inflate_full(&build(&r, &grown));
            for audit in [false, true] {
                let expected = if matches!(algorithm, Algorithm::Ema | Algorithm::Triangular) {
                    (0, 1)
                } else {
                    (1, 1)
                };
                let out = counted(expected, || {
                    build_response(&continued, &grown, None, audit).unwrap()
                });
                assert!(
                    out.delta,
                    "{algorithm:?}: audit={audit} should retain the delta"
                );
                assert!(!out.audit_failed);
                assert!(eq(&splice(&inflate_full(&held), &out), &truth));
            }
        }
    }

    #[test]
    fn unchanged_data_ships_no_columns() {
        let r = req(&["a"], 300);
        let all = [rows(2000, 3)];
        let held = build(&r, &all);
        let r2 = continuation(&r, &held);
        let out = build(&r2, &all);
        assert!(out.delta);
        assert!(out.x_values.is_empty());
        let held_m = inflate_full(&held);
        assert!(eq(&splice(&held_m, &out), &held_m));
    }

    #[test]
    fn tied_stamps_in_a_held_batch_allow_strictly_newer_appends() {
        // A held batch can share one stamp; a later batch with strictly newer stamps extends the same lineage.
        fn a_rows(n: i64, ins: impl Fn(i64) -> i64) -> Arc<Vec<VersionedRawPoint>> {
            Arc::new(
                (0..n)
                    .map(|s| VersionedRawPoint {
                        inserted_ms: ins(s),
                        ..scalar_row(s, ((s * 13) % 97) as f32)
                    })
                    .collect(),
            )
        }
        let r = req(&["a", "b"], 300);
        let held = build(&r, &[a_rows(1500, |_| 0), rows(2000, 7)]);
        assert_eq!(held.series.len(), 2, "the client holds BOTH series");

        let burst_grown = |s: i64| if s < 1500 { 0 } else { 100 };
        let grown = [a_rows(1550, burst_grown), rows(2100, 7)];
        let r2 = continuation(&r, &held);
        let out = build(&r2, &grown);
        assert!(out.delta, "the verified lineage proves both held inputs");
        assert_eq!(out.splice_from_cached, vec![0, 1]);
        assert!(eq(
            &splice(&inflate_full(&held), &out),
            &inflate_full(&build(&r, &grown))
        ));
    }

    #[test]
    fn margin_only_series_keeps_the_delta_as_all_nan() {
        // Run b's samples sit ONLY in the smoothing fetch margin of the zoomed range (steps 1010.., zoom [0, 1000], Savitzky–Golay window 20 → fetch to 1080): it ships as an all-NaN series the client holds and counts in held_series, while the range trim leaves it no in-range sample. Verified pre-trim membership (PreparedSeries::continues) continues it; without that, the held-count gate answered full on EVERY poll for as long as the shape persisted — and with ascending steps a run parked past the zoom edge persists indefinitely.
        let zoomed = |runs: &[&str]| {
            let mut q = savgol_req(runs, 300);
            q.step_min = Some(0);
            q.step_max = Some(1000);
            q
        };
        // The held margin is uniform, then appended rows make its cadence irregular. Because no point renders, neither shape may derive or gate on a smoothing plan.
        let b = |n: i64| {
            rows_shaped(
                n,
                |i| {
                    if i < 40 {
                        1010 + i
                    } else {
                        1050 + 2 * (i - 40)
                    }
                },
                |i| 1_000_000 + i,
            )
        };
        let r = zoomed(&["a", "b"]);
        let held = build(&r, &[rows(900, 3), b(40)]);
        assert_eq!(
            held.series.len(),
            2,
            "the margin-only run ships all-NaN and the client holds it"
        );
        assert!(
            held.series[1].values.is_empty(),
            "an all-gap series carries no value entries on the wire"
        );
        assert!(inflate_full(&held).series[1]
            .values
            .iter()
            .all(|v| v.is_nan()));
        assert_eq!(held.frontiers.get(&smoothing_state_key(0)), Some(&1));
        assert_eq!(held.frontiers.get(&smoothing_state_key(1)), Some(&0));

        let grown = [rows(950, 3), b(60)];
        let r2 = continuation(&r, &held);
        let out = counted((1, 0), || build(&r2, &grown));
        assert!(out.delta, "a margin-only series must not cost the delta");
        assert_eq!(out.splice_from_cached, vec![0, 1]);
        assert!(eq(
            &splice(&inflate_full(&held), &out),
            &inflate_full(&build(&r, &grown))
        ));

        // A margin-only run the held response never saw (no echoed frontier, nothing provably held) ships COMPLETE — all-NaN over the full axis — inside a normal delta.
        let held_a = build(&zoomed(&["a"]), &[rows(900, 3)]);
        let r3 = continuation(&r, &held_a);
        let out = build(&r3, &[rows(900, 3), b(40)]);
        assert!(out.delta);
        assert_eq!(out.splice_from_cached, vec![0, -1]);
        assert!(eq(
            &splice(&inflate_full(&held_a), &out),
            &inflate_full(&build(&r, &[rows(900, 3), b(40)]))
        ));
    }

    #[test]
    fn late_visibility_breaks_the_lineage_then_resumes_deltas() {
        // A row committed after the held query despite an older insertion stamp. Its appearance invalidates the cache lineage, requiring one full response instead of inventing a held column.
        let now = 1_700_000_000_000i64;
        let mk = |groups: &[&[(i64, i64)]]| -> Arc<Vec<VersionedRawPoint>> {
            Arc::new(
                groups
                    .iter()
                    .flat_map(|g| g.iter())
                    .map(|&(s, ins)| VersionedRawPoint {
                        inserted_ms: ins,
                        ..scalar_row(s, ((s * 13) % 97) as f32)
                    })
                    .collect(),
            )
        };
        let ancient: Vec<(i64, i64)> = (0..1990).map(|s| (s, s)).collect();
        let slow = [(1990i64, now - 10_000)]; // stamped BEFORE fast, committed after the held query
        let fast: Vec<(i64, i64)> = (1991..1996).map(|s| (s, now - 8_000)).collect();
        let appended: Vec<(i64, i64)> = (1996..2001).map(|s| (s, now - 100)).collect();
        let held_rows = mk(&[&ancient, &fast]);
        let grown_rows = mk(&[&ancient, &slow, &fast, &appended]);

        // Target well past the step count: every view is passthrough, isolating held-input verification.
        let r = req(&["a"], 10_000);
        let held = build_response(&r, std::slice::from_ref(&held_rows), None, true).unwrap();
        let mut r2 = continuation(&r, &held);
        let out = build_response(&r2, std::slice::from_ref(&grown_rows), None, true).unwrap();
        let truth = build_response(&r, std::slice::from_ref(&grown_rows), None, true).unwrap();
        assert!(!out.delta, "late visibility changed the verified input set");
        assert!(!out.audit_failed);
        assert!(eq(&inflate_full(&out), &inflate_full(&truth)));
        r2.cache_state = echo(&out);
        let unchanged = build_response(&r2, std::slice::from_ref(&grown_rows), None, true).unwrap();
        assert!(unchanged.delta);
        assert!(unchanged.x_values.is_empty());
        assert!(eq(
            &splice(&inflate_full(&out), &unchanged),
            &inflate_full(&truth)
        ));
    }

    #[test]
    fn passthrough_to_grid_transition_answers_full() {
        // The planner reconstructs the exact held distinct count and compares its passthrough decision with the current one. Straddling the fixed target changes the representation and requires a full response.
        let r = req(&["a"], 300);
        let held = build(&r, &[rows(280, 3)]); // 280 distinct ≤ 300: passthrough
        assert!(!held.banded);
        let r2 = continuation(&r, &held);
        let grown = [rows(1000, 3)]; // 1000 > 300 at width 4: a real grid
        let out = build(&r2, &grown);
        let truth = build(&r, &grown);
        assert!(truth.banded);
        assert!(!out.delta, "a decision flip answers full");
        assert!(eq(&inflate_full(&out), &inflate_full(&truth)));
    }

    #[test]
    fn first_finite_samples_still_delta_with_a_nan_prefix() {
        // Run b logged only non-finite values in the held response — series present (markers), envelope families ABSENT — then its first finite samples arrive. The envelope springs into existence full-length; the splice materializes its NaN prefix (chart_delta.rs), so the chart still deltas instead of re-shipping run a.
        let nan_then_finite = |n: i64| {
            rows_with(n, |s| {
                if s < 1500 {
                    f32::NAN
                } else {
                    ((s * 11) % 89) as f32
                }
            })
        };
        let r = req(&["a", "b"], 300);
        let held = build(&r, &[rows(2000, 3), nan_then_finite(1500)]);
        assert!(
            inflate_full(&held).series[1].min_values.is_empty(),
            "no finite sample: envelope absent"
        );

        let grown = [rows(2100, 3), nan_then_finite(1600)];
        let r2 = continuation(&r, &held);
        let out = build(&r2, &grown);
        assert!(out.delta, "the transition must not cost the delta");
        let truth = inflate_full(&build(&r, &grown));
        assert!(
            !truth.series[1].min_values.is_empty(),
            "envelope present once finite"
        );
        assert!(eq(&splice(&inflate_full(&held), &out), &truth));
    }

    /// A smoothed run with output positions but no finite value has an all-NaN curve regardless of spacing. It must derive no plan, preserve every marker kind without leaking clamped infinities into value families, and keep delta-splicing when its cadence changes from uniform to irregular.
    #[test]
    fn all_marker_smoothed_series_deltas_without_a_plan() {
        let mut r = req(&["a", "b"], 500);
        r.smoothing = Some(proto::SmoothingConfig {
            algorithm: proto::smoothing_config::Algorithm::SavitzkyGolay as i32,
            window_size: 10,
            ..Default::default()
        });
        let finite = |n: i64| rows_with(n, |s| ((s * 7) % 89) as f32);
        let markers = |n: i64, irregular_tail: bool| -> Arc<Vec<VersionedRawPoint>> {
            Arc::new(
                (0..n)
                    .map(|i| {
                        let step = if irregular_tail && i >= 200 {
                            200 + 2 * (i - 200)
                        } else {
                            i
                        };
                        VersionedRawPoint {
                            tag: String::new(),
                            step,
                            timestamp_ms: 1_000_000 + step,
                            value: match i % 3 {
                                0 => f32::NAN,
                                1 => f32::INFINITY,
                                _ => f32::NEG_INFINITY,
                            },
                            is_value: 1,
                            inserted_ms: i * 10_000_000,
                        }
                    })
                    .collect(),
            )
        };

        // Only the finite run scans for spacing; the all-marker run proves it has no finite output and stamps NoState.
        let held = counted((1, 0), || build(&r, &[finite(300), markers(200, false)]));
        assert_eq!(
            held.frontiers.get(&smoothing_state_key(0)),
            Some(&1),
            "the finite uniform run stamps Uniform"
        );
        assert_eq!(
            held.frontiers.get(&smoothing_state_key(1)),
            Some(&0),
            "the all-marker run stamps NoState, not a spacing plan"
        );
        let held_m = inflate_full(&held);
        let held_markers = &held_m.series[1];
        assert!(held_markers.values.iter().all(|v| v.is_nan()));
        assert!(
            held_markers.raw_values.is_empty(),
            "all-marker series carries no raw family"
        );
        assert!(
            held_markers.min_values.is_empty(),
            "all-marker series carries no envelope"
        );
        assert_eq!(held_markers.nan_indices, (0..200u32).collect::<Vec<_>>());
        assert_eq!(
            held_markers.nan_kinds,
            (0..200u32).map(|i| 1 + i % 3).collect::<Vec<_>>()
        );

        let grown = [finite(320), markers(210, true)];
        let r2 = continuation(&r, &held);
        let out = counted((1, 0), || build(&r2, &grown));
        assert!(
            out.delta,
            "an irrelevant cadence change must not gate the chart"
        );
        assert_eq!(out.frontiers.get(&smoothing_state_key(1)), Some(&0));

        let truth = counted((1, 0), || inflate_full(&build(&r, &grown)));
        assert!(eq(&splice(&held_m, &out), &truth));
        let truth_markers = &truth.series[1];
        let expected_indices: Vec<u32> = (0..200u32)
            .chain((200..210u32).map(|i| 200 + 2 * (i - 200)))
            .collect();
        assert_eq!(truth_markers.nan_indices, expected_indices);
        assert_eq!(
            truth_markers.nan_kinds,
            (0..210u32).map(|i| 1 + i % 3).collect::<Vec<_>>()
        );
        assert!(truth_markers.values.iter().all(|v| v.is_nan()));
        assert!(truth_markers.raw_values.is_empty());
        assert!(truth_markers.min_values.is_empty());
    }

    /// A forged/corrupt exact-smoothing cache state — one that UNDERCOUNTS held_series so it slips past the count gate while the old-only audit reconstruction still contains the extra group — must answer in full, never panic (a `NoState` plan shifted onto an active Savitzky–Golay series) or 500 (a plan-slice overrun). The audit keys plans by request/tag identity and gates to full on any mismatch, because `held_series` is client-controlled and its count alone cannot prove old-only and continuing groups align.
    #[test]
    fn forged_smoothing_state_answers_full_not_panic() {
        // A: dense across the zoom (continuing). B: OLD rows only in the warmup margin, NEW rows in-range (provably-held yet NON-continuing). M: margin-only (continuing, NoState).
        let at = |steps: &[i64]| -> Arc<Vec<VersionedRawPoint>> {
            Arc::new(
                steps
                    .iter()
                    .map(|&step| scalar_row(step, ((step * 3) % 97) as f32))
                    .collect(),
            )
        };
        let a: Vec<i64> = (60..=240).collect();
        let mut b: Vec<i64> = (60..=99).collect();
        b.extend(150..=200);
        let m: Vec<i64> = (60..=99).collect();
        // [A,B] overruns the plan slice (a 500); [B,A,M] shifts NoState onto the active Savitzky–Golay series (a panic). The identity gate turns BOTH into a clean full answer.
        type ForgedCase = (
            Vec<&'static str>,
            Vec<Arc<Vec<VersionedRawPoint>>>,
            i64,
            Vec<(usize, i64)>,
        );
        let cases: [ForgedCase; 2] = [
            (vec!["A", "B"], vec![at(&a), at(&b)], 1, vec![(0, 1)]),
            (
                vec!["B", "A", "M"],
                vec![at(&b), at(&a), at(&m)],
                2,
                vec![(0, 1), (1, 0)],
            ),
        ];
        for (runs, rows, series_key, plans) in cases {
            let mut r = req(&runs, 100);
            r.smoothing = Some(proto::SmoothingConfig {
                algorithm: proto::smoothing_config::Algorithm::SavitzkyGolay as i32,
                window_size: 10,
                ..Default::default()
            });
            r.step_min = Some(100);
            r.step_max = Some(200);
            let held_rows: Vec<Arc<Vec<VersionedRawPoint>>> = runs
                .iter()
                .zip(&rows)
                .map(|(run, rows)| {
                    let cutoff = if *run == "A" { 150 } else { 99 } * 10_000_000;
                    Arc::new(
                        rows.iter()
                            .filter(|row| row.inserted_ms <= cutoff)
                            .cloned()
                            .collect(),
                    )
                })
                .collect();
            let held = build(&r, &held_rows);
            let mut frontiers = held.frontiers.clone();
            assert!(verified_inputs(&r, &rows, &frontiers).is_some());
            frontiers.insert(
                SMOOTHING_STATE_VERSION_KEY.to_string(),
                SMOOTHING_STATE_VERSION,
            );
            frontiers.insert(SMOOTHING_STATE_SERIES_KEY.to_string(), series_key);
            for (index, plan) in plans {
                frontiers.insert(smoothing_state_key(index), plan);
            }
            r.cache_state = Some(proto::ChartCacheState {
                frontiers,
                held_series: Some(series_key as u32),
            });
            // audit forced on (build_response ..., true, ...): the audit is where the misalignment struck.
            let out = build_response(&r, &rows, None, true).unwrap();
            assert!(!out.delta, "{runs:?}: a forged undercount must answer full");
            assert!(
                !out.audit_failed,
                "{runs:?}: the identity gate fires before the audit, so it is not an audit failure"
            );
        }
    }

    /// Randomized grow-and-splice sweep with the audit oracle forced on: whatever the planner claims for arbitrary mixes of axis kind, log ladders, smoothing, markers, and run growth, the audited delta must splice back to the full truth bit-for-bit (the splice mirror also verifies the shipped result hashes). Catches planner unsoundness the hand-written shapes miss.
    #[test]
    fn randomized_growth_splices_bitexactly() {
        let mut rng = Rng(0x9E3779B97F4A7C15);
        let algorithms = [
            proto::smoothing_config::Algorithm::Ema as i32,
            proto::smoothing_config::Algorithm::Triangular as i32,
            proto::smoothing_config::Algorithm::SavitzkyGolay as i32,
        ];
        let mut covered = [[[false; 2]; 2]; 3];
        let mut deltas = 0;
        let mut marker_deltas = 0;
        for case in 0..90u32 {
            let smoothed = case % 3 != 0;
            let use_time = (case / 3) % 2 == 1;
            let log = (case / 6) % 2 == 1;
            let target = [120u32, 400][(case as usize) % 2];
            let nruns = 1 + (rng.next() % 3) as usize;
            // Zero-less cases exercise the plain-log arm of the conditional +1 shift under the delta oracle.
            let start = if case % 4 == 3 { 5 } else { 0 };
            // Negative x: step cases lead with two negative-step sentinels (always held: a lower-step backfill fails the lineage proof); time cases use absolute timestamps, some negative, logged throughout or (every fifth case) only by growth. On log axes they become kind-4 markers. Deltas here carry settled ones: the appearing ones anchor on column 0 and answer in full, so marker_delta_tests covers appearance deltas.
            let negative = case % 5 >= 3;
            let appearing = case % 5 == 4;
            let mk = move |n: i64, seed: u64, neg_from: i64| -> Arc<Vec<VersionedRawPoint>> {
                Arc::new(
                    (0..n)
                        .map(|s| {
                            let r = (s as u64).wrapping_mul(seed | 1).wrapping_add(seed >> 3);
                            let value = match r % 60 {
                                0 => f32::NAN,
                                1 => f32::INFINITY,
                                _ => ((r % 90_000) as f32) / 11.0 - 2000.0,
                            };
                            let negative_x = negative && s >= neg_from && r % 41 == 7;
                            // Staggered run starts put carried marker anchors inside the chart, not only on its first column.
                            let at = s + start + if negative { (seed % 4) as i64 * 300 } else { 0 };
                            VersionedRawPoint {
                                tag: String::new(),
                                step: if negative && !use_time && s < 2 {
                                    s - 2
                                } else {
                                    at
                                },
                                // Deterministic jitter guarantees that every time-axis case exercises an irregular semantic plan.
                                timestamp_ms: if negative_x {
                                    -1 - s
                                } else {
                                    1_000_000 + at * 7 + ((s % 9 == 0) as i64)
                                },
                                value,
                                is_value: 1,
                                inserted_ms: s * 10_000_000,
                            }
                        })
                        .collect(),
                )
            };
            let seeds: Vec<u64> = (0..nruns).map(|_| rng.next()).collect();
            let lens: Vec<i64> = (0..nruns)
                .map(|_| 200 + (rng.next() % 2200) as i64)
                .collect();
            let neg_from = |n: i64| if appearing { n } else { 0 };
            let held_rows: Vec<Arc<Vec<VersionedRawPoint>>> = seeds
                .iter()
                .zip(&lens)
                .map(|(&sd, &n)| mk(n, sd, neg_from(n)))
                .collect();
            let grown_rows: Vec<Arc<Vec<VersionedRawPoint>>> = seeds
                .iter()
                .zip(&lens)
                .map(|(&sd, &n)| mk(n + 5 + (sd % 60) as i64, sd, neg_from(n)))
                .collect();
            let mut r = req(
                &(0..nruns).map(|i| ["a", "b", "c"][i]).collect::<Vec<_>>(),
                target,
            );
            r.use_timestamp_axis = use_time;
            r.relative_time = use_time && !negative;
            r.log_buckets = log;
            if smoothed {
                // Mixed-radix dimensions: every 36 cases cover smoothing on/off and step/time × linear/log axes; among smoothed cases every algorithm sees every axis pair.
                let algorithm_index = ((case / 12) % 3) as usize;
                covered[algorithm_index][use_time as usize][log as usize] = true;
                r.smoothing = Some(proto::SmoothingConfig {
                    algorithm: algorithms[algorithm_index],
                    window_size: 10,
                    time_constant: 100.0,
                    poly_order: 1,
                });
            }
            let held = build(&r, &held_rows);
            let r2 = continuation(&r, &held);
            let out = build(&r2, &grown_rows); // audit oracle ON inside build()
            let truth = inflate_full(&build(&r, &grown_rows));
            if out.delta {
                deltas += 1;
                if truth.series.iter().any(|s| s.nan_kinds.contains(&4)) {
                    marker_deltas += 1;
                }
                assert!(
                    eq(&splice(&inflate_full(&held), &out), &truth),
                    "case {case}: splice != truth (time={use_time} log={log} smoothed={smoothed})"
                );
            } else {
                assert!(
                    eq(&inflate_full(&out), &truth),
                    "case {case}: full != truth"
                );
            }
            assert!(
                !out.audit_failed,
                "case {case}: the audit caught the planner"
            );
        }
        assert!(
            covered.iter().flatten().flatten().all(|&seen| seen),
            "smoothing/axis coverage incomplete: {covered:?}"
        );
        assert!(
            deltas > 40,
            "only {deltas}/90 cases actually took the delta path"
        );
        assert!(
            marker_deltas >= 5,
            "only {marker_deltas} deltas carried a kind-4 marker"
        );
    }
}

pub struct QueryService {
    ch: Arc<ChClient>,
    pg: Arc<PgStore>,
    bumps: Arc<BumpCoalescer>,
    gates: LifecycleGates,
    chart_admission: ChartAdmission,
    // Fire-and-forget channel to the watchdog notifier task. None when
    // KYMO_WATCHDOG_URL is unset — notifications become no-ops.
    notifier: Option<mpsc::Sender<proto::RunLifecycleEvent>>,
    // Push bus: lifecycle version bumps (run added / terminated / project
    // created) go out to connected dashboards from here.
    events: crate::events::EventSender,
    // Rolls the 1-in-67 sampled audit (see build_chart), which also covers the memo's and held state's answers. Prime, so the round-robin can't phase-lock with a dashboard's periodic panel count and audit the same panels every cycle while never touching others.
    delta_audit: std::sync::atomic::AtomicU64,
    // Stage 1 of docs/chart-request-latency.md: memoized `unchanged` replies. KYMO_CHART_MEMO=0 turns it off.
    chart_memo: Arc<memo::ChartMemo>,
    // Stage 3b: held chart state that append polls advance (KYMO_CHART_HELD_STATE, off by default).
    chart_held: Arc<held::HeldStore>,
}

impl QueryService {
    pub fn new(
        ch: Arc<ChClient>,
        pg: Arc<PgStore>,
        bumps: Arc<BumpCoalescer>,
        gates: LifecycleGates,
        notifier: Option<mpsc::Sender<proto::RunLifecycleEvent>>,
        events: crate::events::EventSender,
    ) -> Self {
        let chart_limit = crate::env::required_bounded_usize(
            "KYMO_CHART_INFLIGHT_SERIES",
            DEFAULT_CHART_INFLIGHT_SERIES,
            1,
            MAX_CHART_INFLIGHT_SERIES,
        )
        .expect("invalid kymo chart-admission environment");
        let chart_budget_mb = crate::env::required_bounded_usize(
            "KYMO_CHART_BYTE_BUDGET_MB",
            DEFAULT_CHART_BYTE_BUDGET_MB,
            1,
            (u64::MAX >> 20) as usize,
        )
        .expect("invalid kymo chart-admission environment");
        let chart_build_slots = crate::env::required_bounded_usize(
            "KYMO_CHART_BUILD_SLOTS",
            DEFAULT_CHART_BUILD_SLOTS,
            1,
            MAX_CHART_BUILD_SLOTS,
        )
        .expect("invalid kymo chart-admission environment");
        tracing::info!(
            chart_limit,
            chart_budget_mb,
            chart_build_slots,
            "chart read admission configured"
        );
        let chart_memo = crate::env::required_bool("KYMO_CHART_MEMO", true)
            .expect("invalid kymo chart-memo environment");
        tracing::info!(chart_memo, "unchanged-chart memo configured");
        let held_raw = crate::env::required_string_or("KYMO_CHART_HELD_STATE", "off")
            .expect("invalid kymo held-chart environment");
        let held_mode = held::HeldMode::parse(&held_raw).unwrap_or_else(|| {
            panic!("KYMO_CHART_HELD_STATE must be off, shadow or on; got {held_raw:?}")
        });
        let held_budget = crate::env::required_mebibytes(
            "KYMO_CHART_HELD_STATE_MB",
            held::DEFAULT_HELD_BUDGET_MB,
        )
        .expect("invalid kymo held-chart environment");
        tracing::info!(?held_mode, held_budget, "held chart state configured");
        Self {
            ch,
            pg,
            bumps,
            gates,
            chart_admission: ChartAdmission::new(
                chart_limit,
                (chart_budget_mb as u64) << 20,
                chart_build_slots,
            ),
            notifier,
            events,
            delta_audit: std::sync::atomic::AtomicU64::new(0),
            chart_memo: Arc::new(memo::ChartMemo::new(chart_memo)),
            chart_held: Arc::new(held::HeldStore::new(held_mode, held_budget)),
        }
    }

    fn notify(&self, ev: proto::RunLifecycleEvent) {
        if let Some(tx) = &self.notifier {
            crate::notifier::try_send(tx, ev);
        }
    }

    fn force_resync(&self) {
        let _ = self.events.send(crate::events::VersionEvent {
            resync: true,
            ..Default::default()
        });
    }

    /// Take each run's read guard ([`LifecycleGates::read_many`]) and read its version before any of its data; versions are keyed by run_id. The versions are lower bounds on the reply's data and become its echoes (docs/first-mount-refresh.md): inserts commit, notify the series cache, then bump the version; cached rows serve only if fetched after the last notification (`series_cache.rs` `is_fresh`).
    async fn readable_guards(
        &self,
        keys: &[RunKey],
    ) -> Result<(ReadGuards, std::collections::HashMap<String, u64>), Status> {
        let guards = self.gates.read_many(keys.iter().cloned()).await;
        let versions = self
            .pg
            .ensure_runs_readable(keys)
            .await
            .map_err(lifecycle_access_status)?;
        Ok((guards, versions))
    }

    // --- Discovery ---

    #[instrument(skip(self))]
    pub async fn list_projects(
        &self,
        _request: Request<proto::ListProjectsRequest>,
    ) -> Result<Response<proto::ListProjectsResponse>, Status> {
        let listing = self
            .pg
            .list_metric_projects()
            .await
            .map_err(|e| Status::internal(format!("ListProjects failed: {e}")))?;
        let mut response = proto::ListProjectsResponse {
            server_now_ms: listing.server_now_ms,
            ..Default::default()
        };
        for (project_id, last_logged_at_ms) in listing.projects {
            if is_reserved_project_id(&project_id) {
                continue;
            }
            if let Some(at) = last_logged_at_ms {
                response.last_logged_at_ms.insert(project_id.clone(), at);
            }
            response.project_ids.push(project_id);
        }
        Ok(Response::new(response))
    }

    #[instrument(skip(self))]
    pub async fn list_runs(
        &self,
        request: Request<proto::ListRunsRequest>,
    ) -> Result<Response<proto::ListRunsResponse>, Status> {
        let req = request.into_inner();
        let snapshot = self
            .pg
            .list_runs(&req.project_id)
            .await
            .map_err(|e| Status::internal(format!("ListRuns failed: {e}")))?;

        let runs = timed_run_rows_to_proto(TimedRunRows {
            rows: snapshot.rows,
            server_now_ms: snapshot.server_now_ms,
        });
        Ok(Response::new(proto::ListRunsResponse {
            runs,
            project_version: snapshot.project_version,
        }))
    }

    /// Shared by InitRun and ImportRun: key and name validation, the run read
    /// gate, the Postgres upsert, and the version event. `import_created_at_ms`
    /// selects the import contract (backdated creation, no liveness reset).
    async fn init_run_inner(
        &self,
        project_id: &str,
        run_id: &str,
        run_name: &str,
        import_created_at_ms: Option<i64>,
    ) -> Result<crate::pg::InitRunOutcome, Status> {
        require_routeable_run_key(project_id, run_id)?;
        if is_reserved_project_id(project_id) {
            return Err(Status::invalid_argument(format!(
                "project_id '{}' is reserved",
                RESERVED_PROJECT_ID
            )));
        }
        let run_name = normalize_run_name(run_name)?;

        let key = RunKey::new(project_id.to_owned(), run_id.to_owned());
        let _guards = self.gates.read_many([key]).await;

        let outcome = self
            .pg
            .init_run(project_id, run_id, run_name, import_created_at_ms)
            .await
            .map_err(init_run_status)?;

        let _ = self.events.send(crate::events::VersionEvent {
            runs: outcome
                .bumped_run
                .map(|v| vec![(outcome.row.run_id.clone(), v)])
                .unwrap_or_default(),
            projects: vec![(outcome.row.project_id.clone(), outcome.bumped_project)],
            global: outcome.bumped_global,
            ..Default::default()
        });
        Ok(outcome)
    }

    #[instrument(skip(self))]
    pub async fn init_run(
        &self,
        request: Request<proto::InitRunRequest>,
    ) -> Result<Response<proto::InitRunResponse>, Status> {
        let req = request.into_inner();
        let outcome = self
            .init_run_inner(&req.project_id, &req.run_id, &req.run_name, None)
            .await?;
        let row = outcome.row;

        // The watchdog treats Started as an idempotent upsert. Send it for
        // resumes too: terminal runs have already been removed from its active
        // set, so without this hint a short resumed run can finish before the
        // periodic reconciliation ever starts tracking it again.
        self.notify(proto::RunLifecycleEvent {
            kind: proto::run_lifecycle_event::Kind::Started as i32,
            project_id: row.project_id.clone(),
            run_id: row.run_id.clone(),
            run_name: row.run_name.clone(),
            exit_code: None,
            restore_baseline_status: proto::RunStatus::Unknown as i32,
        });

        Ok(Response::new(proto::InitRunResponse {
            run: Some(run_info_row_to_proto(row, outcome.server_now_ms)),
            writer_epoch: Some(outcome.writer_epoch),
        }))
    }

    #[instrument(skip(self))]
    pub async fn rename_run(
        &self,
        request: Request<proto::RenameRunRequest>,
    ) -> Result<Response<proto::RenameRunResponse>, Status> {
        let req = request.into_inner();
        require_storable_run_key(&req.project_id, &req.run_id)?;
        let run_name = normalize_run_name(&req.run_name)?;

        let key = RunKey::new(req.project_id.clone(), req.run_id.clone());
        let _guards = self.gates.read_many([key.clone()]).await;
        self.pg
            .ensure_runs_active(std::slice::from_ref(&key))
            .await
            .map_err(lifecycle_access_status)?;

        let outcome = self
            .pg
            .rename_run(&req.project_id, &req.run_id, run_name)
            .await
            .map_err(|error| {
                // COMMIT acknowledgement can fail after PostgreSQL made the
                // edit durable. Make connected clients reconcile instead of
                // relying on an event this handler can no longer construct.
                self.force_resync();
                Status::internal(format!("RenameRun failed: {error}"))
            })?
            .ok_or_else(|| Status::not_found("run was not found"))?;

        if let Some(version) = outcome.bumped_project {
            let _ = self.events.send(crate::events::VersionEvent {
                projects: vec![(req.project_id, version)],
                ..Default::default()
            });
        }

        Ok(Response::new(proto::RenameRunResponse {
            run: Some(run_info_row_to_proto(outcome.row, outcome.server_now_ms)),
        }))
    }

    #[instrument(skip(self))]
    pub async fn terminate_run(
        &self,
        request: Request<proto::TerminateRunRequest>,
    ) -> Result<Response<proto::TerminateRunResponse>, Status> {
        let req = request.into_inner();
        // Legacy rows can predate the storage bound; termination must still
        // reach them even though current InitRun no longer admits such keys.
        require_run_key(&req.project_id, &req.run_id)?;
        let key = RunKey::new(req.project_id.clone(), req.run_id.clone());
        let _guards = self.gates.read_many([key.clone()]).await;
        self.pg
            .ensure_runs_active(std::slice::from_ref(&key))
            .await
            .map_err(lifecycle_access_status)?;
        let pending_ingests = self
            .bumps
            .pending_last_ingested_at_ms(&req.project_id, std::slice::from_ref(&req.run_id));
        let bumped = self
            .pg
            .terminate_run(
                &req.project_id,
                &req.run_id,
                req.exit_code,
                pending_ingests.get(&req.run_id).copied(),
                None,
            )
            .await
            .map_err(|e| Status::internal(format!("TerminateRun failed: {e}")))?;

        if let Some(outcome) = &bumped {
            let _ = self.events.send(crate::events::VersionEvent {
                runs: vec![(req.run_id.clone(), outcome.bumped_run)],
                projects: vec![(req.project_id.clone(), outcome.bumped_project)],
                ..Default::default()
            });
        }

        self.notify(proto::RunLifecycleEvent {
            kind: proto::run_lifecycle_event::Kind::Terminated as i32,
            project_id: req.project_id,
            run_id: req.run_id,
            // Prefer the canonical value read atomically by TerminateRun over
            // watchdog's eventually refreshed cache. This closes the
            // RenameRun -> TerminateRun race for Slack labels.
            run_name: bumped.map(|outcome| outcome.run_name).unwrap_or_default(),
            exit_code: Some(req.exit_code),
            restore_baseline_status: proto::RunStatus::Unknown as i32,
        });

        Ok(Response::new(proto::TerminateRunResponse {}))
    }

    // --- Bulk import (gated in lib.rs by ImportService::require_enabled) ---

    /// ImportRun: InitRun's identity/lifecycle contract with a backdated
    /// creation time. Deliberately does NOT notify the watchdog — replayed
    /// historical runs are not live lifecycle events and must not page
    /// anybody or enter the active tracking set.
    #[instrument(skip(self))]
    pub async fn import_run(
        &self,
        request: Request<proto::ImportRunRequest>,
    ) -> Result<Response<proto::ImportRunResponse>, Status> {
        let req = request.into_inner();
        require_import_timestamp("created_at_ms", req.created_at_ms)?;
        let outcome = self
            .init_run_inner(
                &req.project_id,
                &req.run_id,
                &req.run_name,
                Some(req.created_at_ms),
            )
            .await?;
        Ok(Response::new(proto::ImportRunResponse {
            run: Some(run_info_row_to_proto(outcome.row, outcome.server_now_ms)),
        }))
    }

    /// FinalizeImportRun: backdated terminal state plus synchronous metric
    /// registration. Like ImportRun, never notifies the watchdog.
    #[instrument(skip(self))]
    pub async fn finalize_import_run(
        &self,
        request: Request<proto::FinalizeImportRunRequest>,
    ) -> Result<Response<proto::FinalizeImportRunResponse>, Status> {
        let req = request.into_inner();
        require_run_key(&req.project_id, &req.run_id)?;
        require_import_timestamp("terminated_at_ms", req.terminated_at_ms)?;

        // EXCLUSIVE run gate, unlike TerminateRun's shared one: it drains
        // in-flight chart fetches and import flushes, which is what makes the
        // cache eviction below terminal (rationale at the eviction).
        let key = RunKey::new(req.project_id.clone(), req.run_id.clone());
        let _guards = self.gates.write_many([key.clone()]).await;
        self.pg
            .ensure_runs_active(std::slice::from_ref(&key))
            .await
            .map_err(lifecycle_access_status)?;

        // The run's registry derives from the ClickHouse outbox the import's
        // own inserts populated — read under the write gate, so every committed
        // cut is represented and the registered names cannot disagree with the
        // stored data; rows a reboot already drained are in Postgres from that
        // reconcile. Same `drain` as the boot reconcile: one rule for what is
        // registrable, one batching. Registry first: if it fails the run stays
        // unfinalized and the importer's retry re-runs both halves (upserts and
        // the terminal UPDATE both converge on re-execution).
        // The cursor is lazy, so ClickHouse failures surface inside `drain`:
        // classify them UNAVAILABLE (retryable transport) and everything else
        // (Postgres, corrupt rows) INTERNAL.
        let cursor = self
            .ch
            .run_metric_registry_outbox(&req.project_id, &req.run_id)
            .map_err(|e| Status::unavailable(format!("metric registry read failed: {e}")))?;
        let pg = &self.pg;
        let stats = crate::registry_reconcile::drain(cursor, |batch| async move {
            pg.register_run_metrics(&batch).await?;
            Ok(())
        })
        .await
        .map_err(|e| {
            if e.downcast_ref::<clickhouse::error::Error>().is_some() {
                Status::unavailable(format!("metric registry read failed: {e:#}"))
            } else {
                Status::internal(format!("metric registration failed: {e:#}"))
            }
        })?;
        if stats.skipped_rows > 0 {
            tracing::warn!(
                skipped_rows = stats.skipped_rows,
                samples = %stats.skipped_samples.join("; "),
                "FinalizeImportRun excluded unregistrable metric identities"
            );
        }

        // Evict the run's cache entries BEFORE the terminal commit exposes the new version: entries fetched mid-import may be missing rows buried below their watermark and only a full rebuild recovers them. The write gate makes this terminal — every store path holds the run's read gate across fetch AND store (readable_guards in query_chart_admitted, RefreshDetach in read_and_build_pass), so nothing can re-install a pre-eviction snapshot; same gate-claim-then-purge shape as the deletion reaper.
        self.ch
            .purge_run_caches(&[(req.project_id.as_str(), req.run_id.as_str())]);

        // `last_ingested_at` keeps its server-clock meaning (the import inserted
        // rows just now); the terminal time is the archive's.
        let outcome = self
            .pg
            .terminate_run(
                &req.project_id,
                &req.run_id,
                req.exit_code,
                Some(now_ms()),
                Some(req.terminated_at_ms),
            )
            .await
            .map_err(|e| Status::internal(format!("FinalizeImportRun failed: {e}")))?
            .ok_or_else(|| Status::not_found("run was not found"))?;

        let _ = self.events.send(crate::events::VersionEvent {
            runs: vec![(req.run_id.clone(), outcome.bumped_run)],
            projects: vec![(req.project_id.clone(), outcome.bumped_project)],
            // Unconditional: a spurious re-list on an empty run is cheaper
            // than the conditional's special cases (retried upserts RETURNING
            // nothing; a re-finalize after a reboot drained the outbox).
            metrics_changed_runs: vec![req.run_id.clone()],
            ..Default::default()
        });

        Ok(Response::new(proto::FinalizeImportRunResponse {}))
    }

    // --- Reversible run deletion ---

    #[instrument(skip_all)]
    pub async fn trash_runs(
        &self,
        request: Request<proto::TrashRunsRequest>,
    ) -> Result<Response<proto::TrashRunsResponse>, Status> {
        const INTERNAL_CHUNK_SIZE: usize = 256;

        let req = request.into_inner();
        if req.project_id.is_empty() {
            return Err(Status::invalid_argument("project_id is required"));
        }
        require_run_id_count("run_ids", req.run_ids.len())?;
        if !storable_ident(&req.project_id, MAX_ID_BYTES)
            || req
                .run_ids
                .iter()
                .any(|run_id| run_id.is_empty() || !storable_ident(run_id, MAX_ID_BYTES))
        {
            return Err(Status::invalid_argument(format!(
                "run IDs must be nonempty and project_id/run_id must be at most {MAX_ID_BYTES} bytes with no NUL bytes"
            )));
        }
        let operation_deadline = tokio::time::Instant::now() + LIFECYCLE_OPERATION_TIMEOUT;

        let mut results = Vec::with_capacity(req.run_ids.len());
        for (chunk_index, chunk) in req.run_ids.chunks(INTERNAL_CHUNK_SIZE).enumerate() {
            let remaining =
                operation_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                let first_unattempted = chunk_index * INTERNAL_CHUNK_SIZE;
                extend_trash_errors(
                    &mut results,
                    &req.run_ids[first_unattempted..],
                    "deletion deadline reached; run was not attempted",
                );
                break;
            }
            let keys = chunk
                .iter()
                .map(|run_id| RunKey::new(req.project_id.clone(), run_id.clone()));
            let _guards = match tokio::time::timeout(
                LIFECYCLE_GATE_TIMEOUT.min(remaining),
                self.gates.write_many(keys),
            )
            .await
            {
                Ok(guards) => guards,
                Err(_) => {
                    extend_trash_errors(
                        &mut results,
                        chunk,
                        "run is busy; deletion was not attempted",
                    );
                    continue;
                }
            };
            let pending_ingests = self
                .bumps
                .pending_last_ingested_at_ms(&req.project_id, chunk);
            // Once a database mutation starts, keep both the per-run gates and
            // the outer lifecycle barrier until PostgreSQL reports its final
            // outcome. Cancelling a COMMIT future on a timeout could release
            // those guards while the commit was still completing.
            match self
                .pg
                .trash_runs_chunk(&req.project_id, chunk, &pending_ingests)
                .await
            {
                Ok(outcome) => {
                    if outcome.bumped_project.is_some() || outcome.bumped_global.is_some() {
                        let _ = self.events.send(crate::events::VersionEvent {
                            projects: outcome
                                .bumped_project
                                .map(|version| vec![(req.project_id.clone(), version)])
                                .unwrap_or_default(),
                            global: outcome.bumped_global,
                            ..Default::default()
                        });
                    }
                    // The watchdog reconciles Trash as absence from its authoritative ListRuns poll. Per-run hints here would let one bulk request (up to MAX_REQUEST_RUN_IDS runs) crowd Started/Terminated/Restored events out of the shared bounded notifier queue.
                    for result in outcome.results {
                        let outcome = match result.kind {
                            TrashMutationKind::Trashed => proto::TrashRunOutcome::Trashed,
                            TrashMutationKind::AlreadyTrashed => {
                                proto::TrashRunOutcome::AlreadyTrashed
                            }
                            TrashMutationKind::NotFound => proto::TrashRunOutcome::NotFound,
                            TrashMutationKind::Expired => proto::TrashRunOutcome::Expired,
                        };
                        results.push(proto::TrashRunResult {
                            run_id: result.run_id,
                            outcome: outcome as i32,
                            error: String::new(),
                        });
                    }
                }
                Err(error) => {
                    tracing::error!(
                        project_id = %req.project_id,
                        positions = chunk.len(),
                        "TrashRuns internal chunk failed: {error}"
                    );
                    // A failed commit acknowledgement can hide a committed
                    // lifecycle change. Force all connected dashboards to
                    // poll rather than relying on a version event this path
                    // may no longer know precisely.
                    self.force_resync();
                    extend_trash_errors(&mut results, chunk, "internal deletion error");
                }
            }
        }

        Ok(Response::new(proto::TrashRunsResponse { results }))
    }

    #[instrument(skip(self))]
    pub async fn restore_run(
        &self,
        request: Request<proto::RestoreRunRequest>,
    ) -> Result<Response<proto::RestoreRunResponse>, Status> {
        let req = request.into_inner();
        require_storable_run_key(&req.project_id, &req.run_id)?;
        let key = RunKey::new(req.project_id.clone(), req.run_id.clone());
        let _guards = match tokio::time::timeout(
            LIFECYCLE_GATE_TIMEOUT,
            self.gates.write_many([key]),
        )
        .await
        {
            Ok(guards) => guards,
            Err(_) => {
                return Ok(Response::new(proto::RestoreRunResponse {
                    outcome: proto::RestoreRunOutcome::Error as i32,
                    error: "run is busy; restore was not attempted".to_string(),
                }));
            }
        };

        // As with trashing, an admitted restore is intentionally not timed
        // out: the mutation and run guards must outlive COMMIT/ROLLBACK.
        let outcome = match self.pg.restore_run(&req.project_id, &req.run_id).await {
            Ok(outcome) => outcome,
            Err(error) => {
                tracing::error!(
                    project_id = %req.project_id,
                    run_id = %req.run_id,
                    "RestoreRun failed: {error}"
                );
                self.force_resync();
                return Ok(Response::new(proto::RestoreRunResponse {
                    outcome: proto::RestoreRunOutcome::Error as i32,
                    error: "internal restore error".to_string(),
                }));
            }
        };

        let metric_discovery_runs = restore_metric_discovery_runs(outcome.kind, &req.run_id);
        if outcome.bumped_project.is_some()
            || outcome.bumped_global.is_some()
            || !metric_discovery_runs.is_empty()
        {
            let _ = self.events.send(crate::events::VersionEvent {
                runs: outcome
                    .bumped_run
                    .map(|version| vec![(req.run_id.clone(), version)])
                    .unwrap_or_default(),
                projects: outcome
                    .bumped_project
                    .map(|version| vec![(req.project_id.clone(), version)])
                    .unwrap_or_default(),
                global: outcome.bumped_global,
                // Registry contents do not change on Restore, but a saved
                // cross-project Specific panel may have settled terminal
                // before a leaf mounted. Re-list its metric type immediately;
                // ALREADY_ACTIVE also heals a lost earlier Restore hint.
                metrics_changed_runs: metric_discovery_runs,
                ..Default::default()
            });
        }
        if outcome.kind == RestoreMutationKind::Restored {
            if let Some(row) = &outcome.row {
                self.notify(proto::RunLifecycleEvent {
                    kind: proto::run_lifecycle_event::Kind::Restored as i32,
                    project_id: row.project_id.clone(),
                    run_id: row.run_id.clone(),
                    run_name: row.run_name.clone(),
                    exit_code: row.exit_code,
                    restore_baseline_status: compute_status(
                        outcome.server_now_ms,
                        row.created_at_ms,
                        row.last_main_metric_at_ms,
                        row.last_system_metric_at_ms,
                        row.exit_code,
                    ) as i32,
                });
            }
        }

        let kind = match outcome.kind {
            RestoreMutationKind::Restored => proto::RestoreRunOutcome::Restored,
            RestoreMutationKind::AlreadyActive => proto::RestoreRunOutcome::AlreadyActive,
            RestoreMutationKind::NotFound => proto::RestoreRunOutcome::NotFound,
            RestoreMutationKind::Expired => proto::RestoreRunOutcome::Expired,
        };
        Ok(Response::new(proto::RestoreRunResponse {
            outcome: kind as i32,
            error: String::new(),
        }))
    }

    #[instrument(skip_all)]
    pub async fn list_trash(
        &self,
        request: Request<proto::ListTrashRequest>,
    ) -> Result<Response<proto::ListTrashResponse>, Status> {
        let req = request.into_inner();
        let filtered = list_trash_is_filtered(&req);
        if filtered
            && (req.project_id.is_empty()
                || req.run_ids.is_empty()
                || req.run_ids.len() > LIST_TRASH_MAX_FILTER_IDENTITIES)
        {
            return Err(Status::invalid_argument(format!(
                "filtered ListTrash requires project_id and 1..={LIST_TRASH_MAX_FILTER_IDENTITIES} run_ids"
            )));
        }
        if filtered
            && (!storable_ident(&req.project_id, MAX_ID_BYTES)
                || req
                    .run_ids
                    .iter()
                    .any(|run_id| run_id.is_empty() || !storable_ident(run_id, MAX_ID_BYTES)))
        {
            return Err(Status::invalid_argument("invalid ListTrash identity"));
        }

        let query = if filtered {
            TrashListQuery::Identities {
                project_id: req.project_id,
                run_ids: req.run_ids,
            }
        } else {
            let after = req
                .after
                .map(|cursor| {
                    let ordinal = i64::try_from(cursor.ordinal)
                        .map_err(|_| Status::invalid_argument("invalid Trash cursor ordinal"))?;
                    if cursor.project_id.is_empty()
                        || cursor.run_id.is_empty()
                        || !storable_ident(&cursor.project_id, MAX_ID_BYTES)
                        || !storable_ident(&cursor.run_id, MAX_ID_BYTES)
                    {
                        return Err(Status::invalid_argument("invalid Trash cursor identity"));
                    }
                    Ok(TrashPageCursor {
                        purge_at_ms: cursor.purge_at_ms,
                        project_id: cursor.project_id,
                        ordinal,
                        run_id: cursor.run_id,
                    })
                })
                .transpose()?;
            TrashListQuery::Page {
                page_size: if req.page_size == 0 {
                    LIST_TRASH_DEFAULT_PAGE_SIZE
                } else {
                    (req.page_size as usize).min(LIST_TRASH_MAX_PAGE_SIZE)
                },
                after,
            }
        };
        let page = tokio::time::timeout(LIFECYCLE_DB_TIMEOUT, self.pg.list_trash(query))
            .await
            .map_err(|_| Status::unavailable("ListTrash timed out"))?
            .map_err(|error| Status::internal(format!("ListTrash failed: {error}")))?;
        Ok(Response::new(proto::ListTrashResponse {
            runs: page
                .rows
                .into_iter()
                .map(|row| run_record_row_to_proto(row, page.snapshot.server_now_ms))
                .collect(),
            global_version: page.snapshot.global_version,
            server_now_ms: page.snapshot.server_now_ms,
            total_count: page.total_count.unwrap_or_default(),
            next: page.next.map(|cursor| proto::TrashCursor {
                purge_at_ms: cursor.purge_at_ms,
                project_id: cursor.project_id,
                ordinal: cursor.ordinal as u64,
                run_id: cursor.run_id,
            }),
        }))
    }

    #[instrument(skip(self))]
    pub async fn get_run(
        &self,
        request: Request<proto::GetRunRequest>,
    ) -> Result<Response<proto::GetRunResponse>, Status> {
        let req = request.into_inner();
        require_storable_run_key(&req.project_id, &req.run_id)?;
        let Some((row, snapshot)) = tokio::time::timeout(
            LIFECYCLE_DB_TIMEOUT,
            self.pg.get_run(&req.project_id, &req.run_id),
        )
        .await
        .map_err(|_| Status::unavailable("GetRun timed out"))?
        .map_err(|error| Status::internal(format!("GetRun failed: {error}")))?
        else {
            return Err(Status::not_found(format!(
                "run {}/{} was not found",
                req.project_id, req.run_id
            )));
        };
        Ok(Response::new(proto::GetRunResponse {
            run: Some(run_record_row_to_proto(row, snapshot.server_now_ms)),
            global_version: snapshot.global_version,
            server_now_ms: snapshot.server_now_ms,
        }))
    }

    #[instrument(skip(self))]
    pub async fn list_metrics(
        &self,
        request: Request<proto::ListMetricsRequest>,
    ) -> Result<Response<proto::ListMetricsResponse>, Status> {
        let req = request.into_inner();
        let keys = vec![RunKey::new(req.project_id.clone(), req.run_id.clone())];
        let (_guards, _versions) = self.readable_guards(&keys).await?;
        // Served from the Postgres run_metrics registry (maintained at
        // ingest, seeded once from ClickHouse).
        let rows = self
            .pg
            .list_run_metrics(&req.project_id, &req.run_id)
            .await
            .map_err(|e| Status::internal(format!("ListMetrics failed: {e}")))?;

        Ok(Response::new(proto::ListMetricsResponse {
            metrics: rows.into_iter().map(metric_info_from_row).collect(),
        }))
    }

    #[instrument(skip_all)]
    pub async fn list_run_set_metrics(
        &self,
        request: Request<proto::ListRunSetMetricsRequest>,
    ) -> Result<Response<proto::ListMetricsResponse>, Status> {
        let req = request.into_inner();
        require_run_id_count("run_ids", req.run_ids.len())?;
        if req.run_ids.is_empty() {
            return Ok(Response::new(proto::ListMetricsResponse {
                metrics: Vec::new(),
            }));
        }
        let keys: Vec<_> = req
            .run_ids
            .iter()
            .map(|run_id| RunKey::new(req.project_id.clone(), run_id.clone()))
            .collect();
        let (_guards, _versions) = self.readable_guards(&keys).await?;
        let rows = self
            .pg
            .list_run_set_metrics(&req.project_id, &req.run_ids)
            .await
            .map_err(|e| Status::internal(format!("ListRunSetMetrics failed: {e}")))?;

        Ok(Response::new(proto::ListMetricsResponse {
            metrics: rows.into_iter().map(metric_info_from_row).collect(),
        }))
    }

    // --- Chart queries ---

    #[instrument(skip_all)]
    pub async fn query_chart(
        &self,
        request: Request<proto::ChartRequest>,
    ) -> Result<Response<proto::ChartResponse>, Status> {
        let mut req = request.into_inner();
        canonicalize_chart_request(&mut req)?;
        let requested_weight = chart_admission_weight(&req);
        ChartAdmission::check_width(requested_weight)?;
        if req.y_series.is_empty() {
            return Ok(Response::new(proto::ChartResponse::default()));
        }
        let mut plan = ChartPlan::new(&req, &self.chart_memo, &self.chart_held, || {
            self.draw_audit()
        });
        let estimate = plan.estimate(&self.ch.series_cache().sizes(&plan.series));
        let req = Arc::new(req);
        let response = self
            .chart_admission
            .run_with_timeout(
                requested_weight,
                estimate,
                CHART_QUERY_TIMEOUT,
                |charge, permit| {
                    self.query_chart_admitted(&req, &mut plan, requested_weight, charge, permit)
                },
            )
            .await?;
        Ok(Response::new(response))
    }

    /// 1 in 67 of the chart polls that can audit (the only ones that draw, [`ChartPlan::new`]) audits its answer: a delta against a brute-force reconstruction of the held response (build_chart), and a memo's or held state's answer against a full build ([`ChartPlan::audit`]).
    fn draw_audit(&self) -> bool {
        self.delta_audit
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .is_multiple_of(67)
    }

    /// Everything after admission: the run guards and Postgres's lifecycle check, then [`Self::read_and_build_pass`]. A poll that starts over ([`ChartPlan::restart`]) is admitted again, under the same deadline, and takes its run guards and Postgres's lifecycle check again before its next reads: the run versions read before its first pass's data still bound its reply's. Nothing that answers (the memo, held state or a build) stores run versions: these stamp it.
    async fn query_chart_admitted(
        &self,
        req: &Arc<proto::ChartRequest>,
        plan: &mut ChartPlan,
        requested_weight: usize,
        charge: Arc<budget::ChartCharge>,
        mut permit: ChartAdmissionPermit,
    ) -> Result<proto::ChartResponse, Status> {
        let runs: Vec<_> = req
            .y_series
            .iter()
            .map(|s| RunKey::new(&s.project_id, &s.run_id))
            .collect();
        let (mut guards, run_versions) = self.readable_guards(&runs).await?;
        loop {
            match self
                .read_and_build_pass(req, plan, &charge, permit, guards)
                .await?
            {
                ControlFlow::Break(response) => {
                    return Ok(proto::ChartResponse {
                        run_versions,
                        ..response
                    })
                }
                ControlFlow::Continue((attempt, known)) => {
                    plan.restart(attempt, known);
                    // Its charge becomes step 1's estimate with its transients, waited for holding nothing ([`budget::ChartCharge::recharge`]), then its scan units. Priced in its own statement: the sizes pin snapshots, which must go before the wait.
                    let estimate = plan.estimate(&self.ch.series_cache().sizes(&plan.series));
                    charge.recharge(estimate).await;
                    permit = self.chart_admission.acquire(requested_weight).await;
                    guards = self.readable_guards(&runs).await?.0;
                }
            }
        }
    }

    /// One pass of lookups, reads, stage 1, and the build. Releases the scan units as reads finish and the run guards before waiting for a build slot. `Continue` with what its next build attempts and the charge this pass came to in full when the poll starts over ([`ChartPlan::restart`]), holding nothing once this returns.
    async fn read_and_build_pass(
        &self,
        req: &Arc<proto::ChartRequest>,
        plan: &ChartPlan,
        charge: &Arc<budget::ChartCharge>,
        mut permit: ChartAdmissionPermit,
        guards: ReadGuards,
    ) -> Result<ControlFlow<proto::ChartResponse, (held::Advance, Vec<u64>)>, Status> {
        use futures::stream::{self, StreamExt, TryStreamExt};
        use futures::FutureExt;

        // Once a series' rows are in hand, its charge is their real size; a correction never waits. With transients, it counts what the request keeps of the series, over which a full build's output is bounded ([`ChartPlan::output_bound`]).
        let kept_in_hand = std::sync::Mutex::new(Kept::NONE);
        let correct = |key: &SeriesKey, rows: &SeriesSnapshot| {
            let index = plan.index_of(key);
            let (bytes, kept) = plan.pinned(index, rows, plan.transients);
            let mut in_hand = kept_in_hand.lock().unwrap();
            *in_hand = in_hand.join(kept);
            charge.correct(index, bytes);
        };
        // Fresh hits are judged once, on the snapshots pinned here: each is served as pinned, charged at its real size, and needs no read.
        let mut pinned: std::collections::HashMap<&SeriesKey, Arc<SeriesSnapshot>> =
            std::collections::HashMap::new();
        for key in &plan.series {
            if let Some(rows) = self.ch.series_cache().lookup_fresh(key) {
                correct(key, &rows);
                pinned.insert(key, rows);
            }
        }
        // Their units go back to the gate; each remaining read keeps one.
        permit.keep_for_reads(plan.series.len() - pinned.len() + plan.x_runs);

        let fetch_concurrency = self.chart_admission.fetch_concurrency();
        // Split lazily at election time: a unit leaves the request only when a detached refresh actually spawns, so protection lands on the series that scan. The refresh also holds its series' snapshot bytes, split off the request's charge, until it ends.
        let detach_for = |key: &SeriesKey| {
            permit.split_refresh_unit().map(|unit| {
                let index = plan.index_of(key);
                RefreshDetach {
                    _permit: unit,
                    _run_guard: Arc::clone(&guards[&RunKey::new(&key.project_id, &key.run_id)]),
                    _charge: Box::new(charge.split(index, plan.snapshots[index])),
                }
            })
        };

        // Distinct series grouped by (project, metric); a group's cache misses share one full read (clickhouse.rs fetch_full_many).
        // Election and unit splits run in this synchronous pass, before any per-series fetch below can split units or take slots; each batch is backed by one unit per series and spawns its detached tasks here.
        // A group of one distinct series keeps its own read (a larger group still elects a one-series batch when its other series are cached or busy), and batches take at most half of the request's `fetch_concurrency` reads, leaving the rest to the per-series stream below.
        let mut groups = series_groups(plan.series.iter().filter(|key| !pinned.contains_key(*key)));
        let mut batches = Vec::new();
        for group in &mut groups {
            if batches.len() >= fetch_concurrency / 2 {
                break;
            }
            if group.len() < 2 {
                continue;
            }
            let elected = self
                .ch
                .elect_full_reads(group, permit.spare_refresh_units());
            if elected.is_empty() {
                continue;
            }
            group.retain(|key| !elected.iter().any(|miss| &miss.key == key));
            let ctx = elected
                .iter()
                .map(|miss| detach_for(&miss.key).expect("counted as spare"))
                .collect();
            batches.push(self.ch.spawn_full_read_batch(elected, ctx).map(|batch| {
                if let Ok(rows) = &batch {
                    for (key, rows) in rows {
                        correct(key, rows);
                    }
                }
                batch
            }));
        }
        let per_series_concurrency = fetch_concurrency - batches.len();
        // The first failed batch fails the request at once, as a failed per-series read does; the other batches' tasks finish detached.
        let batched_rows = futures::future::try_join_all(batches);

        // Every other series: its own cached read (attached, incremental, or a full read of its own), concurrently, sharing the request's read bound with the batches.
        let per_series_keys = groups.iter().flatten();
        let fetches: Vec<_> = per_series_keys
            .clone()
            .map(|key| {
                self.ch
                    .query_raw_any_cached(
                        &key.project_id,
                        &key.run_id,
                        &key.metric_name,
                        move || detach_for(key),
                    )
                    .map(move |outcome| {
                        if let Ok(rows) = &outcome {
                            correct(key, rows);
                        }
                        outcome
                    })
            })
            .collect();
        let per_series = stream::iter(fetches)
            .buffered(per_series_concurrency)
            .try_collect::<Vec<_>>();
        let (batched_rows, per_series) =
            futures::try_join!(batched_rows, per_series).map_err(chart_fetch_status)?;
        let rows: std::collections::HashMap<_, _> = batched_rows
            .iter()
            .flatten()
            .map(|(key, rows)| (key, rows))
            .chain(per_series_keys.zip(&per_series))
            .chain(pinned.iter().map(|(&key, rows)| (key, rows)))
            .collect();
        let all_rows: Arc<[Arc<SeriesSnapshot>]> = req
            .y_series
            .iter()
            .map(|s| Arc::clone(rows[&SeriesKey::new(&s.project_id, &s.run_id, &s.metric_name)]))
            .collect();
        // The Y reads are done: keep only the custom-X phase's units.
        permit.keep_for_reads(plan.x_runs);

        // Custom x-axis: map each run's points through THAT run's x metric.
        // The request's x_series names the metric; its run_id is ignored
        // (plotting every run against the first run's x values pairs y
        // samples with x samples from a different run). Ignored in
        // timestamp mode, where point keys are timestamps, not steps.
        let x_maps = match custom_x_runs(req) {
            Some((x_ref, run_refs)) => {
                let step_min = req.step_min.unwrap_or(i64::MIN);
                let step_max = req.step_max.unwrap_or(i64::MAX);
                // One read per project covers all of its runs; each run is charged as its own item, after the Y series.
                let mut projects: Vec<(&str, Vec<String>, Vec<usize>)> = Vec::new();
                for (item, (pid, rid)) in run_refs.into_iter().enumerate() {
                    let item = plan.series.len() + item;
                    match projects.iter_mut().find(|(project, ..)| *project == pid) {
                        Some((_, runs, items)) => {
                            runs.push(rid.to_string());
                            items.push(item);
                        }
                        None => projects.push((pid, vec![rid.to_string()], vec![item])),
                    }
                }
                let x_fetches: Vec<_> = projects
                    .iter()
                    .map(|(pid, runs, items)| async move {
                        let series = self
                            .ch
                            .query_raw_many(pid, runs, &x_ref.metric_name, step_min, step_max)
                            .await?;
                        for (&item, points) in items.iter().zip(&series) {
                            charge.correct(item, points.len() as u64 * budget::X_BYTES_PER_ROW);
                        }
                        anyhow::Ok(series)
                    })
                    .collect();
                let fetched: Vec<_> = stream::iter(x_fetches)
                    .buffered(fetch_concurrency)
                    .try_collect()
                    .await
                    .map_err(|e| Status::internal(format!("x-axis query failed: {e}")))?;
                let mut maps = std::collections::HashMap::new();
                for ((_, runs, _), series) in projects.into_iter().zip(fetched) {
                    for (rid, rows) in runs.into_iter().zip(series) {
                        maps.insert(rid, rows.into_iter().collect());
                    }
                }
                Some(maps)
            }
            _ => None,
        };
        // Every read is done: return the remaining units, then the run guards, before waiting for a build slot.
        drop(permit);
        drop(guards);

        let (audit, audits_prefix) = (plan.audit, plan.audits_prefix);
        let held = plan.held_key.map(|key| (&*self.chart_held, key));
        let memo = match probe_memo(&self.chart_memo, plan.memo_key, &all_rows, audit, held) {
            ControlFlow::Break(reply) => return Ok(ControlFlow::Break(reply)),
            ControlFlow::Continue(memo) => memo,
        };
        // The output's charge item follows the series' and custom-X runs' ([`ChartPlan`]).
        let output = plan.series.len() + plan.x_runs;
        let (memo_key, held_key) = (plan.memo_key, plan.held_key);
        let (x_maps, memo) = (x_maps.map(Arc::new), memo.map(Arc::new));
        let build = |attempt| {
            let (charge, all_rows, x_maps, memo) = (
                charge.clone(),
                all_rows.clone(),
                x_maps.clone(),
                memo.clone(),
            );
            let (req, params, chart_memo, chart_held) = (
                Arc::clone(req),
                plan.params.clone(),
                Arc::clone(&self.chart_memo),
                Arc::clone(&self.chart_held),
            );
            self.chart_admission.builds.run(move || {
                // Its output is charged as soon as the model exists ([`budget::BuildSlots::run`] keeps the charge and snapshots until the build ends).
                let charge_output =
                    |shape| charge.correct(output, ChartPlan::output_bytes(shape, audits_prefix));
                let built = held::build(
                    &chart_held,
                    &req,
                    &params,
                    held_key,
                    &all_rows,
                    x_maps.as_deref(),
                    audit,
                    memo.as_deref(),
                    &charge_output,
                    attempt,
                )?;
                Ok::<_, Status>(built.map(|built| {
                    count_delta(built.kind);
                    settle_memo(&chart_memo, memo_key, &all_rows, &built);
                    built.response
                }))
            })
        };
        // An unaudited poll whose request has held state (`on`) tries the advance first, which step 1 reserved beside its working copies, charged once it has run; should it fall back, it builds in full in a slot taken again.
        let attempt = if plan.held_hint.is_some() {
            charge.settle();
            match build(held::Advance::Only).await?? {
                Some(response) => return Ok(ControlFlow::Break(response)),
                None => held::Advance::Skip,
            }
        } else {
            plan.attempt
        };
        // A full build's charge before its slot wait: each series with its transients, and the output at its bound ([`ChartPlan::output_bound`]), reserved before it is allocated. If step 1 charged transients, the corrections counted it; else (a memo or held state was stored) it is charged here ([`budget::ChartCharge::correct_all`]). Either way, on a first pass ([`ChartPlan::known`]) the poll keeps it within its grant, and past it only if it fits now, ahead of any waiter ([`budget::ChartCharge::overrun`]). Without room the poll starts over at the sizes it now knows: a storm of first reads, reads during a bulk write, a run's appends or a lineage rotation make many polls build at once, and keeping what each came to would pin them all past the budget while they wait for build slots.
        if plan.transients {
            let kept = kept_in_hand.into_inner().unwrap();
            charge.correct(output, plan.output_bound(kept));
        } else {
            // Every item of the charge: neither the memo nor held state serves custom x, so such a poll reads no x.
            let mut kept = Kept::NONE;
            let mut full: Vec<u64> = plan
                .series
                .iter()
                .enumerate()
                .map(|(index, key)| {
                    let (bytes, series) = plan.pinned(index, rows[key], true);
                    kept = kept.join(series);
                    bytes
                })
                .collect();
            full.push(plan.output_bound(kept));
            charge.correct_all(&full);
        }
        if plan.known.is_empty() {
            if let Some(known) = charge.overrun() {
                return Ok(ControlFlow::Continue((attempt, known)));
            }
        } else {
            charge.settle();
        }
        Ok(ControlFlow::Break(build(attempt).await??.expect(
            "only an Advance::Only call answers without building",
        )))
    }

    // --- CDN queries ---

    #[instrument(skip_all)]
    pub async fn query_cdn_keys(
        &self,
        request: Request<proto::QueryCdnKeysRequest>,
    ) -> Result<Response<proto::QueryCdnKeysResponse>, Status> {
        let req = request.into_inner();
        require_run_id_count("refs", req.refs.len())?;
        for series in &req.refs {
            let bytes = crate::clickhouse::cdn_key_cost(
                &series.project_id,
                &series.run_id,
                &series.metric_name,
            );
            if bytes > crate::clickhouse::MAX_CDN_KEY_REF_BYTES {
                return Err(Status::invalid_argument(format!(
                    "refs: a ref's names take {bytes} bytes of SQL, past the {} one statement can give a ref",
                    crate::clickhouse::MAX_CDN_KEY_REF_BYTES
                )));
            }
        }

        let step_min = req.step_min.unwrap_or(i64::MIN);
        let step_max = req.step_max.unwrap_or(i64::MAX);

        if req.refs.is_empty() {
            return Ok(Response::new(proto::QueryCdnKeysResponse::default()));
        }

        let keys: Vec<_> = req
            .refs
            .iter()
            .map(|series| RunKey::new(series.project_id.clone(), series.run_id.clone()))
            .collect();
        let (_guards, run_versions) = self.readable_guards(&keys).await?;

        let key_refs: Vec<(String, String, String)> = req
            .refs
            .into_iter()
            .map(|r| (r.project_id, r.run_id, r.metric_name))
            .collect();

        let rows = self
            .ch
            .query_cdn_keys_batch(&key_refs, step_min, step_max)
            .await
            .map_err(|e| Status::internal(format!("CDN query failed: {e}")))?;

        // One CdnSeries per requested ref, in request order, duplicates included; refs without rows get empty entries.
        let series = assemble_cdn_series(key_refs, rows);

        Ok(Response::new(proto::QueryCdnKeysResponse {
            series,
            run_versions,
        }))
    }

    // --- Text stream queries ---

    pub async fn query_text_window(
        &self,
        request: Request<proto::QueryTextWindowRequest>,
    ) -> Result<Response<proto::QueryTextWindowResponse>, Status> {
        let req = request.into_inner();
        require_storable_run_key(&req.project_id, &req.run_id)?;
        let metric_names = normalize_text_metric_names(req.metric_names)?;
        let search = validate_text_search(&req.search)?;
        let keys = vec![RunKey::new(req.project_id.clone(), req.run_id.clone())];
        let (_guards, run_versions) = self.readable_guards(&keys).await?;
        let line_limit = req.line_limit.clamp(1, MAX_TEXT_WINDOW_LINES);
        let window = self
            .ch
            .query_text_window(
                &req.project_id,
                &req.run_id,
                &metric_names,
                req.line_offset,
                line_limit,
                search,
            )
            .await
            .map_err(|error| {
                if error
                    .downcast_ref::<crate::clickhouse::TextWindowLimitError>()
                    .is_some()
                {
                    Status::resource_exhausted(error.to_string())
                } else {
                    Status::internal(format!("query_text_window failed: {error}"))
                }
            })?;
        let lines = window
            .lines
            .into_iter()
            .map(|row| proto::TextLine {
                step: row.step,
                metric_name: row.metric_name,
                line_index: row.line_index,
                text: row.text,
            })
            .collect();
        Ok(Response::new(proto::QueryTextWindowResponse {
            lines,
            total_lines: window.total_lines,
            first_step: window.first_step,
            run_versions,
        }))
    }

    // --- Change detection ---

    pub async fn poll_versions(
        &self,
        request: Request<proto::PollVersionsRequest>,
    ) -> Result<Response<proto::PollVersionsResponse>, Status> {
        let req = request.into_inner();
        require_run_id_count("run_ids", req.run_ids.len())?;
        let result = self
            .pg
            .poll_versions(req.project_id.as_deref(), &req.run_ids)
            .await
            .map_err(|e| Status::internal(format!("PollVersions failed: {e}")))?;
        Ok(Response::new(proto::PollVersionsResponse {
            global_version: result.global_version,
            project_version: result.project_version,
            run_versions: result.run_versions,
        }))
    }
}

#[cfg(test)]
mod request_list_bound_tests {
    use super::*;

    /// The real service with unreachable databases (nothing listens on port 1, and the test Postgres pool connects lazily): every refusal below comes before either is touched.
    fn service() -> QueryService {
        let (events, _) = tokio::sync::broadcast::channel(8);
        QueryService::new(
            Arc::new(ChClient::new("http://127.0.0.1:1").unwrap()),
            Arc::new(PgStore::test_store()),
            BumpCoalescer::empty_for_test(),
            LifecycleGates::new(),
            None,
            events,
        )
    }

    fn ids(count: usize) -> Vec<String> {
        (0..count).map(|i| i.to_string()).collect()
    }

    fn refs(count: usize) -> Vec<proto::SeriesRef> {
        ids(count)
            .into_iter()
            .map(|run_id| proto::SeriesRef {
                project_id: "p".to_string(),
                run_id,
                metric_name: "m".to_string(),
                tags: Vec::new(),
            })
            .collect()
    }

    fn assert_refused<T: std::fmt::Debug>(result: Result<T, Status>, field: &str) {
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
        assert_eq!(
            status.message(),
            format!("{field} lists 100001 entries; a request may list at most 100000")
        );
    }

    #[test]
    fn the_bound_admits_its_limit_and_refuses_one_more() {
        assert!(require_run_id_count("run_ids", 0).is_ok());
        assert!(require_run_id_count("run_ids", MAX_REQUEST_RUN_IDS).is_ok());
        assert_refused(
            require_run_id_count("run_ids", MAX_REQUEST_RUN_IDS + 1),
            "run_ids",
        );
    }

    #[tokio::test]
    async fn poll_versions_refuses_a_list_past_the_bound() {
        let request = proto::PollVersionsRequest {
            project_id: Some("p".to_string()),
            run_ids: ids(MAX_REQUEST_RUN_IDS + 1),
        };
        assert_refused(
            service().poll_versions(Request::new(request)).await,
            "run_ids",
        );
    }

    #[tokio::test]
    async fn list_run_set_metrics_refuses_a_list_past_the_bound() {
        let request = proto::ListRunSetMetricsRequest {
            project_id: "p".to_string(),
            run_ids: ids(MAX_REQUEST_RUN_IDS + 1),
        };
        assert_refused(
            service().list_run_set_metrics(Request::new(request)).await,
            "run_ids",
        );
    }

    #[tokio::test]
    async fn trash_runs_refuses_a_list_past_the_bound() {
        let request = proto::TrashRunsRequest {
            project_id: "p".to_string(),
            run_ids: ids(MAX_REQUEST_RUN_IDS + 1),
        };
        assert_refused(service().trash_runs(Request::new(request)).await, "run_ids");
    }

    #[tokio::test]
    async fn query_cdn_keys_refuses_refs_past_the_bound() {
        let request = proto::QueryCdnKeysRequest {
            refs: refs(MAX_REQUEST_RUN_IDS + 1),
            ..Default::default()
        };
        assert_refused(
            service().query_cdn_keys(Request::new(request)).await,
            "refs",
        );
    }

    /// A ref no statement could hold even alone is refused before either database, naming the bound.
    #[tokio::test]
    async fn query_cdn_keys_refuses_a_ref_no_statement_holds() {
        let mut refs = refs(2);
        refs[1].metric_name = "'".repeat(70_000);
        let request = proto::QueryCdnKeysRequest {
            refs,
            ..Default::default()
        };
        let status = service()
            .query_cdn_keys(Request::new(request))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
        assert_eq!(
            status.message(),
            "refs: a ref's names take 280026 bytes of SQL, past the 258048 one statement can give a ref"
        );
    }

    #[tokio::test]
    async fn query_text_window_refuses_metric_names_past_the_bound_before_their_dedup() {
        let request = proto::QueryTextWindowRequest {
            project_id: "p".to_string(),
            run_id: "r".to_string(),
            metric_names: vec!["m".to_string(); MAX_REQUEST_RUN_IDS + 1],
            ..Default::default()
        };
        assert_refused(
            service().query_text_window(Request::new(request)).await,
            "metric_names",
        );
    }

    #[tokio::test]
    async fn a_chart_past_the_ceiling_is_refused_on_its_y_refs_alone() {
        let request = proto::ChartRequest {
            y_series: refs(MAX_CHART_REQUEST_RAW_SERIES + 1),
            x_series: refs(1).pop(),
            ..Default::default()
        };
        assert_eq!(
            chart_admission_weight(&request),
            MAX_CHART_REQUEST_RAW_SERIES + 1
        );
        let status = service()
            .query_chart(Request::new(request))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
        assert_eq!(
            status.message(),
            "chart requires 129 raw-series reads; maximum per chart is 128"
        );
    }
}

#[cfg(test)]
mod cdn_series_tests {
    use super::*;

    fn reference(run_id: &str) -> (String, String, String) {
        ("project".into(), run_id.into(), "gallery".into())
    }

    fn row(run_id: &str, step: i64, cdn_key: &str) -> CdnKeyBatchRow {
        CdnKeyBatchRow {
            project_id: "project".to_string(),
            run_id: run_id.to_string(),
            metric_name: "gallery".to_string(),
            step,
            cdn_key: cdn_key.to_string(),
        }
    }

    #[test]
    fn duplicate_cdn_refs_each_receive_their_positional_series() {
        let output = assemble_cdn_series(
            vec![
                reference("a"),
                reference("missing"),
                reference("a"),
                reference("b"),
            ],
            vec![row("a", 1, "one.json"), row("b", 2, "two.json")],
        );

        assert_eq!(
            output
                .iter()
                .map(|item| item.run_id.as_str())
                .collect::<Vec<_>>(),
            ["a", "missing", "a", "b"]
        );
        assert_eq!(output[0].entries, output[2].entries);
        assert_eq!(output[0].entries[0].cdn_key, "one.json");
        assert!(output[1].entries.is_empty());
        assert_eq!(output[3].entries[0].cdn_key, "two.json");
    }
}

#[cfg(test)]
mod chart_admission_tests {
    use super::frontier_delta_tests::{build_chart, continuation};
    use super::*;
    use crate::series_cache::CachedSize;

    /// Polls started over in this test binary ([`ChartPlan::restart`]), which the randomized admission test tallies.
    pub(super) static RESTARTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn admission(limit: usize) -> ChartAdmission {
        ChartAdmission::new(limit, 1 << 30, DEFAULT_CHART_BUILD_SLOTS)
    }

    /// A recovery record of `rows` rows over `steps` in `groups` tag groups, its pin at a scalar snapshot's bytes ([`crate::series_cache::snapshot_bytes`]): what a tagged snapshot charged is more ([`an_evicted_tagged_series_pins_what_its_snapshot_charged`]).
    fn evicted(rows: usize, steps: (i64, i64), groups: usize) -> CachedSize {
        CachedSize::Evicted(EvictedSize {
            rows,
            bytes: crate::series_cache::snapshot_bytes(rows),
            steps,
            groups,
        })
    }

    fn series(run_id: &str) -> proto::SeriesRef {
        proto::SeriesRef {
            project_id: "project".to_string(),
            run_id: run_id.to_string(),
            metric_name: "loss".to_string(),
            tags: Vec::new(),
        }
    }

    /// A numeric row of `tag` at `step`, timestamped at its step.
    fn point(tag: &str, step: i64, value: f32, inserted_ms: i64) -> VersionedRawPoint {
        VersionedRawPoint {
            tag: tag.to_string(),
            step,
            timestamp_ms: step,
            value,
            is_value: 1,
            inserted_ms,
        }
    }

    #[test]
    fn series_groups_keep_first_appearance_order() {
        let key = |project, run, metric| SeriesKey::new(project, run, metric);
        assert_eq!(
            series_groups(&[
                key("p", "a", "loss"),
                key("p", "b", "acc"),
                key("q", "a", "loss"),
                key("p", "b", "loss"),
            ]),
            [
                vec![key("p", "a", "loss"), key("p", "b", "loss")],
                vec![key("p", "b", "acc")],
                vec![key("q", "a", "loss")],
            ]
        );
    }

    #[test]
    fn chart_admission_counts_y_and_distinct_custom_x_runs() {
        let mut request = proto::ChartRequest {
            y_series: vec![series("run-a"), series("run-a"), series("run-b")],
            x_series: Some(series("ignored-by-query")),
            ..Default::default()
        };
        assert_eq!(chart_admission_weight(&request), 5);

        request.use_timestamp_axis = true;
        assert_eq!(chart_admission_weight(&request), 3);
    }

    #[test]
    fn chart_fetch_concurrency_respects_small_admission_limits() {
        let small = admission(2);
        assert_eq!(small.fetch_concurrency(), 2);

        let large = admission(64);
        assert_eq!(large.fetch_concurrency(), MAX_CHART_FETCH_CONCURRENCY);
    }

    #[tokio::test]
    async fn narrow_request_splits_down_to_its_last_unit() {
        let admission = admission(32);
        let permit = admission.acquire(3).await;
        let units: Vec<_> = std::iter::from_fn(|| permit.split_refresh_unit())
            .take(8)
            .collect();
        assert_eq!(units.len(), 3);
    }

    #[tokio::test]
    async fn clamped_request_stops_splitting_at_the_inline_floor() {
        let limit = MAX_CHART_FETCH_CONCURRENCY * 2;
        let admission = admission(limit);
        let permit = admission.acquire(MAX_CHART_REQUEST_RAW_SERIES).await;
        let units: Vec<_> = std::iter::from_fn(|| permit.split_refresh_unit())
            .take(limit * 2)
            .collect();
        assert_eq!(units.len(), limit - admission.fetch_concurrency());
        drop(units);
        // The retained floor is still held: nothing else can be admitted on it.
        assert_eq!(
            admission.slots.available_permits(),
            limit - admission.fetch_concurrency()
        );
    }

    #[tokio::test]
    async fn degenerate_limit_clamped_request_never_splits() {
        let admission = admission(1);
        let permit = admission.acquire(2).await;
        assert!(permit.split_refresh_unit().is_none());
    }

    #[test]
    fn parked_task_deadline_renders_as_loading() {
        use crate::series_cache::RefreshError;
        assert_eq!(
            chart_fetch_status(RefreshError::Timeout).code(),
            tonic::Code::DeadlineExceeded
        );
        assert_eq!(
            chart_fetch_status(RefreshError::Died).code(),
            tonic::Code::Internal
        );
    }

    #[tokio::test]
    async fn chart_admission_sums_request_width_across_inflight_charts() {
        let admission = Arc::new(admission(4));
        let held = admission.acquire(3).await;
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel();
        let waiter_admission = admission.clone();
        let waiter = tokio::spawn(async move {
            ready_tx.send(()).unwrap();
            let permit = waiter_admission.acquire(2).await;
            acquired_tx.send(()).unwrap();
            drop(permit);
        });

        ready_rx.await.unwrap();
        let mut acquired_rx = acquired_rx;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut acquired_rx)
                .await
                .is_err()
        );
        drop(held);
        tokio::time::timeout(Duration::from_secs(1), acquired_rx)
            .await
            .expect("waiter should acquire after the active chart releases")
            .unwrap();
        waiter.await.unwrap();
    }

    #[tokio::test]
    async fn wide_request_initially_takes_the_full_gate() {
        let admission = Arc::new(admission(DEFAULT_CHART_INFLIGHT_SERIES));
        // Three bindings at the editor's default 12 runs require 36 reads.
        let wide = admission.acquire(36).await;
        assert_eq!(admission.slots.available_permits(), 0);

        let waiter_admission = admission.clone();
        let mut waiter = tokio::spawn(async move { waiter_admission.acquire(1).await });
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut waiter)
            .await
            .is_err());
        drop(wide);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("a narrow request should proceed after the wide chart")
            .unwrap();
    }

    #[tokio::test]
    async fn chart_admission_rejects_only_past_the_stable_request_bound() {
        let admission = admission(2);
        // Lower concurrency clamps the widest request instead of shrinking the request shape limit.
        drop(admission.acquire(MAX_CHART_REQUEST_RAW_SERIES).await);
        // The request boundary rejects a wider chart before it plans, reserves or reads anything.
        let service = chart_service("http://127.0.0.1:9", admission);
        let error = service
            .query_chart(Request::new(proto::ChartRequest {
                y_series: (0..=MAX_CHART_REQUEST_RAW_SERIES)
                    .map(|i| series(&format!("run-{i}")))
                    .collect(),
                ..Default::default()
            }))
            .await
            .expect_err("a request cannot exceed the stable shape bound");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            error.message(),
            "chart requires 129 raw-series reads; maximum per chart is 128"
        );
        assert_eq!(service.chart_admission.slots.available_permits(), 2);
        assert_eq!(
            service.chart_admission.bytes.in_use(),
            0,
            "rejected before any byte is reserved"
        );
    }

    #[tokio::test]
    async fn chart_deadline_cancels_work_and_releases_its_full_weight() {
        let admission = admission(2);
        let error = admission
            .run_with_timeout(
                2,
                vec![1_000],
                Duration::from_millis(20),
                |_charge, _permit| std::future::pending::<Result<(), Status>>(),
            )
            .await
            .expect_err("pending work must hit the server deadline");
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        assert_eq!(admission.slots.available_permits(), 2);
        assert_eq!(admission.bytes.in_use(), 0);
        let _next = tokio::time::timeout(Duration::from_secs(1), admission.acquire(2))
            .await
            .expect("the deadline must un-wedge the gate");
    }

    /// `runs` refs whose series each carry `tags` tags of `points` rows; tag `t` of run `r` sits at its own steps, so the shared axis gets about as many columns as the target allows.
    pub(super) fn tagged_chart(
        runs: usize,
        tags: usize,
        points: i64,
        grown: i64,
    ) -> (proto::ChartRequest, Vec<Vec<VersionedRawPoint>>) {
        let request = proto::ChartRequest {
            y_series: (0..runs).map(|r| series(&format!("run-{r}"))).collect(),
            target_resolution: 1000,
            ..Default::default()
        };
        let rows = (0..runs)
            .map(|r| {
                (0..tags)
                    .flat_map(|t| {
                        let lane = (r * tags + t) as i64;
                        (0..points + grown).map(move |i| VersionedRawPoint {
                            tag: format!("t{t:03}"),
                            step: lane * 4 + i * 4 * (runs * tags) as i64,
                            timestamp_ms: lane + i,
                            value: (lane + i) as f32,
                            is_value: 1,
                            inserted_ms: 1 + i,
                        })
                    })
                    .collect()
            })
            .collect();
        (request, rows)
    }

    /// A plan with the memo and held state off.
    fn plan_off(request: &proto::ChartRequest) -> ChartPlan {
        plan_audited(request, false)
    }

    /// A plan with the memo and held state off, audited if `audit` and the poll can audit.
    fn plan_audited(request: &proto::ChartRequest, audit: bool) -> ChartPlan {
        ChartPlan::new(
            request,
            &memo::ChartMemo::new(false),
            &held::HeldStore::new(held::HeldMode::Off, 0),
            || audit,
        )
    }

    /// Peak bytes a build holds, the bytes stage 2 charges it, and those it reserved before its model existed: transients over the rows its refs prepare, as step 1 counts them ([`ChartPlan::prepared_rows`]), plus its output once the model's shape is known (the largest series' transients again among them, which step 1 counts as the build does), reserved at its bound ([`ChartPlan::output_bound`]).
    fn measured_build(
        request: &proto::ChartRequest,
        snapshots: &[Arc<SeriesSnapshot>],
        x_maps: Option<&std::collections::HashMap<String, std::collections::HashMap<i64, f64>>>,
        audit: bool,
    ) -> (proto::ChartResponse, u64, u64, u64, DenseShape) {
        let plan = plan_audited(request, audit);
        let kept = plan
            .series
            .iter()
            .enumerate()
            .map(|(index, key)| {
                let at = request
                    .y_series
                    .iter()
                    .position(|s| SeriesKey::new(&s.project_id, &s.run_id, &s.metric_name) == *key)
                    .expect("every planned series has a ref");
                plan.prepared_rows(index, &snapshots[at])
            })
            .fold(Kept::NONE, Kept::join);
        let shape = std::cell::Cell::new(None);
        let (built, peak, _) = alloc_probe::peak_and_live_bytes(|| {
            super::build_chart(
                request,
                &plan.params,
                snapshots,
                x_maps,
                audit,
                None,
                &|s| shape.set(Some(s)),
                false,
            )
            .unwrap()
        });
        let shape: DenseShape = shape.get().expect("the build reports its model");
        assert_eq!(
            (shape.prepared_rows, shape.largest_series, shape.series),
            (kept.rows, kept.largest, kept.series),
            "step 1 counts what the build prepares"
        );
        assert!(
            shape.columns <= columns_bound(plan.params.spec, kept),
            "{shape:?}"
        );
        let transients = ChartPlan::transient_bytes(kept.rows);
        let charged = transients + ChartPlan::output_bytes(shape, audit);
        let reserved = transients + plan.output_bound(kept);
        (built.response, peak, charged, reserved, shape)
    }

    #[test]
    fn build_charges_cover_measured_peaks() {
        let snapshots = |rows: &[Vec<VersionedRawPoint>]| -> Vec<Arc<SeriesSnapshot>> {
            rows.iter()
                .map(|rows| Arc::new(SeriesSnapshot::full(rows.clone())))
                .collect()
        };
        let mut cases: Vec<(&str, proto::ChartRequest, Vec<Arc<SeriesSnapshot>>, bool)> =
            Vec::new();
        // Many expanded series over few rows: the dense model dwarfs the rows' transients.
        let (wide, rows) = tagged_chart(16, 64, 2, 0);
        cases.push(("16x64 tags", wide.clone(), snapshots(&rows), false));
        let mut smoothed = wide.clone();
        smoothed.target_resolution = 10_000;
        smoothed.smoothing = Some(proto::SmoothingConfig {
            algorithm: Algorithm::Ema as i32,
            time_constant: 4.0,
            ..Default::default()
        });
        cases.push((
            "16x64 tags smoothed passthrough",
            smoothed.clone(),
            snapshots(&rows),
            false,
        ));
        let mut banded = wide.clone();
        banded.use_timestamp_axis = true;
        banded.target_resolution = 10_000;
        let duplicated: Vec<Vec<VersionedRawPoint>> = rows
            .iter()
            .map(|rows| {
                rows.iter()
                    .map(|row| VersionedRawPoint {
                        timestamp_ms: row.timestamp_ms / 2,
                        ..row.clone()
                    })
                    .collect()
            })
            .collect();
        let mut banded_smoothed = banded.clone();
        banded_smoothed.smoothing = smoothed.smoothing;
        cases.push((
            "16x64 tags banded passthrough",
            banded,
            snapshots(&duplicated),
            false,
        ));
        cases.push((
            "16x64 tags banded smoothed passthrough",
            banded_smoothed,
            snapshots(&duplicated),
            false,
        ));
        let mut downsampled_smoothed = wide.clone();
        downsampled_smoothed.smoothing = Some(proto::SmoothingConfig {
            algorithm: Algorithm::SavitzkyGolay as i32,
            window_size: 5,
            poly_order: 1,
            ..Default::default()
        });
        cases.push((
            "16x64 tags smoothed grid",
            downsampled_smoothed,
            snapshots(&rows),
            false,
        ));
        // The prod-average chart: transients dominate.
        let average = proto::ChartRequest {
            y_series: (0..12).map(|r| series(&format!("run-{r}"))).collect(),
            target_resolution: 500,
            ..Default::default()
        };
        let average_rows: Vec<Vec<VersionedRawPoint>> = (0..12)
            .map(|r| {
                (0..26_000)
                    .map(|step| point("", step, ((step * (r + 3)) % 97) as f32, 1 + step))
                    .collect()
            })
            .collect();
        cases.push(("12x26k", average, snapshots(&average_rows), false));
        // 1, 2, 4 and 8 equal series: the fewer, the larger the share of one series' working copies (or a time axis's sort buffer) beside the rows.
        let short: Vec<Vec<VersionedRawPoint>> = average_rows[..8]
            .iter()
            .map(|rows| {
                rows[..20_000]
                    .iter()
                    .map(|row| VersionedRawPoint {
                        timestamp_ms: row.step * 1_000 + (row.step * 7_919) % 13 * 400,
                        ..row.clone()
                    })
                    .collect()
            })
            .collect();
        for (name, runs, smoothing, time) in [
            ("1x20k smoothed", 1, true, false),
            ("1x20k", 1, false, false),
            ("2x20k", 2, false, false),
            ("4x20k", 4, false, false),
            ("8x20k", 8, false, false),
            ("1x20k time", 1, false, true),
            ("2x20k time smoothed", 2, true, true),
            ("8x20k time", 8, false, true),
        ] {
            let request = proto::ChartRequest {
                y_series: (0..runs).map(|r| series(&format!("run-{r}"))).collect(),
                target_resolution: 1000,
                use_timestamp_axis: time,
                smoothing: smoothing.then_some(proto::SmoothingConfig {
                    algorithm: Algorithm::Ema as i32,
                    time_constant: 4.0,
                    ..Default::default()
                }),
                ..Default::default()
            };
            cases.push((name, request, snapshots(&short[..runs]), false));
        }
        // Unequal series: one long run's working copies beside short ones, first or last.
        let unequal = proto::ChartRequest {
            y_series: (0..8).map(|r| series(&format!("run-{r}"))).collect(),
            target_resolution: 1000,
            ..Default::default()
        };
        let mut long_first = vec![short[0].clone()];
        long_first.extend(short[1..].iter().map(|rows| rows[..2_000].to_vec()));
        let mut long_last: Vec<Vec<VersionedRawPoint>> = short[..7]
            .iter()
            .map(|rows| rows[..2_000].to_vec())
            .collect();
        long_last.push(short[7].clone());
        cases.push((
            "20k then 7x2k",
            unequal.clone(),
            snapshots(&long_first),
            false,
        ));
        cases.push(("7x2k then 20k", unequal, snapshots(&long_last), false));
        // Rows of groups no ref selects are charged nothing, and allocated nothing: 8 runs, each picking one of 32 tags by its filter, or each holding scalar rows that a tagged group suppresses. (The runs share one snapshot.)
        let group = |tag: &str, rows: i64| -> Vec<VersionedRawPoint> {
            (0..rows)
                .map(|step| point(tag, step, (step % 89) as f32, 1 + step))
                .collect()
        };
        let eight = |rows: Vec<VersionedRawPoint>| -> Vec<Arc<SeriesSnapshot>> {
            std::iter::repeat_n(Arc::new(SeriesSnapshot::full(rows)), 8).collect()
        };
        let filtered = proto::ChartRequest {
            y_series: (0..8)
                .map(|r| proto::SeriesRef {
                    tags: vec![format!("t{r:03}")],
                    ..series(&format!("run-{r}"))
                })
                .collect(),
            target_resolution: 1000,
            ..Default::default()
        };
        let tags = (0..32)
            .flat_map(|t| group(&format!("t{t:03}"), 20_000))
            .collect();
        cases.push(("8 runs, 1 of 32 tags each", filtered, eight(tags), false));
        let suppressed = proto::ChartRequest {
            y_series: (0..8).map(|r| series(&format!("run-{r}"))).collect(),
            target_resolution: 1000,
            ..Default::default()
        };
        let scalar_and_tag = [group("", 640_000), group("t", 20_000)].concat();
        cases.push((
            "8 runs, scalar suppressed by a tag",
            suppressed,
            eight(scalar_and_tag),
            false,
        ));
        // The audited delta: a reconstructed held model and its prepared rows sit beside the full one.
        let (_, grown_rows) = tagged_chart(16, 64, 2, 1);
        let held_snapshots = snapshots(&rows);
        let held = build_chart(&wide, &held_snapshots, None, false, None, &|_| {}, false)
            .unwrap()
            .response;
        let grown: Vec<Arc<SeriesSnapshot>> = held_snapshots
            .iter()
            .zip(&grown_rows)
            .map(|(held, rows)| held.refreshed_fixture(rows))
            .collect();
        let continued = continuation(&wide, &held);
        cases.push(("16x64 tags audited delta", continued, grown, true));

        // Custom x: each run's points through its x map (built outside the probe, as the request's x reads charge it), monotone or not, smoothed or not.
        let custom = |runs: usize, smoothing: bool, x_series: bool| proto::ChartRequest {
            y_series: (0..runs).map(|r| series(&format!("run-{r}"))).collect(),
            x_series: x_series.then(|| proto::SeriesRef {
                metric_name: "x".to_string(),
                ..series("run-0")
            }),
            target_resolution: 1000,
            smoothing: smoothing.then_some(proto::SmoothingConfig {
                algorithm: Algorithm::Ema as i32,
                time_constant: 4.0,
                ..Default::default()
            }),
            ..Default::default()
        };
        let x_maps = |runs: usize, shuffled: bool| {
            (0..runs)
                .map(|r| {
                    let map: std::collections::HashMap<i64, f64> = (0..20_000i64)
                        .map(|step| {
                            let jitter = if shuffled { (step * 7_919) % 13 } else { 0 };
                            (step, step as f64 * 0.5 + jitter as f64)
                        })
                        .collect();
                    (format!("run-{r}"), map)
                })
                .collect::<std::collections::HashMap<_, _>>()
        };
        let mut custom_cases = Vec::new();
        for (name, runs, smoothing, shuffled) in [
            ("1x20k custom x", 1, false, false),
            ("1x20k custom x shuffled", 1, false, true),
            ("1x20k custom x smoothed shuffled", 1, true, true),
            ("4x20k custom x shuffled", 4, false, true),
        ] {
            custom_cases.push((
                name,
                custom(runs, smoothing, true),
                snapshots(&short[..runs]),
                Some(x_maps(runs, shuffled)),
            ));
        }

        let cases = cases
            .into_iter()
            .map(|(name, request, snapshots, audit)| (name, request, snapshots, None, audit))
            .chain(
                custom_cases
                    .into_iter()
                    .map(|(name, request, snapshots, maps)| {
                        (name, request, snapshots, maps, false)
                    }),
            );
        for (name, request, snapshots, maps, audit) in cases {
            let (response, peak, charged, reserved, shape) =
                measured_build(&request, &snapshots, maps.as_ref(), audit);
            let rows = shape.prepared_rows as u64;
            let cells = shape.series as u64 * shape.columns as u64;
            let models = if audit { 2 } else { 1 };
            // `--nocapture` shows the per-cell figure DENSE_BYTES_PER_CELL is set from, and the per-row one BUILD_BYTES_PER_ROW is.
            eprintln!(
                "PROBE {name}: series={} columns={} rows={rows} largest={} delta={} peak={peak} charged={charged} reserved={reserved} bytes_per_cell={:.1} bytes_per_row={:.1}",
                shape.series,
                shape.columns,
                shape.largest_series,
                response.delta,
                (peak as f64 - (rows * models * budget::BUILD_BYTES_PER_ROW) as f64)
                    / (cells * models) as f64,
                (peak as f64 - (cells * models * budget::DENSE_BYTES_PER_CELL) as f64)
                    / rows as f64,
            );
            assert_eq!(response.delta, audit, "{name}: the intended path");
            assert!(
                peak <= charged,
                "{name}: the build held {peak} bytes, charged {charged}"
            );
            // Its output was reserved at its bound before its model existed.
            assert!(
                peak <= reserved && charged <= reserved,
                "{name}: the build held {peak} bytes, charged {charged}, reserved {reserved} before its model"
            );
            // Where the model dominates, the charge stays within a small factor of what the build holds; where the rows do, within half again (unequal series hold less than the largest one's working copies beside every row).
            if cells * budget::DENSE_BYTES_PER_CELL > rows * budget::BUILD_BYTES_PER_ROW {
                assert!(
                    charged <= 3 * peak,
                    "{name}: charged {charged} for a {peak}-byte build"
                );
            } else {
                assert!(
                    2 * charged <= 3 * peak,
                    "{name}: charged {charged} for a {peak}-byte build"
                );
            }
        }
    }

    #[tokio::test]
    async fn fresh_hit_units_return_to_the_gate_after_the_lookups() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let rendered = recorder.handle();
        let _local = metrics::set_default_local_recorder(&recorder);
        let inflight = || {
            rendered
                .render()
                .lines()
                .find_map(|line| line.strip_prefix("mkdb2_chart_inflight_series "))
                .map(|value| value.parse::<f64>().unwrap())
        };
        let admission = admission(32);
        let mut permit = admission.acquire(12).await;
        assert_eq!(admission.slots.available_permits(), 20);
        assert_eq!(inflight(), Some(12.0));
        // 9 of 12 distinct series were fresh hits: 3 reads keep one unit each, and the gauge counts only those.
        permit.keep_for_reads(3);
        assert_eq!(permit.units(), 3);
        assert_eq!(admission.slots.available_permits(), 29);
        assert_eq!(inflight(), Some(3.0));
        let units: Vec<_> = std::iter::from_fn(|| permit.split_refresh_unit())
            .take(8)
            .collect();
        assert_eq!(
            units.len(),
            3,
            "every remaining read detaches on its own unit"
        );
        drop(units);
        assert_eq!(admission.slots.available_permits(), 32);
        drop(permit);
        assert_eq!(inflight(), Some(0.0));
    }

    #[tokio::test]
    async fn a_clamped_request_returns_only_units_its_reads_do_not_need() {
        let admission = admission(32);
        let floor = admission.fetch_concurrency();
        // More reads than units: everything and the inline floor stay (Stage R (a)).
        let mut permit = admission.acquire(MAX_CHART_REQUEST_RAW_SERIES).await;
        permit.keep_for_reads(40);
        assert_eq!(permit.units(), 32);
        let units: Vec<_> = std::iter::from_fn(|| permit.split_refresh_unit())
            .take(64)
            .collect();
        assert_eq!(units.len(), 32 - floor);
        drop((units, permit));
        // Fewer reads than units: the rest go back and every read can split its own.
        let mut permit = admission.acquire(MAX_CHART_REQUEST_RAW_SERIES).await;
        permit.keep_for_reads(10);
        assert_eq!(admission.slots.available_permits(), 22);
        let units: Vec<_> = std::iter::from_fn(|| permit.split_refresh_unit())
            .take(64)
            .collect();
        assert_eq!(units.len(), 10);
        drop(units);
        // The custom-X phase keeps only its own units.
        let mut permit = admission.acquire(12).await;
        permit.keep_for_reads(5);
        permit.keep_for_reads(2);
        assert_eq!(permit.units(), 2);
        assert_eq!(admission.slots.available_permits(), 30);
    }

    /// Transients are charged over the rows `prepare` keeps: in the fetch range, of the tag groups each ref selects (tagged groups over the scalar one, the ref's filter), for every ref naming a series.
    #[test]
    fn prepared_rows_count_what_prepare_keeps() {
        let rows = |tags: &[(&str, i64)]| {
            Arc::new(SeriesSnapshot::full(
                tags.iter()
                    .flat_map(|&(tag, steps)| {
                        (0..steps).map(move |step| point(tag, step, step as f32, 1))
                    })
                    .collect(),
            ))
        };
        // Run a: scalar rows beside tagged ones (the tagged win); run b: scalar only.
        let a = rows(&[("", 100), ("x", 50), ("y", 70)]);
        let b = rows(&[("", 200)]);
        let tagged = |run: &str, tags: &[&str]| proto::SeriesRef {
            tags: tags.iter().map(|tag| tag.to_string()).collect(),
            ..series(run)
        };
        let zoomed =
            |refs: Vec<proto::SeriesRef>, min: Option<i64>, max: Option<i64>| proto::ChartRequest {
                y_series: refs,
                step_min: min,
                step_max: max,
                target_resolution: 300,
                ..Default::default()
            };
        let requests = [
            zoomed(vec![series("a"), series("b")], None, None),
            zoomed(vec![tagged("a", &["y"]), series("b")], Some(10), Some(30)),
            zoomed(
                vec![series("a"), tagged("a", &["x"]), series("b"), series("b")],
                Some(40),
                None,
            ),
            zoomed(vec![tagged("a", &["z"]), series("b")], None, Some(-1)),
        ];
        for mut request in requests {
            canonicalize_chart_request(&mut request).unwrap();
            let plan = plan_off(&request);
            let snapshots: Vec<Arc<SeriesSnapshot>> = request
                .y_series
                .iter()
                .map(|s| Arc::clone(if s.run_id == "a" { &a } else { &b }))
                .collect();
            let Kept {
                rows: charged,
                largest,
                series,
                ..
            } = plan
                .series
                .iter()
                .enumerate()
                .map(|(index, key)| {
                    plan.prepared_rows(index, if key.run_id == "a" { &a } else { &b })
                })
                .fold(Kept::NONE, Kept::join);
            let p = chart_params(&request);
            let prepared =
                prepare(&request, &p, &snapshots, None, None, false, None, false).unwrap();
            let kept: usize = prepared.series.iter().map(|ps| ps.xs.len()).sum();
            assert_eq!(charged, kept, "{request:?}");
            assert_eq!(series, prepared.series.len(), "{request:?}");
            assert_eq!(
                (prepared.rows, prepared.largest),
                (kept, largest),
                "{request:?}"
            );
            assert_eq!(
                largest,
                prepared
                    .series
                    .iter()
                    .map(|ps| ps.xs.len())
                    .max()
                    .unwrap_or(0),
                "{request:?}"
            );
        }
    }

    /// A ref selecting more tag groups than `prepare` accepts is priced at nothing: `prepare` refuses the request before collecting any of them, so step 1 reserves only the snapshot it pins. At the limit, every group is priced.
    #[test]
    fn refs_past_the_tag_group_limit_are_priced_at_nothing() {
        let wide = |tags: usize| {
            Arc::new(SeriesSnapshot::full(
                (0..tags)
                    .flat_map(|t| {
                        (0..100).map(move |step| point(&format!("t{t:03}"), step, 1.0, 1))
                    })
                    .collect(),
            ))
        };
        let request = proto::ChartRequest {
            y_series: vec![series("a")],
            target_resolution: 300,
            ..Default::default()
        };
        let p = chart_params(&request);
        for tags in [MAX_TAG_GROUPS, MAX_TAG_GROUPS + 1] {
            let rows = wide(tags);
            let mut plan = plan_off(&request);
            let kept = plan.prepared_rows(0, &rows);
            let estimate = plan.estimate(&[CachedSize::Cached(rows.clone())]);
            let prepared = prepare(
                &request,
                &p,
                std::slice::from_ref(&rows),
                None,
                None,
                false,
                None,
                false,
            );
            if tags > MAX_TAG_GROUPS {
                assert_eq!(prepared.err().unwrap().code(), tonic::Code::InvalidArgument);
                assert_eq!(kept, Kept::NONE);
                assert_eq!(estimate, vec![rows.bytes() as u64, 0]);
            } else {
                assert_eq!((kept.rows, kept.series), (prepared.unwrap().rows, tags));
                assert!(
                    estimate.iter().sum::<u64>()
                        > rows.bytes() as u64 + ChartPlan::transient_bytes(kept.rows)
                );
            }
        }
    }

    /// A zoomed chart over heavy series is charged the rows it keeps, so it stays within the budget rather than running alone.
    #[tokio::test]
    async fn a_zoomed_heavy_chart_shares_the_budget() {
        const BUDGET: u64 = 10 << 20;
        let service = chart_service("http://127.0.0.1:9", ChartAdmission::new(32, BUDGET, 1));
        let (_, whole) = cached_chart(&service, 8, 20_000);
        let zoomed = proto::ChartRequest {
            step_min: Some(5_000),
            step_max: Some(5_999),
            ..whole.clone()
        };
        let cache = service.ch.series_cache();
        let estimate = |request: &proto::ChartRequest| {
            let mut plan =
                ChartPlan::new(request, &service.chart_memo, &service.chart_held, || false);
            plan.estimate(&cache.sizes(&plan.series))
                .iter()
                .sum::<u64>()
        };
        let pins = 8 * crate::series_cache::snapshot_bytes(20_000) as u64;
        let per_row = budget::BUILD_BYTES_PER_ROW;
        // Every series' prepared rows, the largest series' once more, and the model's bound.
        assert_eq!(
            estimate(&whole),
            pins + 8 * 20_000 * per_row + output_bound(8, 20_000)
        );
        assert!(estimate(&whole) > BUDGET, "the whole chart runs alone");
        assert_eq!(
            estimate(&zoomed),
            pins + 8 * 1_000 * per_row + output_bound(8, 1_000)
        );

        // Another request holds part of the budget: the zoomed chart is admitted beside it, as the whole one is not.
        let bytes = &service.chart_admission.bytes;
        let other = bytes.reserve(vec![BUDGET / 4]).await;
        let (answer, _, charge) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            admitted_with_charge(&service, zoomed),
        )
        .await
        .expect("admitted beside the other charge");
        answer.await.unwrap();
        assert_eq!(
            charge.items()[..8].iter().sum::<u64>(),
            pins + 8 * 1_000 * per_row,
            "corrected to the rows it prepared"
        );
        drop(charge);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(50),
                bytes.reserve(vec![estimate(&whole)])
            )
            .await
            .is_err(),
            "the whole chart waits for the budget to drain"
        );
        drop(other);
    }

    #[test]
    fn charges_are_estimated_from_the_cache_records_and_corrected_to_real_sizes() {
        let mut request = proto::ChartRequest {
            y_series: vec![series("a"), series("b"), series("a"), series("c")],
            x_series: Some(series("x")),
            ..Default::default()
        };
        let mut plan = plan_off(&request);
        assert_eq!(
            plan.series
                .iter()
                .zip(&plan.refs)
                .map(|(key, refs)| (key.run_id.as_str(), refs.len()))
                .collect::<Vec<_>>(),
            [("a", 2), ("b", 1), ("c", 1)]
        );
        assert_eq!(plan.x_runs, 3);
        let cached = Arc::new(SeriesSnapshot::full(
            (0..100).map(|step| point("", step, 1.0, 1)).collect(),
        ));
        let estimate = plan.estimate(&[
            CachedSize::Cached(cached.clone()),
            evicted(1_000, (0, 999), 1),
            CachedSize::Unknown,
        ]);
        let per_row = budget::BUILD_BYTES_PER_ROW;
        let snapshot = |rows| crate::series_cache::snapshot_bytes(rows) as u64;
        assert_eq!(
            plan.snapshots,
            [
                cached.bytes() as u64,
                snapshot(1_000),
                snapshot(budget::MISS_ESTIMATE_ROWS as usize)
            ],
            "what a detached read of each keeps charged"
        );
        assert_eq!(
            estimate,
            [
                // Both refs of the cached series prepare its 100 rows.
                cached.bytes() as u64 + 100 * 2 * per_row,
                // An evicted series as its record left it: all 1,000 rows on an unzoomed chart.
                series_bytes(1_000),
                series_bytes(budget::MISS_ESTIMATE_ROWS as usize),
                budget::MISS_ESTIMATE_ROWS * budget::X_BYTES_PER_ROW,
                budget::MISS_ESTIMATE_ROWS * budget::X_BYTES_PER_ROW,
                budget::MISS_ESTIMATE_ROWS * budget::X_BYTES_PER_ROW,
                // The largest series' working copies (the prod-average miss's), and a model of its four series with a column per row, its most without a resolution.
                budget::MISS_ESTIMATE_ROWS * per_row
                    + 4 * (200 + 1_000 + budget::MISS_ESTIMATE_ROWS) * budget::DENSE_BYTES_PER_CELL,
            ]
        );
        // A long tag filter is looked up as a set: only the tags it names are prepared.
        let tagged = Arc::new(SeriesSnapshot::full(
            (0..30)
                .map(|i| VersionedRawPoint {
                    tag: format!("t{}", i / 10),
                    step: i % 10,
                    timestamp_ms: i,
                    value: 1.0,
                    is_value: 1,
                    inserted_ms: 1,
                })
                .collect(),
        ));
        let filtered = proto::ChartRequest {
            y_series: vec![proto::SeriesRef {
                tags: (0..4_000).map(|i| format!("t{}", i % 2)).collect(),
                ..series("a")
            }],
            ..Default::default()
        };
        let plan = plan_off(&filtered);
        assert_eq!(plan.params.filters.as_ref().unwrap()[0].len(), 2);
        let kept = plan.prepared_rows(0, &tagged);
        assert_eq!((kept.rows, kept.largest, kept.series), (20, 10, 2));
        request.use_timestamp_axis = true;
        assert_eq!(plan_off(&request).x_runs, 0, "timestamp charts read no x");
    }

    /// An evicted series is priced from its recovery record as a retained one is from its snapshot: the whole snapshot, which its read pins, and transients and output over the rows the chart's step range keeps, at the record's mean density over the steps it held, in as many series as its tag groups.
    #[test]
    fn evicted_series_are_priced_by_the_share_of_their_records_the_range_keeps() {
        let per_row = budget::BUILD_BYTES_PER_ROW;
        let snapshot = crate::series_cache::snapshot_bytes(1_000) as u64;
        let price = |step_min: Option<i64>, step_max: Option<i64>, groups: usize| {
            let request = proto::ChartRequest {
                y_series: vec![series("a")],
                target_resolution: 300,
                step_min,
                step_max,
                ..Default::default()
            };
            plan_off(&request).estimate(&[evicted(1_000, (1_000, 2_998), groups)])
        };
        // Unzoomed: every row, and the output's bound over them (a 300-column step chart: 602 columns).
        assert_eq!(
            price(None, None, 1),
            [snapshot + 1_000 * per_row, output_bound(1, 1_000)]
        );
        // A tenth of the steps it held keeps a tenth of its rows, rounded up, and as many columns.
        assert_eq!(
            price(Some(1_000), Some(1_198), 1),
            [
                snapshot + 100 * per_row,
                100 * per_row + 100 * budget::DENSE_BYTES_PER_CELL
            ]
        );
        assert_eq!(price(Some(1_000), Some(1_000), 1)[0], snapshot + per_row);
        // Past its steps, nothing but the pin.
        assert_eq!(price(Some(3_000), None, 1), [snapshot, 0]);
        // Four tag groups form four series of a quarter of its rows each.
        assert_eq!(
            price(None, None, 4),
            [snapshot + 1_000 * per_row, output_bound(4, 250)]
        );
    }

    /// A ref on an evicted tagged series keeps as many of its groups as the tags it lists, each at the record's mean group, and nothing where that is more than [`MAX_TAG_GROUPS`], as `prepare` refuses it: a retained series is priced the same way ([`ChartPlan::prepared_rows`]).
    #[test]
    fn a_ref_on_an_evicted_tagged_series_keeps_the_groups_its_tags_list() {
        let per_row = budget::BUILD_BYTES_PER_ROW;
        let snapshot = crate::series_cache::snapshot_bytes(1_600) as u64;
        let price = |refs: Vec<proto::SeriesRef>, groups: usize| {
            let request = proto::ChartRequest {
                y_series: refs,
                target_resolution: 300,
                ..Default::default()
            };
            plan_off(&request).estimate(&[evicted(1_600, (0, 1_999), groups)])
        };
        let tagged = |tags: &[&str]| proto::SeriesRef {
            tags: tags.iter().map(|tag| tag.to_string()).collect(),
            ..series("a")
        };
        // Two of 16 groups: 200 rows in two series of 100, a column per row at most.
        assert_eq!(
            price(vec![tagged(&["t1", "t2"])], 16),
            [
                snapshot + 200 * per_row,
                100 * per_row + 2 * 200 * budget::DENSE_BYTES_PER_CELL
            ]
        );
        // Eight refs of one tag each: eight series of 100 rows.
        let eight = (0..8).map(|i| tagged(&[&format!("t{i}")])).collect();
        assert_eq!(
            price(eight, 16),
            [snapshot + 800 * per_row, output_bound(8, 100)]
        );
        let names: Vec<String> = (0..70).map(|i| format!("t{i}")).collect();
        let listed: Vec<&str> = names.iter().map(String::as_str).collect();
        // Every group, unfiltered or listing more tags than it held.
        assert_eq!(
            price(vec![tagged(&[])], 16),
            price(vec![tagged(&listed[..20])], 16)
        );
        // More than 64 groups, unfiltered or listed: refused by `prepare`, so nothing but the pin.
        assert_eq!(price(vec![tagged(&[])], 100), [snapshot, 0]);
        assert_eq!(price(vec![tagged(&listed)], 100), [snapshot, 0]);
        assert_eq!(
            price(vec![tagged(&listed[..64])], 100)[0],
            snapshot + 64 * 16 * per_row,
            "64 of its 100 groups"
        );
    }

    /// An evicted series' pin is priced at the bytes its snapshot charged when it left, which a tagged snapshot's row count alone under-prices (each tag's chunks and tag allocation): 64 tags of two rows.
    #[tokio::test]
    async fn an_evicted_tagged_series_pins_what_its_snapshot_charged() {
        let service = chart_service("http://127.0.0.1:9", ChartAdmission::new(32, 1 << 30, 1));
        let key = SeriesKey::new("project", "run-0", "loss");
        let held = service.ch.series_cache().insert_full(
            key.clone(),
            (0..64)
                .flat_map(|tag| (0..2).map(move |step| point(&format!("t{tag:02}"), step, 1.0, 1)))
                .collect(),
            std::time::Instant::now(),
        );
        service.ch.series_cache().evict_for_test(&key);
        let request = proto::ChartRequest {
            y_series: vec![series("run-0")],
            target_resolution: 300,
            ..Default::default()
        };
        let mut plan = plan_off(&request);
        let estimate = plan.estimate(&service.ch.series_cache().sizes(&plan.series));
        let pin = held.bytes() as u64;
        assert!(pin > crate::series_cache::snapshot_bytes(128) as u64);
        assert_eq!(plan.snapshots, [pin]);
        assert_eq!(estimate[0], pin + 128 * budget::BUILD_BYTES_PER_ROW);
    }

    /// An evicted series of 64 tags and stray scalar rows is priced at its 64 tagged groups, as `prepare` builds it (it drops the scalar rows beside tagged ones, [`selected_groups`]), not refused as 65 groups past [`MAX_TAG_GROUPS`], which priced its pin alone.
    #[tokio::test]
    async fn an_evicted_series_with_stray_scalar_rows_is_priced_at_its_tagged_groups() {
        let service = chart_service("http://127.0.0.1:9", ChartAdmission::new(32, 1 << 30, 1));
        let key = SeriesKey::new("project", "run-0", "loss");
        let held = service.ch.series_cache().insert_full(
            key.clone(),
            std::iter::once(point("", 0, 1.0, 1))
                .chain((0..64).flat_map(|tag| {
                    (0..1_000).map(move |step| point(&format!("t{tag:02}"), step, 1.0, 1))
                }))
                .collect(),
            std::time::Instant::now(),
        );
        service.ch.series_cache().evict_for_test(&key);
        let request = proto::ChartRequest {
            y_series: vec![series("run-0")],
            target_resolution: 300,
            ..Default::default()
        };
        let mut plan = plan_off(&request);
        let estimate = plan.estimate(&service.ch.series_cache().sizes(&plan.series));
        assert_eq!(
            estimate[0],
            held.bytes() as u64 + 64_001 * budget::BUILD_BYTES_PER_ROW,
            "every row, over 64 groups"
        );
        assert!(estimate[1] > 0, "and their output");
    }

    /// An evicted series that held nothing in a chart's range, or nothing at all, adds no series to the chart's output, which the series that do keep rows bound: the output item is the same as with that series absent.
    #[test]
    fn evicted_series_keeping_no_rows_add_no_output_series() {
        let request = |runs: &[&str]| proto::ChartRequest {
            y_series: runs.iter().map(|run| series(run)).collect(),
            target_resolution: 300,
            step_min: Some(0),
            step_max: Some(1_999),
            ..Default::default()
        };
        let kept = Arc::new(SeriesSnapshot::full(
            (0..2_000).map(|step| point("", step, 1.0, 1)).collect(),
        ));
        let alone = plan_off(&request(&["a"])).estimate(&[CachedSize::Cached(kept.clone())]);
        let disjoint = evicted(1_000, (5_000, 5_999), 64);
        let empty = evicted(0, (i64::MAX, i64::MIN), 0);
        for absent in [disjoint, empty] {
            let both = plan_off(&request(&["a", "b"]))
                .estimate(&[CachedSize::Cached(kept.clone()), absent]);
            assert_eq!(
                both[2], alone[1],
                "the output of the one series that keeps rows"
            );
        }
    }

    /// What step 1 charges a series of `rows` rows that one ref prepares in full: its snapshot and its transients.
    fn series_bytes(rows: usize) -> u64 {
        crate::series_cache::snapshot_bytes(rows) as u64 + ChartPlan::transient_bytes(rows)
    }

    /// What step 1 reserves for the output of `series` series, the largest of `largest` rows, on a 300-column step chart spanning more than twice that: the largest series' working copies and a model of the most columns the chart can have (602, [`columns_bound`]).
    fn output_bound(series: u64, largest: usize) -> u64 {
        ChartPlan::transient_bytes(largest) + series * 602 * budget::DENSE_BYTES_PER_CELL
    }

    /// The bound step 1 reserves for 12 series of 1,000 rows on a 300-column step chart: one model of their most columns ([`output_bound`]), nothing else held.
    const BOUND_12X1K: DenseShape = DenseShape {
        series: 12,
        columns: 602,
        prepared_rows: 12_000,
        largest_series: 1_000,
        models: 1,
        extra: 0,
    };

    /// The series cache of a [`chart_service`]: the server's default, whatever the environment says.
    const TEST_CACHE_BYTES: usize = 256 << 20;

    fn chart_service(endpoint: &str, admission: ChartAdmission) -> Arc<QueryService> {
        chart_service_with_cache(endpoint, admission, TEST_CACHE_BYTES)
    }

    /// [`chart_service`] with a series cache of `cache_bytes`.
    fn chart_service_with_cache(
        endpoint: &str,
        admission: ChartAdmission,
        cache_bytes: usize,
    ) -> Arc<QueryService> {
        let (events, _) = tokio::sync::broadcast::channel(8);
        let mut service = QueryService::new(
            Arc::new(
                ChClient::new(endpoint)
                    .unwrap()
                    .with_series_cache_budget(cache_bytes),
            ),
            Arc::new(PgStore::test_store()),
            crate::ingest::BumpCoalescer::empty_for_test(),
            LifecycleGates::new(),
            None,
            events,
        );
        service.chart_admission = admission;
        // A fresh service audits its first draw: a test's polls are unaudited until it sets the draw counter.
        service.delta_audit = std::sync::atomic::AtomicU64::new(1);
        Arc::new(service)
    }

    /// [`chart_service`] with the memo off and held state in `mode`.
    fn held_service(mode: held::HeldMode, admission: ChartAdmission) -> Arc<QueryService> {
        let mut service = chart_service("http://127.0.0.1:9", admission);
        let unshared = Arc::get_mut(&mut service).expect("not yet shared");
        unshared.chart_memo = Arc::new(memo::ChartMemo::new(false));
        unshared.chart_held = Arc::new(held::HeldStore::new(mode, usize::MAX));
        service
    }

    /// `runs` cached series of `rows` rows each, and a 300-column chart of them.
    fn cached_chart(
        service: &QueryService,
        runs: usize,
        rows: i64,
    ) -> (Vec<String>, proto::ChartRequest) {
        let runs: Vec<String> = (0..runs).map(|i| format!("run-{i}")).collect();
        for run in &runs {
            cache_rows(service, run, rows);
        }
        let request = proto::ChartRequest {
            y_series: runs.iter().map(|run| series(run)).collect(),
            target_resolution: 300,
            ..Default::default()
        };
        (runs, request)
    }

    fn cache_rows(service: &QueryService, run_id: &str, rows: i64) {
        service.ch.series_cache().insert_full(
            SeriesKey::new("project", run_id, "loss"),
            (0..rows)
                .map(|step| point("", step, step as f32, 1))
                .collect(),
            std::time::Instant::now(),
        );
    }

    /// Yield 20 times: long enough for spawned polls to reach whatever they wait on next.
    async fn yield_a_while() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    /// Admit a request as `run_with_timeout` does, then answer it as `query_chart` does ([`QueryService::query_chart_admitted`], the test store answering the lifecycle checks); also the step-1 estimate and a handle on the request's charge (it keeps the charge's items readable after the request ends).
    async fn admitted_with_charge(
        service: &Arc<QueryService>,
        request: proto::ChartRequest,
    ) -> (
        impl std::future::Future<Output = Result<proto::ChartResponse, Status>> + Send + 'static,
        Vec<u64>,
        Arc<budget::ChartCharge>,
    ) {
        let mut plan = ChartPlan::new(&request, &service.chart_memo, &service.chart_held, || {
            service.draw_audit()
        });
        let estimate = plan.estimate(&service.ch.series_cache().sizes(&plan.series));
        let admission = &service.chart_admission;
        let charge = admission.bytes.reserve(estimate.clone()).await;
        let handle = charge.clone();
        let weight = chart_admission_weight(&request);
        let permit = admission.acquire(weight).await;
        let (service, request) = (service.clone(), Arc::new(request));
        (
            async move {
                let admitted =
                    service.query_chart_admitted(&request, &mut plan, weight, charge, permit);
                admitted.await
            },
            estimate,
            handle,
        )
    }

    #[tokio::test]
    async fn memo_held_polls_reserve_no_transients_and_a_build_charges_them_first() {
        let service = chart_service("http://127.0.0.1:9", ChartAdmission::new(32, 1 << 30, 1));
        let (_, request) = cached_chart(&service, 12, 1_000);
        let admission = &service.chart_admission;
        let pins = |rows: &[usize]| -> u64 {
            rows.iter()
                .map(|&rows| crate::series_cache::snapshot_bytes(rows) as u64)
                .sum()
        };
        let (first, _, _) = admitted_with_charge(&service, request.clone()).await;
        let first = first.await.unwrap();
        let polled = continuation(&request, &first);
        // No memo for the key yet: the poll reserves its transients, builds, and leaves a memo.
        let (unchanged, estimate, _) = admitted_with_charge(&service, polled.clone()).await;
        assert_eq!(
            estimate.iter().sum::<u64>(),
            12 * series_bytes(1_000) + output_bound(12, 1_000)
        );
        let unchanged = unchanged.await.unwrap();
        assert!(unchanged.delta && unchanged.x_values.is_empty());

        // Audited, the poll builds in full behind the memo: step 1 reserves its transients and its output's bound with the audit's model and second prepare.
        service
            .delta_audit
            .store(67 * 1_000, std::sync::atomic::Ordering::Relaxed);
        let (audited, estimate, _) = admitted_with_charge(&service, polled.clone()).await;
        let audited_output = ChartPlan::output_bytes(BOUND_12X1K, true);
        assert_eq!(
            estimate.iter().sum::<u64>(),
            12 * series_bytes(1_000) + audited_output
        );
        assert_eq!(audited.await.unwrap(), unchanged);

        // A memo is held for the key: only the pins are reserved, and the memo answers without a build slot.
        let blocker = admission.builds.hold().await;
        let (served, estimate, _) = admitted_with_charge(&service, polled.clone()).await;
        assert_eq!(estimate.iter().sum::<u64>(), pins(&[1_000; 12]));
        assert_eq!(served.await.unwrap(), unchanged);
        assert_eq!(admission.bytes.in_use(), 0);

        // The key still holds a memo, but a series moved on: the poll builds, and with no room for its transients it starts over, waiting for room for them with its bytes given back meanwhile.
        cache_rows(&service, "run-0", 1_001);
        let mut rows = vec![1_000; 12];
        rows[0] = 1_001;
        let (rebuilt, estimate, charge) = admitted_with_charge(&service, polled).await;
        assert_eq!(estimate.iter().sum::<u64>(), pins(&rows));
        let full = (1 << 30) - pins(&rows);
        let other = admission.bytes.reserve(vec![full]).await;
        let rebuilt = tokio::spawn(rebuilt);
        crate::wait_until(|| admission.bytes.in_use() == full).await;
        yield_a_while().await;
        assert_eq!(admission.bytes.in_use(), full, "waiting, not overdrawing");
        assert!(!rebuilt.is_finished());
        drop(other);
        let with_transients: u64 = rows.iter().map(|&rows| series_bytes(rows)).sum();
        crate::wait_until(|| admission.bytes.in_use() == with_transients + output_bound(12, 1_001))
            .await;
        drop(blocker);
        let rebuilt = rebuilt.await.unwrap().unwrap();
        assert!(!rebuilt.delta, "a new lineage answers in full");
        assert_eq!(
            admission.bytes.in_use(),
            with_transients + charge.items()[12]
        );
        let output = ChartPlan::output_bytes(
            DenseShape {
                columns: rebuilt.x_values.len(),
                prepared_rows: rows.iter().sum(),
                largest_series: 1_001,
                ..BOUND_12X1K
            },
            false,
        );
        assert_eq!(
            charge.items()[12],
            output,
            "the build charged its dense output"
        );
        drop(charge);
        assert_eq!(admission.bytes.in_use(), 0);
    }

    /// Polls that skipped their transients (a memo was held for each) and build after all, with no room for them beside a request holding the rest of the budget, start over ([`ChartPlan::restart`]): while they wait, none holds a snapshot, a byte, a scan unit or a run guard, and each checks its run's lifecycle again before it reads again. Two polls: what one frees by starting over is no room for the other's transients.
    #[tokio::test]
    async fn polls_starting_over_wait_holding_no_snapshot() {
        let pins = 2 * crate::series_cache::snapshot_bytes(1_001) as u64;
        let (other, slack) = (1 << 30, 1_024);
        let limit = pins + other + slack;
        let service = chart_service("http://127.0.0.1:9", ChartAdmission::new(32, limit, 1));
        let runs: Vec<String> = (0..2).map(|i| format!("run-{i}")).collect();
        let mut polls = Vec::new();
        for run in &runs {
            cache_rows(&service, run, 1_000);
            let request = proto::ChartRequest {
                y_series: vec![series(run)],
                target_resolution: 300,
                ..Default::default()
            };
            let first = admitted_with_charge(&service, request.clone()).await.0;
            let polled = continuation(&request, &first.await.unwrap());
            let unchanged = admitted_with_charge(&service, polled.clone()).await.0;
            assert!(unchanged.await.unwrap().delta, "memoized");
            polls.push(polled);
        }
        // Every run moves on: each poll reserves only its pins, misses its memo and builds.
        for run in &runs {
            cache_rows(&service, run, 1_001);
        }
        let pinned: Vec<std::sync::Weak<SeriesSnapshot>> = runs
            .iter()
            .map(|run| {
                let key = SeriesKey::new("project", run, "loss");
                Arc::downgrade(&service.ch.series_cache().lookup_fresh(&key).unwrap())
            })
            .collect();
        let admission = &service.chart_admission;
        let mut admitted = Vec::new();
        for polled in polls {
            let (poll, estimate, _) = admitted_with_charge(&service, polled).await;
            assert_eq!(
                estimate.iter().sum::<u64>(),
                crate::series_cache::snapshot_bytes(1_001) as u64
            );
            admitted.push(poll);
        }
        // Another request holds all but `slack` of the rest: no room for any poll's transients.
        let other = admission.bytes.reserve(vec![other]).await;
        let admitted: Vec<_> = admitted.into_iter().map(tokio::spawn).collect();
        // Once every poll gave its pins' bytes back, only the other request holds bytes and the polls wait for room.
        crate::wait_until(|| {
            admission.bytes.in_use() == other.items()[0] && admission.bytes.waiters() == runs.len()
        })
        .await;
        // The series cache lets go of the snapshots the polls read.
        for run in &runs {
            cache_rows(&service, run, 1_002);
        }
        let uncounted: usize = pinned
            .iter()
            .filter_map(std::sync::Weak::upgrade)
            .map(|rows| rows.bytes())
            .sum();
        assert_eq!(uncounted, 0, "a waiting poll pins snapshots nothing counts");
        assert_eq!(admission.slots.available_permits(), 32, "nor scan units");
        let writers = tokio::time::timeout(
            Duration::from_secs(1),
            service
                .gates
                .write_many(runs.iter().map(|run| RunKey::new("project", run))),
        )
        .await
        .expect("nor run guards");
        // Meanwhile the first run starts purging.
        let purging = RunKey::new("project", &runs[0]);
        service
            .pg
            .set_test_lifecycle(purging, RunLifecycleClass::Purging);
        drop(writers);
        // The other request ends: each poll is admitted again with its transients, reads the cache's snapshots and answers in full, except the purging run's, which its lifecycle check refuses.
        drop(other);
        for (index, poll) in admitted.into_iter().enumerate() {
            let answer = poll.await.unwrap();
            if index == 0 {
                let status = answer.expect_err("the purging run's poll is refused");
                assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status}");
            } else {
                assert!(!answer.unwrap().delta, "a new lineage answers in full");
            }
        }
        assert_eq!(admission.bytes.in_use(), 0);
        assert_eq!(admission.bytes.waiters(), 0);
    }

    /// A first read larger than step 1 priced it (a series the cache has never seen, priced as the prod average, or a cached one that grew before the poll read it) overdraws the budget only until the poll's reads are done. With no room for it then, the poll starts over, holding none of its bytes or pins while it waits for room at the size it read, then reserves that size (the series a fresh hit now) and builds within it. With room but an earlier request waiting, the read is kept ahead of the waiter, whether step 1 priced it from the cache or as the prod average, since the budget's use stays within its limit, and the poll builds.
    #[tokio::test]
    async fn a_first_read_past_its_price_starts_over_at_its_size_unless_it_may_keep_it() {
        for room in [false, true] {
            for cached in [false, true] {
                first_read_past_its_price(cached, room).await;
            }
        }
    }

    async fn first_read_past_its_price(cached: bool, room: bool) {
        const ROWS: usize = 40_000;
        const LIMIT: u64 = 64 << 20;
        // One full read: the poll that starts over finds the series cached.
        let mut mock = mock_full_reads(&[ROWS]);
        let service = chart_service(mock.url(), ChartAdmission::new(32, LIMIT, 1));
        let admission = &service.chart_admission;
        let priced = if cached {
            // Read from the cache instead.
            mock.non_exhaustive();
            cache_rows(&service, "run-0", 1_000);
            1_000
        } else {
            budget::MISS_ESTIMATE_ROWS as usize
        };
        let request = proto::ChartRequest {
            y_series: vec![series("run-0")],
            target_resolution: 300,
            ..Default::default()
        };
        let (poll, estimate, charge) = admitted_with_charge(&service, request).await;
        let reserved = estimate.iter().sum::<u64>();
        assert_eq!(reserved, series_bytes(priced) + output_bound(1, priced));
        // Priced, the cached series grows before the poll reads it.
        let grown = cached.then(|| {
            cache_rows(&service, "run-0", ROWS as i64);
            let key = SeriesKey::new("project", "run-0", "loss");
            service.ch.series_cache().lookup_fresh(&key).unwrap()
        });
        let read = [series_bytes(ROWS), output_bound(1, ROWS)];
        if room {
            let other = admission.bytes.reserve(vec![LIMIT / 2]).await;
            let mut earlier = Box::pin(admission.bytes.reserve(vec![LIMIT]));
            assert!(futures::FutureExt::now_or_never(earlier.as_mut()).is_none());
            let blocker = admission.builds.hold().await;
            let poll = tokio::spawn(poll);
            crate::wait_until(|| charge.items() == read && admission.bytes.waiters() == 1).await;
            yield_a_while().await;
            assert_eq!(
                (charge.items(), admission.bytes.waiters()),
                (read.to_vec(), 1),
                "cached {cached}"
            );
            drop((blocker, other, charge));
            let (answer, ()) = futures::join!(poll, futures::FutureExt::map(earlier, drop));
            assert!(!answer.unwrap().unwrap().delta);
            assert_eq!(admission.bytes.in_use(), 0);
            return;
        }
        let other = admission.bytes.reserve(vec![LIMIT - reserved]).await;
        let poll = tokio::spawn(poll);
        crate::wait_until(|| admission.bytes.waiters() == 1).await;
        yield_a_while().await;
        assert_eq!(
            (admission.bytes.in_use(), charge.items()),
            (LIMIT - reserved, vec![0, 0]),
            "waiting, holding nothing"
        );
        if let Some(grown) = &grown {
            assert_eq!(
                Arc::strong_count(grown),
                2,
                "no pin: the cache's and this test's"
            );
        }
        assert!(!poll.is_finished());
        // Room again: it reserves the size it read, its build's output at its bound, and builds within it.
        let blocker = admission.builds.hold().await;
        drop(other);
        crate::wait_until(|| charge.items()[0] > 0).await;
        yield_a_while().await;
        assert_eq!(
            (charge.items(), admission.bytes.in_use()),
            (read.to_vec(), read.iter().sum())
        );
        drop(blocker);
        assert!(!poll.await.unwrap().unwrap().delta);
        drop(charge);
        assert_eq!(admission.bytes.in_use(), 0);
    }

    /// A ClickHouse that answers one full read of run-`i`'s `rows[i]` rows (run-0, run-1, ...), steps from 0, in the order a batch asks for them.
    fn mock_full_reads(rows: &[usize]) -> ::clickhouse::test::Mock {
        #[derive(serde::Serialize, ::clickhouse::Row)]
        struct Encoded {
            run_id: String,
            tag: String,
            step: i64,
            timestamp_ms: i64,
            value: f32,
            inserted_ms: i64,
        }
        let mock = ::clickhouse::test::Mock::new();
        mock.add(::clickhouse::test::handlers::provide(
            rows.iter().enumerate().flat_map(|(run, &rows)| {
                (0..rows as i64).map(move |step| Encoded {
                    run_id: format!("run-{run}"),
                    tag: String::new(),
                    step,
                    timestamp_ms: step,
                    value: step as f32,
                    inserted_ms: 1,
                })
            }),
        ));
        mock
    }

    /// A first pass holds back what its corrections take below their prices until it decides, so a waiter is not admitted on bytes its other corrections take back within its grant. Here a cold batch of an empty run and a 40,000-row one, both priced as the prod-average series: the empty run's read frees most of its price, the large run's growth and the output's bound take much of it back, and the poll keeps the rest within its grant. At its build boundary the budget is within its limit and the waiter still waits; before, it was admitted on the empty run's bytes and the budget sat past its limit while the poll waited for a build slot.
    #[tokio::test]
    async fn a_cold_batch_mixing_an_empty_and_a_large_run_keeps_the_budget_within_its_limit() {
        const LIMIT: u64 = 64 << 20;
        let mock = mock_full_reads(&[0, 40_000]);
        let service = chart_service(mock.url(), ChartAdmission::new(32, LIMIT, 1));
        let admission = &service.chart_admission;
        let request = proto::ChartRequest {
            y_series: vec![series("run-0"), series("run-1")],
            target_resolution: 300,
            ..Default::default()
        };
        let (poll, estimate, charge) = admitted_with_charge(&service, request).await;
        let reserved: u64 = estimate.iter().sum();
        let other = admission.bytes.reserve(vec![LIMIT - reserved]).await;
        let mut waiter = Box::pin(admission.bytes.reserve(vec![estimate[0] / 2]));
        assert!(futures::FutureExt::now_or_never(waiter.as_mut()).is_none());
        let blocker = admission.builds.hold().await;
        let poll = tokio::spawn(poll);
        crate::wait_until(|| admission.slots.available_permits() == 32).await;
        yield_a_while().await;
        assert!(
            charge.items().iter().sum::<u64>() <= reserved,
            "it keeps its reads within its grant"
        );
        assert!(
            admission.bytes.in_use() <= LIMIT,
            "within the limit at the build boundary: {} past it",
            admission.bytes.in_use().saturating_sub(LIMIT)
        );
        assert!(futures::FutureExt::now_or_never(waiter.as_mut()).is_none());
        drop((blocker, other));
        assert!(!poll.await.unwrap().unwrap().delta);
        drop(charge);
        drop(waiter.await);
        assert_eq!(admission.bytes.in_use(), 0);
    }

    /// A ClickHouse that answers each full read (`run_id IN [...]`) with run-`i`'s `rows[i]` rows as they are when it asks, steps from 0, each stamped one past its step, and each increment (`run_id = 'run-i'`) with those stamped after its watermark: its URL, and its task.
    fn fake_clickhouse(
        rows: Arc<Vec<std::sync::atomic::AtomicUsize>>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new().fallback(move |uri: axum::http::Uri, body: String| {
            let rows = rows.clone();
            async move {
                // Short reads carry their SQL in the URL, long ones in the body.
                let sql = url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
                    .find(|(name, _)| name == "query")
                    .map_or(body, |(_, sql)| sql.into_owned());
                let index = |run: &str| -> usize {
                    run.trim()
                        .trim_matches('\'')
                        .strip_prefix("run-")
                        .unwrap()
                        .parse()
                        .unwrap()
                };
                let count =
                    |index: usize| rows[index].load(std::sync::atomic::Ordering::Relaxed) as i64;
                let mut out = Vec::new();
                if let Some((_, rest)) = sql.split_once("run_id IN [") {
                    for run in rest[..rest.find(']').unwrap()].split(',') {
                        let run = run.trim().trim_matches('\'');
                        // RowBinary: run_id, tag (empty), step, timestamp_ms, value, inserted_ms.
                        for step in 0..count(index(run)) {
                            out.push(run.len() as u8);
                            out.extend(run.as_bytes());
                            out.push(0);
                            out.extend(step.to_le_bytes());
                            out.extend(step.to_le_bytes());
                            out.extend((step as f32).to_le_bytes());
                            out.extend((step + 1).to_le_bytes());
                        }
                    }
                } else if let (Some((_, run)), Some((_, after))) = (
                    sql.split_once("run_id = "),
                    sql.split_once("fromUnixTimestamp64Milli("),
                ) {
                    let run = index(&run[..run.find(" AND").unwrap()]);
                    let after: i64 = after[..after.find(')').unwrap()].parse().unwrap();
                    // RowBinary: tag (empty), step, timestamp_ms, value, is_value, inserted_ms.
                    for step in after.max(0)..count(run) {
                        out.push(0);
                        out.extend(step.to_le_bytes());
                        out.extend(step.to_le_bytes());
                        out.extend((step as f32).to_le_bytes());
                        out.push(1);
                        out.extend((step + 1).to_le_bytes());
                    }
                }
                out
            }
        });
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, server)
    }

    /// A detached read's split goes back to its request when the read ends, not to the budget: the request holds the rows it read until its correction prices them, so a waiter must not be admitted on those bytes. Here the poll was priced from the cache, which evicted the series before its read, so its full read is detached and comes out at its price; a request waits for exactly the room its split would have freed.
    #[tokio::test]
    async fn a_waiter_is_not_admitted_on_the_bytes_a_detached_read_hands_its_request() {
        const ROWS: usize = 40_000;
        const LIMIT: u64 = 64 << 20;
        let mock = mock_full_reads(&[ROWS]);
        let service = chart_service(mock.url(), ChartAdmission::new(32, LIMIT, 1));
        let admission = &service.chart_admission;
        cache_rows(&service, "run-0", ROWS as i64);
        let request = proto::ChartRequest {
            y_series: vec![series("run-0")],
            target_resolution: 300,
            ..Default::default()
        };
        let (poll, estimate, charge) = admitted_with_charge(&service, request).await;
        let reserved: u64 = estimate.iter().sum();
        let key = SeriesKey::new("project", "run-0", "loss");
        service.ch.series_cache().evict_for_test(&key);
        let other = admission.bytes.reserve(vec![LIMIT - reserved]).await;
        let split = crate::series_cache::snapshot_bytes(ROWS) as u64;
        let mut waiter = Box::pin(admission.bytes.reserve(vec![split]));
        assert!(futures::FutureExt::now_or_never(waiter.as_mut()).is_none());
        // The poll reads, then waits for the build slot this test holds.
        let blocker = admission.builds.hold().await;
        let poll = tokio::spawn(poll);
        // Its reads are done once its scan unit is back.
        crate::wait_until(|| admission.slots.available_permits() == 32).await;
        yield_a_while().await;
        assert_eq!(charge.items(), estimate, "its read came out at its price");
        assert_eq!(admission.bytes.in_use(), LIMIT, "nothing past the budget");
        assert!(
            futures::FutureExt::now_or_never(waiter.as_mut()).is_none(),
            "the poll still holds its rows"
        );
        drop((blocker, other));
        assert!(!poll.await.unwrap().unwrap().delta);
        drop(charge);
        drop(waiter.await);
        assert_eq!(admission.bytes.in_use(), 0);
    }

    /// A cold batch that reads (one ClickHouse read of several runs, which their detached tasks share, each holding its series' split charge) returns every byte, scan unit and build slot to a budget a request waits on in full, whether its poll answers or is cancelled: before its reads, at any point around their end (where a split goes back to the request or, once it is gone, to the budget), waiting for a build slot, or building.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cold_batch_returns_every_byte_unit_and_slot_however_its_poll_ends() {
        let request = proto::ChartRequest {
            y_series: (0..4).map(|i| series(&format!("run-{i}"))).collect(),
            target_resolution: 300,
            ..Default::default()
        };
        let answered = cold_batch_poll(&request, None, false)
            .await
            .expect("not cancelled");
        // Cancelled from at once to half again past the time an answer takes: where each lands varies with the host, and each returns everything wherever it lands.
        for i in 0..=48u32 {
            cold_batch_poll(&request, Some(answered * i / 32), false).await;
        }
        assert_eq!(
            cold_batch_poll(&request, None, true).await,
            None,
            "cancelled waiting for a build slot, its reads done"
        );
    }

    /// One poll of `request`'s cold series (20,000 rows each) against a fresh service, a request waiting for the whole budget behind it: cancelled after `cancel`, or with `blocked` once its reads are done, while the test holds the only build slot, else answered in full. Asserts that everything comes back, and returns how long the poll took if it answered.
    async fn cold_batch_poll(
        request: &proto::ChartRequest,
        cancel: Option<Duration>,
        blocked: bool,
    ) -> Option<Duration> {
        const LIMIT: u64 = 64 << 20;
        let runs = request.y_series.len();
        let mut mock = mock_full_reads(&vec![20_000; runs]);
        // A poll cancelled before it reads never asks.
        mock.non_exhaustive();
        let service = chart_service(mock.url(), ChartAdmission::new(32, LIMIT, 1));
        let admission = &service.chart_admission;
        let (poll, _, charge) = admitted_with_charge(&service, request.clone()).await;
        drop(charge);
        let mut waiter = Box::pin(admission.bytes.reserve(vec![LIMIT]));
        assert!(futures::FutureExt::now_or_never(waiter.as_mut()).is_none());
        let blocker = if blocked {
            Some(admission.builds.hold().await)
        } else {
            None
        };
        let started = std::time::Instant::now();
        let poll = tokio::spawn(poll);
        let keys: Vec<SeriesKey> = (0..runs)
            .map(|i| SeriesKey::new("project", format!("run-{i}"), "loss"))
            .collect();
        let read = || {
            keys.iter()
                .all(|key| service.ch.series_cache().lookup_fresh(key).is_some())
        };
        if blocked {
            crate::wait_until(|| admission.slots.available_permits() == 32 && read()).await;
        } else if let Some(after) = cancel {
            tokio::time::sleep(after).await;
        }
        if blocked || cancel.is_some() {
            poll.abort();
        }
        // An abort lands at the poll's next await, or never if it already answered.
        let end = match poll.await {
            Ok(answer) => {
                assert!(!answer.unwrap().delta);
                Some(started.elapsed())
            }
            Err(error) => {
                assert!(error.is_cancelled());
                None
            }
        };
        drop(blocker);
        let waiting = tokio::time::timeout(Duration::from_secs(10), waiter)
            .await
            .expect("every byte comes back");
        assert_eq!(admission.bytes.in_use(), LIMIT, "the waiter holds them all");
        drop(waiting);
        crate::wait_until(|| {
            admission.slots.available_permits() == 32 && admission.builds.available() == 1
        })
        .await;
        assert_eq!(
            (admission.bytes.in_use(), admission.bytes.waiters()),
            (0, 0)
        );
        end
    }

    /// A poll that started over checks no overrun again: its second pass keeps whatever its reads come to, even past a full budget, so it starts over at most once. Here its cached series grew past its price before each of its two reads.
    #[tokio::test]
    async fn a_poll_that_started_over_keeps_what_its_second_reads_come_to() {
        const LIMIT: u64 = 64 << 20;
        let service = chart_service("http://127.0.0.1:9", ChartAdmission::new(32, LIMIT, 1));
        let admission = &service.chart_admission;
        cache_rows(&service, "run-0", 1_000);
        let request = proto::ChartRequest {
            y_series: vec![series("run-0")],
            target_resolution: 300,
            ..Default::default()
        };
        let (poll, estimate, charge) = admitted_with_charge(&service, request).await;
        let reserved: u64 = estimate.iter().sum();
        // No room for its growth: it starts over, waiting for the size it read.
        let other = admission.bytes.reserve(vec![LIMIT - reserved]).await;
        cache_rows(&service, "run-0", 40_000);
        let poll = tokio::spawn(poll);
        crate::wait_until(|| admission.bytes.waiters() == 1).await;
        let known = series_bytes(40_000) + output_bound(1, 40_000);
        // A request after it takes all the room it leaves, and the series grows again before its second read.
        let mut full = Box::pin(admission.bytes.reserve(vec![LIMIT - known]));
        assert!(futures::FutureExt::now_or_never(full.as_mut()).is_none());
        cache_rows(&service, "run-0", 80_000);
        let blocker = admission.builds.hold().await;
        drop(other);
        let full = full.await;
        let read = vec![series_bytes(80_000), output_bound(1, 80_000)];
        crate::wait_until(|| charge.items() == read).await;
        yield_a_while().await;
        assert_eq!(
            (charge.items(), admission.bytes.waiters()),
            (read, 0),
            "it keeps its overrun and waits for a build slot"
        );
        assert!(admission.bytes.in_use() > LIMIT, "an overdraft");
        drop((blocker, full));
        assert!(!poll.await.unwrap().unwrap().delta);
        drop(charge);
        assert_eq!(admission.bytes.in_use(), 0);
    }

    /// Once the poll starts over, step 1 prices each item at no less than its first pass came to ([`ChartPlan::known`]).
    #[test]
    fn a_poll_starting_over_reserves_at_least_what_its_first_pass_came_to() {
        let request = proto::ChartRequest {
            y_series: vec![series("a"), series("b")],
            target_resolution: 300,
            ..Default::default()
        };
        let mut plan = plan_off(&request);
        let cached = Arc::new(SeriesSnapshot::full(
            (0..1_000).map(|step| point("", step, 1.0, 1)).collect(),
        ));
        let sizes = [
            CachedSize::Cached(cached.clone()),
            evicted(40_000, (0, 39_999), 1),
        ];
        let first = plan.estimate(&sizes);
        let known = vec![first[0] - 1, first[1] + 5_000_000, first[2] + 7];
        plan.restart(held::Advance::Try, known.clone());
        assert_eq!(
            plan.estimate(&sizes),
            [first[0], known[1], known[2]],
            "each item at least what the first pass came to"
        );
    }

    /// Append rows to a cached series as a poll's refresh does, keeping its lineage.
    fn append_rows(
        service: &QueryService,
        run_id: &str,
        steps: std::ops::Range<i64>,
        inserted_ms: i64,
    ) {
        super::frontier_delta_tests::cache_append(
            service.ch.series_cache(),
            &SeriesKey::new("project", run_id, "loss"),
            steps
                .map(|step| point("", step, step as f32, inserted_ms))
                .collect(),
        );
    }

    /// Only a poll that can audit draws the audit: one whose build has a prefix to check, or in `on` mode one whose advance from a held entry the audit checks. A first paint otherwise draws nothing and reserves exactly what it does unaudited; an audited continuation reserves the audit's model and second prepare; a fresh tab's paint of a held chart in `on` mode reserves, unaudited, what a first paint does (it expects no advance) and, audited, the advance's model beside its own, and no audit model.
    #[tokio::test]
    async fn only_polls_that_can_audit_draw_and_reserve_the_audit() {
        use std::sync::atomic::Ordering::Relaxed;
        let service = held_service(held::HeldMode::On, ChartAdmission::new(32, 1 << 30, 4));
        let (_, request) = cached_chart(&service, 12, 1_000);
        let draws = || service.delta_audit.load(Relaxed);
        // A first paint of a chart the store does not hold draws nothing.
        let (_, unaudited, _) = admitted_with_charge(&service, request.clone()).await;
        service.delta_audit.store(67, Relaxed);
        let (first, estimate, _) = admitted_with_charge(&service, request.clone()).await;
        assert_eq!(
            draws(),
            67,
            "a first paint with nothing to audit draws nothing"
        );
        assert_eq!(estimate, unaudited);
        assert_eq!(estimate[12], output_bound(12, 1_000));
        // A continuation draws the audit and reserves its model and second prepare.
        let polled = continuation(&request, &first.await.unwrap());
        let (continued, estimate, _) = admitted_with_charge(&service, polled).await;
        assert_eq!(draws(), 68);
        assert_eq!(estimate[12], ChartPlan::output_bytes(BOUND_12X1K, true));
        continued.await.unwrap();
        assert_eq!(service.chart_held.len_and_bytes().0, 1);
        // Held, the chart's fresh tab draws. Unaudited, it expects no advance, since a first paint answers in full: it reserves what it did with nothing held.
        let (_, estimate, _) = admitted_with_charge(&service, request.clone()).await;
        assert_eq!(draws(), 69);
        assert_eq!(estimate, unaudited);
        // Audited, it reserves the advance's model beside its own, not the audit's.
        service.delta_audit.store(67 * 2, Relaxed);
        let (_, estimate, _) = admitted_with_charge(&service, request).await;
        assert_eq!(draws(), 67 * 2 + 1);
        assert_eq!(
            estimate[12],
            output_bound(12, 1_000) + 12 * 602 * budget::DENSE_BYTES_PER_CELL
        );
    }

    /// In `shadow` mode step 1 reserves the advance's model beside a full build only while the store holds an entry of the request: with nothing held, a first paint and a continuation reserve one model; once it is held, a continuation and a fresh tab's first paint reserve two. A poll planned before the entry was held takes it and charges both models once its model exists.
    #[tokio::test]
    async fn shadow_polls_reserve_the_advance_model_only_for_a_held_request() {
        let service = held_service(held::HeldMode::Shadow, ChartAdmission::new(32, 1 << 30, 4));
        let (_, request) = cached_chart(&service, 12, 1_000);
        let one = output_bound(12, 1_000);
        let two = one + 12 * 602 * budget::DENSE_BYTES_PER_CELL;
        // A tab's first paint, planned while nothing is held, waits to run.
        let (late, estimate, charge) = admitted_with_charge(&service, request.clone()).await;
        assert_eq!(estimate[12], one);
        // Another tab paints and continues, each reserving one model; its continuation's state is held.
        let (first, estimate, _) = admitted_with_charge(&service, request.clone()).await;
        assert_eq!(estimate[12], one);
        let polled = continuation(&request, &first.await.unwrap());
        let (continued, estimate, _) = admitted_with_charge(&service, polled).await;
        assert_eq!(estimate[12], one);
        let continued = continued.await.unwrap();
        assert_eq!(service.chart_held.len_and_bytes().0, 1);
        let next = continuation(&request, &continued);
        for poll in [next, request] {
            let (_, estimate, _) = admitted_with_charge(&service, poll).await;
            assert_eq!(estimate[12], two);
        }
        // The waiting paint takes the entry, advancing it beside its full build: the model's shape corrects its charge to both models.
        let reply = late.await.unwrap();
        let both = ChartPlan::output_bytes(
            DenseShape {
                columns: reply.x_values.len(),
                models: 2,
                ..BOUND_12X1K
            },
            false,
        );
        assert!(
            charge.items()[12] >= both,
            "{} charged for {both}",
            charge.items()[12]
        );
    }

    /// Stage 3b's hint: a poll whose request has held state reserves no transients at step 1; an advance never charges them, only what it held once it has run; an audited poll charges them before its slot wait.
    #[tokio::test]
    async fn held_polls_reserve_no_transients_and_a_full_build_charges_them_first() {
        let service = held_service(held::HeldMode::On, ChartAdmission::new(32, 1 << 30, 1));
        let (runs, request) = cached_chart(&service, 12, 1_000);
        let transients = |rows: &[usize]| -> u64 {
            rows.iter()
                .map(|&rows| rows as u64 * budget::BUILD_BYTES_PER_ROW)
                .sum()
        };
        let keys: Vec<SeriesKey> = runs
            .iter()
            .map(|run| SeriesKey::new("project", run, "loss"))
            .collect();
        // What the cached snapshots weigh (appends add chunks).
        let pins = || -> u64 {
            service
                .ch
                .series_cache()
                .sizes(&keys)
                .iter()
                .map(|size| match size {
                    CachedSize::Cached(rows) => rows.bytes() as u64,
                    _ => panic!("every series is cached"),
                })
                .sum()
        };
        let (first, _, _) = admitted_with_charge(&service, request.clone()).await;
        let first = first.await.unwrap();
        let polled = continuation(&request, &first);
        // Nothing held for the request yet: the poll reserves its transients and its output's bound, builds in full and holds its state.
        let (held, estimate, _) = admitted_with_charge(&service, polled.clone()).await;
        assert_eq!(
            estimate.iter().sum::<u64>(),
            pins() + transients(&[1_000; 12]) + output_bound(12, 1_000)
        );
        // The client holds the latest answer: its advance drops the entry it came from.
        let mut held = held.await.unwrap();

        // Held: step 1 reserves the pins and what the advance holds beside its working copies, and the advance answers without charging transients.
        let mut rows = vec![1_000; 12];
        for (poll, run) in [None, Some(0), Some(5)].into_iter().enumerate() {
            if let Some(run) = run {
                let step = 1_000 + poll as i64;
                append_rows(&service, &runs[run], step..step + 1, 2 + poll as i64);
                rows[run] += 1;
            }
            let next = continuation(&request, &held);
            let (served, estimate, charge) = admitted_with_charge(&service, next).await;
            assert_eq!(estimate[..12].iter().sum::<u64>(), pins());
            assert!(
                estimate[12] > 12 * 300 * budget::DENSE_BYTES_PER_CELL,
                "poll {poll}: the advance's model is reserved"
            );
            let served = served.await.unwrap();
            assert!(served.delta, "poll {poll}: {served:?}");
            assert_eq!(
                charge.items()[..12].iter().sum::<u64>(),
                pins(),
                "poll {poll}: an advance charges no transients"
            );
            assert!(
                charge.items()[12] > 0,
                "poll {poll}: the advance charged its model"
            );
            held = served;
        }

        // An audited poll builds in full though its entry is held: it charges its transients and its output's bound (its largest series' working copies, its model, the audit's model and second prepare, and the advance's model beside it) before it waits for a slot.
        append_rows(&service, "run-1", 1_003..1_004, 6);
        rows[1] += 1;
        let next = continuation(&request, &held);
        service
            .delta_audit
            .store(67 * 1_000, std::sync::atomic::Ordering::Relaxed);
        let blocker = service.chart_admission.builds.hold().await;
        let (audited, _, _) = admitted_with_charge(&service, next).await;
        let audited = tokio::spawn(audited);
        let output = || {
            ChartPlan::output_bytes(
                DenseShape {
                    prepared_rows: rows.iter().sum(),
                    largest_series: *rows.iter().max().unwrap(),
                    models: 2,
                    ..BOUND_12X1K
                },
                true,
            )
        };
        crate::wait_until(|| {
            service.chart_admission.bytes.in_use() == pins() + transients(&rows) + output()
        })
        .await;
        drop(blocker);
        audited.await.unwrap().unwrap();
    }

    /// An `on`-mode poll whose advance falls back, which it learns only in its build slot, leaves the slot and, with no room for its transients, starts over, holding none of its bytes while it waits for room for them; its next pass builds in full without advancing again ([`held::Advance::Skip`]) and charges them: its fallback is counted once, and it answers as the full build does.
    #[test]
    fn a_poll_starting_over_after_its_advance_fell_back_counts_the_fallback_once() {
        const LIMIT: u64 = 1 << 30;
        // Builds run on the runtime's blocking threads: every thread counts into this recorder.
        let recorder: &'static metrics_exporter_prometheus::PrometheusRecorder = Box::leak(
            Box::new(metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder()),
        );
        let rendered = recorder.handle();
        let _local = metrics::set_default_local_recorder(recorder);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .on_thread_start(|| std::mem::forget(metrics::set_default_local_recorder(recorder)))
            .build()
            .unwrap();
        let fallbacks = || -> std::collections::BTreeMap<String, u64> {
            rendered
                .render()
                .lines()
                .filter_map(|line| line.strip_prefix("mkdb2_chart_held_fallback_total{reason=\""))
                .map(|line| {
                    let (reason, count) = line.split_once("\"} ").unwrap();
                    (reason.to_string(), count.parse().unwrap())
                })
                .collect()
        };
        runtime.block_on(async {
            let service = held_service(held::HeldMode::On, ChartAdmission::new(32, LIMIT, 1));
            let (runs, request) = cached_chart(&service, 2, 1_000);
            let (first, _, _) = admitted_with_charge(&service, request.clone()).await;
            let polled = continuation(&request, &first.await.unwrap());
            // Nothing held yet: the continuation builds in full and holds its state.
            let (continued, _, _) = admitted_with_charge(&service, polled).await;
            let continued = continued.await.unwrap();

            // A step past the integers f64 holds exactly: step 1 expects an advance, which falls back in the build slot; the rest of the budget is taken, so the full build's transients do not fit and the poll starts over.
            append_rows(&service, &runs[0], (1 << 53) + 5..(1 << 53) + 6, 10);
            let next = continuation(&request, &continued);
            let before = fallbacks();
            let admission = &service.chart_admission;
            let blocker = admission.builds.hold().await;
            let (poll, estimate, charge) = admitted_with_charge(&service, next.clone()).await;
            let poll = tokio::spawn(poll);
            let reserved = estimate.iter().sum::<u64>();
            yield_a_while().await;
            assert_eq!(admission.bytes.in_use(), reserved);
            let other = admission.bytes.reserve(vec![LIMIT - reserved]).await;
            drop(blocker);
            crate::wait_until(|| admission.bytes.in_use() == LIMIT - reserved).await;
            yield_a_while().await;
            assert_eq!(
                admission.bytes.in_use(),
                LIMIT - reserved,
                "waiting, not overdrawing"
            );
            assert_eq!(admission.builds.available(), 1, "outside the slot");
            assert!(!poll.is_finished());
            drop(other);
            let reply = poll.await.unwrap().unwrap();

            assert!(
                rendered
                    .render()
                    .lines()
                    .any(|line| line == "mkdb2_chart_budget_restarts_total 1"),
                "the poll started over"
            );
            let counted: std::collections::BTreeMap<String, u64> = fallbacks()
                .into_iter()
                .map(|(reason, count)| {
                    let earlier = before.get(&reason).copied().unwrap_or(0);
                    (reason, count - earlier)
                })
                .filter(|&(_, count)| count > 0)
                .collect();
            assert_eq!(counted, [("inexact_x".to_string(), 1)].into());
            let snapshots: Vec<Arc<SeriesSnapshot>> = runs
                .iter()
                .map(|run| {
                    let key = SeriesKey::new("project", run, "loss");
                    service.ch.series_cache().lookup_fresh(&key).unwrap()
                })
                .collect();
            let pins: u64 = snapshots.iter().map(|rows| rows.bytes() as u64).sum();
            assert_eq!(
                charge.items()[..2].iter().sum::<u64>(),
                pins + ChartPlan::transient_bytes(2_001),
                "the full build charged its transients"
            );
            let full = build_chart(&next, &snapshots, None, false, None, &|_| {}, false).unwrap();
            assert_eq!(
                reply,
                proto::ChartResponse {
                    run_versions: reply.run_versions.clone(),
                    ..full.response
                }
            );
        });
    }

    /// Four cached charts of 16 runs × 64 tags, whose dense models dwarf their rows: each reserves its output's bound at step 1, so the budget admits only what it can build at once, never four charts that each allocate their model unreserved in four build slots.
    #[tokio::test]
    async fn wide_charts_reserve_their_output_before_building() {
        use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
        const LIMIT: u64 = 16 << 20;
        let service = chart_service("http://127.0.0.1:9", ChartAdmission::new(32, LIMIT, 4));
        let (request, rows) = tagged_chart(16, 64, 2, 0);
        let snapshots: Vec<Arc<SeriesSnapshot>> = request
            .y_series
            .iter()
            .zip(rows)
            .map(|(s, rows)| {
                let key = SeriesKey::new(&s.project_id, &s.run_id, &s.metric_name);
                service
                    .ch
                    .series_cache()
                    .insert_full(key, rows, std::time::Instant::now())
            })
            .collect();
        let (_, peak, _, _, _) = measured_build(&request, &snapshots, None, false);
        let mut slots = Vec::new();
        for _ in 0..4 {
            slots.push(service.chart_admission.builds.hold().await);
        }
        let admitted = Arc::new(AtomicUsize::new(0));
        let polls: Vec<_> = (0..4)
            .map(|_| {
                let (service, request) = (service.clone(), request.clone());
                let admitted = admitted.clone();
                tokio::spawn(async move {
                    let (poll, _, _) = admitted_with_charge(&service, request).await;
                    admitted.fetch_add(1, Relaxed);
                    poll.await
                })
            })
            .collect();
        let bytes = &service.chart_admission.bytes;
        crate::wait_until(|| admitted.load(Relaxed) + bytes.waiters() == 4).await;
        let admitted = admitted.load(Relaxed) as u64;
        assert!(
            bytes.in_use() >= admitted * peak,
            "{admitted} charts admitted with {} bytes reserved; each build holds {peak}",
            bytes.in_use()
        );
        drop(slots);
        for poll in polls {
            assert_eq!(poll.await.unwrap().unwrap().series.len(), 16 * 64);
        }
        crate::wait_until(|| bytes.in_use() == 0).await;
    }

    #[tokio::test]
    async fn fresh_hits_release_units_and_guards_but_keep_bytes_while_the_build_waits() {
        let service = chart_service("http://127.0.0.1:9", ChartAdmission::new(32, 1 << 30, 1));
        let (runs, request) = cached_chart(&service, 12, 1_000);
        let admission = &service.chart_admission;
        let blocker = admission.builds.hold().await;
        let chart = tokio::spawn(admitted_with_charge(&service, request).await.0);
        // Parked on the build slot: every unit is back and the run guards are gone, but the bytes stay.
        crate::wait_until(|| admission.slots.available_permits() == 32).await;
        let writers = tokio::time::timeout(
            Duration::from_secs(1),
            service
                .gates
                .write_many(runs.iter().map(|run| RunKey::new("project", run))),
        )
        .await
        .expect("run guards are released before the build-slot wait");
        drop(writers);
        assert_eq!(
            admission.bytes.in_use(),
            12 * series_bytes(1_000) + output_bound(12, 1_000)
        );
        assert!(!chart.is_finished());
        drop(blocker);
        let response = chart.await.unwrap().unwrap();
        assert_eq!(response.series.len(), 12);
        assert_eq!(admission.bytes.in_use(), 0);
        assert_eq!(admission.builds.available(), 1);
    }

    #[tokio::test]
    async fn a_cancelled_request_keeps_only_its_detached_reads_charged_until_they_end() {
        // A ClickHouse that accepts the read and never answers until told to hang up.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (hang_up, mut hung_up) = tokio::sync::oneshot::channel::<()>();
        let (accepted_tx, mut accepted) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                tokio::select! {
                    conn = listener.accept() => {
                        held.push(conn.unwrap().0);
                        let _ = accepted_tx.send(());
                    }
                    _ = &mut hung_up => break,
                }
            }
            drop(held);
        });
        let service = chart_service(&endpoint, ChartAdmission::new(32, 1 << 30, 4));
        cache_rows(&service, "cached", 1_000);
        let request = proto::ChartRequest {
            y_series: vec![series("cached"), series("missing")],
            target_resolution: 300,
            ..Default::default()
        };
        let admission = &service.chart_admission;
        let chart = tokio::spawn(admitted_with_charge(&service, request).await.0);
        accepted.recv().await.unwrap();
        // The fresh hit's unit is back; the miss's unit and its snapshot's bytes ride its detached read.
        assert_eq!(admission.slots.available_permits(), 31);
        let charged = admission.bytes.in_use();
        assert_eq!(
            charged,
            series_bytes(1_000)
                + series_bytes(budget::MISS_ESTIMATE_ROWS as usize)
                + output_bound(2, budget::MISS_ESTIMATE_ROWS as usize)
        );
        chart.abort();
        assert!(chart.await.unwrap_err().is_cancelled());
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            admission.bytes.in_use(),
            crate::series_cache::snapshot_bytes(budget::MISS_ESTIMATE_ROWS as usize) as u64,
            "only the cancelled request's read stays charged, at its snapshot's bytes"
        );
        assert_eq!(
            admission.slots.available_permits(),
            31,
            "and still holds its scan unit"
        );
        hang_up.send(()).unwrap();
        crate::wait_until(|| {
            admission.bytes.in_use() == 0 && admission.slots.available_permits() == 32
        })
        .await;
    }

    /// A [`chart_service_with_cache`] with the memo on and held state `on`, reading from `url`.
    fn memo_and_held_service(url: &str, cache_bytes: usize) -> Arc<QueryService> {
        let mut service =
            chart_service_with_cache(url, ChartAdmission::new(32, 1 << 30, 1), cache_bytes);
        let unshared = Arc::get_mut(&mut service).expect("not yet shared");
        unshared.chart_memo = Arc::new(memo::ChartMemo::new(true));
        unshared.chart_held = Arc::new(held::HeldStore::new(held::HeldMode::On, usize::MAX));
        service
    }

    /// A chart whose series the window refuses (its cache holds a hot entry and no room beside it) is read in full at every poll and answers every poll exactly as the same chart over a cache that holds the series: its first paint, an unchanged poll and the memo's answer to it (without a build slot), and after an append an advance of the held chart (without transients), on the lineage its recovery record restores at each read.
    #[tokio::test]
    async fn a_series_the_window_refuses_answers_and_continues_as_a_cached_one() {
        const ROWS: usize = 1_000;
        let rows = Arc::new(vec![
            std::sync::atomic::AtomicUsize::new(0),
            std::sync::atomic::AtomicUsize::new(ROWS),
        ]);
        let (url, server) = fake_clickhouse(rows.clone());
        let blocker: Vec<_> = (0..ROWS as i64)
            .map(|step| point("", step, step as f32, 1))
            .collect();
        let refusing = memo_and_held_service(
            &url,
            crate::series_cache::entry_charge(&SeriesKey::new("project", "run-0", "loss"), blocker),
        );
        cache_rows(&refusing, "run-0", ROWS as i64);
        let holding = memo_and_held_service(&url, TEST_CACHE_BYTES);
        let key = SeriesKey::new("project", "run-1", "loss");
        let refused = |service: &QueryService| {
            let sizes = service
                .ch
                .series_cache()
                .sizes(&[key.clone(), SeriesKey::new("project", "run-0", "loss")]);
            matches!(sizes[..], [CachedSize::Evicted(_), CachedSize::Cached(_)])
        };
        let request = proto::ChartRequest {
            y_series: vec![series("run-1")],
            target_resolution: 300,
            ..Default::default()
        };
        let mut replies = Vec::new();
        for service in [&refusing, &holding] {
            let poll = |request| async {
                admitted_with_charge(service, request)
                    .await
                    .0
                    .await
                    .unwrap()
            };
            let first = poll(request.clone()).await;
            let unchanged = poll(continuation(&request, &first)).await;
            assert!(unchanged.delta && unchanged.x_values.is_empty());
            let blocker = service.chart_admission.builds.hold().await;
            let memoized = poll(continuation(&request, &unchanged)).await;
            drop(blocker);
            rows[1].fetch_add(10, std::sync::atomic::Ordering::Relaxed);
            service
                .ch
                .series_cache()
                .note_bumps(std::iter::once("run-1"));
            let (advance, _, charge) =
                admitted_with_charge(service, continuation(&request, &memoized)).await;
            let advanced = advance.await.unwrap();
            assert!(advanced.delta && !advanced.x_values.is_empty());
            let pinned = match &service.ch.series_cache().sizes(std::slice::from_ref(&key))[..] {
                [CachedSize::Cached(rows)] => rows.bytes(),
                [CachedSize::Evicted(record)] => record.bytes,
                _ => unreachable!("the series was read"),
            };
            assert_eq!(
                charge.items()[0],
                pinned as u64,
                "the held chart advanced, charging no transients"
            );
            rows[1].fetch_sub(10, std::sync::atomic::Ordering::Relaxed);
            // Without the continuation stamps, which name each service's own lineages.
            replies.push([first, unchanged, memoized, advanced].map(|mut reply| {
                reply.frontiers.clear();
                reply
            }));
        }
        assert!(refused(&refusing), "the window refused every read");
        assert!(!refused(&holding));
        assert_eq!(replies[0], replies[1]);
        server.abort();
    }

    /// A memo-served poll reads its series first, so they stay inside the window: a newcomer is still refused their room after they cooled and the memo answered.
    #[tokio::test]
    async fn a_memo_served_poll_keeps_its_series_hot() {
        let service = chart_service_with_cache(
            "http://127.0.0.1:9",
            ChartAdmission::new(32, 1 << 30, 1),
            2 * crate::series_cache::entry_charge(
                &SeriesKey::new("project", "run-0", "loss"),
                (0..1_000)
                    .map(|step| point("", step, step as f32, 1))
                    .collect(),
            ),
        );
        let (runs, request) = cached_chart(&service, 2, 1_000);
        let poll = |request| async {
            admitted_with_charge(&service, request)
                .await
                .0
                .await
                .unwrap()
        };
        let unchanged = poll(continuation(&request, &poll(request.clone()).await)).await;
        service.ch.series_cache().cool_for_test();
        let blocker = service.chart_admission.builds.hold().await;
        assert_eq!(poll(continuation(&request, &unchanged)).await, unchanged);
        drop(blocker);
        let cache = service.ch.series_cache();
        let newcomer = SeriesKey::new("project", "run-9", "loss");
        cache.insert_full(
            newcomer.clone(),
            (0..1_000)
                .map(|step| point("", step, step as f32, 1))
                .collect(),
            std::time::Instant::now(),
        );
        let keys: Vec<_> = runs
            .iter()
            .map(|run| SeriesKey::new("project", run, "loss"))
            .chain([newcomer])
            .collect();
        assert!(matches!(
            cache.sizes(&keys)[..],
            [
                CachedSize::Cached(_),
                CachedSize::Cached(_),
                CachedSize::Evicted(_)
            ]
        ));
    }

    /// Randomized: tabs poll random charts (memo on or off, held state off, `shadow` or `on`, audits, cancellations, small budgets, few build slots and scan units) while a writer appends; every byte, scan unit and build slot comes back, the budget never grants past its limit beside another charge (`count_grant`'s debug assertion, which a release run checks only built with `-C debug-assertions`), nothing deadlocks and no audit fails. In odd cases ClickHouse answers full reads and increments ([`fake_clickhouse`]) of runs of 0 to 30,000 rows, cold at first, so prices miss both ways, the writer's rows land there, tabs evict series between polls, and the series cache often has room for only the smallest series, so its window refuses stores and refreshes evict to grow (both must happen); in even ones it answers nothing and the series start cached. `KYMO_ADMISSION_FUZZ_CASES` lengthens it and `KYMO_ADMISSION_FUZZ_SEED` reseeds it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn randomized_polls_return_every_byte_unit_and_slot() {
        let env = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        let (cases, seed) = (
            env("KYMO_ADMISSION_FUZZ_CASES", 40),
            env("KYMO_ADMISSION_FUZZ_SEED", 1),
        );
        let mut tally = std::collections::BTreeMap::<String, usize>::new();
        // Run alone (filtered to this test), its tally counts only its own polls' restarts; its caches count their own refusals and growth evictions.
        RESTARTS.store(0, std::sync::atomic::Ordering::Relaxed);
        let (mut refusals, mut growth) = (0, 0);
        for case in 0..cases {
            let mut rng = crate::Rng(
                seed.wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    ^ (case + 1).wrapping_mul(0xD1B5_4A32_D192_ED03)
                    | 1,
            );
            let mode = rng.pick(&[
                held::HeldMode::Off,
                held::HeldMode::Shadow,
                held::HeldMode::On,
            ]);
            let memo_on = rng.chance(70);
            let budget_bytes = rng.pick(&[60_000u64, 150_000, 400_000, 2_000_000, 1 << 30]);
            let slots = rng.pick(&[1usize, 2, 4]);
            let units = rng.pick(&[1usize, 3, 32]);
            let held_budget = rng.pick(&[0usize, 50_000, usize::MAX]);
            let answers = case % 2 == 1;
            let read_rows: Arc<Vec<std::sync::atomic::AtomicUsize>> = Arc::new(
                (0..5)
                    .map(|_| match answers {
                        true => rng.pick(&[0, 100, 1_000, 30_000]).into(),
                        false => 0.into(),
                    })
                    .collect(),
            );
            let clickhouse = answers.then(|| fake_clickhouse(read_rows.clone()));
            // Room for the 1 to 4 smallest series as they start, or for all: the window refuses the others while those are hot, and their growth evicts.
            let cache_bytes = match (answers, rng.below(5) as usize) {
                (true, smallest @ 0..4) => {
                    let mut charges: Vec<usize> = (read_rows.iter())
                        .map(|rows| rows.load(std::sync::atomic::Ordering::Relaxed) as i64)
                        .map(|rows| {
                            let rows = (0..rows).map(|step| point("", step, 0.0, step + 1));
                            let key = SeriesKey::new("project", "run-0", "loss");
                            crate::series_cache::entry_charge(&key, rows.collect())
                        })
                        .collect();
                    charges.sort_unstable();
                    charges[..=smallest].iter().sum()
                }
                _ => TEST_CACHE_BYTES,
            };
            let mut service = chart_service_with_cache(
                clickhouse
                    .as_ref()
                    .map_or("http://127.0.0.1:9", |(url, _)| url),
                ChartAdmission::new(units, budget_bytes, slots),
                cache_bytes,
            );
            let unshared = Arc::get_mut(&mut service).expect("not yet shared");
            unshared.chart_memo = Arc::new(memo::ChartMemo::new(memo_on));
            unshared.chart_held = Arc::new(held::HeldStore::new(mode, held_budget));
            let runs: Vec<String> = (0..5).map(|i| format!("run-{i}")).collect();
            let mut next = Vec::new();
            for run in &runs {
                let rows = 50 + rng.below(400) as i64;
                if !answers {
                    cache_rows(&service, run, rows);
                }
                next.push((rows, 2i64));
            }
            // The writer: appends to random runs, one refresh at a time (the polls' own refreshes fail: no ClickHouse answers), or in ClickHouse, where cached copies' refreshes find nothing new and reads after an eviction find them.
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let writer = {
                let (service, runs, stop) = (service.clone(), runs.clone(), stop.clone());
                let (mut rng, read_rows) = (crate::Rng(rng.next() | 1), read_rows.clone());
                tokio::spawn(async move {
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        let r = rng.below(runs.len() as u64) as usize;
                        let (step, stamp) = &mut next[r];
                        let n = 1 + rng.below(20) as i64;
                        if answers {
                            read_rows[r]
                                .fetch_add(n as usize, std::sync::atomic::Ordering::Relaxed);
                            service
                                .ch
                                .series_cache()
                                .note_bumps(std::iter::once(runs[r].as_str()));
                        } else {
                            append_rows(&service, &runs[r], *step..*step + n, *stamp);
                        }
                        (*step, *stamp) = (*step + n, *stamp + 1);
                        tokio::time::sleep(Duration::from_micros(200 + rng.below(2_000))).await;
                    }
                })
            };
            let mut tabs = Vec::new();
            for _ in 0..2 + rng.below(8) {
                let (service, runs) = (service.clone(), runs.clone());
                let mut rng = crate::Rng(rng.next() | 1);
                tabs.push(tokio::spawn(async move {
                    let mut outcomes = std::collections::BTreeMap::<String, usize>::new();
                    let smoothing = match rng.below(4) {
                        0 => Some(proto::SmoothingConfig {
                            algorithm: Algorithm::Ema as i32,
                            time_constant: 5.0,
                            ..Default::default()
                        }),
                        1 => Some(proto::SmoothingConfig {
                            algorithm: Algorithm::SavitzkyGolay as i32,
                            window_size: 9,
                            poly_order: 2,
                            ..Default::default()
                        }),
                        _ => None,
                    };
                    let base = proto::ChartRequest {
                        y_series: (0..1 + rng.below(4))
                            .map(|_| series(&runs[rng.below(runs.len() as u64) as usize]))
                            .collect(),
                        target_resolution: rng.pick(&[0u32, 50, 300]),
                        smoothing,
                        use_timestamp_axis: rng.chance(30),
                        ..Default::default()
                    };
                    let mut held: Option<proto::ChartResponse> = None;
                    for _ in 0..5 + rng.below(25) {
                        if answers && rng.chance(15) {
                            let run = &runs[rng.below(runs.len() as u64) as usize];
                            let key = SeriesKey::new("project", run, "loss");
                            service.ch.series_cache().evict_for_test(&key);
                        }
                        let request = match &held {
                            Some(held) if !rng.chance(10) => continuation(&base, held),
                            _ => base.clone(),
                        };
                        if rng.chance(5) {
                            let draws = &service.delta_audit;
                            let n = draws.load(std::sync::atomic::Ordering::Relaxed);
                            draws.store(
                                n.next_multiple_of(67),
                                std::sync::atomic::Ordering::Relaxed,
                            );
                        }
                        let call = service.query_chart(Request::new(request));
                        let result = if rng.chance(25) {
                            tokio::time::timeout(Duration::from_micros(rng.below(4_000)), call)
                                .await
                                .ok()
                        } else {
                            Some(call.await)
                        };
                        let label = match result {
                            None => "cancelled".to_string(),
                            Some(Ok(reply)) => {
                                let reply = reply.into_inner();
                                assert!(!reply.audit_failed, "an audit failed");
                                let label = if reply.delta { "delta" } else { "full" };
                                match &mut held {
                                    // A splice keeps the held series; the frontiers come from the reply.
                                    Some(held) if reply.delta => held.frontiers = reply.frontiers,
                                    _ => held = Some(reply),
                                }
                                label.to_string()
                            }
                            Some(Err(status)) => format!("{:?}", status.code()),
                        };
                        let reads = if answers { "read " } else { "" };
                        *outcomes.entry(format!("{reads}{label}")).or_default() += 1;
                    }
                    outcomes
                }));
            }
            let finished = tokio::time::timeout(Duration::from_secs(120), async {
                let mut all = Vec::new();
                for tab in tabs {
                    all.push(tab.await.expect("a tab panicked"));
                }
                all
            })
            .await
            .unwrap_or_else(|_| panic!("case {case}: tabs did not finish within 120 s"));
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            writer.await.unwrap();
            for (label, count) in finished.into_iter().flatten() {
                *tally.entry(label).or_default() += count;
            }
            let admission = &service.chart_admission;
            // Builds of cancelled polls finish on their blocking threads.
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while admission.bytes.in_use() != 0
                || admission.bytes.waiters() != 0
                || admission.slots.available_permits() != units
                || admission.builds.available() != slots
            {
                assert!(
                    std::time::Instant::now() < deadline,
                    "case {case} ({mode:?}, memo {memo_on}, budget {budget_bytes}, slots {slots}, units {units}): in_use {} waiters {} units {}/{units} builds {}/{slots}",
                    admission.bytes.in_use(),
                    admission.bytes.waiters(),
                    admission.slots.available_permits(),
                    admission.builds.available(),
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            if let Some((_, server)) = clickhouse {
                server.abort();
            }
            let (refused, evicted) = service.ch.series_cache().tallies();
            refusals += refused;
            growth += evicted;
        }
        eprintln!(
            "ADMISSION FUZZ cases: {cases}, seed: {seed}, outcomes: {tally:?}, restarts: {}, refusals: {refusals}, growth evictions: {growth}",
            RESTARTS.load(std::sync::atomic::Ordering::Relaxed),
        );
        assert!(
            refusals > 0 && growth > 0,
            "the cases never reached the window's refusal or a growth eviction"
        );
    }
}

#[cfg(test)]
mod lifecycle_event_tests {
    use super::*;

    #[test]
    fn successful_or_idempotent_restore_retries_metric_discovery() {
        assert_eq!(
            restore_metric_discovery_runs(RestoreMutationKind::Restored, "run"),
            vec!["run"]
        );
        assert_eq!(
            restore_metric_discovery_runs(RestoreMutationKind::AlreadyActive, "run"),
            vec!["run"]
        );
        assert!(restore_metric_discovery_runs(RestoreMutationKind::Expired, "run").is_empty());
        assert!(restore_metric_discovery_runs(RestoreMutationKind::NotFound, "run").is_empty());
    }
}

#[cfg(test)]
mod rename_run_tests {
    use super::*;

    #[test]
    fn run_names_are_trimmed_visible_and_bounded_for_init_and_rename() {
        assert_eq!(normalize_run_name("renamed run").unwrap(), "renamed run");
        assert_eq!(
            normalize_run_name("  renamed run  ").unwrap(),
            "renamed run"
        );
        assert_eq!(
            normalize_run_name(" \n\t ").unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            normalize_run_name("bad\0name").unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert!(normalize_run_name(&"x".repeat(MAX_RUN_NAME_BYTES)).is_ok());
        assert_eq!(
            normalize_run_name(&"x".repeat(MAX_RUN_NAME_BYTES + 1))
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[cfg(test)]
mod text_window_validation_tests {
    use super::*;

    #[test]
    fn text_metric_names_use_the_ingest_validity_boundary() {
        let longest = "x".repeat(MAX_METRIC_NAME_BYTES);
        assert_eq!(
            normalize_text_metric_names(vec![String::new(), longest.clone(), longest.clone()])
                .unwrap(),
            [String::new(), longest]
        );
        assert_eq!(
            normalize_text_metric_names(vec!["x".repeat(MAX_METRIC_NAME_BYTES + 1)])
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            normalize_text_metric_names(vec!["bad\0metric".to_string()])
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn text_search_preserves_literal_boundary_whitespace() {
        assert_eq!(validate_text_search("  match \t").unwrap(), "  match \t");
        assert_eq!(validate_text_search("   ").unwrap(), "   ");
        assert_eq!(validate_text_search("").unwrap(), "");
        assert_eq!(
            validate_text_search(&"x".repeat(MAX_TEXT_SEARCH_BYTES + 1))
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[cfg(test)]
mod frontier_delta_tests_support {
    use super::*;

    /// Borrow a wire series for the shared codec — the tests' copy of the adapter each crate keeps for its generated types.
    pub fn wire(s: &proto::ChartSeries) -> chart_delta::WireSeries<'_> {
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

    /// Client mirror: inflate a full (non-delta) wire response into the dense model, through the same chart_delta::inflate_chart the frontend's chart_sync::inflate_response uses.
    pub fn inflate_full(resp: &proto::ChartResponse) -> DenseChart {
        assert!(!resp.delta);
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
        .unwrap()
    }
}

#[cfg(test)]
mod emit_inflate_roundtrip {
    use super::*;
    use std::sync::Arc;

    /// THE wire invariant everything else rests on: for any chart the pipeline can produce, inflate(emit_full(model)) reproduces the model bit-for-bit — same bits the result hashes pin, so any gap here would surface as delta-refusal loops in production. Seeded pseudo-random sweep over axis kinds, log ladders, smoothing, NaN/inf markers, negative steps, duplicate timestamps, and short/long runs.
    #[test]
    fn randomized_models_roundtrip_bitexactly() {
        let mut rng = crate::Rng(0x243F6A8885A308D3);
        for case in 0..60u32 {
            let use_time = case % 3 == 1;
            let log = case % 2 == 0;
            let smoothed = case % 5 < 2;
            let target = [50u32, 300, 1000][(case as usize / 3) % 3];
            let nruns = 1 + rng.below(4) as usize;
            // Some cases start past step 0: the log ladder must take its plain-log arm (the +1 shift engages only when the chart contains zero).
            let start = if case % 5 == 4 { 7 } else { 0 };
            let rows: Vec<Arc<Vec<crate::clickhouse::VersionedRawPoint>>> = (0..nruns)
                .map(|_| {
                    let n = 30 + rng.below(2500) as i64;
                    let neg = (rng.below(4) == 0) as i64 * 3; // sometimes a few negative steps
                    Arc::new(
                        (0..n)
                            .map(|s| {
                                let r = rng.next();
                                let value = match r % 50 {
                                    0 => f32::NAN,
                                    1 => f32::INFINITY,
                                    2 => f32::NEG_INFINITY,
                                    _ => ((r % 100_000) as f32) / 7.0 - 3000.0,
                                };
                                crate::clickhouse::VersionedRawPoint {
                                    tag: String::new(),
                                    step: s - neg + start,
                                    // duplicate timestamps sometimes (time-axis dup-x path)
                                    timestamp_ms: 1_000_000 + (s - neg + start) * 10
                                        - (r.is_multiple_of(7) as i64) * 10,
                                    value,
                                    is_value: 1,
                                    inserted_ms: s,
                                }
                            })
                            .collect(),
                    )
                })
                .collect();
            let req = proto::ChartRequest {
                y_series: (0..nruns)
                    .map(|r| proto::SeriesRef {
                        project_id: "p".into(),
                        run_id: format!("run{r}"),
                        metric_name: "loss".into(),
                        tags: vec![],
                    })
                    .collect(),
                target_resolution: target,
                use_timestamp_axis: use_time,
                log_buckets: log,
                smoothing: smoothed.then(|| proto::SmoothingConfig {
                    algorithm: if case % 10 < 5 {
                        proto::smoothing_config::Algorithm::SavitzkyGolay as i32
                    } else {
                        proto::smoothing_config::Algorithm::Triangular as i32
                    },
                    window_size: 12,
                    poly_order: 1,
                    ..Default::default()
                }),
                ..Default::default()
            };
            let p = chart_params(&req);
            let prepared = prepare(
                &req,
                &p,
                &rows
                    .iter()
                    .map(|rows| Arc::new(SeriesSnapshot::full(rows.to_vec())))
                    .collect::<Vec<_>>(),
                None,
                None,
                false,
                None,
                false,
            )
            .unwrap()
            .series;
            let model = respond(&prepared, &p, false).0;
            let wire = emit_full(&model);
            // No filler on the wire: every value entry is finite.
            for s in &wire.series {
                assert!(
                    s.values.iter().all(|v| v.is_finite()),
                    "case {case}: non-finite wire value"
                );
                assert!(
                    s.band_min.iter().all(|v| v.is_finite()),
                    "case {case}: non-finite wire band"
                );
            }
            let out = super::frontier_delta_tests_support::inflate_full(&wire);
            let bits = |v: &[f64]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
            assert_eq!(
                bits(&model.x_values),
                bits(&out.x_values),
                "case {case}: axis"
            );
            assert_eq!(
                bits(&model.xr_min),
                bits(&out.xr_min),
                "case {case}: xr_min"
            );
            assert_eq!(
                bits(&model.xr_max),
                bits(&out.xr_max),
                "case {case}: xr_max"
            );
            assert_eq!(model.series.len(), out.series.len());
            for (i, (a, b)) in model.series.iter().zip(&out.series).enumerate() {
                assert_eq!(
                    bits(&a.values),
                    bits(&b.values),
                    "case {case} series {i}: values"
                );
                assert_eq!(
                    bits(&a.raw_values),
                    bits(&b.raw_values),
                    "case {case} series {i}: raw"
                );
                assert_eq!(
                    bits(&a.min_values),
                    bits(&b.min_values),
                    "case {case} series {i}: min"
                );
                assert_eq!(
                    bits(&a.max_values),
                    bits(&b.max_values),
                    "case {case} series {i}: max"
                );
                assert_eq!(
                    a.nan_indices, b.nan_indices,
                    "case {case} series {i}: markers"
                );
                assert_eq!(a.nan_kinds, b.nan_kinds, "case {case} series {i}: kinds");
                assert_eq!(
                    a.xnan_count, b.xnan_count,
                    "case {case} series {i}: xnan count"
                );
                assert_eq!(chart_delta::hash_series(a), chart_delta::hash_series(b));
            }
            assert_eq!(
                chart_delta::hash_axis(&model),
                chart_delta::hash_axis(&out),
                "case {case}"
            );
        }
    }
}
