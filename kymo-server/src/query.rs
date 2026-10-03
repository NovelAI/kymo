use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tonic::{Request, Response, Status};
use tracing::instrument;

use crate::chart;
use crate::chart_delta::{
    self, SMOOTHING_STATE_SERIES_KEY, SMOOTHING_STATE_VERSION, SMOOTHING_STATE_VERSION_KEY,
};
use crate::clickhouse::{CdnKeyBatchRow, ChClient, RefreshDetach, VersionedRawPoint};
use crate::ingest::{
    is_reserved_project_id, storable_ident, BumpCoalescer, MAX_ID_BYTES, MAX_METRIC_NAME_BYTES,
    RESERVED_PROJECT_ID,
};
use crate::lifecycle::{LifecycleGates, RunKey};
use crate::liveness::{PRESUMED_DEAD_WINDOW_MS, RUNNING_WINDOW_MS};
use crate::pg::{
    InitRunError, PgStore, RestoreMutationKind, RunAccessError, RunInfoRow, RunLifecycleClass,
    RunRecordRow, TimedRunRows, TrashListQuery, TrashMutationKind, TrashPageCursor,
    TRASH_RETENTION_MS,
};
use crate::proto;
use crate::proto::smoothing_config::Algorithm;
use crate::series_cache::{SeriesKey, SeriesSnapshot};

mod lineage;
use lineage::{inspect_lineages, stamp_lineages, VerifiedLineages};

const LIFECYCLE_GATE_TIMEOUT: Duration = Duration::from_secs(15);
const LIFECYCLE_DB_TIMEOUT: Duration = Duration::from_secs(30);
const LIFECYCLE_OPERATION_TIMEOUT: Duration = Duration::from_secs(120);
const LIST_TRASH_DEFAULT_PAGE_SIZE: usize = 100;
const LIST_TRASH_MAX_PAGE_SIZE: usize = 250;
const LIST_TRASH_MAX_FILTER_IDENTITIES: usize = 1_024;
const MAX_RUN_NAME_BYTES: usize = 2_048;
const MAX_TEXT_WINDOW_LINES: u32 = 2_000;
const MAX_TEXT_WINDOW_METRICS: usize = 256;
const MAX_TEXT_SEARCH_BYTES: usize = 512;
/// Chart reads return whole raw series and retain their Arcs through response
/// construction. Charge the exact request shape — Y refs (runs × bindings
/// after frontend resolution) plus the distinct custom-X runs — against
/// one process-wide budget held through response construction. Narrow panels
/// therefore share the budget by their combined width; a request wider than
/// the budget initially takes every permit.
///
/// Wide requests still need a separate hard shape bound because one exclusive
/// request retains every result at once. 128 covers either two editor-max
/// 64-run Y bindings or one such binding plus custom X; more bindings, and
/// persisted max_runs=0, can exceed it and are deliberately rejected. This is
/// a raw-read count, not a byte bound on a pathological individual series.
/// Override the aggregate concurrency budget with
/// KYMO_CHART_INFLIGHT_SERIES; the per-request bound stays stable.
const DEFAULT_CHART_INFLIGHT_SERIES: usize = 32;
const MAX_CHART_REQUEST_RAW_SERIES: usize = 128;
/// `acquire_many_owned` accepts a u32 count while Tokio's semaphore has its own
/// platform limit. This is a primitive boundary, not a product safety cap.
const MAX_CHART_INFLIGHT_SERIES: usize = if Semaphore::MAX_PERMITS < u32::MAX as usize {
    Semaphore::MAX_PERMITS
} else {
    u32::MAX as usize
};
const MAX_CHART_FETCH_CONCURRENCY: usize = 16;
/// Below the frontend's 60-second request deadline. Covers admission waiting
/// and async database work, so a stalled SELECT cannot leave an unread dispatch
/// monopolizing the process-wide gate. Synchronous response construction does
/// not yield to Tokio's timer; the stable per-request ceiling bounds its
/// fan-out.
const CHART_QUERY_TIMEOUT: Duration = Duration::from_secs(45);

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

/// A chart's distinct series grouped by (project, metric), groups and members in first-appearance order.
fn series_groups(keys: &[SeriesKey]) -> Vec<Vec<SeriesKey>> {
    let mut groups: Vec<Vec<SeriesKey>> = Vec::new();
    for key in keys {
        match groups.iter_mut().find(|group| {
            group[0].project_id == key.project_id && group[0].metric_name == key.metric_name
        }) {
            Some(group) if group.contains(key) => {}
            Some(group) => group.push(key.clone()),
            None => groups.push(vec![key.clone()]),
        }
    }
    groups
}

fn chart_admission_weight(request: &proto::ChartRequest) -> usize {
    let x_series = custom_x_runs(request).map_or(0, |(_, runs)| runs.len());
    request.y_series.len().saturating_add(x_series).max(1)
}

struct ChartAdmission {
    slots: Arc<Semaphore>,
    limit: usize,
}

impl ChartAdmission {
    fn new(limit: usize) -> Self {
        let limit = limit.clamp(1, MAX_CHART_INFLIGHT_SERIES);
        metrics::gauge!("mkdb2_chart_inflight_series_limit").set(limit as f64);
        Self {
            slots: Arc::new(Semaphore::new(limit)),
            limit,
        }
    }

    async fn acquire(&self, requested_weight: usize) -> Result<ChartAdmissionPermit, Status> {
        let requested_weight = requested_weight.max(1);
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
        let weight = requested_weight.min(self.limit) as u32;
        let wait_started = std::time::Instant::now();
        let waiting = ChartAdmissionWaiter::new();
        let permit = self
            .slots
            .clone()
            .acquire_many_owned(weight)
            .await
            .map_err(|_| Status::unavailable("chart scheduler shutting down"))?;
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
        Ok(ChartAdmissionPermit {
            permit: std::sync::Mutex::new(permit),
            inline_floor,
            raw_series,
        })
    }

    fn fetch_concurrency(&self) -> usize {
        self.limit.min(MAX_CHART_FETCH_CONCURRENCY)
    }

    async fn run_with_timeout<T, W, F>(
        &self,
        requested_weight: usize,
        deadline: Duration,
        work: W,
    ) -> Result<T, Status>
    where
        W: FnOnce(ChartAdmissionPermit) -> F,
        F: std::future::Future<Output = Result<T, Status>>,
    {
        match tokio::time::timeout(deadline, async {
            // The work owns the weighted permit for the complete request,
            // including response construction (detached refreshes split units
            // off it). While the future is awaiting IO, a timeout drops the
            // outstanding SELECTs before releasing the remainder.
            let permit = self.acquire(requested_weight).await?;
            work(permit).await
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

struct ChartAdmissionWaiter;

impl ChartAdmissionWaiter {
    fn new() -> Self {
        metrics::gauge!("mkdb2_chart_admission_waiters").increment(1.0);
        Self
    }
}

impl Drop for ChartAdmissionWaiter {
    fn drop(&mut self) {
        metrics::gauge!("mkdb2_chart_admission_waiters").decrement(1.0);
    }
}

struct ChartAdmissionPermit {
    /// Mutex so detached refreshes can split units off mid-request.
    permit: std::sync::Mutex<OwnedSemaphorePermit>,
    /// Units the permit must retain rather than split away (see
    /// [`Self::split_refresh_unit`]; full argument in
    /// docs/admission-control.md Stage R (a)).
    inline_floor: usize,
    raw_series: u32,
}

impl ChartAdmissionPermit {
    /// One unit of this request's admitted weight for a detached series
    /// refresh. Completed tasks recycle their units to the shared gate, not
    /// back to this permit, so splitting below `inline_floor` would leave the
    /// request's remaining scans unweighted; `None` keeps the refresh inline,
    /// request-bound. The inflight gauge still moves by the full request
    /// weight at drop — detached tails run on transferred units, outside it.
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
}

impl Drop for ChartAdmissionPermit {
    fn drop(&mut self) {
        metrics::gauge!("mkdb2_chart_inflight_series").decrement(f64::from(self.raw_series));
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
struct ChartParams {
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
}

/// A plan override is safe to consume only when the old-only reconstruction expands to exactly the identities that supplied it. A mismatch is client-controlled cache state, so it is a normal full-answer gate rather than an error.
struct Preparation {
    series: Vec<PreparedSeries>,
    audit_plans_aligned: bool,
}

/// Compare current continuing series against the exact semantic smoothing plans stamped on the response the client actually holds. The caller has already proved the held and continuing counts equal; new complete series do not participate. Missing state safely forces one full answer, which seeds plans for subsequent deltas.
fn smoothing_state_matches(
    req: &proto::ChartRequest,
    p: &ChartParams,
    prepared: &[PreparedSeries],
    continuing: usize,
) -> bool {
    if !p.exact_smoothing_state {
        return true;
    }
    let Some(cache) = req.cache_state.as_ref() else {
        return false;
    };
    let Some(held) = read_smoothing_state(&cache.frontiers, continuing) else {
        return false;
    };
    prepared
        .iter()
        .filter(|ps| ps.continues)
        .map(|ps| ps.smoothing_plan)
        .eq(held)
}

fn stamp_smoothing_state(
    frontiers: &mut std::collections::HashMap<String, i64>,
    p: &ChartParams,
    prepared: &[PreparedSeries],
) {
    if !p.exact_smoothing_state {
        return;
    }
    frontiers.insert(
        SMOOTHING_STATE_VERSION_KEY.to_string(),
        SMOOTHING_STATE_VERSION,
    );
    frontiers.insert(
        SMOOTHING_STATE_SERIES_KEY.to_string(),
        prepared.len() as i64,
    );
    for (index, ps) in prepared.iter().enumerate() {
        frontiers.insert(
            smoothing_state_key(index),
            encode_smoothing_plan(ps.smoothing_plan),
        );
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
    let Some(changing) = (0..plot.len())
        .find(|&i| age[i] != chart::AGE_OLD && (plot[i].is_finite() || kinds[i] == 0))
    else {
        return;
    };
    let mut preceding = (0..changing).rev().filter(|&i| plot[i].is_finite());
    let nearest = preceding.next();
    let dependency = if preceding.next().is_some() {
        nearest
    } else {
        // chart::lerp_at samples adjacent finite pairs for both interpolation and extrapolation. Its first pair supplies the slope for occupied smoother-only holes before either endpoint.
        (0..changing).find(|&i| plot[i].is_finite() || kinds[i] == 0)
    };
    if let Some(i) = dependency {
        // Every possible endpoint before `changing` is OLD by construction. Preserve NEW: it records absence from the held snapshot.
        debug_assert_eq!(age[i], chart::AGE_OLD);
        age[i] = chart::AGE_REACH;
    }
}

/// Prepare chart inputs, smoothing and provenance from fetched rows. The caller supplies verified cutoffs from the cache-lineage proof; those cutoffs drive ages (an absent ref is all NEW). old_only keeps precisely the verified held versions for the audit oracle.
fn prepare(
    req: &proto::ChartRequest,
    p: &ChartParams,
    all_rows: &[&[VersionedRawPoint]],
    x_maps: Option<&std::collections::HashMap<String, std::collections::HashMap<i64, f64>>>,
    held_rows: Option<&VerifiedLineages>,
    old_only: bool,
    smoothing_override: Option<&[(SeriesIdentity, chart::SmoothingPlan)]>,
) -> Result<Preparation, Status> {
    // (step, x_key, value, age) — x_key is the plot key (the step, or
    // timestamp_ms in time mode). The step rides along so range trims and
    // x-metric lookups always work in step space, whatever the chart plots.
    type RawPoint = (i64, i64, f64, u8);
    type TagGroup = (String, Vec<RawPoint>);
    struct RawSeries {
        identity: SeriesIdentity,
        label: String,
        run_id: String,
        points: Vec<RawPoint>,
    }
    let mut raw_series: Vec<RawSeries> = Vec::new();
    for (request_index, (s, rows)) in req.y_series.iter().zip(all_rows).enumerate() {
        let wm = held_rows.and_then(|held| held.cutoff(request_index));
        // Rows are the FULL series from the cache, (tag, step)-ordered, so
        // each tag forms one contiguous group and the scalar group ('') is
        // first if present. Range-trim here (the SQL used to).
        let mut tag_groups: Vec<TagGroup> = Vec::new();
        for row in rows.iter() {
            if row.step < p.fetch_min || row.step > p.fetch_max {
                continue;
            }
            let age = match wm {
                Some(w) if row.inserted_ms <= w => chart::AGE_OLD,
                _ => chart::AGE_NEW,
            };
            if old_only && age == chart::AGE_NEW {
                continue;
            }
            let xk = if p.use_time {
                row.timestamp_ms
            } else {
                row.step
            };
            let pt = (row.step, xk, row.value as f64, age);
            if let Some(last) = tag_groups.last_mut() {
                if last.0 == row.tag {
                    last.1.push(pt);
                    continue;
                }
            }
            tag_groups.push((row.tag.clone(), vec![pt]));
        }

        let has_tagged = tag_groups.last().is_some_and(|(t, _)| !t.is_empty());
        if has_tagged {
            // Tagged rows win over any stray scalar rows for the same
            // metric (the old tagged-probe-first behavior).
            if tag_groups.first().is_some_and(|(t, _)| t.is_empty()) {
                tag_groups.remove(0);
            }
            if !s.tags.is_empty() {
                tag_groups.retain(|(tag, _)| s.tags.contains(tag));
            }
            if tag_groups.len() > 64 {
                return Err(Status::invalid_argument(format!(
                    "Tagged metric '{}' has {} tags, max 64 per chart",
                    s.metric_name,
                    tag_groups.len()
                )));
            }
            let single_run = p.all_same_run || req.y_series.len() == 1;
            let single_metric = p.all_same_metric || req.y_series.len() == 1;
            for (tag, points) in tag_groups {
                let label = if single_run && single_metric {
                    tag.clone()
                } else if single_metric {
                    format!("{}/{}", s.run_id, tag)
                } else if single_run {
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
        } else if let Some((tag, points)) = tag_groups.pop() {
            // Scalar metric (tag = '')
            let label = if p.all_same_metric && !p.all_same_run {
                s.run_id.clone()
            } else if p.all_same_run && !p.all_same_metric {
                s.metric_name.clone()
            } else if p.all_same_metric && p.all_same_run {
                s.run_id.clone()
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
    for (series_index, rs) in raw_series.into_iter().enumerate() {
        let steps_v: Vec<i64> = rs.points.iter().map(|&(s, ..)| s).collect();
        let xkeys_v: Vec<i64> = rs.points.iter().map(|&(_, x, ..)| x).collect();
        let raw_v: Vec<f64> = rs.points.iter().map(|&(_, _, v, ..)| v).collect();
        let mut age_v: Vec<u8> = rs.points.iter().map(|&(_, _, _, a)| a).collect();
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
                ),
                plan,
            )
        } else if has_output_position {
            (raw_v.clone(), chart::SmoothingPlan::NoState)
        } else {
            (Vec::new(), chart::SmoothingPlan::NoState)
        };

        let mut xs = Vec::with_capacity(steps_v.len());
        let mut plot = Vec::with_capacity(xs.capacity());
        let mut raw = Vec::with_capacity(if p.is_smoothed { xs.capacity() } else { 0 });
        let mut kinds = Vec::with_capacity(xs.capacity());
        let mut age = Vec::with_capacity(xs.capacity());
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
                let mut pts: Vec<(f64, f64, f64, u8)> = Vec::new();
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
                // uPlot needs ascending x; an x metric need not be monotonic
                pts.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
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
        });
    }
    Ok(Preparation {
        series: prepared,
        audit_plans_aligned: true,
    })
}

/// The FULL dense model for a prepared set — every chart, delta or not, is computed through here (the audit oracle reconstructs the held one). Every chart — step, timestamp, AND custom-x — is bucketed on ONE slot grid shared by all runs (docs/chart-shared-axis.md); `shared_chart` owns the grid, slot mixture, envelopes, and the marker channel. Y columns are rounded to wire precision HERE (chart_delta::round_y), so the model, the result hashes, and the client's inflated copy are bit-identical — and a smoother's non-finite overshoot is clamped back into the finite range the wire carries.
fn respond(prepared: &[PreparedSeries], p: &ChartParams) -> DenseChart {
    let xs_s: Vec<&[f64]> = prepared.iter().map(|q| q.xs.as_slice()).collect();
    let plot_s: Vec<&[f64]> = prepared.iter().map(|q| q.plot.as_slice()).collect();
    let raw_s: Vec<&[f64]> = prepared.iter().map(|q| q.raw.as_slice()).collect();
    let kinds_s: Vec<&[u8]> = prepared.iter().map(|q| q.kinds.as_slice()).collect();
    let xnan_s: Vec<&[f64]> = prepared.iter().map(|q| q.xnan_xs.as_slice()).collect();
    let mut chart = chart::shared_chart(
        &xs_s,
        &plot_s,
        &raw_s,
        &kinds_s,
        &xnan_s,
        p.is_smoothed,
        p.spec,
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
    chart
}

/// Relative-time offsets require an untrimmed request and timestamps monotone across the held boundary.
/// Verified lineages preserve a held step prefix within each tag, including after eviction recovery; non-relative charts need no additional row scan.
fn relative_offsets_stable(
    req: &proto::ChartRequest,
    p: &ChartParams,
    all_rows: &[&[VersionedRawPoint]],
    held_rows: &VerifiedLineages,
) -> bool {
    if req.step_min.is_some() || req.step_max.is_some() {
        return false;
    }
    for (index, rows) in all_rows.iter().enumerate() {
        let Some(wm) = held_rows.cutoff(index) else {
            continue; // not held: the whole series ships complete
        };
        let mut tag: Option<&str> = None;
        let mut old_max_ts = i64::MIN;
        for row in rows.iter() {
            if row.step < p.fetch_min || row.step > p.fetch_max {
                continue;
            }
            if tag != Some(row.tag.as_str()) {
                tag = Some(row.tag.as_str());
                old_max_ts = i64::MIN;
            }
            if row.inserted_ms > wm {
                if row.timestamp_ms < old_max_ts {
                    return false;
                }
            } else {
                old_max_ts = old_max_ts.max(row.timestamp_ms);
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

/// Shrink the full model to the delta the client needs: continuing series (any held sample) become tails past from_col and name the held series they extend — held order is the request's refs expanded in order, a subsequence both sides share — while all-new series ship complete. Every delta carries content hashes of the full model it reconstructs to (result_x_hash/result_series_hashes): the client hashes its spliced result and refuses a mismatch before rendering — the end-to-end check only the client can run, on every delta.
fn to_delta(
    full: &DenseChart,
    from_col: usize,
    prepared: &[PreparedSeries],
) -> proto::ChartResponse {
    let result_x_hash = chart_delta::hash_axis(full);
    let result_series_hashes: Vec<u64> = full.series.iter().map(chart_delta::hash_series).collect();
    let mut splice_from_cached = Vec::with_capacity(full.series.len());
    let mut next = 0i32;
    let series = full
        .series
        .iter()
        .zip(prepared)
        .map(|(s, ps)| {
            if !ps.continues {
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

/// Everything after the row fetches: build the full response, and shrink it to a frontier delta when the echoed cache state proves the client holds a prefix (anything unprovable answers in full). Pure — the wire tests drive it with fabricated rows; `audit` additionally reconstructs the held response outright and verifies the claimed prefix bit-for-bit ([`prefix_matches`]) — the sampled self-check; wire tests also exercise audit-off responses with independent full-model and hash checks. A failed audit answers in full with `audit_failed` stamped, which the client alerts on. (The end-to-end content check is the client's, on every delta: to_delta's result hashes.)
fn build_response(
    req: &proto::ChartRequest,
    snapshots: &[std::sync::Arc<SeriesSnapshot>],
    x_maps: Option<&std::collections::HashMap<String, std::collections::HashMap<i64, f64>>>,
    audit: bool,
) -> Result<proto::ChartResponse, Status> {
    let p = chart_params(req);
    let cache_frontiers = req
        .cache_state
        .as_ref()
        .map(|cs| &cs.frontiers)
        .filter(|f| !f.is_empty());
    let all_rows: Vec<&[VersionedRawPoint]> = snapshots.iter().map(|rows| &rows[..]).collect();
    let lineages = inspect_lineages(req, snapshots, cache_frontiers);
    let held_rows = lineages
        .verification
        .as_ref()
        .and_then(|result| result.as_ref().ok());
    let mut prepared = prepare(req, &p, &all_rows, x_maps, held_rows, false, None)?.series;
    let mut kind = lineages.rejection().map(lineage::Rejection::label);
    let full = respond(&prepared, &p);
    let resp = if full.x_values.is_empty() {
        // No plottable point anywhere: series ship their unplottable counts over an empty axis, so the panel can say why it has nothing to draw, and nothing is stamped to continue from.
        emit_full(&full)
    } else {
        let continuing = prepared.iter().filter(|ps| ps.continues).count();
        // Verified lineages and shared-ref order prove that continuing groups were held. The client's output-series count still detects removed refs/tag groups and held groups that no longer continue, such as a margin-only group gaining in-range samples. Positional splicing requires that every held series is paired.
        let membership_matches = req
            .cache_state
            .as_ref()
            .and_then(|cs| cs.held_series)
            .is_some_and(|held| held as usize == continuing);
        // Appends can change a global smoothing plan even with verified held inputs; the audit reuses the exact matched plan.
        let deltable = held_rows
            .is_some_and(|f| !p.relative || relative_offsets_stable(req, &p, &all_rows, f))
            && membership_matches
            && smoothing_state_matches(req, &p, &prepared, continuing);
        let mut resp = if deltable {
            let from_col = plan_delta_from_col(&mut prepared, &p);
            // from_col ≤ the new axis by construction; the bound turns a planner bug into a full answer, never a panic in to_delta's slice.
            let mut audit_bad = false;
            let verified = from_col > 0
                && from_col <= full.x_values.len()
                && (!audit || {
                    // The verified held reconstruction must not derive spacing because the exact gate proved the held/current plans equal. Pair each override with stable request/tag identity: `held_series` is client-controlled, so its count alone cannot prove that old-only and continuing groups align.
                    let audit_plans: Vec<(SeriesIdentity, chart::SmoothingPlan)> = prepared
                        .iter()
                        .filter(|ps| ps.continues)
                        .map(|ps| (ps.identity.clone(), ps.smoothing_plan))
                        .collect();
                    let old = prepare(
                        req,
                        &p,
                        &all_rows,
                        x_maps,
                        held_rows,
                        true,
                        p.exact_smoothing_state.then_some(audit_plans.as_slice()),
                    )?;
                    if !old.audit_plans_aligned {
                        kind = Some("gated");
                        false
                    } else {
                        let ok =
                            prefix_matches(&respond(&old.series, &p), &full, from_col, &prepared);
                        if !ok {
                            audit_bad = true;
                            tracing::error!(
                                from_col,
                                "chart delta audit failed; answering in full"
                            );
                            kind = Some("audit_failed");
                        }
                        ok
                    }
                });
            if verified {
                let unchanged =
                    from_col == full.x_values.len() && prepared.iter().all(|ps| ps.continues);
                kind = Some(if unchanged { "unchanged" } else { "delta" });
                to_delta(&full, from_col, &prepared)
            } else {
                if from_col == 0 {
                    kind = Some("miss");
                }
                proto::ChartResponse {
                    audit_failed: audit_bad,
                    ..emit_full(&full)
                }
            }
        } else {
            if cache_frontiers.is_some() {
                kind.get_or_insert("gated");
            }
            emit_full(&full)
        };
        stamp_lineages(&mut resp.frontiers, &lineages);
        stamp_smoothing_state(&mut resp.frontiers, &p, &prepared);
        resp
    };
    if let Some(kind) = kind {
        metrics::counter!("mkdb2_chart_delta_total", "kind" => kind).increment(1);
    }
    Ok(resp)
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
    refs: Vec<proto::SeriesRef>,
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
        .map(|r| {
            let key = (
                r.project_id.clone(),
                r.run_id.clone(),
                r.metric_name.clone(),
            );
            // `get` + clone, not `remove`: duplicate refs must each receive the full entry list.
            let entries = by_key.get(&key).cloned().unwrap_or_default();
            proto::CdnSeries {
                project_id: r.project_id,
                run_id: r.run_id,
                metric_name: r.metric_name,
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
/// Transitions bump `projects.version` first, then push the returned value (pushed versions are real versions). A run APPEARING in the candidate set also counts as a transition: a revival re-enters the window with no lifecycle RPC (for a new run this occasionally doubles InitRun's bump — one redundant ListRuns). The first scan seeds silently: a restart already forces reconnect resyncs.
pub fn spawn_status_watcher(pg: Arc<PgStore>, events: crate::events::EventSender) {
    tokio::spawn(async move {
        let mut prev: std::collections::HashMap<(String, String), proto::RunStatus> =
            std::collections::HashMap::new();
        let mut seeded = false;
        // A failed tick (scan or bump) may have lost transitions — including runs that aged out of the window during the gap. The next healthy tick heals by bumping every project seen on either side of the gap once.
        let mut degraded = false;
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let timed_rows = match pg.status_watch_candidates().await {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::warn!("status watcher scan failed: {e}");
                    degraded = true;
                    continue;
                }
            };
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
                if prev.get(&(row.project_id.clone(), row.run_id.clone())) != Some(&status) {
                    changed.insert(row.project_id.clone());
                }
                cur.insert((row.project_id, row.run_id), status);
            }
            if !seeded {
                seeded = true;
                degraded = false;
                prev = cur;
                continue;
            }
            if degraded {
                changed.extend(prev.keys().chain(cur.keys()).map(|(p, _)| p.clone()));
            }
            if changed.is_empty() {
                // Dropping runs that left the window loses nothing on a healthy tick: they transitioned to their pinned state while still inside it (the 1-minute margin covers a missed tick). prev advances only when announcements succeeded, so failed ticks re-detect. (Empty while degraded = no watched runs on either side of the gap.)
                prev = cur;
                degraded = false;
                continue;
            }
            let ids: Vec<String> = changed.into_iter().collect();
            match pg.bump_project_versions(&ids).await {
                Ok(projects) => {
                    let _ = events.send(crate::events::VersionEvent {
                        projects,
                        ..Default::default()
                    });
                    prev = cur;
                    degraded = false;
                }
                Err(e) => {
                    tracing::warn!("status watcher project bump failed: {e}");
                    degraded = true;
                }
            }
        }
    });
}

#[cfg(test)]
mod frontier_delta_tests {
    use super::*;
    use lineage::{lineage_id, parse_lineages, LINEAGE_PREFIX, LINEAGE_VERSION_KEY};

    use crate::series_cache::LineageOrigin;
    use lineage::Rejection;
    use std::sync::Arc;

    mod bench;
    mod lineage_tests;
    mod marker_delta_tests;
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
        let mut continued = request.clone();
        continued.cache_state = echo(&held);
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

    /// What the client does: echo the held response's frontiers and series count (chart_sync::echo_state).
    fn echo(held: &proto::ChartResponse) -> Option<proto::ChartCacheState> {
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

    /// Bit-level model equality.
    fn eq(a: &DenseChart, b: &DenseChart) -> bool {
        let bits = |v: &[f64]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
        bits(&a.x_values) == bits(&b.x_values)
            && bits(&a.xr_min) == bits(&b.xr_min)
            && bits(&a.xr_max) == bits(&b.xr_max)
            && a.series.len() == b.series.len()
            && a.series.iter().zip(&b.series).all(|(s, t)| {
                s.label == t.label
                    && s.run_id == t.run_id
                    && bits(&s.values) == bits(&t.values)
                    && bits(&s.raw_values) == bits(&t.raw_values)
                    && bits(&s.min_values) == bits(&t.min_values)
                    && bits(&s.max_values) == bits(&t.max_values)
                    && s.nan_indices == t.nan_indices
                    && s.nan_kinds == t.nan_kinds
                    && s.xnan_count == t.xnan_count
            })
    }

    /// Verify the actual client splice and its result hashes even when the server's sampled reconstruction audit is disabled.
    fn checked_delta(
        request: &proto::ChartRequest,
        held: &proto::ChartResponse,
        current: &[Arc<Vec<VersionedRawPoint>>],
        audit: bool,
    ) -> FixtureResponse {
        let mut continued = request.clone();
        continued.cache_state = echo(held);
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
        let mut continued = request.clone();
        continued.cache_state = echo(held);
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
                        Some(old) => {
                            let next = old.refreshed_fixture(rows);
                            if next.lineage() == old.lineage() && next.maximum() == old.maximum() {
                                old
                            } else {
                                Arc::new(next)
                            }
                        }
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
        let mut snapshots = fixture_snapshots(req, rows);
        let response = super::build_response(req, &snapshots, x_maps, audit)?;
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
        let mut r2 = r.clone();
        r2.cache_state = echo(&held);
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

        let mut continued = r.clone();
        continued.cache_state = echo(&held);
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
        let mut r2 = r.clone();
        r2.cache_state = echo(&held);
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

            let mut continued = r.clone();
            continued.cache_state = echo(&held);
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
        let mut continued = r.clone();
        continued.cache_state = echo(&held);
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
            r.smoothing = (algorithm != Algorithm::None).then_some(proto::SmoothingConfig {
                algorithm: algorithm as i32,
                window_size: 10,
                time_constant: std::f64::consts::LOG2_E,
                poly_order: 1,
            });
            let held = build(&r, &[held_a.clone(), b.clone()]);
            let held_model = inflate_full(&held);
            assert!(held_model.series[0].nan_indices.is_empty());

            let mut continued = r.clone();
            continued.cache_state = echo(&held);
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

        let mut continued = r.clone();
        continued.cache_state = echo(&held);
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
        let mut continued = r.clone();
        continued.cache_state = echo(&held);
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
        let mut r2 = r.clone();
        r2.cache_state = echo(&held);
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
        let mut continued = request.clone();
        continued.cache_state = echo(&held);
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
        let mut r2 = r.clone();
        r2.cache_state = echo(&held);
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
            r.smoothing = (algorithm != Algorithm::None).then_some(proto::SmoothingConfig {
                algorithm: algorithm as i32,
                window_size: 9,
                time_constant: std::f64::consts::LOG2_E,
                poly_order: 1,
            });
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
            let mut continued = step_req.clone();
            continued.cache_state = echo(&held);
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
            let mut continued = time_req.clone();
            continued.cache_state = echo(&held);
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
        let mut r2 = r.clone();
        r2.cache_state = echo(&held);
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
        let mut r2 = r.clone();
        r2.cache_state = echo(&held);
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
            let mut continued = r.clone();
            continued.cache_state = echo(&held);
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
            let mut r2 = r.clone();
            r2.cache_state = echo(&held);
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
            let mut continued = r.clone();
            continued.cache_state = echo(&held);
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
        let mut r2 = r.clone();
        r2.cache_state = echo(&held);
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
        let mut r2 = r.clone();
        r2.cache_state = echo(&held);
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
        let mut r2 = r.clone();
        r2.cache_state = echo(&held);
        let out = counted((1, 0), || build(&r2, &grown));
        assert!(out.delta, "a margin-only series must not cost the delta");
        assert_eq!(out.splice_from_cached, vec![0, 1]);
        assert!(eq(
            &splice(&inflate_full(&held), &out),
            &inflate_full(&build(&r, &grown))
        ));

        // A margin-only run the held response never saw (no echoed frontier, nothing provably held) ships COMPLETE — all-NaN over the full axis — inside a normal delta.
        let held_a = build(&zoomed(&["a"]), &[rows(900, 3)]);
        let mut r3 = r.clone();
        r3.cache_state = echo(&held_a);
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
        let mut r2 = r.clone();
        r2.cache_state = echo(&held);
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
        let mut r2 = r.clone();
        r2.cache_state = echo(&held);
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
        let mut r2 = r.clone();
        r2.cache_state = echo(&held);
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
        let mut r2 = r.clone();
        r2.cache_state = echo(&held);
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
        let mut state = 0x9E3779B97F4A7C15u64;
        let mut rng = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
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
            let nruns = 1 + (rng() % 3) as usize;
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
            let seeds: Vec<u64> = (0..nruns).map(|_| rng()).collect();
            let lens: Vec<i64> = (0..nruns).map(|_| 200 + (rng() % 2200) as i64).collect();
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
            let mut r2 = r.clone();
            r2.cache_state = echo(&held);
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
    // Rolls the 1-in-67 sampled delta audit (see build_response). Prime, so the round-robin can't phase-lock with a dashboard's periodic panel count and audit the same panels every cycle while never touching others.
    delta_audit: std::sync::atomic::AtomicU64,
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
        tracing::info!(chart_limit, "chart read admission configured");
        Self {
            ch,
            pg,
            bumps,
            gates,
            chart_admission: ChartAdmission::new(chart_limit),
            notifier,
            events,
            delta_audit: std::sync::atomic::AtomicU64::new(0),
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

    /// Read guards for `keys`, plus the data versions `ensure_runs_readable` read for them, by run_id: read before any of a reply's data, they are its echo (docs/first-mount-refresh.md).
    async fn readable_guards(
        &self,
        keys: &[RunKey],
    ) -> Result<
        (
            Vec<tokio::sync::OwnedRwLockReadGuard<()>>,
            std::collections::HashMap<String, u64>,
        ),
        Status,
    > {
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

        // Evict the run's cache entries BEFORE the terminal commit exposes the
        // new version: entries fetched mid-import may be missing rows buried
        // below their watermark and only a full rebuild recovers them. The
        // write gate makes this terminal — every store path holds the run's
        // read gate across fetch AND store (read_many_keyed / RefreshDetach in
        // query_chart_admitted), so nothing can re-install a pre-eviction
        // snapshot; same gate-claim-then-purge shape as the deletion reaper.
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

    #[instrument(skip(self))]
    pub async fn trash_runs(
        &self,
        request: Request<proto::TrashRunsRequest>,
    ) -> Result<Response<proto::TrashRunsResponse>, Status> {
        const INTERNAL_CHUNK_SIZE: usize = 256;

        let req = request.into_inner();
        if req.project_id.is_empty() {
            return Err(Status::invalid_argument("project_id is required"));
        }
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
                    // The watchdog reconciles Trash as absence from its
                    // authoritative ListRuns poll. Per-run hints here would let
                    // one uncapped bulk request crowd Started/Terminated/Restored
                    // events out of the shared bounded notifier queue.
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

    #[instrument(skip(self))]
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

    #[instrument(skip(self))]
    pub async fn list_run_set_metrics(
        &self,
        request: Request<proto::ListRunSetMetricsRequest>,
    ) -> Result<Response<proto::ListMetricsResponse>, Status> {
        let req = request.into_inner();
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

    #[instrument(skip(self))]
    pub async fn query_chart(
        &self,
        request: Request<proto::ChartRequest>,
    ) -> Result<Response<proto::ChartResponse>, Status> {
        let mut req = request.into_inner();
        canonicalize_chart_request(&mut req)?;
        if req.y_series.is_empty() {
            return Ok(Response::new(proto::ChartResponse::default()));
        }
        let requested_weight = chart_admission_weight(&req);
        let response = self
            .chart_admission
            .run_with_timeout(requested_weight, CHART_QUERY_TIMEOUT, |permit| {
                self.query_chart_admitted(&req, permit)
            })
            .await?;
        Ok(Response::new(response))
    }

    async fn query_chart_admitted(
        &self,
        req: &proto::ChartRequest,
        permit: ChartAdmissionPermit,
    ) -> Result<proto::ChartResponse, Status> {
        let series_keys: Vec<_> = req
            .y_series
            .iter()
            .map(|s| SeriesKey::new(&s.project_id, &s.run_id, &s.metric_name))
            .collect();
        let runs: Vec<_> = series_keys
            .iter()
            .map(|key| RunKey::new(&key.project_id, &key.run_id))
            .collect();
        // Keyed so each detached refresh co-owns exactly its runs' guards — a purge of run A waits for the scans reading A (a batched scan reads several runs), never for run B's own.
        let guards = self.gates.read_many_keyed(runs.iter().cloned()).await;
        // Read before any series data, so each version is a lower bound on what this answer holds: an insert commits, is noted in the series cache, and only then is counted by a version bump; cached rows serve only if fetched after the run's last note (series_cache.rs is_fresh). A version read after the data could count rows the answer lacks.
        let run_versions = self
            .pg
            .ensure_runs_readable(&runs)
            .await
            .map_err(lifecycle_access_status)?;

        use futures::stream::{self, StreamExt, TryStreamExt};
        let fetch_concurrency = self.chart_admission.fetch_concurrency();
        // Split lazily at election time: a unit leaves the request only when a detached refresh actually spawns, so cache-hit series cost nothing and protection lands on the series that scan.
        let detach_for = |key: &SeriesKey| {
            permit.split_refresh_unit().map(|unit| RefreshDetach {
                _permit: unit,
                _run_guard: Arc::clone(&guards[&RunKey::new(&key.project_id, &key.run_id)]),
            })
        };

        // Distinct series grouped by (project, metric); a group's cache misses share one full read (clickhouse.rs fetch_full_many).
        // Election and unit splits run in this synchronous pass, before any per-series fetch below can split units or take slots; each batch is backed by one unit per series and spawns its detached tasks here.
        // A group of one distinct series keeps its own read (a larger group still elects a one-series batch when its other series are cached or busy), and batches take at most half of the request's `fetch_concurrency` reads, leaving the rest to the per-series stream below.
        let mut groups = series_groups(&series_keys);
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
            batches.push(self.ch.spawn_full_read_batch(elected, ctx));
        }
        let per_series_concurrency = fetch_concurrency - batches.len();
        // The first failed batch fails the request at once, as a failed per-series read does; the other batches' tasks finish detached.
        let batched_rows = futures::future::try_join_all(batches);

        // Every other series: its own cached read (fresh, attached, incremental, or a full read of its own), concurrently, sharing the request's read bound with the batches.
        let per_series_keys: Vec<_> = groups.iter().flatten().collect();
        let fetches: Vec<_> = per_series_keys
            .iter()
            .map(|&key| {
                self.ch.query_raw_any_cached(
                    &key.project_id,
                    &key.run_id,
                    &key.metric_name,
                    move || detach_for(key),
                )
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
            .chain(per_series_keys.into_iter().zip(&per_series))
            .collect();
        let all_rows: Vec<_> = series_keys
            .iter()
            .map(|key| Arc::clone(rows[key]))
            .collect();

        // Custom x-axis: map each run's points through THAT run's x metric.
        // The request's x_series names the metric; its run_id is ignored
        // (plotting every run against the first run's x values pairs y
        // samples with x samples from a different run). Ignored in
        // timestamp mode, where point keys are timestamps, not steps.
        let x_maps = match custom_x_runs(req) {
            Some((x_ref, run_refs)) => {
                let step_min = req.step_min.unwrap_or(i64::MIN);
                let step_max = req.step_max.unwrap_or(i64::MAX);
                // One read per project covers all of its runs.
                let mut projects: Vec<(&str, Vec<String>)> = Vec::new();
                for (pid, rid) in run_refs {
                    match projects.iter_mut().find(|(project, _)| *project == pid) {
                        Some((_, runs)) => runs.push(rid.to_string()),
                        None => projects.push((pid, vec![rid.to_string()])),
                    }
                }
                let x_fetches: Vec<_> = projects
                    .iter()
                    .map(|(pid, runs)| {
                        self.ch
                            .query_raw_many(pid, runs, &x_ref.metric_name, step_min, step_max)
                    })
                    .collect();
                let fetched: Vec<_> = stream::iter(x_fetches)
                    .buffered(fetch_concurrency)
                    .try_collect()
                    .await
                    .map_err(|e| Status::internal(format!("x-axis query failed: {e}")))?;
                let mut maps = std::collections::HashMap::new();
                for ((_, runs), series) in projects.into_iter().zip(fetched) {
                    for (rid, rows) in runs.into_iter().zip(series) {
                        maps.insert(rid, rows.into_iter().collect());
                    }
                }
                Some(maps)
            }
            _ => None,
        };

        // 1 in 67 chart queries audits its answer — when it is a delta — against a brute-force reconstruction of the held response (build_response).
        let audit = self
            .delta_audit
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .is_multiple_of(67);
        let mut response = build_response(req, &all_rows, x_maps.as_ref(), audit)?;
        response.run_versions = run_versions;
        Ok(response)
    }

    // --- CDN queries ---

    #[instrument(skip(self))]
    pub async fn query_cdn_keys(
        &self,
        request: Request<proto::QueryCdnKeysRequest>,
    ) -> Result<Response<proto::QueryCdnKeysResponse>, Status> {
        let req = request.into_inner();

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
            .iter()
            .map(|r| {
                (
                    r.project_id.clone(),
                    r.run_id.clone(),
                    r.metric_name.clone(),
                )
            })
            .collect();

        let rows = self
            .ch
            .query_cdn_keys_batch(&key_refs, step_min, step_max)
            .await
            .map_err(|e| Status::internal(format!("CDN query failed: {e}")))?;

        // Emit one CdnSeries per requested ref, in request order. Refs with
        // no rows still get an empty series so callers can rely on positional
        // 1:1 correspondence with their request.
        let series = assemble_cdn_series(req.refs, rows);

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
mod cdn_series_tests {
    use super::*;

    fn reference(run_id: &str, tags: &[&str]) -> proto::SeriesRef {
        proto::SeriesRef {
            project_id: "project".to_string(),
            run_id: run_id.to_string(),
            metric_name: "gallery".to_string(),
            tags: tags.iter().map(|tag| (*tag).to_string()).collect(),
        }
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
                reference("a", &["ignored-a"]),
                reference("missing", &[]),
                reference("a", &["ignored-b"]),
                reference("b", &[]),
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
    use super::*;

    fn series(run_id: &str) -> proto::SeriesRef {
        proto::SeriesRef {
            project_id: "project".to_string(),
            run_id: run_id.to_string(),
            metric_name: "loss".to_string(),
            tags: Vec::new(),
        }
    }

    #[test]
    fn series_groups_dedupe_and_keep_first_appearance_order() {
        let key = |project, run, metric| SeriesKey::new(project, run, metric);
        assert_eq!(
            series_groups(&[
                key("p", "a", "loss"),
                key("p", "b", "acc"),
                key("q", "a", "loss"),
                key("p", "b", "loss"),
                key("p", "a", "loss"),
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
        request.y_series.clear();
        assert_eq!(chart_admission_weight(&request), 1);
    }

    #[test]
    fn chart_fetch_concurrency_respects_small_admission_limits() {
        let small = ChartAdmission::new(2);
        assert_eq!(small.fetch_concurrency(), 2);

        let large = ChartAdmission::new(64);
        assert_eq!(large.fetch_concurrency(), MAX_CHART_FETCH_CONCURRENCY);
    }

    #[tokio::test]
    async fn narrow_request_splits_down_to_its_last_unit() {
        let admission = ChartAdmission::new(32);
        let permit = admission.acquire(3).await.unwrap();
        let units: Vec<_> = std::iter::from_fn(|| permit.split_refresh_unit())
            .take(8)
            .collect();
        assert_eq!(units.len(), 3);
    }

    #[tokio::test]
    async fn clamped_request_stops_splitting_at_the_inline_floor() {
        let limit = MAX_CHART_FETCH_CONCURRENCY * 2;
        let admission = ChartAdmission::new(limit);
        let permit = admission
            .acquire(MAX_CHART_REQUEST_RAW_SERIES)
            .await
            .unwrap();
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
        let admission = ChartAdmission::new(1);
        let permit = admission.acquire(2).await.unwrap();
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
        let admission = Arc::new(ChartAdmission::new(4));
        let held = admission.acquire(3).await.unwrap();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel();
        let waiter_admission = admission.clone();
        let waiter = tokio::spawn(async move {
            ready_tx.send(()).unwrap();
            let permit = waiter_admission.acquire(2).await.unwrap();
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
        let admission = Arc::new(ChartAdmission::new(DEFAULT_CHART_INFLIGHT_SERIES));
        // Three bindings at the editor's default 12 runs require 36 reads.
        let wide = admission.acquire(36).await.unwrap();
        assert_eq!(admission.slots.available_permits(), 0);

        let waiter_admission = admission.clone();
        let mut waiter = tokio::spawn(async move { waiter_admission.acquire(1).await.unwrap() });
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
        let admission = ChartAdmission::new(2);
        let _largest = admission
            .acquire(MAX_CHART_REQUEST_RAW_SERIES)
            .await
            .expect("lower concurrency must not shrink the request shape limit");
        drop(_largest);
        let error = admission
            .run_with_timeout(
                MAX_CHART_REQUEST_RAW_SERIES + 1,
                Duration::from_secs(1),
                |_permit| std::future::ready(Ok::<_, Status>(())),
            )
            .await
            .expect_err("a request cannot exceed the stable shape bound");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("maximum per chart is 128"));
        assert_eq!(admission.slots.available_permits(), 2);
    }

    #[tokio::test]
    async fn chart_deadline_cancels_work_and_releases_its_full_weight() {
        let admission = ChartAdmission::new(2);
        let error = admission
            .run_with_timeout(2, Duration::from_millis(20), |_permit| {
                std::future::pending::<Result<(), Status>>()
            })
            .await
            .expect_err("pending work must hit the server deadline");
        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        assert_eq!(admission.slots.available_permits(), 2);
        let _next = tokio::time::timeout(Duration::from_secs(1), admission.acquire(2))
            .await
            .expect("the deadline must un-wedge the gate")
            .unwrap();
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
        let mut state = 0x243F6A8885A308D3u64;
        let mut rng = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for case in 0..60u32 {
            let use_time = case % 3 == 1;
            let log = case % 2 == 0;
            let smoothed = case % 5 < 2;
            let target = [50u32, 300, 1000][(case as usize / 3) % 3];
            let nruns = 1 + (rng() % 4) as usize;
            // Some cases start past step 0: the log ladder must take its plain-log arm (the +1 shift engages only when the chart contains zero).
            let start = if case % 5 == 4 { 7 } else { 0 };
            let rows: Vec<Arc<Vec<crate::clickhouse::VersionedRawPoint>>> = (0..nruns)
                .map(|_| {
                    let n = 30 + (rng() % 2500) as i64;
                    let neg = (rng() % 4 == 0) as i64 * 3; // sometimes a few negative steps
                    Arc::new(
                        (0..n)
                            .map(|s| {
                                let r = rng();
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
                                        - ((r % 7 == 0) as i64) * 10,
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
                &rows.iter().map(|rows| rows.as_slice()).collect::<Vec<_>>(),
                None,
                None,
                false,
                None,
            )
            .unwrap()
            .series;
            let model = respond(&prepared, &p);
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
