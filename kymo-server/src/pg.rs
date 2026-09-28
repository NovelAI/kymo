//! Postgres-backed metadata store: projects, runs, and three-level version
//! counters (global / project / run) used by the frontend for change detection.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Result;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::FromRow;

use crate::lifecycle::RunKey;
use crate::liveness::STATUS_WATCH_WINDOW;

/// Standard run-metric registry rows per Postgres UNNEST statement. Shared by
/// ordinary write-behind and boot reconciliation.
pub(crate) const RUN_METRICS_BATCH_ROWS: usize = 10_000;
pub const TRASH_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
const MAX_PRE_TERMINAL_RUN_VERSION: i64 = i64::MAX - 1;
const RESTORE_VERSION_INCREMENT: i64 = 2;
const MAX_RESTORABLE_RUN_VERSION: i64 = i64::MAX - RESTORE_VERSION_INCREMENT;

/// Serializes opt-in tests against the shared live databases, including global ownership-table changes and the ClickHouse insert barrier.
#[cfg(test)]
pub(crate) fn live_database_suite_gate() -> &'static tokio::sync::Mutex<()> {
    static GATE: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    GATE.get_or_init(|| tokio::sync::Mutex::new(()))
}

// Keep the canonical projections in one place. These queries use the runtime
// sqlx API, so small concat! wrappers retain static SQL without allocating a
// formatted String at each call site.
macro_rules! run_info_sql {
    ($prefix:literal, $suffix:literal) => {
        concat!(
            $prefix,
            " project_id, run_id, run_name, ordinal,\n",
            "(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_at_ms,\n",
            "(EXTRACT(EPOCH FROM last_main_metric_at) * 1000)::BIGINT AS last_main_metric_at_ms,\n",
            "(EXTRACT(EPOCH FROM last_system_metric_at) * 1000)::BIGINT AS last_system_metric_at_ms,\n",
            "(EXTRACT(EPOCH FROM last_ingested_at) * 1000)::BIGINT AS last_ingested_at_ms,\n",
            "(EXTRACT(EPOCH FROM terminated_at) * 1000)::BIGINT AS terminated_at_ms,\n",
            "exit_code",
            $suffix
        )
    };
}

macro_rules! run_record_sql {
    ($prefix:literal, $suffix:literal) => {
        concat!(
            $prefix,
            " project_id, run_id, run_name, ordinal,\n",
            "(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_at_ms,\n",
            "(EXTRACT(EPOCH FROM last_main_metric_at) * 1000)::BIGINT AS last_main_metric_at_ms,\n",
            "(EXTRACT(EPOCH FROM last_system_metric_at) * 1000)::BIGINT AS last_system_metric_at_ms,\n",
            "(EXTRACT(EPOCH FROM last_ingested_at) * 1000)::BIGINT AS last_ingested_at_ms,\n",
            "(EXTRACT(EPOCH FROM terminated_at) * 1000)::BIGINT AS terminated_at_ms,\n",
            "exit_code,\n",
            "(EXTRACT(EPOCH FROM deleted_at) * 1000)::BIGINT AS deleted_at_ms,\n",
            "(EXTRACT(EPOCH FROM purging_at) * 1000)::BIGINT AS purging_at_ms",
            $suffix
        )
    };
}

#[derive(Debug, Clone, FromRow)]
pub struct RunInfoRow {
    pub project_id: String,
    pub run_id: String,
    pub run_name: String,
    pub ordinal: i64,
    pub created_at_ms: i64,
    // Liveness inputs (ms since epoch, NULL if no metrics of that kind yet).
    pub last_main_metric_at_ms: Option<i64>,
    pub last_system_metric_at_ms: Option<i64>,
    // Server-observed completion time of the newest successful ClickHouse
    // insert containing a point for this run.
    pub last_ingested_at_ms: Option<i64>,
    pub terminated_at_ms: Option<i64>,
    // Terminal exit signal — Some => CRASHED/FINISHED, None => derive from timestamps.
    pub exit_code: Option<i32>,
}

pub struct TimedRunRows {
    pub rows: Vec<RunInfoRow>,
    /// PostgreSQL wall clock captured with the rows. created_at and the re-init liveness baseline are PostgreSQL-authored, and the status-watch candidate filter compares against PostgreSQL's clock, so classification must use this value rather than the application host's independently skewable clock. (Metric heartbeats are client-authored either way; the liveness windows absorb that skew.)
    pub server_now_ms: i64,
}

pub struct ProjectListing {
    /// `(project_id, last_logged_at_ms)`, ordered by project_id.
    pub projects: Vec<(String, Option<i64>)>,
    pub server_now_ms: i64,
}

pub struct ListRunsSnapshot {
    pub rows: Vec<RunInfoRow>,
    pub server_now_ms: i64,
    /// Version of the same statement snapshot as nonempty rows. Empty lists retain conservative refreshes.
    pub project_version: Option<u64>,
}

#[derive(FromRow)]
struct ListRunsRow {
    #[sqlx(flatten)]
    row: RunInfoRow,
    project_version: i64,
    server_now_ms: i64,
}

#[derive(FromRow)]
struct TimedRunInfoRow {
    #[sqlx(flatten)]
    row: RunInfoRow,
    server_now_ms: i64,
}

fn split_timed_run_rows(rows: Vec<TimedRunInfoRow>) -> TimedRunRows {
    let server_now_ms = rows
        .first()
        .map(|row| row.server_now_ms)
        .unwrap_or_default();
    TimedRunRows {
        rows: rows.into_iter().map(|row| row.row).collect(),
        server_now_ms,
    }
}

async fn transaction_clock_ms(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> std::result::Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT")
        .fetch_one(&mut **tx)
        .await
}

pub struct RenameRunOutcome {
    pub row: RunInfoRow,
    pub server_now_ms: i64,
    pub bumped_project: Option<u64>,
}

pub struct TerminateRunOutcome {
    pub run_name: String,
    pub bumped_run: u64,
    pub bumped_project: u64,
}

/// Canonical metadata for a run, including its reversible-deletion fields.
#[derive(Debug, Clone, FromRow)]
pub struct RunRecordRow {
    pub project_id: String,
    pub run_id: String,
    pub run_name: String,
    pub ordinal: i64,
    pub created_at_ms: i64,
    pub last_main_metric_at_ms: Option<i64>,
    pub last_system_metric_at_ms: Option<i64>,
    pub last_ingested_at_ms: Option<i64>,
    pub terminated_at_ms: Option<i64>,
    pub exit_code: Option<i32>,
    pub deleted_at_ms: Option<i64>,
    pub purging_at_ms: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunLifecycleClass {
    Active,
    Trashed,
    Expired,
    Purging,
    Purged,
    Missing,
}

fn classify_lifecycle(
    canonical_exists: bool,
    was_purged: bool,
    deleted_at_ms: Option<i64>,
    purging_at_ms: Option<i64>,
    is_expired: bool,
) -> RunLifecycleClass {
    if purging_at_ms.is_some() {
        RunLifecycleClass::Purging
    } else if deleted_at_ms.is_some() {
        if is_expired {
            RunLifecycleClass::Expired
        } else {
            RunLifecycleClass::Trashed
        }
    } else if canonical_exists {
        RunLifecycleClass::Active
    } else if was_purged {
        RunLifecycleClass::Purged
    } else {
        RunLifecycleClass::Missing
    }
}

impl RunRecordRow {
    pub fn lifecycle_at(&self, now_ms: i64) -> RunLifecycleClass {
        let is_expired = self.deleted_at_ms.is_some_and(|deleted_at_ms| {
            deleted_at_ms.saturating_add(TRASH_RETENTION_MS) <= now_ms
        });
        classify_lifecycle(
            true,
            false,
            self.deleted_at_ms,
            self.purging_at_ms,
            is_expired,
        )
    }
}

#[derive(Debug, Clone)]
pub struct LifecycleSnapshot {
    pub global_version: u64,
    pub server_now_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TrashPageCursor {
    pub purge_at_ms: i64,
    pub project_id: String,
    pub ordinal: i64,
    pub run_id: String,
}

#[derive(Debug, Clone)]
pub enum TrashListQuery {
    Identities {
        project_id: String,
        run_ids: Vec<String>,
    },
    Page {
        page_size: usize,
        after: Option<TrashPageCursor>,
    },
}

pub struct TrashListPage {
    pub rows: Vec<RunRecordRow>,
    pub snapshot: LifecycleSnapshot,
    pub total_count: Option<u64>,
    pub next: Option<TrashPageCursor>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TrashMutationKind {
    Trashed,
    AlreadyTrashed,
    NotFound,
    Expired,
}

#[derive(Debug, Clone)]
pub struct TrashMutationResult {
    pub run_id: String,
    pub kind: TrashMutationKind,
}

pub struct TrashChunkOutcome {
    pub results: Vec<TrashMutationResult>,
    pub bumped_project: Option<u64>,
    pub bumped_global: Option<u64>,
}

fn classify_trash_results(
    run_ids: &[String],
    before_by_id: &HashMap<String, RunRecordRow>,
    purged: &HashSet<String>,
    now_ms: i64,
) -> Vec<TrashMutationResult> {
    let mut newly_trashed = HashSet::new();
    run_ids
        .iter()
        .map(|run_id| match before_by_id.get(run_id) {
            Some(row) => {
                let kind = match row.lifecycle_at(now_ms) {
                    RunLifecycleClass::Active if newly_trashed.insert(run_id) => {
                        TrashMutationKind::Trashed
                    }
                    RunLifecycleClass::Active | RunLifecycleClass::Trashed => {
                        TrashMutationKind::AlreadyTrashed
                    }
                    RunLifecycleClass::Expired | RunLifecycleClass::Purging => {
                        TrashMutationKind::Expired
                    }
                    _ => TrashMutationKind::NotFound,
                };
                TrashMutationResult {
                    run_id: run_id.clone(),
                    kind,
                }
            }
            None if purged.contains(run_id) => TrashMutationResult {
                run_id: run_id.clone(),
                kind: TrashMutationKind::Expired,
            },
            None => TrashMutationResult {
                run_id: run_id.clone(),
                kind: TrashMutationKind::NotFound,
            },
        })
        .collect()
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RestoreMutationKind {
    Restored,
    AlreadyActive,
    NotFound,
    Expired,
}

pub struct RestoreMutationOutcome {
    pub kind: RestoreMutationKind,
    pub row: Option<RunRecordRow>,
    pub server_now_ms: i64,
    /// New runs.version, present only for a successful restore.
    pub bumped_run: Option<u64>,
    pub bumped_project: Option<u64>,
    pub bumped_global: Option<u64>,
}

pub struct PurgeClaimOutcome {
    pub rows: Vec<RunKey>,
    /// Effective terminal versions for every returned row, including claims resumed after a reaper restart.
    pub terminal_versions: Vec<(String, u64)>,
    pub bumped_global: Option<u64>,
}

#[derive(Debug)]
pub enum RunAccessError {
    Store(anyhow::Error),
    NotActive {
        key: RunKey,
        state: RunLifecycleClass,
    },
    NotReadable {
        key: RunKey,
        state: RunLifecycleClass,
    },
}

impl std::fmt::Display for RunAccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(error) => write!(f, "lifecycle lookup failed: {error}"),
            Self::NotActive { key, state } => write!(
                f,
                "run {}/{} is not active ({state:?})",
                key.project_id, key.run_id
            ),
            Self::NotReadable { key, state } => write!(
                f,
                "run {}/{} is not readable ({state:?})",
                key.project_id, key.run_id
            ),
        }
    }
}

impl std::error::Error for RunAccessError {}

/// Post-insert run metadata retained by the write-behind coalescer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TouchedRun {
    pub project_id: String,
    pub run_id: String,
    pub max_main_metric_at_ms: Option<i64>,
    pub max_system_metric_at_ms: Option<i64>,
    /// Server wall clock captured after ClickHouse durably accepted the newest
    /// coalesced flush. Retaining it through Postgres retries keeps an outage
    /// from turning recovery time into ingest time.
    pub last_ingested_at_ms: i64,
}

/// Versions committed by one coalesced ingest-bookkeeping pass.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BumpRunVersionsOutcome {
    pub runs: Vec<(String, u64)>,
    /// Finished/crashed runs can receive delayed uploads. Their project
    /// versions also advance so ListRuns refreshes the server timing shown by
    /// the dashboard.
    pub projects: Vec<(String, u64)>,
}

/// What one InitRun call did to the version counters, so the caller can
/// publish exactly those bumps on the push bus (see events.rs).
pub struct InitRunOutcome {
    pub row: RunInfoRow,
    /// Ordering namespace allocated for this successful InitRun call.
    pub writer_epoch: u32,
    pub server_now_ms: i64,
    /// New global_seq version — Some only when this run made its project listed (see init_run).
    pub bumped_global: Option<u64>,
    /// New projects.version — bumped on every InitRun (see the body).
    pub bumped_project: u64,
    /// New runs.version — Some only on re-init (the bump clearing terminal state); a fresh run is discovered via the project bump instead.
    pub bumped_run: Option<u64>,
}

#[derive(Debug)]
pub enum InitRunError {
    Store(sqlx::Error),
    WriterEpochExhausted(RunKey),
    RunIdOwned {
        run_id: String,
        requested_project_id: String,
    },
    NotInitializable {
        key: RunKey,
        state: RunLifecycleClass,
    },
}

impl std::fmt::Display for InitRunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(error) => write!(f, "run initialization failed: {error}"),
            Self::WriterEpochExhausted(key) => write!(
                f,
                "run {}/{} exhausted its rich writer epochs",
                key.project_id, key.run_id
            ),
            Self::RunIdOwned {
                run_id,
                requested_project_id,
            } => write!(
                f,
                "run id {run_id:?} already belongs to another project and cannot be initialized in {requested_project_id:?}"
            ),
            Self::NotInitializable { key, state } => write!(
                f,
                "run {}/{} cannot be initialized ({state:?})",
                key.project_id, key.run_id
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RichMutationDecision {
    Accepted,
    Idempotent,
    Superseded { stored_version: u64 },
    Conflict { stored_resource_id: String },
    UnallocatedEpoch { current_epoch: u32 },
}

pub struct RichMutationCandidate<'a> {
    pub project_id: &'a str,
    pub run_id: &'a str,
    pub metric_name: &'a str,
    pub tag: &'a str,
    pub step: i64,
    pub mutation_version: u64,
    pub public_resource_id: &'a str,
}

impl std::error::Error for InitRunError {}

impl From<sqlx::Error> for InitRunError {
    fn from(error: sqlx::Error) -> Self {
        Self::Store(error)
    }
}

#[derive(Clone)]
pub struct PgStore {
    pool: PgPool,
}

impl PgStore {
    #[cfg(test)]
    pub(crate) fn test_pool(&self) -> &PgPool {
        &self.pool
    }

    #[cfg(test)]
    pub(crate) fn test_store() -> Self {
        Self {
            pool: PgPoolOptions::new()
                .connect_lazy("postgres://localhost/mkdb2_test")
                .expect("valid test Postgres URL"),
        }
    }

    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .acquire_timeout(Duration::from_secs(10))
            .connect(url)
            .await?;
        let store = Self { pool };
        store.ensure_schema().await?;
        Ok(store)
    }

    async fn ensure_schema(&self) -> Result<()> {
        // Single-row table for the global version counter. CHECK constraint
        // pins the primary key to 1 so there can only ever be one row.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS global_seq (
                id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                version BIGINT NOT NULL DEFAULT 0
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query("INSERT INTO global_seq (id, version) VALUES (1, 0) ON CONFLICT DO NOTHING")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS projects (
                project_id TEXT PRIMARY KEY,
                version    BIGINT NOT NULL DEFAULT 0,
                next_run_ordinal BIGINT NOT NULL DEFAULT 1,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "ALTER TABLE projects
             ADD COLUMN IF NOT EXISTS next_run_ordinal BIGINT NOT NULL DEFAULT 1",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS runs (
                project_id TEXT NOT NULL REFERENCES projects(project_id),
                run_id     TEXT NOT NULL,
                run_name   TEXT NOT NULL,
                ordinal    BIGINT NOT NULL,
                version    BIGINT NOT NULL DEFAULT 0,
                rich_writer_epoch BIGINT NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (project_id, run_id),
                UNIQUE (project_id, ordinal)
            )",
        )
        .execute(&self.pool)
        .await?;

        // Run liveness columns. Added incrementally so existing tables get them
        // on next boot without a separate migration step.
        for stmt in [
            "ALTER TABLE runs ADD COLUMN IF NOT EXISTS last_main_metric_at TIMESTAMPTZ",
            "ALTER TABLE runs ADD COLUMN IF NOT EXISTS last_system_metric_at TIMESTAMPTZ",
            "ALTER TABLE runs ADD COLUMN IF NOT EXISTS last_ingested_at TIMESTAMPTZ",
            "ALTER TABLE runs ADD COLUMN IF NOT EXISTS exit_code INTEGER",
            "ALTER TABLE runs ADD COLUMN IF NOT EXISTS terminated_at TIMESTAMPTZ",
            "ALTER TABLE runs ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ",
            "ALTER TABLE runs ADD COLUMN IF NOT EXISTS purging_at TIMESTAMPTZ",
            "ALTER TABLE runs ADD COLUMN IF NOT EXISTS purge_attempt_at TIMESTAMPTZ",
            "ALTER TABLE runs ADD COLUMN IF NOT EXISTS rich_writer_epoch BIGINT NOT NULL DEFAULT 0",
        ] {
            sqlx::query(stmt).execute(&self.pool).await?;
        }
        sqlx::query(
            "DO $$ BEGIN
                 IF NOT EXISTS (
                     SELECT 1 FROM pg_constraint
                     WHERE conname = 'runs_rich_writer_epoch_range'
                       AND conrelid = 'runs'::regclass
                 ) THEN
                     ALTER TABLE runs ADD CONSTRAINT runs_rich_writer_epoch_range
                     CHECK (rich_writer_epoch >= 0 AND rich_writer_epoch <= 4294967295)
                     NOT VALID;
                 END IF;
             END $$",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS rich_mutation_heads (
                project_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                tag TEXT NOT NULL,
                step BIGINT NOT NULL,
                mutation_version NUMERIC(20, 0) NOT NULL
                    CHECK (mutation_version >= 1 AND mutation_version <= 18446744073709551615),
                public_resource_id TEXT NOT NULL,
                PRIMARY KEY (project_id, run_id, metric_name, tag, step),
                FOREIGN KEY (project_id, run_id)
                    REFERENCES runs(project_id, run_id) ON DELETE CASCADE
            )",
        )
        .execute(&self.pool)
        .await?;
        // Deployments that already had durable purge claims predate the
        // attempt queue column. Put those claims on the attempted branch so
        // the unattempted branch can stay a pure indexed deadline range.
        sqlx::query(
            "UPDATE runs SET purge_attempt_at = purging_at
             WHERE purging_at IS NOT NULL AND purge_attempt_at IS NULL",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_runs_project_ordinal_desc
             ON runs (project_id, ordinal DESC)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_runs_active_project_ordinal_desc
             ON runs (project_id, ordinal DESC)
             WHERE deleted_at IS NULL",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_runs_trash_expiry
             ON runs (deleted_at)
             WHERE deleted_at IS NOT NULL",
        )
        .execute(&self.pool)
        .await?;

        // The new name replaces installations that initialized the original
        // oldest-first index; CREATE INDEX IF NOT EXISTS cannot change it.
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_runs_trash_page_newest
             ON runs (deleted_at DESC, project_id, ordinal DESC, run_id)
             WHERE deleted_at IS NOT NULL",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query("DROP INDEX IF EXISTS idx_runs_trash_page")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_runs_purging
             ON runs (purging_at)
             WHERE purging_at IS NOT NULL",
        )
        .execute(&self.pool)
        .await?;

        // Two mergeable queue branches keep a large expired Trash set from
        // being rescanned and resorted for every bounded reaper page.
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_runs_purge_attempt_queue
             ON runs (purge_attempt_at, project_id, run_id)
             WHERE deleted_at IS NOT NULL AND purge_attempt_at IS NOT NULL",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_runs_purge_deadline_queue
             ON runs (deleted_at, project_id, run_id)
             WHERE deleted_at IS NOT NULL AND purge_attempt_at IS NULL",
        )
        .execute(&self.pool)
        .await?;

        // Backs the status watcher's 5s candidate scan (expression matches
        // status_watch_candidates' WHERE) — non-terminal rows accumulate
        // forever, so without this the scan walks the whole table.
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_runs_active_liveness
             ON runs ((COALESCE(last_system_metric_at, last_main_metric_at, created_at)))
             WHERE exit_code IS NULL AND deleted_at IS NULL",
        )
        .execute(&self.pool)
        .await?;
        // The narrower partial index replaces the pre-Trash definition. Its
        // old name would otherwise survive `IF NOT EXISTS` migrations and
        // impose duplicate write/storage cost forever on upgraded databases.
        sqlx::query("DROP INDEX IF EXISTS idx_runs_liveness")
            .execute(&self.pool)
            .await?;

        // Per-project summary behind ListProjects (docs/run-project-deletion.md). run_rows is maintained next to the only two statements that insert or delete runs. Writers lock this row after their run locks and before global_seq, the same order everywhere, so it cannot join a lock cycle; a foreign key to projects would lock that row from here and invert TerminateRun's project -> run order, and projects rows are never deleted anyway.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS project_activity (
                project_id     TEXT PRIMARY KEY,
                run_rows       BIGINT NOT NULL DEFAULT 0,
                last_logged_at TIMESTAMPTZ
            )",
        )
        .execute(&self.pool)
        .await?;
        // Seed an empty table from the runs it summarizes; emptying it reseeds at the next boot (the rollback step in docs/run-project-deletion.md).
        sqlx::query(
            "INSERT INTO project_activity (project_id, run_rows, last_logged_at)
             SELECT project_id, COUNT(*), MAX(LEAST(last_ingested_at, terminated_at))
             FROM runs
             WHERE NOT EXISTS (SELECT 1 FROM project_activity)
             GROUP BY project_id",
        )
        .execute(&self.pool)
        .await?;

        // Migrate ordinal allocation from MAX(surviving rows)+1 to a durable
        // high-water mark. The predicate makes repeated boots read-only once
        // every project has caught up, while a retained high-water mark still
        // survives deletion of the highest-numbered run.
        sqlx::query(
            "UPDATE projects AS p
             SET next_run_ordinal = maxima.next_run_ordinal
             FROM (
                 SELECT project_id, MAX(ordinal) + 1 AS next_run_ordinal
                 FROM runs
                 GROUP BY project_id
             ) AS maxima
             WHERE p.project_id = maxima.project_id
               AND p.next_run_ordinal < maxima.next_run_ordinal",
        )
        .execute(&self.pool)
        .await?;

        // Minimal tombstones survive physical cleanup so an old InitRun retry
        // can never recreate a purged identity and attach new data to it.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS purged_runs (
                project_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                terminal_version BIGINT NOT NULL DEFAULT 9223372036854775807,
                purged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (project_id, run_id)
            )",
        )
        .execute(&self.pool)
        .await?;
        // Tombstones created before terminal versions were retained cannot recover the deleted counter. The largest positive BIGINT is the only conservative fallback representable by the existing version type.
        sqlx::query(
            "ALTER TABLE purged_runs
             ADD COLUMN IF NOT EXISTS terminal_version BIGINT",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "UPDATE purged_runs SET terminal_version = $1
             WHERE terminal_version IS NULL",
        )
        .bind(i64::MAX)
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "ALTER TABLE purged_runs
             ALTER COLUMN terminal_version SET NOT NULL,
             ALTER COLUMN terminal_version SET DEFAULT 9223372036854775807",
        )
        .execute(&self.pool)
        .await?;

        // The push protocol keys run versions by bare run_id, so ownership is
        // global and permanent even though canonical rows are project-scoped.
        // Backfill every active and purged identity once, refusing to start if
        // historical data cannot be represented without ambiguity.
        let mut ownership_tx = self.pool.begin().await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS run_ids (
                run_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL
            )",
        )
        .execute(&mut *ownership_tx)
        .await?;
        sqlx::query("LOCK TABLE runs, purged_runs IN SHARE MODE")
            .execute(&mut *ownership_tx)
            .await?;
        let duplicate: Option<(String, i64)> = sqlx::query_as(
            "SELECT run_id, COUNT(DISTINCT project_id)::BIGINT
             FROM (
                 SELECT project_id, run_id FROM runs
                 UNION ALL
                 SELECT project_id, run_id FROM purged_runs
             ) AS known_runs
             GROUP BY run_id
             HAVING COUNT(DISTINCT project_id) > 1
             ORDER BY run_id
             LIMIT 1",
        )
        .fetch_optional(&mut *ownership_tx)
        .await?;
        anyhow::ensure!(
            duplicate.is_none(),
            "run-ID ownership migration found cross-project duplicate {:?}; resolve it before restarting",
            duplicate.map(|(run_id, _)| run_id)
        );
        sqlx::query(
            "INSERT INTO run_ids (run_id, project_id)
             SELECT run_id, MIN(project_id)
             FROM (
                 SELECT project_id, run_id FROM runs
                 UNION ALL
                 SELECT project_id, run_id FROM purged_runs
             ) AS known_runs
             GROUP BY run_id
             ON CONFLICT (run_id) DO NOTHING",
        )
        .execute(&mut *ownership_tx)
        .await?;
        // An aggregate, not ORDER BY ... LIMIT 1: under the LIMIT the planner walked run_ids in key order and rescanned the whole union for each row, quadratic in runs (199 s of boot at ~37k runs). The aggregate has to see every row, so it hash-joins.
        let (conflicts, first_conflict): (i64, Option<String>) = sqlx::query_as(
            "SELECT COUNT(*)::BIGINT, MIN(known_runs.run_id)
             FROM (
                 SELECT project_id, run_id FROM runs
                 UNION
                 SELECT project_id, run_id FROM purged_runs
             ) AS known_runs
             JOIN run_ids USING (run_id)
             WHERE known_runs.project_id <> run_ids.project_id",
        )
        .fetch_one(&mut *ownership_tx)
        .await?;
        anyhow::ensure!(
            conflicts == 0,
            "run-ID ownership registry conflicts with existing data: {conflicts} run(s), first {}",
            first_conflict.unwrap_or_default()
        );

        ownership_tx.commit().await?;

        // Registry of every metric a run has ever logged, maintained by the
        // ingest hook: names/types are known at write time and the set only
        // accumulates, so registration is an insert on first appearance.
        // Replaces deriving the list from ClickHouse scans — the set is all
        // the frontend consumes; the per-point statistics the old query
        // computed were never read by anything.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS run_metrics (
                project_id  TEXT NOT NULL,
                run_id      TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_type TEXT NOT NULL,
                PRIMARY KEY (project_id, run_id, metric_name)
            )",
        )
        .execute(&self.pool)
        .await?;

        // Rollback fences, not executable migration machinery. The previous
        // server release interprets missing markers by marking every active
        // run finished and aggregating the entire raw ClickHouse table. Keep
        // fresh installations safe if that binary is restored, while current
        // startup never branches on or executes these completed migrations.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                name       TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "INSERT INTO schema_migrations (name)
             VALUES ('backfill_existing_runs_finished'),
                    ('seed_run_metrics_from_clickhouse')
             ON CONFLICT (name) DO NOTHING",
        )
        .execute(&self.pool)
        .await?;

        tracing::info!("Postgres schema ready");
        Ok(())
    }

    /// Register a run. Idempotent: calling with an existing (project_id, run_id)
    /// resumes that same logical identity and preserves its original creation,
    /// ingest history, and first-write-wins run_name. Its internal liveness
    /// baseline restarts for the new execution; independent creation timing
    /// still requires a new run ID.
    ///
    /// Transactional — acquires `FOR UPDATE` lock on the project row so that
    /// concurrent InitRun calls for the same project assign distinct ordinals.
    ///
    /// `import_created_at_ms` is ImportRun's: the new row carries the archived
    /// creation time and an idempotent re-import skips the re-init liveness
    /// reset — a replayed run must never look alive. Either way this path
    /// advances rich_writer_epoch and bumps the project version on every call.
    pub async fn init_run(
        &self,
        project_id: &str,
        run_id: &str,
        run_name: &str,
        import_created_at_ms: Option<i64>,
    ) -> std::result::Result<InitRunOutcome, InitRunError> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            "INSERT INTO projects (project_id) VALUES ($1) ON CONFLICT (project_id) DO NOTHING",
        )
        .bind(project_id)
        .execute(&mut *tx)
        .await?;

        // Lock before reading `runs`: concurrent InitRun calls for the same
        // new identity serialize here, so the follower observes the leader's
        // committed row instead of racing the bare INSERT below.
        sqlx::query("SELECT project_id FROM projects WHERE project_id = $1 FOR UPDATE")
            .bind(project_id)
            .fetch_one(&mut *tx)
            .await?;

        // Atomically claim the bare run ID before inspecting the project-local
        // lifecycle row. A conflicting INSERT waits for the winner's
        // transaction, then the owner read distinguishes an idempotent
        // same-project retry from a forbidden cross-project reuse.
        let claimed_owner: Option<String> = sqlx::query_scalar(
            "INSERT INTO run_ids (run_id, project_id) VALUES ($1, $2)
             ON CONFLICT (run_id) DO NOTHING
             RETURNING project_id",
        )
        .bind(run_id)
        .bind(project_id)
        .fetch_optional(&mut *tx)
        .await?;
        if claimed_owner.is_none() {
            let owner: String =
                sqlx::query_scalar("SELECT project_id FROM run_ids WHERE run_id = $1")
                    .bind(run_id)
                    .fetch_one(&mut *tx)
                    .await?;
            if owner != project_id {
                return Err(InitRunError::RunIdOwned {
                    run_id: run_id.to_string(),
                    requested_project_id: project_id.to_string(),
                });
            }
        }

        let existing: Option<(Option<i64>, Option<i64>, i64)> = sqlx::query_as(
            "SELECT (EXTRACT(EPOCH FROM deleted_at) * 1000)::BIGINT,
                    (EXTRACT(EPOCH FROM purging_at) * 1000)::BIGINT,
                    (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
             FROM runs WHERE project_id = $1 AND run_id = $2
             FOR UPDATE",
        )
        .bind(project_id)
        .bind(run_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((deleted_at_ms, purging_at_ms, now_ms)) = existing {
            let state = classify_lifecycle(
                true,
                false,
                deleted_at_ms,
                purging_at_ms,
                deleted_at_ms.is_some_and(|deleted_at_ms| {
                    deleted_at_ms.saturating_add(TRASH_RETENTION_MS) <= now_ms
                }),
            );
            if state != RunLifecycleClass::Active {
                return Err(InitRunError::NotInitializable {
                    key: RunKey::new(project_id, run_id),
                    state,
                });
            }
        }
        if existing.is_none() {
            let was_purged: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                    SELECT 1 FROM purged_runs WHERE project_id = $1 AND run_id = $2
                 )",
            )
            .bind(project_id)
            .bind(run_id)
            .fetch_one(&mut *tx)
            .await?;
            if was_purged {
                return Err(InitRunError::NotInitializable {
                    key: RunKey::new(project_id, run_id),
                    state: RunLifecycleClass::Purged,
                });
            }
        }

        // Allocate from a durable high-water mark only for a genuinely new
        // identity. Purging the highest run can therefore never reuse its
        // human-facing number.
        let inserted_ordinal: Option<(i64,)> = if existing.is_none() {
            let ordinal: i64 = sqlx::query_scalar(
                "UPDATE projects
                 SET next_run_ordinal = next_run_ordinal + 1
                 WHERE project_id = $1
                 RETURNING next_run_ordinal - 1",
            )
            .bind(project_id)
            .fetch_one(&mut *tx)
            .await?;
            Some(
                sqlx::query_as(
                    "INSERT INTO runs (project_id, run_id, run_name, ordinal, created_at)
                     VALUES ($1, $2, $3, $4,
                             COALESCE(to_timestamp($5::BIGINT / 1000.0), NOW()))
                     RETURNING ordinal",
                )
                .bind(project_id)
                .bind(run_id)
                .bind(run_name)
                .bind(ordinal)
                .bind(import_created_at_ms)
                .fetch_one(&mut *tx)
                .await?,
            )
        } else {
            None
        };

        let newly_created = inserted_ordinal.is_some();
        // A project is listed while it has any runs row, so discovery changes when this insert gives it its only one.
        let mut bumped_global: Option<i64> = None;
        if newly_created {
            let run_rows: i64 = sqlx::query_scalar(
                "INSERT INTO project_activity (project_id, run_rows) VALUES ($1, 1)
                 ON CONFLICT (project_id)
                 DO UPDATE SET run_rows = project_activity.run_rows + 1
                 RETURNING run_rows",
            )
            .bind(project_id)
            .fetch_one(&mut *tx)
            .await?;
            if run_rows == 1 {
                bumped_global = Some(
                    sqlx::query_scalar(
                        "UPDATE global_seq SET version = version + 1 WHERE id = 1 RETURNING version",
                    )
                    .fetch_one(&mut *tx)
                    .await?,
                );
            }
        }
        let mut bumped_run: Option<i64> = None;
        if !newly_created && import_created_at_ms.is_none() {
            // Re-init of an existing run (rare retry workflow). Clear the
            // terminal exit signal and seed a fresh startup grace. Clear the
            // prior execution's system heartbeat so system-metrics-disabled
            // clients keep using the documented main-heartbeat fallback. Use
            // wall-clock time here because the transaction may have waited on
            // the project lock; PostgreSQL's NOW() is fixed at transaction start.
            bumped_run = Some(
                sqlx::query_scalar(
                    "UPDATE runs
                     SET exit_code = NULL,
                         terminated_at = NULL,
                         last_main_metric_at = clock_timestamp(),
                         last_system_metric_at = NULL,
                         version = version + 1
                     WHERE project_id = $1 AND run_id = $2 AND deleted_at IS NULL
                     RETURNING version",
                )
                .bind(project_id)
                .bind(run_id)
                .fetch_one(&mut *tx)
                .await?,
            );
        }
        // Project bump in BOTH cases: a new run changes the run list; a re-init changes a run's list-level state (terminal -> derived). Either way the fresh liveness baseline re-enters the status watcher's candidate window, but its 5s tick is not prompt — the bump here is the immediate signal, the watcher's is the occasionally-redundant backup.
        let bumped_project: i64 = sqlx::query_scalar(
            "UPDATE projects SET version = version + 1
             WHERE project_id = $1 RETURNING version",
        )
        .bind(project_id)
        .fetch_one(&mut *tx)
        .await?;

        // Read back the canonical row (either freshly inserted or pre-existing).
        let row: RunInfoRow = sqlx::query_as(run_info_sql!(
            "SELECT",
            " FROM runs WHERE project_id = $1 AND run_id = $2"
        ))
        .bind(project_id)
        .bind(run_id)
        .fetch_one(&mut *tx)
        .await?;
        let server_now_ms = transaction_clock_ms(&mut tx).await?;

        let writer_epoch: Option<i64> = sqlx::query_scalar(
            "UPDATE runs
             SET rich_writer_epoch = rich_writer_epoch + 1
             WHERE project_id = $1 AND run_id = $2
               AND rich_writer_epoch < 4294967295
             RETURNING rich_writer_epoch",
        )
        .bind(project_id)
        .bind(run_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(writer_epoch) = writer_epoch else {
            return Err(InitRunError::WriterEpochExhausted(RunKey::new(
                project_id, run_id,
            )));
        };

        tx.commit().await?;
        Ok(InitRunOutcome {
            row,
            writer_epoch: writer_epoch as u32,
            server_now_ms,
            bumped_global: bumped_global.map(|v| v as u64),
            bumped_project: bumped_project as u64,
            bumped_run: bumped_run.map(|v| v as u64),
        })
    }

    /// Advance one rich-key head atomically. The resource id participates in
    /// equal-version comparison so an ambiguous retry can complete while a
    /// writer bug that reuses a version for different content fails closed.
    pub async fn compare_rich_mutation(
        &self,
        candidate: RichMutationCandidate<'_>,
    ) -> Result<RichMutationDecision> {
        let RichMutationCandidate {
            project_id,
            run_id,
            metric_name,
            tag,
            step,
            mutation_version,
            public_resource_id,
        } = candidate;
        anyhow::ensure!(
            mutation_version >> 32 != 0 && mutation_version as u32 != 0,
            "mutation version must contain nonzero epoch and sequence fields"
        );
        let version = mutation_version.to_string();
        let mut tx = self.pool.begin().await?;
        let current_epoch: i64 = sqlx::query_scalar(
            "SELECT rich_writer_epoch FROM runs
             WHERE project_id = $1 AND run_id = $2",
        )
        .bind(project_id)
        .bind(run_id)
        .fetch_one(&mut *tx)
        .await?;
        if mutation_version >> 32 > current_epoch as u64 {
            tx.rollback().await?;
            return Ok(RichMutationDecision::UnallocatedEpoch {
                current_epoch: current_epoch as u32,
            });
        }
        let advanced: Option<(String, String)> = sqlx::query_as(
            "INSERT INTO rich_mutation_heads (
                 project_id, run_id, metric_name, tag, step,
                 mutation_version, public_resource_id
             ) VALUES ($1, $2, $3, $4, $5, $6::NUMERIC, $7)
             ON CONFLICT (project_id, run_id, metric_name, tag, step)
             DO UPDATE SET
                 mutation_version = EXCLUDED.mutation_version,
                 public_resource_id = EXCLUDED.public_resource_id
             WHERE rich_mutation_heads.mutation_version < EXCLUDED.mutation_version
             RETURNING mutation_version::TEXT, public_resource_id",
        )
        .bind(project_id)
        .bind(run_id)
        .bind(metric_name)
        .bind(tag)
        .bind(step)
        .bind(&version)
        .bind(public_resource_id)
        .fetch_optional(&mut *tx)
        .await?;
        if advanced.is_some() {
            tx.commit().await?;
            return Ok(RichMutationDecision::Accepted);
        }

        // The conflicting INSERT retains the row lock through this transaction,
        // so the comparison cannot race another writer between the no-op and
        // this authoritative read.
        let (stored_version, stored_resource_id): (String, String) = sqlx::query_as(
            "SELECT mutation_version::TEXT, public_resource_id
             FROM rich_mutation_heads
             WHERE project_id = $1 AND run_id = $2 AND metric_name = $3
               AND tag = $4 AND step = $5
             FOR UPDATE",
        )
        .bind(project_id)
        .bind(run_id)
        .bind(metric_name)
        .bind(tag)
        .bind(step)
        .fetch_one(&mut *tx)
        .await?;
        let stored_version = stored_version.parse::<u64>()?;
        let decision = if stored_version > mutation_version {
            RichMutationDecision::Superseded { stored_version }
        } else if stored_resource_id == public_resource_id {
            RichMutationDecision::Idempotent
        } else {
            RichMutationDecision::Conflict { stored_resource_id }
        };
        tx.commit().await?;
        Ok(decision)
    }

    pub async fn list_runs(&self, project_id: &str) -> Result<ListRunsSnapshot> {
        // One statement preserves the existing single database round trip and makes the version certify these exact rows. Reading it separately could label pre-mutation rows with a newer version and hide a change.
        let rows: Vec<ListRunsRow> = sqlx::query_as(run_info_sql!(
            "SELECT",
            ", COALESCE((SELECT version FROM projects WHERE project_id = $1), 0)
                   AS project_version,
               (EXTRACT(EPOCH FROM statement_timestamp()) * 1000)::BIGINT
                   AS server_now_ms
             FROM runs
             WHERE project_id = $1 AND deleted_at IS NULL
             ORDER BY ordinal DESC"
        ))
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?;
        // No row means no snapshot token: keep the cheap empty-list refresh instead of adding a singleton join and special decoding to every request.
        let project_version = rows.first().map(|row| row.project_version as u64);
        let server_now_ms = rows
            .first()
            .map(|row| row.server_now_ms)
            .unwrap_or_default();
        Ok(ListRunsSnapshot {
            rows: rows.into_iter().map(|row| row.row).collect(),
            server_now_ms,
            project_version,
        })
    }

    /// Rename an active run and advance the project's run-list version.
    /// Setting the current name is an idempotent no-op. The per-run data
    /// version intentionally stays unchanged: labels come from ListRuns, and
    /// a metadata-only edit must not make every visible chart refetch.
    pub async fn rename_run(
        &self,
        project_id: &str,
        run_id: &str,
        run_name: &str,
    ) -> Result<Option<RenameRunOutcome>> {
        let mut tx = self.pool.begin().await?;
        // Match InitRun's project -> run lock order. InitRun and RenameRun both
        // hold shared lifecycle gates, so PostgreSQL owns their serialization;
        // taking these rows in opposite order lets a concurrent re-init
        // deadlock with a rename.
        let project: Option<String> =
            sqlx::query_scalar("SELECT project_id FROM projects WHERE project_id = $1 FOR UPDATE")
                .bind(project_id)
                .fetch_optional(&mut *tx)
                .await?;
        if project.is_none() {
            tx.rollback().await?;
            return Ok(None);
        }
        let changed = sqlx::query(
            "UPDATE runs
             SET run_name = $3
             WHERE project_id = $1
               AND run_id = $2
               AND deleted_at IS NULL
               AND run_name IS DISTINCT FROM $3",
        )
        .bind(project_id)
        .bind(run_id)
        .bind(run_name)
        .execute(&mut *tx)
        .await?
        .rows_affected()
            != 0;

        let bumped_project = if changed {
            Some(
                sqlx::query_scalar(
                    "UPDATE projects SET version = version + 1
                     WHERE project_id = $1 RETURNING version",
                )
                .bind(project_id)
                .fetch_one(&mut *tx)
                .await?,
            )
        } else {
            None
        };

        let row: Option<RunInfoRow> = sqlx::query_as(run_info_sql!(
            "SELECT",
            " FROM runs
             WHERE project_id = $1 AND run_id = $2 AND deleted_at IS NULL"
        ))
        .bind(project_id)
        .bind(run_id)
        .fetch_optional(&mut *tx)
        .await?;
        let server_now_ms = transaction_clock_ms(&mut tx).await?;

        tx.commit().await?;
        Ok(row.map(|row| RenameRunOutcome {
            row,
            server_now_ms,
            bumped_project: bumped_project.map(|version: i64| version as u64),
        }))
    }

    /// Record an explicit termination signal from the client. Sets `exit_code`
    /// (0 = clean exit, non-zero = crash) and bumps versions so subscribers
    /// see the transition promptly. Returns the bumped (run_version,
    /// project_version) for the push bus, or None if the run doesn't exist.
    ///
    /// `terminated_at_ms` is FinalizeImportRun's archived end time, set
    /// unconditionally so a re-import converges on it; live TerminateRun passes
    /// None and keeps the first end time (only InitRun opens a new execution).
    pub async fn terminate_run(
        &self,
        project_id: &str,
        run_id: &str,
        exit_code: i32,
        pending_last_ingested_at_ms: Option<i64>,
        terminated_at_ms: Option<i64>,
    ) -> Result<Option<TerminateRunOutcome>> {
        let mut tx = self.pool.begin().await?;
        // Match InitRun and RenameRun's project -> run database lock order.
        let project: Option<String> =
            sqlx::query_scalar("SELECT project_id FROM projects WHERE project_id = $1 FOR UPDATE")
                .bind(project_id)
                .fetch_optional(&mut *tx)
                .await?;
        if project.is_none() {
            tx.rollback().await?;
            return Ok(None);
        }
        let run: Option<(i64, String)> = sqlx::query_as(
            "UPDATE runs
             SET exit_code = $3,
                 terminated_at = COALESCE(to_timestamp($5::BIGINT / 1000.0),
                                          runs.terminated_at, clock_timestamp()),
                 last_ingested_at = GREATEST(
                     last_ingested_at,
                     to_timestamp($4::BIGINT / 1000.0)
                 ),
                 version = version + 1
             WHERE project_id = $1 AND run_id = $2 AND deleted_at IS NULL
             RETURNING version, run_name",
        )
        .bind(project_id)
        .bind(run_id)
        .bind(exit_code)
        .bind(pending_last_ingested_at_ms)
        .bind(terminated_at_ms)
        .fetch_optional(&mut *tx)
        .await?;

        let mut outcome = None;
        if let Some((run_version, run_name)) = run {
            let project_version: i64 = sqlx::query_scalar(
                "UPDATE projects SET version = version + 1
                 WHERE project_id = $1 RETURNING version",
            )
            .bind(project_id)
            .fetch_one(&mut *tx)
            .await?;
            outcome = Some(TerminateRunOutcome {
                run_name,
                bumped_run: run_version as u64,
                bumped_project: project_version as u64,
            });
        }

        tx.commit().await?;
        Ok(outcome)
    }

    /// Bump `projects.version` for each given project, returning the new
    /// versions. Used by the status watcher for derived liveness transitions
    /// and by delayed terminal-state ingest to refresh ListRuns timing.
    pub async fn bump_project_versions(
        &self,
        project_ids: &[String],
    ) -> Result<Vec<(String, u64)>> {
        if project_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "UPDATE projects SET version = version + 1
             WHERE project_id = ANY($1)
             RETURNING project_id, version",
        )
        .bind(project_ids)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(p, v)| (p, v as u64)).collect())
    }

    /// Runs whose derived liveness status can still change: no exit signal, and effective heartbeat (compute_status's sys → main → created fallback chain) inside the shared PRESUMED_DEAD window plus the independently named status-watch margin. Past that, status is pinned until new data or an exit signal — both push their own events.
    pub async fn status_watch_candidates(&self) -> Result<TimedRunRows> {
        let rows: Vec<TimedRunInfoRow> = sqlx::query_as(run_info_sql!(
            "SELECT",
            ", (EXTRACT(EPOCH FROM statement_timestamp()) * 1000)::BIGINT
                 AS server_now_ms
             FROM runs
             WHERE exit_code IS NULL
               AND deleted_at IS NULL
               AND COALESCE(last_system_metric_at, last_main_metric_at, created_at)
                   > statement_timestamp() - make_interval(secs => $1::double precision)"
        ))
        .bind(STATUS_WATCH_WINDOW.as_secs_f64())
        .fetch_all(&self.pool)
        .await?;
        Ok(split_timed_run_rows(rows))
    }

    /// Bump `runs.version` and roll forward the client-authored liveness clocks
    /// plus the server-observed ingest clock for every touched run. Called from
    /// the ingest flush path so (a) the frontend notices new metrics via the
    /// version bump, and (b) run timing sees only successfully persisted data.
    ///
    /// Silently skips pairs that don't exist (shouldn't happen in practice,
    /// since ingest requires InitRun first — but we don't want ingest to fail
    /// if someone logs metrics for a non-existent run).
    ///
    /// Returns committed run versions and project versions for explicitly
    /// terminal runs. The latter refresh ListRuns after delayed ingestion.
    pub async fn bump_run_versions(
        &self,
        touched: &[TouchedRun],
    ) -> Result<BumpRunVersionsOutcome> {
        if touched.is_empty() {
            return Ok(BumpRunVersionsOutcome::default());
        }
        // UNNEST handles NULLs in the timestamp arrays cleanly. GREATEST in
        // postgres ignores NULLs, so a batch with no main metrics (sys_ms only)
        // leaves last_main_metric_at unchanged.
        let pids: Vec<&str> = touched.iter().map(|t| t.project_id.as_str()).collect();
        let rids: Vec<&str> = touched.iter().map(|t| t.run_id.as_str()).collect();
        let main_ms: Vec<Option<i64>> = touched.iter().map(|t| t.max_main_metric_at_ms).collect();
        let sys_ms: Vec<Option<i64>> = touched.iter().map(|t| t.max_system_metric_at_ms).collect();
        let ingested_ms: Vec<i64> = touched.iter().map(|t| t.last_ingested_at_ms).collect();

        // The same statement rolls project_activity.last_logged_at forward (rule on ListProjectsResponse.last_logged_at_ms), at most once per clock minute per project, the display's grid. InitRun or the boot seed created each project's row before its runs could log.
        let rows: Vec<(String, String, i64, bool)> = sqlx::query_as(
            "WITH bumped AS (
             UPDATE runs SET
                version = version + 1,
                last_main_metric_at = GREATEST(
                    runs.last_main_metric_at,
                    to_timestamp(touched.main_ms / 1000.0)
                ),
                last_system_metric_at = GREATEST(
                    runs.last_system_metric_at,
                    to_timestamp(touched.sys_ms / 1000.0)
                ),
                last_ingested_at = GREATEST(
                    runs.last_ingested_at,
                    to_timestamp(touched.ingested_ms / 1000.0)
                )
             FROM (
                SELECT * FROM UNNEST(
                    $1::TEXT[], $2::TEXT[], $3::BIGINT[], $4::BIGINT[], $5::BIGINT[]
                ) AS u(project_id, run_id, main_ms, sys_ms, ingested_ms)
             ) AS touched
             WHERE runs.project_id = touched.project_id
               AND runs.run_id = touched.run_id
               AND runs.deleted_at IS NULL
             RETURNING runs.project_id, runs.run_id, runs.version,
                       runs.exit_code IS NOT NULL AS needs_project_refresh,
                       LEAST(runs.last_ingested_at, runs.terminated_at) AS logged_at
             ), logged AS (
                SELECT project_id, MAX(logged_at) AS at FROM bumped GROUP BY project_id
             ), rolled_forward AS (
                UPDATE project_activity SET last_logged_at = logged.at
                FROM logged
                WHERE project_activity.project_id = logged.project_id
                  AND (project_activity.last_logged_at IS NULL
                       OR logged.at >= date_trunc('minute', project_activity.last_logged_at)
                                       + INTERVAL '1 minute')
             )
             SELECT project_id, run_id, version, needs_project_refresh FROM bumped",
        )
        .bind(&pids)
        .bind(&rids)
        .bind(&main_ms)
        .bind(&sys_ms)
        .bind(&ingested_ms)
        .fetch_all(&self.pool)
        .await?;

        let mut refresh_projects: Vec<String> = rows
            .iter()
            .filter(|(_, _, _, needs_project_refresh)| *needs_project_refresh)
            .map(|(project_id, _, _, _)| project_id.clone())
            .collect();
        refresh_projects.sort_unstable();
        refresh_projects.dedup();

        // Deliberately a second autocommitted statement. TerminateRun locks
        // project -> run; retaining run locks while updating projects here
        // would create the inverse run -> project order and a deadlock cycle.
        // If this statement fails, the coalescer keeps the dirty heartbeat and
        // retries both harmless monotonic bumps on its next tick.
        let projects = self.bump_project_versions(&refresh_projects).await?;
        let runs = rows
            .into_iter()
            .map(|(_, run_id, version, _)| (run_id, version as u64))
            .collect();
        Ok(BumpRunVersionsOutcome { runs, projects })
    }

    /// Upsert newly observed metrics into the registry. The type can only
    /// ever upgrade along CDN < NUMERIC < TEXT_STREAM — the precedence the
    /// old ClickHouse multiIf encoded (a metric with both text and values
    /// is a text stream). Rows are (project_id, run_id, metric_name,
    /// metric_type). Returns the distinct run_ids whose registry actually
    /// changed (row inserted or type upgraded) — the push signal for
    /// clients to re-list those runs' metrics.
    pub async fn register_run_metrics(
        &self,
        rows: &[(String, String, String, String)],
    ) -> Result<Vec<String>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(metric_type) = rows
            .iter()
            .map(|row| row.3.as_str())
            .find(|metric_type| !matches!(*metric_type, "CDN" | "NUMERIC" | "TEXT_STREAM"))
        {
            anyhow::bail!("invalid metric registry type {metric_type:?}");
        }
        let pids: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();
        let rids: Vec<&str> = rows.iter().map(|r| r.1.as_str()).collect();
        let names: Vec<&str> = rows.iter().map(|r| r.2.as_str()).collect();
        let types: Vec<&str> = rows.iter().map(|r| r.3.as_str()).collect();
        let changed: Vec<(String,)> = sqlx::query_as(
            "INSERT INTO run_metrics (project_id, run_id, metric_name, metric_type)
             SELECT incoming.project_id, incoming.run_id,
                    incoming.metric_name, incoming.metric_type
             FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[])
                  AS incoming(project_id, run_id, metric_name, metric_type)
             JOIN runs ON runs.project_id = incoming.project_id
                      AND runs.run_id = incoming.run_id
             ON CONFLICT (project_id, run_id, metric_name) DO UPDATE
             SET metric_type = EXCLUDED.metric_type
             WHERE CASE EXCLUDED.metric_type
                       WHEN 'TEXT_STREAM' THEN 3 WHEN 'NUMERIC' THEN 2 ELSE 1 END
                 > CASE run_metrics.metric_type
                       WHEN 'TEXT_STREAM' THEN 3 WHEN 'NUMERIC' THEN 2 ELSE 1 END
             RETURNING run_id",
        )
        .bind(&pids)
        .bind(&rids)
        .bind(&names)
        .bind(&types)
        .fetch_all(&self.pool)
        .await?;
        let mut ids: Vec<String> = changed.into_iter().map(|(r,)| r).collect();
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }

    /// Distinct metric (name, collapsed type) across a SET of runs — the dashboard's layout base, scoped to the visible runs. Types collapse by the registry's CDN < NUMERIC < TEXT_STREAM precedence; name ordered; empty `run_ids` yields no rows.
    pub async fn list_run_set_metrics(
        &self,
        project_id: &str,
        run_ids: &[String],
    ) -> Result<Vec<(String, String)>> {
        if run_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rids: Vec<&str> = run_ids.iter().map(String::as_str).collect();
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT metric_name,
                CASE max(CASE metric_type
                             WHEN 'TEXT_STREAM' THEN 3 WHEN 'NUMERIC' THEN 2 ELSE 1 END)
                     WHEN 3 THEN 'TEXT_STREAM' WHEN 2 THEN 'NUMERIC' ELSE 'CDN' END
             FROM run_metrics
             WHERE project_id = $1 AND run_id = ANY($2)
             GROUP BY metric_name
             ORDER BY metric_name",
        )
        .bind(project_id)
        .bind(&rids)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// (metric_name, metric_type) for one run, name-ordered.
    pub async fn list_run_metrics(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> Result<Vec<(String, String)>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT metric_name, metric_type FROM run_metrics
             WHERE project_id = $1 AND run_id = $2
             ORDER BY metric_name",
        )
        .bind(project_id)
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Projects with at least one runs row, in Trash or not, ordered by id.
    pub async fn list_metric_projects(&self) -> Result<ProjectListing> {
        let rows: Vec<(String, Option<i64>, i64)> = sqlx::query_as(
            "SELECT project_id,
                    (EXTRACT(EPOCH FROM last_logged_at) * 1000)::BIGINT,
                    (EXTRACT(EPOCH FROM statement_timestamp()) * 1000)::BIGINT
             FROM project_activity
             WHERE run_rows > 0
             ORDER BY project_id",
        )
        .fetch_all(&self.pool)
        .await?;
        let server_now_ms = rows.first().map(|r| r.2).unwrap_or_default();
        Ok(ProjectListing {
            projects: rows.into_iter().map(|(id, at, _)| (id, at)).collect(),
            server_now_ms,
        })
    }

    /// Classify run identities against the canonical rows and permanent
    /// tombstones in one bounded query. Missing keys are explicitly returned
    /// as `Missing`, which makes callers safe against accidental omission.
    pub async fn classify_runs(
        &self,
        keys: &[RunKey],
    ) -> Result<HashMap<RunKey, RunLifecycleClass>> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }
        let project_ids: Vec<&str> = keys.iter().map(|key| key.project_id.as_str()).collect();
        let run_ids: Vec<&str> = keys.iter().map(|key| key.run_id.as_str()).collect();
        type ClassifiedRunRow = (String, String, Option<i64>, Option<i64>, bool, bool, bool);
        let rows: Vec<ClassifiedRunRow> = sqlx::query_as(
            "SELECT wanted.project_id, wanted.run_id,
                    (EXTRACT(EPOCH FROM runs.deleted_at) * 1000)::BIGINT AS deleted_at_ms,
                    (EXTRACT(EPOCH FROM runs.purging_at) * 1000)::BIGINT AS purging_at_ms,
                    runs.run_id IS NOT NULL AS canonical_exists,
                    COALESCE(
                      runs.deleted_at + ($3::BIGINT * INTERVAL '1 millisecond')
                        <= clock_timestamp(),
                      FALSE
                    )
                      AS is_expired,
                    purged_runs.run_id IS NOT NULL AS was_purged
             FROM UNNEST($1::TEXT[], $2::TEXT[]) AS wanted(project_id, run_id)
             LEFT JOIN runs ON runs.project_id = wanted.project_id
                           AND runs.run_id = wanted.run_id
             LEFT JOIN purged_runs ON purged_runs.project_id = wanted.project_id
                                   AND purged_runs.run_id = wanted.run_id",
        )
        .bind(&project_ids)
        .bind(&run_ids)
        .bind(TRASH_RETENTION_MS)
        .fetch_all(&self.pool)
        .await?;

        let mut classes = HashMap::with_capacity(keys.len());
        for (
            project_id,
            run_id,
            deleted_at_ms,
            purging_at_ms,
            canonical_exists,
            is_expired,
            was_purged,
        ) in rows
        {
            let class = classify_lifecycle(
                canonical_exists,
                was_purged,
                deleted_at_ms,
                purging_at_ms,
                is_expired,
            );
            classes.insert(RunKey::new(project_id, run_id), class);
        }
        for key in keys {
            classes
                .entry(key.clone())
                .or_insert(RunLifecycleClass::Missing);
        }
        Ok(classes)
    }

    pub async fn ensure_runs_active(&self, keys: &[RunKey]) -> Result<(), RunAccessError> {
        let classes = self
            .classify_runs(keys)
            .await
            .map_err(RunAccessError::Store)?;
        for key in keys {
            let state = classes
                .get(key)
                .copied()
                .unwrap_or(RunLifecycleClass::Missing);
            if state != RunLifecycleClass::Active {
                return Err(RunAccessError::NotActive {
                    key: key.clone(),
                    state,
                });
            }
        }
        Ok(())
    }

    pub async fn ensure_runs_readable(&self, keys: &[RunKey]) -> Result<(), RunAccessError> {
        let classes = self
            .classify_runs(keys)
            .await
            .map_err(RunAccessError::Store)?;
        for key in keys {
            let state = classes
                .get(key)
                .copied()
                .unwrap_or(RunLifecycleClass::Missing);
            if !matches!(
                state,
                RunLifecycleClass::Active | RunLifecycleClass::Trashed
            ) {
                return Err(RunAccessError::NotReadable {
                    key: key.clone(),
                    state,
                });
            }
        }
        Ok(())
    }

    pub async fn lifecycle_snapshot(&self) -> Result<LifecycleSnapshot> {
        let (global_version, server_now_ms): (i64, i64) = sqlx::query_as(
            "SELECT version,
                    (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
             FROM global_seq WHERE id = 1",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(LifecycleSnapshot {
            global_version: global_version as u64,
            server_now_ms,
        })
    }

    /// Trash one internal transaction. The public RPC deliberately has no
    /// size limit; QueryService feeds this method chunks of 256 positions and
    /// continues after a failed chunk.
    pub async fn trash_runs_chunk(
        &self,
        project_id: &str,
        run_ids: &[String],
        pending_last_ingested_at_ms: &HashMap<String, i64>,
    ) -> Result<TrashChunkOutcome> {
        debug_assert!(!run_ids.is_empty());

        let mut tx = self.pool.begin().await?;
        // This path deliberately locks runs before its project row. QueryService
        // holds exclusive per-run lifecycle gates around the whole operation,
        // so other lifecycle mutations for these identities cannot overlap it.
        // Keep that exclusion if this database lock order changes.
        let before: Vec<RunRecordRow> = sqlx::query_as(run_record_sql!(
            "SELECT",
            " FROM runs
             WHERE project_id = $1 AND run_id = ANY($2)
             FOR UPDATE"
        ))
        .bind(project_id)
        .bind(run_ids)
        .fetch_all(&mut *tx)
        .await?;
        let now_ms: i64 =
            sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT")
                .fetch_one(&mut *tx)
                .await?;

        let before_by_id: HashMap<String, RunRecordRow> = before
            .into_iter()
            .map(|row| (row.run_id.clone(), row))
            .collect();
        let active_ids: Vec<String> = before_by_id
            .values()
            .filter(|row| row.lifecycle_at(now_ms) == RunLifecycleClass::Active)
            .map(|row| row.run_id.clone())
            .collect();
        let active_ingested_ms: Vec<Option<i64>> = active_ids
            .iter()
            .map(|run_id| pending_last_ingested_at_ms.get(run_id).copied())
            .collect();

        let mut bumped_project = None;
        let mut bumped_global = None;
        if !active_ids.is_empty() {
            sqlx::query(
                "UPDATE runs
                 SET deleted_at = to_timestamp($3::DOUBLE PRECISION / 1000.0),
                     purging_at = NULL,
                     purge_attempt_at = NULL,
                     last_ingested_at = GREATEST(
                         runs.last_ingested_at,
                         to_timestamp(pending.ingested_ms / 1000.0)
                     ),
                     version = version + 1
                 FROM UNNEST($2::TEXT[], $4::BIGINT[])
                      AS pending(run_id, ingested_ms)
                 WHERE runs.project_id = $1
                   AND runs.run_id = pending.run_id
                   AND runs.deleted_at IS NULL",
            )
            .bind(project_id)
            .bind(&active_ids)
            .bind(now_ms)
            .bind(&active_ingested_ms)
            .execute(&mut *tx)
            .await?;
            let version: i64 = sqlx::query_scalar(
                "UPDATE projects SET version = version + 1
                 WHERE project_id = $1 RETURNING version",
            )
            .bind(project_id)
            .fetch_one(&mut *tx)
            .await?;
            bumped_project = Some(version as u64);
            let global: i64 = sqlx::query_scalar(
                "UPDATE global_seq SET version = version + 1 WHERE id = 1 RETURNING version",
            )
            .fetch_one(&mut *tx)
            .await?;
            bumped_global = Some(global as u64);
        }

        let purged: HashSet<String> = sqlx::query_scalar(
            "SELECT run_id FROM purged_runs WHERE project_id = $1 AND run_id = ANY($2)",
        )
        .bind(project_id)
        .bind(run_ids)
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .collect();

        let results = classify_trash_results(run_ids, &before_by_id, &purged, now_ms);

        tx.commit().await?;
        Ok(TrashChunkOutcome {
            results,
            bumped_project,
            bumped_global,
        })
    }

    pub async fn restore_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> Result<RestoreMutationOutcome> {
        let mut tx = self.pool.begin().await?;
        // See trash_runs_chunk: QueryService's exclusive per-run gate makes
        // this run-first/project-second order safe against project-first
        // lifecycle writers for the same identity.
        let before: Option<RunRecordRow> = sqlx::query_as(run_record_sql!(
            "SELECT",
            " FROM runs WHERE project_id = $1 AND run_id = $2 FOR UPDATE"
        ))
        .bind(project_id)
        .bind(run_id)
        .fetch_optional(&mut *tx)
        .await?;
        let now_ms: i64 =
            sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT")
                .fetch_one(&mut *tx)
                .await?;

        let Some(before) = before else {
            let purged: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                    SELECT 1 FROM purged_runs WHERE project_id = $1 AND run_id = $2
                 )",
            )
            .bind(project_id)
            .bind(run_id)
            .fetch_one(&mut *tx)
            .await?;
            tx.commit().await?;
            return Ok(RestoreMutationOutcome {
                kind: if purged {
                    RestoreMutationKind::Expired
                } else {
                    RestoreMutationKind::NotFound
                },
                row: None,
                server_now_ms: now_ms,
                bumped_run: None,
                bumped_project: None,
                bumped_global: None,
            });
        };

        match before.lifecycle_at(now_ms) {
            RunLifecycleClass::Active => {
                tx.commit().await?;
                Ok(RestoreMutationOutcome {
                    kind: RestoreMutationKind::AlreadyActive,
                    row: None,
                    server_now_ms: now_ms,
                    bumped_run: None,
                    bumped_project: None,
                    bumped_global: None,
                })
            }
            RunLifecycleClass::Trashed => {
                // Advance past the synthetic `stored + 1` generation that an
                // expiry poll can observe while this transaction is waiting
                // to commit. The range guard turns counter exhaustion or a
                // corrupt negative value into a clean rollback instead of a
                // PostgreSQL BIGINT overflow or a bogus u64 event.
                let restored_run_version: i64 = sqlx::query_scalar(
                    "UPDATE runs
                     SET deleted_at = NULL, purging_at = NULL,
                         purge_attempt_at = NULL, version = version + $3
                     WHERE project_id = $1 AND run_id = $2
                       AND version BETWEEN 0 AND $4
                     RETURNING version",
                )
                .bind(project_id)
                .bind(run_id)
                .bind(RESTORE_VERSION_INCREMENT)
                .bind(MAX_RESTORABLE_RUN_VERSION)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "cannot restore {project_id}/{run_id}: run version counter has no terminal-generation headroom"
                    )
                })?;
                let project: i64 = sqlx::query_scalar(
                    "UPDATE projects SET version = version + 1
                     WHERE project_id = $1 RETURNING version",
                )
                .bind(project_id)
                .fetch_one(&mut *tx)
                .await?;
                let global: i64 = sqlx::query_scalar(
                    "UPDATE global_seq SET version = version + 1 WHERE id = 1 RETURNING version",
                )
                .fetch_one(&mut *tx)
                .await?;
                let server_now_ms = transaction_clock_ms(&mut tx).await?;
                tx.commit().await?;
                Ok(RestoreMutationOutcome {
                    kind: RestoreMutationKind::Restored,
                    // The locked pre-update row has all event metadata.
                    row: Some(before),
                    server_now_ms,
                    bumped_run: Some(restored_run_version as u64),
                    bumped_project: Some(project as u64),
                    bumped_global: Some(global as u64),
                })
            }
            _ => {
                tx.commit().await?;
                Ok(RestoreMutationOutcome {
                    kind: RestoreMutationKind::Expired,
                    row: None,
                    server_now_ms: now_ms,
                    bumped_run: None,
                    bumped_project: None,
                    bumped_global: None,
                })
            }
        }
    }

    pub async fn list_trash(&self, query: TrashListQuery) -> Result<TrashListPage> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *tx)
            .await?;
        let (global_version, server_now_ms): (i64, i64) = sqlx::query_as(
            "SELECT version, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
             FROM global_seq WHERE id = 1",
        )
        .fetch_one(&mut *tx)
        .await?;
        let total_count: Option<i64> = if matches!(&query, TrashListQuery::Page { after: None, .. })
        {
            Some(
                sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE deleted_at IS NOT NULL")
                    .fetch_one(&mut *tx)
                    .await?,
            )
        } else {
            None
        };

        let mut rows: Vec<RunRecordRow> = match &query {
            TrashListQuery::Identities {
                project_id,
                run_ids,
            } => {
                sqlx::query_as(run_record_sql!(
                    "SELECT",
                    " FROM runs
                     WHERE deleted_at IS NOT NULL
                       AND project_id = $1 AND run_id = ANY($2)"
                ))
                .bind(project_id)
                .bind(run_ids)
                .fetch_all(&mut *tx)
                .await?
            }
            TrashListQuery::Page {
                page_size,
                after: Some(after),
            } => {
                let deleted_at_ms = after.purge_at_ms.saturating_sub(TRASH_RETENTION_MS);
                sqlx::query_as(run_record_sql!(
                    "SELECT",
                    " FROM runs
                     WHERE deleted_at IS NOT NULL
                       AND (
                         deleted_at < to_timestamp($1::DOUBLE PRECISION / 1000.0)
                         OR (deleted_at = to_timestamp($1::DOUBLE PRECISION / 1000.0) AND project_id > $2)
                         OR (deleted_at = to_timestamp($1::DOUBLE PRECISION / 1000.0) AND project_id = $2 AND ordinal < $3)
                         OR (deleted_at = to_timestamp($1::DOUBLE PRECISION / 1000.0) AND project_id = $2 AND ordinal = $3 AND run_id > $4)
                       )
                     ORDER BY deleted_at DESC, project_id, ordinal DESC, run_id
                     LIMIT $5"
                ))
                .bind(deleted_at_ms)
                .bind(&after.project_id)
                .bind(after.ordinal)
                .bind(&after.run_id)
                .bind((*page_size + 1) as i64)
                .fetch_all(&mut *tx)
                .await?
            }
            TrashListQuery::Page {
                page_size,
                after: None,
            } => {
                sqlx::query_as(run_record_sql!(
                    "SELECT",
                    " FROM runs
                     WHERE deleted_at IS NOT NULL
                     ORDER BY deleted_at DESC, project_id, ordinal DESC, run_id
                     LIMIT $1"
                ))
                .bind((*page_size + 1) as i64)
                .fetch_all(&mut *tx)
                .await?
            }
        };

        let next = match &query {
            TrashListQuery::Page { page_size, .. } if rows.len() > *page_size => {
                rows.truncate(*page_size);
                rows.last().and_then(|row| {
                    row.deleted_at_ms.map(|deleted_at_ms| TrashPageCursor {
                        purge_at_ms: deleted_at_ms.saturating_add(TRASH_RETENTION_MS),
                        project_id: row.project_id.clone(),
                        ordinal: row.ordinal,
                        run_id: row.run_id.clone(),
                    })
                })
            }
            _ => None,
        };
        tx.commit().await?;
        Ok(TrashListPage {
            rows,
            snapshot: LifecycleSnapshot {
                global_version: global_version as u64,
                server_now_ms,
            },
            total_count: total_count.map(|count| count as u64),
            next,
        })
    }

    /// Refuse to start after an upgrade if the newly reserved synthetic route
    /// collides with pre-existing Postgres data. Startup also checks the
    /// ClickHouse source directly to catch raw-only historical or orphan data.
    pub async fn ensure_reserved_project_absent(&self, project_id: &str) -> Result<()> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM projects WHERE project_id = $1)
                 OR EXISTS(SELECT 1 FROM run_metrics WHERE project_id = $1)",
        )
        .bind(project_id)
        .fetch_one(&self.pool)
        .await?;
        if exists {
            anyhow::bail!(
                "project id {project_id:?} is reserved for the Trash route but already exists in Postgres; move that project to another id in both Postgres and ClickHouse before starting this server"
            );
        }
        Ok(())
    }

    pub async fn get_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> Result<Option<(RunRecordRow, LifecycleSnapshot)>> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *tx)
            .await?;
        let (global_version, server_now_ms): (i64, i64) = sqlx::query_as(
            "SELECT version, (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT
             FROM global_seq WHERE id = 1",
        )
        .fetch_one(&mut *tx)
        .await?;
        let row: Option<RunRecordRow> = sqlx::query_as(run_record_sql!(
            "SELECT",
            " FROM runs WHERE project_id = $1 AND run_id = $2"
        ))
        .bind(project_id)
        .bind(run_id)
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(row.map(|row| {
            (
                row,
                LifecycleSnapshot {
                    global_version: global_version as u64,
                    server_now_ms,
                },
            )
        }))
    }

    /// Current physically-deletable backlog and age of its oldest recovery
    /// deadline. This is operational telemetry, not a work-selection query.
    pub async fn purge_backlog(&self) -> Result<(u64, f64)> {
        let (count, oldest_age_seconds): (i64, f64) = sqlx::query_as(
            "SELECT COUNT(*)::BIGINT,
                    GREATEST(
                      COALESCE(EXTRACT(EPOCH FROM (
                        clock_timestamp() - MIN(
                          deleted_at + ($1::BIGINT * INTERVAL '1 millisecond')
                        )
                      )), 0),
                      0
                    )::DOUBLE PRECISION
             FROM runs
             WHERE deleted_at IS NOT NULL
               AND (purging_at IS NOT NULL
                    OR deleted_at + ($1::BIGINT * INTERVAL '1 millisecond')
                       <= clock_timestamp())",
        )
        .bind(TRASH_RETENTION_MS)
        .fetch_one(&self.pool)
        .await?;
        Ok((count as u64, oldest_age_seconds))
    }

    /// Select work that was ready before this pass began and has not already
    /// been selected during it. Ordering by the oldest deadline or attempt
    /// gives both new work and retries eventual service. The limit is an
    /// internal reaper work bound, never a public TrashRuns request cap.
    pub async fn purge_candidates(&self, limit: i64, pass_started_ms: i64) -> Result<Vec<RunKey>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "WITH ready AS (
                 (
                     SELECT project_id, run_id, purge_attempt_at AS queue_at
                     FROM runs
                     WHERE deleted_at IS NOT NULL
                       AND purge_attempt_at IS NOT NULL
                       AND purge_attempt_at
                           < to_timestamp($2::DOUBLE PRECISION / 1000.0)
                     ORDER BY purge_attempt_at, project_id, run_id
                     LIMIT $1
                 )
                 UNION ALL
                 (
                     SELECT project_id, run_id,
                            deleted_at + ($3::BIGINT * INTERVAL '1 millisecond') AS queue_at
                     FROM runs
                     WHERE deleted_at IS NOT NULL
                       AND purge_attempt_at IS NULL
                       AND deleted_at
                           <= to_timestamp($2::DOUBLE PRECISION / 1000.0)
                              - ($3::BIGINT * INTERVAL '1 millisecond')
                     ORDER BY deleted_at, project_id, run_id
                     LIMIT $1
                 )
             )
             SELECT project_id, run_id
             FROM ready
             ORDER BY queue_at, project_id, run_id
             LIMIT $1",
        )
        .bind(limit.max(0))
        .bind(pass_started_ms)
        .bind(TRASH_RETENTION_MS)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(p, r)| RunKey::new(p, r)).collect())
    }

    pub async fn claim_purge_runs(&self, keys: &[RunKey]) -> Result<PurgeClaimOutcome> {
        if keys.is_empty() {
            return Ok(PurgeClaimOutcome {
                rows: Vec::new(),
                terminal_versions: Vec::new(),
                bumped_global: None,
            });
        }
        let project_ids: Vec<&str> = keys.iter().map(|key| key.project_id.as_str()).collect();
        let run_ids: Vec<&str> = keys.iter().map(|key| key.run_id.as_str()).collect();
        let mut tx = self.pool.begin().await?;
        let newly_claimed: Vec<(String, String)> = sqlx::query_as(
            "UPDATE runs SET purging_at = clock_timestamp()
             FROM UNNEST($1::TEXT[], $2::TEXT[]) AS wanted(project_id, run_id)
             WHERE runs.project_id = wanted.project_id
               AND runs.run_id = wanted.run_id
               AND runs.purging_at IS NULL
               AND runs.deleted_at IS NOT NULL
               AND runs.deleted_at + ($3::BIGINT * INTERVAL '1 millisecond') <= NOW()
             RETURNING runs.project_id, runs.run_id",
        )
        .bind(&project_ids)
        .bind(&run_ids)
        .bind(TRASH_RETENTION_MS)
        .fetch_all(&mut *tx)
        .await?;
        let rows: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT runs.project_id, runs.run_id,
                    LEAST(runs.version, $3) + 1 AS terminal_version
             FROM runs
             JOIN UNNEST($1::TEXT[], $2::TEXT[]) AS wanted(project_id, run_id)
               ON runs.project_id = wanted.project_id AND runs.run_id = wanted.run_id
             WHERE runs.purging_at IS NOT NULL",
        )
        .bind(&project_ids)
        .bind(&run_ids)
        .bind(MAX_PRE_TERMINAL_RUN_VERSION)
        .fetch_all(&mut *tx)
        .await?;
        let bumped_global = if newly_claimed.is_empty() {
            None
        } else {
            let version: i64 = sqlx::query_scalar(
                "UPDATE global_seq SET version = version + 1 WHERE id = 1 RETURNING version",
            )
            .fetch_one(&mut *tx)
            .await?;
            Some(version as u64)
        };
        tx.commit().await?;
        let terminal_versions = rows
            .iter()
            .map(|(_, run_id, version)| (run_id.clone(), *version as u64))
            .collect();
        Ok(PurgeClaimOutcome {
            rows: rows
                .into_iter()
                .map(|(project_id, run_id, _)| RunKey::new(project_id, run_id))
                .collect(),
            terminal_versions,
            bumped_global,
        })
    }

    /// Move attempted cleanup work behind older queue entries without changing
    /// lifecycle state. This may stamp an expired, unclaimed row after a gate
    /// timeout, but it does not set `purging_at`; claiming still requires the
    /// exclusive run gate and the transactional lifecycle recheck.
    pub async fn note_purge_attempts(&self, keys: &[RunKey], pass_started_ms: i64) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let project_ids: Vec<&str> = keys.iter().map(|key| key.project_id.as_str()).collect();
        let run_ids: Vec<&str> = keys.iter().map(|key| key.run_id.as_str()).collect();
        sqlx::query(
            "UPDATE runs
             SET purge_attempt_at = GREATEST(
                     clock_timestamp(),
                     to_timestamp($3::DOUBLE PRECISION / 1000.0)
                 )
             FROM UNNEST($1::TEXT[], $2::TEXT[]) AS wanted(project_id, run_id)
             WHERE runs.project_id = wanted.project_id
               AND runs.run_id = wanted.run_id
               AND (runs.purging_at IS NOT NULL
                    OR (runs.deleted_at IS NOT NULL
                        AND runs.deleted_at + ($4::BIGINT * INTERVAL '1 millisecond')
                            <= to_timestamp($3::DOUBLE PRECISION / 1000.0)))",
        )
        .bind(&project_ids)
        .bind(&run_ids)
        .bind(pass_started_ms)
        .bind(TRASH_RETENTION_MS)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// One project's batch: the reaper groups claimed runs by project.
    pub async fn finalize_purged_runs(
        &self,
        project_id: &str,
        run_ids: &[&str],
    ) -> Result<Option<u64>> {
        if run_ids.is_empty() {
            return Ok(None);
        }
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO purged_runs (project_id, run_id, terminal_version)
             SELECT project_id, run_id, LEAST(version, $3) + 1
             FROM runs
             WHERE project_id = $1 AND run_id = ANY($2) AND purging_at IS NOT NULL
             ON CONFLICT (project_id, run_id) DO NOTHING",
        )
        .bind(project_id)
        .bind(run_ids)
        .bind(MAX_PRE_TERMINAL_RUN_VERSION)
        .execute(&mut *tx)
        .await?;
        let deleted = sqlx::query(
            "DELETE FROM runs
             WHERE project_id = $1 AND run_id = ANY($2) AND purging_at IS NOT NULL",
        )
        .bind(project_id)
        .bind(run_ids)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        let bumped_global = if deleted == 0 {
            None
        } else {
            // After the DELETE and its foreign-key cascades, so the project_activity row is locked only once the runs are gone.
            sqlx::query(
                "UPDATE project_activity SET run_rows = run_rows - $2 WHERE project_id = $1",
            )
            .bind(project_id)
            .bind(deleted as i64)
            .execute(&mut *tx)
            .await?;
            let version: i64 = sqlx::query_scalar(
                "UPDATE global_seq SET version = version + 1 WHERE id = 1 RETURNING version",
            )
            .fetch_one(&mut *tx)
            .await?;
            Some(version as u64)
        };
        tx.commit().await?;
        Ok(bumped_global)
    }

    /// Fence delayed write-behind registry work against physical run purge.
    /// `NOT VALID` preserves any historical registry rows without canonical
    /// metadata, while all current writes are checked immediately.
    pub async fn ensure_run_metrics_run_fk(&self) -> Result<()> {
        sqlx::query(
            "DO $$
             BEGIN
               IF NOT EXISTS (
                 SELECT 1 FROM pg_constraint
                 WHERE conname = 'run_metrics_run_fk'
                   AND conrelid = 'run_metrics'::regclass
               ) THEN
                 ALTER TABLE run_metrics
                   ADD CONSTRAINT run_metrics_run_fk
                   FOREIGN KEY (project_id, run_id)
                   REFERENCES runs(project_id, run_id)
                   ON DELETE CASCADE
                   NOT VALID;
               END IF;
             END $$",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Three-level change-detection poll. Returns global_version (always), project_version (if `project_id` provided), and effective per-run versions for the requested run_ids. Expired and purged identities retain one final monotonic generation so cached data can be invalidated.
    pub async fn poll_versions(
        &self,
        project_id: Option<&str>,
        run_ids: &[String],
    ) -> Result<PollVersionsResult> {
        let global_version: i64 = sqlx::query_scalar("SELECT version FROM global_seq WHERE id = 1")
            .fetch_one(&self.pool)
            .await?;

        let project_version: i64 = match project_id {
            Some(pid) => {
                sqlx::query_scalar(
                    "SELECT COALESCE((SELECT version FROM projects WHERE project_id = $1), 0)",
                )
                .bind(pid)
                .fetch_one(&self.pool)
                .await?
            }
            None => 0,
        };

        let mut run_versions: HashMap<String, u64> = HashMap::new();
        if !run_ids.is_empty() {
            if let Some(pid) = project_id {
                let rows: Vec<(String, i64)> = sqlx::query_as(
                    "SELECT wanted.run_id,
                            COALESCE(
                              CASE
                                WHEN runs.purging_at IS NOT NULL
                                  OR (runs.deleted_at IS NOT NULL
                                      AND runs.deleted_at
                                          + ($3::BIGINT * INTERVAL '1 millisecond')
                                          <= clock_timestamp())
                                THEN LEAST(runs.version, $4) + 1
                                ELSE runs.version
                              END,
                              purged_runs.terminal_version,
                              0
                            ) AS version
                     FROM UNNEST($2::TEXT[]) AS wanted(run_id)
                     LEFT JOIN runs ON runs.project_id = $1
                                   AND runs.run_id = wanted.run_id
                     LEFT JOIN purged_runs ON purged_runs.project_id = $1
                                           AND purged_runs.run_id = wanted.run_id",
                )
                .bind(pid)
                .bind(run_ids)
                .bind(TRASH_RETENTION_MS)
                .bind(MAX_PRE_TERMINAL_RUN_VERSION)
                .fetch_all(&self.pool)
                .await?;
                for (rid, ver) in rows {
                    run_versions.insert(rid, ver as u64);
                }
            }
            // Project-less requests and any accidentally omitted identities retain the wire contract of an explicit zero.
            for rid in run_ids {
                run_versions.entry(rid.clone()).or_insert(0);
            }
        }

        Ok(PollVersionsResult {
            global_version: global_version as u64,
            project_version: project_version as u64,
            run_versions,
        })
    }
}

pub struct PollVersionsResult {
    pub global_version: u64,
    pub project_version: u64,
    pub run_versions: HashMap<String, u64>,
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    fn record(deleted_at_ms: Option<i64>, purging_at_ms: Option<i64>) -> RunRecordRow {
        RunRecordRow {
            project_id: "p".into(),
            run_id: "r".into(),
            run_name: "run".into(),
            ordinal: 1,
            created_at_ms: 0,
            last_main_metric_at_ms: None,
            last_system_metric_at_ms: None,
            last_ingested_at_ms: None,
            terminated_at_ms: None,
            exit_code: None,
            deleted_at_ms,
            purging_at_ms,
        }
    }

    #[test]
    fn seven_day_boundary_is_exclusive() {
        assert_eq!(
            record(None, None).lifecycle_at(123),
            RunLifecycleClass::Active
        );
        assert_eq!(
            record(Some(1_000), None).lifecycle_at(1_000 + TRASH_RETENTION_MS - 1),
            RunLifecycleClass::Trashed
        );
        assert_eq!(
            record(Some(1_000), None).lifecycle_at(1_000 + TRASH_RETENTION_MS),
            RunLifecycleClass::Expired
        );
        assert_eq!(
            record(Some(1_000), Some(2_000)).lifecycle_at(1_001),
            RunLifecycleClass::Purging
        );
    }

    #[test]
    fn lifecycle_precedence_is_shared_by_rows_and_lookup_results() {
        assert_eq!(
            classify_lifecycle(false, false, None, None, false),
            RunLifecycleClass::Missing
        );
        assert_eq!(
            classify_lifecycle(false, true, None, None, false),
            RunLifecycleClass::Purged
        );
        assert_eq!(
            classify_lifecycle(true, true, None, None, false),
            RunLifecycleClass::Active
        );
        assert_eq!(
            classify_lifecycle(true, false, Some(1_000), None, false),
            RunLifecycleClass::Trashed
        );
        assert_eq!(
            classify_lifecycle(true, false, Some(1_000), None, true),
            RunLifecycleClass::Expired
        );
        assert_eq!(
            classify_lifecycle(true, false, Some(1_000), Some(2_000), false),
            RunLifecycleClass::Purging
        );
    }

    #[test]
    fn duplicate_trash_positions_report_only_one_transition() {
        let active = record(None, None);
        let run_ids = vec![active.run_id.clone(); 257];
        let before_by_id = HashMap::from([(active.run_id.clone(), active)]);

        let results = classify_trash_results(&run_ids, &before_by_id, &HashSet::new(), 123);

        assert_eq!(results.len(), run_ids.len());
        assert_eq!(results[0].kind, TrashMutationKind::Trashed);
        assert!(results[1..]
            .iter()
            .all(|result| result.kind == TrashMutationKind::AlreadyTrashed));
    }

    #[test]
    fn restore_generation_is_strictly_newer_than_terminal_generation() {
        for current in [0, 1, MAX_RESTORABLE_RUN_VERSION] {
            let terminal = current.min(MAX_PRE_TERMINAL_RUN_VERSION) + 1;
            let restored = current
                .checked_add(RESTORE_VERSION_INCREMENT)
                .expect("guarded counter has headroom");
            assert!(restored > terminal);
        }
        assert_eq!(
            MAX_RESTORABLE_RUN_VERSION.checked_add(RESTORE_VERSION_INCREMENT),
            Some(i64::MAX)
        );
        assert!((MAX_RESTORABLE_RUN_VERSION + 1)
            .checked_add(RESTORE_VERSION_INCREMENT)
            .is_none());
    }

    #[tokio::test]
    async fn registry_rejects_unknown_types_before_querying_postgres() {
        let error = PgStore::test_store()
            .register_run_metrics(&[(
                "project".into(),
                "run".into(),
                "metric".into(),
                "BOGUS".into(),
            )])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("invalid metric registry type"));
    }
}

#[cfg(test)]
mod live_pg_tests {
    use super::*;
    use anyhow::Context as _;

    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL"]
    async fn live_list_runs_version_handles_empty_and_concurrent_snapshots() -> Result<()> {
        let _suite_guard = live_database_suite_gate().lock().await;
        let pg_url = std::env::var("KYMO_LIVE_TEST_DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("KYMO_LIVE_TEST_DATABASE_URL is required"))?;
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        );
        let project_id = format!("list-snapshot-{suffix}");
        let run_id = format!("run-{suffix}");
        let store = PgStore::connect(&pg_url).await?;

        let test_result: Result<()> = async {
            let missing = store.list_runs(&project_id).await?;
            anyhow::ensure!(missing.rows.is_empty());
            anyhow::ensure!(missing.project_version.is_none());

            sqlx::query("INSERT INTO projects (project_id, version) VALUES ($1, 7)")
                .bind(&project_id)
                .execute(&store.pool)
                .await?;
            let empty = store.list_runs(&project_id).await?;
            anyhow::ensure!(empty.rows.is_empty());
            anyhow::ensure!(empty.project_version.is_none());

            let initialized = store.init_run(&project_id, &run_id, "0", None).await?;
            let initial_version = initialized.bumped_project;
            let start = tokio::sync::Barrier::new(2);
            // Every committed rename makes the name exactly the offset from the initial project version. Overlap real production reads and writes: a response may show either side, never a mixed pair.
            tokio::time::timeout(Duration::from_secs(30), async {
                let writer = async {
                    start.wait().await;
                    for generation in 1..=64 {
                        let renamed = store
                            .rename_run(&project_id, &run_id, &generation.to_string())
                            .await?
                            .context("rename lost the snapshot fixture")?;
                        anyhow::ensure!(
                            renamed.bumped_project == Some(initial_version + generation)
                        );
                    }
                    Ok::<_, anyhow::Error>(())
                };
                let reader = async {
                    start.wait().await;
                    for _ in 0..128 {
                        let snapshot = store.list_runs(&project_id).await?;
                        anyhow::ensure!(snapshot.rows.len() == 1);
                        let project_version = snapshot
                            .project_version
                            .context("nonempty list omitted its version")?;
                        anyhow::ensure!(snapshot.server_now_ms > 0);
                        anyhow::ensure!(
                            snapshot.rows[0].run_name
                                == (project_version - initial_version).to_string(),
                            "ListRuns paired name {} with version {} (initial {})",
                            snapshot.rows[0].run_name,
                            project_version,
                            initial_version
                        );
                    }
                    Ok::<_, anyhow::Error>(())
                };
                tokio::try_join!(writer, reader)?;
                Ok::<_, anyhow::Error>(())
            })
            .await
            .context("concurrent snapshot test stalled")??;

            let final_snapshot = store.list_runs(&project_id).await?;
            anyhow::ensure!(final_snapshot.project_version == Some(initial_version + 64));
            anyhow::ensure!(final_snapshot.rows[0].run_name == "64");

            let second_run_id = format!("second-{suffix}");
            store
                .init_run(&project_id, &second_run_id, "second", None)
                .await?;
            let ordered = store.list_runs(&project_id).await?;
            anyhow::ensure!(ordered.project_version == Some(initial_version + 65));
            anyhow::ensure!(ordered.rows.len() == 2);
            anyhow::ensure!(ordered.rows[0].run_id == second_run_id);
            anyhow::ensure!(ordered.rows[1].run_id == run_id);

            // Empty active membership has no token even when PollVersions knows the deletion's bump, preserving the conservative covering refresh for trash-only projects.
            store
                .trash_runs_chunk(
                    &project_id,
                    &[run_id.clone(), second_run_id],
                    &HashMap::new(),
                )
                .await?;
            let trashed = store.list_runs(&project_id).await?;
            let polled = store.poll_versions(Some(&project_id), &[]).await?;
            anyhow::ensure!(trashed.rows.is_empty());
            anyhow::ensure!(trashed.project_version.is_none());
            anyhow::ensure!(polled.project_version == initial_version + 66);
            Ok(())
        }
        .await;
        let cleanup_runs = sqlx::query("DELETE FROM runs WHERE project_id = $1")
            .bind(&project_id)
            .execute(&store.pool)
            .await;
        let cleanup_run_ids = sqlx::query("DELETE FROM run_ids WHERE project_id = $1")
            .bind(&project_id)
            .execute(&store.pool)
            .await;
        let cleanup_project = sqlx::query("DELETE FROM projects WHERE project_id = $1")
            .bind(&project_id)
            .execute(&store.pool)
            .await;
        test_result?;
        cleanup_runs?;
        cleanup_run_ids?;
        cleanup_project?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL"]
    async fn live_rich_writer_epochs_and_mutation_cas_are_monotonic() -> Result<()> {
        let _suite_guard = live_database_suite_gate().lock().await;
        let pg_url = std::env::var("KYMO_LIVE_TEST_DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("KYMO_LIVE_TEST_DATABASE_URL is required"))?;
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let project_id = format!("rich-order-{suffix}");
        let run_id = format!("run-{suffix}");
        let store = PgStore::connect(&pg_url).await?;

        let test_result: Result<()> = async {
            let first = store.init_run(&project_id, &run_id, "first", None).await?;
            let second = store.init_run(&project_id, &run_id, "second", None).await?;
            anyhow::ensure!(second.writer_epoch == first.writer_epoch + 1);
            let low = (u64::from(first.writer_epoch) << 32) | 7;
            let high = (u64::from(second.writer_epoch) << 32) | 1;
            let unallocated = (u64::from(second.writer_epoch + 1) << 32) | 1;
            macro_rules! candidate {
                ($version:expr, $resource:expr) => {
                    RichMutationCandidate {
                        project_id: &project_id,
                        run_id: &run_id,
                        metric_name: "info/run_info",
                        tag: "",
                        step: 0,
                        mutation_version: $version,
                        public_resource_id: $resource,
                    }
                };
            }
            anyhow::ensure!(matches!(
                store
                    .compare_rich_mutation(candidate!(unallocated, "future.json"))
                    .await?,
                RichMutationDecision::UnallocatedEpoch { current_epoch }
                    if current_epoch == second.writer_epoch
            ));
            anyhow::ensure!(matches!(
                store
                    .compare_rich_mutation(candidate!(low, "old.json"))
                    .await?,
                RichMutationDecision::Accepted
            ));
            anyhow::ensure!(matches!(
                store
                    .compare_rich_mutation(candidate!(low, "old.json"))
                    .await?,
                RichMutationDecision::Idempotent
            ));
            anyhow::ensure!(matches!(
                store
                    .compare_rich_mutation(candidate!(low, "different.json"))
                    .await?,
                RichMutationDecision::Conflict { .. }
            ));
            anyhow::ensure!(matches!(
                store
                    .compare_rich_mutation(candidate!(high, "new.json"))
                    .await?,
                RichMutationDecision::Accepted
            ));
            anyhow::ensure!(matches!(
                store
                    .compare_rich_mutation(candidate!(low, "old.json"))
                    .await?,
                RichMutationDecision::Superseded { stored_version } if stored_version == high
            ));
            Ok(())
        }
        .await;
        let cleanup_runs = sqlx::query("DELETE FROM runs WHERE project_id = $1")
            .bind(&project_id)
            .execute(&store.pool)
            .await;
        let cleanup_project = sqlx::query("DELETE FROM projects WHERE project_id = $1")
            .bind(&project_id)
            .execute(&store.pool)
            .await;
        test_result?;
        cleanup_runs?;
        cleanup_project?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL"]
    async fn live_import_run_backdates_and_finalize_converges() -> Result<()> {
        let _suite_guard = live_database_suite_gate().lock().await;
        let pg_url = std::env::var("KYMO_LIVE_TEST_DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("KYMO_LIVE_TEST_DATABASE_URL is required"))?;
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let project_id = format!("import-{suffix}");
        let run_id = format!("run-{suffix}");
        let store = PgStore::connect(&pg_url).await?;

        // Whole seconds so EXTRACT(EPOCH)*1000::BIGINT reads back exactly.
        const CREATED_MS: i64 = 1_680_000_000_000;
        const END_MS: i64 = 1_680_003_700_000;
        const CORRECTED_END_MS: i64 = 1_680_003_800_000;

        let test_result: Result<()> = async {
            let created = store
                .init_run(&project_id, &run_id, "imported", Some(CREATED_MS))
                .await?;
            anyhow::ensure!(created.row.created_at_ms == CREATED_MS);
            anyhow::ensure!(created.row.last_main_metric_at_ms.is_none());

            // Idempotent re-import: stored identity wins wholesale, and no
            // liveness baseline appears (a replayed run must never look live).
            let again = store
                .init_run(&project_id, &run_id, "renamed", Some(CREATED_MS + 5_000))
                .await?;
            anyhow::ensure!(again.row.created_at_ms == CREATED_MS);
            anyhow::ensure!(again.row.run_name == "imported");
            anyhow::ensure!(again.bumped_run.is_none());
            anyhow::ensure!(again.row.last_main_metric_at_ms.is_none());
            anyhow::ensure!(again.row.exit_code.is_none());

            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as i64;
            let finalized = store
                .terminate_run(&project_id, &run_id, 1, Some(now_ms), Some(END_MS))
                .await?
                .context("finalize should find the run")?;
            let row = store
                .list_runs(&project_id)
                .await?
                .rows
                .into_iter()
                .find(|row| row.run_id == run_id)
                .context("finalize hid the fixture run")?;
            anyhow::ensure!(row.exit_code == Some(1));
            anyhow::ensure!(row.terminated_at_ms == Some(END_MS));
            // Heartbeat columns stay NULL for imported runs: the data lane
            // never marks them and finalize leaves them alone.
            anyhow::ensure!(row.last_main_metric_at_ms.is_none());
            anyhow::ensure!(row.last_system_metric_at_ms.is_none());
            anyhow::ensure!(row.last_ingested_at_ms.is_some());

            // Unlike live TerminateRun's COALESCE-keep, an archived end time is
            // set unconditionally so a re-finalize converges on it.
            let corrected = store
                .terminate_run(
                    &project_id,
                    &run_id,
                    0,
                    Some(now_ms),
                    Some(CORRECTED_END_MS),
                )
                .await?
                .context("re-finalize should find the run")?;
            let row = store
                .list_runs(&project_id)
                .await?
                .rows
                .into_iter()
                .find(|row| row.run_id == run_id)
                .context("re-finalize hid the fixture run")?;
            anyhow::ensure!(row.exit_code == Some(0));
            anyhow::ensure!(row.terminated_at_ms == Some(CORRECTED_END_MS));
            anyhow::ensure!(row.last_main_metric_at_ms.is_none());
            anyhow::ensure!(row.last_system_metric_at_ms.is_none());
            anyhow::ensure!(corrected.bumped_run > finalized.bumped_run);
            Ok(())
        }
        .await;
        let cleanup_metrics = sqlx::query("DELETE FROM run_metrics WHERE project_id = $1")
            .bind(&project_id)
            .execute(&store.pool)
            .await;
        let cleanup_runs = sqlx::query("DELETE FROM runs WHERE project_id = $1")
            .bind(&project_id)
            .execute(&store.pool)
            .await;
        let cleanup_project = sqlx::query("DELETE FROM projects WHERE project_id = $1")
            .bind(&project_id)
            .execute(&store.pool)
            .await;
        test_result?;
        cleanup_metrics?;
        cleanup_runs?;
        cleanup_project?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL"]
    async fn live_status_watch_candidates_accepts_shared_window_bind() -> Result<()> {
        let _suite_guard = live_database_suite_gate().lock().await;
        let pg_url = std::env::var("KYMO_LIVE_TEST_DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("KYMO_LIVE_TEST_DATABASE_URL is required"))?;
        let store = PgStore::connect(&pg_url).await?;
        store.status_watch_candidates().await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL"]
    async fn live_run_id_claim_is_global_concurrent_and_idempotent() -> Result<()> {
        let _suite_guard = live_database_suite_gate().lock().await;
        let pg_url = std::env::var("KYMO_LIVE_TEST_DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("KYMO_LIVE_TEST_DATABASE_URL is required"))?;
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let project_a = format!("run-owner-a-{suffix}");
        let project_b = format!("run-owner-b-{suffix}");
        let run_id = format!("global-run-{suffix}");
        let store = PgStore::connect(&pg_url).await?;

        let test_result: Result<()> = async {
            let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
            let barrier_a = barrier.clone();
            let initialize_a = async {
                barrier_a.wait().await;
                store.init_run(&project_a, &run_id, "owner a", None).await
            };
            let initialize_b = async {
                barrier.wait().await;
                store.init_run(&project_b, &run_id, "owner b", None).await
            };
            let (result_a, result_b) = tokio::join!(initialize_a, initialize_b);

            let (owner_project, conflicting_project, conflict) = match (result_a, result_b) {
                (Ok(_), Err(error)) => (&project_a, &project_b, error),
                (Err(error), Ok(_)) => (&project_b, &project_a, error),
                (Ok(_), Ok(_)) => anyhow::bail!("both projects claimed the same run id"),
                (Err(a), Err(b)) => {
                    anyhow::bail!("neither project claimed the run id: {a}; {b}")
                }
            };
            anyhow::ensure!(
                matches!(
                    conflict,
                    InitRunError::RunIdOwned {
                        run_id: ref claimed_run_id,
                        ref requested_project_id,
                    } if claimed_run_id == &run_id && requested_project_id == conflicting_project
                ),
                "loser returned the wrong error: {conflict}"
            );

            let retry = store
                .init_run(owner_project, &run_id, "retry", None)
                .await?;
            let expected_name = if owner_project == &project_a {
                "owner a"
            } else {
                "owner b"
            };
            anyhow::ensure!(
                retry.row.run_name == expected_name,
                "same-owner retry replaced the canonical run name"
            );
            anyhow::ensure!(
                retry.row.last_main_metric_at_ms.is_some(),
                "same-owner retry did not refresh its liveness baseline"
            );
            anyhow::ensure!(
                retry.row.last_system_metric_at_ms.is_none(),
                "same-owner retry retained the previous execution's system heartbeat"
            );
            let stored_owner: String =
                sqlx::query_scalar("SELECT project_id FROM run_ids WHERE run_id = $1")
                    .bind(&run_id)
                    .fetch_one(&store.pool)
                    .await?;
            anyhow::ensure!(stored_owner == *owner_project);

            sqlx::query(
                "UPDATE runs SET purging_at = NOW()
                 WHERE project_id = $1 AND run_id = $2",
            )
            .bind(owner_project)
            .bind(&run_id)
            .execute(&store.pool)
            .await?;
            anyhow::ensure!(
                store
                    .finalize_purged_runs(owner_project, &[run_id.as_str()])
                    .await?
                    .is_some(),
                "fixture purge was not finalized"
            );
            let reservation_survived: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                    SELECT 1 FROM run_ids WHERE run_id = $1 AND project_id = $2
                 )",
            )
            .bind(&run_id)
            .bind(owner_project)
            .fetch_one(&store.pool)
            .await?;
            anyhow::ensure!(reservation_survived, "purge deleted global ownership");

            let post_purge_error = match store
                .init_run(conflicting_project, &run_id, "post-purge reuse", None)
                .await
            {
                Err(error) => error,
                Ok(_) => anyhow::bail!("purge allowed run-ID reuse through InitRun"),
            };
            anyhow::ensure!(matches!(
                post_purge_error,
                InitRunError::RunIdOwned {
                    run_id: ref claimed_run_id,
                    ref requested_project_id,
                } if claimed_run_id == &run_id && requested_project_id == conflicting_project
            ));
            Ok(())
        }
        .await;

        let cleanup_result: Result<()> = async {
            sqlx::query("DELETE FROM runs WHERE project_id = ANY($1)")
                .bind(vec![project_a.clone(), project_b.clone()])
                .execute(&store.pool)
                .await?;
            sqlx::query("DELETE FROM purged_runs WHERE run_id = $1")
                .bind(&run_id)
                .execute(&store.pool)
                .await?;
            sqlx::query("DELETE FROM projects WHERE project_id = ANY($1)")
                .bind(vec![project_a, project_b])
                .execute(&store.pool)
                .await?;
            sqlx::query("DELETE FROM run_ids WHERE run_id = $1")
                .bind(&run_id)
                .execute(&store.pool)
                .await?;
            Ok(())
        }
        .await;
        test_result?;
        cleanup_result?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL"]
    async fn live_run_id_schema_backfills_and_aborts_ambiguous_history() -> Result<()> {
        let _suite_guard = live_database_suite_gate().lock().await;
        let pg_url = std::env::var("KYMO_LIVE_TEST_DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("KYMO_LIVE_TEST_DATABASE_URL is required"))?;
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let project_a = format!("ownership-migration-a-{suffix}");
        let project_b = format!("ownership-migration-b-{suffix}");
        let active_run = format!("ownership-active-{suffix}");
        let purged_run = format!("ownership-purged-{suffix}");
        let duplicate_run = format!("ownership-duplicate-{suffix}");
        let store = PgStore::connect(&pg_url).await?;

        let test_result: Result<()> = async {
            sqlx::query("DROP TABLE run_ids")
                .execute(&store.pool)
                .await?;
            for project_id in [&project_a, &project_b] {
                sqlx::query(
                    "INSERT INTO projects (project_id) VALUES ($1)
                     ON CONFLICT (project_id) DO NOTHING",
                )
                .bind(project_id)
                .execute(&store.pool)
                .await?;
            }
            sqlx::query(
                "INSERT INTO runs (project_id, run_id, run_name, ordinal)
                 VALUES ($1, $2, 'active fixture', 1)",
            )
            .bind(&project_a)
            .bind(&active_run)
            .execute(&store.pool)
            .await?;
            sqlx::query(
                "INSERT INTO purged_runs (project_id, run_id)
                 VALUES ($1, $2)",
            )
            .bind(&project_b)
            .bind(&purged_run)
            .execute(&store.pool)
            .await?;

            store.ensure_schema().await?;
            let owners: Vec<(String, String)> = sqlx::query_as(
                "SELECT run_id, project_id FROM run_ids
                 WHERE run_id = ANY($1) ORDER BY run_id",
            )
            .bind(vec![active_run.clone(), purged_run.clone()])
            .fetch_all(&store.pool)
            .await?;
            anyhow::ensure!(
                owners
                    == vec![
                        (active_run.clone(), project_a.clone()),
                        (purged_run.clone(), project_b.clone()),
                    ]
            );

            sqlx::query("UPDATE run_ids SET project_id = $2 WHERE run_id = $1")
                .bind(&active_run)
                .bind(&project_b)
                .execute(&store.pool)
                .await?;
            let error = store
                .ensure_schema()
                .await
                .expect_err("a wrong registry owner passed ownership verification");
            anyhow::ensure!(
                error
                    .to_string()
                    .contains(&format!("1 run(s), first {active_run}")),
                "unexpected ownership error: {error}"
            );

            sqlx::query("DROP TABLE run_ids")
                .execute(&store.pool)
                .await?;
            sqlx::query(
                "INSERT INTO runs (project_id, run_id, run_name, ordinal)
                 VALUES ($1, $3, 'duplicate a', 2),
                        ($2, $3, 'duplicate b', 1)",
            )
            .bind(&project_a)
            .bind(&project_b)
            .bind(&duplicate_run)
            .execute(&store.pool)
            .await?;

            let error = store
                .ensure_schema()
                .await
                .expect_err("ambiguous history passed ownership migration");
            anyhow::ensure!(error.to_string().contains("cross-project duplicate"));
            let registry_exists: bool =
                sqlx::query_scalar("SELECT to_regclass('public.run_ids') IS NOT NULL")
                    .fetch_one(&store.pool)
                    .await?;
            anyhow::ensure!(
                !registry_exists,
                "failed ownership migration did not roll back"
            );
            Ok(())
        }
        .await;

        let cleanup_result: Result<()> = async {
            sqlx::query("DELETE FROM runs WHERE project_id = ANY($1)")
                .bind(vec![project_a.clone(), project_b.clone()])
                .execute(&store.pool)
                .await?;
            sqlx::query("DELETE FROM purged_runs WHERE project_id = ANY($1)")
                .bind(vec![project_a.clone(), project_b.clone()])
                .execute(&store.pool)
                .await?;
            let registry_exists: bool =
                sqlx::query_scalar("SELECT to_regclass('public.run_ids') IS NOT NULL")
                    .fetch_one(&store.pool)
                    .await?;
            if registry_exists {
                sqlx::query("DELETE FROM run_ids WHERE run_id = ANY($1)")
                    .bind(vec![active_run, purged_run, duplicate_run])
                    .execute(&store.pool)
                    .await?;
            }
            sqlx::query("DELETE FROM projects WHERE project_id = ANY($1)")
                .bind(vec![project_a, project_b])
                .execute(&store.pool)
                .await?;
            Ok(())
        }
        .await;
        // Always attempt both cleanup and schema restoration. The deliberate
        // duplicate must be gone before ensure_schema can reinstall the
        // registry for the rest of the shared live suite.
        let restore_result = store.ensure_schema().await;
        test_result?;
        cleanup_result?;
        restore_result?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL"]
    async fn live_registry_is_idempotent_upgrade_only_and_skips_orphans() -> Result<()> {
        let _suite_guard = live_database_suite_gate().lock().await;
        let pg_url = std::env::var("KYMO_LIVE_TEST_DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("KYMO_LIVE_TEST_DATABASE_URL is required"))?;
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let project_id = format!("registry-{suffix}");
        let run_id = format!("run-{suffix}");
        let orphan_run_id = format!("orphan-{suffix}");
        let store = PgStore::connect(&pg_url).await?;

        let test_result: Result<()> = async {
            store
                .init_run(&project_id, &run_id, "registry test", None)
                .await?;
            let cdn = (
                project_id.clone(),
                run_id.clone(),
                "metric".to_string(),
                "CDN".to_string(),
            );
            let orphan = (
                project_id.clone(),
                orphan_run_id.clone(),
                "ghost".to_string(),
                "TEXT_STREAM".to_string(),
            );
            anyhow::ensure!(
                store.register_run_metrics(&[cdn.clone(), orphan]).await? == vec![run_id.clone()]
            );
            anyhow::ensure!(store
                .register_run_metrics(std::slice::from_ref(&cdn))
                .await?
                .is_empty());

            let numeric = (
                project_id.clone(),
                run_id.clone(),
                "metric".to_string(),
                "NUMERIC".to_string(),
            );
            anyhow::ensure!(store.register_run_metrics(&[numeric]).await? == vec![run_id.clone()]);
            anyhow::ensure!(store.register_run_metrics(&[cdn]).await?.is_empty());

            let text = (
                project_id.clone(),
                run_id.clone(),
                "metric".to_string(),
                "TEXT_STREAM".to_string(),
            );
            anyhow::ensure!(store.register_run_metrics(&[text]).await? == vec![run_id.clone()]);

            let rows: Vec<(String, String, String)> = sqlx::query_as(
                "SELECT run_id, metric_name, metric_type
                 FROM run_metrics WHERE project_id = $1 ORDER BY run_id, metric_name",
            )
            .bind(&project_id)
            .fetch_all(&store.pool)
            .await?;
            anyhow::ensure!(
                rows == vec![(run_id.clone(), "metric".into(), "TEXT_STREAM".into())],
                "registry rows were {rows:?}"
            );
            Ok(())
        }
        .await;

        let cleanup_result: Result<()> = async {
            sqlx::query("DELETE FROM runs WHERE project_id = $1")
                .bind(&project_id)
                .execute(&store.pool)
                .await?;
            sqlx::query("DELETE FROM run_ids WHERE project_id = $1")
                .bind(&project_id)
                .execute(&store.pool)
                .await?;
            sqlx::query("DELETE FROM projects WHERE project_id = $1")
                .bind(&project_id)
                .execute(&store.pool)
                .await?;
            Ok(())
        }
        .await;
        test_result?;
        cleanup_result?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL"]
    async fn live_rename_versions_and_concurrent_lifecycle_lock_order() -> Result<()> {
        let _suite_guard = live_database_suite_gate().lock().await;
        let pg_url = std::env::var("KYMO_LIVE_TEST_DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("KYMO_LIVE_TEST_DATABASE_URL is required"))?;
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let project_id = format!("rename-{suffix}");
        let run_id = format!("run-{suffix}");
        let store = PgStore::connect(&pg_url).await?;

        let test_result: Result<()> = async {
            let initialized = store
                .init_run(&project_id, &run_id, "original", None)
                .await?;

            let renamed = store
                .rename_run(&project_id, &run_id, "renamed")
                .await?
                .ok_or_else(|| anyhow::anyhow!("rename did not find the fixture run"))?;
            anyhow::ensure!(renamed.row.run_name == "renamed");
            anyhow::ensure!(
                renamed.bumped_project == Some(initialized.bumped_project + 1),
                "rename project bump was {:?}, initialized at {}",
                renamed.bumped_project,
                initialized.bumped_project
            );

            let unchanged = store
                .rename_run(&project_id, &run_id, "renamed")
                .await?
                .ok_or_else(|| anyhow::anyhow!("idempotent rename lost the fixture run"))?;
            anyhow::ensure!(unchanged.row.run_name == "renamed");
            anyhow::ensure!(
                unchanged.bumped_project.is_none(),
                "idempotent rename unexpectedly bumped the project"
            );

            let final_ingested_at_ms = 1_700_000_000_123;
            let terminated = store
                .terminate_run(&project_id, &run_id, 0, Some(final_ingested_at_ms), None)
                .await?
                .ok_or_else(|| anyhow::anyhow!("termination did not find the fixture run"))?;
            anyhow::ensure!(terminated.run_name == "renamed");
            anyhow::ensure!(
                terminated.bumped_project == initialized.bumped_project + 2,
                "termination project bump was {}, initialized at {}",
                terminated.bumped_project,
                initialized.bumped_project
            );

            let versions = store
                .poll_versions(Some(&project_id), std::slice::from_ref(&run_id))
                .await?;
            anyhow::ensure!(versions.project_version == terminated.bumped_project);
            anyhow::ensure!(
                versions.run_versions.get(&run_id) == Some(&terminated.bumped_run),
                "stored run version did not match termination outcome"
            );
            let row = store
                .list_runs(&project_id)
                .await?
                .rows
                .into_iter()
                .find(|row| row.run_id == run_id)
                .ok_or_else(|| anyhow::anyhow!("terminated run disappeared"))?;
            anyhow::ensure!(row.last_ingested_at_ms == Some(final_ingested_at_ms));
            let first_terminated_at_ms = row
                .terminated_at_ms
                .context("termination did not record an end time")?;

            sqlx::query("SELECT pg_sleep(0.01)")
                .execute(&store.pool)
                .await?;
            let repeated_ingested_at_ms = final_ingested_at_ms + 1_000;
            let repeated_outcome = store
                .terminate_run(&project_id, &run_id, 9, Some(repeated_ingested_at_ms), None)
                .await?
                .context("repeat termination lost the fixture run")?;
            anyhow::ensure!(repeated_outcome.bumped_run == terminated.bumped_run + 1);
            anyhow::ensure!(repeated_outcome.bumped_project == terminated.bumped_project + 1);
            let repeated_versions = store
                .poll_versions(Some(&project_id), std::slice::from_ref(&run_id))
                .await?;
            anyhow::ensure!(repeated_versions.project_version == repeated_outcome.bumped_project);
            anyhow::ensure!(
                repeated_versions.run_versions.get(&run_id) == Some(&repeated_outcome.bumped_run)
            );
            let repeated = store
                .list_runs(&project_id)
                .await?
                .rows
                .into_iter()
                .find(|row| row.run_id == run_id)
                .context("repeat termination hid the fixture run")?;
            anyhow::ensure!(repeated.terminated_at_ms == Some(first_terminated_at_ms));
            anyhow::ensure!(repeated.exit_code == Some(9));
            anyhow::ensure!(repeated.last_ingested_at_ms == Some(repeated_ingested_at_ms));

            let reinitialized = store
                .init_run(&project_id, &run_id, "renamed", None)
                .await?;
            anyhow::ensure!(reinitialized.row.terminated_at_ms.is_none());
            anyhow::ensure!(reinitialized.row.exit_code.is_none());
            store
                .terminate_run(&project_id, &run_id, 0, None, None)
                .await?
                .context("post-reinit termination lost the fixture run")?;
            let reterminated = store
                .list_runs(&project_id)
                .await?
                .rows
                .into_iter()
                .find(|row| row.run_id == run_id)
                .context("post-reinit termination hid the fixture run")?;
            anyhow::ensure!(
                reterminated.terminated_at_ms > Some(first_terminated_at_ms),
                "reinitialized execution reused its prior end time"
            );

            // These three methods share the same project -> run database lock
            // order, so PostgreSQL must serialize them without a lock cycle.
            // Start every round together and bound the whole
            // stress pass: PostgreSQL reports a deadlock as an operation
            // error, while an unreported lock stall trips the outer timeout.
            tokio::time::timeout(Duration::from_secs(30), async {
                for round in 0..24 {
                    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(3));
                    let init_barrier = barrier.clone();
                    let rename_barrier = barrier.clone();
                    let terminate_barrier = barrier;
                    let concurrent_name = format!("concurrent-{round}");

                    let reinitialize = async {
                        init_barrier.wait().await;
                        store
                            .init_run(&project_id, &run_id, &concurrent_name, None)
                            .await
                    };
                    let rename = async {
                        rename_barrier.wait().await;
                        store
                            .rename_run(&project_id, &run_id, &concurrent_name)
                            .await
                    };
                    let terminate = async {
                        terminate_barrier.wait().await;
                        store
                            .terminate_run(&project_id, &run_id, round, None, None)
                            .await
                    };
                    let (reinitialized, renamed, terminated) =
                        tokio::join!(reinitialize, rename, terminate);
                    reinitialized?;
                    anyhow::ensure!(renamed?.is_some(), "concurrent rename lost the fixture run");
                    anyhow::ensure!(
                        terminated?.is_some(),
                        "concurrent termination lost the fixture run"
                    );
                }
                Ok::<(), anyhow::Error>(())
            })
            .await
            .map_err(|_| anyhow::anyhow!("concurrent lifecycle mutations timed out"))??;
            Ok(())
        }
        .await;

        let cleanup_result: Result<()> = async {
            sqlx::query("DELETE FROM runs WHERE project_id = $1")
                .bind(&project_id)
                .execute(&store.pool)
                .await?;
            sqlx::query("DELETE FROM run_ids WHERE project_id = $1")
                .bind(&project_id)
                .execute(&store.pool)
                .await?;
            sqlx::query("DELETE FROM projects WHERE project_id = $1")
                .bind(&project_id)
                .execute(&store.pool)
                .await?;
            Ok(())
        }
        .await;
        test_result?;
        cleanup_result?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL"]
    async fn live_trash_ingest_snapshot_handles_null_and_preserves_newer_value() -> Result<()> {
        let _suite_guard = live_database_suite_gate().lock().await;
        let pg_url = std::env::var("KYMO_LIVE_TEST_DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("KYMO_LIVE_TEST_DATABASE_URL is required"))?;
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let project_id = format!("trash-ingest-{suffix}");
        let missing_id = format!("missing-{suffix}");
        let newer_id = format!("newer-{suffix}");
        let run_ids = vec![missing_id.clone(), newer_id.clone()];
        let store = PgStore::connect(&pg_url).await?;

        let test_result: Result<()> = async {
            for run_id in &run_ids {
                store
                    .init_run(&project_id, run_id, "trash ingest", None)
                    .await?;
            }
            let stored_ingested_at_ms = 1_700_000_200_123;
            store
                .bump_run_versions(&[TouchedRun {
                    project_id: project_id.clone(),
                    run_id: newer_id.clone(),
                    max_main_metric_at_ms: None,
                    max_system_metric_at_ms: None,
                    last_ingested_at_ms: stored_ingested_at_ms,
                }])
                .await?;
            let pending = HashMap::from([(newer_id.clone(), stored_ingested_at_ms - 1_000)]);

            store
                .trash_runs_chunk(&project_id, &run_ids, &pending)
                .await?;

            let (missing, _) = store
                .get_run(&project_id, &missing_id)
                .await?
                .context("NULL-snapshot fixture disappeared")?;
            anyhow::ensure!(missing.last_ingested_at_ms.is_none());
            let (newer, _) = store
                .get_run(&project_id, &newer_id)
                .await?
                .context("GREATEST fixture disappeared")?;
            anyhow::ensure!(newer.last_ingested_at_ms == Some(stored_ingested_at_ms));
            Ok(())
        }
        .await;

        let cleanup_result: Result<()> = async {
            sqlx::query("DELETE FROM runs WHERE project_id = $1")
                .bind(&project_id)
                .execute(&store.pool)
                .await?;
            sqlx::query("DELETE FROM run_ids WHERE project_id = $1")
                .bind(&project_id)
                .execute(&store.pool)
                .await?;
            sqlx::query("DELETE FROM project_activity WHERE project_id = $1")
                .bind(&project_id)
                .execute(&store.pool)
                .await?;
            sqlx::query("DELETE FROM projects WHERE project_id = $1")
                .bind(&project_id)
                .execute(&store.pool)
                .await?;
            Ok(())
        }
        .await;
        test_result?;
        cleanup_result?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL"]
    async fn live_project_listing_tracks_run_rows_and_last_logged() -> Result<()> {
        // What the reaper does to one expired run: trash, claim, finalize.
        async fn purge(store: &PgStore, project_id: &str, run_id: &str) -> Result<Option<u64>> {
            sqlx::query(
                "UPDATE runs SET deleted_at = COALESCE(deleted_at, clock_timestamp()),
                                 purging_at = clock_timestamp()
                 WHERE project_id = $1 AND run_id = $2",
            )
            .bind(project_id)
            .bind(run_id)
            .execute(&store.pool)
            .await?;
            store.finalize_purged_runs(project_id, &[run_id]).await
        }
        let _suite_guard = live_database_suite_gate().lock().await;
        let pg_url = std::env::var("KYMO_LIVE_TEST_DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("KYMO_LIVE_TEST_DATABASE_URL is required"))?;
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let live = format!("last-logged-live-{suffix}");
        let imported = format!("last-logged-imported-{suffix}");
        let silent = format!("last-logged-silent-{suffix}");
        let projects = vec![live.clone(), imported.clone(), silent.clone()];
        let live_id = format!("live-{suffix}");
        let silent_id = format!("silent-{suffix}");
        let imported_id = format!("imported-{suffix}");
        let store = PgStore::connect(&pg_url).await?;

        // Whole seconds so EXTRACT(EPOCH)*1000::BIGINT reads back exactly.
        const LIVE_MS: i64 = 1_700_000_100_000;
        const ARCHIVED_END_MS: i64 = 1_680_003_700_000;
        const IMPORT_INGEST_MS: i64 = 1_700_000_000_000;

        let test_result: Result<()> = async {
            let listed = |listing: &ProjectListing, project: &str| {
                listing
                    .projects
                    .iter()
                    .find(|(id, _)| id == project)
                    .map(|(_, at)| *at)
            };
            let beat = |project: &str, run_id: &str, ingested_ms: i64| TouchedRun {
                project_id: project.to_string(),
                run_id: run_id.to_string(),
                max_main_metric_at_ms: None,
                max_system_metric_at_ms: None,
                last_ingested_at_ms: ingested_ms,
            };
            store.init_run(&live, &live_id, "live", None).await?;
            store
                .init_run(&imported, &imported_id, "imported", Some(1_680_000_000_000))
                .await?;
            store.init_run(&silent, &silent_id, "silent", None).await?;

            // The first heartbeat records; the rest of its clock minute writes nothing; the next clock minute rolls forward, even under 60 s later.
            for (ingested_ms, expected_ms) in [
                (LIVE_MS + 50_000, LIVE_MS + 50_000),
                (LIVE_MS + 55_000, LIVE_MS + 50_000),
                (LIVE_MS + 70_000, LIVE_MS + 70_000),
            ] {
                store
                    .bump_run_versions(&[beat(&live, &live_id, ingested_ms)])
                    .await?;
                let listing = store.list_metric_projects().await?;
                anyhow::ensure!(listed(&listing, &live) == Some(Some(expected_ms)));
            }
            // Trash keeps the project listed with its value; purging its last run delists it with a global bump; a new run relists it.
            store
                .trash_runs_chunk(&live, std::slice::from_ref(&live_id), &HashMap::new())
                .await?;
            let listing = store.list_metric_projects().await?;
            anyhow::ensure!(listed(&listing, &live) == Some(Some(LIVE_MS + 70_000)));
            anyhow::ensure!(purge(&store, &live, &live_id).await?.is_some());
            let listing = store.list_metric_projects().await?;
            anyhow::ensure!(listed(&listing, &live).is_none());
            let relisted = store
                .init_run(&live, &format!("live-again-{suffix}"), "live again", None)
                .await?;
            anyhow::ensure!(relisted.bumped_global.is_some());
            let listing = store.list_metric_projects().await?;
            anyhow::ensure!(listed(&listing, &live) == Some(Some(LIVE_MS + 70_000)));
            let third = format!("live-third-{suffix}");
            anyhow::ensure!(store
                .init_run(&live, &third, "live third", None)
                .await?
                .bumped_global
                .is_none());
            // Purging one of two runs keeps the project listed.
            anyhow::ensure!(purge(&store, &live, &third).await?.is_some());
            let listing = store.list_metric_projects().await?;
            anyhow::ensure!(listed(&listing, &live) == Some(Some(LIVE_MS + 70_000)));

            // A terminated run's import-time ingest clock is capped at its archived end.
            store
                .terminate_run(&imported, &imported_id, 0, None, Some(ARCHIVED_END_MS))
                .await?
                .context("finalize should find the imported run")?;
            store
                .bump_run_versions(&[beat(&imported, &imported_id, IMPORT_INGEST_MS)])
                .await?;
            let listing = store.list_metric_projects().await?;
            anyhow::ensure!(listing.server_now_ms > LIVE_MS);
            anyhow::ensure!(listed(&listing, &imported) == Some(Some(ARCHIVED_END_MS)));
            anyhow::ensure!(listed(&listing, &silent) == Some(None));

            // An empty table is seeded at boot from every runs row, Trash included (the disposable live-test database's table is emptied for this).
            store
                .trash_runs_chunk(&silent, std::slice::from_ref(&silent_id), &HashMap::new())
                .await?;
            sqlx::query("DELETE FROM project_activity")
                .execute(&store.pool)
                .await?;
            let reseeded = PgStore::connect(&pg_url).await?;
            let listing = reseeded.list_metric_projects().await?;
            anyhow::ensure!(listed(&listing, &imported) == Some(Some(ARCHIVED_END_MS)));
            anyhow::ensure!(listed(&listing, &live) == Some(None));
            anyhow::ensure!(listed(&listing, &silent) == Some(None));
            Ok(())
        }
        .await;

        let cleanup_result: Result<()> = async {
            for table in [
                "runs",
                "run_ids",
                "purged_runs",
                "project_activity",
                "projects",
            ] {
                sqlx::query(&format!("DELETE FROM {table} WHERE project_id = ANY($1)"))
                    .bind(&projects)
                    .execute(&store.pool)
                    .await?;
            }
            Ok(())
        }
        .await;
        test_result?;
        cleanup_result?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL"]
    async fn live_query_gates_serialize_reverse_lifecycle_lock_orders() -> Result<()> {
        let _suite_guard = live_database_suite_gate().lock().await;
        let pg_url = std::env::var("KYMO_LIVE_TEST_DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("KYMO_LIVE_TEST_DATABASE_URL is required"))?;
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let project_id = format!("rename-gates-{suffix}");
        let run_id = format!("run-{suffix}");
        let store = std::sync::Arc::new(PgStore::connect(&pg_url).await?);
        let (events, _events_rx) = tokio::sync::broadcast::channel(64);
        let service = std::sync::Arc::new(crate::query::QueryService::new(
            std::sync::Arc::new(crate::clickhouse::ChClient::new("http://127.0.0.1:9").unwrap()),
            store.clone(),
            crate::ingest::BumpCoalescer::empty_for_test(),
            crate::lifecycle::LifecycleGates::new(),
            None,
            events,
        ));

        let request_error = |operation: &str, status: tonic::Status| {
            anyhow::anyhow!(
                "{operation} failed with {}: {}",
                status.code(),
                status.message()
            )
        };
        service
            .init_run(tonic::Request::new(crate::proto::InitRunRequest {
                project_id: project_id.clone(),
                run_id: run_id.clone(),
                run_name: "original".to_string(),
                local_hold_id: None,
            }))
            .await
            .map_err(|status| request_error("fixture InitRun", status))?;

        let test_result: Result<()> = async {
            tokio::time::timeout(Duration::from_secs(30), async {
                for round in 0..24 {
                    // Trash locks the run row before the project row. These
                    // three handlers lock project before run. Exercising the
                    // public handlers proves their shared/exclusive lifecycle
                    // gates keep the opposite database orders from overlapping.
                    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(4));
                    let init_barrier = barrier.clone();
                    let rename_barrier = barrier.clone();
                    let terminate_barrier = barrier.clone();
                    let trash_barrier = barrier;
                    let concurrent_name = format!("concurrent-{round}");

                    let reinitialize = {
                        let service = service.clone();
                        let project_id = project_id.clone();
                        let run_id = run_id.clone();
                        let run_name = concurrent_name.clone();
                        async move {
                            init_barrier.wait().await;
                            service
                                .init_run(tonic::Request::new(crate::proto::InitRunRequest {
                                    project_id,
                                    run_id,
                                    run_name,
                                    local_hold_id: None,
                                }))
                                .await
                                .map(|_| ())
                        }
                    };
                    let rename = {
                        let service = service.clone();
                        let project_id = project_id.clone();
                        let run_id = run_id.clone();
                        let run_name = concurrent_name;
                        async move {
                            rename_barrier.wait().await;
                            service
                                .rename_run(tonic::Request::new(crate::proto::RenameRunRequest {
                                    project_id,
                                    run_id,
                                    run_name,
                                }))
                                .await
                                .map(|_| ())
                        }
                    };
                    let terminate = {
                        let service = service.clone();
                        let project_id = project_id.clone();
                        let run_id = run_id.clone();
                        async move {
                            terminate_barrier.wait().await;
                            service
                                .terminate_run(tonic::Request::new(
                                    crate::proto::TerminateRunRequest {
                                        project_id,
                                        run_id,
                                        exit_code: round,
                                    },
                                ))
                                .await
                                .map(|_| ())
                        }
                    };
                    let trash = {
                        let service = service.clone();
                        let project_id = project_id.clone();
                        let run_id = run_id.clone();
                        async move {
                            trash_barrier.wait().await;
                            service
                                .trash_runs(tonic::Request::new(crate::proto::TrashRunsRequest {
                                    project_id,
                                    run_ids: vec![run_id],
                                }))
                                .await
                        }
                    };

                    let (reinitialized, renamed, terminated, trashed) =
                        tokio::join!(reinitialize, rename, terminate, trash);
                    for (operation, result) in [
                        ("InitRun", reinitialized),
                        ("RenameRun", renamed),
                        ("TerminateRun", terminated),
                    ] {
                        match result {
                            Ok(()) => {}
                            Err(status) if status.code() == tonic::Code::FailedPrecondition => {}
                            Err(status) => return Err(request_error(operation, status)),
                        }
                    }
                    let trashed = trashed
                        .map_err(|status| request_error("TrashRuns", status))?
                        .into_inner();
                    anyhow::ensure!(
                        trashed.results.len() == 1
                            && trashed.results[0].outcome
                                == crate::proto::TrashRunOutcome::Trashed as i32,
                        "round {round} did not end with the fixture in Trash: {:?}",
                        trashed.results
                    );

                    // Restore has the same run-first/project-second database
                    // order as Trash. Race it with Rename and require that the
                    // pair completes with the fixture active for the next round.
                    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
                    let restore_barrier = barrier.clone();
                    let rename_barrier = barrier;
                    let restore = {
                        let service = service.clone();
                        let project_id = project_id.clone();
                        let run_id = run_id.clone();
                        async move {
                            restore_barrier.wait().await;
                            service
                                .restore_run(tonic::Request::new(crate::proto::RestoreRunRequest {
                                    project_id,
                                    run_id,
                                }))
                                .await
                        }
                    };
                    let rename = {
                        let service = service.clone();
                        let project_id = project_id.clone();
                        let run_id = run_id.clone();
                        async move {
                            rename_barrier.wait().await;
                            service
                                .rename_run(tonic::Request::new(crate::proto::RenameRunRequest {
                                    project_id,
                                    run_id,
                                    run_name: format!("restored-{round}"),
                                }))
                                .await
                                .map(|_| ())
                        }
                    };
                    let (restored, renamed) = tokio::join!(restore, rename);
                    let restored = restored
                        .map_err(|status| request_error("RestoreRun", status))?
                        .into_inner();
                    anyhow::ensure!(
                        restored.outcome == crate::proto::RestoreRunOutcome::Restored as i32,
                        "round {round} restore returned outcome {}: {}",
                        restored.outcome,
                        restored.error
                    );
                    match renamed {
                        Ok(()) => {}
                        Err(status) if status.code() == tonic::Code::FailedPrecondition => {}
                        Err(status) => return Err(request_error("RenameRun after Trash", status)),
                    }
                    store
                        .ensure_runs_active(&[RunKey::new(&project_id, &run_id)])
                        .await
                        .map_err(|error| {
                            anyhow::anyhow!("round {round} did not finish active: {error}")
                        })?;
                }
                Ok::<(), anyhow::Error>(())
            })
            .await
            .map_err(|_| anyhow::anyhow!("reverse-order lifecycle races timed out"))??;
            Ok(())
        }
        .await;

        let cleanup_result: Result<()> = async {
            sqlx::query("DELETE FROM runs WHERE project_id = $1")
                .bind(&project_id)
                .execute(&store.pool)
                .await?;
            sqlx::query("DELETE FROM run_ids WHERE project_id = $1")
                .bind(&project_id)
                .execute(&store.pool)
                .await?;
            sqlx::query("DELETE FROM projects WHERE project_id = $1")
                .bind(&project_id)
                .execute(&store.pool)
                .await?;
            Ok(())
        }
        .await;
        test_result?;
        cleanup_result?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL"]
    async fn live_list_trash_pages_newest_first_across_ties() -> Result<()> {
        let _suite_guard = live_database_suite_gate().lock().await;
        let pg_url = std::env::var("KYMO_LIVE_TEST_DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("KYMO_LIVE_TEST_DATABASE_URL is required"))?;
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let project_a = format!("trash-page-a-{suffix}");
        let project_b = format!("trash-page-b-{suffix}");
        let projects = vec![project_a.clone(), project_b.clone()];
        let store = PgStore::connect(&pg_url).await?;
        let count_before: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE deleted_at IS NOT NULL")
                .fetch_one(&store.pool)
                .await?;

        let test_result: Result<()> = async {
            for (project_id, run_id) in [
                (&project_a, "a1"),
                (&project_a, "a2"),
                (&project_a, "a3"),
                (&project_b, "b1"),
                (&project_b, "b2"),
                (&project_b, "b3"),
            ] {
                store.init_run(project_id, run_id, run_id, None).await?;
            }

            // Put the fixture far ahead of ordinary test data so these are the
            // first three global pages even if the disposable DB is reused.
            const OLDEST_MS: i64 = 253_402_000_000_000;
            const TIED_MS: i64 = OLDEST_MS + 1_000;
            const NEWEST_MS: i64 = TIED_MS + 1_000;
            for (project_id, run_id, deleted_at_ms) in [
                (&project_a, "a1", TIED_MS),
                (&project_a, "a2", TIED_MS),
                (&project_a, "a3", TIED_MS),
                (&project_b, "b1", OLDEST_MS),
                (&project_b, "b2", TIED_MS),
                (&project_b, "b3", NEWEST_MS),
            ] {
                let updated = sqlx::query(
                    "UPDATE runs
                     SET deleted_at = to_timestamp($3::DOUBLE PRECISION / 1000.0)
                     WHERE project_id = $1 AND run_id = $2",
                )
                .bind(project_id)
                .bind(run_id)
                .bind(deleted_at_ms)
                .execute(&store.pool)
                .await?;
                anyhow::ensure!(
                    updated.rows_affected() == 1,
                    "fixture row {project_id}/{run_id} was not updated"
                );
            }

            let expected_pages = [
                vec![format!("{project_b}/b3"), format!("{project_a}/a3")],
                vec![format!("{project_a}/a2"), format!("{project_a}/a1")],
                vec![format!("{project_b}/b2"), format!("{project_b}/b1")],
            ];
            let mut after = None;
            for (page_index, expected) in expected_pages.iter().enumerate() {
                let page = store
                    .list_trash(TrashListQuery::Page {
                        page_size: 2,
                        after,
                    })
                    .await?;
                if page_index == 0 {
                    anyhow::ensure!(
                        page.total_count == Some((count_before + 6) as u64),
                        "first page count was {:?}",
                        page.total_count
                    );
                } else {
                    anyhow::ensure!(
                        page.total_count.is_none(),
                        "continuation repeated the total count"
                    );
                }
                let actual = page
                    .rows
                    .iter()
                    .map(|row| format!("{}/{}", row.project_id, row.run_id))
                    .collect::<Vec<_>>();
                anyhow::ensure!(
                    actual == *expected,
                    "page {page_index} was {actual:?}, expected {expected:?}"
                );
                after = page.next;
                if page_index + 1 < expected_pages.len() {
                    anyhow::ensure!(after.is_some(), "page {page_index} had no cursor");
                }
            }
            Ok(())
        }
        .await;

        let cleanup_result: Result<()> = async {
            sqlx::query("DELETE FROM runs WHERE project_id = ANY($1)")
                .bind(&projects)
                .execute(&store.pool)
                .await?;
            sqlx::query("DELETE FROM run_ids WHERE project_id = ANY($1)")
                .bind(&projects)
                .execute(&store.pool)
                .await?;
            sqlx::query("DELETE FROM projects WHERE project_id = ANY($1)")
                .bind(&projects)
                .execute(&store.pool)
                .await?;
            Ok(())
        }
        .await;
        test_result?;
        cleanup_result?;
        Ok(())
    }
}
