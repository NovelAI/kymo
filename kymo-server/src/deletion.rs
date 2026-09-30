//! Resumable physical cleanup for expired runs.
//!
//! Postgres is the work queue: `purging_at` is an irreversible durable claim.
//! A pass takes each target run's exclusive lifecycle gate long enough to drain
//! admitted work and commit that claim. It then releases the gate so queued
//! requests can promptly read the authoritative lifecycle state and reject the
//! run before ClickHouse. Every operation after the claim is idempotent; a crash
//! simply leaves the claim for a later pass. Each pass considers a fixed ready
//! set and records selection before external work, so one failed project batch
//! cannot own every candidate page.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::clickhouse::ChClient;
use crate::events::{EventSender, VersionEvent};
use crate::lifecycle::{LifecycleGates, RunKey};
use crate::pg::PgStore;
use anyhow::{Context, Result};

const DEFAULT_BATCH_SIZE: usize = 100;
const REAPER_INTERVAL: Duration = Duration::from_secs(60 * 60);
// A local stack can stop before the first hourly pass and restarts its schedule on every boot, so it cannot wait a full interval before its first pass.
const LOCAL_FIRST_PASS_DELAY: Duration = Duration::from_secs(60);
const WORKER_RESTART_DELAY: Duration = Duration::from_secs(5);
const PASS_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const PG_TIMEOUT: Duration = Duration::from_secs(30);
// A successful ClickHouse mutation must still have enough time before the
// hard pass deadline to commit its Postgres tombstone. This exceeds PG_TIMEOUT
// so timeout bookkeeping cannot consume the whole reserve.
const FINALIZATION_HEADROOM: Duration = Duration::from_secs(PG_TIMEOUT.as_secs() * 2);
const GATE_TIMEOUT: Duration = Duration::from_secs(60);
const GATE_PHASE_TIMEOUT: Duration = Duration::from_secs(2 * 60);
// Flush plus verification is one barrier with one retryable budget. A flush
// that consumes this whole budget should retry the pass rather than begin
// another database operation after a minute-long global submission pause.
const BARRIER_TIMEOUT: Duration = Duration::from_secs(60);
// Large project partitions can take about 20 minutes to rewrite. Keep the
// client attached long enough to receive that acknowledgement; the absolute
// pass deadline still shortens this budget to preserve finalization headroom.
const MUTATION_TIMEOUT: Duration = Duration::from_secs(25 * 60);
const REAPER_FAILURE_STAGES: [&str; 9] = [
    "worker",
    "run_gate_budget",
    "run_gate",
    "pass",
    "backlog",
    "submission_gate",
    "ch_barrier",
    "ch_mutation",
    "finalize",
];

fn record_reaper_failure(stage: &'static str) {
    metrics::counter!("mkdb2_run_reaper_failures_total", "stage" => stage).increment(1);
    metrics::gauge!("mkdb2_run_reaper_last_failure_unixtime_seconds").set(unix_time_seconds());
}

fn restore_candidate_order(
    runs: &mut [crate::lifecycle::RunKey],
    candidate_rank: &std::collections::HashMap<crate::lifecycle::RunKey, usize>,
) {
    runs.sort_unstable_by_key(|key| candidate_rank.get(key).copied().unwrap_or(usize::MAX));
}

fn group_runs_by_project(runs: &[RunKey]) -> Vec<Vec<RunKey>> {
    let mut groups = Vec::<Vec<RunKey>>::new();
    let mut project_indexes = std::collections::HashMap::<&str, usize>::new();
    for key in runs {
        if let Some(&index) = project_indexes.get(key.project_id.as_str()) {
            groups[index].push(key.clone());
        } else {
            project_indexes.insert(key.project_id.as_str(), groups.len());
            groups.push(vec![key.clone()]);
        }
    }
    groups
}

fn should_continue_ready_drain(report: &ReapReport, batch_size: usize) -> bool {
    !report.halt_pass && report.candidates >= batch_size
}

async fn acquire_purge_gates(
    gates: &LifecycleGates,
    candidates: &[crate::lifecycle::RunKey],
    phase_budget: Duration,
) -> (
    Vec<(
        crate::lifecycle::RunKey,
        tokio::sync::OwnedRwLockWriteGuard<()>,
    )>,
    Vec<crate::lifecycle::RunKey>,
) {
    let deadline = tokio::time::Instant::now() + phase_budget;
    let mut gate_order = candidates.to_vec();
    gate_order.sort_unstable();
    let mut gate_order = gate_order.into_iter();
    let mut gated = Vec::with_capacity(candidates.len());
    let mut skipped = Vec::new();

    while let Some(key) = gate_order.next() {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            let deferred = 1 + gate_order.len();
            record_reaper_failure("run_gate_budget");
            tracing::warn!(
                deferred,
                "run purge gate phase exhausted its total budget; deferring the untouched tail"
            );
            break;
        }

        // Reserve a fair share of the phase for every remaining identity.
        // Otherwise two lexicographically early hung reads can each consume
        // GATE_TIMEOUT and prevent a later free run from ever being reached.
        let remaining_keys = 1 + gate_order.len();
        let gate_budget = GATE_TIMEOUT.min(remaining / remaining_keys as u32);

        let gate_started = std::time::Instant::now();
        let gate = timeout_value(
            gate_budget,
            "acquiring a run purge gate",
            gates.write_one(key.clone()),
        )
        .await;
        metrics::histogram!(
            "mkdb2_run_reaper_gate_wait_duration_seconds",
            "outcome" => if gate.is_ok() { "success" } else { "timeout" }
        )
        .record(gate_started.elapsed().as_secs_f64());
        match gate {
            Ok(guard) => gated.push((key, guard)),
            Err(error) => {
                record_reaper_failure("run_gate");
                tracing::warn!(
                    project_id = %key.project_id,
                    run_id = %key.run_id,
                    %error,
                    "skipping busy expired run until a later purge pass"
                );
                skipped.push(key);
            }
        }
    }

    (gated, skipped)
}

#[derive(Clone, Debug)]
pub struct ReaperConfig {
    /// Operational kill switch. This is not a retention-policy control:
    /// expired runs remain unreadable and unrestorable while reaping is off.
    pub enabled: bool,
    pub first_pass_delay: Duration,
    pub interval: Duration,
    pub batch_size: usize,
}

impl ReaperConfig {
    /// A local installation owns its data outright, so physical deletion is always on.
    pub fn local() -> Self {
        Self {
            enabled: true,
            first_pass_delay: LOCAL_FIRST_PASS_DELAY,
            interval: REAPER_INTERVAL,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn from_env() -> Result<Self> {
        anyhow::ensure!(
            std::env::var_os("KYMO_RUN_REAPER_INTERVAL_SECONDS").is_none(),
            "KYMO_RUN_REAPER_INTERVAL_SECONDS is not supported; the production cadence is fixed at one hour so health alerts and cleanup scheduling cannot diverge"
        );
        Ok(Self {
            // Physical deletion is an explicit rollout step. Expired runs
            // remain unreadable while this is false, but no bulk data is
            // removed until operators deliberately enable the worker.
            enabled: crate::env::required_bool("KYMO_RUN_REAPER_ENABLED", false)?,
            // Wait one complete interval before the first destructive pass so a newly enabled deployment publishes backlog and health metrics before it claims any rows.
            first_pass_delay: REAPER_INTERVAL,
            interval: REAPER_INTERVAL,
            batch_size: crate::env::required_bounded_usize(
                "KYMO_RUN_REAPER_BATCH_SIZE",
                DEFAULT_BATCH_SIZE,
                1,
                DEFAULT_BATCH_SIZE,
            )?,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReapReport {
    pub candidates: usize,
    pub claimed: usize,
    pub cache_entries_purged: usize,
    pub finalized: usize,
    /// Stop the outer ready-set drain after a global-looking ClickHouse
    /// failure or when the pass must preserve finalization headroom.
    pub halt_pass: bool,
}

enum RunMutationOutcome {
    Deleted { cache_entries_purged: usize },
    Failed(anyhow::Error),
    PassBudgetExhausted,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct MutationDrainReport {
    cache_entries_purged: usize,
    finalized: usize,
    halt_pass: bool,
}

/// Finalize each successful project mutation before starting another
/// potentially blocking mutation. The outer pass timeout may cancel any
/// await point, so holding successes until the end of a pass can starve
/// durable finalization forever when a later project repeatedly hangs.
async fn delete_and_finalize_project_batches<D, DF, F, FF>(
    project_batches: &[Vec<RunKey>],
    mut delete_batch: D,
    mut finalize_batch: F,
) -> Result<MutationDrainReport>
where
    D: FnMut(Vec<RunKey>) -> DF,
    DF: Future<Output = Result<RunMutationOutcome>>,
    F: FnMut(Vec<RunKey>) -> FF,
    FF: Future<Output = Result<()>>,
{
    anyhow::ensure!(
        project_batches.iter().all(|batch| !batch.is_empty()),
        "project deletion batches cannot be empty"
    );
    let mut report = MutationDrainReport::default();
    for (index, batch) in project_batches.iter().enumerate() {
        match delete_batch(batch.clone()).await? {
            RunMutationOutcome::Deleted {
                cache_entries_purged,
            } => {
                report.cache_entries_purged += cache_entries_purged;
                // This must remain the next await after a successful delete.
                // In particular, do not start the next ClickHouse mutation
                // until this batch's tombstones and canonical-row deletions are
                // committed in Postgres.
                finalize_batch(batch.clone()).await?;
                report.finalized += batch.len();
                metrics::counter!("mkdb2_run_reaper_finalized_total").increment(batch.len() as u64);
            }
            RunMutationOutcome::Failed(error) => {
                tracing::error!(
                    project_id = %batch[0].project_id,
                    runs = batch.len(),
                    %error,
                    "ClickHouse project deletion failed; leaving durable claims for retry"
                );
                record_reaper_failure("ch_mutation");
                let deferred = project_batches[index + 1..]
                    .iter()
                    .map(Vec::len)
                    .sum::<usize>();
                tracing::error!(
                    deferred,
                    "stopping this purge pass after a ClickHouse mutation failure"
                );
                // ClickHouse mutations are durable and ordered. A timeout can
                // hide a still-running mutation, so never enqueue another
                // project's mutation behind an ambiguous result in this pass.
                report.halt_pass = true;
                break;
            }
            RunMutationOutcome::PassBudgetExhausted => {
                let deferred = project_batches[index..].iter().map(Vec::len).sum::<usize>();
                tracing::info!(
                    deferred,
                    "stopping this purge batch with finalization headroom intact"
                );
                report.halt_pass = true;
                break;
            }
        }
    }
    Ok(report)
}

fn mutation_timeout_with_finalization_headroom(
    pass_deadline: tokio::time::Instant,
    now: tokio::time::Instant,
) -> Option<Duration> {
    let remaining = pass_deadline.saturating_duration_since(now);
    if remaining <= FINALIZATION_HEADROOM {
        None
    } else {
        Some((remaining - FINALIZATION_HEADROOM).min(MUTATION_TIMEOUT))
    }
}

pub struct DeletionReaper {
    pg: Arc<PgStore>,
    ch: Arc<ChClient>,
    gates: LifecycleGates,
    events: EventSender,
    config: ReaperConfig,
    activity: Arc<crate::activity::ActivityTracker>,
}

struct WorkerAlive {
    worker: &'static str,
}

impl WorkerAlive {
    fn new(worker: &'static str) -> Self {
        metrics::gauge!("mkdb2_deletion_worker_alive", "worker" => worker).set(1.0);
        Self { worker }
    }
}

impl Drop for WorkerAlive {
    fn drop(&mut self) {
        metrics::gauge!("mkdb2_deletion_worker_alive", "worker" => self.worker).set(0.0);
    }
}

async fn supervise_worker<F, Fut>(
    worker: &'static str,
    resync_events: EventSender,
    restart_delay: Duration,
    mut run: F,
) where
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = ()> + Send + 'static,
{
    loop {
        let result = tokio::spawn(run()).await;
        let reason = match &result {
            Ok(()) => "exit",
            Err(error) if error.is_panic() => "panic",
            Err(_) => "cancelled",
        };
        metrics::counter!(
            "mkdb2_deletion_worker_restarts_total",
            "worker" => worker,
            "reason" => reason
        )
        .increment(1);
        record_reaper_failure("worker");
        let _ = resync_events.send(VersionEvent {
            resync: true,
            ..VersionEvent::default()
        });
        match result {
            Ok(()) => tracing::error!(worker, "deletion worker exited unexpectedly; restarting"),
            Err(error) => tracing::error!(worker, %error, "deletion worker failed; restarting"),
        }
        tokio::time::sleep(restart_delay).await;
    }
}

impl DeletionReaper {
    pub fn new(
        pg: Arc<PgStore>,
        ch: Arc<ChClient>,
        gates: LifecycleGates,
        events: EventSender,
        config: ReaperConfig,
        activity: Arc<crate::activity::ActivityTracker>,
    ) -> Self {
        Self {
            pg,
            ch,
            gates,
            events,
            config,
            activity,
        }
    }

    /// Always supervise the observer so backlog metrics are useful before the
    /// destructive rollout is enabled. Panics are contained and restarted;
    /// durable purge claims remain safe to retry, and a conservative resync
    /// heals any ambiguous visible state.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        let worker = Arc::new(self);
        metrics::gauge!("mkdb2_deletion_worker_alive", "worker" => "run_reaper").set(0.0);
        metrics::gauge!("mkdb2_run_reaper_enabled").set(if worker.config.enabled {
            1.0
        } else {
            0.0
        });
        metrics::gauge!("mkdb2_run_reaper_last_success_unixtime_seconds").set(0.0);
        metrics::gauge!("mkdb2_run_reaper_last_failure_unixtime_seconds").set(0.0);
        for stage in REAPER_FAILURE_STAGES {
            // Register every labelled series before its first increment so
            // dashboards have a stable baseline. The timestamp gauge below
            // remains the alert authority even if startup fails before the
            // first Prometheus scrape.
            metrics::counter!("mkdb2_run_reaper_failures_total", "stage" => stage).absolute(0);
        }
        tracing::info!(
            enabled = worker.config.enabled,
            interval_seconds = worker.config.interval.as_secs(),
            batch_size = worker.config.batch_size,
            "expired-run reaper configured"
        );
        if !worker.config.enabled {
            tracing::warn!("expired-run reaper is observe-only; physical deletion is disabled");
        }

        tokio::spawn(worker.supervise_reaper())
    }

    async fn supervise_reaper(self: Arc<Self>) {
        let owner = self.clone();
        supervise_worker(
            "run_reaper",
            self.events.clone(),
            WORKER_RESTART_DELAY,
            move || {
                let worker = owner.clone();
                async move { worker.run_reaper_loop().await }
            },
        )
        .await;
    }

    async fn run_reaper_loop(&self) {
        let _alive = WorkerAlive::new("run_reaper");
        self.observe_backlog().await;
        let mut delay = self.config.first_pass_delay;
        loop {
            tokio::time::sleep(delay).await;
            delay = self.config.interval;
            self.observe_backlog().await;
            if !self.config.enabled {
                continue;
            }
            let _work = self.activity.begin_work();
            let started = std::time::Instant::now();
            let pass_timeout = PASS_TIMEOUT + FINALIZATION_HEADROOM;
            let pass_deadline = tokio::time::Instant::now() + pass_timeout;
            match timed(
                pass_timeout,
                "draining the expired-run backlog",
                self.reap_ready(pass_deadline),
            )
            .await
            {
                Ok(report) => {
                    let outcome = if report.halt_pass {
                        "halted"
                    } else {
                        "success"
                    };
                    metrics::counter!("mkdb2_run_reaper_passes_total", "outcome" => outcome)
                        .increment(1);
                    metrics::histogram!(
                        "mkdb2_run_reaper_pass_duration_seconds",
                        "outcome" => outcome
                    )
                    .record(started.elapsed().as_secs_f64());
                    if !report.halt_pass {
                        metrics::gauge!("mkdb2_run_reaper_last_success_unixtime_seconds")
                            .set(unix_time_seconds());
                    }
                    tracing::info!(
                        candidates = report.candidates,
                        claimed = report.claimed,
                        cache_entries_purged = report.cache_entries_purged,
                        finalized = report.finalized,
                        halted = report.halt_pass,
                        "expired-run reaper pass complete"
                    );
                }
                Err(error) => {
                    metrics::counter!("mkdb2_run_reaper_passes_total", "outcome" => "error")
                        .increment(1);
                    record_reaper_failure("pass");
                    metrics::histogram!(
                        "mkdb2_run_reaper_pass_duration_seconds",
                        "outcome" => "error"
                    )
                    .record(started.elapsed().as_secs_f64());
                    // A timeout or transport error can hide a committed claim
                    // or finalization. Force clients to refetch instead of
                    // relying on a version event whose value is now unknown.
                    let _ = self.events.send(VersionEvent {
                        resync: true,
                        ..VersionEvent::default()
                    });
                    tracing::error!(%error, "expired-run reaper pass failed; durable claims will retry");
                }
            }
            // Publish the post-pass backlog as well; otherwise a successful
            // drain looks stalled until the next hourly observation.
            self.observe_backlog().await;
        }
    }

    async fn observe_backlog(&self) {
        match timed(
            PG_TIMEOUT,
            "measuring the expired-run backlog",
            self.pg.purge_backlog(),
        )
        .await
        {
            Ok((runs, oldest_ready_age_seconds)) => {
                metrics::gauge!("mkdb2_run_reaper_backlog_runs").set(runs as f64);
                metrics::gauge!("mkdb2_run_reaper_oldest_ready_age_seconds")
                    .set(oldest_ready_age_seconds);
            }
            Err(error) => {
                record_reaper_failure("backlog");
                tracing::warn!(%error, "failed to measure expired-run backlog");
            }
        }
    }

    /// Drain all work currently ready for deletion through bounded batches.
    /// The hourly interval controls discovery cadence, not throughput: a large
    /// Trash operation must not turn into `batch_size` purges per hour.
    async fn reap_ready(&self, pass_deadline: tokio::time::Instant) -> Result<ReapReport> {
        if !self.config.enabled {
            return Ok(ReapReport::default());
        }
        // Freeze both readiness and attempt eligibility at the start of this
        // pass. A selected row is stamped after the query and is therefore
        // ineligible for the rest of this pass, even if its work fails.
        let pass_started_ms = timed(
            PG_TIMEOUT,
            "starting an expired-run purge pass",
            self.pg.lifecycle_snapshot(),
        )
        .await?
        .server_now_ms;
        let mut total = ReapReport::default();
        loop {
            let report = self.reap_once(pass_started_ms, pass_deadline).await?;
            let continue_drain = should_continue_ready_drain(&report, self.config.batch_size);
            total.candidates += report.candidates;
            total.claimed += report.claimed;
            total.cache_entries_purged += report.cache_entries_purged;
            total.finalized += report.finalized;
            total.halt_pass |= report.halt_pass;

            if !continue_drain {
                return Ok(total);
            }
            tokio::task::yield_now().await;
        }
    }

    async fn reap_once(
        &self,
        pass_started_ms: i64,
        pass_deadline: tokio::time::Instant,
    ) -> Result<ReapReport> {
        let candidates = timed(
            PG_TIMEOUT,
            "loading purge candidates",
            self.pg
                .purge_candidates(self.config.batch_size as i64, pass_started_ms),
        )
        .await?;
        let mut report = ReapReport {
            candidates: candidates.len(),
            ..ReapReport::default()
        };
        metrics::counter!("mkdb2_run_reaper_candidates_total").increment(candidates.len() as u64);
        if candidates.is_empty() {
            return Ok(report);
        }
        // The candidate query is advisory. Queue exact run gates in the same
        // stable order as every multi-run reader/writer. Writer preference
        // prevents rejected traffic from starving cleanup; a bounded wait
        // skips a genuinely long-running identity. Holding the guards through
        // the durable claim drains every request admitted under the old state.
        let candidate_rank = candidates
            .iter()
            .enumerate()
            .map(|(rank, key)| (key.clone(), rank))
            .collect::<std::collections::HashMap<_, _>>();
        let (gated, mut skipped) =
            acquire_purge_gates(&self.gates, &candidates, GATE_PHASE_TIMEOUT).await;
        let gated_keys = gated.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
        let claim = timed(
            PG_TIMEOUT,
            "claiming expired runs",
            self.pg.claim_purge_runs(&gated_keys),
        )
        .await?;
        if !claim.rows.is_empty() {
            let _ = self.events.send(VersionEvent {
                runs: claim.terminal_versions,
                global: claim.bumped_global,
                ..VersionEvent::default()
            });
        }
        let mut runs = claim.rows;
        // Gate acquisition must use stable identity order, but cleanup should
        // retain the queue's oldest-attempt order. Otherwise lexicographically
        // early poison identities can repeatedly trip the circuit breaker in
        // front of older untouched work.
        restore_candidate_order(&mut runs, &candidate_rank);
        report.claimed = runs.len();
        metrics::counter!("mkdb2_run_reaper_claimed_total").increment(runs.len() as u64);
        let claimed = runs
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        skipped.extend(
            gated
                .iter()
                .filter(|(key, _)| !claimed.contains(key))
                .map(|(key, _)| key.clone()),
        );
        // The claim is now authoritative. Release every gate before any more
        // I/O so queued clients can acquire a shared guard, observe
        // `purging_at`, and fail promptly without reaching ClickHouse.
        drop(gated);
        // Work actually reached but skipped at its gate or claim moves to the
        // back of the retry queue. A tail left untouched when the total gate
        // budget expires keeps its older priority, so early busy identities
        // cannot strand it on every pass.
        timed(
            PG_TIMEOUT,
            "recording skipped purge attempts",
            self.pg.note_purge_attempts(&skipped, pass_started_ms),
        )
        .await?;
        if runs.is_empty() {
            return Ok(report);
        }

        // Keep the global pause deliberately narrow: only drain and verify the
        // server-side queue. Durable lifecycle state, rather than long-held run
        // gates, rejects newly arriving work while deletion proceeds.
        let submission_wait_started = std::time::Instant::now();
        let submission_guard = match timeout_value(
            GATE_TIMEOUT,
            "acquiring ClickHouse submission barrier",
            self.gates.write_submission(),
        )
        .await
        {
            Ok(guard) => {
                metrics::histogram!(
                    "mkdb2_run_reaper_submission_gate_wait_duration_seconds",
                    "outcome" => "success"
                )
                .record(submission_wait_started.elapsed().as_secs_f64());
                guard
            }
            Err(error) => {
                metrics::histogram!(
                    "mkdb2_run_reaper_submission_gate_wait_duration_seconds",
                    "outcome" => "timeout"
                )
                .record(submission_wait_started.elapsed().as_secs_f64());
                record_reaper_failure("submission_gate");
                return Err(error);
            }
        };
        let barrier_started = std::time::Instant::now();
        let barrier_result = timed(
            BARRIER_TIMEOUT,
            "flushing and verifying ClickHouse async inserts",
            self.ch.barrier_metrics_inserts(),
        )
        .await;
        metrics::histogram!(
            "mkdb2_run_reaper_barrier_duration_seconds",
            "outcome" => if barrier_result.is_ok() { "success" } else { "error" }
        )
        .record(barrier_started.elapsed().as_secs_f64());
        drop(submission_guard);
        if let Err(error) = barrier_result {
            record_reaper_failure("ch_barrier");
            return Err(error);
        }

        // MergeTree mutations rewrite whole affected parts. Group claimed runs
        // by project so a part is rewritten once per pass instead of once per
        // run, while failures remain isolated from unrelated project
        // partitions. Finalize each successful project batch before starting
        // another mutation so a later hang cannot strand prior successes.
        let project_batches = group_runs_by_project(&runs);
        let drained = delete_and_finalize_project_batches(
            &project_batches,
            |batch| self.delete_claimed_project(batch, pass_started_ms, pass_deadline),
            |batch| self.finalize_deleted_project(batch),
        )
        .await?;
        report.cache_entries_purged += drained.cache_entries_purged;
        report.finalized += drained.finalized;
        report.halt_pass |= drained.halt_pass;
        Ok(report)
    }

    async fn delete_claimed_project(
        &self,
        batch: Vec<RunKey>,
        pass_started_ms: i64,
        pass_deadline: tokio::time::Instant,
    ) -> Result<RunMutationOutcome> {
        anyhow::ensure!(!batch.is_empty(), "project deletion batch cannot be empty");
        let project_id = &batch[0].project_id;
        anyhow::ensure!(
            batch.iter().all(|key| key.project_id == *project_id),
            "project deletion batch mixed project identities"
        );
        if mutation_timeout_with_finalization_headroom(pass_deadline, tokio::time::Instant::now())
            .is_none()
        {
            return Ok(RunMutationOutcome::PassBudgetExhausted);
        }
        timed(
            PG_TIMEOUT,
            "recording a purge attempt",
            self.pg.note_purge_attempts(&batch, pass_started_ms),
        )
        .await?;
        let Some(mutation_timeout) =
            mutation_timeout_with_finalization_headroom(pass_deadline, tokio::time::Instant::now())
        else {
            return Ok(RunMutationOutcome::PassBudgetExhausted);
        };
        let mutation_started = std::time::Instant::now();
        let run_ids = batch
            .iter()
            .map(|key| key.run_id.as_str())
            .collect::<Vec<_>>();
        let mutation = timed(
            mutation_timeout,
            "deleting one project batch from ClickHouse",
            self.ch.delete_runs_sync(project_id, &run_ids),
        )
        .await;
        metrics::histogram!(
            "mkdb2_run_reaper_ch_mutation_duration_seconds",
            "outcome" => if mutation.is_ok() { "success" } else { "error" }
        )
        .record(mutation_started.elapsed().as_secs_f64());
        match mutation {
            Ok(()) => {
                // Cache entries cannot serve useful data once ClickHouse has
                // confirmed the physical delete. Evict before the Postgres
                // finalization await, whose commit acknowledgement can be lost.
                let runs: Vec<_> = batch
                    .iter()
                    .map(|key| (key.project_id.as_str(), key.run_id.as_str()))
                    .collect();
                let cache_entries_purged = self.ch.purge_run_caches(&runs);
                Ok(RunMutationOutcome::Deleted {
                    cache_entries_purged,
                })
            }
            Err(error) => Ok(RunMutationOutcome::Failed(error)),
        }
    }

    async fn finalize_deleted_project(&self, batch: Vec<RunKey>) -> Result<()> {
        anyhow::ensure!(
            !batch.is_empty(),
            "project finalization batch cannot be empty"
        );
        let run_ids: Vec<&str> = batch.iter().map(|key| key.run_id.as_str()).collect();
        let finalize_started = std::time::Instant::now();
        let finalization = timed(
            PG_TIMEOUT,
            "finalizing a purged project batch",
            self.pg.finalize_purged_runs(&batch[0].project_id, &run_ids),
        )
        .await;
        metrics::histogram!(
            "mkdb2_run_reaper_finalize_duration_seconds",
            "outcome" => if finalization.is_ok() { "success" } else { "error" }
        )
        .record(finalize_started.elapsed().as_secs_f64());
        let final_version = match finalization {
            Ok(version) => version,
            Err(error) => {
                record_reaper_failure("finalize");
                // This is the commit whose acknowledgement can be lost. The
                // outer pass emits the resync before retrying next interval.
                return Err(error);
            }
        };
        if let Some(version) = final_version {
            let _ = self.events.send(VersionEvent {
                global: Some(version),
                ..VersionEvent::default()
            });
        }
        Ok(())
    }
}

pub(crate) fn unix_time_seconds() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

async fn timed<T, F>(duration: Duration, label: &'static str, future: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    tokio::time::timeout(duration, future)
        .await
        .with_context(|| format!("timed out after {duration:?} while {label}"))?
        .with_context(|| label)
}

async fn timeout_value<T, F>(duration: Duration, label: &'static str, future: F) -> Result<T>
where
    F: Future<Output = T>,
{
    tokio::time::timeout(duration, future)
        .await
        .with_context(|| format!("timed out after {duration:?} while {label}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_restores_queue_order_after_stable_gate_order() {
        let oldest = crate::lifecycle::RunKey::new("z", "oldest");
        let newest = crate::lifecycle::RunKey::new("a", "newest");
        let rank =
            std::collections::HashMap::from([(oldest.clone(), 0usize), (newest.clone(), 1usize)]);
        let mut claimed = vec![newest.clone(), oldest.clone()];

        restore_candidate_order(&mut claimed, &rank);

        assert_eq!(claimed, vec![oldest, newest]);
    }

    #[test]
    fn project_batches_preserve_first_project_and_run_order() {
        let runs = [
            RunKey::new("second", "oldest"),
            RunKey::new("first", "middle"),
            RunKey::new("second", "newest"),
        ];

        assert_eq!(
            group_runs_by_project(&runs),
            vec![
                vec![runs[0].clone(), runs[2].clone()],
                vec![runs[1].clone()],
            ]
        );
    }

    #[test]
    fn circuit_breaker_stops_the_outer_ready_drain() {
        let full = ReapReport {
            candidates: DEFAULT_BATCH_SIZE,
            ..ReapReport::default()
        };
        assert!(should_continue_ready_drain(&full, DEFAULT_BATCH_SIZE));

        let halted = ReapReport {
            halt_pass: true,
            ..full.clone()
        };
        assert!(!should_continue_ready_drain(&halted, DEFAULT_BATCH_SIZE));

        let short = ReapReport {
            candidates: DEFAULT_BATCH_SIZE - 1,
            ..ReapReport::default()
        };
        assert!(!should_continue_ready_drain(&short, DEFAULT_BATCH_SIZE));
    }

    #[test]
    fn mutation_budget_preserves_finalization_headroom() {
        let started = tokio::time::Instant::now();
        let pass_deadline = started + PASS_TIMEOUT + FINALIZATION_HEADROOM;
        let first = mutation_timeout_with_finalization_headroom(pass_deadline, started)
            .expect("first mutation fits");
        assert_eq!(first, MUTATION_TIMEOUT);

        // A later project receives only the ordinary pass budget left after a
        // full first mutation; neither can consume the minute reserved for
        // finalizing a success at the edge of that budget.
        let after_success = started + first;
        let second = mutation_timeout_with_finalization_headroom(pass_deadline, after_success)
            .expect("second mutation fits");
        assert_eq!(second, PASS_TIMEOUT - MUTATION_TIMEOUT);
        let success_at_budget_edge = after_success + second;
        assert_eq!(
            pass_deadline.saturating_duration_since(success_at_budget_edge),
            FINALIZATION_HEADROOM
        );
        assert!(
            mutation_timeout_with_finalization_headroom(pass_deadline, success_at_budget_edge)
                .is_none()
        );
    }

    #[tokio::test]
    async fn successful_project_batch_is_finalized_before_a_later_failure() {
        let batches = [
            vec![RunKey::new("success", "a"), RunKey::new("success", "b")],
            vec![RunKey::new("failure", "c")],
            vec![RunKey::new("untouched", "d")],
        ];
        let actions = Arc::new(std::sync::Mutex::new(Vec::new()));
        let delete_actions = actions.clone();
        let finalize_actions = actions.clone();

        let report = delete_and_finalize_project_batches(
            &batches,
            move |batch| {
                delete_actions.lock().unwrap().push(format!(
                    "delete:{}:{}",
                    batch[0].project_id,
                    batch.len()
                ));
                let outcome = if batch[0].project_id == "success" {
                    RunMutationOutcome::Deleted {
                        cache_entries_purged: 3,
                    }
                } else {
                    RunMutationOutcome::Failed(anyhow::anyhow!("expected mutation failure"))
                };
                futures::future::ready(Ok(outcome))
            },
            move |batch| {
                finalize_actions.lock().unwrap().push(format!(
                    "finalize:{}:{}",
                    batch[0].project_id,
                    batch.len()
                ));
                futures::future::ready(Ok(()))
            },
        )
        .await
        .unwrap();

        assert_eq!(
            *actions.lock().unwrap(),
            ["delete:success:2", "finalize:success:2", "delete:failure:1",]
        );
        assert_eq!(
            report,
            MutationDrainReport {
                cache_entries_purged: 3,
                finalized: 2,
                halt_pass: true,
            }
        );
    }

    #[tokio::test]
    async fn cache_eviction_precedes_failed_finalization_and_next_mutation() {
        let batches = [
            vec![RunKey::new("first", "a"), RunKey::new("first", "b")],
            vec![RunKey::new("second", "c")],
        ];
        let actions = Arc::new(std::sync::Mutex::new(Vec::new()));
        let delete_actions = actions.clone();
        let finalize_actions = actions.clone();

        let result = delete_and_finalize_project_batches(
            &batches,
            move |batch| {
                let mut actions = delete_actions.lock().unwrap();
                actions.push(format!("delete:{}", batch[0].project_id));
                actions.push(format!("evict:{}", batch[0].project_id));
                futures::future::ready(Ok(RunMutationOutcome::Deleted {
                    cache_entries_purged: batch.len(),
                }))
            },
            move |batch| {
                finalize_actions
                    .lock()
                    .unwrap()
                    .push(format!("finalize:{}", batch[0].project_id));
                futures::future::ready(Err(anyhow::anyhow!("expected finalization failure")))
            },
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            *actions.lock().unwrap(),
            ["delete:first", "evict:first", "finalize:first"]
        );
    }

    #[tokio::test]
    async fn gate_phase_budget_reaches_a_free_tail_after_busy_early_keys() {
        let gates = LifecycleGates::new();
        let busy_a = crate::lifecycle::RunKey::new("p", "a-busy");
        let busy_b = crate::lifecycle::RunKey::new("p", "b-busy");
        let free_tail = crate::lifecycle::RunKey::new("p", "c-free");
        let readers = gates.read_many([busy_a.clone(), busy_b.clone()]).await;

        let (gated, skipped) = acquire_purge_gates(
            &gates,
            &[free_tail.clone(), busy_b.clone(), busy_a.clone()],
            Duration::from_millis(90),
        )
        .await;

        assert_eq!(
            gated.iter().map(|(key, _)| key).collect::<Vec<_>>(),
            vec![&free_tail]
        );
        assert_eq!(skipped, vec![busy_a, busy_b]);
        drop(readers);
    }

    #[tokio::test]
    async fn supervisor_restarts_a_panicked_worker() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let attempts = Arc::new(AtomicUsize::new(0));
        let restarted = Arc::new(tokio::sync::Notify::new());
        let attempts_for_worker = attempts.clone();
        let restarted_for_worker = restarted.clone();
        let (events, _) = tokio::sync::broadcast::channel(1);
        let supervisor = tokio::spawn(supervise_worker(
            "test_worker",
            events,
            Duration::from_millis(1),
            move || {
                let attempt = attempts_for_worker.fetch_add(1, Ordering::SeqCst);
                let restarted = restarted_for_worker.clone();
                async move {
                    if attempt == 0 {
                        panic!("expected worker panic");
                    }
                    restarted.notify_one();
                    futures::future::pending::<()>().await;
                }
            },
        ));

        tokio::time::timeout(Duration::from_secs(1), restarted.notified())
            .await
            .expect("worker was not restarted");
        assert!(attempts.load(Ordering::SeqCst) >= 2);
        supervisor.abort();
    }

    /// Opt-in end-to-end check for the irreversible path.
    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL and KYMO_LIVE_TEST_CLICKHOUSE_URL"]
    async fn live_reaper_claims_resumes_evicts_and_physically_deletes() -> Result<()> {
        let _suite_guard = crate::pg::live_database_suite_gate().lock().await;
        let pg_url = crate::pg::live_test_url("KYMO_LIVE_TEST_DATABASE_URL")?;
        let ch_url = crate::pg::live_test_url("KYMO_LIVE_TEST_CLICKHOUSE_URL")?;
        let suffix = crate::pg::unique_suffix();
        // Spaces and quotes exercise bound predicates plus String-partition
        // lookup in the ClickHouse deletion-headroom guard.
        let project_id = format!("reaper live 'test-{suffix}");
        let resumed_run_id = format!("resumed-{suffix}");
        let fresh_run_id = format!("fresh-{suffix}");
        let run_ids = vec![resumed_run_id.clone(), fresh_run_id.clone()];

        let pg = Arc::new(PgStore::connect(&pg_url).await?);
        let ch = Arc::new(ChClient::new(&ch_url)?);
        tokio::time::timeout(Duration::from_secs(60), ch.ensure_schema())
            .await
            .context("timed out preparing the live-test ClickHouse schema")??;
        for run_id in &run_ids {
            pg.init_run(&project_id, run_id, "live reaper test", None)
                .await
                .map_err(anyhow::Error::from)?;
        }
        let metric_rows = run_ids
            .iter()
            .map(|run_id| crate::clickhouse::MetricRow {
                project_id: project_id.clone(),
                run_id: run_id.clone(),
                metric_name: "loss".to_string(),
                tag: String::new(),
                step: 1,
                timestamp_ms: 1,
                value: Some(1.0),
                cdn_key: None,
                text_data: None,
            })
            .collect::<Vec<_>>();
        ch.insert_batch(&metric_rows, Duration::from_secs(30), false)
            .await?;
        let pending_ingests = run_ids
            .iter()
            .enumerate()
            .map(|(index, run_id)| (run_id.clone(), 1_700_000_000_000 + index as i64))
            .collect::<std::collections::HashMap<_, _>>();
        pg.trash_runs_chunk(&project_id, &run_ids, &pending_ingests)
            .await?;
        for run_id in &run_ids {
            let (record, _) = pg
                .get_run(&project_id, run_id)
                .await?
                .context("trashed run disappeared before deletion")?;
            assert_eq!(
                record.last_ingested_at_ms,
                pending_ingests.get(run_id).copied()
            );
        }
        let unknown_run_id = format!("unknown-{suffix}");
        let mut recoverable_ids = run_ids.clone();
        recoverable_ids.push(unknown_run_id.clone());
        let recoverable_versions = pg
            .poll_versions(Some(&project_id), &recoverable_ids)
            .await?;
        for run_id in &run_ids {
            assert_eq!(recoverable_versions.run_versions.get(run_id), Some(&1));
        }
        assert_eq!(
            recoverable_versions.run_versions.get(&unknown_run_id),
            Some(&0)
        );

        // Age both rows without weakening production's seven-day rule, then
        // preclaim one as if a previous process died; the other exercises a
        // fresh claim in the same pass.
        sqlx::query(
            "UPDATE runs SET deleted_at = NOW() - INTERVAL '8 days'
             WHERE project_id = $1 AND run_id = ANY($2)",
        )
        .bind(&project_id)
        .bind(&run_ids)
        .execute(pg.test_pool())
        .await?;
        let expired_versions = pg.poll_versions(Some(&project_id), &run_ids).await?;
        for run_id in &run_ids {
            // Trash bumps the stored data version from zero to one; expiry is the next synthetic version.
            assert_eq!(expired_versions.run_versions.get(run_id), Some(&2));
        }
        let resumed_key = crate::lifecycle::RunKey::new(&project_id, &resumed_run_id);
        let fresh_key = crate::lifecycle::RunKey::new(&project_id, &fresh_run_id);
        let keys = vec![resumed_key.clone(), fresh_key.clone()];
        let claim = pg
            .claim_purge_runs(std::slice::from_ref(&resumed_key))
            .await?;
        assert_eq!(claim.rows, vec![resumed_key.clone()]);
        assert_eq!(claim.terminal_versions, vec![(resumed_run_id.clone(), 2)]);
        sqlx::query(
            "UPDATE runs SET purge_attempt_at = NOW() - INTERVAL '1 day'
             WHERE project_id = $1 AND run_id = $2",
        )
        .bind(&project_id)
        .bind(&resumed_run_id)
        .execute(pg.test_pool())
        .await?;

        // Exercise the cache-eviction leg as well as both queue branches. An
        // empty entry is sufficient: the test cares about identity eviction,
        // while ClickHouse below independently proves physical row deletion.
        let cached_series = (project_id.clone(), fresh_run_id.clone(), "loss".to_string());
        ch.series_cache()
            .insert_full(cached_series.clone(), Vec::new(), std::time::Instant::now());

        let (events, mut event_rx) = tokio::sync::broadcast::channel(8);
        let restarted = DeletionReaper::new(
            pg.clone(),
            ch.clone(),
            LifecycleGates::new(),
            events,
            ReaperConfig {
                enabled: true,
                first_pass_delay: Duration::from_secs(60),
                interval: Duration::from_secs(60),
                batch_size: 2,
            },
            crate::activity::ActivityTracker::new_local(),
        );
        let live_timeout = Duration::from_secs(5 * 60);
        let pass_deadline = tokio::time::Instant::now() + live_timeout;
        let report = tokio::time::timeout(live_timeout, restarted.reap_ready(pass_deadline))
            .await
            .context("live reaper timed out")??;
        assert_eq!(report.claimed, 2);
        assert_eq!(report.cache_entries_purged, 1);
        assert_eq!(report.finalized, 2);
        let mut announced_terminal_versions = std::collections::HashMap::new();
        while let Ok(event) = event_rx.try_recv() {
            announced_terminal_versions.extend(event.runs);
        }
        for run_id in &run_ids {
            assert_eq!(announced_terminal_versions.get(run_id), Some(&2));
        }
        let purged_versions = pg.poll_versions(Some(&project_id), &run_ids).await?;
        for run_id in &run_ids {
            assert_eq!(purged_versions.run_versions.get(run_id), Some(&2));
        }
        let classes = pg.classify_runs(&keys).await?;
        for key in &keys {
            assert_eq!(
                classes.get(key),
                Some(&crate::pg::RunLifecycleClass::Purged)
            );
            assert!(matches!(
                pg.init_run(&key.project_id, &key.run_id, "must stay purged", None)
                    .await,
                Err(crate::pg::InitRunError::NotInitializable {
                    state: crate::pg::RunLifecycleClass::Purged,
                    ..
                })
            ));
            assert!(ch
                .query_raw(&key.project_id, &key.run_id, "loss", i64::MIN, i64::MAX)
                .await?
                .is_empty());
        }
        // A retry after a lost acknowledgement must reconcile the stable zero
        // without requiring mutation headroom or enqueuing another mutation.
        ch.delete_runs_sync(
            &project_id,
            &run_ids.iter().map(String::as_str).collect::<Vec<_>>(),
        )
        .await?;
        assert!(matches!(
            ch.series_cache().lookup(&cached_series),
            crate::series_cache::Lookup::Miss
        ));

        ch.delete_live_project(&project_id).await?;
        pg.delete_live_projects(&[project_id]).await?;
        Ok(())
    }

    #[test]
    fn destructive_worker_is_opt_in() {
        assert!(
            !crate::env::required_bool("THIS_KYMO_REAPER_VARIABLE_SHOULD_NOT_EXIST", false)
                .unwrap()
        );
    }
}
