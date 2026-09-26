//! Bulk-import data lane (`ImportMetricsBidi`) for replaying archived runs.
//!
//! The live ingest path is shaped for many small concurrent run streams
//! (~10k-row async-insert cuts with per-flush liveness/registry bookkeeping);
//! a replayer of tens of billions of points would hold every live run in
//! sustained spooling for weeks. This lane is the opposite shape: few streams,
//! insert-sized cuts written as synchronous INSERTs, no liveness heartbeats
//! (historical timestamps would only flap the classifier), and a private
//! insert budget so an import never occupies the live path's flush slots.
//! Metric registration derives from the ClickHouse registry outbox the inserts
//! populate: FinalizeImportRun drains the run's rows through the boot
//! reconcile's `drain`; anything a reboot drained is already in Postgres.
//!
//! Wire contract: identical to `IngestMetricsBidi` — cumulative consumed
//! positions, acked only once the covering rows are committed (skipped points
//! advance the position too, and a cut-sized run of them forces a flush so a
//! window-bounded importer never stalls). A failed stream is retried per run by
//! the importer; ClickHouse's ReplacingMergeTree key dedups the overlap. Every
//! cut passes the live path's pre-insert fences (`acquire_write_fences`), so
//! purge and Trash see import writes exactly like live ones. Stream count is
//! deliberately uncapped (memory = streams x one cut); fence waits are
//! unbounded (imports backpressure, never error). Nonempty cuts and committed
//! inserts count as server activity for the local idle supervisor.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::Semaphore;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::clickhouse::{ChClient, MetricRow};
use crate::ingest::{
    metric_row_string_bytes, point_to_row, validate_batch_ids, IngestAckStream,
    MAX_METRIC_NAME_BYTES,
};
use crate::lifecycle::LifecycleGates;
use crate::pg::PgStore;
use crate::proto;

/// Rows per synchronous INSERT. ClickHouse's bulk guidance is 10k-1M rows per
/// insert at a few inserts per second; the default amortizes the per-insert
/// round trip ~25x over the live path's cut while one full cut per stream
/// (flushes are inline) stays ~tens of MiB. Override: KYMO_IMPORT_CUT_ROWS.
const DEFAULT_IMPORT_CUT_ROWS: usize = 250_000;
const MIN_IMPORT_CUT_ROWS: usize = 10_000;
const MAX_IMPORT_CUT_ROWS: usize = 1_000_000;

/// Flush threshold on one cut's owned strings, so a text/CDN-heavy row mix
/// cannot balloon a row-counted cut (numeric rows dominate real archives, so
/// it rarely trips). Checked per batch, so a cut overshoots it by at most one
/// wire message; that plus the row cap bounds a stream's buffered memory.
const IMPORT_CUT_BYTES: usize = 64 * 1024 * 1024;

/// Server-wide concurrent import INSERTs, deliberately small: the budget is
/// what keeps an import from competing with live ingest for ClickHouse
/// resources. Raise only with merge debt and live-path latency on a
/// dashboard. Override: KYMO_IMPORT_CONCURRENCY.
const DEFAULT_IMPORT_CONCURRENCY: usize = 2;
const MAX_IMPORT_CONCURRENCY: usize = 8;

/// Deadline for one import INSERT. Import cuts are ~25x the live path's, and
/// unlike the live path there is no client spool behind a miss — the stream
/// errors and the importer re-imports the run — so give ClickHouse a
/// proportionally long window before declaring the insert dead.
const IMPORT_CH_IO_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone)]
pub struct ImportService {
    ch: Arc<ChClient>,
    pg: Arc<PgStore>,
    gates: LifecycleGates,
    activity: Arc<crate::activity::ActivityTracker>,
    enabled: bool,
    cut_rows: usize,
    insert_slots: Arc<Semaphore>,
}

impl ImportService {
    pub fn new(
        ch: Arc<ChClient>,
        pg: Arc<PgStore>,
        gates: LifecycleGates,
        activity: Arc<crate::activity::ActivityTracker>,
    ) -> Self {
        let enabled = crate::env::bool_or("KYMO_IMPORT_ENABLED", false);
        let cut_rows = crate::env::bounded_usize(
            "KYMO_IMPORT_CUT_ROWS",
            DEFAULT_IMPORT_CUT_ROWS,
            MIN_IMPORT_CUT_ROWS,
            MAX_IMPORT_CUT_ROWS,
        );
        let concurrency = crate::env::bounded_usize(
            "KYMO_IMPORT_CONCURRENCY",
            DEFAULT_IMPORT_CONCURRENCY,
            1,
            MAX_IMPORT_CONCURRENCY,
        );
        tracing::info!(
            enabled,
            cut_rows,
            concurrency,
            "bulk import lane configured"
        );
        Self {
            ch,
            pg,
            gates,
            activity,
            enabled,
            cut_rows,
            insert_slots: Arc::new(Semaphore::new(concurrency)),
        }
    }

    /// One gate for all three import RPCs. FAILED_PRECONDITION (not
    /// UNIMPLEMENTED) so callers can distinguish "server too old" from
    /// "import lane switched off" and report accordingly.
    pub fn require_enabled(&self) -> Result<(), Status> {
        if self.enabled {
            return Ok(());
        }
        Err(Status::failed_precondition(
            "bulk import is disabled on this server (set KYMO_IMPORT_ENABLED=1)",
        ))
    }

    pub async fn import_metrics_bidi(
        &self,
        request: Request<Streaming<proto::MetricsBatch>>,
    ) -> Result<Response<IngestAckStream>, Status> {
        let stream = request.into_inner();
        let service = self.clone();
        // Depth 1: acks are strictly sequential (one per committed cut), so a
        // stalled reader backpressures before the next cut can buffer.
        let (ack_tx, ack_rx) = tokio::sync::mpsc::channel::<Result<proto::IngestAck, Status>>(1);
        tokio::spawn(async move {
            if let Err(status) = run_import_stream(&service, stream, &ack_tx).await {
                // One terminal error frame; the importer re-imports the run
                // (storage dedups the overlap).
                let _ = ack_tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(ack_rx)))
    }
}

struct ImportCut {
    rows: Vec<MetricRow>,
    owned_bytes: usize,
    // Held while the cut is nonempty (same contract as AttachedCut): buffered
    // import data is in-flight work the local idle supervisor must see.
    work: Option<crate::activity::WorkGuard>,
}

impl ImportCut {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            owned_bytes: 0,
            work: None,
        }
    }

    fn push(&mut self, activity: &Arc<crate::activity::ActivityTracker>, row: MetricRow) {
        if self.rows.is_empty() {
            debug_assert!(self.work.is_none());
            self.work = Some(activity.begin_work());
        }
        self.owned_bytes += metric_row_string_bytes(&row) + std::mem::size_of::<MetricRow>();
        self.rows.push(row);
    }
}

async fn flush_import_cut(service: &ImportService, cut: &mut ImportCut) -> Result<(), Status> {
    if cut.rows.is_empty() {
        return Ok(());
    }
    // Private budget first: this wait can be long (few slots, big inserts) and must not hold any lock the reaper or Trash needs.
    let permit = service
        .insert_slots
        .acquire()
        .await
        .map_err(|_| Status::unavailable("import insert scheduler shutting down"))?;
    // The one shared pre-insert fence sequence (see acquire_write_fences):
    // refuses trashed/purging/missing runs, and holding the guards across the
    // insert is what makes the purge reaper's flush-and-verify barrier cover
    // import writes. No admission deadline: imports backpressure, not error.
    let (_run_guards, _submission_guard) =
        crate::ingest::acquire_write_fences(&service.pg, &service.gates, &cut.rows, None).await?;
    let started = std::time::Instant::now();
    let result = service
        .ch
        .insert_batch(&cut.rows, IMPORT_CH_IO_TIMEOUT, true)
        .await;
    drop(permit);
    metrics::histogram!("mkdb2_import_insert_duration_seconds")
        .record(started.elapsed().as_secs_f64());
    match result {
        Ok(()) => {
            metrics::counter!("mkdb2_import_rows_total").increment(cut.rows.len() as u64);
            service.activity.record_committed_ingest();
            cut.rows.clear();
            cut.owned_bytes = 0;
            cut.work = None;
            Ok(())
        }
        Err(error) => {
            metrics::counter!("mkdb2_import_insert_failures_total").increment(1);
            // The cut was never acked; the importer re-imports the run. The
            // shared mapper keeps transport failures retryable (UNAVAILABLE)
            // and deterministic rejections terminal (INTERNAL).
            let (_outcome, status) =
                crate::ingest::clickhouse_insert_error(error, IMPORT_CH_IO_TIMEOUT);
            Err(status)
        }
    }
}

async fn run_import_stream(
    service: &ImportService,
    mut stream: Streaming<proto::MetricsBatch>,
    ack_tx: &tokio::sync::mpsc::Sender<Result<proto::IngestAck, Status>>,
) -> Result<(), Status> {
    let mut cut = ImportCut::new();
    let mut consumed: u64 = 0;
    let mut acked: u64 = 0;
    let mut skipped_invalid: u64 = 0;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        // Unstorable ids poison every Postgres statement they enter (see
        // MAX_ID_BYTES); refuse the whole stream, same as the live paths.
        validate_batch_ids(&batch)?;
        let pid = batch.project_id;
        let rid = batch.run_id;
        for point in batch.points {
            consumed += 1;
            if let Some(row) = point_to_row(point, &pid, &rid, &mut skipped_invalid) {
                cut.push(&service.activity, row);
            }
        }
        // Flush every cut_rows consumed positions — stored or skipped, since
        // stored rows <= consumed - acked this is also the row cap, and the
        // ack below always advances within one cut so a window-bounded
        // importer never stalls on skipped input — or when the byte cap trips.
        if consumed - acked >= service.cut_rows as u64 || cut.owned_bytes >= IMPORT_CUT_BYTES {
            flush_import_cut(service, &mut cut).await?;
        }
        // Empty cut = everything consumed so far is committed or skipped.
        if cut.rows.is_empty() && consumed > acked {
            acked = consumed;
            if ack_tx
                .send(Ok(proto::IngestAck {
                    points_acked: consumed,
                }))
                .await
                .is_err()
            {
                // Client stopped reading acks; committed rows stay committed
                // and its re-import dedups.
                return Ok(());
            }
        }
    }

    flush_import_cut(service, &mut cut).await?;
    if skipped_invalid > 0 {
        tracing::warn!(
            skipped_invalid,
            "Import skipped unstorable points (metric name > {MAX_METRIC_NAME_BYTES} bytes \
             or NUL, or timestamp outside the Postgres range)"
        );
    }
    // Final ack covers every received position, exactly like the live bidi
    // contract — a successful completion is provably fully committed.
    let _ = ack_tx
        .send(Ok(proto::IngestAck {
            points_acked: consumed,
        }))
        .await;
    tracing::info!(consumed, "Import stream completed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numeric_row(name: &str) -> MetricRow {
        MetricRow {
            project_id: "p".into(),
            run_id: "r".into(),
            metric_name: name.into(),
            tag: String::new(),
            step: 1,
            timestamp_ms: 1_700_000_000_000,
            value: Some(1.0),
            cdn_key: None,
            text_data: None,
        }
    }

    #[test]
    fn owned_bytes_count_the_text_payload() {
        let activity = crate::activity::ActivityTracker::new_local();
        let mut cut = ImportCut::new();
        let mut row = numeric_row("text");
        row.value = None;
        row.text_data = Some("x".repeat(1024));
        cut.push(&activity, row);
        // The byte estimate must include the text payload, so text-heavy rows
        // trip the byte cap long before the row cap.
        assert!(cut.owned_bytes >= 1024 + std::mem::size_of::<MetricRow>());
    }
}
