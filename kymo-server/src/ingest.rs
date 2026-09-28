use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures::stream::FuturesOrdered;
use futures::StreamExt;
use tokio::sync::{Notify, OwnedRwLockReadGuard, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::instrument;

use crate::clickhouse::{ChClient, MetricRow, RichMetricRow};
use crate::events::VersionEvent;
use crate::lifecycle::{LifecycleGates, RunKey};
use crate::pg::{
    PgStore, RichMutationCandidate, RichMutationDecision, RunAccessError, RunLifecycleClass,
    TouchedRun,
};
use crate::proto;

const BATCH_FLUSH_SIZE: usize = 10_000;
/// Pin tonic's existing receive limit instead of relying on its default. The
/// maximum ClickHouse payload below is derived from this bound and the fact
/// that this service does not accept compressed gRPC requests.
pub(crate) const MAX_GRPC_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
/// Retained heap memory per data cut. The row-count bound is enough for numeric
/// metrics but not for large text/CDN payloads or tags. We charge collection
/// backing storage and every retained string, then flush after the row that
/// reaches this threshold (at most one-row overshoot).
const BATCH_FLUSH_BYTES: usize = 8 * 1024 * 1024;
/// Conservative RowBinary overhead: six strings with worst-case 10-byte length
/// prefixes, two i64s, three nullable markers, and one present f32.
const ROW_BINARY_MAX_OVERHEAD_PER_ROW: usize = 6 * 10 + 2 * 8 + 3 + 4;
/// Before the row that triggers a flush, retained strings are below 8 MiB. The
/// threshold-crossing row comes from an accepted, uncompressed protobuf message
/// capped at 4 MiB, and lossy UTF-8 repair can expand only its `text_data`, by
/// at most 3x. Add worst-case RowBinary overhead for all 10k rows. The assertion
/// below keeps this construction proof beneath ClickHouse's pinned ceiling.
const MAX_FLUSH_ROW_BINARY_BYTES: usize = BATCH_FLUSH_BYTES
    + 3 * MAX_GRPC_MESSAGE_BYTES
    + BATCH_FLUSH_SIZE * ROW_BINARY_MAX_OVERHEAD_PER_ROW;
const _: () =
    assert!(MAX_FLUSH_ROW_BINARY_BYTES < crate::clickhouse::ASYNC_INSERT_MAX_DATA_SIZE_BYTES);
/// Max time the per-stream buffer can sit unflushed before we force a flush,
/// even if we haven't hit BATCH_FLUSH_SIZE yet. Low-throughput streams (e.g.
/// runs emitting only system metrics at ~7 pts/s) would otherwise take ~22
/// minutes to fill 10k points, leaving `last_system_metric_at_ms` in
/// postgres stale and tripping the UNRESPONSIVE classifier. Keep
/// FLUSH_INTERVAL + BUMP_INTERVAL well under RUNNING_WINDOW_MS (10s) so runs
/// never flap.
const FLUSH_INTERVAL: Duration = Duration::from_millis(2000);
/// Per-chunk send and final-response deadlines enforced inside clickhouse-rs.
/// They remain live after an RPC is cancelled because the insert itself runs in
/// an independently owned task; dropping clickhouse-rs's `Insert::end` future
/// directly would also drop these timers and detach its HTTP task.
///
/// Cache visibility relies on `ChClient` fixing `async_insert=1`,
/// `wait_for_async_insert=1`, and an async-data ceiling above the construction-
/// bounded maximum cut: ClickHouse buffers the complete raw request before
/// evaluating omitted defaults such as `inserted_at`. A successful insert
/// therefore makes its stamped rows visible within this final-response
/// deadline. Per-chunk send delays can lengthen the whole task, but happen
/// before that server-side stamp.
const CH_IO_TIMEOUT: Duration = Duration::from_secs(10);
/// Bound every cancel-safe wait before ClickHouse submission. Once ClickHouse
/// starts, the independently owned task must run to its native deadline; before
/// then, a blocked gate or Postgres read must not retain a flush permit forever.
const PRE_CH_ADMISSION_TIMEOUT: Duration = Duration::from_secs(5);
/// Polling deadline for one flush result. If observed while the task is still
/// pending, the outcome is unknown and the cut is not acknowledged; the owned
/// task stays detached with its permit and native timeout. This timer is not
/// independently driven while response-channel backpressure suspends polling.
const FLUSH_RESPONSE_TIMEOUT: Duration = Duration::from_secs(12);
/// Max flushes one `ingest_metrics_bidi` stream keeps in flight; each overlaps
/// its ~0.25s ClickHouse durability wait with the others, so per-stream
/// throughput climbs with this (sublinearly — docs step 0). Also the read-loop
/// backpressure bound and ack-channel depth. Tunes throughput without changing
/// the per-cut row/byte limits. Override: KYMO_FLUSH_CONCURRENCY.
const DEFAULT_FLUSH_CONCURRENCY: usize = 4;

/// Clamp the knob: every in-flight flush owns a row/byte-bounded cut, so an
/// extreme value would still multiply memory and ClickHouse work. 32 is ample
/// over the useful 4-8 range.
const MAX_FLUSH_CONCURRENCY: usize = 32;

const MEBIBYTE: usize = 1024 * 1024;
/// Default byte budget for buffered cuts; bounds in-flight bytes, never run count. Override: KYMO_INGEST_BYTE_CAP.
const DEFAULT_INGEST_BYTE_CAP: usize = 512 * MEBIBYTE;
/// Floor: a zero cap closes the gate permanently (`used < 0` never holds); 16 MiB keeps two full cuts admissible.
const MIN_INGEST_BYTE_CAP: usize = 16 * MEBIBYTE;
const MAX_INGEST_BYTE_CAP: usize = 16 * 1024 * MEBIBYTE;

/// Keep a small queue of ordered results beyond the active task limit. Later
/// inserts that finish behind one slow predecessor release their permits and
/// rows immediately; this result-only backlog lets replacement work fill those
/// slots without turning ordered ACK emission into head-of-line underutilization.
fn ordered_flush_limit(active_limit: usize) -> usize {
    active_limit * 2
}

pub(crate) fn metric_row_string_bytes(row: &MetricRow) -> usize {
    [
        row.project_id.capacity(),
        row.run_id.capacity(),
        row.metric_name.capacity(),
        row.tag.capacity(),
        row.cdn_key.as_ref().map_or(0, String::capacity),
        row.text_data.as_ref().map_or(0, String::capacity),
    ]
    .into_iter()
    .fold(0, usize::saturating_add)
}

fn row_vec_heap_bytes(capacity: usize) -> usize {
    capacity.saturating_mul(std::mem::size_of::<MetricRow>())
}

fn batch_flush_due(row_count: usize, owned_bytes: usize) -> bool {
    row_count >= BATCH_FLUSH_SIZE || owned_bytes >= BATCH_FLUSH_BYTES
}

/// Soft global budget for retained cut bytes: each stream gates before reading its next message, so usage may exceed the cap by one decoded message's rows per stream; charging never blocks or fails.
struct ByteBudget {
    cap: usize,
    used: std::sync::atomic::AtomicUsize,
    freed: Notify,
}

impl ByteBudget {
    fn new(cap: usize) -> Arc<Self> {
        Arc::new(Self {
            cap,
            used: std::sync::atomic::AtomicUsize::new(0),
            freed: Notify::new(),
        })
    }

    fn has_room(&self) -> bool {
        self.used.load(std::sync::atomic::Ordering::Relaxed) < self.cap
    }

    fn charge(&self, bytes: usize) {
        self.used
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    fn release(&self, bytes: usize) {
        self.used
            .fetch_sub(bytes, std::sync::atomic::Ordering::Relaxed);
        self.freed.notify_waiters();
    }

    fn used_bytes(&self) -> usize {
        self.used.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Waits until usage is below the cap. Compose in front of the stream read inside the select arm, never around the loop — waiting outside the select would also block the flush timer that dispatches an aged cut and eventually releases its charge.
    async fn room(&self) {
        if self.has_room() {
            return;
        }
        loop {
            let freed = self.freed.notified();
            tokio::pin!(freed);
            // Register before re-checking: notify_waiters wakes only already-registered waiters, so a release between the has_room check and the first poll would otherwise be missed.
            freed.as_mut().enable();
            if self.has_room() {
                return;
            }
            freed.await;
        }
    }
}

/// RAII byte-budget charge that moves with a cut from [`AttachedCut`] into [`FlushTaskGuard`]; dropping its current owner releases the accumulated bytes.
struct ByteCharge {
    budget: Arc<ByteBudget>,
    bytes: usize,
}

impl ByteCharge {
    fn new(budget: Arc<ByteBudget>) -> Self {
        Self { budget, bytes: 0 }
    }

    fn charge_to(&mut self, total: usize) {
        if total > self.bytes {
            self.budget.charge(total - self.bytes);
            self.bytes = total;
        }
    }
}

impl Drop for ByteCharge {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.budget.release(self.bytes);
        }
    }
}

#[derive(Default)]
struct AttachedCut {
    rows: Vec<MetricRow>,
    owned_bytes: usize,
    /// Previous cut sizes, retained as allocation-free hints. Storage is
    /// reserved only when another storable row arrives, so a final/idle stream
    /// retains no empty multi-megabyte buffer.
    next_row_capacity: usize,
    charge: Option<ByteCharge>,
    work: Option<crate::activity::WorkGuard>,
}

impl AttachedCut {
    fn push(
        &mut self,
        budget: &Arc<ByteBudget>,
        activity: &Arc<crate::activity::ActivityTracker>,
        row: MetricRow,
    ) -> bool {
        if self.rows.is_empty() {
            debug_assert!(self.charge.is_none());
            debug_assert!(self.work.is_none());
            self.charge = Some(ByteCharge::new(budget.clone()));
            self.work = Some(activity.begin_work());

            // Restore the preceding cut's useful capacity lazily. Reserving at
            // dispatch would leave an idle stream holding an empty 10k buffer.
            if self.next_row_capacity > 0 {
                self.rows.reserve_exact(self.next_row_capacity);
                self.next_row_capacity = 0;
            }
            self.owned_bytes = row_vec_heap_bytes(self.rows.capacity());
        }

        let row_capacity_before = self.rows.capacity();
        let string_bytes = metric_row_string_bytes(&row);
        self.rows.push(row);
        let collection_growth = row_vec_heap_bytes(self.rows.capacity())
            .saturating_sub(row_vec_heap_bytes(row_capacity_before));
        self.owned_bytes = self
            .owned_bytes
            .saturating_add(collection_growth)
            .saturating_add(string_bytes);
        self.charge
            .as_mut()
            .expect("nonempty attached cut owns a charge")
            .charge_to(self.owned_bytes);
        batch_flush_due(self.rows.len(), self.owned_bytes)
    }

    fn take(&mut self) -> (Vec<MetricRow>, ByteCharge, crate::activity::WorkGuard) {
        debug_assert!(!self.rows.is_empty());
        self.owned_bytes = 0;
        self.next_row_capacity = self.rows.len();
        let rows = std::mem::take(&mut self.rows);
        let charge = self
            .charge
            .take()
            .expect("nonempty attached cut must own a charge");
        let work = self
            .work
            .take()
            .expect("nonempty attached cut must own an activity guard");
        (rows, charge, work)
    }

    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

enum UnaryEvent {
    Stream(Option<Result<proto::MetricsBatch, Status>>),
    Flush,
}

async fn next_unary_event<S>(
    stream: &mut S,
    flush_tick: &mut tokio::time::Interval,
    cut_nonempty: bool,
    budget: &ByteBudget,
) -> UnaryEvent
where
    S: futures::Stream<Item = Result<proto::MetricsBatch, Status>> + Unpin,
{
    tokio::select! {
        biased;
        _ = flush_tick.tick(), if cut_nonempty => UnaryEvent::Flush,
        next = async {
            budget.room().await;
            stream.next().await
        } => UnaryEvent::Stream(next),
    }
}

/// Interval between coalesced bump writes (see [`BumpCoalescer`]). Worst-case heartbeat staleness is FLUSH_INTERVAL + BUMP_INTERVAL ≈ 4s, inside the 10s RUNNING_WINDOW_MS.
const BUMP_INTERVAL: Duration = Duration::from_millis(2000);
const SYSTEM_METRIC_PREFIX: &str = "system/";

#[derive(Clone)]
pub struct IngestService {
    ch: Arc<ChClient>,
    pg: Arc<PgStore>,
    gates: LifecycleGates,
    /// Flushes only mark state dirty here; the coalescer owns all Postgres bookkeeping (version/liveness bumps, metric registration) and the dashboard push (see [`BumpCoalescer`]).
    bumps: Arc<BumpCoalescer>,
    /// Active ClickHouse insert limit, shared service-wide across ingest RPCs.
    flush_concurrency: usize,
    flush_slots: Arc<Semaphore>,
    byte_budget: Arc<ByteBudget>,
    /// Unary and bidi inserts share the service-wide cap by default. Setting
    /// KYMO_CAP_UNARY_FLUSHES=false is an explicit rollout escape hatch.
    cap_unary_flushes: bool,
}

/// Clear the known-metrics map past this size rather than grow unbounded
/// across months of runs (it only suppresses redundant idempotent writes).
const KNOWN_CAP: usize = 1_000_000;
/// Admission cap on the low-latency Postgres write-behind (near-empty except while Postgres is down). Additional new metrics remain durable in the ClickHouse outbox for boot reconciliation. In-flight entries stay in the map until `absorb_committed`, so admission always counts them.
const WRITE_BEHIND_REGISTRATION_CAP: usize = 100_000;
/// Deadline on the registry statement alone. The bump must reclaim the task by the next tick even when the registry write hangs (lock, black-holed connection): heartbeats have a 10s liveness window, the registry has none. A timed-out statement is outcome-unknown — exactly what reg_in_flight already handles. Registry-only pathology is thus bounded at one tick + 4s; only bump slowness itself (Postgres-wide trouble) can stretch heartbeat cadence further, and no worker split would save that.
const REG_TIMEOUT: Duration = Duration::from_secs(4);

/// Identifier limits enforced at the ingest boundary. Postgres TEXT rejects NUL bytes and the run_metrics primary-key index rejects rows past ~2.7KB; ClickHouse accepts both — so an unstorable name would land data no metric list can ever show and fail its whole registry batch on every retry. These are the only data-dependent ways the registry statement can fail (three TEXT columns, UTF-8 guaranteed by proto), so refusing them here makes batch poisoning impossible. Budget: 2048 + 2×256 = 2560 stays under the ~2670 usable bytes of a 3-TEXT-column index row, and sits close enough to the real constraint that "previously storable, now skipped" is an empty set in practice — old clients don't check points_received, so a skip is silent for them.
pub(crate) const MAX_ID_BYTES: usize = 256;
pub(crate) const MAX_METRIC_NAME_BYTES: usize = 2048;
const MAX_RICH_RESOURCE_ID_BYTES: usize = 2048;
// Keep the four TEXT columns in rich_mutation_heads' primary key below the
// same conservative btree payload budget used by run_metrics' three-column
// key. Checking each column independently would allow an index-poisoning row.
const MAX_RICH_HEAD_KEY_BYTES: usize = 2560;
/// Reserved by the frontend's global `/trash` route.
pub(crate) const RESERVED_PROJECT_ID: &str = "trash";

/// Timestamp limits at the same boundary, for the bump statement: to_timestamp() raises outside roughly 4713 BC..294276 AD, and one such value poisons the single UPDATE carrying EVERY run's liveness (and via the bump-first early return starves registration), retried forever since ClickHouse stores any i64. Bounds are the Postgres range rounded inward; the mistake this actually catches is a client logging nanoseconds instead of milliseconds.
const MIN_TIMESTAMP_MS: i64 = -200_000_000_000_000; // ≈ 4400 BC
const MAX_TIMESTAMP_MS: i64 = 9_000_000_000_000_000; // ≈ year 287,000

/// Storable in the runs table's last_*_metric_at columns (see bounds above).
pub(crate) fn storable_timestamp(ts_ms: i64) -> bool {
    (MIN_TIMESTAMP_MS..=MAX_TIMESTAMP_MS).contains(&ts_ms)
}

/// Storable in Postgres under the limits above. Empty strings stay allowed — rejecting them is a compatibility question, not a poison fix. Shared with InitRun (query.rs): a run that could be created but never ingested would just spool its client's data forever.
pub(crate) fn storable_ident(s: &str, max_bytes: usize) -> bool {
    s.len() <= max_bytes && !s.contains('\0')
}

pub(crate) fn is_reserved_project_id(project_id: &str) -> bool {
    project_id == RESERVED_PROJECT_ID
}

pub(crate) fn validate_batch_ids(batch: &proto::MetricsBatch) -> Result<(), Status> {
    if is_reserved_project_id(&batch.project_id) {
        return Err(Status::invalid_argument(format!(
            "project_id '{}' is reserved",
            RESERVED_PROJECT_ID
        )));
    }
    if storable_ident(&batch.project_id, MAX_ID_BYTES)
        && storable_ident(&batch.run_id, MAX_ID_BYTES)
    {
        Ok(())
    } else {
        Err(Status::invalid_argument(format!(
            "project_id/run_id must be at most {MAX_ID_BYTES} bytes with no NUL bytes"
        )))
    }
}

fn storable_rich_head_key(req: &proto::PublishRichMutationRequest) -> bool {
    storable_ident(&req.project_id, MAX_ID_BYTES)
        && storable_ident(&req.run_id, MAX_ID_BYTES)
        && storable_ident(&req.metric_name, MAX_METRIC_NAME_BYTES)
        && storable_ident(&req.tag, MAX_RICH_HEAD_KEY_BYTES)
        && req.project_id.len() + req.run_id.len() + req.metric_name.len() + req.tag.len()
            <= MAX_RICH_HEAD_KEY_BYTES
}

/// CDN < NUMERIC < TEXT_STREAM — the precedence the old ClickHouse multiIf
/// encoded: a metric that ever logged text is a text stream, else numeric
/// if it ever logged a value, else CDN.
fn type_precedence(t: &str) -> u8 {
    match t {
        "TEXT_STREAM" => 3,
        "NUMERIC" => 2,
        _ => 1,
    }
}

fn point_metric_type(row: &MetricRow) -> &'static str {
    if row.text_data.is_some() {
        "TEXT_STREAM"
    } else if row.value.is_some() {
        "NUMERIC"
    } else {
        "CDN"
    }
}

/// Bookkeeping derived only after ClickHouse accepts a flush. Keys borrow the
/// still-owned rows, so the hot ingest path does not allocate a `(project, run)`
/// pair per point; `mark_dirty` clones each distinct key once when absorbing it.
struct BatchMetadata<'a> {
    heartbeats: HashMap<(&'a str, &'a str), RunHeartbeat>,
    candidates: HashMap<(&'a str, &'a str, &'a str), &'static str>,
}

fn batch_metadata(rows: &[MetricRow]) -> BatchMetadata<'_> {
    let mut heartbeats = HashMap::new();
    let mut candidates: HashMap<(&str, &str, &str), &'static str> = HashMap::new();
    for row in rows {
        heartbeats
            .entry((row.project_id.as_str(), row.run_id.as_str()))
            .or_insert_with(RunHeartbeat::default)
            .record(&row.metric_name, row.timestamp_ms);

        let metric_type = point_metric_type(row);
        candidates
            .entry((
                row.project_id.as_str(),
                row.run_id.as_str(),
                row.metric_name.as_str(),
            ))
            .and_modify(|cur| {
                if type_precedence(metric_type) > type_precedence(cur) {
                    *cur = metric_type;
                }
            })
            .or_insert(metric_type);
    }
    BatchMetadata {
        heartbeats,
        candidates,
    }
}

/// Per-run heartbeat aggregator: tracks the max main-metric and max
/// system-metric timestamp observed in the current flush window. Reset after
/// each flush so we only roll forward what's new.
#[derive(Default, Clone, Copy)]
struct RunHeartbeat {
    max_main_ms: Option<i64>,
    max_system_ms: Option<i64>,
}

impl RunHeartbeat {
    fn record(&mut self, metric_name: &str, ts_ms: i64) {
        let slot = if metric_name.starts_with(SYSTEM_METRIC_PREFIX) {
            &mut self.max_system_ms
        } else {
            &mut self.max_main_ms
        };
        *slot = Some(slot.map_or(ts_ms, |cur| cur.max(ts_ms)));
    }

    /// Max-merge another window: coalescing can never move a timestamp
    /// backwards (Option's Ord: None < Some, Some compares values).
    fn merge(&mut self, other: RunHeartbeat) {
        self.max_main_ms = self.max_main_ms.max(other.max_main_ms);
        self.max_system_ms = self.max_system_ms.max(other.max_system_ms);
    }
}

/// Dirty generation paired with the max timestamp values. Timestamp equality
/// cannot identify a flush window: a second cut can commit while a Postgres
/// bump of the first is in flight and carry the same maxima. The generation
/// makes that second cut survive absorption for the next bump.
#[derive(Default, Clone, Copy)]
struct DirtyHeartbeat {
    heartbeat: RunHeartbeat,
    generation: u64,
    /// Server time captured after ClickHouse committed the newest coalesced
    /// flush. Retaining it here preserves the original time across PG retries.
    last_ingested_at_ms: i64,
}

/// Write-behind coalescer for the per-run version/liveness bumps and
/// first-seen metric registrations.
///
/// Bumping inline per stream flush made every ingest ACK wait on its own committed Postgres UPDATE — concurrent streams queued on the runs row locks, exhausted the pool (1–2.3s per single-row statement), and backpressured kymo's send queue. Flushes now max-merge into the dirty state and one task writes the union per BUMP_INTERVAL: one statement, at most one row-touch per active run, regardless of stream count. Registration rode the flush path (with its own retry buffer) until it moved in here too — the ingest ACK now waits on nothing but ClickHouse, and there is one retry mechanism instead of two. Writes stay best-effort — a missed write (error, crash) is healed by the run's next flush.
pub struct BumpCoalescer {
    state: std::sync::Mutex<CoalescerState>,
    /// Set by SIGTERM or the local self-fence RPC: flushes refuse new work so the shutdown drain can quiesce instead of racing arriving marks (see spawn).
    draining: std::sync::atomic::AtomicBool,
    shutdown: tokio::sync::Notify,
    activity: Arc<crate::activity::ActivityTracker>,
}

#[derive(Default)]
struct CoalescerState {
    /// Runs touched since the last write → max timestamps and dirty generation.
    heartbeats: HashMap<(String, String), DirtyHeartbeat>,
    /// Registrations awaiting commit: (project, run, metric) → highest-precedence type observed. Subsumes the old flush-inline registry write and its failed-write retry buffer. Entries stay here while their write is in flight — a failed write needs no merge-back, absorb_committed removes committed ones, and the admission cap counts in-flight work.
    registrations: HashMap<(String, String, String), &'static str>,
    /// Registry changes committed but not yet announced — they ride the next successful bump's event frame (one frame = "data landed" + "re-list metrics", as the inline path sent it), held across ticks when a bump fails. Populated only from register_run_metrics' RETURNING post-commit, so the announcement never precedes the registry rows and a redundant retry can't recompute (and lose) it.
    metrics_changed: std::collections::BTreeSet<String>,
    /// Run bumps committed but not yet announced: run_id → committed version. Staged under the same lock that absorbs heartbeats: the registry statement between bump commit and event send is a cancellation point (tick timeout, SIGTERM budget), and once heartbeats are absorbed nothing else can regenerate the event — a cancelled tick would delay the dashboard until the visible-tab minute poll or the run's next activity. Plain insert, no max-merge: one sequential writer and versions only move forward.
    runs_changed: HashMap<String, u64>,
    /// Terminal-ingest project bumps committed with the run bookkeeping but
    /// not yet announced. These make ListRuns refresh after delayed replay for
    /// explicitly finished or crashed runs.
    projects_changed: HashMap<String, u64>,
    /// Run ids of the registry statement currently in flight — staged BEFORE the await, cleared when the attempt confirms. An unknown-outcome attempt (cancelled mid-await, or an error that may still have committed — a connection lost after the statement reached Postgres) gets its runs folded into [`Self::metrics_changed`] wholesale, safe on both outcomes: committed means the rows are there and the announcement is right; not committed means the entries are still pending, so the real commit announces itself via RETURNING and the early announcement cost one harmless no-op re-list. Cancellation needs no handler — the NEXT attempt finding this non-empty IS the detection.
    reg_in_flight: std::collections::BTreeSet<String>,
    /// Metrics already committed to the registry, (project, run, metric) → type precedence — registration costs a write only on a metric's first appearance or type upgrade, not per flush. Starts empty each boot: re-registering is an idempotent ON CONFLICT, and the re-send doubles as self-healing for anything a previous process missed (e.g. a metric first logged during a deploy overlap). Purely an optimization, so CLEARED when it outgrows [`KNOWN_CAP`] — repopulation costs one redundant upsert per still-active metric. Written only post-commit, so an entry never claims a registration that didn't land.
    known: HashMap<(String, String, String), u8>,
}

impl CoalescerState {
    fn has_pending_work(&self) -> bool {
        !self.heartbeats.is_empty()
            || !self.registrations.is_empty()
            || !self.metrics_changed.is_empty()
            || !self.runs_changed.is_empty()
            || !self.projects_changed.is_empty()
    }

    /// Precedence-max merge one pending registration. Cap admission is mark_dirty's concern, not this rule's: a key that was ever accepted must never be silently dropped.
    fn merge_registration(&mut self, key: (String, String, String), t: &'static str) {
        self.registrations
            .entry(key)
            .and_modify(|cur| {
                if type_precedence(t) > type_precedence(cur) {
                    *cur = t;
                }
            })
            .or_insert(t);
    }

    /// Up to `max` pending registrations for one write statement — chunked so a post-outage backlog can't produce a single statement slow enough to eat the tick timeout and stall heartbeats; the registry has no deadline and drains over a few ticks.
    fn registration_batch(&self, max: usize) -> Vec<((String, String, String), &'static str)> {
        self.registrations
            .iter()
            .take(max)
            .map(|(k, &t)| (k.clone(), t))
            .collect()
    }

    /// Clear only the generation that committed. Every successful ClickHouse
    /// cut advances it, including an equal-timestamp cut completed mid-write.
    fn absorb_heartbeats(&mut self, batch: &[((String, String), DirtyHeartbeat)]) {
        for (key, written) in batch {
            if self
                .heartbeats
                .get(key)
                .is_some_and(|current| current.generation == written.generation)
            {
                self.heartbeats.remove(key);
            }
        }
    }

    /// Fold a committed write batch into `known` and clear it from pending — an entry a flush upgraded mid-write stays for the next tick to write the higher type. `known` max-merges the committed precedence: a stale lower-typed commit must never downgrade what it already records (the registry itself only upgrades).
    fn absorb_committed(&mut self, batch: Vec<((String, String, String), &'static str)>) {
        if self.known.len() > KNOWN_CAP {
            self.known.clear();
        }
        for (key, t) in batch {
            let p = type_precedence(t);
            if self
                .registrations
                .get(&key)
                .is_some_and(|&cur| type_precedence(cur) <= p)
            {
                self.registrations.remove(&key);
            }
            self.known
                .entry(key)
                .and_modify(|k| *k = (*k).max(p))
                .or_insert(p);
        }
    }
}

impl BumpCoalescer {
    fn new(activity: Arc<crate::activity::ActivityTracker>) -> Self {
        Self {
            state: std::sync::Mutex::new(CoalescerState::default()),
            draining: std::sync::atomic::AtomicBool::new(false),
            shutdown: tokio::sync::Notify::new(),
            activity,
        }
    }

    /// Return the newest committed-ingest timestamp still awaiting its
    /// Postgres write for each requested run. Trash snapshots under exclusive
    /// run gates so deletion cannot discard it; Terminate snapshots under its
    /// shared gate so the lifecycle refresh normally includes the final ACK
    /// without delaying reads or ingest behind a queued writer.
    pub fn pending_last_ingested_at_ms(
        &self,
        project_id: &str,
        run_ids: &[String],
    ) -> HashMap<String, i64> {
        let wanted: HashSet<&str> = run_ids.iter().map(String::as_str).collect();
        let state = self.state.lock().unwrap();
        state
            .heartbeats
            .iter()
            .filter_map(|((pid, run_id), dirty)| {
                (pid == project_id && wanted.contains(run_id.as_str()))
                    .then_some((run_id.clone(), dirty.last_ingested_at_ms))
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn empty_for_test() -> Arc<Self> {
        Arc::new(Self::new(crate::activity::ActivityTracker::new_local()))
    }

    /// Enter the one-way admission fence and wake the existing bounded drain.
    /// The caller may lose its response when the process exits; supervisors
    /// prove completion from endpoint closure rather than trusting the RPC ACK.
    pub(crate) fn request_shutdown(&self) {
        if !self
            .draining
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            self.shutdown.notify_one();
        }
    }

    pub(crate) fn is_draining(&self) -> bool {
        self.draining.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Start the write-behind task; ingest flushes mark the returned handle dirty.
    pub fn spawn(
        pg: Arc<PgStore>,
        events: crate::events::EventSender,
        activity: Arc<crate::activity::ActivityTracker>,
    ) -> Arc<Self> {
        let this = Arc::new(Self::new(activity));
        let coalescer = this.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(BUMP_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // Without a drain, every SIGTERM (deploys are the common restart) throws away up to BUMP_INTERVAL of accepted liveness/version bookkeeping. Metric discovery is also drained promptly, but its ClickHouse materialized-view outbox is the hard-kill backstop and is reconciled on boot. This handler owns process exit; the drain is timeboxed so a down Postgres can't stall pod termination into the SIGKILL grace period.
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("install SIGTERM handler");
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        // Timeboxed: a hung statement (black-holed connection, no client-side statement timeout) must not block the loop — and with it heartbeat ticks and SIGTERM observation. Cancellation loses nothing: dirty state stays in its maps until absorbed post-commit, staged announcements (projects_changed / runs_changed / metrics_changed) hold until a frame actually sends, an unknown-outcome registry attempt is announced by the next attempt (see reg_in_flight), and a cancelled bump simply re-bumps.
                        let _ = tokio::time::timeout(
                            Duration::from_secs(10),
                            coalescer.write_dirty(&pg, &events),
                        )
                        .await;
                    }
                    _ = term.recv() => {
                        coalescer.request_shutdown();
                    }
                    _ = coalescer.shutdown.notified() => {}
                }
                if coalescer.is_draining() {
                    // Quiesce, then drain until empty: new flushes are refused (the client spools and re-sends after the restart, exactly as under the old hard kill), and marks land before their stream can ACK — so anything ACKed before a drain pass is in the maps that pass reads. Not airtight: a flush past the draining check but still awaiting ClickHouse has no marks yet, and if the maps read empty right then, exit() kills it unACKed (the client re-sends, storage dedups). Only marks+ACK racing into the microseconds before exit() can be ACKed yet lost — accepted, like the hard-kill window but ~10^5× smaller.
                    let deadline = Instant::now() + Duration::from_secs(5);
                    loop {
                        let _ = tokio::time::timeout(
                            Duration::from_secs(3),
                            coalescer.write_dirty(&pg, &events),
                        )
                        .await;
                        let empty = {
                            let state = coalescer.state.lock().unwrap();
                            !state.has_pending_work()
                        };
                        if empty || Instant::now() >= deadline {
                            tracing::info!(
                                drained = empty,
                                "shutdown: ingest bookkeeping drain finished, exiting"
                            );
                            std::process::exit(0);
                        }
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        });
        this
    }

    /// Merge one flush window into the dirty state. On the ingest hot path — the mutex is held only for the merge, never across an await. `candidates` (the flush's distinct metric → best type map) is filtered against `known` here, under the same lock that guards the pending registrations.
    fn mark_dirty<'a>(
        &self,
        heartbeats: impl Iterator<Item = ((&'a str, &'a str), RunHeartbeat)>,
        candidates: HashMap<(&str, &str, &str), &'static str>,
        ingested_at_ms: i64,
    ) {
        self.activity.record_committed_ingest();
        let mut state = self.state.lock().unwrap();
        for ((project_id, run_id), hb) in heartbeats {
            let key = (project_id.to_owned(), run_id.to_owned());
            let dirty = state
                .heartbeats
                .entry(key)
                .or_insert_with(|| DirtyHeartbeat {
                    last_ingested_at_ms: ingested_at_ms,
                    ..Default::default()
                });
            dirty.heartbeat.merge(hb);
            dirty.last_ingested_at_ms = dirty.last_ingested_at_ms.max(ingested_at_ms);
            dirty.generation += 1;
        }
        let mut dropped = 0usize;
        for ((p, r, m), t) in candidates {
            let key = (p.to_string(), r.to_string(), m.to_string());
            if state
                .known
                .get(&key)
                .is_some_and(|&k| type_precedence(t) <= k)
            {
                continue;
            }
            // Cap admission happens here, once. In-flight batches are still in the map (see absorb_committed), so the bound holds across failed writes; a key refused here re-candidates on the metric's next flush.
            if state.registrations.len() >= WRITE_BEHIND_REGISTRATION_CAP
                && !state.registrations.contains_key(&key)
            {
                dropped += 1;
                continue;
            }
            state.merge_registration(key, t);
        }
        if dropped > 0 {
            tracing::warn!(
                "Deferred {dropped} metric registrations; metrics seen again will be reconsidered on their next flush, with boot reconciliation as the fallback \
                 (write-behind cap {WRITE_BEHIND_REGISTRATION_CAP})"
            );
        }
    }

    /// Write the dirty state as two INDEPENDENT statements — version/liveness bumps first, then registry upserts — publishing one event for whatever committed. Deliberately not one transaction: the registry insert can fail data-dependently (an unstorable metric name passes proto and ClickHouse but is rejected by the run_metrics primary key), and coupled writes would retry that forever, starving every run's liveness over one pathological name; failing alone, the registry statement stalls only registration.
    ///
    /// Bumps go first because only they have a deadline: heartbeat staleness past RUNNING_WINDOW_MS (10s) flaps runs UNRESPONSIVE, and a large post-outage registration batch must not queue in front of that. The push event still publishes after both statements, so an announcement never precedes its registry rows; only the poll path can glimpse a bumped version before the metric list catches up — the same state a registration-failure tick already exposes.
    async fn write_dirty(&self, pg: &PgStore, events: &crate::events::EventSender) {
        // Dirty state is SNAPSHOTTED, never drained: entries stay in their maps until the matching absorb removes them post-commit, so a failed OR cancelled write loses nothing.
        let hb_batch: Vec<((String, String), DirtyHeartbeat)> = {
            let state = self.state.lock().unwrap();
            if !state.has_pending_work() {
                return;
            }
            state
                .heartbeats
                .iter()
                .map(|(k, &hb)| (k.clone(), hb))
                .collect()
        };

        let touched: Vec<TouchedRun> = hb_batch
            .iter()
            .map(|((pid, rid), dirty)| TouchedRun {
                project_id: pid.clone(),
                run_id: rid.clone(),
                max_main_metric_at_ms: dirty.heartbeat.max_main_ms,
                max_system_metric_at_ms: dirty.heartbeat.max_system_ms,
                last_ingested_at_ms: dirty.last_ingested_at_ms,
            })
            .collect();
        if !touched.is_empty() {
            let start = Instant::now();
            let result = pg.bump_run_versions(&touched).await;
            // Recorded on failure too — a slow-then-erroring Postgres must show up here, not vanish from the histogram.
            metrics::histogram!("mkdb2_bump_flush_duration_seconds")
                .record(start.elapsed().as_secs_f64());
            match result {
                Ok(bumped) => {
                    // Absorb and stage the announcement under ONE lock: the registry await below can cancel this future, and absorbed heartbeats can't regenerate the event — the committed versions must already be in runs_changed/projects_changed by the time we could vanish.
                    let mut state = self.state.lock().unwrap();
                    state.absorb_heartbeats(&hb_batch);
                    state.runs_changed.extend(bumped.runs);
                    state.projects_changed.extend(bumped.projects);
                }
                Err(e) => {
                    // Nothing left the maps — the next tick retries. Postgres is likely down, so skip the registry statement too. Staged announcements keep.
                    tracing::warn!("Failed to bump run/project versions (will retry): {e}");
                    return;
                }
            }
        }

        // Snapshot after the bump: an outage tick (bump failed, early return) never pays the clone, and marks made during the bump await ride this tick's batch.
        let reg_batch = {
            let state = self.state.lock().unwrap();
            state.registration_batch(crate::pg::RUN_METRICS_BATCH_ROWS)
        };
        if !reg_batch.is_empty() {
            let regs: Vec<(String, String, String, String)> = reg_batch
                .iter()
                .map(|((p, r, m), t)| (p.clone(), r.clone(), m.clone(), (*t).to_string()))
                .collect();
            {
                // Stage this attempt's run ids BEFORE the await: a cancelled tick leaves them in reg_in_flight, and the next attempt finding them here means this one never resolved — outcome unknown, announce its runs (see reg_in_flight).
                let mut state = self.state.lock().unwrap();
                let leftover = std::mem::take(&mut state.reg_in_flight);
                state.metrics_changed.extend(leftover);
                state.reg_in_flight = reg_batch.iter().map(|((_, r, _), _)| r.clone()).collect();
            }
            match tokio::time::timeout(REG_TIMEOUT, pg.register_run_metrics(&regs)).await {
                Ok(Ok(changed)) => {
                    let mut state = self.state.lock().unwrap();
                    state.absorb_committed(reg_batch);
                    state.metrics_changed.extend(changed);
                    state.reg_in_flight.clear();
                }
                // Error or deadline (see REG_TIMEOUT): entries never left the pending map; the next tick retries them as-is. Neither case proves the statement didn't commit (the connection can die — or be abandoned — after it reached Postgres), so the attempt's runs are announced as unknown-outcome rather than forgotten.
                outcome => {
                    match &outcome {
                        Ok(Err(e)) => {
                            tracing::warn!("Failed to register run metrics (will retry): {e}")
                        }
                        _ => tracing::warn!(
                            "Registry statement exceeded {}s (will retry); outcome unknown",
                            REG_TIMEOUT.as_secs()
                        ),
                    }
                    let mut state = self.state.lock().unwrap();
                    let attempted = std::mem::take(&mut state.reg_in_flight);
                    state.metrics_changed.extend(attempted);
                }
            }
        }
        // Take-and-send with no await in between, so both announcement sets either leave in this frame or stay staged for the next tick.
        let ev = {
            let mut state = self.state.lock().unwrap();
            VersionEvent {
                runs: std::mem::take(&mut state.runs_changed)
                    .into_iter()
                    .collect(),
                projects: std::mem::take(&mut state.projects_changed)
                    .into_iter()
                    .collect(),
                metrics_changed_runs: std::mem::take(&mut state.metrics_changed)
                    .into_iter()
                    .collect(),
                ..Default::default()
            }
        };
        if !ev.is_empty() {
            // Err just means no dashboard is connected right now.
            let _ = events.send(ev);
        }
    }
}

impl IngestService {
    pub fn new(
        ch: Arc<ChClient>,
        bumps: Arc<BumpCoalescer>,
        pg: Arc<PgStore>,
        gates: LifecycleGates,
    ) -> Self {
        let flush_concurrency = crate::env::required_bounded_usize(
            "KYMO_FLUSH_CONCURRENCY",
            DEFAULT_FLUSH_CONCURRENCY,
            1,
            MAX_FLUSH_CONCURRENCY,
        )
        .expect("invalid kymo flush-concurrency environment");
        let ingest_byte_cap = crate::env::required_bounded_usize(
            "KYMO_INGEST_BYTE_CAP",
            DEFAULT_INGEST_BYTE_CAP,
            MIN_INGEST_BYTE_CAP,
            MAX_INGEST_BYTE_CAP,
        )
        .expect("invalid kymo ingest-byte-cap environment");
        let cap_unary_flushes = crate::env::required_bool("KYMO_CAP_UNARY_FLUSHES", true)
            .expect("invalid kymo unary-flush environment");
        tracing::info!(
            flush_concurrency,
            ingest_byte_cap,
            cap_unary_flushes,
            "ingest flush admission configured"
        );
        let flush_slots = Arc::new(Semaphore::new(flush_concurrency));
        let byte_budget = ByteBudget::new(ingest_byte_cap);
        metrics::gauge!("mkdb2_ingest_byte_cap").set(ingest_byte_cap as f64);
        // Sampling keeps gauge writes off the per-row path, and reading permits from the semaphore counts holders no task guard sees (rich mutations). new() runs inside async startup, so spawn is legal; process-lifetime task.
        let sampled_slots = flush_slots.clone();
        let sampled_budget = byte_budget.clone();
        tokio::spawn(async move {
            let permits = metrics::gauge!("mkdb2_flush_permits_available");
            let buffered = metrics::gauge!("mkdb2_ingest_buffered_bytes");
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                permits.set(sampled_slots.available_permits() as f64);
                buffered.set(sampled_budget.used_bytes() as f64);
            }
        });
        Self {
            ch,
            pg,
            gates,
            bumps,
            flush_concurrency,
            flush_slots,
            byte_budget,
            cap_unary_flushes,
        }
    }

    #[instrument(skip_all)]
    pub async fn ingest_metrics(
        &self,
        request: Request<Streaming<proto::MetricsBatch>>,
    ) -> Result<Response<proto::IngestResponse>, Status> {
        let mut stream = request.into_inner();
        let mut cut = AttachedCut::default();
        let mut total_points: u64 = 0;
        let mut skipped_invalid: u64 = 0;
        let mut flush_tick = tokio::time::interval(FLUSH_INTERVAL);
        flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let batch = match next_unary_event(
                &mut stream,
                &mut flush_tick,
                !cut.is_empty(),
                &self.byte_budget,
            )
            .await
            {
                UnaryEvent::Stream(Some(batch)) => batch?,
                UnaryEvent::Stream(None) => break,
                UnaryEvent::Flush => {
                    self.flush(&mut cut).await?;
                    continue;
                }
            };
            // Unstorable ids poison every Postgres statement they enter (see MAX_ID_BYTES); refuse them before any row lands anywhere.
            validate_batch_ids(&batch)?;
            let pid = batch.project_id;
            let rid = batch.run_id;

            for point in batch.points {
                if let Some(row) = point_to_row(point, &pid, &rid, &mut skipped_invalid) {
                    total_points += 1;
                    let was_empty = cut.is_empty();
                    if cut.push(&self.byte_budget, &self.bumps.activity, row) {
                        self.flush(&mut cut).await?;
                    } else if was_empty {
                        // `interval`'s first tick is immediate. Arm it from the
                        // first retained row so an idle tail gets a full window.
                        flush_tick.reset();
                    }
                }
            }
        }

        if !cut.is_empty() {
            self.flush(&mut cut).await?;
        }

        if skipped_invalid > 0 {
            tracing::warn!(
                skipped_invalid,
                "Skipped unstorable points (metric name > {MAX_METRIC_NAME_BYTES} bytes or NUL, or timestamp outside the Postgres range)"
            );
        }
        tracing::info!(total_points, "Ingest stream completed");
        Ok(Response::new(proto::IngestResponse {
            points_received: total_points,
        }))
    }

    /// Pipelined ingest (docs/kymo-pipelined-upload.md). The read-and-ack loop
    /// runs in one spawned task feeding a bounded channel wrapped as the response
    /// stream; that channel's backpressure keeps the task from running ahead of a
    /// client that stopped reading acks.
    #[instrument(skip_all)]
    pub async fn ingest_metrics_bidi(
        &self,
        request: Request<Streaming<proto::MetricsBatch>>,
    ) -> Result<Response<IngestAckStream>, Status> {
        let stream = request.into_inner();
        let service = self.clone();
        // Active inserts own the row buffers; the extra ordered backlog holds
        // only tiny completion results waiting behind an earlier ACK gap.
        let (ack_tx, ack_rx) = tokio::sync::mpsc::channel::<Result<proto::IngestAck, Status>>(
            ordered_flush_limit(self.flush_concurrency) + 1,
        );
        tokio::spawn(async move {
            if let Err(status) = run_bidi_ingest(&service, stream, &ack_tx).await {
                // One terminal error frame; unacked flushes are the client's to re-send.
                let _ = ack_tx.send(Err(status)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(ack_rx)))
    }

    #[instrument(skip_all)]
    pub async fn publish_rich_mutation(
        &self,
        request: Request<proto::PublishRichMutationRequest>,
    ) -> Result<Response<proto::PublishRichMutationResponse>, Status> {
        let req = request.into_inner();
        if is_reserved_project_id(&req.project_id)
            || !storable_rich_head_key(&req)
            || !storable_ident(&req.cdn_key, MAX_RICH_RESOURCE_ID_BYTES)
            || !storable_timestamp(req.timestamp_ms)
        {
            return Err(Status::invalid_argument(
                "rich mutation contains an unstorable identifier, resource id, or timestamp",
            ));
        }
        let _work = self.bumps.activity.begin_work();

        let metadata_row = MetricRow {
            project_id: req.project_id.clone(),
            run_id: req.run_id.clone(),
            metric_name: req.metric_name.clone(),
            tag: req.tag.clone(),
            step: req.step,
            timestamp_ms: req.timestamp_ms,
            value: None,
            cdn_key: Some(req.cdn_key.clone()),
            text_data: None,
        };
        let Some(mutation_version) = req.mutation_version else {
            flush_rows_inner(
                &self.ch,
                &self.pg,
                &self.gates,
                &self.bumps,
                vec![metadata_row],
            )
            .await?;
            return Ok(Response::new(proto::PublishRichMutationResponse {
                disposition: proto::RichMutationDisposition::RichMutationAccepted as i32,
                stored_version: None,
            }));
        };
        if mutation_version >> 32 == 0 || mutation_version as u32 == 0 {
            return Err(Status::invalid_argument(
                "mutation_version must contain nonzero writer_epoch and mutation_seq fields",
            ));
        }
        if self
            .bumps
            .draining
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(Status::unavailable("server shutting down"));
        }
        // Share the same global ClickHouse admission budget as streamed and
        // legacy unary inserts. The client keeps this ordered head until the
        // response, so waiting here adds backpressure without losing work.
        let _flush_permit = if self.cap_unary_flushes {
            Some(self.acquire_flush_slot().await?)
        } else {
            None
        };
        if self.bumps.is_draining() {
            return Err(Status::unavailable("server shutting down"));
        }

        let key = RunKey::new(&req.project_id, &req.run_id);
        let admission_deadline = tokio::time::Instant::now() + PRE_CH_ADMISSION_TIMEOUT;
        let _run_guards = wait_for_pre_ch_admission(
            admission_deadline,
            "rich_run_gate",
            self.gates.read_many([key.clone()]),
        )
        .await?;
        wait_for_pre_ch_admission(
            admission_deadline,
            "rich_lifecycle_check",
            self.pg.ensure_runs_active(&[key]),
        )
        .await?
        .map_err(run_access_status)?;
        let _submission_guard = wait_for_pre_ch_admission(
            admission_deadline,
            "rich_submission_gate",
            self.gates.read_submission(),
        )
        .await?;

        let decision = wait_for_pre_ch_admission(
            admission_deadline,
            "rich_head_cas",
            self.pg.compare_rich_mutation(RichMutationCandidate {
                project_id: &req.project_id,
                run_id: &req.run_id,
                metric_name: &req.metric_name,
                tag: &req.tag,
                step: req.step,
                mutation_version,
                public_resource_id: &req.cdn_key,
            }),
        )
        .await?
        .map_err(|error| Status::unavailable(format!("rich mutation CAS failed: {error}")))?;
        let disposition = match decision {
            RichMutationDecision::Superseded { stored_version } => {
                return Ok(Response::new(proto::PublishRichMutationResponse {
                    disposition: proto::RichMutationDisposition::RichMutationSuperseded as i32,
                    stored_version: Some(stored_version),
                }));
            }
            RichMutationDecision::Conflict { stored_resource_id } => {
                return Err(Status::data_loss(format!(
                    "mutation version {mutation_version} already names a different resource ({stored_resource_id})"
                )));
            }
            RichMutationDecision::UnallocatedEpoch { current_epoch } => {
                return Err(Status::data_loss(format!(
                    "mutation version {mutation_version} uses a writer epoch newer than the run's allocated epoch {current_epoch}"
                )));
            }
            RichMutationDecision::Accepted => proto::RichMutationDisposition::RichMutationAccepted,
            RichMutationDecision::Idempotent => {
                proto::RichMutationDisposition::RichMutationIdempotent
            }
        };

        let rich_row = RichMetricRow {
            project_id: req.project_id,
            run_id: req.run_id,
            metric_name: req.metric_name,
            tag: req.tag,
            step: req.step,
            timestamp_ms: req.timestamp_ms,
            cdn_key: req.cdn_key,
            mutation_version,
        };
        self.ch
            .insert_rich_mutation(&rich_row, CH_IO_TIMEOUT)
            .await
            .map_err(|error| match error {
                clickhouse::error::Error::TimedOut => Status::unavailable(format!(
                    "ClickHouse rich insert exceeded an I/O deadline of {}s; outcome unknown",
                    CH_IO_TIMEOUT.as_secs()
                )),
                error => Status::internal(format!("ClickHouse rich insert failed: {error}")),
            })?;
        let rows = [metadata_row];
        let metadata = batch_metadata(&rows);
        self.bumps.mark_dirty(
            metadata.heartbeats.into_iter(),
            metadata.candidates,
            chrono::Utc::now().timestamp_millis(),
        );
        metrics::counter!("mkdb2_ingest_points_total").increment(1);
        Ok(Response::new(proto::PublishRichMutationResponse {
            disposition: disposition as i32,
            stored_version: Some(mutation_version),
        }))
    }

    async fn flush(&self, cut: &mut AttachedCut) -> Result<(), Status> {
        // Admission precedes detachment, so an overloaded service retains one
        // attached retryable buffer instead of accumulating task-owned buffers.
        let permit = if self.cap_unary_flushes {
            Some(self.acquire_flush_slot().await?)
        } else {
            None
        };
        let (rows, charge, work) = cut.take();
        let deadline = tokio::time::Instant::now() + FLUSH_RESPONSE_TIMEOUT;
        let task = self.spawn_owned_flush(rows, permit, None, work, charge);
        await_owned_flush(task, deadline, None).await
    }

    /// Unbounded FIFO wait. Bidi callers keep disconnect cancellation armed around it, unary and rich-mutation waits die with their dropped RPC futures, and shutdown work is refused by flush_rows_inner's drain check.
    async fn acquire_flush_slot(&self) -> Result<OwnedSemaphorePermit, Status> {
        let started = Instant::now();
        let acquired = self.flush_slots.clone().acquire_owned().await;
        metrics::histogram!("mkdb2_flush_permit_wait_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        acquired.map_err(|_| Status::unavailable("ingest flush scheduler shutting down"))
    }
}

// ---------------------------------------------------------------------------
// Bidi ingest (IngestMetricsBidi) — the pipelined path.
// ---------------------------------------------------------------------------

/// The response stream `ingest_metrics_bidi` returns.
pub type IngestAckStream = ReceiverStream<Result<proto::IngestAck, Status>>;

/// One in-flight flush. Boxed so all cut sites push the same type; output is the
/// cut's watermark or the abort status.
type BoxFlush = Pin<Box<dyn Future<Output = Result<u64, Status>> + Send + 'static>>;

#[derive(Default)]
struct ActiveFlushes {
    /// Local backpressure: a stream that owns all its permits must not pre-queue another cut on the global semaphore.
    count: usize,
    row_keys: HashSet<u64>,
}

#[derive(Default)]
struct StreamTaskState {
    /// Count and key ownership move together at task admission/release, so one lock keeps capacity and same-key decisions on a single snapshot.
    active: Mutex<ActiveFlushes>,
    /// First task or response-deadline failure. An owned-task failure is
    /// published before that task releases its keys; a response timeout is
    /// published while the detached task still owns them. `FuturesOrdered` can
    /// hide either later error behind a slow predecessor, so this latch stops
    /// admission during that ordered-result gap.
    failure: OnceLock<Status>,
    completed: Notify,
}

impl StreamTaskState {
    fn admission(&self, row_keys: &[u64], limit: usize) -> (bool, bool) {
        let active = self.active.lock().unwrap_or_else(|e| e.into_inner());
        let conflict = row_keys.iter().any(|key| active.row_keys.contains(key));
        (active.count < limit && !conflict, conflict)
    }

    fn failure(&self) -> Option<Status> {
        self.failure.get().cloned()
    }

    fn record_failure(&self, status: Status) {
        if self.failure.set(status).is_ok() {
            self.completed.notify_one();
        }
    }
}

struct StreamTaskOwnership {
    state: Arc<StreamTaskState>,
    /// One fingerprint per retained row. Duplicates need no separate
    /// deduplication: the active union stores them once, and all removals happen
    /// under the same mutex after overlapping tasks have been excluded.
    row_keys: Vec<u64>,
}

/// Lives inside the independently owned insert task. Its drop is the single
/// release point for the service permit, byte charge, per-stream active count,
/// same-key barrier, and active-task gauge — including task panic/runtime
/// cancellation.
struct FlushTaskGuard {
    permit: Option<OwnedSemaphorePermit>,
    _charge: ByteCharge,
    stream: Option<StreamTaskOwnership>,
    finished: bool,
}

impl FlushTaskGuard {
    fn new(
        permit: Option<OwnedSemaphorePermit>,
        stream: Option<(Arc<StreamTaskState>, Vec<u64>)>,
        charge: ByteCharge,
    ) -> Self {
        let stream = stream.map(|(state, row_keys)| {
            {
                let mut active = state.active.lock().unwrap_or_else(|e| e.into_inner());
                debug_assert!(!row_keys.iter().any(|key| active.row_keys.contains(key)));
                active.row_keys.extend(row_keys.iter().copied());
                active.count += 1;
            }
            StreamTaskOwnership { state, row_keys }
        });
        metrics::gauge!("mkdb2_ch_insert_tasks_active").increment(1.0);
        Self {
            permit,
            _charge: charge,
            stream,
            finished: false,
        }
    }

    /// Publish a task error while its same-key ownership is still held. A
    /// successful result needs no side channel: it remains ordered by the
    /// `FuturesOrdered` wrapper.
    fn finish(&mut self, result: &Result<(), Status>) {
        if let (Some(stream), Err(status)) = (&self.stream, result) {
            stream.state.record_failure(status.clone());
        }
        self.finished = true;
    }
}

impl Drop for FlushTaskGuard {
    fn drop(&mut self) {
        // Covers task panic and runtime cancellation. Normal error returns are
        // recorded by `finish` with their original status.
        if !self.finished {
            if let Some(stream) = &self.stream {
                stream.state.record_failure(Status::internal(
                    "ClickHouse flush task ended without a result",
                ));
            }
        }
        if let Some(stream) = self.stream.take() {
            {
                let mut active = stream
                    .state
                    .active
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                for key in stream.row_keys {
                    active.row_keys.remove(&key);
                }
                debug_assert!(active.count > 0);
                // Release the global slot before advertising local capacity.
                self.permit.take();
                active.count -= 1;
                if active.count == 0 {
                    debug_assert!(active.row_keys.is_empty());
                    // A long-lived stream should not retain its peak D-cut
                    // fingerprint table after a high-throughput burst ends.
                    active.row_keys = HashSet::new();
                }
            }
            stream.state.completed.notify_one();
        } else {
            self.permit.take();
        }
        metrics::gauge!("mkdb2_ch_insert_tasks_active").decrement(1.0);
    }
}

impl IngestService {
    fn spawn_owned_flush(
        &self,
        rows: Vec<MetricRow>,
        permit: Option<OwnedSemaphorePermit>,
        stream: Option<(Arc<StreamTaskState>, Vec<u64>)>,
        work: crate::activity::WorkGuard,
        charge: ByteCharge,
    ) -> JoinHandle<Result<(), Status>> {
        // Construct outside the async body so runtime shutdown before first poll still releases every piece of admission state.
        let ch = self.ch.clone();
        let pg = self.pg.clone();
        let gates = self.gates.clone();
        let bumps = self.bumps.clone();
        let guard = FlushTaskGuard::new(permit, stream, charge);
        tokio::spawn(async move {
            let _work = work;
            let mut guard = guard;
            let result = flush_rows_inner(&ch, &pg, &gates, &bumps, rows).await;
            guard.finish(&result);
            result
        })
    }
}

async fn await_owned_flush(
    mut task: JoinHandle<Result<(), Status>>,
    deadline: tokio::time::Instant,
    stream: Option<&StreamTaskState>,
) -> Result<(), Status> {
    match tokio::time::timeout_at(deadline, &mut task).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(Status::internal(format!(
            "ClickHouse flush task failed: {error}"
        ))),
        Err(_) => {
            // Dropping the JoinHandle detaches rather than aborts. The task keeps
            // its permit/key ownership and native clickhouse-rs timers.
            metrics::counter!("mkdb2_ch_insert_response_timeouts_total").increment(1);
            let status = Status::unavailable(format!(
                "ClickHouse insert exceeded {}s; outcome unknown",
                FLUSH_RESPONSE_TIMEOUT.as_secs()
            ));
            if let Some(stream) = stream {
                stream.record_failure(status.clone());
            }
            Err(status)
        }
    }
}

struct BidiState {
    cut: AttachedCut,
    inflight: FuturesOrdered<BoxFlush>,
    tasks: Arc<StreamTaskState>,
    consumed: u64,
    last_pushed: u64,
    skipped_invalid: u64,
}

impl BidiState {
    fn new() -> Self {
        Self {
            cut: AttachedCut::default(),
            inflight: FuturesOrdered::new(),
            tasks: Arc::new(StreamTaskState::default()),
            consumed: 0,
            last_pushed: 0,
            skipped_invalid: 0,
        }
    }

    async fn emit_result(
        &mut self,
        result: Result<u64, Status>,
        ack_tx: &tokio::sync::mpsc::Sender<Result<proto::IngestAck, Status>>,
    ) -> Result<(), Status> {
        let watermark = result?;
        ack_tx
            .send(Ok(proto::IngestAck {
                points_acked: watermark,
            }))
            .await
            .map_err(|_| Status::cancelled("client dropped the ack stream"))
    }

    async fn wait_for_capacity(
        &mut self,
        service: &IngestService,
        ack_tx: &tokio::sync::mpsc::Sender<Result<proto::IngestAck, Status>>,
        row_keys: Option<&[u64]>,
    ) -> Result<(), Status> {
        let ordered_limit = ordered_flush_limit(service.flush_concurrency);
        let wait_started = Instant::now();
        let mut waited_on_conflict = false;
        loop {
            if let Some(failure) = self.tasks.failure() {
                return Err(failure);
            }
            let (has_insert_capacity, conflict) = row_keys.map_or((true, false), |keys| {
                self.tasks.admission(keys, service.flush_concurrency)
            });
            waited_on_conflict |= conflict;
            if has_insert_capacity && self.inflight.len() < ordered_limit {
                // A failing task records its status before removing its keys.
                // Recheck after observing no conflict to close that handoff
                // race without coupling ACK order to completion order.
                if let Some(failure) = self.tasks.failure() {
                    return Err(failure);
                }
                if waited_on_conflict {
                    metrics::histogram!("mkdb2_flush_same_key_wait_duration_seconds")
                        .record(wait_started.elapsed().as_secs_f64());
                }
                return Ok(());
            }

            let tasks = self.tasks.clone();
            tokio::select! {
                biased;
                Some(result) = self.inflight.next(), if !self.inflight.is_empty() => {
                    self.emit_result(result, ack_tx).await?;
                }
                _ = tasks.completed.notified() => {}
                _ = ack_tx.closed() => {
                    return Err(Status::cancelled("client dropped the ack stream"));
                }
            }
        }
    }

    /// Admit and enqueue the current cut. Data cuts acquire the service permit
    /// before [`AttachedCut::take`]; ack-only cuts occupy only an ordered result.
    async fn dispatch_cut(
        &mut self,
        service: &IngestService,
        ack_tx: &tokio::sync::mpsc::Sender<Result<proto::IngestAck, Status>>,
    ) -> Result<(), Status> {
        let watermark = self.consumed;
        if self.cut.is_empty() {
            self.wait_for_capacity(service, ack_tx, None).await?;
            self.inflight
                .push_back(Box::pin(async move { Ok(watermark) }));
            self.last_pushed = watermark;
            metrics::counter!("mkdb2_ingest_flush_cuts_total", "kind" => "ack_only").increment(1);
            return Ok(());
        }

        // Fingerprints exist only while a completed bidi cut waits/runs. Keep
        // the attached buffer lean while it is still accepting rows; the row
        // bound makes this compact dispatch-time vector bounded as well.
        let row_keys = row_key_fingerprints(&self.cut.rows);
        self.wait_for_capacity(service, ack_tx, Some(&row_keys))
            .await?;
        let acquire = service.acquire_flush_slot();
        tokio::pin!(acquire);
        let tasks = self.tasks.clone();
        let permit = loop {
            tokio::select! {
                result = &mut acquire => break result?,
                Some(result) = self.inflight.next(), if !self.inflight.is_empty() => {
                    self.emit_result(result, ack_tx).await?;
                }
                _ = tasks.completed.notified() => {
                    if let Some(failure) = tasks.failure() {
                        return Err(failure);
                    }
                }
                _ = ack_tx.closed() => {
                    return Err(Status::cancelled("client dropped the ack stream"));
                }
            }
        };
        // A disjoint task can fail while global admission is queued, and its
        // guard may be what releases this permit. Do not turn that wakeup into
        // one more insert after the stream is already known-fatal.
        if let Some(failure) = tasks.failure() {
            return Err(failure);
        }
        // Permit readiness can race client cancellation in the select above.
        // Keep the still-attached buffer retryable instead of starting one
        // unacknowledgeable insert after the response stream is gone.
        if ack_tx.is_closed() {
            return Err(Status::cancelled("client dropped the ack stream"));
        }

        let (rows, charge, work) = self.cut.take();
        let deadline = tokio::time::Instant::now() + FLUSH_RESPONSE_TIMEOUT;
        let task = service.spawn_owned_flush(
            rows,
            Some(permit),
            Some((tasks.clone(), row_keys)),
            work,
            charge,
        );
        self.inflight.push_back(Box::pin(async move {
            await_owned_flush(task, deadline, Some(&tasks))
                .await
                .map(|()| watermark)
        }));
        self.last_pushed = watermark;
        metrics::counter!("mkdb2_ingest_flush_cuts_total", "kind" => "data").increment(1);
        Ok(())
    }
}

fn should_rearm_bidi_flush_timer(
    was_pending: bool,
    is_pending: bool,
    previous_watermark: u64,
    current_watermark: u64,
) -> bool {
    previous_watermark != current_watermark || (!was_pending && is_pending)
}

/// Buffer points, cut row/byte-bounded (or aged) flushes onto a [`FuturesOrdered`] that
/// overlaps their ClickHouse waits, and ack each cumulatively as it commits. In
/// submission order — which IS the "highest contiguous committed" frontier, so
/// acks stay monotone with no seq/frontier bookkeeping. First flush error aborts;
/// `Ok(())` on a clean half-close.
async fn run_bidi_ingest(
    service: &IngestService,
    mut stream: Streaming<proto::MetricsBatch>,
    ack_tx: &tokio::sync::mpsc::Sender<Result<proto::IngestAck, Status>>,
) -> Result<(), Status> {
    let mut state = BidiState::new();
    let mut stream_done = false;
    let mut tick = tokio::time::interval(FLUSH_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.reset();

    loop {
        let previous_watermark = state.last_pushed;
        let was_pending = state.consumed > state.last_pushed;
        let tasks = state.tasks.clone();
        tokio::select! {
            biased;
            _ = ack_tx.closed() => {
                return Err(Status::cancelled("client dropped the ack stream"));
            }
            // Drain a committed flush in order → cumulative ack; `?` aborts on error.
            Some(result) = state.inflight.next(), if !state.inflight.is_empty() => {
                state.emit_result(result, ack_tx).await?;
            }
            // A later task can fail while FuturesOrdered is still waiting for
            // an earlier cut. Stop admission immediately; already committed
            // but unacknowledged cuts are safe for the client to replay.
            _ = tasks.completed.notified() => {
                if let Some(failure) = tasks.failure() {
                    return Err(failure);
                }
            }
            // Prioritize the age bound over an always-ready request stream. This
            // is also what gives an all-skipped suffix its timer ACK under load.
            _ = tick.tick() => {
                if state.consumed > state.last_pushed {
                    state.dispatch_cut(service, ack_tx).await?;
                }
            }
            // One attached buffer may fill while D tasks run; admission at every cut supplies the actual backpressure, including inside a large batch.
            maybe = async {
                service.byte_budget.room().await;
                stream.next().await
            }, if !stream_done => {
                match maybe {
                    Some(batch) => process_batch(batch?, service, &mut state, ack_tx).await?,
                    None => {
                        stream_done = true;
                        if state.consumed > state.last_pushed {
                            state.dispatch_cut(service, ack_tx).await?;
                        }
                    }
                }
            }
        }
        let is_pending = state.consumed > state.last_pushed;
        // Give the first unresolved position after an idle period a full
        // batching window. Re-arm after every dispatch as well, including when
        // one large request produces a size cut followed by a partial tail.
        if should_rearm_bidi_flush_timer(
            was_pending,
            is_pending,
            previous_watermark,
            state.last_pushed,
        ) {
            tick.reset();
        }
        if stream_done && state.inflight.is_empty() {
            break;
        }
    }

    if state.skipped_invalid > 0 {
        tracing::warn!(
            skipped_invalid = state.skipped_invalid,
            "Skipped unstorable points (metric name > {MAX_METRIC_NAME_BYTES} bytes or NUL, or timestamp outside the Postgres range)"
        );
    }
    tracing::info!(
        points_consumed = state.consumed,
        "Bidi ingest stream completed"
    );
    Ok(())
}

/// Validate and convert one wire point into a `MetricRow`. `None` = skip: an
/// unstorable name/timestamp (counted in `skipped_invalid`) or a payload-less
/// point. A skipped point must not reach ClickHouse or post-insert bookkeeping:
/// an unstorable name would land rows no metric list can ever show; an
/// unstorable timestamp would poison the bump statement (see
/// MIN/MAX_TIMESTAMP_MS), and one that far outside reality is a client unit
/// bug, not data worth keeping. Shared by both ingest paths so "what is
/// storable" is defined exactly once.
pub(crate) fn point_to_row(
    point: proto::MetricPoint,
    pid: &str,
    rid: &str,
    skipped_invalid: &mut u64,
) -> Option<MetricRow> {
    if !storable_ident(&point.metric_name, MAX_METRIC_NAME_BYTES)
        || !storable_timestamp(point.timestamp_ms)
    {
        *skipped_invalid += 1;
        return None;
    }
    let (value, cdn_key, text_data) = match point.payload {
        Some(proto::metric_point::Payload::Value(v)) => (Some(v), None, None),
        Some(proto::metric_point::Payload::CdnKey(k)) => (None, Some(k), None),
        Some(proto::metric_point::Payload::TextData(t)) => {
            let text = String::from_utf8(t)
                .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned());
            (None, None, Some(text))
        }
        None => return None,
    };
    Some(MetricRow {
        project_id: pid.to_owned(),
        run_id: rid.to_owned(),
        metric_name: point.metric_name,
        tag: point.tag,
        step: point.step,
        timestamp_ms: point.timestamp_ms,
        value,
        cdn_key,
        text_data,
    })
}

/// One batch's worth of the read loop: validate ids, then per point advance
/// `consumed`, skip-or-buffer, and cut a flush when the buffer fills.
async fn process_batch(
    batch: proto::MetricsBatch,
    service: &IngestService,
    state: &mut BidiState,
    ack_tx: &tokio::sync::mpsc::Sender<Result<proto::IngestAck, Status>>,
) -> Result<(), Status> {
    // Unstorable ids poison every Postgres statement they enter (see MAX_ID_BYTES); refuse the whole stream.
    validate_batch_ids(&batch)?;
    let pid = batch.project_id;
    let rid = batch.run_id;

    for point in batch.points {
        // Count the position FIRST: skips + payload-less must advance the watermark
        // too, else the client's `del buffer[:delta]` desyncs (see IngestAck).
        state.consumed += 1;
        if let Some(row) = point_to_row(point, &pid, &rid, &mut state.skipped_invalid) {
            if state
                .cut
                .push(&service.byte_budget, &service.bumps.activity, row)
            {
                state.dispatch_cut(service, ack_tx).await?;
            }
        }
    }
    Ok(())
}

fn row_key_fingerprint(row: &MetricRow) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    (
        &row.project_id,
        &row.run_id,
        &row.metric_name,
        &row.tag,
        row.step,
    )
        .hash(&mut hasher);
    hasher.finish()
}

fn row_key_fingerprints(rows: &[MetricRow]) -> Vec<u64> {
    rows.iter().map(row_key_fingerprint).collect()
}

async fn wait_for_pre_ch_admission<T>(
    deadline: tokio::time::Instant,
    stage: &'static str,
    future: impl Future<Output = T>,
) -> Result<T, Status> {
    let started = Instant::now();
    let result = tokio::time::timeout_at(deadline, future).await;
    metrics::histogram!(
        "mkdb2_ingest_admission_wait_duration_seconds",
        "stage" => stage,
        "outcome" => if result.is_ok() { "success" } else { "timeout" }
    )
    .record(started.elapsed().as_secs_f64());
    result.map_err(|_| {
        Status::unavailable(format!(
            "pre-insert admission exceeded {}s",
            PRE_CH_ADMISSION_TIMEOUT.as_secs()
        ))
    })
}

/// Map a lifecycle-check refusal onto the metric-write status contract. One
/// mapping for every path that inserts metric rows (live flush, rich
/// mutations, bulk import): missing/purged reads as not_found, trash-bound
/// states as failed_precondition, store trouble as retryable unavailable.
pub(crate) fn run_access_status(error: RunAccessError) -> Status {
    match error {
        RunAccessError::Store(error) => {
            Status::unavailable(format!("run lifecycle check failed: {error}"))
        }
        RunAccessError::NotActive {
            state: RunLifecycleClass::Missing | RunLifecycleClass::Purged,
            ..
        } => Status::not_found("run does not exist"),
        RunAccessError::NotActive { .. } => {
            Status::failed_precondition("run is in Trash and no longer writable")
        }
        RunAccessError::NotReadable { .. } => Status::failed_precondition("run is not writable"),
    }
}

/// The pre-insert fence sequence every batched ClickHouse metric write
/// passes, defined once: per-run read gates -> lifecycle check -> shared
/// submission gate, with both guards returned to be held across the insert.
/// This is the protocol the purge reaper's claim-then-barrier design and
/// Trash's write-gate claim count on — a write that skips it can commit rows
/// after a purge's zero-row verification. `admission_deadline` bounds each
/// wait (a timed-out live flush falls back to the client spool); the
/// bulk-import lane passes None, preferring indefinite backpressure over
/// erroring.
pub(crate) async fn acquire_write_fences(
    pg: &PgStore,
    gates: &LifecycleGates,
    rows: &[MetricRow],
    admission_deadline: Option<tokio::time::Instant>,
) -> Result<(Vec<OwnedRwLockReadGuard<()>>, OwnedRwLockReadGuard<()>), Status> {
    async fn bounded<T>(
        deadline: Option<tokio::time::Instant>,
        stage: &'static str,
        future: impl Future<Output = T>,
    ) -> Result<T, Status> {
        match deadline {
            Some(deadline) => wait_for_pre_ch_admission(deadline, stage, future).await,
            None => Ok(future.await),
        }
    }

    let mut seen = HashSet::new();
    let mut keys = Vec::new();
    for row in rows {
        if seen.insert((row.project_id.as_str(), row.run_id.as_str())) {
            keys.push(RunKey::new(&row.project_id, &row.run_id));
        }
    }
    keys.sort_unstable();
    let run_guards = bounded(
        admission_deadline,
        "run_gate",
        gates.read_many(keys.clone()),
    )
    .await?;

    let lifecycle_started = Instant::now();
    let lifecycle_result = bounded(
        admission_deadline,
        "lifecycle_check",
        pg.ensure_runs_active(&keys),
    )
    .await;
    let lifecycle_outcome = match &lifecycle_result {
        Ok(Ok(())) => "active",
        Ok(Err(RunAccessError::NotActive { .. } | RunAccessError::NotReadable { .. })) => {
            "rejected"
        }
        Ok(Err(RunAccessError::Store(_))) => "error",
        Err(_) => "timeout",
    };
    metrics::histogram!(
        "mkdb2_ingest_lifecycle_check_duration_seconds",
        "outcome" => lifecycle_outcome
    )
    .record(lifecycle_started.elapsed().as_secs_f64());
    lifecycle_result?.map_err(run_access_status)?;

    let submission_guard = bounded(
        admission_deadline,
        "submission_gate",
        gates.read_submission(),
    )
    .await?;
    Ok((run_guards, submission_guard))
}

async fn flush_rows_inner(
    ch: &ChClient,
    pg: &PgStore,
    gates: &LifecycleGates,
    bumps: &BumpCoalescer,
    rows: Vec<MetricRow>,
) -> Result<(), Status> {
    debug_assert!(!rows.is_empty());
    // SIGTERM drain: refuse work; the client spools and re-sends after restart (as unary `flush`).
    if bumps.draining.load(std::sync::atomic::Ordering::Relaxed) {
        return Err(Status::unavailable("server shutting down"));
    }

    // These guards live in the owned flush task, not the client RPC. A client
    // timeout detaches the task, but Trash still waits for its native CH
    // completion before changing lifecycle state.
    let admission_deadline = tokio::time::Instant::now() + PRE_CH_ADMISSION_TIMEOUT;
    let (_run_guards, _submission_guard) =
        acquire_write_fences(pg, gates, &rows, Some(admission_deadline)).await?;

    let n = rows.len() as u64;
    let flush_start = Instant::now();
    let result = ch.insert_batch(&rows, CH_IO_TIMEOUT, false).await;
    let insert_duration = flush_start.elapsed().as_secs_f64();
    metrics::histogram!("mkdb2_ch_insert_duration_seconds").record(insert_duration);
    match result {
        Ok(()) => {
            metrics::counter!("mkdb2_ch_insert_attempts_total", "outcome" => "success")
                .increment(1);
        }
        Err(error) => {
            let (outcome, status) = clickhouse_insert_error(error, CH_IO_TIMEOUT);
            metrics::counter!("mkdb2_ch_insert_attempts_total", "outcome" => outcome).increment(1);
            metrics::histogram!("mkdb2_flush_duration_seconds").record(insert_duration);
            return Err(status);
        }
    }
    metrics::counter!("mkdb2_ingest_points_total").increment(n);

    // Registration candidates come only from a batch whose CH insert succeeded, so the registry never lists a metric whose data write failed. The materialized-view outbox is already durable at this point; this marks the low-latency Postgres path dirty. Everything else (known-metric filter, Postgres writes, retry) lives in the coalescer, so the ack never waits on the metadata store (see BumpCoalescer).
    let metadata = batch_metadata(&rows);
    bumps.mark_dirty(
        metadata.heartbeats.into_iter(),
        metadata.candidates,
        chrono::Utc::now().timestamp_millis(),
    );

    metrics::histogram!("mkdb2_flush_duration_seconds").record(flush_start.elapsed().as_secs_f64());
    Ok(())
}

pub(crate) fn clickhouse_insert_error(
    error: clickhouse::error::Error,
    io_timeout: Duration,
) -> (&'static str, Status) {
    if !crate::clickhouse::is_transport_error(&error) {
        return (
            "error",
            Status::internal(format!("ClickHouse insert failed: {error}")),
        );
    }
    if matches!(error, clickhouse::error::Error::TimedOut) {
        (
            "timeout",
            Status::unavailable(format!(
                "ClickHouse insert exceeded an I/O deadline of {}s; outcome unknown",
                io_timeout.as_secs()
            )),
        )
    } else {
        (
            "network",
            Status::unavailable(format!(
                "ClickHouse insert transport failed: {error}; outcome unknown"
            )),
        )
    }
}

#[cfg(test)]
#[path = "ingest_live_tests.rs"]
mod live_tests;

#[cfg(test)]
mod registry_tests {
    use super::*;

    fn row(value: Option<f32>, cdn: Option<&str>, text: Option<&str>) -> MetricRow {
        MetricRow {
            project_id: "p".into(),
            run_id: "r".into(),
            metric_name: "m".into(),
            tag: String::new(),
            step: 0,
            timestamp_ms: 0,
            value,
            cdn_key: cdn.map(Into::into),
            text_data: text.map(Into::into),
        }
    }

    fn active_snapshot(state: &StreamTaskState) -> (usize, HashSet<u64>) {
        let active = state.active.lock().unwrap();
        (active.count, active.row_keys.clone())
    }

    #[test]
    fn point_types_follow_payload() {
        assert_eq!(point_metric_type(&row(Some(1.0), None, None)), "NUMERIC");
        assert_eq!(point_metric_type(&row(None, Some("k"), None)), "CDN");
        assert_eq!(
            point_metric_type(&row(None, None, Some("log"))),
            "TEXT_STREAM"
        );
        // text wins over a value in the same point, matching the old
        // ClickHouse multiIf precedence
        assert_eq!(
            point_metric_type(&row(Some(1.0), None, Some("log"))),
            "TEXT_STREAM"
        );
    }

    #[test]
    fn insert_transport_failures_are_retryable() {
        let network = clickhouse::error::Error::Network(Box::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset",
        )));
        let (outcome, status) = clickhouse_insert_error(network, CH_IO_TIMEOUT);
        assert_eq!(outcome, "network");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.message().contains("outcome unknown"));

        let (outcome, status) =
            clickhouse_insert_error(clickhouse::error::Error::TimedOut, CH_IO_TIMEOUT);
        assert_eq!(outcome, "timeout");
        assert_eq!(status.code(), tonic::Code::Unavailable);

        let (outcome, status) = clickhouse_insert_error(
            clickhouse::error::Error::BadResponse("INSERT rejected".into()),
            CH_IO_TIMEOUT,
        );
        assert_eq!(outcome, "error");
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn precedence_orders_cdn_numeric_text() {
        assert!(type_precedence("CDN") < type_precedence("NUMERIC"));
        assert!(type_precedence("NUMERIC") < type_precedence("TEXT_STREAM"));
        // unknown strings rank lowest, never displacing a known type
        assert_eq!(type_precedence("???"), type_precedence("CDN"));
    }

    #[test]
    fn heartbeat_merge_takes_max_per_slot() {
        let mut hb = RunHeartbeat {
            max_main_ms: Some(5),
            max_system_ms: None,
        };
        hb.merge(RunHeartbeat {
            max_main_ms: Some(3),
            max_system_ms: Some(7),
        });
        assert_eq!((hb.max_main_ms, hb.max_system_ms), (Some(5), Some(7)));
        // a window with no points of a kind never regresses a known timestamp
        hb.merge(RunHeartbeat::default());
        assert_eq!((hb.max_main_ms, hb.max_system_ms), (Some(5), Some(7)));
    }

    fn metric_key(m: &str) -> (String, String, String) {
        ("p".to_string(), "r".to_string(), m.to_string())
    }

    #[test]
    fn pending_receipts_keep_the_maximum_and_filter_requested_runs() {
        let c = BumpCoalescer::empty_for_test();
        for (run, receipt) in [("first", 20), ("second", 30), ("first", 10)] {
            c.mark_dirty(
                std::iter::once((("project", run), RunHeartbeat::default())),
                HashMap::new(),
                receipt,
            );
        }
        assert_eq!(
            c.pending_last_ingested_at_ms("project", &["first".into()]),
            HashMap::from([("first".to_owned(), 20)]),
        );
        assert_eq!(
            c.pending_last_ingested_at_ms("project", &["second".into()]),
            HashMap::from([("second".to_owned(), 30)]),
        );
        assert!(c
            .pending_last_ingested_at_ms("other", &["first".into()])
            .is_empty());
    }

    #[test]
    fn coalescer_merges_flush_windows_per_run() {
        let c = BumpCoalescer::empty_for_test();
        let key = ("p".to_string(), "r".to_string());
        c.mark_dirty(
            std::iter::once((
                (key.0.as_str(), key.1.as_str()),
                RunHeartbeat {
                    max_main_ms: Some(1),
                    max_system_ms: Some(9),
                },
            )),
            HashMap::from([(("p", "r", "m"), "NUMERIC")]),
            10,
        );
        c.mark_dirty(
            std::iter::once((
                (key.0.as_str(), key.1.as_str()),
                RunHeartbeat {
                    max_main_ms: Some(2),
                    max_system_ms: None,
                },
            )),
            HashMap::from([(("p", "r", "m"), "CDN"), (("p", "r", "m2"), "TEXT_STREAM")]),
            20,
        );
        assert_eq!(
            c.pending_last_ingested_at_ms("p", std::slice::from_ref(&key.1)),
            HashMap::from([("r".to_string(), 20)])
        );
        assert!(c
            .pending_last_ingested_at_ms("other", std::slice::from_ref(&key.1))
            .is_empty());
        let state = c.state.lock().unwrap();
        assert_eq!(state.heartbeats.len(), 1);
        let hb = state.heartbeats[&key];
        assert_eq!(
            (
                hb.heartbeat.max_main_ms,
                hb.heartbeat.max_system_ms,
                hb.last_ingested_at_ms
            ),
            (Some(2), Some(9), 20)
        );
        assert_eq!(hb.generation, 2);
        // one pending row per metric; a lower-precedence re-sighting never downgrades
        assert_eq!(state.registrations.len(), 2);
        assert_eq!(state.registrations[&metric_key("m")], "NUMERIC");
    }

    #[test]
    fn known_metrics_suppress_reregistration_but_not_upgrades() {
        let c = BumpCoalescer::empty_for_test();
        c.state
            .lock()
            .unwrap()
            .known
            .insert(metric_key("m"), type_precedence("NUMERIC"));
        c.mark_dirty(
            std::iter::empty(),
            HashMap::from([(("p", "r", "m"), "NUMERIC")]),
            0,
        );
        assert!(c.state.lock().unwrap().registrations.is_empty());
        c.mark_dirty(
            std::iter::empty(),
            HashMap::from([(("p", "r", "m"), "TEXT_STREAM")]),
            0,
        );
        assert_eq!(
            c.state.lock().unwrap().registrations[&metric_key("m")],
            "TEXT_STREAM"
        );
    }

    #[test]
    fn cap_is_a_hard_bound_that_admits_once() {
        let c = BumpCoalescer::empty_for_test();
        // fill the pending map exactly to the cap
        let names: Vec<String> = (0..WRITE_BEHIND_REGISTRATION_CAP)
            .map(|i| format!("m{i}"))
            .collect();
        let full: HashMap<(&str, &str, &str), &'static str> = names
            .iter()
            .map(|m| (("p", "r", m.as_str()), "CDN"))
            .collect();
        c.mark_dirty(std::iter::empty(), full, 0);
        assert_eq!(
            c.state.lock().unwrap().registrations.len(),
            WRITE_BEHIND_REGISTRATION_CAP
        );
        // at cap: a new key is refused, an accepted key still upgrades; in-flight batches stay in the map, so the cap holds across failed writes
        c.mark_dirty(
            std::iter::empty(),
            HashMap::from([
                (("p", "r", "overflow"), "CDN"),
                (("p", "r", "m0"), "TEXT_STREAM"),
            ]),
            0,
        );
        let state = c.state.lock().unwrap();
        assert_eq!(state.registrations.len(), WRITE_BEHIND_REGISTRATION_CAP);
        assert_eq!(state.registrations[&metric_key("m0")], "TEXT_STREAM");
    }

    #[test]
    fn absorb_removes_committed_unless_upgraded_mid_write() {
        let c = BumpCoalescer::empty_for_test();
        c.mark_dirty(
            std::iter::empty(),
            HashMap::from([(("p", "r", "m"), "NUMERIC")]),
            0,
        );
        // the batch was snapshotted; a flush upgrades the entry mid-write
        c.mark_dirty(
            std::iter::empty(),
            HashMap::from([(("p", "r", "m"), "TEXT_STREAM")]),
            0,
        );
        let mut state = c.state.lock().unwrap();
        state.absorb_committed(vec![(metric_key("m"), "NUMERIC")]);
        // the upgrade stays pending for the next tick; known records the commit
        assert_eq!(state.registrations[&metric_key("m")], "TEXT_STREAM");
        assert_eq!(state.known[&metric_key("m")], type_precedence("NUMERIC"));
        // the upgrade's own commit clears it and raises known; a stale lower-typed absorb can never downgrade it
        state.absorb_committed(vec![(metric_key("m"), "TEXT_STREAM")]);
        assert!(state.registrations.is_empty());
        state.absorb_committed(vec![(metric_key("m"), "NUMERIC")]);
        assert_eq!(
            state.known[&metric_key("m")],
            type_precedence("TEXT_STREAM")
        );
    }

    #[test]
    fn absorb_heartbeats_keeps_equal_timestamp_new_generation_mid_write() {
        let c = BumpCoalescer::empty_for_test();
        let key = ("p".to_string(), "r".to_string());
        let heartbeat = RunHeartbeat {
            max_main_ms: Some(5),
            max_system_ms: Some(7),
        };
        c.mark_dirty(
            std::iter::once(((key.0.as_str(), key.1.as_str()), heartbeat)),
            HashMap::new(),
            11,
        );
        let first = c.state.lock().unwrap().heartbeats[&key];
        assert_eq!(first.generation, 1);

        // A second committed cut can carry exactly the same timestamp maxima
        // while the first generation's Postgres write is in flight.
        c.mark_dirty(
            std::iter::once(((key.0.as_str(), key.1.as_str()), heartbeat)),
            HashMap::new(),
            13,
        );
        let mut state = c.state.lock().unwrap();
        state.absorb_heartbeats(&[(key.clone(), first)]);
        let second = state.heartbeats[&key];
        assert_eq!(second.generation, 2);
        assert_eq!(
            (second.heartbeat.max_main_ms, second.heartbeat.max_system_ms),
            (Some(5), Some(7))
        );

        // Only a write that covered generation two may clear it.
        state.absorb_heartbeats(&[(key, second)]);
        assert!(state.heartbeats.is_empty());
    }

    #[test]
    fn flush_concurrency_and_admission_config_are_bounded() {
        assert_eq!(ordered_flush_limit(4), 8);
    }

    #[test]
    fn row_string_bytes_counts_all_owned_strings() {
        let mut value = row(None, Some("cdn"), Some("text"));
        value.project_id.reserve(11);
        value.run_id.reserve(13);
        value.metric_name.reserve(17);
        value.tag.reserve(19);
        value.cdn_key.as_mut().unwrap().reserve(23);
        value.text_data.as_mut().unwrap().reserve(29);

        let expected = value.project_id.capacity()
            + value.run_id.capacity()
            + value.metric_name.capacity()
            + value.tag.capacity()
            + value.cdn_key.as_ref().unwrap().capacity()
            + value.text_data.as_ref().unwrap().capacity();
        assert_eq!(metric_row_string_bytes(&value), expected);
    }

    fn empty_charge() -> ByteCharge {
        ByteCharge::new(ByteBudget::new(MEBIBYTE))
    }

    #[test]
    fn flush_cut_triggers_on_either_limit_after_one_row_overshoot() {
        assert!(!batch_flush_due(
            BATCH_FLUSH_SIZE - 1,
            BATCH_FLUSH_BYTES - 1
        ));
        assert!(batch_flush_due(BATCH_FLUSH_SIZE, 0));
        assert!(batch_flush_due(1, BATCH_FLUSH_BYTES));

        let budget = ByteBudget::new(64 * MEBIBYTE);
        let activity = crate::activity::ActivityTracker::new_local();
        let mut cut = AttachedCut::default();
        assert!(!cut.push(&budget, &activity, row(Some(1.0), None, None)));
        cut.owned_bytes = BATCH_FLUSH_BYTES - 1;
        let value = row(Some(2.0), None, None);
        let row_capacity_before = cut.rows.capacity();
        let last_row_string_bytes = metric_row_string_bytes(&value);
        assert!(cut.push(&budget, &activity, value));
        assert_eq!(cut.rows.len(), 2);
        assert_eq!(
            cut.owned_bytes,
            BATCH_FLUSH_BYTES - 1
                + last_row_string_bytes
                + row_vec_heap_bytes(cut.rows.capacity())
                    .saturating_sub(row_vec_heap_bytes(row_capacity_before))
        );
    }

    #[test]
    fn attached_cut_bytes_charge_on_push_and_release_on_drop() {
        // Cap of one byte: the first row's charge crosses it (always admitted) and the gate stays closed until the cut releases.
        let budget = ByteBudget::new(1);
        let activity = crate::activity::ActivityTracker::new_local();
        let mut first = AttachedCut::default();

        assert!(budget.has_room());
        first.push(&budget, &activity, row(Some(1.0), None, None));
        assert!(!budget.has_room());
        assert_eq!(budget.used_bytes(), first.owned_bytes);

        first.push(&budget, &activity, row(Some(2.0), None, None));
        assert_eq!(budget.used_bytes(), first.owned_bytes);

        drop(first);
        assert!(budget.has_room());
        assert_eq!(budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn room_wakes_when_a_release_frees_the_budget() {
        let budget = ByteBudget::new(1);
        budget.charge(8);
        let mut waiter = Box::pin(budget.room());
        assert!(futures::poll!(waiter.as_mut()).is_pending());
        budget.release(8);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("room() must resolve after release");
    }

    #[tokio::test]
    async fn unary_tick_still_flushes_while_budget_is_exhausted() {
        let budget = ByteBudget::new(1);
        budget.charge(8);
        let mut stream = futures::stream::pending::<Result<proto::MetricsBatch, Status>>();
        let mut flush_tick = tokio::time::interval(Duration::from_millis(1));
        flush_tick.reset();
        assert!(matches!(
            tokio::time::timeout(
                Duration::from_secs(1),
                next_unary_event(&mut stream, &mut flush_tick, true, &budget),
            )
            .await
            .unwrap(),
            UnaryEvent::Flush
        ));
    }

    #[tokio::test]
    async fn unary_read_resumes_after_a_release_frees_the_gate() {
        let budget = ByteBudget::new(1);
        budget.charge(8);
        let releaser = {
            let budget = budget.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                budget.release(8);
            })
        };
        let mut stream = futures::stream::iter([Ok::<_, Status>(proto::MetricsBatch::default())]);
        let mut flush_tick = tokio::time::interval(Duration::from_secs(3600));
        flush_tick.reset();
        assert!(matches!(
            tokio::time::timeout(
                Duration::from_secs(1),
                next_unary_event(&mut stream, &mut flush_tick, false, &budget),
            )
            .await
            .unwrap(),
            UnaryEvent::Stream(Some(Ok(_)))
        ));
        releaser.await.unwrap();
    }

    #[test]
    fn byte_charge_survives_cut_take_until_flush_guard_drop() {
        let budget = ByteBudget::new(1);
        let activity = crate::activity::ActivityTracker::new_local();
        let mut cut = AttachedCut::default();
        cut.push(&budget, &activity, row(Some(1.0), None, None));

        let (rows, charge, work) = cut.take();
        assert_eq!(rows.len(), 1);
        assert!(cut.is_empty());
        assert!(!budget.has_room());
        assert_eq!(activity.snapshot().in_flight_work, 1);

        let guard = FlushTaskGuard::new(None, None, charge);
        assert!(!budget.has_room());
        drop(guard);
        assert!(budget.has_room());
        assert_eq!(budget.used_bytes(), 0);
        drop(work);
        assert_eq!(activity.snapshot().in_flight_work, 0);
    }

    #[test]
    fn cut_capacity_is_restored_only_when_the_next_row_arrives() {
        let budget = ByteBudget::new(64 * MEBIBYTE);
        let activity = crate::activity::ActivityTracker::new_local();
        let mut cut = AttachedCut::default();
        for step in 0..17 {
            let mut value = row(Some(1.0), None, None);
            value.step = step;
            cut.push(&budget, &activity, value);
        }

        let (rows, charge, work) = cut.take();
        assert_eq!(rows.len(), 17);
        assert_eq!(cut.rows.capacity(), 0);

        drop((rows, charge, work));
        let value = row(Some(1.0), None, None);
        let string_bytes = metric_row_string_bytes(&value);
        cut.push(&budget, &activity, value);

        assert!(cut.rows.capacity() >= 17);
        assert_eq!(
            cut.owned_bytes,
            row_vec_heap_bytes(cut.rows.capacity()) + string_bytes
        );
    }

    #[tokio::test]
    async fn unary_idle_cut_timer_fires_without_another_batch() {
        let mut stream = futures::stream::pending::<Result<proto::MetricsBatch, Status>>();
        let mut flush_tick = tokio::time::interval(Duration::from_millis(1));
        flush_tick.reset();

        assert!(matches!(
            tokio::time::timeout(
                Duration::from_secs(1),
                next_unary_event(
                    &mut stream,
                    &mut flush_tick,
                    true,
                    &ByteBudget::new(MEBIBYTE)
                ),
            )
            .await
            .unwrap(),
            UnaryEvent::Flush
        ));
    }

    #[test]
    fn batch_metadata_merges_runs_and_metric_types_after_insert() {
        let mut main = row(Some(1.0), None, None);
        main.project_id = "project".into();
        main.run_id = "run".into();
        main.timestamp_ms = 3;

        let mut system = main.clone();
        system.metric_name = "system/cpu".into();
        system.timestamp_ms = 7;

        let mut other = row(None, None, Some("log"));
        other.project_id = "project".into();
        other.run_id = "other-run".into();
        other.timestamp_ms = 5;

        let rows = [main, system, other];
        let metadata = batch_metadata(&rows);
        let heartbeat = metadata.heartbeats[&("project", "run")];
        assert_eq!(
            (heartbeat.max_main_ms, heartbeat.max_system_ms),
            (Some(3), Some(7))
        );
        assert_eq!(
            metadata.heartbeats[&("project", "other-run")].max_main_ms,
            Some(5)
        );
        assert_eq!(metadata.candidates[&("project", "run", "m")], "NUMERIC");
        assert_eq!(
            metadata.candidates[&("project", "other-run", "m")],
            "TEXT_STREAM"
        );
    }

    #[test]
    fn text_conversion_reuses_valid_utf8_and_repairs_invalid_utf8() {
        fn point(bytes: Vec<u8>) -> proto::MetricPoint {
            proto::MetricPoint {
                metric_name: "m".into(),
                step: 0,
                payload: Some(proto::metric_point::Payload::TextData(bytes)),
                tag: String::new(),
                timestamp_ms: 0,
            }
        }

        let bytes = b"valid text".to_vec();
        let original_ptr = bytes.as_ptr();
        let mut skipped = 0;
        let text = point_to_row(point(bytes), "p", "r", &mut skipped)
            .unwrap()
            .text_data
            .unwrap();
        assert_eq!(text, "valid text");
        assert_eq!(text.as_ptr(), original_ptr);

        let repaired = point_to_row(point(vec![b'a', 0xff, b'b']), "p", "r", &mut skipped)
            .unwrap()
            .text_data
            .unwrap();
        assert_eq!(repaired, "a\u{fffd}b");

        let invalid = vec![0xff; 1024];
        let repaired = point_to_row(point(invalid), "p", "r", &mut skipped)
            .unwrap()
            .text_data
            .unwrap();
        assert_eq!(repaired.len(), 3 * 1024);
    }

    #[test]
    fn fingerprint_is_only_the_clickhouse_sort_key() {
        let original = row(Some(1.0), None, None);
        let mut same_key = original.clone();
        same_key.value = Some(2.0);
        same_key.timestamp_ms = 99;
        assert_eq!(
            row_key_fingerprint(&original),
            row_key_fingerprint(&same_key)
        );

        same_key.step += 1;
        assert_ne!(
            row_key_fingerprint(&original),
            row_key_fingerprint(&same_key)
        );
    }

    #[test]
    fn flush_task_guard_releases_stream_ownership_together() {
        let state = Arc::new(StreamTaskState::default());
        // Exact re-logs may repeat a fingerprint within one cut. The task-local
        // vector need not deduplicate them for admission or release to be exact.
        let keys = vec![11, 11, 22];
        {
            let mut guard =
                FlushTaskGuard::new(None, Some((state.clone(), keys.clone())), empty_charge());
            assert_eq!(active_snapshot(&state), (1, HashSet::from([11, 22])));
            assert_eq!(state.admission(&[11], 2), (false, true));
            assert_eq!(state.admission(&[33], 2), (true, false));
            assert_eq!(state.admission(&[33], 1), (false, false));
            guard.finish(&Ok(()));
        }
        assert_eq!(active_snapshot(&state), (0, HashSet::new()));
        assert_eq!(state.admission(&[11], 2), (true, false));
        assert_eq!(state.active.lock().unwrap().row_keys.capacity(), 0);
        assert!(state.failure().is_none());
    }

    #[test]
    fn flush_task_failure_is_visible_before_key_release() {
        let state = Arc::new(StreamTaskState::default());
        let keys = vec![11];
        let mut guard =
            FlushTaskGuard::new(None, Some((state.clone(), keys.clone())), empty_charge());

        let result = Err(Status::unavailable("insert outcome unknown"));
        guard.finish(&result);
        assert_eq!(state.failure().unwrap().message(), "insert outcome unknown");
        assert_eq!(active_snapshot(&state), (1, HashSet::from([11])));

        drop(guard);
        assert_eq!(active_snapshot(&state), (0, HashSet::new()));
    }

    #[test]
    fn unfinished_flush_task_latches_failure_on_drop() {
        let state = Arc::new(StreamTaskState::default());
        let guard = FlushTaskGuard::new(None, Some((state.clone(), vec![11])), empty_charge());
        drop(guard);
        assert_eq!(state.failure().unwrap().code(), tonic::Code::Internal);
    }

    #[test]
    fn bidi_timer_rearms_for_first_pending_position_and_each_dispatch() {
        assert!(should_rearm_bidi_flush_timer(false, true, 10, 10));
        assert!(!should_rearm_bidi_flush_timer(true, true, 10, 10));
        assert!(!should_rearm_bidi_flush_timer(false, false, 10, 10));
        assert!(should_rearm_bidi_flush_timer(true, true, 10, 20));
        assert!(should_rearm_bidi_flush_timer(true, false, 10, 20));
    }

    #[tokio::test]
    async fn later_ordered_failure_wakes_driver_behind_pending_head() {
        let state = Arc::new(StreamTaskState::default());
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let mut ordered = FuturesOrdered::<BoxFlush>::new();
        ordered.push_back(Box::pin(async move {
            let _ = release_rx.await;
            Ok(1)
        }));
        let failure_state = state.clone();
        ordered.push_back(Box::pin(async move {
            let status = Status::unavailable("later insert failed");
            failure_state.record_failure(status.clone());
            Err(status)
        }));

        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                result = ordered.next() => panic!("ordered head unexpectedly resolved: {result:?}"),
                _ = state.completed.notified() => {}
            }
        })
        .await
        .unwrap();
        assert_eq!(state.failure().unwrap().message(), "later insert failed");
        assert_eq!(ordered.len(), 2);

        release_tx.send(()).unwrap();
        assert_eq!(ordered.next().await.unwrap().unwrap(), 1);
        assert_eq!(
            ordered.next().await.unwrap().unwrap_err().message(),
            "later insert failed"
        );
    }

    #[tokio::test]
    async fn completed_ack_drains_while_waiting_for_global_permit() {
        let service = IngestService {
            ch: Arc::new(ChClient::new("http://unused").unwrap()),
            pg: Arc::new(PgStore::test_store()),
            gates: LifecycleGates::new(),
            bumps: BumpCoalescer::empty_for_test(),
            flush_concurrency: 1,
            flush_slots: Arc::new(Semaphore::new(0)),
            byte_budget: ByteBudget::new(MEBIBYTE),
            cap_unary_flushes: true,
        };
        let mut state = BidiState::new();
        state.cut.push(
            &service.byte_budget,
            &service.bumps.activity,
            row(Some(1.0), None, None),
        );
        state.consumed = 2;
        state.last_pushed = 1;
        state.inflight.push_back(Box::pin(async { Ok(1) }));

        let error = {
            let (ack_tx, mut ack_rx) = tokio::sync::mpsc::channel(1);
            let dispatch = state.dispatch_cut(&service, &ack_tx);
            tokio::pin!(dispatch);

            let ack = tokio::time::timeout(Duration::from_secs(1), async {
                tokio::select! {
                    result = &mut dispatch => panic!("permit wait ended before ACK: {result:?}"),
                    ack = ack_rx.recv() => ack,
                }
            })
            .await
            .unwrap()
            .unwrap()
            .unwrap();
            assert_eq!(ack.points_acked, 1);

            // Cancellation while admission is still blocked leaves the data
            // cut attached and retryable; no ClickHouse task is started.
            drop(ack_rx);
            tokio::time::timeout(Duration::from_secs(1), &mut dispatch)
                .await
                .unwrap()
                .unwrap_err()
        };
        assert_eq!(error.code(), tonic::Code::Cancelled);
        assert!(!state.cut.is_empty());
        assert!(state.inflight.is_empty());
    }

    #[tokio::test]
    async fn dropping_ordered_wrapper_detaches_task_and_retains_permit() {
        let slots = Arc::new(Semaphore::new(1));
        let permit = slots.clone().acquire_owned().await.unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let guard = FlushTaskGuard::new(Some(permit), None, empty_charge());
        let task = tokio::spawn(async move {
            let _guard = guard;
            let _ = started_tx.send(());
            let _ = release_rx.await;
            Ok(())
        });
        started_rx.await.unwrap();

        let wrapper: BoxFlush = Box::pin(async move {
            await_owned_flush(
                task,
                tokio::time::Instant::now() + Duration::from_secs(60),
                None,
            )
            .await
            .map(|()| 1)
        });
        drop(wrapper);
        assert_eq!(slots.available_permits(), 0);

        release_tx.send(()).unwrap();
        let restored = tokio::time::timeout(Duration::from_secs(1), slots.acquire_owned())
            .await
            .unwrap()
            .unwrap();
        drop(restored);
    }

    #[tokio::test]
    async fn response_timeout_detaches_task_and_retains_permit() {
        let slots = Arc::new(Semaphore::new(1));
        let permit = slots.clone().acquire_owned().await.unwrap();
        let state = Arc::new(StreamTaskState::default());
        let keys = vec![11];
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let guard = FlushTaskGuard::new(
            Some(permit),
            Some((state.clone(), keys.clone())),
            empty_charge(),
        );
        let task = tokio::spawn(async move {
            let mut guard = guard;
            let _ = started_tx.send(());
            let _ = release_rx.await;
            let result = Ok(());
            guard.finish(&result);
            result
        });
        started_rx.await.unwrap();

        let error = await_owned_flush(task, tokio::time::Instant::now(), Some(&state))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert_eq!(state.failure().unwrap().code(), tonic::Code::Unavailable);
        assert_eq!(active_snapshot(&state), (1, HashSet::from([11])));
        assert_eq!(slots.available_permits(), 0);

        release_tx.send(()).unwrap();
        let restored = tokio::time::timeout(Duration::from_secs(1), slots.acquire_owned())
            .await
            .unwrap()
            .unwrap();
        drop(restored);
        tokio::time::timeout(Duration::from_secs(1), async {
            while active_snapshot(&state).0 != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(active_snapshot(&state), (0, HashSet::new()));
    }

    #[test]
    fn successful_async_insert_fits_the_visibility_horizon() {
        assert!(
            CH_IO_TIMEOUT.as_millis() as i64 + crate::series_cache::WATERMARK_OVERLAP_MS
                < crate::series_cache::VISIBILITY_MARGIN_MS
        );
    }

    #[test]
    fn every_flush_fits_the_pinned_async_insert_limit() {
        assert_eq!(ROW_BINARY_MAX_OVERHEAD_PER_ROW, 83);
        assert_eq!(MAX_FLUSH_ROW_BINARY_BYTES, 21_801_520);
        const {
            assert!(
                MAX_FLUSH_ROW_BINARY_BYTES < crate::clickhouse::ASYNC_INSERT_MAX_DATA_SIZE_BYTES
            );
        }
    }

    #[test]
    fn registration_batches_are_chunked() {
        let c = BumpCoalescer::empty_for_test();
        c.mark_dirty(
            std::iter::empty(),
            HashMap::from([
                (("p", "r", "a"), "CDN"),
                (("p", "r", "b"), "CDN"),
                (("p", "r", "c"), "CDN"),
            ]),
            0,
        );
        let state = c.state.lock().unwrap();
        assert_eq!(state.registration_batch(2).len(), 2);
        assert_eq!(state.registration_batch(usize::MAX).len(), 3);
    }

    #[test]
    fn storable_timestamp_rejects_only_pg_poison() {
        // A current millisecond timestamp and pre-1970 timestamps are valid.
        assert!(storable_timestamp(1_752_000_000_000));
        assert!(storable_timestamp(0));
        assert!(storable_timestamp(-1));
        // A microsecond-unit mistake (~year 57k) is weird but Postgres-storable.
        assert!(storable_timestamp(1_752_000_000_000_000));
        // a nanosecond-unit mistake is past the Postgres range — the poison case
        assert!(!storable_timestamp(1_752_000_000_000_000_000));
        assert!(!storable_timestamp(i64::MAX));
        assert!(!storable_timestamp(i64::MIN));
    }

    #[test]
    fn storable_ident_rejects_only_nul_and_oversize() {
        assert!(storable_ident("loss/train", MAX_METRIC_NAME_BYTES));
        assert!(storable_ident("", MAX_ID_BYTES)); // empty stays allowed
        assert!(!storable_ident("bad\0name", MAX_METRIC_NAME_BYTES));
        assert!(!storable_ident(
            &"x".repeat(MAX_METRIC_NAME_BYTES + 1),
            MAX_METRIC_NAME_BYTES
        ));
        assert!(storable_ident(
            &"x".repeat(MAX_METRIC_NAME_BYTES),
            MAX_METRIC_NAME_BYTES
        ));
    }

    #[test]
    fn rich_head_key_enforces_the_combined_postgres_index_budget() {
        let mut request = proto::PublishRichMutationRequest {
            project_id: "p".repeat(MAX_ID_BYTES),
            run_id: "r".repeat(MAX_ID_BYTES),
            metric_name: "m".repeat(MAX_METRIC_NAME_BYTES),
            tag: String::new(),
            step: 0,
            timestamp_ms: 0,
            cdn_key: "resource.json".to_string(),
            mutation_version: Some((1_u64 << 32) | 1),
        };
        assert!(storable_rich_head_key(&request));
        request.tag.push('t');
        assert!(!storable_rich_head_key(&request));
    }

    #[test]
    fn trash_is_the_only_reserved_project_id() {
        assert!(is_reserved_project_id("trash"));
        assert!(!is_reserved_project_id("Trash"));
        assert!(!is_reserved_project_id("trash-run"));

        let error = validate_batch_ids(&proto::MetricsBatch {
            project_id: "trash".to_string(),
            run_id: "run".to_string(),
            points: Vec::new(),
        })
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("reserved"));

        assert!(validate_batch_ids(&proto::MetricsBatch {
            project_id: "project".to_string(),
            run_id: "trash".to_string(),
            points: Vec::new(),
        })
        .is_ok());
    }
}
