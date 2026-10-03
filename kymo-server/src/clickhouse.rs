use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use clickhouse::Client;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;
use rustls::pki_types::{pem::PemObject, CertificateDer};
use rustls::{ClientConfig, RootCertStore};
use serde::{Deserialize, Serialize};

use crate::series_cache::{
    ElectedMiss, LineageOrigin, RefreshError, RefreshOutcome, SeriesKey, SeriesRefreshLocks,
    SeriesSnapshot,
};
use crate::text_index_cache::{
    IndexedTextChunk, Lookup as TextIndexLookup, TextIndexCache, TextIndexKey, TextIndexRow,
    TextRefreshLocks, TextStreamIndex,
};

/// Ingest's row, owned-memory, and accepted-message limits keep a cut's
/// conservative RowBinary upper bound below 21 MiB. Keep ClickHouse's
/// per-query async ceiling above that bound so a supported request cannot
/// silently fall back to synchronous insertion.
pub(crate) const ASYNC_INSERT_MAX_DATA_SIZE_BYTES: usize = 32 * 1024 * 1024;
const MAX_TEXT_WINDOW_BYTES: u64 = 8 * 1024 * 1024;
const DELETE_HEADROOM_MULTIPLIER: u64 = 2;
const DELETE_HEADROOM_RESERVE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
const METRICS_TABLE: &str = "mkdb2.metrics";
const RICH_METRICS_TABLE: &str = "mkdb2.rich_metrics";
const REGISTRY_OUTBOX_TABLE: &str = "mkdb2.metric_registry_outbox";
const REGISTRY_OUTBOX_VIEW: &str = "mkdb2.metric_registry_outbox_mv";
const RICH_REGISTRY_OUTBOX_VIEW: &str = "mkdb2.rich_metric_registry_outbox_mv";
const CDN_ACKS_TABLE: &str = "mkdb2.cdn_acks";
const CDN_MANIFEST_CHILDREN_TABLE: &str = "mkdb2.cdn_manifest_children";
const CDN_GC_SCRATCH_TABLE: &str = "mkdb2.cdn_gc_scratch";
/// A dedup upload waits on this insert; its failure fails the upload into the client spool.
const CDN_ACK_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize, clickhouse::Row)]
struct SchemaColumn {
    name: String,
    column_type: String,
    default_kind: String,
    default_expression: String,
}

#[derive(Debug, Deserialize, clickhouse::Row)]
struct SchemaTable {
    engine: String,
    engine_full: String,
    sorting_key: String,
    partition_key: String,
}

fn normalized_expression(expression: &str) -> String {
    expression
        .chars()
        .filter(|character| !character.is_whitespace() && *character != '`')
        .collect()
}

fn normalized_key(expression: &str) -> String {
    let normalized = normalized_expression(expression);
    normalized
        .strip_prefix('(')
        .and_then(|inner| inner.strip_suffix(')'))
        .unwrap_or(&normalized)
        .to_string()
}

pub(crate) fn is_transport_error(error: &clickhouse::error::Error) -> bool {
    matches!(
        error,
        clickhouse::error::Error::Network(_) | clickhouse::error::Error::TimedOut
    )
}

fn is_retryable_schema_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<clickhouse::error::Error>()
            .is_some_and(is_transport_error)
    })
}

fn validate_metrics_schema_shape(columns: &[SchemaColumn], table: &SchemaTable) -> Result<()> {
    for (name, expected_type) in [
        ("project_id", "LowCardinality(String)"),
        ("run_id", "LowCardinality(String)"),
        ("metric_name", "LowCardinality(String)"),
        ("tag", "LowCardinality(String)"),
        ("step", "Int64"),
        ("timestamp_ms", "Int64"),
        ("value", "Nullable(Float32)"),
        ("cdn_key", "Nullable(String)"),
        ("text_data", "Nullable(String)"),
        ("inserted_at", "DateTime64(3)"),
    ] {
        let actual = columns
            .iter()
            .find(|column| column.name == name)
            .map(|column| column.column_type.as_str());
        anyhow::ensure!(
            actual == Some(expected_type),
            "mkdb2.metrics column {name:?} must have type {expected_type}, found {actual:?}"
        );
    }
    for (name, expected_expression) in [("tag", "''"), ("timestamp_ms", "0")] {
        let column = columns
            .iter()
            .find(|column| column.name == name)
            .expect("required column checked above");
        anyhow::ensure!(
            column.default_kind == "DEFAULT"
                && normalized_key(&column.default_expression) == expected_expression,
            "mkdb2.metrics column {name:?} must default to {expected_expression}, found {:?} {:?}",
            column.default_kind,
            column.default_expression
        );
    }
    let inserted_at = columns
        .iter()
        .find(|column| column.name == "inserted_at")
        .expect("required column checked above");
    anyhow::ensure!(
        inserted_at.default_kind == "DEFAULT"
            && normalized_key(&inserted_at.default_expression) == "now64(3)",
        "mkdb2.metrics.inserted_at must default to now64(3), found {:?} {:?}",
        inserted_at.default_kind,
        inserted_at.default_expression
    );
    anyhow::ensure!(
        table.engine == "ReplacingMergeTree"
            && normalized_expression(&table.engine_full)
                .starts_with("ReplacingMergeTree(inserted_at)"),
        "mkdb2.metrics must use ReplacingMergeTree(inserted_at), found {:?}",
        table.engine_full
    );
    anyhow::ensure!(
        normalized_key(&table.sorting_key) == "project_id,run_id,metric_name,tag,step",
        "mkdb2.metrics has unexpected sorting key {:?}",
        table.sorting_key
    );
    anyhow::ensure!(
        normalized_key(&table.partition_key) == "project_id",
        "mkdb2.metrics has unexpected partition key {:?}",
        table.partition_key
    );
    Ok(())
}

fn validate_rich_metrics_schema_shape(columns: &[SchemaColumn], table: &SchemaTable) -> Result<()> {
    for (name, expected_type) in [
        ("project_id", "LowCardinality(String)"),
        ("run_id", "LowCardinality(String)"),
        ("metric_name", "LowCardinality(String)"),
        ("tag", "LowCardinality(String)"),
        ("step", "Int64"),
        ("timestamp_ms", "Int64"),
        ("cdn_key", "String"),
        ("mutation_version", "UInt64"),
        ("inserted_at", "DateTime64(3)"),
    ] {
        let actual = columns
            .iter()
            .find(|column| column.name == name)
            .map(|column| column.column_type.as_str());
        anyhow::ensure!(
            actual == Some(expected_type),
            "mkdb2.rich_metrics column {name:?} must have type {expected_type}, found {actual:?}"
        );
    }
    for (name, expected_expression) in [("tag", "''"), ("timestamp_ms", "0")] {
        let column = columns
            .iter()
            .find(|column| column.name == name)
            .expect("required column checked above");
        anyhow::ensure!(
            column.default_kind == "DEFAULT"
                && normalized_key(&column.default_expression) == expected_expression,
            "mkdb2.rich_metrics column {name:?} must default to {expected_expression}, found {:?} {:?}",
            column.default_kind,
            column.default_expression
        );
    }
    let inserted_at = columns
        .iter()
        .find(|column| column.name == "inserted_at")
        .expect("required column checked above");
    anyhow::ensure!(
        inserted_at.default_kind == "DEFAULT"
            && normalized_key(&inserted_at.default_expression) == "now64(3)",
        "mkdb2.rich_metrics.inserted_at must default to now64(3), found {:?} {:?}",
        inserted_at.default_kind,
        inserted_at.default_expression
    );
    anyhow::ensure!(
        table.engine == "ReplacingMergeTree"
            && normalized_expression(&table.engine_full)
                .starts_with("ReplacingMergeTree(mutation_version)"),
        "mkdb2.rich_metrics must use ReplacingMergeTree(mutation_version), found {:?}",
        table.engine_full
    );
    anyhow::ensure!(
        normalized_key(&table.sorting_key) == "project_id,run_id,metric_name,tag,step",
        "mkdb2.rich_metrics has unexpected sorting key {:?}",
        table.sorting_key
    );
    anyhow::ensure!(
        normalized_key(&table.partition_key) == "project_id",
        "mkdb2.rich_metrics has unexpected partition key {:?}",
        table.partition_key
    );
    Ok(())
}

#[derive(Debug)]
pub struct TextWindowLimitError;

impl std::fmt::Display for TextWindowLimitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "text window exceeds the {} MiB payload limit",
            MAX_TEXT_WINDOW_BYTES / (1024 * 1024)
        )
    }
}

impl std::error::Error for TextWindowLimitError {}

fn text_window_limit_error() -> anyhow::Error {
    TextWindowLimitError.into()
}

fn add_text_window_bytes(total: u64, additional: u64) -> Result<u64> {
    let total = total
        .checked_add(additional)
        .ok_or_else(text_window_limit_error)?;
    if total > MAX_TEXT_WINDOW_BYTES {
        return Err(text_window_limit_error());
    }
    Ok(total)
}

fn indexed_chunk_working_bytes(chunk: &IndexedTextChunk) -> u64 {
    chunk
        .normalized_bytes
        .saturating_add(chunk.metric_name.len() as u64)
        .saturating_add(chunk.tag.len() as u64)
        .saturating_add(std::mem::size_of::<TextChunkDataRow>() as u64)
}

fn text_chunk_row_working_bytes(chunk: &TextChunkDataRow) -> u64 {
    (chunk.text.len() as u64)
        .saturating_add(chunk.metric_name.len() as u64)
        .saturating_add(chunk.tag.len() as u64)
        .saturating_add(std::mem::size_of::<TextChunkDataRow>() as u64)
}

fn text_line_working_bytes(text: &str, metric_name: &str) -> u64 {
    (text.len() as u64)
        .saturating_add(metric_name.len() as u64)
        .saturating_add(std::mem::size_of::<TextWindowLine>() as u64)
}

#[derive(Debug, Clone, Serialize, clickhouse::Row)]
pub struct MetricRow {
    pub project_id: String,
    pub run_id: String,
    pub metric_name: String,
    pub tag: String,
    pub step: i64,
    pub timestamp_ms: i64,
    pub value: Option<f32>,
    pub cdn_key: Option<String>,
    pub text_data: Option<String>,
}

#[derive(Debug, Clone, Serialize, clickhouse::Row)]
pub struct RichMetricRow {
    pub project_id: String,
    pub run_id: String,
    pub metric_name: String,
    pub tag: String,
    pub step: i64,
    pub timestamp_ms: i64,
    pub cdn_key: String,
    pub mutation_version: u64,
}

#[derive(Debug, Serialize, clickhouse::Row)]
struct CdnAckRow {
    key: String,
    acked_at: u32,
}

/// A bucket listing row (`kind` is always `inventory`).
#[derive(Debug, Serialize, clickhouse::Row)]
pub struct CdnInventoryRow {
    pub kind: &'static str,
    pub key: String,
    pub size: u64,
    pub created: u32,
}

#[derive(Debug, Serialize, clickhouse::Row)]
pub struct CdnManifestRow {
    pub parent: String,
    pub links_version: u32,
    pub children: Vec<String>,
}

#[derive(Debug, Deserialize, clickhouse::Row)]
pub struct CdnGcKeySize {
    pub key: String,
    pub size: u64,
}

/// Field order must match `ChClient::cdn_gc_report` (the RowBinary decoder is positional).
#[derive(Debug, Default, Deserialize, clickhouse::Row)]
pub struct CdnGcReport {
    pub referenced_objects: u64,
    pub referenced_bytes: u64,
    pub candidate_objects: u64,
    pub candidate_bytes: u64,
    /// Distinct referenced keys, stored or not.
    pub references: u64,
}

impl CdnGcReport {
    /// Referenced keys missing from the bucket.
    pub fn dangling_references(&self) -> u64 {
        self.references.saturating_sub(self.referenced_objects)
    }
}

#[derive(Debug, Clone, Deserialize, clickhouse::Row)]
pub struct CdnKeyBatchRow {
    pub project_id: String,
    pub run_id: String,
    pub metric_name: String,
    pub step: i64,
    pub cdn_key: String,
}

/// Field order must match `ChClient::metric_registry_outbox_at`; the 0.13
/// ClickHouse RowBinary decoder is positional.
#[derive(Debug, Clone, Deserialize, clickhouse::Row)]
#[cfg_attr(test, derive(Serialize))]
pub struct RegistryRow {
    pub project_id: String,
    pub run_id: String,
    pub metric_name: String,
    pub metric_type: u8,
}

/// A raw point plus its `inserted_at` (as unix ms) — the series cache's
/// incremental-fetch watermark. Field order matches the SELECT column
/// order (RowBinary is positional).
#[derive(Debug, Clone, Deserialize, clickhouse::Row)]
pub struct VersionedRawPoint {
    pub tag: String,
    pub step: i64,
    pub timestamp_ms: i64,
    pub value: f32,
    /// Incremental reads retain nonnumeric replacements as tombstones. Full numeric reads always set this to 1.
    pub is_value: u8,
    pub inserted_ms: i64,
}

#[derive(Debug, Clone)]
struct TextChunkWindowRow {
    pub step: i64,
    pub metric_name: String,
    pub text: String,
    pub lines_before: u64,
    /// One or more indexed chunks between the previous surviving row and this one had no payload row — they vanished between the intentionally non-atomic index and payload reads. Positional, so it also catches zero-separator chunks that line arithmetic cannot see.
    pub gap_before: bool,
}

#[derive(Debug, Clone, Deserialize, clickhouse::Row)]
struct TextChunkDataRow {
    step: i64,
    metric_name: String,
    tag: String,
    text: String,
}

#[derive(Debug, Clone, Deserialize, clickhouse::Row)]
struct TextSearchChunkRow {
    step: i64,
    metric_name: String,
    text: String,
}

#[derive(Debug, Clone)]
pub struct TextWindowLine {
    pub step: i64,
    pub metric_name: String,
    pub line_index: u64,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct TextWindow {
    pub first_step: i64,
    pub total_lines: u64,
    pub lines: Vec<TextWindowLine>,
}

#[derive(Debug, Clone, Deserialize, clickhouse::Row)]
#[cfg_attr(test, derive(Serialize))]
pub struct SingleString {
    pub val: String,
}

#[derive(Debug, Clone, Deserialize, clickhouse::Row)]
#[cfg_attr(test, derive(Serialize))]
struct SingleCount {
    val: u64,
}

#[derive(Debug, Clone, Deserialize, clickhouse::Row)]
struct SingleI64 {
    val: i64,
}

#[derive(Debug, Clone, Deserialize, clickhouse::Row)]
struct DeleteHeadroom {
    largest_part_bytes: u64,
    unreserved_bytes: u64,
}

/// Field order must match `metric_registry_outbox_storage_at`; the 0.13
/// ClickHouse RowBinary decoder is positional.
#[derive(Debug, Clone, Deserialize, clickhouse::Row)]
pub struct RegistryOutboxStorage {
    pub rows: u64,
    pub bytes: u64,
    pub parts: u64,
}

/// A multi-run read's row's position in `run_ids` (distinct run ids), reusing the previous row's `position` while rows stay in that run.
/// Rows are ordered by run, so the search runs once per run; ClickHouse's `indexOf` searched the list for every row, several times the read's cost for wide batches.
fn run_position(run_ids: &[String], run_id: &str, position: &mut usize) -> Result<usize> {
    if run_ids.get(*position).is_none_or(|known| known != run_id) {
        *position = run_ids
            .iter()
            .position(|known| known == run_id)
            .with_context(|| {
                format!("multi-run read returned run {run_id:?} it did not ask for")
            })?;
    }
    Ok(*position)
}

/// Held for the detached task's whole lifetime, so a purge of the run waits for its in-flight scans (docs/admission-control.md Stage R).
pub(crate) struct RefreshDetach {
    pub(crate) _permit: tokio::sync::OwnedSemaphorePermit,
    pub(crate) _run_guard: std::sync::Arc<tokio::sync::OwnedRwLockReadGuard<()>>,
}

#[derive(Clone)]
pub struct ChClient {
    client: Client,
    // Arc so clones of the client share one cache.
    series_cache: std::sync::Arc<crate::series_cache::SeriesCache>,
    series_refresh_locks: SeriesRefreshLocks,
    text_index_cache: std::sync::Arc<TextIndexCache>,
    text_refresh_locks: TextRefreshLocks,
    // Gate for ablating retained series-cache state (KYMO_SERIES_CACHE=0).
    // Concurrent duplicate reads still share one in-flight result, because
    // removing that protection can recreate the OOM this cache mitigates.
    series_cache_enabled: bool,
}

impl ChClient {
    #[cfg(test)]
    pub(crate) fn test_client(&self) -> &Client {
        &self.client
    }

    /// Deletes a green live case's rows for its project from the metric tables and the registry outbox; each delete waits for its mutation.
    #[cfg(test)]
    pub(crate) async fn delete_live_project(&self, project_id: &str) -> Result<()> {
        for table in [METRICS_TABLE, RICH_METRICS_TABLE, REGISTRY_OUTBOX_TABLE] {
            self.client
                .query(&format!(
                    "ALTER TABLE {table} DELETE WHERE project_id = ? SETTINGS mutations_sync = 2"
                ))
                .bind(project_id)
                .execute()
                .await?;
        }
        Ok(())
    }

    pub fn new(url: &str) -> Result<Self> {
        // Auth: CLICKHOUSE_USER defaults to "default"; CLICKHOUSE_PASSWORD is
        // empty when unset (local dev / pre-auth clusters). In production both
        // come from a secret so only the server can query CH.
        let user = crate::env::string_or("CLICKHOUSE_USER", "default");
        let password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
        Self::configured(Client::default(), url, user, password)
    }

    pub fn new_local(
        url: &str,
        server_cert_path: &Path,
        user: &str,
        password: &str,
    ) -> Result<Self> {
        validate_local_url(url)?;
        ensure!(!user.is_empty(), "CLICKHOUSE_USER must not be empty");
        ensure!(
            !password.is_empty(),
            "CLICKHOUSE_PASSWORD must not be empty in local mode"
        );

        let pem = crate::private_file::read(
            server_cert_path,
            "ClickHouse server certificate",
            16 * 1024,
        )?;
        let text = std::str::from_utf8(&pem).context("ClickHouse certificate is not UTF-8 PEM")?;
        ensure!(
            text.matches("-----BEGIN ").count() == 1 && text.matches("-----END ").count() == 1,
            "ClickHouse certificate file must contain exactly one PEM object"
        );
        let mut certificates = CertificateDer::pem_slice_iter(&pem);
        let certificate = certificates
            .next()
            .transpose()
            .context("parse ClickHouse server certificate")?
            .context("ClickHouse certificate file contains no certificate")?;
        ensure!(
            certificates.next().is_none(),
            "ClickHouse certificate file must contain exactly one certificate"
        );

        let mut roots = RootCertStore::empty();
        roots
            .add(certificate)
            .context("add pinned ClickHouse server certificate")?;
        // Keep this aligned with tools/local-transport-qualification/src/clickhouse.rs::pinned_client_for_url; drift between the qualified and production connectors is a security bug. The private, launcher-generated self-signed leaf is the only trust root, and https_only prevents credentials or query bytes from reaching a plaintext endpoint.
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let tls = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        let connector = HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_only()
            .enable_http1()
            .wrap_connector(http);
        let http = HyperClient::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(2))
            .build(connector);
        Self::configured(
            Client::with_http_client(http),
            url,
            user.to_owned(),
            password.to_owned(),
        )
    }

    fn configured(client: Client, url: &str, user: String, password: String) -> Result<Self> {
        // Async inserts: every ingest stream flushes its own ~2s batch, so part
        // formation scales with the number of live runs and merges fall behind
        // (ingest stalls, OOM via FINAL-over-many-parts). async_insert makes
        // ClickHouse coalesce those inserts server-side into a few parts at a
        // rate independent of run count (25.3's adaptive busy timeout picks the
        // flush cadence). wait_for_async_insert keeps today's ack semantics:
        // insert_batch returns only once the data is in a part, so a failed
        // write still errors the stream that sent it. Pinning the async data
        // ceiling above ingest's maximum cut prevents large requests from
        // falling back to synchronous streaming. Options are client-wide;
        // the insert ones are no-ops on SELECTs and DDL.
        let client = client
            .with_url(url)
            .with_user(user)
            .with_password(password)
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "1")
            // Metric discovery relies on the dependent outbox MV completing
            // before an ingest ACK. Do not allow a server-profile change to
            // turn an MV failure into silently accepted source data.
            .with_option("materialized_views_ignore_errors", "0")
            .with_option(
                "async_insert_max_data_size",
                ASYNC_INSERT_MAX_DATA_SIZE_BYTES.to_string(),
            );
        let client = COMPLETE_READ_SETTINGS
            .into_iter()
            .fold(client, |client, (name, value)| {
                client.with_option(name, value)
            });
        // Strict because this is an emergency ablation control: a typo while
        // trying to disable retention must not silently leave it enabled.
        let series_cache_enabled = crate::env::required_bool("KYMO_SERIES_CACHE", true)?;
        tracing::info!(series_cache_enabled, "series cache configured");
        Ok(Self {
            client,
            series_cache: std::sync::Arc::new(crate::series_cache::SeriesCache::new()),
            series_refresh_locks: SeriesRefreshLocks::default(),
            text_index_cache: std::sync::Arc::new(TextIndexCache::new()),
            text_refresh_locks: TextRefreshLocks::default(),
            series_cache_enabled,
        })
    }

    /// Evicts these (project, run) identities from both chart caches; returns the entries removed.
    pub fn purge_run_caches(&self, runs: &[(&str, &str)]) -> usize {
        self.series_cache.purge_runs(runs.iter().copied())
            + self.text_index_cache.purge_runs(runs.iter().copied())
    }

    #[cfg(test)]
    pub(crate) fn series_cache(&self) -> &crate::series_cache::SeriesCache {
        &self.series_cache
    }

    pub async fn ensure_schema(&self) -> Result<()> {
        let mut delay = std::time::Duration::from_secs(1);
        let max_delay = std::time::Duration::from_secs(30);

        loop {
            match self.try_ensure_schema().await {
                Ok(()) => {
                    tracing::info!("ClickHouse schema ready");
                    return Ok(());
                }
                Err(e) if is_retryable_schema_error(&e) => {
                    tracing::warn!("ClickHouse not ready, retrying in {:?}: {e}", delay);
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(max_delay);
                }
                Err(e) => {
                    return Err(e)
                        .context("ClickHouse schema setup failed permanently; refusing to start");
                }
            }
        }
    }

    async fn try_ensure_schema(&self) -> Result<()> {
        self.client
            .query("CREATE DATABASE IF NOT EXISTS mkdb2")
            .execute()
            .await?;

        // Fresh databases get the final tagged + codec'd shape directly, so
        // `migrate_to_tagged_schema` below early-returns (tag is already in the
        // sort key). The ALTER statements that follow are no-ops on a fresh DB
        // and only matter when migrating an older non-tagged table in place.
        //
        // Codecs: DoubleDelta+ZSTD on the monotonic Int64s (step/timestamp_ms/
        // inserted_at) and Gorilla+ZSTD on the float value — ~3-4x smaller than
        // the previous uncompressed Float64 layout.
        self.client
            .query(
                "CREATE TABLE IF NOT EXISTS mkdb2.metrics (
                    project_id   LowCardinality(String),
                    run_id       LowCardinality(String),
                    metric_name  LowCardinality(String),
                    tag          LowCardinality(String) DEFAULT '',
                    step         Int64 CODEC(DoubleDelta, ZSTD(1)),
                    timestamp_ms Int64 DEFAULT 0 CODEC(DoubleDelta, ZSTD(1)),
                    value        Nullable(Float32) CODEC(Gorilla, ZSTD(1)),
                    cdn_key      Nullable(String) CODEC(ZSTD(3)),
                    text_data    Nullable(String) CODEC(ZSTD(3)),
                    inserted_at  DateTime64(3) DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1)),
                    INDEX idx_inserted_at inserted_at TYPE minmax GRANULARITY 1,
                    INDEX idx_cdn_key cdn_key TYPE minmax GRANULARITY 1
                ) ENGINE = ReplacingMergeTree(inserted_at)
                ORDER BY (project_id, run_id, metric_name, tag, step)
                PARTITION BY project_id
                SETTINGS index_granularity = 8192",
            )
            .execute()
            .await?;

        // The series cache's incremental reads filter on inserted_at; the
        // minmax skip index is what lets them skip the granules of already-
        // cached history instead of rescanning the series. One-time ADD +
        // MATERIALIZE for tables created before the index existed (ADD
        // alone covers only parts written afterwards; the MATERIALIZE
        // mutation indexes the existing ones, async and only enqueued on
        // the boot that adds the index).
        if !self.metrics_skip_index_exists("idx_inserted_at").await? {
            self.client
                .query(
                    "ALTER TABLE mkdb2.metrics ADD INDEX IF NOT EXISTS
                     idx_inserted_at inserted_at TYPE minmax GRANULARITY 1",
                )
                .execute()
                .await?;
            if let Err(e) = self
                .client
                .query("ALTER TABLE mkdb2.metrics MATERIALIZE INDEX idx_inserted_at")
                .execute()
                .await
            {
                tracing::warn!(
                    "materializing idx_inserted_at failed (old parts stay unpruned \
                     until merges rewrite them): {e}"
                );
            }
        }

        // Migrate existing tables: make value nullable, add cdn_key column.
        // These are no-ops if the column is already in the right state, but a
        // rejected ALTER is fatal: accepting traffic with a partial schema
        // only defers the failure to inserts and queries.
        self.apply_required_metrics_alters().await?;

        // Migrate to v2 schema: add `tag` column in the ORDER BY.
        // MODIFY ORDER BY can't insert a column in the middle, so we
        // recreate the table if the tag column isn't in the sort key. Any
        // future source-table swap must explicitly preserve or recreate the
        // registry outbox MV binding; do not assume RENAME retargets it across
        // ClickHouse versions.
        self.migrate_to_tagged_schema().await?;
        self.validate_metrics_schema().await?;
        self.ensure_cdn_key_index().await?;
        self.ensure_rich_metrics_schema().await?;

        // Durable cross-store outbox for metric discovery. The materialized
        // view is part of the accepted ClickHouse insert, so a hard kill after
        // the metrics ACK but before Postgres registration still leaves the
        // compact metric identity/type here for the next boot to reconcile.
        //
        // ReplacingMergeTree's version is the type precedence itself:
        // CDN (1) < NUMERIC (2) < TEXT_STREAM (3). FINAL therefore returns
        // one maximum-precedence row per metric even when many source blocks
        // observed it. This also makes duplicate MV output from an ambiguous
        // async-insert retry harmless without depending on ClickHouse's MV
        // deduplication behavior. The server clears the table only during
        // startup, after all rows have been idempotently written to Postgres
        // and before it accepts new ingest, so no concurrent MV insert can be
        // lost.
        self.ensure_metric_registry_outbox(
            METRICS_TABLE,
            REGISTRY_OUTBOX_TABLE,
            REGISTRY_OUTBOX_VIEW,
        )
        .await?;
        self.ensure_rich_metric_registry_outbox().await?;

        Ok(())
    }

    async fn ensure_rich_metrics_schema(&self) -> Result<()> {
        self.client
            .query(
                "CREATE TABLE IF NOT EXISTS mkdb2.rich_metrics (
                    project_id      LowCardinality(String),
                    run_id          LowCardinality(String),
                    metric_name     LowCardinality(String),
                    tag             LowCardinality(String) DEFAULT '',
                    step            Int64 CODEC(DoubleDelta, ZSTD(1)),
                    timestamp_ms    Int64 DEFAULT 0 CODEC(DoubleDelta, ZSTD(1)),
                    cdn_key         String CODEC(ZSTD(3)),
                    mutation_version UInt64,
                    inserted_at     DateTime64(3) DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1))
                ) ENGINE = ReplacingMergeTree(mutation_version)
                ORDER BY (project_id, run_id, metric_name, tag, step)
                PARTITION BY project_id
                SETTINGS index_granularity = 8192",
            )
            .execute()
            .await?;

        let columns = self
            .client
            .query(
                "SELECT name, type AS column_type, default_kind, default_expression
                 FROM system.columns
                 WHERE database = 'mkdb2' AND table = 'rich_metrics'",
            )
            .fetch_all::<SchemaColumn>()
            .await
            .context("reading mkdb2.rich_metrics columns")?;
        let table = self
            .client
            .query(
                "SELECT engine, engine_full, sorting_key, partition_key
                 FROM system.tables
                 WHERE database = 'mkdb2' AND name = 'rich_metrics'",
            )
            .fetch_one::<SchemaTable>()
            .await
            .context("reading mkdb2.rich_metrics table definition")?;
        validate_rich_metrics_schema_shape(&columns, &table)
    }

    async fn ensure_rich_metric_registry_outbox(&self) -> Result<()> {
        let ddl = format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {RICH_REGISTRY_OUTBOX_VIEW}
             TO {REGISTRY_OUTBOX_TABLE} AS
             SELECT project_id, run_id, metric_name, toUInt8(1) AS metric_type
             FROM {RICH_METRICS_TABLE}
             GROUP BY project_id, run_id, metric_name"
        );
        self.client.query(&ddl).execute().await?;
        Ok(())
    }

    async fn apply_required_metrics_alters(&self) -> Result<()> {
        self.client
            .query("ALTER TABLE mkdb2.metrics MODIFY COLUMN IF EXISTS value Nullable(Float32)")
            .execute()
            .await
            .context("making mkdb2.metrics.value nullable")?;
        self
            .client
            .query("ALTER TABLE mkdb2.metrics ADD COLUMN IF NOT EXISTS cdn_key Nullable(String) AFTER value")
            .execute()
            .await
            .context("adding mkdb2.metrics.cdn_key")?;

        // Add timestamp_ms column for dual-axis (step + time) support
        self
            .client
            .query("ALTER TABLE mkdb2.metrics ADD COLUMN IF NOT EXISTS timestamp_ms Int64 DEFAULT 0 AFTER step")
            .execute()
            .await
            .context("adding mkdb2.metrics.timestamp_ms")?;

        // Add text_data column for text stream metrics (stdout/stderr)
        self
            .client
            .query("ALTER TABLE mkdb2.metrics ADD COLUMN IF NOT EXISTS text_data Nullable(String) AFTER cdn_key")
            .execute()
            .await
            .context("adding mkdb2.metrics.text_data")?;
        Ok(())
    }

    async fn metrics_skip_index_exists(&self, name: &str) -> Result<bool> {
        Ok(!self
            .client
            .query(
                "SELECT name AS val FROM system.data_skipping_indices
                 WHERE database = 'mkdb2' AND table = 'metrics' AND name = ?",
            )
            .bind(name)
            .fetch_all::<SingleString>()
            .await?
            .is_empty())
    }

    /// The CDN root-scan index (docs/cdn-gcs-migration.md § Root scan index). Boot never MATERIALIZEs it: that mutation reads every part and holds off reaper deletions, so it is an operator step. It is only an optimization, so a rejected ADD warns instead of refusing to start.
    async fn ensure_cdn_key_index(&self) -> Result<()> {
        if self.metrics_skip_index_exists("idx_cdn_key").await? {
            return Ok(());
        }
        match self
            .client
            .query(
                "ALTER TABLE mkdb2.metrics ADD INDEX IF NOT EXISTS
                 idx_cdn_key cdn_key TYPE minmax GRANULARITY 1",
            )
            .execute()
            .await
        {
            Ok(()) => tracing::info!(
                "added idx_cdn_key to mkdb2.metrics; parts written before it stay unindexed until an operator runs MATERIALIZE INDEX"
            ),
            Err(e) => tracing::warn!(
                "adding idx_cdn_key to mkdb2.metrics failed; the CDN collector keeps reading every part: {e}"
            ),
        }
        Ok(())
    }

    async fn validate_metrics_schema(&self) -> Result<()> {
        let columns = self
            .client
            .query(
                "SELECT name, type AS column_type, default_kind, default_expression
                 FROM system.columns
                 WHERE database = 'mkdb2' AND table = 'metrics'",
            )
            .fetch_all::<SchemaColumn>()
            .await
            .context("reading final mkdb2.metrics schema")?;
        let table = self
            .client
            .query(
                "SELECT engine, engine_full, sorting_key, partition_key
                 FROM system.tables
                 WHERE database = 'mkdb2' AND name = 'metrics'",
            )
            .fetch_one::<SchemaTable>()
            .await
            .context("reading final mkdb2.metrics table definition")?;
        validate_metrics_schema_shape(&columns, &table)
            .context("mkdb2.metrics schema validation failed after migrations")
    }

    /// Create the metric-discovery outbox from trusted, code-owned ClickHouse
    /// identifiers. Keeping this DDL in one place lets the live integration
    /// test exercise the exact production table and view definitions.
    async fn ensure_metric_registry_outbox(
        &self,
        source: &str,
        target: &str,
        view: &str,
    ) -> Result<()> {
        let table_ddl = format!(
            "CREATE TABLE IF NOT EXISTS {target} (
                project_id   LowCardinality(String),
                run_id       LowCardinality(String),
                metric_name  LowCardinality(String),
                metric_type  UInt8
            ) ENGINE = ReplacingMergeTree(metric_type)
            ORDER BY (project_id, run_id, metric_name)"
        );
        self.client.query(&table_ddl).execute().await?;

        // Keep the 1/2/3 precedence aligned with registry_reconcile's strict
        // decoder and PgStore::register_run_metrics's upgrade order.
        // IF NOT EXISTS does not update an installed view: changing this SELECT
        // requires an explicit DROP/CREATE rollout for the view while retaining
        // its target table and recovery rows.
        let view_ddl = format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {view}
             TO {target} AS
             SELECT project_id, run_id, metric_name,
                    max(toUInt8(multiIf(text_data IS NOT NULL, 3,
                                        value IS NOT NULL, 2, 1))) AS metric_type
             FROM {source}
             GROUP BY project_id, run_id, metric_name"
        );
        self.client.query(&view_ddl).execute().await?;
        Ok(())
    }

    fn metric_registry_outbox_at(
        &self,
        target: &str,
    ) -> Result<clickhouse::query::RowCursor<RegistryRow>> {
        // The target has no PARTITION BY, so FINAL collapses every version of
        // one identity globally. A streamed Postgres batch therefore cannot
        // contain duplicate conflict keys in one ON CONFLICT statement.
        let sql = format!(
            "SELECT project_id, run_id, metric_name, metric_type
             FROM {target} FINAL"
        );
        Ok(self.client.query(&sql).fetch::<RegistryRow>()?)
    }

    /// Stream the compact metric-discovery outbox. This is intentionally a
    /// cursor rather than `fetch_all`: a maintenance backfill can make the
    /// outbox large, while boot reconciliation needs only one bounded
    /// Postgres batch resident at a time.
    pub fn metric_registry_outbox(&self) -> Result<clickhouse::query::RowCursor<RegistryRow>> {
        self.metric_registry_outbox_at(REGISTRY_OUTBOX_TABLE)
    }

    /// One run's outbox registrations, FINAL-collapsed like the boot cursor;
    /// FinalizeImportRun drains it through the boot reconcile's `drain`.
    pub fn run_metric_registry_outbox(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> Result<clickhouse::query::RowCursor<RegistryRow>> {
        let sql = format!(
            "SELECT project_id, run_id, metric_name, metric_type
             FROM {REGISTRY_OUTBOX_TABLE} FINAL
             WHERE project_id = ? AND run_id = ?"
        );
        Ok(self
            .client
            .query(&sql)
            .bind(project_id)
            .bind(run_id)
            .fetch::<RegistryRow>()?)
    }

    async fn clear_metric_registry_outbox_at(&self, target: &str) -> Result<()> {
        self.client
            .query(&format!("TRUNCATE TABLE {target}"))
            .execute()
            .await?;
        Ok(())
    }

    /// Clear only after every streamed row committed to Postgres. Called
    /// during startup before ingest is reachable; a failure before this point
    /// leaves the whole outbox available for an idempotent retry next boot.
    pub async fn clear_metric_registry_outbox(&self) -> Result<()> {
        self.clear_metric_registry_outbox_at(REGISTRY_OUTBOX_TABLE)
            .await
    }

    async fn metric_registry_outbox_storage_at(
        &self,
        target: &str,
    ) -> Result<RegistryOutboxStorage> {
        let (database, table) = target
            .split_once('.')
            .expect("metric-registry outbox table must be database-qualified");
        Ok(self
            .client
            .query(
                "SELECT sum(rows) AS rows,
                        sum(bytes_on_disk) AS bytes,
                        count() AS parts
                 FROM system.parts
                 WHERE active AND database = ? AND table = ?",
            )
            .bind(database)
            .bind(table)
            .fetch_one::<RegistryOutboxStorage>()
            .await?)
    }

    /// Physical outbox footprint, including duplicate identities not yet
    /// collapsed by background merges. This is the storage/part pressure that
    /// can affect the durability-coupled materialized view.
    pub async fn metric_registry_outbox_storage(&self) -> Result<RegistryOutboxStorage> {
        self.metric_registry_outbox_storage_at(REGISTRY_OUTBOX_TABLE)
            .await
    }

    /// Refuse the newly reserved frontend route even when an old project is
    /// visible only in ClickHouse. Checking the source directly also covers
    /// historical registry gaps and orphan data, and makes the rollout
    /// preflight match startup exactly.
    pub async fn ensure_reserved_project_absent(&self, project_id: &str) -> Result<()> {
        let row = self
            .client
            .query(
                "SELECT
                     (SELECT count() FROM mkdb2.metrics WHERE project_id = ?)
                   + (SELECT count() FROM mkdb2.rich_metrics WHERE project_id = ?)
                   AS val",
            )
            .bind(project_id)
            .bind(project_id)
            .fetch_one::<SingleCount>()
            .await?;
        anyhow::ensure!(
            row.val == 0,
            "project id {project_id:?} is reserved for the Trash route but still has ClickHouse data; move that project to another id in both Postgres and ClickHouse before starting this server"
        );
        Ok(())
    }

    async fn migrate_to_tagged_schema(&self) -> Result<()> {
        // Check if tag is already in the sorting key
        let sorting_key = self
            .client
            .query(
                "SELECT sorting_key AS val FROM system.tables
                 WHERE database = 'mkdb2' AND name = 'metrics'",
            )
            .fetch_one::<SingleString>()
            .await
            .context("reading mkdb2.metrics sorting key")?;

        match normalized_key(&sorting_key.val).as_str() {
            "project_id,run_id,metric_name,tag,step" => return Ok(()),
            "project_id,run_id,metric_name,step" => {}
            other => anyhow::bail!(
                "mkdb2.metrics has unsupported sorting key {other:?}; refusing the legacy migration because it cannot preserve an already-tagged or otherwise noncanonical table"
            ),
        }

        // Some historical deployments added `tag` before the ORDER BY
        // recreation landed. Their key still has the exact legacy shape, but
        // their rows may already contain non-empty tags. Preserve that column
        // when present instead of treating the key alone as proof that the
        // table is untagged.
        let has_tag_column = self
            .client
            .query(
                "SELECT count() AS val FROM system.columns
                 WHERE database = 'mkdb2' AND table = 'metrics' AND name = 'tag'",
            )
            .fetch_one::<SingleCount>()
            .await
            .context("checking whether legacy mkdb2.metrics already has a tag column")?
            .val
            > 0;

        tracing::info!("Migrating mkdb2.metrics to tagged schema (adding tag to ORDER BY)");

        // Create new table with tag in the sort key
        self.client
            .query(
                "CREATE TABLE IF NOT EXISTS mkdb2.metrics_v2 (
                    project_id   LowCardinality(String),
                    run_id       LowCardinality(String),
                    metric_name  LowCardinality(String),
                    tag          LowCardinality(String) DEFAULT '',
                    step         Int64 CODEC(DoubleDelta, ZSTD(1)),
                    timestamp_ms Int64 DEFAULT 0 CODEC(DoubleDelta, ZSTD(1)),
                    value        Nullable(Float32) CODEC(Gorilla, ZSTD(1)),
                    cdn_key      Nullable(String) CODEC(ZSTD(3)),
                    text_data    Nullable(String) CODEC(ZSTD(3)),
                    inserted_at  DateTime64(3) DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1)),
                    INDEX idx_inserted_at inserted_at TYPE minmax GRANULARITY 1,
                    INDEX idx_cdn_key cdn_key TYPE minmax GRANULARITY 1
                ) ENGINE = ReplacingMergeTree(inserted_at)
                ORDER BY (project_id, run_id, metric_name, tag, step)
                PARTITION BY project_id
                SETTINGS index_granularity = 8192",
            )
            .execute()
            .await?;

        // Copy existing data. Must match every column in metrics_v2 —
        // easy to break when new columns are added by earlier ALTER statements,
        // so we name them explicitly and include timestamp_ms/text_data.
        let (tag_projection, final_modifier) = if has_tag_column {
            // Under the old key, FINAL considers distinct tags at the same
            // step to be competing versions. Copy physical rows so the new
            // tag-qualified key can preserve them; its ReplacingMergeTree
            // will still collapse genuine same-tag retries.
            ("tag", "")
        } else {
            ("'' AS tag", " FINAL")
        };
        self.client
            .query(&format!(
                "INSERT INTO mkdb2.metrics_v2
                 SELECT project_id, run_id, metric_name, {tag_projection},
                        step, timestamp_ms, value, cdn_key, text_data, inserted_at
                 FROM mkdb2.metrics{final_modifier}"
            ))
            .execute()
            .await?;

        // Swap tables
        self.client
            .query("RENAME TABLE mkdb2.metrics TO mkdb2.metrics_old, mkdb2.metrics_v2 TO mkdb2.metrics")
            .execute()
            .await?;

        self.client
            .query("DROP TABLE IF EXISTS mkdb2.metrics_old")
            .execute()
            .await?;

        tracing::info!("Migration complete: mkdb2.metrics now has tag in ORDER BY");
        Ok(())
    }

    /// Insert with clickhouse-rs's native per-chunk send and final-response
    /// deadlines (`io_timeout` each), not a single whole-insert deadline. In
    /// clickhouse-rs 0.13.x the HTTP request runs in a spawned task; dropping
    /// `Insert` aborts its chunk sender but detaches the task's `JoinHandle`.
    /// Keeping `Insert::end()` alive is what lets its native end timeout abort
    /// that HTTP task. Ingest therefore drives this method in an independently
    /// owned task and applies a separate polling deadline to its result outside
    /// it; cancellation may detach the owned task, but cannot drop its native
    /// timeout or release its admission permit prematurely.
    ///
    /// `sync` runs the INSERT with `async_insert=0` on a per-call client, so the
    /// ack means the rows are in exactly one new part per project partition —
    /// the bulk-import lane's contract, whose cuts are far larger than live
    /// ones (hence its far longer `io_timeout`).
    pub async fn insert_batch(
        &self,
        rows: &[MetricRow],
        io_timeout: Duration,
        sync: bool,
    ) -> clickhouse::error::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let sync_client;
        let client = if sync {
            sync_client = self.client.clone().with_option("async_insert", "0");
            &sync_client
        } else {
            &self.client
        };
        let result = async {
            let mut insert = client
                .insert(METRICS_TABLE)?
                .with_timeouts(Some(io_timeout), Some(io_timeout));
            for row in rows {
                insert.write(row).await?;
            }
            insert.end().await
        }
        .await;

        // Note before returning, so the ack and any version announcing these rows follow the note (SeriesCache::note_bumps). A failed insert may still commit — the error can follow the commit, or ClickHouse can finish a request we gave up on — so it is noted again once that would long be over; the window bounds any later commit.
        let runs: std::collections::HashSet<&str> =
            rows.iter().map(|row| row.run_id.as_str()).collect();
        self.series_cache.note_bumps(runs.iter().copied());
        if result.is_ok() {
            tracing::debug!("Flushed {} rows to ClickHouse", rows.len());
        } else {
            let cache = self.series_cache.clone();
            let runs: Vec<String> = runs.into_iter().map(str::to_owned).collect();
            tokio::spawn(async move {
                tokio::time::sleep(2 * io_timeout).await;
                cache.note_bumps(runs.iter().map(String::as_str));
            });
        }
        result
    }

    pub async fn insert_rich_mutation(
        &self,
        row: &RichMetricRow,
        io_timeout: Duration,
    ) -> clickhouse::error::Result<()> {
        let mut insert = self
            .client
            .insert(RICH_METRICS_TABLE)?
            .with_timeouts(Some(io_timeout), Some(io_timeout));
        insert.write(row).await?;
        insert.end().await
    }

    // --- Physical run cleanup ---

    /// Drain ClickHouse's server-side async-insert queue and verify that no
    /// metrics INSERT is still executing. `wait_for_async_insert=1` normally
    /// leaves nothing to drain, but this barrier also covers work accepted by
    /// an old server process whose outcome was unknown when it exited.
    pub async fn barrier_metrics_inserts(&self) -> Result<()> {
        self.client
            .query("SYSTEM FLUSH ASYNC INSERT QUEUE")
            .execute()
            .await
            .context("flushing ClickHouse async-insert queue")?;
        let active_query = format!(
            "SELECT count() AS val
             FROM system.processes
             WHERE query_kind = 'Insert'
               AND (positionCaseInsensitive(query, '{METRICS_TABLE}') > 0
                    OR positionCaseInsensitive(query, '{RICH_METRICS_TABLE}') > 0)"
        );
        let active = self
            .client
            .query(&active_query)
            .fetch_one::<SingleCount>()
            .await
            .context("checking for active ClickHouse metrics inserts")?
            .val;
        anyhow::ensure!(
            active == 0,
            "ClickHouse insert barrier left {active} active metrics inserts"
        );
        Ok(())
    }

    /// Delete a project's claimed runs in one synchronous mutation and verify
    /// that no physical rows remain. MergeTree mutations rewrite every
    /// affected part, so deleting identities one at a time can rewrite the
    /// same large project part repeatedly.
    pub async fn delete_runs_sync(&self, project_id: &str, run_ids: &[&str]) -> Result<()> {
        anyhow::ensure!(!run_ids.is_empty(), "run deletion batch cannot be empty");

        let placeholders = run_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let count_sql = |table: &str| {
            format!(
                "SELECT count() AS val FROM {table}
                 WHERE project_id = ? AND run_id IN ({placeholders})"
            )
        };
        let count_rows = async |table: &str| -> Result<u64> {
            let sql = count_sql(table);
            let mut count = self.client.query(&sql).bind(project_id);
            for run_id in run_ids {
                count = count.bind(*run_id);
            }
            Ok(count.fetch_one::<SingleCount>().await?.val)
        };
        let metrics_rows = count_rows(METRICS_TABLE).await?;
        let rich_rows = count_rows(RICH_METRICS_TABLE).await?;
        if metrics_rows == 0 && rich_rows == 0 {
            // A previous synchronous mutation may have committed even when its
            // acknowledgement was lost. The durable lifecycle claim and the
            // reaper's insert barrier make this zero stable.
            return Ok(());
        }

        let unfinished = self
            .client
            .query(
                "SELECT count() AS val FROM system.mutations
                 WHERE database = 'mkdb2'
                   AND table IN ('metrics', 'rich_metrics') AND NOT is_done",
            )
            .fetch_one::<SingleCount>()
            .await?;
        anyhow::ensure!(
            unfinished.val == 0,
            "ClickHouse has {} unfinished metrics mutations; refusing to queue another deletion",
            unfinished.val
        );

        // A DELETE mutation keeps a source part while writing its replacement.
        // Refuse to enqueue one unless unreserved space is conservatively twice
        // the project's largest active part plus a fixed ingest margin.
        let headroom = self
            .client
            .query(
                "SELECT
                     coalesce(max(p.bytes_on_disk), 0) AS largest_part_bytes,
                     coalesce(min(d.unreserved_space), 0) AS unreserved_bytes
                 FROM system.parts AS p
                 INNER JOIN system.disks AS d ON d.name = p.disk_name
                 WHERE p.database = 'mkdb2' AND p.table IN ('metrics', 'rich_metrics')
                   AND p.active AND p.partition = ?",
            )
            .bind(project_id)
            .fetch_one::<DeleteHeadroom>()
            .await?;
        let required_free = headroom
            .largest_part_bytes
            .saturating_mul(DELETE_HEADROOM_MULTIPLIER)
            .saturating_add(DELETE_HEADROOM_RESERVE_BYTES);
        anyhow::ensure!(
            headroom.unreserved_bytes >= required_free,
            "insufficient ClickHouse deletion headroom for project {project_id:?}: \
             unreserved={} bytes, require={} bytes for a {}-byte largest active part",
            headroom.unreserved_bytes,
            required_free,
            headroom.largest_part_bytes,
        );

        for (table, rows) in [
            (METRICS_TABLE, metrics_rows),
            (RICH_METRICS_TABLE, rich_rows),
        ] {
            if rows == 0 {
                continue;
            }
            let delete_sql = format!(
                "ALTER TABLE {table} DELETE
                 WHERE project_id = ? AND run_id IN ({placeholders})
                 SETTINGS mutations_sync = 2"
            );
            let mut delete = self.client.query(&delete_sql).bind(project_id);
            for run_id in run_ids {
                delete = delete.bind(*run_id);
            }
            delete.execute().await?;
        }

        let remaining = count_rows(METRICS_TABLE)
            .await?
            .saturating_add(count_rows(RICH_METRICS_TABLE).await?);
        anyhow::ensure!(
            remaining == 0,
            "ClickHouse run deletion completed with {} rows still visible for project {project_id:?} after deleting {} runs",
            remaining,
            run_ids.len(),
        );
        Ok(())
    }

    // --- Discovery ---

    // --- Numeric queries ---

    /// Each run's scalar (tag = '') numeric (step, value) points of one metric within the step range, in `run_ids` order, from one statement.
    pub async fn query_raw_many(
        &self,
        project_id: &str,
        run_ids: &[String],
        metric_name: &str,
        step_min: i64,
        step_max: i64,
    ) -> Result<Vec<Vec<(i64, f64)>>> {
        #[derive(Deserialize, clickhouse::Row)]
        struct Row<'a> {
            run_id: &'a str,
            step: i64,
            value: f32,
        }
        // ClickHouse resolves a name to a same-named SELECT alias before the column, so a filter on a column its statement re-aliases must qualify it, or it tests the alias: `metrics.value` here and in `fetch_full_many`/`fetch_increment`, `metrics.cdn_key` in `query_cdn_keys_batch`.
        let mut cursor = self
            .client
            .query(
                "SELECT run_id, step, assumeNotNull(value) AS value
                 FROM mkdb2.metrics FINAL
                 WHERE project_id = ? AND run_id IN ? AND metric_name = ?
                   AND step >= ? AND step <= ?
                   AND metrics.value IS NOT NULL AND tag = ''
                 ORDER BY run_id, step",
            )
            .bind(project_id)
            .bind(run_ids)
            .bind(metric_name)
            .bind(step_min)
            .bind(step_max)
            .fetch::<Row>()?;
        let mut series = vec![Vec::new(); run_ids.len()];
        let mut position = 0;
        while let Some(row) = cursor.next().await? {
            series[run_position(run_ids, row.run_id, &mut position)?]
                .push((row.step, f64::from(row.value)));
        }
        Ok(series)
    }

    /// All points of a metric — scalar (tag = '') and tagged together in
    /// one query (no tagged probe + scalar fallback), with `inserted_at`
    /// for the series cache's watermark. Served from [`crate::series_cache::SeriesCache`] when
    /// possible: a hit within the freshness window costs no ClickHouse
    /// round trip at all, a stale hit costs one incremental read of rows
    /// newer than the watermark, and only a miss (or a detected history
    /// rewrite — a resumed run re-logging old steps) pays a full scan.
    /// Returns the FULL series; callers range-filter themselves.
    pub async fn query_raw_any_cached(
        &self,
        project_id: &str,
        run_id: &str,
        metric_name: &str,
        detach: impl FnOnce() -> Option<RefreshDetach>,
    ) -> RefreshOutcome {
        use crate::series_cache::{Lookup, WATERMARK_OVERLAP_MS};
        let key = SeriesKey::new(project_id, run_id, metric_name);
        // The refresh future must own everything it touches: with `detach` it
        // runs in a spawned task that outlives this call and its borrows.
        let ch = self.clone();
        let refresh_key = key.clone();
        self.series_refresh_locks
            .get_or_refresh(&self.series_cache, &key, detach, move |lookup| async move {
                let SeriesKey {
                    project_id,
                    run_id,
                    metric_name,
                } = &refresh_key;
                let origin = if let Lookup::Stale { watermark_ms, gen } = lookup {
                    let (fetch_started, increment) = ch
                        .fetch_increment(
                            project_id,
                            run_id,
                            metric_name,
                            watermark_ms - WATERMARK_OVERLAP_MS,
                        )
                        .await?;
                    if let Ok(rows) =
                        ch.series_cache
                            .apply_increment(&refresh_key, increment, fetch_started, gen)
                    {
                        return Ok(rows);
                    }
                    LineageOrigin::Rewrite
                } else {
                    LineageOrigin::Miss
                };

                // Miss, or an incremental refresh whose base was rewritten, evicted, or replaced: rebuild from the full series.
                let (fetch_started, mut series) = ch
                    .fetch_full_many(project_id, metric_name, std::slice::from_ref(run_id))
                    .await?;
                Ok(ch.store_full(refresh_key, series.swap_remove(0), fetch_started, origin))
            })
            .await
    }

    /// The authoritative read of one metric's whole series for each of `run_ids` (one project) in one statement, in `run_ids` order.
    /// FINAL dedups every series, and with project and metric fixed, ORDER BY (run, tag, step) is the table's sorting-key order, so ClickHouse streams it.
    /// On prod, one read of 12 series cost about a third of the ClickHouse time of their 12 separate reads and opened 17 parts instead of 76.
    /// Returns its start, taken before the query is sent, with the rows: the bump gate and [`crate::series_cache::watermark_cap`] count the stored rows from it. The other cached reads return theirs the same way.
    async fn fetch_full_many(
        &self,
        project_id: &str,
        metric_name: &str,
        run_ids: &[String],
    ) -> Result<(Instant, Vec<Vec<VersionedRawPoint>>)> {
        #[derive(Deserialize, clickhouse::Row)]
        struct Row<'a> {
            run_id: &'a str,
            tag: String,
            step: i64,
            timestamp_ms: i64,
            value: f32,
            inserted_ms: i64,
        }
        let started = Instant::now();
        let mut cursor = self
            .client
            .query(
                "SELECT run_id, tag, step, timestamp_ms,
                        assumeNotNull(value) AS value,
                        toUnixTimestamp64Milli(inserted_at) AS inserted_ms
                 FROM mkdb2.metrics FINAL
                 WHERE project_id = ? AND run_id IN ? AND metric_name = ?
                   AND metrics.value IS NOT NULL
                 ORDER BY run_id, tag, step",
            )
            .bind(project_id)
            .bind(run_ids)
            .bind(metric_name)
            .fetch::<Row>()?;
        let mut series: Vec<Vec<VersionedRawPoint>> = vec![Vec::new(); run_ids.len()];
        let mut position = 0;
        while let Some(row) = cursor.next().await? {
            series[run_position(run_ids, row.run_id, &mut position)?].push(VersionedRawPoint {
                tag: row.tag,
                step: row.step,
                timestamp_ms: row.timestamp_ms,
                value: row.value,
                is_value: 1,
                inserted_ms: row.inserted_ms,
            });
        }
        Ok((started, series))
    }

    /// The (tag, step)-ordered rows of one series inserted after `after_inserted_ms`.
    ///
    /// Deliberately NOT under FINAL: FINAL merges across the series' whole key range AND disables the `idx_inserted_at` skip index (`use_skip_indexes_if_final` is off by default, for good reason in the general case), which would turn every refresh back into the full scan the cache exists to avoid.
    /// Skipping FINAL here is sound for this filter: versions of a row only ever gain a larger `inserted_at` (it is the ReplacingMergeTree version column), so a granule whose whole range is ≤ the watermark holds nothing the increment needs.
    /// The price is that an increment can carry several versions of one (tag, step) — unmerged duplicate inserts, or a re-log inside the window — and `merge_increment_tracked` keeps the latest by `inserted_ms`.
    /// It must also carry nonnumeric payloads: a newer CDN/text row at the same ReplacingMergeTree key is a tombstone for a cached numeric point.
    async fn fetch_increment(
        &self,
        project_id: &str,
        run_id: &str,
        metric_name: &str,
        after_inserted_ms: i64,
    ) -> Result<(Instant, Vec<VersionedRawPoint>)> {
        let started = Instant::now();
        let rows = self
            .client
            .query(
                "SELECT tag, step, timestamp_ms,
                        ifNull(value, toFloat32(0)) AS value,
                        toUInt8(metrics.value IS NOT NULL) AS is_value,
                        toUnixTimestamp64Milli(inserted_at) AS inserted_ms
                 FROM mkdb2.metrics
                 WHERE project_id = ? AND run_id = ? AND metric_name = ?
                   AND inserted_at > fromUnixTimestamp64Milli(?)
                 ORDER BY tag, step",
            )
            .bind(project_id)
            .bind(run_id)
            .bind(metric_name)
            .bind(after_inserted_ms)
            .fetch_all::<VersionedRawPoint>()
            .await?;
        Ok((started, rows))
    }

    /// Retain a full read; under cache ablation (`KYMO_SERIES_CACHE=0`) it is only shared with the requests already waiting for it.
    fn store_full(
        &self,
        key: SeriesKey,
        rows: Vec<VersionedRawPoint>,
        fetch_started: Instant,
        origin: LineageOrigin,
    ) -> Arc<SeriesSnapshot> {
        if self.series_cache_enabled {
            self.series_cache
                .insert_full_with_origin(key, rows, fetch_started, origin)
        } else {
            Arc::new(SeriesSnapshot::full_with_origin(rows, origin))
        }
    }

    /// Elect this request as the full-read refresher of up to `limit` of `keys`, one project and metric (see [`SeriesRefreshLocks::elect_misses`]).
    pub(crate) fn elect_full_reads(&self, keys: &[SeriesKey], limit: usize) -> Vec<ElectedMiss> {
        self.series_refresh_locks
            .elect_misses(&self.series_cache, keys, limit)
    }

    /// Refresh `elected` (nonempty, one project and metric) from a single full read shared by one detached task per key (see [`SeriesRefreshLocks::spawn_batch`]).
    /// `ctx` backs the read with one admission unit and run guard per key; every task holds all of them, so a lifecycle writer on any of these runs waits for the whole batch.
    pub(crate) fn spawn_full_read_batch(
        &self,
        elected: Vec<ElectedMiss>,
        ctx: Vec<RefreshDetach>,
    ) -> impl std::future::Future<Output = Result<Vec<(SeriesKey, Arc<SeriesSnapshot>)>, RefreshError>>
    {
        use futures::FutureExt;
        let project_id = elected[0].key.project_id.clone();
        let metric_name = elected[0].key.metric_name.clone();
        let run_ids: Vec<String> = elected.iter().map(|miss| miss.key.run_id.clone()).collect();
        let ch = self.clone();
        // Shared needs a Clone output: each run's rows sit in their own slot, moved out once by that run's task, which stores its own series.
        let read = {
            let ch = ch.clone();
            async move {
                let (fetch_started, series) = ch
                    .fetch_full_many(&project_id, &metric_name, &run_ids)
                    .await
                    .map_err(|e| format!("{e:#}"))?;
                Ok::<_, String>((
                    fetch_started,
                    Arc::new(
                        series
                            .into_iter()
                            .map(|rows| std::sync::Mutex::new(Some(rows)))
                            .collect::<Vec<_>>(),
                    ),
                ))
            }
        }
        .shared();
        self.series_refresh_locks
            .spawn_batch(elected, Arc::new(ctx), move |index, key| {
                let (read, ch, key) = (read.clone(), ch.clone(), key.clone());
                async move {
                    let (fetch_started, series) = read.await.map_err(anyhow::Error::msg)?;
                    let rows = series[index]
                        .lock()
                        .unwrap()
                        .take()
                        .expect("each run's rows are taken once, by its own task");
                    Ok(ch.store_full(key, rows, fetch_started, LineageOrigin::Miss))
                }
            })
    }

    // --- CDN queries ---

    /// Batched CDN-key lookup. One ClickHouse query (single FINAL scan)
    /// covers every (project_id, run_id, metric_name) tuple in `refs`. Rows
    /// come back interleaved; the caller partitions them back into per-ref
    /// buckets.
    /// `use_skip_indexes_if_final = 0` keeps `idx_cdn_key` out of the FINAL read, where pruning by it could hide the newer row that supersedes a media row.
    /// That is 25.3's default, pinned so a profile or version change can't flip it.
    pub async fn query_cdn_keys_batch(
        &self,
        refs: &[(String, String, String)],
        step_min: i64,
        step_max: i64,
    ) -> Result<Vec<CdnKeyBatchRow>> {
        if refs.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = refs
            .iter()
            .map(|_| "(?, ?, ?)")
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT project_id, run_id, metric_name, step,
                    argMax(cdn_key, tuple(is_versioned, mutation_version, inserted_at)) AS cdn_key
             FROM (
                 SELECT project_id, run_id, metric_name, tag, step,
                        assumeNotNull(cdn_key) AS cdn_key,
                        toUInt8(0) AS is_versioned,
                        toUInt64(0) AS mutation_version,
                        inserted_at
                 FROM mkdb2.metrics FINAL
                 WHERE (project_id, run_id, metric_name) IN ({placeholders})
                   AND step >= ? AND step <= ? AND metrics.cdn_key IS NOT NULL
                 UNION ALL
                 SELECT project_id, run_id, metric_name, tag, step, cdn_key,
                        toUInt8(1) AS is_versioned, mutation_version, inserted_at
                 FROM mkdb2.rich_metrics FINAL
                 WHERE (project_id, run_id, metric_name) IN ({placeholders})
                   AND step >= ? AND step <= ?
             )
             GROUP BY project_id, run_id, metric_name, tag, step
             ORDER BY project_id, run_id, metric_name, step, tag
             SETTINGS use_skip_indexes_if_final = 0"
        );
        let mut q = self.client.query(&sql);
        for (p, r, m) in refs {
            q = q.bind(p).bind(r).bind(m);
        }
        q = q.bind(step_min).bind(step_max);
        for (p, r, m) in refs {
            q = q.bind(p).bind(r).bind(m);
        }
        q = q.bind(step_min).bind(step_max);
        let rows = q.fetch_all::<CdnKeyBatchRow>().await?;
        Ok(rows)
    }

    // --- Text stream queries ---

    /// Returns its start like `fetch_full_many`, the rows, and for a full read the frontier from its later max query (0 for an increment).
    async fn fetch_text_index_rows(
        &self,
        project_id: &str,
        run_id: &str,
        metric_names: &[String],
        after_inserted_ms: Option<i64>,
    ) -> Result<(Instant, Vec<TextIndexRow>, i64)> {
        let started = Instant::now();
        let placeholders = metric_names
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");
        let incremental = after_inserted_ms.is_some();
        let final_clause = if incremental { "" } else { " FINAL" };
        let inserted_filter = if incremental {
            " AND inserted_at > fromUnixTimestamp64Milli(?)"
        } else {
            ""
        };
        // A full FINAL read is authoritative, so a current non-text row is
        // correctly represented by absence. Incremental reads must retain
        // non-text replacements as tombstones for cached text rows.
        let text_filter = if incremental {
            ""
        } else {
            " AND text_data IS NOT NULL"
        };
        let sql = format!(
            "WITH replaceAll(replaceAll(ifNull(text_data, ''),
                                        '\\r\\n', '\\n'),
                             '\\r', '\\n') AS normalized_text
             SELECT step, metric_name, tag,
                    toUInt8(text_data IS NOT NULL) AS is_text,
                    countSubstrings(normalized_text, '\\n') AS separator_count,
                    notEmpty(normalized_text) AS non_empty,
                    endsWith(normalized_text, '\\n') AS ends_with_newline,
                    length(normalized_text) AS normalized_bytes,
                    toUnixTimestamp64Milli(inserted_at) AS inserted_ms
             FROM mkdb2.metrics{final_clause}
             WHERE project_id = ? AND run_id = ?
               AND metric_name IN ({placeholders}){text_filter}{inserted_filter}
             ORDER BY step, metric_name, tag, inserted_at"
        );
        let mut query = self.client.query(&sql).bind(project_id).bind(run_id);
        for metric_name in metric_names {
            query = query.bind(metric_name);
        }
        if let Some(watermark) = after_inserted_ms {
            query = query.bind(watermark);
        }
        let rows = query.fetch_all::<TextIndexRow>().await?;
        if incremental {
            return Ok((started, rows, 0));
        }

        // The full row query deliberately excludes current non-text values.
        // Keep the incremental frontier at the newest selected row of any
        // payload type, otherwise an all-numeric metric would cache an empty
        // index at epoch zero and its next refresh would transfer its entire
        // history as tombstones. Older ReplacingMergeTree versions cannot
        // raise this maximum, so FINAL is unnecessary here.
        let watermark_sql = format!(
            "SELECT ifNull(maxOrNull(toUnixTimestamp64Milli(inserted_at)), toInt64(0)) AS val
             FROM mkdb2.metrics
             WHERE project_id = ? AND run_id = ?
               AND metric_name IN ({placeholders})"
        );
        let mut watermark_query = self
            .client
            .query(&watermark_sql)
            .bind(project_id)
            .bind(run_id);
        for metric_name in metric_names {
            watermark_query = watermark_query.bind(metric_name);
        }
        let observed_max_inserted_ms = watermark_query.fetch_one::<SingleI64>().await?.val;
        Ok((started, rows, observed_max_inserted_ms))
    }

    async fn text_stream_index(
        &self,
        project_id: &str,
        run_id: &str,
        metric_names: &[String],
    ) -> Result<std::sync::Arc<TextStreamIndex>> {
        let key = TextIndexKey::new(project_id, run_id, metric_names);
        if let TextIndexLookup::Fresh(index) = self
            .text_index_cache
            .lookup(&key, self.series_cache.last_bump(run_id))
        {
            return Ok(index);
        }

        // Only one request per exact stream key may refresh at a time. Every
        // follower repeats both the cache lookup and bump-gate check after it
        // acquires the lock: a bump can arrive while the leader is reading,
        // in which case the leader's result must not satisfy that follower.
        let refresh_lease = self.text_refresh_locks.lease_for(&key);
        let _refresh_guard = refresh_lease.lock().await;
        match self
            .text_index_cache
            .lookup(&key, self.series_cache.last_bump(run_id))
        {
            TextIndexLookup::Fresh(index) => return Ok(index),
            TextIndexLookup::Stale {
                watermark_ms,
                generation,
            } => {
                let (fetch_started, increment, _) = self
                    .fetch_text_index_rows(project_id, run_id, metric_names, Some(watermark_ms))
                    .await?;
                if let Ok(index) = self.text_index_cache.apply_increment(
                    &key,
                    increment,
                    fetch_started,
                    generation,
                ) {
                    return Ok(index);
                }
            }
            TextIndexLookup::Miss => {}
        }
        // Miss, or an increment the cache refused: another request replaced the base while it was in flight (merging would use that old generation's watermark), or it holds equal-version rows only FINAL can order.
        let (fetch_started, rows, observed_max_inserted_ms) = self
            .fetch_text_index_rows(project_id, run_id, metric_names, None)
            .await?;
        Ok(self
            .text_index_cache
            .insert_full(key, rows, observed_max_inserted_ms, fetch_started))
    }

    /// Return one bounded, line-oriented slice across the requested text
    /// metrics. Ordinary windows reuse a small per-chunk line index and fetch
    /// text only for chunks intersecting the requested range. Index refreshes
    /// read only recently inserted rows through ClickHouse's inserted_at skip
    /// index. Searches necessarily scan the full stream, but split it only
    /// after reconstructing chunk boundaries.
    pub async fn query_text_window(
        &self,
        project_id: &str,
        run_id: &str,
        metric_names: &[String],
        line_offset: u64,
        line_limit: u32,
        search: &str,
    ) -> Result<TextWindow> {
        if metric_names.is_empty() {
            return Ok(TextWindow {
                first_step: 0,
                total_lines: 0,
                lines: Vec::new(),
            });
        }

        let placeholders = metric_names
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");

        if !search.is_empty() {
            let sql = format!(
                "SELECT step, metric_name,
                        replaceAll(replaceAll(assumeNotNull(text_data),
                                              '\\r\\n', '\\n'),
                                   '\\r', '\\n') AS text
                 FROM mkdb2.metrics FINAL
                 WHERE project_id = ? AND run_id = ?
                   AND metric_name IN ({placeholders})
                   AND text_data IS NOT NULL
                 ORDER BY step, metric_name, tag"
            );
            let mut q = self.client.query(&sql).bind(project_id).bind(run_id);
            for metric_name in metric_names {
                q = q.bind(metric_name);
            }
            let mut cursor = q.fetch::<TextSearchChunkRow>()?;
            let mut search_window =
                TextSearchCollector::new(search, line_offset, line_limit, MAX_TEXT_WINDOW_BYTES);
            while let Some(chunk) = cursor.next().await? {
                search_window.push_chunk(chunk)?;
            }
            return search_window.finish();
        }

        let index = self
            .text_stream_index(project_id, run_id, metric_names)
            .await?;
        if line_offset >= index.total_lines {
            return Ok(TextWindow {
                first_step: index.first_step,
                total_lines: index.total_lines,
                lines: Vec::new(),
            });
        }

        let selected = index.window_chunks(line_offset, line_limit);
        let Some((first, last)) = selected.first().zip(selected.last()) else {
            return Ok(TextWindow {
                first_step: index.first_step,
                total_lines: index.total_lines,
                lines: Vec::new(),
            });
        };
        // This is a cheap rejection before any text payload is read. The
        // subsequent cursor enforces the same bound against the actual rows,
        // because the index and payload SELECTs are intentionally non-atomic.
        selected.iter().try_fold(0u64, |total, chunk| {
            add_text_window_bytes(total, indexed_chunk_working_bytes(chunk))
        })?;
        let chunks_sql = format!(
            "SELECT step, metric_name, tag,
                    replaceAll(replaceAll(assumeNotNull(text_data),
                                          '\\r\\n', '\\n'),
                               '\\r', '\\n') AS text
             FROM mkdb2.metrics FINAL
             PREWHERE project_id = ? AND run_id = ?
               AND metric_name IN ({placeholders})
               AND tuple(step, metric_name, tag) >= tuple(?, ?, ?)
               AND tuple(step, metric_name, tag) <= tuple(?, ?, ?)
             WHERE text_data IS NOT NULL
             ORDER BY step, metric_name, tag"
        );
        let mut chunks_query = self.client.query(&chunks_sql).bind(project_id).bind(run_id);
        for metric_name in metric_names {
            chunks_query = chunks_query.bind(metric_name);
        }
        let mut chunk_cursor = chunks_query
            .bind(first.step)
            .bind(&first.metric_name)
            .bind(&first.tag)
            .bind(last.step)
            .bind(&last.metric_name)
            .bind(&last.tag)
            .fetch::<TextChunkDataRow>()?;
        let mut chunk_rows = Vec::with_capacity(selected.len());
        let mut actual_bytes = 0u64;
        while let Some(chunk) = chunk_cursor.next().await? {
            actual_bytes =
                add_text_window_bytes(actual_bytes, text_chunk_row_working_bytes(&chunk))?;
            chunk_rows.push(chunk);
        }
        let (chunks, trailing_gap) = attach_line_offsets(chunk_rows, selected);

        Ok(TextWindow {
            first_step: index.first_step,
            total_lines: index.total_lines,
            lines: text_lines_from_chunks(
                chunks,
                trailing_gap,
                line_offset,
                line_limit,
                index.total_lines,
                MAX_TEXT_WINDOW_BYTES,
            )?,
        })
    }

    // --- CDN garbage collection (docs/cdn-gcs-migration.md § Garbage collection) ---

    /// The collector's tables. `cdn_acks` holds dedup acks for twice the grace, so the grace can grow without an ALTER; `''` rows never expire: the newest is the log's start time, which delete mode waits on, and boot writes one only if none exists.
    pub async fn ensure_cdn_gc_schema(&self, grace_days: u64, now: u32) -> Result<()> {
        let ttl_days = 2 * grace_days;
        self.client
            .query(&format!(
                "CREATE TABLE IF NOT EXISTS {CDN_ACKS_TABLE} (
                    key      String,
                    acked_at DateTime
                ) ENGINE = ReplacingMergeTree(acked_at)
                ORDER BY key
                TTL acked_at + INTERVAL {ttl_days} DAY DELETE WHERE key != ''"
            ))
            .execute()
            .await
            .context("creating the CDN ack log")?;
        if self.cdn_ack_log_start().await?.is_none() {
            self.record_cdn_ack("", now).await?;
        }
        // One row per parsed root and links version, holding all its children (empty for a leaf). Keys are content addresses, so a root is fetched again only under a new links version. The version is in the sort key so rows of different versions never collapse into each other: an image rolled back across a bump must still find its own.
        self.client
            .query(&format!(
                "CREATE TABLE IF NOT EXISTS {CDN_MANIFEST_CHILDREN_TABLE} (
                    parent        String,
                    links_version UInt32,
                    children      Array(String)
                ) ENGINE = ReplacingMergeTree
                ORDER BY (parent, links_version)"
            ))
            .execute()
            .await
            .context("creating the CDN manifest-children cache")?;
        // Per-pass working set, truncated at each pass start. `kind` is `inventory` (the bucket listing), `ref` (referenced keys), `unparsed` (roots awaiting a manifest parse), or `candidate` (deletable keys).
        self.client
            .query(&format!(
                "CREATE TABLE IF NOT EXISTS {CDN_GC_SCRATCH_TABLE} (
                    kind    LowCardinality(String),
                    key     String,
                    size    UInt64,
                    created DateTime
                ) ENGINE = MergeTree
                ORDER BY (kind, key)"
            ))
            .execute()
            .await
            .context("creating the CDN collector's scratch table")?;
        Ok(())
    }

    pub async fn record_cdn_ack(&self, key: &str, acked_at: u32) -> Result<()> {
        let mut insert = self
            .client
            .insert::<CdnAckRow>(CDN_ACKS_TABLE)?
            .with_timeouts(Some(CDN_ACK_TIMEOUT), Some(CDN_ACK_TIMEOUT));
        insert
            .write(&CdnAckRow {
                key: key.to_owned(),
                acked_at,
            })
            .await?;
        insert.end().await.context("recording a CDN dedup ack")
    }

    /// When the ack log started (unix seconds): the newest `''` row, which is also the one a merge keeps.
    pub async fn cdn_ack_log_start(&self) -> Result<Option<u32>> {
        self.client
            .query(&format!(
                "SELECT toUInt32(acked_at) FROM {CDN_ACKS_TABLE} WHERE key = ''
                 ORDER BY acked_at DESC LIMIT 1"
            ))
            .fetch_optional::<u32>()
            .await
            .context("reading the CDN ack log start")
    }

    /// A client for the GC's heavy statements, carrying [`CDN_GC_SETTINGS`].
    fn gc_client(&self) -> Client {
        CDN_GC_SETTINGS
            .into_iter()
            .fold(self.client.clone(), |client, (name, value)| {
                client.with_option(name, value)
            })
    }

    pub async fn cdn_gc_reset(&self) -> Result<()> {
        self.gc_client()
            .query(&format!("TRUNCATE TABLE {CDN_GC_SCRATCH_TABLE}"))
            .execute()
            .await
            .context("truncating the CDN collector's scratch table")
    }

    async fn cdn_gc_insert<T: clickhouse::Row + Serialize>(
        &self,
        table: &str,
        rows: &[T],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut insert = self.gc_client().insert::<T>(table)?;
        for row in rows {
            insert.write(row).await?;
        }
        Ok(insert.end().await?)
    }

    pub async fn cdn_gc_insert_inventory(&self, rows: &[CdnInventoryRow]) -> Result<()> {
        self.cdn_gc_insert(CDN_GC_SCRATCH_TABLE, rows)
            .await
            .context("inserting the CDN inventory")
    }

    /// Referenced roots: every physical row's key in both metric tables — superseded and trashed-but-unpurged rows included — restricted to the hosted key grammar.
    pub async fn cdn_gc_collect_roots(&self, key_pattern: &str) -> Result<()> {
        self.gc_client()
            .query(&format!(
                "INSERT INTO {CDN_GC_SCRATCH_TABLE} (kind, key) {}",
                cdn_gc_roots_select()
            ))
            .bind(key_pattern)
            .execute()
            .await
            .context("collecting referenced CDN roots")
    }

    /// Stored roots with no children row under this links version, for the manifest parse.
    pub async fn cdn_gc_collect_unparsed(&self, links_version: u32) -> Result<()> {
        self.gc_client()
            .query(&format!(
                "INSERT INTO {CDN_GC_SCRATCH_TABLE} (kind, key, size)
                 SELECT 'unparsed', key, size FROM {CDN_GC_SCRATCH_TABLE}
                 WHERE kind = 'inventory'
                   AND key IN (SELECT key FROM {CDN_GC_SCRATCH_TABLE} WHERE kind = 'ref')
                   AND key NOT IN (SELECT parent FROM {CDN_MANIFEST_CHILDREN_TABLE}
                                   WHERE links_version = ?)"
            ))
            .bind(links_version)
            .execute()
            .await
            .context("collecting unparsed CDN roots")
    }

    pub async fn cdn_gc_insert_manifests(&self, rows: &[CdnManifestRow]) -> Result<()> {
        self.cdn_gc_insert(CDN_MANIFEST_CHILDREN_TABLE, rows)
            .await
            .context("caching CDN manifest children")
    }

    /// Adds the roots' manifest children to the referenced set (one level: nothing reads a child as a manifest).
    pub async fn cdn_gc_collect_children(&self, links_version: u32) -> Result<()> {
        self.gc_client()
            .query(&format!(
                "INSERT INTO {CDN_GC_SCRATCH_TABLE} (kind, key)
                 SELECT 'ref', arrayJoin(children) FROM {CDN_MANIFEST_CHILDREN_TABLE}
                 WHERE links_version = ?
                   AND parent IN (SELECT key FROM {CDN_GC_SCRATCH_TABLE} WHERE kind = 'ref')"
            ))
            .bind(links_version)
            .execute()
            .await
            .context("collecting referenced CDN manifest children")
    }

    /// Deletable objects: unreferenced, and neither created nor dedup-acked since `cutoff`.
    pub async fn cdn_gc_collect_candidates(&self, cutoff: u32) -> Result<()> {
        self.gc_client()
            .query(&format!(
                "INSERT INTO {CDN_GC_SCRATCH_TABLE} (kind, key, size)
                 SELECT 'candidate', key, size FROM {CDN_GC_SCRATCH_TABLE}
                 WHERE kind = 'inventory'
                   AND created < toDateTime(?)
                   AND key NOT IN (SELECT key FROM {CDN_GC_SCRATCH_TABLE} WHERE kind = 'ref')
                   AND key NOT IN (SELECT key FROM {CDN_ACKS_TABLE} WHERE acked_at >= toDateTime(?))"
            ))
            .bind(cutoff)
            .bind(cutoff)
            .execute()
            .await
            .context("collecting CDN deletion candidates")
    }

    pub async fn cdn_gc_report(&self) -> Result<CdnGcReport> {
        self.gc_client()
            .query(&format!(
                "SELECT
                     countIf(referenced) AS referenced_objects,
                     sumIf(size, referenced) AS referenced_bytes,
                     -- Scalar subqueries are Nullable, which would shift the positional decode.
                     ifNull((SELECT count() FROM {CDN_GC_SCRATCH_TABLE} WHERE kind = 'candidate'), 0)
                         AS candidate_objects,
                     ifNull((SELECT sum(size) FROM {CDN_GC_SCRATCH_TABLE} WHERE kind = 'candidate'), 0)
                         AS candidate_bytes,
                     ifNull((SELECT uniqExact(key) FROM {CDN_GC_SCRATCH_TABLE} WHERE kind = 'ref'), 0)
                         AS references
                 FROM (
                     SELECT size,
                            key IN (SELECT key FROM {CDN_GC_SCRATCH_TABLE} WHERE kind = 'ref')
                                AS referenced
                     FROM {CDN_GC_SCRATCH_TABLE}
                     WHERE kind = 'inventory'
                 )"
            ))
            .fetch_one::<CdnGcReport>()
            .await
            .context("summarizing the CDN collector's pass")
    }

    pub async fn cdn_gc_count(&self, kind: &str) -> Result<u64> {
        self.gc_client()
            .query(&format!(
                "SELECT count() FROM {CDN_GC_SCRATCH_TABLE} WHERE kind = ?"
            ))
            .bind(kind)
            .fetch_one::<u64>()
            .await
            .with_context(|| format!("counting the CDN collector's {kind} keys"))
    }

    /// One keyset page of a scratch kind, in key order.
    pub async fn cdn_gc_page(
        &self,
        kind: &str,
        after: &str,
        limit: u64,
    ) -> Result<Vec<CdnGcKeySize>> {
        self.gc_client()
            .query(&format!(
                "SELECT key, size FROM {CDN_GC_SCRATCH_TABLE}
                 WHERE kind = ? AND key > ?
                 ORDER BY key
                 LIMIT ?"
            ))
            .bind(kind)
            .bind(after)
            .bind(limit)
            .fetch_all::<CdnGcKeySize>()
            .await
            .with_context(|| format!("paging the CDN collector's {kind} keys"))
    }

    /// Referenced keys missing from the bucket, the first `limit` in key order.
    pub async fn cdn_gc_dangling(&self, limit: u64) -> Result<Vec<String>> {
        self.gc_client()
            .query(&format!(
                "SELECT DISTINCT key FROM {CDN_GC_SCRATCH_TABLE}
                 WHERE kind = 'ref'
                   AND key NOT IN (SELECT key FROM {CDN_GC_SCRATCH_TABLE} WHERE kind = 'inventory')
                 ORDER BY key
                 LIMIT ?"
            ))
            .bind(limit)
            .fetch_all::<String>()
            .await
            .context("sampling dangling CDN references")
    }

    /// Which of `keys` were dedup-acked since `cutoff`.
    pub async fn cdn_gc_acked_since(
        &self,
        keys: &[String],
        cutoff: u32,
    ) -> Result<std::collections::HashSet<String>> {
        if keys.is_empty() {
            return Ok(Default::default());
        }
        let placeholders = vec!["?"; keys.len()].join(", ");
        let mut query = self.gc_client().query(&format!(
            "SELECT DISTINCT key FROM {CDN_ACKS_TABLE}
             WHERE key IN ({placeholders}) AND acked_at >= toDateTime(?)"
        ));
        for key in keys {
            query = query.bind(key);
        }
        Ok(query
            .bind(cutoff)
            .fetch_all::<String>()
            .await
            .context("rechecking CDN dedup acks")?
            .into_iter()
            .collect())
    }
}

/// Every reader takes a result as complete and current, so no server profile may shorten one or answer from an older snapshot: the caches would store a partial series as whole, under a watermark above the missing rows, and the CDN collector would delete reachable objects. Every overflow mode throws instead of `break`, `limit` and `offset` are 0, and the query cache is off (ClickHouse's defaults). Every client carries these (`ChClient::configured`).
pub(crate) const COMPLETE_READ_SETTINGS: [(&str, &str); 14] = [
    ("read_overflow_mode", "throw"),
    ("read_overflow_mode_leaf", "throw"),
    ("set_overflow_mode", "throw"),
    ("join_overflow_mode", "throw"),
    ("transfer_overflow_mode", "throw"),
    ("group_by_overflow_mode", "throw"),
    ("distinct_overflow_mode", "throw"),
    ("sort_overflow_mode", "throw"),
    ("result_overflow_mode", "throw"),
    ("timeout_overflow_mode", "throw"),
    ("timeout_overflow_mode_leaf", "throw"),
    ("limit", "0"),
    ("offset", "0"),
    ("use_query_cache", "0"),
];

/// The GC's heavy statements, on top of [`COMPLETE_READ_SETTINGS`]: a memory cap fails the statement rather than the server (a ClickHouse OOM takes concurrent ingest with it), and two threads keep full scans off the dashboards' cores. The scratch table's sort key would make each `IN` set be built twice, once more for index analysis that prunes nothing here, which doubles set memory. Sync inserts: scratch batches are large, one part each.
pub(crate) const CDN_GC_SETTINGS: [(&str, &str); 4] = [
    ("use_index_for_in_with_subqueries", "0"),
    ("async_insert", "0"),
    ("max_memory_usage", "4294967296"), // 4 GiB
    ("max_threads", "2"),
];

/// The CDN collector's referenced-roots SELECT; `?` is the hosted key pattern. `idx_cdn_key` limits its metrics half to granules holding a non-NULL `cdn_key`, and ClickHouse reads a part without the index whole, so the key set is the same with or without it. Deliberately no `force_data_skipping_indices`: ClickHouse 25.3 accepts it while no part has the index materialized, so it cannot catch the unmaterialized case, and it fails the statement once the index is missing from the table definition, which would block every pass over an optimization.
pub(crate) fn cdn_gc_roots_select() -> String {
    format!(
        "SELECT 'ref', key FROM (
             SELECT assumeNotNull(cdn_key) AS key FROM {METRICS_TABLE}
             WHERE cdn_key IS NOT NULL
             UNION ALL
             SELECT cdn_key AS key FROM {RICH_METRICS_TABLE}
         )
         WHERE match(key, ?)
         GROUP BY key"
    )
}

fn validate_local_url(url: &str) -> Result<()> {
    let uri: http::Uri = url
        .parse()
        .with_context(|| format!("invalid local CLICKHOUSE_URL {url:?}"))?;
    ensure!(
        uri.scheme_str() == Some("https"),
        "local CLICKHOUSE_URL must use HTTPS"
    );
    let authority = uri
        .authority()
        .context("local CLICKHOUSE_URL must include an authority")?;
    let port = authority
        .port_u16()
        .context("local CLICKHOUSE_URL must include an explicit port")?;
    ensure!(
        port >= 1024,
        "local CLICKHOUSE_URL must use a non-system port"
    );
    ensure!(
        authority.as_str() == format!("localhost:{port}"),
        "local CLICKHOUSE_URL must use the exact host localhost"
    );
    ensure!(
        uri.path() == "/" && uri.query().is_none(),
        "local CLICKHOUSE_URL must not include a path or query"
    );
    Ok(())
}

#[cfg(test)]
mod local_connector_tests {
    use std::os::unix::fs::PermissionsExt;

    use rustls::client::danger::ServerCertVerifier;
    use rustls::client::WebPkiServerVerifier;
    use rustls::pki_types::{ServerName, UnixTime};

    use super::*;

    // webpki verifies DNS subjectAltName and never falls back to the certificate's common name. Keep this fixture representative of the launcher-generated leaf.
    const TEST_CERTIFICATE_PEM: &str = "-----BEGIN CERTIFICATE-----\n\
MIIDQzCCAiugAwIBAgIUViTqSAvMzEmlNBWlTr+NjIeye8MwDQYJKoZIhvcNAQEL\n\
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDgxMjIyMjE1MFoXDTM2MDgw\n\
OTIyMjE1MFowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF\n\
AAOCAQ8AMIIBCgKCAQEAxr36rSCmsZxVdOtP3o5PTzoBQ87bwZ9Kz/cUypcVMVbc\n\
Zi6HVlJASQpiftmTghBLK7M3nYxEmtQhz/Ara4L+tkllITlYaBOPQEVzbjualGiD\n\
hABSnM7G/eNEpmQ43X1iUO+E84P9Z+7AEWS4mHfYiqAiEmU6B7LelAIeMRbpYh6U\n\
IB2I2GZjuyuAuQZU5aJGPvFJhlhFRbxab8tf5VTAio6V4MRmLAyZp/xEX516BtvL\n\
RQh4OtWMVzorBugECcE/OaRIWOg1tyn5aojrSc+LNlzwE7yKwvTug5mb7ItDvZWU\n\
VvOgJP2Li1mJihnpL5meFPyr9XH5DsH2aIwX5nprFwIDAQABo4GMMIGJMB0GA1Ud\n\
DgQWBBRjDkC3MptAHu6I36FW/U57moifJTAfBgNVHSMEGDAWgBRjDkC3MptAHu6I\n\
36FW/U57moifJTAUBgNVHREEDTALgglsb2NhbGhvc3QwDAYDVR0TAQH/BAIwADAO\n\
BgNVHQ8BAf8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUHAwEwDQYJKoZIhvcNAQEL\n\
BQADggEBAMZvAXJMEBWB/QBE0r5qmgzl4683VGfVCb5pMrKeXT5Xc2dbQg+lCAr4\n\
7Jl8rYdsJag7pUcsyYvspUDKXn24kHbiKazLeVlO6qNcZKdpW8qGN1vbdx47Ego7\n\
OJ5Xph5E1ECAGNL4ArTKbzTzmJkDnUHhvf2ec8CDmtLZYxFJ9OcQLYkCUf4BxDFH\n\
BN8XW96vFT2DbW1+rgB4Xj4u7urghK+TMTqWEXIRdS9xpLNwM0B16WDr7lh3s47y\n\
L2DIPh06Un2GEmqMID/bXlccPm2XbOXAUW7qmDgMHsgeFZK1aCFFIilrq2DHYSDF\n\
7DxV9x1WZpqvNUMA3L1qYf2HfRaQqSo=\n\
-----END CERTIFICATE-----\n";

    fn private_certificate() -> (tempfile::TempDir, std::path::PathBuf) {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = root.join("clickhouse.pem");
        std::fs::write(&path, TEST_CERTIFICATE_PEM).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        (temporary, path)
    }

    #[test]
    fn local_connector_requires_exact_https_endpoint_and_credentials() {
        let (_temporary, certificate) = private_certificate();
        ChClient::new_local("https://localhost:18123", &certificate, "mkdb2", "secret").unwrap();

        let certificate_der = CertificateDer::from_pem_slice(TEST_CERTIFICATE_PEM.as_bytes())
            .expect("parse SAN-bearing test certificate");
        let mut roots = RootCertStore::empty();
        roots.add(certificate_der.clone()).unwrap();
        let verifier = WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()
        .unwrap();
        verifier
            .verify_server_cert(
                &certificate_der,
                &[],
                &ServerName::try_from("localhost").unwrap(),
                &[],
                UnixTime::now(),
            )
            .expect("fixture must verify for its DNS:localhost SAN");
        assert!(verifier
            .verify_server_cert(
                &certificate_der,
                &[],
                &ServerName::try_from("127.0.0.1").unwrap(),
                &[],
                UnixTime::now(),
            )
            .is_err());

        for url in [
            "http://localhost:18123",
            "https://127.0.0.1:18123",
            "https://LOCALHOST:18123",
            "https://localhost",
            "https://localhost:443",
            "https://localhost:18123/query",
            "https://localhost:18123/?query=SELECT%201",
        ] {
            assert!(ChClient::new_local(url, &certificate, "mkdb2", "secret",).is_err());
        }
        assert!(
            ChClient::new_local("https://localhost:18123", &certificate, "mkdb2", "",).is_err()
        );
    }

    #[test]
    fn local_connector_requires_one_private_certificate() {
        let (_temporary, certificate) = private_certificate();
        std::fs::set_permissions(&certificate, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(
            ChClient::new_local("https://localhost:18123", &certificate, "mkdb2", "secret",)
                .is_err()
        );

        std::fs::set_permissions(&certificate, std::fs::Permissions::from_mode(0o600)).unwrap();
        let pem = std::fs::read_to_string(&certificate).unwrap();
        std::fs::write(&certificate, format!("{pem}{pem}")).unwrap();
        assert!(
            ChClient::new_local("https://localhost:18123", &certificate, "mkdb2", "secret",)
                .is_err()
        );
    }
}

struct TextSearchCollector {
    needle: String,
    offset: u64,
    limit: usize,
    first_step: Option<i64>,
    matching_lines: u64,
    pending: String,
    tail_source: Option<(i64, String)>,
    lines: Vec<TextWindowLine>,
    retained_bytes: u64,
    byte_limit: u64,
}

impl TextSearchCollector {
    fn new(search: &str, offset: u64, limit: u32, byte_limit: u64) -> Self {
        Self {
            needle: search.to_lowercase(),
            offset,
            limit: limit as usize,
            first_step: None,
            matching_lines: 0,
            pending: String::new(),
            tail_source: None,
            lines: Vec::with_capacity(limit as usize),
            retained_bytes: 0,
            byte_limit,
        }
    }

    fn append_pending(&mut self, text: &str) -> Result<()> {
        let next = (self.pending.len() as u64)
            .checked_add(text.len() as u64)
            .ok_or_else(text_window_limit_error)?;
        if self
            .retained_bytes
            .checked_add(next)
            .is_none_or(|total| total > self.byte_limit)
        {
            return Err(text_window_limit_error());
        }
        self.pending.push_str(text);
        Ok(())
    }

    fn push_chunk(&mut self, chunk: TextSearchChunkRow) -> Result<()> {
        self.first_step.get_or_insert(chunk.step);
        if !chunk.text.is_empty() {
            self.tail_source = Some((chunk.step, chunk.metric_name.clone()));
        }
        for segment in chunk.text.split_inclusive('\n') {
            if let Some(text) = segment.strip_suffix('\n') {
                self.append_pending(text)?;
                self.complete_line(chunk.step, &chunk.metric_name)?;
            } else {
                self.append_pending(segment)?;
            }
        }
        Ok(())
    }

    fn complete_line(&mut self, step: i64, metric_name: &str) -> Result<()> {
        if self.pending.to_lowercase().contains(&self.needle) {
            let index = self.matching_lines;
            self.matching_lines = self.matching_lines.saturating_add(1);
            if index >= self.offset && self.lines.len() < self.limit {
                let line_bytes = text_line_working_bytes(&self.pending, metric_name);
                let retained_bytes = self
                    .retained_bytes
                    .checked_add(line_bytes)
                    .ok_or_else(text_window_limit_error)?;
                if retained_bytes > self.byte_limit {
                    return Err(text_window_limit_error());
                }
                self.retained_bytes = retained_bytes;
                self.lines.push(TextWindowLine {
                    step,
                    metric_name: metric_name.to_string(),
                    line_index: index,
                    text: std::mem::take(&mut self.pending),
                });
                return Ok(());
            }
        }
        self.pending.clear();
        Ok(())
    }

    fn finish(mut self) -> Result<TextWindow> {
        if !self.pending.is_empty() {
            if let Some((step, metric_name)) = self.tail_source.take() {
                self.complete_line(step, &metric_name)?;
            }
        }
        Ok(TextWindow {
            first_step: self.first_step.unwrap_or(0),
            total_lines: self.matching_lines,
            lines: self.lines,
        })
    }
}

/// Pairs payload rows with their index entries. Returns the paired rows plus a trailing-gap flag: true when a CONTENT-BEARING indexed chunk at the end of the selection has no payload row (vanished between the non-atomic reads) — the caller must not emit an unterminated tail then, because the index counted a final line whose content is gone. Vanished zero-byte entries are inert and never flag: losing one loses nothing, and flagging it would degrade an intact reconstruction (clear a valid partial, suppress a surviving tail).
fn attach_line_offsets(
    rows: Vec<TextChunkDataRow>,
    indexed: &[IndexedTextChunk],
) -> (Vec<TextChunkWindowRow>, bool) {
    let mut index_position = 0usize;
    let mut gap_pending = false;
    let mut result = Vec::with_capacity(rows.len().min(indexed.len()));
    for row in rows {
        while index_position < indexed.len() {
            let candidate = &indexed[index_position];
            let ordering = candidate
                .step
                .cmp(&row.step)
                .then_with(|| candidate.metric_name.cmp(&row.metric_name))
                .then_with(|| candidate.tag.cmp(&row.tag));
            if ordering.is_lt() {
                gap_pending |= candidate.carries_content();
                index_position += 1;
                continue;
            }
            if ordering.is_eq() {
                result.push(TextChunkWindowRow {
                    step: row.step,
                    metric_name: row.metric_name,
                    text: row.text,
                    lines_before: candidate.lines_before,
                    gap_before: gap_pending,
                });
                gap_pending = false;
                index_position += 1;
            }
            break;
        }
    }
    let trailing_gap = gap_pending
        || indexed[index_position..]
            .iter()
            .any(|candidate| candidate.carries_content());
    (result, trailing_gap)
}

fn text_lines_from_chunks(
    chunks: Vec<TextChunkWindowRow>,
    trailing_gap: bool,
    line_offset: u64,
    line_limit: u32,
    total_lines: u64,
    byte_limit: u64,
) -> Result<Vec<TextWindowLine>> {
    let Some(first) = chunks.first() else {
        return Ok(Vec::new());
    };
    let line_end = line_offset
        .saturating_add(u64::from(line_limit))
        .min(total_lines);
    let mut completed_lines = first.lines_before;
    let mut pending = String::new();
    let mut lines = Vec::with_capacity(line_limit as usize);
    let mut tail_source = None::<(i64, String)>;
    let mut retained_bytes = 0u64;

    'chunks: for chunk in chunks {
        if chunk.gap_before || chunk.lines_before != completed_lines {
            // The index and payload SELECTs are intentionally non-atomic: a chunk inside the window can vanish (or change shape) between them. The index's numbering is authoritative — re-anchor to this chunk's lines_before instead of silently renumbering everything after the gap, which would hand a paging client the same line_index twice (it advances by line_index, so mis-numbered lines re-send as duplicates its count-based accounting cannot see). Drop the dangling partial too: its true continuation is gone, and splicing it onto the next surviving chunk would fabricate a line that never existed. gap_before catches vanished zero-separator chunks the arithmetic cannot; the arithmetic catches a surviving chunk whose replaced payload changed line count (there the re-anchor can emit a duplicate index within THIS response — the price of staying in the index's frame; the next poll heals it). Residual by design: a replaced chunk that inflates up to line_end can crowd out a later indexed line for one response, and a survivor whose head vanished emits its tail-truncated content at the correct index.
            pending.clear();
            completed_lines = chunk.lines_before;
            if completed_lines >= line_end {
                break 'chunks;
            }
        }
        if !chunk.text.is_empty() {
            tail_source = Some((chunk.step, chunk.metric_name.clone()));
        }
        for segment in chunk.text.split_inclusive('\n') {
            if let Some(text) = segment.strip_suffix('\n') {
                pending.push_str(text);
                if completed_lines >= line_offset && completed_lines < line_end {
                    retained_bytes = retained_bytes
                        .checked_add(text_line_working_bytes(&pending, &chunk.metric_name))
                        .filter(|total| *total <= byte_limit)
                        .ok_or_else(text_window_limit_error)?;
                    lines.push(TextWindowLine {
                        step: chunk.step,
                        metric_name: chunk.metric_name.clone(),
                        line_index: completed_lines,
                        text: std::mem::take(&mut pending),
                    });
                } else {
                    pending.clear();
                }
                completed_lines = completed_lines.saturating_add(1);
                if completed_lines >= line_end || lines.len() >= line_limit as usize {
                    // line_end normally bounds the response by itself; the len cap only bites when a backward re-anchor re-opened the window, keeping the response within the requested limit even then
                    break 'chunks;
                }
            } else {
                pending.push_str(segment);
            }
        }
    }

    // A stream not ending in a delimiter has one final logical line. It is
    // counted by the chunk index and completed here after the last chunk.
    // Fail closed on races — emit only when `pending` is provably that line:
    // it must be the stream's LAST counted line (completed + 1 == total; a
    // deflated chunk exhausting the window mid-stream leaves completed lower,
    // and its dangling partial is a mid-stream head, not the tail), pending
    // must be non-empty (empty means the indexed final line's content
    // vanished), and no content-bearing trailing chunk may be missing. (A
    // mid-tail gap whose survivor refills pending still emits truncated
    // content at the correct index — the documented head-truncation residual
    // above.)
    if let Some((step, metric_name)) = tail_source.filter(|_| {
        completed_lines.saturating_add(1) == total_lines
            && completed_lines >= line_offset
            && completed_lines < line_end
            && !pending.is_empty()
            && !trailing_gap
            && lines.len() < line_limit as usize
    }) {
        retained_bytes
            .checked_add(text_line_working_bytes(&pending, &metric_name))
            .filter(|total| *total <= byte_limit)
            .ok_or_else(text_window_limit_error)?;
        lines.push(TextWindowLine {
            step,
            metric_name,
            line_index: completed_lines,
            text: pending,
        });
    }
    Ok(lines)
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn schema_startup_retries_only_transport_failures() {
        let network = anyhow::Error::new(clickhouse::error::Error::Network(Box::new(
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "not listening"),
        )))
        .context("creating database");
        assert!(is_retryable_schema_error(&network));
        assert!(is_retryable_schema_error(&anyhow::Error::new(
            clickhouse::error::Error::TimedOut
        )));

        assert!(!is_retryable_schema_error(&anyhow::Error::new(
            clickhouse::error::Error::BadResponse("ALTER rejected".to_string())
        )));
        assert!(!is_retryable_schema_error(&anyhow::anyhow!(
            "schema validation failed"
        )));
    }

    fn valid_columns() -> Vec<SchemaColumn> {
        [
            ("project_id", "LowCardinality(String)"),
            ("run_id", "LowCardinality(String)"),
            ("metric_name", "LowCardinality(String)"),
            ("tag", "LowCardinality(String)"),
            ("step", "Int64"),
            ("timestamp_ms", "Int64"),
            ("value", "Nullable(Float32)"),
            ("cdn_key", "Nullable(String)"),
            ("text_data", "Nullable(String)"),
            ("inserted_at", "DateTime64(3)"),
        ]
        .into_iter()
        .map(|(name, column_type)| SchemaColumn {
            name: name.to_string(),
            column_type: column_type.to_string(),
            default_kind: match name {
                "tag" | "timestamp_ms" | "inserted_at" => "DEFAULT".to_string(),
                _ => String::new(),
            },
            default_expression: match name {
                "tag" => "''".to_string(),
                "timestamp_ms" => "0".to_string(),
                "inserted_at" => "now64(3)".to_string(),
                _ => String::new(),
            },
        })
        .collect()
    }

    fn valid_table() -> SchemaTable {
        SchemaTable {
            engine: "ReplacingMergeTree".to_string(),
            engine_full: "ReplacingMergeTree(inserted_at)".to_string(),
            sorting_key: "project_id, run_id, metric_name, tag, step".to_string(),
            partition_key: "project_id".to_string(),
        }
    }

    fn valid_rich_columns() -> Vec<SchemaColumn> {
        [
            ("project_id", "LowCardinality(String)"),
            ("run_id", "LowCardinality(String)"),
            ("metric_name", "LowCardinality(String)"),
            ("tag", "LowCardinality(String)"),
            ("step", "Int64"),
            ("timestamp_ms", "Int64"),
            ("cdn_key", "String"),
            ("mutation_version", "UInt64"),
            ("inserted_at", "DateTime64(3)"),
        ]
        .into_iter()
        .map(|(name, column_type)| SchemaColumn {
            name: name.to_string(),
            column_type: column_type.to_string(),
            default_kind: match name {
                "tag" | "timestamp_ms" | "inserted_at" => "DEFAULT".to_string(),
                _ => String::new(),
            },
            default_expression: match name {
                "tag" => "''".to_string(),
                "timestamp_ms" => "0".to_string(),
                "inserted_at" => "now64(3)".to_string(),
                _ => String::new(),
            },
        })
        .collect()
    }

    fn valid_rich_table() -> SchemaTable {
        SchemaTable {
            engine: "ReplacingMergeTree".to_string(),
            engine_full: "ReplacingMergeTree(mutation_version)".to_string(),
            sorting_key: "project_id, run_id, metric_name, tag, step".to_string(),
            partition_key: "project_id".to_string(),
        }
    }

    #[test]
    fn final_schema_validation_accepts_the_canonical_columns() {
        validate_metrics_schema_shape(&valid_columns(), &valid_table()).unwrap();
        validate_rich_metrics_schema_shape(&valid_rich_columns(), &valid_rich_table()).unwrap();
    }

    #[test]
    fn rich_schema_validation_rejects_arrival_order_storage() {
        let mut table = valid_rich_table();
        table.engine_full = "ReplacingMergeTree(inserted_at)".to_string();
        assert!(
            validate_rich_metrics_schema_shape(&valid_rich_columns(), &table)
                .unwrap_err()
                .to_string()
                .contains("ReplacingMergeTree(mutation_version)")
        );
    }

    #[test]
    fn final_schema_validation_rejects_missing_and_wrong_columns() {
        let mut missing = valid_columns();
        missing.retain(|column| column.name != "timestamp_ms");
        assert!(validate_metrics_schema_shape(&missing, &valid_table())
            .unwrap_err()
            .to_string()
            .contains("timestamp_ms"));

        let mut wrong = valid_columns();
        wrong
            .iter_mut()
            .find(|column| column.name == "value")
            .unwrap()
            .column_type = "Float32".to_string();
        assert!(validate_metrics_schema_shape(&wrong, &valid_table())
            .unwrap_err()
            .to_string()
            .contains("Nullable(Float32)"));
    }

    #[test]
    fn final_schema_validation_rejects_wrong_storage_semantics() {
        let mut wrong_engine = valid_table();
        wrong_engine.engine_full = "ReplacingMergeTree".to_string();
        assert!(
            validate_metrics_schema_shape(&valid_columns(), &wrong_engine)
                .unwrap_err()
                .to_string()
                .contains("ReplacingMergeTree(inserted_at)")
        );

        let mut wrong_sort = valid_table();
        wrong_sort.sorting_key = "tag".to_string();
        assert!(validate_metrics_schema_shape(&valid_columns(), &wrong_sort)
            .unwrap_err()
            .to_string()
            .contains("sorting key"));

        let mut wrong_default = valid_columns();
        wrong_default
            .iter_mut()
            .find(|column| column.name == "inserted_at")
            .unwrap()
            .default_expression = "0".to_string();
        assert!(
            validate_metrics_schema_shape(&wrong_default, &valid_table())
                .unwrap_err()
                .to_string()
                .contains("default to now64(3)")
        );
    }

    /// Correctness rides every request's settings, so a server profile can't change it: inserts acked only once committed and never streamed synchronously (the visibility margin relies on both), dependent views that fail the insert, and reads that are complete and current ([`COMPLETE_READ_SETTINGS`]).
    #[tokio::test]
    async fn every_request_pins_the_settings_correctness_depends_on() {
        use tokio::io::AsyncBufReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = ChClient::new(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let query =
            tokio::spawn(async move { client.test_client().query("SELECT 1").execute().await });
        let (socket, _) = listener.accept().await.unwrap();
        let mut request_line = String::new();
        tokio::io::BufReader::new(socket)
            .read_line(&mut request_line)
            .await
            .unwrap();
        query.abort();
        let target = request_line.split(' ').nth(1).unwrap_or_default();
        let pairs: Vec<&str> = target.split(['?', '&']).skip(1).collect();
        let max_data_size = ASYNC_INSERT_MAX_DATA_SIZE_BYTES.to_string();
        let pinned = [
            ("async_insert", "1"),
            ("wait_for_async_insert", "1"),
            ("async_insert_max_data_size", max_data_size.as_str()),
            ("materialized_views_ignore_errors", "0"),
            ("use_query_cache", "0"),
        ];
        for (name, value) in pinned.into_iter().chain(COMPLETE_READ_SETTINGS) {
            let pair = format!("{name}={value}");
            assert!(
                pairs.contains(&pair.as_str()),
                "{pair} missing: {request_line}"
            );
        }
    }

    #[tokio::test]
    async fn required_alter_failure_prevents_schema_readiness() {
        let mock = ::clickhouse::test::Mock::new();
        mock.add(::clickhouse::test::handlers::failure(
            ::clickhouse::test::status::BAD_REQUEST,
        ));
        let client = ChClient::new(mock.url()).unwrap();

        let error = client.apply_required_metrics_alters().await.unwrap_err();

        assert!(error
            .to_string()
            .contains("making mkdb2.metrics.value nullable"));
    }

    #[tokio::test]
    async fn legacy_sort_key_migration_preserves_an_existing_tag_column() {
        let mock = ::clickhouse::test::Mock::new();
        mock.add(::clickhouse::test::handlers::provide(vec![SingleString {
            val: "project_id, run_id, metric_name, step".to_string(),
        }]));
        mock.add(::clickhouse::test::handlers::provide(vec![SingleCount {
            val: 1,
        }]));
        let create = mock.add(::clickhouse::test::handlers::record_ddl());
        let copy = mock.add(::clickhouse::test::handlers::record_ddl());
        let rename = mock.add(::clickhouse::test::handlers::record_ddl());
        let drop_old = mock.add(::clickhouse::test::handlers::record_ddl());
        let client = ChClient::new(mock.url()).unwrap();

        client.migrate_to_tagged_schema().await.unwrap();

        assert!(create.query().await.contains("CREATE TABLE IF NOT EXISTS"));
        let copy_query = copy.query().await;
        assert!(copy_query.contains("metric_name, tag,"));
        assert!(!copy_query.contains("'' AS tag"));
        assert!(!copy_query.contains("metrics FINAL"));
        assert!(rename.query().await.contains("RENAME TABLE"));
        assert!(drop_old.query().await.contains("DROP TABLE"));
    }

    // The mock fails its drop on any request beyond the installed handlers, so each case also proves that ensure_cdn_key_index never issues MATERIALIZE INDEX.
    #[tokio::test]
    async fn missing_cdn_key_index_is_added_without_materializing() {
        let mock = ::clickhouse::test::Mock::new();
        mock.add(::clickhouse::test::handlers::provide(
            Vec::<SingleString>::new(),
        ));
        let add = mock.add(::clickhouse::test::handlers::record_ddl());
        let client = ChClient::new(mock.url()).unwrap();

        client.ensure_cdn_key_index().await.unwrap();

        let add = add.query().await;
        assert!(add.contains("ADD INDEX IF NOT EXISTS"));
        assert!(add.contains("idx_cdn_key cdn_key TYPE minmax GRANULARITY 1"));
    }

    #[tokio::test]
    async fn rejected_cdn_key_index_does_not_block_startup() {
        let mock = ::clickhouse::test::Mock::new();
        mock.add(::clickhouse::test::handlers::provide(
            Vec::<SingleString>::new(),
        ));
        mock.add(::clickhouse::test::handlers::failure(
            ::clickhouse::test::status::BAD_REQUEST,
        ));
        let client = ChClient::new(mock.url()).unwrap();

        client.ensure_cdn_key_index().await.unwrap();
    }

    #[tokio::test]
    async fn inserts_note_their_runs_in_the_series_cache() {
        use crate::series_cache::Lookup;
        let mock = ::clickhouse::test::Mock::new();
        let client = ChClient::new(mock.url()).unwrap();
        let cache = client.series_cache();
        let key = SeriesKey::new("p", "r", "m");
        let rows = [MetricRow {
            project_id: "p".into(),
            run_id: "r".into(),
            metric_name: "m".into(),
            tag: String::new(),
            step: 1,
            timestamp_ms: 0,
            value: Some(1.0),
            cdn_key: None,
            text_data: None,
        }];
        let io_timeout = Duration::from_millis(50);

        // An acked insert closes entries fetched before it: the ack, and any version announcing the rows, follow the note.
        cache.insert_full(key.clone(), Vec::new(), Instant::now());
        mock.add(::clickhouse::test::handlers::record_ddl());
        client
            .insert_batch(&rows, Duration::from_secs(10), false)
            .await
            .unwrap();
        assert!(matches!(cache.lookup(&key), Lookup::Stale { .. }));

        // A failed insert may still commit: it closes them at once, and again once a late commit would be over.
        cache.insert_full(key.clone(), Vec::new(), Instant::now());
        mock.add(::clickhouse::test::handlers::failure(
            ::clickhouse::test::status::BAD_REQUEST,
        ));
        client
            .insert_batch(&rows, io_timeout, false)
            .await
            .unwrap_err();
        assert!(matches!(cache.lookup(&key), Lookup::Stale { .. }));
        cache.insert_full(key.clone(), Vec::new(), Instant::now());
        assert!(matches!(cache.lookup(&key), Lookup::Fresh(_)));
        tokio::time::sleep(3 * io_timeout).await;
        assert!(matches!(cache.lookup(&key), Lookup::Stale { .. }));
    }

    #[tokio::test]
    async fn a_batch_maps_rows_by_run_and_dates_them_from_the_reads_start() {
        use crate::series_cache::Lookup;
        #[derive(Serialize, ::clickhouse::Row)]
        struct Row {
            run_id: String,
            tag: String,
            step: i64,
            timestamp_ms: i64,
            value: f32,
            inserted_ms: i64,
        }
        let row = |run_id: &str, value: f32| Row {
            run_id: run_id.into(),
            tag: String::new(),
            step: 1,
            timestamp_ms: 0,
            value,
            inserted_ms: 1,
        };
        let mock = ::clickhouse::test::Mock::new();
        mock.add(::clickhouse::test::handlers::provide(vec![
            row("b", 2.0),
            row("c", 3.0),
            row("a", 1.0),
        ]));
        // A relay in front of the mock notes a bump for run "a" once the read's request is in flight.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = ChClient::new(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let upstream = mock.url().trim_start_matches("http://").to_owned();
        let noting = client.clone();
        tokio::spawn(async move {
            let (mut inbound, _) = listener.accept().await.unwrap();
            noting.series_cache().note_bumps(std::iter::once("a"));
            let mut outbound = tokio::net::TcpStream::connect(upstream).await.unwrap();
            let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
        });
        let cache = client.series_cache();
        let keys: Vec<SeriesKey> = ["a", "b", "c"]
            .into_iter()
            .map(|run| SeriesKey::new("p", run, "m"))
            .collect();

        let batch =
            client.spawn_full_read_batch(client.elect_full_reads(&keys, usize::MAX), Vec::new());
        // The current-thread runtime starts the read only at the await below, so this note precedes it: the read sees that insert, and "c" stays fresh.
        cache.note_bumps(std::iter::once("c"));
        let rows = batch.await.unwrap();

        for ((key, rows), value) in rows.into_iter().zip([1.0, 2.0, 3.0]) {
            assert_eq!(rows[0].value, value, "{key:?}");
        }
        // The rows date from before the query, so the note made while it was in flight leaves "a" stale.
        assert!(matches!(cache.lookup(&keys[0]), Lookup::Stale { .. }));
        assert!(matches!(cache.lookup(&keys[1]), Lookup::Fresh(_)));
        assert!(matches!(cache.lookup(&keys[2]), Lookup::Fresh(_)));
    }
}

#[cfg(test)]
mod registry_outbox_tests {
    use super::*;

    #[tokio::test]
    async fn metrics_insert_barrier_flushes_before_checking_active_inserts() {
        let mock = ::clickhouse::test::Mock::new();
        let flush = mock.add(::clickhouse::test::handlers::record_ddl());
        mock.add(::clickhouse::test::handlers::provide(vec![SingleCount {
            val: 0,
        }]));
        let ch = ChClient::new(mock.url()).unwrap();

        ch.barrier_metrics_inserts().await.unwrap();

        assert!(flush
            .query()
            .await
            .contains("SYSTEM FLUSH ASYNC INSERT QUEUE"));
    }

    #[tokio::test]
    async fn metrics_insert_barrier_rejects_a_still_active_insert() {
        let mock = ::clickhouse::test::Mock::new();
        mock.add(::clickhouse::test::handlers::record_ddl());
        mock.add(::clickhouse::test::handlers::provide(vec![SingleCount {
            val: 1,
        }]));
        let ch = ChClient::new(mock.url()).unwrap();

        let error = ch.barrier_metrics_inserts().await.unwrap_err();

        assert!(error.to_string().contains("left 1 active metrics inserts"));
    }

    #[derive(Clone, Serialize, clickhouse::Row)]
    struct LiveSourceRow {
        project_id: String,
        run_id: String,
        metric_name: String,
        value: Option<f32>,
        text_data: Option<String>,
    }

    /// Exercises the production ClickHouse mechanisms that the HTTP mock
    /// cannot model. Every object is uniquely named and removed afterward.
    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_CLICKHOUSE_URL"]
    async fn live_outbox_flushes_and_collapses_type_precedence() -> Result<()> {
        let _suite_guard = crate::pg::live_database_suite_gate().lock().await;
        let url = crate::pg::live_test_url("KYMO_LIVE_TEST_CLICKHOUSE_URL")?;
        let suffix = crate::pg::unique_suffix();
        let database = format!("registry_outbox_test_{suffix}");
        let source = format!("{database}.source");
        let target = format!("{database}.target");
        let view = format!("{database}.outbox_mv");
        let ch = ChClient::new(&url)?;
        let client = ch
            .client
            .clone()
            .with_option("wait_for_async_insert", "0")
            .with_option("async_insert_busy_timeout_ms", "60000");

        client
            .query(&format!("CREATE DATABASE {database}"))
            .execute()
            .await?;
        client
            .query(&format!(
                "CREATE TABLE {source} (
                    project_id String,
                    run_id String,
                    metric_name String,
                    value Nullable(Float32),
                    text_data Nullable(String)
                ) ENGINE = MergeTree ORDER BY tuple()"
            ))
            .execute()
            .await?;
        ch.ensure_metric_registry_outbox(&source, &target, &view)
            .await?;

        for (value, text_data) in [
            (None, None),
            (Some(1.0), None),
            (None, Some("text".to_string())),
        ] {
            let mut insert = client.insert(&source)?;
            insert
                .write(&LiveSourceRow {
                    project_id: "project".to_string(),
                    run_id: "run".to_string(),
                    metric_name: "metric".to_string(),
                    value,
                    text_data,
                })
                .await?;
            insert.end().await?;
            ch.barrier_metrics_inserts().await?;
        }

        let mut cursor = ch.metric_registry_outbox_at(&target)?;
        let mut rows = Vec::new();
        while let Some(row) = cursor.next().await? {
            rows.push(row);
        }
        anyhow::ensure!(
            rows.len() == 1
                && rows[0].project_id == "project"
                && rows[0].run_id == "run"
                && rows[0].metric_name == "metric"
                && rows[0].metric_type == 3,
            "expected one type-3 TEXT_STREAM-precedence row, got {} rows with first type {:?}",
            rows.len(),
            rows.first().map(|row| row.metric_type)
        );

        let storage = ch.metric_registry_outbox_storage_at(&target).await?;
        anyhow::ensure!(
            storage.rows > 0 && storage.bytes > 0 && storage.parts > 0,
            "expected non-empty outbox storage, got {storage:?}"
        );

        drop(cursor);
        ch.clear_metric_registry_outbox_at(&target).await?;
        let empty_storage = ch.metric_registry_outbox_storage_at(&target).await?;
        anyhow::ensure!(
            empty_storage.rows == 0 && empty_storage.bytes == 0 && empty_storage.parts == 0,
            "expected empty outbox storage after truncate, got {empty_storage:?}"
        );
        let remaining = client
            .query(&format!("SELECT count() AS val FROM {target}"))
            .fetch_one::<SingleCount>()
            .await?
            .val;
        anyhow::ensure!(remaining == 0, "outbox truncate left {remaining} rows");
        client
            .query(&format!("DROP DATABASE IF EXISTS {database} SYNC"))
            .execute()
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod text_window_tests {
    use super::{
        add_text_window_bytes, attach_line_offsets, text_lines_from_chunks, TextChunkDataRow,
        TextChunkWindowRow, TextSearchChunkRow, TextSearchCollector, TextWindowLimitError,
        MAX_TEXT_WINDOW_BYTES,
    };
    use crate::text_index_cache::IndexedTextChunk;

    fn chunk(step: i64, text: &str, lines_before: u64) -> TextChunkWindowRow {
        TextChunkWindowRow {
            step,
            metric_name: "stdout".to_string(),
            text: text.to_string(),
            lines_before,
            gap_before: false,
        }
    }

    fn gap_chunk(step: i64, text: &str, lines_before: u64) -> TextChunkWindowRow {
        TextChunkWindowRow {
            gap_before: true,
            ..chunk(step, text, lines_before)
        }
    }

    #[test]
    fn text_window_joins_lines_across_chunk_boundaries() {
        let lines = text_lines_from_chunks(
            vec![chunk(1, "hello ", 0), chunk(2, "world\nnext\n", 0)],
            false,
            0,
            10,
            2,
            MAX_TEXT_WINDOW_BYTES,
        )
        .unwrap();

        assert_eq!(
            lines
                .iter()
                .map(|line| line.text.as_str())
                .collect::<Vec<_>>(),
            ["hello world", "next"]
        );
    }

    #[test]
    fn text_window_keeps_the_preceding_partial_chunk_when_scrolled() {
        let lines = text_lines_from_chunks(
            vec![chunk(1, "first\npar", 0), chunk(2, "tial\nthird", 1)],
            false,
            1,
            1,
            3,
            MAX_TEXT_WINDOW_BYTES,
        )
        .unwrap();

        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].line_index, 1);
        assert_eq!(lines[0].text, "partial");
    }

    #[test]
    fn text_window_reanchors_when_a_middle_chunk_vanished() {
        // the index knew three chunks at lines_before 0/2/4; the middle one vanished between the index and payload reads — survivors keep their indexed numbering instead of sliding down into the gap
        let lines = text_lines_from_chunks(
            vec![chunk(1, "a\nb\n", 0), chunk(3, "e\nf\n", 4)],
            false,
            0,
            10,
            6,
            MAX_TEXT_WINDOW_BYTES,
        )
        .unwrap();

        assert_eq!(
            lines
                .iter()
                .map(|line| (line.line_index, line.text.as_str()))
                .collect::<Vec<_>>(),
            [(0, "a"), (1, "b"), (4, "e"), (5, "f")]
        );
    }

    #[test]
    fn text_window_drops_a_partial_whose_continuation_vanished() {
        // chunk 1 ends mid-line and the chunk carrying the continuation vanished: the partial must not splice onto the next survivor as a fabricated line
        let lines = text_lines_from_chunks(
            vec![chunk(1, "a\npar", 0), chunk(3, "d\n", 3)],
            false,
            0,
            10,
            4,
            MAX_TEXT_WINDOW_BYTES,
        )
        .unwrap();

        assert_eq!(
            lines
                .iter()
                .map(|line| (line.line_index, line.text.as_str()))
                .collect::<Vec<_>>(),
            [(0, "a"), (3, "d")]
        );
    }

    #[test]
    fn text_window_zero_separator_gap_does_not_splice() {
        // the vanished middle chunk ("ti" of "par|ti|al\n") has no separators, so line arithmetic can't see it — the positional gap flag must still stop "par" splicing onto "al" as a fabricated "paral"
        let lines = text_lines_from_chunks(
            vec![chunk(1, "a\npar", 0), gap_chunk(3, "al\n", 1)],
            false,
            0,
            10,
            2,
            MAX_TEXT_WINDOW_BYTES,
        )
        .unwrap();

        assert_eq!(
            lines
                .iter()
                .map(|line| (line.line_index, line.text.as_str()))
                .collect::<Vec<_>>(),
            [(0, "a"), (1, "al")]
        );
    }

    #[test]
    fn text_window_trailing_gap_suppresses_fabricated_tail() {
        // the index counted a final line whose chunk vanished: emitting the tail would fabricate an empty line (or, with a dangling partial, a truncated one) — fail closed and let the next poll heal it
        let lines = text_lines_from_chunks(
            vec![chunk(1, "a\n", 0)],
            true,
            0,
            10,
            2,
            MAX_TEXT_WINDOW_BYTES,
        )
        .unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!((lines[0].line_index, lines[0].text.as_str()), (0, "a"));

        let lines = text_lines_from_chunks(
            vec![chunk(1, "a\npar", 0)],
            true,
            0,
            10,
            2,
            MAX_TEXT_WINDOW_BYTES,
        )
        .unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!((lines[0].line_index, lines[0].text.as_str()), (0, "a"));
    }

    #[test]
    fn text_window_vanished_tail_content_is_not_an_empty_line() {
        // no trailing gap flagged (the tail chunk survived) but its content vanished to empty via replacement: pending stays empty and the tail block must not emit ""
        let lines = text_lines_from_chunks(
            vec![chunk(1, "a\n", 0), chunk(2, "", 1)],
            false,
            0,
            10,
            2,
            MAX_TEXT_WINDOW_BYTES,
        )
        .unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!((lines[0].line_index, lines[0].text.as_str()), (0, "a"));
    }

    #[test]
    fn text_window_backward_drift_stays_in_the_index_frame_and_within_limit() {
        // a surviving chunk's payload was REPLACED with more newlines than the index recorded: the next anchor re-anchors BACKWARD (index-authoritative), which may duplicate an index within this one response — but the response stays bounded by line_limit and the numbering never silently drifts forward
        let lines = text_lines_from_chunks(
            vec![
                chunk(1, "one\nextra\n", 0),
                chunk(2, "two\n", 1),
                chunk(3, "three\n", 2),
            ],
            false,
            0,
            3,
            3,
            MAX_TEXT_WINDOW_BYTES,
        )
        .unwrap();

        assert_eq!(
            lines
                .iter()
                .map(|line| (line.line_index, line.text.as_str()))
                .collect::<Vec<_>>(),
            [(0, "one"), (1, "extra"), (1, "two")]
        );
    }

    #[test]
    fn attach_line_offsets_flags_positional_gaps() {
        let indexed = vec![
            IndexedTextChunk::for_test(1, "stdout", "", 0, "x\n"),
            IndexedTextChunk::for_test(2, "stdout", "", 1, "x\n"),
            IndexedTextChunk::for_test(3, "stdout", "", 2, "x\n"),
        ];
        let row = |step: i64| TextChunkDataRow {
            step,
            metric_name: "stdout".to_string(),
            tag: String::new(),
            text: "x\n".to_string(),
        };

        let (rows, trailing) = attach_line_offsets(vec![row(1), row(2), row(3)], &indexed);
        assert_eq!(
            rows.iter().map(|r| r.gap_before).collect::<Vec<_>>(),
            [false, false, false]
        );
        assert!(!trailing);

        // middle index entry has no payload row: the next survivor is flagged
        let (rows, trailing) = attach_line_offsets(vec![row(1), row(3)], &indexed);
        assert_eq!(
            rows.iter().map(|r| r.gap_before).collect::<Vec<_>>(),
            [false, true]
        );
        assert!(!trailing);

        // last index entry has no payload row: trailing gap
        let (rows, trailing) = attach_line_offsets(vec![row(1), row(2)], &indexed);
        assert_eq!(
            rows.iter().map(|r| r.gap_before).collect::<Vec<_>>(),
            [false, false]
        );
        assert!(trailing);
    }

    #[test]
    fn attach_line_offsets_ignores_vanished_empty_entries() {
        // zero-byte indexed entries are inert: losing one between the reads loses nothing, so neither gap flag may fire (flagging would clear a valid partial or suppress a surviving tail)
        let indexed = vec![
            IndexedTextChunk::for_test(1, "stdout", "", 0, "a\npar"),
            IndexedTextChunk::for_test(2, "stdout", "", 1, ""),
            IndexedTextChunk::for_test(3, "stdout", "", 1, "tial\n"),
            IndexedTextChunk::for_test(4, "stdout", "", 2, ""),
        ];
        let row = |step: i64, text: &str| TextChunkDataRow {
            step,
            metric_name: "stdout".to_string(),
            tag: String::new(),
            text: text.to_string(),
        };

        // both empty entries (middle and trailing) vanished; the content survived intact
        let (rows, trailing) =
            attach_line_offsets(vec![row(1, "a\npar"), row(3, "tial\n")], &indexed);
        assert_eq!(
            rows.iter().map(|r| r.gap_before).collect::<Vec<_>>(),
            [false, false]
        );
        assert!(!trailing);

        // and the reconstruction still splices the spanning line across the inert gap
        let lines =
            text_lines_from_chunks(rows, trailing, 0, 10, 2, MAX_TEXT_WINDOW_BYTES).unwrap();
        assert_eq!(
            lines
                .iter()
                .map(|line| (line.line_index, line.text.as_str()))
                .collect::<Vec<_>>(),
            [(0, "a"), (1, "partial")]
        );

        // a vanished CONTENT gap followed by a vanished inert entry must stay a gap: gap_pending accumulates with |=, an assignment would erase it
        let indexed = vec![
            IndexedTextChunk::for_test(1, "stdout", "", 0, "a\n"),
            IndexedTextChunk::for_test(2, "stdout", "", 1, "b\n"),
            IndexedTextChunk::for_test(3, "stdout", "", 2, ""),
            IndexedTextChunk::for_test(4, "stdout", "", 2, "c\n"),
        ];
        let (rows, trailing) = attach_line_offsets(vec![row(1, "a\n"), row(4, "c\n")], &indexed);
        assert_eq!(
            rows.iter().map(|r| r.gap_before).collect::<Vec<_>>(),
            [false, true]
        );
        assert!(!trailing);
    }

    #[test]
    fn text_window_mid_stream_partial_is_not_a_tail() {
        // the final SELECTED chunk deflated (replacement lost its newline) while the stream continues past the window (line_end 2 < total 5): the dangling partial is a mid-stream line head, not the stream's tail — only completed + 1 == total proves tail-ness
        let lines = text_lines_from_chunks(
            vec![chunk(1, "a\npar", 0)],
            false,
            0,
            2,
            5,
            MAX_TEXT_WINDOW_BYTES,
        )
        .unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!((lines[0].line_index, lines[0].text.as_str()), (0, "a"));
    }

    #[test]
    fn text_window_includes_an_unterminated_tail() {
        let lines = text_lines_from_chunks(
            vec![chunk(1, "tail", 0)],
            false,
            0,
            10,
            1,
            MAX_TEXT_WINDOW_BYTES,
        )
        .unwrap();

        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].text, "tail");
    }

    #[test]
    fn text_window_caps_expanded_response_lines() {
        let metric_name = "x".repeat(2_048);
        let line_bytes = super::text_line_working_bytes("a", &metric_name);
        let error = text_lines_from_chunks(
            vec![TextChunkWindowRow {
                step: 1,
                metric_name,
                text: "a\nb\n".to_string(),
                lines_before: 0,
                gap_before: false,
            }],
            false,
            0,
            2,
            2,
            line_bytes.saturating_mul(2).saturating_sub(1),
        )
        .unwrap_err();

        assert!(error.downcast_ref::<TextWindowLimitError>().is_some());
    }

    #[test]
    fn search_joins_chunks_and_keeps_result_provenance() {
        let mut search = TextSearchCollector::new("HELLO WORLD", 0, 10, 1_024);
        search
            .push_chunk(TextSearchChunkRow {
                step: 1,
                metric_name: "stdout".to_string(),
                text: "hello ".to_string(),
            })
            .unwrap();
        search
            .push_chunk(TextSearchChunkRow {
                step: 2,
                metric_name: "stderr".to_string(),
                text: "world\nno match\n".to_string(),
            })
            .unwrap();

        let window = search.finish().unwrap();
        assert_eq!(window.first_step, 1);
        assert_eq!(window.total_lines, 1);
        assert_eq!(window.lines.len(), 1);
        assert_eq!(window.lines[0].text, "hello world");
        assert_eq!(window.lines[0].step, 2);
        assert_eq!(window.lines[0].metric_name, "stderr");
    }

    #[test]
    fn search_counts_matches_outside_the_requested_window() {
        let mut search = TextSearchCollector::new("match", 1, 1, 1_024);
        search
            .push_chunk(TextSearchChunkRow {
                step: 1,
                metric_name: "stdout".to_string(),
                text: "match one\nmatch two\nmatch three".to_string(),
            })
            .unwrap();

        let window = search.finish().unwrap();
        assert_eq!(window.total_lines, 3);
        assert_eq!(window.lines.len(), 1);
        assert_eq!(window.lines[0].line_index, 1);
        assert_eq!(window.lines[0].text, "match two");
    }

    #[test]
    fn search_rejects_an_unbounded_reconstructed_line_before_appending_it() {
        let mut search = TextSearchCollector::new("match", 0, 1, 8);
        let error = search
            .push_chunk(TextSearchChunkRow {
                step: 1,
                metric_name: "stdout".to_string(),
                text: "nine-byte".to_string(),
            })
            .unwrap_err();

        assert!(error
            .downcast_ref::<super::TextWindowLimitError>()
            .is_some());
        assert!(search.pending.is_empty());
    }

    #[test]
    fn accumulated_window_payload_is_hard_bounded() {
        let error = add_text_window_bytes(MAX_TEXT_WINDOW_BYTES, 1).unwrap_err();
        assert!(error.downcast_ref::<TextWindowLimitError>().is_some());
    }
}
