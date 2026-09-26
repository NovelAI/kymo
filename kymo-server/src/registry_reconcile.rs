use crate::{clickhouse, ingest, pg};
use anyhow::Context;

const SKIPPED_SAMPLE_LIMIT: usize = 10;
const IDENT_PREVIEW_CHARS: usize = 64;

#[derive(Debug, Default)]
pub(crate) struct Stats {
    pub(crate) rows: u64,
    pub(crate) skipped_rows: u64,
    pub(crate) skipped_samples: Vec<String>,
    pub(crate) batches: u64,
}

fn ident_preview(value: &str) -> String {
    let mut chars = value.chars();
    let mut escaped = String::new();
    for character in chars.by_ref().take(IDENT_PREVIEW_CHARS) {
        escaped.extend(character.escape_default());
    }
    if chars.next().is_some() {
        escaped.push('…');
    }
    format!("{escaped} ({} bytes)", value.len())
}

fn skipped_identity(row: &clickhouse::RegistryRow, metric_type: &str) -> String {
    format!(
        "project_id={}, run_id={}, metric_name={}, metric_type={metric_type}",
        ident_preview(&row.project_id),
        ident_preview(&row.run_id),
        ident_preview(&row.metric_name),
    )
}

fn metric_type_name(metric_type: u8) -> anyhow::Result<&'static str> {
    match metric_type {
        1 => Ok("CDN"),
        2 => Ok("NUMERIC"),
        3 => Ok("TEXT_STREAM"),
        other => anyhow::bail!(
            "unsupported metric-registry type code {other}; refusing to checkpoint the outbox"
        ),
    }
}

fn registrable(row: &clickhouse::RegistryRow) -> bool {
    ingest::storable_ident(&row.project_id, ingest::MAX_ID_BYTES)
        && ingest::storable_ident(&row.run_id, ingest::MAX_ID_BYTES)
        && ingest::storable_ident(&row.metric_name, ingest::MAX_METRIC_NAME_BYTES)
}

pub(crate) async fn drain<F: std::future::Future<Output = anyhow::Result<()>>>(
    mut cursor: ::clickhouse::query::RowCursor<clickhouse::RegistryRow>,
    mut write_batch: impl FnMut(Vec<(String, String, String, String)>) -> F,
) -> anyhow::Result<Stats> {
    let mut batch = Vec::with_capacity(pg::RUN_METRICS_BATCH_ROWS);
    let mut stats = Stats::default();

    loop {
        while batch.len() < pg::RUN_METRICS_BATCH_ROWS {
            let Some(row) = cursor
                .next()
                .await
                .context("streaming ClickHouse metric-registry outbox")?
            else {
                break;
            };
            stats.rows += 1;
            // Validate corruption before considering an identity skippable: a
            // bad code must never disappear merely because another field is
            // also outside the historical Postgres boundary.
            let metric_type = metric_type_name(row.metric_type)?;
            if !registrable(&row) {
                stats.skipped_rows += 1;
                if stats.skipped_samples.len() < SKIPPED_SAMPLE_LIMIT {
                    stats
                        .skipped_samples
                        .push(skipped_identity(&row, metric_type));
                }
                continue;
            }
            batch.push((
                row.project_id,
                row.run_id,
                row.metric_name,
                metric_type.to_string(),
            ));
        }

        if batch.is_empty() {
            return Ok(stats);
        }
        let ready = std::mem::replace(&mut batch, Vec::with_capacity(pg::RUN_METRICS_BATCH_ROWS));
        write_batch(ready)
            .await
            .with_context(|| format!("registering metric-registry batch {}", stats.batches + 1))?;
        stats.batches += 1;
    }
}

/// Reconcile metric identities accepted since the previous boot. Some may
/// already be present through the two-second Postgres write-behind; the
/// ClickHouse outbox is an intentionally at-least-once recovery path.
///
/// Startup first drains ClickHouse's server-side async-insert queue and verifies
/// that no old-process INSERT is active. Together with running before either
/// ingest listener starts, that makes the final TRUNCATE safe: no row can arrive
/// after the SELECT snapshot. Any error fails startup; before TRUNCATE the whole
/// outbox remains retryable, and after it every row is committed idempotently.
pub async fn reconcile(ch: &clickhouse::ChClient, pg: &pg::PgStore) -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    tracing::info!("Reconciling metric-registry outbox");
    ch.barrier_metrics_inserts()
        .await
        .context("establishing ClickHouse metric-registry reconciliation barrier")?;
    let cursor = ch
        .metric_registry_outbox()
        .context("opening ClickHouse metric-registry outbox cursor")?;
    // Startup predates the event bus and listeners. A browser that survives
    // the restart reconnects its socket, which advances the frontend's
    // resync_gen and re-lists visible metrics; data-version bumps here
    // would be redundant and would falsely announce that chart data changed.
    let stats = drain(cursor, |batch| async move {
        pg.register_run_metrics(&batch).await?;
        Ok(())
    })
    .await?;
    if stats.skipped_rows > 0 {
        tracing::warn!(
            skipped_rows = stats.skipped_rows,
            samples = %stats.skipped_samples.join("; "),
            "Excluded unregistrable historical metric identities before checkpointing the outbox"
        );
    }
    ch.clear_metric_registry_outbox()
        .await
        .context("clearing reconciled ClickHouse metric-registry outbox")?;
    let elapsed = started.elapsed().as_secs_f64();
    metrics::gauge!("mkdb2_registry_reconcile_rows").set(stats.rows as f64);
    metrics::gauge!("mkdb2_registry_reconcile_skipped_rows").set(stats.skipped_rows as f64);
    metrics::gauge!("mkdb2_registry_reconcile_duration_seconds").set(elapsed);
    tracing::info!(
        rows = stats.rows,
        skipped_rows = stats.skipped_rows,
        batches = stats.batches,
        duration_seconds = elapsed,
        "Reconciled metric-registry outbox"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn registry_rows(count: usize) -> Vec<clickhouse::RegistryRow> {
        (0..count)
            .map(|i| clickhouse::RegistryRow {
                project_id: "project".to_string(),
                run_id: format!("run-{}", i / 100),
                metric_name: format!("metric-{i}"),
                metric_type: if i % 2 == 0 { 2 } else { 3 },
            })
            .collect()
    }

    #[tokio::test]
    async fn batches_rows() {
        let mock = ::clickhouse::test::Mock::new();
        mock.add(::clickhouse::test::handlers::provide(registry_rows(
            pg::RUN_METRICS_BATCH_ROWS + 1,
        )));
        let ch = clickhouse::ChClient::new(mock.url()).unwrap();
        let batch_sizes = std::sync::Mutex::new(Vec::new());

        let stats = drain(ch.metric_registry_outbox().unwrap(), async |batch| {
            batch_sizes.lock().unwrap().push(batch.len());
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(
            *batch_sizes.lock().unwrap(),
            [pg::RUN_METRICS_BATCH_ROWS, 1]
        );
        assert_eq!(stats.rows, (pg::RUN_METRICS_BATCH_ROWS + 1) as u64);
        assert_eq!(stats.skipped_rows, 0);
        assert_eq!(stats.batches, 2);
    }

    #[tokio::test]
    async fn skips_unregistrable_historical_rows() {
        let mock = ::clickhouse::test::Mock::new();
        let mut rows = registry_rows(13);
        rows[0].project_id.push('\0');
        rows[1].metric_name = "x".repeat(ingest::MAX_METRIC_NAME_BYTES + 1);
        for row in &mut rows[2..12] {
            row.run_id.push('\0');
        }
        mock.add(::clickhouse::test::handlers::provide(rows));
        let ch = clickhouse::ChClient::new(mock.url()).unwrap();
        let written = std::sync::Mutex::new(Vec::new());

        let stats = drain(ch.metric_registry_outbox().unwrap(), async |batch| {
            written.lock().unwrap().extend(batch);
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(stats.rows, 13);
        assert_eq!(stats.skipped_rows, 12);
        assert_eq!(stats.skipped_samples.len(), SKIPPED_SAMPLE_LIMIT);
        assert!(stats
            .skipped_samples
            .iter()
            .all(|sample| !sample.contains('\0')));
        assert!(stats.skipped_samples[0].contains(r"\u{0}"));
        assert!(!stats.skipped_samples[0].contains(r"\\u{0}"));
        assert_eq!(written.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn rejects_unknown_metric_type_codes() {
        let mock = ::clickhouse::test::Mock::new();
        let mut rows = registry_rows(1);
        rows[0].metric_type = 4;
        rows[0].project_id.push('\0');
        mock.add(::clickhouse::test::handlers::provide(rows));
        let ch = clickhouse::ChClient::new(mock.url()).unwrap();

        let error = drain(ch.metric_registry_outbox().unwrap(), async |_| Ok(()))
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("unsupported metric-registry type code 4"));
    }

    #[tokio::test]
    async fn reports_the_failing_batch() {
        let mock = ::clickhouse::test::Mock::new();
        mock.add(::clickhouse::test::handlers::provide(registry_rows(
            pg::RUN_METRICS_BATCH_ROWS + 1,
        )));
        let ch = clickhouse::ChClient::new(mock.url()).unwrap();
        let writes = AtomicUsize::new(0);

        let result = drain(ch.metric_registry_outbox().unwrap(), async |_| {
            if writes.fetch_add(1, Ordering::SeqCst) == 1 {
                anyhow::bail!("injected Postgres failure");
            }
            Ok(())
        })
        .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("registering metric-registry batch 2"));
        assert_eq!(writes.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn clear_uses_the_outbox_table() {
        let mock = ::clickhouse::test::Mock::new();
        let recorded = mock.add(::clickhouse::test::handlers::record_ddl());
        let ch = clickhouse::ChClient::new(mock.url()).unwrap();

        ch.clear_metric_registry_outbox().await.unwrap();

        let query = recorded.query().await;
        assert!(query.contains("TRUNCATE TABLE mkdb2.metric_registry_outbox"));
    }
}
