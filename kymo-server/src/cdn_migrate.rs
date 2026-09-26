//! Bucket inventory gauges for the hosted `gcs` CDN mode (docs/cdn-gcs-migration.md observability).

use std::sync::Arc;

use crate::cdn_store::GcsStore;

/// The gcs mode's replacement for the CDN disk gauge: periodic bucket inventory.
/// Six-hourly — a full listing per tick, so the cadence stays coarse.
pub fn spawn_inventory_gauges(gcs: Arc<GcsStore>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(6 * 3600));
        loop {
            tick.tick().await;
            publish_inventory_gauges(&gcs).await;
        }
    });
}

/// Bucket count/bytes gauges. A failed listing sets them to NaN (a gap in the timeseries panels,
/// never a previous listing's count) and is counted as `read` by the store.
async fn publish_inventory_gauges(gcs: &GcsStore) {
    let (objects, bytes) = match gcs.inventory().await {
        Ok((objects, bytes)) => (objects as f64, bytes as f64),
        Err(error) => {
            tracing::warn!(error = %error, "CDN bucket inventory listing failed");
            (f64::NAN, f64::NAN)
        }
    };
    metrics::gauge!("mkdb2_cdn_gcs_objects").set(objects);
    metrics::gauge!("mkdb2_cdn_gcs_bytes").set(bytes);
}
