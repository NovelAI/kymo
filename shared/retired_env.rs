//! Retired `MKDB2_*` environment names and their canonical `KYMO_*` replacements.
//! One list: the server rejects the hosted names, the local-runtime launcher rejects the hosted names plus its own, and both retired-env integration tests iterate these constants — so the rejection surface and its tests cannot drift apart. No dependencies; included via `#[path]`.

/// Hosted server configuration names (also rejected by the launcher, which spawns the server).
pub const RETIRED_HOSTED_ENV: &[(&str, &str)] = &[
    ("MKDB2_SERVER_MODE", "KYMO_SERVER_MODE"),
    ("MKDB2_ALLOWED_ORIGINS", "KYMO_ALLOWED_ORIGINS"),
    ("MKDB2_WATCHDOG_URL", "KYMO_WATCHDOG_URL"),
    ("MKDB2_SERIES_CACHE", "KYMO_SERIES_CACHE"),
    ("MKDB2_SERIES_CACHE_MB", "KYMO_SERIES_CACHE_MB"),
    ("MKDB2_INGEST_BYTE_CAP", "KYMO_INGEST_BYTE_CAP"),
    ("MKDB2_FLUSH_CONCURRENCY", "KYMO_FLUSH_CONCURRENCY"),
    ("MKDB2_CAP_UNARY_FLUSHES", "KYMO_CAP_UNARY_FLUSHES"),
    ("MKDB2_RUN_REAPER_ENABLED", "KYMO_RUN_REAPER_ENABLED"),
    ("MKDB2_RUN_REAPER_BATCH_SIZE", "KYMO_RUN_REAPER_BATCH_SIZE"),
    ("MKDB2_TEXT_INDEX_CACHE_MB", "KYMO_TEXT_INDEX_CACHE_MB"),
    ("MKDB2_CHART_INFLIGHT_SERIES", "KYMO_CHART_INFLIGHT_SERIES"),
    ("MKDB2_CDN_BACKEND", "KYMO_CDN_BACKEND"),
    ("MKDB2_CDN_GCS_BUCKET", "KYMO_CDN_GCS_BUCKET"),
    ("MKDB2_CDN_BACKFILL", "KYMO_CDN_BACKFILL"),
    ("MKDB2_CDN_RECONCILE", "KYMO_CDN_RECONCILE"),
    ("MKDB2_IMPORT_ENABLED", "KYMO_IMPORT_ENABLED"),
    ("MKDB2_IMPORT_CUT_ROWS", "KYMO_IMPORT_CUT_ROWS"),
    ("MKDB2_IMPORT_CONCURRENCY", "KYMO_IMPORT_CONCURRENCY"),
    ("MKDB2_PROMETHEUS_URL", "KYMO_PROMETHEUS_URL"),
];

/// Local-runtime launcher names.
pub const RETIRED_LOCAL_ENV: &[(&str, &str)] = &[
    ("MKDB2_LOCAL_ROOT", "KYMO_LOCAL_ROOT"),
    ("MKDB2_LOCAL_NO_INSTALL", "KYMO_LOCAL_NO_INSTALL"),
];
