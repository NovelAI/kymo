mod activity;
mod alerts;
mod cdn;
mod cdn_gc;
mod cdn_store;
mod chart;
// Wire contract shared verbatim with the frontend (lives next to kymo.proto; both crates include it by path). Each side exercises its half — the server slices, the client splices — so the other half is dead code by design.
#[path = "../../proto/chart_delta.rs"]
#[allow(dead_code)]
mod chart_delta;
mod clickhouse;
mod deletion;
mod embedded_frontend;
#[path = "../../shared/env.rs"]
mod env;
mod events;
mod http_transport;
mod import;
mod ingest;
mod lifecycle;
#[path = "../../shared/liveness.rs"]
mod liveness;
mod local_auth;
mod local_control;
mod notifier;
mod pg;
mod private_file;
mod query;
mod refresh_locks;
mod registry_reconcile;
#[path = "../../shared/retired_env.rs"]
pub mod retired_env;
mod series_cache;
mod sys_stats;
mod text_index_cache;
mod transport;
mod ws_proxy;
#[path = "../../proto/ws_rpc.rs"]
#[allow(dead_code)]
mod ws_rpc;

use std::path::PathBuf;
use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};
use tracing_subscriber::EnvFilter;

pub mod proto {
    tonic::include_proto!("kymo");
}

pub mod local_proto {
    tonic::include_proto!("kymo.local.v1");
}

use proto::kymo_server::Kymo;

fn validate_environment_namespace() -> anyhow::Result<()> {
    env::reject_retired_environment(retired_env::RETIRED_HOSTED_ENV)?;
    // Not in the shared list: the launcher would trip on unrelated software's CDN_ROOT.
    env::reject_retired_environment(&[("CDN_ROOT", "KYMO_CDN_ROOT")])?;
    anyhow::ensure!(
        std::env::var_os("MKDB2_RUN_REAPER_INTERVAL_SECONDS").is_none(),
        "MKDB2_RUN_REAPER_INTERVAL_SECONDS is no longer supported; remove it (the production cadence is fixed at one hour)"
    );
    anyhow::ensure!(
        std::env::var_os("KYMO_RUN_REAPER_INTERVAL_SECONDS").is_none(),
        "KYMO_RUN_REAPER_INTERVAL_SECONDS is not supported; remove it (the production cadence is fixed at one hour)"
    );
    Ok(())
}

/// RAII timer that records its lifetime into `mkdb2_rpc_duration_seconds`
/// labelled by method. Drop it at the end of a handler (just bind it to `_t`)
/// to capture the full request latency, including error paths.
struct RpcTimer {
    method: &'static str,
    start: std::time::Instant,
}

impl RpcTimer {
    fn new(method: &'static str) -> Self {
        Self {
            method,
            start: std::time::Instant::now(),
        }
    }
}

impl Drop for RpcTimer {
    fn drop(&mut self) {
        metrics::histogram!("mkdb2_rpc_duration_seconds", "method" => self.method)
            .record(self.start.elapsed().as_secs_f64());
    }
}

/// Crate-visible: ws_proxy dispatches the unary methods of this service
/// over the browser WebSocket transport.
pub(crate) struct KymoService {
    ingest: ingest::IngestService,
    import: import::ImportService,
    query: Arc<query::QueryService>,
    lifecycle_mutations: lifecycle::LifecycleMutationBarrier,
    activity: Arc<activity::ActivityTracker>,
    /// Version bumps from every change source (the ingest bump coalescer,
    /// run lifecycle RPCs, the status watcher); each dashboard socket
    /// subscribes and pushes coalesced id-0 event frames (see ws_proxy).
    pub(crate) run_events: events::EventSender,
}

async fn wait_for_lifecycle_barrier<T>(
    mode: &'static str,
    timeout_message: &'static str,
    wait: impl std::future::Future<Output = T>,
) -> Result<T, Status> {
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), wait).await;
    metrics::histogram!("mkdb2_lifecycle_barrier_wait_seconds", "mode" => mode)
        .record(started.elapsed().as_secs_f64());
    result.map_err(|_| Status::unavailable(timeout_message))
}

impl KymoService {
    /// Run one Trash/Restore mutation under the lifecycle barrier, owned by a
    /// detached task. If a browser socket or native gRPC client disappears,
    /// dropping the JoinHandle detaches the task rather than cancelling it: the
    /// mutation guard stays held until the database outcome is final, so
    /// ListTrash can reconcile safely. `name` labels the RpcTimer (created
    /// inside the task, so it measures execution) and the failure log; the
    /// barrier-wait metric is shared across every lifecycle mutation.
    async fn run_detached_mutation<F, Fut, T>(
        &self,
        name: &'static str,
        work: F,
    ) -> Result<T, Status>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, Status>> + Send + 'static,
        T: Send + 'static,
    {
        let activity_work = self.activity.begin_work();
        let guard = wait_for_lifecycle_barrier(
            "mutation",
            "lifecycle mutation queue timed out",
            self.lifecycle_mutations.mutation(),
        )
        .await?;
        tokio::spawn(async move {
            let _activity_work = activity_work;
            let _guard = guard;
            let _t = RpcTimer::new(name);
            work().await
        })
        .await
        .map_err(|error| Status::internal(format!("{name} task failed: {error}")))?
    }
}

#[tonic::async_trait]
impl Kymo for KymoService {
    // --- Ingest ---

    async fn ingest_metrics(
        &self,
        request: Request<Streaming<proto::MetricsBatch>>,
    ) -> Result<Response<proto::IngestResponse>, Status> {
        self.ingest.ingest_metrics(request).await
    }

    type IngestMetricsBidiStream = ingest::IngestAckStream;

    async fn ingest_metrics_bidi(
        &self,
        request: Request<Streaming<proto::MetricsBatch>>,
    ) -> Result<Response<Self::IngestMetricsBidiStream>, Status> {
        self.ingest.ingest_metrics_bidi(request).await
    }

    async fn publish_rich_mutation(
        &self,
        request: Request<proto::PublishRichMutationRequest>,
    ) -> Result<Response<proto::PublishRichMutationResponse>, Status> {
        self.ingest.publish_rich_mutation(request).await
    }

    // --- Run registration ---

    async fn init_run(
        &self,
        request: Request<proto::InitRunRequest>,
    ) -> Result<Response<proto::InitRunResponse>, Status> {
        let _t = RpcTimer::new("init_run");
        let _work = self.activity.begin_work();
        let hold_id = request.get_ref().local_hold_id.clone();
        let response = self.query.init_run(request).await?;
        self.activity.record_lifecycle_mutation(hold_id.as_deref());
        Ok(response)
    }

    async fn rename_run(
        &self,
        request: Request<proto::RenameRunRequest>,
    ) -> Result<Response<proto::RenameRunResponse>, Status> {
        let _t = RpcTimer::new("rename_run");
        let _work = self.activity.begin_work();
        let response = self.query.rename_run(request).await?;
        self.activity.record_lifecycle_mutation(None);
        Ok(response)
    }

    async fn terminate_run(
        &self,
        request: Request<proto::TerminateRunRequest>,
    ) -> Result<Response<proto::TerminateRunResponse>, Status> {
        let _t = RpcTimer::new("terminate_run");
        let _work = self.activity.begin_work();
        let response = self.query.terminate_run(request).await?;
        self.activity.record_lifecycle_mutation(None);
        Ok(response)
    }

    // --- Bulk import (all gated by KYMO_IMPORT_ENABLED) ---

    async fn import_run(
        &self,
        request: Request<proto::ImportRunRequest>,
    ) -> Result<Response<proto::ImportRunResponse>, Status> {
        let _t = RpcTimer::new("import_run");
        let _work = self.activity.begin_work();
        self.import.require_enabled()?;
        let response = self.query.import_run(request).await?;
        self.activity.record_lifecycle_mutation(None);
        Ok(response)
    }

    type ImportMetricsBidiStream = ingest::IngestAckStream;

    async fn import_metrics_bidi(
        &self,
        request: Request<Streaming<proto::MetricsBatch>>,
    ) -> Result<Response<Self::ImportMetricsBidiStream>, Status> {
        self.import.require_enabled()?;
        self.import.import_metrics_bidi(request).await
    }

    async fn finalize_import_run(
        &self,
        request: Request<proto::FinalizeImportRunRequest>,
    ) -> Result<Response<proto::FinalizeImportRunResponse>, Status> {
        let _t = RpcTimer::new("finalize_import_run");
        let _work = self.activity.begin_work();
        self.import.require_enabled()?;
        let response = self.query.finalize_import_run(request).await?;
        self.activity.record_lifecycle_mutation(None);
        Ok(response)
    }

    async fn trash_runs(
        &self,
        request: Request<proto::TrashRunsRequest>,
    ) -> Result<Response<proto::TrashRunsResponse>, Status> {
        let query = self.query.clone();
        let response = self
            .run_detached_mutation("trash_runs", move || async move {
                query.trash_runs(request).await
            })
            .await?;
        self.activity.record_lifecycle_mutation(None);
        Ok(response)
    }

    async fn restore_run(
        &self,
        request: Request<proto::RestoreRunRequest>,
    ) -> Result<Response<proto::RestoreRunResponse>, Status> {
        let query = self.query.clone();
        let response = self
            .run_detached_mutation("restore_run", move || async move {
                query.restore_run(request).await
            })
            .await?;
        self.activity.record_lifecycle_mutation(None);
        Ok(response)
    }

    async fn list_trash(
        &self,
        request: Request<proto::ListTrashRequest>,
    ) -> Result<Response<proto::ListTrashResponse>, Status> {
        let _t = RpcTimer::new("list_trash");
        let _work = self.activity.begin_work();
        // Only an identity lookup that reconciles an unknown mutation result
        // needs the outcome fence. Ordinary Trash pages are coherent through
        // their repeatable-read snapshot and global-version cursor check, and
        // should not block an unrelated bulk mutation (or be blocked by one).
        let filtered = query::list_trash_is_filtered(request.get_ref());
        let _snapshot = if filtered {
            let guard = wait_for_lifecycle_barrier(
                "snapshot",
                "Trash snapshot queue timed out",
                self.lifecycle_mutations.snapshot(),
            )
            .await?;
            Some(guard)
        } else {
            None
        };
        self.query.list_trash(request).await
    }

    async fn get_run(
        &self,
        request: Request<proto::GetRunRequest>,
    ) -> Result<Response<proto::GetRunResponse>, Status> {
        let _t = RpcTimer::new("get_run");
        let _work = self.activity.begin_work();
        self.query.get_run(request).await
    }

    // --- Discovery ---

    async fn list_projects(
        &self,
        request: Request<proto::ListProjectsRequest>,
    ) -> Result<Response<proto::ListProjectsResponse>, Status> {
        let _t = RpcTimer::new("list_projects");
        let _work = self.activity.begin_work();
        self.query.list_projects(request).await
    }

    async fn list_runs(
        &self,
        request: Request<proto::ListRunsRequest>,
    ) -> Result<Response<proto::ListRunsResponse>, Status> {
        let _t = RpcTimer::new("list_runs");
        let _work = self.activity.begin_work();
        self.query.list_runs(request).await
    }

    async fn list_metrics(
        &self,
        request: Request<proto::ListMetricsRequest>,
    ) -> Result<Response<proto::ListMetricsResponse>, Status> {
        let _t = RpcTimer::new("list_metrics");
        let _work = self.activity.begin_work();
        self.query.list_metrics(request).await
    }

    async fn list_run_set_metrics(
        &self,
        request: Request<proto::ListRunSetMetricsRequest>,
    ) -> Result<Response<proto::ListMetricsResponse>, Status> {
        let _t = RpcTimer::new("list_run_set_metrics");
        let _work = self.activity.begin_work();
        self.query.list_run_set_metrics(request).await
    }

    // --- Chart queries ---

    async fn query_chart(
        &self,
        request: Request<proto::ChartRequest>,
    ) -> Result<Response<proto::ChartResponse>, Status> {
        let _t = RpcTimer::new("query_chart");
        let _work = self.activity.begin_work();
        self.query.query_chart(request).await
    }

    // --- CDN queries ---

    async fn query_cdn_keys(
        &self,
        request: Request<proto::QueryCdnKeysRequest>,
    ) -> Result<Response<proto::QueryCdnKeysResponse>, Status> {
        let _t = RpcTimer::new("query_cdn_keys");
        let _work = self.activity.begin_work();
        self.query.query_cdn_keys(request).await
    }

    // --- Text stream queries ---

    async fn query_text_window(
        &self,
        request: Request<proto::QueryTextWindowRequest>,
    ) -> Result<Response<proto::QueryTextWindowResponse>, Status> {
        let _t = RpcTimer::new("query_text_window");
        let _work = self.activity.begin_work();
        self.query.query_text_window(request).await
    }

    // --- Change detection ---

    async fn poll_versions(
        &self,
        request: Request<proto::PollVersionsRequest>,
    ) -> Result<Response<proto::PollVersionsResponse>, Status> {
        let _t = RpcTimer::new("poll_versions");
        let _work = self.activity.begin_work();
        self.query.poll_versions(request).await
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CdnBackendMode {
    Filesystem,
    Gcs,
}

/// CDN backend mode (docs/cdn-gcs-migration.md): only a genuinely ABSENT key defaults to the filesystem store — an explicitly empty, unknown, or non-unicode value fails startup.
fn cdn_backend_mode(value: Option<&std::ffi::OsStr>) -> anyhow::Result<CdnBackendMode> {
    match value {
        None => Ok(CdnBackendMode::Filesystem),
        Some(v) if v == "filesystem" => Ok(CdnBackendMode::Filesystem),
        Some(v) if v == "gcs" => Ok(CdnBackendMode::Gcs),
        Some(v) => anyhow::bail!("KYMO_CDN_BACKEND={v:?} is not a backend mode (filesystem | gcs)"),
    }
}

/// Validates the whole CDN env surface up front (docs/cdn-gcs-migration.md: gcs mode with incomplete GCS config fails startup — no silent fallback to the filesystem). `None` is the filesystem store; in gcs mode every credential file is opened and decoded here, before any database work (fail closed, no ambient ADC).
fn cdn_preflight() -> anyhow::Result<Option<(cdn_store::GcsStore, cdn_gc::Config)>> {
    cdn_store::register_cdn_metrics();
    if cdn_backend_mode(std::env::var_os("KYMO_CDN_BACKEND").as_deref())?
        == CdnBackendMode::Filesystem
    {
        return Ok(None);
    }
    let bucket = env::optional_string("KYMO_CDN_GCS_BUCKET")
        .ok_or_else(|| anyhow::anyhow!("KYMO_CDN_BACKEND=gcs requires KYMO_CDN_GCS_BUCKET"))?;
    // Non-empty unicode, and passed explicitly to the store — an empty or non-unicode value
    // must fail startup here, never fall through to ambient ADC or instance credentials.
    let credentials = env::optional_string("GOOGLE_APPLICATION_CREDENTIALS").ok_or_else(|| {
        anyhow::anyhow!(
            "KYMO_CDN_BACKEND=gcs requires GOOGLE_APPLICATION_CREDENTIALS (a non-empty credential file path)"
        )
    })?;
    let gc_config = cdn_gc::Config::from_env(&bucket)?;
    Ok(Some((
        cdn_store::GcsStore::new(bucket, &credentials)?,
        gc_config,
    )))
}

pub async fn run() -> anyhow::Result<()> {
    validate_environment_namespace()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        // A failed log write must not become a panic: reporting it goes through eprintln!, which panics when stderr is the same broken pipe (a local server whose supervisor died), and killing the logging task can take the SIGTERM drain with it.
        .log_internal_errors(false)
        .init();

    // Install the Prometheus recorder before anything emits metrics. Returns a
    // handle we render from the /metrics HTTP endpoint (scraped by Prometheus).
    let prom_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("install prometheus recorder");
    metrics::gauge!("mkdb2_build_info").set(1.0);

    let pg_url = env::string_or("DATABASE_URL", "postgres://mkdb2@localhost:5432/mkdb2");
    let transport = transport::TransportConfig::from_env()?;
    // Hosted only — local-runtime is always the filesystem store and must not be steered by ambient env.
    let gcs = if matches!(transport, transport::TransportConfig::Hosted { .. }) {
        cdn_preflight()?
    } else {
        None
    };
    let local_security = match &transport {
        transport::TransportConfig::Hosted { .. } => None,
        transport::TransportConfig::Local {
            auth_secret_path,
            lifecycle_secret_path,
            ..
        } => {
            let auth = local_auth::LocalAuth::read(auth_secret_path)?;
            let lifecycle = local_control::LifecycleAuth::read(lifecycle_secret_path)?;
            Some(http_transport::LocalSecurity::new(auth, lifecycle)?)
        }
    };

    let ch = Arc::new(match &transport {
        transport::TransportConfig::Hosted { .. } => {
            clickhouse::ChClient::new(&env::string_or("CLICKHOUSE_URL", "http://localhost:8123"))?
        }
        transport::TransportConfig::Local { clickhouse, .. } => clickhouse::ChClient::new_local(
            &clickhouse.url,
            &clickhouse.server_cert_path,
            &clickhouse.user,
            &clickhouse.password,
        )?,
    });
    ch.ensure_schema().await?;
    ch.ensure_reserved_project_absent(ingest::RESERVED_PROJECT_ID)
        .await?;

    let pg = Arc::new(pg::PgStore::connect(&pg_url).await?);

    pg.ensure_reserved_project_absent(ingest::RESERVED_PROJECT_ID)
        .await?;
    pg.ensure_run_metrics_run_fk().await?;
    registry_reconcile::reconcile(&ch, &pg).await?;

    let notifier_tx = match env::required_optional_string("KYMO_WATCHDOG_URL")? {
        Some(url) => {
            tracing::info!(%url, "watchdog notifier enabled");
            Some(notifier::spawn(url))
        }
        _ => {
            tracing::info!("KYMO_WATCHDOG_URL unset, watchdog notifier disabled");
            None
        }
    };

    // Version push channel: every change source sends, dashboard sockets
    // subscribe. Capacity is generous for the tiny payloads; a lagged
    // subscriber gets a `resync` push telling it to re-poll once.
    let (run_events, _) = tokio::sync::broadcast::channel::<events::VersionEvent>(256);
    let lifecycle_gates = lifecycle::LifecycleGates::new();
    let lifecycle_mutations = lifecycle::LifecycleMutationBarrier::default();
    let activity = match &transport {
        transport::TransportConfig::Hosted { .. } => activity::ActivityTracker::disabled(),
        transport::TransportConfig::Local { .. } => activity::ActivityTracker::new_local(),
    };

    // Derived liveness transitions (a run going silent) have no triggering
    // event, so the server watches for them and pushes project bumps.
    query::spawn_status_watcher(pg.clone(), run_events.clone());

    // Coalesces ingest-driven version/liveness bumps and first-seen metric
    // registrations into batched Postgres writes per interval, off the
    // ingest ACK path.
    let bumps = ingest::BumpCoalescer::spawn(
        pg.clone(),
        run_events.clone(),
        ch.series_cache(),
        activity.clone(),
    );
    let shutdown_bumps = bumps.clone();

    // Arc-shared between the native gRPC server and the browser-only WebSocket proxy listener.
    let service = Arc::new(KymoService {
        ingest: ingest::IngestService::new(
            ch.clone(),
            bumps.clone(),
            pg.clone(),
            lifecycle_gates.clone(),
        ),
        import: import::ImportService::new(
            ch.clone(),
            pg.clone(),
            lifecycle_gates.clone(),
            activity.clone(),
        ),
        query: Arc::new(query::QueryService::new(
            ch.clone(),
            pg.clone(),
            bumps,
            lifecycle_gates.clone(),
            notifier_tx,
            run_events.clone(),
        )),
        lifecycle_mutations,
        activity: activity.clone(),
        run_events,
    });

    // Validate all browser-facing admission configuration before starting the
    // destructive worker. A bad origin value must fail startup without giving
    // an enabled reaper even a brief head start.
    let browser_origins = match &transport {
        transport::TransportConfig::Hosted { .. } => ws_proxy::AllowedOrigins::from_env()?,
        transport::TransportConfig::Local { dashboard_addr, .. } => {
            ws_proxy::AllowedOrigins::parse(
                &http_transport::loopback_authorities(dashboard_addr.port())
                    .map(|authority| format!("http://{authority}"))
                    .join(","),
            )?
        }
    };
    tracing::info!(
        allowed_origins = %browser_origins.display(),
        "browser origin policy configured"
    );
    let reaper_config = match &transport {
        transport::TransportConfig::Hosted { .. } => deletion::ReaperConfig::from_env()?,
        transport::TransportConfig::Local { .. } => deletion::ReaperConfig::local(),
    };
    deletion::DeletionReaper::new(
        pg,
        ch.clone(),
        lifecycle_gates,
        service.run_events.clone(),
        reaper_config,
        activity.clone(),
    )
    .spawn();

    let cdn_root = PathBuf::from(env::string_or("KYMO_CDN_ROOT", "/data/cdn"));
    // The gcs mode has no CDN volume: nothing to create, and the disk gauge is replaced by the collector's bucket inventory gauges (docs/cdn-gcs-migration.md § Observability).
    let (cdn_store, cdn_disk, cdn_uploads) = match gcs {
        None => {
            tokio::fs::create_dir_all(&cdn_root).await?;
            let fs = cdn_store::FsStore::new(cdn_root.clone());
            (cdn_store::CdnStore::Fs(fs), Some(cdn_root), None)
        }
        Some((gcs, gc_config)) => {
            let (uploads, collector) = cdn_gc::start(gc_config, gcs.reads(), ch.clone()).await?;
            collector.spawn();
            (cdn_store::CdnStore::Gcs(gcs), None, Some(uploads))
        }
    };

    // ClickHouse's exporter does not cover the CDN volume or this container's
    // cgroup pressure, so publish those gauges from the server process.
    sys_stats::spawn_metrics(cdn_disk, ch);

    // Startup schema work and reconciliation can be long on a recovered laptop. Seed from server-ready, not process construction.
    activity.mark_ready();

    http_transport::serve(
        transport,
        service,
        local_security,
        shutdown_bumps,
        browser_origins,
        Arc::new(cdn::CdnState {
            store: cdn_store,
            activity,
            uploads: cdn_uploads,
        }),
        prom_handle,
    )
    .await
}

#[cfg(test)]
mod cdn_backend_mode_tests {
    use super::CdnBackendMode;
    use std::ffi::OsStr;

    #[test]
    fn only_absent_or_known_modes_pass() {
        assert_eq!(
            super::cdn_backend_mode(None).unwrap(),
            CdnBackendMode::Filesystem
        );
        for (raw, mode) in [
            ("filesystem", CdnBackendMode::Filesystem),
            ("gcs", CdnBackendMode::Gcs),
        ] {
            assert_eq!(
                super::cdn_backend_mode(Some(OsStr::new(raw))).unwrap(),
                mode
            );
        }
        for bad in [
            "",
            "pvc",
            "PVC",
            " filesystem",
            "dual",
            "gcs_first",
            "gcs_frozen",
            "both",
        ] {
            assert!(
                super::cdn_backend_mode(Some(OsStr::new(bad))).is_err(),
                "accepted {bad:?}"
            );
        }
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            assert!(super::cdn_backend_mode(Some(OsStr::from_bytes(b"\xff"))).is_err());
        }
    }
}
