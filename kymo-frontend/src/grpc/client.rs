use super::proto::*;
use super::routes;
use super::ws::WsClient;

/// The dashboard's RPC client. All unary calls ride the shared WebSocket
/// transport (see ws.rs).
#[derive(Clone)]
pub struct GrpcClient {
    ws: WsClient,
}

impl GrpcClient {
    pub fn new() -> Self {
        Self {
            ws: WsClient::singleton(),
        }
    }

    pub async fn list_projects(&self) -> Result<ListProjectsResponse, tonic::Status> {
        self.ws
            .unary_route(routes::LIST_PROJECTS, ListProjectsRequest {})
            .await
    }

    pub async fn list_runs(&self, project_id: &str) -> Result<ListRunsResponse, tonic::Status> {
        self.ws
            .unary_route(
                routes::LIST_RUNS,
                ListRunsRequest {
                    project_id: project_id.to_string(),
                },
            )
            .await
    }

    pub async fn rename_run(
        &self,
        project_id: &str,
        run_id: &str,
        run_name: &str,
    ) -> Result<RenameRunResponse, tonic::Status> {
        self.ws
            .unary_route_no_replay(
                routes::RENAME_RUN,
                RenameRunRequest {
                    project_id: project_id.to_string(),
                    run_id: run_id.to_string(),
                    run_name: run_name.to_string(),
                },
            )
            .await
    }

    pub async fn trash_runs(
        &self,
        project_id: &str,
        run_ids: &[String],
    ) -> Result<TrashRunsResponse, tonic::Status> {
        self.ws
            .unary_route_no_replay(
                routes::TRASH_RUNS,
                TrashRunsRequest {
                    project_id: project_id.to_string(),
                    run_ids: run_ids.to_vec(),
                },
            )
            .await
    }

    pub async fn restore_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> Result<RestoreRunResponse, tonic::Status> {
        self.ws
            .unary_route_no_replay(
                routes::RESTORE_RUN,
                RestoreRunRequest {
                    project_id: project_id.to_string(),
                    run_id: run_id.to_string(),
                },
            )
            .await
    }

    pub async fn list_trash(
        &self,
        request: ListTrashRequest,
    ) -> Result<ListTrashResponse, tonic::Status> {
        self.ws.unary_route(routes::LIST_TRASH, request).await
    }

    /// Finite, at-most-once read used only after a mutation outcome is
    /// unknown. A disconnection should release the UI for manual recovery,
    /// not leave the action busy until the socket eventually reconnects.
    pub async fn reconcile_trash(
        &self,
        request: ListTrashRequest,
    ) -> Result<ListTrashResponse, tonic::Status> {
        self.ws
            .unary_route_no_replay(routes::LIST_TRASH, request)
            .await
    }

    pub async fn get_run(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> Result<GetRunResponse, tonic::Status> {
        self.ws
            .unary_route(
                routes::GET_RUN,
                GetRunRequest {
                    project_id: project_id.to_string(),
                    run_id: run_id.to_string(),
                },
            )
            .await
    }

    pub async fn list_metrics(
        &self,
        project_id: &str,
        run_id: &str,
    ) -> Result<Vec<MetricInfo>, tonic::Status> {
        let resp: ListMetricsResponse = self
            .ws
            .unary_route(
                routes::LIST_METRICS,
                ListMetricsRequest {
                    project_id: project_id.to_string(),
                    run_id: run_id.to_string(),
                },
            )
            .await?;
        Ok(resp.metrics)
    }

    /// Discovery for a run SET in one round trip: distinct metric names
    /// across `run_ids`, types collapsed server-side by the registry's
    /// CDN < NUMERIC < TEXT_STREAM precedence.
    pub async fn list_run_set_metrics(
        &self,
        project_id: &str,
        run_ids: &[String],
    ) -> Result<Vec<MetricInfo>, tonic::Status> {
        // No runs => no metrics, without a round trip (the loader hits this
        // whenever nothing is selected).
        if run_ids.is_empty() {
            return Ok(Vec::new());
        }
        let resp: ListMetricsResponse = self
            .ws
            .unary_route(
                routes::LIST_RUN_SET_METRICS,
                ListRunSetMetricsRequest {
                    project_id: project_id.to_string(),
                    run_ids: run_ids.to_vec(),
                },
            )
            .await?;
        Ok(resp.metrics)
    }

    pub async fn query_chart(&self, request: ChartRequest) -> Result<ChartResponse, tonic::Status> {
        self.ws.unary_route(routes::QUERY_CHART, request).await
    }

    /// Batched CDN-key lookup. One round-trip covers any number of
    /// (project_id, run_id, metric_name) tuples. The response carries one
    /// CdnSeries per input ref, in the same order, with empty entries for
    /// refs that have no CDN data, plus the data-version echo
    /// (QueryCdnKeysResponse.run_versions).
    pub async fn query_cdn_keys(
        &self,
        refs: Vec<SeriesRef>,
    ) -> Result<QueryCdnKeysResponse, tonic::Status> {
        self.ws
            .unary_route(
                routes::QUERY_CDN_KEYS,
                QueryCdnKeysRequest {
                    refs,
                    step_min: None,
                    step_max: None,
                },
            )
            .await
    }

    pub async fn query_text_window(
        &self,
        project_id: &str,
        run_id: &str,
        metric_names: &[String],
        line_offset: u64,
        line_limit: u32,
        search: &str,
    ) -> Result<QueryTextWindowResponse, tonic::Status> {
        self.ws
            .unary_route(
                routes::QUERY_TEXT_WINDOW,
                QueryTextWindowRequest {
                    project_id: project_id.to_string(),
                    run_id: run_id.to_string(),
                    metric_names: metric_names.to_vec(),
                    line_offset,
                    line_limit,
                    search: search.to_string(),
                },
            )
            .await
    }

    pub async fn poll_versions(
        &self,
        project_id: Option<&str>,
        run_ids: &[String],
    ) -> Result<PollVersionsResponse, tonic::Status> {
        self.ws
            .unary_route(
                routes::POLL_VERSIONS,
                PollVersionsRequest {
                    project_id: project_id.map(|s| s.to_string()),
                    run_ids: run_ids.to_vec(),
                },
            )
            .await
    }
}
