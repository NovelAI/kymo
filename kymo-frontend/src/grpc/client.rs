use prost::Message;

use super::proto::*;
use super::routes;
use super::ws::WsClient;
use super::ws_rpc;

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

    /// Batched CDN-key lookup: one round trip for a gallery's (project_id, run_id, metric_name) tuples, as many as fit the request cap ([`cdn_keys_request_fits`]). The response carries one CdnSeries per input ref, in the same order, with empty entries for refs that have no CDN data, plus the data-version echo (QueryCdnKeysResponse.run_versions).
    pub async fn query_cdn_keys(
        &self,
        refs: Vec<SeriesRef>,
    ) -> Result<QueryCdnKeysResponse, tonic::Status> {
        self.ws
            .unary_route(routes::QUERY_CDN_KEYS, cdn_keys_request(refs))
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

/// The request [`GrpcClient::query_cdn_keys`] sends, as [`cdn_keys_frame_len`] measures it.
fn cdn_keys_request(refs: Vec<SeriesRef>) -> QueryCdnKeysRequest {
    QueryCdnKeysRequest {
        refs,
        step_min: None,
        step_max: None,
    }
}

/// Whether a QueryCdnKeys naming `refs` is within the server's bounds: at most [`ws_rpc::MAX_REQUEST_RUN_IDS`] refs, in a frame of at most [`ws_rpc::MAX_REQUEST_BYTES`]. The frame is /grpc-ws's cap exactly, and at most 29 bytes stricter than the body caps of /grpc-ws2 and native gRPC.
pub fn cdn_keys_request_fits(refs: &[SeriesRef]) -> bool {
    refs.len() <= ws_rpc::MAX_REQUEST_RUN_IDS
        && cdn_keys_frame_len(refs) <= ws_rpc::MAX_REQUEST_BYTES
}

/// The request frame of [`GrpcClient::query_cdn_keys`] for `refs`, without encoding them: each ref takes a one-byte key, its length and itself.
fn cdn_keys_frame_len(refs: &[SeriesRef]) -> usize {
    let refs_len: usize = refs
        .iter()
        .map(|series| {
            let len = series.encoded_len();
            1 + prost::length_delimiter_len(len) + len
        })
        .sum();
    let body = cdn_keys_request(Vec::new()).encoded_len() + refs_len;
    ws_rpc::request_frame_len(routes::QUERY_CDN_KEYS.path, body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gallery(runs: usize, metrics: usize, metric_bytes: usize) -> Vec<SeriesRef> {
        (0..runs)
            .flat_map(|run| {
                (0..metrics).map(move |metric| SeriesRef {
                    project_id: "p".repeat(20),
                    run_id: format!("{run:032x}"),
                    metric_name: format!("{metric:0metric_bytes$}"),
                    tags: vec![],
                })
            })
            .collect()
    }

    /// The check measures the frame the socket would send and counts the refs, holding back a gallery of every run of a large project, by bytes or by count, while letting one at the Max runs cap through.
    #[test]
    fn cdn_key_requests_past_the_cap_are_held_back() {
        // 300-byte names take two-byte lengths.
        for refs in [vec![], gallery(3, 2, 40), gallery(2, 2, 300)] {
            let body = cdn_keys_request(refs.clone()).encode_to_vec();
            let frame = ws_rpc::encode_request(0, routes::QUERY_CDN_KEYS.path, &body, false);
            assert_eq!(cdn_keys_frame_len(&refs), frame.len());
        }
        assert!(cdn_keys_request_fits(&gallery(64, 4, 40)));
        let every_run = gallery(11_000, 4, 40);
        assert_eq!(cdn_keys_frame_len(&every_run), 4_400_029);
        assert!(!cdn_keys_request_fits(&every_run));
        // Short run ids: 100,004 refs in 1.5 MB.
        let short_ids: Vec<_> = (0..25_001)
            .flat_map(|run| {
                (0..4).map(move |metric| SeriesRef {
                    project_id: "p".into(),
                    run_id: format!("{run:05}"),
                    metric_name: metric.to_string(),
                    tags: vec![],
                })
            })
            .collect();
        assert!(cdn_keys_frame_len(&short_ids) <= ws_rpc::MAX_REQUEST_BYTES);
        assert!(!cdn_keys_request_fits(&short_ids));
        assert!(cdn_keys_request_fits(
            &short_ids[..ws_rpc::MAX_REQUEST_RUN_IDS]
        ));
    }
}
