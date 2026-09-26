use std::sync::Arc;

use anyhow::Context;
use axum::extract::State;
use axum::routing::{get, post};
use axum::Router;
use http::Method;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Server;
use tonic_web::GrpcWebLayer;
use tower_http::cors::{Any, CorsLayer};

use crate::alerts;
use crate::cdn;
use crate::ingest;
use crate::local_auth;
use crate::local_control;
use crate::local_proto::local_runtime_control_server::LocalRuntimeControlServer;
use crate::proto::kymo_server::KymoServer;
use crate::transport;
use crate::ws_proxy;
use crate::KymoService;

const HOSTED_WS_PATH: &str = "/grpc-ws";
// `trash` is the one project ID the shared server already reserves; nesting local transport there steals no hosted-valid project/run URL.
const LOCAL_WS_PATH: &str = "/trash/_kymo-grpc-ws-local-v1";
const CDN_UPLOAD_PATH: &str = "/cdn/upload";
const CDN_RESOURCE_PATH: &str = "/cdn/{key}";

enum NativeEndpoint {
    Hosted {
        address: std::net::SocketAddr,
        browser_origins: ws_proxy::AllowedOrigins,
    },
    Local {
        path: std::path::PathBuf,
        auth: Arc<local_auth::LocalAuth>,
        lifecycle_auth: Arc<local_control::LifecycleAuth>,
        bumps: Arc<ingest::BumpCoalescer>,
        activity: Arc<crate::activity::ActivityTracker>,
    },
}

pub(crate) struct LocalSecurity {
    auth: Arc<local_auth::LocalAuth>,
    lifecycle_auth: Arc<local_control::LifecycleAuth>,
}

impl LocalSecurity {
    pub(crate) fn new(
        auth: Arc<local_auth::LocalAuth>,
        lifecycle_auth: Arc<local_control::LifecycleAuth>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !lifecycle_auth.conflicts_with(&auth),
            "local lifecycle credential must be distinct from the client credential"
        );
        Ok(Self {
            auth,
            lifecycle_auth,
        })
    }
}

pub(crate) async fn serve(
    config: transport::TransportConfig,
    service: Arc<KymoService>,
    security: Option<LocalSecurity>,
    bumps: Arc<ingest::BumpCoalescer>,
    browser_origins: ws_proxy::AllowedOrigins,
    cdn_state: Arc<cdn::CdnState>,
    prometheus: metrics_exporter_prometheus::PrometheusHandle,
) -> anyhow::Result<()> {
    // Declare guards before the tasks so Rust drops the listener-owning JoinSet first and only then unlinks its Unix socket paths.
    let mut unix_socket_guards = Vec::new();
    let mut http_tasks = tokio::task::JoinSet::new();
    let activity = cdn_state.activity.clone();
    let native = match config {
        transport::TransportConfig::Hosted {
            native_addr,
            cdn_addr,
            metrics_addr,
        } => {
            spawn_http(
                &mut http_tasks,
                "hosted CDN",
                tokio::net::TcpListener::bind(cdn_addr).await?,
                hosted_cdn_router(
                    cdn_state,
                    ws_proxy::WsState::hosted(service.clone(), browser_origins.clone()),
                    // Frontend alert delivery is hosted-only (local runtime has no Prometheus).
                    alerts::AlertsState::from_env()?,
                    &browser_origins,
                ),
            );
            spawn_http(
                &mut http_tasks,
                "metrics",
                tokio::net::TcpListener::bind(metrics_addr).await?,
                metrics_router(prometheus),
            );
            NativeEndpoint::Hosted {
                address: native_addr,
                browser_origins,
            }
        }
        transport::TransportConfig::Local {
            native_socket,
            upload_socket,
            dashboard_addr,
            dashboard_listener_fd,
            cdn_addr,
            cdn_listener_fd,
            ..
        } => {
            anyhow::ensure!(
                crate::embedded_frontend::available(),
                "local dashboard bundle is unavailable; rebuild with KYMO_FRONTEND_BUNDLE_DIR"
            );
            let security = security.context("local authentication is required")?;
            let auth = security.auth;
            spawn_http(
                &mut http_tasks,
                "local dashboard",
                transport::local_tcp_listener(
                    "local dashboard",
                    dashboard_addr,
                    dashboard_listener_fd,
                )?,
                local_dashboard_router(
                    ws_proxy::WsState::local(
                        service.clone(),
                        browser_origins.clone(),
                        activity.clone(),
                    ),
                    dashboard_addr,
                    cdn_addr,
                    activity.clone(),
                ),
            );
            spawn_http(
                &mut http_tasks,
                "local CDN",
                transport::local_tcp_listener("local CDN", cdn_addr, cdn_listener_fd)?,
                local_cdn_router(cdn_state.clone(), browser_origins.clone(), cdn_addr),
            );
            let (upload_listener, upload_guard) = transport::bind_unix_listener(&upload_socket)?;
            unix_socket_guards.push(upload_guard);
            spawn_http(
                &mut http_tasks,
                "local CDN upload",
                upload_listener,
                local_upload_router(cdn_state, auth.clone()),
            );
            NativeEndpoint::Local {
                path: native_socket,
                auth,
                lifecycle_auth: security.lifecycle_auth,
                bumps,
                activity,
            }
        }
    };

    // Hosted also fails the whole process when any sibling listener exits. Kubernetes can restart a complete pod; silently retaining only gRPC would leave a deceptively healthy deployment without its CDN/dashboard or metrics endpoint.
    tokio::select! {
        result = serve_native(native, service) => {
            result?;
            anyhow::bail!("native gRPC listener exited unexpectedly");
        },
        result = http_tasks.join_next() => match result {
            Some(Ok(Ok(()))) => anyhow::bail!("HTTP listener exited unexpectedly"),
            Some(Ok(Err(error))) => Err(error),
            Some(Err(error)) => Err(error.into()),
            None => anyhow::bail!("no HTTP listeners are running"),
        }
    }
}

fn metrics_router(prometheus: metrics_exporter_prometheus::PrometheusHandle) -> Router {
    Router::new().route(
        "/metrics",
        get(move || {
            let handle = prometheus.clone();
            async move { handle.render() }
        }),
    )
}

fn hosted_cdn_router(
    cdn_state: Arc<cdn::CdnState>,
    ws_state: ws_proxy::WsState,
    alerts: Option<Arc<alerts::AlertsState>>,
    browser_origins: &ws_proxy::AllowedOrigins,
) -> Router {
    let app = hosted_cdn_routes(cdn_state).merge(
        Router::new()
            .route(HOSTED_WS_PATH, get(ws_proxy::ws_handler))
            .with_state(ws_state),
    );
    app.merge(alerts::router(alerts, browser_origins))
}

fn hosted_cdn_routes(cdn_state: Arc<cdn::CdnState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_headers(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .expose_headers(Any);
    Router::new()
        .route(CDN_UPLOAD_PATH, post(cdn::upload))
        .route(CDN_RESOURCE_PATH, get(cdn::serve))
        .layer(axum::extract::DefaultBodyLimit::max(cdn::ENVELOPE_BYTES))
        .layer(cors)
        .with_state(cdn_state)
}

fn local_dashboard_router(
    ws_state: ws_proxy::WsState,
    dashboard_addr: std::net::SocketAddr,
    cdn_addr: std::net::SocketAddr,
    activity: Arc<crate::activity::ActivityTracker>,
) -> Router {
    Router::new()
        .route(LOCAL_WS_PATH, get(ws_proxy::ws_handler))
        .route("/", get(crate::embedded_frontend::serve))
        .route("/{*path}", get(crate::embedded_frontend::serve))
        .layer(axum::middleware::from_fn_with_state(
            activity.clone(),
            count_browser_work,
        ))
        .layer(axum::Extension(activity))
        .layer(axum::Extension(
            crate::embedded_frontend::LocalBrowserConfig::new(
                LOCAL_WS_PATH,
                format!("http://{cdn_addr}"),
            ),
        ))
        .layer(axum::middleware::from_fn_with_state(
            loopback_hosts(dashboard_addr),
            require_loopback_host,
        ))
        .with_state(ws_state)
}

fn local_cdn_router(
    cdn_state: Arc<cdn::CdnState>,
    browser_origins: ws_proxy::AllowedOrigins,
    cdn_addr: std::net::SocketAddr,
) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(browser_origins.cors_policy())
        .allow_methods([Method::GET, Method::OPTIONS]);
    let activity = cdn_state.activity.clone();
    Router::new()
        .route(CDN_RESOURCE_PATH, get(cdn::serve_local))
        .layer(cors)
        .layer(axum::middleware::from_fn_with_state(
            activity,
            count_browser_work,
        ))
        .layer(axum::middleware::from_fn_with_state(
            loopback_hosts(cdn_addr),
            require_loopback_host,
        ))
        .with_state(cdn_state)
}

async fn count_browser_work(
    State(activity): State<Arc<crate::activity::ActivityTracker>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let _work = activity.begin_work();
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        http::header::X_CONTENT_TYPE_OPTIONS,
        http::HeaderValue::from_static("nosniff"),
    );
    response
}

/// The two authorities a browser on this machine (or through a same-port SSH forward) uses for a loopback listener; the local listeners bind only 127.0.0.1.
pub(crate) fn loopback_authorities(port: u16) -> [String; 2] {
    [format!("127.0.0.1:{port}"), format!("localhost:{port}")]
}

fn loopback_hosts(address: std::net::SocketAddr) -> Arc<[String; 2]> {
    Arc::new(loopback_authorities(address.port()))
}

/// Any other Host is a rebound DNS name or a stray client, never this dashboard.
async fn require_loopback_host(
    State(hosts): State<Arc<[String; 2]>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let host = request
        .headers()
        .get(http::header::HOST)
        .and_then(|value| value.to_str().ok());
    if !host.is_some_and(|host| hosts.iter().any(|allowed| allowed == host)) {
        return local_auth::private_error(
            http::StatusCode::MISDIRECTED_REQUEST,
            "Host is not this local kymo listener",
        );
    }
    next.run(request).await
}

fn local_upload_router(cdn_state: Arc<cdn::CdnState>, auth: Arc<local_auth::LocalAuth>) -> Router {
    let admission = local_auth::UploadAdmission::new(auth, cdn_state.activity.clone());
    Router::new()
        .route(CDN_UPLOAD_PATH, post(cdn::upload))
        .layer(axum::extract::DefaultBodyLimit::max(cdn::ENVELOPE_BYTES))
        .layer(axum::middleware::from_fn_with_state(
            admission,
            local_auth::require_upload_bearer,
        ))
        .with_state(cdn_state)
}

fn spawn_http<L>(
    tasks: &mut tokio::task::JoinSet<anyhow::Result<()>>,
    name: &'static str,
    listener: L,
    app: Router,
) where
    L: axum::serve::Listener + Send + 'static,
    L::Addr: std::fmt::Debug,
{
    tracing::info!(address = ?listener.local_addr(), listener = name, "HTTP listener starting");
    tasks.spawn(async move {
        axum::serve(listener, app)
            .await
            .with_context(|| format!("{name} listener failed"))
    });
}

async fn serve_native(endpoint: NativeEndpoint, service: Arc<KymoService>) -> anyhow::Result<()> {
    let server =
        KymoServer::from_arc(service).max_decoding_message_size(ingest::MAX_GRPC_MESSAGE_BYTES);
    match endpoint {
        NativeEndpoint::Hosted {
            address,
            browser_origins,
        } => {
            tracing::info!(%address, "hosted native gRPC server starting");
            let cors = CorsLayer::new()
                .allow_origin(browser_origins.cors_policy())
                .allow_headers(Any)
                .allow_methods([Method::POST, Method::OPTIONS])
                .expose_headers(Any);
            Server::builder()
                .accept_http1(true)
                .layer(cors)
                .layer(GrpcWebLayer::new())
                .add_service(server)
                .serve(address)
                .await?;
        }
        NativeEndpoint::Local {
            path,
            auth,
            lifecycle_auth,
            bumps,
            activity,
        } => {
            tracing::info!(socket = %path.display(), "local native gRPC server starting");
            let (listener, _socket_guard) = transport::bind_unix_listener(&path)?;
            let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
            Server::builder()
                .add_service(InterceptedService::new(
                    server,
                    local_auth::grpc_interceptor(auth),
                ))
                .add_service(InterceptedService::new(
                    LocalRuntimeControlServer::new(local_control::LocalRuntimeControlService::new(
                        bumps, activity,
                    )),
                    lifecycle_auth.interceptor(),
                ))
                .serve_with_incoming(incoming)
                .await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::body::{to_bytes, Body};
    use axum::extract::State;
    use http::{header, Request, StatusCode};
    use tower::ServiceExt;

    use super::*;

    const SERVER_TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const OTHER_TOKEN: &str = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCI";
    const DASHBOARD_ORIGIN: &str = "http://127.0.0.1:18080";
    const CDN_ADDR: &str = "127.0.0.1:18081";

    fn local_cdn(state: Arc<cdn::CdnState>) -> Router {
        local_cdn_router(
            state,
            ws_proxy::AllowedOrigins::parse(DASHBOARD_ORIGIN).unwrap(),
            CDN_ADDR.parse().unwrap(),
        )
    }

    /// Stands in for the Host header every browser request carries.
    fn with_cdn_host(app: Router) -> Router {
        app.layer(axum::middleware::map_request(
            |mut request: Request<Body>| async move {
                request
                    .headers_mut()
                    .entry(header::HOST)
                    .or_insert(http::HeaderValue::from_static(CDN_ADDR));
                request
            },
        ))
    }

    fn auth() -> Arc<local_auth::LocalAuth> {
        local_auth::LocalAuth::testing(SERVER_TOKEN)
    }

    fn cdn_state(root: &std::path::Path) -> Arc<cdn::CdnState> {
        Arc::new(cdn::CdnState {
            store: crate::cdn_store::CdnStore::Fs(crate::cdn_store::FsStore::new(root.to_owned())),
            activity: crate::activity::ActivityTracker::new_local(),
        })
    }

    fn bearer(token: &str) -> http::HeaderValue {
        format!("Bearer {token}").parse().unwrap()
    }

    async fn upload(app: Router, token: Option<&str>) -> http::Response<Body> {
        let mut request = Request::post(CDN_UPLOAD_PATH)
            .header("x-extension", "png")
            .body(Body::from("payload"))
            .unwrap();
        if let Some(token) = token {
            request
                .headers_mut()
                .insert(header::AUTHORIZATION, bearer(token));
        }
        app.oneshot(request).await.unwrap()
    }

    async fn observed_upload_work(
        State(activity): State<Arc<crate::activity::ActivityTracker>>,
        _body: axum::body::Bytes,
    ) -> String {
        activity.snapshot().in_flight_work.to_string()
    }

    #[tokio::test]
    async fn local_upload_is_application_work_before_body_extraction() {
        let activity = crate::activity::ActivityTracker::new_local();
        let admission = local_auth::UploadAdmission::new(auth(), activity.clone());
        let app = Router::new()
            .route(CDN_UPLOAD_PATH, post(observed_upload_work))
            .layer(axum::middleware::from_fn_with_state(
                admission,
                local_auth::require_upload_bearer,
            ))
            .with_state(activity.clone());

        let response = upload(app.clone(), Some(SERVER_TOKEN)).await;
        assert_eq!(to_bytes(response.into_body(), 16).await.unwrap(), "1");
        assert_eq!(activity.snapshot().in_flight_work, 0);

        let response = upload(app, None).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(activity.snapshot().in_flight_work, 0);
    }

    #[tokio::test]
    async fn hosted_cdn_keeps_unauthenticated_upload_and_public_reads() {
        let temporary = tempfile::tempdir().unwrap();
        let app = hosted_cdn_routes(cdn_state(temporary.path()));
        let response = upload(app.clone(), None).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let resource_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()
            ["resource_id"]
            .as_str()
            .unwrap()
            .to_owned();

        let response = app
            .oneshot(
                Request::get(format!("/cdn/{resource_id}"))
                    .header(header::ORIGIN, "http://elsewhere.example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CACHE_CONTROL],
            "public, max-age=31536000, immutable"
        );
        // Media stays open-CORS (the alerts sub-router carries its own fence).
        assert_eq!(response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
    }

    #[tokio::test]
    async fn local_routes_keep_upload_private_and_browser_reads_simple() {
        let temporary = tempfile::tempdir().unwrap();
        let state = cdn_state(temporary.path());
        let upload_app = local_upload_router(state.clone(), auth());
        assert_eq!(
            upload(upload_app.clone(), None).await.status(),
            StatusCode::UNAUTHORIZED
        );
        let response = upload(upload_app, Some(SERVER_TOKEN)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let resource_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()
            ["resource_id"]
            .as_str()
            .unwrap()
            .to_owned();

        let app = with_cdn_host(local_cdn(state));
        for (origin, token, expected) in [
            (None, None, StatusCode::OK),
            (Some(DASHBOARD_ORIGIN), None, StatusCode::OK),
            (Some(DASHBOARD_ORIGIN), Some(SERVER_TOKEN), StatusCode::OK),
            (Some(DASHBOARD_ORIGIN), Some(OTHER_TOKEN), StatusCode::OK),
        ] {
            let mut request = Request::get(format!("/cdn/{resource_id}"))
                .body(Body::empty())
                .unwrap();
            if let Some(origin) = origin {
                request
                    .headers_mut()
                    .insert(header::ORIGIN, origin.parse().unwrap());
            }
            if let Some(token) = token {
                request
                    .headers_mut()
                    .insert(header::AUTHORIZATION, bearer(token));
            }
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), expected);
            assert_eq!(
                response.headers()[header::CACHE_CONTROL],
                "private, no-store"
            );
            assert_eq!(
                response.headers()[header::X_CONTENT_TYPE_OPTIONS],
                "nosniff"
            );
        }

        let response = app
            .clone()
            .oneshot(
                Request::options(format!("/cdn/{resource_id}"))
                    .header(header::ORIGIN, DASHBOARD_ORIGIN)
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, Method::GET.as_str())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            DASHBOARD_ORIGIN
        );

        let mut not_found_bodies = Vec::new();
        for key in ["abcd.png".to_owned(), format!("{}.png", "f".repeat(64))] {
            let response = app
                .clone()
                .oneshot(
                    Request::get(format!("/cdn/{key}"))
                        .header(header::ORIGIN, DASHBOARD_ORIGIN)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            not_found_bodies.push(to_bytes(response.into_body(), 1024).await.unwrap());
        }
        assert_eq!(not_found_bodies[0], not_found_bodies[1]);

        let response = app
            .clone()
            .oneshot(
                Request::post(CDN_UPLOAD_PATH)
                    .header(header::ORIGIN, "http://127.0.0.1:19999")
                    .header(header::AUTHORIZATION, bearer(OTHER_TOKEN))
                    .body(Body::from("must not be stored"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);

        for (method, path) in [
            (Method::POST, CDN_UPLOAD_PATH),
            (Method::GET, HOSTED_WS_PATH),
            (Method::GET, LOCAL_WS_PATH),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .header(header::ORIGIN, DASHBOARD_ORIGIN)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(matches!(
                response.status(),
                StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED
            ));
        }
    }

    #[tokio::test]
    async fn local_browser_listeners_answer_only_their_loopback_host_names() {
        let temporary = tempfile::tempdir().unwrap();
        let app = local_cdn(cdn_state(temporary.path()));
        for (host, expected) in [
            (Some(CDN_ADDR), StatusCode::NOT_FOUND),
            (Some("localhost:18081"), StatusCode::NOT_FOUND),
            (Some("127.0.0.1:18080"), StatusCode::MISDIRECTED_REQUEST),
            (
                Some("attacker.example:18081"),
                StatusCode::MISDIRECTED_REQUEST,
            ),
            (None, StatusCode::MISDIRECTED_REQUEST),
        ] {
            let mut request = Request::get(format!("/cdn/{}.png", "f".repeat(64)))
                .body(Body::empty())
                .unwrap();
            if let Some(host) = host {
                request
                    .headers_mut()
                    .insert(header::HOST, host.parse().unwrap());
            }
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), expected, "{host:?}");
        }
    }
}
