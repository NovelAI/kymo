//! Firing alerts for the frontend notice bar. Prometheus (the chart's own instance, in-cluster
//! only) evaluates the rules; this is the one browser-facing surface for them: `GET /alerts` on
//! the hosted CDN listener proxies Prometheus's `/api/v1/alerts`, keeps only alerts that are
//! firing AND labeled `audience=user`, and returns an allowlisted DTO — nothing else from the
//! Prometheus payload crosses the wire. Delivery is the frontend by decision (no pager, no Slack;
//! docs/cdn-gcs-migration.md observability). A short cache makes upstream load one request per
//! interval regardless of open tabs. A failed Prometheus fetch (unreachable, non-2xx, malformed) is a 503,
//! never an empty list: "unknown" and "nothing firing" differ, and the frontend keeps its last set
//! through a 503, so a firing alert survives a Prometheus restart; an outage logs one warn and one
//! info on recovery. `KYMO_PROMETHEUS_URL` unset disables the proxy, but the route stays mounted
//! and answers `[]` so the frontend never meets a CORS-less 404 (the local runtime mounts neither
//! this router nor the poll).

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use http::{Method, StatusCode};
use serde::Serialize;
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

use crate::ws_proxy::AllowedOrigins;

const PROMETHEUS_URL_ENV: &str = "KYMO_PROMETHEUS_URL";
const ALERTS_PATH: &str = "/alerts";
/// Rules evaluate every 15s and the frontend polls every 30s; a 10s cache keeps upstream load at
/// one request per interval however many tabs poll.
const CACHE_TTL: Duration = Duration::from_secs(10);
/// Total per fetch, connect included.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// The allowlist: exactly what the banner shows. Descriptions stay in Grafana (operator detail).
#[derive(Clone, Serialize)]
struct UserAlert {
    name: String,
    summary: String,
    class: Option<String>,
    /// Prometheus's `activeAt`: the frontend keys dismissal on it, so a cleared-then-refired
    /// alert reappears.
    active_at: String,
}

/// The last refresh: when it ran and what it produced (`None` = the fetch failed).
#[derive(Default)]
struct Cache {
    fetched: Option<(Instant, Option<Vec<UserAlert>>)>,
}

pub(crate) struct AlertsState {
    http: reqwest::Client,
    /// `{base}/api/v1/alerts`.
    url: String,
    cache: Mutex<Cache>,
}

impl AlertsState {
    /// `None` when the env is unset (route disabled); a set but malformed URL is a startup error.
    pub(crate) fn from_env() -> anyhow::Result<Option<Arc<Self>>> {
        let Some(base) = crate::env::optional_string(PROMETHEUS_URL_ENV) else {
            tracing::info!("alert proxy disabled ({PROMETHEUS_URL_ENV} unset)");
            return Ok(None);
        };
        let state = Self::new(&base)?;
        tracing::info!(url = %state.url, "alert proxy configured");
        Ok(Some(Arc::new(state)))
    }

    fn new(base: &str) -> anyhow::Result<Self> {
        // Do not echo the URL until userinfo has been rejected (the endpoint is logged).
        let parsed = url::Url::parse(base)
            .map_err(|error| anyhow::anyhow!("{PROMETHEUS_URL_ENV} does not parse: {error}"))?;
        anyhow::ensure!(
            parsed.username().is_empty() && parsed.password().is_none(),
            "{PROMETHEUS_URL_ENV} must not carry userinfo"
        );
        anyhow::ensure!(
            matches!(parsed.scheme(), "http" | "https"),
            "{PROMETHEUS_URL_ENV} must be an http(s) URL, got {base:?}"
        );
        anyhow::ensure!(
            parsed.query().is_none() && parsed.fragment().is_none(),
            "{PROMETHEUS_URL_ENV} must be a bare base URL (no query or fragment), got {base:?}"
        );
        // Built through the URL's path, so a prefixed base (`/prom`) keeps its prefix and a
        // trailing slash never doubles.
        let mut endpoint = parsed.clone();
        endpoint.set_path(&format!(
            "{}/api/v1/alerts",
            parsed.path().trim_end_matches('/')
        ));
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()?,
            url: endpoint.to_string(),
            cache: Mutex::new(Cache::default()),
        })
    }

    /// Refreshes at most once per [`CACHE_TTL`], failures included. The async lock is held across
    /// the refresh, so concurrent polls on an expired cache share the one timeout-bounded fetch
    /// (no stampede, no older response overwriting a newer one).
    async fn firing(&self) -> Option<Vec<UserAlert>> {
        let mut cache = self.cache.lock().await;
        if let Some((at, outcome)) = &cache.fetched {
            if at.elapsed() < CACHE_TTL {
                return outcome.clone();
            }
        }
        let was_failing = matches!(cache.fetched, Some((_, None)));
        let outcome = match self.fetch().await {
            Ok(alerts) => {
                if was_failing {
                    tracing::info!(url = %self.url, "alert proxy: Prometheus fetch succeeded again");
                }
                Some(alerts)
            }
            Err(error) => {
                if !was_failing {
                    tracing::warn!(url = %self.url, error = %error, "alert proxy: Prometheus fetch failed; answering 503 until it recovers");
                }
                None
            }
        };
        cache.fetched = Some((Instant::now(), outcome.clone()));
        outcome
    }

    async fn fetch(&self) -> anyhow::Result<Vec<UserAlert>> {
        let response = self.http.get(&self.url).send().await?;
        anyhow::ensure!(
            response.status().is_success(),
            "status {}",
            response.status()
        );
        select_user_alerts(&response.text().await?)
    }
}

/// Firing alerts labeled `audience=user`, projected onto the allowlist, in a stable order.
fn select_user_alerts(body: &str) -> anyhow::Result<Vec<UserAlert>> {
    let parsed: serde_json::Value = serde_json::from_str(body)?;
    let alerts = parsed["data"]["alerts"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("response has no data.alerts array"))?;
    let text = |v: &serde_json::Value| v.as_str().unwrap_or("").to_owned();
    let mut selected: Vec<UserAlert> = alerts
        .iter()
        .filter(|a| a["state"] == "firing" && a["labels"]["audience"] == "user")
        .map(|a| UserAlert {
            name: text(&a["labels"]["alertname"]),
            summary: text(&a["annotations"]["summary"]),
            class: a["labels"]["class"].as_str().map(str::to_owned),
            active_at: text(&a["activeAt"]),
        })
        .collect();
    selected.sort_by(|x, y| (&x.name, &x.class).cmp(&(&y.name, &y.class)));
    Ok(selected)
}

async fn get_alerts(
    State(state): State<Option<Arc<AlertsState>>>,
) -> Result<Json<Vec<UserAlert>>, StatusCode> {
    match state {
        Some(state) => state
            .firing()
            .await
            .map(Json)
            .ok_or(StatusCode::SERVICE_UNAVAILABLE),
        None => Ok(Json(Vec::new())),
    }
}

/// The `/alerts` sub-router — always mounted on the hosted listener (disabled ⇒ `[]`), fenced to
/// the browser-origin allowlist by CORS (the CDN routes' open CORS is for media).
pub(crate) fn router(state: Option<Arc<AlertsState>>, browser_origins: &AllowedOrigins) -> Router {
    Router::new()
        .route(ALERTS_PATH, get(get_alerts))
        .layer(
            CorsLayer::new()
                .allow_origin(browser_origins.cors_policy())
                .allow_methods([Method::GET]),
        )
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::body::{to_bytes, Body};
    use http::{header, Request};
    use tower::ServiceExt;

    use super::*;

    const FIXTURE: &str = r#"{"status":"success","data":{"alerts":[
      {"labels":{"alertname":"Mkdb2CdnGcsTerminalFailures","audience":"user","class":"http_4xx","severity":"warning"},
       "annotations":{"summary":"some mkdb2 media storage operations failed terminally on the server side (class http_4xx)","description":"operator detail"},
       "state":"firing","activeAt":"2026-09-02T06:00:00Z","value":"1e+00"},
      {"labels":{"alertname":"Mkdb2CdnGcsReadErrors","audience":"user","severity":"warning"},
       "annotations":{"summary":"pending, must not show"},"state":"pending","activeAt":"2026-09-02T06:01:00Z","value":"1"},
      {"labels":{"alertname":"Mkdb2RunReaperWorkerDown","severity":"critical"},
       "annotations":{"summary":"no audience label, must not show"},"state":"firing","activeAt":"2026-09-02T05:00:00Z","value":"1"},
      {"labels":{"alertname":"Mkdb2CdnGcsServerErrors","audience":"user","severity":"warning"},
       "annotations":{"summary":"mkdb2 media storage (GCS) is returning server errors; operations are retrying"},
       "state":"firing","activeAt":"2026-09-02T06:02:00Z","value":"30"}
    ]}}"#;
    const ORIGIN: &str = "http://127.0.0.1:18080";

    #[test]
    fn selects_only_firing_user_alerts_in_stable_order_with_allowlisted_fields() {
        let alerts = select_user_alerts(FIXTURE).unwrap();
        assert_eq!(
            alerts.iter().map(|a| a.name.as_str()).collect::<Vec<_>>(),
            ["Mkdb2CdnGcsServerErrors", "Mkdb2CdnGcsTerminalFailures"]
        );
        let terminal = &alerts[1];
        assert_eq!(terminal.class.as_deref(), Some("http_4xx"));
        assert_eq!(terminal.active_at, "2026-09-02T06:00:00Z");
        assert!(terminal
            .summary
            .starts_with("some mkdb2 media storage operations failed"));
        let json = serde_json::to_value(terminal).unwrap();
        let mut keys: Vec<_> = json.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, ["active_at", "class", "name", "summary"]);
    }

    #[test]
    fn malformed_body_is_an_error_not_an_empty_success() {
        assert!(select_user_alerts("{}").is_err());
        assert!(select_user_alerts("not json").is_err());
    }

    #[test]
    fn env_url_must_be_http() {
        assert!(AlertsState::new("ftp://prometheus:9090").is_err());
        assert!(AlertsState::new("prometheus").is_err());
        assert!(AlertsState::new("http://prometheus:9090?tenant=x").is_err());
        assert!(AlertsState::new("http://prometheus:9090#frag").is_err());
        for with_credentials in [
            "http://user:hunter2@prometheus:9090",
            "http://user:hunter2@:9090",
        ] {
            let error = AlertsState::new(with_credentials)
                .err()
                .expect("credentials must be rejected")
                .to_string();
            assert!(!error.contains("hunter2"), "echoed the credential: {error}");
        }
        assert_eq!(
            AlertsState::new("http://prometheus-server:9090/")
                .unwrap()
                .url,
            "http://prometheus-server:9090/api/v1/alerts"
        );
        assert_eq!(
            AlertsState::new("http://prometheus:9090/prom").unwrap().url,
            "http://prometheus:9090/prom/api/v1/alerts"
        );
    }

    /// A tiny Prometheus stand-in: counts hits, serves the fixture (or a 500).
    async fn mock_prometheus(fail: bool) -> (String, Arc<AtomicUsize>) {
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        let app = Router::new().route(
            "/api/v1/alerts",
            get(move || {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    if fail {
                        (StatusCode::INTERNAL_SERVER_ERROR, String::new())
                    } else {
                        (StatusCode::OK, FIXTURE.to_owned())
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, hits)
    }

    fn app(base: &str) -> Router {
        router(
            Some(Arc::new(AlertsState::new(base).unwrap())),
            &AllowedOrigins::parse(ORIGIN).unwrap(),
        )
    }

    async fn poll(app: Router, origin: Option<&str>) -> http::Response<Body> {
        let mut request = Request::get(ALERTS_PATH);
        if let Some(origin) = origin {
            request = request.header(header::ORIGIN, origin);
        }
        app.oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn route_returns_the_dto_and_caches_across_polls() {
        let (base, hits) = mock_prometheus(false).await;
        let app = app(&base);
        let response = poll(app.clone(), Some(ORIGIN)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            ORIGIN
        );
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let alerts: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
        assert_eq!(alerts.len(), 2);
        assert_eq!(alerts[1]["class"], "http_4xx");
        assert!(
            alerts[1].get("description").is_none(),
            "descriptions never cross the wire"
        );
        // Two more polls inside the TTL — one from a foreign origin, which gets no CORS grant —
        // and one upstream request in total.
        let foreign = poll(app.clone(), Some("http://evil.example")).await;
        assert!(foreign
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none());
        poll(app, Some(ORIGIN)).await;
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn upstream_failure_is_a_503_not_an_empty_list() {
        let (base, hits) = mock_prometheus(true).await;
        let proxy = app(&base);
        let response = poll(proxy.clone(), Some(ORIGIN)).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        // The fence applies to the failure answer too: devtools shows a 503, not a CORS error.
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            ORIGIN
        );
        // A down Prometheus is still one request per TTL.
        poll(proxy, Some(ORIGIN)).await;
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_polls_on_a_cold_cache_fetch_once() {
        let (base, hits) = mock_prometheus(false).await;
        let app = app(&base);
        let responses = futures::future::join_all((0..8).map(|_| poll(app.clone(), None))).await;
        assert!(responses.iter().all(|r| r.status() == StatusCode::OK));
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn disabled_proxy_answers_an_empty_list_with_the_cors_fence() {
        let app = router(
            None,
            &AllowedOrigins::parse(&format!("{ORIGIN},http://localhost:*")).unwrap(),
        );
        let response = poll(app.clone(), Some(ORIGIN)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            ORIGIN
        );
        let body = to_bytes(response.into_body(), 64).await.unwrap();
        assert_eq!(&body[..], b"[]");
        // A loopback wildcard-port entry echoes the exact requesting origin.
        let response = poll(app, Some("http://localhost:4321")).await;
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "http://localhost:4321"
        );
    }
}
