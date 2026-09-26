use axum::body::Body;
use axum::http::{header, HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

mod generated {
    include!(concat!(env!("OUT_DIR"), "/frontend_assets.rs"));
}

pub(crate) fn available() -> bool {
    generated::AVAILABLE
}

#[derive(Clone, Serialize)]
pub(crate) struct LocalBrowserConfig {
    websocket_path: &'static str,
    cdn_origin: String,
}

impl LocalBrowserConfig {
    pub(crate) fn new(websocket_path: &'static str, cdn_origin: String) -> Self {
        Self {
            websocket_path,
            cdn_origin,
        }
    }
}

pub(crate) struct Asset {
    bytes: &'static [u8],
    etag: &'static str,
}

pub(crate) async fn serve(
    uri: Uri,
    headers: HeaderMap,
    axum::Extension(config): axum::Extension<LocalBrowserConfig>,
    axum::Extension(activity): axum::Extension<std::sync::Arc<crate::activity::ActivityTracker>>,
) -> Response {
    let requested = uri.path().trim_start_matches('/');
    let requested = if requested.is_empty() {
        "index.html"
    } else {
        requested
    };
    let Some((name, asset)) = select_asset(generated::ASSETS, requested) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if *name == "index.html" {
        activity.dashboard_page_served();
        return shell(asset.bytes, &config);
    }
    let response = Response::builder()
        .header(header::ETAG, asset.etag)
        .header(header::CACHE_CONTROL, "private, no-cache");
    let unchanged = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|tags| tags.split(',').any(|tag| tag.trim() == asset.etag));
    if unchanged {
        return response
            .status(StatusCode::NOT_MODIFIED)
            .body(Body::empty())
            .expect("static response headers are valid");
    }
    response
        .header(
            header::CONTENT_TYPE,
            mime_guess::from_path(name).first_or_octet_stream().as_ref(),
        )
        .body(Body::from(asset.bytes))
        .expect("static response headers are valid")
}

/// The shell carries per-install runtime config, so it is never cached.
fn shell(bytes: &[u8], config: &LocalBrowserConfig) -> Response {
    let Ok(index) = std::str::from_utf8(bytes) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let Ok(config) = serde_json::to_string(config) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let config = config.replace('<', "\\u003c");
    Response::builder()
        .header(header::CONTENT_TYPE, "text/html")
        .header(header::CACHE_CONTROL, "private, no-store")
        .body(Body::from(index.replacen(
            "</head>",
            &format!(
                r#"<script id="kymo-runtime-config" type="application/json">{config}</script></head>"#
            ),
            1,
        )))
        .expect("static response headers are valid")
}

fn select_asset<'a, T>(assets: &'a [(&'a str, T)], requested: &str) -> Option<&'a (&'a str, T)> {
    assets
        .iter()
        .find(|(candidate, _)| *candidate == requested)
        .or_else(|| {
            // Match hosted nginx's SPA fallback: project/run IDs are arbitrary path segments, including `assets` and names containing dots.
            assets
                .iter()
                .find(|(candidate, _)| *candidate == "index.html")
        })
}

#[cfg(test)]
mod tests {
    use super::select_asset;

    #[test]
    fn embedded_routes_match_hosted_exact_asset_then_spa_precedence() {
        let assets = [("index.html", 1), ("assets/app.js", 2)];
        assert_eq!(select_asset(&assets, "assets/app.js").unwrap().1, 2);
        for route in [
            "assets/project-run",
            "project.with.dot/run.with.dot",
            "_kymo/config.json",
        ] {
            assert_eq!(select_asset(&assets, route).unwrap().1, 1, "{route}");
        }
    }
}
