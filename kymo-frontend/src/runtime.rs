use std::sync::OnceLock;

#[cfg(feature = "local-runtime")]
use serde::Deserialize;

#[cfg(feature = "local-runtime")]
#[derive(Clone, Debug, Deserialize)]
struct LocalConfig {
    websocket_path: String,
    cdn_origin: String,
}

#[derive(Clone, Debug)]
pub(crate) struct RuntimeConfig {
    pub(crate) websocket_url: String,
    pub(crate) cdn_origin: String,
}

static CONFIG: OnceLock<RuntimeConfig> = OnceLock::new();

pub(crate) fn initialize() -> Result<(), String> {
    if CONFIG.get().is_none() {
        let _ = CONFIG.set(load()?);
    }
    Ok(())
}

pub(crate) fn config() -> &'static RuntimeConfig {
    CONFIG.get().expect("runtime configuration initialized")
}

/// Hosted builds compile in the server's HTTP origin, e.g. `http://kymo.example:8080`; the WebSocket, media, and alerts share that listener.
#[cfg(not(feature = "local-runtime"))]
fn load() -> Result<RuntimeConfig, String> {
    hosted_config(option_env!("KYMO_FRONTEND_SERVER_ORIGIN").unwrap_or_default())
}

#[cfg(not(feature = "local-runtime"))]
fn hosted_config(origin: &str) -> Result<RuntimeConfig, String> {
    let origin = origin.trim_end_matches('/');
    let websocket_origin = match origin.split_once("://") {
        Some(("http", host)) if !host.is_empty() => format!("ws://{host}"),
        Some(("https", host)) if !host.is_empty() => format!("wss://{host}"),
        _ => return Err("This dashboard was built without its server origin. Rebuild it with KYMO_FRONTEND_SERVER_ORIGIN set, e.g. http://kymo.example:8080.".to_owned()),
    };
    Ok(RuntimeConfig {
        websocket_url: format!("{websocket_origin}/grpc-ws"),
        cdn_origin: origin.to_owned(),
    })
}

/// Every CDN request goes through here. The collector keeps metric-row roots and the manifest items `resources()` yields (kymo/shared/cdn_manifest.rs); any other kind of link needs a matching collector change, plus a `LINKS_VERSION` bump if it comes from a manifest, or the collector may delete what it links.
pub(crate) fn cdn_url(key: &str) -> String {
    format!("{}/cdn/{}", config().cdn_origin, cdn_path_segment(key))
}

/// The key percent-encoded whole, so the request names exactly this key. Raw, a hand-written key's `?`, `#`, `%xx`, or `/` would make the browser fetch a different object than the key names — one the server's CDN collector doesn't count as referenced. Valid keys (hex, a dot, an extension) encode to themselves.
fn cdn_path_segment(key: &str) -> String {
    use dioxus::prelude::dioxus_router::exports::percent_encoding::{
        utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC,
    };
    const UNRESERVED: &AsciiSet = &NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'.')
        .remove(b'_')
        .remove(b'~');
    utf8_percent_encode(key, UNRESERVED).to_string()
}

/// The server's firing-alerts proxy lives on the same listener as the CDN (see components/notice_bar.rs).
pub(crate) fn alerts_url() -> String {
    format!("{}/alerts", config().cdn_origin)
}

/// Where the bar posts its answer `choice` ("delete" or "keep") to the local collector's question with id `question`, on the same listener (see components/notice_bar.rs).
pub(crate) fn media_cleanup_url(question: u64, choice: &str) -> String {
    format!("{}/media-cleanup/{question}/{choice}", config().cdn_origin)
}

#[cfg(feature = "local-runtime")]
fn load() -> Result<RuntimeConfig, String> {
    let window = web_sys::window().ok_or("browser window is unavailable")?;
    let document = window.document().ok_or("browser document is unavailable")?;
    let encoded = document
        .get_element_by_id("kymo-runtime-config")
        .and_then(|element| element.text_content())
        .ok_or("local dashboard configuration is missing")?;
    let local: LocalConfig = serde_json::from_str(&encoded)
        .map_err(|error| format!("invalid local dashboard configuration: {error}"))?;
    if local.websocket_path != "/trash/_kymo-grpc-ws-local-v1"
        || !valid_loopback_origin(&local.cdn_origin)
    {
        return Err("invalid local dashboard endpoint configuration".to_owned());
    }
    let location = window.location();
    let protocol = location
        .protocol()
        .map_err(|_| "dashboard origin has no protocol")?;
    let ws_scheme = match protocol.as_str() {
        "http:" => "ws",
        "https:" => "wss",
        _ => return Err("dashboard origin is not HTTP".to_owned()),
    };
    let host = location
        .host()
        .map_err(|_| "dashboard origin has no host")?;
    Ok(RuntimeConfig {
        websocket_url: format!("{ws_scheme}://{host}{}", local.websocket_path),
        cdn_origin: local.cdn_origin,
    })
}

#[cfg(feature = "local-runtime")]
fn valid_loopback_origin(value: &str) -> bool {
    value
        .strip_prefix("http://127.0.0.1:")
        .and_then(|port| port.parse::<u16>().ok())
        .is_some_and(|port| port >= 1024)
}

#[cfg(test)]
mod cdn_url_tests {
    use super::cdn_path_segment;

    #[test]
    fn keys_are_requested_exactly_as_named() {
        let key = format!("{}.png", "ab".repeat(32));
        assert_eq!(cdn_path_segment(&key), key);
        assert_eq!(
            cdn_path_segment("abcd.json?view=1#x"),
            "abcd.json%3Fview%3D1%23x"
        );
        assert_eq!(cdn_path_segment("a/../abcd.png"), "a%2F..%2Fabcd.png");
        assert_eq!(cdn_path_segment("abcd.p%6Eg\t"), "abcd.p%256Eg%09");
        assert_eq!(cdn_path_segment("é"), "%C3%A9");
    }
}

#[cfg(all(test, feature = "local-runtime"))]
mod tests {
    use super::valid_loopback_origin;

    #[test]
    fn local_cdn_origin_is_exact_numeric_loopback_with_a_high_port() {
        assert!(valid_loopback_origin("http://127.0.0.1:49152"));
        for value in [
            "http://127.0.0.1:0",
            "http://127.0.0.1:80",
            "http://localhost:49152",
            "https://127.0.0.1:49152",
            "http://127.0.0.1:49152/path",
        ] {
            assert!(!valid_loopback_origin(value), "accepted {value:?}");
        }
    }
}

#[cfg(all(test, not(feature = "local-runtime")))]
mod hosted_tests {
    use super::hosted_config;

    #[test]
    fn hosted_origin_derives_the_websocket_and_rejects_the_rest() {
        let plain = hosted_config("http://kymo.example:8080/").unwrap();
        assert_eq!(plain.websocket_url, "ws://kymo.example:8080/grpc-ws");
        assert_eq!(plain.cdn_origin, "http://kymo.example:8080");
        assert_eq!(
            hosted_config("https://kymo.example").unwrap().websocket_url,
            "wss://kymo.example/grpc-ws"
        );
        for bad in ["", "kymo.example:8080", "http://", "ftp://kymo.example"] {
            assert!(hosted_config(bad).is_err(), "accepted {bad:?}");
        }
    }
}
