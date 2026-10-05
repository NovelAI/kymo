//! WebSocket transport for the WEB FRONTEND ONLY.
//!
//! Browsers cap plain-HTTP origins at ~6 connections and never use HTTP/2
//! without TLS, so the dashboard's parallel chart fetches serialize. This
//! route multiplexes the service's UNARY methods over one socket with
//! correlation ids — one connection, at most 64 in-flight requests.
//!
//! Nothing here touches the native gRPC path: kymo and every other client keep speaking gRPC over the hosted TCP or local Unix-socket transport, including the streaming IngestMetrics RPC. The dashboard's unary queries and its explicit Rename /
//! Trash / Restore controls are proxied; run execution mutations (InitRun and
//! TerminateRun) remain native-gRPC-only.
//!
//! Framing (binary, little-endian), mirrored by the frontend's grpc/ws.rs:
//!   request:  [u32 id][u16 path_len][path utf8][protobuf request bytes]
//!   response: [u32 id][u8 grpc code, 0 = OK][protobuf response bytes,
//!              or utf8 error message when code != 0]
//!
//! The format is frozen: open dashboard tabs keep their WASM bundle (and
//! this framing) until reload, so changing it in place strands them no
//! matter how the deploys are ordered. Evolve by mounting a new route
//! (/grpc-ws2) beside this one and pointing the frontend at it once the
//! server is live.
//!
//! One ADDITIVE extension rides the frozen format: correlation id 0 is
//! reserved for server-initiated push (a RunVersionsEvent payload with
//! code 0) — the dashboard's primary change signal. Visible tabs also run
//! a one-minute PollVersions backstop. Old bundles drop id-0 frames by
//! construction (no parked request matches). Pushes are coalesced per
//! connection to at most one frame per EVENT_COALESCE_MS carrying only the
//! counters that changed; logging rate cannot inflate this (versions bump
//! per ingest flush, ~2s per active run, not per point).
//!
//! The socket URL carries the dashboard's wire revision as `?rev=` (none counts as 1). Below MIN_FRONTEND_WIRE_REVISION every request frame is answered with Unavailable(RELOAD_REQUIRED), nothing is dispatched and nothing is pushed (ws_rpc.rs).

use std::sync::Arc;

use axum::extract::ws::{close_code, CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{RawQuery, State};
use axum::http::{header::ORIGIN, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::{SinkExt, StreamExt};
use prost::Message as _;
use tonic::Status;
use tower_http::cors::AllowOrigin;

use crate::local_auth::private_error;
use crate::proto;
use crate::proto::kymo_server::Kymo;
use crate::KymoService;

const ALLOWED_ORIGINS_ENV: &str = "KYMO_ALLOWED_ORIGINS";
// The binary default is development-only. Production owns its exact browser
// origins in deployment configuration rather than duplicating them here.
const DEFAULT_ALLOWED_ORIGINS: &str = "http://localhost:*,http://127.0.0.1:*";

/// One allowlist shared by the WebSocket upgrade and the CORS fences on `/alerts` and local CDN reads. Browser `Origin` values are serialized origins, not
/// URLs: a path, query, fragment, `*`, or trailing slash is a
/// configuration error rather than something we try to normalize. The one
/// wildcard is a loopback host with any port (`http://localhost:*`; rationale
/// in docs/run-project-deletion.md).
#[derive(Clone, Debug)]
pub(crate) struct AllowedOrigins(Arc<Vec<String>>);

const LOOPBACK_HOSTS: [&str; 3] = ["localhost", "127.0.0.1", "[::1]"];

impl AllowedOrigins {
    pub(crate) fn from_env() -> anyhow::Result<Self> {
        let configured =
            crate::env::required_string_or(ALLOWED_ORIGINS_ENV, DEFAULT_ALLOWED_ORIGINS)?;
        Self::parse(&configured)
    }

    pub(crate) fn parse(value: &str) -> anyhow::Result<Self> {
        let mut origins: Vec<String> = Vec::new();
        for raw in value.split(',') {
            let entry = raw.trim();
            anyhow::ensure!(
                !entry.is_empty(),
                "{ALLOWED_ORIGINS_ENV} contains an empty origin"
            );
            anyhow::ensure!(
                entry != "*",
                "{ALLOWED_ORIGINS_ENV} must list origins, not '*'"
            );
            let origin = entry.strip_suffix(":*").unwrap_or(entry);
            let url = url::Url::parse(origin)
                .map_err(|error| anyhow::anyhow!("invalid origin {entry:?}: {error}"))?;
            anyhow::ensure!(
                matches!(url.scheme(), "http" | "https"),
                "origin {entry:?} must have an http or https scheme and authority"
            );
            anyhow::ensure!(
                origin == entry
                    || (url.port().is_none()
                        && url
                            .host_str()
                            .is_some_and(|host| LOOPBACK_HOSTS.contains(&host))),
                "origin {entry:?}: a wildcard port is allowed only for loopback hosts ({}) and replaces the port",
                LOOPBACK_HOSTS.join(", ")
            );
            let canonical = url.origin().ascii_serialization();
            anyhow::ensure!(
                origin == canonical,
                "origin {entry:?} is not a canonical browser origin; use {:?}",
                if origin == entry {
                    canonical.clone()
                } else {
                    format!("{canonical}:*")
                }
            );
            if !origins.iter().any(|known| known == entry) {
                origins.push(entry.to_owned());
            }
        }
        Ok(Self(Arc::new(origins)))
    }

    /// An entry as written, or a `:*` entry's prefix followed by nothing (the scheme's default
    /// port) or a canonical `:<port>` (a browser never serializes leading zeros or an out-of-range port).
    fn permits(&self, origin: &HeaderValue) -> bool {
        let Ok(origin) = origin.to_str() else {
            return false;
        };
        self.0.iter().any(|entry| match entry.strip_suffix(":*") {
            None => entry == origin,
            Some(prefix) => origin.strip_prefix(prefix).is_some_and(|rest| {
                rest.is_empty()
                    || rest.strip_prefix(':').is_some_and(|port| {
                        port.parse::<u16>()
                            .is_ok_and(|p| p != 0 && p.to_string() == port)
                    })
            }),
        })
    }

    pub(crate) fn cors_policy(&self) -> AllowOrigin {
        let origins = self.clone();
        AllowOrigin::predicate(move |origin: &HeaderValue, _: &http::request::Parts| {
            origins.permits(origin)
        })
    }

    fn permits_headers(&self, headers: &HeaderMap) -> bool {
        let mut supplied = headers.get_all(ORIGIN).iter();
        let Some(origin) = supplied.next() else {
            // CLI/non-browser WebSocket clients normally omit Origin. The
            // allowlist is a browser cross-origin fence, not client auth.
            return true;
        };
        supplied.next().is_none() && self.permits(origin)
    }

    pub(crate) fn permits_exact_headers(&self, headers: &HeaderMap) -> bool {
        let mut supplied = headers.get_all(ORIGIN).iter();
        supplied
            .next()
            .is_some_and(|origin| supplied.next().is_none() && self.permits(origin))
    }

    pub(crate) fn display(&self) -> String {
        self.0.join(",")
    }
}

#[derive(Clone)]
pub(crate) struct WsState {
    service: Arc<KymoService>,
    allowed_origins: AllowedOrigins,
    mode: WsMode,
}

#[derive(Clone)]
enum WsMode {
    Hosted,
    Local {
        activity: Arc<crate::activity::ActivityTracker>,
    },
}

impl WsState {
    pub(crate) fn hosted(service: Arc<KymoService>, allowed_origins: AllowedOrigins) -> Self {
        Self {
            service,
            allowed_origins,
            mode: WsMode::Hosted,
        }
    }

    pub(crate) fn local(
        service: Arc<KymoService>,
        allowed_origins: AllowedOrigins,
        activity: Arc<crate::activity::ActivityTracker>,
    ) -> Self {
        Self {
            service,
            allowed_origins,
            mode: WsMode::Local { activity },
        }
    }
}

/// Parse a request frame. None on malformed input (the frame is dropped —
/// a client that can't frame correctly can't be answered by id either).
fn decode_request(buf: &[u8]) -> Option<(u32, &str, &[u8])> {
    let id = u32::from_le_bytes(buf.get(0..4)?.try_into().ok()?);
    let path_len = u16::from_le_bytes(buf.get(4..6)?.try_into().ok()?) as usize;
    let path = std::str::from_utf8(buf.get(6..6 + path_len)?).ok()?;
    let body = buf.get(6 + path_len..)?;
    Some((id, path, body))
}

fn encode_response(id: u32, result: &Result<Vec<u8>, Status>) -> Vec<u8> {
    let (code, payload): (u8, &[u8]) = match result {
        Ok(bytes) => (0, bytes),
        Err(status) => (status.code() as u8, status.message().as_bytes()),
    };
    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.extend_from_slice(&id.to_le_bytes());
    frame.push(code);
    frame.extend_from_slice(payload);
    frame
}

/// Coalesced version counters must retain the newest value even when two
/// post-commit publishers are scheduled out of order.
fn merge_latest_versions(
    into: &mut std::collections::HashMap<String, u64>,
    versions: impl IntoIterator<Item = (String, u64)>,
) {
    for (key, version) in versions {
        let current = into.entry(key).or_default();
        *current = (*current).max(version);
    }
}

/// A hidden tab does not need an exact replay of every entity that changed
/// while it was away. Drop the potentially unbounded detail and ask it for
/// one authoritative snapshot when it becomes visible again.
fn retain_only_resync(
    runs: &mut std::collections::HashMap<String, u64>,
    projects: &mut std::collections::HashMap<String, u64>,
    global: &mut u64,
    metrics: &mut std::collections::HashSet<String>,
    resync: &mut bool,
) {
    runs.clear();
    projects.clear();
    *global = 0;
    metrics.clear();
    *resync = true;
}

async fn dispatch(svc: &KymoService, path: &str, body: &[u8]) -> Result<Vec<u8>, Status> {
    macro_rules! unary {
        ($req:ty, $resp:ty, $method:ident) => {{
            let msg = <$req>::decode(body)
                .map_err(|e| Status::invalid_argument(format!("request decode: {e}")))?;
            let resp: tonic::Response<$resp> = Kymo::$method(svc, tonic::Request::new(msg)).await?;
            Ok(resp.into_inner().encode_to_vec())
        }};
    }
    macro_rules! dispatch_routes {
        ($(($name:ident, $request:ident, $response:ident, $method:ident)),* $(,)?) => {
            match path {
                $(
                    crate::ws_rpc::$name =>
                        unary!(proto::$request, proto::$response, $method),
                )*
                other => Err(Status::unimplemented(format!(
                    "not proxied over ws: {other}"
                ))),
            }
        };
    }
    crate::ws_rpc::browser_rpc_routes!(dispatch_routes)
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<WsState>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let local = matches!(&state.mode, WsMode::Local { .. });
    let origin_allowed = if local {
        state.allowed_origins.permits_exact_headers(&headers)
    } else {
        state.allowed_origins.permits_headers(&headers)
    };
    if !origin_allowed {
        tracing::warn!(origin = ?headers.get(ORIGIN), "rejected WebSocket origin");
        return private_error(StatusCode::FORBIDDEN, "WebSocket origin is not allowed");
    }
    // Subscribed before the upgrade, so shutdown waits for a socket it has already accepted. The flag only ever turns true, so the socket's `changed()` is the shutdown.
    let closing = state.service.ingest.subscribe_draining();
    // A socket opened now would only get the shutdown Close, and its tab would take the open for the server being back.
    if *closing.borrow() {
        return private_error(StatusCode::SERVICE_UNAVAILABLE, "server shutting down");
    }
    let activity = match &state.mode {
        WsMode::Local { activity } => Some(activity.clone()),
        WsMode::Hosted => None,
    };
    // Bundles without a valid `?rev=` count as revision 1.
    let rev = query
        .as_deref()
        .and_then(|q| q.split('&').find_map(|p| p.strip_prefix("rev=")))
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let refused = rev < crate::ws_rpc::MIN_FRONTEND_WIRE_REVISION;
    // Match native gRPC's transport-byte admission. This bounds one frame,
    // not the number of run ids accepted by a lifecycle request.
    ws.max_message_size(crate::ingest::MAX_GRPC_MESSAGE_BYTES)
        .on_upgrade(move |socket| async move {
            let _frontend = activity
                .as_ref()
                .map(|activity| activity.frontend_connected());
            handle_socket(socket, state.service, local, refused, closing).await;
        })
        .into_response()
}

/// Concurrent dispatches per socket. A dashboard tab fires its chart
/// queries in a burst (and re-bursts after a reconnect); past this cap the
/// reader stops taking frames until a slot frees, so one tab can't
/// fork-bomb ClickHouse.
const MAX_IN_FLIGHT: usize = 64;

/// Floor between push frames on one connection. Bumps arrive per ingest
/// flush (~2s cadence per active run); this only matters when several
/// runs' flushes stagger inside a second — they merge into one frame.
const EVENT_COALESCE_MS: u64 = 1_000;

/// Every socket is pinged: an idle one otherwise carries no bytes, and a proxy with an idle timeout (nginx's default is 60 s) closes it. Browsers answer pings natively, even for hidden tabs.
const KEEPALIVE_PING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(20);
/// Only local sockets are dropped after this much silence: they hold the stack up, so a peer that vanished without a close (a dropped SSH tunnel, a sleeping laptop) must be noticed. Dropping a silent hosted socket would only make every tab reconnect and resync after a sleep.
const KEEPALIVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

async fn handle_socket(
    socket: WebSocket,
    svc: Arc<KymoService>,
    drop_silent: bool,
    refused: bool,
    mut closing: tokio::sync::watch::Receiver<bool>,
) {
    let (mut sink, mut stream) = socket.split();
    // On shutdown the writer sends a Close and the reader waits for the browser's reply: a browser that sees the socket vanish first counts a failure and delays reconnecting to the restarted server. Shutdown waits (briefly) until both halves drop their subscriptions.
    let mut writer_closing = closing.clone();
    // Requests run concurrently (a slow QueryChart must not stall the
    // others), so responses funnel through one writer task.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(256);
    let mut writer = tokio::spawn(async move {
        let mut ping = tokio::time::interval(KEEPALIVE_PING_INTERVAL);
        loop {
            let message = tokio::select! {
                // Shutdown's Close goes first; a due ping goes ahead of queued responses, so a long queue can't delay the pong that keeps a local socket alive.
                biased;
                _ = writer_closing.changed() => {
                    let close = CloseFrame {
                        code: close_code::AWAY,
                        reason: "server shutting down".into(),
                    };
                    let _ = sink.send(Message::Close(Some(close))).await;
                    break;
                }
                _ = ping.tick() => Message::Ping(Default::default()),
                frame = rx.recv() => match frame {
                    Some(frame) => Message::Binary(frame.into()),
                    None => break,
                },
            };
            if sink.send(message).await.is_err() {
                break;
            }
        }
    });
    let mut last_heard = tokio::time::Instant::now();
    // In-flight dispatches live in the set so a dead socket aborts them —
    // a closed dashboard tab takes its unread ClickHouse queries with it.
    let mut tasks = tokio::task::JoinSet::new();
    // Version push: collect bumps into dirty sets and emit at most one
    // id-0 frame per EVENT_COALESCE_MS with only the changed counters.
    let mut events = svc.run_events.subscribe();
    let mut dirty_runs: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    let mut dirty_projects: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();
    let mut dirty_global: u64 = 0;
    let mut dirty_metrics: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut dirty_resync = false;
    // quiet=true (a hidden tab): stop emitting push frames and retain only
    // a resync marker. A long-hidden tab cannot grow an unbounded detail
    // backlog; becoming visible requests one authoritative snapshot.
    let mut quiet = false;
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(EVENT_COALESCE_MS));
    // Delay, not Skip: Delay reschedules a full period after a late tick fires (first event after quiet still flushes immediately), so one connection's frames are genuinely ≥ the period apart; Skip re-anchors to phase boundaries and can emit two frames milliseconds apart. Cache correctness does NOT ride on this spacing (series_cache's bump gate owns that — spacing arguments proved unsound across sockets); this is honest per-connection rate limiting only.
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    'serve: loop {
        let dirty_any = !dirty_runs.is_empty()
            || !dirty_projects.is_empty()
            || dirty_global != 0
            || !dirty_metrics.is_empty()
            || dirty_resync;
        tokio::select! {
            msg = stream.next() => {
                let Some(Ok(msg)) = msg else { break };
                last_heard = tokio::time::Instant::now();
                let Message::Binary(buf) = msg else {
                    continue; // pings are answered by axum; text frames are ignored
                };
                // The frame's one decode: malformed input drops here,
                // before it costs a dispatch slot.
                let Some((id, path, body)) = decode_request(&buf) else {
                    tracing::warn!("ws: dropping malformed frame ({} bytes)", buf.len());
                    continue;
                };
                // Refuse push control too: it may be a hidden tab's first frame.
                if refused {
                    let refusal = Err(Status::unavailable(crate::ws_rpc::RELOAD_REQUIRED));
                    if tx.send(encode_response(id, &refusal)).await.is_err() {
                        break;
                    }
                    continue;
                }
                // Per-connection control frames mutate this loop's push
                // state, which dispatch tasks can't reach — handle inline,
                // with no response (the client parks nothing for them).
                if path == crate::ws_rpc::PUSH_CONTROL {
                    match proto::PushControlRequest::decode(body) {
                        Ok(req) => {
                            quiet = req.quiet;
                            if quiet {
                                retain_only_resync(
                                    &mut dirty_runs,
                                    &mut dirty_projects,
                                    &mut dirty_global,
                                    &mut dirty_metrics,
                                    &mut dirty_resync,
                                );
                            }
                        }
                        Err(e) => tracing::warn!("ws: bad push-control frame: {e}"),
                    }
                    continue;
                }
                // The task needs owned data; `body` borrows `buf`, so hand
                // it the buffer and re-slice by offset instead of re-parsing.
                let path = path.to_owned();
                let body_start = buf.len() - body.len();
                while tasks.len() >= MAX_IN_FLIGHT {
                    tokio::select! {
                        _ = tasks.join_next() => {}
                        _ = closing.changed() => break 'serve,
                    }
                    // This loop stopped reading, so the peer's queued pongs are not its silence.
                    last_heard = tokio::time::Instant::now();
                }
                let svc = svc.clone();
                let tx = tx.clone();
                tasks.spawn(async move {
                    let result = dispatch(&svc, &path, &buf[body_start..]).await;
                    let _ = tx.send(encode_response(id, &result)).await;
                });
            }
            // Reap finished dispatches; the `Some` pattern idles this arm
            // while the set is empty.
            Some(_) = tasks.join_next() => {}
            ev = events.recv(), if !refused => {
                match ev {
                    Ok(ev) => {
                        if quiet {
                            // The detail will be superseded by the snapshot
                            // requested on unhide.
                            dirty_resync = true;
                        } else {
                            merge_latest_versions(&mut dirty_runs, ev.runs);
                            merge_latest_versions(&mut dirty_projects, ev.projects);
                            if let Some(g) = ev.global {
                                dirty_global = dirty_global.max(g);
                            }
                            dirty_metrics.extend(ev.metrics_changed_runs);
                            dirty_resync |= ev.resync;
                        }
                    }
                    // Lagged: this receiver dropped events. Tell the client
                    // to resync immediately rather than waiting for its
                    // one-minute PollVersions backstop. Closed: unreachable
                    // (sender lives on the service for the process lifetime).
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        dirty_resync = true;
                    }
                    Err(_) => {}
                }
            }
            _ = tick.tick(), if dirty_any && !quiet => {
                // A full writer queue keeps the changes for a later tick instead of stalling this loop, which would leave the peer's frames and the silence deadline unpolled.
                let permit = match tx.try_reserve() {
                    Ok(permit) => permit,
                    Err(tokio::sync::mpsc::error::TrySendError::Full(())) => continue,
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => break,
                };
                let ev = proto::RunVersionsEvent {
                    run_versions: std::mem::take(&mut dirty_runs),
                    project_versions: std::mem::take(&mut dirty_projects),
                    global_version: std::mem::take(&mut dirty_global),
                    metrics_changed_runs: dirty_metrics.drain().collect(),
                    resync: std::mem::take(&mut dirty_resync),
                };
                permit.send(encode_response(0, &Ok(ev.encode_to_vec())));
            }
            _ = tokio::time::sleep_until(last_heard + KEEPALIVE_TIMEOUT), if drop_silent => {
                tracing::info!("ws: closing a local socket silent for {:?}", KEEPALIVE_TIMEOUT);
                break;
            }
            _ = closing.changed() => break,
        }
    }
    // Once shutdown began, whatever ended the serve loop (its own shutdown arm, or the writer stopping after its Close and closing the channel), wait here for the browser's reply: the stream ends right after it.
    if *closing.borrow() {
        while let Some(Ok(_)) = stream.next().await {}
    }
    // Abort in-flight work before releasing the writer — the tasks hold tx
    // clones, and the writer drains until every sender is gone.
    tasks.shutdown().await;
    drop(tx);
    // A peer that stopped reading can leave the writer blocked in `send` forever; it must not keep this socket counted as connected.
    if tokio::time::timeout(std::time::Duration::from_secs(5), &mut writer)
        .await
        .is_err()
    {
        writer.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_origins_are_exact_and_deduplicated() {
        let origins = AllowedOrigins::parse(
            " https://dashboard.example, http://localhost:8080,https://dashboard.example ",
        )
        .unwrap();
        assert_eq!(
            origins.display(),
            "https://dashboard.example,http://localhost:8080"
        );

        let mut headers = HeaderMap::new();
        assert!(origins.permits_headers(&headers));
        assert!(!origins.permits_exact_headers(&headers));
        headers.insert(
            ORIGIN,
            HeaderValue::from_static("https://dashboard.example"),
        );
        assert!(origins.permits_headers(&headers));
        assert!(origins.permits_exact_headers(&headers));
        headers.insert(ORIGIN, HeaderValue::from_static("https://other.example"));
        assert!(!origins.permits_headers(&headers));
    }

    #[test]
    fn origin_parser_rejects_invalid_wildcards_urls_and_empty_entries() {
        for value in [
            "*",
            "https://dashboard.example/",
            "https://dashboard.example/path",
            "https://dashboard.example?query",
            "https://dashboard.example:443",
            "http://dashboard.example:80",
            "https://DASHBOARD.example",
            "http://[0:0:0:0:0:0:0:1]:8080",
            "https://dashboard.example:*",
            "http://localhost:80:*",
            "http://localhost:8080:*",
            "http://localhost:*/",
            "dashboard.example",
            "https://ok.example,",
            "",
        ] {
            assert!(AllowedOrigins::parse(value).is_err(), "accepted {value:?}");
        }
    }

    #[test]
    fn wildcard_diagnostic_keeps_the_wildcard() {
        let error = AllowedOrigins::parse("http://LOCALHOST:*")
            .expect_err("non-canonical host must be rejected")
            .to_string();
        assert!(error.contains("\"http://localhost:*\""), "{error}");
    }

    #[test]
    fn origin_parser_accepts_browser_serialized_nondefault_ports_and_ipv6() {
        let origins = AllowedOrigins::parse(
            "https://dashboard.example:8443,http://[::1]:8080,http://localhost",
        )
        .unwrap();
        assert_eq!(
            origins.display(),
            "https://dashboard.example:8443,http://[::1]:8080,http://localhost"
        );
    }

    #[test]
    fn loopback_wildcard_port_admits_any_port_on_that_host_only() {
        let origins = AllowedOrigins::parse("http://localhost:*, http://[::1]:*").unwrap();
        assert_eq!(origins.display(), "http://localhost:*,http://[::1]:*");
        let permitted = |origin: &'static str| {
            let mut headers = HeaderMap::new();
            headers.insert(ORIGIN, HeaderValue::from_static(origin));
            origins.permits_exact_headers(&headers)
        };
        for ok in [
            "http://localhost:8095",
            "http://localhost",
            "http://[::1]:65535",
        ] {
            assert!(permitted(ok), "rejected {ok:?}");
        }
        for bad in [
            "https://localhost:8095",
            "http://127.0.0.1:8095",
            "http://localhost.evil.example",
            "http://localhost:",
            "http://localhost:80a",
            "http://localhost:080",
            "http://localhost:0",
            "http://localhost:65536",
        ] {
            assert!(!permitted(bad), "accepted {bad:?}");
        }
    }

    #[test]
    fn event_coalescing_never_regresses_a_version() {
        let mut versions = std::collections::HashMap::new();
        merge_latest_versions(&mut versions, [("run".to_string(), 9)]);
        merge_latest_versions(
            &mut versions,
            [("run".to_string(), 7), ("other".to_string(), 3)],
        );

        assert_eq!(versions.get("run"), Some(&9));
        assert_eq!(versions.get("other"), Some(&3));
    }

    #[test]
    fn quiet_mode_discards_detail_and_retains_a_resync() {
        let mut runs = std::collections::HashMap::from([("run".to_string(), 9)]);
        let mut projects = std::collections::HashMap::from([("project".to_string(), 4)]);
        let mut global = 12;
        let mut metrics = std::collections::HashSet::from(["run".to_string()]);
        let mut resync = false;

        retain_only_resync(
            &mut runs,
            &mut projects,
            &mut global,
            &mut metrics,
            &mut resync,
        );

        assert!(runs.is_empty());
        assert!(projects.is_empty());
        assert_eq!(global, 0);
        assert!(metrics.is_empty());
        assert!(resync);
    }

    #[test]
    fn binary_defaults_are_local_development_only() {
        let origins = AllowedOrigins::parse(DEFAULT_ALLOWED_ORIGINS).unwrap();
        assert_eq!(origins.display(), "http://localhost:*,http://127.0.0.1:*");
    }

    #[test]
    fn frame_roundtrip() {
        let mut frame = Vec::new();
        frame.extend_from_slice(&7u32.to_le_bytes());
        let path = "/kymo.Kymo/QueryChart";
        frame.extend_from_slice(&(path.len() as u16).to_le_bytes());
        frame.extend_from_slice(path.as_bytes());
        frame.extend_from_slice(&[1, 2, 3]);
        let (id, p, body) = decode_request(&frame).unwrap();
        assert_eq!((id, p, body), (7, path, &[1u8, 2, 3][..]));

        let ok = encode_response(7, &Ok(vec![9, 9]));
        assert_eq!(&ok, &[7, 0, 0, 0, 0, 9, 9]);
        let err = encode_response(1, &Err(Status::unavailable("x")));
        assert_eq!(err[4], tonic::Code::Unavailable as u8);
        assert_eq!(&err[5..], b"x");
    }

    #[test]
    fn malformed_frames_are_rejected_not_panicked() {
        assert!(decode_request(&[]).is_none());
        assert!(decode_request(&[1, 2, 3]).is_none());
        // path_len pointing past the buffer
        let mut frame = Vec::new();
        frame.extend_from_slice(&1u32.to_le_bytes());
        frame.extend_from_slice(&500u16.to_le_bytes());
        frame.extend_from_slice(b"short");
        assert!(decode_request(&frame).is_none());
        // non-utf8 path
        let mut frame = Vec::new();
        frame.extend_from_slice(&1u32.to_le_bytes());
        frame.extend_from_slice(&2u16.to_le_bytes());
        frame.extend_from_slice(&[0xff, 0xfe]);
        assert!(decode_request(&frame).is_none());
    }
}
