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
//! Framing, and why it is frozen: ws_rpc.rs. /grpc-ws2 (hosted only) adds a compression flag bit per direction. The server never compresses an error or an id-0 push, and push-control frames are always raw. A compressed request inflates to at most MAX_GRPC_MESSAGE_BYTES, within a per-socket and a process-wide budget of inflated bytes (INFLATED_BUDGET_BYTES, PROCESS_INFLATED_BUDGET_BYTES); a bad or oversized one is answered with an error and the socket stays up.
//!
//! One ADDITIVE extension rides the frozen format: correlation id 0 is
//! reserved for server-initiated push (a RunVersionsEvent payload with
//! code 0) — the dashboard's primary change signal. Visible tabs also run
//! a one-minute PollVersions backstop. Old bundles drop id-0 frames by
//! construction (no parked request matches). Pushes are coalesced per
//! connection to at most one frame per EVENT_COALESCE_MS carrying only what
//! changed; logging rate cannot inflate this (versions bump per ingest
//! flush, ~2s per active run, and Last-logged values at most once a clock
//! minute per project, not per point).
//!
//! The socket URL carries the dashboard's wire revision as `?rev=` (none counts as 1). Below MIN_FRONTEND_WIRE_REVISION every request frame is answered with Unavailable(RELOAD_REQUIRED), nothing is dispatched and nothing is pushed (ws_rpc.rs).

use std::sync::{Arc, LazyLock};

use axum::body::Bytes;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{RawQuery, State};
use axum::http::{header::ORIGIN, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress};
use futures::{SinkExt, StreamExt};
use prost::Message as _;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;
use tower_http::cors::AllowOrigin;

use crate::local_auth::private_error;
use crate::proto;
use crate::proto::kymo_server::Kymo;
use crate::ws_rpc::{decode_request, DEFLATE_MIN_BYTES, REQUEST_INFLATE_FAILED, RESPONSE_DEFLATED};
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

    /// Also admits a request without Origin, as CLI and other non-browser WebSocket clients send: the allowlist is a browser cross-origin fence, not client auth.
    fn permits_headers(&self, headers: &HeaderMap) -> bool {
        !headers.contains_key(ORIGIN) || self.permits_exact_headers(headers)
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
    /// `deflate`: `/grpc-ws2`, the frozen framing plus per-frame compression (module docs).
    Hosted { deflate: bool },
    Local {
        activity: Arc<crate::activity::ActivityTracker>,
    },
}

impl WsState {
    pub(crate) fn hosted(service: Arc<KymoService>, allowed_origins: AllowedOrigins) -> Self {
        Self {
            service,
            allowed_origins,
            mode: WsMode::Hosted { deflate: false },
        }
    }

    /// This hosted state for `/grpc-ws2`.
    pub(crate) fn deflating(&self) -> Self {
        Self {
            mode: WsMode::Hosted { deflate: true },
            ..self.clone()
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

/// zlib's default level: 945 KB of ListRunSetMetrics shrinks 13.7x in 3.4 ms.
const DEFLATE_LEVEL: u32 = 6;
/// OK payloads this large compress on the blocking pool: level 6 costs about 11 ms per MB of chart reply (0.75 ms for 64 KiB), so a smaller one holds a runtime worker (shared with native gRPC) for under 1 ms.
const DEFLATE_BLOCKING_BYTES: usize = 64 * 1024;
/// The most a [`RequestInflate`] allocates under MAX_GRPC_MESSAGE_BYTES: one byte past the cap tells an oversized body from one that fills it exactly.
const INFLATE_RESERVE_BYTES: usize = crate::ingest::MAX_GRPC_MESSAGE_BYTES + 1;
/// Inflated request bytes that one /grpc-ws2 socket's in-flight requests may hold, at most four cap-sized bodies (16 MiB). A compressed body is tiny next to what it inflates to, so without this budget 64 in-flight requests could pin 256 MiB for about 260 KB of upload; a raw body is held as uploaded and needs none.
const INFLATED_BUDGET_BYTES: usize = 4 * INFLATE_RESERVE_BYTES;
/// Inflated request bytes that every /grpc-ws2 socket together may hold, at most sixteen cap-sized bodies (64 MiB): with only the per-socket budget, each socket a client opens could pin 16 MiB for about 16 KB of upload.
const PROCESS_INFLATED_BUDGET_BYTES: usize = 16 * INFLATE_RESERVE_BYTES;
static PROCESS_INFLATE_BUDGET: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(PROCESS_INFLATED_BUDGET_BYTES)));
/// Flagged request bodies of this many compressed bytes or more inflate on the blocking pool. Run-id lists inflate at about 4 µs per compressed KiB, so one just under this holds a runtime worker for about 65 µs, three times a round trip to the blocking pool.
const INFLATE_BLOCKING_BYTES: usize = 16 * 1024;
/// A smaller body inflates in the dispatch task up to this much output, then goes on from the same state on the blocking pool: a bomb of a few KB that inflates to the cap takes about 0.5 ms, where its first 256 KiB take about 0.02 ms.
const INFLATE_INLINE_BYTES: usize = 256 * 1024;

/// Errors are never compressed.
fn error_frame(id: u32, status: &Status) -> Vec<u8> {
    crate::ws_rpc::encode_response(id, status.code() as u8, status.message().as_bytes())
}

/// A dispatch task's reply frame. On /grpc-ws2 (`deflate`), an OK payload of at least DEFLATE_MIN_BYTES goes compressed when that makes it smaller, compressed on the blocking pool from DEFLATE_BLOCKING_BYTES; an error never reaches the codec.
async fn encode_reply(id: u32, result: Result<Vec<u8>, Status>, deflate: bool) -> Vec<u8> {
    let payload = match result {
        Ok(payload) if deflate && payload.len() >= DEFLATE_MIN_BYTES => payload,
        Ok(payload) => return crate::ws_rpc::encode_response(id, 0, &payload),
        Err(status) => return error_frame(id, &status),
    };
    if payload.len() < DEFLATE_BLOCKING_BYTES {
        return ok_frame(id, &payload);
    }
    tokio::task::spawn_blocking(move || ok_frame(id, &payload))
        .await
        .unwrap_or_else(|error| {
            let failed = Status::internal(format!("response compression failed: {error}"));
            error_frame(id, &failed)
        })
}

/// An OK frame carrying `payload` zlib-compressed, or raw when that would not be smaller: the frame's capacity holds `payload.len()` bytes after the header, so a larger stream cannot finish there, and the raw payload fits it exactly.
fn ok_frame(id: u32, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.extend_from_slice(&id.to_le_bytes());
    frame.push(RESPONSE_DEFLATED);
    let mut deflater = Compress::new(Compression::new(DEFLATE_LEVEL), true);
    let finished = matches!(
        deflater.compress_vec(payload, &mut frame, FlushCompress::Finish),
        Ok(flate2::Status::StreamEnd)
    ) && frame.len() - 5 < payload.len();
    if !finished {
        frame.truncate(4);
        frame.push(0);
        frame.extend_from_slice(payload);
    }
    // The writer queue would otherwise hold the uncompressed size until a compressed frame is sent.
    frame.shrink_to_fit();
    frame
}

/// Inflate a flagged request body within its socket's budget and then the process's: wait in each for the most inflating can allocate, inflate, then keep only what the body inflated to. The returned permits hold that much until the caller drops them, when the dispatch ends; an error releases everything at once. The socket's budget comes first, so a request waiting on it holds none of the process's, and no socket holds more of the process's than its own budget. The dispatch task inflates a body under INFLATE_BLOCKING_BYTES up to INFLATE_INLINE_BYTES of output; the rest of it, and a larger body from the start, inflates on the blocking pool, which owns the permits until it ends, even when the dispatch is aborted first.
async fn inflate_within_budgets(
    socket: &Arc<Semaphore>,
    process: &Arc<Semaphore>,
    compressed: Bytes,
) -> Result<(Vec<u8>, [OwnedSemaphorePermit; 2]), Status> {
    let reserve = |budget: &Arc<Semaphore>| {
        budget
            .clone()
            .acquire_many_owned(INFLATE_RESERVE_BYTES as u32)
    };
    let socket = reserve(socket).await;
    let process = reserve(process).await;
    let held = [socket, process].map(|held| held.expect("the inflate budgets are never closed"));
    let keep = |body: Vec<u8>, mut held: [OwnedSemaphorePermit; 2]| {
        for held in &mut held {
            drop(held.split(held.num_permits() - body.len()));
        }
        (body, held)
    };
    let mut inflate = RequestInflate::new(compressed);
    if inflate.input.len() < INFLATE_BLOCKING_BYTES {
        if let Some(body) = inflate.run(INFLATE_INLINE_BYTES)? {
            return Ok(keep(body, held));
        }
    }
    tokio::task::spawn_blocking(move || {
        let body = inflate.run(usize::MAX)?;
        Ok(keep(body.expect("an inflate that never pauses ends"), held))
    })
    .await
    .unwrap_or_else(|error| {
        Err(Status::internal(format!(
            "{REQUEST_INFLATE_FAILED}: {error}"
        )))
    })
}

/// A flagged /grpc-ws2 request body inflating to at most MAX_GRPC_MESSAGE_BYTES, which can stop partway and go on later, on another thread, from the same state ([`Self::run`]). The whole input must be exactly one zlib stream. The codes are tonic's for a compressed gRPC message (codec/decode.rs): corrupt, truncated or trailing bytes are Internal, and a body past the cap is ResourceExhausted. Neither is InvalidArgument, which a dashboard chart takes as final and stops retrying.
struct RequestInflate {
    input: Bytes,
    inflater: Decompress,
    out: Vec<u8>,
}

impl RequestInflate {
    fn new(input: Bytes) -> Self {
        Self {
            input,
            inflater: Decompress::new(true),
            out: Vec::new(),
        }
    }

    /// Inflate on until the stream ends, returning the body, or until `pause` bytes are out, returning None: run again to go on.
    fn run(&mut self, pause: usize) -> Result<Option<Vec<u8>>, Status> {
        let corrupt = |why: &str| Status::internal(format!("{REQUEST_INFLATE_FAILED}: {why}"));
        let cap = crate::ingest::MAX_GRPC_MESSAGE_BYTES;
        loop {
            if self.out.len() == self.out.capacity() {
                if self.out.len() >= pause {
                    return Ok(None);
                }
                // Double, but once that passes half the cap go straight to one byte past it, so a move copies at most half the cap; and never past `pause`. The buffer stays within INFLATE_RESERVE_BYTES, but that last move briefly holds the old one (at most half the cap) beside it: up to about 1.5 times the reserve. decompress_vec zero-fills the spare capacity before each call (flate2 1.1.10's zlib-rs backend has no uninitialized inflate), so every page of the buffer is touched, the tail past the body included: at most about 0.2 ms for 4 MiB of fresh pages.
                let doubled = (self.out.capacity() * 2)
                    .max(self.input.len() * 4)
                    .max(DEFLATE_MIN_BYTES);
                let grown = if doubled > cap / 2 {
                    INFLATE_RESERVE_BYTES
                } else {
                    doubled
                };
                self.out.reserve_exact(grown.min(pause) - self.out.len());
            }
            let read = self.inflater.total_in() as usize;
            let status = self
                .inflater
                .decompress_vec(&self.input[read..], &mut self.out, FlushDecompress::Finish)
                .map_err(|error| corrupt(&error.to_string()))?;
            if self.out.len() > cap {
                return Err(Status::resource_exhausted(format!(
                    "inflated request body exceeds {cap} bytes"
                )));
            }
            match status {
                flate2::Status::StreamEnd
                    if (self.inflater.total_in() as usize) < self.input.len() =>
                {
                    return Err(corrupt("data after the end of the stream"));
                }
                flate2::Status::StreamEnd => {
                    // Hand back no more memory than the body: the inflate budget holds its length.
                    let mut body = std::mem::take(&mut self.out);
                    body.shrink_to_fit();
                    return Ok(Some(body));
                }
                // Inflating stops short of filling its output room only when the input has run out.
                _ if self.out.len() < self.out.capacity() => {
                    return Err(corrupt("truncated stream"))
                }
                _ => {}
            }
        }
    }
}

/// One connection's changes not yet pushed, as one id-0 frame's fields.
#[derive(Default, PartialEq)]
struct DirtyVersions {
    runs: std::collections::HashMap<String, u64>,
    projects: std::collections::HashMap<String, u64>,
    global: u64,
    metrics: std::collections::HashSet<String>,
    resync: bool,
    last_logged: std::collections::HashMap<String, i64>,
}

impl DirtyVersions {
    fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// Coalesced version counters and Last-logged values must retain the newest value even when two post-commit publishers are scheduled out of order.
    fn add(&mut self, ev: crate::events::VersionEvent) {
        fn raise<T: Ord + Copy>(
            into: &mut std::collections::HashMap<String, T>,
            from: Vec<(String, T)>,
        ) {
            for (key, value) in from {
                let current = into.entry(key).or_insert(value);
                *current = (*current).max(value);
            }
        }
        raise(&mut self.runs, ev.runs);
        raise(&mut self.projects, ev.projects);
        raise(&mut self.last_logged, ev.project_last_logged_at_ms);
        self.global = self.global.max(ev.global.unwrap_or(0));
        self.metrics.extend(ev.metrics_changed_runs);
        self.resync |= ev.resync;
    }

    /// A hidden tab does not need an exact replay of every entity that changed while it was away. Drop the potentially unbounded detail and ask it for one authoritative snapshot when it becomes visible again.
    fn retain_only_resync(&mut self) {
        *self = Self {
            resync: true,
            ..Self::default()
        };
    }

    fn take_event(&mut self) -> proto::RunVersionsEvent {
        let dirty = std::mem::take(self);
        proto::RunVersionsEvent {
            run_versions: dirty.runs,
            project_versions: dirty.projects,
            global_version: dirty.global,
            metrics_changed_runs: dirty.metrics.into_iter().collect(),
            resync: dirty.resync,
            project_last_logged_at_ms: dirty.last_logged,
        }
    }
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
    let (deflate, activity) = match &state.mode {
        WsMode::Hosted { deflate } => (*deflate, None),
        WsMode::Local { activity } => (false, Some(activity.clone())),
    };
    let local = activity.is_some();
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
    // Bundles without a valid `?rev=` count as revision 1.
    let rev = query
        .as_deref()
        .and_then(|q| q.split('&').find_map(|p| p.strip_prefix("rev=")))
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let refused = rev < crate::ws_rpc::MIN_FRONTEND_WIRE_REVISION;
    // Match native gRPC's transport-byte admission. This bounds one frame; the handlers bound the run-id lists inside it (query.rs MAX_REQUEST_RUN_IDS).
    ws.max_message_size(crate::ingest::MAX_GRPC_MESSAGE_BYTES)
        .on_upgrade(move |socket| async move {
            let _frontend = activity
                .as_ref()
                .map(|activity| activity.frontend_connected());
            handle_socket(socket, state.service, local, refused, deflate, closing).await;
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
    deflate: bool,
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
                        code: crate::ws_rpc::SHUTDOWN_CLOSE_CODE,
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
    let inflate_budget = Arc::new(Semaphore::new(INFLATED_BUDGET_BYTES));
    // Version push: collect changes into dirty sets and emit at most one
    // id-0 frame per EVENT_COALESCE_MS with only what changed.
    let mut events = svc.run_events.subscribe();
    let mut dirty = DirtyVersions::default();
    // quiet=true (a hidden tab): stop emitting push frames and retain only
    // a resync marker (DirtyVersions::retain_only_resync).
    let mut quiet = false;
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(EVENT_COALESCE_MS));
    // Delay, not Skip: a late tick reschedules a full period, so one connection's frames stay at least a period apart while the first event after quiet still flushes at once (Skip can emit two frames milliseconds apart). This is per-connection rate limiting only: cache correctness rides on series_cache's bump gate, never on this spacing, which proved unsound across sockets.
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            msg = stream.next() => {
                let Some(Ok(msg)) = msg else { break };
                last_heard = tokio::time::Instant::now();
                let Message::Binary(buf) = msg else {
                    continue; // pings are answered by axum; text frames are ignored
                };
                // The frame's one decode: malformed input drops here,
                // before it costs a dispatch slot.
                let Some((id, path, body, deflated)) = decode_request(&buf, deflate) else {
                    tracing::warn!("ws: dropping malformed frame ({} bytes)", buf.len());
                    continue;
                };
                // Refuse push control too: it may be a hidden tab's first frame.
                if refused {
                    let refusal = Status::unavailable(crate::ws_rpc::RELOAD_REQUIRED);
                    if tx.send(error_frame(id, &refusal)).await.is_err() {
                        break;
                    }
                    continue;
                }
                // Per-connection control frames mutate this loop's push
                // state, which dispatch tasks can't reach — handle inline,
                // with no response (the client parks nothing for them).
                if path == crate::ws_rpc::PUSH_CONTROL {
                    // The dashboard sends push control raw.
                    if deflated {
                        tracing::warn!("ws: dropping a compressed push-control frame");
                        continue;
                    }
                    match proto::PushControlRequest::decode(body) {
                        Ok(req) => {
                            quiet = req.quiet;
                            if quiet {
                                dirty.retain_only_resync();
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
                if tasks.len() >= MAX_IN_FLIGHT {
                    tokio::select! {
                        _ = tasks.join_next() => {}
                        _ = closing.changed() => break,
                    }
                    // The cap wait reads nothing, so the peer's queued pongs are not its silence.
                    last_heard = tokio::time::Instant::now();
                }
                let svc = svc.clone();
                let tx = tx.clone();
                let inflate_budget = inflate_budget.clone();
                tasks.spawn(async move {
                    let result = if deflated {
                        let compressed = buf.slice(body_start..);
                        let process = &PROCESS_INFLATE_BUDGET;
                        match inflate_within_budgets(&inflate_budget, process, compressed).await {
                            Ok((body, _held)) => dispatch(&svc, &path, &body).await,
                            Err(status) => Err(status),
                        }
                    } else {
                        dispatch(&svc, &path, &buf[body_start..]).await
                    };
                    let _ = tx.send(encode_reply(id, result, deflate).await).await;
                });
            }
            // Reap finished dispatches; the `Some` pattern idles this arm
            // while the set is empty.
            Some(_) = tasks.join_next() => {}
            ev = events.recv(), if !refused => {
                match ev {
                    Ok(ev) if !quiet => dirty.add(ev),
                    // Quiet: the detail will be superseded by the snapshot requested on unhide. Lagged: this receiver dropped events; tell the client to resync now rather than at its one-minute PollVersions backstop.
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => dirty.resync = true,
                    // Closed: unreachable (the sender lives on the service for the process lifetime).
                    Err(_) => {}
                }
            }
            _ = tick.tick(), if !dirty.is_empty() && !quiet => {
                // A full writer queue keeps the changes for a later tick instead of stalling this loop, which would leave the peer's frames and the silence deadline unpolled.
                let permit = match tx.try_reserve() {
                    Ok(permit) => permit,
                    Err(tokio::sync::mpsc::error::TrySendError::Full(())) => continue,
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(())) => break,
                };
                let event = dirty.take_event().encode_to_vec();
                permit.send(crate::ws_rpc::encode_response(0, 0, &event));
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
    use crate::ws_rpc::encode_request;
    use futures::FutureExt;

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
        let mut dirty = DirtyVersions::default();
        for (runs, logged) in [
            (vec![("run", 9)], vec![("project", 120_000)]),
            (
                vec![("run", 7), ("other", 3)],
                vec![("project", 60_000), ("idle", 5)],
            ),
        ] {
            dirty.add(crate::events::VersionEvent {
                runs: runs.into_iter().map(|(r, v)| (r.to_string(), v)).collect(),
                project_last_logged_at_ms: logged
                    .into_iter()
                    .map(|(p, at)| (p.to_string(), at))
                    .collect(),
                ..Default::default()
            });
        }
        assert_eq!(dirty.runs.get("run"), Some(&9));
        assert_eq!(dirty.runs.get("other"), Some(&3));
        let event = dirty.take_event();
        assert_eq!(
            event.project_last_logged_at_ms.get("project"),
            Some(&120_000)
        );
        assert_eq!(event.project_last_logged_at_ms.get("idle"), Some(&5));
    }

    #[test]
    fn quiet_mode_discards_detail_and_retains_a_resync() {
        let mut dirty = DirtyVersions::default();
        dirty.add(crate::events::VersionEvent {
            runs: vec![("run".to_string(), 9)],
            projects: vec![("project".to_string(), 4)],
            global: Some(12),
            metrics_changed_runs: vec!["run".to_string()],
            resync: false,
            project_last_logged_at_ms: vec![("project".to_string(), 60_000)],
        });
        dirty.retain_only_resync();
        let resync_only = proto::RunVersionsEvent {
            resync: true,
            ..Default::default()
        };
        assert_eq!(dirty.take_event(), resync_only);
        assert!(dirty.is_empty());
    }

    #[test]
    fn binary_defaults_are_local_development_only() {
        let origins = AllowedOrigins::parse(DEFAULT_ALLOWED_ORIGINS).unwrap();
        assert_eq!(origins.display(), "http://localhost:*,http://127.0.0.1:*");
    }

    fn zlib(bytes: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), Compression::new(6));
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    /// Compressible: a run-id list, the shape of the largest dashboard bodies.
    fn run_ids(bytes: usize) -> Vec<u8> {
        (0..)
            .flat_map(|i: u32| format!("\n\x24run-{i:08}-hyperopt-sweep-trial").into_bytes())
            .take(bytes)
            .collect()
    }

    /// Incompressible: xorshift noise.
    fn noise(bytes: usize) -> Vec<u8> {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        (0..bytes)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect()
    }

    /// An independent decoder, for round trips.
    fn inflate(bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        std::io::Read::read_to_end(&mut flate2::read::ZlibDecoder::new(bytes), &mut out).unwrap();
        out
    }

    #[tokio::test]
    async fn grpc_ws2_compresses_ok_replies_from_4_kib_when_smaller() {
        let just_under = run_ids(DEFLATE_MIN_BYTES - 1);
        let frame = encode_reply(3, Ok(just_under.clone()), true).await;
        assert_eq!(&frame[..5], &[3, 0, 0, 0, 0]);
        assert_eq!(&frame[5..], &just_under[..]);

        // Inline below DEFLATE_BLOCKING_BYTES and on the blocking pool from it, keeping only the compressed size.
        for size in [DEFLATE_MIN_BYTES, 4 * 1024 * 1024] {
            let payload = run_ids(size);
            let frame = encode_reply(3, Ok(payload.clone()), true).await;
            assert_eq!(&frame[..5], &[3, 0, 0, 0, RESPONSE_DEFLATED], "{size}");
            assert!(frame.len() < payload.len() / 2, "{size}: {}", frame.len());
            assert_eq!(frame.capacity(), frame.len(), "{size}");
            assert_eq!(inflate(&frame[5..]), payload, "{size}");
            // /grpc-ws never compresses.
            let plain = encode_reply(3, Ok(payload.clone()), false).await;
            assert_eq!(&plain[..5], &[3, 0, 0, 0, 0]);
            assert_eq!(&plain[5..], &payload[..]);
        }

        // Only if smaller: noise would grow, so it goes raw.
        for size in [8 * 1024, DEFLATE_BLOCKING_BYTES] {
            let payload = noise(size);
            let frame = encode_reply(3, Ok(payload.clone()), true).await;
            assert_eq!(&frame[..5], &[3, 0, 0, 0, 0], "{size}");
            assert_eq!(&frame[5..], &payload[..], "{size}");
        }
        // Errors stay raw however long.
        let message = String::from_utf8(run_ids(DEFLATE_BLOCKING_BYTES)).unwrap();
        let error = encode_reply(7, Err(Status::not_found(message.clone())), true).await;
        assert_eq!(error[4], tonic::Code::NotFound as u8);
        assert_eq!(&error[5..], message.as_bytes());
    }

    #[test]
    fn replies_from_64_kib_compress_off_the_runtime_worker() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            // Occupy the only blocking thread: a reply compressed there cannot finish until it is released.
            let (release, parked) = std::sync::mpsc::channel::<()>();
            let blocker = tokio::task::spawn_blocking(move || parked.recv());
            let below = run_ids(DEFLATE_BLOCKING_BYTES - 1);
            let frame = encode_reply(1, Ok(below.clone()), true)
                .now_or_never()
                .expect("a smaller reply compresses in the dispatch task");
            assert_eq!(&frame[..5], &[1, 0, 0, 0, RESPONSE_DEFLATED]);
            assert_eq!(inflate(&frame[5..]), below);

            let at = run_ids(DEFLATE_BLOCKING_BYTES);
            let mut reply = Box::pin(encode_reply(2, Ok(at.clone()), true));
            assert!(reply.as_mut().now_or_never().is_none());
            release.send(()).unwrap();
            blocker.await.unwrap().unwrap();
            let frame = reply.await;
            assert_eq!(&frame[..5], &[2, 0, 0, 0, RESPONSE_DEFLATED]);
            assert_eq!(inflate(&frame[5..]), at);
        });
    }

    #[tokio::test]
    async fn inflated_bodies_hold_at_most_their_socket_and_process_budgets() {
        let cap = crate::ingest::MAX_GRPC_MESSAGE_BYTES;
        let process = Arc::new(Semaphore::new(PROCESS_INFLATED_BUDGET_BYTES));
        let sockets: Vec<_> = (0..5)
            .map(|_| Arc::new(Semaphore::new(INFLATED_BUDGET_BYTES)))
            .collect();
        let inflate = |socket: usize, input: &[u8]| {
            inflate_within_budgets(&sockets[socket], &process, Bytes::copy_from_slice(input))
        };
        let held_by =
            |permits: &[OwnedSemaphorePermit; 2]| permits.each_ref().map(|p| p.num_permits());
        // A few KB of upload that inflates to the cap.
        let bomb = zlib(&vec![0; cap]);
        assert!(bomb.len() < 8 * 1024, "{}", bomb.len());
        // Each body holds only what it inflated to, in memory and in both budgets.
        let small = run_ids(64 * 1024);
        let (body, permits) = inflate(0, &zlib(&small)).await.unwrap();
        assert_eq!(body, small);
        assert_eq!(body.capacity(), small.len());
        assert_eq!(held_by(&permits), [small.len(); 2]);
        let mut held = vec![permits];
        for _ in 0..3 {
            let (body, permits) = inflate(0, &bomb).await.unwrap();
            assert_eq!((body.capacity(), held_by(&permits)), (cap, [cap; 2]));
            held.push(permits);
        }
        // A fourth cap-sized body waits, uninflated, until its socket's budget covers the most it could inflate to.
        let mut fourth = Box::pin(inflate(0, &bomb));
        assert!(fourth.as_mut().now_or_never().is_none());
        drop(held.remove(0));
        held.push(fourth.await.unwrap().1);
        assert_eq!(
            sockets[0].available_permits(),
            INFLATED_BUDGET_BYTES - 4 * cap
        );
        // A fifth waits on its socket's budget and holds none of the process's meanwhile.
        let mut fifth = Box::pin(inflate(0, &bomb));
        assert!(fifth.as_mut().now_or_never().is_none());
        // Other sockets have budgets of their own, until all of them together hold the process's: sixteen cap-sized bodies.
        for socket in 1..4 {
            for _ in 0..4 {
                held.push(inflate(socket, &bomb).await.unwrap().1);
            }
        }
        assert_eq!(
            process.available_permits(),
            PROCESS_INFLATED_BUDGET_BYTES - 16 * cap
        );
        // Then another socket's first body waits too, holding its own socket's reserve, until any socket releases one.
        let mut waiting = Box::pin(inflate(4, &bomb));
        assert!(waiting.as_mut().now_or_never().is_none());
        assert_eq!(
            sockets[4].available_permits(),
            INFLATED_BUDGET_BYTES - INFLATE_RESERVE_BYTES
        );
        drop(held.pop());
        held.push(waiting.await.unwrap().1);
        drop(fifth);
        // A body that fails to inflate releases its reservations at once.
        drop(held.pop());
        let free = || [sockets[4].available_permits(), process.available_permits()];
        let before = free();
        for (input, code) in [
            (b"not zlib".to_vec(), tonic::Code::Internal),
            (zlib(&vec![0; cap + 1]), tonic::Code::ResourceExhausted),
        ] {
            let status = inflate(4, &input).await.unwrap_err();
            assert_eq!(status.code(), code);
            assert_eq!(free(), before);
        }
        drop(held);
        assert_eq!(process.available_permits(), PROCESS_INFLATED_BUDGET_BYTES);
        for socket in &sockets {
            assert_eq!(socket.available_permits(), INFLATED_BUDGET_BYTES);
        }
    }

    #[test]
    fn requests_inflate_in_the_dispatch_task_only_while_small() {
        let cap = crate::ingest::MAX_GRPC_MESSAGE_BYTES;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let socket = Arc::new(Semaphore::new(INFLATED_BUDGET_BYTES));
            let process = Arc::new(Semaphore::new(PROCESS_INFLATED_BUDGET_BYTES));
            let inflate = |input: &[u8]| {
                inflate_within_budgets(&socket, &process, Bytes::copy_from_slice(input))
            };
            let reserved = || {
                [socket.available_permits(), process.available_permits()]
                    != [INFLATED_BUDGET_BYTES, PROCESS_INFLATED_BUDGET_BYTES]
            };
            // Occupy the only blocking thread: an inflate there cannot finish until it is released.
            let occupy = || {
                let (release, parked) = std::sync::mpsc::channel::<()>();
                (release, tokio::task::spawn_blocking(move || parked.recv()))
            };
            let (release, blocker) = occupy();
            // Below INFLATE_BLOCKING_BYTES of input and within INFLATE_INLINE_BYTES of output, the dispatch task inflates the whole body.
            let below = noise(INFLATE_BLOCKING_BYTES - 64);
            let at = noise(INFLATE_BLOCKING_BYTES);
            let (zlib_below, zlib_at) = (zlib(&below), zlib(&at));
            assert!(
                zlib_below.len() < INFLATE_BLOCKING_BYTES,
                "{}",
                zlib_below.len()
            );
            assert!(zlib_at.len() >= INFLATE_BLOCKING_BYTES, "{}", zlib_at.len());
            let zeros = vec![0; INFLATE_INLINE_BYTES / 2];
            for (input, inflated) in [(zlib(&zeros), zeros), (zlib_below, below)] {
                let (body, _held) = inflate(&input)
                    .now_or_never()
                    .expect("a small body inflates in the dispatch task")
                    .unwrap();
                assert_eq!(body, inflated);
            }
            release.send(()).unwrap();
            blocker.await.unwrap().unwrap();
            // A bomb of a few KB leaves the dispatch task after INFLATE_INLINE_BYTES of output, and a larger input never inflates there: the blocking pool goes on, and owns the reservations until it is done, even when the dispatch is dropped first.
            let bomb = zlib(&vec![0; cap]);
            assert!(bomb.len() < 8 * 1024, "{}", bomb.len());
            for (input, inflated) in [(bomb, vec![0; cap]), (zlib_at, at)] {
                let (release, blocker) = occupy();
                let mut pending = Box::pin(inflate(&input));
                assert!(pending.as_mut().now_or_never().is_none());
                drop(pending);
                assert!(reserved());
                release.send(()).unwrap();
                blocker.await.unwrap().unwrap();
                while reserved() {
                    tokio::task::yield_now().await;
                }
                let (body, held) = inflate(&input).await.unwrap();
                assert_eq!(body, inflated);
                assert_eq!(
                    held.each_ref().map(|p| p.num_permits()),
                    [inflated.len(); 2]
                );
            }
        });
    }

    #[test]
    fn an_inflate_pauses_at_its_bound_and_goes_on_from_the_same_state() {
        let cap = crate::ingest::MAX_GRPC_MESSAGE_BYTES;
        for body in [vec![0; cap], run_ids(1 << 20), noise(1 << 20)] {
            let mut inflate = RequestInflate::new(Bytes::from(zlib(&body)));
            assert_eq!(inflate.run(INFLATE_INLINE_BYTES).unwrap(), None);
            // Paused with exactly the bound inflated and allocated.
            assert_eq!(
                (inflate.out.len(), inflate.out.capacity()),
                (INFLATE_INLINE_BYTES, INFLATE_INLINE_BYTES)
            );
            assert_eq!(inflate.run(usize::MAX).unwrap().unwrap(), body);
        }
        // The cap holds across the pause.
        let mut bomb = RequestInflate::new(Bytes::from(zlib(&vec![0; cap + 1])));
        assert_eq!(bomb.run(INFLATE_INLINE_BYTES).unwrap(), None);
        let status = bomb.run(usize::MAX).unwrap_err();
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    }

    /// The whole body in one run.
    fn inflate_request(input: &[u8]) -> Result<Vec<u8>, Status> {
        let body = RequestInflate::new(Bytes::copy_from_slice(input)).run(usize::MAX)?;
        Ok(body.expect("an inflate that never pauses ends"))
    }

    #[test]
    fn request_inflate_is_capped_and_strict() {
        let cap = crate::ingest::MAX_GRPC_MESSAGE_BYTES;
        let body = run_ids(1000);
        let compressed = zlib(&body);
        assert_eq!(inflate_request(&compressed).unwrap(), body);
        assert_eq!(inflate_request(&zlib(&vec![0; cap])).unwrap(), vec![0; cap]);
        // A bomb stops at the cap instead of inflating in full.
        let bomb = zlib(&vec![0; 16 * 1024 * 1024]);
        assert!(bomb.len() < 32 * 1024);
        let status = inflate_request(&bomb).unwrap_err();
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(inflate_request(&zlib(b"")).unwrap(), b"");

        let mut trailing = compressed.clone();
        trailing.push(0);
        let mut raw_deflate = flate2::write::DeflateEncoder::new(Vec::new(), Compression::new(6));
        std::io::Write::write_all(&mut raw_deflate, &body).unwrap();
        let mut bad_checksum = compressed.clone();
        *bad_checksum.last_mut().unwrap() ^= 1;
        // FDICT: the stream needs a preset dictionary that a request cannot carry.
        let mut with_dictionary = Compress::new(Compression::new(6), true);
        with_dictionary.set_dictionary(b"run-").unwrap();
        let mut preset_dictionary = Vec::with_capacity(body.len());
        let status = with_dictionary
            .compress_vec(&body, &mut preset_dictionary, FlushCompress::Finish)
            .unwrap();
        assert_eq!(status, flate2::Status::StreamEnd);
        assert_ne!(preset_dictionary[1] & 0x20, 0);
        for (name, input) in [
            ("empty", Vec::new()),
            ("truncated", compressed[..compressed.len() / 2].to_vec()),
            ("trailing", trailing),
            ("raw deflate", raw_deflate.finish().unwrap()),
            ("bad checksum", bad_checksum),
            ("preset dictionary", preset_dictionary),
            ("noise", noise(4096)),
        ] {
            let status = inflate_request(&input).unwrap_err();
            assert_eq!(status.code(), tonic::Code::Internal, "{name}");
        }
    }

    /// The real service with unreachable databases (nothing listens on port 1, and the test Postgres pool connects lazily): enough for requests that are answered before touching one.
    fn service_without_databases(bumps: Arc<crate::ingest::BumpCoalescer>) -> Arc<KymoService> {
        let ch = Arc::new(crate::clickhouse::ChClient::new("http://127.0.0.1:1").unwrap());
        let pg = Arc::new(crate::pg::PgStore::test_store());
        let gates = crate::lifecycle::LifecycleGates::new();
        let activity = crate::activity::ActivityTracker::disabled();
        let (run_events, _) = tokio::sync::broadcast::channel(8);
        Arc::new(KymoService {
            ingest: crate::ingest::IngestService::new(
                ch.clone(),
                bumps.clone(),
                pg.clone(),
                gates.clone(),
            ),
            import: crate::import::ImportService::new(
                ch.clone(),
                pg.clone(),
                gates.clone(),
                activity.clone(),
            ),
            query: Arc::new(crate::query::QueryService::new(
                ch,
                pg,
                bumps,
                gates,
                None,
                run_events.clone(),
            )),
            lifecycle_mutations: Default::default(),
            activity,
            run_events,
        })
    }

    /// QueryCdnKeys with no refs is answered OK (empty) before any database is touched. `total` bytes: the rest is a compressible unknown field, which protobuf skips.
    fn cdn_keys_request(total: usize) -> Vec<u8> {
        use prost::encoding::{encode_key, encode_varint, encoded_len_varint, WireType};
        const PADDING_FIELD: u32 = 9_999;
        let key_len = encoded_len_varint(u64::from(PADDING_FIELD << 3));
        let pad = (0..total)
            .rev()
            .find(|pad| key_len + encoded_len_varint(*pad as u64) + pad == total)
            .unwrap();
        let mut body = proto::QueryCdnKeysRequest::default().encode_to_vec();
        encode_key(PADDING_FIELD, WireType::LengthDelimited, &mut body);
        encode_varint(pad as u64, &mut body);
        body.extend(run_ids(pad));
        assert_eq!(body.len(), total);
        body
    }

    type Client = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;

    async fn send(client: &mut Client, frame: Vec<u8>) {
        client
            .send(tokio_tungstenite::tungstenite::Message::Binary(
                frame.into(),
            ))
            .await
            .unwrap();
    }

    async fn receive(client: &mut Client) -> Vec<u8> {
        let next = async {
            loop {
                match client.next().await.unwrap().unwrap() {
                    tokio_tungstenite::tungstenite::Message::Binary(frame) => {
                        return frame.to_vec()
                    }
                    tokio_tungstenite::tungstenite::Message::Ping(_) => {}
                    other => panic!("unexpected message {other:?}"),
                }
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), next)
            .await
            .expect("no frame within 10 s")
    }

    fn push_event(runs: u32) -> crate::events::VersionEvent {
        crate::events::VersionEvent {
            runs: (0..runs)
                .map(|i| (format!("run-{i:08}-hyperopt-sweep-trial"), u64::from(i) + 1))
                .collect(),
            ..Default::default()
        }
    }

    /// Through the hosted route table, the real socket loop and the real dispatch, limited to what needs no database. Response compression of large OK payloads is covered by the `encode_reply` tests above.
    #[tokio::test]
    async fn grpc_ws2_inflates_requests_and_grpc_ws_frames_are_unchanged() {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        let service = service_without_databases(crate::ingest::BumpCoalescer::empty_for_test());
        let events = service.run_events.clone();
        let state = WsState::hosted(
            service,
            AllowedOrigins::parse("http://dashboard.example").unwrap(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, crate::http_transport::hosted_ws_routes(state)).await
        });
        let url = |path: &str, rev: u32| format!("ws://{address}{path}?rev={rev}");
        let current = crate::ws_rpc::FRONTEND_WIRE_REVISION;
        let connect = |path: &str| {
            let url = url(path, current);
            async move { tokio_tungstenite::connect_async(url).await.unwrap().0 }
        };
        let keys = crate::ws_rpc::QUERY_CDN_KEYS;
        let cap = crate::ingest::MAX_GRPC_MESSAGE_BYTES;
        let body = cdn_keys_request(64 * 1024);
        // Unknown paths echo themselves in the error, so this one is long and compressible.
        let unknown = format!("/kymo.Kymo/{}", "Unknown".repeat(700));
        let unknown_reply = |id: u32| {
            let mut frame = id.to_le_bytes().to_vec();
            frame.push(tonic::Code::Unimplemented as u8);
            frame.extend(format!("not proxied over ws: {unknown}").into_bytes());
            frame
        };

        let mut ws2 = connect("/grpc-ws2").await;
        // A compressed request is inflated, decoded and dispatched; the empty OK reply goes raw.
        let compressed = zlib(&body);
        assert!(compressed.len() < body.len() / 10, "{}", compressed.len());
        send(&mut ws2, encode_request(1, keys, &compressed, true)).await;
        assert_eq!(receive(&mut ws2).await, [1, 0, 0, 0, 0]);
        // A raw body is still accepted there.
        send(&mut ws2, encode_request(2, keys, &body, false)).await;
        assert_eq!(receive(&mut ws2).await, [2, 0, 0, 0, 0]);
        // Inflated exactly to the cap is admitted.
        send(
            &mut ws2,
            encode_request(3, keys, &zlib(&cdn_keys_request(cap)), true),
        )
        .await;
        assert_eq!(receive(&mut ws2).await, [3, 0, 0, 0, 0]);
        // Inflation comes before protobuf decoding.
        send(&mut ws2, encode_request(4, keys, &zlib(&[0xff; 64]), true)).await;
        let reply = receive(&mut ws2).await;
        assert_eq!(
            &reply[..5],
            &[4, 0, 0, 0, tonic::Code::InvalidArgument as u8]
        );
        assert!(String::from_utf8_lossy(&reply[5..]).starts_with("request decode"));
        // A bad or oversized body answers its own id and leaves the socket up.
        send(&mut ws2, encode_request(5, keys, b"not zlib", true)).await;
        let reply = receive(&mut ws2).await;
        assert_eq!(&reply[..5], &[5, 0, 0, 0, tonic::Code::Internal as u8]);
        assert!(String::from_utf8_lossy(&reply[5..]).starts_with(REQUEST_INFLATE_FAILED));
        send(
            &mut ws2,
            encode_request(6, keys, &zlib(&vec![0; cap + 1]), true),
        )
        .await;
        let reply = receive(&mut ws2).await;
        assert_eq!(
            &reply[..5],
            &[6, 0, 0, 0, tonic::Code::ResourceExhausted as u8]
        );
        // Errors stay raw however long, flagged request or not.
        send(&mut ws2, encode_request(7, &unknown, &compressed, true)).await;
        assert_eq!(receive(&mut ws2).await, unknown_reply(7));
        // A push-control frame flagged as compressed is dropped, not decoded (the dashboard sends it raw): these bytes parse as quiet=true, which would hold back the push below. The reply to 10 shows the socket read past it.
        let quiet = proto::PushControlRequest { quiet: true }.encode_to_vec();
        let control = crate::ws_rpc::PUSH_CONTROL;
        send(&mut ws2, encode_request(9, control, &quiet, true)).await;
        send(&mut ws2, encode_request(10, keys, &compressed, true)).await;
        assert_eq!(receive(&mut ws2).await, [10, 0, 0, 0, 0]);
        // Pushes stay raw however large.
        events.send(push_event(200)).unwrap();
        let push = receive(&mut ws2).await;
        assert_eq!(&push[..5], &[0, 0, 0, 0, 0]);
        assert!(push.len() > DEFLATE_MIN_BYTES, "{}", push.len());
        let event = proto::RunVersionsEvent::decode(&push[5..]).unwrap();
        assert_eq!(event.run_versions.len(), 200);

        let mut ws = connect("/grpc-ws").await;
        // /grpc-ws: the frozen encoding, byte for byte.
        send(&mut ws, encode_request(1, keys, &body, false)).await;
        assert_eq!(receive(&mut ws).await, [1, 0, 0, 0, 0]);
        send(&mut ws, encode_request(2, &unknown, &body, false)).await;
        assert_eq!(receive(&mut ws).await, unknown_reply(2));
        // The flag bit is length there: the frame is malformed, dropped unanswered.
        send(&mut ws, encode_request(3, keys, &compressed, true)).await;
        send(&mut ws, encode_request(4, keys, &body, false)).await;
        assert_eq!(receive(&mut ws).await, [4, 0, 0, 0, 0]);
        events.send(push_event(200)).unwrap();
        let push = receive(&mut ws).await;
        assert_eq!(&push[..5], &[0, 0, 0, 0, 0]);
        assert_eq!(
            proto::RunVersionsEvent::decode(&push[5..])
                .unwrap()
                .run_versions
                .len(),
            200
        );

        // /grpc-ws2 shares the origin fence and the wire-revision floor.
        let mut foreign = url("/grpc-ws2", current).into_client_request().unwrap();
        foreign
            .headers_mut()
            .insert(ORIGIN, HeaderValue::from_static("http://elsewhere.example"));
        match tokio_tungstenite::connect_async(foreign).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                assert_eq!(response.status(), StatusCode::FORBIDDEN)
            }
            other => panic!("foreign origin was not refused: {:?}", other.map(|_| ())),
        }
        let (mut stale, _) = tokio_tungstenite::connect_async(url("/grpc-ws2", 1))
            .await
            .unwrap();
        send(&mut stale, encode_request(9, keys, &compressed, true)).await;
        let reply = receive(&mut stale).await;
        assert_eq!(&reply[..5], &[9, 0, 0, 0, tonic::Code::Unavailable as u8]);
        assert_eq!(&reply[5..], crate::ws_rpc::RELOAD_REQUIRED.as_bytes());

        server.abort();
    }

    /// Shutdown's Close reaches every socket kind, /grpc-ws2 with a compressed request answered on it as much as /grpc-ws and the local route, and each closes cleanly: the server waits for the client's reply.
    #[tokio::test]
    async fn every_socket_kind_closes_cleanly_on_shutdown() {
        use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Error, Message as Frame};

        let bumps = crate::ingest::BumpCoalescer::empty_for_test();
        let service = service_without_databases(bumps.clone());
        let origins = AllowedOrigins::parse("http://dashboard.example").unwrap();
        let local = WsState::local(
            service.clone(),
            origins.clone(),
            crate::activity::ActivityTracker::new_local(),
        );
        let app = crate::http_transport::hosted_ws_routes(WsState::hosted(service, origins)).merge(
            axum::Router::new()
                .route("/local", axum::routing::get(ws_handler))
                .with_state(local),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let request = |path: &str| {
            let url = format!(
                "ws://{address}{path}?rev={}",
                crate::ws_rpc::FRONTEND_WIRE_REVISION
            );
            let mut request = url.into_client_request().unwrap();
            // The local route admits only an exact allowed origin.
            request
                .headers_mut()
                .insert(ORIGIN, HeaderValue::from_static("http://dashboard.example"));
            request
        };
        let keys = crate::ws_rpc::QUERY_CDN_KEYS;
        let body = cdn_keys_request(64 * 1024);
        let mut clients = Vec::new();
        for (path, deflated) in [("/grpc-ws", false), ("/grpc-ws2", true), ("/local", false)] {
            let (mut client, _) = tokio_tungstenite::connect_async(request(path))
                .await
                .unwrap();
            let sent = if deflated { zlib(&body) } else { body.clone() };
            send(&mut client, encode_request(1, keys, &sent, deflated)).await;
            assert_eq!(receive(&mut client).await, [1, 0, 0, 0, 0], "{path}");
            clients.push((path, client));
        }
        bumps.request_shutdown();
        for (path, client) in &mut clients {
            let close = loop {
                match client.next().await.unwrap().unwrap() {
                    Frame::Ping(_) => {}
                    Frame::Close(close) => break close,
                    other => panic!("{path}: unexpected message {other:?}"),
                }
            };
            let close = close.unwrap_or_else(|| panic!("{path}: a Close without a code"));
            assert_eq!(
                u16::from(close.code),
                crate::ws_rpc::SHUTDOWN_CLOSE_CODE,
                "{path}"
            );
            // Reading the Close sent the client's reply; the server then ends the connection.
            assert!(
                matches!(
                    client.next().await,
                    None | Some(Err(Error::ConnectionClosed))
                ),
                "{path}"
            );
        }
        // An upgrade during the drain is refused, on either hosted route.
        for path in ["/grpc-ws", "/grpc-ws2"] {
            match tokio_tungstenite::connect_async(request(path)).await {
                Err(Error::Http(response)) => {
                    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}")
                }
                other => panic!("{path}: upgraded while draining: {:?}", other.map(|_| ())),
            }
        }
        server.abort();
    }
}
