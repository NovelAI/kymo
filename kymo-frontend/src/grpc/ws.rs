//! Browser-side WebSocket gRPC transport.
//!
//! All of the dashboard's unary RPCs are multiplexed over ONE WebSocket to the server, sidestepping the browser's ~6-connection cap on plain-HTTP origins that serialized parallel chart fetches (no HTTP/2 without TLS). This changes only the web frontend → backend connection — kymo and every other client still speak native gRPC.
//!
//! One background task owns the socket: callers enqueue (path, body) plus a oneshot for the response, the task assigns a correlation id, sends the frame, and parks the oneshot until the matching response arrives. On disconnect, frames already received are still delivered, a compressed one once it inflates or fails to (CODEC_TIMEOUT_MS); then every parked oneshot is dropped with the connection. Read-only unary calls re-enqueue and ride the reconnect — one socket blip must not blank every in-flight chart, because nothing refetches a finished run's chart until the user touches it. Mutations use `unary_route_no_replay`: a lost response has an unknown outcome and must be reconciled, because replaying after an intervening inverse mutation would no longer be idempotent. Their response wait is finite so a live-but-silent socket cannot park the UI forever; a deadline is also outcome-unknown and is followed by reconciliation, unless the request was still queued, which is reported as not sent. Requests issued while disconnected wait in the queue and flush once the socket is OPEN — minus those whose caller stopped waiting (deadline, unmount), which are dropped at dequeue.
//!
//! Framing, and why it is frozen: ws_rpc.rs. /grpc-ws2 (hosted builds, in a browser with both `CompressionStream` and `DecompressionStream`) adds a compression flag bit per direction; each connect tries it first and falls back to /grpc-ws when its handshake fails ([`connect`]), unless a tab of the origin saw it refused recently ([`ws2_refused_recently`]). A request body is compressed once per call, before queueing (raw if that takes CODEC_TIMEOUT_MS, as is every later one on the page): a replay reuses it, a /grpc-ws connection sends the raw body, and a smaller body from a later call can go out first, which nothing relies on, since every call has its own id and reply and the server runs a socket's requests concurrently. Received frames are delivered in arrival order: a raw one with nothing queued ahead of it at once, while a compressed one holds back every later frame, pushes included, until it is decompressed or fails to within CODEC_TIMEOUT_MS.
//!
//! The URL carries `?rev=FRONTEND_WIRE_REVISION`; a server whose floor is above it refuses every request with RELOAD_REQUIRED, and [`connection_task`] then stops connecting for good while the notice bar asks the user to reload.

use std::collections::{hash_map::Entry, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;

use futures::channel::{mpsc, oneshot};
use futures::stream::FuturesOrdered;
use futures::{select, FutureExt, SinkExt, StreamExt};
use gloo_net::websocket::{futures::WebSocket, Message, State, WebSocketError};
use prost::Message as _;
use tokio::sync::watch;
use tonic::Status;

use super::ws_rpc::{decode_response, encode_request, ResponseFrame, DEFLATE_MIN_BYTES};

/// A response that never arrives (server-side task panic, dropped frame)
/// would otherwise park the caller until the next disconnect. Generous —
/// the slowest real queries are a few seconds.
const REQUEST_TIMEOUT_MS: u64 = 60_000;
/// Maximum elapsed UI wait from enqueue through response. When sent promptly,
/// this covers the longest lifecycle RPC's roughly 120-second server budget;
/// expiry never replays a queued or in-flight mutation.
const MUTATION_RESPONSE_TIMEOUT_MS: u64 = 150_000;
/// How long the browser's codec may take on one body ([`in_time`]). A received frame that takes longer counts as failing to decompress: in-order delivery holds back every later frame behind it, and the drain after a disconnect the reconnect. A request body that takes longer to compress goes raw ([`DEFLATE_STALLED`]); its call waits for it before queueing, so a mutation's deadline starts after it.
const CODEC_TIMEOUT_MS: u64 = 5_000;
/// A /grpc-ws2 socket that closes this soon after opening, or this soon after its first request having heard nothing, is a strike; two in a row make it unusable ([`Ws2Silence`]).
const WS2_UNUSABLE_MS: f64 = 5_000.0;
/// Codec failures one /grpc-ws2 connection may see before it is abandoned for /grpc-ws ([`codec_failed`]): replies that fail to inflate here (or take CODEC_TIMEOUT_MS), and requests whose compressed bodies the server could not inflate. A healthy socket sees none.
const WS2_CODEC_FAILURES: u32 = 3;
/// A reply that fails to inflate answers its request with this: Chromium and Firefox report every bad stream alike ([`codec::inflate`]).
const INFLATE_FAILED: &str =
    "response decompression failed: the payload is not one complete zlib stream, or did not inflate in time";

/// Cumulative server-pushed state used only to seed a new subscriber. Steady
/// state is delivered as [`PushUpdate`] deltas, so an event touching one run
/// never clones or scans everything this tab has seen.
#[derive(Default)]
struct PushSnapshot {
    /// Bumped when cached versions can no longer be trusted: per socket (re)connect (events during the gap are gone) and per server-flagged resync (broadcast lag). Consumers answer with one authoritative PollVersions + refetches.
    resync_gen: u64,
    /// Latest pushed run data versions (same numbers PollVersions returns).
    runs: HashMap<String, u64>,
    /// Latest pushed project versions (run list changed).
    projects: HashMap<String, u64>,
    /// Latest pushed global version (global discovery or Trash visibility changed).
    global: u64,
    /// Per-run counter of metric-discovery invalidations — registry changes
    /// and Restore both require affected panels to re-list metric types.
    metrics_gen: HashMap<String, u64>,
}

/// One subscriber delivery. The first delivery is a cumulative seed; later
/// deliveries contain only entries that rose. Scalar values are optional so
/// an unrelated event cannot look like a global/resync change. All values are
/// absolute, which makes coalescing and duplicate seed/delta races harmless.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PushUpdate {
    pub initial: bool,
    pub resync_gen: Option<u64>,
    pub runs: HashMap<String, u64>,
    pub projects: HashMap<String, u64>,
    pub global: Option<u64>,
    pub metrics_gen: HashMap<String, u64>,
    /// Pushed Last-logged values (RunVersionsEvent.project_last_logged_at_ms), passed through and never seeded: the projects page reads ListProjects after it subscribes.
    pub project_last_logged_at_ms: HashMap<String, i64>,
}

impl PushUpdate {
    fn seed(snapshot: &PushSnapshot) -> Self {
        Self {
            initial: true,
            resync_gen: Some(snapshot.resync_gen),
            runs: snapshot.runs.clone(),
            projects: snapshot.projects.clone(),
            global: Some(snapshot.global),
            metrics_gen: snapshot.metrics_gen.clone(),
            project_last_logged_at_ms: HashMap::new(),
        }
    }

    fn is_empty(&self) -> bool {
        !self.initial
            && self.resync_gen.is_none()
            && self.runs.is_empty()
            && self.projects.is_empty()
            && self.global.is_none()
            && self.metrics_gen.is_empty()
            && self.project_last_logged_at_ms.is_empty()
    }

    /// Collapse unread events into the inbox's single slot. Versions and
    /// counters are absolute and monotonic, so keeping the maximum preserves
    /// the exact state a slow consumer needs without retaining a backlog.
    fn merge(&mut self, newer: Self) {
        self.initial |= newer.initial;
        self.resync_gen = self.resync_gen.max(newer.resync_gen);
        merge_versions(&mut self.runs, newer.runs);
        merge_versions(&mut self.projects, newer.projects);
        self.global = self.global.max(newer.global);
        merge_versions(&mut self.metrics_gen, newer.metrics_gen);
        merge_versions(
            &mut self.project_last_logged_at_ms,
            newer.project_last_logged_at_ms,
        );
    }
}

#[derive(Default)]
struct PushHubState {
    initialized: bool,
    snapshot: PushSnapshot,
    subscribers: Vec<Weak<watch::Sender<PushUpdate>>>,
}

#[derive(Default)]
struct PushHub(std::sync::Mutex<PushHubState>);

impl PushHub {
    fn subscribe(&self) -> PushSubscription {
        let mut state = self.0.lock().unwrap();
        state
            .subscribers
            .retain(|subscriber| subscriber.strong_count() > 0);
        let pending = state.initialized.then(|| PushUpdate::seed(&state.snapshot));
        let inbox = Arc::new(watch::Sender::new(pending.unwrap_or_default()));
        state.subscribers.push(Arc::downgrade(&inbox));
        PushSubscription { inbox }
    }

    fn publish(&self, apply: impl FnOnce(&mut PushSnapshot) -> PushUpdate) {
        let mut state = self.0.lock().unwrap();
        let was_initialized = state.initialized;
        let delta = apply(&mut state.snapshot);
        if delta.is_empty() {
            return;
        }
        let update = if was_initialized {
            delta
        } else {
            PushUpdate::seed(&state.snapshot)
        };
        state.initialized = true;
        // Wakes subscribers while holding the hub lock, which is safe because no waker takes that lock: on wasm a wake only queues the task.
        state.subscribers.retain(|weak| {
            let Some(inbox) = weak.upgrade() else {
                return false;
            };
            inbox.send_modify(|pending| pending.merge(update.clone()));
            true
        });
    }

    /// Merge one server push event into the cumulative seed and publish only the
    /// entries that rose. Postgres versions only grow: an older value — reordered
    /// across a reconnect, or racing the resync poll — must never regress an entry
    /// or re-trigger downstream fetches.
    fn apply_event(&self, ev: super::proto::RunVersionsEvent) {
        self.publish(|snapshot| {
            let mut delta = PushUpdate::default();
            merge_version_delta(&mut snapshot.runs, ev.run_versions, &mut delta.runs);
            merge_version_delta(
                &mut snapshot.projects,
                ev.project_versions,
                &mut delta.projects,
            );
            if ev.global_version > snapshot.global {
                snapshot.global = ev.global_version;
                delta.global = Some(ev.global_version);
            }
            for run in ev.metrics_changed_runs {
                let generation = snapshot.metrics_gen.entry(run.clone()).or_insert(0);
                *generation += 1;
                delta.metrics_gen.insert(run, *generation);
            }
            if ev.resync {
                snapshot.resync_gen += 1;
                delta.resync_gen = Some(snapshot.resync_gen);
            }
            // Passed through without a snapshot (never seeded): ws_proxy max-merges them per socket, and every consumer (PushUpdate::merge, the projects page) raises through merge_versions.
            delta.project_last_logged_at_ms = ev.project_last_logged_at_ms;
            delta
        });
    }

    /// The socket (re)connected: anything cached from before the gap is
    /// suspect, and this also seeds the very first resync on page load.
    fn note_connected(&self) {
        self.publish(|snapshot| {
            snapshot.resync_gen += 1;
            PushUpdate {
                resync_gen: Some(snapshot.resync_gen),
                ..Default::default()
            }
        });
    }
}

/// A page-local cursor over server push state. Each subscription owns one
/// coalescing inbox, so a slow or hidden page wakes once with the newest delta
/// instead of draining an event queue.
pub struct PushSubscription {
    /// Unread seeds and deltas, merged (see `PushUpdate::merge`); empty means nothing is pending.
    inbox: Arc<watch::Sender<PushUpdate>>,
}

impl PushSubscription {
    /// Await the next seed/delta while the page is visible. Updates arriving
    /// during the hidden wait are folded into the same delivery.
    pub async fn next_visible(&mut self) -> PushUpdate {
        // Only this subscription empties its inbox, so it is still non-empty at the take.
        let _ = self
            .inbox
            .subscribe()
            .wait_for(|pending| !pending.is_empty())
            .await;
        wait_until_page_visible().await;
        self.take()
    }

    /// The unread merged update, leaving the inbox empty.
    fn take(&self) -> PushUpdate {
        self.inbox.send_replace(PushUpdate::default())
    }
}

static PUSH: LazyLock<PushHub> = LazyLock::new(Default::default);
/// `document.hidden`, set by the root's visibility bridge; the connection task mirrors it to the server.
static HIDDEN: LazyLock<watch::Sender<bool>> = LazyLock::new(|| watch::Sender::new(false));
/// Written by the connection task, which runs outside the dioxus runtime and can't write signals; read through `connection()`.
static CONNECTED: LazyLock<watch::Sender<(u64, bool)>> =
    LazyLock::new(|| watch::Sender::new((0, false)));
/// Set when the server refuses this bundle's wire revision; the connection task has then stopped for good.
static STALE: AtomicBool = AtomicBool::new(false);

pub fn subscribe_push() -> PushSubscription {
    PUSH.subscribe()
}

/// `(generation, connected)`: the generation bumps on every connect and disconnect, so 0 means the socket has never connected.
pub fn connection() -> (u64, bool) {
    *CONNECTED.borrow()
}

/// Whether the server refused this bundle's wire revision, so the tab no longer connects.
pub fn is_stale() -> bool {
    STALE.load(Ordering::Relaxed)
}

/// Resolves on the first connect or disconnect after generation `seen`.
pub async fn connection_changed(seen: u64) {
    let _ = CONNECTED.subscribe().wait_for(|&(gen, _)| gen > seen).await;
}

/// Sleep `ms` of CONNECTED time: parked while the socket is down, restarted
/// on each reconnect. A request queued through an outage must not time out
/// — its response wasn't lost, it was never sent — while a live socket
/// that stays silent for the full window still fails the caller.
async fn live_deadline(ms: u64) {
    let mut connection = CONNECTED.subscribe();
    loop {
        let _ = connection.wait_for(|&(_, connected)| connected).await;
        select! {
            _ = gloo_timers::future::sleep(Duration::from_millis(ms)).fuse() => return,
            _ = connection.changed().fuse() => {} // disconnected: park and restart
        }
    }
}

fn merge_version_delta(
    into: &mut HashMap<String, u64>,
    from: impl IntoIterator<Item = (String, u64)>,
    delta: &mut HashMap<String, u64>,
) {
    for (key, value) in from {
        if raise(into, key.clone(), value) {
            delta.insert(key, value);
        }
    }
}

/// Raise one map entry monotonically, treating an absent key as a change: a version counter, or a pushed Last-logged value.
pub fn raise<T: Ord>(map: &mut HashMap<String, T>, key: String, value: T) -> bool {
    match map.entry(key) {
        Entry::Vacant(entry) => {
            entry.insert(value);
            true
        }
        Entry::Occupied(mut entry) if value > *entry.get() => {
            entry.insert(value);
            true
        }
        Entry::Occupied(_) => false,
    }
}

/// Per-entry monotonic merge: an incoming value only ever raises. The one
/// implementation of the never-regress rule (push.rs folds through it too).
pub fn merge_versions<T: Ord>(
    into: &mut HashMap<String, T>,
    from: impl IntoIterator<Item = (String, T)>,
) {
    for (key, value) in from {
        raise(into, key, value);
    }
}

pub(super) fn page_hidden() -> bool {
    *HIDDEN.borrow()
}

/// Resolves once the page is visible (immediately if it already is).
pub async fn wait_until_page_visible() {
    let _ = HIDDEN.subscribe().wait_for(|hidden| !hidden).await;
}

/// Record a visibility flip (from the root component's visibilitychange
/// listener). The server-side quiet mirror is NOT sent from here —
/// connection_task owns it, level-triggered off this flag: a control frame
/// is a state mutation, and riding the unary path's replay-on-reconnect
/// could re-enqueue opposite flips in drop order, leaving the server quiet
/// on a visible tab.
pub fn set_page_visibility(hidden: bool) {
    HIDDEN.send_replace(hidden);
}

struct Pending {
    path: &'static str,
    body: Vec<u8>,
    /// `body` compressed, sent in its place on /grpc-ws2.
    deflated: Option<Vec<u8>>,
    resp: oneshot::Sender<Result<Vec<u8>, Status>>,
    /// Set just before the frame is written: a no-replay call whose deadline passes while this is unset was never sent.
    sent: Arc<AtomicBool>,
}

/// A mutation that expired before it was sent. Nothing changed on the server, so there is nothing to reconcile.
const NOT_SENT: &str = "no connection to the server; nothing was sent";

/// Whether the server never ran the request: it expired unsent, or the server refused this bundle, which dispatches nothing.
/// Matched by message, so neither text may ever describe a request the server ran.
pub fn not_sent(status: &Status) -> bool {
    status.code() == tonic::Code::Unavailable
        && [NOT_SENT, super::ws_rpc::RELOAD_REQUIRED].contains(&status.message())
}

#[derive(Clone)]
pub struct WsClient {
    tx: mpsc::UnboundedSender<Pending>,
}

static SINGLETON: LazyLock<WsClient> = LazyLock::new(|| {
    let (tx, rx) = mpsc::unbounded();
    wasm_bindgen_futures::spawn_local(connection_task(rx));
    WsClient { tx }
});

impl WsClient {
    /// The process-wide client. First call spawns the connection task.
    pub fn singleton() -> WsClient {
        SINGLETON.clone()
    }

    pub async fn unary_route<Req, Resp>(
        &self,
        route: super::routes::Rpc<Req, Resp>,
        req: Req,
    ) -> Result<Resp, Status>
    where
        Req: prost::Message,
        Resp: prost::Message + Default,
    {
        self.unary_inner(route.path, req, true).await
    }

    /// Send a mutation at most once. Queueing and response share one absolute
    /// deadline; once sent, a connection loss is surfaced as outcome-unknown
    /// instead of transparently replaying the request.
    pub async fn unary_route_no_replay<Req, Resp>(
        &self,
        route: super::routes::Rpc<Req, Resp>,
        req: Req,
    ) -> Result<Resp, Status>
    where
        Req: prost::Message,
        Resp: prost::Message + Default,
    {
        // A mutation, or the read that verifies one, is something the user just did: try the socket now rather than at the next scheduled retry (intent shown while connected is discarded when the socket drops).
        super::reconnect::wake(true);
        self.unary_inner(route.path, req, false).await
    }

    async fn unary_inner<Req, Resp>(
        &self,
        path: &'static str,
        req: Req,
        replay_on_disconnect: bool,
    ) -> Result<Resp, Status>
    where
        Req: prost::Message,
        Resp: prost::Message + Default,
    {
        let body = req.encode_to_vec();
        let deflated = deflate_request(&body).await;
        // Read-only calls retain their connected-time behavior so an outage
        // does not blank finished charts. A mutation's longer deadline is
        // absolute from enqueue through response: if it expires while queued,
        // dropping this receiver makes the connection task discard it without sending;
        // if already sent, reconciliation observes the outcome without replay.
        let mut timeout = std::pin::pin!(async {
            if replay_on_disconnect {
                live_deadline(REQUEST_TIMEOUT_MS).await;
            } else {
                gloo_timers::future::sleep(Duration::from_millis(MUTATION_RESPONSE_TIMEOUT_MS))
                    .await;
            }
        }
        .fuse());
        // A dropped oneshot is the only transport-loss signal — the socket
        // died with this request in flight (server errors arrive as values).
        // Read-only calls re-enqueue and ride the reconnect. Mutations return
        // outcome-unknown so their caller can reconcile authoritative state.
        loop {
            let (resp_tx, resp_rx) = oneshot::channel();
            let sent = Arc::new(AtomicBool::new(false));
            self.tx
                .unbounded_send(Pending {
                    path,
                    body: body.clone(),
                    deflated: deflated.clone(),
                    resp: resp_tx,
                    sent: sent.clone(),
                })
                // The connection task ends only when the server refused this bundle.
                .map_err(|_| Status::unavailable(super::ws_rpc::RELOAD_REQUIRED))?;
            let mut resp_rx = resp_rx.fuse();
            select! {
                r = resp_rx => match r {
                    Ok(Ok(bytes)) => {
                        return Resp::decode(bytes.as_slice())
                            .map_err(|e| Status::internal(format!("response decode: {e}")));
                    }
                    Ok(Err(status)) => return Err(status),
                    Err(_connection_dropped) if replay_on_disconnect => {}
                    // A refused socket dispatches nothing, so the refusal leaves no unknown outcome.
                    Err(_connection_dropped) if is_stale() => {
                        return Err(Status::unavailable(super::ws_rpc::RELOAD_REQUIRED));
                    }
                    Err(_connection_dropped) => {
                        return Err(Status::unavailable(
                            "connection lost; mutation outcome is unknown",
                        ));
                    }
                },
                _ = timeout => {
                    // Exact on single-threaded wasm: the connection task can't dequeue the request between the `sent` check and the return that drops the receiver.
                    return Err(if replay_on_disconnect {
                        Status::deadline_exceeded("ws request timed out")
                    } else if sent.load(Ordering::Relaxed) {
                        Status::deadline_exceeded("mutation response timed out; outcome is unknown")
                    } else {
                        Status::unavailable(NOT_SENT)
                    });
                },
            }
        }
    }
}

/// The browser's own zlib: `CompressionStream`/`DecompressionStream` with "deflate", which is RFC 1950. web-sys gates both behind `web_sys_unstable_apis`, which changes other bindings crate-wide, so they are bound here.
mod codec {
    use std::sync::LazyLock;

    use wasm_bindgen::prelude::*;
    use wasm_bindgen_futures::JsFuture;

    #[wasm_bindgen]
    extern "C" {
        type CompressionStream;
        #[wasm_bindgen(catch, constructor)]
        fn new(format: &str) -> Result<CompressionStream, JsValue>;

        type DecompressionStream;
        #[wasm_bindgen(catch, constructor)]
        fn new(format: &str) -> Result<DecompressionStream, JsValue>;
    }

    /// Both directions exist.
    pub(super) static AVAILABLE: LazyLock<bool> = LazyLock::new(|| {
        CompressionStream::new("deflate").is_ok() && DecompressionStream::new("deflate").is_ok()
    });

    pub(super) async fn deflate(bytes: &[u8]) -> Option<Vec<u8>> {
        let codec = CompressionStream::new("deflate").ok()?;
        transform(bytes, codec.unchecked_ref()).await.ok()
    }

    /// Chromium, Firefox and WebKit reject anything but exactly one complete zlib stream (Node's DecompressionStream accepts a trailing byte). The collecting Response reports a bad header, a truncation or trailing bytes alike in Chromium ("Failed to fetch") and Firefox (an AbortError), so None carries no detail ([`super::INFLATE_FAILED`]).
    pub(super) async fn inflate(bytes: Vec<u8>) -> Option<Vec<u8>> {
        let codec = DecompressionStream::new("deflate").ok()?;
        transform(&bytes, codec.unchecked_ref()).await.ok()
    }

    /// Pipe `bytes` through one codec (its `readable` and `writable` make it a `ReadableWritablePair`) and collect its output.
    async fn transform(
        bytes: &[u8],
        codec: &web_sys::ReadableWritablePair,
    ) -> Result<Vec<u8>, JsValue> {
        let parts = js_sys::Array::of1(&js_sys::Uint8Array::from(bytes));
        let input = web_sys::Blob::new_with_u8_array_sequence(&parts)?;
        let output = input.stream().pipe_through(codec);
        let collected = web_sys::Response::new_with_opt_readable_stream(Some(&output))?;
        let buffer = JsFuture::from(collected.array_buffer()?).await?;
        Ok(js_sys::Uint8Array::new(&buffer).to_vec())
    }
}

/// The /grpc-ws2 URL, when this build has one (hosted) and the browser has both codecs.
fn deflate_socket_url() -> Option<&'static str> {
    crate::runtime::config()
        .deflate_websocket_url
        .as_deref()
        .filter(|_| *codec::AVAILABLE)
}

/// Set once a request body has taken CODEC_TIMEOUT_MS to compress: a CompressionStream that does not settle would cost every later call the same wait, so the rest of the page sends raw bodies.
static DEFLATE_STALLED: AtomicBool = AtomicBool::new(false);

/// `body` compressed for /grpc-ws2, when that socket is possible here, the body is at least DEFLATE_MIN_BYTES, no earlier body has stalled ([`DEFLATE_STALLED`]), and compressing makes it smaller and takes under CODEC_TIMEOUT_MS.
async fn deflate_request(body: &[u8]) -> Option<Vec<u8>> {
    deflate_socket_url()?;
    if body.len() < DEFLATE_MIN_BYTES || DEFLATE_STALLED.load(Ordering::Relaxed) {
        return None;
    }
    let mut settled = false;
    let deflated = in_time(codec::deflate(body).inspect(|_| settled = true)).await;
    DEFLATE_STALLED.fetch_or(!settled, Ordering::Relaxed);
    deflated.filter(|deflated| deflated.len() < body.len())
}

/// One codec pass ([`codec::deflate`], [`codec::inflate`]), None once it has taken CODEC_TIMEOUT_MS.
async fn in_time(codec: impl std::future::Future<Output = Option<Vec<u8>>>) -> Option<Vec<u8>> {
    select! {
        out = codec.fuse() => out,
        _ = gloo_timers::future::sleep(Duration::from_millis(CODEC_TIMEOUT_MS)).fuse() => None,
    }
}

/// A received frame, resolved once its payload is decompressed. Arrivals behind a compressed frame are queued in a `FuturesOrdered`, so a raw frame never overtakes a compressed one received before it.
async fn arrival<F>(
    frame: ResponseFrame,
    inflate: impl FnOnce(Vec<u8>) -> F,
) -> (u32, Result<Vec<u8>, Status>)
where
    F: std::future::Future<Output = Option<Vec<u8>>>,
{
    let mut payload = frame.payload;
    if frame.deflated {
        let Some(inflated) = inflate(payload).await else {
            return (frame.id, Err(Status::internal(INFLATE_FAILED)));
        };
        payload = inflated;
    }
    (frame.id, reply(frame.code, payload))
}

/// A response's result from its (decompressed) payload: a protobuf for code 0, an error message for any other.
fn reply(code: u8, payload: Vec<u8>) -> Result<Vec<u8>, Status> {
    match code {
        0 => Ok(payload),
        code => Err(Status::new(
            tonic::Code::from(code as i32),
            String::from_utf8_lossy(&payload),
        )),
    }
}

/// Whether a result says the codec failed on this connection: a reply that failed to inflate here, or the server's answer to a request body that failed to inflate there (a broken compressor, or something on the way that corrupts compressed frames).
fn codec_failed(result: &Result<Vec<u8>, Status>) -> bool {
    matches!(result, Err(status) if status.message() == INFLATE_FAILED
        || (status.code() == tonic::Code::Internal
            && status.message().starts_with(super::ws_rpc::REQUEST_INFLATE_FAILED)))
}

/// Hand one received frame to its caller, or a push to the hub; a refusal of this bundle's wire revision sets STALE instead.
fn deliver(
    id: u32,
    result: Result<Vec<u8>, Status>,
    inflight: &mut HashMap<u32, oneshot::Sender<Result<Vec<u8>, Status>>>,
) {
    // The refusal ends the transport instead of answering its request, so callers only ever see Unavailable, which every view retries while keeping what it shows, whatever code the server sent. Any request can carry it, push control included (a hidden tab's first frame).
    if matches!(&result, Err(s) if s.message() == super::ws_rpc::RELOAD_REQUIRED) {
        STALE.store(true, Ordering::Relaxed);
        return;
    }
    if id == 0 {
        // Server push (see ws_proxy.rs): id 0 carries a RunVersionsEvent, never a response to a request.
        if let Ok(payload) = result {
            match super::proto::RunVersionsEvent::decode(payload.as_slice()) {
                Ok(ev) => PUSH.apply_event(ev),
                Err(e) => crate::util::warn(&format!("bad push frame: {e}")),
            }
        }
    } else if let Some(tx) = inflight.remove(&id) {
        let _ = tx.send(result);
    }
}

/// What this tab knows about /grpc-ws2 across connects.
#[derive(Default)]
struct Ws2State {
    /// It has worked here.
    reached: bool,
    /// The last /grpc-ws2 connection was open but unusable: a second strike in a row ([`Ws2Silence`]), or WS2_CODEC_FAILURES codec failures ([`codec_failed`]). The next connect goes straight to /grpc-ws.
    unusable: bool,
    /// The last /grpc-ws2 connection was a strike ([`Ws2Silence`]).
    struck: bool,
}

impl Ws2State {
    /// Whether this connect may try /grpc-ws2: not right after an unusable /grpc-ws2 connection, nor while a refusal is recent ([`ws2_refused_recently`]). The connect spends the unusable connection's skip even when a recent refusal keeps it off /grpc-ws2 as well, so the skip never outlasts that connect.
    fn may_try(&mut self, refused_recently: impl FnOnce() -> bool) -> bool {
        !std::mem::take(&mut self.unusable) && !refused_recently()
    }
}

/// One /grpc-ws2 connection, judged at close. A close within WS2_UNUSABLE_MS of opening, or of the first request with nothing heard, is a strike; two in a row send the next connect to /grpc-ws. That catches a proxy that upgrades and then cuts the socket, even one passing frames. One strike is tolerated: a hidden tab's sound socket can drop right after a reconnect, as any can in a server crash or network drop, and settling on /grpc-ws would last that connection's life, possibly days; two such drops in a row put one connection there. The server's shutdown Close (SHUTDOWN_CLOSE_CODE) never strikes and breaks a run; a proxy's close lacks that code.
#[derive(Default)]
struct Ws2Silence {
    /// When this /grpc-ws2 socket opened (None on /grpc-ws).
    opened: Option<f64>,
    /// When its first request expecting a reply went out.
    first_request: Option<f64>,
    /// A frame has arrived on it: the socket works.
    heard: bool,
    /// The server's shutdown Close ended it.
    shut_down: bool,
}

impl Ws2Silence {
    fn new(opened: Option<f64>) -> Self {
        Self {
            opened,
            ..Self::default()
        }
    }

    /// A request that expects a reply was sent at `now`.
    fn sent(&mut self, now: f64) {
        self.first_request.get_or_insert(now);
    }

    /// The socket closed at `now`: record what that says about /grpc-ws2.
    fn closed(&self, now: f64, ws2: &mut Ws2State) {
        let Some(opened) = self.opened else { return };
        let soon_after = |at: f64| now - at < WS2_UNUSABLE_MS;
        let silent = !self.heard && self.first_request.is_some_and(soon_after);
        let strike = !self.shut_down && (soon_after(opened) || silent);
        ws2.unusable |= strike && ws2.struck;
        ws2.struck = strike && !ws2.unusable;
    }
}

/// One connect's socket, and whether it is /grpc-ws2 (`deflate_url`, when this connect may try it). /grpc-ws2 goes first; a failed handshake falls back to /grpc-ws at once (an older server, or the server being down), which Firefox does not delay, since it keeps its failure records per path. Once /grpc-ws2 has `reached` here, that failure is retried after /grpc-ws answers: a server coming back from a deploy can refuse the first handshake and accept the second, and hosted sockets are never dropped for silence, so settling early would last the connection's life.
async fn connect<'a, S, F>(
    url: &'a str,
    deflate_url: Option<&'a str>,
    reached: &mut bool,
    mut open: impl FnMut(&'a str) -> F,
) -> Option<(S, bool)>
where
    F: std::future::Future<Output = Option<S>>,
{
    if let Some(deflate_url) = deflate_url {
        if let Some(ws) = open(deflate_url).await {
            *reached = true;
            return Some((ws, true));
        }
    }
    let plain = open(url).await?;
    if let Some(deflate_url) = deflate_url.filter(|_| *reached) {
        if let Some(ws) = open(deflate_url).await {
            // Dropping the unused /grpc-ws socket closes it.
            return Some((ws, true));
        }
    }
    Some((plain, false))
}

/// The `Date.now()` of the latest /grpc-ws2 refusal in any of the origin's tabs: a connect that tried /grpc-ws2 and settled on /grpc-ws (an older server, or a proxy that refuses the route). A socket the server closes with SHUTDOWN_CLOSE_CODE removes it: the server that refused is going away. In a deploy, which moves the frontend and server images together, a tab can load the new bundle while the old server still runs and be refused; without the removal, every tab of the origin would then reconnect to the new server on /grpc-ws and stay there for that connection's life, which can be days.
const WS2_REFUSED_KEY: &str = "kymo_ws2_refused";

/// Whether a tab of this origin saw /grpc-ws2 refused in the last SLOW_MS, in which case no tab tries it. Firefox delays a handshake to a path that failed recently ([`super::reconnect`]) by what remains of a delay counted from the last failure, 200-400 ms and 1.5 times longer per failure, so tabs reconnecting together behind a server or proxy that refuses /grpc-ws2 would each try it and wait seconds. Refusals are spaced SLOW_MS apart however many tabs reconnect, and Firefox has forgotten one by the next; a tab that settled on /grpc-ws tries /grpc-ws2 again on its first connect after that, or after the server's shutdown Close ([`WS2_REFUSED_KEY`]), so a server that still refuses it is tried once per restart.
fn ws2_refused_recently() -> bool {
    crate::util::local_storage::get(WS2_REFUSED_KEY)
        .and_then(|at| at.parse::<f64>().ok())
        .is_some_and(|at| (js_sys::Date::now() - at).abs() < super::reconnect::SLOW_MS)
}

/// Open a socket and wait out its handshake; None when it never reaches OPEN.
async fn open_socket(url: &str) -> Option<WebSocket> {
    let mut ws = WebSocket::open(url).ok()?;
    // Settle the handshake before dequeuing anything: the schedule needs this attempt's outcome, and a send on a socket that never opened is silently dropped, so a mutation would be reported outcome-unknown though never sent.
    // The socket's open or error event wakes this rather than a timer, which a hidden tab can have throttled to one a minute while other tabs wait on this attempt.
    // The timeout catches a socket that errs without leaving CONNECTING (WebKit, on a port it blocks), which no event would wake again; it outlasts Firefox's longest delay plus its 20 s handshake timeout, and stays within the claim each socket renews.
    select! {
        _ = futures::future::poll_fn(|cx| ws.poll_ready_unpin(cx)).fuse() => {}
        _ = gloo_timers::future::sleep(Duration::from_secs(90)).fuse() => {}
    }
    matches!(ws.state(), State::Open).then_some(ws)
}

async fn connection_task(mut requests: mpsc::UnboundedReceiver<Pending>) {
    let mut next_id: u32 = 0;
    let rev = super::ws_rpc::FRONTEND_WIRE_REVISION;
    let url = format!("{}?rev={rev}", crate::runtime::config().websocket_url);
    let deflate_url = deflate_socket_url().map(|url| format!("{url}?rev={rev}"));
    let mut ws2 = Ws2State::default();
    let mut reconnect = super::reconnect::Reconnect::new();
    loop {
        reconnect.turn().await;
        let ws2_url = deflate_url
            .as_deref()
            .filter(|_| ws2.may_try(ws2_refused_recently));
        let opened = connect(&url, ws2_url, &mut ws2.reached, |url| {
            reconnect.opening();
            open_socket(url)
        })
        .await;
        let Some((ws, deflate)) = opened else {
            reconnect.failed();
            continue;
        };
        if ws2_url.is_some() && !deflate {
            crate::util::local_storage::set(WS2_REFUSED_KEY, &js_sys::Date::now().to_string());
        }
        reconnect.opened();
        let mut silence = Ws2Silence::new(deflate.then(js_sys::Date::now));
        let mut codec_failures = 0;
        // Fresh connection: push events during the gap are lost, so tell
        // consumers to resync. Also fires on the FIRST connect, seeding the
        // page's initial version fetch.
        PUSH.note_connected();
        CONNECTED.send_modify(|(gen, connected)| (*gen, *connected) = (*gen + 1, true));
        let (mut sink, stream) = ws.split();
        let mut stream = stream.fuse();
        // Server-side quiet mirror, level-triggered: converge what this
        // CONNECTION last told the server (fresh connections start
        // un-quieted) to the hidden flag — the visibility arm fires whenever
        // the flag differs from what was last sent, so a hidden tab's fresh
        // connection sends at once. Sending only the latest state (no
        // queued per-flip messages) makes reordering impossible. Control
        // frames take an id but expect no reply (a refusing server still answers them).
        let mut sent_hidden = false;
        let mut visibility = HIDDEN.subscribe();
        // Parked oneshots, by correlation id. Dropped wholesale on
        // disconnect. Read-only unary calls re-enqueue; no-replay mutation
        // calls surface the lost response for authoritative reconciliation.
        let mut inflight: HashMap<u32, oneshot::Sender<Result<Vec<u8>, Status>>> = HashMap::new();
        // Received frames behind a compressed one, delivered in arrival order (see `arrival`).
        let mut arrivals = FuturesOrdered::new();
        loop {
            // A frame to hand over now: a raw one with none queued ahead of it, or the next arrival once inflated.
            let (id, result) = select! {
                pending = requests.next() => {
                    let Some(p) = pending else { return };
                    // A canceled oneshot means the caller stopped waiting —
                    // it timed out, or its component unmounted. Requests
                    // queued through a disconnect outlive their callers'
                    // timeouts, so without this check every reconnect
                    // replayed the whole stale backlog as a burst of
                    // expensive queries nobody reads.
                    if p.resp.is_canceled() {
                        continue;
                    }
                    // id 0 is the server-push channel; a request wearing it would have its response dropped as an event.
                    next_id = next_id.wrapping_add(1).max(1);
                    let frame = match (&p.deflated, deflate) {
                        (Some(deflated), true) => encode_request(next_id, p.path, deflated, true),
                        _ => encode_request(next_id, p.path, &p.body, false),
                    };
                    p.sent.store(true, Ordering::Relaxed);
                    if sink.send(Message::Bytes(frame)).await.is_err() {
                        // Socket died mid-send; this request's oneshot drops here, never having reached the inflight map below.
                        break;
                    }
                    silence.sent(js_sys::Date::now());
                    inflight.insert(next_id, p.resp);
                    continue;
                }
                _ = visibility.wait_for(|&hidden| hidden != sent_hidden).map(drop).fuse() => {
                    sent_hidden = *HIDDEN.borrow();
                    next_id = next_id.wrapping_add(1).max(1);
                    let body =
                        super::proto::PushControlRequest { quiet: sent_hidden }.encode_to_vec();
                    let frame =
                        encode_request(next_id, super::ws_rpc::PUSH_CONTROL, &body, false);
                    if sink.send(Message::Bytes(frame)).await.is_err() {
                        break;
                    }
                    continue;
                }
                msg = stream.next() => {
                    let msg = match msg {
                        Some(Ok(msg)) => msg,
                        // The server's shutdown Close: the socket reached the server, so its end says nothing against /grpc-ws2, and a server that refused /grpc-ws2 is going away ([`WS2_REFUSED_KEY`]).
                        Some(Err(WebSocketError::ConnectionClose(close))) if close.code == super::ws_rpc::SHUTDOWN_CLOSE_CODE => {
                            silence.shut_down = true;
                            crate::util::local_storage::remove(WS2_REFUSED_KEY);
                            break;
                        }
                        _ => break,
                    };
                    silence.heard = true;
                    let Message::Bytes(buf) = msg else { continue };
                    let Some(frame) = decode_response(buf, deflate) else { continue };
                    if frame.deflated || !arrivals.is_empty() {
                        arrivals.push_back(arrival(frame, |bytes| in_time(codec::inflate(bytes))));
                        continue;
                    }
                    (frame.id, reply(frame.code, frame.payload))
                }
                arrived = arrivals.select_next_some() => arrived,
            };
            codec_failures += u32::from(codec_failed(&result));
            deliver(id, result, &mut inflight);
            if is_stale() {
                break;
            }
            if codec_failures == WS2_CODEC_FAILURES {
                ws2.unusable = true;
                break;
            }
        }
        // Close the socket before the drain and the reconnect delay: one abandoned after codec failures is still open, and the server would keep answering it. The drain needs only frames already received.
        drop((sink, stream));
        silence.closed(js_sys::Date::now(), &mut ws2);
        // Disconnected from here, though the drain below can still answer requests for up to CODEC_TIMEOUT_MS: a reload asked for meanwhile would go out beside the reconnect's resync (`request_refresh` in state/app_state.rs).
        CONNECTED.send_modify(|(gen, connected)| (*gen, *connected) = (*gen + 1, false));
        if is_stale() {
            // Stop for good, never reload by ourselves: dropping `requests` fails every waiting and later call at once, and the notice bar asks the user to reload.
            return;
        }
        // Before the drain, as once the socket is gone: intent shown during it (a focus, `online`, a mutation) makes the next attempt at once.
        reconnect.dropped();
        // Frames that arrived before the socket closed still answer their requests; a compressed one may still be decompressing, for at most CODEC_TIMEOUT_MS.
        while let Some((id, result)) = arrivals.next().await {
            deliver(id, result, &mut inflight);
            if is_stale() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod framing_tests {
    use super::super::ws_rpc::{encode_response as response, RESPONSE_DEFLATED};
    use super::*;

    fn not_compressed(_: Vec<u8>) -> futures::future::Ready<Option<Vec<u8>>> {
        panic!("a raw frame was decompressed")
    }

    #[test]
    fn errors_and_pushes_resolve_raw() {
        for ws2 in [false, true] {
            let error = decode_response(response(3, tonic::Code::NotFound as u8, b"gone"), ws2);
            let (id, result) = arrival(error.unwrap(), not_compressed)
                .now_or_never()
                .unwrap();
            let status = result.unwrap_err();
            assert_eq!(
                (id, status.code(), status.message()),
                (3, tonic::Code::NotFound, "gone")
            );
            let push = decode_response(response(0, 0, b"event"), ws2);
            assert!(matches!(
                arrival(push.unwrap(), not_compressed).now_or_never(),
                Some((0, Ok(payload))) if payload == b"event"
            ));
        }
    }

    #[test]
    fn a_compressed_frame_holds_back_every_later_frame() {
        let (release, gate) = oneshot::channel::<Vec<u8>>();
        // Boxed: the two inflate stubs make the arrivals different types.
        let mut arrivals = FuturesOrdered::new();
        let compressed = decode_response(response(1, RESPONSE_DEFLATED, b"zlib"), true).unwrap();
        arrivals.push_back(
            arrival(compressed, move |payload| {
                assert_eq!(payload, b"zlib");
                async move { gate.await.ok() }
            })
            .boxed_local(),
        );
        let push = decode_response(response(0, 0, b"event"), true).unwrap();
        arrivals.push_back(arrival(push, not_compressed).boxed_local());
        let raw = decode_response(response(2, 0, b"raw"), true).unwrap();
        arrivals.push_back(arrival(raw, not_compressed).boxed_local());

        assert!(arrivals.next().now_or_never().is_none());
        release.send(b"inflated".to_vec()).unwrap();
        for (expected_id, expected) in [(1, &b"inflated"[..]), (0, b"event"), (2, b"raw")] {
            assert!(matches!(
                arrivals.next().now_or_never(),
                Some(Some((id, Ok(payload)))) if id == expected_id && payload == expected
            ));
        }
    }

    /// One `connect` against scripted handshake outcomes, in order: the route it settled on, if any, and every route it tried.
    fn connect_scripted(
        deflate: bool,
        reached: &mut bool,
        outcomes: &[bool],
    ) -> (Option<(&'static str, bool)>, Vec<&'static str>) {
        let mut outcomes = outcomes.iter().copied();
        let mut tried = Vec::new();
        let opened = connect("/grpc-ws", deflate.then_some("/grpc-ws2"), reached, |url| {
            tried.push(url);
            let opens = outcomes.next().expect("an unscripted handshake");
            futures::future::ready(opens.then_some(url))
        })
        .now_or_never()
        .unwrap();
        assert_eq!(outcomes.next(), None, "a scripted handshake was not tried");
        (opened, tried)
    }

    #[test]
    fn a_tab_that_reached_grpc_ws2_retries_it_before_settling_on_grpc_ws() {
        let (ws, ws2) = ("/grpc-ws", "/grpc-ws2");
        // Never reached: a failed /grpc-ws2 handshake falls back at once, as against an older server, and settling on /grpc-ws after trying /grpc-ws2 is a refusal.
        let mut reached = false;
        assert_eq!(
            connect_scripted(true, &mut reached, &[false, true]),
            (Some((ws, false)), vec![ws2, ws])
        );
        // Both down is an outage, not a refusal.
        assert_eq!(
            connect_scripted(true, &mut reached, &[false, false]),
            (None, vec![ws2, ws])
        );
        assert!(!reached);
        assert_eq!(
            connect_scripted(true, &mut reached, &[true]),
            (Some((ws2, true)), vec![ws2])
        );
        assert!(reached);
        // The server came back between the first two handshakes (a deploy): back on /grpc-ws2.
        assert_eq!(
            connect_scripted(true, &mut reached, &[false, true, true]),
            (Some((ws2, true)), vec![ws2, ws, ws2])
        );
        // /grpc-ws2 fails while /grpc-ws works (rolled back to an older server): /grpc-ws.
        assert_eq!(
            connect_scripted(true, &mut reached, &[false, true, false]),
            (Some((ws, false)), vec![ws2, ws, ws2])
        );
        // Server down: no retry until /grpc-ws answers.
        assert_eq!(
            connect_scripted(true, &mut reached, &[false, false]),
            (None, vec![ws2, ws])
        );
        // No /grpc-ws2 for this connect (local mode, no codec, a recent refusal in any tab, or an unusable last connection): /grpc-ws only, which refuses nothing.
        assert_eq!(
            connect_scripted(false, &mut reached, &[true]),
            (Some((ws, false)), vec![ws])
        );
    }

    #[test]
    fn grpc_ws2_closes_soon_after_opening_strike_and_two_in_a_row_sit_out_one_connect() {
        let mut state = Ws2State::default();
        // The connection's life, ending at `closed`, decides whether the next connect skips /grpc-ws2, which spends the skip. Every socket opened at 0.
        let mut skips = |silence: Ws2Silence, closed: f64| {
            silence.closed(closed, &mut state);
            !state.may_try(|| false)
        };
        let silent = || Ws2Silence::new(Some(0.0));
        // A hidden tab sends no requests, so one drop right after reconnecting is only a strike.
        assert!(!skips(silent(), 300.0));
        // A socket that lived WS2_UNUSABLE_MS is no strike and breaks the run.
        assert!(!skips(silent(), WS2_UNUSABLE_MS));
        assert!(!skips(silent(), 300.0));
        // A second close in a row soon after opening is a proxy that upgrades and then closes: one connect on /grpc-ws, then /grpc-ws2 again with no strike.
        assert!(skips(silent(), 300.0));
        assert!(!skips(Ws2Silence::new(None), 300.0));
        assert!(!skips(silent(), 300.0));
        // Heard or not: a proxy that passes frames and then cuts the socket strikes too, so two such sockets in a row settle the next connect on /grpc-ws.
        let answered = || {
            let mut silence = silent();
            silence.sent(0.0);
            silence.heard = true;
            silence.sent(100.0);
            silence
        };
        assert!(skips(answered(), 300.0));
        assert!(!skips(Ws2Silence::new(None), 300.0));
        // One alone, as from a server that crashed right after the tab reconnected, leaves the tab on /grpc-ws2.
        assert!(!skips(answered(), 300.0));
        // A socket that worked past WS2_UNUSABLE_MS is no strike, however soon after a request it closes: a server going away mid-reply.
        assert!(!skips(answered(), WS2_UNUSABLE_MS + 100.0));
        // The server's shutdown Close is no strike, and breaks the run: clean restarts in a row keep the tab on /grpc-ws2.
        let shut_down = || {
            let mut silence = silent();
            silence.shut_down = true;
            silence
        };
        assert!(!skips(silent(), 300.0));
        assert!(!skips(shut_down(), 300.0));
        assert!(!skips(silent(), 300.0));
        assert!(!skips(shut_down(), 300.0));
        assert!(!skips(shut_down(), 300.0));
        // A close with requests unanswered is a strike too: one alone is a server that crashed as the tab reconnected, and the tab stays on /grpc-ws2.
        let unanswered = || {
            let mut silence = silent();
            silence.sent(0.0);
            silence.sent(100.0);
            silence
        };
        assert!(!skips(unanswered(), 300.0));
        assert!(skips(unanswered(), 300.0));
        assert!(!skips(Ws2Silence::new(None), 300.0));
        // Every kind of strike counts toward two in a row.
        assert!(!skips(silent(), 300.0));
        assert!(skips(answered(), 300.0));
        assert!(!skips(Ws2Silence::new(None), 300.0));
        // With nothing heard, silence also counts from the first request: a close WS2_UNUSABLE_MS after it ends a slow reply, not a refused socket, and a close soon after a late first request strikes however long the socket was open.
        let mut slow = silent();
        slow.sent(1_000.0);
        slow.sent(2_000.0);
        assert!(!skips(slow, 1_000.0 + WS2_UNUSABLE_MS));
        let mut heard_late = silent();
        heard_late.heard = true;
        heard_late.sent(10_000.0);
        assert!(!skips(heard_late, 10_300.0));
        let mut late = silent();
        late.sent(10_000.0);
        assert!(!skips(late, 10_300.0));
        // /grpc-ws is never judged, and keeps the strike.
        let mut plain = Ws2Silence::new(None);
        plain.sent(0.0);
        assert!(!skips(plain, 300.0));
        assert!(skips(silent(), 300.0));
        // A refusal recorded meanwhile, by any tab, keeps the connect after an unusable connection off /grpc-ws2 too, and that connect spends the skip: once the record lapses, the next connect tries /grpc-ws2.
        silent().closed(300.0, &mut state);
        silent().closed(300.0, &mut state);
        assert!(!state.may_try(|| true));
        assert!(state.may_try(|| false));
    }

    #[test]
    fn a_frame_that_fails_to_decompress_answers_its_request() {
        let compressed = decode_response(response(4, RESPONSE_DEFLATED, b"junk"), true).unwrap();
        let (id, result) = arrival(compressed, |_| futures::future::ready(None))
            .now_or_never()
            .unwrap();
        let status = result.unwrap_err();
        assert_eq!((id, status.code()), (4, tonic::Code::Internal));
        assert_eq!(
            status.message(),
            "response decompression failed: the payload is not one complete zlib stream, or did not inflate in time"
        );
    }

    #[test]
    fn codec_failures_in_either_direction_count_against_the_route() {
        let failed =
            |code: tonic::Code, message: &str| codec_failed(&Err(Status::new(code, message)));
        assert!(failed(tonic::Code::Internal, INFLATE_FAILED));
        // The server's answer to a compressed request body it could not inflate.
        assert!(failed(
            tonic::Code::Internal,
            "request body inflate failed: invalid stored block lengths"
        ));
        // Not the codec: a body past the cap, other errors, and any reply that arrived.
        assert!(!failed(
            tonic::Code::ResourceExhausted,
            "inflated request body exceeds 4194304 bytes"
        ));
        assert!(!failed(tonic::Code::Internal, "query failed"));
        assert!(!failed(
            tonic::Code::Unavailable,
            "request body inflate failed: not Internal"
        ));
        assert!(!codec_failed(&Ok(b"request body inflate failed".to_vec())));
    }
}

#[cfg(test)]
mod push_tests {
    use super::*;
    use crate::grpc::proto::RunVersionsEvent;
    use futures::task::{waker_ref, ArcWake};
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    struct WakeCounter(AtomicUsize);

    impl ArcWake for WakeCounter {
        fn wake_by_ref(counter: &Arc<Self>) {
            counter.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn versions(entries: &[(&str, u64)]) -> HashMap<String, u64> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), *value))
            .collect()
    }

    fn event(
        runs: &[(&str, u64)],
        projects: &[(&str, u64)],
        global_version: u64,
        metrics_changed_runs: &[&str],
        resync: bool,
    ) -> RunVersionsEvent {
        RunVersionsEvent {
            run_versions: versions(runs),
            project_versions: versions(projects),
            global_version,
            metrics_changed_runs: metrics_changed_runs
                .iter()
                .map(|run| (*run).to_string())
                .collect(),
            resync,
            ..Default::default()
        }
    }

    #[test]
    fn preconnect_subscriber_waits_and_first_delivery_is_the_full_seed() {
        let hub = PushHub::default();
        let subscriber = hub.subscribe();
        assert!(subscriber.take().is_empty());

        hub.note_connected();
        let seed = subscriber.take();
        assert!(seed.initial);
        assert_eq!(seed.resync_gen, Some(1));
        assert_eq!(seed.global, Some(0));
    }

    #[test]
    fn a_late_subscriber_gets_its_seed_from_next_visible_without_a_wake() {
        let hub = PushHub::default();
        hub.note_connected();
        let mut late = hub.subscribe();
        assert!(matches!(
            late.next_visible().now_or_never(),
            Some(update) if update.initial
        ));
    }

    #[test]
    fn publishing_wakes_a_parked_subscriber() {
        let hub = PushHub::default();
        hub.note_connected();
        let mut subscriber = hub.subscribe();
        subscriber.take();

        let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = waker_ref(&counter);
        let mut context = Context::from_waker(&waker);
        let next = subscriber.next_visible();
        futures::pin_mut!(next);
        assert!(matches!(next.as_mut().poll(&mut context), Poll::Pending));

        hub.apply_event(event(&[("run", 1)], &[], 0, &[], false));
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            next.as_mut().poll(&mut context),
            Poll::Ready(update) if update.runs == versions(&[("run", 1)])
        ));
    }

    #[test]
    fn steady_state_delivery_contains_only_entries_that_rose() {
        let hub = PushHub::default();
        hub.note_connected();
        hub.apply_event(event(
            &[("old-a", 3), ("old-b", 7)],
            &[("project-a", 4), ("project-b", 8)],
            2,
            &["old-a"],
            false,
        ));
        let subscriber = hub.subscribe();
        let seed = subscriber.take();
        assert!(seed.initial);
        assert_eq!(seed.runs.len(), 2);
        assert_eq!(seed.projects.len(), 2);

        hub.apply_event(event(&[("old-a", 4)], &[("project-a", 5)], 0, &[], false));
        let delta = subscriber.take();
        assert!(!delta.initial);
        assert_eq!(delta.runs, versions(&[("old-a", 4)]));
        assert_eq!(delta.projects, versions(&[("project-a", 5)]));
        assert_eq!(delta.global, None);
        assert_eq!(delta.resync_gen, None);
        assert!(delta.metrics_gen.is_empty());
    }

    #[test]
    fn stale_versions_are_ignored_instead_of_waking_consumers() {
        let hub = PushHub::default();
        hub.note_connected();
        hub.apply_event(event(&[("run", 9)], &[("project", 11)], 4, &[], false));
        let subscriber = hub.subscribe();
        subscriber.take();

        hub.apply_event(event(&[("run", 8)], &[("project", 10)], 3, &[], false));
        assert!(subscriber.take().is_empty());
        let state = hub.0.lock().unwrap();
        assert_eq!(state.snapshot.runs["run"], 9);
        assert_eq!(state.snapshot.projects["project"], 11);
        assert_eq!(state.snapshot.global, 4);
    }

    #[test]
    fn zero_version_for_a_new_entity_is_not_confused_with_no_change() {
        let hub = PushHub::default();
        hub.note_connected();
        let subscriber = hub.subscribe();
        subscriber.take();

        hub.apply_event(event(&[("run", 0)], &[("project", 0)], 0, &[], false));
        let delta = subscriber.take();
        assert_eq!(delta.runs, versions(&[("run", 0)]));
        assert_eq!(delta.projects, versions(&[("project", 0)]));
    }

    #[test]
    fn unread_events_coalesce_into_one_monotonic_delivery() {
        let hub = PushHub::default();
        hub.note_connected();
        let subscriber = hub.subscribe();
        subscriber.take();

        hub.apply_event(event(&[("run", 2)], &[], 5, &["run"], false));
        hub.apply_event(event(
            &[("run", 4), ("other", 3)],
            &[("project", 6)],
            0,
            &["run"],
            true,
        ));

        let delta = subscriber.take();
        assert_eq!(delta.runs, versions(&[("run", 4), ("other", 3)]));
        assert_eq!(delta.projects, versions(&[("project", 6)]));
        assert_eq!(delta.global, Some(5));
        assert_eq!(delta.metrics_gen, versions(&[("run", 2)]));
        assert_eq!(delta.resync_gen, Some(2));
        assert!(subscriber.take().is_empty());
    }

    #[test]
    fn last_logged_values_pass_through_coalesce_by_max_and_are_never_seeded() {
        let hub = PushHub::default();
        hub.note_connected();
        let subscriber = hub.subscribe();
        subscriber.take();
        let logged = |entries: &[(&str, i64)]| RunVersionsEvent {
            project_last_logged_at_ms: entries
                .iter()
                .map(|(project, at)| ((*project).to_string(), *at))
                .collect(),
            ..Default::default()
        };

        hub.apply_event(logged(&[("a", 120_000)]));
        hub.apply_event(logged(&[("a", 60_000), ("b", 5)]));
        let delta = subscriber.take();
        assert!(!delta.initial);
        assert_eq!(
            delta.project_last_logged_at_ms,
            HashMap::from([("a".to_string(), 120_000), ("b".to_string(), 5)])
        );
        assert!(hub.subscribe().take().project_last_logged_at_ms.is_empty());
    }

    #[test]
    fn subscribers_are_independent_and_late_mounts_get_current_seed() {
        let hub = PushHub::default();
        hub.note_connected();
        let first = hub.subscribe();
        first.take();

        hub.apply_event(event(&[("run", 3)], &[], 0, &[], false));
        let late = hub.subscribe();
        let late_seed = late.take();
        assert!(late_seed.initial);
        assert_eq!(late_seed.runs, versions(&[("run", 3)]));

        assert_eq!(first.take().runs, versions(&[("run", 3)]));
        assert_eq!(hub.0.lock().unwrap().subscribers.len(), 2);
        drop(first);
        assert_eq!(hub.0.lock().unwrap().subscribers.len(), 2);

        hub.apply_event(event(&[("run", 4)], &[], 0, &[], false));
        assert_eq!(hub.0.lock().unwrap().subscribers.len(), 1);
        assert_eq!(late.take().runs, versions(&[("run", 4)]));
    }
}

#[cfg(test)]
mod not_sent_tests {
    use super::*;

    #[test]
    fn only_an_unsent_expiry_or_a_stale_bundle_refusal_counts_as_not_sent() {
        assert!(not_sent(&Status::unavailable(NOT_SENT)));
        assert!(not_sent(&Status::unavailable(crate::grpc::RELOAD_REQUIRED)));
        assert!(!not_sent(&Status::unavailable("connection lost")));
        assert!(!not_sent(&Status::deadline_exceeded(NOT_SENT)));
    }
}
