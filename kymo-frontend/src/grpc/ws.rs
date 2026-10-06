//! Browser-side WebSocket gRPC transport.
//!
//! All of the dashboard's unary RPCs are multiplexed over ONE WebSocket to
//! the server's /grpc-ws route, sidestepping the browser's ~6-connection
//! cap on plain-HTTP origins that serialized parallel chart fetches (no
//! HTTP/2 without TLS). This changes only the web frontend → backend
//! connection — kymo and every other client still speak native gRPC.
//!
//! One background task owns the socket: callers enqueue (path, body) plus
//! a oneshot for the response, the task assigns a correlation id, sends
//! the frame, and parks the oneshot until the matching response arrives.
//! On disconnect every parked oneshot is dropped. Read-only unary calls
//! re-enqueue and ride the reconnect — one socket blip must not blank every
//! in-flight chart, because nothing refetches a finished run's chart until
//! the user touches it. Mutations use `unary_route_no_replay`: a lost
//! response has an unknown outcome and must be reconciled, because replaying
//! after an intervening inverse mutation would no longer be idempotent. Their
//! response wait is finite so a live-but-silent socket cannot park the UI
//! forever; a deadline is also outcome-unknown and is followed by reconciliation,
//! unless the request was still queued, which is reported as not sent.
//! Requests issued while
//! disconnected wait in the queue and flush once the socket is OPEN —
//! minus those whose caller stopped waiting (deadline, unmount), which
//! are dropped at dequeue.
//!
//! Framing (binary, little-endian), mirroring the server's ws_proxy.rs:
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
//! The URL carries `?rev=FRONTEND_WIRE_REVISION`; a server whose floor is above it refuses every request with RELOAD_REQUIRED, and [`connection_task`] then stops connecting for good while the notice bar asks the user to reload.

use std::collections::{hash_map::Entry, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;

use futures::channel::{mpsc, oneshot};
use futures::{select, FutureExt, SinkExt, StreamExt};
use gloo_net::websocket::{futures::WebSocket, Message, State};
use prost::Message as _;
use tokio::sync::watch;
use tonic::Status;

/// A response that never arrives (server-side task panic, dropped frame)
/// would otherwise park the caller until the next disconnect. Generous —
/// the slowest real queries are a few seconds.
const REQUEST_TIMEOUT_MS: u64 = 60_000;
/// Maximum elapsed UI wait from enqueue through response. When sent promptly,
/// this covers the longest lifecycle RPC's roughly 120-second server budget;
/// expiry never replays a queued or in-flight mutation.
const MUTATION_RESPONSE_TIMEOUT_MS: u64 = 150_000;

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
        }
    }

    fn is_empty(&self) -> bool {
        !self.initial
            && self.resync_gen.is_none()
            && self.runs.is_empty()
            && self.projects.is_empty()
            && self.global.is_none()
            && self.metrics_gen.is_empty()
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

/// Raise one map entry monotonically, treating an absent key at version zero as a change.
pub fn raise(map: &mut HashMap<String, u64>, key: String, value: u64) -> bool {
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
pub fn merge_versions(
    into: &mut HashMap<String, u64>,
    from: impl IntoIterator<Item = (String, u64)>,
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

fn encode_request(id: u32, path: &str, body: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(6 + path.len() + body.len());
    frame.extend_from_slice(&id.to_le_bytes());
    frame.extend_from_slice(&(path.len() as u16).to_le_bytes());
    frame.extend_from_slice(path.as_bytes());
    frame.extend_from_slice(body);
    frame
}

fn decode_response(mut buf: Vec<u8>) -> Option<(u32, Result<Vec<u8>, Status>)> {
    let id = u32::from_le_bytes(buf.get(0..4)?.try_into().ok()?);
    let code = *buf.get(4)?;
    let result = if code == 0 {
        // Shift the 5-byte header off in place: chart payloads run to
        // megabytes, and a second allocation+copy per frame is real work.
        buf.drain(..5);
        Ok(buf)
    } else {
        Err(Status::new(
            tonic::Code::from(code as i32),
            String::from_utf8_lossy(&buf[5..]).into_owned(),
        ))
    };
    Some((id, result))
}

async fn connection_task(mut requests: mpsc::UnboundedReceiver<Pending>) {
    let mut next_id: u32 = 0;
    let url = format!(
        "{}?rev={}",
        crate::runtime::config().websocket_url,
        super::ws_rpc::FRONTEND_WIRE_REVISION
    );
    let mut reconnect = super::reconnect::Reconnect::new();
    loop {
        reconnect.turn().await;
        let Ok(mut ws) = WebSocket::open(&url) else {
            reconnect.failed();
            continue;
        };
        // Settle the handshake before dequeuing anything: the schedule needs this attempt's outcome, and a send on a socket that never opened is silently dropped, so a mutation would be reported outcome-unknown though never sent.
        // The socket's open or error event wakes this rather than a timer, which a hidden tab can have throttled to one a minute while other tabs wait on this attempt.
        // The timeout catches a socket that errs without leaving CONNECTING (WebKit, on a port it blocks), which no event would wake again; it outlasts Firefox's longest delay plus its 20 s handshake timeout, and stays within an attempt's claim.
        select! {
            _ = futures::future::poll_fn(|cx| ws.poll_ready_unpin(cx)).fuse() => {}
            _ = gloo_timers::future::sleep(Duration::from_secs(90)).fuse() => {}
        }
        if !matches!(ws.state(), State::Open) {
            reconnect.failed();
            continue;
        }
        reconnect.opened();
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
        loop {
            select! {
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
                    let frame = encode_request(next_id, p.path, &p.body);
                    p.sent.store(true, Ordering::Relaxed);
                    if sink.send(Message::Bytes(frame)).await.is_err() {
                        // Socket died mid-send; this request's oneshot drops here, never having reached the inflight map below.
                        break;
                    }
                    inflight.insert(next_id, p.resp);
                }
                _ = visibility.wait_for(|&hidden| hidden != sent_hidden).map(drop).fuse() => {
                    sent_hidden = *HIDDEN.borrow();
                    next_id = next_id.wrapping_add(1).max(1);
                    let body =
                        super::proto::PushControlRequest { quiet: sent_hidden }.encode_to_vec();
                    let frame = encode_request(next_id, super::ws_rpc::PUSH_CONTROL, &body);
                    if sink.send(Message::Bytes(frame)).await.is_err() {
                        break;
                    }
                }
                msg = stream.next() => {
                    match msg {
                        Some(Ok(Message::Bytes(buf))) => {
                            if let Some((id, result)) = decode_response(buf) {
                                // The refusal ends the transport instead of answering its request, so callers only ever see Unavailable, which every view retries while keeping what it shows, whatever code the server sent. Any request can carry it, push control included (a hidden tab's first frame).
                                if matches!(&result, Err(s) if s.message() == super::ws_rpc::RELOAD_REQUIRED) {
                                    STALE.store(true, Ordering::Relaxed);
                                    break;
                                }
                                if id == 0 {
                                    // Server push (see ws_proxy.rs): id 0
                                    // carries a RunVersionsEvent, never a
                                    // response to a request.
                                    if let Ok(payload) = result {
                                        match super::proto::RunVersionsEvent::decode(
                                            payload.as_slice(),
                                        ) {
                                            Ok(ev) => PUSH.apply_event(ev),
                                            Err(e) => crate::util::warn(&format!(
                                                "bad push frame: {e}"
                                            )),
                                        }
                                    }
                                } else if let Some(tx) = inflight.remove(&id) {
                                    let _ = tx.send(result);
                                }
                            }
                        }
                        Some(Ok(Message::Text(_))) => {}
                        Some(Err(_)) | None => break,
                    }
                }
            }
        }
        CONNECTED.send_modify(|(gen, connected)| (*gen, *connected) = (*gen + 1, false));
        if is_stale() {
            // Stop for good, never reload by ourselves: dropping `requests` fails every waiting and later call at once, and the notice bar asks the user to reload.
            return;
        }
        reconnect.dropped();
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
