//! App-root notices the user must see — a server connection that stays down, chart-delta protocol failures (`protocol_alert`), and `audience=user` server alerts polled from `GET /alerts` — rendered above the router so they cover every page; dismissal lasts this tab's session and is keyed by identity, so a new occurrence reappears.

use std::time::Duration;

use dioxus::prelude::*;
use futures::FutureExt;
use gloo_timers::future::sleep;
use serde::Deserialize;

use crate::util::primary;

/// Rules evaluate every 15s and the server caches 10s; polling faster buys nothing.
const POLL: Duration = Duration::from_secs(30);
/// Dismissed keys kept per tab; each key names one occurrence (a firing-since or connection generation), so a stale key never hides a new one.
const DISMISSED_CAP: usize = 32;
/// Outages shorter than this (the first handshake, routine reconnects) never show.
const OFFLINE_GRACE: Duration = Duration::from_secs(3);

#[derive(Clone)]
struct Notice {
    key: String,
    text: String,
    title: String,
}

/// Bounded to a count plus the latest detail so a repeating failure can't grow memory. Module-level, not DashboardState: the caches whose integrity it reports are module-level too (metric_rect CHART_CACHE), and the alarm must survive navigation and remounts.
#[derive(Default)]
struct Protocol {
    count: u64,
    last: String,
}

static PROTOCOL: GlobalSignal<Protocol> = Signal::global(Default::default);
/// The last server-confirmed set.
static SERVER: GlobalSignal<Vec<Notice>> = Signal::global(Vec::new);
static DISMISSED: GlobalSignal<Vec<String>> = Signal::global(Vec::new);
/// The shown outage's connection generation.
static OFFLINE: GlobalSignal<Option<u64>> = Signal::global(|| None);

fn offline_notice(generation: u64) -> Notice {
    Notice {
        key: format!("offline|{generation}"),
        text: if generation == 0 {
            "Connecting to the server…"
        } else {
            "Reconnecting to the server…"
        }
        .to_owned(),
        title: "Charts stop updating until the connection is back; waiting requests resume then."
            .to_owned(),
    }
}

/// Record a chart-delta protocol failure: the browser console gets the detail, the bar gets the count.
pub fn protocol_alert(msg: String) {
    web_sys::console::error_1(&wasm_bindgen::JsValue::from_str(&format!(
        "kymo chart-delta protocol failure: {msg}"
    )));
    let mut protocol = PROTOCOL.write();
    protocol.count += 1;
    protocol.last = msg;
}

/// The server's allowlisted DTO (kymo-server alerts.rs) — the summaries are written as user-facing lines. Fields the bar does not show are simply not declared.
#[derive(Deserialize)]
struct ServerAlert {
    name: String,
    summary: String,
    class: Option<String>,
    active_at: String,
}

/// `None` = this poll learned nothing (transport error, non-2xx — the server answers 503 while its Prometheus fetch fails — or an unparseable body): keep what we have. Silence by design — users can't act on a monitoring hiccup; the next poll retries.
async fn fetch_server_alerts() -> Option<Vec<Notice>> {
    let url = crate::runtime::alerts_url();
    // A poll that outlives its own interval is aborted: a hung connection or a body that never ends must not stall the loop.
    let deadline = web_sys::AbortSignal::timeout_with_u32(POLL.as_millis() as u32);
    let resp = gloo_net::http::Request::get(&url)
        .abort_signal(Some(&deadline))
        .send()
        .await
        .ok()?;
    if !resp.ok() {
        return None;
    }
    let alerts: Vec<ServerAlert> = serde_json::from_str(&resp.text().await.ok()?).ok()?;
    Some(
        alerts
            .into_iter()
            .map(|a| Notice {
                key: format!(
                    "{}|{}|{}",
                    a.name,
                    a.class.as_deref().unwrap_or(""),
                    a.active_at
                ),
                text: if a.summary.is_empty() {
                    a.name.clone()
                } else {
                    a.summary
                },
                title: format!("{} (firing since {})", a.name, a.active_at),
            })
            .collect(),
    )
}

fn dismiss(key: &str) {
    if key == "protocol" {
        *PROTOCOL.write() = Default::default();
        return;
    }
    let mut dismissed = DISMISSED.write();
    if dismissed.iter().any(|k| k == key) {
        return;
    }
    dismissed.push(key.to_owned());
    if dismissed.len() > DISMISSED_CAP {
        dismissed.remove(0);
    }
}

/// Watches the connection, polls server alerts while the page is visible, and renders every undismissed notice; the live region is always in the DOM (empty = 0px) so a first notice is announced.
#[component]
pub fn NoticeBar() -> Element {
    use_future(|| async {
        loop {
            let (generation, connected) = crate::grpc::connection();
            if connected {
                if OFFLINE.peek().is_some() {
                    *OFFLINE.write() = None;
                }
            } else {
                futures::select! {
                    _ = sleep(OFFLINE_GRACE).fuse() => *OFFLINE.write() = Some(generation),
                    _ = crate::grpc::connection_changed(generation).fuse() => continue,
                }
            }
            crate::grpc::connection_changed(generation).await;
        }
    });
    use_future(move || async move {
        // The local runtime has no Prometheus and no /alerts route; polling it would only log CORS noise.
        if cfg!(feature = "local-runtime") {
            return;
        }
        loop {
            crate::grpc::wait_until_page_visible().await;
            if let Some(alerts) = fetch_server_alerts().await {
                *SERVER.write() = alerts;
            }
            sleep(POLL).await;
        }
    });

    let protocol = PROTOCOL.read();
    let server = SERVER.read();
    let dismissed = DISMISSED.read();
    let offline = *OFFLINE.read();
    let mut notices: Vec<Notice> = offline.map(offline_notice).into_iter().collect();
    if protocol.count > 0 {
        notices.push(Notice {
            key: "protocol".to_owned(),
            text: format!("⚠ {} chart-delta failure(s) — see console", protocol.count),
            title: protocol.last.clone(),
        });
    }
    notices.extend(server.iter().cloned());
    notices.retain(|n| !dismissed.contains(&n.key));
    rsx! {
        div { class: "notice-bar", role: "status", aria_live: "polite",
            for notice in notices {
                div { class: "notice", key: "{notice.key}", title: "{notice.title}",
                    span { class: "notice-text", "{notice.text}" }
                    button {
                        class: "notice-dismiss",
                        r#type: "button",
                        aria_label: "Dismiss: {notice.text}",
                        onmousedown: primary({
                            let key = notice.key.clone();
                            move |_| dismiss(&key)
                        }),
                        "Dismiss"
                    }
                }
            }
        }
    }
}
