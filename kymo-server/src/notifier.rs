//! Background task that drains run-lifecycle events off an mpsc channel and
//! pushes them to the watchdog via WatchdogService::NotifyRunLifecycle.
//!
//! Fire-and-forget by design: user-facing RPCs never block on watchdog
//! reachability. Each hint gets one bounded delivery attempt. A connect/RPC
//! failure or ambiguous timeout is counted and dropped rather than replayed:
//! lifecycle hints have no generation token, so retrying an old Restore after
//! the watchdog already applied it could rewind newer polled state. The
//! watchdog's authoritative ListRuns reconciliation heals current tracking
//! state after missed hints; lifecycle notifications themselves are best-effort.

use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint};

use crate::proto::watchdog_service_client::WatchdogServiceClient;
use crate::proto::RunLifecycleEvent;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RPC_TIMEOUT: Duration = Duration::from_secs(5);
const DROP_REASONS: [&str; 5] = ["full", "closed", "connect", "timeout", "rpc_error"];

pub fn spawn(url: String) -> mpsc::Sender<RunLifecycleEvent> {
    for reason in DROP_REASONS {
        metrics::counter!("mkdb2_watchdog_notify_dropped_total", "reason" => reason).absolute(0);
    }
    let (tx, rx) = mpsc::channel::<RunLifecycleEvent>(1024);
    tokio::spawn(async move {
        run(url, rx).await;
    });
    tx
}

pub fn try_send(tx: &mpsc::Sender<RunLifecycleEvent>, event: RunLifecycleEvent) {
    if let Err(error) = tx.try_send(event) {
        let reason = notify_drop_reason(&error);
        record_drop(reason);
        tracing::warn!(reason, %error, "watchdog notify dropped");
    }
}

fn record_drop(reason: &'static str) {
    metrics::counter!("mkdb2_watchdog_notify_dropped_total", "reason" => reason).increment(1);
}

fn notify_drop_reason(error: &mpsc::error::TrySendError<RunLifecycleEvent>) -> &'static str {
    match error {
        mpsc::error::TrySendError::Full(_) => "full",
        mpsc::error::TrySendError::Closed(_) => "closed",
    }
}

async fn run(url: String, mut rx: mpsc::Receiver<RunLifecycleEvent>) {
    let mut client: Option<WatchdogServiceClient<Channel>> = None;
    while let Some(ev) = rx.recv().await {
        // Lazy-connect once for this event. Do not let one stale hint own the
        // worker until an outage ends; later events get their own attempt.
        if client.is_none() {
            match connect(&url).await {
                Ok(c) => client = Some(c),
                Err(error) => {
                    record_drop("connect");
                    tracing::warn!(%error, "watchdog notify dropped after connect failure");
                    continue;
                }
            }
        }

        let c = client.as_mut().expect("client present");
        match tokio::time::timeout(RPC_TIMEOUT, c.notify_run_lifecycle(ev)).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                record_drop("rpc_error");
                tracing::warn!(reason = "rpc_error", %error, "watchdog notify dropped");
                // A failed channel is not reused, but this event is never replayed.
                client = None;
            }
            Err(_) => {
                record_drop("timeout");
                tracing::warn!(reason = "timeout", ?RPC_TIMEOUT, "watchdog notify dropped");
                client = None;
            }
        }
    }
}

async fn connect(url: &str) -> Result<WatchdogServiceClient<Channel>> {
    let channel = Endpoint::from_shared(url.to_string())?
        .connect_timeout(CONNECT_TIMEOUT)
        .connect()
        .await?;
    Ok(WatchdogServiceClient::new(channel))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinguishes_full_and_closed_notify_drops() {
        let (full_tx, _full_rx) = mpsc::channel(1);
        full_tx.try_send(RunLifecycleEvent::default()).unwrap();
        let full = full_tx.try_send(RunLifecycleEvent::default()).unwrap_err();
        assert_eq!(notify_drop_reason(&full), "full");

        let (closed_tx, closed_rx) = mpsc::channel(1);
        drop(closed_rx);
        let closed = closed_tx
            .try_send(RunLifecycleEvent::default())
            .unwrap_err();
        assert_eq!(notify_drop_reason(&closed), "closed");
    }
}
