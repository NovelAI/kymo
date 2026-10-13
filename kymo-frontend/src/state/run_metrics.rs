//! Each run's metric registry as ListMetrics lists it (names and display types), shared by every panel: type detection reads the bound runs' registries (components/metric_rect.rs). A list serves an ask whose run registry counter (`metrics_gen`) and resync epoch are no newer than the ones it was asked under: every registry change reaches the tab as a metrics push, or after a gap as a resync. Panels that ask for a run while its request is in flight share it.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use futures::future::{FutureExt, LocalBoxFuture, Shared};

use crate::grpc::GrpcClient;
use crate::state::panel_cache::Store;
use crate::state::visibility;
use crate::state::DisplayType;

/// One run's registry: each metric's display type, by name.
pub type RunMetrics = Rc<HashMap<String, DisplayType>>;

/// A list, or None for a run that can no longer be read.
pub type Listing = Option<RunMetrics>;

/// What a list was asked for under: the resync epoch and the run's registry counter, both of which only grow. A list asked for under a later stamp answers an earlier one too: registries only grow, so a metric missing from it was missing before.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stamp {
    pub epoch: u64,
    /// None before the run's first metrics push.
    pub metrics_gen: Option<u64>,
}

impl Stamp {
    /// Whether a list asked for under this stamp answers an ask under `ask`: neither counter is newer in `ask`. Each on its own, not the epoch first: a dashboard mounted during an outage starts at epoch 0 until the reconnect's resync, while its registry counters arrive at once with the push seed (state/push.rs), so an earlier epoch can come with a later counter, which a list from before that registry change must not answer.
    fn answers(self, ask: Stamp) -> bool {
        self.epoch >= ask.epoch && self.metrics_gen >= ask.metrics_gen
    }
}

#[derive(Clone)]
enum Entry {
    Asked(Shared<LocalBoxFuture<'static, Result<Listing, tonic::Status>>>),
    Listed(Listing),
}

/// Lists cost tens of bytes per metric; this bounds a session that lists many runs of many metrics each.
const MAX_BYTES: usize = 64 * 1024 * 1024;

thread_local! {
    static LISTS: RefCell<Store<(Stamp, Entry)>> =
        RefCell::new(Store::with_weight_limit(4096, MAX_BYTES));
}

/// The run's registry under `stamp`: held, from the request in flight, or from a new one. `admit` runs before each attempt that no held list answers, and what it returns is held across that attempt only, so a failed attempt's retry wait holds nothing ([`visibility::retry_visible`]: transient failures retry while the page is visible, for as long as the caller waits). A terminal run lists None.
pub async fn list<T>(
    grpc: &GrpcClient,
    project_id: &str,
    run_id: &str,
    stamp: Stamp,
    admit: impl AsyncFn() -> T,
) -> Listing {
    let key = format!("{project_id}\0{run_id}");
    visibility::retry_visible("run metrics", async || {
        if let Some(Entry::Listed(listing)) = held(&key, stamp) {
            return Ok(listing);
        }
        let _admitted = admit().await;
        match entry(grpc, project_id, run_id, stamp) {
            Entry::Listed(listing) => Ok(listing),
            Entry::Asked(asked) => asked.await,
        }
    })
    .await
}

fn entry(grpc: &GrpcClient, project_id: &str, run_id: &str, stamp: Stamp) -> Entry {
    let key = format!("{project_id}\0{run_id}");
    if let Some(entry) = held(&key, stamp) {
        return entry;
    }
    let asked = {
        let (grpc, project_id, run_id, key) = (
            grpc.clone(),
            project_id.to_string(),
            run_id.to_string(),
            key.clone(),
        );
        async move {
            // One request: the askers retry on their own terms, so none runs on for a stamp nobody waits on.
            let listed = match grpc.list_metrics(&project_id, &run_id).await {
                Ok(metrics) => Ok(Some(Rc::new(
                    metrics
                        .iter()
                        .map(|metric| (metric.metric_name.clone(), DisplayType::for_metric(metric)))
                        .collect::<HashMap<_, _>>(),
                ))),
                Err(status) if visibility::is_terminal_run_status(&status) => Ok(None),
                Err(status) => Err(status),
            };
            // Held, weighed, unless a newer stamp's request replaced this one or the store let it go. A failure is dropped, so the next ask sends again.
            LISTS.with(|lists| {
                let mut lists = lists.borrow_mut();
                if !lists.get(&key).is_some_and(|(held, _)| held == stamp) {
                    return;
                }
                match &listed {
                    Ok(listing) => {
                        let weight = listing_bytes(listing);
                        lists.put_weighted(key, (stamp, Entry::Listed(listing.clone())), weight);
                    }
                    Err(_) => {
                        lists.remove(&key);
                    }
                }
            });
            listed
        }
    }
    .boxed_local()
    .shared();
    LISTS.with(|lists| {
        lists
            .borrow_mut()
            .put(key, (stamp, Entry::Asked(asked.clone())))
    });
    // Driven to the end even when every panel waiting on it unmounts, so the answer is held for the next.
    wasm_bindgen_futures::spawn_local(asked.clone().map(drop));
    Entry::Asked(asked)
}

/// The run's entry that answers `stamp`: its list, or the request in flight for it.
fn held(key: &str, stamp: Stamp) -> Option<Entry> {
    LISTS
        .with(|lists| lists.borrow_mut().get(key))
        .and_then(|(held, entry)| held.answers(stamp).then_some(entry))
}

fn listing_bytes(listing: &Listing) -> usize {
    let Some(metrics) = listing else { return 0 };
    metrics
        .keys()
        .map(|name| name.capacity() + std::mem::size_of::<(String, DisplayType)>() + 8)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::Stamp;

    fn stamp(epoch: u64, metrics_gen: Option<u64>) -> Stamp {
        Stamp { epoch, metrics_gen }
    }

    #[test]
    fn a_list_answers_asks_with_neither_counter_newer() {
        let held = stamp(2, Some(5));
        // Its own stamp, and every earlier one: an earlier epoch, or the same epoch before a registry change or the run's first metrics push.
        for ask in [
            stamp(2, Some(5)),
            stamp(2, Some(4)),
            stamp(2, None),
            stamp(1, Some(5)),
            stamp(1, None),
        ] {
            assert!(held.answers(ask), "{ask:?}");
        }
        // A registry change or a resync since it was asked for.
        for ask in [stamp(2, Some(6)), stamp(3, Some(5)), stamp(3, None)] {
            assert!(!held.answers(ask), "{ask:?}");
        }
        assert!(!stamp(2, None).answers(stamp(2, Some(0))));
        // A dashboard remounted during an outage: epoch 0 until the reconnect, with the registry counter that rose after this list.
        assert!(!held.answers(stamp(0, Some(6))));
    }
}
