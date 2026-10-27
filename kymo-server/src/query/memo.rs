//! Stage 1 of docs/chart-request-latency.md: memoized `unchanged` replies.
//!
//! The full build is a function of the canonical request, its echoed cache state included, and of the rows behind each ref. Within one process a snapshot's lineage and maximum insertion stamp name its rows: a lineage only ever gains rows stamped strictly above its maximum, and every other change starts a new lineage (series_cache.rs; eviction recovery carries the 64-bit content-sum caveat of docs/incremental-updates.md). A poll with the same key whose every ref still has the recorded lineage and maximum would therefore rebuild the memoized reply bit for bit, so it is answered without `prepare`, `respond` or `plan_delta_from_col`. Only replies the full build classified `unchanged` are memoized: every case it answers otherwise keeps building.
//!
//! Inputs outside the request and the rows: the process-keyed lineage stamp and the lineage counter, both constant for the life of the memo. Custom-x charts (which also read x maps) never continue, so they are never memoized.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use prost::Message;
use sha2::{Digest, Sha256};

use super::proto;
use crate::series_cache::{SeriesSnapshot, ARC_HEADER};

/// Bound on retained memo bytes, as [`Memo::bytes`] accounts them. A 12-ref chart's memo is about 1.6 KB, so this holds about 5,000 of them; prod's peaked at 559 entries in 0.4 MiB (2026-10-17 to 10-20).
const MEMO_BUDGET_BYTES: usize = 8 << 20;

/// SHA-256 of the canonical request bytes; see [`memo_key`].
pub(super) type MemoKey = [u8; 32];

/// The canonical request without its echoed cache state, hashed (every request field in prost's deterministic encoding): finalized, held state's key (query/held.rs); continued by [`memo_key`], the memo's. A poll hashes it once.
pub(super) fn request_hash(req: &proto::ChartRequest) -> Sha256 {
    // `..*req` copies every field not named here, so a new Copy field is keyed automatically (keying more can only cause misses), while a new non-Copy field cannot move out of `*req` and fails to compile until it is keyed here (a map, for one, needs a canonical order, as the frontiers in [`memo_key`] do).
    let body = proto::ChartRequest {
        y_series: req.y_series.clone(),
        x_series: req.x_series.clone(),
        cache_state: None,
        ..*req
    };
    let mut hash = Sha256::new();
    part(&mut hash, &body.encode_to_vec());
    hash
}

fn part(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
}

/// [`request_hash`] continued with the echoed `held_series` and the frontier map in sorted key order (its wire order is a hash map's).
pub(super) fn memo_key(request: &Sha256, req: &proto::ChartRequest) -> MemoKey {
    let mut hash = request.clone();
    // Exhaustive destructuring: a field added to the cache state fails to compile here until it is keyed.
    match &req.cache_state {
        None => part(&mut hash, &[0]),
        Some(proto::ChartCacheState {
            frontiers,
            held_series,
        }) => {
            part(&mut hash, &[1]);
            match held_series {
                None => part(&mut hash, &[0]),
                Some(held) => part(&mut hash, &[&[1u8][..], &held.to_le_bytes()].concat()),
            }
            let mut entries: Vec<_> = frontiers.iter().collect();
            entries.sort_unstable();
            part(&mut hash, &(entries.len() as u64).to_le_bytes());
            for (key, value) in entries {
                part(&mut hash, key.as_bytes());
                part(&mut hash, &value.to_le_bytes());
            }
        }
    }
    hash.finalize().into()
}

/// The per-ref cutoffs a memoized reply moved its client past (see `BuiltChart::moved_from`).
pub(super) type MovedFrom = Arc<[Option<i64>]>;

/// A snapshot's identity: its lineage and maximum insertion stamp.
fn identity(rows: &Arc<SeriesSnapshot>) -> (u64, Option<i64>) {
    (rows.lineage(), rows.maximum())
}

struct Memo {
    inputs: Box<[(u64, Option<i64>)]>,
    /// The cutoffs the poll echoed, when the reply's maxima moved past them (stage 3b; see `BuiltChart::moved_from`).
    moved_from: Option<MovedFrom>,
    /// The prost encoding of the reply; about a fifth of the decoded message's heap. Shared, so a hit clones it under the store's lock and decodes it after.
    reply: Arc<[u8]>,
    tick: u64,
    bytes: usize,
}

/// The key twice (map and LRU order), the tick, the entry header and hash-table slack, and the reply's reference counts.
const MEMO_ENTRY_OVERHEAD: usize = 2 * std::mem::size_of::<MemoKey>() + 8 + 32 + 16;

impl Memo {
    fn bytes(inputs: usize, reply: usize, moved_from: Option<&[Option<i64>]>) -> usize {
        MEMO_ENTRY_OVERHEAD
            + std::mem::size_of::<Memo>()
            + inputs * std::mem::size_of::<(u64, Option<i64>)>()
            + reply
            + moved_from.map_or(0, |cutoffs| ARC_HEADER + std::mem::size_of_val(cutoffs))
    }
}

#[derive(Default)]
struct MemoStore {
    map: HashMap<MemoKey, Memo>,
    /// Least recently used first.
    order: BTreeMap<u64, MemoKey>,
    next_tick: u64,
    bytes: usize,
}

impl MemoStore {
    fn remove(&mut self, key: &MemoKey) {
        if let Some(memo) = self.map.remove(key) {
            self.order.remove(&memo.tick);
            self.bytes -= memo.bytes;
        }
    }

    fn publish(&self) {
        metrics::gauge!("mkdb2_chart_memo_entries").set(self.map.len() as f64);
        metrics::gauge!("mkdb2_chart_memo_bytes").set(self.bytes as f64);
    }
}

pub(super) struct ChartMemo {
    /// [`Self::disable`] empties the store under its lock and [`Self::record`] checks this again under it, so a disabled memo stores nothing.
    enabled: AtomicBool,
    store: Mutex<MemoStore>,
    budget: usize,
}

/// Counts one stage-1 outcome: `hit` (served from a memo, audited or not), `miss` (a candidate poll that built), `audit_failed` (a memo the audit's full build disagreed with).
pub(super) fn count(outcome: &'static str) {
    metrics::counter!("mkdb2_chart_memo_total", "outcome" => outcome).increment(1);
}

impl ChartMemo {
    pub(super) fn new(enabled: bool) -> Self {
        Self::with_budget(enabled, MEMO_BUDGET_BYTES)
    }

    pub(super) fn with_budget(enabled: bool, budget: usize) -> Self {
        metrics::gauge!("mkdb2_chart_memo_enabled").set(if enabled { 1.0 } else { 0.0 });
        Self {
            enabled: AtomicBool::new(enabled),
            store: Mutex::default(),
            budget,
        }
    }

    /// Whether the poll could be memo-served: stage 1 is on, the chart has a standard axis, and the request echoes a frontier (an `unchanged` reply needs one).
    pub(super) fn serves(&self, req: &proto::ChartRequest) -> bool {
        self.enabled.load(Ordering::Relaxed)
            && req.x_series.is_none()
            && super::echoed_frontiers(req).is_some()
    }

    /// The poll's key when it could be memo-served.
    #[cfg(test)]
    pub(super) fn key_for(&self, req: &proto::ChartRequest) -> Option<MemoKey> {
        self.serves(req).then(|| memo_key(&request_hash(req), req))
    }

    /// Whether a memo is stored for `key`, whatever its inputs; recency is untouched.
    pub(super) fn holds(&self, key: &MemoKey) -> bool {
        self.lock().map.contains_key(key)
    }

    /// The memoized reply for `key`, with the cutoffs it moves its client from (see `BuiltChart::moved_from`), when every ref's snapshot still has the recorded lineage and maximum. A memo whose inputs differ from the poll's is dropped: usually its series have moved on, and a series' lineage and maximum never return to an earlier value; a poll pinned to older snapshots than a concurrent poll's memo drops that newer memo instead, which costs a later poll one full build.
    pub(super) fn lookup(
        &self,
        key: &MemoKey,
        snapshots: &[Arc<SeriesSnapshot>],
    ) -> Option<(proto::ChartResponse, Option<MovedFrom>)> {
        let (reply, moved_from) = {
            let mut guard = self.lock();
            let store = &mut *guard;
            let memo = store.map.get_mut(key)?;
            if !memo
                .inputs
                .iter()
                .copied()
                .eq(snapshots.iter().map(identity))
            {
                store.remove(key);
                store.publish();
                return None;
            }
            let tick = store.next_tick;
            store.next_tick += 1;
            let old = std::mem::replace(&mut memo.tick, tick);
            store.order.remove(&old);
            store.order.insert(tick, *key);
            (Arc::clone(&memo.reply), memo.moved_from.clone())
        };
        // Decoded outside the lock, which every continuation poll takes. Encoded here from a reply, it always decodes; were it not to, the poll would build in full.
        let reply = proto::ChartResponse::decode(&reply[..]).ok()?;
        Some((reply, moved_from))
    }

    /// Memoize a reply the full build classified `unchanged` for this key and these snapshots, and the cutoffs it moves its client from, evicting least recently used memos past the budget.
    pub(super) fn record(
        &self,
        key: MemoKey,
        snapshots: &[Arc<SeriesSnapshot>],
        reply: &proto::ChartResponse,
        moved_from: Option<MovedFrom>,
    ) {
        let reply: Arc<[u8]> = reply.encode_to_vec().into();
        let bytes = Memo::bytes(snapshots.len(), reply.len(), moved_from.as_deref());
        if bytes > self.budget {
            return;
        }
        let inputs = snapshots.iter().map(identity).collect();
        let mut store = self.lock();
        // Checked under the lock: a concurrent audit failure must not see a memo reappear.
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        store.remove(&key);
        let tick = store.next_tick;
        store.next_tick += 1;
        store.order.insert(tick, key);
        store.bytes += bytes;
        store.map.insert(
            key,
            Memo {
                inputs,
                moved_from,
                reply,
                tick,
                bytes,
            },
        );
        while store.bytes > self.budget {
            let Some(&oldest) = store.order.values().next() else {
                break;
            };
            store.remove(&oldest);
        }
        store.publish();
    }

    /// The store. A panic while it was locked leaves it unknown: recover the lock, drop every memo and serve none until restart, as an audit failure does.
    fn lock(&self) -> MutexGuard<'_, MemoStore> {
        self.store.lock().unwrap_or_else(|poisoned| {
            let mut store = poisoned.into_inner();
            self.store.clear_poison();
            tracing::error!("chart memo poisoned by a panic; turning it off until restart");
            self.off(&mut store);
            store
        })
    }

    fn off(&self, store: &mut MemoStore) {
        self.enabled.store(false, Ordering::Relaxed);
        *store = MemoStore::default();
        store.publish();
        metrics::gauge!("mkdb2_chart_memo_enabled").set(0.0);
    }

    /// A memo-served answer disagreed with the full build: drop every memo and serve none until restart.
    pub(super) fn disable(&self) {
        let mut store = self.lock();
        self.off(&mut store);
    }

    #[cfg(test)]
    pub(super) fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(super) fn len_and_bytes(&self) -> (usize, usize) {
        let store = self.lock();
        (store.map.len(), store.bytes)
    }

    /// Corrupt a stored reply, for the audit-failure tests.
    #[cfg(test)]
    pub(super) fn tamper(&self, key: &MemoKey, edit: impl FnOnce(&mut proto::ChartResponse)) {
        let mut store = self.lock();
        let memo = store.map.get_mut(key).expect("memo to tamper with");
        let mut reply = proto::ChartResponse::decode(&memo.reply[..]).unwrap();
        edit(&mut reply);
        memo.reply = reply.encode_to_vec().into();
        let bytes = Memo::bytes(
            memo.inputs.len(),
            memo.reply.len(),
            memo.moved_from.as_deref(),
        );
        let old = std::mem::replace(&mut memo.bytes, bytes);
        store.bytes = store.bytes - old + bytes;
    }

    /// Poison the store's lock as a panic under it would.
    #[cfg(test)]
    pub(super) fn poison_for_test(&self) {
        std::thread::scope(|scope| {
            let _ = scope
                .spawn(|| {
                    let _store = self.store.lock().unwrap();
                    panic!("injected panic under the memo's lock");
                })
                .join();
        });
    }
}
