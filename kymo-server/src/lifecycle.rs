//! In-process fencing between run lifecycle mutations and run data access.
//!
//! A run deletion takes an exclusive guard before it changes Postgres. Ingest
//! and data reads take shared guards and keep them until their ClickHouse work
//! finishes. Once the reaper owns the exclusive guard, all work admitted before
//! its claim has drained. After the durable `purging_at` claim, it releases the
//! guard: queued requests may proceed to their authoritative Postgres lifecycle
//! check, which rejects them before they reach ClickHouse.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// The stable identity of a run. Ordering is used to acquire several gates in
/// one deterministic order, avoiding lock-order inversions between requests.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RunKey {
    pub project_id: String,
    pub run_id: String,
}

impl RunKey {
    pub fn new(project_id: impl Into<String>, run_id: impl Into<String>) -> Self {
        Self {
            project_id: project_id.into(),
            run_id: run_id.into(),
        }
    }
}

#[derive(Clone, Default)]
pub struct LifecycleGates {
    // Weak entries mean the registry does not grow forever as one-off run IDs
    // pass through the service. A live guard keeps its lock strongly owned.
    locks: Arc<Mutex<HashMap<RunKey, Weak<RwLock<()>>>>>,
    // Submission fence for ClickHouse's global async-insert queue. Normal
    // flush tasks hold it shared while they submit; the reaper briefly takes
    // it exclusively while it flushes that queue before deleting rows.
    submission: Arc<RwLock<()>>,
}

/// Separates lifecycle mutations from authoritative Trash snapshots. Mutation
/// RPCs hold a shared guard through their final database outcome; ListTrash
/// takes the exclusive side so outcome-unknown clients cannot observe a
/// mutation that is still running and call that partial view reconciled.
#[derive(Clone, Default)]
pub struct LifecycleMutationBarrier(Arc<RwLock<()>>);

impl LifecycleMutationBarrier {
    pub async fn mutation(&self) -> OwnedRwLockReadGuard<()> {
        self.0.clone().read_owned().await
    }

    pub async fn snapshot(&self) -> OwnedRwLockWriteGuard<()> {
        self.0.clone().write_owned().await
    }
}

impl LifecycleGates {
    pub fn new() -> Self {
        Self::default()
    }

    async fn locks_for(
        &self,
        keys: impl IntoIterator<Item = RunKey>,
    ) -> Vec<(RunKey, Arc<RwLock<()>>)> {
        let mut keys: Vec<_> = keys.into_iter().collect();
        keys.sort_unstable();
        keys.dedup();

        let mut registry = self.locks.lock().await;
        let mut locks = Vec::with_capacity(keys.len());
        for key in keys {
            let lock = registry
                .get(&key)
                .and_then(Weak::upgrade)
                .unwrap_or_else(|| {
                    let lock = Arc::new(RwLock::new(()));
                    registry.insert(key.clone(), Arc::downgrade(&lock));
                    lock
                });
            locks.push((key, lock));
        }
        // Reclaim dead entries opportunistically while this already-bounded
        // critical section is open.
        if registry.len() > 1_024 {
            registry.retain(|_, lock| lock.strong_count() > 0);
        }
        locks
    }

    /// Acquire shared guards for a set of runs. The returned guards must stay
    /// alive through the data-store operation they protect.
    pub async fn read_many(
        &self,
        keys: impl IntoIterator<Item = RunKey>,
    ) -> Vec<OwnedRwLockReadGuard<()>> {
        let locks = self.locks_for(keys).await;
        let mut guards = Vec::with_capacity(locks.len());
        for (_, lock) in locks {
            guards.push(lock.read_owned().await);
        }
        guards
    }

    /// One shared guard per distinct run, keyed — for callers that hand
    /// individual runs' guards to detached work.
    pub(crate) async fn read_many_keyed(
        &self,
        keys: impl IntoIterator<Item = RunKey>,
    ) -> HashMap<RunKey, Arc<OwnedRwLockReadGuard<()>>> {
        let locks = self.locks_for(keys).await;
        let mut guards = HashMap::with_capacity(locks.len());
        for (key, lock) in locks {
            guards.insert(key, Arc::new(lock.read_owned().await));
        }
        guards
    }

    /// Acquire exclusive guards for a set of runs in deterministic order.
    pub async fn write_many(
        &self,
        keys: impl IntoIterator<Item = RunKey>,
    ) -> Vec<OwnedRwLockWriteGuard<()>> {
        let locks = self.locks_for(keys).await;
        let mut guards = Vec::with_capacity(locks.len());
        for (_, lock) in locks {
            guards.push(lock.write_owned().await);
        }
        guards
    }

    /// Queue for one exclusive gate. Reaper callers visit keys in sorted
    /// order, matching every multi-run reader/writer, while Tokio's queued
    /// writer intent prevents a stream of new readers from starving cleanup.
    pub async fn write_one(&self, key: RunKey) -> OwnedRwLockWriteGuard<()> {
        let (_, lock) = self
            .locks_for([key])
            .await
            .into_iter()
            .next()
            .expect("one key produces one lifecycle lock");
        lock.write_owned().await
    }

    pub async fn read_submission(&self) -> OwnedRwLockReadGuard<()> {
        self.submission.clone().read_owned().await
    }

    pub async fn write_submission(&self) -> OwnedRwLockWriteGuard<()> {
        self.submission.clone().write_owned().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn duplicate_keys_share_one_gate() {
        let gates = LifecycleGates::new();
        let key = RunKey::new("p", "r");
        let readers = gates.read_many([key.clone(), key.clone()]).await;
        assert_eq!(readers.len(), 1);

        let gates2 = gates.clone();
        let waiter = tokio::spawn(async move { gates2.write_many([key]).await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        drop(readers);
        assert_eq!(waiter.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn keyed_guards_map_each_run_to_its_own_lock() {
        let gates = LifecycleGates::new();
        let b = RunKey::new("p", "b");
        let a = RunKey::new("p", "a");
        let mut guards = gates
            .read_many_keyed([b.clone(), a.clone(), a.clone()])
            .await;
        assert_eq!(guards.len(), 2);

        guards.remove(&b);
        let write_gates = gates.clone();
        let b2 = b.clone();
        let b_writer = tokio::spawn(async move { write_gates.write_one(b2).await });
        let write_gates = gates.clone();
        let a2 = a.clone();
        let a_writer = tokio::spawn(async move { write_gates.write_one(a2).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), b_writer)
            .await
            .expect("B's write gate must free once B's keyed guard is dropped")
            .unwrap();
        tokio::task::yield_now().await;
        assert!(!a_writer.is_finished());
        drop(guards);
        a_writer.await.unwrap();
    }

    #[tokio::test]
    async fn claim_guard_drains_old_work_then_releases_new_checks() {
        let gates = LifecycleGates::new();
        let key = RunKey::new("p", "r");
        let admitted_before_claim = gates.read_many([key.clone()]).await;

        let claim_gates = gates.clone();
        let claim_key = key.clone();
        let claim = tokio::spawn(async move { claim_gates.write_one(claim_key).await });
        tokio::task::yield_now().await;
        assert!(!claim.is_finished());

        drop(admitted_before_claim);
        let claim_guard = claim.await.unwrap();
        let queued_gates = gates.clone();
        let queued = tokio::spawn(async move { queued_gates.read_many([key]).await });
        tokio::task::yield_now().await;
        assert!(!queued.is_finished());

        // After the durable claim, releasing the exclusive guard lets queued
        // requests reach the authoritative lifecycle check outside this gate.
        drop(claim_guard);
        assert_eq!(queued.await.unwrap().len(), 1);
    }
}
