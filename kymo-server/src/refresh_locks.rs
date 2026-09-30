//! Per-key async serialization without retaining request-derived keys.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Weak};

/// A weak registry gives each active key one Tokio mutex, then removes the key
/// when its last leader or waiter drops. Keeping this lifecycle in one place is
/// important: cancellation while queued must not leak a second unbounded cache.
pub struct RefreshLocks<K, State> {
    inner: Arc<std::sync::Mutex<HashMap<K, Weak<tokio::sync::Mutex<State>>>>>,
}

impl<K, State> Clone for RefreshLocks<K, State> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, State> Default for RefreshLocks<K, State> {
    fn default() -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }
}

impl<K, State> RefreshLocks<K, State>
where
    K: Clone + Eq + Hash,
    State: Default,
{
    pub fn lease_for(&self, key: &K) -> RefreshLease<K, State> {
        let mut inner = self.inner.lock().unwrap();
        // Drop runs before a lease's Arc field is released. Two concurrent
        // droppers can therefore both observe another strong owner and both
        // skip exact removal; sweep those dead Weak entries on the next
        // acquisition so request-derived keys cannot accumulate forever.
        inner.retain(|_, slot| slot.strong_count() > 0);
        let slot = inner.get(key).and_then(Weak::upgrade).unwrap_or_else(|| {
            let slot = Arc::new(tokio::sync::Mutex::new(State::default()));
            inner.insert(key.clone(), Arc::downgrade(&slot));
            slot
        });
        RefreshLease {
            registry: self.inner.clone(),
            key: key.clone(),
            slot,
        }
    }

    #[cfg(test)]
    pub fn registry_len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    #[cfg(test)]
    pub fn lease_count(&self, key: &K) -> usize {
        self.inner
            .lock()
            .unwrap()
            .get(key)
            .map(Weak::strong_count)
            .unwrap_or(0)
    }
}

pub struct RefreshLease<K: Eq + Hash, State> {
    registry: Arc<std::sync::Mutex<HashMap<K, Weak<tokio::sync::Mutex<State>>>>>,
    key: K,
    slot: Arc<tokio::sync::Mutex<State>>,
}

impl<K: Eq + Hash, State> RefreshLease<K, State> {
    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, State> {
        self.slot.lock().await
    }

    /// The slot's lock if it is free right now, owned so a caller can hold several slots without borrowing their leases.
    pub fn try_lock_owned(&self) -> Option<tokio::sync::OwnedMutexGuard<State>> {
        self.slot.clone().try_lock_owned().ok()
    }
}

impl<K: Eq + Hash, State> Drop for RefreshLease<K, State> {
    fn drop(&mut self) {
        let mut registry = self.registry.lock().unwrap();
        let owns_entry = registry.get(&self.key).is_some_and(|stored| {
            stored.ptr_eq(&Arc::downgrade(&self.slot)) && Arc::strong_count(&self.slot) == 1
        });
        if owns_entry {
            registry.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn leases_serialize_and_the_last_drop_removes_the_key() {
        let locks = Arc::new(RefreshLocks::<String, ()>::default());
        let key = "key".to_string();
        let leader = locks.lease_for(&key);
        let leader_guard = leader.lock().await;
        let waiter_locks = locks.clone();
        let waiter_key = key.clone();
        let mut waiter = tokio::spawn(async move {
            let lease = waiter_locks.lease_for(&waiter_key);
            let _guard = lease.lock().await;
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while locks.lease_count(&key) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("waiter should join the leader's slot");
        assert_eq!(locks.registry_len(), 1);
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut waiter)
            .await
            .is_err());

        drop(leader_guard);
        drop(leader);
        waiter.await.unwrap();
        assert_eq!(locks.registry_len(), 0);
    }

    #[tokio::test]
    async fn cancelling_a_waiter_does_not_leak_its_key() {
        let locks = Arc::new(RefreshLocks::<String, ()>::default());
        let key = "key".to_string();
        let leader = locks.lease_for(&key);
        let leader_guard = leader.lock().await;
        let waiter_locks = locks.clone();
        let waiter_key = key.clone();
        let waiter = tokio::spawn(async move {
            let lease = waiter_locks.lease_for(&waiter_key);
            let _guard = lease.lock().await;
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while locks.lease_count(&key) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("waiter should join the leader's slot");
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert_eq!(locks.registry_len(), 1);

        drop(leader_guard);
        drop(leader);
        assert_eq!(locks.registry_len(), 0);
    }

    #[test]
    fn acquiring_a_key_sweeps_dead_weak_entries() {
        let locks = RefreshLocks::<String, ()>::default();
        let dead_key = "dead".to_string();
        let dead_slot = Arc::new(tokio::sync::Mutex::new(()));
        locks
            .inner
            .lock()
            .unwrap()
            .insert(dead_key.clone(), Arc::downgrade(&dead_slot));
        drop(dead_slot);
        assert_eq!(locks.registry_len(), 1);

        let live = locks.lease_for(&"live".to_string());
        assert_eq!(locks.registry_len(), 1);
        assert!(!locks.inner.lock().unwrap().contains_key(&dead_key));
        drop(live);
        assert_eq!(locks.registry_len(), 0);
    }
}
