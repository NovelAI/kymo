//! Session-lifetime panel caches. Panel bodies unmount at Zone::Far (see metric_rect.rs), so state that must survive a scroll-away lives in module-level stores instead of component signals. Query caches revalidate entries against their current inputs, but keys still have to be collision-free because gallery navigation and text search restore their values directly without a second ownership check.

use std::collections::HashMap;

/// Touch-stamped map with a hard entry cap and an optional byte-weight cap.
/// Panels the user scrolls between stay warm; least-recently touched entries
/// age out first. Consumers declare their own `thread_local!` store next to
/// their entry type.
pub struct Store<T> {
    map: HashMap<String, (u64, usize, T)>,
    clock: u64,
    cap: usize,
    max_weight: Option<usize>,
    total_weight: usize,
}

impl<T> Store<T> {
    pub fn new(cap: usize) -> Self {
        Self {
            map: HashMap::new(),
            clock: 0,
            cap,
            max_weight: None,
            total_weight: 0,
        }
    }

    pub fn with_weight_limit(cap: usize, max_weight: usize) -> Self {
        Self {
            map: HashMap::new(),
            clock: 0,
            cap,
            max_weight: Some(max_weight),
            total_weight: 0,
        }
    }

    pub fn get(&mut self, key: &str) -> Option<T>
    where
        T: Clone,
    {
        self.clock += 1;
        let clock = self.clock;
        self.map.get_mut(key).map(|(stamp, _, value)| {
            *stamp = clock;
            value.clone()
        })
    }

    pub fn put(&mut self, key: String, value: T) {
        self.put_weighted(key, value, 0);
    }

    /// Insert an entry with its estimated retained heap bytes. Returns false
    /// when one entry alone exceeds the store's budget; oversized responses
    /// remain usable by their mounted panel but are not retained off-screen.
    pub fn put_weighted(&mut self, key: String, value: T, weight: usize) -> bool {
        if self.cap == 0 || self.max_weight.is_some_and(|limit| weight > limit) {
            self.remove(&key);
            return false;
        }

        self.remove(&key);
        while self.map.len() >= self.cap
            || self
                .max_weight
                .is_some_and(|limit| self.total_weight.saturating_add(weight) > limit)
        {
            let Some(oldest_key) = self
                .map
                .iter()
                .min_by_key(|(_, (stamp, _, _))| *stamp)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            self.remove(&oldest_key);
        }
        self.clock += 1;
        self.total_weight = self.total_weight.saturating_add(weight);
        self.map.insert(key, (self.clock, weight, value));
        true
    }

    pub fn remove(&mut self, key: &str) -> Option<T> {
        self.map.remove(key).map(|(_, weight, value)| {
            self.total_weight = self.total_weight.saturating_sub(weight);
            value
        })
    }
}

/// Cache key scoped to the dashboard: auto-generated rect ids are bare metric names, so the same id recurs across projects.
pub fn panel_key(project_id: &str, rect_id: &str) -> String {
    serde_json::to_string(&(project_id, rect_id))
        .expect("panel identity contains only JSON strings")
}

#[cfg(test)]
mod tests {
    use super::{panel_key, Store};

    #[test]
    fn panel_keys_preserve_identity_boundaries_and_overlay_namespace() {
        assert_ne!(panel_key("a", "b\u{1f}c"), panel_key("a\u{1f}b", "c"),);
        assert_ne!(
            panel_key("project", "metric\u{1f}max"),
            format!("{}\u{1f}max", panel_key("project", "metric")),
        );
    }

    #[test]
    fn entry_cap_evicts_the_least_recently_used_value() {
        let mut store = Store::new(2);
        store.put("first".to_string(), 1);
        store.put("second".to_string(), 2);
        assert_eq!(store.get("first"), Some(1));
        store.put("third".to_string(), 3);
        assert_eq!(store.get("second"), None);
        assert_eq!(store.get("first"), Some(1));
        assert_eq!(store.get("third"), Some(3));
    }

    #[test]
    fn remove_returns_and_evicts_the_entry() {
        let mut store = Store::new(2);
        store.put("panel".to_string(), 7);

        assert_eq!(store.remove("panel"), Some(7));
        assert_eq!(store.get("panel"), None);
        assert_eq!(store.remove("panel"), None);
    }

    #[test]
    fn weighted_store_evicts_lru_until_the_byte_budget_fits() {
        let mut store = Store::with_weight_limit(10, 10);
        assert!(store.put_weighted("old".to_string(), 1, 4));
        assert!(store.put_weighted("warm".to_string(), 2, 4));
        assert_eq!(store.get("warm"), Some(2));

        assert!(store.put_weighted("new".to_string(), 3, 5));
        assert_eq!(store.get("old"), None);
        assert_eq!(store.get("warm"), Some(2));
        assert_eq!(store.get("new"), Some(3));
    }

    #[test]
    fn weighted_store_accounts_for_replacement_and_rejects_oversized_entries() {
        let mut store = Store::with_weight_limit(10, 10);
        assert!(store.put_weighted("panel".to_string(), 1, 9));
        assert!(store.put_weighted("panel".to_string(), 2, 2));
        assert!(store.put_weighted("neighbor".to_string(), 3, 8));
        assert_eq!(store.get("panel"), Some(2));
        assert_eq!(store.get("neighbor"), Some(3));

        assert!(!store.put_weighted("huge".to_string(), 4, 11));
        assert_eq!(store.get("huge"), None);
    }
}
