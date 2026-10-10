//! Session-local search, active tabs, and logical positions; no fetched log text is retained.

use std::cell::RefCell;

use super::viewport::ScrollAnchor;
use crate::state::panel_cache::Store;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct TextPanelState {
    pub(super) draft: String,
    pub(super) committed: String,
    pub(super) tab: Option<String>,
}

thread_local! {
    // Panel state and positions outlive Far unmounts; fetched text never does. Bound both the number of entries and their strings.
    static TEXT_PANEL: RefCell<Store<TextPanelState>> = RefCell::new(Store::with_weight_limit(2048, 2 * 1024 * 1024));
    static TEXT_SCROLL: RefCell<Store<ScrollAnchor>> = RefCell::new(Store::with_weight_limit(2048, 2 * 1024 * 1024));
}

pub(super) fn text_scroll_key(panel: &str, log_key: &str) -> String {
    serde_json::to_string(&(panel, log_key))
        .expect("text scroll identity contains only JSON strings")
}

pub(super) fn remembered_scroll(key: &str) -> Option<ScrollAnchor> {
    TEXT_SCROLL.with(|cache| cache.borrow_mut().get(key))
}

pub(super) fn remember_scroll(key: &str, anchor: ScrollAnchor) {
    TEXT_SCROLL.with(|cache| {
        cache.borrow_mut().put_weighted(
            key.to_string(),
            anchor,
            key.len() + size_of::<ScrollAnchor>(),
        );
    });
}

pub(super) fn remembered_panel(key: &str) -> TextPanelState {
    TEXT_PANEL
        .with(|cache| cache.borrow_mut().get(key))
        .unwrap_or_default()
}

pub(super) fn remember_panel(key: &str, state: TextPanelState) {
    let weight = key.len()
        + state.draft.len()
        + state.committed.len()
        + state.tab.as_ref().map_or(0, String::len);
    TEXT_PANEL.with(|cache| {
        cache
            .borrow_mut()
            .put_weighted(key.to_string(), state, weight)
    });
}

pub(super) fn text_run_key(project_id: &str, run_id: &str) -> String {
    serde_json::to_string(&(project_id, run_id)).expect("run identity contains only JSON strings")
}

// Log identity also scopes persistence; search revisions refresh without remounting.
pub(super) fn text_log_key(
    project_id: &str,
    run_id: &str,
    metric_names: &[String],
    search: &str,
) -> String {
    serde_json::to_string(&(project_id, run_id, metric_names, search))
        .expect("text log identity contains only JSON strings")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::panel_cache::panel_key;

    #[test]
    fn text_log_keys_preserve_identity_boundaries() {
        let key = |project: &str, run: &str, metrics: &[&str]| {
            let metrics = metrics
                .iter()
                .map(|value| (*value).to_string())
                .collect::<Vec<_>>();
            text_log_key(project, run, &metrics, "needle")
        };

        assert_ne!(key("p", "r", &["a\u{1e}b"]), key("p", "r", &["a", "b"]));
        assert_ne!(
            key("a", "b\u{1f}c", &["metric"]),
            key("a\u{1f}b", "c", &["metric"]),
        );
        assert_ne!(key("p", "r", &["a", "b"]), key("p", "r", &["b", "a"]));
        assert_ne!(text_run_key("a", "b\u{1f}c"), text_run_key("a\u{1f}b", "c"));
    }

    #[test]
    fn panel_state_round_trips_per_panel() {
        let panel = panel_key("dashboard", "logs");
        let maximized_panel = format!("{panel}\u{1f}max");
        let expected = TextPanelState {
            draft: " warning ".to_string(),
            committed: "warning".to_string(),
            tab: Some(text_run_key("project", "run-b")),
        };
        remember_panel(&panel, expected.clone());
        assert_eq!(remembered_panel(&panel), expected);
        assert_eq!(
            remembered_panel(&maximized_panel),
            TextPanelState::default()
        );
    }

    #[test]
    fn scroll_positions_are_scoped_to_panel_and_log() {
        let panel = panel_key("dashboard", "logs");
        let maximized_panel = format!("{panel}\u{1f}max");
        let other_panel = panel_key("dashboard", "other-logs");
        let metrics = vec!["logs/std_out".to_string()];
        let log = text_log_key("project", "run", &metrics, "");
        let other_run = text_log_key("project", "other-run", &metrics, "");
        let search = text_log_key("project", "run", &metrics, "warning");
        let keys = [
            text_scroll_key(&panel, &log),
            text_scroll_key(&other_panel, &log),
            text_scroll_key(&maximized_panel, &log),
            text_scroll_key(&panel, &other_run),
            text_scroll_key(&panel, &search),
        ];
        for (index, key) in keys.iter().enumerate() {
            remember_scroll(key, ScrollAnchor::Line((index + 1) as f64 * 1000.0));
        }
        for (index, key) in keys.iter().enumerate() {
            assert_eq!(
                remembered_scroll(key),
                Some(ScrollAnchor::Line((index + 1) as f64 * 1000.0))
            );
        }
        remember_scroll(&keys[0], ScrollAnchor::End);
        assert_eq!(remembered_scroll(&keys[0]), Some(ScrollAnchor::End));

        assert_ne!(
            text_scroll_key("a", "b\u{1f}c"),
            text_scroll_key("a\u{1f}b", "c"),
        );
    }

    #[test]
    fn scroll_cache_accounts_for_identity_bytes() {
        let oversized_key = "z".repeat(2 * 1024 * 1024);
        remember_scroll(&oversized_key, ScrollAnchor::Line(789.0));
        assert_eq!(remembered_scroll(&oversized_key), None);
    }
}
