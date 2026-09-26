use std::borrow::{Borrow, Cow};
use std::collections::{BTreeSet, HashMap};
use std::hash::Hash;

use dioxus::prelude::*;
use serde_json::Value;
use wasm_bindgen::JsValue;

use crate::components::copy_text::CopyText;
use crate::components::icons::{CaretDownIcon, CaretRightIcon};
use crate::grpc::proto::{RunInfo, RunStatus};
use crate::state::trash::compact_duration;
use crate::util::primary;

fn local_time(ms: i64) -> String {
    js_sys::Date::new(&JsValue::from_f64(ms as f64))
        .to_locale_string("en-US", &JsValue::UNDEFINED)
        .as_string()
        .unwrap_or_else(|| ms.to_string())
}

/// Augment the client-authored `info/run_info` document with a distinct set of
/// authoritative server timings, formatted in the viewer's local timezone.
/// The logger-authored `meta.time.start` is deliberately preserved: after a
/// re-init it describes the process that produced the surrounding metadata,
/// while `meta.server_time.run_created` describes the cumulative run identity.
fn add_server_timing_with(
    mut metadata: Value,
    run: &RunInfo,
    format_time: impl Fn(i64) -> String,
) -> Value {
    let Some(root) = metadata.as_object_mut() else {
        return metadata;
    };
    let Some(meta) = root
        .entry("meta")
        .or_insert_with(|| Value::Object(Default::default()))
        .as_object_mut()
    else {
        return metadata;
    };
    let Some(server_time) = meta
        .entry("server_time")
        .or_insert_with(|| Value::Object(Default::default()))
        .as_object_mut()
    else {
        return metadata;
    };

    let mut set_time = |key: &str, ms: i64| {
        server_time.insert(key.to_string(), Value::String(format_time(ms)));
    };

    set_time("run_created", run.created_at_ms);

    // Live ingest bumps only the run version, so ListRuns is too stale to use
    // this as a live activity indicator. Terminal ingest also bumps its
    // project, keeping the explicit final-state value fresh. For presumed-dead
    // runs this remains a snapshot from the latest lifecycle refresh.
    let show_last_ingested = run.terminated_at_ms.is_some()
        || matches!(
            run.status(),
            RunStatus::Crashed | RunStatus::Finished | RunStatus::PresumedDead
        );
    if show_last_ingested {
        if let Some(last_ingested_at_ms) = run.last_ingested_at_ms {
            set_time("last_ingested", last_ingested_at_ms);
        }
    }

    if let Some(terminated_at_ms) = run.terminated_at_ms {
        set_time("end", terminated_at_ms);
        server_time.insert(
            "wall_span".to_string(),
            Value::String(compact_duration(
                terminated_at_ms.saturating_sub(run.created_at_ms),
            )),
        );
    }

    metadata
}

pub(crate) fn add_server_timing(metadata: Value, run: &RunInfo) -> Value {
    add_server_timing_with(metadata, run, local_time)
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MetadataColumn {
    pub(crate) label: String,
    pub(crate) color: String,
    pub(crate) data: Value,
    /// `info/run_info` alone receives viewer-injected server timing, which is noise in a config-oriented diff. Arbitrary metadata has no reserved `meta.server_time` namespace and must retain values at that path.
    pub(crate) ignore_server_timing_in_diff: bool,
}

/// Table-based metadata viewer with per-run columns and diff highlighting.
#[component]
pub fn MetadataViewer(
    columns: Vec<MetadataColumn>,
    /// When true, hide rows whose values match across all runs (and any branches whose subtree contains no diffs). Comparison is meaningful only with two or more runs, so single-run views retain every key.
    #[props(default = false)]
    diff_only: bool,
) -> Element {
    if columns.is_empty() {
        return rsx! { div { class: "metadata-empty", "No metadata" } };
    }

    let all_keys = collect_all_keys(
        &columns
            .iter()
            .map(|column| &column.data)
            .collect::<Vec<_>>(),
    );

    let has_metadata = !all_keys.is_empty();

    // Diff-only mode strips out subtrees with no diffs. For single-run views
    // there are no diffs so we leave the tree intact — diff-only is only
    // meaningful when comparing two or more runs.
    let display_keys: Vec<KeyNode> = if diff_only && columns.len() > 1 {
        filter_diff_only(&all_keys, &columns, "")
    } else {
        all_keys
    };

    // Collapsed state: track which paths are collapsed
    let collapsed = use_signal(BTreeSet::<String>::new);

    if display_keys.is_empty() {
        // Distinguish "the runs logged no metadata" from "diff-only filtering removed every key".
        let message = if has_metadata {
            "No differing keys"
        } else {
            "No metadata"
        };
        return rsx! { div { class: "metadata-empty", "{message}" } };
    }

    rsx! {
        div { class: "metadata-viewer",
            table { class: "metadata-table",
                thead {
                    tr {
                        th { class: "metadata-key-col", "Key" }
                        for column in &columns {
                            {
                                let style = format!("color: {};", column.color);
                                rsx! {
                                    th { class: "metadata-val-col", style: "{style}", "{column.label}" }
                                }
                            }
                        }
                    }
                }
                tbody {
                    {render_rows(&display_keys, &columns, "", 0, &collapsed.read(), collapsed)}
                }
            }
        }
    }
}

/// Prunes the key tree to only paths that contain at least one leaf whose
/// values differ across runs. Branches whose entire subtree is uniform are
/// dropped.
fn filter_diff_only(
    keys: &[KeyNode],
    columns: &[MetadataColumn],
    parent_path: &str,
) -> Vec<KeyNode> {
    keys.iter()
        .filter_map(|node| {
            let key = match node {
                KeyNode::Leaf(key) | KeyNode::Branch(key, _) => key,
            };
            let path = child_pointer(parent_path, key);
            match node {
                KeyNode::Leaf(_) => {
                    if all_equal(&comparable_row(columns, &path)) {
                        None
                    } else {
                        Some(KeyNode::Leaf(key.clone()))
                    }
                }
                KeyNode::Branch(_, children) => {
                    let kept = filter_diff_only(children, columns, &path);
                    if kept.is_empty() {
                        None
                    } else {
                        Some(KeyNode::Branch(key.clone(), kept))
                    }
                }
            }
        })
        .collect()
}

/// A key in the recursive structure.
#[derive(Clone, Debug, PartialEq, Eq)]
enum KeyNode {
    Leaf(String),
    Branch(String, Vec<KeyNode>),
}

/// Collect all keys from multiple JSON values into a unified tree.
fn collect_all_keys<V: Borrow<Value>>(values: &[V]) -> Vec<KeyNode> {
    let mut keys = BTreeSet::new();
    for v in values {
        if let Value::Object(map) = v.borrow() {
            for k in map.keys() {
                keys.insert(k.clone());
            }
        }
    }

    keys.into_iter()
        .map(|k| {
            let children: Vec<&Value> = values.iter().filter_map(|v| v.borrow().get(&k)).collect();

            let all_nonempty_objects = children
                .iter()
                .all(|child| child.as_object().is_some_and(|object| !object.is_empty()));
            if all_nonempty_objects {
                KeyNode::Branch(k.clone(), collect_all_keys(&children))
            } else {
                KeyNode::Leaf(k)
            }
        })
        .collect()
}

/// Render table rows recursively.
fn render_rows(
    keys: &[KeyNode],
    columns: &[MetadataColumn],
    parent_path: &str,
    depth: usize,
    collapsed: &BTreeSet<String>,
    collapsed_signal: Signal<BTreeSet<String>>,
) -> Element {
    let indent = depth as f32 * 12.0;

    rsx! {
        for key_node in keys {
            {match key_node {
                KeyNode::Leaf(key) => {
                    let path = child_pointer(parent_path, key);

                    let values: Vec<Option<&Value>> = columns
                        .iter()
                        .map(|column| resolve_path(&column.data, &path))
                        .collect();

                    let tints = difference_tints(&comparable_row(columns, &path));
                    let displays = format_row_values(&values);

                    rsx! {
                        tr { class: "metadata-row",
                            td {
                                class: "metadata-key-cell",
                                role: "rowheader",
                                style: "padding-left: {indent + 16.0}px;",
                                "{key}"
                            }
                            for (i, (val, display)) in values.iter().zip(displays.iter()).enumerate() {
                                {
                                    let color = &columns[i].color;
                                    let bg = if tints[i] {
                                        format!("background: {}22;", color)
                                    } else {
                                        String::new()
                                    };
                                    let text = val.map(|value| copy_payload(value, display).to_owned());
                                    rsx! {
                                        td { class: "metadata-val-cell", style: "{bg}",
                                            if let Some(text) = text {
                                                CopyText {
                                                    text,
                                                    display: display.clone(),
                                                    class: "metadata-copy-value",
                                                }
                                            } else {
                                                "{display}"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                KeyNode::Branch(key, children) => {
                    let path = child_pointer(parent_path, key);
                    let is_collapsed = collapsed.contains(&path);
                    let toggle_path = path.clone();
                    let mut sig = collapsed_signal;

                    rsx! {
                        tr { class: "metadata-row metadata-branch",
                            td {
                                class: "metadata-key-cell metadata-branch-cell",
                                role: "rowheader",
                                style: "padding-left: {indent}px;",
                                button {
                                    r#type: "button",
                                    class: "metadata-branch-toggle",
                                    aria_expanded: !is_collapsed,
                                    onmousedown: primary(move |_| {
                                        let mut set = sig.write();
                                        if set.contains(&toggle_path) {
                                            set.remove(&toggle_path);
                                        } else {
                                            set.insert(toggle_path.clone());
                                        }
                                    }),
                                    span { class: "metadata-arrow",
                                        if is_collapsed { CaretRightIcon {} } else { CaretDownIcon {} }
                                    }
                                    "{key}"
                                }
                            }
                            for _ in columns {
                                td { class: "metadata-val-cell" }
                            }
                        }
                        if !is_collapsed {
                            {render_rows(children, columns, &path, depth + 1, collapsed, collapsed_signal)}
                        }
                    }
                },
            }}
        }
    }
}

/// Append an object key to an RFC 6901 JSON Pointer.
fn child_pointer(parent: &str, key: &str) -> String {
    let key = key.replace('~', "~0").replace('/', "~1");
    format!("{parent}/{key}")
}

/// Resolve an object-only JSON Pointer. Arrays remain leaf values in the
/// viewer, so numeric tokens must not start indexing them.
fn resolve_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    if path.is_empty() {
        return Some(root);
    }
    let mut current = root;
    for token in path.strip_prefix('/')?.split('/') {
        let key = token.replace("~1", "/").replace("~0", "~");
        current = current.as_object()?.get(&key)?;
    }
    Some(current)
}

fn all_equal<T: PartialEq>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] == pair[1])
}

/// Tint values outside a unique plurality; a tie tints every value.
fn difference_tints<T: Eq + Hash>(values: &[T]) -> Vec<bool> {
    let mut counts: HashMap<&T, usize> = HashMap::new();
    for value in values {
        *counts.entry(value).or_default() += 1;
    }
    let max = counts.values().copied().max().unwrap_or(0);
    let unique = counts.values().filter(|&&count| count == max).count() == 1;
    values
        .iter()
        .map(|value| !(unique && counts[value] == max))
        .collect()
}

/// A leaf's per-run values under the diff's notion of equality, as canonical JSON (serde_json maps are sorted), so a row groups in linear time.
fn comparable_row(columns: &[MetadataColumn], path: &str) -> Vec<Option<String>> {
    columns
        .iter()
        .map(|column| {
            diff_comparable_value(&column.data, path, column.ignore_server_timing_in_diff)
                .map(|value| value.to_string())
        })
        .collect()
}

/// A value for diff comparison, ignoring server timing even when `/meta` renders as one leaf.
fn diff_comparable_value<'a>(
    root: &'a Value,
    path: &str,
    ignore_server_timing: bool,
) -> Option<Cow<'a, Value>> {
    if ignore_server_timing
        && (path == "/meta/server_time" || path.starts_with("/meta/server_time/"))
    {
        return None;
    }
    let value = resolve_path(root, path)?;
    if !ignore_server_timing || path != "/meta" {
        return Some(Cow::Borrowed(value));
    }
    let mut comparable = value.clone();
    if let Some(meta) = comparable.as_object_mut() {
        meta.remove("server_time");
    }
    Some(Cow::Owned(comparable))
}

/// Only strings receive display-only quoting.
fn copy_payload<'a>(value: &'a Value, display: &'a str) -> &'a str {
    value.as_str().unwrap_or(display)
}

/// Keep strings unquoted and serialize other JSON values.
fn format_value(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        _ => v.to_string(),
    }
}

/// Missing values render blank; strings are quoted only when another value in the row would render identically.
fn format_row_values(values: &[Option<&Value>]) -> Vec<String> {
    let mut displays: Vec<String> = values
        .iter()
        .map(|value| value.map_or_else(String::new, format_value))
        .collect();

    let has_collision = (0..values.len()).any(|index| {
        ((index + 1)..values.len()).any(|other_index| {
            values[index] != values[other_index] && displays[index] == displays[other_index]
        })
    });
    if has_collision {
        for (display, value) in displays.iter_mut().zip(values) {
            if let Some(Value::String(text)) = value {
                *display = serde_json::to_string(text).expect("serializing a string cannot fail");
            }
        }
    }

    displays
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn columns(runs: Vec<(String, Value)>) -> Vec<MetadataColumn> {
        columns_with_timing_policy(runs, true)
    }

    fn columns_with_timing_policy(
        runs: Vec<(String, Value)>,
        ignore_server_timing_in_diff: bool,
    ) -> Vec<MetadataColumn> {
        runs.into_iter()
            .map(|(label, data)| MetadataColumn {
                color: format!("#{label}"),
                label,
                data,
                ignore_server_timing_in_diff,
            })
            .collect()
    }

    #[test]
    fn server_timing_preserves_client_start_and_adds_authoritative_section() {
        let metadata = json!({
            "meta": {
                "time": {
                    "start": "original",
                    "start_unix": 123,
                    "last_ingested_unix": 456,
                    "end_unix": 789
                }
            }
        });
        let run = RunInfo {
            created_at_ms: 1_000,
            last_ingested_at_ms: Some(61_000),
            terminated_at_ms: Some(62_000),
            status: RunStatus::Finished as i32,
            ..Default::default()
        };

        let result = add_server_timing_with(metadata, &run, |ms| format!("time-{ms}"));

        assert_eq!(result.pointer("/meta/time/start"), Some(&json!("original")));
        assert_eq!(
            result.pointer("/meta/server_time/run_created"),
            Some(&json!("time-1000"))
        );
        assert_eq!(
            result.pointer("/meta/server_time/last_ingested"),
            Some(&json!("time-61000"))
        );
        assert_eq!(
            result.pointer("/meta/server_time/end"),
            Some(&json!("time-62000"))
        );
        assert_eq!(
            result.pointer("/meta/server_time/wall_span"),
            Some(&json!("1m 1s"))
        );
        assert_eq!(result.pointer("/meta/time/start_unix"), Some(&json!(123)));
        assert_eq!(
            result.pointer("/meta/time/last_ingested_unix"),
            Some(&json!(456))
        );
        assert_eq!(result.pointer("/meta/time/end_unix"), Some(&json!(789)));
    }

    #[test]
    fn server_timing_creates_missing_server_section_without_faking_client_start() {
        let run = RunInfo {
            created_at_ms: 1_000,
            last_ingested_at_ms: Some(2_000),
            status: RunStatus::PresumedDead as i32,
            ..Default::default()
        };

        let result = add_server_timing_with(json!({"config": {}}), &run, |ms| format!("time-{ms}"));

        assert_eq!(result.pointer("/meta/time"), None);
        assert_eq!(
            result.pointer("/meta/server_time/run_created"),
            Some(&json!("time-1000"))
        );
        assert_eq!(
            result.pointer("/meta/server_time/last_ingested"),
            Some(&json!("time-2000"))
        );
        assert_eq!(result.pointer("/meta/server_time/end"), None);
        assert_eq!(result.pointer("/meta/server_time/wall_span"), None);
    }

    #[test]
    fn live_run_does_not_show_stale_last_ingested() {
        let run = RunInfo {
            created_at_ms: 1_000,
            last_ingested_at_ms: Some(2_000),
            status: RunStatus::Running as i32,
            ..Default::default()
        };

        let result = add_server_timing_with(json!({}), &run, |ms| format!("time-{ms}"));

        assert_eq!(
            result.pointer("/meta/server_time/run_created"),
            Some(&json!("time-1000"))
        );
        assert_eq!(result.pointer("/meta/server_time/last_ingested"), None);
    }

    #[test]
    fn diff_only_excludes_server_timing_but_keeps_client_metadata_differences() {
        let runs = columns(vec![
            (
                "one".to_string(),
                json!({
                    "config": {"size": 1},
                    "meta": {
                        "time": {"end": "first", "runtime": "1m"},
                        "server_time": {"end": "server-first", "wall_span": "1m"},
                        "system": {"hostname": "alpha"}
                    }
                }),
            ),
            (
                "two".to_string(),
                json!({
                    "config": {"size": 2},
                    "meta": {
                        "time": {"end": "second", "runtime": "2m"},
                        "server_time": {"end": "server-second", "wall_span": "2m"},
                        "system": {"hostname": "beta"}
                    }
                }),
            ),
        ]);
        let keys = collect_all_keys(
            &runs
                .iter()
                .map(|column| column.data.clone())
                .collect::<Vec<_>>(),
        );

        let filtered = filter_diff_only(&keys, &runs, "");

        assert_eq!(
            filtered,
            vec![
                KeyNode::Branch(
                    "config".to_string(),
                    vec![KeyNode::Leaf("size".to_string())]
                ),
                KeyNode::Branch(
                    "meta".to_string(),
                    vec![
                        KeyNode::Branch(
                            "system".to_string(),
                            vec![KeyNode::Leaf("hostname".to_string())]
                        ),
                        KeyNode::Branch(
                            "time".to_string(),
                            vec![
                                KeyNode::Leaf("end".to_string()),
                                KeyNode::Leaf("runtime".to_string())
                            ]
                        )
                    ]
                )
            ]
        );
    }

    #[test]
    fn metadata_paths_preserve_arbitrary_object_keys_without_indexing_arrays() {
        let value = json!({
            "a.b": "literal dot",
            "a": {"b": "nested", "": "empty child"},
            "a/b": "slash",
            "a~b": "tilde",
            "": {"child": "empty root key"},
            "mixed": {"0": "object key"},
            "array": ["array item"]
        });

        let nested_a = child_pointer("", "a");
        assert_eq!(
            resolve_path(&value, &child_pointer("", "a.b")),
            Some(&json!("literal dot"))
        );
        assert_eq!(
            resolve_path(&value, &child_pointer(&nested_a, "b")),
            Some(&json!("nested"))
        );
        assert_eq!(
            resolve_path(&value, &child_pointer(&nested_a, "")),
            Some(&json!("empty child"))
        );
        assert_eq!(
            resolve_path(&value, &child_pointer("", "a/b")),
            Some(&json!("slash"))
        );
        assert_eq!(
            resolve_path(&value, &child_pointer("", "a~b")),
            Some(&json!("tilde"))
        );
        assert_eq!(
            resolve_path(&value, &child_pointer(&child_pointer("", ""), "child")),
            Some(&json!("empty root key"))
        );
        assert_eq!(resolve_path(&value, "/mixed/0"), Some(&json!("object key")));
        assert_eq!(resolve_path(&value, "/array/0"), None);
    }

    #[test]
    fn diff_only_distinguishes_literal_paths_from_nested_server_timing() {
        let runs = columns(vec![
            (
                "one".to_string(),
                json!({
                    "a.b": 1,
                    "a": {"b": 7},
                    "meta.server_time": "one",
                    "meta": {"server_time": {"end": "one"}}
                }),
            ),
            (
                "two".to_string(),
                json!({
                    "a.b": 2,
                    "a": {"b": 7},
                    "meta.server_time": "two",
                    "meta": {"server_time": {"end": "two"}}
                }),
            ),
        ]);
        let keys = collect_all_keys(
            &runs
                .iter()
                .map(|column| column.data.clone())
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            filter_diff_only(&keys, &runs, ""),
            vec![
                KeyNode::Leaf("a.b".to_string()),
                KeyNode::Leaf("meta.server_time".to_string()),
            ]
        );
    }

    #[test]
    fn mixed_and_empty_object_shapes_render_as_lossless_leaves() {
        let values = vec![
            json!({
                "empty": {},
                "optimizer": "adam",
                "optional": {"x": 1}
            }),
            json!({
                "empty": {"x": 1},
                "optimizer": {"lr": 0.001},
                "optional": {"y": 2}
            }),
            json!({}),
        ];
        let keys = collect_all_keys(&values);

        assert_eq!(
            keys,
            vec![
                KeyNode::Leaf("empty".to_string()),
                KeyNode::Leaf("optimizer".to_string()),
                KeyNode::Branch(
                    "optional".to_string(),
                    vec![
                        KeyNode::Leaf("x".to_string()),
                        KeyNode::Leaf("y".to_string())
                    ]
                ),
            ]
        );
        assert_eq!(
            values
                .iter()
                .map(|value| resolve_path(value, "/optimizer").map(format_value))
                .collect::<Vec<_>>(),
            vec![
                Some("adam".to_string()),
                Some("{\"lr\":0.001}".to_string()),
                None,
            ]
        );
        let runs = values
            .iter()
            .enumerate()
            .map(|(index, value)| (index.to_string(), value.clone()))
            .collect();
        let runs = columns(runs);
        assert_eq!(filter_diff_only(&keys, &runs, ""), keys);
    }

    #[test]
    fn leaf_shaped_meta_still_excludes_server_timing_from_diffs() {
        let timing_only = columns(vec![
            ("one".to_string(), json!({"meta": {}})),
            (
                "two".to_string(),
                json!({"meta": {"server_time": {"end": "two"}}}),
            ),
        ]);
        let timing_keys = collect_all_keys(
            &timing_only
                .iter()
                .map(|column| column.data.clone())
                .collect::<Vec<_>>(),
        );
        assert_eq!(timing_keys, vec![KeyNode::Leaf("meta".to_string())]);
        assert!(filter_diff_only(&timing_keys, &timing_only, "").is_empty());

        let client_difference = columns(vec![
            ("one".to_string(), json!({"meta": {}})),
            (
                "two".to_string(),
                json!({"meta": {"client": 2, "server_time": {"end": "two"}}}),
            ),
        ]);
        let client_keys = collect_all_keys(
            &client_difference
                .iter()
                .map(|column| column.data.clone())
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            filter_diff_only(&client_keys, &client_difference, ""),
            vec![KeyNode::Leaf("meta".to_string())]
        );
    }

    #[test]
    fn custom_metadata_keeps_server_time_differences() {
        let custom_objects = columns_with_timing_policy(
            vec![
                (
                    "one".to_string(),
                    json!({"meta": {"server_time": {"source": "one"}}}),
                ),
                (
                    "two".to_string(),
                    json!({"meta": {"server_time": {"source": "two"}}}),
                ),
            ],
            false,
        );
        let object_keys = collect_all_keys(
            &custom_objects
                .iter()
                .map(|column| column.data.clone())
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            filter_diff_only(&object_keys, &custom_objects, ""),
            object_keys
        );

        let custom_scalars = columns_with_timing_policy(
            vec![
                ("one".to_string(), json!({"meta": {"server_time": "one"}})),
                ("two".to_string(), json!({"meta": {"server_time": "two"}})),
            ],
            false,
        );
        let scalar_keys = collect_all_keys(
            &custom_scalars
                .iter()
                .map(|column| column.data.clone())
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            filter_diff_only(&scalar_keys, &custom_scalars, ""),
            scalar_keys
        );
    }

    #[test]
    fn metadata_comparison_preserves_json_types() {
        let boolean = json!(true);
        let boolean_string = json!("true");
        assert!(!all_equal(&[Some(&boolean), Some(&boolean_string)]));
        assert!(all_equal(&[Some(&boolean), Some(&boolean)]));

        let runs = columns(vec![
            (
                "one".to_string(),
                json!({
                    "array": [1],
                    "boolean": true,
                    "nullish": null,
                    "number": 1,
                    "object": {"a": 1},
                    "same": {"typed": true}
                }),
            ),
            (
                "two".to_string(),
                json!({
                    "array": "[1]",
                    "boolean": "true",
                    "number": "1",
                    "object": "{\"a\":1}",
                    "same": {"typed": true}
                }),
            ),
        ]);
        let keys = collect_all_keys(
            &runs
                .iter()
                .map(|column| column.data.clone())
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            filter_diff_only(&keys, &runs, ""),
            vec![
                KeyNode::Leaf("array".to_string()),
                KeyNode::Leaf("boolean".to_string()),
                KeyNode::Leaf("nullish".to_string()),
                KeyNode::Leaf("number".to_string()),
                KeyNode::Leaf("object".to_string()),
            ]
        );
    }

    #[test]
    fn row_formatting_disambiguates_colliding_json_types() {
        let boolean = json!(true);
        let boolean_string = json!("true");
        let displays = format_row_values(&[Some(&boolean), Some(&boolean_string)]);
        assert_eq!(displays, vec!["true", "\"true\""]);
        assert_eq!(copy_payload(&boolean_string, &displays[1]), "true");
        assert_eq!(copy_payload(&boolean, &displays[0]), displays[0]);

        let number = json!(1);
        let number_string = json!("1");
        assert_eq!(
            format_row_values(&[Some(&number), Some(&number_string)]),
            vec!["1", "\"1\""]
        );

        let object = json!({"a": 1});
        let object_string = json!("{\"a\":1}");
        assert_eq!(
            format_row_values(&[Some(&object), Some(&object_string)]),
            vec!["{\"a\":1}", "\"{\\\"a\\\":1}\""]
        );

        let ordinary = json!("ordinary");
        let other = json!(false);
        assert_eq!(
            format_row_values(&[Some(&ordinary), Some(&ordinary), Some(&other)]),
            vec!["ordinary", "ordinary", "false"]
        );

        let quoted_boolean_string = json!("\"true\"");
        assert_eq!(
            format_row_values(&[
                Some(&quoted_boolean_string),
                Some(&boolean_string),
                Some(&boolean),
            ]),
            vec!["\"\\\"true\\\"\"", "\"true\"", "true"]
        );
    }

    #[test]
    fn missing_values_render_blank_and_quote_colliding_empty_strings() {
        assert_eq!(format_row_values(&[None]), vec![""]);

        let empty = json!("");
        assert_eq!(format_row_values(&[None, Some(&empty)]), vec!["", "\"\""]);
        assert_eq!(
            format_row_values(&[Some(&empty), Some(&empty)]),
            vec!["", ""]
        );

        let question = json!("?");
        assert_eq!(format_row_values(&[None, Some(&question)]), vec!["", "?"]);
    }

    #[test]
    fn difference_tints_mark_cells_outside_a_unique_plurality() {
        assert_eq!(difference_tints(&["a", "a", "b"]), vec![false, false, true]);
        assert_eq!(
            difference_tints(&["a", "b", "a", "c"]),
            vec![false, true, false, true]
        );
        assert_eq!(difference_tints(&["a", "b"]), vec![true, true]);
        assert_eq!(
            difference_tints(&["a", "a", "b", "b"]),
            vec![true, true, true, true]
        );
        assert_eq!(difference_tints(&["a", "b", "c"]), vec![true, true, true]);
        assert_eq!(
            difference_tints(&["a", "a", "a"]),
            vec![false, false, false]
        );
        assert_eq!(difference_tints(&["a"]), vec![false]);

        assert_eq!(
            difference_tints(&[Some(1), Some(1), None]),
            vec![false, false, true]
        );
        assert_eq!(
            difference_tints(&[None, Some(1), None]),
            vec![false, true, false]
        );
    }

    #[test]
    fn difference_tints_ignore_server_timing_like_the_diff_filter() {
        let runs = columns(vec![
            (
                "one".to_string(),
                json!({"lr": 1, "meta": {"server_time": {"end": "one"}}}),
            ),
            (
                "two".to_string(),
                json!({"lr": 1, "meta": {"server_time": {"end": "two"}}}),
            ),
            (
                "three".to_string(),
                json!({"lr": 2, "meta": {"server_time": {"end": "three"}}}),
            ),
        ]);

        assert_eq!(
            difference_tints(&comparable_row(&runs, "/lr")),
            vec![false, false, true]
        );
        assert_eq!(
            difference_tints(&comparable_row(&runs, "/meta/server_time/end")),
            vec![false, false, false]
        );
    }

    #[test]
    fn comparable_rows_ignore_object_key_order() {
        let runs = columns(vec![
            ("one".to_string(), json!({"opt": {"a": 1, "b": 2}})),
            ("two".to_string(), json!({"opt": {"b": 2, "a": 1}})),
        ]);
        assert_eq!(
            difference_tints(&comparable_row(&runs, "/opt")),
            vec![false, false]
        );
    }

    #[test]
    fn diff_only_treats_missing_as_different_from_any_present_value() {
        let runs = columns(vec![
            ("one".to_string(), json!({"empty": "", "value": 1})),
            ("two".to_string(), json!({})),
        ]);
        let keys = collect_all_keys(
            &runs
                .iter()
                .map(|column| column.data.clone())
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            filter_diff_only(&keys, &runs, ""),
            vec![
                KeyNode::Leaf("empty".to_string()),
                KeyNode::Leaf("value".to_string()),
            ]
        );
    }
}
