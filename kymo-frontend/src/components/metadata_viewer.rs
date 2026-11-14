use std::collections::{BTreeSet, HashMap};
use std::hash::Hash;

use dioxus::prelude::*;
use serde_json::Value;

use crate::components::copy_text::CopyText;
use crate::components::icons::{CaretDownIcon, CaretRightIcon};
use crate::grpc::proto::{RunInfo, RunStatus};
use crate::state::trash::compact_duration;
use crate::util::{local_time, primary};

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
    diff_only: bool,
) -> Element {
    let collapsed = use_signal(BTreeSet::<String>::new);

    let all_keys = column_keys(&columns);
    if all_keys.is_empty() {
        return rsx! { div { class: "metadata-empty", "No metadata" } };
    }

    let display_keys = if diff_only && columns.len() > 1 {
        filter_diff_only(&all_keys, &columns, "")
    } else {
        all_keys
    };
    if display_keys.is_empty() {
        return rsx! { div { class: "metadata-empty", "No differing keys" } };
    }

    rsx! {
        div { class: "metadata-viewer",
            table { class: "metadata-table",
                thead {
                    tr {
                        th { class: "metadata-key-col", "Key" }
                        for column in &columns {
                            th { class: "metadata-val-col", style: "color: {column.color};", "{column.label}" }
                        }
                    }
                }
                tbody {
                    {render_rows(&display_keys, &columns, "", 0, collapsed)}
                }
            }
        }
    }
}

/// Prunes the key tree to the leaves whose values differ across runs and the branches holding them.
fn filter_diff_only(
    keys: &[KeyNode],
    columns: &[MetadataColumn],
    parent_path: &str,
) -> Vec<KeyNode> {
    keys.iter()
        .filter_map(|node| match node {
            KeyNode::Leaf(key) => {
                let row = comparable_row(columns, &child_pointer(parent_path, key));
                row.windows(2)
                    .any(|pair| pair[0] != pair[1])
                    .then(|| node.clone())
            }
            KeyNode::Branch(key, children) => {
                let kept = filter_diff_only(children, columns, &child_pointer(parent_path, key));
                (!kept.is_empty()).then(|| KeyNode::Branch(key.clone(), kept))
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
fn collect_all_keys(values: &[&Value]) -> Vec<KeyNode> {
    let mut keys = BTreeSet::new();
    for v in values {
        if let Value::Object(map) = v {
            for k in map.keys() {
                keys.insert(k.clone());
            }
        }
    }

    keys.into_iter()
        .map(|k| {
            let children: Vec<&Value> = values.iter().filter_map(|v| v.get(&k)).collect();

            // A key whose values are all objects is a branch when any of them holds a key, since a branch of only `{}` would open to nothing; each `{}` among them shows on the branch row (empty_object_cells).
            if children.iter().all(|child| child.is_object()) {
                let grandchildren = collect_all_keys(&children);
                if !grandchildren.is_empty() {
                    return KeyNode::Branch(k, grandchildren);
                }
            }
            KeyNode::Leaf(k)
        })
        .collect()
}

/// The key tree of every run's metadata.
fn column_keys(columns: &[MetadataColumn]) -> Vec<KeyNode> {
    collect_all_keys(
        &columns
            .iter()
            .map(|column| &column.data)
            .collect::<Vec<_>>(),
    )
}

/// Render table rows recursively.
fn render_rows(
    keys: &[KeyNode],
    columns: &[MetadataColumn],
    parent_path: &str,
    depth: usize,
    mut collapsed: Signal<BTreeSet<String>>,
) -> Element {
    let indent = depth as f32 * 12.0;

    rsx! {
        for key_node in keys {
            {match key_node {
                KeyNode::Leaf(key) => {
                    let path = child_pointer(parent_path, key);
                    let tints = difference_tints(&comparable_row(columns, &path));

                    rsx! {
                        tr { class: "metadata-row",
                            td {
                                class: "metadata-key-cell",
                                role: "rowheader",
                                style: "padding-left: {indent + 16.0}px;",
                                "{key}"
                            }
                            for (column, tint) in columns.iter().zip(tints) {
                                {value_cell(column, tint, resolve_path(&column.data, &path))}
                            }
                        }
                    }
                },
                KeyNode::Branch(key, children) => {
                    let path = child_pointer(parent_path, key);
                    let cells = empty_object_cells(columns, &path);
                    let tints = cells.iter().any(Option::is_some).then(|| difference_tints(&cells));
                    let is_collapsed = collapsed.read().contains(&path);
                    let toggle_path = path.clone();

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
                                        let mut set = collapsed.write();
                                        if !set.remove(&toggle_path) {
                                            set.insert(toggle_path.clone());
                                        }
                                    }),
                                    span { class: "metadata-arrow",
                                        if is_collapsed { CaretRightIcon {} } else { CaretDownIcon {} }
                                    }
                                    "{key}"
                                }
                            }
                            if let Some(tints) = tints {
                                for ((column, cell), tint) in columns.iter().zip(cells).zip(tints) {
                                    {value_cell(column, tint, cell)}
                                }
                            } else {
                                // No `{}` on this row: plain cells, which skip value_cell's style edit and placeholder node.
                                for _ in columns {
                                    td { class: "metadata-val-cell" }
                                }
                            }
                        }
                        if !is_collapsed {
                            {render_rows(children, columns, &path, depth + 1, collapsed)}
                        }
                    }
                },
            }}
        }
    }
}

/// A run's value cell: its value, if any, as copyable text, tinted in the run's color when `tint`.
fn value_cell(column: &MetadataColumn, tint: bool, value: Option<&Value>) -> Element {
    let cell = value.map(|value| {
        let display = format_value(value);
        (copy_payload(value, &display).to_owned(), display)
    });
    rsx! {
        // An untinted cell has no style: Dioxus keeps the old background when it sets `style: ""`.
        td { class: "metadata-val-cell", style: tint.then(|| format!("background: {}22;", column.color)),
            if let Some((text, display)) = cell {
                CopyText { text, display, class: "metadata-copy-value" }
            }
        }
    }
}

/// A branch row's cells: each run's empty object, or nothing for a non-empty object or an absent key.
fn empty_object_cells<'a>(columns: &'a [MetadataColumn], path: &str) -> Vec<Option<&'a Value>> {
    columns
        .iter()
        .map(|column| {
            resolve_path(&column.data, path)
                .filter(|value| value.as_object().is_some_and(|object| object.is_empty()))
        })
        .collect()
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

/// A leaf's per-run values under the diff's notion of equality, as JSON text (serde_json maps are sorted; numbers stay as written, but for serde_json's spelling of an exponent), so a row groups in linear time. A run that ignores server timing compares as absent under `/meta/server_time`; a `/meta` leaf (some present `meta` is not an object) compares with its timing.
fn comparable_row(columns: &[MetadataColumn], path: &str) -> Vec<Option<String>> {
    let server_timing = path == "/meta/server_time" || path.starts_with("/meta/server_time/");
    columns
        .iter()
        .map(|column| {
            if server_timing && column.ignore_server_timing_in_diff {
                None
            } else {
                resolve_path(&column.data, path).map(Value::to_string)
            }
        })
        .collect()
}

/// Only strings receive display-only quoting.
fn copy_payload<'a>(value: &'a Value, display: &'a str) -> &'a str {
    value.as_str().unwrap_or(display)
}

/// Serialize non-string JSON values, and show a string as its text unless `shows_quoted`.
fn format_value(v: &Value) -> String {
    match v {
        Value::String(s) if !shows_quoted(s) => s.clone(),
        _ => v.to_string(),
    }
}

/// A string shows JSON-quoted when its text parses as JSON as Python's json reads it (NaN and ±Infinity allowed), is empty, or has leading or trailing whitespace, a double space or a control character. A non-string's display parses so and an unquoted string's never does, and a quoted display is always a string literal, so distinct values never share a display.
fn shows_quoted(text: &str) -> bool {
    text.is_empty()
        || text.trim() != text
        || text.contains("  ")
        || text.contains(char::is_control)
        // A JSON value's first character, so a string that can't be JSON skips the parse.
        || (text.starts_with(|c| "{[\"-0123456789tfnNI".contains(c))
            && crate::components::cdn_gallery::parses_as_python_json(text))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dioxus::dioxus_core::{AttributeValue, Mutation};
    use serde_json::json;

    fn columns(runs: Vec<(impl Into<String>, Value)>) -> Vec<MetadataColumn> {
        runs.into_iter()
            .map(|(label, data)| {
                let label = label.into();
                MetadataColumn {
                    color: format!("#{label}"),
                    label,
                    data,
                    ignore_server_timing_in_diff: true,
                }
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
                "one",
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
                "two",
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
        let keys = column_keys(&runs);

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
                "one",
                json!({
                    "a.b": 1,
                    "a": {"b": 7},
                    "meta.server_time": "one",
                    "meta": {"server_time": {"end": "one"}}
                }),
            ),
            (
                "two",
                json!({
                    "a.b": 2,
                    "a": {"b": 7},
                    "meta.server_time": "two",
                    "meta": {"server_time": {"end": "two"}}
                }),
            ),
        ]);
        let keys = column_keys(&runs);

        assert_eq!(
            filter_diff_only(&keys, &runs, ""),
            vec![
                KeyNode::Leaf("a.b".to_string()),
                KeyNode::Leaf("meta.server_time".to_string()),
            ]
        );
    }

    #[test]
    fn mixed_and_empty_object_shapes_render_losslessly() {
        let runs = columns(vec![
            (
                "0",
                json!({
                    "array": {"x": 1},
                    "both_empty": {},
                    "empty": {},
                    "optimizer": "adam",
                    "optional": {"x": 1}
                }),
            ),
            (
                "1",
                json!({
                    "array": [],
                    "both_empty": {},
                    "empty": {"x": 1},
                    "optimizer": {"lr": 0.001},
                    "optional": {"y": 2}
                }),
            ),
            ("2", json!({})),
        ]);
        let keys = column_keys(&runs);

        assert_eq!(
            keys,
            vec![
                KeyNode::Leaf("array".to_string()),
                KeyNode::Leaf("both_empty".to_string()),
                KeyNode::Branch("empty".to_string(), vec![KeyNode::Leaf("x".to_string())]),
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
            runs.iter()
                .map(|run| resolve_path(&run.data, "/optimizer").map(format_value))
                .collect::<Vec<_>>(),
            vec![
                Some("adam".to_string()),
                Some("{\"lr\":0.001}".to_string()),
                None,
            ]
        );
        assert_eq!(filter_diff_only(&keys, &runs, ""), keys);
    }

    #[test]
    fn server_timing_beside_an_empty_meta_is_no_difference() {
        let timing_only = columns(vec![
            ("one", json!({"meta": {}})),
            ("two", json!({"meta": {"server_time": {"end": "two"}}})),
        ]);
        let timing_keys = column_keys(&timing_only);
        assert!(filter_diff_only(&timing_keys, &timing_only, "").is_empty());
        // A documented limit: an empty meta hides beside timing alone, even against a run without meta.
        let mut with_absent = timing_only.clone();
        with_absent.extend(columns(vec![("absent", json!({}))]));
        assert!(filter_diff_only(&timing_keys, &with_absent, "").is_empty());

        let client_difference = columns(vec![
            ("one", json!({"meta": {}})),
            (
                "two",
                json!({"meta": {"client": 2, "server_time": {"end": "two"}}}),
            ),
        ]);
        let client_keys = column_keys(&client_difference);
        assert_eq!(
            filter_diff_only(&client_keys, &client_difference, ""),
            vec![KeyNode::Branch(
                "meta".to_string(),
                vec![KeyNode::Leaf("client".to_string())]
            )]
        );
    }

    #[test]
    fn a_meta_leaf_compares_with_its_server_timing() {
        // A user-authored info/run_info can have a non-object meta, which keeps /meta a leaf: objects that differ by their timing alone still tint.
        let runs = columns(vec![
            ("none", json!({"meta": null})),
            ("one", json!({"meta": {"server_time": {"end": "one"}}})),
            ("two", json!({"meta": {"server_time": {"end": "two"}}})),
        ]);
        let keys = column_keys(&runs);
        assert_eq!(keys, vec![KeyNode::Leaf("meta".to_string())]);
        assert_eq!(
            difference_tints(&comparable_row(&runs, "/meta")),
            vec![true, true, true]
        );
    }

    /// What a table renders, in document order: each tinted cell's `background` style, and each copyable value's `title`.
    fn rendered(columns: Vec<MetadataColumn>, diff_only: bool) -> Vec<String> {
        shown(VirtualDom::new_with_props(
            MetadataViewer,
            MetadataViewerProps { columns, diff_only },
        ))
    }

    /// What a VirtualDom's first render shows, as `rendered` reads a table.
    fn shown(mut dom: VirtualDom) -> Vec<String> {
        let mut shown = Vec::new();
        for edit in dom.rebuild_to_vec().edits {
            if let Mutation::SetAttribute {
                name,
                value: AttributeValue::Text(value),
                ..
            } = edit
            {
                if name == "title" || (name == "style" && value.starts_with("background")) {
                    shown.push(value);
                }
            }
        }
        shown
    }

    #[test]
    fn an_empty_object_shows_on_its_branch_row() {
        let runs = columns(vec![
            ("empty", json!({"config": {}})),
            ("full", json!({"config": {"lr": 1, "kw": {}}})),
            ("absent", json!({"other": 2})),
        ]);
        let tied = columns(vec![
            ("e1", json!({"config": {}})),
            ("e2", json!({"config": {}})),
            ("full", json!({"config": {"lr": 1}})),
            ("absent", json!({})),
        ]);
        for diff_only in [false, true] {
            // The empty run's tinted `{}` on the config row, then the full run's kw and lr and the absent run's other, each tinted alone on its row.
            assert_eq!(
                rendered(runs.clone(), diff_only),
                [
                    "background: #empty22;",
                    "{}",
                    "background: #full22;",
                    "{}",
                    "background: #full22;",
                    "1",
                    "background: #absent22;",
                    "2"
                ]
            );
            // Two `{}` beside two blanks tie on the config row, so every cell there is tinted and the `{}` stay in the e1 and e2 columns, then the full run's lr.
            assert_eq!(
                rendered(tied.clone(), diff_only),
                [
                    "background: #e122;",
                    "{}",
                    "background: #e222;",
                    "{}",
                    "background: #full22;",
                    "background: #absent22;",
                    "background: #full22;",
                    "1"
                ]
            );
        }
        // Collapsed, the config row still shows the empty run's tinted `{}`; its children are hidden, and the absent run's other follows.
        let panel = |runs: Vec<MetadataColumn>| {
            let collapsed = use_signal(|| BTreeSet::from(["/config".to_string()]));
            render_rows(&column_keys(&runs), &runs, "", 0, collapsed)
        };
        assert_eq!(
            shown(VirtualDom::new_with_props(panel, runs)),
            ["background: #empty22;", "{}", "background: #absent22;", "2"]
        );
    }

    /// The edits that re-rendering a table in place makes when its runs or diff-only mode change from before to after.
    fn edits_on_update(
        before: (Vec<(&'static str, Value)>, bool),
        after: (Vec<(&'static str, Value)>, bool),
    ) -> Vec<Mutation> {
        thread_local! {
            static TABLE: std::cell::RefCell<(Vec<(&'static str, Value)>, bool)> = const { std::cell::RefCell::new((Vec::new(), false)) };
        }
        fn panel() -> Element {
            let (runs, diff_only) = TABLE.with_borrow(Clone::clone);
            rsx! { MetadataViewer { columns: columns(runs), diff_only } }
        }
        TABLE.set(before);
        let mut dom = VirtualDom::new(panel);
        dom.rebuild_in_place();
        TABLE.set(after);
        dom.mark_dirty(ScopeId::APP);
        dom.render_immediate_to_vec().edits
    }

    #[test]
    fn a_cell_that_loses_its_tint_drops_its_style() {
        // Dioxus keeps the old properties when it sets a style, so `style: ""` would leave the stale background: the style is removed instead.
        let branch = vec![
            ("one", json!({"config": {}})),
            ("two", json!({"config": {"lr": 1}})),
            ("three", json!({"config": {}})),
        ];
        let rows = vec![
            ("one", json!({"a": 1, "b": 1})),
            ("two", json!({"a": 1, "b": 2})),
        ];
        for (before, after) in [
            // The third run makes `{}` the config row's plurality and a blank the lr row's, so run one's `{}` and its blank lr cell lose their tints.
            ((branch[..2].to_vec(), false), (branch, false)),
            // Turning diff-only off, as the sidebar does, puts the equal a in the row where the tinted b was.
            ((rows.clone(), true), (rows, false)),
        ] {
            let styles: Vec<_> = edits_on_update(before, after)
                .into_iter()
                .filter_map(|edit| match edit {
                    Mutation::SetAttribute {
                        name: "style",
                        value,
                        ..
                    } => Some(value),
                    _ => None,
                })
                .collect();
            assert!(
                styles
                    .iter()
                    .any(|value| matches!(value, AttributeValue::None)),
                "{styles:?}"
            );
            assert!(
                !styles.iter().any(|value| matches!(
                    value,
                    AttributeValue::Text(text) if text.trim().is_empty()
                )),
                "{styles:?}"
            );
        }
    }

    #[test]
    fn custom_metadata_keeps_server_time_differences() {
        for (shape, one, two) in [
            (
                "objects",
                json!({"source": "one"}),
                json!({"source": "two"}),
            ),
            ("scalars", json!("one"), json!("two")),
        ] {
            let mut custom = columns(vec![
                ("one", json!({"meta": {"server_time": one}})),
                ("two", json!({"meta": {"server_time": two}})),
            ]);
            for column in &mut custom {
                column.ignore_server_timing_in_diff = false;
            }
            let keys = column_keys(&custom);
            assert_eq!(filter_diff_only(&keys, &custom, ""), keys, "{shape}");
        }
    }

    #[test]
    fn metadata_comparison_preserves_json_types() {
        let runs = columns(vec![
            (
                "one",
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
                "two",
                json!({
                    "array": "[1]",
                    "boolean": "true",
                    "number": "1",
                    "object": "{\"a\":1}",
                    "same": {"typed": true}
                }),
            ),
        ]);
        let keys = column_keys(&runs);

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

    /// Numbers serde_json's default f64 parse loses: 10^400 and int(float max) fail it; 2^256 prints back rounded.
    fn numbers_beyond_f64() -> [String; 3] {
        [
            format!("1{}", "0".repeat(400)),
            format!("{:.0}", f64::MAX),
            "115792089237316195423570985008687907853269984665640564039457584007913129639936"
                .to_string(),
        ]
    }

    #[test]
    fn numbers_render_and_copy_as_logged() {
        for logged in numbers_beyond_f64() {
            let data: Value = serde_json::from_str(&format!(r#"{{"n": {logged}}}"#)).unwrap();
            let display = format_value(&data["n"]);
            assert_eq!(display, logged);
            assert_eq!(copy_payload(&data["n"], &display), logged);
        }
    }

    #[test]
    fn numbers_keep_the_text_json_dumps_writes() {
        // Python's json.dumps writes floats by repr, ints by str. serde_json's default parse would read 198136.40638684994 one ulp off, showing 198136.4063868499. An integer -0, which json.dumps never writes, keeps its sign too.
        for written in [
            "1e-08",
            "1e+30",
            "-0.0",
            "-0",
            "100000.0",
            "198136.40638684994",
        ] {
            let data: Value = serde_json::from_str(&format!(r#"{{"n": {written}}}"#)).unwrap();
            assert_eq!(format_value(&data["n"]), written);
        }
    }

    #[test]
    fn numbers_compare_as_logged() {
        // Each pair differs only in its last digit, below f64's precision.
        for logged in numbers_beyond_f64() {
            let neighbour = format!("{}1", &logged[..logged.len() - 1]);
            let runs = columns(
                [logged, neighbour]
                    .into_iter()
                    .map(|n| {
                        let data = serde_json::from_str(&format!(r#"{{"n": {n}}}"#)).unwrap();
                        (n, data)
                    })
                    .collect(),
            );
            let keys = column_keys(&runs);
            assert_eq!(
                filter_diff_only(&keys, &runs, ""),
                vec![KeyNode::Leaf("n".to_string())]
            );
            assert_eq!(
                difference_tints(&comparable_row(&runs, "/n")),
                vec![true, true]
            );
        }
    }

    /// `data` as cdn_gallery reads it from a manifest Python's json wrote, NaN and ±Infinity included.
    fn python_json(data: &str) -> Value {
        crate::components::cdn_gallery::parse_manifest(&format!(
            r#"{{"class": "metadata", "data": {data}}}"#
        ))
        .expect("readable")
        .data
        .unwrap_or_default()
    }

    /// Every non-string display, tokens and compact containers included, makes a string with the same text show quoted.
    #[test]
    fn no_string_displays_like_a_non_string() {
        for data in [
            "[NaN]",
            "[NaN, 1]",
            r#"{"a": NaN}"#,
            r#"{"a": [Infinity, -Infinity]}"#,
            "NaN",
            "0",
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "-1",
            "2.5",
            "null",
            "false",
            "[]",
            "{}",
        ] {
            let display = format_value(&python_json(data));
            assert_ne!(format_value(&json!(display)), display, "{display}");
        }
    }

    #[test]
    fn strings_that_read_as_json_or_hide_whitespace_show_quoted() {
        let boolean = json!(true);
        let boolean_string = json!("true");
        let displays = [&boolean, &boolean_string].map(format_value);
        assert_eq!(displays, ["true", "\"true\""]);
        assert_eq!(copy_payload(&boolean_string, &displays[1]), "true");
        assert_eq!(copy_payload(&boolean, &displays[0]), displays[0]);
        assert_eq!(format_value(&json!(false)), "false");

        for text in [
            "1",
            "1e-4",
            "-3",
            "0.1",
            "null",
            "false",
            "[1, 2]",
            "{}",
            r#"{"a":1}"#,
            "NaN",
            "Infinity",
            "-Infinity",
            "[1, NaN]",
            "\"\\ud800\"",
            "\"true\"",
            // A number spelled as a placeholder, which a strict manifest can hold.
            "1e+77777777777777777777",
            // Empty, or whitespace or control characters the bare text would hide.
            "",
            " lead",
            "trail ",
            "   ",
            "double  space",
            "tab\there",
            "line\nbreak",
            "\r",
            "nul\u{0}",
            "del\u{7f}",
            "\u{a0}nbsp",
        ] {
            let string = json!(text);
            assert_eq!(
                format_value(&string),
                serde_json::to_string(text).unwrap(),
                "{text:?}"
            );
            assert_eq!(copy_payload(&string, &format_value(&string)), text);
        }
        for text in [
            "adam",
            "nan",
            "inf",
            "-NaN",
            "Infinity0",
            "1e",
            "1 2",
            "a b",
            "/path/to file",
            "true story",
            "\\ud800",
            "one space",
            "zero\u{200b}width",
            "caf\u{e9}",
            "?",
        ] {
            assert_eq!(format_value(&json!(text)), text, "{text:?}");
        }
    }

    #[test]
    fn the_number_nan_and_the_string_nan_show_and_diff_apart() {
        let runs = columns(vec![
            ("one", python_json(r#"{"loss": NaN, "best": -Infinity}"#)),
            ("two", json!({"loss": "NaN", "best": "-Infinity"})),
            ("three", python_json(r#"{"loss": NaN, "best": Infinity}"#)),
        ]);
        let displays = |path| {
            runs.iter()
                .map(|column| format_value(resolve_path(&column.data, path).unwrap()))
                .collect::<Vec<_>>()
        };
        assert_eq!(displays("/loss"), vec!["NaN", "\"NaN\"", "NaN"]);
        assert_eq!(
            displays("/best"),
            vec!["-Infinity", "\"-Infinity\"", "Infinity"]
        );
        // NaN equals NaN in the diff, and never the string.
        assert_eq!(
            difference_tints(&comparable_row(&runs, "/loss")),
            vec![false, true, false]
        );
        assert_eq!(
            difference_tints(&comparable_row(&runs, "/best")),
            vec![true, true, true]
        );
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
    fn comparable_rows_ignore_object_key_order() {
        let runs = columns(vec![
            ("one", json!({"opt": {"a": 1, "b": 2}})),
            ("two", json!({"opt": {"b": 2, "a": 1}})),
        ]);
        assert_eq!(
            difference_tints(&comparable_row(&runs, "/opt")),
            vec![false, false]
        );
    }

    #[test]
    fn diff_only_treats_missing_as_different_from_any_present_value() {
        let runs = columns(vec![
            ("one", json!({"empty": "", "value": 1})),
            ("two", json!({})),
        ]);
        let keys = column_keys(&runs);

        assert_eq!(
            filter_diff_only(&keys, &runs, ""),
            vec![
                KeyNode::Leaf("empty".to_string()),
                KeyNode::Leaf("value".to_string()),
            ]
        );
    }
}
