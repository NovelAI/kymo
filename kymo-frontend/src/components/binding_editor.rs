use std::collections::{HashMap, HashSet};

use dioxus::prelude::*;

use crate::components::editor_dialog::EditorDialog;
use crate::components::metric_rect::build_view_context;
use crate::components::options_editor::{AxisFields, EditorSection, SmoothingFields};
use crate::components::sidebar::{sidebar_needs_run_ordinals, sidebar_run_label};
use crate::grpc::proto::{metric_info::MetricType, MetricInfo, RunInfo};
use crate::state::layout_config::{
    resolve_all_bindings, CdnDisplayMode, DisplayType, MetricBinding, ProjectRef, RectOptions,
    RunRef, ViewContext,
};
use crate::util::{primary, use_live_apply};

type XMetricSource = (String, String);
type XMetricResult = (Option<XMetricSource>, Vec<String>);

fn x_metric_discovery_source(
    bindings: &[MetricBinding],
    context: &ViewContext,
) -> Option<XMetricSource> {
    resolve_all_bindings(bindings, context)
        .into_iter()
        .next()
        .map(|resolved| (resolved.project_id, resolved.run_id))
}

fn x_metric_names(metrics: Vec<MetricInfo>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut names = Vec::new();
    for metric in metrics {
        if metric.metric_type == MetricType::Numeric as i32
            && seen.insert(metric.metric_name.clone())
        {
            names.push(metric.metric_name);
        }
    }
    names
}

fn matching_x_metric_names(
    source: Option<&XMetricSource>,
    result: Option<&XMetricResult>,
) -> Vec<String> {
    result
        .filter(|(fetched_source, _)| fetched_source.as_ref() == source)
        .map(|(_, names)| names.clone())
        .unwrap_or_default()
}

fn binding_control_label(index: usize, control: &str) -> String {
    format!("Source {} {control}", index + 1)
}

const CURRENT_PROJECT_VALUE: &str = "mode:current";
const SELECTED_RUNS_VALUE: &str = "mode:selected";
const ALL_RUNS_VALUE: &str = "mode:all";
const SPECIFIC_RUNS_VALUE: &str = "mode:specific";

fn id_select_value(id: &str) -> String {
    format!("id:{id}")
}

fn project_select_value(project: &ProjectRef) -> String {
    match project {
        ProjectRef::Current => CURRENT_PROJECT_VALUE.to_string(),
        ProjectRef::Specific(project_id) => id_select_value(project_id),
    }
}

fn parse_project_select_value(value: &str) -> Option<ProjectRef> {
    if value == CURRENT_PROJECT_VALUE {
        Some(ProjectRef::Current)
    } else {
        value
            .strip_prefix("id:")
            .map(|id| ProjectRef::Specific(id.to_string()))
    }
}

/// The view context holds only the current project's runs, so another project's are always a specific list.
fn runs_select_value(project: &ProjectRef, runs: &RunRef) -> &'static str {
    match (project, runs) {
        (ProjectRef::Current, RunRef::Selected) => SELECTED_RUNS_VALUE,
        (ProjectRef::Current, RunRef::All) => ALL_RUNS_VALUE,
        _ => SPECIFIC_RUNS_VALUE,
    }
}

fn parse_runs_select_value(value: &str, seed: Vec<String>) -> Option<RunRef> {
    match value {
        SELECTED_RUNS_VALUE => Some(RunRef::Selected),
        ALL_RUNS_VALUE => Some(RunRef::All),
        SPECIFIC_RUNS_VALUE => Some(RunRef::Specific(seed)),
        _ => None,
    }
}

fn runs_for_project_change(project: &ProjectRef) -> RunRef {
    match project {
        ProjectRef::Current => RunRef::Selected,
        ProjectRef::Specific(_) => RunRef::Specific(Vec::new()),
    }
}

fn checked_run_ids(runs: &RunRef) -> &[String] {
    match runs {
        RunRef::Specific(run_ids) => run_ids,
        RunRef::Selected | RunRef::All => &[],
    }
}

/// Checking appends, so the stored order (which `cap_runs` keeps) is pick order.
fn toggle_specific_run(runs: &RunRef, run_id: &str, checked: bool) -> RunRef {
    let mut run_ids = checked_run_ids(runs).to_vec();
    run_ids.retain(|id| id != run_id);
    if checked {
        run_ids.push(run_id.to_string());
    }
    RunRef::Specific(run_ids)
}

/// Keeps checked runs missing from the project's run list (trashed or gone) visible, labeled by id, so they can be unchecked.
fn specific_run_choices(checked: &[String], catalog: &[RunInfo]) -> Vec<(String, String)> {
    let mut listed: HashSet<&str> = catalog.iter().map(|run| run.run_id.as_str()).collect();
    let ordinals = sidebar_needs_run_ordinals(catalog);
    checked
        .iter()
        .filter(|id| listed.insert(id.as_str()))
        .map(|id| (id.clone(), id.clone()))
        .chain(
            catalog
                .iter()
                .map(|run| (run.run_id.clone(), sidebar_run_label(run, ordinals))),
        )
        .collect()
}

/// Name-sorted (`catalog_kind` binary-searches it), one type per name.
type MetricCatalog = Vec<(String, DisplayType)>;

/// By row id, so each row can gate on its siblings' chosen types.
type RowCatalogs = HashMap<u64, MetricCatalog>;

const KIND_ORDER: [DisplayType; 3] = [
    DisplayType::Numeric,
    DisplayType::Cdn,
    DisplayType::TextStream,
];

fn kind_label(kind: DisplayType) -> &'static str {
    match kind {
        DisplayType::Numeric => "Numeric",
        DisplayType::Cdn => "Media",
        DisplayType::TextStream => "Text logs",
    }
}

/// Match the server's TEXT_STREAM > NUMERIC > CDN precedence for names found in several runs.
fn metric_catalog(metrics: Vec<MetricInfo>) -> MetricCatalog {
    let precedence = |kind: DisplayType| match kind {
        DisplayType::TextStream => 2,
        DisplayType::Numeric => 1,
        DisplayType::Cdn => 0,
    };
    let mut catalog: MetricCatalog = metrics
        .into_iter()
        .map(|metric| {
            let kind = DisplayType::for_metric(&metric);
            (metric.metric_name, kind)
        })
        .collect();
    catalog.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| precedence(b.1).cmp(&precedence(a.1)))
    });
    catalog.dedup_by(|later, earlier| later.0 == earlier.0);
    catalog
}

fn catalog_kind(catalog: &[(String, DisplayType)], name: &str) -> Option<DisplayType> {
    catalog
        .binary_search_by(|(listed, _)| listed.as_str().cmp(name))
        .ok()
        .map(|found| catalog[found].1)
}

/// Keeps `current` listed even when filtered out; the count covers filter matches only.
fn metric_groups<'a>(
    catalog: &'a [(String, DisplayType)],
    filter: &str,
    current: &str,
) -> (Vec<(DisplayType, Vec<&'a str>)>, usize) {
    let needle = filter.trim().to_lowercase();
    let mut matched = 0;
    let listed: Vec<&(String, DisplayType)> = catalog
        .iter()
        .filter(|(name, _)| {
            let hit = needle.is_empty() || name.to_lowercase().contains(&needle);
            matched += usize::from(hit);
            hit || name == current
        })
        .collect();
    let groups = KIND_ORDER
        .into_iter()
        .filter_map(|kind| {
            let names: Vec<&str> = listed
                .iter()
                .filter(|(_, listed_kind)| *listed_kind == kind)
                .map(|(name, _)| name.as_str())
                .collect();
            (!names.is_empty()).then_some((kind, names))
        })
        .collect();
    (groups, matched)
}

/// Constrain a row only when its siblings' known types agree.
fn required_kind(
    bindings: &[MetricBinding],
    row_ids: &[u64],
    catalogs: &RowCatalogs,
    index: usize,
) -> Option<DisplayType> {
    let mut kinds = bindings
        .iter()
        .zip(row_ids)
        .enumerate()
        .filter(|(i, _)| *i != index)
        .filter_map(|(_, (binding, row_id))| {
            catalog_kind(catalogs.get(row_id)?, &binding.metric_name)
        });
    let first = kinds.next()?;
    kinds.all(|kind| kind == first).then_some(first)
}

/// (gallery, metadata) panels; an unknown class keeps the gallery panel so an editor opened before the manifest loads can still set its mode.
fn cdn_option_panels(cdn_class: Option<&str>) -> (bool, bool) {
    (
        matches!(cdn_class, None | Some("image_gallery")),
        matches!(cdn_class, Some("metadata")),
    )
}

/// `None` until the runs of `effective_project` have loaded.
fn matching_project_runs<'a>(
    effective_project: &str,
    result: Option<&'a (String, Vec<RunInfo>)>,
) -> Option<&'a [RunInfo]> {
    result
        .filter(|(project, _)| project == effective_project)
        .map(|(_, runs)| runs.as_slice())
}

/// An empty specific-run list discovers across the project so metrics stay pickable.
fn metric_discovery_run_ids(
    binding_runs: &RunRef,
    effective_project: &str,
    fetched_runs: Option<&(String, Vec<RunInfo>)>,
) -> Vec<String> {
    match binding_runs {
        RunRef::Specific(run_ids) if !run_ids.is_empty() => run_ids.clone(),
        _ => matching_project_runs(effective_project, fetched_runs)
            .unwrap_or_default()
            .iter()
            .map(|run| run.run_id.clone())
            .collect(),
    }
}

fn matching_metric_catalog<'a>(
    effective_project: &str,
    discovery_run_ids: &[String],
    result: Option<&'a (String, Vec<String>, MetricCatalog)>,
) -> &'a [(String, DisplayType)] {
    result
        .filter(|(project, run_ids, _)| {
            project == effective_project && run_ids == discovery_run_ids
        })
        .map_or(&[], |(_, _, catalog)| catalog.as_slice())
}

#[component]
pub fn BindingEditor(
    return_focus_id: String,
    bindings: Vec<MetricBinding>,
    options: RectOptions,
    /// The cascade resolution at this rect's section; draft fields differing from it (chart-level overrides) are highlighted.
    anchor: RectOptions,
    display_type: DisplayType,
    /// The gallery's resolved CDN sub-type ("image_gallery", "metadata", "file_list" or "mixed"); None until its manifest loads.
    #[props(default)]
    cdn_class: Option<String>,
    /// Section's max columns — clamps the Width input.
    max_columns: u32,
    /// Fired on every draft edit (slider ticks included) so the rect applies it live; Cancel re-fires it with the config captured at open.
    on_change: EventHandler<(Vec<MetricBinding>, RectOptions)>,
    /// Also commits mount-time normalization (such as the width clamp), which live apply never emits.
    on_save: EventHandler<(Vec<MetricBinding>, RectOptions)>,
    on_cancel: EventHandler<()>,
) -> Element {
    let id_prefix = format!("{return_focus_id}-dialog");
    let is_numeric = matches!(display_type, DisplayType::Numeric);
    let is_cdn = matches!(display_type, DisplayType::Cdn);
    let (class_has_gallery_panel, class_has_metadata_panel) =
        cdn_option_panels(cdn_class.as_deref());
    let show_gallery_panel = is_cdn && class_has_gallery_panel;
    let show_metadata_panel = is_cdn && class_has_metadata_panel;
    let plots_open = use_signal(|| is_numeric);
    let smoothing_open = use_signal(|| is_numeric);
    let gallery_open = use_signal(|| show_gallery_panel);
    let metadata_open = use_signal(|| show_metadata_panel);
    let mut draft = use_signal(|| bindings.clone());
    // One draft of the whole struct, so options without a control here pass through untouched.
    let mut draft_opts = use_signal(|| RectOptions {
        column_span: options.column_span.clamp(1, max_columns),
        ..options.clone()
    });

    let original = use_hook(|| (bindings.clone(), options.clone()));

    let current_config = move || (draft.read().clone(), draft_opts.read().clone());

    let cancel_live = use_live_apply(
        original.clone(),
        current_config,
        move |current| on_change.call(current),
        move || on_cancel.call(()),
    );

    // Stable ids keep row-local state with its source across removals.
    let mut row_ids = use_signal(|| (0..bindings.len() as u64).collect::<Vec<u64>>());
    let mut next_row_id = use_signal(|| bindings.len() as u64);
    let mut row_catalogs = use_signal(RowCatalogs::new);

    let state = use_context::<crate::state::DashboardState>();
    // One project list for every source row.
    let projects = use_resource(move || {
        let grpc = state.grpc.read().clone();
        async move {
            crate::state::visibility::retry_visible("binding projects", async || {
                grpc.list_projects().await
            })
            .await
        }
    });

    let x_metric_source =
        use_memo(move || x_metric_discovery_source(&draft.read(), &build_view_context(&state)));
    let current_x_metric_source = x_metric_source.read().clone();
    let x_metric_version = crate::state::versions_key(
        *state.resync_gen.read(),
        current_x_metric_source
            .iter()
            .map(|(_, run_id)| run_id.as_str()),
        &state.metrics_gen.read(),
    );
    let available_metrics = use_resource(use_reactive(
        (&current_x_metric_source, &x_metric_version),
        move |(source, _version)| {
            let grpc = state.grpc.read().clone();
            async move {
                // The chart reads the custom X metric from its first source's run, so offer only that run's metrics.
                let names = match &source {
                    Some((project_id, run_id)) => {
                        crate::state::visibility::retry_visible_run("X-axis metrics", async || {
                            grpc.list_metrics(project_id, run_id).await
                        })
                        .await
                        .map(x_metric_names)
                        .unwrap_or_default()
                    }
                    None => Vec::new(),
                };
                (source, names)
            }
        },
    ));

    rsx! {
        EditorDialog {
            return_focus_id,
            title: "Configure Metric",
            on_cancel: move |_| cancel_live(),
            on_save: move |_| on_save.call(current_config()),

            // --- Bindings section ---
            div { class: "editor-section-label", "Data Sources" }

            div { style: "margin-bottom: 12px;",
                for (i, (binding, row_id)) in draft.read().iter().zip(row_ids.read().iter().copied()).enumerate() {
                    BindingRow {
                        key: "{row_id}",
                        id_prefix: format!("{id_prefix}-source-{i}"),
                        index: i,
                        binding: binding.clone(),
                        required_kind: required_kind(&draft.read(), &row_ids.read(), &row_catalogs.read(), i),
                        row_id,
                        row_catalogs,
                        projects,
                        on_change: move |new_binding: MetricBinding| {
                            let Some(i) = row_ids.peek().iter().position(|id| *id == row_id) else {
                                return;
                            };
                            if let Some(slot) = draft.write().get_mut(i) {
                                *slot = new_binding;
                            }
                        },
                        on_remove: move |_: ()| {
                            let Some(i) = row_ids.peek().iter().position(|id| *id == row_id) else {
                                return;
                            };
                            row_ids.write().remove(i);
                            draft.write().remove(i);
                            row_catalogs.write().remove(&row_id);
                        },
                    }
                }
            }

            button {
                class: "btn-link",
                onmousedown: primary(move |_| {
                    draft.write().push(MetricBinding {
                        project: ProjectRef::Current,
                        runs: RunRef::Selected,
                        metric_name: String::new(),
                    });
                    let row_id = *next_row_id.peek();
                    next_row_id.set(row_id + 1);
                    row_ids.write().push(row_id);
                }),
                "+ Add Source"
            }

            if is_numeric {
                EditorSection {
                    title: "Plots",
                    open: plots_open,

                    div { class: "editor-options",
                        AxisFields { id_prefix: id_prefix.clone(), draft: draft_opts, anchor: anchor.clone() }
                        div { class: "binding-field",
                            label { r#for: "{id_prefix}-x-axis", "X axis" }
                            {
                                use crate::state::layout_config::XAxisMode;
                                let mode_val = match &draft_opts.read().x_axis_mode {
                                    XAxisMode::Step => "step",
                                    XAxisMode::RelativeTime => "relative",
                                    XAxisMode::WallTime => "wall",
                                };
                                rsx! {
                                    select {
                                        id: "{id_prefix}-x-axis",
                                        value: "{mode_val}",
                                        onchange: move |e: Event<FormData>| {
                                            let mode = match e.value().as_str() {
                                                "relative" => XAxisMode::RelativeTime,
                                                "wall" => XAxisMode::WallTime,
                                                _ => XAxisMode::Step,
                                            };
                                            draft_opts.write().x_axis_mode = mode;
                                        },
                                        option { value: "step", "Step" }
                                        option { value: "relative", "Relative time" }
                                        option { value: "wall", "Wall time" }
                                    }
                                }
                            }
                        }
                        if matches!(draft_opts.read().x_axis_mode, crate::state::layout_config::XAxisMode::Step) {
                            div { class: "binding-field",
                                label { r#for: "{id_prefix}-x-metric", "X metric" }
                                {
                                    let x_val = draft_opts.read().x_axis_metric.clone();
                                    let mut opts = matching_x_metric_names(
                                        current_x_metric_source.as_ref(),
                                        available_metrics.read().as_ref(),
                                    );
                                    if !x_val.is_empty() && !opts.contains(&x_val) {
                                        opts.insert(0, x_val.clone());
                                    }
                                    rsx! {
                                        select {
                                            id: "{id_prefix}-x-metric",
                                            value: "{x_val}",
                                            onchange: move |e: Event<FormData>| {
                                                draft_opts.write().x_axis_metric = e.value();
                                            },
                                            option { value: "", "Step (default)" }
                                            // `selected` pins the saved metric, as for the source's Project select.
                                            for m in opts {
                                                option { value: "{m}", selected: m == x_val, "{m}" }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                EditorSection {
                    title: "Smoothing",
                    open: smoothing_open,

                    div { class: "editor-options",
                        SmoothingFields {
                            id_prefix: id_prefix.clone(),
                            draft: draft_opts,
                            anchor: anchor.clone(),
                        }
                    }
                }
            }

            if show_gallery_panel {
                EditorSection {
                    title: "Image Gallery",
                    open: gallery_open,

                    div { class: "editor-options",
                        div { class: "binding-field",
                            label { r#for: "{id_prefix}-mode", "Mode" }
                            {
                                let cdn_mode_value = match &draft_opts.read().cdn_display_mode {
                                    CdnDisplayMode::SelectIndex => "select_index",
                                    CdnDisplayMode::GroupByRun => "group_by_run",
                                    CdnDisplayMode::Interleaved => "interleaved",
                                };
                                rsx! {
                                    select {
                                        id: "{id_prefix}-mode",
                                        value: "{cdn_mode_value}",
                                        onchange: move |e: Event<FormData>| {
                                            let m = match e.value().as_str() {
                                                "group_by_run" => CdnDisplayMode::GroupByRun,
                                                "interleaved" => CdnDisplayMode::Interleaved,
                                                _ => CdnDisplayMode::SelectIndex,
                                            };
                                            draft_opts.write().cdn_display_mode = m;
                                        },
                                        option { value: "select_index", "Select index" }
                                        option { value: "group_by_run", "Group by run" }
                                        option { value: "interleaved", "Interleaved" }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if show_metadata_panel {
                EditorSection {
                    title: "Metadata",
                    open: metadata_open,

                    div { class: "editor-options",
                        div { class: "binding-field",
                            label { class: "checkbox-label",
                                input {
                                    r#type: "checkbox",
                                    checked: draft_opts.read().metadata_diff_only,
                                    onchange: move |_| {
                                        draft_opts.with_mut(|o| o.metadata_diff_only = !o.metadata_diff_only);
                                    },
                                }
                                "Diff only (hide keys identical across runs)"
                            }
                        }
                    }
                }
            }

            // --- Appearance section ---
            div { class: "editor-section-label", "Appearance" }

            div { class: "editor-options",
                div { class: "binding-field",
                    label { r#for: "{id_prefix}-width", "Width" }
                    input {
                        id: "{id_prefix}-width",
                        r#type: "number",
                        min: "1",
                        max: "{max_columns}",
                        value: draft_opts.read().column_span.to_string(),
                        oninput: move |e: Event<FormData>| {
                            if let Ok(v) = e.value().parse::<u32>() {
                                draft_opts.write().column_span = v.clamp(1, max_columns);
                            }
                        },
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(current_run: Option<&str>) -> ViewContext {
        ViewContext {
            current_project: "current-project".into(),
            current_run: current_run.map(str::to_string),
            selected_runs: ["run-a".to_string()].into_iter().collect(),
            all_runs: vec!["run-a".into(), "run-b".into()],
        }
    }

    #[test]
    fn mixed_cdn_sources_expose_only_the_source_editor() {
        assert_eq!(cdn_option_panels(None), (true, false));
        assert_eq!(cdn_option_panels(Some("image_gallery")), (true, false));
        assert_eq!(cdn_option_panels(Some("file_list")), (false, false));
        assert_eq!(cdn_option_panels(Some("metadata")), (false, true));
        assert_eq!(cdn_option_panels(Some("mixed")), (false, false));
    }

    #[test]
    fn retained_run_options_never_cross_project_changes() {
        let result = (
            "project-a".to_string(),
            vec![RunInfo {
                project_id: "project-a".into(),
                run_id: "run-a".into(),
                run_name: "Run A".into(),
                ..Default::default()
            }],
        );

        assert_eq!(
            matching_project_runs("project-a", Some(&result)).map(<[_]>::len),
            Some(1)
        );
        assert_eq!(matching_project_runs("project-b", Some(&result)), None);
        assert_eq!(matching_project_runs("project-a", None), None);
    }

    #[test]
    fn repeated_binding_controls_have_source_specific_names() {
        assert_eq!(binding_control_label(0, "project"), "Source 1 project");
        assert_eq!(binding_control_label(1, "metric"), "Source 2 metric");
        assert_ne!(
            binding_control_label(0, "runs"),
            binding_control_label(1, "runs")
        );
    }

    #[test]
    fn metric_discovery_reuses_the_matching_run_catalog() {
        let fetched = (
            "project-a".to_string(),
            vec![
                RunInfo {
                    run_id: "run-a".into(),
                    ..Default::default()
                },
                RunInfo {
                    run_id: "run-b".into(),
                    ..Default::default()
                },
            ],
        );

        assert_eq!(
            metric_discovery_run_ids(&RunRef::Selected, "project-a", Some(&fetched)),
            ["run-a", "run-b"]
        );
        assert_eq!(
            metric_discovery_run_ids(&RunRef::All, "project-a", Some(&fetched)),
            ["run-a", "run-b"]
        );
        assert!(
            metric_discovery_run_ids(&RunRef::Selected, "project-b", Some(&fetched)).is_empty()
        );
        assert_eq!(
            metric_discovery_run_ids(
                &RunRef::Specific(vec!["saved-run".into()]),
                "project-b",
                None,
            ),
            ["saved-run"]
        );
        assert_eq!(
            metric_discovery_run_ids(&RunRef::Specific(Vec::new()), "project-a", Some(&fetched)),
            ["run-a", "run-b"]
        );
    }

    #[test]
    fn metric_discovery_versions_follow_only_target_runs() {
        let target_ids = ["run-a".to_string()];
        let mut versions =
            std::collections::HashMap::from([("run-a".to_string(), 4), ("run-b".to_string(), 8)]);
        let key = crate::state::versions_key(1, target_ids.iter().map(String::as_str), &versions);

        versions.insert("run-b".to_string(), 9);
        assert_eq!(
            crate::state::versions_key(1, target_ids.iter().map(String::as_str), &versions,),
            key
        );

        versions.insert("run-a".to_string(), 5);
        assert_ne!(
            crate::state::versions_key(1, target_ids.iter().map(String::as_str), &versions,),
            key
        );
        assert_ne!(
            crate::state::versions_key(2, target_ids.iter().map(String::as_str), &versions,),
            key
        );
        assert_ne!(
            crate::state::versions_key(1, ["run-a", "run-c"], &versions),
            key
        );
    }

    #[test]
    fn retained_metric_options_never_cross_source_changes() {
        let result = (
            "project-a".to_string(),
            vec!["run-a".to_string()],
            vec![("loss".to_string(), DisplayType::Numeric)],
        );

        assert_eq!(
            matching_metric_catalog("project-a", &["run-a".into()], Some(&result)),
            [("loss".to_string(), DisplayType::Numeric)]
        );
        assert!(matching_metric_catalog("project-b", &["run-a".into()], Some(&result)).is_empty());
        assert!(matching_metric_catalog("project-a", &["run-b".into()], Some(&result)).is_empty());
        assert!(matching_metric_catalog("project-a", &["run-a".into()], None).is_empty());
    }

    fn metric(name: &str, metric_type: MetricType) -> MetricInfo {
        MetricInfo {
            metric_name: name.into(),
            metric_type: metric_type as i32,
        }
    }

    #[test]
    fn metric_catalog_sorts_names_and_collapses_types_like_the_server() {
        let catalog = metric_catalog(vec![
            metric("train/loss", MetricType::Numeric),
            metric("samples", MetricType::Cdn),
            metric("samples", MetricType::Numeric),
            metric("logs/stdout", MetricType::Numeric),
            metric("logs/stdout", MetricType::TextStream),
            metric("logs/stdout", MetricType::Cdn),
            metric("gallery", MetricType::Cdn),
            MetricInfo {
                metric_name: "future_type".into(),
                metric_type: 999,
            },
            metric("eval/loss", MetricType::Numeric),
        ]);
        // Precedence TEXT_STREAM > NUMERIC > CDN, whatever order the per-run fallback merged them in.
        assert_eq!(
            catalog,
            [
                ("eval/loss".to_string(), DisplayType::Numeric),
                ("future_type".to_string(), DisplayType::Numeric),
                ("gallery".to_string(), DisplayType::Cdn),
                ("logs/stdout".to_string(), DisplayType::TextStream),
                ("samples".to_string(), DisplayType::Numeric),
                ("train/loss".to_string(), DisplayType::Numeric),
            ]
        );
        assert_eq!(catalog_kind(&catalog, "gallery"), Some(DisplayType::Cdn));
        assert_eq!(catalog_kind(&catalog, "missing"), None);
    }

    #[test]
    fn metric_groups_filter_by_substring_and_keep_the_current_metric() {
        let catalog = metric_catalog(vec![
            metric("eval/Loss", MetricType::Numeric),
            metric("logs/stdout", MetricType::TextStream),
            metric("samples/loss_curve", MetricType::Cdn),
            metric("train/loss", MetricType::Numeric),
            metric("train/lr", MetricType::Numeric),
        ]);

        let (groups, matched) = metric_groups(&catalog, "", "");
        assert_eq!(matched, 5);
        assert_eq!(
            groups,
            [
                (
                    DisplayType::Numeric,
                    vec!["eval/Loss", "train/loss", "train/lr"]
                ),
                (DisplayType::Cdn, vec!["samples/loss_curve"]),
                (DisplayType::TextStream, vec!["logs/stdout"]),
            ]
        );

        // Case-insensitive substring, not prefix; empty groups drop out.
        let (groups, matched) = metric_groups(&catalog, " LOSS ", "");
        assert_eq!(matched, 3);
        assert_eq!(
            groups,
            [
                (DisplayType::Numeric, vec!["eval/Loss", "train/loss"]),
                (DisplayType::Cdn, vec!["samples/loss_curve"]),
            ]
        );

        // The selected metric stays listed, in its own group, but is not a match.
        let (groups, matched) = metric_groups(&catalog, "stdout", "train/lr");
        assert_eq!(matched, 1);
        assert_eq!(
            groups,
            [
                (DisplayType::Numeric, vec!["train/lr"]),
                (DisplayType::TextStream, vec!["logs/stdout"]),
            ]
        );

        let (groups, matched) = metric_groups(&catalog, "nothing", "");
        assert_eq!((groups.len(), matched), (0, 0));
    }

    #[test]
    fn sources_must_match_their_siblings_chosen_types() {
        let catalog = |entries: &[(&str, DisplayType)]| -> MetricCatalog {
            entries
                .iter()
                .map(|&(name, kind)| (name.to_string(), kind))
                .collect()
        };
        let current_runs = catalog(&[
            ("loss", DisplayType::Numeric),
            ("lr", DisplayType::Numeric),
            ("samples", DisplayType::Cdn),
        ]);
        let catalogs: RowCatalogs = HashMap::from([
            (10, current_runs.clone()),
            (11, current_runs.clone()),
            (12, current_runs),
            // A source over other runs or another project types its names by its own discovery.
            (20, catalog(&[("loss", DisplayType::TextStream)])),
        ]);
        let source = |metric_name: &str| MetricBinding {
            project: ProjectRef::Current,
            runs: RunRef::Selected,
            metric_name: metric_name.into(),
        };
        let required = |sources: &[(u64, &str)], index| {
            let bindings: Vec<MetricBinding> =
                sources.iter().map(|(_, metric)| source(metric)).collect();
            let row_ids: Vec<u64> = sources.iter().map(|(row_id, _)| *row_id).collect();
            required_kind(&bindings, &row_ids, &catalogs, index)
        };

        // A lone source, or one whose siblings are empty, undiscovered, or not in their own catalog, may pick any type.
        assert_eq!(required(&[(10, "loss")], 0), None);
        assert_eq!(required(&[(10, "samples"), (11, "")], 0), None);
        assert_eq!(required(&[(10, "samples"), (30, "loss")], 0), None);
        assert_eq!(required(&[(10, "loss"), (11, "not-logged")], 0), None);
        // The source's own metric never constrains it.
        assert_eq!(
            required(&[(10, "samples"), (11, "loss"), (12, "lr")], 0),
            Some(DisplayType::Numeric)
        );
        assert_eq!(
            required(&[(10, "samples"), (11, "loss")], 1),
            Some(DisplayType::Cdn)
        );
        // Each sibling's type comes from its own catalog, never another row's.
        assert_eq!(
            required(&[(10, "lr"), (20, "loss")], 0),
            Some(DisplayType::TextStream)
        );
        assert_eq!(
            required(&[(20, "lr"), (10, "loss")], 0),
            Some(DisplayType::Numeric)
        );
        // Already-mixed siblings leave nothing to agree with.
        assert_eq!(
            required(&[(10, ""), (11, "loss"), (12, "samples")], 0),
            None
        );
    }

    #[test]
    fn specific_run_lists_hold_several_runs_in_pick_order() {
        let a = || "run-a".to_string();
        let b = || "run-b".to_string();
        let specific = toggle_specific_run(&RunRef::Selected, "run-b", true);
        assert_eq!(specific, RunRef::Specific(vec![b()]));
        let specific = toggle_specific_run(&specific, "run-a", true);
        assert_eq!(specific, RunRef::Specific(vec![b(), a()]));
        assert_eq!(
            toggle_specific_run(&specific, "run-b", false),
            RunRef::Specific(vec![a()])
        );
        assert_eq!(
            toggle_specific_run(&specific, "run-a", true),
            specific,
            "re-checking cannot duplicate a run"
        );

        let run = |run_id: &str, run_name: &str, ordinal| RunInfo {
            run_id: run_id.into(),
            run_name: run_name.into(),
            ordinal,
            ..Default::default()
        };
        let catalog = [run("run-a", "Run A", 2), run("run-b", "Run B", 1)];
        // Checked runs outside the active list lead, by id; the rest keep server order.
        assert_eq!(
            specific_run_choices(
                &["run-b".into(), "trashed".into(), "trashed".into()],
                &catalog
            ),
            [
                ("trashed".to_string(), "trashed".to_string()),
                ("run-a".to_string(), "Run A".to_string()),
                ("run-b".to_string(), "Run B".to_string()),
            ]
        );
        // Repeated names get the sidebar's ordinals.
        assert_eq!(
            specific_run_choices(&[], &[run("run-a", "sweep", 2), run("run-b", "sweep", 1)]),
            [
                ("run-a".to_string(), "sweep #2".to_string()),
                ("run-b".to_string(), "sweep #1".to_string()),
            ]
        );
    }

    #[test]
    fn x_metric_discovery_follows_the_first_resolved_chart_source() {
        let selected = MetricBinding {
            project: ProjectRef::Current,
            runs: RunRef::Selected,
            metric_name: "loss".into(),
        };
        assert_eq!(
            x_metric_discovery_source(&[selected], &context(Some("run-b"))),
            Some(("current-project".into(), "run-b".into()))
        );

        let specific = MetricBinding {
            project: ProjectRef::Specific("other-project".into()),
            runs: RunRef::Specific(vec!["run-c".into()]),
            metric_name: "accuracy".into(),
        };
        assert_eq!(
            x_metric_discovery_source(&[specific], &context(None)),
            Some(("other-project".into(), "run-c".into()))
        );
        assert_eq!(x_metric_discovery_source(&[], &context(None)), None);
    }

    #[test]
    fn retained_x_metric_options_never_cross_source_changes() {
        let result = (
            Some(("project-a".to_string(), "run-a".to_string())),
            vec!["epoch".to_string()],
        );

        assert_eq!(
            matching_x_metric_names(Some(&("project-a".into(), "run-a".into())), Some(&result),),
            ["epoch"]
        );
        assert!(matching_x_metric_names(
            Some(&("project-b".into(), "run-a".into())),
            Some(&result),
        )
        .is_empty());
        assert!(matching_x_metric_names(None, Some(&result)).is_empty());
        assert!(
            matching_x_metric_names(Some(&("project-a".into(), "run-a".into())), None,).is_empty()
        );
    }

    #[test]
    fn binding_select_values_do_not_reserve_valid_ids() {
        for project_id in ["__current__", CURRENT_PROJECT_VALUE, "id:project"] {
            let project = ProjectRef::Specific(project_id.into());
            assert_eq!(
                parse_project_select_value(&project_select_value(&project)),
                Some(project)
            );
        }
        // Run ids live in the checkbox list, never in the select's values.
        for run_ids in [
            vec![],
            vec!["run-a".to_string()],
            vec![SELECTED_RUNS_VALUE.to_string(), "id:run".to_string()],
        ] {
            let runs = RunRef::Specific(run_ids.clone());
            let value = runs_select_value(&ProjectRef::Current, &runs);
            assert_eq!(value, SPECIFIC_RUNS_VALUE);
            assert_eq!(parse_runs_select_value(value, run_ids), Some(runs));
        }
        assert_eq!(
            parse_project_select_value(CURRENT_PROJECT_VALUE),
            Some(ProjectRef::Current)
        );
        for runs in [RunRef::Selected, RunRef::All] {
            let value = runs_select_value(&ProjectRef::Current, &runs);
            assert_eq!(
                parse_runs_select_value(value, vec!["seed".into()]),
                Some(runs)
            );
        }
        assert_eq!(parse_runs_select_value("id:run-a", Vec::new()), None);
    }

    #[test]
    fn project_changes_cannot_reuse_current_project_run_modes() {
        let current = ProjectRef::Current;
        let foreign = ProjectRef::Specific("other-project".into());

        assert_eq!(runs_for_project_change(&current), RunRef::Selected);
        assert_eq!(
            runs_for_project_change(&foreign),
            RunRef::Specific(Vec::new()),
        );
        for runs in [
            RunRef::Selected,
            RunRef::All,
            RunRef::Specific(vec!["run-c".into(), "run-d".into()]),
        ] {
            assert_eq!(runs_select_value(&foreign, &runs), SPECIFIC_RUNS_VALUE);
        }
    }

    #[test]
    fn x_metric_names_include_only_numeric_metrics_in_first_seen_order() {
        assert_eq!(
            x_metric_names(vec![
                metric("image", MetricType::Cdn),
                metric("epoch", MetricType::Numeric),
                metric("logs", MetricType::TextStream),
                MetricInfo {
                    metric_name: "future_type".into(),
                    metric_type: 999,
                },
                metric("epoch", MetricType::Numeric),
                metric("learning_rate", MetricType::Numeric),
            ]),
            ["epoch", "learning_rate"]
        );
    }
}

#[component]
fn BindingRow(
    id_prefix: String,
    index: usize,
    binding: MetricBinding,
    /// Set when the other sources agree on a type: metrics of any other type are disabled, since a chart cannot mix types.
    required_kind: Option<DisplayType>,
    /// Stable identity of this source; keys its entry in `row_catalogs`.
    row_id: u64,
    /// Shared with the sibling rows; holds this row's catalog while it matches the current discovery.
    mut row_catalogs: Signal<RowCatalogs>,
    projects: Resource<Vec<String>>,
    on_change: EventHandler<MetricBinding>,
    on_remove: EventHandler<()>,
) -> Element {
    let state = use_context::<crate::state::DashboardState>();
    let effective_project = match &binding.project {
        ProjectRef::Current => state.project_id.read().clone(),
        ProjectRef::Specific(p) => p.clone(),
    };
    let mut metric_filter = use_signal(String::new);

    let runs_version = state
        .project_versions
        .read()
        .get(&effective_project)
        .copied();
    let runs = use_resource(use_reactive(
        (&effective_project, &runs_version),
        move |(project, _version)| {
            let grpc = state.grpc.read().clone();
            async move {
                let fetched = crate::state::visibility::retry_visible("binding runs", async || {
                    grpc.list_runs(&project).await
                })
                .await
                .runs;
                (project, fetched)
            }
        },
    ));

    let discovery_run_ids =
        metric_discovery_run_ids(&binding.runs, &effective_project, runs.read().as_ref());
    let metric_version = crate::state::versions_key(
        *state.resync_gen.read(),
        discovery_run_ids.iter().map(String::as_str),
        &state.metrics_gen.read(),
    );
    let metrics = use_resource(use_reactive(
        (&effective_project, &discovery_run_ids, &metric_version),
        move |(project, discovery_run_ids, _version)| {
            let grpc = state.grpc.read().clone();
            async move {
                // Invalidate sibling type gating while this source refreshes.
                if row_catalogs.peek().contains_key(&row_id) {
                    row_catalogs.write().remove(&row_id);
                }
                let metrics = match crate::state::visibility::retry_visible_run(
                    "binding metrics",
                    async || {
                        grpc.list_run_set_metrics(&project, &discovery_run_ids)
                            .await
                    },
                )
                .await
                {
                    Ok(metrics) => metrics,
                    // A terminal run can fail the batch; keep metrics from the readable runs.
                    Err(_) => {
                        let mut merged = Vec::new();
                        for run_id in &discovery_run_ids {
                            let metrics = crate::state::visibility::retry_visible_run(
                                "binding metrics",
                                async || grpc.list_metrics(&project, run_id).await,
                            )
                            .await;
                            merged.extend(metrics.unwrap_or_default());
                        }
                        merged
                    }
                };
                let catalog = metric_catalog(metrics);
                row_catalogs.write().insert(row_id, catalog.clone());
                (project, discovery_run_ids, catalog)
            }
        },
    ));

    let project_value = project_select_value(&binding.project);
    let runs_value = runs_select_value(&binding.project, &binding.runs);
    let is_foreign = matches!(&binding.project, ProjectRef::Specific(_));
    let project_label = binding_control_label(index, "project");
    let runs_label = binding_control_label(index, "runs");
    let run_list_label = binding_control_label(index, "specific runs");
    let metric_label = binding_control_label(index, "metric");
    let filter_label = binding_control_label(index, "metric filter");
    let remove_label = format!("Remove source {}", index + 1);

    rsx! {
        div { class: "binding-row",
            div { class: "binding-row-header",
                span { class: "binding-label", "Source {index + 1}" }
                button {
                    class: "binding-remove",
                    aria_label: "{remove_label}",
                    onmousedown: primary(move |_| on_remove.call(())),
                    "Remove"
                }
            }

            div { class: "binding-fields",
                div { class: "binding-field",
                    label { r#for: "{id_prefix}-project", "Project" }
                    select {
                        id: "{id_prefix}-project",
                        aria_label: "{project_label}",
                        value: "{project_value}",
                        onchange: {
                            let binding = binding.clone();
                            move |e: Event<FormData>| {
                                let Some(new_project) = parse_project_select_value(&e.value()) else {
                                    return;
                                };
                                on_change.call(MetricBinding {
                                    runs: runs_for_project_change(&new_project),
                                    project: new_project,
                                    metric_name: binding.metric_name.clone(),
                                });
                            }
                        },
                        option { value: CURRENT_PROJECT_VALUE, "Current Project" }
                        {
                            // `selected` pins the bound project, since Dioxus writes the select's `value` before these options exist or shift.
                            let mut options = projects.cloned().unwrap_or_default();
                            if let ProjectRef::Specific(bound) = &binding.project {
                                options.retain(|project| project != bound);
                                options.insert(0, bound.clone());
                            }
                            rsx! {
                                for p in options {
                                    option {
                                        value: id_select_value(&p),
                                        selected: matches!(&binding.project, ProjectRef::Specific(bound) if *bound == p),
                                        "{p}"
                                    }
                                }
                            }
                        }
                    }
                }

                div { class: "binding-field",
                    label { r#for: "{id_prefix}-runs", "Runs" }
                    select {
                        id: "{id_prefix}-runs",
                        aria_label: "{runs_label}",
                        value: "{runs_value}",
                        onchange: {
                            let binding = binding.clone();
                            move |e: Event<FormData>| {
                                let seed = build_view_context(&state).selected_run_ids();
                                let Some(new_runs) = parse_runs_select_value(&e.value(), seed) else {
                                    return;
                                };
                                on_change.call(MetricBinding {
                                    runs: new_runs,
                                    ..binding.clone()
                                });
                            }
                        },
                        option { value: SELECTED_RUNS_VALUE, disabled: is_foreign, "Shown Runs" }
                        option { value: ALL_RUNS_VALUE, disabled: is_foreign, "All Runs" }
                        option { value: SPECIFIC_RUNS_VALUE, "Specific Runs" }
                    }
                }

                if runs_value == SPECIFIC_RUNS_VALUE {
                    {
                        let checked = checked_run_ids(&binding.runs);
                        let runs_read = runs.read();
                        let project_runs = matching_project_runs(&effective_project, runs_read.as_ref());
                        let choices = project_runs.map_or_else(Vec::new, |catalog| specific_run_choices(checked, catalog));
                        let no_runs = project_runs.is_some() && choices.is_empty();
                        rsx! {
                            div {
                                class: "binding-run-list",
                                role: "group",
                                aria_label: "{run_list_label}",
                                for (run_id, name) in choices {
                                    label { key: "{run_id}", class: "checkbox-label", title: "{run_id}",
                                        input {
                                            r#type: "checkbox",
                                            checked: checked.contains(&run_id),
                                            onchange: {
                                                let binding = binding.clone();
                                                move |e: Event<FormData>| {
                                                    on_change.call(MetricBinding {
                                                        runs: toggle_specific_run(&binding.runs, &run_id, e.checked()),
                                                        ..binding.clone()
                                                    });
                                                }
                                            },
                                        }
                                        span { class: "fade-overflow", span { "{name}" } }
                                    }
                                }
                                if no_runs {
                                    span { class: "binding-run-empty", "No runs in this project" }
                                }
                            }
                        }
                    }
                }

                {
                    let current = binding.metric_name.as_str();
                    let metrics_read = metrics.read();
                    let catalog = matching_metric_catalog(
                        &effective_project,
                        &discovery_run_ids,
                        metrics_read.as_ref(),
                    );
                    let filter = metric_filter.read();
                    let (groups, matched) = metric_groups(catalog, &filter, current);
                    let current_untyped = !current.is_empty() && catalog_kind(catalog, current).is_none();
                    let total = catalog.len();
                    rsx! {
                        div { class: "binding-field",
                            label { r#for: "{id_prefix}-metric", "Metric" }
                            select {
                                id: "{id_prefix}-metric",
                                aria_label: "{metric_label}",
                                value: "{current}",
                                onchange: {
                                    let binding = binding.clone();
                                    move |e: Event<FormData>| {
                                        on_change.call(MetricBinding {
                                            metric_name: e.value(),
                                            ..binding.clone()
                                        });
                                    }
                                },
                                // `selected` pins the saved value as filtering reshuffles the options under it.
                                option { value: "", selected: current.is_empty(), "— Select metric —" }
                                if current_untyped {
                                    option { value: "{current}", selected: true, "{current}" }
                                }
                                for (kind, names) in groups {
                                    {
                                        let blocked = required_kind.is_some_and(|required| required != kind);
                                        let group_label = match required_kind {
                                            Some(required) if blocked => format!(
                                                "{} — other sources are {}",
                                                kind_label(kind),
                                                kind_label(required).to_lowercase(),
                                            ),
                                            _ => kind_label(kind).to_string(),
                                        };
                                        rsx! {
                                            optgroup { label: "{group_label}", disabled: blocked,
                                                for name in names {
                                                    option { value: "{name}", selected: name == current, "{name}" }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        div { class: "binding-field binding-metric-filter",
                            input {
                                r#type: "text",
                                aria_label: "{filter_label}",
                                placeholder: "Filter metrics",
                                autocomplete: "off",
                                spellcheck: "false",
                                value: "{filter}",
                                oninput: move |e: Event<FormData>| metric_filter.set(e.value()),
                            }
                            if !filter.trim().is_empty() {
                                span { class: "binding-filter-count", "{matched} of {total}" }
                            }
                        }
                    }
                }
            }
        }
    }
}
