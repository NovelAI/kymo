use std::collections::{HashMap, HashSet};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::grpc::proto::metric_info::MetricType;
use crate::grpc::proto::MetricInfo;
use crate::util::sections::group_by_prefix;
use crate::util::{local_storage, natural_cmp, warn};

use super::section_order::{OrderContext, SectionGap, SectionKey, SectionOrder};

// --- Binding types ---

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ProjectRef {
    Current,
    Specific(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum RunRef {
    Selected,
    All,
    Specific(Vec<String>),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MetricBinding {
    pub project: ProjectRef,
    pub runs: RunRef,
    pub metric_name: String,
}

// --- Resolution ---

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ResolvedRef {
    pub project_id: String,
    pub run_id: String,
    pub metric_name: String,
}

pub struct ViewContext {
    pub current_project: String,
    pub current_run: Option<String>,
    pub selected_runs: HashSet<String>,
    pub all_runs: Vec<String>,
}

impl ViewContext {
    /// The runs "Shown Runs" resolves to: the direct run, else the sidebar selection.
    pub fn selected_run_ids(&self) -> Vec<String> {
        match &self.current_run {
            Some(r) => vec![r.clone()],
            None => {
                // Walk `all_runs`, not the HashSet, for the server's deterministic order (cap_runs picks survivors by it).
                self.all_runs
                    .iter()
                    .filter(|id| self.selected_runs.contains(*id))
                    .cloned()
                    .collect()
            }
        }
    }
}

pub fn resolve_binding(binding: &MetricBinding, ctx: &ViewContext) -> Vec<ResolvedRef> {
    let project_id = match &binding.project {
        ProjectRef::Current => ctx.current_project.clone(),
        ProjectRef::Specific(p) => p.clone(),
    };

    let run_ids: Vec<String> = match &binding.runs {
        RunRef::Selected => ctx.selected_run_ids(),
        RunRef::All => ctx.all_runs.clone(),
        RunRef::Specific(runs) => runs.clone(),
    };

    run_ids
        .into_iter()
        .map(|run_id| ResolvedRef {
            project_id: project_id.clone(),
            run_id,
            metric_name: binding.metric_name.clone(),
        })
        .collect()
}

pub fn resolve_all_bindings(bindings: &[MetricBinding], ctx: &ViewContext) -> Vec<ResolvedRef> {
    let mut seen = HashSet::new();
    bindings
        .iter()
        .flat_map(|b| resolve_binding(b, ctx))
        // Bindings carry no per-source style or alias, so exact duplicates have no distinct presentation. Keep the first occurrence/order and avoid duplicate chart series, CDN columns, and server reads.
        .filter(|resolved| seen.insert(resolved.clone()))
        .collect()
}

/// Cap `refs` to at most `max_runs` distinct runs (0 = unlimited). Multiple
/// metrics on the same run count as one run. Survivors are the first
/// `max_runs` distinct runs in resolution order, which is deterministic —
/// `resolve_binding` emits Selected and All in run-list order, Specific in
/// stored order — so the subset is stable across renders.
pub fn cap_runs(refs: Vec<ResolvedRef>, max_runs: u32) -> Vec<ResolvedRef> {
    // 0 means unlimited — and without this return the filter below would admit nothing.
    if max_runs == 0 {
        return refs;
    }
    let mut keep: HashSet<String> = HashSet::new();
    refs.into_iter()
        .filter(|r| {
            if keep.len() < max_runs as usize {
                keep.insert(r.run_id.clone());
            }
            keep.contains(&r.run_id)
        })
        .collect()
}

pub fn resolve_capped_bindings(
    bindings: &[MetricBinding],
    ctx: &ViewContext,
    max_runs: u32,
) -> Vec<ResolvedRef> {
    cap_runs(resolve_all_bindings(bindings, ctx), max_runs)
}

#[cfg(test)]
mod binding_resolution_tests {
    use super::*;

    fn context() -> ViewContext {
        ViewContext {
            current_project: "project".to_string(),
            current_run: None,
            selected_runs: HashSet::from(["run-a".to_string(), "run-b".to_string()]),
            all_runs: vec!["run-a".to_string(), "run-b".to_string()],
        }
    }

    fn binding(runs: RunRef, metric_name: &str) -> MetricBinding {
        MetricBinding {
            project: ProjectRef::Current,
            runs,
            metric_name: metric_name.to_string(),
        }
    }

    #[test]
    fn exact_binding_overlaps_resolve_once_in_first_seen_order() {
        let refs = resolve_all_bindings(
            &[
                binding(RunRef::Selected, "loss"),
                binding(RunRef::Specific(vec!["run-a".to_string()]), "loss"),
                binding(RunRef::Specific(vec!["run-a".to_string()]), "accuracy"),
                MetricBinding {
                    project: ProjectRef::Specific("other-project".to_string()),
                    runs: RunRef::Specific(vec!["run-a".to_string()]),
                    metric_name: "loss".to_string(),
                },
            ],
            &context(),
        );

        assert_eq!(
            refs,
            vec![
                ResolvedRef {
                    project_id: "project".to_string(),
                    run_id: "run-a".to_string(),
                    metric_name: "loss".to_string(),
                },
                ResolvedRef {
                    project_id: "project".to_string(),
                    run_id: "run-b".to_string(),
                    metric_name: "loss".to_string(),
                },
                ResolvedRef {
                    project_id: "project".to_string(),
                    run_id: "run-a".to_string(),
                    metric_name: "accuracy".to_string(),
                },
                ResolvedRef {
                    project_id: "other-project".to_string(),
                    run_id: "run-a".to_string(),
                    metric_name: "loss".to_string(),
                },
            ]
        );
    }

    #[test]
    fn binding_run_cap_keeps_every_metric_for_the_first_distinct_runs() {
        let bindings = [
            binding(
                RunRef::Specific(vec!["run-a".into(), "run-b".into(), "run-c".into()]),
                "loss",
            ),
            binding(RunRef::Specific(vec!["run-a".into()]), "accuracy"),
        ];

        let capped = resolve_capped_bindings(&bindings, &context(), 2);
        assert_eq!(
            capped
                .iter()
                .map(|r| (r.run_id.as_str(), r.metric_name.as_str()))
                .collect::<Vec<_>>(),
            vec![("run-a", "loss"), ("run-b", "loss"), ("run-a", "accuracy"),]
        );
        assert_eq!(
            resolve_capped_bindings(&bindings, &context(), 1)
                .iter()
                .map(|r| (r.run_id.as_str(), r.metric_name.as_str()))
                .collect::<Vec<_>>(),
            vec![("run-a", "loss"), ("run-a", "accuracy")]
        );
        assert_eq!(resolve_capped_bindings(&bindings, &context(), 0).len(), 4);
    }
}

// --- Layout config ---

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LayoutConfig {
    pub sections: Vec<SectionConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SectionConfig {
    /// Immutable identity: the metric prefix for auto-generated sections, a
    /// generated id for user-created ones. The persisted `LayoutDiff` keys
    /// sections by `name`, so it never changes — user renames live in
    /// `display_name`.
    pub name: String,
    /// User-chosen label; empty means "use `name`". A plain section setting,
    /// so renames persist through the settings patch like any other edit.
    #[serde(default)]
    pub display_name: String,
    /// The user's collapse toggle; `None` defers to the browser's sections-visible setting (see [`Self::is_collapsed`]).
    pub collapsed: Option<bool>,
    pub rects: Vec<RectConfig>,
    #[serde(default = "default_chart_height")]
    pub chart_height: u32,
    /// Higher priority sorts higher on the page. Auto-generated `info` /
    /// `logs` / `system` sections default to -100 so they sink to the bottom.
    #[serde(default)]
    pub priority: i32,
    /// Number of columns in the section's CSS grid. Each rect occupies
    /// `column_span` of these. Default 4.
    #[serde(default = "default_max_columns")]
    pub max_columns: u32,
    /// How many model-rows to show per page. 0 = unlimited (no pagination).
    /// Default 3.
    #[serde(default = "default_rows_per_page")]
    pub rows_per_page: u32,
    /// Chart-option defaults every rect in this section inherits: a JSON
    /// merge patch over the project-level resolution. An absent key means
    /// "inherit from the project level"; `Null` means nothing is set here.
    /// The middle level of the project → section → rect cascade — rect
    /// overrides beat this, this beats the project patch.
    #[serde(default)]
    pub chart_defaults: Value,
}

fn default_chart_height() -> u32 {
    280
}

fn default_max_columns() -> u32 {
    4
}

fn default_rows_per_page() -> u32 {
    3
}

impl SectionConfig {
    /// A section with the auto-generated defaults for `name`. The single
    /// construction site for `auto_generate`, for `LayoutDiff::apply` when it
    /// must materialize a section the base no longer provides, and (with the
    /// fields the user picks overridden) for user-created sections — so the
    /// defaults can't drift apart.
    pub fn auto(name: String, rects: Vec<RectConfig>) -> Self {
        Self {
            collapsed: None,
            priority: default_priority_auto(&name),
            max_columns: default_max_columns_auto(&name),
            display_name: String::new(),
            chart_height: default_chart_height(),
            rows_per_page: default_rows_per_page(),
            chart_defaults: Value::Null,
            name,
            rects,
        }
    }

    /// The user's toggle, else the default: expanded with `sections_visible` on; with it off, only the unnamed catch-all starts open.
    pub fn is_collapsed(&self, sections_visible: bool) -> bool {
        self.collapsed
            .unwrap_or(!sections_visible && !self.name.is_empty())
    }

    /// Record a toggle; one that lands back on the default clears it, so the saved override stays sparse and follows the setting again.
    pub fn set_collapsed(&mut self, collapsed: bool, sections_visible: bool) {
        self.collapsed = None;
        if self.is_collapsed(sections_visible) != collapsed {
            self.collapsed = Some(collapsed);
        }
    }

    /// Whether the navbar filter shows this section: any panel matches, and an empty `needle` shows every section, even one with no panels, so its add-chart button stays reachable.
    pub fn matches_filter(&self, needle: &str) -> bool {
        needle.is_empty() || self.rects.iter().any(|r| r.matches_filter(needle))
    }

    /// Label shown in the UI and used for sort tie-breaks: the user's
    /// rename if set, the immutable `name` otherwise.
    pub fn display_name(&self) -> &str {
        if self.display_name.is_empty() {
            &self.name
        } else {
            &self.display_name
        }
    }
}

/// Default priority for an auto-generated section, derived from its name.
/// The unnamed catch-all (empty name) pins to the top — it holds the
/// unprefixed metrics, which tend to be the user's primary scalars.
/// Meta/diagnostic sections sink to the bottom; everything else stays at 0.
fn default_priority_auto(name: &str) -> i32 {
    match name {
        "" => 100,
        "info" | "logs" | "system" => -100,
        _ => 0,
    }
}

/// Default column count for an auto-generated section. `info` is 1-up
/// (metadata is wide and easier to read at full width); `logs` is 2-up
/// (text streams need horizontal room but pair fine).
fn default_max_columns_auto(name: &str) -> u32 {
    match name {
        "info" => 1,
        "logs" => 2,
        _ => default_max_columns(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RectConfig {
    pub id: String,
    #[serde(default)]
    pub label: String, // user-facing name; empty = auto-derive from bindings
    pub bindings: Vec<MetricBinding>,
    pub display_type: DisplayType,
    #[serde(default)]
    pub options: RectOptions,
}

/// How a CDN gallery displays multiple runs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub enum CdnDisplayMode {
    /// Slider picks an index; shows that index from each run.
    #[default]
    SelectIndex,
    /// One grid per run, concatenated.
    GroupByRun,
    /// One grid per index, each showing all runs.
    Interleaved,
}

/// X-axis mode: step, relative time, or wall-clock time.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub enum XAxisMode {
    #[default]
    Step,
    RelativeTime,
    WallTime,
}

/// Smoothing algorithm selection (mirrors proto SmoothingConfig.Algorithm).
#[derive(Clone, Debug, Serialize, PartialEq, Default)]
pub enum SmoothingAlgorithm {
    None,
    TriangularPolyfit,
    EmaPolyfit,
    #[default]
    BiweightPolyfit,
}

/// Manual Deserialize so an unknown variant in a stored layout — e.g.
/// "RunningAverage" (removed 2026-06) — falls back to None instead of
/// failing the parse of the whole layout.
impl<'de> Deserialize<'de> for SmoothingAlgorithm {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(match String::deserialize(d)?.as_str() {
            "TriangularPolyfit" => Self::TriangularPolyfit,
            "EmaPolyfit" => Self::EmaPolyfit,
            "BiweightPolyfit" => Self::BiweightPolyfit,
            _ => Self::None,
        })
    }
}

#[cfg(test)]
mod smoothing_default_tests {
    use super::*;

    #[test]
    fn new_and_missing_options_default_to_biweight_polyfit() {
        assert_eq!(
            RectOptions::default().smoothing,
            SmoothingAlgorithm::BiweightPolyfit
        );
        let stored: RectOptions = serde_json::from_str("{}").unwrap();
        assert_eq!(stored.smoothing, SmoothingAlgorithm::BiweightPolyfit);
    }
}

/// The e-folding time constant a stored EMA alpha denotes (old data's weight decays to 1/e after τ steps), as the editor shows it and chart requests send it: τ = -1 / ln(1 - α), at least 1 step, and 10 for an alpha outside (0, 1).
pub fn ema_time_constant(alpha: f64) -> f64 {
    if alpha > 0.0 && alpha < 1.0 {
        (-1.0 / (-alpha).ln_1p()).max(1.0)
    } else {
        10.0
    }
}

/// Per-rect display options.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RectOptions {
    #[serde(default)]
    pub log_x: bool,
    #[serde(default)]
    pub log_y: bool,
    /// How many columns this rect occupies in its section's grid. Clamped
    /// at render time to `[1, section.max_columns]`. Default 1.
    #[serde(default = "default_column_span")]
    pub column_span: u32,
    #[serde(default)]
    pub cdn_display_mode: CdnDisplayMode,
    #[serde(default)]
    pub smoothing: SmoothingAlgorithm,
    #[serde(default = "default_smoothing_window")]
    pub smoothing_window: u32,
    #[serde(default = "default_smoothing_alpha")]
    pub smoothing_alpha: f64,
    /// Polynomial fit degree (0 = weighted average, 1 = local linear,
    /// 2 = local quadratic).
    #[serde(default = "default_smoothing_poly_order")]
    pub smoothing_poly_order: u32,
    /// Custom X-axis metric name. Empty = use step/time mode.
    #[serde(default)]
    pub x_axis_metric: String,
    /// X-axis mode: step, relative time, or wall time.
    #[serde(default)]
    pub x_axis_mode: XAxisMode,
    /// Metadata viewer: only show keys whose values differ across runs.
    #[serde(default = "default_metadata_diff_only")]
    pub metadata_diff_only: bool,
    /// Max distinct runs shown in this panel; runs beyond the first
    /// `max_runs` resolved are dropped before querying (see [`cap_runs`]).
    /// 0 = unlimited.
    #[serde(default = "default_max_runs")]
    pub max_runs: u32,
}

impl RectOptions {
    /// Plain step axis — no custom x metric, no time mode. The only axis
    /// with a shared zoom range, and the only one where an unzoomed
    /// response can prove a run silent (chart_sync noncontrib).
    pub fn is_step_axis(&self) -> bool {
        self.x_axis_metric.is_empty() && matches!(self.x_axis_mode, XAxisMode::Step)
    }
}

fn default_smoothing_window() -> u32 {
    100
}
fn default_smoothing_alpha() -> f64 {
    1.0 - (-0.01f64).exp()
}
fn default_smoothing_poly_order() -> u32 {
    1
}
fn default_column_span() -> u32 {
    1
}
fn default_max_runs() -> u32 {
    12
}
fn default_metadata_diff_only() -> bool {
    true
}

impl Default for RectOptions {
    fn default() -> Self {
        Self {
            log_x: false,
            log_y: false,
            column_span: default_column_span(),
            cdn_display_mode: CdnDisplayMode::default(),
            smoothing: SmoothingAlgorithm::default(),
            smoothing_window: default_smoothing_window(),
            smoothing_alpha: default_smoothing_alpha(),
            smoothing_poly_order: default_smoothing_poly_order(),
            x_axis_metric: String::new(),
            x_axis_mode: XAxisMode::default(),
            metadata_diff_only: default_metadata_diff_only(),
            max_runs: default_max_runs(),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DisplayType {
    Numeric,
    Cdn,
    TextStream,
}

impl DisplayType {
    /// Unknown metric types plot as numeric.
    pub(crate) fn for_metric(metric: &MetricInfo) -> Self {
        match metric.metric_type() {
            MetricType::Cdn => Self::Cdn,
            MetricType::TextStream => Self::TextStream,
            _ => Self::Numeric,
        }
    }
}

impl RectConfig {
    /// Whether this panel matches the navbar filter. `needle` is pre-trimmed and lowercased by the caller; empty matches everything. Substring-matches the label and each binding's full metric name, so `system/` selects those charts.
    pub fn matches_filter(&self, needle: &str) -> bool {
        needle.is_empty()
            || self.label.to_lowercase().contains(needle)
            || self
                .bindings
                .iter()
                .any(|b| b.metric_name.to_lowercase().contains(needle))
    }
}

impl LayoutConfig {
    /// Auto-generate a layout from discovered metrics.
    /// All bindings use (Current, Selected, metric_name) so they adapt to context.
    pub fn auto_generate(metrics: &[MetricInfo]) -> Self {
        // Rect ids below are bare metric names, so name-uniqueness is a
        // correctness requirement (unique Dioxus keys, one rect per saved
        // override/tombstone) — enforce it here, first occurrence wins,
        // rather than assuming it of callers.
        let mut seen_names = HashSet::new();
        let metrics: Vec<MetricInfo> = metrics
            .iter()
            .filter(|m| seen_names.insert(m.metric_name.clone()))
            .cloned()
            .collect();
        let groups = group_by_prefix(&metrics);
        let sections = groups
            .into_iter()
            .map(|(section_name, section_metrics)| {
                let rects = section_metrics
                    .iter()
                    .map(|m| {
                        let display_type = DisplayType::for_metric(m);
                        // System metrics and logs use time-based X axis
                        let is_time_keyed = m.metric_name.starts_with("system/")
                            || m.metric_name.starts_with("logs/");
                        let mut options = RectOptions::default();
                        if is_time_keyed {
                            options.x_axis_mode = XAxisMode::RelativeTime;
                        }
                        RectConfig {
                            // Stable identity for diffing against saved user
                            // edits: metric names are unique across the base
                            // (deduped above), and keying on the name alone
                            // keeps persisted overrides/tombstones valid
                            // even if the prefix-grouping scheme changes.
                            id: m.metric_name.clone(),
                            label: String::new(),
                            bindings: vec![MetricBinding {
                                project: ProjectRef::Current,
                                runs: RunRef::Selected,
                                metric_name: m.metric_name.clone(),
                            }],
                            display_type,
                            options,
                        }
                    })
                    .collect();
                SectionConfig::auto(section_name, rects)
            })
            .collect();
        let mut layout = Self { sections };
        layout.sort_sections();
        layout
    }

    /// Reorder `sections` so higher priority appears first; ties broken by
    /// display name, then immutable name (asc) for deterministic ordering.
    /// This is the inherited order before section placements are projected.
    pub fn sort_sections(&mut self) {
        self.sections
            .sort_by_cached_key(|section| SectionKey::from(section));
    }

    /// The section with the given immutable `name`, if present.
    pub fn find_section(&self, name: &str) -> Option<&SectionConfig> {
        self.sections.iter().find(|s| s.name == name)
    }

    pub fn find_section_mut(&mut self, name: &str) -> Option<&mut SectionConfig> {
        self.sections.iter_mut().find(|s| s.name == name)
    }

    /// The rect with the given id, searching every section.
    pub fn find_rect(&self, id: &str) -> Option<&RectConfig> {
        self.sections
            .iter()
            .flat_map(|s| s.rects.iter())
            .find(|r| r.id == id)
    }

    /// Name of the section holding rect `id`.
    pub fn section_of_rect(&self, id: &str) -> Option<&str> {
        self.sections
            .iter()
            .find(|s| s.rects.iter().any(|r| r.id == id))
            .map(|s| s.name.as_str())
    }

    /// The rect with the given id plus its section's `max_columns` — the pair the maximize overlay is opened with.
    pub fn resolve_rect(&self, id: &str) -> Option<(RectConfig, u32)> {
        let rect = self.find_rect(id)?.clone();
        let max_columns = self
            .section_of_rect(id)
            .and_then(|s| self.find_section(s))
            .map(|s| s.max_columns.max(1))
            .unwrap_or(1);
        Some((rect, max_columns))
    }
}

pub enum LoadResult {
    Loaded(LayoutDiff),
    Missing,
    Corrupt(String),
}

/// Resolve a rect id without waiting for the metrics sweep (tens of seconds on big projects), so a `?chart=` link's maximize overlay opens immediately.
/// No new construction path: user rect creation bases and their sparse edits come from the saved diff applied to an empty base, and anything else gets the rect the pipeline would auto-generate for this id-as-metric-name, with the same diff apply. The bases are tried separately so a user rect is never shadowed by a same-id synthetic.
/// Only the sweep knows metric types, so the synthetic assumes Numeric; the overlay re-resolves against the real layout each render, so the type corrects itself when the sweep lands, and a rect that turns out not to exist shows an empty chart.
pub fn resolve_rect_locally(diff: &LayoutDiff, id: &str) -> Option<(RectConfig, u32)> {
    let synthetic = MetricInfo {
        metric_name: id.to_string(),
        metric_type: MetricType::Numeric as i32,
    };
    let bases = [
        LayoutConfig::auto_generate(&[]),
        LayoutConfig::auto_generate(&[synthetic]),
    ];
    bases
        .iter()
        .find_map(|base| diff.apply(base).resolve_rect(id))
}

// --- User diff from the auto-generated base ---

const LAYOUT_DIFF_FORMAT_VERSION: u8 = 2;

/// The user's edits relative to the auto-generated layout, sparse and
/// keyed on stable rect/section identity. This is the only layout state
/// that persists: the dashboard regenerates the base from live metrics on
/// every load and applies this on top, so charts for new metrics always
/// appear while user changes stick.
///
/// # Updated by intent
///
/// Settings and chart gestures upsert or remove entries for their own
/// elements ([`Self::upsert_section_settings`], [`Self::delete_rect`],
/// ...), against a store re-read from localStorage per edit. Section ordering
/// may re-derive visible placements, while protecting hidden entries and
/// anchors; it never rewrites chart or section settings. Entries for
/// currently-absent metrics stay dormant until their
/// elements return, except a chart with an explicit Specific run binding:
/// that chart remains visible so recoverable Trash data stays reachable.
///
/// # Invariant: rect order is not user state
///
/// The diff stores no rect positions. [`Self::apply`] re-emits a section's
/// rects in auto-generated order with user-added rects appended — so a
/// reordering is never recorded and reverts on the next refresh. The UI
/// must therefore not offer rect reordering. (Section order is different:
/// it is sparse user state in `section_order`; legacy priority overrides
/// still feed its inherited default comparator.) If
/// rect reordering is ever wanted, the diff needs an explicit per-section
/// rect-id ordering.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LayoutDiff {
    /// Version of the persisted diff representation. Version 0 stored
    /// user-added rects whole; version 1 gives every rect the same sparse
    /// override path so project and section defaults can keep flowing.
    /// Version 2 adds live-anchor section ordering. The writer derives the
    /// version from content so users without ordering remain v1-compatible.
    #[serde(default)]
    format_version: u8,
    /// Live-anchor ordering; unreadable raw data survives unrelated saves.
    #[serde(default, skip_serializing_if = "SectionOrder::is_empty")]
    pub section_order: SectionOrder,
    #[serde(default)]
    pub deleted_sections: Vec<String>,
    /// Names of user-created sections — pure existence markers. Settings
    /// ride `section_overrides`; chart creation bases ride `added_rects` and
    /// later edits `rect_overrides`, so resolved cascade state and
    /// prefix-collision merges never bake into the store.
    /// Replaces whole-`SectionConfig` `added_sections`; old keys are ignored on load — an accepted one-time loss, like the legacy full-layout blob.
    #[serde(default)]
    pub user_sections: Vec<String>,
    /// Per-property edits to sections' settings — base-derived and user-created alike — keyed by section name (the sections' rects diff separately below).
    #[serde(default)]
    pub section_overrides: Vec<ConfigPatch>,
    /// Ids of auto-gen rects the user deleted.
    #[serde(default)]
    pub deleted_rects: Vec<String>,
    /// Per-property edits to any rect, keyed by rect id.
    #[serde(default)]
    pub rect_overrides: Vec<ConfigPatch>,
    /// Creation-time bases for user-added charts. Later edits use
    /// `rect_overrides`, exactly like edits to auto-generated charts. If the
    /// live base later owns the same global id, it replaces this base while
    /// the sparse override stays attached to that chart identity.
    #[serde(default)]
    pub added_rects: Vec<AddedRect>,
    /// Project-level chart-option defaults: a JSON merge patch over
    /// `RectOptions::default()`. An absent key means "library default";
    /// `Null` means nothing is set. The coarsest level of the project →
    /// section → rect cascade.
    #[serde(default)]
    pub project_chart_defaults: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AddedRect {
    pub section: String,
    pub rect: RectConfig,
}

impl Default for LayoutDiff {
    fn default() -> Self {
        Self {
            format_version: 1,
            section_order: SectionOrder::default(),
            deleted_sections: Vec::new(),
            user_sections: Vec::new(),
            section_overrides: Vec::new(),
            deleted_rects: Vec::new(),
            rect_overrides: Vec::new(),
            added_rects: Vec::new(),
            project_chart_defaults: Value::Null,
        }
    }
}

fn parse_layout_diff(json: &str) -> Result<LayoutDiff, String> {
    let mut diff = serde_json::from_str::<LayoutDiff>(json).map_err(|error| error.to_string())?;
    if diff.format_version > LAYOUT_DIFF_FORMAT_VERSION {
        return Err(format!(
            "saved layout format {} is newer than this build (supports through {})",
            diff.format_version, LAYOUT_DIFF_FORMAT_VERSION
        ));
    }
    diff.migrate();
    Ok(diff)
}

/// One element's user edits (a rect, or a section's settings), stored as a
/// JSON merge patch (RFC 7386) against the element's serialized
/// auto-generated form. Diffing through serde makes the config struct
/// definitions the single source of truth: properties added to them
/// participate automatically, only changed properties are stored, and
/// untouched ones keep tracking improvements to the auto-generated
/// defaults. This is also the shape a future project → section → rect
/// config cascade resolves: an absent key means "inherit from the coarser
/// level", and resolution is repeated `merge_apply`.
///
/// Objects diff recursively; arrays and scalars replace wholesale (an edit
/// to `bindings` stores the whole list). Patch keys for since-removed
/// properties are ignored by serde on deserialize; a patch value that no
/// longer fits the schema is dropped key-by-key (with a console warning),
/// keeping the element's still-valid customizations rather than reverting
/// it wholesale to its auto-generated form.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConfigPatch {
    /// Rect id or section name.
    pub key: String,
    pub patch: Value,
}

impl ConfigPatch {
    /// The properties of `cur` that differ from `base`; `None` if none do.
    fn between<T: Serialize + PartialEq>(key: &str, base: &T, cur: &T) -> Option<Self> {
        // Fast path, and load-bearing for non-object T: `merge_diff` on two
        // equal scalars returns `cur.clone()` (a non-empty patch), not an
        // empty map, so equality must short-circuit here.
        if base == cur {
            return None;
        }
        Self::between_values(
            key,
            &serde_json::to_value(base).ok()?,
            &serde_json::to_value(cur).ok()?,
        )
    }

    fn between_values(key: &str, base: &Value, cur: &Value) -> Option<Self> {
        let patch = merge_diff(base, cur);
        match &patch {
            Value::Object(map) if map.is_empty() => None,
            _ => Some(Self {
                key: key.to_string(),
                patch,
            }),
        }
    }

    fn apply_to<T: Serialize + DeserializeOwned>(&self, target: &mut T) {
        let Ok(base_value) = serde_json::to_value(&*target) else {
            return;
        };
        let mut value = base_value.clone();
        merge_apply(&mut value, &self.patch);
        match serde_json::from_value::<T>(value) {
            Ok(merged) => {
                *target = merged;
                return;
            }
            Err(e) => warn(&format!("[layout] stale override for {}: {e}", self.key)),
        }
        // Salvage key-by-key: dropping only the keys that no longer fit the
        // schema keeps the element's other customizations alive — on screen
        // and in storage, since the next edit recomputes this entry from the
        // displayed element (a wholesale revert here would make that
        // recompute silently erase them).
        let Value::Object(patch) = &self.patch else {
            return;
        };
        let mut current = base_value;
        for (key, patch_value) in patch {
            let mut candidate = current.clone();
            let single =
                Value::Object(std::iter::once((key.clone(), patch_value.clone())).collect());
            merge_apply(&mut candidate, &single);
            if serde_json::from_value::<T>(candidate.clone()).is_ok() {
                current = candidate;
            } else {
                warn(&format!(
                    "[layout] dropped stale key {key:?} of override {}",
                    self.key
                ));
            }
        }
        if let Ok(merged) = serde_json::from_value::<T>(current) {
            *target = merged;
        }
    }

    /// A base-owned chart can disappear from discovery while one of its
    /// saved bindings still names a specific run (notably while that run is
    /// in Trash). Arrays replace wholesale in a merge patch, so a binding
    /// edit always leaves the complete binding list here.
    fn keeps_missing_rect(&self) -> bool {
        self.patch
            .get("bindings")
            .cloned()
            .and_then(|value| serde_json::from_value::<Vec<MetricBinding>>(value).ok())
            .is_some_and(|bindings| {
                bindings.iter().any(|binding| {
                    matches!(&binding.runs, RunRef::Specific(run_ids) if !run_ids.is_empty())
                })
            })
    }
}

/// The anchor a section's settings patch is computed against: the live base section when the current metric set provides it, else the default stub `apply` would materialize for it — so a patch saved while the section's metrics are absent still reads correctly when they return.
/// User-created sections resolve here too: a base section's settings are exactly the name-derived `SectionConfig::auto` defaults, so the anchor is identical whether or not the base owns the name — a prefix collision changes nothing.
fn section_anchor(base: &LayoutConfig, name: &str) -> SectionConfig {
    base.find_section(name)
        .cloned()
        .unwrap_or_else(|| SectionConfig::auto(name.to_string(), Vec::new()))
}

/// A section's settings as a JSON object, minus `rects` — those diff
/// separately, by rect id.
fn section_settings(s: &SectionConfig) -> Value {
    // Infallible: SectionConfig is plain structs/enums of JSON-native types.
    let mut value = serde_json::to_value(s).expect("SectionConfig serializes to a JSON object");
    if let Some(obj) = value.as_object_mut() {
        obj.remove("rects");
    }
    value
}

/// JSON merge patch turning `base` into `cur`: objects diff recursively to
/// just their changed keys, everything else replaces wholesale.
fn merge_diff(base: &Value, cur: &Value) -> Value {
    match (base, cur) {
        (Value::Object(base), Value::Object(cur)) => {
            let mut patch = serde_json::Map::new();
            for (key, cur_value) in cur {
                match base.get(key) {
                    Some(base_value) if base_value == cur_value => {}
                    Some(base_value) => {
                        patch.insert(key.clone(), merge_diff(base_value, cur_value));
                    }
                    None => {
                        patch.insert(key.clone(), cur_value.clone());
                    }
                }
            }
            for key in base.keys() {
                if !cur.contains_key(key) {
                    patch.insert(key.clone(), Value::Null);
                }
            }
            Value::Object(patch)
        }
        _ => cur.clone(),
    }
}

/// Apply a merge patch produced by `merge_diff` (null = remove key).
fn merge_apply(target: &mut Value, patch: &Value) {
    let Value::Object(patch) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = Value::Object(serde_json::Map::new());
    }
    let target = target.as_object_mut().expect("set to object above");
    for (key, patch_value) in patch {
        if patch_value.is_null() {
            target.remove(key);
        } else {
            merge_apply(
                target.entry(key.clone()).or_insert(Value::Null),
                patch_value,
            );
        }
    }
}

/// Resolve the chart-options cascade up to some level: `base` with each
/// level's merge patch applied in coarse → fine order. An absent key at a
/// level means "inherit from the level above"; a non-object patch (`Null`)
/// means nothing is set at that level. The rect's own sparse override
/// patch applies after these via [`ConfigPatch::apply_to`], like any other
/// rect edit — the finest level always wins.
pub fn cascade_options(base: &RectOptions, patches: &[&Value]) -> RectOptions {
    let Ok(mut value) = serde_json::to_value(base) else {
        return base.clone();
    };
    for patch in patches {
        if patch.is_object() {
            merge_apply(&mut value, patch);
        }
    }
    match serde_json::from_value(value) {
        Ok(resolved) => resolved,
        Err(e) => {
            warn(&format!("[layout] stale chart-defaults patch ignored: {e}"));
            base.clone()
        }
    }
}

/// The merge patch a defaults editor stores: the keys of `edited` that
/// differ from the level above (`anchor`); `Null` when none do. Diffing
/// against the parent level keeps the patch sparse — untouched fields keep
/// inheriting — and editing a field back to its inherited value drops it
/// from the patch, self-cleaning like rect overrides.
pub fn options_patch_between(anchor: &RectOptions, edited: &RectOptions) -> Value {
    let (Ok(a), Ok(e)) = (serde_json::to_value(anchor), serde_json::to_value(edited)) else {
        return Value::Null;
    };
    let patch = merge_diff(&a, &e);
    match &patch {
        Value::Object(map) if map.is_empty() => Value::Null,
        _ => patch,
    }
}

/// A finer-level config pinning one chart-option field — listed by the defaults editors, clearable so the field re-inherits.
#[derive(Clone, Debug, PartialEq)]
pub struct OptionOverride {
    /// `RectOptions` serde key.
    pub field: String,
    pub target: OverrideTarget,
    /// Section display name, or chart label (metric-name id when unlabeled).
    pub label: String,
    /// The pinned value, as stored in the patch.
    pub value: Value,
}

/// What holds the pin: a section's chart defaults or a rect's own override.
#[derive(Clone, Debug, PartialEq)]
pub enum OverrideTarget {
    Section(String),
    Rect(String),
}

/// Every chart-option pin below one cascade level: section chart-defaults keys plus rect override keys for the project editor (`section == None`), just its own rects' for a section editor.
/// Sections first, then charts, in display order; `shown` is the already
/// resolved displayed layout and supplies section defaults, membership,
/// ordering, and labels as one consistent snapshot.
pub fn finer_overrides(
    diff: &LayoutDiff,
    shown: &LayoutConfig,
    section: Option<&str>,
) -> Vec<OptionOverride> {
    let mut out = Vec::new();
    if section.is_none() {
        for s in &shown.sections {
            if let Value::Object(map) = &s.chart_defaults {
                for (field, value) in map {
                    out.push(OptionOverride {
                        field: field.clone(),
                        target: OverrideTarget::Section(s.name.clone()),
                        label: s.display_name().to_string(),
                        value: value.clone(),
                    });
                }
            }
        }
    }
    for s in shown
        .sections
        .iter()
        .filter(|s| section.is_none_or(|n| s.name == n))
    {
        for r in &s.rects {
            let Some(ov) = diff.rect_overrides.iter().find(|o| o.key == r.id) else {
                continue;
            };
            let Some(opts) = ov.patch.get("options").and_then(Value::as_object) else {
                continue;
            };
            let label = if r.label.is_empty() { &r.id } else { &r.label };
            for (field, value) in opts {
                out.push(OptionOverride {
                    field: field.clone(),
                    target: OverrideTarget::Rect(r.id.clone()),
                    label: label.clone(),
                    value: value.clone(),
                });
            }
        }
    }
    out
}

#[cfg(test)]
mod filter_tests {
    use super::*;

    fn rect(label: &str, metrics: &[&str]) -> RectConfig {
        RectConfig {
            id: "id".to_string(),
            label: label.to_string(),
            bindings: metrics
                .iter()
                .map(|m| MetricBinding {
                    project: ProjectRef::Current,
                    runs: RunRef::Selected,
                    metric_name: m.to_string(),
                })
                .collect(),
            display_type: DisplayType::Numeric,
            options: RectOptions::default(),
        }
    }

    #[test]
    fn empty_needle_matches_everything() {
        assert!(rect("", &[]).matches_filter(""));
        assert!(rect("anything", &["train/loss"]).matches_filter(""));
    }

    #[test]
    fn matches_label_or_full_metric_name() {
        // Caller passes the needle already lowercased.
        assert!(rect("", &["train/loss"]).matches_filter("loss"));
        assert!(!rect("", &["train/accuracy"]).matches_filter("loss"));
        // A section-prefix query selects via the full metric name.
        assert!(rect("", &["system/gpu/util_pct"]).matches_filter("system/"));
        assert!(!rect("", &["train/loss"]).matches_filter("system/"));
        // The label is matched too (case-insensitively).
        assert!(rect("My Chart", &["train/loss"]).matches_filter("my chart"));
        // A just-created blank rect (empty label, one empty binding) matches no non-empty needle — the reason create actions clear the filter first.
        assert!(!rect("", &[""]).matches_filter("active-filter"));
    }

    #[test]
    fn a_section_shows_when_any_panel_matches_and_empty_ones_only_unfiltered() {
        let train = SectionConfig::auto(
            "train".to_string(),
            vec![rect("", &["train/accuracy"]), rect("", &["train/loss"])],
        );
        assert!(train.matches_filter("loss"));
        assert!(!train.matches_filter("system/"));
        let empty = SectionConfig::auto("empty".to_string(), Vec::new());
        assert!(empty.matches_filter(""));
        assert!(!empty.matches_filter("empty"));
    }
}

#[cfg(test)]
mod cascade_tests {
    use super::*;

    fn base_with_one_rect() -> LayoutConfig {
        LayoutConfig {
            sections: vec![SectionConfig::auto(
                "train".to_string(),
                vec![RectConfig {
                    id: "train/loss".to_string(),
                    label: String::new(),
                    bindings: vec![],
                    display_type: DisplayType::Numeric,
                    options: RectOptions::default(),
                }],
            )],
        }
    }

    fn opts_of(layout: &LayoutConfig, id: &str) -> RectOptions {
        layout.find_rect(id).expect("rect").options.clone()
    }

    #[test]
    fn finest_level_wins_and_absent_inherits() {
        let base = base_with_one_rect();
        let mut diff = LayoutDiff::default();

        // Project level: EMA polyfit + log_y.
        let proj = RectOptions {
            smoothing: SmoothingAlgorithm::EmaPolyfit,
            log_y: true,
            ..Default::default()
        };
        diff.set_project_chart_defaults(&proj);

        // Section level: switch the algorithm only; log_y must inherit.
        let mut section = base.sections[0].clone();
        section.chart_defaults =
            serde_json::json!({ "smoothing": "BiweightPolyfit", "smoothing_window": 42 });
        diff.upsert_section_settings(&base, &section);

        let shown = diff.apply(&base);
        let o = opts_of(&shown, "train/loss");
        assert_eq!(
            o.smoothing,
            SmoothingAlgorithm::BiweightPolyfit,
            "section beats project"
        );
        assert_eq!(o.smoothing_window, 42);
        assert!(o.log_y, "unset at section level inherits from project");

        // Rect level: window only; algorithm/log_y keep flowing through.
        let mut edited = shown.find_rect("train/loss").unwrap().clone();
        edited.options.smoothing_window = 7;
        let mut diff2 = diff.clone();
        diff2.update_rect(&base, &edited);
        let shown2 = diff2.apply(&base);
        let o2 = opts_of(&shown2, "train/loss");
        assert_eq!(o2.smoothing_window, 7, "rect beats section");
        assert_eq!(o2.smoothing, SmoothingAlgorithm::BiweightPolyfit);
        assert!(o2.log_y);

        // The rect patch must hold ONLY the window — inherited values are
        // not frozen in. Changing the section default later flows through.
        let ov = &diff2.rect_overrides[0];
        assert_eq!(
            ov.patch,
            serde_json::json!({ "options": { "smoothing_window": 7 } }),
            "inherited values must not freeze into the rect patch"
        );
        let mut section2 = shown2.sections[0].clone();
        section2.chart_defaults = serde_json::json!({ "smoothing": "EmaPolyfit" });
        diff2.upsert_section_settings(&base, &section2);
        let shown3 = diff2.apply(&base);
        let o3 = opts_of(&shown3, "train/loss");
        assert_eq!(
            o3.smoothing,
            SmoothingAlgorithm::EmaPolyfit,
            "section change flows through"
        );
        assert_eq!(o3.smoothing_window, 7, "rect override survives");
    }

    #[test]
    fn user_rects_follow_the_same_cascade() {
        let base = base_with_one_rect();
        let mut diff = LayoutDiff::default();

        let mut project = RectOptions {
            log_y: true,
            ..RectOptions::default()
        };
        diff.set_project_chart_defaults(&project);
        let mut section = base.sections[0].clone();
        section.chart_defaults = serde_json::json!({ "smoothing_window": 42 });
        diff.upsert_section_settings(&base, &section);

        let mut user = base.find_rect("train/loss").unwrap().clone();
        user.id = "user-1".to_string();
        diff.add_rect(&base, "train", &user);

        let options = opts_of(&diff.apply(&base), "user-1");
        assert!(options.log_y, "project default reaches user rect");
        assert_eq!(
            options.smoothing_window, 42,
            "section default reaches user rect"
        );

        project.log_y = false;
        diff.set_project_chart_defaults(&project);
        section.chart_defaults = serde_json::json!({ "smoothing_window": 64 });
        diff.upsert_section_settings(&base, &section);
        let options = opts_of(&diff.apply(&base), "user-1");
        assert!(!options.log_y, "project changes keep flowing");
        assert_eq!(options.smoothing_window, 64, "section changes keep flowing");
    }

    #[test]
    fn user_rect_edits_are_sparse_overrides() {
        let base = base_with_one_rect();
        let mut diff = LayoutDiff::default();
        let mut section = base.sections[0].clone();
        section.chart_defaults = serde_json::json!({
            "smoothing": "EmaPolyfit",
            "smoothing_window": 42
        });
        diff.upsert_section_settings(&base, &section);

        let mut user = base.find_rect("train/loss").unwrap().clone();
        user.id = "user-1".to_string();
        diff.add_rect(&base, "train", &user);
        let mut edited = diff.apply(&base).find_rect("user-1").unwrap().clone();
        edited.options.smoothing_window = 7;
        diff.update_rect(&base, &edited);

        assert_eq!(
            diff.rect_overrides[0].patch,
            serde_json::json!({ "options": { "smoothing_window": 7 } })
        );
        section.chart_defaults = serde_json::json!({
            "smoothing": "BiweightPolyfit",
            "smoothing_window": 99
        });
        diff.upsert_section_settings(&base, &section);
        let options = opts_of(&diff.apply(&base), "user-1");
        assert_eq!(options.smoothing, SmoothingAlgorithm::BiweightPolyfit);
        assert_eq!(options.smoothing_window, 7, "rect override wins");

        diff.clear_rect_option(&base, "user-1", "smoothing_window");
        assert!(diff.rect_overrides.is_empty());
        assert_eq!(opts_of(&diff.apply(&base), "user-1").smoothing_window, 99);
    }

    #[test]
    fn editing_back_to_inherited_self_cleans() {
        let base = base_with_one_rect();
        let mut diff = LayoutDiff::default();
        let proj = RectOptions {
            smoothing_window: 99,
            ..Default::default()
        };
        diff.set_project_chart_defaults(&proj);

        // Rect explicitly set to 7, then back to the inherited 99.
        let shown = diff.apply(&base);
        let mut edited = shown.find_rect("train/loss").unwrap().clone();
        edited.options.smoothing_window = 7;
        diff.update_rect(&base, &edited);
        assert_eq!(diff.rect_overrides.len(), 1);
        edited.options.smoothing_window = 99;
        diff.update_rect(&base, &edited);
        assert!(
            diff.rect_overrides.is_empty(),
            "matching inherited value drops the patch"
        );
    }

    /// The section config dialog rebuilds its payload from an open-time
    /// snapshot, so once a defaults edit re-resolves the cascade, the
    /// payload's rects no longer match the displayed section. Settings
    /// intents must therefore never infer rect edits from their payload —
    /// a since-removed path that diffed payload rects against the displayed
    /// section pinned every chart to its pre-edit values as rect overrides
    /// ("save reverts my section defaults and marks every chart modified").
    #[test]
    fn section_dialog_flow_pins_no_rect_options() {
        let base = base_with_one_rect();
        let mut diff = LayoutDiff::default();

        // Dialog opens against the displayed layout; the payload below
        // carries this snapshot's rects (default cascade) unchanged.
        let snapshot = diff.apply(&base).sections[0].clone();
        let mut payload = snapshot.clone();
        payload.chart_defaults = serde_json::json!({ "smoothing": "EmaPolyfit" });

        // Live-apply tick, then Save — two records of the same intent.
        diff.upsert_section_settings(&base, &payload);
        diff.upsert_section_settings(&base, &payload);

        assert!(
            diff.rect_overrides.is_empty(),
            "a defaults edit must not freeze per-rect overrides"
        );
        let o = opts_of(&diff.apply(&base), "train/loss");
        assert_eq!(
            o.smoothing,
            SmoothingAlgorithm::EmaPolyfit,
            "new default reaches the chart"
        );
    }

    #[test]
    fn finer_overrides_list_and_clear_per_field() {
        let base = base_with_one_rect();
        let mut diff = LayoutDiff::default();

        // Section pins the algorithm; the rect pins its window and log_y.
        let mut section = base.sections[0].clone();
        section.chart_defaults = serde_json::json!({ "smoothing": "EmaPolyfit" });
        diff.upsert_section_settings(&base, &section);
        let shown = diff.apply(&base);
        let mut edited = shown.find_rect("train/loss").unwrap().clone();
        edited.options.smoothing_window = 7;
        edited.options.log_y = true;
        diff.update_rect(&base, &edited);

        // User-added rects use the same sparse override path.
        let mut extra = shown.find_rect("train/loss").unwrap().clone();
        extra.id = "user-1".to_string();
        diff.add_rect(&base, "train", &extra);
        let mut extra = diff.apply(&base).find_rect("user-1").unwrap().clone();
        extra.options.smoothing_window = 9;
        diff.update_rect(&base, &extra);

        // The project editor sees both levels; a section editor only its
        // own rects. Unlabeled rects list under their metric-name id.
        let shown = diff.apply(&base);
        let at_project = finer_overrides(&diff, &shown, None);
        assert_eq!(at_project.len(), 4);
        assert!(at_project.iter().any(|o| o.field == "smoothing"
            && o.target == OverrideTarget::Section("train".to_string())
            && o.value == serde_json::json!("EmaPolyfit")));
        let at_section = finer_overrides(&diff, &shown, Some("train"));
        assert_eq!(at_section.len(), 3);
        assert!(at_section.iter().any(|o| {
            o.target == OverrideTarget::Rect("user-1".to_string())
                && o.label == "user-1"
                && o.field == "smoothing_window"
        }));

        // Clearing one rect key re-inherits just that field and keeps the
        // other; clearing the last key drops the whole stored entry.
        diff.clear_rect_option(&base, "train/loss", "smoothing_window");
        let o = opts_of(&diff.apply(&base), "train/loss");
        assert_eq!(o.smoothing_window, default_smoothing_window());
        assert!(o.log_y, "other rect keys survive the clear");
        assert_eq!(diff.rect_overrides.len(), 2);
        diff.clear_rect_option(&base, "train/loss", "log_y");
        assert_eq!(diff.rect_overrides.len(), 1);
        diff.clear_rect_option(&base, "user-1", "smoothing_window");
        assert!(diff.rect_overrides.is_empty());

        // Clearing the section pin re-inherits the project level, and the
        // emptied defaults patch self-cleans out of the store.
        let proj = RectOptions {
            smoothing: SmoothingAlgorithm::EmaPolyfit,
            ..Default::default()
        };
        diff.set_project_chart_defaults(&proj);
        diff.clear_section_chart_default(&base, "train", "smoothing");
        let o = opts_of(&diff.apply(&base), "train/loss");
        assert_eq!(o.smoothing, SmoothingAlgorithm::EmaPolyfit);
        assert!(diff.section_overrides.is_empty());
    }

    #[test]
    fn patches_survive_roundtrip_and_old_diffs_load() {
        let mut diff = LayoutDiff::default();
        let proj = RectOptions {
            log_x: true,
            ..Default::default()
        };
        diff.set_project_chart_defaults(&proj);
        let json = serde_json::to_string(&diff).unwrap();
        let back: LayoutDiff = serde_json::from_str(&json).unwrap();
        assert_eq!(back, diff);
        // A pre-cascade diff (no project_chart_defaults key) still loads.
        let old: LayoutDiff = serde_json::from_str(r#"{"deleted_rects":["x"]}"#).unwrap();
        assert!(old.project_chart_defaults.is_null());
        // A pre-user_sections diff still loads; its whole-SectionConfig `added_sections` entries are dropped (accepted one-time loss).
        let old: LayoutDiff = serde_json::from_str(
            r#"{"added_sections":[{"name":"x","collapsed":false,"rects":[]}]}"#,
        )
        .unwrap();
        assert!(old.is_empty());
    }

    #[test]
    fn legacy_user_rects_migrate_to_sparse_overrides() {
        let base = base_with_one_rect();
        let mut legacy = LayoutDiff {
            format_version: 0,
            ..LayoutDiff::default()
        };

        let mut project = RectOptions {
            log_y: true,
            ..RectOptions::default()
        };
        legacy.set_project_chart_defaults(&project);
        let mut section = base.sections[0].clone();
        section.chart_defaults = serde_json::json!({
            "log_x": true,
            "smoothing_window": 42
        });
        legacy.upsert_section_settings(&base, &section);

        let mut user = base.find_rect("train/loss").unwrap().clone();
        user.id = "user-1".to_string();
        user.options.log_x = true;
        user.options.log_y = true;
        user.options.smoothing_window = 7;
        legacy.added_rects.push(AddedRect {
            section: "train".to_string(),
            rect: user,
        });
        legacy.rect_overrides.push(ConfigPatch {
            key: "user-1".to_string(),
            patch: serde_json::json!({ "label": "renamed" }),
        });

        legacy.migrate();
        assert_eq!(legacy.format_version, 1);
        assert_eq!(legacy.added_rects[0].rect.options, RectOptions::default());
        assert_eq!(
            legacy.rect_overrides[0].patch,
            serde_json::json!({
                "label": "renamed",
                "options": { "smoothing_window": 7 }
            }),
            "existing edits win while only options differing from the cascade stay pinned"
        );

        project.log_y = false;
        legacy.set_project_chart_defaults(&project);
        section.chart_defaults = serde_json::json!({ "smoothing_window": 42 });
        legacy.upsert_section_settings(&base, &section);
        let options = opts_of(&legacy.apply(&base), "user-1");
        assert!(!options.log_y, "matching legacy values become inherited");
        assert!(
            !options.log_x,
            "values matching a section default become inherited"
        );
        assert_eq!(options.smoothing_window, 7, "legacy customization survives");
        assert_eq!(
            legacy.apply(&base).find_rect("user-1").unwrap().label,
            "renamed",
            "an existing same-id override survives migration"
        );

        let json = serde_json::to_string(&legacy).unwrap();
        let mut reloaded: LayoutDiff = serde_json::from_str(&json).unwrap();
        let before_migrate = reloaded.clone();
        reloaded.migrate();
        assert_eq!(
            reloaded, before_migrate,
            "persisted migration is idempotent"
        );
    }
}

#[cfg(test)]
mod user_section_tests {
    use super::section_order_integration_tests::base as base_with_sections;
    use super::*;

    /// Base owning a "train" section with one rect, as auto-gen would build.
    fn base_with_train() -> LayoutConfig {
        LayoutConfig {
            sections: vec![SectionConfig::auto(
                "train".to_string(),
                vec![RectConfig {
                    id: "train/loss".to_string(),
                    label: String::new(),
                    bindings: vec![],
                    display_type: DisplayType::Numeric,
                    options: RectOptions::default(),
                }],
            )],
        }
    }

    fn user_rect(id: &str) -> RectConfig {
        RectConfig {
            id: id.to_string(),
            label: "custom".to_string(),
            bindings: vec![MetricBinding {
                project: ProjectRef::Current,
                runs: RunRef::Selected,
                metric_name: "custom/extra".to_string(),
            }],
            display_type: DisplayType::Numeric,
            options: RectOptions::default(),
        }
    }

    /// The navbar's add-section shape with sections collapsed by default: auto defaults, renamed, explicitly expanded.
    fn new_user_section(name: &str) -> SectionConfig {
        let mut section = SectionConfig::auto(name.to_string(), Vec::new());
        section.display_name = "New Section".to_string();
        section.set_collapsed(false, false);
        section
    }

    #[test]
    fn user_section_rides_generic_entries_and_self_cleans() {
        let base = LayoutConfig::auto_generate(&[]);
        let mut diff = LayoutDiff::default();
        diff.add_section(&base, &new_user_section("section-1-abc"));

        // Existence marker + sparse settings patch; no whole SectionConfig.
        assert_eq!(diff.user_sections, vec!["section-1-abc"]);
        assert_eq!(diff.section_overrides.len(), 1);

        let shown = diff.apply(&base);
        let s = shown.find_section("section-1-abc").expect("materialized");
        assert_eq!(s.display_name(), "New Section");
        assert!(!s.is_collapsed(false));

        // Rect intents ride added_rects, like charts added to any section.
        let rect = user_rect("rect-1");
        diff.add_rect(&base, "section-1-abc", &rect);
        let shown = diff.apply(&base);
        assert_eq!(shown.section_of_rect("rect-1"), Some("section-1-abc"));
        let mut edited = rect.clone();
        edited.label = "renamed".to_string();
        diff.update_rect(&base, &edited);
        assert_eq!(
            diff.apply(&base).find_rect("rect-1").unwrap().label,
            "renamed"
        );

        // Deleting a user rect needs no tombstone; deleting the section (base never owned the name) returns the store to empty.
        diff.delete_rect(&base, "rect-1");
        assert!(diff.deleted_rects.is_empty());
        diff.delete_section(&base, "section-1-abc");
        assert!(diff.is_empty());
    }

    #[test]
    fn user_section_defaults_reach_user_rects() {
        let base = LayoutConfig::auto_generate(&[]);
        let mut diff = LayoutDiff::default();
        let mut section = new_user_section("section-1-abc");
        section.chart_defaults = serde_json::json!({ "log_y": true });
        diff.add_section(&base, &section);
        diff.add_rect(&base, &section.name, &user_rect("rect-1"));

        let shown = diff.apply(&base);
        assert!(shown.find_rect("rect-1").unwrap().options.log_y);

        let mut edited = shown.find_rect("rect-1").unwrap().clone();
        edited.options.smoothing_window = 7;
        diff.update_rect(&base, &edited);
        section.chart_defaults = serde_json::json!({ "log_x": true });
        diff.upsert_section_settings(&base, &section);

        let shown = diff.apply(&base);
        let options = &shown.find_rect("rect-1").unwrap().options;
        assert!(options.log_x, "new section default flows through");
        assert!(!options.log_y, "removed section default re-inherits");
        assert_eq!(options.smoothing_window, 7, "rect override survives");
    }

    #[test]
    fn prefix_collision_merges_for_display_but_stores_no_base_rects() {
        let base = base_with_train();
        let mut diff = LayoutDiff::default();
        diff.add_section(&base, &new_user_section("train"));
        diff.add_rect(&base, "train", &user_rect("rect-1"));

        // Displayed: union of rects, base first, user settings applied.
        let shown = diff.apply(&base);
        let s = shown.find_section("train").unwrap();
        let ids: Vec<&str> = s.rects.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["train/loss", "rect-1"]);
        assert_eq!(s.display_name(), "New Section");
        assert!(!s.is_collapsed(false));

        // A settings save built from the displayed merged section (the collapse toggle, the section dialog) — AI-1359's trigger — must not copy base rects into the store.
        let mut payload = s.clone();
        payload.display_name = "Mine".to_string();
        diff.upsert_section_settings(&base, &payload);
        let json = serde_json::to_string(&diff).unwrap();
        assert!(
            !json.contains("train/loss"),
            "base rect leaked into the store: {json}"
        );
        assert_eq!(
            diff.apply(&base)
                .find_section("train")
                .unwrap()
                .display_name(),
            "Mine"
        );
    }

    #[test]
    fn collision_metrics_vanishing_leaves_only_the_user_half() {
        let with_metrics = base_with_train();
        let empty = LayoutConfig::auto_generate(&[]);
        let mut diff = LayoutDiff::default();
        diff.add_section(&with_metrics, &new_user_section("train"));
        diff.add_rect(&with_metrics, "train", &user_rect("rect-1"));
        // A settings save while merged, then the base metrics vanish.
        let payload = diff
            .apply(&with_metrics)
            .find_section("train")
            .unwrap()
            .clone();
        diff.upsert_section_settings(&with_metrics, &payload);

        let shown = diff.apply(&empty);
        let s = shown
            .find_section("train")
            .expect("user section survives the base");
        let ids: Vec<&str> = s.rects.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["rect-1"],
            "base rects must not resurrect from the store"
        );
        assert_eq!(s.display_name(), "New Section");

        // And the metrics returning restores the merge.
        let shown = diff.apply(&with_metrics);
        let s = shown.find_section("train").unwrap();
        let ids: Vec<&str> = s.rects.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["train/loss", "rect-1"]);
    }

    #[test]
    fn adding_a_base_owned_id_stores_no_entry() {
        let base = base_with_train();
        let mut diff = LayoutDiff::default();

        // Re-adding a deleted base chart: the intent is fully served by clearing the tombstone — no added_rects entry, store back to empty.
        diff.delete_rect(&base, "train/loss");
        let rect = base.find_rect("train/loss").unwrap().clone();
        diff.add_rect(&base, "train", &rect);
        assert!(diff.is_empty());

        // A base-owned id "added" into a DIFFERENT section must not store
        // either; rect ids identify charts globally, regardless of section.
        diff.add_rect(&base, "other", &rect);
        assert!(diff.added_rects.is_empty());
        let shown = diff.apply(&base);
        let copies = shown
            .sections
            .iter()
            .flat_map(|s| &s.rects)
            .filter(|r| r.id == "train/loss")
            .count();
        assert_eq!(copies, 1);
    }

    #[test]
    fn fresh_user_add_discards_a_dormant_same_id_override() {
        let base = LayoutConfig::auto_generate(&[]);
        let mut diff = LayoutDiff::default();
        diff.rect_overrides.push(ConfigPatch {
            key: "rect-1".to_string(),
            patch: serde_json::json!({ "label": "stale" }),
        });

        diff.add_section(&base, &new_user_section("mine"));
        diff.add_rect(&base, "mine", &user_rect("rect-1"));

        assert!(diff.rect_overrides.is_empty());
        assert_eq!(
            diff.apply(&base).find_rect("rect-1").unwrap().label,
            "custom"
        );
    }

    #[test]
    fn base_rect_added_later_wins_globally() {
        let empty = LayoutConfig::auto_generate(&[]);
        let mut diff = LayoutDiff::default();
        diff.add_section(&empty, &new_user_section("mine"));
        diff.add_rect(&empty, "mine", &user_rect("train/loss"));
        let mut edited = diff.apply(&empty).find_rect("train/loss").unwrap().clone();
        edited.label = "renamed".to_string();
        diff.update_rect(&empty, &edited);

        // Discovery later supplies the same globally keyed id in another
        // section. The base owner replaces the saved creation base, while
        // the user's sparse edit remains attached to that identity.
        let shown = diff.apply(&base_with_train());
        let copies: Vec<(&str, &RectConfig)> = shown
            .sections
            .iter()
            .flat_map(|s| s.rects.iter().map(move |r| (s.name.as_str(), r)))
            .filter(|(_, r)| r.id == "train/loss")
            .collect();
        assert_eq!(copies.len(), 1);
        assert_eq!(copies[0].0, "train");
        assert_eq!(copies[0].1.label, "renamed");
    }

    #[test]
    fn deleting_a_merged_section_tombstones_the_base_half() {
        let base = base_with_train();
        let mut diff = LayoutDiff::default();
        diff.add_section(&base, &new_user_section("train"));
        diff.add_rect(&base, "train", &user_rect("rect-1"));
        diff.delete_section(&base, "train");
        assert!(diff.user_sections.is_empty());
        assert!(diff.added_rects.is_empty());
        assert_eq!(diff.deleted_sections, vec!["train"]);
        assert!(diff.apply(&base).find_section("train").is_none());
    }

    #[test]
    fn untoggled_sections_follow_the_expand_setting_and_toggles_stay_sparse() {
        let base = base_with_train();
        let catch_all = SectionConfig::auto(String::new(), Vec::new());
        let mut train = base.find_section("train").unwrap().clone();
        assert!(train.is_collapsed(false) && !catch_all.is_collapsed(false));
        assert!(!train.is_collapsed(true) && !catch_all.is_collapsed(true));

        // Opening under the collapsed default stores the same bool patch older builds wrote, which keeps it open whatever the setting.
        let mut diff = LayoutDiff::default();
        train.set_collapsed(false, false);
        diff.upsert_section_settings(&base, &train);
        assert_eq!(
            diff.section_overrides[0].patch,
            serde_json::json!({ "collapsed": false })
        );
        let shown = diff.apply(&base);
        assert!(!shown.find_section("train").unwrap().is_collapsed(false));

        // Toggling back onto the default removes the entry.
        train.set_collapsed(true, false);
        diff.upsert_section_settings(&base, &train);
        assert!(diff.is_empty());

        // The mirror image: closing under the visible default stores `true`.
        train.set_collapsed(true, true);
        diff.upsert_section_settings(&base, &train);
        let shown = diff.apply(&base);
        let shown = shown.find_section("train").unwrap();
        assert!(shown.is_collapsed(true) && shown.is_collapsed(false));
        train.set_collapsed(false, true);
        diff.upsert_section_settings(&base, &train);
        assert!(diff.is_empty());
    }

    fn section_patch<'a>(diff: &'a LayoutDiff, name: &str) -> Option<&'a Value> {
        diff.section_overrides
            .iter()
            .find(|ov| ov.key == name)
            .map(|ov| &ov.patch)
    }

    #[test]
    fn bulk_collapse_records_only_the_named_sections_that_change() {
        let base = base_with_sections(&["", "train", "eval", "system", "old"]);
        let names = ["", "train", "eval", "old"].map(String::from);
        let mut diff = LayoutDiff::default();
        let mut eval = base.find_section("eval").unwrap().clone();
        eval.chart_height = 400;
        diff.upsert_section_settings(&base, &eval);
        diff.delete_section(&base, "old");

        // Under the visible default every named section is open; "system" is out of scope, and the deleted "old" gets no patch.
        assert!(diff.set_sections_collapsed(&base, &names, true, true));
        let collapsed = serde_json::json!({ "collapsed": true });
        assert_eq!(section_patch(&diff, ""), Some(&collapsed));
        assert_eq!(section_patch(&diff, "train"), Some(&collapsed));
        assert_eq!(
            section_patch(&diff, "eval"),
            Some(&serde_json::json!({ "chart_height": 400, "collapsed": true }))
        );
        assert_eq!(section_patch(&diff, "system"), None);
        assert_eq!(section_patch(&diff, "old"), None);
        let saved = diff.clone();
        assert!(!diff.set_sections_collapsed(&base, &names, true, true));
        assert_eq!(diff, saved);

        // Expanding under the same default removes the collapse patches and keeps the other edit.
        assert!(diff.set_sections_collapsed(&base, &names, false, true));
        assert_eq!(diff.section_overrides.len(), 1);
        assert_eq!(
            section_patch(&diff, "eval"),
            Some(&serde_json::json!({ "chart_height": 400 }))
        );
    }

    #[test]
    fn bulk_collapse_keeps_an_explicit_override_already_in_place() {
        let base = base_with_sections(&["", "train", "eval"]);
        let names = ["", "train", "eval"].map(String::from);
        let mut diff = LayoutDiff::default();
        // Closed under the visible default, so it stays closed if that default returns.
        let mut train = base.find_section("train").unwrap().clone();
        train.set_collapsed(true, true);
        diff.upsert_section_settings(&base, &train);

        // Under the collapsed default only the catch-all is open.
        assert!(diff.set_sections_collapsed(&base, &names, true, false));
        let collapsed = serde_json::json!({ "collapsed": true });
        assert_eq!(section_patch(&diff, ""), Some(&collapsed));
        assert_eq!(section_patch(&diff, "train"), Some(&collapsed));
        assert_eq!(section_patch(&diff, "eval"), None);
        assert!(diff
            .apply(&base)
            .sections
            .iter()
            .all(|s| s.is_collapsed(false)));
    }

    #[test]
    fn specific_binding_keeps_an_absent_base_rect_editable() {
        let base = LayoutConfig::auto_generate(&[MetricInfo {
            metric_name: "train/loss".to_string(),
            metric_type: MetricType::Numeric as i32,
        }]);
        let mut diff = LayoutDiff::default();
        let mut rect = base.find_rect("train/loss").unwrap().clone();
        rect.label = "Pinned loss".to_string();
        rect.bindings[0].runs = RunRef::Specific(vec!["deleted-run".to_string()]);
        diff.update_rect(&base, &rect);

        let empty = LayoutConfig::auto_generate(&[]);
        let shown = diff.apply(&empty);
        let recovered = shown
            .find_rect("train/loss")
            .expect("specific override survives");
        assert_eq!(recovered.label, "Pinned loss");
        assert_eq!(recovered.bindings, rect.bindings);

        let mut edited_again = recovered.clone();
        edited_again.label = "Still editable".to_string();
        diff.update_rect(&empty, &edited_again);
        assert_eq!(
            diff.apply(&empty).find_rect("train/loss").unwrap().label,
            "Still editable"
        );
    }

    #[test]
    fn missing_rect_recovery_does_not_wake_unrelated_stale_overrides() {
        let base = LayoutConfig::auto_generate(&[
            MetricInfo {
                metric_name: "images/sample".to_string(),
                metric_type: MetricType::Cdn as i32,
            },
            MetricInfo {
                metric_name: "train/loss".to_string(),
                metric_type: MetricType::Numeric as i32,
            },
        ]);
        let mut diff = LayoutDiff::default();

        let mut pinned = base.find_rect("images/sample").unwrap().clone();
        pinned.bindings[0].runs = RunRef::Specific(vec!["deleted-run".to_string()]);
        diff.update_rect(&base, &pinned);

        let mut ordinary = base.find_rect("train/loss").unwrap().clone();
        ordinary.label = "Dormant label".to_string();
        diff.update_rect(&base, &ordinary);

        let shown = diff.apply(&LayoutConfig::auto_generate(&[]));
        assert!(shown.find_rect("images/sample").is_some());
        assert!(shown.find_rect("train/loss").is_none());
    }

    #[test]
    fn missing_specific_rect_keeps_id_derived_axis_defaults() {
        let base = LayoutConfig::auto_generate(&[MetricInfo {
            metric_name: "system/gpu/util".to_string(),
            metric_type: MetricType::Numeric as i32,
        }]);
        let mut diff = LayoutDiff::default();
        let mut rect = base.find_rect("system/gpu/util").unwrap().clone();
        rect.bindings[0].runs = RunRef::Specific(vec!["deleted-run".to_string()]);
        diff.update_rect(&base, &rect);

        let effective = diff.base_with_specific_rects(&LayoutConfig::auto_generate(&[]));
        assert!(matches!(
            effective
                .find_rect("system/gpu/util")
                .unwrap()
                .options
                .x_axis_mode,
            XAxisMode::RelativeTime,
        ));
    }
}

fn diff_key(project_id: &str) -> String {
    format!("kymo_layout_diff_{}", project_id)
}

fn legacy_diff_key(project_id: &str) -> String {
    format!("mkdb2_layout_diff_{}", project_id)
}

/// Pre-diff-era key that held the user's full saved layout (their edits
/// included, not just a cache). Deliberately dropped without migration when
/// the diff format shipped — an accepted one-time loss of old customizations.
fn legacy_key(project_id: &str) -> String {
    format!("mkdb2_layout_{}", project_id)
}

fn is_legacy_layout(json: &str) -> bool {
    serde_json::from_str::<LayoutConfig>(json).is_ok()
}

/// Ownership of the legacy diff key, which overlaps another project's pre-diff full-layout key (`legacy_key("diff_x") == legacy_diff_key("x")`): anything that is not a full-layout blob is ours. Readability is deliberately not required — a corrupt or newer-format legacy diff is still ours, and Reset must be able to clear it or the settings-error page becomes inescapable (load_strict reads the same set).
fn is_owned_legacy_diff(json: &str) -> bool {
    !is_legacy_layout(json)
}

fn remove_owned_legacy(project_id: &str) {
    let key = legacy_key(project_id);
    if local_storage::get(&key)
        .as_deref()
        .is_some_and(is_legacy_layout)
    {
        local_storage::remove(&key);
    }
}

fn remove_owned_legacy_diff(project_id: &str) {
    let key = legacy_diff_key(project_id);
    if local_storage::get(&key)
        .as_deref()
        .is_some_and(is_owned_legacy_diff)
    {
        local_storage::remove(&key);
    }
}

impl LayoutDiff {
    /// Add back only base charts whose saved bindings explicitly identify a
    /// run. Ordinary overrides for vanished metrics remain dormant. The
    /// Numeric type is only a construction fallback: Specific bindings
    /// trigger registry type detection in `MetricRect`.
    pub(crate) fn base_with_specific_rects(&self, base: &LayoutConfig) -> LayoutConfig {
        let mut effective = base.clone();
        for ov in &self.rect_overrides {
            if effective.find_rect(&ov.key).is_some()
                || self.added_rects.iter().any(|add| add.rect.id == ov.key)
                || self.deleted_rects.iter().any(|id| id == &ov.key)
                || !ov.keeps_missing_rect()
            {
                continue;
            }
            let synthetic = MetricInfo {
                metric_name: ov.key.clone(),
                metric_type: MetricType::Numeric as i32,
            };
            let generated = LayoutConfig::auto_generate(&[synthetic]);
            let Some(generated_section) = generated.sections.into_iter().next() else {
                continue;
            };
            if let Some(section) = effective.find_section_mut(&generated_section.name) {
                section.rects.extend(generated_section.rects);
                section.rects.sort_by(|a, b| natural_cmp(&a.id, &b.id));
            } else {
                effective.sections.push(generated_section);
            }
        }
        effective
    }

    pub fn is_empty(&self) -> bool {
        self.section_order.is_empty()
            && *self
                == Self {
                    format_version: self.format_version,
                    section_order: self.section_order.clone(),
                    ..Self::default()
                }
    }

    /// Upgrade whole-stored user rects to the shared sparse-override model.
    /// Values equal to the current cascade start inheriting; differing
    /// values remain pinned. The creation base resets to library defaults,
    /// matching the base assigned to charts created by the current UI.
    /// Loading deliberately does not persist this conversion: reads stay
    /// pure and an unedited user retains a rollback-compatible v0 blob; the
    /// next edit saves the already-migrated representation.
    fn migrate(&mut self) {
        if self.format_version != 0 {
            return;
        }

        let empty_base = LayoutConfig::auto_generate(&[]);
        for i in 0..self.added_rects.len() {
            let add = self.added_rects[i].clone();
            let mut anchor = add.rect.clone();
            anchor.options =
                self.inherited_options(&empty_base, &add.section, &RectOptions::default());
            let legacy_patch = ConfigPatch::between(&add.rect.id, &anchor, &add.rect);

            self.added_rects[i].rect.options = RectOptions::default();
            if let Some(mut legacy_patch) = legacy_patch {
                if let Some(pos) = self
                    .rect_overrides
                    .iter()
                    .position(|ov| ov.key == add.rect.id)
                {
                    let existing = self.rect_overrides.remove(pos);
                    merge_apply(&mut legacy_patch.patch, &existing.patch);
                }
                self.rect_overrides.push(legacy_patch);
            }
        }
        self.format_version = 1;
    }

    /// Rect state before its own sparse edit: a live/synthetic auto-gen
    /// base or a user-added creation base, with project and section
    /// defaults applied. This is the single anchor used by editing,
    /// reset-to-inherited UI, and persistence.
    fn rect_edit_anchor(&self, base: &LayoutConfig, id: &str) -> Option<RectConfig> {
        let effective_base = self.base_with_specific_rects(base);
        let (mut rect, section) = match effective_base.find_rect(id) {
            Some(rect) => (
                rect.clone(),
                effective_base.section_of_rect(id)?.to_string(),
            ),
            None => {
                let add = self.added_rects.iter().find(|add| add.rect.id == id)?;
                (add.rect.clone(), add.section.clone())
            }
        };
        rect.options = self.inherited_options(&effective_base, &section, &rect.options);
        Some(rect)
    }

    pub(crate) fn inherited_options_for_rect(
        &self,
        base: &LayoutConfig,
        id: &str,
    ) -> Option<RectOptions> {
        self.rect_edit_anchor(base, id).map(|rect| rect.options)
    }

    /// Record the user's settings for one section — one path for base-derived and user-created alike. Recomputes that single entry; removing it when the settings match the anchor keeps reverts self-cleaning. `section_settings` strips `rects`, so a payload built from displayed state (which for a prefix-collided user section carries base rects) can never store rect content, only settings.
    pub fn upsert_section_settings(&mut self, base: &LayoutConfig, section: &SectionConfig) {
        let anchor = section_anchor(base, &section.name);
        let patch = ConfigPatch::between_values(
            &section.name,
            &section_settings(&anchor),
            &section_settings(section),
        );
        self.section_overrides.retain(|ov| ov.key != section.name);
        if let Some(patch) = patch {
            self.section_overrides.push(patch);
        }
    }

    /// Set the named sections to `collapsed`, recording each one that changes as its header toggle would; one already there keeps its override, and names gone from the layout are skipped. Returns whether anything changed.
    pub fn set_sections_collapsed(
        &mut self,
        base: &LayoutConfig,
        names: &[String],
        collapsed: bool,
        sections_visible: bool,
    ) -> bool {
        let mut changed = false;
        for mut section in self.materialized_sections(base).sections {
            if names.contains(&section.name) && section.is_collapsed(sections_visible) != collapsed
            {
                section.set_collapsed(collapsed, sections_visible);
                self.upsert_section_settings(base, &section);
                changed = true;
            }
        }
        changed
    }

    /// Record deletion of one section, dropping the user content and per-rect entries it carried; base-derived sections get a tombstone (which also keeps them deleted if their metrics vanish and return).
    /// For a pure user section removing the marker and its content IS the deletion, but a name the base also owns (prefix collision) is tombstoned too — the user deleted the merged section they saw, not just their half of it.
    pub fn delete_section(&mut self, base: &LayoutConfig, name: &str) {
        // Capture live anchor coordinates before settings/tombstones disappear,
        // including the pure-user-section early-return path below.
        let current = self.materialized_sections(base);
        let context = self.order_context(&current);
        let previous = self.section_order.parsed(&self.deleted_sections);
        let reordered = context.delete_anchor(name);
        if reordered != previous {
            self.section_order.replace(reordered);
        }
        let effective_base = self.base_with_specific_rects(base);
        let base_section = effective_base.find_section(name);
        let was_user = self.user_sections.iter().any(|s| s == name);
        let mut doomed_rect_ids: HashSet<String> = self
            .added_rects
            .iter()
            .filter(|add| add.section == name)
            .map(|add| add.rect.id.clone())
            .collect();
        if let Some(base_section) = base_section {
            // Only base ids lose stale tombstones; widening this to every
            // doomed id could uncover a same-id base rect elsewhere.
            self.deleted_rects
                .retain(|id| !base_section.rects.iter().any(|rect| rect.id == *id));
            doomed_rect_ids.extend(base_section.rects.iter().map(|rect| rect.id.clone()));
        }
        self.user_sections.retain(|s| s != name);
        self.section_overrides.retain(|ov| ov.key != name);
        self.added_rects.retain(|a| a.section != name);
        self.rect_overrides
            .retain(|ov| !doomed_rect_ids.contains(&ov.key));
        if was_user && base_section.is_none() {
            return;
        }
        if !self.deleted_sections.iter().any(|d| d == name) {
            self.deleted_sections.push(name.to_string());
        }
    }

    /// Record a new user-created section: an existence marker plus the same sparse settings patch any section edit stores. Initial rects (none in the current UI) ride `added_rects` like any other user chart.
    pub fn add_section(&mut self, base: &LayoutConfig, section: &SectionConfig) {
        self.deleted_sections.retain(|d| d != &section.name);
        if !self.user_sections.contains(&section.name) {
            self.user_sections.push(section.name.clone());
        }
        for rect in &section.rects {
            self.add_rect(base, &section.name, rect);
        }
        self.upsert_section_settings(base, section);
    }

    /// Record an edit to one rect, wherever it lives, as a sparse patch
    /// against its inherited project/section resolution.
    pub fn update_rect(&mut self, base: &LayoutConfig, rect: &RectConfig) {
        // The user is editing a rect they can see, so a tombstone for it
        // (e.g. a concurrent tab's delete) must not leave the edit dormant —
        // mirror add_rect's stale-tombstone clearing.
        self.deleted_rects.retain(|d| d != &rect.id);
        // Specific-binding overrides can outlive discovery, and user-added
        // rects have their own creation bases. `rect_edit_anchor` handles
        // both without separate persistence paths.
        if let Some(anchor) = self.rect_edit_anchor(base, &rect.id) {
            let patch = ConfigPatch::between(&rect.id, &anchor, rect);
            self.rect_overrides.retain(|ov| ov.key != rect.id);
            if let Some(patch) = patch {
                self.rect_overrides.push(patch);
            }
            return;
        }
        warn(&format!(
            "[layout] update for unknown rect {} dropped",
            rect.id
        ));
    }

    /// Record deletion of one rect: entry removal for user-added rects, a
    /// tombstone otherwise (which keeps a temporarily absent metric deleted
    /// when it returns).
    pub fn delete_rect(&mut self, base: &LayoutConfig, id: &str) {
        self.rect_overrides.retain(|ov| ov.key != id);
        let mut was_user_rect = false;
        if let Some(pos) = self.added_rects.iter().position(|a| a.rect.id == id) {
            self.added_rects.remove(pos);
            was_user_rect = true;
        }
        // A user entry can shadow a base rect with the same id (apply's
        // dedup renders only one copy). Removing just the entry would leave
        // the base copy on screen, making the delete look ignored — the
        // user deleted the chart they saw, whoever owns it, so tombstone
        // whenever the base also provides the id.
        if (!was_user_rect || base.find_rect(id).is_some())
            && !self.deleted_rects.iter().any(|d| d == id)
        {
            self.deleted_rects.push(id.to_string());
        }
    }

    /// Record a user-added chart in `section` — base-derived or user-created, `added_rects` carries both.
    pub fn add_rect(&mut self, base: &LayoutConfig, section: &str, rect: &RectConfig) {
        // A stale tombstone for this id must not eat the new rect.
        self.deleted_rects.retain(|d| d != &rect.id);
        // A base-provided id never renders from added_rects (apply's dedup keeps the base copy), so an entry for it would be dead weight that resurrects with frozen options when the metric vanishes — for base-owned ids the add intent is fully served by the tombstone clear above.
        if base.find_rect(&rect.id).is_some() {
            return;
        }
        // A new creation intent supersedes any dormant edit for a prior
        // same-id user rect. Keep this outside the match so insert and
        // replacement cannot drift into different reset semantics.
        self.rect_overrides.retain(|ov| ov.key != rect.id);
        match self.added_rects.iter_mut().find(|a| a.rect.id == rect.id) {
            Some(entry) => {
                entry.section = section.to_string();
                entry.rect = rect.clone();
            }
            None => self.added_rects.push(AddedRect {
                section: section.to_string(),
                rect: rect.clone(),
            }),
        }
    }

    /// A section's settings as this diff leaves them: its anchor with this diff's settings override applied.
    fn resolved_section(&self, base: &LayoutConfig, name: &str) -> SectionConfig {
        let mut sec = section_anchor(base, name);
        for ov in self.section_overrides.iter().filter(|o| o.key == name) {
            ov.apply_to(&mut sec);
        }
        sec
    }

    /// The section-level chart-defaults patch as this diff leaves it.
    pub fn section_chart_defaults(&self, base: &LayoutConfig, name: &str) -> Value {
        self.resolved_section(base, name).chart_defaults
    }

    /// The options a rect inherits before its own edits: its auto-generated
    /// options with the project and section defaults cascaded in. Rect
    /// editors diff against this, so values a chart merely inherits never
    /// freeze into its own override patch — later section/project changes
    /// keep flowing through to it.
    pub fn inherited_options(
        &self,
        base: &LayoutConfig,
        section: &str,
        rect_base: &RectOptions,
    ) -> RectOptions {
        cascade_options(
            rect_base,
            &[
                &self.project_chart_defaults,
                &self.section_chart_defaults(base, section),
            ],
        )
    }

    /// Store the project-level defaults as a patch against the library
    /// defaults; matching them exactly clears the level.
    pub fn set_project_chart_defaults(&mut self, opts: &RectOptions) {
        self.project_chart_defaults = options_patch_between(&RectOptions::default(), opts);
    }

    /// Remove one field from `name`'s chart-defaults patch so it re-inherits from the project level, re-recording through [`Self::upsert_section_settings`] so entry self-cleaning comes for free.
    pub fn clear_section_chart_default(&mut self, base: &LayoutConfig, name: &str, field: &str) {
        let mut sec = self.resolved_section(base, name);
        let Value::Object(map) = &mut sec.chart_defaults else {
            return;
        };
        if map.remove(field).is_none() {
            return;
        }
        if map.is_empty() {
            // Null is the anchor's "nothing set" form — clearing the last key must not pin `{}`.
            sec.chart_defaults = Value::Null;
        }
        self.upsert_section_settings(base, &sec);
    }

    /// Remove one options field from rect `id`'s override patch so it re-inherits: set the displayed rect's field back to its inherited value and re-record through [`Self::update_rect`], whose anchor diff drops the now-matching key (and the emptied entry).
    pub fn clear_rect_option(&mut self, base: &LayoutConfig, id: &str, field: &str) {
        let Some(anchor) = self.rect_edit_anchor(base, id) else {
            return;
        };
        let Some(mut rect) = self.apply(base).find_rect(id).cloned() else {
            return;
        };
        let inh =
            serde_json::to_value(&anchor.options).expect("RectOptions serializes to a JSON object");
        let Some(value) = inh.get(field).cloned() else {
            return;
        };
        rect.options = cascade_options(&rect.options, &[&serde_json::json!({ field: value })]);
        self.update_rect(base, &rect);
    }

    /// Resolve section existence and settings without cascading every chart's
    /// options. Ordering and deletion only need this stage of the layout.
    fn materialized_sections(&self, base: &LayoutConfig) -> LayoutConfig {
        let mut layout = self.base_with_specific_rects(base);
        layout
            .sections
            .retain(|s| !self.deleted_sections.contains(&s.name));
        // Materialize the sections user content needs *before* the override pass below, so saved settings apply to resurrected and user-created sections alike (a stub built afterwards would silently drop them).
        // A user-created name the base also owns (prefix collision) needs no arm of its own: the base section stands where the stub would, the settings patch applies to it below, and the user's charts arrive through `added_rects` with the usual dedup — the union the UI shows, base rects first.
        for name in self
            .user_sections
            .iter()
            .chain(self.added_rects.iter().map(|a| &a.section))
        {
            if layout.find_section(name).is_none() {
                layout
                    .sections
                    .push(SectionConfig::auto(name.clone(), Vec::new()));
            }
        }
        for ov in &self.section_overrides {
            if let Some(s) = layout.find_section_mut(&ov.key) {
                ov.apply_to(s);
            }
        }
        layout
    }

    /// Apply this diff on top of a freshly auto-generated base. Overrides
    /// and deletions for metrics that no longer exist drop out silently,
    /// except an override whose bindings explicitly retain a Specific run.
    pub fn apply(&self, base: &LayoutConfig) -> LayoutConfig {
        let mut layout = self.materialized_sections(base);
        // Add user rect bases before resolving options so every rect takes
        // the same project → section → rect path. Base-owned ids win the
        // global defensive dedup, preserving the one-id/one-rect invariant
        // used by lookup and sparse overrides.
        for add in &self.added_rects {
            if layout.find_rect(&add.rect.id).is_some() {
                continue;
            }
            if let Some(s) = layout.find_section_mut(&add.section) {
                s.rects.push(add.rect.clone());
            }
        }
        let deleted_rects: HashSet<&str> = self.deleted_rects.iter().map(String::as_str).collect();
        for s in &mut layout.sections {
            s.rects.retain(|r| !deleted_rects.contains(r.id.as_str()));
            let section_defaults = s.chart_defaults.clone();
            for r in &mut s.rects {
                // Project → section → rect cascade: the coarse defaults
                // slide in under the rect's own sparse override patch, which
                // applies last and wins.
                r.options = cascade_options(
                    &r.options,
                    &[&self.project_chart_defaults, &section_defaults],
                );
                if let Some(ov) = self.rect_overrides.iter().find(|o| o.key == r.id) {
                    ov.apply_to(r);
                }
            }
        }
        let order = self.order_context(&layout).project();
        let positions: HashMap<_, _> = order
            .iter()
            .enumerate()
            .map(|(i, name)| (name.as_str(), i))
            .collect();
        layout
            .sections
            .sort_by_key(|section| positions[section.name.as_str()]);
        layout
    }

    fn order_context(&self, live: &LayoutConfig) -> OrderContext {
        let entries = self.section_order.parsed(&self.deleted_sections);
        let live_keys = live.sections.iter().map(SectionKey::from).collect();
        let names: HashSet<_> = entries
            .iter()
            .flat_map(|entry| [entry.id.as_str(), entry.at.name()])
            .filter(|name| live.find_section(name).is_none())
            .collect();
        let resolved = names
            .into_iter()
            .map(|name| {
                let section = self.resolved_section(live, name);
                SectionKey::from(&section)
            })
            .collect();
        OrderContext::new(live_keys, entries, resolved)
    }

    /// Validate a captured gap against current discovery AND freshly loaded
    /// storage. Returning false still requires the funnel to publish apply(base).
    pub fn reorder_section(&mut self, base: &LayoutConfig, source: &str, gap: &SectionGap) -> bool {
        let current = self.materialized_sections(base);
        let context = self.order_context(&current);
        let Some(index) = gap.index(&context.project(), source) else {
            return false;
        };
        let Some(entries) = context.rebuild(source, index) else {
            return false;
        };
        if entries == self.section_order.parsed(&self.deleted_sections) {
            return false;
        }
        self.section_order.replace(entries);
        true
    }

    pub(crate) fn storage_version(&self) -> u8 {
        if self.section_order.is_empty() {
            1
        } else {
            2
        }
    }

    /// Persist; an empty diff clears storage so reverting all edits returns
    /// the project to pure auto-generation.
    pub fn save(&self, project_id: &str) {
        let key = diff_key(project_id);
        if self.is_empty() {
            local_storage::remove(&key);
            remove_owned_legacy_diff(project_id);
            remove_owned_legacy(project_id);
            return;
        }
        let written = Self {
            format_version: self.storage_version(),
            section_order: self.section_order.for_save(&self.deleted_sections),
            ..self.clone()
        };
        if let Ok(json) = serde_json::to_string(&written) {
            if local_storage::set(&key, &json) {
                // The first successful branded-key write also supersedes the
                // pre-diff full-layout blob. Reads remain pure so untouched
                // rollback state survives an ordinary visit.
                remove_owned_legacy_diff(project_id);
                remove_owned_legacy(project_id);
            }
        }
    }

    /// Distinguishes "no saved diff" from "saved but unparsable" so callers
    /// can route to a settings-error page rather than silently regenerating.
    pub fn load_strict(project_id: &str) -> LoadResult {
        let json = local_storage::get(&diff_key(project_id)).or_else(|| {
            local_storage::get(&legacy_diff_key(project_id))
                .filter(|value| !is_legacy_layout(value))
        });
        let Some(json) = json else {
            return LoadResult::Missing;
        };
        match parse_layout_diff(&json) {
            Ok(diff) => LoadResult::Loaded(diff),
            Err(error) => LoadResult::Corrupt(error),
        }
    }

    pub fn clear(project_id: &str) {
        local_storage::remove(&diff_key(project_id));
        remove_owned_legacy_diff(project_id);
        remove_owned_legacy(project_id);
    }
}

#[cfg(test)]
mod storage_key_tests {
    use super::{
        is_legacy_layout, is_owned_legacy_diff, legacy_diff_key, legacy_key, parse_layout_diff,
        LayoutConfig, LayoutDiff,
    };

    #[test]
    fn legacy_cleanup_cannot_remove_another_projects_current_diff() {
        assert_eq!(legacy_diff_key("project"), legacy_key("diff_project"));

        let legacy = serde_json::to_string(&LayoutConfig {
            sections: Vec::new(),
        })
        .unwrap();
        let mut current = LayoutDiff::default();
        current.deleted_sections.push("charts".into());
        let current = serde_json::to_string(&current).unwrap();

        assert!(is_legacy_layout(&legacy));
        assert!(!is_owned_legacy_diff(&legacy));
        assert!(!is_legacy_layout(&current));
        assert!(is_owned_legacy_diff(&current));
        // Corrupt or newer-format legacy diffs are still ours: Reset must clear them.
        assert!(!is_legacy_layout("not json"));
        assert!(is_owned_legacy_diff("not json"));
        assert!(is_owned_legacy_diff(
            r#"{"format_version":3,"future_state":{"must_survive":true}}"#
        ));
    }

    #[test]
    fn newer_layout_formats_are_rejected_before_unknown_fields_can_be_lost() {
        let current = serde_json::to_string(&LayoutDiff::default()).unwrap();
        assert_eq!(parse_layout_diff(&current).unwrap().format_version, 1);

        let migrated = parse_layout_diff(r#"{"format_version":0}"#).unwrap();
        assert_eq!(migrated.format_version, 1);

        let error =
            parse_layout_diff(r#"{"format_version":3,"future_state":{"must_survive":true}}"#)
                .unwrap_err();
        assert!(error.contains("format 3 is newer than this build"));
    }
}

#[cfg(test)]
mod section_order_integration_tests {
    use super::*;
    use crate::state::section_order::{Anchor, Placement};

    pub(super) fn base(names: &[&str]) -> LayoutConfig {
        LayoutConfig {
            sections: names
                .iter()
                .map(|name| SectionConfig::auto((*name).into(), vec![]))
                .collect(),
        }
    }

    fn names(layout: &LayoutConfig) -> Vec<&str> {
        layout.sections.iter().map(|s| s.name.as_str()).collect()
    }

    fn placement(id: &str, anchor: &str) -> Placement {
        Placement {
            id: id.into(),
            at: Anchor::After(anchor.into()),
        }
    }

    #[test]
    fn drop_round_trip_noop_and_restoring_default_selects_v1() {
        let base = base(&["A", "B", "C"]);
        let mut diff = LayoutDiff::default();
        assert!(diff.reorder_section(
            &base,
            "C",
            &SectionGap {
                predecessor: None,
                successor: Some("A".into())
            }
        ));
        assert_eq!(names(&diff.apply(&base)), ["C", "A", "B"]);
        assert_eq!(diff.storage_version(), 2);
        let original = diff.clone();
        assert!(!diff.reorder_section(
            &base,
            "C",
            &SectionGap {
                predecessor: None,
                successor: Some("A".into())
            }
        ));
        assert_eq!(diff, original);
        assert!(diff.reorder_section(
            &base,
            "C",
            &SectionGap {
                predecessor: Some("B".into()),
                successor: None
            }
        ));
        assert_eq!(names(&diff.apply(&base)), ["A", "B", "C"]);
        assert_eq!(diff.storage_version(), 1);
        assert!(diff.is_empty());
    }

    #[test]
    fn hidden_anchor_resolves_all_fresh_settings_and_never_materializes_it() {
        let all = base(&["A", "B", "C", "D"]);
        let hidden = base(&["A", "C", "D"]);
        let mut diff = parse_layout_diff(r#"{"format_version":2,"section_order":[{"id":"C","at":{"after":"B"}}],"section_overrides":[{"key":"B","patch":{"display_name":"AA"}},{"key":"B","patch":{"display_name":"Z"}}]}"#).unwrap();
        assert_eq!(names(&diff.apply(&hidden)), ["A", "D", "C"]);
        assert_eq!(names(&diff.apply(&all)), ["A", "D", "B", "C"]);
        let original = diff.section_order.clone();
        assert!(diff.reorder_section(
            &hidden,
            "D",
            &SectionGap {
                predecessor: None,
                successor: Some("A".into())
            }
        ));
        assert!(diff
            .section_order
            .parsed(&[])
            .contains(&placement("C", "B")));
        assert_eq!(original.parsed(&[]), vec![placement("C", "B")]);
        assert_eq!(names(&diff.apply(&all)), ["D", "A", "B", "C"]);
    }

    #[test]
    fn malformed_order_survives_settings_and_noops_then_commits_on_real_move() {
        let base = base(&["A", "B", "C"]);
        let raw = serde_json::json!([{"id":"C","at":{"before":"A"}}, {"bad":true}, {"id":"C","at":{"after":"B"}}]);
        let mut diff = parse_layout_diff(
            &serde_json::json!({"format_version":2,"section_order":raw}).to_string(),
        )
        .unwrap();
        assert_eq!(names(&diff.apply(&base)), ["C", "A", "B"]);
        let mut a = base.sections[0].clone();
        a.max_columns = 2;
        diff.upsert_section_settings(&base, &a);
        assert_eq!(
            serde_json::to_value(diff.section_order.for_save(&diff.deleted_sections)).unwrap(),
            raw
        );
        assert!(!diff.reorder_section(
            &base,
            "C",
            &SectionGap {
                predecessor: None,
                successor: Some("A".into())
            }
        ));
        assert_eq!(
            serde_json::to_value(diff.section_order.for_save(&diff.deleted_sections)).unwrap(),
            raw
        );
        assert!(diff.reorder_section(
            &base,
            "C",
            &SectionGap {
                predecessor: Some("B".into()),
                successor: None
            }
        ));
        assert_eq!(
            serde_json::to_value(diff.section_order.for_save(&diff.deleted_sections)).unwrap(),
            serde_json::json!([])
        );
        assert!(diff.section_order.is_empty());
        assert_eq!(diff.storage_version(), 1);
    }

    #[test]
    fn deleting_user_anchor_before_early_return_preserves_hidden_followers() {
        let current_base = base(&["A", "C"]);
        let mut diff = LayoutDiff::default();
        diff.user_sections.push("B".into());
        diff.section_order.replace(vec![placement("D", "B")]);
        diff.delete_section(&current_base, "B");
        assert!(!diff.deleted_sections.contains(&"B".into()));
        let entries = diff.section_order.parsed(&diff.deleted_sections);
        assert!(entries
            .iter()
            .all(|entry| entry.id != "B" && entry.at.name() != "B"));
        assert!(entries.iter().any(|entry| entry.id == "D"));
        assert_eq!(names(&diff.apply(&base(&["A", "C", "D"]))), ["A", "D", "C"]);
    }

    #[test]
    fn deletion_uses_renamed_anchor_before_removing_its_settings() {
        let base = base(&["A", "B", "C", "D", "E"]);
        let mut diff = parse_layout_diff(r#"{"format_version":2,"section_overrides":[{"key":"B","patch":{"display_name":"Z"}}],"section_order":[{"id":"C","at":{"after":"B"}}]}"#).unwrap();
        diff.delete_section(&base, "B");
        assert_eq!(names(&diff.apply(&base)), ["A", "D", "E", "C"]);
        assert!(diff
            .section_order
            .parsed(&diff.deleted_sections)
            .iter()
            .all(|e| e.at.name() != "B"));
    }

    #[test]
    fn v1_rect_overrides_never_run_the_v0_conversion_again() {
        let generated = LayoutConfig::auto_generate(&[MetricInfo {
            metric_name: "A/loss".into(),
            metric_type: MetricType::Numeric as i32,
        }]);
        let mut rect = generated.sections[0].rects[0].clone();
        rect.options.log_y = true;
        let json =
            serde_json::json!({"format_version":1,"added_rects":[{"section":"A","rect":rect}]});
        let once = parse_layout_diff(&json.to_string()).unwrap();
        assert!(once.added_rects[0].rect.options.log_y);
        assert!(once.rect_overrides.is_empty());
        let twice = parse_layout_diff(&serde_json::to_string(&once).unwrap()).unwrap();
        assert_eq!(once, twice);
    }

    #[test]
    fn legacy_priority_remains_a_default_input_and_empty_aliases_are_v1() {
        let base = base(&["A", "B", "C"]);
        let diff = parse_layout_diff(
            r#"{"format_version":1,"section_overrides":[{"key":"C","patch":{"priority":5}}]}"#,
        )
        .unwrap();
        assert_eq!(names(&diff.apply(&base)), ["C", "A", "B"]);
        assert_eq!(diff.storage_version(), 1);
        for raw in ["null", "[]"] {
            let diff =
                parse_layout_diff(&format!("{{\"format_version\":2,\"section_order\":{raw}}}"))
                    .unwrap();
            assert!(diff.is_empty());
            assert_eq!(diff.storage_version(), 1);
        }
        let diff = parse_layout_diff(r#"{"format_version":1,"section_order":false}"#).unwrap();
        assert_eq!(diff.storage_version(), 2);
    }
}
