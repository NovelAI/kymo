use std::cell::RefCell;

use dioxus::prelude::*;

use crate::components::binding_editor::BindingEditor;
use crate::components::cdn_gallery::CdnGallery;
use crate::components::icons::{CloseIcon, GearIcon, MaximizeIcon, SpinnerIcon};
use crate::components::text_stream::TextStreamViewer;
use crate::grpc::proto::{CdnSeries, RunStatus, SeriesRef};
use crate::route::focus_chart;
use crate::state::chart_sync::ChartCacheEntry;
use crate::state::layout_config::{CdnDisplayMode, MetricBinding, RectOptions, RunRef};
use crate::state::panel_cache::{panel_key, Store};
use crate::state::visibility::{self, Zone};
use crate::state::zones::ZoneRegistry;
use crate::state::{resolve_capped_bindings, DashboardState, DisplayType, RectConfig, ViewContext};
use crate::util::{editor_trigger_id, focus_on_mount, is_app_escape, primary};

fn normalize_rect_label(label: String) -> String {
    if label.trim().is_empty() {
        String::new()
    } else {
        label
    }
}

mod numeric_content;
mod resize_js;
use numeric_content::NumericContent;

/// The old count-only assumption was 256 entries at a few hundred KiB each.
/// Make that intended ~128 MiB ceiling explicit; a single larger response is
/// rendered but not retained after its panel unmounts.
const CHART_CACHE_MAX_BYTES: usize = 128 * 1024 * 1024;
const CDN_KEYS_CACHE_MAX_BYTES: usize = 128 * 1024 * 1024;

thread_local! {
    /// Monotonic identity for chart models handed to UPlotChart (see data_key): every distinct model gets a fresh number. A counter, NOT the Rc address — the allocator reuses a dropped model's address for the next one, so pointer identity would silently equate different data.
    static NEXT_DATA_SEQ: std::cell::Cell<u64> = const { std::cell::Cell::new(1) };
    /// Per-panel chart cache (chart_sync::ChartCacheEntry), surviving body unmounts (see MetricRect). Validity = ChartCacheEntry::fresh_for (send-time snapshots vs current values), never which version bump triggered a resource run. Entries always hold a FULL response (deltas splice before storing): they answer run-subset requests locally, splice the next delta, and re-render instantly on remount. Rc: hits are refcount bumps, not multi-MB copies. Both entry count and estimated retained heap bytes are hard-bounded.
    static CHART_CACHE: RefCell<Store<ChartCacheEntry>> =
        RefCell::new(Store::with_weight_limit(256, CHART_CACHE_MAX_BYTES));
    /// Per-panel (trigger version, send-time version, refs) -> raw CDN key series of the last successful fetch. Same two-stamp validity as CHART_CACHE.
    #[allow(clippy::type_complexity)]
    static CDN_KEYS_CACHE: RefCell<Store<(u64, u64, Vec<SeriesRef>, Vec<CdnSeries>)>> =
        RefCell::new(Store::with_weight_limit(256, CDN_KEYS_CACHE_MAX_BYTES));
    /// Per-panel (metrics-gen key, bound metric names) -> detected (numeric, cdn, text). Tiny entries, generous cap.
    #[allow(clippy::type_complexity)]
    static TYPE_CACHE: RefCell<Store<(u64, Vec<String>, (bool, bool, bool))>> =
        RefCell::new(Store::new(2048));
}

fn next_data_seq() -> u64 {
    NEXT_DATA_SEQ.with(|c| {
        let v = c.get();
        c.set(v + 1);
        v
    })
}

fn cdn_cache_heap_bytes(refs: &Vec<SeriesRef>, series: &Vec<CdnSeries>) -> usize {
    let mut bytes = std::mem::size_of::<(u64, u64, Vec<SeriesRef>, Vec<CdnSeries>)>()
        .saturating_add(
            refs.capacity()
                .saturating_mul(std::mem::size_of::<SeriesRef>()),
        )
        .saturating_add(
            series
                .capacity()
                .saturating_mul(std::mem::size_of::<CdnSeries>()),
        );
    for reference in refs {
        bytes = bytes
            .saturating_add(reference.project_id.capacity())
            .saturating_add(reference.run_id.capacity())
            .saturating_add(reference.metric_name.capacity())
            .saturating_add(
                reference
                    .tags
                    .capacity()
                    .saturating_mul(std::mem::size_of::<String>()),
            )
            .saturating_add(
                reference
                    .tags
                    .iter()
                    .map(|tag| tag.capacity())
                    .sum::<usize>(),
            );
    }
    for item in series {
        bytes = bytes
            .saturating_add(item.project_id.capacity())
            .saturating_add(item.run_id.capacity())
            .saturating_add(item.metric_name.capacity())
            .saturating_add(
                item.entries
                    .capacity()
                    .saturating_mul(std::mem::size_of::<crate::grpc::proto::CdnEntry>()),
            )
            .saturating_add(
                item.entries
                    .iter()
                    .map(|entry| entry.cdn_key.capacity())
                    .sum::<usize>(),
            );
    }
    bytes
}

fn evict_panel_caches(cache_key: &str) {
    // Every cache whose contents belong to a panel must be evicted here; a
    // terminal request error must not leave a stale view behind another path.
    CHART_CACHE.with(|c| c.borrow_mut().remove(cache_key));
    CDN_KEYS_CACHE.with(|c| c.borrow_mut().remove(cache_key));
    TYPE_CACHE.with(|c| c.borrow_mut().remove(cache_key));
}

#[derive(Clone, Debug, PartialEq)]
pub struct CdnRunData {
    pub project_id: String,
    pub run_id: String, // UUID, for localStorage color override key
    pub metric_name: String,
    pub label: String, // display text (e.g. "my-run" or "my-run/loss_img")
    pub color: String, // hex color resolved by the caller
    pub keys: Vec<(i64, String)>,
    /// The run has exited or is presumed dead, so its pending uploads are no longer in flight.
    pub ended: bool,
}

fn decorate_cdn_series(
    series: Vec<CdnSeries>,
    all_runs: &[crate::grpc::proto::RunInfo],
    all_same_metric: bool,
    run_color: impl Fn(&str, u64) -> String,
) -> Vec<CdnRunData> {
    series
        .into_iter()
        .map(|series| {
            let run_name = crate::state::run_name_for(all_runs, &series.run_id);
            let ordinal = crate::state::run_ordinal_for(all_runs, &series.run_id);
            let ended =
                crate::state::app_state::find_run(all_runs, &series.run_id).is_some_and(|run| {
                    matches!(
                        run.status(),
                        RunStatus::Crashed | RunStatus::Finished | RunStatus::PresumedDead
                    )
                });
            let (label, color) = if all_same_metric {
                (run_name, run_color(&series.run_id, ordinal))
            } else {
                let label = format!("{}/{}", run_name, series.metric_name);
                let color = crate::components::uplot_chart::hash_color(&label);
                (label, color)
            };
            CdnRunData {
                project_id: series.project_id,
                run_id: series.run_id,
                metric_name: series.metric_name,
                label,
                color,
                keys: series
                    .entries
                    .into_iter()
                    .map(|entry| (entry.step, entry.cdn_key))
                    .collect(),
                ended,
            }
        })
        .collect()
}

pub(crate) fn build_view_context(state: &DashboardState) -> ViewContext {
    ViewContext {
        current_project: state.project_id.read().clone(),
        current_run: state.current_run.read().clone(),
        selected_runs: state.selected_runs.read().clone(),
        all_runs: state.runs.read().iter().map(|r| r.run_id.clone()).collect(),
    }
}

/// Sorted, deduped run ids the bindings resolve to. Its own memo so the
/// version-hash memos downstream recompute cheaply on every pushed event
/// without re-resolving bindings (which clones the whole view context).
fn use_bound_run_ids(
    bindings: Signal<Vec<MetricBinding>>,
    max_runs: Signal<u32>,
) -> Memo<Vec<String>> {
    let state = use_context::<DashboardState>();
    use_memo(move || {
        let ctx = build_view_context(&state);
        let mut ids: Vec<String> =
            resolve_capped_bindings(&bindings.read(), &ctx, *max_runs.read())
                .into_iter()
                .map(|r| r.run_id)
                .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    })
}

#[derive(Clone, Debug)]
pub struct ResizeResult {
    pub new_column_span: Option<u32>,
    pub height_delta: i32,
}

/// The always-mounted slot: one fixed-min-height card div, one zone signal, one registry entry. Everything else — header, hooks, eval channels, fetches, the chart — lives in MetricRectBody, mounted only while the slot is inside the prefetch band. At thousands of panels the Far slots are the only per-panel cost; their data survives in the module-level caches above.
#[component]
pub fn MetricRect(
    config: RectConfig,
    chart_height: u32,
    /// Section's max columns — needed to clamp the column_span input in the
    /// binding editor and the preview label during drag-resize.
    max_columns: u32,
    /// True when this rect is being rendered inside the maximize overlay —
    /// hides the maximize button and disables the resize handle.
    #[props(default = false)]
    is_maximized: bool,
    on_update: EventHandler<RectConfig>,
    on_delete: EventHandler<()>,
    on_resize: EventHandler<ResizeResult>,
) -> Element {
    // Zone from the shared observers (state/zones.rs), which attach via the .metric-slot class — no per-rect observers or channels. The maximize overlay is pinned Visible and stays out of the registry.
    let zone = use_signal(|| {
        if is_maximized {
            Zone::Visible
        } else {
            Zone::Far
        }
    });
    // Held true by the body while a modal (binding editor / rename) is open: scrolling it out of the band must not unmount an editor mid-edit. Fetches still freeze at Far — `allowed` gates on the zone, not on mount.
    let pin = use_signal(|| false);
    let registry = use_context::<ZoneRegistry>();
    use_hook({
        let registry = registry.clone();
        let id = config.id.clone();
        move || {
            if !is_maximized {
                registry.register(id, zone);
            }
        }
    });
    use_drop({
        let registry = registry.clone();
        let id = config.id.clone();
        move || {
            if !is_maximized {
                registry.unregister(&id, zone);
            }
        }
    });

    let slot_class = if is_maximized {
        "metric-rect"
    } else {
        "metric-rect metric-slot"
    };
    rsx! {
        div {
            class: "{slot_class}",
            "data-slot-id": "{config.id}",
            style: "--kymo-chart-height: {chart_height}px;",
            if is_maximized || *pin.read() || *zone.read() != Zone::Far {
                MetricRectBody {
                    config: config.clone(),
                    chart_height: chart_height,
                    max_columns: max_columns,
                    is_maximized: is_maximized,
                    zone: zone,
                    pin: pin,
                    on_update: on_update,
                    on_delete: on_delete,
                    on_resize: on_resize,
                }
            }
        }
    }
}

#[component]
fn MetricRectBody(
    config: RectConfig,
    chart_height: u32,
    max_columns: u32,
    is_maximized: bool,
    zone: Signal<Zone>,
    pin: Signal<bool>,
    on_update: EventHandler<RectConfig>,
    on_delete: EventHandler<()>,
    on_resize: EventHandler<ResizeResult>,
) -> Element {
    let mut editing = use_signal(|| false);
    let mut renaming = use_signal(|| false);
    let initial_label = config.label.clone();
    let editor_trigger_id = editor_trigger_id(
        if is_maximized {
            "rect-max"
        } else {
            "rect-grid"
        },
        &config.id,
    );

    // Keep the slot from unmounting this body while a modal is open.
    let mut pin_out = pin;
    use_effect(move || {
        let hold = *editing.read() || *renaming.read();
        if *pin_out.peek() != hold {
            pin_out.set(hold);
        }
    });

    // True while this rect's content has a query in flight: drives the
    // corner spinner and gates the leaves' version bridges (see
    // use_version_bridge).
    let loading = use_signal(|| false);

    // Sub-type of a CDN metric, learned from the manifest's `class` field as
    // the gallery resolves. None until known; "image_gallery", "metadata",
    // or "file_list" once a manifest has been parsed. Used by BindingEditor
    // to show only panels that actually apply (e.g. hide Image Gallery mode
    // for metadata trees).
    let cdn_class = use_signal(|| Option::<String>::None);
    // AutoContent may need a registry probe when a saved Specific rect has
    // outlived its discovery metric. Keep that observed type ephemeral: the
    // editor needs the right controls, but the synthetic fallback must never
    // become a persisted user override.
    let resolved_display_type = use_signal(|| Option::<DisplayType>::None);

    let display_title = if config.label.is_empty() {
        config
            .bindings
            .iter()
            .map(|b| {
                // Strip prefix before last '/' (same as section grouping)
                match b.metric_name.rfind('/') {
                    Some(pos) => &b.metric_name[pos + 1..],
                    None => b.metric_name.as_str(),
                }
            })
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        config.label.clone()
    };
    let display_title = if display_title.is_empty() {
        "(unconfigured)".to_string()
    } else {
        display_title
    };

    let mut rename_value = use_signal(move || initial_label.clone());

    let state = use_context::<DashboardState>();
    let color_ver = *state.color_version.read();
    let log_x = config.options.log_x;
    let log_y = config.options.log_y;
    let cdn_mode = config.options.cdn_display_mode.clone();
    let options = config.options.clone();
    // Key into the module-level caches. The overlay copy gets its own entries — its width (and so its chart request) differs from the grid rect's, and the two must not evict each other per open/close.
    let cache_key = use_hook(|| {
        let base = panel_key(&state.project_id.peek(), &config.id);
        if is_maximized {
            format!("{base}\u{1f}max")
        } else {
            base
        }
    });
    let content = rsx! {
        AutoContent {
            rect_id: config.id.clone(),
            bindings: config.bindings.clone(),
            display_type_hint: config.display_type,
            log_x: log_x,
            log_y: log_y,
            chart_height: chart_height,
            color_version: color_ver,
            cdn_display_mode: cdn_mode,
            options: options,
            loading: loading,
            cdn_class: cdn_class,
            zone: zone,
            cache_key: cache_key.clone(),
            resolved_display_type: resolved_display_type,
        }
    };

    rsx! {
        Fragment {
            div { class: "rect-header",
                if *renaming.read() {
                    {
                        let mut commit_rename = {
                            let config = config.clone();
                            move || {
                                renaming.set(false);
                                let mut updated = config.clone();
                                updated.label = normalize_rect_label(rename_value.read().clone());
                                on_update.call(updated);
                            }
                        };
                        rsx! {
                            input {
                                class: "rect-title-input",
                                value: "{rename_value}",
                                // Not `autofocus`: browsers honour it once per document, and only while nothing else (e.g. the maximize overlay) has focus.
                                onmounted: focus_on_mount,
                                placeholder: "Label (empty = auto)",
                                oninput: move |e: Event<FormData>| {
                                    rename_value.set(e.value());
                                },
                                onkeydown: {
                                    let mut commit_rename = commit_rename.clone();
                                    move |e: Event<KeyboardData>| {
                                        if e.key() == Key::Enter && !e.is_composing() {
                                            commit_rename();
                                        } else if is_app_escape(&e) {
                                            // Consume the key so cancelling a rename inside a maximized chart doesn't also close the overlay.
                                            e.prevent_default();
                                            renaming.set(false);
                                        }
                                    }
                                },
                                onblur: move |_| commit_rename(),
                            }
                        }
                    }
                } else {
                    {
                        let label_for_click = config.label.clone();
                        rsx! {
                            div {
                                class: "rect-title",
                                ondoubleclick: move |_| {
                                    rename_value.set(label_for_click.clone());
                                    renaming.set(true);
                                },
                                // Inner span is the hover reveal (.rect-title-pop in kymo.css).
                                span { class: "rect-title-text fade-overflow",
                                    span { class: "rect-title-pop", "{display_title}" }
                                }
                            }
                        }
                    }
                }
                div { class: "rect-actions",
                    if !is_maximized {
                        {
                            let rect_id_for_max = config.id.clone();
                            rsx! {
                                button {
                                    class: "rect-action icon-button",
                                    title: "Maximize",
                                    onmousedown: primary(move |_| {
                                        focus_chart(Some(rect_id_for_max.clone()));
                                    }),
                                    MaximizeIcon {}
                                }
                            }
                        }
                    }
                    button {
                        id: "{editor_trigger_id}",
                        class: "rect-action icon-button",
                        title: "Configure",
                        onmousedown: primary(move |_| editing.set(true)),
                        GearIcon {}
                    }
                    if is_maximized {
                        button {
                            class: "rect-action icon-button",
                            title: "Close",
                            onmousedown: primary(move |_| {
                                focus_chart(None);
                            }),
                            CloseIcon {}
                        }
                    } else {
                        {
                            let display_title_for_delete = display_title.clone();
                            rsx! {
                                button {
                                    class: "rect-action rect-action-delete icon-button",
                                    title: "Delete",
                                    onmousedown: primary(move |_| {
                                        if let Some(window) = web_sys::window() {
                                            let msg = format!("Delete chart \"{}\"?", display_title_for_delete);
                                            if window.confirm_with_message(&msg).unwrap_or(false) {
                                                on_delete.call(());
                                            }
                                        }
                                    }),
                                    CloseIcon {}
                                }
                            }
                        }
                    }
                }
            }

            {content}

            if *loading.read() {
                span { class: "rect-loading-indicator", SpinnerIcon {} }
            }

            if !is_maximized {
                {
                let rect_id_for_resize = config.id.clone();
                rsx! {
                    div {
                        class: "rect-resize-handle",
                        "data-rect-id": "{rect_id_for_resize}",
                        title: "Drag to resize",
                        onmousedown: primary({
                            let rect_id = config.id.clone();
                            move |e: Event<MouseData>| {
                                // The default mousedown action anchors a text selection that the drag then extends; the resize JS attaches too late (async eval) to stop it, so kill it here like the sidebar handle's inline preventDefault.
                                e.prevent_default();
                                let start_y = e.page_coordinates().y;
                                let rid = rect_id.clone();
                                spawn(async move {
                                    let js = resize_js::build(start_y, &rid);
                                    let mut eval = document::eval(&js);
                                    match eval.recv::<serde_json::Value>().await {
                                        Ok(val) => {
                                            let dy = val["dy"].as_f64().unwrap_or(0.0) as i32;
                                            let new_column_span = val["span"]
                                                .as_u64()
                                                .map(|v| v as u32)
                                                .filter(|&v| v >= 1);
                                            on_resize.call(ResizeResult { new_column_span, height_delta: dy });
                                        }
                                        Err(e) => crate::util::warn(&format!("[resize] bridge channel lost: {e:?}")),
                                    }
                                });
                            }
                        }),
                    }
                }
                }
            }

            if *editing.read() {
                BindingEditor {
                    return_focus_id: editor_trigger_id.clone(),
                    bindings: config.bindings.clone(),
                    options: config.options.clone(),
                    anchor: state.inherited_options_for_rect(&config.id),
                    display_type: resolved_display_type.read().unwrap_or(config.display_type),
                    cdn_class: cdn_class.read().clone(),
                    max_columns: max_columns,
                    on_change: {
                        let config = config.clone();
                        move |(new_bindings, new_options): (Vec<MetricBinding>, RectOptions)| {
                            on_update.call(RectConfig {
                                bindings: new_bindings,
                                options: new_options,
                                ..config.clone()
                            });
                        }
                    },
                    on_save: {
                        let config = config.clone();
                        move |(new_bindings, new_options): (Vec<MetricBinding>, RectOptions)| {
                            editing.set(false);
                            on_update.call(RectConfig {
                                bindings: new_bindings,
                                options: new_options,
                                ..config.clone()
                            });
                        }
                    },
                    on_cancel: move |_| editing.set(false),
                }
            }
        }
    }
}

fn needs_metric_type_check(rect_id: &str, bindings: &[MetricBinding]) -> bool {
    let distinct_metrics: std::collections::HashSet<&str> = bindings
        .iter()
        .map(|binding| binding.metric_name.as_str())
        .collect();
    distinct_metrics.len() > 1
        || bindings
            .iter()
            .any(|binding| !binding.metric_name.is_empty() && binding.metric_name != rect_id)
        || bindings.iter().any(
            |binding| matches!(&binding.runs, RunRef::Specific(run_ids) if !run_ids.is_empty()),
        )
}

fn every_source_terminal(source_count: usize, terminal_count: usize) -> bool {
    source_count > 0 && terminal_count == source_count
}

fn sole_detected_display_type(types: (bool, bool, bool)) -> Option<DisplayType> {
    match types {
        (true, false, false) => Some(DisplayType::Numeric),
        (false, true, false) => Some(DisplayType::Cdn),
        (false, false, true) => Some(DisplayType::TextStream),
        _ => None,
    }
}

fn note_resolved_display_type(
    mut resolved: Signal<Option<DisplayType>>,
    next: Option<DisplayType>,
) {
    if *resolved.peek() != next {
        resolved.set(next);
    }
}

/// Detects whether bindings resolve to numeric, CDN, or mixed metrics,
/// and renders the appropriate content (or an error for mixed).
/// Multiple or retargeted metric names need type detection. Specific bindings
/// also detect even with one unchanged metric so a legacy saved override can
/// be reconstructed after its discovery metric vanishes without guessing its
/// numeric/CDN/text type.
#[component]
fn AutoContent(
    rect_id: String,
    bindings: Vec<MetricBinding>,
    display_type_hint: DisplayType,
    #[props(default = false)] log_x: bool,
    #[props(default = false)] log_y: bool,
    #[props(default = 280)] chart_height: u32,
    #[props(default = 0)] color_version: u64,
    #[props(default)] cdn_display_mode: CdnDisplayMode,
    #[props(default)] options: RectOptions,
    /// Flipped true while a leaf content's data query is in flight, so the
    /// parent MetricRect can show a spinner and the leaves can gate their
    /// own version-driven refetches against being cancelled mid-flight.
    loading: Signal<bool>,
    /// CDN sub-type bubbled up from the manifest renderer. Numeric and text
    /// paths leave it None.
    cdn_class: Signal<Option<String>>,
    /// Viewport zone from the slot (see MetricRect) — the leaves gate
    /// fetching (and, for charts, rendering) on it.
    zone: Signal<Zone>,
    /// Key into the module-level caches (see the thread_locals up top).
    cache_key: String,
    /// The type actually observed for this binding set, used only to choose
    /// the editor's controls. It never mutates or persists the RectConfig.
    resolved_display_type: Signal<Option<DisplayType>>,
) -> Element {
    let state = use_context::<DashboardState>();

    let needs_type_check = needs_metric_type_check(&rect_id, &bindings);

    let mut bindings_signal = use_signal(|| bindings.clone());
    if *bindings_signal.read() != bindings {
        bindings_signal.set(bindings.clone());
    }
    let mut max_runs_signal = use_signal(|| options.max_runs);
    if *max_runs_signal.read() != options.max_runs {
        max_runs_signal.set(options.max_runs);
    }

    // Registry-change key for the bound runs: a metric's TYPE can only
    // change when its registry entry does (upgrade), so this re-detects
    // exactly then — not on every data flush. Split memos so a pushed
    // event recomputes only the integer hash, not the binding resolution.
    let bound_run_ids = use_bound_run_ids(bindings_signal, max_runs_signal);
    let my_metrics_gen = use_memo(move || {
        crate::state::versions_key(
            *state.resync_gen.read(),
            bound_run_ids.read().iter().map(String::as_str),
            &state.metrics_gen.read(),
        )
    });

    // Zone gate + cache, like the data fetches: an out-of-band rect (only reachable here while an editor pins the body) must not turn a registry event into list_metrics traffic, and a Near/Visible re-detection defers to visible fetches. The cache keys on (metrics-gen hash, bound metric NAMES) — a binding edit that keeps the same runs but swaps metrics must not serve the old types. All-terminal passes never enter it. It lives in TYPE_CACHE so a body remounting after a scroll-away skips re-detection.
    let type_allowed = use_memo(move || *zone.read() != Zone::Far);

    enum TypeFetch {
        UseHint,
        Detected((bool, bool, bool)),
        RunUnavailable,
    }

    // Only do the expensive type detection when the hint is insufficient.
    let detected_types = use_resource({
        let cache_key = cache_key.clone();
        move || {
            let grpc = state.grpc.read().clone();
            let ctx = build_view_context(&state);
            let mg = *my_metrics_gen.read();
            let _selected = state.selected_runs.read().clone();
            let bindings = bindings_signal.read().clone();
            let max_runs = *max_runs_signal.read();
            let allowed = *type_allowed.read();
            let cache_key = cache_key.clone();
            let resolved_display_type = resolved_display_type;
            async move {
                if !needs_type_check {
                    note_resolved_display_type(resolved_display_type, Some(display_type_hint));
                    return TypeFetch::UseHint;
                }
                // A new binding set must not expose controls from the stale
                // type while its probe is in flight.
                note_resolved_display_type(resolved_display_type, None);
                let refs = resolve_capped_bindings(&bindings, &ctx, max_runs);
                let mut metric_names: Vec<String> =
                    refs.iter().map(|r| r.metric_name.clone()).collect();
                metric_names.sort_unstable();
                metric_names.dedup();
                let cached = TYPE_CACHE.with(|c| c.borrow_mut().get(&cache_key));
                let first_paint = cached.is_none();
                if let Some((cached_mg, cached_names, types)) = cached {
                    if (cached_mg == mg && cached_names == metric_names) || !allowed {
                        note_resolved_display_type(
                            resolved_display_type,
                            sole_detected_display_type(types),
                        );
                        return TypeFetch::Detected(types);
                    }
                }
                if !allowed {
                    // Never detected and out of band: fall back to the hint; the allowed flip on re-entry restarts this and detects for real.
                    return TypeFetch::UseHint;
                }
                // One readable run per distinct metric name suffices. Probe
                // candidates in binding order: transient failures retry, but
                // a terminal run is skipped so a healthy sibling (notably a
                // text stream) can still determine the panel type and mount.
                let mut detected_names = std::collections::HashSet::new();
                let mut terminal_sources = 0usize;
                let mut has_numeric = false;
                let mut has_cdn = false;
                let mut has_text = false;
                for r in &refs {
                    if detected_names.contains(&r.metric_name) {
                        continue;
                    }
                    let metrics = visibility::retry_visible_run("type detection", async || {
                        let _hi = visibility::admit_fetch(|| *zone.peek(), first_paint).await;
                        grpc.list_metrics(&r.project_id, &r.run_id).await
                    })
                    .await;
                    let Ok(metrics) = metrics else {
                        terminal_sources += 1;
                        continue;
                    };
                    let Some(metric) = metrics
                        .iter()
                        .find(|metric| metric.metric_name == r.metric_name)
                    else {
                        continue;
                    };
                    detected_names.insert(r.metric_name.clone());
                    match DisplayType::for_metric(metric) {
                        DisplayType::Cdn => has_cdn = true,
                        DisplayType::TextStream => has_text = true,
                        DisplayType::Numeric => has_numeric = true,
                    }
                }
                if every_source_terminal(refs.len(), terminal_sources) {
                    evict_panel_caches(&cache_key);
                    return TypeFetch::RunUnavailable;
                }
                let types = (has_numeric, has_cdn, has_text);
                note_resolved_display_type(
                    resolved_display_type,
                    sole_detected_display_type(types),
                );
                TYPE_CACHE.with(|c| c.borrow_mut().put(cache_key, (mg, metric_names, types)));
                TypeFetch::Detected(types)
            }
        }
    });

    // Helper to render the right content for a display type
    let render_for_type = |dt: &DisplayType| -> Element {
        match dt {
            DisplayType::Cdn => rsx! {
                CdnContent { bindings: bindings.clone(), max_runs: options.max_runs, chart_height: chart_height, cdn_display_mode: cdn_display_mode.clone(), loading: loading, cdn_class: cdn_class, metadata_diff_only: options.metadata_diff_only, zone: zone, cache_key: cache_key.clone() }
            },
            DisplayType::TextStream => rsx! {
                TextStreamContent { bindings: bindings.clone(), max_runs: options.max_runs, chart_height: chart_height, zone: zone, cache_key: cache_key.clone() }
            },
            DisplayType::Numeric => rsx! {
                NumericContent { bindings: bindings.clone(), log_x: log_x, log_y: log_y, chart_height: chart_height, color_version: color_version, options: options.clone(), loading: loading, zone: zone, cache_key: cache_key.clone() }
            },
        }
    };

    let read = detected_types.read();
    match &*read {
        // Type check completed
        Some(TypeFetch::Detected((has_numeric, has_cdn, has_text))) => {
            let type_count = [*has_numeric, *has_cdn, *has_text]
                .iter()
                .filter(|&&x| x)
                .count();
            if type_count > 1 {
                // No leaf mounts here, so heal the shared loading flag: a
                // leaf unmounted mid-fetch left it true, and only leaf
                // bodies heal it — the spinner would spin forever.
                if *loading.peek() {
                    let mut loading = loading;
                    loading.set(false);
                }
                rsx! { div { class: "rect-error", "Cannot mix different metric types" } }
            } else if *has_text {
                render_for_type(&DisplayType::TextStream)
            } else if *has_cdn {
                render_for_type(&DisplayType::Cdn)
            } else {
                render_for_type(&DisplayType::Numeric)
            }
        }
        // No type check needed — use hint
        Some(TypeFetch::UseHint) => render_for_type(&display_type_hint),
        Some(TypeFetch::RunUnavailable) => {
            if *loading.peek() {
                let mut loading = loading;
                loading.set(false);
            }
            rsx! { div { class: "rect-empty", style: "height: {chart_height}px;", "Run no longer available" } }
        }
        // Still loading
        None => {
            if !needs_type_check {
                render_for_type(&display_type_hint)
            } else {
                rsx! { div { class: "rect-loading", style: "height: {chart_height}px;", "Loading..." } }
            }
        }
    }
}

#[derive(PartialEq)]
enum CdnFetch {
    Pending,
    Answer(Vec<CdnRunData>),
    Unavailable,
}

#[component]
fn CdnContent(
    bindings: Vec<MetricBinding>,
    max_runs: u32,
    #[props(default = 280)] chart_height: u32,
    #[props(default)] cdn_display_mode: CdnDisplayMode,
    loading: Signal<bool>,
    cdn_class: Signal<Option<String>>,
    #[props(default = false)] metadata_diff_only: bool,
    zone: Signal<Zone>,
    cache_key: String,
) -> Element {
    let state = use_context::<DashboardState>();

    let mut bindings_signal = use_signal(|| bindings.clone());
    if *bindings_signal.read() != bindings {
        bindings_signal.set(bindings.clone());
    }
    let mut max_runs_signal = use_signal(|| max_runs);
    if *max_runs_signal.read() != max_runs {
        max_runs_signal.set(max_runs);
    }

    // Same version key, bridge, and visibility gating as NumericContent. Like the whole body, the gallery unmounts at Far; its data rebuilds from CDN_KEYS_CACHE and explicit navigation survives in cdn_gallery's bounded session stores (GALLERY_STEP / GALLERY_INDEX).
    let bound_run_ids = use_bound_run_ids(bindings_signal, max_runs_signal);
    let my_version = use_memo(move || {
        crate::state::versions_key(
            0,
            bound_run_ids.read().iter().map(String::as_str),
            &state.run_versions.read(),
        )
    });
    let allowed = use_memo(move || *zone.read() != Zone::Far);
    // One in-flight signal for spinner + bridge, exactly as in NumericContent.
    let data_seq = crate::state::use_version_bridge(my_version, loading, allowed);
    let mut loading = loading;

    // The (data_seq, refs) -> key-series memoization lives in CDN_KEYS_CACHE (module-level, survives this body unmounting at Far), so re-entering the band with nothing changed is network-free. Labels and colors are rebuilt from the live run list on every pass.
    // Written only by settled fetches, so deferred passes retain the last
    // gallery; transient failures retry and terminal failures settle once.
    let mut fetch = use_signal(|| CdnFetch::Pending);

    let _fetch = use_resource({
        let cache_key = cache_key.clone();
        move || {
            let cache_key = cache_key.clone();
            let grpc = state.grpc.read().clone();
            let ctx = build_view_context(&state);
            let ds = *data_seq.read();
            let _selected = state.selected_runs.read().clone();
            let bindings = bindings_signal.read().clone();
            let max_runs = *max_runs_signal.read();
            let all_runs = state.display_runs();
            // Run colors live in localStorage, outside Dioxus. The sidebar bumps this signal after an override changes; tracking it here rebuilds presentation without querying the key series again.
            let _color_version = *state.color_version.read();
            let allowed = *allowed.read();
            let runs_loaded = *state.runs_loaded.read();
            let mut cdn_class = cdn_class;
            async move {
                // Heal a cancelled predecessor's flag (see NumericContent).
                if *loading.peek() {
                    loading.set(false);
                }
                if !allowed {
                    return;
                }
                let refs = resolve_capped_bindings(&bindings, &ctx, max_runs);
                if refs.is_empty() {
                    // Before the first list_runs lands, empty refs mean "runs
                    // unknown", not "no data" — keep showing "Loading..."
                    // (same guard as NumericContent's).
                    if runs_loaded
                        && !matches!(&*fetch.peek(), CdnFetch::Answer(runs) if runs.is_empty())
                    {
                        fetch.set(CdnFetch::Answer(Vec::new()));
                    }
                    return;
                }
                let all_same_metric = refs.iter().all(|r| r.metric_name == refs[0].metric_name);

                let series_refs: Vec<SeriesRef> = refs
                    .iter()
                    .map(|r| SeriesRef {
                        project_id: r.project_id.clone(),
                        run_id: r.run_id.clone(),
                        metric_name: r.metric_name.clone(),
                        tags: vec![],
                    })
                    .collect();

                let cached = CDN_KEYS_CACHE.with(|c| c.borrow_mut().get(&cache_key));
                // Any cached entry = the gallery already shows something: a
                // refresh, not a first paint, for gate purposes.
                let first_paint = cached.is_none();
                // Trigger-or-sent hit rule, exactly as in NumericContent's cache check.
                let series = match cached.and_then(|(cds_trigger, cds_sent, crefs, cseries)| {
                    ((cds_trigger == ds || cds_sent == ds) && crefs == series_refs)
                        .then_some(cseries)
                }) {
                    Some(series) => series,
                    None => {
                        loading.set(true);
                        let response = visibility::retry_visible_run("cdn keys", async || {
                            let _hi = visibility::admit_fetch(|| *zone.peek(), first_paint).await;
                            // Sent stamp peeked at send — see NumericContent for the soundness argument.
                            let sent = *my_version.peek();
                            grpc.query_cdn_keys(series_refs.clone())
                                .await
                                .map(|r| (sent, r))
                        })
                        .await;
                        loading.set(false);
                        let (sent, series) = match response {
                            Ok(result) => result,
                            Err(_) => {
                                evict_panel_caches(&cache_key);
                                if cdn_class.peek().is_some() {
                                    cdn_class.set(None);
                                }
                                if *fetch.peek() != CdnFetch::Unavailable {
                                    fetch.set(CdnFetch::Unavailable);
                                }
                                return;
                            }
                        };
                        let weight = cdn_cache_heap_bytes(&series_refs, &series);
                        CDN_KEYS_CACHE.with(|c| {
                            c.borrow_mut().put_weighted(
                                cache_key,
                                (ds, sent, series_refs, series.clone()),
                                weight,
                            )
                        });
                        series
                    }
                };

                let result = decorate_cdn_series(
                    series,
                    &all_runs,
                    all_same_metric,
                    crate::components::uplot_chart::run_color,
                );
                if !matches!(&*fetch.peek(), CdnFetch::Answer(current) if current == &result) {
                    fetch.set(CdnFetch::Answer(result));
                }
            }
        }
    });

    let shown = fetch.read();
    match &*shown {
        CdnFetch::Answer(run_data) => {
            let mode = cdn_display_mode.clone();
            rsx! {
                CdnGallery { runs: run_data.clone(), height: chart_height, display_mode: mode, cdn_class: cdn_class, metadata_diff_only: metadata_diff_only, persist_key: cache_key.clone() }
            }
        }
        CdnFetch::Unavailable => rsx! {
            div { class: "rect-empty", style: "height: {chart_height}px;", "Run no longer available" }
        },
        CdnFetch::Pending => rsx! {
            div { class: "rect-loading", style: "height: {chart_height}px;", "Loading..." }
        },
    }
}

#[component]
fn TextStreamContent(
    bindings: Vec<MetricBinding>,
    max_runs: u32,
    #[props(default = 280)] chart_height: u32,
    zone: Signal<Zone>,
    cache_key: String,
) -> Element {
    let state = use_context::<DashboardState>();
    let ctx = build_view_context(&state);
    let refs = resolve_capped_bindings(&bindings, &ctx, max_runs);

    if refs.is_empty() {
        // Before the first list_runs lands, empty refs mean "runs unknown"
        // (same guard as NumericContent's).
        if !*state.runs_loaded.read() {
            return rsx! { div { class: "rect-loading", style: "height: {chart_height}px;", "Loading..." } };
        }
        return rsx! { div { class: "rect-empty", style: "height: {chart_height}px;", "No runs shown" } };
    }

    let stream_refs: Vec<(String, String, String)> = refs
        .iter()
        .map(|r| {
            (
                r.project_id.clone(),
                r.run_id.clone(),
                r.metric_name.clone(),
            )
        })
        .collect();

    rsx! {
        TextStreamViewer { stream_refs: stream_refs, height: chart_height, x_axis_mode: crate::state::layout_config::XAxisMode::RelativeTime, zone: Some(zone), persist_key: cache_key }
    }
}

#[cfg(test)]
mod tests {
    use super::{cdn_cache_heap_bytes, decorate_cdn_series, normalize_rect_label};
    use crate::grpc::proto::{CdnEntry, CdnSeries, RunInfo, RunStatus, SeriesRef};

    fn cdn_series(metric_name: &str) -> CdnSeries {
        CdnSeries {
            project_id: "project".into(),
            run_id: "run".into(),
            metric_name: metric_name.into(),
            entries: vec![CdnEntry {
                step: 7,
                cdn_key: "key".into(),
            }],
        }
    }

    fn run() -> RunInfo {
        RunInfo {
            project_id: "project".into(),
            run_id: "run".into(),
            run_name: "Named".into(),
            ordinal: 3,
            ..Default::default()
        }
    }

    #[test]
    fn whitespace_only_rect_labels_restore_auto_naming() {
        assert_eq!(normalize_rect_label(" \t\u{a0} ".into()), "");
        assert_eq!(normalize_rect_label("  Loss  ".into()), "  Loss  ");
        assert_eq!(normalize_rect_label("Accuracy".into()), "Accuracy");
    }

    #[test]
    fn cdn_decoration_reapplies_run_colors_without_changing_data() {
        let series = vec![cdn_series("images")];
        let first = decorate_cdn_series(series.clone(), &[run()], true, |_, _| "#111111".into());
        let recolored =
            decorate_cdn_series(series.clone(), &[run()], true, |_, _| "#222222".into());

        assert_eq!(first[0].label, "Named");
        assert_eq!(first[0].keys, recolored[0].keys);
        assert_eq!(first[0].color, "#111111");
        assert_eq!(recolored[0].color, "#222222");

        let mixed_a = decorate_cdn_series(series.clone(), &[run()], false, |_, _| "#111111".into());
        let mixed_b = decorate_cdn_series(series, &[run()], false, |_, _| "#222222".into());
        assert_eq!(mixed_a[0].label, "Named/images");
        assert_eq!(mixed_a[0].color, mixed_b[0].color);
    }

    #[test]
    fn cdn_decoration_marks_ended_runs() {
        let ended = |runs: &[RunInfo]| {
            decorate_cdn_series(vec![cdn_series("images")], runs, true, |_, _| {
                "#111111".into()
            })[0]
                .ended
        };
        let with = |status: RunStatus| RunInfo {
            status: status as i32,
            ..run()
        };
        assert!(!ended(&[with(RunStatus::Running)]));
        assert!(!ended(&[with(RunStatus::Stuck)]));
        assert!(!ended(&[with(RunStatus::Unresponsive)]));
        assert!(ended(&[with(RunStatus::PresumedDead)]));
        assert!(ended(&[with(RunStatus::Crashed)]));
        assert!(ended(&[with(RunStatus::Finished)]));
        // A run missing from the list can't be shown to have ended.
        assert!(!ended(&[]));
    }

    #[test]
    fn cdn_cache_weight_counts_refs_entries_and_key_storage() {
        let refs = vec![SeriesRef {
            project_id: "project".into(),
            run_id: "run".into(),
            metric_name: "images".into(),
            tags: vec!["tag".into()],
        }];
        let mut series = vec![cdn_series("images")];
        let one_key = cdn_cache_heap_bytes(&refs, &series);

        series[0].entries.push(CdnEntry {
            step: 8,
            cdn_key: "x".repeat(256),
        });
        let two_keys = cdn_cache_heap_bytes(&refs, &series);

        assert!(one_key > std::mem::size_of::<(u64, u64)>());
        assert!(two_keys >= one_key.saturating_add(256));
    }
}
