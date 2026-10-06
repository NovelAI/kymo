use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use dioxus::prelude::*;

use crate::components::cdn_gallery::CdnGallery;
use crate::components::icons::{CloseIcon, GearIcon, MaximizeIcon, SpinnerIcon, TrashIcon};
use crate::components::text_stream::TextStreamViewer;
use crate::grpc::proto::{CdnSeries, RunStatus, SeriesRef};
use crate::route::focus_chart;
use crate::state::app_state::set_chart_facts;
use crate::state::chart_sync::ChartCacheEntry;
use crate::state::layout_config::{CdnDisplayMode, MetricBinding, RectOptions, RunRef};
use crate::state::panel_cache::{panel_key, Store};
use crate::state::visibility::{self, Zone};
use crate::state::zones::ZoneRegistry;
use crate::state::{resolve_capped_bindings, DashboardState, DisplayType, PanelTarget, RectConfig};
use crate::util::{confirm, editor_trigger_id, focus_on_mount, is_app_escape, primary};

/// A chart's title: its label, else its metrics' names without their section prefix.
pub(crate) fn rect_title(config: &RectConfig) -> String {
    if !config.label.is_empty() {
        return config.label.clone();
    }
    let names = config
        .bindings
        .iter()
        .map(|b| {
            // Strip prefix before last '/' (same as section grouping)
            b.metric_name
                .rsplit_once('/')
                .map_or(b.metric_name.as_str(), |(_, name)| name)
        })
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(", ");
    if names.is_empty() {
        "(unconfigured)".to_string()
    } else {
        names
    }
}

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
    /// Per-panel chart cache (chart_sync::ChartCacheEntry), surviving body unmounts (see MetricRect). Validity = ChartCacheEntry::fresh_for (the reply's version echo and send-time snapshots vs current values), never which version bump triggered a resource run. Entries always hold a FULL response (deltas splice before storing): they answer run-subset requests locally, splice the next delta, and re-render instantly on remount. Rc: hits are refcount bumps, not multi-MB copies. Both entry count and estimated retained heap bytes are hard-bounded.
    static CHART_CACHE: RefCell<Store<ChartCacheEntry>> =
        RefCell::new(Store::with_weight_limit(256, CHART_CACHE_MAX_BYTES));
    /// Per-panel (refs, version stamps) -> raw CDN key series of the last successful fetch. Validity = [`cdn_cache_hit`] (version stamps vs current values).
    #[allow(clippy::type_complexity)]
    static CDN_KEYS_CACHE: RefCell<Store<(Vec<SeriesRef>, HashMap<String, u64>, Vec<CdnSeries>)>> =
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

/// The gallery cache hit rule (CDN_KEYS_CACHE): an entry serves a request whose refs match while every requested run's stamp covers what the client knows ([`crate::state::stamp_covers`]).
fn cdn_cache_hit(
    entry_refs: &[SeriesRef],
    entry_stamps: &HashMap<String, u64>,
    request_refs: &[SeriesRef],
    known: &HashMap<String, u64>,
) -> bool {
    entry_refs == request_refs
        && request_refs.iter().all(|series| {
            crate::state::stamp_covers(
                entry_stamps.get(&series.run_id).copied(),
                known.get(&series.run_id).copied(),
            )
        })
}

fn cdn_cache_heap_bytes(
    refs: &Vec<SeriesRef>,
    stamps: &HashMap<String, u64>,
    series: &Vec<CdnSeries>,
) -> usize {
    let mut bytes = std::mem::size_of::<(Vec<SeriesRef>, HashMap<String, u64>, Vec<CdnSeries>)>()
        .saturating_add(
            refs.capacity()
                .saturating_mul(std::mem::size_of::<SeriesRef>()),
        )
        .saturating_add(crate::state::chart_sync::string_map_heap_bytes(stamps))
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
    run_color: impl Fn(&str, u64) -> String,
) -> Vec<CdnRunData> {
    let all_same_metric = series
        .first()
        .is_some_and(|first| series.iter().all(|s| s.metric_name == first.metric_name));
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

/// Whether every binding's runs are known without the project run list: `Specific` lists its own, `Selected` is the run page's URL run once one is set (else the selection, which the list defines), and `All` is the list.
fn refs_known(bindings: &[MetricBinding], current_run: Option<&str>, runs_loaded: bool) -> bool {
    bindings.iter().all(|binding| match &binding.runs {
        RunRef::Specific(_) => true,
        RunRef::Selected => current_run.is_some() || runs_loaded,
        RunRef::All => runs_loaded,
    })
}

/// The always-mounted slot: one fixed-min-height card div, one zone signal, one registry entry. Everything else — header, hooks, eval channels, fetches, the chart — lives in MetricRectBody, mounted only while the slot is inside the prefetch band. At thousands of panels the Far slots are the only per-panel cost; their data survives in the module-level caches above.
#[component]
pub fn MetricRect(
    config: RectConfig,
    chart_height: u32,
    /// True when this rect is being rendered inside the maximize overlay —
    /// hides the maximize button and disables the resize handle.
    #[props(default = false)]
    is_maximized: bool,
) -> Element {
    // Zone from the shared observers (state/zones.rs), which attach via the .metric-slot class — no per-rect observers or channels. The maximize overlay is pinned Visible and stays out of the registry.
    let zone = use_signal(|| {
        if is_maximized {
            Zone::Visible
        } else {
            Zone::Far
        }
    });
    // The body's title rename: scrolling it out of the band mid-rename must not unmount the input. Fetches still freeze at Far — `allowed` gates on the zone, not on mount.
    let renaming = use_signal(|| false);
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
            if is_maximized || *renaming.read() || *zone.read() != Zone::Far {
                MetricRectBody {
                    config: config.clone(),
                    chart_height: chart_height,
                    is_maximized: is_maximized,
                    zone: zone,
                    renaming: renaming,
                }
            }
        }
    }
}

#[component]
fn MetricRectBody(
    config: RectConfig,
    chart_height: u32,
    is_maximized: bool,
    zone: Signal<Zone>,
    mut renaming: Signal<bool>,
) -> Element {
    let initial_label = config.label.clone();
    let trigger_id = editor_trigger_id(
        if is_maximized {
            "rect-max"
        } else {
            "rect-grid"
        },
        &config.id,
    );

    // True while this rect's content has a query in flight: drives the
    // corner spinner and gates the leaves' version bridges (see
    // use_version_bridge).
    let loading = use_signal(|| false);

    // Sub-type of a CDN metric, learned from the manifest's `class` field as
    // the gallery resolves. None until known; "image_gallery", "metadata",
    // or "file_list" once a manifest has been parsed. Reported in the chart
    // facts so the chart panel shows only modes that apply (e.g. hides Image
    // Gallery mode for metadata trees).
    let cdn_class = use_signal(|| Option::<String>::None);
    // AutoContent may need a registry probe when a saved Specific rect has
    // outlived its discovery metric. Keep that observed type ephemeral: the
    // editor needs the right controls, but the synthetic fallback must never
    // become a persisted user override.
    let resolved_display_type = use_signal(|| Option::<DisplayType>::None);

    let display_title = rect_title(&config);

    let mut rename_value = use_signal(move || initial_label.clone());

    let state = use_context::<DashboardState>();
    // The options panel edits the maximized chart from outside this body, so the maximized copy reports what it learns about its content; what the grid copy handed over (Configure, below) fills in what it hasn't learned yet.
    use_effect(use_reactive((&config,), move |(config,)| {
        if is_maximized {
            set_chart_facts(
                state.reported_chart_facts,
                &config,
                cdn_class.read().clone(),
                *resolved_display_type.read(),
            );
        }
    }));
    // What a maximized body learned holds only while it is up: a later maximize relearns the chart rather than trust what its content used to be.
    use_drop({
        let id = config.id.clone();
        move || {
            if is_maximized {
                let mut facts = state.reported_chart_facts;
                if facts.peek().as_ref().is_some_and(|f| f.rect_id == id) {
                    facts.set(None);
                }
            }
        }
    });
    // A maximized chart the loaded layout doesn't hold (its runs hidden, deleted in another tab, or taken out by its own sources edit) can take no edit, so it offers neither Configure nor a rename; Configure is also gone with its settings open beside it. Only the maximized copy reads these, so grid charts don't re-render as they change.
    let out_of_layout = is_maximized
        && state
            .layout_config
            .read()
            .as_ref()
            .is_some_and(|l| l.find_rect(&config.id).is_none());
    let configure_hidden = out_of_layout
        || (is_maximized
            && state
                .options_panel
                .read()
                .as_ref()
                .is_some_and(|p| p.target == PanelTarget::Chart));
    let color_ver = *state.color_version.read();
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
            chart_height: chart_height,
            color_version: color_ver,
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
                        let id = config.id.clone();
                        let mut commit_rename = move || {
                            renaming.set(false);
                            let label = normalize_rect_label(rename_value.read().clone());
                            state.edit_rect(&id, |diff, base| diff.edit_rect(base, &id, |r| r.label = label));
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
                                    if !out_of_layout {
                                        rename_value.set(label_for_click.clone());
                                        renaming.set(true);
                                    }
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
                        button {
                            class: "rect-action icon-button",
                            title: "Maximize",
                            onmousedown: primary({
                                let id = config.id.clone();
                                move |_| focus_chart(Some(id.clone()))
                            }),
                            MaximizeIcon {}
                        }
                    }
                    if !configure_hidden {
                        button {
                            id: "{trigger_id}",
                            class: "rect-action icon-button",
                            title: "Configure",
                            onmousedown: primary({
                                let rect = config.clone();
                                move |_| {
                                    state.open_options_panel(PanelTarget::Chart, trigger_id.clone());
                                    if !is_maximized {
                                        // Configure edits beside the maximized chart: maximize a grid chart, handing the maximized copy what this one already knows so the editor opens on the right sections.
                                        set_chart_facts(state.handed_over_chart_facts, &rect, cdn_class.peek().clone(), *resolved_display_type.peek());
                                        focus_chart(Some(rect.id.clone()));
                                    }
                                }
                            }),
                            GearIcon {}
                        }
                    }
                    if is_maximized {
                        button {
                            class: "rect-action icon-button",
                            title: "Close",
                            onmousedown: primary(move |_| {
                                // Closing the chart takes a chart panel along, undocking it under the pointer.
                                crate::util::panel_moved();
                                focus_chart(None);
                            }),
                            CloseIcon {}
                        }
                    } else {
                        button {
                            class: "rect-action delete-action icon-button",
                            title: "Delete chart",
                            onmousedown: primary({
                                let display_title = display_title.clone();
                                let id = config.id.clone();
                                move |_| {
                                    if confirm(&format!("Delete chart \"{display_title}\"?")) {
                                        state.delete_rect(&id);
                                    }
                                }
                            }),
                            TrashIcon {}
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
                                // The default mousedown action anchors a text selection that the drag then extends; the resize JS attaches too late (async eval) to stop it, so kill it here, as width_drag.js does for the width handles.
                                e.prevent_default();
                                let start_y = e.page_coordinates().y;
                                let rid = rect_id.clone();
                                spawn(async move {
                                    let js = resize_js::build(start_y, &rid);
                                    let mut eval = document::eval(&js);
                                    match eval.recv::<serde_json::Value>().await {
                                        Ok(val) => {
                                            let dy = val["dy"].as_f64().unwrap_or(0.0) as i32;
                                            // One gesture, two edits: the height belongs to the section, the span to the rect.
                                            if dy != 0 {
                                                let section = state.layout_config.peek().as_ref().and_then(|l| l.section_of_rect(&rid).map(str::to_string));
                                                if let Some(section) = section {
                                                    let height = (chart_height as i32 + dy).clamp(100, 800) as u32;
                                                    state.edit_section_settings(&section, |s| s.chart_height = height);
                                                } else {
                                                    crate::util::warn(&format!("[resize] {rid} is no longer in the layout; its height drag was dropped"));
                                                }
                                            }
                                            if let Some(span) = val["span"].as_u64().map(|v| v as u32) {
                                                state.edit_rect(&rid, |diff, base| diff.edit_rect(base, &rid, |r| r.options.column_span = span));
                                            }
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
    chart_height: u32,
    color_version: u64,
    options: RectOptions,
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

    // ONE resolution pass per panel — AutoContent is the parent of every content viewer. The leaves query these refs, so a run list or names landing restarts nothing whose refs are unchanged.
    let refs = use_memo(move || {
        Rc::new(
            resolve_capped_bindings(
                &bindings_signal.read(),
                &state.view_context(),
                *max_runs_signal.read(),
            )
            .into_iter()
            .map(|r| SeriesRef {
                project_id: r.project_id,
                run_id: r.run_id,
                metric_name: r.metric_name,
                tags: vec![],
            })
            .collect::<Vec<_>>(),
        )
    });
    // The one readiness rule: hold the panel until its runs and their names are known.
    let ready = use_memo(move || {
        refs_known(
            &bindings_signal.read(),
            state.current_run.read().as_deref(),
            *state.runs_loaded.read(),
        ) && {
            let known = state.known_runs();
            refs.read()
                .iter()
                .all(|r| known.contains(&(r.project_id.clone(), r.run_id.clone())))
        }
    });

    // Registry-change key for the bound runs: a metric's TYPE can only
    // change when its registry entry does (upgrade), so this re-detects
    // exactly then — not on every data flush. Split memos so a pushed
    // event recomputes only the integer hash, not the binding resolution.
    let my_metrics_gen = use_memo(move || {
        crate::state::versions_key(
            *state.resync_gen.read(),
            refs.read().iter().map(|r| r.run_id.as_str()),
            &state.metrics_gen.read(),
        )
    });

    // Zone gate + cache, like the data fetches: an out-of-band rect (mounted only mid-rename) must not turn a registry event into list_metrics traffic, and a Near/Visible re-detection defers to visible fetches. The cache keys on (metrics-gen hash, bound metric NAMES) — a binding edit that keeps the same runs but swaps metrics must not serve the old types. All-terminal passes never enter it. It lives in TYPE_CACHE so a body remounting after a scroll-away skips re-detection.
    let type_allowed = use_memo(move || *zone.read() != Zone::Far);

    enum TypeFetch {
        UseHint,
        Detected((bool, bool, bool)),
        RunUnavailable,
        /// The panel was not ready, so nothing probed. A distinct value because `use_resource` keeps it through the restart that readiness triggers: on that frame it must keep showing "Loading...", not mount the hint's leaf.
        Unready,
    }

    // Only do the expensive type detection when the hint is insufficient.
    let detected_types = use_resource({
        let cache_key = cache_key.clone();
        move || {
            let grpc = state.grpc.read().clone();
            let mg = *my_metrics_gen.read();
            let refs = refs.read().clone();
            let ready = *ready.read();
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
                // Not ready: the readiness flip restarts this resource.
                if !ready {
                    return TypeFetch::Unready;
                }
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
                for r in refs.iter() {
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
                CdnContent { refs: refs, chart_height: chart_height, cdn_display_mode: options.cdn_display_mode, loading: loading, cdn_class: cdn_class, metadata_diff_only: options.metadata_diff_only, zone: zone, cache_key: cache_key.clone() }
            },
            DisplayType::TextStream => rsx! {
                TextStreamViewer { stream_refs: refs.read().to_vec(), height: chart_height, x_axis_mode: crate::state::layout_config::XAxisMode::RelativeTime, zone: Some(zone), persist_key: cache_key.clone() }
            },
            DisplayType::Numeric => rsx! {
                NumericContent { refs: refs, chart_height: chart_height, color_version: color_version, options: options.clone(), loading: loading, zone: zone, cache_key: cache_key.clone() }
            },
        }
    };

    // Not ready: the fixed-height box holds the layout stable until the missing knowledge lands (the run list, a point lookup, or the URL run's record). No leaf is mounted, so nothing queried under it.
    if !*ready.read() {
        crate::state::heal_loading(loading);
        return rsx! { div { class: "rect-loading", style: "height: {chart_height}px;", "Loading..." } };
    }
    // A ready panel with no refs has no runs to show: knowledge, not a wait. No leaf mounts.
    if refs.read().is_empty() {
        crate::state::heal_loading(loading);
        return rsx! { div { class: "rect-empty", style: "height: {chart_height}px;", "No runs shown" } };
    }

    let read = detected_types.read();
    match &*read {
        // Type check completed
        Some(TypeFetch::Detected((has_numeric, has_cdn, has_text))) => {
            let type_count = [*has_numeric, *has_cdn, *has_text]
                .iter()
                .filter(|&&x| x)
                .count();
            if type_count > 1 {
                crate::state::heal_loading(loading);
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
            crate::state::heal_loading(loading);
            rsx! { div { class: "rect-empty", style: "height: {chart_height}px;", "Run no longer available" } }
        }
        // Still loading, and no type check is needed: use the hint.
        None if !needs_type_check => render_for_type(&display_type_hint),
        // Still loading, or the not-ready value held through the readiness restart.
        None | Some(TypeFetch::Unready) => {
            rsx! { div { class: "rect-loading", style: "height: {chart_height}px;", "Loading..." } }
        }
    }
}

#[derive(PartialEq)]
enum CdnFetch {
    Pending,
    /// RAW key series: decoration happens at render, so the fetch and its cache hold no presentation state.
    Answer(Vec<CdnSeries>),
    Unavailable,
}

#[component]
fn CdnContent(
    refs: Memo<Rc<Vec<SeriesRef>>>,
    chart_height: u32,
    cdn_display_mode: CdnDisplayMode,
    loading: Signal<bool>,
    cdn_class: Signal<Option<String>>,
    metadata_diff_only: bool,
    zone: Signal<Zone>,
    cache_key: String,
) -> Element {
    let state = use_context::<DashboardState>();

    // Same bridge and visibility gating as NumericContent, keyed on the bound runs' versions alone. Like the whole body, the gallery unmounts at Far; its data rebuilds from CDN_KEYS_CACHE and explicit navigation survives in cdn_gallery's bounded session stores (GALLERY_STEP / GALLERY_INDEX).
    let my_version = use_memo(move || {
        crate::state::versions_key(
            0,
            refs.read().iter().map(|r| r.run_id.as_str()),
            &state.run_versions.read(),
        )
    });
    let allowed = use_memo(move || *zone.read() != Zone::Far);
    // One in-flight signal for spinner + bridge, exactly as in NumericContent.
    let data_seq = crate::state::use_version_bridge(my_version, loading, allowed);
    let mut loading = loading;

    // The (refs, stamps) -> key-series memoization lives in CDN_KEYS_CACHE (module-level, survives this body unmounting at Far), so re-entering the band with nothing changed is network-free.
    // Written only by settled fetches, so deferred passes retain the last
    // series; transient failures retry and terminal failures settle once.
    let mut fetch = use_signal(|| CdnFetch::Pending);

    let _fetch = use_resource({
        let cache_key = cache_key.clone();
        move || {
            let cache_key = cache_key.clone();
            let grpc = state.grpc.read().clone();
            // The refresh heartbeat: version-bump propagations (floored and gated in use_version_bridge) restart this resource through it. Only the subscription matters — entry validity is decided by cdn_cache_hit, not the key.
            let _refresh = *data_seq.read();
            let refs = refs.read().clone();
            let allowed = *allowed.read();
            let mut cdn_class = cdn_class;
            async move {
                crate::state::heal_loading(loading);
                if !allowed {
                    return;
                }
                let series_refs = refs.to_vec();

                let cached = CDN_KEYS_CACHE.with(|c| c.borrow_mut().get(&cache_key));
                // Any cached entry = the gallery already shows something: a
                // refresh, not a first paint, for gate purposes.
                let first_paint = cached.is_none();
                let series = match cached.and_then(|(crefs, cstamps, cseries)| {
                    cdn_cache_hit(&crefs, &cstamps, &series_refs, &state.run_versions.peek())
                        .then_some(cseries)
                }) {
                    Some(series) => series,
                    None => {
                        loading.set(true);
                        let response = visibility::retry_visible_run("cdn keys", async || {
                            let _hi = visibility::admit_fetch(|| *zone.peek(), first_paint).await;
                            let sent = crate::state::versions_of(
                                &state.run_versions.peek(),
                                series_refs.iter().map(|s| s.run_id.as_str()),
                            );
                            grpc.query_cdn_keys(series_refs.clone())
                                .await
                                .map(|response| (sent, response))
                        })
                        .await;
                        loading.set(false);
                        let (sent, response) = match response {
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
                        let stamps = crate::state::answer_stamps(
                            state.run_versions,
                            response.run_versions,
                            sent,
                        );
                        let series = response.series;
                        let weight = cdn_cache_heap_bytes(&series_refs, &stamps, &series);
                        CDN_KEYS_CACHE.with(|c| {
                            c.borrow_mut().put_weighted(
                                cache_key,
                                (series_refs, stamps, series.clone()),
                                weight,
                            )
                        });
                        series
                    }
                };

                if !matches!(&*fetch.peek(), CdnFetch::Answer(current) if current == &series) {
                    fetch.set(CdnFetch::Answer(series));
                }
            }
        }
    });

    let shown = fetch.read();
    match &*shown {
        CdnFetch::Answer(series) => {
            // Decoration at render: names, ordinals and colors come from the live run list and the color overrides, so the resource above fetches and caches RAW key series only, and a metadata change re-decorates without restarting or cancelling the key query.
            // Run colors live in localStorage, outside Dioxus. The sidebar bumps this signal after an override changes; tracking it here rebuilds presentation without querying the key series again.
            let _color_version = *state.color_version.read();
            let run_data = decorate_cdn_series(
                series.clone(),
                &state.display_runs(),
                crate::components::uplot_chart::run_color,
            );
            rsx! {
                CdnGallery { runs: run_data, height: chart_height, display_mode: cdn_display_mode, cdn_class: cdn_class, metadata_diff_only: metadata_diff_only, persist_key: cache_key.clone() }
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

#[cfg(test)]
mod tests {
    use super::{
        cdn_cache_heap_bytes, cdn_cache_hit, decorate_cdn_series, normalize_rect_label, refs_known,
    };
    use crate::grpc::proto::{CdnEntry, CdnSeries, RunInfo, RunStatus, SeriesRef};
    use crate::state::layout_config::{MetricBinding, ProjectRef, RunRef};
    use std::collections::HashMap;

    fn binding(runs: RunRef) -> MetricBinding {
        MetricBinding {
            project: ProjectRef::Current,
            runs,
            metric_name: "loss".to_string(),
        }
    }

    #[test]
    fn refs_known_needs_the_list_only_for_selected_without_a_run_and_all() {
        let specific = || binding(RunRef::Specific(vec!["r".into()]));
        let selected = || binding(RunRef::Selected);
        let all = || binding(RunRef::All);

        // Before the list lands, only Specific (and Selected on a run page) is known.
        assert!(refs_known(&[specific()], None, false));
        assert!(!refs_known(&[selected()], None, false));
        assert!(!refs_known(&[all()], None, false));
        assert!(refs_known(&[selected(), specific()], Some("run"), false));
        assert!(!refs_known(&[all()], Some("run"), false));

        // The list landing makes every form known, run page or not.
        assert!(refs_known(&[selected(), all()], None, true));
    }

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
        let first = decorate_cdn_series(series.clone(), &[run()], |_, _| "#111111".into());
        let recolored = decorate_cdn_series(series, &[run()], |_, _| "#222222".into());

        assert_eq!(first[0].label, "Named");
        assert_eq!(first[0].keys, recolored[0].keys);
        assert_eq!(first[0].color, "#111111");
        assert_eq!(recolored[0].color, "#222222");

        // Mixed metrics label and color by run/metric instead.
        let mixed = vec![cdn_series("images"), cdn_series("masks")];
        let mixed_a = decorate_cdn_series(mixed.clone(), &[run()], |_, _| "#111111".into());
        let mixed_b = decorate_cdn_series(mixed, &[run()], |_, _| "#222222".into());
        assert_eq!(mixed_a[0].label, "Named/images");
        assert_eq!(mixed_a[0].color, mixed_b[0].color);
    }

    #[test]
    fn cdn_decoration_marks_ended_runs() {
        let ended = |runs: &[RunInfo]| {
            decorate_cdn_series(vec![cdn_series("images")], runs, |_, _| "#111111".into())[0].ended
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
        let stamps = HashMap::from([("run".to_string(), 7u64)]);
        let mut series = vec![cdn_series("images")];
        let one_key = cdn_cache_heap_bytes(&refs, &stamps, &series);

        series[0].entries.push(CdnEntry {
            step: 8,
            cdn_key: "x".repeat(256),
        });
        let two_keys = cdn_cache_heap_bytes(&refs, &stamps, &series);

        assert!(two_keys >= one_key.saturating_add(256));
    }

    fn cdn_ref(run: &str) -> SeriesRef {
        SeriesRef {
            project_id: "project".into(),
            run_id: run.into(),
            metric_name: "images".into(),
            tags: vec![],
        }
    }

    #[test]
    fn cdn_cache_hit_needs_matching_refs_and_covering_stamps() {
        let request = vec![cdn_ref("a"), cdn_ref("b")];
        let stamped: HashMap<String, u64> = [("a".into(), 5), ("b".into(), 5)].into();
        let at = |a: u64, b: u64| HashMap::from([("a".to_string(), a), ("b".to_string(), b)]);

        assert!(cdn_cache_hit(&request, &stamped, &request, &at(3, 0)));
        // One run above its stamp suffices: the reply may predate that version's rows.
        assert!(!cdn_cache_hit(&request, &stamped, &request, &at(5, 6)));
        // Refs are the entry's identity: added, dropped, or reordered refs never hit.
        assert!(!cdn_cache_hit(
            &request,
            &stamped,
            &[cdn_ref("a")],
            &at(5, 5)
        ));
        assert!(!cdn_cache_hit(
            &request,
            &stamped,
            &[cdn_ref("b"), cdn_ref("a")],
            &at(5, 5)
        ));
        assert!(!cdn_cache_hit(
            &[cdn_ref("a")],
            &[("a".into(), 5)].into(),
            &request,
            &at(5, 5)
        ));
    }
}
