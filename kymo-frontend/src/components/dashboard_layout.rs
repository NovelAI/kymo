use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use dioxus::prelude::*;
use futures::{stream, StreamExt};
use gloo_timers::future::sleep;

use crate::components::metric_rect::MetricRect;
use crate::components::navbar::Navbar;
use crate::components::sidebar::Sidebar;
use crate::components::uplot_chart::ZoomBridge;
use crate::grpc::proto::{MetricInfo, RunLifecycleState};
use crate::route::{focus_chart, Route};
use crate::state::app_state::{ExplicitRunKey, ExplicitRunMetadata};
use crate::state::layout_config::{resolve_rect_locally, RectConfig, RunRef};
use crate::state::push::PushBridge;
use crate::state::visibility::{
    is_terminal_run_status, retry_visible, retry_visible_run, visible_attempt,
};
use crate::state::zones::{ZoneBridge, ZoneRegistry};
use crate::state::{
    load_diff_or_route, DashboardState, DirectRunLoad, DirectRunView, LayoutConfig, MaximizedRect,
};
use crate::util::{focus_on_mount, js_bridge::js_string, primary, TOP_LAYER_SELECTOR};

const EXPLICIT_METADATA_CONCURRENCY: usize = 16;
/// Backoff between retry passes over keys whose lookup failed transiently.
/// Matches `retry_visible_while`'s interval — same failure, same patience.
const EXPLICIT_METADATA_RETRY: Duration = Duration::from_secs(5);

/// Available chart height after the measured content area and font-sized chrome.
static MAXIMIZED_CHART_HEIGHT: GlobalSignal<u32> = Signal::global(|| 280);

/// First measurement immediate, later ones debounced 100 ms so a window drag settles before the maximized chart follows. Mounted through the js_bridge registry so an unmounted layout never leaves an observer and its timer alive.
const MAXIMIZE_RESIZE_JS: &str = r#"(()=>{
const el=document.querySelector('.main-wrap');if(!el)return;
let last=-1,t=0;
function send(){
  const style=getComputedStyle(el);
  // Match .maximize-overlay padding and .metric-rect's padding + 1lh title.
  const chrome=2*parseFloat(style.getPropertyValue('--spacing-md'))
    +2*parseFloat(style.getPropertyValue('--spacing-xs'))+parseFloat(style.lineHeight);
  const h=Math.max(0,Math.floor(el.clientHeight-chrome));
  // A NaN frame (e.g. a stylesheet that failed to load) would close the Rust u32 receiver.
  if(!Number.isFinite(h)||h===last)return;last=h;dioxus.send(h);
}
const ro=new ResizeObserver(()=>{clearTimeout(t);if(last<0)send();else t=setTimeout(send,100);});
window.__kymo_bridges.mount(__BRIDGE_NAME__,__BRIDGE_OWNER__,()=>{clearTimeout(t);ro.disconnect();});
ro.observe(el);
})()"#;

/// Esc closes the maximized chart wherever focus is, including `<body>` and the notice bar outside the app shell. On `window` it runs after the in-app handlers and bulk mode's `document` listener, and skips Esc they consumed (`defaultPrevented`), IME composition Esc, and Esc an open native dialog or popover will close.
const MAXIMIZE_ESCAPE_JS: &str = r#"(()=>{
function key(e){if(e.key==='Escape'&&!e.isComposing&&!e.defaultPrevented&&!document.querySelector(__TOP_LAYER_SELECTOR__)){try{dioxus.send(true);}catch(_){td();}}}
const td=window.__kymo_bridges.mount(__BRIDGE_NAME__,__BRIDGE_OWNER__,()=>window.removeEventListener('keydown',key));
window.addEventListener('keydown',key);
})()"#;

fn explicit_run_keys(layout: &LayoutConfig, current_project: &str) -> Vec<(String, String)> {
    let mut keys = layout
        .sections
        .iter()
        .flat_map(|section| &section.rects)
        .flat_map(|rect| &rect.bindings)
        .filter_map(|binding| match &binding.runs {
            RunRef::Specific(run_ids) => {
                Some((binding.project.id(current_project).to_string(), run_ids))
            }
            _ => None,
        })
        .flat_map(|(project_id, run_ids)| {
            run_ids
                .iter()
                .cloned()
                .map(move |run_id| (project_id.clone(), run_id))
        })
        .collect::<Vec<_>>();
    keys.sort_unstable();
    keys.dedup();
    keys
}

fn explicit_metadata_plan(
    layout: &LayoutConfig,
    current_project: &str,
    active_keys: &HashSet<ExplicitRunKey>,
    project_versions: &HashMap<String, u64>,
) -> Vec<(String, String, Option<u64>)> {
    explicit_run_keys(layout, current_project)
        .into_iter()
        // Current-project active rows already come from ListRuns. Every other
        // Specific identity needs a point lookup, including a cached one after
        // its project's metadata version changes.
        .filter(|key| !active_keys.contains(key))
        .map(|(project_id, run_id)| {
            // Unknown is not version zero: projects legitimately start at
            // zero, and fetching before the resync poll seeds this value makes
            // every newly seeded project cancel and restart the whole fan-out.
            let version = project_versions.get(&project_id).copied();
            (project_id, run_id, version)
        })
        .collect()
}

/// Keys still owing a lookup: no settled entry at all, or one settled at an
/// older project generation. Entries settled at the current generation —
/// `Present` or `Absent` alike — are done, and a key whose project version is
/// still unknown is not askable yet.
///
/// The store is keyed to the binding, not remembered by observation, so this
/// needs no notion of cache capacity or eviction: an entry disappears only
/// when its binding does.
fn pending_metadata_lookups(
    plan: &[(String, String, Option<u64>)],
    store: &BTreeMap<ExplicitRunKey, ExplicitRunMetadata>,
) -> Vec<(String, String, u64)> {
    plan.iter()
        .filter_map(|(project_id, run_id, project_version)| {
            let project_version = (*project_version)?;
            let key = (project_id.clone(), run_id.clone());
            let settled = store
                .get(&key)
                .is_some_and(|entry| entry.generation() == project_version);
            (!settled).then(|| (project_id.clone(), run_id.clone(), project_version))
        })
        .collect()
}

/// Whether a failed lookup SETTLES the key for this generation rather than
/// being worth another attempt. `NotFound`/`FailedPrecondition` are the run
/// lifecycle's terminal states; `InvalidArgument` is the binding itself being
/// unaskable — a saved layout can carry a key this server will never accept,
/// and retrying it forever would spin until the generation moves.
///
/// Deliberately NOT folded into `is_terminal_run_status`: that predicate
/// answers a question about the run's lifecycle, and an unaskable request is
/// not a lifecycle state.
fn metadata_lookup_settles(status: &tonic::Status) -> bool {
    is_terminal_run_status(status) || status.code() == tonic::Code::InvalidArgument
}

#[component]
pub fn DashboardLayout(project_id: String) -> Element {
    let mut state = use_context_provider(|| DashboardState::new(project_id.clone()));
    // Slot zone signals, fed by ZoneBridge below; MetricRect slots register here.
    use_context_provider(ZoneRegistry::default);
    let maximize_bridge = crate::util::js_bridge::use_bridge("maximize_resize");

    // Connection resync bootstraps the list; other invalidations use runs_refresh.
    let _runs_fetch = use_resource({
        let grpc = state.grpc;
        let project_id = project_id.clone();
        let mut runs_signal = state.runs;
        let mut runs_loaded = state.runs_loaded;
        let runs_refresh = state.runs_refresh;
        move || {
            let project_id = project_id.clone();
            let _requested_refresh = *runs_refresh.read();
            let resync_gen = *state.resync_gen.read();
            async move {
                if resync_gen == 0 {
                    return;
                }
                let grpc = grpc.read().clone();
                let snapshot =
                    retry_visible("list_runs", async || grpc.list_runs(&project_id).await).await;
                let new_runs = snapshot.runs;
                // peek: this resource writes runs_signal, and async-body
                // reads subscribe — a read would rerun it per refresh.
                let old_runs = runs_signal.peek().clone();
                // RunInfo derives PartialEq via prost, so this catches any
                // field change — including liveness `status`.
                if old_runs != new_runs {
                    let old_ids: std::collections::HashSet<String> =
                        old_runs.iter().map(|r| r.run_id.clone()).collect();
                    let new_ids: std::collections::HashSet<String> =
                        new_runs.iter().map(|r| r.run_id.clone()).collect();
                    let removed_runs = old_runs
                        .iter()
                        .filter(|run| !new_ids.contains(&run.run_id))
                        .cloned()
                        .collect::<Vec<_>>();
                    state.remember_display_runs(&removed_runs);
                    // Hidden/removed runs leave the project selection, while
                    // genuinely new runs retain the existing auto-select
                    // behavior. A direct run route still scopes itself from
                    // the URL, independently of this project-list selection.
                    let mut next_selected = state.selected_runs.peek().clone();
                    next_selected.retain(|run_id| new_ids.contains(run_id));
                    if old_ids.is_empty() {
                        next_selected.extend(new_ids.iter().cloned());
                    } else {
                        next_selected.extend(new_ids.difference(&old_ids).cloned());
                    }
                    if *state.selected_runs.peek() != next_selected {
                        state.selected_runs.set(next_selected);
                    }
                    runs_signal.set(new_runs);
                }
                // Coverage certifies these rows; a legacy response must clear earlier certification.
                state.runs_project_version.set(snapshot.project_version);
                if !*runs_loaded.peek() {
                    runs_loaded.set(true);
                }
            }
        }
    });

    // Discovery scope: the layout is built from the VISIBLE runs' metrics —
    // the run page's run, or the project page's selection — so a chart
    // exists only when a run the user can see logged its metric. The route
    // is the authority for the run page: `current_run` is set by the child
    // page only after the layout's first render, so keying on it would
    // race the loader's first pass.
    let route = use_route::<Route>();
    crate::route::note_route(&route);
    let scope_run = match &route {
        Route::RunPage { run_id, .. } => Some(run_id.clone()),
        _ => None,
    };
    let mut scope_signal = use_signal(|| scope_run.clone());
    if *scope_signal.peek() != scope_run {
        scope_signal.set(scope_run);
    }

    // A direct run page has a point lookup independent of the active run
    // list. That is what keeps a deleted run viewable without reintroducing
    // it into the sidebar, project selection, or All-run bindings.
    let _direct_run_fetch = use_resource({
        let project_id = project_id.clone();
        move || {
            let project_id = project_id.clone();
            let run_id = scope_signal.read().clone();
            let _manual_refresh = *state.direct_run_refresh.read();
            let resync_gen = *state.resync_gen.read();
            async move {
                let Some(run_id) = run_id else {
                    let mut direct = state.direct_run;
                    if !matches!(&*direct.peek(), DirectRunLoad::Idle) {
                        direct.set(DirectRunLoad::Idle);
                    }
                    return;
                };

                let mut direct = state.direct_run;
                let already_showing_this_run = matches!(&*direct.peek(),
                    DirectRunLoad::Loaded(view) if view.matches(&project_id, &run_id)
                );
                if !already_showing_this_run {
                    direct.set(DirectRunLoad::Loading);
                }
                if resync_gen == 0 {
                    return;
                }
                let grpc = state.grpc.read().clone();
                match retry_visible_run("get_run", async || {
                    grpc.get_run(&project_id, &run_id).await
                })
                .await
                {
                    Ok(response) => match response.run {
                        Some(record)
                            if record.run.as_ref().is_some_and(|run| {
                                run.project_id == project_id && run.run_id == run_id
                            }) =>
                        {
                            if let Some(run) = record.run.as_ref() {
                                state.remember_display_runs(std::slice::from_ref(run));
                            }
                            direct.set(DirectRunLoad::Loaded(DirectRunView::new(
                                record,
                                response.server_now_ms,
                            )))
                        }
                        Some(_) => direct.set(DirectRunLoad::Error(
                            "The server returned a different run.".to_string(),
                        )),
                        None => direct.set(DirectRunLoad::NotFound),
                    },
                    Err(status) if crate::state::visibility::is_terminal_run_status(&status) => {
                        direct.set(DirectRunLoad::NotFound)
                    }
                    Err(_) => unreachable!("retry_visible_run returns only lifecycle errors"),
                }
            }
        }
    });

    let selected_runs = state.selected_runs;
    let visible_run_ids = use_memo(move || match scope_signal.read().clone() {
        Some(rid) => vec![rid],
        None => {
            // Sorted so equal selections compare equal (HashSet iteration
            // order is arbitrary and a memo dedups by value).
            let mut ids: Vec<String> = selected_runs.read().iter().cloned().collect();
            ids.sort_unstable();
            ids
        }
    });

    // URL → overlay: `?chart=<rect id>` is the maximize overlay's source of truth — chart links open focused, and every dismissal goes through `focus_chart`, which rewrites the param and lands back here.
    // Mirrored into a signal like `scope_signal` above, since effects only re-run on reactive reads.
    let chart_param = route.chart_param();
    let mut chart_signal = use_signal(|| chart_param.clone());
    if *chart_signal.peek() != chart_param {
        chart_signal.set(chart_param);
    }
    use_effect(move || {
        let param = chart_signal.read().clone();
        let layout = state.layout_config.read().clone();
        let mut maximized = state.maximized;
        match param {
            None => {
                if maximized.peek().is_some() {
                    maximized.set(None);
                }
            }
            // The loaded layout is the authority, but a chart link must not wait for its metrics sweep (tens of seconds on big projects), so until it holds the rect this falls back to `resolve_rect_locally`.
            // Once open, the overlay is left alone: it re-resolves against the live layout each render, upgrading a fallback config when the sweep lands.
            Some(id) => {
                if maximized.peek().as_ref().is_some_and(|m| m.config.id == id) {
                    return;
                }
                let resolved = layout
                    .as_ref()
                    .and_then(|l| l.resolve_rect(&id))
                    .or_else(|| resolve_rect_locally(&state.peek_diff(), &id));
                if let Some((config, max_columns)) = resolved {
                    maximized.set(Some(MaximizedRect {
                        config,
                        max_columns,
                    }));
                }
            }
        }
    });

    // Registry-change key for the VISIBLE run set: the loader restarts when
    // a pushed metric-registry event touches a run it can see, or a resync
    // invalidates everything. Data landing on known metrics moves
    // run_versions, not this; unselected runs' registry events don't matter
    // until selection changes visible_run_ids itself.
    let metrics_gen_signal = state.metrics_gen;
    let resync_gen_signal = state.resync_gen;
    let metrics_key = use_memo(move || {
        crate::state::versions_key(
            *resync_gen_signal.read(),
            visible_run_ids.read().iter().map(String::as_str),
            &metrics_gen_signal.read(),
        )
    });

    // A run page's layout gates on its run's readability alone, so a GetRun refresh that leaves the run readable does not re-list its metrics.
    let scope_readable = use_memo({
        let project_id = project_id.clone();
        move || {
            let Some(run_id) = &*scope_signal.read() else {
                return true;
            };
            match &*state.direct_run.read() {
                DirectRunLoad::Loaded(view) if view.matches(&project_id, run_id) => matches!(
                    crate::state::trash::effective_lifecycle(
                        &view.record,
                        view.authoritative_now_ms()
                    ),
                    RunLifecycleState::Active | RunLifecycleState::Trashed
                ),
                _ => false,
            }
        }
    });
    let _layout_fetch = use_resource({
        let grpc = state.grpc;
        let project_id = project_id.clone();
        let runs_loaded = state.runs_loaded;
        let mut layout = state.layout_config;
        let mut base_layout = state.base_layout;
        let layout_gen = state.layout_generation;
        move || {
            let project_id = project_id.clone();
            let visible = visible_run_ids.read().clone();
            let _mk = *metrics_key.read();
            let runs_loaded = *runs_loaded.read();
            let _gen = *layout_gen.read();
            let scope_readable = *scope_readable.read();
            async move {
                if !scope_readable {
                    return;
                }
                // Surface a corrupt saved diff before anything else — even
                // the runs gate: the recovery page must be reachable when
                // the server isn't.
                if load_diff_or_route(&project_id, "load gate").is_none() {
                    return;
                }
                // An empty selection only means "nothing visible" once the
                // first list_runs has landed (auto-select fills it); before
                // that, rendering would flash an empty dashboard.
                if !runs_loaded {
                    return;
                }
                let grpc = grpc.read().clone();
                // One aggregated registry query for the visible run set;
                // reloads are event-driven (visible set, registry pushes,
                // resyncs restart the resource).
                let all_metrics: Vec<MetricInfo> = if scope_signal.peek().is_some() {
                    // A deleted direct run crosses a real terminal boundary.
                    // Do not feed NOT_FOUND/FAILED_PRECONDITION into the
                    // generic forever-retry helper; refresh GetRun so the
                    // page can render its expired/purged shell instead.
                    match retry_visible_run("direct layout metrics", async || {
                        grpc.list_run_set_metrics(&project_id, &visible).await
                    })
                    .await
                    {
                        Ok(metrics) => metrics,
                        Err(status)
                            if crate::state::visibility::is_terminal_run_status(&status) =>
                        {
                            let next = state.direct_run_refresh.peek().wrapping_add(1);
                            state.direct_run_refresh.set(next);
                            return;
                        }
                        Err(_) => unreachable!("retry_visible_run returns only lifecycle errors"),
                    }
                } else {
                    retry_visible("layout metrics", async || {
                        grpc.list_run_set_metrics(&project_id, &visible).await
                    })
                    .await
                };
                // Read the saved diff *after* the awaits: an edit made
                // while the fetch was in flight would otherwise be
                // clobbered by a stale pre-edit snapshot.
                let Some(diff) = load_diff_or_route(&project_id, "refresh") else {
                    return;
                };
                // Zero runs / zero metrics still renders: the user's own
                // sections (which may bind other projects' metrics) apply
                // on top of an empty base, and the empty state offers the
                // add UI instead of a dead "Loading..." screen.
                let base = LayoutConfig::auto_generate(&all_metrics);
                let shown = diff.apply(&base);
                // Skip no-op sets: Signal::set notifies subscribers even
                // for equal values.
                if base_layout.peek().as_ref() != Some(&base) {
                    base_layout.set(Some(base));
                }
                if layout.peek().as_ref() != Some(&shown) {
                    layout.set(Some(shown));
                }
            }
        }
    });

    // Saved Specific bindings remain readable while their runs are in Trash,
    // and cross-project active bindings need names despite not belonging to
    // this page's ListRuns. Key each lookup on that project's metadata version:
    // RenameRun then refreshes RunInfo labels without touching run_versions
    // (and therefore without refetching chart data).
    let explicit_metadata = use_memo({
        let project_id = project_id.clone();
        move || {
            let layout = state.layout_config.read();
            let Some(layout) = layout.as_ref() else {
                return Vec::new();
            };
            let active_keys = state
                .runs
                .read()
                .iter()
                .map(|run| (run.project_id.clone(), run.run_id.clone()))
                .collect::<HashSet<_>>();
            explicit_metadata_plan(
                layout,
                &project_id,
                &active_keys,
                &state.project_versions.read(),
            )
        }
    });
    // Lookups land independently under a bounded concurrency limit so a large
    // saved layout cannot flood the point RPC.
    //
    // The resource is keyed on the PLAN and only ever peeks the store it
    // writes, so publishing a completed lookup cannot cancel the lookups still
    // in flight beside it. That is what lets each name appear as it arrives:
    // one key that keeps failing no longer withholds every key that succeeded.
    // Retries live inside the pass for the same reason — restarting the
    // resource to retry would re-issue the whole plan.
    let _explicit_metadata_fetch = use_resource(move || {
        let plan = explicit_metadata.read().clone();
        async move {
            let live_keys = plan
                .iter()
                .map(|(project_id, run_id, _)| (project_id.clone(), run_id.clone()))
                .collect::<HashSet<_>>();
            state.prune_explicit_metadata(&live_keys);
            let grpc = state.grpc.read().clone();

            loop {
                let pending = pending_metadata_lookups(&plan, &state.explicit_run_metadata.peek());
                if pending.is_empty() {
                    return;
                }
                let attempts = pending.into_iter().map(|(bound_project, run_id, generation)| {
                    let grpc = grpc.clone();
                    async move {
                        // One attempt per pass, each parked while the tab is
                        // hidden: buffer_unordered admits requests as slots
                        // free, so gating the pass as a whole would let a late
                        // request fire into a backgrounded tab.
                        let result = visible_attempt(async || {
                            grpc.get_run(&bound_project, &run_id).await
                        })
                        .await;
                        let key = (bound_project.clone(), run_id.clone());
                        match result {
                            Ok(response) => {
                                let run = response.run.and_then(|record| record.run);
                                // A server answering with someone else's run is
                                // neither present nor settled — drop it and let
                                // the next pass ask again.
                                if run.as_ref().is_some_and(|run| {
                                    run.project_id != bound_project || run.run_id != run_id
                                }) {
                                    return (key, None);
                                }
                                let entry = match run {
                                    Some(run) => ExplicitRunMetadata::Present { run, generation },
                                    None => ExplicitRunMetadata::Absent { generation },
                                };
                                (key, Some(entry))
                            }
                            Err(status) if metadata_lookup_settles(&status) => {
                                (key, Some(ExplicitRunMetadata::Absent { generation }))
                            }
                            // Transient: leave the key unknown so this pass
                            // retries it, and publish nothing that would settle
                            // it until the generation moves.
                            Err(status) => {
                                crate::util::warn(&format!(
                                    "[specific run metadata] {bound_project}/{run_id} failed: {status}; retrying"
                                ));
                                (key, None)
                            }
                        }
                    }
                });
                let mut transient = false;
                let mut results =
                    stream::iter(attempts).buffer_unordered(EXPLICIT_METADATA_CONCURRENCY);
                while let Some((key, entry)) = results.next().await {
                    match entry {
                        // Publish per completion. The plan is re-read on
                        // restart, and the generation is carried in the entry,
                        // so a result for a binding that vanished mid-pass is
                        // pruned by the next pass rather than filtered here.
                        Some(entry) => state.record_explicit_metadata(key, entry),
                        None => transient = true,
                    }
                }
                if !transient {
                    return;
                }
                sleep(EXPLICIT_METADATA_RETRY).await;
            }
        }
    });

    // Double duty: freezes the grid's scroll (CSS overflow) and tells the zone watcher (state/zones.rs) to cap covered slots at Near while the overlay is up.
    let main_class = if state.maximized.read().is_some() {
        "main-content main-content-locked"
    } else {
        "main-content"
    };

    // Deleted run metadata comes from the point lookup and is never inserted
    // into the active list just to name its direct page.
    let title = match &route {
        Route::RunPage {
            project_id, run_id, ..
        } => state
            .runs
            .read()
            .iter()
            .find(|run| run.run_id == *run_id)
            .map(|run| run.run_name.clone())
            .or_else(|| match &*state.direct_run.read() {
                DirectRunLoad::Loaded(view) => view
                    .record
                    .run
                    .as_ref()
                    .filter(|run| run.project_id == *project_id && run.run_id == *run_id)
                    .map(|run| run.run_name.clone()),
                _ => None,
            })
            .unwrap_or_else(|| run_id.clone()),
        _ => project_id,
    };

    rsx! {
        document::Title { "{title}" }
        div {
            class: "app-shell",
            onmounted: move |_| {
                let js = maximize_bridge.script(MAXIMIZE_RESIZE_JS);
                spawn(async move {
                    let mut eval = document::eval(&js);
                    while let Ok(height) = eval.recv::<u32>().await {
                        *MAXIMIZED_CHART_HEIGHT.write() = height;
                    }
                });
            },
            Navbar {}
            div { class: "content-row",
                if state.current_run.read().is_none() {
                    Sidebar {}
                }
                // The wrap is the main column's non-scrolling box: the
                // overlay anchors to it absolutely, covering exactly the
                // visible chart area while the sidebar stays visible and
                // usable (run toggles update the maximized chart live).
                // The overlay must be a SIBLING of the scrolling <main>,
                // not its child — anchored inside the scroll container it
                // would sit at the top of the scrollable content,
                // off-screen for anyone who maximized from below the fold.
                div { class: "main-wrap",
                    main { class: "{main_class}",
                        Outlet::<Route> {}
                    }
                    MaximizeOverlay {}
                }
            }
            PushBridge {}
            ZoneBridge {}
            ZoomBridge {}
        }
    }
}

/// Overlay beside `<main>` that fills its column while the sidebar stays visible.
/// `DashboardState::maximized` holds a snapshot for fallback, but the rect
/// is re-resolved against the live layout each render, so edits land
/// against current values rather than the (possibly drifted) snapshot
/// taken when the overlay opened; saves route through
/// `DashboardState::update_rect` like any other edit.
#[component]
fn MaximizeOverlay() -> Element {
    let state = use_context::<DashboardState>();
    let mut maximized_signal = state.maximized;

    let escape_bridge = crate::util::js_bridge::use_bridge("maximize_escape");
    use_future(move || {
        let js = escape_bridge
            .script(MAXIMIZE_ESCAPE_JS)
            .replace("__TOP_LAYER_SELECTOR__", &js_string(TOP_LAYER_SELECTOR));
        async move {
            let mut eval = document::eval(&js);
            while eval.recv::<bool>().await.is_ok() {
                if maximized_signal.peek().is_some() {
                    focus_chart(None);
                }
            }
        }
    });

    let view = match maximized_signal.read().clone() {
        Some(v) => v,
        None => return rsx! {},
    };

    let rect_id = view.config.id.clone();
    let max_columns = view.max_columns;
    let rect_config = state
        .layout_config
        .read()
        .as_ref()
        .and_then(|l| l.find_rect(&rect_id).cloned())
        .unwrap_or(view.config);

    // The content area already excludes the navbar and any notice bar.
    let chart_height = *MAXIMIZED_CHART_HEIGHT.read();

    rsx! {
        div {
            class: "maximize-overlay",
            // Focus on mount so keyboard navigation starts inside the overlay, not on the grid control (e.g. the Maximize button) left focused beneath it.
            tabindex: "-1",
            onmounted: focus_on_mount,
            // Dismiss on click (mouseup) can be annoying if you drag the x-axis and release in the border.
            onmousedown: primary(move |_| focus_chart(None)),
            div {
                class: "maximize-content",
                onmousedown: move |e| e.stop_propagation(),
                MetricRect {
                    // Keyed: ?chart= can jump straight from one maximized rect to another (chart page links), and a reused instance would keep the previous rect's use_hook state (cache key).
                    key: "{rect_id}",
                    config: rect_config,
                    chart_height: chart_height,
                    max_columns: max_columns,
                    is_maximized: true,
                    on_update: move |new_rect: RectConfig| {
                        if state.update_rect(new_rect.clone()) {
                            maximized_signal.set(Some(MaximizedRect {
                                config: new_rect,
                                max_columns,
                            }));
                        } else {
                            // The rect dropped out of a regenerated layout
                            // (its run/metric vanished mid-session). Closing
                            // beats keeping an overlay that pretends the
                            // edit applied.
                            focus_chart(None);
                        }
                    },
                    on_delete: {
                        let rect_id = rect_id.clone();
                        move |_| {
                            state.delete_rect(&rect_id);
                            focus_chart(None);
                        }
                    },
                    on_resize: move |_| {},
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};

    use super::{
        explicit_metadata_plan, explicit_run_keys, metadata_lookup_settles,
        pending_metadata_lookups, ExplicitRunMetadata,
    };
    use crate::grpc::proto::RunInfo;
    use crate::state::layout_config::{
        DisplayType, LayoutConfig, MetricBinding, ProjectRef, RectConfig, RectOptions, RunRef,
        SectionConfig,
    };

    #[test]
    fn maximize_resize_uses_the_shared_lifecycle() {
        crate::util::js_bridge::validate_template(super::MAXIMIZE_RESIZE_JS);
    }

    #[test]
    fn maximize_escape_uses_the_shared_lifecycle() {
        crate::util::js_bridge::validate_template(super::MAXIMIZE_ESCAPE_JS);
    }

    fn explicit_layout() -> LayoutConfig {
        let rect = RectConfig {
            id: "rect".to_string(),
            label: String::new(),
            bindings: vec![
                MetricBinding {
                    project: ProjectRef::Current,
                    runs: RunRef::Specific(vec!["current-run".to_string()]),
                    metric_name: "loss".to_string(),
                },
                MetricBinding {
                    project: ProjectRef::Specific("other".to_string()),
                    runs: RunRef::Specific(vec!["other-run".to_string()]),
                    metric_name: "loss".to_string(),
                },
                MetricBinding {
                    project: ProjectRef::Current,
                    runs: RunRef::All,
                    metric_name: "loss".to_string(),
                },
            ],
            display_type: DisplayType::Numeric,
            options: RectOptions::default(),
        };
        LayoutConfig {
            sections: vec![SectionConfig::auto("section".to_string(), vec![rect])],
        }
    }

    #[test]
    fn metadata_lookup_includes_only_explicit_run_bindings() {
        assert_eq!(
            explicit_run_keys(&explicit_layout(), "current"),
            vec![
                ("current".to_string(), "current-run".to_string()),
                ("other".to_string(), "other-run".to_string()),
            ]
        );
    }

    #[test]
    fn metadata_plan_versions_cross_project_bindings_only() {
        let layout = explicit_layout();
        let active = HashSet::from([("current".to_string(), "current-run".to_string())]);
        let mut versions = HashMap::from([
            ("current".to_string(), 3),
            ("other".to_string(), 7),
            ("unrelated".to_string(), 11),
        ]);

        let first = explicit_metadata_plan(&layout, "current", &active, &versions);
        assert_eq!(
            first,
            vec![("other".to_string(), "other-run".to_string(), Some(7))]
        );

        versions.insert("unrelated".to_string(), 12);
        assert_eq!(
            explicit_metadata_plan(&layout, "current", &active, &versions),
            first
        );

        versions.insert("other".to_string(), 8);
        assert_eq!(
            explicit_metadata_plan(&layout, "current", &active, &versions),
            vec![("other".to_string(), "other-run".to_string(), Some(8))]
        );

        versions.remove("other");
        let unknown = explicit_metadata_plan(&layout, "current", &active, &versions);
        assert_eq!(
            unknown,
            vec![("other".to_string(), "other-run".to_string(), None)]
        );
        assert!(pending_metadata_lookups(&unknown, &BTreeMap::new()).is_empty());
    }

    #[test]
    fn metadata_fetches_only_unsettled_or_stale_generations() {
        let plan = vec![
            ("p".to_string(), "present".to_string(), Some(4)),
            ("p".to_string(), "absent".to_string(), Some(4)),
        ];
        let present_key = ("p".to_string(), "present".to_string());
        let absent_key = ("p".to_string(), "absent".to_string());
        let mut store = BTreeMap::from([
            (
                present_key.clone(),
                ExplicitRunMetadata::Present {
                    run: RunInfo {
                        project_id: "p".to_string(),
                        run_id: "present".to_string(),
                        run_name: "named".to_string(),
                        ordinal: 1,
                        created_at_ms: 0,
                        status: 0,
                        last_ingested_at_ms: None,
                        terminated_at_ms: None,
                    },
                    generation: 4,
                },
            ),
            (
                absent_key.clone(),
                ExplicitRunMetadata::Absent { generation: 4 },
            ),
        ]);

        // Both outcomes settle the key: an absent run is as answered as a
        // present one, and neither is re-asked while the generation holds.
        assert!(pending_metadata_lookups(&plan, &store).is_empty());

        // A key with no entry is unknown, not settled — this is the state a
        // transient failure leaves behind, and it is what makes the retry
        // pass converge without separate bookkeeping.
        store.remove(&present_key);
        assert_eq!(
            pending_metadata_lookups(&plan, &store),
            vec![("p".to_string(), "present".to_string(), 4)]
        );

        let bumped = plan
            .iter()
            .map(|(project, run, _)| (project.clone(), run.clone(), Some(5)))
            .collect::<Vec<_>>();
        assert_eq!(
            pending_metadata_lookups(&bumped, &store),
            vec![
                ("p".to_string(), "present".to_string(), 5),
                ("p".to_string(), "absent".to_string(), 5),
            ]
        );
    }

    #[test]
    fn only_unaskable_and_terminal_lookups_settle_a_generation() {
        assert!(metadata_lookup_settles(&tonic::Status::not_found("purged")));
        assert!(metadata_lookup_settles(
            &tonic::Status::failed_precondition("expired")
        ));
        // A saved layout can carry a key this server will never accept.
        // Retrying it would spin until the project generation moved.
        assert!(metadata_lookup_settles(&tonic::Status::invalid_argument(
            "run_id too long"
        )));
        assert!(!metadata_lookup_settles(&tonic::Status::unavailable(
            "retry"
        )));
        assert!(!metadata_lookup_settles(&tonic::Status::internal("retry")));
    }
}
