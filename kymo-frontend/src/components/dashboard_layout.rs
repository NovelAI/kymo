use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use dioxus::prelude::*;
use futures::{stream, StreamExt};
use gloo_timers::future::sleep;

use crate::components::metric_rect::MetricRect;
use crate::components::navbar::Navbar;
use crate::components::options_panel::DashboardOptionsPanel;
use crate::components::sidebar::Sidebar;
use crate::components::uplot_chart::ZoomBridge;
use crate::grpc::proto::{MetricInfo, RunLifecycleState};
use crate::route::{focus_chart, Route};
use crate::state::app_state::{request_refresh, ExplicitRunKey, ExplicitRunMetadata};
use crate::state::layout_config::RunRef;
use crate::state::push::PushBridge;
use crate::state::visibility::{
    is_terminal_run_status, retry_visible, retry_visible_run, visible_attempt,
};
use crate::state::zones::{ZoneBridge, ZoneRegistry};
use crate::state::{
    load_diff_or_route, DashboardState, DirectRunLoad, DirectRunView, LayoutConfig, OpenPanel,
    PanelTarget,
};
use crate::util::{
    focus_is_within, focus_on_mount, js_bridge::js_string, primary, MAXIMIZE_OVERLAY_ID,
    OPTIONS_PANEL_ID, TOP_LAYER_SELECTOR,
};

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

/// Esc closes the maximized chart (sends 0); plain ←/→ move to the previous/next panel (-1/1). Installed on `window` after the in-app handlers and bulk mode's `document` listener, so it serves any focus (`<body>`, the notice bar) while leaving alone keys they consumed (`defaultPrevented`), IME composition, and keys an open dialog or popover will take (the image preview is one). Arrows also skip controls that take arrows and anything focused in or on a sideways scroller (a metadata value; the log viewer, which Chromium lets Tab focus); ←/→ also skip key repeat, since each switch mounts a panel that queries its data. Plain ↑/↓ press a maximized gallery's own Next/Previous step buttons (as `KB_ACTIVATE_JS` replays presses), so the panel's step rules apply and a held key scrubs; panels without a step bar, and focus outside the overlay, keep the keys' native behaviour. Other keys return before any document query.
const MAXIMIZE_KEYS_JS: &str = r#"(()=>{
// Wider than the scroller's outer box, its own scrollbar included: WebKit can leave a scroller's content at the width it had before a vertical scrollbar arrived.
function scrollsSideways(el){for(;el instanceof Element;el=el.parentElement)if(el.scrollWidth>el.offsetWidth&&/auto|scroll/.test(getComputedStyle(el).overflowX))return true;return false;}
function covered(){return document.querySelector(__TOP_LAYER_SELECTOR__);}
function send(value){try{dioxus.send(value);}catch(_){td();}}
function key(e){
  if(e.isComposing||e.defaultPrevented)return;
  if(e.key==='Escape'){if(!covered())send(0);return;}
  const dir={ArrowLeft:-1,ArrowRight:1}[e.key],step={ArrowUp:'Next step',ArrowDown:'Previous step'}[e.key];
  if(!dir&&!step||dir&&e.repeat||e.altKey||e.ctrlKey||e.metaKey||e.shiftKey||!document.querySelector('.maximize-overlay')||e.target.closest?.('input:not([type=checkbox],[type=button],[type=submit],[type=reset],[type=file],[type=image]),textarea,select,audio,video')||scrollsSideways(e.target)||covered())return;
  if(dir){e.preventDefault();send(dir);return;}
  // ↑/↓ scroll natively everywhere else (the sidebar's run list), unlike ←/→.
  if(e.target!==document.body&&!e.target.closest?.('.maximize-overlay'))return;
  const button=document.querySelector(`.maximize-content .cdn-step-nav button[aria-label="${step}"]`);
  if(!button)return;
  e.preventDefault();
  if(!button.disabled)button.dispatchEvent(new MouseEvent('mousedown',{bubbles:true,button:0}));
}
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

    // Discovery scope: the layout is built from the VISIBLE runs' metrics —
    // the run page's run, or the project page's selection — so a chart
    // exists only when a run the user can see logged its metric. The route
    // is the authority for the run page.
    let route = use_route::<Route>();
    crate::route::note_route(&route);
    let scope_run = match &route {
        Route::RunPage { run_id, .. } => Some(run_id.clone()),
        _ => None,
    };
    // Panels resolve `RunRef::Selected` through `current_run`, so it follows the route from the first render.
    let mut current_run = state.current_run;
    if *current_run.peek() != scope_run {
        current_run.set(scope_run);
    }

    // A run page's layout gates on its run's readability alone, so a GetRun refresh that leaves the run readable does not re-list its metrics.
    let scope_readable = use_memo({
        let project_id = project_id.clone();
        move || {
            let Some(run_id) = &*state.current_run.read() else {
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

    // A run page holds its first ListRuns back until its layout has loaded (or its run settled unreadable) and the visible charts' first fetches have settled, 3 s at most from when it starts waiting: every RPC shares one WebSocket that can't interleave replies, and a big project's list reply (1.2 MB on an 11,830-run project) would queue the chart replies behind it. A project page needs the list for its own layout, so it never waits.
    // The deadline is set once, so a restart mid-wait (a pushed refresh, a reconnect) waits out what's left instead of sending at once, and a layout that never loads can't hold the list forever.
    let mut list_hold_until = use_hook(|| CopyValue::new(None::<f64>));

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
                if !*runs_loaded.peek() && state.current_run.peek().is_some() {
                    let now = crate::state::trash::monotonic_now_ms;
                    let deadline = *list_hold_until.write().get_or_insert(now() + 3_000.0);
                    // Peeked, so leaving the run page ends the wait instead of restarting it.
                    let on_run_page = || state.current_run.peek().is_some();
                    // The charts are known once the layout has loaded, or once the URL run settled unreadable (its layout never loads).
                    let charts_known = || {
                        state.layout_config.peek().is_some()
                            || (!matches!(
                                &*state.direct_run.peek(),
                                DirectRunLoad::Idle | DirectRunLoad::Loading
                            ) && !*scope_readable.peek())
                    };
                    while !charts_known() && on_run_page() && now() < deadline {
                        sleep(Duration::from_millis(50)).await;
                    }
                    crate::state::visibility::wait_for_visible_idle(deadline, on_run_page).await;
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
                        .into_iter()
                        .filter(|run| !new_ids.contains(&run.run_id))
                        .collect::<Vec<_>>();
                    state.remember_display_runs(&removed_runs);
                    // Hidden/removed runs leave the project selection, while
                    // genuinely new runs retain the existing auto-select
                    // behavior. A direct run route still scopes itself from
                    // the URL, independently of this project-list selection.
                    let mut next_selected = state.selected_runs.peek().clone();
                    next_selected.retain(|run_id| new_ids.contains(run_id));
                    next_selected.extend(new_ids.difference(&old_ids).cloned());
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

    // A direct run page has a point lookup independent of the active run
    // list. That is what keeps a deleted run viewable without reintroducing
    // it into the sidebar, project selection, or All-run bindings.
    let _direct_run_fetch = use_resource({
        let project_id = project_id.clone();
        move || {
            let project_id = project_id.clone();
            let run_id = state.current_run.read().clone();
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
    let visible_run_ids = use_memo(move || match state.current_run.read().clone() {
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
    use_effect(use_reactive((&route.chart_param(),), move |(param,)| {
        let mut maximized = state.maximized;
        match param {
            None => {
                if maximized.peek().is_some() {
                    maximized.set(None);
                }
                // However the chart was un-maximized (its Close, Esc, Back, a reset), or a Configure's maximize never landed (pressed again while the close's history pop was pending), the chart panel closes, and any other panel forgets the maximize it was to undo.
                let mut panel = state.options_panel;
                let open = panel.peek().clone();
                match open {
                    Some(p) if p.target == PanelTarget::Chart => panel.set(None),
                    Some(p) if p.unmaximize_on_close => panel.set(Some(OpenPanel {
                        unmaximize_on_close: false,
                        ..p
                    })),
                    _ => {}
                }
            }
            // The loaded layout is the authority, but a chart link must not wait for its metrics sweep (tens of seconds on big projects), so until it holds the rect this falls back to `DashboardState::fresh_rect`'s local resolve.
            // Once open, the overlay is left alone: it re-resolves against the live layout each render, upgrading a fallback config when the sweep lands.
            Some(id) => {
                if maximized.peek().as_ref().is_some_and(|m| m.id == id) {
                    return;
                }
                // Peeks (see `fresh_rect`): only the param drives this. A layout change while a dismissal's history pop is pending would otherwise maximize the chart again for a render.
                let resolved = state.fresh_rect(&id);
                if let Some(config) = resolved {
                    // The maximize moved to another chart (←/→), and a chart panel follows it there, taking focus only if the one still showing holds it.
                    let mut panel = state.options_panel;
                    let follower = panel
                        .peek()
                        .clone()
                        .filter(|p| p.target == PanelTarget::Chart && maximized.peek().is_some());
                    if let Some(p) = follower {
                        let take_focus = focus_is_within(OPTIONS_PANEL_ID);
                        if p.take_focus != take_focus {
                            panel.set(Some(OpenPanel { take_focus, ..p }));
                        }
                    }
                    maximized.set(Some(config));
                }
            }
        }
    }));

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
            // The short-circuit keeps a run page unsubscribed, so the run list landing never re-runs this fetch there.
            let scope_known = state.current_run.read().is_some() || *runs_loaded.read();
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
                // A run page's scope comes from its URL. A project page waits for the first list_runs: an empty selection means "nothing visible" only once auto-select has filled it, and before that, rendering would flash an empty dashboard.
                if !scope_known {
                    return;
                }
                let grpc = grpc.read().clone();
                // One aggregated registry query for the visible run set;
                // reloads are event-driven (visible set, registry pushes,
                // resyncs restart the resource).
                let all_metrics: Vec<MetricInfo> = if state.current_run.peek().is_some() {
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
                            request_refresh(state.direct_run_refresh);
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
                    // Inert under the overlay: nothing covered takes focus or Tab. The attribute also freezes the grid's scroll (CSS), caps covered slots at Near (state/zones.rs) and hides covered charts' synced tooltips (uplot_chart/hover.js).
                    main { class: "main-content", inert: state.maximized.read().is_some().then_some(true),
                        Outlet::<Route> {}
                    }
                    MaximizeOverlay {}
                }
                // Docked after the main column, which narrows while it's open.
                DashboardOptionsPanel {}
            }
            PushBridge {}
            ZoneBridge {}
            ZoomBridge {}
        }
    }
}

/// Overlay beside `<main>` that fills its column while the sidebar stays visible, showing [`DashboardState::maximized_rect`].
#[component]
fn MaximizeOverlay() -> Element {
    let state = use_context::<DashboardState>();
    let maximized_signal = state.maximized;

    let keys_bridge = crate::util::js_bridge::use_bridge("maximize_keys");
    use_future(move || {
        let js = keys_bridge
            .script(MAXIMIZE_KEYS_JS)
            .replace("__TOP_LAYER_SELECTOR__", &js_string(TOP_LAYER_SELECTOR));
        async move {
            let mut eval = document::eval(&js);
            while let Ok(dir) = eval.recv::<i32>().await {
                let Some(id) = maximized_signal.peek().as_ref().map(|m| m.id.clone()) else {
                    continue;
                };
                if dir == 0 {
                    focus_chart(None);
                    continue;
                }
                let needle = state.panel_needle();
                let next = state.layout_config.peek().as_ref().and_then(|layout| {
                    layout
                        .adjacent_rect(&id, &needle, dir > 0)
                        .map(str::to_string)
                });
                if let Some(next) = next {
                    focus_chart(Some(next));
                }
            }
        }
    });

    let Some((rect_config, _)) = state.maximized_rect() else {
        return rsx! {};
    };
    let rect_id = rect_config.id.clone();

    // The content area already excludes the navbar and any notice bar.
    let chart_height = *MAXIMIZED_CHART_HEIGHT.read();

    rsx! {
        div {
            id: MAXIMIZE_OVERLAY_ID,
            class: "maximize-overlay",
            // Focus on mount so keyboard navigation starts inside the overlay, not on the grid control (e.g. the Maximize button) left focused beneath it; a chart panel opening with the maximize (Configure on a grid chart) keeps focus instead.
            // Pause every player: the covered grid's panels stay mounted beneath the overlay (Near), and the overlay's own copies have only just mounted.
            tabindex: "-1",
            onmounted: move |e| {
                document::eval("for(const m of document.querySelectorAll('video,audio'))m.pause();");
                if !state
                    .options_panel
                    .peek()
                    .as_ref()
                    .is_some_and(|p| p.target == PanelTarget::Chart)
                {
                    focus_on_mount(e)
                }
            },
            // Dismiss on click (mouseup) can be annoying if you drag the x-axis and release in the border.
            onmousedown: primary(move |_| {
                crate::util::panel_moved();
                focus_chart(None)
            }),
            div {
                class: "maximize-content",
                onmousedown: move |e| e.stop_propagation(),
                for id in [rect_id] {
                    MetricRect {
                        // A one-item keyed list: Dioxus 0.7.9 ignores `key:` on a component nested in elements, and a reused instance would carry the previous chart's hooks (cache key, fetched data, gallery step) into the next one (←/→, chart links).
                        key: "{id}",
                        config: rect_config.clone(),
                        chart_height: chart_height,
                        is_maximized: true,
                    }
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
    fn maximize_keys_use_the_shared_lifecycle() {
        crate::util::js_bridge::validate_template(super::MAXIMIZE_KEYS_JS);
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
