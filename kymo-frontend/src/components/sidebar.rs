mod view;

pub use view::Sidebar;

use std::collections::HashSet;
use std::rc::Rc;

use dioxus::core::Task;
use dioxus::html::input_data::MouseButton;
use dioxus::prelude::*;
use dioxus::web::WebEventExt;

use crate::components::color_picker::ColorPicker;
use crate::components::icons::{CloseIcon, MoreIcon, TrashIcon};
use crate::components::uplot_chart::run_color;
use crate::grpc::proto::{RunInfo, RunStatus, TrashRunOutcome, TrashRunResult};
use crate::grpc::GrpcClient;
use crate::route::Route;
use crate::state::trash::lookup_trashed_runs;
use crate::state::DashboardState;
use crate::util::{is_app_escape, local_storage, primary};

/// A transport cut, not a product limit. Arbitrarily large selections are
/// sent sequentially so one WebSocket frame and one response stay modest.
const TRASH_RPC_CHUNK: usize = 256;
const MAX_RUN_NAME_BYTES: usize = 2_048;

struct SelectionPaint {
    value: bool,
    last_index: usize,
    visible_run_ids: Rc<[String]>,
}

#[derive(Debug, PartialEq, Eq)]
struct TrashMutationSummary {
    succeeded: HashSet<String>,
    failed: HashSet<String>,
    first_failure: Option<String>,
}

#[derive(Clone, Copy)]
struct TrashMutationUi {
    dashboard: DashboardState,
    picker: Signal<Option<HashSet<String>>>,
    paint: Signal<Option<SelectionPaint>>,
    busy: Signal<bool>,
    feedback: Signal<String>,
}

#[derive(Clone, PartialEq)]
struct RenameRunTarget {
    project_id: String,
    run_id: String,
    current_name: String,
    display_label: String,
    ordinal: u64,
}

#[derive(Clone, PartialEq)]
struct ColorPickerTarget {
    run_id: String,
    run_label: String,
    ordinal: u64,
}

/// Whether any name repeats; callers then give every entry its "#ordinal", not just the repeats.
pub(crate) fn names_repeat<'a>(names: impl IntoIterator<Item = &'a str>) -> bool {
    let mut seen = HashSet::new();
    names.into_iter().any(|name| !seen.insert(name))
}

pub(crate) fn sidebar_needs_run_ordinals(runs: &[RunInfo]) -> bool {
    names_repeat(runs.iter().map(|run| run.run_name.as_str()))
}

pub(crate) fn sidebar_run_label(run: &RunInfo, show_ordinal: bool) -> String {
    if show_ordinal {
        format!("{} #{}", run.run_name, run.ordinal)
    } else {
        run.run_name.clone()
    }
}

fn restored_run_selection(saved: &str, runs: &[RunInfo]) -> Option<HashSet<String>> {
    if saved.is_empty() {
        return Some(HashSet::new());
    }
    filtered_run_selection(
        saved
            .split(',')
            .filter(|run_id| !run_id.is_empty())
            .map(str::to_string)
            .collect(),
        runs,
    )
}

fn restored_json_run_selection(saved: &str, runs: &[RunInfo]) -> Option<HashSet<String>> {
    let run_ids = serde_json::from_str::<Vec<String>>(saved).ok()?;
    if run_ids.is_empty() {
        return Some(HashSet::new());
    }
    filtered_run_selection(run_ids, runs)
}

fn filtered_run_selection(saved_run_ids: Vec<String>, runs: &[RunInfo]) -> Option<HashSet<String>> {
    let valid = saved_run_ids
        .into_iter()
        .filter(|run_id| runs.iter().any(|run| run.run_id == *run_id))
        .collect::<HashSet<_>>();
    (!valid.is_empty()).then_some(valid)
}

fn persisted_run_selection(selected: &HashSet<String>) -> String {
    let mut run_ids = selected.iter().map(String::as_str).collect::<Vec<_>>();
    run_ids.sort_unstable();
    serde_json::to_string(&run_ids).expect("run IDs always serialize as JSON strings")
}

fn selected_runs_key(project_id: &str) -> String {
    format!("kymo_selected_runs_v2_{project_id}")
}

fn legacy_selected_runs_v2_key(project_id: &str) -> String {
    format!("mkdb2_selected_runs_v2_{project_id}")
}

fn legacy_selected_runs_v1_key(project_id: &str) -> String {
    format!("mkdb2_selected_runs_{project_id}")
}

fn is_json_run_selection(saved: &str) -> bool {
    serde_json::from_str::<Vec<String>>(saved).is_ok()
}

fn remove_owned_legacy_run_selections(project_id: &str) {
    let v2 = legacy_selected_runs_v2_key(project_id);
    if local_storage::get(&v2)
        .as_deref()
        .is_some_and(is_json_run_selection)
    {
        local_storage::remove(&v2);
    }
    let v1 = legacy_selected_runs_v1_key(project_id);
    if local_storage::get(&v1)
        .as_deref()
        .is_some_and(|value| !is_json_run_selection(value))
    {
        local_storage::remove(&v1);
    }
}

/// Warns, never blocks (docs/run-deletion-ui.md).
fn live_trash_warning(runs: &[RunInfo], pending: &HashSet<String>) -> String {
    let live = runs
        .iter()
        .filter(|run| {
            pending.contains(&run.run_id)
                && matches!(run.status(), RunStatus::Running | RunStatus::Stuck)
        })
        .count();
    match live {
        0 => String::new(),
        1 => "1 selected run is still logging; its new points will be dropped.".to_string(),
        n => format!("{n} selected runs are still logging; their new points will be dropped."),
    }
}

fn result_failure(result: &TrashRunResult) -> String {
    if !result.error.is_empty() {
        return result.error.clone();
    }
    match result.outcome() {
        TrashRunOutcome::NotFound => "run not found".to_string(),
        TrashRunOutcome::Expired => "recovery already expired".to_string(),
        TrashRunOutcome::Error => "server could not move the run".to_string(),
        TrashRunOutcome::Unknown => "unknown server result".to_string(),
        _ => "run was not moved".to_string(),
    }
}

fn summarize_trash_results(
    requested: &[String],
    results: &[TrashRunResult],
) -> TrashMutationSummary {
    let by_id: std::collections::HashMap<&str, &TrashRunResult> = results
        .iter()
        .map(|result| (result.run_id.as_str(), result))
        .collect();
    let mut succeeded = HashSet::new();
    let mut failed = HashSet::new();
    let mut first_failure = None;
    for run_id in requested {
        let Some(result) = by_id.get(run_id.as_str()) else {
            failed.insert(run_id.clone());
            first_failure.get_or_insert_with(|| "server omitted a run result".to_string());
            continue;
        };
        if matches!(
            result.outcome(),
            TrashRunOutcome::Trashed | TrashRunOutcome::AlreadyTrashed
        ) {
            succeeded.insert(run_id.clone());
        } else {
            failed.insert(run_id.clone());
            first_failure.get_or_insert_with(|| result_failure(result));
        }
    }
    TrashMutationSummary {
        succeeded,
        failed,
        first_failure,
    }
}

/// A project whose runs have all gone to Trash keeps no saved selection, so runs restored or logged to it later show like a first visit instead of restoring an empty "show nothing".
fn forget_run_selection(project_id: &str) {
    if local_storage::remove(&selected_runs_key(project_id)) {
        remove_owned_legacy_run_selections(project_id);
    }
}

fn remove_active_runs(state: DashboardState, run_ids: &HashSet<String>) {
    if !run_ids.is_empty() {
        let mut next_runs = state.runs.peek().clone();
        let removed_runs = next_runs
            .iter()
            .filter(|run| run_ids.contains(&run.run_id))
            .cloned()
            .collect::<Vec<_>>();
        state.remember_display_runs(&removed_runs);
        next_runs.retain(|run| !run_ids.contains(&run.run_id));
        let mut runs = state.runs;
        runs.set(next_runs);

        let mut next_selected = state.selected_runs.peek().clone();
        next_selected.retain(|run_id| !run_ids.contains(run_id));
        let mut selected = state.selected_runs;
        selected.set(next_selected);
    }

    // Also reconcile outcome-unknown transport failures: the server may have
    // committed a chunk whose response was lost.
    state.request_runs_refresh();
}

fn submit_trash_runs(project_id: String, requested: Vec<String>, ui: TrashMutationUi) {
    let mut busy = ui.busy;
    let mut feedback = ui.feedback;
    if requested.is_empty() || *busy.peek() {
        return;
    }
    busy.set(true);
    feedback.set(String::new());
    spawn(async move {
        let grpc = GrpcClient::new();
        let mut results = std::collections::HashMap::<String, TrashRunResult>::new();
        let mut ambiguous = Vec::new();
        for (chunk_index, chunk) in requested.chunks(TRASH_RPC_CHUNK).enumerate() {
            match grpc.trash_runs(&project_id, chunk).await {
                Ok(response) => {
                    let returned = response
                        .results
                        .into_iter()
                        .map(|result| (result.run_id.clone(), result))
                        .collect::<std::collections::HashMap<_, _>>();
                    for run_id in chunk {
                        let result =
                            returned
                                .get(run_id)
                                .cloned()
                                .unwrap_or_else(|| TrashRunResult {
                                    run_id: run_id.clone(),
                                    outcome: TrashRunOutcome::Error as i32,
                                    error: "server omitted this run result".to_string(),
                                });
                        if matches!(
                            result.outcome(),
                            TrashRunOutcome::Error | TrashRunOutcome::Unknown
                        ) {
                            ambiguous.push(run_id.clone());
                        }
                        results.insert(run_id.clone(), result);
                    }
                }
                Err(status) => {
                    let first = chunk_index * TRASH_RPC_CHUNK;
                    let failed_end = (first + chunk.len()).min(requested.len());
                    for run_id in &requested[first..failed_end] {
                        ambiguous.push(run_id.clone());
                        results.insert(
                            run_id.clone(),
                            TrashRunResult {
                                run_id: run_id.clone(),
                                outcome: TrashRunOutcome::Error as i32,
                                error: format!(
                                    "the request did not return a result: {}",
                                    status.message()
                                ),
                            },
                        );
                    }
                    for run_id in &requested[failed_end..] {
                        results.insert(
                            run_id.clone(),
                            TrashRunResult {
                                run_id: run_id.clone(),
                                outcome: TrashRunOutcome::Error as i32,
                                error: "not sent after the connection failed".to_string(),
                            },
                        );
                    }
                    break;
                }
            }
        }

        // Typed replies are final. Only outcome-unknown identities pay a
        // chunked verification pass; a failed lookup leaves them selected.
        if !ambiguous.is_empty() {
            feedback.set("Verifying uncertain results…".to_string());
            if let Ok(records) = lookup_trashed_runs(&project_id, &ambiguous).await {
                let confirmed = records
                    .iter()
                    .filter_map(|record| record.run.as_ref())
                    .filter(|run| run.project_id == project_id)
                    .map(|run| run.run_id.as_str())
                    .collect::<HashSet<_>>();
                for run_id in &ambiguous {
                    if confirmed.contains(run_id.as_str()) {
                        results.insert(
                            run_id.clone(),
                            TrashRunResult {
                                run_id: run_id.clone(),
                                outcome: TrashRunOutcome::AlreadyTrashed as i32,
                                ..Default::default()
                            },
                        );
                    }
                }
            }
        }
        let ordered = requested
            .iter()
            .filter_map(|run_id| results.remove(run_id))
            .collect();
        apply_trash_results(ordered, &requested, ui);
        busy.set(false);
    });
}

fn apply_trash_results(results: Vec<TrashRunResult>, requested: &[String], ui: TrashMutationUi) {
    let mut picker = ui.picker;
    let mut feedback = ui.feedback;
    let summary = summarize_trash_results(requested, &results);
    let was_bulk = picker.peek().is_some();
    remove_active_runs(ui.dashboard, &summary.succeeded);

    if picker.peek().is_some() {
        if summary.failed.is_empty() {
            close_trash_picker(picker, ui.paint);
        } else {
            end_paint_if_active(ui.paint);
            picker.set(Some(summary.failed.clone()));
        }
    }
    if !was_bulk && !summary.succeeded.is_empty() {
        focus_sidebar_trash_trigger();
    }

    let moved = summary.succeeded.len();
    let failed = summary.failed.len();
    let failure_detail = summary.first_failure.as_deref().unwrap_or("unknown error");
    let message = match (moved, failed) {
        (0, 0) => "No runs were selected.".to_string(),
        (moved, 0) => format!(
            "Moved {moved} {} to Trash.",
            if moved == 1 { "run" } else { "runs" }
        ),
        (0, failed) => format!(
            "Couldn’t move {failed} {}: {}",
            if failed == 1 { "run" } else { "runs" },
            failure_detail
        ),
        (moved, failed) => format!(
            "Moved {moved}; {failed} failed or unresolved and remain selected: {failure_detail}",
        ),
    };
    feedback.set(message.clone());
    // A normal-row Trash and a fully successful bulk operation leave bulk
    // mode, so their toast should not occupy the sidebar indefinitely. Partial
    // failures stay inline beside the selected retry set until retry/cancel.
    if picker.peek().is_none() {
        spawn(async move {
            gloo_timers::future::sleep(std::time::Duration::from_secs(4)).await;
            if feedback.peek().as_str() == message.as_str() {
                feedback.set(String::new());
            }
        });
    }
}

fn set_membership(selected: &mut HashSet<String>, run_id: &str, value: bool) {
    if value {
        selected.insert(run_id.to_string());
    } else {
        selected.remove(run_id);
    }
}

/// Flips one run's membership and reports the value it landed on.
fn toggle_membership(selected: &mut HashSet<String>, run_id: &str) -> bool {
    let value = !selected.contains(run_id);
    set_membership(selected, run_id, value);
    value
}

fn all_or_none_selection(selected: &mut HashSet<String>, listed: &[String]) {
    if selected.is_empty() {
        selected.extend(listed.iter().cloned());
    } else {
        selected.clear();
    }
}

fn selection_action_label(
    selected_count: usize,
    filter_active: bool,
    in_trash_mode: bool,
) -> &'static str {
    match (in_trash_mode, selected_count > 0, filter_active) {
        (false, true, _) => "Hide all",
        (false, false, true) => "Show all listed",
        (false, false, false) => "Show all",
        (true, true, _) => "Select none",
        (true, false, true) => "Select all listed",
        (true, false, false) => "Select all",
    }
}

fn run_list_empty_message(total: usize, visible: usize) -> Option<&'static str> {
    if total == 0 {
        Some("No runs")
    } else if visible == 0 {
        Some("No matching runs")
    } else {
        None
    }
}

impl SelectionPaint {
    fn begin(
        selected: &mut HashSet<String>,
        run_id: &str,
        visible_run_ids: Rc<[String]>,
        visible_index: usize,
    ) -> Option<Self> {
        if visible_run_ids.get(visible_index).map(String::as_str) != Some(run_id) {
            return None;
        }
        Some(Self {
            value: toggle_membership(selected, run_id),
            last_index: visible_index,
            visible_run_ids,
        })
    }
}

fn continue_selection_paint(
    paint: &mut Option<SelectionPaint>,
    selected: &mut HashSet<String>,
    visible_run_ids: &[String],
    visible_index: usize,
) {
    let Some(active) = paint.as_mut() else {
        return;
    };
    // A gesture records the visible ordering it was armed against and dies when
    // that changes, so a newly arrived run cannot inherit an old index.
    if active.visible_run_ids.as_ref() != visible_run_ids || visible_index >= visible_run_ids.len()
    {
        *paint = None;
        return;
    }
    let first = active.last_index.min(visible_index);
    let last = active.last_index.max(visible_index);
    for run_id in &visible_run_ids[first..=last] {
        set_membership(selected, run_id, active.value);
    }
    active.last_index = visible_index;
}

/// Ends drag-paint on any primary release, pointer cancellation, or window blur, and handles bulk-mode Escape below native top-layer surfaces. `js_bridge` owns predecessor eviction and late-drop fencing.
const SIDEBAR_PICK_BRIDGE_JS: &str = r#"(()=>{
function send(kind){try{dioxus.send(kind);}catch(_){td();}}
// Only a press that landed on a run row can arm a gesture, in either mode, so tracking that here keeps every other release in the app from crossing into WASM to run a no-op. Over-approximating is fine: Rust may decline to arm, and a spurious 'end' is idempotent.
let armed=false;
function arm(e){if(e.button===0&&e.target.closest&&e.target.closest('.sidebar-run'))armed=true;}
function end(e){if(e.type==='mouseup'&&e.button!==0)return;if(!armed)return;armed=false;send('end');}
function key(e){
  // Bulk mode is a page-level Esc layer: it takes Esc that no in-app layer consumed, no IME composition is using, and no native dialog or popover will close. On `document` it runs before the maximized chart's `window` listener.
  if(e.key==='Escape'&&!e.isComposing&&!e.defaultPrevented&&!document.querySelector(__TOP_LAYER_SELECTOR__)&&document.querySelector('.sidebar-trash-mode:not(.sidebar-trash-busy)')){
    e.preventDefault();send('cancel');
  }
}
function cleanup(){
  window.removeEventListener('mousedown',arm,true);
  window.removeEventListener('mouseup',end,true);
  window.removeEventListener('pointercancel',end,true);
  window.removeEventListener('blur',end);
  document.removeEventListener('keydown',key);
}
const td=window.__kymo_bridges.mount(__BRIDGE_NAME__,__BRIDGE_OWNER__,cleanup);
window.addEventListener('mousedown',arm,true);
window.addEventListener('mouseup',end,true);
window.addEventListener('pointercancel',end,true);
window.addEventListener('blur',end);
document.addEventListener('keydown',key);
})()"#;

/// Peek before writing: an unconditional `set` would dirty the signal and
/// re-render the sidebar on every hover and every pointer release.
fn end_paint_if_active(mut paint: Signal<Option<SelectionPaint>>) {
    if paint.peek().is_some() {
        paint.set(None);
    }
}

fn close_trash_picker(
    mut picker: Signal<Option<HashSet<String>>>,
    paint: Signal<Option<SelectionPaint>>,
) {
    end_paint_if_active(paint);
    picker.set(None);
    focus_sidebar_trash_trigger();
}

fn focus_when_mounted(
    primary_id: &str,
    fallback_selector: Option<&str>,
    preserve_existing_focus: bool,
) {
    let primary_id = serde_json::to_string(primary_id).unwrap_or_else(|_| "\"\"".to_string());
    let fallback_selector = fallback_selector
        .map(|selector| serde_json::to_string(selector).unwrap_or_else(|_| "\"\"".to_string()))
        .unwrap_or_else(|| "null".to_string());
    spawn(async move {
        let _ = document::eval(&format!(
            "(()=>{{\
                let attempts=0;\
                const focus=()=>{{\
                    if({preserve_existing_focus}&&document.activeElement&&document.activeElement!==document.body)return;\
                    const target=document.getElementById({primary_id})\
                        ||({fallback_selector}&&document.querySelector({fallback_selector}));\
                    if(target){{target.focus();return;}}\
                    if(attempts++<10)requestAnimationFrame(focus);\
                }};\
                requestAnimationFrame(focus);\
            }})()"
        ))
        .await;
    });
}

fn focus_sidebar_trash_trigger() {
    focus_when_mounted("sidebar-trash-trigger", None, false);
}

async fn hide_run_popover(menu_id: &str) {
    let menu_id = serde_json::to_string(menu_id).unwrap_or_else(|_| "\"\"".to_string());
    let _ = document::eval(&format!(
        "try{{document.getElementById({menu_id})?.hidePopover()}}catch(_){{}}"
    ))
    .await;
}

fn focus_run_overflow_trigger(ordinal: u64, preserve_existing_focus: bool) {
    focus_when_mounted(
        &format!("run-overflow-trigger-{ordinal}"),
        Some(".sidebar-filter:not(:disabled)"),
        preserve_existing_focus,
    );
}

fn flash_feedback(mut feedback: Signal<String>, mut timer: Signal<Option<Task>>, message: String) {
    if let Some(previous) = timer.take() {
        previous.cancel();
    }
    feedback.set(message.clone());
    timer.set(Some(spawn(async move {
        gloo_timers::future::sleep(std::time::Duration::from_millis(1_800)).await;
        if feedback.peek().as_str() == message {
            feedback.set(String::new());
        }
    })));
}

fn apply_rename_if_current(
    runs: &mut [RunInfo],
    project_id: &str,
    run_id: &str,
    expected_name: &str,
    renamed_name: &str,
) -> Option<RunInfo> {
    let run = runs
        .iter_mut()
        .find(|run| run.project_id == project_id && run.run_id == run_id)?;
    // A second rename or a fresher ListRuns response may have landed while
    // this request was in flight. Only apply the optimistic label when the
    // row is still the snapshot this editor changed, and preserve all newer
    // status/timestamp fields from the list response.
    if run.run_name != expected_name {
        return None;
    }
    run.run_name = renamed_name.to_string();
    Some(run.clone())
}

#[component]
fn InlineRunRename(
    target: RenameRunTarget,
    on_close: EventHandler<bool>,
    on_renamed: EventHandler<String>,
    on_error: EventHandler<String>,
) -> Element {
    let state = use_context::<DashboardState>();
    let mut draft = use_signal(|| target.current_name.clone());
    let mut busy = use_signal(|| false);
    let mut error = use_signal(String::new);
    let normalized = draft.read().trim().to_string();
    let name_too_long = normalized.len() > MAX_RUN_NAME_BYTES;
    let validation_error = if !error.read().is_empty() {
        error.read().clone()
    } else if name_too_long {
        format!("Name must be at most {MAX_RUN_NAME_BYTES} UTF-8 bytes.")
    } else {
        String::new()
    };
    let input_id = format!("run-rename-input-{}", target.ordinal);
    let error_id = format!("{input_id}-error");

    let submit = {
        let target = target.clone();
        move |restore_focus: bool| {
            if *busy.peek() {
                return;
            }
            let run_name = draft.peek().trim().to_string();
            if run_name.is_empty() || run_name == target.current_name {
                on_close.call(restore_focus);
                return;
            }
            let validation = if run_name.len() > MAX_RUN_NAME_BYTES {
                Some(format!(
                    "Name must be at most {MAX_RUN_NAME_BYTES} UTF-8 bytes."
                ))
            } else {
                None
            };
            if let Some(message) = validation {
                // Blur is ordinary light dismissal for an invalid draft; Enter
                // keeps the editor open and exposes the validation result.
                if restore_focus {
                    error.set(message.clone());
                    on_error.call(message);
                } else {
                    on_close.call(false);
                }
                return;
            }

            busy.set(true);
            error.set(String::new());
            on_error.call(String::new());
            let target = target.clone();
            spawn(async move {
                let result = GrpcClient::new()
                    .rename_run(&target.project_id, &target.run_id, &run_name)
                    .await;
                match result {
                    Ok(response) => {
                        let Some(updated) = response.run.filter(|run| {
                            run.project_id == target.project_id && run.run_id == target.run_id
                        }) else {
                            let message = "Server returned an invalid run.".to_string();
                            busy.set(false);
                            error.set(message.clone());
                            on_error.call(message);
                            return;
                        };
                        let renamed_name = updated.run_name.clone();
                        let mut next_runs = state.runs.peek().clone();
                        if let Some(remembered) = apply_rename_if_current(
                            &mut next_runs,
                            &target.project_id,
                            &target.run_id,
                            &target.current_name,
                            &renamed_name,
                        ) {
                            let mut runs = state.runs;
                            runs.set(next_runs);
                            state.remember_display_runs(std::slice::from_ref(&remembered));
                        }
                        // The response is authoritative for this mutation but
                        // can arrive after a newer rename/status ListRuns
                        // refresh. Re-read the canonical list so an older
                        // response cannot become the last local writer.
                        state.request_runs_refresh();
                        busy.set(false);
                        on_renamed.call(renamed_name);
                        on_close.call(restore_focus);
                    }
                    Err(status) => {
                        // A transport cut can hide a committed rename. Reconcile
                        // while leaving the inline draft available for retry.
                        state.request_runs_refresh();
                        busy.set(false);
                        let message = match status.message() {
                            "" => "Couldn’t rename the run.".to_string(),
                            detail => format!("Couldn’t rename the run: {detail}"),
                        };
                        error.set(message.clone());
                        on_error.call(message);
                    }
                }
            });
        }
    };

    rsx! {
        span {
            class: "run-name-host run-name-inline-host",
            aria_busy: *busy.read(),
            input {
                id: "{input_id}",
                class: if validation_error.is_empty() {
                    "run-name-inline-input"
                } else {
                    "run-name-inline-input run-name-inline-input-error"
                },
                r#type: "text",
                value: "{draft}",
                readonly: *busy.read(),
                aria_label: "Rename {target.display_label}",
                aria_invalid: (!validation_error.is_empty()).then_some("true"),
                aria_describedby: (!validation_error.is_empty()).then_some(error_id.as_str()),
                title: (!validation_error.is_empty()).then_some(validation_error.as_str()),
                onmounted: {
                    let input_id = input_id.clone();
                    move |_| {
                        let input_id = serde_json::to_string(&input_id)
                            .unwrap_or_else(|_| "\"\"".to_string());
                        spawn(async move {
                            let _ = document::eval(&format!(
                                "requestAnimationFrame(()=>{{const input=document.getElementById({input_id});if(input){{input.focus();input.select()}}}})"
                            ))
                            .await;
                        });
                    }
                },
                oninput: move |e: Event<FormData>| {
                    draft.set(e.value());
                    if !error.peek().is_empty() {
                        error.set(String::new());
                        on_error.call(String::new());
                    }
                },
                onkeydown: {
                    let mut submit = submit.clone();
                    move |e: Event<KeyboardData>| {
                        if e.key() == Key::Enter && !e.is_composing() {
                            e.prevent_default();
                            submit(true);
                        } else if is_app_escape(&e) {
                            e.prevent_default();
                            if !*busy.peek() {
                                on_close.call(true);
                            }
                        }
                    }
                },
                onblur: {
                    let mut submit = submit;
                    move |_| submit(false)
                },
            }
            if !validation_error.is_empty() {
                span {
                    id: "{error_id}",
                    class: "visually-hidden",
                    role: "alert",
                    "{validation_error}"
                }
            }
        }
    }
}

/// Width drag for the sidebar. Lives entirely outside Dioxus: the width is a
/// CSS var on `<html>`, which the VDOM never diffs, so a re-render can't
/// clobber a mid-drag width and no per-mousemove Rust runs. Window listeners
/// exist only for the drag's duration (same shape as the chart axis-pull).
/// The width is 10-75% of the window width, so it follows window resizes.
/// The minimum width keeps the sidebar always grabbable — collapse-to-zero
/// (and the reopen affordance it required) is gone on purpose.
const SIDEBAR_RESIZE_JS: &str = r#"(function(){
  let clamp=function(p){return Math.max(10,Math.min(p,75))};
  let pct=function(px){return clamp(px/window.innerWidth*100)};
  let apply=function(p){document.documentElement.style.setProperty('--kymo-sidebar-w',p+'vw')};
  let saved;
  try{
    let raw=localStorage.getItem('kymo_sidebar_w');
    if(raw===null)raw=localStorage.getItem('mkdb2_sidebar_w');
    saved=parseFloat(raw);
  }catch(_){}
  // Bundles before FRO-747 stored pixels, always at least 160; percentages are at most 75.
  if(isFinite(saved))apply(saved>75?pct(saved):clamp(saved));
  let h=document.querySelector('.sidebar-resize');
  if(!h||h.__wired)return;h.__wired=true;
  let sb=h.parentElement;
  h.addEventListener('mousedown',function(ev){
    if(ev.button!==0)return;
    ev.preventDefault();
    let live=true,p=pct(sb.getBoundingClientRect().width);
    let resize=function(e){
      p=pct(e.clientX-sb.getBoundingClientRect().left);
      apply(p);
    };
    let move=function(e){
      // A release outside the browser sends no mouseup; the next move's button state still tells us to finish.
      if(e.buttons===0){finish();return}
      resize(e);
    };
    let finish=function(e){
      if(!live)return;live=false;
      if(e&&Number.isFinite(e.clientX))resize(e);
      window.removeEventListener('mousemove',move);
      window.removeEventListener('mouseup',finish);
      window.removeEventListener('blur',finish);
      document.body.style.cursor='';
      try{
        localStorage.setItem('kymo_sidebar_w',String(p));
        localStorage.removeItem('mkdb2_sidebar_w');
      }catch(_){}
    };
    document.body.style.cursor='col-resize';
    window.addEventListener('mousemove',move);
    window.addEventListener('mouseup',finish);
    window.addEventListener('blur',finish);
  });
})()"#;

#[cfg(test)]
mod selection_pick_tests {
    use std::{collections::HashSet, rc::Rc};

    use crate::grpc::proto::{RunInfo, RunStatus, TrashRunOutcome, TrashRunResult};

    use super::{
        all_or_none_selection, apply_rename_if_current, continue_selection_paint,
        is_json_run_selection, legacy_selected_runs_v1_key, legacy_selected_runs_v2_key,
        live_trash_warning, persisted_run_selection, restored_json_run_selection,
        restored_run_selection, run_list_empty_message, selected_runs_key, selection_action_label,
        sidebar_needs_run_ordinals, sidebar_run_label, summarize_trash_results, SelectionPaint,
    };

    #[test]
    fn live_trash_warning_counts_selected_running_and_stuck_runs() {
        let run = |run_id: &str, status: RunStatus| RunInfo {
            run_id: run_id.to_string(),
            status: status as i32,
            ..Default::default()
        };
        let runs = [
            run("running", RunStatus::Running),
            run("stuck", RunStatus::Stuck),
            run("unresponsive", RunStatus::Unresponsive),
            run("finished", RunStatus::Finished),
            run("unselected", RunStatus::Running),
        ];
        let pick = |ids: &[&str]| ids.iter().map(|id| id.to_string()).collect::<HashSet<_>>();
        assert_eq!(
            live_trash_warning(&runs, &pick(&["unresponsive", "finished"])),
            ""
        );
        assert_eq!(
            live_trash_warning(&runs, &pick(&["stuck", "finished"])),
            "1 selected run is still logging; its new points will be dropped."
        );
        assert_eq!(
            live_trash_warning(&runs, &pick(&["running", "stuck", "unresponsive"])),
            "2 selected runs are still logging; their new points will be dropped."
        );
    }

    #[test]
    fn branded_selection_keys_do_not_own_colliding_projects() {
        assert_eq!(
            legacy_selected_runs_v2_key("x"),
            legacy_selected_runs_v1_key("v2_x")
        );
        assert_eq!(selected_runs_key("x"), "kymo_selected_runs_v2_x");
        assert_eq!(
            selected_runs_key("x"),
            "kymo_selected_runs_".to_owned() + "v2_x"
        );
        assert!(is_json_run_selection(r#"["run"]"#));
        assert!(!is_json_run_selection("run,other"));
    }

    #[test]
    fn sidebar_pick_uses_the_shared_lifecycle() {
        crate::util::js_bridge::validate_template(super::SIDEBAR_PICK_BRIDGE_JS);
    }

    #[test]
    fn drag_paints_one_value_across_skipped_rows_and_backtracking() {
        let mut selected = HashSet::new();
        let visible: Rc<[String]> = vec!["a".to_string(), "b".to_string(), "c".to_string()].into();

        let mut paint = SelectionPaint::begin(&mut selected, "a", visible.clone(), 0);
        continue_selection_paint(&mut paint, &mut selected, visible.as_ref(), 2);
        continue_selection_paint(&mut paint, &mut selected, visible.as_ref(), 0);
        assert_eq!(selected.len(), 3);

        // Starting on a selected run paints the opposite value across the range.
        let mut paint = SelectionPaint::begin(&mut selected, "a", visible.clone(), 0);
        continue_selection_paint(&mut paint, &mut selected, visible.as_ref(), 2);
        continue_selection_paint(&mut paint, &mut selected, visible.as_ref(), 0);
        assert!(selected.is_empty());
    }

    #[test]
    fn drag_ends_if_the_live_visible_list_no_longer_matches() {
        let mut selected = HashSet::new();
        let original: Rc<[String]> = vec!["a".to_string(), "b".to_string()].into();
        let refreshed = vec!["new".to_string(), "a".to_string(), "b".to_string()];

        let mut paint = SelectionPaint::begin(&mut selected, "a", original.clone(), 0);
        continue_selection_paint(&mut paint, &mut selected, &refreshed, 2);
        continue_selection_paint(&mut paint, &mut selected, original.as_ref(), 1);
        assert_eq!(selected, HashSet::from(["a".to_string()]));
        assert!(paint.is_none());

        // An index past the end of the live list ends the gesture the same way.
        let mut paint = SelectionPaint::begin(&mut selected, "b", original.clone(), 1);
        continue_selection_paint(&mut paint, &mut selected, original.as_ref(), 5);
        assert_eq!(selected, HashSet::from(["a".to_string(), "b".to_string()]));
        assert!(paint.is_none());
    }

    #[test]
    fn select_all_uses_only_listed_runs_and_select_none_clears_everything() {
        let mut pick = HashSet::new();
        let listed = ["a", "b"].map(String::from);

        all_or_none_selection(&mut pick, &listed);
        assert_eq!(pick, HashSet::from(listed.clone()));

        pick.insert("hidden".to_string());
        all_or_none_selection(&mut pick, &listed);
        assert!(pick.is_empty());

        all_or_none_selection(&mut pick, &listed);
        assert_eq!(pick, HashSet::from(listed));
    }

    #[test]
    fn selection_action_names_empty_filtered_lists_explicitly() {
        assert_eq!(selection_action_label(0, false, false), "Show all");
        assert_eq!(selection_action_label(0, true, false), "Show all listed");
        assert_eq!(selection_action_label(1, false, false), "Hide all");
        assert_eq!(selection_action_label(1, true, false), "Hide all");
        assert_eq!(selection_action_label(0, false, true), "Select all");
        assert_eq!(selection_action_label(0, true, true), "Select all listed");
        assert_eq!(selection_action_label(1, false, true), "Select none");
        assert_eq!(selection_action_label(1, true, true), "Select none");
    }

    #[test]
    fn run_list_empty_state_distinguishes_filters_from_no_runs() {
        assert_eq!(run_list_empty_message(0, 0), Some("No runs"));
        assert_eq!(run_list_empty_message(3, 0), Some("No matching runs"));
        assert_eq!(run_list_empty_message(3, 2), None);
    }

    #[test]
    fn sidebar_run_labels_disambiguate_every_row_only_when_names_repeat() {
        let unique = [
            RunInfo {
                run_name: "baseline".to_string(),
                ordinal: 7,
                ..Default::default()
            },
            RunInfo {
                run_name: "candidate".to_string(),
                ordinal: 2,
                ..Default::default()
            },
        ];
        assert!(!sidebar_needs_run_ordinals(&unique));
        assert_eq!(sidebar_run_label(&unique[0], false), "baseline");

        let duplicates = [
            RunInfo {
                run_name: "baseline".to_string(),
                ordinal: 1,
                ..Default::default()
            },
            RunInfo {
                run_name: "baseline".to_string(),
                ordinal: 2,
                ..Default::default()
            },
            RunInfo {
                run_name: "baseline #1".to_string(),
                ordinal: 3,
                ..Default::default()
            },
        ];
        assert!(sidebar_needs_run_ordinals(&duplicates));
        assert_eq!(
            duplicates
                .iter()
                .map(|run| sidebar_run_label(run, true))
                .collect::<Vec<_>>(),
            ["baseline #1", "baseline #2", "baseline #1 #3"]
        );
    }

    #[test]
    fn saved_run_selection_preserves_explicit_none_and_filters_stale_runs() {
        let runs = ["a", "b", "a,b"].map(|run_id| RunInfo {
            run_id: run_id.to_string(),
            ..Default::default()
        });

        assert_eq!(restored_run_selection("", &runs), Some(HashSet::new()));
        assert_eq!(
            restored_json_run_selection("[]", &runs),
            Some(HashSet::new())
        );
        assert_eq!(restored_run_selection("stale", &runs), None);
        assert_eq!(
            restored_run_selection("stale,b,a", &runs),
            Some(HashSet::from(["a".to_string(), "b".to_string()]))
        );
        assert_eq!(
            restored_json_run_selection(r#"["a,b"]"#, &runs),
            Some(HashSet::from(["a,b".to_string()]))
        );
        assert_eq!(restored_json_run_selection(r#"["stale"]"#, &runs), None);
        assert_eq!(restored_json_run_selection("invalid", &runs), None);

        let json_like_runs = [r#"["a"]"#, "a"].map(|run_id| RunInfo {
            run_id: run_id.to_string(),
            ..Default::default()
        });
        assert_eq!(
            restored_run_selection(r#"["a"]"#, &json_like_runs),
            Some(HashSet::from([r#"["a"]"#.to_string()]))
        );
        assert_eq!(
            restored_json_run_selection(r#"["a"]"#, &json_like_runs),
            Some(HashSet::from(["a".to_string()]))
        );
    }

    #[test]
    fn saved_run_selection_is_stable_json() {
        assert_eq!(
            persisted_run_selection(&HashSet::from([
                "z".to_string(),
                "a,b".to_string(),
                "a".to_string(),
            ])),
            r#"["a","a,b","z"]"#,
        );
        assert_eq!(persisted_run_selection(&HashSet::new()), "[]");
    }

    #[test]
    fn partial_server_results_remove_only_confirmed_runs() {
        let requested = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let results = vec![
            TrashRunResult {
                run_id: "a".to_string(),
                outcome: TrashRunOutcome::Trashed as i32,
                ..Default::default()
            },
            TrashRunResult {
                run_id: "b".to_string(),
                outcome: TrashRunOutcome::Error as i32,
                error: "busy".to_string(),
            },
        ];

        let summary = summarize_trash_results(&requested, &results);
        assert_eq!(summary.succeeded, HashSet::from(["a".to_string()]));
        assert_eq!(
            summary.failed,
            HashSet::from(["b".to_string(), "c".to_string()])
        );
        assert_eq!(summary.first_failure.as_deref(), Some("busy"));
    }

    #[test]
    fn rename_response_changes_only_the_name_on_the_expected_snapshot() {
        let mut runs = vec![
            RunInfo {
                project_id: "p".to_string(),
                run_id: "a".to_string(),
                run_name: "old".to_string(),
                created_at_ms: 42,
                ..Default::default()
            },
            RunInfo {
                project_id: "p".to_string(),
                run_id: "b".to_string(),
                run_name: "keep".to_string(),
                ..Default::default()
            },
        ];

        assert!(apply_rename_if_current(&mut runs, "p", "a", "old", "new").is_some());
        assert_eq!(runs[0].run_name, "new");
        assert_eq!(runs[0].created_at_ms, 42);
        assert_eq!(runs[1].run_name, "keep");

        // A delayed response from the first rename must not overwrite a newer
        // rename that was already observed through ListRuns.
        assert!(apply_rename_if_current(&mut runs, "p", "a", "old", "stale").is_none());
        assert_eq!(runs[0].run_name, "new");
    }
}
