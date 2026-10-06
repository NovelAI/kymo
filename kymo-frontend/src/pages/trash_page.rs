use std::collections::HashSet;

use dioxus::prelude::*;
use wasm_bindgen::JsValue;

use crate::grpc::proto::{
    ListTrashRequest, ListTrashResponse, RestoreRunOutcome, RunLifecycleState, RunRecord,
    TrashCursor,
};
use crate::grpc::GrpcClient;
use crate::route::Route;
use crate::state::app_state::request_refresh;
use crate::state::trash::{
    clock_wait_ms, compact_duration, effective_lifecycle, extrapolated_now_ms, lookup_trashed_runs,
    monotonic_now_ms,
};
use crate::util::{js_bridge::js_string, local_time, primary};

const TRASH_PAGE_SIZE: u32 = 100;
const RESTORE: &str = "Restore";
const RESTORING: &str = "Restoring…";
const UNAVAILABLE: &str = "Unavailable";

fn run_key(project_id: &str, run_id: &str) -> String {
    serde_json::to_string(&(project_id, run_id))
        .expect("trash row identity contains only JSON strings")
}

fn record_is(record: &RunRecord, project_id: &str, run_id: &str) -> bool {
    record
        .run
        .as_ref()
        .is_some_and(|run| run.project_id == project_id && run.run_id == run_id)
}

fn iso_time(ms: i64) -> String {
    js_sys::Date::new(&JsValue::from_f64(ms as f64))
        .to_iso_string()
        .into()
}

fn restore_failure(outcome: RestoreRunOutcome, detail: &str) -> String {
    match outcome {
        RestoreRunOutcome::Expired => "Recovery has expired; this run is being deleted.".into(),
        RestoreRunOutcome::NotFound => "This run no longer exists.".into(),
        RestoreRunOutcome::Unknown => "The server returned an unknown restore result.".into(),
        _ if !detail.is_empty() => detail.to_string(),
        _ => "The run could not be restored.".into(),
    }
}

fn append_snapshot_page(first: &mut ListTrashResponse, next: ListTrashResponse) -> bool {
    if first.global_version != next.global_version {
        return false;
    }
    first.runs.extend(next.runs);
    first.server_now_ms = first.server_now_ms.max(next.server_now_ms);
    first.next = next.next;
    true
}

async fn list_trash_prefix(min_rows: usize) -> Result<ListTrashResponse, tonic::Status> {
    'snapshot: loop {
        crate::grpc::wait_until_page_visible().await;
        let mut response = GrpcClient::new()
            .list_trash(ListTrashRequest {
                page_size: TRASH_PAGE_SIZE,
                ..Default::default()
            })
            .await?;
        while response.runs.len() < min_rows {
            let Some(cursor) = response.next.clone() else {
                break;
            };
            crate::grpc::wait_until_page_visible().await;
            let next = GrpcClient::new()
                .list_trash(ListTrashRequest {
                    page_size: TRASH_PAGE_SIZE,
                    after: Some(cursor),
                    ..Default::default()
                })
                .await?;
            if !append_snapshot_page(&mut response, next) {
                continue 'snapshot;
            }
        }
        return Ok(response);
    }
}

async fn focus_after_final_page(previous_count: usize) {
    let script = format!(
        "(()=>{{\
            let attempts=0;\
            const focus=()=>{{\
                if(document.querySelector('.trash-load-more button')&&attempts++<10){{requestAnimationFrame(focus);return}}\
                const rows=[...document.querySelectorAll('tr.trash-run')];\
                const action=row=>row.querySelector('.trash-run-action button:not(:disabled)');\
                const target=rows.slice({previous_count}).map(action).find(Boolean)\
                    ||rows.slice(0,{previous_count}).reverse().map(action).find(Boolean)\
                    ||document.querySelector('.trash-back-link');\
                target?.focus();\
            }};\
            requestAnimationFrame(focus);\
        }})()"
    );
    let _ = document::eval(&script).await;
}

async fn remove_trash_row_with_focus(
    mut data: Signal<TrashPageData>,
    project_id: &str,
    run_id: &str,
) {
    let encoded_project_id = js_string(project_id);
    let encoded_run_id = js_string(run_id);
    let script = format!(
        "(()=>{{\
            const rows=[...document.querySelectorAll('tr.trash-run')];\
            const i=rows.findIndex(row=>row.dataset.projectId==={encoded_project_id}&&row.dataset.runId==={encoded_run_id});\
            let target=null;\
            if(i>=0){{\
                for(let j=i+1;j<rows.length&&!target;j++)target=rows[j].querySelector('.trash-run-action button:not(:disabled)');\
                for(let j=i-1;j>=0&&!target;j--)target=rows[j].querySelector('.trash-run-action button:not(:disabled)');\
            }}\
            const removed=()=>[...document.querySelectorAll('tr.trash-run')].find(row=>row.dataset.projectId==={encoded_project_id}&&row.dataset.runId==={encoded_run_id});\
            const otherRestore=()=>[...document.querySelectorAll('tr.trash-run')].filter(row=>row.dataset.projectId!=={encoded_project_id}||row.dataset.runId!=={encoded_run_id}).map(row=>row.querySelector('.trash-run-action button:not(:disabled)')).find(Boolean);\
            const fallback=()=>otherRestore()||document.querySelector('.trash-load-more button:not(:disabled)')||document.querySelector('.trash-back-link');\
            let attempts=0;\
            const focus=()=>{{\
                if(removed()&&attempts++<10){{requestAnimationFrame(focus);return}}\
                if(target?.isConnected&&!target.disabled)target.focus();\
                else fallback()?.focus();\
            }};\
            requestAnimationFrame(focus);\
        }})()"
    );
    let _ = document::eval(&script).await;
    data.write().remove(project_id, run_id);
}

#[derive(Clone, Default, PartialEq)]
struct TrashPageData {
    runs: Vec<RunRecord>,
    total_count: u64,
    version: u64,
    next: Option<TrashCursor>,
    server_now_ms: i64,
    observed_monotonic_ms: f64,
    loaded: bool,
    loading: bool,
    error: Option<String>,
    loading_more: bool,
    load_more_error: Option<String>,
}

impl TrashPageData {
    fn now_ms(&self) -> i64 {
        extrapolated_now_ms(self.server_now_ms, self.observed_monotonic_ms)
    }

    fn note_clock(&mut self, server_now_ms: i64) {
        if server_now_ms <= 0 {
            return;
        }
        self.server_now_ms = if self.server_now_ms > 0 {
            server_now_ms.max(self.now_ms())
        } else {
            server_now_ms
        };
        self.observed_monotonic_ms = monotonic_now_ms();
    }

    fn replace_page(&mut self, response: ListTrashResponse) {
        self.note_clock(response.server_now_ms);
        self.runs = response.runs;
        self.total_count = response.total_count;
        self.version = response.global_version;
        self.next = response.next;
        self.loaded = true;
        self.loading = false;
        self.loading_more = false;
        self.error = None;
        self.load_more_error = None;
    }

    fn remove(&mut self, project_id: &str, run_id: &str) {
        let old_len = self.runs.len();
        self.runs
            .retain(|record| !record_is(record, project_id, run_id));
        if self.runs.len() != old_len {
            self.total_count = self.total_count.saturating_sub(1);
        }
    }

    fn replace_record(&mut self, replacement: RunRecord, project_id: &str, run_id: &str) {
        if let Some(record) = self
            .runs
            .iter_mut()
            .find(|record| record_is(record, project_id, run_id))
        {
            *record = replacement;
        }
    }

    fn mark_expired(&mut self, project_id: &str, run_id: &str) {
        if let Some(record) = self
            .runs
            .iter_mut()
            .find(|record| record_is(record, project_id, run_id))
        {
            record.state = RunLifecycleState::Expired as i32;
        }
    }
}

#[component]
pub fn TrashPage() -> Element {
    let mut data = use_signal(TrashPageData::default);
    let refresh = use_signal(|| 0u64);
    let restoring = use_signal(HashSet::<String>::new);
    let action_message = use_signal(String::new);

    // This page is the only consumer of global Trash state. Lifecycle pushes
    // reload the prefix already visible; no app-wide count bridge or poller
    // is needed.
    use_future(move || async move {
        let mut subscription = crate::grpc::subscribe_push();
        loop {
            let update = subscription.next_visible().await;
            if update.global.is_some() || update.resync_gen.is_some() {
                request_refresh(refresh);
            }
        }
    });

    let _first_page = use_resource(move || {
        let generation = *refresh.read();
        async move {
            if generation == 0 {
                return;
            }
            {
                let mut page = data.write();
                page.loading = true;
                page.error = None;
                page.load_more_error = None;
            }
            let min_rows = data.peek().runs.len().max(TRASH_PAGE_SIZE as usize);
            match list_trash_prefix(min_rows).await {
                Ok(response) => data.write().replace_page(response),
                Err(status) => {
                    let mut page = data.write();
                    page.loading = false;
                    page.loading_more = false;
                    page.error = Some(status.message().to_string());
                }
            }
        }
    });

    // Relative times advance from the latest server clock and rows become
    // unavailable at purge_at without waiting for another network event.
    let mut clock_tick = use_signal(|| 0u64);
    let _clock = use_resource(move || {
        let tick = *clock_tick.read();
        let page = data.read();
        let now_ms = page.now_ms();
        let wait_ms = clock_wait_ms(
            now_ms,
            page.runs.iter().filter_map(|record| record.purge_at_ms),
            page.loaded,
        );
        drop(page);
        async move {
            gloo_timers::future::sleep(std::time::Duration::from_millis(wait_ms)).await;
            clock_tick.set(tick.wrapping_add(1));
        }
    });
    let _ = *clock_tick.read();

    let page = data.read().clone();
    let now_ms = page.now_ms();
    let loaded_count = page.runs.len();
    let count_label = if page.total_count == 1 {
        "1 run".to_string()
    } else {
        format!("{} runs", page.total_count)
    };
    let action_message_text = action_message.read().clone();

    rsx! {
        document::Title { "Trash — kymo" }
        main { class: "trash-page",
            div { class: "trash-page-inner",
                header { class: "trash-header",
                    Link { to: Route::ProjectsPage {}, class: "trash-back-link", "← Projects" }
                    h1 { "Trash" }
                    p { class: "trash-description",
                        "Runs are recoverable for 7 days after deletion."
                    }
                }

                if let Some(error) = page.error.as_ref() {
                    div { class: "trash-error", role: "alert",
                        span { "Couldn’t load Trash: {error}" }
                        button {
                            class: "btn btn-ghost",
                            disabled: page.loading,
                            onmousedown: primary(move |_| request_refresh(refresh)),
                            "Retry"
                        }
                    }
                }

                if !action_message_text.is_empty() {
                    div {
                        class: "trash-action-message",
                        role: "status",
                        aria_live: "polite",
                        aria_atomic: "true",
                        "{action_message_text}"
                    }
                }

                section { class: "trash-section", aria_labelledby: "trash-runs-heading",
                    // Preserve table semantics when compact CSS lays rows out as grids.
                    table { class: "trash-table", role: "table",
                        thead { role: "rowgroup",
                            tr { role: "row",
                                th { scope: "col", role: "columnheader",
                                    div { class: "trash-table-title",
                                        h2 { id: "trash-runs-heading", "Runs" }
                                        if page.loaded {
                                            span { "{count_label}" }
                                        }
                                    }
                                }
                                th {
                                    scope: "col",
                                    role: "columnheader",
                                    "Deletes permanently"
                                }
                                th { scope: "col", role: "columnheader", class: "trash-run-action",
                                    span { class: "visually-hidden", "Actions" }
                                    span { class: "btn trash-restore-sizer", aria_hidden: "true",
                                        span { "{RESTORE}" }
                                        span { "{RESTORING}" }
                                        span { "{UNAVAILABLE}" }
                                    }
                                }
                            }
                        }
                        if !page.runs.is_empty() {
                            tbody { role: "rowgroup",
                                for record in page.runs {
                                    {trash_row(record, now_ms, restoring, action_message, data, refresh)}
                                }
                            }
                        }
                    }

                    if !page.loaded && page.error.is_none() {
                        div { class: "trash-empty", role: "status", h3 { "Loading Trash…" } }
                    } else if page.loaded && page.total_count == 0 {
                        div { class: "trash-empty", role: "status", h3 { "Trash is empty" } }
                    }

                    if let Some(cursor) = page.next.clone() {
                        div { class: "trash-load-more",
                            if let Some(error) = page.load_more_error.as_ref() {
                                span { role: "alert", "Couldn’t load more: {error}" }
                            } else {
                                span { "Showing {loaded_count} of {page.total_count}" }
                            }
                            button {
                                class: "btn btn-ghost",
                                disabled: page.loading_more || page.loading,
                                onmousedown: primary(move |_| {
                                    if data.peek().loading_more || data.peek().loading {
                                        return;
                                    }
                                    {
                                        let mut page = data.write();
                                        page.loading_more = true;
                                        page.load_more_error = None;
                                    }
                                    let cursor = cursor.clone();
                                    let expected_version = page.version;
                                    let expected_refresh = *refresh.peek();
                                    spawn(async move {
                                        crate::grpc::wait_until_page_visible().await;
                                        let response = GrpcClient::new()
                                            .list_trash(ListTrashRequest {
                                                page_size: TRASH_PAGE_SIZE,
                                                after: Some(cursor),
                                                ..Default::default()
                                            })
                                            .await;
                                        if *refresh.peek() != expected_refresh {
                                            return;
                                        }
                                        match response {
                                            Ok(response)
                                                if response.global_version == expected_version
                                                    && data.peek().version == expected_version => {
                                                let previous_count = data.peek().runs.len();
                                                let final_page = response.next.is_none();
                                                let mut page = data.write();
                                                page.note_clock(response.server_now_ms);
                                                page.runs.extend(response.runs);
                                                page.next = response.next;
                                                page.loading_more = false;
                                                drop(page);
                                                if final_page {
                                                    focus_after_final_page(previous_count).await;
                                                }
                                            }
                                            Ok(_) => {
                                                data.write().loading_more = false;
                                                request_refresh(refresh);
                                            }
                                            Err(status) => {
                                                let mut page = data.write();
                                                page.loading_more = false;
                                                page.load_more_error = Some(status.message().to_string());
                                            }
                                        }
                                    });
                                }),
                                if page.loading_more { "Loading…" } else { "Load more" }
                            }
                        }
                    }
                }

            }
        }
    }
}

fn trash_row(
    record: RunRecord,
    now_ms: i64,
    mut restoring: Signal<HashSet<String>>,
    mut action_message: Signal<String>,
    mut data: Signal<TrashPageData>,
    refresh: Signal<u64>,
) -> Element {
    let Some(run) = record.run.as_ref() else {
        return rsx! {};
    };
    let project_id = &run.project_id;
    let run_id = &run.run_id;
    let run_name = &run.run_name;
    let ordinal = run.ordinal;
    let key = run_key(project_id, run_id);
    let is_restoring = restoring.read().contains(&key);
    let lifecycle = effective_lifecycle(&record, now_ms);
    let is_expired = matches!(
        lifecycle,
        RunLifecycleState::Expired | RunLifecycleState::Purging
    );
    // The accessible name starts with the visible label's words.
    let (label, button_name) = if is_restoring {
        (
            RESTORING,
            format!("Restoring {run_name} #{ordinal} to project {project_id}"),
        )
    } else if is_expired {
        (
            UNAVAILABLE,
            format!("Unavailable: {run_name} #{ordinal} in project {project_id} has expired"),
        )
    } else {
        (
            RESTORE,
            format!("Restore {run_name} #{ordinal} to project {project_id}"),
        )
    };
    let expires_in = match record.purge_at_ms {
        _ if is_expired => "Expired — deleting…".to_string(),
        Some(purge_at) => format!(
            "{} remaining",
            compact_duration(purge_at.saturating_sub(now_ms))
        ),
        None => "Deletion scheduled".to_string(),
    };

    rsx! {
        tr {
            key: "{key}",
            role: "row",
            class: "trash-run",
            "data-project-id": "{project_id}",
            "data-run-id": "{run_id}",
            td { role: "cell",
                div { class: "trash-run-title-line",
                    Link {
                        to: Route::RunPage {
                            project_id: project_id.clone(),
                            run_id: run_id.clone(),
                            chart: None.into(),
                        },
                        class: "trash-run-link",
                        title: "{run_name}",
                        aria_label: "Open {run_name} #{ordinal}, currently in Trash",
                        span { class: "trash-run-name-host fade-overflow",
                            span {
                                class: "trash-run-name-placeholder",
                                aria_hidden: "true",
                                "{run_name}"
                            }
                            span { class: "trash-run-name", "{run_name}" }
                        }
                        span { class: "trash-run-ordinal", "#{ordinal}" }
                    }
                    Link {
                        to: Route::ProjectPage { project_id: project_id.clone(), chart: None.into() },
                        class: "trash-run-project fade-overflow",
                        title: "{project_id}",
                        span { "{project_id}" }
                    }
                }
            }
            td { role: "cell",
                div { class: "trash-run-expiry",
                    if let Some(ms) = record.purge_at_ms {
                        time { datetime: "{iso_time(ms)}", "{local_time(ms)}" }
                    } else {
                        span { "Scheduled" }
                    }
                    span { class: "trash-expiry-relative", "{expires_in}" }
                }
            }
            td { class: "trash-run-action", role: "cell",
                button {
                    class: "btn btn-ghost",
                    r#type: "button",
                    disabled: is_expired || is_restoring,
                    aria_label: "{button_name}",
                    onmousedown: primary({
                        let project_id = project_id.clone();
                        let run_id = run_id.clone();
                        let key = key.clone();
                        move |_| {
                            if restoring.peek().contains(&key) {
                                return;
                            }
                            restoring.write().insert(key.clone());
                            action_message.set(String::new());
                            let project_id = project_id.clone();
                            let run_id = run_id.clone();
                            let key = key.clone();
                            spawn(async move {
                                match GrpcClient::new().restore_run(&project_id, &run_id).await {
                                    Ok(response)
                                        if matches!(
                                            response.outcome(),
                                            RestoreRunOutcome::Restored
                                                | RestoreRunOutcome::AlreadyActive
                                        ) =>
                                    {
                                        remove_trash_row_with_focus(
                                            data,
                                            &project_id,
                                            &run_id,
                                        )
                                        .await;
                                        action_message.set("Run restored.".to_string());
                                    }
                                    Ok(response) if response.outcome() == RestoreRunOutcome::NotFound => {
                                        remove_trash_row_with_focus(
                                            data,
                                            &project_id,
                                            &run_id,
                                        )
                                        .await;
                                        action_message.set(restore_failure(response.outcome(), &response.error));
                                    }
                                    Ok(response) if response.outcome() == RestoreRunOutcome::Expired => {
                                        data.write().mark_expired(&project_id, &run_id);
                                        action_message.set(restore_failure(response.outcome(), &response.error));
                                    }
                                    Err(status) if crate::grpc::not_sent(&status) => {
                                        action_message.set(format!("Couldn’t restore the run: {}.", status.message()));
                                    }
                                    result => {
                                        let diagnostic = match &result {
                                            Ok(response) => restore_failure(response.outcome(), &response.error),
                                            Err(status) => format!(
                                                "The Restore request did not return a result: {}",
                                                status.message()
                                            ),
                                        };
                                        action_message.set("Verifying the restore outcome…".to_string());
                                        match lookup_trashed_runs(
                                            &project_id,
                                            std::slice::from_ref(&run_id),
                                        )
                                        .await
                                        {
                                            Ok(records) => {
                                                if let Some(record) = records.into_iter().find(|record| {
                                                    record_is(record, &project_id, &run_id)
                                                }) {
                                                    data.write().replace_record(
                                                        record,
                                                        &project_id,
                                                        &run_id,
                                                    );
                                                    action_message.set(diagnostic);
                                                } else {
                                                    remove_trash_row_with_focus(
                                                        data,
                                                        &project_id,
                                                        &run_id,
                                                    )
                                                    .await;
                                                    action_message.set(
                                                        "Run is no longer in Trash.".to_string(),
                                                    );
                                                }
                                            }
                                            Err(error) => action_message.set(format!(
                                                "Couldn’t verify the restore outcome: {error}. Refresh Trash before retrying."
                                            )),
                                        }
                                    }
                                }
                                request_refresh(refresh);
                                restoring.write().remove(&key);
                            });
                        }
                    }),
                    "{label}"
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(run_id: &str) -> RunRecord {
        RunRecord {
            run: Some(crate::grpc::proto::RunInfo {
                project_id: "project".to_string(),
                run_id: run_id.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn trash_row_keys_preserve_identity_boundaries() {
        assert_ne!(run_key("a", "b\u{1f}c"), run_key("a\u{1f}b", "c"));
        assert_eq!(run_key("project", "run"), run_key("project", "run"));
    }

    #[test]
    fn restore_outcomes_have_specific_terminal_messages() {
        assert!(restore_failure(RestoreRunOutcome::Expired, "").contains("expired"));
        assert!(restore_failure(RestoreRunOutcome::NotFound, "").contains("no longer"));
        assert_eq!(
            restore_failure(RestoreRunOutcome::Error, "database busy"),
            "database busy"
        );
    }

    #[test]
    fn refresh_pages_accumulate_without_collapsing_loaded_depth() {
        let mut first = ListTrashResponse {
            runs: vec![record("one")],
            global_version: 7,
            server_now_ms: 10,
            total_count: 2,
            next: Some(TrashCursor::default()),
        };
        let next = ListTrashResponse {
            runs: vec![record("two")],
            global_version: 7,
            server_now_ms: 12,
            total_count: 0,
            next: None,
        };

        assert!(append_snapshot_page(&mut first, next));
        assert_eq!(first.runs.len(), 2);
        assert_eq!(first.server_now_ms, 12);
        assert_eq!(first.total_count, 2);
        assert!(first.next.is_none());
    }

    #[test]
    fn refresh_restarts_instead_of_mixing_snapshot_versions() {
        let mut first = ListTrashResponse {
            runs: vec![record("one")],
            global_version: 7,
            ..Default::default()
        };
        let next = ListTrashResponse {
            runs: vec![record("two")],
            global_version: 8,
            ..Default::default()
        };

        assert!(!append_snapshot_page(&mut first, next));
        assert_eq!(first.runs.len(), 1);
        assert_eq!(first.global_version, 7);
    }
}
