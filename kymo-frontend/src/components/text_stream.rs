mod memory;
mod viewport;

use memory::{
    remember_panel, remember_scroll, remembered_panel, remembered_scroll, text_log_key,
    text_run_key, text_scroll_key, TextPanelState,
};
use viewport::{
    observe_height, MeasuredViewport, ScrollAnchor, ScrollViewport, VirtualWindow,
    DEFAULT_LINE_HEIGHT_PX,
};

use dioxus::prelude::*;
use wasm_bindgen::JsCast;

use crate::components::sidebar::{names_repeat, sidebar_run_label};
use crate::components::uplot_chart::{hash_color, run_color};
use crate::grpc::proto::{RunInfo, RunStatus, SeriesRef, TextLine};
use crate::state::app_state::find_run;
use crate::state::visibility::{self, Zone};
use crate::state::{run_ordinal_for, stamp_covers, DashboardState, UserConfigState};
use crate::util::resize_observer::ElementResizeObserver;
use crate::util::{is_app_escape, primary};

/// A line's time since the stream's first line, for its tooltip: captured output is stepped by its timestamp in ms.
fn format_elapsed(step: i64, first_step: i64) -> String {
    let secs = (step - first_step) as f64 / 1000.0;
    if secs < 60.0 {
        format!("{secs:.0}s")
    } else if secs < 3600.0 {
        format!("{:.1}m", secs / 60.0)
    } else {
        format!("{:.1}h", secs / 3600.0)
    }
}

fn stream_failure_is_permanent(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::InvalidArgument | tonic::Code::ResourceExhausted
    )
}

/// The text skip rule: a fetch-resource re-run for the window and search revision on screen does not query while that answer's version stamp covers the run's known version ([`stamp_covers`]). Any other cause — a new window, a search resubmit, or a version above the stamp (a retry repeats one of these) — queries as before; the last keeps a live log following new lines. An error on screen answers nothing, so it never skips.
fn text_skip_query(
    shown: Option<&WindowData>,
    window: VirtualWindow,
    revision: u64,
    known: Option<u64>,
) -> bool {
    shown.is_some_and(|data| {
        data.window == window && data.revision == revision && stamp_covers(data.version, known)
    })
}

struct WindowData {
    window: VirtualWindow,
    total_lines: u64,
    first_step: i64,
    lines: Vec<TextLine>,
    /// The search revision and the run's version stamp this window answered at, for the text skip.
    revision: u64,
    version: Option<u64>,
}

type TextResponse = Result<WindowData, String>;

#[derive(Clone, Debug, PartialEq)]
struct RunStreams {
    project_id: String,
    run_id: String,
    metric_names: Vec<String>,
}

/// Each tab's (text, full label): repeated names get the sidebar's "name #ordinal" text and a full label that adds the project; otherwise both are the name.
fn text_run_labels(runs: &[RunStreams], display_runs: &[RunInfo]) -> Vec<(String, String)> {
    let resolved = runs
        .iter()
        .map(|run| find_run(display_runs, &run.run_id))
        .collect::<Vec<_>>();
    // Unknown metadata leaves the run ID as the name, with no ordinal.
    let names = runs
        .iter()
        .zip(&resolved)
        .map(|(run, info)| info.map_or(run.run_id.as_str(), |info| info.run_name.as_str()));
    let repeat = names_repeat(names.clone());
    runs.iter()
        .zip(&resolved)
        .zip(names)
        .map(|((run, info), name)| {
            if !repeat {
                return (name.to_string(), name.to_string());
            }
            let text = info.map_or_else(|| name.to_string(), |info| sidebar_run_label(info, true));
            let full = format!("{text} ({})", run.project_id);
            (text, full)
        })
        .collect()
}

fn metric_label<'a>(metric_names: &[String], line_metric_name: &'a str) -> Option<&'a str> {
    (metric_names.len() > 1).then_some(line_metric_name)
}

/// One tab per run over the active run's log, fetching only the visible lines plus an overscan band.
#[component]
pub fn TextStreamViewer(
    stream_refs: Vec<SeriesRef>,
    /// Viewport zone from the owning rect: out-of-band viewers freeze and catch up on re-entry.
    zone: Signal<Zone>,
    /// Key for remembering search, the active tab, and per-log scroll across body unmounts; grid and maximized copies use separate keys.
    persist_key: String,
) -> Element {
    let state = use_context::<DashboardState>();
    let mut runs = Vec::<RunStreams>::new();
    for SeriesRef {
        project_id,
        run_id,
        metric_name,
        ..
    } in stream_refs
    {
        if let Some(run) = runs
            .iter_mut()
            .find(|run| run.project_id == project_id && run.run_id == run_id)
        {
            if !run.metric_names.contains(&metric_name) {
                run.metric_names.push(metric_name);
            }
        } else {
            runs.push(RunStreams {
                project_id,
                run_id,
                metric_names: vec![metric_name],
            });
        }
    }

    let initial = use_hook(|| remembered_panel(&persist_key));
    let mut search_draft = use_signal(|| initial.draft.clone());
    let mut search = use_signal(|| initial.committed.clone());
    let mut chosen_tab = use_signal(|| initial.tab.clone());
    let mut search_revision = use_signal(|| 0u64);
    let mut clear_search = move || {
        search_draft.set(String::new());
        search.set(String::new());
    };
    use_drop({
        let persist_key = persist_key.clone();
        move || {
            remember_panel(
                &persist_key,
                TextPanelState {
                    draft: search_draft.peek().clone(),
                    committed: search.peek().clone(),
                    tab: chosen_tab.peek().clone(),
                },
            )
        }
    });
    // Keyed to its log so a tab switch never shows a stale count.
    let line_count = use_signal(|| None::<(String, u64)>);
    let committed_search = search.read().clone();
    // Tab colors come from localStorage, so subscribe to the generation the sidebar bumps after changing an override.
    let _color_version = *state.color_version.read();
    let display_runs = state.display_runs();
    let tab_labels = text_run_labels(&runs, &display_runs);
    let run_keys = runs
        .iter()
        .map(|run| text_run_key(&run.project_id, &run.run_id))
        .collect::<Vec<_>>();
    // Until a tab is clicked, the first run shown counts as chosen, so a newer run sorting first cannot take the panel over.
    let first_key = run_keys.first().cloned();
    use_effect(use_reactive((&first_key,), move |(first_key,)| {
        if chosen_tab.peek().is_none() && first_key.is_some() {
            chosen_tab.set(first_key);
        }
    }));
    // A chosen run that leaves the panel falls back to the first tab; the choice returns with the run.
    let active = chosen_tab
        .read()
        .as_ref()
        .and_then(|chosen| run_keys.iter().position(|key| key == chosen))
        .unwrap_or(0);
    let active_log = runs.get(active).map(|run| {
        text_log_key(
            &run.project_id,
            &run.run_id,
            &run.metric_names,
            &committed_search,
        )
    });
    // Dioxus keeps tab nodes only when the keyed button is the loop body's root.
    let tabs = runs
        .iter()
        .zip(tab_labels.iter().cloned())
        .zip(run_keys)
        .enumerate()
        .map(|(index, ((run, (text, full)), run_key))| {
            let color = run_color(&run.run_id, run_ordinal_for(&display_runs, &run.run_id));
            (run_key, color, text, full, index == active)
        })
        .collect::<Vec<_>>();
    let count_label = line_count
        .read()
        .as_ref()
        .filter(|(key, _)| Some(key) == active_log.as_ref())
        .map(|(_, total)| {
            if committed_search.is_empty() {
                format!("{total} lines")
            } else {
                format!("{total} matching lines")
            }
        });

    rsx! {
        div { class: "text-stream-viewer",
            form {
                class: "text-stream-toolbar",
                onsubmit: move |event| {
                    event.prevent_default();
                    search.set(search_draft.read().trim().to_string());
                    let next_revision = search_revision.peek().wrapping_add(1);
                    search_revision.set(next_revision);
                },
                // Not `type=search`: its built-in clear button and Esc clear empty only the box and leave an applied search applied.
                input {
                    r#type: "text",
                    role: "searchbox",
                    class: "text-stream-search",
                    placeholder: "Search logs",
                    aria_label: "Search logs",
                    title: "Search is performed on the server",
                    maxlength: 128,
                    value: "{search_draft}",
                    oninput: move |event| search_draft.set(event.value()),
                    // Esc does what Clear does while there's something to clear, consuming the key so a maximized chart stays open.
                    onkeydown: move |e: Event<KeyboardData>| {
                        if is_app_escape(&e)
                            && !(search_draft.peek().is_empty() && search.peek().is_empty())
                        {
                            e.prevent_default();
                            clear_search();
                        }
                    },
                }
                button { r#type: "submit", class: "text-stream-search-submit", "Search" }
                if !committed_search.is_empty() {
                    button {
                        r#type: "button",
                        class: "text-stream-search-clear",
                        onmousedown: primary(move |_| clear_search()),
                        "Clear"
                    }
                }
                if let Some(count) = count_label {
                    span { class: "text-stream-count", "{count}" }
                }
            }

            div { class: "text-stream-tabs", role: "tablist", aria_label: "Runs",
                for (run_key, color, text, full, selected) in tabs {
                    button {
                        key: "{run_key}",
                        r#type: "button",
                        role: "tab",
                        class: "text-stream-tab",
                        aria_selected: selected,
                        title: "{full}",
                        aria_label: "{full}",
                        style: "--run-color: {color};",
                        onmousedown: primary({
                            let run_key = run_key.clone();
                            move |_| chosen_tab.set(Some(run_key.clone()))
                        }),
                        span { class: "fade-overflow",
                            span { "{text}" }
                        }
                    }
                }
            }

            if let (Some(run), Some(log_key)) = (runs.get(active), active_log) {
                {
                    // Stuck runs are alive but silent, so their tail is still live.
                    let live = find_run(&display_runs, &run.run_id).is_some_and(|info| {
                        matches!(info.status(), RunStatus::Running | RunStatus::Stuck)
                    });
                    rsx! {
                        VirtualTextLog {
                            key: "{log_key}",
                            source: run.clone(),
                            display_name: tab_labels[active].1.clone(),
                            live,
                            search: committed_search.clone(),
                            search_revision,
                            zone,
                            scroll_key: text_scroll_key(&persist_key, &log_key),
                            log_key: log_key.clone(),
                            line_count,
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn VirtualTextLog(
    source: RunStreams,
    display_name: String,
    live: bool,
    search: String,
    search_revision: Signal<u64>,
    zone: Signal<Zone>,
    scroll_key: String,
    log_key: String,
    line_count: Signal<Option<(String, u64)>>,
) -> Element {
    let state = use_context::<DashboardState>();
    let user_config = use_context::<UserConfigState>();
    let line_height = use_memo(move || user_config.font_size().scale_px(DEFAULT_LINE_HEIGHT_PX));
    let line_height_px = *line_height.read();
    let mut viewport = use_signal(|| {
        ScrollViewport::new(remembered_scroll(&scroll_key).unwrap_or(if live {
            ScrollAnchor::End
        } else {
            ScrollAnchor::Line(0.0)
        }))
    });
    use_drop(move || remember_scroll(&scroll_key, viewport.peek().anchor()));
    // Wait for layout and deduplicate unchanged request windows.
    let requested = use_memo(move || viewport.read().window(*line_height.read()));
    let mut body = use_signal(|| None::<web_sys::HtmlElement>);
    let mut resize_observer = use_hook(|| CopyValue::new(None::<ElementResizeObserver>));
    let mut content = use_signal(|| None::<TextResponse>);
    let mut retry_tick = use_signal(|| 0u64);

    let version_run_id = source.run_id.clone();
    let my_version = use_memo(move || {
        crate::state::versions_key(
            0,
            std::iter::once(version_run_id.as_str()),
            &state.run_versions.read(),
        )
    });
    let allowed = use_memo(move || *zone.read() != Zone::Far);
    let mut loading = use_signal(|| false);
    let data_seq = crate::state::use_version_bridge(my_version, loading, allowed);
    let fetch_source = source.clone();
    let fetch_search = search.clone();
    let _fetch = use_resource(move || {
        let grpc = state.grpc.read().clone();
        let window = *requested.read();
        let line_height_px = *line_height.peek();
        // Resubmitting refreshes in place.
        let revision = *search_revision.read();
        // Searches are snapshots; live versions refresh only unfiltered logs.
        if fetch_search.is_empty() {
            let _version = *data_seq.read();
        }
        let _retry = *retry_tick.read();
        let allowed = *allowed.read();
        let source = fetch_source.clone();
        let search = fetch_search.clone();
        let log_key = log_key.clone();
        async move {
            crate::state::heal_loading(loading);
            if !allowed {
                return;
            }
            let Some(window) = window else {
                return;
            };
            // The skip answers a version propagation with the window already on screen instead of re-querying (text_skip_query).
            let known = state.run_versions.peek().get(&source.run_id).copied();
            if text_skip_query(
                content.peek().as_ref().and_then(|c| c.as_ref().ok()),
                window,
                revision,
                known,
            ) {
                return;
            }
            let first_paint = !matches!(*content.peek(), Some(Ok(_)));
            let _admission = visibility::admit_fetch(|| *zone.peek(), first_paint).await;
            loading.set(true);

            let sent =
                crate::state::versions_of(&state.run_versions.peek(), [source.run_id.as_str()]);
            let result = grpc
                .query_text_window(
                    &source.project_id,
                    &source.run_id,
                    &source.metric_names,
                    window.offset,
                    window.limit,
                    &search,
                )
                .await;
            let next = match result {
                Ok(response) => {
                    // A length can move the anchor into another band (a followed end, a vanished row), whose request supersedes these rows.
                    let still_planned = {
                        let mut placed = viewport.write();
                        placed.installing(response.total_lines, line_height_px);
                        placed.window(line_height_px) == Some(window)
                    };
                    if !still_planned {
                        loading.set(false);
                        return;
                    }
                    let stamps = crate::state::answer_stamps(
                        state.run_versions,
                        response.run_versions,
                        sent,
                    );
                    Some(Ok(WindowData {
                        window,
                        total_lines: response.total_lines,
                        first_step: response.first_step,
                        lines: response.lines,
                        revision,
                        version: stamps.get(&source.run_id).copied(),
                    }))
                }
                Err(status) if visibility::is_terminal_run_status(&status) => {
                    viewport.write().cleared();
                    Some(Err("Run no longer available".to_string()))
                }
                Err(status) if stream_failure_is_permanent(&status) => {
                    crate::util::warn(&format!(
                        "[text stream {}] rejected: {status}",
                        source.run_id
                    ));
                    viewport.write().discard_anchor();
                    Some(Err(status.message().to_string()))
                }
                Err(status) => {
                    crate::util::warn(&format!(
                        "[text stream {}] failed: {status}; retrying",
                        source.run_id
                    ));
                    None
                }
            };
            let retry_needed = next.is_none();
            if let Some(next) = next {
                let count = next.as_ref().ok().map(|data| (log_key, data.total_lines));
                content.set(Some(next));
                if *line_count.peek() != count {
                    line_count.set(count);
                }
            }
            loading.set(false);
            drop(_admission);
            if retry_needed {
                gloo_timers::future::sleep(std::time::Duration::from_secs(5)).await;
                crate::grpc::wait_until_page_visible().await;
                let next = retry_tick.peek().wrapping_add(1);
                retry_tick.set(next);
            }
        }
    });

    use_effect(move || {
        // Effects run after DOM mutations. Subscribe to band changes as well as responses.
        let element = body.read().clone();
        let content = content.read();
        let _requested = *requested.read();
        let line_height_px = *line_height.read();
        let (Some(element), Some(Ok(snapshot))) = (element, content.as_ref()) else {
            return;
        };
        // Arming changes a subscribed input (content/requested/body/line_height); peek avoids looping on write-guard drop notifications, even for no-op writes.
        let current = *viewport.peek();
        if !current.placing()
            || current.window(line_height_px) != Some(snapshot.window)
            || !element.is_connected()
        {
            return;
        }
        let measured = MeasuredViewport {
            scroll_height: element.scroll_height(),
            client_height: element.client_height(),
        };
        let target = viewport
            .write()
            .restore_target(snapshot.window, measured, line_height_px);
        if let Some(top) = target {
            element.set_scroll_top(top as i32);
        }
    });

    let response = content.read();
    let is_loading = *loading.read();

    rsx! {
        div {
            class: "text-stream-log",
            role: "tabpanel",
            aria_label: "{display_name}",
            style: "--kymo-text-line-height: {line_height_px}px;",
            onmounted: move |event| {
                let Some(element) = event.data().downcast::<web_sys::Element>()
                    .and_then(|element| element.dyn_ref::<web_sys::HtmlElement>()).cloned()
                else { return; };
                resize_observer.set(Some(observe_height(&element, viewport)));
                body.set(Some(element));
            },
            onscroll: move |event: Event<ScrollData>| {
                let data = event.data();
                // A hide can reset scrollTop before its resize callback.
                if data.client_height() > 0 {
                    let measured = MeasuredViewport {
                        scroll_height: data.scroll_height(),
                        client_height: data.client_height(),
                    };
                    viewport.write().scrolled(data.scroll_top(), measured, line_height_px, live);
                }
            },
            if let Some(Err(message)) = &*response {
                div { class: "rect-empty", "{message}" }
            } else if let Some(Ok(value)) = &*response {
                if value.total_lines == 0 {
                    div { class: "rect-empty",
                        if search.is_empty() { "No logs yet" } else { "No matching lines" }
                    }
                } else {
                    {
                        let top_height = value.window.offset.saturating_mul(u64::from(line_height_px));
                        let rendered_end = value.window.offset.saturating_add(value.lines.len() as u64);
                        let bottom_height = value
                            .total_lines
                            .saturating_sub(rendered_end)
                            .saturating_mul(u64::from(line_height_px));
                        rsx! {
                            div { class: "text-stream-spacer", style: "height: {top_height}px;" }
                            for line in &value.lines {
                                {
                                    let color = hash_color(&line.metric_name);
                                    let source_label = metric_label(&source.metric_names, &line.metric_name);
                                    let tooltip = format_elapsed(line.step, value.first_step);
                                    let line_key = format!(
                                        "{}:{}:{}",
                                        line.metric_name, line.step, line.line_index
                                    );
                                    rsx! {
                                        div {
                                            key: "{line_key}",
                                            class: "text-stream-line",
                                            style: "background: {color}22;",
                                            title: "{tooltip}",
                                            if let Some(label) = source_label {
                                                span {
                                                    class: "text-stream-metric-label",
                                                    style: "color: {color};",
                                                    "[{label}]"
                                                }
                                            }
                                            "{line.text}"
                                        }
                                    }
                                }
                            }
                            div { class: "text-stream-spacer", style: "height: {bottom_height}px;" }
                        }
                    }
                }
            } else if is_loading {
                div { class: "rect-loading", "Loading logs..." }
            } else {
                div { class: "rect-loading", "Waiting to load logs..." }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        metric_label, stream_failure_is_permanent, text_run_labels, text_skip_query, RunStreams,
        WindowData,
    };
    use crate::components::text_stream::viewport::VirtualWindow;
    use crate::grpc::proto::RunInfo;
    use crate::state::visibility::is_terminal_run_status;

    fn streams(project_id: &str, run_id: &str) -> RunStreams {
        RunStreams {
            project_id: project_id.to_string(),
            run_id: run_id.to_string(),
            metric_names: vec!["logs/std_out".to_string()],
        }
    }

    fn display_run(project_id: &str, run_id: &str, run_name: &str, ordinal: u64) -> RunInfo {
        RunInfo {
            project_id: project_id.to_string(),
            run_id: run_id.to_string(),
            run_name: run_name.to_string(),
            ordinal,
            ..Default::default()
        }
    }

    #[test]
    fn metric_labels_appear_only_when_a_log_combines_streams() {
        let stdout = "logs/std_out".to_string();
        let stderr = "logs/std_err".to_string();

        assert_eq!(metric_label(std::slice::from_ref(&stdout), &stdout), None);
        assert_eq!(
            metric_label(&[stdout, stderr.clone()], &stderr),
            Some("logs/std_err")
        );
    }

    #[test]
    fn text_tab_labels_disambiguate_duplicate_run_names() {
        let runs = vec![streams("project-a", "run-a"), streams("project-a", "run-b")];
        let display_runs = vec![
            display_run("project-a", "run-a", "first", 1),
            display_run("project-a", "run-b", "second", 2),
        ];
        assert_eq!(
            text_run_labels(&runs, &display_runs),
            [
                ("first".to_owned(), "first".to_owned()),
                ("second".to_owned(), "second".to_owned()),
            ]
        );

        let runs = vec![
            streams("project-a", "run-a"),
            streams("project-a", "run-b"),
            streams("project-b", "run-c"),
            streams("project-b", "run-unknown"),
        ];
        let display_runs = vec![
            display_run("project-a", "run-a", "baseline", 1),
            display_run("project-a", "run-b", "baseline", 2),
            display_run("project-b", "run-c", "baseline", 1),
        ];
        let labels = text_run_labels(&runs, &display_runs);
        assert_eq!(
            labels,
            [
                ("baseline #1", "baseline #1 (project-a)"),
                ("baseline #2", "baseline #2 (project-a)"),
                ("baseline #1", "baseline #1 (project-b)"),
                ("run-unknown", "run-unknown (project-b)"),
            ]
            .map(|(text, full)| (text.to_owned(), full.to_owned()))
        );
        // Label in name: the accessible name starts with the visible text.
        for (text, full) in labels {
            assert!(full.starts_with(&text), "{full:?} lacks {text:?}");
        }
    }

    #[test]
    fn text_skip_needs_the_same_window_revision_and_a_covering_stamp() {
        let window = VirtualWindow {
            offset: 80,
            limit: 480,
        };
        let shown = WindowData {
            window,
            total_lines: 0,
            first_step: 0,
            lines: vec![],
            revision: 2,
            version: Some(5),
        };
        // Same window and revision with the run's knowledge at or below the stamp: the answered window serves.
        assert!(text_skip_query(Some(&shown), window, 2, Some(3)));
        // A version above the stamp refetches — that is how a live log follows new lines.
        assert!(!text_skip_query(Some(&shown), window, 2, Some(6)));
        // A new window, a search resubmit (a new revision), and nothing answered all query as before.
        assert!(!text_skip_query(
            Some(&shown),
            VirtualWindow {
                offset: 160,
                limit: 480,
            },
            2,
            Some(5)
        ));
        assert!(!text_skip_query(Some(&shown), window, 3, Some(5)));
        assert!(!text_skip_query(None, window, 2, Some(5)));
    }

    #[test]
    fn failures_choose_terminal_permanent_or_retry_behavior() {
        assert!(is_terminal_run_status(&tonic::Status::not_found("purged")));
        assert!(is_terminal_run_status(&tonic::Status::failed_precondition(
            "expired"
        )));
        assert!(!is_terminal_run_status(&tonic::Status::unavailable(
            "network"
        )));
        assert!(stream_failure_is_permanent(
            &tonic::Status::resource_exhausted("line too large")
        ));
        assert!(!stream_failure_is_permanent(&tonic::Status::unavailable(
            "network"
        )));
    }
}
