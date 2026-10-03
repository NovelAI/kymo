use dioxus::prelude::*;

use crate::components::metric_grid::MetricGrid;
use crate::grpc::proto::RunLifecycleState;
use crate::route::{ChartQuery, Route};
use crate::state::trash::{clock_wait_ms, compact_duration, effective_lifecycle};
use crate::state::{DashboardState, DirectRunLoad};
use crate::util::primary;

#[component]
pub fn RunPage(project_id: String, run_id: String, chart: ChartQuery) -> Element {
    let mut state = use_context::<DashboardState>();
    // `chart` is consumed by DashboardLayout's URL→overlay sync, not here;
    // it is a prop only because route fields are.
    let _ = chart;

    // Re-render at the authoritative expiry boundary (and once a minute for
    // the relative label). This is a display clock only: the server remains
    // the admission authority for Restore and data reads.
    let mut clock_tick = use_signal(|| 0u64);
    let _clock = use_resource(move || {
        let tick = *clock_tick.read();
        let direct_run = state.direct_run.read();
        let wait_ms = match &*direct_run {
            DirectRunLoad::Loaded(view) => {
                clock_wait_ms(view.authoritative_now_ms(), view.record.purge_at_ms, true)
            }
            _ => clock_wait_ms(0, None, false),
        };
        drop(direct_run);
        async move {
            gloo_timers::future::sleep(std::time::Duration::from_millis(wait_ms)).await;
            clock_tick.set(tick.wrapping_add(1));
        }
    });
    let _ = *clock_tick.read();

    // A focused chart lives outside this page's subtree. Close that sibling
    // overlay when the read deadline arrives, otherwise it could outlive the
    // MetricGrid that the terminal shell unmounts.
    use_effect(use_reactive(
        (&project_id, &run_id),
        move |(terminal_project_id, terminal_run_id)| {
            let _ = *clock_tick.read();
            let terminal = match &*state.direct_run.read() {
                DirectRunLoad::Loaded(view)
                    if view.matches(&terminal_project_id, &terminal_run_id) =>
                {
                    matches!(
                        effective_lifecycle(&view.record, view.authoritative_now_ms()),
                        RunLifecycleState::Expired | RunLifecycleState::Purging
                    )
                }
                DirectRunLoad::NotFound => true,
                _ => false,
            };
            if terminal && state.maximized.peek().is_some() {
                crate::route::focus_chart(None);
            }
        },
    ));

    let load = match state.direct_run.read().clone() {
        DirectRunLoad::Loaded(view) if !view.matches(&project_id, &run_id) => {
            DirectRunLoad::Loading
        }
        load => load,
    };
    match load {
        DirectRunLoad::Idle | DirectRunLoad::Loading => rsx! {
            div { class: "empty-state", "Loading run…" }
        },
        DirectRunLoad::NotFound => rsx! {
            div { class: "empty-state", role: "alert", "Run not found" }
        },
        DirectRunLoad::Error(message) => rsx! {
            div { class: "run-load-error", role: "alert",
                span { "Couldn’t load this run: {message}" }
                button {
                    class: "btn btn-ghost",
                    onmousedown: primary(move |_| {
                        let next = state.direct_run_refresh.peek().wrapping_add(1);
                        state.direct_run_refresh.set(next);
                    }),
                    "Retry"
                }
            }
        },
        DirectRunLoad::Loaded(view) => {
            let now_ms = view.authoritative_now_ms();
            let lifecycle = effective_lifecycle(&view.record, now_ms);
            match lifecycle {
                RunLifecycleState::Active => rsx! { MetricGrid {} },
                RunLifecycleState::Trashed => {
                    let remaining = view
                        .record
                        .purge_at_ms
                        .map(|purge_at| compact_duration(purge_at.saturating_sub(now_ms)))
                        .unwrap_or_else(|| "less than 7 days".to_string());
                    rsx! {
                        aside {
                            class: "trashed-run-notice",
                            role: "status",
                            aria_live: "polite",
                            aria_label: "Run is in Trash",
                            strong { "In Trash" }
                            span { "View only · {remaining} before permanent deletion" }
                            Link { to: Route::TrashPage {}, "View in Trash" }
                        }
                        MetricGrid {}
                    }
                }
                RunLifecycleState::Expired | RunLifecycleState::Purging => rsx! {
                    aside {
                        class: "trashed-run-notice",
                        role: "status",
                        strong { "Recovery expired — deleting…" }
                        Link { to: Route::TrashPage {}, "View Trash" }
                    }
                    div { class: "empty-state", "This run can no longer be viewed or restored." }
                },
                RunLifecycleState::Unknown => rsx! {
                    div { class: "empty-state", "Run state unavailable" }
                },
            }
        }
    }
}
