use dioxus::prelude::*;

use crate::components::icons::{CloseIcon, GearIcon, ResetIcon};
use crate::components::options_editor::ProjectDefaultsEditor;
use crate::components::theme_toggle::ThemeToggle;
use crate::route::Route;
use crate::state::{DashboardState, DirectRunLoad, SectionConfig, UserConfigState};
use crate::util::{editor_trigger_id, is_app_escape, primary, unique_id};

#[component]
pub fn Navbar() -> Element {
    let state = use_context::<DashboardState>();
    let user_config = use_context::<UserConfigState>();
    let project_id = state.project_id.read().clone();
    let current_run = state.current_run.read().clone();
    // The breadcrumb carries the focused chart along (like the sidebar's run links), so the overlay follows dashboard navigation. Hook, so it must run unconditionally up here.
    let chart_focus = use_route::<Route>().chart_param();
    let mut editing_defaults = use_signal(|| false);
    let defaults_trigger_id = editor_trigger_id("project-defaults", "");

    let mut panel_filter = state.panel_filter;
    let filter_value = panel_filter.read().clone();
    rsx! {
        nav { class: "navbar",
            Link { to: Route::ProjectsPage {}, class: "navbar-brand", "kymo" }
            span { class: "navbar-sep", "/" }
            if current_run.is_some() {
                Link {
                    to: Route::ProjectPage {
                        project_id: project_id.clone(),
                        chart: chart_focus.clone().into(),
                    },
                    class: "navbar-current",
                    title: "{project_id}",
                    // The link paints a focus ring, so the fade clips an inner span.
                    span { class: "fade-overflow", span { "{project_id}" } }
                }
            } else {
                span {
                    class: "navbar-current fade-overflow",
                    title: "{project_id}",
                    span { "{project_id}" }
                }
            }
            if let Some(run_id) = current_run {
                {
                    let runs = state.runs.read();
                    let run_name = runs
                        .iter()
                        .find(|r| r.run_id == run_id)
                        .map(|r| r.run_name.clone())
                        .or_else(|| match &*state.direct_run.read() {
                            DirectRunLoad::Loaded(view) => view
                                .record
                                .run
                                .as_ref()
                                .filter(|run| {
                                    run.project_id == project_id && run.run_id == run_id
                                })
                                .map(|run| run.run_name.clone()),
                            _ => None,
                        })
                        .unwrap_or_else(|| run_id.clone());
                    rsx! {
                        span { class: "navbar-sep", "/" }
                        span {
                            class: "navbar-current fade-overflow",
                            title: "{run_name}",
                            span { "{run_name}" }
                        }
                    }
                }
            }
            div { class: "navbar-spacer" }
            // Panel filter; narrows the grid live. See `DashboardState::panel_filter` for scope and persistence.
            div { class: "navbar-search",
                input {
                    class: "navbar-search-input",
                    r#type: "text",
                    placeholder: "Filter panels",
                    title: "Show only panels whose name contains this text (case-insensitive)",
                    value: "{filter_value}",
                    oninput: move |e: Event<FormData>| panel_filter.set(e.value()),
                    onkeydown: move |e: Event<KeyboardData>| {
                        // Esc clears, matching the maximize/dialog dismissal idiom. Consumed only while there's a filter to clear, so one Esc doesn't also close a maximized chart; on an empty filter it falls through untouched — Signal::set notifies subscribers even on equal values, so an unconditional clear would re-render them for nothing.
                        if is_app_escape(&e) && !panel_filter.peek().is_empty() {
                            e.prevent_default();
                            panel_filter.set(String::new());
                        }
                    },
                }
                if !filter_value.is_empty() {
                    button {
                        class: "navbar-search-clear icon-button",
                        title: "Clear filter",
                        onmousedown: primary(move |_| panel_filter.set(String::new())),
                        CloseIcon {}
                    }
                }
            }
            button {
                class: "navbar-action",
                title: "Add a section",
                onmousedown: primary(move |_| {
                    // An active filter would hide the new empty section; clear it first. Guarded like the Esc handler above — set on an already-empty filter still notifies subscribers.
                    if !panel_filter.peek().is_empty() {
                        panel_filter.set(String::new());
                    }
                    // Generated id, not a count: a count-based id could
                    // collide with an earlier added section in the saved
                    // diff once the base regenerates.
                    let name = unique_id("section");
                    let mut section = SectionConfig::auto(name, Vec::new());
                    section.display_name = "New Section".to_string();
                    section.set_collapsed(false, user_config.current().sections_visible);
                    state.add_section(section);
                }),
                "+ Section"
            }
            button {
                id: "{defaults_trigger_id}",
                class: "navbar-action icon-button",
                title: "Project chart defaults (inherited by every chart)",
                onmousedown: primary(move |_| editing_defaults.set(true)),
                GearIcon {}
            }
            button {
                class: "navbar-action icon-button",
                title: "Reset layout to auto-generated",
                onmousedown: primary(move |_| {
                    if let Some(window) = web_sys::window() {
                        if window.confirm_with_message("Reset layout? This will discard all customizations and regenerate from discovered metrics.").unwrap_or(false) {
                            state.reset_layout();
                        }
                    }
                }),
                ResetIcon {}
            }
            ThemeToggle { class: "navbar-action" }
        }
        if *editing_defaults.read() {
            ProjectDefaultsEditor {
                return_focus_id: defaults_trigger_id.clone(),
                on_close: move |_| editing_defaults.set(false),
            }
        }
    }
}
