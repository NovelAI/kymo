use dioxus::prelude::*;

use crate::components::icons::{CloseIcon, CollapseAllIcon, ExpandAllIcon, GearIcon, ResetIcon};
use crate::components::theme_toggle::ThemeToggle;
use crate::route::Route;
use crate::state::{DashboardState, DirectRunLoad, PanelTarget, SectionConfig, UserConfigState};
use crate::util::{confirm, is_app_escape, primary, unique_id};

/// The Project settings button's id, which focus returns to when its panel closes.
const PROJECT_SETTINGS_TRIGGER_ID: &str = "kymo-project-settings-trigger";

#[component]
pub fn Navbar() -> Element {
    let state = use_context::<DashboardState>();
    let user_config = use_context::<UserConfigState>();
    let project_id = state.project_id.read().clone();
    let current_run = state.current_run.read().clone();
    // The breadcrumb carries the focused chart along (like the sidebar's run links), so the overlay follows dashboard navigation. Hook, so it must run unconditionally up here.
    let chart_focus = use_route::<Route>().chart_param();

    let mut panel_filter = state.panel_filter;
    let filter_value = panel_filter.read().clone();
    let sections_visible = user_config.sections_visible();

    // The collapse toggle acts on the sections the grid shows: it collapses them while any is open, else expands them.
    let needle = state.panel_needle();
    let mut shown_names = Vec::new();
    let mut any_open = false;
    if *state.grid_mounted.read() {
        for section in state.layout_config.read().iter().flat_map(|l| &l.sections) {
            if section.matches_filter(&needle) {
                any_open |= !section.is_collapsed(sections_visible);
                shown_names.push(section.name.clone());
            }
        }
    }
    let collapse_verb = if any_open { "Collapse" } else { "Expand" };
    let collapse_scope = if needle.is_empty() {
        "all sections"
    } else {
        "all listed sections"
    };
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
                        // Esc clears, matching the maximize/panel dismissal idiom. Consumed only while there's a filter to clear, so one Esc doesn't also close a maximized chart; on an empty filter it falls through untouched — Signal::set notifies subscribers even on equal values, so an unconditional clear would re-render them for nothing.
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
                class: "navbar-action icon-button",
                title: "{collapse_verb} {collapse_scope}",
                disabled: shown_names.is_empty(),
                onmousedown: primary(move |_| {
                    state.set_sections_collapsed(&shown_names, any_open, sections_visible);
                }),
                if any_open { CollapseAllIcon {} } else { ExpandAllIcon {} }
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
                    section.set_collapsed(false, sections_visible);
                    state.add_section(section);
                }),
                "+ Section"
            }
            button {
                id: PROJECT_SETTINGS_TRIGGER_ID,
                class: "navbar-action icon-button",
                title: "Project settings",
                aria_expanded: state.options_panel.read().as_ref().is_some_and(|p| p.target == PanelTarget::ProjectDefaults),
                onmousedown: primary(move |_| state.open_options_panel(PanelTarget::ProjectDefaults, PROJECT_SETTINGS_TRIGGER_ID.to_string())),
                GearIcon {}
            }
            button {
                class: "navbar-action icon-button",
                title: "Reset layout to auto-generated",
                onmousedown: primary(move |_| {
                    if confirm("Reset layout? This will discard all customizations and regenerate from discovered metrics.") {
                        state.reset_layout();
                        // A maximized chart may be one of the discarded customizations; its bindings would no longer be planned for name lookups, so it could never become ready.
                        crate::route::focus_chart(None);
                    }
                }),
                ResetIcon {}
            }
            ThemeToggle { class: "navbar-action" }
        }
    }
}
