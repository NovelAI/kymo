use dioxus::prelude::*;
use dioxus::web::WebEventExt;

use crate::components::section::Section;
use crate::components::section_drag::SectionDrag;
use crate::components::section_editor::SectionEditor;
use crate::state::{DashboardState, RectConfig, SectionConfig};
use crate::util::editor_trigger_id;

#[component]
pub fn MetricGrid() -> Element {
    let mut state = use_context::<DashboardState>();
    let drag = SectionDrag::provide();
    let mut list = use_signal(|| None::<web_sys::Element>);
    let layout = state.layout_config.read().clone();
    let needle = state.panel_needle();
    use_effect(move || state.grid_mounted.set(true));
    use_drop(move || state.grid_mounted.set(false));

    // Close the section dialog if its section drops out of a regenerated
    // layout mid-edit — better than silently editing a ghost. (Effect, not
    // render-side: clearing a signal during render is a side effect.)
    use_effect(move || {
        let editing = state.editing_section.read().clone();
        if let Some(name) = editing {
            let gone = state
                .layout_config
                .read()
                .as_ref()
                .is_none_or(|l| l.find_section(&name).is_none());
            if gone {
                let mut sig = state.editing_section;
                sig.set(None);
            }
        }
    });

    // Discovery is scoped to the shown runs, so with none shown on the
    // project page an empty layout means "show a run", not "this project
    // has no charts". A run page always has its run.
    let no_runs_visible =
        state.current_run.read().is_none() && state.selected_runs.read().is_empty();

    match layout {
        None => rsx! {
            div { class: "empty-state", "Loading metrics..." }
        },
        Some(config) => {
            let any_visible = config.sections.iter().any(|s| s.matches_filter(&needle));
            rsx! {
            div {
                class: "metric-sections",
                onmounted: move |event: MountedEvent| list.set(event.data().try_as_web_event()),
                ondragenter: move |event: DragEvent| drag.over(&event, list.peek().as_ref()),
                ondragover: move |event: DragEvent| drag.over(&event, list.peek().as_ref()),
                ondragleave: move |event: DragEvent| drag.leave(&event, list.peek().as_ref()),
                ondrop: move |event: DragEvent| drag.drop(state, &event),
                if config.sections.is_empty() {
                    if no_runs_visible {
                        div { class: "empty-state", "Show a run in the sidebar to see its charts." }
                    } else {
                        div { class: "empty-state", "No sections. Add one to get started." }
                    }
                } else if !any_visible {
                    div { class: "empty-state", "No panels match the filter." }
                }
                for section in config.sections.iter().filter(|s| s.matches_filter(&needle)) {
                    {
                        // Handlers record intent against the section's
                        // immutable `name`; the diff store is updated per
                        // touched element, never recomputed wholesale.
                        let name_for_add_rect = section.name.clone();
                        let name_for_delete = section.name.clone();
                        rsx! {
                            Section {
                                key: "{section.name}",
                                config: section.clone(),
                                filter: needle.clone(),
                                on_update_settings: move |new_section: SectionConfig| {
                                    state.update_section_settings(new_section);
                                },
                                on_update_rect: move |new_rect: RectConfig| {
                                    state.update_rect(new_rect);
                                },
                                on_add_rect: move |rect: RectConfig| {
                                    state.add_rect(&name_for_add_rect, rect);
                                },
                                on_delete_rect: move |id: String| {
                                    state.delete_rect(&id);
                                },
                                on_delete: move |_| {
                                    state.delete_section(&name_for_delete);
                                },
                            }
                        }
                    }
                }

                if let Some(editing_name) = state.editing_section.read().clone() {
                    if let Some(section) = config.find_section(&editing_name) {
                        SectionEditor {
                            return_focus_id: editor_trigger_id("section", &editing_name),
                            config: section.clone(),
                            chart_anchor: state.project_level_options(),
                            chart_current: state.section_level_options(&editing_name),
                            on_change: move |s: SectionConfig| {
                                state.update_section_settings(s);
                            },
                            on_close: move |_| {
                                let mut sig = state.editing_section;
                                sig.set(None);
                            },
                        }
                    }
                }
            }
            }
        }
    }
}
