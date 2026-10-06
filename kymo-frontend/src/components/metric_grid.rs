use dioxus::prelude::*;
use dioxus::web::WebEventExt;

use crate::components::section::Section;
use crate::components::section_drag::SectionDrag;
use crate::state::DashboardState;

#[component]
pub fn MetricGrid() -> Element {
    let mut state = use_context::<DashboardState>();
    let drag = SectionDrag::provide();
    let mut list = use_signal(|| None::<web_sys::Element>);
    let layout = state.layout_config.read().clone();
    let needle = state.panel_needle();
    use_effect(move || state.grid_mounted.set(true));
    use_drop(move || state.grid_mounted.set(false));

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
                    Section {
                        key: "{section.name}",
                        config: section.clone(),
                        filter: needle.clone(),
                    }
                }
            }
            }
        }
    }
}
