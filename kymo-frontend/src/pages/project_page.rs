use dioxus::prelude::*;

use crate::components::metric_grid::MetricGrid;
use crate::route::ChartQuery;
use crate::state::DashboardState;

#[component]
pub fn ProjectPage(project_id: String, chart: ChartQuery) -> Element {
    let mut state = use_context::<DashboardState>();
    // `chart` is consumed by DashboardLayout's URL→overlay sync, not here; it's a prop only because route fields are.
    let _ = chart;

    // Mark that we're in project view (no specific run)
    use_effect(move || {
        state.current_run.set(None);
    });

    rsx! {
        MetricGrid {}
    }
}
