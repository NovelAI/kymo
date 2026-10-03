use dioxus::prelude::*;

use crate::components::metric_grid::MetricGrid;
use crate::route::ChartQuery;

#[component]
pub fn ProjectPage(project_id: String, chart: ChartQuery) -> Element {
    // `chart` is consumed by DashboardLayout's URL→overlay sync, not here; it's a prop only because route fields are.
    let _ = chart;

    rsx! {
        MetricGrid {}
    }
}
