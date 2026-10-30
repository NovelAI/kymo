use crate::grpc::proto::MetricInfo;
use crate::util::natural_cmp;

/// The section a metric auto-generates into: everything before the name's last `/`, or the unnamed catch-all (empty name) when it has none.
pub fn section_of_metric(metric_name: &str) -> &str {
    metric_name
        .rsplit_once('/')
        .map_or("", |(section, _)| section)
}

/// Group metrics by [`section_of_metric`]. Within each section,
/// metrics are natural-sorted by name (so `layer2` precedes `layer10`), which
/// becomes the chart order. Section order is NOT set here: the sole caller
/// `auto_generate` always follows with `sort_sections`, which owns it.
pub fn group_by_prefix(metrics: &[MetricInfo]) -> Vec<(String, Vec<MetricInfo>)> {
    let mut groups: Vec<(String, Vec<MetricInfo>)> = Vec::new();

    for m in metrics {
        let section = section_of_metric(&m.metric_name);

        if let Some(group) = groups.iter_mut().find(|(name, _)| name == section) {
            group.1.push(m.clone());
        } else {
            groups.push((section.to_string(), vec![m.clone()]));
        }
    }

    for (_, section_metrics) in &mut groups {
        section_metrics.sort_by(|a, b| natural_cmp(&a.metric_name, &b.metric_name));
    }
    groups
}
