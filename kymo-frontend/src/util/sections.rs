use crate::grpc::proto::MetricInfo;
use crate::util::natural_cmp;

/// Group metrics by their prefix (everything before the last `/`); metrics with
/// no `/` go into the unnamed catch-all (empty name). Within each section,
/// metrics are natural-sorted by name (so `layer2` precedes `layer10`), which
/// becomes the chart order. Section order is NOT set here: the sole caller
/// `auto_generate` always follows with `sort_sections`, which owns it.
pub fn group_by_prefix(metrics: &[MetricInfo]) -> Vec<(String, Vec<MetricInfo>)> {
    let mut groups: Vec<(String, Vec<MetricInfo>)> = Vec::new();

    for m in metrics {
        let section = match m.metric_name.rfind('/') {
            Some(pos) => m.metric_name[..pos].to_string(),
            None => String::new(),
        };

        if let Some(group) = groups.iter_mut().find(|(name, _)| name == &section) {
            group.1.push(m.clone());
        } else {
            groups.push((section, vec![m.clone()]));
        }
    }

    for (_, section_metrics) in &mut groups {
        section_metrics.sort_by(|a, b| natural_cmp(&a.metric_name, &b.metric_name));
    }
    groups
}
