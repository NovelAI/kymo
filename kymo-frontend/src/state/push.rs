use std::collections::{HashMap, HashSet};
use std::time::Duration;

use dioxus::prelude::*;
use gloo_timers::future::sleep;

use crate::grpc::proto::RunLifecycleState;
use crate::state::layout_config::{
    resolve_binding, MetricBinding, ProjectRef, RunRef, ViewContext,
};
use crate::state::visibility::retry_visible;
use crate::state::{DashboardState, DirectRunLoad, LayoutConfig};

fn refresh_direct_run(mut state: DashboardState) {
    if state.current_run.peek().is_some()
        || !matches!(&*state.direct_run.peek(), DirectRunLoad::Idle)
    {
        let next = state.direct_run_refresh.peek().wrapping_add(1);
        state.direct_run_refresh.set(next);
    }
}

/// Run ids for the version catch-up polls, grouped by project (the server answers for one project per call). Scopes come from `resolve_binding` itself, so what gets polled is exactly what charts can render — All-bound charts (every run regardless of selection) and cross-project Specific refs included. Any of these can hold a run that terminated while no connection existed and will never push again.
fn poll_scopes(state: &DashboardState) -> Vec<(String, Vec<String>)> {
    let current_project = state.project_id.peek().clone();
    let ctx = ViewContext {
        current_project: current_project.clone(),
        current_run: state.current_run.peek().clone(),
        selected_runs: state.selected_runs.peek().clone(),
        all_runs: state.runs.peek().iter().map(|r| r.run_id.clone()).collect(),
    };
    // The page's own runs poll even when no binding references them — they ride the normal resolution as the (Current, Selected) binding they are.
    let page = MetricBinding {
        project: ProjectRef::Current,
        runs: RunRef::Selected,
        metric_name: String::new(),
    };
    let layout = state.layout_config.peek();
    let bindings = layout
        .as_ref()
        .into_iter()
        .flat_map(|l| &l.sections)
        .flat_map(|s| &s.rects)
        .flat_map(|r| &r.bindings);
    let mut buckets: HashMap<String, Vec<String>> = HashMap::new();
    // Keep the current project in the version poll even when it has no active
    // runs, so an outcome-unknown Restore can make its first run discoverable.
    if !current_project.is_empty() {
        buckets.entry(current_project).or_default();
    }
    for b in std::iter::once(&page).chain(bindings) {
        for r in resolve_binding(b, &ctx) {
            buckets.entry(r.project_id).or_default().push(r.run_id);
        }
    }
    buckets
        .into_iter()
        .filter_map(|(p, mut ids)| {
            ids.sort_unstable();
            ids.dedup();
            (!p.is_empty()).then_some((p, ids))
        })
        .collect()
}

/// Fold a seed/delta into a version-map signal, notifying only when an entry
/// actually rises. The monotonic merge keeps a stale lower value racing an
/// authoritative poll from re-triggering chart fetches.
fn fold_versions(mut into: Signal<HashMap<String, u64>>, from: &HashMap<String, u64>) {
    let rises = {
        let cur = into.peek();
        from.iter()
            .any(|(key, value)| entry_rises(&cur, key, *value))
    };
    if rises {
        crate::grpc::merge_versions(&mut into.write(), from.iter().map(|(k, v)| (k.clone(), *v)));
    }
}

fn entry_rises(current: &HashMap<String, u64>, key: &str, value: u64) -> bool {
    current.get(key).is_none_or(|old| value > *old)
}

fn project_update_rises(
    current: &HashMap<String, u64>,
    update: &HashMap<String, u64>,
    project: &str,
) -> bool {
    update
        .get(project)
        .is_some_and(|version| entry_rises(current, project, *version))
}

fn merge_scoped_versions(
    into: &mut HashMap<String, u64>,
    from: &HashMap<String, u64>,
    scope: &HashSet<String>,
) -> bool {
    let before_len = into.len();
    into.retain(|project, _| scope.contains(project));
    let mut changed = into.len() != before_len;
    for (project, &version) in from {
        if scope.contains(project) && crate::grpc::raise(into, project.clone(), version) {
            changed = true;
        }
    }
    changed
}

#[derive(Debug, Default, PartialEq, Eq)]
struct ProjectVersionChanges {
    versions_changed: bool,
    refresh_list: bool,
    /// A live rise always refreshes GetRun: its snapshot is independent of the list.
    refresh_direct: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProjectObservation {
    Seed,
    Live,
}

fn merge_project_versions(
    into: &mut HashMap<String, u64>,
    from: &HashMap<String, u64>,
    scope: &HashSet<String>,
    current_project: &str,
    runs_project_version: Option<u64>,
    observation: ProjectObservation,
) -> ProjectVersionChanges {
    // Seeds may predate this page; the resync drives their fetches. All live observations, pushed or polled, use the same coverage rule.
    let current_project_rose = observation == ProjectObservation::Live
        && project_update_rises(into, from, current_project);
    let versions_changed = merge_scoped_versions(into, from, scope);
    // Only a token delivered with the loaded rows proves list coverage. An absent baseline still rises, including version zero; an old server without a token retains the conservative refresh.
    let list_covers_observation = from
        .get(current_project)
        .is_some_and(|observed| runs_project_version.is_some_and(|loaded| loaded >= *observed));
    ProjectVersionChanges {
        versions_changed,
        refresh_list: current_project_rose && !list_covers_observation,
        refresh_direct: current_project_rose,
    }
}

fn apply_project_versions(
    mut state: DashboardState,
    observed: &HashMap<String, u64>,
    observation: ProjectObservation,
) {
    let current_project = state.project_id.peek().clone();
    // Resolve live membership for both pushes and completed polls. A dirty Memo::peek is not authoritative, and an older ambient batch must not undo a newer layout edit.
    let scope = layout_project_scope(&current_project, state.layout_config.peek().as_ref());
    let mut versions = state.project_versions.peek().clone();
    let changes = merge_project_versions(
        &mut versions,
        observed,
        &scope,
        &current_project,
        *state.runs_project_version.peek(),
        observation,
    );
    if changes.versions_changed {
        // Publish once per batch so one project's result cannot cancel another project's explicit metadata lookups.
        state.project_versions.set(versions);
    }
    if changes.refresh_list {
        // Schedule coverage in this synchronous continuation, before an equal delayed observation can interleave.
        state.request_runs_refresh();
    }
    if changes.refresh_direct {
        refresh_direct_run(state);
    }
}

fn layout_project_scope(current_project: &str, layout: Option<&LayoutConfig>) -> HashSet<String> {
    let mut scope = HashSet::new();
    if !current_project.is_empty() {
        scope.insert(current_project.to_string());
    }
    for binding in layout
        .into_iter()
        .flat_map(|layout| &layout.sections)
        .flat_map(|section| &section.rects)
        .flat_map(|rect| &rect.bindings)
    {
        let project = match &binding.project {
            ProjectRef::Current => current_project,
            ProjectRef::Specific(project) => project,
        };
        if !project.is_empty() {
            scope.insert(project.to_string());
        }
    }
    scope
}

/// Invisible component folding server-pushed change events into dashboard
/// state — the only place push frames become signal writes. Per event:
/// run-version bumps merge into `run_versions`, project versions into
/// `project_versions`, registry events into `metrics_gen`, and a bump of the
/// CURRENT project's version refreshes the direct point view and the list when its snapshot does not already cover that version. Global-only reaper changes
/// refresh that point view only while it displays a deleted run. On resync
/// (per socket (re)connect — the first seeds the page's initial fetches — or
/// a server-flagged event loss) it advances `resync_gen` to the transport
/// generation and polls the authoritative versions for each project in the page's scope.
#[component]
pub fn PushBridge() -> Element {
    let state = use_context::<DashboardState>();

    // Key catch-up on project membership, rather than every layout edit, and the shared transport generation; resolve scope again when applying a batch.
    let resync_project_scope = use_memo(move || {
        let current_project = state.project_id.read();
        let layout = state.layout_config.read();
        layout_project_scope(&current_project, layout.as_ref())
    });

    let mut resync_gen = state.resync_gen;

    // Event pump.
    use_future(move || {
        let run_versions = state.run_versions;
        let metrics_gen = state.metrics_gen;
        async move {
            let mut subscription = crate::grpc::subscribe_push();
            loop {
                let update = subscription.next_visible().await;
                if let Some(generation) = update
                    .resync_gen
                    .filter(|generation| *generation > *resync_gen.peek())
                {
                    // List, direct-view and metric resources subscribe to resync; the resource below owns the version poll.
                    resync_gen.set(generation);
                }

                fold_versions(run_versions, &update.runs);
                if update.initial || !update.projects.is_empty() {
                    let observation = if update.initial {
                        ProjectObservation::Seed
                    } else {
                        ProjectObservation::Live
                    };
                    apply_project_versions(state, &update.projects, observation);
                }
                fold_versions(metrics_gen, &update.metrics_gen);

                let global_changed = !update.initial && update.global.is_some();
                if global_changed
                    && matches!(&*state.direct_run.peek(),
                        DirectRunLoad::Loaded(view)
                            if view.record.state() != RunLifecycleState::Active)
                {
                    // Reaper claim/finalize advance only the global version.
                    // Limit that broad invalidation to an open deleted-run
                    // point view; active routes use their project version.
                    // A project rise in the same frame may also request this refresh; Dioxus coalesces these synchronous invalidations before running the resource.
                    refresh_direct_run(state);
                }
            }
        }
    });

    // Resync version catch-up: one PollVersions per project. Split from the
    // pump because the first mount needs ListRuns to establish its run scope,
    // so it keys on (resync_gen, runs_loaded). On later reconnects it may run
    // alongside the resync-triggered ListRuns; an uncovered newer project version below forces one covering list refresh after the poll observation.
    let _resync = use_resource(move || {
        let grpc = state.grpc;
        let run_versions = state.run_versions;
        let n = *resync_gen.read();
        let runs_loaded = *state.runs_loaded.read();
        // A saved layout can become available after the first run-list fetch.
        // Include its project membership in the resource key so a newly bound
        // project does not remain version-unknown until the minute backstop. Scope shrink also reruns this resource: its scoped merge must prune removed projects without waiting for unrelated push traffic.
        let _ = resync_project_scope.read();
        async move {
            // The resource's subscriptions own restart/deduplication. A saved completed key would incorrectly skip A -> B -> A when B was canceled mid-poll.
            if n == 0 || !runs_loaded {
                return;
            }
            let grpc = grpc.read().clone();
            // The main catch-up for versions pushed while no connection
            // existed — a run that logged its last points during the gap
            // never pushes again — so retry until it lands (a newer resync
            // restarts the resource and takes over; the ambient poll below
            // is the slow backstop).
            let scopes = poll_scopes(&state);
            let mut caught_up_project_versions = HashMap::new();
            for (project, runs) in scopes {
                let resp = retry_visible("resync poll", async || {
                    grpc.poll_versions(Some(&project), &runs).await
                })
                .await;
                // Merge, don't replace: pushed entries for runs outside
                // this poll's scope must survive.
                fold_versions(run_versions, &resp.run_versions);
                caught_up_project_versions.insert(project, resp.project_version);
            }
            apply_project_versions(state, &caught_up_project_versions, ProjectObservation::Live);
        }
    });

    // Ambient backstop, deliberately cheap: once a minute (visible tabs
    // only), poll the version COUNTERS for every run the page renders — a
    // few integers, never chart data. Anything a push or resync missed
    // (scope gaps, races, lost frames) becomes at most a minute stale
    // instead of stale-until-reload.
    use_future(move || {
        let run_versions = state.run_versions;
        async move {
            loop {
                sleep(Duration::from_secs(60)).await;
                crate::grpc::wait_until_page_visible().await;
                let grpc = state.grpc.read().clone();
                let scopes = poll_scopes(&state);
                let mut observed_project_versions = HashMap::new();
                for (project, runs) in scopes {
                    if let Ok(resp) = grpc.poll_versions(Some(&project), &runs).await {
                        fold_versions(run_versions, &resp.run_versions);
                        observed_project_versions.insert(project.clone(), resp.project_version);
                    }
                }
                apply_project_versions(state, &observed_project_versions, ProjectObservation::Live);
            }
        }
    });

    rsx! {}
}

#[cfg(test)]
mod tests {
    use super::{
        entry_rises, layout_project_scope, merge_project_versions, merge_scoped_versions,
        ProjectObservation, ProjectVersionChanges,
    };
    use crate::state::layout_config::{
        DisplayType, LayoutConfig, MetricBinding, ProjectRef, RectConfig, RectOptions, RunRef,
        SectionConfig,
    };
    use std::collections::{HashMap, HashSet};

    #[test]
    fn resync_project_scope_tracks_layout_projects_without_run_resolution() {
        let layout = LayoutConfig {
            sections: vec![SectionConfig::auto(
                "section".to_string(),
                vec![RectConfig {
                    id: "rect".to_string(),
                    label: String::new(),
                    bindings: vec![
                        MetricBinding {
                            project: ProjectRef::Current,
                            runs: RunRef::Selected,
                            metric_name: "loss".to_string(),
                        },
                        MetricBinding {
                            project: ProjectRef::Specific("other".to_string()),
                            runs: RunRef::Specific(vec!["run".to_string()]),
                            metric_name: "loss".to_string(),
                        },
                    ],
                    display_type: DisplayType::Numeric,
                    options: RectOptions::default(),
                }],
            )],
        };

        assert_eq!(
            layout_project_scope("current", Some(&layout)),
            HashSet::from(["current".to_string(), "other".to_string()])
        );
        assert_eq!(
            layout_project_scope("current", None),
            HashSet::from(["current".to_string()])
        );
    }

    #[test]
    fn scoped_project_versions_drop_unbound_and_never_regress() {
        let mut versions = HashMap::from([("bound".to_string(), 7), ("stale".to_string(), 99)]);
        let pushed = HashMap::from([("bound".to_string(), 6), ("unrelated".to_string(), 12)]);
        let scope = HashSet::from(["bound".to_string(), "new".to_string()]);

        assert!(merge_scoped_versions(&mut versions, &pushed, &scope));
        assert_eq!(versions, HashMap::from([("bound".to_string(), 7)]));

        let pushed = HashMap::from([("bound".to_string(), 8), ("new".to_string(), 3)]);
        assert!(merge_scoped_versions(&mut versions, &pushed, &scope));
        assert_eq!(
            versions,
            HashMap::from([("bound".to_string(), 8), ("new".to_string(), 3)])
        );
        assert!(!merge_scoped_versions(&mut versions, &pushed, &scope));

        let mut versions = HashMap::new();
        let pushed = HashMap::from([("new".to_string(), 0)]);
        assert!(merge_scoped_versions(&mut versions, &pushed, &scope));
        assert_eq!(versions, pushed);
    }

    #[test]
    fn pushed_project_version_must_beat_a_newer_polled_value() {
        let polled = HashMap::from([("project".to_string(), 12)]);
        assert!(!entry_rises(&polled, "project", 11));
        assert!(!entry_rises(&polled, "project", 12));
        assert!(entry_rises(&polled, "project", 13));
    }

    #[test]
    fn snapshot_coverage_boundaries_have_explicit_refresh_decisions() {
        let scope = HashSet::from(["project".to_string()]);
        // Literal outcomes pin absent, equal, lower and newer coverage boundaries. Pushes and polls both use this live-observation decision.
        for (baseline, covered, observed, expected) in [
            (None, None, 0, (true, true, true)),
            (None, Some(0), 0, (true, false, true)),
            (Some(0), None, 0, (false, false, false)),
            (Some(0), Some(0), 13, (true, true, true)),
            (None, Some(12), 13, (true, true, true)),
            (Some(11), Some(12), 13, (true, true, true)),
            (Some(12), Some(12), 13, (true, true, true)),
            (Some(12), Some(13), 13, (true, false, true)),
            (None, Some(14), 13, (true, false, true)),
            (Some(13), Some(12), 13, (false, false, false)),
            (Some(14), None, 13, (false, false, false)),
        ] {
            let mut versions = baseline
                .map(|value| HashMap::from([("project".to_string(), value)]))
                .unwrap_or_default();
            let observation = HashMap::from([("project".to_string(), observed)]);
            let changes = merge_project_versions(
                &mut versions,
                &observation,
                &scope,
                "project",
                covered,
                ProjectObservation::Live,
            );
            assert_eq!(
                (
                    changes.versions_changed,
                    changes.refresh_list,
                    changes.refresh_direct
                ),
                expected,
                "baseline={baseline:?}, covered={covered:?}, observed={observed:?}",
            );
            assert_eq!(
                versions,
                HashMap::from([("project".to_string(), baseline.unwrap_or(0).max(observed))]),
                "baseline={baseline:?}, covered={covered:?}, observed={observed:?}",
            );
        }
    }

    #[test]
    fn seed_merges_versions_without_competing_with_resync_fetches() {
        let scope = HashSet::from(["project".to_string()]);
        for version in [0, 13] {
            let mut versions = HashMap::new();
            let seed = HashMap::from([("project".to_string(), version)]);
            assert_eq!(
                merge_project_versions(
                    &mut versions,
                    &seed,
                    &scope,
                    "project",
                    None,
                    ProjectObservation::Seed,
                ),
                ProjectVersionChanges {
                    versions_changed: true,
                    refresh_list: false,
                    refresh_direct: false,
                },
            );
            assert_eq!(versions, seed);
        }
    }

    #[test]
    fn scoped_observations_prune_without_refresh_and_ignore_stale_batches() {
        let mut versions =
            HashMap::from([("current".to_string(), 13), ("removed".to_string(), 99)]);
        let scope = layout_project_scope("current", None);
        let polled = HashMap::from([("current".to_string(), 13)]);
        // A covered, equal observation still applies scope pruning and publishes the changed map, without requesting either read.
        assert_eq!(
            merge_project_versions(
                &mut versions,
                &polled,
                &scope,
                "current",
                Some(13),
                ProjectObservation::Live,
            ),
            ProjectVersionChanges {
                versions_changed: true,
                ..ProjectVersionChanges::default()
            },
        );
        assert_eq!(versions, polled);

        versions.insert("new".to_string(), 7);
        let stale_batch =
            HashMap::from([("current".to_string(), 12), ("removed".to_string(), 100)]);
        let scope = HashSet::from(["current".to_string(), "new".to_string()]);
        // Lower and out-of-scope observations are a complete no-op, including preservation of an in-scope entry absent from this batch.
        assert_eq!(
            merge_project_versions(
                &mut versions,
                &stale_batch,
                &scope,
                "current",
                Some(13),
                ProjectObservation::Live,
            ),
            ProjectVersionChanges::default(),
        );
        assert_eq!(
            versions,
            HashMap::from([("current".to_string(), 13), ("new".to_string(), 7)]),
        );
    }
}
