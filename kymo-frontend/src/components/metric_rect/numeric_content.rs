//! Numeric chart content and its fetch/render state.

use std::rc::Rc;

use dioxus::prelude::*;

use super::{
    build_view_context, evict_panel_caches, next_data_seq, use_bound_run_ids, CHART_CACHE,
};
use crate::components::uplot_chart::UPlotChart;
use crate::grpc::chart_delta::DenseChart;
use crate::grpc::proto::{ChartRequest, SeriesRef, SmoothingConfig};
use crate::state::chart_sync::{self, ChartCacheEntry};
use crate::state::layout_config::{MetricBinding, RectOptions};
use crate::state::visibility::{self, Zone};
use crate::state::{resolve_capped_bindings, DashboardState};

/// What one run of the chart resource produced. `use_resource` keeps the
/// stale value across restarts, so the render must be able to tell whether
/// that stale value is knowledge ("the last answer was empty" — a refetch
/// doesn't un-know it, keep saying "No data") or a no-query sentinel
/// ("nothing is known, keep saying Loading..." / "render from cache").
enum ChartFetch {
    /// No query ran: the rect's width wasn't measured yet, or the run list hasn't loaded.
    Unmeasured,
    /// No query ran: the rect is outside the prefetch band (Zone::Far).
    /// The render falls back to the fetch cache, so a chart scrolled far
    /// away and back re-renders without a refetch.
    Deferred,
    /// A real answer paired with the exact request semantics it was built
    /// under. None when no runs resolved; transient query errors retry instead
    /// of settling.
    Answer(Option<ChartAnswer>),
    /// A terminal lifecycle or request error. Stale panel data is evicted and
    /// the preserved explanation stays visible instead of retrying forever.
    Unavailable(String),
}

#[derive(Clone)]
struct ChartAnswer {
    data_seq: u64,
    response: Rc<DenseChart>,
    request: Rc<ChartRequest>,
}

fn settled_failure(status: tonic::Status) -> ChartFetch {
    let message = if visibility::is_terminal_run_status(&status) {
        "Run no longer available".to_string()
    } else {
        format!("Chart unavailable: {}", status.message())
    };
    ChartFetch::Unavailable(message)
}

/// The x-axis semantics a chart renders under: scale kind (step / time / custom metric), wall vs relative, and log bucketing. Derived from the REQUEST a response answered ([`ShownAxis::of_request`]) so the render always describes the response being painted, or from live options for the fresh-panel fallback ([`ShownAxis::of_options`]) — the two agree whenever the cached request came from the same options.
#[derive(Clone, Debug, PartialEq)]
struct ShownAxis {
    log_x: bool,
    time: bool,
    wall: bool,
    /// Custom-x metric name; empty = none.
    x_metric: String,
}

impl ShownAxis {
    fn of_request(r: &ChartRequest) -> Self {
        let time = r.use_timestamp_axis;
        ShownAxis {
            log_x: r.log_buckets,
            time,
            wall: time && !r.relative_time,
            x_metric: if time {
                String::new()
            } else {
                r.x_series
                    .as_ref()
                    .map(|s| s.metric_name.clone())
                    .unwrap_or_default()
            },
        }
    }

    fn of_options(log_x: bool, options: &RectOptions) -> Self {
        use crate::state::layout_config::XAxisMode;
        let time = matches!(
            options.x_axis_mode,
            XAxisMode::RelativeTime | XAxisMode::WallTime
        );
        ShownAxis {
            log_x,
            time,
            wall: matches!(options.x_axis_mode, XAxisMode::WallTime),
            x_metric: if time {
                String::new()
            } else {
                options.x_axis_metric.clone()
            },
        }
    }

    /// Step-based x: zoom refetches through the step store, and the log ladder may shift.
    fn step_axis(&self) -> bool {
        !self.time && self.x_metric.is_empty()
    }

    /// Axis label — also the cursor-sync group key. The custom-x case keeps
    /// the full metric path: the tail alone made train/epoch and eval/epoch
    /// sync as the same axis.
    fn x_label(&self) -> String {
        if self.time {
            "time".to_string()
        } else if !self.x_metric.is_empty() {
            self.x_metric.clone()
        } else {
            "step".to_string()
        }
    }
}

fn request_proves_noncontributors(request: &ChartRequest) -> bool {
    request.x_series.is_none()
        && request.step_min.is_none()
        && request.step_max.is_none()
        && !request.log_buckets
}

fn noncontributors_from_response(
    request: &ChartRequest,
    response: &DenseChart,
) -> std::collections::HashSet<String> {
    if !request_proves_noncontributors(request) {
        return Default::default();
    }
    let contributing: std::collections::HashSet<&str> = response
        .series
        .iter()
        .map(|series| series.run_id.as_str())
        .collect();
    request
        .y_series
        .iter()
        .map(|series| &series.run_id)
        .filter(|run_id| !contributing.contains(run_id.as_str()))
        .cloned()
        .collect()
}

fn options_allow_noncontributors(options: &RectOptions, has_step_zoom: bool) -> bool {
    use crate::state::layout_config::XAxisMode;

    let has_custom_x =
        matches!(options.x_axis_mode, XAxisMode::Step) && !options.x_axis_metric.is_empty();
    let has_step_bounds = options.is_step_axis() && has_step_zoom;
    !has_custom_x && !has_step_bounds && !options.log_x
}

#[cfg(test)]
mod shown_axis_tests {
    use super::super::{
        every_source_terminal, needs_metric_type_check, sole_detected_display_type,
    };
    use super::{
        noncontributors_from_response, options_allow_noncontributors,
        request_proves_noncontributors, ShownAxis,
    };
    use crate::grpc::chart_delta::DenseChart;
    use crate::grpc::proto::{ChartRequest, SeriesRef};
    use crate::state::layout_config::{MetricBinding, ProjectRef, RectOptions, RunRef, XAxisMode};
    use crate::state::DisplayType;

    fn binding(runs: RunRef, metric_name: &str) -> MetricBinding {
        MetricBinding {
            project: ProjectRef::Current,
            runs,
            metric_name: metric_name.to_string(),
        }
    }

    #[test]
    fn retargeted_and_specific_bindings_detect_type() {
        assert!(!needs_metric_type_check(
            "loss",
            &[binding(RunRef::Selected, "loss")]
        ));
        assert!(!needs_metric_type_check(
            "loss",
            &[binding(RunRef::Specific(vec![]), "loss")]
        ));
        assert!(!needs_metric_type_check(
            "new-panel-id",
            &[binding(RunRef::Selected, "")]
        ));
        assert!(needs_metric_type_check(
            "loss",
            &[binding(RunRef::Selected, "logs/std_out")]
        ));
        assert!(needs_metric_type_check(
            "new-panel-id",
            &[binding(RunRef::All, "images/sample")]
        ));
        assert!(needs_metric_type_check(
            "loss",
            &[binding(RunRef::Specific(vec!["run".to_string()]), "loss",)]
        ));
        assert!(needs_metric_type_check(
            "loss",
            &[
                binding(RunRef::Selected, "loss"),
                binding(RunRef::Selected, "accuracy"),
            ]
        ));
    }

    #[test]
    fn type_detection_settles_unavailable_only_when_every_source_is_terminal() {
        assert!(!every_source_terminal(0, 0));
        assert!(!every_source_terminal(2, 1));
        assert!(every_source_terminal(2, 2));
    }

    #[test]
    fn editor_type_requires_one_unambiguous_detected_family() {
        assert_eq!(
            sole_detected_display_type((false, true, false)),
            Some(DisplayType::Cdn),
        );
        assert_eq!(sole_detected_display_type((true, true, false)), None);
        assert_eq!(sole_detected_display_type((false, false, false)), None);
    }

    /// The fallback and the request-derived semantics agree whenever the cached request came from the same options, so a fresh panel can never disagree with its first settled answer.
    #[test]
    fn options_and_their_request_derive_the_same_axis() {
        for (mode, metric, log_x) in [
            (XAxisMode::Step, "", false),
            (XAxisMode::Step, "", true),
            (XAxisMode::Step, "train/epoch", true),
            (XAxisMode::RelativeTime, "", true),
            (XAxisMode::RelativeTime, "train/epoch", true),
            (XAxisMode::WallTime, "", false),
            (XAxisMode::WallTime, "train/epoch", false),
        ] {
            let use_ts = matches!(mode, XAxisMode::RelativeTime | XAxisMode::WallTime);
            let is_relative = matches!(mode, XAxisMode::RelativeTime);
            let options = RectOptions {
                x_axis_mode: mode,
                x_axis_metric: metric.to_string(),
                ..Default::default()
            };
            // The request the fetch builds from these options.
            let request = ChartRequest {
                x_series: (!use_ts && !metric.is_empty()).then(|| SeriesRef {
                    metric_name: metric.to_string(),
                    ..Default::default()
                }),
                use_timestamp_axis: use_ts,
                relative_time: is_relative,
                log_buckets: log_x,
                ..Default::default()
            };
            let shown = ShownAxis::of_request(&request);
            assert_eq!(shown, ShownAxis::of_options(log_x, &options));
            assert_eq!(request.x_series.is_some(), !use_ts && !metric.is_empty());
            assert_eq!(shown.step_axis(), !use_ts && metric.is_empty());
            assert_eq!(
                shown.x_label(),
                if use_ts {
                    "time"
                } else if metric.is_empty() {
                    "step"
                } else {
                    metric
                }
            );
        }
    }

    #[test]
    fn noncontributors_require_a_complete_request_without_custom_x() {
        let plain = ChartRequest::default();
        assert!(request_proves_noncontributors(&plain));

        let mut custom = plain.clone();
        custom.x_series = Some(SeriesRef::default());
        assert!(!request_proves_noncontributors(&custom));

        let mut bounded = plain;
        bounded.step_min = Some(10);
        assert!(!request_proves_noncontributors(&bounded));

        let log_x = ChartRequest {
            log_buckets: true,
            ..Default::default()
        };
        assert!(!request_proves_noncontributors(&log_x));

        let options = |mode, metric: &str| RectOptions {
            x_axis_mode: mode,
            x_axis_metric: metric.to_string(),
            ..Default::default()
        };
        assert!(options_allow_noncontributors(
            &options(XAxisMode::Step, ""),
            false,
        ));
        assert!(!options_allow_noncontributors(
            &options(XAxisMode::Step, ""),
            true,
        ));
        assert!(!options_allow_noncontributors(
            &options(XAxisMode::Step, "train/epoch"),
            false,
        ));
        // Time mode ignores a retained custom metric when it builds the wire request, so it remains eligible for the ordinary optimization.
        assert!(options_allow_noncontributors(
            &options(XAxisMode::RelativeTime, "train/epoch"),
            true,
        ));
        assert!(options_allow_noncontributors(
            &options(XAxisMode::WallTime, "train/epoch"),
            true,
        ));
        let log_options = RectOptions {
            log_x: true,
            ..options(XAxisMode::Step, "")
        };
        assert!(!options_allow_noncontributors(&log_options, false));

        let requested = vec![
            SeriesRef {
                run_id: "a".into(),
                ..Default::default()
            },
            SeriesRef {
                run_id: "b".into(),
                ..Default::default()
            },
        ];
        custom.y_series = requested.clone();
        assert!(noncontributors_from_response(&custom, &DenseChart::default()).is_empty());
        let ordinary = ChartRequest {
            y_series: requested,
            ..Default::default()
        };
        assert_eq!(
            noncontributors_from_response(&ordinary, &DenseChart::default()),
            ["a".to_string(), "b".to_string()].into(),
        );
    }
}

#[component]
pub(super) fn NumericContent(
    bindings: Vec<MetricBinding>,
    #[props(default = false)] log_x: bool,
    #[props(default = false)] log_y: bool,
    #[props(default = 280)] chart_height: u32,
    #[props(default = 0)] color_version: u64,
    #[props(default)] options: RectOptions,
    loading: Signal<bool>,
    zone: Signal<Zone>,
    cache_key: String,
) -> Element {
    let state = use_context::<DashboardState>();

    let mut bindings_signal = use_signal(|| bindings.clone());
    if *bindings_signal.read() != bindings {
        bindings_signal.set(bindings.clone());
    }

    let mut options_signal = use_signal(|| options.clone());
    if *options_signal.read() != options {
        options_signal.set(options.clone());
    }
    let mut max_runs_signal = use_signal(|| options.max_runs);
    if *max_runs_signal.read() != options.max_runs {
        max_runs_signal.set(options.max_runs);
    }

    // The x-zoom lives in DashboardState::step_zoom, not per chart: every
    // zoom gesture (drag, reset click, axis pan, pinch) bubbles into the
    // document-level ZoomBridge (uplot_chart.rs), which writes the store,
    // and every step-axis chart's fetch reads it.
    // One pathway covers linked charts following each other — including
    // reset-click zoom-OUT, which the uPlot cursor sync does not replay — and
    // panels that mount after the zoom (collapsed sections, other pages,
    // the maximize overlay). Time/custom-x charts zoom client-side and
    // stay out of it entirely.

    // Chart x-resolution follows the rect's actual on-screen width in
    // PHYSICAL pixels (css width × devicePixelRatio), tracked by a
    // ResizeObserver so drag-resizes and window resizes refetch at the new
    // width; one bucket per pixel is the most a screen can show. Quantized
    // upward to 250s so minor layout shifts don't refetch.
    // 0 = not yet measured (the fetch falls back to 800).
    let mut chart_px = use_signal(|| 0u32);
    let measure_id = use_hook(|| {
        static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        format!(
            "mrect-measure-{}",
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )
    });

    // Per-run invalidation: this rect refetches only when one of ITS runs'
    // versions changes (or its run set changes) — another run logging
    // elsewhere on the page never touches this chart.
    let bound_run_ids = use_bound_run_ids(bindings_signal, max_runs_signal);
    // Bound runs known to have NO data on this panel's metrics (the last complete linear response without custom X had no series for them) — excluded from the version key below so their steady ingest bumps stop triggering probes. Signal for reactivity, seeded from the panel cache so a Far remount doesn't forget and reprobe.
    let noncontrib: Signal<Rc<std::collections::HashSet<String>>> = use_signal({
        let cache_key = cache_key.clone();
        move || {
            CHART_CACHE
                .with(|c| c.borrow_mut().get(&cache_key))
                .map(|e| e.noncontrib.clone())
                .unwrap_or_default()
        }
    });
    let my_version = use_memo(move || {
        // A range-trimmed response cannot prove a run silent elsewhere, custom X can yield an all-empty exact-step join even when both metrics are already registered, and log X can drop every negative axis position. Mirror the wire semantics so a cache-seeded exclusion cannot suppress those runs after an axis transition.
        let options = options_signal.read();
        let has_step_zoom = options.is_step_axis() && state.step_zoom.read().is_some();
        let allow_noncontributors = options_allow_noncontributors(&options, has_step_zoom);
        let nc = noncontrib.read();
        chart_sync::panel_version_key(
            *state.resync_gen.read(),
            &bound_run_ids.read(),
            &state.metrics_gen.read(),
            &state.run_versions.read(),
            allow_noncontributors.then_some(&**nc),
        )
    });

    // Fetch gate: an out-of-band panel (only mounted here while an editor pins the body) doesn't query and doesn't react to version bumps until it re-enters the band. A memo so Near <-> Visible flips don't restart an identical in-flight fetch (priority is peeked in the body).
    let allowed = use_memo(move || *zone.read() != Zone::Far);
    // The shared `loading` prop is both the corner spinner and the bridge's busy flag (same signal-sharing as text_stream): it spans the whole fetch — admission wait and retries included — so every fetch spins the chart it touches, and a pushed bump propagates when the flag clears instead of cancelling the fetch mid-flight. That propagation usually lands on the cache's sent stamp (see the fetch below) — a cache hit, not a second query.
    let data_seq = crate::state::use_version_bridge(my_version, loading, allowed);
    let mut loading = loading;

    // The (data_seq, request) -> response memoization lives in CHART_CACHE (module-level, survives this body unmounting at Far): re-entering the band with nothing changed serves it instead of re-querying, so scrolling around a settled dashboard is network-free.
    let data = use_resource({
        let cache_key = cache_key.clone();
        move || {
            let cache_key = cache_key.clone();
            let grpc = state.grpc.read().clone();
            let ctx = build_view_context(&state);
            // The refresh heartbeat: version-bump propagations (floored and gated in use_version_bridge) restart this resource through it. Only the subscription matters — entry validity is decided against snapshots (fresh_for), not the key.
            let _refresh = *data_seq.read();
            let _selected = state.selected_runs.read().clone();
            let bindings = bindings_signal.read().clone();
            let opts = options_signal.read().clone();
            // Subscribe to the shared zoom only on step-axis charts — the read
            // is conditional, so time/custom-x charts never react to it.
            let zoom = if opts.is_step_axis() {
                *state.step_zoom.read()
            } else {
                None
            };
            let measured_px = *chart_px.read();
            // Subscribed: on an empty project only this flag's flip re-runs the resource to turn the pre-runs "Loading..." into a real "No data".
            let runs_loaded = *state.runs_loaded.read();
            let allowed = *allowed.read();
            async move {
                // Heal a cancelled predecessor's flag (stuck true would freeze the version bridge and strand the spinner).
                if *loading.peek() {
                    loading.set(false);
                }
                // Wait for the first width measurement instead of fetching at
                // the 800-bucket fallback and refetching at the real width a
                // frame later — that doubled every chart's page-open query.
                // The ResizeObserver fires right after mount.
                if measured_px == 0 {
                    return ChartFetch::Unmeasured;
                }
                if !allowed {
                    return ChartFetch::Deferred;
                }
                let refs = resolve_capped_bindings(&bindings, &ctx, opts.max_runs);
                if refs.is_empty() {
                    // Before the first list_runs lands, empty refs mean "runs unknown", not "no runs match" — keep saying "Loading..." (a chart link opens the overlay ahead of list_runs; Answer(None) here flashed "No data" at it).
                    if !runs_loaded {
                        return ChartFetch::Unmeasured;
                    }
                    return ChartFetch::Answer(None);
                }
                let y_series: Vec<SeriesRef> = refs
                    .iter()
                    .map(|r| SeriesRef {
                        project_id: r.project_id.clone(),
                        run_id: r.run_id.clone(),
                        metric_name: r.metric_name.clone(),
                        tags: vec![],
                    })
                    .collect();

                use crate::state::layout_config::SmoothingAlgorithm;
                let algo = match opts.smoothing {
                    SmoothingAlgorithm::None => 0,
                    SmoothingAlgorithm::EmaPolyfit => 2,
                    SmoothingAlgorithm::BiweightPolyfit => 4,
                    SmoothingAlgorithm::TriangularPolyfit => 5,
                };

                use crate::state::layout_config::XAxisMode;
                let use_ts = matches!(
                    opts.x_axis_mode,
                    XAxisMode::RelativeTime | XAxisMode::WallTime
                );
                let is_relative = matches!(opts.x_axis_mode, XAxisMode::RelativeTime);

                // A saved custom metric only applies in step mode. Keep it in
                // the options so switching back restores the selection.
                let x_series = if !use_ts && !opts.x_axis_metric.is_empty() {
                    // Use the first Y series' project/run for the X metric
                    refs.first().map(|r| SeriesRef {
                        project_id: r.project_id.clone(),
                        run_id: r.run_id.clone(),
                        metric_name: opts.x_axis_metric.clone(),
                        tags: vec![],
                    })
                } else {
                    None
                };

                // Custom-x / time charts never see a step range here: `zoom` is
                // only read from the store on step-axis charts above.
                // cache_state stays None here: `request` is the query identity
                // (cache comparisons, the stored entry); the frontier echo is a
                // transport detail added onto the wire copy at send time.
                let request = ChartRequest {
                    y_series,
                    x_series,
                    smoothing: Some(SmoothingConfig {
                        algorithm: algo,
                        window_size: opts.smoothing_window,
                        alpha: opts.smoothing_alpha,
                        poly_order: opts.smoothing_poly_order,
                    }),
                    target_resolution: measured_px,
                    step_min: zoom.map(|z| z.0),
                    step_max: zoom.map(|z| z.1),
                    use_timestamp_axis: use_ts,
                    relative_time: is_relative,
                    cache_state: None,
                    // The panel renders log-x: the server buckets on the log ladder so slot density stays even across the decades.
                    log_buckets: opts.log_x,
                };

                // Serve the cache while the entry is fresh for the runs asked about (typical after a Far -> Near remount, and for bumps of runs this request doesn't even query, e.g. capped out of the set). All snapshot comparisons peek — the heartbeat read above is the subscription.
                let cached = CHART_CACHE.with(|c| c.borrow_mut().get(&cache_key));
                if let Some(e) = &cached {
                    let ver = state.run_versions.peek();
                    let mg = state.metrics_gen.peek();
                    let epoch = *state.resync_gen.peek();
                    if e.request == request
                        && e.fresh_for(
                            request.y_series.iter().map(|s| s.run_id.as_str()),
                            &ver,
                            &mg,
                            epoch,
                        )
                    {
                        return ChartFetch::Answer(Some(ChartAnswer {
                            data_seq: e.data_seq,
                            response: e.response.clone(),
                            request: Rc::new(e.request.clone()),
                        }));
                    }
                    // Deselection: a run-subset request fresh for the KEPT runs is answered from the superset response — no query, and the superset entry stays put so reselecting is equally free.
                    if let Some(keep) = chart_sync::subset_keep(&e.request, &request) {
                        if e.fresh_for(keep.iter().map(String::as_str), &ver, &mg, epoch) {
                            return ChartFetch::Answer(Some(ChartAnswer {
                                data_seq: next_data_seq(),
                                response: Rc::new(chart_sync::filter_response(&e.response, &keep)),
                                request: Rc::new(request.clone()),
                            }));
                        }
                    }
                }
                // Any cached answer means the panel already renders SOMETHING
                // (the render falls back to it), so this fetch is a refresh,
                // not a first paint, for gate purposes.
                let first_paint = cached.is_none();

                // Transient errors retry until success (with the token released between attempts), keeping the last good chart rendered. A terminal lifecycle or validation error settles unavailable with its explanation instead of retrying.
                loading.set(true);
                // Freshness snapshots peek just before the (ultimately successful) attempt sends: the fetch never subscribes to raw bumps, and an event pushed mid-flight — whose data the response may predate — invalidates instead of being absorbed. Snapshot-at-send is sound because a version reaches the client only after its data is queryable: pushes publish after the CH insert acks (ingest.rs write_dirty) and arrive ≥1s later (ws_proxy coalesce), far past the server cache's 250ms FRESH_WINDOW. Poll-learned versions (resync / minute backstop) skip the coalesce delay but carry exactly the exposure the trigger path always had.
                let response = visibility::retry_visible_chart("chart query", async || {
                    let _hi = visibility::admit_fetch(|| *zone.peek(), first_paint).await;
                    let epoch = *state.resync_gen.peek();
                    let pre = (
                        epoch,
                        state.metrics_gen.peek().clone(),
                        state.run_versions.peek().clone(),
                    );
                    let mut wire = request.clone();
                    // Rebuild the echoed opaque continuation state for every application-level retry from the same epoch snapshot recorded for that send; a reconnect between attempts must force this attempt to request a full response.
                    wire.cache_state = cached
                        .as_ref()
                        .and_then(|e| chart_sync::echo_state(e, &request, epoch));
                    grpc.query_chart(wire).await.map(|r| (pre, r))
                })
                .await;
                let (pre, mut resp) = match response {
                    Ok(response) => response,
                    Err(status) => {
                        loading.set(false);
                        evict_panel_caches(&cache_key);
                        return settled_failure(status);
                    }
                };
                let (epoch_snap, mg_snap, ver_snap) = pre;
                // The panel's name in a protocol alert, should one fire below.
                let alert_metric = request
                    .y_series
                    .first()
                    .map(|s| s.metric_name.clone())
                    .unwrap_or_default();
                if resp.audit_failed {
                    // The server's sampled audit caught a delta it was about to ship wrong; this response is the correct full replacement. A protocol bug to surface, not a data problem.
                    crate::components::notice_bar::protocol_alert(format!(
                        "server delta audit failed on '{alert_metric}'"
                    ));
                }
                // The freshest server-stamped frontiers — delta or full, they describe the response being folded in and ride the next echo.
                let mut frontiers = std::mem::take(&mut resp.frontiers);
                // Every response inflates into the dense model at receipt (chart_sync::inflate_response); a delta splices back into a full one, verified against the server's result hashes (chart_sync::splice_response). A refusal — shape violation, malformed segments, hash mismatch — is a protocol failure: alert, refetch in full, and render only that; a splice is never rendered on guesswork. The entry keeps the FIRST attempt's snapshots: the refetch covers at least as much, so they only under-claim.
                let spliced = if resp.delta {
                    cached
                        .as_ref()
                        .and_then(|e| chart_sync::splice_response(&e.response, &resp))
                } else {
                    chart_sync::inflate_response(&resp)
                };
                let full = match spliced {
                    Some(f) => f,
                    None => {
                        crate::components::notice_bar::protocol_alert(format!(
                            "chart response refused on '{alert_metric}'; refetching in full"
                        ));
                        let replacement =
                            visibility::retry_visible_chart("chart refetch", async || {
                                let _hi = visibility::admit_fetch(|| *zone.peek(), false).await;
                                let mut wire = grpc.query_chart(request.clone()).await?;
                                let f = std::mem::take(&mut wire.frontiers);
                                // A full answer that STILL fails to inflate would loop; surface it as an empty chart instead (the alert above already fired).
                                Ok::<_, tonic::Status>(
                                    chart_sync::inflate_response(&wire).map(|m| (m, f)),
                                )
                            })
                            .await;
                        let replacement = match replacement {
                            Ok(replacement) => replacement,
                            Err(status) => {
                                loading.set(false);
                                evict_panel_caches(&cache_key);
                                return settled_failure(status);
                            }
                        };
                        replacement
                            .map(|(m, f)| {
                                frontiers = f;
                                m
                            })
                            .unwrap_or_default()
                    }
                };
                loading.set(false);
                // Runs absent from a complete linear response without custom X have no data on this metric; remember them so their version bumps stop probing. An all-empty custom-X join or an all-negative log-X series loses every identity on the wire even though later ordinary data can make it plottable.
                let nc_new = noncontributors_from_response(&request, &full);
                // Freshness snapshots for the request's runs (fresh_for looks nothing else up), from the pre-send peeks above.
                let snap_for = |map: &std::collections::HashMap<String, u64>| {
                    request
                        .y_series
                        .iter()
                        .filter_map(|s| map.get(&s.run_id).map(|v| (s.run_id.clone(), *v)))
                        .collect::<std::collections::HashMap<String, u64>>()
                };
                let versions = snap_for(&ver_snap);
                let metrics_gen = snap_for(&mg_snap);
                let resp_rc = Rc::new(full);
                let nc_rc = Rc::new(nc_new);
                let data_seq = next_data_seq();
                let answer = ChartAnswer {
                    data_seq,
                    response: resp_rc.clone(),
                    request: Rc::new(request.clone()),
                };
                CHART_CACHE.with(|c| {
                    let entry = ChartCacheEntry {
                        request,
                        response: resp_rc.clone(),
                        data_seq,
                        versions: Rc::new(versions),
                        metrics_gen: Rc::new(metrics_gen),
                        epoch: epoch_snap,
                        frontiers: Rc::new(frontiers),
                        noncontrib: nc_rc.clone(),
                    };
                    let weight = entry.estimated_heap_bytes();
                    c.borrow_mut().put_weighted(cache_key, entry, weight)
                });
                let mut noncontrib = noncontrib;
                if *noncontrib.peek() != nc_rc {
                    noncontrib.set(nc_rc);
                }
                ChartFetch::Answer(Some(answer))
            }
        }
    });

    let read = data.read();
    // Settled data, or the cache for every no-answer state: deferred out of band (pinned body at Far), the width gate, and the pending polls right after a scroll-back remounts this body — the old chart paints instantly instead of flashing "Loading...". Rc, so these are refcount bumps, not chart copies. (A run-subset answer renders filtered while this fallback holds the superset — the superset is at worst one frame stale here.)
    let cached_entry = CHART_CACHE.with(|c| c.borrow_mut().get(&cache_key));
    let chart_to_show: Option<ChartAnswer> = match &*read {
        Some(ChartFetch::Answer(answer)) => answer.clone(),
        Some(ChartFetch::Unavailable(_)) => None,
        _ => cached_entry.as_ref().map(|e| ChartAnswer {
            data_seq: e.data_seq,
            response: e.response.clone(),
            request: Rc::new(e.request.clone()),
        }),
    };
    // Rendering semantics — axis kind, log bucketing, smoothed — must describe the response being PAINTED, not the live options: mid option-toggle the cache serves the previous response for a refetch cycle, and live-derived semantics would run the ms->s transform over step x, render linear buckets on a log scale, drop a shifted chart's zero slot, or stroke the wrong primary mark. The request cached alongside each response is that response's ground truth (every settled answer is put there before it renders); live options only cover the fresh-panel case where nothing is cached yet.
    let shown_axis = chart_to_show
        .as_ref()
        .map(|answer| ShownAxis::of_request(&answer.request))
        .unwrap_or_else(|| ShownAxis::of_options(log_x, &options));
    let x_label = shown_axis.x_label();
    let shown_smoothed = chart_to_show
        .as_ref()
        .map(|answer| {
            answer
                .request
                .smoothing
                .as_ref()
                .is_some_and(|sm| sm.algorithm != 0)
        })
        .unwrap_or_else(|| {
            !matches!(
                options.smoothing,
                crate::state::layout_config::SmoothingAlgorithm::None
            )
        });
    // A settled answer with no plottable data is knowledge ("No data"),
    // distinct from a fetch that hasn't happened ("Loading...").
    let answered = matches!(&*read, Some(ChartFetch::Answer(_)));
    let unavailable = match &*read {
        Some(ChartFetch::Unavailable(message)) => Some(message.clone()),
        _ => None,
    };
    let has_points = chart_to_show
        .as_ref()
        .is_some_and(|answer| !answer.response.x_values.is_empty());
    // Subscribed read: entering/leaving the viewport mounts/unmounts the
    // chart below.
    let zone_now = *zone.read();
    let body = match chart_to_show {
        // Offscreen: no uPlot instance at all — unmounting UPlotChart
        // destroys it, removing it from the cursor-sync group and the
        // tooltip/highlight loops. The placeholder holds the height; the
        // data stays cached for instant remount.
        Some(_) if has_points && zone_now != Zone::Visible => rsx! {
            div { class: "chart-container", style: "min-height: {chart_height}px;" }
        },
        Some(ChartAnswer {
            data_seq,
            response: chart,
            ..
        }) if has_points => {
            // Time modes: ms -> seconds. The per-bucket x extents live in the same ms domain as the axis and feed the tooltip's x-range header, which formats seconds — convert them together or the header reads ~1000x off.
            let time_log_shift =
                shown_axis.time && shown_axis.log_x && chart.x_values.first() == Some(&0.0);
            let display_chart = if shown_axis.time {
                let mut c = (*chart).clone();
                // Match the server's log(t_ms + 1) buckets before converting to seconds.
                // Tooltip and copy readouts undo the shift; bucket x extents stay unshifted.
                let shift = if time_log_shift { 1.0 } else { 0.0 };
                c.x_values = c.x_values.iter().map(|&t| (t + shift) / 1000.0).collect();
                c.xr_min = c.xr_min.iter().map(|&t| t / 1000.0).collect();
                c.xr_max = c.xr_max.iter().map(|&t| t / 1000.0).collect();
                c
            } else {
                (*chart).clone()
            };

            // Resolve run_id UUIDs in labels to run_names, and compute
            // per-series colors from run ordinals. Identity comes from the
            // series' run_id field; older servers don't send it, so fall
            // back to parsing the label ("run_id", "run_id/tag",
            // "run_id/metric_name", ... — run_id is the prefix up to the
            // first '/').
            let all_runs = state.display_runs();
            let labels: Vec<String> = display_chart
                .series
                .iter()
                .map(|s| crate::state::rewrite_label_with_run_name(&s.label, &s.run_id, &all_runs))
                .collect();
            let (colors, run_names): (Vec<String>, Vec<Option<String>>) = display_chart
                .series
                .iter()
                .map(|s| {
                    let rid = if !s.run_id.is_empty() {
                        s.run_id.clone()
                    } else {
                        crate::state::run_id_from_label(&s.label).to_string()
                    };
                    if let Some(run) = all_runs.iter().find(|r| r.run_id == rid) {
                        (
                            crate::components::uplot_chart::run_color(&run.run_id, run.ordinal),
                            Some(run.run_name.clone()),
                        )
                    } else {
                        (crate::components::uplot_chart::hash_color(&s.label), None)
                    }
                })
                .unzip();

            rsx! {
                UPlotChart {
                    chart: display_chart,
                    data_key: data_seq,
                    labels: Some(labels),
                    run_names: run_names,
                    colors: Some(colors),
                    log_x: shown_axis.log_x,
                    // Step charts render log-x as log(x+1), matching the server's bucket ladder (time axes fold their +1ms into the ms->s transform above; custom-x keeps plain log).
                    log_shift: shown_axis.log_x && shown_axis.step_axis(),
                    time_log_shift: time_log_shift,
                    log_y: log_y,
                    height: chart_height,
                    color_version: color_version,
                    smoothed: shown_smoothed,
                    x_label: x_label.clone(),
                    zoom_refetch: shown_axis.step_axis(),
                    is_time_axis: shown_axis.time,
                    is_wall_time: shown_axis.wall,
                }
            }
        }
        // An empty answer is knowledge, and a refetch in flight doesn't
        // un-know it — these arms hold steady across restarts.
        Some(_) => rsx! {
            div { class: "rect-empty", style: "height: {chart_height}px;", "No data" }
        },
        None if unavailable.is_some() => {
            let message = unavailable.as_deref().unwrap_or_default();
            rsx! {
                div { class: "rect-empty", style: "height: {chart_height}px;", "{message}" }
            }
        }
        None if answered => rsx! {
            div { class: "rect-empty", style: "height: {chart_height}px;", "No data" }
        },
        // Nothing known yet: first fetch in flight, deferred with no cache
        // (a never-fetched offscreen panel), or the width gate.
        None => rsx! {
            div { class: "rect-loading", style: "height: {chart_height}px;", "Loading..." }
        },
    };

    // Observe the rect's width (all states render inside this wrapper, so
    // the observer exists before the first chart draws — ResizeObserver
    // fires once on observe, which provides the initial measurement).
    use_drop({
        let measure_id = measure_id.clone();
        move || {
            // Stop the observer BEFORE its channel's Rust side is gone —
            // RO callbacks (including the detach-time delivery) would
            // otherwise throw in dioxus's glue where no call-site catch
            // can reach.
            let js = format!(
                "let ros=window.__kymo_ros;if(ros&&ros['{measure_id}']){{ros['{measure_id}'].disconnect();delete ros['{measure_id}'];}}"
            );
            // spawn() would park this on the scope being torn down, where it
            // is never polled (dioxus drains the dying scope's tasks before
            // dropping hooks) — root-scope it so it actually runs.
            dioxus::core::spawn_forever(async move {
                let _ = document::eval(&js).await;
            });
        }
    });

    rsx! {
        div {
            id: "{measure_id}",
            style: "width:100%",
            onmounted: {
                let measure_id = measure_id.clone();
                move |_| {
                    let js = format!(
                        r#"(()=>{{
let el=document.getElementById('{measure_id}');
if(!el||el.__kymo_ro)return;
// Registered globally so the component's use_drop can disconnect it: the
// Rust side of this channel dies at unmount, and a send from a callback
// that outlives it throws inside dioxus's queued glue — uncatchable at
// this call site. The try/catch below is only a residual-race belt.
window.__kymo_ros=window.__kymo_ros||{{}};
el.__kymo_ro=new ResizeObserver(es=>{{
  for(let e of es){{
    try{{dioxus.send(e.contentRect.width*devicePixelRatio);}}
    catch(_){{el.__kymo_ro.disconnect();el.__kymo_ro=null;break;}}
  }}
}});
window.__kymo_ros['{measure_id}']=el.__kymo_ro;
el.__kymo_ro.observe(el);
}})()"#
                    );
                    spawn(async move {
                        let mut eval = document::eval(&js);
                        while let Ok(w) = eval.recv::<f64>().await {
                            // Width 0 = hidden / not laid out (display:none
                            // ancestor), not "narrow" — clamping it to the
                            // 400px floor made invisible charts fetch. Keep
                            // the gate shut (or the last real width) until
                            // the rect actually has pixels.
                            if w < 1.0 {
                                continue;
                            }
                            let px = w.clamp(400.0, 4000.0) as u32;
                            let q = px.div_ceil(250) * 250;
                            if *chart_px.peek() != q {
                                chart_px.set(q);
                            }
                        }
                    });
                }
            },
            {body}
        }
    }
}
