//! Numeric chart content and its fetch/render state.

use std::rc::Rc;

use dioxus::prelude::*;

use super::{evict_panel_caches, CHART_CACHE};
use crate::components::uplot_chart::{ChartModel, ShownAxis, UPlotChart};
use crate::grpc::chart_delta::DenseChart;
use crate::grpc::proto::{ChartRequest, SeriesRef, SmoothingConfig};
use crate::state::chart_sync::{self, ChartCacheEntry};
use crate::state::layout_config::{ema_time_constant, RectOptions};
use crate::state::visibility::{self, Zone};
use crate::state::DashboardState;
use crate::util::resize_observer::ElementResizeObserver;

/// What one run of the chart resource produced. `use_resource` keeps the
/// stale value across restarts, so the render must be able to tell whether
/// that stale value is knowledge ("the last answer was empty" — a refetch
/// doesn't un-know it, keep saying "No data") or a no-query sentinel
/// ("nothing is known, keep saying Loading..." / "render from cache").
enum ChartFetch {
    /// No query ran: the rect's width isn't measured yet, or the rect is
    /// outside the prefetch band (Zone::Far). The render falls back to the
    /// fetch cache, so a chart scrolled far away and back re-renders without
    /// a refetch.
    Deferred,
    /// A real answer paired with the exact request semantics it was built
    /// under. Transient query errors retry instead of settling.
    Answer(ChartAnswer),
    /// A terminal lifecycle or request error. Stale panel data is evicted and
    /// the preserved explanation stays visible instead of retrying forever.
    Unavailable(String),
}

#[derive(Clone)]
struct ChartAnswer {
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

impl ShownAxis {
    /// The axis of the request a response answered, so the render always describes the response being painted.
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
mod tests {
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

    /// A request's axis follows its timestamp and custom-x fields: the render describes the response that request answered.
    #[test]
    fn requests_derive_their_axis() {
        for (mode, metric, log_x) in [
            (XAxisMode::Step, "", false),
            (XAxisMode::Step, "", true),
            (XAxisMode::Step, "train/epoch", true),
            (XAxisMode::RelativeTime, "", true),
            (XAxisMode::WallTime, "", false),
        ] {
            let use_ts = matches!(mode, XAxisMode::RelativeTime | XAxisMode::WallTime);
            // A request as the fetch builds one from these axis options.
            let request = ChartRequest {
                x_series: (!use_ts && !metric.is_empty()).then(|| SeriesRef {
                    metric_name: metric.to_string(),
                    ..Default::default()
                }),
                use_timestamp_axis: use_ts,
                relative_time: matches!(mode, XAxisMode::RelativeTime),
                log_buckets: log_x,
                ..Default::default()
            };
            let shown = ShownAxis::of_request(&request);
            assert_eq!(shown.log_x, log_x);
            assert_eq!(shown.wall, matches!(mode, XAxisMode::WallTime));
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
    /// The panel's resolved refs, from AutoContent's single resolution pass.
    refs: Memo<Rc<Vec<SeriesRef>>>,
    options: ReadSignal<RectOptions>,
    loading: Signal<bool>,
    zone: Signal<Zone>,
    cache_key: String,
) -> Element {
    let state = use_context::<DashboardState>();

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
    // 0 = not yet measured (the fetch waits for the first measurement).
    let mut chart_px = use_signal(|| 0u32);
    let mut width_observer = use_hook(|| CopyValue::new(None::<ElementResizeObserver>));

    // Bound runs known to have NO data on this panel's metrics (the last complete linear response without custom X had no series for them) — excluded from the version key below so their steady ingest bumps stop triggering probes. Signal for reactivity, seeded from the panel cache so a Far remount doesn't forget and reprobe.
    let noncontrib: Signal<Rc<std::collections::HashSet<String>>> = use_signal(|| {
        CHART_CACHE
            .with(|c| c.borrow_mut().get(&cache_key))
            .map(|e| e.noncontrib.clone())
            .unwrap_or_default()
    });
    // Per-run invalidation: this rect refetches only when one of ITS runs'
    // versions changes (or its run set changes) — another run logging
    // elsewhere on the page never touches this chart.
    let my_version = use_memo(move || {
        // A range-trimmed response cannot prove a run silent elsewhere, custom X can yield an all-empty exact-step join even when both metrics are already registered, and log X can drop every negative axis position. Mirror the wire semantics so a cache-seeded exclusion cannot suppress those runs after an axis transition.
        let options = options.read();
        let has_step_zoom = options.is_step_axis() && state.step_zoom.read().is_some();
        let allow_noncontributors = options_allow_noncontributors(&options, has_step_zoom);
        let nc = noncontrib.read();
        let refs = refs.read();
        let bound: Vec<&str> = refs.iter().map(|r| r.run_id.as_str()).collect();
        chart_sync::panel_version_key(
            *state.resync_gen.read(),
            &bound,
            &state.metrics_gen.read(),
            &state.run_versions.read(),
            allow_noncontributors.then_some(&**nc),
        )
    });

    // Fetch gate: an out-of-band panel (mounted only mid-rename) doesn't query and doesn't react to version bumps until it re-enters the band. A memo so Near <-> Visible flips don't restart an identical in-flight fetch (priority is peeked in the body).
    let allowed = use_memo(move || *zone.read() != Zone::Far);
    // The shared `loading` prop is both the corner spinner and the bridge's busy flag (same signal-sharing as text_stream): it spans the whole fetch — admission wait and retries included — so every fetch spins the chart it touches, and a pushed bump propagates when the flag clears instead of cancelling the fetch mid-flight. That propagation usually lands on the cache entry's echoed versions (folded into the client's map at receipt, see the fetch below) — a cache hit, not a second query.
    let data_seq = crate::state::use_version_bridge(my_version, loading, allowed);
    let mut loading = loading;

    // The per-panel response cache lives in CHART_CACHE (module-level, survives this body unmounting at Far): re-entering the band with nothing changed serves it instead of re-querying, so scrolling around a settled dashboard is network-free.
    let data = use_resource({
        let cache_key = cache_key.clone();
        move || {
            let cache_key = cache_key.clone();
            let grpc = state.grpc.read().clone();
            // The refresh heartbeat: version-bump propagations (floored and gated in use_version_bridge) restart this resource through it. Only the subscription matters — entry validity is decided by fresh_for, not the key.
            let _refresh = *data_seq.read();
            let refs = refs.read().clone();
            let opts = options.read().clone();
            // Subscribe to the shared zoom only on step-axis charts — the read
            // is conditional, so time/custom-x charts never react to it.
            let zoom = if opts.is_step_axis() {
                *state.step_zoom.read()
            } else {
                None
            };
            let measured_px = *chart_px.read();
            let allowed = *allowed.read();
            async move {
                crate::state::heal_loading(loading);
                // Wait for the first width measurement (delivered right after mount): fetching before it would refetch at the real width a frame later, doubling every chart's page-open query.
                if measured_px == 0 || !allowed {
                    return ChartFetch::Deferred;
                }
                let y_series = refs.to_vec();

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
                        metric_name: opts.x_axis_metric.clone(),
                        ..r.clone()
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
                        poly_order: opts.smoothing_poly_order,
                        time_constant: ema_time_constant(opts.smoothing_alpha),
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
                        return ChartFetch::Answer(ChartAnswer {
                            response: e.response.clone(),
                            request: Rc::new(e.request.clone()),
                        });
                    }
                    // Deselection: a run-subset request fresh for the KEPT runs is answered from the superset response — no query, and the superset entry stays put so reselecting is equally free.
                    if let Some(keep) = chart_sync::subset_keep(&e.request, &request) {
                        if e.fresh_for(keep.iter().map(String::as_str), &ver, &mg, epoch) {
                            return ChartFetch::Answer(ChartAnswer {
                                response: Rc::new(chart_sync::filter_response(&e.response, &keep)),
                                request: Rc::new(request.clone()),
                            });
                        }
                    }
                }
                // Any cached answer means the panel already renders SOMETHING
                // (the render falls back to it), so this fetch is a refresh,
                // not a first paint, for gate purposes.
                let first_paint = cached.is_none();

                // Freshness stamps cover the request's runs only: fresh_for looks nothing else up.
                let snap_for = |map: &std::collections::HashMap<String, u64>| {
                    crate::state::versions_of(
                        map,
                        request.y_series.iter().map(|s| s.run_id.as_str()),
                    )
                };

                // Transient errors retry until success (with the token released between attempts), keeping the last good chart rendered. A terminal lifecycle or validation error settles unavailable with its explanation instead of retrying.
                loading.set(true);
                // Data freshness comes from the reply's version echo (ChartResponse.run_versions), so a catch-up poll landing after the send can't make it look stale. The registry and epoch snapshots, which the server can't echo, peek just before the (ultimately successful) attempt sends: an event pushed mid-flight invalidates instead of being absorbed. So do the request's run versions, the echo's fallback ([`crate::state::answer_stamps`]).
                let response = visibility::retry_visible_chart("chart query", async || {
                    let _hi = visibility::admit_fetch(|| *zone.peek(), first_paint).await;
                    let epoch = *state.resync_gen.peek();
                    let pre = (
                        epoch,
                        snap_for(&state.metrics_gen.peek()),
                        snap_for(&state.run_versions.peek()),
                    );
                    let mut wire = request.clone();
                    // Rebuild the echoed opaque continuation state for every application-level retry from the same epoch snapshot recorded for that send; a reconnect between attempts must force this attempt to request a full response.
                    wire.cache_state = cached
                        .as_ref()
                        .and_then(|e| chart_sync::echo_state(e, &request, epoch));
                    grpc.query_chart(wire).await.map(|r| (pre, r))
                })
                .await;
                let ((epoch_snap, mg_snap, sent_versions), mut resp) = match response {
                    Ok(response) => response,
                    Err(status) => {
                        loading.set(false);
                        evict_panel_caches(&cache_key);
                        return settled_failure(status);
                    }
                };
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
                // Every response inflates into the dense model at receipt (chart_sync::inflate_response); a delta splices back into a full one, verified against the server's result hashes (chart_sync::splice_response). A refusal — shape violation, malformed segments, hash mismatch — is a protocol failure: alert, refetch in full, and render only that; a splice is never rendered on guesswork. The refetch replaces the refused reply, frontiers and version echo included (its data is what renders); the send-time registry/epoch snapshots it keeps only under-claim.
                let spliced = if resp.delta {
                    cached
                        .as_ref()
                        .and_then(|e| chart_sync::splice_response(&e.response, &resp))
                } else {
                    chart_sync::inflate_response(&resp)
                };
                let (full, inflated) = match spliced {
                    Some(f) => (f, true),
                    None => {
                        crate::components::notice_bar::protocol_alert(format!(
                            "chart response refused on '{alert_metric}'; refetching in full"
                        ));
                        let replacement =
                            visibility::retry_visible_chart("chart refetch", async || {
                                let _hi = visibility::admit_fetch(|| *zone.peek(), false).await;
                                grpc.query_chart(request.clone()).await
                            })
                            .await;
                        match replacement {
                            // A full answer that STILL fails to inflate would loop; it renders as an empty chart instead (the alert above already fired), which proves nothing about which runs lack data.
                            Ok(wire) => {
                                let full = chart_sync::inflate_response(&wire);
                                resp = wire;
                                let inflated = full.is_some();
                                (full.unwrap_or_default(), inflated)
                            }
                            Err(status) => {
                                loading.set(false);
                                evict_panel_caches(&cache_key);
                                return settled_failure(status);
                            }
                        }
                    }
                };
                loading.set(false);
                // Runs absent from a complete linear response without custom X have no data on this metric; remember them so their version bumps stop probing. An all-empty custom-X join loses every identity on the wire even though later ordinary data can make it plottable. So does an all-negative log-X chart from a server that predates shipping unplottable counts (AI-1491), which keeps log-X excluded until that server is gone.
                let nc_new = if inflated {
                    noncontributors_from_response(&request, &full)
                } else {
                    Default::default()
                };
                // The freshest server-stamped frontiers — delta or full, they describe the response being folded in and ride the next echo.
                let frontiers = resp.frontiers;
                let versions = Rc::new(crate::state::answer_stamps(
                    state.run_versions,
                    resp.run_versions,
                    sent_versions,
                ));
                let resp_rc = Rc::new(full);
                let nc_rc = Rc::new(nc_new);
                let answer = ChartAnswer {
                    response: resp_rc.clone(),
                    request: Rc::new(request.clone()),
                };
                CHART_CACHE.with(|c| {
                    let entry = ChartCacheEntry {
                        request,
                        response: resp_rc.clone(),
                        versions,
                        metrics_gen: Rc::new(mg_snap),
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
                ChartFetch::Answer(answer)
            }
        }
    });

    let read = data.read();
    // Settled data, or the cache for every no-answer state: deferred out of band (pinned body at Far), the width gate, and the pending polls right after a scroll-back remounts this body — the old chart paints instantly instead of flashing "Loading...". Rc, so these are refcount bumps, not chart copies. (A run-subset answer renders filtered while this fallback holds the superset — the superset is at worst one frame stale here.)
    let cached_entry = CHART_CACHE.with(|c| c.borrow_mut().get(&cache_key));
    let chart_to_show: Option<ChartAnswer> = match &*read {
        Some(ChartFetch::Answer(answer)) => Some(answer.clone()),
        Some(ChartFetch::Unavailable(_)) => None,
        _ => cached_entry.as_ref().map(|e| ChartAnswer {
            response: e.response.clone(),
            request: Rc::new(e.request.clone()),
        }),
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
            div { class: "chart-container" }
        },
        Some(ChartAnswer {
            response: chart,
            request,
        }) if has_points => {
            // The axis and smoothing come from the request this response answered, not the live options: mid option-toggle the cache serves the previous response for a refetch cycle, and live options would run the ms->s transform over step x, render linear buckets on a log scale, drop a shifted chart's zero slot, or stroke the wrong primary mark.
            let smoothed = request
                .smoothing
                .as_ref()
                .is_some_and(|sm| sm.algorithm != 0);
            // Run colors live in localStorage; subscribe to the generation the sidebar bumps after an override changes.
            let _color_version = *state.color_version.read();
            // Each series' run_id names its run: run_name for the label, ordinal for the color.
            let all_runs = state.display_runs();
            let labels: Vec<String> = chart
                .series
                .iter()
                .map(|s| crate::state::rewrite_label_with_run_name(&s.label, &s.run_id, &all_runs))
                .collect();
            let (colors, run_names): (Vec<String>, Vec<Option<String>>) = chart
                .series
                .iter()
                .map(|s| {
                    if let Some(run) = all_runs.iter().find(|r| r.run_id == s.run_id) {
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
                    chart: ChartModel(chart),
                    labels: labels,
                    run_names: run_names,
                    colors: colors,
                    axis: ShownAxis::of_request(&request),
                    log_y: options.read().log_y,
                    smoothed: smoothed,
                }
            }
        }
        // An empty answer is knowledge, and a refetch in flight doesn't
        // un-know it — these arms hold steady across restarts.
        Some(ChartAnswer {
            response: chart, ..
        }) => {
            // Samples whose x this axis can't place still ship their counts over an empty axis.
            let unplottable: u64 = chart.series.iter().map(|s| u64::from(s.xnan_count)).sum();
            let message = match unplottable {
                0 => "No data".to_string(),
                1 => "No plottable data: 1 sample has an x this axis can't show".to_string(),
                n => format!("No plottable data: {n} samples have an x this axis can't show"),
            };
            rsx! {
                div { class: "rect-empty", "{message}" }
            }
        }
        None => match &*read {
            Some(ChartFetch::Unavailable(message)) => rsx! {
                div { class: "rect-empty", "{message}" }
            },
            // Nothing known yet: first fetch in flight, deferred with no cache
            // (a never-fetched offscreen panel), or the width gate.
            _ => rsx! {
                div { class: "rect-loading", "Loading..." }
            },
        },
    };

    // Observe the rect's width (all states render inside this wrapper, so
    // the observer exists before the first chart draws).
    rsx! {
        div {
            style: "width:100%",
            onmounted: move |event| {
                let Some(element) = event.data().downcast::<web_sys::Element>().cloned() else {
                    return;
                };
                width_observer.set(Some(ElementResizeObserver::new(&element, move |entry| {
                    let dpr = web_sys::window().map_or(1.0, |window| window.device_pixel_ratio());
                    let w = entry.content_rect().width() * dpr;
                    // Width 0 = hidden / not laid out (display:none
                    // ancestor), not "narrow" — clamping it to the
                    // 400px floor made invisible charts fetch. Keep
                    // the gate shut (or the last real width) until
                    // the rect actually has pixels.
                    if w < 1.0 {
                        return;
                    }
                    let px = w.clamp(400.0, 4000.0) as u32;
                    let q = px.div_ceil(250) * 250;
                    if *chart_px.peek() != q {
                        chart_px.set(q);
                    }
                })));
            },
            {body}
        }
    }
}
