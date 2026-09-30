use super::*;

#[test]
fn settled_negative_log_markers_keep_polls_and_appends_small() {
    for use_time in [false, true] {
        let shaped = |n| rows_shaped(n, |i| if use_time { i } else { i - 1 }, |i| 10 * (i - 1));
        for algorithm in ALGORITHMS {
            let mut request = req(&["a"], 10_000);
            request.use_timestamp_axis = use_time;
            request.log_buckets = true;
            request.smoothing = (algorithm != Algorithm::None).then_some(proto::SmoothingConfig {
                algorithm: algorithm as i32,
                window_size: 20,
                time_constant: std::f64::consts::LOG2_E,
                poly_order: 1,
            });
            let held_rows = shaped(2_001);
            let held = build(&request, std::slice::from_ref(&held_rows));
            let held_model = inflate_full(&held);
            assert_eq!(held_model.series[0].nan_kinds, vec![4]);
            assert_eq!(held_model.series[0].nan_indices, vec![0]);
            assert_eq!(held_model.series[0].xnan_count, 1);

            for current in [held_rows, shaped(2_005)] {
                for audit in [false, true] {
                    let out = checked_delta(&request, &held, std::slice::from_ref(&current), audit);
                    // Centered smoothers additionally resend their bounded influence near the tail. Settled markers at the start must not force any algorithm to resend the whole chart.
                    let max_tail = match algorithm {
                        Algorithm::SavitzkyGolay => 30,
                        Algorithm::Ema | Algorithm::Triangular => 6,
                        _ => 5,
                    };
                    assert!(
                        out.x_values.len() <= max_tail,
                        "time={use_time}, {algorithm:?}, audit={audit}: resent {} columns",
                        out.x_values.len()
                    );
                    assert!(out.series[0].nan_indices.is_empty());
                    assert_eq!(out.series[0].xnan_count, 1);
                }
            }
        }
    }
}

fn timestamp_rows(timestamps: &[i64]) -> Arc<Vec<VersionedRawPoint>> {
    rows_shaped(timestamps.len() as i64, |i| i, |i| timestamps[i as usize])
}

fn absolute_log_request() -> proto::ChartRequest {
    let mut request = req(&["a", "b"], 1);
    request.use_timestamp_axis = true;
    request.log_buckets = true;
    request
}

#[test]
fn unchanged_marker_right_of_last_envelope_keeps_every_column() {
    let current = [
        timestamp_rows(&[-1, 11, 11]),
        timestamp_rows(&[1, 6, 8, 9, 10, 11, 11]),
    ];
    let request = absolute_log_request();
    let held = build(&request, &current);
    assert_eq!(inflate_full(&held).x_values, vec![1.0, 6.0, 9.5]);
    for audit in [false, true] {
        let out = checked_delta(&request, &held, &current, audit);
        assert_eq!(out.from_col, 3);
        assert!(out.x_values.is_empty());
        assert!(out.series[0].nan_indices.is_empty());
    }
}

#[test]
fn newly_present_marker_right_of_last_envelope_resends_its_slot() {
    let other = timestamp_rows(&[1, 6, 8, 9, 10, 11, 11]);
    let request = absolute_log_request();
    let held = build(&request, &[timestamp_rows(&[11, 11]), other.clone()]);
    assert_eq!(inflate_full(&held).x_values, vec![1.0, 6.0, 9.5]);
    assert!(inflate_full(&held).series[0].nan_indices.is_empty());
    let current = [timestamp_rows(&[11, 11, -1]), other];
    for audit in [false, true] {
        let out = checked_delta(&request, &held, &current, audit);
        assert_eq!(out.from_col, 2);
        assert_eq!(out.series[0].nan_indices, vec![2]);
    }
}

/// The same appearance on a passthrough chart: the anchor x=5 is an exact raw slot, and marking it REACH starts the delta there.
#[test]
fn newly_present_marker_on_a_passthrough_chart_resends_its_anchor_slot() {
    let b = timestamp_rows(&[1, 2, 3, 4]);
    let mut request = absolute_log_request();
    request.target_resolution = 10_000;
    let held = build(&request, &[timestamp_rows(&[5, 7, 9]), b.clone()]);
    assert_eq!(
        inflate_full(&held).x_values,
        vec![1.0, 2.0, 3.0, 4.0, 5.0, 7.0, 9.0]
    );
    let current = [timestamp_rows(&[5, 7, 9, -1]), b];
    assert_eq!(
        inflate_full(&build(&request, &current)).series[0].nan_indices,
        vec![4]
    );
    for audit in [false, true] {
        assert_eq!(checked_delta(&request, &held, &current, audit).from_col, 4);
    }
}

#[test]
fn other_series_envelope_growth_keeps_marker_on_its_anchor_cell() {
    // A's negative timestamp and x=8 samples are held. B's append moves the envelope center to 11.5, nearer raw x=6 than A's anchor, but A's marker stays on its own envelope, and only that changed envelope is resent.
    let a = timestamp_rows(&[-1, 8, 8]);
    let held_b = timestamp_rows(&[1, 6, 8, 9, 10, 11, 11]);
    let grown_b = timestamp_rows(&[1, 6, 8, 9, 10, 11, 11, 15]);
    let request = absolute_log_request();
    let held = build(&request, &[a.clone(), held_b]);
    let held_model = inflate_full(&held);
    assert_eq!(held_model.x_values, vec![1.0, 6.0, 9.5]);
    assert_eq!(held_model.series[0].nan_indices, vec![2]);

    let current = [a, grown_b];
    let truth = inflate_full(&build(&request, &current));
    assert_eq!(truth.x_values, vec![1.0, 6.0, 11.5]);
    assert_eq!(truth.series[0].nan_indices, vec![2]);
    assert_eq!(truth.series[0].nan_kinds, vec![4]);
    for audit in [false, true] {
        let out = checked_delta(&request, &held, &current, audit);
        assert_eq!(out.from_col, 2, "raw x=6 stays reusable");
    }
}

#[test]
fn raw_to_envelope_transition_carries_marker_into_the_new_envelope() {
    let a = timestamp_rows(&[-1, 8, 8]);
    // The [16,32) envelope exists on both sides, preserving the chart's emission shape. The fourth distinct x in [8,16) replaces its three raw slots with center 11.5, and A's x=8 marker follows its anchor into that envelope. The final duplicate in [16,32) leaves the earlier raw slots settled.
    let held_b = timestamp_rows(&[1, 6, 8, 13, 14, 16, 17, 18, 19, 19]);
    let grown_b = timestamp_rows(&[1, 6, 8, 13, 14, 16, 17, 18, 19, 19, 15]);
    let request = absolute_log_request();
    let held = build(&request, &[a.clone(), held_b]);
    let held_model = inflate_full(&held);
    assert_eq!(held_model.x_values, vec![1.0, 6.0, 8.0, 13.0, 14.0, 17.5]);
    assert_eq!(held_model.series[0].nan_indices, vec![2]);

    let current = [a, grown_b];
    let truth = inflate_full(&build(&request, &current));
    assert_eq!(truth.x_values, vec![1.0, 6.0, 11.5, 17.5]);
    assert_eq!(truth.series[0].nan_indices, vec![2]);
    assert_eq!(truth.series[0].nan_kinds, vec![4]);
    for audit in [false, true] {
        let out = checked_delta(&request, &held, &current, audit);
        assert_eq!(out.from_col, 2, "the mode change starts the delta");
    }
}

#[test]
fn earlier_positive_timestamp_changes_the_held_markers_carried_anchor() {
    // A's settled sentinel was carried at its first positive timestamp 10. A later step arrives with timestamp 8 and becomes the anchor; its inserted slot starts the delta, which also drops the held marker at x=10.
    let held_a = timestamp_rows(&[-1, 10, 12]);
    let grown_a = timestamp_rows(&[-1, 10, 12, 8]);
    let b = timestamp_rows(&[1, 6, 20, 22]);
    let mut request = absolute_log_request();
    request.target_resolution = 10_000;
    let held = build(&request, &[held_a, b.clone()]);
    let held_model = inflate_full(&held);
    assert_eq!(held_model.x_values, vec![1.0, 6.0, 10.0, 12.0, 20.0, 22.0]);
    assert_eq!(held_model.series[0].nan_indices, vec![2]);
    assert_eq!(
        held_model.x_values[held_model.series[0].nan_indices[0] as usize],
        10.0
    );

    let current = [grown_a, b];
    let truth = inflate_full(&build(&request, &current));
    assert_eq!(truth.x_values, vec![1.0, 6.0, 8.0, 10.0, 12.0, 20.0, 22.0]);
    assert_eq!(truth.series[0].nan_indices, vec![2]);
    assert_eq!(truth.x_values[truth.series[0].nan_indices[0] as usize], 8.0);
    assert_eq!(truth.series[0].nan_kinds, vec![4]);
    for audit in [false, true] {
        let out = checked_delta(&request, &held, &current, audit);
        assert_eq!(out.from_col, 2, "the new anchor's inserted slot");
    }
}

#[test]
fn anchorless_negative_series_updates_its_count_while_continuing() {
    let axis = timestamp_rows(&[1, 2, 3, 4, 5, 6]);
    let held_rows = [timestamp_rows(&[-8, -4, -1]), axis.clone()];
    let current = [timestamp_rows(&[-8, -4, -1, -2]), axis];
    let mut request = absolute_log_request();
    request.target_resolution = 10_000;
    let held = build(&request, &held_rows);
    let held_model = inflate_full(&held);
    assert_eq!(held_model.series[0].xnan_count, 3);
    assert_eq!(held_model.series[0].nan_indices, vec![0], "left edge");
    assert_eq!(held_model.series[0].nan_kinds, vec![4]);

    for audit in [false, true] {
        let out = checked_delta(&request, &held, &current, audit);
        assert_eq!(out.from_col, 6);
        assert_eq!(out.splice_from_cached, vec![0, 1]);
        assert_eq!(out.series[0].xnan_count, 4);
        assert!(out.series[0].nan_indices.is_empty());
    }
}

/// A continuing run with no plottable x whose held rows lie only in the smoothing warmup margin gains its first in-range unplottable sample. Its marker appears on column 0 while every column is numerically unchanged, so no prefix can be reused.
#[test]
fn anchorless_marker_appearance_answers_full() {
    let held_a = rows_shaped(3, |i| 97 + i, |i| 5 + i);
    let grown_a = rows_shaped(4, |i| 97 + i, |i| if i == 3 { -1 } else { 5 + i });
    let axis = rows_shaped(6, |i| 100 + i, |i| i + 1);
    for algorithm in [
        Algorithm::Ema,
        Algorithm::Triangular,
        Algorithm::SavitzkyGolay,
    ] {
        let mut request = absolute_log_request();
        request.target_resolution = 10_000;
        request.step_min = Some(100);
        request.smoothing = Some(proto::SmoothingConfig {
            algorithm: algorithm as i32,
            window_size: 5,
            time_constant: std::f64::consts::LOG2_E,
            poly_order: 1,
        });
        let held = build(&request, &[held_a.clone(), axis.clone()]);
        let held_model = inflate_full(&held);
        assert_eq!(
            held.series.len(),
            2,
            "{algorithm:?}: the margin-only run is held"
        );
        assert_eq!(held_model.x_values.len(), 6);
        assert!(held_model.series[0].nan_indices.is_empty());
        assert_eq!(held_model.series[0].xnan_count, 0);

        let current = [grown_a.clone(), axis.clone()];
        let truth = inflate_full(&build(&request, &current));
        assert_eq!(truth.series[0].nan_indices, vec![0]);
        assert_eq!(truth.series[0].nan_kinds, vec![4]);
        assert_eq!(truth.series[0].xnan_count, 1);
        assert_full_response(&request, &held, &current, None);
    }
}

/// Unplottable samples of a run with no plottable x mark the chart's first column, for negative log-x (step and timestamp) and non-finite custom x alike, without disturbing the other run's data or its own logged-y precedence elsewhere.
#[test]
fn anchorless_runs_mark_the_first_column() {
    for use_time in [false, true] {
        let marker = rows_shaped(3, |i| if use_time { i } else { -3 + i }, |i| -3 + i);
        let axis = rows_shaped(40, |i| i + 5, |i| 100 + 10 * i);
        for algorithm in ALGORITHMS {
            let mut request = req(&["a", "b"], 12);
            request.use_timestamp_axis = use_time;
            request.log_buckets = true;
            request.smoothing = (algorithm != Algorithm::None).then_some(proto::SmoothingConfig {
                algorithm: algorithm as i32,
                window_size: 5,
                time_constant: std::f64::consts::LOG2_E,
                poly_order: 1,
            });
            let full = inflate_full(&build(&request, &[marker.clone(), axis.clone()]));
            assert!(full.xr_min.iter().any(|v| v.is_finite()), "downsampled");
            assert_eq!(full.series[0].nan_indices, vec![0]);
            assert_eq!(full.series[0].nan_kinds, vec![4]);
            assert_eq!(full.series[0].xnan_count, 3);
            assert!(full.series[0].values.iter().all(|v| v.is_nan()));
            assert!(full.series[1].nan_indices.is_empty());
        }
    }

    let mut request = req(&["a", "b"], 10_000);
    request.x_series = Some(proto::SeriesRef {
        metric_name: "x".into(),
        ..Default::default()
    });
    let rows = rows_shaped(4, |i| i, |i| i);
    for (log, bad) in [(false, f64::NAN), (false, f64::INFINITY), (true, -2.0)] {
        request.log_buckets = log;
        let x_maps = std::collections::HashMap::from([
            ("a".to_string(), (0..4).map(|s| (s, bad)).collect()),
            (
                "b".to_string(),
                (0..4).map(|s| (s, (s + 1) as f64)).collect(),
            ),
        ]);
        let full =
            build_response(&request, &[rows.clone(), rows.clone()], Some(&x_maps), true).unwrap();
        let full = inflate_full(&full);
        assert_eq!(full.x_values, vec![1.0, 2.0, 3.0, 4.0], "log={log} x={bad}");
        assert_eq!(full.series[0].nan_indices, vec![0]);
        assert_eq!(full.series[0].nan_kinds, vec![4]);
        assert_eq!(full.series[0].xnan_count, 4);
    }
}

/// With no plottable point anywhere, the response keeps each series' unplottable count over an empty axis and carries no continuation state (the client never echoes an empty axis either). The first plottable point brings back an axis whose column 0 holds both markers. A chart with nothing in range ships zero counts, which the panel reads as "No data".
#[test]
fn all_unplottable_chart_ships_counts_without_columns() {
    let request = absolute_log_request();
    let only_negative = [timestamp_rows(&[-3, -2]), timestamp_rows(&[-5])];
    let resp = build(&request, &only_negative);
    assert!(resp.x_values.is_empty());
    assert!(resp.frontiers.is_empty(), "nothing to continue from");
    let counts: Vec<u32> = resp.series.iter().map(|s| s.xnan_count).collect();
    assert_eq!(counts, vec![2, 1]);
    let model = inflate_full(&resp);
    assert!(model.series.iter().all(|s| s.nan_indices.is_empty()));

    let first_point = build(
        &request,
        &[timestamp_rows(&[-3, -2, 4]), timestamp_rows(&[-5])],
    );
    let model = inflate_full(&first_point);
    assert_eq!(model.x_values, vec![4.0]);
    assert_eq!(model.series[0].nan_indices, vec![0]);
    assert_eq!(model.series[1].nan_indices, vec![0]);
    assert_eq!(model.series[1].xnan_count, 1);

    let mut out_of_range = request.clone();
    out_of_range.step_min = Some(10);
    let empty = build(&out_of_range, &only_negative);
    assert!(empty.x_values.is_empty());
    assert!(
        empty.series.iter().all(|s| s.xnan_count == 0),
        "\"No data\""
    );
}

#[test]
fn first_plottable_points_seed_a_full_response_then_resume_deltas() {
    let axis = timestamp_rows(&[1, 2, 3, 4, 5, 6, 7, 8, 9]);
    let mut request = absolute_log_request();
    request.target_resolution = 10_000;
    let held = build(&request, &[timestamp_rows(&[-8, -4, -1]), axis.clone()]);
    assert_eq!(inflate_full(&held).series[0].nan_indices, vec![0]);

    let current = [timestamp_rows(&[-8, -4, -1, 3, 5]), axis.clone()];
    let truth = inflate_full(&build(&request, &current));
    assert_eq!(truth.series[0].nan_indices, vec![2]);
    assert_eq!(truth.series[0].nan_kinds, vec![4]);
    assert_eq!(truth.series[0].xnan_count, 3);
    // No held plottable sample proves continuation yet; the full response seeds the marker.
    for (audit, first) in [false, true]
        .into_iter()
        .zip(assert_full_response(&request, &held, &current, None))
    {
        let grown = [timestamp_rows(&[-8, -4, -1, 3, 5, 7]), axis.clone()];
        let delta = checked_delta(&request, &first, &grown, audit);
        assert_eq!(delta.from_col, 6);
        assert!(delta.series[0].nan_indices.is_empty());
        assert_eq!(delta.series[0].xnan_count, 3);
    }
}

#[test]
fn smoothers_bound_new_negative_timestamps_with_an_unchanged_plan() {
    for algorithm in [
        Algorithm::Ema,
        Algorithm::Triangular,
        Algorithm::SavitzkyGolay,
    ] {
        for first_x in [64, 1_000_000] {
            // The spacing irregularity keeps Median(100) fixed. Test nearby and distant new negative inputs; only finite-support smoothers can exclude the distant influence, and every algorithm must reuse the unrelated earlier series' prefix.
            let samples = |n| {
                rows_shaped(
                    n,
                    |i| i,
                    |i| {
                        if i == 60 {
                            -7
                        } else {
                            first_x + i * 100 + if i == 40 { 400 } else { 0 }
                        }
                    },
                )
            };
            let other = rows_shaped(80, |i| i, |i| if i < 40 { i + 1 } else { i * 9_000 });
            let mut request = absolute_log_request();
            request.target_resolution = 30;
            request.smoothing = Some(proto::SmoothingConfig {
                algorithm: algorithm as i32,
                window_size: 5,
                time_constant: std::f64::consts::LOG2_E,
                poly_order: 1,
            });
            let held = build(&request, &[samples(60), other.clone()]);
            let current = [samples(61), other];
            let mut continued = request.clone();
            continued.cache_state = echo(&held);
            let p = chart_params(&continued);
            let verified = verified_inputs(&continued, &current, &held.frontiers).unwrap();
            let prepared = prepare(
                &continued,
                &p,
                &current
                    .iter()
                    .map(|rows| rows.as_slice())
                    .collect::<Vec<_>>(),
                None,
                Some(&verified),
                false,
                None,
            )
            .unwrap()
            .series;
            assert_eq!(
                prepared[0].smoothing_plan,
                chart::SmoothingPlan::Median(100f64.to_bits())
            );
            assert!(
                smoothing_state_matches(&continued, &p, &prepared, 2),
                "{algorithm:?}: fixture must isolate the negative-input gate"
            );
            let truth = inflate_full(&build(&request, &current));
            if first_x == 64 {
                assert!(
                    inflate_full(&held).series[0]
                        .values
                        .iter()
                        .zip(&truth.series[0].values)
                        .any(|(a, b)| a.to_bits() != b.to_bits()),
                    "{algorithm:?}: near negative input must affect a held output"
                );
            }
            for audit in [false, true] {
                let out = checked_delta(&request, &held, &current, audit);
                assert!(out.from_col > 0);
                assert_eq!(out.series[0].xnan_count, 1);
                assert!(truth.series[0].nan_kinds.contains(&4));
            }
        }
    }
}

#[test]
fn uniform_savgol_insertion_before_zoom_reemits_numerically_changed_blocks() {
    for first_x in [0, 1_000] {
        let rows = |insert_earlier| {
            let mut state = 10u64;
            let mut samples: Vec<_> = (0..180)
                .map(|step| {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let value = ((state >> 32) % 10_000) as f32 / 97.0;
                    VersionedRawPoint {
                        timestamp_ms: first_x + step,
                        ..scalar_row(step, value)
                    }
                })
                .collect();
            if insert_earlier {
                samples.push(VersionedRawPoint {
                    timestamp_ms: first_x - 1,
                    ..scalar_row(180, 3.0)
                });
            }
            Arc::new(samples)
        };
        let mut request = req(&["a"], 1_000);
        request.use_timestamp_axis = true;
        request.log_buckets = true;
        request.step_min = Some(32);
        request.step_max = Some(168);
        request.smoothing = Some(proto::SmoothingConfig {
            algorithm: Algorithm::SavitzkyGolay as i32,
            window_size: 20,
            poly_order: 2,
            ..Default::default()
        });
        let held = build(&request, &[rows(false)]);
        let current = [rows(true)];
        assert!(
            verified_inputs(&request, &current, &held.frontiers).is_some(),
            "the cache proof must pass so this fixture exercises the SG dependency"
        );
        let current_full = build(&request, &current);
        assert_eq!(
            held.frontiers[&smoothing_state_key(0)],
            current_full.frontiers[&smoothing_state_key(0)],
            "the global smoothing-plan gate must also pass"
        );
        // The inserted row sorts first in TIME, outside the plotted steps and the planner's window-wide reach (steps 0-19 once sorted), yet it shifts every prefix-sum block under the plotted outputs. Compare f64 bits: the change rarely survives f32 rounding.
        let smoothed = |rows: &[VersionedRawPoint]| {
            let mut rows = rows.to_vec();
            rows.sort_by_key(|row| row.timestamp_ms);
            let xs: Vec<f64> = rows.iter().map(|row| row.timestamp_ms as f64).collect();
            let ys: Vec<f64> = rows.iter().map(|row| f64::from(row.value)).collect();
            chart::smooth_run(&xs, &ys, Algorithm::SavitzkyGolay, 20, 0.5, 2, false)
        };
        let (old, new) = (smoothed(&rows(false)), smoothed(&rows(true)));
        assert!((32..=168).any(|step| old[step].to_bits() != new[step + 1].to_bits()));
        for out in assert_full_response(&request, &held, &current, None) {
            assert_eq!(
                out.series[0].xnan_count, 0,
                "the new row is outside the plotted step range"
            );
        }
    }
}

#[test]
fn uniform_savgol_append_reemits_the_resized_final_block() {
    let mut state = 234u64;
    // Window 20's blocks cover twenty outputs each from step 10, so 195 samples leave steps 170-184 in a partial final block.
    let samples: Vec<_> = (0..195)
        .map(|step| {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let value = (((state >> 32) % 20_001) as i64 - 10_000) as f32 / 97.0;
            scalar_row(step, value)
        })
        .collect();
    let mut request = req(&["a"], 1_000);
    request.smoothing = Some(proto::SmoothingConfig {
        algorithm: Algorithm::SavitzkyGolay as i32,
        window_size: 20,
        poly_order: 2,
        ..Default::default()
    });
    let held = build(&request, &[Arc::new(samples.clone())]);
    let mut grown = samples.clone();
    grown.extend([scalar_row(195, 3.0), scalar_row(196, 4.0)]);
    // Appending resizes the final block and moves its origin, changing f64 outputs at steps 170-184 outside the new rows' ten-sample support.
    let smoothed = |rows: &[VersionedRawPoint]| {
        let xs: Vec<f64> = rows.iter().map(|row| row.step as f64).collect();
        let ys: Vec<f64> = rows.iter().map(|row| f64::from(row.value)).collect();
        chart::smooth_run(&xs, &ys, Algorithm::SavitzkyGolay, 20, 0.5, 2, true)
    };
    let (old, new) = (smoothed(&samples), smoothed(&grown));
    assert!((170..185).any(|step| old[step].to_bits() != new[step].to_bits()));
    let current = [Arc::new(grown)];
    for audit in [false, true] {
        let out = checked_delta(&request, &held, &current, audit);
        assert_eq!(out.from_col, 169, "retain the preceding complete blocks, then include the changed block's interpolation predecessor");
    }
}

/// Custom x on a downsampled chart, linear and log: every gap's marker sits on the slot holding the last plottable x before it, and a run with no plottable x marks column 0.
#[test]
fn custom_x_markers_sit_on_their_anchor_slots() {
    let rows = rows_shaped(400, |i| i, |i| i);
    let mut request = req(&["a", "b", "c"], 40);
    request.x_series = Some(proto::SeriesRef {
        metric_name: "x".into(),
        ..Default::default()
    });
    // A's x skips every 50th step (a gap).
    let a_x = |s: i64| {
        if s % 50 == 25 {
            f64::NAN
        } else {
            (s * s) as f64 / 40.0 + 1.0
        }
    };
    let x_maps = std::collections::HashMap::from([
        ("a".to_string(), (0..400).map(|s| (s, a_x(s))).collect()),
        (
            "b".to_string(),
            (0..400).map(|s| (s, (s * 10 + 3) as f64)).collect(),
        ),
        ("c".to_string(), (0..400).map(|s| (s, f64::NAN)).collect()),
    ]);
    for log in [false, true] {
        request.log_buckets = log;
        let full = inflate_full(
            &build_response(
                &request,
                &[rows.clone(), rows.clone(), rows.clone()],
                Some(&x_maps),
                true,
            )
            .unwrap(),
        );
        assert!(
            full.xr_min.iter().any(|v| v.is_finite()),
            "log={log}: downsampled"
        );
        let mut expected: Vec<u32> = (0..400)
            .filter(|s| s % 50 == 25)
            .map(|s| slot_holding(&full, a_x(s - 1)) as u32)
            .collect();
        expected.dedup();
        let a = &full.series[0];
        assert_eq!(a.nan_indices, expected, "log={log}");
        assert!(a.nan_kinds.iter().all(|&k| k == 4));
        assert_eq!(a.xnan_count, 8);
        let c = &full.series[2];
        assert_eq!((c.nan_indices.as_slice(), c.xnan_count), (&[0][..], 400));
    }
}

/// Duplicate refs pair positionally through a continuation, and a logged y-kind at the anchor keeps the slot's marker precedence over kind 4.
#[test]
fn duplicate_refs_and_logged_y_kind_precedence() {
    let rows = |points: &[(i64, i64, f32)]| {
        Arc::new(
            points
                .iter()
                .enumerate()
                .map(|(i, &(step, timestamp_ms, value))| VersionedRawPoint {
                    timestamp_ms,
                    inserted_ms: i as i64 * 10_000_000,
                    ..scalar_row(step, value)
                })
                .collect(),
        )
    };
    // (step, timestamp, value): the negative timestamp is carried to the first plottable x=5, whose y is logged NaN, so kind 4 and the y-kind share one slot.
    let series = |extra: bool, bias: f32| {
        let mut points = vec![
            (0i64, -1i64, 1.0 + bias),
            (1, 5, f32::NAN),
            (2, 10, 3.0 + bias),
        ];
        if extra {
            points.push((3, 20, 4.0 + bias));
        }
        rows(&points)
    };
    let mut request = absolute_log_request();
    request.target_resolution = 10_000;
    request.y_series = vec![request.y_series[0].clone(), request.y_series[0].clone()];
    let held = build(&request, &[series(false, 0.0), series(false, 100.0)]);
    let held_model = inflate_full(&held);
    assert_eq!(held_model.x_values, vec![5.0, 10.0]);
    for s in &held_model.series {
        assert_eq!(s.nan_indices, vec![0]);
        assert_eq!(s.nan_kinds, vec![1], "the logged y-kind beats kind 4");
        assert_eq!(s.xnan_count, 1);
    }

    let current = [series(true, 0.0), series(true, 100.0)];
    for audit in [false, true] {
        let out = checked_delta(&request, &held, &current, audit);
        assert_eq!(out.splice_from_cached, vec![0, 1]);
        assert_eq!(out.from_col, 2, "only the appended x=20 resends");
    }
}
