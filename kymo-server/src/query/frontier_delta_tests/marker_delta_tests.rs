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
                ..Default::default()
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

#[test]
fn other_series_envelope_growth_moves_marker_before_the_changed_cell() {
    // A's negative timestamp and x=8 samples are held. B's append changes the envelope center; only the marker dependency invalidates the earlier raw x=6.
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
    assert_eq!(truth.series[0].nan_indices, vec![1]);
    assert_eq!(truth.series[0].nan_kinds, vec![4]);
    for audit in [false, true] {
        let out = checked_delta(&request, &held, &current, audit);
        assert_eq!(out.from_col, 1, "the preceding marker slot must be resent");
    }
}

#[test]
fn raw_to_envelope_transition_moves_marker_into_the_preceding_cell() {
    let a = timestamp_rows(&[-1, 8, 8]);
    // The [16,32) envelope exists on both sides, preserving the chart's emission shape. The fourth distinct x in [8,16) replaces its three raw slots with center 11.5, so A's x=8 marker moves backward to x=6. The final duplicate in [16,32) leaves all affected raw slots settled.
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
    assert_eq!(truth.series[0].nan_indices, vec![1]);
    assert_eq!(truth.series[0].nan_kinds, vec![4]);
    for audit in [false, true] {
        let out = checked_delta(&request, &held, &current, audit);
        assert_eq!(out.from_col, 1, "mode changes also bound marker placement");
    }
}

#[test]
fn earlier_positive_timestamp_changes_the_held_markers_carried_anchor() {
    // A's settled sentinel was carried at its first positive timestamp 10. A later step arrives with timestamp 8, moving that carried position. The planner must compare current anchor x=8 with held anchor x=10, even though the negative sentinel itself is unchanged.
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
        assert_eq!(
            out.from_col, 1,
            "keep the safe slot before the anchor range"
        );
    }
}

#[test]
fn multiple_marker_dependencies_choose_the_earliest_bound_in_either_order() {
    // A's held marker stays at x=4. B's marker moves to x=6. Only B tightens the numeric bound, regardless of series order.
    let a = timestamp_rows(&[4, 4, -1]);
    let b = timestamp_rows(&[-1, 8, 8]);
    let held_axis = timestamp_rows(&[1, 4, 6, 8, 9, 10, 11, 11]);
    let grown_axis = timestamp_rows(&[1, 4, 6, 8, 9, 10, 11, 11, 15]);
    for (runs, markers, expected_from) in [
        (vec!["a", "axis"], vec![a.clone()], 3),
        (vec!["b", "axis"], vec![b.clone()], 2),
        (vec!["a", "b", "axis"], vec![a.clone(), b.clone()], 2),
        (vec!["b", "a", "axis"], vec![b.clone(), a.clone()], 2),
    ] {
        let mut request = req(&runs, 1);
        request.use_timestamp_axis = true;
        request.log_buckets = true;
        let mut held_rows = markers.clone();
        held_rows.push(held_axis.clone());
        let held = build(&request, &held_rows);
        assert_eq!(inflate_full(&held).x_values, vec![1.0, 4.0, 6.0, 9.5]);
        let mut current = markers;
        current.push(grown_axis.clone());
        for audit in [false, true] {
            let out = checked_delta(&request, &held, &current, audit);
            assert_eq!(out.from_col, expected_from, "series order {runs:?}");
        }
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
    assert!(held_model.series[0].nan_indices.is_empty());

    for audit in [false, true] {
        let out = checked_delta(&request, &held, &current, audit);
        assert_eq!(out.from_col, 6);
        assert_eq!(out.splice_from_cached, vec![0, 1]);
        assert_eq!(out.series[0].xnan_count, 4);
        assert!(out.series[0].nan_indices.is_empty());
    }
}

#[test]
fn first_plottable_points_seed_a_full_response_then_resume_deltas() {
    let axis = timestamp_rows(&[1, 2, 3, 4, 5, 6, 7, 8, 9]);
    let mut request = absolute_log_request();
    request.target_resolution = 10_000;
    let held = build(&request, &[timestamp_rows(&[-8, -4, -1]), axis.clone()]);
    assert!(inflate_full(&held).series[0].nan_indices.is_empty());

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
                ..Default::default()
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

#[test]
fn log_marker_bound_composes_with_a_real_causal_interpolation_dependency() {
    let marker = timestamp_rows(&[-1, 8, 16, 17]);
    let curve = |points: &[(i64, f32)]| {
        Arc::new(
            points
                .iter()
                .enumerate()
                .map(|(step, &(timestamp_ms, value))| VersionedRawPoint {
                    timestamp_ms,
                    ..scalar_row(step as i64, value)
                })
                .collect(),
        )
    };
    let held_curve = curve(&[(8, 1.0), (9, 4.0), (16, f32::NAN)]);
    let grown_curve = curve(&[(8, 1.0), (9, 4.0), (16, f32::NAN), (24, 20.0)]);
    let axis = timestamp_rows(&[1, 6, 8, 9, 10, 11, 16, 17, 18, 19, 19]);
    let mut request = req(&["marker", "curve", "axis"], 1);
    request.use_timestamp_axis = true;
    request.log_buckets = true;
    request.smoothing = Some(proto::SmoothingConfig {
        algorithm: Algorithm::Ema as i32,
        time_constant: std::f64::consts::LOG2_E,
        poly_order: 1,
        ..Default::default()
    });
    let held = build(&request, &[marker.clone(), held_curve, axis.clone()]);
    let held_model = inflate_full(&held);
    assert_eq!(held_model.x_values, vec![1.0, 6.0, 9.5, 17.5]);
    assert_eq!(held_model.series[0].nan_indices, vec![2]);

    let current = [marker, grown_curve, axis];
    let truth = inflate_full(&build(&request, &current));
    // The new endpoint changes the curve at column 2 even though all new rows occupy column 3. Once interpolation invalidates column 2, its potentially changing center requires the preceding marker slot at column 1 to join the tail.
    assert_ne!(
        held_model.series[1].values[2].to_bits(),
        truth.series[1].values[2].to_bits()
    );
    for audit in [false, true] {
        let out = checked_delta(&request, &held, &current, audit);
        assert_eq!(out.from_col, 1);
    }
}
