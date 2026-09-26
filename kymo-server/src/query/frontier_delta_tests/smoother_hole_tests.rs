use super::*;

use chart::{AGE_NEW as NEW, AGE_OLD as OLD, AGE_REACH as REACH};

#[test]
fn smoother_hole_ages_follow_finite_curve_dependencies() {
    let cases = [
        // Permanent leading and interior holes do not dirty a stable curve.
        (
            vec![f64::NAN, 1.0, f64::INFINITY, 2.0],
            vec![0; 4],
            vec![OLD; 4],
            vec![OLD; 4],
        ),
        // A third endpoint changes its predecessor, not left extrapolation.
        (
            vec![f64::NAN, 1.0, 2.0, f64::NAN, 3.0],
            vec![0; 5],
            vec![OLD, OLD, OLD, OLD, NEW],
            vec![OLD, OLD, REACH, OLD, NEW],
        ),
        // The second endpoint supplies the slope before the first endpoint.
        (
            vec![f64::NAN, 1.0, 2.0],
            vec![0; 3],
            vec![OLD, OLD, NEW],
            vec![REACH, OLD, NEW],
        ),
        // A formerly finite second endpoint can disappear after smoothing.
        (
            vec![f64::NAN, 1.0, f64::INFINITY, 3.0],
            vec![0; 4],
            vec![OLD, OLD, REACH, OLD],
            vec![REACH, OLD, REACH, OLD],
        ),
        // Deliberately stronger synthetic contract: allow a NEW hole to have supplied a held endpoint. Production lineage distinguishes held rows exactly, but conservatively treating the hole as changing keeps this rule simple.
        (
            vec![1.0, f64::NAN],
            vec![0; 2],
            vec![OLD, NEW],
            vec![REACH, NEW],
        ),
        // Logged nonfinite inputs are always masked, never curve endpoints.
        (
            vec![1.0, f64::NAN],
            vec![0, 1],
            vec![OLD, NEW],
            vec![OLD, NEW],
        ),
        (
            vec![f64::NAN, 1.0, 2.0],
            vec![1, 0, 0],
            vec![OLD, OLD, REACH],
            vec![OLD, REACH, REACH],
        ),
        // No finite curve and no changing sample means nothing needs resending.
        (
            vec![f64::NAN, f64::INFINITY],
            vec![0; 2],
            vec![OLD; 2],
            vec![OLD; 2],
        ),
    ];
    for (index, (plot, kinds, mut age, expected)) in cases.into_iter().enumerate() {
        flag_lerp_dependency(&plot, &kinds, &mut age);
        assert_eq!(age, expected, "dependency fixture {index}");
    }
}

/// Inject exceptional smoother outputs while retaining real bucketing, rounding, audit, wire encoding, and client hash-verifying splice. Ordinary f32 inputs cannot reliably force a particular fit hole. The conservative NEW histories also test a stronger age contract than production's exact reconstruction from verified lineages requires.
fn synthetic_series(
    index: usize,
    samples: &[(usize, f64, u8)],
    kinds: &[u8],
    xnan: Option<f64>,
) -> PreparedSeries {
    let age: Vec<u8> = samples.iter().map(|sample| sample.2).collect();
    let old_count = age.iter().filter(|&&a| a != NEW).count();
    let kinds = if kinds.is_empty() {
        vec![0; samples.len()]
    } else {
        kinds.to_vec()
    };
    assert_eq!(kinds.len(), samples.len());
    let raw = kinds
        .iter()
        .map(|kind| match kind {
            0 => 1.0,
            1 => f64::NAN,
            2 => f64::INFINITY,
            3 => f64::NEG_INFINITY,
            _ => panic!("invalid logged-value kind"),
        })
        .collect();
    PreparedSeries {
        identity: SeriesIdentity {
            request_index: index,
            tag: String::new(),
        },
        label: format!("series {index}"),
        run_id: format!("run {index}"),
        xs: samples.iter().map(|sample| sample.0 as f64).collect(),
        plot: samples.iter().map(|sample| sample.1).collect(),
        raw,
        kinds,
        xnan_xs: xnan.into_iter().collect(),
        xnan_count: u32::from(xnan.is_some()),
        xnan_held: xnan.is_some(),
        age,
        old_count,
        provably_held: old_count != 0,
        smoothing_plan: chart::SmoothingPlan::NoState,
    }
}

fn synthetic_chart(samples: &[(usize, f64, u8)]) -> Vec<PreparedSeries> {
    // A dense stable run fixes a width-8 step grid and all bucket centers. Thus every changed prefix in these tests comes from interpolation.
    let anchor: Vec<_> = (0..128).map(|x| (x, x as f64, OLD)).collect();
    vec![
        synthetic_series(0, &anchor, &[], None),
        synthetic_series(1, samples, &[], None),
    ]
}

fn synthetic_params() -> ChartParams {
    let mut request = req(&["anchor", "holes"], 16);
    request.smoothing = Some(proto::SmoothingConfig {
        algorithm: Algorithm::Ema as i32,
        alpha: 0.5,
        ..Default::default()
    });
    chart_params(&request)
}

fn assert_wire_continuation(
    held_samples: &[(usize, f64, u8)],
    current_samples: &[(usize, f64, u8)],
    expected_from: usize,
    prefix_is_tight: bool,
) {
    assert_prepared_wire_continuation(
        synthetic_chart(held_samples),
        synthetic_chart(current_samples),
        expected_from,
        prefix_is_tight,
    );
}

fn assert_prepared_wire_continuation(
    held: Vec<PreparedSeries>,
    mut current: Vec<PreparedSeries>,
    expected_from: usize,
    prefix_is_tight: bool,
) -> DenseChart {
    let p = synthetic_params();
    let held = respond(&held, &p);
    let full = respond(&current, &p);
    let from = plan_delta_from_col(&mut current, &p, &full);
    assert_eq!(from, expected_from);
    assert_eq!(full.x_values.len(), 16, "fixture must use envelope cells");
    assert!(prefix_matches(&held, &full, from, &current));
    if prefix_is_tight {
        assert!(
            !prefix_matches(&held, &full, from + 1, &current),
            "the next envelope value must depend on the changed endpoint"
        );
    }
    let held_wire = inflate_full(&emit_full(&held));
    let delta = to_delta(&full, from, &current);
    let spliced = splice(&held_wire, &delta);
    assert!(eq(&spliced, &inflate_full(&emit_full(&full))));
    spliced
}

#[test]
fn logged_nonfinite_markers_and_smoother_holes_share_the_wire_suffix() {
    let held = [
        (8, f64::NAN, OLD),
        (16, f64::NAN, OLD),
        (24, 1.0, OLD),
        (40, f64::NAN, OLD),
        (64, 20.0, OLD),
        (112, 4.0, OLD),
    ];
    let mut current = held;
    current[4] = (64, f64::INFINITY, REACH);
    let chart = |samples: &[(usize, f64, u8)]| {
        let mut prepared = synthetic_chart(samples);
        prepared[1] = synthetic_series(1, samples, &[1, 0, 0, 2, 0, 0], None);
        prepared
    };
    let spliced = assert_prepared_wire_continuation(chart(&held), chart(&current), 2, true);
    assert_eq!(spliced.series[1].nan_indices, vec![1, 5]);
    assert_eq!(spliced.series[1].nan_kinds, vec![1, 2]);
}

#[test]
fn stable_kind4_marker_and_smoother_hole_stay_in_the_reused_prefix() {
    let held = [
        (16, f64::NAN, OLD),
        (24, f64::NAN, OLD),
        (32, 1.0, OLD),
        (48, 2.0, OLD),
        (64, f64::INFINITY, OLD),
        (80, 5.0, OLD),
    ];
    let mut current = held.to_vec();
    current.push((112, 100.0, NEW));
    let chart = |samples: &[(usize, f64, u8)]| {
        let mut kinds = vec![0; samples.len()];
        kinds[1] = 1;
        let mut prepared = synthetic_chart(samples);
        prepared[1] = synthetic_series(1, samples, &kinds, Some(16.0));
        prepared
    };
    let spliced = assert_prepared_wire_continuation(chart(&held), chart(&current), 10, true);
    assert_eq!(spliced.series[1].nan_indices, vec![2, 3]);
    assert_eq!(spliced.series[1].nan_kinds, vec![4, 1]);
    assert_eq!(spliced.series[1].xnan_count, 1);
}

#[test]
fn permanent_smoother_holes_reuse_the_entire_wire_axis() {
    let samples = [
        (16, f64::NAN, OLD),
        (32, 1.0, OLD),
        (48, 2.0, OLD),
        (64, f64::INFINITY, OLD),
        (80, 5.0, OLD),
        (96, f64::NEG_INFINITY, OLD),
    ];
    assert_wire_continuation(&samples, &samples, 16, false);
    let all_holes = [(16, f64::NAN, OLD), (96, f64::INFINITY, OLD)];
    assert_wire_continuation(&all_holes, &all_holes, 16, false);
}

#[test]
fn endpoint_append_keeps_permanent_leading_holes_in_the_prefix() {
    let held = [
        (16, f64::NAN, OLD),
        (32, 1.0, OLD),
        (48, 2.0, OLD),
        (64, f64::INFINITY, OLD),
        (80, 5.0, OLD),
    ];
    let mut current = held.to_vec();
    current.push((112, 100.0, NEW));
    assert_wire_continuation(&held, &current, 10, true);
}

#[test]
fn removed_endpoint_reemits_its_predecessor_cell() {
    let held = [
        (16, 1.0, OLD),
        (32, 2.0, OLD),
        (64, 99.0, OLD),
        (112, 4.0, OLD),
    ];
    let mut current = held;
    current[2] = (64, f64::INFINITY, REACH);
    assert_wire_continuation(&held, &current, 4, true);
}

#[test]
fn second_endpoint_changes_reemit_leading_extrapolated_holes() {
    let held = [
        (16, f64::NAN, OLD),
        (32, 1.0, OLD),
        (64, 20.0, OLD),
        (112, 4.0, OLD),
    ];
    for output in [10.0, f64::NAN, f64::INFINITY] {
        let mut current = held;
        current[2] = (64, output, REACH);
        assert_wire_continuation(&held, &current, 2, true);
    }
}

#[test]
fn first_endpoint_addition_and_removal_reemit_leading_holes() {
    let holes = [(16, f64::NAN, OLD), (64, f64::INFINITY, OLD)];
    let added = [
        (16, f64::NAN, OLD),
        (32, 1.0, NEW),
        (64, f64::INFINITY, OLD),
    ];
    assert_wire_continuation(&holes, &added, 2, true);

    let held = [
        (16, f64::NAN, OLD),
        (32, 1.0, OLD),
        (64, 10.0, OLD),
        (112, 4.0, OLD),
    ];
    let mut removed = held;
    removed[1] = (32, f64::NAN, REACH);
    assert_wire_continuation(&held, &removed, 2, true);
}

#[test]
fn conservative_synthetic_new_hole_can_have_been_a_held_endpoint() {
    let settled = [(16, 1.0, OLD), (32, 2.0, OLD)];
    // This deliberately broader synthetic history includes a NEW endpoint in the held model, then makes its output nonfinite. It tests the conservative interpolation rule beyond the exact production membership contract.
    let held = [(16, 1.0, OLD), (32, 2.0, OLD), (64, 99.0, NEW)];
    let current = [
        (16, 1.0, OLD),
        (32, 2.0, OLD),
        (64, f64::INFINITY, NEW),
        (96, f64::NAN, NEW),
    ];
    let p = synthetic_params();
    let prepared = synthetic_chart(&current);
    let full = respond(&prepared, &p);
    assert!(prefix_matches(
        &respond(&synthetic_chart(&settled), &p),
        &full,
        8,
        &prepared,
    ));
    assert!(!prefix_matches(
        &respond(&synthetic_chart(&held), &p),
        &full,
        8,
        &prepared,
    ));
    assert_wire_continuation(&held, &current, 4, true);
}

#[test]
fn multiple_endpoint_changes_share_one_suffix_boundary() {
    let held = [
        (16, f64::NAN, OLD),
        (32, 1.0, OLD),
        (48, 2.0, OLD),
        (64, 99.0, OLD),
        (80, 4.0, OLD),
        (112, 9.0, OLD),
    ];
    let mut current = held;
    current[3] = (64, f64::INFINITY, REACH);
    current[4] = (80, 100.0, REACH);
    current[5] = (112, -100.0, NEW);
    assert_wire_continuation(&held, &current, 6, true);
}

#[test]
fn randomized_smoother_hole_histories_splice_to_the_independent_full_model() {
    // Exercise the model-stage contract directly: OLD outputs are fixed, REACH outputs may change, and NEW rows may already exist in the actual held response with different smoother outputs. Expected values come from rendering the complete held/current curves, never from a second copy of the planner.
    let p = synthetic_params();
    let outputs = [
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        -10.0,
        -1.0,
        0.0,
        1.0,
        20.0,
    ];
    for seed in 0..512u64 {
        let mut state = seed;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state >> 32
        };
        let mut held_samples = Vec::new();
        let mut current_samples = Vec::new();
        for i in 0..8 {
            let x = 16 + i * 12;
            // A settled sample keeps series membership unambiguous; the stable anchor run fixes the shared axis independently of these curves.
            let age = if i == 0 {
                OLD
            } else {
                [OLD, REACH, NEW][(next() % 3) as usize]
            };
            let current_value = outputs[(next() % outputs.len() as u64) as usize];
            current_samples.push((x, current_value, age));
            if age != NEW || next() % 2 == 0 {
                let held_value = if age == OLD {
                    current_value
                } else {
                    outputs[(next() % outputs.len() as u64) as usize]
                };
                held_samples.push((x, held_value, age));
            }
        }
        let held = respond(&synthetic_chart(&held_samples), &p);
        let mut current = synthetic_chart(&current_samples);
        let full = respond(&current, &p);
        let from = plan_delta_from_col(&mut current, &p, &full);
        assert!(
            from >= 2,
            "seed {seed}: unrelated leading columns remain reusable"
        );
        assert!(
            prefix_matches(&held, &full, from, &current),
            "seed {seed}: claimed prefix differs from actual held model"
        );
        let rebuilt = splice(
            &inflate_full(&emit_full(&held)),
            &to_delta(&full, from, &current),
        );
        assert!(
            eq(&rebuilt, &inflate_full(&emit_full(&full))),
            "seed {seed}: wire splice differs from complete current render"
        );
    }
}
