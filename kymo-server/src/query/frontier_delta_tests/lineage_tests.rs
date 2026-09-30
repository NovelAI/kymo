use super::*;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use prost::Message;

fn cache_append(
    cache: &crate::series_cache::SeriesCache,
    key: &crate::series_cache::SeriesKey,
    rows: Vec<VersionedRawPoint>,
) -> Arc<SeriesSnapshot> {
    cache.note_bumps(std::iter::once(key.run_id.as_str()));
    let crate::series_cache::Lookup::Stale { gen, .. } = cache.lookup(key) else {
        panic!("bumped fixture must refresh its retained snapshot");
    };
    cache
        .apply_increment(key, rows, std::time::Instant::now(), gen)
        .unwrap()
}

#[test]
fn recovered_evicted_lineages_keep_real_wire_deltas_for_every_smoother() {
    use crate::series_cache::{Lookup, SeriesCache};
    for algorithm in ALGORITHMS {
        let cache = SeriesCache::with_test_budget(1_024);
        let key = crate::series_cache::SeriesKey::new("p", "a", "loss");
        let other = crate::series_cache::SeriesKey::new("p", "other", "loss");
        let original = cache.insert_full(
            key.clone(),
            rows(8, 3).as_ref().clone(),
            std::time::Instant::now(),
        );
        let mut request = req(&["a"], 1_000);
        request.smoothing = (algorithm != Algorithm::None).then_some(proto::SmoothingConfig {
            algorithm: algorithm as i32,
            window_size: 3,
            time_constant: std::f64::consts::LOG2_E,
            poly_order: 1,
        });
        let held =
            super::super::build_response(&request, std::slice::from_ref(&original), None, false)
                .unwrap();
        let mut continued = request.clone();
        continued.cache_state = echo(&held);
        for count in [8, 9] {
            cache.insert_full(
                other.clone(),
                rows(8, 7).as_ref().clone(),
                std::time::Instant::now(),
            );
            assert!(matches!(cache.lookup(&key), Lookup::Miss));
            let reloaded = cache.insert_full(
                key.clone(),
                rows(count, 3).as_ref().clone(),
                std::time::Instant::now(),
            );
            assert_eq!(reloaded.lineage(), original.lineage());
            let truth = inflate_full(
                &super::super::build_response(
                    &request,
                    std::slice::from_ref(&reloaded),
                    None,
                    false,
                )
                .unwrap(),
            );
            for audit in [false, true] {
                let out = super::super::build_response(
                    &continued,
                    std::slice::from_ref(&reloaded),
                    None,
                    audit,
                )
                .unwrap();
                assert!(out.delta && !out.audit_failed, "{algorithm:?}, count={count}, audit={audit}: recovery must preserve a real delta");
                if count == 8 {
                    assert!(out.x_values.is_empty());
                }
                assert!(eq(&splice(&inflate_full(&held), &out), &truth));
            }
        }
    }
}

#[test]
fn actual_cache_lineages_reject_regression_and_late_visibility_then_recover() {
    use lineage::Rejection;
    let cache = crate::series_cache::SeriesCache::new();
    let key = crate::series_cache::SeriesKey::new("p", "a", "loss");
    let request = req(&["a"], 1_000);
    let first = cache.insert_full(
        key.clone(),
        rows(3, 3).as_ref().clone(),
        std::time::Instant::now(),
    );
    let appended = cache_append(&cache, &key, vec![scalar_row(3, 9.0)]);
    assert_eq!(first.lineage(), appended.lineage());
    let held = super::super::build_response(&request, std::slice::from_ref(&appended), None, true)
        .unwrap();
    let mut continued = request.clone();
    continued.cache_state = echo(&held);
    for audit in [false, true] {
        let out =
            super::super::build_response(&continued, std::slice::from_ref(&first), None, audit)
                .unwrap();
        assert!(
            !out.delta,
            "an older in-flight Arc cannot continue a newer response"
        );
        assert!(!out.audit_failed);
    }
    assert_eq!(
        inspect_lineages(
            &continued,
            std::slice::from_ref(&first),
            Some(&held.frontiers)
        )
        .rejection(),
        Some(Rejection::Digest(LineageOrigin::Miss))
    );

    let late = cache_append(
        &cache,
        &key,
        vec![VersionedRawPoint {
            inserted_ms: 30_000_000,
            ..scalar_row(4, 12.0)
        }],
    );
    assert_ne!(late.lineage(), appended.lineage());
    assert_eq!(
        inspect_lineages(
            &continued,
            std::slice::from_ref(&late),
            Some(&held.frontiers)
        )
        .rejection(),
        Some(Rejection::Digest(LineageOrigin::Late))
    );
    let full =
        super::super::build_response(&continued, std::slice::from_ref(&late), None, true).unwrap();
    assert!(!full.delta);
    assert!(!full.audit_failed);
    continued.cache_state = echo(&full);
    for audit in [false, true] {
        let unchanged =
            super::super::build_response(&continued, std::slice::from_ref(&late), None, audit)
                .unwrap();
        assert!(unchanged.delta && unchanged.x_values.is_empty());
        assert!(eq(
            &splice(&inflate_full(&full), &unchanged),
            &inflate_full(&full)
        ));
    }
}

#[test]
fn authenticated_cutoffs_and_rejection_reasons_are_distinct() {
    use lineage::Rejection;
    let request = req(&["a", "b"], 1_000);
    let held = build(&request, &[rows(10, 3), rows(10, 7)]);
    let snapshots = &held.snapshots;
    assert_eq!(
        inspect_lineages(&request, snapshots, None).rejection(),
        None
    );
    let mut altered = held.frontiers.clone();
    let key = altered
        .keys()
        .find(|key| key.starts_with(LINEAGE_PREFIX))
        .unwrap()
        .clone();
    *altered.get_mut(&key).unwrap() -= 1;
    assert_eq!(
        inspect_lineages(&request, snapshots, Some(&altered)).rejection(),
        Some(Rejection::Digest(LineageOrigin::Miss))
    );
    let reordered = req(&["b", "a"], 1_000);
    assert_eq!(
        inspect_lineages(
            &reordered,
            &[snapshots[1].clone(), snapshots[0].clone()],
            Some(&held.frontiers)
        )
        .rejection(),
        Some(Rejection::Order)
    );
}

#[test]
fn lineage_rejections_are_counted_once_even_when_no_chart_points_remain() {
    let request = req(&["a"], 1_000);
    let old = Arc::new(SeriesSnapshot::full(rows(3, 3).as_ref().clone()));
    let held = super::super::build_response(&request, &[old], None, false).unwrap();
    let mut continued = request;
    continued.cache_state = echo(&held);
    for origin in [
        LineageOrigin::Miss,
        LineageOrigin::Rewrite,
        LineageOrigin::Late,
    ] {
        for remaining in [0, 3] {
            let current = Arc::new(SeriesSnapshot::full_with_origin(
                rows(remaining, 3).as_ref().clone(),
                origin,
            ));
            let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();
            let out = metrics::with_local_recorder(&recorder, || {
                super::super::build_response(&continued, &[current], None, false).unwrap()
            });
            assert!(!out.delta && !out.audit_failed);
            let rendered = handle.render();
            let expected = format!(
                "mkdb2_chart_delta_total{{kind=\"{}\"}} 1",
                Rejection::Digest(origin).label()
            );
            assert!(
                rendered.lines().any(|line| line == expected),
                "remaining={remaining}: rejection must be counted exactly once: {rendered}"
            );
            assert!(
                !rendered.contains("kind=\"gated\""),
                "lineage rejection must not also count as a generic gate"
            );
        }
    }
}

#[test]
fn held_timestamp_rewrites_are_detected_for_every_smoother() {
    let a = Arc::new(vec![
        VersionedRawPoint {
            timestamp_ms: 64,
            ..scalar_row(0, 1.0)
        },
        VersionedRawPoint {
            timestamp_ms: 74,
            ..scalar_row(1, 2.0)
        },
        VersionedRawPoint {
            timestamp_ms: 84,
            ..scalar_row(2, 3.0)
        },
        VersionedRawPoint {
            timestamp_ms: 1,
            ..scalar_row(3, 42.0)
        },
    ]);
    let b = rows_shaped(
        100,
        |i| i,
        |i| match i {
            0 => 1,
            1 => 5,
            _ => 100 + (i - 2) * 10,
        },
    );
    for algorithm in ALGORITHMS {
        let mut request = req(&["a", "b"], 1_000);
        request.use_timestamp_axis = true;
        request.log_buckets = true;
        request.smoothing = (algorithm != Algorithm::None).then_some(proto::SmoothingConfig {
            algorithm: algorithm as i32,
            window_size: 3,
            time_constant: std::f64::consts::LOG2_E,
            poly_order: 1,
        });
        let held = build(&request, &[a.clone(), b.clone()]);
        assert_eq!(inflate_full(&held).series[0].values[0], 42.0);
        for timestamp in [-1, 5, 90] {
            for same_version in [false, true] {
                let mut changed = a.as_ref().clone();
                changed[3].timestamp_ms = timestamp;
                if !same_version {
                    changed[3].inserted_ms = 40_000_000;
                }
                let current = [Arc::new(changed), b.clone()];
                assert!(verified_inputs(&request, &current, &held.frontiers).is_none());
                assert_full_response(
                    &request,
                    &held,
                    &current,
                    Some(Rejection::Digest(LineageOrigin::Rewrite)),
                );
            }
        }
    }
}

#[test]
fn same_millisecond_value_rewrites_are_not_mistaken_for_identical_versions() {
    let request = req(&["a"], 1_000);
    let rows = rows(20, 3);
    let held = build(&request, std::slice::from_ref(&rows));
    for replacement in [999.0, f32::NAN, f32::INFINITY] {
        let mut changed = rows.as_ref().clone();
        changed.last_mut().unwrap().value = replacement;
        let current = [Arc::new(changed)];
        assert!(verified_inputs(&request, &current, &held.frontiers).is_none());
        assert_full_response(
            &request,
            &held,
            &current,
            Some(Rejection::Digest(LineageOrigin::Rewrite)),
        );
    }
}

#[test]
fn rewritten_warmup_positions_cannot_hide_a_changed_curve() {
    let mut original: Vec<_> = (0..50)
        .map(|step| VersionedRawPoint {
            timestamp_ms: step + if step == 10 { 4 } else { 0 },
            ..scalar_row(step, step as f32)
        })
        .collect();
    original.push(VersionedRawPoint {
        timestamp_ms: 30,
        ..scalar_row(50, 1_000.0)
    });
    let mut request = req(&["a"], 1_000);
    request.use_timestamp_axis = true;
    request.log_buckets = true;
    request.step_min = Some(20);
    request.step_max = Some(40);
    request.smoothing = Some(proto::SmoothingConfig {
        algorithm: Algorithm::SavitzkyGolay as i32,
        window_size: 3,
        poly_order: 1,
        ..Default::default()
    });
    let held = build(&request, &[Arc::new(original.clone())]);
    for timestamp in [-1, 60] {
        let mut changed = original.clone();
        changed[50].timestamp_ms = timestamp;
        changed[50].inserted_ms = 600_000_000;
        let current = [Arc::new(changed)];
        let truth = inflate_full(&build(&request, &current));
        assert!(inflate_full(&held).series[0]
            .values
            .iter()
            .zip(&truth.series[0].values)
            .any(|(a, b)| a.to_bits() != b.to_bits()));
        assert_full_response(
            &request,
            &held,
            &current,
            Some(Rejection::Digest(LineageOrigin::Rewrite)),
        );
    }
}

#[test]
fn missing_or_corrupt_lineage_state_seeds_a_full_response_then_recovers() {
    let request = req(&["a"], 300);
    let held = build(&request, &[rows(2_000, 3)]);
    let current = [rows(2_010, 3)];
    let current_snapshots = [Arc::new(held.snapshots[0].refreshed_fixture(&current[0]))];
    let truth = inflate_full(
        &super::super::build_response(&request, &current_snapshots, None, false).unwrap(),
    );
    let id = lineage_id(&request, 0);
    let key = held
        .frontiers
        .keys()
        .find(|key| key.starts_with(LINEAGE_PREFIX))
        .unwrap()
        .clone();
    let stamp = *parse_lineages(&held.frontiers)
        .unwrap()
        .get(id.as_str())
        .unwrap();
    for corruption in 0..7 {
        let mut legacy = held.clone();
        match corruption {
            0 => {
                legacy.frontiers.remove(LINEAGE_VERSION_KEY);
            }
            1 => {
                legacy.frontiers.insert(LINEAGE_VERSION_KEY.into(), 99);
            }
            2 => {
                legacy.frontiers.remove(&key);
                legacy
                    .frontiers
                    .insert(format!("{key}:extra"), stamp.maximum);
            }
            3 => {
                legacy.frontiers.remove(&key);
                let digest = URL_SAFE_NO_PAD.encode((stamp.digest ^ 1).to_le_bytes());
                legacy
                    .frontiers
                    .insert(format!("{LINEAGE_PREFIX}{id}:{digest}:0"), stamp.maximum);
            }
            4 => {
                legacy.frontiers.remove(&key);
                let digest = URL_SAFE_NO_PAD.encode(stamp.digest.to_le_bytes());
                legacy
                    .frontiers
                    .insert(format!("{LINEAGE_PREFIX}{id}:{digest}:-1"), stamp.maximum);
            }
            5 => {
                let digest = URL_SAFE_NO_PAD.encode(stamp.digest.to_le_bytes());
                legacy
                    .frontiers
                    .insert(format!("{LINEAGE_PREFIX}{id}:{digest}:1"), stamp.maximum);
            }
            _ => {
                let version = legacy.frontiers.remove(LINEAGE_VERSION_KEY).unwrap();
                legacy
                    .frontiers
                    .insert("\0kymo:row-snapshot-version".into(), version);
            }
        }
        let mut continued = request.clone();
        continued.cache_state = echo(&legacy);
        let expected = if corruption == 3 {
            Rejection::Digest(LineageOrigin::Miss)
        } else {
            Rejection::State
        };
        assert_eq!(
            inspect_lineages(&continued, &current_snapshots, Some(&legacy.frontiers)).rejection(),
            Some(expected)
        );
        for audit in [false, true] {
            let full =
                super::super::build_response(&continued, &current_snapshots, None, audit).unwrap();
            assert!(!full.delta && !full.audit_failed);
            assert!(eq(&inflate_full(&full), &truth));
        }
        let repaired =
            super::super::build_response(&continued, &current_snapshots, None, true).unwrap();
        continued.cache_state = echo(&repaired);
        let unchanged =
            super::super::build_response(&continued, &current_snapshots, None, true).unwrap();
        assert!(unchanged.delta);
        assert!(unchanged.x_values.is_empty());
        assert!(eq(
            &splice(&inflate_full(&repaired), &unchanged),
            &inflate_full(&repaired)
        ));
    }
}

#[test]
fn duplicate_refs_keep_distinct_observed_snapshots() {
    let request = req(&["a", "a"], 1_000);
    let held = build(&request, &[rows(5, 3), rows(6, 3)]);
    assert_ne!(lineage_id(&request, 0), lineage_id(&request, 1));
    assert_eq!(
        stamped_cutoff(&request, 0, &held.frontiers),
        Some(40_000_000)
    );
    assert_eq!(
        stamped_cutoff(&request, 1, &held.frontiers),
        Some(50_000_000)
    );
    let current = [rows(7, 3), rows(7, 3)];
    for audit in [false, true] {
        let out = checked_delta(&request, &held, &current, audit);
        assert_eq!(out.from_col, 5);
        assert_eq!(out.splice_from_cached, vec![0, 1]);
    }
}

#[test]
fn reordered_shared_refs_do_not_reuse_positional_prefixes() {
    let held_request = req(&["a", "b"], 300);
    let held = build(&held_request, &[rows(2_000, 3), rows(2_000, 7)]);
    let reordered = req(&["b", "a"], 300);
    let current = [rows(2_010, 7), rows(2_010, 3)];
    assert!(verified_inputs(&reordered, &current, &held.frontiers).is_none());
    assert_full_response(&reordered, &held, &current, Some(Rejection::Order));
}

#[test]
fn backfill_starts_a_new_lineage_including_relative_time_for_every_smoother() {
    for relative in [false, true] {
        for algorithm in ALGORITHMS {
            let mut request = req(&["a"], 1_000);
            request.use_timestamp_axis = relative;
            request.relative_time = relative;
            request.smoothing = (algorithm != Algorithm::None).then_some(proto::SmoothingConfig {
                algorithm: algorithm as i32,
                window_size: 3,
                time_constant: std::f64::consts::LOG2_E,
                poly_order: 1,
            });
            let old = rows_at(&[(2, 1.0), (3, 3.0), (4, 4.0)]);
            let held = build(&request, std::slice::from_ref(&old));
            let mut changed = old.as_ref().clone();
            changed.insert(
                0,
                VersionedRawPoint {
                    inserted_ms: 50_000_000,
                    timestamp_ms: old[0].timestamp_ms - 10,
                    ..scalar_row(1, 2.0)
                },
            );
            assert_full_response(
                &request,
                &held,
                &[Arc::new(changed)],
                Some(Rejection::Digest(LineageOrigin::Rewrite)),
            );
        }
    }
}

#[test]
fn numeric_tombstone_cannot_continue_the_held_chart() {
    let request = req(&["a", "b"], 1_000);
    let a = rows(4, 3);
    let b = rows(100, 7);
    let held = build(&request, &[a.clone(), b.clone()]);
    // An authoritative numeric reload omits the row replaced by a nonnumeric payload.
    assert_full_response(
        &request,
        &held,
        &[Arc::new(a[..3].to_vec()), b],
        Some(Rejection::Digest(LineageOrigin::Rewrite)),
    );
}

#[test]
#[should_panic(expected = "echoed fixture lineage must remain owned")]
fn a_fixture_must_keep_its_echoed_lineage_alive() {
    let request = req(&["a"], 1_000);
    let rows = [rows(10, 3)];
    let mut continued = request.clone();
    {
        let held = build(&request, &rows);
        continued.cache_state = echo(&held);
    }
    build(&continued, &rows);
}

#[test]
fn unchanged_lineage_then_rewrite_still_fails_closed() {
    let request = req(&["a"], 1_000);
    let original = rows(100, 3);
    let held = build(&request, std::slice::from_ref(&original));
    let mut continued = request.clone();
    continued.cache_state = echo(&held);
    let unchanged = build(&continued, std::slice::from_ref(&original));
    assert!(unchanged.delta);
    assert!(unchanged.x_values.is_empty());
    let rebuilt = splice(&inflate_full(&held), &unchanged);
    let mut next_held = emit_full(&rebuilt);
    next_held.frontiers = unchanged.frontiers.clone();
    let mut rewritten = original.as_ref().clone();
    rewritten[99].value = 1_234.0;
    rewritten[99].inserted_ms += 1;
    assert_full_response(
        &request,
        &next_held,
        &[Arc::new(rewritten)],
        Some(Rejection::Digest(LineageOrigin::Rewrite)),
    );
}

#[test]
fn cache_lineage_covers_warmup_and_conservatively_rejects_outside_rewrites() {
    let mut request = req(&["a"], 1_000);
    request.step_min = Some(100);
    request.step_max = Some(200);
    request.smoothing = Some(proto::SmoothingConfig {
        algorithm: Algorithm::Ema as i32,
        time_constant: std::f64::consts::LOG2_E,
        ..Default::default()
    });
    let p = chart_params(&request);
    assert!(p.fetch_min > 0 && p.fetch_min < 100);
    let original = rows(250, 3);
    let held = build(&request, std::slice::from_ref(&original));
    let mut outside = original.as_ref().clone();
    outside[0].value = 999.0;
    outside[0].inserted_ms += 1;
    outside[249].value = 999.0;
    outside[249].inserted_ms += 1;
    let mut continued = request.clone();
    continued.cache_state = echo(&held);
    let out = build(&continued, &[Arc::new(outside)]);
    // Cache lineage is series-wide: even an out-of-range rewrite reseeds one full response.
    assert!(!out.delta);
    assert!(eq(&inflate_full(&out), &inflate_full(&held)));
    let mut warmup = original.as_ref().clone();
    warmup[(p.fetch_min + 1) as usize].value = 999.0;
    warmup[(p.fetch_min + 1) as usize].inserted_ms += 1;
    assert_full_response(
        &request,
        &held,
        &[Arc::new(warmup)],
        Some(Rejection::Digest(LineageOrigin::Rewrite)),
    );
}

#[test]
fn changed_render_parameters_cannot_reuse_an_old_lineage() {
    let request = req(&["a"], 1_000);
    let rows = [rows(100, 3)];
    let held = build(&request, &rows);
    let mut changed = request.clone();
    changed.log_buckets = true;
    assert_full_response(&changed, &held, &rows, None);
    changed = request.clone();
    changed.smoothing = Some(proto::SmoothingConfig {
        algorithm: Algorithm::Ema as i32,
        time_constant: std::f64::consts::LOG2_E,
        ..Default::default()
    });
    assert_full_response(&changed, &held, &rows, None);
}

#[test]
fn lineage_state_is_compact_and_older_servers_have_no_watermarks() {
    let request = req(&["a"], 1_000);
    let held = build(&request, &[rows(100, 3)]);
    let state = echo(&held).unwrap();
    assert_eq!(
        state.frontiers.len(),
        2,
        "one version and one compact lineage entry"
    );
    assert!(
        state.encoded_len() <= 128,
        "lineage state uses {} bytes",
        state.encoded_len()
    );
    assert!(state.frontiers.keys().all(|key| key.starts_with('\0')));
    assert!(
        !state.frontiers.contains_key("f\0a\0loss"),
        "older servers must see no held data key and answer in full"
    );
}
