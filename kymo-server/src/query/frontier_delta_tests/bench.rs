//! Local measurements and the chart-output digest, excluded from automatic test runs.
use super::*;

#[test]
#[ignore = "local release benchmark; no CI timing job"]
fn response_builder_benchmark() {
    use std::hint::black_box;
    use std::time::Instant;

    let runs = ["a", "b", "c", "d", "e", "f", "g", "h"];
    let cache = crate::series_cache::SeriesCache::with_budget(64 * 1024 * 1024);
    let keys: Vec<_> = runs
        .iter()
        .map(|run| crate::series_cache::SeriesKey::new("p", *run, "loss"))
        .collect();
    let old_inputs: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| {
            cache.insert_full(
                key.clone(),
                rows(100_000, 3 + i as i64).as_ref().clone(),
                Instant::now(),
            )
        })
        .collect();
    let new_inputs: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| cache_append(&cache, key, rows(100_020, 3 + i as i64)[100_000..].to_vec()))
        .collect();
    let response = |request: &proto::ChartRequest, rows: &[Arc<SeriesSnapshot>]| {
        super::super::build_response(request, rows, None, false).unwrap()
    };
    let median = |mut times: Vec<f64>| {
        times.sort_by(f64::total_cmp);
        times[10]
    };
    for algorithm in ALGORITHMS {
        for zoom in [false, true] {
            let range = if zoom { "zoom" } else { "full" };
            let mut req = req(&runs, 500);
            if zoom {
                req.step_min = Some(99_000);
                req.target_resolution = 200; // Keep the zoom grid tier stable across the append.
            }
            req.smoothing = smoothing(algorithm, 20, std::f64::consts::LOG2_E, 1);
            let held = response(&req, &old_inputs);
            let continued = continuation(&req, &held);
            for (mode, request, rows) in [
                ("first", &req, &old_inputs),
                ("unchanged", &continued, &old_inputs),
                ("append", &continued, &new_inputs),
            ] {
                drop(black_box(response(request, rows)));
                let mut times = Vec::new();
                for _ in 0..21 {
                    let start = Instant::now();
                    let out = black_box(response(black_box(request), black_box(rows)));
                    times.push(start.elapsed().as_secs_f64() * 1000.0);
                    drop(out);
                }
                let out = response(request, rows);
                assert_eq!(out.delta, mode != "first", "{algorithm:?}, zoom={zoom}, mode={mode}: benchmark must measure the intended response path");
                if mode == "unchanged" {
                    assert!(out.x_values.is_empty());
                }
                println!("BENCH algorithm={algorithm:?} range={range} mode={mode} median_ms={:.4} delta={} tail={}", median(times), out.delta, out.x_values.len());
            }
            // Stage 1: the same unchanged poll answered from the memo its first full build recorded.
            let memo = super::super::memo::ChartMemo::new(true);
            let key = memo.key_for(&continued);
            let ControlFlow::Continue(None) = probe_memo(&memo, key, &old_inputs, false, None)
            else {
                panic!("nothing is memoized yet");
            };
            let built =
                build_chart(&continued, &old_inputs, None, false, None, &|_| {}, false).unwrap();
            settle_memo(&memo, key, &old_inputs, &built);
            let mut times = Vec::new();
            for _ in 0..21 {
                let start = Instant::now();
                // The key's hash is part of the served path: the service computes it once per poll.
                let out = black_box(probe_memo(
                    &memo,
                    memo.key_for(black_box(&continued)),
                    black_box(&old_inputs),
                    false,
                    None,
                ));
                times.push(start.elapsed().as_secs_f64() * 1000.0);
                let ControlFlow::Break(reply) = out else {
                    panic!("an identical poll must be memo-served");
                };
                assert_eq!(reply, built.response);
            }
            println!(
                "BENCH algorithm={algorithm:?} range={range} mode=memo median_ms={:.4} memo_bytes={}",
                median(times),
                memo.len_and_bytes().1
            );

            // Stage 3b: the append poll answered by advancing the held state its client's response left, as the service's `on` path does (key, lineage proof, advance).
            let mut captures = Vec::new();
            for capture in [false, true] {
                let mut times = Vec::new();
                for _ in 0..21 {
                    let start = Instant::now();
                    let built = black_box(
                        build_chart(&continued, &old_inputs, None, false, None, &|_| {}, capture)
                            .unwrap(),
                    );
                    times.push(start.elapsed().as_secs_f64() * 1000.0);
                    if capture {
                        captures.push(built.held.unwrap().unwrap());
                    }
                }
                println!(
                    "BENCH algorithm={algorithm:?} range={range} mode=unchanged_build capture={capture} median_ms={:.4}",
                    median(times)
                );
            }
            let held_state = captures.pop().unwrap();
            let truth = response(&continued, &new_inputs);
            let p = chart_params(&continued);
            let frontiers = continued.cache_state.as_ref().map(|cs| &cs.frontiers);
            let mut times = Vec::new();
            let mut advanced_bytes = 0;
            for _ in 0..21 {
                let start = Instant::now();
                black_box(held::request_key(&continued));
                let lineages = inspect_lineages(&continued, &new_inputs, frontiers);
                let advanced = black_box(
                    held::advance(&held_state, &continued, &p, &new_inputs, &lineages).unwrap(),
                );
                times.push(start.elapsed().as_secs_f64() * 1000.0);
                assert_eq!(
                    advanced.response, truth,
                    "{algorithm:?}, zoom={zoom}: the advance must answer what the full build does"
                );
                advanced_bytes = advanced.next.bytes();
            }
            println!(
                "BENCH algorithm={algorithm:?} range={range} mode=held_append median_ms={:.4} held_bytes={} next_bytes={advanced_bytes}",
                median(times),
                held_state.bytes()
            );
            // Shadow mode: the full build with capture, the advance, and their comparison.
            let mut times = Vec::new();
            for _ in 0..21 {
                let start = Instant::now();
                let built =
                    build_chart(&continued, &new_inputs, None, false, None, &|_| {}, true).unwrap();
                let lineages = inspect_lineages(&continued, &new_inputs, frontiers);
                let advanced =
                    held::advance(&held_state, &continued, &p, &new_inputs, &lineages).unwrap();
                let reference = built.held.as_ref().unwrap().as_ref().unwrap();
                let same = advanced.response == built.response
                    && advanced.next.state_difference(reference).is_none();
                times.push(start.elapsed().as_secs_f64() * 1000.0);
                assert!(same);
            }
            println!(
                "BENCH algorithm={algorithm:?} range={range} mode=shadow_append median_ms={:.4}",
                median(times)
            );
        }
    }
}

/// A digest of every chart output over a fixed corpus: first paints, deltas against held replies (audited and not), unchanged polls, late rows and a rewrite, on snapshots the series cache built through refreshes. Lineage keys are process-keyed and dropped; everything else is hashed. A storage change of the cache must print the same digest before and after.
#[test]
#[ignore = "local cross-version check; prints a digest of chart outputs"]
fn chart_output_digest() {
    use crate::series_cache::{Lookup, SeriesCache, SeriesKey};
    use prost::Message;
    use sha2::{Digest, Sha256};
    use std::time::Instant;

    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    // Per run: its tags ('' alongside tags is dropped by prepare) and, per tag, (next step, last timestamp).
    let runs: [(&str, &[&str]); 3] = [("a", &[""]), ("b", &["", "g0", "g1"]), ("c", &[""])];
    let mut cursors: Vec<Vec<(i64, i64)>> = runs
        .iter()
        .map(|(_, tags)| vec![(0, 1_000_000); tags.len()])
        .collect();
    // Every row ever written, per run, in write order.
    let mut history: Vec<Vec<VersionedRawPoint>> = vec![Vec::new(); runs.len()];
    let mut write_tick = |tick: i64, stamp: i64, history: &mut Vec<Vec<VersionedRawPoint>>| {
        for (run, (_, tags)) in runs.iter().enumerate() {
            for (tag_index, tag) in tags.iter().enumerate() {
                let r = rng.next();
                if r.is_multiple_of(5) {
                    continue; // tags advance unevenly
                }
                let (step, timestamp) = &mut cursors[run][tag_index];
                *step += 1 + (r >> 8) as i64 % 3;
                *timestamp += if r.is_multiple_of(11) {
                    0
                } else {
                    7 + (r >> 16) as i64 % 40
                };
                let value = match (r >> 24) % 60 {
                    0 => f32::NAN,
                    1 => f32::INFINITY,
                    2 => f32::NEG_INFINITY,
                    _ => ((r >> 32) % 100_000) as f32 / 37.0 - 900.0 + (tick as f32).sqrt(),
                };
                history[run].push(VersionedRawPoint {
                    tag: (*tag).to_string(),
                    step: *step,
                    timestamp_ms: *timestamp,
                    value,
                    is_value: 1,
                    inserted_ms: stamp,
                });
            }
        }
    };

    let mut requests = Vec::new();
    for (index, algorithm) in ALGORITHMS.into_iter().enumerate() {
        for axis in 0..3 {
            for zoom in [false, true] {
                for log in [false, true] {
                    let mut request = req(&["a", "b", "c"], [300, 1000, 0][(index + axis) % 3]);
                    request.use_timestamp_axis = axis > 0;
                    request.relative_time = axis == 2;
                    request.log_buckets = log;
                    if zoom {
                        request.step_min = Some(400);
                        request.step_max = Some(4_000);
                    }
                    if (index + axis) % 2 == 1 {
                        request.y_series[1].tags = vec!["g1".into()];
                    }
                    request.smoothing = smoothing(algorithm, 9, 3.0, 2);
                    requests.push(request);
                }
            }
        }
    }

    let cache = SeriesCache::with_budget(usize::MAX);
    let keys: Vec<_> = runs
        .iter()
        .map(|(run, _)| SeriesKey::new("p", *run, "loss"))
        .collect();
    let mut tick = 0i64;
    let mut stamp = 1_000i64;
    while tick < 1_500 {
        write_tick(tick, stamp, &mut history);
        tick += 1;
        stamp += 10;
    }
    let mut snapshots: Vec<Arc<SeriesSnapshot>> = keys
        .iter()
        .zip(&history)
        .map(|(key, rows)| cache.insert_full(key.clone(), sorted(rows), Instant::now()))
        .collect();

    let mut hasher = Sha256::new();
    let (mut responses, mut deltas, mut unchanged) = (0usize, 0usize, 0usize);
    let mut digest = |response: &proto::ChartResponse| {
        deltas += usize::from(response.delta);
        unchanged += usize::from(response.delta && response.x_values.is_empty());
        let mut response = response.clone();
        let frontiers: std::collections::BTreeMap<String, i64> = response
            .frontiers
            .drain()
            .filter(|(key, _)| !key.starts_with(LINEAGE_PREFIX) && key != LINEAGE_VERSION_KEY)
            .collect();
        hasher.update(response.encode_to_vec());
        for (key, value) in frontiers {
            hasher.update(key.as_bytes());
            hasher.update(value.to_le_bytes());
        }
        responses += 1;
    };
    let build = |request: &proto::ChartRequest, rows: &[Arc<SeriesSnapshot>], audit: bool| {
        super::super::build_response(request, rows, None, audit).unwrap()
    };
    let first: Vec<_> = requests
        .iter()
        .map(|request| build(request, &snapshots, false))
        .collect();
    first.iter().for_each(&mut digest);
    let mut previous = first.clone();
    // Refreshes: appends of uneven size re-reading their overlap, one late batch (equal stamps rotate the lineage), one rewrite (refused and reloaded in full).
    for (round, ticks) in [40i64, 3, 250, 1, 90, 17, 600, 5].into_iter().enumerate() {
        let late = round == 3;
        let rewrite = round == 5;
        let written: Vec<usize> = history.iter().map(Vec::len).collect();
        if late {
            stamp -= 10;
        }
        for _ in 0..ticks {
            write_tick(tick, stamp, &mut history);
            tick += 1;
            stamp += 10;
        }
        if rewrite {
            // A resumed run re-logs one held step of run a with a new value.
            let held = &mut history[0][written[0] / 2];
            held.value += 1.0;
            held.inserted_ms = stamp;
            stamp += 10;
        }
        cache.note_bumps(runs.iter().map(|(run, _)| *run));
        for (index, key) in keys.iter().enumerate() {
            let Lookup::Stale { base, .. } = cache.lookup(key) else {
                panic!("a bumped series must refresh");
            };
            // The re-read overlap: the last few held rows come back with the new ones.
            let overlap = written[index].saturating_sub(4);
            let mut increment: Vec<_> = history[index][overlap..].to_vec();
            if rewrite && index == 0 {
                increment.push(history[0][written[0] / 2].clone());
            }
            snapshots[index] =
                match cache.apply_increment(key, sorted(&increment), Instant::now(), &base) {
                    Ok(rows) => rows,
                    Err(()) => cache.insert_full_with_origin(
                        key.clone(),
                        sorted(&history[index]),
                        Instant::now(),
                        LineageOrigin::Rewrite,
                    ),
                };
        }
        let mut current = Vec::new();
        for (index, request) in requests.iter().enumerate() {
            let full = build(request, &snapshots, false);
            digest(&full);
            for audit in [false, true] {
                digest(&build(
                    &continuation(request, &first[index]),
                    &snapshots,
                    audit,
                ));
            }
            digest(&build(
                &continuation(request, &previous[index]),
                &snapshots,
                false,
            ));
            digest(&build(&continuation(request, &full), &snapshots, true));
            current.push(full);
        }
        previous = current;
    }
    println!(
        "DIGEST chart outputs responses={responses} deltas={deltas} unchanged={unchanged} sha256={:x}",
        hasher.finalize()
    );
}

/// What capturing held state adds to a full build on a timestamp axis (8 runs × 200k rows, an unchanged continuation poll), per smoother.
#[test]
#[ignore = "local release benchmark; no CI timing job"]
fn held_capture_cost_on_time_axes() {
    use std::hint::black_box;
    use std::time::Instant;

    let runs = ["a", "b", "c", "d", "e", "f", "g", "h"];
    let snapshots: Vec<Arc<SeriesSnapshot>> = (0..runs.len())
        .map(|i| {
            Arc::new(SeriesSnapshot::full(
                rows(200_000, 3 + i as i64).as_ref().clone(),
            ))
        })
        .collect();
    for algorithm in [Algorithm::None, Algorithm::Ema, Algorithm::SavitzkyGolay] {
        let mut req = req(&runs, 500);
        req.use_timestamp_axis = true;
        req.smoothing = smoothing(algorithm, 20, 10.0, 1);
        let first = build_chart(&req, &snapshots, None, false, None, &|_| {}, false).unwrap();
        let continued = continuation(&req, &first.response);
        let mut medians = Vec::new();
        for capture in [false, true, false, true] {
            let mut times = Vec::new();
            for _ in 0..11 {
                let start = Instant::now();
                let built = black_box(
                    build_chart(&continued, &snapshots, None, false, None, &|_| {}, capture)
                        .unwrap(),
                );
                times.push(start.elapsed().as_secs_f64() * 1000.0);
                drop(built);
            }
            times.sort_by(f64::total_cmp);
            medians.push(times[5]);
        }
        println!(
            "BENCH capture time-axis algorithm={algorithm:?} off_ms={:.2}/{:.2} capture_ms={:.2}/{:.2}",
            medians[0], medians[2], medians[1], medians[3]
        );
    }
}

/// Held-state bytes for a prod-average chart (12 runs of 26k rows) and a heavy one (16 runs of 1M rows, a 2,000-column physical-pixel panel), per smoother.
#[test]
#[ignore = "local measurement; prints held-state sizes"]
fn held_state_bytes() {
    for (runs, rows_per_run, target) in [(12usize, 26_000i64, 1_000u32), (16, 1_000_000, 2_000)] {
        let names: Vec<String> = (0..runs).map(|i| format!("r{i}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let snapshots: Vec<Arc<SeriesSnapshot>> = (0..runs)
            .map(|i| {
                Arc::new(SeriesSnapshot::full(
                    rows(rows_per_run, 3 + i as i64).as_ref().clone(),
                ))
            })
            .collect();
        for algorithm in [Algorithm::None, Algorithm::Ema, Algorithm::SavitzkyGolay] {
            let mut req = req(&refs, target);
            req.smoothing = smoothing(algorithm, 20, 10.0, 1);
            let (held, _) = super::held_tests::continuation_state(&req, &snapshots);
            println!(
                "HELD BYTES runs={runs} rows={rows_per_run} target={target} algorithm={algorithm:?} columns={} bytes={}",
                held.model().x_values.len(),
                held.bytes()
            );
        }
    }
}
