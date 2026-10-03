//! Local response-builder timings, excluded from automatic test runs.
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
    cache.note_bumps(runs.iter().copied());
    let new_inputs: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| {
            let crate::series_cache::Lookup::Stale { gen, .. } = cache.lookup(key) else {
                panic!("benchmark rows must fit in its fixture cache");
            };
            let appended = rows(100_020, 3 + i as i64)[100_000..].to_vec();
            cache
                .apply_increment(key, appended, Instant::now(), gen)
                .unwrap()
        })
        .collect();
    let response = |request: &proto::ChartRequest, rows: &[Arc<SeriesSnapshot>]| {
        super::super::build_response(request, rows, None, false).unwrap()
    };
    for algorithm in [Algorithm::None, Algorithm::Ema, Algorithm::SavitzkyGolay] {
        for zoom in [false, true] {
            let mut req = req(&runs, 500);
            if zoom {
                req.step_min = Some(99_000);
                req.target_resolution = 200; // Keep the zoom grid tier stable across the append.
            }
            req.smoothing = (algorithm != Algorithm::None).then_some(proto::SmoothingConfig {
                algorithm: algorithm as i32,
                window_size: 20,
                time_constant: std::f64::consts::LOG2_E,
                poly_order: 1,
            });
            let held = response(&req, &old_inputs);
            let mut continued = req.clone();
            continued.cache_state = echo(&held);
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
                times.sort_by(f64::total_cmp);
                let out = response(request, rows);
                assert_eq!(out.delta, mode != "first", "{algorithm:?}, zoom={zoom}, mode={mode}: benchmark must measure the intended response path");
                if mode == "unchanged" {
                    assert!(out.x_values.is_empty());
                }
                println!("BENCH algorithm={algorithm:?} range={} mode={mode} median_ms={:.4} delta={} tail={}", if zoom { "zoom" } else { "full" }, times[10], out.delta, out.x_values.len());
            }
        }
    }
}
