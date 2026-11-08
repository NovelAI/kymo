//! Stage 3b (docs/chart-request-latency.md): held chart state, advanced poll after poll, answers exactly what the full build does, holds exactly the state a full build of the same inputs captures, and visits only the work its appended rows cause.
use super::*;
use crate::query::chart_admission_tests::tagged_chart;
use crate::query::held::{self, Fallback, HeldMode, HeldStore};
use crate::series_cache::{Lookup, SeriesCache, SeriesKey};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

/// A row of `tag` at `step` and `timestamp_ms`, stamped `inserted_ms`.
fn point(
    tag: &str,
    step: i64,
    timestamp_ms: i64,
    value: f32,
    inserted_ms: i64,
) -> VersionedRawPoint {
    VersionedRawPoint {
        tag: tag.into(),
        step,
        timestamp_ms,
        value,
        is_value: 1,
        inserted_ms,
    }
}

/// One tag's writer: where its next row goes.
#[derive(Clone, Default)]
struct Writer {
    tag: String,
    step: i64,
    ts: i64,
    /// Steps advance by 1 or by random gaps.
    gappy: bool,
    /// Timestamps occasionally run backwards.
    jumbled: bool,
    /// Some timestamps repeat.
    repeats: bool,
    /// Timestamps far apart, so time-axis cells hold few distinct x (raw cells, passthrough-shaped grids).
    sparse: bool,
    /// A stuck metric: every value is this non-finite one until it recovers, so long NaN/inf tails sit between finite values.
    stuck: Option<f32>,
    /// Now and then the clock stalls: a long run of rows at one timestamp, after a jump, so a raw slot or a cell holds thousands of samples at one x, and appends may extend them.
    stalls: bool,
    /// Rows left in the current stall.
    stall: usize,
}

/// The fuzzed database: every row ever written per run, keyed (tag, step), and the series cache reading it.
struct World {
    rng: Rng,
    runs: Vec<String>,
    writers: Vec<Vec<Writer>>,
    db: Vec<BTreeMap<(String, i64), VersionedRawPoint>>,
    cache: SeriesCache,
    keys: Vec<SeriesKey>,
    snapshots: Vec<Arc<SeriesSnapshot>>,
    stamp: i64,
    nonfinite: u64,
    /// In some cases, the generator of extreme finite values: signed zeros, the largest magnitudes and subnormals.
    extreme: Option<Rng>,
}

impl World {
    fn value(&mut self, run: usize, tag: usize) -> f32 {
        if let Some(stuck) = self.writers[run][tag].stuck {
            return stuck;
        }
        let r = self.rng.below(1000);
        if r < self.nonfinite {
            [f32::NAN, f32::INFINITY, f32::NEG_INFINITY][(r % 3) as usize]
        } else if let Some(extreme) = self.extreme.as_mut().filter(|_| r.is_multiple_of(4)) {
            extreme.pick(&[-0.0, 0.0, f32::MAX, -f32::MAX, 3e38, 1.2e-38, 1e-45, -1e-40])
        } else {
            ((self.rng.next() >> 20) % 200_000) as f32 / 37.0 - 2000.0
        }
    }

    /// Append `count` rows to one tag, stamped `stamp`.
    fn write(
        &mut self,
        run: usize,
        tag: usize,
        count: usize,
        stamp: i64,
    ) -> Vec<VersionedRawPoint> {
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            let gap = if self.writers[run][tag].gappy {
                1 + self.rng.pick(&[0i64, 0, 0, 1, 2, 7, 40])
            } else {
                1
            };
            let dt = if self.writers[run][tag].stall > 0 {
                self.writers[run][tag].stall -= 1;
                0
            } else if self.writers[run][tag].stalls && self.rng.below(2_000) == 0 {
                self.writers[run][tag].stall = self.rng.pick(&[64usize, 700, 5_000]);
                self.rng.pick(&[1i64, 4_000, 300_000])
            } else if self.writers[run][tag].repeats && self.rng.chance(15) {
                0
            } else if self.writers[run][tag].sparse {
                self.rng.pick(&[1_800i64, 2_000, 2_000, 2_200])
            } else if self.writers[run][tag].jumbled && self.rng.chance(4) {
                -(self.rng.below(30) as i64)
            } else {
                self.rng.pick(&[1i64, 7, 10, 10, 10, 13, 250])
            };
            let value = self.value(run, tag);
            let w = &mut self.writers[run][tag];
            w.step += gap;
            w.ts += dt;
            let row = point(&w.tag, w.step, w.ts, value, stamp);
            self.db[run].insert((row.tag.clone(), row.step), row.clone());
            rows.push(row);
        }
        rows
    }

    fn full(&self, run: usize) -> Vec<VersionedRawPoint> {
        self.db[run].values().cloned().collect()
    }

    /// Read every run's series through the cache, as a poll's refresh does: an increment (new rows plus a re-read overlap), a full reload when the merge refuses it, or after an eviction a full read that may recover the lineage.
    #[allow(clippy::needless_range_loop)] // Runs index several parallel vectors.
    fn refresh(&mut self, increments: &[Vec<VersionedRawPoint>]) {
        self.cache.note_bumps(self.runs.iter().map(String::as_str));
        for run in 0..self.runs.len() {
            let key = self.keys[run].clone();
            self.snapshots[run] = match self.cache.lookup(&key) {
                Lookup::Stale { base, .. } => {
                    let mut increment = increments[run].clone();
                    // The watermark overlap re-reads a few held rows unchanged.
                    let appended: HashSet<(&str, i64)> = increments[run]
                        .iter()
                        .map(|row| (row.tag.as_str(), row.step))
                        .collect();
                    let held: Vec<_> = self.db[run]
                        .values()
                        .rev()
                        .filter(|row| !appended.contains(&(row.tag.as_str(), row.step)))
                        .take(3)
                        .cloned()
                        .collect();
                    increment.extend(held);
                    increment.sort_by(|a, b| (&a.tag, a.step).cmp(&(&b.tag, b.step)));
                    increment.dedup_by(|a, b| a.tag == b.tag && a.step == b.step);
                    match self
                        .cache
                        .apply_increment(&key, increment, Instant::now(), &base)
                    {
                        Ok(rows) => rows,
                        Err(()) => self.cache.insert_full_with_origin(
                            key.clone(),
                            self.full(run),
                            Instant::now(),
                            LineageOrigin::Rewrite,
                        ),
                    }
                }
                Lookup::Miss => self
                    .cache
                    .insert_full(key.clone(), self.full(run), Instant::now()),
                Lookup::Fresh(rows) => rows,
            };
        }
    }
}

/// One chart's request knobs.
fn random_request(rng: &mut Rng, runs: &[String], tags: &[Vec<String>]) -> proto::ChartRequest {
    let mut y_series: Vec<proto::SeriesRef> = runs
        .iter()
        .zip(tags)
        .map(|(run, tags)| proto::SeriesRef {
            project_id: "p".into(),
            run_id: run.clone(),
            metric_name: "loss".into(),
            tags: if tags.len() > 2 && rng.chance(30) {
                vec![tags[1].clone(), "never".into()]
            } else {
                vec![]
            },
        })
        .collect();
    if rng.chance(8) {
        // Duplicate refs are valid and expand twice.
        y_series.push(y_series[0].clone());
    }
    let algorithm = rng.pick(&[
        Algorithm::None,
        Algorithm::Ema,
        Algorithm::Triangular,
        Algorithm::SavitzkyGolay,
        Algorithm::SavitzkyGolay,
    ]);
    let use_time = rng.chance(40);
    let mut req = proto::ChartRequest {
        y_series,
        target_resolution: rng.pick(&[0u32, 6, 12, 40, 120, 120, 300, 500, 2000]),
        use_timestamp_axis: use_time,
        relative_time: use_time && rng.chance(50),
        log_buckets: rng.chance(35),
        smoothing: (algorithm != Algorithm::None).then(|| proto::SmoothingConfig {
            algorithm: algorithm as i32,
            window_size: rng.pick(&[0u32, 3, 4, 5, 8, 9, 10, 16, 21, 33, 64, 130, 600]),
            time_constant: rng.pick(&[0.7, 3.0, 20.0, 250.0]),
            poly_order: rng.pick(&[0u32, 1, 2, 2]),
        }),
        ..Default::default()
    };
    match rng.below(6) {
        0 => req.step_min = Some(rng.pick(&[-5i64, 30, 400, 1500])),
        1 => {
            req.step_min = Some(rng.pick(&[0i64, 200, 900]));
            req.step_max = Some(req.step_min.unwrap() + rng.pick(&[60i64, 700, 4000]));
        }
        2 => req.step_max = Some(rng.pick(&[100i64, 2500, 9000])),
        _ => {}
    }
    req
}

/// The rarer request knobs, drawn from a case's own stream so the base corpus keeps its draws: a second ref on run 0 whose filter names a scalar group, a tag or one that appears later; windows above 600 (past the 4,096 cap too) while every run holds at most 1,000 rows, since irregular Savitzky-Golay's full build costs rows times the window, orders above 2 and extreme time constants; extreme resolutions; inverted zooms.
fn extend_request(ext: &mut Rng, req: &mut proto::ChartRequest, world: &World) {
    if ext.chance(15) {
        req.y_series.push(proto::SeriesRef {
            project_id: "p".into(),
            run_id: world.runs[0].clone(),
            metric_name: "loss".into(),
            tags: ext
                .pick(&[&["g1"][..], &["n3"], &["n2", "g0"], &[""]])
                .iter()
                .map(|tag| tag.to_string())
                .collect(),
        });
    }
    if let Some(smoothing) = req.smoothing.as_mut() {
        let window = ext.pick(&[1u32, 2, 1_000, 2_049, 4_096, 9_000]);
        if ext.chance(6) && world.db.iter().all(|rows| rows.len() <= 1_000) {
            smoothing.window_size = window;
        }
        if ext.chance(10) {
            smoothing.poly_order = ext.pick(&[3u32, 7, 1_000]);
        }
        if ext.chance(10) {
            smoothing.time_constant = ext.pick(&[1e-9, 1e-3, 1e5, 1e12]);
        }
    }
    if ext.chance(4) {
        req.target_resolution = ext.pick(&[1u32, 2, 3, 100_000]);
    }
    if ext.chance(4) {
        req.step_min = Some(ext.pick(&[400i64, 1_500]));
        req.step_max = Some(req.step_min.unwrap() - ext.pick(&[1i64, 5, 30]));
    }
}

/// Totals over a fuzz run.
#[derive(Default, Debug)]
struct Tally {
    cases: usize,
    polls: usize,
    served: usize,
    served_delta: usize,
    served_unchanged: usize,
    served_full: usize,
    fallbacks: BTreeMap<&'static str, usize>,
    /// Full builds whose state could not be held, by reason.
    uncaptured: BTreeMap<&'static str, usize>,
    max_rows: usize,
    max_rows_over_bound: f64,
    algorithms_served: [usize; 4],
    /// Served polls by what they exercised.
    coverage: BTreeMap<&'static str, usize>,
}

impl Tally {
    fn cover(&mut self, what: &'static str, hit: bool) {
        *self.coverage.entry(what).or_default() += usize::from(hit);
    }
}

/// The work an advance may do, in row reads plus causal samples folded: per appended row, per series (a checkpoint replay) and per recomputed slot, and the samples in the Savitzky-Golay windows of the slots it re-evaluates (`windows`), which may hold held samples (clustered timestamps) and appended ones (a stalled clock); the fuzz holds `windows` to the full build's. Nothing else scales with a series' held length but logarithmic searches.
fn work_bound(appended: usize, series: usize, slots: usize, windows: usize, n_max: usize) -> usize {
    let log = (usize::BITS - n_max.leading_zeros()) as usize + 2;
    let per_series = 4 * chart::CAUSAL_CHECKPOINT_STRIDE + 64 * log + 768;
    34 * appended + series * per_series + slots * (16 * log + 160) + 4 * windows
}

fn stuck_value(rng: &mut Rng) -> f32 {
    rng.pick(&[f32::NAN, f32::NAN, f32::INFINITY, f32::NEG_INFINITY])
}

/// One fuzz case: a chart polled poll after poll while its runs grow, its rows sometimes arrive late, get rewritten or evicted, and its request changes.
#[allow(clippy::needless_range_loop)] // Runs index several parallel vectors.
fn fuzz_case(seed: u64, polls: usize, tally: &mut Tally) {
    let mut rng = Rng(seed | 1);
    // The extensions' own stream: a case drawing none of them polls as it would without them.
    let mut ext = Rng(seed.rotate_left(29) | 1);
    let nruns = 1 + rng.below(3) as usize;
    let runs: Vec<String> = (0..nruns).map(|i| format!("r{i}")).collect();
    let mut tags: Vec<Vec<String>> = Vec::new();
    let mut writers = Vec::new();
    let negative_steps = rng.chance(20);
    // One case in ten logs steps or timestamps near 2^53 from a short history, so appends leave the range f64 holds exactly (or it starts outside it).
    let huge = rng.chance(10).then(|| {
        rng.pick(&[
            (1i64 << 53) - 400,
            (1 << 53) - 60,
            (1 << 53) - 60,
            (1 << 53) + 1,
            -(1 << 53) - 40,
        ])
    });
    // One case in four is a sparse time axis of scalar series, mostly unzoomed, at a resolution of five to seven eighths of its x, which grow a few rows per poll without stalls or repeated timestamps: grids whose cells hold under four distinct x ship passthrough-shaped columns.
    let sparse_case = rng.chance(25);
    let sparse = sparse_case || rng.chance(20);
    for _ in 0..nruns {
        let names: Vec<String> = match rng.below(5) {
            _ if sparse_case => vec!["".into()],
            0 => vec!["".into(), "g0".into(), "g1".into()],
            1 => vec!["g0".into(), "g1".into(), "g2".into()],
            _ => vec!["".into()],
        };
        let start_step = if let Some(huge) = huge.filter(|_| rng.chance(70)) {
            huge
        } else if negative_steps {
            -4
        } else {
            rng.pick(&[-1i64, -1, 0, 4])
        };
        let start_ts = if let Some(huge) = huge.filter(|_| rng.chance(50)) {
            huge
        } else {
            rng.pick(&[1_000_000i64, 1_000_000, 0, -50])
        };
        writers.push(
            names
                .iter()
                .map(|tag| Writer {
                    tag: tag.clone(),
                    step: start_step,
                    ts: start_ts,
                    gappy: rng.chance(30),
                    jumbled: rng.chance(8),
                    repeats: rng.chance(15) && !sparse_case,
                    sparse,
                    stuck: None,
                    stalls: rng.chance(15) && !sparse_case,
                    stall: 0,
                })
                .collect::<Vec<_>>(),
        );
        tags.push(names);
    }
    let mut world = World {
        rng: Rng(rng.next() | 1),
        keys: runs
            .iter()
            .map(|run| SeriesKey::new("p", run.as_str(), "loss"))
            .collect(),
        snapshots: Vec::new(),
        runs: runs.clone(),
        writers,
        db: vec![BTreeMap::new(); nruns],
        cache: SeriesCache::with_budget(usize::MAX),
        stamp: 1_000,
        nonfinite: rng.pick(&[0u64, 3, 20, 90]),
        extreme: ext.chance(15).then(|| Rng(ext.next() | 1)),
    };
    // Initial history: uneven lengths, some runs empty.
    for run in 0..nruns {
        for tag in 0..tags[run].len() {
            let len = if sparse_case {
                rng.pick(&[15usize, 30, 50, 90])
            } else if huge.is_some() {
                rng.pick(&[0usize, 1, 3, 5])
            } else {
                if rng.chance(2) {
                    // Past one full-read chunk (32,768 rows), so a tag starts as several chunks.
                    40_000
                } else {
                    rng.pick(&[0usize, 1, 3, 40, 300, 1200, 2600, 6000])
                }
            };
            let stamp = world.stamp;
            world.write(run, tag, len, stamp);
            if world.writers[run][tag].stalls && rng.chance(40) {
                // The history ends inside a stall, past a jump, which the appends continue.
                world.writers[run][tag].ts += rng.pick(&[0i64, 4_000, 300_000]);
                world.writers[run][tag].stall = rng.pick(&[64usize, 700, 5_000, 20_000]);
                let rows = rng.pick(&[64usize, 600, 3_000]);
                world.write(run, tag, rows, stamp);
            }
            if len > 0 && rng.chance(12) {
                // The history ends in a long non-finite tail, which later appends may end.
                world.writers[run][tag].stuck = Some(stuck_value(&mut rng));
                let tail = rng.pick(&[64usize, 300, 2_000, 9_000]);
                world.write(run, tag, tail, stamp);
            }
            world.stamp += 10;
        }
    }
    for run in 0..nruns {
        let rows = world.full(run);
        let key = world.keys[run].clone();
        world
            .snapshots
            .push(world.cache.insert_full(key, rows, Instant::now()));
    }

    let store = HeldStore::new(HeldMode::On, usize::MAX);
    // The service's own build step on the same polls, in `on` mode with audits or in `shadow` mode, sometimes with a budget that evicts, behind stage 1's memo in some cases, and polled by a second tab in some.
    let budget = if rng.chance(25) { 300_000 } else { usize::MAX };
    let mode = if ext.chance(25) {
        HeldMode::Shadow
    } else {
        HeldMode::On
    };
    let service = HeldStore::new(mode, budget);
    let memo = crate::query::memo::ChartMemo::new(ext.chance(40));
    let mut second: Option<Option<proto::ChartResponse>> = ext.chance(20).then_some(None);
    let sparse_request = |rng: &mut Rng, req: &mut proto::ChartRequest, world: &World| {
        if sparse_case {
            let rows: usize = world.db.iter().map(BTreeMap::len).sum();
            req.use_timestamp_axis = true;
            req.target_resolution = (rows * rng.pick(&[5usize, 6, 7]) / 8).max(2) as u32;
            if rng.chance(70) {
                (req.step_min, req.step_max) = (None, None);
            }
        }
    };
    let mut base = random_request(&mut rng, &runs, &tags);
    sparse_request(&mut rng, &mut base, &world);
    extend_request(&mut ext, &mut base, &world);
    tally.cases += 1;
    // What the client holds: the last response it received and the model it inflated or spliced from it.
    let mut client: Option<(proto::ChartResponse, DenseChart)> = None;
    for poll in 0..polls {
        // Data: appends of varied size (crossing checkpoint boundaries), sometimes late, rewritten, evicted, or in a new tag.
        let mut increments: Vec<Vec<VersionedRawPoint>> = vec![Vec::new(); nruns];
        if poll > 0 {
            let late = rng.chance(4);
            if late {
                world.stamp -= 15;
            }
            for run in 0..nruns {
                for tag in 0..world.writers[run].len() {
                    // A metric gets stuck at NaN or inf, or recovers.
                    let writer = &mut world.writers[run][tag];
                    if writer.stuck.is_some() {
                        if rng.chance(30) {
                            writer.stuck = None;
                        }
                    } else if rng.chance(3) {
                        writer.stuck = Some(stuck_value(&mut rng));
                    }
                    let count = match rng.below(20) {
                        0..=6 => 0,
                        7..=12 => 1 + rng.below(4) as usize,
                        13..=17 => 5 + rng.below(60) as usize,
                        18 => 128 + rng.below(512) as usize,
                        _ => 1 + rng.below(20) as usize,
                    };
                    let count = if sparse_case { count % 3 } else { count };
                    let stamp = world.stamp;
                    let rows = world.write(run, tag, count, stamp);
                    increments[run].extend(rows);
                }
                if rng.chance(2) && !world.db[run].is_empty() {
                    // A resumed run re-logs a held step with a new value.
                    let which = rng.below(world.db[run].len() as u64) as usize;
                    let key = world.db[run].keys().nth(which).unwrap().clone();
                    let mut row = world.db[run][&key].clone();
                    row.value += 1.0;
                    row.inserted_ms = world.stamp;
                    world.db[run].insert(key, row.clone());
                    increments[run].push(row);
                }
                if rng.chance(2) && !sparse_case {
                    // A new tag starts logging.
                    let name = format!("n{poll}");
                    let step = world.writers[run][0].step;
                    let ts = world.writers[run][0].ts;
                    world.writers[run].push(Writer {
                        tag: name,
                        step,
                        ts,
                        ..Writer::default()
                    });
                    let tag = world.writers[run].len() - 1;
                    let stamp = world.stamp;
                    let rows = world.write(run, tag, 3, stamp);
                    increments[run].extend(rows);
                }
                if rng.chance(5) {
                    world.cache.evict_for_test(&world.keys[run]);
                }
            }
            world.stamp += 10;
            if late {
                world.stamp += 30;
            }
            world.refresh(&increments);
        }
        // Requests: occasionally a new resolution or zoom (a different chart), a lost response, or a fresh tab.
        if rng.chance(4) {
            base = random_request(&mut rng, &runs, &tags);
            sparse_request(&mut rng, &mut base, &world);
            extend_request(&mut ext, &mut base, &world);
            client = None;
            if let Some(tab) = second.as_mut() {
                *tab = None;
            }
        }
        let mut req = base.clone();
        if rng.chance(5) {
            client = None;
        }
        let lose = rng.chance(6);
        req.cache_state = client.as_ref().and_then(|(held, _)| echo(held));
        if canonicalize_chart_request(&mut req).is_err() {
            continue;
        }
        let snapshots: Vec<Arc<SeriesSnapshot>> = req
            .y_series
            .iter()
            .map(|s| {
                let run = runs.iter().position(|r| *r == s.run_id).unwrap();
                Arc::clone(&world.snapshots[run])
            })
            .collect();
        tally.polls += 1;

        // Samples the Savitzky-Golay slot fits read: an advance re-evaluates some of the full build's slots, each over the same window.
        let windows = || chart::SAVGOL_READS.with(|reads| reads.replace(0));
        windows();
        let truth = match build_chart(&req, &snapshots, None, false, None, &|_| {}, true) {
            Ok(truth) => truth,
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
                continue;
            }
        };
        let captured = truth.held.as_ref().unwrap();
        if let Err(reason) = captured {
            *tally.uncaptured.entry(reason.label()).or_default() += 1;
        }

        // The service's `on` path, step by step.
        let p = chart_params(&req);
        let key = held::request_key(&req);
        let frontiers = echoed_frontiers(&req);
        let lineages = inspect_lineages(&req, &snapshots, frontiers);
        let cutoffs = lineages.verified().map(|verified| verified.cutoffs());
        let (entry, _) = store.take(&key, &snapshots, cutoffs);
        // The rows the entry has not seen: a client that lost responses advances an older entry across several polls' appends.
        let appended: usize = entry.as_ref().map_or(0, |entry| {
            snapshots
                .iter()
                .zip(entry.maxima())
                .map(|(rows, maximum)| {
                    rows.tags()
                        .flat_map(|tag| tag.chunks())
                        .flatten()
                        .filter(|row| maximum.is_none_or(|m| row.inserted_ms > m))
                        .count()
                })
                .sum()
        });
        let full_windows = windows();
        let held_even = entry.as_ref().map_or(Vec::new(), |entry| entry.shape().1);
        chart::FLOOR_GROWN.with(|grown| grown.set(0));
        let outcome =
            entry.and_then(|entry| held::advance(&entry, &req, &p, &snapshots, &lineages));
        let advance_windows = windows();
        let floor_grown = chart::FLOOR_GROWN.with(|grown| grown.get()) > 0;
        let context = || {
            format!(
                "seed {seed} poll {poll}: req={:?} kind={:?}",
                proto::ChartRequest {
                    cache_state: None,
                    ..req.clone()
                },
                truth.kind
            )
        };
        match outcome {
            Ok(advanced) => {
                assert_eq!(advanced.kind, truth.kind, "{}", context());
                assert!(
                    advanced.response == truth.response,
                    "{}: response differs\nadvanced={:?}\ntruth={:?}",
                    context(),
                    advanced.response,
                    truth.response
                );
                let captured = captured.as_ref().unwrap_or_else(|reason| {
                    panic!("{}: advanced but uncapturable ({reason:?})", context())
                });
                assert!(
                    held::same_model(advanced.next.model(), captured.model()),
                    "{}: model differs\nadvanced={:?}\ntruth={:?}",
                    context(),
                    advanced.next.model(),
                    captured.model()
                );
                if let Some(difference) = advanced.next.state_difference(captured) {
                    panic!("{}: held state differs: {difference}", context());
                }
                let n_max = snapshots.iter().map(|s| s.len()).max().unwrap_or(0);
                assert!(
                    advance_windows <= full_windows,
                    "{}: slot fits read {advance_windows} samples, the full build's {full_windows}",
                    context()
                );
                let bound = work_bound(
                    appended,
                    advanced.next.model().series.len(),
                    advanced.work.rebuild.slots,
                    advance_windows,
                    n_max,
                );
                assert!(
                    advanced.work.rows + advanced.work.smoothed <= bound,
                    "{}: work {:?} over its bound {bound}",
                    context(),
                    advanced.work
                );
                tally.max_rows = tally.max_rows.max(advanced.work.rows);
                tally.max_rows_over_bound = tally
                    .max_rows_over_bound
                    .max((advanced.work.rows + advanced.work.smoothed) as f64 / bound as f64);
                tally.served += 1;
                match advanced.kind {
                    Some("delta") => tally.served_delta += 1,
                    Some("unchanged") => tally.served_unchanged += 1,
                    _ => tally.served_full += 1,
                }
                let algorithm = req.smoothing.map_or(0, |s| s.algorithm as usize);
                tally.algorithms_served[[0, 0, 1, 0, 2, 3][algorithm.min(5)]] += 1;
                let (axis, even) = advanced.next.shape();
                let rebuild = advanced.work.rebuild;
                tally.cover(axis, true);
                tally.cover("time", req.use_timestamp_axis);
                tally.cover("relative", req.relative_time);
                tally.cover("log", req.log_buckets);
                tally.cover("zoom", req.step_min.is_some() || req.step_max.is_some());
                tally.cover("appended", appended > 0);
                tally.cover("fresh_slots", rebuild.fresh_slots > 0);
                tally.cover("inserted_slots", rebuild.inserted_slots > 0);
                tally.cover("flipped_cells", rebuild.flipped_cells > 0);
                tally.cover("moved_centers", rebuild.moved > 0);
                tally.cover("reached", rebuild.reached > 0);
                tally.cover("folded_slots", rebuild.folded >= 64);
                tally.cover("replays", advanced.work.replays > 0);
                // A median plan kept across an append that broke its even spacing, which splices.
                tally.cover(
                    "broke_even_spacing",
                    held_even.iter().zip(&even).any(|(&was, &is)| was && !is),
                );
                tally.cover("floored_ladder", floor_grown);
                tally.cover(
                    "kind4_markers",
                    advanced
                        .next
                        .model()
                        .series
                        .iter()
                        .any(|s| s.nan_kinds.contains(&4)),
                );
                tally.cover(
                    "y_markers",
                    advanced
                        .next
                        .model()
                        .series
                        .iter()
                        .any(|s| s.nan_kinds.iter().any(|&k| k < 4)),
                );
                tally.cover(
                    "tagged",
                    advanced.next.model().series.len() > req.y_series.len(),
                );
                tally.cover("first_paint", frontiers.is_none());
                store.insert(key, advanced.next);
            }
            Err(reason) => {
                *tally.fallbacks.entry(reason.label()).or_default() += 1;
                if frontiers.is_some() {
                    if let Ok(captured) = captured {
                        store.insert(key, captured.clone());
                    }
                }
            }
        }
        let audit = rng.chance(15);
        // A poll as the service answers it: stage 1's memo, then the build step, every other poll as one step 1 expected held state to answer (the advance alone, then, should it fall back, the full build in a call of its own). It must answer what the full build does, never failing an audit or diverging in shadow mode.
        let serve = |req: &proto::ChartRequest, truth: &BuiltChart, tally: &mut Tally| {
            let key = memo.key_for(req);
            let held = Some((&service, held::request_key(req)));
            held::take_outcomes();
            let (response, kind) = match probe_memo(&memo, key, &snapshots, audit, held) {
                ControlFlow::Break(reply) => {
                    tally.cover("memo_hit", true);
                    (reply, Some("unchanged"))
                }
                ControlFlow::Continue(memoized) => {
                    let build = |attempt| {
                        held::build(
                            &service,
                            req,
                            &chart_params(req),
                            Some(held::request_key(req)),
                            &snapshots,
                            None,
                            audit,
                            memoized.as_ref(),
                            &|_| {},
                            attempt,
                        )
                        .unwrap()
                    };
                    let built = if poll % 2 == 0 {
                        build(held::Advance::Try)
                    } else {
                        build(held::Advance::Only).or_else(|| build(held::Advance::Skip))
                    }
                    .unwrap();
                    assert_ne!(
                        built.memo_audit,
                        Some(false),
                        "{}: a memo audit failed",
                        context()
                    );
                    settle_memo(&memo, key, &snapshots, &built);
                    (built.response, built.kind)
                }
            };
            assert!(
                response == truth.response && kind == truth.kind,
                "{}: the service's build step ({mode:?}, audit={audit}) answered differently",
                context()
            );
            let outcomes = held::take_outcomes();
            assert!(
                service.mode() == mode && !outcomes.contains_key("shadow_divergence"),
                "{}: an audit failed or shadow mode diverged: {outcomes:?}",
                context()
            );
            tally.cover("shadow_match", outcomes.contains_key("shadow_match"));
        };
        serve(&req, &truth, tally);
        // A second tab polls the same chart with the replies it received, sharing or splitting held entries with the first.
        if let Some(tab) = second.as_mut().filter(|_| ext.chance(60)) {
            let mut other = base.clone();
            other.cache_state = tab.as_ref().and_then(echo);
            if canonicalize_chart_request(&mut other).is_ok() {
                let truth =
                    build_chart(&other, &snapshots, None, false, None, &|_| {}, false).unwrap();
                serve(&other, &truth, tally);
                tally.cover("second_tab", true);
                *tab = Some(truth.response);
            }
        }
        if !lose {
            // The client splices a delta onto its model, checking the result hashes, so a planner unsoundness the advance shares with the full build fails here too.
            let model = match &client {
                Some((_, model)) if truth.response.delta => splice(model, &truth.response),
                _ => inflate_full(&truth.response),
            };
            client = Some((truth.response.clone(), model));
        }
    }
}

fn fuzz(cases: u64, polls: usize, seed: u64) -> Tally {
    let mut tally = Tally::default();
    // Every seed its own stream but 0, which xorshift would never leave and so runs 1's (`| 1` would pair each even seed with the odd one after it).
    let mut rng = Rng(seed.max(1));
    for _ in 0..cases {
        let case_seed = rng.next();
        fuzz_case(case_seed, polls, &mut tally);
    }
    tally
}

/// The differential fuzz: every poll's advance answers bit for bit what the full build answers, holds the state a full build captures, and stays within its work bound. Set KYMO_HELD_FUZZ_CASES for a longer run.
#[test]
fn held_state_advances_bit_identically_to_the_full_build() {
    // The default corpus, whose coverage floors are asserted below.
    const CASES: u64 = 160;
    const SEED: u64 = 0x5DEECE66D;
    let cases = std::env::var("KYMO_HELD_FUZZ_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(CASES);
    let seed = std::env::var("KYMO_HELD_FUZZ_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(SEED);
    if let Some(case) = std::env::var("KYMO_HELD_FUZZ_CASE_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
    {
        let mut tally = Tally::default();
        fuzz_case(case, 14, &mut tally);
        println!("HELD FUZZ {tally:?}");
        return;
    }
    let tally = fuzz(cases, 14, seed);
    println!("HELD FUZZ {tally:?}");
    // Every poll's checks ran above. The corpus-level floors hold for the default corpus, which a default-seed run of at least CASES replays first; another corpus need not reach every rare path.
    if seed != SEED || cases < CASES {
        return;
    }
    assert!(tally.served > tally.polls / 4, "{tally:?}");
    assert!(tally.served_delta > tally.cases, "{tally:?}");
    assert!(tally.algorithms_served.iter().all(|&n| n > 10), "{tally:?}");
    assert!(
        tally.fallbacks.get("inexact_x").copied().unwrap_or(0) > 0,
        "{tally:?}"
    );
    for what in [
        "passthrough",
        "shaped",
        "mixed",
        "time",
        "relative",
        "log",
        "zoom",
        "fresh_slots",
        "inserted_slots",
        "flipped_cells",
        "moved_centers",
        "reached",
        "folded_slots",
        "replays",
        "broke_even_spacing",
        "floored_ladder",
        "kind4_markers",
        "y_markers",
        "tagged",
        "first_paint",
        "shadow_match",
        "memo_hit",
        "second_tab",
    ] {
        assert!(
            tally.coverage.get(what).copied().unwrap_or(0) > 0,
            "the fuzz never served a poll exercising {what}: {tally:?}"
        );
    }
}

/// The work bound's point: an advance's work follows the appended rows, never the held history. Long series, few rows per poll, every smoother and axis, with and without a 30,000-row NaN/inf tail that the last poll's finite rows end, and with no finite value at all; the advance after each poll must stay far below one pass over the rows, and the last one still answers what the full build does.
#[test]
fn held_state_work_does_not_scale_with_history() {
    // Past one full-read chunk (32,768 rows), so each series starts as two chunks.
    const HISTORY: i64 = 40_000;
    // Steps in the tail log NaN, inf and -inf: from 10,000 until the last poll's rows, or every step (a series with no finite output).
    let tails: [Option<std::ops::Range<i64>>; 3] =
        [None, Some(10_000..HISTORY + 40), Some(0..i64::MAX)];
    let runs = ["a", "b", "c", "d"];
    let configs: [(Algorithm, bool, bool, u32); 8] = [
        (Algorithm::None, false, false, 500),
        (Algorithm::Ema, false, true, 500),
        (Algorithm::Triangular, true, false, 300),
        (Algorithm::SavitzkyGolay, false, false, 500),
        (Algorithm::SavitzkyGolay, true, true, 400),
        (Algorithm::Ema, true, false, 120),
        (Algorithm::None, true, true, 2000),
        (Algorithm::Triangular, false, true, 64),
    ];
    for ((algorithm, use_time, log, target), tail) in configs
        .into_iter()
        .flat_map(|config| tails.clone().map(|tail| (config, tail)))
    {
        let cache = SeriesCache::with_budget(usize::MAX);
        let keys: Vec<_> = runs
            .iter()
            .map(|run| SeriesKey::new("p", *run, "loss"))
            .collect();
        // Every 9th timestamp repeats its predecessor's millisecond, so time axes run an irregular plan.
        let row = |run: usize, step: i64| {
            let ts = 1_000_000 + step * 10 + (step % 9 == 0) as i64 * 3 + run as i64;
            let value = if tail.as_ref().is_some_and(|tail| tail.contains(&step)) {
                [f32::NAN, f32::INFINITY, f32::NAN, f32::NEG_INFINITY][(step % 4) as usize]
            } else {
                ((step * (run as i64 + 3)) % 997) as f32 / 7.0
            };
            point("", step, ts, value, 1_000 + step)
        };
        let mut snapshots: Vec<Arc<SeriesSnapshot>> = keys
            .iter()
            .enumerate()
            .map(|(run, key)| {
                cache.insert_full(
                    key.clone(),
                    (0..HISTORY).map(|step| row(run, step)).collect(),
                    Instant::now(),
                )
            })
            .collect();
        let mut req = req(&runs, target);
        req.use_timestamp_axis = use_time;
        req.log_buckets = log;
        req.smoothing = smoothing(algorithm, 20, 30.0, 2);
        let (mut state, mut client) = continuation_state(&req, &snapshots);
        let mut next_step = HISTORY;
        for poll in 0..6 {
            let appended = [1i64, 7, 30, 2, 0, 13][poll];
            for (run, key) in keys.iter().enumerate() {
                let rows = (next_step..next_step + appended)
                    .map(|step| row(run, step))
                    .collect();
                snapshots[run] = cache_append(&cache, key, rows);
            }
            next_step += appended;
            let poll_req = continuation(&req, &client);
            let windows = || chart::SAVGOL_READS.with(|reads| reads.replace(0));
            windows();
            let advanced = advance_held(&state, &poll_req, &snapshots).unwrap_or_else(|reason| {
                panic!("{algorithm:?} time={use_time} log={log} tail={tail:?}: {reason:?}")
            });
            // Each re-evaluated Savitzky-Golay slot reads its window, which the floor widens to about 1.4-2.8 of its bucket's samples (up to twice that near a log ladder's octave end): a few buckets, not the history.
            let windows = windows();
            let columns = advanced.next.model().x_values.len();
            let bucket = 2 * HISTORY as usize / columns + 1;
            assert!(
                windows <= advanced.work.rebuild.slots * (6 * bucket + 64),
                "{algorithm:?} time={use_time} log={log} poll {poll}: windows read {windows} for {} slots over buckets of about {bucket}",
                advanced.work.rebuild.slots
            );
            let work = advanced.work.rows + advanced.work.smoothed;
            // A constant per series (a checkpoint replay, searches), per appended row and per sample in the re-evaluated windows; one pass over the history would read 4 × 40,000 rows.
            let bound = runs.len() * 2_500 + 300 * appended as usize * runs.len() + 4 * windows;
            assert!(
                work <= bound,
                "{algorithm:?} time={use_time} log={log} tail={tail:?} poll {poll}: work {:?} over {bound}",
                advanced.work
            );
            println!(
                "HELD WORK {algorithm:?} time={use_time} log={log} tail={tail:?} appended={appended} rows={} smoothed={} slots={} replays={}",
                advanced.work.rows, advanced.work.smoothed, advanced.work.rebuild.slots, advanced.work.replays
            );
            if poll == 5 {
                let truth =
                    build_chart(&poll_req, &snapshots, None, false, None, &|_| {}, false).unwrap();
                assert_eq!(advanced.response, truth.response);
            }
            client = advanced.response;
            state = advanced.next;
        }
    }
}

/// An advance's work grows neither with the samples a raw slot holds nor with the slots past a series' first change that hold none of its samples: a run whose 20,000 rows share one timestamp sits in one raw slot of a time axis, which an appended row at that timestamp extends; and a passthrough chart's 5,000-row series trailing a 40,000-row one recomputes the 35,000 slots past its new sample. One pass over the rows reads 40,000.
#[test]
fn held_state_work_does_not_scale_with_a_slots_samples() {
    let stalled = |n: i64| -> [Vec<VersionedRawPoint>; 2] {
        [
            (0..n)
                .map(|s| point("", s, 10_000_000 + 10 * s, (s % 13) as f32, 1_000 + s))
                .collect(),
            (0..n)
                .map(|s| point("", s, 0, (s % 7) as f32, 1_000 + s))
                .collect(),
        ]
    };
    let trailing = |n: i64| -> [Vec<VersionedRawPoint>; 2] {
        [
            (0..n - 35_000)
                .map(|s| point("", s, 1_000_000 + s, (s % 13) as f32, 1_000 + s))
                .collect(),
            (0..n)
                .map(|s| point("", s, 1_000_000 + s, (s % 7) as f32, 1_000 + s))
                .collect(),
        ]
    };
    type Rows = fn(i64) -> [Vec<VersionedRawPoint>; 2];
    let shapes: [(&str, bool, u32, i64, Rows); 2] = [
        ("stalled", true, 300, 20_000, stalled),
        ("trailing", false, 0, 40_000, trailing),
    ];
    for (shape, use_time, target, n, rows) in shapes {
        for algorithm in [
            Algorithm::None,
            Algorithm::Ema,
            Algorithm::Triangular,
            Algorithm::SavitzkyGolay,
        ] {
            let snapshots: Vec<_> = rows(n)
                .into_iter()
                .map(|rows| Arc::new(SeriesSnapshot::full(rows)))
                .collect();
            let mut req = req(&["a", "b"], target);
            req.use_timestamp_axis = use_time;
            req.smoothing = smoothing(algorithm, 20, 10.0, 1);
            let (state, held) = continuation_state(&req, &snapshots);
            // One row appended to each run: in the stalled shape, run b's at timestamp 0 again.
            let grown: Vec<_> = snapshots
                .iter()
                .zip(rows(n + 1))
                .map(|(held, rows)| held.refreshed_fixture(&rows))
                .collect();
            let poll = continuation(&req, &held);
            let windows = || chart::SAVGOL_READS.with(|reads| reads.replace(0));
            windows();
            let advanced = advance_held(&state, &poll, &grown)
                .unwrap_or_else(|reason| panic!("{shape} {algorithm:?}: {reason:?}"));
            let windows = windows();
            let truth = build_chart(&poll, &grown, None, false, None, &|_| {}, false).unwrap();
            assert_eq!(advanced.response, truth.response, "{shape} {algorithm:?}");
            println!("HELD SLOT WORK {shape} {algorithm:?}: {:?}", advanced.work);
            let bound = if shape == "stalled" && algorithm == Algorithm::SavitzkyGolay {
                // Run b's samples all sit in one raw slot, at one x, so the appended one there re-evaluates its fit, which reads every one of them (beside run a's re-evaluated windows), and the advance reads a few rows per sample of the stall.
                assert!(windows > n as usize, "{windows}");
                10 * (n as usize + 1)
            } else {
                5_000
            };
            let work = advanced.work.rows + advanced.work.smoothed;
            assert!(
                work <= bound,
                "{shape} {algorithm:?}: work {:?}",
                advanced.work
            );
        }
    }
}

/// An unchanged poll recomputes nothing: at a stalled clock's end, behind a short non-finite run, it re-evaluates no Savitzky-Golay slot, though the last slots' windows hold the whole stall.
#[test]
fn an_unchanged_poll_at_a_stalled_clock_reads_few_rows() {
    let mut request = req(&["a"], 300);
    request.use_timestamp_axis = true;
    request.smoothing = smoothing(Algorithm::SavitzkyGolay, 21, 0.0, 2);
    let rows = || {
        Arc::new(
            (0..3_000i64)
                .map(|i| {
                    let ts = 1_000_000 + 10 * i.min(1_000);
                    let value = if i >= 2_990 {
                        f32::NAN
                    } else {
                        (i % 13) as f32
                    };
                    point("", i, ts, value, i * 10_000_000)
                })
                .collect::<Vec<_>>(),
        )
    };
    let (outcome, truth) = advance_between(&request, &[rows()], &[rows()]);
    let advanced = outcome.expect("advanced");
    assert_eq!(advanced.response, truth);
    assert!(advanced.work.rows < 1_000, "{:?}", advanced.work);
}

/// The service's build step in `on` mode serves the advance, and its audited polls build in full and compare (the differential fuzz runs both modes); a tampered entry fails the audit, answers in full with `audit_failed` and turns held state off until restart.
#[test]
fn an_audit_failure_answers_in_full_and_turns_held_state_off() {
    let runs = ["a", "b"];
    let cache = SeriesCache::with_budget(usize::MAX);
    let keys: Vec<_> = runs
        .iter()
        .map(|run| SeriesKey::new("p", *run, "loss"))
        .collect();
    let row = |run: usize, step: i64| {
        let value = ((step * (run as i64 + 5)) % 101) as f32;
        point("", step, 1_000_000 + step * 10, value, 1_000 + step)
    };
    let mut snapshots: Vec<Arc<SeriesSnapshot>> = keys
        .iter()
        .enumerate()
        .map(|(run, key)| {
            cache.insert_full(
                key.clone(),
                (0..3_000).map(|step| row(run, step)).collect(),
                Instant::now(),
            )
        })
        .collect();
    let mut req = req(&runs, 300);
    req.smoothing = smoothing(Algorithm::Ema, 0, 12.0, 1);
    let store = HeldStore::new(HeldMode::On, usize::MAX);
    let mut client: Option<proto::ChartResponse> = None;
    for poll in 0..8 {
        if poll > 0 {
            for (run, key) in keys.iter().enumerate() {
                let next = 3_000 + 5 * (poll - 1);
                let rows = (next..next + 5).map(|step| row(run, step)).collect();
                snapshots[run] = cache_append(&cache, key, rows);
            }
        }
        let mut poll_req = req.clone();
        poll_req.cache_state = client.as_ref().and_then(echo);
        let truth = build_chart(&poll_req, &snapshots, None, false, None, &|_| {}, false).unwrap();
        // The fourth poll is audited; the eighth audits a tampered entry.
        if poll == 7 {
            store.tamper_for_test();
        }
        let built = build_held(&store, &poll_req, &snapshots, poll % 4 == 3, None).unwrap();
        if poll < 7 {
            assert_eq!(built.response, truth.response, "poll {poll}");
            assert_eq!(built.kind, truth.kind, "poll {poll}");
            client = Some(built.response);
            continue;
        }
        assert!(
            built.response.audit_failed,
            "a tampered entry fails the audit"
        );
        assert!(!built.response.delta, "an audit failure answers in full");
        assert_eq!(built.kind, Some("audit_failed"), "and is counted so");
        assert_eq!(
            store.mode(),
            HeldMode::Off,
            "an audit failure turns held state off"
        );
        assert_eq!(store.len_and_bytes(), (0, 0));
        let full = build_chart(&req, &snapshots, None, false, None, &|_| {}, false).unwrap();
        assert!(eq(
            &inflate_full(&built.response),
            &inflate_full(&full.response)
        ));
        assert_eq!(built.response.frontiers, truth.response.frontiers);
    }
}

/// The store holds a few models per chart (tabs at different maxima), evicts the least recently used past its byte budget, and holds nothing when off.
#[test]
fn held_store_bounds_entries_per_chart_and_bytes() {
    let rows = |n: i64| -> Vec<Arc<Vec<VersionedRawPoint>>> { vec![rows(n, 3)] };
    let req = req(&["a"], 300);
    let state_at = |n: i64| {
        let snapshots = fixture_snapshots(&req, &rows(n));
        (snapshots.clone(), continuation_state(&req, &snapshots).0)
    };
    let key = held::request_key(&req);
    let (one_rows, one) = state_at(1_000);
    let sizing = HeldStore::new(HeldMode::On, usize::MAX);
    sizing.insert(key, one.clone());
    let (_, single) = sizing.len_and_bytes();
    assert!(
        single > one.bytes(),
        "the store's containers are charged too"
    );
    let store = HeldStore::new(HeldMode::On, 3 * single);
    store.insert(key, one.clone());
    assert_eq!(store.len_and_bytes(), (1, single));
    // The same maxima replace their entry.
    store.insert(key, one.clone());
    assert_eq!(store.len_and_bytes(), (1, single));
    let (other_rows, other) = state_at(1_200);
    assert_ne!(
        other_rows[0].lineage(),
        one_rows[0].lineage(),
        "a fresh read is a new lineage"
    );
    store.insert(key, other.clone());
    let (_, third) = state_at(1_400);
    store.insert(key, third.clone());
    let (fourth_rows, fourth) = state_at(1_600);
    store.insert(key, fourth);
    let (held, held_bytes) = store.len_and_bytes();
    assert!(
        held_bytes <= 3 * single,
        "{held} entries, {held_bytes} bytes"
    );
    assert!(
        store.take(&key, &one_rows, None).0.is_err(),
        "the least recently used entry left first"
    );
    assert!(store.take(&key, &fourth_rows, None).0.is_ok());
    // A fresh tab takes only an entry its snapshots extend, not one held at a higher maximum of its lineage.
    let newer = vec![fourth_rows[0].refreshed_fixture(&rows(1_800)[0])];
    assert_eq!(newer[0].lineage(), fourth_rows[0].lineage());
    let built = build_chart(&req, &newer, None, false, None, &|_| {}, true).unwrap();
    let ahead = HeldStore::new(HeldMode::On, usize::MAX);
    ahead.insert(key, built.held.unwrap().unwrap());
    assert!(ahead.take(&key, &fourth_rows, None).0.is_err());
    assert!(ahead.take(&key, &newer, None).0.is_ok());
    let off = HeldStore::new(HeldMode::Off, usize::MAX);
    off.insert(key, one);
    assert_eq!(off.len_and_bytes(), (0, 0));
}

/// A state the store could hold only past its budget, counting what it holds beside the state (its entry list, LRU slot, table and logged divergences), is never held: it is counted `uncaptured{budget}`, and no entry held for another chart is evicted for it, nor is it inserted and then evicted itself. One that fits beside them is held.
#[test]
fn a_state_the_store_cannot_hold_alone_is_never_inserted() {
    let request = req(&["a", "b"], 300);
    let snapshots = fixture_snapshots(&request, &[rows(2_000, 3), rows(2_000, 7)]);
    let (state, _) = continuation_state(&request, &snapshots);
    let small = req(&["c"], 300);
    let (other, _) = continuation_state(&small, &fixture_snapshots(&small, &[rows(50, 5)]));
    let sizing = HeldStore::new(HeldMode::On, usize::MAX);
    sizing.insert(held::request_key(&request), state.clone());
    let (_, alone) = sizing.len_and_bytes();
    assert!(
        alone > state.bytes(),
        "the store's containers are charged too"
    );
    for (budget, held) in [(alone - 1, false), (alone, true)] {
        let store = HeldStore::new(HeldMode::On, budget);
        store.insert(held::request_key(&small), other.clone());
        let ((), rendered) = crate::with_metrics(|| {
            store.insert(held::request_key(&request), state.clone());
        });
        let counted = |line: &str| rendered.lines().any(|l| l == line);
        assert_eq!(
            counted("mkdb2_chart_held_uncaptured_total{reason=\"budget\"} 1"),
            !held,
            "{rendered}"
        );
        assert!(
            store
                .take(&held::request_key(&request), &snapshots, None)
                .0
                .is_ok()
                == held,
            "budget {budget}"
        );
        if !held {
            assert_eq!(store.len_and_bytes().0, 1, "the other chart's entry stays");
            assert!(
                !rendered.contains("mkdb2_chart_held_evicted_total"),
                "{rendered}"
            );
        }
    }
}

/// The state a continuation poll of `req` right after its first paint leaves, both over `snapshots`, and its reply.
pub(super) fn continuation_state(
    req: &proto::ChartRequest,
    snapshots: &[Arc<SeriesSnapshot>],
) -> (held::HeldChart, proto::ChartResponse) {
    let first = build_chart(req, snapshots, None, false, None, &|_| {}, false).unwrap();
    let continued = continuation(req, &first.response);
    let built = build_chart(&continued, snapshots, None, false, None, &|_| {}, true).unwrap();
    (built.held.unwrap().unwrap(), built.response)
}

/// Capture the state a continuation poll of `held_rows` leaves (`Err` when it cannot be held), then advance it to `grown_rows`: the advance's outcome and the full build's reply.
fn advance_between(
    req: &proto::ChartRequest,
    held_rows: &[Arc<Vec<VersionedRawPoint>>],
    grown_rows: &[Arc<Vec<VersionedRawPoint>>],
) -> (Result<held::Advanced, Fallback>, proto::ChartResponse) {
    let first = build_response(req, held_rows, None, false).unwrap();
    let continued = continuation(req, &first);
    let mut state = None;
    let held = respond_with(&continued, held_rows, |snapshots| {
        let built = build_chart(&continued, snapshots, None, false, None, &|_| {}, true)?;
        state = built.held;
        Ok(built.response)
    })
    .unwrap();
    let next = continuation(req, &held);
    let snapshots = fixture_snapshots(&next, grown_rows);
    let truth = build_chart(&next, &snapshots, None, false, None, &|_| {}, false)
        .unwrap()
        .response;
    let outcome = state
        .unwrap()
        .and_then(|state| advance_held(&state, &next, &snapshots));
    (outcome, truth)
}

/// Held state's advance for the poll `req` over `snapshots`, as the service's build step runs it.
fn advance_held(
    state: &held::HeldChart,
    req: &proto::ChartRequest,
    snapshots: &[Arc<SeriesSnapshot>],
) -> Result<held::Advanced, Fallback> {
    let lineages = inspect_lineages(req, snapshots, echoed_frontiers(req));
    held::advance(state, req, &chart_params(req), snapshots, &lineages)
}

/// [`advance_held`], which must answer and hold what the full build of the poll does (`at` names the case).
fn advance_as_full_build(
    state: &held::HeldChart,
    req: &proto::ChartRequest,
    snapshots: &[Arc<SeriesSnapshot>],
    at: &str,
) -> held::Advanced {
    let advanced =
        advance_held(state, req, snapshots).unwrap_or_else(|reason| panic!("{at}: {reason:?}"));
    let truth = build_chart(req, snapshots, None, false, None, &|_| {}, true).unwrap();
    assert_eq!(advanced.response, truth.response, "{at}");
    assert_eq!(advanced.kind, truth.kind, "{at}");
    let captured = truth.held.unwrap().unwrap();
    assert_eq!(advanced.next.state_difference(&captured), None, "{at}");
    advanced
}

fn reason(outcome: &Result<held::Advanced, Fallback>) -> Option<Fallback> {
    outcome.as_ref().err().copied()
}

/// The service's build step for a poll not expecting an advance, its charges ignored.
fn build_held(
    store: &HeldStore,
    req: &proto::ChartRequest,
    snapshots: &[Arc<SeriesSnapshot>],
    audit: bool,
    memo: Option<&proto::ChartResponse>,
) -> Result<BuiltChart, Status> {
    let request = Some(held::request_key(req));
    held::build(
        store,
        req,
        &chart_params(req),
        request,
        snapshots,
        None,
        audit,
        memo,
        &|_| {},
        held::Advance::Try,
    )
    .map(Option::unwrap)
}

/// The service's step after a poll's reads: the memo, else held state's build step, settled into the memo. The reply, and whether held state's advance answered (`None`: the memo did).
fn memo_or_held(
    memo: &crate::query::memo::ChartMemo,
    store: &HeldStore,
    req: &proto::ChartRequest,
    snapshots: &[Arc<SeriesSnapshot>],
    audit: bool,
) -> Result<(proto::ChartResponse, Option<bool>), Status> {
    let key = memo.key_for(req);
    let held = Some((store, held::request_key(req)));
    let memoized = match probe_memo(memo, key, snapshots, audit, held) {
        ControlFlow::Break(reply) => return Ok((reply, None)),
        ControlFlow::Continue(memoized) => memoized,
    };
    let built = build_held(store, req, snapshots, audit, memoized.as_ref())?;
    settle_memo(memo, key, snapshots, &built);
    Ok((built.response, Some(built.advanced)))
}

/// A tab polling one chart, each poll echoing the response it last received.
#[derive(Default)]
struct Tab(Option<FixtureResponse>);

impl Tab {
    /// One poll of `req` over `rows` answered by `answer`, given the request as echoed; the tab keeps the reply.
    fn poll(
        &mut self,
        req: &proto::ChartRequest,
        rows: &[Arc<Vec<VersionedRawPoint>>],
        answer: impl FnOnce(
            &proto::ChartRequest,
            &[Arc<SeriesSnapshot>],
        ) -> Result<proto::ChartResponse, Status>,
    ) -> &FixtureResponse {
        let mut r = req.clone();
        r.cache_state = self.0.as_ref().and_then(|held| echo(held));
        self.0 = Some(respond_with(&r, rows, |snapshots| answer(&r, snapshots)).unwrap());
        self.0.as_ref().unwrap()
    }

    /// One poll through the service's build step, audited if `audit`, which must answer what the full build does.
    fn serve(
        &mut self,
        store: &HeldStore,
        req: &proto::ChartRequest,
        rows: &[Arc<Vec<VersionedRawPoint>>],
        audit: bool,
    ) -> &FixtureResponse {
        self.poll(req, rows, |r, snapshots| {
            let truth = build_chart(r, snapshots, None, false, None, &|_| {}, false)?;
            let built = build_held(store, r, snapshots, audit, None)?;
            assert_eq!(built.response, truth.response);
            assert_eq!(built.kind, truth.kind);
            Ok(built.response)
        })
    }
}

/// Polls answered through the service's build step, audited where `audit(poll)` says, each checked against the full build; returns what `mkdb2_chart_held_total` counted.
fn drive_build(
    store: &HeldStore,
    req: &proto::ChartRequest,
    states: &[Vec<Arc<Vec<VersionedRawPoint>>>],
    audit: impl Fn(usize) -> bool,
) -> BTreeMap<&'static str, usize> {
    held::take_outcomes();
    let mut tab = Tab::default();
    for (poll, rows) in states.iter().enumerate() {
        tab.serve(store, req, rows, audit(poll));
    }
    held::take_outcomes()
}

/// Steps past 2^53 collide as f64, and an append need not keep the chart downsampled: such a chart is never held, and an append that leaves the exact range falls back.
#[test]
fn x_beyond_exact_f64_integers_is_never_advanced() {
    let base = 1i64 << 53;
    let mut request = req(&["a"], 3);
    request.log_buckets = true;
    let at = |steps: &[i64]| {
        Arc::new(
            steps
                .iter()
                .enumerate()
                .map(|(i, &step)| {
                    let i = i as i64;
                    point("", step, 1_000_000 + i, i as f32, (i + 1) * 10_000_000)
                })
                .collect::<Vec<_>>(),
        )
    };
    // The first probe: log axis, resolution 3, steps 2^53 + [0, 4], then 2^53 + 5.
    let held: Vec<i64> = (0..5).map(|i| base + i).collect();
    let grown: Vec<i64> = (0..6).map(|i| base + i).collect();
    let (outcome, _) = advance_between(&request, &[at(&held)], &[at(&grown)]);
    assert_eq!(reason(&outcome), Some(Fallback::InexactX));
    // The second: two runs whose steps stay distinct within each run as f64.
    let a: Vec<i64> = (0..5).map(|i| base + 2 * i).collect();
    let b: Vec<i64> = (0..5).map(|i| base + 2 * i + 1).collect();
    let mut two = req(&["a", "b"], 3);
    two.log_buckets = true;
    let (outcome, _) = advance_between(&two, &[at(&a), at(&b)], &[at(&a), at(&b)]);
    assert_eq!(reason(&outcome), Some(Fallback::InexactX));
    // Held within the exact range, an append leaving it falls back.
    let held: Vec<i64> = (0..40).map(|i| base - 40 + i).collect();
    let grown: Vec<i64> = (0..43).map(|i| base - 40 + i).collect();
    let (outcome, _) = advance_between(&request, &[at(&held)], &[at(&grown)]);
    assert_eq!(reason(&outcome), Some(Fallback::InexactX));
    // And through the service's build step the replies are the full build's.
    let store = HeldStore::new(HeldMode::On, usize::MAX);
    let states: Vec<Vec<Arc<Vec<VersionedRawPoint>>>> = [5usize, 5, 6, 7]
        .iter()
        .map(|&n| vec![at(&(0..n as i64).map(|i| base + i).collect::<Vec<_>>())])
        .collect();
    let counted = drive_build(&store, &request, &states, |_| false);
    assert_eq!(counted.get("served"), None, "{counted:?}");
}

/// Structurally ineligible polls (a first paint, a custom x axis) count as `ineligible`, so `fallback` counts only polls held state could have answered.
#[test]
fn ineligible_polls_are_not_counted_as_fallbacks() {
    let store = HeldStore::new(HeldMode::On, usize::MAX);
    let request = req(&["a"], 300);
    let states = [100, 100, 110].map(|n| vec![rows(n, 3)]);
    let counted = drive_build(&store, &request, &states, |_| false);
    assert_eq!(
        counted,
        BTreeMap::from([("fallback", 1), ("ineligible", 1), ("served", 1)]),
        "a first paint, a continuation with nothing held yet, an advance"
    );

    let mut custom = req(&["a"], 300);
    custom.x_series = Some(proto::SeriesRef {
        metric_name: "x".into(),
        ..Default::default()
    });
    let x_maps = HashMap::from([(
        "a".to_string(),
        (0..200)
            .map(|s| (s, s as f64))
            .collect::<HashMap<i64, f64>>(),
    )]);
    held::take_outcomes();
    respond_with(&custom, &[rows(100, 3)], |snapshots| {
        // The plan keys no custom-x chart.
        Ok(held::build(
            &store,
            &custom,
            &chart_params(&custom),
            None,
            snapshots,
            Some(&x_maps),
            false,
            None,
            &|_| {},
            held::Advance::Try,
        )?
        .unwrap()
        .response)
    })
    .unwrap();
    assert_eq!(held::take_outcomes(), BTreeMap::from([("ineligible", 1)]));
}

/// A full build that failed its own check (here stage 1's memo audit) answers alone: the advance is not compared with it, which counts `audit_skipped`, held state stays on with the build's captured state, and the next poll advances from it. The answer releases the entry the client held, as any answer does: the client replaces its model with it.
#[test]
fn a_full_build_failing_its_own_check_is_no_reference() {
    let store = HeldStore::new(HeldMode::On, usize::MAX);
    let request = req(&["a"], 300);
    let bogus = proto::ChartResponse::default();
    held::take_outcomes();
    let mut tab = Tab::default();
    for (poll, n) in [100, 100, 110, 120].into_iter().enumerate() {
        let memo = (poll == 2).then_some(&bogus);
        let out = tab.poll(&request, &[rows(n, 3)], |r, snapshots| {
            Ok(build_held(&store, r, snapshots, memo.is_some(), memo)?.response)
        });
        assert_eq!(out.audit_failed, poll == 2, "poll {poll}");
        assert_eq!(store.len_and_bytes().0, [0, 1, 1, 1][poll], "poll {poll}");
    }
    assert_eq!(store.mode(), HeldMode::On);
    assert_eq!(
        held::take_outcomes(),
        BTreeMap::from([
            ("audit_skipped", 1),
            ("fallback", 1),
            ("ineligible", 1),
            ("served", 1)
        ])
    );
}

/// Each fallback the fuzz rarely reaches, deterministically.
#[test]
fn rare_fallbacks_answer_with_the_full_build() {
    // lineage: the rows no longer extend the held ones.
    let request = req(&["a"], 300);
    let (outcome, _) = advance_between(&request, &[rows(100, 3)], &[rows(50, 3)]);
    assert_eq!(reason(&outcome), Some(Fallback::Lineage));

    // time_order: a timestamp before the series' latest.
    let mut timed = req(&["a"], 300);
    timed.use_timestamp_axis = true;
    let ts = |i: i64| {
        if i == 120 {
            1_000_000
        } else {
            2_000_000 + 10 * i
        }
    };
    let (outcome, _) = advance_between(
        &timed,
        &[rows_shaped(100, |i| i, ts)],
        &[rows_shaped(121, |i| i, ts)],
    );
    assert_eq!(reason(&outcome), Some(Fallback::TimeOrder));

    // duplicates: a passthrough time chart whose next timestamp repeats the last.
    let mut sparse = req(&["a"], 10_000);
    sparse.use_timestamp_axis = true;
    let ts = |i: i64| 2_000_000 + 10 * i.min(49);
    let (outcome, _) = advance_between(
        &sparse,
        &[rows_shaped(50, |i| i, ts)],
        &[rows_shaped(51, |i| i, ts)],
    );
    assert_eq!(reason(&outcome), Some(Fallback::Duplicates));

    // unplottable: a run whose steps are all negative on a log axis gains another.
    let mut log = req(&["a", "b"], 300);
    log.log_buckets = true;
    let negative = |n: i64| rows_shaped(n, |i| i - 10, |i| 1_000_000 + i);
    let (outcome, _) = advance_between(
        &log,
        &[rows(100, 3), negative(5)],
        &[rows(100, 3), negative(6)],
    );
    assert_eq!(reason(&outcome), Some(Fallback::Unplottable));

    // envelope_flip: a sparse time chart shipping passthrough-shaped (every cell under four distinct x) gains four x in one cell; the full planner answers in full too.
    let mut shaped = req(&["a"], 12);
    shaped.use_timestamp_axis = true;
    let ts = |i: i64| {
        if i < 20 {
            1_000_000 + 2_000 * i
        } else {
            1_000_000 + 2_000 * 19 + (i - 19)
        }
    };
    let (outcome, truth) = advance_between(
        &shaped,
        &[rows_shaped(20, |i| i, ts)],
        &[rows_shaped(24, |i| i, ts)],
    );
    assert_eq!(reason(&outcome), Some(Fallback::EnvelopeFlip));
    assert!(!truth.delta, "the full planner refuses it");

    // plan_shift: one held sample's spacing is 1; samples appended 7 ms apart move its median to 7. Run b's occupied envelope cells keep the chart off the passthrough shape.
    let mut sg = req(&["a", "b"], 300);
    sg.use_timestamp_axis = true;
    sg.smoothing = smoothing(Algorithm::SavitzkyGolay, 20, 1.0, 2);
    let at = |i| 2_005_000 + 7 * i;
    let spread = rows_shaped(2_000, |i| i, |i| 2_000_000 + 10 * i);
    let (outcome, _) = advance_between(
        &sg,
        &[rows_shaped(1, |i| i, at), spread.clone()],
        &[rows_shaped(6, |i| i, at), spread],
    );
    assert_eq!(reason(&outcome), Some(Fallback::PlanShift));
}

/// A relative time axis checks every tag of a ref for a row before its tag's latest (relative_offsets_stable), tags no series reads included: the advance refuses the delta exactly when the full build does. Capture checks time order only on the tags a series reads: a chart is still captured when a held tag no series reads is out of time order, and advances by the delta.
#[test]
fn a_relative_axis_checks_tags_no_series_reads() {
    let mut request = req(&["a"], 300);
    request.use_timestamp_axis = true;
    request.relative_time = true;
    request.y_series[0].tags = vec!["g1".into()];
    // Tag g0, which no series reads, logs step `early` at step 0's timestamp.
    let history = |n: i64, early: i64| {
        let mut rows = Vec::new();
        for tag in ["g0", "g1"] {
            for step in 0..n {
                let ts = if tag == "g0" && step == early {
                    1_000_000
                } else {
                    1_000_000 + 10 * step
                };
                rows.push(point(tag, step, ts, (step % 13) as f32, step * 10_000_000));
            }
        }
        Arc::new(rows)
    };
    // g0's new row precedes its latest: the full build answers in full. With g0's step 20 held out of order, rows after its latest keep the delta.
    for (held, grown, delta) in [
        (history(50, 0), history(51, 50), false),
        (history(50, 20), history(51, 20), true),
    ] {
        let (outcome, truth) = advance_between(&request, &[held], &[grown]);
        assert_eq!(truth.delta, delta);
        assert_eq!(outcome.expect("captured and advanced").response, truth);
    }
}

/// Rows `0..n` of each of `tags`, a step and 10 ms apart, stamped by step.
fn tag_rows(tags: &[String], n: i64) -> Arc<Vec<VersionedRawPoint>> {
    Arc::new(
        tags.iter()
            .flat_map(|tag| {
                (0..n).map(move |step| {
                    let value = ((step * 7) % 101) as f32;
                    point(tag, step, 1_000_000 + 10 * step, value, step * 10_000_000)
                })
            })
            .collect(),
    )
}

/// Held state keeps only the tags a ref's series read (every tag on a relative axis, whose offset check reads them all): a ref keeping one of 100 tags holds what the chart over that tag alone holds, and advances as every tag grows.
#[test]
fn held_state_keeps_only_the_tags_series_read() {
    let names: Vec<String> = (0..100).map(|t| format!("t{t:03}")).collect();
    for (use_time, relative) in [(false, false), (true, false), (true, true)] {
        let mut request = req(&["a"], 300);
        request.y_series[0].tags = vec![names[50].clone()];
        request.use_timestamp_axis = use_time;
        request.relative_time = relative;
        let held_bytes = |tags: &[String]| {
            let rows = tag_rows(tags, 50).as_ref().clone();
            continuation_state(&request, &[Arc::new(SeriesSnapshot::full(rows))])
                .0
                .bytes()
        };
        let (all, one) = (held_bytes(&names), held_bytes(&names[50..51]));
        if relative {
            assert!(all > one + 99 * 32, "relative: {all} against {one}");
        } else {
            assert_eq!(all, one, "time axis {use_time}");
        }
        let store = HeldStore::new(HeldMode::On, usize::MAX);
        let mut tab = Tab::default();
        held::take_outcomes();
        for n in [50i64, 50, 51, 55] {
            tab.serve(&store, &request, &[tag_rows(&names, n)], false);
        }
        let counted = held::take_outcomes();
        assert_eq!(counted.get("served"), Some(&2), "{counted:?}");
    }
}

/// A tag no series reads, which held state does not keep, still decides a ref's series: entering the fetch range, it makes the scalar group yield to tags, and the advance falls back as the full build answers in full.
#[test]
fn an_unread_tag_entering_the_range_changes_the_series() {
    let row = |tag: &str, step: i64| {
        let value = (step % 13) as f32;
        point(tag, step, 1_000_000 + 10 * step, value, step * 10_000_000)
    };
    let rows = |t0: &[i64]| -> Arc<Vec<VersionedRawPoint>> {
        let scalar = (0..300).map(|step| row("", step));
        Arc::new(
            scalar
                .chain(t0.iter().map(|&step| row("t0", step)))
                .collect(),
        )
    };
    let early: Vec<i64> = (0..10).collect();
    let (held, grown) = (rows(&early), rows(&[early.as_slice(), &[160]].concat()));
    // Unfiltered, the scalar group yields to t0; filtered to a tag that never logs, to nothing.
    for filter in [vec![], vec!["t1".to_string()]] {
        let mut request = req(&["a"], 300);
        request.step_min = Some(150);
        request.step_max = Some(1_000);
        request.y_series[0].tags = filter.clone();
        let (outcome, truth) = advance_between(
            &request,
            std::slice::from_ref(&held),
            std::slice::from_ref(&grown),
        );
        assert_eq!(reason(&outcome), Some(Fallback::Tags), "{filter:?}");
        assert!(!truth.delta, "{filter:?}");
    }
}

/// A ref growing from 64 tag groups to 65, which the full build refuses: the advance falls back (`tags`), and the poll answers with the full build's error, on a step axis and on a relative time axis.
#[test]
fn a_ref_growing_past_the_tag_group_limit_answers_with_the_full_builds_error() {
    let names: Vec<String> = (0..=MAX_TAG_GROUPS).map(|t| format!("t{t:02}")).collect();
    for relative in [false, true] {
        let mut request = req(&["a"], 300);
        request.use_timestamp_axis = relative;
        request.relative_time = relative;
        let store = HeldStore::new(HeldMode::On, usize::MAX);
        let mut tab = Tab::default();
        for _ in 0..2 {
            tab.serve(
                &store,
                &request,
                &[tag_rows(&names[..MAX_TAG_GROUPS], 50)],
                false,
            );
        }
        assert_eq!(store.len_and_bytes().0, 1, "relative {relative}: held");
        let mut grown = request.clone();
        grown.cache_state = tab.0.as_ref().and_then(|held| echo(held));
        held::take_outcomes();
        // Each tag gains a row, and the 65th logs its first, all stamped after the held ones.
        let mut rows = tag_rows(&names[..MAX_TAG_GROUPS], 51).as_ref().clone();
        rows.push(point(
            &names[MAX_TAG_GROUPS],
            50,
            1_000_500,
            1.0,
            500_000_000,
        ));
        let (refused, rendered) = crate::with_metrics(|| {
            respond_with(&grown, &[Arc::new(rows)], |snapshots| {
                build_held(&store, &grown, snapshots, false, None).map(|built| built.response)
            })
            .unwrap_err()
        });
        assert_eq!(refused.code(), tonic::Code::InvalidArgument, "{refused}");
        assert_eq!(held::take_outcomes(), BTreeMap::from([("fallback", 1)]));
        assert!(
            rendered
                .lines()
                .any(|line| line == "mkdb2_chart_held_fallback_total{reason=\"tags\"} 1"),
            "relative {relative}: {rendered}"
        );
    }
}

/// An audit failure answers in full with `audit_failed`, and stage 1 never memoizes that reply: the next poll the memo serves is the healthy one.
#[test]
fn an_audit_failure_never_poisons_the_memo() {
    let memo = crate::query::memo::ChartMemo::new(true);
    let store = HeldStore::new(HeldMode::On, usize::MAX);
    let mut request = req(&["a"], 300);
    request.smoothing = smoothing(Algorithm::Ema, 0, 5.0, 0);
    let rows = vec![rows(2_000, 5)];
    let mut client: Option<FixtureResponse> = None;
    for poll in 0..4 {
        let mut r = request.clone();
        r.cache_state = client.as_ref().and_then(|held| echo(held));
        if poll == 2 {
            store.tamper_for_test();
        }
        let audit = poll == 2;
        let out = respond_with(&r, &rows, |snapshots| {
            Ok(memo_or_held(&memo, &store, &r, snapshots, audit)?.0)
        })
        .unwrap();
        assert_eq!(
            out.audit_failed,
            poll == 2,
            "poll {poll}: only the audited poll alerts"
        );
        if poll == 2 {
            assert_eq!(store.mode(), HeldMode::Off);
        } else {
            client = Some(out);
        }
    }
    assert!(memo.enabled(), "stage 1 stays on");
}

/// Stage 1 records only replies a full build produced: an unchanged reply held state's advance served is never memoized, so an advance's fault trips held state's audit, not the memo's.
#[test]
fn the_memo_records_only_full_builds() {
    let memo = crate::query::memo::ChartMemo::new(true);
    let store = HeldStore::new(HeldMode::On, usize::MAX);
    let request = req(&["a"], 300);
    let mut client: Option<FixtureResponse> = None;
    // A first paint, an unchanged continuation built in full, an append and an unchanged poll, both advanced.
    for (poll, n) in [100, 100, 110, 110].into_iter().enumerate() {
        let mut r = request.clone();
        r.cache_state = client.as_ref().and_then(|held| echo(held));
        let key = memo.key_for(&r);
        let out = respond_with(&r, &[rows(n, 3)], |snapshots| {
            let (reply, advanced) = memo_or_held(&memo, &store, &r, snapshots, false)?;
            assert_eq!(advanced, Some(poll >= 2), "poll {poll}");
            Ok(reply)
        })
        .unwrap();
        assert_eq!(
            key.is_some_and(|key| memo.holds(&key)),
            poll == 1,
            "poll {poll}: only the full build's unchanged reply is recorded"
        );
        client = Some(out);
    }
}

/// Rows `0..n` of runs a and b, for the store tests' two-run chart (`req(&["a", "b"], 300)`).
fn grow(n: i64) -> Vec<Arc<Vec<VersionedRawPoint>>> {
    vec![rows(n, 3), rows(n, 7)]
}

/// One viewer holds one entry: an advance served from the entry its client held drops it, unless another viewer stored an answer at the same maxima; so does the answer to a poll whose lineage rotated past the client's entry.
#[test]
fn one_viewer_holds_one_entry() {
    let request = req(&["a", "b"], 300);
    let store = HeldStore::new(HeldMode::On, usize::MAX);
    let mut tab = Tab::default();
    let mut bytes = Vec::new();
    for n in [2_000i64, 2_000, 2_010, 2_020, 2_020, 2_030, 2_040] {
        tab.serve(&store, &request, &grow(n), false);
        let (entries, held_bytes) = store.len_and_bytes();
        assert!(entries <= 1, "{entries} entries for one viewer");
        bytes.push(held_bytes);
    }
    println!("HELD ONE VIEWER bytes per poll {bytes:?}");

    // Regrids answer in full: each full answer moves the viewer off its entry as an advance does, in either mode.
    for mode in [HeldMode::On, HeldMode::Shadow] {
        let store = HeldStore::new(mode, usize::MAX);
        let mut tab = Tab::default();
        held::take_outcomes();
        for n in [
            2_000i64, 2_000, 5_000, 5_010, 10_000, 10_010, 20_000, 20_010,
        ] {
            tab.serve(&store, &request, &grow(n), false);
            let entries = store.len_and_bytes().0;
            assert!(
                entries <= 1,
                "{mode:?} n={n}: {entries} entries for one viewer"
            );
        }
        let outcomes = held::take_outcomes();
        assert_eq!(outcomes.get("fallback"), Some(&4), "{mode:?} {outcomes:?}");
    }

    // A rewrite rotates run a's lineage: the viewer's next answer releases the entry it held, which no poll takes, and holds the full answer's.
    let store = HeldStore::new(HeldMode::On, usize::MAX);
    let mut tab = Tab::default();
    for n in [2_000i64, 2_000, 2_010] {
        tab.serve(&store, &request, &grow(n), false);
    }
    assert_eq!(store.len_and_bytes().0, 1);
    held::take_outcomes();
    tab.serve(&store, &request, &[rows(2_020, 5), rows(2_020, 7)], false);
    assert_eq!(held::take_outcomes().get("fallback"), Some(&1));
    assert_eq!(store.len_and_bytes().0, 1, "the rotated entry is released");

    // Two viewers at different maxima keep one entry each.
    let store = HeldStore::new(HeldMode::On, usize::MAX);
    let mut tabs = [Tab::default(), Tab::default()];
    for (poll, ns) in [
        [2_000i64, 2_000],
        [2_000, 2_000],
        [2_010, 2_000],
        [2_020, 2_010],
        [2_030, 2_020],
    ]
    .into_iter()
    .enumerate()
    {
        for (tab, n) in tabs.iter_mut().zip(ns) {
            tab.serve(&store, &request, &grow(n), false);
        }
        if poll >= 2 {
            assert_eq!(store.len_and_bytes().0, 2, "poll {poll}");
        }
    }
}

/// Two tabs holding the same response both advance from one shared entry; a panic in the advance serves the full build, counted as a fallback that turns held state off until restart, in shadow mode as in `on` (the chart would panic again on every poll).
#[test]
fn viewers_share_entries_and_a_panicking_advance_serves_the_full_build() {
    let request = req(&["a", "b"], 300);
    let store = HeldStore::new(HeldMode::On, usize::MAX);
    // Both tabs paint, continue once (captured), then poll each append in lockstep.
    held::take_outcomes();
    let mut tabs = [Tab::default(), Tab::default()];
    for n in [2_000i64, 2_000, 2_010, 2_020, 2_030] {
        for tab in &mut tabs {
            tab.serve(&store, &request, &grow(n), false);
        }
    }
    let counted = held::take_outcomes();
    assert_eq!(counted.get("served"), Some(&6), "{counted:?}");

    for mode in [HeldMode::Shadow, HeldMode::On] {
        let store = HeldStore::new(mode, usize::MAX);
        held::take_outcomes();
        let mut tab = Tab::default();
        for (poll, n) in [1_000i64, 1_000, 1_000, 1_010].into_iter().enumerate() {
            if poll == 3 {
                held::PANIC_NEXT_ADVANCE.set(true);
            }
            tab.serve(&store, &request, &grow(n), false);
        }
        let counted = held::take_outcomes();
        assert_eq!(store.mode(), HeldMode::Off, "{mode:?} {counted:?}");
        assert_eq!(
            counted.get("shadow_divergence"),
            None,
            "{mode:?} {counted:?}"
        );
        // Off, the next poll holds nothing and answers in full.
        tab.serve(&store, &request, &grow(1_020), false);
        assert_eq!(store.len_and_bytes(), (0, 0), "{mode:?}");
    }
}

/// Held state turned off while an `on`-mode advance runs (another poll's audit failure or panic) serves none of that advance: a poll trying it builds in full, and one expecting only the advance answers `None`, then builds as off mode does.
#[test]
fn an_advance_running_as_held_state_turns_off_is_not_served() {
    let request = req(&["a", "b"], 300);
    for attempt in [held::Advance::Try, held::Advance::Only] {
        let store = HeldStore::new(HeldMode::On, usize::MAX);
        let mut tab = Tab::default();
        for n in [1_000i64, 1_000, 1_010] {
            tab.serve(&store, &request, &grow(n), false);
        }
        // The entry is corrupt, as the audit that turns held state off found: served, the advance would answer wrongly.
        store.tamper_for_test();
        held::DISABLE_AT_NEXT_ADVANCE.set(true);
        held::take_outcomes();
        tab.poll(&request, &grow(1_020), |r, snapshots| {
            let truth = build_chart(r, snapshots, None, false, None, &|_| {}, false)?;
            let build = |attempt| {
                let key = Some(held::request_key(r));
                held::build(
                    &store,
                    r,
                    &chart_params(r),
                    key,
                    snapshots,
                    None,
                    false,
                    None,
                    &|_| {},
                    attempt,
                )
            };
            let built = match build(attempt)? {
                Some(built) => built,
                None => {
                    assert!(attempt == held::Advance::Only);
                    build(held::Advance::Skip)?.unwrap()
                }
            };
            assert!(!built.advanced);
            assert_eq!(built.response, truth.response);
            assert_eq!(built.kind, truth.kind);
            Ok(built.response)
        });
        assert_eq!(store.mode(), HeldMode::Off);
        assert_eq!(held::take_outcomes().get("served"), None);
    }
}

/// The per-key cap evicts the least recently used entry; one a viewer may still continue from is counted by cause, so the cap's cliff (five or more viewers of one chart polling in turn at different maxima, every poll answering in full) shows apart from `cutoff` fallbacks of clients holding other responses.
#[test]
fn evictions_of_viewed_entries_are_counted() {
    let request = req(&["a", "b"], 300);
    let store = HeldStore::new(HeldMode::On, usize::MAX);
    let ((), rendered) = crate::with_metrics(|| {
        let mut tabs: Vec<Tab> = (0..5).map(|_| Tab::default()).collect();
        for round in 0..4i64 {
            for (i, tab) in tabs.iter_mut().enumerate() {
                tab.serve(
                    &store,
                    &request,
                    &grow(2_000 + 10 * (5 * round + i as i64)),
                    false,
                );
            }
        }
    });
    let counted = |cause: &str| -> u64 {
        let prefix = format!("mkdb2_chart_held_evicted_total{{cause=\"{cause}\"}} ");
        rendered
            .lines()
            .find_map(|line| line.strip_prefix(prefix.as_str()))
            .map_or(0, |n| n.parse().unwrap())
    };
    // From the second round on, every poll's insert evicts the entry of the viewer polling next.
    assert!(counted("cap") >= 10, "{rendered}");
    assert_eq!(counted("budget"), 0, "{rendered}");
}

/// A panic in the store itself (taking, releasing or inserting an entry, under its lock) serves the poll as off mode would, in shadow and `on` mode, and turns held state off until restart instead of poisoning every later poll.
#[test]
fn a_panicking_store_serves_the_poll() {
    let request = req(&["a", "b"], 300);
    for mode in [HeldMode::Shadow, HeldMode::On] {
        // The poll after the state is held takes its entry, releases it and inserts the next: each panics in turn.
        for skip in 0..3 {
            let store = HeldStore::new(mode, usize::MAX);
            let mut tab = Tab::default();
            for (poll, n) in [1_000i64, 1_000, 1_010].into_iter().enumerate() {
                if poll == 2 {
                    held::PANIC_STORE_AFTER.set(Some(skip));
                }
                tab.serve(&store, &request, &grow(n), false);
            }
            assert_eq!(store.mode(), HeldMode::Off, "{mode:?} skip {skip}");
            assert_eq!(store.len_and_bytes(), (0, 0), "{mode:?} skip {skip}");
        }
    }
}

/// A diverging chart keeps its full build's state and diverges again on its next poll: each chart and part is logged once, every divergence counted.
#[test]
fn shadow_divergences_are_logged_once_per_chart_and_part() {
    let store = HeldStore::new(HeldMode::Shadow, usize::MAX);
    assert!(store.first_divergence(&[1; 32], "reply"));
    assert!(!store.first_divergence(&[1; 32], "reply"));
    assert!(store.first_divergence(&[1; 32], "panic"));
    assert!(store.first_divergence(&[2; 32], "reply"));
    assert!(!store.first_divergence(&[2; 32], "reply"));
}

/// A shadow divergence serves the full build, is counted, keeps shadow mode on and holds the full build's state, so the next poll matches again.
#[test]
fn a_shadow_divergence_serves_the_full_build_and_keeps_shadow_on() {
    let request = req(&["a", "b"], 300);
    let store = HeldStore::new(HeldMode::Shadow, usize::MAX);
    let mut tab = Tab::default();
    held::take_outcomes();
    for n in [1_000i64, 1_000, 1_010] {
        tab.serve(&store, &request, &grow(n), false);
    }
    let counted = held::take_outcomes();
    assert_eq!(counted.get("shadow_match"), Some(&1), "{counted:?}");
    store.tamper_for_test();
    // Tab::serve asserts the served reply and kind equal the full build's.
    tab.serve(&store, &request, &grow(1_020), false);
    let counted = held::take_outcomes();
    assert_eq!(counted.get("shadow_divergence"), Some(&1), "{counted:?}");
    assert_eq!(store.mode(), HeldMode::Shadow);
    tab.serve(&store, &request, &grow(1_030), false);
    let counted = held::take_outcomes();
    assert_eq!(counted.get("shadow_match"), Some(&1), "{counted:?}");
}

/// A panic while capturing a full build's state serves the full build and turns held state off until restart, with or without an entry held, in shadow mode and under `on` mode's audit, where no advance follows it; later polls build in full.
#[test]
fn a_panicking_capture_serves_the_full_build() {
    let request = req(&["a", "b"], 300);
    // (mode, the poll whose capture panics, whether that poll is audited)
    for (mode, panics_at, audit) in [
        (HeldMode::Shadow, 1, false),
        (HeldMode::On, 1, false),
        (HeldMode::Shadow, 2, false),
        (HeldMode::On, 2, true),
    ] {
        let store = HeldStore::new(mode, usize::MAX);
        let mut tab = Tab::default();
        for (poll, n) in [1_000i64, 1_000, 1_010, 1_020].into_iter().enumerate() {
            if poll == panics_at {
                held::PANIC_NEXT_CAPTURE.set(true);
            }
            held::take_outcomes();
            tab.serve(&store, &request, &grow(n), audit && poll == panics_at);
            if poll == panics_at && (mode == HeldMode::Shadow || audit) {
                assert_eq!(
                    held::take_outcomes(),
                    BTreeMap::new(),
                    "{mode:?}: no advance follows"
                );
            }
            let off = poll >= panics_at;
            assert_eq!(store.mode() == HeldMode::Off, off, "{mode:?} poll {poll}");
            if off {
                assert_eq!(store.len_and_bytes(), (0, 0), "{mode:?} poll {poll}");
            }
        }
    }
}

/// The store charges at least what it keeps allocated, its containers included: many tiny charts, each a new key.
#[test]
fn held_store_charges_cover_what_it_retains() {
    let store = HeldStore::new(HeldMode::On, usize::MAX);
    let snapshots = vec![Arc::new(SeriesSnapshot::full(rows(20, 3).as_ref().clone()))];
    let requests: Vec<proto::ChartRequest> =
        (0..8_192).map(|i| req(&["a"], 100 + i as u32)).collect();
    let ((), _, live) = super::super::alloc_probe::peak_and_live_bytes(|| {
        for request in &requests {
            let built = build_chart(request, &snapshots, None, false, None, &|_| {}, true).unwrap();
            store.insert(held::request_key(request), built.held.unwrap().unwrap());
        }
    });
    let (entries, charged) = store.len_and_bytes();
    println!("HELD RETAINED entries={entries} live={live} charged={charged}");
    assert_eq!(entries, 8_192);
    assert!(
        charged as i64 >= live,
        "the store retains {live} bytes and charges {charged}"
    );
}

/// Past its byte budget the store evicts as it inserts, and the removals leave its map's buckets in place while `capacity()` drops: the map stays charged at its buckets, and the charge covers what the store retains.
#[test]
fn held_store_charges_cover_what_it_retains_under_eviction() {
    let snapshots = vec![Arc::new(SeriesSnapshot::full(rows(20, 3).as_ref().clone()))];
    let requests: Vec<proto::ChartRequest> =
        (0..4_096).map(|i| req(&["a"], 100 + i as u32)).collect();
    let insert = |store: &HeldStore, request: &proto::ChartRequest| {
        let built = build_chart(request, &snapshots, None, false, None, &|_| {}, true).unwrap();
        store.insert(held::request_key(request), built.held.unwrap().unwrap());
    };
    let probe = HeldStore::new(HeldMode::On, usize::MAX);
    for request in &requests[..1_500] {
        insert(&probe, request);
    }
    // A budget of about 1,500 entries (a map near its 7/8 load, where removals leave tombstones), filled over twice.
    let store = HeldStore::new(HeldMode::On, probe.len_and_bytes().1);
    drop(probe);
    let mut peak = 0;
    let mut lowered = false;
    let ((), _, live) = super::super::alloc_probe::peak_and_live_bytes(|| {
        for request in &requests {
            insert(&store, request);
            // The store also sees capacities mid-insert (before its evictions), so its charge can exceed what this loop sees.
            let (charged, now) = store.map_capacity_for_test();
            peak = peak.max(now);
            lowered |= now < charged;
            assert!(
                charged >= peak,
                "the map is charged at its largest capacity: {charged} < {peak}"
            );
        }
    });
    assert!(
        lowered,
        "removals lowered capacity() while the buckets stayed"
    );
    let (entries, charged) = store.len_and_bytes();
    println!("HELD RETAINED UNDER EVICTION entries={entries} live={live} charged={charged}");
    assert!(entries < 1_600);
    assert!(
        charged as i64 >= live,
        "the store retains {live} bytes and charges {charged}"
    );
}

/// A first paint and a continuation poll of `request` over `held_rows`, which leave its state in `store`, then its poll over `grown` through the service's build step, measured: what it built, the most bytes it held, and the model shapes it reported.
fn measured_poll(
    store: &HeldStore,
    request: &proto::ChartRequest,
    held_rows: &[Arc<Vec<VersionedRawPoint>>],
    grown: &[Arc<Vec<VersionedRawPoint>>],
    audit: bool,
) -> (BuiltChart, u64, Vec<DenseShape>) {
    let mut tab = Tab::default();
    for _ in 0..2 {
        tab.poll(request, held_rows, |r, snapshots| {
            Ok(build_held(store, r, snapshots, false, None)?.response)
        });
    }
    let mut next = request.clone();
    next.cache_state = tab.0.as_ref().and_then(|held| echo(held));
    let snapshots = fixture_snapshots(&next, grown);
    let shapes = std::cell::RefCell::new(Vec::new());
    let p = chart_params(&next);
    let (built, peak, _) = super::super::alloc_probe::peak_and_live_bytes(|| {
        held::build(
            store,
            &next,
            &p,
            Some(held::request_key(&next)),
            &snapshots,
            None,
            audit,
            None,
            &|s| shapes.borrow_mut().push(s),
            held::Advance::Try,
        )
        .unwrap()
        .unwrap()
    });
    (built, peak, shapes.into_inner())
}

/// Stage 2 charges what the service's build step holds in every mode, within a small factor: a full build with its state captured, an advance served, and a full build with an advance beside it (shadow, audit), whose two models it charges. A Savitzky-Golay advance copies the windows of the slots it re-evaluates, charged at their capacity.
#[test]
fn held_build_charges_cover_measured_peaks() {
    let runs: Vec<String> = (0..16).map(|r| format!("run-{r}")).collect();
    let refs: Vec<&str> = runs.iter().map(String::as_str).collect();
    let mut request = req(&refs, 100);
    request.smoothing = smoothing(Algorithm::Ema, 0, 4.0, 0);
    let mut grid = req(&refs, 100);
    grid.smoothing = smoothing(Algorithm::SavitzkyGolay, 5, 0.0, 1);
    let mut irregular = req(&["a", "b"], 1_000);
    irregular.use_timestamp_axis = true;
    irregular.smoothing = smoothing(Algorithm::SavitzkyGolay, 200, 0.0, 2);
    // Stage 2's probe chart, 16 runs of 64 tags of 4 rows (a dense model that dwarfs its rows), then a row more per tag.
    let tagged = |grown| -> Vec<Arc<Vec<VersionedRawPoint>>> {
        let (_, rows) = tagged_chart(16, 64, 4, grown);
        rows.into_iter().map(Arc::new).collect()
    };
    let ts = |i: i64| 1_000_000 + 10 * i + (i * 7) % 5;
    let timed = |n: i64| vec![rows_shaped(n, |i| i, ts), rows_shaped(n, |i| i, ts)];
    for (request, held_rows, grown) in [
        (request, tagged(0), tagged(1)),
        (grid, tagged(0), tagged(1)),
        (irregular, timed(4_000), timed(4_020)),
    ] {
        for (mode, budget, audit) in [
            (HeldMode::Off, usize::MAX, false),
            (HeldMode::On, 0, false),
            (HeldMode::On, usize::MAX, false),
            (HeldMode::Shadow, usize::MAX, false),
            (HeldMode::On, usize::MAX, true),
        ] {
            let store = HeldStore::new(mode, budget);
            // The measured poll: rows appended to every tag.
            let (built, peak, shapes) = measured_poll(&store, &request, &held_rows, &grown, audit);
            let (response, built_in_full) = (built.response, !built.advanced);
            let transients: u64 = if built_in_full {
                grown
                    .iter()
                    .map(|rows| rows.len() as u64 * budget::BUILD_BYTES_PER_ROW)
                    .sum()
            } else {
                0
            };
            let shape = *shapes.last().expect("every path reports its model");
            // An advance beside the full build has its model charged with the full build's, before it exists.
            assert_eq!(shapes[0].models, shape.models, "{mode:?} audit={audit}");
            let charged = transients + ChartPlan::output_bytes(shape, audit);
            eprintln!(
                "PROBE held mode={mode:?} budget={budget} audit={audit} full={built_in_full} models={} peak={peak} charged={charged} delta={}",
                shape.models,
                response.delta
            );
            assert!(
                peak <= charged && charged <= 3 * peak,
                "{mode:?} budget={budget} audit={audit}: held {peak} bytes, charged {charged}"
            );
        }
    }
}

/// Two tabs at one entry, and a full build that answers `unchanged` while rows its chart does not show move the maxima: the memo then answers the other tab, whose viewer moves with it to the entry at the new maxima, so the old entry is not left on record and both tabs advance on the next append.
#[test]
fn a_memo_hit_moves_its_viewer_with_the_maxima() {
    use crate::query::memo::ChartMemo;
    let cache = SeriesCache::with_budget(usize::MAX);
    let key = SeriesKey::new("p", "a", "loss");
    let mut stamp = 1_000i64;
    let mut next = [0i64, 0i64];
    let mut rows_for = |tag: usize, n: i64, stamp: i64| -> Vec<VersionedRawPoint> {
        let from = next[tag];
        next[tag] += n;
        (from..from + n)
            .map(|step| {
                let (tag, value) = (format!("g{tag}"), ((step * 7) % 101) as f32);
                point(&tag, step, 1_000_000 + step * 10, value, stamp)
            })
            .collect()
    };
    let mut initial = rows_for(0, 300, stamp);
    initial.extend(rows_for(1, 300, stamp));
    let snapshot = cache.insert_full(key.clone(), initial, Instant::now());
    // The chart shows g0 only.
    let mut request = req(&["a"], 300);
    request.y_series[0].tags = vec!["g0".into()];
    let store = HeldStore::new(HeldMode::On, usize::MAX);
    let memo = ChartMemo::new(true);
    // A poll through the memo and held state ([`memo_or_held`]), checked against the full build. Whether the memo answered.
    let serve = |client: &mut Option<proto::ChartResponse>,
                 snapshots: &[Arc<SeriesSnapshot>],
                 audit: bool|
     -> bool {
        let mut r = request.clone();
        r.cache_state = client.as_ref().and_then(echo);
        let truth = build_chart(&r, snapshots, None, false, None, &|_| {}, false).unwrap();
        let (reply, advanced) = memo_or_held(&memo, &store, &r, snapshots, audit).unwrap();
        assert_eq!(reply, truth.response);
        *client = Some(reply);
        advanced.is_none()
    };
    let (mut a, mut b) = (None, None);
    let s = [snapshot];
    // A paints and continues (captured), then B paints from A's entry: one entry, two viewers.
    assert!(!serve(&mut a, &s, false));
    assert!(!serve(&mut a, &s, false));
    assert!(!serve(&mut b, &s, false));
    assert_eq!(store.len_and_bytes().0, 1);
    // Rows in g1 only: A's audited poll builds an unchanged reply at the moved maxima, which the memo answers B with.
    stamp += 10;
    let s = [cache_append(&cache, &key, rows_for(1, 5, stamp))];
    held::take_outcomes();
    assert!(!serve(&mut a, &s, true));
    assert!(a.as_ref().unwrap().delta && a.as_ref().unwrap().x_values.is_empty());
    assert!(serve(&mut b, &s, false), "the memo answers B");
    assert_eq!(held::take_outcomes(), BTreeMap::from([("audit_ok", 1)]));
    assert_eq!(
        store.len_and_bytes().0,
        1,
        "B's viewer left the old entry with A's"
    );
    // Rows the chart shows: both tabs advance, from one entry to one entry.
    stamp += 10;
    let s = [cache_append(&cache, &key, rows_for(0, 5, stamp))];
    assert!(!serve(&mut a, &s, false));
    assert!(!serve(&mut b, &s, false));
    assert_eq!(held::take_outcomes(), BTreeMap::from([("served", 2)]));
    assert_eq!(store.len_and_bytes().0, 1);
}

/// A late re-log stamped below the maximum, then a rewrite, puts entries at one maxima on two lineages: a poll takes the one on its snapshots' lineage, never the other, whose model differs.
#[test]
fn entries_at_one_maxima_on_two_lineages_advance_their_own() {
    let cache = SeriesCache::with_budget(usize::MAX);
    let key = SeriesKey::new("p", "a", "loss");
    let row =
        |step: i64, value: f32, stamp: i64| point("", step, 1_000_000 + step * 10, value, stamp);
    let mut db: Vec<VersionedRawPoint> = (0..200)
        .map(|s| row(s, (s % 13) as f32, 1_000 + s))
        .collect();
    let first = cache.insert_full(key.clone(), db.clone(), Instant::now());
    let request = req(&["a"], 300);
    let store = HeldStore::new(HeldMode::On, usize::MAX);
    let serve = |client: &mut Option<proto::ChartResponse>, snapshot: &Arc<SeriesSnapshot>| {
        let mut r = request.clone();
        r.cache_state = client.as_ref().and_then(echo);
        let snapshots = std::slice::from_ref(snapshot);
        let truth = build_chart(&r, snapshots, None, false, None, &|_| {}, false).unwrap();
        let built = build_held(&store, &r, snapshots, false, None).unwrap();
        assert_eq!(
            built.response, truth.response,
            "served differs from the full build"
        );
        assert_eq!(built.kind, truth.kind);
        *client = Some(built.response);
    };
    let (mut a, mut b) = (None, None);
    serve(&mut b, &first);
    serve(&mut b, &first);
    // Step 50 re-logged below the maximum: the increment is refused, and the full read starts a new lineage at the same maximum.
    db[50] = row(50, 999.0, 1_100);
    cache.note_bumps(std::iter::once("a"));
    let Lookup::Stale { base, .. } = cache.lookup(&key) else {
        panic!("stale");
    };
    assert!(cache
        .apply_increment(&key, vec![db[50].clone()], Instant::now(), &base)
        .is_err());
    let rewritten = cache.insert_full_with_origin(
        key.clone(),
        db.clone(),
        Instant::now(),
        LineageOrigin::Rewrite,
    );
    assert_ne!(rewritten.lineage(), first.lineage());
    assert_eq!(rewritten.maximum(), first.maximum());
    serve(&mut a, &rewritten);
    serve(&mut a, &rewritten);
    assert_eq!(store.len_and_bytes().0, 2, "one entry per lineage");
    let appended = cache_append(
        &cache,
        &key,
        (200..205).map(|s| row(s, 5.0, 2_000 + s)).collect(),
    );
    assert_eq!(appended.lineage(), rewritten.lineage());
    held::take_outcomes();
    serve(&mut a, &appended);
    assert_eq!(held::take_outcomes(), BTreeMap::from([("served", 1)]));
}

/// A causal slot ahead of its run's samples in the cell has no value, in an advance as in a full build: run a resumes at step 90 inside a cell b centers at 87, then b's appends move the center past 90 and a's slot gains its value, then a appends again.
#[test]
fn a_causal_slot_gains_its_value_when_the_center_passes_its_first_sample() {
    for algorithm in [Algorithm::Ema, Algorithm::Triangular] {
        for use_time in [false, true] {
            let row =
                |step: i64, value: f32, stamp: i64| point("", step, step * 1_000, value, stamp);
            let mut a: Vec<_> = (0..8).map(|s| row(s, (s * s) as f32, 1_000)).collect();
            let mut b: Vec<_> = (0..111).map(|s| row(s, (s % 7) as f32, 1_000)).collect();
            let mut r = req(&["a", "b"], 2);
            r.use_timestamp_axis = use_time;
            r.smoothing = smoothing(algorithm, 3, 2.0, 2);
            let mut snapshots = vec![
                Arc::new(SeriesSnapshot::full(a.clone())),
                Arc::new(SeriesSnapshot::full(b.clone())),
            ];
            let (mut state, mut client) = continuation_state(&r, &snapshots);
            for phase in 0..3 {
                match phase {
                    0 => a.push(row(90, 5.0, 2_000)),
                    1 => b.extend((111..128).map(|s| row(s, (s % 7) as f32, 3_000))),
                    _ => a.push(row(91, 5.5, 4_000)),
                }
                snapshots = snapshots
                    .iter()
                    .zip([&a, &b])
                    .map(|(old, rows)| old.refreshed_fixture(rows))
                    .collect();
                let at = format!("{algorithm:?} time={use_time} phase={phase}");
                let poll = continuation(&r, &client);
                let advanced = advance_as_full_build(&state, &poll, &snapshots, &at);
                let model = advanced.next.model();
                assert_eq!(model.x_values.len(), 2, "{at}");
                assert_eq!(model.series[0].values[1].is_finite(), phase > 0, "{at}");
                client = advanced.response;
                state = advanced.next;
            }
        }
    }
}

/// A held raw slot exactly at the slot rule's x is re-evaluated: the appended 1,034 less window 4's half-width 2 is the slot at 1,032, whose window gains the appended sample at its zero-weight edge and turns symmetric (1,030 to 1,034), which fits as a dot product, and these values round to another f32 than the held direct fit over 1,030 to 1,033. Appended at 1,035 instead, the rule's x is the last held sample, 1,033, where the delta starts.
#[test]
fn a_raw_slot_at_the_slot_rules_x_is_reevaluated() {
    let ys: [f32; 11] = [
        531.407_65,
        -706.696_2,
        -287.198_12,
        536.423_3,
        -671.371_34,
        862.773_5,
        956.301_6,
        20.532_593,
        650.568_7,
        -221.551_47,
        996.490_3,
    ];
    let row = |i: i64, stamp: i64| {
        let step = 1_024 + i;
        point("", step, step * 1_000, ys[i as usize], stamp)
    };
    let a: Vec<_> = (0..10).map(|i| row(i, 1_000)).collect();
    let mut r = req(&["a"], 0);
    r.smoothing = smoothing(Algorithm::SavitzkyGolay, 4, 1.0, 0);
    let plan = chart::SmoothingPlan::Median(1f64.to_bits());
    let rule = chart::SlotSmoother::new(Algorithm::SavitzkyGolay, 4, 1.0, 0, plan);
    let held = vec![Arc::new(SeriesSnapshot::full(a.clone()))];
    let (state, client) = continuation_state(&r, &held);
    let poll = continuation(&r, &client);
    for (step, rule_x) in [(1_034, 1_032.0), (1_035, 1_033.0)] {
        assert_eq!(rule.dirty_from(Some(step as f64)), rule_x);
        let mut a = a.clone();
        a.push(point("", step, step * 1_000, ys[10], 2_000));
        let snapshots = [held[0].refreshed_fixture(&a)];
        let advanced = advance_as_full_build(&state, &poll, &snapshots, &format!("at {step}"));
        assert_eq!(advanced.kind, Some("delta"));
        assert_eq!(advanced.response.x_values[0], rule_x, "the delta's first x");
        if step == 1_034 {
            let at = |model: &DenseChart| {
                let k = model.x_values.iter().position(|&x| x == rule_x).unwrap();
                model.series[0].values[k].to_bits()
            };
            assert_ne!(
                at(state.model()),
                at(advanced.next.model()),
                "the slot's value changes"
            );
        }
    }
}

/// An envelope slot exactly at the slot rule's x is re-evaluated too: run a logs every third step from 1,020, so cell [1,032, 1,040)'s slot sits at its sample 1,035, the rule's x for the appended 1,041 less window 4's half-width 6 at spacing 3. The window gains 1,041 at its zero-weight edge and turns symmetric (1,029 to 1,041), which fits as a dot product: the held direct fit over 1,032 to 1,038 to rounding, which an f32 value rarely shows, so the test counts the re-evaluation.
#[test]
fn an_envelope_slot_at_the_slot_rules_x_is_reevaluated() {
    let ys: [f32; 8] = [
        -930.3273, -151.19495, -830.90076, 866.26685, -497.5519, -469.7627, -832.6223, 159.79578,
    ];
    let row = |i: usize, stamp: i64| {
        let step = 1_020 + 3 * i as i64;
        point("", step, step * 1_000, ys[i], stamp)
    };
    let mut a: Vec<_> = (0..7).map(|i| row(i, 1_000)).collect();
    let mut r = req(&["a"], 4);
    r.smoothing = smoothing(Algorithm::SavitzkyGolay, 4, 1.0, 0);
    let held = vec![Arc::new(SeriesSnapshot::full(a.clone()))];
    let (state, client) = continuation_state(&r, &held);
    a.push(row(7, 2_000));
    let snapshots = [held[0].refreshed_fixture(&a)];
    let poll = continuation(&r, &client);
    let advanced = advance_as_full_build(&state, &poll, &snapshots, "");
    assert!(advanced.next.model().x_values.contains(&1_035.0));
    assert_eq!(advanced.work.rebuild.reached, 1, "the slot at 1,035");
}

/// An advance's work follows its appended rows: a causal series appending k rows over s new envelope cells folds each appended sample once across its ascending slots, not once per slot (about s * k / 2 folds).
#[test]
fn causal_append_work_is_linear_in_the_appended_rows() {
    let history = 100_000i64;
    let rows = |n: i64| {
        let stamp = |s| if s < history { 1_000 } else { 2_000 };
        let rows = (0..n).map(|s| point("", s, 1_000_000 + s, ((s * 37) % 101) as f32, stamp(s)));
        Arc::new(rows.collect::<Vec<_>>())
    };
    let mut r = req(&["a"], 2000);
    r.smoothing = smoothing(Algorithm::Ema, 0, 20.0, 1);
    let work: Vec<usize> = [1_024i64, 4_096, 16_384]
        .into_iter()
        .map(|appended| {
            let (outcome, truth) =
                advance_between(&r, &[rows(history)], &[rows(history + appended)]);
            let advanced = outcome.unwrap_or_else(|reason| panic!("{appended}: {reason:?}"));
            assert_eq!(advanced.response, truth);
            advanced.work.rows + advanced.work.smoothed
        })
        .collect();
    assert!(
        work[2] <= 6 * work[1],
        "work grew {}x for 4x the appended rows: {work:?}",
        work[2] as f64 / work[1] as f64
    );
}

/// Held state advances bit for bit on a log ladder whose smoothing floor grows with x ([`chart::Floor`]): the causal fold's time constant and the Savitzky-Golay half-width grow along the ladder, on step and timestamp axes alike.
#[test]
fn held_state_advances_bit_identically_on_a_growing_log_floor() {
    for (algorithm, use_time) in [
        (Algorithm::Ema, false),
        (Algorithm::Ema, true),
        (Algorithm::Triangular, true),
        (Algorithm::SavitzkyGolay, false),
        (Algorithm::SavitzkyGolay, true),
    ] {
        // Eight samples an octave over five octaves, against five ladder cells: one bucket an octave (m = 0), so the floor outgrows the time constant and the window early.
        let row = |step: i64| {
            let x = (1i64 << (step / 8)) * 8 + step % 8;
            VersionedRawPoint {
                timestamp_ms: if use_time { x * 1_000 } else { step },
                ..point(
                    "",
                    if use_time { step } else { x },
                    0,
                    ((step * 7) % 31) as f32,
                    1_000 + step,
                )
            }
        };
        let rows = |n: i64| Arc::new(SeriesSnapshot::full((0..n).map(row).collect::<Vec<_>>()));
        let mut req = req(&["a", "b"], 5);
        req.use_timestamp_axis = use_time;
        req.log_buckets = true;
        req.smoothing = smoothing(algorithm, 20, 4.0, 1);
        let mut current = vec![rows(40), rows(40)];
        let (mut state, mut client) = continuation_state(&req, &current);
        for poll in 0..3 {
            current = current
                .iter()
                .map(|held| {
                    held.refreshed_fixture(&(0..43 + 3 * poll).map(row).collect::<Vec<_>>())
                })
                .collect();
            let at = format!("{algorithm:?} time={use_time} poll {poll}");
            let poll_req = continuation(&req, &client);
            let advanced = advance_as_full_build(&state, &poll_req, &current, &at);
            client = advanced.response;
            state = advanced.next;
        }
    }
}

/// An advance's copied Savitzky-Golay windows stay within the charge stage 2 holds for it: 16 runs whose windows span every held sample, each copy growing to at most its rows.
#[test]
fn copied_savgol_windows_stay_within_their_charge() {
    let runs: Vec<String> = (0..16).map(|r| format!("run-{r}")).collect();
    let refs: Vec<&str> = runs.iter().map(String::as_str).collect();
    for window in [1024, 4096] {
        let mut request = req(&refs, 4);
        request.smoothing = smoothing(Algorithm::SavitzkyGolay, window, 1.0, 0);
        let rows_at = |n| {
            refs.iter()
                .map(|_| rows_shaped(n, |i| 3 * i, |i| 1_000_000 + i))
                .collect::<Vec<_>>()
        };
        let held_rows = rows_at(i64::from(window));
        let grown = rows_at(i64::from(window) + 1);
        let store = HeldStore::new(HeldMode::On, usize::MAX);
        let (built, peak, shapes) = measured_poll(&store, &request, &held_rows, &grown, false);
        assert!(built.advanced, "window={window}: must measure an advance");
        let shape = *shapes.last().expect("every advance reports its model");
        let charged = ChartPlan::output_bytes(shape, false);
        assert!(
            peak <= charged,
            "window={window}: held {peak} bytes, charged {charged}"
        );
    }
}

/// A window copy restarted apart from the last one restarts its cursor too: run a's appended 1,000 to 1,008, past an isolated NaN at 300, center a window at 1,004 on the held windows' gap of 2, and a cursor carried from the earlier copy's even run would index past the new copy.
#[test]
fn a_restarted_window_copy_restarts_its_cursor() {
    for order in 0..=2u32 {
        let row = |s: i64, v: f32, stamp: i64| point("", s, 1_000 + s, v, stamp);
        let mut a: Vec<_> = (0..50)
            .map(|i| row(2 * i, ((i * 37) % 101) as f32, 1_000))
            .collect();
        let mut r = req(&["a"], 0);
        r.smoothing = smoothing(Algorithm::SavitzkyGolay, 4, 1.0, order);
        let held = vec![Arc::new(SeriesSnapshot::full(a.clone()))];
        let (state, client) = continuation_state(&r, &held);
        a.extend([100, 102, 104, 106, 108].map(|s| row(s, ((s * 13) % 97) as f32, 2_000)));
        a.push(row(300, f32::NAN, 2_000));
        let resumed = [
            (1_000, 5.0),
            (1_001, -40.0),
            (1_004, 17.0),
            (1_007, 90.0),
            (1_008, -3.0),
        ];
        a.extend(resumed.map(|(s, v)| row(s, v, 2_000)));
        let snapshots = [held[0].refreshed_fixture(&a)];
        let poll = continuation(&r, &client);
        advance_as_full_build(&state, &poll, &snapshots, &format!("order {order}"));
    }
}

/// An advance that re-evaluates one early slot copies that slot's window, not the rest of the history after it: run b's new sample moves the first cell's center, so that slot's window in each run, four rows here, is the advance's whole copy, however long run a grows.
#[test]
fn an_early_window_copies_no_more_than_its_rows() {
    let mut peaks = Vec::new();
    for n in [2_048i64, 131_072] {
        let gap = n / 128;
        let a: Vec<_> = (0..4)
            .chain(gap..n)
            .map(|x| point("", x, x, (x % 13) as f32, 1_000))
            .collect();
        let mut b: Vec<_> = (0..3)
            .map(|x| point("", x, x, (x % 7) as f32, 1_000))
            .collect();
        let mut request = req(&["a", "b"], 128);
        request.smoothing = smoothing(Algorithm::SavitzkyGolay, 3, 1.0, 0);
        let before = vec![
            Arc::new(SeriesSnapshot::full(a.clone())),
            Arc::new(SeriesSnapshot::full(b.clone())),
        ];
        let (state, client) = continuation_state(&request, &before);
        b.push(point("", gap / 2, gap / 2, 42.0, 2_000));
        let current = vec![
            before[0].refreshed_fixture(&a),
            before[1].refreshed_fixture(&b),
        ];
        let poll = continuation(&request, &client);
        let (_, peak, _) = super::super::alloc_probe::peak_and_live_bytes(|| {
            advance_held(&state, &poll, &current).unwrap()
        });
        advance_as_full_build(&state, &poll, &current, &format!("{n} rows"));
        peaks.push(peak);
    }
    // 33 KB at either length, where a copy to the history's end would hold 2.1 MB at 131,072 rows.
    assert!(peaks[1] < 2 * peaks[0], "peaks {peaks:?}");
}

/// Held state answers a chart's first finite value in full, as the full build does: the client takes a flip of the banded bit only in a full answer.
#[test]
fn an_advance_gaining_the_first_finite_value_answers_full() {
    let r = req(&["a"], 300);
    let held = [rows_with(2_000, |_| f32::NAN)];
    let grown = [rows_with(2_100, |s| if s < 2_000 { f32::NAN } else { 1.0 })];
    let (outcome, truth) = advance_between(&r, &held, &grown);
    let advanced = outcome.unwrap_or_else(|reason| panic!("fell back {reason:?}"));
    assert!(truth.banded && !truth.delta);
    assert_eq!(advanced.response, truth);
}
