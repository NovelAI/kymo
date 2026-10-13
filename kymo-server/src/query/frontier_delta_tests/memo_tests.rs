//! Stage 1 (docs/chart-request-latency.md): the memo serves exactly the `unchanged` replies the full build would rebuild, and nothing else.
use super::*;
use crate::query::memo::{memo_key, request_hash, ChartMemo};

/// The audit's old-only reconstruction of a request the full build classified `unchanged` aligns with it, whether or not its smoothing needs the check, and the audited build classifies it `unchanged` too: an audited memo hit never meets the alignment gate, which would answer in full and disable the memo for a gate rather than a reply.
fn assert_audit_aligns(req: &proto::ChartRequest, snapshots: &[Arc<SeriesSnapshot>]) {
    let p = chart_params(req);
    let lineages = inspect_lineages(req, snapshots, echoed_frontiers(req));
    let held = lineages.verified();
    let prepared = prepare(req, &p, snapshots, None, held, false, None, false).unwrap();
    let plans: Vec<_> = prepared
        .series
        .iter()
        .filter(|ps| ps.continues)
        .map(|ps| (ps.identity.clone(), ps.smoothing_plan))
        .collect();
    let old = prepare(req, &p, snapshots, None, held, true, Some(&plans), false).unwrap();
    assert!(old.audit_plans_aligned, "{req:?}");
    let audited = build_chart(req, snapshots, None, true, None, &|_| {}, false).unwrap();
    assert_eq!(audited.kind, Some("unchanged"), "{req:?}");
}

/// The service's stage-1 path after a poll's reads: probe, else build and settle (an `unchanged` build checked against the audit's alignment gate). Returns the reply and whether the memo served it.
fn serve(
    memo: &ChartMemo,
    req: &proto::ChartRequest,
    snapshots: &[Arc<SeriesSnapshot>],
    audit: bool,
) -> (proto::ChartResponse, bool) {
    let key = memo.key_for(req);
    match probe_memo(memo, key, snapshots, audit, None) {
        ControlFlow::Break(reply) => (reply, true),
        ControlFlow::Continue(held) => {
            let built =
                build_chart(req, snapshots, None, audit, held.as_ref(), &|_| {}, false).unwrap();
            if built.kind == Some("unchanged") {
                assert_audit_aligns(req, snapshots);
            }
            count_delta(built.kind);
            settle_memo(memo, key, snapshots, &built);
            (built.response, false)
        }
    }
}

/// One poll answered by the stage-1 path and, on the same snapshots, by the full build; the two must be the same message.
fn poll(
    memo: &ChartMemo,
    req: &proto::ChartRequest,
    rows: &[Arc<Vec<VersionedRawPoint>>],
    audit: bool,
) -> (FixtureResponse, bool) {
    let mut served = false;
    let out = respond_with(req, rows, |snapshots| {
        let truth = super::super::build_response(req, snapshots, None, false)?;
        let (reply, was_served) = serve(memo, req, snapshots, audit);
        assert_eq!(
            reply, truth,
            "stage 1 must answer exactly what the full build does"
        );
        served = was_served;
        Ok(reply)
    })
    .unwrap();
    (out, served)
}

/// Poll each row state four times, echoing like a client; returns how many polls the memo served.
fn drive(req: &proto::ChartRequest, states: &[Vec<Arc<Vec<VersionedRawPoint>>>]) -> usize {
    let memo = ChartMemo::new(true);
    let mut held: Option<FixtureResponse> = None;
    let mut served = 0;
    for rows in states {
        for _ in 0..4 {
            let mut r = req.clone();
            r.cache_state = held.as_ref().and_then(|held| echo(held));
            let (out, was_served) = poll(&memo, &r, rows, false);
            served += usize::from(was_served);
            held = Some(out);
        }
    }
    served
}

#[test]
fn memo_answers_bit_identically_across_axes_smoothers_and_growth() {
    let mut rng = Rng(0x2545F4914F6CDD1D);
    let mut served_cases = 0;
    for case in 0..72u32 {
        let algorithm = ALGORITHMS[(case % 4) as usize];
        let use_time = (case / 4) % 2 == 1;
        let log = (case / 8) % 2 == 1;
        let zoom = (case / 16) % 2 == 1;
        let target = [120u32, 400, 10_000][(case % 3) as usize];
        let nruns = 1 + (rng.next() % 3) as usize;
        let mk = move |n: i64, seed: u64| -> Arc<Vec<VersionedRawPoint>> {
            Arc::new(
                (0..n)
                    .map(|s| {
                        let r = (s as u64).wrapping_mul(seed | 1).wrapping_add(seed >> 3);
                        VersionedRawPoint {
                            tag: String::new(),
                            step: if s < 2 && case % 5 == 3 { s - 2 } else { s },
                            timestamp_ms: if case % 7 == 6 && r % 41 == 7 {
                                -1 - s
                            } else {
                                1_000_000 + s * 7 + ((s % 9 == 0) as i64)
                            },
                            value: match r % 60 {
                                0 => f32::NAN,
                                1 => f32::INFINITY,
                                _ => ((r % 90_000) as f32) / 11.0 - 2000.0,
                            },
                            is_value: 1,
                            inserted_ms: s * 10_000_000,
                        }
                    })
                    .collect(),
            )
        };
        let seeds: Vec<u64> = (0..nruns).map(|_| rng.next()).collect();
        let lens: Vec<i64> = (0..nruns)
            .map(|_| 200 + (rng.next() % 1500) as i64)
            .collect();
        let states: Vec<Vec<_>> = [0, 7, 40]
            .iter()
            .map(|grow| {
                seeds
                    .iter()
                    .zip(&lens)
                    .map(|(&seed, &n)| mk(n + grow, seed))
                    .collect()
            })
            .collect();
        let mut r = req(
            &(0..nruns).map(|i| ["a", "b", "c"][i]).collect::<Vec<_>>(),
            target,
        );
        r.use_timestamp_axis = use_time;
        r.relative_time = use_time && case % 3 == 1;
        r.log_buckets = log;
        r.smoothing = smoothing(algorithm, 10, 100.0, 1);
        if zoom {
            r.step_min = Some(50);
            r.step_max = Some(900);
        }
        if drive(&r, &states) > 0 {
            served_cases += 1;
        }
    }
    assert!(
        served_cases > 60,
        "only {served_cases}/72 cases ever served a memo"
    );
}

/// Every memo comes from an `unchanged` build, which `serve` holds to the audit's alignment gate: here over tag groups, a filter, the scalar group yielding to tags, duplicate refs (labels from the request) and smoothers whose plans the client echoes (Savitzky-Golay; EMA on a time axis), beside the 72 cases above.
#[test]
fn memoized_replies_pass_the_audits_alignment_gate() {
    let tagged = |n: i64, scalar: bool| -> Arc<Vec<VersionedRawPoint>> {
        let tags: &[&str] = if scalar { &["", "0", "1"] } else { &["0", "1"] };
        let mut rows: Vec<VersionedRawPoint> = tags
            .iter()
            .flat_map(|tag| {
                (0..n).map(move |step| VersionedRawPoint {
                    tag: tag.to_string(),
                    timestamp_ms: 1_000_000 + step * 7,
                    ..scalar_row(step, ((step * 7) % 23) as f32)
                })
            })
            .collect();
        rows.sort_by(|a, b| (a.tag.as_str(), a.step).cmp(&(b.tag.as_str(), b.step)));
        Arc::new(rows)
    };
    let states =
        |scalar: bool| [400, 410].map(|n| vec![tagged(n, scalar), rows(n, 3), tagged(n, scalar)]);
    for case in 0..6 {
        let mut r = req(&["a", "b", "a"], 300);
        r.y_series[1].metric_name = "acc".into();
        if case % 2 == 1 {
            r.y_series[2].tags = vec!["1".into()];
        }
        r.smoothing = match case / 2 {
            0 => None,
            1 => smoothing(Algorithm::SavitzkyGolay, 10, 100.0, 1),
            _ => {
                r.use_timestamp_axis = true;
                smoothing(Algorithm::Ema, 10, 100.0, 1)
            }
        };
        for scalar in [false, true] {
            assert_eq!(
                drive(&r, &states(scalar)),
                4,
                "case {case}, scalar {scalar}: two served polls per unchanged state"
            );
        }
    }
}

#[test]
fn memo_never_serves_a_poll_the_full_build_answers_in_full() {
    // Duplicate timestamps on a passthrough time axis: the planner answers every identical poll in full (chart.rs passthrough_from_col), so nothing is ever memoized.
    let duplicated = rows_shaped(300, |i| i, |i| 1_000_000 + i / 2);
    let mut r = req(&["a"], 10_000);
    r.use_timestamp_axis = true;
    assert_eq!(drive(&r, &[vec![duplicated]]), 0);

    // A relative-time chart with a step range is gated: every echo answers in full.
    let mut r = req(&["a", "b"], 300);
    r.use_timestamp_axis = true;
    r.relative_time = true;
    r.step_min = Some(10);
    assert_eq!(drive(&r, &[vec![rows(500, 3), rows(400, 5)]]), 0);
}

/// A recorded memo for `r` over `rows`, and the reply the client holds.
fn memoized(
    memo: &ChartMemo,
    r: &proto::ChartRequest,
    rows: &[Arc<Vec<VersionedRawPoint>>],
) -> (proto::ChartRequest, FixtureResponse) {
    let (first, _) = poll(memo, r, rows, false);
    let mut continued = continuation(r, &first);
    let (unchanged, served) = poll(memo, &continued, rows, false);
    assert!(!served && unchanged.delta && unchanged.x_values.is_empty());
    continued.cache_state = echo(&unchanged);
    let (again, served) = poll(memo, &continued, rows, false);
    assert!(served, "an identical poll must be memo-served");
    (continued, again)
}

#[test]
fn memo_requires_the_same_canonical_request_and_echo() {
    let memo = ChartMemo::new(true);
    let r = req(&["a", "b"], 300);
    let rows = [rows(2000, 3), rows(1600, 7)];
    let (continued, held) = memoized(&memo, &r, &rows);
    let key = memo_key(&request_hash(&continued), &continued);

    // Every request field and the whole echo are keyed; the frontier map's order is not.
    let mut reordered = continued.clone();
    let state = reordered.cache_state.as_mut().unwrap();
    let entries: Vec<_> = state.frontiers.drain().collect();
    state.frontiers.extend(entries.into_iter().rev());
    assert_eq!(memo_key(&request_hash(&reordered), &reordered), key);
    let variants: Vec<fn(&mut proto::ChartRequest)> = vec![
        |r| r.target_resolution += 1,
        |r| r.step_min = Some(0),
        |r| r.step_max = Some(5_000),
        |r| r.use_timestamp_axis = true,
        |r| r.relative_time = true,
        |r| r.log_buckets = true,
        |r| r.smoothing = smoothing(Algorithm::Ema, 10, 100.0, 1),
        |r| r.x_series = Some(r.y_series[0].clone()),
        |r| r.y_series[1].tags.push("0".into()),
        |r| r.y_series.swap(0, 1),
        |r| r.cache_state.as_mut().unwrap().held_series = Some(3),
        |r| r.cache_state.as_mut().unwrap().held_series = None,
        |r| {
            r.cache_state
                .as_mut()
                .unwrap()
                .frontiers
                .values_mut()
                .for_each(|value| *value += 1)
        },
        |r| {
            r.cache_state
                .as_mut()
                .unwrap()
                .frontiers
                .insert("\0kymo:unknown".into(), 0);
        },
    ];
    for (i, change) in variants.into_iter().enumerate() {
        let mut changed = continued.clone();
        change(&mut changed);
        assert_ne!(
            memo_key(&request_hash(&changed), &changed),
            key,
            "variant {i} must change the key"
        );
        // Not served, on the very snapshots the memo was recorded for; whatever answers equals the full build.
        let truth = super::super::build_response(&changed, &held.snapshots, None, false).unwrap();
        let (reply, served) = serve(&memo, &changed, &held.snapshots, false);
        assert!(!served, "variant {i} was memo-served");
        assert_eq!(reply, truth, "variant {i}");
    }
    let (_, served) = serve(&memo, &continued, &held.snapshots, false);
    assert!(served, "the original key still serves");
}

#[test]
fn memo_requires_every_snapshot_lineage_and_maximum() {
    let memo = ChartMemo::new(true);
    let r = req(&["a", "b"], 300);
    let rows = [rows(2000, 3), rows(1600, 7)];
    let (continued, held) = memoized(&memo, &r, &rows);

    // The same rows under a new lineage (a reload without recovery): the memo is dropped, the build answers in full.
    let mut reloaded = held.snapshots.clone();
    reloaded[1] = Arc::new(SeriesSnapshot::full(rows[1].as_ref().clone()));
    assert_eq!(reloaded[1].maximum(), held.snapshots[1].maximum());
    let truth = super::super::build_response(&continued, &reloaded, None, false).unwrap();
    let (reply, served) = serve(&memo, &continued, &reloaded, false);
    assert!(!served && !reply.delta);
    assert_eq!(reply, truth);
    assert_eq!(memo.len_and_bytes().0, 0, "a stale memo is dropped");

    // An append moves one series' maximum: the poll builds a delta.
    let (continued, _held) = memoized(&memo, &r, &rows);
    let grown = [rows[0].clone(), super::rows(1610, 7)];
    let (out, served) = poll(&memo, &continued, &grown, false);
    assert!(!served && out.delta && !out.x_values.is_empty());
}

#[test]
fn kill_switch_and_custom_x_bypass_the_memo() {
    let off = ChartMemo::new(false);
    let r = req(&["a"], 300);
    let all = [rows(800, 3)];
    let mut held: Option<FixtureResponse> = None;
    for _ in 0..4 {
        let mut continued = r.clone();
        continued.cache_state = held.as_ref().and_then(|held| echo(held));
        let (out, served) = poll(&off, &continued, &all, false);
        assert!(!served);
        held = Some(out);
    }
    assert_eq!(off.len_and_bytes(), (0, 0));

    let on = ChartMemo::new(true);
    let mut custom = r.clone();
    custom.x_series = Some(r.y_series[0].clone());
    custom.cache_state = Some(proto::ChartCacheState {
        frontiers: [("\0kymo:x".to_string(), 1)].into_iter().collect(),
        held_series: Some(1),
    });
    assert!(on.key_for(&custom).is_none(), "custom-x never continues");
    let mut first_paint = r;
    first_paint.cache_state = None;
    assert!(
        on.key_for(&first_paint).is_none(),
        "an unchanged reply needs an echo"
    );
}

#[test]
fn memo_audit_failure_answers_in_full_and_turns_stage_one_off() {
    let memo = ChartMemo::new(true);
    let r = req(&["a", "b"], 300);
    let rows = [rows(2000, 3), rows(1600, 7)];
    let (continued, held) = memoized(&memo, &r, &rows);
    let key = memo_key(&request_hash(&continued), &continued);

    // An audited hit that agrees builds, serves the same reply and keeps stage 1 on.
    let (reply, served) = serve(&memo, &continued, &held.snapshots, true);
    assert!(!served && reply.delta && !reply.audit_failed);
    assert!(memo.enabled() && memo.len_and_bytes().0 == 1);

    // A memo the full build disagrees with answers in full, flagged and counted so, and stage 1 stays off.
    memo.tamper(&key, |reply| {
        reply.result_x_hash = reply.result_x_hash.map(|hash| hash ^ 1)
    });
    let ((reply, served), rendered) =
        crate::with_metrics(|| serve(&memo, &continued, &held.snapshots, true));
    assert!(!served && !reply.delta && reply.audit_failed);
    assert!(
        rendered
            .lines()
            .any(|line| line == "mkdb2_chart_delta_total{kind=\"audit_failed\"} 1")
            && !rendered.contains("kind=\"unchanged\""),
        "{rendered}"
    );
    let truth = super::super::build_response(&r, &held.snapshots, None, false).unwrap();
    assert_eq!(
        reply,
        proto::ChartResponse {
            audit_failed: true,
            ..truth
        },
        "exactly the full build's answer, flagged"
    );
    assert!(!memo.enabled());
    assert_eq!(memo.len_and_bytes(), (0, 0));
    for audit in [false, false, true] {
        let (reply, served) = serve(&memo, &continued, &held.snapshots, audit);
        assert!(!served && reply.delta && !reply.audit_failed);
        assert_eq!(memo.len_and_bytes(), (0, 0), "nothing is memoized again");
    }
}

/// A chart with no plottable point ships its unplottable counts unstamped: a memo equal to that reply matches, and any other trips the audit and answers in full, flagged, with nothing stamped.
#[test]
fn memo_audit_of_an_empty_chart_compares_the_unstamped_reply() {
    let mut r = req(&["a", "b"], 300);
    r.step_min = Some(1_000_000);
    let rows = [rows(200, 3), rows(160, 7)];
    respond_with(&r, &rows, |snapshots| {
        let clean = build_chart(&r, snapshots, None, true, None, &|_| {}, false)?;
        assert!(clean.response.x_values.is_empty() && clean.response.frontiers.is_empty());
        let same = build_chart(
            &r,
            snapshots,
            None,
            true,
            Some(&clean.response),
            &|_| {},
            false,
        )?;
        assert!(same.memo_audit == Some(true) && same.response == clean.response);
        let other = proto::ChartResponse {
            result_x_hash: Some(1),
            ..clean.response.clone()
        };
        let tripped = build_chart(&r, snapshots, None, true, Some(&other), &|_| {}, false)?;
        assert_eq!(tripped.memo_audit, Some(false));
        assert_eq!(tripped.kind, Some("audit_failed"));
        assert_eq!(
            tripped.response,
            proto::ChartResponse {
                audit_failed: true,
                ..clean.response.clone()
            }
        );
        Ok(clean.response)
    })
    .unwrap();
}

#[test]
fn memo_lru_respects_its_byte_bound() {
    let snapshots: Vec<Arc<SeriesSnapshot>> = (0..2)
        .map(|_| Arc::new(SeriesSnapshot::full(Vec::new())))
        .collect();
    let reply = proto::ChartResponse {
        series: vec![proto::ChartSeries {
            label: "x".repeat(200),
            ..Default::default()
        }],
        ..Default::default()
    };
    let probe = ChartMemo::new(true);
    probe.record([0; 32], &snapshots, &reply, None);
    let one = probe.len_and_bytes().1;
    assert!(one > 200);

    let memo = ChartMemo::with_budget(true, one * 4);
    for i in 0..4u8 {
        memo.record([i; 32], &snapshots, &reply, None);
    }
    assert_eq!(memo.len_and_bytes(), (4, one * 4));
    // Touch the oldest, then overflow: the least recently used (key 1) goes.
    assert!(memo.lookup(&[0; 32], &snapshots).is_some());
    memo.record([9; 32], &snapshots, &reply, None);
    assert_eq!(memo.len_and_bytes(), (4, one * 4));
    assert!(memo.lookup(&[1; 32], &snapshots).is_none());
    for key in [0u8, 2, 3, 9] {
        assert!(memo.lookup(&[key; 32], &snapshots).is_some(), "{key}");
    }
    // Re-recording a key replaces it instead of double counting.
    memo.record([9; 32], &snapshots, &reply, None);
    assert_eq!(memo.len_and_bytes(), (4, one * 4));
    // A memo larger than the whole budget is never retained.
    let tiny = ChartMemo::with_budget(true, one - 1);
    tiny.record([0; 32], &snapshots, &reply, None);
    assert_eq!(tiny.len_and_bytes(), (0, 0));
    // Other snapshots never match.
    let other: Vec<Arc<SeriesSnapshot>> = (0..2)
        .map(|_| Arc::new(SeriesSnapshot::full(Vec::new())))
        .collect();
    assert!(memo.lookup(&[0; 32], &other).is_none());
    assert!(memo.lookup(&[2; 32], &snapshots[..1]).is_none());
}

/// A panic under the memo's lock drops every memo and turns stage 1 off until restart, instead of failing every later poll.
#[test]
fn a_poisoned_memo_drops_every_memo_and_turns_stage_one_off() {
    let snapshots: Vec<Arc<SeriesSnapshot>> = (0..2)
        .map(|_| Arc::new(SeriesSnapshot::full(Vec::new())))
        .collect();
    let reply = proto::ChartResponse::default();
    let memo = ChartMemo::new(true);
    memo.record([1; 32], &snapshots, &reply, None);
    assert!(memo.lookup(&[1; 32], &snapshots).is_some());
    memo.poison_for_test();
    assert!(memo.lookup(&[1; 32], &snapshots).is_none());
    assert!(!memo.enabled());
    assert_eq!(memo.len_and_bytes(), (0, 0));
    memo.record([1; 32], &snapshots, &reply, None);
    assert_eq!(
        memo.len_and_bytes(),
        (0, 0),
        "a disabled memo stores nothing"
    );
}
