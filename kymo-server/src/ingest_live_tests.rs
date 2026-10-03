//! AI-1451: exercise the production insert, write-behind, and query paths with
//! disposable databases. Drive coalescer ticks explicitly; its spawned worker
//! owns process shutdown and would also make the lifecycle ordering nondeterministic.
//! Wire decoding, stream admission/ACK framing, and the periodic status watcher
//! are outside this suite; it starts at the shared production flush body.

use super::*;
use anyhow::{ensure, Context, Result};
use tokio::sync::broadcast::{error::TryRecvError, Receiver};

struct Fixture {
    project: String,
    ch: Arc<ChClient>,
    pg: Arc<PgStore>,
    bumps: Arc<BumpCoalescer>,
    gates: LifecycleGates,
    query: crate::query::QueryService,
    events: crate::events::EventSender,
    received: Receiver<VersionEvent>,
}

impl Fixture {
    async fn new(pg_url: &str, ch_url: &str) -> Result<Self> {
        let project = format!("ingest-timing-{}", crate::pg::unique_suffix());
        let pg = Arc::new(PgStore::connect(pg_url).await?);
        pg.ensure_run_metrics_run_fk().await?;
        let ch = Arc::new(ChClient::new(ch_url)?);
        tokio::time::timeout(Duration::from_secs(60), ch.ensure_schema()).await??;
        let bumps = BumpCoalescer::empty_for_test();
        let gates = LifecycleGates::new();
        let (events, received) = tokio::sync::broadcast::channel(32);
        let query = crate::query::QueryService::new(
            ch.clone(),
            pg.clone(),
            bumps.clone(),
            gates.clone(),
            None,
            events.clone(),
        );
        Ok(Self {
            project,
            ch,
            pg,
            bumps,
            gates,
            query,
            events,
            received,
        })
    }

    async fn cleanup(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(60), async {
            self.ch.delete_live_project(&self.project).await?;
            self.pg
                .delete_live_projects(std::slice::from_ref(&self.project))
                .await
        })
        .await
        .context("ingest timing cleanup timed out")?
    }

    async fn init(&mut self, run: &str) -> Result<()> {
        let response = self
            .query
            .init_run(Request::new(proto::InitRunRequest {
                project_id: self.project.clone(),
                run_id: run.into(),
                run_name: run.into(),
                ..Default::default()
            }))
            .await?
            .into_inner();
        assert_eq!(
            response
                .run
                .context("InitRun omitted run")?
                .last_ingested_at_ms,
            None
        );
        let event = self.event()?;
        let versions = self.versions(run).await?;
        assert_eq!(
            event.projects,
            vec![(self.project.clone(), versions.project_version)]
        );
        assert_eq!(self.listed(run).await?.last_ingested_at_ms, None);
        Ok(())
    }

    async fn terminate(&mut self, run: &str, exit_code: i32) -> Result<proto::RunInfo> {
        self.query
            .terminate_run(Request::new(proto::TerminateRunRequest {
                project_id: self.project.clone(),
                run_id: run.into(),
                exit_code,
            }))
            .await?;
        let event = self.event()?;
        let versions = self.versions(run).await?;
        assert_eq!(
            event.projects,
            vec![(self.project.clone(), versions.project_version)]
        );
        self.listed(run).await
    }

    fn row(&self, run: &str, metric: &str, step: i64, timestamp_ms: i64) -> MetricRow {
        MetricRow {
            project_id: self.project.clone(),
            run_id: run.into(),
            metric_name: metric.into(),
            tag: String::new(),
            step,
            timestamp_ms,
            value: Some(step as f32),
            cdn_key: None,
            text_data: None,
        }
    }

    fn media_row(&self, run: &str, metric: &str, tag: &str, step: i64, cdn_key: &str) -> MetricRow {
        MetricRow {
            tag: tag.into(),
            value: None,
            cdn_key: Some(cdn_key.into()),
            ..self.row(run, metric, step, 1)
        }
    }

    fn text_row(&self, run: &str, metric: &str, step: i64, text: &str) -> MetricRow {
        MetricRow {
            value: None,
            text_data: Some(text.into()),
            ..self.row(run, metric, step, 1)
        }
    }

    async fn ingest(&self, rows: Vec<MetricRow>) -> Result<i64> {
        let run = rows[0].run_id.clone();
        self.ingest_many(rows)
            .await?
            .remove(&run)
            .context("missing pending receipt after successful insert")
    }

    async fn ingest_many(&self, rows: Vec<MetricRow>) -> Result<HashMap<String, i64>> {
        let runs: Vec<String> = rows
            .iter()
            .map(|row| row.run_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let before = chrono::Utc::now().timestamp_millis();
        flush_rows_inner(&self.ch, &self.pg, &self.gates, &self.bumps, rows.clone()).await?;
        let after = chrono::Utc::now().timestamp_millis();
        let accepted = self.bumps.pending_last_ingested_at_ms(&self.project, &runs);
        assert_eq!(
            accepted.len(),
            runs.len(),
            "not every inserted run has a pending receipt"
        );
        for (run, receipt) in &accepted {
            assert!(
                (before..=after).contains(receipt),
                "{run}: receipt {receipt} outside server-clock interval {before}..={after}"
            );
        }
        // Payload rows (CDN, text) are not numeric points.
        for row in rows.iter().filter(|row| row.value.is_some()) {
            let points = self
                .ch
                .query_raw_many(
                    &self.project,
                    std::slice::from_ref(&row.run_id),
                    &row.metric_name,
                    row.step,
                    row.step,
                )
                .await?
                .swap_remove(0);
            assert_eq!(
                points,
                [(row.step, f64::from(row.value.unwrap()))],
                "ACK did not expose the inserted point"
            );
        }
        Ok(accepted)
    }

    async fn listed(&self, run: &str) -> Result<proto::RunInfo> {
        self.query
            .list_runs(Request::new(proto::ListRunsRequest {
                project_id: self.project.clone(),
            }))
            .await?
            .into_inner()
            .runs
            .into_iter()
            .find(|row| row.run_id == run)
            .with_context(|| format!("ListRuns omitted {run}"))
    }

    async fn versions(&self, run: &str) -> Result<proto::PollVersionsResponse> {
        self.versions_for(&[run.to_owned()]).await
    }

    async fn versions_for(&self, runs: &[String]) -> Result<proto::PollVersionsResponse> {
        Ok(self
            .query
            .poll_versions(Request::new(proto::PollVersionsRequest {
                project_id: Some(self.project.clone()),
                run_ids: runs.to_vec(),
            }))
            .await?
            .into_inner())
    }

    fn no_event(&mut self) -> Result<()> {
        let received = self.received.try_recv();
        ensure!(
            matches!(received, Err(TryRecvError::Empty)),
            "unexpected extra version event: {received:?}"
        );
        Ok(())
    }

    fn event(&mut self) -> Result<VersionEvent> {
        let event = self
            .received
            .try_recv()
            .context("missing pushed version event")?;
        ensure!(!event.resync, "expected exact versions, not a resync");
        self.no_event()?;
        Ok(event)
    }

    async fn drain_run(&mut self, run: &str, accepted: i64, project_bump: bool) -> Result<()> {
        let before = self.versions(run).await?;
        self.drain(
            &HashMap::from([(run.to_owned(), accepted)]),
            &before,
            project_bump,
        )
        .await
    }

    async fn drain(
        &mut self,
        receipts: &HashMap<String, i64>,
        before: &proto::PollVersionsResponse,
        project_bump: bool,
    ) -> Result<()> {
        wait_past_receipt(*receipts.values().max().context("no receipts to drain")?).await?;
        self.no_event()?;
        self.bumps.write_dirty(&self.pg, &self.events).await;
        ensure!(
            !self.bumps.state.lock().unwrap().has_pending_work(),
            "coalescer failed to commit all work"
        );
        let event = self.event()?;
        let runs: Vec<_> = receipts.keys().cloned().collect();
        let after = self.versions_for(&runs).await?;
        let expected: HashMap<_, _> = runs
            .iter()
            .map(|run| (run.clone(), before.run_versions[run] + 1))
            .collect();
        assert_eq!(
            after.run_versions, expected,
            "one tick must bump every dirty run exactly once"
        );
        assert_eq!(
            event.runs.len(),
            runs.len(),
            "duplicate or missing pushed run versions"
        );
        assert_eq!(event.runs.into_iter().collect::<HashMap<_, _>>(), expected);
        assert_eq!(
            after.project_version,
            before.project_version + u64::from(project_bump)
        );
        let expected_projects = if project_bump {
            vec![(self.project.clone(), after.project_version)]
        } else {
            vec![]
        };
        assert_eq!(
            event.projects, expected_projects,
            "pushed project version differs from PollVersions"
        );
        ensure!(event.global.is_none());
        for (run, accepted) in receipts {
            assert_eq!(
                self.listed(run).await?.last_ingested_at_ms,
                Some(*accepted),
                "{run}: ListRuns receipt"
            );
            let stored = self
                .pg
                .get_run(&self.project, run)
                .await?
                .context("missing PostgreSQL run")?
                .0;
            assert_eq!(
                stored.last_ingested_at_ms,
                Some(*accepted),
                "{run}: stored receipt"
            );
        }
        self.bumps.write_dirty(&self.pg, &self.events).await;
        self.no_event()?;
        Ok(())
    }

    async fn database_now(&self) -> Result<i64> {
        Ok(self.pg.lifecycle_snapshot().await?.server_now_ms)
    }

    fn replay_receipt(&self, rows: &[MetricRow], receipt: i64) {
        let metadata = batch_metadata(rows);
        self.bumps.mark_dirty(
            metadata.heartbeats.into_iter(),
            metadata.candidates,
            receipt,
        );
    }
}

async fn wait_past_receipt(receipt: i64) -> Result<()> {
    // Separate receipt time from later snapshots, receipts, and writes.
    tokio::time::timeout(Duration::from_secs(5), async {
        while chrono::Utc::now().timestamp_millis() <= receipt {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .context("server receipt clock did not advance")?;
    Ok(())
}

/// Each case owns one unique project and cleans up after success.
async fn with_fixture(case: impl AsyncFnOnce(&mut Fixture, &str) -> Result<()>) -> Result<()> {
    let pg_url = crate::pg::live_test_url("KYMO_LIVE_TEST_DATABASE_URL")?;
    let ch_url = crate::pg::live_test_url("KYMO_LIVE_TEST_CLICKHOUSE_URL")?;
    let _suite_guard = crate::pg::live_database_suite_gate().lock().await;
    let mut fixture = Fixture::new(&pg_url, &ch_url).await?;
    let run = format!("{}-run", fixture.project);
    tokio::time::timeout(Duration::from_secs(180), case(&mut fixture, &run))
        .await
        .context("ingest timing integration test timed out")??;
    fixture.cleanup().await
}

async fn terminal_ingest(fixture: &mut Fixture, run: &str, exit_code: i32) -> Result<()> {
    fixture.init(run).await?;
    let first_rows = vec![fixture.row(run, "loss", 1, 1_000)];
    let first = fixture.ingest(first_rows.clone()).await?;
    ensure!(
        fixture.listed(run).await?.last_ingested_at_ms.is_none(),
        "fixture did not hold the coalescer window open"
    );
    fixture.drain_run(run, first, false).await?;

    // A distinct pending ACK proves TerminateRun snapshots unpersisted receipts.
    wait_past_receipt(first).await?;
    let final_ack = fixture
        .ingest(vec![fixture.row(run, "loss", 2, 750)])
        .await?;
    assert!(
        final_ack > first,
        "final ACK {final_ack} did not advance past {first}"
    );
    assert_eq!(fixture.listed(run).await?.last_ingested_at_ms, Some(first));
    let terminated = fixture.terminate(run, exit_code).await?;
    ensure!(terminated.terminated_at_ms.is_some());
    assert_eq!(
        terminated.last_ingested_at_ms,
        Some(final_ack),
        "TerminateRun lost its pending final ACK"
    );
    let status = if exit_code == 0 {
        proto::RunStatus::Finished
    } else {
        proto::RunStatus::Crashed
    };
    assert_eq!(terminated.status(), status);
    fixture.drain_run(run, final_ack, true).await?;

    wait_past_receipt(final_ack).await?;
    let second = fixture
        .ingest(vec![fixture.row(run, "loss", 3, 500)])
        .await?;
    let newest = fixture.ingest(vec![fixture.row(run, "loss", 4, 1)]).await?;
    assert!(
        newest >= second && second > final_ack,
        "receipts regressed: final ACK {final_ack}, second {second}, newest {newest}"
    );
    assert_eq!(
        fixture.listed(run).await?.last_ingested_at_ms,
        Some(final_ack)
    );

    // Replay before and after draining to isolate coalescer max from SQL GREATEST.
    fixture.replay_receipt(&first_rows, first);
    assert_eq!(
        fixture
            .bumps
            .pending_last_ingested_at_ms(&fixture.project, &[run.into()])[run],
        newest,
        "{run}: coalescer regressed after replaying an older receipt"
    );
    fixture.drain_run(run, newest, true).await?;
    fixture.replay_receipt(&first_rows, first);
    fixture.drain_run(run, newest, true).await?;

    // A separate system-only window must still refresh a terminal run.
    wait_past_receipt(newest).await?;
    let system_only = fixture
        .ingest(vec![fixture.row(run, "system/cpu", 1, 1)])
        .await?;
    assert!(system_only > newest);
    assert_eq!(fixture.listed(run).await?.last_ingested_at_ms, Some(newest));
    fixture.drain_run(run, system_only, true).await?;
    let listed = fixture.listed(run).await?;
    assert_eq!(listed.status(), status);
    assert_eq!(listed.terminated_at_ms, terminated.terminated_at_ms);
    Ok(())
}

async fn mixed_runs_share_one_tick(fixture: &mut Fixture, run: &str) -> Result<()> {
    let runs = vec![
        run.to_owned(),
        format!("{run}-crashed"),
        format!("{run}-live"),
    ];
    for run in &runs {
        fixture.init(run).await?;
    }
    // Two terminal runs require only one project refresh. The third run
    // remains live but must still receive its data-version bump.
    for (exit_code, run) in runs[..2].iter().enumerate() {
        fixture.terminate(run, exit_code as i32).await?;
    }
    let before = fixture.versions_for(&runs).await?;
    let now = fixture.database_now().await?;
    let receipts = fixture
        .ingest_many(vec![
            fixture.row(&runs[0], "loss", 1, 1),
            fixture.row(&runs[1], "loss", 1, 2),
            fixture.row(&runs[2], "loss", 1, now),
            fixture.row(&runs[2], "system/cpu", 1, now),
        ])
        .await?;
    assert_eq!(
        fixture
            .bumps
            .pending_last_ingested_at_ms(&fixture.project, &runs[2..]),
        HashMap::from([(runs[2].clone(), receipts[&runs[2]])]),
        "pending receipts were not filtered to the requested run"
    );
    assert!(fixture
        .bumps
        .pending_last_ingested_at_ms(&format!("{}-other", fixture.project), &runs)
        .is_empty());
    fixture.drain(&receipts, &before, true).await?;
    assert_eq!(
        fixture.listed(&runs[0]).await?.status(),
        proto::RunStatus::Finished
    );
    assert_eq!(
        fixture.listed(&runs[1]).await?.status(),
        proto::RunStatus::Crashed
    );
    assert!(matches!(
        fixture.listed(&runs[2]).await?.status(),
        proto::RunStatus::Running | proto::RunStatus::Unresponsive
    ));
    Ok(())
}

async fn heartbeat_ingest(fixture: &mut Fixture, run: &str, stale_system: bool) -> Result<()> {
    fixture.init(run).await?;
    for step in 1..=2 {
        let now = fixture.database_now().await?;
        let system = if stale_system {
            now - crate::liveness::PRESUMED_DEAD_WINDOW_MS - 60_000
        } else {
            now
        };
        let accepted = fixture
            .ingest(vec![
                fixture.row(run, "loss", step, now),
                fixture.row(run, "system/cpu", step, system),
            ])
            .await?;
        fixture.drain_run(run, accepted, false).await?;
        let stored = fixture
            .pg
            .get_run(&fixture.project, run)
            .await?
            .context("missing heartbeat run")?
            .0;
        assert_eq!(
            stored.last_main_metric_at_ms,
            Some(now),
            "{run}: stored main heartbeat"
        );
        assert_eq!(
            stored.last_system_metric_at_ms,
            Some(system),
            "{run}: stored system heartbeat"
        );
        let status = fixture.listed(run).await?.status();
        if stale_system {
            assert_eq!(
                status,
                proto::RunStatus::PresumedDead,
                "{run}: stale system with fresh main"
            );
        } else {
            // Successful database I/O can outlast the 10-second Running
            // window. The bounded case stays below PresumedDead's window;
            // the assertions above prove both fresh heartbeats were stored.
            assert!(
                matches!(
                    status,
                    proto::RunStatus::Running | proto::RunStatus::Unresponsive
                ),
                "{run}: fresh heartbeat status was {status:?}"
            );
        }
    }
    Ok(())
}

async fn trash_before_drain(fixture: &mut Fixture, run: &str) -> Result<()> {
    fixture.init(run).await?;
    let accepted = fixture.ingest(vec![fixture.row(run, "loss", 1, 1)]).await?;
    ensure!(fixture.listed(run).await?.last_ingested_at_ms.is_none());
    let response = fixture
        .query
        .trash_runs(Request::new(proto::TrashRunsRequest {
            project_id: fixture.project.clone(),
            run_ids: vec![run.into()],
        }))
        .await?
        .into_inner();
    assert_eq!(response.results.len(), 1);
    assert_eq!(
        response.results[0].outcome(),
        proto::TrashRunOutcome::Trashed
    );
    fixture.event()?;
    let before = fixture.versions(run).await?;
    assert_trashed(fixture, run, accepted).await?;
    fixture
        .bumps
        .write_dirty(&fixture.pg, &fixture.events)
        .await;
    ensure!(!fixture.bumps.state.lock().unwrap().has_pending_work());
    // Pending registration may announce metric discovery, but the
    // skipped post-Trash bump must not refresh data or run lists.
    loop {
        match fixture.received.try_recv() {
            Ok(event) => ensure!(
                event.runs.is_empty()
                    && event.projects.is_empty()
                    && event.global.is_none()
                    && !event.resync,
                "{run}: unexpected post-Trash refresh: {event:?}"
            ),
            Err(TryRecvError::Empty) => break,
            Err(error) => return Err(error.into()),
        }
    }
    assert_trashed(fixture, run, accepted).await?;
    let after = fixture.versions(run).await?;
    assert_eq!(
        after.project_version, before.project_version,
        "{run}: post-Trash project version"
    );
    assert_eq!(
        after.run_versions, before.run_versions,
        "{run}: post-Trash run versions"
    );
    let restored = fixture
        .query
        .restore_run(Request::new(proto::RestoreRunRequest {
            project_id: fixture.project.clone(),
            run_id: run.into(),
        }))
        .await?
        .into_inner();
    assert_eq!(restored.outcome(), proto::RestoreRunOutcome::Restored);
    fixture.event()?;
    assert_eq!(
        fixture.listed(run).await?.last_ingested_at_ms,
        Some(accepted)
    );
    Ok(())
}

async fn assert_trashed(fixture: &Fixture, run: &str, accepted: i64) -> Result<()> {
    let listed = fixture
        .query
        .list_runs(Request::new(proto::ListRunsRequest {
            project_id: fixture.project.clone(),
        }))
        .await?
        .into_inner();
    assert!(
        listed.runs.iter().all(|row| row.run_id != run),
        "ListRuns exposed a trashed run"
    );
    let record = fixture
        .query
        .get_run(Request::new(proto::GetRunRequest {
            project_id: fixture.project.clone(),
            run_id: run.into(),
        }))
        .await?
        .into_inner()
        .run
        .context("GetRun omitted Trash record")?;
    assert_eq!(record.state(), proto::RunLifecycleState::Trashed);
    assert_eq!(
        record
            .run
            .context("Trash record omitted run")?
            .last_ingested_at_ms,
        Some(accepted),
        "{run}: Trash lost the pending accepted timestamp"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL and KYMO_LIVE_TEST_CLICKHOUSE_URL"]
async fn finished_run_delayed_ingest_timing() -> Result<()> {
    with_fixture(async |fixture, run| terminal_ingest(fixture, run, 0).await).await
}

#[tokio::test]
#[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL and KYMO_LIVE_TEST_CLICKHOUSE_URL"]
async fn crashed_run_delayed_ingest_timing() -> Result<()> {
    with_fixture(async |fixture, run| terminal_ingest(fixture, run, 1).await).await
}

#[tokio::test]
#[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL and KYMO_LIVE_TEST_CLICKHOUSE_URL"]
async fn mixed_runs_coalesce_into_one_project_refresh() -> Result<()> {
    with_fixture(async |fixture, run| mixed_runs_share_one_tick(fixture, run).await).await
}

#[tokio::test]
#[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL and KYMO_LIVE_TEST_CLICKHOUSE_URL"]
async fn fresh_heartbeat_does_not_refresh_project() -> Result<()> {
    with_fixture(async |fixture, run| heartbeat_ingest(fixture, run, false).await).await
}

#[tokio::test]
#[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL and KYMO_LIVE_TEST_CLICKHOUSE_URL"]
async fn stale_system_with_fresh_main_does_not_refresh_project() -> Result<()> {
    with_fixture(async |fixture, run| heartbeat_ingest(fixture, run, true).await).await
}

#[tokio::test]
#[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL and KYMO_LIVE_TEST_CLICKHOUSE_URL"]
async fn trash_retains_pending_ingest_timing() -> Result<()> {
    with_fixture(async |fixture, run| trash_before_drain(fixture, run).await).await
}

/// Numeric reads return only numeric rows and the CDN lookup only media rows, for a metric name that logged all three payload types; a SELECT alias must not shadow the column a filter tests.
async fn mixed_payload_reads(fixture: &mut Fixture, run: &str) -> Result<()> {
    fixture.init(run).await?;
    let project = fixture.project.clone();
    let metric = "mixed";
    let cached = async || -> Result<Vec<(i64, f32)>> {
        let rows = fixture
            .ch
            .query_raw_any_cached(&project, run, metric, || None)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(rows.iter().map(|row| (row.step, row.value)).collect())
    };
    fixture
        .ingest(vec![
            fixture.row(run, metric, 1, 1),
            fixture.media_row(run, metric, "", 2, "two.png"),
            fixture.text_row(run, metric, 3, "three\n"),
            fixture.media_row(run, metric, "b", 4, "four-b.png"),
            fixture.media_row(run, metric, "a", 4, "four-a.png"),
        ])
        .await?;

    let scalar = fixture
        .ch
        .query_raw_many(&project, &[run.to_string()], metric, i64::MIN, i64::MAX)
        .await?;
    assert_eq!(scalar[0], [(1, 1.0)]);
    assert_eq!(cached().await?, [(1, 1.0)]);
    let keys = fixture
        .ch
        .query_cdn_keys_batch(
            &[(project.clone(), run.to_string(), metric.to_string())],
            i64::MIN,
            i64::MAX,
        )
        .await?;
    assert_eq!(
        keys.iter()
            .map(|row| (row.step, row.cdn_key.as_str()))
            .collect::<Vec<_>>(),
        [(2, "two.png"), (4, "four-a.png"), (4, "four-b.png")]
    );
    // Gallery-key and text-window replies echo the run's data version, as chart replies do.
    let polled = fixture.versions(run).await?.run_versions;
    let cdn = fixture
        .query
        .query_cdn_keys(Request::new(proto::QueryCdnKeysRequest {
            refs: vec![proto::SeriesRef {
                project_id: project.clone(),
                run_id: run.to_string(),
                metric_name: metric.to_string(),
                tags: vec![],
            }],
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert_eq!(cdn.run_versions, polled);
    let text = fixture
        .query
        .query_text_window(Request::new(proto::QueryTextWindowRequest {
            project_id: project.clone(),
            run_id: run.to_string(),
            metric_names: vec![metric.to_string()],
            line_limit: 10,
            ..Default::default()
        }))
        .await?
        .into_inner();
    assert_eq!(text.run_versions, polled);

    // Incremental refreshes: a non-numeric row at a new step is no point, and one replacing a cached step deletes it.
    fixture
        .ingest(vec![fixture.media_row(run, metric, "", 5, "five.png")])
        .await?;
    assert_eq!(cached().await?, [(1, 1.0)]);
    fixture
        .ingest(vec![
            fixture.text_row(run, metric, 1, "replaced\n"),
            fixture.row(run, metric, 6, 1),
        ])
        .await?;
    assert_eq!(cached().await?, [(6, 6.0)]);
    Ok(())
}

/// A cold chart reads all of its runs' series in one ClickHouse statement, each run's rows reaching its own series.
async fn cold_chart_batches_its_reads(fixture: &mut Fixture, run: &str) -> Result<()> {
    // A quote exercises the bound run-id array.
    let runs = [
        run.to_string(),
        format!("{run}-o'brien"),
        format!("{run}-3"),
    ];
    for (index, run) in runs.iter().enumerate() {
        fixture.init(run).await?;
        let steps = index as i64 + 1;
        let rows = (1..=steps)
            .flat_map(|step| {
                [
                    fixture.row(run, "loss", step, 1),
                    fixture.row(run, "acc", step, 1),
                ]
            })
            .collect();
        fixture.ingest(rows).await?;
    }
    // Each read of the counter counts itself, so the chart's statements are the observed difference minus one.
    // Exact because the suite gate serializes the live tests and their throwaway ClickHouse has no other client.
    let selects = async || -> Result<u64> {
        Ok(fixture
            .ch
            .test_client()
            .query("SELECT value FROM system.events WHERE event = 'SelectQuery'")
            .fetch_one::<u64>()
            .await?)
    };
    // Charts (run, metric, points) refs, checks the series come back in request order with those points, and returns the chart's statement count.
    let chart = async |refs: &[(&String, &str, u32)]| -> Result<u64> {
        let before = selects().await?;
        let response = fixture
            .query
            .query_chart(Request::new(proto::ChartRequest {
                y_series: refs
                    .iter()
                    .map(|(run, metric, _)| proto::SeriesRef {
                        project_id: fixture.project.clone(),
                        run_id: (*run).clone(),
                        metric_name: (*metric).into(),
                        tags: vec![],
                    })
                    .collect(),
                ..Default::default()
            }))
            .await?
            .into_inner();
        assert_eq!(
            response
                .series
                .iter()
                .map(|series| (series.run_id.as_str(), series.seg_lens.iter().sum::<u32>()))
                .collect::<Vec<_>>(),
            refs.iter()
                .map(|(run, _, points)| (run.as_str(), *points))
                .collect::<Vec<_>>()
        );
        // The version echo holds one entry per distinct requested run, equal to what PollVersions reports (nothing bumps in between: this fixture applies bumps only when drained).
        let runs: Vec<String> = refs.iter().map(|(run, _, _)| (*run).clone()).collect();
        assert_eq!(
            response.run_versions,
            fixture.versions_for(&runs).await?.run_versions
        );
        Ok(selects().await? - before - 1)
    };
    let cold = [
        (&runs[0], "loss", 1),
        (&runs[1], "loss", 2),
        (&runs[2], "loss", 3),
    ];
    assert_eq!(
        chart(&cold).await?,
        1,
        "one batched full read for the cold chart"
    );

    // Two groups and a duplicate ref, with every series cached but run 0's acc: only that one reads.
    fixture
        .ch
        .query_raw_any_cached(&fixture.project, &runs[1], "acc", || None)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mixed = [
        (&runs[0], "acc", 1),
        (&runs[1], "loss", 2),
        (&runs[0], "acc", 1),
        (&runs[1], "acc", 2),
        (&runs[2], "loss", 3),
    ];
    assert_eq!(chart(&mixed).await?, 1, "one read, for run 0's acc");
    Ok(())
}

#[tokio::test]
#[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL and KYMO_LIVE_TEST_CLICKHOUSE_URL"]
async fn mixed_payload_metric_reads() -> Result<()> {
    with_fixture(async |fixture, run| mixed_payload_reads(fixture, run).await).await
}

#[tokio::test]
#[ignore = "requires KYMO_LIVE_TEST_DATABASE_URL and KYMO_LIVE_TEST_CLICKHOUSE_URL"]
async fn cold_chart_reads_its_runs_in_one_statement() -> Result<()> {
    with_fixture(async |fixture, run| cold_chart_batches_its_reads(fixture, run).await).await
}
