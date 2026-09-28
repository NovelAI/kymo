//! Garbage collector for the hosted GCS CDN (docs/cdn-gcs-migration.md § Garbage collection).
//!
//! A pass lists the bucket into ClickHouse, collects every key a viewer can reach (metric-row roots plus their manifests' children), and derives candidates: unreferenced objects neither created nor dedup-acked within the grace. Report mode publishes the counts; delete mode also deletes the candidates, claiming each batch through the upload fence first.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::body::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt as _, RetryConfig};
use serde::de::IgnoredAny;
use tokio::sync::{Notify, Semaphore};

use crate::cdn::{hosted_key_pattern, validate_hosted_key};
use crate::cdn_store::{count_gcs_error, credentialed_builder, CdnStore, Identity, PutOutcome};
use crate::clickhouse::{CdnGcKeySize, CdnGcReport, CdnInventoryRow, CdnManifestRow, ChClient};
use crate::deletion::unix_time_seconds;

#[path = "../../shared/cdn_manifest.rs"]
mod cdn_manifest;

/// How long an object stays safe after its creation or last dedup ack, referenced or not: the cover for references that arrive after the upload (spooled key-only records).
const GRACE: Duration = Duration::from_secs(30 * 24 * 3600);
const FIRST_PASS_DELAY: Duration = Duration::from_secs(10 * 60);
const PASS_INTERVAL: Duration = Duration::from_secs(6 * 3600);
/// Deletion starts no batch after this; the rest waits for the next pass.
const DELETE_BUDGET: Duration = Duration::from_secs(5 * 3600);
/// Rows (inventory keys, or cached children) per scratch or cache INSERT.
const INSERT_ROWS: usize = 100_000;
/// Unparsed roots per fetch page.
const PAGE: u64 = 10_000;
const FETCH_CONCURRENCY: usize = 16;
/// Manifest bytes in flight across concurrent fetches; a larger root fetches alone.
const FETCH_BUDGET: u32 = 64 * 1024 * 1024;
const DELETE_BATCH: u64 = 1_000;
const DELETE_CONCURRENCY: usize = 16;
/// How long a claim outlives a delete that didn't finish definitively: a DELETE that failed client-side can still land at GCS.
const DELETE_SETTLE: Duration = Duration::from_secs(2 * 60);
/// The ack recheck runs under a claim, and uploads of the claimed keys wait for it.
const RECHECK_TIMEOUT: Duration = Duration::from_secs(60);
/// Delete mode's ceiling is the larger of 1% of the referenced objects and this (docs § Garbage collection).
const CEILING_FLOOR: u64 = 10_000;
/// Dangling keys a pass checks with a HEAD.
const DANGLING_SAMPLE: u64 = 100;
const STAGES: [&str; 6] = [
    "inventory",
    "references",
    "manifests",
    "candidates",
    "report",
    "delete",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Off,
    Report,
    Delete,
}

impl Mode {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "report" => Ok(Mode::Report),
            "off" => Ok(Mode::Off),
            "delete" => Ok(Mode::Delete),
            other => {
                anyhow::bail!(
                    "KYMO_CDN_GC={other:?} is not a collector mode (off | report | delete)"
                )
            }
        }
    }
}

/// `KYMO_CDN_GC` (absent = report). Delete mode impersonates a separate delete-capable identity; its credential file is read here, before any database work.
pub struct Config {
    mode: Mode,
    deletes: Option<Arc<dyn ObjectStore>>,
    max_candidates: Option<u64>,
}

impl Config {
    pub fn from_env(bucket: &str) -> Result<Self> {
        let mode = Mode::parse(&crate::env::required_string_or("KYMO_CDN_GC", "report")?)?;
        let (deletes, max_candidates) = match mode {
            Mode::Delete => {
                let credentials = crate::env::required_optional_string("KYMO_CDN_GC_CREDENTIALS")?
                    .context("KYMO_CDN_GC=delete requires KYMO_CDN_GC_CREDENTIALS (an external_account file that impersonates the delete identity)")?;
                let store = credentialed_builder(&credentials, Identity::Collector)?
                    .with_bucket_name(bucket)
                    // A success or a 404 is then the only attempt there was, so a claim can settle on it.
                    .with_retry(RetryConfig {
                        max_retries: 0,
                        ..Default::default()
                    })
                    .build()?;
                let max_candidates =
                    crate::env::required_optional_string("KYMO_CDN_GC_MAX_CANDIDATES")?
                        .map(|raw| {
                            raw.trim().parse::<u64>().with_context(|| {
                                format!("KYMO_CDN_GC_MAX_CANDIDATES={raw:?} is not a count")
                            })
                        })
                        .transpose()?;
                (
                    Some(Arc::new(store) as Arc<dyn ObjectStore>),
                    max_candidates,
                )
            }
            Mode::Off | Mode::Report => (None, None),
        };
        Ok(Self {
            mode,
            deletes,
            max_candidates,
        })
    }
}

/// The in-process fence between uploads and deletion; the server is the bucket's only writer (a singleton), so this is the whole writer set. An upload holds its key from before the store write until its dedup ack is logged. Deletion claims only keys with no upload in flight, and an upload of a claimed key waits for the release, then creates the object afresh.
#[derive(Default)]
struct Fence {
    state: Mutex<FenceState>,
    released: Notify,
}

#[derive(Default)]
struct FenceState {
    uploading: HashMap<String, usize>,
    deleting: HashSet<String>,
}

impl Fence {
    async fn begin_upload(&self, key: &str) -> UploadHold<'_> {
        loop {
            // Created before the check, so a release between the check and the await still wakes it.
            let released = self.released.notified();
            {
                let mut state = self.state.lock().unwrap();
                if !state.deleting.contains(key) {
                    *state.uploading.entry(key.to_owned()).or_default() += 1;
                    return UploadHold {
                        fence: self,
                        key: key.to_owned(),
                    };
                }
            }
            released.await;
        }
    }

    /// Claims every key with no upload in flight and no earlier claim still settling; the rest stay alive this pass.
    fn claim(self: &Arc<Self>, keys: Vec<CdnGcKeySize>) -> Claim {
        let mut state = self.state.lock().unwrap();
        let keys: Vec<_> = keys
            .into_iter()
            .filter(|c| !state.uploading.contains_key(&c.key) && !state.deleting.contains(&c.key))
            .collect();
        state.deleting.extend(keys.iter().map(|c| c.key.clone()));
        Claim {
            fence: self.clone(),
            keys,
            in_doubt: false,
        }
    }

    fn release(&self, keys: &[CdnGcKeySize]) {
        let mut state = self.state.lock().unwrap();
        for candidate in keys {
            state.deleting.remove(&candidate.key);
        }
        drop(state);
        self.released.notify_waiters();
    }
}

struct UploadHold<'a> {
    fence: &'a Fence,
    key: String,
}

impl Drop for UploadHold<'_> {
    fn drop(&mut self) {
        let mut state = self.fence.state.lock().unwrap();
        if let Some(count) = state.uploading.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                state.uploading.remove(&self.key);
            }
        }
    }
}

/// Released on drop: at once, unless a DELETE sent for the batch may still land (`in_doubt`: an error or a panic after dispatch), then after `DELETE_SETTLE`, since an upload acked meanwhile would lose its object.
struct Claim {
    fence: Arc<Fence>,
    keys: Vec<CdnGcKeySize>,
    in_doubt: bool,
}

impl Drop for Claim {
    fn drop(&mut self) {
        let (fence, keys) = (self.fence.clone(), std::mem::take(&mut self.keys));
        if !self.in_doubt {
            fence.release(&keys);
        } else if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            // Without a runtime the process is ending, and no upload can wait on the keys.
            runtime.spawn(async move {
                tokio::time::sleep(DELETE_SETTLE).await;
                fence.release(&keys);
            });
        }
    }
}

/// The upload route's side of the collector (hosted gcs mode): every upload goes through the fence, and every dedup ack is logged before it is returned.
pub struct Uploads {
    fence: Arc<Fence>,
    ch: Arc<ChClient>,
}

impl Uploads {
    pub async fn put(&self, store: &CdnStore, key: &str, body: Bytes) -> Result<PutOutcome> {
        let _hold = self.fence.begin_upload(key).await;
        let outcome = store.put_if_absent(key, body).await?;
        if outcome == PutOutcome::Existing {
            self.ch
                .record_cdn_ack(key, unix_seconds(SystemTime::now()))
                .await?;
        }
        Ok(outcome)
    }
}

pub struct Collector {
    mode: Mode,
    ch: Arc<ChClient>,
    fence: Arc<Fence>,
    reads: Arc<dyn ObjectStore>,
    deletes: Option<Arc<dyn ObjectStore>>,
    max_candidates: Option<u64>,
}

/// Builds both halves. The ack log's schema must exist before the upload route serves.
pub async fn start(
    config: Config,
    reads: Arc<dyn ObjectStore>,
    ch: Arc<ChClient>,
) -> Result<(Uploads, Collector)> {
    ch.ensure_cdn_gc_schema(
        GRACE.as_secs() / (24 * 3600),
        unix_seconds(SystemTime::now()),
    )
    .await?;
    let fence = Arc::new(Fence::default());
    Ok((
        Uploads {
            fence: fence.clone(),
            ch: ch.clone(),
        },
        Collector {
            mode: config.mode,
            ch,
            fence,
            reads,
            deletes: config.deletes,
            max_candidates: config.max_candidates,
        },
    ))
}

#[derive(Debug, Default)]
struct Inventory {
    objects: u64,
    bytes: u64,
    foreign_objects: u64,
    foreign_bytes: u64,
}

#[derive(Debug)]
struct PassSummary {
    inventory: Inventory,
    report: CdnGcReport,
    unparsed: u64,
    deleted: Deleted,
}

#[derive(Debug, Default)]
struct Deleted {
    objects: u64,
    bytes: u64,
    /// Already gone when deleted.
    missing: u64,
    /// Candidates the fence or the ack recheck kept alive.
    spared: u64,
}

impl Collector {
    pub fn spawn(self) {
        metrics::gauge!("mkdb2_cdn_gc_mode").set(match self.mode {
            Mode::Off => 0.0,
            Mode::Report => 1.0,
            Mode::Delete => 2.0,
        });
        metrics::gauge!("mkdb2_cdn_gc_last_success_unixtime_seconds").set(0.0);
        metrics::gauge!("mkdb2_cdn_gc_last_failure_unixtime_seconds").set(0.0);
        for stage in STAGES {
            metrics::counter!("mkdb2_cdn_gc_failures_total", "stage" => stage).absolute(0);
        }
        metrics::counter!("mkdb2_cdn_gc_deleted_objects_total").absolute(0);
        metrics::counter!("mkdb2_cdn_gc_deleted_bytes_total").absolute(0);
        metrics::gauge!("mkdb2_cdn_gc_delete_armed").set(0.0);
        if self.mode == Mode::Off {
            tracing::warn!(
                "CDN collector is off (KYMO_CDN_GC=off): no bucket inventory, no collection"
            );
            return;
        }
        tracing::info!(mode = ?self.mode, "CDN collector configured");
        let collector = Arc::new(self);
        tokio::spawn(async move {
            tokio::time::sleep(FIRST_PASS_DELAY).await;
            loop {
                let pass = collector.clone();
                let started = Instant::now();
                // A panic ends the pass, not the loop.
                let outcome = tokio::spawn(async move { pass.pass(SystemTime::now()).await })
                    .await
                    .unwrap_or_else(|panic| Err(panic.into()));
                match outcome {
                    Ok(PassSummary {
                        inventory,
                        report,
                        unparsed,
                        deleted,
                    }) => {
                        metrics::gauge!("mkdb2_cdn_gc_last_success_unixtime_seconds")
                            .set(unix_time_seconds());
                        tracing::info!(
                            mode = ?collector.mode,
                            objects = inventory.objects,
                            bytes = inventory.bytes,
                            foreign_objects = inventory.foreign_objects,
                            referenced_objects = report.referenced_objects,
                            candidate_objects = report.candidate_objects,
                            candidate_bytes = report.candidate_bytes,
                            dangling_references = report.dangling_references(),
                            unparsed_roots = unparsed,
                            deleted_objects = deleted.objects,
                            deleted_bytes = deleted.bytes,
                            already_missing = deleted.missing,
                            spared = deleted.spared,
                            seconds = started.elapsed().as_secs(),
                            "CDN collector pass complete"
                        );
                    }
                    Err(error) => {
                        metrics::gauge!("mkdb2_cdn_gc_last_failure_unixtime_seconds")
                            .set(unix_time_seconds());
                        tracing::error!(
                            error = format!("{error:#}"),
                            seconds = started.elapsed().as_secs(),
                            "CDN collector pass failed"
                        );
                    }
                }
                tokio::time::sleep(PASS_INTERVAL).await;
            }
        });
    }

    async fn pass(&self, now: SystemTime) -> Result<PassSummary> {
        let cutoff = unix_seconds(now).saturating_sub(GRACE.as_secs() as u32);
        metrics::gauge!("mkdb2_cdn_gc_delete_armed").set(0.0);
        let inventory = stage("inventory", self.inventory()).await;
        // A failed listing leaves a gap in the size panels, never a previous listing's count.
        let (objects, bytes) = inventory
            .as_ref()
            .map_or((f64::NAN, f64::NAN), |inventory| {
                (inventory.objects as f64, inventory.bytes as f64)
            });
        metrics::gauge!("mkdb2_cdn_gcs_objects").set(objects);
        metrics::gauge!("mkdb2_cdn_gcs_bytes").set(bytes);
        let inventory = inventory.inspect_err(|_| publish_classes(None))?;
        let classified = self.classify(cutoff).await;
        publish_classes(
            classified
                .as_ref()
                .ok()
                .map(|(r, u, m)| (&inventory, r, *u, *m)),
        );
        let (report, unparsed, _) = classified?;
        let deleted = match &self.deletes {
            Some(deletes) => {
                stage(
                    "delete",
                    self.delete_if_safe(deletes, cutoff, unparsed, &report),
                )
                .await?
            }
            None => Deleted::default(),
        };
        Ok(PassSummary {
            inventory,
            report,
            unparsed,
            deleted,
        })
    }

    /// Referenced keys, then candidates, then the report; returns it with the unparsed-root and confirmed-missing counts.
    async fn classify(&self, cutoff: u32) -> Result<(CdnGcReport, u64, u64)> {
        stage(
            "references",
            self.ch.cdn_gc_collect_roots(&hosted_key_pattern()),
        )
        .await?;
        let unparsed = stage("manifests", self.parse_manifests()).await?;
        stage("candidates", self.ch.cdn_gc_collect_candidates(cutoff)).await?;
        let (report, missing) = stage("report", async {
            let report = self.ch.cdn_gc_report().await?;
            let missing = match report.dangling_references() {
                0 => Vec::new(),
                _ => self.missing_references().await?,
            };
            if !missing.is_empty() {
                tracing::warn!(
                    dangling = report.dangling_references(),
                    ?missing,
                    "CDN collector found referenced keys missing from the bucket"
                );
            }
            Ok((report, missing.len() as u64))
        })
        .await?;
        Ok((report, unparsed, missing))
    }

    /// The sampled dangling keys still missing now: an upload that landed after the listing exists by the time of its HEAD. Its failures stay uncounted, like the manifest fetches.
    async fn missing_references(&self) -> Result<Vec<String>> {
        let mut missing = Vec::new();
        for key in self.ch.cdn_gc_dangling(DANGLING_SAMPLE).await? {
            match self.reads.head(&ObjectPath::from(key.as_str())).await {
                Err(object_store::Error::NotFound { .. }) => missing.push(key),
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(key = %key, error = format!("{error:#}"), "CDN collector could not check a dangling key")
                }
            }
        }
        Ok(missing)
    }

    /// Lists the bucket into the scratch table; objects outside the key grammar are counted but never collected.
    async fn inventory(&self) -> Result<Inventory> {
        self.ch.cdn_gc_reset().await?;
        let mut inventory = Inventory::default();
        let mut batch = Vec::with_capacity(INSERT_ROWS);
        let mut listing = self.reads.list(None);
        while let Some(meta) = listing
            .try_next()
            .await
            .inspect_err(|_| count_gcs_error("read"))?
        {
            inventory.objects += 1;
            inventory.bytes += meta.size;
            let key = meta.location.as_ref();
            if !validate_hosted_key(key) {
                inventory.foreign_objects += 1;
                inventory.foreign_bytes += meta.size;
                continue;
            }
            batch.push(CdnInventoryRow {
                kind: "inventory",
                key: key.to_owned(),
                size: meta.size,
                // Last-modified (GCS `updated`): objects are never rewritten, and a later metadata change only delays collection.
                created: unix_seconds(meta.last_modified.into()),
            });
            if batch.len() == INSERT_ROWS {
                self.ch.cdn_gc_insert_inventory(&batch).await?;
                batch.clear();
            }
        }
        self.ch.cdn_gc_insert_inventory(&batch).await?;
        Ok(inventory)
    }

    /// Parses each stored root once, caches its children, and adds every root's children to the referenced set. Returns how many roots stay unparsed (fetch failures); any unparsed root disarms deletion.
    async fn parse_manifests(&self) -> Result<u64> {
        self.ch
            .cdn_gc_collect_unparsed(cdn_manifest::LINKS_VERSION)
            .await?;
        let roots = self.ch.cdn_gc_count("unparsed").await?;
        let reads = self.reads.as_ref();
        let budget = &Semaphore::new(FETCH_BUDGET as usize);
        let (mut done, mut unparsed) = (0u64, 0u64);
        let mut after = String::new();
        loop {
            let page = self.ch.cdn_gc_page("unparsed", &after, PAGE).await?;
            let Some(last) = page.last() else {
                break;
            };
            after = last.key.clone();
            done += page.len() as u64;
            let mut fetched = futures::stream::iter(page)
                .map(|root| async move {
                    let children = fetch_children(reads, budget, &root).await;
                    (root.key, children)
                })
                .buffer_unordered(FETCH_CONCURRENCY);
            // One row per root holds its whole parse, so a partly committed INSERT never caches part of a root's children.
            let (mut rows, mut children) = (Vec::new(), 0);
            while let Some((parent, result)) = fetched.next().await {
                match result {
                    Ok(Some(keys)) => {
                        children += keys.len();
                        rows.push(CdnManifestRow {
                            parent,
                            links_version: cdn_manifest::LINKS_VERSION,
                            children: keys,
                        });
                    }
                    // Gone since the listing: nothing for a viewer to reach.
                    Ok(None) => {}
                    Err(error) => {
                        unparsed += 1;
                        tracing::warn!(key = %parent, error = format!("{error:#}"), "CDN collector could not parse a root");
                    }
                }
                if children >= INSERT_ROWS {
                    self.ch.cdn_gc_insert_manifests(&rows).await?;
                    (rows, children) = (Vec::new(), 0);
                }
            }
            self.ch.cdn_gc_insert_manifests(&rows).await?;
            // A full parse (an empty children cache, or a links-version bump) takes hours.
            tracing::info!(
                done,
                roots,
                unparsed_roots = unparsed,
                "CDN collector manifest parse progress"
            );
        }
        self.ch
            .cdn_gc_collect_children(cdn_manifest::LINKS_VERSION)
            .await?;
        Ok(unparsed)
    }

    async fn delete_if_safe(
        &self,
        deletes: &dyn ObjectStore,
        cutoff: u32,
        unparsed: u64,
        report: &CdnGcReport,
    ) -> Result<Deleted> {
        let ceiling = self
            .max_candidates
            .unwrap_or((report.referenced_objects / 100).max(CEILING_FLOOR));
        metrics::gauge!("mkdb2_cdn_gc_delete_ceiling").set(ceiling as f64);
        // A dedup ack from before the log began is invisible, so the log must span the grace.
        let log_start = self.ch.cdn_ack_log_start().await?;
        let armed = unparsed == 0
            && log_start.is_some_and(|start| start <= cutoff)
            && report.candidate_objects <= ceiling;
        metrics::gauge!("mkdb2_cdn_gc_delete_armed").set(if armed { 1.0 } else { 0.0 });
        if !armed {
            tracing::warn!(
                unparsed_roots = unparsed,
                ack_log_start = ?log_start,
                candidates = report.candidate_objects,
                ceiling,
                "CDN collector delete mode is disarmed this pass"
            );
            return Ok(Deleted::default());
        }
        self.delete_candidates(deletes, cutoff).await
    }

    async fn delete_candidates(&self, deletes: &dyn ObjectStore, cutoff: u32) -> Result<Deleted> {
        let deadline = Instant::now() + DELETE_BUDGET;
        let mut deleted = Deleted::default();
        let mut after = String::new();
        while Instant::now() < deadline {
            let batch = self
                .ch
                .cdn_gc_page("candidate", &after, DELETE_BATCH)
                .await?;
            let Some(last) = batch.last() else {
                break;
            };
            after = last.key.clone();
            let offered = batch.len() as u64;
            let mut claim = self.fence.claim(batch);
            // Acks logged between the candidate query and the claim.
            let keys: Vec<_> = claim.keys.iter().map(|c| c.key.clone()).collect();
            let acked =
                tokio::time::timeout(RECHECK_TIMEOUT, self.ch.cdn_gc_acked_since(&keys, cutoff))
                    .await
                    .context("rechecking CDN dedup acks timed out")??;
            let doomed: Vec<_> = claim
                .keys
                .iter()
                .filter(|candidate| !acked.contains(&candidate.key))
                .map(|candidate| (ObjectPath::from(candidate.key.as_str()), candidate.size))
                .collect();
            deleted.spared += offered - doomed.len() as u64;
            claim.in_doubt = true;
            let outcomes: Vec<_> = futures::stream::iter(doomed)
                .map(|(path, size)| async move { (size, deletes.delete(&path).await) })
                .buffer_unordered(DELETE_CONCURRENCY)
                .collect()
                .await;
            let mut first_error = None;
            for (size, outcome) in outcomes {
                match outcome {
                    Ok(()) => {
                        deleted.objects += 1;
                        deleted.bytes += size;
                        metrics::counter!("mkdb2_cdn_gc_deleted_objects_total").increment(1);
                        metrics::counter!("mkdb2_cdn_gc_deleted_bytes_total").increment(size);
                    }
                    Err(object_store::Error::NotFound { .. }) => deleted.missing += 1,
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
            if let Some(error) = first_error {
                return Err(error).context("deleting CDN objects");
            }
            claim.in_doubt = false;
        }
        Ok(deleted)
    }
}

/// Classes partition the bucket: foreign (outside the key grammar, never collected), referenced, candidate, and grace (unreferenced but recently created or acked). `None` (a failed pass) blanks them rather than leaving a previous pass's split beside fresh size gauges.
fn publish_classes(pass: Option<(&Inventory, &CdnGcReport, u64, u64)>) {
    let value = |n: u64| pass.map_or(f64::NAN, |_| n as f64);
    let blank = (Inventory::default(), CdnGcReport::default());
    let (inventory, report, unparsed, missing) = pass.unwrap_or((&blank.0, &blank.1, 0, 0));
    let valid_objects = inventory.objects - inventory.foreign_objects;
    let valid_bytes = inventory.bytes - inventory.foreign_bytes;
    for (class, objects, bytes) in [
        (
            "foreign",
            inventory.foreign_objects,
            inventory.foreign_bytes,
        ),
        (
            "referenced",
            report.referenced_objects,
            report.referenced_bytes,
        ),
        (
            "candidate",
            report.candidate_objects,
            report.candidate_bytes,
        ),
        (
            "grace",
            valid_objects.saturating_sub(report.referenced_objects + report.candidate_objects),
            valid_bytes.saturating_sub(report.referenced_bytes + report.candidate_bytes),
        ),
    ] {
        metrics::gauge!("mkdb2_cdn_gc_objects", "class" => class).set(value(objects));
        metrics::gauge!("mkdb2_cdn_gc_bytes", "class" => class).set(value(bytes));
    }
    metrics::gauge!("mkdb2_cdn_gc_dangling_references").set(value(report.dangling_references()));
    metrics::gauge!("mkdb2_cdn_gc_unparsed_roots").set(value(unparsed));
    metrics::gauge!("mkdb2_cdn_gc_missing_references").set(value(missing));
}

/// A root's children, or `None` if the object is gone. Its failures stay out of the store's GCS error counters, whose `read` class drives a user-facing alert.
async fn fetch_children(
    reads: &dyn ObjectStore,
    budget: &Semaphore,
    root: &CdnGcKeySize,
) -> Result<Option<Vec<String>>> {
    let _permits = budget
        .acquire_many(root.size.min(FETCH_BUDGET.into()) as u32)
        .await?;
    let bytes = match reads.get(&ObjectPath::from(root.key.as_str())).await {
        Ok(result) => result.bytes().await?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    Ok(Some(manifest_children(&bytes)))
}

/// Every key a viewer can reach through a root, parsed as the frontend parses it: the browser's decode (BOM dropped, lossy UTF-8), then the shared struct, whatever the root's class or extension.
fn manifest_children(bytes: &[u8]) -> Vec<String> {
    let bytes = bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(bytes);
    // Only an object or an array deserializes into a struct. Checking first skips decoding a binary root at all.
    let first = bytes
        .iter()
        .find(|b| !matches!(b, b' ' | b'\t' | b'\n' | b'\r'));
    if !matches!(first, Some(b'{' | b'[')) {
        return Vec::new();
    }
    let folded: String;
    let text = match std::str::from_utf8(bytes) {
        Ok(text) => text,
        // The browser decodes invalid UTF-8 to U+FFFD. Keys and JSON syntax are ASCII, so `?` for every non-ASCII byte yields the same links (a syntax error outside a string, a non-key inside one) in a same-size copy, where U+FFFD could triple a large root.
        Err(_) => {
            folded = bytes
                .iter()
                .map(|&b| if b.is_ascii() { b as char } else { '?' })
                .collect();
            &folded
        }
    };
    let Ok(manifest) = serde_json::from_str::<cdn_manifest::Manifest<IgnoredAny>>(text) else {
        return Vec::new();
    };
    let mut children: Vec<String> = manifest
        .resources()
        .filter(|key| validate_hosted_key(key))
        .map(str::to_owned)
        .collect();
    children.sort_unstable();
    children.dedup();
    children
}

async fn stage<T>(name: &'static str, work: impl Future<Output = Result<T>>) -> Result<T> {
    let started = Instant::now();
    let result = work.await;
    metrics::histogram!("mkdb2_cdn_gc_stage_duration_seconds", "stage" => name)
        .record(started.elapsed().as_secs_f64());
    if result.is_err() {
        metrics::counter!("mkdb2_cdn_gc_failures_total", "stage" => name).increment(1);
    }
    result.with_context(|| format!("CDN collector {name} stage"))
}

fn unix_seconds(time: SystemTime) -> u32 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(u32::MAX.into()) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdn::content_key;
    use serde_json::json;

    fn key(fill: char, ext: &str) -> String {
        format!("{}.{ext}", fill.to_string().repeat(64))
    }

    fn candidate(key: &str) -> CdnGcKeySize {
        CdnGcKeySize {
            key: key.to_owned(),
            size: 1,
        }
    }

    fn claimed(claim: &Claim) -> Vec<String> {
        claim.keys.iter().map(|c| c.key.clone()).collect()
    }

    /// Claims `keys` and releases them at once.
    fn claim_released(fence: &Arc<Fence>, keys: &[&str]) -> Vec<String> {
        claimed(&fence.claim(keys.iter().map(|key| candidate(key)).collect()))
    }

    #[test]
    fn modes_parse_strictly() {
        for (raw, mode) in [
            ("off", Mode::Off),
            ("report", Mode::Report),
            ("delete", Mode::Delete),
        ] {
            assert_eq!(Mode::parse(raw).unwrap(), mode);
        }
        for bad in ["Delete", "on", "true", "dry-run"] {
            assert!(Mode::parse(bad).is_err(), "accepted {bad:?}");
        }
    }

    #[test]
    fn manifest_children_are_what_the_frontend_links() {
        let (a, b) = (key('a', "png"), key('b', "pdf"));
        let children = |manifest: String| manifest_children(manifest.as_bytes());

        // Galleries and resource lists alike; the frontend ignores `class` when linking items.
        assert_eq!(
            children(json!({"v": 1, "class": "image_gallery", "items": [{"resource": a}, {"resource": b, "filename": "b.pdf"}]}).to_string()),
            [a.clone(), b.clone()]
        );
        // Positional arrays, a metadata payload the collector skips, and unknown fields serde skips unparsed.
        let deep = format!("{}1{}", "[".repeat(200), "]".repeat(200));
        for manifest in [
            format!(r#"[1, "image_gallery", [{{"resource": "{a}"}}]]"#),
            format!(r#"{{"items": [["{a}", null]]}}"#),
            format!(r#"{{"items": [{{"resource": "{a}"}}], "data": {{"n": 1e400}}}}"#),
            format!(r#"{{"items": [{{"resource": "{a}", "n": 1e400}}]}}"#),
            format!(r#"{{"items": [{{"resource": "{a}", "content_type": "\ud800"}}]}}"#),
            format!(r#"{{"items": [{{"resource": "{a}", "deep": {deep}}}]}}"#),
        ] {
            assert_eq!(
                children(manifest.clone()),
                std::slice::from_ref(&a),
                "{manifest}"
            );
        }
        // Non-key resources drop out; duplicates collapse.
        assert_eq!(
            children(
                json!({"items": [{"resource": "pending:0123"}, {"resource": a}, {"resource": a}]})
                    .to_string()
            ),
            std::slice::from_ref(&a)
        );
        // Whatever the frontend can't parse shows nothing, so it protects nothing.
        for manifest in [
            json!({"items": [{"resource": a}, {"resource": 5}]}),
            json!({"v": "x", "items": [{"resource": a}]}),
            json!({"items": {"resource": a}}),
            json!({"v": 1, "class": "metadata", "data": {"items": [{"resource": a}]}}),
        ] {
            assert!(children(manifest.to_string()).is_empty(), "{manifest}");
        }
        assert!(manifest_children(b"\x89PNG\r\n\x1a\n").is_empty());

        // The browser decodes lossily and drops a BOM, so neither hides the items.
        let mut bytes = b"\xef\xbb\xbf{\"items\":[{\"resource\":\"".to_vec();
        bytes.extend(a.as_bytes());
        bytes.extend(b"\",\"caption\":\"\xc3\xa9\xff\"}]}");
        assert_eq!(manifest_children(&bytes), std::slice::from_ref(&a));
        // An invalid byte inside a resource makes it no key, as U+FFFD does in the browser.
        let mut bytes = br#"{"items": [{"resource": ""#.to_vec();
        bytes.extend(a.as_bytes());
        bytes.extend(b"\xff\"}]}");
        assert!(manifest_children(&bytes).is_empty());
    }

    #[tokio::test]
    async fn fetch_children_clamps_an_oversized_root_and_skips_a_gone_one() {
        let bucket = object_store::memory::InMemory::new();
        let (root, child) = (key('c', "json"), key('d', "png"));
        let body = format!(r#"{{"items": [{{"resource": "{child}"}}]}}"#);
        bucket
            .put(&ObjectPath::from(root.as_str()), body.into())
            .await
            .unwrap();
        let budget = Semaphore::new(FETCH_BUDGET as usize);
        let fetch = |key: String| CdnGcKeySize {
            key,
            size: 2 * u64::from(FETCH_BUDGET),
        };
        assert_eq!(
            fetch_children(&bucket, &budget, &fetch(root))
                .await
                .unwrap(),
            Some(vec![child])
        );
        assert_eq!(budget.available_permits(), FETCH_BUDGET as usize);
        assert_eq!(
            fetch_children(&bucket, &budget, &fetch(key('e', "json")))
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn a_root_that_fails_to_read_is_an_error_not_gone() {
        let dir = tempfile::tempdir().unwrap();
        let root = key('c', "json");
        // A symlink to itself: opening it fails with ELOOP, even as root.
        std::os::unix::fs::symlink(&root, dir.path().join(&root)).unwrap();
        let bucket = object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let budget = Semaphore::new(FETCH_BUDGET as usize);
        assert!(fetch_children(&bucket, &budget, &candidate(&root))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn an_upload_waits_for_a_claim_on_its_key() {
        let dir = tempfile::tempdir().unwrap();
        let store = CdnStore::Fs(crate::cdn_store::FsStore::new(dir.path().to_owned()));
        let fence = Arc::new(Fence::default());
        let uploads = Uploads {
            fence: fence.clone(),
            // Never contacted: only a dedup upload logs an ack.
            ch: Arc::new(ChClient::new("http://127.0.0.1:9").unwrap()),
        };
        let key = key('a', "png");
        let claim = fence.claim(vec![candidate(&key)]);
        let mut put = std::pin::pin!(uploads.put(&store, &key, Bytes::from_static(b"a")));
        // Unfenced, a one-byte local write finishes well inside this.
        assert!(
            tokio::time::timeout(Duration::from_millis(500), put.as_mut())
                .await
                .is_err()
        );
        drop(claim);
        assert_eq!(put.await.unwrap(), PutOutcome::Created);
    }

    #[tokio::test]
    async fn claims_skip_uploads_in_flight_and_uploads_wait_for_claims() {
        let fence = Arc::new(Fence::default());
        let first = fence.begin_upload("a.png").await;
        let second = fence.begin_upload("a.png").await;
        let claim = fence.claim(vec![candidate("a.png"), candidate("b.png")]);
        assert_eq!(claimed(&claim), ["b.png"]);

        // An upload of a claimed key waits for the release, then proceeds.
        let mut waiting = std::pin::pin!(fence.begin_upload("b.png"));
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        drop(claim);
        let third = waiting.await;
        assert!(claim_released(&fence, &["b.png"]).is_empty());
        drop(third);
        assert_eq!(claim_released(&fence, &["b.png"]), ["b.png"]);

        // A key stays unclaimable until its last concurrent upload finishes.
        drop(first);
        assert!(claim_released(&fence, &["a.png"]).is_empty());
        drop(second);
        assert_eq!(claim_released(&fence, &["a.png"]), ["a.png"]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_claim_in_doubt_stays_fenced_for_the_settle_time() {
        let fence = Arc::new(Fence::default());
        let mut claim = fence.claim(vec![candidate("a.png")]);
        claim.in_doubt = true;
        drop(claim);
        let mut waiting = std::pin::pin!(fence.begin_upload("a.png"));
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        assert!(claim_released(&fence, &["a.png"]).is_empty());
        tokio::time::sleep(DELETE_SETTLE - Duration::from_millis(1)).await;
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        drop(waiting.await);
        assert_eq!(claim_released(&fence, &["a.png"]), ["a.png"]);
    }

    /// The collector's root filter accepts exactly the route's key grammar.
    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_CLICKHOUSE_URL"]
    async fn live_key_pattern_is_the_route_grammar() -> Result<()> {
        let _suite_guard = crate::pg::live_database_suite_gate().lock().await;
        let url = std::env::var("KYMO_LIVE_TEST_CLICKHOUSE_URL")
            .context("KYMO_LIVE_TEST_CLICKHOUSE_URL is required")?;
        let ch = ChClient::new(&url)?;
        for sample in [
            key('a', "png").as_str(),
            "abcd.PNG",
            "ABCD.bin",
            "abc.png",
            "abcd.exe",
            "abcd.png.png",
            "pending:abcd",
            "xyz0.png",
            "abcd.",
        ] {
            let matched = ch
                .test_client()
                .query("SELECT match(?, ?)")
                .bind(sample)
                .bind(hosted_key_pattern())
                .fetch_one::<u8>()
                .await?;
            assert_eq!(matched == 1, validate_hosted_key(sample), "{sample}");
        }
        Ok(())
    }

    /// Delete-mode passes inside and past the grace, the guards between classification and a DELETE, and an upload's dedup ack, against an in-memory bucket and a throwaway ClickHouse (docs/live-database-tests.md).
    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_CLICKHOUSE_URL"]
    async fn live_pass_deletes_only_unreachable_objects_past_the_grace() -> Result<()> {
        use object_store::memory::InMemory;
        let _suite_guard = crate::pg::live_database_suite_gate().lock().await;
        let url = std::env::var("KYMO_LIVE_TEST_CLICKHOUSE_URL")
            .context("KYMO_LIVE_TEST_CLICKHOUSE_URL is required")?;
        let ch = Arc::new(ChClient::new(&url)?);
        ch.ensure_schema().await?;

        // Unique content per run: keys are content addresses, and the ack log and the children cache outlive the run.
        let run = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
        let bucket = Arc::new(InMemory::new());
        let put = |body: String, ext: &str| {
            let bucket = bucket.clone();
            let key = content_key(body.as_bytes(), ext);
            async move {
                bucket
                    .put(&ObjectPath::from(key.as_str()), body.into())
                    .await
                    .map(|_| key)
            }
        };
        let child_a = put(format!("image a {run}"), "png").await?;
        let child_b = put(format!("report b {run}"), "PDF").await?;
        let child_c = put(format!("image c {run}"), "png").await?;
        let gallery = put(
            json!({"v": 1, "class": "image_gallery", "items": [{"resource": child_a}, {"resource": child_b, "filename": "b.pdf"}], "run": run}).to_string(),
            "json",
        )
        .await?;
        let resources = put(
            json!({"items": [{"resource": child_c}], "run": run}).to_string(),
            "json",
        )
        .await?;
        let orphan_body = format!("orphan {run}");
        let orphan = put(orphan_body.clone(), "png").await?;
        let acked = put(format!("acked {run}"), "png").await?;
        let foreign = format!("probe-{run}.txt");
        bucket
            .put(&ObjectPath::from(foreign.as_str()), "probe".into())
            .await?;
        let dangling = content_key(format!("never uploaded {run}").as_bytes(), "png");

        let project_id = format!("cdn gc live {run}");
        let row = |metric_name: &str, cdn_key: &str| crate::clickhouse::MetricRow {
            project_id: project_id.clone(),
            run_id: "r".to_owned(),
            metric_name: metric_name.to_owned(),
            tag: String::new(),
            step: 0,
            timestamp_ms: 0,
            value: None,
            cdn_key: Some(cdn_key.to_owned()),
            text_data: None,
        };
        ch.insert_batch(
            &[
                row("gallery", &gallery),
                row("dangling", &dangling),
                row("placeholder", &format!("pending:{run}")),
            ],
            Duration::from_secs(30),
            true,
        )
        .await?;
        ch.insert_rich_mutation(
            &crate::clickhouse::RichMetricRow {
                project_id: project_id.clone(),
                run_id: "r".to_owned(),
                metric_name: "resources".to_owned(),
                tag: String::new(),
                step: 0,
                timestamp_ms: 0,
                cdn_key: resources.clone(),
                mutation_version: (1 << 32) | 1,
            },
            Duration::from_secs(30),
        )
        .await?;

        let config = Config {
            mode: Mode::Delete,
            deletes: Some(bucket.clone()),
            max_candidates: None,
        };
        let (uploads, collector) = start(config, bucket.clone(), ch.clone()).await?;

        // Everything was just created: the grace keeps all of it.
        let summary = collector.pass(SystemTime::now()).await?;
        assert_eq!(summary.inventory.objects, 8);
        assert_eq!(summary.inventory.foreign_objects, 1);
        assert_eq!(summary.unparsed, 0);
        assert_eq!(summary.report.referenced_objects, 5);
        assert_eq!(summary.report.candidate_objects, 0);
        assert!(summary.report.dangling_references() >= 1);
        assert_eq!(summary.deleted.objects, 0);

        // Past the grace, with `acked` dedup-acked just now: only the orphan goes.
        let later = SystemTime::now() + GRACE + Duration::from_secs(24 * 3600);
        ch.record_cdn_ack(&acked, unix_seconds(later)).await?;
        let summary = collector.pass(later).await?;
        assert_eq!(summary.report.referenced_objects, 5);
        assert_eq!(summary.report.candidate_objects, 1);
        assert_eq!(
            (summary.deleted.objects, summary.deleted.bytes),
            (1, orphan_body.len() as u64)
        );
        assert!(matches!(
            bucket.head(&ObjectPath::from(orphan.as_str())).await,
            Err(object_store::Error::NotFound { .. })
        ));
        for kept in [
            &gallery, &resources, &child_a, &child_b, &child_c, &acked, &foreign,
        ] {
            bucket.head(&ObjectPath::from(kept.as_str())).await?;
        }

        // A dangling key uploaded since the listing is no longer missing.
        assert!(collector.missing_references().await?.contains(&dangling));
        put(format!("never uploaded {run}"), "png").await?;
        assert!(!collector.missing_references().await?.contains(&dangling));

        // A links-version bump re-parses every root.
        ch.cdn_gc_collect_unparsed(cdn_manifest::LINKS_VERSION + 1)
            .await?;
        let reparse: Vec<_> = ch
            .cdn_gc_page("unparsed", "", 100)
            .await?
            .into_iter()
            .map(|root| root.key)
            .collect();
        for root in [&gallery, &resources] {
            assert!(reparse.contains(root), "{root} not re-parsed");
        }

        // Guards after classification: disarming, the ack recheck, the upload fence, and the claim a failed DELETE leaves in doubt (its settle time is `a_claim_in_doubt_stays_fenced_for_the_settle_time`).
        let late_acked = put(format!("late ack {run}"), "png").await?;
        let uploading = put(format!("uploading {run}"), "png").await?;
        let latest = later + GRACE + Duration::from_secs(24 * 3600);
        let cutoff = unix_seconds(latest) - GRACE.as_secs() as u32;
        collector.inventory().await?;
        let (report, _, _) = collector.classify(cutoff).await?;
        let log_start = ch.cdn_ack_log_start().await?.context("no ack log")?;
        let over_ceiling = CdnGcReport {
            candidate_objects: CEILING_FLOOR + 1,
            ..Default::default()
        };
        for (cutoff, unparsed, report) in [
            (cutoff, 1, &report),
            (log_start - 1, 0, &report),
            (cutoff, 0, &over_ceiling),
        ] {
            let deleted = collector
                .delete_if_safe(bucket.as_ref(), cutoff, unparsed, report)
                .await?;
            assert_eq!(
                deleted.objects, 0,
                "armed at {cutoff} with {unparsed} unparsed and {} candidates",
                report.candidate_objects
            );
        }
        // After the candidate query, `late_acked` is acked and `uploading` held: both are spared, and only `acked` goes.
        ch.record_cdn_ack(&late_acked, unix_seconds(latest)).await?;
        let hold = collector.fence.begin_upload(&uploading).await;
        let deleted = collector.delete_candidates(bucket.as_ref(), cutoff).await?;
        drop(hold);
        assert_eq!((deleted.objects, deleted.spared), (1, 2));
        for kept in [&late_acked, &uploading] {
            bucket.head(&ObjectPath::from(kept.as_str())).await?;
        }
        // Removing a directory fails without NotFound.
        let dir = tempfile::tempdir()?;
        std::fs::create_dir(dir.path().join(&uploading))?;
        let refusing = object_store::local::LocalFileSystem::new_with_prefix(dir.path())?;
        assert!(collector
            .delete_candidates(&refusing, cutoff)
            .await
            .is_err());
        assert!(collector
            .fence
            .claim(vec![candidate(&uploading)])
            .keys
            .is_empty());

        // Another links version's row for a cached root never merges this version's away (an image rolled back across a bump).
        ch.cdn_gc_insert_manifests(&[CdnManifestRow {
            parent: gallery.clone(),
            links_version: cdn_manifest::LINKS_VERSION + 1,
            children: Vec::new(),
        }])
        .await?;
        ch.test_client()
            .query("OPTIMIZE TABLE mkdb2.cdn_manifest_children FINAL")
            .execute()
            .await?;
        let kept = ch
            .test_client()
            .query("SELECT count() FROM mkdb2.cdn_manifest_children WHERE parent = ? AND links_version = ?")
            .bind(&gallery)
            .bind(cdn_manifest::LINKS_VERSION)
            .fetch_one::<u64>()
            .await?;
        assert_eq!(kept, 1);

        // An upload logs a dedup ack, not a fresh create.
        let root = tempfile::tempdir()?;
        let store = CdnStore::Fs(crate::cdn_store::FsStore::new(root.path().to_owned()));
        let body = format!("uploaded {run}");
        let uploaded = content_key(body.as_bytes(), "txt");
        let cutoff = unix_seconds(SystemTime::now()) - 60;
        assert_eq!(
            uploads.put(&store, &uploaded, body.clone().into()).await?,
            PutOutcome::Created
        );
        assert!(ch
            .cdn_gc_acked_since(std::slice::from_ref(&uploaded), cutoff)
            .await?
            .is_empty());
        assert_eq!(
            uploads.put(&store, &uploaded, body.into()).await?,
            PutOutcome::Existing
        );
        assert!(ch
            .cdn_gc_acked_since(std::slice::from_ref(&uploaded), cutoff)
            .await?
            .contains(&uploaded));

        // A newer start row restarts the log (the documented reset after a gap); one second later keeps later runs armed.
        let start = ch.cdn_ack_log_start().await?.context("no ack log")?;
        ch.record_cdn_ack("", start + 1).await?;
        assert_eq!(ch.cdn_ack_log_start().await?, Some(start + 1));

        for table in [
            "mkdb2.metrics",
            "mkdb2.rich_metrics",
            "mkdb2.metric_registry_outbox",
        ] {
            ch.test_client()
                .query(&format!(
                    "ALTER TABLE {table} DELETE WHERE project_id = ? SETTINGS mutations_sync = 2"
                ))
                .bind(&project_id)
                .execute()
                .await?;
        }
        Ok(())
    }
}
