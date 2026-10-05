//! Garbage collector for the CDN's media, in the GCS bucket or the filesystem store (docs/cdn-gcs-migration.md § Garbage collection).
//!
//! A pass lists the store into ClickHouse, collects every key a viewer can reach (metric-row roots plus their manifests' children), and derives candidates: unreferenced objects neither created nor re-uploaded within the grace (a dedup ack in the log, or a mark on the file). Report mode publishes the counts; delete mode also deletes the candidates, claiming each batch through the upload fence first.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::body::Bytes;
use futures::{FutureExt as _, StreamExt, TryStreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt as _, RetryConfig};
use serde::de::IgnoredAny;
use tokio::sync::{Notify, Semaphore};

use crate::activity::ActivityTracker;
use crate::alerts::UserAlert;
use crate::cdn::validate_hosted_key;
use crate::cdn_store::{
    count_gcs_error, credentialed_builder, CdnStore, FsStore, Identity, Listed, PutOutcome,
    MARK_INTERVAL,
};
use crate::clickhouse::{
    CdnGcKeySize, CdnGcQuestion, CdnGcReport, CdnInventoryRow, CdnManifestRow, ChClient,
};
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
/// Delete mode's ceiling is this plus a tenth of the referenced objects (docs § Garbage collection).
const CEILING_BASE: u64 = 10_000;
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

/// The collector's settings. Delete mode on GCS impersonates a separate delete-capable identity; the filesystem store needs none, since the server owns the files.
pub struct Config {
    mode: Mode,
    deletes: Option<Arc<dyn ObjectStore>>,
    max_candidates: Option<u64>,
    /// Local mode: a pass over the ceiling asks the user in the notice bar (`Status::answer`), since no env reaches a local server.
    asks: bool,
}

impl Config {
    /// `KYMO_CDN_GC` for the GCS `bucket` (absent = report), or for the filesystem store with `None` (absent = delete). GCS delete mode reads its credential file here, before any database work.
    pub fn from_env(bucket: Option<&str>) -> Result<Self> {
        let default = bucket.map_or("delete", |_| "report");
        let mode = Mode::parse(&crate::env::required_string_or("KYMO_CDN_GC", default)?)?;
        let mut config = Self {
            mode,
            deletes: None,
            max_candidates: None,
            asks: false,
        };
        if mode != Mode::Delete {
            return Ok(config);
        }
        if let Some(bucket) = bucket {
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
            config.deletes = Some(Arc::new(store));
        }
        let max_candidates = crate::env::required_optional_string("KYMO_CDN_GC_MAX_CANDIDATES")?;
        config.max_candidates = max_candidates
            .map(|raw| {
                raw.trim()
                    .parse::<u64>()
                    .with_context(|| format!("KYMO_CDN_GC_MAX_CANDIDATES={raw:?} is not a count"))
            })
            .transpose()?;
        Ok(config)
    }

    /// Local mode reads no env, so the stack behaves the same whichever process woke it: it always deletes, like the run reaper.
    pub fn local() -> Self {
        Self {
            mode: Mode::Delete,
            deletes: None,
            max_candidates: None,
            asks: true,
        }
    }
}

/// The in-process fence between uploads and deletion; the server is the store's only writer (a singleton), so this is the whole writer set. An upload holds its key from before the store write until its dedup ack is logged (or its file marked). Deletion claims only keys with no upload in flight, and an upload of a claimed key waits for the release, then creates the object afresh.
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

/// The upload route's side of the collector: every upload goes through the fence. With GCS, every dedup ack is logged before it is returned; the filesystem store marks the file instead (`cdn_store::MARK_INTERVAL`).
#[cfg_attr(test, derive(Default))]
pub struct Uploads {
    fence: Arc<Fence>,
    /// The ack log, for the GCS bucket only.
    acks: Option<Arc<ChClient>>,
}

impl Uploads {
    pub async fn put(&self, store: &CdnStore, key: &str, body: Bytes) -> Result<PutOutcome> {
        let _hold = self.fence.begin_upload(key).await;
        let outcome = store.put_if_absent(key, body).await?;
        if let (PutOutcome::Existing, Some(acks)) = (outcome, &self.acks) {
            acks.record_cdn_ack(key, unix_seconds(SystemTime::now()))
                .await?;
        }
        Ok(outcome)
    }
}

/// What a collector lists, reads and deletes. The filesystem store is walked directly: `object_store`'s local lister follows symlinks and fails a whole listing on one non-UTF-8 name. `read` and `delete` take keys the inventory listed (canonical for the files store); only `exists` takes any referenced key, so only it validates.
#[derive(Clone)]
pub enum Media {
    Bucket(Arc<dyn ObjectStore>),
    Files(FsStore),
}

impl Media {
    /// An object's bytes, or `None` if it is gone.
    async fn read(&self, key: &str) -> Result<Option<Bytes>> {
        match self {
            Media::Bucket(bucket) => match bucket.get(&ObjectPath::from(key)).await {
                Ok(result) => Ok(Some(result.bytes().await?)),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(error) => Err(error.into()),
            },
            Media::Files(files) => match tokio::fs::read(files.path_for(key)).await {
                Ok(bytes) => Ok(Some(bytes.into())),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(error.into()),
            },
        }
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        match self {
            Media::Bucket(bucket) => match bucket.head(&ObjectPath::from(key)).await {
                Ok(_) => Ok(true),
                Err(object_store::Error::NotFound { .. }) => Ok(false),
                Err(error) => Err(error.into()),
            },
            // Only a canonical key can be served (`FsStore::get`).
            Media::Files(files) => Ok(crate::cdn::validate_local_key(key)
                && tokio::fs::try_exists(files.path_for(key)).await?),
        }
    }

    /// `Ok(false)` when the object was already gone.
    async fn delete(&self, key: &str) -> Result<bool> {
        match self {
            Media::Bucket(bucket) => match bucket.delete(&ObjectPath::from(key)).await {
                Ok(()) => Ok(true),
                Err(object_store::Error::NotFound { .. }) => Ok(false),
                Err(error) => Err(error.into()),
            },
            Media::Files(files) => match tokio::fs::remove_file(files.path_for(key)).await {
                Ok(()) => Ok(true),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(error) => Err(error.into()),
            },
        }
    }
}

/// The collector's conditions, which `/alerts` serves when no Prometheus evaluates its gauges, and a local user's answers to the question a pass over the ceiling asks: delete its files, or keep them.
pub struct Status {
    alerts: Mutex<Vec<UserAlert>>,
    /// The turn: held by a pass from its candidate query through publishing its conditions, and by an answer throughout. It holds the id of the last question "Delete them" approved; ids are unique, so a withdrawn one never matches again.
    turn: tokio::sync::Mutex<Option<u64>>,
    ch: Arc<ChClient>,
    wake: Notify,
}

/// The local user's answer to the question a pass over the ceiling asks (`POST /media-cleanup/{id}/delete|keep`).
#[derive(serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Answer {
    Delete,
    Keep,
}

/// Off ClickHouse: an answer fails.
#[cfg(test)]
impl Default for Status {
    fn default() -> Self {
        Self::new(Arc::new(ChClient::new("http://127.0.0.1:9").unwrap()))
    }
}

impl Status {
    pub(crate) fn new(ch: Arc<ChClient>) -> Self {
        Self {
            alerts: Mutex::default(),
            turn: tokio::sync::Mutex::default(),
            ch,
            wake: Notify::new(),
        }
    }

    pub fn alerts(&self) -> Vec<UserAlert> {
        self.alerts.lock().unwrap().clone()
    }

    /// Answers question `id` and wakes the pass loop. "Keep them" keeps its files out of cleanup for the grace from `now()`, read once the turn is taken (`ChClient::cdn_gc_keep_question`), and withdraws it; "Delete them" approves it, so the next pass that can delete deletes exactly its files. `false` when `id` isn't a standing question awaiting an answer.
    pub async fn answer(
        &self,
        id: u64,
        answer: Answer,
        now: impl FnOnce() -> SystemTime,
    ) -> Result<bool> {
        let mut approved = self.turn.lock().await;
        let standing = *approved != Some(id)
            && (self.ch.cdn_gc_question().await?).is_some_and(|question| question.id == id);
        if standing {
            match answer {
                Answer::Keep => {
                    self.ch
                        .cdn_gc_keep_question(id, unix_seconds(now()))
                        .await?;
                    self.ch.cdn_gc_withdraw_question().await?;
                }
                Answer::Delete => *approved = Some(id),
            }
            self.wake.notify_one();
        }
        // Answered now, or before by an answer whose reply was lost: either way it awaits no answer.
        self.alerts
            .lock()
            .unwrap()
            .retain(|alert| alert.question != Some(id));
        Ok(standing)
    }

    /// The notice for the stored question, unless "Delete them" approved it (`approved`, the turn's value). A failed read keeps the one shown, since every pass reads it again.
    async fn asked(&self, approved: Option<u64>) -> Option<UserAlert> {
        match self.ch.cdn_gc_question().await {
            Ok(question) => question
                .filter(|question| approved != Some(question.id))
                .map(|question| notice(question, SystemTime::now())),
            Err(error) => {
                tracing::warn!(
                    error = format!("{error:#}"),
                    "reading the CDN collector's question failed"
                );
                self.alerts()
                    .into_iter()
                    .find(|alert| alert.question.is_some())
            }
        }
    }

    /// Replaces the conditions, keeping the onset of each that still holds, so a dismissal lasts until it clears.
    pub(crate) fn publish(&self, mut current: Vec<UserAlert>) {
        let mut alerts = self.alerts.lock().unwrap();
        for alert in &mut current {
            // A question is its own condition: a later one has a later onset.
            if let Some(held) = alerts
                .iter()
                .find(|held| held.name == alert.name && held.question == alert.question)
            {
                alert.active_at.clone_from(&held.active_at);
            }
        }
        *alerts = current;
    }
}

pub struct Collector {
    mode: Mode,
    ch: Arc<ChClient>,
    fence: Arc<Fence>,
    media: Media,
    /// Where delete mode deletes: the bucket through its delete identity, or the filesystem store itself.
    deletes: Option<Media>,
    max_candidates: Option<u64>,
    asks: bool,
    status: Arc<Status>,
    activity: Arc<ActivityTracker>,
}

/// Builds both halves. The ack log's schema must exist before the upload route serves: its start row, written at the first start with the collector, also arms the filesystem store, whose marks begin then.
pub async fn start(
    config: Config,
    media: Media,
    ch: Arc<ChClient>,
    activity: Arc<ActivityTracker>,
) -> Result<(Uploads, Collector)> {
    ch.ensure_cdn_gc_schema(
        GRACE.as_secs() / (24 * 3600),
        unix_seconds(SystemTime::now()),
    )
    .await?;
    let status = Arc::new(Status::new(ch.clone()));
    // A question an earlier run asked shows from the start, not once a pass gets to it.
    if config.asks {
        status.publish(status.asked(None).await.into_iter().collect());
    }
    let fence = Arc::new(Fence::default());
    let deletes = match &media {
        Media::Bucket(_) => config.deletes.map(Media::Bucket),
        Media::Files(_) => (config.mode == Mode::Delete).then(|| media.clone()),
    };
    Ok((
        Uploads {
            fence: fence.clone(),
            acks: matches!(media, Media::Bucket(_)).then(|| ch.clone()),
        },
        Collector {
            mode: config.mode,
            status,
            ch,
            fence,
            media,
            deletes,
            max_candidates: config.max_candidates,
            asks: config.asks,
            activity,
        },
    ))
}

#[derive(Debug, Default)]
struct Inventory {
    objects: u64,
    bytes: u64,
    foreign_objects: u64,
    foreign_bytes: u64,
    /// Symlinks in the filesystem store, counted among the foreign objects.
    symlinks: u64,
}

#[derive(Debug)]
struct PassSummary {
    inventory: Inventory,
    report: CdnGcReport,
    unparsed: u64,
    /// Referenced keys confirmed missing.
    missing_references: u64,
    deleted: Deleted,
}

#[derive(Debug, Default)]
struct Deleted {
    objects: u64,
    bytes: u64,
    /// Already gone when deleted.
    already_gone: u64,
    /// Candidates the fence or the re-upload recheck kept alive.
    spared: u64,
    disarmed: Option<Disarmed>,
}

/// Why a delete-mode pass deleted nothing, in the order checked (`disarmed`).
#[derive(Debug, Clone, Copy, PartialEq)]
enum Disarmed {
    /// The dedup log, whose start row also arms the filesystem store, doesn't span the grace yet.
    AckLog,
    Unparsed,
    /// Symlinks in the filesystem store: what they lead to is outside the inventory.
    Symlinks,
    /// A local user hasn't answered the question a pass over the ceiling asked.
    Asked,
    Ceiling(u64),
}

impl Collector {
    /// Starts the pass loop; returns the conditions `/alerts` serves.
    pub fn spawn(self) -> Arc<Status> {
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
        metrics::gauge!("mkdb2_cdn_gc_root_index_verified").set(0.0);
        if self.mode == Mode::Off {
            tracing::warn!("CDN collector is off (KYMO_CDN_GC=off): no inventory, no collection");
            return self.status;
        }
        tracing::info!(mode = ?self.mode, "CDN collector configured");
        let status = self.status.clone();
        tokio::spawn(async move {
            // An answer (`Status::answer`'s permit, kept until taken) cuts any wait short, the first one's included.
            let mut wait = FIRST_PASS_DELAY;
            loop {
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    () = self.status.wake.notified() => {}
                }
                wait = PASS_INTERVAL;
                let started = Instant::now();
                // Blocks a local stack's idle stop until the pass ends, without restarting its idle clock.
                let work = self.activity.begin_work();
                let outcome = caught(self.pass(SystemTime::now())).await;
                drop(work);
                match outcome {
                    Ok(PassSummary {
                        inventory,
                        report,
                        unparsed,
                        deleted,
                        ..
                    }) => {
                        metrics::gauge!("mkdb2_cdn_gc_last_success_unixtime_seconds")
                            .set(unix_time_seconds());
                        tracing::info!(
                            mode = ?self.mode,
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
                            already_gone = deleted.already_gone,
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
            }
        });
        status
    }

    /// The bucket's objects are never rewritten, so their creation time is final. A file's ctime can trail its last re-upload by up to `MARK_INTERVAL`.
    fn grace(&self) -> Duration {
        match self.media {
            Media::Bucket(_) => GRACE,
            Media::Files(_) => GRACE + MARK_INTERVAL,
        }
    }

    /// The notice bar's view of a pass.
    fn conditions(&self, outcome: &Result<PassSummary>, now: SystemTime) -> Vec<UserAlert> {
        let since = rfc3339(now);
        let alert = |name: &str, summary| UserAlert {
            name: name.to_owned(),
            summary,
            class: None,
            active_at: since.clone(),
            question: None,
        };
        let summary = match outcome {
            Ok(summary) => summary,
            Err(_) => {
                // A failed pass learned nothing about the other conditions, so they stand.
                let mut alerts = self.status.alerts();
                if !alerts.iter().any(|held| held.name == "MediaCleanupFailed") {
                    alerts.push(alert(
                        "MediaCleanupFailed",
                        "Garbage collection of media failed; the server log has the cause."
                            .to_owned(),
                    ));
                }
                return alerts;
            }
        };
        let mut alerts = Vec::new();
        if summary.missing_references > 0 {
            alerts.push(alert(
                "MediaMissing",
                format!(
                    "Some images or files that runs reference are missing from media storage ({} confirmed).",
                    summary.missing_references
                ),
            ));
        }
        // Each of these pauses deletion on its own, so each shows whatever else pauses it, the ack log's warmup included.
        if self.mode == Mode::Delete && summary.unparsed > 0 {
            alerts.push(alert(
                "MediaCleanupUnreadable",
                format!("Garbage collection of media paused: {} galleries or file lists couldn't be read; the server log names them.", summary.unparsed),
            ));
        }
        if self.mode == Mode::Delete && summary.inventory.symlinks > 0 {
            alerts.push(alert(
                "MediaCleanupSymlinks",
                format!("Garbage collection of media paused: the media directory holds {} symlinks, which it can't account for; replace them with what they point to.", summary.inventory.symlinks),
            ));
        }
        // Only an operator sees this: a pass that can ask turns it into a question (`delete_if_safe`). The value to set stays ungrouped, so it pastes.
        if let Some(Disarmed::Ceiling(ceiling)) = summary.deleted.disarmed {
            let candidates = summary.report.candidate_objects;
            alerts.push(alert(
                "MediaCleanupOverLimit",
                format!("Garbage collection of media paused: wanted to clean up {} files ({}), but the safety limit is {} files. This safety limit is to prevent a buggy runaway GC from deleting everything. If deleting these files is expected, set KYMO_CDN_GC_MAX_CANDIDATES to at least {candidates}.", count(candidates), size(summary.report.candidate_bytes), count(ceiling)),
            ));
        }
        alerts
    }

    /// A pass and the publication of its conditions. Listing the store and collecting references read neither the question nor the ack log, so they run before the turn, and an answer waits only for the rest.
    async fn pass(&self, now: SystemTime) -> Result<PassSummary> {
        // Each half is caught so that a panic still publishes a failed pass, under the turn.
        let listed = caught(self.list()).await;
        let approved = self.status.turn.lock().await;
        let outcome = match listed {
            Ok(listed) => caught(self.judge(listed, now, *approved)).await,
            Err(error) => Err(error),
        };
        let mut conditions = self.conditions(&outcome, SystemTime::now());
        // The bar shows the question as stored, whatever the pass did.
        if self.asks {
            conditions.retain(|alert| alert.question.is_none());
            conditions.extend(self.status.asked(*approved).await);
        }
        self.status.publish(conditions);
        outcome
    }

    /// The pass's first half: lists the store and collects the referenced keys. Returns the inventory's counts and the unparsed-root count.
    async fn list(&self) -> Result<(Inventory, u64)> {
        metrics::gauge!("mkdb2_cdn_gc_delete_armed").set(0.0);
        metrics::gauge!("mkdb2_cdn_gc_root_index_verified").set(0.0);
        let inventory = stage("inventory", self.inventory()).await;
        // A failed listing leaves a gap in the size panels, never a previous listing's count. The filesystem store has the disk gauge instead.
        if let Media::Bucket(_) = self.media {
            let (objects, bytes) = inventory
                .as_ref()
                .map_or((f64::NAN, f64::NAN), |inventory| {
                    (inventory.objects as f64, inventory.bytes as f64)
                });
            metrics::gauge!("mkdb2_cdn_gcs_objects").set(objects);
            metrics::gauge!("mkdb2_cdn_gcs_bytes").set(bytes);
        }
        async {
            let inventory = inventory?;
            stage("references", self.collect_roots()).await?;
            let unparsed = stage("manifests", self.parse_manifests()).await?;
            Ok((inventory, unparsed))
        }
        .await
        .inspect_err(|_| publish_classes(None))
    }

    /// The pass's second half, in its turn: candidates, the report, and deletion.
    async fn judge(
        &self,
        (inventory, unparsed): (Inventory, u64),
        now: SystemTime,
        approved: Option<u64>,
    ) -> Result<PassSummary> {
        let cutoff = unix_seconds(now).saturating_sub(self.grace().as_secs() as u32);
        let classified = async {
            stage("candidates", self.ch.cdn_gc_collect_candidates(cutoff)).await?;
            stage("report", async {
                let report = self.ch.cdn_gc_report().await?;
                let missing = match report.dangling_references() {
                    0 => Vec::new(),
                    _ => self.missing_references().await?,
                };
                if !missing.is_empty() {
                    tracing::warn!(
                        dangling = report.dangling_references(),
                        ?missing,
                        "CDN collector found referenced keys missing from the store"
                    );
                }
                Ok((report, missing.len() as u64))
            })
            .await
        }
        .await;
        publish_classes(
            classified
                .as_ref()
                .ok()
                .map(|(report, missing)| (&inventory, report, unparsed, *missing)),
        );
        let (report, missing_references) = classified?;
        let deleted = match &self.deletes {
            Some(deletes) => {
                stage(
                    "delete",
                    self.delete_if_safe(
                        deletes,
                        cutoff,
                        unparsed,
                        inventory.symlinks,
                        &report,
                        approved,
                    ),
                )
                .await?
            }
            None => Deleted::default(),
        };
        Ok(PassSummary {
            inventory,
            report,
            unparsed,
            missing_references,
            deleted,
        })
    }

    /// Collects the referenced roots. They're read through `idx_cdn_key` only while a pass in the last `INDEX_CHECK_DAYS` checked it under the current fingerprint ([`ChClient::cdn_gc_index_fingerprint`]); until then each pass reads every granule, so the index can never cost it a reference, and then checks the index against that read. Only the root read can fail the pass: a failed check just leaves the index unverified.
    async fn collect_roots(&self) -> Result<()> {
        let state = async {
            let fingerprint = self.ch.cdn_gc_index_fingerprint().await?;
            let checked = self.ch.cdn_gc_index_checked(&fingerprint).await?;
            anyhow::Ok((fingerprint, checked))
        }
        .await;
        let indexed = matches!(state, Ok((_, true)));
        self.ch.cdn_gc_collect_roots("ref", indexed).await?;
        let verified = match state {
            Ok((_, true)) => Ok(true),
            Ok((fingerprint, false)) => self.check_index(&fingerprint).await,
            Err(error) => Err(error),
        }
        .unwrap_or_else(|error| {
            tracing::warn!(
                error = format!("{error:#}"),
                "CDN collector could not check idx_cdn_key; this pass read roots without it"
            );
            false
        });
        metrics::gauge!("mkdb2_cdn_gc_root_index_verified").set(f64::from(u8::from(verified)));
        Ok(())
    }

    /// Runs the indexed scan beside the unindexed one already in scratch, and records `fingerprint` if that scan kept every key of a non-empty root set and its plan shows the index keeping a granule and skipping another. A key that a merge or the run reaper removes between the two reads fails it spuriously, and the next pass checks again. So does every pass on a store too small for the index to skip anything; its unindexed read is cheap.
    async fn check_index(&self, fingerprint: &str) -> Result<bool> {
        self.ch.cdn_gc_collect_roots("indexed_ref", true).await?;
        let comparison = self.ch.cdn_gc_index_comparison().await?;
        if comparison.missed > 0 {
            tracing::warn!(
                missed = comparison.missed,
                sample = ?comparison.sample,
                "CDN collector's indexed root scan missed referenced keys; passes read every granule until a check finds none"
            );
            return Ok(false);
        }
        if comparison.roots == 0 {
            tracing::info!("CDN collector can't check idx_cdn_key yet: nothing is referenced");
            return Ok(false);
        }
        // A kept granule holds a key (or sits in a part without the index), so roots in `rich_metrics` alone, which has no index, don't count.
        let granules = self.ch.cdn_gc_index_granules().await?;
        if !granules.is_some_and(|(kept, considered)| 0 < kept && kept < considered) {
            tracing::info!(
                ?granules,
                "CDN collector can't check idx_cdn_key yet: it kept no granule or skipped none"
            );
            return Ok(false);
        }
        self.ch.record_cdn_gc_index_check(fingerprint).await?;
        tracing::info!(
            "CDN collector checked idx_cdn_key on this ClickHouse; later passes read roots with it"
        );
        Ok(true)
    }

    /// The sampled dangling keys still missing now: an upload that landed after the listing exists by the time of its HEAD. Its failures stay uncounted, like the manifest fetches.
    async fn missing_references(&self) -> Result<Vec<String>> {
        let mut missing = Vec::new();
        for key in self.ch.cdn_gc_dangling(DANGLING_SAMPLE).await? {
            match self.media.exists(&key).await {
                Ok(false) => missing.push(key),
                Ok(true) => {}
                Err(error) => {
                    tracing::warn!(key = %key, error = format!("{error:#}"), "CDN collector could not check a dangling key")
                }
            }
        }
        Ok(missing)
    }

    /// Lists the store into the scratch table; objects outside the key grammar are counted but never collected.
    async fn inventory(&self) -> Result<Inventory> {
        self.ch.cdn_gc_reset().await?;
        let mut inventory = Inventory::default();
        let mut batch = Vec::with_capacity(INSERT_ROWS);
        let mut listing = self.listing();
        while let Some(listed) = listing.try_next().await? {
            inventory.objects += 1;
            match listed {
                Listed::Object { key, size, created } => {
                    inventory.bytes += size;
                    batch.push(CdnInventoryRow {
                        kind: "inventory",
                        key,
                        size,
                        created,
                    });
                }
                Listed::Foreign { size } => {
                    inventory.bytes += size;
                    inventory.foreign_objects += 1;
                    inventory.foreign_bytes += size;
                }
                Listed::Symlink => {
                    inventory.foreign_objects += 1;
                    inventory.symlinks += 1;
                }
            }
            if batch.len() == INSERT_ROWS {
                self.ch.cdn_gc_insert_inventory(&batch).await?;
                batch.clear();
            }
        }
        self.ch.cdn_gc_insert_inventory(&batch).await?;
        Ok(inventory)
    }

    /// The store's objects, in no particular order.
    fn listing(&self) -> futures::stream::BoxStream<'static, Result<Listed>> {
        match &self.media {
            Media::Bucket(bucket) => bucket
                .list(None)
                .map(|meta| {
                    let meta = meta.inspect_err(|_| count_gcs_error("read"))?;
                    let key = meta.location.to_string();
                    Ok(if validate_hosted_key(&key) {
                        Listed::Object {
                            key,
                            size: meta.size,
                            // Last-modified (GCS `updated`): objects are never rewritten, and a later metadata change only delays collection.
                            created: unix_seconds(meta.last_modified.into()),
                        }
                    } else {
                        Listed::Foreign { size: meta.size }
                    })
                })
                .boxed(),
            Media::Files(files) => {
                // The walk blocks, so it runs on its own thread and hands entries over a bounded channel; dropping the stream stops it. The stream ends with the walk's own result, so a walk that fails or panics fails the listing rather than ending it short: a missing manifest would leave its children unprotected.
                let (sender, receiver) = tokio::sync::mpsc::channel(1024);
                let files = files.clone();
                let walked = tokio::task::spawn_blocking(move || {
                    files.walk(&mut |entry| {
                        sender
                            .blocking_send(entry)
                            .map_err(|_| std::io::ErrorKind::BrokenPipe.into())
                    })
                });
                futures::stream::try_unfold(
                    (receiver, walked),
                    |(mut receiver, walked)| async move {
                        if let Some(entry) = receiver.recv().await {
                            return Ok(Some((entry, (receiver, walked))));
                        }
                        walked.await??;
                        anyhow::Ok(None)
                    },
                )
                .boxed()
            }
        }
    }

    /// Parses each stored root once, caches its children, and adds every root's children to the referenced set. Returns how many roots stay unparsed (fetch failures); any unparsed root disarms deletion.
    async fn parse_manifests(&self) -> Result<u64> {
        self.ch
            .cdn_gc_collect_unparsed(cdn_manifest::LINKS_VERSION)
            .await?;
        let roots = self.ch.cdn_gc_count("unparsed").await?;
        let media = &self.media;
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
                    let children = fetch_children(media, budget, &root).await;
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
        deletes: &Media,
        cutoff: u32,
        unparsed: u64,
        symlinks: u64,
        report: &CdnGcReport,
        approved: Option<u64>,
    ) -> Result<Deleted> {
        let ceiling = self
            .max_candidates
            .unwrap_or(CEILING_BASE + report.referenced_objects / 10);
        metrics::gauge!("mkdb2_cdn_gc_delete_ceiling").set(ceiling as f64);
        // A dedup ack from before the log began is invisible, so the log must span the grace.
        let log_start = self.ch.cdn_ack_log_start().await?;
        let mut question = if self.asks {
            self.ch.cdn_gc_question().await?.map(|question| question.id)
        } else {
            None
        };
        let mut disarmed = disarmed(
            log_start.is_some_and(|start| start <= cutoff),
            unparsed,
            symlinks,
            question,
            approved,
            report.candidate_objects,
            ceiling,
        );
        if self.asks && matches!(disarmed, Some(Disarmed::Ceiling(_))) {
            // Unique per question (one per pass), so an answer to an earlier one can't take it, and past any approval still held, even after a clock rollback; milliseconds, so a JSON double (JavaScript, jq) holds it exactly.
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
            let id = now.max(approved.map_or(0, |id| id + 1));
            self.ch.cdn_gc_ask(id, ceiling).await?;
            (question, disarmed) = (Some(id), Some(Disarmed::Asked));
        }
        metrics::gauge!("mkdb2_cdn_gc_delete_armed").set(f64::from(u8::from(disarmed.is_none())));
        if disarmed.is_some() {
            tracing::warn!(
                reason = ?disarmed,
                question = ?question,
                unparsed_roots = unparsed,
                symlinks,
                ack_log_start = ?log_start,
                candidates = report.candidate_objects,
                ceiling,
                "CDN collector delete mode is disarmed this pass"
            );
            return Ok(Deleted {
                disarmed,
                ..Default::default()
            });
        }
        // Armed with a question standing means "Delete them" approved it: only its files go, and the rest wait for the next pass's ceiling. It's withdrawn even if the delete budget ran out first.
        let Some(question) = question else {
            return self.delete_candidates(deletes, cutoff, "candidate").await;
        };
        self.ch.cdn_gc_collect_approved(question).await?;
        let deleted = self.delete_candidates(deletes, cutoff, "approved").await?;
        self.ch.cdn_gc_withdraw_question().await?;
        Ok(deleted)
    }

    /// Deletes the scratch keys of `kind`: every candidate, or those an approved question named.
    async fn delete_candidates(&self, deletes: &Media, cutoff: u32, kind: &str) -> Result<Deleted> {
        let deadline = Instant::now() + DELETE_BUDGET;
        let mut deleted = Deleted::default();
        let mut after = String::new();
        while Instant::now() < deadline {
            let batch = self.ch.cdn_gc_page(kind, &after, DELETE_BATCH).await?;
            let Some(last) = batch.last() else {
                break;
            };
            after = last.key.clone();
            let offered = batch.len() as u64;
            let mut claim = self.fence.claim(batch);
            // Re-uploads between the candidate query and the claim.
            let keys: Vec<_> = claim.keys.iter().map(|c| c.key.clone()).collect();
            let reuploaded = tokio::time::timeout(RECHECK_TIMEOUT, self.reuploaded(&keys, cutoff))
                .await
                .context("rechecking CDN re-uploads timed out")??;
            let doomed: Vec<_> = claim
                .keys
                .iter()
                .filter(|candidate| !reuploaded.contains(&candidate.key))
                .map(|candidate| (candidate.key.clone(), candidate.size))
                .collect();
            deleted.spared += offered - doomed.len() as u64;
            claim.in_doubt = true;
            let mut outcomes = futures::stream::iter(doomed)
                .map(|(key, size)| async move {
                    let outcome = deletes.delete(&key).await;
                    (
                        size,
                        outcome.with_context(|| format!("deleting CDN key {key:?}")),
                    )
                })
                .buffer_unordered(DELETE_CONCURRENCY);
            let mut first_error = None;
            while let Some((size, outcome)) = outcomes.next().await {
                match outcome {
                    Ok(true) => {
                        deleted.objects += 1;
                        deleted.bytes += size;
                        metrics::counter!("mkdb2_cdn_gc_deleted_objects_total").increment(1);
                        metrics::counter!("mkdb2_cdn_gc_deleted_bytes_total").increment(size);
                    }
                    Ok(false) => deleted.already_gone += 1,
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

    /// Which of `keys` were re-uploaded since `cutoff`: acked in the log, or for the filesystem store re-marked. A file that fails to stat is spared; one already gone is left to the delete, which counts it.
    async fn reuploaded(&self, keys: &[String], cutoff: u32) -> Result<HashSet<String>> {
        let Media::Files(files) = &self.media else {
            return self.ch.cdn_gc_acked_since(keys, cutoff).await;
        };
        let (files, keys) = (files.clone(), keys.to_vec());
        Ok(tokio::task::spawn_blocking(move || {
            use std::os::unix::fs::MetadataExt;
            keys.into_iter()
                .filter(|key| match std::fs::symlink_metadata(files.path_for(key)) {
                    Ok(meta) => meta.ctime() >= i64::from(cutoff),
                    Err(error) => error.kind() != std::io::ErrorKind::NotFound,
                })
                .collect()
        })
        .await?)
    }
}

/// Why a delete-mode pass deletes nothing; `None` arms it. A standing question pauses every pass until it's answered, except that "Delete them" (`approved`) arms the next one for exactly its files, whatever the ceiling.
fn disarmed(
    acks_cover_grace: bool,
    unparsed: u64,
    symlinks: u64,
    question: Option<u64>,
    approved: Option<u64>,
    candidates: u64,
    ceiling: u64,
) -> Option<Disarmed> {
    if !acks_cover_grace {
        return Some(Disarmed::AckLog);
    }
    if unparsed > 0 {
        return Some(Disarmed::Unparsed);
    }
    if symlinks > 0 {
        return Some(Disarmed::Symlinks);
    }
    if question.is_some() {
        return (question != approved).then_some(Disarmed::Asked);
    }
    (candidates > ceiling).then_some(Disarmed::Ceiling(ceiling))
}

/// A panic fails the pass, not the loop.
async fn caught<T>(work: impl Future<Output = Result<T>>) -> Result<T> {
    std::panic::AssertUnwindSafe(work)
        .catch_unwind()
        .await
        .unwrap_or_else(|panic| {
            let message = (panic.downcast_ref::<&str>().copied())
                .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("(no message)");
            Err(anyhow::anyhow!("the pass panicked: {message}"))
        })
}

/// The notice that asks a local user `question`, standing since `since`.
fn notice(question: CdnGcQuestion, since: SystemTime) -> UserAlert {
    UserAlert {
        name: "MediaCleanupOverLimit".to_owned(),
        summary: format!("Garbage collection of media paused: wanted to clean up {} files ({}) that no run references, but the safety limit is {} files. This safety limit is to prevent a buggy runaway GC from deleting everything. \"Delete them\" removes these files permanently; there's no undo. \"Keep them\" protects them for 30 more days while cleanup continues for other media, then asks again if they're still over the limit. Until you answer, no media is cleaned up.", count(question.objects), size(question.bytes), count(question.ceiling)),
        class: None,
        active_at: rfc3339(since),
        question: Some(question.id),
    }
}

fn rfc3339(time: SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(time).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// A count for the notice bar, in thousands: 52,340.
fn count(n: u64) -> String {
    let digits = n.to_string();
    let mut grouped = String::new();
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

/// A size for the notice bar.
fn size(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.1} GB", bytes as f64 / 1e9)
    } else if bytes >= 1_000_000 {
        format!("{:.1} MB", bytes as f64 / 1e6)
    } else {
        format!("{:.1} KB", bytes as f64 / 1e3)
    }
}

/// Classes partition the store: foreign (outside the key grammar, never collected), referenced, candidate, and grace (unreferenced but recently created or re-uploaded). `None` (a failed pass) blanks them rather than leaving a previous pass's split beside fresh size gauges.
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
    media: &Media,
    budget: &Semaphore,
    root: &CdnGcKeySize,
) -> Result<Option<Vec<String>>> {
    let _permits = budget
        .acquire_many(root.size.min(FETCH_BUDGET.into()) as u32)
        .await?;
    let Some(bytes) = media.read(&root.key).await? else {
        return Ok(None);
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

/// `time` as whole seconds for ClickHouse binds (`deletion::unix_time_seconds` is the gauges' clock).
fn unix_seconds(time: SystemTime) -> u32 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(u32::MAX.into()) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdn::{content_key, hosted_key_pattern};
    use crate::clickhouse::CdnGcIndexComparison;
    use serde_json::json;

    use ::clickhouse::test::{handlers, status, Mock};

    fn key(fill: char, ext: &str) -> String {
        format!("{}.{ext}", fill.to_string().repeat(64))
    }

    fn candidate(key: &str) -> CdnGcKeySize {
        CdnGcKeySize {
            key: key.to_owned(),
            size: 1,
        }
    }

    /// A row of `project_id`'s run `r`: media naming `cdn_key`, or else a number.
    fn metric_row(
        project_id: &str,
        metric_name: &str,
        step: i64,
        cdn_key: Option<&str>,
    ) -> crate::clickhouse::MetricRow {
        crate::clickhouse::MetricRow {
            project_id: project_id.to_owned(),
            run_id: "r".to_owned(),
            metric_name: metric_name.to_owned(),
            tag: String::new(),
            step,
            timestamp_ms: 0,
            value: cdn_key.is_none().then_some(1.0),
            cdn_key: cdn_key.map(str::to_owned),
            text_data: None,
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
        let bucket = Media::Bucket(Arc::new(bucket));
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
        let files = FsStore::new(dir.path().to_owned());
        let path = files.path_for(&root);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        // A symlink to itself: opening it fails with ELOOP, even as root.
        std::os::unix::fs::symlink(&root, &path).unwrap();
        let budget = Semaphore::new(FETCH_BUDGET as usize);
        assert!(
            fetch_children(&Media::Files(files), &budget, &candidate(&root))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn an_upload_waits_for_a_claim_on_its_key() {
        let dir = tempfile::tempdir().unwrap();
        let store = CdnStore::Fs(FsStore::new(dir.path().to_owned()));
        let fence = Arc::new(Fence::default());
        let uploads = Uploads {
            fence: fence.clone(),
            acks: None,
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

    fn files_collector(root: &std::path::Path) -> Collector {
        Collector {
            mode: Mode::Delete,
            // Never contacted: these tests stay off ClickHouse.
            ch: Arc::new(ChClient::new("http://127.0.0.1:9").unwrap()),
            fence: Arc::default(),
            media: Media::Files(FsStore::new(root.to_owned())),
            deletes: None,
            max_candidates: None,
            asks: false,
            status: Arc::default(),
            activity: ActivityTracker::disabled(),
        }
    }

    /// A standing question, as the mock tests' ClickHouse returns it.
    const QUESTION: CdnGcQuestion = CdnGcQuestion {
        id: 7,
        objects: 3,
        bytes: 30,
        ceiling: 2,
    };

    /// A collector whose ClickHouse is `mock`. The mock serves its handlers in order, one per request whatever the request, and fails the test on a request past them or a handler left unused.
    fn mock_collector(mock: &Mock) -> Collector {
        let ch = Arc::new(ChClient::new(mock.url()).unwrap());
        Collector {
            status: Arc::new(Status::new(ch.clone())),
            ch,
            // A pass it starts lists nothing real.
            ..files_collector(std::path::Path::new("/nonexistent/kymo-cdn-test"))
        }
    }

    /// An `EXPLAIN json = 1, indexes = 1` plan in which `idx_cdn_key` kept `kept` of `considered` granules.
    fn plan(kept: u64, considered: u64) -> String {
        json!([{"Plan": {"Indexes": [{
            "Type": "Skip",
            "Name": "idx_cdn_key",
            "Initial Granules": considered,
            "Selected Granules": kept,
        }]}}])
        .to_string()
    }

    #[tokio::test]
    async fn an_unchecked_index_is_bypassed_then_checked() {
        let mock = Mock::new();
        mock.add(handlers::provide(vec!["25.3.14.14".to_owned()]));
        mock.add(handlers::provide(vec![false]));
        let read = mock.add(handlers::record_ddl());
        let indexed = mock.add(handlers::record_ddl());
        mock.add(handlers::provide(vec![CdnGcIndexComparison {
            roots: 1,
            missed: 0,
            sample: Vec::new(),
        }]));
        mock.add(handlers::provide(vec![plan(1, 3)]));
        let record = mock.add(handlers::record_ddl());
        mock_collector(&mock).collect_roots().await.unwrap();
        let read = read.query().await;
        assert!(
            read.contains("'ref'") && read.contains("use_skip_indexes = 0"),
            "{read}"
        );
        let indexed = indexed.query().await;
        assert!(
            indexed.contains("'indexed_ref'") && indexed.contains("use_skip_indexes = 1"),
            "{indexed}"
        );
        assert!(record.query().await.contains("cdn_gc_index_checks"));
    }

    #[tokio::test]
    async fn a_checked_index_is_read_through_and_not_checked_again() {
        let mock = Mock::new();
        mock.add(handlers::provide(vec!["25.3.14.14".to_owned()]));
        mock.add(handlers::provide(vec![true]));
        let read = mock.add(handlers::record_ddl());
        mock_collector(&mock).collect_roots().await.unwrap();
        let read = read.query().await;
        assert!(
            read.contains("'ref'") && read.contains("use_skip_indexes = 1"),
            "{read}"
        );
    }

    /// The check is advice: a fingerprint it can't read leaves the pass reading without the index, and a check that fails after the authoritative read leaves the pass's roots standing. Either way the pass goes on, and the mock fails the test on any further request.
    #[tokio::test]
    async fn a_failed_check_never_fails_the_pass() {
        let mock = Mock::new();
        mock.add(handlers::failure(status::INTERNAL_SERVER_ERROR));
        let read = mock.add(handlers::record_ddl());
        mock_collector(&mock).collect_roots().await.unwrap();
        assert!(read.query().await.contains("use_skip_indexes = 0"));

        let mock = Mock::new();
        mock.add(handlers::provide(vec!["25.3.14.14".to_owned()]));
        mock.add(handlers::provide(vec![false]));
        let read = mock.add(handlers::record_ddl());
        mock.add(handlers::failure(status::INTERNAL_SERVER_ERROR));
        mock_collector(&mock).collect_roots().await.unwrap();
        assert!(read.query().await.contains("use_skip_indexes = 0"));
    }

    /// An answer waits out a pass's turn, acts only on the question it names, and is recorded before it returns.
    #[tokio::test]
    async fn an_answer_waits_for_the_turn_and_names_its_question() {
        let standing = |id| UserAlert {
            name: "MediaCleanupOverLimit".to_owned(),
            summary: String::new(),
            class: None,
            active_at: String::new(),
            question: Some(id),
        };
        let mock = Mock::new();
        let status = mock_collector(&mock).status;
        status.publish(vec![standing(7)]);
        let turn = status.turn.lock().await;
        // The keep's time is read once the turn is taken, so waiting can't shorten its grace.
        let clocked = std::cell::Cell::new(false);
        let mut keep = std::pin::pin!(status.answer(7, Answer::Keep, || {
            clocked.set(true);
            SystemTime::now()
        }));
        let waiting = tokio::time::timeout(Duration::from_millis(50), keep.as_mut()).await;
        assert!(waiting.is_err() && !clocked.get());
        mock.add(handlers::provide(vec![QUESTION]));
        let acks = mock.add(handlers::record_ddl());
        let withdrawn = mock.add(handlers::record_ddl());
        drop(turn);
        assert!(keep.await.unwrap());
        let acks = acks.query().await;
        assert!(
            acks.starts_with("INSERT INTO mkdb2.cdn_acks")
                && acks.contains("mkdb2.cdn_gc_question"),
            "{acks}"
        );
        assert_eq!(
            withdrawn.query().await,
            "TRUNCATE TABLE mkdb2.cdn_gc_question"
        );
        assert!(status.alerts().is_empty());

        // Another question, or none, is refused untouched, and a question that no longer stands leaves the bar.
        status.publish(vec![standing(7)]);
        mock.add(handlers::provide(vec![QUESTION]));
        assert!(!status
            .answer(8, Answer::Delete, SystemTime::now)
            .await
            .unwrap());
        mock.add(handlers::provide(Vec::<CdnGcQuestion>::new()));
        assert!(!status
            .answer(7, Answer::Keep, SystemTime::now)
            .await
            .unwrap());
        assert!(status.alerts().is_empty());
        assert_eq!(*status.turn.lock().await, None);
        // "Delete them" writes nothing: it approves the question, drops it from the bar and wakes the collector. An approved question takes no other answer.
        status.publish(vec![standing(8), standing(7)]);
        mock.add(handlers::provide(vec![QUESTION]));
        let woken = status.wake.notified();
        assert!(status
            .answer(7, Answer::Delete, SystemTime::now)
            .await
            .unwrap());
        assert_eq!(*status.turn.lock().await, Some(7));
        assert_eq!(status.alerts()[0].question, Some(8));
        tokio::time::timeout(Duration::from_secs(1), woken)
            .await
            .unwrap();
        for answer in [Answer::Delete, Answer::Keep] {
            assert!(!status.answer(7, answer, SystemTime::now).await.unwrap());
        }
    }

    /// `start` arms deletion only in delete mode and logs dedup acks only for the bucket; local mode deletes, and shows a standing question before any pass.
    #[tokio::test]
    async fn start_wires_deletion_acks_and_the_question() {
        let dir = tempfile::tempdir().unwrap();
        let files = Media::Files(FsStore::new(dir.path().to_owned()));
        let bucket = Media::Bucket(Arc::new(object_store::memory::InMemory::new()));
        let config = |mode| Config {
            mode,
            deletes: None,
            max_candidates: None,
            asks: false,
        };
        for (config, media, deletes, acks, asks) in [
            (config(Mode::Report), &files, false, false, false),
            (config(Mode::Off), &files, false, false, false),
            (config(Mode::Delete), &files, true, false, false),
            (config(Mode::Report), &bucket, false, true, false),
            (Config::local(), &files, true, false, true),
        ] {
            let mock = Mock::new();
            mock.add(handlers::record_ddl()); // the ack log
            mock.add(handlers::provide(vec![1u32])); // its start row
            for _ in 0..4 {
                mock.add(handlers::record_ddl()); // the collector's other tables
            }
            if asks {
                // The mock fails the test if a handler goes unused.
                mock.add(handlers::provide(vec![QUESTION]));
            }
            let ch = Arc::new(ChClient::new(mock.url()).unwrap());
            let (uploads, collector) =
                start(config, media.clone(), ch, ActivityTracker::disabled())
                    .await
                    .unwrap();
            assert_eq!(
                (collector.deletes.is_some(), uploads.acks.is_some()),
                (deletes, acks)
            );
            let shown = collector
                .status
                .alerts()
                .iter()
                .map(|a| a.question)
                .collect::<Vec<_>>();
            assert_eq!(shown, Vec::from_iter(asks.then_some(Some(QUESTION.id))));
        }
    }

    /// Serves a pass's first half, its index check already recorded, and returns its last statement. The manifest parse gets `unparsed` as its one root, or none.
    fn serve_the_first_half(mock: &Mock, unparsed: Option<&str>) -> handlers::RecordDdlControl {
        mock.add(handlers::record_ddl()); // the scratch reset
        mock.add(handlers::provide(vec!["25.3.14.14".to_owned()])); // the root scan's fingerprint
        mock.add(handlers::provide(vec![true])); // an index check recorded
        mock.add(handlers::record_ddl()); // the root read
        mock.add(handlers::record_ddl()); // the unparsed roots
        mock.add(handlers::provide(vec![u64::from(unparsed.is_some())]));
        mock.add(handlers::provide(Vec::from_iter(unparsed.map(candidate))));
        if unparsed.is_some() {
            mock.add(handlers::provide(Vec::<CdnGcKeySize>::new())); // the next parse page
        }
        mock.add(handlers::record_ddl()) // the children
    }

    /// The pass's first half (listing, the root scan, the manifest parse) runs while an answer holds the turn, and the pass waits for it only at its candidate query, as work that blocks a local stack's idle stop. The mock serves exactly the first half's statements.
    #[tokio::test]
    async fn the_turn_starts_at_the_candidate_query() {
        let dir = tempfile::tempdir().unwrap();
        let mock = Mock::new();
        let activity = ActivityTracker::new_local();
        let collector = Collector {
            media: Media::Files(FsStore::new(dir.path().to_owned())),
            activity: activity.clone(),
            ..mock_collector(&mock)
        };
        let status = collector.status.clone();
        let turn = status.turn.lock().await;
        let children = serve_the_first_half(&mock, None);
        // An answer's wake permit ends the wait for the first pass.
        status.wake.notify_one();
        collector.spawn();
        tokio::time::timeout(Duration::from_secs(5), children.query())
            .await
            .expect("the first half ran while the turn was held");
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Still waiting: a pass that went on would have failed at the mock and ended its work.
        assert_eq!(activity.snapshot().in_flight_work, 1);
        drop(turn);
    }

    /// A whole pass hands deletion the grace, the gates and the answer: a filesystem store's cutoff is 31 days back; a held "Delete them" pages its question's keys (scratch kind `approved`, which the failed page names); and a symlink or a root the parse can't read disarms the pass before any page, even an approved one.
    #[tokio::test]
    async fn a_pass_judges_with_the_grace_the_gates_and_the_answer() {
        let now = 1_760_000_000;
        let cutoff: u32 = now - 31 * 24 * 3600;
        let root = key('c', "json");
        for case in ["approved", "symlink", "unreadable"] {
            let dir = tempfile::tempdir().unwrap();
            let files = FsStore::new(dir.path().to_owned());
            match case {
                "symlink" => std::os::unix::fs::symlink(dir.path(), dir.path().join("ee")).unwrap(),
                // A directory where the root's file belongs: the walk calls it foreign, and reading it fails.
                "unreadable" => std::fs::create_dir_all(files.path_for(&root)).unwrap(),
                _ => {}
            }
            let mock = Mock::new();
            let collector = Collector {
                asks: true,
                media: Media::Files(files.clone()),
                deletes: Some(Media::Files(files)),
                ..mock_collector(&mock)
            };
            *collector.status.turn.lock().await = Some(QUESTION.id);
            serve_the_first_half(&mock, (case == "unreadable").then_some(root.as_str()));
            let candidates = mock.add(handlers::record_ddl());
            mock.add(handlers::provide(vec![CdnGcReport::default()]));
            mock.add(handlers::provide(vec![cutoff])); // the ack log covers the grace
            mock.add(handlers::provide(vec![QUESTION]));
            let collected = (case == "approved").then(|| {
                let collected = mock.add(handlers::record_ddl());
                mock.add(handlers::failure(status::INTERNAL_SERVER_ERROR)); // its first page
                collected
            });
            mock.add(handlers::provide(vec![QUESTION])); // the bar's read
            let outcome = collector
                .pass(UNIX_EPOCH + Duration::from_secs(now.into()))
                .await;
            let candidates = candidates.query().await;
            assert!(candidates.contains(&cutoff.to_string()), "{candidates}");
            match collected {
                Some(collected) => {
                    assert!(collected.query().await.contains("WHERE id = 7"));
                    let error = format!("{:#}", outcome.err().unwrap());
                    assert!(error.contains("approved keys"), "{error}");
                }
                None => assert_eq!(
                    outcome.unwrap().deleted.disarmed,
                    Some(match case {
                        "symlink" => Disarmed::Symlinks,
                        _ => Disarmed::Unparsed,
                    })
                ),
            }
        }
    }

    /// The bar shows the stored question after every pass, a failed one too (here at the listing's first statement), keeps the one shown when the read fails, and never shows one "Delete them" approved.
    #[tokio::test]
    async fn a_pass_shows_the_stored_question_whatever_happened() {
        let mock = Mock::new();
        let collector = Collector {
            asks: true,
            ..mock_collector(&mock)
        };
        let shown = |collector: &Collector| {
            let alerts = collector.status.alerts();
            alerts
                .iter()
                .map(|a| (a.name.clone(), a.question))
                .collect::<Vec<_>>()
        };
        // The listing's first statement fails.
        mock.add(handlers::failure(status::INTERNAL_SERVER_ERROR));
        mock.add(handlers::provide(vec![QUESTION]));
        assert!(collector.pass(SystemTime::now()).await.is_err());
        assert_eq!(
            shown(&collector),
            [
                ("MediaCleanupFailed".to_owned(), None),
                ("MediaCleanupOverLimit".to_owned(), Some(7))
            ]
        );
        // A failed read leaves it shown.
        mock.add(handlers::failure(status::INTERNAL_SERVER_ERROR));
        mock.add(handlers::failure(status::INTERNAL_SERVER_ERROR));
        assert!(collector.pass(SystemTime::now()).await.is_err());
        assert_eq!(
            shown(&collector),
            [
                ("MediaCleanupFailed".to_owned(), None),
                ("MediaCleanupOverLimit".to_owned(), Some(7))
            ]
        );
        *collector.status.turn.lock().await = Some(7);
        mock.add(handlers::failure(status::INTERNAL_SERVER_ERROR));
        mock.add(handlers::provide(vec![QUESTION]));
        assert!(collector.pass(SystemTime::now()).await.is_err());
        assert_eq!(shown(&collector), [("MediaCleanupFailed".to_owned(), None)]);
    }

    /// A check records nothing unless the indexed scan kept every key of a non-empty root set and the index kept a granule and skipped another.
    #[tokio::test]
    async fn a_check_without_evidence_records_nothing() {
        for (roots, missed, kept) in [(0, 0, 1), (1, 0, 3), (1, 0, 0), (2, 1, 1)] {
            let mock = Mock::new();
            mock.add(handlers::record_ddl());
            mock.add(handlers::provide(vec![CdnGcIndexComparison {
                roots,
                missed,
                sample: Vec::new(),
            }]));
            if roots > 0 && missed == 0 {
                mock.add(handlers::provide(vec![plan(kept, 3)]));
            }
            let checked = mock_collector(&mock)
                .check_index("fingerprint")
                .await
                .unwrap();
            assert!(
                !checked,
                "{roots} roots, {missed} missed, {kept} of 3 granules kept"
            );
        }
    }

    #[test]
    fn conditions_keep_their_onset_while_they_hold() {
        let alert = |name: &str, since: &str| UserAlert {
            name: name.to_owned(),
            summary: String::new(),
            class: None,
            active_at: since.to_owned(),
            question: None,
        };
        let onsets = |status: &Status| {
            let alerts = status.alerts().into_iter();
            alerts
                .map(|a| format!("{}@{}", a.name, a.active_at))
                .collect::<Vec<_>>()
        };
        let status = Status::default();
        status.publish(vec![alert("A", "1")]);
        status.publish(vec![alert("A", "2"), alert("B", "2")]);
        assert_eq!(onsets(&status), ["A@1", "B@2"]);
        // A condition that cleared and returned is a new occurrence.
        status.publish(vec![alert("B", "3")]);
        status.publish(vec![alert("A", "4"), alert("B", "4")]);
        assert_eq!(onsets(&status), ["A@4", "B@2"]);
    }

    #[test]
    fn counts_group_thousands() {
        let counts = [0, 999, 1_000, 52_340, 1_234_567].map(count);
        assert_eq!(counts, ["0", "999", "1,000", "52,340", "1,234,567"]);
    }

    #[test]
    fn a_pass_over_the_ceiling_asks_a_local_user_and_tells_an_operator() {
        let dir = tempfile::tempdir().unwrap();
        let summary = |disarmed, missing_references| PassSummary {
            inventory: Inventory::default(),
            report: CdnGcReport {
                candidate_objects: 52_340,
                candidate_bytes: 8_100_000_000,
                ..Default::default()
            },
            unparsed: 0,
            missing_references,
            deleted: Deleted {
                disarmed,
                ..Default::default()
            },
        };
        let now = UNIX_EPOCH + Duration::from_secs(1_759_500_000);
        let shown = |alerts: Vec<UserAlert>| {
            let alerts = alerts.into_iter();
            alerts
                .map(|a| format!("{}:{:?}", a.name, a.question))
                .collect::<Vec<_>>()
        };
        let collector = files_collector(dir.path());
        // The question shows the numbers it was asked with; the wire names are what the notice bar reads.
        let asked = notice(
            CdnGcQuestion {
                id: 7,
                objects: 51_000,
                bytes: 2_500_000,
                ceiling: 10_000,
            },
            now,
        );
        let json = serde_json::to_value(&asked).unwrap();
        assert_eq!(
            (&json["name"], &json["active_at"], &json["question"]),
            (
                &json!("MediaCleanupOverLimit"),
                &json!("2025-10-03T14:00:00Z"),
                &json!(7)
            )
        );
        assert_eq!(
            asked.summary,
            "Garbage collection of media paused: wanted to clean up 51,000 files (2.5 MB) that no run references, but the safety limit is 10,000 files. This safety limit is to prevent a buggy runaway GC from deleting everything. \"Delete them\" removes these files permanently; there's no undo. \"Keep them\" protects them for 30 more days while cleanup continues for other media, then asks again if they're still over the limit. Until you answer, no media is cleaned up."
        );
        let told = collector.conditions(&Ok(summary(Some(Disarmed::Ceiling(10_000)), 0)), now);
        assert_eq!(
            told[0].summary,
            "Garbage collection of media paused: wanted to clean up 52,340 files (8.1 GB), but the safety limit is 10,000 files. This safety limit is to prevent a buggy runaway GC from deleting everything. If deleting these files is expected, set KYMO_CDN_GC_MAX_CANDIDATES to at least 52340."
        );
        assert_eq!(told[0].question, None);
        // A standing question and the wait for the ack log are no conditions of the pass (`pass` shows the question); missing media, unreadable manifests and a failed pass are, and a failed pass leaves the others standing.
        for disarmed in [Disarmed::Asked, Disarmed::AckLog] {
            assert!(collector
                .conditions(&Ok(summary(Some(disarmed), 0)), now)
                .is_empty());
        }
        // Unreadable manifests and symlinks each show whatever paused the pass first, the ack log's warmup included; report mode pauses nothing.
        let paused = Ok(PassSummary {
            unparsed: 4,
            inventory: Inventory {
                symlinks: 2,
                ..Default::default()
            },
            ..summary(Some(Disarmed::AckLog), 3)
        });
        assert_eq!(
            shown(collector.conditions(&paused, now)),
            [
                "MediaMissing:None",
                "MediaCleanupUnreadable:None",
                "MediaCleanupSymlinks:None"
            ]
        );
        let reporting = Collector {
            mode: Mode::Report,
            ..files_collector(dir.path())
        };
        assert_eq!(
            shown(reporting.conditions(&paused, now)),
            ["MediaMissing:None"]
        );
        collector.status.publish(vec![asked]);
        assert_eq!(
            shown(collector.conditions(&Err(anyhow::anyhow!("listing")), now)),
            ["MediaCleanupOverLimit:Some(7)", "MediaCleanupFailed:None"]
        );
    }

    #[test]
    fn a_standing_question_pauses_until_answered() {
        let question = Some(7);
        assert_eq!(
            disarmed(false, 1, 1, question, Some(7), 30, 10),
            Some(Disarmed::AckLog)
        );
        assert_eq!(
            disarmed(true, 1, 1, question, Some(7), 30, 10),
            Some(Disarmed::Unparsed)
        );
        assert_eq!(
            disarmed(true, 0, 2, question, Some(7), 30, 10),
            Some(Disarmed::Symlinks)
        );
        // It pauses even a pass that fits the ceiling, until "Delete them" arms one for its files, over the ceiling too.
        for approved in [None, Some(8)] {
            assert_eq!(
                disarmed(true, 0, 0, question, approved, 0, 10),
                Some(Disarmed::Asked)
            );
        }
        assert_eq!(disarmed(true, 0, 0, question, Some(7), 30, 10), None);
        // Without one the ceiling decides, whatever was approved before.
        assert_eq!(disarmed(true, 0, 0, None, None, 10, 10), None);
        assert_eq!(
            disarmed(true, 0, 0, None, Some(7), 11, 10),
            Some(Disarmed::Ceiling(10))
        );
    }

    /// The arming rule and the ceiling, through the gate deletion takes, since CI runs no live test, and only the ceiling asks. The mock fails the test on any request it wasn't given, an ask included.
    #[tokio::test]
    async fn deletion_waits_for_the_ack_log_and_stops_at_the_ceiling() {
        let mock = Mock::new();
        let collector = mock_collector(&mock);
        let cutoff = 1_759_500_000;
        // 10,000 plus a tenth of 1,000 referenced.
        let report = |candidates| CdnGcReport {
            referenced_objects: 1_000,
            candidate_objects: candidates,
            ..Default::default()
        };
        for (log_start, candidates, expected) in [
            (None, 1, Some(Disarmed::AckLog)),
            (Some(cutoff + 1), 1, Some(Disarmed::AckLog)),
            (Some(cutoff), 10_101, Some(Disarmed::Ceiling(10_100))),
            (Some(cutoff), 10_100, None),
        ] {
            mock.add(handlers::provide(
                log_start.into_iter().collect::<Vec<u32>>(),
            ));
            if expected.is_none() {
                // An armed pass's first candidate page, empty.
                mock.add(handlers::provide(Vec::<CdnGcKeySize>::new()));
            }
            let deleted = collector
                .delete_if_safe(&collector.media, cutoff, 0, 0, &report(candidates), None)
                .await
                .unwrap();
            assert_eq!(deleted.disarmed, expected, "{log_start:?}, {candidates}");
        }
        // A collector that asks does so only over the ceiling: the warmup, the gates and a standing question ask nothing.
        let asking = Collector {
            asks: true,
            ..mock_collector(&mock)
        };
        for (log_start, unparsed, symlinks, question, expected) in [
            (cutoff + 1, 0, 0, None, Disarmed::AckLog),
            (cutoff, 1, 0, None, Disarmed::Unparsed),
            (cutoff, 0, 1, None, Disarmed::Symlinks),
            (cutoff, 0, 0, Some(QUESTION), Disarmed::Asked),
        ] {
            mock.add(handlers::provide(vec![log_start]));
            mock.add(handlers::provide(Vec::from_iter(question)));
            let deleted = asking
                .delete_if_safe(
                    &asking.media,
                    cutoff,
                    unparsed,
                    symlinks,
                    &report(10_101),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(deleted.disarmed, Some(expected));
        }
    }

    /// "Delete them" arms a pass over the ceiling for exactly its question's candidates, collected as scratch kind `approved` (which `a_pass_judges_with_the_grace_the_gates_and_the_answer` shows it pages), then withdraws it; a later question's id passes any approval still held, even one from a clock now set back.
    #[tokio::test]
    async fn an_approved_question_deletes_only_its_candidates() {
        let mock = Mock::new();
        let collector = Collector {
            asks: true,
            ..mock_collector(&mock)
        };
        let cutoff = 1_759_500_000;
        let over = CdnGcReport {
            candidate_objects: 1_000_000,
            ..Default::default()
        };
        mock.add(handlers::provide(vec![cutoff]));
        mock.add(handlers::provide(vec![QUESTION]));
        let collected = mock.add(handlers::record_ddl());
        mock.add(handlers::provide(Vec::<CdnGcKeySize>::new()));
        let withdrawn = mock.add(handlers::record_ddl());
        let deleted = collector
            .delete_if_safe(&collector.media, cutoff, 0, 0, &over, Some(QUESTION.id))
            .await
            .unwrap();
        assert_eq!(deleted.disarmed, None);
        let collected = collected.query().await;
        assert!(
            collected.contains("'approved'")
                && collected.contains("kind = 'candidate'")
                && collected.contains("WHERE id = 7"),
            "{collected}"
        );
        assert!(withdrawn.query().await.starts_with("TRUNCATE"));

        let held = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 3_600_000;
        mock.add(handlers::provide(vec![cutoff]));
        mock.add(handlers::provide(Vec::<CdnGcQuestion>::new()));
        let asked = mock.add(handlers::record_ddl());
        let deleted = collector
            .delete_if_safe(&collector.media, cutoff, 0, 0, &over, Some(held))
            .await
            .unwrap();
        assert_eq!(deleted.disarmed, Some(Disarmed::Asked));
        let asked = asked.query().await;
        assert!(asked.contains(&format!("SELECT {}", held + 1)), "{asked}");
    }

    /// Deletion rechecks each filesystem candidate under its claim: one marked since the cutoff, as a re-upload after the candidate query would mark it, survives, as does one it can't stat; a stale one goes. A failed delete leaves its key fenced for the settle time, which a GCS DELETE needs, since one can still land. The mock serves the candidate pages and fails the test on any other request, an ack-log recheck included.
    #[tokio::test]
    async fn deletion_rechecks_each_candidate_under_its_claim() {
        let dir = tempfile::tempdir().unwrap();
        let mock = Mock::new();
        let collector = Collector {
            media: Media::Files(FsStore::new(dir.path().to_owned())),
            ..mock_collector(&mock)
        };
        let Media::Files(files) = &collector.media else {
            unreachable!()
        };
        let file = key('a', "png");
        CdnStore::Fs(files.clone())
            .put_if_absent(&file, Bytes::from_static(b"x"))
            .await
            .unwrap();
        // A file where its first shard belongs: the stat fails, even as root, with ENOTDIR.
        let unstatable = key('b', "png");
        std::fs::write(dir.path().join("bb"), b"").unwrap();
        let now = unix_seconds(SystemTime::now());
        for (cutoff, deleted) in [(now - 10, 0), (now + 10, 1)] {
            mock.add(handlers::provide(vec![
                candidate(&file),
                candidate(&unstatable),
            ]));
            mock.add(handlers::provide(Vec::<CdnGcKeySize>::new()));
            let outcome = collector
                .delete_candidates(&collector.media, cutoff, "candidate")
                .await
                .unwrap();
            assert_eq!(
                (outcome.objects, outcome.spared),
                (deleted, 2 - deleted),
                "cutoff {cutoff}"
            );
            assert_eq!(collector.media.exists(&file).await.unwrap(), deleted == 0);
        }
        // A directory where the file belongs: the unlink fails without NotFound.
        let stuck = key('d', "png");
        std::fs::create_dir_all(files.path_for(&stuck)).unwrap();
        mock.add(handlers::provide(vec![candidate(&stuck)]));
        assert!(collector
            .delete_candidates(&collector.media, now + 10, "candidate")
            .await
            .is_err());
        assert!(collector
            .fence
            .claim(vec![candidate(&stuck)])
            .keys
            .is_empty());
    }

    /// The bucket's guards, which the filesystem tests can't reach: its listing never nominates a name outside the key grammar; a dedup upload logs its ack before it returns, and a failed log fails it; deletion rechecks the ack log under each claim, sparing a key acked since the candidate query; and a DELETE that fails without a 404 leaves its key fenced.
    #[tokio::test]
    async fn the_bucket_logs_dedup_acks_and_rechecks_them_under_the_claim() {
        let mock = Mock::new();
        let bucket = Arc::new(object_store::memory::InMemory::new());
        let collector = Collector {
            media: Media::Bucket(bucket.clone()),
            ..mock_collector(&mock)
        };
        let (acked, stale) = (key('a', "png"), key('b', "png"));
        for name in [acked.as_str(), stale.as_str(), "probe.txt"] {
            bucket
                .put(&ObjectPath::from(name), "x".into())
                .await
                .unwrap();
        }
        let listed: Vec<_> = collector.listing().try_collect().await.unwrap();
        let foreign = listed
            .iter()
            .filter(|listed| matches!(listed, Listed::Foreign { .. }))
            .count();
        assert_eq!((listed.len(), foreign), (3, 1));

        let dir = tempfile::tempdir().unwrap();
        let store = CdnStore::Fs(FsStore::new(dir.path().to_owned()));
        let uploads = Uploads {
            fence: collector.fence.clone(),
            acks: Some(collector.ch.clone()),
        };
        let uploaded = key('e', "png");
        let put = || uploads.put(&store, &uploaded, Bytes::from_static(b"e"));
        assert_eq!(put().await.unwrap(), PutOutcome::Created);
        mock.add(handlers::failure(status::INTERNAL_SERVER_ERROR));
        assert!(put().await.is_err());

        mock.add(handlers::provide(vec![
            candidate(&acked),
            candidate(&stale),
        ]));
        mock.add(handlers::provide(vec![acked.clone()]));
        mock.add(handlers::provide(Vec::<CdnGcKeySize>::new()));
        let deleted = collector
            .delete_candidates(&collector.media, 0, "candidate")
            .await
            .unwrap();
        assert_eq!((deleted.objects, deleted.spared), (1, 1));
        assert!(collector.media.exists(&acked).await.unwrap());
        assert!(!collector.media.exists(&stale).await.unwrap());

        // Removing a directory fails without NotFound.
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join(&stale)).unwrap();
        let refusing =
            object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap();
        mock.add(handlers::provide(vec![candidate(&stale)]));
        mock.add(handlers::provide(Vec::<String>::new()));
        assert!(collector
            .delete_candidates(&Media::Bucket(Arc::new(refusing)), 0, "candidate")
            .await
            .is_err());
        assert!(collector
            .fence
            .claim(vec![candidate(&stale)])
            .keys
            .is_empty());
    }

    /// Only a canonical key is present, as only one is served (`FsStore::get`): another spelling of it misses even on a case-insensitive volume.
    #[tokio::test]
    async fn the_filesystem_store_holds_only_canonical_keys() {
        let dir = tempfile::tempdir().unwrap();
        let collector = files_collector(dir.path());
        let key = format!("{}.png", "ab".repeat(32));
        let Media::Files(files) = &collector.media else {
            unreachable!()
        };
        let path = files.path_for(&key);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "body").unwrap();
        assert!(collector.media.exists(&key).await.unwrap());
        // On a case-sensitive volume, a file of its own under the other spelling.
        let other = key.to_uppercase();
        let path = dir.path().join(&other[..2]).join(&other[2..4]).join(&other);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "body").unwrap();
        assert!(!collector.media.exists(&other).await.unwrap());
    }

    #[tokio::test]
    async fn the_filesystem_store_is_listed_rechecked_and_deleted_by_its_fanout() {
        let dir = tempfile::tempdir().unwrap();
        let collector = files_collector(dir.path());
        let Media::Files(files) = &collector.media else {
            unreachable!()
        };
        // A file's ctime trails its last re-upload by up to the mark interval.
        assert_eq!(collector.grace(), GRACE + MARK_INTERVAL);
        let (a, b) = (key('a', "png"), key('b', "png"));
        for key in [&a, &b] {
            CdnStore::Fs(files.clone())
                .put_if_absent(key, Bytes::from_static(b"x"))
                .await
                .unwrap();
        }
        std::fs::write(dir.path().join("stray"), "x").unwrap();
        // Dated by ctime, which a copy or restore keeping mtime (`rsync -a`, `cp -p`) can't set back: a file whose mtime says 60 days is still new.
        let old = SystemTime::now() - Duration::from_secs(60 * 24 * 3600);
        std::fs::File::options()
            .write(true)
            .open(files.path_for(&a))
            .unwrap()
            .set_times(
                std::fs::FileTimes::new()
                    .set_modified(old)
                    .set_accessed(old),
            )
            .unwrap();
        let now = unix_seconds(SystemTime::now());
        let listed: Vec<_> = collector.listing().try_collect().await.unwrap();
        let mut objects: Vec<_> = listed
            .iter()
            .filter_map(|listed| match listed {
                Listed::Object { key, created, .. } => {
                    assert!(*created >= now - 10, "{key} dated by mtime");
                    Some(key.clone())
                }
                _ => None,
            })
            .collect();
        objects.sort();
        assert_eq!((objects, listed.len()), (vec![a.clone(), b.clone()], 3));

        // A cutoff past both files' ctime nominates them; one before it spares them, as a re-upload's mark would (`a` too, whatever its mtime says). A gone file is left to the delete, which counts it.
        let keys = [a.clone(), b.clone(), key('c', "png")];
        assert!(collector
            .reuploaded(&keys, now + 10)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            collector.reuploaded(&keys, now - 10).await.unwrap(),
            HashSet::from([a.clone(), b.clone()])
        );
        assert!(collector.media.delete(&a).await.unwrap());
        assert!(!collector.media.delete(&a).await.unwrap());
        assert!(!collector.media.exists(&a).await.unwrap());
        assert!(collector.media.exists(&b).await.unwrap());
    }

    #[tokio::test]
    async fn a_walk_that_fails_fails_the_listing() {
        let dir = tempfile::tempdir().unwrap();
        let collector = files_collector(&dir.path().join("absent"));
        let listed: Result<Vec<_>> = collector.listing().try_collect().await;
        assert!(listed.is_err());
        // A shard it can't read could hold a manifest, whose children would then look unreferenced.
        use std::os::unix::fs::PermissionsExt;
        let shard = dir.path().join("ab");
        std::fs::create_dir(&shard).unwrap();
        std::fs::set_permissions(&shard, std::fs::Permissions::from_mode(0o000)).unwrap();
        let unreadable = std::fs::read_dir(&shard).is_err();
        let collector = files_collector(dir.path());
        let listed: Result<Vec<_>> = collector.listing().try_collect().await;
        std::fs::set_permissions(&shard, std::fs::Permissions::from_mode(0o700)).unwrap();
        // Root reads it anyway, so it can't test this.
        if unreadable {
            assert!(listed.is_err());
        } else {
            eprintln!("skipped the unreadable shard: this user reads it anyway");
        }
    }

    /// An answer ends the wait for the next pass even when nothing waits yet, here before the collector starts, as a standing question shows from boot.
    #[tokio::test]
    async fn an_answer_ends_the_wait_for_the_first_pass() {
        let mock = Mock::new();
        let collector = mock_collector(&mock);
        let status = collector.status.clone();
        mock.add(handlers::provide(vec![QUESTION]));
        assert!(status
            .answer(7, Answer::Delete, SystemTime::now)
            .await
            .unwrap());
        collector.spawn();
        // The first pass's first statement, the listing's scratch reset, long before `FIRST_PASS_DELAY`.
        let reset = mock.add(handlers::record_ddl());
        let reset = tokio::time::timeout(Duration::from_secs(5), reset.query())
            .await
            .expect("an answer starts the first pass");
        assert!(
            reset.starts_with("TRUNCATE TABLE mkdb2.cdn_gc_scratch"),
            "{reset}"
        );
    }

    /// The collector's root filter accepts exactly the route's key grammar.
    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_CLICKHOUSE_URL"]
    async fn live_key_pattern_is_the_route_grammar() -> Result<()> {
        let _suite_guard = crate::pg::live_database_suite_gate().lock().await;
        let url = crate::pg::live_test_url("KYMO_LIVE_TEST_CLICKHOUSE_URL")?;
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

    /// Deletion safety under `idx_cdn_key` on this ClickHouse version: the unindexed root read really reads every granule, the Nullable minmax index prunes an all-NULL granule, and the pass's own check finds every root an unindexed scan read, records its fingerprint (which needs a pruned granule), and fails on a root the indexed scan can't see.
    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_CLICKHOUSE_URL"]
    async fn live_root_scan_uses_the_cdn_key_index() -> Result<()> {
        let _suite_guard = crate::pg::live_database_suite_gate().lock().await;
        let url = crate::pg::live_test_url("KYMO_LIVE_TEST_CLICKHOUSE_URL")?;
        let ch = Arc::new(ChClient::new(&url)?);
        ch.ensure_schema().await?;
        let run = crate::pg::unique_suffix();
        let project_id = format!("cdn key index live {run}");
        // One sync insert is one part in its own partition. The 16,384 NULL-key rows sort before the keyed row, so granule 0 is all NULL whichever granule the last row joins.
        let mut rows: Vec<_> = (0..16_384)
            .map(|step| metric_row(&project_id, "a/numeric", step, None))
            .collect();
        rows.push(metric_row(
            &project_id,
            "b/gallery",
            0,
            Some(&content_key(run.as_bytes(), "png")),
        ));
        ch.insert_batch(&rows, Duration::from_secs(30), true)
            .await?;

        // The fingerprint reads the index and the key column it covers.
        let fingerprint = ch.cdn_gc_index_fingerprint().await?;
        for fact in [
            "metrics.idx_cdn_key minmax cdn_key 1",
            "metrics.cdn_key Nullable(String)",
            "rich_metrics.cdn_key String",
        ] {
            assert!(
                fingerprint.lines().any(|line| line == fact),
                "{fingerprint}"
            );
        }

        // Through the collector: an unchecked fingerprint is checked and recorded; a recorded one is read with the index and not checked again.
        let (_, collector) = start(
            Config {
                mode: Mode::Report,
                deletes: None,
                max_candidates: None,
                asks: false,
            },
            Media::Bucket(Arc::new(object_store::memory::InMemory::new())),
            ch.clone(),
            ActivityTracker::disabled(),
        )
        .await?;
        // The statement's trailing SETTINGS reaches the insert's SELECT: the unindexed read selects every mark, the indexed one skips the all-NULL granule.
        let selected_marks = || async {
            ch.test_client()
                .query("SELECT value FROM system.events WHERE event = 'SelectedMarks'")
                .fetch_optional::<u64>()
                .await
                .map(Option::unwrap_or_default)
        };
        ch.cdn_gc_reset().await?;
        let before = selected_marks().await?;
        ch.cdn_gc_collect_roots("ref", false).await?;
        let unindexed = selected_marks().await? - before;
        ch.cdn_gc_collect_roots("indexed_ref", true).await?;
        let indexed = selected_marks().await? - before - unindexed;
        assert!(
            unindexed > indexed,
            "{unindexed} marks unindexed, {indexed} indexed"
        );
        ch.test_client()
            .query("TRUNCATE TABLE mkdb2.cdn_gc_index_checks")
            .execute()
            .await?;
        for _ in 0..2 {
            ch.cdn_gc_reset().await?;
            collector.collect_roots().await?;
            assert!(ch.cdn_gc_count("ref").await? >= 1);
            let records = ch
                .test_client()
                .query("SELECT count() FROM mkdb2.cdn_gc_index_checks WHERE fingerprint = ?")
                .bind(&fingerprint)
                .fetch_one::<u64>()
                .await?;
            assert_eq!(records, 1);
        }
        // A record counts for `INDEX_CHECK_DAYS`.
        for (age_days, counts) in [(6, true), (8, false)] {
            ch.test_client()
                .query("TRUNCATE TABLE mkdb2.cdn_gc_index_checks")
                .execute()
                .await?;
            ch.test_client()
                .query(&format!(
                    "INSERT INTO mkdb2.cdn_gc_index_checks VALUES (?, now() - INTERVAL {age_days} DAY)"
                ))
                .bind(&fingerprint)
                .execute()
                .await?;
            assert_eq!(
                ch.cdn_gc_index_checked(&fingerprint).await?,
                counts,
                "{age_days} days"
            );
        }

        // A root inserted after the unindexed read is the indexed scan's alone, so it can't fail the check.
        ch.cdn_gc_reset().await?;
        ch.cdn_gc_collect_roots("ref", false).await?;
        ch.insert_rich_mutation(
            &crate::clickhouse::RichMetricRow {
                project_id: project_id.clone(),
                run_id: "r".to_owned(),
                metric_name: "c/resource".to_owned(),
                tag: String::new(),
                step: 0,
                timestamp_ms: 0,
                cdn_key: content_key(format!("late {run}").as_bytes(), "png"),
                mutation_version: 0,
            },
            Duration::from_secs(30),
        )
        .await?;
        ch.cdn_gc_collect_roots("indexed_ref", true).await?;
        assert_eq!(
            ch.cdn_gc_count("indexed_ref").await?,
            ch.cdn_gc_count("ref").await? + 1
        );
        let comparison = ch.cdn_gc_index_comparison().await?;
        assert!(
            comparison.roots >= 1 && comparison.missed == 0,
            "{comparison:?}"
        );
        // A root the indexed scan doesn't return, as if pruned, fails it, and the collector records nothing.
        ch.cdn_gc_reset().await?;
        let pruned = content_key(format!("pruned {run}").as_bytes(), "png");
        ch.cdn_gc_insert_inventory(&[CdnInventoryRow {
            kind: "ref",
            key: pruned.clone(),
            size: 0,
            created: 0,
        }])
        .await?;
        ch.test_client()
            .query("TRUNCATE TABLE mkdb2.cdn_gc_index_checks")
            .execute()
            .await?;
        collector.collect_roots().await?;
        let comparison = ch.cdn_gc_index_comparison().await?;
        assert_eq!((comparison.missed, comparison.sample), (1, vec![pruned]));
        assert!(!ch.cdn_gc_index_checked(&fingerprint).await?);
        ch.delete_live_project(&project_id).await?;
        Ok(())
    }

    /// Delete-mode passes inside and past the grace, the guards between classification and a DELETE, and an upload's dedup ack, against an in-memory bucket and a throwaway ClickHouse (docs/live-database-tests.md).
    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_CLICKHOUSE_URL"]
    async fn live_pass_deletes_only_unreachable_objects_past_the_grace() -> Result<()> {
        use object_store::memory::InMemory;
        let _suite_guard = crate::pg::live_database_suite_gate().lock().await;
        let url = crate::pg::live_test_url("KYMO_LIVE_TEST_CLICKHOUSE_URL")?;
        let ch = Arc::new(ChClient::new(&url)?);
        ch.ensure_schema().await?;

        // Unique content per run: keys are content addresses, and the ack log and the children cache outlive the run.
        let run = crate::pg::unique_suffix();
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
        let row = |metric_name, cdn_key| metric_row(&project_id, metric_name, 0, Some(cdn_key));
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
            asks: false,
        };
        let (uploads, collector) = start(
            config,
            Media::Bucket(bucket.clone()),
            ch.clone(),
            ActivityTracker::disabled(),
        )
        .await?;

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
        collector.list().await?;
        ch.cdn_gc_collect_candidates(cutoff).await?;
        let report = ch.cdn_gc_report().await?;
        let log_start = ch.cdn_ack_log_start().await?.context("no ack log")?;
        // 10,000 plus a tenth of 1,000 referenced.
        let over_ceiling = CdnGcReport {
            referenced_objects: 1_000,
            candidate_objects: CEILING_BASE + 101,
            ..Default::default()
        };
        for (cutoff, unparsed, report, reason) in [
            (cutoff, 1, &report, Disarmed::Unparsed),
            (log_start - 1, 0, &report, Disarmed::AckLog),
            (cutoff, 0, &over_ceiling, Disarmed::Ceiling(10_100)),
        ] {
            let deleted = collector
                .delete_if_safe(
                    &Media::Bucket(bucket.clone()),
                    cutoff,
                    unparsed,
                    0,
                    report,
                    None,
                )
                .await?;
            assert_eq!(
                (deleted.disarmed, deleted.objects),
                (Some(reason), 0),
                "armed at {cutoff} with {unparsed} unparsed and {} candidates",
                report.candidate_objects
            );
        }
        // After the candidate query, `late_acked` is acked and `uploading` held: both are spared, and only `acked` goes.
        ch.record_cdn_ack(&late_acked, unix_seconds(latest)).await?;
        let hold = collector.fence.begin_upload(&uploading).await;
        let deleted = collector
            .delete_candidates(&Media::Bucket(bucket.clone()), cutoff, "candidate")
            .await?;
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
            .delete_candidates(&Media::Bucket(Arc::new(refusing)), cutoff, "candidate")
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
        let store = CdnStore::Fs(FsStore::new(root.path().to_owned()));
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

        ch.delete_live_project(&project_id).await?;
        Ok(())
    }

    /// A filesystem-store pass against a throwaway ClickHouse: past the grace only unreachable files go, a symlink pauses deletion, and a pass over the ceiling asks the local user and deletes nothing until the answer.
    #[tokio::test]
    #[ignore = "requires KYMO_LIVE_TEST_CLICKHOUSE_URL"]
    async fn live_filesystem_pass_deletes_past_the_grace_and_asks_over_the_ceiling() -> Result<()> {
        let _suite_guard = crate::pg::live_database_suite_gate().lock().await;
        let url = crate::pg::live_test_url("KYMO_LIVE_TEST_CLICKHOUSE_URL")?;
        let ch = Arc::new(ChClient::new(&url)?);
        ch.ensure_schema().await?;

        let run = crate::pg::unique_suffix();
        let dir = tempfile::tempdir()?;
        let files = FsStore::new(dir.path().join("cdn"));
        let store = CdnStore::Fs(files.clone());
        let put = |body: String, ext: &str| {
            let key = content_key(body.as_bytes(), ext);
            let store = &store;
            async move { store.put_if_absent(&key, body.into()).await.map(|_| key) }
        };
        let child = put(format!("child {run}"), "png").await?;
        let gallery = put(
            json!({"items": [{"resource": child}], "run": run}).to_string(),
            "json",
        )
        .await?;
        let orphan = put(format!("orphan {run}"), "png").await?;

        let project_id = format!("cdn gc filesystem live {run}");
        ch.insert_batch(
            &[metric_row(&project_id, "gallery", 0, Some(&gallery))],
            Duration::from_secs(30),
            true,
        )
        .await?;

        let (_, mut collector) = start(
            Config::local(),
            Media::Files(files.clone()),
            ch.clone(),
            ActivityTracker::disabled(),
        )
        .await?;
        // A question an earlier run left standing would pause this one.
        ch.cdn_gc_withdraw_question().await?;
        // Past the grace and the mark interval, from now and so from any ack-log start.
        let later = SystemTime::now() + GRACE + MARK_INTERVAL + Duration::from_secs(3600);
        let summary = collector.pass(later).await?;
        assert_eq!(
            (summary.deleted.disarmed, summary.deleted.objects),
            (None, 1)
        );
        assert!(!collector.media.exists(&orphan).await?);
        for kept in [&gallery, &child] {
            assert!(collector.media.exists(kept).await?, "{kept}");
        }

        // Over the ceiling a pass asks, and until an answer nothing goes: not after symlinks clear, not under a ceiling that grew, not after a restart.
        let link = dir.path().join("cdn").join("ee");
        std::os::unix::fs::symlink(dir.path(), &link)?;
        collector.max_candidates = Some(1);
        let orphans = [
            put(format!("orphan 1 {run}"), "png").await?,
            put(format!("orphan 2 {run}"), "png").await?,
        ];
        let summary = collector.pass(later).await?;
        assert_eq!(summary.deleted.disarmed, Some(Disarmed::Symlinks));
        std::fs::remove_file(&link)?;
        let summary = collector.pass(later).await?;
        assert_eq!(summary.deleted.disarmed, Some(Disarmed::Asked));
        let question = ch.cdn_gc_question().await?.context("asked")?;
        assert_eq!((question.objects, question.ceiling), (2, 1));
        let shown = |status: &Status| {
            let alerts = status.alerts();
            let asked = alerts.iter().filter_map(|a| a.question).collect::<Vec<_>>();
            asked
        };
        assert_eq!(shown(&collector.status), [question.id]);
        collector.max_candidates = None;
        let restart = || {
            start(
                Config::local(),
                Media::Files(files.clone()),
                ch.clone(),
                ActivityTracker::disabled(),
            )
        };
        let (_, collector) = restart().await?;
        assert_eq!(shown(&collector.status), [question.id]);
        let summary = collector.pass(later).await?;
        assert_eq!(summary.deleted.disarmed, Some(Disarmed::Asked));
        assert_eq!(shown(&collector.status), [question.id]);
        for answer in [Answer::Delete, Answer::Keep] {
            assert!(
                !collector
                    .status
                    .answer(question.id + 1, answer, || later)
                    .await?
            );
        }

        // "Delete them" deletes exactly its files, whatever the ceiling; a file that became eligible since waits for the next pass, which judges it by the ceiling. The answer lives in memory, so a restart before its pass asks again.
        let newcomer = put(format!("eligible after the question {run}"), "png").await?;
        assert!(
            collector
                .status
                .answer(question.id, Answer::Delete, || later)
                .await?
        );
        assert!(shown(&collector.status).is_empty());
        assert!(
            !collector
                .status
                .answer(question.id, Answer::Keep, || later)
                .await?
        );
        let (_, mut collector) = restart().await?;
        collector.max_candidates = Some(1);
        assert_eq!(shown(&collector.status), [question.id]);
        assert!(
            collector
                .status
                .answer(question.id, Answer::Delete, || later)
                .await?
        );
        let summary = collector.pass(later).await?;
        assert_eq!(
            (
                summary.report.candidate_objects,
                summary.deleted.disarmed,
                summary.deleted.objects
            ),
            (3, None, 2)
        );
        for orphan in &orphans {
            assert!(!collector.media.exists(orphan).await?);
        }
        assert!(collector.media.exists(&newcomer).await?);
        assert!(ch.cdn_gc_question().await?.is_none());
        assert!(shown(&collector.status).is_empty());
        assert_eq!(collector.pass(later).await?.deleted.objects, 1);
        assert!(!collector.media.exists(&newcomer).await?);

        // "Keep them" spares exactly the question's files: the next pass neither deletes them nor asks about them, and a file that became eligible since goes.
        let kept = [
            put(format!("kept 1 {run}"), "png").await?,
            put(format!("kept 2 {run}"), "png").await?,
        ];
        collector.pass(later).await?;
        let question = ch.cdn_gc_question().await?.context("asked")?;
        let since = put(format!("eligible since {run}"), "png").await?;
        assert!(
            collector
                .status
                .answer(question.id, Answer::Keep, || later)
                .await?
        );
        assert!(shown(&collector.status).is_empty());
        let summary = collector.pass(later).await?;
        assert_eq!(
            (summary.report.candidate_objects, summary.deleted.objects),
            (1, 1)
        );
        assert!(ch.cdn_gc_question().await?.is_none());
        assert!(!collector.media.exists(&since).await?);
        for file in &kept {
            assert!(collector.media.exists(file).await?, "{file}");
        }
        // Past the grace from the answer they're candidates again, so still over the limit they're asked about again.
        let lapsed = later + GRACE + MARK_INTERVAL + Duration::from_secs(1);
        let summary = collector.pass(lapsed).await?;
        assert_eq!(summary.deleted.disarmed, Some(Disarmed::Asked));
        assert_eq!(
            ch.cdn_gc_question().await?.map(|question| question.objects),
            Some(2)
        );

        ch.delete_live_project(&project_id).await?;
        Ok(())
    }
}
