//! GCS backend (docs/cdn-gcs-migration.md).
//!
//! Reads go through `object_store`'s native GCS support — streaming `get_opts` only (`get_range`
//! buffers whole ranges and must not be used). Production writes go through a custom
//! multipart/related JSON-API upload (the crate's put options cannot express CRC32C validation
//! or the 412 handling below): a single-shot conditional create with `ifGenerationMatch=0`,
//! `crc32c` in the metadata part so GCS rejects corruption BEFORE storing anything (write-once
//! IAM makes a corrupt stored object permanent), and the key's SHA-256 as custom metadata.
//! Every terminal `412` is discriminated by an object-existence check — live
//! probes (2026-08-31) proved the error body's `reason` field is `conditionNotMet` for
//! encryption-enforcement rejections too, so the body can never be trusted to mean "dedup".

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use axum::body::Bytes;
use base64::Engine as _;
use futures::StreamExt;
use object_store::gcp::{GcpCredentialProvider, GoogleCloudStorageBuilder};
use object_store::path::Path as ObjectPath;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt as _};

use super::{resolve_range, ByteRange, ByteStream, PutOutcome, StoreError, StoreRead};

mod external_account;

pub struct GcsStore {
    reads: Arc<dyn ObjectStore>,
    uploader: Uploader,
}

/// Which identity a credential file serves: the store's must not impersonate and the collector's must (docs/cdn-gcs-migration.md § Credentials).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Identity {
    Store,
    Collector,
}

/// A builder carrying the credentials in `credentials_path`, whose `type` picks the auth: `service_account` (a JSON key, the crate's native support) or `external_account` ([`external_account`]). Either way the builder gets explicit credentials (never `from_env`), so it never falls through to ambient ADC or instance credentials.
pub(crate) fn credentialed_builder(
    credentials_path: &str,
    identity: Identity,
) -> anyhow::Result<GoogleCloudStorageBuilder> {
    #[derive(serde::Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum CredentialFile {
        ServiceAccount,
        ExternalAccount(external_account::Config),
    }
    let json = std::fs::read(credentials_path)
        .with_context(|| format!("reading GCS credentials {credentials_path}"))?;
    let file: CredentialFile = serde_json::from_slice(&json)
        .with_context(|| format!("{credentials_path}: unsupported GCS credential file"))?;
    let impersonates = match &file {
        CredentialFile::ServiceAccount => false,
        CredentialFile::ExternalAccount(config) => config.impersonates(),
    };
    anyhow::ensure!(
        impersonates == (identity == Identity::Collector),
        "{credentials_path}: {}",
        match identity {
            Identity::Store => "the store's credentials must not impersonate a service account",
            Identity::Collector => "the collector's credentials must be an external_account that impersonates its delete identity",
        }
    );
    Ok(match file {
        CredentialFile::ServiceAccount => {
            tracing::info!(
                ?identity,
                credentials = "service_account",
                "CDN GCS credentials"
            );
            GoogleCloudStorageBuilder::new().with_service_account_key(String::from_utf8(json)?)
        }
        CredentialFile::ExternalAccount(config) => {
            let federated = external_account::ExternalAccount::new(config)
                .with_context(|| format!("{credentials_path}: unsupported external_account"))?;
            tracing::info!(
                ?identity,
                credentials = "external_account",
                audience = federated.audience(),
                "CDN GCS credentials"
            );
            GoogleCloudStorageBuilder::new().with_credentials(Arc::new(federated))
        }
    })
}

impl GcsStore {
    pub fn new(bucket: String, credentials_path: &str) -> anyhow::Result<Self> {
        let builder = credentialed_builder(credentials_path, Identity::Store)?;
        let store = builder
            .with_bucket_name(&bucket)
            // FsStore has no whole-transfer deadline and neither may GCS reads: the crate's
            // default client timeout spans the ENTIRE response body, which would abort large or
            // slow-consumer streams mid-transfer. The read timeout bounds stalls instead — each
            // read must make progress — so dead peers still fail without capping transfer
            // duration. Known residual (reviewed, accepted): h2 flow control can leave an armed
            // timer unpolled across a slow-consumer pause on objects larger than the h2 window,
            // so the timer can fire without a real stall — the crate then transparently resumes
            // with an etag-pinned ranged GET (always valid under write-once; ~10 resumes/3min
            // budget), so only a consumer repeatedly stalling >60s on a large object ever sees
            // an abort, and a refetch heals it. Metadata HEADs (Suffix(0), range-rejection
            // recovery) ride this same client and get the same progress bound; federated token
            // exchanges have their own bounded client.
            .with_client_options(
                object_store::ClientOptions::new()
                    .with_timeout_disabled()
                    .with_read_timeout(Duration::from_secs(60))
                    .with_connect_timeout(Duration::from_secs(10)),
            )
            .build()?;
        // The uploader reuses the crate's credential provider: one auth stack, one token cache.
        let credentials = store.credentials().clone();
        Ok(Self {
            reads: Arc::new(store),
            uploader: Uploader::new(
                credentials,
                "https://storage.googleapis.com".to_owned(),
                bucket,
                PROD_BACKOFF_BASE,
            )?,
        })
    }

    #[cfg(test)]
    fn assemble_for_tests(reads: Arc<dyn ObjectStore>, uploader: Uploader) -> Self {
        Self { reads, uploader }
    }

    pub(crate) async fn put_if_absent(&self, key: &str, body: Bytes) -> anyhow::Result<PutOutcome> {
        // The routes' grammar is the one rule (it also guarantees keys need no URL escaping
        // and no ObjectPath normalization surprises); assert so the contract is enforced, not
        // folklore.
        debug_assert!(
            crate::cdn::validate_hosted_key(key),
            "store received an unvalidated key: {key}"
        );
        self.uploader.put_if_absent(key, body).await
    }

    pub(super) async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<StoreRead, StoreError> {
        debug_assert!(
            crate::cdn::validate_hosted_key(key),
            "store received an unvalidated key: {key}"
        );
        let location = ObjectPath::from(key);
        // Suffix(0) is never satisfiable, but backends disagree on how to say so (the crate's
        // Display would send `bytes=-0`); resolve it locally against the real length.
        if let Some(ByteRange::Suffix(0)) = range {
            let total_len = self.head_len(&location).await?;
            return Err(StoreError::RangeNotSatisfiable { total_len });
        }
        let options = GetOptions {
            range: range.map(|r| match r {
                ByteRange::Bounded(s, e) => GetRange::Bounded(s..e),
                ByteRange::From(s) => GetRange::Offset(s),
                ByteRange::Suffix(n) => GetRange::Suffix(n),
            }),
            ..Default::default()
        };
        match self.reads.get_opts(&location, options).await {
            Ok(result) => {
                let total_len = result.meta.size;
                let start = result.range.start;
                let read_len = result.range.end - result.range.start;
                Ok(StoreRead {
                    start,
                    read_len,
                    total_len,
                    stream: exact_len_stream(result.into_stream(), read_len),
                })
            }
            Err(object_store::Error::NotFound { .. }) => Err(StoreError::NotFound),
            Err(e) => {
                let Some(range) = range else {
                    return Err(read_error(e));
                };
                // Range rejections take backend-specific shapes (the crate's client-side
                // validation, GCS's remote 416). Classify by re-resolving against the object's
                // actual length with the same rule FsStore applies: unsatisfiable → 416,
                // satisfiable-but-empty (non-zero suffix on an empty object) → an empty read,
                // and a range that should have worked means the failure was genuine.
                let total_len = match self.head_len(&location).await {
                    Ok(len) => len,
                    Err(StoreError::NotFound) => return Err(StoreError::NotFound),
                    Err(_) => return Err(anyhow::anyhow!("Failed to read object: {e}").into()),
                };
                match resolve_range(Some(range), total_len) {
                    Ok((start, 0)) => Ok(StoreRead {
                        start,
                        read_len: 0,
                        total_len,
                        stream: Box::pin(futures::stream::empty()),
                    }),
                    Ok(_) => Err(read_error(e)),
                    Err(range_error) => Err(range_error),
                }
            }
        }
    }

    /// The read-side client (runtime identity: list + get), shared with the collector's listing and manifest reads (`cdn_gc.rs`).
    pub(crate) fn reads(&self) -> Arc<dyn ObjectStore> {
        self.reads.clone()
    }

    async fn head_len(&self, location: &ObjectPath) -> Result<u64, StoreError> {
        match self.reads.head(location).await {
            Ok(meta) => Ok(meta.size),
            Err(object_store::Error::NotFound { .. }) => Err(StoreError::NotFound),
            Err(e) => Err(read_error(e)),
        }
    }
}

/// A stream that never lies about length: yielding short (a clean end before `promised` bytes)
/// or long (past the window) is an error, mirroring FsStore's `read_exact` honesty.
fn exact_len_stream(
    inner: futures::stream::BoxStream<'static, object_store::Result<Bytes>>,
    promised: u64,
) -> ByteStream {
    Box::pin(futures::stream::try_unfold(
        (inner, promised),
        |(mut inner, remaining)| async move {
            match inner.next().await {
                Some(Ok(chunk)) => {
                    let n = chunk.len() as u64;
                    if n > remaining {
                        count_gcs_error("read");
                        return Err(std::io::Error::other(
                            "object yielded past its promised length",
                        ));
                    }
                    Ok(Some((chunk, (inner, remaining - n))))
                }
                Some(Err(e)) => {
                    count_gcs_error("read");
                    Err(std::io::Error::other(e))
                }
                None if remaining == 0 => Ok(None),
                None => {
                    count_gcs_error("read");
                    Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "object ended before its promised length",
                    ))
                }
            }
        },
    ))
}

/// `mkdb2_cdn_gcs_errors_total{class}` — every GCS error event, transient attempts included
/// (the sustained-5xx alert needs attempt-level visibility, not just terminal outcomes; the
/// classes are [`GCS_ERROR_CLASSES`]).
pub(crate) fn count_gcs_error(class: &'static str) {
    metrics::gauge!("mkdb2_cdn_gcs_last_error_unixtime_seconds", "class" => class)
        .set(crate::deletion::unix_time_seconds());
    metrics::counter!("mkdb2_cdn_gcs_errors_total", "class" => class).increment(1);
}

/// GCS's CRC32C wire form — big-endian bytes, standard base64 — defined once for the uploader
/// and the tests.
fn crc32c_b64(crc: u32) -> String {
    base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes())
}

/// Every class `count_gcs_error` can emit; pre-registered at zero by `register_cdn_metrics`.
pub(crate) const GCS_ERROR_CLASSES: [&str; 10] = [
    "token",
    "transport",
    "http_408",
    "http_429",
    "http_5xx",
    "http_4xx",
    "precondition_no_object",
    "existence_check",
    "budget_exhausted",
    "read",
];

/// The read-failure rule, applied once: count the event, keep the message user-shaped.
fn read_error(e: impl std::fmt::Display) -> StoreError {
    count_gcs_error("read");
    anyhow::anyhow!("Failed to read object: {e}").into()
}

fn note_transient(key: &str, op: &'static str, attempt: u32, class: &'static str, error: &str) {
    count_gcs_error(class);
    tracing::warn!(key = %key, op, attempt, class, error = %error, "CDN GCS transient");
}

/// Production retry pacing: 5 attempts, exponential from 2s ≈ 30s budget (doc-ratified).
const PROD_BACKOFF_BASE: Duration = Duration::from_secs(2);
const MAX_ATTEMPTS: u32 = 5;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

struct Uploader {
    http: reqwest::Client,
    /// `https://storage.googleapis.com` in production; tests point this at a local mock.
    base_url: String,
    bucket: String,
    credentials: GcpCredentialProvider,
    backoff_base: Duration,
    /// The multipart boundary: 128 random bits per process. Request bodies are arbitrary
    /// client bytes, so a predictable boundary could be embedded in one and corrupt the
    /// framing; boundary reuse across independent MIME entities is legal, so no counter.
    boundary_salt: String,
}

impl Uploader {
    fn new(
        credentials: GcpCredentialProvider,
        base_url: String,
        bucket: String,
        backoff_base: Duration,
    ) -> anyhow::Result<Self> {
        let mut salt = [0u8; 16];
        std::io::Read::read_exact(&mut std::fs::File::open("/dev/urandom")?, &mut salt)?;
        Ok(Self {
            http: reqwest::Client::builder().build()?,
            base_url,
            bucket,
            credentials,
            backoff_base,
            boundary_salt: hex::encode(salt),
        })
    }

    async fn bearer(&self) -> anyhow::Result<String> {
        // Hard deadline: however the provider waits, a token must not escape the write budget.
        let credential = tokio::time::timeout(REQUEST_TIMEOUT, self.credentials.get_credential())
            .await
            .map_err(|_| anyhow::anyhow!("token fetch timed out"))??;
        Ok(credential.bearer.clone())
    }

    async fn put_if_absent(&self, key: &str, body: Bytes) -> anyhow::Result<PutOutcome> {
        let crc = crc32c_b64(crc32c::crc32c(&body));
        self.put_with_crc(key, body, &crc).await
    }

    /// Split from [`Self::put_if_absent`] so the live tests can send a deliberately wrong
    /// checksum through the REAL framing path and pin GCS's reject-before-store behavior.
    async fn put_with_crc(&self, key: &str, body: Bytes, crc: &str) -> anyhow::Result<PutOutcome> {
        let boundary = format!("kymo_{}", self.boundary_salt);
        let mime = mime_guess::from_path(key)
            .first_or_octet_stream()
            .to_string();
        // metadata.sha256 = the body's SHA-256 digest, which IS the key's hash: the upload route
        // derives every key from it, and dedup already trusts that equality.
        let sha256 = key.split('.').next().unwrap_or(key);
        anyhow::ensure!(sha256.len() == 64, "{key} is not a full SHA-256 key");
        let resource = serde_json::json!({
            "name": key,
            "contentType": mime,
            // GCS validates a provided crc32c and rejects the create on mismatch — corruption
            // fails BEFORE anything is stored (probed live 2026-08-31: 400 `reason: invalid`,
            // no object).
            "crc32c": crc,
            // Permanent under write-once; CMEK serving overrides it today, but it is kept for
            // header-consistency and any future non-CMEK world (docs/cdn-gcs-migration.md).
            "cacheControl": "private, max-age=31536000, immutable",
            "metadata": { "sha256": sha256 },
        });
        // Three refcounted chunks streamed per attempt — never a contiguous copy of the body
        // (a 256MiB envelope upload would otherwise hold 512MiB for the whole retry window).
        let head = Bytes::from(format!(
            "--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{resource}\r\n--{boundary}\r\nContent-Type: {mime}\r\n\r\n"
        ));
        let tail = Bytes::from(format!("\r\n--{boundary}--\r\n"));

        let url = format!(
            "{}/upload/storage/v1/b/{}/o?uploadType=multipart&ifGenerationMatch=0",
            self.base_url, self.bucket
        );
        let content_type = format!("multipart/related; boundary={boundary}");
        let (status, detail) = self
            .request_text_retried(key, "upload", || {
                let chunks = [head.clone(), body.clone(), tail.clone()];
                self.http
                    .post(&url)
                    .header("Content-Type", &content_type)
                    .body(reqwest::Body::wrap_stream(futures::stream::iter(
                        chunks.map(Ok::<_, std::convert::Infallible>),
                    )))
            })
            .await?;
        if (200..300).contains(&status) {
            tracing::info!(key = %key, size = body.len(), backend = "gcs", "CDN resource stored");
            return Ok(PutOutcome::Created);
        }
        let detail = truncate(&detail, 600);
        if status == 412 {
            return self.ack_if_exists(key, &detail).await;
        }
        // 400 (checksum mismatch included), 401/403, and anything else are terminal.
        count_gcs_error("http_4xx");
        anyhow::bail!("CDN GCS upload failed ({status}): {detail}")
    }

    /// THE transient retry rule, applied once: 5 attempts, exponential backoff (~30s
    /// cumulative); token fetches, transport failures, 408/429/5xx, and a dropped 2xx body all
    /// retry with every error counted; the first fully-read non-transient response returns as
    /// `(status, body)`; exhaustion errors. The build closure produces a fresh request per
    /// attempt (streamed bodies are single-use). Only a 2xx body is load-bearing — error
    /// bodies are detail-only and read best-effort — and a dropped SUCCESS body retrying the
    /// PUT converges through the 412 + existence-check path, so no ack is ever lost.
    async fn request_text_retried(
        &self,
        key: &str,
        op: &'static str,
        build: impl Fn() -> reqwest::RequestBuilder,
    ) -> anyhow::Result<(u16, String)> {
        for attempt in 0..MAX_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(self.backoff_base * 2u32.pow(attempt - 1)).await;
            }
            // A failed token fetch is transient like a transport error, not terminal.
            let bearer = match self.bearer().await {
                Ok(b) => b,
                Err(e) => {
                    note_transient(key, op, attempt, "token", &e.to_string());
                    continue;
                }
            };
            let response = match build()
                .header("Authorization", format!("Bearer {bearer}"))
                .timeout(REQUEST_TIMEOUT)
                .send()
                .await
            {
                Ok(r) => r,
                // Transport-level failures (connect, timeout) are transient like 5xx.
                Err(e) => {
                    note_transient(key, op, attempt, "transport", &e.to_string());
                    continue;
                }
            };
            let status = response.status().as_u16();
            if status == 429 || status == 408 || response.status().is_server_error() {
                let class = match status {
                    408 => "http_408",
                    429 => "http_429",
                    _ => "http_5xx",
                };
                note_transient(key, op, attempt, class, &format!("status {status}"));
                continue;
            }
            match response.text().await {
                Ok(body) => return Ok((status, body)),
                Err(e) if status < 300 => {
                    note_transient(key, op, attempt, "transport", &e.to_string());
                }
                Err(_) => return Ok((status, String::new())),
            }
        }
        count_gcs_error("budget_exhausted");
        anyhow::bail!("CDN GCS {op} retry budget exhausted for {key}")
    }

    /// The 412 discriminator: a precondition 412 means the object exists; an
    /// encryption-enforcement rejection is ALSO a 412 with the same JSON `reason`
    /// (`conditionNotMet` — probed live), so existence is the only trustworthy ack condition.
    /// Single-shot deliberately: a transient here fails the upload, and the client spool
    /// retries the whole write — never ack on ambiguity. Every failure of the check counts once
    /// as `existence_check` (a partition of events); the root cause rides the error to the
    /// caller, which logs it with the key.
    async fn ack_if_exists(&self, key: &str, rejection: &str) -> anyhow::Result<PutOutcome> {
        let url = format!("{}/storage/v1/b/{}/o/{}", self.base_url, self.bucket, key);
        let failed = |detail: String| {
            count_gcs_error("existence_check");
            anyhow::anyhow!("412 existence check for {key}: {detail}")
        };
        let bearer = self
            .bearer()
            .await
            .map_err(|e| failed(format!("token: {e}")))?;
        let response = self
            .http
            .get(&url)
            .header("Authorization", format!("Bearer {bearer}"))
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|e| failed(e.to_string()))?;
        match response.status().as_u16() {
            200 => {
                tracing::debug!(key = %key, backend = "gcs", "CDN resource already exists (dedup)");
                Ok(PutOutcome::Existing)
            }
            404 => {
                count_gcs_error("precondition_no_object");
                anyhow::bail!(
                    "CDN GCS upload got 412 but no stored object — not a dedup hit (encryption enforcement or bucket misconfig?): {rejection}"
                )
            }
            other => {
                let detail = truncate(&response.text().await.unwrap_or_default(), 300);
                Err(failed(format!("discrimination failed (returned {other}: {detail}); rejection was: {rejection}")))
            }
        }
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_owned();
    }
    let mut cut = max;
    while !s.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}…", &s[..cut])
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::{get, post};
    use axum::Router;
    use object_store::memory::InMemory;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    // Real bodies captured live from the production bucket on 2026-08-31 (see the design doc): the
    // genuine precondition 412, the encryption-enforcement 412 — note the IDENTICAL
    // `conditionNotMet` reason — and the pre-store checksum rejection.
    pub(crate) const FIXTURE_DEDUP_412: &str = r#"{"error":{"code":412,"message":"At least one of the pre-conditions you specified did not hold.","errors":[{"message":"At least one of the pre-conditions you specified did not hold.","domain":"global","reason":"conditionNotMet","locationType":"header","location":"If-Match"}]}}"#;
    pub(crate) const FIXTURE_ENFORCEMENT_412: &str = r#"{"error":{"code":412,"message":"Requested encryption type for object is not compliant with the bucket's encryption enforcement configuration.","errors":[{"message":"Requested encryption type for object is not compliant with the bucket's encryption enforcement configuration.","domain":"global","reason":"conditionNotMet","locationType":"header","location":"If-Match"}]}}"#;
    pub(crate) const FIXTURE_CRC_MISMATCH_400: &str = r#"{"error":{"code":400,"message":"Provided CRC32C \"AAAAAA==\" doesn't match calculated CRC32C \"OIcRkQ==\".","errors":[{"message":"Provided CRC32C \"AAAAAA==\" doesn't match calculated CRC32C \"OIcRkQ==\".","domain":"global","reason":"invalid"}]}}"#;

    pub(crate) const KEY: &str =
        "aabbccdd0123456789aabbccdd0123456789aabbccdd0123456789aabbccdd01.txt";
    /// sha256 of the empty input — a genuine content-addressed empty object.
    pub(crate) const EMPTY_KEY: &str =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855.gz";

    pub(crate) fn static_credentials(bearer: &str) -> GcpCredentialProvider {
        Arc::new(object_store::StaticCredentialProvider::new(
            object_store::gcp::GcpCredential {
                bearer: bearer.to_owned(),
            },
        ))
    }

    pub(crate) async fn collect(read: StoreRead) -> Vec<u8> {
        use futures::TryStreamExt as _;
        read.stream
            .try_collect::<Vec<Bytes>>()
            .await
            .unwrap()
            .concat()
    }

    /// The RFC 9110 range contract every backend must honor identically, run against a store
    /// pre-seeded with `KEY` → b"0123456789" and `EMPTY_KEY` → b"" (RFC_RANGE_PARITY: one
    /// suite, both implementations).
    pub(crate) async fn assert_range_contract(store: &crate::cdn_store::CdnStore) {
        let full = store.get(KEY, None).await.unwrap();
        assert_eq!((full.start, full.read_len, full.total_len), (0, 10, 10));
        assert_eq!(collect(full).await, b"0123456789");

        let mid = store
            .get(KEY, Some(ByteRange::Bounded(2, 6)))
            .await
            .unwrap();
        assert_eq!((mid.start, mid.read_len, mid.total_len), (2, 4, 10));
        assert_eq!(collect(mid).await, b"2345");

        // end past EOF clamps
        let tail = store
            .get(KEY, Some(ByteRange::Bounded(8, 100)))
            .await
            .unwrap();
        assert_eq!(collect(tail).await, b"89");

        let from = store.get(KEY, Some(ByteRange::From(8))).await.unwrap();
        assert_eq!((from.start, from.read_len), (8, 2));
        assert_eq!(collect(from).await, b"89");

        let suffix = store.get(KEY, Some(ByteRange::Suffix(4))).await.unwrap();
        assert_eq!((suffix.start, suffix.read_len), (6, 4));
        assert_eq!(collect(suffix).await, b"6789");

        // start at/past EOF (Bounded or From) and zero suffix-lengths are unsatisfiable (416)
        for r in [
            ByteRange::Bounded(10, 12),
            ByteRange::From(10),
            ByteRange::Suffix(0),
        ] {
            assert!(matches!(
                store.get(KEY, Some(r)).await,
                Err(StoreError::RangeNotSatisfiable { total_len: 10 })
            ));
        }
        // Zero-width and inverted bounds can't come from a valid RFC parse — caller bug, not 416.
        for r in [ByteRange::Bounded(5, 5), ByteRange::Bounded(6, 5)] {
            assert!(matches!(
                store.get(KEY, Some(r)).await,
                Err(StoreError::Other(_))
            ));
        }
        assert!(matches!(
            store.get("ee00ee00.txt", None).await,
            Err(StoreError::NotFound)
        ));

        // Zero-length representation: full read is empty; NO absolute range matches it…
        let read = store.get(EMPTY_KEY, None).await.unwrap();
        assert_eq!((read.read_len, read.total_len), (0, 0));
        assert!(collect(read).await.is_empty());
        assert!(matches!(
            store.get(EMPTY_KEY, Some(ByteRange::Bounded(0, 4))).await,
            Err(StoreError::RangeNotSatisfiable { total_len: 0 })
        ));
        // …but a non-zero suffix range serves the (empty) entire representation.
        let suffix = store
            .get(EMPTY_KEY, Some(ByteRange::Suffix(5)))
            .await
            .unwrap();
        assert_eq!((suffix.start, suffix.read_len), (0, 0));
        assert!(collect(suffix).await.is_empty());
    }

    /// Scripted mock: uploads pop responses front-to-back (and record the request); the
    /// object-existence GET answers the fixed `exists_status` and counts hits.
    pub(crate) struct Mock {
        pub(crate) uploads: Mutex<Vec<(u16, &'static str)>>,
        pub(crate) upload_requests: Mutex<Vec<(String, HeaderMap, Bytes)>>,
        pub(crate) exists_status: u16,
        pub(crate) exists_hits: AtomicU64,
    }

    pub(super) async fn mock_uploader(mock: Arc<Mock>, backoff: Duration) -> Uploader {
        let app = Router::new()
            .route(
                "/upload/storage/v1/b/{bucket}/o",
                post(
                    |State(m): State<Arc<Mock>>,
                     uri: axum::http::Uri,
                     headers: HeaderMap,
                     body: Bytes| async move {
                        m.upload_requests
                            .lock()
                            .unwrap()
                            .push((uri.to_string(), headers, body));
                        let (status, body) = m.uploads.lock().unwrap().remove(0);
                        (StatusCode::from_u16(status).unwrap(), body)
                    },
                ),
            )
            .route(
                "/storage/v1/b/{bucket}/o/{key}",
                get(|State(m): State<Arc<Mock>>| async move {
                    m.exists_hits.fetch_add(1, Ordering::Relaxed);
                    (StatusCode::from_u16(m.exists_status).unwrap(), "{}")
                }),
            )
            .with_state(mock);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Uploader::new(
            static_credentials("test-token"),
            base_url,
            "test-bucket".into(),
            backoff,
        )
        .unwrap()
    }

    pub(crate) fn mock(uploads: Vec<(u16, &'static str)>, exists_status: u16) -> Arc<Mock> {
        Arc::new(Mock {
            uploads: Mutex::new(uploads),
            upload_requests: Mutex::new(Vec::new()),
            exists_status,
            exists_hits: AtomicU64::new(0),
        })
    }

    /// A GcsStore whose reads are an `InMemory` seeded with `objects` and whose write path is
    /// the scripted HTTP mock.
    pub(crate) async fn gcs_with_mock(mock: Arc<Mock>, objects: &[(&str, &[u8])]) -> GcsStore {
        let uploader = mock_uploader(mock, Duration::from_millis(1)).await;
        let mem = InMemory::new();
        for (key, body) in objects {
            mem.put(&ObjectPath::from(*key), Bytes::copy_from_slice(body).into())
                .await
                .unwrap();
        }
        GcsStore::assemble_for_tests(Arc::new(mem), uploader)
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use crate::cdn::content_key;
    use crate::cdn_store::CdnStore;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use std::sync::atomic::Ordering;

    /// Every fetch fails, pinning the token-transient retry arm.
    #[derive(Debug)]
    struct FailingCredentials;

    #[async_trait::async_trait]
    impl object_store::CredentialProvider for FailingCredentials {
        type Credential = object_store::gcp::GcpCredential;

        async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
            Err(object_store::Error::Generic {
                store: "GCS",
                source: "scripted token failure".into(),
            })
        }
    }

    fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|w| w == needle)
    }

    /// Byte-exact RFC 2387 parse: returns each part's raw bytes (headers + payload), asserting
    /// opening delimiter, part separators, and the close delimiter.
    fn split_multipart<'a>(body: &'a [u8], boundary: &str) -> Vec<(&'a [u8], &'a [u8])> {
        let open = format!("--{boundary}\r\n");
        let delim = format!("\r\n--{boundary}");
        assert!(
            body.starts_with(open.as_bytes()),
            "missing opening delimiter"
        );
        let mut parts = Vec::new();
        let mut rest = &body[open.len()..];
        loop {
            let end = find(rest, delim.as_bytes()).expect("unterminated part");
            let part = &rest[..end];
            let split = find(part, b"\r\n\r\n").expect("part without header separator");
            parts.push((&part[..split], &part[split + 4..]));
            rest = &rest[end + delim.len()..];
            if let Some(after) = rest.strip_prefix(b"--") {
                assert_eq!(after, b"\r\n", "trailing bytes after close delimiter");
                return parts;
            }
            rest = rest
                .strip_prefix(b"\r\n")
                .expect("malformed part separator");
        }
    }

    #[tokio::test]
    async fn fresh_create_acks_and_sends_validating_multipart() {
        // Binary, non-UTF8 media containing CRLFs and a fake delimiter prefix: the framing must
        // survive arbitrary client bytes byte-for-byte.
        const MEDIA: &[u8] = b"\x89PNG\r\n\x1a\n\x00\xff--kymo_binary\r\n\x01";
        let m = mock(vec![(200, r#"{"name":"x"}"#)], 404);
        let up = mock_uploader(m.clone(), Duration::from_millis(1)).await;
        assert_eq!(
            up.put_if_absent(KEY, Bytes::from_static(MEDIA))
                .await
                .unwrap(),
            PutOutcome::Created
        );

        let requests = m.upload_requests.lock().unwrap();
        let (uri, headers, body) = &requests[0];
        assert_eq!(
            uri,
            "/upload/storage/v1/b/test-bucket/o?uploadType=multipart&ifGenerationMatch=0"
        );
        assert_eq!(headers["authorization"], "Bearer test-token");
        let content_type = headers["content-type"].to_str().unwrap();
        let boundary = content_type
            .strip_prefix("multipart/related; boundary=")
            .unwrap();

        let parts = split_multipart(body, boundary);
        assert_eq!(parts.len(), 2, "exactly metadata part then media part");
        let (json_headers, json_payload) = parts[0];
        assert_eq!(
            json_headers,
            b"Content-Type: application/json; charset=UTF-8"
        );
        // Metadata part carries the pre-store CRC32C (base64 big-endian), the key's sha256,
        // and the doc-mandated permanent cacheControl.
        let resource: serde_json::Value = serde_json::from_slice(json_payload).unwrap();
        assert_eq!(resource["name"], KEY);
        let expected_crc = crc32c_b64(crc32c::crc32c(MEDIA));
        assert_eq!(resource["crc32c"], expected_crc.as_str());
        assert_eq!(
            resource["metadata"]["sha256"],
            KEY.split('.').next().unwrap()
        );
        assert_eq!(
            resource["cacheControl"],
            "private, max-age=31536000, immutable"
        );
        let (media_headers, media_payload) = parts[1];
        assert_eq!(media_headers, b"Content-Type: text/plain");
        assert_eq!(media_payload, MEDIA, "media must arrive byte-identical");
    }

    #[tokio::test]
    async fn dedup_412_with_existing_object_acks() {
        let m = mock(vec![(412, FIXTURE_DEDUP_412)], 200);
        let up = mock_uploader(m, Duration::from_millis(1)).await;
        assert_eq!(
            up.put_if_absent(KEY, Bytes::from_static(b"0123456789"))
                .await
                .unwrap(),
            PutOutcome::Existing
        );
    }

    #[tokio::test]
    async fn enforcement_412_without_object_is_an_error_not_a_dedup() {
        let m = mock(vec![(412, FIXTURE_ENFORCEMENT_412)], 404);
        let up = mock_uploader(m.clone(), Duration::from_millis(1)).await;
        let err = up
            .put_if_absent(KEY, Bytes::from_static(b"0123456789"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no stored object"), "{err}");
        assert_eq!(
            m.upload_requests.lock().unwrap().len(),
            1,
            "412 is terminal"
        );
        assert_eq!(
            m.exists_hits.load(Ordering::Relaxed),
            1,
            "single-shot check"
        );
    }

    #[tokio::test]
    async fn crc_mismatch_is_terminal() {
        let m = mock(vec![(400, FIXTURE_CRC_MISMATCH_400)], 404);
        let up = mock_uploader(m.clone(), Duration::from_millis(1)).await;
        let err = up
            .put_if_absent(KEY, Bytes::from_static(b"0123456789"))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("doesn't match calculated"),
            "{err}"
        );
        assert_eq!(
            m.upload_requests.lock().unwrap().len(),
            1,
            "must not retry a 400"
        );
    }

    #[tokio::test]
    async fn transients_retry_to_success_and_budget_is_bounded() {
        let m = mock(
            vec![
                (503, "unavailable"),
                (408, "request timeout"),
                (429, "slow down"),
                (200, "{}"),
            ],
            404,
        );
        let up = mock_uploader(m.clone(), Duration::from_millis(1)).await;
        up.put_if_absent(KEY, Bytes::from_static(b"0123456789"))
            .await
            .unwrap();
        assert_eq!(m.upload_requests.lock().unwrap().len(), 4);

        let m = mock(vec![(503, "u"); 8], 404);
        let up = mock_uploader(m.clone(), Duration::from_millis(1)).await;
        let err = up
            .put_if_absent(KEY, Bytes::from_static(b"0123456789"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("retry budget exhausted"), "{err}");
        assert_eq!(
            m.upload_requests.lock().unwrap().len(),
            MAX_ATTEMPTS as usize
        );
    }

    #[tokio::test]
    async fn existence_check_failure_is_an_error_never_an_ack() {
        // The doc's fourth matrix row: 412 whose discrimination GET itself fails must error.
        let m = mock(vec![(412, FIXTURE_DEDUP_412)], 503);
        let up = mock_uploader(m.clone(), Duration::from_millis(1)).await;
        let err = up
            .put_if_absent(KEY, Bytes::from_static(b"0123456789"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("discrimination failed"), "{err}");
        assert_eq!(m.upload_requests.lock().unwrap().len(), 1, "no PUT retry");
        assert_eq!(
            m.exists_hits.load(Ordering::Relaxed),
            1,
            "single-shot check"
        );
    }

    #[tokio::test]
    async fn token_fetch_failures_are_transient_until_the_budget() {
        let m = mock(Vec::new(), 404);
        let mut up = mock_uploader(m.clone(), Duration::from_millis(1)).await;
        up.credentials = Arc::new(FailingCredentials);
        let err = up
            .put_if_absent(KEY, Bytes::from_static(b"0123456789"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("retry budget exhausted"), "{err}");
        assert_eq!(
            m.upload_requests.lock().unwrap().len(),
            0,
            "no request without a token"
        );
    }

    /// Crate-behavior canary only (production PUTs never rely on this mapping — the design doc
    /// requires pinning it so a crate upgrade changing the mapping is caught).
    #[tokio::test]
    async fn crate_canary_put_mode_create_maps_to_already_exists() {
        let mem = InMemory::new();
        let path = ObjectPath::from(KEY);
        let opts = object_store::PutOptions {
            mode: object_store::PutMode::Create,
            ..Default::default()
        };
        mem.put_opts(&path, Bytes::from_static(b"x").into(), opts.clone())
            .await
            .unwrap();
        let err = mem
            .put_opts(&path, Bytes::from_static(b"x").into(), opts)
            .await
            .unwrap_err();
        assert!(matches!(err, object_store::Error::AlreadyExists { .. }));
    }

    // ---- Read-side conformance: the ByteRange contract must match FsStore exactly, pinned
    // against the crate's InMemory backend (same GetRange semantics as the remote path).

    #[tokio::test]
    async fn gcs_store_honors_the_shared_range_contract() {
        let gcs = gcs_with_mock(
            mock(Vec::new(), 404),
            &[(KEY, b"0123456789"), (EMPTY_KEY, b"")],
        )
        .await;
        let store = CdnStore::Gcs(gcs);
        assert_range_contract(&store).await;
    }

    // ---- Recovery classifier: production-only branches (remote range rejections) pinned
    // against a scripted store whose ranged gets always fail. `head()` reaches the double via
    // ObjectStoreExt's default impl (get_opts with head=true) — the same path production takes.

    enum HeadScript {
        Size(u64),
        NotFound,
        Fail,
    }

    struct ScriptedReads(HeadScript);

    impl std::fmt::Debug for ScriptedReads {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("ScriptedReads")
        }
    }
    impl std::fmt::Display for ScriptedReads {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("ScriptedReads")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for ScriptedReads {
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if options.head {
                return match self.0 {
                    HeadScript::Size(size) => Ok(object_store::GetResult {
                        payload: object_store::GetResultPayload::Stream(
                            futures::stream::empty().boxed(),
                        ),
                        meta: object_store::ObjectMeta {
                            location: location.clone(),
                            last_modified: chrono::Utc::now(),
                            size,
                            e_tag: None,
                            version: None,
                        },
                        range: 0..0,
                        attributes: Default::default(),
                        extensions: Default::default(),
                    }),
                    HeadScript::NotFound => Err(object_store::Error::NotFound {
                        path: location.to_string(),
                        source: "scripted".into(),
                    }),
                    HeadScript::Fail => Err(object_store::Error::Generic {
                        store: "scripted",
                        source: "head failed".into(),
                    }),
                };
            }
            Err(object_store::Error::Generic {
                store: "scripted",
                source: "range rejected remotely".into(),
            })
        }
        async fn put_opts(
            &self,
            _: &ObjectPath,
            _: object_store::PutPayload,
            _: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            unimplemented!()
        }
        async fn put_multipart_opts(
            &self,
            _: &ObjectPath,
            _: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            unimplemented!()
        }
        fn list(
            &self,
            _: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            unimplemented!()
        }
        async fn list_with_delimiter(
            &self,
            _: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            unimplemented!()
        }
        async fn copy_opts(
            &self,
            _: &ObjectPath,
            _: &ObjectPath,
            _: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            unimplemented!()
        }
        fn delete_stream(
            &self,
            _: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
            unimplemented!()
        }
    }

    fn scripted_store(head: HeadScript) -> GcsStore {
        GcsStore::assemble_for_tests(
            Arc::new(ScriptedReads(head)),
            Uploader::new(
                static_credentials("unused"),
                "http://127.0.0.1:1".into(),
                "unused".into(),
                Duration::from_millis(1),
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn remote_range_rejections_classify_via_head() {
        // Unsatisfiable against the real length → 416 carrying that length.
        let store = scripted_store(HeadScript::Size(10));
        assert!(matches!(
            store.get(KEY, Some(ByteRange::From(10))).await,
            Err(StoreError::RangeNotSatisfiable { total_len: 10 })
        ));
        // A range that SHOULD have worked means the failure was genuine — original error kept.
        let err = store.get(KEY, Some(ByteRange::Bounded(2, 6))).await;
        match err {
            Err(StoreError::Other(e)) => {
                assert!(e.to_string().contains("range rejected remotely"), "{e}")
            }
            _ => panic!("expected the original error"),
        }
        // Full (rangeless) reads never classify — the original error propagates.
        let err = store.get(KEY, None).await;
        assert!(matches!(err, Err(StoreError::Other(_))));

        // Satisfiable-but-empty: non-zero suffix on an empty object serves an empty read even
        // when the backend rejected the wire form.
        let store = scripted_store(HeadScript::Size(0));
        let read = store.get(KEY, Some(ByteRange::Suffix(5))).await.unwrap();
        assert_eq!((read.start, read.read_len, read.total_len), (0, 0, 0));
        assert!(collect(read).await.is_empty());
        // Suffix(0) resolves locally through the same head.
        assert!(matches!(
            store.get(KEY, Some(ByteRange::Suffix(0))).await,
            Err(StoreError::RangeNotSatisfiable { total_len: 0 })
        ));

        // The object vanished (or never existed): NotFound, not a range error.
        let store = scripted_store(HeadScript::NotFound);
        assert!(matches!(
            store.get(KEY, Some(ByteRange::From(10))).await,
            Err(StoreError::NotFound)
        ));

        // Head itself failing keeps the original ranged-get error.
        let store = scripted_store(HeadScript::Fail);
        let err = store.get(KEY, Some(ByteRange::From(10))).await;
        match err {
            Err(StoreError::Other(e)) => {
                assert!(e.to_string().contains("range rejected remotely"), "{e}")
            }
            _ => panic!("expected the original error"),
        }
    }

    #[tokio::test]
    async fn exact_len_stream_never_lies() {
        let chunks = |v: Vec<&'static [u8]>| {
            futures::stream::iter(v.into_iter().map(|c| Ok(Bytes::from_static(c)))).boxed()
        };
        // Exact length passes through.
        let ok: Vec<u8> = exact_len_stream(chunks(vec![b"01", b"234"]), 5)
            .try_collect::<Vec<Bytes>>()
            .await
            .unwrap()
            .concat();
        assert_eq!(ok, b"01234");
        // Short: clean end before the promise is UnexpectedEof, not success.
        let err = exact_len_stream(chunks(vec![b"01"]), 5)
            .try_collect::<Vec<Bytes>>()
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        // Long: yielding past the window is an error too.
        let err = exact_len_stream(chunks(vec![b"012345"]), 5)
            .try_collect::<Vec<Bytes>>()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("past its promised length"));
    }

    #[test]
    fn credential_files_pick_the_auth_and_match_their_identity() {
        let dir = tempfile::tempdir().unwrap();
        let file = |name: &str, json: serde_json::Value| {
            let path = dir.path().join(name);
            std::fs::write(&path, json.to_string()).unwrap();
            path.to_str().unwrap().to_owned()
        };
        // What `gcloud iam workload-identity-pools create-cred-config --credential-source-file` writes.
        let federated = file(
            "wif.json",
            serde_json::json!({
                "universe_domain": "googleapis.com",
                "type": "external_account",
                "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/k",
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "token_url": "https://sts.googleapis.com/v1/token",
                "credential_source": { "file": "/nonexistent" },
                "token_info_url": "https://sts.googleapis.com/v1/introspect",
            }),
        );
        GcsStore::new("bucket".into(), &federated).unwrap();
        // A key file's other fields ride past the tag; `disable_oauth` spares the test a real private key.
        let key = file(
            "key.json",
            serde_json::json!({
                "type": "service_account",
                "private_key": "unused",
                "private_key_id": "unused",
                "client_email": "kymo@sa.example",
                "disable_oauth": true,
            }),
        );
        GcsStore::new("bucket".into(), &key).unwrap();
        // The store never impersonates; the collector only impersonates. What `create-cred-config --service-account` writes (it drops `token_info_url`).
        let impersonating = file(
            "gc.json",
            serde_json::json!({
                "universe_domain": "googleapis.com",
                "type": "external_account",
                "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/k",
                "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
                "token_url": "https://sts.googleapis.com/v1/token",
                "credential_source": { "file": "/nonexistent" },
                "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/gc@sa.example:generateAccessToken",
            }),
        );
        let store_err = GcsStore::new("bucket".into(), &impersonating)
            .err()
            .unwrap();
        assert!(
            format!("{store_err:#}").contains("must not impersonate"),
            "{store_err:#}"
        );
        credentialed_builder(&impersonating, Identity::Collector).unwrap();
        for direct in [&federated, &key] {
            let err = credentialed_builder(direct, Identity::Collector)
                .err()
                .unwrap();
            assert!(
                format!("{err:#}").contains("must be an external_account that impersonates"),
                "{err:#}"
            );
        }
        let user = file(
            "user.json",
            serde_json::json!({ "type": "authorized_user" }),
        );
        let err = GcsStore::new("bucket".into(), &user).err().unwrap();
        assert!(
            format!("{err:#}").contains("unknown variant `authorized_user`"),
            "{err:#}"
        );
    }

    fn live_store() -> GcsStore {
        let bucket = std::env::var("KYMO_GCS_LIVE_TEST_BUCKET")
            .expect("set KYMO_GCS_LIVE_TEST_BUCKET (and GOOGLE_APPLICATION_CREDENTIALS)");
        let credentials = std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .expect("set GOOGLE_APPLICATION_CREDENTIALS to a credential file path");
        GcsStore::new(bucket, &credentials).unwrap()
    }

    /// The doc's live adapter matrix (docs/cdn-gcs-migration.md §Write path) plus the read
    /// contract, against the real bucket:
    /// `GOOGLE_APPLICATION_CREDENTIALS=<credentials.json> KYMO_GCS_LIVE_TEST_BUCKET=<bucket> \
    ///  cargo test -p kymo-server gcs_live -- --ignored`
    /// Keys are content addresses, as the upload route makes them, so test objects are genuine
    /// CDN objects: unreferenced, which the collector reclaims after its grace. Fixed-content
    /// keys take the dedup path until then; the fresh-create case uses unique content.
    #[tokio::test]
    #[ignore]
    async fn gcs_live_round_trip() {
        let store = live_store();

        // Fixed object: dedup 412 + existence ack, or a create if it's new or was reclaimed.
        let body: &[u8] = b"kymo gcs live conformance object v1\n";
        let key = content_key(body, "txt");
        store
            .put_if_absent(&key, Bytes::from_static(body))
            .await
            .unwrap();

        // Fresh create + same-run dedup, exercising the 200 path every run (unique content).
        let unique = format!(
            "kymo gcs live fresh-create probe {:?}\n",
            std::time::SystemTime::now()
        );
        let ukey = content_key(unique.as_bytes(), "txt");
        let ubytes = Bytes::from(unique.into_bytes());
        store.put_if_absent(&ukey, ubytes.clone()).await.unwrap();
        store.put_if_absent(&ukey, ubytes).await.unwrap();

        // Zero-byte envelope edge.
        let zkey = content_key(b"", "gz");
        store.put_if_absent(&zkey, Bytes::new()).await.unwrap();
        let zread = store.get(&zkey, None).await.unwrap();
        assert_eq!((zread.read_len, zread.total_len), (0, 0));

        // Checksum mismatch through the REAL framing path: rejected terminally, and rejected
        // BEFORE storing — the key must stay absent.
        let corrupt = b"kymo gcs live wrong-crc probe body";
        let ckey = content_key(corrupt, "txt");
        let err = store
            .uploader
            .put_with_crc(&ckey, Bytes::from_static(corrupt), "AAAAAA==")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("doesn't match"), "{err}");
        assert!(matches!(
            store.get(&ckey, None).await,
            Err(StoreError::NotFound)
        ));

        // Enforcement rejection: a CSEK write is refused by the bucket's CMEK enforcement with
        // a 412 whose body matches the dedup reason — the discriminator must land on "absent
        // object = error", never an ack. (Raw request: the production adapter cannot itself
        // construct a non-compliant write.) The probe name sits OUTSIDE the hosted key grammar
        // and is unique per run, so if enforcement ever regressed the accepted junk could never
        // collide with or be admitted as a real CDN object; no precondition is sent, keeping
        // the 412 attributable to enforcement alone.
        let ekey = format!(
            "_live-enforcement-probe-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let csek_key = base64::engine::general_purpose::STANDARD.encode([0x42u8; 32]);
        let csek_sha = base64::engine::general_purpose::STANDARD
            .encode(<sha2::Sha256 as sha2::Digest>::digest([0x42u8; 32]));
        let response = store
            .uploader
            .http
            .post(format!(
                "{}/upload/storage/v1/b/{}/o?uploadType=media&name={}",
                store.uploader.base_url, store.uploader.bucket, ekey
            ))
            .header(
                "Authorization",
                format!("Bearer {}", store.uploader.bearer().await.unwrap()),
            )
            .header("x-goog-encryption-algorithm", "AES256")
            .header("x-goog-encryption-key", &csek_key)
            .header("x-goog-encryption-key-sha256", &csek_sha)
            .body("enforcement probe")
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status().as_u16(),
            412,
            "CMEK enforcement must 412 — if this write was ACCEPTED, ask an admin to delete {ekey}"
        );
        let err = store
            .uploader
            .ack_if_exists(&ekey, "live enforcement probe")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no stored object"), "{err}");

        // Crate-behavior canary against real GCS (production never relies on this mapping).
        let canary = store
            .reads
            .put_opts(
                &ObjectPath::from(key.as_str()),
                Bytes::from_static(body).into(),
                object_store::PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(canary, object_store::Error::AlreadyExists { .. }));

        // Read contract.
        let full = store.get(&key, None).await.unwrap();
        assert_eq!(full.total_len, body.len() as u64);
        assert_eq!(collect(full).await, body);

        let ranged = store
            .get(&key, Some(ByteRange::Bounded(6, 9)))
            .await
            .unwrap();
        assert_eq!(
            (ranged.start, ranged.read_len, ranged.total_len),
            (6, 3, body.len() as u64)
        );
        assert_eq!(collect(ranged).await, b"gcs");

        let over = store
            .get(&key, Some(ByteRange::From(body.len() as u64)))
            .await;
        assert!(matches!(
            over,
            Err(StoreError::RangeNotSatisfiable { total_len }) if total_len == body.len() as u64
        ));
        assert!(matches!(
            store.get("ee00ee00ff11ff11.txt", None).await,
            Err(StoreError::NotFound)
        ));
    }

    /// The 256MiB envelope ceiling (docs/cdn-gcs-migration.md live matrix), separate because it
    /// moves 256MiB over the wire — run it where bandwidth allows. Deterministic content, so it
    /// stores one object, which later runs dedup until the collector reclaims it.
    #[tokio::test]
    #[ignore]
    async fn gcs_live_256mib_envelope() {
        let store = live_store();
        let body: Bytes = (0..256u32 << 20)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<u8>>()
            .into();
        let key = content_key(&body, "bin");
        store.put_if_absent(&key, body.clone()).await.unwrap();

        let suffix = store.get(&key, Some(ByteRange::Suffix(16))).await.unwrap();
        assert_eq!(suffix.total_len, body.len() as u64);
        assert_eq!(collect(suffix).await, body[body.len() - 16..]);
    }
}
