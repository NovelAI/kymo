use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use sha2::{Digest, Sha256};

use crate::cdn_store::{ByteRange, CdnStore, StoreError};

pub struct CdnState {
    pub store: CdnStore,
    pub(crate) activity: Arc<crate::activity::ActivityTracker>,
    /// Hosted gcs mode: uploads go through the collector's fence and ack log.
    pub(crate) uploads: Option<crate::cdn_gc::Uploads>,
}

/// The CDN upload envelope: the routes' `DefaultBodyLimit`.
pub(crate) const ENVELOPE_BYTES: usize = 256 * 1024 * 1024;

/// Allowed extensions (prevent path traversal / weird filenames). Local raw-ID v1 persists these names, so the set is append-only.
const ALLOWED_EXTENSIONS: &[&str] = &[
    "png", "jpg", "jpeg", "gif", "webp", "bmp", "svg", "ico", "tiff", "json", "txt", "csv", "wav",
    "mp3", "mp4", "webm", "ogg", "pdf", "gz",
    // kymo's declared fallback for extensionless Resource filenames — served as application/octet-stream (download-only, no sniffing risk). Without it the client's own default was an unconditional 400.
    "bin",
];

fn validate_extension(ext: &str) -> bool {
    ALLOWED_EXTENSIONS.contains(&ext.to_lowercase().as_str())
}

/// Hosted key grammar: `<hex, ≥4 chars>.<allowed ext>`. The store owns key→location mapping
/// (filesystem fanout is a backend detail, not part of the key). Crate-visible: both stores
/// assert it on the keys they receive.
pub(crate) fn validate_hosted_key(key: &str) -> bool {
    let Some((hash, ext)) = key.rsplit_once('.') else {
        return false;
    };
    hash.len() >= 4 && hash.chars().all(|c| c.is_ascii_hexdigit()) && validate_extension(ext)
}

/// The hosted key grammar as an RE2 pattern, for ClickHouse's `match` (the CDN collector's root filter).
pub(crate) fn hosted_key_pattern() -> String {
    format!(
        "^[0-9A-Fa-f]{{4,}}\\.(?i:{})$",
        ALLOWED_EXTENSIONS.join("|")
    )
}

/// Local raw-ID v1 refines the hosted grammar: exactly a full SHA-256 hash, nothing uppercase —
/// so case-insensitive filesystems such as default APFS cannot alias two textual IDs to one object.
fn validate_local_key(key: &str) -> bool {
    // A hosted-valid key has exactly one dot, so everything before the first is the hash.
    let hash_len = key.find('.').unwrap_or(key.len());
    validate_hosted_key(key) && hash_len == 64 && !key.bytes().any(|b| b.is_ascii_uppercase())
}

/// The upload route's key for `body`: its SHA-256 in hex, then the extension.
pub(crate) fn content_key(body: &[u8], ext: &str) -> String {
    format!("{}.{ext}", hex::encode(Sha256::digest(body)))
}

/// POST /cdn/upload
///
/// Accepts raw binary body with `X-Extension` header (e.g., "png").
/// Returns JSON: {"resource_id": "abcdef1234.png"}
pub async fn upload(
    State(state): State<Arc<CdnState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let ext = headers
        .get("x-extension")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("bin")
        .to_lowercase();

    if !validate_extension(&ext) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Unsupported extension: {ext}"),
        ));
    }

    let key = content_key(&body, &ext);
    // The store logs stored/dedup itself; this is the one log line per failed upload.
    let stored = match &state.uploads {
        Some(uploads) => uploads.put(&state.store, &key, body).await,
        None => state.store.put_if_absent(&key, body).await,
    };
    stored.map_err(|e| {
        let error = format!("{e:#}");
        tracing::warn!(key = %key, error = %error, "CDN upload failed");
        (StatusCode::INTERNAL_SERVER_ERROR, error)
    })?;

    Ok(axum::Json(serde_json::json!({ "resource_id": key })))
}

/// Parse a Range header into the single-range forms the store understands. `None` means serve
/// the full representation with 200 — which RFC 9110 makes the correct handling for every case
/// a server may decline: no header, multi-range specs, malformed specs (including inverted
/// bounds — an invalid Range header is IGNORED, never an error), and `If-Range` (we emit no
/// validators, so no client-held validator can match; RFC: ignore Range then).
fn parse_range_header(method: &axum::http::Method, headers: &HeaderMap) -> Option<ByteRange> {
    // Range is defined for GET only (RFC 9110 §14.2); axum routes HEAD through the same
    // handlers, and a HEAD must carry the full representation's headers.
    if method != axum::http::Method::GET || headers.contains_key(header::IF_RANGE) {
        return None;
    }
    // Repeated Range field lines combine into a multi-range list (RFC 9110 §5.5) — declined
    // like any other multi-range.
    let mut values = headers.get_all(header::RANGE).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    let value = value.to_str().ok()?;
    // The range unit is case-insensitive.
    if value.len() < 6 || !value[..6].eq_ignore_ascii_case("bytes=") {
        return None;
    }
    let spec = value[6..].trim();
    if spec.contains(',') {
        return None;
    }
    // RFC positions are bare DIGITs — u64::parse would also admit a leading '+'.
    let digits = |s: &str| {
        (!s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
            .then(|| s.parse::<u64>().ok())
            .flatten()
    };
    let (first, last) = spec.split_once('-')?;
    match (first.is_empty(), last.is_empty()) {
        (true, false) => Some(ByteRange::Suffix(digits(last)?)),
        (false, true) => Some(ByteRange::From(digits(first)?)),
        (false, false) => {
            let first = digits(first)?;
            let last = digits(last)?;
            if last < first {
                return None;
            }
            // The RFC's last-byte-pos is inclusive; the store's Bounded end is exclusive. A
            // u64::MAX last-byte-pos can't be represented exclusively — it means "through the
            // end", which is exactly the open-ended form (the store clamps ends past EOF, so
            // the semantics are identical for every real object).
            if last == u64::MAX {
                return Some(ByteRange::From(first));
            }
            Some(ByteRange::Bounded(first, last + 1))
        }
        (true, true) => None,
    }
}

/// GET /cdn/:key
///
/// Serves the file with appropriate Content-Type and immutable caching.
pub async fn serve(
    State(state): State<Arc<CdnState>>,
    Path(key): Path<String>,
    method: axum::http::Method,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    if !validate_hosted_key(&key) {
        return Err((StatusCode::BAD_REQUEST, "Invalid key format".to_string()));
    }
    let range = parse_range_header(&method, &headers);
    serve_from_store(&state, key, "public, max-age=31536000, immutable", range).await
}

/// Local browser GET keeps the hosted content identity and MIME behavior but does not let a browser cache outlive the local stack's current contents.
pub async fn serve_local(
    State(state): State<Arc<CdnState>>,
    Path(key): Path<String>,
    method: axum::http::Method,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    if !validate_local_key(&key) {
        return Err((StatusCode::NOT_FOUND, "Not found".to_string()));
    }
    let range = parse_range_header(&method, &headers);
    serve_from_store(&state, key, "private, no-store", range).await
}

async fn serve_from_store(
    state: &CdnState,
    key: String,
    cache_control: &'static str,
    range: Option<ByteRange>,
) -> Result<Response, (StatusCode, String)> {
    // Streaming, not buffered: a whole-object read competes with the series cache and ingest
    // buffers inside the pod's memory limit (docs/cdn-gcs-migration.md).
    let read = match state.store.get(&key, range).await {
        Ok(read) => read,
        Err(StoreError::NotFound) => {
            // The CDN collector's alarm (docs/cdn-gcs-migration.md § Garbage collection); the key is what a restore from soft delete needs.
            metrics::counter!("mkdb2_cdn_not_found_total").increment(1);
            tracing::info!(key = %key, "CDN object not found");
            return Err((StatusCode::NOT_FOUND, "Not found".to_string()));
        }
        Err(StoreError::RangeNotSatisfiable { total_len }) => {
            return Ok(Response::builder()
                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                .header(header::CONTENT_RANGE, format!("bytes */{total_len}"))
                .header(header::ACCEPT_RANGES, "bytes")
                .header(header::CACHE_CONTROL, cache_control)
                .body(Body::empty())
                .unwrap());
        }
        // The store's messages are already user-shaped ("Failed to read file: …" etc.).
        Err(StoreError::Other(e)) => {
            tracing::warn!(key = %key, error = %e, "CDN read failed");
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
        }
    };
    // A ranged read serves 206 with Content-Range — except an EMPTY satisfiable read (non-zero
    // suffix on an empty object), which 206 cannot express and which IS the entire
    // representation: plain 200 (see StoreRead).
    let ranged = range.is_some() && read.read_len > 0;
    let mut response = Response::builder()
        .status(if ranged {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        })
        .header(
            header::CONTENT_TYPE,
            mime_guess::from_path(&key)
                .first_or_octet_stream()
                .to_string(),
        )
        // Explicit Content-Length: a stream body would otherwise switch responses to chunked
        // transfer, and clients (and tests) rely on the header being present.
        .header(header::CONTENT_LENGTH, read.read_len)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CACHE_CONTROL, cache_control);
    if ranged {
        response = response.header(
            header::CONTENT_RANGE,
            format!(
                "bytes {}-{}/{}",
                read.start,
                read.start + read.read_len - 1,
                read.total_len
            ),
        );
    }
    // The middleware's WorkGuard drops when the response HEAD is built, but the body is lazy:
    // hold a guard inside the stream so the local idle-shutdown supervisor sees the transfer
    // as in-flight work until the body completes or is dropped (no-op in hosted mode).
    let guard = state.activity.begin_work();
    let stream = futures::TryStreamExt::inspect_err(read.stream, move |error| {
        let _ = &guard;
        // Mid-body failures are the read path's other sink (the store counts them; this logs them).
        tracing::warn!(key = %key, error = %error, "CDN read failed mid-body");
    });
    Ok(response.body(Body::from_stream(stream)).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdn_store::FsStore;

    fn state(root: &std::path::Path) -> Arc<CdnState> {
        Arc::new(CdnState {
            store: CdnStore::Fs(FsStore::new(root.to_path_buf())),
            activity: crate::activity::ActivityTracker::disabled(),
            uploads: None,
        })
    }

    #[test]
    fn local_resource_ids_are_strict_while_hosted_compatibility_is_retained() {
        let hash = "a".repeat(64);
        assert!(validate_local_key(&format!("{hash}.png")));
        assert!(validate_local_key(&format!("{hash}.bin")));
        for key in [
            format!("{}.png", "a".repeat(63)),
            format!("{}.png", "A".repeat(64)),
            format!("{hash}.PNG"),
            format!("{hash}.exe"),
        ] {
            assert!(!validate_local_key(&key), "accepted {key}");
        }
        assert!(validate_hosted_key("abcd.png"));
    }

    async fn do_serve(
        state: &Arc<CdnState>,
        key: &str,
        method: axum::http::Method,
        headers: HeaderMap,
    ) -> Result<Response, (StatusCode, String)> {
        serve(State(state.clone()), Path(key.to_string()), method, headers).await
    }

    async fn ranged(state: &Arc<CdnState>, key: &str, range: &str) -> Response {
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, range.parse().unwrap());
        do_serve(state, key, axum::http::Method::GET, headers)
            .await
            .unwrap()
    }

    async fn body_bytes(response: Response) -> Vec<u8> {
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec()
    }

    #[tokio::test]
    async fn range_requests_get_206_416_or_are_ignored_per_rfc() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let key = "aabbccdd0123456789aabbccdd0123456789aabbccdd0123456789aabbccdd01.txt";
        state
            .store
            .put_if_absent(key, axum::body::Bytes::from_static(b"0123456789"))
            .await
            .unwrap();

        // Single satisfiable range: 206 + Content-Range + windowed Content-Length.
        let response = ranged(&state, key, "bytes=2-5").await;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes 2-5/10");
        assert_eq!(response.headers()[header::CONTENT_LENGTH], "4");
        assert_eq!(response.headers()[header::ACCEPT_RANGES], "bytes");
        assert_eq!(body_bytes(response).await, b"2345");

        // Suffix and open-ended forms; end-past-EOF clamps into a valid 206.
        let response = ranged(&state, key, "bytes=-3").await;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes 7-9/10");
        assert_eq!(body_bytes(response).await, b"789");
        let response = ranged(&state, key, "bytes=8-").await;
        assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes 8-9/10");
        let response = ranged(&state, key, "bytes=8-99").await;
        assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes 8-9/10");

        // Unsatisfiable: 416 with the star form.
        let response = ranged(&state, key, "bytes=10-12").await;
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes */10");
        let response = ranged(&state, key, "bytes=-0").await;
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);

        // Declined forms are IGNORED (200 full body), never errors: multi-range, inverted,
        // non-bytes units, garbage, and If-Range (we emit no validators).
        for decline in [
            "bytes=0-1,3-4",
            "bytes=5-2",
            "items=0-1",
            "bytes=x-y",
            "bytes=-",
        ] {
            let response = ranged(&state, key, decline).await;
            assert_eq!(response.status(), StatusCode::OK, "for {decline}");
            assert_eq!(body_bytes(response).await, b"0123456789", "for {decline}");
        }
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, "bytes=2-5".parse().unwrap());
        headers.insert(header::IF_RANGE, "\"some-etag\"".parse().unwrap());
        let response = do_serve(&state, key, axum::http::Method::GET, headers)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // The 206-inexpressible case: a satisfiable-but-empty read (suffix on an empty object)
        // serves the entire (empty) representation as a plain 200.
        let empty_key = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855.gz";
        state
            .store
            .put_if_absent(empty_key, axum::body::Bytes::new())
            .await
            .unwrap();
        let response = ranged(&state, empty_key, "bytes=-5").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_LENGTH], "0");
        assert!(body_bytes(response).await.is_empty());
        // ...while an absolute range on it is unsatisfiable.
        let response = ranged(&state, empty_key, "bytes=0-4").await;
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes */0");
    }

    #[tokio::test]
    async fn head_requests_ignore_range_and_parser_handles_grammar_edges() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let key = "aabbccdd0123456789aabbccdd0123456789aabbccdd0123456789aabbccdd01.txt";
        state
            .store
            .put_if_absent(key, axum::body::Bytes::from_static(b"0123456789"))
            .await
            .unwrap();

        // Range applies to GET only (RFC 9110 §14.2): a HEAD with Range carries the full
        // representation's headers.
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, "bytes=2-5".parse().unwrap());
        let response = do_serve(&state, key, axum::http::Method::HEAD, headers)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_LENGTH], "10");
        assert!(response.headers().get(header::CONTENT_RANGE).is_none());

        // The range unit is case-insensitive.
        let response = ranged(&state, key, "BYTES=2-5").await;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);

        // Bare-DIGIT positions only: u64::parse would admit '+', the RFC does not.
        let response = ranged(&state, key, "bytes=+2-5").await;
        assert_eq!(response.status(), StatusCode::OK);

        // A u64::MAX last-byte-pos means "through the end", never an ignored header.
        let response = ranged(&state, key, "bytes=8-18446744073709551615").await;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes 8-9/10");
        // ...including the degenerate MAX-MAX single-byte spec: a valid range that no real
        // object can satisfy — 416, never a 500 (and the 416 carries Cache-Control).
        let response = ranged(
            &state,
            key,
            "bytes=18446744073709551615-18446744073709551615",
        )
        .await;
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes */10");
        assert!(response.headers().contains_key(header::CACHE_CONTROL));

        // Repeated Range field lines combine into a multi-range list — declined, 200.
        let mut headers = HeaderMap::new();
        headers.append(header::RANGE, "bytes=0-1".parse().unwrap());
        headers.append(header::RANGE, "bytes=3-4".parse().unwrap());
        let response = do_serve(&state, key, axum::http::Method::GET, headers)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_bytes(response).await, b"0123456789");
    }

    #[tokio::test]
    async fn served_body_holds_in_flight_work_until_consumed_or_dropped() {
        let root = tempfile::tempdir().unwrap();
        let activity = crate::activity::ActivityTracker::new_local();
        let state = Arc::new(CdnState {
            store: CdnStore::Fs(FsStore::new(root.path().to_path_buf())),
            activity: activity.clone(),
            uploads: None,
        });
        let key = "aabbccdd0123456789aabbccdd0123456789aabbccdd0123456789aabbccdd01.txt";
        state
            .store
            .put_if_absent(key, axum::body::Bytes::from_static(b"0123456789"))
            .await
            .unwrap();

        // The response HEAD alone must count as in-flight work: the body is lazy, and the local
        // idle-shutdown supervisor would otherwise see zero work mid-transfer.
        let response = do_serve(&state, key, axum::http::Method::GET, HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(activity.snapshot().in_flight_work, 1);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], b"0123456789");
        assert_eq!(activity.snapshot().in_flight_work, 0);

        // A client disconnect drops the body without polling it to completion — the guard must release then too.
        let response = do_serve(&state, key, axum::http::Method::GET, HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(activity.snapshot().in_flight_work, 1);
        drop(response);
        assert_eq!(activity.snapshot().in_flight_work, 0);
    }

    #[tokio::test]
    async fn empty_gzip_resources_store_deduplicate_and_serve() {
        let root = tempfile::tempdir().unwrap();
        let state = state(root.path());
        let mut headers = HeaderMap::new();
        headers.insert("x-extension", "gz".parse().unwrap());
        let key = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855.gz";

        let missing = do_serve(&state, key, axum::http::Method::GET, HeaderMap::new())
            .await
            .unwrap_err();
        assert_eq!(missing.0, StatusCode::NOT_FOUND);

        for _ in 0..2 {
            let response = upload(
                State(state.clone()),
                headers.clone(),
                axum::body::Bytes::new(),
            )
            .await
            .unwrap()
            .into_response();
            assert_eq!(response.status(), StatusCode::OK);
        }

        // The filesystem backend keeps the two-level fanout layout.
        let path = root.path().join("e3").join("b0").join(key);
        assert_eq!(tokio::fs::read(path).await.unwrap(), Vec::<u8>::new());

        let response = do_serve(&state, key, axum::http::Method::GET, HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/gzip");
        assert_eq!(response.headers()[header::CONTENT_LENGTH], "0");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(body.is_empty());
    }
}
