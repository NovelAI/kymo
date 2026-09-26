//! CDN blob storage backends behind one dispatch surface (docs/cdn-gcs-migration.md).
//!
//! Realized as a closed enum rather than a trait: the backend set is closed (filesystem, the
//! default in both modes, and opt-in GCS) and enum dispatch keeps async methods dyn-safe without
//! an async_trait dependency.
//! Routes validate keys (hosted vs local grammars differ); the store receives validated keys.

use std::io::SeekFrom;
use std::path::PathBuf;
use std::pin::Pin;

use axum::body::Bytes;
use futures::stream::Stream;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

mod gcs;
#[cfg(test)]
pub(crate) use gcs::test_support as gcs_test_support;
pub use gcs::GcsStore;

/// Register the CDN error counters the alerts read, at zero, at hosted startup in both modes:
/// `increase()` needs a prior sample, so a series born at its first increment is invisible to
/// the rule that exists for it. The per-class last-error gauges need no registration (their only
/// reader treats 0 and absent alike).
pub fn register_cdn_metrics() {
    for class in gcs::GCS_ERROR_CLASSES {
        metrics::counter!("mkdb2_cdn_gcs_errors_total", "class" => class).absolute(0);
    }
}

/// RFC 9110 §14.1.2 byte-range forms. `Bounded`/`From` are unsatisfiable when start is at/past
/// the representation length (a zero-length representation satisfies NO absolute range); a
/// non-zero `Suffix` is always satisfiable (clamped to the representation, empty included).
/// Callers construct these only from a successfully parsed Range header, so `Bounded` requires
/// end > start (the RFC's inclusive last-byte-pos admits no empty range) — a zero-width or
/// inverted `Bounded` is a caller bug, reported as `Other`, never a 416.
#[derive(Debug, Clone, Copy)]
pub enum ByteRange {
    /// start..end, end exclusive (already +1 from the RFC's inclusive last-byte-pos).
    Bounded(u64, u64),
    From(u64),
    Suffix(u64),
}

#[derive(Debug)]
pub enum StoreError {
    NotFound,
    /// A legal-but-unsatisfiable range — the Range route (`cdn.rs`) maps this to 416 with
    /// `Content-Range: bytes */{total_len}`.
    RangeNotSatisfiable {
        total_len: u64,
    },
    Other(anyhow::Error),
}

impl From<anyhow::Error> for StoreError {
    fn from(e: anyhow::Error) -> Self {
        StoreError::Other(e)
    }
}

pub type ByteStream = Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send>>;

/// A satisfiable ranged read can still be empty (a non-zero `Suffix` on an empty object); 206
/// `Content-Range` cannot express an empty window, so the Range route (`cdn.rs`) must downgrade such a
/// read to a plain 200 — never transcribe it into a 206.
pub struct StoreRead {
    /// Resolved first byte offset of this read (for `Content-Range: bytes start-…`).
    pub start: u64,
    /// Length of the bytes this read will yield (the range length for ranged reads).
    pub read_len: u64,
    /// Full object length, for Content-Range synthesis on ranged reads.
    pub total_len: u64,
    pub stream: ByteStream,
}

/// `GcsStore` sits in an `Arc` so the inventory-gauge task can hold it without borrowing the
/// state tree.
pub enum CdnStore {
    Fs(FsStore),
    Gcs(std::sync::Arc<GcsStore>),
}

impl CdnStore {
    /// Store `body` under `key`, or ack bare if the key already exists — content-addressed keys
    /// make the existing bytes identical by construction, so dedup needs no verification read
    /// (docs/cdn-gcs-migration.md).
    pub async fn put_if_absent(&self, key: &str, body: Bytes) -> anyhow::Result<()> {
        match self {
            CdnStore::Fs(s) => s.put_if_absent(key, &body).await,
            CdnStore::Gcs(s) => s.put_if_absent(key, body).await,
        }
    }

    pub async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<StoreRead, StoreError> {
        match self {
            CdnStore::Fs(s) => s.get(key, range).await,
            CdnStore::Gcs(s) => s.get(key, range).await,
        }
    }
}

/// RFC 9110 range resolution against a representation length — the single rule for every
/// backend (FsStore resolves with it directly; GcsStore re-resolves with it when a remote or
/// crate-level range rejection needs classifying). Returns `(start, read_len)`.
fn resolve_range(range: Option<ByteRange>, total_len: u64) -> Result<(u64, u64), StoreError> {
    match range {
        None => Ok((0, total_len)),
        // A zero-width or inverted bound cannot come from a valid RFC parse (see ByteRange) — caller bug, not a 416.
        Some(ByteRange::Bounded(s, e)) if s >= e => {
            Err(anyhow::anyhow!("malformed range {s}..{e}").into())
        }
        // Absolute ranges are unsatisfiable when start is at/past the representation length — including the zero-length representation, which NO absolute range matches (RFC 9110 §14.1.2).
        Some(ByteRange::Bounded(s, _) | ByteRange::From(s)) if s >= total_len => {
            Err(StoreError::RangeNotSatisfiable { total_len })
        }
        Some(ByteRange::Bounded(s, e)) => Ok((s, e.min(total_len) - s)),
        Some(ByteRange::From(s)) => Ok((s, total_len - s)),
        Some(ByteRange::Suffix(0)) => {
            // RFC 9110: a zero suffix-length is never satisfiable.
            Err(StoreError::RangeNotSatisfiable { total_len })
        }
        Some(ByteRange::Suffix(n)) => {
            let n = n.min(total_len);
            Ok((total_len - n, n))
        }
    }
}

pub struct FsStore {
    root: PathBuf,
}

async fn sync_directory(path: &std::path::Path) -> std::io::Result<()> {
    fs::File::open(path).await?.sync_all().await
}

impl FsStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// `{root}/{hash[0:2]}/{hash[2:4]}/{key}` — the fanout is a filesystem-inode artifact and
    /// does not exist in other backends (GCS names are flat, per the design doc).
    fn path_for(&self, key: &str) -> PathBuf {
        // The routes' grammar is the one rule; assert it so the contract is enforced, not
        // folklore (a valid hosted key has ≥4 hex chars before its single dot, making the
        // slicing below safe).
        debug_assert!(
            crate::cdn::validate_hosted_key(key),
            "store received an unvalidated key: {key}"
        );
        let hash = key.split('.').next().unwrap_or(key);
        self.root.join(&hash[..2]).join(&hash[2..4]).join(key)
    }

    async fn put_if_absent(&self, key: &str, body: &[u8]) -> anyhow::Result<()> {
        let path = self.path_for(key);
        // Both parents exist by construction: path_for always returns root/xx/yy/key.
        let dir = path.parent().unwrap();
        let first_level = dir.parent().unwrap();

        // Opportunistic GC of orphaned temps in this shard dir: the Drop guard below can't run through SIGTERM's process::exit (deploys!) or a hard kill, and the store has no other GC. Stale = older than an hour; no live upload holds a temp that long. Runs on EVERY upload, dedup hits included — an orphan's own retry completes, takes the dedup branch forever after, and would otherwise never revisit this shard (cold shards see unrelated new content ~once per 65k uploads). Shard dirs are content-hash fanout, so the readdir is a handful of entries.
        if let Ok(mut rd) = fs::read_dir(dir).await {
            while let Ok(Some(ent)) = rd.next_entry().await {
                if ent.file_name().to_string_lossy().ends_with(".tmp")
                    && ent
                        .metadata()
                        .await
                        .ok()
                        .and_then(|m| m.modified().ok())
                        .and_then(|m| m.elapsed().ok())
                        .is_some_and(|age| age.as_secs() > 3600)
                {
                    let _ = fs::remove_file(ent.path()).await;
                }
            }
        }

        // Dedup: if file exists, skip write
        if !path.exists() {
            fs::create_dir_all(dir)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create dir: {e}"))?;
            // Persist new fanout entries before publishing an object beneath them. Sync bottom-up: the shard entry lives in `first_level`, whose own entry lives in `root`. Always repeat both syncs: if a prior attempt created the directories but failed one sync, this attempt must heal that durability gap before writing and acknowledging an object.
            for parent in [first_level, self.root.as_path()] {
                sync_directory(parent)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to sync CDN fanout: {e}"))?;
            }

            // Write to a temp name and rename into place: this dedup branch trusts bare existence forever, so a crash or disk-full mid-write must never leave a truncated file at the final content-addressed path (readers also never observe partial content). Concurrent uploads of the same content race benignly — both temps hold identical bytes and rename atomically replaces.
            // Counter, not wall clock: two tasks storing identical content in the same clock tick would share one tmp path — and File::create truncates, so their interleaved writes could be promoted to the trusted final name. The counter is unique for the process's lifetime; the pid keeps a restarted server clear of a predecessor's orphans.
            static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let tmp = dir.join(format!(
                "{key}.{}.{}.tmp",
                std::process::id(),
                TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            // Drop-guard removes the temp on ANY exit that didn't rename it — errors, but also cancellation (the uploader's client timeout dropping the connection while the write stalls on a slow volume), which skips ordinary error handling entirely. The store has no GC, so an unguarded temp would sit forever. Only a hard kill mid-write can still leak one.
            struct TmpGuard(Option<std::path::PathBuf>);
            impl Drop for TmpGuard {
                fn drop(&mut self) {
                    if let Some(p) = self.0.take() {
                        let _ = std::fs::remove_file(p);
                    }
                }
            }
            let mut guard = TmpGuard(Some(tmp.clone()));
            let write = async {
                let mut file = fs::File::create(&tmp).await?;
                file.write_all(body).await?;
                // sync before rename: without it a host crash can promote an empty/partial inode to the trusted final name.
                file.sync_all().await?;
                drop(file);
                fs::rename(&tmp, &path).await
            }
            .await;
            if let Err(e) = write {
                return Err(anyhow::anyhow!("Failed to store file: {e}"));
            }
            guard.0 = None; // renamed into place — nothing to clean up
            tracing::info!(key = %key, size = body.len(), "CDN resource stored");
        } else {
            tracing::debug!(key = %key, "CDN resource already exists (dedup)");
        }

        // The final name is durable only after its containing directory is synced. Do this on dedup hits too: a retry after rename succeeded but this sync failed must repair durability before it can acknowledge the resource ID.
        sync_directory(dir)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to sync CDN shard: {e}"))?;
        Ok(())
    }

    async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<StoreRead, StoreError> {
        let path = self.path_for(key);
        let mut file = match fs::File::open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(StoreError::NotFound),
            Err(e) => return Err(anyhow::anyhow!("Failed to read file: {e}").into()),
        };
        let total_len = file
            .metadata()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read file: {e}"))?
            .len();
        let (start, read_len) = resolve_range(range, total_len)?;
        if start > 0 {
            file.seek(SeekFrom::Start(start))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to read file: {e}"))?;
        }
        // Hand-rolled file→stream (no tokio-util dependency), ≤64KiB chunks. `Take` owns the
        // range ceiling, so a ranged read never yields past its window; `read_exact` makes a
        // short object loud — shrinking under us is impossible under write-once, but never lie:
        // a clean end would hand direct consumers a silently truncated body (this way it
        // surfaces as UnexpectedEof).
        let stream = futures::stream::try_unfold(file.take(read_len), |mut reader| async move {
            let chunk_len = reader.limit().min(64 * 1024) as usize;
            if chunk_len == 0 {
                return Ok(None);
            }
            let mut buf = vec![0u8; chunk_len];
            reader.read_exact(&mut buf).await?;
            Ok(Some((Bytes::from(buf), reader)))
        });
        Ok(StoreRead {
            start,
            read_len,
            total_len,
            stream: Box::pin(stream),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    use crate::cdn_store::gcs_test_support::{assert_range_contract, collect, EMPTY_KEY, KEY};

    #[tokio::test]
    async fn fs_store_dedups_and_honors_the_shared_range_contract() {
        let root = tempfile::tempdir().unwrap();
        let store = FsStore::new(root.path().to_path_buf());
        store.put_if_absent(KEY, b"0123456789").await.unwrap();
        // Dedup trusts bare existence: a second put under the same key must ack without
        // touching the stored bytes (deliberately different bytes here to make an overwrite
        // visible — content-addressing forbids this input in production). The contract's full
        // read then pins that the original bytes survived.
        store.put_if_absent(KEY, b"XXXXXXXXXX").await.unwrap();
        store.put_if_absent(EMPTY_KEY, b"").await.unwrap();
        assert_range_contract(&CdnStore::Fs(store)).await;
    }

    #[tokio::test]
    async fn multi_chunk_reads_reassemble_exactly() {
        let root = tempfile::tempdir().unwrap();
        let store = FsStore::new(root.path().to_path_buf());
        let key = "aabbccdd0123456789aabbccdd0123456789aabbccdd0123456789aabbccdd01.bin";
        // Spans several 64KiB chunks, with a period-251 pattern so any misjoined boundary shows.
        let body: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        store.put_if_absent(key, &body).await.unwrap();

        let full = store.get(key, None).await.unwrap();
        assert_eq!(collect(full).await, body);

        // A window whose start and end both fall mid-chunk.
        let mid = store
            .get(key, Some(ByteRange::Bounded(60_001, 190_003)))
            .await
            .unwrap();
        assert_eq!((mid.start, mid.read_len), (60_001, 130_002));
        assert_eq!(collect(mid).await, &body[60_001..190_003]);
    }

    #[tokio::test]
    async fn truncation_under_a_reader_is_an_error_not_a_clean_end() {
        let root = tempfile::tempdir().unwrap();
        let store = FsStore::new(root.path().to_path_buf());
        let key = "aabbccdd0123456789aabbccdd0123456789aabbccdd0123456789aabbccdd01.txt";
        store.put_if_absent(key, b"0123456789").await.unwrap();

        let read = store.get(key, None).await.unwrap();
        // Shrink the object under the open reader (write-once forbids this in production; the
        // stream must fail loudly rather than return a truncated body as complete).
        let path = root.path().join("aa").join("bb").join(key);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(3)
            .unwrap();
        let err = read.stream.try_collect::<Vec<Bytes>>().await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }
}
