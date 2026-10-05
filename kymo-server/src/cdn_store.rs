//! CDN blob storage backends behind one dispatch surface (docs/cdn-gcs-migration.md).
//!
//! Realized as a closed enum rather than a trait: the backend set is closed (filesystem, the
//! default in both modes, and opt-in GCS) and enum dispatch keeps async methods dyn-safe without
//! an async_trait dependency.
//! The hosted routes validate the hosted key grammar; `FsStore::get` checks the local one, so
//! every filesystem serves one spelling of a key, as GCS does.

use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use futures::stream::Stream;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

mod gcs;
#[cfg(test)]
pub(crate) use gcs::test_support as gcs_test_support;
pub use gcs::GcsStore;
pub(crate) use gcs::{count_gcs_error, credentialed_builder, Identity};

/// Register the CDN counters that `increase()` reads (the GCS error classes the alerts read, and the 404 counter), at zero, at hosted startup in both modes: `increase()` needs a prior sample, so a series born at its first increment is invisible to the rule that exists for it. The per-class last-error gauges need no registration (their only reader treats 0 and absent alike).
pub fn register_cdn_metrics() {
    for class in gcs::GCS_ERROR_CLASSES {
        metrics::counter!("mkdb2_cdn_gcs_errors_total", "class" => class).absolute(0);
    }
    metrics::counter!("mkdb2_cdn_not_found_total").absolute(0);
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

/// How a successful `put_if_absent` acked. `Existing` is a dedup hit, which leaves the object's creation time old: with GCS the collector learns of it from its ack log, and the filesystem store marks the file itself (docs/cdn-gcs-migration.md § Garbage collection).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    Created,
    Existing,
}

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

pub enum CdnStore {
    Fs(FsStore),
    Gcs(GcsStore),
}

impl CdnStore {
    /// Store `body` under `key`, trusting existing content on a dedup hit: content-addressed keys
    /// make the bytes identical by construction, so dedup needs no verification read
    /// (docs/cdn-gcs-migration.md). A filesystem hit refreshes a stale mark, or rewrites a file it
    /// can't mark.
    pub async fn put_if_absent(&self, key: &str, body: Bytes) -> anyhow::Result<PutOutcome> {
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

#[derive(Clone)]
pub struct FsStore {
    root: PathBuf,
    /// `MARK_INTERVAL`, shorter only in tests: nothing can backdate a ctime.
    mark_interval: Duration,
}

/// A dedup hit moves a file's ctime only when it is older than this, so a ctime can trail the last upload by this much and the collector adds it to its grace (cdn_gc.rs).
pub(crate) const MARK_INTERVAL: Duration = Duration::from_secs(24 * 3600);

/// Whether `path` exists with a fresh mark, after refreshing a stale one.
///
/// The collector's grace counts from a file's ctime, so a dedup hit must move it: a re-upload's reference can arrive later. Unlike mtime, no file-level copy or restore can carry ctime backwards. A refreshed mark is synced before the upload is acked.
///
/// `false` means absent, or present but unmarkable (unreadable, not writable, or a mount that ignores the touch); the caller then rewrites the file, which marks the new inode.
async fn marked(path: &Path, interval: Duration) -> bool {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::OpenOptionsExt;
        // Only a regular file can carry the mark; anything else is left to the rewrite, which replaces it: opening a FIFO would block, and a symlink's target isn't the key's file.
        if !std::fs::symlink_metadata(&path).is_ok_and(|meta| meta.is_file()) {
            return false;
        }
        // Not through a symlink swapped in since the lstat.
        let Ok(file) = std::fs::File::options()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
        else {
            return false;
        };
        let fresh = || {
            file.metadata()
                .is_ok_and(|meta| changed_within(&meta, interval))
        };
        if fresh() {
            return true;
        }
        // Null times: the kernel stamps the current time itself, which needs only write permission (explicit times need ownership).
        let touched = unsafe { libc::futimens(file.as_raw_fd(), std::ptr::null()) } == 0;
        touched && fresh() && file.sync_all().is_ok()
    })
    .await
    .unwrap_or(false)
}

/// Whether `meta`'s ctime is within `window` of now (a future ctime counts).
fn changed_within(meta: &std::fs::Metadata, window: Duration) -> bool {
    use std::os::unix::fs::MetadataExt;
    // A ctime before 1970 is stale, so the mark refreshes it.
    let Ok(seconds) = u64::try_from(meta.ctime()) else {
        return false;
    };
    let changed = UNIX_EPOCH + Duration::new(seconds, meta.ctime_nsec() as u32);
    SystemTime::now()
        .duration_since(changed)
        .map_or(true, |age| age < window)
}

/// One entry of a collector listing (the bucket's, or [`FsStore::walk`]).
pub(crate) enum Listed {
    /// An object a viewer can open by its key. `created` starts its grace: GCS last-modified, or a file's ctime.
    Object {
        key: String,
        size: u64,
        created: u32,
    },
    /// Outside the key grammar; counted, never collected.
    Foreign { size: u64 },
    /// Whatever it leads to is beyond the inventory, so it disarms deletion.
    Symlink,
}

/// One level of [`FsStore::walk`]; `prefix` is the shard names above it, so `"abcd"` lists files.
fn walk_level(
    dir: &Path,
    prefix: &str,
    each: &mut dyn FnMut(Listed) -> std::io::Result<()>,
) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let meta = match entry.metadata() {
            Ok(meta) => meta,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let listed = match entry.file_name().into_string() {
            _ if meta.is_symlink() => Listed::Symlink,
            Ok(shard)
                if prefix.len() < 4
                    && meta.is_dir()
                    && shard.len() == 2
                    && shard.bytes().all(|b| b.is_ascii_hexdigit()) =>
            {
                walk_level(&entry.path(), &format!("{prefix}{shard}"), each)?;
                continue;
            }
            Ok(key)
                if prefix.len() == 4
                    && meta.is_file()
                    && crate::cdn::validate_local_key(&key)
                    && key[..4] == *prefix =>
            {
                Listed::Object {
                    created: meta.ctime().clamp(0, u32::MAX.into()) as u32,
                    size: meta.len(),
                    key,
                }
            }
            _ => Listed::Foreign {
                size: if meta.is_file() { meta.len() } else { 0 },
            },
        };
        each(listed)?;
    }
    Ok(())
}

async fn sync_directory(path: &std::path::Path) -> std::io::Result<()> {
    fs::File::open(path).await?.sync_all().await
}

impl FsStore {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            mark_interval: MARK_INTERVAL,
        }
    }

    /// `{root}/{hash[0:2]}/{hash[2:4]}/{key}` — the fanout is a filesystem-inode artifact and
    /// does not exist in other backends (GCS names are flat, per the design doc).
    pub(crate) fn path_for(&self, key: &str) -> PathBuf {
        // The routes' grammar is the one rule; assert it so the contract is enforced, not
        // folklore (a valid hosted key has ≥4 hex chars before its single dot, making the
        // slicing below safe).
        debug_assert!(
            crate::cdn::validate_hosted_key(key),
            "store received an unvalidated key: {key}"
        );
        self.root.join(&key[..2]).join(&key[2..4]).join(key)
    }

    /// Walks the fanout for the collector's inventory, without following symlinks (`DirEntry::metadata` is an lstat). An object is a regular file with a canonical name (`validate_local_key`, which every uploaded key meets) at exactly its `path_for`. A symlink anywhere it looks is reported as one; everything else, temps and files under any other name or path included, is foreign. An error from `each` stops the walk with it; files that vanish mid-walk are skipped, and a shard that vanishes fails it.
    pub(crate) fn walk(
        &self,
        each: &mut dyn FnMut(Listed) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        walk_level(&self.root, "", each)
    }

    async fn put_if_absent(&self, key: &str, body: &[u8]) -> anyhow::Result<PutOutcome> {
        let path = self.path_for(key);
        // Both parents exist by construction: path_for always returns root/xx/yy/key.
        let dir = path.parent().unwrap();
        let first_level = dir.parent().unwrap();

        // Sweep this shard's orphaned temps, older than an hour (no live upload holds one that long). The Drop guard below misses SIGTERM's process::exit and hard kills, and the collector never removes temps, which are outside the key grammar. Dedup hits sweep too: an orphan's own retry completes and takes the dedup branch from then on, while a cold shard sees new content about once per 65k uploads. A shard dir holds a handful of entries.
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

        // Dedup: an existing file is acked without a write once its mark is fresh; one that can't be marked is rewritten below.
        let outcome = if !marked(&path, self.mark_interval).await {
            fs::create_dir_all(dir)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create dir: {e}"))?;
            // Persist new fanout entries before publishing an object beneath them. Sync bottom-up: the shard entry lives in `first_level`, whose own entry lives in `root`. Always repeat both syncs: if a prior attempt created the directories but failed one sync, this attempt must heal that durability gap before writing and acknowledging an object.
            for parent in [first_level, self.root.as_path()] {
                sync_directory(parent)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to sync CDN fanout: {e}"))?;
            }

            // Write to a temp name and rename into place: the dedup branch never re-reads a stored file, so a crash or disk-full mid-write must never leave a truncated file at the final content-addressed path (readers also never observe partial content). Concurrent uploads of the same content race benignly — both temps hold identical bytes and rename atomically replaces.
            // Counter, not wall clock: two tasks storing identical content in the same clock tick would share one tmp path — and File::create truncates, so their interleaved writes could be promoted to the trusted final name. The counter is unique for the process's lifetime; the pid keeps a restarted server clear of a predecessor's orphans.
            static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let tmp = dir.join(format!(
                "{key}.{}.{}.tmp",
                std::process::id(),
                TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            // Drop-guard removes the temp on ANY exit that didn't rename it — errors, but also cancellation (the uploader's client timeout dropping the connection while the write stalls on a slow volume), which skips ordinary error handling entirely. Only a hard kill mid-write still leaves one, for the sweep above.
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
            PutOutcome::Created
        } else {
            tracing::debug!(key = %key, "CDN resource already exists (dedup)");
            PutOutcome::Existing
        };

        // The final name is durable only after its containing directory is synced. Do this on dedup hits too: a retry after rename succeeded but this sync failed must repair durability before it can acknowledge the resource ID.
        sync_directory(dir)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to sync CDN shard: {e}"))?;
        Ok(outcome)
    }

    async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<StoreRead, StoreError> {
        // Uploads store only canonical names, and a case-insensitive volume would otherwise open one under another spelling of its key. Refusing the rest keeps lookups case-exact on every filesystem, as on GCS, so the collector can match keys exactly.
        if !crate::cdn::validate_local_key(key) {
            return Err(StoreError::NotFound);
        }
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

    /// A mark is due by ctime, which a restore keeping mtime can't set back: a file whose mtime says 60 days is still fresh.
    #[test]
    fn a_restored_mtime_leaves_a_mark_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        std::fs::write(&path, "x").unwrap();
        let old = SystemTime::now() - Duration::from_secs(60 * 24 * 3600);
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(old))
            .unwrap();
        assert!(changed_within(
            &std::fs::metadata(&path).unwrap(),
            Duration::from_secs(3600)
        ));
    }

    #[tokio::test]
    async fn fs_store_dedups_and_honors_the_shared_range_contract() {
        let root = tempfile::tempdir().unwrap();
        let store = FsStore::new(root.path().to_path_buf());
        assert_eq!(
            store.put_if_absent(KEY, b"0123456789").await.unwrap(),
            PutOutcome::Created
        );
        // Dedup never re-reads the stored file: a second put under the same key must ack without
        // touching the stored bytes (deliberately different bytes here to make an overwrite
        // visible — content-addressing forbids this input in production). The contract's full
        // read then pins that the original bytes survived.
        assert_eq!(
            store.put_if_absent(KEY, b"XXXXXXXXXX").await.unwrap(),
            PutOutcome::Existing
        );
        store.put_if_absent(EMPTY_KEY, b"").await.unwrap();
        assert_range_contract(&CdnStore::Fs(store)).await;
    }

    #[tokio::test]
    async fn only_the_canonical_spelling_of_a_key_is_served() {
        let root = tempfile::tempdir().unwrap();
        let store = FsStore::new(root.path().to_path_buf());
        store.put_if_absent(KEY, b"0123456789").await.unwrap();
        assert!(store.get(KEY, None).await.is_ok());
        // A case-insensitive volume would open the file under either spelling; a case-sensitive one gets a file there too, which the guard must refuse as well.
        let (hash, ext) = KEY.split_once('.').unwrap();
        for other in [KEY.to_uppercase(), format!("{hash}.{}", ext.to_uppercase())] {
            let path = root
                .path()
                .join(&other[..2])
                .join(&other[2..4])
                .join(&other);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, b"0123456789").unwrap();
            assert!(
                matches!(store.get(&other, None).await, Err(StoreError::NotFound)),
                "{other}"
            );
        }
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

    fn ctime(path: &Path) -> (i64, i64) {
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::metadata(path).unwrap();
        (meta.ctime(), meta.ctime_nsec())
    }

    #[tokio::test]
    async fn a_dedup_hit_marks_a_stale_file_and_leaves_a_fresh_one() {
        use std::os::unix::fs::MetadataExt;
        let root = tempfile::tempdir().unwrap();
        let store = FsStore::new(root.path().to_path_buf());
        store.put_if_absent(KEY, b"0123456789").await.unwrap();
        let path = store.path_for(KEY);
        let created = ctime(&path);
        // Fresh within the interval: a dedup hit writes nothing.
        assert_eq!(
            store.put_if_absent(KEY, b"0123456789").await.unwrap(),
            PutOutcome::Existing
        );
        assert_eq!(ctime(&path), created);
        // Stale under a short interval: a dedup hit touches the file instead of writing it.
        let store = FsStore {
            mark_interval: Duration::from_millis(200),
            ..store
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        let inode = std::fs::metadata(&path).unwrap().ino();
        assert_eq!(
            store.put_if_absent(KEY, b"0123456789").await.unwrap(),
            PutOutcome::Existing
        );
        assert!(ctime(&path) > created);
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);
        // An absent file has nothing to mark, so the upload writes it.
        assert!(!marked(&root.path().join("absent"), MARK_INTERVAL).await);
        // A symlink isn't the key's file, even when what it leads to is fresh: it's replaced by the upload.
        std::fs::remove_file(&path).unwrap();
        let elsewhere = root.path().join("elsewhere");
        std::fs::write(&elsewhere, b"other bytes").unwrap();
        std::os::unix::fs::symlink(&elsewhere, &path).unwrap();
        assert_eq!(
            store.put_if_absent(KEY, b"0123456789").await.unwrap(),
            PutOutcome::Created
        );
        assert!(std::fs::symlink_metadata(&path).unwrap().is_file());
        assert_eq!(std::fs::read(&path).unwrap(), b"0123456789");
        assert_eq!(std::fs::read(&elsewhere).unwrap(), b"other bytes");
        // So is a FIFO, which opening would block on.
        std::fs::remove_file(&path).unwrap();
        let c_path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        assert_eq!(
            store.put_if_absent(KEY, b"0123456789").await.unwrap(),
            PutOutcome::Created
        );
        assert!(std::fs::symlink_metadata(&path).unwrap().is_file());
    }

    #[test]
    fn the_walk_lists_only_canonical_files_at_their_fanout_path() {
        use std::os::unix::ffi::OsStrExt;
        let root = tempfile::tempdir().unwrap();
        let store = FsStore::new(root.path().to_path_buf());
        let write = |path: &Path, body: &[u8]| {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, body).unwrap();
        };
        let canonical = format!("{}.png", "ab".repeat(32));
        let legacy = "abab.PNG";
        let linked = format!("{}.json", "ab".repeat(32));
        let misplaced = format!("{}.png", "cd".repeat(32));
        let shard = root.path().join("ab").join("ab");
        write(&store.path_for(&canonical), b"canonical");
        write(&store.path_for(legacy), b"legacy");
        std::os::unix::fs::symlink(store.path_for(&canonical), store.path_for(&linked)).unwrap();
        write(&shard.join(format!("{canonical}.1.2.tmp")), b"temp");
        write(&shard.join(&misplaced), b"misplaced");
        // APFS refuses a name that isn't UTF-8; Linux filesystems accept it.
        let non_utf8 =
            std::fs::write(shard.join(std::ffi::OsStr::from_bytes(b"\xff.png")), b"x").is_ok();
        std::fs::create_dir(shard.join(format!("{}.bin", "ab".repeat(32)))).unwrap();
        write(&root.path().join("README"), b"stray");
        std::fs::create_dir(root.path().join("zz")).unwrap();
        // A shard moved elsewhere and linked back.
        let moved = tempfile::tempdir().unwrap();
        write(
            &moved.path().join(format!("{}.png", "cd".repeat(32))),
            b"moved",
        );
        std::os::unix::fs::symlink(moved.path(), root.path().join("cd")).unwrap();

        let (mut objects, mut symlinks, mut foreign) = (Vec::new(), 0, 0);
        store
            .walk(&mut |entry| {
                match entry {
                    Listed::Object { key, .. } => objects.push(key),
                    Listed::Symlink => symlinks += 1,
                    Listed::Foreign { .. } => foreign += 1,
                }
                Ok(())
            })
            .unwrap();
        objects.sort();
        assert_eq!(objects, [canonical]);
        // The linked file and the linked shard, whose contents the walk doesn't enter.
        assert_eq!(symlinks, 2);
        // The legacy name, the temp, the misplaced file, the key-named directory, the two strays at the root, and the non-UTF-8 name.
        assert_eq!(foreign, 6 + usize::from(non_utf8));
    }
}
