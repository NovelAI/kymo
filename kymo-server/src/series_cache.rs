//! In-memory raw series: each entry holds a series' complete rows, (tag, step)-sorted, in immutable per-tag chunks.
//!
//! A refresh reads only rows stamped after the entry's `inserted_at` watermark, re-reading a short overlap ([`WATERMARK_OVERLAP_MS`]); ClickHouse's `idx_inserted_at` minmax index skips the cached history. Re-read rows identical to held ones keep their original stamps. New rows must extend their tag past its highest step; a rewrite, deletion or backfill below it refuses the merge, and the caller reads the series in full.
//!
//! Readers clone a snapshot's `Arc`, and later snapshots share its chunks: a refresh adds each tag's new rows as a chunk outside the cache's lock, so readers never block on a merge. Compaction ([`COMPACT_ROWS`]) keeps an append a few times its own rows amortized, copying fewer than 64k held rows per tag it extends.
//!
//! Only raw rows are cached; derived chart state and its append dependencies belong to `query/held.rs` and `query.rs`'s delta planner.

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hash};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use crate::clickhouse::VersionedRawPoint;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LineageOrigin {
    Miss,
    Rewrite,
    Late,
}

/// `v` at exactly its length, so a charge that follows capacity follows its contents: moved into a new allocation rather than shrunk in place, since an allocator may keep the larger block.
fn exact_vec<T>(v: Vec<T>) -> Vec<T> {
    if v.capacity() == v.len() {
        return v;
    }
    let mut exact = Vec::with_capacity(v.len());
    exact.extend(v);
    exact
}

/// One cached point; its tag lives on its [`Chunk`]. Full reads and merged increments only ever hold numeric rows, so no row carries the numeric flag.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CachedRow {
    pub(crate) step: i64,
    pub(crate) timestamp_ms: i64,
    pub(crate) inserted_ms: i64,
    pub(crate) value: f32,
}

impl CachedRow {
    fn of(row: &VersionedRawPoint) -> Self {
        Self {
            step: row.step,
            timestamp_ms: row.timestamp_ms,
            inserted_ms: row.inserted_ms,
            value: row.value,
        }
    }
}

/// A row's content hash: its tag, step, timestamp, value bits and stamp.
fn row_content(tag: &str, row: &CachedRow) -> u64 {
    static KEY: OnceLock<std::collections::hash_map::RandomState> = OnceLock::new();
    let CachedRow {
        step,
        timestamp_ms,
        inserted_ms,
        value,
    } = row;
    KEY.get_or_init(std::collections::hash_map::RandomState::new)
        .hash_one((tag, step, timestamp_ms, value.to_bits(), inserted_ms))
}

/// A run of one tag's rows in step order, immutable and shared by every snapshot that holds it.
#[derive(Clone, Debug)]
struct Chunk {
    /// One allocation per tag, shared by the tag's chunks (counted once in [`SeriesSnapshot::bytes`]).
    tag: Arc<str>,
    /// Never empty, and never reallocated once built: [`SeriesSnapshot::bytes`] charges its capacity.
    rows: Arc<Vec<CachedRow>>,
}

/// Chunk size. A full read fills chunks of this many rows. On a refresh, a tag's new rows absorb its trailing chunks, in one copy, while each holds fewer than this many rows and at most twice the rows gathered so far (the new rows and the chunks absorbed).
/// Chunks below it therefore more than halve from one to the next, so a tag holds at most `rows / COMPACT_ROWS + 15` chunks (15 = log2 of this). A held row is copied only into a chunk at least 1.5 times its own, so at most about log1.5(COMPACT_ROWS / its append's rows) times, and a refresh copies fewer than `2 * COMPACT_ROWS` held rows (2 MiB) per tag it extends, besides its own rows: up to T times that for a refresh extending T tags. The copy runs outside the cache's lock ([`SeriesCache::apply_increment`]).
const COMPACT_ROWS: usize = 1 << 15;

/// Upper bound on what the allocator holds for one live allocation of `bytes` (none for an empty buffer, which allocates nothing): glibc's on Linux (the pod, local mode's manylinux wheel), macOS's on macOS (local mode, development).
pub(crate) const fn buffer_bytes(bytes: usize) -> usize {
    if bytes == 0 {
        0
    } else if cfg!(target_os = "macos") {
        macos_allocation_bytes(bytes)
    } else {
        glibc_allocation_bytes(bytes)
    }
}

/// glibc malloc: a chunk of the request plus an 8-byte header, 16-byte aligned, on the heap; from the 128 KiB mmap threshold (compared with that chunk size), whole 4 KiB pages with a header. A larger dynamic threshold only moves allocations to the cheaper heap branch.
const fn glibc_allocation_bytes(bytes: usize) -> usize {
    if bytes + 32 < 128 << 10 {
        bytes + 32
    } else {
        bytes.next_multiple_of(4096) + 4096
    }
}

/// macOS (checked against `malloc_size`): size classes at most a quarter above the request up to 15 KiB, whole 16 KiB pages above. From 2 MiB, macOS can serve a request from a larger freed block it keeps cached, which this does not bound.
const fn macos_allocation_bytes(bytes: usize) -> usize {
    if bytes <= 15 << 10 {
        bytes + bytes / 4 + 128
    } else {
        bytes.next_multiple_of(16 << 10)
    }
}

/// A vector's buffer at its capacity ([`buffer_bytes`]).
pub(crate) fn vec_bytes<T>(v: &Vec<T>) -> usize {
    buffer_bytes(v.capacity() * std::mem::size_of::<T>())
}

/// An `Arc`'s two reference counts precede its value in the allocation.
pub(crate) const ARC_HEADER: usize = 2 * std::mem::size_of::<usize>();

/// What one chunk costs: its `Arc`, holding the vector, and the vector's buffer at `capacity` rows.
fn chunk_bytes(capacity: usize) -> usize {
    buffer_bytes(ARC_HEADER + std::mem::size_of::<Vec<CachedRow>>())
        + buffer_bytes(capacity * std::mem::size_of::<CachedRow>())
}

/// The snapshot's own `Arc` allocation and its chunk list at `capacity` chunks.
fn snapshot_overhead(capacity: usize) -> usize {
    buffer_bytes(ARC_HEADER + std::mem::size_of::<SeriesSnapshot>())
        + buffer_bytes(capacity * std::mem::size_of::<Chunk>())
}

/// A full read decoded straight into chunks. Rows must arrive (tag, step)-ordered, as ClickHouse returns them, each (tag, step) once; a row out of that order fails the read ([`Self::push`]). Each tag's rows fill chunks of [`COMPACT_ROWS`] rows, which become chunks without a copy; a tag's last, partly filled chunk is copied once to its length. So a read holds no more than its rows plus one filling chunk (at most [`COMPACT_ROWS`] rows, 1 MiB; briefly twice that while a last chunk is copied to its length) at a time, and each tag is allocated once. A batched read (`ChClient::fetch_full_many`) decodes into one per run, so it holds one filling chunk per run until it finishes.
#[derive(Default)]
pub(crate) struct FullRead {
    chunks: Vec<Chunk>,
    tag: Option<Arc<str>>,
    filling: Vec<CachedRow>,
    /// The last row's step, which the next row of its tag must exceed.
    last_step: i64,
}

impl FullRead {
    /// Add the read's next row. One out of (tag, step) order is an error: the snapshot's searches and merges rely on that order.
    pub(crate) fn push(&mut self, tag: &str, row: CachedRow) -> anyhow::Result<()> {
        if self.tag.as_deref() != Some(tag) {
            if let Some(held) = self.tag.as_deref().filter(|&held| held >= tag) {
                anyhow::bail!("full read out of (tag, step) order: tag {tag:?} after {held:?}");
            }
            self.seal();
            self.tag = Some(Arc::from(tag));
        } else if row.step <= self.last_step {
            anyhow::bail!(
                "full read out of (tag, step) order: step {} after {} in tag {tag:?}",
                row.step,
                self.last_step
            );
        } else if self.filling.len() == COMPACT_ROWS {
            self.seal();
        }
        self.last_step = row.step;
        self.filling.push(row);
        Ok(())
    }

    fn seal(&mut self) {
        if self.filling.is_empty() {
            return;
        }
        self.chunks.push(Chunk {
            tag: self.tag.clone().expect("rows have a tag"),
            rows: Arc::new(exact_vec(std::mem::take(&mut self.filling))),
        });
    }

    fn finish(mut self) -> Vec<Chunk> {
        self.seal();
        self.chunks
    }
}

/// Fixtures arrive as flat rows.
#[cfg(test)]
impl From<Vec<VersionedRawPoint>> for FullRead {
    fn from(rows: Vec<VersionedRawPoint>) -> Self {
        let mut read = Self::default();
        for row in &rows {
            read.push(&row.tag, CachedRow::of(row))
                .expect("fixture rows are (tag, step)-ordered");
        }
        read
    }
}

/// One tag's rows, in step order across its chunks.
pub(crate) struct TagRows<'a> {
    chunks: &'a [Chunk],
}

impl<'a> TagRows<'a> {
    pub(crate) fn tag(&self) -> &'a str {
        &self.chunks[0].tag
    }

    /// The tag's rows, chunk by chunk.
    pub(crate) fn chunks(&self) -> impl Iterator<Item = &'a [CachedRow]> + 'a {
        self.chunks.iter().map(|chunk| chunk.rows.as_slice())
    }

    /// The (chunk, row) of the first row whose step is not `below`, (chunks, 0) past the last: a binary search over the chunks' last steps, then within the chunk.
    fn seek(&self, below: impl Fn(i64) -> bool) -> (usize, usize) {
        let c = self
            .chunks
            .partition_point(|chunk| below(chunk.rows[chunk.rows.len() - 1].step));
        let r = self
            .chunks
            .get(c)
            .map_or(0, |chunk| chunk.rows.partition_point(|row| below(row.step)));
        (c, r)
    }

    /// The tag's rows with a step in `lo..=hi`, chunk by chunk ([`Self::seek`] for each bound).
    pub(crate) fn between(&self, lo: i64, hi: i64) -> impl Iterator<Item = &'a [CachedRow]> + 'a {
        let chunks = self.chunks;
        let start = self.seek(|step| step < lo);
        let end = self.seek(|step| step <= hi).max(start);
        chunks[start.0..chunks.len().min(end.0 + 1)]
            .iter()
            .zip(start.0..)
            .map(move |(chunk, c)| {
                let from = if c == start.0 { start.1 } else { 0 };
                let to = if c == end.0 { end.1 } else { chunk.rows.len() };
                &chunk.rows[from..to]
            })
    }

    /// How many of the tag's rows have a step in `lo..=hi` ([`Self::between`]).
    pub(crate) fn rows_between(&self, lo: i64, hi: i64) -> usize {
        self.between(lo, hi).map(<[CachedRow]>::len).sum()
    }

    /// The tag's lowest step: rows are step-sorted and chunks are never empty.
    fn first_step(&self) -> i64 {
        self.chunks[0].rows[0].step
    }

    /// The tag's high-water step: rows are step-sorted and chunks are never empty.
    fn last_step(&self) -> i64 {
        let rows = &self.chunks[self.chunks.len() - 1].rows;
        rows[rows.len() - 1].step
    }

    fn find(&self, step: i64) -> Option<&'a CachedRow> {
        let (c, r) = self.seek(|held| held < step);
        let row = self.chunks.get(c)?.rows.get(r)?;
        (row.step == step).then_some(row)
    }
}

/// Immutable rows and their append lineage travel together through retained and pass-through refreshes.
/// Within a lineage, every addition has a stamp strictly above the preceding snapshot's actual maximum.
/// Filtering at a previously authenticated maximum therefore recovers exactly that held snapshot.
#[derive(Debug)]
pub struct SeriesSnapshot {
    /// (tag, step) order: tags ascend, a tag's chunks are adjacent and share one tag allocation, and steps ascend across them. Never reallocated once built: [`Self::bytes`] charges its capacity.
    chunks: Vec<Chunk>,
    /// What the snapshot holds, each allocation once: see [`Self::bytes`].
    bytes: usize,
    lineage: u64,
    maximum: Option<i64>,
    origin: LineageOrigin,
    content: u64,
}

impl SeriesSnapshot {
    fn next_lineage() -> u64 {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        // The process-keyed chart stamp separately invalidates tokens across restarts.
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn assemble(
        chunks: Vec<Chunk>,
        lineage: u64,
        maximum: Option<i64>,
        origin: LineageOrigin,
        content: u64,
    ) -> Self {
        let chunks = exact_vec(chunks);
        let mut bytes = snapshot_overhead(chunks.capacity());
        for tag in chunks.chunk_by(|a, b| Arc::ptr_eq(&a.tag, &b.tag)) {
            bytes += buffer_bytes(ARC_HEADER + tag[0].tag.len());
            bytes += tag
                .iter()
                .map(|chunk| chunk_bytes(chunk.rows.capacity()))
                .sum::<usize>();
        }
        Self {
            chunks,
            bytes,
            lineage,
            maximum,
            origin,
            content,
        }
    }

    #[cfg(test)]
    pub(crate) fn full(rows: Vec<VersionedRawPoint>) -> Self {
        Self::full_with_origin(rows, LineageOrigin::Miss)
    }

    pub(crate) fn full_with_origin(rows: impl Into<FullRead>, origin: LineageOrigin) -> Self {
        Self::reload(rows.into(), origin, None)
    }

    /// Adopt a full read's chunks, hashing each row once.
    fn reload(read: FullRead, origin: LineageOrigin, evicted: Option<&EvictedLineage>) -> Self {
        let chunks = read.finish();
        let (mut maximum, mut content) = (None, 0u64);
        let (mut held_count, mut held_content) = (0, 0u64);
        let mut prefix = true;
        for tag in chunks.chunk_by(|a, b| Arc::ptr_eq(&a.tag, &b.tag)) {
            let mut seen_new = false;
            for row in tag.iter().flat_map(|chunk| chunk.rows.iter()) {
                let hash = row_content(&tag[0].tag, row);
                content = content.wrapping_add(hash);
                maximum = maximum.max(Some(row.inserted_ms));
                if let Some(old) = evicted {
                    if old.maximum.is_some_and(|max| row.inserted_ms <= max) {
                        held_count += 1;
                        held_content = held_content.wrapping_add(hash);
                        prefix &= !seen_new;
                    } else {
                        seen_new = true;
                    }
                }
            }
        }
        // Full reads are (tag, step)-sorted. Count and sum prove the held set; the prefix check also rejects newly stamped lower-step backfills, preserving the cache merge's append contract.
        let restored = evicted
            .filter(|old| prefix && old.size.rows == held_count && old.content == held_content);
        Self::assemble(
            chunks,
            restored.map_or_else(Self::next_lineage, |old| old.lineage),
            maximum,
            restored.map_or(origin, |old| old.origin),
            content,
        )
    }

    pub(crate) fn lineage(&self) -> u64 {
        self.lineage
    }
    pub(crate) fn maximum(&self) -> Option<i64> {
        self.maximum
    }
    pub(crate) fn origin(&self) -> LineageOrigin {
        self.origin
    }

    /// The number of rows.
    pub(crate) fn len(&self) -> usize {
        self.chunks.iter().map(|chunk| chunk.rows.len()).sum()
    }

    /// Upper bound on the live allocations this snapshot holds: its own allocation and chunk list, each chunk, and each tag allocation once, at the allocator's rounding ([`buffer_bytes`]). It does not cover memory the allocator retains after a free, nor a read's transients. A chunk two snapshots share counts in each: the cache charges the one snapshot it retains per series, and chart admission the ones a request pins.
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Each tag's rows, tags ascending.
    pub(crate) fn tags(&self) -> impl Iterator<Item = TagRows<'_>> {
        // A tag's chunks share one tag allocation, as [`Self::assemble`] counts them.
        self.chunks
            .chunk_by(|a, b| Arc::ptr_eq(&a.tag, &b.tag))
            .map(|chunks| TagRows { chunks })
    }

    fn tag(&self, tag: &str) -> Option<TagRows<'_>> {
        let start = self.chunks.partition_point(|chunk| &*chunk.tag < tag);
        let len = self.chunks[start..].partition_point(|chunk| &*chunk.tag == tag);
        (len > 0).then(|| TagRows {
            chunks: &self.chunks[start..start + len],
        })
    }

    /// Every row in (tag, step) order, as the flat form a read returns.
    #[cfg(test)]
    pub(crate) fn to_vec(&self) -> Vec<VersionedRawPoint> {
        self.chunks
            .iter()
            .flat_map(|chunk| {
                chunk.rows.iter().map(|row| VersionedRawPoint {
                    tag: chunk.tag.to_string(),
                    step: row.step,
                    timestamp_ms: row.timestamp_ms,
                    value: row.value,
                    is_value: 1,
                    inserted_ms: row.inserted_ms,
                })
            })
            .collect()
    }

    /// This snapshot with `increment` merged in, or `None` when the increment rewrites history: a row whose (tag, step) the snapshot holds with a different value or timestamp (a resumed run re-logging), a new row at or below its tag's high-water step, or a deletion of a held row. Exact re-reads (the watermark overlap) are dropped. An increment that adds no row returns this same snapshot: same lineage, maximum and content.
    fn merge(self: &Arc<Self>, increment: &[VersionedRawPoint]) -> Option<Arc<Self>> {
        // The increment is read WITHOUT FINAL (see `fetch_increment`), so one (tag, step) can appear as several versions — unmerged duplicate inserts, or a re-log inside the watermark window. ORDER BY (tag, step) leaves versions adjacent but unordered; keep the one FINAL would have kept (max `inserted_ms` — the ReplacingMergeTree version column) before any rewrite comparison, so a superseded sibling never masquerades as a conflict. That order is also what lets each tag's additions extend its chunks: anything else, tombstones included, is refused, so the caller reads the series in full.
        let mut versions: Vec<&VersionedRawPoint> = Vec::with_capacity(increment.len());
        let mut maximum_version_conflicts = false;
        for r in increment {
            let key = (r.tag.as_str(), r.step);
            match versions.last_mut() {
                Some(last) if (last.tag.as_str(), last.step) == key => {
                    if r.inserted_ms > last.inserted_ms {
                        *last = r;
                        maximum_version_conflicts = false;
                    } else if r.inserted_ms == last.inserted_ms
                        && (r.is_value != last.is_value
                            || (r.is_value != 0
                                && (r.value.to_bits() != last.value.to_bits()
                                    || r.timestamp_ms != last.timestamp_ms)))
                    {
                        // ReplacingMergeTree cannot deterministically choose between disagreeing equal-version siblings from this non-FINAL result. Let an authoritative FINAL read settle the key instead of depending on physical row order.
                        maximum_version_conflicts = true;
                    }
                }
                Some(last) if (last.tag.as_str(), last.step) > key => return None,
                _ => {
                    if maximum_version_conflicts {
                        return None;
                    }
                    versions.push(r);
                }
            }
        }
        if maximum_version_conflicts {
            return None;
        }

        // A newer nonnumeric payload at an existing numeric key is a deletion from this series. Fall back to the authoritative FINAL read. Tombstones for keys the numeric cache never held are irrelevant but still advance the fetch watermark in apply_increment.
        if versions.iter().any(|row| {
            row.is_value == 0
                && self
                    .tag(&row.tag)
                    .and_then(|held| held.find(row.step))
                    .is_some_and(|held| row.inserted_ms >= held.inserted_ms)
        }) {
            return None;
        }
        versions.retain(|row| row.is_value != 0);

        // Existing (tag, step) pairs near each tag's tail can re-appear via the overlap window; identical rows are dropped, differing ones are rewrites.
        // An added row stamped at or below the held maximum is late, and starts a new lineage.
        let (mut late, mut maximum, mut content) = (false, self.maximum, self.content);
        let mut appended: Vec<(&str, &[&VersionedRawPoint])> = Vec::new();
        for group in versions.chunk_by(|a, b| a.tag == b.tag) {
            let tag = group[0].tag.as_str();
            let held = self.tag(tag);
            let high_water = held.as_ref().map(TagRows::last_step);
            // The group ascends in step, so the rows past the tag's high-water step are its suffix.
            let (reread, added) =
                group.split_at(group.partition_point(|y| high_water.is_some_and(|m| y.step <= m)));
            for y in reread {
                // A genuinely new row must extend its tag past the cached high-water step; anything else must be an identical re-read of a held row, whose ORIGINAL insertion stamp is retained: the append-lineage proof depends on that stamp staying unchanged even when a re-log supplies a newer one.
                let x = held.as_ref().and_then(|held| held.find(y.step))?;
                if x.value.to_bits() != y.value.to_bits() || x.timestamp_ms != y.timestamp_ms {
                    return None;
                }
            }
            for y in added {
                late |= self.maximum.is_some_and(|max| y.inserted_ms <= max);
                maximum = maximum.max(Some(y.inserted_ms));
                content = content.wrapping_add(row_content(tag, &CachedRow::of(y)));
            }
            if !added.is_empty() {
                appended.push((tag, added));
            }
        }
        if appended.is_empty() {
            return Some(Arc::clone(self));
        }
        let (lineage, origin) = if late {
            (Self::next_lineage(), LineageOrigin::Late)
        } else {
            (self.lineage, self.origin)
        };
        Some(Arc::new(Self::assemble(
            append_chunks(&self.chunks, appended),
            lineage,
            maximum,
            origin,
            content,
        )))
    }

    /// Fixture source snapshots pass through the same merge proof. Missing/replaced rows model an authoritative refetch.
    #[cfg(test)]
    pub(crate) fn refreshed_fixture(self: &Arc<Self>, rows: &[VersionedRawPoint]) -> Arc<Self> {
        self.merge(rows)
            .filter(|merged| tests::same_rows(&merged.to_vec(), rows))
            .unwrap_or_else(|| {
                Arc::new(Self::full_with_origin(
                    rows.to_vec(),
                    LineageOrigin::Rewrite,
                ))
            })
    }
}

/// One cached series: a metric of a run.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SeriesKey {
    pub project_id: String,
    pub run_id: String,
    pub metric_name: String,
}

impl SeriesKey {
    pub fn new(
        project_id: impl Into<String>,
        run_id: impl Into<String>,
        metric_name: impl Into<String>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            run_id: run_id.into(),
            metric_name: metric_name.into(),
        }
    }
}

/// Bounded, process-local history of LRU victims and of series not retained (too large, or refused for hot entries' room), at most this many; neither rows nor tags are retained, and a restart discards it.
///
/// Records drop oldest first, so a refused cycle of more series than this finds none of its own: each read is then priced at the miss estimate and starts a new lineage.
///
/// This many take 10-26 MB at most (300-800 bytes a record, as the cache charges its key and slot).
const EVICTED_LINEAGE_LIMIT: usize = 32_768;

#[derive(Clone)]
struct EvictedLineage {
    lineage: u64,
    maximum: Option<i64>,
    origin: LineageOrigin,
    content: u64,
    size: EvictedSize,
    /// The order records were written in ([`Inner::record_evicted`]), which drops the earliest.
    written: u64,
    /// A full read has claimed the lineage and is hashing its rows: no other read restores it, and stage 2 still prices the series from it ([`SeriesCache::insert_full_with_origin`]).
    claimed: bool,
}

/// What a recovery record held when it left, for chart admission (docs/chart-request-latency.md stage 2): its rows, the bytes its snapshot charged ([`SeriesSnapshot::bytes`]), the lowest and highest of their steps (`rows` 0: an empty range) and the tag groups a chart builds of it: its tagged groups when it has any, as `prepare` drops stray scalar rows beside them (query.rs `selected_groups`), else its scalar group. Stage 2 prices its pin at those bytes, and from the rest the share of its rows a chart's step range keeps and the series its groups form.
#[derive(Clone, Copy)]
pub(crate) struct EvictedSize {
    pub(crate) rows: usize,
    pub(crate) bytes: usize,
    pub(crate) steps: (i64, i64),
    pub(crate) groups: usize,
}

impl EvictedLineage {
    /// The record an LRU victim, or a series not retained, leaves: the `written`th.
    fn of(rows: &SeriesSnapshot, written: u64) -> Self {
        let (mut steps, mut tagged) = ((i64::MAX, i64::MIN), 0);
        for tag in rows.tags() {
            steps = (steps.0.min(tag.first_step()), steps.1.max(tag.last_step()));
            tagged += usize::from(!tag.tag().is_empty());
        }
        Self {
            lineage: rows.lineage,
            maximum: rows.maximum,
            origin: rows.origin,
            content: rows.content,
            size: EvictedSize {
                rows: rows.len(),
                bytes: rows.bytes(),
                steps,
                // Its tagged groups, else its scalar one; an empty record's 1 is never read, its steps an empty range.
                groups: tagged.max(1),
            },
            written,
            claimed: false,
        }
    }
}

/// Re-read this much of the watermark's past on each incremental fetch.
/// `inserted_at` has millisecond resolution, so rows landing in the same
/// millisecond as the watermark AFTER our read would be missed by a
/// strict `>` — the overlap re-reads them and the merge drops exact
/// duplicates.
pub const WATERMARK_OVERLAP_MS: i64 = 1_000;

/// Watermarks never advance past the read's START minus this ([`watermark_cap`]). A row is STAMPED
/// (`inserted_at DEFAULT now64(3)`) when ClickHouse starts processing its
/// INSERT but only becomes VISIBLE when the insert commits — so a slow
/// flush can surface rows stamped seconds in the past. If a faster
/// concurrent flush to the same series had meanwhile pushed the watermark
/// beyond those stamps, no later incremental read would ever see them
/// (they sit below the high-water mark of an already-seen tag, so even
/// rewrite detection can't notice). Capping the watermark at start − margin
/// means a row is only ever missed if its insert takes longer than
/// margin + overlap to commit — flushes run well under a second (see the
/// mkdb2_ch_insert_duration_seconds histogram). The cost is re-reading
/// an active series from ~16s before the previous read's start each refresh; the merge drops the
/// duplicates. Compares this process' clock against ClickHouse's: both
/// run in one NTP-synced cluster, skew ≪ margin.
/// Writes that can exceed margin + overlap: an insert that commits after its client timed out (the client's retry restamps its rows), and bulk imports, whose caches FinalizeImportRun purges (docs/bulk-import.md).
pub const VISIBILITY_MARGIN_MS: i64 = 15_000;

fn unix_ms_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The highest watermark a read that started at `fetch_started` may record ([`VISIBILITY_MARGIN_MS`]), on the wall clock ClickHouse stamps rows with. The start, not the completion: a read's snapshot is no earlier than its start, so every row it lacks committed after it, while a cap at completion would let a read slower than the margin record a watermark past rows still committing at its snapshot.
pub(crate) fn watermark_cap(fetch_started: Instant) -> i64 {
    unix_ms_now() - fetch_started.elapsed().as_millis() as i64 - VISIBILITY_MARGIN_MS
}

/// How long an entry serves without even the incremental query, counted from its fetch START. Long because each expiry costs a ClickHouse round trip per series, held under the chart request's admission units.
/// Safety comes from the bump gate ([`SeriesCache::note_bumps`]), not the window. The window alone bounds writes nothing notes that stamp a fresh `inserted_at` (the incremental read's key): a manual INSERT, a failed insert that commits after its late re-note, or a legacy unversioned rich publish cancelled mid-insert. One that keeps old stamps (an in-place ALTER UPDATE/DELETE, an INSERT copying `inserted_at`) reaches a retained entry only through a full read: eviction or a restart.
const FRESH_WINDOW: Duration = Duration::from_secs(30 * 60);

/// How long a note must survive: LONGER THAN ANY ROWS FETCHED BEFORE IT CAN STILL SERVE, which [`is_fresh`] bounds at [`FRESH_WINDOW`] after their fetch start; pruning the note sooner would serve rows that may predate it.
const BUMP_RETENTION: Duration = Duration::from_secs(3600);
const _: () = assert!(FRESH_WINDOW.as_secs() < BUMP_RETENTION.as_secs());

/// Pruning scans every note and every insert notes, so it runs at most this often; retention is only a lower bound.
const PRUNE_INTERVAL: Duration = Duration::from_secs(60);

/// An entry read within this window keeps its room: a series the cache does not hold, whose retention would evict one, is served without being retained ([`SeriesCache::insert_full_with_origin`]). LRU alone misses every lookup of a dashboard cycling through more series than fit; refusing the newcomers keeps a stable share.
///
/// A panel polls when its runs gain rows, at least 5 s after its previous poll (the frontend's refresh floor) and not before that one answered. Its series stay hot while those polls come within about 40 s of each other. A panel whose runs log less often is left to the LRU. Series no poll reads (a panel scrolled away, a hidden or closed tab, a finished run) give up their room 40 s after their last read.
///
/// Not 60 s, the period of the frontend's chart request deadline and version backstop, the local socket's keepalive timeout and [`PRUNE_INTERVAL`]: the window bounds an entry's age and has no phase, but a read cadence one of those clocks set would sit at its edge.
const HOT_WINDOW: Duration = Duration::from_secs(40);

/// Whole-task bound for a detached chart refresh (docs/admission-control.md Stage R). Detached tasks have no enclosing request deadline, so this is what frees a hung scan's admission unit and refresh slot; it sits inside [`FRESH_WINDOW`] so a finished scan's rows can still serve.
const DETACHED_REFRESH_TIMEOUT: Duration = Duration::from_secs(300);
const _: () = assert!(DETACHED_REFRESH_TIMEOUT.as_secs() < FRESH_WINDOW.as_secs());

/// A failure is never published: the caller that receives it surfaces the error, and later arrivals elect a fresh attempt.
#[derive(Debug)]
pub(crate) enum RefreshError {
    Source(anyhow::Error),
    Timeout,
    Died,
}

pub(crate) type RefreshOutcome = Result<Arc<SeriesSnapshot>, RefreshError>;

/// The one freshness rule for cached and shared rows: their fetch STARTED within [`FRESH_WINDOW`] and at or after the run's last note.
pub(crate) fn is_fresh(last_bump: Option<Instant>, started: Instant) -> bool {
    started.elapsed() < FRESH_WINDOW && last_bump.is_none_or(|bump| started >= bump)
}

impl std::fmt::Display for RefreshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefreshError::Source(e) => e.fmt(f),
            RefreshError::Timeout => write!(
                f,
                "detached series refresh exceeded {}s",
                DETACHED_REFRESH_TIMEOUT.as_secs()
            ),
            RefreshError::Died => write!(
                f,
                "detached series refresh ended without a result (task panic or shutdown)"
            ),
        }
    }
}

impl From<anyhow::Error> for RefreshError {
    fn from(e: anyhow::Error) -> Self {
        RefreshError::Source(e)
    }
}

fn default_budget_bytes() -> usize {
    crate::env::required_mebibytes("KYMO_SERIES_CACHE_MB", 256)
        .expect("invalid kymo series-cache environment")
}

/// What a full read of `rows` rows of one tag will cost once cached (about 32 bytes per row; [`FullRead`]'s layout), for estimates made before its rows exist; the rows in hand are charged [`SeriesSnapshot::bytes`].
pub(crate) fn snapshot_bytes(rows: usize) -> usize {
    let (full, rest) = (rows / COMPACT_ROWS, rows % COMPACT_ROWS);
    let chunks = full + usize::from(rest > 0);
    let tag = if chunks == 0 {
        0
    } else {
        buffer_bytes(ARC_HEADER)
    };
    let partial = if rest == 0 { 0 } else { chunk_bytes(rest) };
    snapshot_overhead(chunks) + tag + full * chunk_bytes(COMPACT_ROWS) + partial
}

/// What the cache charges an entry: its snapshot and its key's strings. Its map slot is charged with the whole table ([`table_bytes`]).
fn entry_bytes(key: &SeriesKey, rows: &SeriesSnapshot) -> usize {
    rows.bytes()
        + [&key.project_id, &key.run_id, &key.metric_name]
            .into_iter()
            .map(|text| buffer_bytes(text.capacity()))
            .sum::<usize>()
}

/// What a hash table holds at `capacity` with `slot`-byte entries, occupied or not: hashbrown's one allocation of a slot and a control byte per bucket, plus a group of trailing control bytes. Buckets are a power of two, at least 4, at most 7/8 usable; removals never return them and can lower `capacity()` while they stay, so a table is charged at the largest capacity it has reported since it last shrank, and shrunk once a quarter full of it ([`shrink_table`]).
pub(crate) fn hash_table_bytes(capacity: usize, slot: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    let buckets = (capacity * 8).div_ceil(7).next_power_of_two().max(4);
    buffer_bytes(buckets * (slot + 1) + 16)
}

/// What the cache's map holds at `capacity` ([`hash_table_bytes`]).
fn table_bytes(capacity: usize) -> usize {
    hash_table_bytes(capacity, std::mem::size_of::<(SeriesKey, Entry)>())
}

/// Raise `capacity`, the high-water capacity `map` is charged at ([`hash_table_bytes`]), to what it reports, and shrink a table a quarter full of it.
pub(crate) fn shrink_table<K: Eq + Hash, V>(map: &mut HashMap<K, V>, capacity: &mut usize) {
    *capacity = (*capacity).max(map.capacity());
    if map.len() * 4 <= *capacity {
        map.shrink_to_fit();
        *capacity = map.capacity();
    }
}

/// What the cache charges an entry of these rows under `key` as its only entry: the entry, and the smallest table.
#[cfg(test)]
pub(crate) fn entry_charge(key: &SeriesKey, rows: Vec<VersionedRawPoint>) -> usize {
    entry_bytes(key, &SeriesSnapshot::full(rows)) + table_bytes(1)
}

/// What the cache knows about a series' size (chart admission, docs/chart-request-latency.md stage 2).
pub(crate) enum CachedSize {
    /// A retained entry, fresh or stale: its snapshot, for the caller to price outside the cache's lock ([`SeriesCache::sizes`]).
    Cached(Arc<SeriesSnapshot>),
    /// A recovery record (an LRU victim's, or a series' not retained): what it held when it left.
    Evicted(EvictedSize),
    Unknown,
}

pub struct Entry {
    pub rows: Arc<SeriesSnapshot>,
    pub max_inserted_ms: i64,
    /// When the fetch that produced these rows STARTED — the bump-gate stamp (a note after this instant means the rows may predate a write) and the start of the [`FRESH_WINDOW`]. Start, not completion, so query duration can't fake safety.
    started: Instant,
    last_access: Instant,
    bytes: usize,
}

#[derive(Default)]
struct Inner {
    map: HashMap<SeriesKey, Entry>,
    evicted: HashMap<SeriesKey, EvictedLineage>,
    /// Every entry's [`entry_bytes`]; the cache charges these and its table ([`Inner::total_bytes`]).
    entries: usize,
    /// The largest capacity `map` has reported since it last shrank ([`hash_table_bytes`]).
    table_capacity: usize,
    /// run_id → the run's latest note ([`SeriesCache::note_bumps`]). Notes must outlive any fetch that started before them (see [`BUMP_RETENTION`]); pruned every [`PRUNE_INTERVAL`], the map self-bounds at "runs written in the last hour".
    bumps: HashMap<String, Instant>,
    last_prune: Option<Instant>,
    /// Recovery records written so far, which number them ([`EvictedLineage::written`]).
    records_written: u64,
    /// Stores this cache's window refused ([`SeriesCache::tallies`]).
    #[cfg(test)]
    refused: u64,
    /// Entries this cache's growth evicted ([`SeriesCache::tallies`]).
    #[cfg(test)]
    growth_evicted: u64,
}

impl Inner {
    /// Record `rows`, the snapshot last handed out for `key` and no longer retained, for lineage recovery and stage 2's pricing ([`SeriesCache::sizes`]).
    ///
    /// Past [`EVICTED_LINEAGE_LIMIT`] records exactly the earliest written eighth go at once, so the pass that finds them runs once per 4,096 records.
    fn record_evicted(&mut self, key: SeriesKey, rows: &SeriesSnapshot) {
        self.records_written += 1;
        let record = EvictedLineage::of(rows, self.records_written);
        self.evicted.insert(key, record);
        if self.evicted.len() > EVICTED_LINEAGE_LIMIT {
            let mut written: Vec<u64> =
                self.evicted.values().map(|record| record.written).collect();
            let (_, &mut last, _) = written.select_nth_unstable(EVICTED_LINEAGE_LIMIT / 8 - 1);
            self.evicted.retain(|_, record| record.written > last);
        }
    }

    /// Retain `rows` (`bytes`) under `key`, read from `started` on, which caps its watermark ([`watermark_cap`]).
    fn install(
        &mut self,
        key: SeriesKey,
        rows: &Arc<SeriesSnapshot>,
        started: Instant,
        bytes: usize,
    ) {
        let max_inserted_ms = rows.maximum.unwrap_or(0).min(watermark_cap(started));
        self.entries += bytes;
        let entry = Entry {
            rows: rows.clone(),
            max_inserted_ms,
            started,
            last_access: Instant::now(),
            bytes,
        };
        let replaced = self.map.insert(key, entry);
        debug_assert!(replaced.is_none(), "a store removes the entry it replaces");
    }

    /// What the cache charges: every entry, and its table at `table_capacity` ([`table_bytes`]).
    fn total_bytes(&self) -> usize {
        self.entries + table_bytes(self.table_capacity)
    }

    /// Whether retaining a new entry of `bytes` must evict an entry read within [`HOT_WINDOW`]: the hot entries, the new one and the smallest table holding them exceed `budget`. Otherwise [`SeriesCache::settle`] evicts every older entry before a hot one, and compacts the table before it would. With nothing hot this is the oversized test: a cache with nothing hot evicts as the LRU does.
    /// The pass over the entries runs only when the store needs room at all: every entry, the new one, and the table as charged or, if larger, the smallest holding one more (an insert can double it).
    fn evicts_hot(&self, bytes: usize, budget: usize) -> bool {
        let table = table_bytes(self.table_capacity).max(table_bytes(self.map.len() + 1));
        if self.entries + bytes + table <= budget {
            return false;
        }
        let now = Instant::now();
        let (count, hot) = self
            .map
            .values()
            .filter(|entry| now.duration_since(entry.last_access) < HOT_WINDOW)
            .fold((0, 0), |(count, hot), entry| (count + 1, hot + entry.bytes));
        hot + bytes + table_bytes(count + 1) > budget
    }
}

pub struct SeriesCache {
    inner: std::sync::Mutex<Inner>,
    budget_bytes: usize,
}

/// Per-series refresh serialization. The cache deliberately does not put
/// in-flight reads in `Inner`: a ClickHouse read may take seconds, and holding
/// the cache mutex across it would block every other series. A weak registry
/// gives each key its own refresh slot without retaining request-derived keys
/// after the last leader/waiter is gone. The slot briefly publishes a leader's
/// successful Arc to the waiters that were already in flight, whether or not
/// the cache retains it.
pub type SeriesRefreshLocks = crate::refresh_locks::RefreshLocks<SeriesKey, SlotState>;

#[derive(Clone)]
struct PublishedRefresh {
    rows: Arc<SeriesSnapshot>,
    started: Instant,
}

type RunningOutcome = Result<PublishedRefresh, RefreshError>;

#[derive(Default)]
pub(crate) struct SlotState {
    published: Option<PublishedRefresh>,
    /// A detached refresh in flight for this key; waiters attach here instead
    /// of electing a second leader.
    running: Option<tokio::task::JoinHandle<RunningOutcome>>,
}

impl SeriesRefreshLocks {
    /// Return a fresh entry or elect exactly one refresher for this key. Every
    /// waiter repeats the cache lookup after acquiring the key lock: it can use
    /// the leader's newly stored Arc, while a bump that arrived during the
    /// leader's read leaves that result stale and elects the waiter for another
    /// refresh. Errors and cancellation drop the guard, allowing the next
    /// waiter to retry.
    pub async fn get_or_refresh<F, Fut>(
        &self,
        cache: &Arc<SeriesCache>,
        key: &SeriesKey,
        detach: impl FnOnce() -> Option<crate::clickhouse::RefreshDetach>,
        refresh: F,
    ) -> RefreshOutcome
    where
        F: FnOnce(Lookup) -> Fut,
        Fut: std::future::Future<Output = RefreshOutcome> + Send + 'static,
    {
        if let Some(rows) = cache.lookup_fresh(key) {
            return Ok(rows);
        }
        let lease = self.lease_for(key);
        let wait_started = Instant::now();
        let mut state = lease.lock().await;
        metrics::histogram!("mkdb2_series_refresh_wait_duration_seconds")
            .record(wait_started.elapsed().as_secs_f64());
        if state.running.is_some() {
            let done = Self::await_running(&mut state).await?;
            // Attach deliveries are bump-gated (docs/admission-control.md
            // goal 4): a snapshot whose fetch predates a bump this caller's
            // consult postdates falls through to a fresh election instead.
            if is_fresh(cache.last_bump(&key.run_id), done.started) {
                state.published = Some(done.clone());
                SeriesCache::record_lookup_result("fresh");
                SeriesCache::record_shared("running");
                return Ok(done.rows);
            }
        }
        match cache.lookup_untracked(key) {
            Lookup::Fresh(rows) => {
                // Another caller refreshed while we waited; this request did
                // no ClickHouse work and is therefore a fresh service for the
                // cache-work metrics.
                SeriesCache::record_lookup_result("fresh");
                SeriesCache::record_shared("retained");
                Ok(rows)
            }
            needs_refresh => {
                // A result the cache did not retain is absent from it; existing waiters can still use the leader's Arc while it is fresh.
                if matches!(needs_refresh, Lookup::Miss) {
                    if let Some(rows) = state
                        .published
                        .as_ref()
                        .filter(|result| is_fresh(cache.last_bump(&key.run_id), result.started))
                        .map(|result| result.rows.clone())
                    {
                        SeriesCache::record_lookup_result("fresh");
                        SeriesCache::record_shared("published");
                        return Ok(rows);
                    }
                }
                // Released before the replacement read: nobody after this caller can use it either (a bump outdated it, or the cache holds the series), and once its requester is done only the slot would pin it, charged nowhere.
                state.published = None;
                SeriesCache::record_lookup(&needs_refresh);
                // Conservatively predate the loader invocation. A bump after
                // this point makes a pass-through result ineligible for
                // sharing, just as Entry::started gates retained rows.
                let refresh_started = Instant::now();
                let fut = refresh(needs_refresh);
                let done = match detach() {
                    None => fut.await.map(|rows| PublishedRefresh {
                        rows,
                        started: refresh_started,
                    }),
                    Some(ctx) => {
                        state.running =
                            Some(self.spawn_detached(cache, key, ctx, fut, refresh_started));
                        // The leader's consult predates its task's fetch
                        // start, so its delivery is never bump-gated.
                        Self::await_running(&mut state).await
                    }
                };
                state.published = done.as_ref().ok().cloned();
                done.map(|published| published.rows)
            }
        }
    }

    /// Stage R's detached refresh: the task owns `ctx` (the admission units and run guards backing its read) and its own lease, so the refresh slot, the attach point for re-polls, outlives every cancelled requester for the task's bounded lifetime. A result nobody awaits when the read ends (its requesters were cancelled or timed out) is retained even where the window refused it ([`SeriesCache::retain_orphan`]): refused, it would live only in this slot, which retires with the task, and the re-poll would read the series again.
    fn spawn_detached<D: Send + 'static>(
        &self,
        cache: &Arc<SeriesCache>,
        key: &SeriesKey,
        ctx: D,
        refresh: impl std::future::Future<Output = RefreshOutcome> + Send + 'static,
        started: Instant,
    ) -> tokio::task::JoinHandle<RunningOutcome> {
        let task_lease = self.lease_for(key);
        let cache = cache.clone();
        tokio::spawn(async move {
            // Locals drop in reverse: the lease, the slot's last owner, retires before `_detach` releases the run guard, so a writer taking the gate (finalize or purge evicting the run) never finds an attachable slot holding a pre-eviction result.
            let _detach = ctx;
            let lease = task_lease;
            // Both drop before the JoinHandle resolves, so a requester awaiting it finds `ctx`'s split back on its request (query/budget.rs `SplitCharge`) before it corrects the series, which would otherwise count those bytes twice.
            let outcome = tokio::time::timeout(DETACHED_REFRESH_TIMEOUT, refresh)
                .await
                .unwrap_or(Err(RefreshError::Timeout));
            metrics::counter!(
                "mkdb2_series_refresh_detached_total",
                "outcome" => match &outcome {
                    Ok(_) => "ok",
                    Err(RefreshError::Timeout) => "timeout",
                    Err(_) => "error",
                }
            )
            .increment(1);
            // A requester or attached waiter awaits the result holding the slot's lock.
            if let (Ok(rows), Some(_)) = (&outcome, lease.try_lock_owned()) {
                cache.retain_orphan(lease.key(), rows, started);
            }
            outcome.map(|rows| PublishedRefresh { rows, started })
        })
    }

    /// Elect this caller as the full-read refresher of up to `limit` of `keys`, taking only slots that are free right now, so electing never waits.
    /// A key that is not a miss, whose slot is busy, or that a running or still-fresh published refresh already covers is left for [`Self::get_or_refresh`], which waits or attaches as usual.
    /// No deadlock: the future holding the elected slots ([`Self::spawn_batch`]) awaits only its own tasks, which take no locks.
    pub(crate) fn elect_misses(
        &self,
        cache: &SeriesCache,
        keys: &[SeriesKey],
        limit: usize,
    ) -> Vec<ElectedMiss> {
        let mut elected = Vec::new();
        for key in keys {
            if elected.len() == limit {
                break;
            }
            // Keeps fresh series, every poll's common case, off the refresh registry.
            if !matches!(cache.lookup_untracked(key), Lookup::Miss) {
                continue;
            }
            let lease = self.lease_for(key);
            let Some(mut state) = lease.try_lock_owned() else {
                continue;
            };
            // The checks get_or_refresh makes once it holds the lock.
            let covered = state.running.is_some()
                || state
                    .published
                    .as_ref()
                    .is_some_and(|result| is_fresh(cache.last_bump(&key.run_id), result.started));
            if covered || !matches!(cache.lookup_untracked(key), Lookup::Miss) {
                continue;
            }
            // As in get_or_refresh: an outdated publication is released before its replacement is read.
            state.published = None;
            SeriesCache::record_lookup(&Lookup::Miss);
            elected.push(ElectedMiss {
                key: key.clone(),
                state,
                _lease: lease,
            });
        }
        elected
    }

    /// Refresh each of `elected` in its own detached task running `refresh(its position in elected, its key)`, as [`Self::get_or_refresh`] would.
    /// Every task is spawned, and its slot's `running` set, before this returns: cancelling the caller cannot strand an elected slot or leave the shared read running on released units.
    /// Every task keeps a clone of `ctx`, so it is released only when the last task ends.
    /// Its published stamp is taken here, before any task can start the read, as a leader's is.
    /// The returned future awaits each task under its slot lock and publishes, as a leader does, up to the first failure; the tasks it leaves stay attachable, as a cancelled leader's do.
    pub(crate) fn spawn_batch<D, Fut>(
        &self,
        cache: &Arc<SeriesCache>,
        mut elected: Vec<ElectedMiss>,
        ctx: D,
        mut refresh: impl FnMut(usize, &SeriesKey) -> Fut,
    ) -> impl std::future::Future<Output = Result<Vec<(SeriesKey, Arc<SeriesSnapshot>)>, RefreshError>>
    where
        D: Clone + Send + 'static,
        Fut: std::future::Future<Output = RefreshOutcome> + Send + 'static,
    {
        let started = Instant::now();
        for (index, miss) in elected.iter_mut().enumerate() {
            let fut = refresh(index, &miss.key);
            miss.state.running =
                Some(self.spawn_detached(cache, &miss.key, ctx.clone(), fut, started));
        }
        async move {
            let mut rows = Vec::with_capacity(elected.len());
            for mut miss in elected {
                let published = Self::await_running(&mut miss.state).await?;
                miss.state.published = Some(published.clone());
                rows.push((miss.key, published.rows));
            }
            Ok(rows)
        }
    }

    /// Await a detached refresh's result while holding the key lock (later
    /// callers queue on the lock exactly as they do behind an inline leader).
    /// Awaiting `&mut JoinHandle` is cancel-safe: cancellation releases the
    /// lock and leaves the task and its pending result in place for the next
    /// caller.
    async fn await_running(state: &mut SlotState) -> RunningOutcome {
        let handle = state.running.as_mut().expect("caller checked running");
        let done = handle.await.unwrap_or(Err(RefreshError::Died));
        state.running = None;
        done
    }
}

type SeriesRefreshLease = crate::refresh_locks::RefreshLease<SeriesKey, SlotState>;

/// A key [`SeriesRefreshLocks::elect_misses`] elected; its slot stays locked until the batch that refreshes it publishes.
pub(crate) struct ElectedMiss {
    pub(crate) key: SeriesKey,
    // Declared before the lease so it drops first: the lease's exact registry removal needs the slot's last Arc.
    state: tokio::sync::OwnedMutexGuard<SlotState>,
    _lease: SeriesRefreshLease,
}

/// What the cache knows when a query starts.
pub enum Lookup {
    /// Entry is fresh enough to serve as-is.
    Fresh(Arc<SeriesSnapshot>),
    /// Entry exists; fetch rows with `inserted_at > watermark - overlap` and call [`SeriesCache::apply_increment`] with `base`, the snapshot the watermark came from.
    Stale {
        watermark_ms: i64,
        base: Arc<SeriesSnapshot>,
    },
    /// No entry; fetch everything and call [`SeriesCache::insert_full_with_origin`].
    Miss,
}

impl SeriesCache {
    pub fn new() -> Self {
        Self::with_budget(default_budget_bytes())
    }

    pub(crate) fn with_budget(budget_bytes: usize) -> Self {
        Self {
            inner: std::sync::Mutex::new(Inner::default()),
            budget_bytes,
        }
    }

    /// Record that these runs just gained rows. [`crate::clickhouse::ChClient::insert_batch`] calls this when an insert returns, before its ack, so the note predates EVERY channel that can reveal a version including the rows (the push frame, PollVersions/resync polls, TerminateRun): entries whose fetch STARTED before it may predate the rows and must not serve Fresh. Without that, a run's final flush would permanently truncate its chart. The first lookup after a note goes Stale (one cheap incremental), re-stamps past it, and the rest of the burst serves Fresh.
    pub fn note_bumps<'a>(&self, run_ids: impl Iterator<Item = &'a str>) {
        let mut inner = self.inner.lock().unwrap();
        if inner
            .last_prune
            .is_none_or(|pruned| pruned.elapsed() >= PRUNE_INTERVAL)
        {
            inner.bumps.retain(|_, t| t.elapsed() < BUMP_RETENTION);
            inner.last_prune = Some(Instant::now());
        }
        // Stamped under the lock, so a run's note only ever moves later.
        let now = Instant::now();
        for run_id in run_ids {
            inner.bumps.insert(run_id.to_string(), now);
        }
    }

    /// Latest note for a run. Other caches use the same
    /// start-before-bump gate as the raw-series cache so a response triggered
    /// by a pushed version cannot reuse metadata from before that insert.
    pub fn last_bump(&self, run_id: &str) -> Option<Instant> {
        self.inner.lock().unwrap().bumps.get(run_id).copied()
    }

    /// Probe without emitting a lookup disposition. Refresh coordination uses
    /// this before and after waiting, then records exactly one final work
    /// disposition: only the elected loader reports miss/stale (including a
    /// failed attempt); followers that avoid ClickHouse report fresh.
    fn lookup_untracked(&self, key: &SeriesKey) -> Lookup {
        let mut inner = self.inner.lock().unwrap();
        let last_bump = inner.bumps.get(&key.run_id).copied();
        match inner.map.get_mut(key) {
            None => Lookup::Miss,
            Some(e) => {
                e.last_access = Instant::now();
                if is_fresh(last_bump, e.started) {
                    Lookup::Fresh(e.rows.clone())
                } else {
                    Lookup::Stale {
                        watermark_ms: e.max_inserted_ms,
                        base: e.rows.clone(),
                    }
                }
            }
        }
    }

    /// Each key's size as the cache records it, without touching recency: a retained snapshot's `Arc` is cloned under the cache's lock, which the caller prices after it is released (pricing a request's tag groups can take milliseconds).
    pub(crate) fn sizes(&self, keys: &[SeriesKey]) -> Vec<CachedSize> {
        let inner = self.inner.lock().unwrap();
        keys.iter()
            .map(|key| match inner.map.get(key) {
                Some(entry) => CachedSize::Cached(entry.rows.clone()),
                None => inner
                    .evicted
                    .get(key)
                    .map_or(CachedSize::Unknown, |record| {
                        CachedSize::Evicted(record.size)
                    }),
            })
            .collect()
    }

    /// A fresh retained entry, pinned and counted as a fresh lookup. Anything else is left to [`SeriesRefreshLocks::get_or_refresh`], which records its own disposition.
    pub(crate) fn lookup_fresh(&self, key: &SeriesKey) -> Option<Arc<SeriesSnapshot>> {
        match self.lookup_untracked(key) {
            Lookup::Fresh(rows) => {
                Self::record_lookup_result("fresh");
                Some(rows)
            }
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn lookup(&self, key: &SeriesKey) -> Lookup {
        let result = self.lookup_untracked(key);
        Self::record_lookup(&result);
        result
    }

    fn record_lookup(result: &Lookup) {
        let label = match result {
            Lookup::Fresh(_) => "fresh",
            Lookup::Stale { .. } => "stale",
            Lookup::Miss => "miss",
        };
        Self::record_lookup_result(label);
    }

    fn record_shared(source: &'static str) {
        metrics::counter!("mkdb2_series_refresh_shared_total", "source" => source).increment(1);
    }

    fn record_lookup_result(label: &'static str) {
        metrics::counter!("mkdb2_series_cache_lookups_total", "result" => label).increment(1);
    }

    #[cfg(test)]
    pub fn insert_full(
        &self,
        key: SeriesKey,
        rows: Vec<VersionedRawPoint>,
        fetch_started: Instant,
    ) -> Arc<SeriesSnapshot> {
        self.insert_full_with_origin(key, rows, fetch_started, LineageOrigin::Miss)
    }

    /// Store a full fetch. `fetch_started` anchors the watermark cap, feeds the bump gate and starts the fresh window. An LRU victim can recover its lineage only from a matching, single-use eviction record. A series the cache does not hold is retained only if that evicts no entry read within [`HOT_WINDOW`]; a cached one replaced by its full read is retained as a grown one is ([`Self::apply_increment`]), evicting the least recently read.
    pub(crate) fn insert_full_with_origin(
        &self,
        key: SeriesKey,
        rows: impl Into<FullRead>,
        fetch_started: Instant,
        origin: LineageOrigin,
    ) -> Arc<SeriesSnapshot> {
        // Claim before hashing outside the mutex: exactly one full fetch can own this lineage while no cache entry holds it. Never restore a claimed record, even if the fetched contents do not match. The claim leaves the record in place for stage 2's pricing (a refused series takes this path at every read).
        let evicted = {
            let mut inner = self.inner.lock().unwrap();
            let record = inner.evicted.get_mut(&key).filter(|record| !record.claimed);
            record.map(|record| {
                record.claimed = true;
                record.clone()
            })
        };
        let rows = Arc::new(SeriesSnapshot::reload(
            rows.into(),
            origin,
            evicted.as_ref(),
        ));
        let bytes = entry_bytes(&key, &rows);
        let mut inner = self.inner.lock().unwrap();
        // A concurrent fetch may have published and evicted a different lineage while this one was hashing. That record must not survive this replacement.
        inner.evicted.remove(&key);
        // Concurrent full fetches race their stores; last writer wins, even if its snapshot is older (local start order can't order ClickHouse snapshots anyway). Safe without an ordering guard: a noted insert is protected by the bump gate in lookup regardless of which racer won, and a regression past a write nothing notes heals on the first incremental after FRESH_WINDOW.
        let cached = inner
            .map
            .remove(&key)
            .inspect(|old| inner.entries -= old.bytes)
            .is_some();
        // An entry larger than the whole budget (with the smallest table) is never retained — eviction can't save a sole entry, so once stored it would sit over budget for as long as it's read. Serving it pass-through makes the budget a hard bound on retained memory, at ablated-mode read cost for just this series. It leaves a recovery record as an LRU victim does, so its size is known.
        if bytes + table_bytes(1) > self.budget_bytes {
            metrics::counter!("mkdb2_series_cache_oversized_total", "reason" => "budget")
                .increment(1);
            inner.record_evicted(key, &rows);
            Self::settle(&mut inner, self.budget_bytes);
            return rows;
        }
        // Nor a new entry retained only by evicting an entry read within HOT_WINDOW: served pass-through the same way, leaving the same record; the cache is unchanged.
        if !cached && inner.evicts_hot(bytes, self.budget_bytes) {
            metrics::counter!("mkdb2_series_cache_oversized_total", "reason" => "hot").increment(1);
            #[cfg(test)]
            {
                inner.refused += 1;
            }
            inner.record_evicted(key, &rows);
            return rows;
        }
        inner.install(key, &rows, fetch_started, bytes);
        #[cfg(test)]
        let held = inner.map.len();
        Self::settle(&mut inner, self.budget_bytes);
        #[cfg(test)]
        if cached {
            inner.growth_evicted += (held - inner.map.len()) as u64;
        }
        rows
    }

    /// Fold an incremental fetch into the cached entry: `Ok` is the merged series, `Err` a refusal after which the caller reads in full ([`Self::insert_full_with_origin`]). `base` is the [`Lookup::Stale`] snapshot the increment's watermark came from; `fetch_started` is the fetch START, as in [`Self::insert_full_with_origin`], and only ever advances `started` (a concurrent later-started query may have stamped first), which restarts the fresh window: the rows are verified current as of that fetch.
    ///
    /// Growth evicts the least recently read entries, hot or not ([`HOT_WINDOW`] gates only new entries): a refresh adds a live run's few rows, so it rarely evicts, while dropping the growing entry instead would leave every live series uncached once the cache is full, and each poll of a live run waiting for a full read.
    ///
    /// The merge refuses an increment that rewrites history ([`SeriesSnapshot::merge`]). It runs against `base` outside the cache's lock (its compaction can copy up to 2 MiB per tag the increment extends), and its result is installed only if the entry still holds `base`: an entry replaced (by a full read, which may be older than `base`, so the increment could leave a hole below the merged watermark that no later increment reads), evicted, purged or since refreshed by a direct caller ([`SeriesRefreshLocks`] serializes a series' refreshes) refuses.
    pub fn apply_increment(
        &self,
        key: &SeriesKey,
        increment: Vec<VersionedRawPoint>,
        fetch_started: Instant,
        base: &Arc<SeriesSnapshot>,
    ) -> Result<Arc<SeriesSnapshot>, ()> {
        let holds_base = |e: &&mut Entry| Arc::ptr_eq(&e.rows, base);
        {
            let mut inner = self.inner.lock().unwrap();
            let e = inner.map.get_mut(key).filter(holds_base).ok_or(())?;
            if increment.is_empty() {
                e.started = e.started.max(fetch_started);
                return Ok(e.rows.clone());
            }
        }
        let Some(merged) = base.merge(&increment) else {
            metrics::counter!("mkdb2_series_cache_rewrites_total").increment(1);
            return Err(());
        };
        let mut inner = self.inner.lock().unwrap();
        let e = inner.map.get_mut(key).filter(holds_base).ok_or(())?;
        let new_bytes = entry_bytes(key, &merged);
        // The merge grew the entry past the whole budget: hand out the merged rows but stop retaining the entry (see the full-store path), leaving the merged rows' recovery record — the next poll misses, full-fetches, and stays pass-through while the series is this large.
        if new_bytes + table_bytes(1) > self.budget_bytes {
            metrics::counter!("mkdb2_series_cache_oversized_total", "reason" => "budget")
                .increment(1);
            let e = inner.map.remove(key).unwrap();
            inner.entries -= e.bytes;
            inner.record_evicted(key.clone(), &merged);
            Self::settle(&mut inner, self.budget_bytes);
            return Ok(merged);
        }
        // Capped like the full-store watermark; never regresses below the old mark (it
        // was capped the same way when set).
        let new_wm = increment
            .iter()
            .map(|r| r.inserted_ms)
            .max()
            .unwrap_or(e.max_inserted_ms)
            .min(watermark_cap(fetch_started))
            .max(e.max_inserted_ms);
        let old_bytes = e.bytes;
        e.rows = merged;
        e.max_inserted_ms = new_wm;
        e.bytes = new_bytes;
        e.started = e.started.max(fetch_started);
        let rows = e.rows.clone();
        inner.entries = inner.entries + new_bytes - old_bytes;
        #[cfg(test)]
        let held = inner.map.len();
        Self::settle(&mut inner, self.budget_bytes);
        #[cfg(test)]
        {
            inner.growth_evicted += (held - inner.map.len()) as u64;
        }
        Ok(rows)
    }

    /// The stores this cache's window refused and the entries its growth evicted, for the randomized admission test, which reads its own services' caches.
    #[cfg(test)]
    pub(crate) fn tallies(&self) -> (u64, u64) {
        let inner = self.inner.lock().unwrap();
        (inner.refused, inner.growth_evicted)
    }

    /// Retain `rows`, a detached read's result that nobody awaited, which the window refused (its unclaimed recovery record still matches this snapshot), as the LRU would, evicting the least recently read: the re-poll after a cancelled or timed-out request then finds it (docs/admission-control.md Stage R). `started` predates the read, as its store's fetch start does, so the watermark and fresh window it stamps are no later than the store's. A series cached since (whose store removed its record), a record a later read replaced or a purge removed, and a series larger than the budget are left as they are.
    fn retain_orphan(&self, key: &SeriesKey, rows: &Arc<SeriesSnapshot>, started: Instant) {
        let mut inner = self.inner.lock().unwrap();
        let refused = inner.evicted.get(key).is_some_and(|record| {
            !record.claimed
                && (record.lineage, record.maximum, record.content)
                    == (rows.lineage, rows.maximum, rows.content)
        });
        let bytes = entry_bytes(key, rows);
        if !refused || bytes + table_bytes(1) > self.budget_bytes {
            return;
        }
        inner.evicted.remove(key);
        inner.install(key.clone(), rows, started, bytes);
        Self::settle(&mut inner, self.budget_bytes);
    }

    /// Evict one entry as the LRU would, leaving its recovery record.
    #[cfg(test)]
    pub(crate) fn evict_for_test(&self, key: &SeriesKey) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(e) = inner.map.remove(key) {
            inner.entries -= e.bytes;
            inner.record_evicted(key.clone(), &e.rows);
        }
    }

    /// Age every entry past [`HOT_WINDOW`], keeping the order and spacing of their reads: the most recently read one was read a window ago. Not cumulative, so it needs no `Instant` older than a window and the entries' spread; on a host up less than that it panics, which fails as loudly as its callers' assertions would.
    #[cfg(test)]
    pub(crate) fn cool_for_test(&self) {
        let mut inner = self.inner.lock().unwrap();
        let Some(newest) = inner.map.values().map(|entry| entry.last_access).max() else {
            return;
        };
        let cold = Instant::now() - HOT_WINDOW;
        for entry in inner.map.values_mut() {
            entry.last_access = cold - (newest - entry.last_access);
        }
    }

    /// Remove every cached metric for the exact `(project_id, run_id)` identities. The full identity matches the cache/storage key and keeps this eviction defensively project-scoped, even though admission reserves each run ID to one project for good.
    ///
    /// Bump notes follow a separate safety lifetime: they must outlive any fetch that started before the insert they note (see [`BUMP_RETENTION`]). They hold no series rows and age out on their existing retention schedule.
    pub fn purge_runs<'a>(&self, runs: impl IntoIterator<Item = (&'a str, &'a str)>) -> usize {
        let runs = runs.into_iter().collect::<HashSet<_>>();
        let mut inner = self.inner.lock().unwrap();
        let mut removed = 0usize;
        let mut removed_bytes = 0usize;
        inner.map.retain(|key, entry| {
            if runs.contains(&(key.project_id.as_str(), key.run_id.as_str())) {
                removed += 1;
                removed_bytes += entry.bytes;
                false
            } else {
                true
            }
        });
        inner
            .evicted
            .retain(|key, _| !runs.contains(&(key.project_id.as_str(), key.run_id.as_str())));
        inner.entries -= removed_bytes;
        Self::settle(&mut inner, self.budget_bytes);
        removed
    }

    /// Drop least-recently-accessed entries until under budget, then publish the size gauges. Call before returning from any mutation. Before each eviction the map's table is charged at `table_capacity`, shrunk once a quarter full of it, so an emptied cache neither holds nor charges its buckets and every retained entry, individually within budget beside the smallest table (no larger one is ever retained), outlasts the loop if it is alone. Before evicting an entry read within [`HOT_WINDOW`], it compacts a table larger than the smallest holding its entries (an insert can double a table that then shrinks only once a quarter full), once: the window's admission ([`Inner::evicts_hot`]) counts on that table.
    fn settle(inner: &mut Inner, budget: usize) {
        let mut compacted = false;
        loop {
            shrink_table(&mut inner.map, &mut inner.table_capacity);
            if inner.total_bytes() <= budget {
                break;
            }
            let Some((key, entry)) = inner.map.iter().min_by_key(|(_, e)| e.last_access) else {
                break;
            };
            let hot = entry.last_access.elapsed() < HOT_WINDOW;
            if !compacted && hot && table_bytes(inner.map.len()) < table_bytes(inner.table_capacity)
            {
                compacted = true;
                inner.map.shrink_to_fit();
                inner.table_capacity = inner.map.capacity();
                continue;
            }
            let key = key.clone();
            let e = inner.map.remove(&key).unwrap();
            inner.entries -= e.bytes;
            inner.record_evicted(key, &e.rows);
            // Only growth and orphans evict hot entries (the window refuses a new series first), which the window counts on being rare.
            let victim = if hot { "hot" } else { "cold" };
            metrics::counter!("mkdb2_series_cache_evictions_total", "victim" => victim)
                .increment(1);
        }
        metrics::gauge!("mkdb2_series_cache_bytes").set(inner.total_bytes() as f64);
        metrics::gauge!("mkdb2_series_cache_entries").set(inner.map.len() as f64);
    }
}

/// [`SeriesSnapshot::merge`] of flat rows, as fixtures hold them.
#[cfg(test)]
pub fn merge_increment(
    cached: &[VersionedRawPoint],
    increment: &[VersionedRawPoint],
) -> Option<Vec<VersionedRawPoint>> {
    Arc::new(SeriesSnapshot::full(cached.to_vec()))
        .merge(increment)
        .map(|merged| merged.to_vec())
}

/// The held chunks, shared, with each tag's additions (tags ascending) in a new chunk after the tag's last one; the additions absorb the tag's trailing chunks as [`COMPACT_ROWS`] describes, so each absorbed row is copied once, into a chunk allocated at its length.
fn append_chunks(held: &[Chunk], appended: Vec<(&str, &[&VersionedRawPoint])>) -> Vec<Chunk> {
    let mut chunks = Vec::with_capacity(held.len() + appended.len());
    let mut rest = held;
    for (tag, added) in appended {
        // The held chunks through the tag's own, which share one tag allocation.
        let through = rest.partition_point(|chunk| &*chunk.tag <= tag);
        chunks.extend_from_slice(&rest[..through]);
        rest = &rest[through..];
        let tag = match chunks.last() {
            Some(chunk) if &*chunk.tag == tag => chunk.tag.clone(),
            _ => Arc::from(tag),
        };
        let mut start = chunks.len();
        let mut merged = added.len();
        while start > 0
            && Arc::ptr_eq(&chunks[start - 1].tag, &tag)
            && chunks[start - 1].rows.len() < COMPACT_ROWS
            && chunks[start - 1].rows.len() <= merged * 2
        {
            start -= 1;
            merged += chunks[start].rows.len();
        }
        let mut rows = Vec::with_capacity(merged);
        for chunk in chunks.drain(start..) {
            rows.extend_from_slice(&chunk.rows);
        }
        rows.extend(added.iter().map(|row| CachedRow::of(row)));
        chunks.push(Chunk {
            tag,
            rows: Arc::new(rows),
        });
    }
    chunks.extend_from_slice(rest);
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wait_until;

    fn row(tag: &str, step: i64, value: f32, ins: i64) -> VersionedRawPoint {
        VersionedRawPoint {
            tag: tag.to_string(),
            step,
            timestamp_ms: step * 10,
            value,
            is_value: 1,
            inserted_ms: ins,
        }
    }

    fn tombstone(tag: &str, step: i64, ins: i64) -> VersionedRawPoint {
        VersionedRawPoint {
            tag: tag.to_string(),
            step,
            timestamp_ms: step * 10,
            value: 0.0,
            is_value: 0,
            inserted_ms: ins,
        }
    }

    /// What the cache charges a snapshot of these rows.
    fn charged(rows: Vec<VersionedRawPoint>) -> usize {
        entry_charge(&SeriesKey::new("p", "r", "m"), rows)
    }

    /// `rows` rows of one untagged series at distinct steps, as a full read returns them: for sizes and filler.
    fn filler(rows: usize) -> Vec<VersionedRawPoint> {
        (0..rows as i64).map(|step| row("", step, 0.0, 0)).collect()
    }

    fn point_content(row: &VersionedRawPoint) -> u64 {
        row_content(&row.tag, &CachedRow::of(row))
    }

    /// The cached entry's snapshot (what Lookup::Stale would hand a caller).
    fn base_of(cache: &SeriesCache, k: &SeriesKey) -> Arc<SeriesSnapshot> {
        cache.inner.lock().unwrap().map[k].rows.clone()
    }

    #[test]
    fn snapshot_lineage_tracks_additions_not_fetch_watermarks_or_overlap_restamps() {
        let cache = SeriesCache::new();
        let key = SeriesKey::new("p", "r", "m");
        let now = unix_ms_now();
        let first = cache.insert_full(key.clone(), vec![row("", 0, 1.0, now)], Instant::now());
        assert_eq!(first.maximum(), Some(now));
        assert!(cache.inner.lock().unwrap().map[&key].max_inserted_ms < now);
        let overlap = cache
            .apply_increment(
                &key,
                vec![row("", 0, 1.0, now + 1)],
                Instant::now(),
                &base_of(&cache, &key),
            )
            .unwrap();
        assert_eq!(overlap.lineage(), first.lineage());
        assert_eq!(overlap.to_vec()[0].inserted_ms, now);
        assert_eq!(overlap.maximum(), Some(now));
        let appended = cache
            .apply_increment(
                &key,
                vec![row("", 1, 2.0, now + 2)],
                Instant::now(),
                &base_of(&cache, &key),
            )
            .unwrap();
        assert_eq!(appended.lineage(), first.lineage());
        assert_eq!(appended.maximum(), Some(now + 2));
        let equal_stamp = cache
            .apply_increment(
                &key,
                vec![row("", 2, 3.0, now + 2)],
                Instant::now(),
                &base_of(&cache, &key),
            )
            .unwrap();
        assert_ne!(equal_stamp.lineage(), appended.lineage());
        assert_eq!(equal_stamp.origin(), LineageOrigin::Late);
        let older_new_tag = cache
            .apply_increment(
                &key,
                vec![row("tag", 0, 4.0, now - 1)],
                Instant::now(),
                &base_of(&cache, &key),
            )
            .unwrap();
        assert_ne!(older_new_tag.lineage(), equal_stamp.lineage());
        assert_eq!(older_new_tag.maximum(), Some(now + 2));
        assert_eq!(first.origin(), LineageOrigin::Miss);
        // The cached sum includes additions exactly once and retains overlap stamps.
        for snapshot in [&first, &overlap, &appended, &equal_stamp, &older_new_tag] {
            assert_layout(snapshot);
        }
        assert_eq!(first.len(), 1, "published snapshots remain immutable");
    }

    #[test]
    fn irrelevant_tombstone_advances_fetch_watermark_without_forging_numeric_membership() {
        let cache = SeriesCache::new();
        let key = SeriesKey::new("p", "r", "m");
        let first = cache.insert_full(key.clone(), vec![row("", 0, 1.0, 100)], Instant::now());
        let next = cache
            .apply_increment(
                &key,
                vec![tombstone("", 5, 500)],
                Instant::now(),
                &base_of(&cache, &key),
            )
            .unwrap();
        assert_eq!(next.lineage(), first.lineage());
        assert_eq!(next.maximum(), Some(100));
        assert_eq!(cache.inner.lock().unwrap().map[&key].max_inserted_ms, 500);
    }

    #[test]
    fn replacement_purge_and_unrecorded_reloads_start_new_lineages() {
        let source = vec![row("", 0, 1.0, 100)];
        let cache = SeriesCache::with_budget(charged(source.clone()));
        let key = SeriesKey::new("p", "r", "m");
        let first = cache.insert_full(key.clone(), source.clone(), Instant::now());
        let stale_base = base_of(&cache, &key);
        let replacement = cache.insert_full(key.clone(), source.clone(), Instant::now());
        assert_ne!(first.lineage(), replacement.lineage());
        assert!(cache
            .apply_increment(
                &key,
                vec![row("", 1, 2.0, 200)],
                Instant::now(),
                &stale_base
            )
            .is_err());
        let another_cache = SeriesCache::new();
        let other = another_cache.insert_full(key.clone(), source.clone(), Instant::now());
        assert_ne!(other.lineage(), first.lineage());
        another_cache.purge_runs([("p", "r")]);
        let after_purge = another_cache.insert_full(key, source.clone(), Instant::now());
        assert_ne!(after_purge.lineage(), other.lineage());
        assert_ne!(
            SeriesSnapshot::full(source).lineage(),
            after_purge.lineage()
        );
    }

    /// Room for exactly one `evict` pressure entry of `row_count` rows, whose key is the longest the eviction tests use.
    fn eviction_cache(row_count: usize) -> SeriesCache {
        SeriesCache::with_budget(entry_charge(
            &SeriesKey::new("p", "eviction-pressure", "m"),
            filler(row_count),
        ))
    }

    /// Evict `key` as the LRU does once it has gone unread for [`HOT_WINDOW`], leaving the pressure entry as cold for the next store.
    fn evict(cache: &SeriesCache, key: &SeriesKey, row_count: usize) {
        let other = SeriesKey::new("p", "eviction-pressure", "m");
        cache.cool_for_test();
        cache.insert_full(other, filler(row_count), Instant::now());
        cache.cool_for_test();
        assert!(matches!(cache.lookup(key), Lookup::Miss));
        assert!(cache.inner.lock().unwrap().evicted.contains_key(key));
    }

    #[test]
    fn lru_reload_restores_lineage_and_origin_through_repeated_evictions() {
        let cache = eviction_cache(4);
        let key = SeriesKey::new("p", "r", "m");
        let first = cache.insert_full_with_origin(
            key.clone(),
            vec![row("a", 2, 1.0, 100)],
            Instant::now(),
            LineageOrigin::Rewrite,
        );
        evict(&cache, &key, 4);
        let second = cache.insert_full(
            key.clone(),
            vec![row("a", 2, 1.0, 100), row("a", 4, 2.0, 200)],
            Instant::now(),
        );
        assert_eq!(second.lineage(), first.lineage());
        assert_eq!(second.origin(), LineageOrigin::Rewrite);
        evict(&cache, &key, 4);
        let third = cache.insert_full(key.clone(), second.to_vec(), Instant::now());
        assert_eq!(third.lineage(), first.lineage());
        evict(&cache, &key, 4);
        let regressed = cache.insert_full(key, first.to_vec(), Instant::now());
        assert_ne!(
            regressed.lineage(),
            first.lineage(),
            "the newest eviction maximum cannot regress"
        );
        assert_eq!(regressed.origin(), LineageOrigin::Miss);
    }

    #[test]
    fn lru_reload_rejects_changed_held_rows_late_rows_and_step_backfills() {
        let original = vec![row("a", 2, 1.0, 100), row("a", 6, 2.0, 200)];
        let mut timestamp_rewrite = original.clone();
        timestamp_rewrite[0].timestamp_ms += 1;
        let cases = [
            vec![row("a", 2, 9.0, 100), original[1].clone()],
            timestamp_rewrite,
            vec![original[0].clone()],
            vec![row("a", 2, 1.0, 300), original[1].clone()],
            vec![
                original[0].clone(),
                original[1].clone(),
                row("a", 8, 3.0, 200),
            ],
            vec![
                original[0].clone(),
                row("a", 4, 3.0, 300),
                original[1].clone(),
            ],
        ];
        for (case, rows) in cases.into_iter().enumerate() {
            let cache = eviction_cache(4);
            let key = SeriesKey::new("p", "r", "m");
            let first = cache.insert_full(key.clone(), original.clone(), Instant::now());
            evict(&cache, &key, 4);
            let reloaded = cache.insert_full(key, rows, Instant::now());
            assert_ne!(reloaded.lineage(), first.lineage(), "case {case}");
        }
    }

    #[test]
    fn empty_lineage_can_be_reloaded_and_then_extended() {
        let cache = eviction_cache(2);
        let key = SeriesKey::new("p", "r", "m");
        let first = cache.insert_full(key.clone(), vec![], Instant::now());
        evict(&cache, &key, 2);
        let empty = cache.insert_full(key.clone(), vec![], Instant::now());
        assert_eq!(empty.lineage(), first.lineage());
        evict(&cache, &key, 2);
        let populated = cache.insert_full(key, vec![row("new", 0, 1.0, -5)], Instant::now());
        assert_eq!(populated.lineage(), first.lineage());
        assert_eq!(populated.maximum(), Some(-5));
    }

    /// An LRU victim's record carries what stage 2 prices the series by until its read: its rows, the bytes its snapshot charged, the lowest and highest of their steps across its tags, and its tag groups.
    #[test]
    fn eviction_records_carry_the_rows_steps_and_tag_groups_they_held() {
        let cache = SeriesCache::with_budget(1 << 20);
        let key = SeriesKey::new("p", "r", "m");
        let held = cache.insert_full(
            key.clone(),
            vec![
                row("", 30, 1.0, 100),
                row("a", 10, 1.0, 100),
                row("a", 11, 1.0, 100),
                row("b", 5, 1.0, 100),
                row("b", 20, 1.0, 100),
                row("b", 21, 1.0, 100),
            ],
            Instant::now(),
        );
        cache.evict_for_test(&key);
        let bytes = held.bytes();
        // The stray scalar row counts in rows and steps, not as a group: `prepare` drops it beside tagged ones.
        assert!(matches!(
            cache.sizes(std::slice::from_ref(&key))[..],
            [CachedSize::Evicted(EvictedSize {
                rows: 6,
                bytes: b,
                steps: (5, 30),
                groups: 2
            })] if b == bytes
        ));
        // One scalar and one tagged group: the tagged one.
        let key = SeriesKey::new("p", "s", "m");
        cache.insert_full(
            key.clone(),
            vec![row("", 1, 1.0, 100), row("a", 2, 1.0, 100)],
            Instant::now(),
        );
        cache.evict_for_test(&key);
        assert!(matches!(
            cache.sizes(std::slice::from_ref(&key))[..],
            [CachedSize::Evicted(EvictedSize { groups: 1, .. })]
        ));
    }

    /// A series too large to retain is served without being stored (eviction cannot help a sole entry, so the budget stays a hard bound on retained memory) and leaves the recovery record of the rows it handed out, from a full read or an increment, as an LRU victim does: stage 2 prices it from that record, and a matching full read keeps its lineage.
    #[test]
    fn oversized_results_leave_recovery_records() {
        let cache = eviction_cache(1);
        let retained_bytes = || cache.inner.lock().unwrap().total_bytes();
        let recorded = |key: &SeriesKey| match cache.sizes(std::slice::from_ref(key))[..] {
            [CachedSize::Evicted(record)] => Some(record.rows),
            _ => None,
        };
        // A full read past the budget after an LRU eviction restores the victim's lineage and leaves its own record.
        let key = SeriesKey::new("p", "r", "m");
        let first = cache.insert_full(key.clone(), vec![row("", 0, 1.0, 100)], Instant::now());
        evict(&cache, &key, 1);
        let before = retained_bytes();
        let full = vec![row("", 0, 1.0, 100), row("", 1, 2.0, 200)];
        let oversized = cache.insert_full(key.clone(), full.clone(), Instant::now());
        assert_eq!((oversized.len(), oversized.lineage()), (2, first.lineage()));
        assert!(matches!(cache.lookup(&key), Lookup::Miss));
        assert_eq!(retained_bytes(), before, "nothing retained");
        assert_eq!(recorded(&key), Some(2));
        let repeated = cache.insert_full(key.clone(), full, Instant::now());
        assert_eq!(repeated.lineage(), oversized.lineage());
        assert_eq!(recorded(&key), Some(2));
        // An increment growing a retained entry past the budget hands out the merged rows and leaves their record.
        let other = SeriesKey::new("p", "other", "m");
        let retained = cache.insert_full(other.clone(), vec![row("", 0, 1.0, 100)], Instant::now());
        assert_eq!(recorded(&other), None);
        let merged = cache
            .apply_increment(
                &other,
                vec![row("", 1, 2.0, 200)],
                Instant::now(),
                &base_of(&cache, &other),
            )
            .unwrap();
        assert_eq!((merged.len(), merged.lineage()), (2, retained.lineage()));
        assert!(matches!(cache.lookup(&other), Lookup::Miss));
        assert_eq!(retained_bytes(), 0, "the entry is dropped");
        assert_eq!(recorded(&other), Some(2));
        let reloaded = cache.insert_full(other.clone(), merged.to_vec(), Instant::now());
        assert_eq!(reloaded.lineage(), merged.lineage());
    }

    #[test]
    fn concurrent_full_reloads_cannot_fork_an_evicted_lineage() {
        let cache = eviction_cache(3);
        let key = SeriesKey::new("p", "r", "m");
        let first = cache.insert_full(key.clone(), vec![row("", 0, 1.0, 100)], Instant::now());
        evict(&cache, &key, 3);
        let barrier = std::sync::Barrier::new(2);
        let returned = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for step in [1, 2] {
                let (cache, key, barrier) = (&cache, &key, &barrier);
                handles.push(scope.spawn(move || {
                    barrier.wait();
                    cache.insert_full(
                        key.clone(),
                        vec![row("", 0, 1.0, 100), row("", step, 2.0, 200)],
                        Instant::now(),
                    )
                }));
            }
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(
            returned
                .iter()
                .filter(|rows| rows.lineage() == first.lineage())
                .count(),
            1
        );
        assert_ne!(returned[0].lineage(), returned[1].lineage());
    }

    /// A full read claims its series' record while it hashes its rows outside the cache's lock, and stage 2 still prices the series from it meanwhile: a refused series takes this path at every read.
    #[test]
    fn a_claimed_record_still_prices_its_series() {
        let cache = SeriesCache::with_budget(usize::MAX);
        let key = SeriesKey::new("p", "r", "m");
        let rows = filler(500_000);
        cache.insert_full(key.clone(), rows.clone(), Instant::now());
        cache.evict_for_test(&key);
        let unknown = std::thread::scope(|scope| {
            let read = scope.spawn(|| cache.insert_full(key.clone(), rows, Instant::now()));
            let mut unknown = 0;
            while !read.is_finished() {
                unknown += usize::from(matches!(
                    cache.sizes(std::slice::from_ref(&key))[..],
                    [CachedSize::Unknown]
                ));
            }
            read.join().unwrap();
            unknown
        });
        assert_eq!(unknown, 0);
        assert!(matches!(cache.lookup(&key), Lookup::Fresh(_)));
        assert!(!cache.inner.lock().unwrap().evicted.contains_key(&key));
    }

    #[test]
    fn eviction_records_are_single_use_and_purged_by_exact_run() {
        let cache = eviction_cache(1);
        let key = SeriesKey::new("p", "r", "m");
        let first = cache.insert_full(key.clone(), vec![row("", 0, 1.0, 100)], Instant::now());
        evict(&cache, &key, 1);
        let claimed = cache.inner.lock().unwrap().evicted.remove(&key).unwrap();
        let concurrent = cache.insert_full(key.clone(), first.to_vec(), Instant::now());
        assert_ne!(concurrent.lineage(), claimed.lineage);
        evict(&cache, &key, 1);
        let other_project = SeriesKey::new("other-project", "r", "m");
        let other = cache.insert_full(other_project.clone(), first.to_vec(), Instant::now());
        evict(&cache, &other_project, 1);
        cache.purge_runs([("p", "r")]);
        assert!(!cache.inner.lock().unwrap().evicted.contains_key(&key));
        let reloaded = cache.insert_full(other_project, first.to_vec(), Instant::now());
        assert_eq!(reloaded.lineage(), other.lineage());
    }

    /// Past the cap the earliest written eighth go, exactly: records written back to back (which an `Instant` often cannot order) lose no more.
    #[test]
    fn the_record_cap_drops_exactly_the_earliest_eighth() {
        let mut inner = Inner::default();
        let snapshot = SeriesSnapshot::full(filler(1));
        let key = |i: usize| SeriesKey::new("p", format!("r{i}"), "m");
        for i in 0..=EVICTED_LINEAGE_LIMIT {
            inner.record_evicted(key(i), &snapshot);
        }
        let dropped = EVICTED_LINEAGE_LIMIT / 8;
        assert_eq!(inner.evicted.len(), EVICTED_LINEAGE_LIMIT + 1 - dropped);
        assert!(!inner.evicted.contains_key(&key(dropped - 1)));
        assert!(inner.evicted.contains_key(&key(dropped)));
        assert!(inner.evicted.contains_key(&key(EVICTED_LINEAGE_LIMIT)));
    }

    /// A refused cycle of 8,192 series, twice the 4,096 records the cache once kept, finds its records at every later read: each is priced from its record and keeps its lineage.
    #[test]
    fn a_refused_cycle_of_eight_thousand_series_keeps_its_records() {
        let (cache, k) = guard_cache(1, 100, 1);
        cache.insert_full(k[0].clone(), filler(100), Instant::now());
        let keys: Vec<_> = (0..8_192)
            .map(|i| SeriesKey::new("p", format!("refused-{i}"), "m"))
            .collect();
        let mut lineages = vec![0; keys.len()];
        for wave in 0..3 {
            for (key, lineage) in keys.iter().zip(&mut lineages) {
                let priced = cache.sizes(std::slice::from_ref(key));
                assert_eq!(matches!(priced[..], [CachedSize::Evicted(_)]), wave > 0);
                let rows = cache.insert_full(key.clone(), filler(10), Instant::now());
                assert!(wave == 0 || rows.lineage() == *lineage, "wave {wave}");
                *lineage = rows.lineage();
            }
        }
        assert!(matches!(cache.lookup(&k[0]), Lookup::Fresh(_)));
    }

    #[test]
    fn cache_lineage_retains_every_prior_held_set_as_a_per_tag_step_prefix() {
        let mut comparisons = 0;
        let mut restored = 0;
        let mut refused = 0;
        let mut late = 0;
        for seed in 1..=8u64 {
            // Room for a multi-chunk series of three tags (each tag's appends are compacted chunks).
            let cache = eviction_cache(256);
            let key = SeriesKey::new("p", "r", "m");
            let mut current =
                cache.insert_full(key.clone(), vec![row("", 2, 1.0, 100)], Instant::now());
            let mut history = vec![current.clone()];
            let mut rng = crate::Rng(seed);
            for tick in 1..=120 {
                let random = rng.next();
                let tag = ["", "a", "b"][(random as usize >> 8) % 3];
                let step = current
                    .to_vec()
                    .iter()
                    .filter(|row| row.tag == tag)
                    .map(|row| row.step)
                    .max()
                    .unwrap_or(0)
                    + 2;
                let stamp = 100 + tick * 5;
                let old_lineage = current.lineage();
                if random % 9 == 8 {
                    let mut reloaded = current.to_vec();
                    if random & 32 != 0 {
                        let step = if random & 64 != 0 { -tick } else { step };
                        reloaded.push(row(tag, step, tick as f32, stamp));
                        reloaded.sort_by(|a, b| (&a.tag, a.step).cmp(&(&b.tag, b.step)));
                    }
                    evict(&cache, &key, 256);
                    current = cache.insert_full(key.clone(), reloaded, Instant::now());
                    restored += usize::from(current.lineage() == old_lineage);
                } else {
                    let mut changed = row(tag, step, tick as f32, stamp);
                    if current.len() > 0 {
                        match random % 9 {
                            3 => changed.inserted_ms = current.maximum().unwrap(),
                            4 | 5 | 7 => {
                                changed = current.to_vec()[(random as usize >> 16) % current.len()]
                                    .clone();
                                changed.inserted_ms = stamp;
                                if random % 9 == 5 {
                                    changed.value += 1.0;
                                } else if random % 9 == 7 {
                                    changed.is_value = 0;
                                }
                            }
                            6 => changed.step = -tick,
                            _ => {}
                        }
                    }
                    current = match cache.apply_increment(
                        &key,
                        vec![changed.clone()],
                        Instant::now(),
                        &base_of(&cache, &key),
                    ) {
                        Ok(rows) => rows,
                        Err(()) => {
                            refused += 1;
                            let mut full = current.to_vec();
                            full.retain(|row| row.tag != changed.tag || row.step != changed.step);
                            if changed.is_value != 0 {
                                full.push(changed);
                            }
                            full.sort_by(|a, b| (&a.tag, a.step).cmp(&(&b.tag, b.step)));
                            cache.insert_full_with_origin(
                                key.clone(),
                                full,
                                Instant::now(),
                                LineageOrigin::Rewrite,
                            )
                        }
                    };
                    late += usize::from(
                        current.lineage() != old_lineage && current.origin() == LineageOrigin::Late,
                    );
                }
                for held in history
                    .iter()
                    .filter(|held| held.lineage() == current.lineage())
                {
                    comparisons += 1;
                    let mut recovered = Vec::new();
                    let (mut last_tag, mut seen_new) = (None, false);
                    let rows = current.to_vec();
                    for row in &rows {
                        if last_tag != Some(row.tag.as_str()) {
                            last_tag = Some(row.tag.as_str());
                            seen_new = false;
                        }
                        if held
                            .maximum()
                            .is_some_and(|maximum| row.inserted_ms <= maximum)
                        {
                            assert!(
                                !seen_new,
                                "seed {seed}, tick {tick}: new step precedes held row"
                            );
                            recovered.push(row.clone());
                        } else {
                            seen_new = true;
                        }
                    }
                    assert!(
                        same_rows(&recovered, &held.to_vec()),
                        "seed {seed}, tick {tick}"
                    );
                }
                history.push(current.clone());
            }
        }
        assert!(
            comparisons > 500 && restored > 20 && refused > 100 && late > 20,
            "comparisons={comparisons}, restored={restored}, refused={refused}, late={late}"
        );
    }

    #[test]
    fn append_extends_tail() {
        let cached = vec![row("", 1, 1.0, 100), row("", 2, 2.0, 200)];
        let inc = vec![row("", 3, 3.0, 300), row("", 4, 4.0, 400)];
        let merged = merge_increment(&cached, &inc).unwrap();
        assert_eq!(merged.len(), 4);
        assert!(merged.windows(2).all(|w| w[0].step < w[1].step));
    }

    #[test]
    fn overlap_duplicates_are_dropped() {
        let cached = vec![row("", 1, 1.0, 100), row("", 2, 2.0, 200)];
        let inc = vec![row("", 2, 2.0, 200), row("", 3, 3.0, 300)];
        let merged = merge_increment(&cached, &inc).unwrap();
        assert_eq!(merged.len(), 3);
    }

    #[test]
    fn rewrite_is_detected() {
        let cached = vec![row("", 1, 1.0, 100), row("", 5, 5.0, 200)];
        // value changed at an existing step
        assert!(merge_increment(&cached, &[row("", 5, 9.9, 300)]).is_none());
        // new row before the high-water step
        assert!(merge_increment(&cached, &[row("", 3, 3.0, 300)]).is_none());
    }

    #[test]
    fn new_tags_interleave_sorted() {
        let cached = vec![row("0", 1, 1.0, 100), row("2", 1, 1.0, 100)];
        let inc = vec![row("0", 2, 2.0, 200), row("1", 1, 1.0, 200)];
        let merged = merge_increment(&cached, &inc).unwrap();
        let keys: Vec<(String, i64)> = merged.iter().map(|r| (r.tag.clone(), r.step)).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
        assert_eq!(merged.len(), 4);
    }

    #[test]
    fn nan_values_compare_by_bits() {
        let cached = vec![row("", 1, f32::NAN, 100)];
        let inc = vec![row("", 1, f32::NAN, 100)];
        // NaN != NaN must not read as a rewrite
        assert!(merge_increment(&cached, &inc).is_some());
    }

    #[test]
    fn increment_versions_keep_latest() {
        // No-FINAL reads can return several versions of one (tag, step);
        // the max-inserted_ms one wins regardless of adjacency order.
        let cached = vec![row("", 1, 1.0, 100)];
        for inc in [
            vec![row("", 2, 2.0, 300), row("", 2, 2.5, 350)],
            vec![row("", 2, 2.5, 350), row("", 2, 2.0, 300)],
        ] {
            let merged = merge_increment(&cached, &inc).unwrap();
            assert_eq!(merged.len(), 2);
            assert_eq!(merged[1].value, 2.5);
        }
        // A superseded sibling of a row the cache already holds must not
        // read as a rewrite when the surviving version matches the cache.
        let inc = vec![row("", 1, 0.5, 50), row("", 1, 1.0, 100)];
        assert_eq!(merge_increment(&cached, &inc).unwrap().len(), 1);
        // ...but a NEWER conflicting version of a cached step still is one.
        let inc = vec![row("", 1, 1.0, 100), row("", 1, 9.9, 400)];
        assert!(merge_increment(&cached, &inc).is_none());
    }

    #[test]
    fn nonnumeric_replacements_invalidate_only_cached_numeric_keys() {
        let cached = vec![row("", 1, 1.0, 100)];

        assert!(merge_increment(&cached, &[tombstone("", 1, 200)]).is_none());
        assert_eq!(
            merge_increment(&cached, &[tombstone("", 2, 200)])
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            merge_increment(&cached, &[tombstone("", 2, 200), row("", 2, 2.0, 300)])
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            merge_increment(&cached, &[tombstone("", 1, 50)])
                .unwrap()
                .len(),
            1
        );

        for conflict in [
            vec![tombstone("", 2, 200), row("", 2, 2.0, 200)],
            vec![row("", 2, 2.0, 200), tombstone("", 2, 200)],
            vec![row("", 2, 2.0, 200), row("", 2, 3.0, 200)],
        ] {
            assert!(merge_increment(&cached, &conflict).is_none());
        }

        // A strictly newer version resolves an older equal-version conflict.
        assert_eq!(
            merge_increment(
                &cached,
                &[
                    tombstone("", 2, 200),
                    row("", 2, 2.0, 200),
                    row("", 2, 3.0, 300),
                ],
            )
            .unwrap()
            .len(),
            2
        );
    }

    #[test]
    fn slow_reads_cap_the_watermark_at_their_start() {
        let cache = SeriesCache::with_budget(usize::MAX);
        let k = SeriesKey::new("p", "r", "m");
        // A row stamped "now" must not advance the watermark past
        // its read's START − margin, or rows from a still-committing concurrent flush
        // (stamped earlier, visible later) would be skipped forever. This read ran 20 s.
        let slow = Duration::from_secs(20);
        let started = Instant::now() - slow;
        let now = unix_ms_now();
        let expected = now - slow.as_millis() as i64 - VISIBILITY_MARGIN_MS;
        cache.insert_full(k.clone(), vec![row("", 1, 1.0, now)], started);
        let wm = cache.inner.lock().unwrap().map[&k].max_inserted_ms;
        assert!(wm.abs_diff(expected) < 1_000, "{wm} vs {expected}");
        // Ancient stamps are below the cap already: watermark = the stamp
        // (idle series keep their cheap empty increments).
        cache.insert_full(k.clone(), vec![row("", 1, 1.0, 100)], started);
        let wm = cache.inner.lock().unwrap().map[&k].max_inserted_ms;
        assert_eq!(wm, 100);
        cache
            .apply_increment(
                &k,
                vec![row("", 2, 2.0, now)],
                started,
                &base_of(&cache, &k),
            )
            .unwrap();
        let wm = cache.inner.lock().unwrap().map[&k].max_inserted_ms;
        assert!(wm.abs_diff(expected) < 1_000, "{wm} vs {expected}");
    }

    #[test]
    fn bump_gate_forces_stale_inside_the_fresh_window() {
        // Room for both one-row entries.
        let cache = SeriesCache::with_budget(charged(filler(64)));
        let k = SeriesKey::new("p", "r", "m");
        cache.insert_full(k.clone(), vec![row("", 1, 1.0, 100)], Instant::now());
        assert!(matches!(cache.lookup(&k), Lookup::Fresh(_)));
        // A note for the run invalidates entries stamped before
        // it, even though they are well inside FRESH_WINDOW...
        cache.note_bumps(std::iter::once("r"));
        assert!(matches!(cache.lookup(&k), Lookup::Stale { .. }));
        // ...while other runs' entries are untouched.
        let k2 = SeriesKey::new("p", "r2", "m");
        cache.insert_full(k2.clone(), vec![row("", 1, 1.0, 100)], Instant::now());
        assert!(matches!(cache.lookup(&k2), Lookup::Fresh(_)));
        // The refetch the bump triggers re-stamps past the note; the rest of
        // its burst dedups as Fresh again.
        cache
            .apply_increment(&k, vec![], Instant::now(), &base_of(&cache, &k))
            .expect("empty increment ok");
        assert!(matches!(cache.lookup(&k), Lookup::Fresh(_)));
    }

    #[test]
    fn increment_against_a_replaced_base_forces_full_rebuild() {
        let cache = SeriesCache::with_budget(charged(filler(8)));
        let k = SeriesKey::new("p", "r", "m");
        cache.insert_full(k.clone(), vec![row("", 1, 1.0, 100)], Instant::now());
        let stale_base = base_of(&cache, &k);
        // A concurrent full fetch replaces the entry while the increment (computed against stale_base's watermark) is still in flight...
        cache.insert_full(k.clone(), vec![row("", 1, 1.0, 100)], Instant::now());
        // ...so applying it must refuse: merging into the replacement could
        // bury a hole below the merged watermark that no incremental
        // re-reads.
        assert!(cache
            .apply_increment(&k, vec![row("", 2, 2.0, 300)], Instant::now(), &stale_base)
            .is_err());
        // Against the CURRENT snapshot the same increment applies fine.
        let rows = cache
            .apply_increment(
                &k,
                vec![row("", 2, 2.0, 300)],
                Instant::now(),
                &base_of(&cache, &k),
            )
            .expect("the current snapshot applies");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn bump_notes_survive_pruning_while_a_fetch_is_in_flight() {
        let cache = SeriesCache::with_budget(charged(filler(8)));
        let k = SeriesKey::new("p", "r", "m");
        // A bump noted seconds ago while a slow
        // fetch is still in flight. Backdate it directly; note_bumps always
        // stamps now. (10s, not minutes: Instant subtraction panics if it
        // would predate the platform's Instant epoch on a fresh boot.)
        let bump_at = Instant::now() - Duration::from_secs(10);
        cache
            .inner
            .lock()
            .unwrap()
            .bumps
            .insert("r".to_string(), bump_at);
        // The first note of a fresh cache prunes.
        cache.note_bumps(std::iter::once("other"));
        // The slow fetch (started before the bump) finally stores, well inside its window — only the surviving note may keep the gate closed.
        let started = bump_at - Duration::from_secs(1);
        cache.insert_full(k.clone(), vec![row("", 1, 1.0, 100)], started);
        assert!(matches!(cache.lookup(&k), Lookup::Stale { .. }));
        // Truly ancient notes (older than any possible in-flight read) do
        // get pruned.
        if let Some(ancient) = Instant::now().checked_sub(BUMP_RETENTION + Duration::from_secs(60))
        {
            let mut inner = cache.inner.lock().unwrap();
            inner.bumps.insert("ancient".to_string(), ancient);
            inner.last_prune = None;
            drop(inner);
            cache.note_bumps(std::iter::once("trigger"));
            assert!(!cache.inner.lock().unwrap().bumps.contains_key("ancient"));
        }
    }

    #[test]
    fn fresh_window_counts_from_fetch_start() {
        let cache = SeriesCache::with_budget(charged(filler(8)));
        // A fetch that started a whole window ago is stale on arrival, however recently it stored.
        if let Some(started) = Instant::now().checked_sub(FRESH_WINDOW + Duration::from_secs(1)) {
            let k = SeriesKey::new("p", "r", "m");
            cache.insert_full(k.clone(), vec![row("", 1, 1.0, 100)], started);
            assert!(matches!(cache.lookup(&k), Lookup::Stale { .. }));
        }
    }

    #[test]
    fn cache_insert_lookup_increment_evict() {
        let k1 = SeriesKey::new("p", "r1", "m");
        let k2 = SeriesKey::new("p", "r2", "m");
        let cache = SeriesCache::with_budget(entry_charge(&k2, filler(3)));

        assert!(matches!(cache.lookup(&k1), Lookup::Miss));
        cache.insert_full(k1.clone(), vec![row("", 1, 1.0, 100)], Instant::now());
        // Fresh right after insert
        assert!(matches!(cache.lookup(&k1), Lookup::Fresh(_)));

        // Empty increment refreshes the clock and keeps rows
        let rows = cache
            .apply_increment(&k1, vec![], Instant::now(), &base_of(&cache, &k1))
            .expect("empty increment ok");
        assert_eq!(rows.len(), 1);

        // Real increment extends the rows and the watermark
        let rows = cache
            .apply_increment(
                &k1,
                vec![row("", 2, 2.0, 250)],
                Instant::now(),
                &base_of(&cache, &k1),
            )
            .expect("append ok");
        assert_eq!(rows.len(), 2);
        {
            let inner = cache.inner.lock().unwrap();
            assert_eq!(inner.map[&k1].max_inserted_ms, 250);
        }

        // Room for one entry: once k1 has cooled, the LRU evicts it for k2, leaving its record.
        cache.cool_for_test();
        cache.insert_full(k2.clone(), filler(3), Instant::now());
        assert!(matches!(cache.lookup(&k1), Lookup::Miss));
        assert!(matches!(cache.lookup(&k2), Lookup::Fresh(_)));
        let inner = cache.inner.lock().unwrap();
        assert!(inner.evicted.contains_key(&k1));
        assert!(inner.total_bytes() <= cache.budget_bytes);
    }

    /// The map's bucket array is charged at its capacity, which removals never shrink by themselves: a filled-then-purged cache shrinks its table and charges nothing, and a full one charges every bucket it holds.
    #[test]
    fn the_map_table_is_charged_at_capacity_and_given_back_when_emptied() {
        let cache = SeriesCache::with_budget(usize::MAX);
        let runs: Vec<String> = (0..100_000).map(|i| format!("run-{i}")).collect();
        for run in &runs {
            cache.insert_full(
                SeriesKey::new("p", run.as_str(), "m"),
                Vec::new(),
                Instant::now(),
            );
        }
        {
            let inner = cache.inner.lock().unwrap();
            let entries: usize = inner.map.values().map(|entry| entry.bytes).sum();
            assert!(inner.map.capacity() >= 100_000);
            let table = table_bytes(inner.table_capacity);
            assert_eq!(table, table_bytes(inner.map.capacity()));
            assert!(table >= inner.map.capacity() * std::mem::size_of::<(SeriesKey, Entry)>());
            assert_eq!(inner.total_bytes(), entries + table);
        }
        assert_eq!(
            cache.purge_runs(runs.iter().map(|run| ("p", run.as_str()))),
            100_000
        );
        let inner = cache.inner.lock().unwrap();
        assert_eq!(inner.map.capacity(), 0, "an emptied table is given back");
        assert_eq!(inner.total_bytes(), 0);
    }

    /// A tag's rows in a step range, counted by binary searches over its chunks, are the rows a scan finds.
    #[test]
    fn rows_between_counts_across_chunks() {
        let rows: Vec<VersionedRawPoint> = (0..2 * COMPACT_ROWS as i64 + 1_000)
            .map(|step| row("t", 2 * step, 1.0, 1))
            .collect();
        let snapshot = SeriesSnapshot::full(rows.clone());
        let tag = snapshot.tags().next().unwrap();
        assert!(tag.chunks().count() >= 3);
        let last = 2 * (rows.len() as i64 - 1);
        for (lo, hi) in [
            (i64::MIN, i64::MAX),
            (-5, -1),
            (0, 0),
            (1, 1),
            (65_535, 65_537),
            (100, 140_000),
            (last, last + 10),
            (last + 1, i64::MAX),
            (10, 5),
        ] {
            let scanned = rows.iter().filter(|r| (lo..=hi).contains(&r.step)).count();
            assert_eq!(tag.rows_between(lo, hi), scanned, "{lo}..={hi}");
        }
    }

    /// Removals that leave tombstones lower the map's `capacity()` while its buckets stay: the table is charged at the largest capacity since it last shrank, and that capacity decides the quarter-full shrink.
    #[test]
    fn the_map_table_is_charged_at_its_buckets_after_removals() {
        for (fill, remove) in [(1_792usize, 1_000usize), (1_792, 1_500), (14_336, 12_000)] {
            let cache = SeriesCache::with_budget(usize::MAX);
            let runs: Vec<String> = (0..fill).map(|i| format!("run-{i}")).collect();
            for run in &runs {
                cache.insert_full(
                    SeriesKey::new("p", run.as_str(), "m"),
                    Vec::new(),
                    Instant::now(),
                );
            }
            let full = cache.inner.lock().unwrap().map.capacity();
            cache.purge_runs(runs[..remove].iter().map(|run| ("p", run.as_str())));
            let inner = cache.inner.lock().unwrap();
            let left = fill - remove;
            if left * 4 <= full {
                assert!(
                    inner.map.capacity() < full,
                    "fill {fill} remove {remove}: a quarter-full table shrinks"
                );
                assert_eq!(inner.table_capacity, inner.map.capacity());
            } else {
                assert_eq!(
                    inner.table_capacity, full,
                    "fill {fill} remove {remove}: the buckets stay charged"
                );
            }
        }
    }

    /// An entry that fits beside the smallest table is retained, however large a table the cache's earlier entries grew: eviction charges the table as it shrinks.
    #[test]
    fn a_grown_table_never_evicts_an_entry_that_fits() {
        let key = SeriesKey::new("p", "r", "m");
        let rows: Vec<_> = (0..200_000).map(|step| row("", step, 1.0, 1)).collect();
        let cache = SeriesCache::with_budget(entry_charge(&key, rows.clone()));
        for i in 0..3_000 {
            let empty = SeriesKey::new("p", format!("run-{i}"), "m");
            cache.insert_full(empty, Vec::new(), Instant::now());
        }
        cache.cool_for_test();
        cache.insert_full(key.clone(), rows, Instant::now());
        assert!(matches!(cache.lookup(&key), Lookup::Fresh(_)));
        assert_eq!(cache.inner.lock().unwrap().map.len(), 1);
    }

    /// An entry that fits beside the hot entries and the smallest table is retained, however large a table the cold entries grew: the LRU evicts them and shrinks it.
    #[test]
    fn a_grown_table_of_cold_entries_never_refuses_an_entry_that_fits_beside_the_hot_ones() {
        let (hot, key) = (SeriesKey::new("p", "h", "m"), SeriesKey::new("p", "r", "m"));
        let rows: Vec<_> = (0..200_000).map(|step| row("", step, 1.0, 1)).collect();
        let cache = SeriesCache::with_budget(
            entry_bytes(&hot, &SeriesSnapshot::full(filler(1)))
                + entry_bytes(&key, &SeriesSnapshot::full(rows.clone()))
                + table_bytes(2),
        );
        for i in 0..3_000 {
            let empty = SeriesKey::new("p", format!("run-{i}"), "m");
            cache.insert_full(empty, Vec::new(), Instant::now());
        }
        cache.cool_for_test();
        cache.insert_full(hot.clone(), filler(1), Instant::now());
        cache.insert_full(key.clone(), rows, Instant::now());
        assert!(matches!(cache.lookup(&key), Lookup::Fresh(_)));
        assert!(matches!(cache.lookup(&hot), Lookup::Fresh(_)));
        assert_eq!(cache.inner.lock().unwrap().map.len(), 2);
    }

    /// Room for `n` entries of `rows` filler rows under the keys `r0`, `r1`, ... (`keys` of them), beside the table one more would grow.
    fn guard_cache(n: usize, rows: usize, keys: usize) -> (SeriesCache, Vec<SeriesKey>) {
        let keys: Vec<_> = (0..keys)
            .map(|i| SeriesKey::new("p", format!("r{i}"), "m"))
            .collect();
        let entry = entry_bytes(&keys[0], &SeriesSnapshot::full(filler(rows)));
        (
            SeriesCache::with_budget(n * entry + table_bytes(n + 1)),
            keys,
        )
    }

    fn cached_keys(cache: &SeriesCache) -> Vec<String> {
        let mut runs: Vec<_> = cache
            .inner
            .lock()
            .unwrap()
            .map
            .keys()
            .map(|key| key.run_id.clone())
            .collect();
        runs.sort();
        runs
    }

    /// A new series the cache could retain only by evicting an entry read within HOT_WINDOW is served without being retained, and leaves the recovery record stage 2 prices it by and a matching full read restores its lineage from; cold entries make room as the LRU's victims.
    #[test]
    fn a_new_series_is_retained_only_in_the_room_of_cold_entries() {
        let (cache, k) = guard_cache(3, 100, 6);
        for key in &k[..3] {
            cache.insert_full(key.clone(), filler(100), Instant::now());
        }
        cache.cool_for_test();
        assert!(matches!(cache.lookup(&k[1]), Lookup::Fresh(_)));
        // The cold r0 and r2 make room, least recently read first, though r1 is hot.
        cache.insert_full(k[3].clone(), filler(100), Instant::now());
        assert_eq!(cached_keys(&cache), ["r1", "r2", "r3"]);
        cache.insert_full(k[4].clone(), filler(100), Instant::now());
        assert_eq!(cached_keys(&cache), ["r1", "r3", "r4"]);
        // Every entry is hot: r5 is served and not retained, and the cache is unchanged.
        let total = cache.inner.lock().unwrap().total_bytes();
        let refused = cache.insert_full(k[5].clone(), filler(100), Instant::now());
        assert_eq!(refused.len(), 100);
        assert!(matches!(cache.lookup(&k[5]), Lookup::Miss));
        assert_eq!(cached_keys(&cache), ["r1", "r3", "r4"]);
        assert_eq!(cache.inner.lock().unwrap().total_bytes(), total);
        assert!(matches!(
            cache.sizes(std::slice::from_ref(&k[5]))[..],
            [CachedSize::Evicted(EvictedSize { rows: 100, bytes, .. })] if bytes == refused.bytes()
        ));
        // Once they cool, its next read is retained and keeps the lineage it was served with.
        cache.cool_for_test();
        let retained = cache.insert_full(k[5].clone(), filler(100), Instant::now());
        assert_eq!(retained.lineage(), refused.lineage());
        assert_eq!(cached_keys(&cache), ["r3", "r4", "r5"]);
    }

    /// A cached series that grows, by an increment or by the full read that replaces it, evicts the least recently read entry even when it is hot: only new series wait for room.
    #[test]
    fn growth_evicts_the_least_recently_read_even_when_hot() {
        for replace in [false, true] {
            let (cache, k) = guard_cache(2, 100, 2);
            for key in &k {
                cache.insert_full(key.clone(), filler(100), Instant::now());
            }
            assert!(matches!(cache.lookup(&k[1]), Lookup::Fresh(_)));
            let grown = if replace {
                cache.insert_full(k[1].clone(), filler(101), Instant::now())
            } else {
                cache
                    .apply_increment(
                        &k[1],
                        vec![row("", 100, 0.0, 1)],
                        Instant::now(),
                        &base_of(&cache, &k[1]),
                    )
                    .unwrap()
            };
            assert_eq!(grown.len(), 101);
            assert_eq!(cached_keys(&cache), ["r1"], "replace {replace}");
            assert!(matches!(cache.lookup(&k[1]), Lookup::Fresh(rows) if rows.len() == 101));
            assert!(matches!(
                cache.sizes(std::slice::from_ref(&k[0]))[..],
                [CachedSize::Evicted(_)]
            ));
        }
    }

    /// Evictions say whether their victim was read within the window: growth's (and orphans') take hot entries, a new series only cold ones.
    #[test]
    fn evictions_count_hot_and_cold_victims() {
        let ((), rendered) = crate::with_metrics(|| {
            let (cache, k) = guard_cache(2, 100, 3);
            for key in &k[..2] {
                cache.insert_full(key.clone(), filler(100), Instant::now());
            }
            assert!(matches!(cache.lookup(&k[1]), Lookup::Fresh(_)));
            let base = base_of(&cache, &k[1]);
            cache
                .apply_increment(&k[1], vec![row("", 100, 0.0, 1)], Instant::now(), &base)
                .unwrap();
            cache.cool_for_test();
            cache.insert_full(k[2].clone(), filler(100), Instant::now());
            assert_eq!(cached_keys(&cache), ["r2"]);
        });
        for victim in ["hot", "cold"] {
            let line = format!("mkdb2_series_cache_evictions_total{{victim=\"{victim}\"}} 1");
            assert!(rendered.contains(&line), "{line} in {rendered}");
        }
    }

    /// A dashboard that reads more series than the cache holds, in the same order every poll: the LRU evicts each series just before its next read and never hits, while the window keeps the series it first retained.
    #[test]
    fn a_cyclic_dashboard_larger_than_the_cache_keeps_a_stable_share() {
        for lru in [false, true] {
            let (cache, k) = guard_cache(3, 100, 8);
            let mut hits = Vec::new();
            for _ in 0..6 {
                let mut hit = Vec::new();
                for key in &k {
                    if let Lookup::Fresh(_) = cache.lookup(key) {
                        hit.push(key.run_id.clone());
                    } else {
                        if lru {
                            // Nothing hot: the cache evicts as the LRU alone.
                            cache.cool_for_test();
                        }
                        cache.insert_full(key.clone(), filler(100), Instant::now());
                    }
                }
                hits.push(hit);
            }
            let expected: Vec<Vec<String>> = if lru {
                vec![vec![]; 6]
            } else {
                std::iter::once(vec![])
                    .chain(std::iter::repeat_n(
                        vec!["r0".into(), "r1".into(), "r2".into()],
                        5,
                    ))
                    .collect()
            };
            assert_eq!(hits, expected, "lru {lru}");
        }
    }

    /// The table a new series would grow counts against its room: with room for `n` entries in their table, but not in the doubled one another entry takes, a dashboard cycling past them keeps its `n` (a new series once fit by bytes alone, evicting a hot entry for the table, on every miss: none hit in 2,000 series of 100 rows through 9.24 MiB).
    #[test]
    fn a_new_series_that_would_grow_the_table_waits_for_cold_room() {
        for (n, keys) in [(3, 8), (1_792, 2_000)] {
            let keys: Vec<_> = (0..keys)
                .map(|i| SeriesKey::new("p", format!("r{i:04}"), "m"))
                .collect();
            let entry = entry_bytes(&keys[0], &SeriesSnapshot::full(filler(100)));
            assert!(table_bytes(n + 1) > table_bytes(n));
            let cache = SeriesCache::with_budget((n + 1) * entry + table_bytes(n));
            for wave in 0..4 {
                let mut hits = 0;
                for key in &keys {
                    if let Lookup::Fresh(_) = cache.lookup(key) {
                        hits += 1;
                    } else {
                        cache.insert_full(key.clone(), filler(100), Instant::now());
                    }
                }
                assert_eq!(hits, if wave == 0 { 0 } else { n }, "n {n}, wave {wave}");
            }
            assert_eq!(cache.inner.lock().unwrap().map.len(), n);
        }
    }

    /// A table larger than its entries need (grown by entries since replaced) is compacted before a hot entry is evicted, so the window's smallest-table admission holds: 4 empty entries grow it to 8 buckets, their replacements by 3 entries' worth fit in the 4-bucket table, and a cycle of 8 keeps those 3.
    #[test]
    fn a_grown_table_is_compacted_before_a_hot_entry_is_evicted() {
        let (_, k) = guard_cache(0, 0, 8);
        let entry = entry_bytes(&k[0], &SeriesSnapshot::full(filler(1_000)));
        let cache = SeriesCache::with_budget(3 * entry + table_bytes(3));
        for key in &k[..4] {
            cache.insert_full(key.clone(), Vec::new(), Instant::now());
        }
        assert_eq!(cache.inner.lock().unwrap().table_capacity, 7);
        for key in &k[..4] {
            cache.insert_full(key.clone(), filler(1_000), Instant::now());
        }
        assert_eq!(cached_keys(&cache), ["r0", "r1", "r2"]);
        assert_eq!(cache.inner.lock().unwrap().table_capacity, 3);
        for wave in 0..4 {
            let mut hits = 0;
            for key in &k {
                if let Lookup::Fresh(_) = cache.lookup(key) {
                    hits += 1;
                } else {
                    cache.insert_full(key.clone(), filler(1_000), Instant::now());
                }
            }
            assert_eq!(hits, 3, "wave {wave}");
        }
    }

    #[test]
    fn purge_runs_is_exact_and_removes_every_metric() {
        let cache = SeriesCache::with_budget(usize::MAX);
        let target_a = SeriesKey::new("project-a", "same-run", "loss");
        let target_b = SeriesKey::new("project-a", "same-run", "lr");
        let other_project = SeriesKey::new("project-b", "same-run", "loss");
        let target_c = SeriesKey::new("project-a", "other-run", "loss");
        let kept_run = SeriesKey::new("project-a", "kept-run", "loss");

        for key in [&target_a, &target_b, &target_c, &other_project, &kept_run] {
            cache.insert_full(key.clone(), vec![row("", 1, 1.0, 100)], Instant::now());
        }
        cache.note_bumps(std::iter::once("same-run"));

        assert_eq!(
            cache.purge_runs([("project-a", "same-run"), ("project-a", "other-run"),]),
            3
        );
        assert!(matches!(cache.lookup(&target_a), Lookup::Miss));
        assert!(matches!(cache.lookup(&target_b), Lookup::Miss));
        assert!(matches!(cache.lookup(&target_c), Lookup::Miss));
        assert!(!matches!(cache.lookup(&other_project), Lookup::Miss));
        assert!(!matches!(cache.lookup(&kept_run), Lookup::Miss));

        let inner = cache.inner.lock().unwrap();
        assert_eq!(inner.map.len(), 2);
        assert_eq!(
            inner.total_bytes(),
            inner.map.values().map(|entry| entry.bytes).sum::<usize>()
                + table_bytes(inner.table_capacity)
        );
        assert!(inner.bumps.contains_key("same-run"));
    }

    fn no_detach() -> Option<crate::clickhouse::RefreshDetach> {
        None
    }

    fn detach_ctx(
        sem: &Arc<tokio::sync::Semaphore>,
    ) -> impl FnOnce() -> Option<crate::clickhouse::RefreshDetach> {
        let sem = sem.clone();
        move || {
            Some(crate::clickhouse::RefreshDetach {
                _permit: sem.try_acquire_owned().unwrap(),
                _run_guard: std::sync::Arc::new(
                    std::sync::Arc::new(tokio::sync::RwLock::new(()))
                        .try_read_owned()
                        .unwrap(),
                ),
                _charge: Box::new(()),
            })
        }
    }

    fn plain_cache() -> Arc<SeriesCache> {
        Arc::new(SeriesCache::with_budget(1024 * 1024))
    }

    #[tokio::test]
    async fn detached_refresh_survives_requester_cancellation() {
        let cache = plain_cache();
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let sem = Arc::new(tokio::sync::Semaphore::new(2));
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let leader = {
            let (cache, locks, key, sem) = (cache.clone(), locks.clone(), key.clone(), sem.clone());
            let (refresh_cache, refresh_key, calls) = (cache.clone(), key.clone(), calls.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, detach_ctx(&sem), move |_| async move {
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let _ = gate_rx.await;
                        Ok(refresh_cache.insert_full(
                            refresh_key,
                            vec![row("", 1, 1.0, 1)],
                            Instant::now(),
                        ))
                    })
                    .await
            })
        };
        // The task holds one of the two permits once spawned.
        wait_until(|| sem.available_permits() != 2).await;

        let waiter = {
            let (cache, locks, key) = (cache.clone(), locks.clone(), key.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, no_detach, |_| async {
                        unreachable!("waiter must attach, not elect")
                    })
                    .await
            })
        };
        // Leader + task already hold two leases; the third is the waiter's,
        // so this proves the waiter joined before the leader dies.
        wait_until(|| locks.lease_count(&key) >= 3).await;
        leader.abort();
        assert!(leader.await.unwrap_err().is_cancelled());

        gate_tx.send(()).unwrap();
        let rows = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap()
            .expect("waiter shares the detached result");
        assert_eq!(rows.len(), 1);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        // The task's split permit is returned once it completes.
        wait_until(|| sem.available_permits() == 2).await;
        // The cache is populated even though the original requester died.
        assert!(matches!(
            cache.lookup_untracked(&key),
            Lookup::Fresh(_) | Lookup::Stale { .. }
        ));
    }

    /// Stage R under the window: a detached read whose requester was cancelled is retained though the window refused it, so the re-poll after its task has ended finds it instead of reading again; one its requester awaited stays refused, so detached reads keep the window.
    #[tokio::test]
    async fn an_orphaned_detached_read_the_window_refused_is_retained_for_its_re_poll() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        for cancelled in [true, false] {
            let (cache, k) = guard_cache(1, 100, 2);
            let (cache, locks) = (Arc::new(cache), Arc::new(SeriesRefreshLocks::default()));
            cache.insert_full(k[0].clone(), filler(100), Instant::now());
            let sem = Arc::new(tokio::sync::Semaphore::new(1));
            let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
            let reads = Arc::new(AtomicUsize::new(0));
            let leader = {
                let (cache, locks, key, sem) =
                    (cache.clone(), locks.clone(), k[1].clone(), sem.clone());
                let (store, reads) = (cache.clone(), reads.clone());
                tokio::spawn(async move {
                    locks
                        .get_or_refresh(
                            &cache,
                            &key.clone(),
                            detach_ctx(&sem),
                            move |_| async move {
                                reads.fetch_add(1, SeqCst);
                                let _ = gate_rx.await;
                                Ok(store.insert_full(key, filler(100), Instant::now()))
                            },
                        )
                        .await
                })
            };
            wait_until(|| sem.available_permits() == 0).await;
            if cancelled {
                leader.abort();
                assert!(leader.await.unwrap_err().is_cancelled());
                gate_tx.send(()).unwrap();
            } else {
                gate_tx.send(()).unwrap();
                assert_eq!(leader.await.unwrap().unwrap().len(), 100);
            }
            wait_until(|| sem.available_permits() == 1 && locks.registry_len() == 0).await;
            let counted = reads.clone();
            let repolled = locks
                .get_or_refresh(&cache, &k[1], no_detach, |_| async move {
                    counted.fetch_add(1, SeqCst);
                    Ok(Arc::new(SeriesSnapshot::full(filler(100))))
                })
                .await
                .unwrap();
            assert_eq!(repolled.len(), 100);
            assert_eq!(
                reads.load(SeqCst),
                if cancelled { 1 } else { 2 },
                "cancelled {cancelled}"
            );
        }
    }

    /// A publication a bump outdated is released before the next read of its series, a request's or a batch's: once its requester is done, nothing else holds a refused result, so the slot would otherwise pin a whole snapshot, charged nowhere, beside its replacement's.
    #[tokio::test]
    async fn an_outdated_publication_is_released_before_its_replacement_is_read() {
        for batch in [false, true] {
            let (cache, k) = guard_cache(1, 1_000, 2);
            let (cache, locks) = (Arc::new(cache), SeriesRefreshLocks::default());
            cache.insert_full(k[0].clone(), filler(1_000), Instant::now());
            // A queued request keeps the slot, and its publication, alive.
            let _queued = locks.lease_for(&k[1]);
            let (store, key) = (cache.clone(), k[1].clone());
            let rows = locks
                .get_or_refresh(&cache, &k[1], no_detach, move |_| async move {
                    Ok(store.insert_full(key, filler(1_000), Instant::now()))
                })
                .await
                .unwrap();
            // Refused, and its requester is done: only the slot's publication still holds it.
            let published = Arc::downgrade(&rows);
            cache.note_bumps(std::iter::once("r1"));
            drop(rows);
            assert!(published.upgrade().is_some());
            if batch {
                assert_eq!(locks.elect_misses(&cache, &k[1..], 1).len(), 1);
                assert!(published.upgrade().is_none(), "released at election");
            } else {
                locks
                    .get_or_refresh(&cache, &k[1], no_detach, move |_| async move {
                        assert!(published.upgrade().is_none(), "released before the read");
                        Ok(Arc::new(SeriesSnapshot::full(filler(1_000))))
                    })
                    .await
                    .unwrap();
            }
        }
    }

    #[tokio::test]
    async fn repoll_after_all_requesters_died_attaches_to_the_running_task() {
        let cache = plain_cache();
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let sem = Arc::new(tokio::sync::Semaphore::new(2));
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let leader = {
            let (cache, locks, key, sem) = (cache.clone(), locks.clone(), key.clone(), sem.clone());
            let (refresh_cache, refresh_key, calls) = (cache.clone(), key.clone(), calls.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, detach_ctx(&sem), move |_| async move {
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let _ = gate_rx.await;
                        Ok(refresh_cache.insert_full(
                            refresh_key,
                            vec![row("", 1, 1.0, 1)],
                            Instant::now(),
                        ))
                    })
                    .await
            })
        };
        wait_until(|| sem.available_permits() != 2).await;

        // The requester dies with no waiter in flight.
        leader.abort();
        assert!(leader.await.unwrap_err().is_cancelled());
        tokio::task::yield_now().await;

        // A later poll attaches; the scan must not run twice.
        let repoll = {
            let (cache, locks, key) = (cache.clone(), locks.clone(), key.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, no_detach, |_| async {
                        panic!("re-poll must attach to the running task, not elect")
                    })
                    .await
            })
        };
        wait_until(|| locks.lease_count(&key) >= 2).await;
        gate_tx.send(()).unwrap();
        let rows = tokio::time::timeout(Duration::from_secs(1), repoll)
            .await
            .unwrap()
            .unwrap()
            .expect("re-poll shares the detached result");
        assert_eq!(rows.len(), 1);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn attacher_after_a_bump_is_gated_off_the_running_snapshot() {
        let cache = plain_cache();
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let sem = Arc::new(tokio::sync::Semaphore::new(2));
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();

        let leader = {
            let (cache, locks, key, sem) = (cache.clone(), locks.clone(), key.clone(), sem.clone());
            let (refresh_cache, refresh_key) = (cache.clone(), key.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, detach_ctx(&sem), move |_| async move {
                        // Stamp fetch START, as production does: the gate
                        // compares bumps against when the read began.
                        let fetch_started = Instant::now();
                        let _ = gate_rx.await;
                        Ok(refresh_cache.insert_full(
                            refresh_key,
                            vec![row("", 1, 1.0, 1)],
                            fetch_started,
                        ))
                    })
                    .await
            })
        };
        wait_until(|| sem.available_permits() != 2).await;
        // The requester dies so the attacher (not the leader) receives the
        // task's outcome directly through the running slot.
        leader.abort();
        assert!(leader.await.unwrap_err().is_cancelled());

        let attacher_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attacher = {
            let (cache, locks, key) = (cache.clone(), locks.clone(), key.clone());
            let (refresh_cache, refresh_key, calls) =
                (cache.clone(), key.clone(), attacher_calls.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, no_detach, move |_| async move {
                        // Gated off the snapshot: the attacher re-elects with
                        // its own loader and serves post-bump rows.
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(refresh_cache.insert_full(
                            refresh_key,
                            vec![row("", 1, 1.0, 1)],
                            Instant::now(),
                        ))
                    })
                    .await
            })
        };
        wait_until(|| locks.lease_count(&key) >= 2).await;
        // The bump lands after the task's fetch started and after the
        // attacher's consult; the snapshot must not be served to it.
        cache.note_bumps(std::iter::once(key.run_id.as_str()));
        gate_tx.send(()).unwrap();
        let rows = tokio::time::timeout(Duration::from_secs(1), attacher)
            .await
            .unwrap()
            .unwrap()
            .expect("gated attacher re-elects and succeeds");
        assert_eq!(rows.len(), 1);
        assert_eq!(attacher_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn detached_failure_surfaces_once_and_later_waiters_reelect() {
        let cache = plain_cache();
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let leader = {
            let (cache, locks, key, sem) = (cache.clone(), locks.clone(), key.clone(), sem.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, detach_ctx(&sem), move |_| async move {
                        let _ = gate_rx.await;
                        Err(anyhow::anyhow!("scan failed").into())
                    })
                    .await
            })
        };
        let waiter = {
            let (cache, locks, key) = (cache.clone(), locks.clone(), key.clone());
            let (refresh_cache, refresh_key) = (cache.clone(), key.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, no_detach, move |_| async move {
                        Ok(refresh_cache.insert_full(
                            refresh_key,
                            vec![row("", 1, 1.0, 1)],
                            Instant::now(),
                        ))
                    })
                    .await
            })
        };
        wait_until(|| locks.lease_count(&key) >= 3).await;
        gate_tx.send(()).unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), leader)
            .await
            .unwrap()
            .unwrap()
            .expect_err("the attached leader receives the failure");
        assert_eq!(error.to_string(), "scan failed");
        let rows = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap()
            .expect("a later waiter re-elects past the failure");
        assert_eq!(rows.len(), 1);
        assert_eq!(sem.available_permits(), 1);
        assert_eq!(locks.registry_len(), 0);
    }

    #[tokio::test]
    async fn task_death_without_a_result_reports_failed_not_timeout() {
        let cache = plain_cache();
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let error = locks
            .get_or_refresh(&cache, &key, detach_ctx(&sem), |_| async {
                panic!("loader died")
            })
            .await
            .expect_err("a dead task must surface as a failure");
        assert!(matches!(error, RefreshError::Died));
        wait_until(|| sem.available_permits() == 1).await;
    }

    #[tokio::test(start_paused = true)]
    async fn detached_refresh_is_bounded_by_the_task_deadline() {
        let cache = plain_cache();
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let error = locks
            .get_or_refresh(&cache, &key, detach_ctx(&sem), |_| {
                std::future::pending::<RefreshOutcome>()
            })
            .await
            .expect_err("a stuck scan must hit the whole-task deadline");
        assert!(matches!(error, RefreshError::Timeout));
        assert_eq!(sem.available_permits(), 1);
    }

    #[tokio::test]
    async fn concurrent_oversized_misses_share_one_loader_result() {
        // Every non-empty result is pass-through rather than retained in
        // the LRU. Singleflight must still share it with current waiters.
        let cache = Arc::new(SeriesCache::with_budget(0));
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let start = Arc::new(tokio::sync::Barrier::new(9));
        let mut tasks = Vec::new();

        for _ in 0..8 {
            let cache = cache.clone();
            let refresh_cache = cache.clone();
            let locks = locks.clone();
            let key = key.clone();
            let refresh_key = key.clone();
            let calls = calls.clone();
            let start = start.clone();
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                locks
                    .get_or_refresh(&cache, &key, no_detach, move |lookup| async move {
                        assert!(matches!(lookup, Lookup::Miss));
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Ok::<_, RefreshError>(refresh_cache.insert_full(
                            refresh_key,
                            vec![row("", 1, 1.0, 100)],
                            Instant::now(),
                        ))
                    })
                    .await
                    .unwrap()
            }));
        }
        start.wait().await;
        let mut results = Vec::new();
        for task in tasks {
            results.push(task.await.unwrap());
        }
        assert!(results.iter().all(|rows| rows.len() == 1));
        assert!(results[1..]
            .iter()
            .all(|rows| Arc::ptr_eq(&results[0], rows)));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(matches!(cache.lookup(&key), Lookup::Miss));
        assert_eq!(locks.registry_len(), 0);
    }

    #[tokio::test]
    async fn bump_during_the_leader_read_elects_a_waiter_to_refresh_again() {
        let cache = Arc::new(SeriesCache::with_budget(usize::MAX));
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (leader_started_tx, leader_started_rx) = tokio::sync::oneshot::channel();
        let (release_leader_tx, release_leader_rx) = tokio::sync::oneshot::channel();

        let leader_cache = cache.clone();
        let leader_refresh_cache = cache.clone();
        let leader_locks = locks.clone();
        let leader_key = key.clone();
        let leader_refresh_key = key.clone();
        let leader_calls = calls.clone();
        let leader = tokio::spawn(async move {
            leader_locks
                .get_or_refresh(
                    &leader_cache,
                    &leader_key,
                    no_detach,
                    move |lookup| async move {
                        assert!(matches!(lookup, Lookup::Miss));
                        leader_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let fetch_started = Instant::now();
                        leader_started_tx.send(()).unwrap();
                        release_leader_rx.await.unwrap();
                        Ok::<_, RefreshError>(leader_refresh_cache.insert_full(
                            leader_refresh_key,
                            vec![row("", 1, 1.0, 100)],
                            fetch_started,
                        ))
                    },
                )
                .await
                .unwrap()
        });
        leader_started_rx.await.unwrap();

        let follower_cache = cache.clone();
        let follower_refresh_cache = cache.clone();
        let follower_locks = locks.clone();
        let follower_key = key.clone();
        let follower_refresh_key = key.clone();
        let follower_calls = calls.clone();
        let (follower_started_tx, follower_started_rx) = tokio::sync::oneshot::channel();
        let follower = tokio::spawn(async move {
            follower_started_tx.send(()).unwrap();
            follower_locks
                .get_or_refresh(
                    &follower_cache,
                    &follower_key,
                    no_detach,
                    move |lookup| async move {
                        follower_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let Lookup::Stale { base, .. } = lookup else {
                            panic!("the leader's pre-bump snapshot must remain stale");
                        };
                        Ok::<_, RefreshError>(
                            follower_refresh_cache
                                .apply_increment(
                                    &follower_refresh_key,
                                    Vec::new(),
                                    Instant::now(),
                                    &base,
                                )
                                .unwrap(),
                        )
                    },
                )
                .await
                .unwrap()
        });
        follower_started_rx.await.unwrap();
        cache.note_bumps(std::iter::once("r"));
        release_leader_tx.send(()).unwrap();

        assert_eq!(leader.await.unwrap().len(), 1);
        assert_eq!(follower.await.unwrap().len(), 1);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(matches!(cache.lookup(&key), Lookup::Fresh(_)));
    }

    #[tokio::test]
    async fn bump_invalidates_an_oversized_published_result_for_all_waiters() {
        let cache = Arc::new(SeriesCache::with_budget(0));
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (leader_started_tx, leader_started_rx) = tokio::sync::oneshot::channel();
        let (release_leader_tx, release_leader_rx) = tokio::sync::oneshot::channel();

        let leader_cache = cache.clone();
        let leader_refresh_cache = cache.clone();
        let leader_locks = locks.clone();
        let leader_key = key.clone();
        let leader_refresh_key = key.clone();
        let leader_calls = calls.clone();
        let leader = tokio::spawn(async move {
            leader_locks
                .get_or_refresh(
                    &leader_cache,
                    &leader_key,
                    no_detach,
                    move |lookup| async move {
                        assert!(matches!(lookup, Lookup::Miss));
                        leader_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let fetch_started = Instant::now();
                        leader_started_tx.send(()).unwrap();
                        release_leader_rx.await.unwrap();
                        Ok::<_, RefreshError>(leader_refresh_cache.insert_full(
                            leader_refresh_key,
                            vec![row("", 1, 1.0, 100)],
                            fetch_started,
                        ))
                    },
                )
                .await
                .unwrap()
        });
        leader_started_rx.await.unwrap();

        let mut followers = Vec::new();
        for step in 2..=3 {
            let follower_cache = cache.clone();
            let follower_refresh_cache = cache.clone();
            let follower_locks = locks.clone();
            let follower_key = key.clone();
            let follower_refresh_key = key.clone();
            let follower_calls = calls.clone();
            followers.push(tokio::spawn(async move {
                follower_locks
                    .get_or_refresh(
                        &follower_cache,
                        &follower_key,
                        no_detach,
                        move |lookup| async move {
                            assert!(matches!(lookup, Lookup::Miss));
                            follower_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            Ok::<_, RefreshError>(follower_refresh_cache.insert_full(
                                follower_refresh_key,
                                vec![row("", step, step as f32, step * 100)],
                                Instant::now(),
                            ))
                        },
                    )
                    .await
                    .unwrap()
            }));
        }

        // Prove both followers hold this slot before releasing the leader, so
        // both exercise publication rather than starting a later burst.
        wait_until(|| locks.lease_count(&key) >= 3).await;
        cache.note_bumps(std::iter::once("r"));
        release_leader_tx.send(()).unwrap();

        let stale_leader_rows = leader.await.unwrap();
        let first_follower_rows = followers.remove(0).await.unwrap();
        let second_follower_rows = followers.remove(0).await.unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(!Arc::ptr_eq(&stale_leader_rows, &first_follower_rows));
        assert!(Arc::ptr_eq(&first_follower_rows, &second_follower_rows));
        assert!(matches!(cache.lookup(&key), Lookup::Miss));
        assert_eq!(locks.registry_len(), 0);
    }

    #[tokio::test]
    async fn cancellation_and_loader_errors_release_refresh_election() {
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = SeriesKey::new("p", "r", "m");
        let leader_lease = locks.lease_for(&key);
        let leader_guard = leader_lease.lock().await;
        let (waiter_ready_tx, waiter_ready_rx) = tokio::sync::oneshot::channel();
        let waiter_locks = locks.clone();
        let waiter_key = key.clone();
        let waiter = tokio::spawn(async move {
            let lease = waiter_locks.lease_for(&waiter_key);
            waiter_ready_tx.send(()).unwrap();
            let _guard = lease.lock().await;
        });
        waiter_ready_rx.await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert_eq!(locks.registry_len(), 1);
        drop(leader_guard);
        drop(leader_lease);
        assert_eq!(locks.registry_len(), 0);

        let cache = Arc::new(SeriesCache::with_budget(usize::MAX));
        let error = locks
            .get_or_refresh(&cache, &key, no_detach, |_| async {
                Err::<Arc<SeriesSnapshot>, _>(anyhow::anyhow!("load failed").into())
            })
            .await
            .expect_err("loader error must be returned");
        assert_eq!(error.to_string(), "load failed");
        assert_eq!(locks.registry_len(), 0);

        let (leader_ready_tx, leader_ready_rx) = tokio::sync::oneshot::channel();
        let cancelled_locks = locks.clone();
        let cancelled_key = key.clone();
        let cancelled_leader = tokio::spawn(async move {
            let lease = cancelled_locks.lease_for(&cancelled_key);
            let _guard = lease.lock().await;
            leader_ready_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        leader_ready_rx.await.unwrap();
        cancelled_leader.abort();
        assert!(cancelled_leader.await.unwrap_err().is_cancelled());
        assert_eq!(locks.registry_len(), 0);
    }

    fn series_key(run: &str) -> SeriesKey {
        SeriesKey::new("p", run, "m")
    }

    #[tokio::test]
    async fn batch_election_takes_only_free_misses_up_to_the_limit() {
        let cache = plain_cache();
        let locks = SeriesRefreshLocks::default();
        let keys: Vec<_> = ["fresh", "busy", "a", "b"]
            .into_iter()
            .map(series_key)
            .collect();
        cache.insert_full(keys[0].clone(), vec![row("", 1, 1.0, 1)], Instant::now());
        let busy = locks.lease_for(&keys[1]);
        let _busy_guard = busy.lock().await;

        let elected = locks.elect_misses(&cache, &keys, 1);
        assert_eq!(
            elected.iter().map(|miss| &miss.key).collect::<Vec<_>>(),
            [&keys[2]]
        );
        // An elected slot stays locked until its batch publishes, so a second election skips it.
        let second = locks.elect_misses(&cache, &keys, usize::MAX);
        assert_eq!(
            second.iter().map(|miss| &miss.key).collect::<Vec<_>>(),
            [&keys[3]]
        );
        drop((elected, second));
        assert_eq!(locks.elect_misses(&cache, &keys, usize::MAX).len(), 2);
    }

    #[tokio::test]
    async fn a_batch_reads_once_and_outlives_its_cancelled_requester() {
        use futures::FutureExt;
        let cache = plain_cache();
        let locks = Arc::new(SeriesRefreshLocks::default());
        let keys: Vec<_> = ["a", "b"].into_iter().map(series_key).collect();
        let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let read = {
            let reads = reads.clone();
            async move {
                reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let _ = gate_rx.await;
            }
            .boxed()
            .shared()
        };
        // The batch's units, shared by its tasks.
        let sem = Arc::new(tokio::sync::Semaphore::new(2));
        let units = Arc::new(sem.clone().try_acquire_many_owned(2).unwrap());

        let elected = locks.elect_misses(&cache, &keys, usize::MAX);
        let batch = locks.spawn_batch(&cache, elected, units, |index, key| {
            let (read, cache, key) = (read.clone(), cache.clone(), key.clone());
            async move {
                // The first task ends before the read completes, as one timing out would.
                if index == 1 {
                    read.await;
                }
                Ok(cache.insert_full(key, vec![row("", 1, index as f32, 1)], Instant::now()))
            }
        });
        // Cancelled before it is ever polled: the tasks were already spawned.
        drop(batch);

        let attach = |key: SeriesKey| {
            let (cache, locks) = (cache.clone(), locks.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, no_detach, |_| async {
                        unreachable!("waiters must attach to the batch, not elect")
                    })
                    .await
            })
        };
        let first = attach(keys[0].clone()).await.unwrap().unwrap();
        assert_eq!(first.to_vec()[0].value, 0.0);
        wait_until(|| reads.load(std::sync::atomic::Ordering::SeqCst) == 1).await;
        // The finished task released nothing: the read still runs.
        assert_eq!(sem.available_permits(), 0);
        let second = attach(keys[1].clone());
        gate_tx.send(()).unwrap();
        let rows = tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap()
            .expect("the waiter shares the batch's result");
        assert_eq!(rows.to_vec()[0].value, 1.0);
        wait_until(|| sem.available_permits() == 2).await;
        assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(locks.registry_len(), 0);
    }

    #[tokio::test]
    async fn a_re_poll_after_a_cancelled_batch_attaches_instead_of_electing() {
        use futures::FutureExt;
        let cache = plain_cache();
        let locks = Arc::new(SeriesRefreshLocks::default());
        let key = series_key("a");
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let read = async move {
            let _ = gate_rx.await;
        }
        .boxed()
        .shared();
        let elected = locks.elect_misses(&cache, std::slice::from_ref(&key), 1);
        drop(locks.spawn_batch(&cache, elected, (), |_, key| {
            let (read, cache, key) = (read.clone(), cache.clone(), key.clone());
            async move {
                read.await;
                Ok(cache.insert_full(key, vec![row("", 1, 1.0, 1)], Instant::now()))
            }
        }));

        // The cancelled request's task still runs: a re-poll must attach to it, not start a second read.
        assert!(locks
            .elect_misses(&cache, std::slice::from_ref(&key), 1)
            .is_empty());
        let waiter = {
            let (cache, locks, key) = (cache.clone(), locks.clone(), key.clone());
            tokio::spawn(async move {
                locks
                    .get_or_refresh(&cache, &key, no_detach, |_| async {
                        unreachable!("the re-poll must attach to the running batch task")
                    })
                    .await
            })
        };
        gate_tx.send(()).unwrap();
        let rows = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap()
            .expect("the re-poll shares the batch's result");
        assert_eq!(rows.to_vec()[0].value, 1.0);
    }

    #[tokio::test]
    async fn a_batch_task_publishes_the_batch_stamp() {
        let cache = plain_cache();
        let locks = SeriesRefreshLocks::default();
        let key = series_key("a");
        let elected = locks.elect_misses(&cache, std::slice::from_ref(&key), 1);
        // A pass-through result, as under cache ablation, reaches others only through the slot, stamped by the task.
        drop(locks.spawn_batch(&cache, elected, (), |_, _| async {
            Ok(Arc::new(SeriesSnapshot::full_with_origin(
                vec![row("", 1, 1.0, 1)],
                LineageOrigin::Miss,
            )))
        }));
        // The single-threaded test runtime hasn't run the task yet: this bump lands after the batch's stamp.
        cache.note_bumps(std::iter::once(key.run_id.as_str()));
        let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = reads.clone();
        locks
            .get_or_refresh(&cache, &key, no_detach, |_| async move {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(Arc::new(SeriesSnapshot::full_with_origin(
                    Vec::new(),
                    LineageOrigin::Miss,
                )))
            })
            .await
            .unwrap();
        // The batch started before the bump, so its result must not satisfy a later consult.
        assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// The differential oracle: the merge and restore rules over one (tag, step)-sorted vector per snapshot, stated over maps rather than chunks. It hashes rows as the cache does, so the two contents agree, and draws lineage identifiers from its own counter; the differential test pairs them.
    mod flat {
        use super::super::{EvictedLineage, EvictedSize, LineageOrigin};
        use super::point_content;
        use crate::clickhouse::VersionedRawPoint;
        use std::collections::{BTreeMap, HashMap};
        use std::sync::atomic::{AtomicU64, Ordering};

        fn next_lineage() -> u64 {
            static NEXT: AtomicU64 = AtomicU64::new(1);
            NEXT.fetch_add(1, Ordering::Relaxed)
        }

        fn content<'a>(rows: impl Iterator<Item = &'a VersionedRawPoint>) -> u64 {
            rows.map(point_content).fold(0, u64::wrapping_add)
        }

        pub(super) struct FlatSnapshot {
            pub(super) rows: Vec<VersionedRawPoint>,
            pub(super) lineage: u64,
            pub(super) maximum: Option<i64>,
            pub(super) origin: LineageOrigin,
            pub(super) content: u64,
        }

        impl FlatSnapshot {
            /// What `SeriesCache::settle` records for an LRU victim.
            pub(super) fn evicted(&self) -> EvictedLineage {
                EvictedLineage {
                    lineage: self.lineage,
                    maximum: self.maximum,
                    origin: self.origin,
                    content: self.content,
                    size: EvictedSize {
                        rows: self.rows.len(),
                        // Pricing only: the oracle prices nothing.
                        bytes: 0,
                        steps: (i64::MAX, i64::MIN),
                        groups: 0,
                    },
                    written: 0,
                    claimed: false,
                }
            }

            /// A full read. It restores `evicted`'s lineage when the rows that record held (stamped at or below its maximum) lead each tag and match its count and content.
            pub(super) fn reload(
                rows: Vec<VersionedRawPoint>,
                origin: LineageOrigin,
                evicted: Option<&EvictedLineage>,
            ) -> Self {
                let restored = evicted.filter(|old| {
                    let held = |row: &&VersionedRawPoint| {
                        old.maximum.is_some_and(|max| row.inserted_ms <= max)
                    };
                    rows.chunk_by(|a, b| a.tag == b.tag)
                        .all(|tag| tag.iter().skip_while(held).all(|row| !held(&row)))
                        && rows.iter().filter(held).count() == old.size.rows
                        && content(rows.iter().filter(held)) == old.content
                });
                Self {
                    lineage: restored.map_or_else(next_lineage, |old| old.lineage),
                    maximum: rows.iter().map(|row| row.inserted_ms).max(),
                    origin: restored.map_or(origin, |old| old.origin),
                    content: content(rows.iter()),
                    rows,
                }
            }

            /// The increment merged in, or `None` (read in full) for disagreeing versions at a key's newest stamp, a deletion stamped at or after its held row, a held row re-logged with another value or timestamp, or a new row at or below its tag's highest held step.
            pub(super) fn merge(&self, increment: &[VersionedRawPoint]) -> Option<Self> {
                // Each key's newest version, and whether a version at that stamp disagrees with it.
                let mut newest: BTreeMap<(&str, i64), (&VersionedRawPoint, bool)> = BTreeMap::new();
                for row in increment {
                    let (kept, ambiguous) = newest
                        .entry((row.tag.as_str(), row.step))
                        .or_insert((row, false));
                    if row.inserted_ms > kept.inserted_ms {
                        (*kept, *ambiguous) = (row, false);
                    } else if row.inserted_ms == kept.inserted_ms
                        && (row.is_value != kept.is_value
                            || (row.is_value != 0
                                && (row.value.to_bits(), row.timestamp_ms)
                                    != (kept.value.to_bits(), kept.timestamp_ms)))
                    {
                        *ambiguous = true;
                    }
                }
                let held: HashMap<(&str, i64), &VersionedRawPoint> = self
                    .rows
                    .iter()
                    .map(|row| ((row.tag.as_str(), row.step), row))
                    .collect();
                // Rows are step-sorted within a tag, so its last row is its highest step.
                let high: HashMap<&str, i64> = self
                    .rows
                    .iter()
                    .map(|row| (row.tag.as_str(), row.step))
                    .collect();
                let mut added = Vec::new();
                for ((tag, step), (row, ambiguous)) in newest {
                    let refused = match held.get(&(tag, step)) {
                        _ if ambiguous => true,
                        Some(held) if row.is_value == 0 => row.inserted_ms >= held.inserted_ms,
                        Some(held) => {
                            (row.value.to_bits(), row.timestamp_ms)
                                != (held.value.to_bits(), held.timestamp_ms)
                        }
                        None if row.is_value == 0 => false,
                        None => {
                            added.push(row.clone());
                            high.get(tag).is_some_and(|&max| step <= max)
                        }
                    };
                    if refused {
                        return None;
                    }
                }
                let late = added
                    .iter()
                    .any(|row| self.maximum.is_some_and(|max| row.inserted_ms <= max));
                let mut rows = self.rows.clone();
                rows.extend(added.iter().cloned());
                rows.sort_by(|a, b| (&a.tag, a.step).cmp(&(&b.tag, b.step)));
                Some(Self {
                    rows,
                    lineage: if late { next_lineage() } else { self.lineage },
                    maximum: self
                        .maximum
                        .max(added.iter().map(|row| row.inserted_ms).max()),
                    origin: if late {
                        LineageOrigin::Late
                    } else {
                        self.origin
                    },
                    content: self.content.wrapping_add(content(added.iter())),
                })
            }
        }
    }

    pub(super) fn same_rows(a: &[VersionedRawPoint], b: &[VersionedRawPoint]) -> bool {
        a.len() == b.len()
            && a.iter().zip(b).all(|(a, b)| {
                (
                    a.tag.as_str(),
                    a.step,
                    a.timestamp_ms,
                    a.value.to_bits(),
                    a.is_value,
                    a.inserted_ms,
                ) == (
                    b.tag.as_str(),
                    b.step,
                    b.timestamp_ms,
                    b.value.to_bits(),
                    b.is_value,
                    b.inserted_ms,
                )
            })
    }

    /// The layout's invariants: tags ascend, a tag's chunks are adjacent, non-empty, share one tag allocation and ascend in step; within a tag every chunk below COMPACT_ROWS holds more than twice its successor, which bounds the count; `bytes` counts each allocation once; `content` sums the rows' hashes.
    fn assert_layout(snapshot: &SeriesSnapshot) {
        assert_chunk_layout(snapshot);
        for tag in snapshot.tags() {
            for rows in tag.chunks() {
                assert!(rows.windows(2).all(|pair| pair[0].step < pair[1].step));
            }
        }
        let content = snapshot
            .chunks
            .iter()
            .flat_map(|chunk| chunk.rows.iter().map(|row| row_content(&chunk.tag, row)))
            .fold(0u64, u64::wrapping_add);
        assert_eq!(snapshot.content, content);
    }

    /// [`assert_layout`] at chunk granularity: steps are checked across chunk boundaries only.
    fn assert_chunk_layout(snapshot: &SeriesSnapshot) {
        let mut previous_tag: Option<&str> = None;
        for tag in snapshot.tags() {
            assert!(previous_tag.is_none_or(|previous| previous < tag.tag()));
            previous_tag = Some(tag.tag());
            assert!(tag.chunks.iter().all(|chunk| {
                Arc::ptr_eq(&chunk.tag, &tag.chunks[0].tag) && !chunk.rows.is_empty()
            }));
            for pair in tag.chunks.windows(2) {
                assert!(pair[0].rows[pair[0].rows.len() - 1].step < pair[1].rows[0].step);
                assert!(
                    pair[0].rows.len() >= COMPACT_ROWS
                        || pair[0].rows.len() > 2 * pair[1].rows.len(),
                    "uncompacted chunks: {} then {}",
                    pair[0].rows.len(),
                    pair[1].rows.len()
                );
            }
            let rows: usize = tag.chunks.iter().map(|chunk| chunk.rows.len()).sum();
            assert!(
                tag.chunks.len() <= rows / COMPACT_ROWS + 15,
                "{} chunks for {rows} rows",
                tag.chunks.len()
            );
        }
        let mut tags = HashSet::new();
        let mut bytes = snapshot_overhead(snapshot.chunks.capacity());
        for chunk in &snapshot.chunks {
            if tags.insert(Arc::as_ptr(&chunk.tag) as *const u8) {
                bytes += buffer_bytes(ARC_HEADER + chunk.tag.len());
            }
            bytes += chunk_bytes(chunk.rows.capacity());
        }
        assert_eq!(snapshot.bytes(), bytes);
    }

    /// Rows of `next` in chunks `previous` does not hold: what a refresh copied or added.
    fn unshared_rows(previous: &SeriesSnapshot, next: &SeriesSnapshot) -> usize {
        next.chunks
            .iter()
            .filter(|chunk| {
                !previous
                    .chunks
                    .iter()
                    .any(|held| Arc::ptr_eq(&held.rows, &chunk.rows))
            })
            .map(|chunk| chunk.rows.len())
            .sum()
    }

    #[derive(Default, Debug)]
    struct Coverage {
        appends: usize,
        unchanged: usize,
        late: usize,
        refused: usize,
        restored: usize,
        reloads: usize,
        big_chunks: usize,
        tags_extended: usize,
    }

    /// Random refreshes applied to the flat oracle and to the chunked cache: the same rows, merge outcomes, maxima, origins and restore decisions, and lineages that pair one to one, at every step. `tags` is the tag pool, `appended` the most tags an increment extends, and `rows` the most rows it adds to one tag.
    fn differential(
        seed: u64,
        ticks: i64,
        tags: &[&str],
        appended: u64,
        rows: u64,
        coverage: &mut Coverage,
    ) {
        use flat::FlatSnapshot;
        use std::collections::BTreeMap;
        let mut rng = crate::Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        // The source table as a FINAL read sees it: (tag, step) → its newest version.
        let mut table: BTreeMap<(String, i64), VersionedRawPoint> = BTreeMap::new();
        let mut stamp = 1_000i64;
        let point = |tag: &str, step: i64, value: f32, stamp: i64| VersionedRawPoint {
            tag: tag.to_string(),
            step,
            timestamp_ms: step * 10 + 7,
            value,
            is_value: 1,
            inserted_ms: stamp,
        };
        for tag in &tags[..1 + (seed as usize % 3).min(tags.len() - 1)] {
            for step in 0..rng.below(rows * 4) as i64 {
                table.insert(
                    (tag.to_string(), step * 2),
                    point(tag, step * 2, step as f32, stamp),
                );
                stamp += 1;
            }
        }
        let full = |table: &BTreeMap<(String, i64), VersionedRawPoint>| -> Vec<VersionedRawPoint> {
            table
                .values()
                .filter(|row| row.is_value != 0)
                .cloned()
                .collect()
        };
        // A random numeric row the source holds, in O(log rows).
        let pick = |table: &BTreeMap<(String, i64), VersionedRawPoint>, random: u64| {
            let tag = tags[(random % tags.len() as u64) as usize].to_string();
            let mut range = table.range((tag.clone(), i64::MIN)..=(tag.clone(), i64::MAX));
            let (first, last) = (
                range.next()?.0 .1,
                range.next_back().map_or(i64::MIN, |row| row.0 .1),
            );
            let step = first + ((random >> 20) % (last.max(first) - first + 1) as u64) as i64;
            table
                .range((tag.clone(), step)..=(tag, i64::MAX))
                .map(|(_, row)| row)
                .find(|row| row.is_value != 0)
                .cloned()
        };
        let mut old = FlatSnapshot::reload(full(&table), LineageOrigin::Miss, None);
        let mut new = Arc::new(SeriesSnapshot::reload(
            full(&table).into(),
            LineageOrigin::Miss,
            None,
        ));
        // Lineage identifiers pair one to one.
        let mut pairs: HashMap<u64, u64> = HashMap::new();
        let mut reverse: HashMap<u64, u64> = HashMap::new();
        let mut check = |old: &FlatSnapshot, new: &SeriesSnapshot, context: &str| {
            assert!(
                new.len() == old.rows.len()
                    && new
                        .chunks
                        .iter()
                        .flat_map(|chunk| chunk.rows.iter().map(move |row| (&chunk.tag, row)))
                        .zip(&old.rows)
                        .all(|((tag, row), flat)| {
                            **tag == *flat.tag
                                && (row.step, row.timestamp_ms, row.value.to_bits())
                                    == (flat.step, flat.timestamp_ms, flat.value.to_bits())
                                && (1, row.inserted_ms) == (flat.is_value, flat.inserted_ms)
                        }),
                "{context}: rows"
            );
            assert_eq!(
                *pairs.entry(old.lineage).or_insert(new.lineage),
                new.lineage,
                "{context}: lineage"
            );
            assert_eq!(
                *reverse.entry(new.lineage).or_insert(old.lineage),
                old.lineage,
                "{context}: lineage"
            );
            assert_eq!(old.maximum, new.maximum(), "{context}: maximum");
            assert_eq!(old.origin, new.origin(), "{context}: origin");
            assert_eq!(old.content, new.content, "{context}: content");
            // The row comparison covers step order.
            assert_chunk_layout(new);
        };
        check(&old, &new, &format!("seed {seed} load"));
        for tick in 0..ticks {
            let context = format!("seed {seed} tick {tick}");
            let choice = rng.below(100);
            if choice >= 88 {
                // LRU eviction, possibly a change in the source, then a full reload against each side's record.
                let (old_record, new_record) = (old.evicted(), EvictedLineage::of(&new, 0));
                match rng.below(4) {
                    0 => {
                        let tag = rng.pick(tags);
                        let step = table
                            .range((tag.to_string(), i64::MIN)..=(tag.to_string(), i64::MAX))
                            .next_back()
                            .map_or(0, |(key, _)| key.1)
                            + 1;
                        stamp += 1;
                        table.insert((tag.to_string(), step), point(tag, step, 0.5, stamp));
                    }
                    1 => {
                        if let Some(row) = pick(&table, rng.next()) {
                            stamp += 1;
                            let held = table.get_mut(&(row.tag.clone(), row.step)).unwrap();
                            held.value += 1.0;
                            held.inserted_ms = stamp;
                        }
                    }
                    _ => {}
                }
                let rows = full(&table);
                old = FlatSnapshot::reload(rows.clone(), LineageOrigin::Miss, Some(&old_record));
                new = Arc::new(SeriesSnapshot::reload(
                    rows.into(),
                    LineageOrigin::Miss,
                    Some(&new_record),
                ));
                coverage.restored += usize::from(new.lineage() == new_record.lineage);
                coverage.reloads += 1;
                check(&old, &new, &format!("{context}: reload"));
                continue;
            }
            let maximum = old.maximum.unwrap_or(stamp);
            let mut increment: Vec<VersionedRawPoint> = Vec::new();
            // New rows past each chosen tag's high-water step, some tags new, stamped above the maximum or, for late rows, at or below it.
            let is_late = choice < 10;
            for _ in 0..1 + rng.below(appended) {
                let tag = rng.pick(tags);
                let high = table
                    .range((tag.to_string(), i64::MIN)..=(tag.to_string(), i64::MAX))
                    .next_back()
                    .map_or(-3, |(key, _)| key.1);
                let mut step = high;
                for _ in 0..1 + rng.below(rows) {
                    step += 1 + rng.below(3) as i64;
                    stamp += 1;
                    let at = if is_late {
                        maximum - rng.below(3) as i64
                    } else {
                        stamp.max(maximum + 1)
                    };
                    let value = if rng.below(17) == 0 {
                        f32::NAN
                    } else {
                        rng.below(1000) as f32 / 8.0
                    };
                    increment.push(point(tag, step, value, at));
                    if rng.below(29) == 0 {
                        // An older sibling version of the same key: deduplicated away.
                        let mut sibling = point(tag, step, -1.0, at - 1);
                        sibling.is_value = rng.below(2) as u8;
                        increment.push(sibling);
                    }
                }
            }
            // The overlap re-read: held rows come back identical, some re-stamped.
            for _ in 0..rng.below(6) {
                if let Some(mut row) = pick(&table, rng.next()) {
                    if rng.below(3) == 0 {
                        stamp += 1;
                        row.inserted_ms = stamp;
                    }
                    increment.push(row);
                }
            }
            // Tombstones the cache never held, or older than the held row: irrelevant.
            if rng.below(4) == 0 {
                let tag = rng.pick(tags);
                let step = -100 - rng.below(50) as i64;
                let mut tombstone = point(tag, step, 0.0, stamp);
                tombstone.is_value = 0;
                increment.push(tombstone);
            }
            if rng.below(5) == 0 {
                if let Some(mut tombstone) = pick(&table, rng.next()) {
                    tombstone.is_value = 0;
                    tombstone.value = 0.0;
                    tombstone.inserted_ms -= 1;
                    increment.push(tombstone);
                }
            }
            // History changes the merge must refuse.
            match choice {
                10..=14 => {
                    // A held key re-logged with a different value or timestamp.
                    if let Some(mut row) = pick(&table, rng.next()) {
                        stamp += 1;
                        row.inserted_ms = stamp;
                        if rng.below(2) == 0 {
                            row.value += 0.25;
                        } else {
                            row.timestamp_ms += 1;
                        }
                        increment.push(row);
                    }
                }
                15..=17 => {
                    // A new step below a tag's high-water step.
                    if let Some(row) = pick(&table, rng.next()) {
                        let step = row.step - 1;
                        if !table.contains_key(&(row.tag.clone(), step)) {
                            stamp += 1;
                            increment.push(point(&row.tag, step, 3.0, stamp));
                        }
                    }
                }
                18..=20 => {
                    // A deletion of a held key, newer than the held row or stamped exactly as it is.
                    if let Some(mut tombstone) = pick(&table, rng.next()) {
                        if rng.below(2) == 0 {
                            stamp += 1;
                            tombstone.inserted_ms = stamp;
                        }
                        tombstone.is_value = 0;
                        tombstone.value = 0.0;
                        increment.push(tombstone);
                    }
                }
                21..=22 => {
                    // Disagreeing versions at one stamp, of a held key or a new one.
                    stamp += 1;
                    let (tag, step) = match pick(&table, rng.next()) {
                        Some(row) if rng.below(2) == 0 => (row.tag, row.step),
                        _ => ("zz".to_string(), 1_000_000 + tick),
                    };
                    increment.push(point(&tag, step, 1.0, stamp));
                    increment.push(point(&tag, step, 2.0, stamp));
                }
                23..=24 => {
                    // A value and a nonnumeric payload at one stamp, of a held key or a new one.
                    stamp += 1;
                    let (tag, step) = match pick(&table, rng.next()) {
                        Some(row) if rng.below(2) == 0 => (row.tag, row.step),
                        _ => ("zz".to_string(), 1_000_000 + tick),
                    };
                    increment.push(point(&tag, step, 1.0, stamp));
                    let mut tombstone = point(&tag, step, 0.0, stamp);
                    tombstone.is_value = 0;
                    increment.push(tombstone);
                }
                25..=26 => increment.clear(),
                _ => {}
            }
            // ClickHouse's ORDER BY tag, step: versions of a key adjacent, in arbitrary order.
            let mut keyed: Vec<_> = increment.into_iter().map(|row| (rng.next(), row)).collect();
            keyed.sort_by(|(a, x), (b, y)| {
                (x.tag.as_str(), x.step, a).cmp(&(y.tag.as_str(), y.step, b))
            });
            let increment: Vec<VersionedRawPoint> = keyed.into_iter().map(|(_, row)| row).collect();
            let merged = (old.merge(&increment), new.merge(&increment));
            // The source applies the increment: each key keeps its newest version.
            for row in &increment {
                let key = (row.tag.clone(), row.step);
                if table
                    .get(&key)
                    .is_none_or(|held| row.inserted_ms >= held.inserted_ms)
                {
                    table.insert(key, row.clone());
                }
            }
            match merged {
                (Some(old_merged), Some(new_merged)) => {
                    let added = new_merged.len() - new.len();
                    let rows_of = |snapshot: &SeriesSnapshot| -> HashMap<String, usize> {
                        snapshot
                            .tags()
                            .map(|tag| (tag.tag().to_string(), tag.chunks().map(<[_]>::len).sum()))
                            .collect()
                    };
                    let before = rows_of(&new);
                    let extended = rows_of(&new_merged)
                        .into_iter()
                        .filter(|(tag, rows)| before.get(tag).is_none_or(|held| rows > held))
                        .count();
                    assert!(
                        unshared_rows(&new, &new_merged) <= added + extended * 2 * COMPACT_ROWS,
                        "{context}: copy bound"
                    );
                    coverage.late += usize::from(
                        new_merged.origin() == LineageOrigin::Late
                            && new_merged.lineage() != new.lineage(),
                    );
                    if added == 0 {
                        assert!(
                            Arc::ptr_eq(&new, &new_merged),
                            "{context}: an empty append shares"
                        );
                        coverage.unchanged += 1;
                    } else {
                        coverage.appends += 1;
                        coverage.tags_extended = coverage.tags_extended.max(extended);
                    }
                    coverage.big_chunks += new_merged
                        .chunks
                        .iter()
                        .filter(|chunk| {
                            chunk.rows.len() >= COMPACT_ROWS
                                && !new
                                    .chunks
                                    .iter()
                                    .any(|held| Arc::ptr_eq(&held.rows, &chunk.rows))
                        })
                        .count();
                    old = old_merged;
                    new = new_merged;
                    check(&old, &new, &format!("{context}: merge"));
                }
                (None, None) => {
                    coverage.refused += 1;
                    let rows = full(&table);
                    old = FlatSnapshot::reload(rows.clone(), LineageOrigin::Rewrite, None);
                    new = Arc::new(SeriesSnapshot::reload(
                        rows.into(),
                        LineageOrigin::Rewrite,
                        None,
                    ));
                    check(&old, &new, &format!("{context}: refused"));
                }
                (old_merged, new_merged) => panic!(
                    "{context}: flat merged={}, chunked merged={}",
                    old_merged.is_some(),
                    new_merged.is_some()
                ),
            }
        }
    }

    #[test]
    fn chunked_snapshots_match_the_flat_representation_across_random_refreshes() {
        let mut coverage = Coverage::default();
        // Many small refreshes over eight tags, up to five extended at once.
        let tags = ["", "a", "b", "c", "loss/x", "m", "q", "zz"];
        for seed in 1..=32u64 {
            differential(seed, 150, &tags, 5, 12, &mut coverage);
        }
        assert!(
            coverage.appends > 2_500
                && coverage.unchanged > 40
                && coverage.late > 200
                && coverage.refused > 500
                && coverage.restored > 100
                && coverage.reloads > 300
                && coverage.tags_extended == 5,
            "{coverage:?}"
        );
        // Bulk refreshes over two tags, so tags pass COMPACT_ROWS: full-read chunks, compacted chunks past it, and lookups across big and small chunks.
        let mut bulk = Coverage::default();
        differential(101, 60, &["", "a"], 1, 8_000, &mut bulk);
        assert!(
            bulk.appends > 30 && bulk.refused > 5 && bulk.reloads > 3 && bulk.big_chunks > 0,
            "{bulk:?}"
        );
    }

    #[test]
    fn append_only_refreshes_share_held_chunks_and_charge_them_once() {
        let cache = SeriesCache::with_budget(usize::MAX);
        let key = SeriesKey::new("p", "r", "m");
        let mut rows: Vec<_> = (0..1_000)
            .map(|step| row("", step, 1.0, 100 + step))
            .collect();
        rows.extend((0..500).map(|step| row("x", step, 2.0, 100 + step)));
        let first = cache.insert_full(key.clone(), rows, Instant::now());
        assert_eq!(first.chunks.len(), 2);
        assert_eq!(
            cache.inner.lock().unwrap().total_bytes(),
            entry_bytes(&key, &first) + table_bytes(1)
        );
        // The overlap re-read plus ten new scalar rows.
        let increment: Vec<_> = (990..1_010)
            .map(|step| row("", step, 1.0, 100 + step))
            .collect();
        let second = cache
            .apply_increment(&key, increment, Instant::now(), &base_of(&cache, &key))
            .unwrap();
        assert_eq!(second.len(), 1_510);
        assert_eq!(second.lineage(), first.lineage());
        // Every held chunk is shared; only the ten rows are new, in a chunk allocated at their number.
        assert!(Arc::ptr_eq(&first.chunks[0].rows, &second.chunks[0].rows));
        assert!(Arc::ptr_eq(&first.chunks[1].rows, &second.chunks[2].rows));
        assert!(Arc::ptr_eq(&first.chunks[0].tag, &second.chunks[1].tag));
        assert_eq!(unshared_rows(&first, &second), 10);
        assert_eq!(second.chunks[1].rows.capacity(), 10);
        assert_eq!(
            second.bytes() + snapshot_overhead(first.chunks.capacity()),
            first.bytes() + chunk_bytes(10) + snapshot_overhead(second.chunks.capacity())
        );
        // The cache charges the snapshot it retains, the shared chunks once; the replaced snapshot is charged by whoever still pins it.
        assert_eq!(
            cache.inner.lock().unwrap().total_bytes(),
            entry_bytes(&key, &second) + table_bytes(1)
        );
        // Chart admission reads the same record.
        assert!(matches!(
            &cache.sizes(std::slice::from_ref(&key))[..],
            [CachedSize::Cached(rows)] if Arc::ptr_eq(rows, &second)
        ));
        for rows in [1, 1_000, COMPACT_ROWS, COMPACT_ROWS + 1, 100_000] {
            assert_eq!(
                snapshot_bytes(rows),
                SeriesSnapshot::full((0..rows as i64).map(|step| row("", step, 1.0, 1)).collect())
                    .bytes(),
                "the estimate of a read of {rows} rows is what one tag of it costs"
            );
        }
        // A refresh that adds nothing keeps the held snapshot.
        let third = cache
            .apply_increment(
                &key,
                vec![row("x", 499, 2.0, 599)],
                Instant::now(),
                &base_of(&cache, &key),
            )
            .unwrap();
        assert!(Arc::ptr_eq(&second, &third));
        assert!(Arc::ptr_eq(
            &cache.inner.lock().unwrap().map[&key].rows,
            &second
        ));
    }

    #[test]
    fn a_full_read_fills_chunks_at_their_length() {
        let mut read = FullRead::default();
        for step in 0..(2 * COMPACT_ROWS + 5) as i64 {
            read.push("", CachedRow::of(&row("", step, 0.0, step)))
                .unwrap();
        }
        for step in 0..3 {
            read.push("t", CachedRow::of(&row("t", step, 0.0, step)))
                .unwrap();
        }
        let snapshot = SeriesSnapshot::reload(read, LineageOrigin::Miss, None);
        let shape: Vec<_> = snapshot
            .chunks
            .iter()
            .map(|chunk| (&*chunk.tag, chunk.rows.len(), chunk.rows.capacity()))
            .collect();
        assert_eq!(
            shape,
            [
                ("", COMPACT_ROWS, COMPACT_ROWS),
                ("", COMPACT_ROWS, COMPACT_ROWS),
                ("", 5, 5),
                ("t", 3, 3)
            ]
        );
        assert!(Arc::ptr_eq(
            &snapshot.chunks[0].tag,
            &snapshot.chunks[2].tag
        ));
        assert_layout(&snapshot);
    }

    /// A full read whose rows leave (tag, step) order, or repeat a (tag, step), fails, in a release build too; a chunk boundary or a new tag's first step changes nothing.
    #[test]
    fn a_full_read_out_of_order_fails() {
        let read = |rows: &[(&str, i64)]| {
            let mut read = FullRead::default();
            rows.iter()
                .try_for_each(|&(tag, step)| read.push(tag, CachedRow::of(&row(tag, step, 0.0, 1))))
        };
        assert!(read(&[("a", 5), ("a", 7), ("b", -3), ("c", -1_000)]).is_ok());
        let across: Vec<_> = (0..=COMPACT_ROWS as i64).map(|step| ("a", step)).collect();
        assert!(read(&across).is_ok());
        for rows in [
            &[("a", 1), ("a", 0)][..],
            &[("a", 1), ("a", 1)],
            &[("b", 0), ("a", 1)],
            &[("a", 0), ("b", 0), ("a", 1)],
        ] {
            assert!(read(rows).is_err(), "{rows:?}");
        }
        let mut late = across;
        late.push(("a", 3));
        assert!(read(&late).is_err(), "after a sealed chunk");
    }

    #[test]
    fn compaction_bounds_chunks_and_the_rows_a_refresh_copies() {
        let mut rng = crate::Rng(0x2545_F491_4F6C_DD1D);
        // Tiny appends (the common poll), then large ones (a backlog), on a series loaded with full chunks.
        let mut snapshot = Arc::new(SeriesSnapshot::full(
            (0..50_000).map(|step| row("", step, 0.0, step)).collect(),
        ));
        let mut expected = snapshot.to_vec();
        let mut next = 50_000i64;
        for round in 0..1_600 {
            let count = if round < 1_500 {
                1 + rng.below(40)
            } else {
                1 + rng.below(9_000)
            } as i64;
            let increment: Vec<_> = (next..next + count)
                .map(|step| row("", step, 1.0, step))
                .collect();
            next += count;
            expected.extend(increment.iter().cloned());
            let merged = snapshot.merge(&increment).unwrap();
            assert!(unshared_rows(&snapshot, &merged) <= count as usize + 2 * COMPACT_ROWS);
            assert!(merged
                .chunks
                .iter()
                .all(|chunk| chunk.rows.capacity() == chunk.rows.len()));
            assert_chunk_layout(&merged);
            snapshot = merged;
        }
        assert_layout(&snapshot);
        assert!(same_rows(&snapshot.to_vec(), &expected));
        assert!(snapshot.chunks.len() > 2);
    }

    #[test]
    fn increments_out_of_tag_step_order_are_refused() {
        let held = Arc::new(SeriesSnapshot::full(vec![
            row("", 1, 1.0, 100),
            row("b", 1, 1.0, 100),
        ]));
        // ClickHouse orders increments by (tag, step); an unordered one is read again in full.
        assert!(held
            .merge(&[row("", 3, 1.0, 200), row("", 2, 1.0, 200)])
            .is_none());
        assert!(held
            .merge(&[row("b", 2, 1.0, 200), row("a", 1, 1.0, 200)])
            .is_none());
        // A tombstone out of order is refused too, though deduplication drops it before any value row is placed.
        assert!(held
            .merge(&[
                row("", 2, 1.0, 200),
                tombstone("c", 5, 200),
                row("a", 1, 1.0, 200)
            ])
            .is_none());
        let merged = held
            .merge(&[
                row("", 2, 1.0, 200),
                row("a", 1, 1.0, 200),
                row("b", 2, 1.0, 200),
            ])
            .unwrap();
        assert!(same_rows(
            &merged.to_vec(),
            &[
                row("", 1, 1.0, 100),
                row("", 2, 1.0, 200),
                row("a", 1, 1.0, 200),
                row("b", 1, 1.0, 100),
                row("b", 2, 1.0, 200),
            ]
        ));
        assert_layout(&merged);
    }

    /// glibc's chunk for a request: the request and an 8-byte header, 16-byte aligned, at least 32 bytes; mmapped as whole pages with the header once that chunk reaches the 128 KiB threshold (the lowest it can be).
    fn glibc_chunk(bytes: usize) -> usize {
        let chunk = ((bytes + 8 + 15) & !15).max(32);
        if chunk >= 128 << 10 {
            (chunk + 8).next_multiple_of(4096)
        } else {
            chunk
        }
    }

    #[test]
    fn the_allocation_models_bound_their_allocators() {
        for bytes in (1..=300_000).chain((1..4_000).map(|n| n * 25_013)) {
            assert!(
                glibc_allocation_bytes(bytes) >= glibc_chunk(bytes),
                "glibc: {bytes} bytes"
            );
        }
        // At the mmap threshold, compared with the chunk: 4,094 and 4,095 rows of 32 bytes.
        assert!(glibc_allocation_bytes(131_008) >= 131_024);
        assert!(glibc_allocation_bytes(131_040) >= 135_168);
        // Below 2 MiB: from there macOS can serve a request from a larger freed block it keeps cached.
        #[cfg(target_os = "macos")]
        for bytes in (1..=70_000).chain((70..2_097).map(|n| n * 1_000 + 7)) {
            // SAFETY: a fresh allocation, measured and freed at once.
            let held = unsafe {
                let pointer = libc::malloc(bytes);
                let held = libc::malloc_size(pointer);
                libc::free(pointer);
                held
            };
            assert!(
                macos_allocation_bytes(bytes) >= held,
                "macOS: {bytes} bytes"
            );
        }
    }

    /// Bytes the process allocator reports in use (allocation sizes as rounded by the allocator).
    fn allocator_bytes_in_use() -> Option<usize> {
        #[cfg(target_os = "macos")]
        {
            let mut stats = libc::malloc_statistics_t {
                blocks_in_use: 0,
                size_in_use: 0,
                max_size_in_use: 0,
                size_allocated: 0,
            };
            // SAFETY: a null zone asks for every zone's totals; `stats` is a valid out-pointer.
            unsafe { libc::malloc_zone_statistics(std::ptr::null_mut(), &mut stats) };
            Some(stats.size_in_use)
        }
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        {
            // SAFETY: mallinfo2 only reads the allocator's counters.
            let info = unsafe { libc::mallinfo2() };
            Some(info.uordblks + info.hblkhd)
        }
        #[cfg(not(any(target_os = "macos", all(target_os = "linux", target_env = "gnu"))))]
        {
            None
        }
    }

    /// Refresh cost per append, at the doc's sizes. Run alone:
    /// `cargo test --release -p kymo-server --lib series_cache_append_benchmark -- --ignored --nocapture --test-threads=1`
    #[test]
    #[ignore = "local release benchmark; no CI timing job"]
    fn series_cache_append_benchmark() {
        const APPENDS: usize = 4096;
        const NEW: i64 = 16;
        // A refresh re-reads the visibility margin: rows the cache already holds come back with the new ones.
        const OVERLAP: i64 = 16;
        for size in [100_000i64, 1_000_000] {
            let cache = SeriesCache::with_budget(usize::MAX);
            let key = SeriesKey::new("p", "r", "m");
            let point = |step: i64| row("", step, step as f32 * 0.5, 1_000 + step);
            cache.insert_full(key.clone(), (0..size).map(point).collect(), Instant::now());
            let mut times = Vec::with_capacity(APPENDS);
            let mut next = size;
            for _ in 0..APPENDS {
                let increment: Vec<_> = (next - OVERLAP..next + NEW).map(point).collect();
                let base = base_of(&cache, &key);
                let start = Instant::now();
                let rows = cache
                    .apply_increment(&key, increment, Instant::now(), &base)
                    .unwrap();
                times.push(start.elapsed().as_secs_f64() * 1e3);
                assert_eq!(rows.len() as i64, next + NEW);
                drop(std::hint::black_box(rows));
                next += NEW;
            }
            let mean = times.iter().sum::<f64>() / times.len() as f64;
            times.sort_by(f64::total_cmp);
            println!(
                "BENCH append rows={size} appends={APPENDS} new={NEW} overlap={OVERLAP} median_ms={:.4} mean_ms={:.4} p99_ms={:.4} max_ms={:.4}",
                times[times.len() / 2],
                mean,
                times[times.len() * 99 / 100],
                times[times.len() - 1]
            );
        }
    }

    /// Memory of a cache full of typical series: rows retained, the cache's accounted bytes, and the allocator's bytes in use. Run alone (the allocator counters are process-wide):
    /// `cargo test --release -p kymo-server --lib series_cache_memory_benchmark -- --ignored --nocapture --test-threads=1`
    #[test]
    #[ignore = "local release benchmark; no CI timing job"]
    fn series_cache_memory_benchmark() {
        const BUDGET: usize = 256 << 20;
        // The prod-average series (26k rows): a full load of 20k rows, then 300 refreshes of 20 new rows, each re-reading 10.
        const LOADED: i64 = 20_000;
        const REFRESHES: i64 = 300;
        const NEW: i64 = 20;
        const OVERLAP: i64 = 10;
        for tags in [
            &[""][..],
            &[
                "layer.00.grad_norm",
                "layer.01.grad_norm",
                "layer.02.grad_norm",
                "layer.03.grad_norm",
            ][..],
        ] {
            let before = allocator_bytes_in_use();
            let cache = SeriesCache::with_budget(BUDGET);
            let t = tags.len() as i64;
            // (tag, step)-ordered, as ClickHouse returns them; the tag names are sorted.
            let rows_in = |from: i64, to: i64| -> Vec<VersionedRawPoint> {
                tags.iter()
                    .flat_map(|tag| {
                        (from..to).map(move |step| row(tag, step, step as f32, 1_000 + step))
                    })
                    .collect()
            };
            let mut series = 0usize;
            let mut stored_after_first_eviction = None;
            loop {
                let key = SeriesKey::new("p", format!("run-{series}"), "loss");
                cache.insert_full(key.clone(), rows_in(0, LOADED / t), Instant::now());
                for refresh in 0..REFRESHES {
                    let end = LOADED / t + (refresh + 1) * (NEW / t);
                    let base = base_of(&cache, &key);
                    cache
                        .apply_increment(
                            &key,
                            rows_in(end - NEW / t - OVERLAP / t, end),
                            Instant::now(),
                            &base,
                        )
                        .unwrap();
                }
                series += 1;
                let evicted = !cache.inner.lock().unwrap().evicted.is_empty();
                if evicted && stored_after_first_eviction.is_none() {
                    stored_after_first_eviction = Some(series);
                }
                if stored_after_first_eviction.is_some_and(|first| series >= first + first / 10) {
                    break;
                }
            }
            let after = allocator_bytes_in_use();
            let inner = cache.inner.lock().unwrap();
            let rows: usize = inner.map.values().map(|entry| entry.rows.len()).sum();
            let real = before
                .zip(after)
                .map(|(before, after)| after.saturating_sub(before));
            println!(
                "BENCH memory tags={} budget_bytes={BUDGET} entries={} rows={rows} accounted_bytes={} allocator_bytes={} allocator_per_row={:.1} accounted_per_row={:.1}",
                tags.len(),
                inner.map.len(),
                inner.total_bytes(),
                real.map_or("n/a".into(), |real| real.to_string()),
                real.map_or(f64::NAN, |real| real as f64 / rows as f64),
                inner.total_bytes() as f64 / rows as f64,
            );
        }
    }

    /// A cold full read end to end through the client (decode, chunking, hashing, storing) from a local mock of ClickHouse that serves the rows already encoded: the median time, and the allocator's peak above its level before the read (sampled every 50 µs), per row. Run alone:
    /// `cargo test --release -p kymo-server --lib series_cache_cold_read_benchmark -- --ignored --nocapture --test-threads=1`
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "local release benchmark; no CI timing job"]
    async fn series_cache_cold_read_benchmark() {
        use std::sync::atomic::{AtomicBool, Ordering};
        #[derive(serde::Serialize, ::clickhouse::Row)]
        struct Encoded {
            run_id: String,
            tag: String,
            step: i64,
            timestamp_ms: i64,
            value: f32,
            inserted_ms: i64,
        }
        for (rows, tags) in [
            (100_000i64, 1i64),
            (100_000, 4),
            (1_000_000, 1),
            (1_000_000, 4),
        ] {
            let names: Vec<String> = (0..tags)
                .map(|i| {
                    if tags == 1 {
                        String::new()
                    } else {
                        format!("layer.{i:02}.grad_norm")
                    }
                })
                .collect();
            let mock = ::clickhouse::test::Mock::new();
            let client = crate::clickhouse::ChClient::new(mock.url()).unwrap();
            let (mut times, mut peaks) = (Vec::new(), Vec::new());
            for _ in 0..7 {
                client.purge_run_caches(&[("p", "r")]);
                mock.add(::clickhouse::test::handlers::provide(
                    names.iter().flat_map(|tag| {
                        (0..rows / tags).map(move |step| Encoded {
                            run_id: "r".into(),
                            tag: tag.clone(),
                            step,
                            timestamp_ms: 1_000 + step * 10,
                            value: step as f32 * 0.5,
                            inserted_ms: 1_000 + step,
                        })
                    }),
                ));
                let before = allocator_bytes_in_use().unwrap_or(0);
                let stop = Arc::new(AtomicBool::new(false));
                let sampler = {
                    let stop = stop.clone();
                    std::thread::spawn(move || {
                        let mut peak = 0;
                        while !stop.load(Ordering::Relaxed) {
                            peak = peak.max(allocator_bytes_in_use().unwrap_or(0));
                            std::thread::sleep(Duration::from_micros(50));
                        }
                        peak.max(allocator_bytes_in_use().unwrap_or(0))
                    })
                };
                let start = Instant::now();
                let snapshot = client
                    .query_raw_any_cached("p", "r", "m", || None)
                    .await
                    .unwrap();
                times.push(start.elapsed().as_secs_f64() * 1e3);
                stop.store(true, Ordering::Relaxed);
                peaks.push(sampler.join().unwrap().saturating_sub(before) as f64 / rows as f64);
                assert_eq!(snapshot.len() as i64, rows);
            }
            times.sort_by(f64::total_cmp);
            peaks.sort_by(f64::total_cmp);
            println!(
                "BENCH cold_read rows={rows} tags={tags} median_ms={:.2} min_ms={:.2} peak_bytes_per_row={:.1}",
                times[times.len() / 2],
                times[0],
                peaks[peaks.len() / 2]
            );
        }
    }

    /// The cache's lock under refreshes that extend many tags at once: 64 tags of 20k rows, 1,024 refreshes of 16 new rows per tag, while another thread times lookups of an unrelated entry, each of which waits for the lock. Run alone:
    /// `cargo test --release -p kymo-server --lib series_cache_lockstep_tags_benchmark -- --ignored --nocapture --test-threads=1`
    #[test]
    #[ignore = "local release benchmark; no CI timing job"]
    fn series_cache_lockstep_tags_benchmark() {
        use std::sync::atomic::{AtomicBool, Ordering};
        const TAGS: usize = 64;
        const LOADED: i64 = 20_000;
        const REFRESHES: i64 = 1_024;
        const NEW: i64 = 16;
        let cache = Arc::new(SeriesCache::with_budget(usize::MAX));
        let key = SeriesKey::new("p", "r", "m");
        let probe = SeriesKey::new("p", "probe", "m");
        cache.insert_full(probe.clone(), vec![row("", 0, 0.0, 1)], Instant::now());
        let tags: Vec<String> = (0..TAGS).map(|i| format!("t{i:02}")).collect();
        let point = |tag: &str, step: i64| row(tag, step, step as f32, 1_000 + step);
        cache.insert_full(
            key.clone(),
            tags.iter()
                .flat_map(|tag| (0..LOADED).map(move |step| point(tag, step)))
                .collect(),
            Instant::now(),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let prober = {
            let (cache, stop, probe) = (cache.clone(), stop.clone(), probe.clone());
            std::thread::spawn(move || {
                let mut waits = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let start = Instant::now();
                    drop(std::hint::black_box(cache.lookup(&probe)));
                    waits.push(start.elapsed().as_secs_f64() * 1e3);
                }
                waits
            })
        };
        let mut times = Vec::new();
        for refresh in 0..REFRESHES {
            let from = LOADED + refresh * NEW;
            let increment: Vec<_> = tags
                .iter()
                .flat_map(|tag| (from..from + NEW).map(move |step| point(tag, step)))
                .collect();
            let base = base_of(&cache, &key);
            let start = Instant::now();
            let rows = cache
                .apply_increment(&key, increment, Instant::now(), &base)
                .unwrap();
            times.push(start.elapsed().as_secs_f64() * 1e3);
            drop(std::hint::black_box(rows));
        }
        stop.store(true, Ordering::Relaxed);
        let mut waits = prober.join().unwrap();
        let mean = times.iter().sum::<f64>() / times.len() as f64;
        times.sort_by(f64::total_cmp);
        waits.sort_by(f64::total_cmp);
        println!(
            "BENCH lockstep tags={TAGS} refreshes={REFRESHES} refresh_median_ms={:.4} refresh_mean_ms={:.4} refresh_max_ms={:.3} lookups={} lookup_p999_ms={:.4} lookup_max_ms={:.3}",
            times[times.len() / 2],
            mean,
            times[times.len() - 1],
            waits.len(),
            waits[waits.len() * 999 / 1000],
            waits[waits.len() - 1]
        );
    }
}
