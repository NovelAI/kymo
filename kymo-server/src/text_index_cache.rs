//! Bounded cache of line-count metadata for text streams.
//!
//! A line offset cannot be mapped to ClickHouse rows without first counting
//! delimiters in the preceding text. Retaining only that small per-chunk
//! index keeps scroll-band reads from repeatedly decompressing the full log.
//! Refreshes use the table's `inserted_at` skip index and replace entries by
//! the full text-row key `(step, metric_name, tag)`.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::series_cache::{VISIBILITY_MARGIN_MS, WATERMARK_OVERLAP_MS};

const FRESH_WINDOW: Duration = Duration::from_millis(250);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TextIndexKey {
    project_id: String,
    run_id: String,
    metric_names: Vec<String>,
}

impl TextIndexKey {
    pub fn new(project_id: &str, run_id: &str, metric_names: &[String]) -> Self {
        Self {
            project_id: project_id.to_string(),
            run_id: run_id.to_string(),
            metric_names: metric_names.to_vec(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, clickhouse::Row)]
pub struct TextIndexRow {
    pub step: i64,
    pub metric_name: String,
    pub tag: String,
    pub is_text: u8,
    pub separator_count: u64,
    pub non_empty: u8,
    pub ends_with_newline: u8,
    pub normalized_bytes: u64,
    pub inserted_ms: i64,
}

#[derive(Debug, Clone)]
pub struct IndexedTextChunk {
    pub step: i64,
    pub metric_name: String,
    pub tag: String,
    separator_count: u64,
    non_empty: bool,
    ends_with_newline: bool,
    pub normalized_bytes: u64,
    inserted_ms: i64,
    pub lines_before: u64,
    pub lines_after: u64,
}

impl IndexedTextChunk {
    /// Whether this chunk holds any normalized text. Zero-byte entries are semantically inert: one vanishing between the window's non-atomic index and payload reads loses nothing, so gap detection must not fire on them.
    pub(crate) fn carries_content(&self) -> bool {
        self.non_empty
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        step: i64,
        metric_name: &str,
        tag: &str,
        lines_before: u64,
        // pass NORMALIZED text (LF-only): production metadata derives from the post-\r\n/\r normalization expression, which this helper does not reapply
        text: &str,
    ) -> Self {
        let separator_count = text.matches('\n').count() as u64;
        Self {
            step,
            metric_name: metric_name.to_string(),
            tag: tag.to_string(),
            separator_count,
            non_empty: !text.is_empty(),
            ends_with_newline: text.ends_with('\n'),
            normalized_bytes: text.len() as u64,
            inserted_ms: 0,
            lines_before,
            lines_after: lines_before + separator_count,
        }
    }

    fn from_row(row: TextIndexRow) -> Self {
        Self {
            step: row.step,
            metric_name: row.metric_name,
            tag: row.tag,
            separator_count: row.separator_count,
            non_empty: row.non_empty != 0,
            ends_with_newline: row.ends_with_newline != 0,
            normalized_bytes: row.normalized_bytes,
            inserted_ms: row.inserted_ms,
            lines_before: 0,
            lines_after: 0,
        }
    }

    fn into_row(self) -> TextIndexRow {
        TextIndexRow {
            step: self.step,
            metric_name: self.metric_name,
            tag: self.tag,
            is_text: 1,
            separator_count: self.separator_count,
            non_empty: u8::from(self.non_empty),
            ends_with_newline: u8::from(self.ends_with_newline),
            normalized_bytes: self.normalized_bytes,
            inserted_ms: self.inserted_ms,
        }
    }
}

#[derive(Debug)]
pub struct TextStreamIndex {
    pub first_step: i64,
    pub total_lines: u64,
    pub chunks: Vec<IndexedTextChunk>,
    max_inserted_ms: i64,
}

impl TextStreamIndex {
    fn from_rows(rows: Vec<TextIndexRow>, observed_max_inserted_ms: i64) -> Self {
        let rows = latest_rows(rows);
        let max_inserted_ms = rows
            .iter()
            .map(|row| row.inserted_ms)
            .max()
            .unwrap_or(0)
            .max(observed_max_inserted_ms)
            .min(unix_ms_now() - VISIBILITY_MARGIN_MS);
        let rows = rows
            .into_iter()
            .filter(|row| row.is_text != 0)
            .collect::<Vec<_>>();
        let first_step = rows.first().map_or(0, |row| row.step);
        let mut completed_lines = 0u64;
        let mut last_non_empty_ends_with_newline = None;
        let mut chunks = Vec::with_capacity(rows.len());
        for row in rows {
            let mut chunk = IndexedTextChunk::from_row(row);
            chunk.lines_before = completed_lines;
            completed_lines = completed_lines.saturating_add(chunk.separator_count);
            chunk.lines_after = completed_lines;
            if chunk.non_empty {
                last_non_empty_ends_with_newline = Some(chunk.ends_with_newline);
            }
            chunks.push(chunk);
        }
        let total_lines = completed_lines
            .saturating_add(u64::from(last_non_empty_ends_with_newline == Some(false)));
        Self {
            first_step,
            total_lines,
            chunks,
            max_inserted_ms,
        }
    }

    pub fn window_chunks(&self, offset: u64, limit: u32) -> &[IndexedTextChunk] {
        if offset >= self.total_lines {
            return &[];
        }
        let end_line = offset
            .saturating_add(u64::from(limit))
            .min(self.total_lines);
        let end = self
            .chunks
            .partition_point(|chunk| chunk.lines_before < end_line);
        let mut start = self
            .chunks
            .partition_point(|chunk| chunk.lines_after < offset);
        // Chunks ending exactly at the offset's line boundary contribute nothing to the window; bounding by end keeps a zero-line window an empty slice.
        while self.chunks[..end].get(start).is_some_and(|chunk| {
            chunk.lines_after == offset && (!chunk.non_empty || chunk.ends_with_newline)
        }) {
            start += 1;
        }
        &self.chunks[start..end]
    }
}

fn unix_ms_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn row_key_cmp(left: &TextIndexRow, right: &TextIndexRow) -> Ordering {
    left.step
        .cmp(&right.step)
        .then_with(|| left.metric_name.cmp(&right.metric_name))
        .then_with(|| left.tag.cmp(&right.tag))
}

fn same_key(left: &TextIndexRow, right: &TextIndexRow) -> bool {
    left.step == right.step && left.metric_name == right.metric_name && left.tag == right.tag
}

/// Sort into display order and retain the newest version of every table key.
/// Incremental reads deliberately omit FINAL so the inserted_at skip index can
/// prune old granules; overlap can therefore return several row versions. The
/// stable sort makes a newly appended incremental row win when timestamps tie.
fn latest_rows(mut rows: Vec<TextIndexRow>) -> Vec<TextIndexRow> {
    rows.sort_by(|left, right| {
        row_key_cmp(left, right).then_with(|| left.inserted_ms.cmp(&right.inserted_ms))
    });
    let mut latest = Vec::<TextIndexRow>::with_capacity(rows.len());
    for row in rows {
        if latest.last().is_some_and(|current| same_key(current, &row)) {
            *latest.last_mut().expect("checked above") = row;
        } else {
            latest.push(row);
        }
    }
    latest
}

fn default_budget_bytes() -> usize {
    crate::env::required_mebibytes("KYMO_TEXT_INDEX_CACHE_MB", 64)
        .expect("invalid kymo text-index-cache environment")
}

fn index_bytes(index: &TextStreamIndex) -> usize {
    std::mem::size_of::<TextStreamIndex>()
        .saturating_add(
            index
                .chunks
                .capacity()
                .saturating_mul(std::mem::size_of::<IndexedTextChunk>()),
        )
        .saturating_add(index.chunks.iter().fold(0usize, |total, chunk| {
            total
                .saturating_add(chunk.metric_name.capacity())
                .saturating_add(chunk.tag.capacity())
        }))
}

struct Entry {
    index: Arc<TextStreamIndex>,
    started: Instant,
    checked: Instant,
    generation: u64,
    last_access: Instant,
    bytes: usize,
}

fn key_bytes(key: &TextIndexKey) -> usize {
    std::mem::size_of::<TextIndexKey>()
        .saturating_add(key.project_id.capacity())
        .saturating_add(key.run_id.capacity())
        .saturating_add(
            key.metric_names
                .capacity()
                .saturating_mul(std::mem::size_of::<String>()),
        )
        .saturating_add(key.metric_names.iter().fold(0usize, |total, metric_name| {
            total.saturating_add(metric_name.capacity())
        }))
}

fn entry_bytes(key: &TextIndexKey, index: &TextStreamIndex) -> usize {
    // HashMap's bucket/control allocation is implementation-defined. A small
    // fixed allowance keeps the accounting conservative without depending on
    // hashbrown internals; the owned key, entry, Arc target, chunk vector, and
    // strings are all charged exactly from their capacities.
    const MAP_BUCKET_ALLOWANCE: usize = 32;
    key_bytes(key)
        .saturating_add(std::mem::size_of::<Entry>())
        .saturating_add(index_bytes(index))
        .saturating_add(MAP_BUCKET_ALLOWANCE)
}

/// Per-stream refresh serialization. The lease removes its weak registry key
/// when the last leader/waiter drops, including cancellation while awaiting
/// the Tokio mutex, so request-derived keys cannot become a second cache
/// outside the byte budget.
pub type TextRefreshLocks = crate::refresh_locks::RefreshLocks<TextIndexKey, ()>;

#[derive(Default)]
struct Inner {
    entries: HashMap<TextIndexKey, Entry>,
    total_bytes: usize,
    next_generation: u64,
}

pub enum Lookup {
    Fresh(Arc<TextStreamIndex>),
    Stale { watermark_ms: i64, generation: u64 },
    Miss,
}

pub struct TextIndexCache {
    inner: std::sync::Mutex<Inner>,
    budget_bytes: usize,
}

impl TextIndexCache {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(Inner::default()),
            budget_bytes: default_budget_bytes(),
        }
    }

    pub fn lookup(&self, key: &TextIndexKey, last_bump: Option<Instant>) -> Lookup {
        if self.budget_bytes == 0 {
            return Lookup::Miss;
        }
        let mut inner = self.inner.lock().unwrap();
        let result = match inner.entries.get_mut(key) {
            None => Lookup::Miss,
            Some(entry) => {
                entry.last_access = Instant::now();
                if entry.checked.elapsed() < FRESH_WINDOW
                    && last_bump.is_none_or(|bump| entry.started >= bump)
                {
                    Lookup::Fresh(entry.index.clone())
                } else {
                    Lookup::Stale {
                        watermark_ms: entry
                            .index
                            .max_inserted_ms
                            .saturating_sub(WATERMARK_OVERLAP_MS),
                        generation: entry.generation,
                    }
                }
            }
        };
        let label = match &result {
            Lookup::Fresh(_) => "fresh",
            Lookup::Stale { .. } => "stale",
            Lookup::Miss => "miss",
        };
        metrics::counter!("mkdb2_text_index_cache_lookups_total", "result" => label).increment(1);
        result
    }

    pub fn insert_full(
        &self,
        key: TextIndexKey,
        rows: Vec<TextIndexRow>,
        observed_max_inserted_ms: i64,
        fetch_started: Instant,
    ) -> Arc<TextStreamIndex> {
        self.store(
            key,
            TextStreamIndex::from_rows(rows, observed_max_inserted_ms),
            fetch_started,
        )
    }

    pub fn apply_increment(
        &self,
        key: &TextIndexKey,
        increment: Vec<TextIndexRow>,
        fetch_started: Instant,
        generation: u64,
    ) -> Result<Arc<TextStreamIndex>, ()> {
        let base = {
            let mut inner = self.inner.lock().unwrap();
            let Some(entry) = inner.entries.get_mut(key) else {
                return Err(());
            };
            if entry.generation != generation {
                return Err(());
            }
            if increment.is_empty() {
                entry.started = entry.started.max(fetch_started);
                entry.checked = Instant::now();
                return Ok(entry.index.clone());
            }
            entry.index.clone()
        };
        let rows = base
            .chunks
            .iter()
            .cloned()
            .map(IndexedTextChunk::into_row)
            .chain(increment)
            .collect();
        let index = TextStreamIndex::from_rows(rows, 0);
        let mut inner = self.inner.lock().unwrap();
        if inner
            .entries
            .get(key)
            .is_none_or(|entry| entry.generation != generation)
        {
            return Err(());
        }
        Ok(store_locked(
            &mut inner,
            key.clone(),
            index,
            fetch_started,
            self.budget_bytes,
        ))
    }

    /// Remove every cached text stream for the exact run identities. Run IDs
    /// are only unique within a project, so both key fields must match.
    pub fn purge_runs<'a>(&self, runs: impl IntoIterator<Item = (&'a str, &'a str)>) -> usize {
        let runs = runs.into_iter().collect::<HashSet<_>>();
        let mut inner = self.inner.lock().unwrap();
        let mut removed = 0usize;
        let mut removed_bytes = 0usize;
        inner.entries.retain(|key, entry| {
            if runs.contains(&(key.project_id.as_str(), key.run_id.as_str())) {
                removed += 1;
                removed_bytes = removed_bytes.saturating_add(entry.bytes);
                false
            } else {
                true
            }
        });
        inner.total_bytes = inner.total_bytes.saturating_sub(removed_bytes);
        removed
    }

    fn store(
        &self,
        key: TextIndexKey,
        index: TextStreamIndex,
        fetch_started: Instant,
    ) -> Arc<TextStreamIndex> {
        let mut inner = self.inner.lock().unwrap();
        store_locked(&mut inner, key, index, fetch_started, self.budget_bytes)
    }
}

fn store_locked(
    inner: &mut Inner,
    key: TextIndexKey,
    index: TextStreamIndex,
    fetch_started: Instant,
    budget_bytes: usize,
) -> Arc<TextStreamIndex> {
    let bytes = entry_bytes(&key, &index);
    let index = Arc::new(index);
    if let Some(old) = inner.entries.remove(&key) {
        inner.total_bytes = inner.total_bytes.saturating_sub(old.bytes);
    }
    if bytes > budget_bytes || budget_bytes == 0 {
        settle(inner, budget_bytes);
        metrics::counter!("mkdb2_text_index_cache_oversized_total").increment(1);
        return index;
    }
    let generation = inner.next_generation;
    inner.next_generation = inner.next_generation.wrapping_add(1);
    inner.total_bytes = inner.total_bytes.saturating_add(bytes);
    inner.entries.insert(
        key,
        Entry {
            index: index.clone(),
            started: fetch_started,
            checked: Instant::now(),
            generation,
            last_access: Instant::now(),
            bytes,
        },
    );
    settle(inner, budget_bytes);
    index
}

fn settle(inner: &mut Inner, budget_bytes: usize) {
    while inner.total_bytes > budget_bytes {
        let Some(oldest_key) = inner
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_access)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        if let Some(old) = inner.entries.remove(&oldest_key) {
            inner.total_bytes = inner.total_bytes.saturating_sub(old.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{TextIndexCache, TextIndexKey, TextIndexRow, TextRefreshLocks, TextStreamIndex};
    use std::sync::Arc;
    use std::time::Instant;

    fn row(step: i64, metric: &str, tag: &str, text: &str, inserted_ms: i64) -> TextIndexRow {
        TextIndexRow {
            step,
            metric_name: metric.to_string(),
            tag: tag.to_string(),
            is_text: 1,
            separator_count: text.matches('\n').count() as u64,
            non_empty: u8::from(!text.is_empty()),
            ends_with_newline: u8::from(text.ends_with('\n')),
            normalized_bytes: text.len() as u64,
            inserted_ms,
        }
    }

    fn key(project_id: &str, run_id: &str, metric_names: &[&str]) -> TextIndexKey {
        TextIndexKey::new(
            project_id,
            run_id,
            &metric_names
                .iter()
                .map(|name| (*name).to_string())
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn tag_is_part_of_the_stable_line_order() {
        let index = TextStreamIndex::from_rows(
            vec![
                row(7, "stdout", "b", "two\n", 1),
                row(7, "stdout", "a", "one\n", 1),
            ],
            0,
        );
        assert_eq!(index.total_lines, 2);
        assert_eq!(index.chunks[0].tag, "a");
        assert_eq!(index.chunks[0].lines_before, 0);
        assert_eq!(index.chunks[0].normalized_bytes, 4);
        assert_eq!(index.chunks[1].tag, "b");
        assert_eq!(index.chunks[1].lines_before, 1);
    }

    #[test]
    fn empty_trailing_chunk_keeps_the_unterminated_tail() {
        let index = TextStreamIndex::from_rows(
            vec![row(1, "stdout", "", "tail", 1), row(2, "stdout", "", "", 2)],
            0,
        );
        assert_eq!(index.total_lines, 1);
        assert_eq!(index.window_chunks(0, 1).len(), 2);
    }

    #[test]
    fn exact_line_boundary_skips_only_completed_leading_chunks() {
        let index = TextStreamIndex::from_rows(
            vec![
                row(1, "stdout", "", "previous\n", 1),
                row(2, "stdout", "", "", 1),
                row(3, "stdout", "", "part", 1),
                row(4, "stdout", "", "ial\n", 1),
            ],
            0,
        );

        let selected = index.window_chunks(1, 1);
        assert_eq!(
            selected.iter().map(|chunk| chunk.step).collect::<Vec<_>>(),
            vec![3, 4]
        );
        assert!(index.window_chunks(1, 0).is_empty());
    }

    #[test]
    fn incremental_replacement_recomputes_following_offsets() {
        let cache = TextIndexCache {
            inner: std::sync::Mutex::new(Default::default()),
            budget_bytes: usize::MAX,
        };
        let key = key("p", "r", &["stdout"]);
        cache.insert_full(
            key.clone(),
            vec![
                row(1, "stdout", "", "one\n", 1),
                row(2, "stdout", "", "two\n", 1),
            ],
            0,
            Instant::now(),
        );
        let generation = cache.inner.lock().unwrap().entries[&key].generation;
        let index = cache
            .apply_increment(
                &key,
                vec![row(1, "stdout", "", "one\nextra\n", 2)],
                Instant::now(),
                generation,
            )
            .unwrap();
        assert_eq!(index.total_lines, 3);
        assert_eq!(index.chunks[1].lines_before, 2);
    }

    #[test]
    fn equal_timestamp_increment_replaces_the_cached_row() {
        let cache = TextIndexCache {
            inner: std::sync::Mutex::new(Default::default()),
            budget_bytes: usize::MAX,
        };
        let key = key("p", "r", &["stdout"]);
        cache.insert_full(
            key.clone(),
            vec![row(1, "stdout", "", "old\n", 1)],
            0,
            Instant::now(),
        );
        let generation = cache.inner.lock().unwrap().entries[&key].generation;

        let index = cache
            .apply_increment(
                &key,
                vec![row(1, "stdout", "", "new\nextra\n", 1)],
                Instant::now(),
                generation,
            )
            .unwrap();

        assert_eq!(index.total_lines, 2);
        assert_eq!(index.chunks.len(), 1);
    }

    #[test]
    fn non_text_replacement_removes_a_cached_chunk() {
        let cache = TextIndexCache {
            inner: std::sync::Mutex::new(Default::default()),
            budget_bytes: usize::MAX,
        };
        let key = key("p", "r", &["stdout"]);
        cache.insert_full(
            key.clone(),
            vec![
                row(1, "stdout", "", "one\n", 1),
                row(2, "stdout", "", "two\n", 1),
            ],
            0,
            Instant::now(),
        );
        let generation = cache.inner.lock().unwrap().entries[&key].generation;
        let mut replacement = row(1, "stdout", "", "", 2);
        replacement.is_text = 0;
        let index = cache
            .apply_increment(&key, vec![replacement], Instant::now(), generation)
            .unwrap();
        assert_eq!(index.total_lines, 1);
        assert_eq!(index.chunks.len(), 1);
        assert_eq!(index.chunks[0].step, 2);
        assert_eq!(index.chunks[0].lines_before, 0);
    }

    #[test]
    fn stale_generation_cannot_replace_a_newer_index() {
        let cache = TextIndexCache {
            inner: std::sync::Mutex::new(Default::default()),
            budget_bytes: usize::MAX,
        };
        let key = key("p", "r", &["stdout"]);
        cache.insert_full(
            key.clone(),
            vec![row(1, "stdout", "", "old\n", 1)],
            0,
            Instant::now(),
        );
        let stale_generation = cache.inner.lock().unwrap().entries[&key].generation;
        cache.insert_full(
            key.clone(),
            vec![row(2, "stdout", "", "new\n", 2)],
            0,
            Instant::now(),
        );
        assert!(cache
            .apply_increment(
                &key,
                vec![row(3, "stdout", "", "stale\n", 3)],
                Instant::now(),
                stale_generation,
            )
            .is_err());
        let inner = cache.inner.lock().unwrap();
        assert_eq!(inner.entries[&key].index.chunks[0].step, 2);
    }

    #[test]
    fn oversized_key_is_not_retained_as_an_empty_index() {
        let cache = TextIndexCache {
            inner: std::sync::Mutex::new(Default::default()),
            budget_bytes: 1_024,
        };
        let key = TextIndexKey::new("p", "r", &["x".repeat(4_096)]);

        let index = cache.insert_full(key, Vec::new(), 0, Instant::now());

        assert_eq!(index.total_lines, 0);
        let inner = cache.inner.lock().unwrap();
        assert!(inner.entries.is_empty());
        assert_eq!(inner.total_bytes, 0);
    }

    #[test]
    fn purge_runs_is_exact_across_projects_and_streams() {
        let cache = TextIndexCache {
            inner: std::sync::Mutex::new(Default::default()),
            budget_bytes: usize::MAX,
        };
        let target_a = key("project-a", "same-run", &["stdout"]);
        let target_b = key("project-a", "same-run", &["stderr"]);
        let other_project = key("project-b", "same-run", &["stdout"]);
        let other_run = key("project-a", "kept-run", &["stdout"]);
        for key in [&target_a, &target_b, &other_project, &other_run] {
            cache.insert_full(
                key.clone(),
                vec![row(1, "stdout", "", "line\n", 1)],
                0,
                Instant::now(),
            );
        }

        assert_eq!(cache.purge_runs([("project-a", "same-run")]), 2);
        let inner = cache.inner.lock().unwrap();
        assert!(!inner.entries.contains_key(&target_a));
        assert!(!inner.entries.contains_key(&target_b));
        assert!(inner.entries.contains_key(&other_project));
        assert!(inner.entries.contains_key(&other_run));
        assert_eq!(
            inner.total_bytes,
            inner
                .entries
                .values()
                .map(|entry| entry.bytes)
                .sum::<usize>()
        );
    }

    #[test]
    fn empty_full_index_keeps_the_non_text_refresh_frontier() {
        let cache = TextIndexCache {
            inner: std::sync::Mutex::new(Default::default()),
            budget_bytes: usize::MAX,
        };
        let key = key("p", "r", &["numeric"]);
        let observed = 123_456;
        cache.insert_full(key.clone(), Vec::new(), observed, Instant::now());

        assert_eq!(
            cache.inner.lock().unwrap().entries[&key]
                .index
                .max_inserted_ms,
            observed
        );
    }

    #[tokio::test]
    async fn same_key_refreshes_serialize_and_preserve_distinct_increments() {
        let cache = Arc::new(TextIndexCache {
            inner: std::sync::Mutex::new(Default::default()),
            budget_bytes: usize::MAX,
        });
        let locks = Arc::new(TextRefreshLocks::default());
        let key = key("p", "r", &["stdout"]);
        cache.insert_full(
            key.clone(),
            vec![row(1, "stdout", "", "one\n", 1)],
            0,
            Instant::now(),
        );

        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let mut tasks = Vec::new();
        for (step, text) in [(2, "two\n"), (3, "three\n")] {
            let cache = cache.clone();
            let locks = locks.clone();
            let key = key.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                let lock = locks.lease_for(&key);
                barrier.wait().await;
                let _guard = lock.lock().await;
                let generation = cache.inner.lock().unwrap().entries[&key].generation;
                cache
                    .apply_increment(
                        &key,
                        vec![row(step, "stdout", "", text, step)],
                        Instant::now(),
                        generation,
                    )
                    .unwrap();
            }));
        }
        barrier.wait().await;
        for task in tasks {
            task.await.unwrap();
        }

        let inner = cache.inner.lock().unwrap();
        let steps = inner.entries[&key]
            .index
            .chunks
            .iter()
            .map(|chunk| chunk.step)
            .collect::<Vec<_>>();
        assert_eq!(steps, [1, 2, 3]);
    }
}
