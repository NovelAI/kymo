# Text stream virtualization

A text panel shows one tab per run, in the panel's run order, above a single
full-width log of the active run; only that log fetches rows. The active tab is
remembered per panel for the session, like scroll positions, and falls back to
the first tab while its run is absent from the panel. Until a tab is clicked,
the first run shown counts as chosen, so a newly started run does not take
over. Past 40% of the panel's height the tab strip scrolls, so the log always
keeps room to fetch.

The log loads automatically and renders a fixed-height window around
the visible lines. `QueryTextWindow` returns at most 2,000 logical lines, and
both fetched source chunks and reconstructed output are bounded to 8 MiB.

For unfiltered windows, the server keeps a bounded per-chunk line index. The
index maps an absolute line offset to the ClickHouse row-key range containing
that window, so ordinary scrolling reads text only from intersecting chunks.
The cache defaults to 64 MiB and is configured with
`KYMO_TEXT_INDEX_CACHE_MB`. Its accounting includes request-controlled cache
keys as well as chunk metadata.

Index refreshes deliberately retain the machinery needed for ClickHouse row
replacement semantics:

- `(step, metric_name, tag)` is the total display and replacement order.
- Incremental reads overlap the visibility watermark and include non-text
  replacements as tombstones.
- Per-stream singleflight prevents concurrent viewers from cascading into full
  rebuilds, while generation checks prevent a stale merge from replacing a
  newer or evicted entry.
- Physical run deletion purges both numeric-series and text-index entries.

Search is point-in-time. The committed query belongs to the panel and applies
to whichever tab is active. Submitting, switching tabs, or scrolling into
another result band scans the active text stream, but pushed live-data versions
do not continuously rescan an active search. Re-submit to refresh its result set.

## Scroll restoration

- Persist a logical row and fraction, or the end of a followed log, per panel,
  project, run, ordered metrics, and committed search once at unmount, including
  a tab switch; grid and maximized views keep independent positions and tabs.
  Cache recency follows mounts and unmounts, rather than scroll events.
- If the saved anchor is beyond the stream or its band is rejected, discard
  it and start at line 0. Empty streams also reset to zero. Terminal run errors
  clear the rows and keep the anchor for a later push.
- Font changes through Projects settings preserve the logical row, and horizontal
  scrollbars do not trigger a refetch. Same-search refreshes keep the mounted log.
- Do not retain fetched text, horizontal position, or positions across reloads;
  entry and byte limits can evict old positions.
- A remembered position is a row number, so if a stream's rows are replaced
  under the same key, it restores to that row of the new content.

## Following a live log

- A live run, whose `RunInfo` status is `Running` or `Stuck` (alive but silent,
  e.g. compiling or hung), opens at its last line unless it has a remembered
  position. A remembered end resumes at the new bottom, even after the run
  stops; a remembered row restores as above.
- An end anchor's first response only measures the stream. Any response whose
  length moves the anchor into another band is not rendered; that band's request
  replaces it, so the head of the log never flashes.
- While following, each install keeps the distance from the last line. The
  planned row ignores the horizontal scrollbar, which only bands with a wide line
  show; placement still targets the measured bottom.
- A search is its own log, so on a live run it opens at the last match and
  re-pins only when a re-submit refreshes it.
- A scroll within half a row of the bottom of a live log starts following;
  any position above it stops.
- A rejected tail band stops following and resets to line 0, as above.

## Regression and hand test

Run the browser regression against a clean `dx serve` or a static server hosting a frontend bundle. A hosted build needs a server origin, but the fence intercepts `/grpc-ws`, so any `http://` origin works (`KYMO_FRONTEND_SERVER_ORIGIN=http://127.0.0.1:1 dx serve`):

```sh
python tools/local-transport-qualification/text_scroll_fences.py http://127.0.0.1:8080/ --browser chromium
```

`--browser` also accepts `firefox` and `webkit`. This is a local check, with
visible scrollbars; it is not wired into CI. Before deploying, Kevin must hand-test
on `dx serve`: in Chrome and Firefox, use a mouse wheel and PageDown/arrow keys
to scroll across several band boundaries on both a finished run and a live run
receiving pushes, watching for hitches. Switch tabs with the pointer and with
Tab plus Enter/Space, drag the scrollbar in two tabs to different lines and switch
between them, move the panel far off-screen and back (including a quick
interrupted return), submit/resubmit/clear search using pointer and keyboard,
maximize/close/reopen, resize near the tail, and change the font through Projects.
On a live run, check that it opens at the bottom and keeps up with new lines,
that a small wheel or arrow scroll up stops following, and that returning to the
bottom resumes it. With many runs in a narrow panel, scroll the tab strip.

## Deferred work

### Keep a stale window navigable after a size rejection

`ResourceExhausted` currently discards the saved anchor and returns to line 0.
Preserving the last successful `WindowData` with an error overlay could instead
let users navigate away from an oversized line without losing their place.
Stale rows alone do not cover a remount, where no prior window is retained.

### Very large indexes and browser scroll ranges

An index larger than the cache budget is served to the current request but not
retained. A highly fragmented stream can therefore rebuild its metadata on
later scroll requests. Fixing this fully requires a storage/index design that
handles out-of-order backfill, row replacement, and text-to-non-text rewrites;
persisting naive cumulative offsets during ingest is not sufficient.

Index canonicalization uses a stable sort so an appended incremental row wins
when `(step, metric_name, tag, inserted_at)` ties a cached row. Stable sorting
uses temporary scratch proportional to the row count; a near-budget index can
therefore add tens of MiB to peak memory while it is rebuilt, and singleflight
does not serialize different stream keys. If concurrent cold index builds show
up as material memory pressure, bound their global concurrency rather than
reintroducing a second row-merge implementation.

The fixed-height top and bottom spacers also eventually reach browser element
height limits at roughly two million lines. Segmented spacers or a rebased
virtual scroll origin can lift that ceiling if real streams approach it.
