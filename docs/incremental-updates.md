Chart updates must be incremental.
If a run is removed from the visible run list, no points are sent from the server. (There may be communication to prevent polling.)
If a new run is made visible, or if new points are sent, only the fresh information is sent.

We want incremental updates to always happen when possible. Avoid fuzzy fallbacks unless there is some inherent inefficiency involved, like extra state (or worse, latency) being needed to figure out if a cache is stale. We should store the optimal state for this incrementality.

Some factors that can affect incrementality: the smoothing algorithm, the last bucket (with envelopes) having values added, buckets changing with downsampling or zoom. For example, if we zoom in and the buckets don't change, then the number of new points sent should be small or zero.

We should have a fallback audit which runs occasionally, like every 67 times, which verifies that the incrementality exactly matches resending the full data. (Can the server know? Or maybe only the client can know?)

67 is a prime. It was actually not chosen because of memes, but because it's the smallest prime above 64.

## Exact smoothing continuation state

The client echoes the whole-series smoothing plan used by its held response. Verified cache lineage makes the held rows reconstructible; echoing the plan avoids another uniform-spacing scan or median sort per poll. Continuing with a changed plan can rescale the retained prefix, so an exact-plan mismatch requires a full response.

Only output-determining global state is recorded:

| Smoother | Axis/branch | Exact state | Derivation work per live output series |
|---|---|---|---|
| EMA / Triangular | step-sized | none | no uniform scan, no median sort |
| EMA / Triangular | time | median bits | one median sort |
| Savitzky–Golay | uniform | `Uniform` sentinel | one uniform scan |
| Savitzky–Golay | irregular | median bits | one uniform scan and one median sort |
| Any | no finite rendered value (including no position after range/x filtering) | `NoState` sentinel | no plan derivation and no smoothing |

Step EMA/Triangular still use each local x gap; “none” means only that they have no whole-series derived scalar which an append could use to rescale earlier output. Custom-x charts never use frontier deltas, so they compute only the plan needed to render the full response and neither stamp nor echo continuation state.

The live preparation path derives this semantic plan once and reuses it for smoothing, Savitzky–Golay reach marking, exact state stamping, and comparison on the next request. The sampled old-only audit receives the already-proven current plans paired with stable request/tag identities. It reuses them positionally only after proving the complete old-only identity sequence matches; client-controlled count or membership mismatches answer in full before a plan can be consumed. The reconstruction therefore performs no local uniform scan or median sort. `NoState` remains an explicit sequence entry when all-marker and margin-only series interleave.

The opaque frontier map uses a version and response-series count plus one word per output series: `0 = NoState`, `1 = Uniform`, otherwise positive-finite median `f64` bits. Clients send the literal held-series count; dashboards before wire revision 2 send `UINT32_MAX` when this exact state is required, and the server recovers the real count from the state.

A server rejects unsupported lineage state and seeds one full response. Frontend epoch changes suppress stale echoes after reconnect; the keyed stamp also invalidates pre-restart state.

Test-only derivation counters enforce the table above through the production response builder. They count semantic uniform scans and median sorts rather than elapsed time, including audited deltas and mixed active/margin-only charts.

## Cached held-input proof

Each immutable cached snapshot carries an append-lineage identifier, its origin (`Miss`, `Rewrite`, or `Late`), the actual maximum numeric insertion stamp, and a keyed content sum. The SQL fetch watermark remains separately visibility-capped. The incremental merge preserves lineage only when retained rows remain unchanged and genuinely added numeric rows have strictly newer stamps. Identical-content overlaps keep their original cached insertion stamp. Equal/older-stamped additions rotate to `Late`; a refused increment followed by an authoritative full fetch starts `Rewrite`.

The response authenticates lineage, cutoff, request/ref identity and order. Verification uses constant-size work per requested ref, with no per-poll row hashing. Filtering a verified lineage at an earlier authenticated maximum recovers exactly the held rows. A cutoff above the current maximum rejects an older in-flight snapshot. The content proof and response hashes retain their 64-bit collision caveat.

LRU eviction retains up to 4,096 process-local recovery records containing lineage, origin, maximum, row count and content sum. A full reload hashes each row once; the rows up to the recorded maximum must have the same count and sum, and held rows must remain a step prefix within each tag. Only then can the reload re-adopt the old lineage. The prefix condition rejects a newly stamped lower-step backfill even if all held rows remain present. Records are single-use, and purge removes them. This limits entry count, not bytes: identifier strings also consume memory, separately from the raw-row cache budget. Oversized/pass-through results do not publish recovery records.

A matching reload after eviction preserves deltas, including across repeated evictions. A missing, discarded or mismatched recovery record seeds a full answer for previously held inputs; newly requested inputs can still ship complete alongside continuing series in a delta. Recovery does not survive a server restart: both recovery records and process hash keys are lost, so open charts receive a new full response. The proof remains series-wide; an out-of-range rewrite can conservatively require full. No per-row hash array is retained: the sum is accumulated during full fetches and only genuinely added rows are hashed during the existing incremental merge.

Sustained deltas across independent refreshes require a retained cache entry. With `KYMO_SERIES_CACHE=0`, or for a series larger than the whole budget, each subsequent independent full fetch creates a new lineage and normally answers echoed polls in full (`digest_miss`). Concurrent callers can share one pass-through snapshot, and the first refresh crossing the budget or using a matching eviction record can still preserve lineage.

The SQL visibility margin and overlap remain necessary. A row absent from every incremental read cannot be discovered by this proof; this is the pre-existing source-completeness limit, distinct from proving a splice matches the exact current cached input.

`mkdb2_chart_delta_total` records these continuation outcomes:

| `kind` | Meaning |
| --- | --- |
| `delta` | Reuses a prefix and sends replacement columns or complete newly requested series. |
| `unchanged` | Reuses every axis/value column and every series continues; metadata may still change. |
| `miss` | The planner found no reusable prefix (`from_col == 0`). |
| `gated` | Another eligibility, membership, relative-offset, smoothing, or audit-plan alignment gate refused continuation. |
| `audit_failed` | The sampled reconstruction disagreed with the proposed held prefix. |
| `state` | Echoed lineage state is malformed, unsupported, or structurally incompatible. |
| `order` | Shared refs no longer preserve their echoed order. |
| `digest_miss` | Stamp/cutoff verification failed against a lineage born from a cache-miss load. |
| `digest_rewrite` | Stamp/cutoff verification failed against a lineage born from a full load after a refused increment. |
| `digest_late` | Stamp/cutoff verification failed against a lineage born from an equal/older-stamped addition. |

Successful recovery preserves the lineage's origin. The digest labels count chart refusals, not exact eviction or mutation events; corrupt echoes and older in-flight snapshots can also fail verification. Initial full loads, some empty responses and failed requests are not counted.

For the first days after deployment, compare `digest_late` and `digest_miss` rates against `delta`. A `digest_late` share above about 1% of counted cache-echo polls means the rotation rule needs a look; compute that share as its rate divided by the sum of rates across all `kind` values, including `unchanged`, over the same interval.

Run the ignored response-builder benchmark locally with `cargo test --release -p kymo-server --lib response_builder_benchmark -- --ignored --nocapture`. It reports 21-sample medians with audit disabled for first paint, unchanged and append polls; actual cache setup occurs outside timing with a fixed fixture budget. It has no CI wiring.
