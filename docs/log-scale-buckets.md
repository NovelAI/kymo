We want to support log-scale downsampling.
Each bucket length will be a power of 2, like [x, x+2^n). Bucket left boundaries should try to align on powers of 2: like [16, 20), [20, 24), [24, 28), [28, 32), [32, 40).

That means charts will be a mixture of raw points and downsampled buckets. For step-count charts, a bucket should only downsample if it expects to have 4 points (if there are points missing, that's fine). This is because a bucket with only 2 points should just be two non-downsampled points instead of rendering an envelope. (This should also apply to linear (non-log) charts - 2 buckets is better than 1 envelope bucket.)

The frontend should handle these mixed charts efficiently, without wasted work. The rendered point will still be at the arithmetic average of the first and last point in a bucket.

In addition, the wire format should be cleaned up. Transmit data efficiently, not having NaNs everywhere to represent missing data. Consider sparse points (labeled with their x, such as for time series), or ranges of dense integers, or other data formats as possible.

No need to maintain compatibility with old clients.

When step 0 is present, the log scale should become log(x+1). When negative x is present, it should be logged as an exceptional point, like NaNs.

---

Claude says:

Implemented 2026-07-10 (chart.rs `GridKind::Log` / `plan_axis`, kymo.proto `log_buckets`): octaves split into 2^m power-of-two buckets aligned on powers of two; a bucket downsamples only when it expects >= 4 points (step axes: width; non-step: distinct count), everything below ships raw — including linear grids, whose sub-4 widths now pass through. Step/timestamp ladders run in log(x+1) exactly when the chart contains x = 0 (the client mirrors the rule off the same model; downsampled deltas pin the resulting grid kind, while passthrough keeps zero as its exact first slot, so no wire field is needed); custom-x keeps log(x); negative/nonpositive-unplottable x becomes a kind-4 exceptional marker. The wire ships occupancy segments + f32 values with no filler NaNs, envelopes only where they differ from the value, and one chart-level x-extent pair (see chart-shared-axis.md).
