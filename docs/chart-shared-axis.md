Chart logic written by Kevin (no AI):


After downsampling (or not downsampling), every run's points share exactly the same x buckets. So they all either downsample or don't downsample, and they do so the same way.
Each downsample bucket also emits the smoothed value in the center of the bucket, where the x-position displays at the average of the first and last endpoints (so a bucket with steps 1 and 2 emits at 1.5). If the x-axis or y-axis has multiple values, the range will be displayed.

Buckets are always disjoint, having no overlap. Half-open intervals help here.
Smoothed values will always calculated at the center point of the bucket, even if the run's x-values do not reach there.

Be careful of how chart incremental updating works with this logic, since values can be inserted in existing buckets (typically at the endpoints of a run's existing points).

When hovering, highlight the run's values.

Points are never dropped - each point is represented in the output chart. There is one exception: a point can be overridden by a non-finite value, which is more important.

Downsampling should kick in only when a bucket expects 4 or more points, because a bucket with 2 points (rendering an envelope) is worse than 2 buckets with 1 point.

---

AI section

## Current implementation

- The shared axis mixes raw slots for exact x values with envelope slots for downsampling buckets. Step-axis cells become envelopes at width >= 4 (data-independent; missing points are fine); other axes require four observed distinct x values. Every point, including the global minimum and maximum x values, follows its cell's ordinary raw-or-envelope rule. See `shared_chart`/`plan_axis` in chart.rs.
- Linear grids use anchored power-of-two widths. Log grids use power-of-two subdivisions within each octave; step and timestamp charts use `log(x + 1)` when x = 0 is present (the frontend mirrors the same zero-present rule off the model — uplot_chart `log_shift`). Values outside the log domain use the exceptional-marker channel.
- In unsmoothed envelope slots, `values` is the bucket mean and the min/max envelope is the rendered raw evidence. A run with spread (`min < max`) anywhere in its loaded series gets faint 1px envelope borders. If it has no spread, its coincident edges become one full-strength 1.5px line in the raw block. That line uses the bounds because a bucket containing a non-finite sample can retain finite raw evidence while its `values` entry is NaN. This is per series, not per segment, so a partly collapsed envelope keeps consistent border styling. With smoothing, `values` is evaluated at the slot x and drawn full-strength over both faint raw edges, even when they coincide. `raw_values` is limited to smoothed passthrough charts.
- Raw slots carry plottable points exactly; envelope slots retain their min/max evidence; non-finite samples use the marker channel. Known gap: an exceptional-x run with no plottable anchor currently has no marker, and an entirely unplottable chart returns empty. The required left-edge marker and counted empty-panel notice are tracked in AI-1491.
- The wire format stores occupied column ranges and finite f32 values; the client inflates the dense shared-axis model (`chart_sync::inflate_response`). `banded` carries the chart-wide envelope contract, and chart-level `xr_min`/`xr_max` carry bucket x extents.
- Main-page Settings stores two browser-local hover options, both off by default: highlight runs with the same exact name, and show each run's nearest available point in the loaded data when its hovered column is empty. Nearest compares x values, including endpoints and logged non-finite samples, and picks the earlier x on ties. Borrowed rows show their own x or bucket range. Only samples at the hovered column choose the highlighted run; borrowed rows are readouts. Chart hover dims the other runs and lifts that run only in the hovered chart, so a run change repaints one canvas; synced charts still mark its tooltip row. Sidebar hover dims and lifts in every chart.
- Deltas require a cache-lineage stamp proving the exact rows behind the held response; legacy watermark keys are absent so older servers also decline deltas. See [incremental updates](incremental-updates.md).
- `plan_delta_from_col` composes interpolation, numeric, and marker dependencies after lineage, membership, and exact smoothing-plan checks. Causal smoothers mark their affected suffix before output filtering. Uniform Savitzky–Golay also marks prefix-sum block dependencies, since a shift or a resized final block can change wire-rounded output outside the mathematical window. Only the earliest changing interpolation endpoint can move the splice start. Stable smoother-only holes remain reusable; endpoint changes include the preceding finite cell and leading holes that extrapolate from a changing first segment.
- Absolute log-x markers use the proven numeric prefix to bound nearest-slot placement, including cross-series envelope-center and raw/envelope mode changes. Verified negative sentinels retain incremental reuse. Sampled server audits reconstruct the verified held row set; client hashes verify every splice. Y is rounded to wire f32 before hashing so the comparison remains bit-exact.

See also [log scale buckets](log-scale-buckets.md), [incremental updates](incremental-updates.md), and [chart zoom gesture architecture](chart-zoom-gesture-architecture.md).
