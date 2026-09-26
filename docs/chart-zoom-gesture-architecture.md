# Chart zoom gesture architecture

## Current contract

The chart where a selection begins owns the gesture until commit or cancellation. Pointer movement may cross gutters or other charts, but every horizontal coordinate is interpreted through the source chart's geometry, x transform, visual range, and bucket evidence as they existed at mousedown. Synced peers only mirror the source range; they never become gesture owners or contribute coordinates.

Step-axis selections resolve against the complete x extents of the rendered buckets. Every bucket intersecting the original selected interval is included once, a selection wholly inside a data gap is a no-op, and extrapolated space outside the current coverage is retained. Client-only time and custom-x charts instead use their exact plotted domain. Axis-bound pulls share the page-level ownership slot but keep their separate pull geometry.

A real container resize invalidates frozen source geometry and cancels the matching owner. uPlot's internal axis-size convergence is not a container resize: it only repaints the active selection after uPlot updates its plot rectangle. Structural source replacement and unmount also cancel; peer replacement only repaints.

## Deferred improvements

These changes would reduce overhead or centralize lifecycle policy, but are not required for correctness and should not be mixed into the boundary-selection fix.

### Install zoom math once per page

`zoom_math.js` is currently embedded in every generated chart-create script. The window guard executes it once, but the browser still parses the embedded source for every eval. Install the helper beside the page-level uPlot and bridge scripts instead, then make chart creation require that installed version. The refactor needs an explicit load-order and versioning contract so a chart cannot run before the helper is available or retain an incompatible helper across a hot update.

### Consolidate gesture installation

Each mounted chart currently installs small selection and axis closures while sharing one page-global owner. A page-level controller could register chart elements and metadata, delegate starts, and own the window-duration listeners. This would reduce per-chart closures and put source replacement, unmount, resize, and cancellation policy in one module. It is worthwhile only if profiling shows listener or closure cost beyond the existing viewport-bounded chart population; native uPlot drag still cannot satisfy source-owned extrapolation over peers.

### Coalesce peer selection painting

An active owner's `paint` scans its sync group and updates every peer. Because uPlot scale and size hooks can each request a repaint, a live flush across many synced charts can repeat that scan several times in one frame. A dirty flag plus one `requestAnimationFrame` flush would bound painting to one group pass per frame. Commit and cancellation must synchronously clear any scheduled paint, and the final pointer coordinate must be resolved without losing the last event.

## Qualification follow-up

The standalone zoom fence can run unchanged in Chromium, Firefox, and WebKit, but CI currently gates Chromium only, matching the existing browser-gesture job. Adding the other engines is a coverage/cost decision rather than a gesture-design dependency. If enabled, keep viewport and fixture constants in the standalone test module so workflow YAML remains orchestration rather than behavioral specification.
