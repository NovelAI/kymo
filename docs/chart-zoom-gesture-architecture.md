# Chart zoom gesture architecture

## Current contract

The chart where a gesture (a selection or an axis pull) begins owns it, through one page-level slot, until commit or cancellation. Pointer movement may cross gutters or other charts, but every horizontal coordinate is interpreted through the source chart. Synced peers only mirror the source range; they never become gesture owners or contribute coordinates.

A selection reads the source's plot as it is now, so it can take in points a live reply adds mid-drag. It reads the pointer as hover does, so within 1 px of an edge is that edge. The press keeps the x it had or, made at or past an edge, its distance past that edge; the moving end is the x under the pointer. Past an edge, a step-axis chart extrapolates from its current bucket coverage at one coverage span per plot width, and a client-only time or custom-x chart takes that edge's current x bound.

Step-axis selections resolve against the complete x extents of the buckets rendered when they resolve. Every bucket intersecting the selected interval is included once, a selection wholly inside a data gap is a no-op, and extrapolated space outside the coverage is retained.

Live replies only add data; one that removed some mid-drag could leave the window outside what remains, and the commit would keep it.

A selection whose end reaches the right edge of a chart that showed its newest data makes a following zoom: the window keeps its start, and its end stays on the newest data as replies arrive. A selection that ends inside the plot, or at the edge of a window short of the newest data, makes a fixed zoom. A rebuild carries a following zoom onto the data it brings, and a reset clears it.

A client-only chart shows its newest data when its window ends at or past its newest x, and each synced chart follows its own newest x (one whose data ends before the start, or has none, keeps the window it was given).

A step-axis chart shows its newest step when the answer it shows had no step maximum (unzoomed or following) and its window reaches that answer's newest step, which it doesn't after a right-half pull brought its right edge inside the data, until the answer to that pull's refetch. Its following zoom asks the server for an open-ended step window.

A left-half pull on a chart that shows its newest data at its window's right edge follows it, while held and after: its pinned right edge is the newest x, and a reply mid-pull moves that edge with the grabbed value kept under the hand (between the pixel clamps). Any other pull fixes its window, which a reply mid-pull leaves alone: a right-half one, or a left-half one on a window short of the newest data or reaching past it (a lone x value, which uPlot widens, or a zoom out past the data, whose far edge would otherwise jump to the newest x at the first move).

A fixed step pull widens a pinned edge at or past either end of the data to that end bucket's coverage, never narrowing it, and sends one inside the data as shown.

A cancel, or a release back within a pixel of the press that left something changed (a reply mid-hold, or a window its moves shifted), restores what the press found, on the data held at the exit: a fixed window as it was, a following one from its start, an unzoomed chart over all its data (a step chart's window an earlier pull left, its refetch pending, as it was). A plain click on the strip commits no zoom (with single-click unzoom on, its click still resets one).

A pull holds the y gutter at its pressed width, so the plot's left edge stays where the pull's frame (the plot's page x at the press) has it when the y labels change width; labels wider than it are cut at the canvas edge until the pull ends (Kevin accepted that, sign flips included). Every exit re-fits the gutter: a cancel always, a release only when the width changed.

A real container resize invalidates the geometry a gesture took at mousedown and cancels the matching owner. A move without a resize, as when a new metric's chart lands before the source in its section, goes undetected: a gesture (a selection or an axis pull) keeps the page position it took at mousedown, so its window doesn't jump. uPlot's internal axis resizing is not a container resize: it only repaints the active selection after uPlot updates its plot rectangle. Structural source replacement and unmount also cancel; peer replacement only repaints.

Every cancel latches the click its release may make, so that click can't reset what the cancel kept. A gesture ends where its last move left it, its click latch included: a browser delivers any pending move before the mouseup, and the dashboard's document never scrolls under a held button (its shell scrolls the main content, which moves no pointer page position). Known limit, shared with stock uPlot: with double-click unzoom (single-click unzoom off), a press the OS counts as the second of a double click resets the zoom at its release, even when it dragged.

## Deferred improvements

These changes would reduce overhead or centralize lifecycle policy, but are not required for correctness.

### Install zoom math once per page

`zoom_math.js` is currently embedded in every generated chart-create script. The window guard executes it once, but the browser still parses the embedded source for every eval. Install the helper beside the page-level uPlot and bridge scripts instead, then make chart creation require that installed version. The refactor needs an explicit load-order and versioning contract so a chart cannot run before the helper is available or retain an incompatible helper across a hot update.

### Consolidate gesture installation

Each mounted chart currently installs small selection and axis closures while sharing one page-global owner. A page-level controller could register chart elements and metadata, delegate starts, and own the window-duration listeners. This would reduce per-chart closures and put source replacement, unmount, resize, and cancellation policy in one module. It is worthwhile only if profiling shows listener or closure cost beyond the existing viewport-bounded chart population; native uPlot drag still cannot satisfy source-owned extrapolation over peers.

### Coalesce peer selection painting

An active owner's `paint` scans its sync group and updates every peer. Because uPlot scale and size hooks can each request a repaint, a live flush across many synced charts can repeat that scan several times in one frame. A dirty flag plus one `requestAnimationFrame` flush would bound painting to one group pass per frame. Commit and cancellation must synchronously clear any scheduled paint, and the final pointer coordinate must be resolved without losing the last event.

## Qualification follow-up

The standalone zoom fence can run unchanged in Chromium, Firefox, and WebKit, but CI currently gates Chromium only, matching the existing browser-gesture job. Adding the other engines is a coverage/cost decision rather than a gesture-design dependency. If enabled, keep viewport and fixture constants in the standalone test module so workflow YAML remains orchestration rather than behavioral specification.
