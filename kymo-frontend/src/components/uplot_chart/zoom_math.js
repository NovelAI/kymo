// Pure zoom-selection math shared by the browser integration and its Node
// regression tests. This file intentionally has no DOM or uPlot dependency;
// the gesture owner snapshots its source chart, then passes ordinary numbers
// and array-like columns here.

const AXIS_LINEAR = "linear";
const AXIS_LOG = "log";
const AXIS_LOG1P = "log1p";

function finite(value) {
  return Number.isFinite(value);
}

function slotBounds(snapshot, index) {
  const plotted = snapshot.plotXs?.[index];
  const center = plotted - snapshot.xShift;
  if (!finite(plotted) || !finite(center)) return null;
  const candidateLo = snapshot.xrMin?.[index];
  const candidateHi = snapshot.xrMax?.[index];
  return finite(candidateLo) &&
    finite(candidateHi) &&
    candidateLo <= center &&
    center <= candidateHi
    ? [candidateLo, candidateHi]
    : [center, center];
}

/** Retain the immutable gesture-start columns and derive their real-x coverage. */
function buildFrozenSupports({ plotXs, xrMin, xrMax, xShift = 0 }) {
  const snapshot = { plotXs, xrMin, xrMax, xShift, coverage: null };
  if (!plotXs || !finite(xShift)) return snapshot;
  let lo = Infinity;
  let hi = -Infinity;
  for (let index = 0; index < plotXs.length; index += 1) {
    const bounds = slotBounds(snapshot, index);
    if (!bounds) continue;
    lo = Math.min(lo, bounds[0]);
    hi = Math.max(hi, bounds[1]);
  }
  if (lo <= hi) snapshot.coverage = { lo, hi };
  return snapshot;
}

function transform(axis, value) {
  if (!finite(value)) return null;
  if (axis === AXIS_LINEAR) return value;
  if (axis === AXIS_LOG) return value > 0 ? Math.log(value) : null;
  if (axis === AXIS_LOG1P) return value > -1 ? Math.log1p(value) : null;
  return null;
}

function inverse(axis, value) {
  if (!finite(value)) return null;
  let result;
  if (axis === AXIS_LINEAR) result = value;
  else if (axis === AXIS_LOG) result = Math.exp(value);
  else if (axis === AXIS_LOG1P) result = Math.expm1(value);
  else return null;

  if (!finite(result)) return null;
  if (axis === AXIS_LOG && !(result > 0)) return null;
  if (axis === AXIS_LOG1P && !(result > -1)) return null;
  return result;
}

/**
 * Freeze source-chart geometry and axis transforms for one gesture.
 *
 * Inside the plot, pixels map through the visual scale the user pressed on.
 * Outside, extrapolation begins at the true rendered coverage and advances by
 * one transformed coverage span per plot width. `plotLeft` is in page
 * coordinates so callers can feed MouseEvent.pageX even after scrolling.
 */
function buildFrozenSourceMap({
  axis,
  plotLeft,
  width,
  visualLo,
  visualHi,
  coverageLo,
  coverageHi,
}) {
  if (!finite(plotLeft) || !finite(width) || !(width > 0)) return null;

  const visualTLo = transform(axis, visualLo);
  const visualTHi = transform(axis, visualHi);
  const coverageTLo = transform(axis, coverageLo);
  const coverageTHi = transform(axis, coverageHi);
  if (
    visualTLo === null ||
    visualTHi === null ||
    coverageTLo === null ||
    coverageTHi === null ||
    !(visualTHi > visualTLo) ||
    !(coverageTHi > coverageTLo)
  ) {
    return null;
  }

  return {
    axis,
    plotLeft,
    width,
    visualTLo,
    visualSpan: visualTHi - visualTLo,
    coverageTLo,
    coverageTHi,
    coverageSpan: coverageTHi - coverageTLo,
  };
}

/** Map a page-x coordinate using only the frozen source chart. */
function mapSourcePageX(sourceMap, pageX) {
  if (!sourceMap || !finite(pageX)) return null;
  const pixel = pageX - sourceMap.plotLeft;
  const fraction = pixel / sourceMap.width;
  let transformed;

  if (pixel <= 0) {
    transformed = sourceMap.coverageTLo + fraction * sourceMap.coverageSpan;
  } else if (pixel >= sourceMap.width) {
    transformed =
      sourceMap.coverageTHi + (fraction - 1) * sourceMap.coverageSpan;
  } else {
    transformed = sourceMap.visualTLo + fraction * sourceMap.visualSpan;
  }

  return inverse(sourceMap.axis, transformed);
}

/**
 * Expand a nominal interval to include every source bucket it intersects.
 *
 * Intersection is always tested against the original interval, never the
 * growing result, so expansion cannot cascade through adjacent buckets. The
 * nominal endpoints seed the result, preserving extrapolated range outside
 * the source coverage. A gap-only selection returns null.
 */
function expandWholeBuckets(frozenSupports, endpointA, endpointB) {
  if (
    !frozenSupports ||
    !frozenSupports.plotXs ||
    !finite(endpointA) ||
    !finite(endpointB)
  ) {
    return null;
  }

  const selectedLo = Math.min(endpointA, endpointB);
  const selectedHi = Math.max(endpointA, endpointB);
  let lo = selectedLo;
  let hi = selectedHi;
  let intersected = false;

  for (let index = 0; index < frozenSupports.plotXs.length; index += 1) {
    const bounds = slotBounds(frozenSupports, index);
    if (!bounds) continue;
    if (bounds[1] >= selectedLo && bounds[0] <= selectedHi) {
      lo = Math.min(lo, bounds[0]);
      hi = Math.max(hi, bounds[1]);
      intersected = true;
    }
  }

  return intersected ? { lo, hi } : null;
}

const KYMO_ZOOM_MATH = {
  AXIS_LINEAR,
  AXIS_LOG,
  AXIS_LOG1P,
  buildFrozenSupports,
  buildFrozenSourceMap,
  mapSourcePageX,
  expandWholeBuckets,
};

// Node tests load this as CommonJS. Browser integration can prepend the file
// to create.js and call the declarations directly without a module runtime.
if (typeof module !== "undefined" && module.exports) {
  module.exports = KYMO_ZOOM_MATH;
}
