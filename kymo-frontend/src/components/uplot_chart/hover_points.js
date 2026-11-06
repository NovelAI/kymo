(() => {
  if (window.__kymo_hp) return;
  // uPlot reads only null as a gap: a NaN passes on a linear scale only because the canvas ignores non-finite coords, and on a log scale clamps to the floor, with lines plunging to it (a false fill under log-y lines).
  // create.js and set_data_js pass every column but x (data[0], gapless, kept typed) through nullGaps, so every reader of u.data meets a gap only as null.
  // An indexed loop, not Array.from(c,fn): the callback form costs ~7x more on typed arrays. One long-lived column function: an arrow made per eval ran up to a third slower whenever it came in cold (a first flush, a rebuild, after a GC).
  function nullColumn(c, i) {
    if (i === 0) return c;
    const n = c.length, o = new Array(n);
    for (let j = 0; j < n; j++) { const v = c[j]; o[j] = v !== v ? null : v; }
    return o;
  }
  function nullGaps(data) {
    return data.map(nullColumn);
  }

  // A logged NaN/Infinity is still a point: only its marker column is populated.
  function hasHoverPoint(values, markers, index) {
    return values[index] != null || (markers != null && markers[index] != null);
  }

  // Lazily index each series once per immutable u.data array; cached fallbacks binary-search its samples.
  function createHoverPointLookup(data, lineBase, nanBase, nanCols) {
    const indexed = [];
    return function pointIndex(series, hoveredIndex, showNearest) {
      const values = data[lineBase + series];
      const markers = nanCols > 0 ? data[nanBase + series] : null;
      if (hasHoverPoint(values, markers, hoveredIndex)) return hoveredIndex;
      if (!showNearest) return -1;

      let points = indexed[series];
      if (points == null) {
        points = [];
        for (let index = 0; index < values.length; index++) {
          if (hasHoverPoint(values, markers, index)) points.push(index);
        }
        indexed[series] = points;
      }
      if (points.length === 0) return -1;

      let lo = 0, hi = points.length;
      while (lo < hi) {
        const mid = Math.floor((lo + hi) / 2);
        if (points[mid] < hoveredIndex) lo = mid + 1;
        else hi = mid;
      }
      if (lo === 0) return points[0];
      if (lo === points.length) return points[lo - 1];
      const xs = data[0], left = points[lo - 1], right = points[lo];
      // Compare real x distances, not the count of other runs' intervening slots.
      // A tie keeps the earlier sample; additive log rendering shifts cancel.
      return xs[hoveredIndex] - xs[left] <= xs[right] - xs[hoveredIndex]
        ? left : right;
    };
  }

  // The one pointer mapping for hover, copy, the zoom-select press, and the pointer style: offsets from the plot overlay's live rect (uPlot's cached one misses layout shifts without a scroll or resize), null outside the plot band, and x within 1px of a side or beyond it snapped to exactly 0 or the plot width, where posToVal is the scale bound. The overlay's layout width is uPlot's plot width; bbox is rounded to half device pixels.
  function plotPosition(chart, clientX, clientY) {
    const rect = chart.over.getBoundingClientRect();
    const left = clientX - rect.left, top = clientY - rect.top;
    if (!(rect.width > 0 && rect.height > 0) || top < 0 || top > rect.height) return null;
    return { left: left <= 1 ? 0 : left >= rect.width - 1 ? rect.width : left, top };
  }

  window.__kymo_hp = { nullGaps, createHoverPointLookup, plotPosition };
})();
