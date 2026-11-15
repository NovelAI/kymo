// Contextual help for a numeric chart (docs/contextual-help.md): the texts only chart script state decides, for util/help.js to show on the chart's container. It runs when help scans the page and at no other time, reads what create.js already keeps on the uPlot instance, what uplot_chart.rs renders on the container (data-live, data-smoothed, data-step-zoomed, data-xnan) and two preferences on the root element, and writes only the X axis's text and the readout's label properties, so it adds nothing to hover, repaint or rebuild.
(() => {
  const TEXT = {
    axis: 'Drag axis to rescale',
    copyMac: 'Hover, then ⌘C copies values',
    copy: 'Hover, then Ctrl+C copies values',
    reset: 'Double-click to reset zoom',
    resetClick: 'Click to reset zoom',
    synced: 'Matching X axes zoom together',
    follow: 'Include right edge to follow new data',
    trace: 'Faint line: unsmoothed values',
    bands: 'Shaded band: range of values',
    colored: 'Smoothed',
    raw: 'Raw value',
    range: 'Range',
    markers: 'Hollow circles: non-finite, unplottable or log-clipped values',
    xnan: '×N: samples with unplottable X',
    borrowed: '@: nearest sample’s X or bucket range',
  };
  const root = document.documentElement;
  window.__kymo_help.providers.chart = el => {
    const charts = window.__kymo_charts;
    const u = charts?.[el.id];
    if (!u) return [];
    // The X-axis strip's own note sits on uPlot's element for that axis (its first), which takes the text here, at the first scan after the chart's creation, so that creating a chart does no work for help; the attribute's arrival brings the next scan (util/help.js).
    const axis = u.root.querySelector('.u-axis');
    if (axis && !axis.dataset.help) axis.dataset.help = TEXT.axis;
    const out = [navigator.platform.startsWith('Mac') ? TEXT.copyMac : TEXT.copy];
    // A step chart zooms through the step store and keeps no zoom on its instance (create.js); uplot_chart.rs says whether the answer shown is a zoomed one.
    const zoomed = u.__kymo_userzoom || el.dataset.stepZoomed;
    if (zoomed) {
      out.push(root.getAttribute('data-kymo-single-click-unzoom') === 'true' ? TEXT.resetClick : TEXT.reset);
      // The other mounted charts on this one's x axis, which the zoom moved too (create.js eachPeer, which also passes over one still being created).
      const key = u.cursor.sync.key;
      if (Object.values(charts).some(c => c !== u && c.__kymo_zoom && c.cursor.sync.key === key)) out.push(TEXT.synced);
    }
    // An unzoomed chart of a live run shows its newest data (create.js showsNewest), so a selection to its right edge would follow the run.
    const xs = u.data[0];
    if (!zoomed && el.dataset.live && u.scales.x.max >= xs[xs.length - 1] && (!u.__kymo_zoom.zr || xs.stepOpen)) out.push(TEXT.follow);
    // The raw stride (create.js rawStride) is odd when the chart draws the raw values behind its smoothed lines.
    if (u.__kymo_hl.rs % 2) out.push(TEXT.trace);
    if (u.bands.length) out.push(TEXT.bands);
    if (u.__kymo_copy.nanCols) out.push(TEXT.markers);
    // The readout's colored values (smoothed ones, on a smoothed chart) and its gray column (raw values or ranges; hover.js classes the first cell that holds one) are labelled on the readout itself, which follows the pointer: this only leaves the labels on it, as custom properties, and kymo.css draws the ones that fit what the hovered chart's readout shows while notes show, so help runs no script as the pointer moves.
    const tip = u.__kymo_tip.style;
    for (const [name, value] of [['--help-colored', el.dataset.smoothed ? JSON.stringify(TEXT.colored) : ''], ['--help-raw', JSON.stringify(TEXT.raw)], ['--help-range', JSON.stringify(TEXT.range)]]) {
      if (tip.getPropertyValue(name) !== value) tip.setProperty(name, value);
    }
    // What else this chart's readout can hold: an unplottable-x count, and, with the nearest-point preference on and a second series to borrow from, a borrowed point's x.
    if (el.dataset.xnan) out.push(TEXT.xnan);
    if (u.__kymo_copy.seriesCount > 1 && root.getAttribute('data-kymo-show-nearest-point') === 'true') out.push(TEXT.borrowed);
    return out;
  };
})();
