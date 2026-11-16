// Contextual help for a numeric chart (docs/contextual-help.md): the texts only chart script state decides, for util/help.js to show on the chart's container. It runs when help scans the page and at no other time, reads what create.js already keeps on the uPlot instance, what uplot_chart.rs renders on the container (data-live, data-smoothed, data-step-zoomed), and writes only the X axis's text and the readout's label properties, so it adds nothing to hover, repaint or rebuild.
(() => {
  const TEXT = {
    axis: 'Drag axis to rescale',
    copyMac: '⌘C copies values',
    copy: 'Ctrl+C copies values',
    follow: 'Include right edge to follow new data',
    colored: 'Smoothed',
    raw: 'Raw value',
    range: 'Range',
    borrowed: '@: nearest sample’s X',
  };
  window.__kymo_help.providers.chart = el => {
    const u = window.__kymo_charts?.[el.id];
    if (!u) return [];
    // The X-axis strip's own note sits on uPlot's element for that axis (its first), which takes the text here, at the first scan after the chart's creation, so that creating a chart does no work for help; the attribute's arrival brings the next scan (util/help.js).
    const axis = u.root.querySelector('.u-axis');
    if (axis && !axis.dataset.help) axis.dataset.help = TEXT.axis;
    // What labels the readout is left on the readout itself, as custom properties: its colored values (smoothed ones, on a smoothed chart), its gray column (raw value or range; hover.js classes the first cell that holds one), the copy shortcut, and a borrowed point's @ (hover.js classes a readout that has one). kymo.css draws them while notes show, so help runs no script as the readout follows the pointer.
    const tip = u.__kymo_tip.style;
    for (const [name, value] of [['--help-colored', el.dataset.smoothed ? JSON.stringify(TEXT.colored) : ''], ['--help-raw', JSON.stringify(TEXT.raw)], ['--help-range', JSON.stringify(TEXT.range)], ['--help-borrowed', JSON.stringify(TEXT.borrowed)], ['--help-copy', JSON.stringify(navigator.platform.startsWith('Mac') ? TEXT.copyMac : TEXT.copy)]]) {
      tip.setProperty(name, value);
    }
    // An unzoomed chart of a live run shows its newest data (create.js showsNewest), so a selection to its right edge would follow the run. A step chart zooms through the step store and keeps no zoom on its instance (create.js); uplot_chart.rs says whether the answer shown is a zoomed one, and an unzoomed one has no right bound.
    const xs = u.data[0];
    return !u.__kymo_userzoom && !el.dataset.stepZoomed && el.dataset.live && u.scales.x.max >= xs[xs.length - 1] ? [TEXT.follow] : [];
  };
})();
