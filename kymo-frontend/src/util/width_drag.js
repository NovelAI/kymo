// Width drags for the run list (its right edge) and the docked options panel (its left edge). Each width is a percentage of the window's, so it follows window resizes, kept in a CSS var on <html>, which the VDOM never diffs, so a re-render can't clobber it; the handler runs no Rust, and window listeners exist only for the drag. A var changed on <html> restyles the whole page, so mid-drag the width goes on the dragged box alone, as an inline flex-basis (neither box's rsx sets a style), and the release moves it to <html>. Installed before the app mounts, so the saved widths are in place before anything lays out at a default and then resizes every chart again.
(() => {
  const root = document.documentElement;
  // Each saved width stays 10-75% of the window: the minimum keeps a saved run list wide enough to grab. A row too short for everything squeezes them (kymo.css), but the saved width stays the one dragged to.
  const DRAGS = {
    'sidebar-resize': {var: '--kymo-sidebar-w', key: 'kymo_sidebar_w', legacy: 'mkdb2_sidebar_w', anchor: 'left'},
    'options-panel-resize': {var: '--kymo-options-w', key: 'kymo_options_w', anchor: 'right'},
  };
  const HANDLES = Object.keys(DRAGS).map(name => '.' + name).join(', ');
  const apply = (style, name, percent) => {
    percent = Math.max(10, Math.min(percent, 75));
    style.setProperty(name, percent + 'vw');
    return percent;
  };

  for (const o of Object.values(DRAGS)) {
    let saved = NaN;
    try {
      saved = parseFloat(localStorage.getItem(o.key) ?? (o.legacy && localStorage.getItem(o.legacy)));
    } catch (_) {}
    // Bundles before FRO-747 stored the run list's width in pixels, always at least 160; percentages are at most 75.
    if (o.legacy && saved > 75) saved = (saved / innerWidth) * 100;
    if (Number.isFinite(saved)) apply(root.style, o.var, saved);
  }

  document.addEventListener('mousedown', ev => {
    const handle = ev.target.closest?.(HANDLES);
    if (!handle || ev.button !== 0) return;
    ev.preventDefault();
    const o = DRAGS[[...handle.classList].find(name => name in DRAGS)];
    const box = handle.parentElement;
    const anchor = box.getBoundingClientRect()[o.anchor];
    // The box grows away from its anchored edge; past that edge (a drag out of the window) it is at its narrowest.
    const away = o.anchor === 'left' ? 1 : -1;
    // Until the release the chart-height drag's overlay (metric_rect/resize.js) covers the page, so the pointer, which runs ahead of the edge, hovers no run row or chart and the cursor holds; one inherited from <body> would restyle the page.
    const overlay = document.getElementById('resize-drag-overlay') ?? document.body.appendChild(Object.assign(document.createElement('div'), {id: 'resize-drag-overlay'}));
    overlay.style.cssText = 'position:fixed;inset:0;z-index:9998;cursor:col-resize;';
    let percent;
    const resize = e => (percent = apply(box.style, 'flex-basis', ((e.clientX - anchor) * away / innerWidth) * 100));
    const move = e => {
      // A release outside the browser sends no mouseup; the next move's button state still tells us to finish.
      if (e.buttons === 0 || !box.isConnected) return finish();
      resize(e);
    };
    const finish = e => {
      window.removeEventListener('mousemove', move);
      window.removeEventListener('mouseup', finish);
      window.removeEventListener('blur', finish);
      overlay.style.display = 'none';
      // A press without a drag changes nothing, and a box that unmounted mid-drag (the panel closing, or a project switch) has no width to keep: it reopens at the width <html> still holds.
      if (percent === undefined || !box.isConnected) return;
      if (Number.isFinite(e?.clientX)) resize(e);
      apply(root.style, o.var, percent);
      box.style.removeProperty('flex-basis');
      try {
        localStorage.setItem(o.key, String(percent));
        if (o.legacy) localStorage.removeItem(o.legacy);
      } catch (_) {}
    };
    window.addEventListener('mousemove', move);
    window.addEventListener('mouseup', finish);
    window.addEventListener('blur', finish);
  });
})();
