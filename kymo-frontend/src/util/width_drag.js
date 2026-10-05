// Width drags for the run list (its right edge) and the docked options panel (its left edge). Each width is a percentage of the window's, so it follows window resizes, kept in a CSS var on <html>, which the VDOM never diffs, so a re-render can't clobber a mid-drag width and no Rust runs per mousemove; window listeners exist only for the drag. Installed before the app mounts, so the saved widths are in place before anything lays out at a default and then resizes every chart again.
(() => {
  const root = document.documentElement;
  // Each stays 10-75% of the window: the minimum keeps a run list always grabbable (it never collapses to zero). A row too short for everything squeezes them (kymo.css), but the saved width stays the one dragged to.
  const DRAGS = {
    'sidebar-resize': {var: '--kymo-sidebar-w', key: 'kymo_sidebar_w', legacy: 'mkdb2_sidebar_w', anchor: 'left'},
    'options-panel-resize': {var: '--kymo-options-w', key: 'kymo_options_w', anchor: 'right'},
  };
  const HANDLES = Object.keys(DRAGS).map(name => '.' + name).join(', ');
  const apply = (o, percent) => {
    percent = Math.max(10, Math.min(percent, 75));
    root.style.setProperty(o.var, percent + 'vw');
    return percent;
  };

  for (const o of Object.values(DRAGS)) {
    let saved = NaN;
    try {
      saved = parseFloat(localStorage.getItem(o.key) ?? (o.legacy && localStorage.getItem(o.legacy)));
    } catch (_) {}
    // Bundles before FRO-747 stored the run list's width in pixels, always at least 160; percentages are at most 75.
    if (o.legacy && saved > 75) saved = (saved / innerWidth) * 100;
    if (Number.isFinite(saved)) apply(o, saved);
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
    const start = root.style.getPropertyValue(o.var);
    let live = true, moved = false, percent;
    const resize = e => (percent = apply(o, ((e.clientX - anchor) * away / innerWidth) * 100));
    const move = e => {
      // A release outside the browser sends no mouseup; the next move's button state still tells us to finish.
      if (e.buttons === 0 || !box.isConnected) return finish();
      moved = true;
      resize(e);
    };
    const finish = e => {
      if (!live) return;
      live = false;
      window.removeEventListener('mousemove', move);
      window.removeEventListener('mouseup', finish);
      window.removeEventListener('blur', finish);
      document.body.style.cursor = '';
      // A press without a drag changes nothing, and a box that unmounted mid-drag (the panel closing) has no width to keep: it reopens at the width it had.
      if (!box.isConnected) root.style.setProperty(o.var, start);
      if (!moved || !box.isConnected) return;
      if (e && Number.isFinite(e.clientX)) resize(e);
      // The width dragged to, which a short row only shrinks for as long as it stays short.
      try {
        localStorage.setItem(o.key, String(percent));
        if (o.legacy) localStorage.removeItem(o.legacy);
      } catch (_) {}
    };
    document.body.style.cursor = 'col-resize';
    window.addEventListener('mousemove', move);
    window.addEventListener('mouseup', finish);
    window.addEventListener('blur', finish);
  });
})();
