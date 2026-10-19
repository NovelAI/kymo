// Records what a press lands on before its handlers run, for a blur it causes that hands focus to nothing (`util::press_target`): WebKit and Firefox on macOS focus no button on a press, and a handler may replace what was pressed. The link, button or input under the pointer, else its section; none in the section whose name field is open, whose bar and grip follow the section. A press inside another section blurs that field at once, since a handler there (a chart's zoom) may cancel the press's focus change. The press's blur comes in the press's task; a timeout clears the record after it.
// Limit: a control that takes no focus on a press and isn't a link, button or input counts as neutral, as does a press on a disabled control, which no engine sends a mousedown.
(() => {
  const CONTROLS = 'a[href],button,input,.section';
  document.addEventListener('mousedown', e => {
    const hit = e.target.closest?.(CONTROLS);
    window.__kymo_press = hit?.closest('.section')?.querySelector('.section-name-input') ? null : hit;
    const field = document.activeElement;
    if (field?.matches('.section-name-input') && window.__kymo_press?.closest('.section')) field.blur();
    setTimeout(() => { window.__kymo_press = null; });
  }, true);
})();
