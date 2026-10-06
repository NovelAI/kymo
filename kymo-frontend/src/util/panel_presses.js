// The options panel docks and undocks under the pointer, so the second press of a double-click on a control that opens, closes or switches it would land on whatever moved there (a section header, a source's Remove). `window.__kymo_panel_moved()`, called on every such press, swallows the rest of that multi-click: the presses counted as its second or later, until a fresh single press.
(() => {
  let armed = false;
  const swallow = e => {
    if (e.detail >= 2 && armed) {
      e.preventDefault();
      e.stopImmediatePropagation();
    } else if (e.type === 'mousedown' && e.detail === 1) {
      armed = false;
    }
  };
  for (const type of ['mousedown', 'mouseup', 'click', 'dblclick']) document.addEventListener(type, swallow, true);
  window.__kymo_panel_moved = () => { armed = true; };
})();
