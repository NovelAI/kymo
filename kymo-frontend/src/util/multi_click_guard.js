// A control acts on its press (util::primary), and a press can move what lies under the pointer: the options panel docks and undocks, a chart maximizes and un-maximizes, a removed row's neighbour slides up. A later press of the same multi-click comes too soon to be aimed at what moved in (a section header, which collapses on a press; a source's Remove, which deletes its source), so it acts only if it reaches the control the first press reached or a field with a caret; anywhere else, it is swallowed with its mouseup, click and dblclick. A control that stays under the pointer takes every press: a double-click on the navbar's or a section's gear opens the panel and closes it again.
// A press in a field with a caret is never swallowed: it only places the caret or selects, and a double-click that opened a field in place (a chart title's rename) goes on to select in it.
// Limits: controls are told apart by element, so a control that its own press replaces with a new element counts as gone (the run list's Trash, which turns into Cancel, takes one press of a double-click), and a list whose items a press removes needs keys, or Dioxus renders the next item into the pressed element and it takes the second press.
// The swallow stops the mouse events in the document's capture phase: window capture listeners (help.js, the run list's pick bridge) and pointer events (which light-dismiss a popover) still see the press, and in Firefox so does a swallowed press's mouseup when its target is replaced before the release (it arrives with detail 0), though a swallowed press begins no drag, so no mouseup handler waits for it.
// A press the guard doesn't judge (on a disabled control, which sends no mousedown, or with another button) leaves `first` alone, so a multi-click begun there is judged against the first press of the last multi-click the guard judged.
// A control only the browser or plain JS acts on (the run row's ⋯, a width-drag edge, a scrollbar) counts as part of the nearest element Dioxus listens on, so two of them in one such element are one control.
(() => {
  // The control a press reaches is the nearest element that acts on it: a chart (uPlot listens on its plot itself), a link or label (the browser acts for them; a label is one control with the box it labels), or an element Dioxus listens on, which it marks with data-dioxus-id and routes the press to. So a press anywhere on a chart title reaches the title, one on a button's icon (which the button may swap as it acts) the button, and one on a chart's plot (which a rebuild replaces) the chart. Elements are compared, not their ids: Dioxus reuses the values.
  const control = el => {
    const acting = el.closest?.('.chart-container, a[href], label, [data-dioxus-id]') ?? el;
    return acting instanceof HTMLLabelElement ? acting.control ?? acting : acting;
  };
  // Text, search, URL, tel and password inputs and textareas report a caret position; email inputs (kymo has none), number, range, checkbox and colour inputs (a press there can change a value) and other elements report none.
  const hasCaret = el => typeof el.selectionStart === 'number';
  let first = null;
  let swallowing = false;
  const guard = e => {
    // Only the primary button presses controls.
    if (e.button !== 0) return;
    // A keyboard activation's press (detail 0, KB_ACTIVATE_JS) is no part of a multi-click: it neither starts one nor ends a swallow under way.
    if (e.type === 'mousedown' && e.detail > 0) {
      const reached = control(e.target);
      if (e.detail === 1) first = reached;
      swallowing = reached !== first && !hasCaret(e.target);
    }
    if (swallowing && e.detail >= 2) {
      e.preventDefault();
      e.stopImmediatePropagation();
    }
  };
  for (const type of ['mousedown', 'mouseup', 'click', 'dblclick']) document.addEventListener(type, guard, true);
})();
