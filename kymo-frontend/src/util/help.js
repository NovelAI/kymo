// Contextual help (docs/contextual-help.md): while the navbar's ? is hovered, focused or pinned, every on-screen element with a data-help attribute gets a short note beside it. A text shows once however many elements of a kind carry it (one example per repeated family), and an element's lines are separated by newlines. Nothing here runs, and no listener or observer exists, until the ? button's own handlers (navbar.rs) call enter or toggle, and none after the preview and the pin have both ended.
(() => {
  if (window.__kymo_help) return;
  // Esc closes these natively, and page notes paint beneath them (util::TOP_LAYER_SELECTOR, put here by main.rs).
  const TOP_LAYER = __TOP_LAYER_SELECTOR__;
  const TARGETS = '[data-help]';
  const GAP = 6;
  // Placement (spots, arrange; docs/contextual-help.md, Implementation). What a spot costs besides its side's rank (0 to 3) and its slide (in note lengths): COVER for lying over all of another control that has a note or is hidden, CROWD times the share of a waiting neighbour's spots it overlaps, LAST when it overlaps all that neighbour has left, NEXT for each member of its family passed over (the first MEMBERS on screen may carry the note). WORK is the bound: roughly the boxes one arrangement may compare (a dashboard takes under a tenth of it).
  const COVER = 8;
  const CROWD = 2;
  const LAST = 1000;
  const NEXT = 3;
  const MEMBERS = 3;
  const WORK = 400000;
  // What holds the preview open: the pointer over ?, and keyboard focus on it.
  const held = new Set();
  let pinned = false;
  // A mouse button is down somewhere other than ?: no note shows until its release.
  let pressed = false;
  let frame = 0;
  // The app rendered since the last scan: one more scan follows it, a frame later (renders).
  let settle = false;
  // Boxes compared in the current arrangement (arrange).
  let work = 0;
  let notes = [];
  // The families (text and kind) seen with a member rendered but not shown, since the notes last came up: their controls are ones the pointer brings out. Kept across scans so that the last hidden one coming out (a page's only chart) is still known for one.
  const shy = new Set();

  // Texts that only chart script state decides (uplot_chart/help.js), keyed by data-help-fn.
  const providers = {};

  const lines = el => {
    if (el.dataset.helpWithin && !el.closest(el.dataset.helpWithin)) return [];
    const own = el.dataset.help.split('\n');
    const fn = providers[el.dataset.helpFn];
    if (!fn) return own;
    // A chart whose creation failed part way (registered, but without all that create.js keeps on it) must not take the page's help down with it.
    try {
      return [...own, ...fn(el)];
    } catch (_) {
      return own;
    }
  };

  const clear = () => {
    for (const note of notes) note.remove();
    notes = [];
  };

  const overlaps = (a, b) => a.left < b.right && b.left < a.right && a.top < b.bottom && b.top < a.bottom;
  const at = (left, top, width, height) => ({left, top, right: left + width, bottom: top + height, width, height});
  const turn = ({left, top, width, height}) => at(top, left, height, width);
  const SIDES = ['below', 'above', 'beside', 'before'];
  const clamp = (value, min, max) => Math.max(min, Math.min(value, max));
  // The spots a pick's note may take against its control (ring 0, where it has a tick: `side` and `tick`), cheapest first: under it, over it, right of it and left of it (the sides first for a control over half the window tall: a resize edge is explained at its side, not from a far end), for each of the family's members that may carry it. Along a side the note slides as far as its tick still lands on the control (on the middle of an icon-sized one, anywhere from 16px inside either end of a longer one), and the starts tried are the one nearest the control's own start (the centred one, at its side), both ends of the slide, and each that just clears something the note keeps off. Every spot is in the window and off what the pick keeps off. Ring n is the same, n notes further off on the control's first two sides, tied to it by a line (`tail`) in place of the tick.
  const spots = (pick, ring, controls) => {
    const {boxes, keep, size} = pick;
    const out = [];
    boxes.forEach((box, nth) => {
      if ((work += keep.length + controls.length) > WORK) return;
      // Only what is within a note's reach of the control matters to its spots.
      const zone = at(box.left - (ring + 1) * (size.width + GAP), box.top - (ring + 1) * (size.height + GAP), box.width + 2 * (ring + 1) * (size.width + GAP), box.height + 2 * (ring + 1) * (size.height + GAP));
      const near = keep.filter(other => overlaps(other, zone));
      const under = controls.filter(other => overlaps(other, zone) && !boxes.includes(other));
      const tall = box.height > innerHeight / 2;
      const shown = at(Math.max(box.left, 0), Math.max(box.top, 0), Math.min(box.right, innerWidth) - Math.max(box.left, 0), Math.min(box.bottom, innerHeight) - Math.max(box.top, 0));
      // Right and left of the control are under and over it with x and y changed round.
      for (const turned of [false, true]) {
        if (ring && turned !== tall) continue;
        const see = turned ? turn : rect => rect;
        const {left, top, right, bottom} = see(shown);
        const {width: len, height: thick} = see(size);
        const {width: room, height: depth} = see(at(0, 0, innerWidth, innerHeight));
        const into = right - left <= 48 ? (right - left) / 2 : 16;
        const min = Math.max(GAP, left + into + 6 - len);
        const max = Math.min(room - GAP - len, right - into - 6);
        const first = clamp(turned ? (left + right - len) / 2 : left, min, max);
        const seen = near.map(see);
        for (const down of [true, false]) {
          const cross = down ? bottom + GAP + ring * (thick + GAP) : top - (ring + 1) * (thick + GAP);
          if (min > max || cross < GAP || cross + thick + GAP > depth) continue;
          const starts = [first, min, max];
          for (const other of seen) if (other.top < cross + thick && cross < other.bottom) starts.push(other.right + GAP, other.left - GAP - len);
          starts.forEach((start, at2) => {
            if (start < min || start > max || starts.indexOf(start) < at2) return;
            const spot = see(at(start, cross, len, thick));
            work += near.length + under.length;
            if (near.some(other => overlaps(spot, other))) return;
            spot.cost = NEXT * nth + (turned === tall ? 0 : 2) + !down + Math.abs(start - first) / len;
            // A note lying on another control reads as that control's.
            for (const over of under) if (overlaps(spot, over) && over.width * over.height) spot.cost += COVER * (Math.min(spot.right, over.right) - Math.max(spot.left, over.left)) * (Math.min(spot.bottom, over.bottom) - Math.max(spot.top, over.top)) / (over.width * over.height);
            spot.side = SIDES[turned * 2 + !down];
            spot.tick = clamp(left + into, start + 6, start + len - 6) - start - 4;
            // Further off, a line runs back to the control where the tick would be, under any note between them (kymo.css).
            if (ring) {
              spot.tail = see(at(start + spot.tick + 3, down ? bottom : cross + thick, 2, down ? cross - bottom : top - cross - thick));
              // A line that crosses what the note keeps off: taken only where no other spot of its ring is free.
              spot.crosses = near.some(other => overlaps(spot.tail, other));
            }
            out.push(spot);
          });
        }
      }
    });
    return out.sort((a, b) => a.cost - b.cost);
  };
  // Gives every pick that has a note its spot, all of them together. Each pick starts with its spots against its control. The lowest tier chooses first (0: a popover's notes, which paint over the page's; 1: the page's; 2: those of controls only the pointer brings out; 3, the hidden controls' picks, never choose), and within a tier the pick with the fewest spots left. It takes its cheapest, where a spot also costs what it takes from the picks still waiting, of its tier or a later one (CROWD, LAST). So in a row of icons no note takes its neighbour's only place, a note that always shows leaves room for the ones the pointer will bring out and does not move when they come, and no order is written down. A pick left with no spot takes the nearest free one further off, in line with its control and tied to it by a line; failing that it lies in its control's top left corner (a maximized chart's). WORK bounds it all: once that many boxes are compared, what is left goes to its corner.
  const arrange = picks => {
    work = 0;
    const placed = [];
    const controls = picks.flatMap(pick => pick.boxes);
    for (const pick of picks) {
      pick.spots = spots(pick, 0, controls);
      // All of them lie in here, and most picks are nowhere near each other.
      pick.reach = pick.spots.reduce((all, spot) => ({left: Math.min(all.left, spot.left), top: Math.min(all.top, spot.top), right: Math.max(all.right, spot.right), bottom: Math.max(all.bottom, spot.bottom)}), {left: Infinity, top: Infinity, right: -Infinity, bottom: -Infinity});
    }
    for (const pick of picks) pick.near = (work += picks.length) > WORK ? [] : picks.filter(other => other !== pick && overlaps(pick.reach, other.reach));
    const waiting = pick => !pick.spot && pick.spots.length;
    const rank = pick => pick.spots.length + pick.tier * 1e6;
    for (;;) {
      let next;
      for (const pick of picks) if (pick.tier < 3 && waiting(pick) && !(next && rank(pick) >= rank(next))) next = pick;
      if (!next || (work += picks.length) > WORK) break;
      let least = Infinity;
      for (const spot of next.spots) {
        let cost = spot.cost;
        for (const other of next.near) {
          if (!waiting(other) || (work++, !overlaps(spot, other.reach))) continue;
          work += other.spots.length;
          const lost = other.spots.reduce((sum, theirs) => sum + overlaps(spot, theirs), 0);
          cost += lost === other.spots.length ? LAST : (CROWD * lost) / other.spots.length;
        }
        if (cost < least) [next.spot, least] = [spot, cost];
        if (work > WORK) break;
      }
      placed.push(next.spot);
      for (const other of next.near) if (waiting(other) && overlaps(next.spot, other.reach)) other.spots = other.spots.filter(theirs => (work++, !overlaps(next.spot, theirs)));
    }
    for (const pick of picks) {
      if (pick.spot || pick.tier === 3) continue;
      const {boxes: [box], size} = pick;
      for (let ring = 1; !pick.spot && work < WORK && ring * (size.height + GAP) < innerHeight; ring++) {
        const free = spots(pick, ring, controls).filter(spot => ((work += placed.length), !placed.some(other => overlaps(spot, other))));
        pick.spot = free.find(spot => !spot.crosses) ?? free[0];
      }
      const left = clamp(box.left + GAP, GAP, innerWidth - size.width - GAP);
      const top = clamp(Math.max(box.top, 0) + GAP, GAP, innerHeight - size.height - GAP);
      pick.spot ??= at(left, top, size.width, size.height);
      placed.push(pick.spot);
    }
  };

  // One pass: reads first (an element's box only while it is hidden, has a text not yet taken, may yet carry its family's note, or is a button; the last scan's notes are still up, and neither move a box nor take a hit), then the old notes go and the new ones are added, measured and placed.
  const scan = () => {
    frame = 0;
    if (settle) {
      settle = false;
      rescan();
    }
    if (pressed) return;
    const taken = new Map();
    const picks = [];
    const rect = el => (({left, top, width, height}) => at(left, top, width, height))(el.getBoundingClientRect());
    // What a page note keeps off, besides the other notes: the open popovers, which would cut it off, ? itself, and every icon-sized thing that has a note, is an explained button, or is hidden (a chart's actions, until the pointer brings them out). A note may lie over a long name, a chart or a list.
    const keep = [...document.querySelectorAll(TOP_LAYER), document.getElementById('kymo-help')].filter(Boolean).map(rect);
    const visible = el => el.checkVisibility?.({opacityProperty: true, visibilityProperty: true, contentVisibilityAuto: true}) ?? true;
    for (const el of document.querySelectorAll(TARGETS)) {
      // A family is a text on one kind of element (its first class, else its tag): the run list's resize edge and the settings panel's both say theirs.
      const family = '\n' + (el.classList[0] ?? el.tagName);
      const said = lines(el);
      const keys = said.map(text => text + family);
      const fresh = said.filter((text, nth) => !taken.has(keys[nth]));
      // The family's note may yet go to this member instead of the first: it says the same, and fewer than MEMBERS do so far.
      const first = fresh.length ? null : taken.get(keys[0]);
      const maybe = first?.boxes.length < MEMBERS && first.text === said.join('\n');
      if (!fresh.length && !maybe && !el.matches('button')) continue;
      const hidden = !visible(el);
      // Rendered and not shown is what the pointer brings out (a chart's actions); not rendered at all (a closed menu's item, a run row far down the list, whose content the browser skips) is not.
      if (hidden && (el.checkVisibility?.({contentVisibilityAuto: true}) ?? true)) for (const key of keys) shy.add(key);
      const box = rect(el);
      if (box.width === 0 && box.height === 0 || box.bottom <= 0 || box.top >= innerHeight || box.right <= 0 || box.left >= innerWidth) continue;
      // A note for a control inside an open popover goes inside that popover, the only place it can paint above it, and keeps off the other notes only.
      const host = el.closest(TOP_LAYER) ?? document.body;
      // A hidden control has a pick and no note (tier 3): the notes that show leave it room for the one it will have.
      const pick = {boxes: [box], host, keep: host === document.body ? keep : [], keys, text: (hidden ? said : fresh).join('\n'), tier: hidden ? 3 : host === document.body ? 1 : 0};
      if (!hidden && (fresh.length || maybe)) {
        // On screen means hit near the left end of its part inside the window, at mid height: not scrolled out of its panel, and not under a maximized chart or a modal. Near the left, since a chart title's hidden actions can lie over its middle.
        const left = Math.max(box.left, 0);
        const hit = document.elementFromPoint(left + Math.min((Math.min(box.right, innerWidth) - left) / 2, 12), (Math.max(box.top, 0) + Math.min(box.bottom, innerHeight)) / 2);
        if (!el.contains(hit)) continue;
        for (const text of fresh) taken.set(text + family, pick);
        if (maybe) first.boxes.push(box);
      }
      if (box.height <= 48 && box.width <= 48) keep.push(box);
      if (hidden || fresh.length) picks.push(pick);
    }
    clear();
    // A hidden control's pick is measured through one stand-in note per text, gone again before anything paints.
    const probes = new Map();
    for (const pick of picks) {
      // A control that only the pointer brings out, known by the hidden ones of its family (another chart's actions), chooses after the rest (tier 2): so no note that was showing moves when it appears.
      if (pick.tier === 1 && pick.keys.some(key => shy.has(key))) pick.tier = 2;
      if (pick.tier === 3 && probes.has(pick.text)) continue;
      const note = document.createElement('div');
      note.className = 'kymo-help-note';
      note.textContent = pick.text;
      // For sight only: the live region below is what a screen reader gets.
      note.setAttribute('aria-hidden', 'true');
      pick.host.appendChild(note);
      if (pick.tier === 3) probes.set(pick.text, note);
      else notes.push((pick.note = note));
    }
    for (const pick of picks) pick.size = rect(pick.note ?? probes.get(pick.text));
    for (const probe of probes.values()) probe.remove();
    arrange(picks);
    for (const {note, spot} of picks) {
      if (!note) continue;
      note.style.left = spot.left + 'px';
      note.style.top = spot.top + 'px';
      // The tick (8px across) runs along the note's top or bottom edge, or down its side for a note right or left of its control.
      if (spot.side) note.dataset.side = spot.side;
      if (spot.side) note.style.setProperty('--tick', spot.tick + 'px');
      if (spot.tail) {
        note.dataset.far = '';
        note.style.setProperty('--reach', Math.max(spot.tail.width, spot.tail.height) + 'px');
      }
    }
  };
  const rescan = () => {
    if (!frame) frame = requestAnimationFrame(scan);
  };

  // kymo.css brings out what help explains in place while notes show: every run row's … button, and a chart readout's own labels.
  const mark = on => document.documentElement.toggleAttribute('data-kymo-help', on);
  // A held button takes the readout's labels off with the notes, by an attribute of its own: the first one matches every run row, and changing it at each press and release would restyle them all (10 ms in WebKit on 253 rows), at the start of a chart drag among others.
  const hold = on => document.documentElement.toggleAttribute('data-kymo-held', on);
  // A held button hides the notes until its release, so no note sits under a drag (visibility painting, a resize, a chart gesture) or takes part in it. One handler for a press, a release and the pointer crossing onto an element, since a drag can begin or end outside the window, where no release reaches the page. A release is a pointerup (a button that disables itself on its press gets no mouseup), the end of a native drag (a section's reorder gets neither), or a context menu opening. The pointer on ? is exempt, since a press there toggles the pin. A keyboard activation holds no button (main.rs replays it as a mousedown with none, and no release), and a pointer moving with none reaches controls that appear only under it.
  const pointer = e => {
    if (!e.buttons || e.type === 'dragend' || e.type === 'contextmenu') {
      // From one run row to another, or within one, the pointer brings out nothing that help explains, and each crossing there repaints the charts' highlight: no scan.
      if (!pressed && e.type === 'mouseover' && e.target.closest?.('.sidebar-run') && e.relatedTarget?.closest?.('.sidebar-run')) return;
      pressed = false;
      hold(false);
      return rescan();
    }
    if (e.target.closest?.('#kymo-help')) return;
    pressed = true;
    hold(true);
    clear();
  };
  // The pointer left the window: what it had brought out is gone, and no crossing onto another element says so.
  const gone = e => {
    if (!e.relatedTarget) rescan();
  };
  // Nothing ties a note to its control for a screen reader, so while help is pinned this region, in the page only then, gives it the texts of the control that takes focus or of the explained element around it (a field's row), else, for a control, of those inside it (a run's link); a focused container (a panel's body) says nothing.
  const spoken = Object.assign(document.createElement('div'), {className: 'visually-hidden'});
  spoken.setAttribute('aria-live', 'polite');
  const speak = e => {
    rescan();
    const own = e.target.closest?.(TARGETS);
    const els = own ? [own] : e.target.matches?.('a, button, input, select, textarea, [role="checkbox"]') ? [...e.target.querySelectorAll(TARGETS)] : [];
    spoken.textContent = [...new Set(els.flatMap(lines))].join(' ');
  };
  // Pinned help takes Esc last: only when no in-app layer consumed it, no native layer will close on it, and neither Trash selection, a maximized chart nor a chart gesture (window.__kymo_zg, create.js) is there to take it.
  const key = e => {
    if (!pinned || pressed || e.key !== 'Escape' || e.isComposing || e.defaultPrevented || window.__kymo_zg) return;
    if (document.querySelector(TOP_LAYER + ', .sidebar-trash-mode, .maximize-overlay')) return;
    e.preventDefault();
    toggle();
  };
  // The app rendered: an explained control appeared, went, or changed what it says. Nothing else counts: not a chart's loading spinner coming and going with each live reply, not the notes themselves (a popover's are inside the root, and carry no help attribute). Chart readouts live outside the app's root, and a chart's repaint changes none of its children or of these attributes, so chart hover never lands here.
  const explained = nodes => [...nodes].some(node => node.nodeType === 1 && (node.matches(TARGETS) || node.querySelector(TARGETS)));
  const renders = new MutationObserver(records => {
    if (!records.some(({type, addedNodes, removedNodes}) => type !== 'childList' || explained(addedNodes) || explained(removedNodes))) return;
    // Rows just added to the run list report themselves not rendered until the frame after (content-visibility: auto), so the scan this starts is followed by one more.
    settle = true;
    rescan();
  });
  const RENDERED = {subtree: true, childList: true, attributeFilter: ['data-help', 'data-help-within', 'data-live', 'data-smoothed', 'data-step-zoomed', 'data-xnan']};
  // A scroll that can move an explained control: the page's, or that of something holding one (not a log following its tail).
  const scrolled = e => {
    if (e.target === document || e.target.querySelector?.(TARGETS)) rescan();
  };
  // While notes show: scrolls and resizes move targets under them, a held button hides them, the pointer reaches controls that exist only under it, focus is spoken (and its loss ends a text shown only within focus), and Esc unpins.
  const WATCH = [['scroll', scrolled, true], ['resize', rescan, false], ['mouseover', pointer, true], ['mousedown', pointer, true], ['pointerup', pointer, true], ['dragend', pointer, true], ['contextmenu', pointer, true], ['mouseout', gone, true], ['focusin', speak, true], ['focusout', rescan, true], ['keydown', key, false]];

  // Both are safe to repeat: a listener or an observed root added twice is there once, and one removed twice is gone.
  const show = () => {
    for (const [type, fn, capture] of WATCH) window.addEventListener(type, fn, capture);
    renders.observe(document.querySelector('.app-root'), RENDERED);
    mark(true);
    rescan();
  };
  const hide = () => {
    for (const [type, fn, capture] of WATCH) window.removeEventListener(type, fn, capture);
    renders.disconnect();
    mark(false);
    hold(false);
    shy.clear();
    settle = false;
    pressed = false;
    cancelAnimationFrame(frame);
    frame = 0;
    clear();
  };
  const pin = on => {
    pinned = on;
    // The live region is in the page before the first focus it reports, or a screen reader would miss that one.
    if (on) document.body.appendChild(spoken);
    else spoken.remove();
    spoken.textContent = '';
    document.getElementById('kymo-help')?.setAttribute('aria-pressed', String(on));
  };
  // An unpin hides the notes although ? is still hovered or focused; the pointer's or focus's next entry shows them again.
  const toggle = () => {
    pin(!pinned);
    if (pinned) show();
    else hide();
  };
  // The dashboard page changed or went (navbar.rs).
  const end = () => {
    pin(false);
    held.clear();
    hide();
  };

  window.__kymo_help = {
    providers,
    toggle,
    end,
    enter(what) {
      held.add(what);
      if (!pinned) show();
    },
    leave(what) {
      held.delete(what);
      if (!held.size && !pinned) hide();
    },
  };
})();
