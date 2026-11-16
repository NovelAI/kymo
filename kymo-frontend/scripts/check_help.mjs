#!/usr/bin/env node

import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { createContext, Script } from "node:vm";

const read = (path) => readFileSync(new URL(path, import.meta.url), "utf8");
// main.rs puts util::TOP_LAYER_SELECTOR into help.js as it installs it.
const TOP_LAYER = /TOP_LAYER_SELECTOR: &str = "(.*)";/.exec(read("../src/util/mod.rs"))[1];
const helpScript = new Script(read("../src/util/help.js").replace("__TOP_LAYER_SELECTOR__", JSON.stringify(TOP_LAYER)), { filename: "help.js" });
const chartScript = new Script(read("../src/components/uplot_chart/help.js"), { filename: "uplot_chart/help.js" });

let passed = 0;
function test(name, run) {
  run();
  console.log(`ok ${++passed} - ${name}`);
}

const box = (left, top, width, height) => ({ left, top, width, height, right: left + width, bottom: top + height });
const overlaps = (a, b) => a.left < b.right && b.left < a.right && a.top < b.bottom && b.top < a.bottom;
const TARGETS = "[data-help]";
const LAYERS = `${TOP_LAYER}, .sidebar-trash-mode, .maximize-overlay`;

// A stand-in page of 1000 by 600: elements are plain objects with a box, and elementFromPoint finds the last one (the topmost) whose box holds the point, and nothing outside the window. An element is `shown` unless it is rendered invisibly (opacity 0, as hover-revealed actions are), which the hit test does not see, and `covered` when something else takes the hit.
function fixture() {
  const listeners = [];
  const frames = [];
  const elements = [];
  const appended = [];
  let layer = null;
  let popovers = [];
  let observing = null;
  let rendered;
  const attributes = new Map([["aria-pressed", "false"]]);
  // ? sits in the window's top right corner.
  const button = { setAttribute: (name, value) => attributes.set(name, value), getBoundingClientRect: () => box(960, 4, 20, 20) };
  function element(first, help, rect, extra = {}) {
    const el = {
      dataset: { ...(help === undefined ? {} : { help }), ...extra.dataset },
      classList: first ? [first] : [],
      tagName: extra.tagName ?? "DIV",
      id: extra.id,
      shown: true,
      covered: false,
      inside: extra.inside ?? [],
      host: extra.host ?? null,
      children: extra.children ?? [],
      // Is it rendered at all (`gone` says it is not: a closed menu's item, a run row far down the list) and, when opacity counts, shown?
      checkVisibility: (options) => !el.gone && (!options.opacityProperty || el.shown),
      // Elements are buttons, and controls for focus, unless a test says otherwise.
      matches: (selector) => (selector === "button" ? extra.button !== false : (assert.equal(selector, 'a, button, input, select, textarea, [role="checkbox"]'), extra.control !== false)),
      getBoundingClientRect: () => rect,
      contains: (other) => other === el || el.children.includes(other),
      closest: (selector) => (selector === TOP_LAYER ? el.host : selector === TARGETS ? (help === undefined ? null : el) : el.inside.includes(selector) ? el : null),
      querySelectorAll: (selector) => (assert.equal(selector, TARGETS), el.children),
    };
    if (extra.listed !== false) elements.push(el);
    return el;
  }
  const made = () => {
    const node = { style: { setProperty: (name, value) => (node.style[name] = value) }, dataset: {}, textContent: "", attributes: new Map(), setAttribute: (name, value) => node.attributes.set(name, value) };
    node.remove = () => {
      if (appended.includes(node)) appended.splice(appended.indexOf(node), 1);
    };
    // Notes measure 8px a character and 20px a line.
    node.getBoundingClientRect = () => {
      const rows = node.textContent.split("\n");
      return box(0, 0, 8 * Math.max(...rows.map((row) => row.length)), 20 * rows.length);
    };
    return node;
  };
  const host = (name) => ({
    name,
    appendChild(child) {
      child.parent = name;
      appended.push(child);
      return child;
    },
  });
  const root = new Map();
  const appRoot = {};
  const document = {
    body: host("body"),
    documentElement: { toggleAttribute: (name, on) => (on ? root.set(name, "") : root.delete(name)) },
    getElementById: (id) => (id === "kymo-help" ? button : null),
    createElement: made,
    querySelectorAll: (selector) => (selector === TOP_LAYER ? popovers : (assert.equal(selector, TARGETS), elements.filter((el) => el.dataset.help !== undefined))),
    querySelector: (selector) => (selector === ".app-root" ? appRoot : (assert.equal(selector, LAYERS), layer)),
    elementFromPoint: (x, y) => (x >= 1000 || y >= 600 ? null : elements.findLast((el) => !el.covered && x >= el.getBoundingClientRect().left && x < el.getBoundingClientRect().right && y >= el.getBoundingClientRect().top && y < el.getBoundingClientRect().bottom) ?? null),
  };
  // As in a browser, adding a listener that is there and removing one that is not do nothing.
  const same = (type, fn, capture) => (l) => l.type === type && l.fn === fn && l.capture === capture;
  const window = {
    addEventListener: (type, fn, capture) => {
      if (!listeners.some(same(type, fn, capture))) listeners.push({ type, fn, capture });
    },
    removeEventListener: (type, fn, capture) => {
      if (listeners.some(same(type, fn, capture))) listeners.splice(listeners.findIndex(same(type, fn, capture)), 1);
      assert.ok(!listeners.some((l) => l.type === type && l.fn === fn), `a ${type} listener was removed in the wrong phase`);
    },
  };
  class MutationObserver {
    constructor(callback) {
      rendered = callback;
    }
    observe(target, options) {
      assert.equal(target, appRoot);
      observing = options;
    }
    disconnect() {
      observing = null;
    }
  }
  const context = createContext({
    window,
    document,
    MutationObserver,
    innerWidth: 1000,
    innerHeight: 600,
    navigator: { platform: "MacIntel" },
    requestAnimationFrame: (fn) => frames.push(fn),
    cancelAnimationFrame: (id) => {
      if (id) frames[id - 1] = null;
    },
  });
  helpScript.runInContext(context);
  chartScript.runInContext(context);
  const noteNodes = () => appended.filter((node) => node.className === "kymo-help-note");
  return {
    help: window.__kymo_help,
    window,
    element,
    host,
    attributes,
    root,
    listeners,
    flush: () => {
      for (const fn of frames.splice(0)) fn?.();
    },
    fire: (type, event = {}) => {
      event.type = type;
      // A scroll is the page's unless a test says what scrolled.
      if (type === "scroll") event.target ??= document;
      for (const l of listeners.filter((l) => l.type === type)) l.fn(event);
    },
    covers: (value) => {
      layer = value;
    },
    opens: (...rects) => {
      popovers = rects.map((rect) => ({ getBoundingClientRect: () => rect }));
    },
    platform: (name) => {
      context.navigator.platform = name;
    },
    observing: () => observing,
    render: (records) => rendered(records),
    notes: noteNodes,
    texts: () => noteNodes().map((node) => node.textContent),
    // Each note's box as placed.
    boxes: () => noteNodes().map((node) => box(parseFloat(node.style.left), parseFloat(node.style.top), node.getBoundingClientRect().width, node.getBoundingClientRect().height)),
    spoken: () => appended.find((node) => node.attributes.has("aria-live")),
  };
}

test("nothing listens, observes or shows until ? is hovered, and nothing is left when the pointer goes", () => {
  const f = fixture();
  f.element("run-marker-toggle", "Click (drag) to show/hide runs", box(10, 100, 20, 20));
  assert.equal(f.listeners.length, 0);
  assert.equal(f.observing(), null);
  f.help.enter("pointer");
  assert.deepEqual(f.listeners.map((l) => l.type), ["scroll", "resize", "mouseover", "mousedown", "pointerup", "dragend", "contextmenu", "mouseout", "focusin", "focusout", "keydown"]);
  assert.deepEqual(f.listeners.filter((l) => !l.capture).map((l) => l.type), ["resize", "keydown"], "the rest capture: a panel's scroll does not bubble and a control may stop its press, but Escape goes to the app's own layers first");
  assert.ok(f.observing().childList && f.observing().subtree);
  // Every attribute a text depends on, whether the mechanism or a chart's texts read it.
  assert.deepEqual([...f.observing().attributeFilter], ["data-help", "data-help-within", "data-live", "data-smoothed", "data-step-zoomed"]);
  f.flush();
  assert.deepEqual(f.texts(), ["Click (drag) to show/hide runs"]);
  assert.equal(f.notes()[0].attributes.get("aria-hidden"), "true");
  assert.ok(f.root.has("data-kymo-help"));
  f.help.leave("pointer");
  assert.ok(!f.root.has("data-kymo-help"), "the run rows' dots would stay out");
  assert.equal(f.listeners.length, 0);
  assert.equal(f.observing(), null);
  assert.deepEqual(f.texts(), []);
});

test("a family shows once, a text on another kind of element again, and an element's lines together", () => {
  const f = fixture();
  for (let i = 0; i < 3; i++) f.element("run-name-host", "Edits apply immediately\n#N: run number, shown when names repeat", box(40, 100 + 30 * i, 100, 20));
  f.element("sidebar-resize", "Drag to resize", box(200, 0, 4, 600));
  f.element("options-panel-resize", "Drag to resize", box(700, 0, 4, 600));
  // Elements with no class are a family by tag.
  f.element(null, "Same image position across runs", box(400, 300, 80, 20), { tagName: "SELECT" });
  f.element(null, "Same image position across runs", box(400, 400, 80, 20), { tagName: "SELECT" });
  f.element(null, "Same image position across runs", box(400, 500, 80, 20), { tagName: "INPUT" });
  f.help.enter("pointer");
  f.flush();
  assert.deepEqual(f.texts(), ["Edits apply immediately\n#N: run number, shown when names repeat", "Drag to resize", "Drag to resize", "Same image position across runs", "Same image position across runs"]);
});

test("only controls on screen are explained", () => {
  const f = fixture();
  f.element("a", "hidden", box(10, 10, 20, 20)).shown = false;
  f.element("b", "below the window", box(10, 700, 20, 20));
  f.element("c", "no box", box(0, 0, 0, 0));
  f.element("d", "covered", box(10, 200, 20, 20)).covered = true;
  f.element("e", "outside its state", box(10, 300, 20, 20), { dataset: { helpWithin: ":focus" } });
  f.element("f", "inside its state", box(10, 400, 20, 20), { dataset: { helpWithin: ":focus" }, inside: [":focus"] });
  f.element("g", "shown", box(10, 500, 20, 20));
  // Only a sliver in the window, at its bottom edge: the sliver is what must be hit, near its left end, where a control lying over its middle does not hide it.
  f.element("h", "a sliver", box(200, 590, 300, 400));
  f.element("h-cover", undefined, box(300, 590, 200, 10));
  // The child is on top, as a descendant is, so it takes the hit.
  const parent = f.element("i", "hit on a child", box(300, 300, 40, 20));
  parent.children.push(f.element("i-child", undefined, box(300, 300, 40, 20)));
  f.help.enter("focus");
  f.flush();
  assert.deepEqual(f.texts(), ["inside its state", "shown", "a sliver", "hit on a child"]);
});

test("a hidden first example gives way to the next one of its family", () => {
  const f = fixture();
  f.element("rect-action", "Remove chart; keep logged data", box(300, 100, 20, 20)).shown = false;
  f.element("rect-action", "Remove chart; keep logged data", box(600, 100, 20, 20));
  f.help.enter("pointer");
  f.flush();
  assert.equal(f.notes().length, 1);
  assert.equal(f.notes()[0].style.left, "600px");
});

test("a note sits under its target, above it at the window's bottom, and beside a full-height edge", () => {
  const f = fixture();
  f.element("a", "under", box(100, 100, 50, 20));
  f.element("b", "above", box(100, 590, 50, 10));
  f.element("c", "edge", box(300, 0, 4, 600));
  f.element("d", "left of a target that fills the right edge", box(990, 0, 10, 600));
  f.element("e", "kept in the window sideways", box(900, 100, 50, 20));
  f.element("f", "tall", box(500, 40, 4, 560));
  f.help.enter("pointer");
  f.flush();
  const [under, above, edge, left, kept, tall] = f.notes();
  assert.deepEqual([tall.style.left, tall.style.top, tall.dataset.side], ["510px", "310px", "beside"], "not above it, where there is room");
  // The tick stays over the target's left end when the note starts left of it.
  assert.deepEqual([kept.style.left, kept.style.top, kept.style["--tick"]], [`${1000 - 6 - 8 * 27}px`, "126px", `${900 + 16 - (1000 - 6 - 8 * 27) - 4}px`]);
  assert.equal(under.style["--tick"], "12px", "a long target's tick aims 16px into it");
  assert.equal(edge.style["--tick"], "2px", "a note beside its target is ticked down its side, within its own height");
  assert.deepEqual([under.style.left, under.style.top, under.dataset.side], ["100px", "126px", "below"]);
  assert.deepEqual([above.style.left, above.style.top, above.dataset.side], ["100px", "564px", "above"]);
  assert.deepEqual([edge.style.left, edge.style.top, edge.dataset.side], ["310px", "290px", "beside"]);
  assert.deepEqual([left.style.left, left.style.top, left.dataset.side], [`${990 - 6 - 8 * 42}px`, "290px", "before"]);
});

test("notes stay inside the window, off their targets and the explained buttons, and off each other", () => {
  const f = fixture();
  // Crowded corners and a row of neighbours whose notes would land on one another.
  const targets = [box(100, 100, 50, 20), box(160, 100, 50, 20), box(220, 100, 50, 20), box(700, 570, 90, 24), box(800, 570, 90, 24), box(900, 570, 90, 24), box(950, 10, 40, 20), box(4, 300, 20, 280), box(0, 4, 1000, 16), box(0, 22, 1000, 16), box(0, 40, 1000, 16)];
  targets.forEach((rect, i) => f.element(`kind-${i}`, `the note of target number ${i}`, rect));
  f.help.enter("pointer");
  f.flush();
  const placed = f.boxes();
  assert.equal(placed.length, targets.length);
  placed.forEach((note, i) => {
    assert.ok(note.left >= 0 && note.top >= 0 && note.right <= 1000 && note.bottom <= 600, `note ${i} leaves the window: ${JSON.stringify(note)}`);
    assert.ok(!overlaps(note, targets[i]), `note ${i} covers its target`);
    targets.forEach((target, j) => assert.ok(target.height > 48 || target.width > 48 || !overlaps(note, target), `note ${i} covers the button ${j}`));
    placed.forEach((other, j) => assert.ok(i === j || !overlaps(note, other), `notes ${i} and ${j} overlap`));
  });
});

// Where a note's tick ends: on the edge of its control, a gap away from the note's side that faces it.
const tip = (node, rect) => {
  const along = parseFloat(node.style["--tick"]) + 4;
  return { below: [rect.left + along, rect.top - 6], above: [rect.left + along, rect.bottom + 6], beside: [rect.left - 6, rect.top + along], before: [rect.right + 6, rect.top + along] }[node.dataset.side];
};
const holds = (rect, [x, y]) => x >= rect.left && x <= rect.right && y >= rect.top && y <= rect.bottom;

test("in a row of adjacent icons every note touches its own icon, ticked at its middle", () => {
  const f = fixture();
  // Four icons at the window's top, 30px apart, with notes 160px long: two fit under the row, one before it and one after it, and only if no note takes another's place.
  const icons = [0, 1, 2, 3].map((i) => box(600 + 30 * i, 4, 24, 24));
  icons.forEach((rect, i) => f.element(`icon-${i}`, `what icon number ${i} does`, rect));
  f.help.enter("pointer");
  f.flush();
  const placed = f.boxes();
  f.notes().forEach((node, i) => {
    assert.ok(node.dataset.side, `note ${i} is not against its icon: ${JSON.stringify(placed[i])}`);
    const [x, y] = tip(node, placed[i]);
    assert.ok(holds(icons[i], [x, y]), `note ${i}'s tick misses its icon`);
    assert.ok(Math.abs(x - (icons[i].left + 12)) < 1e-9 || Math.abs(y - 16) < 1e-9, `note ${i}'s tick is not at its icon's middle`);
    icons.forEach((icon, j) => assert.ok(!overlaps(placed[i], icon), `note ${i} covers icon ${j}`));
    placed.forEach((other, j) => assert.ok(i === j || !overlaps(placed[i], other), `notes ${i} and ${j} overlap`));
  });
});

test("a note slides along its control to keep off what is in its way, its tick still on the control", () => {
  const f = fixture();
  // A name with an open popover over the start of the row under it, and the window's top above it: its note goes under it, starting past the popover.
  const name = box(100, 8, 200, 20);
  f.element("name", "a note 21 chars long", name);
  f.opens(box(90, 34, 24, 24));
  f.help.enter("pointer");
  f.flush();
  const [note] = f.notes();
  assert.deepEqual([note.style.left, note.style.top, note.dataset.side], ["120px", "34px", "below"]);
  assert.ok(holds(name, tip(note, f.boxes()[0])));
});

test("notes keep off every explained button, the later ones of a family too", () => {
  const f = fixture();
  // Four rows' buttons down from the window's top, shut in by popovers on both sides as far down as the third: the only spot against one of the three that may carry the note is under the third, on the fourth.
  const dots = [0, 1, 2, 3].map((i) => box(200, 4 + 28 * i, 26, 26));
  for (const rect of dots) f.element("run-overflow-trigger", "More actions", rect);
  f.opens(box(0, 0, 194, 90), box(232, 0, 768, 90));
  f.help.enter("pointer");
  f.flush();
  assert.deepEqual(f.texts(), ["More actions"]);
  for (const rect of dots) assert.ok(!overlaps(f.boxes()[0], rect), `the note covers a row's button: ${JSON.stringify(f.boxes()[0])}`);
});

test("a note keeps off ?", () => {
  const f = fixture();
  // A control under ?, with a popover under it: the note's place over it would lie on ?.
  f.element("a", "ten chars.", box(940, 32, 50, 20));
  f.opens(box(0, 54, 1000, 40));
  f.help.enter("pointer");
  f.flush();
  assert.ok(!overlaps(f.boxes()[0], box(960, 4, 20, 20)), `the note covers ?: ${JSON.stringify(f.boxes()[0])}`);
});

test("a family's note goes to a later member when the first has no room", () => {
  const f = fixture();
  // The first handle is hemmed in by open popovers on all four sides.
  f.opens(box(0, 70, 400, 24), box(0, 122, 400, 24), box(60, 94, 34, 28), box(122, 94, 278, 28));
  f.element("section-drag-handle", "Drag to reorder", box(100, 100, 16, 16));
  const second = box(100, 300, 16, 16);
  f.element("section-drag-handle", "Drag to reorder", second);
  f.help.enter("pointer");
  f.flush();
  assert.deepEqual(f.texts(), ["Drag to reorder"]);
  assert.equal(f.notes()[0].dataset.side, "below");
  assert.ok(holds(second, tip(f.notes()[0], f.boxes()[0])), "the tick is on the second handle");
});

test("no note moves when the pointer brings out a chart's actions", () => {
  const f = fixture();
  // Three hidden actions at the window's top, and a title under them whose cheapest free spot, over it, is the only one the middle action's note will have: the title's note leaves it free while the action is hidden, and so has no cause to move when it shows.
  const actions = [470, 500, 530].map((left) => f.element("rect-action", "Maximize", box(left, 4, 24, 24)));
  for (const action of actions) action.shown = false;
  f.element("rect-title", "Double-click to rename", box(440, 60, 200, 20));
  f.opens(box(0, 82, 1000, 30));
  f.help.enter("pointer");
  f.flush();
  assert.deepEqual(f.texts(), ["Double-click to rename"]);
  const before = f.boxes()[0];
  for (const action of actions) action.shown = true;
  f.fire("mouseover", { buttons: 0, target: { closest: () => null } });
  f.flush();
  assert.deepEqual(f.texts(), ["Maximize", "Double-click to rename"]);
  assert.deepEqual(f.boxes()[1], before, "the title's note moved");
  f.notes().forEach((node, i) => assert.ok(node.dataset.side && node.dataset.far === undefined, `note ${i} is not against its control`));
});

test("the same page is arranged the same way on every scan, and past the work bound every note is in its corner, in the window", () => {
  const f = fixture();
  // 400 icons edge to edge, each with a note of its own: far more than there is room for.
  for (let i = 0; i < 400; i++) f.element(`icon-${i}`, `the note of icon number ${i}`, box(100 + 24 * (i % 25), 100 + 24 * Math.floor(i / 25), 24, 24));
  f.help.enter("pointer");
  f.flush();
  const first = f.boxes();
  assert.equal(first.length, 400);
  // Unbounded, the icons at the block's edges would have spots (and the scan would take fifty times as long).
  for (const node of f.notes()) assert.equal(node.dataset.side, undefined);
  for (const note of first) assert.ok(note.left >= 0 && note.top >= 0 && note.right <= 1000 && note.bottom <= 600, `a note leaves the window: ${JSON.stringify(note)}`);
  f.fire("resize");
  f.flush();
  assert.deepEqual(f.boxes(), first);
});

test("a note with no room against its control sits a row off, tied to it by a line", () => {
  const f = fixture();
  f.element("icon", "an icon's note", box(500, 300, 20, 20));
  // Hidden buttons all round it, with a slit under its middle and none over it.
  const hide = (rect) => (f.element("rect-action", "Maximize", rect).shown = false);
  for (const top of [274, 326]) for (const left of [412, 460, 512, 560]) hide(box(left, top, 48, 20));
  hide(box(526, 296, 40, 28));
  hide(box(454, 296, 40, 28));
  f.help.enter("pointer");
  f.flush();
  const [note] = f.notes();
  assert.deepEqual([note.dataset.side, note.style.top, note.dataset.far, note.style["--reach"]], ["below", "352px", "", "32px"], "under the row of hidden buttons, its line through the slit");
  // With the slit under it shut, the line would cross a hidden button there: the note goes over the icon, where the other slit is.
  hide(box(506, 326, 8, 20));
  f.fire("resize");
  f.flush();
  assert.deepEqual([f.notes()[0].dataset.side, f.notes()[0].dataset.far], ["above", ""]);
});

test("a family's hidden member carries no note", () => {
  const f = fixture();
  // Two charts' resize corners, which are not buttons. The first is shown, in the window's corner with buttons under and beside it, so it has no spot against it; the second is transparent, in open space.
  f.element("rect-resize-handle", "Drag to resize the chart", box(0, 0, 12, 12), { button: false });
  for (const left of [0, 44, 88, 132, 176]) f.element(`under-${left}`, "u", box(left, 18, 40, 24));
  f.element("right", "r", box(18, 0, 40, 14));
  f.element("rect-resize-handle", "Drag to resize the chart", box(600, 300, 12, 12), { button: false }).shown = false;
  f.help.enter("pointer");
  f.flush();
  const note = f.boxes()[f.texts().indexOf("Drag to resize the chart")];
  assert.ok(!overlaps(note, box(400, 250, 500, 120)), `the note went to the hidden corner: ${JSON.stringify(note)}`);
});

test("a family with a member not rendered at all is not one the pointer brings out", () => {
  const f = fixture();
  // Two icons side by side at the window's top, whose notes want the same place: the one with fewer spots chooses first, unless it is taken for a control the pointer brings out.
  f.element("run-overflow-trigger", "what the first icon does", box(300, 4, 24, 24));
  f.element("other", "what the other icon does", box(330, 4, 24, 24));
  f.opens(box(0, 0, 294, 30));
  // A closed menu's item, or a run row far down the list.
  const far = f.element("run-overflow-trigger", "what the first icon does", box(0, 0, 0, 0));
  far.shown = false;
  far.gone = true;
  f.help.enter("pointer");
  f.flush();
  assert.deepEqual([f.notes()[0].style.left, f.notes()[0].dataset.side], ["300px", "below"], "the first icon's note chose after the other's");
});

test("a page's only chart: its actions coming out move no note either", () => {
  const f = fixture();
  f.element("rect-title-pop", "Double-click to rename", box(270, 82, 142, 20));
  const actions = ["Maximize", "Chart settings", "Remove chart; keep logged data"].map((text, i) => f.element("rect-action", text, box(416 + 30 * i, 80, 24, 24)));
  for (const action of actions) action.shown = false;
  f.help.enter("pointer");
  f.flush();
  const before = f.boxes()[0];
  // No other chart's actions stay hidden to say what these are: they were seen hidden while the notes were up.
  for (const action of actions) action.shown = true;
  f.fire("mouseover", { buttons: 0, target: { closest: () => null } });
  f.flush();
  assert.equal(f.texts().length, 4);
  assert.deepEqual(f.boxes()[0], before, "the title's note moved");
});

test("Escape during a held press leaves the pin, and the pointer leaving the window rescans", () => {
  const f = fixture();
  const action = f.element("rect-action", "Maximize", box(300, 100, 20, 20));
  f.help.toggle();
  f.flush();
  f.fire("mousedown", { target: { closest: () => null }, buttons: 1 });
  f.fire("keydown", { key: "Escape", preventDefault() {} });
  assert.equal(f.attributes.get("aria-pressed"), "true");
  f.fire("pointerup", { target: { closest: () => null }, buttons: 0 });
  f.flush();
  assert.deepEqual(f.texts(), ["Maximize"]);
  // The chart's action goes when the pointer leaves the window, and no crossing onto another element says so.
  action.shown = false;
  f.fire("mouseout", { relatedTarget: {} });
  f.flush();
  assert.deepEqual(f.texts(), ["Maximize"], "a crossing inside the window is the mouseover's business");
  f.fire("mouseout", { relatedTarget: null });
  f.flush();
  assert.deepEqual(f.texts(), []);
});

test("a target with no room around it takes its note inside", () => {
  const f = fixture();
  f.element("chart-container", "Drag horizontally to zoom", box(0, 0, 1000, 600));
  f.help.enter("pointer");
  f.flush();
  assert.deepEqual([f.notes()[0].style.left, f.notes()[0].style.top], ["6px", "6px"]);
});

test("a note for a control inside an open popover is that popover's child, and page notes keep clear of the popover", () => {
  const f = fixture();
  const menu = f.host("menu");
  f.opens(box(40, 40, 200, 100));
  f.element(null, "Recoverable for 7 days, then deletes permanently", box(50, 110, 80, 20), { host: menu, tagName: "BUTTON" });
  f.element("sidebar-meta", "Shown / total runs", box(100, 10, 30, 20));
  // A page note under the menu's own note: the two are in different layers and never meet.
  f.element("run-name-host", "Edits apply immediately", box(50, 130, 100, 10));
  f.help.enter("pointer");
  f.flush();
  assert.deepEqual(f.notes().map((n) => n.parent), ["menu", "body", "body"]);
  const [inMenu, counter, name] = f.boxes();
  assert.deepEqual([inMenu.left, inMenu.top, f.notes()[0].dataset.side], [50, 84, "above"], "the menu's note chooses first, and leaves the page's the room under its control");
  assert.ok(!overlaps(counter, box(40, 40, 200, 100)), "a page note under the menu would be cut off");
  assert.ok(!overlaps(name, inMenu) && !overlaps(name, box(40, 40, 200, 100)), "the menu's note paints over the page's, which keeps clear of it");
});

test("a press pins and unpins; an unpin hides the notes until the pointer or focus next enters ?", () => {
  const f = fixture();
  f.element("a", "text", box(10, 10, 20, 20));
  f.help.enter("pointer");
  f.help.toggle();
  f.flush();
  assert.equal(f.attributes.get("aria-pressed"), "true");
  // A second holder changes nothing that is already in place.
  f.help.enter("focus");
  f.help.leave("focus");
  assert.equal(f.listeners.length, 11);
  f.help.leave("pointer");
  assert.deepEqual(f.texts(), ["text"], "a pin outlives the pointer");
  f.help.enter("pointer");
  f.help.toggle();
  assert.equal(f.attributes.get("aria-pressed"), "false");
  assert.equal(f.listeners.length, 0);
  assert.equal(f.observing(), null);
  f.flush();
  assert.deepEqual(f.texts(), [], "the pointer is still on ?");
  f.help.enter("focus");
  f.flush();
  assert.deepEqual(f.texts(), ["text"], "focus entering ? is a new entry");
  f.help.toggle();
  f.help.toggle();
  f.help.leave("focus");
  f.flush();
  assert.deepEqual(f.texts(), [], "one of the two leaving is not an entry");
  f.help.leave("pointer");
  f.help.enter("pointer");
  f.flush();
  assert.deepEqual(f.texts(), ["text"]);
});

test("pinned help takes Escape last", () => {
  const f = fixture();
  f.help.enter("pointer");
  const escape = (extra = {}) => {
    const event = { key: "Escape", prevented: false, preventDefault() { this.prevented = true; }, ...extra };
    f.fire("keydown", event);
    return event.prevented;
  };
  assert.equal(escape(), false, "a preview is not Escape's to close");
  f.help.toggle();
  assert.equal(escape({ defaultPrevented: true }), false, "an in-app layer consumed it");
  assert.equal(escape({ isComposing: true }), false);
  assert.equal(escape({ key: "a" }), false);
  f.covers({});
  assert.equal(escape(), false, "a popover, dialog, Trash selection or maximized chart takes it");
  f.covers(null);
  f.window.__kymo_zg = {};
  assert.equal(escape(), false, "a chart gesture takes it");
  f.window.__kymo_zg = null;
  assert.equal(f.attributes.get("aria-pressed"), "true");
  assert.equal(escape(), true);
  assert.equal(f.attributes.get("aria-pressed"), "false");
  assert.equal(f.listeners.length, 0, "although the pointer is still on ?");
});

test("a held press hides the notes until its release, whatever else happens meanwhile", () => {
  const f = fixture();
  f.element("a", "text", box(10, 10, 20, 20));
  // Not only while pinned: a preview that focus on ? holds open hides for a press too.
  f.help.enter("focus");
  f.flush();
  const elsewhere = { closest: () => null };
  f.fire("mousedown", { target: elsewhere, buttons: 1 });
  assert.deepEqual(f.texts(), []);
  // The press moved focus, the drag scrolled a list, the app rendered, the pointer crossed elements.
  f.fire("focusin", { target: { closest: () => null, querySelectorAll: () => [] } });
  f.fire("scroll");
  f.fire("resize");
  f.render([{ type: "attributes", addedNodes: [], removedNodes: [] }]);
  f.fire("mouseover", { target: elsewhere, buttons: 1 });
  f.flush();
  assert.deepEqual(f.texts(), [], "no note under a drag");
  assert.deepEqual([f.root.has("data-kymo-help"), f.root.has("data-kymo-held")], [true, true], "the readout's labels go with the notes, by the held mark");
  f.fire("pointerup", { target: elsewhere, buttons: 0 });
  f.flush();
  assert.deepEqual(f.texts(), ["text"]);
  assert.deepEqual([f.root.has("data-kymo-help"), f.root.has("data-kymo-held")], [true, false]);
  // A button that disabled itself on its press, a native drag and a context menu end a press with no mouseup.
  for (const release of ["dragend", "contextmenu"]) {
    f.fire("mousedown", { target: elsewhere, buttons: 1 });
    f.fire(release, { target: elsewhere, buttons: 2 });
    f.flush();
    assert.deepEqual(f.texts(), ["text"], release);
  }
  // A release outside the window: the pointer comes back with no button down.
  f.fire("mousedown", { target: elsewhere, buttons: 1 });
  f.fire("mouseover", { target: elsewhere, buttons: 0 });
  f.flush();
  assert.deepEqual(f.texts(), ["text"]);
  // A drag that began outside the window, or on ?, is a held button all the same once it crosses the page.
  f.fire("mouseover", { target: elsewhere, buttons: 1 });
  f.flush();
  assert.deepEqual(f.texts(), []);
  f.fire("pointerup", { target: elsewhere, buttons: 0 });
  f.flush();
  assert.deepEqual(f.texts(), ["text"]);
  // A press that outlives the preview is forgotten with it.
  f.fire("mousedown", { target: elsewhere, buttons: 1 });
  f.help.leave("focus");
  assert.ok(!f.root.has("data-kymo-held"));
  f.help.enter("pointer");
  f.flush();
  assert.deepEqual(f.texts(), ["text"]);
});

test("a press on ? and a keyboard activation hide nothing", () => {
  const f = fixture();
  f.element("a", "text", box(10, 10, 20, 20));
  f.help.toggle();
  f.flush();
  f.fire("mousedown", { target: { closest: (selector) => (selector === "#kymo-help" ? {} : null) }, buttons: 1 });
  assert.deepEqual(f.texts(), ["text"]);
  // main.rs replays Enter and Space on a button as a mousedown with no button held, and no mouseup follows: what it changed shows at once.
  f.element("b", "more", box(10, 100, 20, 20));
  f.fire("mousedown", { target: { closest: () => null }, buttons: 0 });
  f.flush();
  assert.deepEqual(f.texts(), ["text", "more"]);
});

test("the pointer going from run row to run row scans nothing", () => {
  const f = fixture();
  const first = f.element("a", "first", box(10, 10, 20, 20));
  f.help.toggle();
  f.flush();
  // What a scan would show, were there one.
  first.shown = false;
  const within = (row) => ({ closest: (selector) => (selector === ".sidebar-run" ? row : null) });
  f.fire("mouseover", { target: within({}), relatedTarget: within({}), buttons: 0 });
  f.flush();
  assert.deepEqual(f.texts(), ["first"]);
  // Onto a row from elsewhere, something the pointer had brought out may have gone.
  f.fire("mouseover", { target: within({}), relatedTarget: within(null), buttons: 0 });
  f.flush();
  assert.deepEqual(f.texts(), []);
  // A release over the rows still ends a press.
  first.shown = true;
  f.fire("mouseover", { target: within({}), relatedTarget: within({}), buttons: 1 });
  f.fire("mouseover", { target: within({}), relatedTarget: within({}), buttons: 0 });
  f.flush();
  assert.deepEqual(f.texts(), ["first"]);
});

test("a render refreshes the notes; the notes' own comings and goings do not", () => {
  const f = fixture();
  const first = f.element("a", "first", box(10, 10, 20, 20));
  f.help.enter("pointer");
  f.flush();
  first.shown = false;
  f.element("b", "second", box(10, 100, 20, 20));
  const node = (className, explained) => ({ nodeType: 1, className, matches: () => explained === "itself", querySelector: () => (explained === "inside" ? {} : null) });
  const note = node("kymo-help-note", false);
  f.render([{ type: "childList", addedNodes: [note], removedNodes: [note] }]);
  f.flush();
  assert.deepEqual(f.texts(), ["first"], "a scan's own writes must not start another");
  // A chart's loading spinner and a text node: nothing explained came or went.
  f.render([{ type: "childList", addedNodes: [node("rect-loading-indicator", false), { nodeType: 8 }], removedNodes: [{ nodeType: 3 }] }]);
  f.flush();
  assert.deepEqual(f.texts(), ["first"], "a live reply must not start a scan");
  f.render([{ type: "childList", addedNodes: [note], removedNodes: [] }, { type: "childList", addedNodes: [], removedNodes: [node("sidebar-run", "inside")] }]);
  f.flush();
  assert.deepEqual(f.texts(), ["second"]);
  first.shown = true;
  f.render([{ type: "attributes", addedNodes: [], removedNodes: [] }]);
  f.flush();
  assert.deepEqual(f.texts(), ["first", "second"]);
  first.shown = false;
  f.render([{ type: "childList", addedNodes: [node("run-marker-toggle", "itself")], removedNodes: [] }]);
  f.flush();
  assert.deepEqual(f.texts(), ["second"]);
  // What a render adds can count as not rendered for one frame more (a run row, content-visibility: auto): the scan it starts is followed by another, with no event between.
  first.shown = true;
  f.flush();
  assert.deepEqual(f.texts(), ["first", "second"], "no second scan followed the render's");
  first.shown = false;
  f.flush();
  assert.deepEqual(f.texts(), ["first", "second"], "and only one");
});

test("a text shown only within focus comes and goes with it", () => {
  const f = fixture();
  const filter = f.element("sidebar-filter", "Matches names, IDs, #numbers", box(10, 10, 100, 20), { dataset: { helpWithin: ":focus" } });
  f.help.toggle();
  f.flush();
  assert.deepEqual(f.texts(), []);
  filter.inside.push(":focus");
  f.fire("focusin", { target: filter });
  f.flush();
  assert.deepEqual(f.texts(), ["Matches names, IDs, #numbers"]);
  filter.inside.pop();
  f.fire("focusout");
  f.flush();
  assert.deepEqual(f.texts(), []);
});

test("scrolls and resizes move the notes with their targets", () => {
  const f = fixture();
  const rect = box(10, 10, 20, 20);
  f.element("a", "text", rect);
  f.help.enter("pointer");
  f.flush();
  for (const [type, top] of [["scroll", 200], ["resize", 300]]) {
    Object.assign(rect, box(10, top, 20, 20));
    f.fire(type);
    f.flush();
    assert.equal(f.notes()[0].style.top, `${top + 26}px`);
  }
  // A scroll of something that holds no explained control (a log following its tail) moves no note, and scans nothing.
  Object.assign(rect, box(10, 400, 20, 20));
  f.fire("scroll", { target: { querySelector: () => null } });
  f.flush();
  assert.equal(f.notes()[0].style.top, "326px");
  f.fire("scroll", { target: { querySelector: () => ({}) } });
  f.flush();
  assert.equal(f.notes()[0].style.top, "426px");
});

test("while pinned, hover shows controls that appear only then, and focus speaks the focused control's texts", () => {
  const f = fixture();
  const action = f.element("rect-action", "Remove chart; keep logged data", box(300, 100, 20, 20));
  action.shown = false;
  assert.equal(f.spoken(), undefined, "no live region until help is pinned");
  f.help.toggle();
  f.flush();
  assert.equal(f.spoken().attributes.get("aria-live"), "polite", "the live region is in the page before the first focus");
  assert.deepEqual(f.texts(), []);
  action.shown = true;
  f.fire("mouseover", { target: action, buttons: 0 });
  f.flush();
  assert.deepEqual(f.texts(), ["Remove chart; keep logged data"]);
  f.fire("focusin", { target: action });
  assert.equal(f.spoken().textContent, "Remove chart; keep logged data");
  // A run's link carries no text of its own; its name inside does.
  const name = f.element("run-name-host", "Edits apply immediately\n#N: run number, shown when names repeat", box(40, 300, 100, 20));
  const link = f.element("run-details", undefined, box(30, 300, 120, 20), { children: [name], listed: false });
  f.fire("focusin", { target: link });
  assert.equal(f.spoken().textContent, "Edits apply immediately #N: run number, shown when names repeat");
  f.fire("focusin", { target: f.element("plain", undefined, box(0, 0, 1, 1), { listed: false }) });
  assert.equal(f.spoken().textContent, "");
  // A focused container (a settings panel's body) does not read out every hint inside it.
  f.fire("focusin", { target: f.element("panel-body", undefined, box(30, 300, 120, 20), { children: [name], listed: false, control: false }) });
  assert.equal(f.spoken().textContent, "");
  f.help.toggle();
  assert.equal(f.spoken(), undefined);
});

test("leaving the dashboard ends a pin", () => {
  const f = fixture();
  f.element("a", "text", box(10, 10, 20, 20));
  f.help.enter("pointer");
  f.help.toggle();
  f.flush();
  f.help.end();
  assert.equal(f.listeners.length, 0);
  assert.equal(f.observing(), null);
  assert.deepEqual(f.texts(), []);
  assert.equal(f.attributes.get("aria-pressed"), "false");
  f.help.enter("focus");
  f.help.leave("focus");
  f.flush();
  assert.deepEqual(f.texts(), [], "the pointer that was on ? holds nothing open any more");
  f.help.enter("pointer");
  f.flush();
  assert.deepEqual(f.texts(), ["text"], "the next dashboard starts unpinned");
});

test("a chart that cannot say its texts keeps its rendered ones, and the page keeps its help", () => {
  const f = fixture();
  f.window.__kymo_charts = { "uplot-1": {} };
  f.element("chart-container", "Drag horizontally to zoom", box(300, 100, 400, 200), { id: "uplot-1", dataset: { helpFn: "chart" } });
  f.element("a", "text", box(10, 10, 20, 20));
  f.help.enter("pointer");
  f.flush();
  assert.deepEqual(f.texts(), ["Drag horizontally to zoom", "text"]);
});

// A uPlot instance as create.js leaves it, with what the chart provider reads.
function chart(over = {}) {
  const axis = { dataset: {} };
  return { root: { axis, querySelector: (selector) => (assert.equal(selector, ".u-axis"), axis) }, data: [over.xs ?? [0, 1, 2]], scales: { x: { max: over.max === undefined ? 2 : over.max } }, __kymo_userzoom: over.userzoom, __kymo_tip: { style: { labels: {}, getPropertyValue(name) { return this.labels[name]; }, setProperty(name, value) { this.labels[name] = value; } } } };
}

test("a chart's texts follow its state", () => {
  const f = fixture();
  const lines = (id, dataset = {}) => [...f.help.providers.chart({ id, dataset })];
  const FOLLOW = "Include right edge to follow new data";
  f.window.__kymo_charts = { plain: chart(), zoomed: chart({ userzoom: true }), following: chart({ userzoom: "follow" }), smoothed: chart(), fresh: chart({ max: null }), empty: chart({ xs: [] }) };
  assert.deepEqual(lines("missing"), [], "an unmounted chart has none");
  assert.deepEqual(lines("plain"), [], "a chart with no live run has none");
  assert.equal(f.window.__kymo_charts.plain.root.axis.dataset.help, "Drag axis to rescale", "the X-axis strip takes its own text");
  assert.deepEqual(lines("plain", { live: "true" }), [FOLLOW]);
  assert.deepEqual(lines("zoomed", { live: "true" }), [], "no follow hint on a zoomed chart");
  assert.deepEqual(lines("following", { live: "true" }), [], "a zoom that already follows needs no hint");
  assert.deepEqual(lines("fresh", { live: "true" }), [], "no window yet, right after creation");
  assert.deepEqual(lines("empty", { live: "true" }), []);
  assert.deepEqual(lines("plain", { live: "true", stepZoomed: "true" }), [], "nor a zoomed step answer");
  assert.deepEqual(lines("smoothed", { smoothed: "true", live: "true" }), [FOLLOW], "smoothing adds no chart text");
  // The readout's own labels are left on the readout, for kymo.css to draw on it while notes show.
  const labels = (id) => ({ ...f.window.__kymo_charts[id].__kymo_tip.style.labels });
  assert.deepEqual(labels("plain"), { "--help-colored": "", "--help-raw": '"Raw value"', "--help-range": '"Range"', "--help-borrowed": '"@: nearest sample’s X"', "--help-copy": '"⌘C copies values"' }, "an unsmoothed chart's colored values are not smoothed");
  assert.deepEqual(labels("smoothed"), { ...labels("plain"), "--help-colored": '"Smoothed"' });
  f.platform("Win32");
  assert.deepEqual(lines("plain"), []);
  assert.equal(labels("plain")["--help-copy"], '"Ctrl+C copies values"');
});

test("a chart container shows its rendered lines and its script's together", () => {
  const f = fixture();
  f.window.__kymo_charts = { "uplot-1": chart() };
  f.element("chart-container", "Drag horizontally to zoom", box(300, 100, 400, 200), { id: "uplot-1", dataset: { helpFn: "chart", live: "true" } });
  f.element("chart-container", "Drag horizontally to zoom", box(300, 320, 400, 200), { id: "uplot-2", dataset: { helpFn: "chart" } });
  f.help.enter("pointer");
  f.flush();
  assert.deepEqual(f.texts(), ["Drag horizontally to zoom\nInclude right edge to follow new data"]);
});

// The release bundle's CSS minifier (lightningcss, in `dx bundle`) splits off a selector it must rewrite for older browsers (one with a functional pseudo-class such as `:is()`), and when the rest of the selector list is the rule just before, that selector loses the declarations. The readout labels lost their look that way, in release builds only.
test("no rule repeats the rule before it and adds a selector the release minifier splits off", () => {
  const css = read("../assets/kymo.css").replace(/\/\*[\s\S]*?\*\//g, "");
  const list = (selectors) => {
    const out = [];
    let depth = 0, from = 0;
    for (let at = 0; at <= selectors.length; at++) {
      const c = selectors[at];
      if (c === "(" || c === "[") depth++;
      else if (c === ")" || c === "]") depth--;
      else if (c === undefined || (c === "," && !depth)) { out.push(selectors.slice(from, at).trim().replace(/\s+/g, " ")); from = at + 1; }
    }
    return out;
  };
  let before = [];
  for (const [, head] of css.matchAll(/([^{}]+)\{[^{}]*\}/g)) {
    const selectors = list(head);
    assert.ok(!(before.length && before.every((selector) => selectors.includes(selector)) && selectors.some((selector) => !before.includes(selector) && /:[a-z-]+\(/.test(selector))), `"${selectors.join(", ")}": give the selector with a functional pseudo-class its own rule`);
    before = selectors;
  }
});

test("components take their help texts from help.rs", () => {
  // A text written where it is used would escape the word limit and the design document's check. Only the format strings that join constants may be literals.
  const src = new URL("../src/", import.meta.url);
  for (const name of readdirSync(src, { recursive: true }).filter((name) => name.endsWith(".rs") && name !== "help.rs")) {
    for (const [at, line] of readFileSync(new URL(name, src), "utf8").split("\n").entries()) {
      const value = /"data-help(?:-fn)?":(.*)|\bhelp: (.*)/.exec(line);
      const literals = (value?.[1] ?? value?.[2] ?? "").match(/"[^"]*"/g) ?? [];
      for (const literal of literals) assert.ok(/^"(?:[{}]|\\n)*"$/.test(literal) || literal === '"chart"', `${name}:${at + 1} writes a help text in place: ${literal}`);
    }
  }
});

// Every text shown: the constants of help.rs and the chart script's TEXT table.
const consts = [...read("../src/help.rs").matchAll(/^pub const \w+: &str = "(.*)";$/gm)].map((match) => match[1]);
const chartTexts = [...read("../src/components/uplot_chart/help.js").split("const TEXT = {")[1].split("};")[0].matchAll(/: '(.*)',$/gm)].map((match) => match[1]);
const coded = new Set([...consts, ...chartTexts]);

test("every text is at most eight words and has no closing period", () => {
  assert.ok(consts.length > 40 && chartTexts.length > 5, `found only ${consts.length} and ${chartTexts.length} texts`);
  assert.equal(read("../src/help.rs").split("\n").filter((line) => line.startsWith("pub const ")).length, consts.length, "a constant is not on one line");
  for (const text of coded) {
    assert.ok(text.split(/\s+/).length <= 8, `${JSON.stringify(text)} is over 8 words`);
    assert.ok(!text.endsWith("."), `${JSON.stringify(text)} ends with a period`);
  }
});

// The design document lists every text.
const design = new URL("../../docs/contextual-help.md", import.meta.url);
if (existsSync(design)) {
  test("the code's texts are the design document's Explain and Conditional rows, one for one", () => {
    // A cell is one text, or a row's state variants separated by " / ".
    const cells = readFileSync(design, "utf8").split("\n").map((line) => line.split(" | ")).filter((row) => row.length === 5 && ["Explain", "Conditional"].includes(row[1])).map((row) => row[2]);
    const listed = new Set(cells.flatMap((cell) => [cell, ...cell.split(" / ")]));
    assert.ok(cells.length > 50, `found only ${cells.length} rows`);
    for (const text of coded) assert.ok(listed.has(text), `${JSON.stringify(text)} is not a displayed explanation in docs/contextual-help.md`);
    for (const cell of cells) assert.ok(coded.has(cell) || cell.split(" / ").every((text) => coded.has(text)), `the design's ${JSON.stringify(cell)} has no text in the code`);
  });
}

console.log(`contextual help: ${passed} checks passed`);
