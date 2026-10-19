#!/usr/bin/env node

// Runs the production chart-create script (create.js, with hover.js and zoom_math.js embedded as build_create_js embeds them) against a stand-in uPlot and DOM, to check gestures that span charts.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const read = name => readFileSync(new URL(`../src/components/uplot_chart/${name}`, import.meta.url), "utf8");
const template = read("create.js").replace(/\n$/, "");
const hover = read("hover.js");
const zoomMath = read("zoom_math.js").replace(/\n$/, "");
const hoverPoints = read("hover_points.js");

let passed = 0;
function test(name, run) {
  run();
  console.log(`ok ${++passed} - ${name}`);
}

/** A chart's x axis, as ShownAxis and the series it plots: one run, one values line. */
function render(id, { axis = "rel", logX = false, shifted = false, logY = false, labels = ["run"], hash = "cfg" } = {}) {
  const step = axis === "step";
  const time = axis === "rel" || axis === "wall";
  const tokens = {
    __KYMO_HOVER__: hover,
    __KYMO_ZOOM_MATH__: zoomMath,
    __KYMO_ID__: id,
    __KYMO_AXIS_FONT_SIZE__: "12",
    __KYMO_X_AXIS_SIZE__: "23",
    __KYMO_Y_AXIS_MIN_SIZE__: "35",
    __KYMO_RAW_STRIDE__: "0",
    __KYMO_HAS_RAW__: "false",
    __KYMO_HAS_RANGE__: "false",
    __KYMO_SMOOTHED__: "false",
    __KYMO_HAS_XRANGE__: "false",
    __KYMO_NAN_MARKERS__: "0",
    __KYMO_ZOOM_REFETCH__: String(step),
    // A zero-present log x plots x+1 in the wire's units (ChartJsConfig::log_shift): the step zoom geometry shifts by 1, readouts by 1 step or 1 ms.
    __KYMO_READOUT_X_SHIFT__: !shifted ? "0" : time ? "0.001" : "1",
    __KYMO_X_DISTR__: logX ? "3" : "1",
    __KYMO_Y_DISTR__: logY ? "3" : "1",
    __KYMO_SERIES_LIST__: "{}," + labels.map(label => `{'label':'${label}','stroke':'#123456','width':1.5}`).join(","),
    __KYMO_LABELS__: labels.map(label => `'${label}'`).join(","),
    __KYMO_COLORS__: labels.map(() => "'#123456'").join(","),
    __KYMO_RUN_IDS__: labels.map(label => `'${label}'`).join(","),
    __KYMO_RUN_NAMES__: labels.map(() => "null").join(","),
    __KYMO_XNAN_COUNTS__: labels.map(() => "0").join(","),
    __KYMO_BANDS_LIST__: "",
    __KYMO_CFG_HASH__: hash,
    __KYMO_IS_TIME__: String(axis === "rel" || axis === "wall"),
    __KYMO_IS_WALL__: String(axis === "wall"),
    __KYMO_X_LABEL__: step ? "step" : "time",
    __KYMO_SYNC_KEY__: axis,
    __KYMO_SYNC_SCALE__: step ? "{syncX:{from:'x',range:(u,mn,mx)=>[mn,mx]}}" : "{}",
    __KYMO_SYNC_SCALE_KEY__: step ? "syncX" : "x",
  };
  const js = template.replace(/__KYMO_[A-Z_]+__/g, token => {
    assert.ok(token in tokens, `the harness knows create.js's ${token}`);
    return tokens[token];
  });
  assert.ok(!js.includes("__KYMO_"));
  return js;
}

function element(id = "") {
  const listeners = {};
  return {
    id, style: {}, className: "", innerHTML: "", offsetWidth: 100, offsetHeight: 40, listeners, dispatched: [],
    classList: { toggle() {}, add() {}, remove() {} },
    addEventListener(type, fn) { (listeners[type] ||= []).push(fn); },
    dispatchEvent(event) { this.dispatched.push(event); return true; },
    width: 400,
    getBoundingClientRect() { return { left: 0, top: 0, right: this.width, bottom: 220, width: this.width, height: 220 }; },
    appendChild() {}, closest() { return null; },
    fire(type, event) { for (const fn of listeners[type] || []) fn(event); },
  };
}

/** One page: charts share `window`, as the dashboard's do. */
function page({ dpr = 1 } = {}) {
  const windowListeners = {};
  const nodes = {};
  const created = [];
  class FakeUPlot {
    constructor(opts, data) {
      this.opts = opts;
      this.data = data;
      this.root = { isConnected: true };
      this.over = { getBoundingClientRect: () => ({ left: 0, top: 0, width: 400, height: 200 }), appendChild() {} };
      this.bbox = { left: 0, top: 0, width: 400, height: 200 };
      // 7 px per character.
      // At a device pixel ratio above 1, uPlot scales its fonts, so the canvas measures in device pixels.
      this.ctx = { font: "", measureText: text => ({ width: 7 * text.length * dpr }) };
      this.cursor = { sync: { key: opts.cursor.sync.key }, idx: null, left: 0, top: 0 };
      this.series = opts.series;
      this.axes = opts.axes;
      this.scales = { x: { distr: opts.scales.x.distr }, y: {} };
      this.rebuilds = 0;
      this.width = opts.width;
      this.height = opts.height;
      this.resizes = 0;
      this.fitX();
    }
    fitX() { const x = this.data[0]; this.scales.x.min = x[0]; this.scales.x.max = x[x.length - 1]; }
    setScale(key, { min, max }) {
      // uPlot runs the scale's range function on every explicit setScale.
      const range = this.opts.scales[key].range;
      if (typeof range === "function") [min, max] = range(this, min, max, key);
      Object.assign(this.scales[key], { min, max });
      this.rebuilds++;
      for (const hook of this.opts.hooks.setScale || []) hook(this, key);
    }
    setData(data) { this.data = data; if (this.opts.scales.x.auto(this)) this.fitX(); this.rebuilds++; }
    setCursor() {}
    setSize({ width, height }) { Object.assign(this, { width, height }); this.resizes++; }
    setSelect(box) { this.box = box; }
    destroy() { this.root.isConnected = false; }
    valToPos(value) { return value; }
  }
  const document = {
    body: element("body"),
    documentElement: { getAttribute: () => null },
    getElementById(id) { return nodes[id] || null; },
    createElement() { const node = element(); created.push(node); return node; },
  };
  const window = {
    __kymo_charts: {},
    __kymo_data: {},
    __kymo_applyHl() {}, __kymo_refreshHl() {}, __kymo_setHl() {}, __kymo_retagRows() {},
    addEventListener(type, fn) { (windowListeners[type] ||= []).push(fn); },
    removeEventListener(type, fn) { windowListeners[type] = (windowListeners[type] || []).filter(f => f !== fn); },
    innerWidth: 1000, innerHeight: 800, scrollX: 0,
  };
  const context = vm.createContext({
    window, document, uPlot: FakeUPlot, devicePixelRatio: dpr, CustomEvent: class { constructor(type, init) { this.type = type; Object.assign(this, init); } },
    ResizeObserver: class { constructor(fire) { this.fire = fire; } observe() {} disconnect() {} }, getComputedStyle: () => ({ height: "220px", getPropertyValue: () => "" }),
    queueMicrotask, dioxus: { close() {} },
  });
  vm.runInContext(hoverPoints, context);
  return {
    window,
    create(id, x, options) {
      nodes[id] ||= element(id);
      window.__kymo_data[id] = [Float64Array.from(x), Float64Array.from(x, v => v / 10)];
      const status = vm.runInContext(`(function(){${render(id, options)}})()`, context);
      assert.equal(status, "", `create ${id}`);
      return window.__kymo_charts[id];
    },
    el(id) { return nodes[id]; },
    /** Lays a container out `width` px wide, and runs its chart's ResizeObserver if one is mounted. */
    resize(id, width) {
      (nodes[id] ||= element(id)).width = width;
      window.__kymo_ro?.[id]?.fire();
    },
    tipOf(id) { return created.find(node => node.id === `${id}-tip`); },
    fireWindow(type, event) { for (const fn of [...(windowListeners[type] || [])]) fn(event); },
  };
}

// A press on the axis strip below the plot (plot 200 px tall inside a 220 px container), then a move and the release or Escape.
function pullAxis(p, id, from, to, end = "mouseup") {
  p.el(id).fire("mousedown", { button: 0, clientX: from, clientY: 210, pageX: from, pageY: 210, preventDefault() {} });
  p.fireWindow("mousemove", { buttons: 1, clientX: to, clientY: 210, pageX: to, pageY: 210 });
  if (end === "mouseup") p.fireWindow("mouseup", { button: 0, clientX: to, clientY: 210, pageX: to, pageY: 210 });
  else p.fireWindow("keydown", { key: "Escape" });
}

// uPlot calls an axis's values with (chart, ticks, axis, found space, found increment); the increment is the tick spacing.
function tickLabels(c, values, incr = values[1] - values[0]) {
  return c.axes[0].values(c, values, 0, 50, incr);
}

// The ticks a linear axis picks for a window on a `dim` px plot and their labels, asked as uPlot asks: incrs, splits, then values on the very list splits returned.
function picked(c, mn, mx, dim = 800) {
  c.setScale("x", { min: mn, max: mx });
  const ax = c.axes[0];
  const [step] = ax.incrs(c, 0, mn, mx, dim);
  const ticks = ax.splits(c, 0, mn, mx, step);
  return { step, ticks: [...ticks], labels: [...tickLabels(c, ticks, step)] };
}

// The same for a log axis's window, on the fake's 400 px.
function logTicks(c, lo, hi) {
  c.setScale("x", { min: lo, max: hi });
  const ticks = c.axes[0].splits(c, 0, lo, hi);
  return { ticks: [...ticks], labels: [...tickLabels(c, ticks, Infinity)] };
}

// Every neighbouring pair of ticks keeps uPlot's 50 px, and the wider label plus 18 px, on the fake's 7 px a character, placed linearly or on a log axis.
function fits({ ticks, labels }, mn, mx, dim = 800, log = false) {
  const at = log ? v => (dim * Math.log10(v / mn)) / Math.log10(mx / mn) : v => (dim * (v - mn)) / (mx - mn);
  for (let i = 1; i < ticks.length; i++) {
    const need = Math.max(50, 7 * Math.max(labels[i - 1].length, labels[i].length) + 18);
    assert.ok(at(ticks[i]) - at(ticks[i - 1]) >= need - 1e-9, `${labels[i - 1]} and ${labels[i]}`);
  }
}

// A reproducible pseudo-random 0..1 sequence, so a fuzz failure repeats.
const random = seed => () => (seed = (seed * 16807) % 2147483647) / 2147483647;

// The source tooltip's x header with the cursor on column `idx` of chart `id`.
function readout(p, id, idx) {
  const c = p.window.__kymo_charts[id];
  p.window.__kymo_hoversrc = id;
  c.cursor.idx = idx;
  c.opts.hooks.setCursor[0](c);
  return /kymo-tip-step">([^<]*)</.exec(p.tipOf(id).innerHTML)?.[1];
}

test("an axis pull on a client-x chart zooms its sync group at release, and only the source while it moves", () => {
  const p = page();
  const xs = [0, 25, 50, 75, 100];
  const source = p.create("a", xs);
  const peer = p.create("b", xs);
  const wall = p.create("c", xs.map(x => 1.76e9 + x), { axis: "wall" });
  p.el("a").fire("mousedown", { button: 0, clientX: 300, clientY: 210, pageX: 300, pageY: 210, preventDefault() {} });
  p.fireWindow("mousemove", { buttons: 1, clientX: 350, clientY: 210, pageX: 350, pageY: 210 });
  assert.notEqual(source.scales.x.max, 100);
  assert.equal(peer.scales.x.max, 100, "peers wait for the release");
  assert.equal(peer.rebuilds, 0);
  p.fireWindow("mouseup", { button: 0, clientX: 350, clientY: 210, pageX: 350, pageY: 210 });
  assert.deepEqual([peer.scales.x.min, peer.scales.x.max], [source.scales.x.min, source.scales.x.max]);
  assert.equal(peer.__kymo_userzoom, true, "the peer keeps the window through live refreshes");
  assert.equal(peer.rebuilds, 1, "one rebuild per peer");
  assert.equal(source.rebuilds, 1, "the source, already showing the window, is not rescaled again");
  assert.equal(wall.scales.x.max, 1.76e9 + 100, "another axis's group is untouched");
});

test("a cancelled pull leaves the peers alone and restores the source", () => {
  const p = page();
  const xs = [0, 25, 50, 75, 100];
  const source = p.create("a", xs);
  const peer = p.create("b", xs);
  pullAxis(p, "a", 300, 350, "escape");
  assert.deepEqual([source.scales.x.min, source.scales.x.max], [0, 100]);
  assert.deepEqual([peer.scales.x.min, peer.scales.x.max], [0, 100]);
  assert.equal(peer.rebuilds, 0);
  // A press whose hand moves under a pixel along the axis marks no zoom and moves no peer: the chart still follows live data, and a zoomed one stays zoomed.
  const still = () => {
    p.el("a").fire("mousedown", { button: 0, clientX: 300, clientY: 210, pageX: 300, pageY: 210, preventDefault() {} });
    p.fireWindow("mousemove", { buttons: 1, clientX: 300.5, clientY: 216, pageX: 300.5, pageY: 216 });
    p.fireWindow("mouseup", { button: 0, clientX: 300, clientY: 216, pageX: 300, pageY: 216 });
  };
  still();
  assert.ok(!source.__kymo_userzoom);
  assert.equal(peer.rebuilds, 0);
  source.__kymo_userzoom = true;
  still();
  assert.equal(source.__kymo_userzoom, true);
});

test("a pull past a linear chart's data hands a log-x peer a positive window", () => {
  const p = page();
  // A custom x, which can be linear on one chart and log on another.
  const source = p.create("a", [0, 25, 50, 75, 100], { axis: "m-x" });
  // Its data reaches past the source's, so only the release's commit brings its x-max to 100.
  const logPeer = p.create("b", [1, 25, 50, 75, 200], { axis: "m-x", logX: true });
  // A left-half grab pins x-max; dragging right reveals empty space below 0.
  pullAxis(p, "a", 100, 300);
  assert.ok(source.scales.x.min < 0, "the source shows the empty space it was pulled to");
  assert.equal(logPeer.scales.x.min, 1, "the bound below 0 becomes the peer's first x");
  assert.equal(logPeer.scales.x.max, source.scales.x.max);
  assert.equal(logPeer.__kymo_userzoom, true);
  assert.equal(logPeer.rebuilds, 1);
});

test("a log-x peer with nothing positive in the window, or a window across 0 that ends before its data, is left alone", () => {
  // A selection from `from` to `to` px across the 400 px plot of a custom x, which shows -100..100.
  const select = (from, to) => {
    const p = page();
    const charts = { source: p.create("a", [-100, -50, 0, 50, 100], { axis: "m-x" }), logPeer: p.create("b", [1, 10, 100], { axis: "m-x", logX: true }), late: p.create("l", [50, 60, 100], { axis: "m-x", logX: true }), linear: p.create("m", [50, 60, 100], { axis: "m-x" }) };
    p.el("a").fire("mousedown", { button: 0, clientX: from, clientY: 100, pageX: from, pageY: 100, preventDefault() {} });
    p.fireWindow("mousemove", { buttons: 1, clientX: to, clientY: 100, pageX: to, pageY: 100 });
    p.fireWindow("mouseup", { button: 0, clientX: to, clientY: 100, pageX: to, pageY: 100 });
    assert.equal(charts.source.__kymo_userzoom, true, "the source zoomed");
    return charts;
  };
  const untouched = peer => {
    assert.ok(!peer.__kymo_userzoom, "not marked zoomed");
    assert.equal(peer.rebuilds, 0, "not rescaled");
  };
  // Inside the source's negative half, no log-x peer can show any of it.
  const negative = select(40, 120);
  assert.ok(negative.source.scales.x.max <= 0, `source window ${negative.source.scales.x.min}..${negative.source.scales.x.max}`);
  untouched(negative.logPeer);
  assert.equal(negative.linear.__kymo_userzoom, true, "a linear peer takes any window, its data's or not");
  // Inside the positive half, the peer takes the window, and so does one whose data starts after the window ends: only a window reaching 0 is left to its range function.
  const positive = select(240, 320);
  assert.deepEqual([positive.logPeer.scales.x.min, positive.logPeer.scales.x.max], [20, 60]);
  const before = select(240, 260);
  assert.deepEqual([before.late.scales.x.min, before.late.scales.x.max], [20, 30]);
  // Across 0, a peer takes the window from its first x; one whose data starts past the window's end is left alone, not given a window its range function would turn inside out.
  const across = select(100, 250);
  assert.deepEqual([across.logPeer.scales.x.min, across.logPeer.scales.x.max], [1, 25]);
  untouched(across.late);
});

test("a log-x pull keeps its window positive and finite", () => {
  const p = page();
  const c = p.create("c", [500, 1000, 4000], { axis: "m-x", logX: true });
  // Far past the panel, the min stops at exp(-690), a normal double, and the axis still ticks there.
  pullAxis(p, "c", 120, 3000);
  assert.ok(Math.abs(c.scales.x.min / Math.exp(-690) - 1) < 1e-9, `min ${c.scales.x.min}`);
  assert.ok(c.axes[0].splits(c, 0, c.scales.x.min, c.scales.x.max).length > 1);
  // The max has no such bound to meet: a custom x near 1e23 (training FLOPs, say) pulled at its max edge keeps its data in view.
  const flops = p.create("f", [1e20, 1e22, 1e23], { axis: "m-x", logX: true });
  pullAxis(p, "f", 300, 280);
  assert.ok(flops.scales.x.max > 1e23, `max ${flops.scales.x.max}`);
});

test("a step chart's pull still refetches through the step store", () => {
  const p = page();
  const step = p.create("s", [0, 25, 50, 75, 100], { axis: "step" });
  pullAxis(p, "s", 300, 350);
  const zoom = p.el("s").dispatched.find(event => event.type === "kymo-zoom");
  assert.ok(zoom, "the release dispatches kymo-zoom");
  assert.equal(step.__kymo_userzoom, undefined);
});

test("relative-time ticks keep neighbours apart at every spacing, as a stopwatch", () => {
  const p = page();
  const c = p.create("t", [0, 20000]);
  const ticks = values => tickLabels(c, values);
  for (const [values, labels] of [
    [[0, 10, 20, 30, 40, 50], ["0s", "10s", "20s", "30s", "40s", "50s"]],
    [[0, 0.05, 0.1, 0.15, 0.2], ["0.00s", "0.05s", "0.10s", "0.15s", "0.20s"]],
    [[60, 61, 62, 63, 64, 65], ["1:00", "1:01", "1:02", "1:03", "1:04", "1:05"]],
    [[59.5, 59.75, 60, 60.25], ["0:59.50", "0:59.75", "1:00.00", "1:00.25"]],
    [[3600, 3620, 3640, 3660], ["1:00:00", "1:00:20", "1:00:40", "1:01:00"]],
    [[18000, 18100, 18200, 18300], ["5:00:00", "5:01:40", "5:03:20", "5:05:00"]],
    [[0, 2000, 4000, 6000], ["0:00:00", "0:33:20", "1:06:40", "1:40:00"]],
  ]) assert.deepEqual(ticks(values), labels);
});

test("log-time ticks take each value's own precision", () => {
  const p = page();
  // A log relative-time chart is always zero-present, so its ticks arrive 1 ms late.
  const c = p.create("t", [0.001, 1000.001], { logX: true, shifted: true });
  const ticks = real => tickLabels(c, real.map(v => v + 0.001), 10);
  assert.deepEqual(ticks([0.001, 0.01, 0.1, 1, 10, 100, 1000]), ["0.001s", "0.01s", "0.1s", "1s", "10s", "1:40", "16:40"]);
  // Below a millisecond too, zoomed into the first one: the three digits a linear axis stops at would print them all 0.000s.
  assert.deepEqual(ticks([0.0001, 0.0002, 0.0005, 0.0012]), ["0.0001s", "0.0002s", "0.0005s", "0.0012s"]);
  // A log wall-time axis pulled below a second ticks at 1e-4 s, where wall time stops at the three digits Intl prints: it throws on more.
  const wall = p.create("w", [1.76e9, 1.76e9 + 3600], { axis: "wall", logX: true });
  assert.match(tickLabels(wall, [1e-4], 10)[0], /[.,]000(\D|$)/);
});

test("a log relative-time chart plotted 1 ms late labels and hands on its real times", () => {
  const p = page();
  // Rust plots a zero-present log time axis at x + 1 ms; the real times here are 0, 1, 2 and 4.1 ms.
  const shifted = p.create("s", [0.001, 0.002, 0.003, 0.0051], { logX: true, shifted: true });
  assert.deepEqual(tickLabels(shifted, [0.001, 0.002, 0.003], 0.001), ["0s", "0.001s", "0.002s"]);
  // Its zoom geometry stays unshifted; a shifted step chart's works in its 1 step.
  assert.equal(shifted.__kymo_zoom.xShift, 0);
  assert.equal(p.create("st", [1, 2, 11], { axis: "step", logX: true, shifted: true }).__kymo_zoom.xShift, 1);
  // It ticks at real decades, not at uPlot's plotted ones (which read 0.009s, 0.099s, 9.999s); 1 ms, 36 px from its 0 on this 400 px axis, gives way to it.
  const decades = p.create("d", [0.001, 2.001], { logX: true, shifted: true });
  assert.deepEqual(logTicks(decades, 0.001, 2.001).labels, ["0s", "0.01s", "0.1s", "1s"]);
  const near = (a, b) => assert.ok(Math.abs(a - b) < 1e-12, `${a} vs ${b}`);
  const real = c => [c.scales.x.min - (c === shifted ? 0.001 : 0), c.scales.x.max - (c === shifted ? 0.001 : 0)];
  // A pull on it moves a linear peer to the same real window, and a pull on the peer moves it back by that window plus its 1 ms.
  const linear = p.create("l", [0, 0.001, 0.002, 0.0041]);
  // Another log relative-time peer, its real 0 its first point and its 0s tick.
  const late = p.create("e", [0.001, 0.002, 0.0041], { logX: true, shifted: true });
  pullAxis(p, "s", 300, 350);
  assert.equal(linear.__kymo_userzoom, true);
  real(linear).forEach((v, i) => near(v, real(shifted)[i]));
  // The pinned edge is its real 0 exactly, not 0.001 through log and back (a real 2e-19), so the peer's window starts on its first point and keeps the 0s tick.
  assert.equal(shifted.scales.x.min, 0.001);
  assert.equal(late.scales.x.min, 0.001);
  pullAxis(p, "l", 300, 250);
  real(shifted).forEach((v, i) => near(v, real(linear)[i]));
  // A selection on it paints and commits the real window on the peer, and its own window back 1 ms late.
  p.el("s").fire("mousedown", { button: 0, clientX: 100, clientY: 100, pageX: 100, pageY: 100, preventDefault() {} });
  p.fireWindow("mousemove", { buttons: 1, clientX: 300, clientY: 100, pageX: 300, pageY: 100 });
  near(linear.box.left, shifted.box.left - 0.001);
  near(linear.box.left + linear.box.width, shifted.box.left + shifted.box.width - 0.001);
  const painted = [shifted.box.left, shifted.box.left + shifted.box.width];
  p.fireWindow("mouseup", { button: 0, clientX: 300, clientY: 100, pageX: 300, pageY: 100 });
  [shifted.scales.x.min, shifted.scales.x.max].forEach((v, i) => near(v, painted[i]));
  real(linear).forEach((v, i) => near(v, real(shifted)[i]));
  const q = page();
  const source = q.create("s", [0.001, 0.002, 0.003, 0.0051], { logX: true, shifted: true });
  const after = q.create("e", [0.001, 0.002, 0.0041], { logX: true, shifted: true });
  // A cancelled pull restores its bounds exactly too.
  pullAxis(q, "s", 300, 350, "escape");
  assert.deepEqual([source.scales.x.min, source.scales.x.max], [0.001, 0.0051]);
  // A selection dragged past its left edge starts at its real 0 exactly, and so does the peer's window.
  q.el("s").fire("mousedown", { button: 0, clientX: 100, clientY: 100, pageX: 100, pageY: 100, preventDefault() {} });
  q.fireWindow("mousemove", { buttons: 1, clientX: -50, clientY: 100, pageX: -50, pageY: 100 });
  q.fireWindow("mouseup", { button: 0, clientX: -50, clientY: 100, pageX: -50, pageY: 100 });
  assert.equal(source.scales.x.min, 0.001);
  assert.equal(after.scales.x.min, 0.001);
});

test("wall-time ticks show fractional digits where the chosen increment needs them, and only then", () => {
  const p = page();
  const t = 1.76e9;
  const c = p.create("w", [t, t + 1], { axis: "wall" });
  const sub = tickLabels(c, [t, t + 0.1, t + 0.2, t + 0.3], 0.1);
  assert.equal(new Set(sub).size, 4, sub.join(" | "));
  // Separator-agnostic: a decimal comma in some locales.
  assert.ok(/[.,]1(\D|$)/.test(sub[1]), sub[1]);
  const whole = tickLabels(c, [t, t + 10, t + 20], 10);
  assert.equal(new Set(whole).size, 3);
  assert.ok(!/\d[.,]\d/.test(whole.join(" ")), whole.join(" | "));
  // A whole-millisecond tick in 2038 whose seconds times 1000 come back just under it reads its own millisecond, not the one before.
  assert.match(tickLabels(c, [2170000000.101, 2170000000.102], 0.001)[0], /[.,]101(\D|$)/);
  // Ticks near 1.76e9 differ by their spacing only to within an ulp (2.4e-7): the digits follow the increment uPlot chose, not the differences.
  for (const [incr, digits] of [[2.5, 1], [0.2, 1], [0.1, 1], [0.05, 2], [0.02, 2], [0.01, 2], [0.005, 3]]) {
    const ticks = Array.from({ length: 4 }, (_, i) => t + 0.37 + i * incr);
    const labels = tickLabels(c, ticks, incr);
    assert.equal(new Set(labels).size, 4, labels.join(" | "));
    for (const label of labels) assert.equal(/[.,](\d+)/.exec(label)?.[1].length, digits, `${incr} s: ${label}`);
  }
});

test("the relative-time readout names the hovered time to a tenth of a second", () => {
  const p = page();
  p.create("t", [42.5, 18100.25]);
  assert.equal(readout(p, "t", 0), "42.5s");
  assert.equal(readout(p, "t", 1), "5:01:40.3");
});

test("a rebuild for anything but the x axis keeps a client-side zoom", () => {
  const p = page();
  const xs = [0, 25, 50, 75, 100];
  const zoomed = p.create("z", xs, { hash: "a" });
  zoomed.setScale("x", { min: 25, max: 75 });
  zoomed.__kymo_userzoom = true;
  // A recolor or rename changes the structural config, so the chart is created anew.
  const relabeled = p.create("z", xs, { labels: ["renamed"], hash: "b" });
  assert.notEqual(relabeled, zoomed);
  assert.deepEqual([relabeled.scales.x.min, relabeled.scales.x.max], [25, 75]);
  assert.equal(relabeled.__kymo_userzoom, true, "live refreshes keep the window");
  // An unzoomed chart rebuilds with no extra rescale.
  relabeled.__kymo_userzoom = false;
  const fresh = p.create("z", xs, { labels: ["again"], hash: "c" });
  assert.equal(fresh.rebuilds, 0);
  // Another x axis, or the same one turned logarithmic, starts over: each is part of what a zoom belongs to.
  for (const [from, to] of [[{}, { axis: "wall" }], [{}, { logX: true, shifted: true }]]) {
    const zoomedOn = p.create("k", [1, 25, 50, 75, 100], { ...from, hash: "a" });
    zoomedOn.setScale("x", { min: 25, max: 75 });
    zoomedOn.__kymo_userzoom = true;
    const other = p.create("k", [1, 25, 50, 75, 100], { ...to, hash: "b" });
    assert.deepEqual([other.scales.x.min, other.scales.x.max], [1, 100], JSON.stringify(to));
    assert.ok(!other.__kymo_userzoom);
  }
});

test("linear time axes tick at round durations, step axes at whole steps", () => {
  const p = page();
  p.create("c", [0, 100]);
  const tables = p.window.__kymo_incrs, { time, step, num } = tables;
  for (const round of [1, 5, 15, 30, 60, 300, 900, 3600, 21600, 43200, 86400]) assert.ok(time.includes(round), `${round} s`);
  assert.ok(!time.includes(2.5) && !time.includes(25), "no 2.5 steps, which read as 0:00:25 and 0:00:02.5");
  // Increasing, up to ten million years.
  assert.ok(time.every((v, i) => i === 0 || v > time[i - 1]));
  assert.equal(time.length, 57);
  assert.equal(time.at(-1), 10 * 31536000 * 1e6);
  p.create("again", [0, 100]);
  assert.equal(p.window.__kymo_incrs, tables, "built once per page");
  // Steps are integers; custom x keeps uPlot's own increments, spelled as it spells them.
  assert.ok(step.every(Number.isInteger) && step[0] === 1 && step.includes(25) && !step.includes(2.5));
  assert.ok(num.includes(2.5) && num.includes(0.025) && num[0] === 1e-32 && num.at(-1) === 5e31);
  // Each axis picks from its own: a wall clock round minutes, a step axis whole steps where uPlot ticked 1,500,020.2.
  const t = 1.76e9;
  assert.equal(picked(p.create("w", [t, t + 3600], { axis: "wall" }), t, t + 3600).step % 60, 0);
  assert.deepEqual(picked(p.create("s", [0, 10], { axis: "step" }), 1500020, 1500022).labels, ["1,500,020", "1,500,021", "1,500,022"]);
  // A log axis ticks through xSplits; its one increment fits any window, so uPlot always asks for them.
  assert.deepEqual([...p.create("rl", [0.001, 100.001], { axis: "rel", logX: true, shifted: true }).axes[0].incrs], [Infinity]);
});

test("a linear axis ticks at the finest increment whose labels all fit where they land", () => {
  const p = page();
  const check = (c, mn, mx, step, dim = 800) => {
    const pick = picked(c, mn, mx, dim);
    assert.equal(pick.step, step, `${mn}..${mx}: ${pick.labels.join(" | ")}`);
    fits(pick, mn, mx, dim);
  };
  // Relative time on 800 px: seconds at uPlot's 50 px, then h:mm:ss labels, three-digit hours, the millisecond digits 5 ms ticks would print, and a sign each push it coarser.
  const c = p.create("rel", [0, 100]);
  for (const [mn, mx, step] of [[0, 30, 2], [0, 7200, 900], [0, 36000, 3600], [0, 400 * 3600, 172800], [3600, 3600.0667, 0.01], [-7200, 60, 900]]) check(c, mn, mx, step);
  // In CSS pixels at a device pixel ratio of 2 too.
  check(page({ dpr: 2 }).create("rel", [0, 100]), 0, 7200, 900);
  // Steps past a million print 9 characters, so 2-step ticks overlapped at uPlot's 50 px; short and suffixed steps keep it.
  const s = p.create("step", [0, 100], { axis: "step" });
  for (const [mn, mx, step] of [[1500012, 1500042, 5], [0, 300, 20], [0, 2e7, 2e6]]) check(s, mn, mx, step);
  // Custom x: 0.02 ticks print "1,000,000.12" and 0.025 ones a digit more, so 0.05; signed below 0 too.
  const x = p.create("custom", [0, 100], { axis: "custom" });
  for (const [mn, mx] of [[1000000.1, 1000000.3], [-1000000.3, -1000000.1]]) check(x, mn, mx, 0.05);
  // Exactly 1e-6 apart, ticks print their six digits.
  assert.deepEqual(picked(x, 0.147441, 0.147446, 600).labels.slice(0, 2), ["0.147441", "0.147442"]);
  // FLOPs ticks are the decimals they spell (28 times 1e24 read 27999999999999996B), and a window too narrow for two labels shows one.
  const flops = p.create("flops", [2e25, 3e25], { axis: "custom" });
  for (const label of picked(flops, 2.2e25, 2.9e25).labels) assert.match(label, /^\d+T$/);
  assert.deepEqual(picked(flops, 1.0700394e25, 1.077871e25, 200).labels, ["10750000000000T"]);
  // Fuzz: random windows on each linear kind at 200 to 1000 px; one tick at least wherever one fits.
  const t = 1.76e9;
  const rnd = random(11);
  for (const [chart, lo, hi, least] of [[c, 0, 3 * 86400, 0.001], [s, 0, 2e7, 1], [x, -12.9, 3.7e6, 1e-4], [flops, 1e20, 6e25, 1e18], [p.create("wall", [t, t + 1], { axis: "wall" }), t, t + 400 * 86400, 0.001]]) {
    for (let k = 0; k < 150; k++) {
      const span = Math.max(least * 40, (hi - lo) * Math.pow(10, -6 * rnd())), mn = lo + (hi - lo - span) * rnd(), dim = 200 + 800 * rnd();
      const pick = picked(chart, mn, mn + span, dim);
      fits(pick, mn, mn + span, dim);
      assert.ok(pick.ticks.length >= 1 && pick.ticks.every(v => v >= mn - 1e-9 * span && v <= mn + span * (1 + 1e-9)), `${mn}..${mn + span} at ${dim} px: ${pick.labels.join(" | ")}`);
    }
  }
});

test("a linear wall-time axis spaces its ticks a millisecond apart at least, where their labels still differ", () => {
  const p = page();
  const t = 1.76e9;
  const c = p.create("w", [t, t + 1], { axis: "wall" });
  // A millisecond apart the labels differ; closer, the three digits Intl prints would make them alike.
  const at = spacing => Array.from({ length: 4 }, (_, i) => t + 0.37 + i * spacing);
  assert.equal(new Set(tickLabels(c, at(0.001), 0.001)).size, 4);
  assert.ok(new Set(tickLabels(c, at(0.0002), 0.0002)).size < 4);
  // Sub-second ticks fit their wider labels too.
  for (const span of [0.035, 60]) fits(picked(c, t, t + span), t, t + span);
  // Ticks a day or more apart fall on local midnights and read as dates, which fit many more to the window than date-and-time labels.
  assert.equal(tickLabels(c, [t], 86400)[0], new Date(t * 1000).toLocaleDateString());
  const month = picked(c, t, t + 30 * 86400);
  assert.ok(month.step >= 86400 && month.ticks.length >= 4 && month.labels.every(label => !label.includes(":")), month.labels.join(" | "));
  fits(month, t, t + 30 * 86400);
  // A window too narrow for two dates shows one, not a coarser increment with no midnight in it.
  assert.equal(picked(c, t, t + 1.3 * 86400, 90).ticks.length, 1);
  // Nothing throws in Date's last year, which stops in September.
  assert.doesNotThrow(() => picked(c, 8639999999999, 8640000000000));
  // Its ticks a day or more apart are epoch multiples, not local midnights, so they keep their times.
  const lw = p.create("lw2", [t, t + 30 * 86400], { axis: "wall", logX: true });
  const days = logTicks(lw, t, t + 30 * 86400);
  assert.ok(days.ticks[1] - days.ticks[0] >= 86400 && days.labels.every(label => /\d:\d\d/.test(label)), days.labels.join(" | "));
});

test("wall-time ticks fall on round local times, through a DST change too", () => {
  const tz = process.env.TZ;
  try {
    for (const [zone, from, span, incr] of [
      // US clocks fall back at 2 AM on 2026-11-01: 6-hour ticks stay at midnight, 6 AM, noon and 6 PM either side.
      ["America/New_York", Date.UTC(2026, 9, 31, 12) / 1000, 2 * 86400, 21600],
      ["America/New_York", Date.UTC(2026, 9, 30, 12) / 1000, 4 * 86400, 86400],
      // Quarter hours tick on through the repeated hour, twice.
      ["America/New_York", Date.UTC(2026, 10, 1, 5) / 1000, 7200, 900],
      // US clocks spring forward at 2 AM on 2026-03-08: no 2:00 or 2:30 ticks, and 3:00 follows 1:30.
      ["America/Los_Angeles", Date.UTC(2026, 2, 8, 8) / 1000, 5 * 3600, 1800],
      ["America/Los_Angeles", Date.UTC(2026, 2, 8, 9, 59, 30) / 1000, 60, 60],
      // Its midnight and 3 AM would sit 2 hours apart on 3-hour ticks: 3 AM gives way.
      ["America/Los_Angeles", Date.UTC(2026, 2, 7, 12) / 1000, 86400, 10800],
      ["America/Los_Angeles", Date.UTC(2026, 2, 7, 12) / 1000, 2 * 86400, 21600],
      // Nepal is UTC+5:45: quarter hours of its clock, not of UTC's.
      ["Asia/Kathmandu", Date.UTC(2026, 0, 1, 3, 7) / 1000, 7200, 900],
      ["Asia/Kathmandu", Date.UTC(2026, 0, 1, 3, 7, 13.0123) / 1000, 0.05, 0.005],
    ]) {
      process.env.TZ = zone;
      const p = page();
      const c = p.create("w", [from, from + span], { axis: "wall" });
      const ticks = c.axes[0].splits(c, 0, from, from + span, incr);
      assert.ok(ticks.length >= Math.floor(span / incr) - 1, `${zone}: ${ticks.length} ticks`);
      assert.ok(ticks.every((t, i) => i === 0 || t - ticks[i - 1] >= 0.9 * incr), `${zone}: in order, once each, an increment apart`);
      for (const tick of ticks) {
        assert.ok(tick >= from && tick <= from + span);
        const d = new Date(Math.round(tick * 1000));
        const clock = ((d.getHours() * 60 + d.getMinutes()) * 60 + d.getSeconds()) * 1000 + d.getMilliseconds();
        assert.equal(clock % Math.round(incr * 1000), 0, `${zone}: ${d.toString()}`);
      }
      // Months tick at their local starts, a quarter from January; years every 2 from year 0.
      for (const [incr, n, months] of [[7862400, 3, true], [2 * 31536000, 2, false]]) {
        const ticks = c.axes[0].splits(c, 0, from, from + 9 * 31536000, incr);
        const at = ticks.map(tick => new Date(tick * 1000)), index = d => d.getFullYear() * 12 + d.getMonth();
        assert.ok(ticks[0] - from < incr * 1.1 && from + 9 * 31536000 - ticks.at(-1) < incr * 1.1, `${zone}: ${ticks.length} ticks`);
        for (const [i, d] of at.entries()) {
          assert.ok(d.getDate() === 1 && d.getHours() === 0 && d.getMinutes() === 0 && (months ? d.getMonth() % n : d.getMonth() + (d.getFullYear() % n)) === 0, `${zone}: ${d.toString()}`);
          if (i) assert.equal(index(d) - index(at[i - 1]), months ? n : 12 * n);
        }
      }
    }
    // Santiago skips its midnight on 2026-09-06: that day still ticks, where Date reads the midnight, at 1 AM.
    process.env.TZ = "America/Santiago";
    const santiago = page().create("s", [1788600000, 1788800000], { axis: "wall" });
    const days = [...santiago.axes[0].splits(santiago, 0, Date.UTC(2026, 8, 5, 16) / 1000, Date.UTC(2026, 8, 7, 15) / 1000, 86400)].map(t => new Date(t * 1000));
    assert.deepEqual(days.map(d => [d.getDate(), d.getHours()]), [[6, 1], [7, 0]]);
    // A window millions of years out gets no ticks rather than a counter that stops advancing.
    const p = page();
    const far = p.create("far", [1.76e9, 1.76e9 + 1], { axis: "wall" });
    assert.deepEqual([...far.axes[0].splits(far, 0, 1e13, 1e13 + 1, 0.001)], []);
  } finally {
    if (tz === undefined) delete process.env.TZ;
    else process.env.TZ = tz;
  }
});

test("every log x axis ticks at real values its labels fit between, zoomed or not", () => {
  // The fake plots on 400 px and measures 7 px a character; xSplits reads both.
  const ticked = (c, lo, hi) => {
    const pick = logTicks(c, lo, hi), { ticks, labels } = pick;
    assert.ok(ticks.every(v => v >= lo && v <= hi * (1 + 1e-12)), `${ticks} in ${lo}..${hi}`);
    assert.equal(new Set(labels).size, labels.length, labels.join(" | "));
    fits(pick, c.scales.x.min, c.scales.x.max, 400, true);
    return labels;
  };
  const p = page();
  // A zoomed step chart refetches a window without its step 0, which plots plain log x: uPlot's own log ticks left steps 2000..3000 and 120k..800k blank.
  const plain = p.create("plain", [2000, 3000], { axis: "step", logX: true });
  assert.deepEqual(ticked(plain, 2000, 3000), ["2,000", "2,200", "2,400", "2,600", "2,800", "3,000"]);
  assert.deepEqual(ticked(plain, 120000, 800000), ["200k", "400k", "600k", "800k"]);
  assert.deepEqual(ticked(plain, 300, 90000), ["1k", "10k"], "decades where 1, 2, 5 crowd");
  assert.deepEqual(ticked(plain, 2000.2, 2000.8), [], "and no fractional steps");
  // Nor below a step, where a pull can take the window.
  assert.deepEqual(ticked(plain, 0.1, 0.8), []);
  assert.deepEqual(ticked(plain, 0.1, 10), ["1", "2", "5", "10"]);
  // A zero-present chart plots steps + 1; its ticks a decade apart or less are labelled by their spacing, not as 1M six times.
  const shifted = p.create("shifted", [1, 2e6 + 1], { axis: "step", logX: true, shifted: true });
  assert.deepEqual(ticked(shifted, 1000001, 1000011), ["1,000,000", "1,000,005", "1,000,010"]);
  assert.deepEqual(ticked(shifted, 2101, 2201), ["2,100", "2,120", "2,140", "2,160", "2,180", "2,200"]);
  assert.deepEqual(ticked(shifted, 1, 2e6 + 1), ["0", "100", "10k", "1M"]);
  // Custom x at any magnitude: these FLOPs were all blank, and these learning rates read only 1e-4.
  const flops = p.create("flops", [3e20, 7e20], { axis: "custom", logX: true });
  assert.deepEqual(ticked(flops, 3e20, 7e20), ["400000000T", "600000000T"]);
  assert.deepEqual(ticked(flops, 2e-5, 9e-4), ["2e-5", "5e-5", "1e-4", "2e-4", "5e-4"]);
  // A log time axis's linear ticks fall on round durations: 2:46:40, 5:33:20 overlapped.
  const rel = p.create("rel", [0.001, 100000.001], { logX: true, shifted: true });
  assert.deepEqual(ticked(rel, 10000.001, 90000.001), ["12:00:00", "24:00:00"]);
  // Pulled past every increment into a window with no 1, 2 or 5 times a power of ten, an axis gets no ticks rather than a search that never ends, or the one tick that fits of its largest increments; across hundreds of decades it measures a few labels, not hundreds.
  assert.deepEqual(ticked(flops, 2.1e33, 4.9e33), []);
  assert.deepEqual(ticked(plain, 2.1e33, 4.9e33), []);
  assert.equal(ticked(rel, 1.1e15, 1.99e15).length, 1);
  // A crowded tier's stride can pass every power in a window: on a 113 px plot 1e25 and 1e26 crowd, and every third power lies outside 1.1e24..1.1e26, so the power nearest its middle stands alone.
  flops.bbox.width = 113;
  assert.deepEqual(ticked(flops, 1.1e24, 1.1e26), ["10000000000000T"]);
  flops.bbox.width = 400;
  // A pulled log time axis ticks no finer than its labels print, a microsecond: its ladder below one read 0s five times.
  const relPlain = p.create("relPlain", [5, 50, 500], { logX: true });
  assert.deepEqual(ticked(relPlain, 1e-21, 100), ["0.00001s", "1s"]);
  let measured = 0;
  const measure = flops.ctx.measureText;
  flops.ctx.measureText = text => (measured++, measure(text));
  assert.ok(ticked(flops, 1.9e-22, 1.19e300).length >= 1 && measured < 40, `${measured} labels measured`);
  flops.ctx.measureText = measure;
  // FLOPs ticks are the decimals they spell: 20 times 1e24 read 19999999999999996B.
  for (const label of ticked(flops, 1.9e25, 2.2e25)) assert.match(label, /^(19|2[0-2])0+T$/);
  // And so do they over their unit: 5.5e26 over 1e12 read 549999999999999.94T.
  for (const label of ticked(flops, 5.5e26, 6.5e26)) assert.match(label, /^\d+T$/);
  // Past 2^53 the finest increment's multiples would round, or its counter stop advancing: a coarser one ticks.
  assert.deepEqual(ticked(flops, 1e15, 1e15 + 0.125), ["1,000,000,000,000,000"]);
  const narrow = logTicks(flops, 110198468853707550, 110198468853707840).labels;
  assert.ok(narrow.length && narrow.every(label => label.endsWith("00")), `${narrow}, not ...760`);
  // Fuzz: random windows on each kind, wall time's dates among them, whose labels change width across a window.
  const t = 1.76e9;
  const wall = p.create("wall", [t, t + 400 * 86400], { axis: "wall", logX: true });
  const rnd = random(7);
  for (const c of [plain, shifted, flops, rel, wall]) {
    const x = c.data[0], a = Math.log10(x[0]), b = Math.log10(x[x.length - 1]);
    for (let k = 0; k < 200; k++) {
      const w = (b - a) * Math.pow(10, -4 * rnd()), lo = a + (b - a - w) * rnd();
      ticked(c, Math.pow(10, lo), Math.pow(10, lo + w));
    }
  }
});

test("y labels take the increment uPlot chose and every digit their ticks differ in", () => {
  const p = page();
  const y = (c, vals, incr) => [...c.axes[1].values(c, vals, 1, 50, incr)];
  const c = p.create("y", [0, 1, 2]);
  // The smallest tick difference, which labelled them before, carries float error: 0.02 five times.
  assert.deepEqual(y(c, [0.019385, 0.019386, 0.019387, 0.019388, 0.019389], 1e-6), ["0.019385", "0.019386", "0.019387", "0.019388", "0.019389"]);
  // Suffixed labels move each value's own digits to the unit: rounded to 15 digits these read 1000000000000000T three times.
  assert.deepEqual(y(c, [1e27, 1.000000000000002e27, 1.000000000000004e27], 2e12), ["1000000000000000T", "1000000000000002T", "1000000000000004T"]);
  // A log y axis labels each tick by its own value, and the splits its filter drops arrive as null and print blank.
  const log = p.create("logy", [0, 1, 2], { logY: true });
  assert.deepEqual(y(log, [1, null, 100, 2000], 0), ["1", "", "100", "2k"]);
});

test("relative-time labels take their fields from the rounded value, both signs", () => {
  const p = page();
  // A readout rounds a logged time to a tenth, its fields after the rounding.
  const xs = [0.04, 59.96, 3599.95, 3599.96];
  const c = p.create("r", xs);
  assert.deepEqual(xs.map((_, i) => readout(p, "r", i)), ["0.0s", "1:00.0", "1:00:00.0", "1:00:00.0"]);
  // A list whose largest value reaches an hour labels every tick with hours, before 0 too (a pull past the start).
  const ticks = values => tickLabels(c, values);
  assert.deepEqual(ticks([3540, 3570, 3600]), ["0:59:00", "0:59:30", "1:00:00"]);
  assert.deepEqual(ticks([-3600, -1800, 0]), ["-1:00:00", "-0:30:00", "0:00:00"]);
});

test("a rebuild during an axis pull carries the window the pull started from", () => {
  const p = page();
  const xs = [0, 25, 50, 75, 100];
  // Unzoomed: the pull's transient window is not kept, and the rebuilt chart is not zoomed.
  p.create("g", xs, { hash: "a" });
  p.el("g").fire("mousedown", { button: 0, clientX: 300, clientY: 210, pageX: 300, pageY: 210, preventDefault() {} });
  p.fireWindow("mousemove", { buttons: 1, clientX: 350, clientY: 210, pageX: 350, pageY: 210 });
  let rebuilt = p.create("g", xs, { labels: ["renamed"], hash: "b" });
  assert.deepEqual([rebuilt.scales.x.min, rebuilt.scales.x.max], [0, 100]);
  assert.ok(!rebuilt.__kymo_userzoom);
  assert.equal(p.window.__kymo_zg, null, "the pull was cancelled");
  // Zoomed before the pull: the earlier window is kept.
  rebuilt.setScale("x", { min: 25, max: 75 });
  rebuilt.__kymo_userzoom = true;
  p.el("g").fire("mousedown", { button: 0, clientX: 300, clientY: 210, pageX: 300, pageY: 210, preventDefault() {} });
  p.fireWindow("mousemove", { buttons: 1, clientX: 350, clientY: 210, pageX: 350, pageY: 210 });
  assert.notEqual(rebuilt.scales.x.max, 75, "the pull moved the window");
  rebuilt = p.create("g", xs, { labels: ["again"], hash: "c" });
  assert.deepEqual([rebuilt.scales.x.min, rebuilt.scales.x.max], [25, 75]);
  assert.equal(rebuilt.__kymo_userzoom, true);
});

test("a rebuild during a selection carries the window it started from", () => {
  const p = page();
  const xs = [0, 25, 50, 75, 100];
  const c = p.create("s", xs, { hash: "a" });
  c.setScale("x", { min: 25, max: 75 });
  c.__kymo_userzoom = true;
  p.el("s").fire("mousedown", { button: 0, clientX: 40, clientY: 100, pageX: 40, pageY: 100, preventDefault() {} });
  p.fireWindow("mousemove", { buttons: 1, clientX: 120, clientY: 100, pageX: 120, pageY: 100 });
  assert.ok(p.window.__kymo_zg, "the selection is under way");
  const rebuilt = p.create("s", xs, { labels: ["renamed"], hash: "b" });
  assert.deepEqual([rebuilt.scales.x.min, rebuilt.scales.x.max], [25, 75]);
  assert.equal(rebuilt.__kymo_userzoom, true);
  assert.equal(p.window.__kymo_zg, null, "the selection was cancelled");
});

test("a chart takes its box's width floored, at creation and on resize", () => {
  // uPlot hides its cursor line once the line's rounded x passes a fractional plot width, which loses it on right-edge hovers.
  const p = page();
  p.resize("w", 400.6);
  const c = p.create("w", [0, 50, 100]);
  assert.equal(c.width, 400);
  // The observer's first call, just after the create, finds the box within 1 px of the chart and resizes nothing.
  p.resize("w", 400.6);
  assert.equal(c.resizes, 0);
  p.resize("w", 452.5);
  assert.equal(c.width, 452);
  // A box read a hair short, as at browser zoom, keeps the chart; more than 1 px away resizes it.
  p.resize("w", 451.997);
  assert.equal(c.width, 452);
  p.resize("w", 453.9);
  assert.equal(c.width, 453);
});
