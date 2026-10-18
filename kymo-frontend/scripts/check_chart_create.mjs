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
function render(id, { axis = "rel", logX = false, shifted = false, labels = ["run"], hash = "cfg" } = {}) {
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
    __KYMO_X_SPLITS__: shifted ? "{splits:xSplits,filter:function(u,s){return s}}" : "{}",
    __KYMO_X_DISTR__: logX ? "3" : "1",
    __KYMO_X_RANGE__: logX ? "{range:logSafeX}" : "{}",
    __KYMO_Y_DISTR__: "1",
    __KYMO_Y_RANGE__: "{range:yNice}",
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
    getBoundingClientRect: () => ({ left: 0, top: 0, right: 400, bottom: 220, width: 400, height: 220 }),
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
    ResizeObserver: class { observe() {} disconnect() {} }, getComputedStyle: () => ({ height: "220px", getPropertyValue: () => "" }),
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

test("a log-x pull stops where uPlot's log tick walk still ends", () => {
  const p = page();
  const c = p.create("c", [500, 1000, 4000], { axis: "m-x", logX: true });
  // Far past the panel: below about 1e-25 uPlot's walk never ends, and the tab hangs.
  pullAxis(p, "c", 120, 3000);
  assert.ok(c.scales.x.min >= Math.exp(-50) * (1 - 1e-9), `min ${c.scales.x.min}`);
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
  // Filtered-out splits arrive as null and stay blank.
  assert.deepEqual(tickLabels(c, [0, null, 20], 10), ["0s", "", "20s"]);
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
  // It ticks at real decades, not at uPlot's plotted ones (which read 0.009s, 0.099s, 9.999s).
  const decades = p.create("d", [0.001, 2.001], { logX: true, shifted: true });
  assert.deepEqual(tickLabels(decades, [...decades.axes[0].splits(decades, 0, 0.001, 2.001)]), ["0s", "0.001s", "0.01s", "0.1s", "1s"]);
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

test("only a linear relative-time axis ticks at round durations", () => {
  const p = page();
  const incrs = options => p.create(`c${Object.values(options).join("-")}`, [0, 100], options).axes[0].incrs;
  const rel = incrs({ axis: "rel" });
  for (const round of [1, 5, 15, 30, 60, 300, 900, 3600, 21600, 43200, 86400]) assert.ok(rel.includes(round), `${round} s`);
  assert.ok(!rel.includes(2.5) && !rel.includes(25), "no 2.5 steps, which read as 0:00:25 and 0:00:02.5");
  const c = p.create("spaced", [0, 100]);
  c.axes[0].font = ["12px sans-serif"];
  // Spaced by the largest label at the finest increment uPlot's 50 px allows on an 800 px axis, plus a gap.
  assert.equal(c.axes[0].space(c, 0, 0, 30, 800), 50, "seconds keep uPlot's spacing");
  assert.equal(c.axes[0].space(c, 0, 0, 7200, 800), 7 * 7 + 18, "h:mm:ss labels are spaced by their width");
  assert.equal(c.axes[0].space(c, 0, 0, 36000, 800), 7 * 8 + 18);
  assert.equal(c.axes[0].space(c, 0, 0, 400 * 3600, 800), 7 * 9 + 18, "three-digit hours widen it");
  assert.equal(c.axes[0].space(c, 0, 3600, 3600.0667, 800), 7 * 11 + 18, "and so do the millisecond digits 5 ms ticks show");
  assert.equal(c.axes[0].space(c, 0, -7200, 60, 800), 7 * 8 + 18, "and a sign");
  const sharp = page({ dpr: 2 }).create("spaced", [0, 100]);
  sharp.axes[0].font = ["24px sans-serif"];
  assert.equal(sharp.axes[0].space(sharp, 0, 0, 7200, 800), 7 * 7 + 18, "in CSS pixels at a device pixel ratio of 2 too");
  // Increasing, up to ten million years.
  assert.ok(rel.every((v, i) => i === 0 || v > rel[i - 1]));
  assert.equal(rel.length, 55);
  assert.equal(rel.at(-1), 10 * 31536000 * 1e6);
  assert.equal(p.create("again", [0, 100]).axes[0].incrs, rel, "built once per page");
  assert.equal(p.create("rl", [0.001, 100.001], { axis: "rel", logX: true, shifted: true }).axes[0].incrs, undefined);
  assert.equal(incrs({ axis: "step" }), undefined);
});

test("a linear wall-time axis spaces its ticks a millisecond apart at least, where their labels still differ", () => {
  const p = page();
  const t = 1.76e9;
  const c = p.create("w", [t, t + 1], { axis: "wall" });
  const incrs = c.axes[0].incrs;
  assert.equal(incrs[0], 0.001);
  assert.ok(incrs.every((v, i) => i === 0 || v > incrs[i - 1]));
  // uPlot's own increments, as it spells them: 1, 2, 2.5 and 5 times each power of ten.
  assert.ok(incrs.includes(0.025) && incrs.includes(2.5e9) && incrs.at(-1) === 5e31);
  // Whole milliseconds, which the three digits Intl prints show exactly: no 2.5 ms.
  assert.ok(incrs.filter(v => v < 1).every(v => Number.isInteger(Math.round(v * 1e6) / 1e3)), "only whole milliseconds");
  assert.equal(p.create("w2", [t, t + 1], { axis: "wall" }).axes[0].incrs, incrs, "built once per page");
  // A millisecond apart the labels differ; closer, the three digits Intl prints would make them alike.
  const at = spacing => Array.from({ length: 4 }, (_, i) => t + 0.37 + i * spacing);
  assert.equal(new Set(tickLabels(c, at(0.001), 0.001)).size, 4);
  assert.ok(new Set(tickLabels(c, at(0.0002), 0.0002)).size < 4);
  // Spaced by the width of a label at the finest increment that fits uPlot's 50 px, plus a gap: sub-second labels are twice that wide.
  c.axes[0].font = ["12px sans-serif"];
  for (const [span, incr] of [[0.035, 0.001], [60, 5]]) {
    const label = tickLabels(c, [t + span], incr)[0];
    assert.equal(c.axes[0].space(c, 0, t, t + span, 800), 7 * label.length + 18, `${span} s: ${label}`);
  }
  assert.equal(p.create("lw", [t, t + 1], { axis: "wall", logX: true }).axes[0].incrs, undefined);
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
