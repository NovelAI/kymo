#!/usr/bin/env node

// Runs the production chart-create script (create.js, with hover.js and zoom_math.js embedded as build_create_js embeds them) against a stand-in uPlot and DOM.
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

// Two values within `tol`, 1e-12 unless given.
const near = (a, b, tol = 1e-12) => assert.ok(Math.abs(a - b) < tol, `${a} vs ${b}`);

/** A chart's x axis, as ShownAxis, and the series it plots: a values line per label. */
function render(id, { axis = "rel", logX = false, shifted = false, logY = false, labels, hash = "cfg", extraSeries = "", nanMarkers = 0, rawStride = 0, xr = null } = {}) {
  const step = axis === "step";
  const time = axis === "rel" || axis === "wall";
  const tokens = {
    __KYMO_HOVER__: hover,
    __KYMO_ZOOM_MATH__: zoomMath,
    __KYMO_ID__: id,
    __KYMO_AXIS_FONT_SIZE__: "12",
    __KYMO_X_AXIS_SIZE__: "23",
    __KYMO_Y_AXIS_MIN_SIZE__: "35",
    __KYMO_RAW_STRIDE__: String(rawStride),
    __KYMO_HAS_RAW__: "false",
    __KYMO_HAS_RANGE__: "false",
    __KYMO_SMOOTHED__: "false",
    __KYMO_HAS_XRANGE__: String(!!xr),
    __KYMO_NAN_MARKERS__: String(nanMarkers),
    __KYMO_ZOOM_REFETCH__: String(step),
    // A zero-present log x plots x+1 in the wire's units (ChartJsConfig::log_shift): the step zoom geometry shifts by 1, readouts by 1 step or 1 ms.
    __KYMO_READOUT_X_SHIFT__: !shifted ? "0" : time ? "0.001" : "1",
    __KYMO_X_DISTR__: logX ? "3" : "1",
    __KYMO_Y_DISTR__: logY ? "3" : "1",
    __KYMO_SERIES_LIST__: "{}," + labels.map(label => `{'label':'${label}','stroke':'#123456','width':1.5,'spanGaps':true,'paths':linePaths,'points':lonePoints()}`).join(",") + extraSeries,
    __KYMO_LABELS__: labels.map(label => `'${label}'`).join(","),
    __KYMO_COLORS__: labels.map(() => "'#123456'").join(","),
    __KYMO_RUN_IDS__: labels.map(label => `'${label}'`).join(","),
    __KYMO_RUN_NAMES__: labels.map(() => "null").join(","),
    __KYMO_XNAN_COUNTS__: labels.map(() => "0").join(","),
    __KYMO_BANDS_LIST__: "",
    __KYMO_CFG_HASH__: hash,
    __KYMO_IS_TIME__: String(time),
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

/** A node `left` px into the page, its rect in the viewport of a page scrolled `scroll()` px, as a browser reports it. */
function element(id = "", scroll = () => 0) {
  const listeners = {};
  return {
    id, style: {}, className: "", innerHTML: "", offsetWidth: 100, offsetHeight: 40, listeners, dispatched: [],
    classList: { toggle() {}, add() {}, remove() {} },
    addEventListener(type, fn) { (listeners[type] ||= []).push(fn); },
    dispatchEvent(event) { this.dispatched.push(event); return true; },
    left: 0, width: 400,
    getBoundingClientRect() { return { left: this.left - scroll(), top: 0, right: this.left + this.width - scroll(), bottom: 220, width: this.width, height: 220 }; },
    appendChild() {}, closest() { return null; },
    fire(type, event) { for (const fn of listeners[type] || []) fn(event); },
  };
}

// A reply's data, as create's default: x, a values line at x/10, and a step chart's bucket x ranges, [lows, highs], as two data-only columns after it.
const chartData = (x, xr) => [Float64Array.from(x), Float64Array.from(x, v => v / 10), ...(xr || []).map(col => Float64Array.from(col))];

/** One page: charts share `window`, as the dashboard's do; `singleClick` turns single-click unzoom on. */
function page({ dpr = 1, singleClick = false } = {}) {
  const windowListeners = {};
  const scroll = () => window.scrollX;
  const nodes = {};
  const created = [];
  class FakeUPlot {
    // The builder records what it was handed and reads no samples.
    static paths = { linear: () => (u, si, i0, i1) => ({ range: [i0, i1] }) };
    constructor(opts, data) {
      this.opts = opts;
      this.data = data;
      this.root = { isConnected: true };
      // uPlot writes the overlay's place inline, relative to its parent (uPlot's wrap), which sits `pageLeft` into the page; the rect reports both, in the scrolled viewport.
      this.over = {
        pageLeft: 0,
        style: { left: "0px", width: "400px" },
        getBoundingClientRect() { return { left: this.pageLeft - window.scrollX + parseFloat(this.style.left), top: 0, width: parseFloat(this.style.width), height: 200 }; },
        appendChild() {},
      };
      // In device pixels, as uPlot's is.
      this.bbox = { left: 0, top: 0, width: 400 * dpr, height: 200 * dpr };
      this.pxRatio = dpr;
      // 7 px per character.
      // At a device pixel ratio above 1, uPlot scales its fonts, so the canvas measures in device pixels.
      this.ctx = { font: "", measureText: text => ({ width: 7 * text.length * dpr }) };
      this.cursor = { sync: { key: opts.cursor.sync.key }, idx: null, left: 0, top: 0 };
      this.series = opts.series;
      this.axes = opts.axes;
      // uPlot's default x ranger widens a single value by about its own magnitude (10 -> 0..20); this fake's widen pads it by 1, which the one-value checks pin.
      const widen = (u, min, max) => min != null && min === max ? [min - 1, max + 1] : [min, max];
      this.scales = { x: { distr: opts.scales.x.distr, range: opts.scales.x.range || widen }, y: { min: 0, max: 200 } };
      this.rebuilds = 0;
      this.redraws = 0;
      this.width = opts.width;
      this.height = opts.height;
      this.resizes = 0;
      this.fitX();
      this.axes[1]._values = [];
      this.layout();
    }
    // uPlot lays the y axis out at its size function's answer, ceiled, over the labels it last made (`axes[1]._values`, which a test sets): at creation, on a changed scale (y's too, which here shows only as new labels) and on a redraw that asks for it.
    layout() { const a = this.axes[1]; a._size = Math.ceil(a.size(this, a._values, 1)); this.laid = a._values; }
    redraw(paths, layout) { this.redraws++; if (layout) this.layout(); }
    // uPlot runs the scale's range function on the data's extent when it auto-ranges or is given open bounds, and never on an explicit setScale's bounds.
    fitX() { const x = this.data[0]; [this.scales.x.min, this.scales.x.max] = this.scales.x.range(this, x[0], x[x.length - 1], "x"); }
    setScale(key, { min, max }) {
      const was = [this.scales[key].min, this.scales[key].max];
      if (key === "x" && min == null && max == null) this.fitX();
      else Object.assign(this.scales[key], { min, max });
      this.rebuilds++;
      for (const hook of this.opts.hooks.setScale || []) hook(this, key);
      if (this.scales[key].min !== was[0] || this.scales[key].max !== was[1]) this.layout();
    }
    // An auto-ranged x follows the data, and as in uPlot's commit, only a range that changes runs the setScale hooks, before any layout change.
    setData(data) {
      this.data = data;
      const x = data[0], [min, max] = this.scales.x.range(this, x[0], x[x.length - 1], "x");
      if (this.opts.scales.x.auto(this) && (min !== this.scales.x.min || max !== this.scales.x.max)) this.setScale("x", { min, max });
      else this.rebuilds++;
      if (this.axes[1]._values !== this.laid) this.layout();
    }
    setCursor() {}
    setSize({ width, height }) { Object.assign(this, { width, height }); this.resizes++; }
    setSelect(box) { this.box = box; }
    destroy() { this.root.isConnected = false; }
    // One CSS pixel per unit, so a selection's box reads in plotted x; y counts down from the plot's 200 px top as uPlot's does; canvas positions are in device pixels.
    valToPos(value, scale, canvas) { const css = scale === "x" ? value : 200 - value; return canvas ? css * dpr + (scale === "x" ? this.bbox.left : this.bbox.top) : css; }
    // A plot-relative pointer through the scale and the plot's width as they are now, in log space on a log axis, as uPlot maps it.
    posToVal(px) {
      const { min, max, distr } = this.scales.x, f = px / parseFloat(this.over.style.width);
      return distr === 3 ? 10 ** (Math.log10(min) + f * (Math.log10(max) - Math.log10(min))) : min + f * (max - min);
    }
  }
  const document = {
    body: element("body"),
    documentElement: { getAttribute: name => (singleClick && name === "data-kymo-single-click-unzoom" ? "true" : null) },
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
    Path2D: class { ops = []; rect(...args) { this.ops.push(["rect", ...args]); } arc(...args) { this.ops.push(["arc", ...args]); } },
  });
  vm.runInContext(hoverPoints, context);
  return {
    window,
    /** A chart over `x`, plotting `options.columns` (NaN for a gap; default x / 10), one series each, with a step chart's bucket x ranges, `options.xr`, after them; `options.stepOpen`, on its x column, says its step answer had no right bound. */
    create(id, x, options = {}) {
      nodes[id] ||= element(id, scroll);
      const columns = options.columns ?? [x.map(v => v / 10)];
      window.__kymo_data[id] = [Object.assign(Float64Array.from(x), { stepOpen: options.stepOpen }), ...columns.map(column => Float64Array.from(column)), ...(options.xr || []).map(column => Float64Array.from(column))];
      const status = vm.runInContext(`(function(){${render(id, { labels: columns.map((_, i) => `run${i}`), ...options })}})()`, context);
      assert.equal(status, "", `create ${id}`);
      return window.__kymo_charts[id];
    },
    el(id) { return nodes[id]; },
    /** Lays a container out `width` px wide, and runs its chart's ResizeObserver if one is mounted. */
    resize(id, width) {
      (nodes[id] ||= element(id, scroll)).width = width;
      window.__kymo_ro?.[id]?.fire();
    },
    tipOf(id) { return created.find(node => node.id === `${id}-tip`); },
    /** The windows chart `id` has asked the step store for, as [xmin, xmax]. */
    zooms(id) { return nodes[id].dispatched.filter(event => event.type === "kymo-zoom").map(event => [event.detail.xmin, event.detail.xmax]); },
    fireWindow(type, event) { for (const fn of [...(windowListeners[type] || [])]) fn(event); },
    /** How many of a drag's window listeners are registered: none once a gesture ends. */
    listening() { return ["mousemove", "mouseup", "blur", "keydown"].reduce((n, type) => n + (windowListeners[type] || []).length, 0); },
  };
}

// A press on chart `id`'s axis strip below the plot (plot 200 px tall inside a 220 px container) at page x, a move of the held hand to x, and a release at x.
const pressAxis = (p, id, x) => p.el(id).fire("mousedown", { button: 0, clientX: x, clientY: 210, pageX: x, pageY: 210, preventDefault() {} });
const moveAxis = (p, x) => p.fireWindow("mousemove", { buttons: 1, clientX: x, clientY: 210, pageX: x, pageY: 210 });
const releaseAxis = (p, x) => p.fireWindow("mouseup", { button: 0, clientX: x, clientY: 210, pageX: x, pageY: 210 });

// A whole pull: a press at page x `from` and a move to `to`, then the release, Escape, or ("held") neither.
function pullAxis(p, id, from, to, end = "mouseup") {
  pressAxis(p, id, from);
  moveAxis(p, to);
  if (end === "mouseup") releaseAxis(p, to);
  else if (end === "escape") p.fireWindow("keydown", { key: "Escape" });
  if (end !== "held") assert.equal(p.listening(), 0, "the pull left window listeners");
}

// A selection's press, moves and release at page x, at plot height: the press goes to the chart's element, the rest to the window, as a drag delivers them.
function selectAt(p, id) {
  return (type, x) => (type === "mousedown" ? p.el(id).fire : p.fireWindow)(type, { button: 0, buttons: 1, clientX: x, clientY: 100, pageX: x, pageY: 100, preventDefault() {} });
}

// A whole selection, pressed at page x `from` and released at `to` after one move there.
function dragSelect(p, id, from, to) {
  const at = selectAt(p, id);
  at("mousedown", from);
  at("mousemove", to);
  at("mouseup", to);
  assert.equal(p.listening(), 0, "the selection left window listeners");
}

// A fixed client zoom on chart c from lo to hi, as a selection leaves one.
const zoomTo = (c, lo, hi) => { c.setScale("x", { min: lo, max: hi }); c.__kymo_userzoom = true; };

// A step value rounded to 1e-9, since a pull solves through log and exp.
const nanostep = v => v === null ? v : Math.round(v * 1e9) / 1e9;

// A chart's x window, [min, max].
const xWindow = c => [c.scales.x.min, c.scales.x.max];

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
  pullAxis(p, "a", 300, 350, "held");
  assert.notEqual(source.scales.x.max, 100);
  assert.equal(peer.scales.x.max, 100, "peers wait for the release");
  assert.equal(peer.rebuilds, 0);
  releaseAxis(p, 350);
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
});

test("a pull past a linear chart's data hands a log-x peer a positive window", () => {
  const p = page();
  // A custom x, which can be linear on one chart and log on another.
  const source = p.create("a", [0, 25, 50, 75, 100], { axis: "m-x" });
  // Its data reaches past the source's: the source showed its newest point, so the pull follows, and the peer follows its own.
  const logPeer = p.create("b", [1, 25, 50, 75, 200], { axis: "m-x", logX: true });
  // A left-half grab pins x-max; dragging right reveals empty space below 0.
  pullAxis(p, "a", 100, 300);
  assert.ok(source.scales.x.min < 0, "the source shows the empty space it was pulled to");
  assert.equal(logPeer.scales.x.min, 1, "the bound below 0 becomes the peer's first x");
  assert.deepEqual([logPeer.scales.x.max, logPeer.__kymo_userzoom], [200, "follow"]);
  assert.equal(logPeer.rebuilds, 1);
});

test("a log-x peer with nothing positive in the window, or a window across 0 that ends before its data, is left alone", () => {
  // A selection from `from` to `to` px across the 400 px plot of a custom x, which shows -100..100.
  const select = (from, to) => {
    const p = page();
    const charts = { source: p.create("a", [-100, -50, 0, 50, 100], { axis: "m-x" }), logPeer: p.create("b", [1, 10, 100], { axis: "m-x", logX: true }), late: p.create("l", [50, 60, 100], { axis: "m-x", logX: true }), linear: p.create("m", [50, 60, 100], { axis: "m-x" }) };
    dragSelect(p, "a", from, to);
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
  // Inside the positive half, the peer takes the window, and so does one whose data starts after the window ends: only a window reaching 0 is clamped.
  const positive = select(240, 320);
  assert.deepEqual([positive.logPeer.scales.x.min, positive.logPeer.scales.x.max], [20, 60]);
  const before = select(240, 260);
  assert.deepEqual([before.late.scales.x.min, before.late.scales.x.max], [20, 30]);
  // Across 0, a peer takes the window from its first x; one whose data starts past the window's end is left alone, not given a window that would turn inside out.
  const across = select(100, 250);
  assert.deepEqual([across.logPeer.scales.x.min, across.logPeer.scales.x.max], [1, 25]);
  untouched(across.late);
});

test("a chart with one x value ranges around it at creation and after a reset, on log and linear x", () => {
  const p = page();
  for (const [id, logX, range] of [["log", true, [1, 100]], ["linear", false, [9, 11]]]) {
    const c = p.create(id, [10], { axis: "m-x", logX });
    assert.deepEqual([c.scales.x.min, c.scales.x.max], range, `${id} at creation`);
    zoomTo(c, 5, 50);
    p.el(id).fire("dblclick", {});
    assert.deepEqual([c.scales.x.min, c.scales.x.max], range, `${id} after a reset`);
    assert.equal(c.__kymo_userzoom, false);
  }
  // A log-x chart whose every x was trimmed (x <= 0) shows a neutral decade.
  const none = p.create("none", [], { axis: "m-x", logX: true });
  assert.deepEqual([none.scales.x.min, none.scales.x.max], [1, 10]);
});

test("a line builder gets the stroked range narrowed to its samples; an empty column gets the stand-in", () => {
  const c = page().create("c", [0, 10, 20, 30, 40, 50], {
    columns: [[NaN, NaN, 3, 4, NaN, NaN], [NaN, NaN, NaN, NaN, NaN, NaN]],
  });
  const [, line, empty] = c.series;
  assert.deepEqual(line.paths(c, 1, 0, 5).range, [2, 3], "uPlot's range out to the column's ends");
  // A null path would have uPlot call the builder again on every repaint.
  assert.equal(empty.paths(c, 2, 0, 5).flags, 0, "an empty column gets a stand-in uPlot caches, without reaching the builder");
});

// Counts the element reads of chart c's column si from here on.
function countReads(c, si = 1) {
  const reads = { n: 0 };
  c.data[si] = new Proxy(c.data[si], { get(target, key) { if (/^\d+$/.test(String(key))) reads.n++; return target[key]; } });
  return reads;
}

test("a column's gaps are scanned once per data array, and not at all when uPlot's range ends on samples", () => {
  const p = page();
  const x = Array.from({ length: 1000 }, (_, i) => i * 10);
  const c = p.create("c", x, { columns: [x.map((_, i) => (i < 900 ? NaN : i)), x.map(() => NaN)] });
  const reads = countReads(c), empty = countReads(c, 2);
  // Each build reads the first end of uPlot's range; that end here is a gap, so the first build scans.
  assert.deepEqual(c.series[1].paths(c, 1, 0, 999).range, [900, 999]);
  assert.equal(reads.n, 1 + 901 + 1, "in from the start to the first sample, then the last slot, a sample");
  c.series[1].paths(c, 1, 0, 999);
  assert.equal(reads.n, 905, "a rebuild without new data scans nothing");
  for (let k = 0; k < 2; k++) c.series[2].paths(c, 2, 0, 999);
  assert.equal(empty.n, 2 + 1000, "an empty column is scanned once too, from its start only");
  c.setData([...c.data]);
  c.series[1].paths(c, 1, 0, 999);
  assert.equal(reads.n, 905 + 903, "new data is scanned again");
  // A zoomed window: uPlot's range runs out to the samples nearest it, so both ends are samples and nothing is scanned.
  c.setData([...c.data]);
  assert.deepEqual(c.series[1].paths(c, 1, 949, 961).range, [949, 961]);
  assert.equal(reads.n, 1808 + 2, "both ends read, nothing scanned");
  // A window past a finished run's end: only the end on a gap is scanned, not the gap before the run's start.
  const run = p.create("r", x, { columns: [x.map((_, i) => (i >= 100 && i < 900 ? i : NaN))] }), runReads = countReads(run);
  assert.deepEqual(run.series[1].paths(run, 1, 850, 999).range, [850, 899]);
  assert.equal(runReads.n, 1 + 1 + 101, "the start, a sample; the end, a gap, scanned back to the last sample");
});

test("a run with one sample gets a mark; two samples, however close, draw their segment", () => {
  const mark = (x, values) => {
    const c = page().create("l", x, { columns: [values] });
    return !!c.series[1].paths(c, 1, 0, x.length - 1).mark;
  };
  // Both ends of the fake's 0 to 200 y scale: the geometry check pins the cull.
  for (const value of [0, 200]) assert.equal(mark([0, 10, 20], [NaN, value, NaN]), true, `a lone ${value}`);
  assert.equal(mark([40, 40.001, 60], [7, 7, NaN]), false);
  assert.equal(mark([5], [3]), true, "a one-slot axis: uPlot's range is that slot, a sample at both ends");
});

test("uPlot gets a line's mark through points.show, which is also what it draws, built once per path", () => {
  const c = page().create("f", [0, 10.4, 20], { columns: [[NaN, 3, NaN]] });
  const s = c.series[1];
  assert.deepEqual([s.points.width, s.points.fill], [0, undefined], "uPlot fills a point of width 0 with the line's stroke");
  s._paths = s.paths(c, 1, 0, 2);
  const mark = s.points.show(c, 1);
  assert.deepEqual(mark.fill.ops, [["arc", 10, 197, 2.5, 0, 2 * Math.PI]], "uPlot's 5 px dot, on whole pixels");
  assert.equal(s.points.paths(c, 1, 0, 2, null), mark);
  assert.equal(s.points.show(c, 1), mark, "a repaint reuses it");
  s._paths = s.paths(c, 1, 0, 2);
  assert.notEqual(s.points.show(c, 1), mark, "a rebuilt path builds its own");
});

test("the draw hook paints data-only NaN markers with their run's values-line alpha", () => {
  // With raw blocks of two, the hook reads run i's alpha at lineBase + i, series 5 and 6 (placeholders here: the harness lists its lines first), so reading a block's series or the other run's fails.
  const col = [1, 2, 3];
  const c = page().create("n", [0, 10, 20], { labels: ["a", "b"], columns: [col, col, col, col, col, col, [NaN, 1, NaN], [1, NaN, NaN]], extraSeries: ",{},{},{},{}", nanMarkers: 2, rawStride: 2 });
  c.series[5].alpha = 0.25;
  c.series[6].alpha = 0.5;
  const arcs = [];
  c.ctx = { save() {}, restore() {}, beginPath() {}, rect() {}, clip() {}, stroke() {}, arc: x => arcs.push([x, c.ctx.globalAlpha]) };
  c.opts.hooks.draw[0](c);
  assert.deepEqual(arcs, [[10, 0.25], [0, 0.5]]);
});

// The fake places x and y one CSS pixel per unit, y counting down from 200 (its y scale is 0 to 200); at device pixel ratio 2 kymo's 5 px dot is 10 device px. This plot box starts 20 device px in and 16 down, off the canvas origin as a real one sits right of the y axis and below kymo's top padding.
test("an envelope's lone bucket is a bar from its min to its max, or a dot when they're within a dot; none outside the scales", () => {
  const mark = (min, max, window, x = [0, 10, 20]) => {
    const c = page({ dpr: 2 }).create("e", x, {
      labels: ["run"],
      columns: [[NaN, NaN, NaN], [NaN, min, NaN], [NaN, max, NaN]],
      extraSeries: ",{'paths':linePaths,'points':lonePoints('#123456',1.5)},{'paths':linePaths,'points':{show:false}}",
    });
    c.bbox = { left: 20, top: 16, width: 800, height: 400 };
    if (window) c.setScale("x", window);
    const s = c.series[2];
    assert.deepEqual([s.points.width, s.points.fill, s.points.bar], [0, "#123456", 1.5]);
    s._paths = s.paths(c, 2, 0, 2);
    const built = s.points.show(c, 2);
    return built && built.fill.ops;
  };
  // x 10 is device px 40; y 7 is 402, 12 is 392: 10 device px apart, a dot.
  assert.deepEqual(mark(7, 12), [["rect", 39, 392, 3, 10]], "a dot apart: the bar at the line's 1.5 CSS px");
  assert.deepEqual(mark(7, 12, null, [0, 10.3, 20]), [["rect", 40, 392, 3, 10]], "at x 40.6, the columns a 3 px stroke covers there");
  assert.deepEqual(mark(7, 11.5), [["arc", 40, 398, 5, 0, 2 * Math.PI]], "closer: a dot between them");
  assert.deepEqual(mark(-5, 3), [["rect", 39, 410, 3, 6]], "partly below the axis: the bar, cut at the plot's bottom");
  assert.deepEqual(mark(195, 205), [["rect", 39, 16, 3, 10]], "partly above the axis: the bar, cut at the plot's top");
  assert.deepEqual(mark(-5, 0), [["rect", 39, 416, 3, 0]], "its max on the axis: cut to nothing, like its band (on a log axis the border circle marks it)");
  assert.equal(mark(-5, -3), null, "wholly below the axis (a log axis's border circle marks it)");
  assert.equal(mark(205, 210), null, "wholly above the axis");
  for (const window of [{ min: 11, max: 20 }, { min: 0, max: 9 }]) assert.equal(mark(7, 12, window), null, `past the window's edge, ${JSON.stringify(window)}`);
  for (const window of [{ min: 10, max: 20 }, { min: 0, max: 10 }]) assert.ok(mark(7, 12, window), `on the window's edge, ${JSON.stringify(window)}`);
});

const vendor = readFileSync(new URL("../assets/vendor/uPlot.iife.min.js", import.meta.url), "utf8");

// The narrowed range reads uPlot's stroked range as the window widened to the nearest sample outside it on each side (or to the column's end).
test("vendored uPlot builds a series' paths over the window widened to its nearest samples outside", () => {
  assert.match(vendor, /let (\w+)=\w+\((\w+)-1,0,(\w+)-1\),(\w+)=\w+\((\w+)\+1,0,\3-1\);for\(;null==(\w+)\[\1\]&&\1>0;\)\1--;for\(;null==\6\[\4\]&&\3-1>\4;\)\4\+\+;return\[\1,\4\]\}\(\w+\[(\w+)\]\);\w+\._paths=\w+\.paths\(\w+,\7,(\w+)\[0\],\8\[1\]\)/);
});

// A series that draws nothing is hidden (create_js.rs).
test("vendored uPlot builds paths for shown series only, and scans only them for a scale's range", () => {
  assert.match(vendor, /forEach\(\((\w+),(\w+)\)=>\{if\(\2>0&&\1\.show&&\(\w+\(\2,!1\),\w+\(\2,!0\),null==\1\._paths\)\)/);
  assert.match(vendor, /let (\w+)=\w+\[\w+\];\1\.show&&\1\.scan&&\1\.scale==\w+&&\w+\(/);
});

// A line's dot names no fill: its series' stroke fills it.
test("vendored uPlot fills a point of width 0 with its series' stroke", () => {
  assert.match(vendor, /(\w+)\.points=\w+\(\{\},\{size:\w+,width:\w+\(1,\.2\*\w+\),stroke:\1\.stroke,/);
  assert.match(vendor, /_stroke:(\w+)=\w+\._stroke,_fill:(\w+)=\w+\._fill,_width:(\w+)=\w+\.width\}=\w+\._paths;.{0,200}?null==\2&&\(\2=\3>0\?"#fff":\1\)/);
});

// The three uPlot internals an axis pull's exit re-fits the y gutter on (create.js), as assets/vendor/README.md lists them.
test("vendored uPlot lays an axis out at its size function's answer over its last labels, keeps a redraw's layout request, and drops a queued commit on destroy", () => {
  assert.match(vendor, /let (\w+)=\w+\((\w+)\.size\(\w+,0==\w+\?null:\2\._values,\w+\)\);\w+=\w+\|\|\1!=\2\._size,\2\._size=\1/);
  const [, , , flag] = vendor.match(/\.redraw=\((\w+),(\w+)\)=>\{if\((\w+)=\3\|\|\2\|\|!1/);
  assert.match(vendor, new RegExp(`${flag}\\)\\{let (\\w+)=(\\w+)\\(\\);${flag}=!1;`));
  const [, destroyed, queued] = vendor.match(/\.destroy=function\(\)\{(\w+)\|\|\(\1=!0,(\w+)=!1/);
  assert.match(vendor, new RegExp(`let (\\w+)=${queued}=\\(\\)=>\\{${queued}==\\1&&(\\w+)\\(\\)\\}.{0,40}function \\2\\(\\)\\{if\\(${destroyed}\\)return;`));
});

test("vendored uPlot draws a series' points when its points.show returns a mark", () => {
  assert.match(vendor, /(\w+)=\w+\.points\.show\((\w+),(\w+),(\w+),(\w+),\w+\),(\w+)=\w+\.points\.filter\(\2,\3,\1,\w+\);\(\1\|\|\6\)&&\(\w+\.points\._paths=\w+\.points\.paths\(\2,\3,\4,\5,\6\)/);
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
  // It ticks at real decades, not at uPlot's plotted ones (which read 0.009s, 0.099s, 9.999s); 1 ms, 36 px from its 0 on this 400 px axis, gives way to it.
  const decades = p.create("d", [0.001, 2.001], { logX: true, shifted: true });
  assert.deepEqual(logTicks(decades, 0.001, 2.001).labels, ["0s", "0.01s", "0.1s", "1s"]);
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
  let at = selectAt(p, "s");
  at("mousedown", 100);
  at("mousemove", 300);
  near(linear.box.left, shifted.box.left - 0.001);
  near(linear.box.left + linear.box.width, shifted.box.left + shifted.box.width - 0.001);
  const painted = [shifted.box.left, shifted.box.left + shifted.box.width];
  // The moving end is the plotted value under the pointer, taken through log space.
  near(painted[1], shifted.posToVal(300));
  at("mouseup", 300);
  [shifted.scales.x.min, shifted.scales.x.max].forEach((v, i) => near(v, painted[i]));
  real(linear).forEach((v, i) => near(v, real(shifted)[i]));
  const q = page();
  const source = q.create("s", [0.001, 0.002, 0.003, 0.0051], { logX: true, shifted: true });
  const after = q.create("e", [0.001, 0.002, 0.0041], { logX: true, shifted: true });
  // A cancelled pull restores its bounds exactly too.
  pullAxis(q, "s", 300, 350, "escape");
  assert.deepEqual([source.scales.x.min, source.scales.x.max], [0.001, 0.0051]);
  // A selection dragged past its left edge starts at its real 0 exactly, and so does the peer's window.
  dragSelect(q, "s", 100, -50);
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
  zoomTo(zoomed, 25, 75);
  // A recolor or rename changes the structural config, so the chart is created anew.
  const relabeled = p.create("z", xs, { labels: ["renamed"], hash: "b" });
  assert.notEqual(relabeled, zoomed);
  assert.deepEqual([relabeled.scales.x.min, relabeled.scales.x.max], [25, 75]);
  relabeled.setData(chartData([0, 50, 100, 150, 200]));
  assert.deepEqual([relabeled.scales.x.min, relabeled.scales.x.max], [25, 75], "live refreshes keep the window");
  // An unzoomed chart rebuilds with no extra rescale.
  relabeled.__kymo_userzoom = false;
  const fresh = p.create("z", xs, { labels: ["again"], hash: "c" });
  assert.equal(fresh.rebuilds, 0);
  // Another x axis, or the same one turned logarithmic, starts over: each is part of what a zoom belongs to.
  for (const [from, to] of [[{}, { axis: "wall" }], [{}, { logX: true, shifted: true }]]) {
    const zoomedOn = p.create("k", [1, 25, 50, 75, 100], { ...from, hash: "a" });
    zoomTo(zoomedOn, 25, 75);
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
  // kymo's tick functions at any magnitude: these FLOPs were all blank, and these learning rates read only 1e-4. The vendored uPlot itself still asks for no splits from 1e17 up (assets/vendor/README.md).
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

test("the log y range snaps positive bounds and gives null bounds a neutral decade", () => {
  const c = page().create("logy", [0, 1, 2], { logY: true });
  const range = c.opts.scales.y.range;
  // A positive window snaps outward on the 1/2/5 ladder.
  assert.deepEqual([...range(c, 2, 700)], [2, 1000]);
  // uPlot passes null when the window holds no positive y.
  assert.deepEqual([...range(c, null, null)], [1, 10]);
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
  // Unzoomed, fixed at [25, 75], or following from 50: the pull's own window is dropped, and a reply to the rebuilt chart moves an unzoomed or following one as ever.
  for (const [zoom, carried] of [[undefined, [0, 200]], [true, [25, 75]], ["follow", [50, 200]]]) {
    const p = page(), c = p.create("g", [0, 25, 50, 75, 100], { hash: "a" });
    if (zoom === true) zoomTo(c, 25, 75);
    if (zoom === "follow") dragSelect(p, "g", 200, 405);
    const before = xWindow(c);
    pullAxis(p, "g", 300, 350, "held");
    assert.notDeepEqual(xWindow(c), before, "the pull moved the window");
    const rebuilt = p.create("g", [0, 25, 50, 75, 100], { hash: "b" });
    assert.equal(p.window.__kymo_zg, null, "the pull was cancelled");
    rebuilt.setData(chartData([0, 50, 100, 150, 200]));
    assert.deepEqual([...xWindow(rebuilt), rebuilt.__kymo_userzoom], [...carried, zoom], String(zoom));
  }
});

test("a rebuild during a selection carries the window it started from", () => {
  const p = page();
  const xs = [0, 25, 50, 75, 100];
  const c = p.create("s", xs, { hash: "a" });
  zoomTo(c, 25, 75);
  const at = selectAt(p, "s");
  at("mousedown", 40);
  at("mousemove", 120);
  assert.ok(p.window.__kymo_zg, "the selection is under way");
  const rebuilt = p.create("s", xs, { labels: ["renamed"], hash: "b" });
  assert.deepEqual([rebuilt.scales.x.min, rebuilt.scales.x.max], [25, 75]);
  assert.equal(rebuilt.__kymo_userzoom, true);
  assert.equal(p.window.__kymo_zg, null, "the selection was cancelled");
});

test("every column but x reaches uPlot with null gaps, on a new chart and on a same-config create", () => {
  // A values line and its data-only marker column, each with NaN gaps.
  const p = page(), opts = { labels: ["a"], nanMarkers: 1 };
  const c = p.create("g", [0, 10, 20], { ...opts, columns: [[NaN, 3, NaN], [2, NaN, NaN]] });
  assert.deepEqual(Array.from(c.data.slice(1), col => Array.from(col)), [[null, 3, null], [2, null, null]]);
  assert.equal(p.create("g", [0, 10, 20], { ...opts, columns: [[5, NaN, NaN], [NaN, NaN, 1]] }), c, "a same-config create updates the live chart through setData");
  assert.deepEqual(Array.from(c.data.slice(1), col => Array.from(col)), [[5, null, null], [null, null, 1]]);
});

test("a selection on a time chart takes in points a live reply adds mid-drag", () => {
  const p = page();
  const c = p.create("g", [0, 100, 200, 300, 400]);
  // The plot runs from 100 to 480 in the page: a 20 px y gutter in a container 80 px in.
  Object.assign(c.over, { pageLeft: 80, style: { left: "20px", width: "380px" } });
  const at = selectAt(p, "g");
  at("mousedown", 290);
  at("mousemove", 390);
  // A reply as uPlot commits it: the unzoomed x follows the data to 800 (the setScale hooks run on the old layout), then the wider y labels grow the gutter to 40 px and the setSize hooks run.
  c.setData(chartData([0, 200, 400, 600, 800]));
  c.over.style = { left: "40px", width: "360px" };
  for (const hook of c.opts.hooks.setSize) hook(c);
  // With the hand still, the press keeps the 200 it had and the moving end reads the plot as it is now: 600 under the pointer, where the press-time plot read 305.
  assert.deepEqual([c.box.left, c.box.left + c.box.width], [200, 600]);
  // The release's x, 10 px past the right edge the reply left at 480, is the scale's own max.
  at("mousemove", 490);
  at("mouseup", 490);
  assert.deepEqual([c.scales.x.min, c.scales.x.max], [200, 800]);
});

test("a selection pressed past a time chart's right edge, or in its last pixel column, keeps that edge as a reply moves it", () => {
  for (const press of [405, 399.5]) {
    const p = page();
    const c = p.create("g", [0, 100, 200, 300, 400]);
    const at = selectAt(p, "g");
    at("mousedown", press);
    at("mousemove", 300);
    c.setData(chartData([0, 200, 400, 600, 800]));
    assert.deepEqual([c.box.left, c.box.left + c.box.width], [600, 800], `pressed at ${press}`);
    at("mouseup", 300);
    assert.deepEqual([c.scales.x.min, c.scales.x.max], [600, 800]);
  }
});

test("a peer's own reply mid-drag repaints its box from its new scale", () => {
  const p = page();
  p.create("a", [0, 100, 200, 300, 400]);
  const b = p.create("b", [0, 100, 200, 300]);
  const at = selectAt(p, "a");
  at("mousedown", 200);
  at("mousemove", 390);
  assert.deepEqual([b.box.left, b.box.left + b.box.width], [200, 300]);
  b.setData(chartData([0, 100, 200, 300, 400, 500]));
  assert.deepEqual([b.box.left, b.box.left + b.box.width], [200, 390]);
  at("mouseup", 390);
});

test("a selection within 1 px of a log chart's edge, as the hover reads it, ends exactly on that edge", () => {
  // Through log space those edges read 3600.0009999999957 and 5.000000000000001, just inside the points they are on. The right one is zoomed short of its newest point so its window is fixed, ending on the edge; a following one would end on the newest x whatever the edge read.
  const p = page();
  const right = p.create("g", [0.001, 1, 10, 100, 3600.001, 7200.001], { logX: true });
  zoomTo(right, 0.001, 3600.001);
  let at = selectAt(p, "g");
  at("mousedown", 200);
  at("mousemove", 399.5);
  at("mouseup", 399.5);
  assert.equal(right.scales.x.max, 3600.001);
  const q = page();
  const left = q.create("g", [5, 50, 500, 5000], { logX: true });
  dragSelect(q, "g", 200, 0.5);
  assert.equal(left.scales.x.min, 5);
});

test("a step chart's selection extrapolates past the plot at both ends", () => {
  const p = page();
  // Pressed in a 40 px y gutter, released 50 px past the plot's right edge at 440.
  p.create("st", [0, 25, 50, 75, 100], { axis: "step" }).over.style.left = "40px";
  dragSelect(p, "st", 0, 490);
  assert.deepEqual(p.zooms("st"), [[-10, 112.5]]);
});

test("a step chart's selection takes in steps a live reply adds mid-drag, pressed inside the plot or past either edge", () => {
  const p = page();
  const grow = c => c.setData(chartData([0, 50, 100, 150, 200]));
  const st = p.create("st", [0, 25, 50, 75, 100], { axis: "step" });
  let at = selectAt(p, "st");
  at("mousedown", 200);
  at("mousemove", 300);
  // The step axis follows the reply to 200, so the hand at 300 is over step 150 now, not 75.
  grow(st);
  assert.deepEqual([st.box.left, st.box.left + st.box.width], [50, 150]);
  at("mouseup", 300);
  assert.deepEqual(p.zooms("st"), [[50, 150]]);
  // Pressed 10 px past the right edge, the press stays 10 px past it: a fortieth of the coverage, 5 steps beyond the newest.
  const q = page();
  const late = q.create("st", [0, 25, 50, 75, 100], { axis: "step" });
  at = selectAt(q, "st");
  at("mousedown", 410);
  at("mousemove", 300);
  grow(late);
  at("mouseup", 300);
  const [[lo, hi]] = q.zooms("st");
  assert.equal(lo, 150);
  near(hi, 205);
  // Pressed in a 40 px y gutter, a tenth of the plot before its left edge, the press stays a tenth of the coverage before the first step: 20 steps once the reply doubles it.
  const r = page();
  const early = r.create("st", [0, 25, 50, 75, 100], { axis: "step" });
  early.over.style.left = "40px";
  at = selectAt(r, "st");
  at("mousedown", 0);
  at("mousemove", 240);
  grow(early);
  at("mouseup", 240);
  assert.deepEqual(r.zooms("st"), [[-20, 100]]);
});

test("a step chart's selection takes in whole buckets as a reply re-buckets them", () => {
  const p = page();
  // Four 25-step buckets, each plotted at its middle.
  const st = p.create("st", [12, 37, 62, 87], { axis: "step", xr: [[0, 25, 50, 75], [24, 49, 74, 99]] });
  const at = selectAt(p, "st");
  at("mousedown", 100);
  at("mousemove", 300);
  // A reply puts the run's 200 steps in four 50-step buckets. The press keeps step 30.75 and the hand is now over 137.5, so the window is the buckets those fall in now; the box shows it from the plot's first x.
  st.setData(chartData([25, 75, 125, 175], [[0, 50, 100, 150], [49, 99, 149, 199]]));
  assert.deepEqual([st.box.left, st.box.left + st.box.width], [25, 149]);
  at("mouseup", 300);
  assert.deepEqual(p.zooms("st"), [[0, 149]]);
});

test("a log step chart's selection reads real steps through its 1 step shift, and takes in steps a reply adds", () => {
  const p = page();
  // Steps 0, 1, 10 and 100, plotted 1 step late; the selection maps the real steps in log(1 + step).
  const st = p.create("st", [1, 2, 11, 101], { axis: "step", logX: true, shifted: true });
  const at = selectAt(p, "st");
  at("mousedown", 100);
  at("mousemove", 300);
  // A reply to step 1000: the press keeps the step a quarter of the way along, and the hand three quarters of the way along is over a later one.
  st.setData(chartData([1, 11, 101, 1001]));
  near(st.box.left, 101 ** 0.25);
  near(st.box.left + st.box.width, 1001 ** 0.75);
  at("mouseup", 300);
  const [[lo, hi]] = p.zooms("st");
  near(lo, 101 ** 0.25 - 1);
  near(hi, 1001 ** 0.75 - 1);
});

test("a selection whose end reaches a time chart's right edge follows new points; one inside the plot, or on a chart zoomed short of its newest point, stays put", () => {
  // Released past the right edge, in its last pixel column, or pressed in the right gutter and dragged left: the window keeps its start, and each reply moves its end to the newest point.
  for (const [press, release] of [[200, 405], [200, 399.5], [405, 200]]) {
    const p = page(), c = p.create("g", [0, 100, 200, 300, 400]);
    dragSelect(p, "g", press, release);
    assert.deepEqual(xWindow(c), [200, 400]);
    c.setData(chartData([0, 200, 400, 600, 800]));
    assert.deepEqual(xWindow(c), [200, 800], `${press} to ${release}`);
  }
  const p = page(), c = p.create("g", [0, 100, 200, 300, 400]);
  dragSelect(p, "g", 100, 300);
  c.setData(chartData([0, 200, 400, 600, 800]));
  assert.deepEqual(xWindow(c), [100, 300]);
  // Zoomed to [100, 300], the right edge isn't the newest point, so a selection past it stays put too.
  dragSelect(p, "g", 200, 405);
  c.setData(chartData([0, 300, 600, 900, 1200]));
  assert.deepEqual(xWindow(c), [200, 300]);
});

test("a following zoom reaches every peer, each following its own newest point, and survives a rebuild until a reset", () => {
  const p = page();
  p.create("a", [0, 100, 200, 300, 400]);
  const b = p.create("b", [0, 100, 200, 300, 400, 500]);
  const early = p.create("c", [0, 50, 100, 150]);
  const empty = p.create("e", [0, 100]);
  empty.setData(chartData([]));
  dragSelect(p, "a", 200, 405);
  assert.deepEqual(xWindow(b), [200, 500]);
  b.setData(chartData([0, 200, 400, 600]));
  assert.deepEqual(xWindow(b), [200, 600]);
  // Its end is its data's end while that lies past the start, so a reply that ends earlier pulls it back.
  b.setData(chartData([0, 200, 450]));
  assert.deepEqual(xWindow(b), [200, 450]);
  // A peer whose data ends before the start, or has none, keeps the window it was given until its data passes the start.
  assert.deepEqual(xWindow(early), [200, 400]);
  early.setData(chartData([0, 60, 120, 180]));
  assert.deepEqual(xWindow(early), [200, 400]);
  assert.deepEqual(xWindow(empty), [200, 400]);
  early.setData(chartData([0, 150, 300, 450, 600]));
  assert.deepEqual(xWindow(early), [200, 600]);
  // A rebuild carries it onto the data the rebuild brings.
  const rebuilt = p.create("a", [0, 200, 400, 600], { hash: "b" });
  assert.deepEqual(xWindow(rebuilt), [200, 600]);
  rebuilt.setData(chartData([0, 250, 500, 750, 1000]));
  assert.deepEqual(xWindow(rebuilt), [200, 1000]);
  p.el("a").fire("dblclick", {});
  rebuilt.setData(chartData([0, 300, 600, 900, 1200]));
  assert.deepEqual(xWindow(rebuilt), [0, 1200]);
  assert.ok(!rebuilt.__kymo_userzoom);
  // A selection to the right edge of a following chart moves the start and keeps following; one inside fixes the window.
  dragSelect(p, "a", 300, 405);
  dragSelect(p, "a", 200, 405);
  rebuilt.setData(chartData([0, 300, 600, 900, 1300]));
  assert.deepEqual(xWindow(rebuilt), [1050, 1300]);
  dragSelect(p, "a", 100, 300);
  rebuilt.setData(chartData([0, 300, 600, 900, 1400]));
  assert.deepEqual(xWindow(rebuilt), [1112.5, 1237.5]);
});

test("a left-half pull on a chart showing its newest point follows it while held and after release, with the grabbed value under the hand; its peers follow their own", () => {
  // Unzoomed, following from 200, or fixed at the newest point: each shows its newest point.
  for (const zoom of ["none", "follow", "fixed"]) {
    const p = page(), c = p.create("g", [0, 100, 200, 300, 400]), h = p.create("h", [0, 100, 200, 300, 400]);
    if (zoom === "follow") dragSelect(p, "g", 200, 405);
    if (zoom === "fixed") zoomTo(c, 100, 400);
    const grabbed = c.posToVal(100);
    pullAxis(p, "g", 100, 150, "held");
    assert.equal(xWindow(c)[1], 400, zoom);
    // A reply mid-pull moves the right edge to the new newest point and keeps the grabbed value under the hand.
    c.setData(chartData([0, 200, 400, 600, 800]));
    assert.equal(xWindow(c)[1], 800, zoom);
    assert.ok(Math.abs(c.posToVal(150) - grabbed) < 1e-9, `${zoom}: ${c.posToVal(150)} under the hand`);
    const lo = xWindow(c)[0];
    releaseAxis(p, 150);
    c.setData(chartData([0, 300, 600, 900, 1200]));
    assert.deepEqual([...xWindow(c), c.__kymo_userzoom], [lo, 1200, "follow"], zoom);
    assert.deepEqual([...xWindow(h), h.__kymo_userzoom], [lo, 400, "follow"], `${zoom} peer`);
  }
  // From the press on: a reply before the first move moves the right edge, the grabbed value kept under the still hand.
  const o = page(), still = o.create("g", [0, 100, 200, 300, 400]);
  pressAxis(o, "g", 100);
  still.setData(chartData([0, 200, 400, 600, 800]));
  assert.equal(xWindow(still)[1], 800);
  near(still.posToVal(100), 100, 1e-9);
  // On a log x axis, in log space: grabbed at 10^0.5, a quarter of the way across 1..100.
  const p = page(), c = p.create("l", [1, 10, 100], { axis: "m-x", logX: true });
  pullAxis(p, "l", 100, 150, "held");
  c.setData(chartData([1, 10, 100, 1000]));
  assert.equal(c.scales.x.max, 1000);
  near(c.posToVal(150), 10 ** 0.5, 1e-9);
});

test("a step chart showing its newest step follows a left-half pull with no end, taking in steps a reply adds mid-pull; one showing a closed window keeps its end", () => {
  const zoom = stepOpen => {
    const p = page(), st = p.create("st", [0, 25, 50, 75, 100], { axis: "step", stepOpen });
    pullAxis(p, "st", 100, 150, "held");
    st.setData(chartData([0, 50, 100, 150, 200]));
    releaseAxis(p, 150);
    return [xWindow(st).map(nanostep), p.zooms("st").map(z => z.map(nanostep))];
  };
  // Grabbed at 25, held at 150 px: [-20, 100] on the press's data, then [-80, 200] on the reply's.
  assert.deepEqual(zoom(true), [[-80, 200], [[-80, null]]]);
  assert.deepEqual(zoom(false), [[-20, 100], [[-20, 100]]]);
  // A closed one's fixed pull widens its pinned end, the last bucket's x, to the coverage that bucket had at the press, though a reply mid-pull adds a bucket past it.
  const b = page(), bk = b.create("bk", [5, 15, 25], { axis: "step", stepOpen: false, xr: [[0, 10, 20], [9, 19, 29]] });
  pullAxis(b, "bk", 100, 150, "held");
  bk.setData(chartData([5, 15, 25, 35], [[0, 10, 20, 30], [9, 19, 29, 39]]));
  releaseAxis(b, 150);
  assert.equal(b.zooms("bk")[0][1], 29);
  // A log step chart, plotted 1 step late, dispatches real steps.
  const q = page(), ls = q.create("ls", [1, 2, 11, 101], { axis: "step", logX: true, shifted: true, stepOpen: true });
  pullAxis(q, "ls", 100, 150);
  const [[lo, hi]] = q.zooms("ls");
  assert.deepEqual([ls.scales.x.max, hi], [101, null]);
  near(lo, ls.scales.x.min - 1);
});

test("a step chart showing a window an earlier pull left, its refetch pending, pins pulls and selections to the edges shown, not its answer's", () => {
  const zooms = p => p.zooms("st").map(z => z.map(nanostep));
  // A right-half pull narrows the window to [0, 85.71]; the answer still reaches step 100 and had no right bound.
  let p = page(), st = p.create("st", [0, 25, 50, 75, 100], { axis: "step", stepOpen: true });
  pullAxis(p, "st", 300, 350);
  const hi = nanostep(xWindow(st)[1]);
  // A cancelled pull leaves that window as it was, though its start is the data's.
  pullAxis(p, "st", 100, 150, "escape");
  assert.deepEqual(xWindow(st).map(nanostep), [0, hi]);
  // A left-half pull then fixes, its pinned end as shown, and a selection to the right edge keeps an end too.
  pullAxis(p, "st", 100, 150);
  const lo = nanostep(xWindow(st)[0]);
  dragSelect(p, "st", 200, 405);
  const [first, pulled, selected] = zooms(p);
  assert.deepEqual([first, pulled], [[0, hi], [lo, hi]]);
  assert.notEqual(selected[1], null);
  // A left-half pull, which follows, leaves [14.29, 100]; a right-half pull then keeps that start.
  p = page();
  st = p.create("st", [0, 25, 50, 75, 100], { axis: "step", stepOpen: true });
  pullAxis(p, "st", 100, 50);
  const start = nanostep(xWindow(st)[0]);
  pullAxis(p, "st", 300, 350);
  assert.deepEqual(zooms(p).map(z => z[0]), [start, start]);
  assert.ok(start > 0);
});

test("a step chart's cancel, or a release back at its press, after a reply mid-hold ranges all its data, a lone bucket's included, so the next pull or selection to its right edge still follows", () => {
  // A reply to the open answer, its x column tagged as push_data_to_js tags it.
  const open = d => { d[0].stepOpen = true; return d; };
  for (const end of ["escape", "back", "click"]) {
    const p = page(), st = p.create("st", [0, 50, 100], { axis: "step", stepOpen: true });
    if (end === "click") pressAxis(p, "st", 100);
    else pullAxis(p, "st", 100, 150, "held");
    if (end === "back") moveAxis(p, 100.5);
    st.setData(open(chartData([0, 50, 100, 150, 200])));
    if (end === "escape") p.fireWindow("keydown", { key: "Escape" });
    else releaseAxis(p, end === "back" ? 100.5 : 100);
    assert.deepEqual(xWindow(st), [0, 200], end);
    pullAxis(p, "st", 100, 150);
    dragSelect(p, "st", 200, 405);
    assert.deepEqual(p.zooms("st").map(z => z[1]), [null, null], end);
  }
  // A lone bucket's window is uPlot's widening of it, not an earlier pull's: it restores unzoomed too, on the data held then.
  for (const end of ["escape", "back"]) {
    const p = page(), one = p.create("one", [500], { axis: "step", stepOpen: true });
    pullAxis(p, "one", 100, 150, "held");
    if (end === "back") moveAxis(p, 100.5);
    one.setData(open(chartData([500, 600, 700])));
    if (end === "escape") p.fireWindow("keydown", { key: "Escape" });
    else releaseAxis(p, 100.5);
    assert.deepEqual(xWindow(one), [500, 700], end);
    pullAxis(p, "one", 100, 150);
    assert.deepEqual(p.zooms("one").map(z => z[1]), [null], end);
  }
});

test("a fixed step pull never narrows the pinned edge it showed, so a lone bucket's widened window stays a window", () => {
  for (const [from, to] of [[100, 150], [100, 50], [300, 350]]) {
    const p = page(), one = p.create("one", [500], { axis: "step", stepOpen: true });
    const shown = xWindow(one);
    pullAxis(p, "one", from, to);
    const [[lo, hi]] = p.zooms("one");
    assert.ok(lo < hi, `${from} to ${to}: [${lo}, ${hi}]`);
    if (from < 200) assert.equal(hi, shown[1]);
    else assert.equal(lo, shown[0]);
  }
});

test("a right-half pull, or a left-half pull on a window not ending exactly at the newest point, is fixed: a reply mid-pull leaves its window, from the press on", () => {
  const p = page(), c = p.create("g", [0, 100, 200, 300, 400]);
  // Pressed, not yet moved: the reply doesn't re-range the unzoomed chart under the hand.
  pressAxis(p, "g", 300);
  c.setData(chartData([0, 200, 400, 600, 800]));
  assert.deepEqual(xWindow(c), [0, 400]);
  moveAxis(p, 350);
  const pulled = xWindow(c);
  c.setData(chartData([0, 300, 600, 900, 1200]));
  assert.deepEqual(xWindow(c), pulled);
  releaseAxis(p, 350);
  c.setData(chartData([0, 400, 800, 1200, 1600]));
  assert.deepEqual([...xWindow(c), c.__kymo_userzoom], [...pulled, true]);
  // Zoomed to [100, 300] of data reaching 1600.
  zoomTo(c, 100, 300);
  pullAxis(p, "g", 100, 150);
  const fixed = xWindow(c);
  assert.equal(fixed[1], 300);
  c.setData(chartData([0, 500, 1000, 1500, 2000]));
  assert.deepEqual([...xWindow(c), c.__kymo_userzoom], [...fixed, true]);
  // A window reaching past its data fixes a left-half pull too, rather than its far edge jumping to the newest x at the first move: zoomed to [0, 6000] past data ending at 2000, or a lone x value, which uPlot widens.
  zoomTo(c, 0, 6000);
  pullAxis(p, "g", 100, 101);
  c.setData(chartData([0, 500, 1000, 1500, 2000]));
  assert.deepEqual([xWindow(c)[1], c.__kymo_userzoom], [6000, true]);
  for (const logX of [false, true]) {
    const lone = p.create(`one${logX}`, [500], { axis: "m-x", logX });
    const widened = xWindow(lone);
    pullAxis(p, `one${logX}`, 100, 101);
    assert.deepEqual([xWindow(lone)[1], lone.__kymo_userzoom], [widened[1], true], `log x ${logX}`);
  }
  // On a step chart too, a reply mid-pull leaves the window (AI-1533), and a release before another move asks the step store for the pulled window, marking no client zoom.
  const q = page(), st = q.create("st", [0, 25, 50, 75, 100], { axis: "step" });
  pullAxis(q, "st", 300, 350, "held");
  const held = xWindow(st);
  st.setData(chartData([0, 50, 100, 150, 200]));
  assert.deepEqual(xWindow(st), held);
  releaseAxis(q, 350);
  assert.deepEqual([q.zooms("st"), st.__kymo_userzoom], [[[0, held[1]]], undefined]);
});

test("a plain click on the strip commits nothing; a release back at the press, or a cancel, restores what the press found, on the data held at the exit", () => {
  const following = () => {
    const p = page(), c = p.create("g", [0, 100, 200, 300, 400]);
    dragSelect(p, "g", 200, 405);
    return [p, c];
  };
  let [p, c] = following();
  // A plain click on the strip commits and draws nothing.
  const commits = [c.rebuilds, c.redraws];
  pressAxis(p, "g", 300);
  releaseAxis(p, 300);
  assert.deepEqual([c.rebuilds, c.redraws], commits);
  // A hand back within a pixel of its press along the axis, though 6 px across it, after 40 px of travel, gets the press's window back exactly, still following.
  pullAxis(p, "g", 300, 340, "held");
  p.fireWindow("mousemove", { buttons: 1, clientX: 300.5, clientY: 216, pageX: 300.5, pageY: 216 });
  releaseAxis(p, 300.5);
  assert.deepEqual([...xWindow(c), c.__kymo_userzoom], [200, 400, "follow"]);
  c.setData(chartData([0, 200, 400, 600, 800]));
  assert.deepEqual([...xWindow(c), c.__kymo_userzoom], [200, 800, "follow"]);
  // So does a fixed pull, here a left-half one on a window reaching past its data; the peer is untouched.
  const o = page(), g = o.create("g", [0, 100, 200, 300, 400]), peer = o.create("h", [0, 100, 200, 300, 400]);
  for (const chart of [g, peer]) zoomTo(chart, 0, 1200);
  pullAxis(o, "g", 100, 140, "held");
  assert.notEqual(xWindow(g)[0], 0);
  moveAxis(o, 100.5);
  releaseAxis(o, 100.5);
  for (const chart of [g, peer]) assert.deepEqual([...xWindow(chart), chart.__kymo_userzoom], [0, 1200, true]);
  // A step chart gets back the window it pressed on, though it isn't its data's extent (a refetch for an earlier pull pending), on a cancel and after a reply mid-hold.
  const t = page(), st = t.create("st", [0, 25, 50, 75, 100], { axis: "step", stepOpen: true });
  st.setScale("x", { min: -20, max: 100 });
  pullAxis(t, "st", 300, 350, "escape");
  assert.deepEqual(xWindow(st), [-20, 100]);
  pullAxis(t, "st", 100, 100.5, "held");
  st.setData(chartData([0, 50, 100, 150, 200]));
  releaseAxis(t, 100.5);
  assert.deepEqual([xWindow(st), t.zooms("st")], [[-20, 100], []]);
  // A reply mid-hold moves a following pull's window; released without travel, it is restored on the reply's data: a following window from its start.
  [p, c] = following();
  pullAxis(p, "g", 100, 100.5, "held");
  c.setData(chartData([0, 200, 400, 600, 800]));
  releaseAxis(p, 100.5);
  assert.deepEqual([...xWindow(c), c.__kymo_userzoom], [200, 800, "follow"]);
  // Pressed on the right half and never moved, a fixed pull keeps its window through a reply; released, it restores the zoom on the reply's data: an unzoomed chart over all of it.
  const r = page(), still = r.create("s", [0, 100, 200, 300, 400]);
  pressAxis(r, "s", 300);
  still.setData(chartData([0, 200, 400, 600, 800]));
  assert.deepEqual(xWindow(still), [0, 400]);
  releaseAxis(r, 300.5);
  assert.deepEqual(xWindow(still), [0, 800]);
  assert.ok(!still.__kymo_userzoom);
  // Cancelled after a reply, a following chart follows from its start on the data held now, and an unzoomed one ranges that data.
  [p, c] = following();
  pullAxis(p, "g", 300, 350, "held");
  c.setData(chartData([0, 200, 400, 600, 800]));
  p.fireWindow("keydown", { key: "Escape" });
  assert.deepEqual([...xWindow(c), c.__kymo_userzoom], [200, 800, "follow"]);
  const q = page(), u = q.create("u", [0, 100, 200, 300, 400]);
  pullAxis(q, "u", 100, 150, "held");
  u.setData(chartData([-100, 200, 400, 600, 800]));
  q.fireWindow("keydown", { key: "Escape" });
  assert.deepEqual(xWindow(u), [-100, 800]);
  assert.ok(!u.__kymo_userzoom);
});

test("an axis pull holds the y gutter at its pressed width, and every exit re-fits it: a release only when its labels changed width", () => {
  // Labels 7 px a character, in CSS px at any device pixel ratio: "1" needs the 35 px floor, "1000" 39, "10000" 46, "0.0001" 53.
  const labelled = (labels, dpr = 1, axis = "rel") => {
    const p = page({ dpr }), c = p.create("g", [0, 100, 200, 300, 400], { axis });
    c.axes[1]._values = labels;
    c.layout();
    return [p, c];
  };
  // A pull pressed at page x `press` on a chart labelled `from`, moved 10 px, then 10 more after its labels became `to`, and ended by `end`: the redraws and the gutter after.
  const pull = ({ end = "mouseup", from = ["0", "1000"], to, press = 100, dpr = 1, axis }) => {
    const [p, c] = labelled(from, dpr, axis);
    const held = c.axes[1]._size;
    pullAxis(p, "g", press, press + 10, "held");
    c.axes[1]._values = to;
    moveAxis(p, press + 20);
    assert.equal(c.axes[1]._size, held, "held while the pull owns the chart");
    if (end === "mouseup") releaseAxis(p, press + 20);
    else p.fireWindow("keydown", { key: "Escape" });
    return [c.redraws, c.axes[1]._size];
  };
  assert.deepEqual(pull({ to: ["0", "0.0001"] }), [1, 53]);
  assert.deepEqual(pull({ to: ["0", "0.0001"], dpr: 2 }), [1, 53]);
  assert.deepEqual(pull({ to: ["0", "0.0001"], press: 300 }), [1, 53], "a right-half pull");
  assert.deepEqual(pull({ to: ["0", "0.0001"], axis: "step" }), [1, 53], "a step chart, whose release refetches and changes no scale");
  assert.deepEqual(pull({ from: ["0", "10000"], to: ["0", "1"] }), [1, 35], "narrower labels");
  assert.deepEqual(pull({ to: ["0", "2000"] }), [0, 39]);
  assert.deepEqual(pull({ end: "escape", to: ["0", "2000"] }), [1, 39]);
  // A reply under a still hand on a fixed pull changes the labels and no x: held, then re-fitted by the cancel, whose restore changes no scale.
  const [o, f] = labelled(["0", "1000"]);
  pressAxis(o, "g", 300);
  f.axes[1]._values = ["0", "0.0001"];
  f.setData(chartData([0, 100, 200, 300, 400]));
  assert.equal(f.axes[1]._size, 39);
  o.fireWindow("keydown", { key: "Escape" });
  assert.deepEqual([f.redraws, f.axes[1]._size], [1, 53]);
  // A selection's owner, on the same chart, holds nothing: the gutter fits its labels mid-drag.
  const [p, c] = labelled(["0", "1000"]);
  const at = selectAt(p, "g");
  at("mousedown", 100);
  c.axes[1]._values = ["0", "0.0001"];
  at("mousemove", 200);
  c.layout();
  assert.equal(c.axes[1]._size, 53);
  at("mouseup", 200);
  // A peer's reply mid-pull is the peer's own: it ranges its own data and fits its own labels, and the pulled chart keeps its window.
  const q = page(), g = q.create("g", [0, 100, 200, 300, 400]), h = q.create("h", [0, 100, 200, 300, 400]);
  pullAxis(q, "g", 100, 150, "held");
  const pulled = xWindow(g);
  h.axes[1]._values = ["0", "0.0001"];
  h.setData(chartData([0, 200, 400, 600, 800]));
  assert.deepEqual([xWindow(h), h.axes[1]._size, xWindow(g)], [[0, 800], 53, pulled]);
});

test("a step chart showing its newest step zooms to its right edge with no end, for the store to leave open; one showing a closed window keeps its end", () => {
  const selections = [[100, 300], [100, 399.5], [405, 100], [-5, 300]];
  const zooms = stepOpen => {
    const p = page();
    p.create("st", [0, 25, 50, 75, 100], { axis: "step", stepOpen });
    for (const [press, release] of selections) dragSelect(p, "st", press, release);
    return p.zooms("st");
  };
  assert.deepEqual(zooms(true), [[25, 75], [25, null], [25, null], [-1.25, 75]]);
  assert.deepEqual(zooms(false), [[25, 75], [25, 100], [25, 101.25], [-1.25, 75]]);
});

test("a selection that moves under 3 px, or lies wholly past one edge, paints no box and zooms nothing", () => {
  const p = page();
  const c = p.create("g", [0, 100, 200, 300, 400]);
  const at = selectAt(p, "g");
  at("mousedown", 200);
  at("mousemove", 202);
  assert.equal(c.box.width, 0);
  at("mouseup", 202);
  assert.deepEqual([c.scales.x.min, c.scales.x.max], [0, 400]);
  // Both ends past the right edge are its x max, which makes no window, so the chart keeps following live data.
  at("mousedown", 405);
  at("mousemove", 420);
  assert.equal(c.box.width, 0);
  at("mouseup", 420);
  assert.deepEqual([c.scales.x.min, c.scales.x.max], [0, 400]);
  assert.ok(!c.__kymo_userzoom);
});

test("Escape or a lost focus cancels a drag; travel latches the click a release makes, from 1 px on a pull; a move with the button up ends a drag as its release would", () => {
  const p = page(), c = p.create("g", [0, 100, 200, 300, 400]), at = selectAt(p, "g");
  at("mousedown", 100);
  at("mousemove", 300);
  p.fireWindow("keydown", { key: "Escape" });
  assert.deepEqual([...xWindow(c), c.__kymo_userzoom, p.window.__kymo_zg], [0, 400, undefined, null]);
  // Travel latches the click that ends a drag, though the hand came back, so the click can't reset a zoom.
  at("mousedown", 100);
  at("mousemove", 300);
  at("mousemove", 100);
  at("mouseup", 100);
  assert.deepEqual([...xWindow(c), c.__kymo_userzoom, p.el("g").__kymo_skipUnzoomClick], [0, 400, undefined, true]);
  // A mouseup outside the window never arrives: the next move, its button up, commits at the last place held.
  at("mousedown", 100);
  at("mousemove", 300);
  p.fireWindow("mousemove", { buttons: 0, clientX: 350, clientY: 100, pageX: 350, pageY: 100 });
  assert.deepEqual([...xWindow(c), c.__kymo_userzoom, p.window.__kymo_zg], [100, 300, true, null]);
  pullAxis(p, "g", 300, 350, "held");
  const pulled = xWindow(c);
  p.fireWindow("mousemove", { buttons: 0, clientX: 380, clientY: 210, pageX: 380, pageY: 210 });
  assert.deepEqual([...xWindow(c), p.window.__kymo_zg, p.listening()], [...pulled, null, 0]);
  // A lost window focus cancels a pull as Escape does.
  pullAxis(p, "g", 300, 350, "held");
  p.fireWindow("blur", {});
  assert.deepEqual([...xWindow(c), p.window.__kymo_zg, p.listening()], [...pulled, null, 0]);
  // A page scroll mid-pull moves the plot under a still pointer, which the pull takes as travel along the plot, as a selection does.
  const scrolled = page(), sc = scrolled.create("g", [0, 100, 200, 300, 400]), unscrolled = page(), un = unscrolled.create("g", [0, 100, 200, 300, 400]);
  pullAxis(unscrolled, "g", 100, 150, "held");
  pressAxis(scrolled, "g", 100);
  scrolled.window.scrollX = 50;
  scrolled.fireWindow("mousemove", { buttons: 1, clientX: 100, clientY: 210, pageX: 150, pageY: 210 });
  assert.deepEqual(xWindow(sc), xWindow(un));
  // A pull's 1 px of travel latches the click too, short of a selection's 3 px.
  pullAxis(p, "g", 300, 302);
  assert.deepEqual([c.__kymo_userzoom, p.el("g").__kymo_skipUnzoomClick], [true, true]);
});

test("an axis pull reads the hand against the plot's page x at its press, as a selection does: the plot's place in its box and the page's scroll count, and a chart that moves without resizing mid-pull leaves the window where the hand had it", () => {
  const p = page(), c = p.create("g", [0, 100, 200, 300, 400]);
  // The plot sits 70 px into the page, 40 px into uPlot's wrap, on a page scrolled 50 px.
  Object.assign(c.over, { pageLeft: 30, style: { left: "40px", width: "360px" } });
  p.window.scrollX = 50;
  const ev = x => ({ button: 0, buttons: 1, clientX: x, clientY: 210, pageX: x + 50, pageY: 210, preventDefault() {} });
  const valueAt = px => c.scales.x.min + px / 360 * (c.scales.x.max - c.scales.x.min);
  p.el("g").fire("mousedown", ev(100));
  const grabbed = valueAt(80);
  p.fireWindow("mousemove", ev(60));
  near(valueAt(40), grabbed, 1e-9);
  const held = xWindow(c);
  // A new metric's chart lands before it: the chart moves 100 px right at the same width, so no resize cancels the pull.
  p.el("g").left += 100;
  c.over.pageLeft += 100;
  p.fireWindow("mousemove", ev(60));
  assert.deepEqual(xWindow(c), held);
  p.fireWindow("mouseup", ev(60));
  assert.deepEqual([...xWindow(c), c.__kymo_userzoom, p.window.__kymo_zg], [...held, "follow", null]);
});

test("with single-click unzoom on, the click a cancelled drag's release makes resets nothing", () => {
  for (const gesture of ["select", "pull"]) {
    const p = page({ singleClick: true }), c = p.create("g", [0, 100, 200, 300, 400]), at = selectAt(p, "g");
    zoomTo(c, 100, 300);
    // Pressed, cancelled before any travel, released on the chart: its click is the drag's, not a reset.
    if (gesture === "select") at("mousedown", 200);
    else pressAxis(p, "g", 200);
    p.fireWindow("keydown", { key: "Escape" });
    at("mouseup", 200);
    p.el("g").fire("click", { detail: 1 });
    assert.deepEqual([...xWindow(c), c.__kymo_userzoom], [100, 300, true], gesture);
    // The next plain click resets, as ever.
    at("mousedown", 200);
    at("mouseup", 200);
    p.el("g").fire("click", { detail: 1 });
    assert.ok(!c.__kymo_userzoom, gesture);
  }
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
