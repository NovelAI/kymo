#!/usr/bin/env node

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const hoverScript = new vm.Script(
  readFileSync(new URL("../src/components/uplot_chart/hover_points.js", import.meta.url), "utf8"),
  { filename: "hover_points.js" },
);
function installHoverPoints(context) {
  hoverScript.runInContext(context);
  return context.window.__kymo_hp;
}
const { createHoverPointLookup, plotPosition } = installHoverPoints(vm.createContext({ window: {} }));
let passed = 0;
function test(name, run) {
  run();
  console.log(`ok ${++passed} - ${name}`);
}

test("browser startup exposes only the namespaced helpers and preserves them on reload", () => {
  const context = vm.createContext({ window: {} });
  const helper = installHoverPoints(context);
  assert.equal(typeof helper.createHoverPointLookup, "function");
  for (const name of ["hasHoverPoint", "createHoverPointLookup", "plotPosition", "KYMO_HOVER_POINTS"]) {
    assert.equal(vm.runInContext(`typeof ${name}`, context), "undefined", `${name} stays out of the global scope`);
  }
  assert.equal(installHoverPoints(context), helper);
  assert.equal(installHoverPoints(context).createHoverPointLookup, helper.createHoverPointLookup);
});

test("the shared pointer mapping reads the live overlay rect, snaps the outer pixel to exact bounds, and rejects moves outside the plot band", () => {
  const chart = { over: { getBoundingClientRect: () => ({ left: 100.25, top: 50, width: 235.390625, height: 200 }) } };
  const inside = plotPosition(chart, 150.25, 60);
  assert.deepEqual([inside.left, inside.top], [50, 10]);
  for (const x of [80, 100.25, 101.25]) assert.equal(plotPosition(chart, x, 60).left, 0, `x ${x} maps to the first column`);
  for (const x of [334.640625, 335.640625, 400]) assert.equal(plotPosition(chart, x, 60).left, 235.390625, `x ${x} maps to the last column`);
  assert.equal(plotPosition(chart, 150, 49.9), null, "above the plot band");
  assert.equal(plotPosition(chart, 150, 250.1), null, "below the plot band");
  assert.equal(plotPosition({ over: { getBoundingClientRect: () => ({ left: 0, top: 0, width: 0, height: 200 }) } }, 0, 10), null, "an unsized plot");
});

test("nearest is opt-in, measured by x, and keeps exact-column samples", () => {
  const lookup = createHoverPointLookup([
    [0, 90, 91, 92, 100], [7, null, null, null, 8],
  ], 1, 2, 0);
  assert.equal(lookup(0, 1, false), -1);
  assert.equal(lookup(0, 1, true), 4);
  assert.equal(lookup(0, 0, true), 0);
});

test("ties prefer earlier samples and sparse endpoints remain available", () => {
  const lookup = createHoverPointLookup([
    [-1, 0, 5, 10, 11], [null, 7, null, 8, null],
  ], 1, 2, 0);
  assert.equal(lookup(0, 2, true), 1);
  assert.equal(lookup(0, 0, true), 1);
  assert.equal(lookup(0, 4, true), 3);
});

test("marker-only samples are real points and empty runs remain absent", () => {
  const lookup = createHoverPointLookup([
    [0, 1, 2], [null, null, null], [null, null, null],
    [null, 2, null], [null, null, null],
  ], 1, 3, 2);
  assert.equal(lookup(0, 1, false), 1);
  assert.equal(lookup(0, 0, true), 1);
  assert.equal(lookup(1, 0, true), -1);
});

test("fallback indices are cached instead of rescanning gaps per hover", () => {
  let reads = 0;
  const values = new Proxy([1, null, null, null, 2], {
    get(target, key) {
      if (/^\d+$/.test(String(key))) reads++;
      return target[key];
    },
  });
  const lookup = createHoverPointLookup([[0, 1, 2, 3, 4], values], 1, 2, 0);
  lookup(0, 2, true);
  reads = 0;
  lookup(0, 3, true);
  assert.ok(reads <= 2, `cached fallback read ${reads} value slots`);
});

// Run the complete production hook with browser/uPlot boundaries mocked.
const hookSource = readFileSync(new URL("../src/components/uplot_chart/hover.js", import.meta.url), "utf8");
const hookScript = new vm.Script(`(${hookSource}\n)`, { filename: "hover.js" });

// Inspect the generated rows as cells, so moving metadata back into a value cannot pass on matching text alone.
function tooltipRows(tip) {
  return Array.from(tip.innerHTML.matchAll(/<div class="kymo-tip-row" data-r="(\d+)">([\s\S]*?)<\/div>/g), match => ({
    series: Number(match[1]),
    cells: Array.from(match[2].matchAll(/<span class="kymo-tip-([^" ]+)[^"]*"(?: style="([^"]*)")?>([^<]*)<\/span>/g), cell => ({
      role: cell[1], style: cell[2] ?? "", text: cell[3],
    })),
  }));
}
function tooltipCell(tip, series, role) {
  const row = tooltipRows(tip).find(row => row.series === series);
  assert.ok(row, `tooltip has series ${series}`);
  const cell = row.cells.find(cell => cell.role === role);
  assert.ok(cell, `series ${series} has a ${role} cell`);
  return cell;
}

function fixture(overrides = {}) {
  let enabled = null;
  let markup = "";
  let builds = 0;
  const tip = {
    style: {}, classList: { toggle() {} }, offsetWidth: 100, offsetHeight: 50,
    get innerHTML() { return markup; },
    set innerHTML(value) { markup = value; builds++; },
  };
  const hotpt = { style: {} };
  const el = {
    id: "chart-fixture",
    closest() { return null; },
    getBoundingClientRect() { return { left: 0, right: 500, top: 0, bottom: 200 }; },
  };
  const window = {
    innerWidth: 1000, innerHeight: 800, __kymo_hoversrc: el.id,
    __kymo_setHl(run, name, chartId) { this.hot = { run, name, chartId }; },
    __kymo_retagRows() {},
  };
  const context = {
    document: { documentElement: { getAttribute(name) { return name === "data-kymo-show-nearest-point" ? enabled : null; } } },
    window, el, tip, hotpt, devicePixelRatio: 1, createHoverPointLookup,
    labels: ["dense", "sparse"], colors: ["#ff0000", "#0000ff"],
    runIds: ["dense-run", "sparse-run"], runNames: ["Dense run", "Resumed run"], lineBase: 1,
    nanBase: 3, nanCols: 0, markR: 3, markGap: 2,
    hasRange: false, hasRaw: false, hasXr: false, isSmoothed: false,
    rawStride: 0, xrBase: 3, xnanC: [0, 0], readoutXShift: 0,
    fmtX: value => String(value), ...overrides,
  };
  const hook = hookScript.runInNewContext(context);
  const chart = {
    data: [[0, 10, 20], [100, 100, 100], [5, null, 8]],
    cursor: { idx: 1, left: 10, top: 5 },
    bbox: { left: 0, top: 0, width: 500, height: 200 },
    scales: { x: { min: 0, max: 500 }, y: { min: 0, max: 200 } },
    valToPos: value => value,
  };
  return {
    chart, tip, hotpt, window, context,
    run() { hook(chart); },
    enable(value = "true") { enabled = value; },
    get builds() { return builds; },
  };
}

test("nearest readouts do not change the highlighted run or marker", () => {
  const f = fixture();
  f.run();
  assert.ok(!f.tip.innerHTML.includes("sparse"));
  assert.equal(f.window.hot.run, "dense-run");
  assert.equal(f.window.hot.chartId, "chart-fixture", "chart hover scopes line highlighting to the hovered chart");
  f.enable();
  f.run();
  assert.equal(tooltipCell(f.tip, 1, "val").text, "5.000");
  assert.equal(tooltipCell(f.tip, 1, "x").text, "@ 0");
  assert.equal(f.window.hot.run, "dense-run", "a borrowed point cannot steal highlight even when nearer to the pointer");
  assert.equal(f.window.hot.name, "Dense run");
  assert.equal(f.hotpt.style.left, "10px");
  assert.equal(f.hotpt.style.top, "100px");
  const builds = f.builds;
  f.run();
  assert.equal(f.builds, builds, "steady hover reuses tooltip markup");
  f.enable("false");
  f.run();
  assert.ok(!f.tip.innerHTML.includes("sparse"));
});

test("production source and synced readouts use the same fallback and annotation", () => {
  const f = fixture();
  f.enable();
  f.run();
  assert.deepEqual(tooltipRows(f.tip).map(row => row.cells.map(cell => cell.role)), [["name", "val", "x"], ["name", "val", "x"]]);
  assert.equal(tooltipCell(f.tip, 0, "val").text, "100.0");
  assert.equal(tooltipCell(f.tip, 0, "x").text, "", "exact-column rows reserve the metadata cell");
  assert.equal(tooltipCell(f.tip, 1, "val").text, "5.000");
  assert.equal(tooltipCell(f.tip, 1, "x").text, "@ 0");
  assert.equal(tooltipRows(f.tip)[1].cells[0].style, "color:#0000ff", "source marker remains on the colored name");
  assert.equal(tooltipCell(f.tip, 1, "x").style, "", "metadata uses its muted CSS color");
  assert.match(f.tip.innerHTML, /grid-template-columns:minmax\(0,max-content\) max-content max-content/);
  delete f.window.hot;
  f.window.__kymo_hoversrc = "peer";
  f.run();
  assert.deepEqual(tooltipRows(f.tip).map(row => row.cells.map(cell => cell.role)), [["val", "x"], ["val", "x"]]);
  assert.equal(tooltipCell(f.tip, 0, "x").text, "");
  assert.equal(tooltipCell(f.tip, 1, "val").text, "5.000");
  assert.equal(tooltipCell(f.tip, 1, "x").text, "@ 0");
  assert.equal(tooltipRows(f.tip)[1].cells[0].style, "color:#0000ff", "synced marker remains on the colored value");
  assert.match(f.tip.innerHTML, /grid-template-columns:max-content max-content/);
  assert.ok(!f.tip.innerHTML.includes("kymo-tip-step"));
  assert.equal(f.window.hot, undefined, "synced charts do not choose highlight");
});

test("default-off and exact-only readouts omit the metadata column", () => {
  for (const source of [true, false]) for (const withRaw of [false, true]) {
    const f = fixture(withRaw ? { hasRaw: true, rawStride: 1, lineBase: 3 } : {});
    if (withRaw) {
      f.chart.data = [[0, 10, 20], [90, 90, 90], [4, null, 7], [100, 100, 100], [5, null, 8]];
    }
    if (!source) f.window.__kymo_hoversrc = "peer";
    for (const enabled of [false, true]) {
      if (enabled) {
        f.enable();
        f.chart.cursor.idx = 0;
      }
      f.run();
      const rows = tooltipRows(f.tip);
      assert.equal(rows.length, enabled ? 2 : 1);
      for (const row of rows) assert.deepEqual(row.cells.map(cell => cell.role), source ? (withRaw ? ["name", "val", "raw"] : ["name", "val"]) : ["val"]);
      const columns = f.tip.innerHTML.match(/grid-template-columns:([^"]+)/)?.[1];
      assert.equal(columns, source ? (withRaw ? "minmax(0,max-content) max-content max-content" : "minmax(0,max-content) max-content") : "max-content");
    }
  }
});

test("source and synced borrowed buckets show their real, unshifted x range", () => {
  for (const shift of [0, 1, 0.001]) {
    const f = fixture({ hasXr: true, readoutXShift: shift });
    f.enable();
    f.chart.data = [
      [5 + shift, 10 + shift, 20 + shift], [100, 100, 100], [5, null, 8],
      [2, 9, 18], [8, 11, 22],
    ];
    f.run();
    assert.match(f.tip.innerHTML, /kymo-tip-step">9–11<\/div>/);
    assert.equal(tooltipCell(f.tip, 1, "val").text, "5.000");
    assert.equal(tooltipCell(f.tip, 1, "x").text, "@ 2–8");
    assert.equal(f.hotpt.style.left, `${10 + shift}px`, "marker stays at the hovered bucket center");
    f.window.__kymo_hoversrc = "peer";
    f.run();
    assert.equal(tooltipCell(f.tip, 1, "val").text, "5.000");
    assert.equal(tooltipCell(f.tip, 1, "x").text, "@ 2–8");
    assert.ok(!f.tip.innerHTML.includes("kymo-tip-step"));
  }
});

test("single-point headers and borrowed labels undo rendering shifts", () => {
  for (const shift of [1, 0.001]) {
    const f = fixture({ hasXr: true, readoutXShift: shift });
    f.enable();
    f.chart.data = [
      [shift, 10 + shift, 20 + shift], [100, 100, 100], [5, null, 8],
      [0, null, 20], [0, null, 20],
    ];
    f.run();
    assert.match(f.tip.innerHTML, /kymo-tip-step">10<\/div>/);
    assert.equal(tooltipCell(f.tip, 1, "val").text, "5.000");
    assert.equal(tooltipCell(f.tip, 1, "x").text, "@ 0");
  }
});

test("setData invalidates both the point index and tooltip markup", () => {
  const f = fixture();
  f.enable();
  f.run();
  f.chart.data = [[0, 10, 20], [100, 100, 100], [null, null, 8]];
  f.run();
  assert.equal(tooltipCell(f.tip, 1, "val").text, "8.000");
  assert.equal(tooltipCell(f.tip, 1, "x").text, "@ 20");
  assert.equal(f.hotpt.style.left, "10px");
  assert.equal(f.window.hot.run, "dense-run");
});

test("fresh data clears empty results and remeasures tooltip content", () => {
  const f = fixture();
  f.window.innerWidth = 500;
  f.chart.cursor.left = 270;
  f.chart.valToPos = (value, axis) => axis === "x" ? value * 27 : value;
  f.tip.offsetWidth = 200;
  f.run();
  assert.equal(f.tip.style.left, "284px");
  const populatedBuilds = f.builds;

  f.chart.data = [[0, 10, 20], [100, null, 100], [5, null, 8]];
  f.run();
  assert.equal(f.tip.style.display, "none");
  assert.equal(f.window.hot.run, null);
  f.run();
  assert.equal(f.builds, populatedBuilds, "empty hover does not rebuild markup");

  f.chart.data = [[0, 10, 20], [100, 7, 100], [5, 9, 8]];
  f.tip.offsetWidth = 240;
  f.run();
  assert.equal(f.tip.style.display, "block");
  assert.equal(tooltipCell(f.tip, 0, "val").text, "7.000");
  assert.equal(tooltipCell(f.tip, 1, "val").text, "9.000");
  assert.equal(f.builds, populatedBuilds + 1);
  assert.equal(f.tip.style.left, "16px", "new content width changes the placement");
  f.run();
  assert.equal(f.builds, populatedBuilds + 1, "steady hover reuses the new markup");
});

test("one data snapshot still rebuilds for source role and viewport changes", () => {
  const f = fixture();
  f.enable();
  f.run();
  const data = f.chart.data;
  f.window.__kymo_hoversrc = "peer";
  f.run();
  assert.equal(f.builds, 2);
  assert.ok(!f.tip.innerHTML.includes("kymo-tip-name"));
  f.window.__kymo_hoversrc = f.context.el.id;
  f.run();
  assert.equal(f.builds, 3);
  assert.ok(f.tip.innerHTML.includes("kymo-tip-name"));
  f.window.innerWidth -= 100;
  f.run();
  assert.equal(f.builds, 4, "viewport width can change the name cap");
  f.run();
  assert.equal(f.builds, 4);
  assert.equal(f.chart.data, data, "only presentation inputs changed");
});

test("borrowed raw and envelope values come from the selected sample", () => {
  const f = fixture({ hasRaw: true, hasRange: true, rawStride: 3, lineBase: 7 });
  f.enable();
  f.chart.data = [
    [0, 10, 20],
    [90, 90, 90], [80, 80, 80], [110, 110, 110],
    [4, null, 7], [3, null, 6], [6, null, 9],
    [100, 100, 100], [5, null, 8],
  ];
  f.run();
  assert.deepEqual(tooltipRows(f.tip).map(row => row.cells.map(cell => cell.role)), [["name", "val", "raw", "x"], ["name", "val", "raw", "x"]]);
  assert.equal(tooltipCell(f.tip, 1, "val").text, "5.000");
  assert.equal(tooltipCell(f.tip, 1, "raw").text, "3.000–6.000");
  assert.equal(tooltipCell(f.tip, 1, "x").text, "@ 0");
  f.chart.data = f.chart.data.map(column => column.slice());
  f.chart.data[5][0] = f.chart.data[6][0] = 4;
  f.run();
  assert.equal(tooltipCell(f.tip, 1, "raw").text, "4.000");
});

test("borrowed smoothing evidence uses collapsed envelopes and raw-only columns", () => {
  const f = fixture({ hasRange: true, isSmoothed: true, rawStride: 2, lineBase: 5 });
  f.enable();
  f.chart.data = [
    [0, 10, 20],
    [90, 90, 90], [90, 90, 90], [4, null, 7], [4, null, 7],
    [100, 100, 100], [5, null, 8],
  ];
  f.run();
  assert.equal(tooltipCell(f.tip, 1, "val").text, "5.000");
  assert.equal(tooltipCell(f.tip, 1, "raw").text, "4.000");
  assert.equal(tooltipCell(f.tip, 1, "x").text, "@ 0");

  const raw = fixture({ hasRaw: true, isSmoothed: true, rawStride: 1, lineBase: 3 });
  raw.enable();
  raw.chart.data = [
    [0, 10, 20], [90, 90, 90], [4, null, 7], [100, 100, 100], [5, null, 8],
  ];
  raw.run();
  assert.equal(tooltipCell(raw.tip, 1, "val").text, "5.000");
  assert.equal(tooltipCell(raw.tip, 1, "raw").text, "4.000");
  assert.equal(tooltipCell(raw.tip, 1, "x").text, "@ 0");
});

test("borrowed NaN and Infinity stay readable and exact-column markers stay selectable", () => {
  for (const [kind, label] of [[1, "NaN"], [2, "+∞"], [3, "-∞"]]) {
    const f = fixture({ nanCols: 2 });
    f.enable();
    f.chart.data = [
      [0, 10, 20], [100, 100, 100], [null, null, null],
      [null, null, null], [kind, null, 1],
    ];
    f.chart.cursor.top = kind === 2 ? 5 : 195;
    f.run();
    assert.equal(tooltipCell(f.tip, 1, "val").text, label);
    assert.equal(tooltipCell(f.tip, 1, "x").text, "@ 0");
    assert.equal(f.window.hot.run, "dense-run", "a borrowed non-finite marker cannot steal highlight");
    assert.equal(f.hotpt.style.display, "block");
    f.window.__kymo_hoversrc = "peer";
    f.run();
    assert.equal(tooltipCell(f.tip, 1, "val").text, label);
    assert.equal(tooltipCell(f.tip, 1, "x").text, "@ 0");
    assert.equal(tooltipCell(f.tip, 0, "x").text, "");
    f.window.__kymo_hoversrc = f.context.el.id;
    f.chart.cursor.idx = 0;
    f.chart.cursor.left = 0;
    f.run();
    assert.equal(f.window.hot.run, "sparse-run", "an exact-column non-finite marker remains selectable");
    assert.equal(f.hotpt.style.display, "none", "non-finite sample uses its hollow marker");
  }
});

test("kind-4 counts and log-time rendering shifts use the selected index", () => {
  const f = fixture({ nanCols: 2, xnanC: [0, 3], readoutXShift: 0.001 });
  f.enable();
  f.chart.data = [
    [0.001, 10.001, 20.001], [100, 100, 100], [5, null, 8],
    [null, null, null], [4, null, null],
  ];
  f.run();
  assert.equal(tooltipCell(f.tip, 1, "val").text, "5.000 ×3");
  assert.equal(tooltipCell(f.tip, 1, "x").text, "@ 0");
});

test("log charts borrow by data-x while highlighting the hovered column", () => {
  const f = fixture();
  f.enable();
  f.chart.data = [[1, 100, 1000], [100, 100, 100], [5, null, 8]];
  f.chart.cursor.left = 200;
  f.chart.valToPos = (value, axis) => axis === "x" ? Math.log10(value) * 100 : value;
  f.run();
  assert.equal(tooltipCell(f.tip, 1, "val").text, "5.000");
  assert.equal(tooltipCell(f.tip, 1, "x").text, "@ 1");
  assert.equal(f.window.hot.run, "dense-run");
  assert.equal(f.hotpt.style.left, "200px");
});

test("an empty hovered column keeps borrowed readouts without a highlight or marker", () => {
  const f = fixture();
  f.enable();
  f.chart.data = [[-100, 10, 20], [null, null, null], [5, null, null]];
  f.run();
  assert.equal(tooltipCell(f.tip, 1, "val").text, "5.000");
  assert.equal(tooltipCell(f.tip, 1, "x").text, "@ -100");
  assert.equal(f.window.hot.run, null, "readouts alone do not create a hover target in an empty column");
  assert.equal(f.hotpt.style.display, "none");
});

test("exact-column values outside the plot stay readable without painting into axes", () => {
  const f = fixture();
  f.run();
  assert.equal(f.hotpt.style.display, "block");
  f.chart.data = [[0, 10, 20], [100, 500, 100], [5, null, 8]];
  f.run();
  assert.equal(f.window.hot.run, "dense-run");
  assert.equal(tooltipCell(f.tip, 0, "val").text, "500.0");
  assert.equal(f.hotpt.style.top, "500px");
  assert.equal(f.hotpt.style.display, "none");
});

test("the hot dot shows on a sample exactly at the plot's edge even when bbox rounds below it", () => {
  const f = fixture();
  f.chart.scales = { x: { min: 0, max: 20 }, y: { min: 0, max: 200 } };
  f.chart.bbox = { left: 0, top: 0, width: 19.75, height: 200 };
  f.chart.cursor = { idx: 2, left: 20, top: 100 };
  f.run();
  assert.equal(f.window.hot.run, "dense-run");
  assert.equal(f.hotpt.style.display, "block");
});

console.log(`chart hover: ${passed} checks passed`);
