#!/usr/bin/env node

import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const {
  AXIS_LINEAR,
  AXIS_LOG,
  AXIS_LOG1P,
  buildFrozenSupports,
  buildFrozenSourceMap,
  mapSourcePageX,
  expandWholeBuckets,
} = require("../src/components/uplot_chart/zoom_math.js");

let passed = 0;
function test(name, run) {
  run();
  console.log(`ok ${++passed} - ${name}`);
}

function close(actual, expected) {
  const tolerance = 1e-9 * Math.max(1, Math.abs(expected));
  assert.ok(Math.abs(actual - expected) <= tolerance, `${actual} != ${expected}`);
}

function assertRange(frozen, a, b, lo, hi) {
  const range = expandWholeBuckets(frozen, a, b);
  assert.ok(range);
  assert.equal(range.lo, lo);
  assert.equal(range.hi, hi);
}

function sourceMap(overrides = {}) {
  const map = buildFrozenSourceMap({
    axis: AXIS_LINEAR,
    plotLeft: 100,
    width: 300,
    visualLo: 300,
    visualHi: 600,
    coverageLo: 300,
    coverageHi: 600,
    ...overrides,
  });
  assert.ok(map);
  return map;
}

test("the final envelope retains step 89999", () => {
  const frozen = buildFrozenSupports({
    plotXs: [127.5, 89_927.5],
    xrMin: [0, 89_856],
    xrMax: [255, 89_999],
  });
  assertRange(frozen, 45_000, 89_927.5, 45_000, 89_999);
});

test("source coordinates extrapolate 400 to 700", () => {
  const source = sourceMap();
  close(mapSourcePageX(source, 200), 400);
  close(mapSourcePageX(source, 500), 700);
});

test("outside extrapolation anchors to coverage rather than visual midpoints", () => {
  const map = sourceMap({ visualLo: 320, visualHi: 580 });
  close(mapSourcePageX(map, 400), 600);
  close(mapSourcePageX(map, 500), 700);
});

test("whole-bucket expansion does not cascade", () => {
  const frozen = buildFrozenSupports({
    plotXs: [5, 15],
    xrMin: [0, 10],
    xrMax: [10, 20],
  });
  assertRange(frozen, 5, 5, 0, 10);
});

test("a selection wholly in a gap is a no-op", () => {
  const frozen = buildFrozenSupports({
    plotXs: [5, 25],
    xrMin: [0, 20],
    xrMax: [10, 30],
  });
  assert.equal(expandWholeBuckets(frozen, 12, 18), null);
});

test("mixed raw and envelope slots preserve reversed extrapolation", () => {
  const frozen = buildFrozenSupports({
    plotXs: [5, 15, 25],
    xrMin: [NaN, 10, NaN],
    xrMax: [NaN, 20, NaN],
  });
  assertRange(frozen, 30, 5, 5, 30);
});

test("invalid bucket extents fall back to their plotted center", () => {
  const frozen = buildFrozenSupports({
    plotXs: [5],
    xrMin: [6],
    xrMax: [10],
  });
  assert.deepEqual(frozen.coverage, { lo: 5, hi: 5 });
});

test("non-finite selection endpoints fail closed", () => {
  const frozen = buildFrozenSupports({ plotXs: [5] });
  assert.equal(expandWholeBuckets(frozen, Number.NaN, 5), null);
});

test("log(x+1) maps shifted plots from real coverage", () => {
  const frozen = buildFrozenSupports({
    plotXs: [1, 1_000],
    xrMin: [0, 900],
    xrMax: [0, 999],
    xShift: 1,
  });
  assert.deepEqual(frozen.coverage, { lo: 0, hi: 999 });
  const map = sourceMap({
    axis: AXIS_LOG1P,
    plotLeft: 0,
    width: 100,
    visualLo: 0,
    visualHi: 999,
    coverageLo: 0,
    coverageHi: 999,
  });
  close(mapSourcePageX(map, 50), Math.sqrt(1_000) - 1);
  close(mapSourcePageX(map, 150), 1_000 ** 1.5 - 1);
});

test("plain log extrapolation advances by source decades", () => {
  const map = sourceMap({
    axis: AXIS_LOG,
    plotLeft: 0,
    width: 100,
    visualLo: 100,
    visualHi: 1_000,
    coverageLo: 100,
    coverageHi: 1_000,
  });
  close(mapSourcePageX(map, 50), Math.sqrt(100 * 1_000));
  close(mapSourcePageX(map, 200), 10_000);
});

test("log(x+1) extrapolation fails closed at the inverse-domain boundary", () => {
  const map = sourceMap({
    axis: AXIS_LOG1P,
    plotLeft: 0,
    width: 100,
    visualLo: 0,
    visualHi: 1,
    coverageLo: 0,
    coverageHi: 1,
  });
  assert.equal(mapSourcePageX(map, -200_000), null);
});

test("invalid source geometry fails closed", () => {
  assert.equal(
    buildFrozenSourceMap({
      axis: AXIS_LINEAR,
      plotLeft: 0,
      width: 0,
      visualLo: 0,
      visualHi: 1,
      coverageLo: 0,
      coverageHi: 1,
    }),
    null,
  );
});

console.log(`zoom math: ${passed} tests passed`);
