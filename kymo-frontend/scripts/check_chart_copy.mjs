#!/usr/bin/env node

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const helperUrl = new URL("../src/components/uplot_chart/copy.js", import.meta.url);
let copyListener;
let keydownListener;
let mousemoveListener;
let mouseoutListener;
let selectionCollapsed = true;
let copiedText = null;
let execMode = "dispatch";
let execCalls = 0;
let pointTarget = "chart";
let lastPos = null;
let plotRect = { left: 0, top: 0, width: 100, height: 100 };

const chartHost = {
  id: "chart",
  closest: () => chartHost,
};
const otherHost = {
  id: "other",
  closest: () => otherHost,
};
const chartHit = { closest: () => chartHost };
const otherHit = { closest: () => otherHost };
const outsideHit = { closest: () => null };

const body = {
  appendChild(node) {
    node.parentNode = body;
  },
};
globalThis.window = {
  __kymo_charts: {},
  __kymo_hoversrc: null,
  getSelection: () => ({ isCollapsed: selectionCollapsed }),
  addEventListener(type, listener, capture) {
    assert.equal(capture, true);
    if (type === "keydown") keydownListener = listener;
    else if (type === "mousemove") mousemoveListener = listener;
    else if (type === "mouseout") mouseoutListener = listener;
    else assert.fail(`unexpected window listener: ${type}`);
  },
};
globalThis.document = {
  activeElement: body,
  body,
  elementFromPoint() {
    return pointTarget === "chart"
      ? chartHit
      : pointTarget === "host"
        ? chartHost
        : pointTarget === "other"
          ? otherHit
          : outsideHit;
  },
  addEventListener(type, listener, capture) {
    assert.equal(type, "copy");
    assert.equal(capture, true);
    copyListener = listener;
  },
  createElement(tag) {
    assert.equal(tag, "textarea");
    return {
      value: "",
      readOnly: false,
      style: {},
      select() {
        document.activeElement = this;
      },
      setSelectionRange() {},
      remove() {
        this.parentNode = null;
      },
    };
  },
  execCommand(command) {
    assert.equal(command, "copy");
    execCalls += 1;
    if (execMode === "fail-first" && execCalls === 1) return false;
    if (document.activeElement?.value !== undefined) {
      copiedText = document.activeElement.value;
      return true;
    }
    const event = copyEvent();
    copyListener(event);
    return event.defaultPrevented;
  },
};

function copyEvent(target = body) {
  return {
    defaultPrevented: false,
    target,
    clipboardData: {
      setData(type, text) {
        assert.equal(type, "text/plain");
        copiedText = text;
      },
    },
    preventDefault() {
      this.defaultPrevented = true;
    },
  };
}

function keyEvent(overrides = {}) {
  return {
    defaultPrevented: false,
    repeat: false,
    isComposing: false,
    key: "c",
    code: "KeyC",
    ctrlKey: true,
    metaKey: false,
    altKey: false,
    shiftKey: false,
    target: body,
    preventDefault() {
      this.defaultPrevented = true;
    },
    ...overrides,
  };
}

const hoverPointsUrl = new URL("../src/components/uplot_chart/hover_points.js", import.meta.url);
vm.runInThisContext(readFileSync(fileURLToPath(hoverPointsUrl), "utf8"), {
  filename: fileURLToPath(hoverPointsUrl),
});
vm.runInThisContext(readFileSync(fileURLToPath(helperUrl), "utf8"), {
  filename: fileURLToPath(helperUrl),
});
assert.ok(copyListener, "copy listener was installed");
assert.ok(keydownListener, "Control+C listener was installed");
assert.ok(mousemoveListener, "pointer tracking listener was installed");
assert.ok(mouseoutListener, "pointer exit listener was installed");
mousemoveListener({ clientX: 10, clientY: 20 });

const chart = {
  root: { isConnected: true },
  over: { getBoundingClientRect: () => plotRect },
  cursor: { idx: 0 },
  posToIdx(pos) {
    lastPos = pos;
    return 0;
  },
  data: [
    [42],
    [1.25],
    [null],
    [null],
    [null],
    [null],
    [null],
    [2],
    [3],
    [null],
    [1],
  ],
  series: [
    {},
    { label: "=run\tone|x" },
    { label: "=run\tone|x" },
    { label: "<raw&>\nline" },
    { label: "=run\tone|x" },
    { label: "nan" },
  ],
  __kymo_copy: {
    xLabel: "=custom\tx",
    lineBase: 1,
    seriesCount: 5,
    nanBase: 6,
    nanCols: 5,
    xShift: 1,
  },
};
const expected =
  "'=custom x: 41\n\n" +
  "'=run one|x\t1.25\n" +
  "'=run one|x\tInfinity\n" +
  "<raw&> line\t-Infinity\n" +
  "nan\tNaN";
window.__kymo_charts.chart = chart;
window.__kymo_hoversrc = "chart";
assert.equal(window.__kymo_chartCopy.buildText(chart, 0), expected);

const logTime = {
  ...chart,
  data: chart.data.map((column) => column.slice()),
  __kymo_copy: {
    ...chart.__kymo_copy,
    xLabel: "time",
    xShift: 0.001,
  },
};
logTime.data[0][0] = 1.001;
assert.match(window.__kymo_chartCopy.buildText(logTime, 0), /^time: 1\n\n/);

// A kind-4 marker without a value (a run with no plottable x) has no sample at this x.
const anchorless = {
  ...chart,
  data: [[42], [1.25], [null], [null], [4]],
  series: [{}, { label: "a" }, { label: "n" }],
  __kymo_copy: { ...chart.__kymo_copy, seriesCount: 2, nanBase: 3, nanCols: 2 },
};
assert.equal(window.__kymo_chartCopy.buildText(anchorless, 0), "'=custom x: 41\n\na\t1.25");

let event = copyEvent();
copyListener(event);
assert.equal(copiedText, expected);
assert.equal(event.defaultPrevented, true);
assert.equal(lastPos, 10);

plotRect = { left: 120, top: 60, width: 100, height: 50 };
mousemoveListener({ clientX: 150, clientY: 80 });
lastPos = null;
event = copyEvent();
copyListener(event);
assert.equal(lastPos, 30, "copy converts viewport coordinates through the live plot rectangle");

mousemoveListener({ clientX: 90, clientY: 80 });
lastPos = null;
event = copyEvent();
copyListener(event);
assert.equal(lastPos, 0, "copy clamps a side-gutter hover to the first point");

for (const [clientX, pos] of [[120.6, 0], [219.5, 100]]) {
  mousemoveListener({ clientX, clientY: 80 });
  lastPos = null;
  event = copyEvent();
  copyListener(event);
  assert.equal(lastPos, pos, "copy snaps the outer pixel of the plot to its bound, like the hover cursor");
}

mousemoveListener({ clientX: 150, clientY: 111 });
copiedText = null;
event = copyEvent();
copyListener(event);
assert.equal(copiedText, null);
assert.equal(event.defaultPrevented, false, "copy rejects a pointer below the plot band");

plotRect = { left: 0, top: 0, width: 100, height: 100 };
mousemoveListener({ clientX: 10, clientY: 20 });

pointTarget = "host";
copiedText = null;
event = copyEvent();
copyListener(event);
assert.equal(copiedText, expected, "a pointer directly on the chart host is copyable");
pointTarget = "chart";

copiedText = null;
window.__kymo_hoversrc = null;
event = copyEvent();
copyListener(event);
assert.equal(copiedText, expected, "the physical chart is copyable without a hover source");
assert.equal(event.defaultPrevented, true);
window.__kymo_hoversrc = "chart";

pointTarget = "outside";
copiedText = null;
event = copyEvent();
copyListener(event);
assert.equal(copiedText, null);
assert.equal(event.defaultPrevented, false, "copy stays native when the pointer is outside charts");
pointTarget = "chart";

mouseoutListener({ relatedTarget: null });
copiedText = null;
event = copyEvent();
copyListener(event);
assert.equal(copiedText, null);
assert.equal(event.defaultPrevented, false, "leaving the window clears the last pointer position");
mousemoveListener({ clientX: 10, clientY: 20 });

const other = {
  ...chart,
  root: { isConnected: true },
  data: chart.data.map((column) => column.slice()),
  __kymo_copy: { ...chart.__kymo_copy },
};
other.data[0][0] = 7;
other.data[1][0] = 70;
other.series = chart.series.map((series) => ({ ...series }));
other.series[1].label = "other";
window.__kymo_charts.other = other;
pointTarget = "other";
copiedText = null;
event = copyEvent();
copyListener(event);
assert.equal(
  copiedText,
  "'=custom x: 6\n\n" +
    "other\t70\n" +
    "'=run one|x\tInfinity\n" +
    "<raw&> line\t-Infinity\n" +
    "nan\tNaN",
);
assert.equal(event.defaultPrevented, true, "copy resolves the chart currently under the pointer");
pointTarget = "chart";

copiedText = null;
event = copyEvent();
event.defaultPrevented = true;
copyListener(event);
assert.equal(copiedText, null);
assert.equal(event.defaultPrevented, true, "an earlier copy handler keeps ownership");

copiedText = null;
event = keyEvent();
keydownListener(event);
assert.equal(copiedText, expected);
assert.equal(event.defaultPrevented, true);

copiedText = null;
event = keyEvent({ metaKey: true });
keydownListener(event);
assert.equal(copiedText, null, "Meta+C stays on the native copy-event path");
assert.equal(event.defaultPrevented, false);

copiedText = null;
event = keyEvent({ key: "с", code: "KeyC" });
keydownListener(event);
assert.equal(copiedText, expected, "physical Control+C works on a non-Latin layout");
assert.equal(event.defaultPrevented, true);

copiedText = null;
event = keyEvent({ defaultPrevented: true });
keydownListener(event);
assert.equal(copiedText, null);

for (const blocked of [
  { shiftKey: true },
  { altKey: true },
  { repeat: true },
  { isComposing: true },
]) {
  copiedText = null;
  event = keyEvent(blocked);
  keydownListener(event);
  assert.equal(copiedText, null);
  assert.equal(event.defaultPrevented, false, "modified or repeated Control+C stays native");
}

selectionCollapsed = false;
event = copyEvent();
copyListener(event);
assert.equal(event.defaultPrevented, false, "selected text keeps native copy semantics");
selectionCollapsed = true;

const input = { closest: () => input };
document.activeElement = input;
event = keyEvent({ target: input });
keydownListener(event);
assert.equal(event.defaultPrevented, false, "editable fields keep native copy semantics");
document.activeElement = body;

assert.equal(window.__kymo_chartCopy.buildText(chart, null), null);
chart.root.isConnected = false;
assert.equal(window.__kymo_chartCopy.buildText(chart, 0), null);
chart.root.isConnected = true;

const plain = {
  ...chart,
  __kymo_copy: { ...chart.__kymo_copy, nanCols: 0, xShift: 0 },
};
assert.match(window.__kymo_chartCopy.buildText(plain, 0), /^'=custom x: 42\n\n/);

const replacement = {
  ...chart,
  data: chart.data.map((column) => column.slice()),
  __kymo_copy: { ...chart.__kymo_copy },
};
replacement.data[1][0] = 9.5;
window.__kymo_charts.chart = replacement;
event = copyEvent();
copyListener(event);
assert.equal(copiedText, expected.replace("\t1.25", "\t9.5"));
window.__kymo_charts.chart = chart;

const savedData = chart.data;
chart.data = chart.data.map((column) => [null]);
chart.data[0][0] = 42;
assert.equal(window.__kymo_chartCopy.buildText(chart, 0), null, "an empty column is not copied");
chart.data = savedData;

execMode = "fail-first";
execCalls = 0;
copiedText = null;
let focusRestored = false;
const button = { focus: () => (focusRestored = true) };
document.activeElement = button;
event = keyEvent();
keydownListener(event);
assert.equal(copiedText, expected, "textarea fallback copies the same text table");
assert.equal(event.defaultPrevented, true);
assert.equal(focusRestored, true, "textarea fallback restores focus");

console.log("chart hover copy: ok");
