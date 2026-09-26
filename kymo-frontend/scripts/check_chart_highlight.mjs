#!/usr/bin/env node

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createContext, Script } from "node:vm";

const highlightScript = new Script(readFileSync(new URL("../src/components/uplot_chart/highlight.js", import.meta.url), "utf8"), { filename: "highlight.js" });

let passed = 0;
function test(name, run) {
  run();
  console.log(`ok ${++passed} - ${name}`);
}

function row(dataset) {
  const classes = new Set();
  return {
    dataset,
    getAttribute: (name) => name === "data-run-name" ? dataset.name : null,
    classList: {
      add: (name) => classes.add(name),
      remove: (name) => classes.delete(name),
      contains: (name) => classes.has(name),
      toggle(name, on) {
        if (on) classes.add(name);
        else classes.delete(name);
      },
    },
  };
}

function fixture(setting, initialSidebar = []) {
  let pending, sidebar, sidebarBody;
  const attributes = new Map();
  const work = { frames: 0, sidebarQueries: 0, tipQueries: 0 };
  if (setting !== undefined) attributes.set("data-kymo-highlight-same-name", setting);
  function setSidebar(rows) {
    sidebar = rows;
    sidebarBody = rows === null ? null : {
      querySelectorAll(selector) {
        work.sidebarQueries++;
        if (selector === ".sidebar-run-hl") {
          return rows.filter((node) => node.classList.contains("sidebar-run-hl"));
        }
        const match = /^\.sidebar-run\[data-run-(id|name)="([^"]*)"\]$/.exec(selector);
        assert.ok(match, `unexpected selector ${selector}`);
        const value = match[2].replace(/\\([0-9a-f]+) /g, (_, hex) => String.fromCodePoint(Number.parseInt(hex, 16)));
        return rows.filter((node) => node.dataset[match[1]] === value);
      },
      querySelector(selector) {
        return this.querySelectorAll(selector)[0] ?? null;
      },
    };
  }
  setSidebar(initialSidebar);
  const document = {
    documentElement: { getAttribute: (name) => attributes.get(name) ?? null },
    querySelector(selector) {
      work.sidebarQueries++;
      assert.equal(selector, ".sidebar-body");
      return sidebarBody;
    },
  };
  // Hex-escape every code point to exercise selector escaping for arbitrary names.
  const window = { __kymo_charts: {}, CSS: { escape: (value) => Array.from(value, (char) => `\\${char.codePointAt(0).toString(16)} `).join("") } };
  const requestAnimationFrame = (callback) => {
    assert.equal(pending, undefined, "highlight frames must coalesce");
    work.frames++;
    pending = callback;
    // Frame IDs may be zero, so coalescing must not depend on their truthiness.
    return work.frames - 1;
  };
  const context = createContext({ window, document, requestAnimationFrame });
  const browserLoad = () => highlightScript.runInContext(context);
  browserLoad();
  return {
    window,
    get sidebar() { return sidebar; },
    setSidebar,
    attributes,
    work,
    browserLoad,
    flush() {
      const callback = pending;
      pending = undefined;
      callback?.();
    },
    chart(name, runs, names, { rawStride = 0, nan = false, colors = [], labels = [] } = {}) {
      const rows = runs.map((_, index) => row({ r: `${index}` }));
      const chart = {
        __kymo_hl: { runs, names, n: runs.length, rs: rawStride, nan },
        __kymo_hleff: "",
        __kymo_tip: {
          style: { display: "block" },
          querySelectorAll() {
            work.tipQueries++;
            return rows;
          },
        },
        series: Array.from({ length: 1 + runs.length * (rawStride + 1 + Number(nan)) }, () => ({ alpha: 1, _focus: false })),
        redraws: 0,
        redraw(rebuildPaths) {
          assert.equal(rebuildPaths, false, "highlight must reuse chart paths");
          this.redraws++;
        },
        rows,
      };
      for (let i = 0; i < runs.length; i++) {
        chart.series[1 + runs.length * rawStride + i].stroke = colors[i];
        chart.series[1 + runs.length * rawStride + i].label = labels[i];
      }
      window.__kymo_charts[name] = chart;
      window.__kymo_applyHl(chart);
      return chart;
    },
  };
}

function assertHighlight(chart, focused, active = true) {
  const { n, rs, nan } = chart.__kymo_hl;
  const lineBase = 1 + n * rs;
  for (let i = 0; i < n; i++) {
    const alpha = !active || focused.includes(i) ? 1 : 0.25;
    const focus = active && focused.includes(i);
    for (const column of [...Array.from({ length: rs }, (_, k) => 1 + i * rs + k), lineBase + i]) {
      assert.equal(chart.series[column].alpha, alpha, `series ${column} alpha`);
      assert.equal(chart.series[column]._focus, focus, `series ${column} focus`);
    }
    if (nan) assert.equal(chart.series[lineBase + n + i].alpha, alpha, `run ${i} marker alpha`);
    assert.equal(chart.rows[i].classList.contains("kymo-tip-row-hot"), focus, `run ${i} tooltip`);
  }
}

test("browser startup installs highlighting before charts exist and preserves the installed helper", () => {
  const f = fixture("true");
  f.sidebar.push(row({ id: "a", name: "resume" }), row({ id: "b", name: "resume" }));
  f.window.__kymo_setHl("a", "resume");
  const setHighlight = f.window.__kymo_setHl;
  f.browserLoad();
  assert.equal(f.window.__kymo_setHl, setHighlight);
  f.window.__kymo_setHl("b", "resume");
  assert.equal(f.work.frames, 1);
  f.flush();
  assert.ok(f.sidebar.every((node) => node.classList.contains("sidebar-run-hl")));
  const chart = f.chart("first", ["b", "c"], ["resume", "other"]);
  assertHighlight(chart, [0]);
});

test("missing, remounted and replaced sidebar roots preserve live names without touching detached rows", () => {
  const f = fixture("true", null);
  const peer = f.chart("peer", ["b", "c"], ["old", "new"]);
  f.window.__kymo_setHl("a", "old");
  f.flush();
  f.window.__kymo_refreshHl();
  f.flush();
  assertHighlight(peer, [0]);

  const source = f.chart("source", ["a"], ["new"]);
  f.window.__kymo_refreshHl();
  f.flush();
  assertHighlight(source, [0]);
  assertHighlight(peer, [1]);
  delete f.window.__kymo_charts.source;

  const detached = [row({ id: "a", name: "new" }), row({ id: "c", name: "new" })];
  f.setSidebar(detached);
  f.window.__kymo_refreshHl();
  f.flush();
  assert.ok(detached.every((node) => node.classList.contains("sidebar-run-hl")));
  f.setSidebar(null);
  detached[0].dataset.name = "stale";
  f.window.__kymo_refreshHl();
  f.flush();
  assertHighlight(peer, [1]);
  f.window.__kymo_setHl(null);
  f.flush();
  assertHighlight(peer, [], false);
  assert.ok(detached.every((node) => node.classList.contains("sidebar-run-hl")));

  const remounted = [row({ id: "a", name: "old" }), row({ id: "b", name: "old" }), row({ id: "c", name: "new" })];
  f.setSidebar(remounted);
  f.window.__kymo_setHl("a", "old");
  f.flush();
  assertHighlight(peer, [0]);
  assert.deepEqual(remounted.map((node) => node.classList.contains("sidebar-run-hl")), [true, true, false]);

  f.setSidebar([row({ id: "a", name: "new" }), row({ id: "b", name: "old" }), row({ id: "c", name: "new" })]);
  f.window.__kymo_refreshHl();
  f.flush();
  assertHighlight(peer, [1]);
  assert.deepEqual(f.sidebar.map((node) => node.classList.contains("sidebar-run-hl")), [true, false, true]);
  assert.deepEqual(remounted.map((node) => node.classList.contains("sidebar-run-hl")), [true, true, false]);
  f.window.__kymo_setHl(null);
  f.flush();
  assert.ok(f.sidebar.every((node) => !node.classList.contains("sidebar-run-hl")));
  assert.ok(detached.every((node) => node.classList.contains("sidebar-run-hl")));
});

test("missing and false preferences preserve single-run highlight, including tagged series", () => {
  for (const setting of [undefined, "false"]) {
    const f = fixture(setting);
    const chart = f.chart("one", ["a", "a", "b"], ["resume", "resume", "resume"], { rawStride: 3, nan: true });
    const peer = f.chart("peer", ["b"], ["resume"]);
    f.window.__kymo_setHl("a", "resume");
    f.flush();
    assertHighlight(chart, [0, 1]);
    assertHighlight(peer, []);
    f.window.__kymo_setHl(null);
    f.flush();
    assertHighlight(chart, [], false);
    assertHighlight(peer, [], false);
  }
});

test("name groups span peers without the hovered run and include every raw, envelope, line, marker and tooltip column", () => {
  const f = fixture("true");
  const chart = f.chart("one", ["a", "b", "b", "c"], ["resume", "resume", "resume", "other"], { rawStride: 3, nan: true });
  const peer = f.chart("peer", ["b", "c"], ["resume", "other"], { nan: true });
  f.window.__kymo_setHl("a", "resume");
  f.flush();
  assertHighlight(chart, [0, 1, 2]);
  assertHighlight(peer, [0]);
});

test("equal raw names group across different colors and labels, while equal colors never group different names", () => {
  const f = fixture("true");
  const chart = f.chart("one", ["a", "b", "c"], ["resume", "resume", "different"], {
    colors: ["#abcdef", "#123456", "#abcdef"],
    labels: ["resume #1 / train", "resume #2 / validation", "different #3 / train"],
  });
  f.window.__kymo_setHl("a", "resume");
  f.flush();
  assertHighlight(chart, [0, 1]);
});

test("names match exactly, including case and whitespace", () => {
  const f = fixture("true");
  const chart = f.chart("one", ["a", "b", "c", "d"], ["Resume", "resume", "Resume ", "Resume"]);
  f.window.__kymo_setHl("a", "Resume");
  f.flush();
  assertHighlight(chart, [0, 3]);
});

test("empty names are real groups and unknown names retain single-run highlighting", () => {
  const f = fixture("true");
  const chart = f.chart("one", ["a", "b", "c", "d"], ["", "", null, null]);
  f.sidebar.push(row({ id: "a", name: "" }), row({ id: "b", name: "" }), row({ id: "c", name: null }));
  f.window.__kymo_setHl("a", "");
  f.flush();
  assertHighlight(chart, [0, 1]);
  assert.deepEqual(f.sidebar.map((node) => node.classList.contains("sidebar-run-hl")), [true, true, false]);
  f.window.__kymo_refreshHl();
  f.flush();
  assertHighlight(chart, [0, 1]);
  f.window.__kymo_setHl("c", null);
  f.flush();
  assertHighlight(chart, [2]);
});

test("quotes, backslashes, newlines and Unicode in raw names remain exact across sidebar and chart matching", () => {
  const f = fixture("true");
  const name = 'resume "quoted"\\path\n🚀';
  const rid = 'source"\\\n';
  const chart = f.chart("one", [rid, "b", "c"], [name, name, name.replace("\n", " ")]);
  f.sidebar.push(row({ id: rid, name }), row({ id: "b", name }), row({ id: "c", name: "other" }));
  f.window.__kymo_setHl(rid, name);
  f.flush();
  assertHighlight(chart, [0, 1]);
  assert.deepEqual(f.sidebar.map((node) => node.classList.contains("sidebar-run-hl")), [true, true, false]);
  f.window.__kymo_refreshHl();
  f.flush();
  assertHighlight(chart, [0, 1]);
});

test("sidebar hover carries a hidden run's name and stamps all visible matching rows", () => {
  const f = fixture("true");
  f.sidebar.push(row({ id: "a", name: "resume" }), row({ id: "b", name: "resume" }), row({ id: "c", name: "other" }));
  const peer = f.chart("peer", ["b", "c"], ["resume", "other"]);
  f.window.__kymo_setHl("hidden-source", "resume");
  f.flush();
  assertHighlight(peer, [0]);
  assert.deepEqual(f.sidebar.map((node) => node.classList.contains("sidebar-run-hl")), [true, true, false]);
  f.sidebar.splice(0, 2, row({ id: "newly-visible", name: "resume" }));
  f.window.__kymo_refreshHl();
  f.flush();
  assert.deepEqual(f.sidebar.map((node) => node.classList.contains("sidebar-run-hl")), [true, false]);
});

test("same-group member switches retain the latest raw ID without chart or sidebar work", () => {
  const f = fixture("true");
  const chart = f.chart("one", ["a", "b"], ["resume", "resume"]);
  const absent = f.chart("absent", ["c"], ["other"]);
  f.sidebar.push(row({ id: "a", name: "resume" }), row({ id: "b", name: "resume" }));
  f.window.__kymo_setHl("a", "resume");
  f.flush();
  const work = { ...f.work };
  f.window.__kymo_setHl("b", "resume");
  f.flush();
  assert.equal(f.window.__kymo_hlrun, "b");
  assert.deepEqual(f.work, work);
  assert.equal(chart.redraws, 1);
  assert.equal(absent.redraws, 1);
  f.attributes.set("data-kymo-highlight-same-name", "false");
  f.window.__kymo_refreshHl();
  f.flush();
  assertHighlight(chart, [1]);
  assert.deepEqual(f.sidebar.map((node) => node.classList.contains("sidebar-run-hl")), [false, true]);
});

test("switching absent groups reuses existing canvas pixels", () => {
  const f = fixture("true");
  const chart = f.chart("one", ["a", "b"], ["resume", "resume"]);
  const absent = f.chart("absent", ["c"], ["other"]);
  f.window.__kymo_setHl("a", "resume");
  f.flush();
  f.window.__kymo_setHl("d", "unplotted");
  f.flush();
  assert.equal(absent.redraws, 1);
  assertHighlight(chart, []);
});

test("unchanged refresh restamps replaced sidebar rows without chart work", () => {
  for (const setting of [undefined, "false", "true"]) {
    const f = fixture(setting);
    const chart = f.chart("one", ["a", "b"], ["resume", "resume"]);
    f.sidebar.push(row({ id: "a", name: "resume" }));
    f.window.__kymo_setHl("a", "resume");
    f.flush();
    f.sidebar.splice(0, 1, row({ id: "a", name: "resume" }));
    const work = { ...f.work };
    f.window.__kymo_refreshHl();
    f.flush();
    assert.equal(f.sidebar[0].classList.contains("sidebar-run-hl"), true);
    assert.ok(f.work.sidebarQueries > work.sidebarQueries);
    assert.equal(f.work.frames, work.frames);
    assert.equal(f.work.tipQueries, work.tipQueries);
    assert.equal(chart.redraws, 1);
    assertHighlight(chart, setting === "true" ? [0, 1] : [0]);
  }
});

test("renaming the same raw selection with grouping off retains the name without chart work", () => {
  const f = fixture("false");
  const chart = f.chart("peer", ["b", "c"], ["new", "old"]);
  f.window.__kymo_setHl("hidden", "old");
  f.flush();
  const work = { ...f.work };
  f.window.__kymo_setHl("hidden", "new");
  f.flush();
  assert.deepEqual(f.work, work);
  f.attributes.set("data-kymo-highlight-same-name", "true");
  f.window.__kymo_refreshHl();
  f.flush();
  assertHighlight(chart, [0]);
});

test("resolving a previously unknown name refreshes the active group", () => {
  const f = fixture("true");
  const chart = f.chart("one", ["a", "b"], [null, "resume"]);
  f.window.__kymo_setHl("a");
  f.flush();
  assertHighlight(chart, [0]);
  chart.__kymo_hl.names[0] = "resume";
  f.window.__kymo_refreshHl(chart);
  f.flush();
  assertHighlight(chart, [0, 1]);
});

test("changing the global setting reapplies the hovered run immediately without rebuilding charts", () => {
  const f = fixture();
  const chart = f.chart("one", ["a", "b"], ["resume", "resume"]);
  f.window.__kymo_setHl("a", "resume");
  f.flush();
  assertHighlight(chart, [0]);
  f.attributes.set("data-kymo-highlight-same-name", "true");
  f.window.__kymo_refreshHl();
  f.flush();
  assertHighlight(chart, [0, 1]);
  f.attributes.set("data-kymo-highlight-same-name", "false");
  f.window.__kymo_refreshHl();
  f.flush();
  assertHighlight(chart, [0]);
});

test("a rebuilt chart adopts the current name group", () => {
  const f = fixture("true");
  f.window.__kymo_setHl("a", "resume");
  f.flush();
  const chart = f.chart("replacement", ["b", "c"], ["resume", "other"], { rawStride: 2, nan: true });
  assertHighlight(chart, [0]);
});

test("a live sidebar name change refreshes group membership, including Trash rows", () => {
  const f = fixture("true");
  const source = row({ id: "a", name: "resume" });
  const trash = row({ id: "b", name: "other" });
  trash.classList.add("sidebar-run-trash");
  f.sidebar.push(source, trash);
  const chart = f.chart("one", ["b", "c"], ["other", "resume"]);
  f.window.__kymo_setHl("a", "resume");
  f.flush();
  assertHighlight(chart, [1]);
  source.dataset.name = "other";
  f.window.__kymo_refreshHl();
  f.flush();
  assertHighlight(chart, [0]);
  assert.equal(source.classList.contains("sidebar-run-hl"), true);
  assert.equal(trash.classList.contains("sidebar-run-hl"), true);
});

test("chart highlights mirror into every matching sidebar row, including Trash with grouping off and on", () => {
  for (const setting of [undefined, "false", "true"]) {
    const f = fixture(setting);
    const chart = f.chart("one", ["a", "b", "c"], ["resume", "resume", "other"]);
    f.sidebar.push(row({ id: "a", name: "resume" }), row({ id: "a", name: "resume" }), row({ id: "b", name: "resume" }), row({ id: "c", name: "other" }));
    for (const node of f.sidebar.slice(1)) node.classList.add("sidebar-run-trash");
    f.window.__kymo_setHl("a", "resume");
    f.flush();
    assertHighlight(chart, setting === "true" ? [0, 1] : [0]);
    assert.deepEqual(f.sidebar.map((node) => node.classList.contains("sidebar-run-hl")), [true, true, setting === "true", false]);
    f.window.__kymo_setHl(null);
    f.flush();
    assert.ok(f.sidebar.every((node) => !node.classList.contains("sidebar-run-hl")));
  }
});

test("a freshly registered source chart refreshes the name even when the sidebar source is filtered out", () => {
  const f = fixture("true");
  const source = f.chart("source", ["a"], ["resume"]);
  const peer = f.chart("peer", ["b", "c"], ["other", "resume"]);
  f.window.__kymo_setHl("a", "resume");
  f.flush();
  assertHighlight(peer, [1]);
  source.__kymo_hl.names = ["other"];
  f.window.__kymo_refreshHl(source);
  f.flush();
  assertHighlight(source, [0]);
  assertHighlight(peer, [0]);
});

test("coalesced A to B to A flips retag a tooltip rebuilt during the intermediate highlight", () => {
  const f = fixture("false");
  const chart = f.chart("one", ["a", "b"], ["resume", "other"]);
  f.window.__kymo_setHl("a", "resume");
  f.window.__kymo_setHl("b", "other");
  f.window.__kymo_setHl("a", "resume");
  assert.equal(f.work.frames, 1);
  f.flush();
  f.window.__kymo_setHl("b", "other");
  f.window.__kymo_retagRows(chart.__kymo_tip, chart.__kymo_hl.runs, chart.__kymo_hl.names);
  assert.equal(chart.rows[1].classList.contains("kymo-tip-row-hot"), true);
  f.window.__kymo_setHl("a", "resume");
  f.flush();
  assert.equal(chart.redraws, 1);
  assertHighlight(chart, [0]);
});

console.log(`chart highlight: ${passed} checks passed`);
