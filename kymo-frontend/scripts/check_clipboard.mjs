#!/usr/bin/env node

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { performance } from "node:perf_hooks";

const started = performance.now();
const source = readFileSync(new URL("../src/util/clipboard.js", import.meta.url), "utf8");
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const copy = new AsyncFunction("text", "window", "document", "navigator", "dioxus", source);
let cases = 0;

function range(id) {
  return { id, cloneRange: () => range(id) };
}

function browser({ secure = false, writeText, exec = "success", selected = true, connected = true } = {}) {
  const state = {
    nodes: [],
    areas: [],
    writes: [],
    results: [],
    commands: 0,
    restoredFocus: 0,
    clipboard: "previous clipboard",
  };
  const selection = selected ? {
    ranges: [range("first"), range("second")],
    get rangeCount() { return this.ranges.length; },
    getRangeAt(index) { return this.ranges[index]; },
    removeAllRanges() { this.ranges = []; },
    addRange(value) { this.ranges.push(value); },
  } : null;
  const active = {
    isConnected: connected,
    focus(options) {
      assert.deepEqual(options, { preventScroll: true });
      state.restoredFocus += 1;
      document.activeElement = this;
    },
  };
  const document = {
    activeElement: active,
    body: {
      appendChild(node) { state.nodes.push(node); },
    },
    createElement(tag) {
      assert.equal(tag, "textarea");
      let value = "";
      const listeners = new Map();
      const area = {
        get value() { return value; },
        set value(text) { value = text.replace(/\r\n?/g, "\n"); },
        style: {},
        listeners,
        addEventListener(type, listener) {
          assert.equal(type, "copy");
          listeners.set(type, listener);
        },
        focus(options) {
          assert.deepEqual(options, { preventScroll: true });
          document.activeElement = this;
        },
        select() {
          assert.equal(document.activeElement, this);
          if (selection) selection.ranges = [range("textarea")];
        },
        setSelectionRange(start, end) {
          assert.equal(start, 0);
          assert.equal(end, this.value.length);
        },
        remove() {
          state.nodes.splice(state.nodes.indexOf(this), 1);
        },
      };
      state.areas.push(area);
      return area;
    },
    execCommand(command) {
      assert.equal(command, "copy");
      state.commands += 1;
      const area = document.activeElement;
      assert.ok(state.nodes.includes(area));
      assert.equal(area.readOnly, true);
      assert.ok(area.value.length, "empty payload still gets a nonempty textarea selection");
      if (exec === "throw") throw new Error("copy denied");
      if (exec === "false") return false;
      if (exec === "no-event") return true;
      const event = {
        defaultPrevented: false,
        clipboardData: {
          setData(type, text) {
            assert.equal(type, "text/plain");
            state.clipboard = text;
          },
        },
        preventDefault() { this.defaultPrevented = true; },
      };
      area.listeners.get("copy")(event);
      assert.equal(event.defaultPrevented, true, "copy event overrides textarea line-ending normalization");
      return true;
    },
  };
  const window = { isSecureContext: secure, getSelection: () => selection };
  const navigator = writeText ? {
    clipboard: {
      writeText(text) {
        state.writes.push(text);
        return writeText(text, state);
      },
    },
  } : {};
  return {
    state,
    run: text => copy(text, window, document, navigator, {
      send(result) { state.results.push(result); },
    }),
    check({ ok, text, fallbacks = 1, writes = [] }) {
      assert.deepEqual(state.results, [ok], "exactly one result reaches Dioxus");
      assert.deepEqual(state.writes, writes);
      assert.equal(state.commands, fallbacks);
      assert.equal(state.areas.length, fallbacks);
      assert.deepEqual(state.nodes, [], "temporary textarea is removed");
      assert.equal(state.restoredFocus, connected ? fallbacks : 0);
      if (connected) assert.equal(document.activeElement, active);
      if (selection) assert.deepEqual(selection.ranges.map(value => value.id), ["first", "second"]);
      assert.equal(state.clipboard, ok ? text : "previous clipboard");
      cases += 1;
    },
  };
}

const payloads = ["  original\ttext  ", "first\r\nsecond\rthird\n", "雪 😀 e\u0301 <&> ", ""];
for (const text of payloads) {
  const secure = browser({
    secure: true,
    writeText: async (value, state) => { state.clipboard = value; },
  });
  await secure.run(text);
  secure.check({ ok: true, text, fallbacks: 0, writes: [text] });

  const http = browser({ writeText: () => assert.fail("insecure context must use the synchronous fallback") });
  const pending = http.run(text);
  assert.equal(http.state.commands, 1, "HTTP fallback runs before yielding the user gesture");
  await pending;
  http.check({ ok: true, text });
}

const text = "exact\r\npayload";
const missingApi = browser({ secure: true });
await missingApi.run(text);
missingApi.check({ ok: true, text });

let resolveWrite;
const pendingWrite = new Promise(resolve => { resolveWrite = resolve; });
const delayedSuccess = browser({
  secure: true,
  writeText: async (value, state) => {
    await pendingWrite;
    state.clipboard = value;
  },
});
const pendingSuccess = delayedSuccess.run(text);
assert.deepEqual(delayedSuccess.state.results, []);
assert.equal(delayedSuccess.state.commands, 0);
resolveWrite();
await pendingSuccess;
delayedSuccess.check({ ok: true, text, fallbacks: 0, writes: [text] });

for (const writeText of [
  () => { throw new Error("synchronous rejection"); },
  () => Promise.reject(new Error("immediate rejection")),
]) {
  const rejected = browser({ secure: true, writeText });
  await rejected.run(text);
  rejected.check({ ok: true, text, writes: [text] });
}

// Controlled completion checks the late-rejection path; browser gesture permission is covered by the local probe.
for (const exec of ["success", "false", "throw"]) {
  let rejectWrite;
  const pendingWrite = new Promise((_, reject) => { rejectWrite = reject; });
  const late = browser({ secure: true, writeText: () => pendingWrite, exec });
  const pending = late.run(text);
  assert.deepEqual(late.state.writes, [text]);
  assert.equal(late.state.commands, 0, "fallback waits for writeText to settle");
  assert.deepEqual(late.state.results, []);
  assert.deepEqual(late.state.nodes, []);
  rejectWrite(new Error("late rejection"));
  await pending;
  late.check({ ok: exec === "success", text, writes: [text] });
}

for (const exec of ["false", "throw", "no-event"]) {
  const failed = browser({ exec });
  await failed.run(text);
  failed.check({ ok: false, text });
}

for (const options of [{ selected: false }, { connected: false }]) {
  const absent = browser(options);
  await absent.run(text);
  absent.check({ ok: true, text });
}

for (const outcome of ["reject", "resolve"]) {
  let rejectOld, resolveOld;
  const oldWrite = new Promise((resolve, reject) => {
    resolveOld = resolve;
    rejectOld = reject;
  });
  const shared = browser({
    secure: true,
    writeText(value, state) {
      if (value === "old") {
        // A successful write can precede its delayed acknowledgement.
        if (outcome === "resolve") state.clipboard = value;
        return oldWrite;
      }
      state.clipboard = value;
      return Promise.resolve();
    },
  });
  const old = shared.run("old");
  await shared.run("new");
  if (outcome === "reject") rejectOld(new Error("obsolete rejection"));
  else resolveOld();
  await old;
  assert.equal(shared.state.clipboard, "new", "an old retry must not overwrite a newer copy");
  assert.equal(shared.state.commands, 0, "superseded fallback is skipped");
  assert.deepEqual(shared.state.results, [true, null]);
  cases += 1;
}

console.log(`clipboard checks passed (${cases} cases, ${(performance.now() - started).toFixed(1)} ms)`);
