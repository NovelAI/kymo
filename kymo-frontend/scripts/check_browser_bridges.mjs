#!/usr/bin/env node

// Regression checks for the root JavaScript bridges in main.rs. Start a Chromium-family browser with --remote-debugging-port=9222, then run this script against the locally served main page before opening any chart.

const pageUrl = process.argv[2] ?? "http://localhost:8080/";
const cdpBase = process.env.CDP_URL ?? "http://127.0.0.1:9222";

if (typeof WebSocket === "undefined") {
  throw new Error("Node.js 22 or newer is required (built-in WebSocket missing)");
}

let target;
let socket;
let nextId = 1;
const pending = new Map();

function command(method, params = {}) {
  const id = nextId++;
  socket.send(JSON.stringify({ id, method, params }));
  return new Promise((resolve, reject) => pending.set(id, { resolve, reject }));
}

async function evaluate(expression, awaitPromise = false) {
  const response = await command("Runtime.evaluate", {
    expression,
    awaitPromise,
    returnByValue: true,
  });
  if (response.exceptionDetails) {
    throw new Error(
      response.exceptionDetails.exception?.description ??
        response.exceptionDetails.text ??
        "browser evaluation failed",
    );
  }
  return response.result.value;
}

try {
  const targetResponse = await fetch(
    `${cdpBase}/json/new?${encodeURIComponent(pageUrl)}`,
    { method: "PUT" },
  );
  if (!targetResponse.ok) {
    throw new Error(`CDP target creation failed: ${targetResponse.status}`);
  }
  target = await targetResponse.json();
  socket = new WebSocket(target.webSocketDebuggerUrl);
  socket.addEventListener("message", ({ data }) => {
    const message = JSON.parse(data);
    if (message.id === undefined) return;
    const waiter = pending.get(message.id);
    if (!waiter) return;
    pending.delete(message.id);
    if (message.error) waiter.reject(new Error(message.error.message));
    else waiter.resolve(message.result);
  });
  await new Promise((resolve, reject) => {
    socket.addEventListener("open", resolve, { once: true });
    socket.addEventListener("error", reject, { once: true });
  });
  await command("Runtime.enable");
  const deadline = Date.now() + 10_000;
  while (!(await evaluate("window.__kymo_kbActivate === 1"))) {
    if (Date.now() >= deadline) {
      throw new Error("frontend did not install the keyboard activation bridge");
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }

  const chartHelpers = await evaluate(`(() => ({
    noCharts: Object.keys(window.__kymo_charts ?? {}).length === 0 &&
      document.querySelector('.chart-container') === null,
    highlight: typeof window.__kymo_setHl === 'function' &&
      typeof window.__kymo_refreshHl === 'function' &&
      typeof window.__kymo_applyHl === 'function',
    hover: typeof window.__kymo_hp?.createHoverPointLookup === 'function',
  }))()`);
  if (!chartHelpers.noCharts || !chartHelpers.highlight || !chartHelpers.hover) {
    throw new Error(`chart helpers must be installed on the main page before any chart: ${JSON.stringify(chartHelpers)}`);
  }
  console.log("chart helpers installed before any chart: ok");

  const bridged = await evaluate(
    `new Promise(resolve => {
      const button = document.createElement('button');
      button.style.display = 'none';
      let activated = false;
      button.addEventListener('mousedown', event => {
        activated = event.button === 0;
      });
      document.body.appendChild(button);
      button.click();
      queueMicrotask(() => {
        button.remove();
        resolve(activated);
      });
    })`,
    true,
  );
  if (!bridged) {
    throw new Error("detail-0 click did not produce a primary mousedown");
  }
  console.log("keyboard activation bridge: ok");

  const lifecycle = await evaluate(
    `(() => {
      let predecessorCleanups = 0;
      let successorCleanups = 0;
      let directCleanups = 0;
      const predecessor = window.__kymo_bridges.mount('__check__', 'predecessor', () => predecessorCleanups++);
      window.__kymo_bridges.mount('__check__', 'successor', () => successorCleanups++);
      const evictedAtMount = predecessorCleanups === 1;
      window.__kymo_bridges.unmount('__check__', 'predecessor');
      predecessor();
      const predecessorWasFenced = evictedAtMount && predecessorCleanups === 1 && successorCleanups === 0;
      window.__kymo_bridges.unmount('__check__', 'successor');
      window.__kymo_bridges.unmount('__check__', 'successor');
      const direct = window.__kymo_bridges.mount('__check__', 'direct', () => directCleanups++);
      direct();
      direct();
      window.__kymo_bridges.unmount('__check__', 'direct');
      window.__kymo_bridges.mount('__throws__', 'predecessor', () => { throw new Error('expected cleanup failure'); });
      let throwingCleanupDidNotBlockSuccessor = false;
      try {
        const successor = window.__kymo_bridges.mount('__throws__', 'successor', () => {});
        throwingCleanupDidNotBlockSuccessor = true;
        successor();
      } catch (_) {}
      return predecessorWasFenced && successorCleanups === 1 && directCleanups === 1 && throwingCleanupDidNotBlockSuccessor;
    })()`,
  );
  if (!lifecycle) {
    throw new Error("bridge lifecycle did not evict, owner-fence, and idempotently unmount");
  }
  console.log("shared bridge lifecycle: ok");
} finally {
  socket?.close();
  if (target?.id) await fetch(`${cdpBase}/json/close/${target.id}`);
}
