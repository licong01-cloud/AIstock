import { test, expect, type Page } from "@playwright/test";
import fs from "node:fs";
import path from "node:path";
import ts from "typescript";
import { QEReadCache, mergeTaskUpdates } from "../../src/app/quantevolver/components/read-cache";
import { isExecutingTask } from "../../src/app/quantevolver/components/use-active-refresh";

const src = (file: string) => fs.readFileSync(path.resolve("src/app/quantevolver", file), "utf8");

test("read cache deduplicates, bounds entries, invalidates versions, cancels and retries", async () => {
  const original = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async (_url, init) => {
    calls++;
    await Promise.resolve();
    if (init?.signal?.aborted) throw new DOMException("aborted", "AbortError");
    return new Response(JSON.stringify({ status: "success", data: calls }));
  };
  const cache = new QEReadCache(2);
  try {
    const [first, same] = await Promise.all([cache.read("/a"), cache.read("/a")]);
    expect(first).toBe(same); expect(calls).toBe(1);
    await cache.read("/b"); await cache.read("/c"); await cache.read("/a");
    expect(calls).toBe(4);
    await cache.read("/a", "/a:terminal");
    expect(calls).toBe(5);
    const pending = cache.read("/pending");
    cache.clear();
    await expect(pending).rejects.toThrow();
    await cache.read("/pending");
    expect(calls).toBe(7);
    globalThis.fetch = async () => new Response("failure", { status: 503 });
    await expect(cache.read("/error")).rejects.toThrow("503");
    globalThis.fetch = async () => new Response(JSON.stringify({ status: "success" }));
    await expect(cache.read("/error")).resolves.toEqual({ status: "success" });
  } finally { cache.clear(); globalThis.fetch = original; }
});

test("only executing tasks refresh; unchanged and historical row identities are preserved", () => {
  for (const status of ["completed", "failed", "cancelled", "paused", "pending"]) {
    expect(isExecutingTask({ status })).toBe(false);
  }
  expect(isExecutingTask({ status: "completed", running_count: 1 })).toBe(false);
  expect(isExecutingTask({ status: "pending", queued_count: 2 })).toBe(true);
  expect(isExecutingTask({ status: "running" })).toBe(true);
  const rows = [{ task_id: "live", status: "running" }, { task_id: "history", status: "completed" }];
  expect(mergeTaskUpdates(rows, [{ task_id: "live", status: "running" }])).toBe(rows);
  const terminal = mergeTaskUpdates(rows, [{ task_id: "live", status: "completed" }]);
  expect(terminal[1]).toBe(rows[1]);
  expect(terminal[0].status).toBe("completed");
});

async function mountRefreshHarness(page: Page) {
  await page.clock.install();
  await page.addScriptTag({ path: path.join(path.dirname(require.resolve("react/package.json")), "umd/react.development.js") });
  await page.addScriptTag({ path: path.join(path.dirname(require.resolve("react-dom/package.json")), "umd/react-dom.development.js") });
  const compiled = ts.transpileModule(src("components/use-active-refresh.ts"), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
  }).outputText;
  await page.addScriptTag({ content: `window.qeModule={};(function(exports,require){${compiled}})(window.qeModule,()=>window.React);` });
  await page.evaluate(() => {
    const w = window as any;
    w.hits = 0; w.fail = false; w.held = false;
    Object.defineProperty(document, "visibilityState", { configurable: true, get: () => w.hidden ? "hidden" : "visible" });
    const host = document.createElement("div"); document.body.appendChild(host);
    function Harness() {
      const [enabled, setEnabled] = w.React.useState(true);
      w.setEnabled = setEnabled;
      w.qeModule.useActiveRefresh(async () => {
        w.hits++;
        if (w.held) await new Promise(resolve => { w.release = resolve; });
        if (w.fail) throw new Error("test network failure");
      }, enabled, "observed", 10_000);
      return null;
    }
    w.root = w.ReactDOM.createRoot(host);
    w.root.render(w.React.createElement(Harness));
  });
  await expect.poll(() => page.evaluate(() => !!(window as any).setEnabled)).toBe(true);
}

test("refresh clamps to 60s, prevents overlap, pauses hidden and stops at terminal", async ({ page }) => {
  await mountRefreshHarness(page);
  await page.clock.runFor(59_000);
  expect(await page.evaluate(() => (window as any).hits)).toBe(0);
  await page.evaluate(() => { (window as any).held = true; });
  await page.clock.runFor(1000);
  expect(await page.evaluate(() => (window as any).hits)).toBe(1);
  await page.clock.runFor(180_000);
  expect(await page.evaluate(() => (window as any).hits)).toBe(1);
  await page.evaluate(() => { const w=window as any; w.held=false; w.release(); w.hidden=true; document.dispatchEvent(new Event("visibilitychange")); });
  await page.clock.runFor(180_000);
  expect(await page.evaluate(() => (window as any).hits)).toBe(1);
  await page.evaluate(() => { (window as any).hidden=false; document.dispatchEvent(new Event("visibilitychange")); });
  await page.clock.runFor(60_000);
  expect(await page.evaluate(() => (window as any).hits)).toBe(2);
  await page.evaluate(() => { (window as any).setEnabled(false); });
  await page.waitForTimeout(1);
  await page.clock.runFor(300_000);
  expect(await page.evaluate(() => (window as any).hits)).toBe(2);
  await page.evaluate(() => { (window as any).root.unmount(); });
});

test("failures back off rather than creating a catch-up storm", async ({ page }) => {
  await mountRefreshHarness(page);
  await page.evaluate(() => { (window as any).fail = true; });
  await page.clock.runFor(60_000);
  expect(await page.evaluate(() => (window as any).hits)).toBe(1);
  await page.clock.runFor(119_000);
  expect(await page.evaluate(() => (window as any).hits)).toBe(1);
  await page.clock.runFor(1000);
  expect(await page.evaluate(() => (window as any).hits)).toBe(2);
  await page.evaluate(() => { (window as any).root.unmount(); });
});

test("all four QE views use active refresh; full payload and full Plotly are not eager", () => {
  for (const file of ["evolution/page.tsx", "evolution/[taskId]/page.tsx",
    "multi-alpha/combine-backtest/page.tsx", "multi-alpha/combine-backtest/[taskKey]/page.tsx"]) {
    const code = src(file);
    expect(code).toContain("useActiveRefresh(");
    expect(code).not.toContain("setInterval(");
    // Explicit clone/edit/export actions may still request the legacy full API.
    if (file.includes("[taskId]")) expect(code.includes("detail=full")).toBe(false);
  }
  expect(src("evolution/page.tsx")).toContain("EVOLUTION_TASK_LIST_PAGE_SIZE = 50");
  expect(src("evolution/page.tsx")).toContain("/tasks/status?");
  expect(src("evolution/[taskId]/page.tsx")).toContain("/loops/comparison");
  const lazy = src("components/charts/LazyPlot.tsx");
  expect(lazy).toContain("IntersectionObserver");
  expect(lazy).toContain('import("plotly.js-basic-dist")');
  expect(lazy).not.toContain('import("react-plotly.js")');
  for (const file of ["ReturnCurveChart", "IcSeriesChart", "LossCurveChart", "DimensionHeatmap", "ICDecayTrendChart", "MetricsTrajectoryChart"]) {
    expect(src(`components/charts/${file}.tsx`)).toContain('from "./LazyPlot"');
  }
  expect(src("evolution/components/LogsPanel.tsx")).toContain("useSyncExternalStore");
});
