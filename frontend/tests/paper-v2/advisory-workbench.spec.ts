import { expect, test, type Page, type Route } from "@playwright/test";

const snapshot = "a".repeat(64);
const target = "2026-10-28";
const stamp = "2026-10-28T09:00:00+08:00";
const api = "/advisory/workbench";
const programIds = ["parent", "single", "lstm", "tcn", "lgb"];
const names = ["多 Alpha 父包", "单 Alpha 旧包", "LSTM 策略包", "TCN 策略包", "LGB 策略包"];
const version = (p: string, date = target) => ({ list_version_id: `${p}-${date}`, binding_version_id: `${p}-binding`, trade_date: date, created_at: stamp, version_status: "PUBLISHED", active_count: 5, entered_count: 3, held_count: 5, exited_count: 2, waiting_count: 0, date_context: { selection_as_of_trade_date: "2026-10-27" } });
const envelope = (p: string) => ({ schema_version: "advisory_workbench_v1", program_id: p, binding_version_id: `${p}-binding`, target_date: target, list_version_id: `${p}-${target}`, snapshot_id: snapshot, generated_at: stamp });
function summary(p: string) {
  return { ...envelope(p), program: { program_id: p, program_name: names[programIds.indexOf(p)], status: "ENABLED", enabled_since: stamp, target_count: 20, package_ids: [p] }, active_binding: { binding_version_id: `${p}-binding` }, bindings: [{ binding_version_id: `${p}-binding`, program_version: 1, activation_status: "ACTIVE", effective_from_trade_date: "2026-10-12" }],
    metrics: { active_count: 8, entered_episode_count: 49, metric_evaluable_count: p === "lgb" ? 0 : 41, win_rate: p === "lgb" ? null : .3077, avg_return_bps: p === "lgb" ? null : -224.38, median_return_bps: -100, max_drawdown_bps: -450, avg_holding_days: 5, take_profit_count: 2, stop_loss_count: 16, metric_status: p === "lgb" ? "UNREALIZED_ONLY" : "READY", metric_as_of_trade_date: "2026-10-27", metric_mark_trade_date: "2026-10-27", missing_open_mark_count: 0, coverage: { settled_count: 41, due_count: 41, settled_coverage: 1 }, unrealized: { gross: { avg_return_bps: 123, sample_count: 8 } } },
    runtime: { state: p === "parent" ? "ASSET_BLOCKED" : "PUBLISHED", reason: p === "parent" ? "seed_assets_incomplete" : null, settlement: "WAITING_DATA", target_date: target, updated_at: stamp }, selected: p === "parent" ? null : version(p), latest: version(p, p === "parent" ? "2026-10-09" : target), default_target_date: target,
    coverage: { first_published_target_date: "2026-08-14", through: target, missing_dates: [], missing_count: 0, status: "COMPLETE" }, date_options: ["2026-10-26", "2026-10-27", target], source_counts: { FORWARD: 49 }, versions_count: 41 };
}
function episode(index: number, p = "lstm") {
  return { episode_id: `episode-${index}`, symbol: index < 2 ? "600001.SH" : `600${String(index).padStart(3, "0")}.SH`, stock_name: index < 2 ? "重复入选股票" : `历史股票 ${index + 1}`, status: "EXITED", signal_date: "2026-10-12", effective_entry_date: "2026-10-13", effective_exit_date: "2026-10-20", exit_signal_date: "2026-10-19", entry_price: 18.5, exit_price: 17.8, current_rank: null, entry_rank: 1, exit_reason: index % 2 ? "STOP_LOSS" : "TAKE_PROFIT", holding_trading_days: 5, performance_state: "SETTLED", performance_reason_code: null, return_bps: index % 2 ? -410 : 620, return_kind: "REALIZED", progress: "ENDED", mark_date: "2026-10-27", mark_price: 17.8, source: index === 40 ? "UNKNOWN" : "FORWARD", binding_version_id: `${p}-binding`, metric_as_of_trade_date: "2026-10-27" };
}
const stocks = Array.from({ length: 8 }, (_, i) => ({ symbol: `60010${i}.SH`, stock_name: `当日股票 ${String.fromCharCode(65 + i)}`, episode_id: [0, 2, 6].includes(i) ? null : `episode-${i}`, rank: i + 1, membership: "IN_SCOPE", change: [0, 2, 6].includes(i) ? "NEW" : "CONTINUING", progress: [0, 2, 6].includes(i) ? "PENDING_ENTRY" : "TRACKING", consecutive_days: [0, 2, 6].includes(i) ? 1 : i + 1, reference_price: 42.18, reference_date: "2026-10-27", episode: [0, 2, 6].includes(i) ? undefined : { ...episode(i), status: "ACTIVE", progress: "TRACKING", return_kind: "UNREALIZED" } }));
type State = { seen: string[]; fail?: boolean; conflict?: boolean; hold?: Promise<void>; wrongIdentity?: boolean; waiting?: boolean };
async function json(route: Route, body: unknown, status = 200) { await route.fulfill({ status, contentType: "application/json", body: JSON.stringify(body) }); }
async function setup(page: Page, state: State) {
  await page.route("**/api/**", async route => {
    const request = route.request(), url = new URL(request.url());
    state.seen.push(`${request.method()} ${url.pathname}${url.search}`);
    expect(request.method()).toBe("GET");
    const resource = url.pathname.replace("/api/v1", "");
    if (resource.includes("/integrity/days/")) return json(route, { fact_date: target, all_sync_complete: false });
    if (resource.endsWith("/reranking-shadow")) return json(route, { ok: true, program_id: "lstm", status: "SHADOW_ONLY", captured_days: 1, complete_days: 0, mean_delta_bps: null, last_waiting: state.waiting ? { results: [{ status: "WAITING_DATA", decision_date: "2026-10-27", message: "输入分区未验证或已修订", context: { dataset: "index_daily", fact_date: "2026-10-27" } }] } : null, selected: { target_date: target, observed_at: stamp, predictions: [{ instrument: "600001.SH", selection_effective_rank: 12, advisory_rank: 3, selected: true }] } });
    if (!resource.startsWith(api)) return json(route, { count: 0, items: [] });
    if (resource === api + "/overview") {
      if (state.fail) return json(route, { detail: { message: "读取失败，请重试" } }, 503);
      return json(route, { schema_version: "advisory_workbench_v1", generated_at: stamp, items: programIds.map(summary), count: 5, status_counts: { PUBLISHED: 4, ASSET_BLOCKED: 1 } });
    }
    const parts = resource.slice((api + "/programs/").length).split("/"), p = parts[0], view = parts[1];
    if (state.hold && p === "lstm" && !view) await state.hold;
    if (!view) return json(route, { ...summary(p), program_id: state.wrongIdentity ? "wrong" : p });
    if (view === "readiness") return json(route, { program_id: p, data_date: "2026-10-27", status: "AVAILABLE", items: [{ consumer_ready: false, all_sync_complete: false }], errors: [] });
    if (state.conflict) return json(route, { detail: { reason_code: "SNAPSHOT_CHANGED", message: "读取期间数据已更新，请刷新后继续查看。" } }, 409);
    const limit = Number(url.searchParams.get("limit") || 20), offset = Number(url.searchParams.get("offset") || 0);
    let rows: unknown[] = [];
    if (view === "daily") {
      if (p === "parent") return json(route, { ...envelope(p), status: "NO_CURRENT_LIST", items: [], total_count: 0, limit, offset, summary: null });
      const group = url.searchParams.get("group") || "IN_SCOPE";
      let items = group === "IN_SCOPE" ? stocks : group === "REMOVED" || group === "OUTSIDE_PENDING" ? ["I", "J"].map((name, i) => ({ ...stocks[i], symbol: `60020${i}.SH`, stock_name: `移出股票 ${name}`, membership: "OUTSIDE_PENDING", change: "REMOVED", progress: "WAITING_REVIEW", reason_code: "PENDING_TARGET_OPEN_REVIEW" })) : [];
      const change = url.searchParams.get("change"), q = url.searchParams.get("q");
      if (change) items = items.filter(row => row.change === change);
      if (q) items = items.filter(row => row.stock_name.includes(q));
      return json(route, { ...envelope(p), status: "PUBLISHED", items: items.slice(offset, offset + limit), total_count: items.length, limit, offset, summary: { in_scope: 8, new: 3, continuing: 5, removed: 2, unknown_change: 0, observation: 3, outside_pending: 2, unknown: 0, previous_count: 7, comparison_known: true } });
    }
    if (view === "episodes" && parts[2]) return json(route, { ...envelope(p), episode: episode(Number(parts[2].split("-")[1]), p), items: [{ trade_date: "2026-10-19", action: "EXIT", reason_code: "STOP_LOSS" }], total_count: 1, limit, offset });
    if (view === "episodes") {
      let episodes = Array.from({ length: 41 }, (_, i) => episode(i, p));
      if (url.searchParams.get("source") === "UNKNOWN") episodes = episodes.filter(row => row.source === "UNKNOWN");
      rows = episodes;
    }
    if (view === "versions") rows = Array.from({ length: 41 }, (_, i) => ({ ...version(p), list_version_id: `v-${i}` }));
    if (view === "decisions") rows = [{ symbol: "600001.SH", stock_name: "重复入选股票", trade_date: target, action: "EXIT", reason_code: "STOP_LOSS", review_status: "SUCCEEDED" }];
    return json(route, { ...envelope(p), items: rows.slice(offset, offset + limit), total_count: rows.length, limit, offset });
  });
}
const main = (page: Page) => page.getByTestId("advisory-workbench");

test("model waiting shows precise source reason without a task action", async ({ page }) => {
  const state: State = { seen: [], waiting: true }; await setup(page, state);
  await page.goto("/paper-v2/advisory?program_id=lstm");
  await main(page).getByRole("button", { name: "模型观察", exact: true }).click();
  await expect(main(page).getByText(/等待输入数据完成验证/)).toBeVisible();
  await expect(main(page).getByText(/index_daily 2026-10-27/)).toBeVisible();
  expect(state.seen.every(request => request.startsWith("GET "))).toBe(true);
});

test("overview exposes the five tasks, true old date and null samples; sidebar links to overview", async ({ page }) => {
  const state: State = { seen: [] }; await setup(page, state); await page.goto("/paper-v2/advisory");
  await expect(main(page).getByRole("heading", { name: "荐股总览" })).toBeVisible();
  await expect(main(page).locator("article")).toHaveCount(5);
  await expect(main(page).getByText("2026-10-09", { exact: true })).toBeVisible();
  await expect(main(page).getByText("暂无已结束样本", { exact: true })).toBeVisible();
  const nav = page.locator(".sidebar").getByRole("link", { name: /荐股$/ });
  await expect(nav).toHaveAttribute("href", "/paper-v2/advisory"); await expect(nav).toHaveAttribute("aria-current", "page");
  expect(state.seen.some(x => x.includes("model-shadow") || x.includes("reranking-shadow"))).toBe(false);
});

test("daily membership separates additions, continuing and removed; awaiting entry has no fake entry price", async ({ page }) => {
  const state: State = { seen: [] }; await setup(page, state); await page.goto("/paper-v2/advisory?program_id=lstm");
  await expect(main(page).getByRole("heading", { name: "当日推荐名单" })).toBeVisible();
  await expect(main(page).getByText("范围内 · 8 只", { exact: true })).toBeVisible();
  await main(page).getByRole("button", { name: "新加入", exact: true }).click();
  await expect(main(page).getByRole("button", { name: "当日股票 A", exact: true })).toBeVisible();
  await expect(main(page).getByRole("button", { name: "当日股票 B", exact: true })).toHaveCount(0);
  await main(page).getByRole("button", { name: "当日股票 A", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "股票荐股详情" }); await expect(dialog).toBeVisible();
  await expect(dialog.getByText(/尚未建立有效进入/)).toBeVisible();
  await page.keyboard.press("Escape"); await expect(dialog).toHaveCount(0);
  await expect(main(page).getByRole("heading", { name: /今日移出/ })).toBeVisible();
  expect(state.seen.some(x => x.includes("model-shadow"))).toBe(false);
});

test("history pages are requested from server and repeated stock episodes remain distinct", async ({ page }) => {
  const state: State = { seen: [] }; await setup(page, state); await page.goto("/paper-v2/advisory?program_id=lstm");
  await main(page).getByRole("button", { name: "历史记录", exact: true }).click();
  await expect(main(page).getByRole("button", { name: "重复入选股票", exact: true })).toHaveCount(2);
  await main(page).getByRole("button", { name: "重复入选股票", exact: true }).nth(1).click();
  await expect(page.getByRole("dialog")).toBeVisible();
  expect(state.seen.some(x => x.includes("/episodes/episode-1?"))).toBe(true);
  await page.getByRole("button", { name: "关闭股票详情" }).click();
  await main(page).getByRole("button", { name: "下一页", exact: true }).click();
  await expect(main(page).getByRole("button", { name: "历史股票 21", exact: true })).toBeVisible();
  expect(state.seen.some(x => x.includes("/episodes?") && x.includes("offset=20") && x.includes("snapshot_id="))).toBe(true);
  await main(page).getByLabel("记录来源", { exact: true }).selectOption("UNKNOWN");
  await expect(main(page).getByRole("button", { name: "历史股票 41", exact: true })).toBeVisible();
});

test("a blocked current target never silently displays the last published stock list", async ({ page }) => {
  await setup(page, { seen: [] }); await page.goto("/paper-v2/advisory?program_id=parent");
  await expect(main(page).getByText(/所选目标日.*尚无有效推荐名单/)).toBeVisible();
  await expect(main(page).getByRole("button", { name: /查看最近有效名单/ })).toBeVisible();
  await expect(main(page).getByRole("heading", { name: "当日推荐名单" })).toHaveCount(0);
});

test("snapshot conflicts and wrong program identities fail visibly", async ({ page }) => {
  const state: State = { seen: [], conflict: true }; await setup(page, state); await page.goto("/paper-v2/advisory?program_id=lstm");
  await expect(main(page).getByRole("alert").first()).toContainText("数据已更新");
  state.conflict = false; state.wrongIdentity = true;
  await page.reload(); await expect(main(page).getByRole("alert").first()).toContainText("不属于所选策略");
});

test("refresh error retains labelled old cards and model observation is loaded only on selection", async ({ page }) => {
  const state: State = { seen: [] }; await setup(page, state); await page.goto("/paper-v2/advisory");
  await expect(main(page).locator("article")).toHaveCount(5); state.fail = true;
  await main(page).getByRole("button", { name: "刷新", exact: true }).click();
  await expect(main(page).getByRole("alert")).toContainText("最后一次成功");
  await expect(main(page).locator("article")).toHaveCount(5); state.fail = false;
  await main(page).getByRole("link", { name: "LSTM 策略包", exact: true }).click();
  await expect(main(page).getByRole("heading", { name: "当日推荐名单" })).toBeVisible();
  expect(state.seen.some(x => x.includes("reranking-shadow"))).toBe(false);
  await main(page).getByRole("button", { name: "模型观察", exact: true }).click();
  await expect(main(page).getByRole("heading", { name: "重排序模型观察" })).toBeVisible();
  await expect(main(page).getByRole("cell", { name: "12", exact: true })).toBeVisible();
  await expect(main(page).getByRole("cell", { name: "3", exact: true })).toBeVisible();
});

test("narrow viewport keeps page inside its container and stock drawer supports keyboard close", async ({ page }) => {
  await page.setViewportSize({ width: 760, height: 1000 }); await setup(page, { seen: [] });
  await page.goto("/paper-v2/advisory?program_id=lstm");
  await main(page).getByRole("button", { name: "当日股票 A", exact: true }).click();
  await expect(page.getByRole("dialog")).toBeVisible(); await page.keyboard.press("Escape");
  expect(await main(page).evaluate(node => node.scrollWidth <= node.clientWidth + 2)).toBe(true);
});

test("a slow response from the previous program cannot replace the selected program", async ({ page }) => {
  let release!: () => void;
  const state: State = { seen: [], hold: new Promise<void>(resolve => { release = resolve; }) };
  await setup(page, state); await page.goto("/paper-v2/advisory?program_id=lstm");
  await main(page).getByRole("link", { name: /返回荐股总览/ }).click();
  await main(page).getByRole("link", { name: "TCN 策略包", exact: true }).click();
  await expect(main(page).getByRole("heading", { name: "TCN 策略包", exact: true })).toBeVisible();
  release();
  await expect(main(page).getByRole("heading", { name: "当日推荐名单" })).toBeVisible();
  await expect(main(page).getByRole("heading", { name: "LSTM 策略包", exact: true })).toHaveCount(0);
});

test("real read-only capture renders overview daily history and detail without writes", async ({ page }) => {
  test.skip(!process.env.ADVISORY_REAL_CAPTURE, "Explicit task-local real database capture is required");
  const fs = await import("node:fs/promises");
  const nodePath = await import("node:path");
  const captureFile = process.env.ADVISORY_REAL_CAPTURE!;
  const captured = JSON.parse(await fs.readFile(captureFile, "utf8")) as Record<string, any>;
  const overview = captured["/api/v1/advisory/workbench/overview"];
  const lstm = overview.items.find((c: any) => c.program_id === "advp_ed918504a02f4b15892f0dba65da139b");
  expect(overview.items).toHaveLength(5);
  const errors: string[] = []; page.on("pageerror", e => errors.push(e.message));
  const normalized = (raw: string) => {
    const u = new URL(raw, "http://capture");
    for (const [key, value] of [...u.searchParams]) {
      if (["snapshot_id", "target_date"].includes(key) || value === "" || value === "ALL" || (key === "status" && value === "ENABLED") || (key === "group" && value === "IN_SCOPE") || (key === "limit" && value === "20") || (key === "offset" && value === "0")) u.searchParams.delete(key);
    }
    u.searchParams.sort(); return u.pathname + u.search;
  };
  const indexed = new Map(Object.entries(captured).map(([key, value]) => [normalized(key), value]));
  await page.route("**/api/**", async route => {
    expect(route.request().method()).toBe("GET");
    const url = new URL(route.request().url());
    if (!url.pathname.includes("/advisory/") && !url.pathname.includes("/integrity/")) return json(route, { count: 0, items: [] });
    const value = indexed.get(normalized(url.pathname.replace(/^\/api\//, "/api/v1/").replace("/api/v1/v1/", "/api/v1/") + url.search));
    expect(value, `No captured business response for ${url.pathname}${url.search}`).toBeDefined();
    return json(route, value);
  });
  await page.setViewportSize({ width: 1700, height: 1100 });
  await page.goto("/paper-v2/advisory"); await expect(main(page).locator("article")).toHaveCount(5);
  await page.screenshot({ path: nodePath.join(nodePath.dirname(captureFile), "actual-overview.png"), fullPage: true });
  await main(page).getByRole("link", { name: lstm.program.program_name, exact: true }).click();
  await expect(main(page).getByText("范围内 · 21 只", { exact: true })).toBeVisible();
  await page.screenshot({ path: nodePath.join(nodePath.dirname(captureFile), "actual-daily.png"), fullPage: true });
  await main(page).getByRole("button", { name: "历史记录", exact: true }).click();
  const table = main(page).getByRole("table").first();
  await expect(table.getByRole("row")).toHaveCount(21);
  await table.getByRole("button").first().click();
  await expect(page.getByRole("dialog").getByText("逐日调整记录", { exact: true })).toBeVisible();
  await page.screenshot({ path: nodePath.join(nodePath.dirname(captureFile), "actual-history-detail.png"), fullPage: false });
  expect(errors).toEqual([]);
});
