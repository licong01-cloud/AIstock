import { expect, test, type Page, type Route } from "@playwright/test";
import historicalReplay from "../fixtures/advisory-lstm-historical-replay.json";

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
    coverage: { first_published_target_date: "2026-08-14", through: target, missing_dates: [], missing_count: 0, status: "COMPLETE" }, date_options: ["2026-10-26", "2026-10-27", target], source_counts: { FORWARD: 49 }, versions_count: 41, latest_in_scope_count: 8 };
}
function episode(index: number, p = "lstm") {
  return { episode_id: `episode-${index}`, symbol: index < 2 ? "600001.SH" : `600${String(index).padStart(3, "0")}.SH`, stock_name: index < 2 ? "重复入选股票" : `历史股票 ${index + 1}`, status: "EXITED", signal_date: "2026-10-12", effective_entry_date: "2026-10-13", effective_exit_date: "2026-10-20", exit_signal_date: "2026-10-19", entry_price: 18.5, exit_price: 17.8, current_rank: null, entry_rank: 1, exit_reason: index % 2 ? "STOP_LOSS" : "TAKE_PROFIT", holding_trading_days: 5, performance_state: "SETTLED", performance_reason_code: null, return_bps: index % 2 ? -410 : 620, return_kind: "REALIZED", progress: "ENDED", mark_date: "2026-10-27", mark_price: 17.8, source: index === 40 ? "UNKNOWN" : "FORWARD", binding_version_id: `${p}-binding`, metric_as_of_trade_date: "2026-10-27" };
}
const stocks = Array.from({ length: 8 }, (_, i) => ({ symbol: `60010${i}.SH`, stock_name: `当日股票 ${String.fromCharCode(65 + i)}`, episode_id: [0, 2, 6].includes(i) ? null : `episode-${i}`, rank: i + 1, membership: "IN_SCOPE", change: [0, 2, 6].includes(i) ? "NEW" : "CONTINUING", progress: [0, 2, 6].includes(i) ? "PENDING_ENTRY" : "TRACKING", consecutive_days: [0, 2, 6].includes(i) ? 1 : i + 1, reference_price: 42.18, reference_date: "2026-10-27", episode: [0, 2, 6].includes(i) ? undefined : { ...episode(i), status: "ACTIVE", progress: "TRACKING", return_kind: "UNREALIZED" } }));
type State = { seen: string[]; fail?: boolean; conflict?: boolean; hold?: Promise<void>; wrongIdentity?: boolean; waiting?: boolean; dense?: boolean; business?: "complete" | "unknown" | "immature"; businessMissed?: boolean };
function businessShadow(mode: NonNullable<State["business"]>, missed = false) {
  const complete = mode === "complete", unknown = mode === "unknown";
  const evaluation = { status: complete ? "COMPLETE" : unknown ? "UNKNOWN" : "IMMATURE_OR_PENDING", native_net_bps: complete || unknown ? 200 : null, specialist_net_bps: complete ? 400 : null, delta_bps: complete ? 200 : null, native_unknown_slots: unknown ? 0 : complete ? 0 : null, specialist_unknown_slots: unknown ? 1 : complete ? 0 : null, native_cash_slots: complete ? 0 : null, specialist_cash_slots: complete ? 0 : null };
  const rows = Array.from({ length: 50 }, (_, i) => ({ instrument: `600${String(i + 1).padStart(3, "0")}.SH`, selection_effective_rank: i + 1, advisory_rank: i === 0 ? 11 : i === 10 ? 1 : i + 1, change: i === 0 ? "EXITED" : i === 10 ? "ENTERED" : i < 10 ? "RETAINED" : "OUTSIDE", net_bps: complete ? i === 0 ? 2000 : i === 10 ? 4000 : 0 : null, delta_contribution_bps: complete ? i === 0 ? -200 : i === 10 ? 400 : 0 : null }));
  return { ok: true, program_id: "lstm", status: "SHADOW_ONLY", captured_days: 1, complete_days: complete ? 1 : 0, mean_delta_bps: evaluation.delta_bps,
    missed: missed ? [{ publication: { decision_date: "2026-10-27", target_date: "2026-10-28" } }, { publication: { decision_date: "2026-11-02", target_date: "2026-11-03" } }] : [],
    business_contract: { role: "TOP10_AUXILIARY_OBSERVATION", target_increment_bps: 100 },
    business_summary: { native_mean_net_bps: complete ? 200 : null, specialist_mean_net_bps: complete ? 400 : null, mean_delta_bps: evaluation.delta_bps, captured_days: 1, complete_pairs: complete ? 1 : 0, unknown_days: unknown ? 1 : 0, unevaluated_days: !complete && !unknown ? 1 : 0, target_status: complete ? "POINT_ESTIMATE_AT_OR_ABOVE_TARGET" : "NOT_EVALUATED" },
    selected: { target_date: target, observed_at: stamp, predictions: rows, business_review: { ...evaluation, rows, changes: { ENTERED: 1, EXITED: 1, RETAINED: 9, OUTSIDE: 39 }, components: { added_profit_bps: complete ? 400 : null, missed_profit_bps: complete ? 200 : null, avoided_loss_bps: complete ? 0 : null, added_loss_bps: complete ? 0 : null }, target_status: complete ? "POINT_ESTIMATE_AT_OR_ABOVE_TARGET" : "NOT_EVALUATED", target_increment_bps: 100 } },
    days: [{ target_date: target, observed_at: stamp, evaluation }] };
}
function comparisonFixture(url: URL) {
  const distribution = (value: number) => ({ sample_count: 8, avg_return_bps: value, median_return_bps: value - 10, win_rate: .625, avg_win_bps: 320, avg_loss_bps: -180, payoff_ratio: 1.78, profit_factor: 2.96 });
  const ids = url.searchParams.get("program_ids")?.split(",") || programIds;
  return { schema_version: "advisory_native_comparison_v1", source_version: snapshot, generated_at: stamp, range: { start: url.searchParams.get("start") || "2026-08-10", end: url.searchParams.get("end") || target, period: url.searchParams.get("period") || "60" }, cost: { buy_cost_bps: .95, sell_cost_bps: 5.95, slippage_bps: 0 }, filters: { source: url.searchParams.get("source") || "FORWARD", binding: "CURRENT", sample: url.searchParams.get("sample") || "COMMON" }, common_count: 8, status: "READY", items: programIds.filter(p => ids.includes(p)).map((p, i) => ({ arm_id: p, program_id: p, name: names[programIds.indexOf(p)], binding_version_id: `${p}-binding`, fixed_5td: { net: distribution(i * 80 - 100), gross: distribution(i * 80 - 93), excess: distribution(i * 80 - 140), excess_interval_bps: [-160, 170], worst_bps: -450, tail_10pct_bps: -380 }, coverage: { published_count: 12, mature_count: 8, evaluable_count: 8, compared_count: 8, common_count: 8, missing_publication_dates: [], missing_publication_count: 0, original_slots: 120, unknown_slots: 0, cash_slots: 2, status_counts: { EVALUABLE: 8, IMMATURE: 4 }, source_counts: { FORWARD: 12 } }, tracking: { basis: "AS_OF_EPISODE_ENTRY_COHORTS_OWN_SAMPLE", gross: distribution(-224), net: distribution(-231), floating_gross: distribution(123), floating_liquidation_net: distribution(116), avg_holding_days: 5, exit_reasons: { STOP_LOSS: 16 }, missing_mark_count: 0, pending_exit_count: 1, mark_date: target, as_of: target }, portfolio: { status: "READY", reason: null, total_return_bps: i * 120 - 200, max_drawdown_bps: -180 + i * 15, cash_fraction: .15, invested_fraction: .85, cost_drag_bps: 12, unknown_from: null }, benchmark_portfolio: { status: "READY", reason: null, total_return_bps: 100, max_drawdown_bps: -80, cash_fraction: .1, invested_fraction: .9, cost_drag_bps: 0, unknown_from: null } })) };
}
async function json(route: Route, body: unknown, status = 200) { await route.fulfill({ status, contentType: "application/json", body: JSON.stringify(body) }); }
async function setup(page: Page, state: State) {
  await page.route("**/api/**", async route => {
    const request = route.request(), url = new URL(request.url());
    state.seen.push(`${request.method()} ${url.pathname}${url.search}`);
    expect(request.method()).toBe("GET");
    const resource = url.pathname.replace("/api/v1", "");
    if (resource === api + "/quotes") {
      const symbols = (url.searchParams.get("symbols") || "").split(",");
      return json(route, { schema_version: "advisory_display_quotes_v1", items: Object.fromEntries(symbols.map(symbol => [symbol, { symbol, price: symbol === "600102.SH" ? null : 46.398, kind: "CLOSE", trade_date: target, observed_at: stamp, reason_code: symbol === "600102.SH" ? "TDX_CLOSE_MISSING" : null }])) });
    }
    if (resource.includes("/integrity/days/")) return json(route, { fact_date: target, all_sync_complete: false });
    if (resource.endsWith("/reranking-shadow")) return json(route, state.business ? businessShadow(state.business, state.businessMissed) : { ok: true, program_id: "lstm", status: "SHADOW_ONLY", captured_days: 1, complete_days: 0, mean_delta_bps: null, last_waiting: state.waiting ? { results: [{ status: "WAITING_DATA", decision_date: "2026-10-27", message: "输入分区未验证或已修订", context: { dataset: "index_daily", fact_date: "2026-10-27" } }] } : null, selected: { target_date: target, observed_at: stamp, predictions: [{ instrument: "600001.SH", selection_effective_rank: 12, advisory_rank: 3, selected: true }] } });
    if (!resource.startsWith(api)) return json(route, { count: 0, items: [] });
    if (resource === api + "/overview") {
      if (state.fail) return json(route, { detail: { message: "读取失败，请重试" } }, 503);
      return json(route, { schema_version: "advisory_workbench_v1", generated_at: stamp, items: programIds.map(summary), count: 5, status_counts: { PUBLISHED: 4, ASSET_BLOCKED: 1 } });
    }
    if (resource === api + "/comparison") return json(route, comparisonFixture(url));
    if (resource === api + "/comparison/series") return json(route, { ...comparisonFixture(url), series: comparisonFixture(url).items.map((r, i) => ({ arm_id: r.arm_id, name: r.name, nav: [["2026-10-01", 1], [target, 1 + (i - 1) / 100]], benchmark: [["2026-10-01", 1], [target, 1.01]], monthly: [{ month: "2026-10", excess_bps: i * 80 - 140, count: 8 }] })) });
    const parts = resource.slice((api + "/programs/").length).split("/"), p = parts[0], view = parts[1];
    if (state.hold && p === "lstm" && !view) await state.hold;
    if (!view) return json(route, { ...summary(p), program_id: state.wrongIdentity ? "wrong" : p });
    if (view === "returns") {
      if (state.dense) await new Promise(resolve => setTimeout(resolve, 200));
      const symbols = (url.searchParams.get("symbols") || "").split(",");
      return json(route, { schema_version: "advisory_display_quotes_v1", return_schema_version: "advisory_recommendation_returns_v1", program_id: p, list_version_id: url.searchParams.get("list_version_id"), items: Object.fromEntries(symbols.map(symbol => [symbol, { symbol, price: symbol === "600102.SH" ? null : 46.398, kind: "CLOSE", trade_date: target, observed_at: stamp, today_return_bps: symbol === "600102.SH" ? null : 500, reason_code: symbol === "600102.SH" ? "TDX_CLOSE_MISSING" : null,
        recommendation: { segment_id: `${p}-${symbol}`, start_date: "2026-10-26", base_price: 42.18, known_through_date: target, end_date: symbol.startsWith("6002") ? target : null, end_price: symbol.startsWith("6002") ? 43 : null, return_bps: symbol === "600102.SH" ? null : symbol.startsWith("6002") ? 194.405 : 1000, state: symbol.startsWith("6002") ? "ENDED" : "RECOMMENDED", valuation_date: target, reason_code: symbol === "600102.SH" ? "QUOTE_NOT_VALID_FOR_SEGMENT" : null } }])) });
    }
    if (view === "readiness") return json(route, { program_id: p, data_date: "2026-10-27", status: "AVAILABLE", items: [{ consumer_ready: false, all_sync_complete: false }], errors: [] });
    if (state.conflict) return json(route, { detail: { reason_code: "SNAPSHOT_CHANGED", message: "读取期间数据已更新，请刷新后继续查看。" } }, 409);
    const limit = Number(url.searchParams.get("limit") || 20), offset = Number(url.searchParams.get("offset") || 0);
    let rows: unknown[] = [];
    if (view === "history" && !parts[2]) {
      let versions = Array.from({ length: 43 }, (_, i) => ({ ...version(p), list_version_id: `${p}-history-${i}`, source: i === 1 ? "REPLAY" : "FORWARD", version_status: i === 1 ? "REPLAY" : "PUBLISHED" }));
      if (url.searchParams.get("source") === "REPLAY") versions = versions.filter(v => v.source === "REPLAY");
      return json(route, { program_id: p, items: versions.slice(offset, offset + limit), total_count: versions.length, limit, offset, generated_at: stamp });
    }
    if (view === "history" && parts[2]) {
      const group = url.searchParams.get("group") || "IN_SCOPE";
      const items = group === "IN_SCOPE" ? [{ ...stocks[0], symbol: "600200.SH", stock_name: "已结束荐股期间股票" }, ...stocks.slice(1)] : [];
      return json(route, { ...envelope(p), list_version_id: parts[2], selected: { ...version(p), list_version_id: parts[2], source: "FORWARD" }, status: "PUBLISHED", items: items.slice(offset, offset + limit), total_count: items.length, limit, offset, summary: { in_scope: 8, new: 3, continuing: 5, removed: 0, unknown_change: 0, observation: 0, outside_pending: 0, unknown: 0, previous_count: 5, comparison_known: true } });
    }
    if (view === "daily") {
      if (p === "parent") return json(route, { ...envelope(p), status: "NO_CURRENT_LIST", items: [], total_count: 0, limit, offset, summary: null });
      const group = url.searchParams.get("group") || "IN_SCOPE";
      let items = group === "IN_SCOPE" ? stocks : group === "REMOVED" || group === "OUTSIDE_PENDING" ? ["I", "J"].map((name, i) => ({ ...stocks[i], symbol: `60020${i}.SH`, stock_name: `移出股票 ${name}`, membership: "OUTSIDE_PENDING", change: "REMOVED", progress: "WAITING_REVIEW", reason_code: "PENDING_TARGET_OPEN_REVIEW" })) : [];
      if (state.dense && group === 'IN_SCOPE') items = Array.from({ length: 25 }, (_, i) => ({ ...stocks[i % stocks.length], symbol: `600${100 + i}.SH`, stock_name: `密集名单 ${i + 1}`, rank: i + 1 }));
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

// Only the observation payload is real archived evidence. Program shell metadata
// is test transport and does not assert that this Program existed in 2025.
async function historicalSetup(page: Page, mode: keyof typeof historicalReplay.cases, invalid = false) {
  const payload = structuredClone(historicalReplay.cases[mode]);
  const state: State = { seen: [] };
  await setup(page, state);
  if (invalid) payload.replay_context.schema = "unrecognized";
  await page.route("**/api/**", async route => {
    const request = route.request(), url = new URL(request.url());
    const resource = url.pathname.replace("/api/v1", "");
    if (resource === `/advisory/programs/${payload.program_id}/reranking-shadow`) {
      state.seen.push(`${request.method()} ${url.pathname}${url.search}`);
      expect(request.method()).toBe("GET");
      expect(url.searchParams.get("target_trade_date")).toBe(payload.selected.target_date);
      return json(route, payload);
    }
    if (resource.startsWith(`${api}/programs/${payload.program_id}`)) {
      state.seen.push(`${request.method()} ${url.pathname}${url.search}`);
      expect(request.method()).toBe("GET");
      const shell = { ...summary("lstm"), program_id: payload.program_id,
        program: { ...summary("lstm").program, program_id: payload.program_id, program_name: "LSTM历史工程回放验收" },
        target_date: payload.selected.target_date, default_target_date: payload.selected.target_date,
        selected: { ...version(payload.program_id, payload.selected.target_date), date_context: { selection_as_of_trade_date: payload.selected.decision_date } },
        date_options: [payload.selected.target_date] };
      if (resource.endsWith(`/${payload.program_id}`)) return json(route, shell);
      if (resource.endsWith("/daily")) return json(route, { ...shell, status: "NO_CURRENT_LIST", items: [], total_count: 0, limit: 20, offset: 0, summary: null });
    }
    return route.fallback();
  });
  await page.goto(`/paper-v2/advisory?program_id=${payload.program_id}`);
  await main(page).getByRole("button", { name: "模型观察", exact: true }).click();
  return { payload, state };
}

// JSON transport canonicalizes IEEE negative zero to zero.
const historicalBps = (v: number | null) => v == null ? "—" : `${v > 0 ? "+" : ""}${((v || 0) / 100).toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`;
for (const mode of ["complete", "unknown", "immature"] as const) {
  test(`real archived historical replay ${mode} preserves values and disables live quotes`, async ({ page }) => {
    const errors: string[] = []; page.on("pageerror", error => errors.push(error.message));
    await page.setViewportSize({ width: 1700, height: 1000 });
    expect(historicalReplay.provenance.real_historical_data).toBe(true);
    const { payload, state } = await historicalSetup(page, mode);
    await expect(page.getByTestId("reranking-replay-evidence")).toContainText(payload.replay_context.training_cutoff);
    await expect(page.getByTestId("reranking-replay-evidence")).toContainText("模拟");
    const selected = page.getByTestId("reranking-selected-review"), review = payload.selected.business_review;
    for (const [label, value] of [["所选批次原生Top10", review.native_net_bps], ["所选批次辅助Top10", review.specialist_net_bps], ["所选批次净增量", review.delta_bps]] as const) {
      await expect(selected.locator("div").filter({ has: page.getByText(label, { exact: true }) }).last().locator("strong")).toHaveText(historicalBps(value));
    }
    await selected.getByText("查看增量来源与损益分解", { exact: true }).click();
    for (const [label, value] of [["新增盈利贡献", review.components.added_profit_bps], ["错失盈利贡献", review.components.missed_profit_bps], ["避免损失贡献", review.components.avoided_loss_bps], ["新增损失贡献", review.components.added_loss_bps]] as const) {
      await expect(selected.locator("div").filter({ has: page.getByText(label, { exact: true }) }).last().locator("strong")).toHaveText(historicalBps(value));
    }
    const table = page.getByTestId("reranking-business-rows");
    await expect(table.locator("tbody tr")).toHaveCount(10);
    await main(page).getByRole("button", { name: "原生Top10", exact: true }).click();
    await expect(table.locator("tbody tr")).toHaveCount(10);
    await main(page).getByRole("button", { name: "全部原始候选", exact: true }).click();
    await expect(table.locator("tbody tr")).toHaveCount(50);
    for (const row of review.rows) {
      const actual = table.getByRole("row").filter({ has: page.getByRole("cell", { name: row.instrument, exact: true }) });
      await expect(actual.locator("td").nth(1)).toHaveText("不适用");
      await expect(actual.locator("td").nth(5)).toContainText(historicalBps(row.net_bps));
      await expect(actual.locator("td").nth(6)).toHaveText(historicalBps(row.delta_contribution_bps));
    }
    expect(state.seen.filter(url => /\/(quotes|returns)\?/.test(url))).toEqual([]);
    expect(errors).toEqual([]);
    if (process.env.ADVISORY_HISTORY_SCREENSHOT_DIR) {
      await main(page).getByRole("button", { name: "辅助Top10", exact: true }).click();
      await page.screenshot({ path: `${process.env.ADVISORY_HISTORY_SCREENSHOT_DIR}/${mode}.png`, fullPage: true });
    }
  });
}

test("real archived historical replay rejects malformed evidence identity", async ({ page }) => {
  const { state } = await historicalSetup(page, "complete", true);
  await expect(main(page).getByRole("alert")).toContainText("历史回放证据身份不完整");
  await expect(page.getByTestId("reranking-business-rows")).toHaveCount(0);
  expect(state.seen.filter(url => /\/(quotes|returns)\?/.test(url))).toEqual([]);
});

test('dense recommendation rows do not refetch on quote-induced height changes', async ({ page }) => {
  const state: State = { seen: [], dense: true };
  await page.setViewportSize({ width: 1700, height: 900 });
  await setup(page, state);
  await page.goto('/paper-v2/advisory?program_id=lstm');
  const row = main(page).getByRole('row').filter({ has: page.getByRole('button', { name: '密集名单 12', exact: true }) });
  await row.scrollIntoViewIfNeeded();
  await expect(row.getByTestId('advisory-recommendation-return')).toContainText('+10.00%');
  // Observe real layout/IntersectionObserver callbacks; do not fake the visibility contract.
  await page.waitForTimeout(5500);
  const reads = state.seen.filter(url => url.includes('/returns?'));
  expect(reads.length, 'initial visibility stabilization must not become a request loop').toBeLessThanOrEqual(6);
  await expect(row.getByTestId('advisory-recommendation-return')).toContainText('+10.00%');
});

test("recommendation returns include new stocks without an episode and keep process returns separate", async ({ page }) => {
  const state: State = { seen: [] }; await setup(page, state);
  await page.goto("/paper-v2/advisory?program_id=lstm");
  const row = main(page).getByRole("row").filter({ has: page.getByRole("button", { name: "当日股票 A", exact: true }) });
  await expect(row.getByTestId("advisory-current-price")).toContainText("¥46.40");
  await expect(row.getByTestId("advisory-current-price")).toContainText("最近收盘");
  await expect(row.getByTestId("advisory-recommendation-return")).toContainText("+10.00%");
  await expect(row.getByTestId("advisory-today-change")).toContainText("+5.00%");
  await expect(row.getByTestId("advisory-recommendation-base")).toContainText("¥42.18");
  await expect(row).toContainText("新加入");
  await expect(main(page)).not.toContainText("待进入");
  await expect(row).not.toContainText("过程参考收益");
  const missing = main(page).getByRole("row").filter({ has: page.getByRole("button", { name: "当日股票 C", exact: true }) });
  await expect(missing.getByTestId("advisory-current-price")).toContainText("TDX暂无收盘价");
  await expect(missing.getByTestId("advisory-recommendation-return")).toContainText("等待本段有效行情");
  expect(state.seen.filter(s => s.includes("/returns")).every(s => s.startsWith("GET ") && s.includes("list_version_id="))).toBe(true);
  const removed = main(page).getByRole("row").filter({ has: page.getByRole("button", { name: "移出股票 I", exact: true }) }).first();
  await removed.scrollIntoViewIfNeeded();
  await expect(removed.getByTestId("advisory-recommendation-return")).toContainText("已结束·冻结于 2026-10-28 开盘");
  await main(page).getByRole("button", { name: "跟踪历史", exact: true }).click();
  const old = main(page).getByRole("row").filter({ hasText: "重复入选股票" }).first();
  await expect(old.getByTestId("advisory-current-price")).toContainText("¥46.40");
  await expect(old).toContainText("+6.20%");
  await expect(old).toContainText("已结束参考收益");
});

test("quote request failures remain visible and do not hide recommendation rows", async ({ page }) => {
  const state: State = { seen: [] }; await setup(page, state);
  await page.route("**/advisory/workbench/programs/lstm/returns?**", route => json(route, { detail: "TDX unavailable" }, 503));
  await page.goto("/paper-v2/advisory?program_id=lstm");
  const row = main(page).getByRole("row").filter({ has: page.getByRole("button", { name: "当日股票 A", exact: true }) });
  await expect(row.getByTestId("advisory-current-price")).toContainText("行情或名单身份不可用，请刷新");
  await expect(row.getByTestId("advisory-recommendation-return")).toContainText("推荐涨跌暂不可用");
  await expect(row.getByRole("button", { name: "当日股票 A", exact: true })).toBeVisible();
});

test("polling removes previous returns when the pinned response identity changes", async ({ page }) => {
  await page.clock.install();
  await setup(page, { seen: [] });
  await page.goto('/paper-v2/advisory?program_id=lstm');
  await page.clock.runFor(100);
  const row = main(page).getByRole('row').filter({ has: page.getByRole('button', { name: '当日股票 A', exact: true }) });
  await expect(row.getByTestId('advisory-recommendation-return')).toContainText('+10.00%');
  await page.route('**/advisory/workbench/programs/lstm/returns?**', async route => {
    await json(route, { schema_version: 'advisory_display_quotes_v1', return_schema_version: 'advisory_recommendation_returns_v1', program_id: 'wrong-program', list_version_id: 'wrong-list', items: {} });
  });
  await page.clock.runFor(15100);
  await expect(row.getByTestId('advisory-recommendation-return')).toContainText('推荐涨跌暂不可用');
  await expect(row.getByTestId('advisory-recommendation-return')).not.toContainText('+10.00%');
  await expect(row.getByRole('button', { name: '当日股票 A', exact: true })).toBeVisible();
});

test("model waiting shows precise source reason without a task action", async ({ page }) => {
  const state: State = { seen: [], waiting: true }; await setup(page, state);
  await page.goto("/paper-v2/advisory?program_id=lstm");
  await main(page).getByRole("button", { name: "模型观察", exact: true }).click();
  await expect(main(page).getByText(/等待输入数据完成验证/)).toBeVisible();
  await expect(main(page).getByText(/index_daily 2026-10-27/)).toBeVisible();
  expect(state.seen.every(request => request.startsWith("GET "))).toBe(true);
});

test("LSTM auxiliary business review shows both arms, user target and exact membership filters", async ({ page }) => {
  const state: State = { seen: [], business: "complete" }; await setup(page, state);
  await page.setViewportSize({ width: 1700, height: 1000 });
  await page.goto("/paper-v2/advisory?program_id=lstm");
  await main(page).getByRole("button", { name: "模型观察", exact: true }).click();
  await expect(main(page).getByText(/100bps（1个百分点）/)).toBeVisible();
  const selected = page.getByTestId("reranking-selected-review");
  await expect(selected).toContainText("+2.00%");
  await expect(selected).toContainText("+4.00%");
  await expect(selected).toContainText("当前点估计达到参考目标");
  await selected.getByText("查看增量来源与损益分解", { exact: true }).click();
  await expect(selected.getByText("新增盈利贡献", { exact: true })).toBeVisible();
  await selected.getByText("查看增量来源与损益分解", { exact: true }).click();
  const rows = page.getByTestId("reranking-business-rows");
  await expect(rows.locator("tbody tr")).toHaveCount(10);
  await expect(rows).toContainText("换入辅助Top10");
  await expect(rows).toContainText("两榜均入选");
  await rows.locator("tbody tr").first().scrollIntoViewIfNeeded();
  await expect(rows).toContainText("最近收盘");
  if (process.env.ADVISORY_BUSINESS_SCREENSHOT) {
    await page.evaluate(() => { const note = document.createElement("p"); note.textContent = "合成合同测试数据 · 非真实收益成果"; document.querySelector("main")?.prepend(note); });
    await page.screenshot({ path: process.env.ADVISORY_BUSINESS_SCREENSHOT, fullPage: true });
  }
  await main(page).getByRole("button", { name: "原生Top10", exact: true }).click();
  await expect(rows.locator("tbody tr")).toHaveCount(10);
  await expect(rows).toContainText("移出辅助Top10");
  await main(page).getByRole("button", { name: "全部原始候选", exact: true }).click();
  await expect(rows.locator("tbody tr")).toHaveCount(50);
  await expect(rows).toContainText("两榜范围外");
  await expect(page.getByTestId("reranking-history")).toContainText("+4.00%");
  expect(state.seen.every(request => request.startsWith("GET "))).toBe(true);
});

for (const mode of ["immature", "unknown"] as const) {
  test(`LSTM auxiliary ${mode} keeps unavailable valuations blank and current quotes separate`, async ({ page }) => {
    const state: State = { seen: [], business: mode }; await setup(page, state);
    await page.goto("/paper-v2/advisory?program_id=lstm");
    await main(page).getByRole("button", { name: "模型观察", exact: true }).click();
    const selected = page.getByTestId("reranking-selected-review");
    await expect(selected).toContainText("尚无完整配对评价");
    await expect(selected).not.toContainText("+0.00%");
    const rows = page.getByTestId("reranking-business-rows");
    await rows.locator("tbody tr").first().scrollIntoViewIfNeeded();
    await expect(rows).toContainText("最近收盘");
    await expect(page.getByTestId("reranking-business-summary")).toContainText(mode === "unknown" ? "未知评价批次" : "未评价批次");
    expect(state.seen.every(request => request.startsWith("GET "))).toBe(true);
  });
}

test("LSTM capture gap is visible early and excludes pre-confirmation pilot misses", async ({ page }) => {
  const state: State = { seen: [], business: "immature", businessMissed: true }; await setup(page, state);
  await page.goto("/paper-v2/advisory?program_id=lstm");
  await main(page).getByRole("button", { name: "模型观察", exact: true }).click();
  await expect(page.getByTestId("reranking-capture-gaps")).toContainText("2026-11-02");
  await expect(page.getByTestId("reranking-capture-gaps")).not.toContainText("2026-10-27");
  await expect(page.getByTestId("reranking-capture-gaps")).toContainText("原生推荐继续运行");
  expect(state.seen.every(request => request.startsWith("GET "))).toBe(true);
});

test("overview exposes the five tasks, true old date and null samples; sidebar links to overview", async ({ page }) => {
  const state: State = { seen: [] }; await setup(page, state); await page.goto("/paper-v2/advisory");
  await expect(main(page).getByRole("heading", { name: "荐股总览" })).toBeVisible();
  await expect(main(page).locator("article")).toHaveCount(5);
  const firstCard = main(page).locator("article").first();
  await expect(firstCard).toContainText("最新名单范围8");
  await expect(main(page).getByText("2026-10-09", { exact: true })).toBeVisible();
  await expect(main(page).getByRole("heading", { name: "策略包收益对比", exact: true })).toBeVisible();
  const nav = page.locator(".sidebar").getByRole("link", { name: /荐股$/ });
  await expect(nav).toHaveAttribute("href", "/paper-v2/advisory"); await expect(nav).toHaveAttribute("aria-current", "page");
  expect(state.seen.some(x => x.includes("model-shadow") || x.includes("reranking-shadow"))).toBe(false);
});

test("daily membership separates additions and removals from strategy tracking", async ({ page }) => {
  const state: State = { seen: [] }; await setup(page, state); await page.goto("/paper-v2/advisory?program_id=lstm");
  await expect(main(page).getByRole("heading", { name: "当日推荐名单" })).toBeVisible();
  await expect(main(page).getByText("范围内 · 8 只", { exact: true })).toBeVisible();
  await main(page).getByRole("button", { name: "新加入", exact: true }).click();
  await expect(main(page).getByRole("button", { name: "当日股票 A", exact: true })).toBeVisible();
  await expect(main(page).getByRole("button", { name: "当日股票 B", exact: true })).toHaveCount(0);
  await main(page).getByRole("button", { name: "当日股票 A", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "股票荐股详情" }); await expect(dialog).toBeVisible();
  await expect(dialog.getByTestId("advisory-recommendation-return")).toContainText("+10.00%");
  await dialog.getByText("策略过程复盘 · 与名单累计涨跌分别计算", { exact: true }).click();
  await expect(dialog.getByText(/尚无可关联的策略跟踪过程/)).toBeVisible();
  await expect(dialog).not.toContainText("待进入");
  await page.keyboard.press("Escape"); await expect(dialog).toHaveCount(0);
  await expect(main(page).getByRole("heading", { name: /今日移出/ })).toBeVisible();
  expect(state.seen.some(x => x.includes("model-shadow"))).toBe(false);
});

test("history pages are requested from server and repeated stock episodes remain distinct", async ({ page }) => {
  const state: State = { seen: [] }; await setup(page, state); await page.goto("/paper-v2/advisory?program_id=lstm");
  await main(page).getByRole("button", { name: "跟踪历史", exact: true }).click();
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
  await main(page).getByLabel("策略荐股概况", { exact: true }).getByRole("link", { name: "LSTM 策略包", exact: true }).click();
  await expect(main(page).getByRole("heading", { name: "当日推荐名单" })).toBeVisible();
  expect(state.seen.some(x => x.includes("reranking-shadow"))).toBe(false);
  await main(page).getByRole("button", { name: "模型观察", exact: true }).click();
  await expect(main(page).getByRole("heading", { name: "重排序模型观察" })).toBeVisible();
  await expect(main(page).getByText("业务收益视图尚未加载，保留原观察结果。", { exact: true })).toBeVisible();
  await expect(main(page).getByText("已捕获交易日", { exact: true })).toBeVisible();
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
  await main(page).getByLabel("策略荐股概况", { exact: true }).getByRole("link", { name: "TCN 策略包", exact: true }).click();
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
  await main(page).getByRole("button", { name: "跟踪历史", exact: true }).click();
  const table = main(page).getByRole("table").first();
  await expect(table.getByRole("row")).toHaveCount(21);
  await table.getByRole("button").first().click();
  await expect(page.getByRole("dialog").getByText("逐日调整记录", { exact: true })).toBeVisible();
  await page.screenshot({ path: nodePath.join(nodePath.dirname(captureFile), "actual-history-detail.png"), fullPage: false });
  expect(errors).toEqual([]);
});


test("comparison applies date selection explicitly, expands all metrics and loads charts only on demand", async ({ page }) => {
  const state: State = { seen: [] }; await setup(page, state); await page.goto("/paper-v2/advisory");
  const comparison = main(page).getByRole("region", { name: "策略包收益横向对比" });
  await expect(comparison.getByRole("columnheader", { name: "5TD净超额" })).toBeVisible();
  expect(state.seen.filter(x => x.includes("/comparison/series"))).toHaveLength(0);
  expect(state.seen.some(x => x.includes("mode=brief"))).toBe(true);
  await comparison.getByRole("button", { name: "展开LSTM 策略包指标" }).click();
  await expect(comparison.getByText("实际过程 · 已结束净收益", { exact: false })).toBeVisible();
  await expect(comparison.getByText("平均盈利 / 亏损", { exact: true })).toHaveCount(2);
  const before = state.seen.filter(x => x.includes("/comparison?")).length;
  await comparison.getByRole("button", { name: "自定义", exact: true }).click();
  await comparison.getByLabel("对比开始日期").fill("2026-10-01");
  await comparison.getByLabel("对比结束日期").fill("2026-10-20");
  expect(state.seen.filter(x => x.includes("/comparison?")).length).toBe(before);
  await comparison.getByRole("button", { name: "应用对比", exact: true }).click();
  await expect(comparison.getByText("2026-10-01 — 2026-10-20", { exact: true })).toBeVisible();
  await comparison.getByRole("button", { name: "查看横向对比图表", exact: true }).click();
  await expect(comparison.getByRole("img", { name: /参考组合与沪深300净值曲线/ })).toBeVisible();
  expect(state.seen.filter(x => x.includes("/comparison/series"))).toHaveLength(1);
  if (process.env.ADVISORY_COMPARISON_SCREENSHOT) {
    await page.evaluate(() => { const note = document.createElement("p"); note.textContent = "界面合同测试数据 · 非实际收益"; document.querySelector("main")?.prepend(note); });
    await page.screenshot({ path: process.env.ADVISORY_COMPARISON_SCREENSHOT, fullPage: true });
  }
});

test("historical lists live only in detail, paginate saved versions and pin cumulative returns to the selected list", async ({ page }) => {
  const state: State = { seen: [] }; await setup(page, state); await page.goto("/paper-v2/advisory");
  await expect(main(page).getByRole("heading", { name: "荐股总览" })).toBeVisible();
  await expect(main(page).getByRole("button", { name: "历史名单", exact: true })).toHaveCount(0);
  expect(state.seen.some(x => x.includes("/history"))).toBe(false);
  await page.goto("/paper-v2/advisory?program_id=lstm");
  await main(page).getByRole("button", { name: "历史名单", exact: true }).click();
  const section = main(page).locator("section").filter({ has: page.getByRole("heading", { name: "历史名单", exact: true }) });
  await expect(section.getByRole("row")).toHaveCount(21);
  await section.getByRole("button", { name: "下一页", exact: true }).click();
  await expect(section.getByRole("button", { name: /名单版本lstm-history-20$/ })).toBeVisible();
  await section.getByRole("button", { name: /名单版本lstm-history-20$/ }).click();
  await expect(main(page).getByRole("columnheader", { name: "荐股期间累计涨跌", exact: true }).first()).toBeVisible();
  const row = main(page).getByRole("row").filter({ hasText: "已结束荐股期间股票" });
  await expect(row).toContainText("1.94%");
  await expect(row).toContainText("已结束·冻结于");
  expect(state.seen.some(x => x.includes("list_version_id=lstm-history-20"))).toBe(true);
  await main(page).getByRole("button", { name: "← 返回历史名单目录", exact: true }).click();
  await expect(main(page).getByRole("button", { name: /名单版本lstm-history-20$/ })).toBeVisible();
});

test("comparison narrow viewport stays within page and stale source charts remain visibly unavailable", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const state: State = { seen: [] }; await setup(page, state);
  await page.route("**/comparison/series?**", route => json(route, { ...comparisonFixture(new URL(route.request().url())), source_version: "b".repeat(64), series: [] }));
  await page.goto("/paper-v2/advisory");
  await main(page).getByRole("button", { name: "查看横向对比图表", exact: true }).click();
  await expect(main(page).getByText("来源版本已变化，请刷新对比后重新查看图表。", { exact: true })).toBeVisible();
  const dimensions = await page.evaluate(() => ({ width: innerWidth, document: document.documentElement.scrollWidth,
    oversized: [...document.querySelectorAll("main *")].filter(n => n.getBoundingClientRect().right > innerWidth + 1).slice(0, 10).map(n => ({ tag: n.tagName, class: n.className, right: n.getBoundingClientRect().right })) }));
  console.log(JSON.stringify(dimensions));
  await page.screenshot({ path: test.info().outputPath("narrow-comparison.png"), fullPage: true });
  expect(dimensions.document <= dimensions.width + 1).toBe(true);
});

test("saved list deep link opens its exact version without a date guess", async ({ page }) => {
  const state: State = { seen: [] }; await setup(page, state);
  await page.goto("/paper-v2/advisory?program_id=lstm&list_version_id=lstm-history-42");
  await expect(main(page).getByText("版本 lstm-history-42 · 绑定 lstm-binding", { exact: true })).toBeVisible();
  await expect(main(page).getByRole("row").filter({ hasText: "已结束荐股期间股票" })).toBeVisible();
  expect(state.seen.some(url => url.includes("/history/lstm-history-42"))).toBe(true);
  expect(state.seen.some(url => url.includes("/programs/lstm?") && url.includes("mode=brief"))).toBe(true);
  await main(page).getByRole("button", { name: "当日推荐", exact: true }).click();
  await expect(page).not.toHaveURL(/list_version_id/);
});

test("comparison first paint and controls remain responsive with deferred chart transport", async ({ page }) => {
  test.skip(!process.env.ADVISORY_COMPARISON_PERF, "Opt-in bounded browser performance receipt");
  type Perf = { lcp: number; events: number[]; longTasks: number[] };
  await page.addInitScript(() => {
    const perf = { lcp: 0, events: [] as number[], longTasks: [] as number[] };
    (window as unknown as { advisoryPerf: Perf }).advisoryPerf = perf;
    new PerformanceObserver(list => { for (const e of list.getEntries()) perf.lcp = e.startTime; }).observe({ type: "largest-contentful-paint", buffered: true });
    new PerformanceObserver(list => { for (const e of list.getEntries()) if ("interactionId" in e && Number(e.interactionId) > 0) perf.events.push(e.duration); }).observe({ type: "event", buffered: true, durationThreshold: 16 } as PerformanceObserverInit);
    new PerformanceObserver(list => { for (const e of list.getEntries()) perf.longTasks.push(e.duration); }).observe({ type: "longtask", buffered: true });
  });
  const state: State = { seen: [] }; await setup(page, state);
  const samples: Perf[] = [];
  for (let i = 0; i < 8; i++) {
    await page.goto("/paper-v2/advisory");
    await expect(main(page).getByRole("columnheader", { name: "5TD净超额", exact: true })).toBeVisible();
    await main(page).getByRole("button", { name: "近20交易日", exact: true }).click();
    await main(page).getByRole("button", { name: "应用对比", exact: true }).click();
    await page.evaluate(() => new Promise<void>(resolve => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))));
    samples.push(await page.evaluate(() => (window as unknown as { advisoryPerf: Perf }).advisoryPerf));
  }
  const percentile = (values: number[]) => [...values].sort((a, b) => a - b)[Math.ceil(values.length * .75) - 1] ?? 0;
  const report = { sample_count: samples.length, transport: "synthetic API fixture; isolated warmed Next dev process; max interaction EventTiming per navigation is an INP laboratory proxy, not field INP; no entries means below 16ms observation threshold", lcp_p75_ms: percentile(samples.map(s => s.lcp)), inp_proxy_p75_ms: percentile(samples.map(s => Math.max(16, ...s.events))), event_count: samples.flatMap(s => s.events).length, samples };
  const fs = await import("node:fs/promises");
  await fs.writeFile(process.env.ADVISORY_COMPARISON_PERF!, JSON.stringify(report, null, 2));
  expect(report.lcp_p75_ms).toBeLessThanOrEqual(2500);
  expect(samples.every(s => s.lcp > 0)).toBe(true);
  expect(report.inp_proxy_p75_ms).toBeLessThanOrEqual(200);
  expect(state.seen.some(url => url.includes("/comparison/series"))).toBe(false);
});

test("real captured comparison and saved history render without mutations", async ({ page }) => {
  test.skip(!process.env.ADVISORY_COMPARISON_CAPTURE, "Explicit task-local read-only capture required");
  const fs = await import("node:fs/promises"), nodePath = await import("node:path");
  const file = process.env.ADVISORY_COMPARISON_CAPTURE!;
  const captured = JSON.parse(await fs.readFile(file, "utf8"));
  const errors: string[] = [];
  page.on("pageerror", e => errors.push(e.message));
  await page.route("**/api/**", async route => {
    expect(route.request().method()).toBe("GET");
    const url = new URL(route.request().url());
    const resource = url.pathname.replace("/api/v1", "").replace(/^\/api(?=\/)/, "");
    if (resource.includes("/integrity/")) return json(route, captured.integrity);
    if (!resource.startsWith(api)) return json(route, { items: [], count: 0 });
    if (resource === api + "/overview") return json(route, captured.brief);
    if (resource === api + "/comparison") return json(route, url.searchParams.get("period") === "CUSTOM" ? captured.selected : captured.comparison);
    if (resource === api + "/comparison/series") return json(route, captured.selected_series);
    if (resource.endsWith("/history")) return json(route, Number(url.searchParams.get("offset") || 0) ? captured.history_second : captured.history);
    if (resource.includes("/history/")) {
      const group = url.searchParams.get("group") || "IN_SCOPE";
      return json(route, captured.history_details[group]);
    }
    if (resource.endsWith("/returns")) return json(route, captured.returns);
    if (resource.endsWith("/readiness")) return json(route, captured.readiness);
    if (resource === api + `/programs/${captured.program_id}`) return json(route, url.searchParams.get("mode") === "brief" ? captured.program_metadata : captured.program);
    return json(route, { ...captured.history_details.IN_SCOPE, items: [], total_count: 0, summary: null });
  });
  await page.setViewportSize({ width: 1700, height: 1100 });
  await page.goto("/paper-v2/advisory");
  await expect(main(page).locator("article")).toHaveCount(5);
  await expect(main(page).locator("article").first()).toContainText(`最新名单范围${captured.brief.items[0].latest_in_scope_count}`);
  await expect(main(page).getByText(/当前选择没有可比较的完整成熟样本/)).toBeVisible();
  await main(page).locator("summary").filter({ hasText: "选择策略与费用假设" }).click();
  for (const id of captured.exclude_ids) {
    const program = captured.brief.items.find((r: { program_id: string }) => r.program_id === id);
    await main(page).getByLabel(program.program.program_name, { exact: true }).uncheck();
  }
  await main(page).getByRole("button", { name: "自定义", exact: true }).click();
  await main(page).getByLabel("对比开始日期").fill(captured.selected.range.start);
  await main(page).getByLabel("对比结束日期").fill(captured.selected.range.end);
  await main(page).getByRole("button", { name: "应用对比", exact: true }).click();
  await expect(main(page).getByText(`${captured.selected.range.start} — ${captured.selected.range.end}`, { exact: true })).toBeVisible();
  await main(page).getByRole("button", { name: "查看横向对比图表", exact: true }).click();
  await expect(main(page).getByRole("img", { name: /参考组合与沪深300净值曲线/ })).toBeVisible();
  await page.screenshot({ path: nodePath.join(nodePath.dirname(file), "actual-comparison.png"), fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBe(true);
  await page.screenshot({ path: nodePath.join(nodePath.dirname(file), "actual-mobile.png"), fullPage: true });
  await page.setViewportSize({ width: 1700, height: 1100 });
  await page.goto(`/paper-v2/advisory?program_id=${captured.program_id}&list_version_id=${captured.version_id}`);
  await expect(main(page).getByText(`版本 ${captured.version_id} · 绑定 ${captured.binding_id}`, { exact: true })).toBeVisible();
  await expect(main(page).getByRole("table").first().getByRole("row")).toHaveCount(captured.history_details.IN_SCOPE.items.length + 1);
  await expect(main(page).getByTestId("advisory-recommendation-start").first()).not.toContainText("读取推荐涨跌中");
  const valued = Object.values(captured.returns.items).find((q: any) => q.recommendation?.return_bps != null) as any;
  expect(valued).toBeDefined();
  const expected = `${valued.recommendation.return_bps > 0 ? "+" : ""}${(valued.recommendation.return_bps / 100).toFixed(2)}%`;
  const valuedRow = main(page).getByRole("row").filter({ hasText: valued.symbol }).first();
  await valuedRow.scrollIntoViewIfNeeded();
  await expect(valuedRow).toContainText(expected);
  await page.screenshot({ path: nodePath.join(nodePath.dirname(file), "actual-historical-list.png"), fullPage: true });
  expect(errors).toEqual([]);
});

test("ten programs with five-year series stay bounded and interactive", async ({ page }) => {
  test.skip(!process.env.ADVISORY_SCALE_CAPTURE, "Opt-in synthetic scale artifact required");
  const fs = await import("node:fs/promises");
  const scale = JSON.parse(await fs.readFile(process.env.ADVISORY_SCALE_CAPTURE!, "utf8"));
  expect(scale.kind).toBe("SYNTHETIC_SCALE_NOT_FINANCIAL_EVIDENCE");
  const state: State = { seen: [] }; await setup(page, state);
  await page.addInitScript(() => {
    const events: number[] = [];
    (window as unknown as { scaleEvents: number[] }).scaleEvents = events;
    new PerformanceObserver(list => { for (const e of list.getEntries()) if ("interactionId" in e && Number(e.interactionId) > 0) events.push(e.duration); }).observe({ type: "event", buffered: true, durationThreshold: 16 } as PerformanceObserverInit);
  });
  await page.route("**/api/**", async route => {
    const url = new URL(route.request().url());
    expect(route.request().method()).toBe("GET");
    if (url.pathname.endsWith("/overview")) return json(route, scale.brief);
    if (url.pathname.endsWith("/comparison/series")) return json(route, scale.series);
    if (url.pathname.endsWith("/comparison")) return json(route, url.searchParams.get("period") === "ALL" ? scale.summary : scale.default);
    await route.fallback();
  });
  await page.goto("/paper-v2/advisory");
  await expect(main(page).locator("article")).toHaveCount(10);
  await main(page).getByRole("button", { name: "全部历史", exact: true }).click();
  await main(page).getByRole("button", { name: "应用对比", exact: true }).click();
  await expect(main(page).getByText("2021-11-09 — 2026-11-09", { exact: true })).toBeVisible();
  await main(page).getByRole("button", { name: "查看横向对比图表", exact: true }).click();
  await expect(main(page).getByRole("img", { name: /参考组合与沪深300净值曲线/ })).toBeVisible();
  await main(page).getByRole("button", { name: "展开SYNTHETIC-0指标", exact: true }).click();
  await expect(main(page).getByText("实际过程 · 已结束净收益 · 10000 个", { exact: true })).toBeVisible();
  await main(page).getByRole("button", { name: "收起图表", exact: true }).click();
  await expect(main(page).getByRole("img", { name: /参考组合与沪深300净值曲线/ })).toHaveCount(0);
  await page.evaluate(() => new Promise<void>(resolve => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))));
  const durations = await page.evaluate(() => (window as unknown as { scaleEvents: number[] }).scaleEvents);
  const report = { kind: scale.kind, programs: 10, max_interaction_event_ms: Math.max(16, ...durations), event_count: durations.length, curve_bytes: Buffer.byteLength(JSON.stringify(scale.series)), max_points: Math.max(...scale.series.series.flatMap((s: any) => [s.nav.length, s.benchmark.length])) };
  await fs.writeFile(process.env.ADVISORY_SCALE_CAPTURE!.replace("scale_payload.json", "scale_browser_receipt.json"), JSON.stringify(report, null, 2));
  expect(report.max_interaction_event_ms).toBeLessThanOrEqual(200);
  expect(report.curve_bytes).toBeLessThanOrEqual(300000);
  expect(report.max_points).toBeLessThanOrEqual(800);
});
