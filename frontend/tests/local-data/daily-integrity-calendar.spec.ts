import { expect, test } from "@playwright/test";
import { datasetLabel, datasetName } from "../../src/app/local-data/datasetLabels";

test("calendar dataset names do not expose internal identifiers", () => {
  expect(datasetName("daily_basic")).toBe("股票每日指标");
  expect(datasetName("bak_basic")).toBe("备用基础信息");
  expect(datasetName("_stk_alert_morning")).toBe("个股重点提示·晨间补采");
  expect(datasetName("unknown_source")).toBe("未知数据集");
  expect(datasetName(undefined)).toBe("未指定数据集");
});

test("dataset labels include Chinese names without replacing technical keys", () => {
  for (const [key, name] of Object.entries({
    daily_basic: "股票每日指标", stk_shock: "个股异常波动",
    stk_high_shock: "个股严重异常波动", stk_alert: "个股重点提示",
    idx_anns: "指数公司公告", stk_seasoned: "增发进程",
    stock_daily_afterhours: "股票盘后成交量额", vix_index: "VIX美国日频指数",
    _stk_alert_morning: "个股重点提示·晨间补采",
    _vix_index_midday: "VIX美国日频指数·午间补采",
  })) expect(datasetLabel(key)).toBe(`${name}（${key}）`);
  expect(datasetLabel("not_registered")).toBe("未知数据集（not_registered）");
  expect(datasetLabel("vendor_key", "供应商数据")).toBe("供应商数据（vendor_key）");
  expect(datasetLabel(undefined)).toBe("未指定数据集");
});

const states = ["NOT_TRADING", "UNVERIFIED", "IN_PROGRESS", "PARTIAL", "ERROR", "COMPLETE"];
const calendar = {
  month: "2026-10", as_of: "2026-10-27T20:00:00+08:00",
  trading_day_count: 5, recovered_day_count: 1,
  counts: Object.fromEntries(states.map(state => [state, 1])),
  days: states.map((state, i) => ({ fact_date: `2026-10-${String(i + 1).padStart(2, "0")}`, state,
    watermark: String(i), revision: i, all_sync_complete: state === "COMPLETE",
    completion_kind: state === "COMPLETE" ? "RECOVERED" : null })),
};

test("calendar exposes real states, null counts, release boundary and revision paging", async ({ page }) => {
  await page.clock.install({ time: new Date("2026-10-27T12:00:00Z") });
  await page.route("**/api/v1/local-data/integrity/calendar?*", route => route.fulfill({ json: calendar }));
  await page.route("**/api/v1/local-data/integrity/days/2026-10-04", route => route.fulfill({ json: {
    ...calendar.days[3], release_contains_revision: false, release_coverage_state: "OUTSIDE_CUTOFF", partitions: [
      { dataset: "daily_basic", state: "VALID", data_revision: 2, validated_revision: 2,
        due_at: "2026-10-06T23:59:00+08:00", next_retry_at: null,
        metadata: { validation: { counts: [{ unit: "security_date", expected: null,
          valid: 3, missing: null, invalid: 0, legal_na: null, unverified: null }] } } },
      { dataset: "bak_basic", state: "INCOMPLETE", data_revision: 1, validated_revision: 1,
        due_at: null, metadata: { validation: { counts: [{ unit: "security_date", expected: 4,
          valid: 3, missing: 1, invalid: 0, legal_na: 0, unverified: 0 }] } } },
    ],
  } }));
  await page.route("**/api/v1/local-data/integrity/days/*/gaps?*", route => route.fulfill({ json: {
    dataset: "bak_basic", groups: [{ classification: "MISSING", symbol: "000001.SZ" }], group_count: 101,
    next_cursor: new URL(route.request().url()).searchParams.has("cursor") ? null : "revision-bound-cursor",
  } }));
  await page.goto("/local-data");
  const card = page.getByTestId("daily-integrity-calendar");
  await expect(card.getByText("交易日 5 · 补齐 1")).toBeVisible();
  await expect(card.getByRole("button", { name: "2026-10-04 部分完成" })).toBeVisible();
  await card.getByRole("button", { name: "2026-10-04 部分完成" }).focus();
  await page.keyboard.press("Enter");
  await expect(card.getByText("股票每日指标", { exact: true })).toBeVisible();
  const normal = card.locator('tr[data-dataset="daily_basic"]');
  const missing = card.locator('tr[data-dataset="bak_basic"]');
  await expect(normal.getByRole("button", { name: "查看缺口" })).toHaveCount(0);
  await expect(missing).toHaveAttribute("data-gap-status", "known-gap");
  await expect(card.getByRole("button", { name: "查看缺口" })).toHaveCount(1);
  await expect(card.getByTestId("partition-summary")).toContainText("备用基础信息");
  await expect(card.getByText("daily_basic", { exact: true })).toHaveCount(0);
  await expect(card.getByText("security_date：应有 未知", { exact: false })).toBeVisible();
  await expect(card.getByText("已包含于活动发布：未覆盖", { exact: false })).toBeVisible();
  await card.getByRole("button", { name: "查看缺口" }).click();
  await expect(card.getByRole("button", { name: "下一页缺口" })).toBeVisible();
  await card.getByRole("button", { name: "下一页缺口" }).click();
  await expect(card.getByRole("button", { name: "下一页缺口" })).toHaveCount(0);
});

test("only current known gaps are highlighted; legal NA, pending, stale and failed checks stay distinct", async ({ page }) => {
  await page.clock.install({ time: new Date("2026-10-27T12:00:00Z") });
  let recovered = false;
  await page.route("**/api/v1/local-data/integrity/calendar?*", route => route.fulfill({ json: {
    ...calendar, days: calendar.days.map(day => day.fact_date === "2026-10-04"
      ? { ...day, watermark: recovered ? "recovered" : "initial" } : day),
  } }));
  const partition = (dataset: string, state: string, missing: number | null = 0, invalid = 0) => ({
    dataset, state, data_revision: 1, validated_revision: 1, due_at: null,
    metadata: { validation: { counts: [{ unit: "security_date", missing, invalid }] } },
  });
  await page.route("**/api/v1/local-data/integrity/days/2026-10-04", route => route.fulfill({ json: {
    ...calendar.days[3], partitions: [
      { ...partition("daily_basic", "VALID"), data_revision: 0, validated_revision: 0,
        metadata: { validation: { counts: [{ unit: "field_cell", missing: 0, invalid: 0, legal_na: 1 }],
          gaps: [{ classification: "LEGAL_NA", symbols: ["001381.SZ"] }] } } },
      partition("bak_basic", recovered ? "VALID" : "INCOMPLETE", recovered ? 0 : 1),
      partition("index_daily", "INCOMPLETE", 0, 1),
      { ...partition("sector_data", "INCOMPLETE", 4), data_revision: 2 },
      partition("moneyflow_ts", "UNVERIFIED", null),
      { dataset: "margin_detail", state: "NOT_DUE", due_at: null },
      partition("cyq_perf", "ERROR", null),
      { ...partition("stk_limit", "INCOMPLETE", null),
        metadata: { validation: { gaps: [{ classification: "MISSING", symbols: ["000001.SZ"] }] } } },
    ],
  } }));
  await page.goto("/local-data");
  const card = page.getByTestId("daily-integrity-calendar");
  await card.getByRole("button", { name: "2026-10-04 部分完成" }).focus();
  await page.keyboard.press("Enter");
  await expect(card.getByRole("button", { name: "查看缺口" })).toHaveCount(3);
  for (const key of ["bak_basic", "index_daily", "stk_limit"])
    await expect(card.locator(`tr[data-dataset="${key}"]`)).toHaveAttribute("data-gap-status", "known-gap");
  for (const key of ["daily_basic", "sector_data", "moneyflow_ts", "margin_detail", "cyq_perf"])
    await expect(card.locator(`tr[data-dataset="${key}"]`).getByRole("button")).toHaveCount(0);
  await expect(card.locator('tr[data-dataset="sector_data"]')).toContainText("待重新核验");
  await expect(card.locator('tr[data-dataset="margin_detail"]')).toContainText("未到期");
  await expect(card.locator('tr[data-dataset="cyq_perf"]')).toContainText("检查异常");
  const badColor = await card.locator('tr[data-dataset="bak_basic"]').evaluate(el => getComputedStyle(el).backgroundColor);
  const normalColor = await card.locator('tr[data-dataset="daily_basic"]').evaluate(el => getComputedStyle(el).backgroundColor);
  expect(badColor).not.toBe(normalColor);
  recovered = true;
  await card.getByRole("button", { name: "刷新状态" }).click();
  await expect(card.locator('tr[data-dataset="bak_basic"]')).toHaveAttribute("data-gap-status", "none-confirmed");
  await expect(card.locator('tr[data-dataset="bak_basic"]').getByRole("button")).toHaveCount(0);
  await expect(card.getByRole("button", { name: "查看缺口" })).toHaveCount(2);
  await expect(card.getByTestId("partition-summary")).not.toContainText("备用基础信息");
});

test("failed refresh keeps last read visibly stale and never manufactures persistent red", async ({ page }) => {
  await page.clock.install({ time: new Date("2026-10-27T12:00:00Z") });
  let failed = false;
  let calls = 0;
  await page.route("**/api/v1/local-data/integrity/calendar?*", route => {
    calls += 1;
    return failed ? route.fulfill({ status: 503, json: { reason_code: "DAILY_INTEGRITY_READ_UNAVAILABLE" } })
      : route.fulfill({ json: calendar });
  });
  await page.goto("/local-data");
  const card = page.getByTestId("daily-integrity-calendar");
  await expect(card.getByRole("button", { name: "2026-10-06 完整 补齐" })).toBeVisible();
  failed = true;
  await card.getByRole("button", { name: "刷新状态" }).click();
  await expect(card.getByRole("alert")).toContainText("仅为上次读回");
  await expect(card.getByRole("button", { name: /检查异常/ })).toHaveCount(1);
  const before = calls;
  await page.evaluate(() => {
    Object.defineProperty(document, "hidden", { configurable: true, value: true });
    document.dispatchEvent(new Event("visibilitychange"));
  });
  await page.clock.runFor(31000);
  expect(calls).toBe(before);
  await page.evaluate(() => {
    Object.defineProperty(document, "hidden", { configurable: true, value: false });
    document.dispatchEvent(new Event("visibilitychange"));
  });
  await expect.poll(() => calls).toBeGreaterThan(before);
});
