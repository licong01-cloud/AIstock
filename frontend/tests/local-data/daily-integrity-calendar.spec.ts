import { expect, test } from "@playwright/test";

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
  await page.route("**/api/v1/local-data/integrity/days/2026-10-06", route => route.fulfill({ json: {
    ...calendar.days[5], release_contains_revision: false, release_coverage_state: "OUTSIDE_CUTOFF", partitions: [
      { dataset: "daily_basic", state: "VALID", data_revision: 2, validated_revision: 2,
        due_at: "2026-10-06T23:59:00+08:00", next_retry_at: null,
        metadata: { validation: { counts: [{ unit: "security_date", expected: null,
          valid: 3, missing: null, invalid: 0, legal_na: null, unverified: null }] } } },
    ],
  } }));
  await page.route("**/api/v1/local-data/integrity/days/*/gaps?*", route => route.fulfill({ json: {
    dataset: "daily_basic", groups: [{ classification: "MISSING", symbol: "000001.SZ" }], group_count: 101,
    next_cursor: new URL(route.request().url()).searchParams.has("cursor") ? null : "revision-bound-cursor",
  } }));
  await page.goto("/local-data");
  const card = page.getByTestId("daily-integrity-calendar");
  await expect(card.getByText("交易日 5 · 补齐 1")).toBeVisible();
  await expect(card.getByRole("button", { name: "2026-10-06 完整 补齐" })).toBeVisible();
  await card.getByRole("button", { name: "2026-10-06 完整 补齐" }).focus();
  await page.keyboard.press("Enter");
  await expect(card.getByText("security_date：应有 未知", { exact: false })).toBeVisible();
  await expect(card.getByText("已包含于活动发布：未覆盖", { exact: false })).toBeVisible();
  await card.getByRole("button", { name: "查看缺口" }).click();
  await expect(card.getByRole("button", { name: "下一页缺口" })).toBeVisible();
  await card.getByRole("button", { name: "下一页缺口" }).click();
  await expect(card.getByRole("button", { name: "下一页缺口" })).toHaveCount(0);
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
