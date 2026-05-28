import { expect, test } from "@playwright/test";

test("local data page shows unified trading-day status", async ({ page }) => {
  await page.route("**/api/v1/trading-calendar/status", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        as_of_date: "2026-05-29",
        timezone: "Asia/Shanghai",
        is_trading_day: true,
        latest_completed_trading_day: "2026-05-29",
        previous_trading_day: "2026-05-28",
        next_trading_day: "2026-06-01",
        source: "market.trading_calendar:file_cache",
        warnings: [],
        cache: {
          coverage_start: "2026-01-01",
          coverage_end: "2026-12-31",
          calendar_row_count: 365,
          refresh_reason: "calendar_sync",
        },
      }),
    });
  });
  await page.route("**/api/testing/schedule", async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: "{}" });
  });

  await page.goto("/local-data");

  const statusCard = page.getByTestId("local-data-trading-day-status");
  await expect(statusCard).toContainText("统一交易日状态");
  await expect(statusCard).toContainText("2026-05-29");
  await expect(statusCard).toContainText("交易日");
  await expect(statusCard).toContainText("2026-06-01");
  await expect(statusCard).toContainText("calendar_sync");
});
