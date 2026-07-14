import { expect, test } from "@playwright/test";

test("data alert can be dismissed without acknowledging or leaving the current page", async ({ page }) => {
  let acknowledgeRequests = 0;

  await page.route("**/api/**", async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: "{}" });
  });
  await page.route("**/api/ingestion/alerts/active?limit=10", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        alerts: [
          {
            alert_id: "alert-suspend-d",
            created_at: "2026-07-14T23:55:03+08:00",
            severity: "error",
            dataset: "suspend_d",
            alert_type: "retry_exhausted",
            title: "suspend_d 重试耗尽仍失败",
            message: "数据同步需要人工检查",
            details: { failure_category: "provider_or_persistence_error" },
            acknowledged: false,
          },
        ],
      }),
    });
  });
  await page.route("**/api/ingestion/alerts/*/acknowledge", async (route) => {
    acknowledgeRequests += 1;
    await route.fulfill({ status: 200, contentType: "application/json", body: "{}" });
  });

  await page.goto("/local-data?tab=schedules");

  const alert = page.getByRole("alert").filter({ hasText: "suspend_d" });
  await expect(alert).toBeVisible();
  const currentUrl = page.url();

  await alert.getByRole("button", { name: "关闭 suspend_d 提醒" }).click();

  await expect(alert).toBeHidden();
  await expect(page).toHaveURL(currentUrl);
  expect(acknowledgeRequests).toBe(0);
});
