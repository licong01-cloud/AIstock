import { expect, test } from "@playwright/test";

const categories = [{ id: 1, name: "默认", description: null }];
const items = Array.from({ length: 50 }, (_, idx) => ({
  id: idx + 1,
  code: `${String(idx + 1).padStart(6, "0")}.SZ`,
  name: `测试股票${idx + 1}`,
  category_names: "默认",
  category_ids: [1],
  entry_price: 20,
  entry_price_adjusted: idx === 0 ? 5 : 20,
  entry_price_basis: idx === 0 ? "qfq_adjusted" : "raw_fallback_missing_adj_factor",
  entry_adjustment_factor: idx === 0 ? 0.25 : null,
  entry_adj_factor_date: "2024-01-02",
  latest_adj_factor_date: "2024-06-03",
  entry_as_of: "2024-01-02",
  last: 10,
  pct_change: 2,
  pct_since_entry: idx === 0 ? 100 : -50,
}));

test("watchlist uses adjusted joined return metadata and expands table height by page size", async ({ page }) => {
  await page.route("**/api/v1/watchlist/categories", async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(categories) });
  });
  await page.route("**/api/v1/watchlist/items?**", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ total: items.length, items }),
    });
  });
  await page.route("**/api/v1/tdx-blocks/available", async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ available: false }) });
  });

  await page.goto("/watchlist");
  await expect(page.getByTestId("watchlist-items-table")).toBeVisible();

  await page.getByTitle("每页条数").selectOption("50");
  await expect(page.getByText("第 1 / 1 页")).toBeVisible();

  const tableWrapper = page.getByTestId("watchlist-items-table").locator("xpath=..");
  await expect(tableWrapper).toHaveCSS("overflow-x", "visible");
  await expect(tableWrapper).toHaveCSS("overflow-y", "visible");

  const minHeight = await tableWrapper.evaluate((node) => Number.parseFloat(getComputedStyle(node).minHeight));
  expect(minHeight).toBeGreaterThanOrEqual(1688);

  await expect(page.getByTestId("watchlist-cell-entry-price-000001.SZ").locator("span")).toHaveAttribute(
    "title",
    /Adjusted entry price: 5\.000; factor: 0\.250000/,
  );
  await expect(page.getByText("100.00%")).toBeVisible();
});

test("watchlist syncs the selected category to TDX by category id", async ({ page }) => {
  const syncCalls: unknown[] = [];
  await page.route("**/api/v1/watchlist/categories", async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(categories) });
  });
  await page.route("**/api/v1/watchlist/items?**", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ total: items.length, items }),
    });
  });
  await page.route("**/api/v1/tdx-blocks/available", async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ available: true }) });
  });
  await page.route("**/api/v1/tdx-blocks/sync-from-category-id", async (route) => {
    syncCalls.push(route.request().postDataJSON());
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        name: "AIstock_1",
        display_name: "默认",
        count: 2,
        codes: ["000001.SZ", "000002.SZ"],
      }),
    });
  });
  page.on("dialog", async (dialog) => {
    expect(dialog.message()).toContain("AIstock_1");
    await dialog.accept();
  });

  await page.goto("/watchlist");
  await expect(page.getByTestId("watchlist-items-table")).toBeVisible();
  await expect(page.getByTestId("watchlist-tdx-sync")).toBeDisabled();

  await page.getByTestId("watchlist-category-filter").selectOption("1");
  await expect(page.getByTestId("watchlist-tdx-sync")).toBeEnabled();
  await page.getByTestId("watchlist-tdx-sync").click();

  await expect.poll(() => syncCalls.length).toBe(1);
  expect(syncCalls[0]).toEqual({ category_id: 1 });
  await expect(page.getByTestId("watchlist-tdx-sync-result")).toContainText("AIstock_1");
});
