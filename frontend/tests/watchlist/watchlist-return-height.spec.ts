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
    /前复权加入价: 5\.000；复权因子: 0\.250000/,
  );
  await expect(page.getByText("100.00%")).toBeVisible();
});
