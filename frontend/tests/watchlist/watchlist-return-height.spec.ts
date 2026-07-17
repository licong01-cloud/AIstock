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

test("watchlist uses the selected category baseline and sorts refreshed rows", async ({ page }) => {
  const categoryOptions = [
    { id: 1, name: "分类一", description: null },
    { id: 2, name: "分类二", description: null },
  ];
  const makeItem = (
    id: number,
    code: string,
    categoryId: number,
    categoryAddedAt: string,
    categoryEntryPrice: number,
    last: number,
  ) => ({
    id,
    code,
    name: `股票${id}`,
    category_names: categoryId === 1 ? "分类一" : "分类二",
    category_ids: [categoryId],
    created_at: "2023-01-01T09:30:00+08:00",
    entry_price: 5,
    entry_as_of: "2023-01-01",
    category_added_at: categoryAddedAt,
    category_entry_date: categoryAddedAt.slice(0, 10),
    category_entry_price: categoryEntryPrice,
    effective_entry_price_source: "category",
    last,
    pct_change: last,
    pct_since_entry: ((last - categoryEntryPrice) / categoryEntryPrice) * 100,
    open: last - 1,
    prev_close: last - 2,
    high: last + 1,
    low: last - 2,
    volume_hand: last * 100,
    amount: last * 1000,
  });
  const categoryOneItems = [
    makeItem(1, "000001.SZ", 1, "2024-03-01T09:30:00+08:00", 10, 30),
    makeItem(2, "000002.SZ", 1, "2024-01-01T09:30:00+08:00", 10, 10),
    makeItem(3, "000003.SZ", 1, "2024-02-01T09:30:00+08:00", 10, 20),
  ];
  const categoryTwoItems = [
    makeItem(1, "000001.SZ", 2, "2024-04-01T09:30:00+08:00", 40, 50),
  ];
  const allCategoryItems = [
    {
      ...categoryOneItems[0],
      category_names: "分类一,分类二",
      category_ids: [1, 2],
      category_added_at: null,
      category_entry_date: null,
      category_entry_price: null,
      pct_since_entry: null,
      effective_entry_price_source: "not_applicable_all_categories",
    },
  ];

  await page.route("**/api/v1/watchlist/categories", async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(categoryOptions) });
  });
  await page.route("**/api/v1/watchlist/items?**", async (route) => {
    const url = new URL(route.request().url());
    const categoryId = url.searchParams.get("category_id");
    const responseItems = categoryId === "1"
      ? categoryOneItems
      : categoryId === "2"
        ? categoryTwoItems
        : allCategoryItems;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ total: responseItems.length, items: responseItems }),
    });
  });
  await page.route("**/api/v1/tdx-blocks/available", async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ available: false }) });
  });

  await page.goto("/watchlist");
  await expect(page.getByTestId("watchlist-cell-pct-since-entry-000001.SZ")).toHaveText("-");

  await page.getByTestId("watchlist-category-filter").selectOption("1");
  await expect(page.getByTestId("watchlist-cell-joined-at-000001.SZ")).toHaveText("2024-03-01 09:30:00");
  await expect(page.getByTestId("watchlist-cell-entry-price-000001.SZ")).toHaveText("10.000");
  await expect(page.getByTestId("watchlist-cell-pct-since-entry-000001.SZ")).toHaveText("200.00%");

  await page.getByTestId("watchlist-category-filter").selectOption("2");
  await expect(page.getByTestId("watchlist-cell-joined-at-000001.SZ")).toHaveText("2024-04-01 09:30:00");
  await expect(page.getByTestId("watchlist-cell-entry-price-000001.SZ")).toHaveText("40.000");
  await expect(page.getByTestId("watchlist-cell-pct-since-entry-000001.SZ")).toHaveText("25.00%");

  await page.getByTestId("watchlist-category-filter").selectOption("1");
  await page.getByRole("button", { name: "刷新价格" }).click();
  const table = page.getByTestId("watchlist-items-table");
  const rowOrder = () => table.locator("tbody tr").evaluateAll((rows) =>
    rows.map((row) => row.getAttribute("data-testid")),
  );

  await table.locator("th").filter({ hasText: "最新价" }).click();
  await expect.poll(rowOrder).toEqual([
    "watchlist-row-000001.SZ",
    "watchlist-row-000003.SZ",
    "watchlist-row-000002.SZ",
  ]);
  await table.locator("th").filter({ hasText: "最新价" }).click();
  await expect.poll(rowOrder).toEqual([
    "watchlist-row-000002.SZ",
    "watchlist-row-000003.SZ",
    "watchlist-row-000001.SZ",
  ]);

  await table.locator("th").filter({ hasText: "加入时间" }).click();
  await expect.poll(rowOrder).toEqual([
    "watchlist-row-000001.SZ",
    "watchlist-row-000003.SZ",
    "watchlist-row-000002.SZ",
  ]);

  for (const header of ["成交量(手)", "成交额"]) {
    await table.locator("th").filter({ hasText: header }).click();
    await expect.poll(rowOrder).toEqual([
      "watchlist-row-000001.SZ",
      "watchlist-row-000003.SZ",
      "watchlist-row-000002.SZ",
    ]);
    await table.locator("th").filter({ hasText: header }).click();
    await expect.poll(rowOrder).toEqual([
      "watchlist-row-000002.SZ",
      "watchlist-row-000003.SZ",
      "watchlist-row-000001.SZ",
    ]);
  }
});
