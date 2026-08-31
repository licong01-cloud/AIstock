import { expect, test, type Route } from "@playwright/test";

test("shared shell is layout-transparent for the established multi-alpha workspace", async ({ page }) => {
  await page.route(/\/api\/v1\/multi-alpha\/combine\/tasks\?/, (route: Route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({ status: "success", data: { tasks: [], count: 0, total: 0, limit: 50, offset: 0 } }),
  }));
  await page.goto("/quantevolver/evolution?task_type=multi_alpha_combine");
  const shell = page.locator('[data-qe-workspace-shell="canonical"]');
  await expect(shell).toHaveCSS("display", "contents");
  await expect(page.getByText("组合回测控制中心", { exact: true })).toBeVisible();
  expect(await page.getByText("组合回测控制中心", { exact: true }).evaluate((node) => ({
    fontFamily: getComputedStyle(node).fontFamily,
    color: getComputedStyle(node).color,
  }))).toMatchObject({ color: "rgb(30, 41, 59)" });
  await expect(page).toHaveScreenshot("multi-alpha-shared-workspace.png", { fullPage: true, animations: "disabled", maxDiffPixels: 100 });
});
