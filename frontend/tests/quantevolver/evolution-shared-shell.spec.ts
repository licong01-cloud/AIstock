import { expect, test, type Page, type Route } from "@playwright/test";

function success(route: Route, data: unknown) {
  return route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "success", data }) });
}

async function mockTaskList(page: Page) {
  await page.route(/\/api\/v1\/multi-alpha\/combine\/tasks\?/, (route) => success(route, { tasks: [], count: 0, total: 0, limit: 50, offset: 0 }));
}

test("canonical evolution route owns the multi-alpha workspace", async ({ page }) => {
  await mockTaskList(page);
  await page.goto("/quantevolver/evolution?task_type=multi_alpha_combine");

  await expect(page.locator('[data-qe-workspace-shell="canonical"]')).toHaveAttribute("data-qe-task-type", "multi_alpha_combine");
  await expect(page.getByText("组合回测任务列表", { exact: true })).toBeVisible();
});

test("legacy multi-alpha list URL delegates to the canonical route and keeps query", async ({ page }) => {
  await mockTaskList(page);
  await page.goto("/quantevolver/multi-alpha/combine-backtest?status=failed");

  await expect(page).toHaveURL(/\/quantevolver\/evolution\?.*task_type=multi_alpha_combine/);
  await expect(page).toHaveURL(/status=failed/);
  await expect(page.locator('[data-qe-task-type="multi_alpha_combine"]')).toBeVisible();
});
