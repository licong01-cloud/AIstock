import { expect, test } from "@playwright/test";

const strategyPackage = {
  package_id: "pkg_governance_nightly",
  package_name: "Nightly Governance Package",
  package_status: "BACKTEST_APPROVED",
  source_type: "qe_experiment",
  source_id: "qe_nightly_001",
  manifest_sha256: "a".repeat(64),
  asset_eligibility: { eligible: true },
  selection_health: { runnable: true },
  paper_health: { runnable: true },
};

test.beforeEach(async ({ page }) => {
  await page.route("**/api/v1/strategy-packages/qe-sources?*", async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ sources: [] }) });
  });
  await page.route("**/api/v1/strategy-packages?*", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ packages: [strategyPackage] }),
    });
  });
  await page.route("**/api/v1/strategy-packages/*/execution-policies", async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ execution_policies: [] }) });
  });
  await page.route("**/api/v1/strategy-packages/*/status-events", async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ events: [] }) });
  });
  await page.route("**/api/v1/strategy-packages/*/model-state", async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ model_state: { status: "READY" } }) });
  });
  await page.route("**/api/v1/strategy-packages/*/delete-dependencies", async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ dependencies: {} }) });
  });
});

test("renders the StrategyPackage governance state and downstream readiness", async ({ page }) => {
  await page.goto("/paper-v2/packages");

  await expect(page.getByRole("heading", { name: "从 QE 创建策略包" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "StrategyPackage 列表" })).toBeVisible();
  await expect(page.getByRole("button", { name: strategyPackage.package_name })).toBeVisible();
  await expect(page.getByText("资产合格", { exact: true }).first()).toBeVisible();
  await expect(page.getByText("生命周期状态", { exact: true })).toBeVisible();
  await expect(page.getByText("可进入选股", { exact: true })).toBeVisible();
  await expect(page.getByText("可创建模拟盘", { exact: true })).toBeVisible();
});
