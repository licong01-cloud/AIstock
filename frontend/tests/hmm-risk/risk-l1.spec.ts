import { expect, test } from "@playwright/test";

test("renders the real model-bound 31-sector Risk_L1 surface without mocks", async ({ page }) => {
  await page.goto("/hmm-risk");

  const risk = page.getByRole("region", { name: "申万一级板块风险预警" });
  await expect(risk).toBeVisible();
  await expect(risk.getByRole("heading", { name: "L1 风险预警" })).toBeVisible();
  await expect(risk.getByText("风险分数仅用于研究排序，不是校准概率", { exact: false })).toBeVisible();
  await expect(risk.getByRole("region", { name: "31 个申万一级板块风险热力图" })).toBeVisible();
  await expect(risk.locator("article").filter({ has: page.locator("code") })).toHaveCount(31);
  await expect(risk.getByText("未完成前瞻确认", { exact: false })).toBeVisible();
});
