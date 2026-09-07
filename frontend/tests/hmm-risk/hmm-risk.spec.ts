import { expect, test } from "@playwright/test";

test("renders the real model-bound 31-sector rotation surface", async ({ page }) => {
  await page.goto("/hmm-risk");

  await expect(page.getByRole("heading", { name: "L1 板块轮动预测" })).toBeVisible();
  await expect(page.getByRole("link", { name: "🧩 L1 板块轮动" })).toHaveAttribute("aria-current", "page");
  await expect(page.getByText("研究展示与正式 advisory 状态严格分离", { exact: false })).toBeVisible();
  await expect(page.getByRole("region", { name: "31 个申万一级板块轮动热力图" })).toBeVisible();
  await expect(page.locator("article").filter({ has: page.locator("code") })).toHaveCount(31);
  await expect(page.getByText("rotation score 不是概率或置信度", { exact: false })).toBeVisible();
});
