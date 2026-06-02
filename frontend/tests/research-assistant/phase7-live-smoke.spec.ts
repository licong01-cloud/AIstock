import { expect, test } from "@playwright/test";

test.skip(process.env.RA_PHASE7_LIVE_SMOKE !== "1", "Manual read-only smoke only; user must start dev backend/frontend on 8011/3011 or 8012/3012.");

test("600584 是否值得买入 shows evidence or blockers without placeholders", async ({ page }) => {
  const forbiddenRequests: string[] = [];
  page.on("request", (request) => {
    const url = request.url();
    if (url.includes(":8001") || url.includes(":3000") || url.includes(":19080")) forbiddenRequests.push(url);
  });

  await page.goto("/research-assistant");
  await page.locator(".ra-chat-input").fill("600584 是否值得买入");
  await page.locator(".ra-chat-send").click();

  const evidenceOrBlocker = page.locator("[data-testid='ra-evidence-card'], [data-testid='ra-blocker-card']");
  await expect(evidenceOrBlocker.first()).toBeVisible();
  const text = await page.locator("body").innerText();
  expect(text).not.toContain("TODO");
  expect(text).not.toContain("placeholder");
  expect(text).not.toContain("XX");
  expect(text).not.toContain("X%");
  expect(text).not.toContain("约X");
  expect(text).not.toContain("mock");
  expect(forbiddenRequests).toEqual([]);
});
