import { expect, test } from "@playwright/test";

test("Selection Center HMM config loading does not poll in a render loop", async ({ page }) => {
  const configUrls: string[] = [];
  page.on("request", (request) => {
    const url = request.url();
    if (/\/api\/v1\/hmm-training\/configs(?:\?|$)/.test(url)) {
      configUrls.push(url);
    }
  });

  await page.goto("/paper-v2/selection");
  await expect(page.getByTestId("selection-hmm-config")).toBeVisible({ timeout: 60_000 });
  await page.getByTestId("selection-hmm-enabled").check();
  await expect(page.getByTestId("selection-hmm-coverage")).toContainText(/HMM/, { timeout: 60_000 });
  await expect(page.getByTestId("selection-hmm-snapshot")).toHaveCount(0);

  await page.waitForTimeout(2_000);
  const countAfterInitialLoad = configUrls.length;
  await page.waitForTimeout(5_000);
  const extraRequests = configUrls.length - countAfterInitialLoad;

  expect(configUrls.length).toBeLessThanOrEqual(3);
  expect(extraRequests).toBeLessThanOrEqual(1);
});

test("Portfolio creation HMM config loading does not poll in a render loop", async ({ page }) => {
  const configUrls: string[] = [];
  page.on("request", (request) => {
    const url = request.url();
    if (/\/api\/v1\/hmm-training\/configs(?:\?|$)/.test(url)) {
      configUrls.push(url);
    }
  });

  await page.goto("/paper-v2/portfolios");
  await expect(page.getByTestId("portfolio-hmm-config")).toBeVisible({ timeout: 60_000 });
  await page.getByTestId("portfolio-hmm-enabled").check();
  await expect(page.getByTestId("portfolio-hmm-coverage")).toContainText(/HMM/, { timeout: 60_000 });
  await expect(page.getByTestId("portfolio-hmm-snapshot")).toHaveCount(0);

  await page.waitForTimeout(2_000);
  const countAfterInitialLoad = configUrls.length;
  await page.waitForTimeout(5_000);
  const extraRequests = configUrls.length - countAfterInitialLoad;

  expect(configUrls.length).toBeLessThanOrEqual(3);
  expect(extraRequests).toBeLessThanOrEqual(1);
});
