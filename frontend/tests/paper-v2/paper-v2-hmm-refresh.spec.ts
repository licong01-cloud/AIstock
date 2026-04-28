import { expect, test } from "@playwright/test";

test("Selection Center HMM snapshot loading does not poll in a render loop", async ({ page }) => {
  const snapshotUrls: string[] = [];
  page.on("request", (request) => {
    const url = request.url();
    if (/\/api\/v1\/hmm-training\/configs\/[^/]+\/snapshots/.test(url)) {
      snapshotUrls.push(url);
    }
  });

  await page.goto("/paper-v2/selection");
  await expect(page.getByTestId("selection-hmm-config")).toBeVisible({ timeout: 60_000 });
  await page.getByTestId("selection-hmm-enabled").check();
  await expect(page.getByTestId("selection-hmm-snapshot")).toBeVisible({ timeout: 60_000 });

  await page.waitForTimeout(2_000);
  const countAfterInitialLoad = snapshotUrls.length;
  await page.waitForTimeout(5_000);
  const extraRequests = snapshotUrls.length - countAfterInitialLoad;

  expect(snapshotUrls.length).toBeLessThanOrEqual(3);
  expect(extraRequests).toBeLessThanOrEqual(1);
});
