import { expect, test } from "@playwright/test";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";


test("registered QE run is visible before dispatch and logs require an explicit action", async ({ page }) => {
  const experimentId = "qe_registered_ui_1";
  let statusReads = 0;
  let logReads = 0;
  let terminal = false;

  await page.route("**/api/v1/qe-archive/source-status", async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ status: "success", data: { experiments: {}, tasks: {}, loops: {} } }),
    });
  });
  await page.route(new RegExp(`/api/v1/quantevolver/experiments/${experimentId}/run-status$`), async route => {
    statusReads += 1;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ status: terminal ? "completed" : "running" }),
    });
  });
  await page.route(new RegExp(`/api/v1/quantevolver/experiments/${experimentId}/logs(?:/tail.*)?$`), async route => {
    logReads += 1;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "success",
        data: {
          experiment_id: experimentId,
          experiment_status: "completed",
          terminal: true,
          logs: ["registered terminal log"],
        },
      }),
    });
  });
  await page.route("**/api/v1/quantevolver/experiments?**", async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        total: 1,
        items: [{
          experiment_id: experimentId,
          experiment_name: "registered before dispatch",
          status: "running",
          factor_count: 2,
          model_id: "LSTM",
          strategy_id: "TWAP",
          registration_summary: {
            source_type: "mcp",
            purpose: "research",
            node_id: "rdagent-node1",
            dataset_release_id: "qe-20260831",
            dataset_cutoff: "2026-08-31",
          },
          progress_summary: {
            kind: "evolution",
            current: 1,
            total: 4,
            counts: { running: 1, pending: 3 },
          },
          created_at: "2026-09-07T09:00:00+08:00",
          updated_at: "2026-09-07T09:01:00+08:00",
        }],
      }),
    });
  });

  await page.goto("/quantevolver/experiments");
  await expect(page.getByText(experimentId).first()).toBeVisible({ timeout: 120_000 });
  await expect(page.getByText(/mcp\s*\/\s*research/).first()).toBeVisible();
  await expect(page.getByText(/qe-20260831/).first()).toBeVisible();
  await expect(page.getByText(/running:1/).first()).toBeVisible();
  await expect.poll(() => statusReads).toBeGreaterThan(0);
  expect(logReads).toBe(0);

  await page.getByRole("button", { name: "查看日志" }).click();
  await expect(page.getByText("registered terminal log").first()).toBeVisible();
  expect(logReads).toBe(1);
  terminal = true;

  await page.getByText("自动刷新").click();
  const intervalValues = await page.locator('[data-testid="qe-refresh-interval"] option').evaluateAll(options =>
    options.map(option => Number((option as HTMLOptionElement).value)).filter(Number.isFinite),
  );
  expect(intervalValues).toEqual(expect.arrayContaining([30, 60, 120]));
  expect(intervalValues.every(value => value >= 30)).toBe(true);
});


test("hidden-page guards suppress QE status and log traffic", () => {
  const hook = readFileSync(
    resolve(process.cwd(), "src/app/quantevolver/components/useExperimentSSE.ts"),
    "utf8",
  );
  const pageSource = readFileSync(
    resolve(process.cwd(), "src/app/quantevolver/experiments/page.tsx"),
    "utf8",
  );

  expect(hook).toContain('document.visibilityState !== "visible"');
  expect(hook).toContain("explicitLogRequestRef.current");
  expect(hook).toContain("ACTIVE_STATUS_POLL_INTERVAL = 30000");
  expect(hook).toContain("DISCONNECTED_STATUS_POLL_INTERVAL = 30000");
  expect(pageSource).toContain('if (document.visibilityState !== "visible") return;');
  expect(pageSource).toContain("Math.max(30, refreshInterval)");
});
