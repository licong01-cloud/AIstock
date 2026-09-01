import { expect, test } from "@playwright/test";

const apiBase = process.env.QE_API_BASE || process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8011/api/v1";

test("QE UI and API project V25 retirement without fallback", async ({ page, request }) => {
  const catalogResp = await request.get(`${apiBase}/quantevolver/execution-algorithms`);
  expect(catalogResp.ok()).toBeTruthy();
  const catalog = await catalogResp.json();
  const byCode = new Map<string, any>((catalog.items || []).map((item: any) => [item.algo_code, item]));
  expect(byCode.get("V25_TWO_STAGE")?.qe_supported).toBe(false);
  expect(byCode.get("V25_TWO_STAGE")?.retired).toBe(true);
  expect(byCode.get("V25_TWO_STAGE")?.is_enabled).toBe(false);
  expect(byCode.get("V25_TWO_STAGE")?.retirement_reason_code).toBe("V25_EXECUTION_ALGO_RETIRED");
  expect(byCode.get("V25_TWO_STAGE")?.qe_effective_module).toBeNull();
  expect(byCode.get("VWAP")?.qe_supported).toBe(false);

  await page.goto("/quantevolver/compose");
  await page.locator('[data-testid="qe-step-3"]').click();
  await expect(page.locator('option[value="V25_TWO_STAGE"]')).toHaveCount(0);
  await expect(page.locator('option[value="VWAP"]')).toHaveCount(0);
  await page.locator('[data-testid="qe-step-5"]').click();
  await expect(page.locator('[data-testid="qe-filter-suspended"]')).toBeVisible();

  const genResp = await request.post(`${apiBase}/quantevolver/config/generate`, {
    data: {
      factor_names: ["BVPS20DayChange"],
      data_split: {
        train_start: "2020-01-01",
        train_end: "2020-06-30",
        valid_start: "2020-07-01",
        valid_end: "2020-08-31",
        test_start: "2024-04-01",
        test_end: "2024-04-15",
      },
      custom_params: {
        topk: 10,
        n_drop: 2,
        hold_thresh: 5,
        label_horizon: 5,
        quick_train: true,
        execution_algo: "V25_TWO_STAGE",
        execution_algo_params: { device: "cpu" },
        filter_suspended_on_signal: true,
        suspend_filter_strict: false,
      },
    },
  });
  expect(genResp.status()).toBe(422);
  const rejected = await genResp.json();
  expect(rejected.detail?.error_code).toBe("V25_EXECUTION_ALGO_RETIRED");
  expect(rejected.detail?.context?.fallback_used).toBe(false);
});
