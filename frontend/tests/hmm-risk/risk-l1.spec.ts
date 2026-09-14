import { expect, test } from "@playwright/test";

const MODEL_HASH = "a".repeat(64);
const INPUT_HASH = "b".repeat(64);
const MAPPING_HASH = "c".repeat(64);

function riskRows() {
  return Array.from({ length: 31 }, (_, index) => ({
    prediction_id: `00000000-0000-5000-8000-${String(index).padStart(12, "0")}`,
    trade_date: "2026-03-31",
    as_of_date: "2026-03-30",
    sector_code: `801${String(index).padStart(3, "0")}.SI`,
    sector_name: `Sector ${index + 1}`,
    risk_score: index / 30,
    risk_percentile: index / 30,
    risk_level: index >= 24 ? "high" : index >= 18 ? "watch" : "normal",
    predicted_warning: index >= 24,
    availability: "available",
    reason_code: null,
    model_hash: MODEL_HASH,
    input_hash: INPUT_HASH,
    mapping_snapshot_hash: MAPPING_HASH,
    revision: 1,
  }));
}

test("renders the 31-sector Risk_L1 UI contract with deterministic CI responses", async ({ page }) => {
  await page.route("**/api/v1/hmm-risk/risk-l1/overview", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        status: "ok",
        data: {
          model_hash: MODEL_HASH,
          trade_date: "2026-03-31",
          as_of_date: "2026-03-30",
          sector_count: 31,
          available_count: 31,
          high_warning_count: 7,
          risk_l1_research_surface_status: "AVAILABLE_EXPERIMENTAL",
          risk_l1_capability_status: "RESEARCH_RISK_WARNING_AVAILABLE_FORWARD_UNCONFIRMED",
          forward_power_status: "UNAVAILABLE",
          forward_confirmation: "NOT_STARTED",
          advisory_status: "NOT_AVAILABLE",
          validation_basis: "development_causal_oof",
          development_precision_lift: 0.12,
          development_recall: 0.3,
          input_hash: INPUT_HASH,
          mapping_snapshot_hash: MAPPING_HASH,
          tail_accessed: false,
        },
      }),
    });
  });
  await page.route("**/api/v1/hmm-risk/risk-l1?*", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        status: "ok",
        data: { model_hash: MODEL_HASH, trade_date: "2026-03-31", rows: riskRows() },
      }),
    });
  });

  await page.goto("/hmm-risk");

  const risk = page.getByRole("region", { name: "申万一级板块风险预警" });
  await expect(risk).toBeVisible();
  await expect(risk.getByRole("heading", { name: "L1 风险预警" })).toBeVisible();
  await expect(risk.getByText("风险分数仅用于研究排序，不是校准概率", { exact: false })).toBeVisible();
  await expect(risk.getByRole("region", { name: "31 个申万一级板块风险热力图" })).toBeVisible();
  await expect(risk.locator("article").filter({ has: page.locator("code") })).toHaveCount(31);
  await expect(risk.getByText("未完成前瞻确认", { exact: false })).toBeVisible();
});

test("renders the real model-bound 31-sector Risk_L1 surface without mocks", async ({ page }) => {
  test.skip(process.env.HMM_RISK_LIVE !== "1", "requires explicit DEV DDL/DML and live backend authorization");

  await page.goto("/hmm-risk");
  const risk = page.getByRole("region", { name: "申万一级板块风险预警" });
  await expect(risk).toBeVisible();
  await expect(risk.getByRole("region", { name: "31 个申万一级板块风险热力图" })).toBeVisible();
  await expect(risk.locator("article").filter({ has: page.locator("code") })).toHaveCount(31);
});
