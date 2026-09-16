import { expect, test } from "@playwright/test";

const MODEL_HASH = "d".repeat(64);
const INPUT_HASH = "e".repeat(64);
const MAPPING_HASH = "f".repeat(64);

function rotationRows() {
  return Array.from({ length: 31 }, (_, index) => ({
    prediction_id: `10000000-0000-5000-8000-${String(index).padStart(12, "0")}`,
    trade_date: "2026-08-31",
    as_of_date: "2026-08-28",
    sector_code: `801${String(index).padStart(3, "0")}.SI`,
    sector_name: `Sector ${index + 1}`,
    rotation_score: (index - 15) / 30,
    forecast_state: index < 7 ? "fading" : index >= 24 ? "trending" : "neutral",
    availability: "available",
    reason_code: null,
    model_hash: MODEL_HASH,
    input_hash: INPUT_HASH,
    mapping_snapshot_hash: MAPPING_HASH,
    revision: 1,
  }));
}

async function assertRotationSurface(page: import("@playwright/test").Page) {
  await page.goto("/");

  await page.locator(".sidebar-group-title").filter({ hasText: "HMM" }).click();
  const rotationLink = page.locator('.sidebar-link[href="/hmm-risk"]');
  await expect(rotationLink).toBeVisible();
  await expect(rotationLink).toHaveAttribute("href", "/hmm-risk");
  await rotationLink.click();
  await expect(page).toHaveURL(/\/hmm-risk$/);
  await expect(page.getByRole("heading", { name: "L1 板块轮动预测" })).toBeVisible();
  await expect(page.getByText("研究展示与正式 advisory 状态严格分离", { exact: false })).toBeVisible();
  await expect(page.getByRole("region", { name: "31 个申万一级板块轮动热力图" })).toBeVisible();
  await expect(page.locator("article").filter({ has: page.locator("code") })).toHaveCount(31);
  await expect(page.getByText("rotation score 不是概率或置信度", { exact: false })).toBeVisible();
}

test("renders the 31-sector rotation UI contract with deterministic CI responses", async ({ page }) => {
  await page.route("**/api/v1/hmm-risk/overview", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        status: "ok",
        data: {
          model_hash: MODEL_HASH,
          trade_date: "2026-08-31",
          as_of_date: "2026-08-28",
          sector_count: 31,
          available_count: 31,
          binding_mbe_rank_ic: 0.02,
          research_surface_status: "AVAILABLE_EXPERIMENTAL",
          rotation_l1_capability_status: "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED",
          forward_power_status: "INSUFFICIENT",
          forward_confirmation: "PENDING_INSUFFICIENT_POWER",
          advisory_status: "NOT_AVAILABLE",
          validation_basis: "single_date_frozen_model",
          development_oof_rank_ic: 0.039580909571655214,
          development_oof_rank_ic_hac_lower: 0.0013856051597341199,
          development_oof_rank_ic_hac_upper: 0.07777621398357631,
          input_hash: INPUT_HASH,
          mapping_snapshot_hash: MAPPING_HASH,
          tail_accessed: false,
        },
      }),
    });
  });
  await page.route("**/api/v1/hmm-risk/rotation-l1?*", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        status: "ok",
        data: { model_hash: MODEL_HASH, trade_date: "2026-08-31", rows: rotationRows() },
      }),
    });
  });

  await assertRotationSurface(page);
});

test("renders the real model-bound 31-sector rotation surface without mocks", async ({ page }) => {
  test.skip(process.env.HMM_RISK_LIVE !== "1", "requires explicit DEV DDL/DML and live backend authorization");

  await assertRotationSurface(page);
});
