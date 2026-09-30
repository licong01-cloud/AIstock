import { expect, test } from "@playwright/test";

const RUN_ID = "a".repeat(64);
const MODEL_HASH = "d".repeat(64);
const INPUT_HASH = "e".repeat(64);
const MAPPING_HASH = "f".repeat(64);

function rotationRows(availableCount = 131) {
  return Array.from({ length: 131 }, (_, index) => ({
    prediction_id: `10000000-0000-5000-8000-${String(index).padStart(12, "0")}`,
    run_id: RUN_ID,
    trade_date: "2026-03-31",
    as_of_date: "2026-03-30",
    sector_code: `801${String(index).padStart(3, "0")}.SI`,
    sector_name: `Sector ${index + 1}`,
    rotation_score: index < availableCount ? index / Math.max(1, availableCount - 1) - 0.5 : null,
    forecast_state:
      index >= availableCount ? null : index < Math.ceil(availableCount * 0.2) ? "fading" : index >= Math.floor(availableCount * 0.8) ? "trending" : "neutral",
    availability: index < availableCount ? "available" : "unavailable",
    reason_code: index < availableCount ? null : "hmm_risk_rotation_l2_quote_unavailable",
    structural_eligible: true,
    feature_eligible: index < availableCount,
    outcome_status: index < availableCount ? "available" : "prediction_unavailable",
    revision: 1,
  }));
}

async function mockRotationApi(page: import("@playwright/test").Page, availableCount = 131) {
  await page.route("**/api/v1/hmm-risk/rotation-l2/overview?*", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        status: "ok",
        data: {
          run_id: RUN_ID,
          model_hash: MODEL_HASH,
          trade_date: "2026-03-31",
          as_of_date: "2026-03-30",
          sector_count: 131,
          available_count: availableCount,
          binding_mbe_rank_ic: 0.02,
          research_surface_status: "AVAILABLE_EXPERIMENTAL",
          rotation_l2_capability_status: "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED",
          effect_status: "DEVELOPMENT_EFFECT_QUALIFIED",
          forward_power_status: "UNAVAILABLE",
          forward_confirmation: "NOT_STARTED",
          advisory_status: "NOT_AVAILABLE",
          validation_basis: "HISTORICAL_CAUSAL_REPLAY_ZERO_FIT",
          input_hash: INPUT_HASH,
          mapping_hash: MAPPING_HASH,
          quote_authority_hash: "1".repeat(64),
          tail_accessed: false,
          metrics: {
            overall: { mean_daily_rank_ic: 0.031, coverage_pass_day_share: 0.97 },
            hac: { lower: -0.01, upper: 0.07, mean: 0.031, status: "AVAILABLE" },
          },
        },
      }),
    });
  });
  await page.route("**/api/v1/hmm-risk/rotation-l2?*", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        status: "ok",
        data: { run_id: RUN_ID, trade_date: "2026-03-31", rows: rotationRows(availableCount) },
      }),
    });
  });
}

async function assertRotationSurface(page: import("@playwright/test").Page) {
  await page.goto(`/hmm-risk?run_id=${RUN_ID}`);

  await expect(page.getByRole("heading", { name: "申万二级行业轮动研究预测" })).toBeVisible();
  await expect(page.getByText("不是收益保证、交易信号或 advisory 能力", { exact: false })).toBeVisible();
  const ranking = page.getByRole("region", { name: "申万二级行业轮动排名" });
  await expect(ranking).toBeVisible();
  await expect(ranking.locator("article")).toHaveCount(20);
  await expect(page.getByText("合计 20 / 30", { exact: false })).toBeVisible();
  await expect(page.getByText("forward_confirmation=NOT_STARTED", { exact: false })).toBeVisible();
}

test("renders configurable L2 top and bottom ranks from a complete 131-sector API", async ({ page }) => {
  await mockRotationApi(page);

  await assertRotationSurface(page);
  await expect(page.getByRole("region", { name: "L1 历史风险独立能力" })).toBeVisible();
  await page.getByLabel("前列数量").fill("15");
  await expect(page.getByText("合计 25 / 30", { exact: false })).toBeVisible();
  await page.getByLabel("后列数量").fill("16");
  await expect(page.getByText("合计 25 / 30", { exact: false })).toBeVisible();
  await expect(page.getByText("合计必须在 1 到 30", { exact: false })).toBeVisible();
});

test("never duplicates sectors when fewer rows are available than requested", async ({ page }) => {
  await mockRotationApi(page, 15);
  await page.goto(`/hmm-risk?run_id=${RUN_ID}`);

  const ranking = page.getByRole("region", { name: "申万二级行业轮动排名" });
  await expect(ranking.locator("article")).toHaveCount(15);
  await expect(page.getByText("实际展示 15 项且不重复", { exact: false })).toBeVisible();
  const labels = await ranking.locator("article strong").allTextContents();
  expect(new Set(labels).size).toBe(labels.length);
});

test("does not guess a latest run when product configuration is absent", async ({ page }) => {
  await page.goto("/hmm-risk");
  await expect(page.getByText("NOT_CONFIGURED", { exact: false })).toBeVisible();
});

test("keeps the historical L1 view behind an explicit version entry", async ({ page }) => {
  await page.goto("/hmm-risk?level=l1");
  await expect(page.getByRole("link", { name: "L2 主线" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "L1 板块轮动预测" })).toBeVisible();
});

test("renders the real run-bound L2 surface without mocks", async ({ page }) => {
  test.skip(process.env.HMM_RISK_LIVE !== "1", "requires explicit DEV DDL/DML and live backend authorization");
  await assertRotationSurface(page);
});
