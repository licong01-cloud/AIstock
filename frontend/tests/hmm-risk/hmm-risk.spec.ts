import { expect, test } from "@playwright/test";

const RUN_ID = "a".repeat(64);
const MODEL_HASH = "d".repeat(64);
const INPUT_HASH = "e".repeat(64);
const MAPPING_HASH = "f".repeat(64);

function rotationRows(availableCount = 131, tradeDate = "2026-03-31") {
  return Array.from({ length: 131 }, (_, index) => ({
    prediction_id: `10000000-0000-5000-8000-${String(index).padStart(12, "0")}`,
    run_id: RUN_ID,
    model_hash: MODEL_HASH,
    input_hash: INPUT_HASH,
    mapping_hash: MAPPING_HASH,
    quote_authority_hash: "1".repeat(64),
    trade_date: tradeDate,
    as_of_date: tradeDate === "2026-03-31" ? "2026-03-30" : "2026-03-27",
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

async function mockRotationApi(page: import("@playwright/test").Page, availableCount = 131, hmm = false, mixed = false, supervised = false, price = false) {
  const version = "hmm_risk_l2_postcalibration_effect_v1";
  await page.route("**/api/v1/hmm-risk/rotation-l2/overview?*", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        status: "ok",
        data: {
          run_id: RUN_ID,
          model_hash: MODEL_HASH,
          trade_date: "2026-03-31",
          available_trade_dates: ["2026-03-30", "2026-03-31"],
          as_of_date: "2026-03-30",
          sector_count: 131,
          available_count: availableCount,
          binding_mbe_rank_ic: 0.02,
          research_surface_status: "AVAILABLE_EXPERIMENTAL",
          rotation_l2_capability_status: "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED",
          ...(hmm ? { model_version: version } : supervised ? {
            model_version: price ? "hmm_risk_rotation_l2_moneyflow_price_supervised_v1" : "hmm_risk_rotation_l2_moneyflow_supervised_v1",
            training_end: "2025-03-31", training_outcome_end: "2025-04-15",
            selection_basis: "RETROSPECTIVE_DEVELOPMENT_SELECTED",
          } : {}),
          effect_status: hmm ? "DEVELOPMENT_EFFECT_REACHED_FORWARD_UNCONFIRMED" : "DEVELOPMENT_EFFECT_QUALIFIED",
          forward_power_status: "UNAVAILABLE",
          forward_confirmation: "NOT_STARTED",
          advisory_status: "NOT_AVAILABLE",
          validation_basis: hmm ? "POST_CALIBRATION_RETROSPECTIVE_DEVELOPMENT" : supervised ? "HISTORICAL_CAUSAL_FIXED_TRAIN_DEVELOPMENT" : "HISTORICAL_CAUSAL_REPLAY_ZERO_FIT",
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
    const tradeDate = new URL(route.request().url()).searchParams.get("trade_date")!;
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        status: "ok",
        data: { run_id: RUN_ID, trade_date: tradeDate, rows: rotationRows(availableCount, tradeDate).map((row, index) => hmm ? {
          ...row, model_version: mixed && index === 0 ? undefined : version,
          semantic_state: row.availability === "available" ? "neutral" : null,
          forecast_state: row.availability === "available" ? "neutral" : null,
          daily_rank_group: row.forecast_state,
        } : supervised ? {
          ...row, model_version: mixed && index === 0 ? undefined : price ? "hmm_risk_rotation_l2_moneyflow_price_supervised_v1" : "hmm_risk_rotation_l2_moneyflow_supervised_v1",
          ...(price ? { feature_contributions: row.availability === "available" ? {
            raw_prediction: row.rotation_score, intercept: 0, moneyflow_level_linear_term: 0,
            moneyflow_delta_linear_term: 0, relative_momentum_linear_term: row.rotation_score,
            relative_downside_linear_term: 0, average_rank_score: row.rotation_score,
            daily_rank_group: row.forecast_state, model_parameter_sha256: "2".repeat(64),
          } : null } : {}),
        } : row) },
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
  await expect(page.getByRole("region", { name: "L1 历史风险独立能力" })).toHaveCount(0);
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

test("distinguishes frozen industry semantics from daily relative ranks", async ({ page }) => {
  await mockRotationApi(page, 127, true);
  await assertRotationSurface(page);
  const first = page.getByRole("region", { name: "申万二级行业轮动排名" }).locator("article").first();
  await expect(first).toContainText("行业内状态");
  await expect(first).toContainText("当日相对排名");
  await expect(first).toContainText("中性");
  await expect(first).toContainText("前列");
});

test("rejects mixed HMM and delta product rows", async ({ page }) => {
  await mockRotationApi(page, 127, true, true);
  await page.goto(`/hmm-risk?run_id=${RUN_ID}`);
  await expect(page.getByRole("alert")).toContainText("hmm_risk_rotation_l2_ui_version_invalid");
  await expect(page.getByRole("region", { name: "申万二级行业轮动排名" })).toHaveCount(0);
});

test("shows the supervised fixed-train boundaries without claiming forward confirmation", async ({ page }) => {
  await mockRotationApi(page, 127, false, false, true);
  await assertRotationSurface(page);
  await expect(page.getByText("训练 decision 截止 2025-03-31", { exact: false })).toBeVisible();
  await expect(page.getByText("不是 untouched / 前瞻确认", { exact: false })).toBeVisible();
});

test("rejects mixed supervised and baseline detail rows", async ({ page }) => {
  await mockRotationApi(page, 127, false, true, true);
  await page.goto(`/hmm-risk?run_id=${RUN_ID}`);
  await expect(page.getByRole("alert")).toContainText("hmm_risk_rotation_l2_ui_version_invalid");
  await expect(page.getByRole("region", { name: "申万二级行业轮动排名" })).toHaveCount(0);
});

test("shows four-feature linear explanation separately from rank without claiming forward value", async ({ page }) => {
  await mockRotationApi(page, 127, false, false, true, true);
  await assertRotationSurface(page);
  const first = page.getByRole("region", { name: "申万二级行业轮动排名" }).locator("article").first();
  await expect(first).toContainText("原始预测");
  await expect(first).toContainText("相对动量项");
  await expect(first).toContainText("下行半偏差项");
  await expect(page.getByText("不是 untouched / 前瞻确认", { exact: false })).toBeVisible();
});

test("rejects mixed four-feature and old model detail rows", async ({ page }) => {
  await mockRotationApi(page, 127, false, true, true, true);
  await page.goto(`/hmm-risk?run_id=${RUN_ID}`);
  await expect(page.getByRole("alert")).toContainText("hmm_risk_rotation_l2_ui_version_invalid");
  await expect(page.getByRole("region", { name: "申万二级行业轮动排名" })).toHaveCount(0);
});

test("rejects four-feature missing explanation rather than silently displaying scores", async ({ page }) => {
  await mockRotationApi(page, 127, false, false, true, true);
  await page.route("**/api/v1/hmm-risk/rotation-l2?*", async (route) => {
    await route.fulfill({ contentType: "application/json", body: JSON.stringify({ status: "ok", data: {
      run_id: RUN_ID, trade_date: "2026-03-31", rows: rotationRows(127).map((row) => ({
        ...row, model_version: "hmm_risk_rotation_l2_moneyflow_price_supervised_v1",
      })),
    } }) });
  });
  await page.goto(`/hmm-risk?run_id=${RUN_ID}`);
  await expect(page.getByRole("alert")).toContainText("hmm_risk_rotation_l2_ui_explanation_invalid");
  await expect(page.getByRole("region", { name: "申万二级行业轮动排名" })).toHaveCount(0);
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

test("reads the selected historical date and shows its own coverage and as-of", async ({ page }) => {
  await mockRotationApi(page, 119);
  await assertRotationSurface(page);
  await page.getByLabel("历史预测日", { exact: true }).selectOption("2026-03-30");
  await expect(page.getByText("2026-03-30 / 2026-03-27", { exact: true })).toBeVisible();
  await expect(page.getByText("119 / 131", { exact: true })).toBeVisible();
  await expect(page.getByRole("region", { name: "申万二级行业轮动排名" }).locator("article")).toHaveCount(20);
});

test("a slow previous-date response cannot overwrite the current selection", async ({ page }) => {
  await mockRotationApi(page);
  await assertRotationSurface(page);
  let release!: () => void;
  const oldResponse = new Promise<void>((resolve) => { release = resolve; });
  let started!: () => void;
  const oldStarted = new Promise<void>((resolve) => { started = resolve; });
  await page.route("**/api/v1/hmm-risk/rotation-l2?*", async (route) => {
    const tradeDate = new URL(route.request().url()).searchParams.get("trade_date")!;
    if (tradeDate === "2026-03-30") {
      started();
      await oldResponse;
    }
    await route.fulfill({ contentType: "application/json", body: JSON.stringify({
      status: "ok", data: { run_id: RUN_ID, trade_date: tradeDate, rows: rotationRows(131, tradeDate) },
    }) });
  });
  await page.getByLabel("历史预测日", { exact: true }).selectOption("2026-03-30");
  await oldStarted;
  await page.getByLabel("历史预测日", { exact: true }).selectOption("2026-03-31");
  await expect(page.getByText("2026-03-31 / 2026-03-30", { exact: true })).toBeVisible();
  const lateResponse = page.waitForResponse((response) => response.url().includes("rotation-l2?")
    && new URL(response.url()).searchParams.get("trade_date") === "2026-03-30");
  release();
  await (await lateResponse).finished();
  await page.evaluate(() => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))));
  await expect(page.getByText("2026-03-30 / 2026-03-27", { exact: true })).toHaveCount(0);
  await expect(page.getByText("2026-03-31 / 2026-03-30", { exact: true })).toBeVisible();
});

for (const field of ["run_id", "trade_date", "model_hash", "input_hash", "mapping_hash", "quote_authority_hash"] as const) {
  test(`rejects baseline detail ${field} drift without keeping old cards`, async ({ page }) => {
    await mockRotationApi(page);
    await page.route("**/api/v1/hmm-risk/rotation-l2?*", async (route) => {
      const rows = rotationRows();
      rows[0] = { ...rows[0], [field]: field === "trade_date" ? "2026-03-30" : "9".repeat(64) };
      await route.fulfill({ contentType: "application/json", body: JSON.stringify({
        status: "ok", data: { run_id: RUN_ID, trade_date: "2026-03-31", rows },
      }) });
    });
    await page.goto(`/hmm-risk?run_id=${RUN_ID}`);
    await expect(page.getByText("hmm_risk_rotation_l2_ui_identity_invalid", { exact: true })).toBeVisible();
    await expect(page.getByRole("region", { name: "申万二级行业轮动排名" })).toHaveCount(0);
  });
}
