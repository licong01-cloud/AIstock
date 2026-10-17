import { expect, test } from "@playwright/test";

const RUN = "a".repeat(64);
const dates = ["2024-07-01", "2024-08-01", "2026-03-31", ...Array.from({ length: 421 }, (_, i) => `fixture-${i}`)];

function detail(day: string, warnings = 131, unavailable = false) {
  const rows = Array.from({ length: 131 }, (_, i) => ({
    run_id: RUN, trade_date: day, as_of_date: "2024-06-28", sector_level: "L2",
    sector_code: `801${String(i).padStart(3, "0")}.SI`, sector_name: null, name_authority: "CANONICAL_CODE_ONLY",
    probability: unavailable ? null : i < warnings ? 0.6 : 0.1, warning: unavailable ? null : i < warnings,
    availability: unavailable ? "unavailable" : "available", reason_code: unavailable ? "hmm_risk_c010_observation_unavailable" : null,
    structural_eligible: !unavailable, outcome_status: "OUTCOME_NOT_MATURE", event: null, realized_drawdown: null, realized_return: null,
  }));
  return {
    run_id: RUN, model_hash: "b".repeat(64), input_hash: "c".repeat(64), mapping_hash: "d".repeat(64),
    model_version: "hmm_risk_l2_absolute_drawdown_logistic_v1", trade_date: day, as_of_date: "2024-06-28", dates,
    validation_basis: "HISTORICAL_CAUSAL_FIXED_TRAIN_DEVELOPMENT", effect_status: "DEVELOPMENT_RISK_EFFECT_REACHED_FORWARD_UNCONFIRMED",
    risk_l2_capability_status: "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED", research_surface_status: "NOT_AVAILABLE",
    forward_power_status: "UNAVAILABLE", forward_confirmation: "NOT_STARTED", advisory_status: "NOT_AVAILABLE", tail_accessed: false,
    day_summary: { sector_count: 131, available_count: unavailable ? 0 : 131, unavailable_count: unavailable ? 131 : 0,
      warning_count: unavailable ? 0 : warnings, unknown_warning_count: unavailable ? 131 : 0, outcome_status_counts: { OUTCOME_NOT_MATURE: 131 } },
    compact_summary: { overall: { precision: 0.25, base_rate: 0.13, precision_lift: 0.12, recall: 0.4 }, hac: { status: "HAC_UNAVAILABLE" },
      prediction_coverage: 1, valid_mature_day_share: 1, outcome_status_counts: { OUTCOME_NOT_MATURE: 1310 } }, rows,
  };
}

test("synthetic L2 contract: full warnings, <=30 display, zero alerts and typed errors", async ({ page }) => {
  await page.route("**/api/v1/hmm-risk/risk-l2/overview?*", async (route) => {
    await route.fulfill({ json: { status: "ok", data: detail("2026-03-31") } });
  });
  await page.route("**/api/v1/hmm-risk/risk-l2?*", async (route) => {
    const day = new URL(route.request().url()).searchParams.get("trade_date")!;
    if (day === "2024-07-01") {
      await route.fulfill({ status: 500, json: { detail: { reason_code: "hmm_risk_risk_l2_readback_failed", message: "pinned date corrupted" } } });
    } else await route.fulfill({ json: { status: "ok", data: detail(day, day === "2024-08-01" ? 0 : 131) } });
  });
  await page.goto(`/hmm-risk?view=risk-l2&risk_run_id=${RUN}`);
  const region = page.getByRole("region", { name: "申万二级行业风险研究" });
  await expect(region.getByTestId("warning-summary")).toContainText("全部报警 131；当前显示报警 20；隐藏报警 111");
  await expect(region.locator("article")).toHaveCount(20);
  await expect(region).toContainText("非 OOF/非 untouched");
  await expect(region).toContainText("未完成前瞻确认");
  await page.getByLabel("最高概率数量").fill("30");
  await page.getByLabel("最低概率数量").fill("0");
  await expect(region.locator("article")).toHaveCount(30);
  await page.getByLabel("最高概率数量").fill("31");
  await expect(region.locator("article")).toHaveCount(0);
  await page.getByLabel("最高概率数量").fill("10");
  await page.getByLabel("历史决策日期").selectOption("2024-08-01");
  await expect(region.getByTestId("warning-summary")).toContainText("全部报警 0");
  await expect(region.locator("article")).toHaveCount(10);
  await page.getByLabel("历史决策日期").selectOption("2024-07-01");
  await expect(region.getByRole("alert")).toContainText("pinned date corrupted");
  await expect(region.locator("article")).toHaveCount(0);
});

test("synthetic all-unavailable is not low risk; late old-date response cannot overwrite", async ({ page }) => {
  await page.route("**/api/v1/hmm-risk/risk-l2/overview?*", async (route) => {
    await route.fulfill({ json: { status: "ok", data: detail("2026-03-31") } });
  });
  await page.route("**/api/v1/hmm-risk/risk-l2?*", async (route) => {
    const day = new URL(route.request().url()).searchParams.get("trade_date")!;
    if (day === "2024-07-01") await new Promise((resolve) => setTimeout(resolve, 200));
    await route.fulfill({ json: { status: "ok", data: detail(day, 0, day === "2024-08-01") } });
  });
  await page.goto(`/hmm-risk?view=risk-l2&risk_run_id=${RUN}`);
  await expect(page.getByRole("region", { name: "L2风险概率展示" })).toBeVisible();
  await page.getByLabel("历史决策日期").selectOption("2024-07-01");
  await page.getByLabel("历史决策日期").selectOption("2024-08-01");
  await expect(page.getByText("该日无可用概率，请查看以下原始不可用原因。")).toBeVisible();
  await expect(page.locator("article")).toHaveCount(0);
  await expect(page.getByRole("region", { name: "申万二级行业风险研究" })).toContainText("历史日期：2024-08-01");
});

test("real L2 historical risk surface without mocks requires separately authorized DB/runtime", async ({ page }) => {
  test.skip(process.env.HMM_RISK_L2_LIVE !== "1", "DEV import and live runtime verification are not source-test success");
  const run = process.env.HMM_RISK_L2_RUN_ID;
  expect(run).toMatch(/^[0-9a-f]{64}$/);
  await page.goto(`/hmm-risk?view=risk-l2&risk_run_id=${run}`);
  const region = page.getByRole("region", { name: "申万二级行业风险研究" });
  for (const day of ["2024-07-01", "2024-08-01", "2026-03-31"]) {
    await page.getByLabel("历史决策日期").selectOption(day);
    await expect(region).toContainText(`历史日期：${day}`);
    await expect(region).toContainText("完整目录 131");
    await expect(region.getByRole("alert")).toHaveCount(0);
    if (day === "2024-08-01") await expect(region.getByTestId("warning-summary")).toContainText("全部报警 0");
  }
});
