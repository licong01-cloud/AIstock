import { expect, test, type Page, type Route } from "@playwright/test";

const BASE_URL = process.env.HMM_EVOLUTION_UI_BASE_URL;

test.skip(!BASE_URL, "Set HMM_EVOLUTION_UI_BASE_URL to an already running safe validation target.");

const candidate = {
  candidate_id: "hmmc_1234567890abcdef12345678",
  manifest_hash: "a".repeat(64),
  display_name: "old_covfix_penalty_f096",
  description: "受控候选",
  source_type: "qe_experiment_coefficients",
  source_ref: { task_id: "qe_task", loop_name: "Loop8", asset_path: "coefficients.json" },
  artifact_manifest: {
    schema_version: "hmm_candidate_manifest_v1",
    artifact_type: "hmm_sector_coefficients",
    source_type: "qe_experiment_coefficients",
    source_ref: { task_id: "qe_task", loop_name: "Loop8", asset_path: "coefficients.json" },
    artifact_uri: "qe://qe_task/Loop8/coefficients.json",
    artifact_sha256: "b".repeat(64),
    size_bytes: 2048,
    detected_format: "hmm_sector_coefficients_legacy_v1",
    coverage: {
      start_date: "2025-01-02",
      end_date: "2025-12-31",
      date_count: 240,
      sector_count_min: 31,
      sector_count_max: 31,
      stock_sector_map_count: 5000,
    },
    coefficient_stats: { min: 0.8, max: 1.2 },
    algorithm_version: "score_times_sector_coefficient_v1",
  },
  algorithm_version: "score_times_sector_coefficient_v1",
  lifecycle_status: "research_only",
  invalid_reason_code: null,
  invalid_context: null,
  created_by: "test",
  row_version: 1,
  created_at: "2026-07-18T00:00:00Z",
  updated_at: "2026-07-18T00:00:00Z",
  retired_at: null,
};

const batch = {
  batch_id: "hmmb_ui_contract",
  status: "completed",
  retry_of_batch_id: null,
  retry_generation: 1,
  candidate_count: 1,
  queued_count: 0,
  running_count: 0,
  succeeded_count: 1,
  failed_count: 0,
  cancelled_count: 0,
  timed_out_count: 0,
  heartbeat_at: "2026-07-18T00:02:00Z",
  created_at: "2026-07-18T00:00:00Z",
  started_at: "2026-07-18T00:00:10Z",
  completed_at: "2026-07-18T00:02:00Z",
  updated_at: "2026-07-18T00:02:00Z",
  reason_code: null,
  error_context: null,
};

async function fulfill(route: Route, data: unknown, status = 200) {
  await route.fulfill({
    status,
    contentType: "application/json",
    body: JSON.stringify(status < 400 ? { status: "ok", data, trace_id: "ui-test" } : data),
  });
}

async function installOverviewApi(page: Page) {
  await page.route("**/api/v1/hmm-evolution/candidates?**", (route) => fulfill(route, [candidate]));
  await page.route("**/api/v1/hmm-evolution/batches?**", (route) => fulfill(route, [batch]));
  await page.route("**/api/v1/hmm-evolution/batches/hmmb_ui_contract", (route) => fulfill(route, {
    ...batch,
    recommendation_version: "hmm_recommendation_v1",
    recommendation_spec: { thresholds: null },
    items: [{
      batch_id: batch.batch_id,
      candidate_id: candidate.candidate_id,
      candidate_display_name: candidate.display_name,
      candidate_source_type: candidate.source_type,
      candidate_lifecycle_status: candidate.lifecycle_status,
      eval_id: "hmme_ui_contract",
      ordinal: 0,
      item_status: "succeeded",
      evaluation_status: "succeeded",
      label_horizon_days: 20,
      as_of_date: "2025-12-31",
      window_start: "2025-01-02",
      window_end: "2025-12-31",
      trading_days_count: 240,
      changed_day_count: 120,
      label_comparable_day_count: 120,
      db_comparable_day_count: 120,
      replacement_count: 1500,
      primary_coverage_ratio: 1,
      net_label_return: 0.0482,
      net_db_10d: 0.031,
      positive_net_label_day_ratio: 0.634,
      evidence_quality: "complete",
      result_validity: "valid",
      result_validity_reason: "content_verified",
      warnings_json: [],
      recommendation_score: 91.8,
      metric_availability_ratio: 1,
      recommendation_rank: 1,
      is_top3: true,
      recommendation_components: { thresholds: null },
      reason_code: null,
      evaluation_reason_code: null,
      evaluation_error_message: null,
      evaluation_started_at: batch.started_at,
      evaluation_completed_at: batch.completed_at,
    }],
  }));
}

test("HMM 演进实验室默认出现在左侧导航且不依赖环境开关", async ({ page }) => {
  await page.goto(`${BASE_URL}/quantevolver`);
  const groupTitle = page.locator(".sidebar-group-title").filter({ hasText: "HMM 研究工作台" });
  await expect(groupTitle).toBeVisible();
  await groupTitle.click();
  await expect(page.getByRole("link", { name: "🧭 演进实验室" })).toHaveAttribute(
    "href",
    "/hmm-evolution",
  );
});

test("演进实验室遵循确认视觉和真实业务字段，不出现死 tab 或 raw JSON", async ({ page }) => {
  await installOverviewApi(page);
  await page.goto(`${BASE_URL}/hmm-evolution`);
  await expect(page.getByRole("heading", { name: "让每个候选的优势和缺口一眼可见" })).toBeVisible();
  await expect(page.getByText("候选排行榜")).toBeVisible();
  await expect(page.getByText("Top-3 研究推荐")).toBeVisible();
  await expect(page.getByText("固定证据区")).toBeVisible();
  await expect(page.getByText("91.8").first()).toBeVisible();
  await expect(page.getByText("4.82%", { exact: false })).toBeVisible();
  await expect(page.locator("aside.sidebar")).toBeVisible();
  await expect(page.getByRole("navigation", { name: "HMM 研究模块导航" }).getByRole("link")).toHaveCount(1);
  await expect(page.getByText("板块风险", { exact: true })).toHaveCount(0);
  await expect(page.getByText("滚动训练", { exact: true })).toHaveCount(0);
  await expect(page.locator("pre")).toHaveCount(0);
  await expect(page.locator('[class*="pv2-"]')).toHaveCount(0);
  await expect(page.locator('[role="dialog"]')).toHaveCount(0);
});

test("逐日结果区分当日无调整与收益证据缺失并保持全局侧栏", async ({ page }) => {
  await page.route("**/api/v1/hmm-evolution/evaluations/hmme_ui_contract", (route) => fulfill(route, {
    eval_id: "hmme_ui_contract",
    candidate_id: candidate.candidate_id,
    candidate_display_name: candidate.display_name,
    candidate_source_type: candidate.source_type,
    candidate_lifecycle_status: candidate.lifecycle_status,
    base_loop_ref: "qe_task/Loop8",
    status: "succeeded",
    run_generation: 1,
    source_manifest: {},
    source_manifest_hash: "c".repeat(64),
    candidate_manifest_hash: candidate.manifest_hash,
    evaluation_spec: {
      schema_version: "hmm_evaluation_spec_v2",
      base_loop_ref: "qe_task/Loop8",
      window_start: "2025-01-02",
      window_end: "2025-12-31",
      as_of: { policy: "explicit", requested_date: "2025-12-31" },
      label_horizon_days: 20,
      universe: { type: "source_loop_stock_pool_st_pit" },
      topk: 46,
      date_coverage_policy: "batch_common_intersection_with_evidence",
      missing_sector_policy: "neutral_with_evidence",
      market_forward_return: { mode: "required", horizon_trading_days: 10 },
      sort_policy: "score_desc_symbol_asc_v1",
      metric_version: "hmm_replacement_metrics_v2",
      recommendation_version: "hmm_recommendation_v1",
    },
    evaluation_spec_hash: "d".repeat(64),
    evaluator_version: "hmm_replacement_evaluator_v2",
    input_hash: "e".repeat(64),
    as_of_date: "2025-12-31",
    window_start: "2025-01-02",
    window_end: "2025-12-31",
    label_horizon_days: 20,
    universe_id: "filtered_pool_fixture:qe_st_pit_fixture",
    universe_hash: "f".repeat(64),
    topk: 46,
    trading_days_count: 243,
    changed_day_count: 1,
    label_comparable_day_count: 1,
    db_comparable_day_count: 0,
    replacement_count: 2,
    primary_coverage_ratio: 1,
    net_label_return: 0.1,
    net_db_10d: null,
    positive_net_label_day_ratio: 1,
    evidence_quality: "degraded",
    result_validity: "valid",
    result_validity_reason: "content_verified",
    warnings_json: [{ code: "hmm_evolution_partial_market_return_coverage", message: "部分调整日缺少完整行情收益证据" }],
    metrics_json: {
      daily_summary: [
        { date: "2025-02-17", replacement_count: 0, daily_net_label: null, daily_net_db_10d: null, calculation_status: "no_adjustment", missing_return_evidence_count: 0 },
        { date: "2025-05-09", replacement_count: 2, daily_net_label: 0.1, daily_net_db_10d: null, calculation_status: "incomplete_evidence", missing_return_evidence_count: 1 },
      ],
      incomplete_return_evidence: [
        { date: "2025-05-09", symbol: "600358.SH", replacement_type: "dropped_by_hmm", evidence_type: "market_return", horizon_trading_days: 10, required_start_date: "2025-05-09", required_label_date: "2025-05-23", reason: "horizon_price_missing" },
      ],
    },
    result_hash: "1".repeat(64),
    error_code: null,
    reason_code: null,
    error_message: null,
    error_context: null,
    heartbeat_at: "2026-07-18T00:02:00Z",
    queued_at: "2026-07-18T00:00:00Z",
    started_at: "2026-07-18T00:00:10Z",
    completed_at: "2026-07-18T00:02:00Z",
    updated_at: "2026-07-18T00:02:00Z",
  }));

  await page.goto(`${BASE_URL}/hmm-evolution/evaluations/hmme_ui_contract`);
  await expect(page.locator("aside.sidebar")).toBeVisible();
  const noAdjustmentRow = page.getByRole("row").filter({ hasText: "2025-02-17" });
  await expect(noAdjustmentRow.getByText("当日无调整").first()).toBeVisible();
  const incompleteRow = page.getByRole("row").filter({ hasText: "2025-05-09" }).first();
  await expect(incompleteRow.getByText("证据缺失 1 项")).toBeVisible();
  const evidenceRow = page.getByRole("row").filter({ hasText: "600358.SH" });
  await expect(evidenceRow.getByText("目标交易日有效收盘价缺失（常见于停牌）")).toBeVisible();
  await expect(page.locator("pre")).toHaveCount(0);
});

test("API 失败显示 reason code、中文说明和重试条件", async ({ page }) => {
  await page.route("**/api/v1/hmm-evolution/candidates?**", (route) => fulfill(route, {
    error_code: "HMM_EVOLUTION_ERROR",
    reason_code: "hmm_evolution_schema_unavailable",
    message: "schema unavailable",
    context: { retry_condition: "部署独立 schema 后重试" },
    trace_id: "trace-schema",
  }, 503));
  await page.route("**/api/v1/hmm-evolution/batches?**", (route) => fulfill(route, []));
  await page.goto(`${BASE_URL}/hmm-evolution`);
  await expect(page.getByText("hmm_evolution_schema_unavailable")).toBeVisible();
  await expect(page.getByText("独立 hmm_evolution schema 当前不可用。")).toBeVisible();
  await expect(page.getByText("部署独立 schema 后重试")).toBeVisible();
  await expect(page.getByText("trace-schema")).toBeVisible();
});

test("QE 资产目录超过 200 项仍可分页搜索并展示脱敏 schema 摘要", async ({ page }) => {
  await installOverviewApi(page);
  const assets = Array.from({ length: 221 }, (_, index) => ({
    relative_path: `reports/asset-${String(index).padStart(3, "0")}.json`,
    size_bytes: 128,
    sha256: "c".repeat(64),
    content_type: "application/json",
    modified_at: "2026-07-18T00:00:00Z",
    source: "qe_workspace_catalog",
    trust_level: "unverified_evidence",
    access_mode: "inspection_only",
    schema_version: "qe_report_v1",
    parser_contract: "json_object_v1",
    catalog_completeness: "complete",
  }));
  await page.route("**/api/v1/hmm-evolution/qe-assets/qe_task/Loop8?require_complete=false", (route) => fulfill(route, {
    schema_version: "hmm_qe_asset_catalog_v1",
    task_id: "qe_task",
    loop_name: "Loop8",
    catalog_completeness: "complete",
    assets,
    warnings: [],
  }));
  await page.route("**/api/v1/hmm-evolution/qe-assets/qe_task/Loop8/stat?**", (route) => fulfill(route, assets[220]));
  await page.route("**/api/v1/hmm-evolution/qe-assets/qe_task/Loop8/content?**", (route) => fulfill(route, {
    content_kind: "bounded_text",
    text: JSON.stringify({ metric: 0.12, sensitive_value: "<redacted>" }),
    schema_kind: "json",
    redaction_count: 1,
  }));

  await page.goto(`${BASE_URL}/hmm-evolution`);
  await page.getByPlaceholder("QE task id").fill("qe_task");
  await page.getByPlaceholder("Loop8").fill("Loop8");
  await page.getByRole("button", { name: "读取完整目录" }).click();
  await expect(page.getByText("221", { exact: true }).first()).toBeVisible();
  await expect(page.getByText("第 1 / 5 页", { exact: false })).toBeVisible();
  await page.getByRole("button", { name: "下一页" }).click();
  await expect(page.getByText("reports/asset-050.json", { exact: true })).toBeVisible();
  await page.getByLabel("搜索 QE 资产").fill("asset-220");
  const row = page.getByRole("row").filter({ hasText: "reports/asset-220.json" });
  await row.getByRole("button", { name: "检查" }).click();
  await expect(page.getByText("JSON 结构摘要")).toBeVisible();
  await expect(page.getByText("脱敏 1 项", { exact: false })).toBeVisible();
  await expect(page.locator("pre")).toHaveCount(0);
});
