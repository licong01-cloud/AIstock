import { expect, test, type Page, type Route } from "@playwright/test";

const PROGRAM_ID = "advp_r5_existing";
const BATCH_ID = "ahrb_r5_001";
const RUN_ID = "ahrrun_r5_001";
const OPERATION_ID = "ahrop_r5_refresh_failed";

type MockState = {
  batchStatus?: string;
  runStatus?: string;
  recoverableProgramCount?: number;
};

function json(route: Route, data: unknown, status = 200) {
  return route.fulfill({ status, contentType: "application/json", body: JSON.stringify(data) });
}

function observeBrowserFailures(page: Page) {
  const consoleErrors: string[] = [];
  const failedRequests: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("requestfailed", (request) => {
    const errorText = request.failure()?.errorText || "unknown request failure";
    if (!errorText.includes("ERR_ABORTED") && !errorText.includes("NS_BINDING_ABORTED")) {
      failedRequests.push(`${request.method()} ${request.url()} ${errorText}`);
    }
  });
  return { consoleErrors, failedRequests };
}

async function mockPage(page: Page, state: MockState = {}) {
  const requests: string[] = [];
  const batchStatus = state.batchStatus || "COMPLETED";
  const runStatus = state.runStatus || "PARTIAL";
  const recoverableProgramCount = state.recoverableProgramCount ?? 0;
  await page.route("**/api/ingestion/**", (route) => json(route, { count: 0, alerts: [] }));
  await page.route("**/api/v1/paper-v2/trading-days/defaults**", (route) => json(route, {
    as_of_date: "2026-07-24",
    trading_day_status: { is_trading_day: true },
    lookback_trading_days: 10,
    latest_trading_day: "2026-07-24",
    trading_days: ["2026-07-01", "2026-07-21", "2026-07-24"],
    replay_start_date: "2026-07-01",
    replay_end_date: "2026-07-21",
  }));
  await page.route("**/api/v1/selection-center/selectable-packages**", (route) => json(route, { ok: true, packages: [] }));
  await page.route("**/api/v1/tdx-blocks/**", (route) => json(route, { ok: true, available: false }));
  await page.route("**/api/v1/advisory/**", async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    requests.push(`${request.method()} ${path}`);
    if (path.endsWith("/historical-range-options")) return json(route, { ok: true, data: {
      existing_programs: [{ program_id: PROGRAM_ID, name: "Existing Program R5", version: 3, active_binding_version_id: "advbind_r5_3", package_id: "pkg_single", target_count: 20, review_policy_summary: {} }],
      admitted_packages: [
        { package_id: "pkg_single", name: "Single Alpha", alpha_mode: "single_alpha", component_count: 1, manifest_sha256: "a".repeat(64), package_version: "v1" },
        { package_id: "pkg_native_parent", name: "Native Multi Alpha", alpha_mode: "multi_alpha", component_count: 3, manifest_sha256: "b".repeat(64), package_version: "v7" },
      ],
      outcome_catalog: { catalog_version: "v1", catalog_content_hash: "c".repeat(64), default_horizons: [1, 3, 5, 10, 20], long_trend_horizons: [20, 40, 60, 120, 180], allowed_maturity_statuses: ["COMPLETE", "CENSORED", "TERMINAL"] },
    }});
    if (path.endsWith("/historical-range-batches") && request.method() === "GET") return json(route, { ok: true, data: { batches: [{ batch_id: BATCH_ID, start_trade_date: "2026-07-01", end_trade_date: "2026-07-21", program_count: 2, status: batchStatus, successful_day_count: 28, planned_day_count: 30, recoverable_program_count: recoverableProgramCount, row_version: 12, created_at: "2026-07-22T08:00:00Z" }] }, page: { limit: 50, next_cursor: null, has_more: false } });
    if (path.endsWith("/historical-range-batches") && request.method() === "POST") return json(route, { ok: true, data: { batch: { batch_id: "ahrb_new", status: "PLANNING", row_version: 1 }, operation: { operation_id: "ahrop_catalog", operation_type: "BUILD_SOURCE_CATALOG", status: "QUEUED" }, operation_id: "ahrop_catalog", exact_retry: false, dispatch_state: "SCHEDULED", links: { operation: "/api/v1/advisory/historical-range-operations/ahrop_catalog" } } }, 202);
    if (path.endsWith(`/historical-range-batches/${BATCH_ID}`)) return json(route, { ok: true, data: { batch: { batch_id: BATCH_ID, status: batchStatus, row_version: 12, successful_day_count: 28, terminal_failed_day_count: 1, recoverable_program_count: recoverableProgramCount, planning_recoverable: false, catalog_phase: "VERIFY" } } });
    if (path.endsWith(`/historical-range-batches/${BATCH_ID}/runs`)) return json(route, { ok: true, data: { runs: [{ range_run_id: RUN_ID, research_program_id: PROGRAM_ID, package_id: "pkg_native_parent", package_version: "v7", alpha_mode: "multi_alpha", status: runStatus, completed_day_count: 14, total_day_count: 15 }] }, page: { limit: 50, next_cursor: null, has_more: false } });
    if (path.endsWith(`/historical-range-batches/${BATCH_ID}/operations`)) return json(route, { ok: true, data: { operations: [{ operation_id: OPERATION_ID, operation_type: "REFRESH_OUTCOMES", status: "FAILED", error_json: { reason_code: "SOURCE_GAP", message: "Outcome source is incomplete", context: { missing_trade_date: "2026-07-18" } }, updated_at: "2026-07-24T09:00:00Z" }] }, page: { limit: 50, next_cursor: null, has_more: false } });
    if (path.endsWith(`/historical-range-operations/${OPERATION_ID}`)) return json(route, { ok: true, data: { operation: { operation_id: OPERATION_ID, operation_type: "REFRESH_OUTCOMES", status: "FAILED", error_json: { reason_code: "SOURCE_GAP", message: "Outcome source is incomplete", context: { missing_trade_date: "2026-07-18" } }, updated_at: "2026-07-24T09:00:00Z" } } });
    if (path.endsWith(`/historical-range-runs/${RUN_ID}/days`)) return json(route, { ok: true, data: { days: [
      { day_run_id: "day_1", ordinal: 1, decision_trade_date: "2026-07-01", status: "VALID_NO_CANDIDATE", candidate_count: 0, enter_count: 0, hold_count: 0, exit_count: 0, watch_count: 0 },
      { day_run_id: "day_2", ordinal: 2, decision_trade_date: "2026-07-02", status: "COMPLETE", candidate_count: 20, enter_count: 5, hold_count: 0, exit_count: 0, watch_count: 15 },
    ] }, page: { limit: 50, next_cursor: null, has_more: false } });
    if (path.endsWith(`/historical-range-runs/${RUN_ID}/days/2026-07-02`)) return json(route, { ok: true, data: { day: { day_run_id: "day_2", ordinal: 2, decision_trade_date: "2026-07-02", status: "COMPLETE" }, candidates: [{ candidate_id: "candidate_1", symbol: "600000.SH", selection_raw_rank: 2, selection_effective_rank: 1, membership_status: "INCLUDED", selection_score: "0.82" }] }, page: { limit: 50, next_cursor: null, has_more: false } });
    if (path.endsWith(`/historical-range-runs/${RUN_ID}/lists/2026-07-02`)) return json(route, { ok: true, data: { list: { list_version_id: "list_2", active_count: 5 }, items: [{ list_version_id: "list_2", symbol: "600000.SH", action: "ENTER", rank: 1, previous_rank: null, reason_codes: ["TOP_RANKED"], episode_id: "episode_1", recommendation_state: "ACTIVE", intended_execution_basis: "NEXT_OPEN" }] }, page: { limit: 50, next_cursor: null, has_more: false } });
    if (path.endsWith(`/historical-range-runs/${RUN_ID}/outcomes`)) return json(route, { ok: true, data: { outcomes: [{ outcome_version_id: "out_1", subject_type: "EPISODE", subject_id: "episode_1", projection: "EXECUTABLE", horizon_trade_days: 5, maturity_status: "COMPLETE", label_as_of_trade_date: "2026-07-24", cost_policy_hash: "e".repeat(64), benchmark_hash: "f".repeat(64), outcome_version: 1, outcome_json: { calculation_results: [{ projection: "RETURN_GROSS", projection_value_decimal: "0.0312" }, { projection: "EXECUTABLE_MFE", projection_value_decimal: "0.0471" }, { projection: "EXECUTABLE_MAE", projection_value_decimal: "-0.0124" }] } }] }, page: { limit: 50, next_cursor: null, has_more: false } });
    if (path.endsWith(`/historical-range-runs/${RUN_ID}/summaries`)) return json(route, { ok: true, data: { summaries: [{ summary_id: "sum_1", summary_version: 1, covered_outcome_set_hash: "d".repeat(64), summary_json: { metrics: [{ metric_name: "mean_return", value: "0.0211" }], unavailable_metrics: [{ metric_name: "recall_at_5", status: "DENOMINATOR_UNAVAILABLE" }] } }] }, page: { limit: 50, next_cursor: null, has_more: false } });
    if (path.endsWith("/programs")) return json(route, { ok: true, programs: [] });
    if (path.endsWith("/leaderboard")) return json(route, { ok: true, leaderboard: [] });
    return json(route, { detail: { error_code: "TEST_UNEXPECTED", reason_code: "TEST_UNEXPECTED", message: `${request.method()} ${path}`, retryable: false, context: {}, correlation_id: "test" } }, 404);
  });
  return requests;
}

test("historical range view separates batch and failed operation state without legacy replay", async ({ page }, testInfo) => {
  const browserFailures = observeBrowserFailures(page);
  const requests = await mockPage(page);
  await page.goto(`/paper-v2/advisory?view=historical-range&program_id=${PROGRAM_ID}`);
  await expect(page.getByTestId("historical-range-view")).toBeVisible();
  await expect(page.getByText("Existing Program R5")).toBeVisible();
  await page.getByRole("button", { name: "查看任务" }).click();
  await expect(page.getByTestId("historical-range-detail")).toContainText("COMPLETED");
  await expect(page.getByTestId("historical-range-detail")).toContainText("FAILED");
  await page.getByRole("button", { name: /REFRESH_OUTCOMES/ }).click();
  await expect(page.getByTestId("historical-range-detail")).toContainText("SOURCE_GAP");
  await expect(page.getByTestId("historical-range-detail")).toContainText("missing_trade_date");
  await page.getByRole("button", { name: "查看 Program run" }).click();
  await expect(page.getByText("VALID_NO_CANDIDATE")).toBeVisible();
  await expect(page.getByText(/RETURN_GROSS: 0.0312/)).toBeVisible();
  await expect(page.getByText(/mean_return = 0.0211/)).toBeVisible();
  await page.getByRole("button", { name: "查看 2026-07-02 证据" }).click();
  await expect(page.getByTestId("historical-range-day-detail")).toContainText("600000.SH");
  await expect(page.getByTestId("historical-range-day-detail")).toContainText("ACTIVE");
  await expect(page.getByText("未来成熟后的历史结果")).toBeVisible();
  expect(requests.some((item) => item.includes("/replay"))).toBeFalsy();
  expect(browserFailures.consoleErrors).toEqual([]);
  expect(browserFailures.failedRequests).toEqual([]);
  await page.screenshot({ path: testInfo.outputPath("historical-range-1440x900.png"), fullPage: true });
});

test("historical range view distinguishes finished partial and retryable facts", async ({ page }) => {
  await mockPage(page, { batchStatus: "PARTIAL", runStatus: "RETRYABLE_FAILED", recoverableProgramCount: 0 });
  await page.goto("/paper-v2/advisory?view=historical-range");
  await page.getByRole("button", { name: "查看任务" }).click();
  await expect(page.getByTestId("historical-range-detail")).toContainText("部分结果，当前无可恢复项");
  await expect(page.getByTestId("historical-range-detail")).toContainText("RETRYABLE_FAILED");
  await expect(page.getByTestId("historical-range-detail")).toContainText("不会重复成功事实");
});

test("historical range layout has no horizontal page overflow at required viewports", async ({ page }, testInfo) => {
  const browserFailures = observeBrowserFailures(page);
  await mockPage(page);
  for (const viewport of [{ width: 375, height: 812 }, { width: 768, height: 1024 }, { width: 1440, height: 900 }]) {
    await page.setViewportSize(viewport);
    await page.goto("/paper-v2/advisory?view=historical-range");
    await expect(page.getByTestId("historical-range-view")).toBeVisible();
    const layout = await page.evaluate(() => ({
      overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
      offenders: Array.from(document.querySelectorAll("body *"))
        .map((element) => ({ tag: element.tagName, className: element.className, right: element.getBoundingClientRect().right, width: element.getBoundingClientRect().width }))
        .filter((item) => item.right > document.documentElement.clientWidth + 1)
        .slice(0, 12),
    }));
    expect(layout.overflow, JSON.stringify(layout.offenders)).toBeLessThanOrEqual(1);
    await page.screenshot({ path: testInfo.outputPath(`historical-range-${viewport.width}x${viewport.height}.png`), fullPage: true });
  }
  expect(browserFailures.consoleErrors).toEqual([]);
  expect(browserFailures.failedRequests).toEqual([]);
});

test("current view no longer exposes a legacy replay creation card", async ({ page }) => {
  const requests = await mockPage(page);
  await page.goto("/paper-v2/advisory?view=current");
  await expect(page.getByTestId("advisory-view-switch")).toBeVisible();
  await expect(page.getByText("历史荐股生命周期回放")).toHaveCount(0);
  await expect(page.getByRole("button", { name: "执行回放" })).toHaveCount(0);
  expect(requests.some((item) => item.includes("/replay"))).toBeFalsy();
});

test("missing page in a successful response is a visible contract error", async ({ page }) => {
  await mockPage(page);
  await page.route("**/api/v1/advisory/historical-range-batches", (route) =>
    json(route, { ok: true, data: { batches: [] } }),
  );
  await page.goto("/paper-v2/advisory?view=historical-range");
  await expect(page.getByTestId("historical-range-view")).toContainText("分页合同无效");
});

test("runs operations and summaries load every cursor page", async ({ page }) => {
  await mockPage(page);
  await page.route(`**/api/v1/advisory/historical-range-batches/${BATCH_ID}/runs**`, (route) => {
    const cursor = new URL(route.request().url()).searchParams.get("cursor");
    return json(route, cursor
      ? { ok: true, data: { runs: [{ range_run_id: "run_page_2", research_program_id: "program_page_2", status: "COMPLETED" }] }, page: { limit: 50, next_cursor: null, has_more: false } }
      : { ok: true, data: { runs: [{ range_run_id: RUN_ID, research_program_id: PROGRAM_ID, status: "PARTIAL" }] }, page: { limit: 50, next_cursor: "runs-next", has_more: true } });
  });
  await page.route(`**/api/v1/advisory/historical-range-batches/${BATCH_ID}/operations**`, (route) => {
    const cursor = new URL(route.request().url()).searchParams.get("cursor");
    return json(route, cursor
      ? { ok: true, data: { operations: [{ operation_id: "operation_page_2", operation_type: "BUILD_DATASET_BRIDGE", status: "FAILED" }] }, page: { limit: 50, next_cursor: null, has_more: false } }
      : { ok: true, data: { operations: [{ operation_id: OPERATION_ID, operation_type: "REFRESH_OUTCOMES", status: "FAILED" }] }, page: { limit: 50, next_cursor: "operations-next", has_more: true } });
  });
  await page.route(`**/api/v1/advisory/historical-range-runs/${RUN_ID}/summaries**`, (route) => {
    const cursor = new URL(route.request().url()).searchParams.get("cursor");
    return json(route, cursor
      ? { ok: true, data: { summaries: [{ summary_id: "sum_2", summary_version: 2, covered_outcome_set_hash: "2".repeat(64), summary_json: {} }] }, page: { limit: 50, next_cursor: null, has_more: false } }
      : { ok: true, data: { summaries: [{ summary_id: "sum_1", summary_version: 1, covered_outcome_set_hash: "1".repeat(64), summary_json: {} }] }, page: { limit: 50, next_cursor: "summaries-next", has_more: true } });
  });
  await page.goto("/paper-v2/advisory?view=historical-range");
  await page.getByRole("button", { name: "查看任务" }).click();
  await page.getByRole("button", { name: "加载更多 Program" }).click();
  await expect(page.getByText("program_page_2")).toBeVisible();
  await page.getByRole("button", { name: "加载更多 Operations" }).click();
  await expect(page.getByText("BUILD_DATASET_BRIDGE")).toBeVisible();
  await page.getByRole("button", { name: "查看 Program run" }).first().click();
  await page.getByRole("button", { name: "加载更多 Summary" }).click();
  await expect(page.getByText("Summary v2")).toBeVisible();
});

test("typed Dataset bridge SEALED and VALID_EMPTY receipts are rendered", async ({ page }) => {
  await mockPage(page);
  let receiptRead = 0;
  await page.route(`**/api/v1/advisory/historical-range-operations/${OPERATION_ID}`, (route) => {
    receiptRead += 1;
    return json(route, receiptRead === 1 ? {
      ok: true,
      data: { operation: { operation_id: OPERATION_ID, operation_type: "BUILD_DATASET_BRIDGE", status: "COMPLETED", result_status: "SEALED", snapshot: { snapshot_id: "snapshot_r5_001", status: "SEALED" }, bridge_receipt: { result_status: "SEALED", sealed_snapshot_id: "snapshot_r5_001" } } },
    } : {
      ok: true,
      data: { operation: { operation_id: OPERATION_ID, operation_type: "BUILD_DATASET_BRIDGE", status: "COMPLETED", result_status: "VALID_EMPTY", snapshot: null, bridge_receipt: { result_status: "VALID_EMPTY", reason_codes: ["ADVISORY_HR_DATASET_BRIDGE_VALID_EMPTY"] } } },
    });
  });
  await page.goto("/paper-v2/advisory?view=historical-range");
  await page.getByRole("button", { name: "查看任务" }).click();
  await page.getByRole("button", { name: /REFRESH_OUTCOMES/ }).click();
  await expect(page.getByTestId("historical-range-detail")).toContainText("snapshot_r5_001");
  await expect(page.getByTestId("historical-range-detail")).toContainText("SEALED");
  await page.getByRole("button", { name: /REFRESH_OUTCOMES/ }).click();
  await expect(page.getByTestId("historical-range-detail")).toContainText("VALID_EMPTY");
});
