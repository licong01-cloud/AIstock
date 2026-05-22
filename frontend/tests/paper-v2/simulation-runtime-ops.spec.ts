import { expect, test, type Page } from "@playwright/test";
import type { JsonObject } from "@/lib/paper-v2/types";

const MOCK_API = process.env.SIMULATION_RUNTIME_UI_MOCK_API !== "0";
test.skip(!MOCK_API, "simulation runtime ops UI spec is mock-first; set SIMULATION_RUNTIME_UI_MOCK_API=1 to run");

const TRADE_DATE = "2026-05-21";
const LOCAL_RUN_ID = "simrun_local_20260521";
const QMT_RUN_ID = "simrun_miniqmt_20260521";
const LOCAL_PLAN_ID = "plan_local_20260521";
const QMT_PLAN_ID = "plan_miniqmt_20260521";

const schedulerPayload = {
  ok: true,
  scheduler: {
    ok: true,
    scheduler: "simulation_lifecycle_scheduler",
    autostart: false,
    default_submit: false,
    read_only_ops_api: true,
    manual_tick_endpoint_enabled: false,
    approval_states: ["SIM_VALIDATING", "SIM_PASSED"],
    restart_recovery_mode: "persisted_state_only",
    schedule_windows: [
      { window_id: "pre_open", label: "pre-open", start: "08:50", end: "09:10", action: "readiness", state: "COMPLETED" },
      { window_id: "selection", label: "selection", start: "09:10", end: "09:20", action: "selection_evidence", state: "COMPLETED" },
      { window_id: "planning", label: "planning", start: "09:20", end: "09:25", action: "execution_plan", state: "ACTIVE" },
      { window_id: "execution", label: "execution", start: "09:25", end: "15:00", action: "submit", state: "UPCOMING" },
    ],
    summary: {
      label: "simulation lifecycle scheduler",
      next_action: "monitor persisted scheduler windows and runs",
      safety_note: "This read-only API does not submit broker orders; execution uses the controlled scheduler path.",
    },
  },
};

const localRun = {
  run_id: LOCAL_RUN_ID,
  trade_date: TRADE_DATE,
  strategy_id: "strategy_local_ops",
  broker_backend: "local_sim",
  package_id: "pkg_ops",
  manifest_sha256: "manifest_local_hash",
  release_id: "srr_local_ops",
  release_hash: "release_local_hash_1234567890",
  binding_id: "simbind_local_ops",
  binding_hash: "binding_local_hash_1234567890",
  selection_evidence_id: "dse_local_ops",
  selection_artifact_hash: "selection_hash_shared_1234567890",
  execution_plan_id: LOCAL_PLAN_ID,
  execution_plan_hash: "plan_hash_local_1234567890",
  status: "PLANNING_EXECUTION",
  last_stage: "PLANNING_EXECUTION",
  stage_counts: { target_count: 2, execution_plan_intent_count: 2, submitted_intents: 0, failed_intents: 0 },
  broker_context: { no_rebalance_required: false, broker_called: false },
  strategy_performance: { nav: 1.0, total_equity: 100000, realized_pnl: 0, unrealized_pnl: 0, market_value: 0, cash: 100000, positions: [] },
  orders: [],
  fills: [],
  errors: [],
  audit: { created_at: "2026-05-21T09:25:00Z", updated_at: "2026-05-21T09:25:01Z" },
};

const qmtRun = {
  run_id: QMT_RUN_ID,
  trade_date: TRADE_DATE,
  strategy_id: "strategy_miniqmt_ops",
  broker_backend: "minqmt_sim",
  package_id: "pkg_ops",
  manifest_sha256: "manifest_qmt_hash",
  release_id: "srr_qmt_ops",
  release_hash: "release_qmt_hash_1234567890",
  binding_id: "simbind_qmt_ops",
  binding_hash: "binding_qmt_hash_1234567890",
  selection_evidence_id: "dse_qmt_ops",
  selection_artifact_hash: "selection_hash_shared_1234567890",
  execution_plan_id: QMT_PLAN_ID,
  execution_plan_hash: "plan_hash_qmt_1234567890",
  status: "SUCCEEDED",
  last_stage: "SUBMITTED",
  stage_counts: { target_count: 2, execution_plan_intent_count: 2, submitted_intents: 2, failed_intents: 0 },
  broker_context: { no_rebalance_required: false, broker_called: true, qmt_batch_status: "SUCCEEDED" },
  strategy_performance: { nav: 1.001, total_equity: 100100, realized_pnl: 0, unrealized_pnl: 100, market_value: 10100, cash: 90000, positions: [{ symbol: "000001.SZ", quantity: 1000 }] },
  orders: [{ source: "miniqmt_managed_order", intent_id: "intent_qmt_buy", qmt_order_id: "900000001", success: true }],
  fills: [{ source: "miniqmt_sync_summary", trades_seen: 1, cash_entries_appended: 1 }],
  errors: [],
  audit: { created_at: "2026-05-21T09:26:00Z", updated_at: "2026-05-21T09:27:00Z" },
};

const plans = {
  [LOCAL_PLAN_ID]: {
    plan_id: LOCAL_PLAN_ID,
    plan_hash: "plan_hash_local_1234567890",
    strategy_id: "strategy_local_ops",
    portfolio_id: "portfolio_local_ops",
    package_id: "pkg_ops",
    release_id: "srr_local_ops",
    release_hash: "release_local_hash_1234567890",
    binding_id: "simbind_local_ops",
    binding_hash: "binding_local_hash_1234567890",
    selection_evidence_id: "dse_local_ops",
    selection_evidence_hash: "selection_hash_shared_1234567890",
    target_trade_date: TRADE_DATE,
    execution_policy_version_id: "exec_policy_v25_1_small_cap",
    execution_policy_sha256: "exec_hash_local",
    tail_policy_version_id: "tail_policy_close_v1",
    tail_policy_sha256: "tail_hash_local",
    intent_count: 2,
    buy_intent_count: 1,
    sell_intent_count: 1,
    trading_rule_decision_count: 2,
    symbols: ["000001.SZ", "000003.SZ"],
    intents: [
      { intent_id: "intent_local_buy", symbol: "000001.SZ", side: "BUY", order_quantity: 1000 },
      { intent_id: "intent_local_sell", symbol: "000003.SZ", side: "SELL", order_quantity: 77 },
    ],
  },
  [QMT_PLAN_ID]: {
    plan_id: QMT_PLAN_ID,
    plan_hash: "plan_hash_qmt_1234567890",
    strategy_id: "strategy_miniqmt_ops",
    portfolio_id: "portfolio_qmt_ops",
    package_id: "pkg_ops",
    release_id: "srr_qmt_ops",
    release_hash: "release_qmt_hash_1234567890",
    binding_id: "simbind_qmt_ops",
    binding_hash: "binding_qmt_hash_1234567890",
    selection_evidence_id: "dse_qmt_ops",
    selection_evidence_hash: "selection_hash_shared_1234567890",
    target_trade_date: TRADE_DATE,
    execution_policy_version_id: "exec_policy_v25_1_small_cap",
    execution_policy_sha256: "exec_hash_qmt",
    tail_policy_version_id: "tail_policy_close_v1",
    tail_policy_sha256: "tail_hash_qmt",
    intent_count: 2,
    buy_intent_count: 1,
    sell_intent_count: 1,
    trading_rule_decision_count: 2,
    symbols: ["000001.SZ", "000003.SZ"],
    intents: [
      { intent_id: "intent_qmt_buy", symbol: "000001.SZ", side: "BUY", order_quantity: 1000 },
      { intent_id: "intent_qmt_sell", symbol: "000003.SZ", side: "SELL", order_quantity: 77 },
    ],
  },
};

type MockRun = JsonObject & { broker_backend: string; status: string; strategy_id: string };

function listPayload(runs: MockRun[]) {
  const byStatus = runs.reduce<Record<string, number>>((acc, row) => {
    acc[row.status] = (acc[row.status] || 0) + 1;
    return acc;
  }, {});
  const byBackend = runs.reduce<Record<string, number>>((acc, row) => {
    acc[row.broker_backend] = (acc[row.broker_backend] || 0) + 1;
    return acc;
  }, {});
  return {
    ok: true,
    summary: {
      run_count: runs.length,
      active_run_count: runs.filter((row) => row.status !== "SUCCEEDED").length,
      terminal_run_count: runs.filter((row) => row.status === "SUCCEEDED").length,
      by_status: byStatus,
      by_broker_backend: byBackend,
    },
    runs,
  };
}

async function mockApi(page: Page) {
  const writeMethods: string[] = [];
  await page.route("**/api/v1/simulation-runtime/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname;
    const method = request.method();
    if (method !== "GET") writeMethods.push(`${method} ${path}`);

    const respond = (data: unknown, status = 200) =>
      route.fulfill({ status, contentType: "application/json", body: JSON.stringify(data) });

    if (path.endsWith("/api/v1/simulation-runtime/scheduler/status")) {
      return respond(schedulerPayload);
    }

    if (path.endsWith("/api/v1/simulation-runtime/runs")) {
      const backend = url.searchParams.get("broker_backend");
      const status = url.searchParams.get("status");
      const strategyId = url.searchParams.get("strategy_id");
      let rows: MockRun[] = [localRun, qmtRun];
      if (backend) rows = rows.filter((row) => row.broker_backend === backend);
      if (status) rows = rows.filter((row) => row.status === status);
      if (strategyId) rows = rows.filter((row) => row.strategy_id === strategyId);
      return respond(listPayload(rows));
    }

    if (path.endsWith(`/api/v1/simulation-runtime/runs/${LOCAL_RUN_ID}`)) {
      return respond({
        ok: true,
        run: localRun,
        selection_evidence: {
          evidence_id: "dse_local_ops",
          artifact_hash: "selection_hash_shared_1234567890",
          target_trade_date: TRADE_DATE,
          package_id: "pkg_ops",
          manifest_sha256: "manifest_local_hash",
          release_id: "srr_local_ops",
          release_hash: "release_local_hash_1234567890",
          runtime_profile_version_id: "runtime_profile_ops_v1",
          runtime_profile_hash: "runtime_hash_ops",
          candidate_count: 2,
          excluded_count: 0,
          source_type: "live_inference",
          data_source: "DB_HISTORICAL",
        },
        execution_plan: plans[LOCAL_PLAN_ID],
      });
    }

    if (path.endsWith(`/api/v1/simulation-runtime/execution-plans/${LOCAL_PLAN_ID}`)) {
      return respond({ ok: true, execution_plan: plans[LOCAL_PLAN_ID] });
    }

    return respond({ detail: `unexpected simulation-runtime route: ${path}` }, 404);
  });
  return writeMethods;
}

test("simulation runtime ops page displays read-only scheduler, shared run trace, and filters", async ({ page }) => {
  const pageErrors: string[] = [];
  const consoleErrors: string[] = [];
  const badResponses: string[] = [];
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("response", (response) => {
    if (response.status() >= 400) badResponses.push(`${response.status()} ${response.url()}`);
  });
  const writeMethods = await mockApi(page);

  await page.goto("/paper-v2/simulation-runtime");

  await expect(page.getByTestId("sim-runtime-total-runs")).toBeVisible();
  await expect(page.getByTestId("sim-runtime-total-runs")).toContainText("2");
  await expect(page.getByTestId("sim-runtime-local-count")).toContainText("1");
  await expect(page.getByTestId("sim-runtime-miniqmt-count")).toContainText("1");
  await expect(page.getByTestId("sim-runtime-submit-default")).toContainText("OFF");
  await expect(page.getByTestId("sim-runtime-scheduler-status")).toContainText("simulation_lifecycle_scheduler");
  await expect(page.getByTestId("sim-runtime-restart-recovery-mode")).toContainText("persisted_state_only");
  await expect(page.getByText("execution_plan")).toBeVisible();
  await expect(page.getByTestId("sim-runtime-scheduler-status")).toContainText("未启用");

  await expect(page.getByText("strategy_local_ops")).toBeVisible();
  await expect(page.getByText("strategy_miniqmt_ops")).toBeVisible();
  await page.getByTestId(`sim-runtime-run-detail-${LOCAL_RUN_ID}`).click();
  await expect(page.getByTestId("sim-runtime-selected-run-id")).toContainText(LOCAL_RUN_ID);
  await expect(page.getByTestId("sim-runtime-selected-evidence-id")).toContainText("dse_local_ops");
  await expect(page.getByTestId("sim-runtime-selected-plan-id")).toContainText(LOCAL_PLAN_ID);
  await expect(page.getByTestId("sim-runtime-selected-intent-counts")).toContainText("BUY 1 / SELL 1 / total 2");
  await expect(page.getByTestId("sim-runtime-selected-nav")).toContainText("1");
  await expect(page.getByTestId("sim-runtime-selected-order-fill-errors")).toContainText("orders");

  await page.getByTestId("sim-runtime-backend-filter").selectOption("minqmt_sim");
  await expect(page.getByText("strategy_miniqmt_ops")).toBeVisible();
  await expect(page.getByText("strategy_local_ops")).toHaveCount(0);

  expect(writeMethods).toEqual([]);
  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
  expect(badResponses).toEqual([]);
});
