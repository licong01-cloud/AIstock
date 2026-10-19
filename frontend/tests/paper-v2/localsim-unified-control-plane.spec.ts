import { expect, test, type Page, type Route } from "@playwright/test";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";


const account = {
  schema_version: "simulation_account_v1",
  account_id: "simacct_ui_contract",
  account_hash: "a".repeat(64),
  account_name: "LocalSIM UI Contract",
  broker_backend: "local_sim",
  package_id: "pkg_ui_contract",
  manifest_sha256: "b".repeat(64),
  admission_receipt_id: "admission_ui_contract",
  initial_capital: 1_000_000,
  status: "ACTIVE",
  version: 1,
  created_at: "2026-08-31T00:00:00Z",
  updated_at: "2026-08-31T00:00:00Z",
};

const profile = {
  profile_id: "lsprofile_ui_contract",
  profile_hash: "c".repeat(64),
  package_id: account.package_id,
  manifest_sha256: account.manifest_sha256,
  profile_name: "UI Contract",
  status: "ACTIVE",
  version: 1,
  created_at: account.created_at,
  updated_at: account.updated_at,
};

const profileVersion = {
  profile_version_id: "lsprofilever_ui_contract",
  profile_version_hash: "d".repeat(64),
  profile_id: profile.profile_id,
  package_id: account.package_id,
  manifest_sha256: account.manifest_sha256,
  version_no: 1,
  config_json: {},
  config_sha256: "e".repeat(64),
  daily_strategy_profile_version_id: "daily_ui_contract",
  validation_status: "VALIDATED",
  validation_evidence: {},
  created_at: account.created_at,
};

const replay = {
  schema_version: "localsim_replay_job_v1",
  replay_job_id: "lsreplay_ui_contract",
  replay_hash: "f".repeat(64),
  simulation_account_id: account.account_id,
  release_id: "srr_ui_contract",
  binding_id: "simbind_ui_contract",
  start_trade_date: "2026-02-27",
  end_trade_date: "2026-08-28",
  historical_source_id: "market.kline_minute_raw.v1",
  status: "CREATED",
  next_trade_date: "2026-02-27",
  completed_trade_date: null as string | null,
  activation_trade_date: null,
  version: 1,
  created_at: account.created_at,
  updated_at: account.updated_at,
};

const readiness = {
  schema_version: "localsim_cutover_readiness_v1",
  ready: true,
  checked_at: account.updated_at,
  blockers: [] as string[],
  retained_legacy_account_ids: [],
  missing_lineage_account_ids: [],
  legacy_active_session_count: 0,
  legacy_auto_run_count: 0,
  legacy_sentinel_count: 0,
  in_flight_economic_run_count: 0,
};

type MockState = {
  readiness?: typeof readiness;
  readinessStatus?: number;
  readinessGate?: () => Promise<void>;
  packagesStatus?: number;
  replayStatus?: number;
  schedulerStatus?: number;
  replays?: Array<typeof replay & { failure_code?: string; failure_context?: Record<string, unknown> }>;
};

const list = (items: unknown[]) => ({
  ok: true,
  schema_version: "localsim_list_response_v1",
  items,
  next_cursor: null,
  limit: 200,
});

async function json(route: Route, payload: unknown, status = 200) {
  await route.fulfill({ status, contentType: "application/json", body: JSON.stringify(payload) });
}

async function mockLocalSimApi(page: Page, requests: Array<{ method: string; path: string; body?: unknown }>, state: MockState = {}) {
  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname.replace(/^\/api\/v1/, "");
    const method = request.method();
    const body = request.postDataJSON?.();
    requests.push({ method, path, body });
    if (method === "GET" && path === "/simulation-runtime/localsim/cutover-readiness") {
      await state.readinessGate?.();
      if (state.readinessStatus) return json(route, { detail: { code: "READBACK_UNAVAILABLE", message: "切流核验暂不可用" } }, state.readinessStatus);
      return json(route, { readiness: state.readiness || readiness });
    }
    if (method === "GET" && path === "/simulation-runtime/scheduler/verification-status") {
      expect(url.searchParams.get("broker_backend")).toBe("local_sim");
      if (state.schedulerStatus) return json(route, { detail: { code: "SCHEDULER_UNAVAILABLE", message: "调度核验暂不可用" } }, state.schedulerStatus);
      return json(route, { ok: true, scheduler: {
        running: true, thread_alive: true,
        scheduler_loop_health: { status: "HEALTHY", reason_code: "SIMULATION_BACKGROUND_SCHEDULER_RUN_LOOP_OK", consecutive_failure_count: 0 },
        current_trade_date_blockers: { trade_date: "2026-10-17", status: "CLEAR", blocker_count: 0 },
        effective_runtime_health: "NO_CURRENT_DAY_BLOCKER",
      } });
    }
    if (method === "GET" && path === "/strategy-packages") {
      if (state.packagesStatus) return json(route, { detail: { code: "PACKAGES_UNAVAILABLE", message: "策略包列表读取失败" } }, state.packagesStatus);
      return json(route, { ok: true, packages: [{
        package_id: account.package_id,
        package_name: "UI Contract Package",
        package_status: "ACTIVE",
      }] });
    }
    if (method === "GET" && path === "/trading-calendar/status") {
      return json(route, {
        ok: true,
        as_of_date: url.searchParams.get("as_of_date") || "2026-08-31",
        is_trading_day: true,
        latest_completed_trading_day: url.searchParams.get("as_of_date")?.startsWith("2026-02")
          ? "2026-02-27"
          : "2026-08-28",
        previous_trading_day: "2026-08-28",
        next_trading_day: "2026-09-01",
      });
    }
    if (method === "GET" && path === "/simulation-runtime/localsim/accounts") return json(route, list([account]));
    if (method === "GET" && path === "/simulation-runtime/localsim/runtime-profiles") return json(route, list([profile]));
    if (method === "GET" && path === `/simulation-runtime/localsim/runtime-profiles/${profile.profile_id}/versions`) return json(route, list([profileVersion]));
    if (method === "GET" && path === `/strategy-packages/${account.package_id}/execution-policies`) {
      return json(route, { ok: true, execution_policies: [{
        policy_id: "execpol_ui_contract",
        policy_name: "TWAP",
        algo_code: "TWAP",
      }] });
    }
    if (method === "POST" && path === "/simulation-runtime/localsim/accounts") {
      return json(route, { ok: true, schema_version: "localsim_control_response_v1", account });
    }
    if (method === "GET" && path === "/simulation-runtime/localsim/replays") {
      if (state.replayStatus) return json(route, { detail: { code: "REPLAYS_UNAVAILABLE", message: "回放记录暂不可用" } }, state.replayStatus);
      return json(route, list(state.replays || [replay]));
    }
    if (method === "GET" && path.startsWith("/simulation-runtime/localsim/replays/")) {
      if (state.replayStatus) return json(route, { detail: { code: "REPLAY_UNAVAILABLE", message: "回放记录暂不可用" } }, state.replayStatus);
      return json(route, { ok: true, replay: state.replays?.[0] || replay });
    }
    if (method === "POST" && path === "/simulation-runtime/localsim/replays") {
      return json(route, { ok: true, schema_version: "localsim_control_response_v1", account, replay });
    }
    return json(route, { detail: { code: "UNMOCKED", message: `${method} ${path}` } }, 404);
  });
}

test("LocalSIM UI uses only the successor control plane and never calls session or scheduler mutations", async ({ page }) => {
  const requests: Array<{ method: string; path: string; body?: unknown }> = [];
  await mockLocalSimApi(page, requests);

  await page.goto("/simulation/localsim?package_id=pkg_ui_contract&top_k=30");
  await expect(page.getByRole("heading", { name: "模拟账户创建与配置" })).toBeVisible();
  await expect(page.getByRole("button", { name: "创建模拟账户" })).toBeEnabled();
  await page.getByRole("button", { name: "创建模拟账户" }).click();
  const create = requests.find((item) => item.method === "POST" && item.path.endsWith("/localsim/accounts"));
  expect(create?.body).toMatchObject({
    schema_version: "localsim_account_create_request_v1",
    package_id: account.package_id,
    runtime_profile_version_id: profileVersion.profile_version_id,
    execution_policy_version_id: "execpol_ui_contract",
  });
  expect(JSON.stringify(create?.body)).not.toMatch(/manifest_sha256|admission_receipt_id|release_id|binding_id|ledger_scope|policy_json/);

  await page.goto("/simulation/localsim/replays?package_id=pkg_ui_contract");
  await expect(page.getByRole("heading", { name: "历史回放", exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "创建历史回放" })).toBeEnabled();
  await page.getByRole("button", { name: "创建历史回放" }).click();
  const replayCreate = requests.find((item) => item.method === "POST" && item.path.endsWith("/localsim/replays"));
  expect(replayCreate?.body).toMatchObject({
    schema_version: "localsim_replay_create_request_v1",
    historical_source_id: "dataset_files.frozen_market.v1",
    end_trade_date: "2026-08-28",
  });
  expect(JSON.stringify(replayCreate?.body)).not.toMatch(/source_sha256|calendar_snapshot|day_engine|cursor|safe_boundary/);

  const inventory = requests.map((item) => `${item.method} ${item.path}`);
  expect(inventory.join("\n")).not.toMatch(/\/paper-v2\/(portfolios|sessions|replay|auto-run|scheduler)/);
  expect(inventory.join("\n")).not.toMatch(/\/simulation-runtime\/scheduler\/(start|stop|tick)/);
});

test("retired LocalSIM pages have no compatibility route", async ({ request }) => {
  for (const path of ["/paper-v2", "/paper-v2/portfolios", "/paper-v2/running", "/paper-v2/simulation-runtime"]) {
    const response = await request.get(path);
    expect(response.status(), path).toBe(404);
  }
});

test("remaining Selection and MiniQMT pages do not call the retired Paper control plane", () => {
  const miniQmtSource = readFileSync(resolve(process.cwd(), "src/app/paper-v2/miniqmt-sim/page.tsx"), "utf8");
  const selectionSource = readFileSync(resolve(process.cwd(), "src/app/paper-v2/selection/page.tsx"), "utf8");

  expect(miniQmtSource).not.toContain("paperV2Api");
  expect(miniQmtSource).not.toMatch(/auto-run|miniqmtPortfolios|PaperPortfolio|recoverAutoRun/);
  expect(miniQmtSource).toContain("simulationRuntimeApi.listRuns");
  expect(selectionSource).not.toContain("tradingDayDefaults");
  expect(selectionSource).toContain("tradingDayStatus");
});

for (const path of ["/simulation/localsim/configure", "/simulation/localsim/replays"]) {
  test(`${path} follows economic in-flight changes without reloading or resetting form edits`, async ({ page }) => {
    await page.clock.install();
    const requests: Array<{ method: string; path: string }> = [];
    const state: MockState = { readiness: { ...readiness, ready: false, blockers: ["in_flight_economic_run_count:1"], in_flight_economic_run_count: 1 } };
    await mockLocalSimApi(page, requests, state);
    await page.goto(path);
    await expect(page.getByText("切流条件 · 有 1 个交易日正在执行，切流暂时等待", { exact: true })).toBeVisible();
    const create = page.getByRole("button", { name: path.endsWith("replays") ? "创建历史回放" : "创建模拟账户", exact: true });
    await expect(create).toBeDisabled();
    await page.getByLabel("账户名称", { exact: true }).fill("保持我的输入");
    await expect(page.getByText("in_flight_economic_run_count:1", { exact: false })).not.toBeVisible();
    state.readiness = { ...readiness };
    await page.clock.runFor(5_100);
    await expect(page.getByText("切流条件 · 切流前置检查通过", { exact: true })).toBeVisible();
    await expect(create).toBeEnabled();
    await expect(page.getByLabel("账户名称", { exact: true })).toHaveValue("保持我的输入");
    state.readiness = { ...readiness, ready: false, blockers: ["in_flight_economic_run_count:1", "orphan_ledger_scope_count:2"], in_flight_economic_run_count: 1 };
    await page.clock.runFor(5_100);
    await expect(page.getByText("切流条件 · 切流存在阻塞", { exact: true })).toBeVisible();
    await expect(page.getByText("存在未关联的账本记录：2")).toBeVisible();
    await expect(create).toBeDisabled();
    expect(requests.filter((request) => request.method !== "GET")).toEqual([]);
  });
}

test("failed readiness invalidates controls and a successful read clears the error", async ({ page }) => {
  await page.clock.install();
  const requests: Array<{ method: string; path: string }> = [];
  const state: MockState = {};
  await mockLocalSimApi(page, requests, state);
  await page.goto("/simulation/localsim/configure");
  const create = page.getByRole("button", { name: "创建模拟账户", exact: true });
  await expect(create).toBeEnabled();
  state.readinessStatus = 503;
  await page.clock.runFor(5_100);
  await expect(page.getByText("切流条件 · 切流状态读取失败", { exact: true })).toBeVisible();
  await expect(create).toBeDisabled();
  await expect(page.getByText("当日调度核验 · 未发现当日调度阻塞", { exact: true })).toBeVisible();
  state.readinessStatus = undefined;
  await page.getByRole("button", { name: "刷新运行状态" }).click();
  await expect(page.getByText("切流条件 · 切流前置检查通过", { exact: true })).toBeVisible();
  await expect(page.getByText("切流条件 · 切流状态读取失败", { exact: true })).not.toBeVisible();
  await expect(create).toBeEnabled();
  expect(requests.filter((request) => request.method !== "GET")).toEqual([]);
});

test("returning to a page refreshes readiness independently of package-list failures", async ({ page }) => {
  const requests: Array<{ method: string; path: string }> = [];
  const state: MockState = { packagesStatus: 503 };
  await mockLocalSimApi(page, requests, state);
  await page.goto("/simulation/localsim/replays");
  await expect(page.getByText("切流条件 · 切流前置检查通过", { exact: true })).toBeVisible();
  await expect(page.getByText("策略目录读取失败，请刷新重试。", { exact: true })).toBeVisible();
  state.readiness = { ...readiness, ready: false, blockers: ["in_flight_economic_run_count:1"], in_flight_economic_run_count: 1 };
  await page.evaluate(() => { window.dispatchEvent(new Event("focus")); });
  await expect(page.getByText("切流条件 · 有 1 个交易日正在执行，切流暂时等待", { exact: true })).toBeVisible();
  expect(requests.filter((request) => request.method !== "GET")).toEqual([]);
});

test("contradictory readiness cannot enable controls or masquerade as normal in-flight waiting", async ({ page }) => {
  const state: MockState = { readiness: { ...readiness, ready: true, blockers: ["in_flight_economic_run_count:1"], in_flight_economic_run_count: 1 } };
  await mockLocalSimApi(page, [], state);
  await page.goto("/simulation/localsim/configure");
  await expect(page.getByText("切流条件 · 切流状态读取失败", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "创建模拟账户", exact: true })).toBeDisabled();
});

test("replay polling separates known asynchronous inference from real historical failures", async ({ page }, testInfo) => {
  await page.clock.install();
  const requests: Array<{ method: string; path: string }> = [];
  const state: MockState = { replays: [{ ...replay, status: "FAILED_RETRYABLE", failure_code: "DataUnavailableError", failure_context: { reason_code: "SIMULATION_SELECTION_INFERENCE_IN_PROGRESS" } }] };
  await mockLocalSimApi(page, requests, state);
  await page.goto("/simulation/localsim/replays");
  await expect(page.getByText("等待选股推理（自动接续）", { exact: true })).toBeVisible();
  state.replays = [{ ...replay, status: "FAILED_TERMINAL", completed_trade_date: "2026-07-09", next_trade_date: "2026-07-10", failure_code: "BrokerConnectivityError", failure_context: { message: "LocalSim could not load minute market data", trade_date: "2026-07-10" } }];
  await page.clock.runFor(5_100);
  await expect(page.getByText("回放已终止，需诊断", { exact: true })).toBeVisible();
  await expect(page.getByText("历史分钟行情读取失败，需要核对数据及交易资格；尚未完成该交易日。", { exact: true })).toBeVisible();
  await expect(page.getByText("下一日 2026-07-10", { exact: true })).toBeVisible();
  await expect(page.getByText("当日调度核验 · 未发现当日调度阻塞", { exact: true })).toBeVisible();
  await expect(page.getByText("等待选股推理（自动接续）", { exact: true })).not.toBeVisible();
  state.replays = [{ ...replay, status: "FAILED_RETRYABLE", failure_code: "DataUnavailableError", failure_context: { reason_code: "REFERENCE_CLOSE_MISSING", message: "真实参考价缺失" } }];
  await page.clock.runFor(5_100);
  await expect(page.getByText("执行受阻，等待自动重试", { exact: true })).toBeVisible();
  await expect(page.getByText("真实参考价缺失，需要核对合法估值依据。", { exact: true })).toBeVisible();
  state.readiness = { ...readiness, ready: false, blockers: ["in_flight_economic_run_count:1"], in_flight_economic_run_count: 1 };
  state.replays = [
    { ...replay, replay_job_id: "lsreplay_lstm_contract", status: "FAILED_RETRYABLE", failure_context: { reason_code: "SIMULATION_SELECTION_INFERENCE_IN_PROGRESS" } },
    { ...replay, replay_job_id: "lsreplay_tcn_contract", status: "FAILED_TERMINAL", completed_trade_date: "2026-07-09", next_trade_date: "2026-07-10", failure_code: "BrokerConnectivityError", failure_context: { message: "LocalSim could not load minute market data" } },
  ];
  await page.getByRole("button", { name: "刷新运行状态" }).click();
  await expect(page.getByText("等待选股推理（自动接续）", { exact: true })).toBeVisible();
  await expect(page.getByText("回放已终止，需诊断", { exact: true })).toBeVisible();
  await expect(page.getByText("切流条件 · 有 1 个交易日正在执行，切流暂时等待", { exact: true })).toBeVisible();
  await page.screenshot({ path: testInfo.outputPath("localsim-replay-status.png"), fullPage: true });
  expect(requests.filter((request) => request.method !== "GET")).toEqual([]);
});

test("replay detail clears read errors after recovery without issuing lifecycle mutations", async ({ page }) => {
  const requests: Array<{ method: string; path: string }> = [];
  const state: MockState = { replayStatus: 503 };
  await mockLocalSimApi(page, requests, state);
  await page.goto(`/simulation/localsim/replays/${replay.replay_job_id}`);
  await expect(page.getByText("本次读取失败，保留上次成功结果；请按账户与发生时间核查。", { exact: true })).toBeVisible();
  state.replayStatus = undefined;
  await page.getByRole("button", { name: "刷新回放状态" }).click();
  await expect(page.getByText("等待开始", { exact: true })).toBeVisible();
  await expect(page.getByText("本次读取失败，保留上次成功结果；请按账户与发生时间核查。", { exact: true })).not.toBeVisible();
  expect(requests.filter((request) => request.method !== "GET")).toEqual([]);
});

test("slow readback is serial, times out visibly, then resumes polling", async ({ page }) => {
  await page.clock.install();
  const requests: Array<{ method: string; path: string }> = [];
  const state: MockState = {};
  await mockLocalSimApi(page, requests, state);
  await page.goto("/simulation/localsim/configure");
  const create = page.getByRole("button", { name: "创建模拟账户", exact: true });
  await expect(create).toBeEnabled();
  const count = () => requests.filter((request) => request.path.endsWith("cutover-readiness")).length;
  const before = count();
  let release!: () => void;
  const barrier = new Promise<void>((resolve) => { release = resolve; });
  state.readinessGate = () => barrier;
  try {
    await page.clock.runFor(5_100);
    await expect.poll(count).toBe(before + 1);
    await page.evaluate(() => { for (let i = 0; i < 5; i++) window.dispatchEvent(new Event("focus")); });
    expect(count()).toBe(before + 1);
    await page.clock.runFor(12_100);
    await expect(page.getByText("切流条件 · 切流状态读取失败", { exact: true })).toBeVisible();
    await expect(page.getByText(/读取超时，请重试/)).toBeVisible();
    await expect(create).toBeDisabled();
  } finally {
    state.readinessGate = undefined;
    release();
  }
  await page.clock.runFor(5_100);
  await expect(create).toBeEnabled();
  expect(requests.filter((request) => request.method !== "GET")).toEqual([]);
});

test("hidden pages stop polling and invalidate old readiness before resuming", async ({ page }) => {
  await page.clock.install();
  const requests: Array<{ method: string; path: string }> = [];
  await mockLocalSimApi(page, requests);
  await page.goto("/simulation/localsim/configure");
  const create = page.getByRole("button", { name: "创建模拟账户", exact: true });
  await expect(create).toBeEnabled();
  const before = requests.filter((request) => request.path.endsWith("cutover-readiness")).length;
  await page.evaluate(() => {
    Object.defineProperty(document, "visibilityState", { configurable: true, value: "hidden" });
    document.dispatchEvent(new Event("visibilitychange"));
  });
  await page.clock.runFor(30_000);
  expect(requests.filter((request) => request.path.endsWith("cutover-readiness"))).toHaveLength(before);
  await expect(create).toBeDisabled();
  await page.evaluate(() => {
    Object.defineProperty(document, "visibilityState", { configurable: true, value: "visible" });
    document.dispatchEvent(new Event("visibilitychange"));
  });
  await expect(create).toBeEnabled();
});

test("scheduler read failures remain visible without overriding backend cutover authority", async ({ page }) => {
  const state: MockState = { schedulerStatus: 503 };
  await mockLocalSimApi(page, [], state);
  await page.goto("/simulation/localsim/configure");
  await expect(page.getByText("当日调度核验 · 当日调度核验读取失败", { exact: true })).toBeVisible();
  await expect(page.getByText("切流条件 · 切流前置检查通过", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "创建模拟账户", exact: true })).toBeEnabled();
  state.schedulerStatus = undefined;
  await page.getByRole("button", { name: "刷新运行状态" }).click();
  await expect(page.getByText("当日调度核验 · 未发现当日调度阻塞", { exact: true })).toBeVisible();
});

test("detail rejects a different replay identity and keeps controls unavailable", async ({ page }) => {
  await mockLocalSimApi(page, []);
  await page.goto("/simulation/localsim/replays/lsreplay_different_contract");
  await expect(page.getByText("本次读取失败，保留上次成功结果；请按账户与发生时间核查。", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "取消回放" })).not.toBeVisible();
});
