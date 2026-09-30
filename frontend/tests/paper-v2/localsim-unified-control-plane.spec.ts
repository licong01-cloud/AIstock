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
  completed_trade_date: null,
  activation_trade_date: null,
  version: 1,
  created_at: account.created_at,
  updated_at: account.updated_at,
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

async function mockLocalSimApi(page: Page, requests: Array<{ method: string; path: string; body?: unknown }>) {
  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname.replace(/^\/api\/v1/, "");
    const method = request.method();
    const body = request.postDataJSON?.();
    requests.push({ method, path, body });
    if (method === "GET" && path === "/simulation-runtime/localsim/cutover-readiness") {
      return json(route, { readiness: {
        schema_version: "localsim_cutover_readiness_v1",
        ready: true,
        checked_at: account.updated_at,
        blockers: [],
        retained_legacy_account_ids: [],
        missing_lineage_account_ids: [],
        legacy_active_session_count: 0,
        legacy_auto_run_count: 0,
        legacy_sentinel_count: 0,
        in_flight_economic_run_count: 0,
      } });
    }
    if (method === "GET" && path === "/strategy-packages") {
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
    if (method === "GET" && path === "/simulation-runtime/localsim/replays") return json(route, list([replay]));
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
  await expect(page.getByRole("heading", { name: "LocalSIM 模拟盘" })).toBeVisible();
  await expect(page.getByRole("button", { name: "原子创建 LocalSIM" })).toBeEnabled();
  await page.getByRole("button", { name: "原子创建 LocalSIM" }).click();
  const create = requests.find((item) => item.method === "POST" && item.path.endsWith("/localsim/accounts"));
  expect(create?.body).toMatchObject({
    schema_version: "localsim_account_create_request_v1",
    package_id: account.package_id,
    runtime_profile_version_id: profileVersion.profile_version_id,
    execution_policy_version_id: "execpol_ui_contract",
  });
  expect(JSON.stringify(create?.body)).not.toMatch(/manifest_sha256|admission_receipt_id|release_id|binding_id|ledger_scope|policy_json/);

  await page.goto("/simulation/localsim/replays?package_id=pkg_ui_contract");
  await expect(page.getByRole("heading", { name: "LocalSIM 历史回放" })).toBeVisible();
  await expect(page.getByRole("button", { name: "原子创建回放" })).toBeEnabled();
  await page.getByRole("button", { name: "原子创建回放" }).click();
  const replayCreate = requests.find((item) => item.method === "POST" && item.path.endsWith("/localsim/replays"));
  expect(replayCreate?.body).toMatchObject({
    schema_version: "localsim_replay_create_request_v1",
    historical_source_id: "market.kline_minute_raw.v1",
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
