import { expect, test, type Page, type Route } from "@playwright/test";

type JsonObject = Record<string, any>;

const configId = "hmm_cfg_bug076";
const snapshotId = "hmm_snap_bug076";
const coveredPath = "artifacts/hmm/bug076/preset_A_2026-05-18_2026-05-20.parquet";
const coveredDates = ["2026-05-18", "2026-05-19", "2026-05-20"];

const packageRow = {
  package_id: "pkg_bug076",
  package_name: "BUG-076 StrategyPackage",
  package_status: "PAPER_ENABLED",
  manifest_sha256: "sha_bug076",
};

const portfolioRow = {
  portfolio_id: "pf_bug076",
  package_id: packageRow.package_id,
  portfolio_name: "BUG-076 Portfolio",
  manifest_sha256: packageRow.manifest_sha256,
  initial_cash: 1000000,
  start_date: "2026-05-18",
  data_source: "DB_HISTORICAL",
  status: "READY",
};

const hmmConfig = {
  config_id: configId,
  model_type: "HMM",
  display_name: "BUG-076 HMM",
  config_json: {},
  snapshot_count: 1,
};

const hmmSnapshot = {
  snapshot_id: snapshotId,
  config_id: configId,
  display_name: "BUG-076 snapshot",
  trained_at: "2026-05-17T00:00:00Z",
  model_path: "artifacts/hmm/bug076/model.pkl",
  sector_count: 31,
  status: "completed",
  coefficient_artifacts: [
    {
      path: coveredPath,
      preset: "preset_A",
      start_date: "2026-05-18",
      end_date: "2026-05-20",
      covered_trade_dates: coveredDates,
      date_count: coveredDates.length,
    },
  ],
};

function ok(payload: JsonObject = {}): JsonObject {
  return { ok: true, ...payload };
}

function runtimeHmm(payload: JsonObject): JsonObject {
  return payload?.runtime_config?.runtime_profile?.hmm || payload?.config_json?.runtime_profile?.hmm || {};
}

async function respond(route: Route, payload: JsonObject, status = 200) {
  await route.fulfill({ status, contentType: "application/json", body: JSON.stringify(payload) });
}

async function mockPaperV2Api(page: Page, captures: JsonObject[]) {
  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname.replace(/^\/api\/v1/, "");
    const method = request.method();
    const post = () => {
      try {
        return request.postDataJSON() as JsonObject;
      } catch {
        return {};
      }
    };

    if (method === "GET" && path === "/paper-v2/trading-days/defaults") {
      return respond(route, { latest_trading_day: "2026-05-20", replay_start_date: "2026-05-18", replay_end_date: "2026-05-20", lookback_trading_days: 10, available_trading_day_count: 3 });
    }
    if (method === "GET" && path === "/hmm-training/configs") return respond(route, [hmmConfig]);
    if (method === "GET" && path === `/hmm-training/configs/${configId}/snapshots`) return respond(route, [hmmSnapshot]);
    if (method === "GET" && path.startsWith("/strategy-packages")) {
      if (path.endsWith("/execution-policies")) return respond(route, { execution_policies: [{ policy_id: "policy_bug076", policy_name: "V25", algo_code: "V25_TWO_STAGE", paper_enabled: true }] });
      return respond(route, { packages: [packageRow] });
    }
    if (method === "GET" && path === "/paper-v2/portfolios") return respond(route, { portfolios: [portfolioRow] });
    if (method === "POST" && path === "/paper-v2/portfolios") return respond(route, { portfolio: portfolioRow });
    if (method === "GET" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}`) return respond(route, { portfolio: portfolioRow });
    if (method === "GET" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/runs`) return respond(route, { runs: [] });
    if (method === "GET" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/sessions`) return respond(route, { sessions: [{ session_id: "active_bug076", portfolio_id: portfolioRow.portfolio_id, mode: "REPLAY_ONLY", status: "REPLAYING", phase: "HISTORICAL_REPLAY", start_date: "2026-05-18", end_date: "2026-05-20" }] });
    if (method === "GET" && path === "/paper-v2/session-scheduler/status") return respond(route, { scheduler: { running: false } });
    if (method === "GET" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/run-events`) return respond(route, { run_events: [] });
    if (method === "GET" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/errors`) return respond(route, { errors: [] });
    if (method === "GET" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/execution-policies`) return respond(route, { execution_policies: [{ policy_id: "policy_bug076", policy_name: "V25", algo_code: "V25_TWO_STAGE", paper_enabled: true, is_portfolio_default: true }] });
    if (method === "GET" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/execution-policy-activations`) return respond(route, { activations: [] });
    if (method === "GET" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/session-capabilities`) return respond(route, { capabilities: { modes: { REPLAY_ONLY: { can_start: true, errors: [] }, CATCHUP_THEN_LIVE: { can_start: true, errors: [] }, LIVE_ONLY: { can_start: true, errors: [] } } } });
    if (method === "GET" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/runtime-profiles`) return respond(route, { profiles: [{ profile_id: "rp_bug076", profile_name: "Runtime", status: "ACTIVE" }] });
    if (method === "GET" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/runtime-config-activations`) return respond(route, { activations: [] });
    if (method === "GET" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/config-change-audit`) return respond(route, { audit: [] });
    if (method === "GET" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/runtime-profiles/rp_bug076/versions`) return respond(route, { versions: [{ profile_version_id: "rpv_bug076", profile_id: "rp_bug076", version_no: 1, config_sha256: "cfgsha" }] });

    if (method === "POST" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/runtime-profiles`) {
      const payload = await post();
      captures.push({ path, payload });
      return respond(route, { profile: { profile_id: "rp_created", profile_name: payload.profile_name, status: "ACTIVE" }, version: { profile_version_id: "rpv_created", version_no: 1, config_sha256: "cfgsha" } });
    }
    if (method === "POST" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/runtime-profiles/rp_bug076/versions`) {
      const payload = await post();
      captures.push({ path, payload });
      return respond(route, { version: { profile_version_id: "rpv_new", version_no: 2, config_sha256: "cfgsha2" } });
    }
    if (method === "POST" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/runtime-config-activations`) return respond(route, { activation: { activation_id: "act_bug076", status: "ACTIVE", trade_date: "2026-05-18" } });
    if (method === "POST" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/readiness`) {
      const payload = await post();
      captures.push({ path, payload });
      return respond(route, { readiness: { trade_date: payload.trade_date, checks: [{ check_name: "runtime", status: "passed" }] } });
    }
    if (method === "POST" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/run-day`) {
      const payload = await post();
      captures.push({ path, payload });
      return respond(route, { result: { run_id: "run_bug076", status: "SUCCEEDED", runtime_hmm: runtimeHmm(payload) } });
    }
    if (method === "POST" && path === `/paper-v2/portfolios/${portfolioRow.portfolio_id}/sessions`) {
      const payload = await post();
      captures.push({ path, payload });
      return respond(route, { session: { session_id: `session_${captures.length}`, portfolio_id: portfolioRow.portfolio_id, mode: payload.mode, status: "SUCCEEDED", phase: "DONE", start_date: payload.start_date, end_date: payload.end_date } });
    }
    if (method === "POST" && /^\/paper-v2\/sessions\/session_\d+\/tick$/.test(path)) return respond(route, { progress: { session: { session_id: path.split("/")[3], status: "SUCCEEDED", mode: "REPLAY_ONLY" }, day_count: 1, events: [] } });
    if (method === "GET" && /^\/paper-v2\/sessions\/session_\d+\/progress$/.test(path)) return respond(route, { progress: { session: { session_id: path.split("/")[3], status: "SUCCEEDED", mode: "REPLAY_ONLY" }, day_count: 1, events: [] } });
    if (method === "POST" && /^\/paper-v2\/sessions\/[^/]+\/switch-mode$/.test(path)) {
      const payload = await post();
      captures.push({ path, payload });
      return respond(route, { session: { session_id: "session_switch", portfolio_id: portfolioRow.portfolio_id, mode: payload.target_mode, status: "SUCCEEDED", phase: "DONE", start_date: payload.start_date, end_date: payload.end_date } });
    }

    return respond(route, { detail: `unexpected route ${method} ${path}` }, 404);
  });
}

test("Portfolio creation persists and starts sessions with explicit HMM coefficients_path", async ({ page }) => {
  const captures: JsonObject[] = [];
  await mockPaperV2Api(page, captures);

  await page.goto(`/paper-v2/portfolios?package_id=${packageRow.package_id}`);
  await page.getByTestId("portfolio-hmm-enabled").check();
  await page.getByTestId("portfolio-hmm-config").selectOption(configId);
  await page.getByTestId("portfolio-hmm-preset").selectOption("preset_A");
  await expect(page.getByTestId("portfolio-hmm-coverage")).toContainText("coefficients_path");
  await page.getByTestId("portfolio-replay-start").fill("2026-05-18");
  await page.getByTestId("portfolio-replay-end").fill("2026-05-20");
  await page.getByTestId("portfolio-create").click();

  await expect.poll(() => captures.length, { timeout: 30_000 }).toBeGreaterThanOrEqual(2);
  const profile = captures.find((item) => item.path.endsWith("/runtime-profiles"));
  const session = captures.find((item) => item.path.endsWith("/sessions"));
  expect(profile?.payload.config_json.runtime_profile.hmm.coefficients_path).toBe(coveredPath);
  expect(session?.payload.runtime_config.runtime_profile.hmm.coefficients_path).toBe(coveredPath);
});

test("Run console sends explicit HMM coefficients_path for day, replay, live, switch, and runtime profile actions", async ({ page }) => {
  const captures: JsonObject[] = [];
  await mockPaperV2Api(page, captures);

  await page.goto(`/paper-v2/portfolios/${portfolioRow.portfolio_id}/run-console`);
  await page.getByTestId("console-runtime-hmm-enabled").check();
  await page.getByTestId("console-runtime-hmm-config").selectOption(configId);
  await page.getByTestId("console-runtime-hmm-preset").selectOption("preset_A");
  await page.getByTestId("console-trade-date").fill("2026-05-18");
  await expect(page.getByTestId("console-runtime-hmm-coverage")).toContainText("coefficients_path");

  await page.getByTestId("console-readiness").click();
  await expect(page.getByTestId("console-run-day")).toBeEnabled({ timeout: 30_000 });
  await page.getByTestId("console-run-day").click();
  await page.getByTestId("console-runtime-save-profile").click();
  await page.getByTestId("console-runtime-save-version").click();
  await page.getByTestId("console-replay-start").fill("2026-05-18");
  await page.getByTestId("console-replay-end").fill("2026-05-20");
  await page.getByTestId("console-replay-reject").click();
  await page.getByTestId("console-live-start").fill("2026-05-18");
  await page.getByTestId("console-live-create").click();
  await page.getByTestId("console-switch-mode").selectOption("CATCHUP_THEN_LIVE");
  await page.getByTestId("console-switch-start").fill("2026-05-18");
  await page.getByTestId("console-switch-end").fill("2026-05-20");
  await page.getByTestId("console-switch-mode-apply").click();

  for (const item of captures) {
    const hmm = runtimeHmm(item.payload);
    if (item.path.includes("readiness") || item.path.includes("run-day") || item.path.includes("sessions") || item.path.includes("runtime-profiles")) {
      expect(hmm.coefficients_path, `${item.path} must include coefficients_path`).toBe(coveredPath);
      expect(hmm.model_snapshot_id, `${item.path} must include model snapshot`).toBe(snapshotId);
      expect(hmm.signal_preset, `${item.path} must include preset`).toBe("preset_A");
    }
  }
});

test("Run console blocks HMM-enabled replay before API submission when coefficient coverage is missing", async ({ page }) => {
  const captures: JsonObject[] = [];
  await mockPaperV2Api(page, captures);

  await page.goto(`/paper-v2/portfolios/${portfolioRow.portfolio_id}/run-console`);
  await page.getByTestId("console-runtime-hmm-enabled").check();
  await page.getByTestId("console-runtime-hmm-config").selectOption(configId);
  await page.getByTestId("console-runtime-hmm-preset").selectOption("preset_A");
  await page.getByTestId("console-replay-start").fill("2026-05-18");
  await page.getByTestId("console-replay-end").fill("2026-05-21");
  const before = captures.length;
  await page.getByTestId("console-replay-reject").click();

  await expect(page.locator(".pv2-error-panel")).toContainText("HMM coefficient artifact does not cover", { timeout: 30_000 });
  expect(captures.length).toBe(before);
});
