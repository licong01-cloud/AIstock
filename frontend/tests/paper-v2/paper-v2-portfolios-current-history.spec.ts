import { expect, test, type Page, type Route } from "@playwright/test";

type JsonObject = Record<string, any>;

const packageRow = {
  package_id: "pkg_bug504",
  package_name: "BUG-504 StrategyPackage",
  package_status: "PAPER_ENABLED",
  manifest_sha256: "sha_bug504",
};

const activeLocalPortfolio = {
  portfolio_id: "pf_active_local",
  package_id: packageRow.package_id,
  portfolio_name: "Active LocalSim",
  manifest_sha256: packageRow.manifest_sha256,
  initial_cash: 1000000,
  start_date: "2026-06-01",
  data_source: "TDX_REALTIME",
  broker_backend: "local_sim",
  status: "RUNNING",
};

const activeMiniQmtPortfolio = {
  ...activeLocalPortfolio,
  portfolio_id: "pf_active_miniqmt",
  portfolio_name: "Active MiniQMT",
  broker_backend: "minqmt_sim",
  status: "RUNNING",
};

const staleRunningPortfolio = {
  ...activeLocalPortfolio,
  portfolio_id: "pf_stale_running",
  portfolio_name: "Stale Running History",
  broker_backend: "local_sim",
  status: "RUNNING",
};

const readyHistoryPortfolio = {
  ...activeLocalPortfolio,
  portfolio_id: "pf_ready_history",
  portfolio_name: "Ready History",
  data_source: "DB_HISTORICAL",
  broker_backend: "local_sim",
  status: "READY",
};

const completedHistoryPortfolio = {
  ...activeLocalPortfolio,
  portfolio_id: "pf_completed_history",
  portfolio_name: "Completed History",
  data_source: "DB_HISTORICAL",
  broker_backend: "local_sim",
  status: "COMPLETED",
};

const failedMiniQmtHistoryPortfolio = {
  ...activeLocalPortfolio,
  portfolio_id: "pf_failed_miniqmt_history",
  portfolio_name: "Failed MiniQMT History",
  data_source: "MINIQMT_REALTIME",
  broker_backend: "minqmt_sim",
  status: "FAILED",
};

function ok(payload: JsonObject = {}): JsonObject {
  return { ok: true, ...payload };
}

async function respond(route: Route, payload: JsonObject | JsonObject[], status = 200) {
  await route.fulfill({ status, contentType: "application/json", body: JSON.stringify(payload) });
}

function runningSummary(portfolio: JsonObject, hasTickableSession: boolean): JsonObject {
  return {
    portfolio,
    latest_run: hasTickableSession ? {
      run_id: `run_${portfolio.portfolio_id}`,
      portfolio_id: portfolio.portfolio_id,
      trade_date: "2026-06-23",
      status: "SUCCEEDED",
      data_source: portfolio.data_source,
    } : null,
    latest_session: {
      session_id: `session_${portfolio.portfolio_id}`,
      portfolio_id: portfolio.portfolio_id,
      mode: portfolio.broker_backend === "minqmt_sim" ? "LIVE_ONLY" : "CATCHUP_THEN_LIVE",
      status: hasTickableSession ? "LIVE_WAITING_FOR_BAR" : "FAILED",
      phase: hasTickableSession ? "LIVE" : "DONE",
      start_date: portfolio.start_date,
    },
    operability: {
      tickable_session_count: hasTickableSession ? 1 : 0,
      has_tickable_session: hasTickableSession,
      latest_session_status: hasTickableSession ? "LIVE_WAITING_FOR_BAR" : "FAILED",
      latest_session_mode: portfolio.broker_backend === "minqmt_sim" ? "LIVE_ONLY" : "CATCHUP_THEN_LIVE",
      latest_session_is_terminal: !hasTickableSession,
      no_operable_session: !hasTickableSession,
      remediation_hint: hasTickableSession ? null : "no scheduler-tickable live/replay session exists",
    },
    counts: { orders: 0, fills: 0, positions: 0, errors: hasTickableSession ? 0 : 1 },
    latest_snapshot: null,
    recent_snapshots: [],
    latest_positions: [],
  };
}

async function mockPortfolioPageApi(page: Page) {
  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname.replace(/^\/api\/v1/, "");
    const method = request.method();

    if (method === "GET" && path === "/paper-v2/trading-days/defaults") {
      return respond(route, ok({
        latest_trading_day: "2026-06-23",
        replay_start_date: "2026-06-10",
        replay_end_date: "2026-06-23",
        lookback_trading_days: 10,
        available_trading_day_count: 10,
      }));
    }
    if (method === "GET" && path === "/hmm-training/configs") return respond(route, []);
    if (method === "GET" && path === "/selection-center/industry-tree") return respond(route, { tree: [] });
    if (method === "GET" && path === `/strategy-packages/${packageRow.package_id}/execution-policies`) {
      return respond(route, { execution_policies: [] });
    }
    if (method === "GET" && path === "/strategy-packages") {
      return respond(route, { packages: [packageRow] });
    }
    if (method === "GET" && path === "/paper-v2/running-summary") {
      return respond(route, ok({
        summaries: [
          runningSummary(activeLocalPortfolio, true),
          runningSummary(activeMiniQmtPortfolio, true),
          runningSummary(staleRunningPortfolio, false),
        ],
        pagination: { page: 1, page_size: 50, total: 3, total_pages: 1, sort_by: "latest_run_time", sort_dir: "desc" },
      }));
    }
    if (method === "GET" && path === "/paper-v2/portfolios") {
      const portfolios = [
        activeLocalPortfolio,
        activeMiniQmtPortfolio,
        staleRunningPortfolio,
        readyHistoryPortfolio,
        completedHistoryPortfolio,
        failedMiniQmtHistoryPortfolio,
      ];
      return respond(route, ok({
        portfolios,
        pagination: { page: 1, page_size: 20, total: portfolios.length, total_pages: 1 },
      }));
    }

    return respond(route, { detail: `unexpected route ${method} ${path}` }, 404);
  });
}

test("Portfolio page separates current LocalSim/MiniQMT runs from history and uses full-width launch card", async ({ page }) => {
  await mockPortfolioPageApi(page);
  await page.goto("/paper-v2/portfolios");

  const launchSection = page.locator("section").filter({ hasText: "从单个策略包启动模拟盘" });
  await expect(launchSection).toBeVisible();
  await expect(launchSection.locator("input, select, button").first()).toBeVisible();
  await expect.poll(async () => launchSection.evaluate((node) => node.parentElement?.classList.contains("pv2-grid-main") || false)).toBe(false);

  const currentSection = page.locator("section").filter({ hasText: "当前运行中模拟盘" });
  await expect(currentSection).toContainText("Active LocalSim");
  await expect(currentSection).toContainText("Active MiniQMT");
  await expect(currentSection).not.toContainText("Ready History");
  await expect(currentSection).not.toContainText("Completed History");
  await expect(currentSection).not.toContainText("Failed MiniQMT History");
  await expect(currentSection).not.toContainText("Stale Running History");

  const historySection = page.locator("section").filter({ hasText: "历史模拟盘记录" });
  await expect(historySection).toContainText("Ready History");
  await expect(historySection).toContainText("Completed History");
  await expect(historySection).toContainText("Failed MiniQMT History");
  await expect(historySection).toContainText("Stale Running History");
  await expect(historySection).toContainText("状态残留/无可推进会话");
  await expect(historySection).not.toContainText("Active LocalSim");
  await expect(historySection).not.toContainText("Active MiniQMT");
});
