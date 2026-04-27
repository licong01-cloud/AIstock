import { expect, test, type APIRequestContext, type Page } from "@playwright/test";

const API_BASE = process.env.PAPER_V2_API_BASE || process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8011/api/v1";
const TDX_BASE = process.env.TDX_BASE_URL || "http://127.0.0.1:19080";
const QE_EXPERIMENTS = ["qe_20260416_002701", "qe_20260413_084216", "qe_20260416_082012"];
const REPLAY_TRADE_DATE = process.env.PAPER_V2_E2E_TRADE_DATE || "2026-04-24";

type JsonObject = Record<string, any>;

type PackageSummary = {
  package_id: string;
  package_name: string;
  source_id?: string;
  package_status: string;
  manifest_sha256: string;
  metrics_summary?: JsonObject;
};

type SelectionRunSummary = {
  run_id: string;
  mode: string;
  trade_date: string;
  status: string;
  package_ids: string[];
  aggregate_results?: JsonObject[];
};

let ensuredPackages: PackageSummary[] = [];
let ensuredRuns: SelectionRunSummary[] = [];

async function apiFetch(request: APIRequestContext, path: string, init?: Parameters<APIRequestContext["fetch"]>[1]) {
  return request.fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers || {}),
    },
  });
}

async function apiJson(request: APIRequestContext, path: string, init?: Parameters<APIRequestContext["fetch"]>[1]) {
  const response = await apiFetch(request, path, init);
  const text = await response.text();
  const payload = text ? JSON.parse(text) : {};
  return { response, payload };
}

async function listPackages(request: APIRequestContext): Promise<PackageSummary[]> {
  const { response, payload } = await apiJson(request, "/strategy-packages?limit=500");
  expect(response.ok(), JSON.stringify(payload)).toBeTruthy();
  return payload.packages || [];
}

async function ensurePackageFromExperiment(request: APIRequestContext, experimentId: string): Promise<PackageSummary> {
  let rows = await listPackages(request);
  let found = rows.find((item) => item.source_id === experimentId || item.package_name === experimentId);
  if (!found) {
    const { response, payload } = await apiJson(request, "/strategy-packages/from-qe-experiment", {
      method: "POST",
      data: { experiment_id: experimentId, resolve_runtime_assets: false },
    });
    expect(response.ok(), `create package ${experimentId}: ${JSON.stringify(payload)}`).toBeTruthy();
    found = payload.package;
  }
  if (found.package_status === "BACKTEST_APPROVED") {
    const { response, payload } = await apiJson(request, `/strategy-packages/${found.package_id}/enable-selection`, { method: "POST" });
    expect(response.ok(), `enable selection ${experimentId}: ${JSON.stringify(payload)}`).toBeTruthy();
  }
  rows = await listPackages(request);
  const refreshed = rows.find((item) => item.package_id === found!.package_id);
  expect(refreshed, `package ${experimentId} should exist after setup`).toBeTruthy();
  return refreshed!;
}

function runtimeConfig(topK = 20): JsonObject {
  return {
    selection_artifact_config: { auto_generate: true, inference_backend: "wsl" },
    runtime_profile: {
      selection: { top_k: topK },
      tradability: { exclude_suspended: true },
      industry_blacklist: [],
      hmm: { enabled: false },
    },
  };
}

async function ensureSuccessfulSelectionRun(request: APIRequestContext, pkg: PackageSummary): Promise<SelectionRunSummary> {
  const { response, payload } = await apiJson(request, "/selection-center/runs", {
    method: "POST",
    data: {
      package_ids: [pkg.package_id],
      trade_date: REPLAY_TRADE_DATE,
      data_source: "DB_HISTORICAL",
      mode: "single_package",
      runtime_config: runtimeConfig(20),
    },
    timeout: 300_000,
  });
  expect(response.ok(), `selection run ${pkg.package_name}: ${JSON.stringify(payload).slice(0, 1200)}`).toBeTruthy();
  expect(payload.run.status).toBe("SUCCEEDED");
  expect(payload.run.aggregate_results?.length || 0).toBeGreaterThan(0);
  return payload.run;
}

async function openSection(page: Page, heading: string) {
  return page.locator("section").filter({ has: page.getByRole("heading", { name: heading }) });
}

function field(section: ReturnType<typeof openSection> extends Promise<infer T> ? T : never, label: string) {
  return section.locator(".pv2-field").filter({ hasText: label });
}

test.describe.serial("Paper Trading v2 UI real-backend validation", () => {
  test.beforeAll(async ({ request }) => {
    const health = await request.get(`${API_BASE.replace(/\/api\/v1$/, "")}/openapi.json`);
    expect(health.ok(), `temporary backend must be reachable at ${API_BASE}`).toBeTruthy();

    const defaults = await apiJson(request, "/paper-v2/trading-days/defaults?lookback_trading_days=10");
    expect(defaults.response.ok(), JSON.stringify(defaults.payload)).toBeTruthy();
    expect(defaults.payload.latest_trading_day).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(defaults.payload.replay_start_date).toMatch(/^\d{4}-\d{2}-\d{2}$/);

    ensuredPackages = [];
    ensuredRuns = [];
    for (const experimentId of QE_EXPERIMENTS) {
      const pkg = await ensurePackageFromExperiment(request, experimentId);
      ensuredPackages.push(pkg);
      ensuredRuns.push(await ensureSuccessfulSelectionRun(request, pkg));
    }
  });

  test("StrategyPackage page shows unpackaged QE sources, packaged strategies, and paper entry", async ({ page, request }) => {
    await page.goto("/paper-v2/packages");
    await expect(page.getByRole("heading", { name: "模拟盘 v2" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "从 QE 创建策略包" })).toBeVisible();
    await expect(page.getByText("只显示未打包来源")).toBeVisible();

    const sources = await apiJson(request, "/strategy-packages/qe-sources?source_kind=all&limit=20");
    expect(sources.response.ok(), JSON.stringify(sources.payload)).toBeTruthy();
    const sourceSection = await openSection(page, "从 QE 创建策略包");
    const sourceText = await sourceSection.locator("select").nth(1).textContent();
    for (const experimentId of QE_EXPERIMENTS) {
      expect(sourceText || "").not.toContain(experimentId);
    }
    if ((sources.payload.sources || []).length > 0) {
      await expect(sourceSection.locator("select").nth(1)).toContainText(/年化|IC|回撤/);
    }

    await expect(page.getByRole("heading", { name: "StrategyPackage 策略包中心" })).toBeVisible();
    for (const experimentId of QE_EXPERIMENTS) {
      await expect(page.getByText(experimentId).first()).toBeVisible();
    }
    await expect(page.locator("body")).toContainText("RankIC");
    await expect(page.locator("body")).toContainText("最大回撤");
    await expect(page.getByRole("link", { name: "从此包启动模拟盘" }).first()).toBeVisible();
  });

  test("Selection Center runs live-data inference, displays history, and imports watchlist items", async ({ page }) => {
    const target = ensuredPackages[0];
    await page.goto("/paper-v2/selection");
    await expect(page.getByRole("heading", { name: "选股控制" })).toBeVisible();

    const control = await openSection(page, "选股控制");
    await control.locator("select").first().selectOption("single_package");
    await control.locator('input[type="date"]').fill(REPLAY_TRADE_DATE);
    await control.locator('input[type="number"]').first().fill("20");

    const picker = await openSection(page, "策略包选择器");
    const checkboxes = picker.locator('tbody input[type="checkbox"]');
    for (let i = 0; i < await checkboxes.count(); i += 1) {
      const box = checkboxes.nth(i);
      if (await box.isChecked()) await box.uncheck();
    }
    const targetRow = picker.locator("tbody tr").filter({ hasText: target.package_name });
    await expect(targetRow).toBeVisible();
    await targetRow.locator('input[type="checkbox"]').check();

    await control.getByRole("button", { name: "运行选股" }).click();
    const results = await openSection(page, "选股结果");
    await expect(results.locator("tbody tr").first()).toBeVisible({ timeout: 90_000 });
    await expect(results).toContainText("选股参考价");
    await expect(results).toContainText(/live_qe_model_inference_v1|artifact_source|raw_rank/);

    await results.locator("input.pv2-input").first().fill(`PaperV2-E2E-${Date.now()}`);
    await results.getByRole("button", { name: "一键加入自选股票池" }).click();
    await expect(results.locator(".pv2-json")).toContainText("imported_symbols", { timeout: 30_000 });
    await expect(results.locator(".pv2-json")).toContainText(target.package_name);

    const history = await openSection(page, "历史选股记录与动态聚合");
    await expect(history).toContainText("点击记录可显示结果");
    await history.locator("tbody button").first().click();
    await expect(results.locator("tbody tr").first()).toBeVisible();
  });

  test("Multi-package historical run aggregation is clickable and produces aggregate selections", async ({ page }) => {
    await page.goto("/paper-v2/selection");
    const control = await openSection(page, "选股控制");
    await control.locator("select").first().selectOption("union");
    await expect(page.getByText("多策略包当前只用于统一选股研究")).toBeVisible();

    const history = await openSection(page, "历史选股记录与动态聚合");
    const rows = history.locator("tbody tr").filter({ hasText: "single_package" });
    await expect(rows.nth(0)).toBeVisible();
    await expect(rows.nth(1)).toBeVisible();
    await rows.nth(0).locator('input[type="checkbox"]').check();
    await rows.nth(1).locator('input[type="checkbox"]').check();
    await history.getByRole("button", { name: "聚合已选股票" }).click();

    const results = await openSection(page, "选股结果");
    await expect(results.locator("tbody tr").first()).toBeVisible({ timeout: 30_000 });
    await expect(results).toContainText(/union|source_run|raw_rank|artifact_source/);
  });

  test("Portfolio page exposes single-package paper setup and fails fast on unavailable V24/V25 assets", async ({ page }) => {
    const target = ensuredPackages[0];
    await page.goto(`/paper-v2/portfolios?package_id=${target.package_id}`);
    await expect(page.getByRole("heading", { name: "从单个策略包启动模拟盘" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "当前正在运行或已创建的模拟盘" })).toBeVisible();

    const createSection = await openSection(page, "从单个策略包启动模拟盘");
    await createSection.locator("select").first().selectOption(target.package_id);
    await field(createSection, "组合名称").locator("input").fill(`E2E-${Date.now()}`);
    await field(createSection, "初始资金").locator("input").fill("1000000");
    await field(createSection, "启动模式").locator("select").selectOption("replay");
    await expect(field(createSection, "数据源").locator("input")).toHaveValue("DB_HISTORICAL");
    await field(createSection, "回放开始日期").locator("input").fill("2026-04-20");
    await field(createSection, "回放结束日期").locator("input").fill(REPLAY_TRADE_DATE);
    await field(createSection, "TopK").locator("input").fill("20");
    await createSection.locator("button.pv2-button-primary").click();

    await expect(page.getByText("组合操作失败")).toBeVisible({ timeout: 30_000 });
    await expect(page.locator("body")).toContainText(/V24_PLAN|DATA_UNAVAILABLE|EXECUTION_ALGO_ERROR|model_path/);
    await expect(page.locator("body")).not.toContainText("created_portfolio_id");
  });

  test("Negative APIs return structured errors and TDX realtime minute endpoint is reachable", async ({ request }) => {
    const missing = await apiJson(request, "/strategy-packages/from-qe-experiment/not_exists_for_failfast/manifest");
    expect(missing.response.status()).toBe(404);
    expect(missing.payload.detail.error_code).toBe("DATA_UNAVAILABLE");

    const badSelection = await apiJson(request, "/selection-center/runs", {
      method: "POST",
      data: {
        package_ids: [ensuredPackages[0].package_id],
        trade_date: "2026-04-26",
        data_source: "DB_HISTORICAL",
        mode: "single_package",
        runtime_config: runtimeConfig(20),
      },
    });
    expect(badSelection.response.ok()).toBeFalsy();
    expect(JSON.stringify(badSelection.payload)).toMatch(/not a trading day|DATA_UNAVAILABLE|trade_date/);

    const tdx = await request.get(`${TDX_BASE}/api/kline-all/tdx?code=SZ000001&type=minute1`, { timeout: 30_000 });
    expect(tdx.ok(), `TDX backend must respond at ${TDX_BASE}`).toBeTruthy();
    const payload = await tdx.json();
    expect(payload.code).toBe(0);
    const bars = payload.data?.list || [];
    expect(Array.isArray(bars)).toBeTruthy();
    expect(bars.length, "TDX minute endpoint should return real minute bars instead of a fake default").toBeGreaterThan(0);
  });
});
