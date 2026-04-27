import { expect, test, type APIRequestContext, type Page } from "@playwright/test";

const API_BASE = process.env.PAPER_V2_API_BASE || process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8011/api/v1";
const TDX_BASE = process.env.TDX_BASE_URL || "http://127.0.0.1:19080";
const QE_EXPERIMENTS = ["qe_20260416_002701", "qe_20260413_084216", "qe_20260416_082012"];
const REPLAY_TRADE_DATE = process.env.PAPER_V2_E2E_TRADE_DATE || "2026-04-24";
const ACTIVATION_TRADE_DATE = process.env.PAPER_V2_E2E_ACTIVATION_DATE || "2026-04-28";

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

type ExecutionPolicySummary = {
  policy_id: string;
  algo_code?: string;
  paper_enabled?: boolean;
  policy_name?: string;
};

type PaperPortfolioSummary = {
  portfolio_id: string;
  portfolio_name: string;
  package_id: string;
  manifest_sha256: string;
  status: string;
};

type HmmRuntimeChoice = {
  config_id: string;
  snapshot_id: string;
};

let ensuredPackages: PackageSummary[] = [];
let ensuredRuns: SelectionRunSummary[] = [];
let replayPortfolioId = "";

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
  if (!found) {
    throw new Error(`StrategyPackage setup failed for ${experimentId}`);
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

async function requireV25PaperPolicy(request: APIRequestContext, pkg: PackageSummary): Promise<ExecutionPolicySummary> {
  const { response, payload } = await apiJson(request, `/strategy-packages/${pkg.package_id}/execution-policies`);
  expect(response.ok(), `load execution policies for ${pkg.package_name}: ${JSON.stringify(payload)}`).toBeTruthy();
  const policies: ExecutionPolicySummary[] = payload.execution_policies || [];
  const policy = policies.find((item) => item.algo_code === "V25_TWO_STAGE" && item.paper_enabled);
  expect(policy, `${pkg.package_name} must already have a paper-enabled V25_TWO_STAGE policy; test must not mutate strategy assets`).toBeTruthy();
  return policy!;
}

async function requireHmmRuntimeChoice(request: APIRequestContext): Promise<HmmRuntimeChoice> {
  const { response, payload } = await apiJson(request, "/hmm-training/configs");
  expect(response.ok(), `load HMM configs: ${JSON.stringify(payload)}`).toBeTruthy();
  const configs: JsonObject[] = Array.isArray(payload) ? payload : (payload.configs || []);
  const preferred = configs.find((item) => String(item.display_name || "").includes("w5_zscore")) || configs[0];
  expect(preferred?.config_id, "HMM config must exist for UI runtime selection").toBeTruthy();
  const snapshots = await apiJson(request, `/hmm-training/configs/${preferred.config_id}/snapshots`);
  expect(snapshots.response.ok(), `load HMM snapshots: ${JSON.stringify(snapshots.payload)}`).toBeTruthy();
  const rows: JsonObject[] = Array.isArray(snapshots.payload) ? snapshots.payload : (snapshots.payload.snapshots || []);
  const ready = rows.find((item) => ["completed", "ready", "success", "succeeded"].includes(String(item.status || "").toLowerCase()));
  expect(ready?.snapshot_id, "HMM completed snapshot must exist for UI runtime selection").toBeTruthy();
  return { config_id: String(preferred.config_id), snapshot_id: String(ready!.snapshot_id) };
}

async function createPaperPortfolioOnly(
  request: APIRequestContext,
  pkg: PackageSummary,
  policy: ExecutionPolicySummary,
  portfolioName: string,
): Promise<PaperPortfolioSummary> {
  const { response, payload } = await apiJson(request, "/paper-v2/portfolios", {
    method: "POST",
    data: {
      package_id: pkg.package_id,
      portfolio_name: portfolioName,
      initial_cash: 1000000,
      start_date: REPLAY_TRADE_DATE,
      data_source: "DB_HISTORICAL",
      execution_policy: { validated_execution_policy_id: policy.policy_id },
    },
  });
  expect(response.ok(), `create UI-console test portfolio: ${JSON.stringify(payload)}`).toBeTruthy();
  return payload.portfolio;
}

async function openSection(page: Page, heading: string) {
  return page.locator("section").filter({ has: page.getByRole("heading", { name: heading }) });
}

function field(section: ReturnType<typeof openSection> extends Promise<infer T> ? T : never, label: string) {
  return section.locator(".pv2-field").filter({ hasText: label });
}

async function chooseSelectionPackages(page: Page, packageIds: string[]) {
  for (const pkg of ensuredPackages) {
    const checkbox = page.getByTestId(`selection-package-${pkg.package_id}`);
    if (await checkbox.count()) {
      if (await checkbox.isChecked()) await checkbox.uncheck();
    }
  }
  for (const packageId of packageIds) {
    await page.getByTestId(`selection-package-${packageId}`).check();
  }
}

function consoleRuntimeText(topK = 20): string {
  return JSON.stringify(
    {
      paper_v2_session: { signal_data_source: "DB_HISTORICAL" },
      ...runtimeConfig(topK),
    },
    null,
    2,
  );
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

  test("Selection Center validates weighted fusion, HMM, blacklist backfill, and TopK guard through UI", async ({ page, request }) => {
    const hmm = await requireHmmRuntimeChoice(request);
    const [first, second, third] = ensuredPackages;
    await page.goto("/paper-v2/selection");

    await page.getByTestId("selection-mode").selectOption("weighted_fusion");
    await page.getByTestId("selection-trade-date").fill(REPLAY_TRADE_DATE);
    await page.getByTestId("selection-top-k").fill("50");
    await chooseSelectionPackages(page, [first.package_id, second.package_id]);
    await page.getByTestId(`selection-weight-${first.package_id}`).fill("2");
    await page.getByTestId(`selection-weight-${second.package_id}`).fill("1");
    await page.getByTestId("selection-run").click();

    const results = await openSection(page, "选股结果");
    await expect(results.locator("tbody tr").first()).toBeVisible({ timeout: 90_000 });
    await expect(results).toContainText("weighted_fusion_aggregate");
    await expect(results).toContainText("package_weights");

    await page.getByTestId("selection-mode").selectOption("intersection");
    await chooseSelectionPackages(page, [second.package_id, third.package_id]);
    await page.getByTestId("selection-run").click();
    await expect(results.locator("tbody tr").first()).toBeVisible({ timeout: 90_000 });
    await expect(results).toContainText("intersection_aggregate");

    await page.getByTestId("selection-mode").selectOption("single_package");
    await chooseSelectionPackages(page, [first.package_id]);
    await page.getByTestId("selection-top-k").fill("20");
    await page.getByTestId("selection-industry-blacklist").fill("计算机");
    await page.getByTestId("selection-hmm-enabled").check();
    await page.getByTestId("selection-hmm-config").selectOption(hmm.config_id);
    await expect(page.locator(`[data-testid="selection-hmm-snapshot"] option[value="${hmm.snapshot_id}"]`)).toHaveCount(1, { timeout: 30_000 });
    await page.getByTestId("selection-hmm-snapshot").selectOption(hmm.snapshot_id);
    await page.getByTestId("selection-hmm-preset").selectOption("preset_A");
    await page.getByTestId("selection-run").click();
    await expect(results.locator("tbody tr").first()).toBeVisible({ timeout: 90_000 });
    await expect(results).toContainText("hmm");
    const excluded = await openSection(page, "剔除与补位追踪");
    await expect(excluded).toContainText("industry_blacklisted");

    await page.getByTestId("selection-top-k").fill("51");
    await page.getByTestId("selection-run").click();
    await expect(page.locator(".pv2-error-panel")).toContainText("TopK");
  });

  test("Portfolio page creates a V25 replay portfolio and exposes ledger/performance", async ({ page, request }) => {
    const target = ensuredPackages[0];
    const policy = await requireV25PaperPolicy(request, target);
    const portfolioName = `E2E-V25-${Date.now()}`;

    await page.goto(`/paper-v2/portfolios?package_id=${target.package_id}`);
    await expect(page.locator("body")).toContainText("V25_TWO_STAGE");

    const createSection = page.locator("section.pv2-card").first();
    await createSection.locator("select").nth(0).selectOption(target.package_id);
    await createSection.locator("input.pv2-input").nth(0).fill(portfolioName);
    await createSection.locator("input.pv2-input").nth(1).fill("1000000");
    await createSection.locator("select").nth(1).selectOption("replay");
    await expect(createSection.locator("input.pv2-input").nth(2)).toHaveValue("DB_HISTORICAL");
    await createSection.locator("select").nth(2).selectOption(policy.policy_id);
    await expect(createSection.locator("select").nth(2)).toContainText("V25_TWO_STAGE");
    await createSection.locator("input.pv2-input").nth(3).fill(REPLAY_TRADE_DATE);
    await createSection.locator("input.pv2-input").nth(4).fill(REPLAY_TRADE_DATE);
    await createSection.locator('input[type="number"]').nth(1).fill("20");
    await createSection.locator("button.pv2-button-primary").click();

    const createdJson = page.locator("pre.pv2-json").filter({ hasText: "created_portfolio_id" }).last();
    await expect(createdJson).toBeVisible({ timeout: 180_000 });
    await expect(createdJson).toContainText("SUCCEEDED", { timeout: 30_000 });
    const createdPayload = JSON.parse((await createdJson.textContent()) || "{}");
    const portfolioId = String(createdPayload.created_portfolio_id || "");
    expect(portfolioId).toMatch(/^paper_/);
    replayPortfolioId = portfolioId;
    expect(createdPayload.session_progress?.session?.status).toBe("SUCCEEDED");

    const [runs, orders, fills, positions, snapshots, errors, performance] = await Promise.all([
      apiJson(request, `/paper-v2/portfolios/${portfolioId}/runs?limit=100`),
      apiJson(request, `/paper-v2/portfolios/${portfolioId}/orders?limit=1000`),
      apiJson(request, `/paper-v2/portfolios/${portfolioId}/fills?limit=1000`),
      apiJson(request, `/paper-v2/portfolios/${portfolioId}/positions?limit=1000`),
      apiJson(request, `/paper-v2/portfolios/${portfolioId}/daily-snapshots?limit=1000`),
      apiJson(request, `/paper-v2/portfolios/${portfolioId}/errors?limit=1000`),
      apiJson(request, `/paper-v2/portfolios/${portfolioId}/performance-report`),
    ]);
    expect(runs.payload.runs?.[0]?.status).toBe("SUCCEEDED");
    expect(orders.payload.orders?.length || 0).toBeGreaterThan(0);
    expect(fills.payload.fills?.length || 0).toBeGreaterThan(0);
    expect(positions.payload.positions?.length || 0).toBeGreaterThan(0);
    expect(snapshots.payload.daily_snapshots?.length || 0).toBeGreaterThan(0);
    expect(errors.payload.errors?.length || 0).toBe(0);
    expect(performance.payload.performance_report?.snapshot_count).toBeGreaterThan(0);

    await page.goto(`/paper-v2/portfolios/${portfolioId}/ledger`);
    await expect(page.locator("table").nth(0).locator("tbody tr").first()).toBeVisible();
    await expect(page.locator("table").nth(1).locator("tbody tr").first()).toBeVisible();
    await expect(page.locator("table").nth(3).locator("tbody tr").first()).toBeVisible();
    await expect(page.locator("table").nth(4).locator("tbody tr").first()).toBeVisible();

    await page.goto(`/paper-v2/portfolios/${portfolioId}/performance`);
    await expect(page.locator("pre.pv2-json")).toContainText("snapshot_count");
    await expect(page.locator("table").nth(0).locator("tbody tr").first()).toBeVisible();
  });

  test("Overview, settings, and portfolio detail lifecycle are usable without market-time streams", async ({ page, request }) => {
    expect(replayPortfolioId, "previous replay portfolio must be available for UI detail validation").toMatch(/^paper_/);

    await page.goto("/paper-v2");
    await expect(page.locator('a[href="/paper-v2/selection"]').first()).toBeVisible();
    await expect(page.locator('a[href="/paper-v2/portfolios"]').first()).toBeVisible();
    await expect(page.locator(`a[href="/paper-v2/portfolios/${replayPortfolioId}"]`).first()).toBeVisible({ timeout: 60_000 });

    await page.goto("/paper-v2/settings");
    await expect(page.locator('a[href="/paper-v2/packages"]').first()).toBeVisible();
    await expect(page.locator('a[href="/paper-v2/selection"]').first()).toBeVisible();
    await expect(page.locator('a[href="/paper-v2/portfolios"]').first()).toBeVisible();

    await page.goto(`/paper-v2/portfolios/${replayPortfolioId}`);
    await expect(page.locator(`a[href="/paper-v2/portfolios/${replayPortfolioId}/run-console"]`).first()).toBeVisible();
    await expect(page.locator(`a[href="/paper-v2/portfolios/${replayPortfolioId}/ledger"]`).first()).toBeVisible();
    await expect(page.locator(`a[href="/paper-v2/portfolios/${replayPortfolioId}/performance"]`).first()).toBeVisible();
    await expect(page.getByTestId("portfolio-lifecycle-complete")).toBeVisible();
    await expect(page.getByTestId("portfolio-lifecycle-retire")).toBeVisible();

    await page.getByTestId("portfolio-lifecycle-pause-resume").click();
    await expect.poll(async () => {
      const { payload } = await apiJson(request, `/paper-v2/portfolios/${replayPortfolioId}`);
      return payload.portfolio?.status;
    }, { timeout: 30_000 }).toBe("PAUSED");
    await expect(page.locator("body")).toContainText("PAUSED");

    await page.getByTestId("portfolio-lifecycle-pause-resume").click();
    await expect.poll(async () => {
      const { payload } = await apiJson(request, `/paper-v2/portfolios/${replayPortfolioId}`);
      return payload.portfolio?.status;
    }, { timeout: 30_000 }).toBe("READY");
    await expect(page.locator("body")).toContainText("READY");
  });

  test("Run console validates readiness, policy/runtime audit, replay reject/reset, and live waiting controls", async ({ page, request }) => {
    const target = ensuredPackages[0];
    const policy = await requireV25PaperPolicy(request, target);
    const portfolio = await createPaperPortfolioOnly(request, target, policy, `E2E-Console-${Date.now()}`);
    const consolePath = `/paper-v2/portfolios/${portfolio.portfolio_id}/run-console`;

    await page.goto(consolePath);
    await page.getByTestId("console-runtime-json").fill(consoleRuntimeText(20));
    await page.getByTestId("console-trade-date").fill(REPLAY_TRADE_DATE);
    await page.getByTestId("console-readiness").click();
    const readinessJson = page.locator("pre.pv2-json").filter({ hasText: "order_intent_count" }).last();
    await expect(readinessJson).toContainText("passed", { timeout: 120_000 });
    await expect(page.getByTestId("console-run-day")).toBeEnabled();
    await page.getByTestId("console-run-day").click();
    const runJson = page.locator("pre.pv2-json").filter({ hasText: "run_id" }).last();
    await expect(runJson).toContainText("SUCCEEDED", { timeout: 180_000 });

    await page.getByTestId("console-runtime-json").fill(consoleRuntimeText(21));
    await page.getByTestId("console-runtime-profile-name").fill(`E2E Runtime ${Date.now()}`);
    await page.getByTestId("console-runtime-reason").fill("E2E runtime profile audit validation");
    await page.getByTestId("console-runtime-save-profile").click();
    await expect(page.getByTestId("console-runtime-version-select")).not.toHaveValue("", { timeout: 30_000 });
    await page.getByTestId("console-runtime-activation-date").fill(ACTIVATION_TRADE_DATE);
    const replaceRuntime = page.getByTestId("console-runtime-replace");
    if (!(await replaceRuntime.isChecked())) await replaceRuntime.check();
    await page.getByTestId("console-runtime-activate").click();
    await expect.poll(async () => {
      const { payload } = await apiJson(request, `/paper-v2/portfolios/${portfolio.portfolio_id}/runtime-config-activations`);
      return Boolean((payload.activations || []).find((item: JsonObject) => item.trade_date === ACTIVATION_TRADE_DATE && item.status === "ACTIVE"));
    }, { timeout: 30_000 }).toBeTruthy();
    await expect(page.locator("pre.pv2-json").filter({ hasText: "config_change_audit" })).toContainText("runtime_profile");

    await page.getByTestId("console-policy-date").fill(ACTIVATION_TRADE_DATE);
    const uiPolicyId = await page.getByTestId("console-policy-select").locator("option").nth(1).getAttribute("value");
    expect(uiPolicyId, "run-console must expose a portfolio execution policy option returned by the backend").toBeTruthy();
    await page.getByTestId("console-policy-select").selectOption(uiPolicyId!);
    const replacePolicy = page.getByTestId("console-policy-replace");
    if (!(await replacePolicy.isChecked())) await replacePolicy.check();
    await page.getByTestId("console-policy-reason").fill("E2E execution policy activation validation");
    await page.getByTestId("console-policy-activate").click();
    await expect.poll(async () => {
      const { payload } = await apiJson(request, `/paper-v2/portfolios/${portfolio.portfolio_id}/execution-policy-activations`);
      return Boolean((payload.activations || []).find((item: JsonObject) => item.trade_date === ACTIVATION_TRADE_DATE && item.status === "ACTIVE"));
    }, { timeout: 30_000 }).toBeTruthy();
    await expect(page.locator("body")).toContainText("V25_TWO_STAGE");

    await page.getByTestId("console-replay-start").fill(REPLAY_TRADE_DATE);
    await page.getByTestId("console-replay-end").fill(REPLAY_TRADE_DATE);
    await page.getByTestId("console-replay-reject").click();
    await expect(page.locator(".pv2-error-panel")).toContainText(/reject_existing|already|已有|DUPLICATE/i, { timeout: 60_000 });

    await page.getByTestId("console-replay-reset").click();
    await page.getByTestId("console-replay-reset-input").fill(portfolio.portfolio_id);
    await page.getByTestId("console-replay-reset-confirm").click();
    const resetJson = page.locator("pre.pv2-json").filter({ hasText: "session" }).last();
    await expect(resetJson).toContainText("REPLAY_ONLY", { timeout: 180_000 });
    await expect(resetJson).toContainText("SUCCEEDED");

    await page.getByTestId("console-live-start").fill(ACTIVATION_TRADE_DATE);
    const liveButton = page.getByTestId("console-live-create");
    if (await liveButton.isEnabled()) {
      await liveButton.click();
      await expect(page.locator("pre.pv2-json").last()).toContainText(/LIVE_|FAILED|WAITING|SESSION|ALGO/, { timeout: 90_000 });
    } else {
      await expect(liveButton).toBeDisabled();
      await expect(page.locator("body")).toContainText(/实时模拟|TDX_REALTIME|TDX 实时/);
    }
  });

  test("Model and HMM maintenance previews are available without triggering training jobs", async ({ page, request }) => {
    const target = ensuredPackages[0];
    const hmm = await requireHmmRuntimeChoice(request);
    await page.goto("/paper-v2/model-hmm");

    await page.getByTestId("model-package").selectOption(target.package_id);
    await page.getByTestId("model-as-of-date").fill(REPLAY_TRADE_DATE);
    await page.getByTestId("model-lookback-days").fill("756");
    await page.getByTestId("model-retrain-preview").click();
    await expect(page.locator("pre.pv2-json").filter({ hasText: "requires_manual_confirmation" })).toContainText("recommended_train_end_date", { timeout: 60_000 });

    await page.getByTestId("hmm-config").selectOption(hmm.config_id);
    await page.getByTestId("hmm-as-of-date").fill(REPLAY_TRADE_DATE);
    await page.getByTestId("hmm-validation-months").fill("3");
    await page.getByTestId("hmm-train-years").fill("3");
    await page.getByTestId("hmm-rolling-preview").click();
    await expect(page.locator("pre.pv2-json").filter({ hasText: "latest_completed_trade_date" })).toContainText(REPLAY_TRADE_DATE, { timeout: 60_000 });
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
