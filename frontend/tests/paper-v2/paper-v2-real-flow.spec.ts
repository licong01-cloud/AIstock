import { expect, test, type APIRequestContext, type Page } from "@playwright/test";

const API_BASE = process.env.PAPER_V2_API_BASE || process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8011/api/v1";
const TDX_BASE = process.env.TDX_BASE_URL || "http://127.0.0.1:19080";
const LEGACY_QE_EXPERIMENTS = ["qe_20260416_002701", "qe_20260413_084216", "qe_20260416_082012"];
const PREFERRED_PACKAGE_MARKERS = (
  process.env.PAPER_V2_E2E_PACKAGE_MARKERS
  || "qe_20260601_172505_fe17,qe_20260520_215627_abbc"
)
  .split(",")
  .map((item) => item.trim())
  .filter(Boolean);
let replayTradeDate = process.env.PAPER_V2_E2E_TRADE_DATE || "";
const ACTIVATION_TRADE_DATE = process.env.PAPER_V2_E2E_ACTIVATION_DATE || "2026-04-28";
const HMM_UNCOVERED_TRADE_DATE = process.env.PAPER_V2_E2E_HMM_UNCOVERED_DATE || "2026-04-29";
const E2E_BLACKLIST_L1_CODE = "801030.SI";
const E2E_BLACKLIST_L2_CODE = "801036.SI";
const SKIP_REALTIME =
  process.env.PAPER_V2_E2E_SKIP_REALTIME === "1" || process.env.PAPER_V2_SKIP_REALTIME === "1";

type JsonObject = Record<string, any>;

type PackageSummary = {
  package_id: string;
  package_name: string;
  source_type?: string;
  source_id?: string;
  package_status: string;
  manifest_sha256: string;
  metrics_summary?: JsonObject;
  selection_health?: JsonObject;
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

type WatchlistImportPayload = {
  ok: boolean;
  run_id: string;
  category_id: number;
  entry_source: string;
  entry_as_of: string;
  requested_top_k: number;
  imported_symbols: string[];
};

type HmmRuntimeChoice = {
  config_id: string;
  snapshot_id: string;
  coefficient_path: string;
  trade_date: string;
};

let ensuredPackages: PackageSummary[] = [];
let ensuredRuns: SelectionRunSummary[] = [];
let runnableSelectionPackageIds = new Set<string>();
let replayPortfolioId = "";
let replayCutoffDate = "2026-04-23";
let paperPortfolioRuntimeBlocked = "";

function paperPortfolioBlockText(payloadOrText: unknown): string {
  const text = typeof payloadOrText === "string" ? payloadOrText : JSON.stringify(payloadOrText || {});
  if (
    /DATA_UNAVAILABLE|INVALID_STATE_TRANSITION|V24_PLAN|EXECUTION_ALGO_ERROR|V25_TWO_STAGE|V25_1_SMALL_CAP|early_model_path|late_model_path|model_path|not accessible|execution policy must match|runtime is not available|validated execution policy/i.test(text)
  ) {
    return text.slice(0, 1200);
  }
  return "";
}

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

async function listSelectablePackages(request: APIRequestContext): Promise<PackageSummary[]> {
  const { response, payload } = await apiJson(request, "/selection-center/selectable-packages?limit=500");
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

function packagePreference(pkg: PackageSummary): number {
  const haystack = `${pkg.package_name || ""} ${pkg.source_id || ""}`.toLowerCase();
  const idx = PREFERRED_PACKAGE_MARKERS.findIndex((marker) => haystack.includes(marker.toLowerCase()));
  return idx >= 0 ? idx : PREFERRED_PACKAGE_MARKERS.length + 1;
}

async function discoverValidationPackages(request: APIRequestContext): Promise<PackageSummary[]> {
  const selectable = await listSelectablePackages(request);
  const leakedRetiredPackageIds = selectable
    .filter((pkg) => String(pkg.package_status || "").toUpperCase() === "RETIRED")
    .map((pkg) => pkg.package_id);
  expect(
    leakedRetiredPackageIds,
    "Selection Center selectable-packages must not expose RETIRED StrategyPackages",
  ).toEqual([]);
  const runnable = selectable
    .filter((pkg) => pkg.selection_health?.runnable === true)
    .sort((a, b) => packagePreference(a) - packagePreference(b));
  if (runnable.length >= 2) {
    return runnable.slice(0, Math.min(3, runnable.length));
  }

  const fallbackErrors: string[] = [];
  const fallbackPackages: PackageSummary[] = [...runnable];
  for (const experimentId of LEGACY_QE_EXPERIMENTS) {
    try {
      const pkg = await ensurePackageFromExperiment(request, experimentId);
      if (!fallbackPackages.some((item) => item.package_id === pkg.package_id)) {
        fallbackPackages.push(pkg);
      }
    } catch (err) {
      fallbackErrors.push(`${experimentId}: ${err instanceof Error ? err.message : String(err)}`.slice(0, 500));
    }
    if (fallbackPackages.length >= 2) break;
  }
  if (fallbackPackages.length < 2) {
    throw new Error(`Paper v2 UI E2E requires at least two runnable packages; fallback errors=${fallbackErrors.join(" | ")}`);
  }
  return fallbackPackages;
}

function runtimeConfig(topK = 20): JsonObject {
  return {
    top_k: topK,
    selection_artifact_config: {
      auto_generate: true,
      inference_backend: "wsl",
      pit_mode: "PREVIOUS_TRADING_DAY_CLOSE",
      cutoff_date: replayCutoffDate,
    },
    runtime_profile: {
      selection: { top_k: topK },
      tradability: { exclude_suspended: true },
      industry_blacklist: [],
      hmm: { enabled: false },
    },
  };
}

async function resolveReplayCutoff(request: APIRequestContext): Promise<string> {
  const { response, payload } = await apiJson(
    request,
    `/selection-center/pit-cutoff?trade_date=${replayTradeDate}&pit_mode=PREVIOUS_TRADING_DAY_CLOSE`,
  );
  expect(response.ok(), `resolve PIT cutoff: ${JSON.stringify(payload)}`).toBeTruthy();
  expect(payload.point_in_time_context?.cutoff_date, "PIT cutoff date must be resolved by backend calendar").toMatch(/^\d{4}-\d{2}-\d{2}$/);
  return payload.point_in_time_context.cutoff_date;
}

async function ensureSuccessfulSelectionRun(request: APIRequestContext, pkg: PackageSummary): Promise<SelectionRunSummary> {
  const { response, payload } = await apiJson(request, "/selection-center/runs", {
    method: "POST",
    data: {
      package_ids: [pkg.package_id],
      trade_date: replayTradeDate,
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

async function loadExecutionPolicyOrDefault(request: APIRequestContext, pkg: PackageSummary): Promise<ExecutionPolicySummary | null> {
  const { response, payload } = await apiJson(request, `/strategy-packages/${pkg.package_id}/execution-policies`);
  expect(response.ok(), `load execution policies for ${pkg.package_name}: ${JSON.stringify(payload)}`).toBeTruthy();
  const policies: ExecutionPolicySummary[] = payload.execution_policies || [];
  return policies.find((item) => item.paper_enabled)
    || policies.find((item) => ["V25_TWO_STAGE", "V25_1_SMALL_CAP"].includes(String(item.algo_code || "").toUpperCase()))
    || policies[0]
    || null;
}

async function requireHmmRuntimeChoice(request: APIRequestContext): Promise<HmmRuntimeChoice> {
  const { response, payload } = await apiJson(request, "/hmm-training/configs");
  expect(response.ok(), `load HMM configs: ${JSON.stringify(payload)}`).toBeTruthy();
  const configs: JsonObject[] = Array.isArray(payload) ? payload : (payload.configs || []);
  expect(configs.length, "HMM config must exist for UI runtime selection").toBeGreaterThan(0);
  const ordered = [
    ...configs.filter((item) => String(item.display_name || "").includes("w5_zscore_candidate")),
    ...configs.filter((item) => !String(item.display_name || "").includes("w5_zscore_candidate")),
  ];
  for (const config of ordered) {
    const snapshots = await apiJson(request, `/hmm-training/configs/${config.config_id}/snapshots`);
    expect(snapshots.response.ok(), `load HMM snapshots: ${JSON.stringify(snapshots.payload)}`).toBeTruthy();
    const rows: JsonObject[] = Array.isArray(snapshots.payload) ? snapshots.payload : (snapshots.payload.snapshots || []);
    const ready = rows.find((item) => {
      const artifacts: JsonObject[] = Array.isArray(item.coefficient_artifacts) ? item.coefficient_artifacts : [];
      return ["completed", "ready", "success", "succeeded"].includes(String(item.status || "").toLowerCase())
        && artifacts.some((artifact) => artifact.preset === "preset_A" && Array.isArray(artifact.covered_trade_dates) && artifact.covered_trade_dates.length > 0);
    });
    if (!ready) continue;
    const artifact = (Array.isArray(ready.coefficient_artifacts) ? ready.coefficient_artifacts : []).find((item: JsonObject) => item.preset === "preset_A" && Array.isArray(item.covered_trade_dates) && item.covered_trade_dates.length > 0);
    if (artifact?.path) {
      const covered = artifact.covered_trade_dates as string[];
      return {
        config_id: String(config.config_id),
        snapshot_id: String(ready.snapshot_id),
        coefficient_path: String(artifact.path),
        trade_date: String(covered[covered.length - 1]),
      };
    }
  }
  throw new Error("HMM completed snapshot with preset_A coefficients must exist for UI runtime selection");
}


async function requireHmmAutomaticRuntimeConfig(request: APIRequestContext): Promise<{ config_id: string; trade_date: string }> {
  const { response, payload } = await apiJson(request, "/hmm-training/configs");
  expect(response.ok(), `load HMM configs: ${JSON.stringify(payload)}`).toBeTruthy();
  const configs: JsonObject[] = Array.isArray(payload) ? payload : (payload.configs || []);
  expect(configs.length, "HMM config must exist for automatic UI runtime selection").toBeGreaterThan(0);
  const ordered = [
    ...configs.filter((item) => String(item.display_name || "").includes("w5_zscore_candidate")),
    ...configs.filter((item) => !String(item.display_name || "").includes("w5_zscore_candidate")),
  ];
  return { config_id: String(ordered[0].config_id), trade_date: replayTradeDate };
}

async function createPaperPortfolioOnly(
  request: APIRequestContext,
  pkg: PackageSummary,
  policy: ExecutionPolicySummary | null,
  portfolioName: string,
): Promise<PaperPortfolioSummary | null> {
  const { response, payload } = await apiJson(request, "/paper-v2/portfolios", {
    method: "POST",
    data: {
      package_id: pkg.package_id,
      portfolio_name: portfolioName,
      initial_cash: 1000000,
      start_date: replayTradeDate,
      data_source: "DB_HISTORICAL",
      ...(policy ? { execution_policy: { validated_execution_policy_id: policy.policy_id } } : {}),
    },
  });
  if (!response.ok()) {
    paperPortfolioRuntimeBlocked = paperPortfolioBlockText(payload);
    if (paperPortfolioRuntimeBlocked) return null;
  }
  expect(response.ok(), `create UI-console test portfolio: ${JSON.stringify(payload)}`).toBeTruthy();
  return payload.portfolio;
}

async function waitForPortfolioByName(request: APIRequestContext, portfolioName: string, timeoutMs = 120_000): Promise<PaperPortfolioSummary> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const { response, payload } = await apiJson(
      request,
      `/paper-v2/portfolios?page=1&page_size=5&search=${encodeURIComponent(portfolioName)}`,
    );
    expect(response.ok(), `portfolio search ${portfolioName}: ${JSON.stringify(payload).slice(0, 1200)}`).toBeTruthy();
    const rows: PaperPortfolioSummary[] = payload.portfolios || [];
    const found = rows.find((item) => item.portfolio_name === portfolioName);
    if (found) return found;
    await new Promise((resolve) => setTimeout(resolve, 2_000));
  }
  throw new Error(`created portfolio ${portfolioName} was not visible through API within ${timeoutMs}ms`);
}

async function waitForReplayRunOutcome(request: APIRequestContext, portfolioId: string, timeoutMs = 700_000): Promise<JsonObject> {
  const deadline = Date.now() + timeoutMs;
  let latest: JsonObject | null = null;
  while (Date.now() < deadline) {
    const { response, payload } = await apiJson(request, `/paper-v2/portfolios/${portfolioId}/runs?limit=5`);
    expect(response.ok(), `portfolio runs ${portfolioId}: ${JSON.stringify(payload).slice(0, 1200)}`).toBeTruthy();
    const runs: JsonObject[] = payload.runs || [];
    latest = runs[0] || null;
    const status = String(latest?.status || "");
    if (["SUCCEEDED", "FAILED", "CANCELLED"].includes(status)) return latest!;
    await new Promise((resolve) => setTimeout(resolve, 2_000));
  }
  throw new Error(`portfolio ${portfolioId} replay run did not settle; latest=${JSON.stringify(latest).slice(0, 1200)}`);
}

async function openSection(page: Page, heading: string) {
  return page.locator("section").filter({ has: page.getByRole("heading", { name: heading }) });
}

function field(section: ReturnType<typeof openSection> extends Promise<infer T> ? T : never, label: string) {
  return section.locator(".pv2-field").filter({ hasText: label });
}

async function chooseSelectionPackages(page: Page, packageIds: string[]) {
  expect(packageIds.length, "Selection UI now accepts exactly one StrategyPackage").toBe(1);
  const packageId = packageIds[0];
  const radio = page.getByTestId(`selection-package-${packageId}`);
  if (await radio.count() === 0) {
    const refresh = page.getByRole("button", { name: /刷新策略包/ });
    if (await refresh.isVisible({ timeout: 2_000 }).catch(() => false)) {
      await refresh.click();
    }
  }
  await expect(radio, `selection package ${packageId} must be visible after selector refresh`).toBeVisible({ timeout: 60_000 });
  await expect(radio, `selection package ${packageId} must be enabled by health gate`).toBeEnabled();
  await expect(radio, `selection package ${packageId} must be a single-select radio`).toHaveAttribute("type", "radio");
  await radio.check();
  await expect(radio).toBeChecked();
}

async function chooseIndustryBlacklist(page: Page, l1Code: string, l2Code: string) {
  const selector = page.getByTestId("paper-industry-blacklist-selector");
  await expect(selector, "industry blacklist selector must be visible in Selection Center controls").toBeVisible({ timeout: 60_000 });
  const group = page.getByTestId(`paper-industry-group-${l1Code}`);
  await expect(group, `industry group ${l1Code} must be loaded from backend tree`).toBeVisible({ timeout: 60_000 });
  await group.click();
  const addButton = page.getByTestId(`paper-industry-add-${l2Code}`);
  await expect(addButton, `industry child ${l2Code} must be visible after expanding ${l1Code}`).toBeVisible({ timeout: 30_000 });
  await addButton.click();
  await expect(page.getByTestId(`paper-industry-selected-${l2Code}`), `industry blacklist ${l2Code} must be selected in runtime profile UI`).toBeVisible();
}

async function assertSelectionHealthGateBlocksLegacy(page: Page, packageId: string) {
  const checkbox = page.getByTestId(`selection-package-${packageId}`);
  await expect(checkbox, `legacy package ${packageId} should still be visible`).toBeVisible({ timeout: 30_000 });
  await expect(checkbox, `legacy package ${packageId} must be disabled before operator run`).toBeDisabled();
  await expect(page.locator("body")).toContainText(/LEGACY_NON_ST_PIT|BLOCKED|旧版非 ST PIT|健康预检阻断/);
}

type SelectionRunClickResult = {
  ok: boolean;
  status: number;
  payload: JsonObject;
  text: string;
};

async function clickSelectionRunAndAcceptPitResult(page: Page): Promise<SelectionRunClickResult> {
  const dialogPromise = page.waitForEvent("dialog", { timeout: 10_000 }).then(async (dialog) => {
    expect(dialog.type(), "PIT cutoff confirmation must be explicit before running selection").toBe("confirm");
    await dialog.accept();
  });
  const responsePromise = page.waitForResponse((response) =>
    response.url().endsWith("/selection-center/runs")
    && response.request().method() === "POST",
    { timeout: 300_000 },
  );
  await page.getByTestId("selection-run").click();
  await dialogPromise;
  const response = await responsePromise;
  const text = await response.text();
  let payload: JsonObject = {};
  if (text) {
    payload = JSON.parse(text) as JsonObject;
  }
  return { ok: response.ok(), status: response.status(), payload, text };
}

async function clickSelectionRunAndAcceptPit(page: Page): Promise<JsonObject> {
  const result = await clickSelectionRunAndAcceptPitResult(page);
  if (!result.ok) {
    throw new Error(`selection run HTTP ${result.status}: ${result.text.slice(0, 1200)}`);
  }
  return result.payload;
}

async function assertSinglePackageRunArtifactEvidence(request: APIRequestContext, runId: string) {
  expect(runId, "selection run response must include the persisted run id").toMatch(/^sel_/);
  const aggregate = await apiJson(request, `/selection-center/runs/${runId}/aggregate-results`);
  expect(aggregate.response.ok(), `load aggregate evidence for ${runId}: ${JSON.stringify(aggregate.payload).slice(0, 1200)}`).toBeTruthy();
  const rows: JsonObject[] = aggregate.payload.aggregate_results || [];
  expect(rows.length, `selection run ${runId} must persist aggregate rows`).toBeGreaterThan(0);
  const componentScores = rows[0].component_scores || {};
  expect(componentScores.artifact_source, `selection run ${runId} must keep live artifact authority`).toBe("live_qe_model_inference_v1");
  expect(Number(componentScores.raw_rank), `selection run ${runId} must keep raw rank provenance`).toBeGreaterThan(0);
  expect(Number(rows[0].selection_entry_price), `selection run ${runId} must expose persisted entry price`).toBeGreaterThan(0);
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

async function expectNoRawJsonUi(page: Page) {
  await expect(page.locator("pre, .pv2-json")).toHaveCount(0);
  await expect(page.locator("body")).not.toContainText(/\bJSON\b/i);
}

async function recoverFromDevChunkError(page: Page) {
  const chunkError = page.getByText(/Loading chunk .* failed|页面加载出错|ChunkLoadError/i).first();
  if (await chunkError.isVisible({ timeout: 5_000 }).catch(() => false)) {
    await page.reload({ waitUntil: "domcontentloaded" });
  }
}

async function expectSelectOptionValue(page: Page, testId: string, value: string, timeout = 60_000) {
  const readValues = async () =>
    page.getByTestId(testId).locator("option").evaluateAll((options) =>
      options.map((option) => (option as HTMLOptionElement).value),
    );
  try {
    await expect.poll(readValues, { timeout }).toContain(value);
  } catch (error) {
    // Next dev can occasionally serve an unhydrated page after repeated route
    // compiles. Reload once, then require the real backend data to appear.
    await page.reload({ waitUntil: "domcontentloaded" });
    await recoverFromDevChunkError(page);
    await expect.poll(readValues, { timeout }).toContain(value);
  }
}

async function assertWatchlistImportPersistence(
  request: APIRequestContext,
  payload: WatchlistImportPayload,
  expectedCategoryName: string,
  expectedSourceName: string,
): Promise<Record<string, string>> {
  expect(payload.ok, `watchlist import response: ${JSON.stringify(payload)}`).toBeTruthy();
  expect(payload.run_id).toBeTruthy();
  expect(payload.category_id).toBeGreaterThan(0);
  expect(payload.entry_source).toContain(expectedSourceName);
  expect(payload.entry_as_of).toBe(replayTradeDate);
  expect(payload.requested_top_k).toBe(20);
  expect(payload.imported_symbols.length).toBeGreaterThan(0);
  expect(payload.imported_symbols.length).toBeLessThanOrEqual(20);

  const categories = await apiJson(request, "/watchlist/categories");
  expect(categories.response.ok(), JSON.stringify(categories.payload)).toBeTruthy();
  const category = (categories.payload || []).find((item: JsonObject) => Number(item.id) === Number(payload.category_id));
  expect(category, `watchlist category ${payload.category_id} should exist`).toBeTruthy();
  expect(String(category.name)).toBe(expectedCategoryName);

  const aggregate = await apiJson(request, `/selection-center/runs/${payload.run_id}/aggregate-results`);
  expect(aggregate.response.ok(), JSON.stringify(aggregate.payload)).toBeTruthy();
  const aggregateRows: JsonObject[] = aggregate.payload.aggregate_results || [];
  const selectedRows = aggregateRows
    .filter((row) => payload.imported_symbols.includes(String(row.symbol)))
    .sort((a, b) => Number(a.rank) - Number(b.rank));
  expect(selectedRows.length).toBe(payload.imported_symbols.length);

  const items = await apiJson(
    request,
    `/watchlist/items?category_id=${payload.category_id}&page=1&page_size=50&sort_by=entry_rank&sort_dir=asc`,
  );
  expect(items.response.ok(), JSON.stringify(items.payload)).toBeTruthy();
  const watchRows: JsonObject[] = items.payload.items || [];
  expect(items.payload.total).toBeGreaterThanOrEqual(payload.imported_symbols.length);

  const entryAsOfBySymbol: Record<string, string> = {};
  for (const selectedRow of selectedRows) {
    const symbol = String(selectedRow.symbol);
    const watchRow = watchRows.find((row) => row.code === symbol);
    if (!watchRow) {
      throw new Error(`watchlist row for ${symbol} should be persisted`);
    }
    expect(watchRow.category_names || "").toContain(expectedCategoryName);
    expect(watchRow.entry_source || "").toContain(expectedSourceName);
    expect(watchRow.entry_task_id).toBe(payload.run_id);
    const expectedEntryAsOf = selectionEntryPriceBasisDate(selectedRow);
    entryAsOfBySymbol[symbol] = expectedEntryAsOf;
    expect(watchRow.entry_as_of).toBe(expectedEntryAsOf);
    expect(watchRow.entry_rank).toBe(Number(selectedRow.rank));
    expect(Number(watchRow.entry_price)).toBeCloseTo(Number(selectedRow.reference_price), 4);
    expect(Number(watchRow.entry_price)).toBeGreaterThan(0);
    expect(String(watchRow.created_at || "")).toMatch(/^\d{4}-\d{2}-\d{2}/);
    expect(String(watchRow.updated_at || "")).toMatch(/^\d{4}-\d{2}-\d{2}/);
  }
  return entryAsOfBySymbol;
}

function selectionEntryPriceBasisDate(row: JsonObject): string {
  const componentScores = (row.component_scores && typeof row.component_scores === "object") ? row.component_scores as JsonObject : {};
  const display = (componentScores.selection_result_display && typeof componentScores.selection_result_display === "object")
    ? componentScores.selection_result_display as JsonObject
    : {};
  for (const raw of [
    row.selection_entry_price_time,
    display.selection_entry_price_time,
    display.reference_price_trade_date,
    componentScores.reference_price_trade_date,
    replayTradeDate,
  ]) {
    const value = String(raw || "").slice(0, 10);
    if (/^\d{4}-\d{2}-\d{2}$/.test(value)) return value;
  }
  return replayTradeDate;
}

test.describe.serial("Paper Trading v2 UI real-backend validation", () => {
  test.beforeAll(async ({ request }) => {
    const health = await request.get(`${API_BASE.replace(/\/api\/v1$/, "")}/openapi.json`);
    expect(health.ok(), `temporary backend must be reachable at ${API_BASE}`).toBeTruthy();

    const defaults = await apiJson(request, "/paper-v2/trading-days/defaults?lookback_trading_days=10");
    expect(defaults.response.ok(), JSON.stringify(defaults.payload)).toBeTruthy();
    expect(defaults.payload.latest_trading_day).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(defaults.payload.replay_start_date).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    if (!replayTradeDate) {
      replayTradeDate = defaults.payload.latest_trading_day;
    }
    replayCutoffDate = await resolveReplayCutoff(request);

    ensuredPackages = await discoverValidationPackages(request);
    expect(ensuredPackages.length, "Paper v2 UI E2E requires at least two runnable StrategyPackages").toBeGreaterThanOrEqual(2);
    ensuredRuns = [];
    for (const pkg of ensuredPackages) {
      ensuredRuns.push(await ensureSuccessfulSelectionRun(request, pkg));
    }
    const selectable = await listSelectablePackages(request);
    runnableSelectionPackageIds = new Set(
      selectable
        .filter((item) => item.selection_health?.runnable === true)
        .map((item) => item.package_id),
    );
  });

  test("StrategyPackage page shows unpackaged QE sources, packaged strategies, and paper entry", async ({ page, request }) => {
    await page.goto("/paper-v2/packages");
    await expect(page.getByRole("heading", { name: "模拟盘 v2" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "从 QE 创建策略包" })).toBeVisible();
    await expect(page.getByText("创建时解析并复制运行资产")).toBeVisible();

    const sources = await apiJson(request, "/strategy-packages/qe-sources?source_kind=all&limit=20");
    expect(sources.response.ok(), JSON.stringify(sources.payload)).toBeTruthy();
    const sourceSection = await openSection(page, "从 QE 创建策略包");
    const sourceText = await sourceSection.locator("select").nth(1).textContent();
    for (const pkg of ensuredPackages.filter((item) => item.source_type === "qe_experiment" || String(item.source_id || "").startsWith("qe_"))) {
      expect(sourceText || "").not.toContain(String(pkg.source_id || pkg.package_name));
    }
    if ((sources.payload.sources || []).length > 0) {
      await expect(sourceSection.locator("select").nth(1)).toContainText(/年化|IC|回撤/);
    }

    await expect(page.getByRole("heading", { name: "StrategyPackage 列表" })).toBeVisible();
    const packageTable = page.locator("table").filter({ hasText: "Manifest" }).first();
    for (const pkg of ensuredPackages) {
      await expect(packageTable.locator("tbody tr").filter({ hasText: pkg.package_name }).first()).toBeVisible();
    }
    await expect(page.locator("body")).toContainText("RankIC");
    await expect(page.locator("body")).toContainText("最大回撤");
    await expect(page.locator("body")).toContainText("生命周期状态");
    await expect(page.locator("body")).toContainText("选股准入");
    await expect(page.locator("body")).toContainText("模拟盘准入");
    await expect(page.getByText("可进入选股").first()).toBeVisible();
    await expect(page.getByText("可创建模拟盘").first()).toBeVisible();
    await expect(page.getByText("用此包创建模拟盘").first()).toBeVisible();
    await expect(page.getByText("退役策略包").first()).toBeVisible();
  });

  test("Selection Center runs live-data inference, displays history, and imports watchlist items", async ({ page, request }) => {
    const target = ensuredPackages[0];
    await page.goto("/paper-v2/selection");
    await expect(page.getByRole("heading", { name: "选股控制" })).toBeVisible();

    const control = await openSection(page, "选股控制");
    await control.locator("select").first().selectOption("single_package");
    await control.locator('input[type="date"]').fill(replayTradeDate);
    await control.locator('input[type="number"]').first().fill("20");

    const picker = await openSection(page, "策略包选择器");
    const checkboxes = picker.locator('tbody input[type="checkbox"]');
    for (let i = 0; i < await checkboxes.count(); i += 1) {
      const box = checkboxes.nth(i);
      if (await box.isChecked()) await box.uncheck();
    }
    const targetRow = picker.locator("tbody tr").filter({ hasText: target.package_name });
    await expect(targetRow).toBeVisible();
    if (!runnableSelectionPackageIds.has(target.package_id)) {
      await assertSelectionHealthGateBlocksLegacy(page, target.package_id);
      return;
    }
    await targetRow.locator('input[type="checkbox"]').check();

    const runPayload = await clickSelectionRunAndAcceptPit(page);
    const runId = String(runPayload.run?.run_id || runPayload.run_id || "");
    await assertSinglePackageRunArtifactEvidence(request, runId);
    const results = await openSection(page, "选股结果");
    await expect(page.getByTestId("selection-run")).toBeEnabled({ timeout: 300_000 });
    await expect(results).toContainText("live_qe_model_inference_score", { timeout: 300_000 });
    await expect(results.locator("tbody tr").first()).toBeVisible();
    await expect(results).toContainText(/选股参考价|入池价/);

    const watchlistCategoryName = `PaperV2-E2E-Watchlist-${Date.now()}`;
    await results.locator("input.pv2-input").first().fill(watchlistCategoryName);
    const importResponse = page.waitForResponse((response) =>
      response.url().includes("/selection-center/runs/")
        && response.url().endsWith("/add-to-watchlist")
        && response.request().method() === "POST",
    );
    await results.getByRole("button", { name: "一键加入自选股票池" }).click();
    const importResult = await importResponse;
    expect(importResult.ok(), `watchlist import HTTP ${importResult.status()}`).toBeTruthy();
    const importEnvelope = await importResult.json() as { ok: boolean; result: WatchlistImportPayload };
    expect(importEnvelope.ok, `watchlist import envelope: ${JSON.stringify(importEnvelope)}`).toBeTruthy();
    const importPayload = importEnvelope.result;
    await expect(results).toContainText("已加入自选股票池", { timeout: 30_000 });
    await expect(results).toContainText(target.package_name);
    const entryAsOfBySymbol = await assertWatchlistImportPersistence(request, importPayload, watchlistCategoryName, target.package_name);
    await expectNoRawJsonUi(page);

    await page.goto("/watchlist");
    await expect(page.getByTestId("watchlist-title")).toBeVisible({ timeout: 60_000 });
    await expectSelectOptionValue(page, "watchlist-category-filter", String(importPayload.category_id));
    await page.getByTestId("watchlist-category-filter").selectOption(String(importPayload.category_id));
    const firstImportedSymbol = importPayload.imported_symbols[0];
    await expect(page.getByTestId(`watchlist-row-${firstImportedSymbol}`)).toBeVisible({ timeout: 60_000 });
    await expect(page.getByTestId("watchlist-items-table")).toContainText(watchlistCategoryName);
    await expect(page.getByTestId("watchlist-items-table")).toContainText(target.package_name);
    await expect(page.getByTestId(`watchlist-cell-rank-${firstImportedSymbol}`)).toContainText("1");
    await expect(page.getByTestId(`watchlist-cell-entry-price-${firstImportedSymbol}`)).not.toContainText("-");
    await expect(page.getByTestId(`watchlist-cell-entry-as-of-${firstImportedSymbol}`)).toContainText(entryAsOfBySymbol[firstImportedSymbol] || replayTradeDate);
    await expect(page.getByTestId("watchlist-items-table")).toContainText("加入以来涨幅");
    await expect(page.getByTestId("watchlist-items-table")).toContainText("加入时间");
    await expectNoRawJsonUi(page);

    await page.goto("/paper-v2/selection");
    const history = await openSection(page, "历史选股记录");
    await expect(history).toContainText("点击记录可显示结果");
    await expect(page.getByTestId("selection-history-select-page")).toBeVisible();
    await page.getByTestId("selection-history-select-page").click();
    await expect(history.locator('tbody input[type="checkbox"]:checked').first()).toBeVisible();
    await page.getByTestId("selection-history-clear-page").click();
    await expect(history.locator('tbody input[type="checkbox"]:checked')).toHaveCount(0);
    await history.locator("tbody button").first().click();
    await expect(results.locator("tbody tr").first()).toBeVisible();
  });

  test("Selection history keeps bulk selection but removes manual aggregate entry", async ({ page }) => {
    await page.goto("/paper-v2/selection");
    const control = await openSection(page, "选股控制");
    await expect(control.getByTestId("selection-mode-fixed")).toContainText("单策略包");
    await expect(page.getByTestId("selection-mode")).toHaveCount(0);
    await expect(page.locator("[data-testid^='selection-weight-']")).toHaveCount(0);
    await expect(page.getByText("from-multi-alpha-combine-run")).toBeVisible();

    const history = await openSection(page, "历史选股记录");
    const sourceRunIds = ensuredRuns.slice(0, 2).map((item) => item.run_id);
    expect(sourceRunIds.length, "E2E setup must create at least two compatible single-package runs").toBe(2);
    for (const runId of sourceRunIds) {
      const checkbox = page.getByTestId(`selection-run-checkbox-${runId}`);
      await expect(checkbox, `compatible selection run ${runId} must be visible in history`).toBeVisible();
      await checkbox.check();
    }
    await expect(page.getByTestId("selection-aggregate-runs")).toHaveCount(0);
    await expect(history.getByRole("button", { name: /删除选中/ })).toBeEnabled();
    await page.getByTestId("selection-history-clear-page").click();
    await expect(history.locator('tbody input[type="checkbox"]:checked')).toHaveCount(0);
  });

  test("Selection Center validates single-package HMM, blacklist backfill, and TopK guard through UI", async ({ page, request }) => {
    const hmm = await requireHmmAutomaticRuntimeConfig(request);
    const [first] = ensuredPackages;
    await page.goto("/paper-v2/selection");

    const results = await openSection(page, "选股结果");
    await expect(page.getByTestId("selection-mode-fixed")).toContainText("单策略包");
    await expect(page.getByTestId("selection-mode")).toHaveCount(0);
    await expect(page.locator("[data-testid^='selection-weight-']")).toHaveCount(0);
    await chooseSelectionPackages(page, [first.package_id]);
    await page.getByTestId("selection-top-k").fill("20");
    await page.getByTestId("selection-trade-date").fill(hmm.trade_date);
    await expect(page.getByTestId("selection-cutoff-date")).toContainText(/^\d{4}-\d{2}-\d{2}$/, { timeout: 30_000 });
    await chooseIndustryBlacklist(page, E2E_BLACKLIST_L1_CODE, E2E_BLACKLIST_L2_CODE);
    await page.getByTestId("selection-hmm-enabled").check();
    await expect(page.getByTestId("selection-hmm-enabled")).toBeChecked();
    await expect(page.getByTestId("selection-hmm-config")).toBeEnabled({ timeout: 30_000 });
    await expectSelectOptionValue(page, "selection-hmm-config", hmm.config_id);
    await page.getByTestId("selection-hmm-config").selectOption(hmm.config_id);
    await expect(page.getByTestId("selection-hmm-snapshot")).toHaveCount(0);
    await expect(page.getByTestId("selection-hmm-preset")).toBeEnabled({ timeout: 30_000 });
    await page.getByTestId("selection-hmm-preset").selectOption("preset_A");
    await expect(page.getByTestId("selection-hmm-coverage")).toContainText(/HMM/, { timeout: 30_000 });
    const hmmResult = await clickSelectionRunAndAcceptPitResult(page);
    await expect(page.getByTestId("selection-run")).toBeEnabled({ timeout: 300_000 });
    const hmmRuntimeError = page.locator(".pv2-error-panel").filter({ hasText: /HMM/ });
    const hmmFailedFast = !hmmResult.ok || await hmmRuntimeError.isVisible({ timeout: 1_000 }).catch(() => false);
    if (hmmFailedFast) {
      expect(hmmResult.status, `HMM fail-fast response: ${hmmResult.text.slice(0, 1200)}`).toBe(400);
      expect(hmmResult.text).toMatch(/HMM|coefficients|ARTIFACT_GENERATION|snapshot|stock sector mapping/i);
      await expect(hmmRuntimeError).toContainText(/HMM|stock sector mapping|coefficients|ARTIFACT_GENERATION|snapshot/);
    } else {
      await expect(results).toContainText("hmm", { timeout: 300_000 });
      await expect(results.locator("tbody tr").first()).toBeVisible();
      const excluded = await openSection(page, "剔除与补位追踪");
      await expect(excluded).toContainText("industry_blacklisted");
    }

    await page.getByTestId("selection-top-k").fill("51");
    await page.getByTestId("selection-run").click();
    await expect(page.locator(".pv2-error-panel")).toContainText("TopK");

    await page.getByTestId("selection-top-k").fill("20");
    await page.getByTestId("selection-trade-date").fill(HMM_UNCOVERED_TRADE_DATE);
    await expect(page.getByTestId("selection-cutoff-date")).toContainText(/^\d{4}-\d{2}-\d{2}$/, { timeout: 30_000 });
    await expect(page.getByTestId("selection-hmm-snapshot")).toHaveCount(0);
    await expect(page.getByTestId("selection-hmm-coverage")).toContainText(/HMM/);
  });

  test("Portfolio page creates a replay portfolio or surfaces runtime asset block", async ({ page, request }) => {
    const target = ensuredPackages[0];
    const policy = await loadExecutionPolicyOrDefault(request, target);
    const portfolioName = `E2E-Paper-${Date.now()}`;

    await page.goto(`/paper-v2/portfolios?package_id=${target.package_id}`);
    await expect(page.locator("body")).toContainText(target.package_name);

    const createSection = page.locator("section.pv2-card").first();
    await expect(createSection.locator("select").nth(0).locator(`option[value="${target.package_id}"]`)).toHaveCount(1, { timeout: 30_000 });
    await createSection.locator("select").nth(0).selectOption(target.package_id, { timeout: 30_000 });
    await createSection.locator("input.pv2-input").nth(0).fill(portfolioName);
    await createSection.locator("input.pv2-input").nth(1).fill("1000000");
    await createSection.locator("select").nth(1).selectOption("REPLAY_ONLY", { timeout: 30_000 });
    await expect(createSection.locator("input.pv2-input").nth(2)).toHaveValue(/DB_HISTORICAL/);
    if (policy) {
      await expect(createSection.locator("select").nth(2)).toContainText(/V25_TWO_STAGE|V25_1_SMALL_CAP|Platform runtime default|execution policy|manifest_default_execution_policy/i, { timeout: 30_000 });
      await createSection.locator("select").nth(2).selectOption(policy.policy_id, { timeout: 30_000 });
    }
    await createSection.locator("input.pv2-input").nth(3).fill(replayTradeDate);
    await createSection.locator("input.pv2-input").nth(4).fill(replayTradeDate);
    await createSection.locator('input[type="number"]').nth(1).fill("5");
    const createButton = createSection.locator("button.pv2-button-primary");
    await expect(createButton).toBeEnabled({ timeout: 30_000 });
    await createButton.click({ timeout: 30_000 });

    const errorPanel = page.locator(".pv2-error-panel").first();
    const blockedText = await errorPanel.textContent({ timeout: 10_000 }).catch(() => "");
    paperPortfolioRuntimeBlocked = paperPortfolioBlockText(blockedText || "");
    if (paperPortfolioRuntimeBlocked) {
      await expect(errorPanel).toContainText(/DATA_UNAVAILABLE|INVALID_STATE_TRANSITION|V24_PLAN|EXECUTION_ALGO_ERROR|model_path|not accessible|execution policy/i);
      await expect(page.locator(".pv2-readable-panel").filter({ hasText: /Created Portfolio Id|created_portfolio_id/ })).toHaveCount(0);
      return;
    }

    const createdNotice = page.locator(".pv2-notice-success").filter({ hasText: portfolioName });
    await expect(createdNotice, "portfolio creation success notice must identify the created portfolio").toBeVisible({ timeout: 120_000 });
    const portfolio = await waitForPortfolioByName(request, portfolioName);
    const portfolioId = portfolio.portfolio_id;
    expect(portfolioId).toMatch(/^paper_/);
    replayPortfolioId = portfolioId;

    const replayRun = await waitForReplayRunOutcome(request, portfolioId);
    if (String(replayRun.status) === "FAILED") {
      paperPortfolioRuntimeBlocked = paperPortfolioBlockText(replayRun.error_json || "");
      expect(
        paperPortfolioRuntimeBlocked,
        `portfolio replay failed with unrecognized error: ${JSON.stringify(replayRun.error_json).slice(0, 1200)}`,
      ).toBeTruthy();
      return;
    }
    expect(replayRun.status).toBe("SUCCEEDED");

    const [runs, orders, fills, positions, cashLedger, snapshots, errors, performance] = await Promise.all([
      apiJson(request, `/paper-v2/portfolios/${portfolioId}/runs?limit=100`),
      apiJson(request, `/paper-v2/portfolios/${portfolioId}/orders?limit=1000`),
      apiJson(request, `/paper-v2/portfolios/${portfolioId}/fills?limit=1000`),
      apiJson(request, `/paper-v2/portfolios/${portfolioId}/positions?limit=1000`),
      apiJson(request, `/paper-v2/portfolios/${portfolioId}/cash-ledger?limit=1000`),
      apiJson(request, `/paper-v2/portfolios/${portfolioId}/daily-snapshots?limit=1000`),
      apiJson(request, `/paper-v2/portfolios/${portfolioId}/errors?limit=1000`),
      apiJson(request, `/paper-v2/portfolios/${portfolioId}/performance-report`),
    ]);
    expect(runs.payload.runs?.[0]?.status).toBe("SUCCEEDED");
    expect(orders.payload.orders?.length || 0).toBeGreaterThan(0);
    expect(fills.payload.fills?.length || 0).toBeGreaterThan(0);
    expect(positions.payload.positions?.length || 0).toBeGreaterThan(0);
    expect(cashLedger.payload.cash_ledger?.length || 0).toBeGreaterThan(0);
    expect(orders.payload.orders?.[0]?.stock_name).toBeTruthy();
    expect(fills.payload.fills?.[0]?.stock_name).toBeTruthy();
    expect(positions.payload.positions?.[0]?.stock_name).toBeTruthy();
    expect(cashLedger.payload.cash_ledger?.[0]?.stock_name).toBeTruthy();
    expect(snapshots.payload.daily_snapshots?.length || 0).toBeGreaterThan(0);
    expect(errors.payload.errors?.length || 0).toBe(0);
    expect(performance.payload.performance_report?.snapshot_count).toBeGreaterThan(0);

    await page.goto(`/paper-v2/portfolios/${portfolioId}/ledger`);
    await expect(page.locator("table").nth(0).locator("tbody tr").first()).toBeVisible();
    await page.getByTestId("ledger-refresh").click();
    await expect(page.locator("body")).toContainText("已全部成交");
    await page.getByTestId("ledger-order-trace-0").click();
    await expect(page.getByTestId("ledger-order-trace-detail")).toBeVisible();
    await expect(page.locator(".pv2-readable-panel").last()).toContainText("FILLED");
    await expectNoRawJsonUi(page);
    await expect(page.locator("table").nth(1).locator("tbody tr").first()).toBeVisible();
    await expect(page.locator("table").nth(3).locator("tbody tr").first()).toBeVisible();
    await expect(page.locator("table").nth(4).locator("tbody tr").first()).toBeVisible();

    await page.goto(`/paper-v2/portfolios/${portfolioId}/performance`);
    await expect(page.locator(".pv2-readable-panel")).toContainText("Snapshot Count");
    await expect(page.locator("table").nth(0).locator("tbody tr").first()).toBeVisible();
    await expectNoRawJsonUi(page);
  });

  test("Overview and portfolio detail lifecycle are usable without market-time streams", async ({ page, request }) => {
    test.skip(Boolean(paperPortfolioRuntimeBlocked), `Paper portfolio runtime asset block: ${paperPortfolioRuntimeBlocked}`);
    expect(replayPortfolioId, "previous replay portfolio must be available for UI detail validation").toMatch(/^paper_/);

    await page.goto("/paper-v2");
    await expect(page.locator('a[href="/paper-v2/selection"]').first()).toBeVisible();
    await expect(page.locator('a[href="/paper-v2/running"]').first()).toBeVisible();
    await expect(page.locator('a[href="/paper-v2/portfolios"]').first()).toBeVisible();
    await expect(page.locator(`a[href="/paper-v2/portfolios/${replayPortfolioId}"]`).first()).toBeVisible({ timeout: 60_000 });

    await page.goto("/paper-v2/running");
    await expect(page.locator(".pv2-error-panel")).toHaveCount(0);
    await expect(page.locator("body")).not.toContainText("Failed to fetch");
    await expect(page.getByRole("heading", { name: "正在运行模拟盘列表" })).toBeVisible();
    await expect(page.locator(`a[href="/paper-v2/portfolios/${replayPortfolioId}/live-dashboard"]`).first()).toBeVisible({ timeout: 60_000 });
    await expect(page.locator(".pv2-error-panel")).toHaveCount(0);
    await expect(page.locator("body")).not.toContainText("Failed to fetch");
    await expect(page.locator("body")).toContainText("净值曲线");

    const runningSummary = await apiJson(request, "/paper-v2/running-summary?limit=300&snapshot_limit=30&position_limit=8");
    expect(runningSummary.response.ok(), JSON.stringify(runningSummary.payload)).toBeTruthy();
    expect((runningSummary.payload.summaries || []).some((row: JsonObject) => row.portfolio?.portfolio_id === replayPortfolioId)).toBeTruthy();

    await page.goto(`/paper-v2/portfolios/${replayPortfolioId}/live-dashboard`);
    await expect(page.getByTestId("paper-live-dashboard")).toBeVisible({ timeout: 60_000 });
    await expect(page.locator("body")).toContainText(/今日信号|当日候选信号/);
    await expect(page.locator("body")).toContainText("分钟执行时间轴");
    await expect(page.locator("body")).toContainText(/实时资产曲线|分钟资产快照缺失/);
    await expectNoRawJsonUi(page);

    await page.goto("/paper-v2/miniqmt-sim");
    await expect(page.getByTestId("miniqmt-local-fields-help")).toBeVisible({ timeout: 60_000 });
    await expect(page.getByTestId("miniqmt-position-sort-code")).toBeVisible();
    await expect(page.getByTestId("miniqmt-trades-toggle")).toBeVisible();
    await expect(page.getByTestId("miniqmt-trades-table")).toHaveCount(0);
    await page.getByTestId("miniqmt-trades-toggle").click();
    await expect(page.getByTestId("miniqmt-trades-table")).toBeVisible();
    await expect(page.getByTestId("miniqmt-trade-sort-name")).toBeVisible();
    await page.getByTestId("miniqmt-position-sort-code").click();
    await page.getByTestId("miniqmt-position-sort-code").click();
    await page.getByTestId("miniqmt-position-sort-code").click();

    await expect(page.locator('a[href="/paper-v2/settings"]')).toHaveCount(0);

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

  test("Portfolio detail completes and retires an isolated test portfolio", async ({ page, request }) => {
    const target = ensuredPackages[0];
    const policy = await loadExecutionPolicyOrDefault(request, target);
    const portfolio = await createPaperPortfolioOnly(request, target, policy, `E2E-Lifecycle-${Date.now()}`);
    if (!portfolio) {
      test.skip(true, `Paper portfolio runtime asset block: ${paperPortfolioRuntimeBlocked}`);
      return;
    }

    await page.goto(`/paper-v2/portfolios/${portfolio.portfolio_id}`);
    await page.getByTestId("portfolio-lifecycle-complete").click();
    await expect.poll(async () => {
      const { payload } = await apiJson(request, `/paper-v2/portfolios/${portfolio.portfolio_id}`);
      return payload.portfolio?.status;
    }, { timeout: 30_000 }).toBe("COMPLETED");
    await expect(page.locator("body")).toContainText("COMPLETED");

    await page.getByTestId("portfolio-lifecycle-retire").click();
    await expect.poll(async () => {
      const { payload } = await apiJson(request, `/paper-v2/portfolios/${portfolio.portfolio_id}`);
      return payload.portfolio?.status;
    }, { timeout: 30_000 }).toBe("RETIRED");
    await expect(page.locator("body")).toContainText("RETIRED");
    await expectNoRawJsonUi(page);
  });

  test("Run console validates readiness, policy/runtime audit, replay reject/reset, and live waiting controls", async ({ page, request }) => {
    const target = ensuredPackages[0];
    const policy = await loadExecutionPolicyOrDefault(request, target);
    const portfolio = await createPaperPortfolioOnly(request, target, policy, `E2E-Console-${Date.now()}`);
    if (!portfolio) {
      test.skip(true, `Paper portfolio runtime asset block: ${paperPortfolioRuntimeBlocked}`);
      return;
    }
    const consolePath = `/paper-v2/portfolios/${portfolio.portfolio_id}/run-console`;

    await page.goto(consolePath);
    await page.getByTestId("console-runtime-top-k").fill("20");
    await page.getByTestId("console-trade-date").fill(replayTradeDate);
    await page.getByTestId("console-readiness").click();
    const readinessCard = page.locator(".pv2-readiness-card").last();
    await expect(readinessCard).toBeVisible({ timeout: 120_000 });
    await expect(readinessCard).toContainText(/rebalance|minute_market_data|订单意图|Order Intent/i);
    await expect(readinessCard.locator('.pv2-badge[title="passed"]').first()).toBeVisible();
    await expect(page.getByTestId("console-run-day")).toBeEnabled();
    await page.getByTestId("console-run-day").click();
    const dayRun = await waitForReplayRunOutcome(request, portfolio.portfolio_id);
    if (String(dayRun.status) === "FAILED") {
      paperPortfolioRuntimeBlocked = paperPortfolioBlockText(dayRun.error_json || "");
      expect(
        paperPortfolioRuntimeBlocked,
        `run-console day run failed with unrecognized error: ${JSON.stringify(dayRun.error_json).slice(0, 1200)}`,
      ).toBeTruthy();
      return;
    }
    expect(dayRun.status).toBe("SUCCEEDED");
    const runJson = page.locator(".pv2-readable-panel").filter({ hasText: /daily run result|运行ID|Run ID/ }).last();
    await expect(runJson).toContainText("SUCCEEDED", { timeout: 30_000 });

    await page.getByTestId("console-runtime-top-k").fill("21");
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
    await expect(page.locator(".pv2-readable-panel").filter({ hasText: "Config Change Audit" })).toContainText("runtime_profile");

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
    await expect(page.locator("body")).toContainText(/V25_TWO_STAGE|V25_1_SMALL_CAP|Platform runtime default|execution policy|manifest_default_execution_policy/i);

    await page.getByTestId("console-replay-start").fill(replayTradeDate);
    await page.getByTestId("console-replay-end").fill(replayTradeDate);
    await page.getByTestId("console-replay-reject").click();
    await expect(page.locator(".pv2-error-panel")).toContainText(/reject_existing|already|已有|DUPLICATE/i, { timeout: 60_000 });

    await page.getByTestId("console-replay-reset").click();
    await page.getByTestId("console-replay-reset-input").fill(portfolio.portfolio_id);
    await page.getByTestId("console-replay-reset-confirm").click();
    const resetJson = page.locator(".pv2-readable-panel").filter({ hasText: "Session" }).last();
    await expect(resetJson).toContainText("REPLAY_ONLY", { timeout: 180_000 });
    await expect(resetJson).toContainText("SUCCEEDED", { timeout: 180_000 });
    await expectNoRawJsonUi(page);

    if (!SKIP_REALTIME) {
      await page.getByTestId("console-live-start").fill(ACTIVATION_TRADE_DATE);
      const liveButton = page.getByTestId("console-live-create");
      if (await liveButton.isEnabled()) {
        await liveButton.click();
        await expect(page.locator(".pv2-readable-panel").last()).toContainText(/LIVE_|FAILED|WAITING|SESSION|ALGO/, { timeout: 90_000 });
      } else {
        await expect(liveButton).toBeDisabled();
        await expect(page.locator("body")).toContainText(/实时模拟|TDX_REALTIME|TDX 实时/);
      }
    }
  });

  test("Model and HMM maintenance previews fail fast or run without implicit training", async ({ page, request }) => {
    const target = ensuredPackages[0];
    const hmm = await requireHmmRuntimeChoice(request);
    await page.goto("/paper-v2/model-hmm");
    await recoverFromDevChunkError(page);
    await expect(page.getByTestId("model-package")).toBeVisible({ timeout: 60_000 });
    await expectSelectOptionValue(page, "model-package", target.package_id);

    await page.getByTestId("model-package").selectOption(target.package_id);
    await page.getByTestId("model-as-of-date").fill(replayTradeDate);
    await page.getByTestId("model-lookback-days").fill("756");
    await page.getByTestId("model-retrain-preview").click();
    const modelPreview = page.locator(".pv2-readable-panel").filter({ hasText: /manual retrain|original backtest model|重训|已生成/i }).last();
    await expect(modelPreview, "model retrain preview must render the current readable summary UI").toBeVisible({ timeout: 60_000 });
    await expect(modelPreview).toContainText(/manual retrain|original backtest model|重训|已生成/i);

    await page.getByTestId("hmm-config").selectOption(hmm.config_id);
    await page.getByTestId("hmm-as-of-date").fill(replayTradeDate);
    await page.getByTestId("hmm-validation-months").fill("3");
    await page.getByTestId("hmm-train-years").fill("3");
    await page.getByTestId("hmm-rolling-preview").click();
    const hmmSection = page.getByTestId("hmm-rolling-preview").locator("xpath=ancestor::section[1]");
    const hmmPreview = hmmSection.locator(".pv2-readable-panel").last();
    await expect(hmmPreview, "HMM rolling preview must render a readable summary for the selected as-of date").toBeVisible({ timeout: 60_000 });
    await expect(hmmPreview.locator(".pv2-readable-row")).toHaveCount(4);

    await expect(page.getByText("HMM 每日系数由平台自动计算")).toBeVisible();
    await expect(page.getByTestId("hmm-daily-snapshot")).toHaveCount(0);
    await expect(page.getByTestId("hmm-daily-preview")).toHaveCount(0);
    await expect(page.getByTestId("hmm-daily-generate")).toHaveCount(0);
    await expect(page.locator("section").filter({ hasText: "HMM 模型缓存状态" })).toContainText(/运行时自动生成|preset_A|READY|COMPLETED/i);
    await expectNoRawJsonUi(page);
  });

  test("Negative APIs return structured errors and optional TDX realtime minute endpoint is reachable", async ({ request }) => {
    const missing = await apiJson(request, "/strategy-packages/from-qe-experiment/not_exists_for_failfast/manifest");
    expect(missing.response.status()).toBe(404);
    expect(missing.payload.detail.error_code).toBe("DATA_UNAVAILABLE");

    const badSelection = await apiJson(request, "/selection-center/runs", {
      method: "POST",
      data: {
        package_ids: ["not_exists_for_failfast"],
        trade_date: replayTradeDate,
        data_source: "DB_HISTORICAL",
        mode: "single_package",
        runtime_config: runtimeConfig(20),
      },
    });
    expect(badSelection.response.ok()).toBeFalsy();
    expect(JSON.stringify(badSelection.payload)).toMatch(/DATA_UNAVAILABLE|not_exists_for_failfast|package/i);

    if (!SKIP_REALTIME) {
      const tdx = await request.get(`${TDX_BASE}/api/kline-all/tdx?code=SZ000001&type=minute1`, { timeout: 30_000 });
      expect(tdx.ok(), `TDX backend must respond at ${TDX_BASE}`).toBeTruthy();
      const payload = await tdx.json();
      expect(payload.code).toBe(0);
      const bars = payload.data?.list || [];
      expect(Array.isArray(bars)).toBeTruthy();
      expect(bars.length, "TDX minute endpoint should return real minute bars instead of a fake default").toBeGreaterThan(0);
    }
  });
});
