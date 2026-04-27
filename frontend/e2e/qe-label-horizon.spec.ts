import { expect, test, type APIRequestContext, type Page } from "@playwright/test";

const backendPort = process.env.BACKEND_PORT || "8012";
const apiBase =
  process.env.API_BASE ||
  process.env.NEXT_PUBLIC_API_BASE ||
  `http://127.0.0.1:${backendPort}/api/v1`;

const goldenModelName =
  process.env.QE_E2E_MODEL_NAME || "LGBModel Golden Seed (validated 2026-03-14)";
const goldenModelId = process.env.QE_E2E_MODEL_ID || "__seed_LGBModel_golden_v1__";
const runBacktest = process.env.QE_E2E_RUN_BACKTEST !== "0";
const runTimeoutMs = Number(process.env.QE_E2E_RUN_TIMEOUT_MS || 45 * 60 * 1000);

type ModelRow = {
  model_id: string;
  model_name?: string;
  display_name?: string;
};

type FactorRow = {
  factor_name: string;
  source?: string;
  is_available?: boolean;
};

function searchParamEquals(url: string, value: string): boolean {
  try {
    return new URL(url).searchParams.get("search") === value;
  } catch {
    return false;
  }
}

function endpoint(path: string): string {
  return `${apiBase}${path}`;
}

async function jsonFrom(response: { json: () => Promise<unknown>; text: () => Promise<string> }): Promise<any> {
  try {
    return await response.json();
  } catch {
    return { raw: await response.text() };
  }
}

async function expectOk(response: any, context: string): Promise<any> {
  const body = await jsonFrom(response);
  expect(response.ok(), `${context}: ${JSON.stringify(body).slice(0, 1000)}`).toBeTruthy();
  return body;
}

async function findGoldenModel(request: APIRequestContext): Promise<ModelRow> {
  const urls = [
    `/quantevolver/models?limit=50&search=${encodeURIComponent(goldenModelName)}`,
    `/quantevolver/models?limit=200`,
  ];
  const seen: ModelRow[] = [];
  for (const url of urls) {
    const body = await expectOk(await request.get(endpoint(url)), `load models ${url}`);
    const items = (body.items || body.data || []) as ModelRow[];
    seen.push(...items);
    const match = items.find(
      (m) =>
        m.model_id === goldenModelId ||
        m.model_name === goldenModelName ||
        m.display_name === goldenModelName,
    );
    if (match) return match;
  }
  throw new Error(
    `Required QE model not found: ${goldenModelName}. Seen models: ${seen
      .slice(0, 20)
      .map((m) => `${m.model_id}:${m.display_name || m.model_name}`)
      .join(", ")}`,
  );
}

async function findTestFactor(request: APIRequestContext): Promise<FactorRow> {
  const explicit = process.env.QE_E2E_FACTOR_NAME;
  const urls = explicit
    ? [`/quantevolver/factors?limit=20&search=${encodeURIComponent(explicit)}`]
    : [
        "/quantevolver/factors?limit=20&availability=enabled&exclude_source=alpha158%2Calpha360",
        "/quantevolver/factors?limit=20&exclude_source=alpha158%2Calpha360",
        "/quantevolver/factors?limit=20",
      ];
  for (const url of urls) {
    const body = await expectOk(await request.get(endpoint(url)), `load factors ${url}`);
    const items = (body.items || body.data || []) as FactorRow[];
    const match = items.find((f) => f.factor_name && f.is_available !== false);
    if (match) return match;
  }
  throw new Error("No available QE factor found for UI label_horizon E2E");
}

async function selectFactor(page: Page, factor: FactorRow): Promise<void> {
  await Promise.all([
    page.waitForResponse(
      (r) =>
        r.url().includes("/quantevolver/factors?") &&
        searchParamEquals(r.url(), factor.factor_name) &&
        r.request().method() === "GET",
    ),
    page.getByTestId("qe-factor-search").fill(factor.factor_name),
  ]);
  const row = page.locator('[data-testid^="qe-factor-row-"]').filter({ hasText: factor.factor_name }).first();
  await expect(row, `factor row ${factor.factor_name}`).toBeVisible();
  const checkbox = page.getByTestId(`qe-factor-checkbox-${factor.factor_name}`).first();
  await expect(checkbox).toBeVisible();
  await expect(checkbox).toBeEnabled();
  await checkbox.check();
  await expect(checkbox).toBeChecked();
  await expect(page.getByTestId("qe-step-next-factors")).toBeEnabled();
}

async function selectGoldenModel(page: Page, model: ModelRow): Promise<void> {
  await Promise.all([
    page.waitForResponse(
      (r) =>
        r.url().includes("/quantevolver/models?") &&
        searchParamEquals(r.url(), goldenModelName) &&
        r.request().method() === "GET",
    ),
    page.getByTestId("qe-model-search").fill(goldenModelName),
  ]);
  const row = page
    .locator('[data-testid^="qe-model-row-"]')
    .filter({ hasText: model.display_name || model.model_name || goldenModelName })
    .first();
  await expect(row, `golden model row ${goldenModelName}`).toBeVisible();
  const radio = page.getByTestId(`qe-model-radio-${model.model_id}`).first();
  await expect(radio).toBeVisible();
  await expect(radio).toBeEnabled();
  await radio.check();
  await expect(radio).toBeChecked();
  await expect(page.getByTestId("qe-step-next-model")).toBeEnabled();
}

async function fillShortWindow(page: Page): Promise<void> {
  const split = {
    train_start: process.env.QE_E2E_TRAIN_START || "2024-01-01",
    train_end: process.env.QE_E2E_TRAIN_END || "2024-12-31",
    valid_start: process.env.QE_E2E_VALID_START || "2025-01-01",
    valid_end: process.env.QE_E2E_VALID_END || "2025-06-30",
    test_start: process.env.QE_E2E_TEST_START || "2025-07-01",
    test_end: process.env.QE_E2E_TEST_END || "2025-12-31",
  };
  for (const [key, value] of Object.entries(split)) {
    await page.getByTestId(`qe-date-${key}`).fill(value);
  }
  const quickTrain = page.getByTestId("qe-quick-train");
  if (!(await quickTrain.isChecked())) {
    await quickTrain.check();
  }
}

async function createExperimentFromUi(
  page: Page,
  factor: FactorRow,
  model: ModelRow,
  horizon: 1 | 3 | 5 | 10 | 20,
): Promise<string> {
  await page.goto("/quantevolver/compose");
  await expect(page.getByRole("heading", { name: /因子选择/ })).toBeVisible();
  await selectFactor(page, factor);
  await page.getByTestId("qe-step-next-factors").click();

  await expect(page.getByRole("heading", { name: /模型选择/ })).toBeVisible();
  await selectGoldenModel(page, model);
  await page.getByTestId("qe-step-next-model").click();

  await page.getByTestId("qe-topk").fill("20");
  await page.getByTestId("qe-n-drop").fill("2");
  await page.getByTestId("qe-hold-thresh").fill(String(horizon));
  await page.getByTestId("qe-step-next-strategy").click();
  await page.getByTestId("qe-step-next-review").click();

  await fillShortWindow(page);
  await page.getByTestId(`qe-label-horizon-${horizon}`).click();
  await page.getByTestId("qe-sync-hold-thresh").click();

  const requestPromise = page.waitForRequest(
    (r) => r.url().includes("/quantevolver/config/generate") && r.method() === "POST",
  );
  const responsePromise = page.waitForResponse(
    (r) => r.url().includes("/quantevolver/config/generate") && r.request().method() === "POST",
  );
  await page.getByTestId("qe-generate-config").click();
  const req = await requestPromise;
  const payload = req.postDataJSON() as any;
  expect(payload.model_id).toBe(model.model_id);
  expect(payload.factor_names).toContain(factor.factor_name);
  expect(payload.custom_params?.quick_train).toBe(true);
  expect(payload.custom_params?.hold_thresh).toBe(horizon);
  if (horizon === 1) {
    expect(payload.custom_params?.label_horizon ?? 1).toBe(1);
  } else {
    expect(payload.custom_params?.label_horizon).toBe(horizon);
  }

  const response = await responsePromise;
  const body = await expectOk(response, `generate ${horizon}d experiment`);
  expect(body.ok).toBeTruthy();
  expect(body.experiment_id).toBeTruthy();
  await expect(page.getByText(`Experiment ID: ${body.experiment_id}`).first()).toBeVisible();
  return String(body.experiment_id);
}

function hasMetricKey(payload: any, candidates: string[]): boolean {
  const keys = new Set<string>();
  const visit = (value: any): void => {
    if (!value || typeof value !== "object") return;
    for (const [key, child] of Object.entries(value)) {
      keys.add(key.toLowerCase());
      visit(child);
    }
  };
  visit(payload);
  return candidates.some((candidate) => {
    const normalized = candidate.toLowerCase();
    return [...keys].some((key) => key === normalized || key.endsWith(`.${normalized}`));
  });
}

function objectFromJsonField(value: any): Record<string, any> {
  if (!value) return {};
  if (typeof value === "string") return JSON.parse(value);
  return value;
}

async function pollCompleted(request: APIRequestContext, experimentId: string): Promise<any> {
  const deadline = Date.now() + runTimeoutMs;
  let last: any = null;
  while (Date.now() < deadline) {
    const response = await request.get(endpoint(`/quantevolver/experiments/${experimentId}/run-status`));
    last = await jsonFrom(response);
    if (last.status === "completed") return last;
    if (["failed", "timeout", "interrupted"].includes(last.status)) {
      throw new Error(`Experiment ${experimentId} terminal status ${last.status}: ${JSON.stringify(last).slice(0, 2000)}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 10_000));
  }
  throw new Error(`Experiment ${experimentId} did not complete before timeout. Last=${JSON.stringify(last).slice(0, 2000)}`);
}

async function runAndAssertMetrics(page: Page, request: APIRequestContext, experimentId: string): Promise<void> {
  const runResponsePromise = page.waitForResponse(
    (r) => r.url().includes(`/quantevolver/experiments/${experimentId}/run`) && r.request().method() === "POST",
    { timeout: 60_000 },
  );
  await page.getByTestId("qe-run-backtest").click();
  const runResponse = await runResponsePromise;
  const runBody = await expectOk(runResponse, `submit run ${experimentId}`);
  expect(runBody.ok).toBeTruthy();

  const status = await pollCompleted(request, experimentId);
  const enhancedResponse = await request.get(endpoint(`/quantevolver/experiments/${experimentId}/enhanced-metrics`));
  const enhanced = await expectOk(enhancedResponse, `enhanced metrics ${experimentId}`);
  const combined = { status, enhanced };
  expect(hasMetricKey(combined, ["ic", "rank_ic", "rank_ic_series", "rank_ic_mean"])).toBeTruthy();
  expect(hasMetricKey(combined, ["icir", "rank_icir"])).toBeTruthy();
  expect(hasMetricKey(combined, ["annualized_return", "ann_return_no_cost", "top_annual_return"])).toBeTruthy();
  expect(hasMetricKey(combined, ["max_drawdown", "max_drawdown_no_cost", "top_max_drawdown"])).toBeTruthy();
  expect(hasMetricKey(combined, ["avg_turnover", "turnover", "annualized_turnover", "total_trading_days"])).toBeTruthy();
}

test.describe("QE label_horizon UI flow", () => {
  test("creates and runs single-alpha 1d/5d experiments from UI with golden LGBModel", async ({ page, request }) => {
    test.setTimeout(runBacktest ? runTimeoutMs * 2 + 10 * 60_000 : 10 * 60_000);
    const model = await findGoldenModel(request);
    const factor = await findTestFactor(request);

    const horizons = (process.env.QE_E2E_HORIZONS || "1,5")
      .split(",")
      .map((v) => Number(v.trim()))
      .filter((v): v is 1 | 3 | 5 | 10 | 20 => [1, 3, 5, 10, 20].includes(v));

    for (const horizon of horizons) {
      const experimentId = await createExperimentFromUi(page, factor, model, horizon);
      const detail = await expectOk(
        await request.get(endpoint(`/quantevolver/experiments/${experimentId}`)),
        `experiment detail ${experimentId}`,
      );
      const customParams = objectFromJsonField(detail.experiment?.custom_params);
      expect(customParams.quick_train).toBe(true);
      expect(customParams.hold_thresh).toBe(horizon);
      expect(customParams.label_horizon ?? 1).toBe(horizon);
      if (runBacktest) {
        await runAndAssertMetrics(page, request, experimentId);
      }
    }
  });

  test("rejects invalid label_horizon without silently falling back to 1d", async ({ request }) => {
    const model = await findGoldenModel(request);
    const factor = await findTestFactor(request);
    const response = await request.post(endpoint("/quantevolver/config/generate"), {
      data: {
        factor_names: [factor.factor_name],
        model_id: model.model_id,
        data_split: {
          train_start: "2024-01-01",
          train_end: "2024-12-31",
          valid_start: "2025-01-01",
          valid_end: "2025-06-30",
          test_start: "2025-07-01",
          test_end: "2025-12-31",
        },
        custom_params: {
          quick_train: true,
          label_type: "close",
          label_horizon: 2,
        },
      },
    });
    expect(response.status()).toBe(400);
    const body = await jsonFrom(response);
    expect(JSON.stringify(body)).toContain("label_horizon");
  });
});
