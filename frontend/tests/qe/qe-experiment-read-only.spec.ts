import { expect, test } from "@playwright/test";

const apiBase = process.env.QE_API_BASE || process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8011/api/v1";
const preferredExperimentId = process.env.QE_READ_EXPERIMENT_ID;

type PickedExperiment = {
  experiment: any;
  enhanced: any;
};

function parseMaybeJson(value: any): any {
  if (!value) return value;
  if (typeof value === "string") {
    try { return JSON.parse(value); } catch { return null; }
  }
  return value;
}

function hasData(value: any): boolean {
  return value != null && (!Array.isArray(value) || value.length > 0) && !(typeof value === "object" && !Array.isArray(value) && Object.keys(value).length === 0);
}

function hasEnhancedData(value: any): boolean {
  const obj = parseMaybeJson(value);
  if (!obj || typeof obj !== "object") return false;
  const data = obj.enhanced_metrics && typeof obj.enhanced_metrics === "object" ? obj.enhanced_metrics : obj;
  return [
    "dates", "ic_series", "rank_ic_series", "return_dates",
    "cumulative_excess_with_cost", "cumulative_excess_no_cost",
    "all_stocks", "top_stocks", "bottom_stocks", "factor_analysis",
    "absolute_returns", "summary",
  ].some(key => hasData(data[key])) || hasData(data.ic_diagnostics) || hasData(data.return_curves);
}

function firstMetric(...values: any[]): number | null {
  for (const value of values) {
    if (value == null) continue;
    const parsed = typeof value === "number" ? value : Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

async function fetchExperiment(request: any, experimentId: string): Promise<any> {
  const resp = await request.get(`${apiBase}/quantevolver/experiments/${experimentId}`);
  expect(resp.ok(), `experiment detail failed for ${experimentId}: ${resp.status()} ${await resp.text()}`).toBeTruthy();
  const payload = await resp.json();
  return payload.data || payload.experiment || payload;
}

async function fetchEnhanced(request: any, experimentId: string): Promise<any | null> {
  const resp = await request.get(`${apiBase}/quantevolver/experiments/${experimentId}/enhanced-metrics`);
  if (!resp.ok()) return null;
  const payload = await resp.json();
  return payload.data || payload;
}

async function pickExperimentWithEnhancedData(request: any): Promise<PickedExperiment> {
  if (preferredExperimentId) {
    const experiment = await fetchExperiment(request, preferredExperimentId);
    const enhanced = await fetchEnhanced(request, preferredExperimentId);
    expect(enhanced && hasEnhancedData(enhanced), `preferred experiment ${preferredExperimentId} must expose enhanced metrics`).toBeTruthy();
    return { experiment, enhanced };
  }

  const listResp = await request.get(`${apiBase}/quantevolver/experiments?limit=80&offset=0&include_children=true`);
  expect(listResp.ok(), `experiment list failed: ${listResp.status()} ${await listResp.text()}`).toBeTruthy();
  const listPayload = await listResp.json();
  const items: any[] = Array.isArray(listPayload.items) ? listPayload.items : (listPayload.data || []);
  expect(items.length, "QE experiment list must contain historical rows").toBeGreaterThan(0);

  const likely = items.filter(item => hasEnhancedData(item.result_metrics)).slice(0, 10);
  const candidates = likely.length ? likely : items.slice(0, 8);
  for (const item of candidates) {
    const experimentId = item.experiment_id;
    if (!experimentId) continue;
    const enhanced = await fetchEnhanced(request, experimentId);
    if (enhanced && hasEnhancedData(enhanced)) {
      const experiment = await fetchExperiment(request, experimentId);
      return { experiment, enhanced };
    }
  }
  throw new Error("No QE experiment with displayable enhanced metrics was found for UI validation");
}

test("QE experiment detail reads accurate enhanced data through experiment API only", async ({ page, request }) => {
  const failures: string[] = [];
  const guessedFallbackRequests: string[] = [];

  page.on("pageerror", error => failures.push(`pageerror: ${error.message}`));
  page.on("console", message => {
    if (message.type() === "error") failures.push(`console error: ${message.text()}`);
  });
  page.on("requestfailed", req => failures.push(`request failed: ${req.method()} ${req.url()} ${req.failure()?.errorText || "unknown"}`));
  page.on("request", req => {
    if (req.url().includes("/quantevolver/evolution/tasks/") && req.url().includes("/enhanced-metrics")) {
      guessedFallbackRequests.push(req.url());
    }
  });
  page.on("response", resp => {
    const url = resp.url();
    if (url.includes("/api/") && resp.status() >= 400) failures.push(`api ${resp.status()}: ${url}`);
  });

  const { experiment, enhanced } = await pickExperimentWithEnhancedData(request);
  const experimentId = experiment.experiment_id;
  expect(experimentId, "picked experiment must have experiment_id").toBeTruthy();

  await page.goto(`/quantevolver/experiments/${experimentId}`);
  await expect(page.getByText(experiment.experiment_name || experimentId).first()).toBeVisible({ timeout: 120_000 });
  await expect(page.getByText(String(experiment.status)).first()).toBeVisible({ timeout: 60_000 });

  const summary = enhanced.summary || {};
  const expectedIc = firstMetric(experiment.ic, summary.ic, summary.IC);
  if (expectedIc != null) {
    await expect(page.getByText(expectedIc.toFixed(4)).first()).toBeVisible({ timeout: 60_000 });
  }

  const expectedRankIc = firstMetric(experiment.rank_ic, summary.rank_ic, summary.Rank_IC);
  if (expectedRankIc != null) {
    await expect(page.getByText(expectedRankIc.toFixed(4)).first()).toBeVisible({ timeout: 60_000 });
  }

  const allStocks = Array.isArray(enhanced.all_stocks) ? enhanced.all_stocks : [];
  if (allStocks.length > 0 && allStocks[0]?.code) {
    await expect(page.getByText(String(allStocks[0].code)).first()).toBeVisible({ timeout: 60_000 });
  }

  const icSeries = Array.isArray(enhanced.ic_series) ? enhanced.ic_series : [];
  if (icSeries.length > 0) {
    await expect(page.getByText("IC 诊断").first()).toBeVisible({ timeout: 60_000 });
  }

  expect(guessedFallbackRequests, "detail page must not guess evolution task/Loop1 enhanced-metrics URL").toEqual([]);
  expect(failures).toEqual([]);
});

test("QE terminal log UI shows node log tail without local workspace wording", async ({ page }) => {
  const mockExperimentId = "qe_mock_terminal_node_tail";
  const failures: string[] = [];

  page.on("pageerror", error => failures.push(`pageerror: ${error.message}`));
  page.on("console", message => {
    if (message.type() === "error") failures.push(`console error: ${message.text()}`);
  });
  page.on("requestfailed", req => failures.push(`request failed: ${req.method()} ${req.url()} ${req.failure()?.errorText || "unknown"}`));
  page.on("response", resp => {
    const url = resp.url();
    if (url.includes("/api/") && resp.status() >= 400) failures.push(`api ${resp.status()}: ${url}`);
  });

  await page.route(/\/api\/v1\/quantevolver\/experiments\?.*/, async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        total: 1,
        items: [{
          experiment_id: mockExperimentId,
          experiment_name: "Mock terminal node-tail experiment",
          status: "running",
          factor_names: ["mock_factor"],
          model_id: "mock_model",
          strategy_id: "mock_strategy",
          result_metrics: { IC: 0.123456 },
          ic: 0.123456,
          custom_params: { execution_node_id: "node-a" },
          created_at: "2026-05-02T09:00:00+08:00",
          updated_at: "2026-05-02T09:05:00+08:00",
        }],
      }),
    });
  });
  await page.route(new RegExp(`/api/v1/quantevolver/experiments/${mockExperimentId}/run-status$`), async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ status: "completed", result_metrics: { IC: 0.123456 } }),
    });
  });
  await page.route(new RegExp(`/api/v1/quantevolver/experiments/${mockExperimentId}/logs/tail.*`), async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "success",
        data: {
          experiment_id: mockExperimentId,
          experiment_status: "completed",
          terminal: true,
          log_source: "qe_workspace_api",
          node_id: "node-a",
          logs: ["node tail line 1", "node tail line 2"],
        },
      }),
    });
  });

  await page.goto("/quantevolver/experiments");
  await expect(page.getByText(mockExperimentId).first()).toBeVisible({ timeout: 120_000 });
  await expect(page.getByText("node tail line 1").first()).toBeVisible({ timeout: 60_000 });
  await expect(page.getByText("QE 节点日志尾部").first()).toBeVisible({ timeout: 60_000 });
  await expect(page.getByText(/本地\s*run\.log|本地日志/)).toHaveCount(0);

  expect(failures).toEqual([]);
});
