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

let ensuredPackages: PackageSummary[] = [];

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

async function openSection(page: Page, heading: string) {
  return page.locator("section").filter({ has: page.getByRole("heading", { name: heading }) });
}

test.describe.serial("Paper Trading v2 UI real-backend validation", () => {
  test.beforeAll(async ({ request }) => {
    const health = await request.get(`${API_BASE.replace(/\/api\/v1$/, "")}/openapi.json`);
    expect(health.ok(), `temporary backend must be reachable at ${API_BASE}`).toBeTruthy();
    ensuredPackages = [];
    for (const experimentId of QE_EXPERIMENTS) {
      ensuredPackages.push(await ensurePackageFromExperiment(request, experimentId));
    }
  });

  test("StrategyPackage 页面中文化、指标展示和模拟盘就绪 fail-fast", async ({ page }) => {
    await page.goto("/paper-v2/packages");
    await expect(page.getByRole("heading", { name: "模拟盘 v2" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "从 QE 创建策略包" })).toBeVisible();

    const sourceSection = await openSection(page, "从 QE 创建策略包");
    await sourceSection.getByPlaceholder("qe_20260416_002701").fill(QE_EXPERIMENTS[0]);
    await sourceSection.getByRole("button", { name: "预览实验 Manifest" }).click();
    await expect(sourceSection.locator(".pv2-json")).toContainText(QE_EXPERIMENTS[0]);

    await sourceSection.getByRole("button", { name: "验证模拟盘就绪度" }).click();
    await expect(page.getByText("策略包操作失败")).toBeVisible();
    await expect(page.locator("body")).toContainText(/EXECUTION_ALGO_ERROR|DATA_UNAVAILABLE|V24_PLAN/);

    await expect(page.getByRole("heading", { name: "StrategyPackage 策略包中心" })).toBeVisible();
    for (const experimentId of QE_EXPERIMENTS) {
      await expect(page.getByText(experimentId).first()).toBeVisible();
    }
    await expect(page.locator("body")).toContainText("RankIC");
    await expect(page.locator("body")).toContainText("最大回撤");
    await expect(page.locator("body")).toContainText(/SELECTION_ENABLED|BACKTEST_APPROVED/);
  });

  test("Selection Center 透传缺少 selection_runtime 的严格错误，不伪造空选股成功", async ({ page }) => {
    const target = ensuredPackages[0];
    await page.goto("/paper-v2/selection");
    await expect(page.getByRole("heading", { name: "选股控制" })).toBeVisible();
    await page.locator("select").first().selectOption("single_package");
    await page.locator('input[type="date"]').first().fill(REPLAY_TRADE_DATE);

    const picker = await openSection(page, "策略包选择器");
    const checkboxes = picker.locator('tbody input[type="checkbox"]');
    for (let i = 0; i < await checkboxes.count(); i += 1) {
      const box = checkboxes.nth(i);
      if (await box.isChecked()) await box.uncheck();
    }
    const targetRow = picker.locator("tbody tr").filter({ hasText: target.package_name });
    await expect(targetRow).toBeVisible();
    await targetRow.locator('input[type="checkbox"]').check();

    await page.getByRole("button", { name: "运行选股" }).click();
    await expect(page.getByText("选股操作失败")).toBeVisible();
    await expect(page.locator("body")).toContainText(/selection_runtime|DATA_UNAVAILABLE|strategy package is missing/);
    await expect(page.getByText("运行选股后查看排序候选股。")) .toBeVisible();
  });

  test("多策略包聚合入口只允许研究选股，未完成单包运行时前端阻止伪聚合", async ({ page }) => {
    await page.goto("/paper-v2/selection");
    await page.locator("select").first().selectOption("weighted_fusion");
    await expect(page.getByText("多策略包聚合目前仅支持研究选股")).toBeVisible();
    await page.getByRole("button", { name: "聚合已选运行" }).click();
    await expect(page.getByText("选股操作失败")).toBeVisible();
    await expect(page.locator("body")).toContainText("请至少选择两个已完成的选股运行进行聚合");
  });

  test("组合创建在 V24 运行时不可用时 fail-fast，不创建假组合", async ({ page }) => {
    const target = ensuredPackages[0];
    await page.goto("/paper-v2/portfolios");
    await expect(page.getByRole("heading", { name: "创建模拟盘 v2 组合" })).toBeVisible();
    const createSection = await openSection(page, "创建模拟盘 v2 组合");
    await createSection.locator("select").first().selectOption(target.package_id);
    await createSection.locator("input.pv2-input").first().fill(`E2E-${Date.now()}`);
    await createSection.locator('input[type="date"]').first().fill(REPLAY_TRADE_DATE);
    await createSection.getByRole("button", { name: "创建冻结组合" }).click();
    await expect(page.getByText("组合操作失败")).toBeVisible();
    await expect(page.locator("body")).toContainText(/V24_PLAN|DATA_UNAVAILABLE|EXECUTION_ALGO_ERROR|model_path/);
    await expect(page.getByText("created_portfolio_id")).toHaveCount(0);
  });

  test("模型与 HMM 页面可加载配置，滚动训练保持人工触发", async ({ page }) => {
    await page.goto("/paper-v2/model-hmm");
    await expect(page.getByRole("link", { name: "模型与 HMM" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "StrategyPackage 模型新鲜度" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "HMM 滚动训练" })).toBeVisible();
    await expect(page.locator("body")).toContainText(/滚动训练|HMM|模型/);
    await expect(page.getByRole("button", { name: /预览|刷新|触发/ }).first()).toBeVisible();
  });

  test("API 负向用例返回结构化错误，TDX 分钟线接口可访问", async ({ request }) => {
    const missing = await apiJson(request, "/strategy-packages/from-qe-experiment/not_exists_for_failfast/manifest");
    expect(missing.response.status()).toBe(404);
    expect(missing.payload.detail.error_code).toBe("DATA_UNAVAILABLE");

    const readiness = await apiJson(request, `/strategy-packages/from-qe-experiment/${QE_EXPERIMENTS[0]}/paper-readiness`);
    expect(readiness.response.ok()).toBeFalsy();
    expect(JSON.stringify(readiness.payload)).toMatch(/V24_PLAN|EXECUTION_ALGO_ERROR|DATA_UNAVAILABLE/);

    const selection = await apiJson(request, "/selection-center/runs", {
      method: "POST",
      data: {
        package_ids: [ensuredPackages[0].package_id],
        trade_date: REPLAY_TRADE_DATE,
        data_source: "DB_HISTORICAL",
        mode: "single_package",
        runtime_config: {
          runtime_profile: {
            selection: { top_k: 50 },
            tradability: { exclude_suspended: true },
            industry_blacklist: [],
            hmm: { enabled: false },
          },
        },
      },
    });
    expect(selection.response.ok()).toBeFalsy();
    expect(JSON.stringify(selection.payload)).toContain("selection_runtime");

    const tdx = await request.get(`${TDX_BASE}/api/kline-all/tdx?code=SZ000001&type=minute1`, { timeout: 30_000 });
    expect(tdx.ok(), `TDX backend must respond at ${TDX_BASE}`).toBeTruthy();
    const payload = await tdx.json();
    expect(payload.code).toBe(0);
    const bars = payload.data?.list || [];
    expect(Array.isArray(bars)).toBeTruthy();
    expect(bars.length, "TDX minute endpoint should return real minute bars instead of a fake default").toBeGreaterThan(0);
  });
});
