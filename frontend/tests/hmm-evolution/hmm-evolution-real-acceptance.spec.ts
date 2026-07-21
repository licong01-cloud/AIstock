import { expect, test, type Page, type APIRequestContext } from "@playwright/test";

/**
 * HMM Evolution Phase 1 — REAL UI acceptance (任务C, 2026-07-22).
 *
 * Unlike hmm-evolution.spec.ts (route-mocked contract tests), this suite runs
 * against the REAL DEV stack:
 *   backend  http://127.0.0.1:8011 (aistock_dev, seeded benchmark reference data)
 *   frontend http://127.0.0.1:3011 (next dev, started by playwright webServer)
 *
 * Hard guards:
 *   - any request to production ports (8001/3000/19080) fails the test;
 *   - no route mocking anywhere in this file;
 *   - every scenario archives a full-page screenshot under
 *     repo tmp/hmm_ui_acceptance_20260722/.
 *
 * Enable with HMM_EVOLUTION_REAL_ACCEPTANCE=1 (skipped by default so generic
 * CI never runs environment-bound acceptance).
 */

const ENABLED = process.env.HMM_EVOLUTION_REAL_ACCEPTANCE === "1";
const API_BASE = (process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8011/api/v1").replace(/\/+$/, "");
const SHOT_DIR = process.env.HMM_ACCEPTANCE_SHOT_DIR || "../tmp/hmm_ui_acceptance_20260722";

test.skip(!ENABLED, "Set HMM_EVOLUTION_REAL_ACCEPTANCE=1 with the real DEV stack on 8011/3011.");

const PROD_PORT_RE = /127\.0\.0\.1:(8001|3000|19080)\b|localhost:(8001|3000|19080)\b/;

async function guardProductionPorts(page: Page) {
  page.on("request", (request) => {
    if (PROD_PORT_RE.test(request.url())) {
      throw new Error(`REFUSING production-port request from UI: ${request.url()}`);
    }
  });
}

async function shot(page: Page, name: string) {
  await page.screenshot({ path: `${SHOT_DIR}/${name}.png`, fullPage: true });
}

/** Nearest ancestor <section> of a panel heading — CSS-module safe. */
function sectionPanel(page: Page, title: string) {
  return page.getByRole("heading", { name: title, exact: true }).locator("xpath=ancestor::section[1]");
}

/** div.panel wrapping an h2.panelTitle (structure: div.panel > div.panelHeader > h2). */
function subPanel(page: Page, title: string) {
  return page.getByRole("heading", { name: title, exact: true }).locator("xpath=../..");
}

type BatchSummary = {
  batch_id: string;
  status: string;
  candidate_count: number;
  retry_generation: number;
};
type BatchItem = {
  eval_id: string;
  candidate_id: string;
  candidate_display_name: string;
  evaluation_status: string;
  evidence_quality: string | null;
};
type BatchDetail = BatchSummary & { items: BatchItem[] };
type CandidateRecord = { candidate_id: string; display_name: string; lifecycle_status: string };
type EvaluationDetail = {
  eval_id: string;
  label_horizon_days: number;
  trading_days_count: number;
  base_loop_ref: string;
  status: string;
};

async function apiGet<T>(request: APIRequestContext, path: string): Promise<T> {
  const resp = await request.get(`${API_BASE}${path}`);
  expect(resp.status(), `GET ${path}`).toBe(200);
  const body = await resp.json();
  return (body && typeof body === "object" && "data" in body ? body.data : body) as T;
}

let candidates: CandidateRecord[] = [];
let completed10Batch: BatchSummary | undefined;
let failedBatch: BatchSummary | undefined;
let retryBatch: BatchSummary | undefined;
let canonicalEvalId = "";
let canonicalTradingDays = 0;
let fallbackEvalId = "";

test.beforeAll(async ({ request }) => {
  candidates = await apiGet<CandidateRecord[]>(request, "/hmm-evolution/candidates?limit=50");
  const batches = await apiGet<BatchSummary[]>(request, "/hmm-evolution/batches?limit=50");

  const tenCandidate = batches.filter((b) => b.candidate_count === 10 && b.status === "completed");
  completed10Batch = tenCandidate[tenCandidate.length - 1];
  if (completed10Batch) {
    const detail = await apiGet<BatchDetail>(request, `/hmm-evolution/batches/${completed10Batch.batch_id}`);
    const succeeded = detail.items.find((item) => item.evaluation_status === "succeeded");
    canonicalEvalId = succeeded?.eval_id || "";
    if (canonicalEvalId) {
      const evaluation = await apiGet<EvaluationDetail>(request, `/hmm-evolution/evaluations/${canonicalEvalId}`);
      canonicalTradingDays = evaluation.trading_days_count;
    }
  }
  failedBatch = batches.find((b) => b.status === "failed");
  retryBatch = batches.find((b) => b.status === "completed" && b.retry_generation >= 2);

  // The fallback (h10) evaluation: a succeeded single-candidate evaluation whose
  // detail carries label_horizon_days=10.  Resolved lazily through the API,
  // never hardcoded.
  const singleCompleted = batches.filter((b) => b.candidate_count === 1 && b.status === "completed");
  for (const batch of singleCompleted) {
    const detail = await apiGet<BatchDetail>(request, `/hmm-evolution/batches/${batch.batch_id}`);
    for (const item of detail.items) {
      if (item.evaluation_status !== "succeeded") continue;
      const evaluation = await apiGet<EvaluationDetail>(request, `/hmm-evolution/evaluations/${item.eval_id}`);
      if (evaluation.label_horizon_days === 10) {
        fallbackEvalId = item.eval_id;
        break;
      }
    }
    if (fallbackEvalId) break;
  }
});

test.describe("Page 1 — /hmm-evolution 演进实验室（真实 DEV 数据）", () => {
  test.beforeEach(async ({ page }) => {
    await guardProductionPorts(page);
  });

  test("S01 候选库渲染真实候选（内容寻址 ID/覆盖区间/SHA/lifecycle）", async ({ page }) => {
    expect(candidates.length).toBeGreaterThanOrEqual(10);
    await page.goto("/hmm-evolution");
    const library = sectionPanel(page, "候选库");
    await expect(library).toBeVisible();
    const first = candidates[0];
    await expect(library.getByText(first.display_name).first()).toBeVisible();
    await expect(library.getByText(first.candidate_id.slice(0, 8)).first()).toBeVisible();
    await expect(library.getByLabel("状态：研究候选").first()).toBeVisible();
    await expect(library.locator("tbody tr")).toHaveCount(candidates.length);
    await shot(page, "s01_candidate_library");
  });

  test("S02 概览指标卡为真实值（候选计数/共同水位/中位耗时非占位）", async ({ page }) => {
    test.skip(!completed10Batch, "no completed 10-candidate batch in DEV");
    await page.goto("/hmm-evolution");
    await page.getByLabel("选择评估批次").selectOption(completed10Batch!.batch_id);
    const grid = page.locator("section[aria-label='HMM 演进概览']");
    await expect(grid).toBeVisible();
    const cardCandidate = grid.locator("article").nth(0);
    await expect(cardCandidate).toContainText("当前加载候选");
    await expect(cardCandidate).toContainText(String(candidates.length));
    // 共同数据水位必须是真实 as-of 日期（YYYY-MM-DD），不是 "尚无批次证据"
    await expect(grid.locator("article").nth(2)).toContainText(/\d{4}-\d{2}-\d{2}/);
    // 中位耗时必须基于真实批次（Xm Ys），不是 "—"
    await expect(grid.locator("article").nth(3)).toContainText(/\d+m \d+s/);
    await shot(page, "s02_overview_metrics");
  });

  test("S03 导航边界：仅演进实验室激活，无风险/训练占位 tab", async ({ page }) => {
    await page.goto("/hmm-evolution");
    const nav = page.getByRole("navigation", { name: "HMM 研究模块导航" });
    await expect(nav).toBeVisible();
    await expect(nav.getByRole("link", { name: "演进实验室" })).toHaveAttribute("aria-current", "page");
    await expect(nav.getByText("板块风险")).toHaveCount(0);
    await expect(nav.getByText("滚动训练")).toHaveCount(0);
    // 不得存在 disabled 占位 tab
    await expect(nav.locator("[aria-disabled='true']")).toHaveCount(0);
    await shot(page, "s03_navigation_boundary");
  });

  test("S04 当前批次面板：completed 10 候选批次进度与步骤条", async ({ page }) => {
    test.skip(!completed10Batch, "no completed 10-candidate batch in DEV");
    await page.goto("/hmm-evolution");
    await page.getByLabel("选择评估批次").selectOption(completed10Batch!.batch_id);
    const panel = sectionPanel(page, "当前批次");
    await expect(panel.getByLabel("状态：已完成")).toBeVisible();
    await expect(panel.getByText("10 / 10 完成")).toBeVisible();
    await expect(panel.getByText("输入冻结")).toBeVisible();
    await expect(panel.getByText("评估计算")).toBeVisible();
    await expect(panel.getByText("证据归集")).toBeVisible();
    await expect(panel.getByText("研究推荐")).toBeVisible();
    // 终态批次不得出现取消按钮
    await expect(panel.getByRole("button", { name: /取消/ })).toHaveCount(0);
    await shot(page, "s04_current_batch_panel");
  });

  test("S05 候选排行榜：真实指标、行数=候选数、链接到真实评估", async ({ page }) => {
    test.skip(!completed10Batch, "no completed 10-candidate batch in DEV");
    await page.goto("/hmm-evolution");
    await page.getByLabel("选择评估批次").selectOption(completed10Batch!.batch_id);
    const ranking = sectionPanel(page, "候选排行榜");
    await expect(ranking.getByLabel("状态：已完成")).toBeVisible();
    await expect(ranking.locator("tbody tr")).toHaveCount(10);
    await expect(ranking.getByRole("columnheader", { name: "净标签收益" })).toBeVisible();
    await expect(ranking.getByRole("columnheader", { name: "DB 10D" })).toBeVisible();
    // 至少一行有真实排名（succeeded 指标非空）
    await expect(ranking.locator("tbody td", { hasText: /^\d+$/ }).first()).toBeVisible();
    const firstLink = ranking.locator("tbody a[href^='/hmm-evolution/evaluations/hmme_']").first();
    await expect(firstLink).toBeVisible();
    const href = await firstLink.getAttribute("href");
    expect(href).toMatch(/^\/hmm-evolution\/evaluations\/hmme_[0-9a-f]{32}$/);
    await shot(page, "s05_ranking_table");
  });

  test("S06 固定证据区：输入身份/数据水位/计算推荐三段真实证据", async ({ page }) => {
    test.skip(!completed10Batch, "no completed 10-candidate batch in DEV");
    await page.goto("/hmm-evolution");
    await page.getByLabel("选择评估批次").selectOption(completed10Batch!.batch_id);
    const evidence = sectionPanel(page, "固定证据区");
    await expect(evidence.getByRole("heading", { name: "输入身份", exact: true })).toBeVisible();
    await expect(evidence.getByText(completed10Batch!.batch_id)).toBeVisible();
    await expect(evidence.getByText("2025-01-02 → 2025-12-31")).toBeVisible();
    await expect(evidence.getByText("20 个交易日")).toBeVisible();
    await expect(evidence.getByRole("heading", { name: "数据水位与质量", exact: true })).toBeVisible();
    await expect(evidence.getByRole("heading", { name: "计算与推荐", exact: true })).toBeVisible();
    await expect(evidence.getByText("hmm_recommendation_v1")).toBeVisible();
    await expect(evidence.getByText("推荐供 QE 终审；无淘汰阈值")).toBeVisible();
    await shot(page, "s06_evidence_panel");
  });
});

test.describe("Page 2 — /hmm-evolution/batches/[batchId] 批次详情", () => {
  test.beforeEach(async ({ page }) => {
    await guardProductionPorts(page);
  });

  test("S07 10 候选批次详情：逐项状态全成功、计数真实", async ({ page }) => {
    test.skip(!completed10Batch, "no completed 10-candidate batch in DEV");
    await page.goto(`/hmm-evolution/batches/${completed10Batch!.batch_id}`);
    await expect(page.getByRole("heading", { name: `批次 ${completed10Batch!.batch_id}` })).toBeVisible();
    await expect(page.getByLabel("状态：已完成")).toBeVisible();
    const items = sectionPanel(page, "候选项目");
    await expect(items.locator("tbody tr")).toHaveCount(10);
    await expect(items.getByLabel("状态：已成功")).toHaveCount(10);
    // 失败原因列全部为 "无"
    const failCells = items.locator("tbody tr td:last-child");
    await expect(failCells).toHaveCount(10);
    for (const text of await failCells.allTextContents()) {
      expect(text.trim()).toBe("无");
    }
    await shot(page, "s07_batch_detail_10c");
  });

  test("S08 批次身份与推荐证据：durable state 时间真实、版本正确", async ({ page }) => {
    test.skip(!completed10Batch, "no completed 10-candidate batch in DEV");
    await page.goto(`/hmm-evolution/batches/${completed10Batch!.batch_id}`);
    const identity = subPanel(page, "批次身份");
    await expect(identity.getByText("Durable state")).toBeVisible();
    await expect(identity.getByText(completed10Batch!.batch_id)).toBeVisible();
    await expect(identity.getByText(/\d{4}年\d{1,2}月\d{1,2}日/).first()).toBeVisible();
    const recommendation = subPanel(page, "推荐证据");
    await expect(recommendation.getByText("hmm_recommendation_v1")).toBeVisible();
    await expect(recommendation.getByText("仅研究推荐；无生产替换或交易动作")).toBeVisible();
    await shot(page, "s08_batch_identity");
  });

  test("S09 失败批次：中文失败语义 + 仅重试失败项入口，不白屏", async ({ page }) => {
    test.skip(!failedBatch, "no failed batch in DEV");
    await page.goto(`/hmm-evolution/batches/${failedBatch!.batch_id}`);
    await expect(page.getByRole("heading", { name: `批次 ${failedBatch!.batch_id}` })).toBeVisible();
    await expect(page.getByLabel("状态：失败")).toBeVisible();
    await expect(page.getByRole("button", { name: "仅重试失败项" })).toBeVisible();
    // 失败不是空集合假成功：失败/超时指标卡存在
    await expect(page.getByText("失败 / 超时")).toBeVisible();
    await shot(page, "s09_failed_batch");
  });

  test("S10 retry 链证据：retry generation 在详情中显式呈现", async ({ page }) => {
    test.skip(!retryBatch, "no completed retry batch in DEV");
    await page.goto(`/hmm-evolution/batches/${retryBatch!.batch_id}`);
    await expect(page.getByText(`retry generation ${retryBatch!.retry_generation}`)).toBeVisible();
    await expect(page.getByLabel("状态：已完成")).toBeVisible();
    await shot(page, "s10_retry_generation");
  });

  test("S11 终态轮询停止：completed 批次加载后不再自动请求", async ({ page }) => {
    test.skip(!completed10Batch, "no completed 10-candidate batch in DEV");
    const batchRequests: string[] = [];
    page.on("request", (request) => {
      if (request.url().includes(`/api/v1/hmm-evolution/batches/${completed10Batch!.batch_id}`)) {
        batchRequests.push(request.url());
      }
    });
    await page.goto(`/hmm-evolution/batches/${completed10Batch!.batch_id}`);
    await expect(page.getByLabel("状态：已完成")).toBeVisible();
    const settled = batchRequests.length;
    expect(settled).toBeGreaterThan(0);
    await page.waitForTimeout(8_000); // POLL_FAST_MS=3s → 非终态至少会再请求 2 次
    expect(batchRequests.length, "terminal batch must stop polling").toBe(settled);
    await shot(page, "s11_terminal_polling_stopped");
  });

  test("S12 无 raw JSON dump、无抽屉式详情", async ({ page }) => {
    test.skip(!completed10Batch, "no completed 10-candidate batch in DEV");
    await page.goto(`/hmm-evolution/batches/${completed10Batch!.batch_id}`);
    await expect(page.getByLabel("状态：已完成")).toBeVisible();
    const bodyText = await page.locator("main[class*='hmm-research_page']").innerText();
    expect(bodyText).not.toContain("\"schema_version\"");
    expect(bodyText).not.toContain("\"artifact_manifest\"");
    expect(bodyText).not.toContain("\"stage_timings\"");
    await expect(page.locator("[role='dialog']")).toHaveCount(0);
    await expect(page.locator("pre")).toHaveCount(0);
    await shot(page, "s12_no_raw_json_no_drawer");
  });
});

test.describe("Page 3 — /hmm-evolution/evaluations/[evalId] 评估详情", () => {
  test.beforeEach(async ({ page }) => {
    await guardProductionPorts(page);
  });

  test("S13 canonical h20 评估：指标卡动态 horizon 与真实值", async ({ page }) => {
    test.skip(!canonicalEvalId, "no succeeded canonical evaluation in DEV");
    await page.goto(`/hmm-evolution/evaluations/${canonicalEvalId}`);
    const grid = page.locator("section[class*='metricsGrid']").first();
    await expect(grid.getByText("净标签收益 · 20 交易日", { exact: true })).toBeVisible();
    await expect(grid.getByText("Net DB 10D", { exact: true })).toBeVisible();
    await expect(grid.getByText("正值日比例", { exact: true })).toBeVisible();
    await expect(grid.getByText("覆盖率", { exact: true })).toBeVisible();
    // 真实百分比值（含正负号），非 "未计算"
    await expect(grid).toContainText(/[+-]\d+\.\d{2}%/);
    await shot(page, "s13_eval_metrics_h20");
  });

  test("S14 四证据区：输入身份/数据水位/计算版本/执行状态", async ({ page }) => {
    test.skip(!canonicalEvalId, "no succeeded canonical evaluation in DEV");
    await page.goto(`/hmm-evolution/evaluations/${canonicalEvalId}`);
    const identity = subPanel(page, "输入身份");
    await expect(identity.getByText("qe_20260705_004409_4437/Loop10")).toBeVisible();
    await expect(identity.getByText("2025-01-02 → 2025-12-31")).toBeVisible();
    await expect(identity.getByText("20 个交易日")).toBeVisible();
    const watermark = subPanel(page, "数据水位");
    await expect(watermark.getByText("交易日数")).toBeVisible();
    await expect(watermark.getByText(String(canonicalTradingDays))).toBeVisible();
    const version = subPanel(page, "计算版本");
    await expect(version.getByText("…").first()).toBeVisible(); // shortHash 形式
    await expect(version.getByText("有效", { exact: true })).toBeVisible();
    const state = subPanel(page, "执行状态");
    await expect(state.getByText("succeeded")).toBeVisible();
    await shot(page, "s14_eval_evidence_sections");
  });

  test("S15 逐日替换摘要：全部真实交易日逐行渲染", async ({ page }) => {
    test.skip(!canonicalEvalId || !canonicalTradingDays, "no succeeded canonical evaluation in DEV");
    await page.goto(`/hmm-evolution/evaluations/${canonicalEvalId}`);
    const panel = sectionPanel(page, "逐日替换摘要");
    await expect(panel).toBeVisible();
    await expect(panel.locator("tbody tr")).toHaveCount(canonicalTradingDays);
    // 窗口首日在逐日表中出现
    await expect(panel.getByText("2025-01-02").first()).toBeVisible();
    // 状态列为中文业务语义（无调整/完整计算/证据缺失至少出现其一）
    await expect(panel.locator("tbody td", { hasText: /当日无调整|已完整计算|证据缺失/ }).first()).toBeVisible();
    await shot(page, "s15_daily_summary");
  });

  test("S16 fallback h10 评估：标签 horizon 按 10 交易日展示", async ({ page }) => {
    test.skip(!fallbackEvalId, "no succeeded fallback (h10) evaluation in DEV");
    await page.goto(`/hmm-evolution/evaluations/${fallbackEvalId}`);
    await expect(page.getByText("净标签收益 · 10 交易日", { exact: true })).toBeVisible();
    const identity = subPanel(page, "输入身份");
    await expect(identity.getByText("qe_20260502_131502_9b54/Loop1")).toBeVisible();
    await expect(identity.getByText("10 个交易日")).toBeVisible();
    await shot(page, "s16_eval_fallback_h10");
  });

  test("S17 不存在的评估：VisibleErrorState（reason code/中文说明/重试条件）", async ({ page }) => {
    await page.goto("/hmm-evolution/evaluations/hmme_0000000000000000000000000000dead");
    // Next.js route announcer 也带 role=alert；按业务 reason code 过滤出 VisibleErrorState
    const alert = page.locator("div[role='alert']", { hasText: "hmm_evolution_evaluation_not_found" });
    await expect(alert).toBeVisible();
    await expect(alert.getByText("hmm_evolution_evaluation_not_found", { exact: true })).toBeVisible();
    await expect(alert.getByText("离线评估记录不存在。")).toBeVisible();
    await expect(alert.getByText("重试条件")).toBeVisible();
    await expect(alert.getByRole("button", { name: /重试/ })).toBeVisible();
    // 不得停留在无限 loading
    await expect(page.getByText("正在加载评估证据")).toHaveCount(0);
    await shot(page, "s17_eval_not_found_error_state");
  });

  test("S18 可访问性：导航 aria、按钮键盘可达、状态非颜色语义", async ({ page }) => {
    await page.goto("/hmm-evolution");
    await expect(page.getByRole("navigation", { name: "HMM 研究模块导航" })).toBeVisible();
    // 状态徽标使用 aria-label 文本语义，不仅靠颜色
    const badge = page.locator("[aria-label^='状态：']").first();
    await expect(badge).toBeVisible();
    // 主操作键盘可达：聚焦刷新按钮并回车触发
    const refresh = page.getByRole("button", { name: /刷新/ }).first();
    await refresh.focus();
    await expect(refresh).toBeFocused();
    await page.keyboard.press("Enter");
    // 触发后页面仍处于可用状态（候选库仍渲染）
    await expect(sectionPanel(page, "候选库")).toBeVisible();
    await shot(page, "s18_accessibility");
  });
});
