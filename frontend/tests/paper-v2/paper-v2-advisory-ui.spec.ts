import { expect, test, type Page, type Route } from "@playwright/test";

type JsonObject = Record<string, unknown>;

const PROGRAM_ID = "adv_codex_smoke_20260604";
const REVIEW_COUNT = 35;
const ACTIVE_SORT_COLUMNS = [
  "symbol",
  "status",
  "signal_date",
  "effective_entry_date",
  "entry_price",
  "entry_rank",
  "current_rank",
  "holding_trading_days",
  "return_bps",
  "max_drawdown_bps",
  "price_quality_status",
  "exit_reason",
];

const program = {
  program_id: PROGRAM_ID,
  program_name: "codex_smoke_20260604",
  status: "PAUSED",
  target_count: 20,
  package_mode: "single_package",
  package_ids: ["pkg_codex_smoke"],
  package_weights: { pkg_codex_smoke: 1 },
  review_policy: {},
  review_policy_sha256: "review_hash",
  entry_price_basis: "next_open_executable",
  exit_price_basis: "next_open_executable",
  review_schedule: { mode: "daily" },
  version: 1,
  enabled_since: "2026-06-04T11:16:00+08:00",
  last_review_status: "SUCCEEDED",
  latest_review_trade_date: "2026-06-03",
};

const activePool = [
  {
    episode_id: "episode_002",
    program_id: PROGRAM_ID,
    symbol: "000002.SZ",
    status: "ACTIVE",
    signal_date: "2026-05-29",
    effective_entry_date: "2026-06-01",
    entry_price: 12.34,
    entry_price_basis: "next_open_executable",
    entry_rank: 2,
    current_rank: 1,
    holding_trading_days: 3,
    return_bps: 350,
    max_drawdown_bps: -120,
    price_quality_status: "OK",
    exit_reason: null,
  },
  {
    episode_id: "episode_001",
    program_id: PROGRAM_ID,
    symbol: "000001.SZ",
    status: "ACTIVE",
    signal_date: "2026-05-28",
    effective_entry_date: "2026-05-29",
    entry_price: 10.2,
    entry_price_basis: "next_open_executable",
    entry_rank: 1,
    current_rank: 3,
    holding_trading_days: 4,
    return_bps: -120,
    max_drawdown_bps: -300,
    price_quality_status: "WARN",
    exit_reason: null,
  },
  {
    episode_id: "episode_003",
    program_id: PROGRAM_ID,
    symbol: "000003.SZ",
    status: "ACTIVE",
    signal_date: "2026-06-02",
    effective_entry_date: "2026-06-03",
    entry_price: 8.88,
    entry_price_basis: "next_open_executable",
    entry_rank: 3,
    current_rank: 2,
    holding_trading_days: 1,
    return_bps: 80,
    max_drawdown_bps: -40,
    price_quality_status: "OK",
    exit_reason: null,
  },
];

const reviews = Array.from({ length: REVIEW_COUNT }, (_, index) => ({
  symbol: `000${String(index + 1).padStart(3, "0")}.SZ`,
  action: index % 5 === 0 ? "EXIT" : "HOLD",
  reason_code: index % 5 === 0 ? "TAKE_PROFIT" : "KEEP_TOPK",
  review_status: "SUCCEEDED",
  trade_date: `2026-06-${String(3 - (index % 3)).padStart(2, "0")}`,
  episode_id: `episode_review_${index}`,
  rank: index + 1,
  score: 100 - index,
  return_bps: index % 2 === 0 ? 120 : -80,
}));

const returns = activePool.map((row, index) => ({
  ...row,
  status: index === 0 ? "EXITED" : row.status,
  exit_reason: index === 0 ? "TAKE_PROFIT" : null,
  is_win: row.return_bps > 0,
}));

function json(route: Route, data: unknown, status = 200) {
  return route.fulfill({
    status,
    contentType: "application/json",
    body: JSON.stringify(data),
  });
}

async function mockShellApis(page: Page) {
  await page.route("**/api/ingestion/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    return json(route, path.endsWith("/unack-count") ? { count: 0 } : { alerts: [] });
  });
}

async function mockAdvisoryApis(page: Page) {
  const calls: string[] = [];
  await page.route("**/api/v1/advisory/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname;
    const method = request.method();
    calls.push(`${method} ${path}${url.search}`);

    if (path.endsWith("/api/v1/advisory/programs") && method === "GET") {
      return json(route, { ok: true, programs: [program] });
    }
    if (path.endsWith("/api/v1/advisory/leaderboard") && method === "GET") {
      return json(route, {
        ok: true,
        leaderboard: [{
          ...program,
          status: "ENABLED",
          entered_episode_count: 42,
          active_count: 3,
          take_profit_count: 5,
          stop_loss_count: 2,
          win_rate: 0.64,
          avg_return_bps: 235,
          median_return_bps: 120,
          max_drawdown_bps: -310,
          avg_holding_days: 4.5,
        }],
      });
    }
    if (path.endsWith(`/api/v1/advisory/programs/${PROGRAM_ID}/active-pool`) && method === "GET") {
      return json(route, { ok: true, active_pool: activePool });
    }
    if (path.endsWith(`/api/v1/advisory/programs/${PROGRAM_ID}/reviews`) && method === "GET") {
      const limit = Number(url.searchParams.get("limit") || "20");
      const offset = Number(url.searchParams.get("offset") || "0");
      return json(route, {
        ok: true,
        reviews: reviews.slice(offset, offset + limit),
        total_count: reviews.length,
        limit,
        offset,
      });
    }
    if (path.endsWith(`/api/v1/advisory/programs/${PROGRAM_ID}/returns`) && method === "GET") {
      return json(route, { ok: true, returns, metrics: { win_rate: 0.64 } });
    }
    if (path.endsWith(`/api/v1/advisory/programs/${PROGRAM_ID}/enable`) && method === "POST") {
      return json(route, { ok: true, program: { ...program, status: "ENABLED" } });
    }
    if (path.endsWith("/api/v1/advisory/programs") && method === "POST") {
      return json(route, { ok: true, program: { ...program, status: "ENABLED" } });
    }
    if (path.endsWith(`/api/v1/advisory/programs/${PROGRAM_ID}/replay`) && method === "POST") {
      return json(route, { ok: true, replay: { replay_run: { status: "SUCCEEDED" }, summary: { win_rate: 0.64 } } });
    }
    if (path.endsWith("/api/v1/advisory/quality-report") && method === "POST") {
      return json(route, {
        ok: true,
        report: { report_type: "quality", sample_count: 1, min_bucket_size: 1, metrics: {}, buckets: [], warnings: [] },
      });
    }
    return json(route, { detail: `unexpected advisory route: ${method} ${path}` }, 404);
  });
  return calls;
}

async function activeSymbols(page: Page) {
  return page.getByTestId("advisory-active-cell-symbol").allTextContents();
}

test("Advisory page confirms enable, paginates reviews, sorts active pool, and hides raw payload editors", async ({ page }) => {
  const pageErrors: string[] = [];
  const consoleErrors: string[] = [];
  const badResponses: string[] = [];
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("response", (response) => {
    if (response.url().includes("/api/v1/advisory/") && response.status() >= 400) {
      badResponses.push(`${response.status()} ${response.url()}`);
    }
  });

  await mockShellApis(page);
  const calls = await mockAdvisoryApis(page);
  await page.goto("/paper-v2/advisory");

  await expect(page.getByRole("heading", { name: "运行中的荐股任务排行榜" })).toBeVisible();
  await expect(page.getByText("codex_smoke_20260604").first()).toBeVisible();
  await expect(page.getByText("高级候选")).toHaveCount(0);
  await expect(page.getByText("高级行情")).toHaveCount(0);
  await expect(page.locator("textarea")).toHaveCount(0);
  await expect(page.locator("body")).not.toContainText("JSON");

  await expect(page.getByTestId("advisory-review-page-size").locator("option")).toHaveText(["20", "50", "100"]);
  await expect(page.getByTestId("advisory-review-row")).toHaveCount(20);
  await expect(page.getByText(`第 1 / 2 页，共 ${REVIEW_COUNT} 条`)).toBeVisible();
  await page.getByRole("button", { name: "下一页" }).click();
  await expect(page.getByTestId("advisory-review-row")).toHaveCount(15);
  await expect(page.getByText(`第 2 / 2 页，共 ${REVIEW_COUNT} 条`)).toBeVisible();
  await page.getByTestId("advisory-review-page-size").selectOption("50");
  await expect(page.getByTestId("advisory-review-row")).toHaveCount(REVIEW_COUNT);
  await expect(page.getByText(`第 1 / 1 页，共 ${REVIEW_COUNT} 条`)).toBeVisible();

  for (const column of ACTIVE_SORT_COLUMNS) {
    await expect(page.getByTestId(`advisory-active-sort-${column}`)).toBeVisible();
  }
  await expect.poll(() => activeSymbols(page)).toEqual(["000002.SZ", "000001.SZ", "000003.SZ"]);
  await page.getByTestId("advisory-active-sort-symbol").click();
  await expect.poll(() => activeSymbols(page)).toEqual(["000001.SZ", "000002.SZ", "000003.SZ"]);
  await page.getByTestId("advisory-active-sort-symbol").click();
  await expect.poll(() => activeSymbols(page)).toEqual(["000003.SZ", "000002.SZ", "000001.SZ"]);
  await page.getByTestId("advisory-active-sort-symbol").click();
  await expect.poll(() => activeSymbols(page)).toEqual(["000002.SZ", "000001.SZ", "000003.SZ"]);

  const enableCallsBefore = calls.filter((entry) => entry.includes("/enable")).length;
  page.once("dialog", async (dialog) => {
    expect(dialog.message()).toContain("确认启用荐股任务");
    await dialog.dismiss();
  });
  await page.getByTestId(`advisory-enable-${PROGRAM_ID}`).click();
  await expect.poll(() => calls.filter((entry) => entry.includes("/enable")).length).toBe(enableCallsBefore);

  page.once("dialog", async (dialog) => {
    expect(dialog.message()).toContain("确认启用荐股任务");
    await dialog.accept();
  });
  await page.getByTestId(`advisory-enable-${PROGRAM_ID}`).click();
  await expect.poll(() => calls.filter((entry) => entry.includes("/enable")).length).toBe(enableCallsBefore + 1);

  page.once("dialog", async (dialog) => {
    expect(dialog.message()).toContain("确认创建并启用荐股任务");
    await dialog.accept();
  });
  await page.getByPlaceholder("strategy_package_id").fill("pkg_codex_smoke");
  await page.getByRole("button", { name: "创建并启用" }).click();
  await expect.poll(() => calls.filter((entry) => entry === "POST /api/v1/advisory/programs").length).toBe(1);

  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
  expect(badResponses).toEqual([]);
});
