import { expect, test } from "@playwright/test";
import type { Page } from "@playwright/test";

// MOCK-first spec. Honours MARKET_REGIME_UI_MOCK_API so MOCK_API=0 in the nox
// session actually skips this spec rather than silently installing mocks.
const MOCK_API = process.env.MARKET_REGIME_UI_MOCK_API !== "0";
test.skip(!MOCK_API, "market-regime spec is mock-first; set MARKET_REGIME_UI_MOCK_API=1 (default) to run");

const SIMPLE_TIMELINE = {
  source_method: "simple_quadrant",
  items: [
    {
      trade_date: "2024-12-30",
      regime: "bull",
      regime_confidence: 0.82,
      source_method: "simple_quadrant",
      source_signal_json: { csi300_6m_ret: 0.15, csi300_60d_vol: 0.18 },
      labeled_at: "2024-12-30T16:00:00",
    },
    {
      trade_date: "2025-01-02",
      regime: "high_vol",
      regime_confidence: 0.61,
      source_method: "simple_quadrant",
      source_signal_json: { csi300_6m_ret: 0.04, csi300_60d_vol: 0.31 },
      labeled_at: "2025-01-02T16:00:00",
    },
    {
      trade_date: "2025-01-03",
      regime: "oscillation",
      regime_confidence: 0.55,
      source_method: "simple_quadrant",
      source_signal_json: { csi300_6m_ret: 0.01, csi300_60d_vol: 0.18 },
      labeled_at: "2025-01-03T16:00:00",
    },
  ],
};

const SIMPLE_DISTRIBUTION = {
  source_method: "simple_quadrant",
  start_date: null,
  end_date: null,
  total: 100,
  items: [
    { regime: "bull", count: 40, pct: 0.4 },
    { regime: "bear", count: 5, pct: 0.05 },
    { regime: "oscillation", count: 30, pct: 0.3 },
    { regime: "high_vol", count: 20, pct: 0.2 },
    { regime: "low_vol", count: 5, pct: 0.05 },
  ],
};

const HMM_DISTRIBUTION = {
  source_method: "hmm_viterbi",
  start_date: null,
  end_date: null,
  total: 0,
  items: [
    { regime: "bull", count: 0, pct: 0 },
    { regime: "bear", count: 0, pct: 0 },
    { regime: "oscillation", count: 0, pct: 0 },
    { regime: "high_vol", count: 0, pct: 0 },
    { regime: "low_vol", count: 0, pct: 0 },
  ],
};

type MockOptions = {
  methodsStatus?: number;
  timelineStatus?: number;
  available?: string[];
  timelineCallSpy?: { calls: URLSearchParams[] };
};

async function mockApi(page: Page, options: MockOptions = {}) {
  await page.route("**/api/v1/market/regime-label/**", async (route) => {
    const url = route.request().url();
    const respond = (data: unknown, status = 200) =>
      route.fulfill({ status, contentType: "application/json", body: JSON.stringify(data) });

    if (url.includes("/methods")) {
      if (options.methodsStatus && options.methodsStatus >= 400) {
        return respond({ detail: "methods_unavailable" }, options.methodsStatus);
      }
      return respond({
        supported: ["simple_quadrant", "hmm_viterbi", "bbq", "ensemble"],
        available: options.available ?? ["simple_quadrant"],
      });
    }

    const parsedUrl = new URL(url);
    const sourceMethod = parsedUrl.searchParams.get("source_method") ?? "simple_quadrant";

    if (url.includes("/timeline")) {
      if (options.timelineCallSpy) options.timelineCallSpy.calls.push(parsedUrl.searchParams);
      if (options.timelineStatus && options.timelineStatus >= 400) {
        return respond({ detail: "timeline_unavailable" }, options.timelineStatus);
      }
      if (sourceMethod !== "simple_quadrant") {
        return respond({ source_method: sourceMethod, items: [] });
      }
      return respond(SIMPLE_TIMELINE);
    }

    if (url.includes("/distribution")) {
      if (sourceMethod === "hmm_viterbi") return respond(HMM_DISTRIBUTION);
      return respond(SIMPLE_DISTRIBUTION);
    }

    if (url.includes("/current")) {
      if (sourceMethod !== "simple_quadrant") return respond(null);
      return respond(SIMPLE_TIMELINE.items[SIMPLE_TIMELINE.items.length - 1]);
    }

    return respond({ detail: "unexpected route" }, 404);
  });
}

test("market regime page loads timeline and distribution for simple_quadrant", async ({ page }) => {
  await mockApi(page);
  await page.goto("/market-regime");
  await expect(page.getByText("市场状态分类")).toBeVisible();
  await expect(page.getByTestId("timeline-regime-2024-12-30")).toContainText("牛市");
  await expect(page.getByTestId("dist-bull")).toContainText("40.0%");
  await expect(page.getByTestId("dist-high_vol")).toContainText("20.0%");
});

test("source_method switch triggers fresh timeline request and shows empty state", async ({ page }) => {
  const spy = { calls: [] as URLSearchParams[] };
  await mockApi(page, { timelineCallSpy: spy });
  await page.goto("/market-regime");
  await expect(page.getByTestId("dist-bull")).toContainText("40.0%");
  await page.getByTestId("source-method").selectOption("hmm_viterbi");
  await expect.poll(() => spy.calls.some((p) => p.get("source_method") === "hmm_viterbi")).toBe(true);
  await expect(page.getByTestId("distribution-empty")).toBeVisible();
});

test("date range filter forwards start_date and end_date", async ({ page }) => {
  const spy = { calls: [] as URLSearchParams[] };
  await mockApi(page, { timelineCallSpy: spy });
  await page.goto("/market-regime");
  await expect(page.getByText("市场状态分类")).toBeVisible();
  await page.getByTestId("start-date").fill("2024-01-01");
  await page.getByTestId("end-date").fill("2024-12-31");
  await expect
    .poll(() =>
      spy.calls.some(
        (p) => p.get("start_date") === "2024-01-01" && p.get("end_date") === "2024-12-31",
      ),
    )
    .toBe(true);
});

test("methods endpoint marks unavailable methods in the selector", async ({ page }) => {
  await mockApi(page, { available: ["simple_quadrant"] });
  await page.goto("/market-regime");
  await expect(page.getByText("市场状态分类")).toBeVisible();
  const select = page.getByTestId("source-method");
  await expect(select.locator('option[value="hmm_viterbi"]')).toContainText("无数据");
  await expect(select.locator('option[value="simple_quadrant"]')).not.toContainText("无数据");
});

test("methods API failure surfaces the error panel without crashing", async ({ page }) => {
  const pageErrors: string[] = [];
  page.on("pageerror", (error) => pageErrors.push(error.message));
  await mockApi(page, { methodsStatus: 500 });
  await page.goto("/market-regime");
  await expect(page.getByText("Market Regime API 调用失败")).toBeVisible();
  await expect(page.getByText("市场状态分类")).toBeVisible();
  expect(pageErrors).toEqual([]);
});
