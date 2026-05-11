import { expect, test } from "@playwright/test";

type CandidateOverrides = {
  candidates?: unknown[];
  page?: number;
  has_more?: boolean;
};

function mockArchiveBase(
  page: import("@playwright/test").Page,
  options: {
    summary?: Record<string, unknown>;
    summaryStatus?: number;
    outbox?: unknown[];
    jobs?: unknown[];
    candidates?: CandidateOverrides;
    candidateAssertions?: (params: URLSearchParams) => void;
    healthCallCounter?: { count: number };
  } = {},
) {
  return page.route("**/api/v1/qe-archive/**", async (route) => {
    const url = route.request().url();
    const respond = (data: unknown, status = 200) =>
      route.fulfill({ status, contentType: "application/json", body: JSON.stringify(data) });

    if (url.includes("/health")) {
      if (options.healthCallCounter) options.healthCallCounter.count += 1;
      if (options.summaryStatus && options.summaryStatus >= 400) {
        return respond({ detail: "boom" }, options.summaryStatus);
      }
      return respond({
        status: "success",
        data: options.summary ?? {
          run_count: 0,
          research_valid_counts: { true: 0, false: 0 },
          pending_outbox_count: 0,
          outbox_status_counts: {},
          archive_job_status_counts: {},
          latest_archived_at: null,
        },
      });
    }

    if (url.includes("/outbox")) {
      return respond({ status: "success", data: options.outbox ?? [] });
    }

    if (url.includes("/jobs")) {
      return respond({ status: "success", data: options.jobs ?? [] });
    }

    if (url.includes("/backfill-candidates")) {
      const parsedUrl = new URL(url);
      if (options.candidateAssertions) options.candidateAssertions(parsedUrl.searchParams);
      const overrides = options.candidates ?? {};
      const pageNumber = overrides.page ?? Number(parsedUrl.searchParams.get("page") || "1");
      const pageSize = Number(parsedUrl.searchParams.get("page_size") || "20");
      return respond({
        status: "success",
        data: {
          status: parsedUrl.searchParams.get("status") || "completed",
          include_archived: parsedUrl.searchParams.get("include_archived") === "true",
          page: pageNumber,
          page_size: pageSize,
          count: (overrides.candidates ?? []).length,
          has_more: overrides.has_more ?? false,
          candidates: overrides.candidates ?? [],
        },
      });
    }

    return respond({ status: "success", data: [] });
  });
}

test("dashboard renders empty state when warehouse has no records", async ({ page }) => {
  await mockArchiveBase(page);
  await page.goto("/qe-archive");
  await expect(page.getByText("QE Archive Warehouse")).toBeVisible();
  await expect(page.getByText("暂无可补录的 QE 实验")).toBeVisible();
  await expect(page.getByText("暂无 outbox 事件")).toBeVisible();
  await expect(page.getByText("暂无 worker job")).toBeVisible();
});

test("status filter selector forwards selection to backfill candidates API", async ({ page }) => {
  const seen: string[] = [];
  await mockArchiveBase(page, {
    candidateAssertions: (params) => {
      const status = params.get("status");
      if (status) seen.push(status);
    },
  });
  await page.goto("/qe-archive");
  await expect(page.getByText("QE Archive Warehouse")).toBeVisible();
  await page.locator("select.pv2-select").first().selectOption("terminal");
  await expect.poll(() => seen).toContain("terminal");
  await page.locator("select.pv2-select").first().selectOption("all");
  await expect.poll(() => seen).toContain("all");
});

test("include_archived toggle propagates to backfill candidates request", async ({ page }) => {
  const includeFlags: string[] = [];
  await mockArchiveBase(page, {
    candidateAssertions: (params) => {
      const value = params.get("include_archived");
      if (value !== null) includeFlags.push(value);
    },
  });
  await page.goto("/qe-archive");
  await expect(page.getByText("QE Archive Warehouse")).toBeVisible();
  await page.locator("select.pv2-select").nth(2).selectOption("yes");
  await expect.poll(() => includeFlags).toContain("true");
});

test("health API failure surfaces the error panel without crashing the page", async ({ page }) => {
  const pageErrors: string[] = [];
  page.on("pageerror", (error) => pageErrors.push(error.message));
  await mockArchiveBase(page, { summaryStatus: 500 });
  await page.goto("/qe-archive");
  await expect(page.getByText("QE Archive 操作失败")).toBeVisible();
  await expect(page.getByText("QE Archive Warehouse")).toBeVisible();
  expect(pageErrors).toEqual([]);
});

test("refresh button triggers a fresh health request", async ({ page }) => {
  const counter = { count: 0 };
  await mockArchiveBase(page, { healthCallCounter: counter });
  await page.goto("/qe-archive");
  await expect(page.getByText("QE Archive Warehouse")).toBeVisible();
  await expect.poll(() => counter.count).toBeGreaterThanOrEqual(1);
  const before = counter.count;
  await page.getByRole("button", { name: /刷新候选/ }).click();
  await expect.poll(() => counter.count).toBeGreaterThan(before);
});
