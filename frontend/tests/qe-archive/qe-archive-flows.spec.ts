import { expect, test } from "@playwright/test";

// MOCK-first spec. Honours QE_ARCHIVE_UI_MOCK_API so MOCK_API=0 in the nox
// session actually skips this spec rather than silently installing mocks.
// Live-mode assertions are TBD; the route fixtures below encode mock data.
const MOCK_API = process.env.QE_ARCHIVE_UI_MOCK_API !== "0";
test.skip(!MOCK_API, "qe-archive flows spec is mock-first; set QE_ARCHIVE_UI_MOCK_API=1 (default) to run");

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
    backfillRequests?: unknown[];
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

    if (url.endsWith("/api/v1/qe-archive/backfill")) {
      const payload = route.request().postDataJSON() as Record<string, any>;
      options.backfillRequests?.push(payload);
      return respond({
        status: "success",
        data: {
          dry_run: !payload.write,
          write_enabled: Boolean(payload.write),
          source: payload.source,
          status: payload.status,
          processed_count: (payload.loop_ids || []).length + (payload.task_ids || []).length + (payload.experiment_ids || []).length,
          results: [],
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
  await expect(page.getByText("暂无 outbox 事件").first()).toBeVisible();
  await expect(page.getByText("暂无 worker job").first()).toBeVisible();
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
  await page.locator("label").filter({ hasText: "已入库项" }).locator("select").selectOption("yes");
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

test("candidate task can expand loops and preview selected loop ids only", async ({ page }) => {
  const backfillRequests: unknown[] = [];
  await mockArchiveBase(page, {
    backfillRequests,
    candidates: {
      candidates: [
        {
          candidate_id: "task:qe_archive_task",
          candidate_type: "evolution_task",
          source: "task",
          task_id: "qe_archive_task",
          display_name: "QE Archive loop selection task",
          status: "completed",
          loop_count: 2,
          selected_run_count: 2,
          archived_run_count: 0,
          pending_run_count: 2,
          recommended_run_count: 1,
          is_fully_archived: false,
          loops: [
            {
              task_id: "qe_archive_task",
              loop_id: "qe_archive_task_Loop1",
              loop_index: 1,
              status: "completed",
              action_type: "baseline",
              archive_status: "recommended",
              eligible: true,
              recommended: true,
              IC: 0.12,
              annualized_return: 0.18,
            },
            {
              task_id: "qe_archive_task",
              loop_id: "qe_archive_task_Loop2",
              loop_index: 2,
              status: "completed",
              action_type: "variant",
              archive_status: "eligible",
              eligible: true,
              recommended: false,
              IC: 0.08,
              annualized_return: 0.11,
            },
          ],
        },
      ],
    },
  });

  await page.goto("/qe-archive");
  await expect(page.getByText("QE Archive loop selection task")).toBeVisible();
  await page.getByRole("button", { name: "展开 loop" }).click();
  await expect(page.getByText("推荐入仓").first()).toBeVisible();
  await page.getByRole("button", { name: "选推荐 loop" }).click();
  await page.getByRole("button", { name: /dry-run 预览选中项/ }).click();

  await expect.poll(() => backfillRequests.length).toBeGreaterThan(0);
  expect(backfillRequests[backfillRequests.length - 1]).toMatchObject({
    source: "all",
    task_ids: [],
    experiment_ids: [],
    loop_ids: ["qe_archive_task_Loop1"],
    write: false,
  });
});
