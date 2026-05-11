import { expect, test } from "@playwright/test";

// MOCK-first spec. Honours QE_ARCHIVE_UI_MOCK_API so MOCK_API=0 in the nox
// session actually skips this spec rather than silently installing mocks.
// The qe-archive backend lives on origin/main so a live-mode spec is feasible
// in the future, but the assertions below encode mock fixtures.
const MOCK_API = process.env.QE_ARCHIVE_UI_MOCK_API !== "0";
test.skip(!MOCK_API, "qe-archive dashboard spec is mock-first; set QE_ARCHIVE_UI_MOCK_API=1 (default) to run");

test("QE archive dashboard uses mocked warehouse APIs", async ({ page }) => {
  const consoleErrors: string[] = [];
  const pageErrors: string[] = [];

  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("pageerror", (error) => pageErrors.push(error.message));

  await page.route("**/api/v1/qe-archive/**", async (route) => {
    const url = route.request().url();
    const method = route.request().method();
    const response = (data: unknown, status = 200) => route.fulfill({
      status,
      contentType: "application/json",
      body: JSON.stringify(data),
    });

    if (url.includes("/health")) {
      return response({
        status: "success",
        data: {
          run_count: 11,
          research_valid_counts: { true: 10, false: 1 },
          pending_outbox_count: 1,
          outbox_status_counts: { pending: 1, completed: 2 },
          archive_job_status_counts: { completed: 2 },
          latest_archived_at: "2026-05-02T20:00:00+08:00",
        },
      });
    }

    if (url.includes("/outbox")) {
      return response({
        status: "success",
        data: [{
          event_id: "qear_evt_demo",
          event_type: "qe.loop.completed",
          source_system: "qe",
          source_id: "qe_task_demo",
          source_sub_id: "qe_task_demo_Loop1",
          status: "pending",
          retry_count: 0,
          created_at: "2026-05-02T20:01:00+08:00",
        }],
      });
    }

    if (url.includes("/jobs")) {
      return response({
        status: "success",
        data: [{
          job_id: "qear_job_demo",
          event_id: "qear_evt_done",
          run_id: "qear_run_demo",
          job_type: "qe.loop.completed",
          status: "completed",
          retry_count: 0,
          completed_at: "2026-05-02T20:02:00+08:00",
        }],
      });
    }

    if (new URL(url).pathname.endsWith("/api/v1/qe-archive/runs")) {
      return response({
        status: "success",
        data: [{
          run_id: "qear_run_demo",
          source_system: "qe",
          run_type: "evolution_loop",
          status: "completed",
          research_valid: true,
          task_id: "qe_task_demo",
          loop_id: "qe_task_demo_Loop3",
          loop_index: 3,
          model_type: "LSTM",
          factor_count: 57,
          freq: "1min",
          label_horizon: 5,
          archived_at: "2026-05-02T20:03:00+08:00",
          metric_count: 81,
          curve_count: 3489,
          factor_count_rows: 57,
          symbol_summary_count: 1310,
          trade_count: 4100,
        }],
      });
    }

    if (url.includes("/backfill-candidates")) {
      const parsedUrl = new URL(url);
      const pageNumber = Number(parsedUrl.searchParams.get("page") || "1");
      const pageSize = Number(parsedUrl.searchParams.get("page_size") || "20");
      expect(pageSize).toBe(20);
      return response({
        status: "success",
        data: {
          status: "completed",
          include_archived: false,
          page: pageNumber,
          page_size: pageSize,
          count: 1,
          has_more: pageNumber < 2,
          candidates: [{
            candidate_id: "task:qe_task_demo",
            candidate_type: "evolution_task",
            source: "task",
            task_id: "qe_task_demo",
            display_name: "demo evolution",
            description: "demo target",
            status: "completed",
            experiment_type: "custom_evolution",
            loop_count: 3,
            selected_run_count: 3,
            archived_run_count: 2,
            pending_run_count: 1,
            is_fully_archived: false,
            model_id: "LSTM",
            label_horizon: 5,
            started_at: "2026-05-02T20:00:00+08:00",
            completed_at: "2026-05-02T20:10:00+08:00",
          }],
        },
      });
    }

    if (method === "POST" && url.includes("/backfill")) {
      const body = route.request().postDataJSON() as {
        task_ids?: string[];
        experiment_ids?: string[];
        write?: boolean;
        min_metrics?: number;
        min_curves?: number;
        min_factors?: number;
        include_archived?: boolean;
      };
      expect(body.task_ids).toEqual(["qe_task_demo"]);
      expect(body.experiment_ids || []).toEqual([]);
      expect(body.include_archived).toBe(false);
      expect(body.min_metrics).toBe(60);
      expect(body.min_curves).toBe(3000);
      expect(body.min_factors).toBe(1);
      return response({
        status: "success",
        data: {
          dry_run: !body.write,
          write_enabled: Boolean(body.write),
          source: "all",
          status: "completed",
          processed_count: 1,
          results: [{
            run_id: "qear_run_demo",
            event_type: "qe.loop.completed",
            source_id: "qe_task_demo",
            source_sub_id: "qe_task_demo_Loop3",
            quality: body.write ? {
              passed: true,
              metric_count: 81,
              curve_count: 3489,
              factor_count_rows: 57,
              symbol_summary_count: 1310,
              trade_count: 4100,
              execution_event_count: 2,
            } : undefined,
            stats: {
              metrics_written: 81,
              curves_written: 3489,
              factors_written: 57,
              symbol_summary_count: 1310,
              trade_count: 4100,
              execution_event_count: 2,
            },
          }],
        },
      });
    }

    if (method === "POST" && url.includes("/worker/run-once")) {
      return response({ status: "success", data: { claimed: 1, completed: 1, failed: 0, skipped_reason: null } });
    }

    if (url.includes("/runs/qear_run_demo/quality")) {
      return response({
        status: "success",
        data: {
          run_id: "qear_run_demo",
          exists: true,
          source_system: "qe",
          run_type: "evolution_loop",
          status: "completed",
          research_valid: true,
          freq: "1min",
          label_horizon: 5,
          factor_count: 57,
          archived_at: "2026-05-02T20:03:00+08:00",
          config_capture_complete: true,
          missing_config_item_count: 0,
          reproducibility_level: "full",
          manifest_verification_status: "not_verified",
          manifest_missing_item_count: 0,
          source_count: 1,
          data_context_count: 1,
          account_summary_count: 1,
          metric_count: 81,
          curve_count: 3489,
          factor_count_rows: 57,
          symbol_summary_count: 1310,
          trade_count: 4100,
          execution_event_count: 2,
          artifact_count: 0,
          raw_payload_count: 3,
          priority_score_count: 0,
        },
      });
    }

    return response({ detail: "unexpected mocked QE archive route" }, 404);
  });

  await page.goto("/qe-archive");
  await expect(page.getByText("QE Archive Warehouse")).toBeVisible();
  await expect(page.getByText("demo evolution")).toBeVisible();
  await expect(page.getByText("qear_evt_demo")).toBeVisible();
  await expect(page.getByText("qear_job_demo")).toBeVisible();
  await expect(page.getByLabel("candidate pagination status")).toContainText("1");
  await page.getByLabel("next candidate page").click();
  await expect(page.getByLabel("candidate pagination status")).toContainText("2");
  await page.getByLabel("previous candidate page").click();
  await expect(page.getByLabel("candidate pagination status")).toContainText("1");

  await page.getByRole("button", { name: "选择全部待入库" }).click();
  await page.getByRole("button", { name: /dry-run/i }).click();
  await expect(page.getByText("qear_run_demo").first()).toBeVisible();
  await expect(page.getByText(/metrics 81/)).toBeVisible();
  await expect(page.getByText(/symbols 1310/).first()).toBeVisible();

  await expect(page.getByLabel("archive write disabled reason")).toContainText("QE_ARCHIVE_WRITE");
  await page.getByLabel("fill archive write confirm").click();
  await page.getByLabel("write selected candidates to archive").click();
  await expect(page.getByText("已通过").first()).toBeVisible();

  await page.getByPlaceholder("QE_ARCHIVE_WORKER_RUN").fill("QE_ARCHIVE_WORKER_RUN");
  await page.getByRole("button", { name: /Outbox/i }).click();
  await expect(page.getByText("Claimed")).toBeVisible();
  await expect(page.getByText("Completed", { exact: true })).toBeVisible();

  await page.getByLabel("Select archived run for quality").selectOption("qear_run_demo");
  await page.getByLabel("check run quality").click();
  await expect(page.getByText("full")).toBeVisible();
  await expect(page.getByText(/3K|3\.49K/)).toBeVisible();
  await expect(page.getByText("股票汇总")).toBeVisible();
  await expect(page.getByText("交易明细")).toBeVisible();
  await expect(page.getByText(/4K|4,100/)).toBeVisible();

  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
});
