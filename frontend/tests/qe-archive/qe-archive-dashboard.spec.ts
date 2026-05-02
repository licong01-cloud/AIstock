import { expect, test } from "@playwright/test";

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

    if (method === "POST" && url.includes("/backfill")) {
      return response({
        status: "success",
        data: {
          dry_run: true,
          write_enabled: false,
          source: "loop",
          status: "completed",
          processed_count: 1,
          results: [{
            run_id: "qear_run_demo",
            event_type: "qe.loop.completed",
            source_id: "qe_task_demo",
            source_sub_id: "qe_task_demo_Loop1",
            stats: { metrics_written: 81, curves_written: 3489, factors_written: 57 },
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
  await expect(page.getByText("qear_evt_demo")).toBeVisible();
  await expect(page.getByText("qear_job_demo")).toBeVisible();

  await page.getByRole("button", { name: /dry-run/i }).click();
  await expect(page.getByText("qear_run_demo").first()).toBeVisible();
  await expect(page.getByText(/metrics 81/)).toBeVisible();

  await page.getByPlaceholder("QE_ARCHIVE_WORKER_RUN").fill("QE_ARCHIVE_WORKER_RUN");
  await page.getByRole("button", { name: /Outbox/i }).click();
  await expect(page.getByText("Claimed")).toBeVisible();
  await expect(page.getByText("Completed", { exact: true })).toBeVisible();

  await page.getByPlaceholder("qear_run_...").fill("qear_run_demo");
  await page.getByRole("button", { name: /查询质量/ }).click();
  await expect(page.getByText("full")).toBeVisible();
  await expect(page.getByText(/3K|3\.49K/)).toBeVisible();

  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
});
