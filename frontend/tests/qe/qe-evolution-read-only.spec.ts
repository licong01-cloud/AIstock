import { expect, test } from "@playwright/test";

const apiBase = process.env.QE_API_BASE || process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8011/api/v1";
const taskId = process.env.QE_READ_TASK_ID || "qe_20260414_173338_d1c5";

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

test("QE evolution terminal task detail is read-only, accurate, and observable", async ({ page, request }) => {
  const failures: string[] = [];

  page.on("pageerror", error => failures.push(`pageerror: ${error.message}`));
  page.on("console", message => {
    if (message.type() === "error") failures.push(`console error: ${message.text()}`);
  });
  page.on("requestfailed", req => failures.push(`request failed: ${req.method()} ${req.url()} ${req.failure()?.errorText || "unknown"}`));
  page.on("response", resp => {
    const url = resp.url();
    if (url.includes("/api/") && resp.status() >= 500) {
      failures.push(`api ${resp.status()}: ${url}`);
    }
  });

  const listResp = await request.get(`${apiBase}/quantevolver/evolution/tasks`);
  expect(listResp.ok(), `task list failed: ${listResp.status()}`).toBeTruthy();
  const listPayload = await listResp.json();
  const tasks = Array.isArray(listPayload.data) ? listPayload.data : listPayload.tasks || [];
  const listedTask = tasks.find((item: any) => item.task_id === taskId);
  expect(listedTask, `target task ${taskId} must be present in task list`).toBeTruthy();

  const detailResp = await request.get(`${apiBase}/quantevolver/evolution/tasks/${taskId}`);
  expect(detailResp.ok(), `detail failed: ${detailResp.status()} ${await detailResp.text()}`).toBeTruthy();
  const detailPayload = await detailResp.json();
  const detail = detailPayload.data || detailPayload;
  expect(detail.task_id).toBe(taskId);
  expect(detail.status).toBe(listedTask.status);
  expect(Number(detail.current_loop)).toBe(Number(listedTask.current_loop));
  expect(Number(detail.max_loops)).toBe(Number(listedTask.max_loops));
  expect(Array.isArray(detail.loops)).toBeTruthy();
  expect(detail.loops.length).toBeGreaterThan(0);

  await page.goto("/quantevolver/evolution");
  const taskCell = page.getByText(taskId, { exact: true }).first();
  await expect(taskCell).toBeVisible({ timeout: 120_000 });
  await expect(page.getByText(`${detail.current_loop}/${detail.max_loops}`).first()).toBeVisible();

  const detailWait = page.waitForResponse(resp =>
    resp.url().includes(`/quantevolver/evolution/tasks/${taskId}`) && resp.status() === 200,
    { timeout: 120_000 },
  );
  await taskCell.click();
  await detailWait;

  for (const loop of detail.loops) {
    await expect(page.getByText(`LOOP ${loop.loop_index}`).first()).toBeVisible({ timeout: 60_000 });
  }

  const loopsWithIc = detail.loops.filter((loop: any) => loop?.status === "completed" && loop?.metrics_json?.IC != null);
  if (loopsWithIc.length > 0) {
    const expectedIc = Number(loopsWithIc[0].metrics_json.IC).toFixed(4);
    await expect(page.getByText(new RegExp(`IC:\\s*${escapeRegExp(expectedIc)}`)).first()).toBeVisible({ timeout: 60_000 });
  }

  expect(failures).toEqual([]);
});

test("QE dashboard stops automatic polling when task list has no active task", async ({ page }) => {
  const mockTaskId = "qe_mock_terminal_no_poll";
  const mockLoopId = `${mockTaskId}_Loop1`;
  const task = {
    task_id: mockTaskId,
    task_name: "QE no-poll terminal task",
    target_desc: "mock terminal task for polling contract",
    max_loops: 1,
    current_loop: 1,
    status: "completed",
    base_experiment_id: `${mockTaskId}_base`,
    source_type: "custom",
    task_type: "custom_evo",
    created_at: "2026-05-02T09:00:00+08:00",
    updated_at: "2026-05-02T09:10:00+08:00",
    evolution_mode: "auto",
  };
  const detail = {
    ...task,
    loops: [
      {
        loop_id: mockLoopId,
        loop_index: 1,
        status: "completed",
        action_type: "initial",
        is_sota: true,
        config_json: { model_id: "mock_model", factor_list: ["mock_factor"] },
        metrics_json: { IC: 0.123456, Rank_IC: 0.234567 },
      },
    ],
  };
  let listHits = 0;
  let detailHits = 0;

  await page.route(/\/api\/v1\/quantevolver\/evolution\/tasks$/, async route => {
    listHits += 1;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ status: "success", data: [task] }),
    });
  });
  await page.route(new RegExp(`/api/v1/quantevolver/evolution/tasks/${mockTaskId}$`), async route => {
    detailHits += 1;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ status: "success", data: detail }),
    });
  });
  await page.route(new RegExp(`/api/v1/quantevolver/evolution/tasks/${mockTaskId}/logs/tail.*`), async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "success",
        data: { task_status: "completed", logs: ["mock terminal log"] },
      }),
    });
  });
  await page.route(new RegExp(`/api/v1/quantevolver/evolution/tasks/${mockTaskId}/loops/Loop1/enhanced-metrics$`), async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "success",
        data: { summary: { IC: 0.123456, Rank_IC: 0.234567 } },
      }),
    });
  });

  await page.goto("/quantevolver/evolution");
  const taskCell = page.getByText(mockTaskId, { exact: true }).first();
  await expect(taskCell).toBeVisible({ timeout: 120_000 });
  await expect(page.getByText("手动").first()).toBeVisible();
  await taskCell.click();
  await expect(page.getByText("LOOP 1").first()).toBeVisible({ timeout: 60_000 });
  await expect(page.getByText(/IC:\s*0\.1235/).first()).toBeVisible({ timeout: 60_000 });

  const listBaseline = listHits;
  const detailBaseline = detailHits;
  await page.waitForTimeout(65_000);

  expect(listHits, "terminal/no-active task list must not be automatically polled").toBe(listBaseline);
  expect(detailHits, "terminal selected task detail must not be automatically polled").toBe(detailBaseline);
});
