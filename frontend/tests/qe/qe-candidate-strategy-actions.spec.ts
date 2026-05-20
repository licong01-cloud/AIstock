import { expect, test } from "@playwright/test";

test("QE experiment list exposes candidate strategy package actions", async ({ page }) => {
  const parentExperimentId = "qe_candidate_parent";
  const childExperimentId = "qe_candidate_parent_Loop1";
  const taskId = "qe_candidate_task";
  const candidatePosts: any[] = [];
  const archivePosts: any[] = [];

  await page.route(/\/api\/v1\/quantevolver\/experiments\?.*/, async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        total: 2,
        items: [
          {
            experiment_id: parentExperimentId,
            experiment_name: "Candidate parent experiment",
            status: "completed",
            factor_names: ["factor_a"],
            model_id: "mock_model",
            strategy_id: "mock_strategy",
            ic: 0.061,
            created_at: "2026-05-18T09:00:00+08:00",
          },
          {
            experiment_id: childExperimentId,
            experiment_name: "Candidate child Loop1",
            status: "completed",
            parent_experiment_id: parentExperimentId,
            is_evolution_loop: true,
            qe_task_id: taskId,
            qe_loop_id: "Loop1",
            loop_index: 1,
            factor_names: ["factor_a"],
            model_id: "mock_model",
            strategy_id: "mock_strategy",
            ic: 0.071,
            annualized_return: 0.18,
            created_at: "2026-05-18T09:10:00+08:00",
          },
        ],
      }),
    });
  });

  await page.route(/\/api\/v1\/qe-archive\/source-status$/, async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "success",
        data: {
          experiments: {
            [parentExperimentId]: { archive_status: "recommended", eligible: true, recommended: true, run_ids: [], run_count: 0 },
          },
          tasks: {
            [taskId]: { archive_status: "recommended", loop_count: 1, archived_loop_count: 0, eligible_loop_count: 1, pending_loop_count: 1, recommended_loop_count: 1, run_ids: [] },
          },
          loops: {
            [`${taskId}_Loop1`]: { archive_status: "recommended", eligible: true, recommended: true, run_ids: [], run_count: 0 },
          },
          include_recommendation: true,
        },
      }),
    });
  });

  await page.route(/\/api\/v1\/strategy-packages\/candidates\/from-qe-experiment$/, async route => {
    candidatePosts.push(await route.request().postDataJSON());
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ ok: true, candidate: { candidate_id: "csp_from_exp" } }),
    });
  });

  await page.route(/\/api\/v1\/strategy-packages\/candidates\/from-qe-loop$/, async route => {
    candidatePosts.push(await route.request().postDataJSON());
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ ok: true, candidate: { candidate_id: "csp_from_loop" } }),
    });
  });

  await page.route(/\/api\/v1\/qe-archive\/backfill$/, async route => {
    archivePosts.push(await route.request().postDataJSON());
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ status: "success", data: { dry_run: true, results: [{ will_archive: true, dry_run: true }] } }),
    });
  });

  await page.goto("/quantevolver/experiments");
  await page.getByText(parentExperimentId, { exact: true }).click();

  await page.getByTestId("qe-exp-add-candidate").click();
  await expect(page.getByText(/csp_from_exp/)).toBeVisible();
  expect(candidatePosts[0]).toMatchObject({
    experiment_id: parentExperimentId,
    created_by: "quantevolver_experiments_list",
    manual_action: true,
  });

  await page.getByTestId(`qe-experiment-loops-toggle-${parentExperimentId}`).click();
  await page.getByText(childExperimentId, { exact: true }).click();
  await page.getByTestId("qe-child-loop-add-candidate").click();
  await expect(page.getByText(/csp_from_loop/)).toBeVisible();
  expect(candidatePosts[1]).toMatchObject({
    qe_task_id: taskId,
    qe_loop_id: "Loop1",
    experiment_id: childExperimentId,
    created_by: "quantevolver_experiments_list",
    manual_action: true,
  });

  await page.getByTestId("qe-child-loop-archive-preview").click();
  await expect.poll(() => archivePosts.length).toBe(1);
  expect(archivePosts[0]).toMatchObject({
    source: "loop",
    loop_ids: [`${taskId}_Loop1`],
    write: false,
  });
});

test("QE evolution task detail posts completed loop candidate action", async ({ page }) => {
  const taskId = "qe_candidate_task_detail";
  let candidatePayload: any = null;

  await page.route(new RegExp(`/api/v1/quantevolver/evolution/tasks/${taskId}$`), async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "success",
        data: {
          task: { task_id: taskId, task_name: "Candidate task detail", status: "completed" },
          loops: [
            {
              loop_id: `${taskId}_Loop1`,
              loop_index: 1,
              status: "completed",
              is_sota: true,
              action_type: "initial",
              experiment_id: `${taskId}_exp_L1`,
              config_json: { factor_list: ["factor_a"], model_id: "mock_model", strategy_id: "mock_strategy" },
              metrics_json: { ic: 0.055, annualized_return: 0.12, max_drawdown: -0.08 },
            },
          ],
        },
      }),
    });
  });

  await page.route(new RegExp(`/api/v1/quantevolver/evolution/tasks/${taskId}/loops/.+/enhanced-metrics$`), async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ status: "success", data: { summary: { ic: 0.055 } } }),
    });
  });

  await page.route(/\/api\/v1\/qe-archive\/source-status$/, async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "success",
        data: {
          tasks: {
            [taskId]: { archive_status: "recommended", loop_count: 1, archived_loop_count: 0, eligible_loop_count: 1, pending_loop_count: 1, recommended_loop_count: 1, run_ids: [] },
          },
          loops: {
            [`${taskId}_Loop1`]: { archive_status: "recommended", eligible: true, recommended: true, run_ids: [], run_count: 0 },
          },
          include_recommendation: true,
        },
      }),
    });
  });

  await page.route(/\/api\/v1\/strategy-packages\/candidates\/from-qe-loop$/, async route => {
    candidatePayload = await route.request().postDataJSON();
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ ok: true, candidate: { candidate_id: "csp_task_detail" } }),
    });
  });

  await page.goto(`/quantevolver/evolution/${taskId}`);
  await page.getByTestId("qe-task-loop-add-candidate").click();

  await expect(page.getByText(/csp_task_detail/)).toBeVisible();
  expect(candidatePayload).toMatchObject({
    qe_task_id: taskId,
    qe_loop_id: "Loop1",
    experiment_id: `${taskId}_exp_L1`,
    created_by: "quantevolver_evolution_task_detail",
    manual_action: true,
  });
});

test("QE evolution LoopDetailPanel exposes candidate action with feedback", async ({ page }) => {
  const taskId = "qe_candidate_panel_task";
  let candidatePayload: any = null;

  await page.route(/\/api\/v1\/quantevolver\/evolution\/tasks$/, async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "success",
        data: [
          {
            task_id: taskId,
            task_name: "Candidate panel task",
            target_desc: "mock candidate panel task",
            max_loops: 1,
            current_loop: 1,
            status: "completed",
            source_type: "custom_evo",
            task_type: "custom_evo",
            evolution_mode: "auto",
            created_at: "2026-05-18T09:00:00+08:00",
            updated_at: "2026-05-18T09:10:00+08:00",
          },
        ],
      }),
    });
  });

  await page.route(new RegExp(`/api/v1/quantevolver/evolution/tasks/${taskId}$`), async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "success",
        data: {
          task: { task_id: taskId, task_name: "Candidate panel task", status: "completed", task_type: "custom_evo" },
          loops: [
            {
              loop_id: `${taskId}_Loop1`,
              task_id: taskId,
              loop_index: 1,
              status: "completed",
              action_type: "initial",
              is_sota: true,
              experiment_id: `${taskId}_exp_L1`,
              config_json: { factor_list: ["factor_a"], model_id: "mock_model", strategy_id: "mock_strategy" },
              metrics_json: { IC: 0.066, Rank_IC: 0.077 },
              created_at: "2026-05-18T09:00:00+08:00",
              updated_at: "2026-05-18T09:10:00+08:00",
            },
          ],
        },
      }),
    });
  });

  await page.route(new RegExp(`/api/v1/quantevolver/evolution/tasks/${taskId}/logs/tail.*`), async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ status: "success", data: { task_status: "completed", logs: ["mock log"] } }),
    });
  });

  await page.route(new RegExp(`/api/v1/quantevolver/evolution/tasks/${taskId}/loops/Loop1/enhanced-metrics$`), async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ status: "success", data: { summary: { IC: 0.066 } } }),
    });
  });

  await page.route(/\/api\/v1\/qe-archive\/source-status$/, async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "success",
        data: {
          tasks: {
            [taskId]: { archive_status: "recommended", loop_count: 1, archived_loop_count: 0, eligible_loop_count: 1, pending_loop_count: 1, recommended_loop_count: 1, run_ids: [] },
          },
          loops: {
            [`${taskId}_Loop1`]: { archive_status: "recommended", eligible: true, recommended: true, run_ids: [], run_count: 0 },
          },
          include_recommendation: true,
        },
      }),
    });
  });

  await page.route(/\/api\/v1\/strategy-packages\/candidates\/from-qe-loop$/, async route => {
    candidatePayload = await route.request().postDataJSON();
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ ok: true, candidate: { candidate_id: "csp_panel_loop" } }),
    });
  });

  await page.goto("/quantevolver/evolution");
  await page.getByText(taskId, { exact: true }).click();

  await page.getByTestId("qe-loop-panel-add-candidate").click();
  await expect(page.getByText(/csp_panel_loop/)).toBeVisible();
  expect(candidatePayload).toMatchObject({
    qe_task_id: taskId,
    qe_loop_id: "Loop1",
    experiment_id: `${taskId}_exp_L1`,
    created_by: "quantevolver_loop_detail_panel",
    manual_action: true,
  });
});

test("QE evolution main page previews task-level archive selection through QE Archive API", async ({ page }) => {
  const taskId = "qe_archive_main_task";
  const backfillPosts: any[] = [];

  await page.route(/\/api\/v1\/quantevolver\/evolution\/tasks$/, async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "success",
        data: [
          {
            task_id: taskId,
            task_name: "Archive main task",
            target_desc: "mock archive main task",
            max_loops: 2,
            current_loop: 2,
            status: "completed",
            source_type: "custom_evo",
            task_type: "custom_evo",
            evolution_mode: "auto",
            created_at: "2026-05-18T09:00:00+08:00",
            updated_at: "2026-05-18T09:10:00+08:00",
          },
        ],
      }),
    });
  });

  await page.route(/\/api\/v1\/qe-archive\/source-status$/, async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "success",
        data: {
          tasks: {
            [taskId]: { archive_status: "recommended", loop_count: 2, archived_loop_count: 0, eligible_loop_count: 2, pending_loop_count: 2, recommended_loop_count: 1, run_ids: [] },
          },
          include_recommendation: true,
        },
      }),
    });
  });

  await page.route(/\/api\/v1\/qe-archive\/backfill$/, async route => {
    backfillPosts.push(await route.request().postDataJSON());
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ status: "success", data: { dry_run: true, results: [{ will_archive: true, dry_run: true }] } }),
    });
  });

  page.on("dialog", dialog => dialog.accept());
  await page.goto("/quantevolver/evolution");
  await expect(page.getByText(taskId, { exact: true })).toBeVisible();
  await expect(page.getByText("推荐入仓").first()).toBeVisible();
  await page.getByTestId("qe-evolution-task-archive-preview").click();

  await expect.poll(() => backfillPosts.length).toBe(1);
  expect(backfillPosts[0]).toMatchObject({
    source: "task",
    task_ids: [taskId],
    status: "completed",
    include_archived: false,
    write: false,
  });
});
