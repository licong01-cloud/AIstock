import { expect, test } from "@playwright/test";

test("QE experiment list exposes candidate strategy package actions", async ({ page }) => {
  const parentExperimentId = "qe_candidate_parent";
  const childExperimentId = "qe_candidate_parent_Loop1";
  const taskId = "qe_candidate_task";
  const candidatePosts: any[] = [];

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
