import { expect, test, type Page, type Route } from "@playwright/test";

const taskKey = "scenario_replay_task";
const sourceRunId = "macb_source_terminal";

function respond(route: Route, data: unknown) {
  return route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({ status: "success", data }),
  });
}

async function installMocks(
  page: Page,
  submittedBodies: Array<Record<string, unknown>>,
  durableBodies: Array<{ path: string; body: Record<string, unknown>; idempotencyKey: string | null }> = [],
) {
  const task = {
    task_id: taskKey,
    task_name: "R12P 场景回测",
    task_type: "multi_alpha_combine",
    status: "completed",
    current_loop: 1,
    max_loops: 1,
    created_at: "2026-07-18T08:00:00+08:00",
    updated_at: "2026-07-18T09:00:00+08:00",
    roster_hash: "roster_hash",
    normalize_method: "zscore",
    walk_forward_signature: "wf60",
    available_schemes: ["equal"],
    default_scheme: "equal",
    completed_count: 1,
    failed_count: 0,
  };
  const loop = {
    loop_id: sourceRunId,
    loop_index: 1,
    run_id: sourceRunId,
    status: "completed",
    raw_status: "succeeded",
    phase: "completed",
    retryable: true,
    deletable: true,
    is_sota: true,
    created_at: task.created_at,
    updated_at: task.updated_at,
    config_json: {
      runtime_flags: { run_id: sourceRunId },
      strategy_params: { topk: 50 },
      backtest_config: { topk: 50, initial_cash: 100_000_000 },
    },
    metrics_json: { annualized_return: 0.42, sharpe: 1.8, max_drawdown: -0.12 },
    scheme_results: [{ weighting_scheme: "equal", cagr: 0.42, sharpe: 1.8, pred_persisted: true }],
  };
  const retryDraft = {
    run_id: sourceRunId,
    retryable: true,
    exact: true,
    source: "request_snapshot",
    assumptions: [],
    payload: {
      roster: [
        { leg_id: "lgbm_h60", seed_run_ids: ["qe_lgbm_h60_loop1"] },
        { leg_id: "gat_h40", seed_run_ids: ["qe_gat_h40_loop1"] },
      ],
      oos_start: "2024-07-01",
      oos_end: "2026-06-29",
      weighting_schemes: ["equal"],
      normalize_method: "zscore",
      walk_forward: { enabled: true, window: 60, min_periods: 2 },
      rank_fusion: {},
      backtest_config: {
        node_id: "rdagent-node1",
        node_parallelism: { "rdagent-node1": 4 },
        topk: 50,
        initial_cash: 100_000_000,
        strategy_kwargs: { n_drop: 5, max_n_drop: 5, min_n_drop: 0, hold_thresh: 2 },
      },
      baseline_leg_id: "lgbm_h60",
      topk: 50,
      min_date_coverage: 0.8,
      run_async: true,
    },
  };

  await page.route(new RegExp(`/api/v1/multi-alpha/combine/tasks/${taskKey}(?:\\?.*)?$`), route => respond(route, {
    task,
    loops: [loop],
    scheme: "equal",
    available_schemes: ["equal"],
  }));
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${sourceRunId}/logs(?:\\?.*)?$`), route => respond(route, {
    run_id: sourceRunId,
    status: "succeeded",
    phase: "completed",
    history_available: true,
    events: [],
    files: [],
  }));
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${sourceRunId}/archive-status$`), route => respond(route, {
    run_id: sourceRunId,
    archive_status: "archived",
  }));
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${sourceRunId}/retry-draft$`), route => respond(route, retryDraft));
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${sourceRunId}/retry$`), async route => {
    submittedBodies.push(await route.request().postDataJSON());
    await respond(route, { run_id: "macb_scenario_10m_top20", status: "running" });
  });
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${sourceRunId}/control-capabilities$`), route => respond(route, {
    run_id: sourceRunId,
    run_status: "failed",
    run_terminal: true,
    actions: { pause: { state: "available" }, recovery: { state: "available" } },
    evidence: {
      execution_identity_hash: "a".repeat(64),
      execution_identity_evidence: { complete: false, missing: ["dataset_manifest"], acquisition_suggestions: ["publish manifest"] },
    },
  }));
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${sourceRunId}/children(?:\\?.*)?$`), route => respond(route, {
    children: [{
      child_id: "macbc_failed_scheme",
      child_key: "scheme:equal",
      child_kind: "scheme",
      status: "failed",
      selected_attempt_id: "macba_failed_scheme_1",
      execution_disposition: "execute",
    }],
  }));
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${sourceRunId}/commands\\?`), route => respond(route, {
    commands: [],
  }));
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${sourceRunId}/(?:pause|resume|cancel|stop|reconcile)$`), async route => {
    durableBodies.push({
      path: new URL(route.request().url()).pathname,
      body: await route.request().postDataJSON(),
      idempotencyKey: route.request().headers()["idempotency-key"] || null,
    });
    await respond(route, { command: { command_id: "macmd_pause", action: "pause", status: "accepted" } });
  });
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${sourceRunId}/children/macbc_failed_scheme/recovery/preview$`), async route => {
    durableBodies.push({
      path: new URL(route.request().url()).pathname,
      body: await route.request().postDataJSON(),
      idempotencyKey: route.request().headers()["idempotency-key"] || null,
    });
    await respond(route, {
      topology: "successor_recovery_run",
      source_run_id: sourceRunId,
      target_child_id: "macbc_failed_scheme",
      retry_mode: "backtest_only",
      command_id: "macmd_preview",
      scope_hash: "b".repeat(64),
      successor_run_id: "macb_recovery_target",
      state_allowed: true,
      evidence: { complete: false, missing: ["prediction"], acquisition_suggestions: ["restore artifact"] },
      dependency_plan: [{ child_id: "macbc_failed_scheme", disposition: "execute" }],
    });
  });
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${sourceRunId}/children/macbc_failed_scheme/recovery$`), async route => {
    durableBodies.push({
      path: new URL(route.request().url()).pathname,
      body: await route.request().postDataJSON(),
      idempotencyKey: route.request().headers()["idempotency-key"] || null,
    });
    await respond(route, { command: { command_id: "macmd_recovery", action: "child_retry", status: "accepted" } });
  });
}

test("combine run launches explicit capital and holding scenario from frozen predictions", async ({ page }) => {
  const submittedBodies: Array<Record<string, unknown>> = [];
  await installMocks(page, submittedBodies);

  await page.goto(`/quantevolver/multi-alpha/combine-backtest/${taskKey}?tab=runtime`);
  await expect(page.getByText(sourceRunId, { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "新建资金/持仓场景" }).click();

  await expect(page.getByText("不会重新训练模型", { exact: false })).toBeVisible();
  await page.getByTestId("scenario-preset-10m-top20").click();
  await expect(page.getByTestId("scenario-initial-cash")).toHaveValue("10000000");
  await expect(page.getByTestId("scenario-topk")).toHaveValue("20");
  await expect(page.getByTestId("scenario-n-drop")).toHaveValue("5");

  await page.getByTestId("scenario-name").fill("r12p_10m_top20_hold10");
  await page.getByTestId("scenario-n-drop").fill("2");
  await page.getByTestId("scenario-max-n-drop").fill("4");
  await page.getByTestId("scenario-min-n-drop").fill("1");
  await page.getByTestId("scenario-hold-thresh").fill("10");
  await page.getByTestId("scenario-submit").click();

  await expect(page.getByText("已创建预测复用场景回测: macb_scenario_10m_top20")).toBeVisible();
  expect(submittedBodies).toHaveLength(1);
  const payload = submittedBodies[0].payload as Record<string, unknown>;
  const backtestConfig = payload.backtest_config as Record<string, unknown>;
  const strategyKwargs = backtestConfig.strategy_kwargs as Record<string, unknown>;
  expect(payload.topk).toBe(20);
  expect(payload.run_async).toBe(true);
  expect(payload.roster).toEqual(retryDraftRoster());
  expect(backtestConfig.initial_cash).toBe(10_000_000);
  expect(backtestConfig.topk).toBe(20);
  expect(backtestConfig.scenario_name).toBe("r12p_10m_top20_hold10");
  expect(backtestConfig.scenario_type).toBe("capital_holding_pred_replay");
  expect(strategyKwargs).toMatchObject({ n_drop: 2, max_n_drop: 4, min_n_drop: 1, hold_thresh: 10 });
});

function retryDraftRoster() {
  return [
    { leg_id: "lgbm_h60", seed_run_ids: ["qe_lgbm_h60_loop1"] },
    { leg_id: "gat_h40", seed_run_ids: ["qe_gat_h40_loop1"] },
  ];
}

test("durable controls preserve explicit evidence and submit no approval payload", async ({ page }) => {
  const submittedBodies: Array<Record<string, unknown>> = [];
  const durableBodies: Array<{ path: string; body: Record<string, unknown>; idempotencyKey: string | null }> = [];
  await installMocks(page, submittedBodies, durableBodies);

  await page.goto(`/quantevolver/multi-alpha/combine-backtest/${taskKey}?tab=runtime`);
  await expect(page.getByText("Durable QE 控制与 Child Recovery")).toBeVisible();
  await expect(page.getByText("dataset_manifest", { exact: false })).toBeVisible();

  await page.getByRole("button", { name: "pause", exact: true }).click();
  await page.getByRole("button", { name: "预览恢复闭包", exact: true }).click();
  await expect(page.getByText("scope_hash=", { exact: false })).toBeVisible();
  await page.getByRole("button", { name: "执行已预览恢复", exact: true }).click();

  expect(durableBodies).toHaveLength(3);
  expect(durableBodies[0]).toMatchObject({ path: `/api/v1/multi-alpha/combine-backtest/runs/${sourceRunId}/pause`, body: { request: {} } });
  expect(durableBodies[1].body).toEqual({ retry_mode: "backtest_only" });
  expect(durableBodies[2].body).toEqual({
    retry_mode: "backtest_only",
    scope_hash: "b".repeat(64),
    preview_command_id: "macmd_preview",
  });
  expect(durableBodies[2].idempotencyKey).toBe(durableBodies[1].idempotencyKey);
  for (const request of durableBodies) {
    expect(request.idempotencyKey).toBeTruthy();
    expect(JSON.stringify(request.body)).not.toContain("approval");
    expect(JSON.stringify(request.body)).not.toContain("confirm");
  }
});
