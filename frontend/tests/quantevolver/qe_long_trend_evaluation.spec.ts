import { expect, test } from "@playwright/test";

const evaluationId = `qelt_${"a".repeat(64)}`;
const snapshotId = "qlib_outcome_snapshot_20260630";

test("completed QE Loop restores long-trend DB state and posts the single idempotent action", async ({ page }) => {
  const taskId = "qe_long_trend_ui_task";
  const posts: unknown[] = [];

  await page.route(/\/api\/v1\/quantevolver\/evolution\/tasks(?:\?.*)?$/, async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "success",
        data: [{
          task_id: taskId,
          task_name: "Long trend UI task",
          target_desc: "F-014 UI fixture",
          max_loops: 1,
          current_loop: 1,
          status: "completed",
          source_type: "custom_evo",
          task_type: "custom_evo",
          created_at: "2026-07-29T08:00:00+08:00",
          updated_at: "2026-07-29T09:00:00+08:00",
        }],
      }),
    });
  });

  await page.route(new RegExp(`/api/v1/quantevolver/evolution/tasks/${taskId}$`), async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "success",
        data: {
          task: { task_id: taskId, task_name: "Long trend UI task", status: "completed", task_type: "custom_evo" },
          loops: [{
            loop_id: `${taskId}_Loop1`,
            task_id: taskId,
            loop_index: 1,
            status: "completed",
            action_type: "initial",
            is_sota: true,
            experiment_id: `${taskId}_exp_L1`,
            config_json: { factor_list: ["factor_a"], model_id: "LGBM", label_horizon: 60 },
            metrics_json: { IC: 0.066, Rank_IC: 0.077 },
          }],
        },
      }),
    });
  });

  await page.route(new RegExp(`/api/v1/quantevolver/evolution/tasks/${taskId}/logs/tail.*`), async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "success", data: { task_status: "completed", logs: [] } }) });
  });
  await page.route(new RegExp(`/api/v1/quantevolver/evolution/tasks/${taskId}/loops/Loop1/enhanced-metrics$`), async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "success", data: { summary: { IC: 0.066 } } }) });
  });
  await page.route(/\/api\/v1\/qe-archive\/source-status$/, async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "success", data: { tasks: {}, loops: {}, include_recommendation: true } }) });
  });

  await page.route(new RegExp(`/api/v1/quantevolver/evolution/tasks/${taskId}/loops/1/long-trend-evaluations(?:\\?.*)?$`), async (route) => {
    if (route.request().method() === "POST") {
      posts.push(await route.request().postDataJSON());
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ evaluation_id: evaluationId, status: "partial", profile_id: "qe_long_trend_v1", outcome_dataset_snapshot_id: snapshotId, ready_for_node: false }),
      });
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        items: [{
          evaluation_id: evaluationId,
          run_id: "qear_run_fixture",
          parent_task_id: taskId,
          parent_loop_index: 1,
          profile_id: "qe_long_trend_v1",
          outcome_dataset_snapshot_id: snapshotId,
          status: "partial",
          platform_delivery_status_json: { db: "published", db_metric_count: 1, db_artifact_count: 1 },
        }],
        next_cursor: null,
        limit: 100,
      }),
    });
  });

  await page.route(new RegExp(`/api/v1/quantevolver/evolution/tasks/${taskId}/loops/1/long-trend-input-preview(?:\\?.*)?$`), async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: "qe_long_trend_input_preview_v1",
        task_id: taskId,
        loop_index: 1,
        run_id: "qear_run_fixture",
        node_id: "wsl",
        profile: { profile_id: "qe_long_trend_v1", profile_sha256: "p".repeat(64), horizons: [20, 40, 60, 120, 180], barriers: [0.3, 0.5, 0.7], calendar_slices: ["all_oos", "last_252_signal_days", "last_126_signal_days"] },
        requested_outcome_dataset_snapshot_id: snapshotId,
        backtest_freq: "1day",
        dataset_inputs: [
          { input_name: "feature_dataset_identity", category: "dataset_identity", available: true },
          { input_name: "outcome_dataset_identity", category: "dataset_identity", available: true },
        ],
        artifact_inputs: [
          { input_name: "prediction", category: "recorder_artifact", available: true },
          { input_name: "positions", category: "recorder_artifact", available: false, reason_code: "QELT_POSITION_ARTIFACT_MISSING" },
          { input_name: "portfolio_report", category: "recorder_artifact", available: true },
          { input_name: "indicator_object", category: "recorder_artifact", available: false, reason_code: "QELT_INDICATOR_OBJECT_MISSING" },
        ],
        ready_for_node: true,
        technical_readiness_only: true,
        research_gate: false,
        data_action_plan: [{ action: "archive_missing_qe_recorder_artifact", input_name: "positions", reason_code: "QELT_POSITION_ARTIFACT_MISSING" }],
        warnings: [],
        side_effects: { training_started: false, backtest_started: false, evaluation_created: false, control_row_created: false, worker_submitted: false, cas_written: false, database_written: false },
      }),
    });
  });

  await page.route(new RegExp(`/api/v1/quantevolver/evolution/long-trend-evaluations/${evaluationId}(?:\\?.*)?$`), async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        evaluation: {
          evaluation_id: evaluationId,
          run_id: "qear_run_fixture",
          parent_task_id: taskId,
          parent_loop_index: 1,
          profile_id: "qe_long_trend_v1",
          feature_dataset_snapshot_id: "feature_snapshot",
          outcome_dataset_snapshot_id: snapshotId,
          evaluation_asof: "2026-06-30",
          status: "partial",
          family_status_json: {
            signal_path: { status: "COMPUTED" },
            position_episode: { status: "COMPUTED_WITH_LIMITATIONS", limitations: ["left_censored episode retained"] },
            portfolio_result: { status: "COMPUTED" },
            order_fill: { status: "COMPUTED_WITH_LIMITATIONS", coverage: { entry_coverage: 0.8 }, limitations: ["delayed fill evidence"] },
            execution_cause: { status: "NOT_VERIFIABLE", reason_codes: ["QELT_EXECUTION_EVIDENCE_INSUFFICIENT"], coverage: { direct_cause_coverage: 0.25 }, missing_inputs: ["order_queue"], data_actions: ["backfill order evidence"] },
            sector_regime: { status: "COMPUTED" },
          },
          platform_delivery_status_json: { db: "published", cas: "published", worker: "partial", db_metric_count: 1, db_artifact_count: 1 },
          updated_at: "2026-07-29T09:00:00+08:00",
        },
        metrics: [
          {
            evaluation_metric_id: 1,
            evaluation_id: evaluationId,
            metric_key: "rank_ic",
            metric_scope: "signal_path",
            horizon: 60,
            sector_code: null,
            dimension_key: "b".repeat(64),
            dimension_json: { slice: "all_oos", horizon: 60, k: null, barrier: null },
            value_num: 0.1234,
            value_json: { icir: 0.5 },
            quality_flag: "ok",
          },
          {
            evaluation_metric_id: 2,
            evaluation_id: evaluationId,
            metric_key: "maturity",
            metric_scope: "signal_path",
            horizon: 60,
            sector_code: null,
            dimension_key: "f".repeat(64),
            dimension_json: { slice: "all_oos", horizon: 60, k: null, barrier: null },
            value_num: null,
            value_json: { matured: 90, right_censored: 8, path_incomplete: 2 },
            quality_flag: "computed_with_limitations",
          },
          {
            evaluation_metric_id: 3,
            evaluation_id: evaluationId,
            metric_key: "entry_execution_summary",
            metric_scope: "order_fill",
            horizon: null,
            sector_code: null,
            dimension_key: "g".repeat(64),
            dimension_json: { slice: "all_oos", horizon: null, k: null, barrier: null },
            value_num: null,
            value_json: {
              entry_status_counts: { filled_t1: 80, delayed_fill: 20 },
              exit_status_counts: { filled_on_exit_signal_day: 70, delayed_exit: 20, not_verifiable: 10 },
              entry_evidence_level_counts: { reconciled_trade: 80, qlib_indicator_object: 20 },
              exit_evidence_level_counts: { position_transition: 70, exit_signal_only: 30 },
              entry_block_reason_counts: { blocked_limit_up: 3 },
              exit_block_reason_counts: { blocked_limit_down: 4, blocked_suspension: 2 },
              entry_delay_days: { p50: 1 },
              exit_delay_days: { p50: 2 },
              missed_mfe_due_to_entry_block: { mean: 0.04 },
              blocked_exit_extra_drawdown: { mean: 0.03 },
              blocked_exit_extra_holding_days: { p50: 2 },
            },
            quality_flag: "computed_with_limitations",
          },
        ],
        metric_next_cursor: null,
        artifacts: [{ evaluation_artifact_id: 1, evaluation_id: evaluationId, artifact_type: "artifact_manifest", artifact_uri: "qe-long-trend://fixture/manifest.json", sha256: "c".repeat(64), row_count: 1, status: "published" }],
      }),
    });
  });

  await page.goto("/quantevolver/evolution");
  await page.getByText(taskId, { exact: true }).click();
  await page.getByRole("button", { name: "长期趋势" }).click();

  await expect(page.getByTestId("qe-long-trend-panel")).toBeVisible();
  await expect(page.getByTestId("qe-long-trend-evaluation-status")).toContainText("partial");
  await expect(page.getByTestId("qe-long-trend-outcome-snapshot")).toHaveValue(snapshotId);
  await expect(page.getByTestId("qe-long-trend-evaluation-asof")).toHaveText("2026-06-30");
  await expect(page.getByTestId("qe-long-trend-family-execution_cause")).toContainText("QELT_EXECUTION_EVIDENCE_INSUFFICIENT");
  await expect(page.getByTestId("qe-long-trend-family-execution_cause")).toContainText("direct_cause_coverage: 0.25");
  await expect(page.getByTestId("qe-long-trend-coverage-censoring")).toContainText("right_censored 8");
  await expect(page.getByTestId("qe-long-trend-panel")).toContainText("blocked_limit_down 4");
  await expect(page.getByTestId("qe-long-trend-panel")).toContainText("reconciled_trade 80");
  await expect(page.getByTestId("qe-long-trend-horizon-table").getByText("0.1234")).toBeVisible();

  await page.getByTestId("qe-long-trend-preview").click();
  await expect(page.getByTestId("qe-long-trend-input-preview")).toContainText("技术提交就绪：是");
  await expect(page.getByTestId("qe-long-trend-preview-positions")).toContainText("QELT_POSITION_ARTIFACT_MISSING");
  await expect(page.getByTestId("qe-long-trend-create")).toBeEnabled();

  await page.getByTestId("qe-long-trend-create").click();
  await expect.poll(() => posts.length).toBe(1);
  expect(posts[0]).toEqual({ profile_id: "qe_long_trend_v1", outcome_dataset_snapshot_id: snapshotId });
  await expect(page.getByTestId("qe-long-trend-message")).toContainText("节点提交 未发生/无需");
});

test("new QE task sends only the immutable registered long-trend profile", async ({ page }) => {
  const createPayloads: Record<string, unknown>[] = [];
  page.on("dialog", async (dialog) => { await dialog.accept(); });

  await page.route(/\/api\/v1\/.*/, async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    if (url.pathname.endsWith("/quantevolver/evolution/source-experiments")) {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ status: "success", data: [{ experiment_id: "exp-profile-1", experiment_name: "Profile source", status: "completed", factor_count: 2 }] }),
      });
      return;
    }
    if (url.pathname.endsWith("/quantevolver/evolution/source-tasks")) {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "success", data: [] }) });
      return;
    }
    if (url.pathname.endsWith("/quantevolver/evolution/tasks") && request.method() === "POST") {
      createPayloads.push(await request.postDataJSON());
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "success", task_id: "exp-profile-1" }) });
      return;
    }
    if (url.pathname.endsWith("/quantevolver/evolution/tasks")) {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "success", data: [] }) });
      return;
    }
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "success", data: [] }) });
  });

  await page.goto("/quantevolver/evolution");
  await page.getByTestId("qe-create-task-open").click();
  await page.getByTestId("qe-create-task-name").fill("profile task");
  await page.getByTestId("qe-create-task-base-experiment").selectOption("exp-profile-1");
  await page.getByTestId("qe-create-task-target").fill("verify immutable profile");
  await page.getByTestId("qe-create-task-guidance").fill("keep the normal QE research flow");

  await page.getByTestId("qe-create-task-submit").click();
  await expect.poll(() => createPayloads.length).toBe(1);
  expect(createPayloads[0]).not.toHaveProperty("long_trend_profile_id");
  expect(createPayloads[0]).not.toHaveProperty("long_trend_enabled");

  await page.getByTestId("qe-create-task-open").click();
  await page.getByTestId("qe-create-task-name").fill("profile task enabled");
  await page.getByTestId("qe-create-task-base-experiment").selectOption("exp-profile-1");
  await page.getByTestId("qe-create-task-target").fill("verify immutable profile enabled");
  await page.getByTestId("qe-create-task-guidance").fill("keep the normal QE research flow");
  await page.getByTestId("qe-long-trend-task-profile-toggle").check();

  await expect(page.getByTestId("qe-long-trend-task-profile")).toContainText("20/40/60/120/180D");
  await expect(page.getByTestId("qe-long-trend-task-profile")).toContainText("页面不提供参数覆盖");
  await page.getByTestId("qe-create-task-submit").click();

  await expect.poll(() => createPayloads.length).toBe(2);
  expect(createPayloads[1].long_trend_profile_id).toBe("qe_long_trend_v1");
  expect(createPayloads[1]).not.toHaveProperty("long_trend_enabled");
  expect(createPayloads[1]).not.toHaveProperty("long_trend_horizons");
  expect(createPayloads[1]).not.toHaveProperty("long_trend_barriers");
});

test("QE Archive uses business search selectors, one outcome vintage and readable evidence", async ({ page }) => {
  let qualityUrl = "";
  await page.route(/\/api\/v1\/qe-archive\/.*/, async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith("/operator-options/runs")) {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ status: "success", data: { items: [{ value: "qear_run_fixture", task_name: "长期趋势 UI 验证任务", run_type: "evolution_loop", model_type: "LGBM", label_horizon: 60, factor_count: 42, loop_index: 1, completed_at: "2026-07-18T12:05:00+08:00" }], limit: 30 } }),
      });
      return;
    }
    if (url.pathname.endsWith("/analytics/long-trend-options")) {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          tasks: [{ value: "qe_long_trend_ui_task", task_name: "长期趋势 UI 验证任务", first_evaluation_asof: "2026-06-30", latest_evaluation_asof: "2026-06-30", evaluation_count: 1, snapshot_count: 1, model_types: ["LGBM"] }],
          snapshots: [{ value: snapshotId, first_evaluation_asof: "2026-06-30", latest_evaluation_asof: "2026-06-30", evaluation_count: 1, task_count: 1, task_names: ["长期趋势 UI 验证任务"] }],
          sectors: [{ value: "801010", sector_name: "农林牧渔", evaluation_count: 1, latest_evaluation_asof: "2026-06-30" }],
          limit: 30,
        }),
      });
      return;
    }
    if (url.pathname.endsWith("/analytics/topk-quality")) {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ status: "success", data: [
          { run_id: "historical_missing", topk_quality_status: null, topk_date_count: 0, topk_joined_observation_count: 0 },
          { run_id: "evidence_complete", topk_quality_status: "ok", topk_source: "pred_label_artifacts", topk_date_count: 126, topk_joined_observation_count: 25000, topk_return_20: 0.031, topk_return_50: 0.018, topk_hit_rate_20: 0.61, topk_hit_rate_50: 0.55 },
        ] }),
      });
      return;
    }
    if (url.pathname.endsWith("/analytics/long-trend-quality")) {
      qualityUrl = route.request().url();
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: [{
            evaluation_id: evaluationId,
            run_id: "qear_run_fixture",
            parent_task_id: "qe_long_trend_ui_task",
            parent_loop_index: 1,
            profile_id: "qe_long_trend_v1",
            evaluation_status: "partial",
            outcome_dataset_snapshot_id: snapshotId,
            evaluation_asof: "2026-06-30",
            model_type: "LGBM",
            factor_set_hash: "d".repeat(64),
            factor_count: 42,
            label_horizon: 60,
            random_seed: 123,
            metric_key: "rank_ic",
            metric_scope: "signal_path",
            horizon: 60,
            sector_code: null,
            dimension_key: "e".repeat(64),
            dimension_json: { slice: "all_oos", horizon: 60 },
            value_num: 0.1234,
            value_json: { icir: 0.5 },
            quality_flag: "complete",
          }],
          next_cursor: null,
          limit: 100,
        }),
      });
      return;
    }
    const data = url.pathname.endsWith("/health") ? {} : [];
    if (url.pathname.endsWith("/backfill-candidates")) {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "success", data: { status: "success", include_archived: false, count: 0, candidates: [] } }) });
      return;
    }
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "success", data }) });
  });

  await page.goto("/qe-archive");
  await expect(page.getByTestId("qe-long-trend-archive-comparison")).toBeVisible();
  await expect(page.getByTestId("qe-archive-quality-run-search")).toHaveAttribute("placeholder", /任务名称/);
  await expect(page.getByText("Run ID", { exact: true })).toHaveCount(0);
  await expect(page.getByText("1 完整 / 1 缺失", { exact: true })).toBeVisible();
  await expect(page.getByText("数据完整", { exact: true })).toBeVisible();
  await expect(page.getByText("pred_label_artifacts", { exact: true })).toHaveCount(0);
  await expect(page.getByText("missing_forward_only", { exact: true })).toHaveCount(0);
  await expect(page.getByTestId("qe-long-trend-archive-task-search")).toHaveAttribute("placeholder", /任务名称/);
  await expect(page.getByTestId("qe-long-trend-archive-snapshot-search")).toHaveAttribute("placeholder", /截止日期/);
  await expect(page.getByTestId("qe-long-trend-archive-task-select")).toHaveValue("qe_long_trend_ui_task");
  await expect(page.getByTestId("qe-long-trend-archive-snapshot-select")).toHaveValue(snapshotId);
  await expect(page.getByText("Task ID（可选）")).toHaveCount(0);
  await expect(page.getByText("Outcome snapshot（必填）")).toHaveCount(0);
  await expect(page.locator("pre")).toHaveCount(0);
  await page.getByTestId("qe-long-trend-archive-entry-status").selectOption("filled_t1");
  await page.getByTestId("qe-long-trend-archive-exit-status").selectOption("filled_on_exit_signal_day");
  await page.getByTestId("qe-long-trend-archive-entry-evidence").selectOption("reconciled_trade");
  await page.getByTestId("qe-long-trend-archive-exit-evidence").selectOption("position_transition");
  await page.getByTestId("qe-long-trend-archive-query").click();

  await expect(page.getByTestId("qe-long-trend-archive-table")).toContainText("LGBM");
  await expect(page.getByTestId("qe-long-trend-archive-table")).toContainText("123");
  await expect(page.getByTestId("qe-long-trend-archive-table")).toContainText("42");
  await expect(page.getByTestId("qe-long-trend-archive-table")).not.toContainText("qear_run_fixture");
  const query = new URL(qualityUrl).searchParams;
  expect(query.get("task_id")).toBe("qe_long_trend_ui_task");
  expect(query.get("outcome_dataset_snapshot_id")).toBe(snapshotId);
  expect(query.get("metric_key")).toBe("rank_ic");
  expect(query.get("horizon")).toBe("60");
  expect(query.get("entry_execution_status")).toBe("filled_t1");
  expect(query.get("exit_execution_status")).toBe("filled_on_exit_signal_day");
  expect(query.get("entry_execution_evidence_level")).toBe("reconciled_trade");
  expect(query.get("exit_execution_evidence_level")).toBe("position_transition");
  expect(query.get("limit")).toBe("100");
  await page.screenshot({ path: "../tmp/playwright-results-f014-phase4/qe-archive-human-search.png", fullPage: true });
});

test("live QE Archive UI reads the materialized F-014 canary without writes", async ({ page }) => {
  test.skip(process.env.QE_LONG_TREND_UI_LIVE !== "1", "set QE_LONG_TREND_UI_LIVE=1 for the read-only runtime smoke");
  const liveTaskSearch = process.env.QE_LONG_TREND_TASK_SEARCH || "";
  let qualityStatus = 0;
  page.on("response", (response) => {
    if (response.url().includes("/api/v1/qe-archive/analytics/long-trend-quality")) qualityStatus = response.status();
  });

  await page.goto("/qe-archive");
  if (liveTaskSearch) await page.getByTestId("qe-long-trend-archive-task-search").fill(liveTaskSearch);
  const taskSelect = page.getByTestId("qe-long-trend-archive-task-select");
  await expect(taskSelect.locator("option")).not.toHaveCount(1);
  if (!(await taskSelect.inputValue())) await taskSelect.selectOption({ index: 1 });
  const snapshotSelect = page.getByTestId("qe-long-trend-archive-snapshot-select");
  await expect(snapshotSelect.locator("option")).not.toHaveCount(1);
  if (!(await snapshotSelect.inputValue())) await snapshotSelect.selectOption({ index: 1 });
  await page.getByTestId("qe-long-trend-archive-query").click();

  await expect(page.getByTestId("qe-long-trend-archive-table").locator("tbody tr").first()).toBeVisible();
  await expect(page.getByTestId("qe-long-trend-archive-table")).not.toContainText("qear_run_");
  expect(qualityStatus).toBe(200);
  await page.screenshot({ path: "../tmp/playwright-results-f014-phase4/qe-archive-live-readonly.png", fullPage: true });
});
