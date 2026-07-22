import { expect, test, type Page, type Route } from "@playwright/test";

const taskId = "mact_grid";
const runId = "macb_grid";

function success(route: Route, data: unknown) {
  return route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "success", data }) });
}

async function mocks(page: Page) {
  const task = { task_id: taskId, task_name: "Grid fixture", task_type: "multi_alpha_combine", status: "running", current_loop: 1, max_loops: 1, created_at: "2026-07-22T01:00:00Z", updated_at: "2026-07-22T01:05:00Z", roster_hash: "r", normalize_method: "zscore", walk_forward_signature: "wf" };
  const loop = { loop_id: runId, loop_index: 1, run_id: runId, status: "running", raw_status: "running", phase: "reconciling", retryable: true, deletable: false, config_json: { runtime_flags: { run_id: runId }, strategy_params: { topk: 20 } }, metrics_json: {}, created_at: task.created_at, updated_at: task.updated_at };
  await page.route(new RegExp(`/api/v1/multi-alpha/combine/tasks/${taskId}`), (route) => success(route, { task, loops: [loop], scheme: "equal", available_schemes: ["equal"] }));
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${runId}/logs`), (route) => success(route, { run_id: runId, status: "running", history_available: true, events: [], files: [] }));
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${runId}/archive-status`), (route) => success(route, { run_id: runId, archive_status: "not_archived" }));
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${runId}/control-capabilities`), (route) => success(route, { run_id: runId, run_status: "running", actions: {}, evidence: {} }));
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${runId}/commands`), (route) => success(route, { commands: [] }));
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${runId}/children(?:\\?.*)?$`), (route) => success(route, { children: [
    { child_id: "child_2", child_key: "scheme:risk", child_kind: "scheme", ordinal: "10", status: "failed", phase: "reconciling", selected_attempt_id: "attempt_2", error_code: "remote_result_missing", artifact_manifest_json: { prediction: "sha" }, updated_at: "2026-07-22T01:04:00Z", attempts: [{ attempt_id: "attempt_2", attempt_no: 2, status: "failed", phase: "collect", node_id: "rdagent-node1", qe_task_id: "qe_task_2", qe_loop_id: "L2", selected: true, error_code: "artifact_missing", error_json: { path: "pred.pkl" }, artifact_manifest_json: { logs: ["run.log"] } }] },
    { child_id: "child_1", child_key: "scheme:equal", child_kind: "scheme", ordinal: 2, status: "running", phase: "remote", selected_attempt_id: "attempt_1", updated_at: "2026-07-22T01:03:00Z", attempts: [{ attempt_id: "attempt_1", attempt_no: "1", status: "running", phase: "remote", node_id: "wsl2-5080", qe_task_id: "qe_task_1", qe_loop_id: 1, selected: true, heartbeat_at: "2026-07-22T01:03:00Z", environment_identity_hash: "e".repeat(64), dataset_identity_hash: "d".repeat(64) }] },
  ] }));
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${runId}/events\?`), (route) => success(route, { run_id: runId, events: [{ event_id: 8, run_id: runId, child_id: "child_2", attempt_id: "attempt_2", event_type: "failed", reason_code: "artifact_missing", created_at: "2026-07-22T01:04:00Z" }], count: 1, after_event_id: 0, next_event_id: 8, has_more: false }));
  await page.route(new RegExp(`/api/v1/multi-alpha/combine-backtest/runs/${runId}/events/stream`), (route) => route.fulfill({ status: 200, contentType: "text/event-stream", body: "event: stream_end\ndata: {\"run_id\":\"macb_grid\"}\n\n" }));
}

test("child grid sorts mixed ordinals and exposes exact attempt identities and errors", async ({ page }) => {
  await mocks(page);
  await page.goto(`/quantevolver/evolution?task_type=multi_alpha_combine&task_id=${taskId}&tab=runtime`);

  await expect(page.getByText("Child / Attempt 权威明细", { exact: true })).toBeVisible();
  const grid = page.getByTestId("multi-alpha-child-grid");
  const rows = grid.locator("tbody > tr").filter({ has: page.locator("td") });
  await expect(rows.nth(0)).toContainText("scheme:equal");
  await rows.nth(0).getByRole("button").first().click();
  await expect(page.getByText("qe_task=qe_task_1", { exact: true })).toBeVisible();
  await expect(page.getByText("node=wsl2-5080", { exact: true })).toBeVisible();

  await page.getByLabel("仅错误").check();
  await expect(grid.getByText("remote_result_missing", { exact: false })).toBeVisible();
  await grid.locator("tbody > tr").first().getByRole("button").first().click();
  await expect(grid.getByText("error=artifact_missing", { exact: true })).toBeVisible();
  await expect(page.getByText("Durable DB events（1）", { exact: true })).toBeVisible();
});
