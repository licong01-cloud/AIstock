import { expect, test, type Route } from "@playwright/test";

function success(route: Route, data: unknown) {
  return route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "success", data }) });
}

test("formal composer submits full independent scenario payloads without training calls", async ({ page }) => {
  const requests: Array<{ url: string; body: Record<string, unknown>; idempotencyKey?: string }> = [];
  await page.route(/\/api\/v1\/multi-alpha\/combine\/tasks\?/, (route) => success(route, { tasks: [], count: 0, total: 0, limit: 50, offset: 0 }));
  await page.route(/\/api\/v1\/multi-alpha\/combine-backtest\/run$/, async (route) => {
    requests.push({ url: route.request().url(), body: await route.request().postDataJSON(), idempotencyKey: route.request().headers()["idempotency-key"] });
    return success(route, { run_id: `macb_${requests.length}`, task_id: "mact_shared", status: "queued" });
  });
  page.on("request", (request) => {
    if (/train|experiment.*run/i.test(request.url())) requests.push({ url: `UNEXPECTED:${request.url()}`, body: {} });
  });

  await page.goto("/quantevolver/evolution?task_type=multi_alpha_combine");
  await page.getByRole("button", { name: "创建组合回测" }).click();
  const seeds = page.getByLabel("seed run ids（逗号或换行）");
  await seeds.nth(0).fill("qe_trend_1\nqe_trend_2");
  await seeds.nth(1).fill("qe_sector_1");
  await page.getByRole("button", { name: "添加场景" }).click();
  await page.getByRole("button", { name: /创建 2 个场景 run/ }).click();

  await expect(page.getByText("逐场景提交结果", { exact: true })).toBeVisible();
  expect(requests.filter((item) => !item.url.startsWith("UNEXPECTED:"))).toHaveLength(2);
  expect(requests.some((item) => item.url.startsWith("UNEXPECTED:"))).toBe(false);
  for (const request of requests) {
    expect(request.idempotencyKey).toMatch(/^multi-alpha-create-scenario_/);
    expect(request.body).toMatchObject({
      oos_start: "2024-07-01",
      oos_end: "2026-06-29",
      normalize_method: "zscore",
      baseline_leg_id: "trend_leg",
      run_async: true,
      min_date_coverage: 0.8,
    });
    expect(request.body.roster).toEqual([
      { leg_id: "trend_leg", seed_run_ids: ["qe_trend_1", "qe_trend_2"], metadata: { family: "trend" } },
      { leg_id: "sector_leg", seed_run_ids: ["qe_sector_1"], metadata: { family: "sector" } },
    ]);
    expect(request.body.weighting_schemes).toEqual(["equal", "orthogonality_aware", "ic_weighted", "risk_parity"]);
    expect(request.body.walk_forward).toEqual({ enabled: true, window: 60, min_periods: 20, expanding: false });
    expect(request.body.rank_fusion).toEqual({});
    expect(request.body.scheme_timeout_seconds).toBe(7200);
    expect(request.body.run_timeout_seconds).toBe(28800);
    expect(request.body.wait_timeout_seconds).toBeNull();
  }
  expect((requests[0].body.backtest_config as Record<string, unknown>).topk).not.toBe((requests[1].body.backtest_config as Record<string, unknown>).topk);
});
