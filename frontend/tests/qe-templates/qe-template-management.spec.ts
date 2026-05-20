import { expect, test } from "@playwright/test";

type Template = Record<string, unknown> & {
  template_id: string;
  template_kind: "single_experiment" | "custom_evo";
  status: string;
  title: string;
  config_json: Record<string, unknown>;
  archive_policy: string;
};

const singleTemplate: Template = {
  template_id: "qet_single_demo",
  template_kind: "single_experiment",
  status: "draft",
  title: "MCP 单次实验候选",
  description: "由 MCP 基于历史数仓提出",
  config_json: {
    experiment_name: "mcp_single_demo",
    factor_names: ["alpha_mom", "alpha_vol"],
    factor_sources: { alpha_mom: "sota", alpha_vol: "sota" },
    model_id: "lgbm_v1",
    strategy_id: "topk",
    label_horizon: 5,
    node_id: "wsl",
    data_split: {
      train_start: "2018-01-01",
      train_end: "2022-12-31",
      valid_start: "2023-01-01",
      valid_end: "2023-12-31",
      test_start: "2024-01-01",
      test_end: "2024-12-31",
      backtest_end: "2024-12-31",
    },
    custom_params: {
      topk: 50,
      n_drop: 5,
      execution_algo: "TWAP",
      execution_algo_params: { participation_rate: 0.2 },
      label_horizon: 5,
    },
  },
  archive_policy: "AUTO",
  created_by_type: "agent",
  created_by_name: "codex-mcp",
  created_at: "2026-05-16T08:00:00+08:00",
  updated_at: "2026-05-16T08:30:00+08:00",
  validation_json: {},
  approval_json: {},
};

const customTemplate: Template = {
  template_id: "qet_custom_demo",
  template_kind: "custom_evo",
  status: "ready_for_review",
  title: "MCP 自定义演进候选",
  description: "两个 loop 的自定义演进模板",
  config_json: {
    task_name: "mcp_custom_evo_demo",
    target_desc: "验证 seed 与模型超参组合",
    base_experiment_id: "qe_base_1",
    node_parallelism: 2,
    loops: [
      {
        label: "Loop A",
        factor_keys: ["alpha_mom"],
        model_id: "catboost",
        strategy_id: "topk",
        strategy_params: { topk: 50 },
        execution_algo: "TWAP",
        execution_algo_params: { participation_rate: 0.2 },
        node_id: "wsl",
        seed: 7,
        label_horizon: 5,
      },
      {
        label: "Loop B",
        factor_keys: ["alpha_mom", "alpha_size"],
        model_id: "lgbm",
        strategy_id: "topk",
        strategy_params: { topk: 30 },
        execution_algo: "CLOSE_PRICE",
        node_id: "local",
        seed: 11,
        label_horizon: 3,
      },
    ],
  },
  archive_policy: "MANUAL_ONLY",
  created_by_type: "agent",
  created_by_name: "codex-mcp",
  created_at: "2026-05-16T08:05:00+08:00",
  updated_at: "2026-05-16T08:31:00+08:00",
  validation_json: { valid: true, errors: [], warnings: ["远端 CPU-only 为软限制"] },
  approval_json: {},
};

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

async function installTemplateMocks(page: import("@playwright/test").Page) {
  let currentSingle = clone(singleTemplate);
  let currentCustom = clone(customTemplate);
  const calls: string[] = [];
  const updates: Record<string, unknown>[] = [];

  await page.route("**/api/v1/quantevolver/strategies**", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      ok: true,
      items: [
        {
          strategy_id: "topk",
          display_name: "TopK 策略",
          default_kwargs: { topk: 50, n_drop: 5 },
          param_schema: [
            { name: "topk", type: "int", default: 50, min: 1, max: 200, title: "topk" },
            { name: "n_drop", type: "int", default: 5, min: 0, max: 50, title: "n_drop" },
          ],
        },
      ],
    }),
  }));
  await page.route("**/api/v1/quantevolver/execution-algorithms**", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      ok: true,
      items: [
        {
          algo_code: "TWAP",
          algo_name: "TWAP",
          qe_supported: true,
          default_config: { participation_rate: 0.2 },
          param_schema: [{ name: "participation_rate", type: "float", default: 0.2, min: 0.01, max: 1 }],
        },
        { algo_code: "CLOSE_PRICE", algo_name: "Close Price", qe_supported: true, default_config: {} },
      ],
    }),
  }));
  await page.route("**/api/v1/dispatch/nodes**", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify([{ node_id: "wsl", display_name: "WSL" }, { node_id: "local", display_name: "Local" }]),
  }));
  await page.route("**/api/v1/hmm-training/configs**", async (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify([{ config_id: "hmm_cfg_1", display_name: "Sector HMM" }]),
  }));

  await page.route("**/api/v1/qe-templates**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const method = request.method();
    const respond = (data: unknown, status = 200) => route.fulfill({
      status,
      contentType: "application/json",
      body: JSON.stringify(data),
    });
    const path = url.pathname;

    if (method === "GET" && path.endsWith("/api/v1/qe-templates")) {
      return respond({ status: "success", data: [currentSingle, currentCustom] });
    }

    const id = decodeURIComponent(path.split("/qe-templates/")[1]?.split("/")[0] || "");
    const selected = id === currentCustom.template_id ? currentCustom : currentSingle;

    if (method === "GET" && path.endsWith(`/qe-templates/${id}`)) {
      return respond({ status: "success", data: selected });
    }

    if (method === "PUT" && path.endsWith(`/qe-templates/${id}`)) {
      calls.push("update");
      const body = request.postDataJSON() as Record<string, unknown>;
      updates.push(body);
      if (id === currentCustom.template_id) currentCustom = { ...currentCustom, ...body, status: "draft" } as Template;
      else currentSingle = { ...currentSingle, ...body, status: "draft" } as Template;
      return respond({ status: "success", data: id === currentCustom.template_id ? currentCustom : currentSingle });
    }

    if (method === "POST" && path.endsWith(`/qe-templates/${id}/validate`)) {
      calls.push("validate");
      const validation = { valid: true, errors: [], warnings: id === currentCustom.template_id ? ["远端 CPU-only 为软限制"] : [] };
      if (id === currentCustom.template_id) currentCustom = { ...currentCustom, status: "ready_for_review", validation_json: validation };
      else currentSingle = { ...currentSingle, status: "ready_for_review", validation_json: validation };
      return respond({ status: "success", data: { template: id === currentCustom.template_id ? currentCustom : currentSingle, validation } });
    }

    if (method === "POST" && path.endsWith(`/qe-templates/${id}/approve`)) {
      calls.push("approve");
      if (id === currentCustom.template_id) currentCustom = { ...currentCustom, status: "approved", approval_json: request.postDataJSON() as Record<string, unknown> };
      else currentSingle = { ...currentSingle, status: "approved", approval_json: request.postDataJSON() as Record<string, unknown> };
      return respond({ status: "success", data: id === currentCustom.template_id ? currentCustom : currentSingle });
    }

    if (method === "POST" && path.endsWith(`/qe-templates/${id}/materialize`)) {
      calls.push("materialize");
      if (id === currentCustom.template_id) currentCustom = { ...currentCustom, status: "materialized", submitted_task_id: "qe_task_from_template" };
      else currentSingle = { ...currentSingle, status: "materialized", submitted_experiment_id: "qe_exp_from_template" };
      return respond({ status: "success", data: { template: id === currentCustom.template_id ? currentCustom : currentSingle, materialized: { ok: true } } });
    }

    if (method === "POST" && path.endsWith(`/qe-templates/${id}/run`)) {
      calls.push("run");
      const body = request.postDataJSON() as Record<string, unknown>;
      if (id === currentCustom.template_id) {
        expect(body.confirm_run).toBe("QE_CUSTOM_EVO_RUN");
        currentCustom = { ...currentCustom, status: "run_requested" };
      } else {
        expect(body.confirm_run).toBe("QE_EXPERIMENT_RUN");
        currentSingle = { ...currentSingle, status: "run_requested" };
      }
      return respond({ status: "success", data: { template_id: id, run_result: { accepted: true } } });
    }

    if (method === "POST" && path.endsWith(`/qe-templates/${id}/supersede`)) {
      if (id === currentCustom.template_id) currentCustom = { ...currentCustom, status: "superseded" };
      else currentSingle = { ...currentSingle, status: "superseded" };
      return respond({ status: "success", data: id === currentCustom.template_id ? currentCustom : currentSingle });
    }

    return respond({ detail: `unexpected route ${method} ${path}` }, 404);
  });

  return { calls, updates };
}

test("QE templates list shows MCP single and custom evolution templates", async ({ page }) => {
  const pageErrors: string[] = [];
  const consoleErrors: string[] = [];
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("console", (message) => { if (message.type() === "error") consoleErrors.push(message.text()); });
  await installTemplateMocks(page);

  await page.goto("/quantevolver/templates");
  await expect(page.getByText("QE 待执行实验管理台")).toBeVisible();
  await expect(page.getByText("MCP 单次实验候选")).toBeVisible();
  await expect(page.getByText("MCP 自定义演进候选")).toBeVisible();
  await expect(page.getByText("QE 单次实验").first()).toBeVisible();
  await expect(page.getByText("自定义演进").first()).toBeVisible();

  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
});

test("operator can edit, save, validate and execute a single experiment template", async ({ page }) => {
  const mock = await installTemplateMocks(page);
  page.on("dialog", (dialog) => dialog.accept());

  await page.goto("/quantevolver/templates/qet_single_demo");
  await expect(page.getByText("结构化配置编辑")).toBeVisible();
  await expect(page.getByLabel("full template config json")).toHaveCount(0);
  await expect(page.getByText("完整配置 JSON")).toHaveCount(0);
  await page.getByLabel("single model id").fill("catboost_v2");
  await page.getByLabel("single execution algo").fill("CLOSE_PRICE");
  await page.getByLabel("single factor names").fill("alpha_new\nalpha_quality");
  await page.getByRole("button", { name: "保存配置" }).click();
  await expect(page.getByText("模板配置已保存，尚未执行")).toBeVisible();
  const updatePayload = mock.updates.at(-1);
  expect(updatePayload).toBeTruthy();
  const config = updatePayload?.config_json as Record<string, unknown>;
  expect(config.model_id).toBe("catboost_v2");
  expect(config.factor_names).toEqual(["alpha_new", "alpha_quality"]);
  expect((config.custom_params as Record<string, unknown>).execution_algo).toBe("CLOSE_PRICE");
  await page.getByRole("button", { name: "保存并执行" }).click();
  await expect(page.getByText(/执行请求已提交/)).toBeVisible();
  await expect(page.getByRole("link", { name: "查看运行详情" })).toBeVisible();
  expect(mock.calls).toEqual(["update", "validate", "approve", "materialize", "run"]);
});

test("custom evolution template edits loop details and executes through custom confirm token", async ({ page }) => {
  const mock = await installTemplateMocks(page);
  page.on("dialog", (dialog) => dialog.accept());

  await page.goto("/quantevolver/templates/qet_custom_demo");
  await expect(page.getByText("Loop 1: Loop A")).toBeVisible();
  await expect(page.getByText("Loop 2: Loop B")).toBeVisible();
  await expect(page.getByLabel("full template config json")).toHaveCount(0);
  await expect(page.getByText("完整配置 JSON")).toHaveCount(0);
  await expect(page.getByText("现有自动演进页面")).toBeVisible();
  await page.getByLabel("loop 1 label", { exact: true }).fill("Loop A patched");
  await page.getByLabel("loop 1 model id").fill("xgb_v2");
  await page.getByLabel("loop 1 factor keys").fill("alpha_mom\nalpha_quality");
  await page.getByRole("button", { name: "保存配置" }).click();
  await expect(page.getByText("模板配置已保存，尚未执行")).toBeVisible();
  const updatePayload = mock.updates.at(-1);
  expect(updatePayload).toBeTruthy();
  const config = updatePayload?.config_json as Record<string, unknown>;
  const loops = config.loops as Record<string, unknown>[];
  expect(loops[0].label).toBe("Loop A patched");
  expect(loops[0].model_id).toBe("xgb_v2");
  expect(loops[0].factor_keys).toEqual(["alpha_mom", "alpha_quality"]);
  await page.getByRole("button", { name: "保存并执行" }).click();
  await expect(page.getByText(/执行请求已提交/)).toBeVisible();
  await expect(page.getByRole("link", { name: "查看运行详情" })).toBeVisible();
  expect(mock.calls).toEqual(["update", "validate", "approve", "materialize", "run"]);
});
