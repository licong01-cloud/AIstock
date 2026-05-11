import { expect, test } from "@playwright/test";
import type { Page } from "@playwright/test";

const MODELS = [
  {
    id: 1,
    dev_version: "v24",
    roll_tag: "B1",
    version_tag: "v24-B1",
    dev_description: "v24 fixed minute price bins",
    parent_dev: "v20",
    policy_path: "/models/v24-B1.pt",
    train_type: "rolling",
    train_start: "2024-06-01",
    train_end: "2024-12-31",
    train_epochs: 120,
    train_duration_sec: 3600,
    state_dim: 64,
    action_dim: 4,
    network_arch: "MLP-128x2",
    eval_pa_bps: 6.35,
    eval_ffr: 0.91,
    eval_oracle_gap_bps: 4.2,
    eval_vs_twap_bps: 1.1,
    eval_urgency_cost_bps: 0.4,
    status: "active",
    created_at: "2025-01-05T10:00:00",
    activated_at: "2025-01-08T11:00:00",
  },
  {
    id: 2,
    dev_version: "v20",
    roll_tag: "A2",
    version_tag: "v20-A2",
    dev_description: "v20 production baseline",
    parent_dev: "v18",
    policy_path: "/models/v20-A2.pt",
    train_type: "rolling",
    train_start: "2024-01-01",
    train_end: "2024-06-30",
    train_epochs: 100,
    eval_pa_bps: 5.1,
    eval_ffr: 0.88,
    eval_oracle_gap_bps: 4.9,
    eval_vs_twap_bps: 0.9,
    eval_urgency_cost_bps: 0.5,
    status: "archived",
    created_at: "2024-07-01T09:00:00",
    activated_at: "2024-07-05T09:00:00",
  },
  {
    id: 3,
    dev_version: "v24",
    roll_tag: "B2",
    version_tag: "v24-B2",
    dev_description: null,
    parent_dev: "v24",
    policy_path: "/models/v24-B2.pt",
    train_type: "rolling",
    eval_pa_bps: 4.0,
    eval_ffr: 0.85,
    status: "candidate",
    created_at: "2025-02-10T10:00:00",
    activated_at: null,
  },
];

const DEV_LINEAGE = [
  {
    dev_version: "v24",
    dev_description: "v24 fixed minute price bins",
    parent_dev: "v20",
    roll_count: 2,
    latest_train_end: "2024-12-31",
    roll_tags: ["B1", "B2"],
  },
  {
    dev_version: "v20",
    dev_description: "v20 production baseline",
    parent_dev: "v18",
    roll_count: 1,
    latest_train_end: "2024-06-30",
    roll_tags: ["A2"],
  },
];

type MockOptions = {
  modelsStatus?: number;
  modelCallSpy?: { calls: URLSearchParams[] };
  models?: typeof MODELS;
};

async function mockApi(page: Page, options: MockOptions = {}) {
  await page.route("**/api/v1/rl-execution/**", async (route) => {
    const url = route.request().url();
    const respond = (data: unknown, status = 200) =>
      route.fulfill({ status, contentType: "application/json", body: JSON.stringify(data) });

    if (url.includes("/dev-versions")) {
      return respond(DEV_LINEAGE);
    }

    if (url.includes("/models")) {
      const parsedUrl = new URL(url);
      if (options.modelCallSpy) options.modelCallSpy.calls.push(parsedUrl.searchParams);
      if (options.modelsStatus && options.modelsStatus >= 400) {
        return respond({ detail: "models_unavailable" }, options.modelsStatus);
      }
      const dataset = options.models ?? MODELS;
      const dev = parsedUrl.searchParams.get("dev_version");
      const status = parsedUrl.searchParams.get("status");
      let filtered = dataset;
      if (dev) filtered = filtered.filter((m) => m.dev_version === dev);
      if (status) filtered = filtered.filter((m) => m.status === status);
      return respond(filtered);
    }

    return respond({ detail: "unexpected route" }, 404);
  });
}

test("rl-execution page renders models, dev lineage and summary metrics", async ({ page }) => {
  await mockApi(page);
  await page.goto("/rl-execution");
  await expect(page.getByText("RL 执行模型")).toBeVisible();
  await expect(page.getByTestId("model-v24-B1")).toBeVisible();
  await expect(page.getByTestId("dev-v24")).toBeVisible();
  await expect(page.getByTestId("pa-v24-B1")).toContainText("6.35 bps");
});

test("status filter forwards status query parameter to /models", async ({ page }) => {
  const spy = { calls: [] as URLSearchParams[] };
  await mockApi(page, { modelCallSpy: spy });
  await page.goto("/rl-execution");
  await expect(page.getByTestId("model-v24-B1")).toBeVisible();
  await page.getByTestId("status-filter").selectOption("archived");
  await expect.poll(() => spy.calls.some((p) => p.get("status") === "archived")).toBe(true);
  // After filter applies, only v20-A2 remains
  await expect(page.getByTestId("model-v20-A2")).toBeVisible();
  await expect(page.getByTestId("model-v24-B1")).not.toBeVisible();
});

test("dev_version filter forwards dev_version query parameter", async ({ page }) => {
  const spy = { calls: [] as URLSearchParams[] };
  await mockApi(page, { modelCallSpy: spy });
  await page.goto("/rl-execution");
  await expect(page.getByTestId("model-v24-B1")).toBeVisible();
  await page.getByTestId("dev-filter").selectOption("v24");
  await expect.poll(() => spy.calls.some((p) => p.get("dev_version") === "v24")).toBe(true);
  await expect(page.getByTestId("model-v20-A2")).not.toBeVisible();
});

test("refresh button triggers an additional models fetch", async ({ page }) => {
  const spy = { calls: [] as URLSearchParams[] };
  await mockApi(page, { modelCallSpy: spy });
  await page.goto("/rl-execution");
  await expect(page.getByTestId("model-v24-B1")).toBeVisible();
  const before = spy.calls.length;
  await page.getByTestId("refresh-rl").click();
  await expect.poll(() => spy.calls.length).toBeGreaterThan(before);
});

test("models API failure surfaces the error panel without crashing", async ({ page }) => {
  const pageErrors: string[] = [];
  page.on("pageerror", (error) => pageErrors.push(error.message));
  await mockApi(page, { modelsStatus: 500 });
  await page.goto("/rl-execution");
  await expect(page.getByText("RL Execution API 调用失败")).toBeVisible();
  await expect(page.getByText("RL 执行模型")).toBeVisible();
  expect(pageErrors).toEqual([]);
});
