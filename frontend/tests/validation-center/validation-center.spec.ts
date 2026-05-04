import { expect, test } from "@playwright/test";

const passedRunId = "validation_center_20260504_l2_api__abc123";
const markdownOnlyRunId = "validation_center_20260504_l1_markdown_only__def456";
const malformedRunId = "validation_center_20260504_l1_malformed__bad789";
const coverageId = "validation_center_coverage_demo__cov123";
const evidenceId = "validation_center_evidence_demo__evd123";

const runItems = [
  {
    run_id: passedRunId,
    module: "validation_center",
    module_slug: "validation_center",
    level: "L2",
    title: "Validation API Run",
    status: "passed",
    git_commit: "abc1234",
    operator: "codex",
    started_at: "2026-05-04T12:00:00+08:00",
    finished_at: "2026-05-04T12:02:00+08:00",
    markdown_path: "tests/aistock_validation/history/validation_center/20260504_l2_validation-api.md",
    metadata_path: "tests/aistock_validation/history/validation_center/20260504_l2_validation-api.json",
    metadata_missing: false,
    metadata_parse_error: null,
    source_type: "markdown_with_json",
    coverage: { status: "passed", line: 84.99, branch: 71.74 },
    coverage_snapshot_id: coverageId,
    coverage_missing: false,
    evidence_manifest_id: evidenceId,
    evidence_missing: false,
    pass_scope: {
      level: "L2",
      real_backend: true,
      real_database: false,
      real_node_api: false,
      real_frontend_click: false,
      writes_business_state: false,
      positive_business_success: false,
      negative_failfast_only: false,
      mock_api_used: false,
      production_8001_touched: false,
    },
    business_assertion: {
      can_user_complete_operation: false,
      operation_name: "read validation history",
      evidence: { api: "FastAPI TestClient", ui: "mock UI E2E", db: "not touched", logs: "nox output" },
      unresolved_blockers: ["UI first-stage read-only display"],
    },
    success_scope_recorded: true,
    quality_gates: [{ metric: "line", status: "passed", actual: 84.99, threshold: 75 }],
    parse_error: null,
  },
  {
    run_id: markdownOnlyRunId,
    module: "validation_center",
    module_slug: "validation_center",
    level: "L1",
    title: "Markdown Only",
    status: "unknown",
    git_commit: null,
    operator: null,
    started_at: "2026-05-04T11:00:00+08:00",
    finished_at: null,
    markdown_path: "tests/aistock_validation/history/validation_center/20260504_l1_markdown-only.md",
    metadata_path: null,
    metadata_missing: true,
    metadata_parse_error: null,
    source_type: "markdown_only",
    coverage: null,
    coverage_snapshot_id: null,
    coverage_missing: true,
    evidence_manifest_id: null,
    evidence_missing: true,
    pass_scope: null,
    business_assertion: null,
    success_scope_recorded: false,
    quality_gates: [],
    parse_error: null,
  },
  {
    run_id: malformedRunId,
    module: "validation_center",
    module_slug: "validation_center",
    level: "L1",
    title: "Malformed Metadata",
    status: "unknown",
    git_commit: null,
    operator: null,
    started_at: "2026-05-04T10:00:00+08:00",
    finished_at: null,
    markdown_path: "tests/aistock_validation/history/validation_center/20260504_l1_malformed.md",
    metadata_path: "tests/aistock_validation/history/validation_center/20260504_l1_malformed.json",
    metadata_missing: false,
    metadata_parse_error: "invalid JSON: Expecting property name enclosed in double quotes",
    source_type: "markdown_only",
    coverage: null,
    coverage_snapshot_id: null,
    coverage_missing: true,
    evidence_manifest_id: null,
    evidence_missing: true,
    pass_scope: null,
    business_assertion: null,
    success_scope_recorded: false,
    quality_gates: [],
    parse_error: "invalid JSON: Expecting property name enclosed in double quotes",
  },
];

const coverageSummary = {
  snapshot_id: coverageId,
  schema_version: "aistock_validation_coverage_snapshot_v1",
  module: "validation_center",
  level: "L2",
  title: "Validation Center read-only API coverage",
  run_id: passedRunId,
  generated_at: "2026-05-04T12:03:00+08:00",
  git_commit: "abc1234",
  status: "passed",
  snapshot_path: "tmp/validation/coverage/validation_center_backend_snapshot.json",
  totals: { lines_valid: 500, lines_covered: 425, line_percent: 84.99, branches_valid: 200, branches_covered: 143, branch_percent: 71.74 },
  diff: { enabled: false, line_percent: null, files: [] },
  quality_gates: [{ metric: "line", status: "passed", actual: 84.99, threshold: 75 }],
  failed_gates: [],
};

const evidenceSummary = {
  manifest_id: evidenceId,
  schema_version: "aistock_validation_evidence_manifest_v1",
  module: "validation_center",
  level: "L2",
  title: "Validation Center read-only API evidence",
  run_id: passedRunId,
  generated_at: "2026-05-04T12:04:00+08:00",
  git_commit: "abc1234",
  manifest_path: "tests/aistock_validation/history/validation_center/20260504_l2_validation-api-evidence.json",
  missing_count: 0,
  evidence_count: 3,
  missing: [],
};

test("Validation Center read-only UI uses mocked validation APIs", async ({ page }) => {
  const consoleErrors: string[] = [];
  const pageErrors: string[] = [];
  const requestFailures: string[] = [];
  const badResponses: string[] = [];
  const writeMethods: string[] = [];

  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("requestfailed", (request) => requestFailures.push(`${request.method()} ${request.url()}`));
  page.on("response", (response) => {
    if (response.url().includes("/api/v1/validation/") && response.status() >= 400) {
      badResponses.push(`${response.status()} ${response.url()}`);
    }
  });

  await page.route("**/api/v1/validation/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname;
    const method = request.method();
    const respond = (data: unknown, status = 200) => route.fulfill({
      status,
      contentType: "application/json",
      body: JSON.stringify(data),
    });

    if (method !== "GET") {
      writeMethods.push(`${method} ${path}`);
      return respond({ detail: "Validation Center UI is read-only" }, 405);
    }

    if (path.endsWith("/api/v1/validation/health")) {
      return respond({
        status: "success",
        data: {
          status: "ok",
          mode: "read_only",
          history: {
            mode: "read_only",
            history_root: "tests/aistock_validation/history",
            exists: true,
            run_count: 3,
            coverage_snapshot_count: 1,
            evidence_manifest_count: 1,
          },
          plan_catalog: { catalog_path: "tests/aistock_validation/catalog/test_plans.yaml", missing: false, plan_count: 2 },
          production_8001_touched: false,
        },
      });
    }

    if (path.endsWith("/api/v1/validation/summary")) {
      return respond({
        status: "success",
        data: {
          history_root: "tests/aistock_validation/history",
          run_count: 3,
          coverage_snapshot_count: 1,
          evidence_manifest_count: 1,
          plan_count: 2,
          runs_by_status: { passed: 1, unknown: 2 },
          modules: [{ module: "validation_center", run_count: 3, latest_run: runItems[0] }],
          latest_runs: runItems,
          latest_coverage: coverageSummary,
        },
      });
    }

    if (path.endsWith("/api/v1/validation/plans")) {
      return respond({
        status: "success",
        data: {
          catalog_path: "tests/aistock_validation/catalog/test_plans.yaml",
          missing: false,
          plans: [
            {
              plan_key: "validation_center_backend",
              title: "Validation Center backend contract",
              module: "validation_center",
              level: "L2",
              command_key: "nox_validation_center_backend",
              nox_session: "validation_center_backend",
              enabled: true,
              requires_backend: false,
              requires_frontend: false,
              allowed_backend_ports: [],
              allowed_frontend_ports: [],
              writes_database: false,
              writes_artifacts: true,
              writes_business_state: false,
            },
            {
              plan_key: "validation_center_ui",
              title: "Validation Center read-only UI",
              module: "validation_center",
              level: "L3",
              command_key: "nox_validation_center_ui",
              nox_session: "validation_center_ui",
              enabled: true,
              requires_backend: false,
              requires_frontend: true,
              allowed_backend_ports: [8011, 8012],
              allowed_frontend_ports: [3011, 3012],
              writes_database: false,
              writes_artifacts: true,
              writes_business_state: false,
            },
          ],
        },
      });
    }

    if (path.endsWith("/api/v1/validation/runs")) {
      const pageNumber = Number(url.searchParams.get("page") || "1");
      const pageSize = Number(url.searchParams.get("page_size") || "20");
      const search = (url.searchParams.get("search") || "").toLowerCase();
      const level = (url.searchParams.get("level") || "").toUpperCase();
      const includeMarkdownOnly = url.searchParams.get("include_markdown_only") !== "false";
      expect(pageSize).toBeGreaterThanOrEqual(10);
      let items = runItems;
      if (!includeMarkdownOnly) items = items.filter((item) => !item.metadata_missing);
      if (level) items = items.filter((item) => item.level === level);
      if (search) items = items.filter((item) => `${item.run_id} ${item.title} ${item.markdown_path}`.toLowerCase().includes(search));
      const start = (pageNumber - 1) * pageSize;
      return respond({
        status: "success",
        data: {
          items: items.slice(start, start + pageSize),
          total: items.length,
          page: pageNumber,
          page_size: pageSize,
          has_more: start + pageSize < items.length,
        },
      });
    }

    if (path.endsWith(`/api/v1/validation/runs/${passedRunId}`)) {
      return respond({ status: "success", data: { ...runItems[0], markdown_text: "# Validation API Run\n\n- Final status: PASS\n", metadata: { title: "Validation API Run" }, coverage_snapshot: coverageSummary, evidence_manifest: evidenceSummary } });
    }

    if (path.endsWith(`/api/v1/validation/runs/${markdownOnlyRunId}`)) {
      return respond({ status: "success", data: { ...runItems[1], markdown_text: "# Markdown Only\n\nNo JSON sidecar.\n", metadata: null, coverage_snapshot: null, evidence_manifest: null } });
    }

    if (path.endsWith("/api/v1/validation/coverage")) {
      return respond({ status: "success", data: { items: [coverageSummary], total: 1, page: 1, page_size: 10, has_more: false } });
    }

    if (path.endsWith(`/api/v1/validation/coverage/${coverageId}`)) {
      return respond({ status: "success", data: { summary: coverageSummary, snapshot: { ...coverageSummary, files: [] } } });
    }

    if (path.endsWith("/api/v1/validation/evidence")) {
      return respond({ status: "success", data: { items: [evidenceSummary], total: 1, page: 1, page_size: 10, has_more: false } });
    }

    if (path.endsWith(`/api/v1/validation/evidence/${evidenceId}`)) {
      return respond({ status: "success", data: { summary: evidenceSummary, manifest: { ...evidenceSummary, evidence: [{ kind: "pytest", path: "backend/tests/test_validation_center_api.py", exists: true }] } } });
    }

    return respond({ detail: `unexpected mocked validation route: ${path}` }, 404);
  });

  await page.goto("/validation-center");
  await expect(page.getByRole("heading", { name: "自动化测试流水线中心" })).toBeVisible();
  await expect(page.getByText("只读 API")).toBeVisible();
  await expect(page.getByRole("button", { name: "controlled execution disabled" })).toBeDisabled();
  await expect(page.getByText("Validation Center backend contract")).toBeVisible();
  await expect(page.getByText("Validation API Run")).toBeVisible();
  await expect(page.getByLabel("validation run pagination status")).toContainText("共 3 条");

  await page.getByRole("button", { name: "查看详情" }).first().click();
  await expect(page.getByText("read validation history")).toBeVisible();
  await expect(page.getByText("mock_api_used")).toBeVisible();
  await expect(page.getByText("positive_business_success")).toBeVisible();
  await expect(page.getByText("Validation Center read-only API coverage")).toBeVisible();
  await expect(page.getByText("Validation Center read-only API evidence")).toBeVisible();

  await page.getByRole("button", { name: "查看快照" }).click();
  await expect(page.getByText("branch_percent")).toBeVisible();
  await expect(page.getByText("71.74", { exact: true })).toBeVisible();

  await page.getByRole("button", { name: "查看证据" }).click();
  await expect(page.getByText("evidence_count")).toBeVisible();
  await expect(page.getByText("missing_count", { exact: true })).toBeVisible();

  await page.getByLabel("搜索").fill("Markdown");
  await expect(page.getByText("Markdown Only", { exact: true })).toBeVisible();
  await expect(page.getByText("metadata_missing：缺少 JSON run metadata")).toBeVisible();
  await page.getByRole("button", { name: "查看详情" }).first().click();
  await expect(page.getByText("未记录 / 未证明")).toBeVisible();
  await expect(page.getByText("coverage_missing：未发现覆盖率快照").first()).toBeVisible();

  await expect(page.getByText("aistock_validation_run_v1")).toHaveCount(0);
  expect(writeMethods).toEqual([]);
  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
  expect(requestFailures).toEqual([]);
  expect(badResponses).toEqual([]);
});
