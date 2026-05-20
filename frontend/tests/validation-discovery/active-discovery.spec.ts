import { expect, test, type Page, type Route } from "@playwright/test";

const report = {
  report_id: "disc_run_demo",
  generated_at: "2026-05-20T01:00:00Z",
  run: { run_id: "run_demo", title: "Nightly active discovery", branch: "feature/demo", commit: "abc1234", status: "needs_review", started_at: "2026-05-20T01:00:00Z", finished_at: "2026-05-20T01:30:00Z" },
  summary_cards: [
    { card_id: "nightly_status", title: "夜间状态", value: "needs_review", hint: "status", tone: "red" },
    { card_id: "new_candidates", title: "新发现", value: 2, hint: "candidate", tone: "amber" },
    { card_id: "llm_exploration", title: "LLM 探索", value: 1, hint: "profiles", tone: "blue" },
    { card_id: "cleanup", title: "资源清理", value: 0, hint: "resources", tone: "green" },
  ],
  modules: [
    { module_id: "validation", display_name: "Validation Center", status: "warning", coverage: { status: "passed", line_percent: 82.5 }, candidate_count: 1, p0_p1_count: 1, issue_count: 1, finding_count: 0, test_plans: { required: ["validation_center_backend"] } },
    { module_id: "qe", display_name: "QuantEvolver", status: "critical", coverage: { status: "missing", line_percent: null }, candidate_count: 1, p0_p1_count: 1, issue_count: 1, finding_count: 0, test_plans: { required: ["qe_readonly_probe"] } },
    { module_id: "strategy_package", display_name: "Strategy Package", status: "healthy", coverage: { status: "passed", line_percent: 90 }, candidate_count: 0, p0_p1_count: 0, issue_count: 0, finding_count: 0 },
    { module_id: "selection", display_name: "Selection Center", status: "warning", coverage: { status: "missing" }, candidate_count: 1, p0_p1_count: 0, issue_count: 1, finding_count: 0 },
    { module_id: "paper_v2", display_name: "Paper v2", status: "healthy", coverage: { status: "passed", line_percent: 81 }, candidate_count: 0, p0_p1_count: 0, issue_count: 0, finding_count: 0 },
  ],
  execution_tree: [
    { node_id: "nightly_baseline", label: "Nightly Baseline", status: "passed", duration_ms: 1200, children: [{ task_id: "task_baseline", title: "Baseline scan", source: "nightly_baseline", module: "validation", risk_level: "L2", status: "passed", evidence_manifest_id: "evid_baseline" }] },
    { node_id: "change_driven", label: "Change Driven", status: "unknown", duration_ms: 0, children: [] },
    { node_id: "manual_mcp", label: "Manual MCP", status: "unknown", duration_ms: 0, children: [] },
  ],
  llm_summary: { profile_count: 1, draft_candidate_count: 1 },
  candidate_summary: { total: 2, by_severity: { P1: 1, P2: 1 }, by_review_status: { pending_review: 1, needs_evidence: 1 }, needs_review: 2 },
  issue_sync: { linked_count: 1, missing_link_count: 1 },
  cleanup: { namespace: "validation", validation_resource_count: 0, overdue_count: 0, failed_count: 0 },
  evidence_manifest_id: "evid_report",
};

const profiles = [
  { profile_id: "validation_design_consistency_checker_deepseek", agent_role: "design_consistency_checker", provider_id: "deepseek", provider_status: "configured", model_id: "deepseek-v4-pro", prompt_id: "validation_discovery_design_consistency_checker", prompt_version: "v1", prompt_management_url: "/quantevolver/prompts?agent_type=validation_discovery", model_config_url: "/config/rdagent-llm", enabled_for_nightly: true, enabled_for_manual_mcp: true, last_7_runs: { success_rate: 0.9, candidate_hit_rate: 0.4, false_positive_rate: 0.1, cost_estimate: 0.02 }, secret_visible: false },
];

const candidates = [
  { candidate_id: "ic_bug-001", source: "bug_registry", title: "QE fixed pool mismatch", module: "qe", severity: "P1", confidence: 0.91, review_status: "pending_review", evidence_status: "verified", deterministic_status: "verified", github_issue_url: "https://github.example/issues/1", github_issue_number: 1, evidence_types: ["bug_json", "reproduce_command"], evidence_manifest_id: "evid_bug-001", reproduce_command: "pytest backend/tests/test_qe.py" },
  { candidate_id: "ic_guardrail-001", source: "guardrail", title: "UI target missing coverage", module: "validation", severity: "P2", confidence: 0.72, review_status: "needs_evidence", evidence_status: "needs_evidence", deterministic_status: "detected", evidence_manifest_id: "evid_guardrail-001" },
];

const tasks = [
  { task_id: "task_baseline", title: "Baseline scan", source: "nightly_baseline", module: "validation", risk_level: "L2", status: "ready", detectors: ["contract_alignment_adapter"], evidence_manifest_id: "evid_baseline" },
  { task_id: "task_manual", title: "Manual MCP probe", source: "manual_mcp", module: "qe", risk_level: "L4", status: "scheduled", detectors: ["playwright_trace_probe_adapter"], cleanup_required: true, evidence_manifest_id: "evid_manual" },
];

const evidence = {
  manifest_id: "evid_bug-001",
  logs: [{ kind: "pytest", text: "failed then reproduced" }],
  api_responses: [{ path: "/api/v1/validation/discovery/candidates", status: 200 }],
  mcp_responses: [{ tool: "mcp_github_issue_sync_bug", status: "linked" }],
  screenshots: [{ path: "trace.png" }],
  artifacts: [{ path: "artifact.json" }],
  reproduce_command: "pytest backend/tests/test_qe.py",
};

async function mockDiscoveryApi(page: Page) {
  await page.route("**/api/v1/validation/discovery/**", async (route: Route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const method = route.request().method();
    const data = (() => {
      if (path.endsWith("/nightly-reports")) return { items: [{ report_id: report.report_id, run: report.run, candidate_summary: report.candidate_summary, llm_summary: report.llm_summary, cleanup: report.cleanup }], total: 1, page: 1, page_size: 7, has_more: false };
      if (path.endsWith("/nightly-reports/current") || path.endsWith(`/nightly-reports/${report.report_id}`)) return report;
      if (path.endsWith("/nightly-reports/current/llm") || path.endsWith(`/nightly-reports/${report.report_id}/llm`)) return { report_id: report.report_id, profiles, draft_candidates: [candidates[1]], eval_summary: { case_count: 2, status: "ready_for_dry_run" } };
      if (path.endsWith("/llm-profiles")) return { items: profiles, total: profiles.length, page: 1, page_size: 20, has_more: false, prompt_management_url: "/quantevolver/prompts", model_config_url: "/config/rdagent-llm" };
      if (path.endsWith("/tasks")) return { items: tasks, total: tasks.length, page: 1, page_size: 20, has_more: false };
      if (path.endsWith("/candidates")) return { items: candidates, total: candidates.length, page: 1, page_size: 20, has_more: false };
      if (path.endsWith("/candidates/ic_bug-001")) return { ...candidates[0], reviews: [], evidence_manifest: evidence };
      if (path.includes("/traces/")) return evidence;
      if (path.endsWith("/tool-adapters")) return { items: [{ adapter_id: "playwright_trace_probe_adapter", title: "Playwright trace probe", kind: "ui_trace", status: "configured", dry_run_supported: true, writes_production: false }], total: 1, page: 1, page_size: 20, has_more: false };
      if (path.endsWith("/llm-evals")) return { case_count: 2, status: "ready_for_dry_run" };
      if (method === "POST" && path.includes("/dry-run")) return { adapter_id: "playwright_trace_probe_adapter", dry_run: true, result: { trace_required: true } };
      if (method === "POST" && path.includes("/llm-evals/run")) return { case_count: 2, dry_run: true, recall_rate: 0.8 };
      if (method === "POST") return { status: "ok", task_id: "task_new", result: { summary: "dry-run completed" } };
      return {};
    })();
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "success", data }) });
  });
}

test.beforeEach(async ({ page }) => {
  await mockDiscoveryApi(page);
});

test("nightly report page shows module details, llm draft, candidate groups and evidence drawer", async ({ page }) => {
  await page.goto("/validation/nightly-reports");
  await expect(page.getByRole("heading", { name: "流水线主动发现中心" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "模块级测试与 Issue 详情" })).toBeVisible();
  await expect(page.getByRole("button", { name: /QuantEvolver/ })).toBeVisible();
  await expect(page.getByText("LLM 输出只作为 draft")).toBeVisible();
  await expect(page.getByRole("button", { name: /待审核/ })).toBeVisible();
  await page.getByRole("button", { name: "打开当前报告证据" }).click();
  await expect(page.getByLabel("证据详情抽屉")).toBeVisible();
  await expect(page.getByText("pytest backend/tests/test_qe.py")).toBeVisible();
});

test("candidate page supports filters, table detail and github sync status", async ({ page }) => {
  await page.goto("/validation/discovery-candidates");
  await expect(page.getByRole("heading", { name: "候选 Issue 审核表" })).toBeVisible();
  await page.getByPlaceholder("搜索标题或 candidate_id").fill("fixed pool");
  await expect(page.getByText("QE fixed pool mismatch")).toBeVisible();
  await expect(page.getByRole("link", { name: "#1" })).toBeVisible();
  await page.getByRole("button", { name: /QE fixed pool mismatch/ }).click();
  await expect(page.getByLabel("证据详情抽屉")).toBeVisible();
  await expect(page.getByText("mcp_github_issue_sync_bug")).toBeVisible();
});

test("tasks, business probes and llm profile pages expose operational controls", async ({ page }) => {
  await page.goto("/validation/discovery-tasks");
  await expect(page.getByRole("heading", { name: "创建专项探测任务" })).toBeVisible();
  await expect(page.getByRole("cell", { name: "nightly_baseline" })).toBeVisible();

  await page.goto("/validation/business-probes");
  await expect(page.getByRole("heading", { name: /QE -> Archive/ })).toBeVisible();
  await expect(page.getByText("React Flow 节点颜色来自真实 report/module/task 数据")).toBeVisible();

  await page.goto("/validation/discovery-llm-profiles");
  await expect(page.getByRole("heading", { name: "Profile 表格" })).toBeVisible();
  await expect(page.getByRole("cell", { name: "deepseek / deepseek-v4-pro" })).toBeVisible();
  await page.getByRole("button", { name: "运行 Eval dry-run" }).first().click();
  await expect(page.getByText("Eval dry-run 完成")).toBeVisible();
});
