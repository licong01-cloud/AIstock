import { expect, test } from "@playwright/test";

const passedRunId = "validation_center_20260504_l2_api__abc123";
const markdownOnlyRunId = "validation_center_20260504_l1_markdown_only__def456";
const coverageId = "validation_center_coverage_demo__cov123";
const evidenceId = "validation_center_evidence_demo__evd123";
const findingId = "guardrail_guardrail_fp_001";
const bugId = "bug_demo_001";
const executionJobId = "valjob_20260504_210000_mocked";

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

const findingItem = {
  finding_id: findingId,
  source_type: "guardrail",
  source_schema: "aistock_guardrail_scan_result_v1",
  module: "backend",
  severity: "P1",
  status: "detected",
  title: "No silent fallback",
  description: "Exception handler may hide a business failure.",
  rule_id: "NO-SILENT-FALLBACK",
  category: "reliability",
  file_path: "backend/services/demo.py",
  line: 42,
  fingerprint: "guardrail_fp_001",
  evidence_uri: "tmp/validation/guardrails/changed_scan.json",
  allowed_write_scope: ["backend/services/demo.py"],
  required_verification: [
    "python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1",
    "python -m nox -s l0 -- <changed files>",
  ],
};

const findingAgentContext = {
  schema_version: "aistock_validation_agent_context_v1",
  context_type: "quality_finding",
  finding_id: findingId,
  problem_statement: "Exception handler may hide a business failure.",
  finding_source: "guardrail",
  severity: "P1",
  status: "detected",
  reproduce_command: "python scripts/aistock_guardrail_scan.py backend/services/demo.py --fail-on-severity NONE",
  evidence_uris: ["tmp/validation/guardrails/changed_scan.json"],
  allowed_write_scope: ["backend/services/demo.py"],
  suspected_modules: ["backend", "backend/services/demo.py"],
  required_verification: ["python -m nox -s l0 -- backend/services/demo.py"],
  closure_requirements: ["Record a verification run before closing."],
};

const bugItem = {
  bug_id: bugId,
  title: "Demo validation failure",
  description: "A mocked validation failure for registry UI tests.",
  module: "validation_center",
  severity: "P2",
  risk_area: "validation",
  status: "detected",
  trigger_condition: { plan_key: "validation_center_backend" },
  reproduce_command: "python -m nox -s validation_center_backend",
  failing_run_id: "run_failed_demo",
  evidence_uris: ["tests/aistock_validation/history/validation_center/demo.md"],
  fingerprint: "bug_fp_001",
  github_issue_url: "https://github.com/example/aistock/issues/1",
  assigned_agent: "codex",
  allowed_write_scope: ["backend/services/validation"],
  suspected_modules: ["backend/services/validation"],
  required_verification: ["python -m nox -s validation_center_backend"],
  closure_requirements: ["verification_run_id required"],
};

const bugAgentContext = {
  schema_version: "aistock_validation_agent_context_v1",
  context_type: "bug",
  bug_id: bugId,
  problem_statement: "A mocked validation failure for registry UI tests.",
  finding_source: "validation_failure",
  severity: "P2",
  status: "detected",
  reproduce_command: "python -m nox -s validation_center_backend",
  evidence_uris: ["tests/aistock_validation/history/validation_center/demo.md"],
  allowed_write_scope: ["backend/services/validation"],
  suspected_modules: ["backend/services/validation"],
  required_verification: ["python -m nox -s validation_center_backend"],
  closure_requirements: ["verification_run_id required"],
  github_issue_url: "https://github.com/example/aistock/issues/1",
  verification_run_id: null,
};

const executionJob = {
  schema_version: "aistock_validation_execution_job_v1",
  job_id: executionJobId,
  status: "passed",
  plan_key: "validation_center_backend",
  title: "Validation Center backend contract",
  module: "validation_center",
  level: "L2",
  command_key: "nox_validation_center_backend",
  nox_session: "validation_center_backend",
  command: ["python", "-m", "nox", "-s", "validation_center_backend"],
  requested_by: "ui",
  requested_at: "2026-05-04T21:00:00+08:00",
  started_at: "2026-05-04T21:00:01+08:00",
  finished_at: "2026-05-04T21:00:08+08:00",
  timeout_seconds: 300,
  return_code: 0,
  backend_port: null,
  frontend_port: null,
  writes_database: false,
  writes_artifacts: true,
  writes_business_state: false,
  production_8001_touched: false,
  arbitrary_shell_allowed: false,
  log_path: "tmp/validation/runner/jobs/valjob_20260504_210000_mocked.log",
  evidence_path: "tmp/validation/runner/jobs/valjob_20260504_210000_mocked_evidence.json",
  archive: {
    status: "archived",
    run_id: "validation_center_runner_archived__run123",
    run_record_path: "tests/aistock_validation/history/validation_center/20260504_l2_validation-center-backend-runner-validation.md",
    metadata_path: "tests/aistock_validation/history/validation_center/20260504_l2_validation-center-backend-runner-validation.json",
    evidence_manifest_path: "tests/aistock_validation/history/validation_center/20260504_l2_validation-center-backend-runner-evidence.json",
    runner_log_archive_path: "tests/aistock_validation/history/validation_center/20260504_l2_validation-center-backend-runner-log.txt",
    coverage_snapshot_path: "tests/aistock_validation/history/validation_center/20260504_l2_validation-center-backend-runner-snapshot.json",
  },
  error: null,
};

const workspaceStatus = {
  schema_version: "aistock_git_workspace_status_v1",
  generated_at: "2026-05-05T22:00:00+08:00",
  repo_root: "F:/Dev/AIstock",
  branch: "main",
  upstream: "origin/main",
  head_commit: "abcdef123456",
  short_head_commit: "abcdef1",
  ahead_count: 2,
  behind_count: 0,
  dirty: true,
  summary: {
    changed_files: 4,
    staged_files: 1,
    unstaged_files: 2,
    untracked_files: 1,
    conflicted_files: 0,
    deleted_files: 0,
    renamed_files: 0,
    unmapped_files: 1,
    ambiguous_files: 0,
    critical_risk_files: 0,
  },
  by_status: { staged_modified: 1, unstaged_modified: 2, untracked: 1 },
  by_module: [
    { module_id: "validation.center", changed_file_count: 2, max_risk_level: "medium", statuses: { staged_modified: 1 } },
  ],
  files: [
    {
      path: "frontend/src/app/validation-center/page.tsx",
      status: "staged_modified",
      git_xy: "M.",
      staged: true,
      unstaged: false,
      untracked: false,
      conflicted: false,
      primary_module: "validation.center",
      impact_modules: ["validation.module_quality"],
      layer: "frontend_ui",
      risk_level: "medium",
      ownership_status: "mapped",
      matched_rule_ids: ["validation_frontend"],
      reason_codes: [],
      recommended_action: "run_changed_files_guard_and_commit",
    },
    {
      path: "root_tmp.py",
      status: "untracked",
      git_xy: "??",
      staged: false,
      unstaged: false,
      untracked: true,
      conflicted: false,
      primary_module: null,
      impact_modules: [],
      layer: null,
      risk_level: null,
      ownership_status: "unmapped",
      matched_rule_ids: [],
      reason_codes: ["no_matching_file_ownership_rule", "git_untracked"],
      recommended_action: "add_file_ownership_mapping_before_commit",
    },
  ],
  reason_codes: ["workspace_dirty", "untracked_files_present", "unmapped_files_present"],
  git_command_mode: "read_only_allowlist",
  arbitrary_shell_allowed: false,
  production_8001_touched: false,
};

const branchStatus = {
  schema_version: "aistock_git_branch_status_v1",
  generated_at: "2026-05-05T22:00:00+08:00",
  repo_root: "F:/Dev/AIstock",
  branch: "main",
  detached: false,
  upstream: "origin/main",
  head_commit: "abcdef123456",
  short_head_commit: "abcdef1",
  ahead_count: 2,
  behind_count: 0,
  upstream_known: true,
  git_command_mode: "read_only_allowlist",
  arbitrary_shell_allowed: false,
  production_8001_touched: false,
};

const commitActivity = {
  schema_version: "aistock_git_commit_activity_v1",
  generated_at: "2026-05-05T22:10:00+08:00",
  repo_root: "F:/Dev/AIstock",
  branch: "main",
  upstream: "origin/main",
  head_commit: "abcdef123456",
  short_head_commit: "abcdef1",
  limit: 50,
  summary: {
    commit_count: 3,
    changed_file_count: 9,
    unmapped_commit_count: 1,
    ambiguous_commit_count: 0,
    latest_commit: {
      commit_hash: "abcdef123456",
      short_hash: "abcdef1",
      authored_at: "2026-05-05T22:00:00+08:00",
      subject: "feat(validation): show git workspace status",
    },
  },
  by_day: [
    { period: "2026-05-05", commit_count: 2 },
    { period: "2026-05-04", commit_count: 1 },
  ],
  by_week: [{ period: "2026-W19", commit_count: 3 }],
  by_month: [{ period: "2026-05", commit_count: 3 }],
  by_module: [
    {
      module_id: "validation.center",
      display_name: "Validation Center",
      commit_count: 2,
      changed_file_count: 6,
      latest_commit: {
        commit_hash: "abcdef123456",
        short_hash: "abcdef1",
        authored_at: "2026-05-05T22:00:00+08:00",
        subject: "feat(validation): show git workspace status",
      },
      max_risk_level: "medium",
      file_status_counts: { M: 6 },
      required_test_plans: ["l0", "validation_center_backend"],
      recommended_test_plans: ["validation_center_ui"],
    },
  ],
  commits: [
    {
      commit_hash: "abcdef123456",
      short_hash: "abcdef1",
      author_name: "codex",
      author_email: "codex@example.invalid",
      authored_at: "2026-05-05T22:00:00+08:00",
      subject: "feat(validation): show git workspace status",
      changed_file_count: 3,
      file_status_counts: { M: 3 },
      module_ids: ["validation.center"],
      ownership_summary: { mapped: 3, unmapped: 0, ambiguous: 0 },
      max_risk_level: "medium",
      files: [],
    },
    {
      commit_hash: "0123456789ab",
      short_hash: "0123456",
      author_name: "codex",
      authored_at: "2026-05-05T21:30:00+08:00",
      subject: "feat(validation): expose git workspace status",
      changed_file_count: 4,
      file_status_counts: { A: 2, M: 2 },
      module_ids: ["validation.module_quality"],
      ownership_summary: { mapped: 3, unmapped: 1, ambiguous: 0 },
      max_risk_level: "high",
      files: [],
    },
  ],
  git_command_mode: "read_only_allowlist",
  arbitrary_shell_allowed: false,
  production_8001_touched: false,
};

const moduleQuality = {
  schema_version: "aistock_validation_module_quality_v1",
  generated_at: "2026-05-05T22:11:00+08:00",
  repo_root: "F:/Dev/AIstock",
  summary: {
    module_count: 4,
    modules_with_workspace_changes: 1,
    modules_with_recent_commits: 2,
    modules_needing_validation: 2,
    unmapped_workspace_files: 1,
    ambiguous_workspace_files: 0,
    recent_commit_count: 3,
  },
  modules: [
    {
      module_id: "validation.center",
      display_name: "Validation Center",
      parent_module: "validation",
      module_type: "cross_cutting",
      registry_risk_level: "high",
      description_zh: "覆盖 Validation Center 的页面、API、历史记录、质量发现、Bug 展示和汇总面板。",
      ui_routes: ["/validation-center"],
      test_plans: {
        required_on_change: ["l0", "validation_center_backend"],
        recommended: ["validation_center_ui"],
      },
      workspace: {
        changed_file_count: 2,
        staged_file_count: 1,
        unstaged_file_count: 1,
        untracked_file_count: 0,
        max_risk_level: "medium",
        files: [],
      },
      commits: {
        commit_count: 2,
        changed_file_count: 6,
        latest_commit: {
          short_hash: "abcdef1",
          subject: "feat(validation): show git workspace status",
        },
        max_risk_level: "medium",
      },
      coverage: {
        snapshot_id: "validation_center_snapshot",
        status: "passed",
        line_percent: 81.35,
        branch_percent: 64.35,
        generated_at: "2026-05-05T22:05:00+08:00",
      },
      quality: {
        finding_count: 2,
        bug_count: 0,
        by_severity: { P2: 2 },
        by_status: { detected: 2 },
      },
      priority: {
        score: 36,
        level: "high",
        reason_codes: ["workspace_changed", "recent_commits", "quality_findings"],
      },
    },
    {
      module_id: "validation.module_quality",
      display_name: "Module quality cockpit",
      parent_module: "validation",
      module_type: "cross_cutting",
      registry_risk_level: "high",
      description_zh: "覆盖模块注册表、文件归属、commit 影响分析、工作区 dirty 状态和模块质量优先级矩阵。",
      ui_routes: ["/validation-center"],
      test_plans: {
        required_on_change: ["l0", "validation_module_registry_l0"],
        recommended: ["validation_center_ui"],
      },
      workspace: {
        changed_file_count: 0,
        staged_file_count: 0,
        unstaged_file_count: 0,
        untracked_file_count: 0,
        max_risk_level: null,
        files: [],
      },
      commits: {
        commit_count: 1,
        changed_file_count: 3,
        latest_commit: { short_hash: "0123456", subject: "feat(validation): expose git workspace status" },
        max_risk_level: "high",
      },
      coverage: { status: "missing", line_percent: null, branch_percent: null },
      quality: { finding_count: 0, bug_count: 0, by_severity: {}, by_status: {} },
      priority: { score: 18, level: "medium", reason_codes: ["recent_commits"] },
    },
  ],
  workspace_summary: workspaceStatus.summary,
  commit_summary: commitActivity.summary,
  global_reason_codes: ["unmapped_workspace_files_present", "modules_need_validation"],
  git_command_mode: "read_only_allowlist",
  arbitrary_shell_allowed: false,
  production_8001_touched: false,
};
const uiTargetItem = {
  route_id: "validation.center",
  href: "/validation-center",
  label: "Validation Center",
  nav_group: "Validation Pipeline",
  primary_module: "validation.center",
  impact_modules: ["validation.runner", "validation.coverage", "validation.module_quality"],
  risk_level: "medium",
  required_test_plans: ["l0", "validation_center_backend"],
  recommended_test_plans: ["validation_center_ui", "validation_center_real_port_ui"],
  business_operations: ["Open Validation Center", "Review quality state", "Run controlled validation"],
  coverage_status: "partial",
  module_quality: moduleQuality.modules[0],
  latest_run: runItems[0],
  warnings: ["route_coverage_not_fully_proven"],
  proven_by_real_business_evidence: false,
};

const uiTargetPage = {
  schema_version: "aistock_validation_ui_targets_v1",
  catalog_path: "tests/aistock_validation/catalog/ui_targets.yaml",
  missing: false,
  items: [uiTargetItem],
  total: 1,
  page: 1,
  page_size: 100,
  has_more: false,
};

const uiTargetSummary = {
  schema_version: "aistock_validation_ui_targets_v1",
  generated_at: "2026-05-07T09:00:00+08:00",
  catalog_path: "tests/aistock_validation/catalog/ui_targets.yaml",
  missing: false,
  target_count: 1,
  nav_group_count: 1,
  warning_count: 1,
  targets_requiring_action: 1,
  by_nav_group: [{ nav_group: "Validation Pipeline", target_count: 1, warning_count: 1 }],
  by_coverage_status: { partial: 1 },
  by_risk_level: { medium: 1 },
  production_8001_touched: false,
};


test("Validation Center UI uses mocked APIs and controlled runner POST", async ({ page }) => {
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

  await page.route("**/api/ingestion/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const data = path.endsWith("/unack-count") ? { count: 0 } : { alerts: [], items: [], total: 0 };
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(data),
    });
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

    const isExecutionStart = path.endsWith("/api/v1/validation/executions") && method === "POST";
    if (method !== "GET" && !isExecutionStart) {
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
            run_count: 2,
            coverage_snapshot_count: 1,
            evidence_manifest_count: 1,
          },
          plan_catalog: { catalog_path: "tests/aistock_validation/catalog/test_plans.yaml", missing: false, plan_count: 2 },
          quality: { mode: "read_only", finding_count: 1, bug_count: 1, parse_errors: [] },
          runner: {
            mode: "controlled_execution",
            execution_root: "tmp/validation/runner/jobs",
            exists: true,
            job_count: 1,
            jobs_by_status: { passed: 1 },
            allowed_command_type: "nox_session_allowlist_only",
            arbitrary_shell_allowed: false,
            production_8001_touched: false,
          },
          production_8001_touched: false,
        },
      });
    }

    if (path.endsWith("/api/v1/validation/summary")) {
      return respond({
        status: "success",
        data: {
          history_root: "tests/aistock_validation/history",
          run_count: 2,
          coverage_snapshot_count: 1,
          evidence_manifest_count: 1,
          plan_count: 2,
          quality: { finding_count: 1, bug_count: 1 },
          runner: {
            mode: "controlled_execution",
            execution_root: "tmp/validation/runner/jobs",
            exists: true,
            job_count: 1,
            jobs_by_status: { passed: 1 },
            arbitrary_shell_allowed: false,
            production_8001_touched: false,
          },
          runs_by_status: { passed: 1, unknown: 1 },
          modules: [{ module: "validation_center", run_count: 2, latest_run: runItems[0] }],
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
              runner_enabled: true,
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
              runner_enabled: false,
            },
          ],
        },
      });
    }

    if (path.endsWith("/api/v1/validation/git/workspace-status")) {
      return respond({ status: "success", data: workspaceStatus });
    }

    if (path.endsWith("/api/v1/validation/git/branch-status")) {
      return respond({ status: "success", data: branchStatus });
    }

    if (path.endsWith("/api/v1/validation/git/commit-activity")) {
      expect(Number(url.searchParams.get("limit") || "0")).toBeGreaterThan(0);
      return respond({ status: "success", data: commitActivity });
    }

    if (path.endsWith("/api/v1/validation/modules/quality-summary")) {
      expect(Number(url.searchParams.get("commit_limit") || "0")).toBeGreaterThan(0);
      return respond({ status: "success", data: moduleQuality });
    }

    if (path.endsWith("/api/v1/validation/ui-targets/summary")) {
      return respond({ status: "success", data: uiTargetSummary });
    }

    if (path.endsWith("/api/v1/validation/ui-targets")) {
      expect(Number(url.searchParams.get("page_size") || "0")).toBeGreaterThanOrEqual(20);
      return respond({ status: "success", data: uiTargetPage });
    }

    if (path.endsWith("/api/v1/validation/ui-targets/validation.center")) {
      return respond({
        status: "success",
        data: {
          schema_version: "aistock_validation_ui_targets_v1",
          catalog_path: "tests/aistock_validation/catalog/ui_targets.yaml",
          missing: false,
          target: uiTargetItem,
        },
      });
    }

    if (path.endsWith("/api/v1/validation/executions") && method === "GET") {
      return respond({ status: "success", data: { items: [executionJob], total: 1, page: 1, page_size: 10, has_more: false } });
    }

    if (path.endsWith("/api/v1/validation/executions") && method === "POST") {
      writeMethods.push(`${method} ${path}`);
      const payload = JSON.parse(request.postData() || "{}") as { plan_key?: string; backend_port?: number };
      expect(payload.plan_key).toBe("validation_center_backend");
      expect(payload.backend_port).toBeUndefined();
      return respond({ status: "success", data: executionJob });
    }

    if (path.endsWith(`/api/v1/validation/executions/${executionJobId}/log`)) {
      return respond({ status: "success", data: { job_id: executionJobId, exists: true, content: "api runner ok\narchive complete\n", tail_lines: 120, truncated: false } });
    }

    if (path.endsWith(`/api/v1/validation/executions/${executionJobId}/evidence`)) {
      return respond({
        status: "success",
        data: {
          job_id: executionJobId,
          job: executionJob,
          runner_evidence_path: executionJob.evidence_path,
          standard_evidence_path: executionJob.archive.evidence_manifest_path,
          runner_evidence: { schema_version: "aistock_validation_runner_evidence_v1" },
          standard_evidence: { schema_version: "aistock_validation_evidence_manifest_v1", missing_count: 0 },
        },
      });
    }

    if (path.endsWith(`/api/v1/validation/executions/${executionJobId}`)) {
      return respond({ status: "success", data: executionJob });
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
        data: { items: items.slice(start, start + pageSize), total: items.length, page: pageNumber, page_size: pageSize, has_more: start + pageSize < items.length },
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

    if (path.endsWith("/api/v1/validation/findings/summary")) {
      return respond({
        status: "success",
        data: {
          finding_count: 1,
          by_source_type: { guardrail: 1 },
          by_severity: { P1: 1 },
          by_status: { detected: 1 },
          by_module: { backend: 1 },
          latest_findings: [findingItem],
          parse_errors: [],
        },
      });
    }

    if (path.endsWith("/api/v1/validation/findings")) {
      return respond({ status: "success", data: { items: [findingItem], total: 1, page: 1, page_size: 20, has_more: false } });
    }

    if (path.endsWith(`/api/v1/validation/findings/${findingId}`)) {
      return respond({ status: "success", data: { ...findingItem, agent_context: findingAgentContext } });
    }

    if (path.endsWith("/api/v1/validation/bugs/summary")) {
      return respond({
        status: "success",
        data: {
          bug_count: 1,
          by_severity: { P2: 1 },
          by_status: { detected: 1 },
          by_module: { validation_center: 1 },
          latest_bugs: [bugItem],
          parse_errors: [],
        },
      });
    }

    if (path.endsWith("/api/v1/validation/bugs")) {
      return respond({ status: "success", data: { items: [bugItem], total: 1, page: 1, page_size: 20, has_more: false } });
    }

    if (path.endsWith(`/api/v1/validation/bugs/${bugId}/agent-context`)) {
      return respond({ status: "success", data: bugAgentContext });
    }

    if (path.endsWith(`/api/v1/validation/bugs/${bugId}`)) {
      return respond({ status: "success", data: { ...bugItem, agent_context: bugAgentContext } });
    }

    return respond({ detail: `unexpected mocked validation route: ${path}` }, 404);
  });

  await page.goto("/validation-center");
  const pipelineGroup = page.locator(".sidebar-group-title", { hasText: "自动化流水线" });
  await expect(pipelineGroup).toBeVisible();
  await pipelineGroup.click();
  await expect(page.getByRole("link", { name: /流水线中心/ })).toHaveAttribute("href", "/validation-center");
  await expect(page.getByRole("heading", { name: "自动化测试流水线中心" })).toBeVisible();
  await expect(page.getByText("只读 API")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Git 工作区状态" })).toBeVisible();
  await expect(page.getByText("未提交文件", { exact: true })).toBeVisible();
  await expect(page.getByText("未归属文件", { exact: true })).toBeVisible();
  await expect(page.getByText("本地未推送", { exact: true })).toBeVisible();
  await expect(page.getByText("root_tmp.py")).toBeVisible();
  await expect(page.getByText("validation.center").first()).toBeVisible();
  await expect(page.getByText("read_only_allowlist").first()).toBeVisible();
  await expect(page.getByText("add_file_ownership_mapping_before_commit")).toBeVisible();
  await expect(page.getByRole("heading", { name: "模块质量优先级" })).toBeVisible();
  await expect(page.getByText("近期 Commit", { exact: true }).first()).toBeVisible();
  await expect(page.getByText("需要验证模块", { exact: true })).toBeVisible();
  await expect(page.getByText("按文件归属自动聚合")).toBeVisible();
  await expect(page.getByText("feat(validation): show git workspace status").first()).toBeVisible();
  await expect(page.getByText("unmapped_workspace_files_present")).toBeVisible();
  await expect(page.getByText("validation_module_registry_l0")).toBeVisible();
  await expect(page.getByRole("heading", { name: "UI Target Route Coverage" })).toBeVisible();
  await expect(page.getByText("Catalog: tests/aistock_validation/catalog/ui_targets.yaml")).toBeVisible();
  await expect(page.getByText("Route coverage boundary")).toBeVisible();
  const validationRouteRow = page.locator("tr", { hasText: "Validation Center" }).filter({ hasText: "/validation-center" }).first();
  await expect(validationRouteRow.getByText("validation.center").first()).toBeVisible();
  await expect(validationRouteRow.getByText("route_coverage_not_fully_proven")).toBeVisible();
  await validationRouteRow.getByRole("button", { name: "View UI target coverage" }).click();
  await expect(page.getByRole("heading", { name: "UI Target Detail" })).toBeVisible();
  const routeDetailPanel = page.locator("section").filter({ has: page.getByRole("heading", { name: "UI Target Detail" }) }).last();
  await expect(routeDetailPanel.getByText("Open Validation Center")).toBeVisible();
  await expect(routeDetailPanel.getByText("Line 81.35% / Branch 64.35%")).toBeVisible();
  await expect(routeDetailPanel.getByText(passedRunId)).toBeVisible();
  await expect(page.getByText("受控 Runner：allowlist only")).toBeVisible();
  await expect(page.getByText("Validation Center backend contract")).toBeVisible();
  await expect(page.getByText("Runner 执行队列")).toBeVisible();
  await expect(page.getByText("tmp/validation/runner/jobs/valjob_20260504_210000_mocked.log")).toBeVisible();
  await expect(page.getByText("tests/aistock_validation/history/validation_center/20260504_l2_validation-center-backend-runner-validation.md")).toBeVisible();
  await page.getByRole("button", { name: "run validation plan validation_center_backend" }).click();
  await expect(page.getByText("Runner 已提交")).toBeVisible();
  await expect(page.getByText("状态=passed")).toBeVisible();
  await page.getByRole("button", { name: "Open Runner detail" }).click();
  await expect(page.getByRole("heading", { name: "Runner Detail" })).toBeVisible();
  await expect(page.getByText("api runner ok")).toBeVisible();
  await expect(page.getByText("aistock_validation_evidence_manifest_v1")).toBeVisible();
  await expect(page.getByText("Validation API Run").first()).toBeVisible();
  await expect(page.getByText("质量发现与 Bug Registry")).toBeVisible();
  await expect(page.getByText("No silent fallback")).toBeVisible();
  await expect(page.getByText("Demo validation failure")).toBeVisible();
  await expect(page.getByLabel("validation run pagination status")).toContainText("共 2 条");

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

  await page.getByRole("button", { name: "查看发现" }).click();
  await expect(page.getByText("quality_finding", { exact: true })).toBeVisible();
  await expect(page.getByText("python scripts/aistock_guardrail_scan.py backend/services/demo.py --fail-on-severity NONE")).toBeVisible();

  await page.getByRole("button", { name: "查看 Bug" }).click();
  await expect(page.getByText("bug", { exact: true })).toBeVisible();
  await expect(page.getByText("python -m nox -s validation_center_backend").first()).toBeVisible();
  await expect(page.getByText("verification_run_id required").first()).toBeVisible();

  await page.locator("#validation-search").fill("Markdown");
  await expect(page.getByText("Markdown Only", { exact: true })).toBeVisible();
  await expect(page.getByText("metadata_missing：缺少 JSON run metadata")).toBeVisible();
  await page.getByRole("button", { name: "查看详情" }).first().click();
  await expect(page.getByText("未记录 / 未证明")).toBeVisible();
  await expect(page.getByText("coverage_missing：未发现覆盖率快照").first()).toBeVisible();

  await expect(page.getByText("aistock_validation_run_v1")).toHaveCount(0);
  expect(writeMethods).toEqual(["POST /api/v1/validation/executions"]);
  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
  expect(requestFailures).toEqual([]);
  expect(badResponses).toEqual([]);
});
