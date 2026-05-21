import { expect, test } from "@playwright/test";

const taskId = "rat_demo_1";
const memoryId = "mem_demo_1";
const approvalId = "appr_demo_1";
const candidateId = "issuecand_demo_1";

const tasks = [{
  task_id: taskId,
  title: "QE 10 loop 实验规划",
  status: "running",
  task_type: "research_stream",
  risk_level: "medium",
  created_at: "2026-05-21T09:00:00+08:00",
  updated_at: "2026-05-21T09:05:00+08:00",
}];

const taskEvents = [{
  event_id: "ratev_demo_1",
  task_id: taskId,
  event_type: "planned",
  severity: "info",
  message: "已生成计划草稿",
  payload_json: { source: "playwright" },
  evidence_refs: ["docs/architecture/aistock_research_agent_console_design_20260520.md"],
  created_at: "2026-05-21T09:00:01+08:00",
}];

const memories = [{
  memory_id: memoryId,
  memory_type: "core",
  namespace: "aistock",
  subject_key: "assistant.memory",
  title: "长期记忆原则",
  content_text: "Memory Ledger 是事实源。",
  approval_status: "approved",
  risk_level: "medium",
  source_ref: "docs/architecture/aistock_research_agent_console_design_20260520.md",
  evidence_refs: ["design"],
  updated_at: "2026-05-21T09:01:00+08:00",
}];

const mcpServers = [{
  server_id: "mcp_server_research_assistant",
  server_key: "research-assistant",
  title: "研究助理 MCP",
  status: "ready",
  health_json: { mode: "loopback" },
}];

const mcpTools = [
  {
    tool_id: "mcp_tool_research_assistant_issue",
    server_key: "research-assistant",
    tool_name: "assistant_create_issue_candidate",
    title: "创建候选 Issue",
    risk_level: "high",
    requires_approval: true,
    status: "enabled",
    input_schema_json: { type: "object" },
    preflight_schema_json: { checks: ["dedupe_key", "github_gate"] },
    required_confirmations: ["APPROVE_RESEARCH_ASSISTANT_ACTION"],
  },
  {
    tool_id: "mcp_tool_research_assistant_context",
    server_key: "research-assistant",
    tool_name: "assistant_build_context_pack",
    title: "构建 Context Pack",
    risk_level: "low",
    requires_approval: false,
    status: "enabled",
    input_schema_json: { type: "object" },
    preflight_schema_json: { checks: ["token_budget"] },
    required_confirmations: [],
  },
];

const skills = [{
  skill_id: "skill_qe-evolution-diagnostics",
  skill_key: "qe-evolution-diagnostics",
  title: "QE 实验诊断",
  description: "分析 QE evolution 实验。",
  domain: "qe",
  risk_level: "medium",
  permission_scope: "read_analysis",
  checksum: "abc123",
  status: "enabled",
}];

const approvals = [{
  approval_id: approvalId,
  task_id: taskId,
  approval_type: "mcp.high_risk",
  risk_level: "high",
  plan_digest: "digest-abcdef",
  summary: "高风险 MCP 调用",
  required_confirmation_text: "APPROVE_RESEARCH_ASSISTANT_ACTION",
  status: "pending",
  created_at: "2026-05-21T09:02:00+08:00",
}];

const issueCandidates = [{
  candidate_id: candidateId,
  title: "策略包选股前置检查阻断",
  severity: "P1",
  module: "selection_center",
  status: "needs_review",
  problem_statement: "平台能力错误绑定在策略包健康检查上。",
  github_sync_status: "not_requested",
  evidence_refs: ["log"],
}];

const modelProfiles = [{
  model_profile_id: "model_deepseek_v4_pro_primary",
  provider: "deepseek",
  model_name: "deepseek-v4-pro",
  role: "primary_reasoner",
  status: "enabled",
  capabilities_json: { long_context: true },
  cost_json: { tier: "medium" },
  limits_json: { writes_long_term_memory: true },
}];

const routingPolicies = [{
  policy_id: "route_primary_high_risk",
  role: "primary_reasoner",
  risk_level: "high",
  model_profile_id: "model_deepseek_v4_pro_primary",
  status: "enabled",
}];

const externalSessions = [{
  session_id: "extsess_demo_1",
  agent_type: "claude_code",
  agent_name: "Claude Code",
  status: "connected",
  auth_scope: { mcp_only: true },
  metadata_json: { workspace: "research-agent-console" },
  created_at: "2026-05-21T09:03:00+08:00",
}];

const externalEvents = [{
  external_event_id: "extev_demo_1",
  session_id: "extsess_demo_1",
  event_type: "handoff_received",
  risk_level: "low",
  payload_json: { channel: "mempalace" },
  evidence_refs: ["cross-tool"],
  created_at: "2026-05-21T09:03:30+08:00",
}];

const traceEvents = [{
  trace_id: "trace_demo_1",
  task_id: taskId,
  event_type: "mcp_preflight",
  component: "research_assistant.mcp",
  status: "approval_required",
  payload_json: { tool_name: "assistant_create_issue_candidate" },
  cost_json: { tokens: 0 },
  duration_ms: 42,
  created_at: "2026-05-21T09:04:00+08:00",
}];

function page<T>(items: T[]) {
  return { items, total: items.length, page: 1, page_size: 100, has_more: false };
}

test("Research Assistant phase1 console uses mocked API contracts", async ({ page: browserPage }) => {
  const writeMethods: string[] = [];
  const consoleErrors: string[] = [];
  const pageErrors: string[] = [];

  browserPage.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  browserPage.on("pageerror", (error) => pageErrors.push(error.message));

  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const method = route.request().method();
    const respond = (data: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify({ status: status >= 400 ? "error" : "success", data }) });
    if (method !== "GET") writeMethods.push(`${method} ${path}`);

    if (path.endsWith("/health")) return respond({ service: "research-assistant", status: "ok", phase: "phase1", repository: { status: "ok" }, runtime_boundaries: { mouse_keyboard_control: false, auto_github_issue: false, silent_fallback: false } });
    if (path.endsWith("/overview")) return respond({ running_tasks: 1, pending_approvals: 1, candidate_issues: 1, approved_memories: 1, task_status: { running: 1 }, approval_status: { pending: 1 }, issue_candidate_status: { needs_review: 1 }, memory_approval_status: { approved: 1 }, trace_status: { passed: 1 } });
    if (path.endsWith("/tasks") && method === "GET") return respond(page(tasks));
    if (path.endsWith(`/tasks/${taskId}`)) return respond({ task: tasks[0], events: taskEvents });
    if (path.endsWith("/tasks") && method === "POST") return respond({ ...tasks[0], task_id: "rat_created", title: "请帮我规划一个 QE 10 loop 实验，先生成计划和审批草稿，不执行。" });
    if (path.endsWith("/events") && method === "POST") return respond({ ...taskEvents[0], event_id: "ratev_created" });
    if (path.endsWith("/memories") && method === "GET") return respond(page(memories));
    if (path.endsWith("/memories") && method === "POST") return respond({ ...memories[0], memory_id: "mem_created", approval_status: "draft" });
    if (path.includes("/memories/") && path.endsWith("/status")) return respond({ ...memories[0], approval_status: "approved" });
    if (path.endsWith("/context-packs") && method === "GET") return respond(page([{ context_pack_id: "ctx_demo", task_id: taskId, pack_summary: "Context Pack: 1 approved memories", token_budget: 16000, checksum: "hash-demo", pack_json: { mandatory_rules: ["Memory Ledger 是事实源"] } }]));
    if (path.endsWith("/context-packs") && method === "POST") return respond({ context_pack_id: "ctx_created", pack_summary: "Context Pack: 1 approved memories", token_budget: 16000, checksum: "hash-demo" });
    if (path.endsWith("/graph/summary")) return respond({ namespace: "aistock", entity_count: 1, relation_count: 1, evolution_path_count: 1, entities: [{ entity_id: "ent_1", entity_key: "QE", entity_type: "module", title: "QE", summary: "QuantEvolver" }], relations: [{ relation_id: "rel_1", relation_type: "depends_on" }], evolution_paths: [{ path_id: "path_1", objective: "提升 QE" }] });
    if (path.endsWith("/skills") && method === "GET") return respond(page(skills));
    if (path.endsWith("/skills/usage-events") && method === "GET") return respond(page([{ skill_event_id: "skillev_1", skill_key: "qe-evolution-diagnostics", status: "completed", created_at: "2026-05-21T09:04:30+08:00" }]));
    if (path.endsWith("/skills/qe-evolution-diagnostics/disable")) return respond({ ...skills[0], status: "blocked" });
    if (path.endsWith("/skills/qe-evolution-diagnostics/enable")) return respond({ ...skills[0], status: "approved" });
    if (path.endsWith("/mcp/servers")) return respond(page(mcpServers));
    if (path.endsWith("/mcp/tools")) return respond(page(mcpTools));
    if (path.endsWith("/mcp/preflight")) return respond({ server_key: "research-assistant", tool_name: "assistant_create_issue_candidate", risk_level: "high", approval_required: true, passed: false, status: "approval_required", trace_event: { event_type: "approval_required" }, deep_links: ["/research-assistant/approvals"], missing_confirmations: ["APPROVE_RESEARCH_ASSISTANT_ACTION"] });
    if (path.endsWith("/workbench/dry-run-execute")) return respond({ dry_run: true, status: "approval_required", preflight: { server_key: "research-assistant", tool_name: "assistant_create_issue_candidate", approval_required: true, passed: false, missing_confirmations: ["APPROVE_RESEARCH_ASSISTANT_ACTION"], tool_event_id: "mcptev_demo" }, tool_result: { executed: false, reason: "dry_run_execute_only" }, deep_link: "/research-assistant/workbench?tool_event_id=mcptev_demo" });
    if (path.endsWith("/approvals") && method === "GET") return respond(page(approvals));
    if (path.endsWith("/approvals") && method === "POST") return respond({ ...approvals[0], approval_id: "appr_created" });
    if (path.endsWith(`/approvals/${approvalId}/approve`)) return respond({ ...approvals[0], status: "approved" });
    if (path.endsWith(`/approvals/${approvalId}/reject`)) return respond({ ...approvals[0], status: "rejected" });
    if (path.endsWith("/issue-candidates") && method === "GET") return respond(page(issueCandidates));
    if (path.endsWith(`/issue-candidates/${candidateId}/github-sync`)) return respond({ ...issueCandidates[0], github_sync_status: "dry_run", github_sync_json: { direct_github_create_performed: false, formal_github_issue_requires_approval: true } });
    if (path.endsWith("/models/profiles")) return respond(page(modelProfiles));
    if (path.endsWith("/models/routing-policies")) return respond(page(routingPolicies));
    if (path.endsWith("/models/route")) return respond({ role: "cheap_worker", model_profile: { provider: "glm", role: "cheap_worker" }, temp_memory_only_for_low_cost: true });
    if (path.endsWith("/temp-memories")) return respond({ temp_memory_id: "tmpmem_demo", content_text: "progress" });
    if (path.endsWith("/notifications/summary")) return respond({ unread: 1, counts: { unread: 1 }, items: [] });
    if (path.endsWith("/notifications")) return respond(page([{ notification_id: "notif_1", title: "ready", message: "ok", status: "unread", risk_level: "low", created_at: "2026-05-21T09:06:00+08:00" }]));
    if (path.endsWith("/external-agent/sessions")) return respond(page(externalSessions));
    if (path.endsWith("/external-agent/events")) return respond(page(externalEvents));
    if (path.endsWith("/trace-events")) return respond(page(traceEvents));
    if (path.endsWith("/reports")) return respond(page([{ report_id: "report_1", report_type: "morning", title: "研究助理晨报模板", body_md: "阶段一提供真实报告数据结构", status: "draft" }]));
    if (path.endsWith("/agenda")) return respond(page([{ agenda_item_id: "agenda_1", title: "今日关注", status: "open" }]));
    if (path.endsWith("/validation-discovery/summary")) return respond({ latest_reports: [{ discovery_report_id: "vdr_1", title: "夜间测试汇报", status: "draft", run_date: "2026-05-21" }], candidate_issues_needing_review: issueCandidates });

    return route.fulfill({ status: 404, contentType: "application/json", body: JSON.stringify({ detail: `unexpected research assistant route: ${path}` }) });
  });

  await browserPage.goto("/research-assistant");
  await expect(browserPage.getByRole("heading", { name: "研究与实验综合助理" })).toBeVisible();
  await expect(browserPage.getByText("不控制鼠标键盘", { exact: true })).toBeVisible();
  await expect(browserPage.getByText("阶段一总览")).toBeVisible();
  await expect(browserPage.getByText("候选 Issue").first()).toBeVisible();

  await browserPage.getByRole("link", { name: "MCP 工具" }).click();
  await expect(browserPage.getByRole("heading", { name: "MCP Tools" })).toBeVisible();
  await expect(browserPage.getByText("assistant_create_issue_candidate").first()).toBeVisible();

  await browserPage.getByRole("link", { name: "工作台" }).click();
  await expect(browserPage.getByRole("heading", { name: "MCP 执行工作台" })).toBeVisible();
  await browserPage.getByRole("button", { name: "执行 preflight" }).click();
  await expect(browserPage.getByText("Missing Confirmations").first()).toBeVisible();
  await expect(browserPage.getByText("需要审批").first()).toBeVisible();
  await expect(browserPage.getByText("/research-assistant/approvals").first()).toBeVisible();
  await browserPage.getByRole("button", { name: /dry-run/ }).click();
  await browserPage.getByText("dry-run tool result / deep link").click();
  await expect(browserPage.getByText("dry_run_execute_only").first()).toBeVisible();

  await browserPage.getByRole("link", { name: "任务" }).click();
  await expect(browserPage.getByRole("heading", { name: "Task Ledger" })).toBeVisible();
  await expect(browserPage.getByText("QE 10 loop 实验规划").first()).toBeVisible();
  await browserPage.getByRole("button", { name: /QE 10 loop 实验规划/ }).click();
  await expect(browserPage.getByText("已生成计划草稿").first()).toBeVisible();
  await browserPage.getByRole("button", { name: "标记 triage" }).click();

  await browserPage.getByRole("link", { name: "记忆" }).click();
  await expect(browserPage.getByRole("heading", { name: "Memory Ledger" })).toBeVisible();
  await expect(browserPage.getByText("长期记忆原则").first()).toBeVisible();

  await browserPage.getByRole("link", { name: "审批" }).click();
  await expect(browserPage.getByRole("heading", { name: "审批中心" })).toBeVisible();
  await expect(browserPage.getByText("高风险 MCP 调用").first()).toBeVisible();
  await expect(browserPage.getByRole("button", { name: "批准" })).toBeDisabled();
  await browserPage.getByLabel(/输入确认文本/).fill("APPROVE_RESEARCH_ASSISTANT_ACTION");
  await expect(browserPage.getByRole("button", { name: "批准" })).toBeEnabled();

  await browserPage.getByRole("link", { name: "Skills" }).click();
  await expect(browserPage.getByText("QE 实验诊断").first()).toBeVisible();

  await browserPage.getByRole("link", { name: "模型路由" }).click();
  await browserPage.getByRole("button", { name: "测试低价模型路由" }).click();
  await browserPage.getByText("路由结果").click();
  await expect(browserPage.getByText("Temp Memory Only For Low Cost")).toBeVisible();

  await browserPage.getByRole("link", { name: "候选 Issue" }).click();
  await expect(browserPage.getByRole("heading", { name: "候选 Issue 队列" })).toBeVisible();
  await expect(browserPage.getByText("策略包选股前置检查阻断").first()).toBeVisible();

  await browserPage.getByRole("link", { name: "通知" }).click();
  await expect(browserPage.getByRole("heading", { name: "通知中心" })).toBeVisible();
  await expect(browserPage.getByText("ready").first()).toBeVisible();

  await browserPage.getByRole("link", { name: "外部 Agent" }).click();
  await expect(browserPage.getByRole("heading", { name: "External Agent Connector" })).toBeVisible();
  await expect(browserPage.getByText("Claude Code").first()).toBeVisible();

  await browserPage.getByRole("link", { name: "Trace" }).click();
  await expect(browserPage.getByRole("heading", { name: "Trace 与成本" })).toBeVisible();
  await expect(browserPage.getByText("mcp_preflight").first()).toBeVisible();

  await browserPage.getByRole("link", { name: "设置" }).click();
  await expect(browserPage.getByRole("heading", { name: "研究助理设置" })).toBeVisible();
  await expect(browserPage.getByText("http://127.0.0.1", { exact: false })).toBeVisible();

  await browserPage.getByRole("link", { name: "发现流" }).click();
  await expect(browserPage.getByText("夜间测试汇报").first()).toBeVisible();

  await browserPage.getByRole("link", { name: "对话" }).click();
  await browserPage.getByRole("button", { name: "生成计划任务" }).click();
  await expect(browserPage.getByText("已创建的 Task Ledger 记录")).toBeVisible();

  expect(writeMethods).toContain("POST /api/v1/research-assistant/mcp/preflight");
  expect(writeMethods).toContain("POST /api/v1/research-assistant/workbench/dry-run-execute");
  expect(writeMethods).toContain("POST /api/v1/research-assistant/tasks/rat_demo_1/events");
  expect(writeMethods).toContain("POST /api/v1/research-assistant/models/route");
  expect(writeMethods).toContain("POST /api/v1/research-assistant/tasks");
  expect(writeMethods).toContain("POST /api/v1/research-assistant/tasks/rat_created/events");
  expect(pageErrors).toEqual([]);
  expect(consoleErrors.filter((message) => !message.includes("Failed to load resource"))).toEqual([]);
});


test("Research Assistant independent pages show real empty and error states", async ({ page: browserPage }) => {
  const consoleErrors: string[] = [];
  const pageErrors: string[] = [];
  browserPage.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  browserPage.on("pageerror", (error) => pageErrors.push(error.message));

  await browserPage.route("**/api/v1/research-assistant/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;
    const method = route.request().method();
    const respond = (data: unknown, status = 200) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify(status >= 400 ? data : { status: "success", data }) });
    if (path.endsWith("/overview")) return respond({ trace_status: {} });
    if (path.endsWith("/notifications/summary")) return respond({ unread: 0, counts: {} });
    if (path.endsWith("/notifications")) return respond(page([]));
    if (path.endsWith("/issue-candidates")) return respond(page([]));
    if (path.endsWith("/external-agent/sessions")) return respond(page([]));
    if (path.endsWith("/external-agent/events")) return respond(page([]));
    if (path.endsWith("/trace-events")) return respond(page([]));
    if (path.endsWith("/health")) return respond({ detail: "schema_missing for settings" }, 503);
    if (path.endsWith("/mcp/servers")) return respond(page([]));
    if (path.endsWith("/mcp/tools")) return respond(page(mcpTools));
    if (path.endsWith("/skills") || path.endsWith("/skills/usage-events")) return respond(page([]));
    if (path.endsWith("/tasks") && method === "GET") return respond(page([]));
    if (path.endsWith("/mcp/preflight") || path.endsWith("/workbench/dry-run-execute")) return respond({ detail: "preflight contract failed" }, 400);
    return respond(page([]));
  });

  await browserPage.goto("/research-assistant/issue-candidates");
  await expect(browserPage.getByText("候选 Issue 队列为空")).toBeVisible();

  await browserPage.getByRole("link", { name: "通知" }).click();
  await expect(browserPage.getByText("通知列表为空")).toBeVisible();

  await browserPage.getByRole("link", { name: "外部 Agent" }).click();
  await expect(browserPage.getByText("外部 Agent session 为空")).toBeVisible();

  await browserPage.getByRole("link", { name: "Trace" }).click();
  await expect(browserPage.getByText("Trace Event 为空")).toBeVisible();

  await browserPage.getByRole("link", { name: "设置" }).click();
  await expect(browserPage.getByText("schema_missing for settings")).toBeVisible();

  await browserPage.getByRole("link", { name: "工作台" }).click();
  await browserPage.getByRole("button", { name: "执行 preflight" }).click();
  await expect(browserPage.getByText("preflight contract failed")).toBeVisible();

  expect(pageErrors).toEqual([]);
  expect(consoleErrors.filter((message) => !message.includes("Failed to load resource"))).toEqual([]);
});




