export type JsonObject = Record<string, unknown>;

export const API_BASE = (process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1").replace(/\/+$/, "");

export type AssistantEnvelope<T> = {
  status: string;
  data: T;
};

export type AssistantPage<T> = {
  items: T[];
  total: number;
  page: number;
  page_size: number;
  has_more: boolean;
};

export type AssistantHealth = JsonObject & {
  service?: string;
  status?: string;
  repository?: JsonObject;
  runtime_boundaries?: JsonObject;
};

export type AssistantOverview = JsonObject & {
  task_status?: Record<string, number>;
  approval_status?: Record<string, number>;
  issue_candidate_status?: Record<string, number>;
  memory_approval_status?: Record<string, number>;
  trace_status?: Record<string, number>;
  running_tasks?: number;
  pending_approvals?: number;
  candidate_issues?: number;
  approved_memories?: number;
};

export type AssistantTask = JsonObject & {
  task_id: string;
  title: string;
  status?: string;
  task_type?: string;
  risk_level?: string;
  plan_digest?: string | null;
  created_by?: string;
  created_at?: string;
  updated_at?: string;
};

export type AssistantTaskEvent = JsonObject & {
  event_id: string;
  task_id?: string;
  event_type?: string;
  severity?: string;
  message?: string;
  payload_json?: JsonObject;
  evidence_refs?: string[];
  created_at?: string;
};

export type AssistantMemory = JsonObject & {
  memory_id: string;
  memory_type?: string;
  namespace?: string;
  subject_key?: string;
  title?: string;
  content_text?: string;
  approval_status?: string;
  risk_level?: string;
  source_ref?: string | null;
  evidence_refs?: string[];
  updated_at?: string;
};

export type AssistantContextPack = JsonObject & {
  context_pack_id: string;
  task_id?: string | null;
  pack_summary?: string;
  token_budget?: number;
  checksum?: string;
  pack_json?: JsonObject;
};

export type AssistantMcpServer = JsonObject & {
  server_id: string;
  server_key: string;
  title?: string;
  status?: string;
  health_json?: JsonObject;
};

export type AssistantMcpTool = JsonObject & {
  tool_id: string;
  server_key: string;
  tool_name: string;
  title?: string;
  description?: string | null;
  capability_key?: string | null;
  capability?: string | null;
  mcp_module?: string | null;
  module?: string | null;
  category?: string | null;
  phase?: string | null;
  tags?: string[];
  risk_level?: string;
  side_effect_level?: string;
  requires_approval?: boolean;
  input_schema_json?: JsonObject;
  preflight_schema_json?: JsonObject;
  required_confirmations?: string[];
  status?: string;
};

export type AssistantCapability = JsonObject & {
  capability_id: string;
  capability_key: string;
  capability_type?: string;
  title?: string;
  description_for_llm?: string;
  risk_level?: string;
  side_effect_level?: string;
  required_confirmations?: string[];
  input_slots?: JsonObject;
  output_cards?: string[];
  mcp_tool_refs?: JsonObject[];
  skill_refs?: string[];
  status?: string;
  checksum?: string;
};

export type AssistantActionProposal = JsonObject & {
  action_proposal_id: string;
  task_id?: string;
  capability_key?: string;
  proposal_type?: string;
  title?: string;
  summary?: string;
  risk_level?: string;
  side_effect_level?: string;
  input_json?: JsonObject;
  expected_result_json?: JsonObject;
  plan_digest?: string;
  approval_id?: string | null;
  status?: string;
  created_at?: string;
  updated_at?: string;
};

export type AssistantActionProposalResult = JsonObject & {
  status?: string;
  executed?: boolean;
  proposal?: AssistantActionProposal;
  tool_event?: JsonObject;
  trace_id?: string;
  human_cards?: JsonObject[];
  error?: JsonObject;
};

export type LocalDataPhaseKey = "check" | "plan" | "confirm" | "execute" | "review";

export type LocalDataPhase = {
  key: LocalDataPhaseKey;
  title: string;
  shortTitle: string;
  description: string;
  primaryTools: string[];
  riskLevel: "read_only" | "plan_only" | "write_control_plane" | "run_data_job";
  requiresConfirmation: boolean;
};

export const LOCAL_DATA_MANAGEMENT_CAPABILITY = {
  capabilityKey: "local_data_management",
  displayName: "本地数据管理",
  gatewayModule: "local_data",
  promptBranch: "prompt.local_data_management",
  memorySubject: "architecture.local_data_management.mcp_gateway",
  summary: "通过统一 MCP Gateway 调用后端 local-data facade，完成数据状态检查、修复计划、用户确认、执行和复查。",
};

export const LOCAL_DATA_MANAGEMENT_PHASES: LocalDataPhase[] = [
  {
    key: "check",
    title: "本地数据检查",
    shortTitle: "检查",
    description: "只读读取 data_stats、最近任务、活跃告警、sync targets 和业务 readiness。",
    primaryTools: [
      "local_data_health_overview",
      "local_data_list_jobs",
      "local_data_list_alerts",
      "local_data_list_sync_targets",
      "local_data_check_gaps",
    ],
    riskLevel: "read_only",
    requiresConfirmation: false,
  },
  {
    key: "plan",
    title: "生成修复计划",
    shortTitle: "计划",
    description: "把缺口、阻断 target、失败任务和影响模块整理为中文修复步骤，不执行写操作。",
    primaryTools: [
      "local_data_plan_repair",
      "local_data_plan_schedule_reset",
      "local_data_explain_business_impact",
    ],
    riskLevel: "plan_only",
    requiresConfirmation: false,
  },
  {
    key: "confirm",
    title: "等待用户确认",
    shortTitle: "确认",
    description: "展示将调用的工具、写入范围、长任务风险和确认口令；确认前禁止执行。",
    primaryTools: [
      "local_data_apply_repair_confirmed",
      "local_data_run_dataset_sync_confirmed",
      "local_data_apply_schedule_reset_confirmed",
    ],
    riskLevel: "write_control_plane",
    requiresConfirmation: true,
  },
  {
    key: "execute",
    title: "执行数据任务",
    shortTitle: "执行",
    description: "确认后调度同步、刷新缓存、ack 告警或维护计划任务，并记录 trace。",
    primaryTools: [
      "local_data_run_incremental_confirmed",
      "local_data_refresh_stats_confirmed",
      "local_data_apply_repair_confirmed",
      "local_data_run_schedule_confirmed",
    ],
    riskLevel: "run_data_job",
    requiresConfirmation: true,
  },
  {
    key: "review",
    title: "复查与结论",
    shortTitle: "复查",
    description: "重新读取健康总览、任务状态和业务影响，给出完成、仍阻断或需转 Issue 的结论。",
    primaryTools: [
      "local_data_get_repair_status",
      "local_data_health_overview",
      "local_data_get_job",
    ],
    riskLevel: "read_only",
    requiresConfirmation: false,
  },
];

export const LOCAL_DATA_TOOL_LABELS: Record<string, string> = {
  local_data_health_overview: "数据健康总览",
  local_data_get_dataset_status: "数据集状态",
  local_data_list_data_stats: "数据看板列表",
  local_data_check_gaps: "缺口检查",
  local_data_compute_auto_range: "自动补齐区间",
  local_data_list_alerts: "活跃告警列表",
  local_data_get_unack_alert_count: "未确认告警数量",
  local_data_list_sync_targets: "同步 target 列表",
  local_data_get_sync_target: "同步 target 详情",
  local_data_list_sync_attempts: "同步 attempt 时间线",
  local_data_list_jobs: "同步任务列表",
  local_data_get_job: "同步任务详情",
  local_data_get_job_logs: "关键日志摘要",
  local_data_cancel_job_confirmed: "取消运行中任务",
  local_data_clear_queued_jobs_confirmed: "清理排队任务",
  local_data_delete_job_confirmed: "删除历史任务",
  local_data_run_dataset_sync_confirmed: "运行数据同步",
  local_data_run_incremental_confirmed: "运行增量同步",
  local_data_run_init_confirmed: "运行初始化",
  local_data_run_schedule_confirmed: "立即运行计划任务",
  local_data_run_single_preset_confirmed: "运行单个预置任务",
  local_data_run_all_presets_confirmed: "运行全部预置任务",
  local_data_refresh_stats_confirmed: "刷新数据看板缓存",
  local_data_sync_calendar_confirmed: "同步交易日历",
  local_data_build_sector_data_confirmed: "构建申万行业数据",
  local_data_export_sector_data_confirmed: "导出行业数据",
  local_data_sync_tushare_all_confirmed: "批量同步 Tushare",
  local_data_list_schedules: "计划任务列表",
  local_data_get_schedule_defaults: "默认计划模板",
  local_data_upsert_schedule_confirmed: "创建或更新计划任务",
  local_data_batch_create_schedules_confirmed: "批量创建或更新计划任务",
  local_data_toggle_schedule_confirmed: "启停计划任务",
  local_data_delete_schedule_confirmed: "删除计划任务",
  local_data_plan_schedule_reset: "生成计划任务重置 diff",
  local_data_apply_schedule_reset_confirmed: "应用计划任务重置",
  local_data_get_preset_stats: "预置计划覆盖情况",
  local_data_get_preset_daily_status: "当日预置任务状态",
  local_data_run_source_test_confirmed: "运行数据源测试",
  local_data_list_source_test_runs: "数据源测试历史",
  local_data_list_source_test_schedules: "数据源测试计划",
  local_data_upsert_source_test_schedule_confirmed: "创建或更新测试计划",
  local_data_toggle_source_test_schedule_confirmed: "启停测试计划",
  local_data_run_source_test_schedule_confirmed: "立即运行测试计划",
  local_data_plan_repair: "生成本地数据修复计划",
  local_data_apply_repair_confirmed: "执行本地数据修复计划",
  local_data_get_repair_status: "修复进度复查",
  local_data_explain_business_impact: "解释业务影响",
};

export const LOCAL_DATA_RISK_LABELS: Record<string, string> = {
  read_only: "只读检查",
  plan_only: "只生成计划",
  write_control_plane: "写控制面",
  run_data_job: "启动数据任务",
  destructive: "破坏性操作",
  high: "高风险",
  medium: "中风险",
  low: "低风险",
};

export function localDataRiskLabel(risk: unknown): string {
  const key = String(risk || "").trim();
  return LOCAL_DATA_RISK_LABELS[key] || LOCAL_DATA_RISK_LABELS[key.toLowerCase()] || key || "未标注风险";
}

export function localDataToolTitle(tool: Pick<AssistantMcpTool, "tool_name" | "title">): string {
  return tool.title || LOCAL_DATA_TOOL_LABELS[tool.tool_name] || tool.tool_name;
}

export function localDataToolPhase(toolName: string): LocalDataPhase | undefined {
  return LOCAL_DATA_MANAGEMENT_PHASES.find((phase) => phase.primaryTools.includes(toolName));
}

export function isLocalDataManagementTool(tool: Pick<AssistantMcpTool, "server_key" | "tool_name"> & JsonObject): boolean {
  const values = [
    tool.server_key,
    tool.tool_name,
    tool.capability_key,
    tool.capability,
    tool.mcp_module,
    tool.module,
    tool.category,
  ].map((value) => String(value || "").toLowerCase());
  return values.some((value) => value === "local_data" || value === "local_data_management" || value === "capability.local_data_management" || value.includes("local_data"));
}

export type AssistantSkill = JsonObject & {
  skill_id: string;
  skill_key: string;
  title?: string;
  description?: string;
  domain?: string;
  risk_level?: string;
  permission_scope?: string;
  checksum?: string;
  status?: string;
};

export type AssistantSkillUsageEvent = JsonObject & {
  skill_event_id: string;
  skill_id?: string;
  skill_key?: string;
  task_id?: string;
  status?: string;
  input_summary_json?: JsonObject;
  output_summary_json?: JsonObject;
  evidence_refs?: string[];
  error_message?: string | null;
  started_at?: string | null;
  completed_at?: string | null;
  created_at?: string;
};

export type AssistantApproval = JsonObject & {
  approval_id: string;
  task_id?: string | null;
  approval_type?: string;
  risk_level?: string;
  plan_digest?: string;
  summary?: string;
  required_confirmation_text?: string;
  status?: string;
  created_at?: string;
};

export type AssistantIssueCandidate = JsonObject & {
  candidate_id: string;
  title?: string;
  severity?: string;
  module?: string;
  status?: string;
  problem_statement?: string;
  reproduce_command?: string | null;
  github_sync_status?: string;
  github_issue_number?: number | null;
  github_issue_url?: string | null;
  github_sync_json?: JsonObject;
  evidence_refs?: string[];
};

export type AssistantWorkbenchDryRunResult = JsonObject & {
  dry_run?: boolean;
  status?: string;
  preflight?: JsonObject;
  tool_result?: JsonObject;
  deep_link?: string;
};

export type AssistantModelProfile = JsonObject & {
  model_profile_id: string;
  provider?: string;
  model_name?: string;
  role?: string;
  status?: string;
  capabilities_json?: JsonObject;
  cost_json?: JsonObject;
  limits_json?: JsonObject;
};

export type AssistantRoutingPolicy = JsonObject & {
  policy_id: string;
  role?: string;
  risk_level?: string;
  model_profile_id?: string;
  status?: string;
};

export type AssistantNotification = JsonObject & {
  notification_id: string;
  title?: string;
  message?: string;
  status?: string;
  risk_level?: string;
  created_at?: string;
};

export type AssistantExternalSession = JsonObject & {
  session_id: string;
  agent_type?: string;
  agent_name?: string;
  status?: string;
  auth_scope?: JsonObject;
  metadata_json?: JsonObject;
  created_at?: string;
};

export type AssistantExternalEvent = JsonObject & {
  external_event_id: string;
  session_id?: string;
  event_type?: string;
  risk_level?: string;
  payload_json?: JsonObject;
  evidence_refs?: string[];
  created_at?: string;
};

export type AssistantTraceEvent = JsonObject & {
  trace_id: string;
  task_id?: string;
  event_type?: string;
  component?: string;
  status?: string;
  payload_json?: JsonObject;
  cost_json?: JsonObject;
  duration_ms?: number;
  created_at?: string;
};

export type AssistantReport = JsonObject & {
  report_id: string;
  report_type?: string;
  title?: string;
  body_md?: string;
  status?: string;
  summary_json?: JsonObject;
  created_at?: string;
};

export type AssistantGraphSummary = JsonObject & {
  namespace?: string;
  entity_count?: number;
  relation_count?: number;
  evolution_path_count?: number;
  entities?: JsonObject[];
  relations?: JsonObject[];
  evolution_paths?: JsonObject[];
};


export type AssistantPromptNode = JsonObject & {
  prompt_node_id: string;
  prompt_key?: string;
  title?: string;
  category?: string;
  tree_path?: string;
  phase?: string;
  status?: string;
};

export type AssistantPromptBundle = JsonObject & {
  prompt_bundle_id: string;
  phase?: string;
  checksum?: string;
  node_refs?: JsonObject[];
  selection_trace_json?: JsonObject;
  cache_path?: string | null;
};

export type AssistantModeDecision = JsonObject & {
  mode?: "dialogue" | "analysis" | "planning" | "preflight" | "execution" | "audit" | "recovery" | string;
  intent_type?: string;
  confidence?: number;
  mode_reason?: string;
  requires_tool?: boolean;
  allowed_tool_side_effect?: string;
  requires_user_confirmation?: boolean;
  requires_approval?: boolean;
  visible_audit_default?: boolean;
};

export type AssistantConversationMessage = JsonObject & {
  message_id: string;
  conversation_id?: string;
  role?: "user" | "assistant" | "system" | "tool";
  content_text?: string;
  content_json?: JsonObject;
  task_id?: string | null;
  created_at?: string;
};

export type AssistantChatTurnResult = JsonObject & {
  conversation?: JsonObject;
  user_message?: AssistantConversationMessage;
  assistant_message?: AssistantConversationMessage;
  task?: AssistantTask;
  task_events?: AssistantTaskEvent[];
  prompt_bundle?: AssistantPromptBundle;
  context_pack?: JsonObject;
  context_health?: JsonObject;
  trace?: JsonObject;
  cards?: JsonObject;
  mode_decision?: AssistantModeDecision;
};

export type AssistantCatalogReadinessCheck = JsonObject & {
  catalog: string;
  label: string;
  expected_min: number;
  present: number;
  ready: boolean;
  filters?: JsonObject;
  missing_count?: number;
};

export type AssistantCatalogReadiness = JsonObject & {
  ready: boolean;
  status: "ready" | "catalog_not_ready" | string;
  checks: AssistantCatalogReadinessCheck[];
  missing_catalogs: string[];
  operator_action?: string | null;
  human_message?: string;
  generated_at?: string;
};

export type AssistantValidationDiscoverySummary = JsonObject & {
  latest_reports?: JsonObject[];
  candidate_issues_needing_review?: AssistantIssueCandidate[];
};

export class ResearchAssistantApiError extends Error {
  status: number;
  raw: unknown;

  constructor(message: string, status: number, raw: unknown) {
    super(message);
    this.name = "ResearchAssistantApiError";
    this.status = status;
    this.raw = raw;
  }
}

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorMessage(payload: unknown, status: number): string {
  if (isObject(payload)) {
    if (typeof payload.detail === "string") return payload.detail;
    if (isObject(payload.detail)) return JSON.stringify(payload.detail);
    if (typeof payload.message === "string") return payload.message;
    if (typeof payload.error === "string") return payload.error;
  }
  if (typeof payload === "string" && payload.trim()) return payload;
  return `HTTP ${status}`;
}

function appendQuery(path: string, params: Record<string, string | number | boolean | undefined | null>): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") continue;
    query.set(key, String(value));
  }
  const text = query.toString();
  return text ? `${path}?${text}` : path;
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers || {}),
    },
  });
  const text = await response.text();
  let payload: unknown = {};
  try {
    payload = text ? JSON.parse(text) : {};
  } catch {
    payload = text;
  }
  if (!response.ok) throw new ResearchAssistantApiError(errorMessage(payload, response.status), response.status, payload);
  return payload as T;
}

async function unwrap<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await apiFetch<AssistantEnvelope<T>>(path, init);
  return response.data;
}

function post<T>(path: string, body: unknown): Promise<T> {
  return unwrap<T>(path, { method: "POST", body: JSON.stringify(body) });
}

export const researchAssistantApi = {
  health(): Promise<AssistantHealth> {
    return unwrap<AssistantHealth>("/research-assistant/health");
  },
  overview(): Promise<AssistantOverview> {
    return unwrap<AssistantOverview>("/research-assistant/overview");
  },
  seedCatalogs(): Promise<JsonObject> {
    return post<JsonObject>("/research-assistant/catalogs/seed", {});
  },
  catalogReadiness(): Promise<AssistantCatalogReadiness> {
    return unwrap<AssistantCatalogReadiness>("/research-assistant/catalogs/readiness");
  },

  chatTurn(payload: JsonObject): Promise<AssistantChatTurnResult> {
    return post<AssistantChatTurnResult>("/research-assistant/chat/turn", payload);
  },
  conversation(conversationId: string): Promise<JsonObject> {
    return unwrap<JsonObject>(`/research-assistant/conversations/${encodeURIComponent(conversationId)}`);
  },
  promptNodes(params: { phase?: string; category?: string; status?: string; search?: string; limit?: number; offset?: number } = {}): Promise<AssistantPage<AssistantPromptNode>> {
    return unwrap<AssistantPage<AssistantPromptNode>>(appendQuery("/research-assistant/prompt-nodes", { limit: 100, ...params }));
  },
  buildPromptBundle(payload: JsonObject): Promise<AssistantPromptBundle> {
    return post<AssistantPromptBundle>("/research-assistant/prompt-bundles", payload);
  },
  promptBundles(params: { task_id?: string; conversation_id?: string; phase?: string; limit?: number; offset?: number } = {}): Promise<AssistantPage<AssistantPromptBundle>> {
    return unwrap<AssistantPage<AssistantPromptBundle>>(appendQuery("/research-assistant/prompt-bundles", { limit: 50, ...params }));
  },
  tasks(params: { status?: string; task_type?: string; search?: string; limit?: number; offset?: number } = {}): Promise<AssistantPage<AssistantTask>> {
    return unwrap<AssistantPage<AssistantTask>>(appendQuery("/research-assistant/tasks", { limit: 50, ...params }));
  },
  createTask(payload: JsonObject): Promise<AssistantTask> {
    return post<AssistantTask>("/research-assistant/tasks", payload);
  },
  task(taskId: string): Promise<{ task: AssistantTask; events: AssistantTaskEvent[] }> {
    return unwrap<{ task: AssistantTask; events: AssistantTaskEvent[] }>(`/research-assistant/tasks/${encodeURIComponent(taskId)}`);
  },
  addTaskEvent(taskId: string, payload: JsonObject): Promise<AssistantTaskEvent> {
    return post<AssistantTaskEvent>(`/research-assistant/tasks/${encodeURIComponent(taskId)}/events`, payload);
  },
  memories(params: { namespace?: string; memory_type?: string; approval_status?: string; search?: string; limit?: number; offset?: number } = {}): Promise<AssistantPage<AssistantMemory>> {
    return unwrap<AssistantPage<AssistantMemory>>(appendQuery("/research-assistant/memories", { limit: 50, ...params }));
  },
  createMemory(payload: JsonObject): Promise<AssistantMemory> {
    return post<AssistantMemory>("/research-assistant/memories", payload);
  },
  updateMemoryStatus(memoryId: string, payload: JsonObject): Promise<AssistantMemory> {
    return post<AssistantMemory>(`/research-assistant/memories/${encodeURIComponent(memoryId)}/status`, payload);
  },
  contextPacks(params: { task_id?: string; agent_id?: string; limit?: number; offset?: number } = {}): Promise<AssistantPage<AssistantContextPack>> {
    return unwrap<AssistantPage<AssistantContextPack>>(appendQuery("/research-assistant/context-packs", { limit: 50, ...params }));
  },
  buildContextPack(payload: JsonObject): Promise<AssistantContextPack> {
    return post<AssistantContextPack>("/research-assistant/context-packs", payload);
  },
  graphSummary(namespace = "aistock"): Promise<AssistantGraphSummary> {
    return unwrap<AssistantGraphSummary>(appendQuery("/research-assistant/graph/summary", { namespace }));
  },
  skills(): Promise<AssistantPage<AssistantSkill>> {
    return unwrap<AssistantPage<AssistantSkill>>("/research-assistant/skills?limit=200");
  },
  enableSkill(skillKey: string): Promise<JsonObject> {
    return post<JsonObject>(`/research-assistant/skills/${encodeURIComponent(skillKey)}/enable`, {});
  },
  disableSkill(skillKey: string): Promise<JsonObject> {
    return post<JsonObject>(`/research-assistant/skills/${encodeURIComponent(skillKey)}/disable`, {});
  },
  skillUsageEvents(params: { skill_key?: string; task_id?: string; status?: string; limit?: number; offset?: number } = {}): Promise<AssistantPage<AssistantSkillUsageEvent>> {
    return unwrap<AssistantPage<AssistantSkillUsageEvent>>(appendQuery("/research-assistant/skills/usage-events", { limit: 100, ...params }));
  },
  createSkillUsageEvent(payload: JsonObject): Promise<AssistantSkillUsageEvent> {
    return post<AssistantSkillUsageEvent>("/research-assistant/skills/usage-events", payload);
  },
  mcpServers(): Promise<AssistantPage<AssistantMcpServer>> {
    return unwrap<AssistantPage<AssistantMcpServer>>("/research-assistant/mcp/servers");
  },
  mcpTools(params: { server_key?: string; risk_level?: string; search?: string; limit?: number; offset?: number; include_schema?: boolean } = {}): Promise<AssistantPage<AssistantMcpTool>> {
    const includeSchema = params.include_schema === true;
    const requestedLimit = params.limit ?? 50;
    const compactLimit = Math.min(requestedLimit, 50);
    return unwrap<AssistantPage<AssistantMcpTool>>(appendQuery("/research-assistant/mcp/tools", { ...params, limit: includeSchema ? requestedLimit : compactLimit, include_schema: includeSchema }));
  },
  capabilities(params: { status?: string; risk_level?: string; search?: string; limit?: number; offset?: number } = {}): Promise<AssistantPage<AssistantCapability>> {
    return unwrap<AssistantPage<AssistantCapability>>(appendQuery("/research-assistant/capabilities", { limit: 100, ...params }));
  },
  syncCapabilities(payload: JsonObject): Promise<JsonObject> {
    return post<JsonObject>("/research-assistant/capabilities/sync", payload);
  },
  actionProposals(params: { task_id?: string; capability_key?: string; status?: string; limit?: number; offset?: number } = {}): Promise<AssistantPage<AssistantActionProposal>> {
    return unwrap<AssistantPage<AssistantActionProposal>>(appendQuery("/research-assistant/actions", { limit: 100, ...params }));
  },
  createActionProposal(payload: JsonObject): Promise<AssistantActionProposal> {
    return post<AssistantActionProposal>("/research-assistant/actions/propose", payload);
  },
  actionProposal(actionProposalId: string): Promise<JsonObject> {
    return unwrap<JsonObject>(`/research-assistant/actions/${encodeURIComponent(actionProposalId)}`);
  },
  actionProposalEvents(actionProposalId: string): Promise<JsonObject> {
    return unwrap<JsonObject>(`/research-assistant/actions/${encodeURIComponent(actionProposalId)}/events`);
  },
  confirmActionProposal(actionProposalId: string, payload: JsonObject): Promise<AssistantActionProposal> {
    return post<AssistantActionProposal>(`/research-assistant/actions/${encodeURIComponent(actionProposalId)}/confirm`, payload);
  },
  rejectActionProposal(actionProposalId: string, payload: JsonObject): Promise<AssistantActionProposal> {
    return post<AssistantActionProposal>(`/research-assistant/actions/${encodeURIComponent(actionProposalId)}/reject`, payload);
  },
  preflightActionProposal(actionProposalId: string, payload: JsonObject): Promise<JsonObject> {
    return post<JsonObject>(`/research-assistant/actions/${encodeURIComponent(actionProposalId)}/preflight`, payload);
  },
  approveActionProposal(actionProposalId: string, payload: JsonObject): Promise<JsonObject> {
    return post<JsonObject>(`/research-assistant/actions/${encodeURIComponent(actionProposalId)}/approve`, payload);
  },
  executeActionProposal(actionProposalId: string, payload: JsonObject): Promise<AssistantActionProposalResult> {
    return post<AssistantActionProposalResult>(`/research-assistant/actions/${encodeURIComponent(actionProposalId)}/execute`, payload);
  },
  preflightMcpTool(payload: JsonObject): Promise<JsonObject> {
    return post<JsonObject>("/research-assistant/mcp/preflight", payload);
  },
  dryRunExecuteTool(payload: JsonObject): Promise<AssistantWorkbenchDryRunResult> {
    return post<AssistantWorkbenchDryRunResult>("/research-assistant/workbench/dry-run-execute", payload);
  },
  approvals(params: { status?: string; risk_level?: string; limit?: number; offset?: number } = {}): Promise<AssistantPage<AssistantApproval>> {
    return unwrap<AssistantPage<AssistantApproval>>(appendQuery("/research-assistant/approvals", { limit: 50, ...params }));
  },
  createApproval(payload: JsonObject): Promise<AssistantApproval> {
    return post<AssistantApproval>("/research-assistant/approvals", payload);
  },
  approve(approvalId: string, confirmation_text: string): Promise<AssistantApproval> {
    return post<AssistantApproval>(`/research-assistant/approvals/${encodeURIComponent(approvalId)}/approve`, { confirmation_text });
  },
  reject(approvalId: string): Promise<AssistantApproval> {
    return post<AssistantApproval>(`/research-assistant/approvals/${encodeURIComponent(approvalId)}/reject`, { confirmation_text: "" });
  },
  issueCandidates(params: { status?: string; module?: string; search?: string; limit?: number; offset?: number } = {}): Promise<AssistantPage<AssistantIssueCandidate>> {
    return unwrap<AssistantPage<AssistantIssueCandidate>>(appendQuery("/research-assistant/issue-candidates", { limit: 50, ...params }));
  },
  createIssueCandidate(payload: JsonObject): Promise<AssistantIssueCandidate> {
    return post<AssistantIssueCandidate>("/research-assistant/issue-candidates", payload);
  },
  githubSyncIssueCandidate(candidateId: string, payload: JsonObject): Promise<AssistantIssueCandidate> {
    return post<AssistantIssueCandidate>(`/research-assistant/issue-candidates/${encodeURIComponent(candidateId)}/github-sync`, payload);
  },
  modelProfiles(): Promise<AssistantPage<AssistantModelProfile>> {
    return unwrap<AssistantPage<AssistantModelProfile>>("/research-assistant/models/profiles");
  },
  routingPolicies(): Promise<AssistantPage<AssistantRoutingPolicy>> {
    return unwrap<AssistantPage<AssistantRoutingPolicy>>("/research-assistant/models/routing-policies");
  },
  routeModel(payload: JsonObject): Promise<JsonObject> {
    return post<JsonObject>("/research-assistant/models/route", payload);
  },
  createTempMemory(payload: JsonObject): Promise<JsonObject> {
    return post<JsonObject>("/research-assistant/temp-memories", payload);
  },
  notifications(): Promise<AssistantPage<AssistantNotification>> {
    return unwrap<AssistantPage<AssistantNotification>>("/research-assistant/notifications");
  },
  externalSessions(params: { status?: string; limit?: number; offset?: number } = {}): Promise<AssistantPage<AssistantExternalSession>> {
    return unwrap<AssistantPage<AssistantExternalSession>>(appendQuery("/research-assistant/external-agent/sessions", { limit: 50, ...params }));
  },
  externalEvents(params: { session_id?: string; risk_level?: string; limit?: number; offset?: number } = {}): Promise<AssistantPage<AssistantExternalEvent>> {
    return unwrap<AssistantPage<AssistantExternalEvent>>(appendQuery("/research-assistant/external-agent/events", { limit: 50, ...params }));
  },
  traceEvents(params: { task_id?: string; component?: string; status?: string; limit?: number; offset?: number } = {}): Promise<AssistantPage<AssistantTraceEvent>> {
    return unwrap<AssistantPage<AssistantTraceEvent>>(appendQuery("/research-assistant/trace-events", { limit: 100, ...params }));
  },
  notificationSummary(): Promise<JsonObject> {
    return unwrap<JsonObject>("/research-assistant/notifications/summary");
  },
  reports(): Promise<AssistantPage<AssistantReport>> {
    return unwrap<AssistantPage<AssistantReport>>("/research-assistant/reports");
  },
  agenda(): Promise<AssistantPage<JsonObject>> {
    return unwrap<AssistantPage<JsonObject>>("/research-assistant/agenda");
  },
  validationDiscoverySummary(): Promise<AssistantValidationDiscoverySummary> {
    return unwrap<AssistantValidationDiscoverySummary>("/research-assistant/validation-discovery/summary");
  },
};
