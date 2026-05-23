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
  risk_level?: string;
  requires_approval?: boolean;
  input_schema_json?: JsonObject;
  preflight_schema_json?: JsonObject;
  required_confirmations?: string[];
  status?: string;
};

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
  trace?: JsonObject;
  cards?: JsonObject;
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
  mcpTools(params: { server_key?: string; risk_level?: string; search?: string; limit?: number; offset?: number } = {}): Promise<AssistantPage<AssistantMcpTool>> {
    return unwrap<AssistantPage<AssistantMcpTool>>(appendQuery("/research-assistant/mcp/tools", { limit: 100, ...params }));
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
