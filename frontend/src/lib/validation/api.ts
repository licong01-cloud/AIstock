export type JsonObject = Record<string, unknown>;

export type ValidationPage<T> = {
  items: T[];
  total: number;
  page: number;
  page_size: number;
  has_more: boolean;
};

export type ValidationEnvelope<T> = {
  status: string;
  data: T;
};

export type ValidationHealth = {
  status?: string;
  mode?: string;
  history?: {
    mode?: string;
    history_root?: string;
    exists?: boolean;
    run_count?: number;
    coverage_snapshot_count?: number;
    evidence_manifest_count?: number;
  };
  plan_catalog?: {
    catalog_path?: string;
    missing?: boolean;
    plan_count?: number;
  };
  quality?: {
    mode?: string;
    finding_count?: number;
    bug_count?: number;
    parse_errors?: JsonObject[];
  };
  runner?: ValidationRunnerHealth;
  production_8001_touched?: boolean;
};

export type ValidationPlan = JsonObject & {
  plan_key: string;
  title?: string;
  module?: string;
  level?: string;
  command_key?: string;
  nox_session?: string;
  enabled?: boolean;
  requires_backend?: boolean;
  requires_frontend?: boolean;
  allowed_backend_ports?: number[];
  allowed_frontend_ports?: number[];
  writes_database?: boolean;
  writes_artifacts?: boolean;
  writes_business_state?: boolean;
  runner_enabled?: boolean;
};

export type ValidationPlanCatalog = {
  catalog_path?: string;
  missing?: boolean;
  plans: ValidationPlan[];
};

export type ValidationPassScope = JsonObject & {
  level?: string;
  real_backend?: boolean;
  real_database?: boolean;
  real_node_api?: boolean;
  real_frontend_click?: boolean;
  writes_business_state?: boolean;
  positive_business_success?: boolean;
  negative_failfast_only?: boolean;
  mock_api_used?: boolean;
  production_8001_touched?: boolean;
};

export type ValidationBusinessAssertion = JsonObject & {
  can_user_complete_operation?: boolean;
  operation_name?: string;
  evidence?: JsonObject;
  unresolved_blockers?: string[];
};

export type ValidationCoverageSummary = JsonObject & {
  snapshot_id: string;
  schema_version?: string;
  module?: string;
  level?: string;
  title?: string;
  run_id?: string | null;
  generated_at?: string;
  git_commit?: string;
  status?: string;
  snapshot_path?: string;
  totals?: JsonObject;
  diff?: JsonObject;
  quality_gates?: JsonObject[];
  failed_gates?: JsonObject[];
};

export type ValidationEvidenceSummary = JsonObject & {
  manifest_id: string;
  schema_version?: string;
  module?: string;
  level?: string;
  title?: string;
  run_id?: string | null;
  generated_at?: string;
  git_commit?: string;
  manifest_path?: string;
  missing_count?: number;
  evidence_count?: number;
  missing?: unknown[];
};

export type ValidationRunSummary = JsonObject & {
  run_id: string;
  module?: string;
  module_slug?: string;
  level?: string;
  title?: string;
  status?: string;
  git_commit?: string | null;
  operator?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  markdown_path?: string;
  metadata_path?: string | null;
  metadata_missing?: boolean;
  metadata_parse_error?: string | null;
  source_type?: string;
  coverage?: JsonObject | null;
  coverage_snapshot_id?: string | null;
  coverage_missing?: boolean;
  evidence_manifest_id?: string | null;
  evidence_missing?: boolean;
  pass_scope?: ValidationPassScope | null;
  business_assertion?: ValidationBusinessAssertion | null;
  success_scope_recorded?: boolean;
  quality_gates?: JsonObject[];
  parse_error?: string | null;
};

export type ValidationRunDetail = ValidationRunSummary & {
  markdown_text?: string | null;
  metadata?: JsonObject | null;
  coverage_snapshot?: ValidationCoverageSummary | null;
  evidence_manifest?: ValidationEvidenceSummary | null;
};

export type ValidationCoverageDetail = {
  summary: ValidationCoverageSummary;
  snapshot: JsonObject;
};

export type ValidationEvidenceDetail = {
  summary: ValidationEvidenceSummary;
  manifest: JsonObject;
};

export type ValidationSummary = {
  history_root?: string;
  run_count?: number;
  coverage_snapshot_count?: number;
  evidence_manifest_count?: number;
  plan_count?: number;
  quality?: {
    finding_count?: number;
    bug_count?: number;
  };
  runner?: ValidationRunnerHealth;
  runs_by_status?: Record<string, number>;
  modules?: Array<JsonObject & { module?: string; run_count?: number; latest_run?: ValidationRunSummary }>;
  latest_runs?: ValidationRunSummary[];
  latest_coverage?: ValidationCoverageSummary | null;
};

export type ValidationRunnerHealth = JsonObject & {
  mode?: string;
  execution_root?: string;
  exists?: boolean;
  job_count?: number;
  jobs_by_status?: Record<string, number>;
  allowed_command_type?: string;
  arbitrary_shell_allowed?: boolean;
  production_8001_touched?: boolean;
};

export type ValidationExecutionJob = JsonObject & {
  schema_version?: string;
  job_id: string;
  status?: string;
  plan_key?: string;
  title?: string;
  module?: string;
  level?: string;
  command_key?: string;
  nox_session?: string;
  command?: string[];
  cwd?: string;
  requested_by?: string;
  requested_at?: string;
  started_at?: string | null;
  finished_at?: string | null;
  timeout_seconds?: number;
  return_code?: number | null;
  backend_port?: number | null;
  frontend_port?: number | null;
  writes_database?: boolean;
  writes_artifacts?: boolean;
  writes_business_state?: boolean;
  production_8001_touched?: boolean;
  arbitrary_shell_allowed?: boolean;
  log_path?: string;
  evidence_path?: string;
  error?: string | null;
};

export type ValidationExecutionQuery = {
  status?: string;
  page?: number;
  page_size?: number;
};

export type ValidationExecutionStartRequest = {
  plan_key: string;
  requested_by?: string;
  backend_port?: number;
  frontend_port?: number;
  timeout_seconds?: number;
  confirm_text?: string;
};

export type ValidationQualityFinding = JsonObject & {
  finding_id: string;
  source_type?: string;
  source_schema?: string;
  module?: string;
  severity?: string;
  status?: string;
  title?: string;
  description?: string;
  rule_id?: string | null;
  category?: string | null;
  file_path?: string | null;
  line?: number | null;
  fingerprint?: string;
  first_seen_at?: string | null;
  last_seen_at?: string | null;
  evidence_uri?: string | null;
  remediation?: string | null;
  baseline_policy?: string | null;
  lifecycle_status?: string | null;
  risk?: string | null;
  confidence?: string | null;
  allowed_write_scope?: string[];
  required_verification?: string[];
  linked_issue?: string | null;
  agent_context?: ValidationAgentContext;
};

export type ValidationBug = JsonObject & {
  bug_id: string;
  title?: string;
  description?: string | null;
  module?: string;
  severity?: string;
  risk_area?: string | null;
  status?: string;
  trigger_condition?: JsonObject;
  reproduce_command?: string | null;
  failing_run_id?: string | null;
  evidence_uris?: string[];
  fingerprint?: string;
  github_issue_number?: number | null;
  github_issue_url?: string | null;
  assigned_agent?: string | null;
  fix_branch?: string | null;
  fix_commit?: string | null;
  verification_run_id?: string | null;
  first_seen_at?: string | null;
  last_seen_at?: string | null;
  allowed_write_scope?: string[];
  suspected_modules?: string[];
  required_verification?: string[];
  closure_requirements?: string[];
  source_path?: string;
  agent_context?: ValidationAgentContext;
};

export type ValidationAgentContext = JsonObject & {
  schema_version?: string;
  context_type?: "bug" | "quality_finding" | string;
  bug_id?: string;
  finding_id?: string;
  problem_statement?: string | null;
  finding_source?: string;
  severity?: string;
  status?: string;
  reproduce_command?: string | null;
  evidence_uris?: Array<string | null | undefined>;
  allowed_write_scope?: string[];
  suspected_modules?: Array<string | null | undefined>;
  required_verification?: string[];
  closure_requirements?: string[];
  github_issue_url?: string | null;
  verification_run_id?: string | null;
};

export type ValidationFindingSummary = {
  finding_count?: number;
  by_source_type?: Record<string, number>;
  by_severity?: Record<string, number>;
  by_status?: Record<string, number>;
  by_module?: Record<string, number>;
  latest_findings?: ValidationQualityFinding[];
  parse_errors?: Array<JsonObject>;
};

export type ValidationBugSummary = {
  bug_count?: number;
  by_severity?: Record<string, number>;
  by_status?: Record<string, number>;
  by_module?: Record<string, number>;
  latest_bugs?: ValidationBug[];
  parse_errors?: Array<JsonObject>;
};

export type ValidationRunQuery = {
  module?: string;
  level?: string;
  status?: string;
  search?: string;
  include_markdown_only?: boolean;
  page?: number;
  page_size?: number;
};

export type ValidationListQuery = {
  module?: string;
  status?: string;
  page?: number;
  page_size?: number;
};

export type ValidationFindingQuery = ValidationListQuery & {
  source_type?: string;
  severity?: string;
  search?: string;
};

export type ValidationBugQuery = ValidationListQuery & {
  severity?: string;
  agent?: string;
  search?: string;
};

const API_BASE = (process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1").replace(/\/+$/, "");

export class ValidationApiError extends Error {
  status: number;
  raw: unknown;

  constructor(message: string, status: number, raw: unknown) {
    super(message);
    this.name = "ValidationApiError";
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
    if (isObject(payload.detail)) {
      const detail = payload.detail;
      if (typeof detail.message === "string") return detail.message;
      return JSON.stringify(detail);
    }
    if (typeof payload.message === "string") return payload.message;
    if (typeof payload.error === "string") return payload.error;
  }
  return `HTTP ${status}`;
}

function appendQuery(path: string, params: Record<string, string | number | boolean | undefined | null>): string {
  const qs = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") continue;
    qs.set(key, String(value));
  }
  const query = qs.toString();
  return query ? `${path}?${query}` : path;
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
  const payload = text ? JSON.parse(text) as unknown : {};
  if (!response.ok) throw new ValidationApiError(errorMessage(payload, response.status), response.status, payload);
  return payload as T;
}

async function unwrap<T>(path: string): Promise<T> {
  const response = await apiFetch<ValidationEnvelope<T>>(path);
  return response.data;
}

export const validationApi = {
  health(): Promise<ValidationHealth> {
    return unwrap<ValidationHealth>("/validation/health");
  },
  plans(): Promise<ValidationPlanCatalog> {
    return unwrap<ValidationPlanCatalog>("/validation/plans");
  },
  plan(planKey: string): Promise<ValidationPlan> {
    return unwrap<ValidationPlan>(`/validation/plans/${encodeURIComponent(planKey)}`);
  },
  runs(query: ValidationRunQuery = {}): Promise<ValidationPage<ValidationRunSummary>> {
    return unwrap<ValidationPage<ValidationRunSummary>>(appendQuery("/validation/runs", query));
  },
  run(runId: string): Promise<ValidationRunDetail> {
    return unwrap<ValidationRunDetail>(`/validation/runs/${encodeURIComponent(runId)}`);
  },
  coverage(query: ValidationListQuery = {}): Promise<ValidationPage<ValidationCoverageSummary>> {
    return unwrap<ValidationPage<ValidationCoverageSummary>>(appendQuery("/validation/coverage", query));
  },
  coverageDetail(snapshotId: string): Promise<ValidationCoverageDetail> {
    return unwrap<ValidationCoverageDetail>(`/validation/coverage/${encodeURIComponent(snapshotId)}`);
  },
  evidence(query: Omit<ValidationListQuery, "status"> = {}): Promise<ValidationPage<ValidationEvidenceSummary>> {
    return unwrap<ValidationPage<ValidationEvidenceSummary>>(appendQuery("/validation/evidence", query));
  },
  evidenceDetail(manifestId: string): Promise<ValidationEvidenceDetail> {
    return unwrap<ValidationEvidenceDetail>(`/validation/evidence/${encodeURIComponent(manifestId)}`);
  },
  summary(): Promise<ValidationSummary> {
    return unwrap<ValidationSummary>("/validation/summary");
  },
  findings(query: ValidationFindingQuery = {}): Promise<ValidationPage<ValidationQualityFinding>> {
    return unwrap<ValidationPage<ValidationQualityFinding>>(appendQuery("/validation/findings", query));
  },
  finding(findingId: string): Promise<ValidationQualityFinding> {
    return unwrap<ValidationQualityFinding>(`/validation/findings/${encodeURIComponent(findingId)}`);
  },
  findingSummary(): Promise<ValidationFindingSummary> {
    return unwrap<ValidationFindingSummary>("/validation/findings/summary");
  },
  bugs(query: ValidationBugQuery = {}): Promise<ValidationPage<ValidationBug>> {
    return unwrap<ValidationPage<ValidationBug>>(appendQuery("/validation/bugs", query));
  },
  bug(bugId: string): Promise<ValidationBug> {
    return unwrap<ValidationBug>(`/validation/bugs/${encodeURIComponent(bugId)}`);
  },
  bugSummary(): Promise<ValidationBugSummary> {
    return unwrap<ValidationBugSummary>("/validation/bugs/summary");
  },
  bugAgentContext(bugId: string): Promise<ValidationAgentContext> {
    return unwrap<ValidationAgentContext>(`/validation/bugs/${encodeURIComponent(bugId)}/agent-context`);
  },
  executions(query: ValidationExecutionQuery = {}): Promise<ValidationPage<ValidationExecutionJob>> {
    return unwrap<ValidationPage<ValidationExecutionJob>>(appendQuery("/validation/executions", query));
  },
  execution(jobId: string): Promise<ValidationExecutionJob> {
    return unwrap<ValidationExecutionJob>(`/validation/executions/${encodeURIComponent(jobId)}`);
  },
  startExecution(request: ValidationExecutionStartRequest): Promise<ValidationExecutionJob> {
    return apiFetch<ValidationEnvelope<ValidationExecutionJob>>("/validation/executions", {
      method: "POST",
      body: JSON.stringify(request),
    }).then((response) => response.data);
  },
};

export { API_BASE };
