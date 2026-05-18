export type JsonObject = Record<string, unknown>;

const API_BASE = (process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1").replace(/\/+$/, "");

export type ResearchPipelineEnvelope<T> = {
  status: string;
  data: T;
};

export type ResearchPipelineHealth = JsonObject & {
  service?: string;
  status?: string;
};

export type ResearchPipelineTypeConfig = JsonObject & {
  display_name?: string;
  stages?: string[];
  default_criteria?: JsonObject;
};

export type ResearchExperimentStatus =
  | "draft"
  | "running"
  | "stage_failed"
  | "validated"
  | "rejected"
  | "blocked"
  | "promotion_requested"
  | "promoted"
  | string;

export type ResearchStageStatus = "queued" | "running" | "passed" | "failed" | "cancelled" | "timeout" | string;

export type ResearchExperimentSummary = JsonObject & {
  experiment_id: string;
  pipeline_type: string;
  title: string;
  description?: string | null;
  status: ResearchExperimentStatus;
  criteria_json?: JsonObject;
  baseline_ref_json?: JsonObject;
  issue_url?: string | null;
  blocked_reason?: string | null;
  metadata_json?: JsonObject;
  created_by?: string;
  validated_at?: string | null;
  promotion_requested_at?: string | null;
  promoted_at?: string | null;
  rejected_at?: string | null;
  blocked_at?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type ResearchStagePlan = JsonObject & {
  stage_id: string;
  experiment_id: string;
  stage_name: string;
  stage_order: number;
  status: ResearchStageStatus;
  planned_config_json?: JsonObject;
  latest_attempt_no?: number | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type ResearchStageAttempt = JsonObject & {
  stage_attempt_id: string;
  stage_id: string;
  experiment_id: string;
  stage_name: string;
  attempt_no: number;
  status: ResearchStageStatus;
  input_json?: JsonObject;
  result_json?: JsonObject;
  error_message?: string | null;
  started_at?: string | null;
  completed_at?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type ResearchExternalRunLink = JsonObject & {
  link_id: string;
  experiment_id: string;
  stage_attempt_id?: string | null;
  run_type: string;
  external_id: string;
  external_url?: string | null;
  status?: string | null;
  metadata_json?: JsonObject;
  created_at?: string | null;
};

export type ResearchArtifactRef = JsonObject & {
  artifact_ref_id: string;
  experiment_id: string;
  stage_attempt_id?: string | null;
  domain_type: string;
  domain_id?: string | null;
  artifact_uri?: string | null;
  artifact_sha256?: string | null;
  status?: string;
  metadata_json?: JsonObject;
  created_at?: string | null;
  updated_at?: string | null;
};

export type ResearchComparison = JsonObject & {
  comparison_id: string;
  experiment_id: string;
  stage_attempt_id?: string | null;
  baseline_ref_json?: JsonObject;
  candidate_ref_json?: JsonObject;
  metrics_json?: JsonObject;
  criteria_json?: JsonObject;
  verdict: string;
  reason_md?: string | null;
  created_by?: string;
  created_at?: string | null;
};

export type ResearchPipelineEvent = JsonObject & {
  event_id: string;
  experiment_id?: string | null;
  stage_attempt_id?: string | null;
  event_type: string;
  severity?: string;
  message: string;
  payload_json?: JsonObject;
  created_by?: string;
  created_at?: string | null;
};

export type ResearchBacktestRecord = JsonObject & {
  record_id: string;
  experiment_id: string;
  stage_attempt_id?: string | null;
  pipeline_type?: string;
  research_domain?: string;
  source_type: string;
  source_task_id: string;
  source_loop_id: string;
  source_loop_index?: number | null;
  source_experiment_id?: string | null;
  source_created_at?: string | null;
  record_version?: string;
  record_key_sha256?: string;
  non_hmm_config_sig?: string | null;
  hmm_config_sig?: string | null;
  strict_family_sig?: string | null;
  archive_family_sig?: string | null;
  dedup_status?: string;
  qe_archive_eligible?: boolean;
  qe_archive_representative?: boolean;
  rejection_reason?: string | null;
  ann?: number | null;
  mdd?: number | null;
  ir?: number | null;
  ic?: number | null;
  rank_ic?: number | null;
  sharpe?: number | null;
  turnover?: number | null;
  metrics_json?: JsonObject;
  hmm_config_summary_json?: JsonObject;
  config_summary_json?: JsonObject;
  source_payload_json?: JsonObject;
  recorded_by?: string;
  created_at?: string | null;
  updated_at?: string | null;
};

export type ResearchBackfillRun = JsonObject & {
  backfill_run_id: string;
  experiment_id: string;
  backfill_type?: string;
  status: string;
  dry_run?: boolean;
  source_scope_json?: JsonObject;
  source_fingerprint_json?: JsonObject;
  counts_json?: JsonObject;
  stage_attempt_id?: string | null;
  error_message?: string | null;
  created_by?: string;
  started_at?: string | null;
  completed_at?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type ResearchExperimentDetail = ResearchExperimentSummary & {
  stages?: ResearchStagePlan[];
  attempts?: ResearchStageAttempt[];
  artifact_refs?: ResearchArtifactRef[];
  external_run_links?: ResearchExternalRunLink[];
  comparisons?: ResearchComparison[];
  events?: ResearchPipelineEvent[];
};

export type ResearchExperimentQuery = {
  status?: string;
  pipeline_type?: string;
  search?: string;
  limit?: number;
  offset?: number;
};

export type ResearchArtifactRefQuery = {
  domain_type?: string;
  status?: string;
  limit?: number;
};

export type ResearchBacktestRecordQuery = {
  research_domain?: string;
  dedup_status?: string;
  qe_archive_representative?: boolean;
  source_task_id?: string;
  hmm_config_sig?: string;
  non_hmm_config_sig?: string;
  limit?: number;
  offset?: number;
};

export class ResearchPipelineApiError extends Error {
  status: number;
  raw: unknown;

  constructor(message: string, status: number, raw: unknown) {
    super(message);
    this.name = "ResearchPipelineApiError";
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
  if (!response.ok) throw new ResearchPipelineApiError(errorMessage(payload, response.status), response.status, payload);
  return payload as T;
}

async function unwrap<T>(path: string): Promise<T> {
  const response = await apiFetch<ResearchPipelineEnvelope<T>>(path);
  return response.data;
}

export const researchPipelineApi = {
  health(): Promise<ResearchPipelineHealth> {
    return unwrap<ResearchPipelineHealth>("/research-pipeline/health");
  },
  pipelineTypes(): Promise<Record<string, ResearchPipelineTypeConfig>> {
    return unwrap<Record<string, ResearchPipelineTypeConfig>>("/research-pipeline/pipeline-types");
  },
  experiments(query: ResearchExperimentQuery = {}): Promise<ResearchExperimentSummary[]> {
    return unwrap<ResearchExperimentSummary[]>(appendQuery("/research-pipeline/experiments", { limit: 50, ...query }));
  },
  experiment(experimentId: string): Promise<ResearchExperimentDetail> {
    return unwrap<ResearchExperimentDetail>(`/research-pipeline/experiments/${encodeURIComponent(experimentId)}`);
  },
  artifactRefs(experimentId: string, query: ResearchArtifactRefQuery = {}): Promise<ResearchArtifactRef[]> {
    return unwrap<ResearchArtifactRef[]>(appendQuery(`/research-pipeline/experiments/${encodeURIComponent(experimentId)}/artifact-refs`, { limit: 100, ...query }));
  },
  backtestRecords(experimentId: string, query: ResearchBacktestRecordQuery = {}): Promise<ResearchBacktestRecord[]> {
    return unwrap<ResearchBacktestRecord[]>(appendQuery(`/research-pipeline/experiments/${encodeURIComponent(experimentId)}/backtest-records`, { research_domain: "hmm", limit: 100, ...query }));
  },
  backfillRuns(experimentId: string, limit = 50): Promise<ResearchBackfillRun[]> {
    return unwrap<ResearchBackfillRun[]>(appendQuery(`/research-pipeline/experiments/${encodeURIComponent(experimentId)}/backfill-runs`, { limit }));
  },
  backfillRun(backfillRunId: string): Promise<ResearchBackfillRun> {
    return unwrap<ResearchBackfillRun>(`/research-pipeline/backfill-runs/${encodeURIComponent(backfillRunId)}`);
  },
};

export { API_BASE };
