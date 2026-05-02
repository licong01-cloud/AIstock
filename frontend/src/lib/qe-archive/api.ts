export type JsonObject = Record<string, unknown>;

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

export class QEArchiveApiError extends Error {
  status: number;
  raw: unknown;

  constructor(message: string, status: number, raw: unknown) {
    super(message);
    this.name = "QEArchiveApiError";
    this.status = status;
    this.raw = raw;
  }
}

export type ArchiveSummary = {
  run_count?: number;
  research_valid_counts?: Record<string, number>;
  pending_outbox_count?: number;
  outbox_status_counts?: Record<string, number>;
  archive_job_status_counts?: Record<string, number>;
  latest_archived_at?: string | null;
};

export type OutboxEvent = {
  event_id: string;
  event_type: string;
  source_system: string;
  source_id: string;
  source_sub_id?: string | null;
  status: string;
  retry_count?: number;
  next_retry_at?: string | null;
  locked_by?: string | null;
  locked_at?: string | null;
  error_message?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
  payload?: JsonObject;
};

export type ArchiveJob = {
  job_id: string;
  event_id?: string | null;
  run_id?: string | null;
  job_type: string;
  status: string;
  level?: string | null;
  started_at?: string | null;
  completed_at?: string | null;
  retry_count?: number;
  error_message?: string | null;
  stats?: JsonObject;
  created_at?: string | null;
  updated_at?: string | null;
};

export type BackfillRequest = {
  source: "experiment" | "loop" | "task" | "all";
  experiment_ids?: string[];
  task_ids?: string[];
  loop_ids?: string[];
  task_id?: string | null;
  loop_index?: number | null;
  status?: string;
  limit?: number;
  write?: boolean;
  confirm_write?: string;
  validate_after_write?: boolean;
  min_metrics?: number;
  min_curves?: number;
  min_factors?: number;
  require_account_summary?: boolean;
};

export type BackfillCandidate = {
  candidate_id: string;
  candidate_type: "evolution_task" | "single_experiment";
  source: "task" | "experiment";
  task_id?: string | null;
  experiment_id?: string | null;
  display_name?: string | null;
  description?: string | null;
  status?: string | null;
  experiment_type?: string | null;
  loop_count?: number;
  selected_run_count?: number;
  archived_run_count?: number;
  pending_run_count?: number;
  is_fully_archived?: boolean;
  node_id?: string | null;
  model_id?: string | null;
  model_catalog_id?: number | null;
  strategy_id?: string | null;
  factor_count?: number | null;
  label_horizon?: number | null;
  created_at?: string | null;
  started_at?: string | null;
  completed_at?: string | null;
  updated_at?: string | null;
  archive_action?: string | null;
};

export type BackfillCandidateReport = {
  status: string;
  include_archived: boolean;
  count: number;
  candidates: BackfillCandidate[];
};

export type BackfillResultItem = {
  run_id?: string;
  dry_run?: boolean;
  event_type?: string;
  source_system?: string | null;
  source_id?: string | null;
  source_sub_id?: string | null;
  stats?: JsonObject;
  quality?: RunQuality;
};

export type BackfillReport = {
  dry_run?: boolean;
  write_enabled?: boolean;
  source?: string;
  status?: string;
  processed_count?: number;
  results?: BackfillResultItem[];
  archive_summary?: ArchiveSummary | null;
};

export type WorkerRunReport = {
  claimed?: number;
  completed?: number;
  failed?: number;
  skipped_reason?: string | null;
};

export type RunQuality = {
  run_id: string;
  exists?: boolean;
  source_system?: string;
  run_type?: string;
  status?: string;
  research_valid?: boolean;
  invalid_reason?: string | null;
  freq?: string | null;
  label_horizon?: number | null;
  factor_count?: number | null;
  completed_at?: string | null;
  archived_at?: string | null;
  config_capture_complete?: boolean | null;
  missing_config_item_count?: number | null;
  reproducibility_level?: string | null;
  manifest_verification_status?: string | null;
  manifest_missing_item_count?: number | null;
  source_count?: number;
  data_context_count?: number;
  account_summary_count?: number;
  metric_count?: number;
  curve_count?: number;
  factor_count_rows?: number;
  symbol_summary_count?: number;
  trade_count?: number;
  execution_event_count?: number;
  artifact_count?: number;
  raw_payload_count?: number;
  priority_score_count?: number;
  failures?: string[];
  passed?: boolean;
};

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
  if (!response.ok) throw new QEArchiveApiError(errorMessage(payload, response.status), response.status, payload);
  return payload as T;
}

function body(payload: unknown): RequestInit {
  return { method: "POST", body: JSON.stringify(payload) };
}

export const qeArchiveApi = {
  async health(): Promise<ArchiveSummary> {
    const response = await apiFetch<{ status: string; data: ArchiveSummary }>("/qe-archive/health");
    return response.data;
  },
  async outbox(limit = 50, status?: string): Promise<OutboxEvent[]> {
    const qs = new URLSearchParams({ limit: String(limit) });
    if (status) qs.set("status", status);
    const response = await apiFetch<{ status: string; data: OutboxEvent[] }>(`/qe-archive/outbox?${qs.toString()}`);
    return response.data || [];
  },
  async jobs(limit = 50, status?: string): Promise<ArchiveJob[]> {
    const qs = new URLSearchParams({ limit: String(limit) });
    if (status) qs.set("status", status);
    const response = await apiFetch<{ status: string; data: ArchiveJob[] }>(`/qe-archive/jobs?${qs.toString()}`);
    return response.data || [];
  },
  async backfillCandidates(payload: { limit?: number; status?: string; include_archived?: boolean } = {}): Promise<BackfillCandidateReport> {
    const qs = new URLSearchParams({
      limit: String(payload.limit ?? 100),
      status: payload.status || "completed",
      include_archived: payload.include_archived ? "true" : "false",
    });
    const response = await apiFetch<{ status: string; data: BackfillCandidateReport }>(`/qe-archive/backfill-candidates?${qs.toString()}`);
    return response.data;
  },
  async backfill(payload: BackfillRequest): Promise<BackfillReport> {
    const response = await apiFetch<{ status: string; data: BackfillReport }>("/qe-archive/backfill", body(payload));
    return response.data;
  },
  async runWorkerOnce(payload: { limit: number; worker_id?: string; confirm_run: string }): Promise<WorkerRunReport> {
    const response = await apiFetch<{ status: string; data: WorkerRunReport }>("/qe-archive/worker/run-once", body(payload));
    return response.data;
  },
  async quality(runId: string): Promise<RunQuality> {
    const response = await apiFetch<{ status: string; data: RunQuality }>(`/qe-archive/runs/${encodeURIComponent(runId)}/quality`);
    return response.data;
  },
};

export { API_BASE };
