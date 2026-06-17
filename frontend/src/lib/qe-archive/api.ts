export type JsonObject = Record<string, unknown>;

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";
const QE_ARCHIVE_WRITE_CONFIRM_TEXT = "QE_ARCHIVE_WRITE";
const QE_ARCHIVE_BACKFILL_CONFIRM_TEXT = "QE_ARCHIVE_BACKFILL";

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
  loop_indices?: number[];
  status?: string;
  limit?: number;
  include_archived?: boolean;
  write?: boolean;
  confirm_write?: string;
  validate_after_write?: boolean;
  min_metrics?: number;
  min_curves?: number;
  min_factors?: number;
  require_account_summary?: boolean;
};

export type BackfillRunRequest = Omit<BackfillRequest, "source" | "write" | "confirm_write"> & {
  source_mode:
    | "completed_single_experiments"
    | "completed_custom_evo_loops"
    | "all_completed_qe_sources"
    | "specific_ids";
  confirm_backfill?: string;
  requested_by?: string;
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
  recommended_run_count?: number;
  manual_only_run_count?: number;
  not_recommended_run_count?: number;
  is_fully_archived?: boolean;
  loops?: BackfillCandidateLoop[];
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

export type BackfillCandidateLoop = {
  task_id?: string | null;
  loop_id?: string | null;
  loop_index?: number | null;
  status?: string | null;
  action_type?: string | null;
  experiment_id?: string | null;
  is_sota?: boolean;
  archive_status?: string;
  eligible?: boolean;
  recommended?: boolean;
  reason?: string | null;
  run_ids?: string[];
  run_count?: number;
  created_at?: string | null;
  updated_at?: string | null;
  IC?: number | null;
  ic?: number | null;
  Rank_IC?: number | null;
  rank_ic?: number | null;
  annualized_return?: number | null;
  max_drawdown?: number | null;
  information_ratio?: number | null;
};

export type ArchiveSourceItemStatus = {
  archive_status: string;
  run_ids?: string[];
  run_count?: number;
  eligible?: boolean;
  recommended?: boolean;
  reason?: string;
};

export type ArchiveTaskStatus = {
  archive_status: string;
  loop_count?: number;
  archived_loop_count?: number;
  eligible_loop_count?: number;
  pending_loop_count?: number;
  recommended_loop_count?: number;
  manual_only_loop_count?: number;
  not_recommended_loop_count?: number;
  run_ids?: string[];
};

export type ArchiveSourceStatus = {
  experiments?: Record<string, ArchiveSourceItemStatus>;
  tasks?: Record<string, ArchiveTaskStatus>;
  loops?: Record<string, ArchiveSourceItemStatus>;
  include_recommendation?: boolean;
};

export type ArchiveSourceStatusRequest = {
  experiment_ids?: string[];
  task_ids?: string[];
  loop_ids?: string[];
  include_recommendation?: boolean;
};

export type ArchivedRunListItem = {
  run_id: string;
  source_system?: string | null;
  run_type?: string | null;
  status?: string | null;
  research_valid?: boolean | null;
  invalid_reason?: string | null;
  logical_experiment_id?: string | null;
  experiment_id?: string | null;
  task_id?: string | null;
  loop_id?: string | null;
  loop_index?: number | null;
  node_id?: string | null;
  model_type?: string | null;
  model_catalog_id?: number | null;
  factor_count?: number | null;
  freq?: string | null;
  label_horizon?: number | null;
  completed_at?: string | null;
  archived_at?: string | null;
  metric_count?: number;
  curve_count?: number;
  factor_count_rows?: number;
  symbol_summary_count?: number;
  trade_count?: number;
};

export type BackfillCandidateReport = {
  status: string;
  include_archived: boolean;
  page?: number;
  page_size?: number;
  offset?: number;
  count: number;
  has_more?: boolean;
  candidates: BackfillCandidate[];
};

export type BackfillResultItem = {
  run_id?: string;
  dry_run?: boolean;
  event_type?: string;
  source_system?: string | null;
  source_id?: string | null;
  source_sub_id?: string | null;
  archive_policy?: string | null;
  archive_policy_source?: string | null;
  reason?: string | null;
  will_archive?: boolean;
  skipped_reason?: string | null;
  skip_id?: string | null;
  loop_index?: number | null;
  error?: string | null;
  stats?: JsonObject;
  quality?: RunQuality;
};

export type BackfillReport = {
  dry_run?: boolean;
  write_enabled?: boolean;
  backfill_run_id?: string;
  source_mode?: string;
  source?: string;
  status?: string;
  candidate_count?: number;
  processed_count?: number;
  ingested_count?: number;
  skipped_count?: number;
  failed_count?: number;
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

export type RunLeaderboardItem = {
  run_id: string;
  task_id?: string | null;
  loop_index?: number | null;
  experiment_id?: string | null;
  model_type?: string | null;
  factor_count?: number | null;
  label_horizon?: number | null;
  cagr?: number | null;
  max_drawdown?: number | null;
  calmar?: number | null;
  sharpe?: number | null;
  information_ratio?: number | null;
  ic?: number | null;
  icir?: number | null;
  rank_ic?: number | null;
  rank_icir?: number | null;
  topk_return_20?: number | null;
  topk_return_50?: number | null;
  topk_hit_rate_20?: number | null;
  topk_hit_rate_50?: number | null;
  topk_decay?: number | null;
  within_portfolio_rankic?: number | null;
  topk_dispersion_20?: number | null;
  topk_dispersion_50?: number | null;
  topk_quality_status?: string | null;
  topk_source?: string | null;
  topk_date_count?: number | null;
  topk_joined_observation_count?: number | null;
  completed_at?: string | null;
};

export type TopKQualityItem = RunLeaderboardItem & {
  topk_error?: string | null;
  topk_label_source?: string | null;
  topk_rank_direction?: string | null;
  topk_pred_observation_count?: number | null;
  topk_label_observation_count?: number | null;
  topk_rankic_date_count?: number | null;
  topk_observation_count_20?: number | null;
  topk_observation_count_50?: number | null;
  selected_topk_return?: number | null;
  selected_topk_hit_rate?: number | null;
  selected_topk_dispersion?: number | null;
};

export type PromotionCandidateItem = {
  factor_set_hash: string;
  model_type?: string | null;
  label_horizon?: number | null;
  undertrain_mode?: string | null;
  topk?: string | null;
  run_count?: number | null;
  distinct_seed_count?: number | null;
  cagr_mean?: number | null;
  max_drawdown_mean?: number | null;
  cagr_cv?: number | null;
  calmar?: number | null;
  calmar_mean?: number | null;
  icir_mean?: number | null;
  rank_icir_mean?: number | null;
  topk_return_20_mean?: number | null;
  topk_hit_rate_20_mean?: number | null;
  within_portfolio_rankic_mean?: number | null;
  topk_dispersion_20_mean?: number | null;
  topk_decay_mean?: number | null;
  topk_return_20_sample_count?: number | null;
  topk_return_20_present?: boolean | null;
  topk_soft_gate_status?: string | null;
  cagr_gate_passes?: boolean | null;
  max_drawdown_gate_passes?: boolean | null;
  cagr_cv_gate_passes?: boolean | null;
  overfit_gate_passes?: boolean | null;
  cagr_gate_threshold?: number | null;
  max_drawdown_gate_threshold?: number | null;
  cagr_cv_gate_threshold?: number | null;
  passes_gate?: boolean | null;
  latest_completed_at?: string | null;
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

function selectionToBackfillRun(payload: Omit<BackfillRequest, "write" | "confirm_write">): BackfillRunRequest {
  return {
    source_mode: "specific_ids",
    experiment_ids: payload.experiment_ids || [],
    task_ids: payload.task_ids || [],
    loop_ids: payload.loop_ids || [],
    task_id: payload.task_id,
    loop_index: payload.loop_index,
    loop_indices: payload.loop_indices || [],
    status: payload.status || "completed",
    limit: payload.limit,
    include_archived: payload.include_archived,
    validate_after_write: payload.validate_after_write,
    min_metrics: payload.min_metrics,
    min_curves: payload.min_curves,
    min_factors: payload.min_factors,
    require_account_summary: payload.require_account_summary,
    requested_by: "qe_archive_ui",
  };
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
  async runs(payload: { limit?: number; status?: string; run_type?: string; search?: string } = {}): Promise<ArchivedRunListItem[]> {
    const qs = new URLSearchParams({ limit: String(payload.limit ?? 100) });
    if (payload.status) qs.set("status", payload.status);
    if (payload.run_type) qs.set("run_type", payload.run_type);
    if (payload.search) qs.set("search", payload.search);
    const response = await apiFetch<{ status: string; data: ArchivedRunListItem[] }>(`/qe-archive/runs?${qs.toString()}`);
    return response.data || [];
  },
  async backfillCandidates(payload: { limit?: number; page?: number; page_size?: number; status?: string; include_archived?: boolean } = {}): Promise<BackfillCandidateReport> {
    const qs = new URLSearchParams({
      page: String(payload.page ?? 1),
      page_size: String(payload.page_size ?? payload.limit ?? 20),
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
  async previewSelection(payload: Omit<BackfillRequest, "write" | "confirm_write">): Promise<BackfillReport> {
    const response = await apiFetch<{ status: string; data: BackfillReport }>(
      "/qe-archive/backfill/preview",
      body(selectionToBackfillRun(payload)),
    );
    return response.data;
  },
  async executeSelection(payload: Omit<BackfillRequest, "write"> & { confirm_write: string }): Promise<BackfillReport> {
    const { confirm_write: _confirmWrite, ...selectionPayload } = payload;
    if (_confirmWrite !== QE_ARCHIVE_WRITE_CONFIRM_TEXT) {
      throw new Error(`write mode requires confirm_write=${QE_ARCHIVE_WRITE_CONFIRM_TEXT}`);
    }
    const response = await apiFetch<{ status: string; data: BackfillReport }>(
      "/qe-archive/backfill/execute",
      body({
        ...selectionToBackfillRun(selectionPayload),
        confirm_backfill: QE_ARCHIVE_BACKFILL_CONFIRM_TEXT,
      }),
    );
    return response.data;
  },
  async sourceStatus(payload: ArchiveSourceStatusRequest): Promise<ArchiveSourceStatus> {
    const response = await apiFetch<{ status: string; data: ArchiveSourceStatus }>("/qe-archive/source-status", body(payload));
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
  async runLeaderboard(payload: { limit?: number; model_type?: string; min_icir?: number; min_ir?: number; order_by?: string } = {}): Promise<RunLeaderboardItem[]> {
    const qs = new URLSearchParams({ limit: String(payload.limit ?? 20), order_by: payload.order_by || "calmar" });
    if (payload.model_type) qs.set("model_type", payload.model_type);
    if (payload.min_icir !== undefined) qs.set("min_icir", String(payload.min_icir));
    if (payload.min_ir !== undefined) qs.set("min_ir", String(payload.min_ir));
    const response = await apiFetch<{ status: string; data: RunLeaderboardItem[] }>(`/qe-archive/analytics/run-leaderboard?${qs.toString()}`);
    return response.data || [];
  },
  async topkQuality(payload: { run_id?: string; task_id?: string; k?: number; limit?: number } = {}): Promise<TopKQualityItem[]> {
    const qs = new URLSearchParams({ limit: String(payload.limit ?? 20) });
    if (payload.run_id) qs.set("run_id", payload.run_id);
    if (payload.task_id) qs.set("task_id", payload.task_id);
    if (payload.k !== undefined) qs.set("k", String(payload.k));
    const response = await apiFetch<{ status: string; data: TopKQualityItem[] }>(`/qe-archive/analytics/topk-quality?${qs.toString()}`);
    return response.data || [];
  },
  async promotionCandidates(payload: { limit?: number; model_type?: string; min_seed_count?: number; order_by?: string } = {}): Promise<PromotionCandidateItem[]> {
    const qs = new URLSearchParams({
      limit: String(payload.limit ?? 20),
      min_seed_count: String(payload.min_seed_count ?? 5),
      order_by: payload.order_by || "calmar",
    });
    if (payload.model_type) qs.set("model_type", payload.model_type);
    const response = await apiFetch<{ status: string; data: PromotionCandidateItem[] }>(`/qe-archive/analytics/promotion-candidates?${qs.toString()}`);
    return response.data || [];
  },
};

export { API_BASE };
