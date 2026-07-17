export type CandidateSourceType =
  | "existing_snapshot_coefficients"
  | "configured_local_coefficients"
  | "qe_experiment_coefficients";

export type CandidateLifecycle = "research_only" | "retired" | "invalid";

export type EvaluationStatus =
  | "queued"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "timed_out";

export type BatchStatus =
  | "queued"
  | "running"
  | "cancel_requested"
  | "completed"
  | "partial_failed"
  | "failed"
  | "cancelled"
  | "timed_out";

export type EvidenceQuality = "complete" | "degraded" | "insufficient" | null;

export interface CandidateCoverage {
  start_date: string;
  end_date: string;
  date_count: number;
  sector_count_min: number;
  sector_count_max: number;
  stock_sector_map_count: number;
}

export interface CandidateManifest {
  schema_version: "hmm_candidate_manifest_v1";
  artifact_type: "hmm_sector_coefficients";
  source_type: CandidateSourceType;
  source_ref: Record<string, unknown>;
  artifact_uri: string;
  artifact_sha256: string;
  size_bytes: number;
  detected_format: string;
  coverage: CandidateCoverage;
  coefficient_stats: { min: number; max: number };
  algorithm_version: string;
}

export interface CandidateRecord {
  candidate_id: string;
  manifest_hash: string;
  display_name: string;
  description: string | null;
  source_type: CandidateSourceType;
  source_ref: Record<string, unknown>;
  artifact_manifest: CandidateManifest;
  algorithm_version: string;
  lifecycle_status: CandidateLifecycle;
  invalid_reason_code: string | null;
  invalid_context: Record<string, unknown> | null;
  created_by: string;
  row_version: number;
  created_at: string;
  updated_at: string;
  retired_at: string | null;
}

export interface CandidatePreview {
  candidate_id: string;
  manifest_hash: string;
  manifest: CandidateManifest;
}

export interface QEAssetEntry {
  relative_path: string;
  size_bytes: number;
  sha256: string | null;
  content_type: string | null;
  modified_at: string | null;
  source: string;
  trust_level: "trusted_computational_input" | "unverified_evidence";
  access_mode: "inspection_only" | "computational_input";
  schema_version: string | null;
  parser_contract: string | null;
  catalog_completeness: "complete" | "partial";
}

export interface QEAssetCatalog {
  schema_version: "hmm_qe_asset_catalog_v1";
  task_id: string;
  loop_name: string;
  catalog_completeness: "complete" | "partial";
  assets: QEAssetEntry[];
  warnings: string[];
}

export interface EvaluationSpecPayload {
  schema_version: "hmm_evaluation_spec_v1";
  base_loop_ref: string;
  window_start: string;
  window_end: string;
  as_of: {
    policy: "explicit" | "latest_common_completed";
    requested_date: string | null;
  };
  label_horizon_days: number;
  universe: { type: "prediction_artifact_all" };
  topk: number;
  date_coverage_policy: "batch_common_intersection_with_evidence" | "strict_full";
  missing_sector_policy: "neutral_with_evidence";
  market_forward_return: {
    mode: "required" | "disabled";
    horizon_trading_days: 10;
  };
  sort_policy: "score_desc_symbol_asc_v1";
  metric_version: "hmm_replacement_metrics_v1";
  recommendation_version: "hmm_recommendation_v1";
}

export interface BatchSummary {
  batch_id: string;
  status: BatchStatus;
  retry_of_batch_id: string | null;
  retry_generation: number;
  candidate_count: number;
  queued_count: number;
  running_count: number;
  succeeded_count: number;
  failed_count: number;
  cancelled_count: number;
  timed_out_count: number;
  heartbeat_at: string | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  updated_at: string;
  reason_code: string | null;
  error_context: Record<string, unknown> | null;
}

export interface BatchItem {
  batch_id: string;
  candidate_id: string;
  candidate_display_name: string;
  candidate_source_type: CandidateSourceType;
  candidate_lifecycle_status: CandidateLifecycle;
  eval_id: string;
  ordinal: number;
  item_status: string;
  evaluation_status: EvaluationStatus;
  label_horizon_days: number;
  as_of_date: string;
  window_start: string;
  window_end: string;
  trading_days_count: number;
  changed_day_count: number;
  label_comparable_day_count: number;
  db_comparable_day_count: number;
  replacement_count: number;
  primary_coverage_ratio: number | null;
  net_label_return: number | null;
  net_db_10d: number | null;
  positive_net_label_day_ratio: number | null;
  evidence_quality: EvidenceQuality;
  warnings_json: Array<Record<string, unknown>>;
  recommendation_score: number | null;
  evidence_confidence: number | null;
  recommendation_rank: number | null;
  is_top3: boolean;
  recommendation_components: Record<string, unknown> | null;
  reason_code: string | null;
  evaluation_reason_code: string | null;
  evaluation_error_message: string | null;
  evaluation_started_at: string | null;
  evaluation_completed_at: string | null;
}

export interface BatchDetail extends BatchSummary {
  recommendation_version: string;
  recommendation_spec: Record<string, unknown>;
  items: BatchItem[];
}

export interface EvaluationDetail {
  eval_id: string;
  candidate_id: string;
  candidate_display_name: string;
  candidate_source_type: CandidateSourceType;
  candidate_lifecycle_status: CandidateLifecycle;
  base_loop_ref: string;
  status: EvaluationStatus;
  run_generation: number;
  source_manifest: Record<string, unknown>;
  source_manifest_hash: string;
  candidate_manifest_hash: string;
  evaluation_spec: EvaluationSpecPayload;
  evaluation_spec_hash: string;
  evaluator_version: string;
  input_hash: string;
  as_of_date: string;
  window_start: string;
  window_end: string;
  label_horizon_days: number;
  universe_id: string;
  universe_hash: string;
  topk: number;
  trading_days_count: number;
  changed_day_count: number;
  label_comparable_day_count: number;
  db_comparable_day_count: number;
  replacement_count: number;
  primary_coverage_ratio: number | null;
  net_label_return: number | null;
  net_db_10d: number | null;
  positive_net_label_day_ratio: number | null;
  evidence_quality: EvidenceQuality;
  warnings_json: Array<Record<string, unknown>>;
  metrics_json: Record<string, unknown> | null;
  result_hash: string | null;
  error_code: string | null;
  reason_code: string | null;
  error_message: string | null;
  error_context: Record<string, unknown> | null;
  heartbeat_at: string | null;
  queued_at: string;
  started_at: string | null;
  completed_at: string | null;
  updated_at: string;
}

export type EvaluationSummary = Pick<
  EvaluationDetail,
  | "eval_id"
  | "candidate_id"
  | "candidate_display_name"
  | "candidate_source_type"
  | "base_loop_ref"
  | "status"
  | "run_generation"
  | "as_of_date"
  | "window_start"
  | "window_end"
  | "label_horizon_days"
  | "topk"
  | "trading_days_count"
  | "changed_day_count"
  | "primary_coverage_ratio"
  | "net_label_return"
  | "net_db_10d"
  | "positive_net_label_day_ratio"
  | "evidence_quality"
  | "reason_code"
  | "queued_at"
  | "started_at"
  | "completed_at"
  | "updated_at"
>;

export interface ApiEnvelope<T> {
  status: "ok";
  data: T;
  trace_id: string;
}

export interface ApiFailure {
  error_code: string;
  reason_code: string;
  message: string;
  context: Record<string, unknown>;
  trace_id?: string;
}

export interface CandidateSourcePayload {
  source_type: CandidateSourceType;
  snapshot_id?: string;
  artifact_name?: string;
  root_alias?: string;
  relative_path?: string;
  task_id?: string;
  loop_name?: string;
}

export const TERMINAL_BATCH_STATUSES = new Set<BatchStatus>([
  "completed",
  "partial_failed",
  "failed",
  "cancelled",
  "timed_out",
]);

export const TERMINAL_EVALUATION_STATUSES = new Set<EvaluationStatus>([
  "succeeded",
  "failed",
  "cancelled",
  "timed_out",
]);
