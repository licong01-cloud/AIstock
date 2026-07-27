import { apiFetch, type JsonObject } from "./selectionCenter";

export type AdvisoryPackageMode =
  | "single_package"
  | "fusion_pool"
  | "weighted_rank_fusion"
  | "union"
  | "intersection"
  | "sleeve_mode_future";

export type AdvisoryProgram = {
  program_id: string;
  program_name: string;
  status: string;
  target_count: number;
  package_mode: AdvisoryPackageMode;
  package_ids: string[];
  package_weights: Record<string, number>;
  fusion_method?: string | null;
  fusion_policy_sha256?: string | null;
  review_policy: JsonObject;
  review_policy_sha256: string;
  entry_price_basis: string;
  exit_price_basis: string;
  review_schedule: JsonObject;
  version: number;
  enabled_since?: string | null;
  last_review_status?: string | null;
  latest_review_trade_date?: string | null;
  latest_recommendation_list_version_id?: string | null;
  latest_recommendation_trade_date?: string | null;
  latest_recommendation_target_trade_date?: string | null;
  latest_recommendation_selection_as_of_trade_date?: string | null;
  latest_recommendation_generated_at?: string | null;
  latest_recommendation_version_status?: string | null;
  published_recommendation_target_trade_dates?: string[];
};

export type AdvisoryLeaderboardRow = AdvisoryProgram & {
  entered_episode_count?: number;
  active_count?: number;
  take_profit_count?: number;
  stop_loss_count?: number;
  win_rate?: number | null;
  avg_return_bps?: number | null;
  median_return_bps?: number | null;
  max_drawdown_bps?: number | null;
  avg_holding_days?: number | null;
  metric_status?: string | null;
  metric_evaluable_count?: number | null;
  open_mark_count?: number | null;
  missing_open_mark_count?: number | null;
  metric_mark_trade_date?: string | null;
};

export type AdvisoryEpisode = {
  episode_id: string;
  program_id: string;
  symbol: string;
  stock_name?: string | null;
  symbol_name?: string | null;
  status: string;
  signal_date: string;
  effective_entry_date: string;
  entry_price: number;
  entry_price_basis: string;
  entry_rank: number;
  entry_score?: number | null;
  current_rank?: number | null;
  current_score?: number | null;
  exit_signal_date?: string | null;
  effective_exit_date?: string | null;
  exit_price?: number | null;
  exit_price_basis?: string | null;
  exit_reason?: string | null;
  holding_trading_days?: number | null;
  return_bps?: number | null;
  is_win?: boolean | null;
  max_drawdown_bps?: number | null;
  max_runup_bps?: number | null;
  still_active_mark_price?: number | null;
  price_quality_status?: string | null;
};

export type AdvisoryReviewDecision = {
  symbol: string;
  stock_name?: string | null;
  symbol_name?: string | null;
  action: string;
  reason_code: string;
  review_status: string;
  trade_date: string;
  binding_version_id?: string | null;
  review_run_id?: string | null;
  list_version_id?: string | null;
  episode_id?: string | null;
  rank?: number | null;
  score?: number | null;
  return_bps?: number | null;
  evidence_json?: JsonObject;
  operation_advice_json?: JsonObject;
};

export type AdvisoryStrategyBindingVersion = {
  binding_version_id: string;
  program_id: string;
  program_version: number;
  package_mode: AdvisoryPackageMode;
  package_ids: string[];
  package_weights: Record<string, number>;
  fusion_method?: string | null;
  package_set_hash: string;
  fusion_policy_sha256?: string | null;
  runtime_config_json?: JsonObject | null;
  effective_from_trade_date?: string | null;
  effective_to_trade_date?: string | null;
  binding_interval_semantics?: "LEFT_CLOSED_RIGHT_OPEN" | string;
  binding_payload_hash?: string;
  activation_status: "DRAFT" | "ACTIVE" | "RETIRED" | string;
  activation_reason?: string | null;
  source_replay_run_id?: string | null;
  created_by?: string | null;
  created_at?: string | null;
  activated_at?: string | null;
};

export type AdvisoryRecommendationListVersion = {
  list_version_id: string;
  program_id: string;
  binding_version_id: string;
  review_run_id: string;
  trade_date: string;
  previous_list_version_id?: string | null;
  version_status: "PREVIEW" | "PUBLISHED" | "REPLAY" | string;
  target_count: number;
  active_count: number;
  entered_count: number;
  held_count: number;
  exited_count: number;
  waiting_count: number;
  changed_count: number;
  turnover_rate?: number | null;
  overlap_rate?: number | null;
  summary_json?: JsonObject;
  target_trade_date?: string | null;
  selection_as_of_trade_date?: string | null;
  created_at?: string | null;
};

export type AdvisoryRecommendationListItem = {
  list_item_id: string;
  list_version_id: string;
  program_id: string;
  binding_version_id: string;
  episode_id?: string | null;
  symbol: string;
  stock_name?: string | null;
  symbol_name?: string | null;
  item_state: string;
  action: string;
  previous_action?: string | null;
  rank?: number | null;
  score?: number | null;
  previous_rank?: number | null;
  previous_score?: number | null;
  entry_price?: number | null;
  exit_price?: number | null;
  price_basis?: string | null;
  effective_trade_date?: string | null;
  reason_code: string;
  operation_advice_json: JsonObject;
  component_scores_json?: JsonObject;
  evidence_json?: JsonObject;
  created_at?: string | null;
};

export type AdvisoryReviewResult = {
  program: AdvisoryProgram;
  trade_date: string;
  review_status: string;
  decisions: AdvisoryReviewDecision[];
  active_pool: AdvisoryEpisode[];
  metrics: JsonObject;
  preview: boolean;
  binding_version_id?: string | null;
  review_run_id?: string | null;
  list_version_id?: string | null;
  change_summary?: JsonObject;
  list_items?: AdvisoryRecommendationListItem[];
};

export type AdvisoryQualityReport = {
  report_type: string;
  sample_count: number;
  min_bucket_size: number;
  metrics: JsonObject;
  buckets: JsonObject[];
  warnings: string[];
};

export type AdvisoryTradingDayDefaults = {
  as_of_date?: string;
  latest_trading_day: string;
  next_trading_day?: string | null;
  data_ready_latest_date?: string | null;
  trading_days?: string[];
  replay_start_date?: string;
  replay_end_date?: string;
  trading_day_status?: JsonObject;
};

export type CreateAdvisoryProgramPayload = {
  program_name: string;
  package_mode: AdvisoryPackageMode;
  package_ids: string[];
  target_count?: number;
  package_weights?: Record<string, number>;
  review_policy?: JsonObject;
  entry_price_basis?: string;
  exit_price_basis?: string;
  review_schedule?: JsonObject;
  created_by?: string;
  status?: string;
};

export type AdvisoryReviewPayload = {
  trade_date: string;
  target_trade_date?: string;
  selection_as_of_trade_date?: string;
  data_source?: string;
  runtime_config?: JsonObject;
  candidates?: JsonObject[];
  market_by_symbol?: Record<string, JsonObject>;
};

export type AdvisoryBindingPayload = {
  package_mode: AdvisoryPackageMode;
  package_ids: string[];
  package_weights?: Record<string, number>;
  target_count?: number;
  runtime_config_json?: JsonObject;
};

export type AdvisoryBindingApplyPayload = {
  binding: AdvisoryBindingPayload;
  activation_reason: string;
  expected_program_version: number;
  expected_binding_version_id: string;
  source_replay_run_id?: string | null;
  effective_from_trade_date?: string | null;
  created_by?: string | null;
};

export type AdvisoryBindingApplyResponse = {
  program: AdvisoryProgram;
  binding: AdvisoryStrategyBindingVersion;
};

export type AdvisoryBindingDefaults = {
  program_id: string;
  expected_program_version: number;
  expected_binding_version_id: string;
  effective_from_trade_date: string;
  binding_interval_semantics: "LEFT_CLOSED_RIGHT_OPEN" | string;
};

export type AdvisoryReviewPreviewPayload = {
  items: JsonObject[];
  package_evidence_by_code: Record<string, Record<string, JsonObject>>;
  market_by_code: Record<string, JsonObject>;
  trade_date: string;
  exit_guard_policy: JsonObject;
  fusion_policy: JsonObject;
};

export type AdvisoryListVersionDetail = {
  list_version: AdvisoryRecommendationListVersion;
  items: AdvisoryRecommendationListItem[];
};

export type WatchlistCategory = {
  id: number;
  name: string;
  description?: string | null;
};

export type WatchlistBulkAddResponse = {
  added?: number;
  inserted?: number;
  skipped?: number;
  moved?: number;
};

export type HistoricalRangeRecord = Record<string, unknown>;
export type HistoricalRangePage = { limit: number; next_cursor: string | null; has_more: boolean };
export type HistoricalRangeEnvelope<T extends HistoricalRangeRecord> = {
  ok: true;
  data: T;
  page?: HistoricalRangePage;
};
export type HistoricalRangeOptions = {
  existing_programs: Array<{
    program_id: string;
    name: string;
    version: number;
    active_binding_version_id: string;
    package_id: string | null;
    target_count: number;
    review_policy_summary: JsonObject;
  }>;
  admitted_packages: Array<{
    package_id: string;
    name: string;
    alpha_mode: "single_alpha" | "multi_alpha";
    component_count: number;
    manifest_sha256: string;
    package_version: string;
  }>;
  outcome_catalog: {
    catalog_version: string;
    catalog_content_hash: string;
    default_horizons: number[];
    long_trend_horizons: number[];
    allowed_maturity_statuses: string[];
  };
};
export type HistoricalRangeProgramSpec =
  | { source_kind: "EXISTING_PROGRAM"; program_id: string; expected_program_version: number; expected_binding_version_id: string }
  | {
      source_kind: "RESEARCH_PROGRAM_SPEC";
      program_name: string;
      package_id: string;
      target_count: number;
      review_policy: JsonObject;
      runtime_config: JsonObject;
      entry_price_basis: string;
      exit_price_basis: string;
      style_profile_ref: string | null;
      style_profile_hash: string | null;
    };
export type HistoricalRangeCreatePayload = { program_specs: HistoricalRangeProgramSpec[]; start_trade_date: string; end_trade_date: string };
export type HistoricalRangeCommandPayload = { operation_idempotency_key: string; expected_row_version: number };
export type HistoricalRangeMutationData = {
  batch: HistoricalRangeRecord;
  operation: HistoricalRangeRecord;
  operation_id: string;
  exact_retry: boolean;
  dispatch_state: string;
  links: Record<string, string>;
};

export class AdvisoryApiError extends Error {
  readonly http_status: number | null;
  readonly error_code: string;
  readonly reason_code: string | null;
  readonly retryable: boolean;
  readonly context: JsonObject;
  readonly correlation_id: string | null;

  constructor(input: {
    message: string;
    http_status?: number | null;
    error_code: string;
    reason_code?: string | null;
    retryable?: boolean;
    context?: JsonObject;
    correlation_id?: string | null;
  }) {
    super(input.message);
    this.name = "AdvisoryApiError";
    this.http_status = input.http_status ?? null;
    this.error_code = input.error_code;
    this.reason_code = input.reason_code ?? null;
    this.retryable = input.retryable ?? false;
    this.context = input.context ?? {};
    this.correlation_id = input.correlation_id ?? null;
  }
}

const HISTORICAL_RANGE_API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

async function historicalRangeFetch<T extends HistoricalRangeRecord>(path: string, init?: RequestInit): Promise<HistoricalRangeEnvelope<T>> {
  let response: Response;
  try {
    response = await fetch(`${HISTORICAL_RANGE_API_BASE}${path}`, {
      ...init,
      headers: { "Content-Type": "application/json", ...(init?.headers || {}) },
    });
  } catch (cause) {
    throw new AdvisoryApiError({
      error_code: "ADVISORY_API_NETWORK_ERROR",
      message: cause instanceof Error ? cause.message : "历史验证 API 网络连接失败",
    });
  }
  const correlationHeader = response.headers.get("x-correlation-id");
  const text = await response.text();
  let payload: unknown;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch {
    throw new AdvisoryApiError({
      error_code: "ADVISORY_API_INVALID_RESPONSE",
      message: `历史验证 API 返回了无法解析的响应（HTTP ${response.status}）`,
      http_status: response.status,
      correlation_id: correlationHeader,
    });
  }
  if (!response.ok) {
    const detail = payload && typeof payload === "object" && "detail" in payload
      ? (payload as { detail?: unknown }).detail
      : null;
    if (detail && typeof detail === "object") {
      const typed = detail as JsonObject;
      throw new AdvisoryApiError({
        error_code: String(typed.error_code || "ADVISORY_HISTORICAL_RANGE_ERROR"),
        reason_code: typed.reason_code ? String(typed.reason_code) : null,
        message: String(typed.message || `HTTP ${response.status}`),
        retryable: typed.retryable === true,
        context: typed.context && typeof typed.context === "object" ? typed.context as JsonObject : {},
        correlation_id: typed.correlation_id ? String(typed.correlation_id) : correlationHeader,
        http_status: response.status,
      });
    }
    throw new AdvisoryApiError({
      error_code: "ADVISORY_API_INVALID_RESPONSE",
      message: `历史验证 API 错误响应不符合结构化合同（HTTP ${response.status}）`,
      http_status: response.status,
      correlation_id: correlationHeader,
    });
  }
  if (!payload || typeof payload !== "object" || (payload as JsonObject).ok !== true || typeof (payload as JsonObject).data !== "object") {
    throw new AdvisoryApiError({
      error_code: "ADVISORY_API_CONTRACT_ERROR",
      message: `历史验证 API 成功响应不符合合同（HTTP ${response.status}）`,
      http_status: response.status,
      correlation_id: correlationHeader,
    });
  }
  return payload as HistoricalRangeEnvelope<T>;
}

function requireHistoricalRangePage(envelope: HistoricalRangeEnvelope<HistoricalRangeRecord>, path: string): HistoricalRangePage {
  const page = envelope.page;
  if (!page || typeof page !== "object"
    || !Number.isInteger(page.limit) || page.limit < 1 || page.limit > 500
    || typeof page.has_more !== "boolean"
    || !(page.next_cursor === null || typeof page.next_cursor === "string")
    || (page.has_more && (!page.next_cursor || page.next_cursor.length === 0))
    || (!page.has_more && page.next_cursor !== null)) {
    throw new AdvisoryApiError({
      error_code: "ADVISORY_API_CONTRACT_ERROR",
      message: `历史验证 API 分页合同无效：${path}`,
    });
  }
  return page;
}

function r5Body(payload: unknown, headers?: HeadersInit): RequestInit {
  return { method: "POST", body: JSON.stringify(payload), headers };
}

export const historicalRangeApi = {
  async options(signal?: AbortSignal): Promise<HistoricalRangeOptions> {
    return (await historicalRangeFetch<{ existing_programs: HistoricalRangeOptions["existing_programs"]; admitted_packages: HistoricalRangeOptions["admitted_packages"]; outcome_catalog: HistoricalRangeOptions["outcome_catalog"] }>("/advisory/historical-range-options", { signal })).data;
  },
  async batches(cursor?: string | null, signal?: AbortSignal): Promise<{ rows: HistoricalRangeRecord[]; page: HistoricalRangePage }> {
    const query = cursor ? `?cursor=${encodeURIComponent(cursor)}` : "";
    const envelope = await historicalRangeFetch<{ batches: HistoricalRangeRecord[] }>(`/advisory/historical-range-batches${query}`, { signal });
    return { rows: envelope.data.batches, page: requireHistoricalRangePage(envelope, "batches") };
  },
  async create(payload: HistoricalRangeCreatePayload, idempotencyKey: string): Promise<HistoricalRangeMutationData> {
    return (await historicalRangeFetch<{ batch: HistoricalRangeRecord; operation: HistoricalRangeRecord; operation_id: string; exact_retry: boolean; dispatch_state: string; links: Record<string, string> }>(
      "/advisory/historical-range-batches",
      r5Body(payload, { "Idempotency-Key": idempotencyKey }),
    )).data;
  },
  async batch(batchId: string, signal?: AbortSignal): Promise<HistoricalRangeRecord> {
    return (await historicalRangeFetch<{ batch: HistoricalRangeRecord }>(`/advisory/historical-range-batches/${encodeURIComponent(batchId)}`, { signal })).data.batch;
  },
  async runs(batchId: string, cursor?: string | null, signal?: AbortSignal): Promise<{ rows: HistoricalRangeRecord[]; page: HistoricalRangePage }> {
    const query = cursor ? `?cursor=${encodeURIComponent(cursor)}` : "";
    const envelope = await historicalRangeFetch<{ runs: HistoricalRangeRecord[] }>(`/advisory/historical-range-batches/${encodeURIComponent(batchId)}/runs${query}`, { signal });
    return { rows: envelope.data.runs, page: requireHistoricalRangePage(envelope, "runs") };
  },
  async operations(batchId: string, cursor?: string | null, signal?: AbortSignal): Promise<{ rows: HistoricalRangeRecord[]; page: HistoricalRangePage }> {
    const query = cursor ? `?cursor=${encodeURIComponent(cursor)}` : "";
    const envelope = await historicalRangeFetch<{ operations: HistoricalRangeRecord[] }>(`/advisory/historical-range-batches/${encodeURIComponent(batchId)}/operations${query}`, { signal });
    return { rows: envelope.data.operations, page: requireHistoricalRangePage(envelope, "operations") };
  },
  async operation(operationId: string, signal?: AbortSignal): Promise<HistoricalRangeRecord> {
    return (await historicalRangeFetch<{ operation: HistoricalRangeRecord }>(`/advisory/historical-range-operations/${encodeURIComponent(operationId)}`, { signal })).data.operation;
  },
  async days(rangeRunId: string, cursor?: string | null, signal?: AbortSignal): Promise<{ rows: HistoricalRangeRecord[]; page: HistoricalRangePage }> {
    const query = cursor ? `?cursor=${encodeURIComponent(cursor)}` : "";
    const envelope = await historicalRangeFetch<{ days: HistoricalRangeRecord[] }>(`/advisory/historical-range-runs/${encodeURIComponent(rangeRunId)}/days${query}`, { signal });
    return { rows: envelope.data.days, page: requireHistoricalRangePage(envelope, "days") };
  },
  async day(rangeRunId: string, tradeDate: string, cursor?: string | null, signal?: AbortSignal): Promise<{ day: HistoricalRangeRecord; rows: HistoricalRangeRecord[]; page: HistoricalRangePage }> {
    const query = cursor ? `?candidate_cursor=${encodeURIComponent(cursor)}` : "";
    const envelope = await historicalRangeFetch<{ day: HistoricalRangeRecord; candidates: HistoricalRangeRecord[] }>(`/advisory/historical-range-runs/${encodeURIComponent(rangeRunId)}/days/${encodeURIComponent(tradeDate)}${query}`, { signal });
    return { day: envelope.data.day, rows: envelope.data.candidates, page: requireHistoricalRangePage(envelope, "candidates") };
  },
  async list(rangeRunId: string, tradeDate: string, cursor?: string | null, signal?: AbortSignal): Promise<{ list: HistoricalRangeRecord; rows: HistoricalRangeRecord[]; page: HistoricalRangePage }> {
    const query = cursor ? `?item_cursor=${encodeURIComponent(cursor)}` : "";
    const envelope = await historicalRangeFetch<{ list: HistoricalRangeRecord; items: HistoricalRangeRecord[] }>(`/advisory/historical-range-runs/${encodeURIComponent(rangeRunId)}/lists/${encodeURIComponent(tradeDate)}${query}`, { signal });
    return { list: envelope.data.list, rows: envelope.data.items, page: requireHistoricalRangePage(envelope, "list-items") };
  },
  async outcomes(rangeRunId: string, cursor?: string | null, signal?: AbortSignal): Promise<{ rows: HistoricalRangeRecord[]; page: HistoricalRangePage }> {
    const query = cursor ? `?cursor=${encodeURIComponent(cursor)}` : "";
    const envelope = await historicalRangeFetch<{ outcomes: HistoricalRangeRecord[] }>(`/advisory/historical-range-runs/${encodeURIComponent(rangeRunId)}/outcomes${query}`, { signal });
    return { rows: envelope.data.outcomes, page: requireHistoricalRangePage(envelope, "outcomes") };
  },
  async summaries(rangeRunId: string, cursor?: string | null, signal?: AbortSignal): Promise<{ rows: HistoricalRangeRecord[]; page: HistoricalRangePage }> {
    const query = cursor ? `?cursor=${encodeURIComponent(cursor)}` : "";
    const envelope = await historicalRangeFetch<{ summaries: HistoricalRangeRecord[] }>(`/advisory/historical-range-runs/${encodeURIComponent(rangeRunId)}/summaries${query}`, { signal });
    return { rows: envelope.data.summaries, page: requireHistoricalRangePage(envelope, "summaries") };
  },
  async command(batchId: string, action: "resume" | "cancel", payload: HistoricalRangeCommandPayload): Promise<HistoricalRangeMutationData> {
    return (await historicalRangeFetch<{ batch: HistoricalRangeRecord; operation: HistoricalRangeRecord; operation_id: string; exact_retry: boolean; dispatch_state: string; links: Record<string, string> }>(`/advisory/historical-range-batches/${encodeURIComponent(batchId)}/${action}`, r5Body(payload))).data;
  },
  async refreshOutcomes(batchId: string, payload: HistoricalRangeCommandPayload & { label_as_of_trade_date: string; range_run_ids: string[]; horizons: number[] }): Promise<HistoricalRangeMutationData> {
    return (await historicalRangeFetch<{ batch: HistoricalRangeRecord; operation: HistoricalRangeRecord; operation_id: string; exact_retry: boolean; dispatch_state: string; links: Record<string, string> }>(`/advisory/historical-range-batches/${encodeURIComponent(batchId)}/refresh-outcomes`, r5Body(payload))).data;
  },
  async buildBridge(batchId: string, payload: HistoricalRangeCommandPayload & { range_run_ids: string[]; requested_horizons: number[]; requested_maturity_statuses: Array<"COMPLETE" | "CENSORED" | "TERMINAL"> }): Promise<HistoricalRangeMutationData> {
    return (await historicalRangeFetch<{ batch: HistoricalRangeRecord; operation: HistoricalRangeRecord; operation_id: string; exact_retry: boolean; dispatch_state: string; links: Record<string, string> }>(`/advisory/historical-range-batches/${encodeURIComponent(batchId)}/build-dataset-bridge`, r5Body(payload))).data;
  },
};

function body(payload: unknown, method = "POST"): RequestInit {
  return { method, body: globalThis.JSON["stringify"](payload) };
}

export const advisoryApi = {
  async programs(includeArchived = false): Promise<AdvisoryProgram[]> {
    const data = await apiFetch<{ programs: AdvisoryProgram[] }>(`/advisory/programs?include_archived=${includeArchived}`);
    return data.programs || [];
  },
  async createProgram(payload: CreateAdvisoryProgramPayload): Promise<AdvisoryProgram> {
    const data = await apiFetch<{ program: AdvisoryProgram }>("/advisory/programs", body(payload));
    return data.program;
  },
  async updateProgram(programId: string, payload: Partial<CreateAdvisoryProgramPayload> & { status?: string }): Promise<AdvisoryProgram> {
    const data = await apiFetch<{ program: AdvisoryProgram }>(`/advisory/programs/${encodeURIComponent(programId)}`, body(payload, "PATCH"));
    return data.program;
  },
  async enable(programId: string): Promise<AdvisoryProgram> {
    const data = await apiFetch<{ program: AdvisoryProgram }>(`/advisory/programs/${encodeURIComponent(programId)}/enable`, body({}));
    return data.program;
  },
  async pause(programId: string): Promise<AdvisoryProgram> {
    const data = await apiFetch<{ program: AdvisoryProgram }>(`/advisory/programs/${encodeURIComponent(programId)}/pause`, body({}));
    return data.program;
  },
  async archive(programId: string): Promise<AdvisoryProgram> {
    const data = await apiFetch<{ program: AdvisoryProgram }>(`/advisory/programs/${encodeURIComponent(programId)}/archive`, body({}));
    return data.program;
  },
  async clone(programId: string, programName?: string): Promise<AdvisoryProgram> {
    const data = await apiFetch<{ program: AdvisoryProgram }>(
      `/advisory/programs/${encodeURIComponent(programId)}/clone`,
      body({ program_name: programName }),
    );
    return data.program;
  },
  async leaderboard(sortBy = "win_rate"): Promise<AdvisoryLeaderboardRow[]> {
    const data = await apiFetch<{ leaderboard: AdvisoryLeaderboardRow[] }>(`/advisory/leaderboard?sort_by=${encodeURIComponent(sortBy)}`);
    return data.leaderboard || [];
  },
  async tradingDayDefaults(lookbackTradingDays = 10): Promise<AdvisoryTradingDayDefaults> {
    return apiFetch<AdvisoryTradingDayDefaults>(`/paper-v2/trading-days/defaults?lookback_trading_days=${lookbackTradingDays}`);
  },
  async activePool(programId: string): Promise<AdvisoryEpisode[]> {
    const data = await apiFetch<{ active_pool: AdvisoryEpisode[] }>(`/advisory/programs/${encodeURIComponent(programId)}/active-pool`);
    return data.active_pool || [];
  },
  async bindings(programId: string): Promise<AdvisoryStrategyBindingVersion[]> {
    const data = await apiFetch<{ bindings: AdvisoryStrategyBindingVersion[] }>(`/advisory/programs/${encodeURIComponent(programId)}/bindings`);
    return data.bindings || [];
  },
  async activeBinding(programId: string): Promise<AdvisoryStrategyBindingVersion> {
    const data = await apiFetch<{ binding: AdvisoryStrategyBindingVersion }>(`/advisory/programs/${encodeURIComponent(programId)}/bindings/active`);
    return data.binding;
  },
  async bindingDefaults(programId: string): Promise<AdvisoryBindingDefaults> {
    return apiFetch<AdvisoryBindingDefaults>(`/advisory/programs/${encodeURIComponent(programId)}/bindings/defaults`);
  },
  async applyBinding(programId: string, payload: AdvisoryBindingApplyPayload): Promise<AdvisoryBindingApplyResponse> {
    return apiFetch<AdvisoryBindingApplyResponse>(`/advisory/programs/${encodeURIComponent(programId)}/bindings/apply`, body(payload));
  },
  async reviews(programId: string, limit = 20, offset = 0): Promise<{ reviews: AdvisoryReviewDecision[]; total_count: number; limit: number; offset: number }> {
    const data = await apiFetch<{ reviews: AdvisoryReviewDecision[]; total_count?: number; limit?: number; offset?: number }>(
      `/advisory/programs/${encodeURIComponent(programId)}/reviews?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`,
    );
    const reviews = data.reviews || [];
    return {
      reviews,
      total_count: data.total_count ?? reviews.length,
      limit: data.limit ?? limit,
      offset: data.offset ?? offset,
    };
  },
  async returns(programId: string): Promise<{ returns: AdvisoryEpisode[]; metrics: JsonObject }> {
    return apiFetch<{ returns: AdvisoryEpisode[]; metrics: JsonObject }>(`/advisory/programs/${encodeURIComponent(programId)}/returns`);
  },
  async listVersions(programId: string, limit = 20, offset = 0): Promise<AdvisoryRecommendationListVersion[]> {
    const data = await apiFetch<{ list_versions: AdvisoryRecommendationListVersion[] }>(
      `/advisory/programs/${encodeURIComponent(programId)}/list-versions?limit=${encodeURIComponent(String(limit))}&offset=${encodeURIComponent(String(offset))}`,
    );
    return data.list_versions || [];
  },
  async listVersionDetail(listVersionId: string): Promise<AdvisoryListVersionDetail> {
    return apiFetch<AdvisoryListVersionDetail>(`/advisory/list-versions/${encodeURIComponent(listVersionId)}`);
  },
  async previewReview(programId: string, payload: AdvisoryReviewPayload): Promise<AdvisoryReviewResult> {
    const data = await apiFetch<{ review: AdvisoryReviewResult }>(`/advisory/programs/${encodeURIComponent(programId)}/reviews/preview`, body(payload));
    return data.review;
  },
  async runReview(programId: string, payload: AdvisoryReviewPayload): Promise<AdvisoryReviewResult> {
    const data = await apiFetch<{ review: AdvisoryReviewResult }>(`/advisory/programs/${encodeURIComponent(programId)}/reviews/run`, body(payload));
    return data.review;
  },
  async replay(programId: string, payload: JsonObject): Promise<JsonObject> {
    const data = await apiFetch<{ replay: JsonObject }>(`/advisory/programs/${encodeURIComponent(programId)}/replay`, body(payload));
    return data.replay;
  },
  async qualityReport(records: JsonObject[], minBucketSize = 30): Promise<AdvisoryQualityReport> {
    const data = await apiFetch<{ report: AdvisoryQualityReport }>("/advisory/quality-report", body({ records, min_bucket_size: minBucketSize }));
    return data.report;
  },
  async reviewPreview(payload: AdvisoryReviewPreviewPayload): Promise<JsonObject[]> {
    const data = await apiFetch<{ records: JsonObject[] }>(
      "/selection-center/advisory/multi-package-review/preview",
      body(payload),
    );
    return data.records || [];
  },
  async watchlistCategories(): Promise<WatchlistCategory[]> {
    return apiFetch<WatchlistCategory[]>("/watchlist/categories");
  },
  async createWatchlistCategory(name: string, description?: string | null): Promise<WatchlistCategory> {
    const data = await apiFetch<{ id: number }>("/watchlist/categories", body({ name, description: description ?? null }));
    return { id: data.id, name, description: description ?? null };
  },
  async addWatchlistItems(codes: string[], categoryId: number): Promise<WatchlistBulkAddResponse> {
    return apiFetch<WatchlistBulkAddResponse>(
      "/watchlist/items/bulk-add",
      body({ codes, category_id: categoryId, on_conflict: "ignore" }),
    );
  },
  async tdxAvailable(): Promise<boolean> {
    const data = await apiFetch<{ available: boolean }>("/tdx-blocks/available");
    return data.available;
  },
  async tdxSyncFromCategory(categoryName: string): Promise<{ name: string; display_name: string; count: number; codes: string[] }> {
    return apiFetch(
      "/tdx-blocks/sync-from-category",
      body({ category_name: categoryName }),
    );
  },
};
