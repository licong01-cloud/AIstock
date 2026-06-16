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
  runtime_config_json?: JsonObject;
  effective_from_trade_date?: string | null;
  effective_to_trade_date?: string | null;
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
  source_replay_run_id?: string | null;
  effective_from_trade_date?: string | null;
  created_by?: string | null;
};

export type AdvisoryBindingApplyResponse = {
  program: AdvisoryProgram;
  binding: AdvisoryStrategyBindingVersion;
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
