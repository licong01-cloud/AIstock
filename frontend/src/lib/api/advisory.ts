import { apiFetch, type JsonObject } from "./selectionCenter";

export type AdvisoryProgram = {
  program_id: string;
  program_name: string;
  status: string;
  target_count: number;
  package_mode: "single_package" | "fusion_pool" | "sleeve_mode_future";
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
};

export type AdvisoryEpisode = {
  episode_id: string;
  program_id: string;
  symbol: string;
  status: string;
  signal_date: string;
  effective_entry_date: string;
  entry_price: number;
  entry_price_basis: string;
  entry_rank: number;
  current_rank?: number | null;
  exit_signal_date?: string | null;
  exit_price?: number | null;
  exit_reason?: string | null;
  return_bps?: number | null;
  is_win?: boolean | null;
  max_drawdown_bps?: number | null;
  max_runup_bps?: number | null;
};

export type AdvisoryReviewDecision = {
  symbol: string;
  action: string;
  reason_code: string;
  review_status: string;
  trade_date: string;
  episode_id?: string | null;
  rank?: number | null;
  score?: number | null;
  return_bps?: number | null;
  evidence_json?: JsonObject;
};

export type AdvisoryReviewResult = {
  program: AdvisoryProgram;
  trade_date: string;
  review_status: string;
  decisions: AdvisoryReviewDecision[];
  active_pool: AdvisoryEpisode[];
  metrics: JsonObject;
  preview: boolean;
};

export type AdvisoryQualityReport = {
  report_type: string;
  sample_count: number;
  min_bucket_size: number;
  metrics: JsonObject;
  buckets: JsonObject[];
  warnings: string[];
};

export type CreateAdvisoryProgramPayload = {
  program_name: string;
  package_mode: "single_package" | "fusion_pool";
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
  selection_run_id?: string;
  data_source?: string;
  runtime_config?: JsonObject;
  candidates?: JsonObject[];
  market_by_symbol?: Record<string, JsonObject>;
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
  async activePool(programId: string): Promise<AdvisoryEpisode[]> {
    const data = await apiFetch<{ active_pool: AdvisoryEpisode[] }>(`/advisory/programs/${encodeURIComponent(programId)}/active-pool`);
    return data.active_pool || [];
  },
  async reviews(programId: string): Promise<AdvisoryReviewDecision[]> {
    const data = await apiFetch<{ reviews: AdvisoryReviewDecision[] }>(`/advisory/programs/${encodeURIComponent(programId)}/reviews`);
    return data.reviews || [];
  },
  async returns(programId: string): Promise<{ returns: AdvisoryEpisode[]; metrics: JsonObject }> {
    return apiFetch<{ returns: AdvisoryEpisode[]; metrics: JsonObject }>(`/advisory/programs/${encodeURIComponent(programId)}/returns`);
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
};
