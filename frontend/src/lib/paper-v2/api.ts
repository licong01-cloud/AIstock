import type {
  Activation,
  CandidateStrategyPackage,
  CandidateStrategyPackageInput,
  CreateMiniQMTAutoRunPortfolioResult,
  DataSource,
  TradingDayStatus,
  ExecutionPolicy,
  HmmConfig,
  HmmDailyCoefficientJob,
  HmmJob,
  HmmSnapshot,
  JsonObject,
  PaperPortfolio,
  PaperAutoRunSummary,
  PaperLiveDashboard,
  PaperRun,
  PaperSchedulerBootstrapStatus,
  PaperSchedulerRunResult,
  PaperSchedulerStatus,
  PaperSession,
  PaperSessionCapabilities,
  PaperSessionProgress,
  QEPackagingSource,
  ReadinessResult,
  ReplayResult,
  RunningSummaryResponse,
  RunningSummarySortBy,
  RunningSummarySortDir,
  RuntimeConfigActivation,
  RuntimeProfile,
  RuntimeProfileVersion,
  SelectablePackage,
  SelectionWatchlistImportResult,
  SelectionMode,
  SelectionRun,
  SimulationRuntimePlanSummary,
  SimulationRuntimeRunDetail,
  SimulationRuntimeRunsResponse,
  SimulationRuntimeSchedulerStatus,
  StrategyPackage,
  TradingDayDefaults,
} from "./types";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

export class PaperV2ApiError extends Error {
  status: number;
  errorCode?: string;
  context?: JsonObject;
  raw: unknown;

  constructor(message: string, status: number, raw: unknown, errorCode?: string, context?: JsonObject) {
    super(message);
    this.name = "PaperV2ApiError";
    this.status = status;
    this.errorCode = errorCode;
    this.context = context;
    this.raw = raw;
  }
}

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseError(payload: unknown, status: number): PaperV2ApiError {
  if (isObject(payload)) {
    const detail = payload.detail;
    if (isObject(detail)) {
      const errorCode = typeof detail.error_code === "string" ? detail.error_code : undefined;
      const message = typeof detail.message === "string" ? detail.message : JSON.stringify(detail);
      const context = isObject(detail.context) ? detail.context : undefined;
      return new PaperV2ApiError(message, status, payload, errorCode, context);
    }
    if (typeof detail === "string") return new PaperV2ApiError(detail, status, payload);
    if (typeof payload.error === "string") return new PaperV2ApiError(payload.error, status, payload);
    if (typeof payload.message === "string") return new PaperV2ApiError(payload.message, status, payload);
  }
  return new PaperV2ApiError(`HTTP ${status}`, status, payload);
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
  if (!response.ok) throw parseError(payload, response.status);
  return payload as T;
}

function body(payload: unknown): RequestInit {
  return { method: "POST", body: JSON.stringify(payload) };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isSessionSettled(progress: PaperSessionProgress, settleStatuses?: string[]): boolean {
  const status = String(progress.session?.status || "").toUpperCase();
  const expected = settleStatuses?.length ? settleStatuses : ["SUCCEEDED", "FAILED", "STOPPED"];
  return expected.map((item) => item.toUpperCase()).includes(status);
}

function isNetworkAbort(error: unknown): boolean {
  if (error instanceof PaperV2ApiError) return false;
  if (error instanceof SyntaxError) return true;
  const message = error instanceof Error ? error.message : String(error || "");
  return /Failed to fetch|NetworkError|socket hang up|ECONNRESET|aborted|Load failed|Unexpected token|Unexpected end of JSON/i.test(message);
}

async function fetchSessionProgress(sessionId: string): Promise<PaperSessionProgress> {
  const data = await apiFetch<{ progress: PaperSessionProgress }>(`/paper-v2/sessions/${sessionId}/progress`);
  return data.progress;
}

async function fetchTickSession(sessionId: string, payload: JsonObject = {}): Promise<PaperSessionProgress> {
  const data = await apiFetch<{ progress: PaperSessionProgress }>(`/paper-v2/sessions/${sessionId}/tick`, body(payload));
  return data.progress;
}

export const strategyPackageApi = {
  async qeSources(sourceKind = "all", limit = 200): Promise<QEPackagingSource[]> {
    const qs = new URLSearchParams({ source_kind: sourceKind, limit: String(limit) });
    const data = await apiFetch<{ sources: QEPackagingSource[] }>(`/strategy-packages/qe-sources?${qs.toString()}`);
    return data.sources || [];
  },
  async createFromQEExperiment(payload: { experiment_id: string; resolve_runtime_assets?: boolean }): Promise<StrategyPackage> {
    const data = await apiFetch<{ package: StrategyPackage }>("/strategy-packages/from-qe-experiment", body(payload));
    return data.package;
  },
  async createFromQEEvolutionLoop(payload: { qe_task_id: string; qe_loop_id: string; resolve_runtime_assets?: boolean }): Promise<StrategyPackage> {
    const data = await apiFetch<{ package: StrategyPackage }>("/strategy-packages/from-qe-evolution-loop", body(payload));
    return data.package;
  },
  async createFromCandidate(candidateId: string, payload: { manifest_json?: JsonObject | null } = {}): Promise<StrategyPackage> {
    const data = await apiFetch<{ package: StrategyPackage }>(
      `/strategy-packages/from-candidate/${encodeURIComponent(candidateId)}`,
      body(payload),
    );
    return data.package;
  },
  async candidateList(status = "ACTIVE", limit = 200): Promise<CandidateStrategyPackage[]> {
    const qs = new URLSearchParams({ limit: String(limit) });
    if (status) qs.set("status", status);
    const data = await apiFetch<{ candidates: CandidateStrategyPackage[] }>(`/strategy-packages/candidates?${qs.toString()}`);
    return data.candidates || [];
  },
  async candidate(candidateId: string): Promise<CandidateStrategyPackage> {
    const data = await apiFetch<{ candidate: CandidateStrategyPackage }>(`/strategy-packages/candidates/${encodeURIComponent(candidateId)}`);
    return data.candidate;
  },
  async createCandidateFromQEExperiment(
    payload: CandidateStrategyPackageInput & { experiment_id: string },
  ): Promise<CandidateStrategyPackage> {
    const data = await apiFetch<{ candidate: CandidateStrategyPackage }>("/strategy-packages/candidates/from-qe-experiment", body(payload));
    return data.candidate;
  },
  async createCandidateFromQELoop(
    payload: CandidateStrategyPackageInput & { qe_task_id: string; qe_loop_id: string; experiment_id?: string | null },
  ): Promise<CandidateStrategyPackage> {
    const data = await apiFetch<{ candidate: CandidateStrategyPackage }>("/strategy-packages/candidates/from-qe-loop", body(payload));
    return data.candidate;
  },
  async cloneCandidate(
    candidateId: string,
    payload: { created_by?: string; display_name?: string | null; overrides?: JsonObject } = {},
  ): Promise<CandidateStrategyPackage> {
    const data = await apiFetch<{ candidate: CandidateStrategyPackage }>(
      `/strategy-packages/candidates/${encodeURIComponent(candidateId)}/clone`,
      body(payload),
    );
    return data.candidate;
  },
  async refreshCandidateSnapshot(
    candidateId: string,
    payload: { refreshed_by?: string } = {},
  ): Promise<CandidateStrategyPackage> {
    const data = await apiFetch<{ candidate: CandidateStrategyPackage }>(
      `/strategy-packages/candidates/${encodeURIComponent(candidateId)}/refresh-snapshot`,
      body(payload),
    );
    return data.candidate;
  },
  async deleteCandidate(
    candidateId: string,
    payload: { deleted_by?: string; delete_reason?: string | null } = {},
  ): Promise<CandidateStrategyPackage> {
    const data = await apiFetch<{ candidate: CandidateStrategyPackage }>(
      `/strategy-packages/candidates/${encodeURIComponent(candidateId)}`,
      { method: "DELETE", body: JSON.stringify(payload) },
    );
    return data.candidate;
  },
  async qeExperimentManifest(experimentId: string): Promise<JsonObject> {
    const data = await apiFetch<{ manifest: JsonObject }>(`/strategy-packages/from-qe-experiment/${encodeURIComponent(experimentId)}/manifest`);
    return data.manifest;
  },
  async qeExperimentPaperReadiness(experimentId: string): Promise<JsonObject> {
    const data = await apiFetch<{ manifest: JsonObject }>(`/strategy-packages/from-qe-experiment/${encodeURIComponent(experimentId)}/paper-readiness`);
    return data.manifest;
  },
  async list(status?: string, limit = 200): Promise<StrategyPackage[]> {
    const qs = new URLSearchParams({ limit: String(limit) });
    if (status) qs.set("status", status);
    const data = await apiFetch<{ packages: StrategyPackage[] }>(`/strategy-packages?${qs.toString()}`);
    return data.packages || [];
  },
  async get(packageId: string): Promise<StrategyPackage> {
    const data = await apiFetch<{ package: StrategyPackage }>(`/strategy-packages/${packageId}`);
    return data.package;
  },
  async statusEvents(packageId: string): Promise<JsonObject[]> {
    const data = await apiFetch<{ events: JsonObject[] }>(`/strategy-packages/${packageId}/status-events`);
    return data.events || [];
  },
  async executionPolicies(packageId: string): Promise<ExecutionPolicy[]> {
    const data = await apiFetch<{ execution_policies: ExecutionPolicy[] }>(`/strategy-packages/${packageId}/execution-policies`);
    return data.execution_policies || [];
  },
  async retire(packageId: string): Promise<StrategyPackage> {
    const data = await apiFetch<{ package: StrategyPackage }>(`/strategy-packages/${packageId}/retire`, { method: "POST" });
    return data.package;
  },
  async modelState(packageId: string): Promise<JsonObject> {
    const data = await apiFetch<{ model_state: JsonObject }>(`/strategy-packages/${packageId}/model-state`);
    return data.model_state;
  },
  async modelRetrainPreview(packageId: string, payload: JsonObject): Promise<JsonObject> {
    const data = await apiFetch<{ preview: JsonObject }>(`/strategy-packages/${packageId}/model-retrain/preview`, body(payload));
    return data.preview;
  },
  async modelRetrainStart(packageId: string, payload: JsonObject): Promise<JsonObject> {
    const data = await apiFetch<{ job: JsonObject }>(`/strategy-packages/${packageId}/model-retrain/start`, body(payload));
    return data.job;
  },
  async modelRetrainJobs(packageId: string): Promise<JsonObject[]> {
    const data = await apiFetch<{ jobs: JsonObject[] }>(`/strategy-packages/${packageId}/model-retrain/jobs`);
    return data.jobs || [];
  },
  async deleteDependencies(packageId: string): Promise<JsonObject> {
    return apiFetch(`/strategy-packages/${encodeURIComponent(packageId)}/delete-dependencies`);
  },
  async deletePackage(packageId: string): Promise<JsonObject> {
    return apiFetch(`/strategy-packages/${encodeURIComponent(packageId)}`, {
      method: "DELETE",
      body: JSON.stringify({ confirm_delete: true }),
    });
  },
};

export const selectionCenterApi = {
  async selectablePackages(limit = 300): Promise<SelectablePackage[]> {
    const data = await apiFetch<{ packages: SelectablePackage[] }>(`/selection-center/selectable-packages?limit=${limit}`);
    return data.packages || [];
  },
  async industryTree(): Promise<JsonObject[]> {
    const data = await apiFetch<{ tree: JsonObject[] }>("/selection-center/industry-tree");
    return data.tree || [];
  },
  async listRuns(limit = 100): Promise<SelectionRun[]> {
    const data = await apiFetch<{ runs: SelectionRun[] }>(`/selection-center/runs?limit=${limit}`);
    return data.runs || [];
  },
  async listRunsPage(params: { page?: number; pageSize?: number; limit?: number } = {}): Promise<{ runs: SelectionRun[]; pagination: JsonObject }> {
    const qs = new URLSearchParams({
      page: String(params.page || 1),
      page_size: String(params.pageSize || params.limit || 20),
      limit: String(params.limit || params.pageSize || 20),
    });
    const data = await apiFetch<{ runs: SelectionRun[]; pagination?: JsonObject }>(`/selection-center/runs?${qs.toString()}`);
    return {
      runs: data.runs || [],
      pagination: data.pagination || {
        page: params.page || 1,
        page_size: params.pageSize || params.limit || 20,
        total: data.runs?.length || 0,
        total_pages: 1,
      },
    };
  },
  async getRun(runId: string): Promise<SelectionRun> {
    const data = await apiFetch<{ run: SelectionRun }>(`/selection-center/runs/${runId}`);
    return data.run;
  },
  async runSelection(payload: { package_ids: string[]; trade_date: string; data_source: string; mode: SelectionMode; runtime_config: JsonObject }): Promise<SelectionRun> {
    const data = await apiFetch<{ run: SelectionRun }>("/selection-center/runs", body(payload));
    return data.run;
  },
  async resolvePitCutoff(payload: { trade_date: string; pit_mode?: string; cutoff_date?: string | null }): Promise<JsonObject> {
    const qs = new URLSearchParams({
      trade_date: payload.trade_date,
      pit_mode: payload.pit_mode || "PREVIOUS_TRADING_DAY_CLOSE",
    });
    if (payload.cutoff_date) qs.set("cutoff_date", payload.cutoff_date);
    const data = await apiFetch<{ point_in_time_context: JsonObject }>(`/selection-center/pit-cutoff?${qs.toString()}`);
    return data.point_in_time_context;
  },
  async aggregateRuns(payload: { source_run_ids: string[]; mode: SelectionMode; runtime_config: JsonObject }): Promise<SelectionRun> {
    const data = await apiFetch<{ run: SelectionRun }>("/selection-center/aggregate-runs", body(payload));
    return data.run;
  },
  async excludedResults(runId: string): Promise<Record<string, unknown[]>> {
    const data = await apiFetch<{ excluded_results: Record<string, unknown[]> }>(`/selection-center/runs/${runId}/excluded-results`);
    return data.excluded_results || {};
  },
  async addToWatchlist(runId: string, payload: JsonObject): Promise<SelectionWatchlistImportResult> {
    const data = await apiFetch<{ result: SelectionWatchlistImportResult }>(`/selection-center/runs/${runId}/add-to-watchlist`, body(payload));
    return data.result;
  },
  async createPaperPortfolio(runId: string, payload: JsonObject): Promise<{ portfolio: PaperPortfolio; link: JsonObject; paper_runtime_config: JsonObject }> {
    return apiFetch(`/selection-center/runs/${runId}/create-paper-portfolio`, body(payload));
  },
  async deleteRun(runId: string): Promise<JsonObject> {
    return apiFetch(`/selection-center/runs/${runId}`, { method: "DELETE" });
  },
  async deleteRuns(runIds: string[]): Promise<JsonObject> {
    return apiFetch("/selection-center/runs/bulk-delete", {
      method: "POST",
      body: JSON.stringify({ run_ids: runIds, confirm_delete: true }),
    });
  },
};

export const paperV2Api = {
  async tradingDayDefaults(lookbackTradingDays = 10): Promise<TradingDayDefaults> {
    const data = await apiFetch<TradingDayDefaults>(`/paper-v2/trading-days/defaults?lookback_trading_days=${lookbackTradingDays}`);
    return data;
  },
  async tradingDayStatus(): Promise<TradingDayStatus> {
    return apiFetch<TradingDayStatus>("/trading-calendar/status");
  },
  async listPortfolios(limit = 200): Promise<PaperPortfolio[]> {
    const data = await apiFetch<{ portfolios: PaperPortfolio[] }>(`/paper-v2/portfolios?limit=${limit}`);
    return data.portfolios || [];
  },
  async listPortfoliosPage(params: {
    page?: number;
    pageSize?: number;
    limit?: number;
    statuses?: string[];
    search?: string;
    sortBy?: string;
    sortDir?: string;
  } = {}): Promise<{ portfolios: PaperPortfolio[]; pagination: JsonObject }> {
    const qs = new URLSearchParams({
      page: String(params.page || 1),
      page_size: String(params.pageSize || params.limit || 20),
      limit: String(params.limit || params.pageSize || 20),
      sort_by: params.sortBy || "created_at",
      sort_dir: params.sortDir || "desc",
    });
    for (const status of params.statuses || []) {
      if (status) qs.append("status", status);
    }
    if (params.search?.trim()) qs.set("search", params.search.trim());
    const data = await apiFetch<{ portfolios: PaperPortfolio[]; pagination?: JsonObject }>(`/paper-v2/portfolios?${qs.toString()}`);
    return {
      portfolios: data.portfolios || [],
      pagination: data.pagination || {
        page: params.page || 1,
        page_size: params.pageSize || params.limit || 20,
        total: data.portfolios?.length || 0,
        total_pages: 1,
      },
    };
  },
  async runningSummary(limit = 100, snapshotLimit = 30, positionLimit = 8): Promise<JsonObject[]> {
    const qs = new URLSearchParams({
      limit: String(limit),
      snapshot_limit: String(snapshotLimit),
      position_limit: String(positionLimit),
    });
    const data = await apiFetch<{ summaries: JsonObject[] }>(`/paper-v2/running-summary?${qs.toString()}`);
    return data.summaries || [];
  },
  async runningSummaryPage(params: {
    page?: number;
    pageSize?: number;
    snapshotLimit?: number;
    positionLimit?: number;
    statuses?: string[];
    sortBy?: RunningSummarySortBy | string;
    sortDir?: RunningSummarySortDir | string;
    search?: string;
    searchFields?: string[];
    minInitialCash?: number | null;
    maxInitialCash?: number | null;
  } = {}): Promise<RunningSummaryResponse> {
    const qs = new URLSearchParams({
      page: String(params.page || 1),
      page_size: String(params.pageSize || 20),
      snapshot_limit: String(params.snapshotLimit || 30),
      position_limit: String(params.positionLimit || 8),
      sort_by: params.sortBy || "latest_run_time",
      sort_dir: params.sortDir || "desc",
    });
    for (const status of params.statuses || []) {
      if (status) qs.append("status", status);
    }
    for (const field of params.searchFields || []) {
      if (field) qs.append("search_fields", field);
    }
    if (params.search?.trim()) qs.set("search", params.search.trim());
    if (params.minInitialCash !== undefined && params.minInitialCash !== null) qs.set("min_initial_cash", String(params.minInitialCash));
    if (params.maxInitialCash !== undefined && params.maxInitialCash !== null) qs.set("max_initial_cash", String(params.maxInitialCash));
    const data = await apiFetch<{ summaries: JsonObject[]; pagination?: RunningSummaryResponse["pagination"] }>(`/paper-v2/running-summary?${qs.toString()}`);
    return {
      summaries: data.summaries || [],
      pagination: data.pagination || {
        page: params.page || 1,
        page_size: params.pageSize || 20,
        total: data.summaries?.length || 0,
        total_pages: 1,
        sort_by: params.sortBy || "latest_run_time",
        sort_dir: params.sortDir || "desc",
      },
    };
  },
  async createPortfolio(payload: { package_id: string; portfolio_name: string; initial_cash: number; start_date: string; data_source: DataSource; broker_backend?: "local_sim" | "minqmt_sim"; fee_policy?: JsonObject; risk_policy?: JsonObject; execution_policy?: JsonObject }): Promise<PaperPortfolio> {
    const data = await apiFetch<{ portfolio: PaperPortfolio }>("/paper-v2/portfolios", body(payload));
    return data.portfolio;
  },
  async createMiniQMTAutoRunPortfolio(payload: {
    package_id: string;
    portfolio_name: string;
    initial_cash: number;
    start_date: string;
    broker_account_id: string;
    top_k?: number | null;
    hmm?: JsonObject | null;
    industry_blacklist?: string[];
    fee_policy?: JsonObject | null;
    risk_policy?: JsonObject | null;
    execution_policy?: JsonObject | null;
    trade_window_policy?: JsonObject | null;
    auto_run_config?: JsonObject | null;
    created_by?: string | null;
    create_session?: boolean;
  }): Promise<CreateMiniQMTAutoRunPortfolioResult> {
    const data = await apiFetch<CreateMiniQMTAutoRunPortfolioResult & { ok?: boolean }>(
      "/paper-v2/auto-run/miniqmt-portfolios",
      body(payload),
    );
    return data;
  },
  async getPortfolio(portfolioId: string): Promise<PaperPortfolio> {
    const data = await apiFetch<{ portfolio: PaperPortfolio }>(`/paper-v2/portfolios/${portfolioId}`);
    return data.portfolio;
  },
  async autoRunStatus(portfolioId: string): Promise<PaperAutoRunSummary> {
    const data = await apiFetch<{ auto_run: PaperAutoRunSummary }>(`/paper-v2/portfolios/${portfolioId}/auto-run/status`);
    return data.auto_run;
  },
  async enableAutoRun(portfolioId: string, payload: { broker_account_id: string; config?: JsonObject | null; updated_by?: string | null; create_session?: boolean }): Promise<CreateMiniQMTAutoRunPortfolioResult> {
    const data = await apiFetch<CreateMiniQMTAutoRunPortfolioResult & { ok?: boolean }>(
      `/paper-v2/portfolios/${portfolioId}/auto-run/enable`,
      body(payload),
    );
    return data;
  },
  async disableAutoRun(portfolioId: string, payload: { updated_by?: string | null } = {}): Promise<{ portfolio: PaperPortfolio; retired_bindings: JsonObject[]; auto_run: PaperAutoRunSummary }> {
    return apiFetch(`/paper-v2/portfolios/${portfolioId}/auto-run/disable`, body(payload));
  },
  async patchAutoRunConfig(portfolioId: string, payload: { patch: JsonObject; updated_by?: string | null }): Promise<{ portfolio: PaperPortfolio; auto_run: PaperAutoRunSummary }> {
    return apiFetch(`/paper-v2/portfolios/${portfolioId}/auto-run/config`, {
      method: "PATCH",
      body: JSON.stringify(payload),
    });
  },
  async lifecycle(portfolioId: string, action: "pause" | "resume" | "complete" | "retire"): Promise<PaperPortfolio> {
    const data = await apiFetch<{ portfolio: PaperPortfolio }>(`/paper-v2/portfolios/${portfolioId}/${action}`, { method: "POST" });
    return data.portfolio;
  },
  async bulkLifecycle(portfolioIds: string[], action: "pause" | "resume" | "complete" | "retire"): Promise<JsonObject> {
    return apiFetch("/paper-v2/portfolios/bulk-lifecycle", body({ portfolio_ids: portfolioIds, action }));
  },
  async deletePortfolio(portfolioId: string): Promise<JsonObject> {
    return apiFetch(`/paper-v2/portfolios/${portfolioId}`, {
      method: "DELETE",
      body: JSON.stringify({ confirm_delete: true }),
    });
  },
  async deletePortfolios(portfolioIds: string[]): Promise<JsonObject> {
    return apiFetch("/paper-v2/portfolios/bulk-delete", {
      method: "POST",
      body: JSON.stringify({ portfolio_ids: portfolioIds, confirm_delete: true }),
    });
  },
  async readiness(portfolioId: string, payload: { trade_date: string; runtime_config: JsonObject }): Promise<ReadinessResult> {
    const data = await apiFetch<{ readiness: ReadinessResult }>(`/paper-v2/portfolios/${portfolioId}/readiness`, body(payload));
    return data.readiness;
  },
  async runDay(portfolioId: string, payload: { trade_date: string; runtime_config: JsonObject }): Promise<JsonObject> {
    const data = await apiFetch<{ result: JsonObject }>(`/paper-v2/portfolios/${portfolioId}/run-day`, body(payload));
    return data.result;
  },
  async replay(portfolioId: string, payload: JsonObject): Promise<ReplayResult> {
    const data = await apiFetch<{ result: ReplayResult }>(`/paper-v2/portfolios/${portfolioId}/replay`, body(payload));
    return data.result;
  },
  async sessionCapabilities(portfolioId: string): Promise<PaperSessionCapabilities> {
    const data = await apiFetch<{ capabilities: PaperSessionCapabilities }>(`/paper-v2/portfolios/${portfolioId}/session-capabilities`);
    return data.capabilities;
  },
  async createSession(portfolioId: string, payload: JsonObject): Promise<PaperSession> {
    const data = await apiFetch<{ session: PaperSession }>(`/paper-v2/portfolios/${portfolioId}/sessions`, body(payload));
    return data.session;
  },
  async listSessions(portfolioId: string): Promise<PaperSession[]> {
    const data = await apiFetch<{ sessions: PaperSession[] }>(`/paper-v2/portfolios/${portfolioId}/sessions`);
    return data.sessions || [];
  },
  async sessions(portfolioId: string): Promise<PaperSession[]> {
    const data = await apiFetch<{ sessions: PaperSession[] }>(`/paper-v2/portfolios/${portfolioId}/sessions`);
    return data.sessions || [];
  },
  async sessionProgress(sessionId: string): Promise<PaperSessionProgress> {
    return fetchSessionProgress(sessionId);
  },
  async tickSession(sessionId: string, payload: JsonObject = {}): Promise<PaperSessionProgress> {
    return fetchTickSession(sessionId, payload);
  },
  async tickSessionAndWait(
    sessionId: string,
    payload: JsonObject = {},
    options: { timeoutMs?: number; pollMs?: number; settleStatuses?: string[] } = {},
  ): Promise<PaperSessionProgress> {
    const timeoutMs = options.timeoutMs ?? 240_000;
    const pollMs = options.pollMs ?? 2_000;
    const startedAt = Date.now();
    try {
      const progress = await fetchTickSession(sessionId, payload);
      if (isSessionSettled(progress, options.settleStatuses)) return progress;
    } catch (error) {
      if (!isNetworkAbort(error)) throw error;
      // The backend may continue a long replay after the dev proxy drops the socket.
      // Poll persisted session progress; do not fabricate success.
    }

    let lastProgress = await fetchSessionProgress(sessionId);
    while (!isSessionSettled(lastProgress, options.settleStatuses)) {
      if (Date.now() - startedAt > timeoutMs) {
        throw new PaperV2ApiError(
          `session ${sessionId} did not reach the expected status within ${timeoutMs}ms`,
          408,
          { session_id: sessionId, expected_statuses: options.settleStatuses || ["SUCCEEDED", "FAILED", "STOPPED"], last_progress: lastProgress },
          "SESSION_PROGRESS_TIMEOUT",
          { session_id: sessionId, last_status: lastProgress.session?.status },
        );
      }
      await sleep(pollMs);
      lastProgress = await fetchSessionProgress(sessionId);
    }
    return lastProgress;
  },
  async sessionLifecycle(sessionId: string, action: "pause" | "resume" | "stop"): Promise<PaperSession> {
    const data = await apiFetch<{ session: PaperSession }>(`/paper-v2/sessions/${sessionId}/${action}`, { method: "POST" });
    return data.session;
  },
  async switchSessionMode(sessionId: string, payload: JsonObject): Promise<PaperSession> {
    const data = await apiFetch<{ session: PaperSession }>(`/paper-v2/sessions/${sessionId}/switch-mode`, body(payload));
    return data.session;
  },
  async schedulerStatus(): Promise<PaperSchedulerStatus> {
    const data = await apiFetch<{ scheduler: PaperSchedulerStatus }>("/paper-v2/session-scheduler/status");
    return data.scheduler;
  },
  async schedulerBootstrapStatus(): Promise<PaperSchedulerBootstrapStatus> {
    const data = await apiFetch<{ bootstrap: PaperSchedulerBootstrapStatus }>("/paper-v2/session-scheduler/bootstrap-status");
    return data.bootstrap;
  },
  async startScheduler(payload: { interval_seconds?: number | null } = {}): Promise<PaperSchedulerStatus> {
    const data = await apiFetch<{ scheduler: PaperSchedulerStatus }>("/paper-v2/session-scheduler/start", body(payload));
    return data.scheduler;
  },
  async stopScheduler(): Promise<PaperSchedulerStatus> {
    const data = await apiFetch<{ scheduler: PaperSchedulerStatus }>("/paper-v2/session-scheduler/stop", { method: "POST" });
    return data.scheduler;
  },
  async runSchedulerOnce(payload: { limit?: number; as_of_time?: string | null } = {}): Promise<PaperSchedulerRunResult> {
    const data = await apiFetch<{ result: PaperSchedulerRunResult }>("/paper-v2/session-scheduler/run-once", body(payload));
    return data.result;
  },
  async recoverAutoRun(payload: { limit?: number; as_of_time?: string | null } = {}): Promise<JsonObject> {
    const data = await apiFetch<{ recovery: JsonObject }>("/paper-v2/session-scheduler/recover-auto-run", body(payload));
    return data.recovery;
  },
  async executionPolicies(portfolioId: string): Promise<ExecutionPolicy[]> {
    const data = await apiFetch<{ execution_policies: ExecutionPolicy[] }>(`/paper-v2/portfolios/${portfolioId}/execution-policies`);
    return data.execution_policies || [];
  },
  async activatePolicy(portfolioId: string, payload: JsonObject): Promise<Activation> {
    const data = await apiFetch<{ activation: Activation }>(`/paper-v2/portfolios/${portfolioId}/execution-policy-activations`, body(payload));
    return data.activation;
  },
  async activations(portfolioId: string): Promise<Activation[]> {
    const data = await apiFetch<{ activations: Activation[] }>(`/paper-v2/portfolios/${portfolioId}/execution-policy-activations`);
    return data.activations || [];
  },
  async createRuntimeProfile(portfolioId: string, payload: { profile_name: string; config_json: JsonObject; created_by?: string | null; reason?: string | null }): Promise<{ profile: RuntimeProfile; version: RuntimeProfileVersion }> {
    return apiFetch(`/paper-v2/portfolios/${portfolioId}/runtime-profiles`, body(payload));
  },
  async runtimeProfiles(portfolioId: string): Promise<RuntimeProfile[]> {
    const data = await apiFetch<{ profiles: RuntimeProfile[] }>(`/paper-v2/portfolios/${portfolioId}/runtime-profiles`);
    return data.profiles || [];
  },
  async createRuntimeProfileVersion(portfolioId: string, profileId: string, payload: { config_json: JsonObject; created_by?: string | null; reason?: string | null }): Promise<RuntimeProfileVersion> {
    const data = await apiFetch<{ version: RuntimeProfileVersion }>(`/paper-v2/portfolios/${portfolioId}/runtime-profiles/${profileId}/versions`, body(payload));
    return data.version;
  },
  async runtimeProfileVersions(portfolioId: string, profileId: string): Promise<RuntimeProfileVersion[]> {
    const data = await apiFetch<{ versions: RuntimeProfileVersion[] }>(`/paper-v2/portfolios/${portfolioId}/runtime-profiles/${profileId}/versions`);
    return data.versions || [];
  },
  async activateRuntimeConfig(portfolioId: string, payload: { trade_date: string; profile_version_id: string; activated_by?: string | null; reason?: string | null; replace_existing?: boolean }): Promise<RuntimeConfigActivation> {
    const data = await apiFetch<{ activation: RuntimeConfigActivation }>(`/paper-v2/portfolios/${portfolioId}/runtime-config-activations`, body(payload));
    return data.activation;
  },
  async runtimeConfigActivations(portfolioId: string): Promise<RuntimeConfigActivation[]> {
    const data = await apiFetch<{ activations: RuntimeConfigActivation[] }>(`/paper-v2/portfolios/${portfolioId}/runtime-config-activations`);
    return data.activations || [];
  },
  async configChangeAudit(portfolioId: string): Promise<JsonObject[]> {
    const data = await apiFetch<{ audit: JsonObject[] }>(`/paper-v2/portfolios/${portfolioId}/config-change-audit`);
    return data.audit || [];
  },
  async liveDashboard(portfolioId: string, payload: { trade_date?: string | null; event_limit?: number } = {}): Promise<PaperLiveDashboard> {
    const qs = new URLSearchParams();
    if (payload.trade_date) qs.set("trade_date", payload.trade_date);
    if (payload.event_limit) qs.set("event_limit", String(payload.event_limit));
    const suffix = qs.toString() ? `?${qs.toString()}` : "";
    const data = await apiFetch<{ dashboard: PaperLiveDashboard }>(`/paper-v2/portfolios/${portfolioId}/live-dashboard${suffix}`);
    return data.dashboard;
  },
  async intradaySnapshots(portfolioId: string, payload: { trade_date?: string | null; limit?: number } = {}): Promise<JsonObject[]> {
    const qs = new URLSearchParams();
    if (payload.trade_date) qs.set("trade_date", payload.trade_date);
    if (payload.limit) qs.set("limit", String(payload.limit));
    const suffix = qs.toString() ? `?${qs.toString()}` : "";
    const data = await apiFetch<{ intraday_snapshots: JsonObject[] }>(`/paper-v2/portfolios/${portfolioId}/intraday-snapshots${suffix}`);
    return data.intraday_snapshots || [];
  },
  async minuteExecution(portfolioId: string, payload: { trade_date?: string | null; symbol?: string | null; limit?: number } = {}): Promise<JsonObject> {
    const qs = new URLSearchParams();
    if (payload.trade_date) qs.set("trade_date", payload.trade_date);
    if (payload.symbol) qs.set("symbol", payload.symbol);
    if (payload.limit) qs.set("limit", String(payload.limit));
    const suffix = qs.toString() ? `?${qs.toString()}` : "";
    const data = await apiFetch<{ minute_execution: JsonObject }>(`/paper-v2/portfolios/${portfolioId}/minute-execution${suffix}`);
    return data.minute_execution;
  },
  async orders(portfolioId: string): Promise<JsonObject[]> {
    const data = await apiFetch<{ orders: JsonObject[] }>(`/paper-v2/portfolios/${portfolioId}/orders`);
    return data.orders || [];
  },
  async fills(portfolioId: string): Promise<JsonObject[]> {
    const data = await apiFetch<{ fills: JsonObject[] }>(`/paper-v2/portfolios/${portfolioId}/fills`);
    return data.fills || [];
  },
  async cashLedger(portfolioId: string): Promise<JsonObject[]> {
    const data = await apiFetch<{ cash_ledger: JsonObject[] }>(`/paper-v2/portfolios/${portfolioId}/cash-ledger`);
    return data.cash_ledger || [];
  },
  async positions(portfolioId: string): Promise<JsonObject[]> {
    const data = await apiFetch<{ positions: JsonObject[] }>(`/paper-v2/portfolios/${portfolioId}/positions`);
    return data.positions || [];
  },
  async snapshots(portfolioId: string): Promise<JsonObject[]> {
    const data = await apiFetch<{ daily_snapshots: JsonObject[] }>(`/paper-v2/portfolios/${portfolioId}/daily-snapshots`);
    return data.daily_snapshots || [];
  },
  async performance(portfolioId: string): Promise<JsonObject> {
    const data = await apiFetch<{ performance_report: JsonObject }>(`/paper-v2/portfolios/${portfolioId}/performance-report`);
    return data.performance_report;
  },
  async performanceOrNull(portfolioId: string): Promise<JsonObject | null> {
    try {
      return await this.performance(portfolioId);
    } catch (error) {
      if (error instanceof PaperV2ApiError && error.status === 404 && error.errorCode === "DATA_UNAVAILABLE") {
        return null;
      }
      throw error;
    }
  },
  async runs(portfolioId: string): Promise<PaperRun[]> {
    const data = await apiFetch<{ runs: PaperRun[] }>(`/paper-v2/portfolios/${portfolioId}/runs`);
    return data.runs || [];
  },
  async runEvents(portfolioId: string, runId?: string): Promise<JsonObject[]> {
    const qs = runId ? `?run_id=${encodeURIComponent(runId)}` : "";
    const data = await apiFetch<{ run_events: JsonObject[] }>(`/paper-v2/portfolios/${portfolioId}/run-events${qs}`);
    return data.run_events || [];
  },
  async errors(portfolioId: string): Promise<JsonObject[]> {
    const data = await apiFetch<{ errors: JsonObject[] }>(`/paper-v2/portfolios/${portfolioId}/errors`);
    return data.errors || [];
  },
};

export type QmtStatus = {
  enabled?: boolean;
  connected?: boolean;
  mode?: string;
  account_id?: string | null;
  provider?: string;
  userdata_path?: string | null;
  session_id?: number | null;
  last_error?: string | null;
  pid?: number;
  client_class?: string;
  [key: string]: unknown;
};

export const simulationRuntimeApi = {
  async schedulerStatus(): Promise<SimulationRuntimeSchedulerStatus> {
    const data = await apiFetch<{ scheduler: SimulationRuntimeSchedulerStatus }>("/simulation-runtime/scheduler/status");
    return data.scheduler;
  },
  async listRuns(params: {
    tradeDate?: string;
    brokerBackend?: string;
    strategyId?: string;
    status?: string;
    limit?: number;
  } = {}): Promise<SimulationRuntimeRunsResponse> {
    const qs = new URLSearchParams({ limit: String(params.limit || 100) });
    if (params.tradeDate) qs.set("trade_date", params.tradeDate);
    if (params.brokerBackend) qs.set("broker_backend", params.brokerBackend);
    if (params.strategyId?.trim()) qs.set("strategy_id", params.strategyId.trim());
    if (params.status) qs.set("status", params.status);
    const data = await apiFetch<SimulationRuntimeRunsResponse>(`/simulation-runtime/runs?${qs.toString()}`);
    return { summary: data.summary || {}, runs: data.runs || [] };
  },
  async getRun(runId: string): Promise<SimulationRuntimeRunDetail> {
    return apiFetch(`/simulation-runtime/runs/${encodeURIComponent(runId)}`);
  },
  async getExecutionPlan(planId: string): Promise<SimulationRuntimePlanSummary> {
    const data = await apiFetch<{ execution_plan: SimulationRuntimePlanSummary }>(`/simulation-runtime/execution-plans/${encodeURIComponent(planId)}`);
    return data.execution_plan;
  },
  async liveAdmissionEvidence(params: {
    paperV2RunId: string;
    miniqmtSimRunId: string;
    targetBrokerBackend?: string;
  }): Promise<JsonObject> {
    const qs = new URLSearchParams({
      paper_v2_run_id: params.paperV2RunId,
      miniqmt_sim_run_id: params.miniqmtSimRunId,
      target_broker_backend: params.targetBrokerBackend || "minqmt_live",
    });
    return apiFetch<JsonObject>(`/simulation-runtime/live-admission/evidence?${qs.toString()}`);
  },
};

export const qmtApi = {
  async status(): Promise<QmtStatus> {
    return apiFetch<QmtStatus>("/qmt/status");
  },
  async connect(): Promise<JsonObject> {
    return apiFetch<JsonObject>("/qmt/connect", { method: "POST" });
  },
  async account(): Promise<JsonObject> {
    return apiFetch<JsonObject>("/qmt/account");
  },
  async positions(): Promise<JsonObject[]> {
    return apiFetch<JsonObject[]>("/qmt/positions");
  },
  async orders(cancelableOnly = false): Promise<JsonObject[]> {
    const qs = new URLSearchParams({ cancelable_only: String(cancelableOnly) });
    return apiFetch<JsonObject[]>(`/qmt/orders?${qs.toString()}`);
  },
  async trades(): Promise<JsonObject[]> {
    return apiFetch<JsonObject[]>("/qmt/trades");
  },
  async monitorStrategies(): Promise<JsonObject> {
    return apiFetch<JsonObject>("/qmt/monitor/strategies");
  },
};

export const hmmTrainingApi = {
  async configs(): Promise<HmmConfig[]> {
    return apiFetch<HmmConfig[]>("/hmm-training/configs");
  },
  async previewRolling(configId: string, payload: JsonObject): Promise<JsonObject> {
    return apiFetch(`/hmm-training/configs/${configId}/rolling-training/preview`, body(payload));
  },
  async triggerRolling(configId: string, payload: JsonObject): Promise<HmmJob> {
    return apiFetch(`/hmm-training/configs/${configId}/rolling-training/trigger`, body(payload));
  },
  async jobs(configId: string): Promise<HmmJob[]> {
    return apiFetch<HmmJob[]>(`/hmm-training/configs/${configId}/jobs`);
  },
  async snapshots(configId: string): Promise<HmmSnapshot[]> {
    return apiFetch<HmmSnapshot[]>(`/hmm-training/configs/${configId}/snapshots`);
  },
  async previewDailyCoefficients(snapshotId: string, payload: JsonObject): Promise<JsonObject> {
    return apiFetch(`/hmm-training/snapshots/${snapshotId}/daily-coefficients/preview`, body(payload));
  },
  async generateDailyCoefficients(snapshotId: string, payload: JsonObject): Promise<JsonObject> {
    return apiFetch(`/hmm-training/snapshots/${snapshotId}/daily-coefficients/generate`, body(payload));
  },
  async startDailyCoefficientJob(snapshotId: string, payload: JsonObject): Promise<HmmDailyCoefficientJob> {
    return apiFetch(`/hmm-training/snapshots/${snapshotId}/daily-coefficients/jobs`, body(payload));
  },
  async dailyCoefficientJob(jobId: string): Promise<HmmDailyCoefficientJob> {
    return apiFetch<HmmDailyCoefficientJob>(`/hmm-training/daily-coefficients/jobs/${jobId}`);
  },
  async dailyCoefficientJobs(snapshotId: string): Promise<HmmDailyCoefficientJob[]> {
    return apiFetch<HmmDailyCoefficientJob[]>(`/hmm-training/snapshots/${snapshotId}/daily-coefficients/jobs`);
  },
};

export { API_BASE };
