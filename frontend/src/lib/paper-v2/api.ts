import type {
  Activation,
  CandidateStrategyPackage,
  CandidateStrategyPackageInput,
  DataSource,
  ExecutionPolicy,
  HmmConfig,
  HmmDailyCoefficientJob,
  HmmJob,
  HmmSnapshot,
  JsonObject,
  PaperPortfolio,
  PaperLiveDashboard,
  PaperRun,
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
  async enableSelection(packageId: string): Promise<StrategyPackage> {
    const data = await apiFetch<{ package: StrategyPackage }>(`/strategy-packages/${packageId}/enable-selection`, { method: "POST" });
    return data.package;
  },
  async enablePaper(packageId: string): Promise<StrategyPackage> {
    const data = await apiFetch<{ package: StrategyPackage }>(`/strategy-packages/${packageId}/enable-paper`, { method: "POST" });
    return data.package;
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
};

export const selectionCenterApi = {
  async selectablePackages(limit = 300): Promise<SelectablePackage[]> {
    const data = await apiFetch<{ packages: SelectablePackage[] }>(`/selection-center/selectable-packages?limit=${limit}`);
    return data.packages || [];
  },
  async listRuns(limit = 100): Promise<SelectionRun[]> {
    const data = await apiFetch<{ runs: SelectionRun[] }>(`/selection-center/runs?limit=${limit}`);
    return data.runs || [];
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
};

export const paperV2Api = {
  async tradingDayDefaults(lookbackTradingDays = 10): Promise<TradingDayDefaults> {
    const data = await apiFetch<TradingDayDefaults>(`/paper-v2/trading-days/defaults?lookback_trading_days=${lookbackTradingDays}`);
    return data;
  },
  async listPortfolios(limit = 200): Promise<PaperPortfolio[]> {
    const data = await apiFetch<{ portfolios: PaperPortfolio[] }>(`/paper-v2/portfolios?limit=${limit}`);
    return data.portfolios || [];
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
  async createPortfolio(payload: { package_id: string; portfolio_name: string; initial_cash: number; start_date: string; data_source: DataSource; fee_policy?: JsonObject; risk_policy?: JsonObject; execution_policy?: JsonObject }): Promise<PaperPortfolio> {
    const data = await apiFetch<{ portfolio: PaperPortfolio }>("/paper-v2/portfolios", body(payload));
    return data.portfolio;
  },
  async getPortfolio(portfolioId: string): Promise<PaperPortfolio> {
    const data = await apiFetch<{ portfolio: PaperPortfolio }>(`/paper-v2/portfolios/${portfolioId}`);
    return data.portfolio;
  },
  async lifecycle(portfolioId: string, action: "pause" | "resume" | "complete" | "retire"): Promise<PaperPortfolio> {
    const data = await apiFetch<{ portfolio: PaperPortfolio }>(`/paper-v2/portfolios/${portfolioId}/${action}`, { method: "POST" });
    return data.portfolio;
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
