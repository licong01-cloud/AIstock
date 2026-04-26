import type {
  Activation,
  DataSource,
  ExecutionPolicy,
  HmmConfig,
  HmmJob,
  HmmSnapshot,
  JsonObject,
  PaperPortfolio,
  PaperRun,
  QEPackagingSource,
  ReadinessResult,
  ReplayResult,
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
};

export { API_BASE };
