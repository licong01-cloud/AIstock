import type { JsonObject, PaperPortfolio, PaperRun, PaperSession } from "./types";

export type RunningPortfolioSummary = {
  portfolio: PaperPortfolio;
  latestRun?: PaperRun;
  latestSession?: PaperSession;
  operability?: JsonObject;
  counts: { orders: number; fills: number; positions: number; errors: number };
  latestSnapshot?: JsonObject | null;
  recentSnapshots: JsonObject[];
  latestPositions: JsonObject[];
};

export const ACTIVE_RUNNING_STATUSES = ["RUNNING", "PAUSED"];
export const CURRENT_ACTIVE_SIMULATION_BROKERS = ["local_sim", "minqmt_sim"];

const CURRENT_ACTIVE_SIMULATION_BROKER_SET = new Set(CURRENT_ACTIVE_SIMULATION_BROKERS);

export const RUNNING_STATUS_OPTIONS = [
  { value: "ACTIVE", label: "运行/暂停" },
  { value: "READY", label: "未就绪（READY）" },
  { value: "RUNNING", label: "运行中（RUNNING）" },
  { value: "PAUSED", label: "已暂停（PAUSED）" },
  { value: "FAILED", label: "FAILED" },
  { value: "COMPLETED", label: "COMPLETED" },
  { value: "RETIRED", label: "RETIRED" },
];

export const RUNNING_SEARCH_FIELD_OPTIONS = [
  { value: "all", label: "全部字段" },
  { value: "portfolio_id", label: "模拟盘ID" },
  { value: "package_id", label: "策略包ID" },
  { value: "status", label: "状态" },
  { value: "data_source", label: "数据源" },
  { value: "initial_cash", label: "初始资金" },
  { value: "latest_run_status", label: "最近运行状态" },
  { value: "latest_run_trade_date", label: "最近运行日期" },
  { value: "manifest_sha256", label: "Manifest Hash" },
];

export const RUNNING_SORT_OPTIONS = [
  { value: "latest_run_time", label: "最近运行时间" },
  { value: "status", label: "状态" },
  { value: "initial_cash", label: "初始资金" },
  { value: "updated_at", label: "更新时间" },
  { value: "created_at", label: "创建时间" },
  { value: "portfolio_name", label: "模拟盘名称" },
];

export function n(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

export function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function asRows(value: unknown): JsonObject[] {
  return Array.isArray(value) ? value.filter(isObject) : [];
}

export function parseRunningSummaryItem(item: JsonObject): RunningPortfolioSummary {
  const counts = isObject(item.counts) ? item.counts : {};
  return {
    portfolio: item.portfolio as PaperPortfolio,
    latestRun: isObject(item.latest_run) ? item.latest_run as PaperRun : undefined,
    latestSession: isObject(item.latest_session) ? item.latest_session as PaperSession : undefined,
    operability: isObject(item.operability) ? item.operability : undefined,
    counts: {
      orders: n(counts.orders),
      fills: n(counts.fills),
      positions: n(counts.positions),
      errors: n(counts.errors),
    },
    latestSnapshot: isObject(item.latest_snapshot) ? item.latest_snapshot : null,
    recentSnapshots: asRows(item.recent_snapshots),
    latestPositions: asRows(item.latest_positions),
  };
}

export function latestSnapshot(row: RunningPortfolioSummary): JsonObject | undefined {
  return (row.latestSnapshot && isObject(row.latestSnapshot) ? row.latestSnapshot : undefined) || row.recentSnapshots[0];
}

export function totalReturn(row: RunningPortfolioSummary): number | null {
  const snapshot = latestSnapshot(row);
  const nav = n(snapshot?.nav);
  const initial = n(row.portfolio.initial_cash);
  if (!nav || !initial) return null;
  return nav / initial - 1;
}

export function packageName(portfolio: PaperPortfolio): string {
  return String(portfolio.frozen_manifest?.["package_name"] || portfolio.package_id || "-");
}

export function packageSource(portfolio: PaperPortfolio): string {
  return String(portfolio.frozen_manifest?.["source_id"] || portfolio.frozen_manifest?.["run_id"] || portfolio.package_id || "-");
}

export function brokerBackendLabel(value: unknown): string {
  const backend = String(value || "local_sim").toLowerCase();
  if (backend === "minqmt_sim") return "MiniQMT 模拟盘";
  if (backend === "local_sim") return "LocalSim 本地模拟";
  return String(value || "-");
}

export function isCurrentActiveSimulation(row: RunningPortfolioSummary): boolean {
  const broker = String(row.portfolio.broker_backend || "local_sim").toLowerCase();
  const status = String(row.portfolio.status || "").toUpperCase();
  return ACTIVE_RUNNING_STATUSES.includes(status) && CURRENT_ACTIVE_SIMULATION_BROKER_SET.has(broker);
}

function firstText(...values: unknown[]): string {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return "";
}

function errorText(value: unknown): string {
  if (typeof value === "string") return value.trim();
  if (!isObject(value)) return "";
  const nested = isObject(value.error) ? value.error : {};
  return firstText(
    value.message,
    value.detail,
    value.error_message,
    nested.message,
    value.error_code,
    value.code,
  );
}

export function sessionErrorMessage(row: RunningPortfolioSummary): string {
  const sessionError = errorText(row.latestSession?.last_error);
  if (sessionError) return sessionError;
  const runError = errorText((row.latestRun as unknown as JsonObject | undefined)?.error || (row.latestRun as unknown as JsonObject | undefined)?.error_json);
  if (runError) return runError;
  return firstText(row.operability?.remediation_hint);
}

export function activeSimulationState(row: RunningPortfolioSummary): { label: string; hint: string; badgeTone: "success" | "danger" | "warning" | "info" | "neutral" } {
  const portfolioStatus = String(row.portfolio.status || "").toUpperCase();
  const sessionStatus = String(row.latestSession?.status || row.operability?.latest_session_status || "").toUpperCase();
  const hasTickableSession = row.operability?.has_tickable_session === true;
  const noOperableSession = row.operability?.no_operable_session === true;
  const message = sessionErrorMessage(row);
  if (portfolioStatus === "PAUSED") {
    return { label: "已暂停 / 激活保留", hint: message || "模拟盘仍属于当前激活集合，可恢复后继续运行。", badgeTone: "info" };
  }
  if (sessionStatus === "FAILED") {
    return { label: "当日失败 / 需恢复", hint: message || "最近会话失败，仍保留在当前激活模拟盘便于排障。", badgeTone: "danger" };
  }
  if (noOperableSession) {
    return { label: "无可推进会话 / 需恢复", hint: message || "模拟盘处于激活状态，但没有 scheduler 可推进会话。", badgeTone: "warning" };
  }
  if (hasTickableSession) {
    return { label: "可推进", hint: "存在 scheduler 可推进会话。", badgeTone: "success" };
  }
  return { label: "激活中 / 待检查", hint: message || "模拟盘状态仍为激活，但缺少可推进会话证据。", badgeTone: "warning" };
}

export function runningScenario(row: RunningPortfolioSummary): { label: string; hint: string } {
  const status = String(row.portfolio.status || "").toUpperCase();
  const operability = isObject(row.operability) ? row.operability : {};
  if (operability.no_operable_session) {
    return {
      label: "NO_OPERABLE_SESSION",
      hint: String(operability.remediation_hint || "Portfolio is active but no scheduler-tickable live/replay session exists."),
    };
  }
  const session = row.latestSession;
  const mode = String(session?.mode || "").toUpperCase();
  const sessionStatus = String(session?.status || "").toUpperCase();
  const phase = String(session?.phase || "").toLowerCase();
  if (status === "READY") {
    return { label: "未就绪/未运行", hint: "READY 不代表正在运行，需要启动历史追赶或实时会话" };
  }
  if (mode === "REPLAY_ONLY") {
    return { label: "仅历史追赶", hint: sessionStatus === "SUCCEEDED" ? "追赶完成后停止" : "只处理历史分钟线，不会自动切实时" };
  }
  if (mode === "CATCHUP_THEN_LIVE" && phase.includes("historical")) {
    return { label: "历史追赶中", hint: "追赶至最新可回放日后自动进入实时行情" };
  }
  if (mode === "CATCHUP_THEN_LIVE") {
    return { label: "追赶后实时", hint: "已进入实时阶段或等待下一交易日" };
  }
  if (mode === "LIVE_ONLY") {
    return { label: "完全实时运行", hint: "直接使用 TDX 实时分钟线" };
  }
  return { label: "无运行会话", hint: "没有可追踪的 replay/live session" };
}

export function statusFilterToStatuses(statusFilter: string): string[] {
  return statusFilter === "ACTIVE" ? ACTIVE_RUNNING_STATUSES : [statusFilter];
}
