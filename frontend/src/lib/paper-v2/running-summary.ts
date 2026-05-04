import type { JsonObject, PaperPortfolio, PaperRun, PaperSession } from "./types";

export type RunningPortfolioSummary = {
  portfolio: PaperPortfolio;
  latestRun?: PaperRun;
  latestSession?: PaperSession;
  counts: { orders: number; fills: number; positions: number; errors: number };
  latestSnapshot?: JsonObject | null;
  recentSnapshots: JsonObject[];
  latestPositions: JsonObject[];
};

export const ACTIVE_RUNNING_STATUSES = ["READY", "RUNNING", "PAUSED"];

export const RUNNING_STATUS_OPTIONS = [
  { value: "ACTIVE", label: "全部活跃" },
  { value: "READY", label: "READY" },
  { value: "RUNNING", label: "RUNNING" },
  { value: "PAUSED", label: "PAUSED" },
  { value: "FAILED", label: "FAILED" },
  { value: "COMPLETED", label: "COMPLETED" },
  { value: "RETIRED", label: "RETIRED" },
];

export const RUNNING_SEARCH_FIELD_OPTIONS = [
  { value: "all", label: "全部字段" },
  { value: "portfolio_id", label: "组合ID" },
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
  { value: "portfolio_name", label: "组合名" },
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

export function statusFilterToStatuses(statusFilter: string): string[] {
  return statusFilter === "ACTIVE" ? ACTIVE_RUNNING_STATUSES : [statusFilter];
}
