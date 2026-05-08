export function formatNumber(value: unknown, digits = 2): string {
  if (value === null || value === undefined || value === "") return "-";
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return n.toLocaleString("en-US", { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

export function formatCompact(value: unknown, digits = 2): string {
  if (value === null || value === undefined || value === "") return "-";
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return n.toLocaleString("en-US", { notation: "compact", maximumFractionDigits: digits });
}

export function formatPercent(value: unknown, digits = 2): string {
  if (value === null || value === undefined || value === "") return "-";
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `${(n * 100).toFixed(digits)}%`;
}

export function shortHash(value: unknown, size = 8): string {
  const text = String(value || "");
  if (!text) return "-";
  if (text.length <= size * 2 + 3) return text;
  return `${text.slice(0, size)}...${text.slice(-size)}`;
}

export function hmmSnapshotLabel(snapshot: {
  display_name?: string | null;
  snapshot_id?: string | null;
  trained_at?: string | null;
  metrics_json?: Record<string, unknown> | null;
}): string {
  const explicit = String(snapshot.display_name || "").trim();
  if (explicit) return explicit;
  const metrics = snapshot.metrics_json && typeof snapshot.metrics_json === "object" ? snapshot.metrics_json : null;
  const metricsLabel = String(metrics?.snapshot_display_name || metrics?.display_name || "").trim();
  if (metricsLabel) return metricsLabel;
  const trainedDate = String(snapshot.trained_at || "").slice(0, 10);
  const id = shortHash(snapshot.snapshot_id);
  return trainedDate ? `${id} / ${trainedDate}` : id;
}

export function todayIso(): string {
  return new Date().toISOString().slice(0, 10);
}

export function asText(value: unknown): string {
  if (value === null || value === undefined || value === "") return "-";
  return String(value);
}

export function statusTone(status: unknown): "success" | "danger" | "warning" | "info" | "neutral" {
  const s = String(status || "").toUpperCase();
  if (["PASSED", "SUCCEEDED", "SUCCESS", "COMPLETED", "PAPER_ENABLED", "SELECTION_ENABLED", "ACTIVE", "CURRENT", "FILLED", "ALL_TRADED", "RUNNABLE"].includes(s)) return "success";
  if (["FAILED", "ERROR", "REJECTED", "PAPER_FAILED", "BLOCKED", "PAPER_DATA_BLOCKED", "CANCELED", "CANCELLED", "EXPIRED"].includes(s)) return "danger";
  if (["STALE", "STALE_WARNING", "WARNING", "WARN", "LEGACY_NON_ST_PIT", "CACHE_ONLY", "PENDING", "DRAFT", "RETRAINING", "RUNNING", "PAUSED", "PREFLIGHTING", "REPLAYING", "CATCHING_UP", "SWITCHING_TO_LIVE", "LIVE_RUNNING", "LIVE_WAITING_FOR_BAR", "PARTIALLY_FILLED", "PARTIAL_FILLED", "NEW", "SUBMITTED"].includes(s)) return "warning";
  if (["READY", "CREATED", "STOPPED", "LIVE_WAITING_NEXT_TRADING_DAY", "UNSUPPORTED", "NOT_RUN", "NO_DATA"].includes(s)) return "info";
  return "neutral";
}

const STATUS_LABELS: Record<string, string> = {
  READY: "未就绪",
  PASSED: "已通过",
  SUCCEEDED: "成功",
  SUCCESS: "成功",
  COMPLETED: "已完成",
  PAPER_ENABLED: "已启用模拟盘",
  SELECTION_ENABLED: "已启用选股",
  RUNNABLE: "可运行",
  ACTIVE: "生效中",
  CURRENT: "当前",
  FAILED: "失败",
  ERROR: "错误",
  REJECTED: "已拒绝",
  PAPER_FAILED: "模拟盘失败",
  BLOCKED: "已阻断",
  STALE: "已过期",
  STALE_WARNING: "过期提醒",
  WARNING: "警告",
  WARN: "预警",
  LEGACY_NON_ST_PIT: "旧版非 ST PIT",
  CACHE_ONLY: "仅当前缓存可用",
  PAPER_DATA_BLOCKED: "模拟盘数据阻断",
  PENDING: "等待中",
  DRAFT: "草稿",
  RETRAINING: "重训练中",
  RUNNING: "运行中",
  PAUSED: "已暂停",
  CREATED: "已创建",
  PREFLIGHTING: "预检查中",
  REPLAYING: "历史回放中",
  CATCHING_UP: "历史追赶中",
  SWITCHING_TO_LIVE: "切换实时中",
  LIVE_RUNNING: "实时运行中",
  LIVE_WAITING_FOR_BAR: "等待实时分钟线",
  LIVE_WAITING_NEXT_TRADING_DAY: "等待下一交易日",
  STOPPING: "停止中",
  STOPPED: "已停止",
  REPLAY_ONLY: "仅历史追赶",
  CATCHUP_THEN_LIVE: "追赶后自动实时",
  LIVE_ONLY: "完全实时运行",
  HISTORICAL_REPLAY: "历史追赶",
  CURRENT_DAY_CATCHUP: "当日追赶",
  LIVE_INTRADAY: "实时日内",
  DAY_FINALIZATION: "收盘结算",
  WAITING_NEXT_DAY: "等待下一交易日",
  UNSUPPORTED: "不支持",
  NOT_RUN: "未运行",
  NO_DATA: "无数据",
  DISABLED: "未启用",
  UNKNOWN: "未知",
  BUY: "买入",
  SELL: "卖出",
  FILLED: "已全部成交",
  PARTIALLY_FILLED: "部分成交",
  PARTIAL_FILLED: "部分成交",
  NEW: "新订单",
  SUBMITTED: "已提交",
  CANCELED: "已取消",
  CANCELLED: "已取消",
  EXPIRED: "已过期",
  EVENT: "事件",
  EXCLUDED: "已剔除",
  SINGLE_PACKAGE: "单策略包",
  WEIGHTED_FUSION: "加权融合",
  INTERSECTION: "交集",
  UNION: "并集",
};

export function statusLabel(status: unknown): string {
  const raw = String(status || "unknown");
  const key = raw.toUpperCase();
  return STATUS_LABELS[key] || raw;
}

export function dataSourceLabel(source: unknown): string {
  const raw = String(source || "");
  if (raw === "DB_HISTORICAL") return "历史分钟线库（DB_HISTORICAL）";
  if (raw === "TDX_REALTIME") return "TDX 实时分钟线（TDX_REALTIME）";
  return raw || "-";
}

export function packageDisplayLabel(pkg: {
  package_name?: string | null;
  package_id?: string | null;
  alpha_mode?: string | null;
  created_at?: string | null;
  manifest?: { metadata?: { name?: string | null } | null } | null;
}): string {
  const metadataName = pkg.manifest?.metadata?.name;
  const explicit = String(metadataName || pkg.package_name || "").trim();
  if (explicit) return explicit;
  return shortHash(pkg.package_id, 7);
}

export function selectionRunLabel(run: {
  trade_date?: string | null;
  mode?: string | null;
  run_id?: string | null;
}): string {
  const parts: string[] = [];
  if (run.trade_date) parts.push(String(run.trade_date));
  if (run.mode) parts.push(statusLabel(run.mode));
  if (!parts.length) return shortHash(run.run_id);
  return parts.join(" / ");
}
