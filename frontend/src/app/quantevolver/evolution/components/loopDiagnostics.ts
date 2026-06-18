"use client";

type AnyRecord = Record<string, any>;

export interface LoopLike {
  loop_index?: number;
  action_type?: string;
  config_json?: AnyRecord | null;
  metrics_json?: AnyRecord | null;
  metrics_summary?: AnyRecord | null;
  sota_metric_summary?: AnyRecord | null;
  agent_analysis?: AnyRecord | string | null;
  is_sota?: boolean;
  status?: string;
  [key: string]: any;
}

export interface LoopModelDiagnostics {
  modelId?: string;
  modelType?: string;
  labelHorizon?: string;
  customFactorCount?: number;
  alpha158Enabled?: boolean;
  hmm: {
    enabled?: boolean;
    version?: string;
    snapshot?: string;
    signalPreset?: string;
  };
}

export interface LoopPositionDiagnostics {
  minCount?: number;
  avgCount?: number;
  maxCount?: number;
  p95Count?: number;
  finalStockCount?: number;
  finalCash?: number;
  finalStockValue?: number;
  finalTotalValue?: number;
  finalCashRatio?: number;
}

export interface LoopComment {
  shortText: string;
  fullText: string;
  source: string;
}

export interface LoopDiagnostics {
  metrics: AnyRecord;
  enhanced: AnyRecord;
  absoluteReturns: AnyRecord;
  model: LoopModelDiagnostics;
  position: LoopPositionDiagnostics;
  comment: LoopComment;
}

const ACTION_FALLBACKS: Record<string, string> = {
  initial: "初始配置基线，用于后续 Loop 横向对比。",
  factor_adjust: "因子调整 Loop，重点观察信号质量、收益和回撤是否同步改善。",
  param_tune: "参数调优 Loop，重点观察持仓、换手、收益和回撤的平衡变化。",
  model_switch: "模型切换 Loop，重点比较模型结构带来的收益和风险差异。",
  factor_model_joint: "因子与模型联合调整 Loop，重点关注收益提升是否来自稳定信号而非偶然过拟合。",
};

function asRecord(value: any): AnyRecord | undefined {
  return value && typeof value === "object" && !Array.isArray(value) ? value : undefined;
}

function hasKeys(value: AnyRecord | undefined): value is AnyRecord {
  return !!value && Object.keys(value).length > 0;
}

function readPath(obj: AnyRecord | undefined, path: string): any {
  if (!obj) return undefined;
  return path.split(".").reduce<any>((acc, key) => {
    if (acc == null) return undefined;
    return acc[key];
  }, obj);
}

function firstValue(sources: Array<AnyRecord | undefined>, keys: string[]): any {
  for (const source of sources) {
    for (const key of keys) {
      const value = readPath(source, key);
      if (value !== undefined && value !== null && value !== "") {
        return value;
      }
    }
  }
  return undefined;
}

function firstNumber(sources: Array<AnyRecord | undefined>, keys: string[]): number | undefined {
  const value = firstValue(sources, keys);
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value.replace(/,/g, ""));
    if (Number.isFinite(parsed)) return parsed;
  }
  return undefined;
}

function firstBoolean(sources: Array<AnyRecord | undefined>, keys: string[]): boolean | undefined {
  const value = firstValue(sources, keys);
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["true", "1", "yes", "y", "on", "enabled"].includes(normalized)) return true;
    if (["false", "0", "no", "n", "off", "disabled"].includes(normalized)) return false;
  }
  return undefined;
}

function tradeAction(value: any): "buy" | "sell" | undefined {
  const text = String(value ?? "").trim().toLowerCase();
  if (!text) return undefined;
  if (["buy", "b", "open", "long", "entry"].includes(text) || text.includes("buy")) return "buy";
  if (["sell", "s", "close", "exit"].includes(text) || text.includes("sell")) return "sell";
  return undefined;
}

function curveDates(enhanced: AnyRecord): string[] {
  const curves = asRecord(enhanced.return_curves);
  const dates = Array.isArray(curves?.dates) ? curves?.dates : [];
  return dates.map((item) => String(item).slice(0, 10)).filter(Boolean);
}

function derivePositionFromStockTrades(enhanced: AnyRecord): LoopPositionDiagnostics {
  const stockTrades = asRecord(enhanced.stock_trades);
  if (!stockTrades) return {};

  const events: Record<string, Array<{ action: "buy" | "sell"; symbol: string }>> = {};
  Object.entries(stockTrades).forEach(([symbol, trades]) => {
    if (!Array.isArray(trades)) return;
    trades.forEach((trade) => {
      const row = asRecord(trade);
      if (!row) return;
      const date = row.date ?? row.datetime ?? row.trade_date;
      const action = tradeAction(row.type ?? row.side ?? row.action);
      if (!date || !action) return;
      const key = String(date).slice(0, 10);
      events[key] = events[key] || [];
      events[key].push({ action, symbol });
    });
  });

  const eventDates = Object.keys(events);
  if (eventDates.length === 0) return {};

  const orderedCurveDates = curveDates(enhanced);
  const countDates = orderedCurveDates.length > 0 ? orderedCurveDates : eventDates.sort();
  const active = new Set<string>();
  const counts: number[] = [];
  Array.from(new Set([...countDates, ...eventDates])).sort().forEach((date) => {
    (events[date] || []).forEach(({ action, symbol }) => {
      if (action === "buy") active.add(symbol);
      if (action === "sell") active.delete(symbol);
    });
    if (countDates.includes(date) || events[date]) counts.push(active.size);
  });

  if (counts.length === 0) return {};
  const sorted = [...counts].sort((a, b) => a - b);
  const p95Index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * 0.95) - 1));
  return {
    minCount: Math.min(...counts),
    avgCount: counts.reduce((sum, value) => sum + value, 0) / counts.length,
    maxCount: Math.max(...counts),
    p95Count: sorted[p95Index],
    finalStockCount: counts[counts.length - 1],
  };
}

function toText(value: any): string | undefined {
  if (value === undefined || value === null || value === "") return undefined;
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (typeof value === "object") {
    const record = asRecord(value);
    const named = record?.name ?? record?.id ?? record?.model_id ?? record?.class ?? record?.class_name;
    if (named !== undefined && named !== null && named !== "") return String(named);
  }
  return undefined;
}

function getEnhancedMetrics(loop: LoopLike, overrideEnhanced?: AnyRecord | null): AnyRecord {
  const override = asRecord(overrideEnhanced);
  if (hasKeys(override)) {
    if (hasKeys(asRecord(override.enhanced_metrics))) return asRecord(override.enhanced_metrics)!;
    return override;
  }

  const metrics = asRecord(loop.metrics_json);
  const enhanced = asRecord(metrics?.enhanced_metrics);
  return enhanced || {};
}

function mergeRecords(...records: Array<AnyRecord | undefined>): AnyRecord {
  return records.reduce<AnyRecord>((acc, record) => {
    if (record) Object.assign(acc, record);
    return acc;
  }, {});
}

function inferModelType(modelId?: string): string | undefined {
  if (!modelId) return undefined;
  const upper = modelId.toUpperCase();
  if (upper.includes("LIGHTGBM") || upper.includes("LGB")) return "LGB";
  if (upper.includes("CATBOOST")) return "CatBoost";
  if (upper.includes("XGBOOST") || upper.includes("XGB")) return "XGBoost";
  if (upper.includes("ALSTM")) return "ALSTM";
  if (upper.includes("GRU")) return "GRU";
  if (upper.includes("LSTM")) return "LSTM";
  if (upper.includes("TCN")) return "TCN";
  if (upper.includes("TRANSFORMER")) return "Transformer";
  if (upper.includes("PTNN") || upper.includes("NEURAL")) return "PTNN";
  return undefined;
}

function normalizeHorizon(value: any): string | undefined {
  if (value === undefined || value === null || value === "") return undefined;
  if (typeof value === "number" && Number.isFinite(value)) return `${Math.round(value)}D`;
  const text = String(value).trim();
  if (!text) return undefined;

  const dayMatch = text.match(/(\d+)\s*(?:d|D|day|days|日)/);
  if (dayMatch) return `${Number(dayMatch[1])}D`;

  const numeric = Number(text);
  if (Number.isFinite(numeric)) return `${Math.round(numeric)}D`;

  const refs = Array.from(text.matchAll(/Ref\(\$close,\s*-(\d+)\)/g)).map((m) => Number(m[1]));
  if (refs.length >= 2) {
    const horizon = Math.max(...refs) - Math.min(...refs);
    if (horizon > 0) return `${horizon}D`;
  }
  if (refs.length === 1 && refs[0] > 1) return `${refs[0] - 1}D`;

  return text.length <= 16 ? text : undefined;
}

function truncateText(text: string, maxLength: number): string {
  const normalized = text.replace(/\s+/g, " ").trim();
  if (normalized.length <= maxLength) return normalized;
  return `${normalized.slice(0, maxLength - 1)}…`;
}

function pickCommentText(loop: LoopLike): LoopComment {
  const agent = asRecord(loop.agent_analysis);
  const cfg = asRecord(loop.config_json);
  const action = loop.action_type || cfg?.action_type || "initial";

  const sources: Array<{ source: string; value: any }> = [
    { source: "config.label", value: cfg?.label },
    { source: "config.loop_description", value: cfg?.loop_description ?? cfg?.loop_desc ?? cfg?.loop_note },
    { source: "config.comment", value: cfg?.comment ?? cfg?.note ?? cfg?.description },
    { source: "direction.reason", value: agent?.direction?.reason },
    { source: "direction.rationale", value: agent?.direction?.rationale },
    { source: "direction.summary", value: agent?.direction?.summary },
    { source: "analyst.summary", value: agent?.analyst?.summary },
    { source: "analyst.report_text", value: agent?.analyst?.report_text },
    { source: "researcher.reason", value: agent?.researcher?.reason },
    { source: "researcher.summary", value: agent?.researcher?.summary },
    { source: "evaluator.reason", value: agent?.evaluator?.reason },
    { source: "evaluator.summary", value: agent?.evaluator?.summary },
  ];

  for (const item of sources) {
    const text = toText(item.value);
    if (text && text.trim().length >= 4) {
      return {
        source: item.source,
        fullText: text.trim(),
        shortText: truncateText(text, 118),
      };
    }
  }

  const fallback = ACTION_FALLBACKS[action] || `动作类型 ${action}，请结合模型、因子、持仓与收益指标判断效果。`;
  return { source: "fallback", fullText: fallback, shortText: fallback };
}

export function extractLoopDiagnostics(loop: LoopLike, overrideEnhanced?: AnyRecord | null): LoopDiagnostics {
  const cfg = asRecord(loop.config_json) || {};
  const loopTopLevelMetrics: AnyRecord = {
    cagr: loop.cagr,
    annualized_return: loop.annualized_return,
    annual_return: loop.annual_return,
    max_drawdown: loop.max_drawdown,
    calmar: loop.calmar,
    ic: loop.ic,
    IC: loop.IC,
    rank_ic: loop.rank_ic,
    Rank_IC: loop.Rank_IC,
    icir: loop.icir,
    information_ratio: loop.information_ratio,
    sharpe: loop.sharpe,
  };
  const baseSummary = asRecord(loop.metrics_summary);
  const sotaPrimary = asRecord(asRecord(loop.sota_metric_summary)?.primary);
  const loopBaseMetrics = mergeRecords(
    loopTopLevelMetrics,
    baseSummary,
    asRecord(loop.sota_metric_summary),
    sotaPrimary,
  );
  const metrics = mergeRecords(
    loopBaseMetrics,
    asRecord(loop.metrics_json),
  );
  const enhanced = getEnhancedMetrics(loop, overrideEnhanced);

  const absoluteReturns = mergeRecords(
    loopBaseMetrics,
    asRecord(enhanced.summary),
    asRecord(metrics.summary),
    asRecord(enhanced.absolute_returns),
    asRecord(metrics.absolute_returns),
  );

  const holdingSummary = mergeRecords(
    asRecord(enhanced.holding_audit),
    asRecord(enhanced.position_diagnostics),
    asRecord(enhanced.position_summary),
    asRecord(metrics.holding_audit),
    asRecord(metrics.position_diagnostics),
    asRecord(metrics.position_summary),
  );

  const customParams = asRecord(cfg.custom_params);
  const modelParams = asRecord(cfg.model_params);
  const strategyParams = asRecord(cfg.strategy_params);
  const datasetParams = asRecord(cfg.dataset_params);
  const runtimeProfile = asRecord(cfg.runtime_profile);
  const hmmObject = asRecord(cfg.hmm) || asRecord(customParams?.hmm) || asRecord(strategyParams?.hmm) || asRecord(runtimeProfile?.hmm);

  const modelSources = [cfg, modelParams, customParams, datasetParams, enhanced, metrics];
  const modelId = toText(firstValue(modelSources, [
    "model_id",
    "model_name",
    "model",
    "model.class",
    "model.class_name",
    "model_type",
  ]));
  const explicitModelType = toText(firstValue(modelSources, [
    "model_type",
    "model_class",
    "model_name",
    "class",
    "class_name",
  ]));
  const modelType = explicitModelType || inferModelType(modelId);
  const labelHorizon = normalizeHorizon(firstValue(modelSources, [
    "label_horizon_days",
    "label_horizon",
    "train_horizon_days",
    "training_horizon_days",
    "pred_horizon",
    "horizon",
    "label",
    "label_expr",
  ]));

  const hmmSpecificSources = [hmmObject, runtimeProfile?.hmm, customParams?.hmm, strategyParams?.hmm, modelParams?.hmm];
  const hmmGeneralSources = [cfg, customParams, strategyParams, modelParams, enhanced, metrics];
  let hmmEnabled = firstBoolean(hmmSpecificSources, ["enabled", "hmm_enabled", "enable_sector_hmm", "enable_hmm", "use_hmm"]);
  if (hmmEnabled === undefined) {
    hmmEnabled = firstBoolean(hmmGeneralSources, ["enable_sector_hmm", "hmm_enabled", "enable_hmm", "use_hmm"]);
  }
  const hmmVersion = toText(firstValue([...hmmSpecificSources, ...hmmGeneralSources], [
    "version",
    "hmm_version",
    "hmm_config_id",
    "config_id",
    "hmm_model_version",
  ]));
  const hmmSnapshot = toText(firstValue([...hmmSpecificSources, ...hmmGeneralSources], [
    "model_snapshot_id",
    "snapshot_id",
    "hmm_snapshot_id",
    "hmm_model_snapshot_id",
    "snapshot",
    "hmm_snapshot",
  ]));
  const hmmSignalPreset = toText(firstValue([...hmmSpecificSources, ...hmmGeneralSources], [
    "signal_preset",
    "hmm_signal_preset",
    "preset",
  ]));
  if (hmmEnabled === undefined && (hmmVersion || hmmSnapshot || hmmSignalPreset)) {
    hmmEnabled = true;
  }

  const derivedPosition = derivePositionFromStockTrades(enhanced);
  const positionSources = [holdingSummary, absoluteReturns, enhanced, metrics];
  const finalTotalValue = firstNumber(positionSources, [
    "final_total_value",
    "final_account_value",
    "final_total_account",
    "final_account",
    "final_nav_value",
  ]);
  const finalCash = firstNumber(positionSources, ["final_cash", "final_cash_amount", "ending_cash", "end_cash", "cash"]);
  const position: LoopPositionDiagnostics = {
    minCount: firstNumber(positionSources, ["position_count_min", "min_position_count", "holding_count_min", "min_holding_count", "min_holdings"]) ?? derivedPosition.minCount,
    avgCount: firstNumber(positionSources, ["position_count_avg", "avg_position_count", "holding_count_avg", "avg_holding_count", "average_holding_count", "avg_holdings", "average_holdings"]) ?? derivedPosition.avgCount,
    maxCount: firstNumber(positionSources, ["position_count_max", "max_position_count", "holding_count_max", "max_holding_count", "max_holdings"]) ?? derivedPosition.maxCount,
    p95Count: firstNumber(positionSources, ["position_count_p95", "p95_position_count", "holding_count_p95", "p95_holding_count"]) ?? derivedPosition.p95Count,
    finalStockCount: firstNumber(positionSources, ["final_stock_count", "final_position_count", "end_position_count"]) ?? derivedPosition.finalStockCount,
    finalCash,
    finalStockValue: firstNumber(positionSources, ["final_stock_value", "final_stock_market_value", "ending_stock_market_value", "end_stock_market_value", "final_value", "stock_market_value"]),
    finalTotalValue,
    finalCashRatio: firstNumber(positionSources, ["final_cash_ratio"]) ?? (
      finalCash !== undefined && finalTotalValue !== undefined && finalTotalValue !== 0 ? finalCash / finalTotalValue : undefined
    ),
  };

  const factorList = Array.isArray(cfg.factor_list) ? cfg.factor_list : [];
  const alpha158Enabled = firstBoolean([cfg, customParams, modelParams], ["use_alpha158", "alpha158", "enable_alpha158", "use_alpha_baseline"]);

  return {
    metrics,
    enhanced,
    absoluteReturns,
    model: {
      modelId,
      modelType,
      labelHorizon,
      customFactorCount: factorList.length,
      alpha158Enabled,
      hmm: {
        enabled: hmmEnabled,
        version: hmmVersion,
        snapshot: hmmSnapshot,
        signalPreset: hmmSignalPreset,
      },
    },
    position,
    comment: pickCommentText(loop),
  };
}

export function formatPercent(value: number | undefined | null, digits = 2, signed = false): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  const percent = value * 100;
  const sign = signed && percent > 0 ? "+" : "";
  return `${sign}${percent.toFixed(digits)}%`;
}

export function formatDecimal(value: number | undefined | null, digits = 2): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  return value.toFixed(digits);
}

export function formatCount(value: number | undefined | null, digits = 0): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  return digits > 0 ? value.toFixed(digits) : String(Math.round(value));
}

export function formatMoneyCompact(value: number | undefined | null): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  const abs = Math.abs(value);
  if (abs >= 100000000) return `¥${(value / 100000000).toFixed(2)}亿`;
  if (abs >= 10000) return `¥${(value / 10000).toFixed(2)}万`;
  return `¥${value.toFixed(2)}`;
}

export function formatBool(value: boolean | undefined): string {
  if (value === true) return "启用";
  if (value === false) return "未启用";
  return "-";
}

export function formatShortText(value: string | undefined, maxLength = 20): string {
  if (!value) return "-";
  return truncateText(value, maxLength);
}
