"use client";

import React from "react";
import { BarChart3, TrendingUp } from "lucide-react";
import {
  extractLoopDiagnostics,
  formatBool,
  formatCount,
  formatDecimal,
  formatMoneyCompact,
  formatPercent,
  formatShortText,
  type LoopDiagnostics,
} from "./loopDiagnostics";

interface LoopMetricsComparisonProps {
  loops: any[];
  taskType?: string;
  evolutionMode?: string;
  sourceType?: string;
  onLoopSelect?: (loopIndex: number) => void;
  selectedLoopIndex?: number;
}

interface LoopRow {
  loop: any;
  sourceIndex: number;
  diagnostics: LoopDiagnostics;
  rank: LoopRankMetrics;
  poolLabel: string;
  topk?: number;
  policy: Record<string, any>;
  otherMetrics: Array<{ key: string; value: string }>;
  bestEligible: boolean;
}

interface LoopRankMetrics {
  cagr?: number;
  totalReturn?: number;
  annualizedReturn?: number;
  benchmarkAnnualizedReturn?: number;
  absMaxDrawdown?: number;
  sharpe?: number;
  calmar?: number;
  informationRatio?: number;
  ic?: number;
  icir?: number;
  rankIc?: number;
  rankIcir?: number;
  topkReturn20?: number;
  topkHitRate20?: number;
  turnover?: number;
  avgCount?: number;
  maxCount?: number;
  finalCash?: number;
  finalStockValue?: number;
}

function metricNumber(metrics: Record<string, any>, keys: string[]): number | undefined {
  for (const key of keys) {
    const value = metrics?.[key];
    if (typeof value === "number" && Number.isFinite(value)) return value;
    if (typeof value === "string" && value.trim() !== "") {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) return parsed;
    }
  }
  return undefined;
}

function isFiniteMetric(value: number | undefined): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

const NON_SCALAR_METRIC_BRANCHES = new Set([
  "return_curves",
  "stock_trades",
  "top_stocks",
  "bottom_stocks",
  "all_stocks",
  "feature_importance",
  "ic_series",
  "loss_curve",
]);

function scalarMetricEntries(
  value: any,
  prefix = "",
): Array<{ key: string; value: string }> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return [];
  const entries: Array<{ key: string; value: string }> = [];
  Object.entries(value).forEach(([key, nested]) => {
    if (NON_SCALAR_METRIC_BRANCHES.has(key) || key.toLowerCase().endsWith("_json")) return;
    const path = prefix ? `${prefix}.${key}` : key;
    if (typeof nested === "number" && Number.isFinite(nested)) {
      entries.push({ key: path, value: String(nested) });
      return;
    }
    if (typeof nested === "boolean") {
      entries.push({ key: path, value: nested ? "true" : "false" });
      return;
    }
    if (typeof nested === "string" && nested.trim()) {
      entries.push({ key: path, value: nested.trim().slice(0, 160) });
      return;
    }
    if (
      Array.isArray(nested)
      && nested.length <= 20
      && nested.every((item) => ["string", "number", "boolean"].includes(typeof item))
    ) {
      entries.push({ key: path, value: nested.map(String).join(", ") });
      return;
    }
    if (nested && typeof nested === "object" && !Array.isArray(nested)) {
      entries.push(...scalarMetricEntries(nested, path));
    }
  });
  return entries;
}

function buildRankMetrics(diagnostics: LoopDiagnostics): LoopRankMetrics {
  const metrics = diagnostics.metrics || {};
  const ar = diagnostics.absoluteReturns || {};
  const pos = diagnostics.position;

  return {
    cagr: metricNumber(ar, ["cagr", "cagr_absolute", "annualized_return_absolute", "annualized_return", "annual_return"]),
    totalReturn: metricNumber(ar, ["total_return", "absolute_total_return"]),
    annualizedReturn: metricNumber(metrics, ["annualized_return", "ann_return"]),
    benchmarkAnnualizedReturn: metricNumber(metrics, ["benchmark_annualized_return", "benchmark_annual_return"]),
    absMaxDrawdown: metricNumber(ar, ["max_drawdown", "max_drawdown_absolute"]),
    sharpe: metricNumber(ar, ["sharpe", "sharpe_absolute"]) ?? metricNumber(metrics, ["sharpe", "Sharpe"]),
    calmar: metricNumber(ar, ["calmar", "calmar_ratio", "calmar_absolute"]),
    informationRatio: metricNumber(metrics, ["information_ratio"]),
    ic: metricNumber(metrics, ["IC", "ic"]),
    icir: metricNumber(metrics, ["ICIR", "icir"]),
    rankIc: metricNumber(metrics, ["Rank_IC", "rank_ic", "Rank IC"]),
    rankIcir: metricNumber(metrics, ["Rank_ICIR", "rank_icir", "Rank ICIR"]),
    topkReturn20: metricNumber(metrics, ["topk_return_20"]),
    topkHitRate20: metricNumber(metrics, ["topk_hit_rate_20"]),
    turnover: metricNumber(diagnostics.enhanced?.trade_diagnostics || {}, ["annualized_turnover", "avg_turnover"]),
    avgCount: pos.avgCount,
    maxCount: pos.maxCount,
    finalCash: pos.finalCash,
    finalStockValue: pos.finalStockValue,
  };
}

function hasAuthoritativeAbsoluteMetrics(loop: any, diagnostics: LoopDiagnostics): boolean {
  const metricContract = loop?.metrics_summary?.metric_contract
    ?? diagnostics.metrics?.metric_contract;
  const absoluteSource = metricContract?.absolute_source;
  const rawAbsolute = diagnostics.enhanced?.absolute_returns;
  return (
    absoluteSource === "enhanced_metrics.absolute_returns"
    || (
      rawAbsolute
      && typeof rawAbsolute === "object"
      && !Array.isArray(rawAbsolute)
      && Object.keys(rawAbsolute).length > 0
    )
  );
}

function hasCompleteRankMetrics(
  loop: any,
  diagnostics: LoopDiagnostics,
  rank: LoopRankMetrics,
  isCombine = false,
): boolean {
  const status = loop?.status;
  const statusEligible = !status || status === "completed";
  if (isCombine) {
    return (
      statusEligible &&
      isFiniteMetric(rank.annualizedReturn) &&
      isFiniteMetric(rank.sharpe)
    );
  }
  return (
    statusEligible &&
    hasAuthoritativeAbsoluteMetrics(loop, diagnostics) &&
    isFiniteMetric(rank.cagr) &&
    isFiniteMetric(rank.absMaxDrawdown) &&
    isFiniteMetric(rank.sharpe) &&
    isFiniteMetric(rank.avgCount) &&
    isFiniteMetric(rank.maxCount) &&
    isFiniteMetric(rank.finalCash) &&
    isFiniteMetric(rank.finalStockValue)
  );
}

function isBetterRankCandidate(current: LoopRow, best: LoopRow): boolean {
  const currentReturn = (current.rank.cagr ?? current.rank.annualizedReturn) ?? -Infinity;
  const bestReturn = (best.rank.cagr ?? best.rank.annualizedReturn) ?? -Infinity;
  if (currentReturn !== bestReturn) return currentReturn > bestReturn;

  const currentDrawdown = Math.abs(current.rank.absMaxDrawdown ?? Infinity);
  const bestDrawdown = Math.abs(best.rank.absMaxDrawdown ?? Infinity);
  if (currentDrawdown !== bestDrawdown) return currentDrawdown < bestDrawdown;

  return (current.rank.sharpe ?? -Infinity) > (best.rank.sharpe ?? -Infinity);
}

function statusText(status?: string): string {
  switch (status) {
    case "completed":
      return "完成";
    case "running":
      return "运行中";
    case "failed":
      return "失败";
    case "cancelled":
      return "取消";
    case "pending":
      return "等待";
    case "processing":
      return "处理中";
    default:
      return status || "-";
  }
}

function statusStyle(status?: string): React.CSSProperties {
  switch (status) {
    case "completed":
      return { color: "#16a34a", backgroundColor: "#dcfce7" };
    case "running":
      return { color: "#1d4ed8", backgroundColor: "#dbeafe" };
    case "failed":
      return { color: "#dc2626", backgroundColor: "#fee2e2" };
    case "cancelled":
      return { color: "#9f1239", backgroundColor: "#ffe4e6" };
    default:
      return { color: "#64748b", backgroundColor: "#f1f5f9" };
  }
}

function policyText(policy: any, countKey: string): string {
  if (!policy || typeof policy !== "object") return "请求? / 启用? / 生效?";
  const count = metricNumber(policy, [countKey]);
  return `请求${formatBool(policy.requested)} / 启用${formatBool(policy.enabled)} / 生效${formatBool(policy.effective)}${count == null ? "" : ` / ${count}`}`;
}

const thStyle: React.CSSProperties = {
  padding: "10px 12px",
  textAlign: "left",
  fontWeight: 700,
  color: "#475569",
  borderRight: "1px solid #e5e7eb",
  whiteSpace: "nowrap",
};

const tdStyle: React.CSSProperties = {
  padding: "10px 12px",
  borderRight: "1px solid #e5e7eb",
  verticalAlign: "top",
  whiteSpace: "nowrap",
};

const tdRightStyle: React.CSSProperties = {
  ...tdStyle,
  textAlign: "right",
  fontFamily: "monospace",
};

export default function LoopMetricsComparison({
  loops,
  taskType,
  evolutionMode,
  sourceType,
  onLoopSelect,
  selectedLoopIndex,
}: LoopMetricsComparisonProps) {
  if (!loops || loops.length === 0) return null;
  const normalizedType = taskType || sourceType || "evolution";
  const isCombine = normalizedType === "multi_alpha_combine";
  const showAction = normalizedType === "evolution" && (evolutionMode || "auto") === "auto";

  const rows: LoopRow[] = loops.map((loop, sourceIndex) => {
    const diagnostics = extractLoopDiagnostics(loop);
    const rank = buildRankMetrics(diagnostics);
    const configSummary = loop?.config_summary || {};
    const universe = configSummary?.universe || {};
    const poolIds = Array.isArray(universe.pool_ids) ? universe.pool_ids : [];
    const poolLabel = poolIds.length > 0
      ? poolIds.join("+")
      : (universe.label || configSummary.stock_pool || "stock_universe");

    return {
      loop,
      sourceIndex,
      diagnostics,
      rank,
      poolLabel,
      topk: metricNumber(configSummary?.strategy_params || {}, ["topk"]),
      policy: loop?.policy_summary || {},
      otherMetrics: scalarMetricEntries(diagnostics.metrics),
      bestEligible: hasCompleteRankMetrics(loop, diagnostics, rank, isCombine),
    };
  });

  const bestCandidateRows = rows.filter((row) => row.bestEligible);
  const bestLoop = bestCandidateRows.reduce<LoopRow | undefined>((best, current) => {
    if (!best) return current;
    return isBetterRankCandidate(current, best) ? current : best;
  }, undefined);

  return (
    <div style={{
      backgroundColor: "#f8fafc",
      border: "1px solid #e5e7eb",
      borderRadius: "8px",
      padding: "16px",
      marginTop: "16px",
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "12px" }}>
        <BarChart3 size={18} color="#64748b" />
        <h3 style={{ margin: 0, fontSize: "15px", fontWeight: 700, color: "#1e293b" }}>
          {isCombine ? "配置指标对比" : "Loop 指标对比"}
        </h3>
        {taskType && (
          <span style={{ fontSize: "11px", color: "#64748b", backgroundColor: "#e2e8f0", padding: "2px 8px", borderRadius: "999px" }}>
            {taskType}
          </span>
        )}
        {bestLoop && (
          <div style={{ marginLeft: "auto", fontSize: "12px", color: "#64748b", display: "flex", alignItems: "center", gap: "4px" }}>
            <span>当前最优：</span>
            <span style={{ padding: "2px 8px", backgroundColor: "#fef3c7", color: "#d97706", borderRadius: "12px", fontSize: "11px", fontWeight: 700 }}>
              {isCombine ? "配置" : "Loop"} {bestLoop.loop.loop_index}
            </span>
          </div>
        )}
      </div>

      <div style={{ overflowX: "auto", border: "1px solid #e5e7eb", borderRadius: "6px", backgroundColor: "#fff" }}>
        <table style={{ width: "100%", minWidth: showAction ? "3150px" : "3050px", borderCollapse: "collapse", fontSize: "13px" }}>
          <thead>
            <tr style={{ backgroundColor: "#f1f5f9", borderBottom: "2px solid #e5e7eb" }}>
              <th style={thStyle}>{isCombine ? "配置" : "Loop"}</th>
              <th style={thStyle}>{isCombine ? "最优配置" : "SOTA"}</th>
              {showAction && <th style={thStyle}>动作</th>}
              <th style={thStyle}>{isCombine ? "配置说明" : "Loop说明"}</th>
              <th style={thStyle}>模型</th>
              <th style={thStyle}>周期</th>
              <th style={thStyle}>股票池</th>
              <th style={{ ...thStyle, textAlign: "right" }}>TopK</th>
              <th style={thStyle}>HMM 请求/启用/生效/触发</th>
              <th style={thStyle}>黑名单 请求/启用/生效/动作</th>
              <th style={{ ...thStyle, textAlign: "right" }}>累计收益</th>
              <th style={{ ...thStyle, textAlign: "right" }}>CAGR</th>
              <th style={{ ...thStyle, textAlign: "right" }}>绝对 MaxDD</th>
              <th style={{ ...thStyle, textAlign: "right" }}>Calmar</th>
              <th style={{ ...thStyle, textAlign: "right" }}>Sharpe</th>
              <th style={{ ...thStyle, textAlign: "right" }}>基准年化</th>
              <th style={{ ...thStyle, textAlign: "right" }}>超额年化</th>
              <th style={{ ...thStyle, textAlign: "right" }}>IR</th>
              <th style={{ ...thStyle, textAlign: "right" }}>IC</th>
              <th style={{ ...thStyle, textAlign: "right" }}>ICIR</th>
              <th style={{ ...thStyle, textAlign: "right" }}>RankIC</th>
              <th style={{ ...thStyle, textAlign: "right" }}>RankICIR</th>
              <th style={{ ...thStyle, textAlign: "right" }}>Top20收益</th>
              <th style={{ ...thStyle, textAlign: "right" }}>Top20命中</th>
              <th style={{ ...thStyle, textAlign: "right" }}>换手</th>
              <th style={{ ...thStyle, textAlign: "right" }}>平均持仓</th>
              <th style={{ ...thStyle, textAlign: "right" }}>最大持仓</th>
              <th style={{ ...thStyle, textAlign: "right" }}>结束现金</th>
              <th style={{ ...thStyle, textAlign: "right", borderRight: "none" }}>股票市值</th>
              <th style={thStyle}>其他已产出指标</th>
              <th style={{ ...thStyle, textAlign: "center", borderRight: "none" }}>状态</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const { loop, diagnostics } = row;
              const pos = diagnostics.position;
              const model = diagnostics.model;
              const hmmPolicy = row.policy?.hmm;
              const blacklistPolicy = row.policy?.sector_blacklist;
              const isBest = bestLoop?.loop?.loop_index === loop.loop_index;
              const isSelected = selectedLoopIndex === loop.loop_index;
              const cagr = row.rank.cagr;
              const totalReturn = row.rank.totalReturn;
              const absMaxDrawdown = row.rank.absMaxDrawdown;
              const annualizedReturn = row.rank.annualizedReturn;
              const benchmarkAnnualizedReturn = row.rank.benchmarkAnnualizedReturn;
              const sharpe = row.rank.sharpe;
              const ic = row.rank.ic;
              const statusColors = statusStyle(loop.status);

              return (
                <tr
                  key={loop.loop_id || loop.loop_index}
                  onClick={() => onLoopSelect?.(loop.loop_index)}
                  title={row.bestEligible ? undefined : "Incomplete metrics: loop is excluded from best-loop ranking."}
                  style={{
                    cursor: onLoopSelect ? "pointer" : "default",
                    backgroundColor: isSelected ? "#eff6ff" : (row.sourceIndex % 2 === 0 ? "#fff" : "#f8fafc"),
                    borderBottom: "1px solid #e5e7eb",
                  }}
                  onMouseEnter={(e) => {
                    if (onLoopSelect && !isSelected) e.currentTarget.style.backgroundColor = "#f1f5f9";
                  }}
                  onMouseLeave={(e) => {
                    if (onLoopSelect && !isSelected) e.currentTarget.style.backgroundColor = row.sourceIndex % 2 === 0 ? "#fff" : "#f8fafc";
                  }}
                >
                  <td style={{ ...tdStyle, fontWeight: isBest ? 700 : 500 }}>
                    {isBest && <TrendingUp size={14} color="#16a34a" style={{ verticalAlign: "middle", marginRight: "4px" }} />}
                    {isCombine ? `配置 ${loop.loop_index}` : `L${loop.loop_index}`}
                  </td>
                  <td style={tdStyle}>
                    <span style={{
                      display: "inline-block",
                      minWidth: "36px",
                      textAlign: "center",
                      padding: "2px 7px",
                      borderRadius: "999px",
                      backgroundColor: loop.is_sota ? "#fef3c7" : "#f1f5f9",
                      color: loop.is_sota ? "#d97706" : "#64748b",
                      fontWeight: 700,
                      fontSize: "11px",
                    }}>
                      {loop.is_sota ? "是" : "否"}
                    </span>
                  </td>
                  {showAction && <td style={tdStyle}>{loop.action_type || diagnostics.model.modelId || "-"}</td>}
                  <td
                    title={diagnostics.comment.fullText}
                    style={{ ...tdStyle, minWidth: "220px", maxWidth: "320px", whiteSpace: "normal", lineHeight: 1.45, color: "#334155" }}
                  >
                    {diagnostics.comment.source === "fallback" ? "-" : diagnostics.comment.shortText}
                  </td>
                  <td style={tdStyle}>
                    <div style={{ fontFamily: "monospace", fontWeight: 700, color: "#334155" }}>
                      {formatShortText(model.modelId || model.modelType, 22)}
                    </div>
                    <div style={{ fontSize: "11px", color: "#64748b", marginTop: "2px" }}>
                      {model.modelType || "-"}
                    </div>
                  </td>
                  <td style={tdStyle}>{model.labelHorizon || "-"}</td>
                  <td style={tdStyle}>
                    <div style={{ fontWeight: 700, color: "#334155" }}>{row.poolLabel}</div>
                    {loop?.config_summary?.universe?.protocol_status && (
                      <div style={{ fontSize: "10px", color: "#b45309", marginTop: "2px" }}>历史非Top20，仅展示</div>
                    )}
                  </td>
                  <td style={tdRightStyle}>{formatCount(row.topk)}</td>
                  <td style={tdStyle}>
                    <div style={{ fontWeight: 700, color: hmmPolicy?.enabled ? "#166534" : "#64748b" }}>
                      {policyText(hmmPolicy, "trigger_count")}
                    </div>
                    <div title={[model.hmm.version, model.hmm.snapshot, model.hmm.signalPreset].filter(Boolean).join(" / ")} style={{ fontSize: "11px", color: "#64748b", marginTop: "2px" }}>
                      {formatShortText(model.hmm.snapshot || model.hmm.version || model.hmm.signalPreset, 24)}
                    </div>
                  </td>
                  <td style={tdStyle}>{policyText(blacklistPolicy, "action_count")}</td>
                  <td style={tdRightStyle}>{formatPercent(totalReturn, 2, true)}</td>
                  <td style={{ ...tdRightStyle, color: cagr != null && cagr >= 0 ? "#16a34a" : "#dc2626", fontWeight: 700 }}>
                    {formatPercent(cagr, 2, true)}
                  </td>
                  <td style={{ ...tdRightStyle, color: absMaxDrawdown != null && absMaxDrawdown < -0.25 ? "#dc2626" : "#475569" }}>
                    {formatPercent(absMaxDrawdown, 2)}
                  </td>
                  <td style={tdRightStyle}>{formatDecimal(row.rank.calmar, 2)}</td>
                  <td style={tdRightStyle}>{formatDecimal(sharpe, 2)}</td>
                  <td style={tdRightStyle}>{formatPercent(benchmarkAnnualizedReturn, 2, true)}</td>
                  <td style={tdRightStyle}>{formatPercent(annualizedReturn, 2, true)}</td>
                  <td style={tdRightStyle}>{formatDecimal(row.rank.informationRatio, 2)}</td>
                  <td style={tdRightStyle}>{formatDecimal(ic, 4)}</td>
                  <td style={tdRightStyle}>{formatDecimal(row.rank.icir, 3)}</td>
                  <td style={tdRightStyle}>{formatDecimal(row.rank.rankIc, 4)}</td>
                  <td style={tdRightStyle}>{formatDecimal(row.rank.rankIcir, 3)}</td>
                  <td style={tdRightStyle}>{formatPercent(row.rank.topkReturn20, 2, true)}</td>
                  <td style={tdRightStyle}>{formatPercent(row.rank.topkHitRate20, 2)}</td>
                  <td style={tdRightStyle}>{formatDecimal(row.rank.turnover, 2)}</td>
                  <td style={tdRightStyle}>{formatCount(pos.avgCount, 1)}</td>
                  <td style={tdRightStyle}>{formatCount(pos.maxCount)}</td>
                  <td style={tdRightStyle}>{formatMoneyCompact(pos.finalCash)}</td>
                  <td style={{ ...tdRightStyle, borderRight: "none" }}>{formatMoneyCompact(pos.finalStockValue)}</td>
                  <td style={{ ...tdStyle, minWidth: "210px", whiteSpace: "normal" }}>
                    {row.otherMetrics.length > 0 ? (
                      <details>
                        <summary style={{ cursor: "pointer", color: "#2563eb", fontWeight: 700 }}>
                          {row.otherMetrics.length} 项标量
                        </summary>
                        <div style={{ marginTop: "6px", maxHeight: "260px", overflowY: "auto", fontSize: "11px", lineHeight: 1.55 }}>
                          {row.otherMetrics.map((entry) => (
                            <div key={entry.key} title={`${entry.key}: ${entry.value}`}>
                              <span style={{ color: "#64748b" }}>{entry.key}: </span>
                              <span style={{ color: "#0f172a", fontFamily: "monospace" }}>{entry.value}</span>
                            </div>
                          ))}
                        </div>
                      </details>
                    ) : "-"}
                  </td>
                  <td style={{ ...tdStyle, textAlign: "center", borderRight: "none" }}>
                    <span style={{
                      display: "inline-block",
                      padding: "2px 8px",
                      borderRadius: "999px",
                      fontSize: "11px",
                      fontWeight: 700,
                      ...statusColors,
                    }}>
                      {statusText(loop.status)}
                    </span>
                    {!row.bestEligible && (
                      <div style={{ marginTop: "4px", color: "#b45309", fontSize: "10px", fontWeight: 700 }}>
                        Incomplete metrics
                      </div>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <div style={{ marginTop: "10px", color: "#64748b", fontSize: "12px", lineHeight: 1.6 }}>
        {isCombine
          ? "配置对比仅读取 multi-alpha combine-backtest 结果表；IC/持仓类 QE 字段无组合语义时显示 “-”，不会触发重跑。"
          : "持仓最小/平均/最大只从已缓存的 enhanced metrics 或已回填 holding audit 摘要读取；旧 Loop 未回填该摘要时显示 “-”，不会在页面加载时重跑实验或修改实验行为。"}
      </div>
      <div style={{ marginTop: "6px", color: "#64748b", fontSize: "12px", lineHeight: 1.6 }}>
        {isCombine
          ? "最优配置只在 completed 配置中按 CAGR/Sharpe 评选，failed/partial_failed/running 不参评。"
          : "Best-loop selection only ranks completed loops with CAGR, absolute MaxDD, Sharpe, average/max holdings, ending cash, and stock market value."}
      </div>
    </div>
  );
}
