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
  onLoopSelect?: (loopIndex: number) => void;
  selectedLoopIndex?: number;
}

interface LoopRow {
  loop: any;
  sourceIndex: number;
  diagnostics: LoopDiagnostics;
  rankReturn: number;
  rankDrawdown: number;
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
  onLoopSelect,
  selectedLoopIndex,
}: LoopMetricsComparisonProps) {
  if (!loops || loops.length === 0) return null;

  const rows: LoopRow[] = loops.map((loop, sourceIndex) => {
    const diagnostics = extractLoopDiagnostics(loop);
    const metrics = diagnostics.metrics || {};
    const absoluteReturns = diagnostics.absoluteReturns || {};
    const cagr = metricNumber(absoluteReturns, ["cagr", "cagr_absolute", "annualized_return_absolute"]);
    const annReturn = metricNumber(metrics, ["annualized_return", "ann_return", "return"]);
    const maxDrawdown = metricNumber(absoluteReturns, ["max_drawdown", "max_drawdown_absolute"]);

    return {
      loop,
      sourceIndex,
      diagnostics,
      rankReturn: cagr ?? annReturn ?? -Infinity,
      rankDrawdown: Math.abs(maxDrawdown ?? metricNumber(metrics, ["max_drawdown"]) ?? Infinity),
    };
  });

  const bestLoop = rows.reduce((best, current) => {
    if (current.rankReturn > best.rankReturn) return current;
    if (current.rankReturn === best.rankReturn && current.rankDrawdown < best.rankDrawdown) return current;
    return best;
  }, rows[0]);

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
          Loop 指标对比
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
              Loop {bestLoop.loop.loop_index}
            </span>
          </div>
        )}
      </div>

      <div style={{ overflowX: "auto", border: "1px solid #e5e7eb", borderRadius: "6px", backgroundColor: "#fff" }}>
        <table style={{ width: "100%", minWidth: "1700px", borderCollapse: "collapse", fontSize: "13px" }}>
          <thead>
            <tr style={{ backgroundColor: "#f1f5f9", borderBottom: "2px solid #e5e7eb" }}>
              <th style={thStyle}>Loop</th>
              <th style={thStyle}>SOTA</th>
              <th style={thStyle}>动作</th>
              <th style={thStyle}>Loop说明</th>
              <th style={thStyle}>模型</th>
              <th style={thStyle}>周期</th>
              <th style={thStyle}>HMM / 快照</th>
              <th style={{ ...thStyle, textAlign: "right" }}>CAGR</th>
              <th style={{ ...thStyle, textAlign: "right" }}>绝对 MaxDD</th>
              <th style={{ ...thStyle, textAlign: "right" }}>含成本年化</th>
              <th style={{ ...thStyle, textAlign: "right" }}>含成本 MaxDD</th>
              <th style={{ ...thStyle, textAlign: "right" }}>Sharpe</th>
              <th style={{ ...thStyle, textAlign: "right" }}>IC</th>
              <th style={{ ...thStyle, textAlign: "right" }}>平均持仓</th>
              <th style={{ ...thStyle, textAlign: "right" }}>最大持仓</th>
              <th style={{ ...thStyle, textAlign: "right" }}>结束现金</th>
              <th style={{ ...thStyle, textAlign: "right", borderRight: "none" }}>股票市值</th>
              <th style={{ ...thStyle, textAlign: "center", borderRight: "none" }}>状态</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const { loop, diagnostics } = row;
              const metrics = diagnostics.metrics || {};
              const ar = diagnostics.absoluteReturns || {};
              const pos = diagnostics.position;
              const model = diagnostics.model;
              const isBest = bestLoop?.loop?.loop_index === loop.loop_index;
              const isSelected = selectedLoopIndex === loop.loop_index;
              const cagr = metricNumber(ar, ["cagr", "cagr_absolute", "annualized_return_absolute"]);
              const absMaxDrawdown = metricNumber(ar, ["max_drawdown", "max_drawdown_absolute"]);
              const annualizedReturn = metricNumber(metrics, ["annualized_return", "ann_return"]);
              const costMaxDrawdown = metricNumber(metrics, ["max_drawdown"]);
              const sharpe = metricNumber(metrics, ["sharpe", "Sharpe"]);
              const ic = metricNumber(metrics, ["IC", "ic"]);
              const statusColors = statusStyle(loop.status);

              return (
                <tr
                  key={loop.loop_id || loop.loop_index}
                  onClick={() => onLoopSelect?.(loop.loop_index)}
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
                    L{loop.loop_index}
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
                  <td style={tdStyle}>{loop.action_type || diagnostics.model.modelId || "-"}</td>
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
                    <div style={{ fontWeight: 700, color: model.hmm.enabled ? "#166534" : "#64748b" }}>
                      {formatBool(model.hmm.enabled)}
                    </div>
                    <div title={[model.hmm.version, model.hmm.snapshot, model.hmm.signalPreset].filter(Boolean).join(" / ")} style={{ fontSize: "11px", color: "#64748b", marginTop: "2px" }}>
                      {formatShortText(model.hmm.snapshot || model.hmm.version || model.hmm.signalPreset, 24)}
                    </div>
                  </td>
                  <td style={{ ...tdRightStyle, color: cagr != null && cagr >= 0 ? "#16a34a" : "#dc2626", fontWeight: 700 }}>
                    {formatPercent(cagr, 2, true)}
                  </td>
                  <td style={{ ...tdRightStyle, color: absMaxDrawdown != null && absMaxDrawdown < -0.25 ? "#dc2626" : "#475569" }}>
                    {formatPercent(absMaxDrawdown, 2)}
                  </td>
                  <td style={tdRightStyle}>{formatPercent(annualizedReturn, 2, true)}</td>
                  <td style={tdRightStyle}>{formatPercent(costMaxDrawdown, 2)}</td>
                  <td style={tdRightStyle}>{formatDecimal(sharpe, 2)}</td>
                  <td style={tdRightStyle}>{formatDecimal(ic, 4)}</td>
                  <td style={tdRightStyle}>{formatCount(pos.avgCount, 1)}</td>
                  <td style={tdRightStyle}>{formatCount(pos.maxCount)}</td>
                  <td style={tdRightStyle}>{formatMoneyCompact(pos.finalCash)}</td>
                  <td style={{ ...tdRightStyle, borderRight: "none" }}>{formatMoneyCompact(pos.finalStockValue)}</td>
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
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <div style={{ marginTop: "10px", color: "#64748b", fontSize: "12px", lineHeight: 1.6 }}>
        持仓最小/平均/最大只从已缓存的 enhanced metrics 或已回填 holding audit 摘要读取；旧 Loop 未回填该摘要时显示 “-”，不会在页面加载时重跑实验或修改实验行为。
      </div>
    </div>
  );
}
