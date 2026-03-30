"use client";

import React from "react";

interface MetricDef {
  key: string;
  label: string;
  fmt: (v: number) => string;
  color: string;
}

const DEFAULT_METRICS: MetricDef[] = [
  { key: "IC", label: "IC", fmt: (v) => v.toFixed(4), color: "#2563eb" },
  { key: "annualized_return", label: "年化收益", fmt: (v) => (v * 100).toFixed(2) + "%", color: "#10b981" },
  { key: "max_drawdown", label: "最大回撤", fmt: (v) => (v * 100).toFixed(2) + "%", color: "#ef4444" },
  { key: "sharpe", label: "Sharpe", fmt: (v) => v.toFixed(3), color: "#8b5cf6" },
];

interface MetricsSummaryProps {
  metrics: Record<string, any> | null;
  /** 自定义指标定义列表，默认为 IC/年化收益/最大回撤/Sharpe */
  metricDefs?: MetricDef[];
  /** 背景色，默认 #f0fdf4 */
  background?: string;
  /** 边框色，默认 #86efac */
  borderColor?: string;
}

export default function MetricsSummary({
  metrics,
  metricDefs = DEFAULT_METRICS,
  background = "#f0fdf4",
  borderColor = "#86efac",
}: MetricsSummaryProps) {
  if (!metrics) return null;

  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: `repeat(${metricDefs.length}, 1fr)`,
      gap: 12,
      padding: 16,
      backgroundColor: background,
      borderRadius: 8,
      border: `1px solid ${borderColor}`,
    }}>
      {metricDefs.map(m => {
        const raw = metrics[m.key];
        const numVal = raw != null ? (typeof raw === "number" ? raw : parseFloat(raw)) : null;
        const valid = numVal != null && !isNaN(numVal);
        return (
          <div key={m.key} style={{ textAlign: "center" }}>
            <div style={{
              fontSize: 18, fontWeight: 700, fontFamily: "monospace",
              color: valid ? m.color : "#d1d5db",
            }}>
              {valid ? m.fmt(numVal!) : "-"}
            </div>
            <div style={{ fontSize: 11, color: "#6b7280", marginTop: 2 }}>{m.label}</div>
          </div>
        );
      })}
    </div>
  );
}
