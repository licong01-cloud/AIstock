"use client";

import React, { useEffect, useState, useMemo } from "react";

type PlotlyComponent = React.ComponentType<any>;

function PlotlyChart(props: any) {
  const [Plot, setPlot] = React.useState<PlotlyComponent | null>(null);

  React.useEffect(() => {
    let cancelled = false;
    import("react-plotly.js")
      .then((module) => {
        if (!cancelled) setPlot(() => module.default as PlotlyComponent);
      })
      .catch((error) => console.error("Failed to load Plotly chart", error));
    return () => {
      cancelled = true;
    };
  }, []);

  if (!Plot) {
    const placeholderHeight = props?.style?.height ?? props?.layout?.height ?? 260;
    return (
      <div style={{ height: placeholderHeight, display: "flex", alignItems: "center", justifyContent: "center", color: "#94a3b8", fontSize: 12 }}>
        Loading chart...
      </div>
    );
  }

  return <Plot {...props} />;
}


const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface TrendPoint {
  snapshot_date: string;
  ic_mean: number | null;
  rank_ic_mean: number | null;
  icir: number | null;
  rank_icir: number | null;
  ic_positive_ratio: number | null;
  rank_ic_1d: number | null;
  rank_ic_5d: number | null;
  rank_ic_10d: number | null;
  rank_ic_20d: number | null;
  top_annual_return: number | null;
  top_excess_annual_return: number | null;
  top_sharpe: number | null;
  group_return_monotonicity: number | null;
  n_trading_days: number | null;
}

interface ICDecayTrendChartProps {
  factorName: string;
  evalWindow?: string;
}

export default function ICDecayTrendChart({ factorName, evalWindow = "full" }: ICDecayTrendChartProps) {
  const [trend, setTrend] = useState<TrendPoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!factorName) return;
    setLoading(true);
    setError(null);
    fetch(`${API}/quantevolver/official-evaluation/factors/${encodeURIComponent(factorName)}/ic-decay?eval_window=${evalWindow}`)
      .then(res => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then(data => setTrend(data.trend || []))
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, [factorName, evalWindow]);

  const { traces, layout } = useMemo(() => {
    if (trend.length === 0) return { traces: [], layout: {} };

    const dates = trend.map(t => t.snapshot_date);

    const traces = [
      {
        x: dates,
        y: trend.map(t => t.rank_ic_mean),
        name: "Rank IC",
        type: "scatter",
        mode: "lines+markers",
        line: { color: "#8b5cf6", width: 2 },
        marker: { size: 6 },
      },
      {
        x: dates,
        y: trend.map(t => t.ic_mean),
        name: "IC",
        type: "scatter",
        mode: "lines+markers",
        line: { color: "#3b82f6", width: 2, dash: "dot" },
        marker: { size: 5 },
      },
      {
        x: dates,
        y: trend.map(t => t.rank_icir),
        name: "Rank ICIR",
        type: "scatter",
        mode: "lines+markers",
        yaxis: "y2",
        line: { color: "#10b981", width: 2 },
        marker: { size: 5 },
      },
      {
        x: dates,
        y: trend.map(t => t.ic_positive_ratio != null ? t.ic_positive_ratio * 100 : null),
        name: "IC 胜率%",
        type: "scatter",
        mode: "lines+markers",
        yaxis: "y3",
        line: { color: "#f59e0b", width: 1.5, dash: "dash" },
        marker: { size: 4 },
        visible: "legendonly" as const,
      },
    ];

    const layout = {
      height: 300,
      margin: { t: 30, r: 80, b: 50, l: 55 },
      xaxis: {
        title: "快照日期",
        tickangle: -45,
        type: "category" as const,
      },
      yaxis: {
        title: "IC / Rank IC",
        side: "left" as const,
        zeroline: true,
        zerolinecolor: "#e5e7eb",
      },
      yaxis2: {
        title: "ICIR",
        overlaying: "y" as const,
        side: "right" as const,
        showgrid: false,
      },
      yaxis3: {
        title: "胜率%",
        overlaying: "y" as const,
        side: "right" as const,
        showgrid: false,
        position: 0.95,
        visible: false,
      },
      legend: {
        orientation: "h" as const,
        y: -0.25,
        x: 0.5,
        xanchor: "center" as const,
      },
      hovermode: "x unified" as const,
    };

    return { traces, layout };
  }, [trend]);

  if (loading) return <p style={{ fontSize: 12, color: "#9ca3af", padding: 8 }}>加载衰变趋势...</p>;
  if (error) return <p style={{ fontSize: 12, color: "#ef4444", padding: 8 }}>加载失败: {error}</p>;
  if (trend.length < 2) return <p style={{ fontSize: 12, color: "#9ca3af", padding: 8 }}>需要至少 2 个快照时间点才能展示衰变趋势</p>;

  return (
    <div>
      <PlotlyChart
        data={traces}
        layout={layout}
        config={{ displayModeBar: false, responsive: true }}
        style={{ width: "100%" }}
      />
    </div>
  );
}
