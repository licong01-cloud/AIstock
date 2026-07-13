"use client";

import React, { useMemo } from "react";

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


interface TrajectoryEntry {
  loop_id: number;
  label?: string;
  ic?: number | null;
  rank_ic?: number | null;
  icir?: number | null;
  ann_ret?: number | null;
  sharpe?: number | null;
  max_drawdown?: number | null;
  is_sota?: boolean;
}

interface SotaEntry {
  loop_id: number;
  label?: string;
  ic?: number | null;
  ann_ret?: number | null;
}

interface MetricsTrajectoryChartProps {
  trajectory: TrajectoryEntry[];
  sota_history?: SotaEntry[];
  mode?: "evolution" | "combine";
}

const LAYOUT = {
  autosize: true,
  height: 320,
  margin: { t: 30, r: 120, b: 40, l: 50 },
  xaxis: { dtick: 1, domain: [0, 0.85], tickprefix: "Loop " },
  yaxis: { title: "IC / Rank IC", side: "left" as const },
  yaxis2: {
    title: "收益 %",
    overlaying: "y" as const, side: "right" as const,
    showgrid: false,
    ticksuffix: "%",
    tickfont: { color: "#10b981" },
    titlefont: { color: "#10b981" },
  },
  yaxis3: {
    title: "回撤 %",
    overlaying: "y" as const, side: "right" as const,
    anchor: "free" as const, position: 0.95,
    showgrid: false,
    ticksuffix: "%",
    tickfont: { color: "#ef4444" },
    titlefont: { color: "#ef4444" },
  },
  legend: { orientation: "h" as const, y: 1.15 },
  font: { size: 11 },
};

const CONFIG = { responsive: true, displayModeBar: false };
const plotStyle = { width: "100%", height: "320px", minHeight: "320px" };

export default React.memo(function MetricsTrajectoryChart({
  trajectory,
  sota_history,
  mode = "evolution",
}: MetricsTrajectoryChartProps) {
  const traces = useMemo(() => {
    if (!trajectory || trajectory.length === 0) return null;

    const isCombine = mode === "combine";
    const loops = isCombine ? trajectory.map((t) => t.label || `配置 ${t.loop_id}`) : trajectory.map((t) => t.loop_id);
    const traceMode = isCombine ? "markers" : "lines+markers";
    const result: any[] = [];

    const icVals = trajectory.map((t) => t.ic ?? null);
    if (icVals.some((v) => v != null)) {
      result.push({
        x: loops, y: icVals,
        type: "scatter", mode: traceMode,
        name: "IC", yaxis: "y",
        line: { color: "#3b82f6", width: 2 },
        marker: { size: 5 },
        hovertemplate: "%{y:.4f}<extra></extra>",
      });
    }

    const ricVals = trajectory.map((t) => t.rank_ic ?? null);
    if (ricVals.some((v) => v != null)) {
      result.push({
        x: loops, y: ricVals,
        type: "scatter", mode: traceMode,
        name: "Rank IC", yaxis: "y",
        line: { color: "#8b5cf6", width: 2 },
        marker: { size: 5 },
        hovertemplate: "%{y:.4f}<extra></extra>",
      });
    }

    const retVals = trajectory.map((t) =>
      t.ann_ret != null ? t.ann_ret * 100 : null
    );
    if (retVals.some((v) => v != null)) {
      result.push({
        x: loops, y: retVals,
        type: "scatter", mode: traceMode,
        name: "年化收益", yaxis: "y2",
        line: { color: "#10b981", width: 2, dash: "dot" },
        marker: { size: 5 },
        hovertemplate: "%{y:.2f}%<extra></extra>",
      });
    }

    // 最大回撤：保留原始负数，转为百分比显示（越低=回撤越深）
    const ddVals = trajectory.map((t) =>
      t.max_drawdown != null ? t.max_drawdown * 100 : null
    );
    if (ddVals.some((v) => v != null)) {
      result.push({
        x: loops, y: ddVals,
        type: "scatter", mode: traceMode,
        name: "最大回撤", yaxis: "y3",
        line: { color: "#ef4444", width: 2, dash: "dashdot" },
        marker: { size: 5, symbol: "triangle-down" },
        hovertemplate: "%{y:.2f}%<extra></extra>",
      });
    }

    if (sota_history && sota_history.length > 0) {
      result.push({
        x: isCombine ? sota_history.map((s) => s.label || `配置 ${s.loop_id}`) : sota_history.map((s) => s.loop_id),
        y: isCombine ? sota_history.map((s) => s.ann_ret != null ? s.ann_ret * 100 : null) : sota_history.map((s) => s.ic ?? null),
        type: "scatter", mode: "markers",
        name: isCombine ? "最优配置" : "SOTA",
        yaxis: isCombine ? "y2" : "y",
        marker: {
          color: "#f59e0b", size: 14,
          symbol: "star", line: { width: 1, color: "#d97706" },
        },
        hovertemplate: "%{y:.4f}<extra></extra>",
      });
    }

    return result;
  }, [trajectory, sota_history, mode]);

  if (!traces) {
    return (
      <div style={{ color: "#94a3b8", textAlign: "center", padding: 24 }}>
        暂无演进轨迹数据
      </div>
    );
  }

  return (
    <PlotlyChart
      data={traces}
      layout={{
        ...LAYOUT,
        xaxis: mode === "combine"
          ? { ...LAYOUT.xaxis, tickprefix: "", title: "配置(窗口×topk)", type: "category" }
          : LAYOUT.xaxis,
      }}
      config={CONFIG}
      useResizeHandler
      style={plotStyle}
    />
  );
});
