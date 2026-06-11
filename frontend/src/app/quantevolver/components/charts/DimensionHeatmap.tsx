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


interface DimensionStats {
  [dimension: string]: {
    attempts: number;
    successes: number;
  };
}

interface DimensionHeatmapProps {
  dimension_stats: DimensionStats;
}

const LAYOUT = {
  autosize: true,
  height: 260,
  margin: { t: 30, r: 20, b: 60, l: 40 },
  barmode: "group" as const,
  xaxis: { title: "", tickangle: -30 },
  yaxis: { title: "次数", dtick: 1 },
  legend: { orientation: "h" as const, y: 1.12 },
  font: { size: 11 },
};

const CONFIG = { responsive: true, displayModeBar: false };
const plotStyle = { width: "100%", height: "260px", minHeight: "260px" };

export default React.memo(function DimensionHeatmap({
  dimension_stats,
}: DimensionHeatmapProps) {
  const { dims, traces, successRates } = useMemo(() => {
    const d = Object.keys(dimension_stats || {});
    const attempts = d.map((k) => dimension_stats[k].attempts);
    const successes = d.map((k) => dimension_stats[k].successes);
    const rates = d.map((k) => {
      const a = dimension_stats[k].attempts;
      return a > 0 ? Math.round((dimension_stats[k].successes / a) * 100) : 0;
    });
    const t = [
      {
        x: d, y: attempts,
        type: "bar", name: "尝试次数",
        marker: { color: "#94a3b8" },
      },
      {
        x: d, y: successes,
        type: "bar", name: "成功次数",
        marker: { color: "#10b981" },
      },
    ];
    return { dims: d, traces: t, successRates: rates };
  }, [dimension_stats]);

  if (dims.length === 0) {
    return (
      <div style={{ color: "#94a3b8", textAlign: "center", padding: 24 }}>
        暂无维度探索数据
      </div>
    );
  }

  return (
    <div>
      <PlotlyChart
        data={traces}
        layout={LAYOUT}
        config={CONFIG}
        useResizeHandler
        style={plotStyle}
      />
      <div style={{
        display: "flex", flexWrap: "wrap", gap: 12,
        justifyContent: "center", marginTop: 8,
      }}>
        {dims.map((d, i) => (
          <span key={d} style={{
            fontSize: 11, color: "#475569",
            padding: "2px 8px", borderRadius: 4,
            backgroundColor: successRates[i] >= 50
              ? "#dcfce7" : "#fef2f2",
          }}>
            {d}: {successRates[i]}%
          </span>
        ))}
      </div>
    </div>
  );
});
