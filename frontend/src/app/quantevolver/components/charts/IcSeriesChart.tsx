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


const plotStyle = { width: "100%" };
const CONFIG = { responsive: true, displayModeBar: false } as const;

interface IcSeriesChartProps {
  dates: string[];
  ic_series?: number[];
  rank_ic_series?: number[];
  ic_rolling_30d_mean?: (number | null)[];
  ic_rolling_30d_std?: (number | null)[];
  ic_positive_ratio?: number | null;
}

export default React.memo(function IcSeriesChart({
  dates,
  ic_series,
  rank_ic_series,
  ic_rolling_30d_mean,
  ic_rolling_30d_std,
  ic_positive_ratio,
}: IcSeriesChartProps) {
  const hasDates = Boolean(dates && dates.length > 0);

  const traces = useMemo(() => {
    if (!hasDates) return [];

    const t: any[] = [];

    if (ic_series) {
      t.push({
        x: dates,
        y: ic_series,
        type: "scatter",
        mode: "lines",
        name: "IC",
        line: { color: "#3b82f6", width: 1 },
        opacity: 0.6,
      });
    }

    if (rank_ic_series) {
      t.push({
        x: dates,
        y: rank_ic_series,
        type: "scatter",
        mode: "lines",
        name: "Rank IC",
        line: { color: "#8b5cf6", width: 1 },
        opacity: 0.6,
      });
    }

    if (ic_rolling_30d_mean) {
      t.push({
        x: dates,
        y: ic_rolling_30d_mean,
        type: "scatter",
        mode: "lines",
        name: "30日滚动均值",
        line: { color: "#ef4444", width: 2 },
      });
    }

    // Rolling std band (mean ± std)
    if (ic_rolling_30d_mean && ic_rolling_30d_std) {
      const upper = ic_rolling_30d_mean.map((m, i) => {
        const s = ic_rolling_30d_std[i];
        return m != null && s != null ? m + s : null;
      });
      const lower = ic_rolling_30d_mean.map((m, i) => {
        const s = ic_rolling_30d_std[i];
        return m != null && s != null ? m - s : null;
      });
      t.push({
        x: dates,
        y: upper,
        type: "scatter",
        mode: "lines",
        name: "+1σ",
        line: { width: 0 },
        showlegend: false,
      });
      t.push({
        x: dates,
        y: lower,
        type: "scatter",
        mode: "lines",
        name: "-1σ",
        line: { width: 0 },
        fill: "tonexty",
        fillcolor: "rgba(239,68,68,0.1)",
        showlegend: false,
      });
    }

    // Zero line
    t.push({
      x: [dates[0], dates[dates.length - 1]],
      y: [0, 0],
      type: "scatter",
      mode: "lines",
      name: "零线",
      line: { color: "#94a3b8", width: 1, dash: "dash" },
      showlegend: false,
    });

    return t;
  }, [dates, hasDates, ic_series, rank_ic_series, ic_rolling_30d_mean, ic_rolling_30d_std]);

  const layout = useMemo(() => ({
    height: 320,
    margin: { t: 30, r: 20, b: 40, l: 50 },
    xaxis: { title: "", type: "date" as const },
    yaxis: { title: "IC" },
    legend: { orientation: "h" as const, y: 1.12 },
    font: { size: 11 },
  }), []);

  if (!hasDates) {
    return <div style={{ color: "#94a3b8", textAlign: "center", padding: 24 }}>暂无 IC 数据</div>;
  }

  return (
    <div>
      <PlotlyChart
        data={traces}
        layout={layout}
        config={CONFIG}
        style={plotStyle}
      />
      {ic_positive_ratio != null && (
        <div style={{ textAlign: "center", fontSize: 12, color: "#64748b", marginTop: 4 }}>
          IC 胜率: {(ic_positive_ratio * 100).toFixed(1)}%
        </div>
      )}
    </div>
  );
});
