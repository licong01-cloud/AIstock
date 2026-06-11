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

const LABELS = {
  noData: "\u6682\u65e0\u6536\u76ca\u66f2\u7ebf\u6570\u636e",
  portfolioNav: "\u7ec4\u5408\u51c0\u503c",
  portfolioNavWithCost: "\u7ec4\u5408\u51c0\u503c(\u6263\u8d39)",
  csi300: "\u6caa\u6df1300",
  excessNoCost: "\u8d85\u989d(\u65e0\u6210\u672c)",
  excessWithCost: "\u8d85\u989d(\u542b\u6210\u672c)",
  benchmark: "\u57fa\u51c6",
  tradingDayIndex: "\u4ea4\u6613\u65e5\u5e8f\u53f7",
  nav: "\u51c0\u503c",
  cumulativeReturnPct: "\u7d2f\u8ba1\u6536\u76ca (%)",
  drawdown: "\u56de\u64a4",
  drawdownPct: "\u56de\u64a4 (%)",
  backtestRange: "\u56de\u6d4b\u533a\u95f4",
  tradingDays: "\u4e2a\u4ea4\u6613\u65e5",
  total: "\u5171",
};

function isDateString(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}/.test(value);
}

interface ReturnCurveChartProps {
  dates: string[];
  cumulative_excess_no_cost?: number[];
  cumulative_excess_with_cost?: number[];
  cumulative_portfolio?: number[];
  cumulative_portfolio_with_cost?: number[];
  cumulative_benchmark?: number[];
  drawdown_series?: number[];
}

export default React.memo(function ReturnCurveChart({
  dates,
  cumulative_excess_no_cost,
  cumulative_excess_with_cost,
  cumulative_portfolio,
  cumulative_portfolio_with_cost,
  cumulative_benchmark,
  drawdown_series,
}: ReturnCurveChartProps) {
  const chartDates = dates ?? [];
  const hasDates = chartDates.length > 0;
  const datesAreReal = hasDates && isDateString(chartDates[0]);
  const hasNavData = !!cumulative_portfolio && cumulative_portfolio.length > 0;

  const returnTraces = useMemo(() => {
    const traces: any[] = [];

    if (hasNavData) {
      traces.push({
        x: chartDates,
        y: cumulative_portfolio,
        type: "scatter",
        mode: "lines",
        name: LABELS.portfolioNav,
        line: { color: "#3b82f6", width: 2 },
      });

      if (cumulative_portfolio_with_cost && cumulative_portfolio_with_cost.length > 0) {
        const isDifferent = cumulative_portfolio!.some(
          (value, index) => Math.abs(value - (cumulative_portfolio_with_cost[index] ?? value)) > 1e-6
        );

        if (isDifferent) {
          traces.push({
            x: chartDates,
            y: cumulative_portfolio_with_cost,
            type: "scatter",
            mode: "lines",
            name: LABELS.portfolioNavWithCost,
            line: { color: "#10b981", width: 2 },
          });
        }
      }

      if (cumulative_benchmark && cumulative_benchmark.length > 0) {
        traces.push({
          x: chartDates,
          y: cumulative_benchmark,
          type: "scatter",
          mode: "lines",
          name: LABELS.csi300,
          line: { color: "#94a3b8", width: 1, dash: "dot" },
        });
      }
    } else {
      if (cumulative_excess_no_cost && cumulative_excess_no_cost.length > 0) {
        traces.push({
          x: chartDates,
          y: cumulative_excess_no_cost.map((value) => value * 100),
          type: "scatter",
          mode: "lines",
          name: LABELS.excessNoCost,
          line: { color: "#3b82f6", width: 1.5 },
          fill: "tozeroy",
          fillcolor: "rgba(59,130,246,0.08)",
        });
      }

      if (cumulative_excess_with_cost && cumulative_excess_with_cost.length > 0) {
        traces.push({
          x: chartDates,
          y: cumulative_excess_with_cost.map((value) => value * 100),
          type: "scatter",
          mode: "lines",
          name: LABELS.excessWithCost,
          line: { color: "#10b981", width: 2 },
        });
      }

      if (cumulative_benchmark && cumulative_benchmark.length > 0) {
        traces.push({
          x: chartDates,
          y: cumulative_benchmark.map((value) => value * 100),
          type: "scatter",
          mode: "lines",
          name: LABELS.benchmark,
          line: { color: "#94a3b8", width: 1, dash: "dot" },
        });
      }
    }

    return traces;
  }, [
    chartDates,
    cumulative_benchmark,
    cumulative_excess_no_cost,
    cumulative_excess_with_cost,
    cumulative_portfolio,
    cumulative_portfolio_with_cost,
    hasNavData,
  ]);

  const xaxisConfig = useMemo(() => {
    if (datesAreReal) {
      return {
        showticklabels: true,
        type: "date" as const,
        tickformat: "%Y-%m",
        nticks: 8,
      };
    }

    return {
      showticklabels: true,
      type: "linear" as const,
      title: LABELS.tradingDayIndex,
      nticks: 8,
    };
  }, [datesAreReal]);

  const returnLayout = useMemo(
    () => ({
      height: 260,
      margin: { t: 20, r: 20, b: 40, l: 55 },
      xaxis: xaxisConfig,
      yaxis: { title: hasNavData ? LABELS.nav : LABELS.cumulativeReturnPct },
      legend: { orientation: "h" as const, y: 1.15 },
      font: { size: 11 },
    }),
    [hasNavData, xaxisConfig]
  );

  const ddTraces = useMemo(() => {
    if (!drawdown_series || drawdown_series.length === 0) return [];

    return [
      {
        x: chartDates,
        y: drawdown_series.map((value) => value * 100),
        type: "scatter",
        mode: "lines",
        name: LABELS.drawdown,
        line: { color: "#ef4444", width: 1 },
        fill: "tozeroy",
        fillcolor: "rgba(239,68,68,0.15)",
      },
    ];
  }, [chartDates, drawdown_series]);

  const ddXaxis = useMemo(() => {
    if (datesAreReal) {
      return { title: "", type: "date" as const, tickformat: "%Y-%m", nticks: 8 };
    }

    return { title: LABELS.tradingDayIndex, type: "linear" as const, nticks: 8 };
  }, [datesAreReal]);

  const ddLayout = useMemo(
    () => ({
      height: 120,
      margin: { t: 0, r: 20, b: 40, l: 55 },
      xaxis: ddXaxis,
      yaxis: { title: LABELS.drawdownPct },
      showlegend: false,
      font: { size: 11 },
    }),
    [ddXaxis]
  );

  if (!hasDates) {
    return (
      <div style={{ color: "#94a3b8", textAlign: "center", padding: 24 }}>
        {LABELS.noData}
      </div>
    );
  }

  const dateRangeText = datesAreReal
    ? `${LABELS.backtestRange}: ${chartDates[0]} ~ ${chartDates[chartDates.length - 1]} (${chartDates.length} ${LABELS.tradingDays})`
    : `${LABELS.total} ${chartDates.length} ${LABELS.tradingDays}`;

  return (
    <div>
      <div style={{ fontSize: 12, color: "#64748b", marginBottom: 4, fontFamily: "monospace" }}>
        {dateRangeText}
      </div>
      <PlotlyChart data={returnTraces} layout={returnLayout} config={CONFIG} style={plotStyle} />
      {ddTraces.length > 0 && <PlotlyChart data={ddTraces} layout={ddLayout} config={CONFIG} style={plotStyle} />}
    </div>
  );
});
