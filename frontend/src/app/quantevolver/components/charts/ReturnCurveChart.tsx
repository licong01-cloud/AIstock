"use client";

import React, { useMemo } from "react";
import dynamic from "next/dynamic";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false }) as any;

const plotStyle = { width: "100%" };
const CONFIG = { responsive: true, displayModeBar: false } as const;

/** Check if a string looks like YYYY-MM-DD */
function isDateString(s: string): boolean {
  return /^\d{4}-\d{2}-\d{2}/.test(s);
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
  if (!dates || dates.length === 0) {
    return (
      <div style={{ color: "#94a3b8", textAlign: "center", padding: 24 }}>
        暂无收益曲线数据
      </div>
    );
  }

  // Detect if dates are real date strings or numeric indices
  const datesAreReal = dates.length > 0 && isDateString(dates[0]);

  // Determine mode: NAV curves (portfolio) vs legacy excess return curves
  const hasNavData = !!cumulative_portfolio && cumulative_portfolio.length > 0;

  // Upper chart traces
  const returnTraces = useMemo(() => {
    const t: any[] = [];

    if (hasNavData) {
      // NAV mode: show portfolio net value curves
      t.push({
        x: dates,
        y: cumulative_portfolio,
        type: "scatter",
        mode: "lines",
        name: "组合净值",
        line: { color: "#3b82f6", width: 2 },
      });

      if (cumulative_portfolio_with_cost && cumulative_portfolio_with_cost.length > 0) {
        // Only show if different from portfolio (i.e. cost > 0)
        const isDifferent = cumulative_portfolio!.some(
          (v, i) => Math.abs(v - (cumulative_portfolio_with_cost![i] ?? v)) > 1e-6
        );
        if (isDifferent) {
          t.push({
            x: dates,
            y: cumulative_portfolio_with_cost,
            type: "scatter",
            mode: "lines",
            name: "组合净值(扣费)",
            line: { color: "#10b981", width: 2 },
          });
        }
      }

      if (cumulative_benchmark && cumulative_benchmark.length > 0) {
        t.push({
          x: dates,
          y: cumulative_benchmark,
          type: "scatter",
          mode: "lines",
          name: "沪深300",
          line: { color: "#94a3b8", width: 1, dash: "dot" },
        });
      }
    } else {
      // Legacy excess return mode
      if (cumulative_excess_no_cost) {
        t.push({
          x: dates,
          y: cumulative_excess_no_cost.map((v) => v * 100),
          type: "scatter",
          mode: "lines",
          name: "超额(无成本)",
          line: { color: "#3b82f6", width: 1.5 },
          fill: "tozeroy",
          fillcolor: "rgba(59,130,246,0.08)",
        });
      }

      if (cumulative_excess_with_cost) {
        t.push({
          x: dates,
          y: cumulative_excess_with_cost.map((v) => v * 100),
          type: "scatter",
          mode: "lines",
          name: "超额(含成本)",
          line: { color: "#10b981", width: 2 },
        });
      }

      if (cumulative_benchmark) {
        t.push({
          x: dates,
          y: cumulative_benchmark.map((v) => v * 100),
          type: "scatter",
          mode: "lines",
          name: "基准",
          line: { color: "#94a3b8", width: 1, dash: "dot" },
        });
      }
    }

    return t;
  }, [dates, cumulative_excess_no_cost, cumulative_excess_with_cost, cumulative_portfolio, cumulative_portfolio_with_cost, cumulative_benchmark, hasNavData]);

  // X-axis config: use "date" type only when dates are real YYYY-MM-DD strings
  const xaxisConfig = useMemo(() => {
    if (datesAreReal) {
      return {
        showticklabels: true,
        type: "date" as const,
        tickformat: "%Y-%m",
        nticks: 8,
      };
    }
    // Numeric index dates: use linear axis with evenly spaced ticks
    return {
      showticklabels: true,
      type: "linear" as const,
      title: "交易日序号",
      nticks: 8,
    };
  }, [datesAreReal]);

  const returnLayout = useMemo(() => ({
    height: 260,
    margin: { t: 20, r: 20, b: 40, l: 55 },
    xaxis: xaxisConfig,
    yaxis: { title: hasNavData ? "净值" : "累计收益 (%)" },
    legend: { orientation: "h" as const, y: 1.15 },
    font: { size: 11 },
  }), [hasNavData, xaxisConfig]);

  // Lower chart: drawdown
  const ddTraces = useMemo(() => {
    if (!drawdown_series) return [];
    return [{
      x: dates,
      y: drawdown_series.map((v) => v * 100),
      type: "scatter",
      mode: "lines",
      name: "回撤",
      line: { color: "#ef4444", width: 1 },
      fill: "tozeroy",
      fillcolor: "rgba(239,68,68,0.15)",
    }];
  }, [dates, drawdown_series]);

  const ddXaxis = useMemo(() => {
    if (datesAreReal) {
      return { title: "", type: "date" as const, tickformat: "%Y-%m", nticks: 8 };
    }
    return { title: "交易日序号", type: "linear" as const, nticks: 8 };
  }, [datesAreReal]);

  const ddLayout = useMemo(() => ({
    height: 120,
    margin: { t: 0, r: 20, b: 40, l: 55 },
    xaxis: ddXaxis,
    yaxis: { title: "回撤 (%)" },
    showlegend: false,
    font: { size: 11 },
  }), [ddXaxis]);

  // Summary line: show actual backtest date range
  const dateRangeText = datesAreReal
    ? `回测区间: ${dates[0]} ~ ${dates[dates.length - 1]}  (${dates.length} 交易日)`
    : `共 ${dates.length} 个交易日`;

  return (
    <div>
      <div style={{ fontSize: 12, color: "#64748b", marginBottom: 4, fontFamily: "monospace" }}>
        {dateRangeText}
      </div>
      <Plot
        data={returnTraces}
        layout={returnLayout}
        config={CONFIG}
        style={plotStyle}
      />
      {ddTraces.length > 0 && (
        <Plot
          data={ddTraces}
          layout={ddLayout}
          config={CONFIG}
          style={plotStyle}
        />
      )}
    </div>
  );
});
