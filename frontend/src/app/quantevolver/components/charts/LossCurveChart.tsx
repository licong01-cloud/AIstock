"use client";

import React, { useMemo } from "react";
import dynamic from "next/dynamic";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false }) as any;

const plotStyle = { width: "100%" };
const CONFIG = { responsive: true, displayModeBar: false } as const;

interface LossCurveChartProps {
  train_loss_curve?: number[];
  val_loss_curve?: number[];
  best_epoch?: number;
  overfit_ratio?: number | null;
  convergence_ratio?: number | null;
}

export default React.memo(function LossCurveChart({
  train_loss_curve,
  val_loss_curve,
  best_epoch,
  overfit_ratio,
  convergence_ratio,
}: LossCurveChartProps) {
  if (!train_loss_curve || train_loss_curve.length === 0) {
    return (
      <div style={{ color: "#94a3b8", textAlign: "center", padding: 24 }}>
        暂无训练过程数据
      </div>
    );
  }

  const traces = useMemo(() => {
    const epochs = train_loss_curve.map((_, i) => i + 1);
    const t: any[] = [];

    t.push({
      x: epochs,
      y: train_loss_curve,
      type: "scatter",
      mode: "lines",
      name: "Train Loss",
      line: { color: "#3b82f6", width: 2 },
    });

    if (val_loss_curve && val_loss_curve.length > 0) {
      t.push({
        x: epochs,
        y: val_loss_curve,
        type: "scatter",
        mode: "lines",
        name: "Val Loss",
        line: { color: "#ef4444", width: 2 },
      });
    }

    // Best epoch marker
    if (best_epoch && val_loss_curve && best_epoch <= val_loss_curve.length) {
      t.push({
        x: [best_epoch],
        y: [val_loss_curve[best_epoch - 1]],
        type: "scatter",
        mode: "markers",
        name: `Best Epoch (${best_epoch})`,
        marker: { color: "#10b981", size: 10, symbol: "star" },
      });
    }

    return t;
  }, [train_loss_curve, val_loss_curve, best_epoch]);

  const layout = useMemo(() => ({
    height: 280,
    margin: { t: 30, r: 20, b: 40, l: 50 },
    xaxis: { title: "Epoch" },
    yaxis: { title: "Loss" },
    legend: { orientation: "h" as const, y: 1.12 },
    font: { size: 11 },
  }), []);

  return (
    <div>
      <Plot
        data={traces}
        layout={layout}
        config={CONFIG}
        style={plotStyle}
      />
      <div style={{
        display: "flex", justifyContent: "center", gap: 24,
        fontSize: 12, color: "#64748b", marginTop: 4,
      }}>
        {overfit_ratio != null && (
          <span>过拟合比: <b style={{ color: overfit_ratio > 1.3 ? "#ef4444" : "#10b981" }}>
            {overfit_ratio.toFixed(3)}
          </b></span>
        )}
        {convergence_ratio != null && (
          <span>收敛比: <b>{convergence_ratio.toFixed(3)}</b></span>
        )}
      </div>
    </div>
  );
});
