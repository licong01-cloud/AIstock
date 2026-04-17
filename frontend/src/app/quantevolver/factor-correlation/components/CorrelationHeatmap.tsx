"use client";

import React, { useState, useMemo } from "react";
import dynamic from "next/dynamic";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false }) as any;

interface Props {
  factorNames: string[];
  matrix: number[][];
  disabledFactors?: string[];
  onCellClick: (fa: string, fb: string, corr: number) => void;
}

export default function CorrelationHeatmap({
  factorNames,
  matrix,
  disabledFactors = [],
  onCellClick,
}: Props) {
  const [search, setSearch] = useState("");

  // 搜索过滤：匹配的因子子矩阵
  const { filteredNames, filteredMatrix } = useMemo(() => {
    if (!search.trim()) return { filteredNames: factorNames, filteredMatrix: matrix };
    const kw = search.trim().toLowerCase();
    const indices = factorNames
      .map((n, i) => (n.toLowerCase().includes(kw) ? i : -1))
      .filter((i) => i >= 0);
    if (indices.length === 0) return { filteredNames: factorNames, filteredMatrix: matrix };
    const names = indices.map((i) => factorNames[i]);
    const mat = indices.map((r) => indices.map((c) => matrix[r][c]));
    return { filteredNames: names, filteredMatrix: mat };
  }, [factorNames, matrix, search]);

  // 为禁用因子添加显示标记
  const disabledSet = useMemo(() => new Set(disabledFactors), [disabledFactors]);
  const displayNames = useMemo(
    () => filteredNames.map((n) => (disabledSet.has(n) ? `${n} [OFF]` : n)),
    [filteredNames, disabledSet]
  );

  const size = filteredNames.length;
  const chartHeight = Math.max(400, Math.min(800, size * 14 + 120));

  return (
    <div>
      <div style={{ marginBottom: 12 }}>
        <input
          type="text"
          placeholder="搜索因子名（筛选子矩阵）..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={{
            padding: "7px 12px",
            border: "1px solid #e5e7eb",
            borderRadius: 8,
            fontSize: 13,
            width: 280,
            outline: "none",
          }}
        />
        <span style={{ fontSize: 12, color: "#9ca3af", marginLeft: 12 }}>
          显示 {size} × {size} 因子矩阵
        </span>
      </div>

      {size === 0 ? (
        <div style={{ color: "#9ca3af", textAlign: "center", padding: 48 }}>
          暂无矩阵数据
        </div>
      ) : (
        <Plot
          data={[
            {
              z: filteredMatrix,
              x: displayNames,
              y: displayNames,
              type: "heatmap",
              colorscale: [
                [0, "#2563eb"],
                [0.5, "#f8fafc"],
                [1, "#dc2626"],
              ],
              zmin: -1,
              zmax: 1,
              hovertemplate:
                "%{y} vs %{x}<br>相关系数: %{z:.4f}<extra></extra>",
              colorbar: {
                title: { text: "Corr", side: "right" },
                thickness: 14,
                len: 0.8,
              },
            },
          ]}
          layout={{
            height: chartHeight,
            margin: { t: 30, r: 80, b: size > 20 ? 140 : 80, l: size > 20 ? 140 : 80 },
            xaxis: {
              tickangle: -45,
              tickfont: { size: size > 40 ? 8 : 10 },
              side: "bottom",
            },
            yaxis: {
              tickfont: { size: size > 40 ? 8 : 10 },
              autorange: "reversed",
            },
            font: { size: 11 },
          }}
          config={{ responsive: true, displayModeBar: true, displaylogo: false }}
          style={{ width: "100%" }}
          onClick={(data: any) => {
            if (data?.points?.[0]) {
              const pt = data.points[0];
              onCellClick(
                filteredNames[pt.pointIndex[1]],
                filteredNames[pt.pointIndex[0]],
                pt.z
              );
            }
          }}
        />
      )}
    </div>
  );
}
