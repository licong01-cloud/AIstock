"use client";

import React, { useState, useEffect, useMemo } from "react";
import MetricsTrajectoryChart from "./charts/MetricsTrajectoryChart";
import DimensionHeatmap from "./charts/DimensionHeatmap";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface EvolutionTrajectoryProps {
  taskId: string | null;
  taskType?: string;
  evolutionMode?: string;
  sourceType?: string;
}

/**
 * 从 metrics_json 中安全提取数值
 */
function extractMetric(metrics: any, ...keys: string[]): number | null {
  if (!metrics) return null;
  for (const key of keys) {
    const v = metrics[key];
    if (typeof v === "number" && isFinite(v)) return v;
  }
  return null;
}

export default React.memo(function EvolutionTrajectory({
  taskId,
  taskType,
  evolutionMode,
  sourceType,
}: EvolutionTrajectoryProps) {
  const [rawData, setRawData] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!taskId) return;
    setLoading(true);
    setError(null);

    fetch(`${API}/quantevolver/evolution/tasks/${taskId}/trajectory`)
      .then((res) => res.json())
      .then((json) => {
        if (json.status === "success" && json.data) {
          setRawData(json.data);
        } else {
          setError("无法获取轨迹数据");
        }
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [taskId]);

  // 从后端原始数据转换为组件所需的格式
  const processedData = useMemo(() => {
    if (!rawData) return null;

    const trajectory = (rawData.trajectory || []).map((loop: any) => {
      const m = loop.metrics_json || {};
      return {
        loop_id: loop.loop_index ?? 0,
        ic: extractMetric(m, "IC", "ic"),
        rank_ic: extractMetric(m, "Rank_IC", "rank_ic", "Rank IC"),
        icir: extractMetric(m, "ICIR", "icir"),
        ann_ret: extractMetric(m, "annualized_return", "ann_return", "annual_return"),
        sharpe: extractMetric(m, "sharpe", "Sharpe"),
        max_drawdown: extractMetric(m, "max_drawdown", "max_drawdown_no_cost"),
        is_sota: loop.is_sota === true,
        action_type: loop.action_type || "unknown",
      };
    });

    // 构建 sota_history
    const sotaHistory = trajectory
      .filter((t: any) => t.is_sota)
      .map((t: any) => ({
        loop_id: t.loop_id,
        ic: t.ic,
        ann_ret: t.ann_ret,
        sharpe: t.sharpe,
        max_drawdown: t.max_drawdown,
      }));

    // 构建 dimension_stats (按 action_type 统计)
    const dimMap: Record<string, { attempts: number; successes: number }> = {};
    for (const t of trajectory) {
      const dim = t.action_type || "unknown";
      if (!dimMap[dim]) dimMap[dim] = { attempts: 0, successes: 0 };
      dimMap[dim].attempts += 1;
      if (t.is_sota) dimMap[dim].successes += 1;
    }

    // 计算最佳指标
    const completedLoops = trajectory.filter((t: any) => t.ic != null);
    const bestIc = completedLoops.length > 0
      ? Math.max(...completedLoops.map((t: any) => t.ic))
      : null;
    const bestSharpe = completedLoops.filter((t: any) => t.sharpe != null).length > 0
      ? Math.max(...completedLoops.filter((t: any) => t.sharpe != null).map((t: any) => t.sharpe))
      : null;

    return {
      total_loops: trajectory.length,
      trajectory,
      sota_history: sotaHistory,
      dimension_stats: dimMap,
      best_ic: bestIc,
      best_sharpe: bestSharpe,
    };
  }, [rawData]);

  if (!taskId) {
    return (
      <div style={{ color: "#94a3b8", textAlign: "center", padding: 40 }}>
        请先选择一个任务
      </div>
    );
  }

  if (loading) {
    return (
      <div style={{ color: "#64748b", textAlign: "center", padding: 40 }}>
        加载演进轨迹中...
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ color: "#ef4444", textAlign: "center", padding: 40 }}>
        {error}
      </div>
    );
  }

  if (!processedData) return null;
  const data = processedData;
  const normalizedMode = evolutionMode || "auto";
  const normalizedType = taskType || sourceType || "evolution";
  const showDimensionStats = normalizedType === "evolution" && normalizedMode === "auto";

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20, minWidth: 0 }}>
      {/* Summary stats */}
      <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
        {[
          { label: "总轮次", value: data.total_loops, color: "#0f172a" },
          { label: "SOTA 次数", value: data.sota_history.length, color: "#d97706" },
          { label: "最佳 IC", value: data.best_ic != null ? data.best_ic.toFixed(4) : "-", color: "#059669" },
          { label: "最佳 Sharpe", value: data.best_sharpe != null ? data.best_sharpe.toFixed(2) : "-", color: "#3b82f6" },
        ].map((s) => (
          <div key={s.label} style={{
            flex: "1 1 120px", padding: "12px 16px",
            backgroundColor: "#f8fafc", borderRadius: 8,
            border: "1px solid #e2e8f0", textAlign: "center",
          }}>
            <div style={{ fontSize: 11, color: "#64748b", fontWeight: 600, textTransform: "uppercase" }}>
              {s.label}
            </div>
            <div style={{ fontSize: 22, fontWeight: 700, color: s.color, fontFamily: "monospace" }}>
              {s.value}
            </div>
          </div>
        ))}
      </div>

      {/* Metrics trajectory chart */}
      <div style={{
        backgroundColor: "#fff", borderRadius: 8,
        border: "1px solid #e2e8f0", padding: 16, minWidth: 0, overflow: "hidden",
      }}>
        <h4 style={{
          margin: "0 0 12px", fontSize: 13, fontWeight: 700,
          color: "#3b82f6", textTransform: "uppercase", letterSpacing: "0.05em",
        }}>
          指标演进折线
        </h4>
        <MetricsTrajectoryChart
          trajectory={data.trajectory}
          sota_history={data.sota_history}
        />
      </div>

      {/* SOTA evolution staircase */}
      {data.sota_history.length > 0 && (
        <div style={{
          backgroundColor: "#fff", borderRadius: 8,
          border: "1px solid #e2e8f0", padding: 16, minWidth: 0, overflowX: "auto",
        }}>
          <h4 style={{
            margin: "0 0 12px", fontSize: 13, fontWeight: 700,
            color: "#f59e0b", textTransform: "uppercase", letterSpacing: "0.05em",
          }}>
            SOTA 进化阶梯
          </h4>
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {data.sota_history.map((s: any, i: number) => (
              <div key={i} style={{
                display: "flex", alignItems: "center", gap: 12,
                padding: "8px 12px", backgroundColor: "#fffbeb",
                borderRadius: 6, border: "1px solid #fef3c7",
              }}>
                <span style={{
                  fontWeight: 700, color: "#d97706",
                  fontFamily: "monospace", minWidth: 60,
                }}>
                  Loop {s.loop_id}
                </span>
                <span style={{ fontSize: 13, color: "#475569" }}>
                  IC: {s.ic?.toFixed(4) ?? "-"}
                </span>
                <span style={{ fontSize: 13, color: "#475569" }}>
                  Sharpe: {s.sharpe?.toFixed(2) ?? "-"}
                </span>
                <span style={{ fontSize: 13, color: "#475569" }}>
                  年化: {s.ann_ret != null ? (s.ann_ret * 100).toFixed(1) + "%" : "-"}
                </span>
                <span style={{ fontSize: 13, color: "#ef4444", fontWeight: 600 }}>
                  回撤: {s.max_drawdown != null ? (Math.abs(s.max_drawdown) * 100).toFixed(1) + "%" : "-"}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Dimension exploration stats are meaningful only for automatic evolution. */}
      {showDimensionStats && Object.keys(data.dimension_stats).length > 0 && (
        <div style={{
          backgroundColor: "#fff", borderRadius: 8,
          border: "1px solid #e2e8f0", padding: 16, minWidth: 0, overflow: "hidden",
        }}>
          <h4 style={{
            margin: "0 0 12px", fontSize: 13, fontWeight: 700,
            color: "#8b5cf6", textTransform: "uppercase", letterSpacing: "0.05em",
          }}>
            维度探索统计
          </h4>
          <DimensionHeatmap dimension_stats={data.dimension_stats} />
        </div>
      )}
    </div>
  );
});
