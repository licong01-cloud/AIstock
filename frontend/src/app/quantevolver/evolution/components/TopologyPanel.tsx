"use client";

import React from "react";
import { GitMerge } from "lucide-react";

export interface Loop {
  loop_id: string;
  task_id: string;
  loop_index: number;
  action_type: string;
  config_json?: any;
  metrics_json?: any;
  agent_analysis?: any;
  is_sota: boolean;
  status: string;
  experiment_id: string;
  created_at: string;
  updated_at: string;
}

interface TopologyPanelProps {
  loops: Loop[];
  activeLoopIndex: number | null;
  onSelectLoop: (index: number) => void;
}

const cardStyle: React.CSSProperties = {
  backgroundColor: "#ffffff",
  borderRadius: "12px",
  boxShadow: "0 4px 12px rgba(0, 0, 0, 0.05)",
  display: "flex",
  flexDirection: "column",
  overflow: "hidden",
  border: "1px solid rgba(255, 255, 255, 0.2)",
  flex: "0 0 420px",
};

const headerStyle: React.CSSProperties = {
  padding: "16px 20px",
  borderBottom: "1px solid #f1f5f9",
  backgroundColor: "#f8fafc",
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
};

export default React.memo(function TopologyPanel({ loops, activeLoopIndex, onSelectLoop }: TopologyPanelProps) {
  // 找到第一个 SOTA Loop 的因子集作为基准（初始 SOTA）
  const sotaFactorSet = React.useMemo(() => {
    const sotaLoop = loops.find(l => l.is_sota);
    const factors: string[] = sotaLoop?.config_json?.factor_list || [];
    return new Set(factors);
  }, [loops]);

  return (
    <div style={cardStyle}>
      <div style={headerStyle}>
        <h2 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#1e293b", display: "flex", alignItems: "center", gap: "8px" }}>
          <GitMerge color="#a855f7" size={20} />
          演进拓扑
        </h2>
      </div>
      <div style={{ flex: 1, overflowY: "auto", padding: "24px", position: "relative", backgroundColor: "#fafaf9" }}>
        {/* 垂直时间线 */}
        <div style={{ position: "absolute", left: "43px", top: "24px", bottom: "24px", width: "2px", backgroundColor: "#e5e7eb", zIndex: 0 }}></div>

        <div style={{ display: "flex", flexDirection: "column", gap: "24px", position: "relative", zIndex: 1 }}>
          {loops.length === 0 ? (
            <div style={{ textAlign: "center", color: "#94a3b8", fontSize: "14px", marginTop: "40px" }}>
              暂无记录的 Loops
            </div>
          ) : loops.map(loop => {
            const isActive = activeLoopIndex === loop.loop_index;
            let iconBg = "#f1f5f9";
            let iconBorder = "#cbd5e1";
            let iconColor = "#64748b";

            if (loop.is_sota) {
              iconBg = "#fef3c7";
              iconBorder = "#f59e0b";
              iconColor = "#d97706";
            } else if (loop.status === "completed") {
              iconBg = "#dcfce7";
              iconBorder = "#22c55e";
              iconColor = "#15803d";
            } else if (loop.status === "running") {
              iconBg = "#dbeafe";
              iconBorder = "#3b82f6";
              iconColor = "#1d4ed8";
            }

            // 因子列表：以初始 SOTA 因子集为基准，标记保留/删除/新增
            const thisFactors: string[] = loop.config_json?.factor_list || [];
            const thisFactorSet = new Set(thisFactors);
            // 显示：SOTA 因子先列出（保留绿色/删除红色删除线），再列新增因子
            const sotaFactorsArr = Array.from(sotaFactorSet) as string[];
            const newFactors = thisFactors.filter((f: string) => !sotaFactorSet.has(f));

            return (
              <div
                key={loop.loop_id}
                style={{ display: "flex", alignItems: "flex-start", gap: "16px", cursor: "pointer" }}
                onClick={() => onSelectLoop(loop.loop_index)}
              >
                <div style={{
                  width: "40px", height: "40px", flexShrink: 0, borderRadius: "50%",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  backgroundColor: iconBg, border: `2px solid ${iconBorder}`, color: iconColor,
                  fontSize: "14px", fontWeight: 700, marginTop: "2px",
                  boxShadow: loop.status === "running" ? `0 0 0 4px ${iconBorder}40` : "0 2px 4px rgba(0,0,0,0.05)",
                  animation: loop.status === "running" ? "pulse 2s infinite" : "none",
                  transition: "transform 0.2s"
                }}>
                  {loop.is_sota ? "\u2B50" : loop.status === "running" ? "\u27F3" : loop.loop_index}
                </div>
                <div style={{
                  flex: 1, padding: "12px 16px", borderRadius: "8px",
                  backgroundColor: "#ffffff",
                  border: `1px solid ${isActive ? "#60a5fa" : "#e2e8f0"}`,
                  boxShadow: isActive ? "0 4px 6px -1px rgba(59, 130, 246, 0.1), 0 2px 4px -1px rgba(59, 130, 246, 0.06)" : "0 1px 2px 0 rgba(0, 0, 0, 0.05)",
                  transition: "all 0.2s"
                }}>
                  <div style={{ fontWeight: 700, color: "#1e293b", fontSize: "14px", display: "flex", justifyContent: "space-between" }}>
                    <span>LOOP {loop.loop_index}</span>
                    <div style={{ display: "flex", gap: "4px", alignItems: "center" }}>
                      {loop.is_sota && <span style={{ fontSize: "10px", color: "#d97706", backgroundColor: "#fef3c7", padding: "2px 6px", borderRadius: "4px" }}>SOTA</span>}
                      {loop.status === "running" && <span style={{ fontSize: "10px", color: "#3b82f6", backgroundColor: "#dbeafe", padding: "2px 6px", borderRadius: "4px" }}>运行中</span>}
                      {loop.status === "failed" && <span style={{ fontSize: "10px", color: "#ef4444", backgroundColor: "#fef2f2", padding: "2px 6px", borderRadius: "4px" }}>失败</span>}
                    </div>
                  </div>
                  <div style={{ fontSize: "11px", color: "#64748b", marginTop: "4px", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em" }}>{loop.action_type || "UNKNOWN"}</div>
                  {/* 指标速览 */}
                  {loop.metrics_json && loop.status === "completed" && (
                    <div style={{ display: "flex", gap: "8px", marginTop: "6px", fontSize: "11px", color: "#475569", fontFamily: "monospace" }}>
                      {loop.metrics_json.IC != null && <span>IC:{typeof loop.metrics_json.IC === "number" ? loop.metrics_json.IC.toFixed(4) : loop.metrics_json.IC}</span>}
                      {loop.metrics_json.sharpe != null && <span>Sh:{typeof loop.metrics_json.sharpe === "number" ? loop.metrics_json.sharpe.toFixed(2) : loop.metrics_json.sharpe}</span>}
                    </div>
                  )}
                  {/* 因子列表：以 SOTA 因子为基准显示保留/删除/新增 */}
                  {thisFactors.length > 0 && (
                    <div style={{ marginTop: "8px" }}>
                      <div style={{ fontSize: "10px", color: "#94a3b8", fontWeight: 600, marginBottom: "4px" }}>
                        因子 {thisFactors.length} 个
                        {sotaFactorSet.size > 0 && loop.loop_index > 0 && (() => {
                          const removed = sotaFactorsArr.filter((f: string) => !thisFactorSet.has(f)).length;
                          const added = newFactors.length;
                          return (
                            <span style={{ marginLeft: "4px", fontWeight: 400 }}>
                              {removed > 0 && <span style={{ color: "#dc2626" }}>−{removed} </span>}
                              {added > 0 && <span style={{ color: "#16a34a" }}>+{added}</span>}
                            </span>
                          );
                        })()}
                      </div>
                      <div style={{ display: "flex", flexWrap: "wrap", gap: "3px" }}>
                        {sotaFactorsArr.map((f: string) => {
                          const kept = thisFactorSet.has(f);
                          return kept ? (
                            <span key={f} style={{ display: "inline-block", padding: "1px 6px", borderRadius: "3px", fontSize: "10px", fontFamily: "monospace", backgroundColor: "#f0f9ff", color: "#0369a1", border: "1px solid #bae6fd" }}>{f}</span>
                          ) : (
                            <span key={f} style={{ display: "inline-block", padding: "1px 6px", borderRadius: "3px", fontSize: "10px", fontFamily: "monospace", backgroundColor: "#fee2e2", color: "#991b1b", border: "1px solid #fecaca", textDecoration: "line-through", opacity: 0.7 }}>{f}</span>
                          );
                        })}
                        {newFactors.map((f: string) => (
                          <span key={f} style={{ display: "inline-block", padding: "1px 6px", borderRadius: "3px", fontSize: "10px", fontFamily: "monospace", backgroundColor: "#dcfce7", color: "#166534", border: "1px solid #bbf7d0" }}>+{f}</span>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
});
