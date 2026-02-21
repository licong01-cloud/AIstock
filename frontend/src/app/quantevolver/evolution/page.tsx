"use client";

import React, { useState } from "react";
import { 
  Play, Terminal, GitMerge, FileCode2, 
  Activity, ArrowRight, DownloadCloud, CheckCircle2,
  AlertCircle
} from "lucide-react";

export default function EvolutionDashboard() {
  const [activeTaskId, setActiveTaskId] = useState<string | null>(null);
  const [activeLoop, setActiveLoop] = useState<number | null>(null);
  const [logs] = useState<string[]>([
    "[System] 演进控制中心已启动...",
    "[System] 等待连接至 AIstock 演进调度引擎..."
  ]);

  const mockTasks = [
    { id: "Evo_20260221_01", name: "Alpha挖掘与调优-001", status: "running", currentLoop: 3, maxLoops: 10 },
    { id: "Evo_20260220_02", name: "稳健收益策略演进", status: "completed", currentLoop: 10, maxLoops: 10 },
  ];

  const mockLoops = [
    { index: 0, status: "completed", isSota: false, action: "initial", score: 65 },
    { index: 1, status: "completed", isSota: true, action: "factor_adjust", score: 72 },
    { index: 2, status: "completed", isSota: false, action: "param_tune", score: 68 },
    { index: 3, status: "running", isSota: false, action: "factor_adjust", score: 0 },
  ];

  // 公共样式常量
  const cardStyle: React.CSSProperties = {
    backgroundColor: "#ffffff",
    borderRadius: "12px",
    boxShadow: "0 4px 12px rgba(0, 0, 0, 0.05)",
    display: "flex",
    flexDirection: "column",
    overflow: "hidden",
    border: "1px solid rgba(255, 255, 255, 0.2)",
  };

  const headerStyle: React.CSSProperties = {
    padding: "16px 20px",
    borderBottom: "1px solid #f1f5f9",
    backgroundColor: "#f8fafc",
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
  };

  return (
    <div style={{ 
      display: "flex", 
      height: "calc(100vh - 48px)", 
      gap: "24px", 
      padding: "24px", 
      boxSizing: "border-box",
      fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif"
    }}>
      
      {/* 左侧：任务控制中心与实时终端 */}
      <div style={{ flex: "0 0 350px", display: "flex", flexDirection: "column", gap: "24px" }}>
        
        {/* 任务列表卡片 */}
        <div style={{ ...cardStyle, flex: 1 }}>
          <div style={headerStyle}>
            <h2 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#1e293b", display: "flex", alignItems: "center", gap: "8px" }}>
              <Activity color="#3b82f6" size={20} />
              演进控制中心
            </h2>
            <button style={{
              padding: "6px 12px",
              backgroundColor: "#2563eb",
              color: "#fff",
              border: "none",
              borderRadius: "6px",
              fontSize: "13px",
              fontWeight: 600,
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              gap: "4px",
              boxShadow: "0 2px 4px rgba(37, 99, 235, 0.2)"
            }}>
              <Play size={14} />
              新建任务
            </button>
          </div>

          <div style={{ flex: 1, overflowY: "auto", padding: "16px", display: "flex", flexDirection: "column", gap: "12px" }}>
            <div style={{ fontSize: "12px", fontWeight: 600, color: "#64748b", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: "4px" }}>活跃任务</div>
            {mockTasks.map(task => {
              const isActive = activeTaskId === task.id;
              return (
                <div 
                  key={task.id} 
                  style={{
                    padding: "16px",
                    borderRadius: "8px",
                    border: `1px solid ${isActive ? "#93c5fd" : "#e2e8f0"}`,
                    backgroundColor: isActive ? "#eff6ff" : "#ffffff",
                    cursor: "pointer",
                    transition: "all 0.2s ease"
                  }}
                  onClick={() => setActiveTaskId(task.id)}
                >
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: "8px" }}>
                    <div style={{ fontWeight: 600, color: "#0f172a", fontSize: "14px" }}>{task.name}</div>
                    {task.status === 'running' ? (
                      <div style={{ position: "relative", width: "10px", height: "10px", marginTop: "4px" }}>
                        <div style={{ position: "absolute", width: "100%", height: "100%", borderRadius: "50%", backgroundColor: "#22c55e", opacity: 0.7, animation: "ping 1.5s cubic-bezier(0, 0, 0.2, 1) infinite" }}></div>
                        <div style={{ position: "relative", width: "100%", height: "100%", borderRadius: "50%", backgroundColor: "#22c55e" }}></div>
                      </div>
                    ) : (
                      <CheckCircle2 size={16} color="#10b981" />
                    )}
                  </div>
                  <div style={{ fontSize: "12px", color: "#64748b", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <span style={{ fontFamily: "monospace" }}>{task.id}</span>
                    <span style={{ backgroundColor: isActive ? "#dbeafe" : "#f1f5f9", padding: "2px 8px", borderRadius: "12px", fontWeight: 500 }}>
                      Loop: {task.currentLoop} / {task.maxLoops}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* 实时终端卡片 */}
        <div style={{ ...cardStyle, height: "260px", backgroundColor: "#0f172a" }}>
          <div style={{ padding: "8px 16px", backgroundColor: "#020617", borderBottom: "1px solid #1e293b", display: "flex", alignItems: "center", gap: "8px", color: "#94a3b8", fontSize: "12px", fontFamily: "monospace" }}>
            <Terminal size={14} />
            Live Logs
          </div>
          <div style={{ flex: 1, overflowY: "auto", padding: "16px", fontFamily: "'Fira Code', Consolas, monospace", fontSize: "12px", color: "#4ade80", lineHeight: 1.6 }}>
            {logs.map((log, i) => (
              <div key={i}>{log}</div>
            ))}
            <div style={{ animation: "pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite", marginTop: "4px" }}>_</div>
          </div>
        </div>

      </div>

      {/* 中间：演进血脉拓扑树 */}
      <div style={{ ...cardStyle, flex: "0 0 300px" }}>
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
            {mockLoops.map(loop => {
              const isActive = activeLoop === loop.index;
              let iconBg = "#f1f5f9";
              let iconBorder = "#cbd5e1";
              let iconColor = "#64748b";

              if (loop.isSota) {
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

              return (
                <div 
                  key={loop.index} 
                  style={{ display: "flex", alignItems: "center", gap: "16px", cursor: "pointer" }}
                  onClick={() => setActiveLoop(loop.index)}
                >
                  <div style={{
                    width: "40px", height: "40px", flexShrink: 0, borderRadius: "50%",
                    display: "flex", alignItems: "center", justifyContent: "center",
                    backgroundColor: iconBg, border: `2px solid ${iconBorder}`, color: iconColor,
                    fontSize: "14px", fontWeight: 700, boxShadow: "0 2px 4px rgba(0,0,0,0.05)",
                    transition: "transform 0.2s"
                  }}>
                    {loop.isSota ? "⭐" : loop.index}
                  </div>
                  <div style={{
                    flex: 1, padding: "12px 16px", borderRadius: "8px",
                    backgroundColor: "#ffffff",
                    border: `1px solid ${isActive ? "#60a5fa" : "#e2e8f0"}`,
                    boxShadow: isActive ? "0 4px 6px -1px rgba(59, 130, 246, 0.1), 0 2px 4px -1px rgba(59, 130, 246, 0.06)" : "0 1px 2px 0 rgba(0, 0, 0, 0.05)",
                    transition: "all 0.2s"
                  }}>
                    <div style={{ fontWeight: 700, color: "#1e293b", fontSize: "14px" }}>LOOP {loop.index}</div>
                    <div style={{ fontSize: "11px", color: "#64748b", marginTop: "4px", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em" }}>{loop.action}</div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* 右侧：LOOP 深度看板 */}
      <div style={{ ...cardStyle, flex: 1 }}>
        {activeLoop !== null ? (
          <>
            <div style={headerStyle}>
              <h2 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#1e293b", display: "flex", alignItems: "center", gap: "8px" }}>
                <FileCode2 color="#10b981" size={20} />
                LOOP {activeLoop} 详情看板
              </h2>
              <button style={{
                padding: "6px 16px", backgroundColor: "#10b981", color: "#fff", border: "none", borderRadius: "6px",
                fontSize: "13px", fontWeight: 600, cursor: "pointer", display: "flex", alignItems: "center", gap: "6px",
                boxShadow: "0 2px 4px rgba(16, 185, 129, 0.2)"
              }}>
                <DownloadCloud size={16} />
                一键同步资产
              </button>
            </div>
            
            <div style={{ flex: 1, overflowY: "auto", padding: "24px", display: "flex", flexDirection: "column", gap: "24px", backgroundColor: "#fafaf9" }}>
              
              {/* Agent 诊断报告 */}
              <div style={{ backgroundColor: "#ffffff", borderRadius: "8px", border: "1px solid #e2e8f0", padding: "20px", boxShadow: "0 1px 3px rgba(0,0,0,0.05)" }}>
                <h3 style={{ margin: "0 0 16px 0", fontSize: "13px", fontWeight: 700, color: "#3b82f6", textTransform: "uppercase", letterSpacing: "0.05em", display: "flex", alignItems: "center", gap: "6px" }}>
                  <AlertCircle size={16} />
                  Agent 结案陈词 & 决策逻辑
                </h3>
                <div style={{ fontSize: "14px", color: "#334155", lineHeight: 1.6, backgroundColor: "#f8fafc", padding: "16px", borderRadius: "6px", border: "1px solid #f1f5f9" }}>
                  <p style={{ margin: "0 0 12px 0" }}><strong style={{ color: "#0f172a" }}>诊断 (Analyst):</strong> 本轮多空收益不对称，多头表现优异但空头衰减极快，且相关性矩阵显示新加入的 3 个因子存在轻微共线性。</p>
                  <p style={{ margin: "0 0 12px 0" }}><strong style={{ color: "#0f172a" }}>决策 (Researcher):</strong> 建议进入调参分支，提升模型对特征的正则化惩罚以抑制空头过拟合。</p>
                  <p style={{ margin: 0 }}><strong style={{ color: "#0f172a" }}>审查 (Reviewer):</strong> 配置合法，将 LightGBM 的 reg_lambda 从 0.1 提升至 1.0。</p>
                </div>
              </div>

              {/* 回测表现雷达/指标 */}
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "20px" }}>
                <div style={{ backgroundColor: "#ffffff", borderRadius: "8px", border: "1px solid #e2e8f0", padding: "20px", boxShadow: "0 1px 3px rgba(0,0,0,0.05)" }}>
                  <h3 style={{ margin: "0 0 16px 0", fontSize: "13px", fontWeight: 700, color: "#64748b", textTransform: "uppercase", letterSpacing: "0.05em" }}>核心指标</h3>
                  <div style={{ display: "flex", flexDirection: "column", gap: "16px" }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                      <span style={{ fontSize: "14px", fontWeight: 500, color: "#475569" }}>Rank IC</span>
                      <span style={{ fontSize: "20px", fontWeight: 700, fontFamily: "monospace", color: "#059669" }}>0.054</span>
                    </div>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                      <span style={{ fontSize: "14px", fontWeight: 500, color: "#475569" }}>ICIR</span>
                      <span style={{ fontSize: "20px", fontWeight: 700, fontFamily: "monospace", color: "#059669" }}>0.68</span>
                    </div>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end" }}>
                      <span style={{ fontSize: "14px", fontWeight: 500, color: "#475569" }}>Max Drawdown</span>
                      <span style={{ fontSize: "20px", fontWeight: 700, fontFamily: "monospace", color: "#e11d48" }}>-12.4%</span>
                    </div>
                  </div>
                </div>
                
                <div style={{ backgroundColor: "#ffffff", borderRadius: "8px", border: "1px solid #e2e8f0", padding: "20px", boxShadow: "0 1px 3px rgba(0,0,0,0.05)", display: "flex", flexDirection: "column" }}>
                  <h3 style={{ margin: "0 0 16px 0", fontSize: "13px", fontWeight: 700, color: "#64748b", textTransform: "uppercase", letterSpacing: "0.05em" }}>多维分析雷达图 (Mock)</h3>
                  <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", color: "#94a3b8", border: "2px dashed #e2e8f0", borderRadius: "6px", backgroundColor: "#f8fafc", fontSize: "14px", fontWeight: 500 }}>
                    [ 雷达图表区域 ]
                  </div>
                </div>
              </div>

              {/* 配置 Diff */}
              <div style={{ backgroundColor: "#ffffff", borderRadius: "8px", border: "1px solid #e2e8f0", padding: "20px", boxShadow: "0 1px 3px rgba(0,0,0,0.05)" }}>
                <h3 style={{ margin: "0 0 16px 0", fontSize: "13px", fontWeight: 700, color: "#64748b", textTransform: "uppercase", letterSpacing: "0.05em" }}>Config Diff (对比上一轮)</h3>
                <div style={{ fontFamily: "'Fira Code', Consolas, monospace", fontSize: "13px", padding: "16px", backgroundColor: "#f8fafc", borderRadius: "6px", border: "1px solid #e2e8f0", overflowX: "auto" }}>
                  <div style={{ color: "#475569" }}>  model_params:</div>
                  <div style={{ color: "#e11d48", backgroundColor: "#ffe4e6", padding: "2px 8px", margin: "4px 0", borderRadius: "4px" }}>-   reg_lambda: 0.1</div>
                  <div style={{ color: "#059669", backgroundColor: "#d1fae5", padding: "2px 8px", margin: "4px 0", borderRadius: "4px" }}>+   reg_lambda: 1.0</div>
                  <div style={{ color: "#475569", marginTop: "12px" }}>  factor_list:</div>
                  <div style={{ color: "#475569" }}>    - Alpha158_Vol</div>
                  <div style={{ color: "#94a3b8", fontStyle: "italic", marginTop: "4px" }}>    ... (保持不变)</div>
                </div>
              </div>
            </div>
          </>
        ) : (
          <div style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", color: "#94a3b8", backgroundColor: "#f8fafc" }}>
            <ArrowRight size={48} style={{ marginBottom: "16px", opacity: 0.3 }} />
            <p style={{ fontSize: "16px", fontWeight: 500 }}>请在左侧选择一个任务并在拓扑树中点击具体的 LOOP</p>
          </div>
        )}
      </div>
      
    </div>
  );
}
