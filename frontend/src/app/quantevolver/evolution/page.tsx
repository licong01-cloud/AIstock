"use client";

import React, { useState, useEffect, useCallback, useRef } from "react";
import { 
  Play, Terminal, GitMerge, FileCode2, 
  Activity, ArrowRight, DownloadCloud, CheckCircle2,
  AlertCircle
} from "lucide-react";

const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8001/api/v1";

interface Task {
  task_id: string;
  task_name: string;
  target_desc: string;
  max_loops: number;
  current_loop: number;
  status: string;
  base_experiment_id: string;
  created_at: string;
  updated_at: string;
}

interface Loop {
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

export default function EvolutionDashboard() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [activeTaskId, setActiveTaskId] = useState<string | null>(null);
  const [loops, setLoops] = useState<Loop[]>([]);
  const [activeLoopIndex, setActiveLoopIndex] = useState<number | null>(null);
  
  const [showCreateTask, setShowCreateTask] = useState(false);
  const [newTask, setNewTask] = useState({
    task_name: "",
    target_desc: "",
    max_loops: 10,
    base_experiment_id: ""
  });
  const [isCreating, setIsCreating] = useState(false);

  const [logs, setLogs] = useState<string[]>([
    "[System] 演进控制中心已启动...",
    "[System] 等待连接至 AIstock 演进调度引擎..."
  ]);
  
  const logsEndRef = useRef<HTMLDivElement>(null);
  const eventSourceRef = useRef<EventSource | null>(null);

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

  // 滚动到底部
  useEffect(() => {
    if (logsEndRef.current) {
      logsEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [logs]);

  // 获取任务列表
  const fetchTasks = useCallback(async () => {
    try {
      const res = await fetch(`${API}/quantevolver/evolution/tasks`);
      const data = await res.json();
      if (data.status === "success" && data.data) {
        setTasks(data.data);
      }
    } catch (e) {
      console.error("Failed to fetch tasks:", e);
    }
  }, []);

  useEffect(() => {
    fetchTasks();
    const interval = setInterval(fetchTasks, 10000); // 10秒轮询更新任务状态
    return () => clearInterval(interval);
  }, [fetchTasks]);

  // 获取任务详情和 Loops
  const fetchTaskDetail = useCallback(async (taskId: string) => {
    try {
      const res = await fetch(`${API}/quantevolver/evolution/tasks/${taskId}`);
      const data = await res.json();
      if (data.status === "success" && data.data) {
        setLoops(data.data.loops || []);
        // 如果没有选中的 loop 且有数据，默认选中最新或SOTA
        if (activeLoopIndex === null && data.data.loops && data.data.loops.length > 0) {
          const sotaLoop = data.data.loops.find((l: Loop) => l.is_sota);
          if (sotaLoop) setActiveLoopIndex(sotaLoop.loop_index);
          else setActiveLoopIndex(data.data.loops[data.data.loops.length - 1].loop_index);
        }
      }
    } catch (e) {
      console.error("Failed to fetch task detail:", e);
    }
  }, [activeLoopIndex]);

  // 监听 Task 选中切换
  useEffect(() => {
    if (activeTaskId) {
      fetchTaskDetail(activeTaskId);
      setActiveLoopIndex(null); // 切换任务时重置选中的 loop
      
      // 连接 SSE 日志流
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }
      
      setLogs([`[System] 已连接到任务 ${activeTaskId} 的实时日志流...`]);
      
      const sse = new EventSource(`${API}/quantevolver/evolution/tasks/${activeTaskId}/logs`);
      sse.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.status === "running" && data.logs) {
            setLogs(prev => [...prev.slice(-100), ...data.logs]); // 保留最近100行
          }
        } catch(e) {
          setLogs(prev => [...prev.slice(-100), event.data]);
        }
      };
      
      sse.onerror = (err) => {
        console.error("SSE Error:", err);
      };
      
      eventSourceRef.current = sse;
    }
    
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }
    };
  }, [activeTaskId, fetchTaskDetail]);

  // 提交新建任务
  const handleCreateTask = async () => {
    if (!newTask.task_name || !newTask.target_desc || !newTask.base_experiment_id) {
      alert("请填写完整的任务信息");
      return;
    }
    setIsCreating(true);
    try {
      const res = await fetch(`${API}/quantevolver/evolution/tasks`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(newTask)
      });
      const data = await res.json();
      if (data.status === "success") {
        alert("演进任务创建成功并已在后台启动！");
        setShowCreateTask(false);
        setNewTask({ task_name: "", target_desc: "", max_loops: 10, base_experiment_id: "" });
        fetchTasks();
        setActiveTaskId(data.task_id); // 自动选中新任务
      } else {
        alert("创建任务失败: " + (data.detail || "未知错误"));
      }
    } catch (e: any) {
      alert("创建任务失败: " + (e?.message || "网络错误"));
    } finally {
      setIsCreating(false);
    }
  };

  // 获取当前选中的 Loop
  const activeLoopData = loops.find(l => l.loop_index === activeLoopIndex);
  const prevLoopData = activeLoopData
    ? loops.find(l => l.loop_index === activeLoopData.loop_index - 1)
    : undefined;

  const configDiffLines = React.useMemo(() => {
    if (!activeLoopData?.config_json || !prevLoopData?.config_json) return [] as string[];
    const curr = activeLoopData.config_json;
    const prev = prevLoopData.config_json;
    const keys = Array.from(new Set([...Object.keys(curr), ...Object.keys(prev)])).sort();
    const lines: string[] = [];
    for (const key of keys) {
      const before = JSON.stringify(prev[key]);
      const after = JSON.stringify(curr[key]);
      if (before !== after) {
        lines.push(`${key}: ${before} -> ${after}`);
      }
    }
    return lines;
  }, [activeLoopData, prevLoopData]);

  // 同步资产
  const handleSyncAssets = async (loopId: string) => {
    if (!confirm("确定要将此 Loop 的模型资产同步到实盘环境吗？")) return;
    try {
      const res = await fetch(`${API}/quantevolver/evolution/loops/${loopId}/sync_assets`, {
        method: "POST"
      });
      const data = await res.json();
      if (data.status === "success") {
        alert("资产同步成功！\n保存路径：" + data.local_path);
      } else {
        alert("资产同步失败: " + (data.detail || "未知错误"));
      }
    } catch (e: any) {
      alert("资产同步失败: " + (e?.message || "网络错误"));
    }
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
            <button 
              onClick={() => setShowCreateTask(true)}
              style={{
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
            <div style={{ fontSize: "12px", fontWeight: 600, color: "#64748b", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: "4px" }}>活跃任务 ({tasks.length})</div>
            {tasks.map(task => {
              const isActive = activeTaskId === task.task_id;
              return (
                <div 
                  key={task.task_id} 
                  style={{
                    padding: "16px",
                    borderRadius: "8px",
                    border: `1px solid ${isActive ? "#93c5fd" : "#e2e8f0"}`,
                    backgroundColor: isActive ? "#eff6ff" : "#ffffff",
                    cursor: "pointer",
                    transition: "all 0.2s ease"
                  }}
                  onClick={() => setActiveTaskId(task.task_id)}
                >
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: "8px" }}>
                    <div style={{ fontWeight: 600, color: "#0f172a", fontSize: "14px" }}>{task.task_name}</div>
                    {task.status === 'running' ? (
                      <div style={{ position: "relative", width: "10px", height: "10px", marginTop: "4px" }}>
                        <div style={{ position: "absolute", width: "100%", height: "100%", borderRadius: "50%", backgroundColor: "#22c55e", opacity: 0.7, animation: "ping 1.5s cubic-bezier(0, 0, 0.2, 1) infinite" }}></div>
                        <div style={{ position: "relative", width: "100%", height: "100%", borderRadius: "50%", backgroundColor: "#22c55e" }}></div>
                      </div>
                    ) : (
                      <CheckCircle2 size={16} color={task.status === 'completed' ? "#10b981" : "#94a3b8"} />
                    )}
                  </div>
                  <div style={{ fontSize: "12px", color: "#64748b", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <span style={{ fontFamily: "monospace" }}>{task.task_id.substring(0, 18)}...</span>
                    <span style={{ backgroundColor: isActive ? "#dbeafe" : "#f1f5f9", padding: "2px 8px", borderRadius: "12px", fontWeight: 500 }}>
                      Loop: {task.current_loop} / {task.max_loops}
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
            <div ref={logsEndRef} style={{ animation: "pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite", marginTop: "4px" }}>_</div>
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

              return (
                <div 
                  key={loop.loop_id} 
                  style={{ display: "flex", alignItems: "center", gap: "16px", cursor: "pointer" }}
                  onClick={() => setActiveLoopIndex(loop.loop_index)}
                >
                  <div style={{
                    width: "40px", height: "40px", flexShrink: 0, borderRadius: "50%",
                    display: "flex", alignItems: "center", justifyContent: "center",
                    backgroundColor: iconBg, border: `2px solid ${iconBorder}`, color: iconColor,
                    fontSize: "14px", fontWeight: 700, boxShadow: "0 2px 4px rgba(0,0,0,0.05)",
                    transition: "transform 0.2s"
                  }}>
                    {loop.is_sota ? "⭐" : loop.loop_index}
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
                      {loop.is_sota && <span style={{ fontSize: "10px", color: "#d97706", backgroundColor: "#fef3c7", padding: "2px 6px", borderRadius: "4px" }}>SOTA</span>}
                    </div>
                    <div style={{ fontSize: "11px", color: "#64748b", marginTop: "4px", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em" }}>{loop.action_type || "UNKNOWN"}</div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* 右侧：LOOP 深度看板 */}
      <div style={{ ...cardStyle, flex: 1 }}>
        {activeLoopData ? (
          <>
            <div style={headerStyle}>
              <h2 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#1e293b", display: "flex", alignItems: "center", gap: "8px" }}>
                <FileCode2 color="#10b981" size={20} />
                LOOP {activeLoopData.loop_index} 详情看板
              </h2>
              <button 
                onClick={() => handleSyncAssets(activeLoopData.loop_id)}
                style={{
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
                  {activeLoopData.agent_analysis ? (
                    <>
                      {activeLoopData.agent_analysis.analyst && (
                        <p style={{ margin: "0 0 12px 0", whiteSpace: "pre-wrap" }}><strong style={{ color: "#0f172a" }}>诊断 (Analyst):</strong><br/>{activeLoopData.agent_analysis.analyst}</p>
                      )}
                      {activeLoopData.agent_analysis.researcher && (
                        <p style={{ margin: "0 0 12px 0", whiteSpace: "pre-wrap" }}><strong style={{ color: "#0f172a" }}>决策 (Researcher):</strong><br/>{activeLoopData.agent_analysis.researcher}</p>
                      )}
                      {activeLoopData.agent_analysis.reviewer && (
                        <p style={{ margin: 0, whiteSpace: "pre-wrap" }}><strong style={{ color: "#0f172a" }}>审查 (Reviewer):</strong><br/>{activeLoopData.agent_analysis.reviewer}</p>
                      )}
                    </>
                  ) : (
                    <p style={{ margin: 0, color: "#94a3b8" }}>暂无 Agent 报告数据...</p>
                  )}
                </div>
              </div>

              {/* 回测表现雷达/指标 */}
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "20px" }}>
                <div style={{ backgroundColor: "#ffffff", borderRadius: "8px", border: "1px solid #e2e8f0", padding: "20px", boxShadow: "0 1px 3px rgba(0,0,0,0.05)" }}>
                  <h3 style={{ margin: "0 0 16px 0", fontSize: "13px", fontWeight: 700, color: "#64748b", textTransform: "uppercase", letterSpacing: "0.05em" }}>核心指标</h3>
                  <div style={{ display: "flex", flexDirection: "column", gap: "16px" }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                      <span style={{ fontSize: "14px", fontWeight: 500, color: "#475569" }}>Rank IC</span>
                      <span style={{ fontSize: "20px", fontWeight: 700, fontFamily: "monospace", color: "#059669" }}>
                        {activeLoopData.metrics_json?.IC?.toFixed(4) || "-"}
                      </span>
                    </div>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                      <span style={{ fontSize: "14px", fontWeight: 500, color: "#475569" }}>ICIR</span>
                      <span style={{ fontSize: "20px", fontWeight: 700, fontFamily: "monospace", color: "#059669" }}>
                        {activeLoopData.metrics_json?.ICIR?.toFixed(4) || "-"}
                      </span>
                    </div>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end" }}>
                      <span style={{ fontSize: "14px", fontWeight: 500, color: "#475569" }}>Status</span>
                      <span style={{ fontSize: "16px", fontWeight: 700, fontFamily: "monospace", color: activeLoopData.status === "completed" ? "#059669" : "#e11d48", textTransform: "uppercase" }}>
                        {activeLoopData.status}
                      </span>
                    </div>
                  </div>
                </div>
                
                <div style={{ backgroundColor: "#ffffff", borderRadius: "8px", border: "1px solid #e2e8f0", padding: "20px", boxShadow: "0 1px 3px rgba(0,0,0,0.05)", display: "flex", flexDirection: "column" }}>
                  <h3 style={{ margin: "0 0 16px 0", fontSize: "13px", fontWeight: 700, color: "#64748b", textTransform: "uppercase", letterSpacing: "0.05em" }}>实验配置数据 (Config)</h3>
                  <div style={{ flex: 1, display: "flex", alignItems: "flex-start", justifyContent: "flex-start", color: "#475569", border: "1px solid #e2e8f0", borderRadius: "6px", backgroundColor: "#f8fafc", fontSize: "12px", fontFamily: "monospace", padding: "12px", overflowY: "auto", overflowX: "hidden" }}>
                    <pre style={{ margin: 0, whiteSpace: "pre-wrap", wordWrap: "break-word" }}>
                      {activeLoopData.config_json ? JSON.stringify(activeLoopData.config_json, null, 2) : "No config data available."}
                    </pre>
                  </div>
                </div>
              </div>

              <div style={{ backgroundColor: "#ffffff", borderRadius: "8px", border: "1px solid #e2e8f0", padding: "20px", boxShadow: "0 1px 3px rgba(0,0,0,0.05)" }}>
                <h3 style={{ margin: "0 0 16px 0", fontSize: "13px", fontWeight: 700, color: "#64748b", textTransform: "uppercase", letterSpacing: "0.05em" }}>
                  Config Diff (对比上一轮)
                </h3>
                {activeLoopData.loop_index === 0 ? (
                  <p style={{ margin: 0, color: "#94a3b8" }}>LOOP 0 为初始配置，无上一轮可对比。</p>
                ) : configDiffLines.length === 0 ? (
                  <p style={{ margin: 0, color: "#94a3b8" }}>未检测到配置差异。</p>
                ) : (
                  <ul style={{ margin: 0, paddingLeft: "18px", color: "#334155", fontFamily: "monospace", fontSize: "12px", lineHeight: 1.7 }}>
                    {configDiffLines.map((line, idx) => (
                      <li key={idx}>{line}</li>
                    ))}
                  </ul>
                )}
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
      
      {/* 新建任务弹窗 */}
      {showCreateTask && (
        <div style={{
          position: "fixed", top: 0, left: 0, right: 0, bottom: 0,
          backgroundColor: "rgba(0,0,0,0.5)", zIndex: 100,
          display: "flex", alignItems: "center", justifyContent: "center",
        }}>
          <div style={{
            backgroundColor: "#fff", padding: "24px", borderRadius: "12px",
            width: "480px", boxShadow: "0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04)"
          }}>
            <h2 style={{ margin: "0 0 20px", fontSize: "18px", color: "#1e293b", display: "flex", alignItems: "center", gap: "8px" }}>
              <Play size={20} color="#3b82f6" />
              新建演进任务
            </h2>
            
            <div style={{ display: "flex", flexDirection: "column", gap: "16px" }}>
              <div>
                <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>任务名称</label>
                <input 
                  type="text" 
                  value={newTask.task_name}
                  onChange={e => setNewTask({...newTask, task_name: e.target.value})}
                  placeholder="例如: Alpha158-基于XGBoost的演进"
                  style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box" }}
                />
              </div>
              
              <div>
                <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>基础实验 ID (Base Experiment ID)</label>
                <input 
                  type="text" 
                  value={newTask.base_experiment_id}
                  onChange={e => setNewTask({...newTask, base_experiment_id: e.target.value})}
                  placeholder="输入作为起点的 qe_experiments 的 ID"
                  style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box", fontFamily: "monospace" }}
                />
              </div>

              <div>
                <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>演进目标描述 (给 Agent 的指引)</label>
                <textarea 
                  value={newTask.target_desc}
                  onChange={e => setNewTask({...newTask, target_desc: e.target.value})}
                  placeholder="例如: 尝试提升 ICIR，降低多头波动率，重点探索树模型的深度参数..."
                  rows={3}
                  style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box", resize: "vertical" }}
                />
              </div>
              
              <div>
                <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>最大演进轮次 (Max Loops)</label>
                <input 
                  type="number" 
                  aria-label="最大演进轮次"
                  min={1} max={50}
                  value={newTask.max_loops}
                  onChange={e => setNewTask({...newTask, max_loops: parseInt(e.target.value) || 10})}
                  style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box" }}
                />
              </div>
            </div>
            
            <div style={{ display: "flex", justifyContent: "flex-end", gap: "12px", marginTop: "24px", paddingTop: "16px", borderTop: "1px solid #f1f5f9" }}>
              <button 
                onClick={() => setShowCreateTask(false)}
                style={{ padding: "8px 16px", backgroundColor: "#f1f5f9", color: "#475569", border: "none", borderRadius: "6px", fontSize: "14px", fontWeight: 600, cursor: "pointer" }}
              >
                取消
              </button>
              <button 
                onClick={handleCreateTask}
                disabled={isCreating}
                style={{ padding: "8px 16px", backgroundColor: "#2563eb", color: "#fff", border: "none", borderRadius: "6px", fontSize: "14px", fontWeight: 600, cursor: isCreating ? "not-allowed" : "pointer", opacity: isCreating ? 0.7 : 1 }}
              >
                {isCreating ? "创建中..." : "创建并启动演进"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
