"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import NodeStatusPanel from "./components/NodeStatusPanel";
import TaskCreatePanel from "./components/TaskCreatePanel";
import ActiveTaskList from "./components/ActiveTaskList";
import TaskDetailPanel from "./components/TaskDetailPanel";
import TaskHistoryTable from "./components/TaskHistoryTable";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type Node = {
  node_id: string;
  display_name: string;
  api_base_url: string;
  gpu_model: string | null;
  gpu_vram_mb: number | null;
  capabilities: string[];
  status: string;
  current_task_id: string | null;
  last_heartbeat: string | null;
  metrics_snapshot: Record<string, any> | null;
  grafana_dashboard_url: string | null;
  prometheus_target: string | null;
};

type Task = {
  task_id: string;
  task_name: string;
  task_type: string;
  node_id: string;
  status: string;
  config: Record<string, any>;
  current_loop: number;
  total_loops: number | null;
  progress_pct: number;
  remote_task_id: string | null;
  qe_task_id: string | null;
  best_ic: number | null;
  best_sharpe: number | null;
  best_ann_return: number | null;
  best_max_dd: number | null;
  sota_factors: number;
  sota_models: number;
  started_at: string | null;
  finished_at: string | null;
  created_at: string;
  log_tail: string;
  error_message: string | null;
};

type DashboardData = {
  node_stats: Record<string, number>;
  task_stats: Record<string, number>;
};

export default function DispatchPage() {
  const [nodes, setNodes] = useState<Node[]>([]);
  const [activeTasks, setActiveTasks] = useState<Task[]>([]);
  const [historyTasks, setHistoryTasks] = useState<Task[]>([]);
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);
  const [dashboard, setDashboard] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const loadNodes = useCallback(async () => {
    const res = await fetch(`${API_BASE}/dispatch/nodes`);
    if (!res.ok) throw new Error(`加载节点失败: ${res.status}`);
    setNodes(await res.json());
  }, []);

  const loadTasks = useCallback(async () => {
    const res = await fetch(`${API_BASE}/dispatch/tasks?limit=100`);
    if (!res.ok) throw new Error(`加载任务失败: ${res.status}`);
    const allTasks: Task[] = await res.json();
    setActiveTasks(allTasks.filter(t => ["pending", "running", "queued", "paused"].includes(t.status)));
    setHistoryTasks(allTasks.filter(t => ["success", "failed", "canceled"].includes(t.status)));
  }, []);

  const loadDashboard = useCallback(async () => {
    const res = await fetch(`${API_BASE}/dispatch/dashboard`);
    if (!res.ok) throw new Error(`加载仪表盘失败: ${res.status}`);
    setDashboard(await res.json());
  }, []);

  const loadAll = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      await Promise.all([loadNodes(), loadTasks(), loadDashboard()]);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }, [loadNodes, loadTasks, loadDashboard]);

  useEffect(() => {
    loadAll();
    pollingRef.current = setInterval(async () => {
      try { await loadNodes(); } catch (e) { console.warn("轮询节点失败:", e); }
      try { await loadTasks(); } catch (e) { console.warn("轮询任务失败:", e); }
      try { await loadDashboard(); } catch (e) { console.warn("轮询仪表盘失败:", e); }
    }, 15000);
    return () => { if (pollingRef.current) clearInterval(pollingRef.current); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleTaskCreated = useCallback(() => {
    loadTasks();
    loadNodes();
    loadDashboard();
  }, [loadTasks, loadNodes, loadDashboard]);

  const selectedTask = useMemo(
    () => [...activeTasks, ...historyTasks].find(t => t.task_id === selectedTaskId) || null,
    [activeTasks, historyTasks, selectedTaskId],
  );

  const nodesOnline = dashboard?.node_stats?.online || 0;
  const nodesTotal = nodes.length;
  const tasksRunning = dashboard?.task_stats?.running || 0;
  const tasksSuccess = dashboard?.task_stats?.success || 0;
  const tasksFailed = dashboard?.task_stats?.failed || 0;

  return (
    <div style={{ padding: 24, maxWidth: 1600, margin: "0 auto" }}>
      {/* 头部 + 统计摘要 */}
      <div style={{
        background: "linear-gradient(135deg, #6366f1 0%, #8b5cf6 50%, #a855f7 100%)",
        borderRadius: 16, padding: "24px 32px", color: "#fff", marginBottom: 24,
      }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
          <div>
            <h1 style={{ fontSize: 24, fontWeight: 700, margin: 0 }}>RDAgent 任务调度中心</h1>
            <p style={{ fontSize: 14, opacity: 0.85, margin: "4px 0 0" }}>
              多节点任务创建、监控、管理
            </p>
          </div>
          {dashboard && (
            <div style={{ display: "flex", gap: 24 }}>
              <DashStat label="节点在线" value={`${nodesOnline}/${nodesTotal}`} />
              <DashStat label="运行中" value={String(tasksRunning)} />
              <DashStat label="已完成" value={String(tasksSuccess)} />
              <DashStat label="失败" value={String(tasksFailed)} />
            </div>
          )}
        </div>
      </div>

      {error && (
        <div style={{
          padding: 12, background: "#fee2e2", border: "1px solid #fecaca",
          borderRadius: 8, marginBottom: 16, color: "#b91c1c", fontSize: 13,
        }}>
          {error}
          <button onClick={loadAll} style={{ marginLeft: 12, fontSize: 12, cursor: "pointer" }}>重试</button>
        </div>
      )}

      {/* 主体: 左栏(节点) + 右栏(创建+活跃任务) */}
      <div style={{ display: "flex", gap: 24, marginBottom: 24 }}>
        {/* 左栏: 节点状态 */}
        <div style={{ width: 320, flexShrink: 0 }}>
          <NodeStatusPanel nodes={nodes} onRefresh={loadNodes} />
        </div>

        {/* 右栏 */}
        <div style={{ flex: 1, minWidth: 0 }}>
          {/* 任务创建 */}
          <TaskCreatePanel nodes={nodes} onCreated={handleTaskCreated} />

          {/* 活跃任务列表 */}
          <ActiveTaskList
            tasks={activeTasks}
            nodes={nodes}
            selectedTaskId={selectedTaskId}
            onSelect={setSelectedTaskId}
            onRefresh={loadTasks}
          />
        </div>
      </div>

      {/* 任务详情 */}
      {selectedTask && (
        <TaskDetailPanel task={selectedTask} nodes={nodes} onClose={() => setSelectedTaskId(null)} />
      )}

      {/* 历史任务 */}
      <TaskHistoryTable
        tasks={historyTasks}
        nodes={nodes}
        onSelect={setSelectedTaskId}
        onRefresh={loadTasks}
      />
    </div>
  );
}

function DashStat({ label, value }: { label: string; value: string }) {
  return (
    <div style={{ textAlign: "center" }}>
      <div style={{ fontSize: 22, fontWeight: 700 }}>{value}</div>
      <div style={{ fontSize: 11, opacity: 0.8 }}>{label}</div>
    </div>
  );
}
