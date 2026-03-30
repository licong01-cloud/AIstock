"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type Node = {
  node_id: string;
  display_name: string;
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
  best_ic: number | null;
  best_sharpe: number | null;
  best_ann_return: number | null;
  best_max_dd: number | null;
  sota_factors: number;
  sota_models: number;
  started_at: string | null;
  finished_at: string | null;
  log_tail: string;
  error_message: string | null;
  remote_task_id?: string | null;
};

export default function TaskDetailPanel({
  task,
  nodes,
  onClose,
}: {
  task: Task;
  nodes: Node[];
  onClose: () => void;
}) {
  const nodeMap = useMemo(() => {
    const m: Record<string, Node> = {};
    for (const n of nodes) m[n.node_id] = n;
    return m;
  }, [nodes]);

  const [tab, setTab] = useState<"overview" | "logs" | "metrics">("overview");
  const [logLines, setLogLines] = useState<string[]>([]);
  const [logLoading, setLogLoading] = useState(false);
  const [logError, setLogError] = useState<string | null>(null);

  // 加载日志预览 (最后 30 行)
  useEffect(() => {
    if (tab === "logs") {
      setLogLoading(true);
      setLogError(null);
      fetch(`${API_BASE}/dispatch/tasks/${task.task_id}/logs/full?offset=0&limit=100`)
        .then(r => {
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          return r.json();
        })
        .then(d => setLogLines(d.lines || []))
        .catch(e => {
          setLogError(`加载日志失败: ${e.message}`);
        })
        .finally(() => setLogLoading(false));
    }
  }, [tab, task.task_id]);

  const openLogWindow = () => {
    window.open(
      `/rdagent/dispatch/log-viewer?task_id=${task.task_id}`,
      "_blank",
      "width=1000,height=700",
    );
  };

  const elapsed = task.started_at
    ? (() => {
        const start = new Date(task.started_at).getTime();
        const end = task.finished_at ? new Date(task.finished_at).getTime() : Date.now();
        const mins = Math.floor((end - start) / 60000);
        return mins >= 60 ? `${Math.floor(mins / 60)}h${mins % 60}m` : `${mins}m`;
      })()
    : "-";

  const tabStyle = (active: boolean) => ({
    padding: "8px 16px", fontSize: 13, fontWeight: active ? 600 : 400,
    borderBottom: active ? "2px solid #6366f1" : "2px solid transparent",
    color: active ? "#6366f1" : "#6b7280",
    cursor: "pointer", background: "none", border: "none",
    borderBottomWidth: 2, borderBottomStyle: "solid" as const,
    borderBottomColor: active ? "#6366f1" : "transparent",
  });

  return (
    <div style={{
      background: "#fff", borderRadius: 12, padding: 20, marginBottom: 24,
      border: "1px solid #e5e7eb",
    }}>
      {/* 头部 */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
        <div>
          <h3 style={{ fontSize: 15, fontWeight: 600, margin: 0, color: "#1f2937" }}>
            任务详情: {task.task_name}
          </h3>
          {task.remote_task_id && (
            <div style={{ fontSize: 11, color: "#9ca3af", fontFamily: "Consolas, monospace", marginTop: 2 }}>
              {task.remote_task_id}
            </div>
          )}
        </div>
        <button onClick={onClose} style={{
          background: "none", border: "none", fontSize: 18, cursor: "pointer", color: "#9ca3af",
        }}>
          ✕
        </button>
      </div>

      {/* Tab 栏 */}
      <div style={{ display: "flex", gap: 0, borderBottom: "1px solid #e5e7eb", marginBottom: 16 }}>
        <button style={tabStyle(tab === "overview")} onClick={() => setTab("overview")}>概览</button>
        <button style={tabStyle(tab === "logs")} onClick={() => setTab("logs")}>日志</button>
        <button style={tabStyle(tab === "metrics")} onClick={() => setTab("metrics")}>指标</button>
      </div>

      {/* 概览 Tab */}
      {tab === "overview" && (
        <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
          <InfoCard label="类型" value={task.task_type} />
          <InfoCard label="节点" value={nodeMap[task.node_id]?.display_name || task.node_id} />
          <InfoCard label="状态" value={task.status} />
          <InfoCard label="进度" value={`${task.current_loop}/${task.total_loops || "?"}`} />
          <InfoCard label="耗时" value={elapsed} />
          {task.error_message && (
            <div style={{
              width: "100%", padding: 12, background: "#fef2f2", borderRadius: 8,
              color: "#991b1b", fontSize: 13,
            }}>
              错误: {task.error_message}
            </div>
          )}
        </div>
      )}

      {/* 日志 Tab */}
      {tab === "logs" && (
        <div>
          <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 8 }}>
            <button
              onClick={openLogWindow}
              style={{
                padding: "4px 12px", fontSize: 12, borderRadius: 6,
                border: "1px solid #6366f1", background: "#eef2ff", cursor: "pointer",
                color: "#4338ca",
              }}
            >
              在新窗口打开
            </button>
          </div>
          <div style={{
            background: "#0f172a", color: "#e2e8f0", borderRadius: 8,
            padding: 12, maxHeight: 300, overflow: "auto",
            fontFamily: "Consolas, monospace", fontSize: 12, lineHeight: 1.6,
          }}>
            {logLoading && <div style={{ color: "#94a3b8" }}>加载中...</div>}
            {!logLoading && logError && (
              <div style={{ color: "#f87171" }}>{logError}</div>
            )}
            {!logLoading && !logError && logLines.length === 0 && (
              <div style={{ color: "#94a3b8" }}>暂无日志</div>
            )}
            {logLines.slice(-30).map((line, i) => (
              <div key={i} style={{ whiteSpace: "pre-wrap", wordBreak: "break-all" }}>{line}</div>
            ))}
          </div>
        </div>
      )}

      {/* 指标 Tab */}
      {tab === "metrics" && (
        <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
          <MetricCard label="IC" value={task.best_ic} format={v => v.toFixed(4)} />
          <MetricCard label="Sharpe" value={task.best_sharpe} format={v => v.toFixed(2)} />
          <MetricCard label="年化收益" value={task.best_ann_return} format={v => `${(v * 100).toFixed(1)}%`} />
          <MetricCard label="最大回撤" value={task.best_max_dd} format={v => `${(v * 100).toFixed(1)}%`} />
          <InfoCard label="SOTA 因子" value={String(task.sota_factors)} />
          <InfoCard label="SOTA 模型" value={String(task.sota_models)} />
        </div>
      )}
    </div>
  );
}

function InfoCard({ label, value }: { label: string; value: string }) {
  return (
    <div style={{
      padding: "8px 16px", background: "#f9fafb", borderRadius: 8,
      minWidth: 100,
    }}>
      <div style={{ fontSize: 11, color: "#6b7280" }}>{label}</div>
      <div style={{ fontSize: 14, fontWeight: 600, color: "#1f2937" }}>{value}</div>
    </div>
  );
}

function MetricCard({ label, value, format }: { label: string; value: number | null; format: (v: number) => string }) {
  return (
    <div style={{
      padding: "8px 16px", background: "#f9fafb", borderRadius: 8,
      minWidth: 100,
    }}>
      <div style={{ fontSize: 11, color: "#6b7280" }}>{label}</div>
      <div style={{ fontSize: 14, fontWeight: 600, color: value != null ? "#1f2937" : "#9ca3af" }}>
        {value != null ? format(value) : "-"}
      </div>
    </div>
  );
}
