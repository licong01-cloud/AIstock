"use client";

import React, { useMemo } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type Node = {
  node_id: string;
  display_name: string;
  status: string;
};

type Task = {
  task_id: string;
  task_name: string;
  task_type: string;
  node_id: string;
  status: string;
  current_loop: number;
  total_loops: number | null;
  progress_pct: number;
  started_at: string | null;
  config?: Record<string, unknown>;
  remote_task_id?: string | null;
};

const TYPE_BADGES: Record<string, { label: string; bg: string; color: string }> = {
  fin_factor: { label: "F", bg: "#dbeafe", color: "#1d4ed8" },
  fin_model: { label: "M", bg: "#fce7f3", color: "#be185d" },
  fin_quant: { label: "Q", bg: "#dcfce7", color: "#15803d" },
  fin_factor_report: { label: "R", bg: "#fef3c7", color: "#92400e" },
  qe_evolution: { label: "QE", bg: "#ede9fe", color: "#6d28d9" },
};

const STATUS_ICONS: Record<string, string> = {
  pending: "\u23F3",  // ⏳
  running: "\u25B6",  // ▶
  queued: "\u23F3",   // ⏳
  paused: "\u23F8",   // ⏸
};

const NODE_STATUS_DOT: Record<string, string> = {
  online: "#22c55e",
  busy: "#3b82f6",
  offline: "#9ca3af",
};

export default function ActiveTaskList({
  tasks,
  nodes,
  selectedTaskId,
  onSelect,
  onRefresh,
}: {
  tasks: Task[];
  nodes: Node[];
  selectedTaskId: string | null;
  onSelect: (id: string) => void;
  onRefresh: () => void;
}) {
  const nodeMap = useMemo(() => {
    const m: Record<string, Node> = {};
    for (const n of nodes) m[n.node_id] = n;
    return m;
  }, [nodes]);

  const handleAction = async (taskId: string, action: "pause" | "resume" | "cancel") => {
    const res = await fetch(`${API_BASE}/dispatch/tasks/${taskId}/${action}`, { method: "POST" });
    if (!res.ok) {
      const d = await res.json().catch(() => ({ detail: `HTTP ${res.status}` }));
      alert(`操作失败: ${d.detail || d.error || res.status}`);
      return;
    }
    onRefresh();
  };

  const openLogViewer = (taskId: string) => {
    window.open(
      `/rdagent/dispatch/log-viewer?task_id=${taskId}`,
      "_blank",
      "width=1000,height=700",
    );
  };

  return (
    <div style={{
      background: "#fff", borderRadius: 12, padding: 20,
      border: "1px solid #e5e7eb", marginBottom: 20,
    }}>
      <h3 style={{ fontSize: 15, fontWeight: 600, margin: "0 0 12px", color: "#1f2937" }}>
        活跃任务 ({tasks.length})
      </h3>

      <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        {nodes.map(node => {
          const nodeTasks = tasks.filter(t => t.node_id === node.node_id);
          const dotColor = NODE_STATUS_DOT[node.status] || NODE_STATUS_DOT.offline;

          return (
            <div key={node.node_id} style={{
              border: "1px solid #e5e7eb", borderRadius: 8, overflow: "hidden",
            }}>
              {/* 节点标题 */}
              <div style={{
                display: "flex", alignItems: "center", gap: 8,
                padding: "8px 12px", background: "#f9fafb",
                borderBottom: "1px solid #e5e7eb",
              }}>
                <span style={{
                  width: 8, height: 8, borderRadius: "50%",
                  background: dotColor, flexShrink: 0,
                }} />
                <span style={{ fontSize: 13, fontWeight: 600, color: "#1f2937" }}>
                  {node.display_name}
                </span>
                <span style={{ fontSize: 11, color: "#9ca3af" }}>
                  ({node.status})
                </span>
              </div>

              {/* 该节点的任务列表 */}
              {nodeTasks.length === 0 ? (
                <div style={{ color: "#9ca3af", fontSize: 13, textAlign: "center", padding: 12 }}>
                  暂无活跃任务
                </div>
              ) : (
                <div style={{ display: "flex", flexDirection: "column" }}>
                  {nodeTasks.map(task => {
                    const badge = task.config?.source === "manual"
                      ? { label: "手工", bg: "#fef3c7", color: "#92400e" }
                      : (TYPE_BADGES[task.task_type] || TYPE_BADGES.fin_factor);
                    const pct = task.progress_pct || 0;
                    const isSelected = task.task_id === selectedTaskId;

                    return (
                      <div
                        key={task.task_id}
                        onClick={() => onSelect(task.task_id)}
                        style={{
                          display: "flex", alignItems: "center", gap: 10, padding: "10px 12px",
                          cursor: "pointer",
                          background: isSelected ? "#eef2ff" : "#fff",
                          borderBottom: "1px solid #f3f4f6",
                          transition: "all 0.15s",
                        }}
                      >
                        {/* 类型标签 */}
                        <span style={{
                          fontSize: 11, fontWeight: 700, padding: "2px 6px", borderRadius: 4,
                          background: badge.bg, color: badge.color, flexShrink: 0,
                        }}>
                          {badge.label}
                        </span>

                        {/* 名称 + Task ID */}
                        <div style={{ flex: 1, minWidth: 0 }}>
                          <div style={{ fontSize: 13, fontWeight: 500, color: "#1f2937", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                            {task.task_name}
                          </div>
                          {task.remote_task_id && (
                            <div style={{ fontSize: 10, color: "#9ca3af", fontFamily: "Consolas, monospace", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                              {task.remote_task_id}
                            </div>
                          )}
                        </div>

                        {/* 状态 */}
                        <span style={{ fontSize: 13, flexShrink: 0 }}>
                          {STATUS_ICONS[task.status] || ""} {task.status}
                        </span>

                        {/* 进度条 */}
                        <div style={{ width: 100, flexShrink: 0 }}>
                          <div style={{
                            height: 6, background: "#e5e7eb", borderRadius: 3, overflow: "hidden",
                          }}>
                            <div style={{
                              height: "100%", width: `${Math.min(pct, 100)}%`,
                              background: "#6366f1", borderRadius: 3,
                              transition: "width 0.3s",
                            }} />
                          </div>
                          <div style={{ fontSize: 10, color: "#6b7280", textAlign: "center", marginTop: 2 }}>
                            {task.current_loop}/{task.total_loops || "?"} ({pct.toFixed(0)}%)
                          </div>
                        </div>

                        {/* 操作按钮 */}
                        <div style={{ display: "flex", gap: 4, flexShrink: 0 }}
                             onClick={e => e.stopPropagation()}>
                          {task.status === "running" && (
                            <ActionBtn label="⏸" title="暂停" onClick={() => handleAction(task.task_id, "pause")} />
                          )}
                          {task.status === "paused" && (
                            <ActionBtn label="▶" title="恢复" onClick={() => handleAction(task.task_id, "resume")} />
                          )}
                          <ActionBtn label="⏹" title="取消" onClick={() => handleAction(task.task_id, "cancel")} />
                          <ActionBtn label="📋" title="日志" onClick={() => openLogViewer(task.task_id)} />
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function ActionBtn({ label, title, onClick }: { label: string; title: string; onClick: () => void }) {
  return (
    <button
      title={title}
      onClick={onClick}
      style={{
        width: 26, height: 26, borderRadius: 4, border: "1px solid #d1d5db",
        background: "#fff", cursor: "pointer", fontSize: 12,
        display: "flex", alignItems: "center", justifyContent: "center",
      }}
    >
      {label}
    </button>
  );
}
