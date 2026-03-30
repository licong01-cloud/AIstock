"use client";

import React, { useMemo, useState } from "react";

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
  best_ic: number | null;
  best_ann_return: number | null;
  best_max_dd: number | null;
  started_at: string | null;
  finished_at: string | null;
  created_at: string;
  remote_task_id?: string | null;
};

const TYPE_SHORT: Record<string, string> = {
  fin_factor: "F",
  fin_model: "M",
  fin_quant: "Q",
  fin_factor_report: "R",
  qe_evolution: "QE",
};

const STATUS_STYLE: Record<string, { icon: string; color: string }> = {
  success: { icon: "\u2713", color: "#22c55e" },
  failed: { icon: "\u2717", color: "#ef4444" },
  canceled: { icon: "\u2013", color: "#9ca3af" },
};

export default function TaskHistoryTable({
  tasks,
  nodes,
  onSelect,
  onRefresh,
}: {
  tasks: Task[];
  nodes: Node[];
  onSelect: (id: string) => void;
  onRefresh: () => void;
}) {
  const nodeMap = useMemo(() => {
    const m: Record<string, Node> = {};
    for (const n of nodes) m[n.node_id] = n;
    return m;
  }, [nodes]);

  const [filterType, setFilterType] = useState("");
  const [filterNode, setFilterNode] = useState("");
  const [filterStatus, setFilterStatus] = useState("");
  const [deleting, setDeleting] = useState<string | null>(null);

  const filteredTasks = tasks.filter(t => {
    if (filterType && t.task_type !== filterType) return false;
    if (filterNode && t.node_id !== filterNode) return false;
    if (filterStatus && t.status !== filterStatus) return false;
    return true;
  });

  const uniqueNodes = [...new Set(tasks.map(t => t.node_id))];

  const handleDelete = async (taskId: string) => {
    if (!confirm("确定删除此任务？将同时删除日志和 workspace")) return;
    setDeleting(taskId);
    try {
      const res = await fetch(`${API_BASE}/dispatch/tasks/${taskId}`, { method: "DELETE" });
      if (!res.ok) {
        const d = await res.json().catch(() => ({ detail: `HTTP ${res.status}` }));
        alert(`删除失败: ${d.detail || d.error || res.status}`);
      } else {
        onRefresh();
      }
    } catch (e: any) {
      alert(`删除失败: ${e.message}`);
    }
    setDeleting(null);
  };

  const formatDuration = (task: Task) => {
    if (!task.started_at || !task.finished_at) return "-";
    const ms = new Date(task.finished_at).getTime() - new Date(task.started_at).getTime();
    const mins = Math.floor(ms / 60000);
    return mins >= 60 ? `${Math.floor(mins / 60)}h${mins % 60}m` : `${mins}m`;
  };

  const selectStyle = {
    padding: "4px 8px", fontSize: 12, borderRadius: 4,
    border: "1px solid #d1d5db", background: "#fff",
  };

  return (
    <div style={{
      background: "#fff", borderRadius: 12, padding: 20,
      border: "1px solid #e5e7eb",
    }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
        <h3 style={{ fontSize: 15, fontWeight: 600, margin: 0, color: "#1f2937" }}>
          历史任务 ({filteredTasks.length})
        </h3>
        <div style={{ display: "flex", gap: 8 }}>
          <select style={selectStyle} value={filterType} onChange={e => setFilterType(e.target.value)}>
            <option value="">全部类型</option>
            {Object.entries(TYPE_SHORT).map(([k, v]) => (
              <option key={k} value={k}>{v} - {k}</option>
            ))}
          </select>
          <select style={selectStyle} value={filterNode} onChange={e => setFilterNode(e.target.value)}>
            <option value="">全部节点</option>
            {uniqueNodes.map(n => <option key={n} value={n}>{nodeMap[n]?.display_name || n}</option>)}
          </select>
          <select style={selectStyle} value={filterStatus} onChange={e => setFilterStatus(e.target.value)}>
            <option value="">全部状态</option>
            <option value="success">成功</option>
            <option value="failed">失败</option>
            <option value="canceled">取消</option>
          </select>
        </div>
      </div>

      {filteredTasks.length === 0 ? (
        <div style={{ color: "#9ca3af", fontSize: 14, textAlign: "center", padding: 24 }}>
          暂无历史任务
        </div>
      ) : (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
            <thead>
              <tr style={{ borderBottom: "1px solid #e5e7eb" }}>
                <th style={thStyle}>类型</th>
                <th style={{ ...thStyle, textAlign: "left" }}>名称</th>
                <th style={thStyle}>节点</th>
                <th style={thStyle}>状态</th>
                <th style={thStyle}>IC</th>
                <th style={thStyle}>年化</th>
                <th style={thStyle}>MaxDD</th>
                <th style={thStyle}>耗时</th>
                <th style={thStyle}>操作</th>
              </tr>
            </thead>
            <tbody>
              {filteredTasks.map(task => {
                const ss = STATUS_STYLE[task.status] || STATUS_STYLE.canceled;
                return (
                  <tr
                    key={task.task_id}
                    onClick={() => onSelect(task.task_id)}
                    style={{ borderBottom: "1px solid #f3f4f6", cursor: "pointer" }}
                    onMouseEnter={e => (e.currentTarget.style.background = "#f9fafb")}
                    onMouseLeave={e => (e.currentTarget.style.background = "transparent")}
                  >
                    <td style={tdStyle}>
                      <span style={{
                        fontSize: 11, fontWeight: 700, padding: "1px 5px",
                        borderRadius: 3, background: "#f3f4f6", color: "#374151",
                      }}>
                        {TYPE_SHORT[task.task_type] || "?"}
                      </span>
                    </td>
                    <td style={{ ...tdStyle, textAlign: "left", maxWidth: 260 }}>
                      <div style={{ whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                        {task.task_name}
                      </div>
                      {task.remote_task_id && (
                        <div style={{ fontSize: 10, color: "#9ca3af", fontFamily: "Consolas, monospace", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                          {task.remote_task_id}
                        </div>
                      )}
                    </td>
                    <td style={tdStyle}>
                      <span style={{ fontSize: 11, color: "#6b7280" }}>{nodeMap[task.node_id]?.display_name || task.node_id}</span>
                    </td>
                    <td style={tdStyle}>
                      <span style={{ color: ss.color, fontWeight: 600 }}>{ss.icon}</span>
                    </td>
                    <td style={tdStyle}>{task.best_ic != null ? task.best_ic.toFixed(4) : "-"}</td>
                    <td style={tdStyle}>
                      {task.best_ann_return != null ? `${(task.best_ann_return * 100).toFixed(1)}%` : "-"}
                    </td>
                    <td style={tdStyle}>
                      {task.best_max_dd != null ? `${(task.best_max_dd * 100).toFixed(1)}%` : "-"}
                    </td>
                    <td style={tdStyle}>{formatDuration(task)}</td>
                    <td style={tdStyle} onClick={e => e.stopPropagation()}>
                      <button
                        onClick={() => handleDelete(task.task_id)}
                        disabled={deleting === task.task_id}
                        style={{
                          padding: "2px 8px", fontSize: 11, borderRadius: 4,
                          border: "1px solid #fca5a5", background: "#fff",
                          color: "#ef4444", cursor: "pointer",
                          opacity: deleting === task.task_id ? 0.5 : 1,
                        }}
                      >
                        删除
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

const thStyle: React.CSSProperties = {
  padding: "8px 10px", fontSize: 12, fontWeight: 500,
  color: "#6b7280", textAlign: "center",
};

const tdStyle: React.CSSProperties = {
  padding: "8px 10px", textAlign: "center",
};
