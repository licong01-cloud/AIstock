"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Activity, DownloadCloud, RefreshCw, Trash2 } from "lucide-react";
import Link from "next/link";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";
const PAGE_SIZE = 50;
const DELETE_APPROVAL_MESSAGE = "删除属于写操作，当前设计实现为只读查询；如需启用删除端点，请单独审批写入范围。";

type CombineTask = {
  task_id: string;
  task_name: string;
  task_type: "multi_alpha_combine";
  status: string;
  current_loop: number;
  max_loops: number;
  created_at: string;
  updated_at: string;
  roster_hash: string;
  normalize_method: string;
  walk_forward_signature: string;
  available_schemes?: string[];
  default_scheme?: string;
  phase?: string | null;
  running_count?: number;
};

type TaskListResponse = {
  tasks: CombineTask[];
  count: number;
  total: number;
  limit: number;
  offset: number;
};

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

function normalizeError(detail: any, fallback: string): string {
  if (!detail) return fallback;
  if (typeof detail === "string") return detail;
  if (typeof detail === "object") {
    const code = detail.reason_code ? `reason_code=${detail.reason_code}` : "";
    const message = detail.message || detail.detail || fallback;
    return [code, message].filter(Boolean).join(": ");
  }
  return fallback;
}

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  const json = await response.json().catch(() => ({}));
  if (!response.ok || json.status !== "success") {
    throw new Error(normalizeError(json.detail || json.message, `HTTP ${response.status}`));
  }
  return json.data as T;
}

function formatTime(value?: string | null): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function taskStatusLabel(status: string): string {
  return combineStatusInfo(status).label;
}

function combineStatusInfo(status: string): { color: string; bgColor: string; label: string } {
  switch (status === "succeeded" ? "completed" : status) {
    case "running":
      return { label: "运行中", color: "#0369a1", bgColor: "#e0f2fe" };
    case "completed":
      return { label: "已完成", color: "#047857", bgColor: "#d1fae5" };
    case "failed":
    case "partial_failed":
      return { label: status === "partial_failed" ? "部分失败" : "已失败", color: "#b91c1c", bgColor: "#fee2e2" };
    case "pending":
      return { label: "等待调度", color: "#64748b", bgColor: "#f1f5f9" };
    default:
      return { label: status || "-", color: "#64748b", bgColor: "#f1f5f9" };
  }
}

function exportTaskJson(task: CombineTask) {
  const payload = JSON.stringify(task, null, 2);
  const blob = new Blob([payload], { type: "application/json;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `${task.task_id.replace(/[^a-zA-Z0-9_.-]+/g, "_")}.json`;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function schemeText(task: CombineTask): string {
  const schemes = task.available_schemes || [];
  if (schemes.length === 0) return task.default_scheme || "ic_weighted";
  return schemes.join(" / ");
}

export default function MultiAlphaCombineBacktestPage() {
  const [tasks, setTasks] = useState<CombineTask[]>([]);
  const [statusFilter, setStatusFilter] = useState<"all" | "running" | "completed" | "failed">("all");
  const [offset, setOffset] = useState(0);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastLoadedAt, setLastLoadedAt] = useState<string | null>(null);

  const loadTasks = useCallback(async () => {
    setLoading(true);
    setError(null);
    const params = new URLSearchParams({ limit: String(PAGE_SIZE), offset: String(offset) });
    if (statusFilter !== "all") params.set("status", statusFilter);
    try {
      const data = await fetchJson<TaskListResponse>(`${API}/multi-alpha/combine/tasks?${params.toString()}`);
      setTasks(data.tasks || []);
      setTotal(data.total || 0);
      setLastLoadedAt(new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }));
    } catch (exc) {
      setTasks([]);
      setTotal(0);
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setLoading(false);
    }
  }, [offset, statusFilter]);

  useEffect(() => {
    void loadTasks();
  }, [loadTasks]);

  const hasRunningTask = useMemo(() => tasks.some((task) => task.status === "running"), [tasks]);
  const currentPage = Math.floor(offset / PAGE_SIZE) + 1;
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

  return (
    <div style={{
      display: "flex",
      flexDirection: "column",
      minHeight: "calc(100vh - 48px)",
      gap: "16px",
      padding: "24px",
      boxSizing: "border-box",
      backgroundColor: "#f1f5f9",
      fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif"
    }}>
      <div style={{ ...cardStyle }}>
        <div style={{ display: "flex", minHeight: 0 }}>
          <div style={{ flex: "0 0 180px", display: "flex", flexDirection: "column", gap: "12px", padding: "16px 20px", borderRight: "1px solid #f1f5f9", backgroundColor: "#f8fafc" }}>
            <h2 style={{ margin: 0, fontSize: "15px", fontWeight: 700, color: "#1e293b", display: "flex", alignItems: "center", gap: "8px" }}>
              <Activity color="#3b82f6" size={18} />
              组合回测控制中心
            </h2>
          <select
            value={statusFilter}
            onChange={(event) => { setOffset(0); setStatusFilter(event.target.value as any); }}
            style={{ padding: "7px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px", color: "#475569", backgroundColor: "#fff", width: "100%" }}
          >
            <option value="all">全部状态</option>
            <option value="running">运行中</option>
            <option value="completed">已完成</option>
            <option value="failed">已失败</option>
          </select>
          <button
            onClick={() => void loadTasks()}
            title={hasRunningTask ? "存在运行中任务，建议定期刷新" : "手动刷新"}
            style={{
              padding: "6px 14px",
              backgroundColor: "#fff",
              color: "#64748b",
              border: "1px solid #e2e8f0",
              borderRadius: "6px",
              fontSize: "12px",
              fontWeight: 600,
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              gap: "6px",
            }}>
            <RefreshCw size={12} />
            刷新
            <span style={{ fontSize: "10px", color: "#94a3b8", fontWeight: 400 }}>
              {lastLoadedAt || "手动"}
            </span>
          </button>
          <div style={{ display: "grid", gap: 4, padding: "8px 10px", borderRadius: 8, backgroundColor: "#fff", border: "1px solid #e2e8f0", fontSize: 11, color: "#475569", lineHeight: 1.35 }}>
            <strong style={{ color: "#0f172a", fontSize: 12 }}>Full task view</strong>
            <span>Tasks {total}</span>
            <span>Page {currentPage}/{totalPages}</span>
            <span>{hasRunningTask ? "running visible" : "no running visible"}</span>
          </div>
            <div style={{ fontSize: "11px", color: "#64748b", lineHeight: 1.5 }}>
              roster=task；窗口×topk run=配置；只读查询 macb_ 组合回测结果。
            </div>
          </div>

          <div style={{ flex: 1, overflow: "hidden", display: "flex", flexDirection: "column" }}>
            <div style={headerStyle}>
              <h2 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#1e293b", display: "flex", alignItems: "center", gap: "8px" }}>
                <Activity color="#10b981" size={20} />
                组合回测任务列表
              </h2>
              <div style={{ display: "flex", alignItems: "center", gap: 10, fontSize: "12px", color: "#64748b" }}>
                {hasRunningTask && (
                  <span style={{ display: "inline-flex", alignItems: "center", gap: 4, color: "#0369a1", backgroundColor: "#e0f2fe", borderRadius: 999, padding: "2px 8px", fontWeight: 700 }}>
                    <span style={{ display: "inline-block", width: "6px", height: "6px", borderRadius: "50%", backgroundColor: "#0369a1", animation: "pulse 2s infinite" }} />
                    有运行中配置
                  </span>
                )}
                <span>只读 + 导出；删除需单独审批写操作。</span>
              </div>
            </div>

            {error && (
              <div style={{ margin: "12px 20px 0", padding: "10px 12px", backgroundColor: "#fef2f2", border: "1px solid #ef4444", borderRadius: 8, fontSize: 12, color: "#991b1b" }}>
                {error}
              </div>
            )}

            <div style={{ maxHeight: "660px", overflowY: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "13px" }}>
              <thead>
                <tr style={{ backgroundColor: "#f8fafc", borderBottom: "1px solid #e2e8f0", position: "sticky", top: 0, zIndex: 1 }}>
                  <th style={{ padding: "10px 16px", textAlign: "left", fontWeight: 700, color: "#475569", fontSize: "12px", textTransform: "uppercase", letterSpacing: "0.05em" }}>组合任务</th>
                  <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "110px" }}>类型</th>
                  <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "120px" }}>状态</th>
                  <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "90px" }}>配置</th>
                  <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "150px" }}>阶段</th>
                  <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "170px" }}>Scheme</th>
                  <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "130px" }}>更新时间</th>
                  <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "220px" }}>操作</th>
                </tr>
              </thead>
              <tbody>
                {loading ? (
                  <tr><td colSpan={8} style={{ padding: "24px", textAlign: "center", color: "#94a3b8" }}>加载组合回测任务中...</td></tr>
                ) : tasks.length === 0 ? (
                  <tr><td colSpan={8} style={{ padding: "24px", textAlign: "center", color: "#94a3b8" }}>暂无组合回测任务</td></tr>
                ) : tasks.map((task) => {
                  const statusInfo = combineStatusInfo(task.status);
                  return (
                    <tr
                      key={task.task_id}
                      style={{ cursor: "pointer", backgroundColor: "#fff", borderBottom: "1px solid #f1f5f9", transition: "background-color 0.15s" }}
                      onMouseEnter={event => { event.currentTarget.style.backgroundColor = "#fafafa"; }}
                      onMouseLeave={event => { event.currentTarget.style.backgroundColor = "#fff"; }}
                      onClick={() => { window.location.href = `/quantevolver/multi-alpha/combine-backtest/${encodeURIComponent(task.task_id)}`; }}
                    >
                      <td style={{ padding: "8px 16px", fontWeight: 500, color: "#0f172a", maxWidth: "360px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        <div title={task.task_name}>{task.task_name}</div>
                        <div style={{ fontSize: "10px", color: "#94a3b8", fontFamily: "monospace", fontWeight: 400 }}>{task.task_id}</div>
                      </td>
                      <td style={{ padding: "10px 12px", textAlign: "center" }}>
                        <span style={{ fontSize: "10px", fontWeight: 700, padding: "2px 8px", borderRadius: "12px", backgroundColor: "#e0e7ff", color: "#4338ca" }}>
                          多Alpha
                        </span>
                      </td>
                      <td style={{ padding: "10px 12px", textAlign: "center" }}>
                        <span style={{ fontSize: "11px", fontWeight: 600, padding: "2px 8px", borderRadius: "12px", color: statusInfo.color, backgroundColor: statusInfo.bgColor, whiteSpace: "nowrap", display: "inline-flex", alignItems: "center", gap: "4px" }}>
                          {task.status === "running" && <span style={{ display: "inline-block", width: "6px", height: "6px", borderRadius: "50%", backgroundColor: "#22c55e", animation: "pulse 2s infinite" }} />}
                          {taskStatusLabel(task.status)}
                        </span>
                      </td>
                      <td style={{ padding: "10px 12px", textAlign: "center", fontFamily: "monospace", fontSize: "12px", color: "#475569" }}>
                        {task.current_loop}/{task.max_loops}
                      </td>
                      <td style={{ padding: "10px 12px", textAlign: "center", fontSize: "11px", color: task.phase ? "#0369a1" : "#64748b", whiteSpace: "nowrap" }}>
                        {task.phase || "-"}
                      </td>
                      <td style={{ padding: "10px 12px", textAlign: "center", fontSize: "11px", color: "#475569" }}>
                        <span title={schemeText(task)} style={{ display: "inline-block", maxWidth: 150, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                          {schemeText(task)}
                        </span>
                      </td>
                      <td style={{ padding: "10px 12px", textAlign: "center", fontSize: "11px", color: "#64748b", whiteSpace: "nowrap" }}>
                        {formatTime(task.updated_at)}
                      </td>
                      <td style={{ padding: "10px 12px", textAlign: "center" }}>
                        <div style={{ display: "flex", gap: "4px", justifyContent: "center" }}>
                          <Link
                            href={`/quantevolver/multi-alpha/combine-backtest/${encodeURIComponent(task.task_id)}`}
                            onClick={(event) => event.stopPropagation()}
                            title="查看组合回测详情"
                            style={{ padding: "4px 8px", border: "1px solid #3b82f6", borderRadius: "4px", backgroundColor: "#eff6ff", color: "#3b82f6", fontSize: "11px", fontWeight: 600, cursor: "pointer", display: "flex", alignItems: "center", gap: "3px", textDecoration: "none" }}
                          >
                            详情
                          </Link>
                          <button
                            onClick={(event) => { event.stopPropagation(); exportTaskJson(task); }}
                            title="导出任务摘要 JSON"
                            style={{ padding: "4px 8px", border: "1px solid #2563eb", borderRadius: "4px", backgroundColor: "#eff6ff", color: "#1d4ed8", fontSize: "11px", fontWeight: 600, cursor: "pointer", display: "flex", alignItems: "center", gap: "3px" }}
                          >
                            <DownloadCloud size={11} /> 导出
                          </button>
                          <button
                            onClick={(event) => { event.stopPropagation(); alert(DELETE_APPROVAL_MESSAGE); }}
                            title={DELETE_APPROVAL_MESSAGE}
                            style={{ padding: "4px 8px", border: "1px solid #fca5a5", borderRadius: "4px", backgroundColor: "#fff", color: "#ef4444", fontSize: "11px", fontWeight: 600, cursor: "not-allowed", display: "flex", alignItems: "center", gap: "3px", opacity: 0.65 }}
                          >
                            <Trash2 size={11} /> 删除
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

            <div style={{ padding: "12px 20px", borderTop: "1px solid #f1f5f9", backgroundColor: "#f8fafc", display: "flex", justifyContent: "space-between", alignItems: "center", fontSize: "12px", color: "#64748b" }}>
              <span>共 {total} 个组合任务；当前页 {tasks.length} 个。</span>
              <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                <button
                  onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
                  disabled={offset === 0 || loading}
                  style={{ padding: "4px 10px", fontSize: 11, cursor: offset === 0 ? "not-allowed" : "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: offset === 0 ? "#f3f4f6" : "#fff", color: "#475569" }}
                >
                  上一页
                </button>
                <span>{currentPage} / {totalPages}</span>
                <button
                  onClick={() => setOffset(offset + PAGE_SIZE)}
                  disabled={offset + PAGE_SIZE >= total || loading}
                  style={{ padding: "4px 10px", fontSize: 11, cursor: offset + PAGE_SIZE >= total ? "not-allowed" : "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: offset + PAGE_SIZE >= total ? "#f3f4f6" : "#fff", color: "#475569" }}
                >
                  下一页
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
