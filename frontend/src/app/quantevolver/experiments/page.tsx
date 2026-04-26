"use client";

import { useEffect, useState, useCallback, useRef, useMemo } from "react";
import dynamic from "next/dynamic";
import { useExperimentSSE } from "../components/useExperimentSSE";
import LogTerminal from "../components/LogTerminal";
import MetricsSummary from "../components/MetricsSummary";

const IcSeriesChart = dynamic(() => import("../components/charts/IcSeriesChart"), { ssr: false });
const LossCurveChart = dynamic(() => import("../components/charts/LossCurveChart"), { ssr: false });
const ReturnCurveChart = dynamic(() => import("../components/charts/ReturnCurveChart"), { ssr: false });

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type Experiment = {
  experiment_id: string;
  experiment_name: string;
  status: string;
  factor_names?: string[];
  model_id?: string;
  strategy_id?: string;
  workspace_path?: string;
  wsl_command?: string;
  result_metrics?: any;
  custom_params?: any;
  qe_task_id?: string;
  qe_loop_id?: string;
  loop_index?: number;
  parent_experiment_id?: string | null;
  is_evolution_loop?: boolean;
  ic?: number | null;
  icir?: number | null;
  rank_ic?: number | null;
  rank_icir?: number | null;
  annualized_return?: number | null;
  max_drawdown?: number | null;
  information_ratio?: number | null;
  annualized_return_no_cost?: number | null;
  max_drawdown_no_cost?: number | null;
  information_ratio_no_cost?: number | null;
  alpha_mode?: string;
  created_at?: string;
  updated_at?: string;
};

function parseCustomParams(exp: Experiment): Record<string, any> {
  const raw = exp.custom_params;
  if (!raw) return {};
  if (typeof raw === "string") {
    try {
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch {
      return {};
    }
  }
  return typeof raw === "object" ? raw : {};
}

function getInheritedExecutionNodeId(exp: Experiment): string | undefined {
  const params = parseCustomParams(exp);
  const nodeId = params.execution_node_id;
  return typeof nodeId === "string" && nodeId.trim() ? nodeId.trim() : undefined;
}

const KEY_METRICS = [
  { key: "IC", label: "IC", fmt: (v: number) => v.toFixed(4), good: (v: number) => v > 0.03 },
  { key: "ICIR", label: "ICIR", fmt: (v: number) => v.toFixed(4), good: (v: number) => v > 0.3 },
  { key: "Rank IC", label: "Rank IC", fmt: (v: number) => v.toFixed(4), good: (v: number) => v > 0.05 },
  { key: "Rank ICIR", label: "Rank ICIR", fmt: (v: number) => v.toFixed(4), good: (v: number) => v > 0.3 },
  { key: "annualized_return", label: "年化收益", fmt: (v: number) => (v * 100).toFixed(2) + "%", good: (v: number) => v > 0.1 },
  { key: "max_drawdown", label: "最大回撤", fmt: (v: number) => (v * 100).toFixed(2) + "%", good: (v: number) => v > -0.2 },
  { key: "sharpe", label: "Sharpe", fmt: (v: number) => v.toFixed(3), good: (v: number) => v > 1.0 },
  { key: "information_ratio", label: "IR", fmt: (v: number) => v.toFixed(3), good: (v: number) => v > 0.5 },
];

const MODEL_NAMES: Record<string, string> = {
  LGBModel: "LightGBM",
  linear: "线性模型",
  XGBModel: "XGBoost",
  CatBoostModel: "CatBoost",
  DNNModel: "深度神经网络",
  TabNetModel: "TabNet",
};

const STATUS_MAP: Record<string, { label: string; color: string; border: string }> = {
  created:     { label: "已创建", color: "#3b82f6", border: "4px solid #3b82f6" },
  running:     { label: "运行中", color: "#f59e0b", border: "4px solid #f59e0b" },
  completed:   { label: "已完成", color: "#10b981", border: "4px solid #10b981" },
  failed:      { label: "失败",   color: "#ef4444", border: "4px solid #ef4444" },
  interrupted: { label: "已中断", color: "#8b5cf6", border: "4px solid #8b5cf6" },
  timeout:     { label: "超时",   color: "#f97316", border: "4px solid #f97316" },
};

function getMetrics(exp: Experiment): Record<string, any> {
  // 优先使用数据库独立列，其次从 result_metrics JSON 解析
  const m: Record<string, any> = {};

  // 从独立列读取
  if (exp.ic != null) m["IC"] = exp.ic;
  if (exp.icir != null) m["ICIR"] = exp.icir;
  if (exp.rank_ic != null) m["Rank IC"] = exp.rank_ic;
  if (exp.rank_icir != null) m["Rank ICIR"] = exp.rank_icir;
  if (exp.annualized_return != null) m["annualized_return"] = exp.annualized_return;
  if (exp.max_drawdown != null) m["max_drawdown"] = exp.max_drawdown;
  if (exp.information_ratio != null) m["information_ratio"] = exp.information_ratio;
  if (exp.annualized_return_no_cost != null) m["annualized_return_no_cost"] = exp.annualized_return_no_cost;
  if (exp.max_drawdown_no_cost != null) m["max_drawdown_no_cost"] = exp.max_drawdown_no_cost;
  if (exp.information_ratio_no_cost != null) m["information_ratio_no_cost"] = exp.information_ratio_no_cost;

  // 补充 result_metrics JSON 中的其他字段（详细指标展示用）
  if (exp.result_metrics) {
    let raw: Record<string, any>;
    if (typeof exp.result_metrics === "string") {
      try { raw = JSON.parse(exp.result_metrics); } catch { raw = {}; }
    } else {
      raw = exp.result_metrics;
    }
    for (const [k, v] of Object.entries(raw)) {
      if (m[k] == null && v != null) m[k] = v;
    }
  }

  // sharpe 别名（QLib IR ≈ Sharpe）
  if (m["sharpe"] == null && m["information_ratio"] != null) m["sharpe"] = m["information_ratio"];

  return m;
}

export default function ExperimentsPage() {
  const [experiments, setExperiments] = useState<Experiment[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [loopsExpandedIds, setLoopsExpandedIds] = useState<Set<string>>(new Set());
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [refreshInterval, setRefreshInterval] = useState(5); // 秒
  const [actionId, setActionId] = useState<string | null>(null);
  const [actionType, setActionType] = useState<string>("");
  const [toast, setToast] = useState<{ msg: string; ok: boolean } | null>(null);

  // 分页状态
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  /* ── 单次实验执行（共享 Hook） ── */
  const [logsExpId, setLogsExpId] = useState<string | null>(null);
  const sse = useExperimentSSE({
    pollAfterDisconnect: true,
    onDisconnect: () => loadExperiments(),
  });

  const showToast = useCallback((msg: string, ok: boolean) => {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 3500);
  }, []);

  async function loadExperiments(page?: number) {
    setLoading(true);
    setError(null);
    const targetPage = page ?? currentPage;
    const offset = (targetPage - 1) * pageSize;
    try {
      const res = await fetch(`${API}/quantevolver/experiments?limit=${pageSize}&offset=${offset}`);
      const data = await res.json();
      const items = data.items || [];
      setExperiments(items);
      setTotal(data.total || 0);
      if (page !== undefined) setCurrentPage(page);
      const runningIds = items
        .filter((exp: Experiment) => exp.status === "running")
        .map((exp: Experiment) => exp.experiment_id)
        .slice(0, 10);
      if (runningIds.length > 0) {
        void Promise.allSettled(
          runningIds.map((id: string) =>
            fetch(`${API}/quantevolver/experiments/${id}/run-status`)
              .then((r) => r.ok ? r.json() : null)
          )
        ).then((results) => {
          const changed = results.some((r) => (
            r.status === "fulfilled"
            && r.value
            && r.value.status
            && r.value.status !== "running"
          ));
          if (changed) loadExperiments(targetPage);
        });
      }
    } catch (e: any) {
      setError(e?.message || "加载失败");
    }
    setLoading(false);
  }

  useEffect(() => { loadExperiments(1); }, []);

  // 页面加载后自动连接第一个 running 实验的日志流
  useEffect(() => {
    if (!logsExpId && experiments.length > 0) {
      const runningExp = experiments.find(e => e.status === "running");
      if (runningExp) {
        setLogsExpId(runningExp.experiment_id);
        setExpandedId(runningExp.experiment_id);
        sse.openLogs(runningExp.experiment_id);
      }
    }
  }, [experiments]);

  // 自动刷新：仅在用户开启时按设定间隔刷新
  useEffect(() => {
    if (!autoRefresh) return;
    const timer = setInterval(() => loadExperiments(currentPage), refreshInterval * 1000);
    return () => clearInterval(timer);
  }, [autoRefresh, refreshInterval]);

  async function syncResult(expId: string) {
    setActionId(expId);
    setActionType("sync");
    try {
      const res = await fetch(`${API}/quantevolver/experiments/${expId}/sync-results`, { method: "POST" });
      const data = await res.json();
      if (data.ok) {
        showToast("同步成功！回测指标已更新", true);
        loadExperiments();
      } else {
        showToast("同步失败: " + (data.error || "未知错误"), false);
      }
    } catch (e: any) {
      showToast("同步失败: " + (e?.message || ""), false);
    }
    setActionId(null);
    setActionType("");
  }

  async function regenerateExperiment(expId: string) {
    setActionId(expId);
    setActionType("regen");
    try {
      const res = await fetch(`${API}/quantevolver/experiments/${expId}/regenerate`, { method: "POST" });
      const data = await res.json();
      if (data.ok) {
        showToast(`脚本已重新生成 (${data.factor_count} 个因子)`, true);
        loadExperiments();
      } else {
        showToast("重新生成失败: " + (data.error || "未知错误"), false);
      }
    } catch (e: any) {
      showToast("重新生成失败: " + (e?.message || ""), false);
    }
    setActionId(null);
    setActionType("");
  }

  async function runExperiment(exp: Experiment) {
    const expId = exp.experiment_id;
    const inheritedNodeId = getInheritedExecutionNodeId(exp);
    setActionId(expId);
    setActionType("run");
    try {
      setLogsExpId(expId);
      setExpandedId(expId);
      await sse.startRun(expId, inheritedNodeId);
      showToast(inheritedNodeId ? `Submitted to ${inheritedNodeId}` : "Submitted run", true);
      loadExperiments();
    } catch (e: any) {
      showToast("Run failed: " + (e?.message || ""), false);
    }
    setActionId(null);
    setActionType("");
  }

  function openLogs(expId: string) {
    setLogsExpId(expId);
    setExpandedId(expId);
    sse.openLogs(expId);
  }

  function closeLogs() {
    sse.closeLogs();
    setLogsExpId(null);
  }

  async function handleDelete(experimentId: string) {
    const isRunning = experiments.find(e => e.experiment_id === experimentId)?.status === "running";
    const msg = isRunning
      ? `实验 ${experimentId} 当前状态为"运行中"，强制删除可能导致 RDAgent 侧残留。\n\n确认删除？`
      : `确认删除实验 ${experimentId}？\n\n此操作将删除实验的workspace文件和数据库记录，不可撤销。`;
    if (!confirm(msg)) return;
    try {
      const res = await fetch(`${API}/quantevolver/experiments/${experimentId}?cleanup_workspace=true`, { method: "DELETE" });
      const data = await res.json();
      if (data.ok) {
        loadExperiments();
        showToast("实验已删除", true);
      } else {
        alert(`删除失败: ${data.detail || "未知错误"}`);
      }
    } catch (e: any) {
      alert(`删除失败: ${e?.message || "网络错误"}`);
    }
  }

  async function checkRunStatus(experimentId: string) {
    setActionId(experimentId);
    setActionType("check");
    try {
      const res = await fetch(`${API}/quantevolver/experiments/${experimentId}/run-status`);
      const data = await res.json();

      if (data.status !== "running") {
        // 后端已自动同步 DB（completed/failed/interrupted）
        const label = STATUS_MAP[data.status]?.label || data.status;
        showToast(`状态已更新: ${label}`, true);
        loadExperiments();
      } else {
        showToast("实验仍在执行中", true);
      }
    } catch (e: any) {
      showToast("查询状态失败: " + (e?.message || ""), false);
    }
    setActionId(null);
    setActionType("");
  }

  // Group experiments: main experiments + their child loops
  const groupedExperiments = useMemo(() => {
    const mainExps = experiments.filter(e => !e.parent_experiment_id);
    const childMap = new Map<string, Experiment[]>();
    experiments.filter(e => e.parent_experiment_id).forEach(e => {
      const children = childMap.get(e.parent_experiment_id!) || [];
      children.push(e);
      childMap.set(e.parent_experiment_id!, children);
    });
    // Sort children by loop_index
    childMap.forEach(children => children.sort((a, b) => (a.loop_index || 0) - (b.loop_index || 0)));
    return mainExps.map(exp => {
      const children = childMap.get(exp.experiment_id) || [];
      // 找最后一个 SOTA loop（从后往前找第一个有正 IC 且有年化收益的 completed loop）
      const completedChildren = children.filter(c => c.status === "completed");
      // 优先取最后一个 completed child 作为 SOTA 代表
      const sotaLoop = completedChildren.length > 0 ? completedChildren[completedChildren.length - 1] : null;
      return {
        ...exp,
        childLoops: children,
        sotaLoop,
      };
    });
  }, [experiments]);

  return (
    <main style={{ padding: 24 }}>
      {/* Toast */}
      {toast && (
        <div style={{
          position: "fixed", top: 16, right: 16, zIndex: 50, padding: "12px 16px",
          borderRadius: 8, boxShadow: "0 4px 6px rgba(0,0,0,0.1)", fontSize: 14, fontWeight: 500,
          background: toast.ok ? "#16a34a" : "#dc2626", color: "#fff", transition: "all 0.3s"
        }}>
          {toast.msg}
        </div>
      )}

      {/* Banner */}
      <section
        style={{
          background: "linear-gradient(135deg, #ef4444 0%, #f59e0b 100%)",
          borderRadius: 16, padding: 20, color: "#fff", marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>实验历史</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          查看QLib回测实验记录，一键执行回测，实时查看日志，同步WSL执行结果
        </p>
      </section>

      {/* 工具栏 */}
      <section style={{ background: "#fff", borderRadius: 12, padding: 16, marginBottom: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
        <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
          <button onClick={() => loadExperiments(currentPage)} disabled={loading} style={{ padding: "6px 12px", fontSize: 12, cursor: "pointer", borderRadius: 6, border: "1px solid #d1d5db", background: "#fff" }}>
            {loading ? "加载中..." : "手动刷新"}
          </button>
          <span style={{ fontSize: 12, color: "#9ca3af" }}>共 {total} 条 | 第 {currentPage}/{totalPages} 页</span>

          <select
            value={pageSize}
            onChange={e => { setPageSize(Number(e.target.value)); loadExperiments(1); }}
            style={{ padding: "3px 6px", fontSize: 11, borderRadius: 4, border: "1px solid #d1d5db", background: "#f9fafb", color: "#374151" }}
          >
            {[10, 20, 50, 100, 200].map(n => <option key={n} value={n}>{n} 条/页</option>)}
          </select>

          <span style={{ width: 1, height: 16, background: "#e5e7eb" }} />

          <label style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 12, color: "#475569", cursor: "pointer" }}>
            <input type="checkbox" checked={autoRefresh} onChange={e => setAutoRefresh(e.target.checked)} style={{ width: 14, height: 14, accentColor: "#2563eb" }} />
            自动刷新
          </label>
          {autoRefresh && (
            <select
              value={refreshInterval}
              onChange={e => setRefreshInterval(Number(e.target.value))}
              style={{ padding: "3px 6px", fontSize: 11, borderRadius: 4, border: "1px solid #d1d5db", background: "#f9fafb", color: "#374151" }}
            >
              <option value={2}>2秒</option>
              <option value={5}>5秒</option>
              <option value={10}>10秒</option>
              <option value={30}>30秒</option>
            </select>
          )}
          {autoRefresh && (
            <span style={{ fontSize: 11, color: "#2563eb", fontWeight: 600 }}>
              (每{refreshInterval}秒自动刷新)
            </span>
          )}
        </div>
        {error && <div style={{ marginTop: 8, padding: 8, background: "#fee2e2", borderRadius: 6, fontSize: 12, color: "#991b1b" }}>{error}</div>}
      </section>

      {/* 实验卡片列表（按 parent_experiment_id 分组） */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(360px, 1fr))", gap: 16 }}>
        {groupedExperiments.map(exp => {
          const expanded = expandedId === exp.experiment_id;
          const sm = STATUS_MAP[exp.status] || { label: exp.status, color: "#6b7280", border: "4px solid #e5e7eb" };
          const metrics = getMetrics(exp);
          const hasMetrics = Object.keys(metrics).length > 0;
          const isActioning = actionId === exp.experiment_id;
          const isShowingLogs = logsExpId === exp.experiment_id;
          const canRun = exp.status !== "completed" && exp.status !== "running";

          let ic: number | undefined;
          if (metrics["IC"] != null) ic = typeof metrics["IC"] === "number" ? metrics["IC"] : parseFloat(metrics["IC"]);
          let annRet: number | undefined;
          if (metrics["annualized_return"] != null) annRet = typeof metrics["annualized_return"] === "number" ? metrics["annualized_return"] : parseFloat(metrics["annualized_return"]);
          let maxDD: number | undefined;
          if (metrics["max_drawdown"] != null) maxDD = typeof metrics["max_drawdown"] === "number" ? metrics["max_drawdown"] : parseFloat(metrics["max_drawdown"]);
          let ir: number | undefined;
          if (metrics["information_ratio"] != null) ir = typeof metrics["information_ratio"] === "number" ? metrics["information_ratio"] : parseFloat(metrics["information_ratio"]);

          const timeStr = exp.created_at ? new Date(exp.created_at).toLocaleString("zh-CN", {
            month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit"
          }) : "-";

          return (
            <div key={exp.experiment_id}>
              <div
                style={{
                  background: "#fff", borderRadius: 12, padding: 16,
                  boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
                  borderLeft: sm.border,
                  cursor: "pointer",
                }}
                onClick={() => setExpandedId(expandedId === exp.experiment_id ? null : exp.experiment_id)}
              >
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                  <div>
                    <div style={{ fontWeight: 700, fontSize: 14 }}>
                      {exp.experiment_id}
                    </div>
                    <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 2 }}>
                      时间: {timeStr}
                    </div>
                  </div>
                  <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                    {exp.status === "running" && (
                      <div style={{
                        width: 6, height: 6, borderRadius: "50%", backgroundColor: "#f59e0b",
                        animation: "pulse 1.5s infinite",
                      }} />
                    )}
                    <span style={{ fontSize: 11, background: `${sm.color}15`, color: sm.color, padding: "2px 8px", borderRadius: 12, fontWeight: 600 }}>
                      {sm.label}
                    </span>
                    {exp.alpha_mode === "multi" && (
                      <span style={{ fontSize: 10, background: "#8b5cf615", color: "#8b5cf6", padding: "2px 6px", borderRadius: 12, fontWeight: 600 }}>
                        多Alpha
                      </span>
                    )}
                  </div>
                </div>

                {/* 描述信息 / 标签 */}
                <div style={{ marginTop: 8, fontSize: 11, color: "#6b7280", lineHeight: 1.5, background: "#f9fafb", padding: "6px 10px", borderRadius: 6 }}>
                  <div><strong>因子数量:</strong> {exp.factor_names?.length || 0}</div>
                  <div><strong>模型:</strong> {MODEL_NAMES[exp.model_id || ""] || exp.model_id || "默认"}</div>
                  <div><strong>策略:</strong> {exp.strategy_id || "默认"}</div>
                </div>

                {/* 指标 */}
                <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 8, marginTop: 12 }}>
                  <MetricBox label="IC" value={ic} fmt={v => v.toFixed(4)} color="#2563eb" />
                  <MetricBox label="年化收益" value={annRet} fmt={v => (v * 100).toFixed(2) + "%"} color="#10b981" />
                  <MetricBox label="最大回撤" value={maxDD} fmt={v => (v * 100).toFixed(2) + "%"} color="#ef4444" />
                  <MetricBox label="IR" value={ir} fmt={v => v.toFixed(3)} color="#8b5cf6" />
                </div>

                {/* 展开详情 */}
                {expanded && (
                  <div style={{ marginTop: 12, paddingTop: 12, borderTop: "1px solid #f3f4f6", fontSize: 12 }} onClick={e => e.stopPropagation()}>
                    {/* 因子列表 */}
                    {exp.factor_names && exp.factor_names.length > 0 && (
                      <div style={{ marginBottom: 12 }}>
                        <strong style={{ color: "#374151" }}>因子列表：</strong>
                        <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginTop: 4, maxHeight: 100, overflow: "auto", padding: 4 }}>
                          {exp.factor_names.map(fn => (
                            <span key={fn} style={{ padding: "2px 6px", borderRadius: 4, fontSize: 10, fontFamily: "monospace", background: "#f3f4f6", border: "1px solid #e5e7eb", color: "#4b5563" }}>
                              {fn}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* 完整回测指标表格 */}
                    {hasMetrics && (
                      <div style={{ marginBottom: 12 }}>
                        <strong style={{ color: "#374151" }}>详细指标：</strong>
                        <div style={{ background: "#f9fafb", borderRadius: 8, border: "1px solid #e5e7eb", marginTop: 4, overflow: "hidden" }}>
                          <table style={{ width: "100%", fontSize: 11, borderCollapse: "collapse" }}>
                            <tbody>
                              {Object.entries(metrics).map(([k, v]) => {
                                const km = KEY_METRICS.find(m => m.key === k);
                                const numVal = typeof v === "number" ? v : parseFloat(String(v));
                                const isNum = !isNaN(numVal);
                                return (
                                  <tr key={k} style={{ borderBottom: "1px solid #f3f4f6" }}>
                                    <td style={{ padding: "4px 8px", color: "#6b7280" }}>{km?.label || k}</td>
                                    <td style={{ padding: "4px 8px", fontWeight: 600, fontFamily: "monospace", color: "#374151" }}>
                                      {isNum && km ? km.fmt(numVal) : String(v)}
                                    </td>
                                  </tr>
                                );
                              })}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}

                    {exp.workspace_path && (
                      <div style={{ marginBottom: 12 }}>
                        <strong style={{ color: "#374151" }}>工作目录：</strong>
                        <div style={{ color: "#6b7280", marginTop: 2, fontSize: 10, fontFamily: "monospace", wordBreak: "break-all" }}>{exp.workspace_path}</div>
                      </div>
                    )}

                    {exp.wsl_command && (
                      <div style={{ marginBottom: 12 }}>
                        <strong style={{ color: "#374151" }}>WSL命令：</strong>
                        <div style={{ position: "relative", marginTop: 4 }}>
                          <pre style={{ background: "#1e293b", color: "#e2e8f0", padding: 8, borderRadius: 6, fontSize: 10, overflow: "auto", maxHeight: 100, whiteSpace: "pre-wrap", fontFamily: "monospace", margin: 0 }}>
                            {exp.wsl_command}
                          </pre>
                          <button
                            onClick={() => {
                              navigator.clipboard.writeText(exp.wsl_command || "");
                              showToast("已复制", true);
                            }}
                            style={{ position: "absolute", top: 4, right: 4, padding: "2px 6px", fontSize: 9, border: "1px solid #475569", borderRadius: 4, background: "#334155", color: "#cbd5e1", cursor: "pointer" }}
                          >
                            复制
                          </button>
                        </div>
                      </div>
                    )}

                    {/* SSE 日志展开区域 */}
                    {isShowingLogs && (
                      <div style={{ marginBottom: 12 }}>
                        <LogTerminal
                          logs={sse.runLogs}
                          logsEndRef={sse.logsEndRef}
                          maxHeight={200}
                          fontSize={10}
                          onClose={closeLogs}
                        />

                        {/* 完成后展示核心指标 */}
                        {sse.runStatus === "completed" && sse.runMetrics && (
                          <div style={{ marginTop: 8 }}>
                            <MetricsSummary metrics={sse.runMetrics} />
                          </div>
                        )}

                        {/* 完成后展示图表 */}
                        {sse.runStatus === "completed" && sse.enhancedMetrics && (
                          <div style={{ marginTop: 12, display: "grid", gap: 12 }}>
                            {sse.enhancedMetrics.ic_series && (
                              <div style={{ background: "#f9fafb", borderRadius: 8, padding: 12, border: "1px solid #e5e7eb" }}>
                                <div style={{ fontSize: 12, fontWeight: 600, color: "#374151", marginBottom: 6 }}>IC 时序诊断</div>
                                <IcSeriesChart
                                  dates={sse.enhancedMetrics.dates || []}
                                  ic_series={sse.enhancedMetrics.ic_series}
                                  rank_ic_series={sse.enhancedMetrics.rank_ic_series}
                                  ic_rolling_30d_mean={sse.enhancedMetrics.ic_rolling_30d_mean}
                                  ic_rolling_30d_std={sse.enhancedMetrics.ic_rolling_30d_std}
                                  ic_positive_ratio={sse.enhancedMetrics.ic_positive_ratio}
                                />
                              </div>
                            )}
                            {sse.enhancedMetrics.cumulative_excess_with_cost && (
                              <div style={{ background: "#f9fafb", borderRadius: 8, padding: 12, border: "1px solid #e5e7eb" }}>
                                <div style={{ fontSize: 12, fontWeight: 600, color: "#374151", marginBottom: 6 }}>收益曲线</div>
                                <ReturnCurveChart
                                  dates={sse.enhancedMetrics.dates || []}
                                  cumulative_excess_no_cost={sse.enhancedMetrics.cumulative_excess_no_cost}
                                  cumulative_excess_with_cost={sse.enhancedMetrics.cumulative_excess_with_cost}
                                  cumulative_benchmark={sse.enhancedMetrics.cumulative_benchmark}
                                  drawdown_series={sse.enhancedMetrics.drawdown_series}
                                />
                              </div>
                            )}
                            {sse.enhancedMetrics.train_loss_curve && (
                              <div style={{ background: "#f9fafb", borderRadius: 8, padding: 12, border: "1px solid #e5e7eb" }}>
                                <div style={{ fontSize: 12, fontWeight: 600, color: "#374151", marginBottom: 6 }}>训练过程</div>
                                <LossCurveChart
                                  train_loss_curve={sse.enhancedMetrics.train_loss_curve}
                                  val_loss_curve={sse.enhancedMetrics.val_loss_curve}
                                  best_epoch={sse.enhancedMetrics.best_epoch}
                                  overfit_ratio={sse.enhancedMetrics.overfit_ratio}
                                  convergence_ratio={sse.enhancedMetrics.convergence_ratio}
                                />
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    )}

                    {/* 操作按钮 */}
                    <div style={{ display: "flex", gap: 8, marginTop: 12, justifyContent: "flex-end", flexWrap: "wrap", alignItems: "center" }}>
                      {canRun && (
                        <>
                          <div style={{ display: "flex", gap: 4 }}>
                            <span style={{ padding: "3px 8px", fontSize: 10, borderRadius: 4, border: "1.5px solid #0ea5e9", background: "#f0f9ff", color: "#0284c7", fontWeight: 600 }}>统一执行层</span>
                          </div>
                          {getInheritedExecutionNodeId(exp) && (
                            <span style={{ padding: "3px 8px", fontSize: 10, borderRadius: 4, border: "1px solid #0284c7", background: "#e0f2fe", color: "#0369a1", fontWeight: 600 }}>
                              Node: {getInheritedExecutionNodeId(exp)}
                            </span>
                          )}
                          <button onClick={() => runExperiment(exp)}
                            disabled={isActioning}
                            style={{
                              padding: "4px 10px", fontSize: 11, cursor: "pointer", borderRadius: 4,
                              border: "1px solid #059669", background: "#ecfdf5", color: "#059669", fontWeight: 600,
                            }}>
                            {isActioning && actionType === "run" ? "提交中..." : "执行回测"}
                          </button>
                        </>
                      )}
                      {exp.status === "running" && !isShowingLogs && (
                        <button onClick={() => openLogs(exp.experiment_id)}
                          style={{
                            padding: "4px 10px", fontSize: 11, cursor: "pointer", borderRadius: 4,
                            border: "1px solid #f59e0b", background: "#fffbeb", color: "#d97706", fontWeight: 600,
                          }}>
                          查看日志
                        </button>
                      )}
                      {exp.status === "running" && (
                        <button onClick={() => checkRunStatus(exp.experiment_id)}
                          disabled={isActioning}
                          style={{
                            padding: "4px 10px", fontSize: 11, cursor: "pointer", borderRadius: 4,
                            border: "1px solid #8b5cf6", background: "#f5f3ff", color: "#7c3aed", fontWeight: 600,
                          }}>
                          {isActioning && actionType === "check" ? "查询中..." : "刷新状态"}
                        </button>
                      )}
                      <button onClick={() => window.open(`/quantevolver/experiments/${exp.experiment_id}`, '_blank')}
                        style={{ padding: "4px 10px", fontSize: 11, cursor: "pointer", borderRadius: 4, border: "1px solid #3b82f6", background: "#eff6ff", color: "#3b82f6", fontWeight: 600 }}>
                        查看详情
                      </button>
                      {exp.alpha_mode === "multi" && (
                        <button onClick={() => window.open(`/quantevolver/multi-alpha/diagnostics/${exp.experiment_id}`, '_blank')}
                          style={{ padding: "4px 10px", fontSize: 11, cursor: "pointer", borderRadius: 4, border: "1px solid #8b5cf6", background: "#f5f3ff", color: "#7c3aed", fontWeight: 600 }}>
                          查看诊断
                        </button>
                      )}
                      <button onClick={() => syncResult(exp.experiment_id)}
                        disabled={isActioning}
                        style={{ padding: "4px 10px", fontSize: 11, cursor: "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: "#fff" }}>
                        {isActioning && actionType === "sync" ? "同步中..." : "同步结果"}
                      </button>
                      <button onClick={() => regenerateExperiment(exp.experiment_id)}
                        disabled={isActioning}
                        style={{ padding: "4px 10px", fontSize: 11, cursor: "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: "#fff" }}>
                        {isActioning && actionType === "regen" ? "生成中..." : "重新生成"}
                      </button>
                      <button onClick={() => handleDelete(exp.experiment_id)}
                        style={{
                          padding: "4px 10px", fontSize: 11, cursor: "pointer", borderRadius: 4,
                          border: "1px solid #ef4444", background: "#fff",
                          color: "#ef4444", fontWeight: 600,
                        }}>
                        删除
                      </button>
                    </div>
                  </div>
                )}
              </div>

              {/* 演进 Loop 区域 */}
              {exp.childLoops.length > 0 && (() => {
                const loopsOpen = loopsExpandedIds.has(exp.experiment_id);
                const sotaM = exp.sotaLoop ? getMetrics(exp.sotaLoop) : {};
                const sotaIc = sotaM["IC"] != null ? (typeof sotaM["IC"] === "number" ? sotaM["IC"] : parseFloat(sotaM["IC"])) : undefined;
                const sotaAnnRet = sotaM["annualized_return"] != null ? (typeof sotaM["annualized_return"] === "number" ? sotaM["annualized_return"] : parseFloat(sotaM["annualized_return"])) : undefined;
                const sotaMaxDD = sotaM["max_drawdown"] != null ? (typeof sotaM["max_drawdown"] === "number" ? sotaM["max_drawdown"] : parseFloat(sotaM["max_drawdown"])) : undefined;
                return (
                  <div style={{ marginLeft: 20, marginTop: 4 }}>
                    {/* 折叠摘要条 */}
                    <div
                      onClick={() => setLoopsExpandedIds(prev => {
                        const next = new Set(prev);
                        if (next.has(exp.experiment_id)) next.delete(exp.experiment_id); else next.add(exp.experiment_id);
                        return next;
                      })}
                      style={{
                        background: "#f0f9ff", borderRadius: 8, padding: "8px 12px", cursor: "pointer",
                        border: "1px solid #bae6fd", display: "flex", justifyContent: "space-between", alignItems: "center",
                      }}
                    >
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <span style={{ fontSize: 11, fontWeight: 700, color: "#0369a1" }}>
                          {loopsOpen ? "▼" : "▶"} 演进 Loop ({exp.childLoops.length} 轮)
                        </span>
                        {exp.sotaLoop && (
                          <span style={{ fontSize: 10, background: "#fef3c7", color: "#b45309", padding: "1px 6px", borderRadius: 10, fontWeight: 600 }}>
                            SOTA: Loop {exp.sotaLoop.loop_index}
                          </span>
                        )}
                      </div>
                      {exp.sotaLoop && (
                        <div style={{ display: "flex", gap: 8 }}>
                          {sotaIc != null && <span style={{ fontSize: 10, color: "#2563eb", fontFamily: "monospace" }}>IC: {sotaIc.toFixed(4)}</span>}
                          {sotaAnnRet != null && <span style={{ fontSize: 10, color: "#10b981", fontFamily: "monospace" }}>年化: {(sotaAnnRet * 100).toFixed(1)}%</span>}
                          {sotaMaxDD != null && <span style={{ fontSize: 10, color: "#ef4444", fontFamily: "monospace" }}>回撤: {(sotaMaxDD * 100).toFixed(1)}%</span>}
                        </div>
                      )}
                    </div>

                    {/* 展开的 Loop 详情列表 */}
                    {loopsOpen && (
                      <div style={{ borderLeft: "2px solid #e5e7eb", paddingLeft: 12, marginTop: 4 }}>
                        {exp.childLoops.map(child => {
                          const childSm = STATUS_MAP[child.status] || { label: child.status, color: "#6b7280", border: "4px solid #e5e7eb" };
                          const childMetrics = getMetrics(child);
                          const childExpanded = expandedId === child.experiment_id;
                          let childIc: number | undefined;
                          if (childMetrics["IC"] != null) childIc = typeof childMetrics["IC"] === "number" ? childMetrics["IC"] : parseFloat(childMetrics["IC"]);
                          let childAnnRet: number | undefined;
                          if (childMetrics["annualized_return"] != null) childAnnRet = typeof childMetrics["annualized_return"] === "number" ? childMetrics["annualized_return"] : parseFloat(childMetrics["annualized_return"]);

                          return (
                            <div
                              key={child.experiment_id}
                              style={{
                                background: "#fafbfc", borderRadius: 8, padding: 10, marginBottom: 4,
                                boxShadow: "0 1px 2px rgba(0,0,0,0.04)",
                                borderLeft: childSm.border,
                                cursor: "pointer",
                              }}
                              onClick={() => setExpandedId(expandedId === child.experiment_id ? null : child.experiment_id)}
                            >
                              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                                <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                                  <span style={{ fontSize: 11, color: "#6b7280", fontWeight: 600 }}>
                                    Loop {child.loop_index || "?"}
                                  </span>
                                  <span style={{ fontSize: 10, color: "#9ca3af" }}>{child.experiment_id}</span>
                                </div>
                                <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                                  {childIc != null && (
                                    <span style={{ fontSize: 10, color: "#2563eb", fontFamily: "monospace" }}>IC: {childIc.toFixed(4)}</span>
                                  )}
                                  {childAnnRet != null && (
                                    <span style={{ fontSize: 10, color: "#10b981", fontFamily: "monospace" }}>收益: {(childAnnRet * 100).toFixed(2)}%</span>
                                  )}
                                  <span style={{ fontSize: 10, background: `${childSm.color}15`, color: childSm.color, padding: "1px 6px", borderRadius: 10, fontWeight: 600 }}>
                                    {childSm.label}
                                  </span>
                                </div>
                              </div>

                              {/* 子Loop展开详情 */}
                              {childExpanded && (
                                <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px solid #f3f4f6", fontSize: 11 }} onClick={e => e.stopPropagation()}>
                                  {Object.keys(childMetrics).length > 0 && (
                                    <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 6, marginBottom: 8 }}>
                                      <MetricBox label="IC" value={childIc} fmt={v => v.toFixed(4)} color="#2563eb" />
                                      <MetricBox label="年化收益" value={childAnnRet} fmt={v => (v * 100).toFixed(2) + "%"} color="#10b981" />
                                      <MetricBox label="最大回撤" value={childMetrics["max_drawdown"] != null ? parseFloat(String(childMetrics["max_drawdown"])) : undefined} fmt={v => (v * 100).toFixed(2) + "%"} color="#ef4444" />
                                      <MetricBox label="IR" value={childMetrics["information_ratio"] != null ? parseFloat(String(childMetrics["information_ratio"])) : undefined} fmt={v => v.toFixed(3)} color="#8b5cf6" />
                                    </div>
                                  )}
                                  <div style={{ display: "flex", gap: 6, justifyContent: "flex-end" }}>
                                    <button onClick={() => window.open(`/quantevolver/experiments/${child.experiment_id}`, '_blank')}
                                      style={{ padding: "3px 8px", fontSize: 10, cursor: "pointer", borderRadius: 4, border: "1px solid #3b82f6", background: "#eff6ff", color: "#3b82f6", fontWeight: 600 }}>
                                      详情
                                    </button>
                                    <button onClick={() => syncResult(child.experiment_id)}
                                      disabled={actionId === child.experiment_id}
                                      style={{ padding: "3px 8px", fontSize: 10, cursor: "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: "#fff" }}>
                                      同步结果
                                    </button>
                                    <button onClick={() => handleDelete(child.experiment_id)}
                                      style={{
                                        padding: "3px 8px", fontSize: 10, cursor: "pointer", borderRadius: 4,
                                        border: "1px solid #ef4444", background: "#fff",
                                        color: "#ef4444", fontWeight: 600,
                                      }}>
                                      删除
                                    </button>
                                  </div>
                                </div>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                );
              })()}
            </div>
          );
        })}
      </div>

      {/* 分页控件 */}
      {total > 0 && (
        <div style={{ display: "flex", justifyContent: "center", alignItems: "center", marginTop: 16, gap: 6 }}>
          <button onClick={() => loadExperiments(1)} disabled={currentPage <= 1 || loading}
            style={{ padding: "4px 8px", fontSize: 11, cursor: currentPage <= 1 ? "not-allowed" : "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: currentPage <= 1 ? "#f3f4f6" : "#fff" }}>
            首页
          </button>
          <button onClick={() => loadExperiments(currentPage - 1)} disabled={currentPage <= 1 || loading}
            style={{ padding: "4px 10px", fontSize: 11, cursor: currentPage <= 1 ? "not-allowed" : "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: currentPage <= 1 ? "#f3f4f6" : "#fff" }}>
            上一页
          </button>
          <span style={{ fontSize: 12, fontWeight: 500, minWidth: 80, textAlign: "center" }}>
            {currentPage} / {totalPages}
          </span>
          <button onClick={() => loadExperiments(currentPage + 1)} disabled={currentPage >= totalPages || loading}
            style={{ padding: "4px 10px", fontSize: 11, cursor: currentPage >= totalPages ? "not-allowed" : "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: currentPage >= totalPages ? "#f3f4f6" : "#fff" }}>
            下一页
          </button>
          <button onClick={() => loadExperiments(totalPages)} disabled={currentPage >= totalPages || loading}
            style={{ padding: "4px 8px", fontSize: 11, cursor: currentPage >= totalPages ? "not-allowed" : "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: currentPage >= totalPages ? "#f3f4f6" : "#fff" }}>
            末页
          </button>
        </div>
      )}

      {!loading && experiments.length === 0 && (
        <div style={{ textAlign: "center", padding: 60, color: "#9ca3af", background: "#fff", borderRadius: 12 }}>
          暂无实验记录。请先在组合配置页面生成QLib配置。
        </div>
      )}

      {/* 脉冲动画 CSS */}
      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.3; }
        }
      `}</style>
    </main>
  );
}

function MetricBox({ label, value, fmt, color }: {
  label: string; value?: number | null; fmt: (v: number) => string; color: string;
}) {
  return (
    <div style={{ textAlign: "center" }}>
      <div style={{ fontSize: 15, fontWeight: 700, color: value != null ? color : "#d1d5db" }}>
        {value != null ? fmt(value) : "-"}
      </div>
      <div style={{ fontSize: 10, color: "#9ca3af" }}>{label}</div>
    </div>
  );
}
