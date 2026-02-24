"use client";

import { useEffect, useState, useCallback } from "react";

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
  created_at?: string;
  updated_at?: string;
};

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
  created:   { label: "已创建", color: "#3b82f6", border: "4px solid #3b82f6" },
  running:   { label: "运行中", color: "#f59e0b", border: "4px solid #f59e0b" },
  completed: { label: "已完成", color: "#10b981", border: "4px solid #10b981" },
  failed:    { label: "失败",   color: "#ef4444", border: "4px solid #ef4444" },
};

function getMetrics(exp: Experiment): Record<string, any> {
  if (!exp.result_metrics) return {};
  if (typeof exp.result_metrics === "string") {
    try { return JSON.parse(exp.result_metrics); } catch { return {}; }
  }
  return exp.result_metrics;
}

export default function ExperimentsPage() {
  const [experiments, setExperiments] = useState<Experiment[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [actionId, setActionId] = useState<string | null>(null);
  const [actionType, setActionType] = useState<string>("");
  const [toast, setToast] = useState<{ msg: string; ok: boolean } | null>(null);

  const showToast = useCallback((msg: string, ok: boolean) => {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 3500);
  }, []);

  async function loadExperiments() {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API}/quantevolver/experiments?limit=50`);
      const data = await res.json();
      setExperiments(data.items || []);
      setTotal(data.total || 0);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    }
    setLoading(false);
  }

  useEffect(() => { loadExperiments(); }, []);

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
          查看QLib回测实验记录，同步WSL执行结果，重新生成实验脚本
        </p>
      </section>

      {/* 工具栏 */}
      <section style={{ background: "#fff", borderRadius: 12, padding: 16, marginBottom: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
        <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
          <button onClick={loadExperiments} disabled={loading} style={{ padding: "6px 12px", fontSize: 12, cursor: "pointer", borderRadius: 6, border: "1px solid #d1d5db", background: "#fff" }}>
            {loading ? "加载中..." : "刷新"}
          </button>
          <span style={{ fontSize: 12, color: "#9ca3af" }}>共 {total} 条</span>
        </div>
        {error && <div style={{ marginTop: 8, padding: 8, background: "#fee2e2", borderRadius: 6, fontSize: 12, color: "#991b1b" }}>{error}</div>}
      </section>

      {/* 实验卡片列表 */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(360px, 1fr))", gap: 16 }}>
        {experiments.map(exp => {
          const expanded = expandedId === exp.experiment_id;
          const sm = STATUS_MAP[exp.status] || { label: exp.status, color: "#6b7280", border: "4px solid #e5e7eb" };
          const metrics = getMetrics(exp);
          const hasMetrics = Object.keys(metrics).length > 0;
          const isActioning = actionId === exp.experiment_id;
          
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
            <div
              key={exp.experiment_id}
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
                <span style={{ fontSize: 11, background: `${sm.color}15`, color: sm.color, padding: "2px 8px", borderRadius: 12, fontWeight: 600 }}>
                  {sm.label}
                </span>
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

                  {/* 操作按钮 */}
                  <div style={{ display: "flex", gap: 8, marginTop: 12, justifyContent: "flex-end" }}>
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
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>

      {!loading && experiments.length === 0 && (
        <div style={{ textAlign: "center", padding: 60, color: "#9ca3af", background: "#fff", borderRadius: 12 }}>
          暂无实验记录。请先在组合配置页面生成QLib配置。
        </div>
      )}
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
