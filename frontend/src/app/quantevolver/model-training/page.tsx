"use client";

import React, { useState, useEffect, useCallback } from "react";
import {
  Plus, Play, Trash2, ChevronDown, ChevronRight,
  Clock, Settings, RefreshCw, Brain, Loader2,
} from "lucide-react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

// ---------------------------------------------------------------------------
// 模型类型 Tab 配置 — 新增模型类型只需在此数组添加一项
// ---------------------------------------------------------------------------
type ConfigFieldDef = {
  key: string;
  label: string;
  type: "number" | "integer";
  default: number;
  step?: number;
};

type ModelTypeTab = {
  model_type: string;
  display_label: string;
  config_fields: ConfigFieldDef[];
};

const MODEL_TYPE_TABS: ModelTypeTab[] = [
  {
    model_type: "sector_hmm",
    display_label: "行业 HMM",
    config_fields: [
      { key: "n_states", label: "隐状态数 (n_states)", type: "integer", default: 2 },
      { key: "history_years", label: "历史年数 (history_years)", type: "number", default: 3.0, step: 0.5 },
      { key: "min_trading_days", label: "最小交易日 (min_trading_days)", type: "integer", default: 120 },
      { key: "cooldown_days", label: "冷却天数 (cooldown_days)", type: "integer", default: 3 },
      { key: "trending_coeff", label: "趋势系数 (trending_coeff)", type: "number", default: 1.5, step: 0.1 },
      { key: "fading_coeff", label: "衰退系数 (fading_coeff)", type: "number", default: 0.5, step: 0.1 },
      { key: "neutral_coeff", label: "中性系数 (neutral_coeff)", type: "number", default: 1.0, step: 0.1 },
    ],
  },
];

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
interface Config {
  config_id: string;
  model_type: string;
  display_name: string;
  config_json: Record<string, any>;
  snapshot_count: number;
  cron_expression: string | null;
  cron_enabled: boolean;
  created_at: string;
}

interface Snapshot {
  snapshot_id: string;
  config_id: string;
  trained_at: string;
  sector_count: number;
  status: string;
  metrics_json?: Record<string, any> | null;
}

interface Job {
  job_id: string;
  config_id: string;
  status: string;
  started_at: string | null;
  completed_at: string | null;
  error_message: string | null;
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------
const cardStyle: React.CSSProperties = {
  backgroundColor: "#ffffff",
  borderRadius: "12px",
  boxShadow: "0 4px 12px rgba(0, 0, 0, 0.05)",
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

const inputStyle: React.CSSProperties = {
  width: "100%", padding: "7px 10px", border: "1px solid #e5e7eb", borderRadius: 8,
  fontSize: 13, outline: "none", fontFamily: "inherit", color: "#1f2937", boxSizing: "border-box",
};

const btnPrimary: React.CSSProperties = {
  padding: "8px 18px", fontSize: 13, fontWeight: 600, background: "#2563eb",
  color: "#fff", border: "none", borderRadius: 8, cursor: "pointer",
  display: "inline-flex", alignItems: "center", gap: 6,
};

const btnDanger: React.CSSProperties = {
  padding: "6px 12px", fontSize: 12, fontWeight: 500, background: "#fff",
  color: "#ef4444", border: "1px solid #fecaca", borderRadius: 6, cursor: "pointer",
};

const statusColors: Record<string, { bg: string; text: string }> = {
  pending: { bg: "#fef3c7", text: "#d97706" },
  running: { bg: "#dbeafe", text: "#2563eb" },
  completed: { bg: "#d1fae5", text: "#059669" },
  failed: { bg: "#fee2e2", text: "#dc2626" },
};

function StatusBadge({ status }: { status: string }) {
  const c = statusColors[status] || { bg: "#f3f4f6", text: "#6b7280" };
  return (
    <span style={{ padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 600, backgroundColor: c.bg, color: c.text }}>
      {status}
    </span>
  );
}

function formatDate(s: string | null) {
  if (!s) return "-";
  try { return new Date(s).toLocaleString("zh-CN"); } catch { return s; }
}

function configSummary(cfg: Record<string, any>): string {
  const keys = ["n_states", "history_years", "min_trading_days"];
  return keys.filter(k => cfg[k] !== undefined).map(k => `${k}=${cfg[k]}`).join(", ");
}

// ---------------------------------------------------------------------------
// Main Page Component
// ---------------------------------------------------------------------------
export default function ModelTrainingPage() {
  const [activeTab, setActiveTab] = useState(MODEL_TYPE_TABS[0].model_type);
  const [configs, setConfigs] = useState<Config[]>([]);
  const [loading, setLoading] = useState(false);

  // Expanded config → snapshots & jobs
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [jobs, setJobs] = useState<Job[]>([]);

  // Create dialog
  const [showCreate, setShowCreate] = useState(false);
  const [createName, setCreateName] = useState("");
  const [createConfig, setCreateConfig] = useState<Record<string, number>>({});
  const [creating, setCreating] = useState(false);

  // Cron editing
  const [cronEditing, setCronEditing] = useState<string | null>(null);
  const [cronExpr, setCronExpr] = useState("");
  const [cronEnabled, setCronEnabled] = useState(false);

  const currentTab = MODEL_TYPE_TABS.find(t => t.model_type === activeTab) || MODEL_TYPE_TABS[0];

  // ── Fetch configs ──
  const fetchConfigs = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API}/hmm-training/configs?model_type=${activeTab}`);
      if (res.ok) {
        const data = await res.json();
        setConfigs(Array.isArray(data) ? data : []);
      }
    } catch (e) { console.error("Failed to fetch configs:", e); }
    setLoading(false);
  }, [activeTab]);

  useEffect(() => {
    fetchConfigs();
    setExpandedId(null);
  }, [fetchConfigs]);

  // ── Fetch snapshots & jobs for expanded config ──
  const fetchDetails = useCallback(async (configId: string) => {
    try {
      const [snapRes, jobRes] = await Promise.all([
        fetch(`${API}/hmm-training/configs/${configId}/snapshots`),
        fetch(`${API}/hmm-training/configs/${configId}/jobs`),
      ]);
      if (snapRes.ok) { const d = await snapRes.json(); setSnapshots(Array.isArray(d) ? d : []); }
      if (jobRes.ok) { const d = await jobRes.json(); setJobs(Array.isArray(d) ? d : []); }
    } catch (e) { console.error("Failed to fetch details:", e); }
  }, []);

  const toggleExpand = (configId: string) => {
    if (expandedId === configId) {
      setExpandedId(null);
    } else {
      setExpandedId(configId);
      fetchDetails(configId);
    }
  };

  // ── Create config ──
  const handleCreate = async () => {
    if (!createName.trim()) { alert("请输入配置名称"); return; }
    setCreating(true);
    try {
      const res = await fetch(`${API}/hmm-training/configs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model_type: activeTab,
          display_name: createName.trim(),
          config_json: createConfig,
        }),
      });
      if (res.ok) {
        setShowCreate(false);
        setCreateName("");
        setCreateConfig({});
        fetchConfigs();
      } else {
        const err = await res.json().catch(() => ({}));
        alert(err.detail || "创建失败");
      }
    } catch (e: any) { alert("创建失败: " + (e?.message || "网络错误")); }
    setCreating(false);
  };

  // ── Delete config ──
  const handleDeleteConfig = async (configId: string) => {
    if (!confirm("确认删除此配置？")) return;
    try {
      const res = await fetch(`${API}/hmm-training/configs/${configId}`, { method: "DELETE" });
      if (res.ok) { fetchConfigs(); }
      else { const err = await res.json().catch(() => ({})); alert(err.detail || "删除失败"); }
    } catch (e: any) { alert("删除失败: " + (e?.message || "网络错误")); }
  };

  // ── Trigger training ──
  const handleTrigger = async (configId: string) => {
    try {
      const res = await fetch(`${API}/hmm-training/configs/${configId}/trigger-training`, { method: "POST" });
      if (res.ok) {
        fetchConfigs();
        if (expandedId === configId) fetchDetails(configId);
      } else {
        const err = await res.json().catch(() => ({}));
        alert(err.detail || "触发训练失败");
      }
    } catch (e: any) { alert("触发训练失败: " + (e?.message || "网络错误")); }
  };

  // ── Delete snapshot ──
  const handleDeleteSnapshot = async (snapshotId: string) => {
    if (!confirm("确认删除此快照？")) return;
    try {
      const res = await fetch(`${API}/hmm-training/snapshots/${snapshotId}`, { method: "DELETE" });
      if (res.ok && expandedId) {
        fetchDetails(expandedId);
        fetchConfigs();
      } else {
        const err = await res.json().catch(() => ({}));
        alert(err.detail || "删除快照失败");
      }
    } catch (e: any) { alert("删除快照失败: " + (e?.message || "网络错误")); }
  };

  // ── Update cron ──
  const handleSaveCron = async (configId: string) => {
    try {
      const res = await fetch(`${API}/hmm-training/configs/${configId}/cron`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ cron_expression: cronExpr || null, cron_enabled: cronEnabled }),
      });
      if (res.ok) {
        setCronEditing(null);
        fetchConfigs();
      } else {
        const err = await res.json().catch(() => ({}));
        alert(err.detail || "保存失败");
      }
    } catch (e: any) { alert("保存失败: " + (e?.message || "网络错误")); }
  };

  const hasActiveJob = (configId: string): boolean => {
    if (expandedId !== configId) return false;
    return jobs.some(j => j.status === "pending" || j.status === "running");
  };

  // ── Render ──
  return (
    <main style={{ padding: 24, maxWidth: 1200, margin: "0 auto" }}>
      {/* Page title */}
      <h1 style={{ fontSize: 24, fontWeight: 700, color: "#1e293b", marginBottom: 20 }}>
        <Brain size={22} style={{ verticalAlign: "middle", marginRight: 8 }} />
        模型训练
      </h1>

      {/* Model type tabs */}
      <div style={{ display: "flex", gap: 4, marginBottom: 20 }}>
        {MODEL_TYPE_TABS.map(tab => (
          <button
            key={tab.model_type}
            onClick={() => setActiveTab(tab.model_type)}
            style={{
              padding: "8px 20px", fontSize: 13, fontWeight: 600, borderRadius: 8, border: "none", cursor: "pointer",
              backgroundColor: activeTab === tab.model_type ? "#2563eb" : "#f1f5f9",
              color: activeTab === tab.model_type ? "#fff" : "#64748b",
              transition: "all 0.15s",
            }}
          >
            {tab.display_label}
          </button>
        ))}
      </div>

      {/* Toolbar */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
        <span style={{ fontSize: 13, color: "#64748b" }}>
          {loading ? "加载中..." : `共 ${configs.length} 个配置`}
        </span>
        <div style={{ display: "flex", gap: 8 }}>
          <button onClick={fetchConfigs} style={{ ...btnPrimary, background: "#f1f5f9", color: "#475569" }}>
            <RefreshCw size={14} /> 刷新
          </button>
          <button onClick={() => { setShowCreate(true); setCreateName(""); setCreateConfig({}); }} style={btnPrimary}>
            <Plus size={14} /> 创建新配置
          </button>
        </div>
      </div>

      {/* Config list */}
      <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
        {configs.map(cfg => {
          const isExpanded = expandedId === cfg.config_id;
          const activeJob = isExpanded && jobs.find(j => j.status === "pending" || j.status === "running");
          return (
            <div key={cfg.config_id} style={cardStyle}>
              {/* Config header row */}
              <div
                style={{ ...headerStyle, cursor: "pointer" }}
                onClick={() => toggleExpand(cfg.config_id)}
              >
                <div style={{ display: "flex", alignItems: "center", gap: 10, flex: 1 }}>
                  {isExpanded ? <ChevronDown size={16} color="#64748b" /> : <ChevronRight size={16} color="#64748b" />}
                  <div>
                    <div style={{ fontSize: 14, fontWeight: 600, color: "#1e293b" }}>{cfg.display_name}</div>
                    <div style={{ fontSize: 12, color: "#94a3b8", marginTop: 2 }}>{configSummary(cfg.config_json)}</div>
                  </div>
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 16, fontSize: 12, color: "#64748b" }}>
                  <span>快照: {cfg.snapshot_count}</span>
                  {cfg.cron_enabled && <span style={{ color: "#059669" }}>⏰ 定时训练</span>}
                  <span>{formatDate(cfg.created_at)}</span>
                  <button
                    onClick={e => { e.stopPropagation(); handleTrigger(cfg.config_id); }}
                    disabled={!!activeJob}
                    style={{
                      ...btnPrimary, padding: "6px 12px", fontSize: 12,
                      opacity: activeJob ? 0.5 : 1,
                      cursor: activeJob ? "not-allowed" : "pointer",
                    }}
                    title={activeJob ? `训练中 (${activeJob.status})` : "触发训练"}
                  >
                    {activeJob ? <Loader2 size={13} className="animate-spin" /> : <Play size={13} />}
                    {activeJob ? activeJob.status : "触发训练"}
                  </button>
                  <button
                    onClick={e => {
                      e.stopPropagation();
                      setCronEditing(cfg.config_id);
                      setCronExpr(cfg.cron_expression || "");
                      setCronEnabled(cfg.cron_enabled);
                    }}
                    style={{ padding: "6px 10px", fontSize: 12, background: "#f1f5f9", border: "none", borderRadius: 6, cursor: "pointer", color: "#475569", display: "inline-flex", alignItems: "center", gap: 4 }}
                  >
                    <Clock size={13} /> Cron
                  </button>
                  <button
                    onClick={e => { e.stopPropagation(); handleDeleteConfig(cfg.config_id); }}
                    style={btnDanger}
                  >
                    <Trash2 size={13} />
                  </button>
                </div>
              </div>

              {/* Expanded: Snapshots & Jobs */}
              {isExpanded && (
                <div style={{ padding: "16px 20px" }}>
                  {/* 模型说明 */}
                  {cfg.config_json?.description && (
                    <div style={{ fontSize: 12, color: "#475569", padding: "8px 12px", backgroundColor: "#f0f9ff", borderRadius: 6, marginBottom: 12, lineHeight: 1.6 }}>
                      {cfg.config_json.description}
                    </div>
                  )}

                  {/* Snapshot list with test metrics */}
                  <h4 style={{ fontSize: 13, fontWeight: 600, color: "#475569", marginBottom: 8 }}>快照列表</h4>
                  {snapshots.length === 0 ? (
                    <div style={{ fontSize: 12, color: "#94a3b8", padding: "8px 0" }}>暂无快照，请触发训练</div>
                  ) : (
                    snapshots.map(snap => {
                      const tm = snap.metrics_json?.test_metrics;
                      const rank = snap.metrics_json?.test_rank;
                      const rankTotal = snap.metrics_json?.test_rank_total;
                      return (
                        <div key={snap.snapshot_id} style={{ border: "1px solid #e2e8f0", borderRadius: 8, marginBottom: 8, overflow: "hidden" }}>
                          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 12px", backgroundColor: "#f8fafc" }}>
                            <div style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 12 }}>
                              {rank && (
                                <span style={{
                                  display: "inline-flex", alignItems: "center", justifyContent: "center",
                                  width: 24, height: 24, borderRadius: "50%", fontSize: 11, fontWeight: 700,
                                  backgroundColor: rank === 1 ? "#fbbf24" : rank <= 3 ? "#a3e635" : rank <= 5 ? "#93c5fd" : "#e2e8f0",
                                  color: rank === 1 ? "#78350f" : rank <= 3 ? "#365314" : rank <= 5 ? "#1e3a5f" : "#64748b",
                                }}>
                                  #{rank}
                                </span>
                              )}
                              <span style={{ color: "#1e293b", fontWeight: 500 }}>{formatDate(snap.trained_at)}</span>
                              <span style={{ color: "#64748b" }}>{snap.sector_count} 行业</span>
                              <StatusBadge status={snap.status} />
                            </div>
                            <button onClick={() => handleDeleteSnapshot(snap.snapshot_id)} style={btnDanger}>
                              <Trash2 size={12} />
                            </button>
                          </div>
                          {/* Test metrics table */}
                          {tm && (
                            <div style={{ padding: "8px 12px" }}>
                              <div style={{ fontSize: 11, color: "#64748b", marginBottom: 4 }}>测试集表现 ({tm.test_period})</div>
                              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                                <thead>
                                  <tr style={{ borderBottom: "1px solid #f1f5f9" }}>
                                    <th style={{ textAlign: "left", padding: "3px 6px", color: "#94a3b8", fontWeight: 500 }}>窗口</th>
                                    {[1,2,3,5,10,20].map(w => (
                                      <th key={w} style={{ textAlign: "right", padding: "3px 6px", color: "#94a3b8", fontWeight: 500 }}>{w}d</th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody>
                                  <tr>
                                    <td style={{ padding: "3px 6px", color: "#64748b" }}>Spread</td>
                                    {[1,2,3,5,10,20].map(w => {
                                      const v = tm[`spread_${w}d`];
                                      const color = v == null ? "#94a3b8" : v > 0 ? "#16a34a" : v < 0 ? "#dc2626" : "#64748b";
                                      return <td key={w} style={{ textAlign: "right", padding: "3px 6px", color, fontWeight: v != null && v > 0 ? 600 : 400 }}>
                                        {v != null ? (v > 0 ? "+" : "") + v.toFixed(4) + "%" : "-"}
                                      </td>;
                                    })}
                                  </tr>
                                  <tr>
                                    <td style={{ padding: "3px 6px", color: "#64748b" }}>热态胜率</td>
                                    {[1,2,3,5,10,20].map(w => {
                                      const v = tm[`trending_${w}d_winrate`];
                                      return <td key={w} style={{ textAlign: "right", padding: "3px 6px", color: "#475569" }}>
                                        {v != null ? v.toFixed(1) + "%" : "-"}
                                      </td>;
                                    })}
                                  </tr>
                                  <tr>
                                    <td style={{ padding: "3px 6px", color: "#64748b" }}>冷态胜率</td>
                                    {[1,2,3,5,10,20].map(w => {
                                      const v = tm[`fading_${w}d_winrate`];
                                      return <td key={w} style={{ textAlign: "right", padding: "3px 6px", color: "#475569" }}>
                                        {v != null ? v.toFixed(1) + "%" : "-"}
                                      </td>;
                                    })}
                                  </tr>
                                </tbody>
                              </table>
                              {rank && rankTotal && (
                                <div style={{ fontSize: 11, color: "#64748b", marginTop: 4 }}>
                                  综合排名: <span style={{ fontWeight: 600, color: rank <= 3 ? "#16a34a" : "#475569" }}>第 {rank} / {rankTotal}</span>
                                </div>
                              )}
                            </div>
                          )}
                        </div>
                      );
                    })
                  )}

                  {/* Recent jobs */}
                  {jobs.length > 0 && (
                    <div style={{ marginTop: 16 }}>
                      <h4 style={{ fontSize: 13, fontWeight: 600, color: "#475569", marginBottom: 8 }}>最近训练任务</h4>
                      <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                        {jobs.slice(0, 5).map(job => (
                          <div key={job.job_id} style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 12, padding: "4px 8px", backgroundColor: "#f8fafc", borderRadius: 6 }}>
                            <StatusBadge status={job.status} />
                            <span style={{ color: "#64748b" }}>
                              {job.started_at ? formatDate(job.started_at) : "等待中"}
                              {job.completed_at && ` → ${formatDate(job.completed_at)}`}
                            </span>
                            {job.error_message && (
                              <span style={{ color: "#ef4444", fontSize: 11 }} title={job.error_message}>
                                {job.error_message.slice(0, 60)}...
                              </span>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        })}
        {!loading && configs.length === 0 && (
          <div style={{ textAlign: "center", padding: 40, color: "#94a3b8", fontSize: 14 }}>
            暂无配置，点击「创建新配置」开始
          </div>
        )}
      </div>

      {/* ── Create Config Dialog ── */}
      {showCreate && (
        <div style={{ position: "fixed", top: 0, left: 0, right: 0, bottom: 0, backgroundColor: "rgba(0,0,0,0.4)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000 }}>
          <div style={{ backgroundColor: "#fff", borderRadius: 12, padding: 24, width: 480, maxHeight: "80vh", overflowY: "auto", boxShadow: "0 20px 60px rgba(0,0,0,0.15)" }}>
            <h3 style={{ margin: "0 0 16px", fontSize: 16, fontWeight: 700, color: "#1e293b" }}>
              创建新配置 — {currentTab.display_label}
            </h3>

            <div style={{ marginBottom: 12 }}>
              <label style={{ display: "block", fontSize: 12, fontWeight: 500, color: "#6b7280", marginBottom: 4 }}>
                配置名称 <span style={{ color: "#ef4444" }}>*</span>
              </label>
              <input
                value={createName}
                onChange={e => setCreateName(e.target.value)}
                placeholder="例如：默认2状态配置"
                style={inputStyle}
              />
            </div>

            <div style={{ fontSize: 12, fontWeight: 600, color: "#475569", marginBottom: 8 }}>超参设置</div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
              {currentTab.config_fields.map(field => (
                <div key={field.key}>
                  <label style={{ display: "block", fontSize: 11, color: "#6b7280", marginBottom: 2 }}>{field.label}</label>
                  <input
                    type="number"
                    step={field.step || (field.type === "integer" ? 1 : 0.1)}
                    value={createConfig[field.key] ?? field.default}
                    onChange={e => setCreateConfig(prev => ({ ...prev, [field.key]: parseFloat(e.target.value) || field.default }))}
                    style={inputStyle}
                  />
                </div>
              ))}
            </div>

            <div style={{ display: "flex", justifyContent: "flex-end", gap: 8, marginTop: 20 }}>
              <button onClick={() => setShowCreate(false)} style={{ padding: "8px 16px", fontSize: 13, background: "#f1f5f9", color: "#475569", border: "none", borderRadius: 8, cursor: "pointer" }}>
                取消
              </button>
              <button onClick={handleCreate} disabled={creating} style={{ ...btnPrimary, opacity: creating ? 0.6 : 1 }}>
                {creating ? "创建中..." : "创建"}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* ── Cron Config Dialog ── */}
      {cronEditing && (
        <div style={{ position: "fixed", top: 0, left: 0, right: 0, bottom: 0, backgroundColor: "rgba(0,0,0,0.4)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000 }}>
          <div style={{ backgroundColor: "#fff", borderRadius: 12, padding: 24, width: 400, boxShadow: "0 20px 60px rgba(0,0,0,0.15)" }}>
            <h3 style={{ margin: "0 0 16px", fontSize: 16, fontWeight: 700, color: "#1e293b" }}>
              <Settings size={16} style={{ verticalAlign: "middle", marginRight: 6 }} />
              滚动训练计划
            </h3>

            <div style={{ marginBottom: 12 }}>
              <label style={{ display: "block", fontSize: 12, fontWeight: 500, color: "#6b7280", marginBottom: 4 }}>Cron 表达式</label>
              <input
                value={cronExpr}
                onChange={e => setCronExpr(e.target.value)}
                placeholder="例如：0 6 * * 1-5（工作日早6点）"
                style={inputStyle}
              />
            </div>

            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 16 }}>
              <input
                type="checkbox"
                id="cron-enabled"
                checked={cronEnabled}
                onChange={e => setCronEnabled(e.target.checked)}
              />
              <label htmlFor="cron-enabled" style={{ fontSize: 13, color: "#475569" }}>启用定时训练</label>
            </div>

            <div style={{ display: "flex", justifyContent: "flex-end", gap: 8 }}>
              <button onClick={() => setCronEditing(null)} style={{ padding: "8px 16px", fontSize: 13, background: "#f1f5f9", color: "#475569", border: "none", borderRadius: 8, cursor: "pointer" }}>
                取消
              </button>
              <button onClick={() => handleSaveCron(cronEditing)} style={btnPrimary}>
                保存
              </button>
            </div>
          </div>
        </div>
      )}
    </main>
  );
}
