"use client";

import { useEffect, useState, useCallback } from "react";
import dynamic from "next/dynamic";
import type { EditorProps } from "@monaco-editor/react";

// @ts-ignore
const MonacoEditor = dynamic(
  () => import("@monaco-editor/react").then((mod) => mod.default as React.ComponentType<any>),
  { ssr: false }
);

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

/* ── 类型定义 ── */
type ParamDef = {
  name: string;
  type: string;
  default: any;
  description?: string;
  min?: number;
  max?: number;
  options?: string[];
};

type Strategy = {
  strategy_id: string;
  display_name: string;
  description?: string;
  strategy_type?: string;
  catalog_source?: string;
  scenario?: string;
  market?: string;
  freq?: string;
  source_code?: string;
  portfolio_config?: any;
  default_kwargs?: any;
  param_schema?: ParamDef[];
  in_selection_center?: boolean;
  parent_strategy_id?: string;
  created_at?: string;
  updated_at?: string;
  llm_analysis?: any;
};

type LLMReview = {
  syntax_issues?: string[];
  logic_issues?: string[];
  suggestions?: string[];
  overall_rating?: string;
  summary?: string;
};

type AnalysisResult = {
  syntax_ok: boolean;
  syntax_errors: { line: number; col: number; message: string; text: string }[];
  params_detected: { name: string; type: string; default: any }[];
  llm_review?: LLMReview;
};

/* ── 类型标签颜色 ── */
const TYPE_COLORS: Record<string, { bg: string; text: string; label: string }> = {
  daily: { bg: "#dbeafe", text: "#2563eb", label: "日频" },
  intraday: { bg: "#fef3c7", text: "#d97706", label: "日内" },
};

/* ── 评级颜色 ── */
const RATING_COLORS: Record<string, string> = {
  A: "#10b981",
  B: "#3b82f6",
  C: "#f59e0b",
  D: "#ef4444",
};

/* ── 通用样式 ── */
const inputStyle: React.CSSProperties = {
  width: "100%", padding: "7px 10px", border: "1px solid #e5e7eb", borderRadius: 8,
  fontSize: 13, outline: "none", fontFamily: "inherit", color: "#1f2937",
};
const labelStyle: React.CSSProperties = {
  display: "block", fontSize: 12, fontWeight: 500, color: "#6b7280", marginBottom: 4,
};
const btnPrimary: React.CSSProperties = {
  padding: "8px 18px", fontSize: 13, fontWeight: 600, background: "#10b981",
  color: "#fff", border: "none", borderRadius: 8, cursor: "pointer",
};
const btnSecondary: React.CSSProperties = {
  padding: "8px 18px", fontSize: 13, fontWeight: 500, background: "#fff",
  color: "#374151", border: "1px solid #e5e7eb", borderRadius: 8, cursor: "pointer",
};

export default function StrategiesPage() {
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [typeFilter, setTypeFilter] = useState<string>("");
  const [searchText, setSearchText] = useState("");

  // 编辑/新建弹窗
  const [editOpen, setEditOpen] = useState(false);
  const [editMode, setEditMode] = useState<"create" | "edit" | "clone">("create");
  const [editStrategy, setEditStrategy] = useState<Strategy | null>(null);
  const [editCode, setEditCode] = useState("");
  const [editParams, setEditParams] = useState<Record<string, any>>({});
  const [editForm, setEditForm] = useState({
    strategy_id: "",
    display_name: "",
    description: "",
    strategy_type: "daily",
    market: "csi300",
    freq: "day",
  });
  const [saving, setSaving] = useState(false);

  // LLM分析
  const [analyzing, setAnalyzing] = useState(false);
  const [analysisResult, setAnalysisResult] = useState<AnalysisResult | null>(null);

  // 删除确认
  const [deleteConfirm, setDeleteConfirm] = useState<string | null>(null);

  /* ── 加载策略列表 ── */
  const loadStrategies = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({ limit: "100" });
      if (typeFilter) params.set("strategy_type", typeFilter);
      if (searchText) params.set("search", searchText);
      const res = await fetch(`${API}/quantevolver/strategies?${params}`);
      const data = await res.json();
      setStrategies(data.items || []);
      setTotal(data.total || 0);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    }
    setLoading(false);
  }, [typeFilter, searchText]);

  useEffect(() => { loadStrategies(); }, [loadStrategies]);

  /* ── 打开编辑弹窗 ── */
  const openEdit = async (strategyId: string) => {
    try {
      const res = await fetch(`${API}/quantevolver/strategies/${strategyId}`);
      const data = await res.json();
      if (!data.ok) throw new Error(data.detail || "加载失败");
      const s = data.data as Strategy;
      setEditStrategy(s);
      setEditMode("edit");
      setEditCode(s.source_code || "");
      setEditForm({
        strategy_id: s.strategy_id,
        display_name: s.display_name,
        description: s.description || "",
        strategy_type: s.strategy_type || "daily",
        market: s.market || "csi300",
        freq: s.freq || "day",
      });
      const kwargs = typeof s.default_kwargs === "string" ? JSON.parse(s.default_kwargs) : (s.default_kwargs || {});
      setEditParams(kwargs);
      setAnalysisResult(null);
      setEditOpen(true);
    } catch (e: any) {
      alert("加载策略详情失败: " + (e?.message || "未知错误"));
    }
  };

  /* ── 打开新建弹窗 ── */
  const openCreate = () => {
    setEditStrategy(null);
    setEditMode("create");
    setEditCode("");
    setEditParams({});
    setEditForm({
      strategy_id: "",
      display_name: "",
      description: "",
      strategy_type: "daily",
      market: "csi300",
      freq: "day",
    });
    setAnalysisResult(null);
    setEditOpen(true);
  };

  /* ── 从模板新建 ── */
  const openClone = async (strategyId: string) => {
    try {
      const res = await fetch(`${API}/quantevolver/strategies/${strategyId}`);
      const data = await res.json();
      if (!data.ok) throw new Error(data.detail || "加载失败");
      const s = data.data as Strategy;
      setEditStrategy(s);
      setEditMode("clone");
      setEditCode(s.source_code || "");
      const kwargs = typeof s.default_kwargs === "string" ? JSON.parse(s.default_kwargs) : (s.default_kwargs || {});
      setEditParams(kwargs);
      setEditForm({
        strategy_id: s.strategy_id + "_copy",
        display_name: s.display_name + " (副本)",
        description: s.description || "",
        strategy_type: s.strategy_type || "daily",
        market: s.market || "csi300",
        freq: s.freq || "day",
      });
      setAnalysisResult(null);
      setEditOpen(true);
    } catch (e: any) {
      alert("加载模板策略失败: " + (e?.message || "未知错误"));
    }
  };

  /* ── 保存策略 ── */
  const handleSave = async () => {
    setSaving(true);
    try {
      if (editMode === "create" || editMode === "clone") {
        const url = editMode === "clone" && editStrategy
          ? `${API}/quantevolver/strategies/${editStrategy.strategy_id}/clone`
          : `${API}/quantevolver/strategies`;
        const body = {
          strategy_id: editForm.strategy_id,
          display_name: editForm.display_name,
          description: editForm.description,
          strategy_type: editForm.strategy_type,
          source_code: editCode,
          market: editForm.market,
          freq: editForm.freq,
          default_kwargs: editParams,
        };
        const res = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || "创建失败");
      } else {
        const body: any = {
          display_name: editForm.display_name,
          description: editForm.description,
          strategy_type: editForm.strategy_type,
          source_code: editCode,
          market: editForm.market,
          freq: editForm.freq,
          default_kwargs: editParams,
        };
        const res = await fetch(`${API}/quantevolver/strategies/${editForm.strategy_id}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || "更新失败");
      }
      setEditOpen(false);
      loadStrategies();
    } catch (e: any) {
      alert("保存失败: " + (e?.message || "未知错误"));
    }
    setSaving(false);
  };

  /* ── 删除策略 ── */
  const handleDelete = async (strategyId: string) => {
    try {
      const res = await fetch(`${API}/quantevolver/strategies/${strategyId}`, { method: "DELETE" });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || "删除失败");
      setDeleteConfirm(null);
      loadStrategies();
    } catch (e: any) {
      alert("删除失败: " + (e?.message || "未知错误"));
    }
  };

  /* ── LLM分析 ── */
  const handleAnalyze = async () => {
    setAnalyzing(true);
    setAnalysisResult(null);
    try {
      const res = await fetch(`${API}/quantevolver/strategies/analyze`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          strategy_id: editForm.strategy_id || "temp_analysis",
          source_code: editCode,
        }),
      });
      const data = await res.json();
      if (data.ok && data.analysis) {
        setAnalysisResult(data.analysis);
      } else {
        alert("分析失败: " + (data.detail || "未知错误"));
      }
    } catch (e: any) {
      alert("分析请求失败: " + (e?.message || ""));
    }
    setAnalyzing(false);
  };

  /* ── 获取param_schema ── */
  const getParamSchema = (): ParamDef[] => {
    if (editStrategy?.param_schema) {
      const ps = typeof editStrategy.param_schema === "string"
        ? JSON.parse(editStrategy.param_schema)
        : editStrategy.param_schema;
      return Array.isArray(ps) ? ps : [];
    }
    return [];
  };

  /* ── 渲染参数输入控件 ── */
  const renderParamInput = (p: ParamDef) => {
    if (p.type === "bool") {
      return (
        <select value={String(editParams[p.name] ?? p.default ?? false)}
          onChange={e => setEditParams(prev => ({ ...prev, [p.name]: e.target.value === "true" }))}
          style={inputStyle} title={p.description || p.name}>
          <option value="true">true</option>
          <option value="false">false</option>
        </select>
      );
    }
    if (p.type === "select" && p.options) {
      const opts = typeof p.options === "string" ? (p.options as string).split(/\s+/) : p.options;
      return (
        <select value={String(editParams[p.name] ?? p.default ?? "")}
          onChange={e => setEditParams(prev => ({ ...prev, [p.name]: e.target.value }))}
          style={inputStyle} title={p.description || p.name}>
          {opts.map((o: string) => <option key={o} value={o}>{o}</option>)}
        </select>
      );
    }
    return (
      <input
        type={p.type === "int" || p.type === "float" ? "number" : "text"}
        step={p.type === "float" ? "0.01" : "1"}
        min={p.min} max={p.max}
        value={editParams[p.name] ?? p.default ?? ""}
        onChange={e => {
          let val: any = e.target.value;
          if (p.type === "int") val = parseInt(val) || 0;
          else if (p.type === "float") val = parseFloat(val) || 0;
          setEditParams(prev => ({ ...prev, [p.name]: val }));
        }}
        style={inputStyle}
        placeholder={p.description || p.name}
        title={p.description || p.name}
      />
    );
  };

  /* ── 渲染 ── */
  return (
    <main style={{ padding: 24 }}>
      {/* 顶部横幅 — 与总览页一致的渐变风格 */}
      <section style={{
        background: "linear-gradient(135deg, #7c3aed 0%, #2563eb 50%, #06b6d4 100%)",
        borderRadius: 16, padding: 24, color: "#fff", marginBottom: 24,
      }}>
        <h1 style={{ margin: 0, fontSize: 28, fontWeight: 700 }}>策略库</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 14 }}>
          管理交易策略：查看、编辑、新建策略，支持参数调整和LLM代码分析
        </p>
      </section>

      {/* 工具栏 */}
      <section style={{
        background: "#fff", borderRadius: 12, padding: 16, marginBottom: 16,
        boxShadow: "0 1px 3px rgba(0,0,0,0.08)", display: "flex", flexWrap: "wrap",
        alignItems: "center", gap: 12,
      }}>
        <button onClick={openCreate} style={{ ...btnPrimary, background: "#7c3aed" }}>+ 新建策略</button>
        <input type="text" placeholder="搜索策略..." value={searchText}
          onChange={e => setSearchText(e.target.value)}
          style={{ ...inputStyle, width: 200 }} title="搜索策略" />
        <select value={typeFilter} onChange={e => setTypeFilter(e.target.value)}
          style={{ ...inputStyle, width: 130 }} title="筛选策略类型">
          <option value="">全部类型</option>
          <option value="daily">日频策略</option>
          <option value="intraday">日内策略</option>
        </select>
        <button onClick={() => loadStrategies()} disabled={loading}
          style={{ ...btnSecondary, opacity: loading ? 0.6 : 1 }}>
          {loading ? "加载中..." : "刷新"}
        </button>
        <span style={{ fontSize: 12, color: "#9ca3af", marginLeft: "auto" }}>共 {total} 条策略</span>
      </section>

      {error && (
        <div style={{ background: "#fef2f2", color: "#b91c1c", padding: 12, borderRadius: 8, marginBottom: 16, fontSize: 13 }}>
          {error}
        </div>
      )}

      {/* 策略列表 */}
      <section style={{
        background: "#fff", borderRadius: 12, boxShadow: "0 1px 3px rgba(0,0,0,0.08)", overflow: "hidden",
      }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
          <thead>
            <tr style={{ background: "#f9fafb", textAlign: "left" }}>
              <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12 }}>策略名称</th>
              <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12 }}>描述</th>
              <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12, width: 70 }}>类型</th>
              <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12, width: 70 }}>市场</th>
              <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12, width: 80 }}>来源</th>
              <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12, width: 180, textAlign: "right" }}>操作</th>
            </tr>
          </thead>
          <tbody>
            {strategies.map(s => {
              const tc = TYPE_COLORS[s.strategy_type || "daily"] || TYPE_COLORS.daily;
              return (
                <tr key={s.strategy_id} style={{ borderTop: "1px solid #f3f4f6" }}
                  onMouseEnter={e => (e.currentTarget.style.background = "#f9fafb")}
                  onMouseLeave={e => (e.currentTarget.style.background = "transparent")}>
                  <td style={{ padding: "10px 16px" }}>
                    <div style={{ fontWeight: 600, color: "#111827" }}>{s.display_name}</div>
                    <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 2 }}>{s.strategy_id}</div>
                  </td>
                  <td style={{ padding: "10px 16px", color: "#4b5563", fontSize: 12, maxWidth: 320, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {s.description || "-"}
                  </td>
                  <td style={{ padding: "10px 16px" }}>
                    <span style={{ padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 600, background: tc.bg, color: tc.text }}>
                      {tc.label}
                    </span>
                  </td>
                  <td style={{ padding: "10px 16px", fontSize: 12, color: "#6b7280" }}>{s.market || "-"}</td>
                  <td style={{ padding: "10px 16px", fontSize: 12, color: "#6b7280" }}>{s.catalog_source || "-"}</td>
                  <td style={{ padding: "10px 16px", textAlign: "right" }}>
                    <div style={{ display: "flex", gap: 6, justifyContent: "flex-end" }}>
                      <button onClick={() => openEdit(s.strategy_id)}
                        style={{ padding: "4px 10px", fontSize: 12, background: "#eff6ff", color: "#2563eb", border: "none", borderRadius: 6, cursor: "pointer" }}>
                        编辑
                      </button>
                      <button onClick={() => openClone(s.strategy_id)}
                        style={{ padding: "4px 10px", fontSize: 12, background: "#ecfdf5", color: "#059669", border: "none", borderRadius: 6, cursor: "pointer" }}>
                        模板新建
                      </button>
                      <button onClick={() => setDeleteConfirm(s.strategy_id)}
                        style={{ padding: "4px 10px", fontSize: 12, background: "#fef2f2", color: "#dc2626", border: "none", borderRadius: 6, cursor: "pointer" }}>
                        删除
                      </button>
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        {!loading && strategies.length === 0 && (
          <div style={{ textAlign: "center", padding: 48, color: "#9ca3af", fontSize: 14 }}>暂无策略数据</div>
        )}
      </section>

      {/* 删除确认弹窗 */}
      {deleteConfirm && (
        <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.4)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000 }}>
          <div style={{ background: "#fff", borderRadius: 12, padding: 24, width: 380, boxShadow: "0 20px 60px rgba(0,0,0,0.15)" }}>
            <h3 style={{ margin: "0 0 8px", fontSize: 17, fontWeight: 700, color: "#111827" }}>确认删除</h3>
            <p style={{ fontSize: 13, color: "#6b7280", marginBottom: 20 }}>
              确定要删除策略 <strong>{deleteConfirm}</strong> 吗？此操作不可撤销。
            </p>
            <div style={{ display: "flex", gap: 10, justifyContent: "flex-end" }}>
              <button onClick={() => setDeleteConfirm(null)} style={btnSecondary}>取消</button>
              <button onClick={() => handleDelete(deleteConfirm)}
                style={{ ...btnPrimary, background: "#dc2626" }}>删除</button>
            </div>
          </div>
        </div>
      )}

      {/* ═══════ 编辑/新建弹窗 — 全屏白色背景 ═══════ */}
      {editOpen && (
        <div style={{
          position: "fixed", inset: 0, background: "#f3f4f6", zIndex: 1000,
          display: "flex", flexDirection: "column", overflow: "hidden",
        }}>
          {/* 弹窗顶部栏 */}
          <div style={{
            background: "linear-gradient(135deg, #7c3aed 0%, #2563eb 100%)",
            padding: "14px 24px", display: "flex", alignItems: "center", justifyContent: "space-between",
            flexShrink: 0,
          }}>
            <div>
              <h2 style={{ margin: 0, fontSize: 18, fontWeight: 700, color: "#fff" }}>
                {editMode === "create" ? "新建策略" : editMode === "clone" ? "从模板新建策略" : "编辑策略"}
              </h2>
              {editMode === "clone" && editStrategy && (
                <span style={{ fontSize: 12, color: "rgba(255,255,255,0.7)" }}>基于: {editStrategy.display_name}</span>
              )}
            </div>
            <button onClick={() => setEditOpen(false)}
              style={{ background: "rgba(255,255,255,0.2)", border: "none", color: "#fff", fontSize: 20, width: 32, height: 32, borderRadius: 8, cursor: "pointer", lineHeight: "32px" }}>
              &times;
            </button>
          </div>

          {/* 基本信息表单 */}
          <div style={{
            background: "#fff", padding: "16px 24px", borderBottom: "1px solid #e5e7eb", flexShrink: 0,
          }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 16 }}>
              <div>
                <label style={labelStyle}>策略ID</label>
                <input type="text" value={editForm.strategy_id}
                  onChange={e => setEditForm(f => ({ ...f, strategy_id: e.target.value }))}
                  disabled={editMode === "edit"} style={{ ...inputStyle, ...(editMode === "edit" ? { background: "#f9fafb", color: "#9ca3af" } : {}) }}
                  placeholder="my_strategy_v1" title="策略ID" />
              </div>
              <div>
                <label style={labelStyle}>显示名称</label>
                <input type="text" value={editForm.display_name}
                  onChange={e => setEditForm(f => ({ ...f, display_name: e.target.value }))}
                  style={inputStyle} placeholder="我的策略" title="显示名称" />
              </div>
              <div>
                <label style={labelStyle}>类型</label>
                <select value={editForm.strategy_type}
                  onChange={e => setEditForm(f => ({ ...f, strategy_type: e.target.value }))}
                  style={inputStyle} title="策略类型">
                  <option value="daily">日频</option>
                  <option value="intraday">日内</option>
                </select>
              </div>
              <div>
                <label style={labelStyle}>市场</label>
                <input type="text" value={editForm.market}
                  onChange={e => setEditForm(f => ({ ...f, market: e.target.value }))}
                  style={inputStyle} placeholder="csi300" title="市场" />
              </div>
            </div>
            <div style={{ marginTop: 12 }}>
              <label style={labelStyle}>描述</label>
              <textarea value={editForm.description}
                onChange={e => setEditForm(f => ({ ...f, description: e.target.value }))}
                rows={2} style={{ ...inputStyle, resize: "none" }}
                placeholder="策略功能描述..." title="策略描述" />
            </div>
          </div>

          {/* 左右分栏：参数 + 代码 */}
          <div style={{ display: "grid", gridTemplateColumns: "340px 1fr", flex: 1, minHeight: 0, overflow: "hidden" }}>
            {/* 左侧：参数面板 */}
            <div style={{ background: "#fff", borderRight: "1px solid #e5e7eb", overflowY: "auto", padding: 20 }}>
              <h3 style={{ margin: "0 0 14px", fontSize: 14, fontWeight: 700, color: "#374151" }}>策略参数</h3>
              {getParamSchema().length > 0 ? (
                <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                  {getParamSchema().map(p => (
                    <div key={p.name}>
                      <label style={labelStyle}>
                        {p.description || p.name}
                        <span style={{ color: "#d1d5db", marginLeft: 4 }}>({p.type})</span>
                      </label>
                      {renderParamInput(p)}
                    </div>
                  ))}
                </div>
              ) : Object.keys(editParams).length > 0 ? (
                <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                  {Object.entries(editParams).map(([k, v]) => (
                    <div key={k}>
                      <label style={labelStyle}>{k}</label>
                      <input type={typeof v === "number" ? "number" : "text"}
                        value={v as any}
                        onChange={e => {
                          let val: any = e.target.value;
                          if (typeof v === "number") val = Number(val) || 0;
                          if (typeof v === "boolean") val = val === "true";
                          setEditParams(prev => ({ ...prev, [k]: val }));
                        }}
                        style={inputStyle} title={k} placeholder={k} />
                    </div>
                  ))}
                </div>
              ) : (
                <p style={{ fontSize: 12, color: "#9ca3af" }}>无参数定义。保存策略后可通过LLM分析自动识别参数。</p>
              )}

              {/* LLM分析结果 */}
              {analysisResult && (
                <div style={{ marginTop: 20, paddingTop: 16, borderTop: "1px solid #e5e7eb" }}>
                  <h3 style={{ margin: "0 0 12px", fontSize: 14, fontWeight: 700, color: "#374151" }}>分析结果</h3>
                  <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
                    <span style={{ width: 8, height: 8, borderRadius: "50%", background: analysisResult.syntax_ok ? "#10b981" : "#ef4444" }} />
                    <span style={{ fontSize: 12 }}>{analysisResult.syntax_ok ? "语法正确" : "语法错误"}</span>
                  </div>
                  {analysisResult.syntax_errors.length > 0 && (
                    <div style={{ background: "#fef2f2", borderRadius: 6, padding: 8, marginBottom: 12, fontSize: 12, color: "#b91c1c" }}>
                      {analysisResult.syntax_errors.map((e, i) => (
                        <div key={i}>行{e.line}: {e.message}</div>
                      ))}
                    </div>
                  )}
                  {analysisResult.llm_review && (
                    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                      {analysisResult.llm_review.overall_rating && (
                        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                          <span style={{ fontSize: 12, color: "#6b7280" }}>评级:</span>
                          <span style={{ fontSize: 20, fontWeight: 700, color: RATING_COLORS[analysisResult.llm_review.overall_rating] || "#6b7280" }}>
                            {analysisResult.llm_review.overall_rating}
                          </span>
                        </div>
                      )}
                      {analysisResult.llm_review.summary && (
                        <p style={{ fontSize: 12, color: "#4b5563", background: "#f9fafb", borderRadius: 6, padding: 8 }}>
                          {analysisResult.llm_review.summary}
                        </p>
                      )}
                      {analysisResult.llm_review.syntax_issues && analysisResult.llm_review.syntax_issues.length > 0 && (
                        <div>
                          <div style={{ fontSize: 12, fontWeight: 600, color: "#dc2626", marginBottom: 4 }}>语法问题:</div>
                          {analysisResult.llm_review.syntax_issues.map((s, i) => (
                            <div key={i} style={{ fontSize: 12, color: "#dc2626", background: "#fef2f2", borderRadius: 4, padding: 6, marginBottom: 4 }}>{s}</div>
                          ))}
                        </div>
                      )}
                      {analysisResult.llm_review.logic_issues && analysisResult.llm_review.logic_issues.length > 0 && (
                        <div>
                          <div style={{ fontSize: 12, fontWeight: 600, color: "#d97706", marginBottom: 4 }}>逻辑问题:</div>
                          {analysisResult.llm_review.logic_issues.map((s, i) => (
                            <div key={i} style={{ fontSize: 12, color: "#92400e", background: "#fffbeb", borderRadius: 4, padding: 6, marginBottom: 4 }}>{s}</div>
                          ))}
                        </div>
                      )}
                      {analysisResult.llm_review.suggestions && analysisResult.llm_review.suggestions.length > 0 && (
                        <div>
                          <div style={{ fontSize: 12, fontWeight: 600, color: "#2563eb", marginBottom: 4 }}>改进建议:</div>
                          {analysisResult.llm_review.suggestions.map((s, i) => (
                            <div key={i} style={{ fontSize: 12, color: "#1e40af", background: "#eff6ff", borderRadius: 4, padding: 6, marginBottom: 4 }}>{s}</div>
                          ))}
                        </div>
                      )}
                    </div>
                  )}
                  {analysisResult.params_detected.length > 0 && (
                    <div style={{ marginTop: 12 }}>
                      <div style={{ fontSize: 12, fontWeight: 600, color: "#6b7280", marginBottom: 4 }}>检测到的参数:</div>
                      {analysisResult.params_detected.map((p, i) => (
                        <div key={i} style={{ fontSize: 12, color: "#4b5563", background: "#f9fafb", borderRadius: 4, padding: 6, marginBottom: 4 }}>
                          <strong>{p.name}</strong>: {p.type} = {JSON.stringify(p.default)}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>

            {/* 右侧：代码编辑器 */}
            <div style={{ display: "flex", flexDirection: "column", minWidth: 0, minHeight: 0, overflow: "hidden" }}>
              <div style={{
                display: "flex", alignItems: "center", justifyContent: "space-between",
                padding: "10px 16px", background: "#f9fafb", borderBottom: "1px solid #e5e7eb", flexShrink: 0,
              }}>
                <span style={{ fontSize: 12, fontWeight: 600, color: "#6b7280" }}>策略源代码 (Python)</span>
                <button onClick={handleAnalyze} disabled={analyzing || !editCode}
                  style={{ ...btnPrimary, background: "#7c3aed", padding: "6px 14px", fontSize: 12, opacity: (analyzing || !editCode) ? 0.5 : 1 }}>
                  {analyzing ? "分析中..." : "LLM 分析"}
                </button>
              </div>
              <div style={{ flex: 1, minHeight: 0, position: "relative" }}>
                {/* @ts-ignore */}
                <MonacoEditor
                  height="100%"
                  language="python"
                  theme="vs-dark"
                  value={editCode}
                  onChange={(v: string | undefined) => setEditCode(v || "")}
                  options={{
                    minimap: { enabled: false },
                    fontSize: 13,
                    lineNumbers: "on",
                    scrollBeyondLastLine: false,
                    wordWrap: "on",
                    tabSize: 4,
                    automaticLayout: true,
                  }}
                />
              </div>
            </div>
          </div>

          {/* 弹窗底部 */}
          <div style={{
            background: "#fff", borderTop: "1px solid #e5e7eb", padding: "12px 24px",
            display: "flex", alignItems: "center", justifyContent: "flex-end", gap: 10, flexShrink: 0,
          }}>
            <button onClick={() => setEditOpen(false)} style={btnSecondary}>取消</button>
            <button onClick={handleSave}
              disabled={saving || !editForm.strategy_id || !editForm.display_name}
              style={{ ...btnPrimary, opacity: (saving || !editForm.strategy_id || !editForm.display_name) ? 0.5 : 1 }}>
              {saving ? "保存中..." : editMode === "edit" ? "保存修改" : "创建策略"}
            </button>
          </div>
        </div>
      )}
    </main>
  );
}
