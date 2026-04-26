"use client";

import React, { useEffect, useState, useCallback, useMemo, useRef } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

export type Model = {
  model_id: string;
  model_name: string;
  model_type?: string;
  display_name?: string;
  catalog_source?: string;
  ic?: number;
  annualized_return?: number;
  max_drawdown?: number;
  information_ratio?: number;
  sharpe?: number;
  all_metrics?: Record<string, any>;
  is_sota?: boolean;
  task_run_id?: string;
  loop_id?: number;
  hypothesis_text?: string;
  model_architecture?: string;
  model_formulation?: string;
  description?: string;
  generated_at_utc?: string;
  // 训练诊断
  best_epoch?: number;
  total_epochs?: number;
  convergence_ratio?: number;
  overfit_ratio?: number;
  training_failed?: boolean;
  train_loss_final?: number;
  val_loss_final?: number;
  training_curves?: any;
  // 分析
  model_grade?: string;
  grade_reason?: string;
  training_quality_score?: number;
  analysis_profile?: any;
  // 代码
  code_text?: string;
  source_code_relpath?: string;
  // 反馈
  feedback_observations?: string;
  feedback_evaluation?: string;
  feedback_reason?: string;
  feedback_decision?: boolean;
};

const GRADE_COLORS: Record<string, string> = {
  S: "#7c3aed", A: "#2563eb", B: "#10b981", C: "#f59e0b", D: "#ef4444",
};

function fmtNum(n: number | null | undefined, frac: number = 4) {
  return n != null ? n.toFixed(frac) : "-";
}
function fmtPct(n: number | null | undefined, frac: number = 2) {
  return n != null ? (n * 100).toFixed(frac) + "%" : "-";
}

function getTrainingStatusBadge(m: Model): { label: string; bg: string; color: string } {
  if (m.training_failed) return { label: "失败", bg: "#fee2e2", color: "#dc2626" };
  if (m.convergence_ratio != null && m.convergence_ratio > 0.5)
    return { label: "收敛", bg: "#d1fae5", color: "#059669" };
  if (m.overfit_ratio != null && m.overfit_ratio > 1.5)
    return { label: "过拟合", bg: "#fef3c7", color: "#d97706" };
  if (m.best_epoch != null)
    return { label: "已训练", bg: "#dbeafe", color: "#2563eb" };
  return { label: "-", bg: "#f3f4f6", color: "#9ca3af" };
}

// ---- 简易 CSS 折线图 ----
function MiniLossChart({ curves }: { curves: any }) {
  if (!curves) return null;

  let trainLoss: number[] = [];
  let valLoss: number[] = [];

  if (Array.isArray(curves)) {
    // curves is array of {train_loss, val_loss} or just numbers
    if (typeof curves[0] === "number") {
      trainLoss = curves;
    } else {
      trainLoss = curves.map((c: any) => c.train_loss ?? c.train ?? c).filter((v: any) => typeof v === "number");
      valLoss = curves.map((c: any) => c.val_loss ?? c.val ?? c.valid).filter((v: any) => typeof v === "number");
    }
  } else if (curves.train_loss || curves.val_loss || curves.train || curves.val) {
    trainLoss = curves.train_loss || curves.train || [];
    valLoss = curves.val_loss || curves.val || [];
  }

  if (trainLoss.length === 0 && valLoss.length === 0) return null;

  const allVals = [...trainLoss, ...valLoss].filter(v => isFinite(v));
  if (allVals.length === 0) return null;

  const maxVal = Math.max(...allVals);
  const minVal = Math.min(...allVals);
  const range = maxVal - minVal || 1;

  const W = 200;
  const H = 60;
  const pad = 4;

  function toPoints(data: number[]) {
    if (data.length === 0) return "";
    const step = (W - pad * 2) / Math.max(data.length - 1, 1);
    return data.map((v, i) => {
      const x = pad + i * step;
      const y = H - pad - ((v - minVal) / range) * (H - pad * 2);
      return `${x},${y}`;
    }).join(" ");
  }

  return (
    <div style={{ marginTop: 8 }}>
      <div style={{ fontSize: 10, color: "#6b7280", marginBottom: 4 }}>Loss 曲线</div>
      <svg width={W} height={H} style={{ background: "#f9fafb", borderRadius: 4, border: "1px solid #e5e7eb" }}>
        {trainLoss.length > 1 && (
          <polyline
            points={toPoints(trainLoss)}
            fill="none"
            stroke="#2563eb"
            strokeWidth="1.5"
          />
        )}
        {valLoss.length > 1 && (
          <polyline
            points={toPoints(valLoss)}
            fill="none"
            stroke="#ef4444"
            strokeWidth="1.5"
            strokeDasharray="3,2"
          />
        )}
      </svg>
      <div style={{ display: "flex", gap: 12, fontSize: 9, color: "#9ca3af", marginTop: 2 }}>
        {trainLoss.length > 0 && <span><span style={{ color: "#2563eb" }}>—</span> Train</span>}
        {valLoss.length > 0 && <span><span style={{ color: "#ef4444" }}>- -</span> Val</span>}
      </div>
    </div>
  );
}

export default function ModelList({
  mode = "display",
  selectedModel = null,
  onSelectModel,
}: {
  mode?: "display" | "selection";
  selectedModel?: string | null;
  onSelectModel?: (modelId: string) => void;
}) {
  const [models, setModels] = useState<Model[]>([]);
  const loadModelsRequestRef = useRef(0);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [modelTypeFilter, setModelTypeFilter] = useState("");
  const [gradeFilter, setGradeFilter] = useState("");
  const [sotaOnly, setSotaOnly] = useState(false);
  const [page, setPage] = useState(1);
  const [pageSize] = useState(50);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [codeExpanded, setCodeExpanded] = useState<Set<string>>(new Set());

  // 排序
  const [sortField, setSortField] = useState<string | null>(null);
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc");

  // 批量选择
  const [selectedModels, setSelectedModels] = useState<Set<string>>(new Set());

  // 批量分析
  const [batchLoading, setBatchLoading] = useState(false);
  const [batchProgress, setBatchProgress] = useState<{ current: number; total: number; model_name?: string } | null>(null);
  const [batchResult, setBatchResult] = useState<{ total?: number; analyzed?: number; errors?: string[] } | null>(null);

  // 模型类型列表（从数据中动态获取）
  const modelTypes = useMemo(() => {
    const types = new Set(models.map(m => m.model_type).filter(Boolean));
    return Array.from(types).sort();
  }, [models]);

  const isSelectionMode = mode === "selection";

  const loadModels = useCallback(async (queryOverride?: string) => {
    const requestId = ++loadModelsRequestRef.current;
    setLoading(true);
    setError(null);
    try {
      const effectiveSearch = typeof queryOverride === "string" ? queryOverride : search;
      const params = new URLSearchParams({
        limit: String(pageSize),
        offset: String((page - 1) * pageSize),
      });
      if (effectiveSearch) params.set("search", effectiveSearch);
      if (modelTypeFilter) params.set("model_type", modelTypeFilter);
      if (gradeFilter) params.set("grade", gradeFilter);
      if (sotaOnly) params.set("sota_only", "true");
      if (sortField) {
        params.set("sort_field", sortField);
        params.set("sort_order", sortOrder);
      }
      const res = await fetch(`${API}/quantevolver/models?${params.toString()}`);
      const data = await res.json();
      if (requestId !== loadModelsRequestRef.current) return;
      setModels(data.items || []);
      setTotal(data.total || 0);
    } catch (e: any) {
      if (requestId !== loadModelsRequestRef.current) return;
      setError(e?.message || "加载失败");
    } finally {
      if (requestId === loadModelsRequestRef.current) setLoading(false);
    }
  }, [search, modelTypeFilter, gradeFilter, sotaOnly, page, pageSize, sortField, sortOrder]);

  useEffect(() => { loadModels(); }, [loadModels]);

  useEffect(() => { setPage(1); }, [search, modelTypeFilter, gradeFilter, sotaOnly, sortField, sortOrder]);

  // ---- 排序 ----
  function handleSort(field: string) {
    if (sortField === field) {
      if (sortOrder === "desc") setSortOrder("asc");
      else { setSortField(null); setSortOrder("desc"); }
    } else {
      setSortField(field);
      setSortOrder("desc");
    }
  }

  function getSortIndicator(field: string) {
    if (sortField !== field) return "";
    return sortOrder === "desc" ? " ▼" : " ▲";
  }

  // ---- 选择 ----
  function toggleSelect(id: string) {
    setSelectedModels(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  }

  function selectAllPage() {
    const ids = models.map(m => m.model_id);
    setSelectedModels(new Set(ids));
  }

  function clearSelection() {
    setSelectedModels(new Set());
  }

  // ---- 展开/折叠 ----
  function toggleExpand(id: string) {
    setExpandedId(prev => prev === id ? null : id);
  }

  function toggleCode(id: string) {
    setCodeExpanded(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  }

  async function deleteModel(modelId: string, displayName: string) {
    if (!confirm(
      `确定要删除模型「${displayName}」吗？\n\n` +
      `将同时删除：演进循环记录、因子实验指标、实验模型关联等所有关联数据。\n\n此操作不可撤销！`
    )) return;
    try {
      const res = await fetch(`${API}/quantevolver/models/${encodeURIComponent(modelId)}`, { method: "DELETE" });
      const data = await res.json();
      if (!res.ok) {
        alert("删除失败: " + (data.detail || res.statusText));
        return;
      }
      alert(`模型「${displayName}」已删除`);
      loadModels();
    } catch (e: any) {
      alert("删除失败: " + (e?.message || "网络错误"));
    }
  }

  // ---- 批量分析（SSE 流式） ----
  async function batchAnalyze() {
    const count = selectedModels.size;
    if (count === 0) { alert("请先选择要分析的模型"); return; }
    if (!confirm(`确定要分析选中的 ${count} 个模型吗？`)) return;

    setBatchLoading(true);
    setBatchProgress(null);
    setBatchResult(null);

    const handleBeforeUnload = (e: BeforeUnloadEvent) => { e.preventDefault(); e.returnValue = ''; };
    window.addEventListener('beforeunload', handleBeforeUnload);

    try {
      const body = { use_llm: true, model_ids: Array.from(selectedModels) };
      const res = await fetch(`${API}/quantevolver/model-analyst/batch-analyze-stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      const reader = res.body?.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (reader) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          try {
            const event = JSON.parse(line.slice(6));
            if (event.type === "progress") {
              setBatchProgress({ current: event.current, total: event.total, model_name: event.model_name });
            } else if (event.type === "done") {
              setBatchResult({ total: event.total, analyzed: event.analyzed, errors: event.errors });
              loadModels();
            }
          } catch {}
        }
      }
    } catch (e: any) {
      alert("批量分析失败: " + (e?.message || ""));
    }

    window.removeEventListener('beforeunload', handleBeforeUnload);
    setBatchLoading(false);
    setBatchProgress(null);
  }

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  const thStyle: React.CSSProperties = {
    padding: "8px 10px", fontWeight: 600, color: "#4b5563", background: "#f9fafb",
    whiteSpace: "nowrap", cursor: "pointer", userSelect: "none",
  };
  const tdStyle: React.CSSProperties = { padding: "8px 10px" };

  return (
    <div style={!isSelectionMode ? { padding: 24 } : {}}>
      {/* 标题横幅 */}
      {!isSelectionMode && (
        <section
          style={{
            background: "linear-gradient(135deg, #06b6d4 0%, #2563eb 100%)",
            borderRadius: 16, padding: 20, color: "#fff", marginBottom: 16,
          }}
        >
          <h1 style={{ margin: 0, fontSize: 24 }}>模型库</h1>
          <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
            浏览RDAgent训练的模型，查看性能指标、训练诊断和LLM分析
          </p>
        </section>
      )}

      {/* 筛选栏 */}
      <section style={{ background: "#fff", borderRadius: 12, padding: 16, marginBottom: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
        <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
          <input
            data-testid="qe-model-search"
            placeholder="搜索模型名称/类型..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            onKeyDown={e => { if (e.key === "Enter") void loadModels(e.currentTarget.value); }}
            style={{ padding: "6px 10px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db", width: 200 }}
          />

          <select
            value={modelTypeFilter}
            onChange={e => setModelTypeFilter(e.target.value)}
            title="模型类型筛选"
            style={{ padding: "6px 10px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db" }}
          >
            <option value="">全部类型</option>
            {modelTypes.map(t => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>

          <select
            value={gradeFilter}
            onChange={e => setGradeFilter(e.target.value)}
            title="评级筛选"
            style={{ padding: "6px 10px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db" }}
          >
            <option value="">全部评级</option>
            {["S", "A", "B", "C", "D"].map(g => (
              <option key={g} value={g}>{g}</option>
            ))}
          </select>

          <label style={{ display: "flex", gap: 4, alignItems: "center", fontSize: 12, cursor: "pointer" }}>
            <input type="checkbox" checked={sotaOnly} onChange={e => setSotaOnly(e.target.checked)} />
            仅SOTA
          </label>

          <button onClick={() => loadModels()} disabled={loading}
            style={{ padding: "6px 12px", fontSize: 12, cursor: "pointer", borderRadius: 6, border: "1px solid #d1d5db", background: "#fff" }}>
            {loading ? "加载中..." : "刷新"}
          </button>

          {!isSelectionMode && (
            <>
              <button
                onClick={() => batchAnalyze()}
                disabled={batchLoading || selectedModels.size === 0}
                style={{
                  padding: "6px 14px", fontSize: 12,
                  cursor: (batchLoading || selectedModels.size === 0) ? "not-allowed" : "pointer",
                  borderRadius: 6, border: "none", background: "#2563eb", color: "#fff", fontWeight: 600,
                  opacity: (batchLoading || selectedModels.size === 0) ? 0.5 : 1,
                }}
              >
                {batchLoading ? "分析中..." : `批量分析(${selectedModels.size})`}
              </button>
            </>
          )}

          <button onClick={selectAllPage}
            style={{ padding: "4px 10px", fontSize: 11, cursor: "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: "#fff" }}>
            全选页
          </button>
          <button onClick={clearSelection}
            style={{ padding: "4px 10px", fontSize: 11, cursor: "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: "#fff" }}>
            清空
          </button>

          <span style={{ fontSize: 12, color: "#9ca3af" }}>
            共 {total} 条  已选 {selectedModels.size}  第 {page}/{totalPages} 页
          </span>
        </div>

        {/* 批量分析进度条 */}
        {batchProgress && (
          <div style={{ marginTop: 8, padding: 10, borderRadius: 6, fontSize: 12, background: "#dbeafe", color: "#1e40af" }}>
            <div style={{ marginBottom: 4 }}>
              <strong>批量分析进行中：</strong> {batchProgress.current} / {batchProgress.total}
              {batchProgress.model_name && <span style={{ marginLeft: 8, color: "#3b82f6" }}>当前: {batchProgress.model_name}</span>}
            </div>
            <div style={{ width: "100%", height: 8, background: "#e5e7eb", borderRadius: 4, overflow: "hidden" }}>
              <div style={{ width: `${(batchProgress.current / batchProgress.total) * 100}%`, height: "100%", background: "#2563eb", transition: "width 0.3s" }} />
            </div>
          </div>
        )}

        {/* 批量分析结果 */}
        {batchResult && (
          <div style={{
            marginTop: 8, padding: 10, borderRadius: 6, fontSize: 12,
            background: (batchResult.errors?.length || 0) > 0 ? "#fef3c7" : "#d1fae5",
            color: (batchResult.errors?.length || 0) > 0 ? "#92400e" : "#065f46",
          }}>
            <strong>批量分析完成：</strong>
            共 {batchResult.total} 个模型，成功分析 {batchResult.analyzed} 个
            {(batchResult.errors?.length || 0) > 0 && (
              <span>，{batchResult.errors!.length} 个失败</span>
            )}
          </div>
        )}

        {error && <div style={{ marginTop: 8, padding: 8, background: "#fee2e2", borderRadius: 6, fontSize: 12 }}>{error}</div>}
      </section>

      {/* 表格 */}
      <section style={{ background: "#fff", borderRadius: 12, boxShadow: "0 1px 3px rgba(0,0,0,0.08)", overflow: "hidden" }}>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>
                <th style={{ ...thStyle, width: 32, cursor: "default" }}>
                  <input
                    type="checkbox"
                    checked={models.length > 0 && models.every(m => selectedModels.has(m.model_id))}
                    onChange={e => e.target.checked ? selectAllPage() : clearSelection()}
                    style={{ accentColor: "#7c3aed" }}
                  />
                </th>
                <th style={{ ...thStyle, maxWidth: 180 }} onClick={() => handleSort("model_name")}>
                  模型名称{getSortIndicator("model_name")}
                </th>
                <th style={{ ...thStyle, cursor: "default" }}>来源</th>
                <th style={{ ...thStyle, cursor: "default" }}>类型</th>
                <th style={thStyle} onClick={() => handleSort("loop_id")}>
                  Loop{getSortIndicator("loop_id")}
                </th>
                <th style={thStyle} onClick={() => handleSort("model_grade")}>
                  评级{getSortIndicator("model_grade")}
                </th>
                <th style={thStyle} onClick={() => handleSort("ic")}>
                  IC{getSortIndicator("ic")}
                </th>
                <th style={thStyle} onClick={() => handleSort("annualized_return")}>
                  年化收益{getSortIndicator("annualized_return")}
                </th>
                <th style={thStyle} onClick={() => handleSort("max_drawdown")}>
                  最大回撤{getSortIndicator("max_drawdown")}
                </th>
                <th style={thStyle} onClick={() => handleSort("information_ratio")}>
                  IR{getSortIndicator("information_ratio")}
                </th>
                <th style={{ ...thStyle, cursor: "default" }}>训练状态</th>
                <th style={{ ...thStyle, cursor: "default" }}>说明</th>
              </tr>
            </thead>
            <tbody>
              {models.map(m => {
                const isExpanded = expandedId === m.model_id;
                const isSelected = isSelectionMode
                  ? selectedModel === m.model_id
                  : selectedModels.has(m.model_id);
                const trainStatus = getTrainingStatusBadge(m);

                return (
                  <React.Fragment key={m.model_id}>
                    {/* 主行 */}
                    <tr
                      data-testid={`qe-model-row-${m.model_id}`}
                      style={{
                        borderBottom: isExpanded ? "none" : "1px solid #f3f4f6",
                        background: isSelected ? "#eff6ff" : "transparent",
                        cursor: isSelectionMode ? "pointer" : "default",
                        transition: "background 0.15s",
                      }}
                      onClick={() => {
                        if (isSelectionMode && onSelectModel) {
                          onSelectModel(m.model_id);
                        }
                      }}
                    >
                      {/* 选择框 */}
                      <td style={tdStyle}>
                        {isSelectionMode ? (
                          <input
                            data-testid={`qe-model-radio-${m.model_id}`}
                            type="radio"
                            checked={selectedModel === m.model_id}
                            onChange={() => onSelectModel && onSelectModel(m.model_id)}
                            onClick={e => e.stopPropagation()}
                            style={{ accentColor: "#3b82f6", cursor: "pointer" }}
                          />
                        ) : (
                          <input
                            type="checkbox"
                            checked={selectedModels.has(m.model_id)}
                            onChange={() => toggleSelect(m.model_id)}
                            style={{ accentColor: "#7c3aed", cursor: "pointer" }}
                          />
                        )}
                      </td>

                      {/* 模型名称 */}
                      <td style={{ ...tdStyle, maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        <span style={{ fontFamily: "monospace", fontWeight: 600, fontSize: 11 }} title={m.model_name}>
                          {m.display_name || m.model_name || m.model_id}
                        </span>
                        {m.is_sota && (
                          <span style={{
                            marginLeft: 6, fontSize: 9, background: "#fef3c7", color: "#92400e",
                            padding: "1px 5px", borderRadius: 4, fontWeight: 600, verticalAlign: "middle",
                          }}>SOTA</span>
                        )}
                      </td>

                      {/* 来源 */}
                      <td style={tdStyle}>
                        <span style={{
                          fontSize: 10, padding: "2px 6px", borderRadius: 4,
                          background: m.catalog_source === "rdagent_task_sync" ? "#ede9fe" : "#f3f4f6",
                          color: m.catalog_source === "rdagent_task_sync" ? "#7c3aed" : "#6b7280",
                          whiteSpace: "nowrap",
                        }}>
                          {m.catalog_source === "rdagent_task_sync" ? "RDAgent" : (m.catalog_source || "-")}
                        </span>
                      </td>

                      {/* 类型 */}
                      <td style={{ ...tdStyle, fontSize: 11, color: "#6b7280" }}>
                        {m.model_type || "-"}
                      </td>

                      {/* Loop */}
                      <td style={{ ...tdStyle, fontSize: 11, color: "#6b7280" }}>
                        {m.loop_id != null ? `#${m.loop_id}` : "-"}
                      </td>

                      {/* 评级 */}
                      <td style={tdStyle}>
                        {m.model_grade ? (
                          <span style={{
                            padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 700,
                            color: "#fff",
                            background: GRADE_COLORS[m.model_grade] || "#9ca3af",
                          }}>
                            {m.model_grade}
                          </span>
                        ) : (
                          <span style={{ color: "#d1d5db", fontSize: 11 }}>-</span>
                        )}
                      </td>

                      {/* IC */}
                      <td style={{
                        ...tdStyle, fontFamily: "monospace", fontSize: 11, fontWeight: 600,
                        color: m.ic != null ? (m.ic > 0 ? "#7c3aed" : "#ef4444") : "#d1d5db",
                      }}>
                        {fmtNum(m.ic)}
                      </td>

                      {/* 年化收益 */}
                      <td style={{
                        ...tdStyle, fontFamily: "monospace", fontSize: 11, fontWeight: 600,
                        color: m.annualized_return != null ? (m.annualized_return > 0 ? "#059669" : "#ef4444") : "#d1d5db",
                      }}>
                        {fmtPct(m.annualized_return)}
                      </td>

                      {/* 最大回撤 */}
                      <td style={{
                        ...tdStyle, fontFamily: "monospace", fontSize: 11, fontWeight: 600,
                        color: m.max_drawdown != null ? "#ef4444" : "#d1d5db",
                      }}>
                        {fmtPct(m.max_drawdown)}
                      </td>

                      {/* IR */}
                      <td style={{
                        ...tdStyle, fontFamily: "monospace", fontSize: 11, fontWeight: 600,
                        color: m.information_ratio != null ? "#2563eb" : "#d1d5db",
                      }}>
                        {fmtNum(m.information_ratio, 3)}
                      </td>

                      {/* 训练状态 */}
                      <td style={tdStyle}>
                        <span style={{
                          fontSize: 10, padding: "2px 6px", borderRadius: 4,
                          background: trainStatus.bg, color: trainStatus.color,
                          whiteSpace: "nowrap",
                        }}>
                          {trainStatus.label}
                        </span>
                      </td>

                      {/* 展开/收起 + 删除 */}
                      <td style={tdStyle}>
                        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                        <span
                          onClick={e => { e.stopPropagation(); toggleExpand(m.model_id); }}
                          style={{
                            color: "#3b82f6", cursor: "pointer", fontSize: 11,
                            borderBottom: "1px dashed #3b82f6", userSelect: "none",
                          }}
                        >
                          {isExpanded ? "收起" : "展开"}
                        </span>
                        {!isSelectionMode && (
                          <span
                            onClick={(e) => { e.stopPropagation(); deleteModel(m.model_id, m.display_name || m.model_name || m.model_id); }}
                            title="删除此模型及所有关联数据"
                            style={{
                              color: "#dc2626", cursor: "pointer", fontSize: 11,
                              opacity: 0.6, userSelect: "none",
                            }}
                            onMouseEnter={e => (e.currentTarget.style.opacity = "1")}
                            onMouseLeave={e => (e.currentTarget.style.opacity = "0.6")}
                          >删除</span>
                        )}
                        </div>
                      </td>
                    </tr>

                    {/* 展开详情行 */}
                    {isExpanded && (
                      <tr style={{ background: isSelectionMode ? "#eff6ff" : "#faf5ff" }}>
                        <td colSpan={12} style={{ padding: 0 }}>
                          <div style={{
                            borderLeft: `3px solid ${isSelectionMode ? "#3b82f6" : "#7c3aed"}`,
                            padding: "16px 20px",
                            fontSize: 12, lineHeight: 1.7,
                          }}>
                            {/* 模型假设 */}
                            {m.hypothesis_text && (
                              <div style={{ marginBottom: 12 }}>
                                <strong style={{ color: "#374151" }}>模型假设</strong>
                                <div style={{
                                  marginTop: 4, color: "#6b7280", whiteSpace: "pre-wrap",
                                  background: "#f9fafb", padding: "8px 12px", borderRadius: 6,
                                }}>
                                  {m.hypothesis_text}
                                </div>
                              </div>
                            )}

                            {/* 模型架构 */}
                            {m.model_architecture && (
                              <div style={{ marginBottom: 12, borderTop: "1px dashed #e5e7eb", paddingTop: 12 }}>
                                <strong style={{ color: "#374151" }}>模型架构</strong>
                                <div style={{ marginTop: 4, color: "#6b7280" }}>
                                  {m.model_architecture}
                                </div>
                                {m.model_formulation && (
                                  <div style={{
                                    marginTop: 4, color: "#6b7280", fontSize: 11,
                                    background: "#fffbeb", padding: "6px 10px", borderRadius: 4,
                                    fontFamily: "monospace",
                                  }}>
                                    {m.model_formulation}
                                  </div>
                                )}
                              </div>
                            )}

                            {/* 训练诊断 */}
                            {(m.best_epoch != null || m.training_failed) && (
                              <div style={{ marginBottom: 12, borderTop: "1px dashed #e5e7eb", paddingTop: 12 }}>
                                <strong style={{ color: "#374151" }}>训练诊断</strong>
                                <div style={{
                                  display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(130px, 1fr))",
                                  gap: 8, marginTop: 8,
                                }}>
                                  <DiagBox
                                    label="最优Epoch"
                                    value={m.best_epoch != null && m.total_epochs != null
                                      ? `${m.best_epoch}/${m.total_epochs}`
                                      : m.best_epoch != null ? String(m.best_epoch) : "-"}
                                  />
                                  <DiagBox
                                    label="收敛比例"
                                    value={m.convergence_ratio != null ? m.convergence_ratio.toFixed(3) : "-"}
                                    color={m.convergence_ratio != null && m.convergence_ratio > 0.5 ? "#059669" : undefined}
                                  />
                                  <DiagBox
                                    label="过拟合度"
                                    value={m.overfit_ratio != null ? m.overfit_ratio.toFixed(3) : "-"}
                                    color={m.overfit_ratio != null && m.overfit_ratio > 1.5 ? "#d97706" : undefined}
                                  />
                                  <DiagBox
                                    label="训练状态"
                                    value={trainStatus.label}
                                    color={trainStatus.color}
                                  />
                                  <DiagBox
                                    label="Train Loss"
                                    value={m.train_loss_final != null ? m.train_loss_final.toFixed(4) : "-"}
                                  />
                                  <DiagBox
                                    label="Val Loss"
                                    value={m.val_loss_final != null ? m.val_loss_final.toFixed(4) : "-"}
                                  />
                                </div>

                                {/* Loss 曲线 */}
                                <MiniLossChart curves={m.training_curves} />
                              </div>
                            )}

                            {/* LLM 分析 */}
                            {(m.model_grade || m.analysis_profile || m.description) && (
                              <div style={{ marginBottom: 12, borderTop: "1px dashed #e5e7eb", paddingTop: 12 }}>
                                <strong style={{ color: "#374151" }}>LLM 分析</strong>
                                <div style={{ marginTop: 6, display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
                                  {m.model_grade && (
                                    <span>
                                      评级：
                                      <span style={{
                                        padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 700,
                                        color: "#fff", background: GRADE_COLORS[m.model_grade] || "#9ca3af",
                                      }}>
                                        {m.model_grade}
                                      </span>
                                    </span>
                                  )}
                                  {m.training_quality_score != null && (
                                    <span>训练质量：<strong>{m.training_quality_score.toFixed(1)}</strong></span>
                                  )}
                                </div>
                                {m.grade_reason && (
                                  <div style={{ marginTop: 4, color: "#6b7280", fontSize: 11 }}>
                                    评级原因：{m.grade_reason}
                                  </div>
                                )}
                                {m.description && (
                                  <div style={{
                                    marginTop: 6, color: "#6b7280", whiteSpace: "pre-wrap",
                                    background: "#f9fafb", padding: "8px 12px", borderRadius: 6,
                                    fontSize: 11, lineHeight: 1.6,
                                  }}>
                                    {m.description}
                                  </div>
                                )}
                                {m.analysis_profile && typeof m.analysis_profile === "object" && (
                                  <details style={{ marginTop: 6 }}>
                                    <summary style={{ fontSize: 11, color: "#7c3aed", cursor: "pointer" }}>
                                      查看完整分析 JSON
                                    </summary>
                                    <pre style={{
                                      marginTop: 4, padding: 10, borderRadius: 6, fontSize: 10,
                                      background: "#1e293b", color: "#e2e8f0", overflow: "auto",
                                      maxHeight: 300, lineHeight: 1.4,
                                    }}>
                                      {JSON.stringify(m.analysis_profile, null, 2)}
                                    </pre>
                                  </details>
                                )}
                              </div>
                            )}

                            {/* 模型代码 */}
                            {m.code_text && (
                              <div style={{ marginBottom: 12, borderTop: "1px dashed #e5e7eb", paddingTop: 12 }}>
                                <strong style={{ color: "#374151" }}>模型代码</strong>
                                {m.source_code_relpath && (
                                  <span style={{ fontSize: 10, color: "#9ca3af", marginLeft: 8 }}>
                                    {m.source_code_relpath}
                                  </span>
                                )}
                                <div style={{ marginTop: 4 }}>
                                  <span
                                    onClick={() => toggleCode(m.model_id)}
                                    style={{ fontSize: 11, color: "#7c3aed", cursor: "pointer", borderBottom: "1px dashed #7c3aed" }}
                                  >
                                    {codeExpanded.has(m.model_id) ? "收起代码" : "展开代码"}
                                  </span>
                                  {codeExpanded.has(m.model_id) && (
                                    <pre style={{
                                      marginTop: 6, padding: 12, borderRadius: 6, fontSize: 11,
                                      background: "#1e293b", color: "#e2e8f0", overflow: "auto",
                                      maxHeight: 400, lineHeight: 1.4, whiteSpace: "pre-wrap",
                                    }}>
                                      {m.code_text}
                                    </pre>
                                  )}
                                </div>
                              </div>
                            )}

                            {/* 反馈评价 */}
                            {(m.feedback_observations || m.feedback_evaluation || m.feedback_reason) && (
                              <div style={{ marginBottom: 12, borderTop: "1px dashed #e5e7eb", paddingTop: 12 }}>
                                <strong style={{ color: "#374151" }}>反馈评价</strong>
                                {m.feedback_decision != null && (
                                  <span style={{
                                    marginLeft: 8, fontSize: 10, padding: "2px 6px", borderRadius: 4,
                                    background: m.feedback_decision ? "#d1fae5" : "#fee2e2",
                                    color: m.feedback_decision ? "#059669" : "#dc2626",
                                  }}>
                                    {m.feedback_decision ? "ACCEPT" : "REJECT"}
                                  </span>
                                )}
                                {m.feedback_observations && (
                                  <div style={{ marginTop: 4, fontSize: 11, color: "#6b7280" }}>
                                    <span style={{ fontWeight: 600 }}>观察：</span>{m.feedback_observations}
                                  </div>
                                )}
                                {m.feedback_evaluation && (
                                  <div style={{ marginTop: 2, fontSize: 11, color: "#6b7280" }}>
                                    <span style={{ fontWeight: 600 }}>评估：</span>{m.feedback_evaluation}
                                  </div>
                                )}
                                {m.feedback_reason && (
                                  <div style={{ marginTop: 2, fontSize: 11, color: "#6b7280" }}>
                                    <span style={{ fontWeight: 600 }}>原因：</span>{m.feedback_reason}
                                  </div>
                                )}
                              </div>
                            )}

                            {/* 元信息 */}
                            <div style={{ borderTop: "1px dashed #e5e7eb", paddingTop: 12, fontSize: 11, color: "#9ca3af" }}>
                              <div>Model ID: <span style={{ fontFamily: "monospace" }}>{m.model_id}</span></div>
                              <div>
                                Task: <span style={{ fontFamily: "monospace" }}>{m.task_run_id || "-"}</span>
                                {" "}Loop: #{m.loop_id ?? "-"}
                              </div>
                              {m.generated_at_utc && <div>生成时间: {m.generated_at_utc}</div>}
                            </div>

                            {/* 收起按钮 */}
                            <div style={{ marginTop: 12, textAlign: "right" }}>
                              <button
                                onClick={() => toggleExpand(m.model_id)}
                                style={{
                                  padding: "4px 16px", fontSize: 11, borderRadius: 4,
                                  border: "1px solid #d1d5db", background: "#fff", cursor: "pointer",
                                  color: "#6b7280",
                                }}
                              >
                                收起
                              </button>
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>

        {!loading && models.length === 0 && (
          <div style={{ textAlign: "center", padding: 60, color: "#9ca3af" }}>
            暂无模型数据。请先同步RDAgent任务或等待任务完成。
          </div>
        )}
      </section>

      {/* 分页 */}
      {total > pageSize && (
        <div style={{
          display: "flex", justifyContent: "center", alignItems: "center", gap: 8,
          marginTop: 16, paddingTop: 12, borderTop: "1px solid #f3f4f6",
        }}>
          <PaginationBtn label="首页" disabled={page <= 1} onClick={() => setPage(1)} />
          <PaginationBtn label="上一页" disabled={page <= 1} onClick={() => setPage(p => Math.max(1, p - 1))} />
          <span style={{ fontSize: 12, color: "#6b7280", minWidth: 80, textAlign: "center" }}>
            {page} / {totalPages}
          </span>
          <PaginationBtn label="下一页" disabled={page >= totalPages} onClick={() => setPage(p => Math.min(totalPages, p + 1))} />
          <PaginationBtn label="末页" disabled={page >= totalPages} onClick={() => setPage(totalPages)} />
        </div>
      )}
    </div>
  );
}

// ---- 辅助组件 ----

function DiagBox({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={{
      textAlign: "center", padding: "8px 6px",
      background: "#f0fdf4", borderRadius: 6, border: "1px solid #e5e7eb",
    }}>
      <div style={{ fontSize: 14, fontWeight: 700, color: color || "#374151" }}>{value}</div>
      <div style={{ fontSize: 10, color: "#9ca3af", marginTop: 2 }}>{label}</div>
    </div>
  );
}

function PaginationBtn({ label, disabled, onClick }: { label: string; disabled: boolean; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        padding: "4px 8px", borderRadius: 4, border: "1px solid #e5e7eb",
        background: disabled ? "#f3f4f6" : "#fff",
        cursor: disabled ? "not-allowed" : "pointer",
        fontSize: 11, color: disabled ? "#d1d5db" : "#374151",
      }}
    >
      {label}
    </button>
  );
}
