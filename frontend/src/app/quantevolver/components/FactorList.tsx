"use client";

import React, { useEffect, useState, useCallback } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type Factor = {
  factor_name: string;
  source: string;
  expression?: string;
  ic?: number;
  sharpe?: number;
  annualized_return?: number;
  is_sota_factor?: boolean;
  catalog_source?: string;
  description_cn?: string;
  generated_at_utc?: string;
};

type Classification = {
  id: number;
  factor_name: string;
  factor_source: string;
  category: string;
  grade: string;
  grade_reason?: string;
  classification_reason?: string;
  ic_value?: number;
  sharpe_value?: number;
  ann_ret_value?: number;
  description?: string;
  factor_dimension?: string;
  analyzed_at?: string;
};

export type MergedFactor = {
  factor_name: string;
  source: string;
  ic?: number;
  sharpe?: number;
  annualized_return?: number;
  is_sota_factor?: boolean;
  description_cn?: string;
  category?: string;
  grade?: string;
  grade_reason?: string;
  classification_reason?: string;
  factor_dimension?: string;
  description?: string;
  classification_id?: number;
};

const GRADE_COLORS: Record<string, string> = {
  S: "#7c3aed", A: "#2563eb", B: "#10b981", C: "#f59e0b", D: "#ef4444",
};

const CATEGORY_NAMES: Record<string, string> = {
  MOM: "动量", VOL: "波动率", LIQ: "流动性", VAL: "价值",
  QUAL: "质量", CORR: "相关性", TECH: "技术指标", SIZE: "规模",
  STAT: "统计", MF: "资金流", CHIP: "筹码", ML: "机器学习",
};

const DIMENSION_NAMES: Record<string, { label: string; color: string; bg: string; desc: string }> = {
  cross_sectional: { label: "截面", color: "#2563eb", bg: "#dbeafe", desc: "在同一时间点对不同股票进行横向比较排名" },
  time_series: { label: "时序", color: "#059669", bg: "#d1fae5", desc: "对同一股票在不同时间点进行纵向分析" },
};

type FactorDetail = {
  name: string;
  expression?: string;
  code_text?: string;
  source_task_id?: string;
  source_loop_tag?: string;
  first_sota_task_id?: string;
  source_code_origin?: string;
  source_code_relpath?: string;
  description_cn?: string;
  performance_metrics?: Record<string, any>;
};

type ExpMetricRow = {
  experiment_id: string;
  experiment_name?: string;
  ic?: number;
  icir?: number;
  ann_return_no_cost?: number;
  max_drawdown_no_cost?: number;
  daily_win_rate?: number;
  stock_win_rate?: number;
  avg_profit_pct?: number;
  avg_loss_pct?: number;
  profit_loss_ratio?: number;
  sharpe_ratio?: number;
  model_id?: string;
  other_factors?: string[];
  collected_at?: string;
  total_trades?: number;
  winning_trades?: number;
  losing_trades?: number;
  max_single_profit_pct?: number;
  max_single_loss_pct?: number;
  max_consecutive_win?: number;
  max_consecutive_loss?: number;
  weekly_win_rate?: number;
  avg_turnover?: number;
  total_trading_days?: number;
  calmar_ratio?: number;
};

type ExpMetricsSummary = {
  experiment_count: number;
  avg_ic?: number;
  best_ic?: number;
  worst_ic?: number;
  avg_ann_return?: number;
  avg_daily_win_rate?: number;
  avg_sharpe?: number;
  avg_stock_win_rate?: number;
  avg_profit_loss_ratio?: number;
};

type FactorExpMetrics = {
  metrics: ExpMetricRow[];
  summary: ExpMetricsSummary;
  total: number;
};

export default function FactorList({
  mode = "display",
  selectedFactors = new Set(),
  onFactorSelect,
}: {
  mode?: "display" | "selection";
  selectedFactors?: Set<string>;
  onFactorSelect?: (selected: Set<string>) => void;
}) {
  const [factors, setFactors] = useState<Factor[]>([]);
  const [classifications, setClassifications] = useState<Classification[]>([]);
  const [mergedFactors, setMergedFactors] = useState<MergedFactor[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sourceFilter, setSourceFilter] = useState("");
  const [search, setSearch] = useState("");
  const [categoryFilter, setCategoryFilter] = useState("");
  const [gradeFilter, setGradeFilter] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize] = useState(50);
  const [showAlpha, setShowAlpha] = useState(false);
  const [batchLoading, setBatchLoading] = useState(false);
  const [batchResult, setBatchResult] = useState<{ total?: number; analyzed?: number; errors?: string[] } | null>(null);
  const [expandedDescriptions, setExpandedDescriptions] = useState<Set<string>>(new Set());
  const [localSelectedFactors, setLocalSelectedFactors] = useState<Set<string>>(selectedFactors);
  const [factorDetails, setFactorDetails] = useState<Record<string, FactorDetail>>({});
  const [detailLoading, setDetailLoading] = useState<Set<string>>(new Set());
  const [codeExpanded, setCodeExpanded] = useState<Set<string>>(new Set());
  const [factorExpMetrics, setFactorExpMetrics] = useState<Record<string, FactorExpMetrics>>({});
  const [expMetricsLoading, setExpMetricsLoading] = useState<Set<string>>(new Set());

  // 排序状态
  const [sortField, setSortField] = useState<string | null>(null);
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc");

  const actualSelectedFactors = mode === "selection" ? selectedFactors : localSelectedFactors;

  function toggleSelect(key: string) {
    const next = new Set(actualSelectedFactors);
    if (next.has(key)) next.delete(key); else next.add(key);
    
    if (mode === "selection" && onFactorSelect) {
      onFactorSelect(next);
    } else {
      setLocalSelectedFactors(next);
    }
  }

  function selectAll() {
    const keys = mergedFactors.map(f => `${f.factor_name}||${f.source}`);
    const next = new Set(keys);
    if (mode === "selection" && onFactorSelect) {
      onFactorSelect(next);
    } else {
      setLocalSelectedFactors(next);
    }
  }

  function clearSelection() {
    const next = new Set<string>();
    if (mode === "selection" && onFactorSelect) {
      onFactorSelect(next);
    } else {
      setLocalSelectedFactors(next);
    }
  }

  function toggleDescription(key: string, factorName?: string, source?: string) {
    setExpandedDescriptions(prev => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
        if (factorName && source && !factorDetails[key] && !detailLoading.has(key)) {
          loadFactorDetail(key, factorName, source);
        }
      }
      return next;
    });
  }

  async function loadFactorDetail(key: string, factorName: string, source: string) {
    setDetailLoading(prev => new Set(prev).add(key));
    try {
      const params = new URLSearchParams({ source });
      const res = await fetch(`${API}/rdagent/catalogs/factors/${encodeURIComponent(factorName)}?${params.toString()}`);
      if (res.ok) {
        const data = await res.json();
        setFactorDetails(prev => ({ ...prev, [key]: data }));
      }
    } catch (e) {
      console.error("加载因子详情失败:", e);
    }
    setDetailLoading(prev => { const n = new Set(prev); n.delete(key); return n; });
    // 同时加载实验指标
    loadFactorExpMetrics(key, factorName, source);
  }

  async function loadFactorExpMetrics(key: string, factorName: string, source: string) {
    if (factorExpMetrics[key] || expMetricsLoading.has(key)) return;
    setExpMetricsLoading(prev => new Set(prev).add(key));
    try {
      const params = new URLSearchParams({ source, limit: "10", order_by: "collected_at" });
      const res = await fetch(`${API}/quantevolver/factors/${encodeURIComponent(factorName)}/experiment-metrics?${params.toString()}`);
      if (res.ok) {
        const data = await res.json();
        if (data.ok) {
          setFactorExpMetrics(prev => ({ ...prev, [key]: { metrics: data.metrics || [], summary: data.summary || {}, total: data.total || 0 } }));
        }
      }
    } catch (e) {
      console.error("加载因子实验指标失败:", e);
    }
    setExpMetricsLoading(prev => { const n = new Set(prev); n.delete(key); return n; });
  }

  function toggleCode(key: string) {
    setCodeExpanded(prev => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key); else next.add(key);
      return next;
    });
  }

  // 排序逻辑
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
    if (sortField !== field) return " ";
    return sortOrder === "desc" ? " " : " ";
  }

  // 合并因子目录和分类结果
  useEffect(() => {
    const classMap: Record<string, Classification> = {};
    classifications.forEach(c => {
      classMap[`${c.factor_name}||${c.factor_source}`] = c;
    });

    const merged: MergedFactor[] = factors.map(f => {
      const cls = classMap[`${f.factor_name}||${f.source}`];
      return {
        factor_name: f.factor_name,
        source: f.source,
        ic: cls?.ic_value ?? f.ic,
        sharpe: cls?.sharpe_value ?? f.sharpe,
        annualized_return: cls?.ann_ret_value ?? f.annualized_return,
        is_sota_factor: f.is_sota_factor,
        description_cn: f.description_cn,
        category: cls?.category,
        grade: cls?.grade,
        grade_reason: cls?.grade_reason,
        classification_reason: cls?.classification_reason,
        factor_dimension: cls?.factor_dimension,
        description: cls?.description,
        classification_id: cls?.id,
      };
    });

    // 客户端筛选：类别和评级
    let filtered = merged;
    if (categoryFilter) filtered = filtered.filter(f => f.category === categoryFilter);
    if (gradeFilter) filtered = filtered.filter(f => f.grade === gradeFilter);

    // 客户端排序
    if (sortField) {
      filtered.sort((a, b) => {
        let va = (a as any)[sortField];
        let vb = (b as any)[sortField];

        // 处理特殊排序逻辑 (例如 grade 字母序相反，S>A>B>C>D)
        if (sortField === "grade") {
          const gradeOrder: Record<string, number> = { "S": 5, "A": 4, "B": 3, "C": 2, "D": 1 };
          va = gradeOrder[va as string] || 0;
          vb = gradeOrder[vb as string] || 0;
        }

        if (va === vb) return 0;
        if (va == null) return 1;
        if (vb == null) return -1;
        
        return sortOrder === "asc" ? (va > vb ? 1 : -1) : (va < vb ? 1 : -1);
      });
    }

    setMergedFactors(filtered);
  }, [factors, classifications, categoryFilter, gradeFilter, sortField, sortOrder]);

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const factorParams = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
      if (sourceFilter) factorParams.set("source", sourceFilter);
      if (search) factorParams.set("search", search);
      if (!showAlpha) factorParams.set("exclude_source", "alpha158,alpha360");

      const classParams = new URLSearchParams({ limit: "500", offset: "0", active_only: "false" });
      if (sourceFilter) classParams.set("source", sourceFilter);
      if (!showAlpha) classParams.set("exclude_source", "alpha158,alpha360");

      const [fRes, cRes] = await Promise.all([
        fetch(`${API}/quantevolver/factors?${factorParams.toString()}`).then(r => r.json()),
        fetch(`${API}/quantevolver/factor-analyst/classifications?${classParams.toString()}`).then(r => r.json()).catch(() => ({ items: [] })),
      ]);

      setFactors(fRes.items || []);
      setTotal(fRes.total || 0);
      setClassifications(cRes.items || []);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    }
    setLoading(false);
  }, [sourceFilter, search, page, pageSize, showAlpha]);

  useEffect(() => { loadData(); }, [loadData]);

  useEffect(() => {
    setPage(1);
  }, [sourceFilter, search, categoryFilter, gradeFilter, showAlpha]);

  async function batchAnalyze(useLlm: boolean = false) {
    const selectedCount = actualSelectedFactors.size;
    if (selectedCount === 0) {
      alert("请先选择要分析的因子");
      return;
    }
    const factorNames = Array.from(actualSelectedFactors).map(k => k.split("||")[0]);
    if (!confirm(`确定要批量分析选中的 ${selectedCount} 个因子吗？\n这可能需要一些时间。`)) return;
    
    setBatchLoading(true);
    setBatchResult(null);
    try {
      const res = await fetch(`${API}/quantevolver/factor-analyst/batch-analyze`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ use_llm: useLlm, source_filter: sourceFilter || undefined, factor_names: factorNames }),
      });
      const data = await res.json();
      setBatchResult(data);
      if (data.analyzed > 0) loadData();
    } catch (e: any) {
      alert("批量分析失败: " + (e?.message || ""));
    }
    setBatchLoading(false);
  }

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  const isSelection = mode === "selection";

  const thStyle = { padding: "8px 10px", fontWeight: 600, color: "#4b5563", background: "#f9fafb", whiteSpace: "nowrap" as const, cursor: "pointer", userSelect: "none" as const };
  const tdStyle = { padding: "8px 10px" };

  function fmtNum(n: number | null | undefined, frac: number = 4) { return n != null ? n.toFixed(frac) : "-"; }
  function fmtPct(n: number | null | undefined, frac: number = 2) { return n != null ? (n * 100).toFixed(frac) + "%" : "-"; }

  return (
    <div style={!isSelection ? { padding: 24 } : {}}>
      {!isSelection && (
        <section
          style={{
            background: "linear-gradient(135deg, #8b5cf6 0%, #06b6d4 100%)",
            borderRadius: 16, padding: 20, color: "#fff", marginBottom: 16,
          }}
        >
          <h1 style={{ margin: 0, fontSize: 24 }}>因子库</h1>
          <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
            浏览所有因子，查看分类和评级，支持按来源、类别、评级筛选
          </p>
        </section>
      )}

      {/* 筛选栏 */}
      <section style={{ background: "#fff", borderRadius: 12, padding: 16, marginBottom: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
        <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
          <select
            value={sourceFilter}
            onChange={e => setSourceFilter(e.target.value)}
            title="来源筛选"
            style={{ padding: "6px 10px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db" }}
          >
            <option value="">全部来源</option>
            <option value="rdagent_task_sync">RDAgent SOTA</option>
            <option value="alpha158">Alpha158</option>
            <option value="alpha360">Alpha360</option>
          </select>

          <input
            placeholder="搜索因子名称..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            onKeyDown={e => e.key === "Enter" && loadData()}
            style={{ padding: "6px 10px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db", width: 200 }}
          />

          <select
            value={categoryFilter}
            onChange={e => setCategoryFilter(e.target.value)}
            title="类别筛选"
            style={{ padding: "6px 10px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db" }}
          >
            <option value="">全部类别</option>
            {Object.entries(CATEGORY_NAMES).map(([k, v]) => (
              <option key={k} value={k}>{k} - {v}</option>
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

          <button
            onClick={loadData}
            disabled={loading}
            style={{ padding: "6px 12px", fontSize: 12, cursor: "pointer", borderRadius: 6, border: "1px solid #d1d5db", background: "#fff" }}
          >
            {loading ? "加载中..." : "刷新"}
          </button>

          <label style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 12, cursor: "pointer" }}>
            <input
              type="checkbox"
              checked={showAlpha}
              onChange={e => setShowAlpha(e.target.checked)}
            />
            显示Alpha因子
          </label>

          {!isSelection && (
            <>
              <button
                onClick={() => batchAnalyze(false)}
                disabled={batchLoading || actualSelectedFactors.size === 0}
                style={{
                  padding: "6px 14px", fontSize: 12, cursor: (batchLoading || actualSelectedFactors.size === 0) ? "not-allowed" : "pointer",
                  borderRadius: 6, border: "none", background: "#7c3aed", color: "#fff", fontWeight: 600,
                  opacity: (batchLoading || actualSelectedFactors.size === 0) ? 0.5 : 1,
                }}
              >
                {batchLoading ? "批量分析中..." : `批量分析-规则(${actualSelectedFactors.size})`}
              </button>

              <button
                onClick={() => batchAnalyze(true)}
                disabled={batchLoading || actualSelectedFactors.size === 0}
                style={{
                  padding: "6px 14px", fontSize: 12, cursor: (batchLoading || actualSelectedFactors.size === 0) ? "not-allowed" : "pointer",
                  borderRadius: 6, border: "none", background: "#2563eb", color: "#fff", fontWeight: 600,
                  opacity: (batchLoading || actualSelectedFactors.size === 0) ? 0.5 : 1,
                }}
              >
                {batchLoading ? "分析中..." : `批量分析-LLM(${actualSelectedFactors.size})`}
              </button>
            </>
          )}

          <button
            onClick={selectAll}
            style={{ padding: "4px 10px", fontSize: 11, cursor: "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: "#fff" }}
          >全选页</button>
          <button
            onClick={clearSelection}
            style={{ padding: "4px 10px", fontSize: 11, cursor: "pointer", borderRadius: 4, border: "1px solid #d1d5db", background: "#fff" }}
          >清空</button>

          <span style={{ fontSize: 12, color: "#9ca3af" }}>共 {total} 条  已选 {actualSelectedFactors.size}  第 {page}/{totalPages} 页</span>
        </div>

        {/* 批量分析结果提示 */}
        {batchResult && (
          <div style={{
            marginTop: 8, padding: 10, borderRadius: 6, fontSize: 12,
            background: (batchResult.errors?.length || 0) > 0 ? "#fef3c7" : "#d1fae5",
            color: (batchResult.errors?.length || 0) > 0 ? "#92400e" : "#065f46",
          }}>
            <strong>批量分析完成：</strong>
            共 {batchResult.total} 个因子，成功分析 {batchResult.analyzed} 个
            {(batchResult.errors?.length || 0) > 0 && (
              <span>，{batchResult.errors!.length} 个失败</span>
            )}
          </div>
        )}

        {error && <div style={{ marginTop: 8, padding: 8, background: "#fee2e2", borderRadius: 6, fontSize: 12 }}>{error}</div>}
      </section>

      {/* 统一数据表格 */}
      <section style={{ background: "#fff", borderRadius: 12, padding: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>
                <th style={{ ...thStyle, width: 32 }}>
                  <input type="checkbox" title="全选/取消全选"
                    checked={mergedFactors.length > 0 && mergedFactors.every(f => actualSelectedFactors.has(`${f.factor_name}||${f.source}`))}
                    onChange={e => { if (e.target.checked) selectAll(); else clearSelection(); }}
                  />
                </th>
                <th style={{ ...thStyle, maxWidth: 180 }} onClick={() => handleSort("factor_name")}>因子名称{getSortIndicator("factor_name")}</th>
                <th style={thStyle} onClick={() => handleSort("source")}>来源{getSortIndicator("source")}</th>
                <th style={thStyle}>维度</th>
                <th style={thStyle} onClick={() => handleSort("category")}>类别{getSortIndicator("category")}</th>
                <th style={thStyle} onClick={() => handleSort("grade")}>评级{getSortIndicator("grade")}</th>
                <th style={thStyle} onClick={() => handleSort("ic")}>IC{getSortIndicator("ic")}</th>
                <th style={thStyle} onClick={() => handleSort("sharpe")}>Sharpe{getSortIndicator("sharpe")}</th>
                <th style={thStyle} onClick={() => handleSort("annualized_return")}>年化{getSortIndicator("annualized_return")}</th>
                <th style={thStyle}>SOTA</th>
                <th style={thStyle}>说明</th>
              </tr>
            </thead>
            <tbody>
              {mergedFactors.map(f => {
                const rowKey = `${f.factor_name}-${f.source}`;
                const selectKey = `${f.factor_name}||${f.source}`;
                const isExpanded = expandedDescriptions.has(rowKey);
                const isSelected = actualSelectedFactors.has(selectKey);
                const dim = f.factor_dimension ? DIMENSION_NAMES[f.factor_dimension] : null;

                return (
                  <React.Fragment key={rowKey}>
                    <tr 
                      style={{ 
                        borderBottom: isExpanded ? "none" : "1px solid #f3f4f6", 
                        background: isSelected ? (isSelection ? "#eff6ff" : "#faf5ff") : undefined,
                        cursor: isSelection ? "pointer" : "default"
                      }}
                      onClick={() => {
                        if (isSelection) toggleSelect(selectKey);
                      }}
                    >
                      <td style={{ ...tdStyle, width: 32 }}>
                        <input 
                          type="checkbox" 
                          checked={isSelected} 
                          onChange={() => toggleSelect(selectKey)} 
                          onClick={(e) => e.stopPropagation()}
                        />
                      </td>
                      <td style={{ ...tdStyle, maxWidth: 180 }}>
                        <span style={{ fontFamily: "monospace", fontWeight: 600, fontSize: 11, wordBreak: "break-all", color: isSelected && isSelection ? "#1e40af" : "#111827" }}>{f.factor_name}</span>
                      </td>
                      <td style={tdStyle}>
                        <span style={{
                          padding: "2px 6px", borderRadius: 4, fontSize: 10, whiteSpace: "nowrap",
                          background: f.source === "rdagent_task_sync" ? "#ede9fe" : f.source === "alpha158" ? "#dbeafe" : "#d1fae5",
                          color: f.source === "rdagent_task_sync" ? "#7c3aed" : f.source === "alpha158" ? "#2563eb" : "#059669",
                        }}>
                          {f.source === "rdagent_task_sync" ? "SOTA" : f.source}
                        </span>
                      </td>
                      <td style={tdStyle}>
                        {dim ? (
                          <span style={{
                            padding: "2px 6px", borderRadius: 4, fontSize: 10, fontWeight: 600,
                            background: dim.bg, color: dim.color,
                          }}>
                            {dim.label}
                          </span>
                        ) : (
                          <span style={{ color: "#d1d5db", fontSize: 10 }}>-</span>
                        )}
                      </td>
                      <td style={tdStyle}>
                        {f.category ? (
                          <span style={{
                            padding: "2px 6px", borderRadius: 4, fontSize: 10, fontWeight: 600,
                            background: "#f3f4f6",
                          }}>
                            {f.category}{CATEGORY_NAMES[f.category] ? ` ${CATEGORY_NAMES[f.category]}` : ""}
                          </span>
                        ) : (
                          <span style={{ color: "#d1d5db", fontSize: 10 }}>-</span>
                        )}
                      </td>
                      <td style={tdStyle}>
                        {f.grade ? (
                          <span style={{
                            padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 700,
                            background: GRADE_COLORS[f.grade] || "#6b7280",
                            color: "#fff",
                          }}>
                            {f.grade}
                          </span>
                        ) : (
                          <span style={{ color: "#d1d5db", fontSize: 10 }}>-</span>
                        )}
                      </td>
                      <td style={tdStyle}>{f.ic != null ? f.ic.toFixed(4) : "-"}</td>
                      <td style={tdStyle}>{f.sharpe != null ? f.sharpe.toFixed(3) : "-"}</td>
                      <td style={tdStyle}>{f.annualized_return != null ? (f.annualized_return * 100).toFixed(1) + "%" : "-"}</td>
                      <td style={tdStyle}>{f.is_sota_factor ? "" : ""}</td>
                      <td style={tdStyle}>
                        {(f.description || f.classification_reason || f.source === "rdagent_task_sync") ? (
                          <span
                            onClick={(e) => {
                              e.stopPropagation();
                              toggleDescription(rowKey, f.factor_name, f.source);
                            }}
                            style={{
                              color: isSelection ? "#3b82f6" : "#7c3aed", cursor: "pointer",
                              borderBottom: isSelection ? "1px dashed #3b82f6" : "1px dashed #7c3aed", fontSize: 10,
                              userSelect: "none",
                            }}
                          >
                            {isExpanded ? "收起" : "展开"}
                          </span>
                        ) : (
                          <span style={{ color: "#d1d5db", fontSize: 10 }}>-</span>
                        )}
                      </td>
                    </tr>
                    {isExpanded && (() => {
                      const detail = factorDetails[rowKey];
                      const isDetailLoading = detailLoading.has(rowKey);
                      const isCodeOpen = codeExpanded.has(rowKey);

                      return (
                      <tr style={{ borderBottom: "1px solid #f3f4f6" }}>
                        <td colSpan={11} style={{ padding: "0 10px 10px 10px" }}>
                          <div style={{
                            background: isSelection ? "#eff6ff" : "#faf5ff", borderRadius: 8, padding: "10px 14px",
                            fontSize: 12, lineHeight: 1.7, color: "#374151",
                            borderLeft: isSelection ? "3px solid #3b82f6" : "3px solid #7c3aed",
                          }}>
                            {isDetailLoading && (
                              <div style={{ color: "#9ca3af", fontSize: 11, marginBottom: 8 }}>加载详情中...</div>
                            )}

                            {/* 因子说明 */}
                            {f.description && (
                              <div>
                                <strong style={{ color: isSelection ? "#1d4ed8" : "#7c3aed", fontSize: 11 }}>因子说明</strong>
                                <div style={{ marginTop: 4 }}>{f.description}</div>
                              </div>
                            )}

                            {/* 因子维度说明 */}
                            {f.factor_dimension && DIMENSION_NAMES[f.factor_dimension] && (
                              <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #e5e7eb" }}>
                                <strong style={{ color: "#6366f1", fontSize: 11 }}>因子维度</strong>
                                <div style={{ marginTop: 4, fontSize: 11, color: "#4b5563" }}>
                                  <span style={{
                                    padding: "2px 6px", borderRadius: 4, fontSize: 10, fontWeight: 600,
                                    background: DIMENSION_NAMES[f.factor_dimension].bg,
                                    color: DIMENSION_NAMES[f.factor_dimension].color,
                                    marginRight: 6,
                                  }}>
                                    {DIMENSION_NAMES[f.factor_dimension].label}因子
                                  </span>
                                  {DIMENSION_NAMES[f.factor_dimension].desc}
                                </div>
                              </div>
                            )}

                            {/* 分类原因 */}
                            {f.classification_reason && (
                              <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #e5e7eb" }}>
                                <strong style={{ color: "#2563eb", fontSize: 11 }}>分类原因</strong>
                                <div style={{ marginTop: 4 }}>{f.classification_reason}</div>
                              </div>
                            )}

                            {/* 评级原因 */}
                            {f.grade_reason && (
                              <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #e5e7eb" }}>
                                <strong style={{ color: "#ea580c", fontSize: 11 }}>评级原因</strong>
                                <div style={{ marginTop: 4 }}>{f.grade_reason}</div>
                              </div>
                            )}

                            {/* RDAgent Task 来源信息 */}
                            {f.source === "rdagent_task_sync" && (
                              <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #e5e7eb" }}>
                                <strong style={{ color: "#059669", fontSize: 11 }}>RDAgent Task 来源</strong>
                                <div style={{ marginTop: 4, fontSize: 11, color: "#4b5563" }}>
                                  <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "2px 12px", alignItems: "baseline" }}>
                                    <span style={{ color: "#6b7280", whiteSpace: "nowrap" }}>Task ID:</span>
                                    <code style={{ background: "#f3f4f6", padding: "1px 4px", borderRadius: 3, fontSize: 10 }}>
                                      {detail?.source_task_id || "加载中..."}
                                    </code>
                                    <span style={{ color: "#6b7280", whiteSpace: "nowrap" }}>Loop 轮次:</span>
                                    <span>{detail?.source_loop_tag ?? "-"}</span>
                                    {detail?.first_sota_task_id && (
                                      <>
                                        <span style={{ color: "#6b7280", whiteSpace: "nowrap" }}>首次SOTA Task:</span>
                                        <code style={{ background: "#f3f4f6", padding: "1px 4px", borderRadius: 3, fontSize: 10 }}>
                                          {detail.first_sota_task_id}
                                        </code>
                                      </>
                                    )}
                                    <span style={{ color: "#6b7280", whiteSpace: "nowrap" }}>代码来源:</span>
                                    <span>{detail?.source_code_origin || "-"}</span>
                                    {detail?.source_code_relpath && (
                                      <>
                                        <span style={{ color: "#6b7280", whiteSpace: "nowrap" }}>代码路径:</span>
                                        <code style={{ background: "#f3f4f6", padding: "1px 4px", borderRadius: 3, fontSize: 10 }}>
                                          {detail.source_code_relpath}
                                        </code>
                                      </>
                                    )}
                                  </div>
                                  {detail?.description_cn && (
                                    <div style={{ marginTop: 4, color: "#6b7280" }}>{detail.description_cn}</div>
                                  )}
                                </div>
                              </div>
                            )}
                            {f.source !== "rdagent_task_sync" && (
                              <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #e5e7eb" }}>
                                <strong style={{ color: "#059669", fontSize: 11 }}>因子来源</strong>
                                <div style={{ marginTop: 4, fontSize: 11, color: "#4b5563" }}>
                                  内置因子库: <code style={{ background: "#f3f4f6", padding: "1px 4px", borderRadius: 3, fontSize: 10 }}>{f.source}</code>
                                </div>
                              </div>
                            )}

                            {/* 因子表达式 */}
                            {detail?.expression && (
                              <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #e5e7eb" }}>
                                <strong style={{ color: "#d97706", fontSize: 11 }}>因子表达式</strong>
                                <div style={{
                                  marginTop: 4, padding: "6px 10px", background: "#fffbeb",
                                  borderRadius: 6, fontSize: 11, fontFamily: "monospace",
                                  color: "#92400e", wordBreak: "break-all", lineHeight: 1.5,
                                }}>
                                  {detail.expression}
                                </div>
                              </div>
                            )}

                            {/* 因子代码 */}
                            {detail?.code_text && (
                              <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #e5e7eb" }}>
                                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                                  <strong style={{ color: "#0891b2", fontSize: 11 }}>因子代码</strong>
                                  <button
                                    onClick={() => toggleCode(rowKey)}
                                    style={{
                                      padding: "1px 6px", fontSize: 10, border: "1px solid #d1d5db",
                                      borderRadius: 3, background: "#fff", cursor: "pointer", color: "#6b7280",
                                    }}
                                  >
                                    {isCodeOpen ? "收起代码" : "展开代码"}
                                  </button>
                                </div>
                                {isCodeOpen && (
                                  <pre style={{
                                    marginTop: 4, padding: "8px 10px", background: "#1e293b",
                                    borderRadius: 6, fontSize: 10, fontFamily: "monospace",
                                    color: "#e2e8f0", overflow: "auto", maxHeight: 400,
                                    lineHeight: 1.5, whiteSpace: "pre-wrap", wordBreak: "break-all",
                                  }}>
                                    {detail.code_text}
                                  </pre>
                                )}
                              </div>
                            )}

                            {/* 历史实验表现 */}
                            {(() => {
                              const em = factorExpMetrics[rowKey];
                              const emLoading = expMetricsLoading.has(rowKey);
                              
                              if (emLoading) return (
                                <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #e5e7eb" }}>
                                  <strong style={{ color: "#dc2626", fontSize: 11 }}>历史实验表现</strong>
                                  <div style={{ color: "#9ca3af", fontSize: 11, marginTop: 4 }}>加载实验指标中...</div>
                                </div>
                              );
                              
                              if (!em || em.total === 0) return (
                                <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #e5e7eb" }}>
                                  <strong style={{ color: "#dc2626", fontSize: 11 }}>历史实验表现</strong>
                                  <div style={{ color: "#9ca3af", fontSize: 11, marginTop: 4 }}>暂无实验记录</div>
                                </div>
                              );

                              const s = em.summary;
                              return (
                                <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #e5e7eb" }}>
                                  <strong style={{ color: "#dc2626", fontSize: 11 }}>历史实验表现</strong>
                                  <span style={{ fontSize: 10, color: "#9ca3af", marginLeft: 8 }}>共 {em.total} 次实验</span>
                                  
                                  {/* 汇总卡片 */}
                                  <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(120px, 1fr))", gap: 6, marginTop: 6 }}>
                                    {[
                                      { label: "平均IC", value: fmtNum(s.avg_ic), color: (s.avg_ic ?? 0) > 0 ? "#059669" : "#dc2626" },
                                      { label: "最佳IC", value: fmtNum(s.best_ic), color: "#2563eb" },
                                      { label: "平均年化", value: fmtPct(s.avg_ann_return), color: (s.avg_ann_return ?? 0) > 0 ? "#059669" : "#dc2626" },
                                      { label: "平均日胜率", value: fmtPct(s.avg_daily_win_rate), color: "#7c3aed" },
                                      { label: "平均夏普", value: fmtNum(s.avg_sharpe, 2), color: "#0891b2" },
                                      { label: "平均盈亏比", value: fmtNum(s.avg_profit_loss_ratio, 2), color: "#d97706" },
                                    ].map((item, idx) => (
                                      <div key={idx} style={{
                                        background: "#f9fafb", borderRadius: 6, padding: "6px 8px",
                                        border: "1px solid #f3f4f6", textAlign: "center",
                                      }}>
                                        <div style={{ fontSize: 10, color: "#6b7280" }}>{item.label}</div>
                                        <div style={{ fontSize: 13, fontWeight: 700, color: item.color, marginTop: 2 }}>{item.value}</div>
                                      </div>
                                    ))}
                                  </div>

                                  {/* 实验明细表 */}
                                  <div style={{ marginTop: 8, overflowX: "auto" }}>
                                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10 }}>
                                      <thead>
                                        <tr style={{ background: "#f9fafb", borderBottom: "1px solid #e5e7eb" }}>
                                          {["实验名称", "IC", "ICIR", "年化收益", "最大回撤", "夏普", "日胜率", "个股胜率", "盈亏比", "总交易", "时间"].map(h => (
                                            <th key={h} style={{ padding: "4px 6px", fontWeight: 600, whiteSpace: "nowrap", textAlign: "left" }}>{h}</th>
                                          ))}
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {em.metrics.map((m, mi) => (
                                          <tr key={mi} style={{ borderBottom: "1px solid #f3f4f6" }}>
                                            <td style={{ padding: "3px 6px", maxWidth: 140, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                                                title={m.experiment_name || m.experiment_id}>
                                              {m.experiment_name || m.experiment_id?.slice(0, 8)}
                                            </td>
                                            <td style={{ padding: "3px 6px", color: (m.ic ?? 0) > 0 ? "#059669" : "#dc2626", fontWeight: 600 }}>{fmtNum(m.ic)}</td>
                                            <td style={{ padding: "3px 6px" }}>{fmtNum(m.icir)}</td>
                                            <td style={{ padding: "3px 6px", color: (m.ann_return_no_cost ?? 0) > 0 ? "#059669" : "#dc2626", fontWeight: 600 }}>{fmtPct(m.ann_return_no_cost)}</td>
                                            <td style={{ padding: "3px 6px", color: "#dc2626" }}>{fmtPct(m.max_drawdown_no_cost)}</td>
                                            <td style={{ padding: "3px 6px" }}>{fmtNum(m.sharpe_ratio, 2)}</td>
                                            <td style={{ padding: "3px 6px" }}>{fmtPct(m.daily_win_rate)}</td>
                                            <td style={{ padding: "3px 6px" }}>{fmtPct(m.stock_win_rate)}</td>
                                            <td style={{ padding: "3px 6px", color: (m.profit_loss_ratio ?? 0) > 1 ? "#059669" : "#dc2626" }}>{fmtNum(m.profit_loss_ratio, 2)}</td>
                                            <td style={{ padding: "3px 6px" }}>{m.total_trades ?? "-"}</td>
                                            <td style={{ padding: "3px 6px", color: "#9ca3af", whiteSpace: "nowrap" }}>
                                              {m.collected_at ? new Date(m.collected_at).toLocaleDateString() : "-"}
                                            </td>
                                          </tr>
                                        ))}
                                      </tbody>
                                    </table>
                                  </div>
                                </div>
                              );
                            })()}

                            <button
                              onClick={() => toggleDescription(rowKey)}
                              style={{
                                marginTop: 8, padding: "2px 8px", fontSize: 11,
                                border: "1px solid #d1d5db", borderRadius: 4,
                                background: "#fff", cursor: "pointer", color: "#6b7280",
                              }}
                            >
                              收起
                            </button>
                          </div>
                        </td>
                      </tr>
                      );
                    })()}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
          {!loading && mergedFactors.length === 0 && (
            <div style={{ textAlign: "center", padding: 40, color: "#9ca3af" }}>暂无数据</div>
          )}
        </div>

        {/* 翻页控件 */}
        {total > pageSize && (
          <div style={{ display: "flex", justifyContent: "center", alignItems: "center", gap: 8, marginTop: 16, paddingTop: 12, borderTop: "1px solid #f3f4f6" }}>
            <button
              disabled={page <= 1}
              onClick={() => setPage(1)}
              style={{ padding: "4px 8px", borderRadius: 4, border: "1px solid #e5e7eb", background: page <= 1 ? "#f3f4f6" : "#fff", cursor: page <= 1 ? "not-allowed" : "pointer", fontSize: 11 }}
            >
              首页
            </button>
            <button
              disabled={page <= 1}
              onClick={() => setPage(p => Math.max(1, p - 1))}
              style={{ padding: "4px 8px", borderRadius: 4, border: "1px solid #e5e7eb", background: page <= 1 ? "#f3f4f6" : "#fff", cursor: page <= 1 ? "not-allowed" : "pointer", fontSize: 11 }}
            >
              上一页
            </button>
            <span style={{ fontSize: 12, color: "#374151", minWidth: 80, textAlign: "center" }}>
              {page} / {totalPages}
            </span>
            <button
              disabled={page >= totalPages}
              onClick={() => setPage(p => Math.min(totalPages, p + 1))}
              style={{ padding: "4px 8px", borderRadius: 4, border: "1px solid #e5e7eb", background: page >= totalPages ? "#f3f4f6" : "#fff", cursor: page >= totalPages ? "not-allowed" : "pointer", fontSize: 11 }}
            >
              下一页
            </button>
            <button
              disabled={page >= totalPages}
              onClick={() => setPage(totalPages)}
              style={{ padding: "4px 8px", borderRadius: 4, border: "1px solid #e5e7eb", background: page >= totalPages ? "#f3f4f6" : "#fff", cursor: page >= totalPages ? "not-allowed" : "pointer", fontSize: 11 }}
            >
              末页
            </button>
          </div>
        )}
      </section>
    </div>
  );
}
