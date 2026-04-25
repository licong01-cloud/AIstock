"use client";

import React, { useState, useMemo, useCallback, useRef } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";
const BASE = `${API}/quantevolver/evolution`;

interface FactorMetrics {
  ic_mean: number | null;
  rank_ic_mean: number | null;
  icir: number | null;
  rank_icir: number | null;
  top_sharpe: number | null;
  top_excess_sharpe: number | null;
  top_annual_return: number | null;
  top_excess_annual_return: number | null;
  top_max_drawdown: number | null;
  ic_positive_ratio: number | null;
}

interface RelatedFactorWithMetrics {
  factor: string;
  correlation: number;
  method?: string;
  computed_at?: string | null;
  source?: string;
  is_available?: boolean;
  metrics?: FactorMetrics;
}

interface BaseMetrics extends FactorMetrics {
  source?: string;
  is_available?: boolean;
}

interface AnalysisResult {
  recommendation: "DELETE" | "KEEP" | "MANUAL";
  detail: string;
}

type SortKey = "abs_corr" | "ic_mean" | "rank_ic" | "icir" | "sharpe" | "return" | "drawdown" | "factor";
type SortDir = "asc" | "desc";

const THRESHOLD_OPTIONS = [0.7, 0.8, 0.9, 0.95, 0.99, 1.0];

function fmtMetric(v: number | null | undefined, pct = false): string {
  if (v == null) return "-";
  if (pct) return `${(v * 100).toFixed(1)}%`;
  return v.toFixed(4);
}

function compareArrow(
  val: number | null | undefined,
  base: number | null | undefined,
  higherBetter: boolean
): { text: string; color: string } {
  if (val == null || base == null) return { text: "", color: "#9ca3af" };
  const diff = val - base;
  if (Math.abs(diff) < 1e-8) return { text: "", color: "#9ca3af" };
  const better = higherBetter ? diff > 0 : diff < 0;
  return {
    text: better ? "\u2191" : "\u2193",
    color: better ? "#10b981" : "#ef4444",
  };
}

function isAllWorse(
  m: FactorMetrics | undefined,
  base: FactorMetrics | undefined
): boolean {
  if (!m || !base) return false;
  // |IC| 比较：IC 的绝对值代表预测力，符号只代表方向
  const absIc = (v: number | null) => v == null ? null : Math.abs(v);
  const checks: [number | null, number | null, boolean][] = [
    [absIc(m.ic_mean), absIc(base.ic_mean), true],
    [absIc(m.rank_ic_mean), absIc(base.rank_ic_mean), true],
    [m.icir, base.icir, true],
    [m.top_sharpe, base.top_sharpe, true],
    [m.top_annual_return, base.top_annual_return, true],
    [m.top_max_drawdown, base.top_max_drawdown, true],  // 负值，越大（越接近0）越好
  ];
  let hasComparable = false;
  for (const [v, b, higherBetter] of checks) {
    if (v == null || b == null) continue;
    hasComparable = true;
    const worse = higherBetter ? v < b : v > b;
    if (!worse) return false;
  }
  return hasComparable;
}

function recBadge(rec: string): { label: string; bg: string; color: string } {
  switch (rec) {
    case "DELETE":
      return { label: "\ud83d\udd34\u5efa\u8bae\u5220\u9664", bg: "#fef2f2", color: "#dc2626" };
    case "KEEP":
      return { label: "\ud83d\udfe2\u4fdd\u7559", bg: "#f0fdf4", color: "#16a34a" };
    default:
      return { label: "\ud83d\udfe1\u4eba\u5de5\u5224\u65ad", bg: "#fefce8", color: "#ca8a04" };
  }
}

interface Props {
  factorNames: string[];
  disabledFactors?: string[];
  initialBaseFactor?: string;
  showToast: (text: string, type: "success" | "error") => void;
  onRefreshMatrix?: () => void;
  includeDisabled?: boolean;
}

export default function FactorDedupPanel({
  factorNames,
  disabledFactors,
  initialBaseFactor,
  showToast,
  onRefreshMatrix,
  includeDisabled,
}: Props) {
  const [baseFactor, setBaseFactor] = useState(initialBaseFactor || "");
  const [threshold, setThreshold] = useState(0.9);
  const [loading, setLoading] = useState(false);
  const [relatedFactors, setRelatedFactors] = useState<RelatedFactorWithMetrics[]>([]);
  const [baseMetrics, setBaseMetrics] = useState<BaseMetrics | null>(null);
  const [queried, setQueried] = useState(false);

  // checkbox 选择
  const [selected, setSelected] = useState<Set<string>>(new Set());

  // AI 分析结果 (跨刷新保留)
  const analysisMapRef = useRef<Map<string, AnalysisResult>>(new Map());
  const [analysisMap, setAnalysisMap] = useState<Map<string, AnalysisResult>>(new Map());
  const [aiLoading, setAiLoading] = useState(false);

  // 排序
  const [sortKey, setSortKey] = useState<SortKey>("abs_corr");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  // 确认对话框
  const [confirmDialog, setConfirmDialog] = useState<{
    action: "delete" | "set_unavailable";
    factors: { factor_name: string; source: string }[];
  } | null>(null);
  const [actionLoading, setActionLoading] = useState(false);

  // AI 分析详情展开
  const [expandedAnalysis, setExpandedAnalysis] = useState<string | null>(null);
  // 批量分析的原始文本
  const [batchAnalysisText, setBatchAnalysisText] = useState<string | null>(null);

  // 禁用因子集合（来自上游 HDF5 disabled_factors 列表）
  const disabledSet = useMemo(
    () => new Set(disabledFactors ?? []),
    [disabledFactors]
  );

  // ── 查询（使用参数传递因子名，避免 stale closure）──
  const doQuery = useCallback(async (factorToQuery: string, thresholdVal: number) => {
    if (!factorToQuery) return;
    setLoading(true);
    setQueried(true);
    setSelected(new Set());
    try {
      const res = await fetch(
        `${BASE}/correlations/factors/${encodeURIComponent(factorToQuery)}/related` +
          `?threshold=${thresholdVal}&limit=500&include_metrics=true&include_disabled=${includeDisabled ?? false}`
      );
      if (!res.ok) {
        showToast("查询失败", "error");
        setRelatedFactors([]);
        setBaseMetrics(null);
        return;
      }
      const data = await res.json();
      const factors: RelatedFactorWithMetrics[] = data.related_factors || [];
      setRelatedFactors(factors);
      setBaseMetrics(data.base_metrics || null);

      // 自动预勾选: 指标全面劣于基准的因子
      const autoSelected = new Set<string>();
      for (const f of factors) {
        if (isAllWorse(f.metrics, data.base_metrics)) {
          autoSelected.add(f.factor);
        }
      }
      setSelected(autoSelected);

      // 保留已有分析结果中仍存在的因子（含基准因子）
      const existingNames = new Set(factors.map((f) => f.factor));
      existingNames.add(factorToQuery); // 基准因子也保留
      const newMap = new Map<string, AnalysisResult>();
      analysisMapRef.current.forEach((v, k) => {
        if (existingNames.has(k)) newMap.set(k, v);
      });
      analysisMapRef.current = newMap;
      setAnalysisMap(new Map(newMap));
    } catch (e) {
      showToast("查询异常", "error");
    } finally {
      setLoading(false);
    }
  }, [showToast, includeDisabled]);

  const handleQuery = useCallback(() => {
    doQuery(baseFactor, threshold);
  }, [doQuery, baseFactor, threshold]);

  // 当 initialBaseFactor 变更时（含首次传入），自动查询
  const prevInitialRef = useRef<string | undefined>(undefined);
  React.useEffect(() => {
    if (initialBaseFactor && initialBaseFactor !== prevInitialRef.current) {
      prevInitialRef.current = initialBaseFactor;
      setBaseFactor(initialBaseFactor);
      doQuery(initialBaseFactor, threshold);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialBaseFactor, doQuery]);

  // 切换 includeDisabled 时，如已查询过则自动以新参数重查
  const prevIncludeDisabledRef = useRef(includeDisabled);
  React.useEffect(() => {
    if (prevIncludeDisabledRef.current !== includeDisabled) {
      prevIncludeDisabledRef.current = includeDisabled;
      if (baseFactor && queried) {
        doQuery(baseFactor, threshold);
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [includeDisabled]);

  // ── 构建包含基准因子的完整列表 ──
  const allRows = useMemo((): RelatedFactorWithMetrics[] => {
    if (!baseMetrics || !baseFactor) return relatedFactors;
    const baseRow: RelatedFactorWithMetrics = {
      factor: baseFactor,
      correlation: 1.0,
      source: baseMetrics.source,
      is_available: baseMetrics.is_available,
      metrics: {
        ic_mean: baseMetrics.ic_mean,
        rank_ic_mean: baseMetrics.rank_ic_mean,
        icir: baseMetrics.icir,
        rank_icir: baseMetrics.rank_icir,
        top_sharpe: baseMetrics.top_sharpe,
        top_excess_sharpe: baseMetrics.top_excess_sharpe,
        top_annual_return: baseMetrics.top_annual_return,
        top_excess_annual_return: baseMetrics.top_excess_annual_return,
        top_max_drawdown: baseMetrics.top_max_drawdown,
        ic_positive_ratio: baseMetrics.ic_positive_ratio,
      },
    };
    return [baseRow, ...relatedFactors];
  }, [baseFactor, baseMetrics, relatedFactors]);

  // ── 选择 ──
  const toggleSelect = (name: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  const selectAll = () => {
    // 全选排除基准因子（用户可手动勾上）
    const all = new Set(relatedFactors.map((f) => f.factor));
    setSelected(all);
  };

  const deselectAll = () => setSelected(new Set());

  // allSelected 判断: 所有非基准因子都被选中
  const allNonBaseSelected =
    relatedFactors.length > 0 &&
    relatedFactors.every((f) => selected.has(f.factor));

  // 总因子数（基准 + 关联）
  const totalFactorCount = allRows.length;
  // 是否勾选了全部因子（禁止全删）
  const allFactorsSelected = selected.size >= totalFactorCount;

  // ── 排序 ──
  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir((prev) => (prev === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir(key === "factor" ? "asc" : "desc");
    }
  };

  const sortIndicator = (key: SortKey) => {
    if (sortKey !== key) return " \u2195";
    return sortDir === "asc" ? " \u2191" : " \u2193";
  };

  const sorted = useMemo(() => {
    // 基准因子始终在第一行，其余排序
    if (!baseFactor || allRows.length === 0) return allRows;
    const baseRow = allRows.find((f) => f.factor === baseFactor);
    const rest = allRows.filter((f) => f.factor !== baseFactor);
    rest.sort((a, b) => {
      let cmp = 0;
      switch (sortKey) {
        case "abs_corr":
          cmp = Math.abs(a.correlation) - Math.abs(b.correlation);
          break;
        case "factor":
          cmp = a.factor.localeCompare(b.factor);
          break;
        case "ic_mean":
          cmp = (a.metrics?.ic_mean ?? -999) - (b.metrics?.ic_mean ?? -999);
          break;
        case "rank_ic":
          cmp = (a.metrics?.rank_ic_mean ?? -999) - (b.metrics?.rank_ic_mean ?? -999);
          break;
        case "icir":
          cmp = (a.metrics?.icir ?? -999) - (b.metrics?.icir ?? -999);
          break;
        case "sharpe":
          cmp = (a.metrics?.top_sharpe ?? -999) - (b.metrics?.top_sharpe ?? -999);
          break;
        case "return":
          cmp =
            (a.metrics?.top_annual_return ?? -999) -
            (b.metrics?.top_annual_return ?? -999);
          break;
        case "drawdown":
          cmp =
            (a.metrics?.top_max_drawdown ?? -999) -
            (b.metrics?.top_max_drawdown ?? -999);
          break;
      }
      return sortDir === "asc" ? cmp : -cmp;
    });
    return baseRow ? [baseRow, ...rest] : rest;
  }, [allRows, baseFactor, sortKey, sortDir]);

  // ── 批量 AI 分析 ──
  const selectedCount = selected.size;
  // AI 分析: 选中的因子中，排除基准因子后作为 compare_factors（基准因子由 API 自动包含）
  const compareFactorsForAI = useMemo(() => {
    return Array.from(selected).filter((f) => f !== baseFactor);
  }, [selected, baseFactor]);
  const canAiAnalyze = compareFactorsForAI.length > 0 && compareFactorsForAI.length <= 15 && !aiLoading;

  const handleBatchAnalyze = useCallback(async () => {
    if (compareFactorsForAI.length === 0 || compareFactorsForAI.length > 15) return;
    setAiLoading(true);
    setBatchAnalysisText(null);
    try {
      const res = await fetch(`${BASE}/correlations/batch-analyze`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          base_factor: baseFactor,
          compare_factors: compareFactorsForAI,
        }),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        showToast(`AI \u5206\u6790\u5931\u8d25: ${err.detail || res.statusText}`, "error");
        return;
      }
      const data = await res.json();

      // 保存原始分析文本
      setBatchAnalysisText(data.analysis || null);

      // merge recommendations
      const recs: Record<string, string> = data.recommendations || {};
      const newMap = new Map(analysisMapRef.current);
      for (const [fname, rec] of Object.entries(recs)) {
        // 从完整 analysis 文本中截取该因子相关段落作为 detail
        let detail = "";
        const fullText: string = data.analysis || "";
        const idx = fullText.indexOf(fname);
        if (idx >= 0) {
          // 找到下一个 "###" 或文本末尾
          const nextSection = fullText.indexOf("###", idx + fname.length);
          detail = fullText.slice(idx, nextSection > 0 ? nextSection : idx + 500).trim();
        }
        newMap.set(fname, {
          recommendation: rec as "DELETE" | "KEEP" | "MANUAL",
          detail,
        });
      }
      analysisMapRef.current = newMap;
      setAnalysisMap(new Map(newMap));
      showToast(`AI \u5206\u6790\u5b8c\u6210\uff0c\u5df2\u5206\u6790 ${compareFactorsForAI.length + 1} \u4e2a\u56e0\u5b50`, "success");
    } catch (e) {
      showToast("AI \u5206\u6790\u5f02\u5e38", "error");
    } finally {
      setAiLoading(false);
    }
  }, [compareFactorsForAI, baseFactor, showToast]);

  // ── 批量操作 ──
  const selectedFactorsForAction = useMemo(() => {
    return allRows
      .filter((f) => selected.has(f.factor))
      .map((f) => ({
        factor_name: f.factor,
        source: f.source || "unknown",
      }));
  }, [selected, allRows]);

  // 禁止全部删除: 勾选了所有因子（基准 + 全部关联）时禁用
  const canDelete =
    selectedFactorsForAction.length > 0 &&
    !allFactorsSelected &&
    !actionLoading;

  const openConfirm = (action: "delete" | "set_unavailable") => {
    setConfirmDialog({ action, factors: selectedFactorsForAction });
  };

  const executeAction = useCallback(async () => {
    if (!confirmDialog) return;
    setActionLoading(true);
    const deletedNames = new Set(confirmDialog.factors.map((f) => f.factor_name));
    const baseWasDeleted = deletedNames.has(baseFactor);
    try {
      const res = await fetch(`${API}/quantevolver/factors/batch-action`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action: confirmDialog.action,
          factors: confirmDialog.factors,
        }),
      });
      if (!res.ok) {
        showToast("\u64cd\u4f5c\u5931\u8d25", "error");
        return;
      }
      const data = await res.json();
      const actionLabel =
        confirmDialog.action === "delete" ? "\u5220\u9664" : "\u8bbe\u4e3a\u4e0d\u53ef\u7528";
      showToast(
        `${actionLabel}\u6210\u529f ${data.succeeded_count} \u4e2a` +
          (data.failed_count > 0 ? `\uff0c\u5931\u8d25 ${data.failed_count} \u4e2a` : ""),
        data.failed_count > 0 ? "error" : "success"
      );

      setConfirmDialog(null);
      setSelected(new Set());

      if (baseWasDeleted && confirmDialog.action === "delete") {
        // 基准因子被删除，选择列表中第一个未被删除的关联因子作为新基准
        const remaining = relatedFactors.filter((f) => !deletedNames.has(f.factor));
        if (remaining.length > 0) {
          const newBase = remaining[0].factor;
          setBaseFactor(newBase);
          await doQuery(newBase, threshold);
        } else {
          // 所有关联因子都删了，清空
          setRelatedFactors([]);
          setBaseMetrics(null);
          setQueried(false);
        }
      } else {
        // 基准没被删除，正常刷新
        await doQuery(baseFactor, threshold);
      }
      onRefreshMatrix?.();
    } catch (e) {
      showToast("\u64cd\u4f5c\u5f02\u5e38", "error");
    } finally {
      setActionLoading(false);
    }
  }, [confirmDialog, showToast, baseFactor, relatedFactors, threshold, doQuery, onRefreshMatrix]);

  // ── 表头样式 ──
  const thStyle: React.CSSProperties = {
    padding: "8px 10px",
    cursor: "pointer",
    userSelect: "none",
    whiteSpace: "nowrap",
    fontSize: 12,
    textTransform: "uppercase",
    color: "#6b7280",
  };

  return (
    <div>
      {/* 工具栏 */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 12,
          marginBottom: 16,
          flexWrap: "wrap",
        }}
      >
        <label style={{ fontSize: 13, color: "#6b7280" }}>
          基准因子:
        </label>
        <select
          value={baseFactor}
          onChange={(e) => setBaseFactor(e.target.value)}
          style={{
            padding: "6px 10px",
            border: "1px solid #e5e7eb",
            borderRadius: 8,
            fontSize: 13,
            outline: "none",
            maxWidth: 300,
          }}
        >
          <option value="">-- 选择因子 --</option>
          {factorNames.map((f) => (
            <option key={f} value={f}>
              {f}{disabledSet.has(f) ? " [禁用]" : ""}
            </option>
          ))}
        </select>

        <label style={{ fontSize: 13, color: "#6b7280" }}>阈值:</label>
        <select
          value={threshold}
          onChange={(e) => setThreshold(Number(e.target.value))}
          style={{
            padding: "6px 10px",
            border: "1px solid #e5e7eb",
            borderRadius: 8,
            fontSize: 13,
            outline: "none",
          }}
        >
          {THRESHOLD_OPTIONS.map((t) => (
            <option key={t} value={t}>
              |corr| &ge; {t}
            </option>
          ))}
        </select>

        <button
          onClick={handleQuery}
          disabled={!baseFactor || loading}
          style={{
            padding: "6px 16px",
            fontSize: 13,
            fontWeight: 600,
            background: baseFactor ? "#7c3aed" : "#d1d5db",
            color: "#fff",
            border: "none",
            borderRadius: 8,
            cursor: baseFactor ? "pointer" : "default",
            opacity: loading ? 0.6 : 1,
          }}
        >
          {loading ? "\u67e5\u8be2\u4e2d..." : "\u67e5\u8be2"}
        </button>
      </div>

      {/* 结果区域 */}
      {queried && !loading && (
        <>
          {relatedFactors.length === 0 ? (
            <div
              style={{
                color: "#9ca3af",
                textAlign: "center",
                padding: 48,
              }}
            >
              未找到 |corr| &ge; {threshold} 的高相关因子
            </div>
          ) : (
            <>
              {/* 操作栏 */}
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 10,
                  marginBottom: 12,
                  flexWrap: "wrap",
                }}
              >
                <span style={{ fontSize: 13, color: "#374151" }}>
                  共 {allRows.length} 个因子（含基准，|corr| &ge;{" "}
                  {threshold}）
                </span>

                <span style={{ marginLeft: "auto" }} />

                {allFactorsSelected && (
                  <span style={{ fontSize: 12, color: "#ef4444", fontWeight: 600 }}>
                    必须至少保留一个因子
                  </span>
                )}

                <button
                  onClick={allNonBaseSelected ? deselectAll : selectAll}
                  style={{
                    padding: "4px 12px",
                    fontSize: 12,
                    border: "1px solid #e5e7eb",
                    borderRadius: 6,
                    cursor: "pointer",
                    background: "#fff",
                  }}
                >
                  {allNonBaseSelected ? "\u53d6\u6d88\u5168\u9009" : "\u5168\u9009"}
                </button>

                <button
                  onClick={handleBatchAnalyze}
                  disabled={!canAiAnalyze}
                  title={
                    compareFactorsForAI.length > 15
                      ? "\u6700\u591a\u9009\u62e9 15 \u4e2a\u6bd4\u8f83\u56e0\u5b50\u8fdb\u884c AI \u5206\u6790"
                      : selectedCount === 0
                        ? "\u8bf7\u5148\u52fe\u9009\u56e0\u5b50"
                        : ""
                  }
                  style={{
                    padding: "4px 12px",
                    fontSize: 12,
                    fontWeight: 600,
                    border: "none",
                    borderRadius: 6,
                    cursor: canAiAnalyze ? "pointer" : "default",
                    background: canAiAnalyze ? "#8b5cf6" : "#e5e7eb",
                    color: canAiAnalyze ? "#fff" : "#9ca3af",
                  }}
                >
                  {aiLoading
                    ? "AI \u5206\u6790\u4e2d..."
                    : `\u6279\u91cf AI \u5206\u6790(${selectedCount})`}
                  {compareFactorsForAI.length > 15 && " \u26a0\ufe0f\u8d85\u8fc715"}
                </button>

                <button
                  onClick={() => openConfirm("set_unavailable")}
                  disabled={!canDelete}
                  style={{
                    padding: "4px 12px",
                    fontSize: 12,
                    fontWeight: 600,
                    border: "none",
                    borderRadius: 6,
                    cursor: canDelete ? "pointer" : "default",
                    background: canDelete ? "#f59e0b" : "#e5e7eb",
                    color: canDelete ? "#fff" : "#9ca3af",
                  }}
                >
                  批量设为不可用({selectedCount})
                </button>

                <button
                  onClick={() => openConfirm("delete")}
                  disabled={!canDelete}
                  style={{
                    padding: "4px 12px",
                    fontSize: 12,
                    fontWeight: 600,
                    border: "none",
                    borderRadius: 6,
                    cursor: canDelete ? "pointer" : "default",
                    background: canDelete ? "#ef4444" : "#e5e7eb",
                    color: canDelete ? "#fff" : "#9ca3af",
                  }}
                >
                  批量彻底删除({selectedCount})
                </button>
              </div>

              {/* 表格 */}
              <div style={{ overflowX: "auto" }}>
                <table
                  style={{
                    width: "100%",
                    borderCollapse: "collapse",
                    fontSize: 13,
                  }}
                >
                  <thead>
                    <tr style={{ borderBottom: "2px solid #e5e7eb" }}>
                      <th style={{ padding: "8px 6px", width: 32 }}>
                        <input
                          type="checkbox"
                          checked={allNonBaseSelected && relatedFactors.length > 0}
                          onChange={allNonBaseSelected ? deselectAll : selectAll}
                        />
                      </th>
                      <th
                        style={{ ...thStyle, textAlign: "left" }}
                        onClick={() => handleSort("factor")}
                      >
                        因子名称{sortIndicator("factor")}
                      </th>
                      <th
                        style={{ ...thStyle, textAlign: "center" }}
                        onClick={() => handleSort("abs_corr")}
                      >
                        相关系数{sortIndicator("abs_corr")}
                      </th>
                      <th
                        style={{ ...thStyle, textAlign: "center" }}
                        onClick={() => handleSort("ic_mean")}
                      >
                        IC均值{sortIndicator("ic_mean")}
                      </th>
                      <th
                        style={{ ...thStyle, textAlign: "center" }}
                        onClick={() => handleSort("rank_ic")}
                      >
                        RankIC{sortIndicator("rank_ic")}
                      </th>
                      <th
                        style={{ ...thStyle, textAlign: "center" }}
                        onClick={() => handleSort("icir")}
                      >
                        ICIR{sortIndicator("icir")}
                      </th>
                      <th
                        style={{ ...thStyle, textAlign: "center" }}
                        onClick={() => handleSort("sharpe")}
                      >
                        Sharpe{sortIndicator("sharpe")}
                      </th>
                      <th
                        style={{ ...thStyle, textAlign: "center" }}
                        onClick={() => handleSort("return")}
                      >
                        年化{sortIndicator("return")}
                      </th>
                      <th
                        style={{ ...thStyle, textAlign: "center" }}
                        onClick={() => handleSort("drawdown")}
                      >
                        回撤{sortIndicator("drawdown")}
                      </th>
                      <th style={{ ...thStyle, textAlign: "center" }}>来源</th>
                      <th style={{ ...thStyle, textAlign: "center" }}>
                        AI建议
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {sorted.map((f) => {
                      const isBase = f.factor === baseFactor;
                      const isSelected = selected.has(f.factor);
                      const isUnavailable = f.is_available === false;
                      const analysis = analysisMap.get(f.factor);
                      const icArr = compareArrow(
                        f.metrics?.ic_mean,
                        baseMetrics?.ic_mean,
                        true
                      );
                      const ricArr = compareArrow(
                        f.metrics?.rank_ic_mean,
                        baseMetrics?.rank_ic_mean,
                        true
                      );
                      const icirArr = compareArrow(
                        f.metrics?.icir,
                        baseMetrics?.icir,
                        true
                      );
                      const shArr = compareArrow(
                        f.metrics?.top_sharpe,
                        baseMetrics?.top_sharpe,
                        true
                      );
                      const retArr = compareArrow(
                        f.metrics?.top_annual_return,
                        baseMetrics?.top_annual_return,
                        true
                      );
                      const ddArr = compareArrow(
                        f.metrics?.top_max_drawdown,
                        baseMetrics?.top_max_drawdown,
                        true  // 负值，越大（越接近0）越好
                      );

                      return (
                        <React.Fragment key={f.factor}>
                          <tr
                            style={{
                              borderBottom: isBase
                                ? "2px solid #c4b5fd"
                                : "1px solid #f3f4f6",
                              opacity: isUnavailable ? 0.5 : 1,
                              background: isBase
                                ? "#f5f3ff"
                                : isUnavailable
                                  ? "#f9fafb"
                                  : isSelected
                                    ? "#faf5ff"
                                    : undefined,
                            }}
                          >
                            <td style={{ padding: "6px", textAlign: "center" }}>
                              <input
                                type="checkbox"
                                checked={isSelected}
                                onChange={() => toggleSelect(f.factor)}
                              />
                            </td>
                            <td
                              style={{
                                padding: "6px 10px",
                                fontFamily: "monospace",
                                fontSize: 12,
                              }}
                            >
                              {f.factor}
                              {isBase && (
                                <span
                                  style={{
                                    marginLeft: 6,
                                    fontSize: 10,
                                    padding: "1px 6px",
                                    borderRadius: 4,
                                    background: "#ede9fe",
                                    color: "#7c3aed",
                                    fontWeight: 600,
                                  }}
                                >
                                  基准
                                </span>
                              )}
                              {isUnavailable && (
                                <span
                                  style={{
                                    marginLeft: 6,
                                    fontSize: 10,
                                    padding: "1px 5px",
                                    borderRadius: 4,
                                    background: "#fee2e2",
                                    color: "#dc2626",
                                  }}
                                >
                                  禁用
                                </span>
                              )}
                            </td>
                            <td
                              style={{
                                padding: "6px 10px",
                                textAlign: "center",
                                fontWeight: 700,
                                fontFamily: "monospace",
                                color:
                                  Math.abs(f.correlation) >= 0.95
                                    ? "#dc2626"
                                    : "#ea580c",
                              }}
                            >
                              {f.correlation.toFixed(4)}
                            </td>
                            <td
                              style={{
                                padding: "6px 10px",
                                textAlign: "center",
                                fontFamily: "monospace",
                                fontSize: 12,
                              }}
                            >
                              <span style={{ color: icArr.color }}>
                                {icArr.text}
                              </span>
                              {fmtMetric(f.metrics?.ic_mean)}
                            </td>
                            <td
                              style={{
                                padding: "6px 10px",
                                textAlign: "center",
                                fontFamily: "monospace",
                                fontSize: 12,
                              }}
                            >
                              <span style={{ color: ricArr.color }}>
                                {ricArr.text}
                              </span>
                              {fmtMetric(f.metrics?.rank_ic_mean)}
                            </td>
                            <td
                              style={{
                                padding: "6px 10px",
                                textAlign: "center",
                                fontFamily: "monospace",
                                fontSize: 12,
                              }}
                            >
                              <span style={{ color: icirArr.color }}>
                                {icirArr.text}
                              </span>
                              {fmtMetric(f.metrics?.icir)}
                            </td>
                            <td
                              style={{
                                padding: "6px 10px",
                                textAlign: "center",
                                fontFamily: "monospace",
                                fontSize: 12,
                              }}
                            >
                              <span style={{ color: shArr.color }}>
                                {shArr.text}
                              </span>
                              {fmtMetric(f.metrics?.top_sharpe)}
                            </td>
                            <td
                              style={{
                                padding: "6px 10px",
                                textAlign: "center",
                                fontFamily: "monospace",
                                fontSize: 12,
                              }}
                            >
                              <span style={{ color: retArr.color }}>
                                {retArr.text}
                              </span>
                              {fmtMetric(f.metrics?.top_annual_return, true)}
                            </td>
                            <td
                              style={{
                                padding: "6px 10px",
                                textAlign: "center",
                                fontFamily: "monospace",
                                fontSize: 12,
                              }}
                            >
                              <span style={{ color: ddArr.color }}>
                                {ddArr.text}
                              </span>
                              {fmtMetric(f.metrics?.top_max_drawdown, true)}
                            </td>
                            <td
                              style={{
                                padding: "6px 10px",
                                textAlign: "center",
                                fontSize: 12,
                                color: "#6b7280",
                              }}
                            >
                              {f.source || "-"}
                            </td>
                            <td
                              style={{
                                padding: "6px 10px",
                                textAlign: "center",
                              }}
                            >
                              {analysis ? (
                                <button
                                  onClick={() =>
                                    setExpandedAnalysis(
                                      expandedAnalysis === f.factor
                                        ? null
                                        : f.factor
                                    )
                                  }
                                  style={{
                                    padding: "2px 8px",
                                    fontSize: 11,
                                    border: "none",
                                    borderRadius: 4,
                                    cursor: "pointer",
                                    background: recBadge(
                                      analysis.recommendation
                                    ).bg,
                                    color: recBadge(analysis.recommendation)
                                      .color,
                                    fontWeight: 600,
                                  }}
                                >
                                  {recBadge(analysis.recommendation).label}
                                </button>
                              ) : (
                                <span
                                  style={{ color: "#d1d5db", fontSize: 12 }}
                                >
                                  -
                                </span>
                              )}
                            </td>
                          </tr>
                          {/* 展开的分析详情 */}
                          {expandedAnalysis === f.factor && analysis && (
                            <tr>
                              <td colSpan={11}>
                                <div
                                  style={{
                                    padding: "10px 16px",
                                    background: "#faf5ff",
                                    fontSize: 12,
                                    lineHeight: 1.6,
                                    whiteSpace: "pre-wrap",
                                    borderBottom: "2px solid #e5e7eb",
                                  }}
                                >
                                  {analysis.detail || "无详细分析"}
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

              {/* 批量分析原始文本（可折叠） */}
              {batchAnalysisText && (
                <details style={{ marginTop: 16 }}>
                  <summary
                    style={{
                      fontSize: 13,
                      cursor: "pointer",
                      color: "#7c3aed",
                      fontWeight: 600,
                    }}
                  >
                    查看完整 AI 分析报告
                  </summary>
                  <div
                    style={{
                      marginTop: 8,
                      padding: 16,
                      background: "#f9fafb",
                      borderRadius: 8,
                      fontSize: 12,
                      lineHeight: 1.7,
                      whiteSpace: "pre-wrap",
                      maxHeight: 600,
                      overflow: "auto",
                    }}
                  >
                    {batchAnalysisText}
                  </div>
                </details>
              )}
            </>
          )}
        </>
      )}

      {/* 确认对话框 */}
      {confirmDialog && (
        <div
          style={{
            position: "fixed",
            inset: 0,
            zIndex: 10000,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            background: "rgba(0,0,0,0.4)",
          }}
        >
          <div
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 24,
              maxWidth: 500,
              width: "90%",
              boxShadow: "0 8px 32px rgba(0,0,0,0.2)",
            }}
          >
            <h3 style={{ margin: "0 0 12px", fontSize: 16, fontWeight: 700 }}>
              确认
              {confirmDialog.action === "delete"
                ? "批量删除"
                : "批量设为不可用"}
            </h3>
            <p
              style={{
                fontSize: 13,
                color: "#ef4444",
                fontWeight: 600,
                margin: "0 0 12px",
              }}
            >
              {confirmDialog.action === "delete"
                ? "\u26a0\ufe0f 此操作不可逆！将彻底删除以下因子及所有关联数据。"
                : "将以下因子标记为不可用（可恢复）。"}
            </p>
            <div
              style={{
                maxHeight: 200,
                overflow: "auto",
                marginBottom: 16,
                padding: 8,
                background: "#f9fafb",
                borderRadius: 8,
                fontSize: 12,
                fontFamily: "monospace",
              }}
            >
              {confirmDialog.factors.map((f) => (
                <div key={f.factor_name} style={{ padding: "2px 0" }}>
                  {f.factor_name}{" "}
                  <span style={{ color: "#9ca3af" }}>({f.source})</span>
                </div>
              ))}
            </div>
            <div
              style={{ display: "flex", gap: 10, justifyContent: "flex-end" }}
            >
              <button
                onClick={() => setConfirmDialog(null)}
                disabled={actionLoading}
                style={{
                  padding: "6px 16px",
                  fontSize: 13,
                  border: "1px solid #e5e7eb",
                  borderRadius: 8,
                  background: "#fff",
                  cursor: "pointer",
                }}
              >
                取消
              </button>
              <button
                onClick={executeAction}
                disabled={actionLoading}
                style={{
                  padding: "6px 16px",
                  fontSize: 13,
                  fontWeight: 600,
                  border: "none",
                  borderRadius: 8,
                  cursor: actionLoading ? "default" : "pointer",
                  background:
                    confirmDialog.action === "delete" ? "#ef4444" : "#f59e0b",
                  color: "#fff",
                  opacity: actionLoading ? 0.6 : 1,
                }}
              >
                {actionLoading
                  ? "执行中..."
                  : confirmDialog.action === "delete"
                    ? "确认删除"
                    : "确认设为不可用"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
