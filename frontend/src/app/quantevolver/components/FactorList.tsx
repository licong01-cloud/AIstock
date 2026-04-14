"use client";

import React, { useEffect, useState, useCallback, useMemo } from "react";
import FullPipelineDialog from "./FullPipelineDialog";
import ManualFactorDialog from "./ManualFactorDialog";
import ICDecayTrendChart from "./charts/ICDecayTrendChart";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type Factor = {
  factor_name: string;
  source: string;
  expression?: string;
  ic?: number;
  sharpe?: number;
  annualized_return?: number;
  is_sota_factor?: boolean;
  is_available?: boolean;
  catalog_source?: string;
  description_cn?: string;
  generated_at_utc?: string;
  ind_ic?: number | null;
  ind_sharpe?: number | null;
  ind_annual_return?: number | null;
  ind_rank_ic?: number | null;
  ind_icir?: number | null;
};

export type MergedFactor = {
  factor_name: string;
  source: string;
  ic?: number;
  sharpe?: number;
  annualized_return?: number;
  is_sota_factor?: boolean;
  is_available?: boolean;
  description_cn?: string;
  category?: string;
  grade?: string;
  grade_reason?: string;
  classification_reason?: string;
  factor_dimension?: string;
  description?: string;
  classification_id?: number;
  ind_ic?: number | null;
  ind_rank_ic?: number | null;
  ind_rank_ic_1m?: number | null;
  ind_sharpe?: number | null;
  ind_annual_return?: number | null;
  has_ind_metrics?: boolean;
  ind_calculated_at?: string | null;
  generated_at_utc?: string | null;
  decay_status?: "ok" | "warning" | "danger" | null;
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
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sourceFilter, setSourceFilter] = useState("");
  const [search, setSearch] = useState("");
  const [categoryFilter, setCategoryFilter] = useState("");
  const [gradeFilter, setGradeFilter] = useState("");
  const [availabilityFilter, setAvailabilityFilter] = useState("enabled");
  const [page, setPage] = useState(1);
  const [pageSize] = useState(50);
  const [showAlpha, setShowAlpha] = useState(false);
  const [batchLoading, setBatchLoading] = useState(false);
  const [batchProgress, setBatchProgress] = useState<{ current: number; total: number; factor_name?: string } | null>(null);
  const [batchResult, setBatchResult] = useState<{ total?: number; analyzed?: number; errors?: string[] } | null>(null);
  const [expandedDescriptions, setExpandedDescriptions] = useState<Set<string>>(new Set());
  const [localSelectedFactors, setLocalSelectedFactors] = useState<Set<string>>(selectedFactors);
  const [factorDetails, setFactorDetails] = useState<Record<string, FactorDetail>>({});
  const [detailLoading, setDetailLoading] = useState<Set<string>>(new Set());
  const [codeExpanded, setCodeExpanded] = useState<Set<string>>(new Set());
  const [factorExpMetrics, setFactorExpMetrics] = useState<Record<string, FactorExpMetrics>>({});
  const [expMetricsLoading, setExpMetricsLoading] = useState<Set<string>>(new Set());
  const [metricsLoading, setMetricsLoading] = useState(false);
  const [metricsResult, setMetricsResult] = useState<{ ok?: boolean; total_metrics_inserted?: number; total_metrics_skipped?: number; fail_count?: number; error?: string } | null>(null);
  const [factorIndMetrics, setFactorIndMetrics] = useState<Record<string, any[]>>({});
  const [indSummary, setIndSummary] = useState<Record<string, { ic_mean: number | null; sharpe: number | null; annual_return: number | null }>>({});

  // Task分组相关状态
  type SourceTask = { task_id: string; factor_count: number; ok_count: number; skipped_count: number; error_count: number };
  const [viewMode, setViewMode] = useState<"list" | "task">("list");
  const [sourceTasks, setSourceTasks] = useState<SourceTask[]>([]);
  const [selectedTasks, setSelectedTasks] = useState<Set<string>>(new Set());
  const [taskComputing, setTaskComputing] = useState<Set<string>>(new Set());
  const [taskResults, setTaskResults] = useState<Record<string, { ok: boolean; msg: string }>>({});
  const [expandedTask, setExpandedTask] = useState<string | null>(null);
  const [taskFactors, setTaskFactors] = useState<Record<string, any[]>>({});
  const [taskFactorsLoading, setTaskFactorsLoading] = useState<Set<string>>(new Set());
  const [taskAnalyzing, setTaskAnalyzing] = useState(false);
  const [taskAnalyzeResult, setTaskAnalyzeResult] = useState<{ ok?: boolean; total?: number; analyzed?: number; errors?: string[] } | null>(null);

  // 全流程批处理
  const [pipelineOpen, setPipelineOpen] = useState(false);
  const [manualDialogOpen, setManualDialogOpen] = useState(false);
  const [pipelineTaskIds, setPipelineTaskIds] = useState<string[]>([]);
  const [pipelineFactorNames, setPipelineFactorNames] = useState<string[]>([]);

  // 因子计算日志详情
  type CalcWindow = { eval_window: string; status: string; error_message?: string | null; n_trading_days?: number | null; required_days?: number | null; data_start?: string | null; data_end?: string | null; calculated_at?: string | null };
  type CalcFactor = { factor_name: string; windows: CalcWindow[] };
  type CalcDetail = { factors: CalcFactor[]; summary: { ok_count: number; skipped_count: number; error_count: number } };
  const [taskCalcDetail, setTaskCalcDetail] = useState<Record<string, CalcDetail>>({});
  const [calcDetailLoading, setCalcDetailLoading] = useState<Set<string>>(new Set());

  // 数据快照管理
  type Snapshot = { data_date: string; status: string; start_date?: string; end_date?: string; instruments_count?: number; created_at?: string; realtime_rows?: number; static_rows?: number; disk_size_mb?: number };
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [snapshotLoading, setSnapshotLoading] = useState(false);
  const [snapshotCreating, setSnapshotCreating] = useState(false);
  const [snapshotDeleting, setSnapshotDeleting] = useState<string | null>(null);
  const [activeSnapshot, setActiveSnapshot] = useState<string>("");
  const [newSnapshotDate, setNewSnapshotDate] = useState("");
  const [snapshotPanelOpen, setSnapshotPanelOpen] = useState(false);
  const [timeEstimate, setTimeEstimate] = useState<{
    has_history: boolean;
    stats?: { avg_sec: number; median_sec: number; p90_sec: number; max_sec: number };
    estimate?: { factor_count: number; serial_min: number; parallel_4_min: number };
    slowest_5?: { factor_name: string; elapsed_sec: number }[];
  } | null>(null);

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

  // ── 数据快照管理 ──

  const loadSnapshots = useCallback(async () => {
    setSnapshotLoading(true);
    try {
      const [snapRes, estRes] = await Promise.all([
        fetch(`${API}/quantevolver/evolution/factor-values/snapshots`),
        fetch(`${API}/quantevolver/evolution/factor-values/time-estimate`),
      ]);
      if (snapRes.ok) {
        const data = await snapRes.json();
        setSnapshots(data.snapshots || []);
      }
      if (estRes.ok) {
        setTimeEstimate(await estRes.json());
      }
    } catch (e: any) {
      console.error("加载快照列表失败:", e);
    } finally {
      setSnapshotLoading(false);
    }
  }, []);

  async function createSnapshot() {
    if (!newSnapshotDate || newSnapshotDate.length !== 8) {
      alert("请从日历选择日期");
      return;
    }
    setSnapshotCreating(true);
    try {
      const params = new URLSearchParams({ data_date: newSnapshotDate });
      const res = await fetch(`${API}/quantevolver/evolution/factor-values/compute?${params}`, {
        method: "POST",
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        const detail = err.detail;
        const msg = typeof detail === "string" ? detail
          : Array.isArray(detail) ? detail.map((d: any) => d.msg || JSON.stringify(d)).join("; ")
          : JSON.stringify(err) || `HTTP ${res.status}`;
        throw new Error(msg);
      }
      setNewSnapshotDate("");
      // 延迟刷新（后台创建需要时间）
      setTimeout(() => loadSnapshots(), 3000);
      alert(`快照 ${newSnapshotDate} 创建已启动，请稍后刷新查看`);
    } catch (e: any) {
      alert(`创建快照失败: ${e.message}`);
    } finally {
      setSnapshotCreating(false);
    }
  }

  async function deleteSnapshot(dataDate: string) {
    if (!confirm(`确认删除快照 ${dataDate}？此操作不可恢复。`)) return;
    setSnapshotDeleting(dataDate);
    try {
      const res = await fetch(`${API}/quantevolver/evolution/factor-values/snapshots/${dataDate}`, { method: "DELETE" });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        const detail = err.detail;
        const msg = typeof detail === "string" ? detail
          : Array.isArray(detail) ? detail.map((d: any) => d.msg || JSON.stringify(d)).join("; ")
          : JSON.stringify(err) || `HTTP ${res.status}`;
        throw new Error(msg);
      }
      if (activeSnapshot === dataDate) setActiveSnapshot("");
      await loadSnapshots();
    } catch (e: any) {
      alert(`删除快照失败: ${e.message}`);
    } finally {
      setSnapshotDeleting(null);
    }
  }

  // 初始加载快照列表
  useEffect(() => { loadSnapshots(); }, [loadSnapshots]);

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
        if (factorName) loadFactorIndependentMetrics(key, factorName);
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
    if (sortField !== field) return "";
    return sortOrder === "desc" ? " ▼" : " ▲";
  }

  // 合并因子数据 — 排序已在后端完成，这里只做字段映射和客户端筛选
  const mergedFactors = useMemo(() => {
    const merged: MergedFactor[] = factors.map((f: any) => {
      const ind = indSummary[f.factor_name];
      return {
        factor_name: f.factor_name,
        source: f.source,
        ic: f.ic,
        sharpe: f.sharpe,
        annualized_return: f.annualized_return,
        is_sota_factor: f.is_sota_factor,
        is_available: f.is_available !== false,
        description_cn: f.description_cn,
        category: f.category,
        grade: f.grade,
        grade_reason: f.grade_reason,
        classification_reason: f.classification_reason,
        factor_dimension: f.factor_dimension,
        description: f.cl_description,
        classification_id: f.classification_id,
        ind_ic: f.ind_ic ?? ind?.ic_mean ?? null,
        ind_rank_ic: f.ind_rank_ic ?? null,
        ind_sharpe: f.ind_sharpe ?? ind?.sharpe ?? null,
        ind_annual_return: f.ind_annual_return ?? ind?.annual_return ?? null,
        has_ind_metrics: f.ind_ic != null || !!ind,
        ind_calculated_at: f.ind_calculated_at ?? null,
        generated_at_utc: f.generated_at_utc ?? null,
        ind_rank_ic_1m: f.ind_rank_ic_1m ?? null,
        decay_status: (() => {
          const fullIC = f.ind_rank_ic ?? null;
          const ic1m = f.ind_rank_ic_1m ?? null;
          if (fullIC == null || ic1m == null) return null;
          // 红色: recent_1m IC < 0 且 full IC > 0.03（因子可能已失效）
          if (ic1m < 0 && Math.abs(fullIC) > 0.03) return "danger" as const;
          // 黄色: recent_1m IC / full IC < 0.5（衰减过半）
          if (Math.abs(fullIC) > 0.01 && Math.abs(ic1m / fullIC) < 0.5) return "warning" as const;
          return "ok" as const;
        })(),
      };
    });

    let filtered = merged;

    return filtered;
  }, [factors, indSummary, categoryFilter, gradeFilter]);

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const factorParams = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
      if (sourceFilter) factorParams.set("source", sourceFilter);
      if (search) factorParams.set("search", search);
      if (!showAlpha) factorParams.set("exclude_source", "alpha158,alpha360");
      if (sortField) factorParams.set("sort_field", sortField);
      if (sortField) factorParams.set("sort_order", sortOrder);
      if (categoryFilter) factorParams.set("category", categoryFilter);
      if (gradeFilter) factorParams.set("grade", gradeFilter);
      if (availabilityFilter && availabilityFilter !== "all") factorParams.set("availability", availabilityFilter);

      const fRes = await fetch(`${API}/quantevolver/factors?${factorParams.toString()}`).then(r => r.json());

      setFactors(fRes.items || []);
      setTotal(fRes.total || 0);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    }
    setLoading(false);
  }, [sourceFilter, search, page, pageSize, showAlpha, sortField, sortOrder, categoryFilter, gradeFilter, availabilityFilter]);

  const loadIndSummary = useCallback(async () => {
    try {
      const res = await fetch(`${API}/quantevolver/factors/independent-metrics-summary`);
      if (res.ok) { const d = await res.json(); if (d.ok) setIndSummary(d.summary || {}); }
    } catch {}
  }, []);

  useEffect(() => { loadData(); loadIndSummary(); }, [loadData, loadIndSummary]);

  // 加载 source tasks 列表
  const loadSourceTasks = useCallback(async () => {
    try {
      const res = await fetch(`${API}/rdagent/catalogs/factors/source-tasks`);
      if (res.ok) {
        const data = await res.json();
        setSourceTasks(data.items || []);
      }
    } catch (e) { console.warn("加载source tasks失败:", e); }
  }, []);
  useEffect(() => { loadSourceTasks(); }, [loadSourceTasks]);

  function toggleTask(tid: string) {
    setSelectedTasks(prev => {
      const next = new Set(prev);
      if (next.has(tid)) next.delete(tid); else next.add(tid);
      return next;
    });
  }

  async function toggleExpandTask(tid: string) {
    if (expandedTask === tid) { setExpandedTask(null); return; }
    setExpandedTask(tid);
    if (taskFactors[tid]) return; // already loaded
    setTaskFactorsLoading(prev => new Set(prev).add(tid));
    try {
      const res = await fetch(`${API}/rdagent/catalogs/factors?source_task_id=${encodeURIComponent(tid)}&limit=200`);
      if (res.ok) {
        const data = await res.json();
        setTaskFactors(prev => ({ ...prev, [tid]: data.items || [] }));
      }
    } catch (e) { console.warn("加载task因子失败:", e); }
    finally { setTaskFactorsLoading(prev => { const n = new Set(prev); n.delete(tid); return n; }); }
    // 同时加载计算日志详情
    if (!taskCalcDetail[tid]) loadCalcDetail(tid);
  }

  async function loadCalcDetail(tid: string) {
    if (calcDetailLoading.has(tid)) return;
    setCalcDetailLoading(prev => new Set(prev).add(tid));
    try {
      const res = await fetch(`${API}/rdagent/catalogs/factors/source-tasks/${encodeURIComponent(tid)}/calc-detail`);
      if (res.ok) {
        const data = await res.json();
        setTaskCalcDetail(prev => ({ ...prev, [tid]: data }));
      }
    } catch (e) { console.warn("加载计算日志详情失败:", e); }
    finally { setCalcDetailLoading(prev => { const n = new Set(prev); n.delete(tid); return n; }); }
  }

  async function computeSelectedTasksMetrics() {
    const tasks = Array.from(selectedTasks);
    if (tasks.length === 0) return;
    setTaskResults({});
    // 逐task反查因子名，再用unified端点统一计算
    for (const tid of tasks) {
      setTaskComputing(prev => new Set(prev).add(tid));
      try {
        // 强制快照模式检查
        if (!activeSnapshot) {
          setTaskResults(prev => ({ ...prev, [tid]: { ok: false, msg: "请先选择数据快照" } }));
          setTaskComputing(prev => { const n = new Set(prev); n.delete(tid); return n; });
          continue;
        }
        // 1. 反查该task下的因子列表
        const listRes = await fetch(`${API}/rdagent/catalogs/factors?source_task_id=${encodeURIComponent(tid)}&limit=500`);
        const listData = await listRes.json();
        const factorNames = (listData.items || []).map((f: any) => f.factor_name || f.name).filter(Boolean);
        if (factorNames.length === 0) {
          setTaskResults(prev => ({ ...prev, [tid]: { ok: false, msg: "无因子" } }));
          setTaskComputing(prev => { const n = new Set(prev); n.delete(tid); return n; });
          continue;
        }
        // 2. 用unified端点计算指标
        const res = await fetch(`${API}/quantevolver/factors/batch-compute-metrics-unified`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            factor_names: factorNames,
            data_date: activeSnapshot,
          }),
        });
        const data = await res.json();
        const okCount = data.db_result?.inserted || 0;
        setTaskResults(prev => ({
          ...prev,
          [tid]: {
            ok: data.success !== false,
            msg: data.success !== false ? `${factorNames.length}因子, ${okCount}条指标写入` : (data.error || "失败"),
          },
        }));
      } catch (e: any) {
        setTaskResults(prev => ({ ...prev, [tid]: { ok: false, msg: e.message } }));
      }
      setTaskComputing(prev => { const n = new Set(prev); n.delete(tid); return n; });
    }
    loadSourceTasks();
    loadIndSummary();
  }

  async function analyzeSelectedTasksFactors() {
    const tasks = Array.from(selectedTasks);
    if (tasks.length === 0) return;
    // 先查出所有选中 task 下的因子名称
    const allFactorNames: string[] = [];
    for (const tid of tasks) {
      try {
        const res = await fetch(`${API}/rdagent/catalogs/factors?source_task_id=${encodeURIComponent(tid)}&limit=500`);
        if (res.ok) {
          const data = await res.json();
          const names = (data.items || []).map((f: any) => f.name || f.factor_name).filter(Boolean);
          allFactorNames.push(...names);
        }
      } catch {}
    }
    if (allFactorNames.length === 0) { alert("选中的Task下没有因子"); return; }
    const unique = [...new Set(allFactorNames)];
    if (!confirm(`将对 ${tasks.length} 个Task下共 ${unique.length} 个因子执行分析，确定继续？`)) return;

    setTaskAnalyzing(true);
    setTaskAnalyzeResult(null);
    try {
      const res = await fetch(`${API}/quantevolver/factor-analyst/batch-analyze`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ use_llm: true, factor_names: unique }),
      });
      const data = await res.json();
      setTaskAnalyzeResult(data);
      if (data.analyzed > 0) { loadData(); loadSourceTasks(); }
    } catch (e: any) {
      setTaskAnalyzeResult({ ok: false, errors: [e?.message || "分析失败"] });
    }
    setTaskAnalyzing(false);
    setLocalSelectedFactors(new Set());
  }

  useEffect(() => {
    setPage(1);
  }, [sourceFilter, search, categoryFilter, gradeFilter, showAlpha, sortField, sortOrder, availabilityFilter]);

  async function batchAnalyze() {
    const selectedCount = actualSelectedFactors.size;
    if (selectedCount === 0) {
      alert("请先选择要分析的因子");
      return;
    }
    const factorNames = Array.from(actualSelectedFactors).map(k => k.split("||")[0]);
    if (!confirm(`确定要批量分析选中的 ${selectedCount} 个因子吗？\n这可能需要一些时间。`)) return;

    setBatchLoading(true);
    setBatchProgress(null);
    setBatchResult(null);

    const handleBeforeUnload = (e: BeforeUnloadEvent) => {
      e.preventDefault();
      e.returnValue = '';
    };
    window.addEventListener('beforeunload', handleBeforeUnload);

    try {
      const body = { use_llm: true, source_filter: sourceFilter || undefined, factor_names: factorNames };
      const res = await fetch(`${API}/quantevolver/factor-analyst/batch-analyze-stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      const reader = res.body?.getReader();
      const decoder = new TextDecoder();
      if (!reader) throw new Error("无法读取响应流");

      let buffer = '';
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try {
              const data = JSON.parse(line.slice(6));
              if (data.type === 'progress') {
                setBatchProgress({ current: data.current, total: data.total, factor_name: data.factor_name });
              } else if (data.type === 'done') {
                setBatchResult({ total: data.total, analyzed: data.analyzed, errors: data.errors });
                if (data.analyzed > 0) loadData();
              }
            } catch (e) {
              console.error('JSON解析失败:', line, e);
            }
          }
        }
      }
    } catch (e: any) {
      alert("批量分析失败: " + (e?.message || ""));
    }
    setBatchLoading(false);
    setLocalSelectedFactors(new Set());
    window.removeEventListener('beforeunload', handleBeforeUnload);
  }

  async function batchFetchMetrics() {
    const selectedCount = actualSelectedFactors.size;
    if (selectedCount === 0) { alert("请先选择要计算指标的因子"); return; }
    if (!activeSnapshot) {
      alert("请先选择数据快照。所有因子必须使用相同快照数据计算，确保横向可比。");
      return;
    }
    const factorNames = Array.from(actualSelectedFactors).map(k => k.split("||")[0]);
    setMetricsLoading(true);
    setMetricsResult(null);
    try {
      const res = await fetch(`${API}/quantevolver/factors/batch-compute-metrics-unified`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          factor_names: factorNames,
          data_date: activeSnapshot,
        }),
      });
      const data = await res.json();
      const isOk = data.success !== false;
      const dbResult = data.db_result || {};
      setMetricsResult({
        ok: isOk,
        total_metrics_inserted: dbResult.inserted || 0,
        total_metrics_skipped: dbResult.skipped || 0,
        fail_count: (dbResult.errors || []).length,
        error: data.error,
      });
      if (isOk) { loadData(); loadIndSummary(); }
    } catch (e: any) {
      setMetricsResult({ ok: false, error: e?.message || "计算失败" });
    }
    setMetricsLoading(false);
    setLocalSelectedFactors(new Set());
  }

  async function loadFactorIndependentMetrics(key: string, factorName: string) {
    if (factorIndMetrics[key]) return;
    try {
      const res = await fetch(`${API}/quantevolver/factors/${encodeURIComponent(factorName)}/independent-metrics?limit=10`);
      if (res.ok) {
        const data = await res.json();
        if (data.ok) setFactorIndMetrics(prev => ({ ...prev, [key]: data.metrics || [] }));
      }
    } catch (e) { console.error("加载独立指标失败:", e); }
  }

  async function deleteFactor(factorName: string, factorSource: string) {
    if (!confirm(
      `确定要删除因子「${factorName}」(来源: ${factorSource}) 吗？\n\n` +
      `将同时删除：计算日志、独立指标、分类评级、实验指标、相关性矩阵、实时追踪等所有关联数据。\n\n此操作不可撤销！`
    )) return;
    try {
      const params = new URLSearchParams({ factor_name: factorName, source: factorSource });
      const res = await fetch(`${API}/quantevolver/factors?${params.toString()}`, { method: "DELETE" });
      const data = await res.json();
      if (!res.ok) {
        alert("删除失败: " + (data.detail || res.statusText));
        return;
      }
      alert(`因子「${factorName}」已删除`);
      loadData();
    } catch (e: any) {
      alert("删除失败: " + (e?.message || "网络错误"));
    }
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

      {/* 数据快照管理 */}
      {!isSelection && (
        <section style={{ background: "#fff", borderRadius: 12, padding: "12px 16px", marginBottom: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
            <span style={{ fontWeight: 600, fontSize: 13, color: "#374151" }}>数据快照</span>

            {/* 当前选中的快照 */}
            <select
              value={activeSnapshot}
              onChange={e => setActiveSnapshot(e.target.value)}
              style={{ padding: "5px 8px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db", minWidth: 160 }}
              title="选择数据快照用于因子计算"
            >
              <option value="">实时数据（无快照）</option>
              {snapshots.filter(s => s.status === "ready").map(s => (
                <option key={s.data_date} value={s.data_date}>
                  {s.data_date} ({s.instruments_count || "?"}只 / {s.disk_size_mb || "?"}MB)
                </option>
              ))}
            </select>

            {activeSnapshot ? (
              <span style={{ fontSize: 11, color: "#059669", background: "#ecfdf5", padding: "2px 8px", borderRadius: 4 }}>
                快照 {activeSnapshot} — 所有因子使用相同数据，可横向比对
              </span>
            ) : (
              <span style={{ fontSize: 11, color: "#dc2626", background: "#fef2f2", padding: "2px 8px", borderRadius: 4 }}>
                未选择快照 — 请先选择或创建快照后再计算因子指标
              </span>
            )}

            <button
              onClick={() => setSnapshotPanelOpen(!snapshotPanelOpen)}
              style={{ padding: "4px 10px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db", background: snapshotPanelOpen ? "#f3f4f6" : "#fff", cursor: "pointer" }}
            >
              {snapshotPanelOpen ? "收起管理" : "管理快照"}
            </button>

            <button
              onClick={loadSnapshots}
              disabled={snapshotLoading}
              style={{ padding: "4px 8px", fontSize: 11, borderRadius: 6, border: "1px solid #d1d5db", background: "#fff", cursor: "pointer", opacity: snapshotLoading ? 0.5 : 1 }}
            >
              {snapshotLoading ? "刷新中..." : "刷新"}
            </button>
          </div>

          {/* 展开的快照管理面板 */}
          {snapshotPanelOpen && (
            <div style={{ marginTop: 12, borderTop: "1px solid #e5e7eb", paddingTop: 12 }}>
              {/* 创建新快照 */}
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 12 }}>
                <span style={{ fontSize: 12, color: "#6b7280" }}>新建快照:</span>
                <input
                  type="date"
                  value={newSnapshotDate ? `${newSnapshotDate.slice(0,4)}-${newSnapshotDate.slice(4,6)}-${newSnapshotDate.slice(6,8)}` : ""}
                  onChange={e => setNewSnapshotDate(e.target.value.replace(/-/g, ""))}
                  max={new Date().toISOString().split("T")[0]}
                  style={{ padding: "4px 8px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db", width: 160 }}
                />
                <button
                  onClick={createSnapshot}
                  disabled={snapshotCreating || !newSnapshotDate}
                  style={{ padding: "4px 12px", fontSize: 12, borderRadius: 6, border: "none", background: "#8b5cf6", color: "#fff", cursor: "pointer", opacity: (snapshotCreating || !newSnapshotDate) ? 0.5 : 1 }}
                >
                  {snapshotCreating ? "创建中..." : "创建"}
                </button>
                <span style={{ fontSize: 11, color: "#9ca3af" }}>首次创建需从数据库加载，约 5-8 分钟</span>
              </div>

              {/* 时间预估 */}
              {timeEstimate?.has_history && timeEstimate.stats && timeEstimate.estimate && (
                <div style={{ display: "flex", gap: 16, marginBottom: 12, fontSize: 11, color: "#6b7280", flexWrap: "wrap" }}>
                  <span>因子计算预估: <b>{timeEstimate.estimate.factor_count}</b> 个因子</span>
                  <span>串行 ≈ <b>{timeEstimate.estimate.serial_min}</b> 分钟</span>
                  <span>4线程 ≈ <b>{timeEstimate.estimate.parallel_4_min}</b> 分钟</span>
                  <span>单因子均值 <b>{timeEstimate.stats.avg_sec.toFixed(1)}s</b> / P90 <b>{timeEstimate.stats.p90_sec.toFixed(1)}s</b> / 最慢 <b>{timeEstimate.stats.max_sec.toFixed(1)}s</b></span>
                  {timeEstimate.slowest_5 && timeEstimate.slowest_5.length > 0 && (
                    <span title={timeEstimate.slowest_5.map(s => `${s.factor_name}: ${s.elapsed_sec.toFixed(1)}s`).join("\n")}>
                      最慢因子: {timeEstimate.slowest_5[0].factor_name} ({timeEstimate.slowest_5[0].elapsed_sec.toFixed(1)}s)
                    </span>
                  )}
                </div>
              )}

              {/* 快照列表 */}
              {snapshots.length === 0 ? (
                <p style={{ fontSize: 12, color: "#9ca3af", margin: 0 }}>暂无快照</p>
              ) : (
                <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
                  <thead>
                    <tr style={{ borderBottom: "1px solid #e5e7eb" }}>
                      <th style={{ textAlign: "left", padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>日期</th>
                      <th style={{ textAlign: "left", padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>状态</th>
                      <th style={{ textAlign: "right", padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>股票数</th>
                      <th style={{ textAlign: "right", padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>行情行数</th>
                      <th style={{ textAlign: "right", padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>磁盘</th>
                      <th style={{ textAlign: "left", padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>创建时间</th>
                      <th style={{ textAlign: "center", padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {snapshots.map(s => (
                      <tr key={s.data_date} style={{ borderBottom: "1px solid #f3f4f6" }}>
                        <td style={{ padding: "6px 8px", fontWeight: activeSnapshot === s.data_date ? 600 : 400 }}>
                          {s.data_date}
                          {activeSnapshot === s.data_date && <span style={{ marginLeft: 6, color: "#059669", fontSize: 10 }}>当前</span>}
                        </td>
                        <td style={{ padding: "6px 8px" }}>
                          <span style={{
                            padding: "1px 6px", borderRadius: 4, fontSize: 11,
                            background: s.status === "ready" ? "#ecfdf5" : s.status === "creating" ? "#fef3c7" : "#fef2f2",
                            color: s.status === "ready" ? "#059669" : s.status === "creating" ? "#d97706" : "#dc2626",
                          }}>
                            {s.status === "ready" ? "就绪" : s.status === "creating" ? "创建中" : "异常"}
                          </span>
                        </td>
                        <td style={{ padding: "6px 8px", textAlign: "right" }}>{s.instruments_count?.toLocaleString() || "-"}</td>
                        <td style={{ padding: "6px 8px", textAlign: "right" }}>{s.realtime_rows?.toLocaleString() || "-"}</td>
                        <td style={{ padding: "6px 8px", textAlign: "right" }}>{s.disk_size_mb ? `${s.disk_size_mb} MB` : "-"}</td>
                        <td style={{ padding: "6px 8px", fontSize: 11, color: "#6b7280" }}>{s.created_at ? new Date(s.created_at).toLocaleString("zh-CN") : "-"}</td>
                        <td style={{ padding: "6px 8px", textAlign: "center" }}>
                          {s.status === "ready" && (
                            <>
                              {activeSnapshot !== s.data_date && (
                                <button
                                  onClick={() => setActiveSnapshot(s.data_date)}
                                  style={{ padding: "2px 8px", fontSize: 11, borderRadius: 4, border: "1px solid #8b5cf6", background: "#fff", color: "#8b5cf6", cursor: "pointer", marginRight: 4 }}
                                >
                                  使用
                                </button>
                              )}
                              <button
                                onClick={() => deleteSnapshot(s.data_date)}
                                disabled={snapshotDeleting === s.data_date}
                                style={{ padding: "2px 8px", fontSize: 11, borderRadius: 4, border: "1px solid #ef4444", background: "#fff", color: "#ef4444", cursor: "pointer", opacity: snapshotDeleting === s.data_date ? 0.5 : 1 }}
                              >
                                {snapshotDeleting === s.data_date ? "删除中..." : "删除"}
                              </button>
                            </>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          )}
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
            <option value="manual">手工因子</option>
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
            <option value="__empty__">未分类</option>
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
            <option value="__empty__">未评级</option>
            {["S", "A", "B", "C", "D"].map(g => (
              <option key={g} value={g}>{g}</option>
            ))}
          </select>

          <select
            value={availabilityFilter}
            onChange={e => setAvailabilityFilter(e.target.value)}
            title="状态筛选"
            style={{ padding: "6px 10px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db" }}
          >
            <option value="enabled">仅启用</option>
            <option value="all">全部状态</option>
            <option value="disabled">仅禁用</option>
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
                onClick={() => setManualDialogOpen(true)}
                style={{
                  padding: "6px 14px", fontSize: 12, cursor: "pointer",
                  borderRadius: 6, border: "none", background: "#f59e0b", color: "#fff", fontWeight: 600,
                }}
              >+ 手工创建因子</button>

              <button
                onClick={() => batchAnalyze()}
                disabled={batchLoading || actualSelectedFactors.size === 0}
                style={{
                  padding: "6px 14px", fontSize: 12, cursor: (batchLoading || actualSelectedFactors.size === 0) ? "not-allowed" : "pointer",
                  borderRadius: 6, border: "none", background: "#2563eb", color: "#fff", fontWeight: 600,
                  opacity: (batchLoading || actualSelectedFactors.size === 0) ? 0.5 : 1,
                }}
              >
                {batchLoading ? "分析中..." : `批量分析(${actualSelectedFactors.size})`}
              </button>

              <button
                onClick={batchFetchMetrics}
                disabled={metricsLoading || actualSelectedFactors.size === 0}
                style={{
                  padding: "6px 14px", fontSize: 12, cursor: (metricsLoading || actualSelectedFactors.size === 0) ? "not-allowed" : "pointer",
                  borderRadius: 6, border: "none", background: "#059669", color: "#fff", fontWeight: 600,
                  opacity: (metricsLoading || actualSelectedFactors.size === 0) ? 0.5 : 1,
                }}
              >
                {metricsLoading ? "计算中..." : `计算指标(${actualSelectedFactors.size})`}
              </button>

              <button
                onClick={() => {
                  const names = Array.from(actualSelectedFactors).map(key => key.split("||")[0]);
                  setPipelineFactorNames(names);
                  setPipelineTaskIds([]);
                  setPipelineOpen(true);
                }}
                disabled={actualSelectedFactors.size === 0 || pipelineOpen}
                style={{
                  padding: "6px 14px", fontSize: 12, cursor: (actualSelectedFactors.size === 0 || pipelineOpen) ? "not-allowed" : "pointer",
                  borderRadius: 6, border: "none", background: "#7c3aed", color: "#fff", fontWeight: 600,
                  opacity: (actualSelectedFactors.size === 0 || pipelineOpen) ? 0.5 : 1,
                }}
              >
                全流程处理({actualSelectedFactors.size})
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

          {!isSelection && (
            <div style={{ display: "flex", gap: 2, marginLeft: 8, background: "#f3f4f6", borderRadius: 6, padding: 2 }}>
              {(["list", "task"] as const).map(m => (
                <button key={m} onClick={() => setViewMode(m)} style={{
                  padding: "4px 10px", fontSize: 11, borderRadius: 4, border: "none", cursor: "pointer",
                  background: viewMode === m ? "#fff" : "transparent", fontWeight: viewMode === m ? 600 : 400,
                  color: viewMode === m ? "#7c3aed" : "#6b7280", boxShadow: viewMode === m ? "0 1px 2px rgba(0,0,0,0.1)" : "none",
                }}>{m === "list" ? "列表视图" : "按Task分组"}</button>
              ))}
            </div>
          )}

          <span style={{ fontSize: 12, color: "#9ca3af" }}>共 {total} 条  已选 {actualSelectedFactors.size}  第 {page}/{totalPages} 页{mergedFactors.filter(f => !f.is_available).length > 0 && <span style={{ color: "#dc2626", marginLeft: 6 }}>禁用 {mergedFactors.filter(f => !f.is_available).length}</span>}</span>
        </div>

        {/* 批量分析进度 */}
        {batchProgress && (
          <div style={{ marginTop: 8, padding: 10, borderRadius: 6, fontSize: 12, background: "#dbeafe", color: "#1e40af" }}>
            <div style={{ marginBottom: 4 }}>
              <strong>批量分析进行中：</strong> {batchProgress.current} / {batchProgress.total}
              {batchProgress.factor_name && <span style={{ marginLeft: 8, color: "#3b82f6" }}>当前: {batchProgress.factor_name}</span>}
            </div>
            <div style={{ width: "100%", height: 8, background: "#e5e7eb", borderRadius: 4, overflow: "hidden" }}>
              <div style={{ width: `${(batchProgress.current / batchProgress.total) * 100}%`, height: "100%", background: "#2563eb", transition: "width 0.3s" }} />
            </div>
          </div>
        )}

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

        {/* 获取指标结果提示 */}
        {metricsResult && (
          <div style={{
            marginTop: 8, padding: 10, borderRadius: 6, fontSize: 12,
            background: metricsResult.ok ? "#d1fae5" : "#fee2e2",
            color: metricsResult.ok ? "#065f46" : "#991b1b",
          }}>
            {metricsResult.ok ? (
              <span>
                <strong>指标获取完成：</strong>
                新增 {metricsResult.total_metrics_inserted} 条，跳过 {metricsResult.total_metrics_skipped} 条（已存在）
                {(metricsResult as any).fail_count > 0 && (
                  <span style={{ color: "#b45309" }}>（{(metricsResult as any).fail_count} 个任务失败）</span>
                )}
              </span>
            ) : (
              <span><strong>指标获取失败：</strong>{metricsResult.error || ((metricsResult as any).details?.filter((d: any) => !d.ok).map((d: any) => d.errors?.[0]).filter(Boolean).join("; ")) || "未知错误"}</span>
            )}
          </div>
        )}

        {error && <div style={{ marginTop: 8, padding: 8, background: "#fee2e2", borderRadius: 6, fontSize: 12 }}>{error}</div>}
      </section>

      {/* Task分组视图 */}
      {viewMode === "task" && (
        <section style={{ background: "#fff", borderRadius: 12, padding: 16, marginBottom: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 12 }}>
            <strong style={{ fontSize: 13 }}>Source Tasks</strong>
            <span style={{ fontSize: 11, color: "#9ca3af" }}>共 {sourceTasks.length} 个Task</span>
            <button onClick={() => { const all = new Set(sourceTasks.map(t => t.task_id)); setSelectedTasks(all); }}
              style={{ padding: "3px 8px", fontSize: 10, borderRadius: 4, border: "1px solid #d1d5db", background: "#fff", cursor: "pointer" }}>全选</button>
            <button onClick={() => setSelectedTasks(new Set())}
              style={{ padding: "3px 8px", fontSize: 10, borderRadius: 4, border: "1px solid #d1d5db", background: "#fff", cursor: "pointer" }}>清空</button>
            <button
              onClick={computeSelectedTasksMetrics}
              disabled={selectedTasks.size === 0 || taskComputing.size > 0 || pipelineOpen}
              style={{
                padding: "4px 12px", fontSize: 11, borderRadius: 6, border: "none", cursor: selectedTasks.size === 0 || taskComputing.size > 0 || pipelineOpen ? "not-allowed" : "pointer",
                background: "#7c3aed", color: "#fff", fontWeight: 600,
                opacity: selectedTasks.size === 0 || taskComputing.size > 0 || pipelineOpen ? 0.5 : 1,
              }}
            >
              {taskComputing.size > 0 ? `计算中(${taskComputing.size})...` : `计算选中Task的IC指标(${selectedTasks.size})`}
            </button>
            <button
              onClick={() => analyzeSelectedTasksFactors()}
              disabled={selectedTasks.size === 0 || taskAnalyzing || pipelineOpen}
              style={{
                padding: "4px 12px", fontSize: 11, borderRadius: 6, border: "none", cursor: selectedTasks.size === 0 || taskAnalyzing || pipelineOpen ? "not-allowed" : "pointer",
                background: "#2563eb", color: "#fff", fontWeight: 600,
                opacity: selectedTasks.size === 0 || taskAnalyzing || pipelineOpen ? 0.5 : 1,
              }}
            >
              {taskAnalyzing ? "分析中..." : `批量分析(${selectedTasks.size})`}
            </button>
            <button
              onClick={() => {
                const tasks = Array.from(selectedTasks);
                if (tasks.length === 0) return;
                setPipelineTaskIds(tasks);
                setPipelineOpen(true);
              }}
              disabled={selectedTasks.size === 0 || taskComputing.size > 0 || taskAnalyzing || pipelineOpen}
              style={{
                padding: "4px 12px", fontSize: 11, borderRadius: 6, border: "none",
                cursor: selectedTasks.size === 0 || taskComputing.size > 0 || taskAnalyzing || pipelineOpen ? "not-allowed" : "pointer",
                background: "linear-gradient(135deg, #7c3aed, #2563eb)", color: "#fff", fontWeight: 600,
                opacity: selectedTasks.size === 0 || taskComputing.size > 0 || taskAnalyzing || pipelineOpen ? 0.5 : 1,
              }}
            >
              全流程处理({selectedTasks.size})
            </button>
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {sourceTasks.map(t => {
              const sel = selectedTasks.has(t.task_id);
              const computing = taskComputing.has(t.task_id);
              const result = taskResults[t.task_id];
              const done = t.ok_count > 0 || t.skipped_count > 0 || t.error_count > 0;
              const isExp = expandedTask === t.task_id;
              const factors = taskFactors[t.task_id];
              const fLoading = taskFactorsLoading.has(t.task_id);
              return (
                <div key={t.task_id} style={{
                  border: isExp ? "2px solid #2563eb" : sel ? "2px solid #7c3aed" : done ? "1px solid #86efac" : "1px solid #e5e7eb",
                  borderRadius: 8, overflow: "hidden",
                  background: isExp ? "#f0f7ff" : sel ? "#faf5ff" : done ? "#f0fdf4" : "#fff",
                  transition: "all 0.15s",
                }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 8, padding: 12, cursor: "pointer" }}
                    onClick={() => toggleExpandTask(t.task_id)}>
                    <input type="checkbox" checked={sel} style={{ accentColor: "#7c3aed" }}
                      onClick={e => { e.stopPropagation(); toggleTask(t.task_id); }} readOnly />
                    <span style={{ fontSize: 12, color: isExp ? "#2563eb" : "#6b7280" }}>{isExp ? "▼" : "▶"}</span>
                    <span style={{ fontFamily: "monospace", fontSize: 11, fontWeight: 600, color: "#374151", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                      title={t.task_id}>{t.task_id}</span>
                    <span style={{ fontSize: 11, color: "#6b7280", whiteSpace: "nowrap" }}>因子: <strong>{t.factor_count}</strong></span>
                    {(t.ok_count > 0 || t.skipped_count > 0 || t.error_count > 0) ? (
                      <span style={{ fontSize: 11, whiteSpace: "nowrap" }}>
                        <span style={{ color: "#059669" }}>✅{t.ok_count}</span>
                        {t.skipped_count > 0 && <span style={{ color: "#d97706", marginLeft: 4 }}>⚠️{t.skipped_count}</span>}
                        {t.error_count > 0 && <span style={{ color: "#dc2626", marginLeft: 4 }}>❌{t.error_count}</span>}
                      </span>
                    ) : (
                      <span style={{ fontSize: 11, color: "#9ca3af", whiteSpace: "nowrap" }}>未计算</span>
                    )}
                    {computing && <span style={{ fontSize: 11, color: "#7c3aed" }}>计算中...</span>}
                    {result && <span style={{ fontSize: 11, color: result.ok ? "#059669" : "#dc2626", fontWeight: 600 }}>{result.msg}</span>}
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        setPipelineTaskIds([t.task_id]);
                        setPipelineOpen(true);
                      }}
                      disabled={pipelineOpen}
                      style={{
                        padding: "2px 8px", fontSize: 10, borderRadius: 4,
                        border: "1px solid #7c3aed", background: pipelineOpen ? "#f3f4f6" : "#faf5ff",
                        color: pipelineOpen ? "#9ca3af" : "#7c3aed", fontWeight: 600,
                        cursor: pipelineOpen ? "not-allowed" : "pointer",
                        whiteSpace: "nowrap",
                      }}
                    >
                      全流程
                    </button>
                  </div>
                  {isExp && (
                    <div style={{ borderTop: "1px solid #e5e7eb", padding: 12, background: "#fff" }}>
                      {fLoading && <div style={{ textAlign: "center", padding: 16, color: "#9ca3af", fontSize: 12 }}>加载因子列表...</div>}
                      {!fLoading && factors && factors.length === 0 && <div style={{ textAlign: "center", padding: 16, color: "#9ca3af", fontSize: 12 }}>该Task无因子</div>}
                      {!fLoading && factors && factors.length > 0 && (
                        <div style={{ overflowX: "auto" }}>
                          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                            <thead>
                              <tr style={{ borderBottom: "1px solid #e5e7eb", textAlign: "left" }}>
                                <th style={{ padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>因子名称</th>
                                <th style={{ padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>类型</th>
                                <th style={{ padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>IC</th>
                                <th style={{ padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>Sharpe</th>
                                <th style={{ padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>年化</th>
                                <th style={{ padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>SOTA</th>
                                <th style={{ padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>描述</th>
                              </tr>
                            </thead>
                            <tbody>
                              {factors.map((fac: any) => (
                                <tr key={fac.name} style={{ borderBottom: "1px solid #f3f4f6" }}>
                                  <td style={{ padding: "4px 8px", fontFamily: "monospace", fontWeight: 600, color: "#374151" }}>{fac.name}</td>
                                  <td style={{ padding: "4px 8px", color: "#6b7280" }}>{fac.factor_type || "-"}</td>
                                  <td style={{ padding: "4px 8px", color: "#374151" }}>{fac.ic != null ? fac.ic.toFixed(4) : "-"}</td>
                                  <td style={{ padding: "4px 8px", color: "#374151" }}>{fac.sharpe != null ? fac.sharpe.toFixed(3) : "-"}</td>
                                  <td style={{ padding: "4px 8px", color: "#374151" }}>{fac.annualized_return != null ? (fac.annualized_return * 100).toFixed(2) + "%" : "-"}</td>
                                  <td style={{ padding: "4px 8px" }}>{fac.is_sota_factor ? "✓" : ""}</td>
                                  <td style={{ padding: "4px 8px", color: "#6b7280", maxWidth: 300, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{fac.description_cn || "-"}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      )}

                      {/* 因子×窗口计算状态矩阵 */}
                      {(() => {
                        const detail = taskCalcDetail[t.task_id];
                        const dLoading = calcDetailLoading.has(t.task_id);
                        if (dLoading) return <div style={{ textAlign: "center", padding: 12, color: "#9ca3af", fontSize: 11 }}>加载计算日志...</div>;
                        if (!detail || detail.factors.length === 0) return <div style={{ textAlign: "center", padding: 12, color: "#9ca3af", fontSize: 11 }}>暂无计算日志</div>;
                        const winLabels: Record<string, string> = { full: "全量", out_sample: "样本外", recent_6m: "近6月", recent_3m: "近3月" };
                        const winKeys = ["full", "out_sample", "recent_6m", "recent_3m"];
                        return (
                          <div style={{ marginTop: 12, borderTop: "1px solid #e5e7eb", paddingTop: 10 }}>
                            <div style={{ fontSize: 12, fontWeight: 600, color: "#374151", marginBottom: 6 }}>
                              计算状态矩阵
                              <span style={{ fontSize: 10, fontWeight: 400, color: "#9ca3af", marginLeft: 8 }}>
                                ✅{detail.summary.ok_count} ⚠️{detail.summary.skipped_count} ❌{detail.summary.error_count}
                              </span>
                            </div>
                            <div style={{ overflowX: "auto" }}>
                              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                                <thead>
                                  <tr style={{ borderBottom: "1px solid #e5e7eb", textAlign: "center" }}>
                                    <th style={{ padding: "4px 8px", textAlign: "left", color: "#6b7280", fontWeight: 500 }}>因子名称</th>
                                    {winKeys.map(w => <th key={w} style={{ padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>{winLabels[w]}</th>)}
                                  </tr>
                                </thead>
                                <tbody>
                                  {detail.factors.map(f => {
                                    const winMap: Record<string, CalcWindow> = {};
                                    f.windows.forEach(w => { winMap[w.eval_window] = w; });
                                    return (
                                      <tr key={f.factor_name} style={{ borderBottom: "1px solid #f3f4f6" }}>
                                        <td style={{ padding: "4px 8px", fontFamily: "monospace", fontWeight: 600, color: "#374151" }}>{f.factor_name}</td>
                                        {winKeys.map(wk => {
                                          const w = winMap[wk];
                                          if (!w) return <td key={wk} style={{ padding: "4px 8px", textAlign: "center", color: "#d1d5db" }}>-</td>;
                                          const icon = w.status === "ok" ? "✅" : w.status === "skipped" ? "⚠️" : w.status === "error" ? "❌" : "-";
                                          const tip = w.status === "ok"
                                            ? `${w.n_trading_days ?? 0}天 (${w.data_start || "?"} ~ ${w.data_end || "?"})`
                                            : w.error_message || `实际${w.n_trading_days ?? 0}天, 需要${w.required_days ?? 0}天`;
                                          return (
                                            <td key={wk} style={{ padding: "4px 8px", textAlign: "center", cursor: "help" }} title={tip}>
                                              {icon}
                                            </td>
                                          );
                                        })}
                                      </tr>
                                    );
                                  })}
                                </tbody>
                              </table>
                            </div>
                          </div>
                        );
                      })()}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
          {sourceTasks.length === 0 && <div style={{ textAlign: "center", padding: 30, color: "#9ca3af", fontSize: 12 }}>暂无 Source Task 数据</div>}

          {/* Task分析结果提示 */}
          {taskAnalyzeResult && (
            <div style={{
              marginTop: 12, padding: 10, borderRadius: 6, fontSize: 12,
              background: (taskAnalyzeResult.errors?.length || 0) > 0 ? "#fef3c7" : "#d1fae5",
              color: (taskAnalyzeResult.errors?.length || 0) > 0 ? "#92400e" : "#065f46",
            }}>
              <strong>因子分析完成：</strong>
              共 {taskAnalyzeResult.total} 个因子，成功分析 {taskAnalyzeResult.analyzed} 个
              {(taskAnalyzeResult.errors?.length || 0) > 0 && (
                <span>，{taskAnalyzeResult.errors!.length} 个失败</span>
              )}
            </div>
          )}
        </section>
      )}

      {/* 统一数据表格 */}
      {viewMode === "list" && <section style={{ background: "#fff", borderRadius: 12, padding: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
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
                <th style={{ ...thStyle, maxWidth: 180, cursor: "pointer" }} onClick={() => handleSort("factor_name")}>因子名称{getSortIndicator("factor_name")}</th>
                <th style={{ ...thStyle, cursor: "pointer" }} onClick={() => handleSort("source")}>来源{getSortIndicator("source")}</th>
                <th style={{ ...thStyle, cursor: "pointer" }} onClick={() => handleSort("factor_dimension")}>维度{getSortIndicator("factor_dimension")}</th>
                <th style={{ ...thStyle, cursor: "pointer" }} onClick={() => handleSort("category")}>类别{getSortIndicator("category")}</th>
                <th style={{ ...thStyle, cursor: "pointer" }} onClick={() => handleSort("grade")}>评级{getSortIndicator("grade")}</th>
                <th style={{ ...thStyle, width: 50 }}>状态</th>
                <th style={{ ...thStyle, cursor: "pointer" }} onClick={() => handleSort("ind_ic")}>IC(独立){getSortIndicator("ind_ic")}</th>
                <th style={{ ...thStyle, cursor: "pointer" }} onClick={() => handleSort("ind_rank_ic")}>RankIC(独立){getSortIndicator("ind_rank_ic")}</th>
                <th style={{ ...thStyle, cursor: "pointer" }} onClick={() => handleSort("ind_sharpe")}>Sharpe(独立){getSortIndicator("ind_sharpe")}</th>
                <th style={{ ...thStyle, cursor: "pointer" }} onClick={() => handleSort("ind_annual_return")}>年化(独立){getSortIndicator("ind_annual_return")}</th>
                <th style={{ ...thStyle, cursor: "pointer" }} onClick={() => handleSort("has_ind_metrics")}>独立指标{getSortIndicator("has_ind_metrics")}</th>
                <th style={{ ...thStyle, cursor: "pointer", width: 50 }} onClick={() => handleSort("decay_status")}>衰变{getSortIndicator("decay_status")}</th>
                <th style={{ ...thStyle, cursor: "pointer", width: 90 }} onClick={() => handleSort("ind_calculated_at")}>指标计算{getSortIndicator("ind_calculated_at")}</th>
                <th style={{ ...thStyle, cursor: "pointer", width: 80 }} onClick={() => handleSort("generated_at_utc")}>入库时间{getSortIndicator("generated_at_utc")}</th>
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
                        background: !f.is_available ? "#f9fafb" : isSelected ? (isSelection ? "#eff6ff" : "#faf5ff") : undefined,
                        opacity: !f.is_available ? 0.7 : 1,
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
                          background: f.source === "rdagent_task_sync" ? "#ede9fe" : f.source === "manual" ? "#fef3c7" : f.source === "alpha158" ? "#dbeafe" : "#d1fae5",
                          color: f.source === "rdagent_task_sync" ? "#7c3aed" : f.source === "manual" ? "#d97706" : f.source === "alpha158" ? "#2563eb" : "#059669",
                        }}>
                          {f.source === "rdagent_task_sync" ? "SOTA" : f.source === "manual" ? "手工" : f.source}
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
                      <td style={tdStyle}>
                        {f.is_available ? (
                          <span style={{ padding: "2px 6px", borderRadius: 4, fontSize: 9, fontWeight: 600, background: "#d1fae5", color: "#059669" }}>启用</span>
                        ) : (
                          <span style={{ padding: "2px 6px", borderRadius: 4, fontSize: 9, fontWeight: 600, background: "#fee2e2", color: "#dc2626" }}>禁用</span>
                        )}
                      </td>
                      <td style={tdStyle}>
                        <span
                          title={f.ind_ic != null ? `独立 IC: ${f.ind_ic.toFixed(4)}` : undefined}
                          style={{ color: f.ind_ic != null ? ((f.ind_ic ?? 0) > 0 ? "#059669" : "#dc2626") : "#9ca3af" }}
                        >
                          {f.ind_ic != null ? f.ind_ic.toFixed(4) : "-"}
                        </span>
                      </td>
                      <td style={tdStyle}>
                        <span
                          title={f.ind_rank_ic != null ? `独立 Rank IC: ${f.ind_rank_ic.toFixed(4)}` : undefined}
                          style={{ color: f.ind_rank_ic != null ? ((f.ind_rank_ic ?? 0) > 0 ? "#059669" : "#dc2626") : "#9ca3af" }}
                        >
                          {f.ind_rank_ic != null ? f.ind_rank_ic.toFixed(4) : "-"}
                        </span>
                      </td>
                      <td style={tdStyle}>
                        <span style={{ color: f.ind_sharpe != null ? "#2563eb" : "#9ca3af" }}>
                          {f.ind_sharpe != null ? f.ind_sharpe.toFixed(3) : "-"}
                        </span>
                      </td>
                      <td style={tdStyle}>
                        <span style={{ color: f.ind_annual_return != null ? ((f.ind_annual_return ?? 0) > 0 ? "#059669" : "#dc2626") : "#9ca3af" }}>
                          {f.ind_annual_return != null ? (f.ind_annual_return * 100).toFixed(1) + "%" : "-"}
                        </span>
                      </td>
                      <td style={tdStyle}>
                        {f.has_ind_metrics ? (
                          <span style={{ padding: "2px 6px", borderRadius: 4, fontSize: 9, fontWeight: 600, background: "#d1fae5", color: "#059669" }}>已计算</span>
                        ) : (
                          <span style={{ padding: "2px 6px", borderRadius: 4, fontSize: 9, background: "#f3f4f6", color: "#9ca3af" }}>未计算</span>
                        )}
                      </td>
                      <td style={tdStyle}>
                        {f.decay_status === "danger" ? (
                          <span title={`近1月 RankIC=${f.ind_rank_ic_1m?.toFixed(4)}, 全量=${f.ind_rank_ic?.toFixed(4)}`} style={{ padding: "2px 6px", borderRadius: 4, fontSize: 9, fontWeight: 600, background: "#fef2f2", color: "#dc2626" }}>失效</span>
                        ) : f.decay_status === "warning" ? (
                          <span title={`近1月 RankIC=${f.ind_rank_ic_1m?.toFixed(4)}, 全量=${f.ind_rank_ic?.toFixed(4)}`} style={{ padding: "2px 6px", borderRadius: 4, fontSize: 9, fontWeight: 600, background: "#fef3c7", color: "#d97706" }}>衰减</span>
                        ) : f.decay_status === "ok" ? (
                          <span title={`近1月 RankIC=${f.ind_rank_ic_1m?.toFixed(4)}, 全量=${f.ind_rank_ic?.toFixed(4)}`} style={{ padding: "2px 6px", borderRadius: 4, fontSize: 9, background: "#ecfdf5", color: "#059669" }}>正常</span>
                        ) : (
                          <span style={{ color: "#d1d5db", fontSize: 9 }}>-</span>
                        )}
                      </td>
                      <td style={{ ...tdStyle, fontSize: 10, color: f.ind_calculated_at ? "#64748b" : "#d1d5db", whiteSpace: "nowrap" }} title={f.ind_calculated_at || undefined}>
                        {f.ind_calculated_at ? f.ind_calculated_at.slice(0, 16).replace("T", " ") : "-"}
                      </td>
                      <td style={{ ...tdStyle, fontSize: 10, color: "#94a3b8", whiteSpace: "nowrap" }}>{f.generated_at_utc ? f.generated_at_utc.slice(0, 10) : "-"}</td>
                      <td style={tdStyle}>
                        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                        {(f.description || f.classification_reason || f.source === "rdagent_task_sync" || f.source === "manual") ? (
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
                        {!isSelection && (
                          <span
                            onClick={(e) => { e.stopPropagation(); deleteFactor(f.factor_name, f.source); }}
                            title="删除此因子及所有关联数据"
                            style={{
                              color: "#dc2626", cursor: "pointer", fontSize: 10,
                              opacity: 0.6, userSelect: "none",
                            }}
                            onMouseEnter={e => (e.currentTarget.style.opacity = "1")}
                            onMouseLeave={e => (e.currentTarget.style.opacity = "0.6")}
                          >删除</span>
                        )}
                        </div>
                      </td>
                    </tr>
                    {isExpanded && (() => {
                      const detail = factorDetails[rowKey];
                      const isDetailLoading = detailLoading.has(rowKey);
                      const isCodeOpen = codeExpanded.has(rowKey);

                      return (
                      <tr style={{ borderBottom: "1px solid #f3f4f6" }}>
                        <td colSpan={13} style={{ padding: "0 10px 10px 10px" }}>
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

                            {/* TASK组合指标(参考) */}
                            {(f.ic != null || f.sharpe != null || f.annualized_return != null) && (
                              <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #e5e7eb" }}>
                                <strong style={{ color: "#9ca3af", fontSize: 11 }}>TASK组合指标(参考)</strong>
                                <div style={{ display: "flex", gap: 12, marginTop: 4, fontSize: 11, color: "#6b7280" }}>
                                  <span>IC: <strong>{f.ic != null ? f.ic.toFixed(4) : "-"}</strong></span>
                                  <span>Sharpe: <strong>{f.sharpe != null ? f.sharpe.toFixed(3) : "-"}</strong></span>
                                  <span>年化: <strong>{f.annualized_return != null ? (f.annualized_return * 100).toFixed(1) + "%" : "-"}</strong></span>
                                </div>
                              </div>
                            )}

                            {/* 独立因子指标（17项） */}
                            {(() => {
                              const indMetrics = factorIndMetrics[rowKey];
                              if (!indMetrics || indMetrics.length === 0) return null;

                              // 取 full 窗口的最新一条作为主指标
                              const fullM = indMetrics.find((m: any) => m.eval_window === "full") || indMetrics[0];
                              const windows = indMetrics.reduce((acc: Record<string, any>, m: any) => { acc[m.eval_window] = m; return acc; }, {});

                              return (
                                <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #e5e7eb" }}>
                                  <strong style={{ color: "#059669", fontSize: 11 }}>独立因子指标</strong>
                                  <span style={{ fontSize: 10, color: "#9ca3af", marginLeft: 8 }}>
                                    数据区间: {fullM.data_start} ~ {fullM.data_end}
                                  </span>

                                  {/* 核心指标卡片 */}
                                  <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(110px, 1fr))", gap: 5, marginTop: 6 }}>
                                    {[
                                      { label: "IC均值", value: fmtNum(fullM.ic_mean), color: (fullM.ic_mean ?? 0) > 0 ? "#059669" : "#dc2626" },
                                      { label: "Rank IC", value: fmtNum(fullM.rank_ic_mean), color: (fullM.rank_ic_mean ?? 0) > 0 ? "#059669" : "#dc2626" },
                                      { label: "ICIR", value: fmtNum(fullM.icir, 2), color: "#2563eb" },
                                      { label: "Rank ICIR", value: fmtNum(fullM.rank_icir, 2), color: "#2563eb" },
                                      { label: "IC胜率", value: fmtPct(fullM.ic_positive_ratio), color: "#7c3aed" },
                                      { label: "多头年化", value: fmtPct(fullM.top_annual_return), color: (fullM.top_annual_return ?? 0) > 0 ? "#059669" : "#dc2626" },
                                      { label: "超额年化", value: fmtPct(fullM.top_excess_annual_return), color: (fullM.top_excess_annual_return ?? 0) > 0 ? "#059669" : "#dc2626" },
                                      { label: "多头夏普", value: fmtNum(fullM.top_sharpe, 2), color: "#0891b2" },
                                      { label: "最大回撤", value: fmtPct(fullM.top_max_drawdown), color: "#dc2626" },
                                      { label: "超额夏普", value: fmtNum(fullM.top_excess_sharpe, 2), color: "#0891b2" },
                                      { label: "基准年化", value: fmtPct(fullM.benchmark_annual_return), color: "#6b7280" },
                                      { label: "单调性", value: fmtNum(fullM.group_return_monotonicity, 2), color: "#6366f1" },
                                      { label: "换手率", value: fmtNum(fullM.turnover, 4), color: "#d97706" },
                                      { label: "IC半衰期", value: fullM.ic_decay_half_life != null ? fullM.ic_decay_half_life.toFixed(1) + "天" : "-", color: "#0891b2" },
                                      { label: "覆盖率", value: fmtPct(fullM.coverage), color: "#7c3aed" },
                                      { label: "交易日数", value: fullM.n_trading_days ?? "-", color: "#374151" },
                                    ].map((item, idx) => (
                                      <div key={idx} style={{
                                        background: "#f0fdf4", borderRadius: 5, padding: "4px 6px",
                                        border: "1px solid #d1fae5", textAlign: "center",
                                      }}>
                                        <div style={{ fontSize: 9, color: "#6b7280" }}>{item.label}</div>
                                        <div style={{ fontSize: 12, fontWeight: 700, color: item.color, marginTop: 1 }}>{item.value}</div>
                                      </div>
                                    ))}
                                  </div>

                                  {/* 多持有期 Rank IC */}
                                  {(fullM.rank_ic_1d != null || fullM.rank_ic_5d != null || fullM.rank_ic_10d != null || fullM.rank_ic_20d != null) && (
                                    <div style={{ display: "flex", gap: 6, marginTop: 6, alignItems: "center" }}>
                                      <span style={{ fontSize: 10, color: "#6b7280", fontWeight: 600, whiteSpace: "nowrap" }}>多持有期RankIC:</span>
                                      {[
                                        { label: "1天", value: fullM.rank_ic_1d },
                                        { label: "5天", value: fullM.rank_ic_5d },
                                        { label: "10天", value: fullM.rank_ic_10d },
                                        { label: "20天", value: fullM.rank_ic_20d },
                                      ].map((item, idx) => (
                                        <div key={idx} style={{
                                          background: "#eff6ff", borderRadius: 4, padding: "3px 8px",
                                          border: "1px solid #dbeafe", textAlign: "center", minWidth: 60,
                                        }}>
                                          <div style={{ fontSize: 9, color: "#6b7280" }}>{item.label}</div>
                                          <div style={{
                                            fontSize: 12, fontWeight: 700, marginTop: 1,
                                            color: item.value != null ? ((item.value ?? 0) > 0 ? "#059669" : "#dc2626") : "#9ca3af",
                                          }}>
                                            {item.value != null ? item.value.toFixed(4) : "-"}
                                          </div>
                                        </div>
                                      ))}
                                      {fullM.ic_csz_mean != null && (
                                        <div style={{
                                          background: "#fef3c7", borderRadius: 4, padding: "3px 8px",
                                          border: "1px solid #fde68a", textAlign: "center", minWidth: 70, marginLeft: 8,
                                        }}>
                                          <div style={{ fontSize: 9, color: "#92400e" }}>IC(截面)</div>
                                          <div style={{
                                            fontSize: 12, fontWeight: 700, marginTop: 1,
                                            color: (fullM.ic_csz_mean ?? 0) > 0 ? "#059669" : "#dc2626",
                                          }}>
                                            {fullM.ic_csz_mean.toFixed(4)}
                                          </div>
                                        </div>
                                      )}
                                    </div>
                                  )}

                                  {/* 多窗口对比 */}
                                  {Object.keys(windows).length > 1 && (
                                    <div style={{ marginTop: 6, overflowX: "auto" }}>
                                      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10 }}>
                                        <thead>
                                          <tr style={{ background: "#f0fdf4", borderBottom: "1px solid #d1fae5" }}>
                                            {["窗口", "IC均值", "Rank IC", "ICIR", "多头年化", "超额年化", "多头夏普", "最大回撤", "单调性"].map(h => (
                                              <th key={h} style={{ padding: "3px 5px", fontWeight: 600, whiteSpace: "nowrap", textAlign: "left" }}>{h}</th>
                                            ))}
                                          </tr>
                                        </thead>
                                        <tbody>
                                          {["full", "out_sample", "recent_6m", "recent_3m", "recent_1m"].filter(w => windows[w]).map(w => {
                                            const wm = windows[w];
                                            const wLabels: Record<string, string> = { full: "全量", out_sample: "样本外", recent_6m: "近6月", recent_3m: "近3月", recent_1m: "近1月" };
                                            return (
                                              <tr key={w} style={{ borderBottom: "1px solid #f3f4f6" }}>
                                                <td style={{ padding: "2px 5px", fontWeight: 600 }}>{wLabels[w] || w}</td>
                                                <td style={{ padding: "2px 5px", color: (wm.ic_mean ?? 0) > 0 ? "#059669" : "#dc2626" }}>{fmtNum(wm.ic_mean)}</td>
                                                <td style={{ padding: "2px 5px", color: (wm.rank_ic_mean ?? 0) > 0 ? "#059669" : "#dc2626" }}>{fmtNum(wm.rank_ic_mean)}</td>
                                                <td style={{ padding: "2px 5px" }}>{fmtNum(wm.icir, 2)}</td>
                                                <td style={{ padding: "2px 5px", color: (wm.top_annual_return ?? 0) > 0 ? "#059669" : "#dc2626" }}>{fmtPct(wm.top_annual_return)}</td>
                                                <td style={{ padding: "2px 5px", color: (wm.top_excess_annual_return ?? 0) > 0 ? "#059669" : "#dc2626" }}>{fmtPct(wm.top_excess_annual_return)}</td>
                                                <td style={{ padding: "2px 5px" }}>{fmtNum(wm.top_sharpe, 2)}</td>
                                                <td style={{ padding: "2px 5px", color: "#dc2626" }}>{fmtPct(wm.top_max_drawdown)}</td>
                                                <td style={{ padding: "2px 5px" }}>{fmtNum(wm.group_return_monotonicity, 2)}</td>
                                              </tr>
                                            );
                                          })}
                                        </tbody>
                                      </table>
                                    </div>
                                  )}
                                </div>
                              );
                            })()}

                            {/* IC 衰变趋势图 */}
                            {(() => {
                              const fname = f.factor_name;
                              if (!fname) return null;
                              return (
                                <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #e5e7eb" }}>
                                  <strong style={{ color: "#6366f1", fontSize: 11 }}>IC 衰变趋势</strong>
                                  <span style={{ fontSize: 10, color: "#9ca3af", marginLeft: 8 }}>
                                    不同快照时间点的 IC/ICIR 变化
                                  </span>
                                  <ICDecayTrendChart factorName={fname} evalWindow="full" />
                                </div>
                              );
                            })()}

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
      </section>}

      {pipelineOpen && (
        <FullPipelineDialog
          open={pipelineOpen}
          taskIds={pipelineTaskIds.length > 0 ? pipelineTaskIds : undefined}
          factorNames={pipelineFactorNames.length > 0 ? pipelineFactorNames : undefined}
          dataDate={activeSnapshot || undefined}
          onClose={() => setPipelineOpen(false)}
          onComplete={() => {
            loadData();
            loadSourceTasks();
            loadIndSummary();
          }}
        />
      )}

      <ManualFactorDialog
        open={manualDialogOpen}
        onClose={() => setManualDialogOpen(false)}
        onCreated={() => {
          loadData();
          loadSourceTasks();
          loadIndSummary();
        }}
      />
    </div>
  );
}
