"use client";

import React, { useEffect, useState, useCallback, useMemo, useRef } from "react";
import FullPipelineDialog from "./FullPipelineDialog";
import ManualFactorDialog from "./ManualFactorDialog";
import IcSeriesChart from "./charts/IcSeriesChart";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";
const FACTOR_CACHE_DEFAULT_START = "2018-08-01";
const FACTOR_CACHE_DEFAULT_END = "2026-04-30";
const FACTOR_CACHE_WARMUP_TOLERANCE_DAYS = 60;

type FactorCacheCoverageInput = {
  cache_date_range?: string | null;
  cache_end_date?: string | null;
  cache_window_train_start?: string | null;
};

function factorCacheCoversRequestedWindow(
  factor: FactorCacheCoverageInput,
  targetStart?: string | null,
  targetEnd?: string | null,
): boolean {
  if (!targetStart || !targetEnd || !factor.cache_date_range?.includes("~")) return true;
  const [cacheStart, cacheEndFromRange] = factor.cache_date_range.split("~");
  const cacheEnd = factor.cache_end_date || cacheEndFromRange;
  if (!cacheEnd || cacheEnd < targetEnd) return false;
  if (factor.cache_window_train_start && factor.cache_window_train_start <= targetStart) return true;
  if (cacheStart <= targetStart) return true;
  const gapDays = Math.round((new Date(cacheStart).getTime() - new Date(targetStart).getTime()) / 86400000);
  return gapDays <= FACTOR_CACHE_WARMUP_TOLERANCE_DAYS;
}

async function fetchJsonOrThrow(url: string, init?: RequestInit) {
  const res = await fetch(url, init);
  const text = await res.text();
  let data: any = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      if (!res.ok) throw new Error(`HTTP ${res.status}: ${text.slice(0, 200)}`);
      throw new Error(`响应不是有效 JSON: ${text.slice(0, 200)}`);
    }
  }
  if (!res.ok) {
    const detail = typeof data?.detail === "string" ? data.detail : data?.message || text;
    throw new Error(detail ? `HTTP ${res.status}: ${detail}` : `HTTP ${res.status}`);
  }
  return data;
}

// ── 月频 IC 衰变趋势面板 ──
function MonthlyIcPanel({ factorName, apiBase }: { factorName: string; apiBase: string }) {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!factorName) return;
    setLoading(true);
    setError(null);
    fetch(`${apiBase}/quantevolver/official-evaluation/factors/${factorName}/monthly-ic`)
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(d => {
        setData(d);
        if (d.count === 0) setError(d.message || "无数据");
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, [factorName, apiBase]);

  if (loading) return <div style={{ padding: 12, fontSize: 12, color: "#6b7280" }}>加载月频IC数据...</div>;
  if (error) return <div style={{ padding: 12, fontSize: 12, color: "#b45309" }}>{error}</div>;
  if (!data || !data.series || data.series.length === 0) return null;

  const months = data.series.map((s: any) => s.month_end);
  const icMean = data.series.map((s: any) => s.ic_mean);
  const rankIcMean = data.series.map((s: any) => s.rank_ic_mean);
  const ewma6m = data.series.map((s: any) => s.ic_ewma_6m);

  return (
    <section style={{ background: "#fff", borderRadius: 12, padding: 16, marginBottom: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
      <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 8 }}>
        IC 月度衰变趋势 — {factorName}
        <span style={{ fontSize: 11, color: "#6b7280", marginLeft: 8 }}>({data.count} 个月)</span>
      </div>
      <IcSeriesChart
        dates={months}
        ic_series={icMean}
        rank_ic_series={rankIcMean}
        ic_rolling_30d_mean={ewma6m}
      />
    </section>
  );
}

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
  ind_rank_ic_1d?: number | null;
  ind_rank_ic_5d?: number | null;
  ind_rank_ic_10d?: number | null;
  ind_rank_ic_20d?: number | null;
  ind_rank_ic_best_abs?: number | null;
  ind_icir?: number | null;
  official_grade?: string | null;
  official_score?: number | null;
  official_grade_reason_structured?: { summary?: string; failed_gates?: string[]; core_ic?: number; holding_period_class?: string } | null;
  official_rule_version?: string | null;
  official_llm_audit_summary?: string | null;
  official_llm_risk_notes?: string[] | null;
  has_cache?: boolean;
  cache_date_range?: string | null;
  cache_start_date?: string | null;
  cache_end_date?: string | null;
  cache_computed_at?: string | null;
  cache_as_of_date?: string | null;
  cache_window_train_start?: string | null;
  cache_window_backtest_end?: string | null;
  cache_source?: string | null;
  cache_source_label?: string | null;
  cache_data_source_mode?: string | null;
  cache_coverage_status?: "covered" | "partial" | "hash_mismatch" | "no_cache" | "error" | null;
  cache_status?: string | null;
  cache_size_mb?: number | null;
  cache_hash_match?: boolean | null;
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
  official_grade?: string | null;
  official_grade_reason?: string | null;
  classification_reason?: string;
  factor_dimension?: string;
  description?: string;
  classification_id?: number;
  // v2 分类维度
  ts_info_density?: string | null;
  cross_horizon_consistency?: number | null;
  direction?: number | null;
  signal_mechanism?: string | null;
  sector_exposure_corr?: number | null;
  horizon_class?: string | null;
  best_horizon?: number | null;
  best_horizon_advantage?: number | null;
  linearity?: string | null;
  holding_period_class?: string | null;
  data_source_group?: string | null;
  update_freq?: string | null;
  ic_sign_consistency_12m?: number | null;
  ic_oos_is_ratio?: number | null;
  monthly_ic_trend_slope?: number | null;
  cluster_id?: number | null;
  cluster_role?: string | null;
  cluster_size?: number | null;
  intra_cluster_max_corr?: number | null;
  representative_score?: number | null;
  ind_ic?: number | null;
  ind_rank_ic?: number | null;
  ind_rank_ic_1d?: number | null;
  ind_rank_ic_5d?: number | null;
  ind_rank_ic_10d?: number | null;
  ind_rank_ic_20d?: number | null;
  ind_rank_ic_best_abs?: number | null;
  ind_rank_ic_1m?: number | null;
  ind_sharpe?: number | null;
  ind_annual_return?: number | null;
  has_ind_metrics?: boolean;
  ind_calculated_at?: string | null;
  generated_at_utc?: string | null;
  decay_status?: "ok" | "warning" | "danger" | null;
  official_score?: number | null;
  official_rule_version?: string | null;
  official_grade_reason_structured?: { summary?: string; failed_gates?: string[]; core_ic?: number; holding_period_class?: string } | null;
  llm_audit_summary?: string | null;
  llm_risk_notes?: string[] | null;
  // 因子值缓存
  has_cache?: boolean;
  cache_date_range?: string | null;
  cache_start_date?: string | null;
  cache_end_date?: string | null;
  cache_computed_at?: string | null;
  cache_as_of_date?: string | null;
  cache_window_train_start?: string | null;
  cache_window_backtest_end?: string | null;
  cache_source?: string | null;
  cache_source_label?: string | null;
  cache_data_source_mode?: string | null;
  cache_coverage_status?: "covered" | "partial" | "hash_mismatch" | "no_cache" | "error" | null;
  cache_status?: string | null;
  cache_size_mb?: number | null;
  cache_hash_match?: boolean | null;
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
  asset_path?: string;
  description_cn?: string;
  performance_metrics?: Record<string, any>;
  llm_audit_summary?: string | null;
  llm_risk_notes?: string[] | null;
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
  cacheContext,
}: {
  mode?: "display" | "selection";
  selectedFactors?: Set<string>;
  onFactorSelect?: (selected: Set<string>) => void;
  cacheContext?: {
    experimentId?: string | null;
    trainStart?: string | null;
    backtestEnd?: string | null;
  };
}) {
  const [factors, setFactors] = useState<Factor[]>([]);
  const loadDataRequestRef = useRef(0);
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
  const [ratingRules, setRatingRules] = useState<Array<{ rule_version: string; version_name: string; status: string; description_md?: string }>>([]);
  const [activeRatingVersion, setActiveRatingVersion] = useState<string>("");
  const [selectedRatingVersion, setSelectedRatingVersion] = useState<string>("");
  const [ratingRuleDetail, setRatingRuleDetail] = useState<any | null>(null);
  const [ratingRunLoading, setRatingRunLoading] = useState(false);
  const [ratingRunResult, setRatingRunResult] = useState<{ ok?: boolean; run_id?: string; total_factors?: number; success_count?: number; failed_count?: number; errors?: { factor_name: string; error: string }[] } | null>(null);
  const [ratingRuns, setRatingRuns] = useState<any[]>([]);
  const [ratingResultsPreview, setRatingResultsPreview] = useState<any[]>([]);
  // 一键流水线模式配置
  const [runMode, setRunMode] = useState<"rating_only" | "full_pipeline">("rating_only");
  const [pipelineParallelism, setPipelineParallelism] = useState<number>(4);
  const [pipelineEnableLlmAnalysis, setPipelineEnableLlmAnalysis] = useState<boolean>(true);
  const [pipelineEnableLlmAudit, setPipelineEnableLlmAudit] = useState<boolean>(true);
  const [pipelineProgress, setPipelineProgress] = useState<{ done: number; total: number; ok: number; failed: number } | null>(null);
  const [pipelineLog, setPipelineLog] = useState<string[]>([]);
  const [pipelineAbort, setPipelineAbort] = useState<AbortController | null>(null);
  const [expandedDescriptions, setExpandedDescriptions] = useState<Set<string>>(new Set());
  const [localSelectedFactors, setLocalSelectedFactors] = useState<Set<string>>(selectedFactors);
  const [selectedFactor, setSelectedFactor] = useState<string | null>(null);
  const [factorDetails, setFactorDetails] = useState<Record<string, FactorDetail>>({});
  const [detailLoading, setDetailLoading] = useState<Set<string>>(new Set());
  const [codeExpanded, setCodeExpanded] = useState<Set<string>>(new Set());
  const [factorExpMetrics, setFactorExpMetrics] = useState<Record<string, FactorExpMetrics>>({});
  const [expMetricsLoading, setExpMetricsLoading] = useState<Set<string>>(new Set());
  const [metricsLoading, setMetricsLoading] = useState(false);
  const [metricsResult, setMetricsResult] = useState<{
    ok?: boolean;
    success?: boolean;
    total_metrics_inserted?: number;
    total_metrics_skipped?: number;
    fail_count?: number;
    error?: string;
    logs?: string[];
    dispatch_status?: string;
    details?: any[];
    db_result?: { inserted?: number; skipped?: number; errors?: string[] };
    pipeline_summary?: { factor_results?: { name?: string; error?: string }[] };
  } | null>(null);
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
  const [ratingDetailExpanded, setRatingDetailExpanded] = useState(false);
  const [taskFactors, setTaskFactors] = useState<Record<string, any[]>>({});
  const [taskFactorsLoading, setTaskFactorsLoading] = useState<Set<string>>(new Set());
  const [taskAnalyzing, setTaskAnalyzing] = useState(false);
  const [taskAnalyzeResult, setTaskAnalyzeResult] = useState<{ ok?: boolean; total?: number; analyzed?: number; errors?: string[] } | null>(null);

  // 因子清洗 (cleanup)
  const [cleanupOpen, setCleanupOpen] = useState(false);
  const [cleanupLoading, setCleanupLoading] = useState(false);
  const [cleanupExecuting, setCleanupExecuting] = useState(false);
  const [cleanupRules, setCleanupRules] = useState<{ near_identical: boolean; pure_noise_v2: boolean; reverse_redundant: boolean }>({
    near_identical: true,
    pure_noise_v2: true,
    reverse_redundant: true,
  });
  const [cleanupResult, setCleanupResult] = useState<{
    summary: { total_enabled: number; total_candidates: number; after_cleanup: number; by_rule: Record<string, number>; thresholds: Record<string, number>; rules_applied: string[] };
    candidates: any[];
    reverse_pairs: any[];
  } | null>(null);
  const [cleanupSelected, setCleanupSelected] = useState<Set<number>>(new Set());
  const [cleanupExecuteResult, setCleanupExecuteResult] = useState<{ ok: boolean; batch_id: string; disabled_count: number; by_reason: Record<string, number>; errors: string[]; rollback_sql: string } | null>(null);

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
  const [snapshotStartDate, setSnapshotStartDate] = useState("2018-08-01");
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

  // 因子值缓存管理
  type CacheStats = {
    total_cached: number; total_code_factors: number; coverage_pct: number;
    total_size_mb: number; date_range_dominant: string; active_tasks: number;
    last_backfill?: any;
    hash_ok: number; hash_mismatch: number; cache_error: number; no_cache: number;
    disabled_total: number; disabled_cached: number;
    by_source?: Record<string, number>;
  };
  type CacheTask = {
    task_id: string;
    status: string;
    started_at?: string;
    finished_at?: string;
    workers?: number;
    batch_size?: number;
    factor_count?: number | string;
    incremental?: boolean;
    start?: string;
    end?: string;
    error?: string;
    dispatch_task_id?: string | null;
    remote_task_id?: string | null;
    node_id?: string | null;
    experiment_id?: string | null;
    strict_backtest_data?: boolean;
    cache_source?: string | null;
    code_source?: string | null;
    data_source_mode?: string | null;
    factor_data_dir?: string | null;
    qlib_bin_path?: string | null;
    window_train_start?: string | null;
    window_backtest_end?: string | null;
  };
  type CacheTaskDetail = CacheTask & {
    recent_log?: string;
    result?: any;
    task_state?: any;
    failed_tail?: any[];
  };
  type RemoteNodeStats = {
    node_id: string;
    display_name?: string | null;
    status?: string | null;
    host?: string;
    configured?: boolean;
    reachable?: boolean;
    factor_cache_dir?: string | null;
    resolved_factor_cache_dir?: string | null;
    remote_cached?: number;
    synced?: number;
    missing?: number;
    stale?: number;
    error?: string;
  };
  type RemoteFactorStatus = {
    status: "synced" | "missing" | "stale" | string;
    local_date_range?: string | null;
    remote_date_range?: string | null;
  };
  type RemoteCacheStats = {
    ok: boolean;
    local: { cached: number; size_mb: number; cache_root?: string; meta_sha256?: string | null };
    selected_node_id?: string | null;
    remote_nodes: RemoteNodeStats[];
    factor_status: Record<string, RemoteFactorStatus>;
    last_sync?: any;
  };
  const [cacheStats, setCacheStats] = useState<CacheStats | null>(null);
  const [remoteStats, setRemoteStats] = useState<RemoteCacheStats | null>(null);
  const [selectedRemoteNodeId, setSelectedRemoteNodeId] = useState<string>("");
  const [remoteStatsLoading, setRemoteStatsLoading] = useState(false);
  const [remoteStatsError, setRemoteStatsError] = useState<string>("");
  const [remoteSyncBusy, setRemoteSyncBusy] = useState(false);
  const [cacheWorkers, setCacheWorkers] = useState(4);
  const [cacheBusy, setCacheBusy] = useState(false);
  const [cacheStartDate, setCacheStartDate] = useState(FACTOR_CACHE_DEFAULT_START);
  const [cacheEndDate, setCacheEndDate] = useState(FACTOR_CACHE_DEFAULT_END);
  const [cacheCoverageFilter, setCacheCoverageFilter] = useState("all");
  const [cacheIncremental, setCacheIncremental] = useState(false);
  const [cacheTasks, setCacheTasks] = useState<CacheTask[]>([]);
  const [cacheTaskLoading, setCacheTaskLoading] = useState(false);
  const [selectedCacheTaskId, setSelectedCacheTaskId] = useState<string | null>(null);
  const [selectedCacheTask, setSelectedCacheTask] = useState<CacheTaskDetail | null>(null);

  const isSelection = mode === "selection";
  const isAlphaSourceFilter = sourceFilter === "alpha158" || sourceFilter === "alpha360";
  const shouldExcludeAlphaSources = !showAlpha && !isAlphaSourceFilter;

  const fetchCacheStats = useCallback(async () => {
    try {
      const r = await fetch(`${API}/quantevolver/factor-cache/stats`);
      const d = await r.json();
      if (d.ok) setCacheStats(d);
    } catch {}
  }, []);

  const fetchRemoteStats = useCallback(async (nodeId?: string | null) => {
    if (isSelection) return;
    setRemoteStatsLoading(true);
    try {
      const params = new URLSearchParams();
      const effectiveNode = nodeId || selectedRemoteNodeId;
      if (effectiveNode) params.set("node_id", effectiveNode);
      const r = await fetch(`${API}/quantevolver/factor-cache/remote-stats${params.toString() ? `?${params.toString()}` : ""}`);
      const d = await r.json();
      if (!r.ok || d.ok === false) throw new Error(d.detail || d.error || "远端缓存统计加载失败");
      if (d.ok) {
        setRemoteStats(d);
        setRemoteStatsError("");
        if (!selectedRemoteNodeId && d.selected_node_id) setSelectedRemoteNodeId(d.selected_node_id);
      }
    } catch (e: any) {
      setRemoteStatsError(e?.message || "远端缓存统计加载失败");
    }
    finally { setRemoteStatsLoading(false); }
  }, [isSelection, selectedRemoteNodeId]);

  const fetchCacheTasks = useCallback(async () => {
    setCacheTaskLoading(true);
    try {
      const r = await fetch(`${API}/quantevolver/factor-cache/active-tasks`);
      const d = await r.json();
      if (d.ok) {
        const tasks = [...(d.tasks || [])].sort((a: CacheTask, b: CacheTask) => (b.started_at || "").localeCompare(a.started_at || ""));
        setCacheTasks(tasks);
        if (!selectedCacheTaskId && tasks.length > 0) {
          setSelectedCacheTaskId(tasks[0].task_id);
        }
      }
    } catch {}
    finally { setCacheTaskLoading(false); }
  }, [selectedCacheTaskId]);

  const fetchCacheTaskDetail = useCallback(async (taskId?: string | null) => {
    if (!taskId) return;
    try {
      const r = await fetch(`${API}/quantevolver/factor-cache/compute-status/${encodeURIComponent(taskId)}`);
      if (!r.ok) return;
      const d = await r.json();
      setSelectedCacheTask(d);
    } catch {}
  }, []);

  useEffect(() => {
    if (!isSelection) {
      fetchCacheStats();
      fetchRemoteStats();
      fetchCacheTasks();
    }
  }, [isSelection, fetchCacheStats, fetchRemoteStats, fetchCacheTasks]);

  useEffect(() => {
    if (isSelection) return;
    const hasRunning = cacheTasks.some(t => t.status === "running" || t.status === "queued");
    if (!hasRunning) return;
    const timer = setInterval(() => {
      fetchCacheStats();
      fetchRemoteStats();
      fetchCacheTasks();
      if (selectedCacheTaskId) fetchCacheTaskDetail(selectedCacheTaskId);
    }, 5000);
    return () => clearInterval(timer);
  }, [isSelection, cacheTasks, selectedCacheTaskId, fetchCacheStats, fetchRemoteStats, fetchCacheTasks, fetchCacheTaskDetail]);

  const triggerCacheCompute = useCallback(async (
    factorNames?: string[],
    force = false,
  ) => {
    if (!cacheStartDate || !cacheEndDate) {
      alert("请先选择开始/结束日期");
      return;
    }
    if (cacheStartDate > cacheEndDate) {
      alert("开始日期不能晚于结束日期");
      return;
    }
    setCacheBusy(true);
    try {
      const body: any = {
        workers: cacheWorkers,
        timeout_per_factor: 1800,
        force,
        start_date: cacheStartDate,
        end_date: cacheEndDate,
        strict_backtest_data: true,
      };
      if (cacheContext?.experimentId) body.experiment_id = cacheContext.experimentId;
      if (factorNames && factorNames.length > 0) body.factor_names = factorNames;
      const r = await fetch(`${API}/quantevolver/factor-cache/compute`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
      const d = await r.json();
      if (d.ok) {
        setSelectedCacheTaskId(d.task_id);
        await Promise.all([fetchCacheStats(), fetchRemoteStats(), fetchCacheTasks(), fetchCacheTaskDetail(d.task_id)]);
        alert(`Official offline factor compute submitted (task_id: ${d.task_id})`);
      }
      else alert(d.detail || "提交失败");
    } catch (e: any) { alert(e.message); } finally { setCacheBusy(false); }
  }, [cacheWorkers, cacheStartDate, cacheEndDate, cacheContext?.experimentId, fetchCacheStats, fetchRemoteStats, fetchCacheTasks, fetchCacheTaskDetail]);

  const triggerRemoteSync = useCallback(async (factorNames?: string[]) => {
    setRemoteSyncBusy(true);
    try {
      const body: any = {
        node_id: selectedRemoteNodeId || undefined,
        factor_names: factorNames && factorNames.length > 0 ? factorNames : undefined,
        force: false,
        configure_default_dir: true,
      };
      const r = await fetch(`${API}/quantevolver/factor-cache/sync-to-node`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const d = await r.json();
      if (!r.ok || d.ok === false) throw new Error(d.detail || d.error || d.job?.error || "同步失败");
      await fetchRemoteStats(selectedRemoteNodeId);
      const job = d.job || d.jobs?.[0];
      alert(`远端同步完成：同步 ${job?.sync_count ?? "-"}，跳过 ${job?.skipped_count ?? "-"}`);
    } catch (e: any) {
      alert(e.message || "远端同步失败");
      await fetchRemoteStats(selectedRemoteNodeId);
    } finally {
      setRemoteSyncBusy(false);
    }
  }, [selectedRemoteNodeId, fetchRemoteStats]);

  const clearAllCache = useCallback(async () => {
    if (!confirm("确认清空所有因子值缓存？此操作不可恢复。")) return;
    try {
      const r = await fetch(`${API}/quantevolver/factor-cache/all`, { method: "DELETE" });
      const d = await r.json();
      alert(`已删除 ${d.deleted} 个缓存文件`);
      fetchCacheStats();
      fetchRemoteStats();
      window.location.reload();
    } catch (e: any) { alert(e.message); }
  }, [fetchCacheStats, fetchRemoteStats]);

  const clearOneCache = useCallback(async (factorName: string) => {
    if (!confirm(`确认删除因子 ${factorName} 的缓存？`)) return;
    try {
      const r = await fetch(`${API}/quantevolver/factor-cache/${encodeURIComponent(factorName)}`, { method: "DELETE" });
      const d = await r.json();
      if (!r.ok || !d.ok) throw new Error(d.detail || d.error || "删除失败");
      await Promise.all([fetchCacheStats(), fetchRemoteStats(), fetchCacheTasks()]);
      window.location.reload();
    } catch (e: any) {
      alert(e.message || "删除失败");
    }
  }, [fetchCacheStats, fetchRemoteStats, fetchCacheTasks]);

  const actualSelectedFactors = mode === "selection" ? selectedFactors : localSelectedFactors;
  const selectedRemoteNode = remoteStats?.remote_nodes?.find(n => n.node_id === selectedRemoteNodeId)
    || remoteStats?.remote_nodes?.[0]
    || null;
  const remoteFactorStatusByName = remoteStats?.factor_status || {};
  const selectedRatingRule = ratingRules.find(rule => rule.rule_version === selectedRatingVersion);
  const selectedRatingRuleExecutable = Boolean(
    selectedRatingRule
      && selectedRatingRule.status !== "archived"
      && selectedRatingRule.rule_version.toLowerCase().startsWith("v2")
  );

  function toggleSelect(key: string) {
    const next = new Set<string>(actualSelectedFactors);
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
      const params = new URLSearchParams({ data_date: newSnapshotDate, start_date: snapshotStartDate });
      const res = await fetch(`${API}/quantevolver/evolution/factor-values/snapshots/create?${params}`, {
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
      // 轮询快照创建状态
      const poll = async () => {
        while (true) {
          await new Promise(r => setTimeout(r, 3000));
          try {
            const st = await fetch(`${API}/quantevolver/evolution/factor-values/snapshots/status`);
            const data = await st.json();
            if (!data.creating) {
              await loadSnapshots();
              if (data.last_error) {
                alert(`快照创建失败: ${data.last_error}`);
              } else {
                alert("快照创建成功!");
              }
              return;
            }
          } catch { /* 网络抖动，继续轮询 */ }
        }
      };
      poll().finally(() => setSnapshotCreating(false));
    } catch (e: any) {
      alert(`创建快照失败: ${e.message}`);
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
        is_sota_factor: f.is_sota_factor,
        is_available: f.is_available !== false,
        description_cn: f.description_cn,
        category: f.category,
        official_grade: f.official_grade ?? null,
        official_grade_reason: f.official_grade_reason_structured?.summary ?? null,
        classification_reason: f.classification_reason,
        factor_dimension: f.factor_dimension,
        description: f.cl_description,
        classification_id: f.classification_id,
        ts_info_density: f.ts_info_density ?? null,
        cross_horizon_consistency: f.cross_horizon_consistency ?? null,
        direction: f.direction ?? null,
        signal_mechanism: f.signal_mechanism ?? null,
        sector_exposure_corr: f.sector_exposure_corr ?? null,
        horizon_class: f.horizon_class ?? null,
        best_horizon: f.best_horizon ?? null,
        best_horizon_advantage: f.best_horizon_advantage ?? null,
        linearity: f.linearity ?? null,
        holding_period_class: f.holding_period_class ?? null,
        data_source_group: f.data_source_group ?? null,
        update_freq: f.update_freq ?? null,
        ic_sign_consistency_12m: f.ic_sign_consistency_12m ?? null,
        ic_oos_is_ratio: f.ic_oos_is_ratio ?? null,
        monthly_ic_trend_slope: f.monthly_ic_trend_slope ?? null,
        cluster_id: f.cluster_id ?? null,
        cluster_role: f.cluster_role ?? null,
        cluster_size: f.cluster_size ?? null,
        intra_cluster_max_corr: f.intra_cluster_max_corr ?? null,
        representative_score: f.representative_score ?? null,
        official_score: f.official_score ?? null,
        official_rule_version: f.official_rule_version ?? null,
        official_grade_reason_structured: f.official_grade_reason_structured ?? null,
        llm_audit_summary: f.official_llm_audit_summary ?? null,
        llm_risk_notes: f.official_llm_risk_notes ?? null,
        ind_ic: f.ind_ic ?? ind?.ic_mean ?? null,
        ind_rank_ic: f.ind_rank_ic ?? null,
        ind_rank_ic_1d: f.ind_rank_ic_1d ?? null,
        ind_rank_ic_5d: f.ind_rank_ic_5d ?? null,
        ind_rank_ic_10d: f.ind_rank_ic_10d ?? null,
        ind_rank_ic_20d: f.ind_rank_ic_20d ?? null,
        ind_rank_ic_best_abs: f.ind_rank_ic_best_abs ?? null,
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
        has_cache: f.has_cache ?? false,
        cache_date_range: f.cache_date_range ?? null,
        cache_start_date: f.cache_start_date ?? null,
        cache_end_date: f.cache_end_date ?? null,
        cache_computed_at: f.cache_computed_at ?? null,
        cache_as_of_date: f.cache_as_of_date ?? null,
        cache_window_train_start: f.cache_window_train_start ?? null,
        cache_window_backtest_end: f.cache_window_backtest_end ?? null,
        cache_source: f.cache_source ?? null,
        cache_source_label: f.cache_source_label ?? null,
        cache_data_source_mode: f.cache_data_source_mode ?? null,
        cache_coverage_status: f.cache_coverage_status ?? null,
        cache_status: f.cache_status ?? null,
        cache_size_mb: f.cache_size_mb ?? null,
        cache_hash_match: f.cache_hash_match ?? null,
      };
    });

    let filtered = merged;

    // 缓存状态后端已支持全量排序；这里保留当前页内兜底排序。
    if (sortField === "cache_status") {
      const statusScore: Record<string, number> = { no_cache: 0, error: 0, hash_mismatch: 1, partial: 2, covered: 3, ok: 3 };
      const cacheScore = (f: MergedFactor) => {
        if (f.cache_coverage_status) return statusScore[f.cache_coverage_status] ?? 0;
        if (!f.has_cache) return 0;
        if (f.cache_hash_match === false) return 1;
        // partial range
        const s = cacheStartDate || cacheContext?.trainStart;
        const e = cacheEndDate || cacheContext?.backtestEnd;
        if (s && e && f.cache_date_range?.includes("~")) {
          if (factorCacheCoversRequestedWindow(f, s, e)) return 3; // full match including warm-up window
          return 2; // partial
        }
        return 3; // has cache, no context to judge
      };
      filtered = [...merged].sort((a, b) => {
        const sa = cacheScore(a), sb = cacheScore(b);
        return sortOrder === "desc" ? sb - sa : sa - sb;
      });
    }

    return filtered;
  }, [factors, indSummary, categoryFilter, gradeFilter, sortField, sortOrder, cacheContext, cacheStartDate, cacheEndDate]);

  const loadData = useCallback(async (queryOverride?: string) => {
    const requestId = ++loadDataRequestRef.current;
    setLoading(true);
    setError(null);
    try {
      const effectiveSearch = typeof queryOverride === "string" ? queryOverride : search;
      const factorParams = new URLSearchParams({ limit: String(pageSize), offset: String((page - 1) * pageSize) });
      const isCacheSort = !!sortField && (sortField === "cache_status" || sortField.startsWith("cache_"));
      if (sourceFilter) factorParams.set("source", sourceFilter);
      if (effectiveSearch) factorParams.set("search", effectiveSearch);
      if (shouldExcludeAlphaSources) factorParams.set("exclude_source", "alpha158,alpha360");
      if (sortField) factorParams.set("sort_field", sortField);
      if (sortField) factorParams.set("sort_order", sortOrder);
      if (categoryFilter) factorParams.set("category", categoryFilter);
      if (gradeFilter) factorParams.set("grade", gradeFilter);
      if (availabilityFilter && availabilityFilter !== "all") factorParams.set("availability", availabilityFilter);
      if (cacheCoverageFilter !== "all") factorParams.set("cache_filter", cacheCoverageFilter);
      if (cacheCoverageFilter !== "all" || isCacheSort) {
        if (cacheStartDate) factorParams.set("cache_start_date", cacheStartDate);
        if (cacheEndDate) factorParams.set("cache_end_date", cacheEndDate);
      }

      const fRes = await fetchJsonOrThrow(`${API}/quantevolver/factors?${factorParams.toString()}`);
      if (!fRes?.ok || !Array.isArray(fRes.items)) {
        throw new Error(fRes?.detail || fRes?.message || "因子列表接口返回结构异常");
      }

      if (requestId !== loadDataRequestRef.current) return;
      setFactors(fRes.items || []);
      setTotal(fRes.total || 0);
    } catch (e: any) {
      if (requestId !== loadDataRequestRef.current) return;
      setError(e?.message || "加载失败");
    } finally {
      if (requestId === loadDataRequestRef.current) setLoading(false);
    }
  }, [sourceFilter, search, page, pageSize, shouldExcludeAlphaSources, sortField, sortOrder, categoryFilter, gradeFilter, availabilityFilter, cacheCoverageFilter, cacheStartDate, cacheEndDate]);

  const loadIndSummary = useCallback(async () => {
    try {
      let res = await fetch(`${API}/quantevolver/official-evaluation/summary`);
      if (res.status === 404) {
        res = await fetch(`${API}/quantevolver/factors/independent-metrics-summary`);
      }
      if (res.ok) {
        const d = await res.json();
        if (d.ok) setIndSummary(d.summary || {});
      }
    } catch {}
  }, []);

  const loadRatingRules = useCallback(async () => {
    try {
      const res = await fetch(`${API}/quantevolver/rating/rules`);
      if (!res.ok) return;
      const data = await res.json();
      if (!data.ok) return;
      const rules = data.rules || [];
      setRatingRules(rules);
      const active = data.active_version || data.default_version || rules[0]?.rule_version || "";
      setActiveRatingVersion(active);
      setSelectedRatingVersion(prev => prev || active);
    } catch {}
  }, []);

  const loadRatingRuleDetail = useCallback(async (version: string) => {
    if (!version) { setRatingRuleDetail(null); return; }
    try {
      const res = await fetch(`${API}/quantevolver/rating/rules/${encodeURIComponent(version)}`);
      if (!res.ok) return;
      const data = await res.json();
      if (data.ok) setRatingRuleDetail(data);
    } catch {}
  }, []);

  const loadRatingRuns = useCallback(async () => {
    try {
      const res = await fetch(`${API}/quantevolver/rating/runs?limit=8`);
      if (!res.ok) return;
      const data = await res.json();
      if (data.ok) setRatingRuns(data.items || []);
    } catch {}
  }, []);

  const loadRatingResultsPreview = useCallback(async (version: string) => {
    if (!version) { setRatingResultsPreview([]); return; }
    try {
      const res = await fetch(`${API}/quantevolver/rating/results?rule_version=${encodeURIComponent(version)}&limit=5&offset=0`);
      if (!res.ok) return;
      const data = await res.json();
      if (data.ok) setRatingResultsPreview(data.items || []);
    } catch {}
  }, []);

  useEffect(() => { loadData(); loadIndSummary(); loadRatingRules(); loadRatingRuns(); }, [loadData, loadIndSummary, loadRatingRules, loadRatingRuns]);
  useEffect(() => { if (selectedRatingVersion) { loadRatingRuleDetail(selectedRatingVersion); loadRatingResultsPreview(selectedRatingVersion); } }, [selectedRatingVersion, loadRatingRuleDetail, loadRatingResultsPreview]);

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
        let res = await fetch(`${API}/quantevolver/official-evaluation/compute`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            factor_names: factorNames,
            data_date: activeSnapshot,
            include_disabled: true,
          }),
        });
        if (res.status === 404) {
          res = await fetch(`${API}/quantevolver/factors/batch-compute-metrics-unified`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              factor_names: factorNames,
              data_date: activeSnapshot,
              all_available: false,
            }),
          });
        }
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

  async function batchFetchMetrics() {
    if (!activeSnapshot) {
      alert("请先选择数据快照后再执行指标计算。");
      return;
    }
    const selectedCount = actualSelectedFactors.size;
    if (selectedCount === 0) {
      alert("请先选择要计算指标的因子");
      return;
    }
    const factorNames = Array.from(actualSelectedFactors).map(k => k.split("||")[0]);
    if (!confirm(`确定要基于快照 ${activeSnapshot} 计算选中的 ${selectedCount} 个因子指标吗？`)) return;

    setMetricsLoading(true);
    setMetricsResult(null);
    try {
      let res = await fetch(`${API}/quantevolver/official-evaluation/compute`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ factor_names: factorNames, data_date: activeSnapshot, include_disabled: true }),
      });
      if (res.status === 404) {
        res = await fetch(`${API}/quantevolver/factors/batch-compute-metrics-unified`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ factor_names: factorNames, data_date: activeSnapshot, all_available: false }),
        });
      }
      const data = await res.json();
      if (!res.ok) {
        setMetricsResult({
          ...data,
          ok: false,
          success: false,
          error: data.detail || data.error || `HTTP ${res.status}`,
        });
        return;
      }
      const ok = data.success === true;
      setMetricsResult({ ...data, ok });
      if (ok) {
        loadData();
        loadIndSummary();
      }
    } catch (e: any) {
      setMetricsResult({ ok: false, success: false, error: e?.message || "指标计算失败" });
    } finally {
      setMetricsLoading(false);
      setLocalSelectedFactors(new Set());
    }
  }

  useEffect(() => {
    setPage(1);
  }, [sourceFilter, search, categoryFilter, gradeFilter, showAlpha, sortField, sortOrder, availabilityFilter, cacheCoverageFilter, cacheStartDate, cacheEndDate]);

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

  async function runOfficialRating(scopeType: "selected" | "filter" | "all") {
    const ruleVersion = selectedRatingVersion || activeRatingVersion;
    if (!ruleVersion) {
      alert("请先选择评级规则版本");
      return;
    }

    // 构造 selected_factors / filters
    let selectedPayload: Array<{ factor_name: string; source: string }> | undefined;
    let filtersPayload: Record<string, string | undefined> | undefined;
    if (scopeType === "selected") {
      if (actualSelectedFactors.size === 0) {
        alert("请先选择要评级的因子");
        return;
      }
      selectedPayload = Array.from(actualSelectedFactors).map((key) => {
        const [factor_name, source] = key.split("||");
        return { factor_name, source };
      });
    } else if (scopeType === "filter") {
      filtersPayload = {
        source: sourceFilter || undefined,
        exclude_source: shouldExcludeAlphaSources ? "alpha158,alpha360" : undefined,
        search: search || undefined,
        category: categoryFilter || undefined,
        grade: gradeFilter || undefined,
        availability: availabilityFilter || undefined,
      };
    }

    const scopeLabel = scopeType === "selected"
      ? `选中的 ${actualSelectedFactors.size} 个因子`
      : scopeType === "filter"
        ? "当前筛选结果"
        : "全量因子";
    const modeLabel = runMode === "full_pipeline" ? "一键全流程(分类+评级+LLM)" : "仅评级";
    if (!confirm(`将使用规则 ${ruleVersion} / ${modeLabel} 对${scopeLabel}执行，确定继续？`)) return;

    if (runMode === "full_pipeline") {
      await runPipelineStream(scopeType, ruleVersion, selectedPayload, filtersPayload);
    } else {
      await runRatingOnly(scopeType, ruleVersion, selectedPayload, filtersPayload);
    }
  }

  // 仅评级 —— 同步调用 /rating/run
  async function runRatingOnly(
    scopeType: "selected" | "filter" | "all",
    ruleVersion: string,
    selectedPayload: Array<{ factor_name: string; source: string }> | undefined,
    filtersPayload: Record<string, string | undefined> | undefined,
  ) {
    const payload: any = {
      rule_version: ruleVersion,
      scope_type: scopeType,
      triggered_from: "ui_toolbar",
    };
    if (selectedPayload) payload.selected_factors = selectedPayload;
    if (filtersPayload) payload.filters = filtersPayload;

    setRatingRunLoading(true);
    setRatingRunResult(null);
    try {
      const res = await fetch(`${API}/quantevolver/rating/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!res.ok || data.ok === false) {
        throw new Error(data.detail || data.error || "正式评级失败");
      }
      setRatingRunResult(data);
      loadData();
      loadRatingRules();
      loadRatingRuns();
    } catch (e: any) {
      setRatingRunResult({ ok: false, errors: [{ factor_name: "system", error: e?.message || "正式评级失败" }] });
      alert(`正式评级失败: ${e?.message || "未知错误"}`);
    } finally {
      setRatingRunLoading(false);
    }
  }

  // 一键全流程 —— SSE 流式 /pipeline/full-stream
  async function runPipelineStream(
    scopeType: "selected" | "filter" | "all",
    ruleVersion: string,
    selectedPayload: Array<{ factor_name: string; source: string }> | undefined,
    filtersPayload: Record<string, string | undefined> | undefined,
  ) {
    if (pipelineParallelism < 1 || pipelineParallelism > 16) {
      alert(`并行度必须在 [1,16] 区间, 当前: ${pipelineParallelism}`);
      return;
    }

    const body: any = {
      scope_type: scopeType,
      parallelism: pipelineParallelism,
      enable_llm_analysis: pipelineEnableLlmAnalysis,
      enable_llm_audit: pipelineEnableLlmAudit,
      rule_version: ruleVersion,
    };
    if (selectedPayload) body.selected_factors = selectedPayload;
    if (filtersPayload) {
      // 后端 filters 只接受字符串, 过滤掉 undefined
      const cleaned: Record<string, string> = {};
      Object.entries(filtersPayload).forEach(([k, v]) => { if (v) cleaned[k] = v; });
      body.filters = cleaned;
    }

    const ctrl = new AbortController();
    setPipelineAbort(ctrl);
    setRatingRunLoading(true);
    setRatingRunResult(null);
    setPipelineProgress(null);
    setPipelineLog([`[${new Date().toLocaleTimeString()}] POST /pipeline/full-stream ${JSON.stringify(body)}`]);

    const pushLog = (line: string) => {
      setPipelineLog(prev => {
        const next = [...prev, `[${new Date().toLocaleTimeString()}] ${line}`];
        if (next.length > 1000) next.splice(0, next.length - 1000);
        return next;
      });
    };

    try {
      const res = await fetch(`${API}/quantevolver/pipeline/full-stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: ctrl.signal,
      });
      if (!res.ok || !res.body) {
        throw new Error(`HTTP ${res.status}`);
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder("utf-8");
      let buffer = "";

      // eslint-disable-next-line no-constant-condition
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let idx: number;
        while ((idx = buffer.indexOf("\n\n")) >= 0) {
          const chunk = buffer.slice(0, idx);
          buffer = buffer.slice(idx + 2);
          chunk.split(/\n/).forEach(line => {
            if (!line.startsWith("data:")) return;
            const raw = line.slice(5).trim();
            if (!raw) return;
            let ev: any;
            try {
              ev = JSON.parse(raw);
            } catch (e) {
              pushLog(`[parse-error] ${raw.slice(0, 200)}`);
              return;
            }
            switch (ev.event) {
              case "start":
                pushLog(`[run] 开始 total=${ev.total} parallel=${ev.parallelism} rule=${ev.rule_version} run_id=${ev.run_id}`);
                break;
              case "progress":
                setPipelineProgress({ done: ev.done, total: ev.total, ok: ev.ok, failed: ev.failed });
                break;
              case "factor_step":
                if (ev.phase === "done") {
                  const bits: string[] = [];
                  if (ev.category) bits.push(`cat=${ev.category}`);
                  if (ev.direction !== undefined && ev.direction !== null) bits.push(`dir=${ev.direction}`);
                  if (ev.signal_mechanism) bits.push(`mech=${ev.signal_mechanism}`);
                  if (ev.official_grade) bits.push(`grade=${ev.official_grade}`);
                  if (ev.official_score !== undefined) bits.push(`score=${ev.official_score}`);
                  pushLog(`[${ev.factor_name}/Step${ev.step}] ✅ ${bits.join(" ")}`);
                } else if (ev.phase === "error") {
                  pushLog(`[${ev.factor_name}/Step${ev.step}] ✗ ${ev.error}`);
                }
                break;
              case "factor_done":
                if (!ev.ok) {
                  pushLog(`[${ev.factor_name}] ✗ ${ev.step_b_error || ev.step_a_error || "failed"}`);
                }
                break;
              case "done":
                pushLog(`[done] ok=${ev.ok} total=${ev.total_factors} success=${ev.success_count} failed=${ev.failed_count}`);
                setRatingRunResult({
                  ok: ev.ok,
                  run_id: ev.run_id,
                  total_factors: ev.total_factors,
                  success_count: ev.success_count,
                  failed_count: ev.failed_count,
                });
                break;
              case "error":
                pushLog(`[error] ${ev.error}`);
                throw new Error(ev.error || "pipeline error");
            }
          });
        }
      }
    } catch (e: any) {
      const msg = e?.message || String(e);
      if (msg !== "The user aborted a request.") {
        setRatingRunResult({ ok: false, errors: [{ factor_name: "system", error: msg }] });
        pushLog(`[fetch-error] ${msg}`);
        alert(`一键流水线失败: ${msg}`);
      } else {
        pushLog(`[stop] 用户中止`);
      }
    } finally {
      setRatingRunLoading(false);
      setPipelineAbort(null);
      loadData();
      loadRatingRules();
      loadRatingRuns();
    }
  }

  async function loadFactorIndependentMetrics(key: string, factorName: string) {
    if (factorIndMetrics[key]) return;
    try {
      const res = await fetch(`${API}/quantevolver/official-evaluation/factors/${encodeURIComponent(factorName)}?limit=10`);
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

          {!isSelection && (
            <section style={{ background: "#fff", borderRadius: 12, padding: 16, marginBottom: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-start", flexWrap: "wrap" }}>
                <div style={{ minWidth: 320, flex: 1 }}>
                  <div style={{ fontSize: 14, fontWeight: 700, color: "#1f2937", marginBottom: 6 }}>因子评级管理</div>
                  <div style={{ fontSize: 12, color: "#6b7280", lineHeight: 1.6 }}>
                    正式评级仅允许通过此工具栏触发。可选择模板(规则版本)，以及执行模式：<strong>仅评级</strong>（快速, 需先有分类）或
                    <strong>一键全流程</strong>（Step A 分类+方向+机制+行业敞口 → Step B 打分+LLM审阅, 并行流式执行）。
                  </div>
                </div>
                <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                  <span style={{ fontSize: 11, color: "#6b7280" }}>当前激活版本</span>
                  <span style={{ padding: "4px 8px", borderRadius: 999, background: "#ede9fe", color: "#6d28d9", fontSize: 11, fontWeight: 700 }}>
                    {activeRatingVersion || "-"}
                  </span>
                  <select
                    value={selectedRatingVersion}
                    onChange={e => setSelectedRatingVersion(e.target.value)}
                    style={{ padding: "6px 10px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db", minWidth: 240 }}
                  >
                    {ratingRules.map(rule => (
                      <option
                        key={rule.rule_version}
                        value={rule.rule_version}
                        disabled={rule.status === "archived" || !rule.rule_version.toLowerCase().startsWith("v2")}
                      >
                        {rule.rule_version} · {rule.version_name} · {rule.status === "archived" ? "已归档/不可执行" : rule.status}
                      </option>
                    ))}
                  </select>
                  <button
                    onClick={async () => {
                      if (!selectedRatingVersion) return;
                      if (!confirm(`将把 ${selectedRatingVersion} 设为当前激活规则版本，确定继续？`)) return;
                      try {
                        const res = await fetch(`${API}/quantevolver/rating/rules/activate`, {
                          method: "POST",
                          headers: { "Content-Type": "application/json" },
                          body: JSON.stringify({ version: selectedRatingVersion }),
                        });
                        const data = await res.json();
                        if (!res.ok || data.ok === false) throw new Error(data.detail || data.error || "激活失败");
                        loadRatingRules();
                        loadRatingRuleDetail(selectedRatingVersion);
                        loadData();
                      } catch (e: any) {
                        alert(`激活规则失败: ${e?.message || "未知错误"}`);
                      }
                    }}
                    disabled={!selectedRatingVersion || selectedRatingVersion === activeRatingVersion || !selectedRatingRuleExecutable}
                    title={!selectedRatingRuleExecutable ? "归档或非 v2 规则不可激活/执行" : undefined}
                    style={{ padding: "6px 12px", fontSize: 12, borderRadius: 6, border: "1px solid #7c3aed", background: "#fff", color: "#7c3aed", fontWeight: 600, cursor: !selectedRatingVersion || selectedRatingVersion === activeRatingVersion || !selectedRatingRuleExecutable ? "not-allowed" : "pointer", opacity: !selectedRatingVersion || selectedRatingVersion === activeRatingVersion || !selectedRatingRuleExecutable ? 0.5 : 1 }}
                  >
                    设为激活版本
                  </button>
                  {/* 模式选择 */}
                  <span style={{ fontSize: 11, color: "#6b7280", marginLeft: 8 }}>模式</span>
                  <select
                    value={runMode}
                    onChange={e => setRunMode(e.target.value as "rating_only" | "full_pipeline")}
                    disabled={ratingRunLoading}
                    style={{ padding: "6px 10px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db", background: runMode === "full_pipeline" ? "#eff6ff" : "#fff", fontWeight: 600 }}
                  >
                    <option value="rating_only">仅评级</option>
                    <option value="full_pipeline">一键全流程 (分类+评级+LLM)</option>
                  </select>
                  <button
                    onClick={() => runOfficialRating("selected")}
                    disabled={ratingRunLoading || actualSelectedFactors.size === 0}
                    style={{ padding: "6px 12px", fontSize: 12, borderRadius: 6, border: "none", background: "#2563eb", color: "#fff", fontWeight: 600, cursor: ratingRunLoading || actualSelectedFactors.size === 0 ? "not-allowed" : "pointer", opacity: ratingRunLoading || actualSelectedFactors.size === 0 ? 0.5 : 1 }}
                  >
                    {ratingRunLoading ? "执行中..." : `${runMode === "full_pipeline" ? "🚀 流水线" : "评级"}选中(${actualSelectedFactors.size})`}
                  </button>
                  <button
                    onClick={() => runOfficialRating("filter")}
                    disabled={ratingRunLoading}
                    style={{ padding: "6px 12px", fontSize: 12, borderRadius: 6, border: "1px solid #2563eb", background: "#eff6ff", color: "#2563eb", fontWeight: 600, cursor: ratingRunLoading ? "not-allowed" : "pointer", opacity: ratingRunLoading ? 0.5 : 1 }}
                  >
                    {runMode === "full_pipeline" ? "🚀 筛选流水线" : "当前筛选评级"}
                  </button>
                  <button
                    onClick={() => runOfficialRating("all")}
                    disabled={ratingRunLoading}
                    style={{ padding: "6px 12px", fontSize: 12, borderRadius: 6, border: "1px solid #7c3aed", background: "#f5f3ff", color: "#7c3aed", fontWeight: 700, cursor: ratingRunLoading ? "not-allowed" : "pointer", opacity: ratingRunLoading ? 0.5 : 1 }}
                  >
                    {runMode === "full_pipeline" ? "🚀 全量流水线" : "全量评级"}
                  </button>
                  {ratingRunLoading && pipelineAbort && runMode === "full_pipeline" && (
                    <button
                      onClick={() => pipelineAbort.abort()}
                      style={{ padding: "6px 12px", fontSize: 12, borderRadius: 6, border: "1px solid #dc2626", background: "#fff", color: "#dc2626", fontWeight: 600, cursor: "pointer" }}
                    >
                      ■ 停止接收
                    </button>
                  )}
                  <button
                    onClick={() => setRatingDetailExpanded(!ratingDetailExpanded)}
                    style={{ padding: "6px 12px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db", background: "#fff", color: "#374151", fontWeight: 600, cursor: "pointer", display: "flex", alignItems: "center", gap: 4 }}
                  >
                    {ratingDetailExpanded ? "收起详情" : "详情"}
                    <span style={{ fontSize: 10, transition: "transform 0.2s", transform: ratingDetailExpanded ? "rotate(90deg)" : "rotate(0deg)", display: "inline-block" }}>▶</span>
                  </button>
                  <button
                    onClick={() => {
                      setCleanupOpen(true);
                      setCleanupResult(null);
                      setCleanupSelected(new Set());
                      setCleanupExecuteResult(null);
                    }}
                    style={{ padding: "6px 12px", fontSize: 12, borderRadius: 6, border: "1px solid #ea580c", background: "#fff7ed", color: "#c2410c", fontWeight: 700, cursor: "pointer" }}
                    title="基于 IC≈0 + corr=±1 + 簇内冗余 三规则的一键清洗"
                  >
                    🧹 因子清洗
                  </button>
                </div>
              </div>

              {/* 一键全流程配置（仅 full_pipeline 模式显示） */}
              {runMode === "full_pipeline" && (
                <div style={{ marginTop: 10, padding: "8px 12px", borderRadius: 8, background: "#f8fafc", border: "1px solid #e2e8f0", display: "flex", gap: 16, flexWrap: "wrap", alignItems: "center" }}>
                  <span style={{ fontSize: 12, fontWeight: 600, color: "#475569" }}>🚀 流水线配置</span>
                  <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: "#334155" }}>
                    并行度:
                    <input
                      type="range" min={1} max={8}
                      value={pipelineParallelism}
                      onChange={e => setPipelineParallelism(Number(e.target.value))}
                      disabled={ratingRunLoading}
                    />
                    <span style={{ minWidth: 20, fontWeight: 700 }}>{pipelineParallelism}</span>
                  </label>
                  <label style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 12, color: "#334155" }}>
                    <input
                      type="checkbox" checked={pipelineEnableLlmAnalysis}
                      onChange={e => setPipelineEnableLlmAnalysis(e.target.checked)}
                      disabled={ratingRunLoading}
                    />
                    Step A LLM 分类
                  </label>
                  <label style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 12, color: "#334155" }}>
                    <input
                      type="checkbox" checked={pipelineEnableLlmAudit}
                      onChange={e => setPipelineEnableLlmAudit(e.target.checked)}
                      disabled={ratingRunLoading}
                    />
                    Step B LLM 审阅
                  </label>
                  <span style={{ fontSize: 11, color: "#64748b" }}>
                    Step A 失败则跳过 Step B（评级依赖分类字段）
                  </span>
                </div>
              )}

              {/* 流水线进度 + 日志 */}
              {(pipelineProgress || pipelineLog.length > 0) && (
                <div style={{ marginTop: 10 }}>
                  {pipelineProgress && (
                    <div>
                      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 12, color: "#334155", marginBottom: 4 }}>
                        <span>
                          {pipelineProgress.done}/{pipelineProgress.total}  ✅{pipelineProgress.ok}  ✗{pipelineProgress.failed}
                        </span>
                        <span>{pipelineProgress.total > 0 ? Math.round((pipelineProgress.done / pipelineProgress.total) * 100) : 0}%</span>
                      </div>
                      <div style={{ background: "#e2e8f0", height: 6, borderRadius: 3 }}>
                        <div style={{
                          background: "#2563eb", height: "100%",
                          width: pipelineProgress.total > 0 ? `${(pipelineProgress.done / pipelineProgress.total) * 100}%` : "0%",
                          borderRadius: 3, transition: "width 0.25s",
                        }} />
                      </div>
                    </div>
                  )}
                  {pipelineLog.length > 0 && (
                    <details style={{ marginTop: 8 }}>
                      <summary style={{ fontSize: 12, color: "#475569", cursor: "pointer" }}>
                        流式日志 ({pipelineLog.length} 行)
                      </summary>
                      <pre style={{
                        background: "#0f172a", color: "#e2e8f0", padding: 10, borderRadius: 6,
                        fontFamily: "ui-monospace, monospace", fontSize: 11,
                        maxHeight: 240, overflow: "auto", whiteSpace: "pre-wrap", margin: "6px 0 0 0",
                      }}>{pipelineLog.join("\n")}</pre>
                    </details>
                  )}
                </div>
              )}

              {ratingDetailExpanded && ratingRuleDetail && (
                <div style={{ marginTop: 12, borderTop: "1px solid #eef2f7", paddingTop: 12, display: "grid", gridTemplateColumns: "1.3fr 1fr", gap: 16 }}>
                  <div>
                    <div style={{ fontSize: 12, fontWeight: 700, color: "#374151", marginBottom: 6 }}>
                      规则说明 · {ratingRuleDetail.rule_version}
                    </div>
                    <pre style={{ margin: 0, whiteSpace: "pre-wrap", fontFamily: "inherit", fontSize: 12, lineHeight: 1.65, color: "#4b5563", background: "#f8fafc", border: "1px solid #e5e7eb", borderRadius: 8, padding: 12, maxHeight: 240, overflowY: "auto" }}>
                      {ratingRuleDetail.description_md || "暂无规则说明"}
                    </pre>
                  </div>
                  <div>
                    <div style={{ fontSize: 12, fontWeight: 700, color: "#374151", marginBottom: 6 }}>评分摘要</div>
                    <div style={{ background: "#f8fafc", border: "1px solid #e5e7eb", borderRadius: 8, padding: 12, fontSize: 12, color: "#4b5563", lineHeight: 1.7 }}>
                      {ratingRuleDetail.spec?.weights && (
                        <div style={{ marginBottom: 8 }}>
                          <strong style={{ color: "#111827" }}>维度权重</strong>
                          {Object.entries(ratingRuleDetail.spec.weights).map(([k, v]) => (
                            <div key={k}>{k}: {String(v)}</div>
                          ))}
                        </div>
                      )}
                      {ratingRuleDetail.grade_bands && (
                        <div style={{ marginBottom: 8 }}>
                          <strong style={{ color: "#111827" }}>等级门槛</strong>
                          {Object.entries(ratingRuleDetail.grade_bands).map(([k, v]: any) => (
                            <div key={k}>{k}: ≥ {v.min_score}</div>
                          ))}
                        </div>
                      )}
                      {ratingRuleDetail.spec?.hard_gates?.S && (
                        <div>
                          <strong style={{ color: "#111827" }}>S/A hard gates</strong>
                          <div>S: core_ic≥{ratingRuleDetail.spec.hard_gates.S.min_core_ic}, coverage≥{ratingRuleDetail.spec.hard_gates.S.min_coverage}, turnover≤{ratingRuleDetail.spec.hard_gates.S.max_turnover}</div>
                          <div>A: core_ic≥{ratingRuleDetail.spec.hard_gates.A.min_core_ic}, coverage≥{ratingRuleDetail.spec.hard_gates.A.min_coverage}, turnover≤{ratingRuleDetail.spec.hard_gates.A.max_turnover}</div>
                        </div>
                      )}
                      {ratingResultsPreview.length > 0 && (
                        <div style={{ marginTop: 8 }}>
                          <strong style={{ color: "#111827" }}>最近结果预览</strong>
                          {ratingResultsPreview.map((item) => (
                            <div key={`${item.factor_name}-${item.source}`} style={{ marginTop: 4 }}>
                              {item.factor_name} · {item.official_grade} · {item.official_score?.toFixed?.(1) ?? item.official_score}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              )}

              {ratingDetailExpanded && (
              <div style={{ marginTop: 12, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                <div style={{ background: "#f8fafc", border: "1px solid #e5e7eb", borderRadius: 8, padding: 12 }}>
                  <div style={{ fontSize: 12, fontWeight: 700, color: "#374151", marginBottom: 6 }}>最近评级任务</div>
                  {ratingRuns.length === 0 ? (
                    <div style={{ fontSize: 12, color: "#94a3b8" }}>暂无评级任务</div>
                  ) : (
                    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                      {ratingRuns.map((run) => (
                        <div key={run.run_id} style={{ fontSize: 11, color: "#475569", border: "1px solid #e5e7eb", borderRadius: 6, background: "#fff", padding: "6px 8px" }}>
                          <div style={{ fontWeight: 600 }}>{run.rule_version} · {run.scope_type}</div>
                          <div>{run.run_id.slice(0, 8)} | {run.status}</div>
                          {run.summary?.total_factors != null && (
                            <div>total {run.summary.total_factors} / success {run.summary.success_count ?? 0} / failed {run.summary.failed_count ?? 0}</div>
                          )}
                          {run.summary?.errors?.length ? (
                            <div style={{ marginTop: 4, color: "#b91c1c" }}>
                              最近错误: {run.summary.errors[0].factor_name} - {run.summary.errors[0].error}
                            </div>
                          ) : null}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
                <div style={{ background: "#f8fafc", border: "1px solid #e5e7eb", borderRadius: 8, padding: 12 }}>
                  <div style={{ fontSize: 12, fontWeight: 700, color: "#374151", marginBottom: 6 }}>本次执行结果</div>
                  {!ratingRunResult ? (
                    <div style={{ fontSize: 12, color: "#94a3b8" }}>尚未执行正式评级</div>
                  ) : ratingRunResult.ok === false ? (
                    <div style={{ fontSize: 12, color: "#b91c1c" }}>{ratingRunResult.errors?.[0]?.error || "正式评级失败"}</div>
                  ) : (
                    <div style={{ fontSize: 12, color: "#374151", lineHeight: 1.7 }}>
                      <div><strong>run_id:</strong> {ratingRunResult.run_id}</div>
                      <div><strong>total:</strong> {ratingRunResult.total_factors}</div>
                      <div><strong>success:</strong> {ratingRunResult.success_count} / <strong>failed:</strong> {ratingRunResult.failed_count}</div>
                    </div>
                  )}
                </div>
              </div>
              )}
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
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 12, flexWrap: "wrap" }}>
                <span style={{ fontSize: 12, color: "#6b7280" }}>起始日期:</span>
                <input
                  type="date"
                  value={snapshotStartDate}
                  onChange={e => setSnapshotStartDate(e.target.value)}
                  style={{ padding: "4px 8px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db", width: 140 }}
                />
                <span style={{ fontSize: 12, color: "#6b7280" }}>截止日期:</span>
                <input
                  type="date"
                  value={newSnapshotDate ? `${newSnapshotDate.slice(0,4)}-${newSnapshotDate.slice(4,6)}-${newSnapshotDate.slice(6,8)}` : ""}
                  onChange={e => setNewSnapshotDate(e.target.value.replace(/-/g, ""))}
                  max={new Date().toISOString().split("T")[0]}
                  style={{ padding: "4px 8px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db", width: 140 }}
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

      {/* 因子值缓存管理 */}
      {!isSelection && (
        <section style={{ background: "#fff", borderRadius: 12, padding: "10px 16px", marginBottom: 12, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
            <span style={{ fontWeight: 600, fontSize: 13, color: "#374151" }}>因子值缓存</span>
            {cacheStats ? (
              <>
                <span style={{ fontSize: 12, color: "#6b7280" }}>
                  启用 {cacheStats.hash_ok + cacheStats.hash_mismatch + (cacheStats.cache_error || 0)}/{cacheStats.total_code_factors}
                  {" "}<span style={{ color: "#059669" }}>✓{cacheStats.hash_ok}</span>
                  {(cacheStats.cache_error || 0) > 0 && <span style={{ color: "#dc2626" }}> ✗{cacheStats.cache_error}</span>}
                  {cacheStats.hash_mismatch > 0 && <span style={{ color: "#f59e0b" }}> △{cacheStats.hash_mismatch}</span>}
                  {cacheStats.no_cache > 0 && <span style={{ color: "#9ca3af" }}> —{cacheStats.no_cache}</span>}
                  {" "}|{" "}
                  {cacheStats.total_size_mb > 1024 ? `${(cacheStats.total_size_mb / 1024).toFixed(1)} GB` : `${cacheStats.total_size_mb} MB`} |
                  {" "}{cacheStats.date_range_dominant}
                  {cacheStats.by_source && (
                    <> | QE回测{cacheStats.by_source.backtest || 0}</>
                  )}
                </span>
                {cacheStats.disabled_total > 0 && (
                  <span style={{ fontSize: 11, color: "#9ca3af", background: "#f3f4f6", padding: "1px 6px", borderRadius: 4 }}>
                    禁用 {cacheStats.disabled_cached}/{cacheStats.disabled_total}
                  </span>
                )}
                <span style={{ fontSize: 11, color: "#9ca3af" }}>
                  {cacheStats.active_tasks > 0 && `⏳ ${cacheStats.active_tasks} 个任务运行中`}
                </span>
                {remoteStats && (
                  <span style={{ fontSize: 12, color: selectedRemoteNode?.reachable === false ? "#dc2626" : "#64748b", background: "#f8fafc", border: "1px solid #e5e7eb", borderRadius: 6, padding: "2px 8px" }}>
                    WSL缓存 {remoteStats.local.cached}
                    {selectedRemoteNode ? (
                      <>
                        {" "}｜远端 {selectedRemoteNode.display_name || selectedRemoteNode.node_id}: {selectedRemoteNode.remote_cached ?? 0}
                        {" "}｜已同步 <span style={{ color: "#059669" }}>{selectedRemoteNode.synced ?? 0}</span>
                        {(selectedRemoteNode.missing || 0) > 0 && <>｜缺失 <span style={{ color: "#d97706" }}>{selectedRemoteNode.missing}</span></>}
                        {(selectedRemoteNode.stale || 0) > 0 && <>｜过期 <span style={{ color: "#dc2626" }}>{selectedRemoteNode.stale}</span></>}
                      </>
                    ) : "｜无远端节点"}
                  </span>
                )}
                {remoteStatsLoading && <span style={{ fontSize: 11, color: "#94a3b8" }}>远端统计刷新中...</span>}
                {remoteStatsError && (
                  <span style={{ fontSize: 12, color: "#dc2626", background: "#fef2f2", border: "1px solid #fecaca", borderRadius: 6, padding: "2px 8px" }}>
                    远端统计失败：{remoteStatsError}
                  </span>
                )}
              </>
            ) : (
              <span style={{ fontSize: 12, color: "#9ca3af" }}>加载中...</span>
            )}
            <div style={{ marginLeft: "auto", display: "flex", gap: 6, alignItems: "center", flexWrap: "wrap" }}>
              {remoteStats?.remote_nodes?.length ? (
                <>
                  <span style={{ fontSize: 11, color: "#9ca3af" }}>远端:</span>
                  <select
                    value={selectedRemoteNodeId}
                    onChange={e => { setSelectedRemoteNodeId(e.target.value); fetchRemoteStats(e.target.value); }}
                    style={{ padding: "3px 6px", fontSize: 11, borderRadius: 4, border: "1px solid #d1d5db" }}
                  >
                    {remoteStats.remote_nodes.map(node => (
                      <option key={node.node_id} value={node.node_id}>
                        {node.display_name || node.node_id}{node.reachable === false ? " (不可达)" : ""}
                      </option>
                    ))}
                  </select>
                  <button onClick={() => triggerRemoteSync()} disabled={remoteSyncBusy}
                    title="增量同步本地已有但远端缺失、过期或上次同步失败的因子缓存文件"
                    style={{ padding: "4px 10px", fontSize: 11, borderRadius: 4, border: "1px solid #2563eb", background: "#eff6ff", color: "#2563eb", fontWeight: 600, cursor: "pointer", opacity: remoteSyncBusy ? 0.5 : 1 }}>
                    {remoteSyncBusy ? "同步中..." : "补充同步"}
                  </button>
                </>
              ) : null}
              <span style={{ fontSize: 11, color: "#9ca3af" }}>区间:</span>
              <input type="date" value={cacheStartDate} onChange={e => setCacheStartDate(e.target.value)} style={{ padding: "3px 6px", fontSize: 11, borderRadius: 4, border: "1px solid #d1d5db" }} />
              <span style={{ fontSize: 11, color: "#9ca3af" }}>~</span>
              <input type="date" value={cacheEndDate} onChange={e => setCacheEndDate(e.target.value)} style={{ padding: "3px 6px", fontSize: 11, borderRadius: 4, border: "1px solid #d1d5db" }} />
              <span style={{ fontSize: 11, color: "#9ca3af" }}>筛选:</span>
              <select
                value={cacheCoverageFilter}
                onChange={e => setCacheCoverageFilter(e.target.value)}
                title="按所选回测区间筛选因子值缓存。未覆盖包含无缓存、缓存结束日期不足、起点缺口超过60天或源码hash失效。"
                style={{ padding: "3px 6px", fontSize: 11, borderRadius: 4, border: "1px solid #d1d5db" }}
              >
                <option value="all">全部缓存状态</option>
                <option value="missing_range">未覆盖该区间</option>
                <option value="covers_range">已覆盖该区间</option>
                <option value="has_cache">已有缓存</option>
                <option value="no_cache">无缓存</option>
                <option value="hash_mismatch">源码变更</option>
              </select>
              <span style={{ fontSize: 11, color: "#9ca3af" }}>并行:</span>
              <select value={cacheWorkers} onChange={e => setCacheWorkers(Number(e.target.value))} style={{ padding: "3px 6px", fontSize: 11, borderRadius: 4, border: "1px solid #d1d5db" }}>
                {[1, 2, 4, 8].map(n => <option key={n} value={n}>{n}</option>)}
              </select>
              <button onClick={() => triggerCacheCompute()} disabled={cacheBusy}
                style={{ padding: "4px 10px", fontSize: 11, borderRadius: 4, border: "1px solid #059669", background: "#ecfdf5", color: "#059669", fontWeight: 600, cursor: "pointer", opacity: cacheBusy ? 0.5 : 1 }}>
                {cacheBusy ? "Submitting..." : "Official full compute"}
              </button>
              <button onClick={() => triggerCacheCompute(undefined, true)} disabled={cacheBusy}
                style={{ padding: "4px 10px", fontSize: 11, borderRadius: 4, border: "1px solid #8b5cf6", background: "#f5f3ff", color: "#7c3aed", fontWeight: 600, cursor: "pointer", opacity: cacheBusy ? 0.5 : 1 }}>
                Force recompute
              </button>
              {actualSelectedFactors.size > 0 && (
                <button onClick={() => triggerCacheCompute(Array.from(actualSelectedFactors).map(k => k.split("||")[0]))} disabled={cacheBusy}
                  style={{ padding: "4px 10px", fontSize: 11, borderRadius: 4, border: "1px solid #0284c7", background: "#f0f9ff", color: "#0284c7", fontWeight: 600, cursor: "pointer", opacity: cacheBusy ? 0.5 : 1 }}>
                  Compute selected ({actualSelectedFactors.size})
                </button>
              )}
              {actualSelectedFactors.size > 0 && remoteStats?.remote_nodes?.length ? (
                <button onClick={() => triggerRemoteSync(Array.from(actualSelectedFactors).map(k => k.split("||")[0]))} disabled={remoteSyncBusy}
                  style={{ padding: "4px 10px", fontSize: 11, borderRadius: 4, border: "1px solid #0f766e", background: "#f0fdfa", color: "#0f766e", fontWeight: 600, cursor: "pointer", opacity: remoteSyncBusy ? 0.5 : 1 }}>
                  Sync selected
                </button>
              ) : null}
              <button onClick={clearAllCache}
                style={{ padding: "4px 10px", fontSize: 11, borderRadius: 4, border: "1px solid #dc2626", background: "#fef2f2", color: "#dc2626", fontWeight: 600, cursor: "pointer" }}>
                Clear all
              </button>
              <button onClick={() => { fetchCacheStats(); fetchCacheTasks(); if (selectedCacheTaskId) fetchCacheTaskDetail(selectedCacheTaskId); }} style={{ padding: "4px 8px", fontSize: 10, borderRadius: 4, border: "1px solid #d1d5db", cursor: "pointer" }}>Refresh</button>
            </div>
          </div>
          {cacheTasks.length > 0 && (
            <div style={{ marginTop: 10, paddingTop: 10, borderTop: "1px solid #eef2f7" }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap", marginBottom: 8 }}>
                <span style={{ fontSize: 12, fontWeight: 600, color: "#475569" }}>最近任务</span>
                <span style={{ fontSize: 11, color: "#94a3b8" }}>{cacheTaskLoading ? "加载中..." : `${cacheTasks.length} 个任务`}</span>
              </div>
              <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                {cacheTasks.slice(0, 8).map(task => (
                  <button
                    key={task.task_id}
                    onClick={() => setSelectedCacheTaskId(task.task_id)}
                    style={{
                      padding: "4px 8px",
                      fontSize: 10,
                      borderRadius: 6,
                      border: selectedCacheTaskId === task.task_id ? "1px solid #7c3aed" : "1px solid #d1d5db",
                      background: selectedCacheTaskId === task.task_id ? "#f5f3ff" : "#fff",
                      color: selectedCacheTaskId === task.task_id ? "#7c3aed" : "#475569",
                      cursor: "pointer",
                    }}
                    title={task.task_id}
                  >
                    {task.task_id.slice(-8)} | {task.status}
                  </button>
                ))}
              </div>
              {selectedCacheTask && (
                <div style={{ marginTop: 10, background: "#f8fafc", border: "1px solid #e5e7eb", borderRadius: 8, padding: 10 }}>
                  <div style={{ fontSize: 12, color: "#475569", marginBottom: 6, lineHeight: 1.6 }}>
                    Task {selectedCacheTask.task_id} | status {selectedCacheTask.status} | window {selectedCacheTask.window_train_start || selectedCacheTask.start || "-"} ~ {selectedCacheTask.window_backtest_end || selectedCacheTask.end || "-"} | workers={selectedCacheTask.workers || "-"} | batch={selectedCacheTask.batch_size || "-"}
                    {selectedCacheTask.error ? ` | error: ${selectedCacheTask.error}` : ""}
                  </div>
                  <div style={{ fontSize: 12, color: "#374151", marginBottom: 6, lineHeight: 1.6 }}>
                    official offline | code={selectedCacheTask.code_source || "code_text"} | source={selectedCacheTask.data_source_mode || "official_offline_backtest_factor_data"} | node={selectedCacheTask.node_id || "-"}
                  </div>
                  {selectedCacheTask.factor_data_dir && (
                    <div style={{ fontSize: 11, color: "#64748b", marginBottom: 6, wordBreak: "break-all" }}>
                      factor_data_dir: {selectedCacheTask.factor_data_dir}
                    </div>
                  )}
                  <div style={{ fontSize: 11, color: "#64748b", marginBottom: 8 }}>
                    Official path uses WSL dispatch only; legacy resume/backfill is disabled. Retry failed factors by resubmitting selected factor_names or force recompute.
                  </div>
                  {selectedCacheTask.task_state && (
                    <div style={{ fontSize: 12, color: "#374151", marginBottom: 6 }}>
                      checkpoint: 成功 {selectedCacheTask.task_state.success_factors?.length ?? 0} |
                      失败 {selectedCacheTask.task_state.failed_factors?.length ?? 0} |
                      跳过 {selectedCacheTask.task_state.skipped_factors?.length ?? 0}
                    </div>
                  )}
                  {selectedCacheTask.result && (
                    <div style={{ fontSize: 12, color: "#374151", marginBottom: 6 }}>
                      result: success {selectedCacheTask.result.success ?? "-"} / failed {selectedCacheTask.result.failed ?? "-"} / skipped {selectedCacheTask.result.skipped ?? "-"} / total {selectedCacheTask.result.total ?? "-"}
                    </div>
                  )}
                  {selectedCacheTask.failed_tail && selectedCacheTask.failed_tail.length > 0 && (
                    <details style={{ marginBottom: 6 }}>
                      <summary style={{ fontSize: 12, cursor: "pointer", color: "#b91c1c" }}>最近失败因子 ({selectedCacheTask.failed_tail.length})</summary>
                      <div style={{ marginTop: 6, display: "flex", flexDirection: "column", gap: 4 }}>
                        {selectedCacheTask.failed_tail.map((item: any, idx: number) => (
                          <div key={`${item.factor_name || "failed"}-${idx}`} style={{ fontSize: 11, color: "#7f1d1d", background: "#fff", border: "1px solid #fecaca", borderRadius: 6, padding: "6px 8px" }}>
                            <div style={{ fontWeight: 600 }}>{item.factor_name}</div>
                            <div>{item.error_type || "Error"}: {item.error_short || item.error || "unknown error"}</div>
                          </div>
                        ))}
                      </div>
                    </details>
                  )}
                  {selectedCacheTask.recent_log && (
                    <details>
                      <summary style={{ fontSize: 12, cursor: "pointer", color: "#475569" }}>任务日志</summary>
                      <pre style={{ marginTop: 6, background: "#0f172a", color: "#e2e8f0", padding: 10, borderRadius: 6, fontSize: 10, overflowX: "auto", whiteSpace: "pre-wrap" }}>{selectedCacheTask.recent_log}</pre>
                    </details>
                  )}
                </div>
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
            onChange={e => {
              const nextSource = e.target.value;
              setSourceFilter(nextSource);
              setPage(1);
              if (nextSource === "alpha158" || nextSource === "alpha360") {
                setShowAlpha(true);
              }
            }}
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
            data-testid="qe-factor-search"
            placeholder="搜索因子名称..."
            value={search}
            onChange={e => setSearch(e.target.value)}
            onKeyDown={e => { if (e.key === "Enter") void loadData(e.currentTarget.value); }}
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
            onClick={() => loadData()}
            disabled={loading}
            style={{ padding: "6px 12px", fontSize: 12, cursor: "pointer", borderRadius: 6, border: "1px solid #d1d5db", background: "#fff" }}
          >
            {loading ? "加载中..." : "刷新"}
          </button>

          <label style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 12, cursor: "pointer" }}>
            <input
              type="checkbox"
              checked={showAlpha || isAlphaSourceFilter}
              disabled={isAlphaSourceFilter}
              onChange={e => setShowAlpha(e.target.checked)}
            />
            显示Alpha158/Alpha360因子
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
                {batchLoading ? "分析中..." : `批量分析说明(${actualSelectedFactors.size})`}
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
                  if (!activeSnapshot) { alert("请先选择数据快照后再执行全流程处理。"); return; }
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
              <strong>批量分析说明进行中：</strong> {batchProgress.current} / {batchProgress.total}
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
            <strong>批量分析说明完成：</strong>
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
                新增 {metricsResult.total_metrics_inserted ?? metricsResult.db_result?.inserted ?? 0} 条，跳过 {metricsResult.total_metrics_skipped ?? metricsResult.db_result?.skipped ?? 0} 条（已存在）
                {(metricsResult.fail_count ?? 0) > 0 && (
                  <span style={{ color: "#b45309" }}>（{metricsResult.fail_count} 个任务失败）</span>
                )}
                {(metricsResult.db_result?.errors?.length ?? 0) > 0 && (
                  <div style={{ marginTop: 6, color: "#b45309", fontSize: 11 }}>
                    <strong>入库警告 ({metricsResult.db_result?.errors?.length ?? 0} 条):</strong>
                    <div style={{ whiteSpace: "pre-wrap", maxHeight: 120, overflowY: "auto", marginTop: 4 }}>
                      {metricsResult.db_result?.errors?.slice(0, 10).join("\n")}
                      {(metricsResult.db_result?.errors?.length ?? 0) > 10 && `\n...还有 ${(metricsResult.db_result?.errors?.length ?? 0) - 10} 条`}
                    </div>
                  </div>
                )}
                {metricsResult.error && (
                  <div style={{ marginTop: 6, color: "#b45309", fontSize: 11 }}>
                    <strong>警告:</strong> {metricsResult.error}
                  </div>
                )}
              </span>
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                <span>
                  <strong>指标获取失败：</strong>
                  {metricsResult.error
                    || metricsResult.details?.filter((d: any) => !d.ok).map((d: any) => d.errors?.[0]).filter(Boolean).join("; ")
                    || metricsResult.db_result?.errors?.join("; ")
                    || "未知错误"}
                  {metricsResult.dispatch_status ? `（状态: ${metricsResult.dispatch_status}）` : ""}
                </span>
                {metricsResult.logs && metricsResult.logs.length > 0 && (
                  <div style={{
                    background: "#111827",
                    color: "#e5e7eb",
                    borderRadius: 6,
                    padding: 10,
                    fontFamily: "monospace",
                    fontSize: 11,
                    lineHeight: 1.6,
                    whiteSpace: "pre-wrap",
                    overflowX: "auto",
                    maxHeight: 260,
                  }}>
                    <div style={{ color: "#fca5a5", fontWeight: 700, marginBottom: 6 }}>计算日志</div>
                    {metricsResult.logs.join("\n")}
                  </div>
                )}
                {metricsResult.pipeline_summary?.factor_results?.some((f: any) => f.error) && (
                  <div style={{
                    background: "#1e1b2e",
                    color: "#e5e7eb",
                    borderRadius: 6,
                    padding: 10,
                    fontFamily: "monospace",
                    fontSize: 11,
                    lineHeight: 1.6,
                    whiteSpace: "pre-wrap",
                    overflowX: "auto",
                    maxHeight: 260,
                  }}>
                    <div style={{ color: "#fbbf24", fontWeight: 700, marginBottom: 6 }}>
                      因子执行详情（{metricsResult.pipeline_summary.factor_results.filter((f: any) => f.error).length} 个失败）
                    </div>
                    {metricsResult.pipeline_summary.factor_results
                      .filter((f: any) => f.error)
                      .slice(0, 10)
                      .map((f: any, i: number) => (
                        <div key={i} style={{ marginBottom: 4 }}>
                          <span style={{ color: "#f87171" }}>{f.name}</span>: {f.error}
                        </div>
                      ))}
                    {metricsResult.pipeline_summary.factor_results.filter((f: any) => f.error).length > 10 && (
                      <div style={{ color: "#9ca3af", marginTop: 4 }}>
                        ...还有 {metricsResult.pipeline_summary.factor_results.filter((f: any) => f.error).length - 10} 个失败因子
                      </div>
                    )}
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {error && <div style={{ marginTop: 8, padding: 8, background: "#fee2e2", borderRadius: 6, fontSize: 12 }}>{error}</div>}
      </section>

      {/* 月频 IC 衰变趋势图 */}
      {selectedFactor && (
        <MonthlyIcPanel factorName={selectedFactor} apiBase={API} />
      )}

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
                if (!activeSnapshot) { alert("请先选择数据快照后再执行全流程处理。"); return; }
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
                        if (!activeSnapshot) { alert("请先选择数据快照后再执行全流程处理。"); return; }
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
                                <th style={{ padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>独立IC</th>
                                <th style={{ padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>独立Sharpe</th>
                                <th style={{ padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>独立年化</th>
                                <th style={{ padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>SOTA</th>
                                <th style={{ padding: "4px 8px", color: "#6b7280", fontWeight: 500 }}>描述</th>
                              </tr>
                            </thead>
                            <tbody>
                              {factors.map((fac: any) => (
                                <tr key={fac.name} style={{ borderBottom: "1px solid #f3f4f6" }}>
                                  <td style={{ padding: "4px 8px", fontFamily: "monospace", fontWeight: 600, color: "#374151" }}>{fac.name}</td>
                                  <td style={{ padding: "4px 8px", color: "#6b7280" }}>{fac.factor_type || "-"}</td>
                                  <td style={{ padding: "4px 8px", color: "#374151" }}>{fac.ind_ic != null ? fac.ind_ic.toFixed(4) : "-"}</td>
                                  <td style={{ padding: "4px 8px", color: "#374151" }}>{fac.ind_sharpe != null ? fac.ind_sharpe.toFixed(3) : "-"}</td>
                                  <td style={{ padding: "4px 8px", color: "#374151" }}>{fac.ind_annual_return != null ? (fac.ind_annual_return * 100).toFixed(2) + "%" : "-"}</td>
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
                                        <td style={{ padding: "4px 8px", fontFamily: "monospace", fontWeight: 600, color: "#374151" }}>
                                          <span
                                            style={{ cursor: "pointer", textDecoration: selectedFactor === f.factor_name ? "underline" : "none" }}
                                            onClick={() => setSelectedFactor(selectedFactor === f.factor_name ? null : f.factor_name)}
                                            title="点击查看 IC 衰变趋势"
                                          >{f.factor_name}</span>
                                        </td>
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
                <th style={{ ...thStyle, cursor: "pointer" }} title="按 1D 独立 Rank IC 排序" onClick={() => handleSort("ind_rank_ic_1d")}>RIC 1D{getSortIndicator("ind_rank_ic_1d")}</th>
                <th style={{ ...thStyle, cursor: "pointer" }} title="按 5D 独立 Rank IC 排序" onClick={() => handleSort("ind_rank_ic_5d")}>RIC 5D{getSortIndicator("ind_rank_ic_5d")}</th>
                <th style={{ ...thStyle, cursor: "pointer" }} title="按 10D 独立 Rank IC 排序" onClick={() => handleSort("ind_rank_ic_10d")}>RIC 10D{getSortIndicator("ind_rank_ic_10d")}</th>
                <th style={{ ...thStyle, cursor: "pointer" }} title="按 20D 独立 Rank IC 排序" onClick={() => handleSort("ind_rank_ic_20d")}>RIC 20D{getSortIndicator("ind_rank_ic_20d")}</th>
                <th style={{ ...thStyle, cursor: "pointer" }} title="按 1D/5D/10D/20D 中 |Rank IC| 最大值排序，用于发现可反向使用的强信号" onClick={() => handleSort("ind_rank_ic_best_abs")}>Best |RIC|{getSortIndicator("ind_rank_ic_best_abs")}</th>
                <th style={{ ...thStyle, cursor: "pointer" }} onClick={() => handleSort("ind_sharpe")}>Sharpe(独立){getSortIndicator("ind_sharpe")}</th>
                <th style={{ ...thStyle, cursor: "pointer" }} onClick={() => handleSort("ind_annual_return")}>年化(独立){getSortIndicator("ind_annual_return")}</th>
                <th style={{ ...thStyle, cursor: "pointer" }} onClick={() => handleSort("has_ind_metrics")}>独立指标{getSortIndicator("has_ind_metrics")}</th>
                <th style={{ ...thStyle, cursor: "pointer", width: 50 }} onClick={() => handleSort("decay_status")}>衰变{getSortIndicator("decay_status")}</th>
                <th style={{ ...thStyle, cursor: "pointer", width: 90 }} onClick={() => handleSort("ind_calculated_at")}>指标计算{getSortIndicator("ind_calculated_at")}</th>
                <th style={{ ...thStyle, cursor: "pointer", width: 110 }} onClick={() => handleSort("cache_status")}>因子值缓存{getSortIndicator("cache_status")}</th>
                <th style={{ ...thStyle, width: 90 }}>远端同步</th>
                <th style={{ ...thStyle, cursor: "pointer", width: 90 }} onClick={() => handleSort("cache_start_date")}>缓存开始{getSortIndicator("cache_start_date")}</th>
                <th style={{ ...thStyle, cursor: "pointer", width: 90 }} onClick={() => handleSort("cache_end_date")}>缓存结束{getSortIndicator("cache_end_date")}</th>
                <th style={{ ...thStyle, cursor: "pointer", width: 120 }} onClick={() => handleSort("cache_computed_at")}>缓存计算{getSortIndicator("cache_computed_at")}</th>
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
                const horizonRankIc = [
                  { label: "1D", value: f.ind_rank_ic_1d },
                  { label: "5D", value: f.ind_rank_ic_5d },
                  { label: "10D", value: f.ind_rank_ic_10d },
                  { label: "20D", value: f.ind_rank_ic_20d },
                ];
                const bestHorizonRankIc = horizonRankIc.reduce<{ label: string; value: number } | null>((best, item) => {
                  if (item.value == null || !Number.isFinite(Number(item.value))) return best;
                  const value = Number(item.value);
                  return !best || Math.abs(value) > Math.abs(best.value) ? { label: item.label, value } : best;
                }, null);
                const bestRankIcAbs = f.ind_rank_ic_best_abs ?? (bestHorizonRankIc ? Math.abs(bestHorizonRankIc.value) : null);
                const cacheSourceLabel = f.cache_source_label || (f.cache_source === "backtest" ? "QE回测缓存" : "");
                const cacheTitleSuffix = [
                  cacheSourceLabel ? `来源: ${cacheSourceLabel}` : null,
                  f.cache_data_source_mode ? `数据模式: ${f.cache_data_source_mode}` : null,
                  f.cache_window_train_start || f.cache_window_backtest_end ? `请求窗口: ${f.cache_window_train_start || "-"} ~ ${f.cache_window_backtest_end || "-"}` : null,
                  f.cache_as_of_date ? `截至: ${f.cache_as_of_date}` : null,
                ].filter(Boolean).join("\n");
                const cacheTargetStart = cacheStartDate || cacheContext?.trainStart;
                const cacheTargetEnd = cacheEndDate || cacheContext?.backtestEnd;
                const cacheCoverageOk = factorCacheCoversRequestedWindow(f, cacheTargetStart, cacheTargetEnd);
                const remoteSyncStatus = remoteFactorStatusByName[f.factor_name];

                return (
                  <React.Fragment key={rowKey}>
                    <tr
                      data-testid={`qe-factor-row-${f.factor_name}`}
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
                          data-testid={`qe-factor-checkbox-${f.factor_name}`}
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
                        {f.official_grade ? (
                          <span style={{
                            padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 700,
                            background: GRADE_COLORS[f.official_grade] || "#6b7280",
                            color: "#fff",
                          }}>
                            {f.official_grade}
                          </span>
                        ) : (
                          <span style={{ color: "#d1d5db", fontSize: 10 }}>-</span>
                        )}
                        {f.official_score != null && (
                          <div style={{ fontSize: 10, color: "#6b7280", marginTop: 4 }}>
                            {f.official_score.toFixed(1)} {f.official_rule_version ? `· ${f.official_rule_version}` : ""}
                          </div>
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
                      {horizonRankIc.map(item => (
                        <td key={item.label} style={tdStyle}>
                          <span
                            title={item.value != null ? `${item.label} 独立 Rank IC: ${item.value.toFixed(4)}` : undefined}
                            style={{ color: item.value != null ? ((item.value ?? 0) > 0 ? "#059669" : "#dc2626") : "#9ca3af" }}
                          >
                            {item.value != null ? item.value.toFixed(4) : "-"}
                          </span>
                        </td>
                      ))}
                      <td style={tdStyle}>
                        <span
                          title={bestHorizonRankIc ? `最强周期 ${bestHorizonRankIc.label}: signed RankIC=${bestHorizonRankIc.value.toFixed(4)}` : undefined}
                          style={{ color: bestHorizonRankIc ? (bestHorizonRankIc.value > 0 ? "#059669" : "#dc2626") : "#9ca3af", fontWeight: bestHorizonRankIc ? 700 : 400 }}
                        >
                          {bestRankIcAbs != null ? bestRankIcAbs.toFixed(4) : "-"}
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
                      <td style={{ ...tdStyle, whiteSpace: "nowrap" }}>
                        {f.has_cache ? (
                          f.cache_hash_match === false ? (
                            <span title={`源码已变更，缓存失效\n${f.cache_date_range}${cacheTitleSuffix ? `\n${cacheTitleSuffix}` : ""}`} style={{ padding: "2px 6px", borderRadius: 4, fontSize: 10, fontWeight: 600, background: "#fee2e2", color: "#dc2626" }}>✗ hash不匹配</span>
                          ) : (
                            <span title={`${f.cache_date_range} (${f.cache_size_mb} MB)\n计算时间: ${f.cache_computed_at || "-"}${cacheTitleSuffix ? `\n${cacheTitleSuffix}` : ""}`} style={{ padding: "2px 6px", borderRadius: 4, fontSize: 10, fontWeight: 600, background: cacheCoverageOk ? "#d1fae5" : "#fef3c7", color: cacheCoverageOk ? "#059669" : "#d97706" }}>
                              {(() => {
                                if (!cacheTargetStart || !cacheTargetEnd || !f.cache_date_range?.includes("~")) {
                                  const prefix = `✓ ${f.cache_date_range?.split("~")[0]?.slice(0, 7) || "已缓存"}~${f.cache_date_range?.split("~")[1]?.slice(0, 7) || ""}`;
                                  return cacheSourceLabel ? `${prefix} · ${cacheSourceLabel}` : prefix;
                                }
                                const [cs, ce] = f.cache_date_range.split("~");
                                const prefix = cacheCoverageOk
                                  ? `✓ ${cs.slice(0, 7)}~${ce.slice(0, 7)}`
                                  : `△ ${cs.slice(0, 7)}~${ce.slice(0, 7)}`;
                                return cacheSourceLabel ? `${prefix} · ${cacheSourceLabel}` : prefix;
                              })()}
                            </span>
                          )
                        ) : (
                          f.cache_status === "error" ? (
                            <span title={`最近一次缓存计算失败${cacheTitleSuffix ? `\n${cacheTitleSuffix}` : ""}\n计算时间: ${f.cache_computed_at || "-"}`} style={{ padding: "2px 6px", borderRadius: 4, fontSize: 10, color: "#dc2626", background: "#fee2e2" }}>✗ 计算失败</span>
                          ) : (
                            <span title={cacheTitleSuffix || undefined} style={{ padding: "2px 6px", borderRadius: 4, fontSize: 10, color: "#9ca3af", background: "#f3f4f6" }}>— 无缓存</span>
                          )
                        )}
                      </td>
                      <td style={{ ...tdStyle, whiteSpace: "nowrap" }}>
                        {remoteSyncStatus ? (
                          <span
                            title={`本地: ${remoteSyncStatus.local_date_range || "-"}\n远端: ${remoteSyncStatus.remote_date_range || "-"}\n节点: ${selectedRemoteNode?.display_name || selectedRemoteNode?.node_id || "-"}`}
                            style={{
                              padding: "2px 6px",
                              borderRadius: 4,
                              fontSize: 10,
                              fontWeight: 600,
                              background: remoteSyncStatus.status === "synced" ? "#d1fae5" : remoteSyncStatus.status === "stale" ? "#fee2e2" : "#fef3c7",
                              color: remoteSyncStatus.status === "synced" ? "#059669" : remoteSyncStatus.status === "stale" ? "#dc2626" : "#d97706",
                            }}
                          >
                            {remoteSyncStatus.status === "synced" ? "已同步" : remoteSyncStatus.status === "stale" ? "远端过期" : "未同步"}
                          </span>
                        ) : (
                          <span style={{ color: "#d1d5db", fontSize: 10 }}>-</span>
                        )}
                      </td>
                      <td style={{ ...tdStyle, fontSize: 10, color: f.cache_start_date ? "#64748b" : "#d1d5db", whiteSpace: "nowrap" }}>
                        {f.cache_start_date || "-"}
                      </td>
                      <td style={{ ...tdStyle, fontSize: 10, color: f.cache_end_date ? "#64748b" : "#d1d5db", whiteSpace: "nowrap" }}>
                        {f.cache_end_date || "-"}
                      </td>
                      <td style={{ ...tdStyle, fontSize: 10, color: f.cache_computed_at ? "#64748b" : "#d1d5db", whiteSpace: "nowrap" }} title={f.cache_computed_at || undefined}>
                        {f.cache_computed_at ? f.cache_computed_at.slice(0, 16).replace("T", " ") : "-"}
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
                        {!isSelection && f.has_cache && (
                          <span
                            onClick={(e) => { e.stopPropagation(); clearOneCache(f.factor_name); }}
                            title="删除该因子缓存"
                            style={{
                              color: "#b45309", cursor: "pointer", fontSize: 10,
                              opacity: 0.7, userSelect: "none",
                            }}
                            onMouseEnter={e => (e.currentTarget.style.opacity = "1")}
                            onMouseLeave={e => (e.currentTarget.style.opacity = "0.7")}
                          >清缓存</span>
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
        <td colSpan={23} style={{ padding: "0 10px 10px 10px" }}>
                          <div style={{
                            background: isSelection ? "#eff6ff" : "#faf5ff", borderRadius: 8, padding: "10px 14px",
                            fontSize: 12, lineHeight: 1.7, color: "#374151",
                            borderLeft: isSelection ? "3px solid #3b82f6" : "3px solid #7c3aed",
                          }}>
                            {isDetailLoading && (
                              <div style={{ color: "#9ca3af", fontSize: 11, marginBottom: 8 }}>加载详情中...</div>
                            )}

                            {!f.description && f.llm_audit_summary && (
                        <div>
                          <strong style={{ color: isSelection ? "#1d4ed8" : "#7c3aed", fontSize: 11 }}>规则审阅摘要</strong>
                          <div style={{ marginTop: 4 }}>{f.llm_audit_summary}</div>
                        </div>
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

                            {/* v2 分类维度 (多Alpha + 周期 + 聚类) */}
                            {(f.ts_info_density || f.cross_horizon_consistency != null ||
                              f.direction != null || f.signal_mechanism || f.horizon_class ||
                              f.linearity || f.data_source_group || f.cluster_id != null) && (
                              <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #e5e7eb" }}>
                                <strong style={{ color: "#0891b2", fontSize: 11 }}>v2 分类维度</strong>
                                <div style={{
                                  marginTop: 4, display: "grid",
                                  gridTemplateColumns: "auto 1fr auto 1fr",
                                  gap: "3px 10px", fontSize: 11, color: "#4b5563",
                                }}>
                                  {f.ts_info_density && (<>
                                    <span style={{ color: "#6b7280" }}>时序信息密度:</span>
                                    <span style={{ fontWeight: 600, color:
                                      f.ts_info_density === "high" ? "#059669" :
                                      f.ts_info_density === "low" ? "#dc2626" : "#6b7280" }}>
                                      {f.ts_info_density}
                                    </span>
                                  </>)}
                                  {f.cross_horizon_consistency != null && (<>
                                    <span style={{ color: "#6b7280" }}>跨窗口一致性:</span>
                                    <span style={{ fontWeight: 600, color:
                                      f.cross_horizon_consistency >= 0.67 ? "#059669" :
                                      f.cross_horizon_consistency <= 0.33 ? "#dc2626" : "#f59e0b" }}>
                                      {f.cross_horizon_consistency.toFixed(2)}
                                    </span>
                                  </>)}
                                  {f.direction != null && (<>
                                    <span style={{ color: "#6b7280" }}>方向:</span>
                                    <span>
                                      {f.direction === 1 ? "多头 (+1)" :
                                       f.direction === -1 ? "空头 (-1)" : "双向 (0)"}
                                    </span>
                                  </>)}
                                  {f.signal_mechanism && (<>
                                    <span style={{ color: "#6b7280" }}>信号机制:</span>
                                    <span>{f.signal_mechanism}</span>
                                  </>)}
                                  {f.horizon_class && (<>
                                    <span style={{ color: "#6b7280" }}>持有周期:</span>
                                    <span>
                                      {f.horizon_class}
                                      {f.best_horizon != null ? ` (最佳${f.best_horizon}天)` : ""}
                                    </span>
                                  </>)}
                                  {f.best_horizon_advantage != null && (<>
                                    <span style={{ color: "#6b7280" }}>最佳窗口优势:</span>
                                    <span>{(f.best_horizon_advantage * 100).toFixed(1)}%</span>
                                  </>)}
                                  {f.linearity && (<>
                                    <span style={{ color: "#6b7280" }}>线性度:</span>
                                    <span>{f.linearity}</span>
                                  </>)}
                                  {f.data_source_group && (<>
                                    <span style={{ color: "#6b7280" }}>数据源组:</span>
                                    <span>{f.data_source_group}</span>
                                  </>)}
                                  {f.update_freq && (<>
                                    <span style={{ color: "#6b7280" }}>更新频率:</span>
                                    <span>{f.update_freq}</span>
                                  </>)}
                                  {f.holding_period_class && (<>
                                    <span style={{ color: "#6b7280" }}>半衰期分类:</span>
                                    <span>{f.holding_period_class}</span>
                                  </>)}
                                  {f.sector_exposure_corr != null && (<>
                                    <span style={{ color: "#6b7280" }}>行业相关性:</span>
                                    <span>{f.sector_exposure_corr.toFixed(2)}</span>
                                  </>)}
                                  {f.ic_sign_consistency_12m != null && (<>
                                    <span style={{ color: "#6b7280" }}>12M IC 符号一致性:</span>
                                    <span>{f.ic_sign_consistency_12m.toFixed(2)}</span>
                                  </>)}
                                  {f.ic_oos_is_ratio != null && (<>
                                    <span style={{ color: "#6b7280" }}>OOS/IS IC:</span>
                                    <span>{f.ic_oos_is_ratio.toFixed(2)}</span>
                                  </>)}
                                  {f.monthly_ic_trend_slope != null && (<>
                                    <span style={{ color: "#6b7280" }}>月度IC趋势:</span>
                                    <span style={{ color:
                                      f.monthly_ic_trend_slope < -0.001 ? "#dc2626" :
                                      f.monthly_ic_trend_slope > 0.001 ? "#059669" : "#6b7280" }}>
                                      {f.monthly_ic_trend_slope.toFixed(4)}
                                    </span>
                                  </>)}
                                  {f.cluster_id != null && (<>
                                    <span style={{ color: "#6b7280" }}>聚类:</span>
                                    <span>
                                      #{f.cluster_id}
                                      {f.cluster_role ? ` · ${f.cluster_role}` : ""}
                                      {f.cluster_size != null ? ` · size=${f.cluster_size}` : ""}
                                      {f.intra_cluster_max_corr != null ? ` · maxCorr=${f.intra_cluster_max_corr.toFixed(2)}` : ""}
                                    </span>
                                  </>)}
                                  {f.representative_score != null && (<>
                                    <span style={{ color: "#6b7280" }}>代表度:</span>
                                    <span>{f.representative_score.toFixed(2)}</span>
                                  </>)}
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
                            {f.official_grade_reason && (
                              <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #e5e7eb" }}>
                                <strong style={{ color: "#ea580c", fontSize: 11 }}>评级原因</strong>
                                <div style={{ marginTop: 4 }}>{f.official_grade_reason}</div>
                                {f.official_grade_reason_structured?.failed_gates?.length ? (() => {
                                  const gates = f.official_grade_reason_structured.failed_gates;
                                  const aFails = gates.filter((g: string) => g.startsWith("a_"));
                                  const sFails = gates.filter((g: string) => g.startsWith("s_"));
                                  const others = gates.filter((g: string) => !g.startsWith("a_") && !g.startsWith("s_"));
                                  // 注意: a_*/s_* 都是等级门槛(封顶), 不直接导致 D 级;
                                  // 真正强制 D 的是 hard_gate_flags.a_core_ic=false 或 overfit_force_d=true
                                  return (
                                    <>
                                      {aFails.length > 0 && (
                                        <div style={{ marginTop: 6, fontSize: 11, color: "#92400e" }}>
                                          未达 A 级门槛 (封顶 B/C/D): {aFails.join(", ")}
                                        </div>
                                      )}
                                      {sFails.length > 0 && (
                                        <div style={{ marginTop: 6, fontSize: 11, color: "#6b7280" }}>
                                          未达 S 级门槛 (封顶 A/B/C/D): {sFails.join(", ")}
                                        </div>
                                      )}
                                      {others.length > 0 && (
                                        <div style={{ marginTop: 6, fontSize: 11, color: "#991b1b" }}>
                                          其他门槛: {others.join(", ")}
                                        </div>
                                      )}
                                    </>
                                  );
                                })() : null}
                              </div>
                            )}

                            {detail?.llm_audit_summary && (
                              <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px dashed #e5e7eb" }}>
                                <strong style={{ color: "#2563eb", fontSize: 11 }}>LLM审阅</strong>
                                <div style={{ marginTop: 4 }}>{detail.llm_audit_summary}</div>
                                {Array.isArray(detail.llm_risk_notes) && detail.llm_risk_notes.length > 0 && (
                                  <ul style={{ marginTop: 6, paddingLeft: 18, color: "#4b5563" }}>
                                    {detail.llm_risk_notes.map((note: string, idx: number) => (
                                      <li key={`${rowKey}-audit-${idx}`}>{note}</li>
                                    ))}
                                  </ul>
                                )}
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
                                    {detail?.asset_path && (
                                      <>
                                        <span style={{ color: "#6b7280", whiteSpace: "nowrap" }}>代码路径:</span>
                                        <code style={{ background: "#f3f4f6", padding: "1px 4px", borderRadius: 3, fontSize: 10 }}>
                                          {detail.asset_path}
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
                                  <strong style={{ color: "#6366f1", fontSize: 11 }}>IC 月度衰变趋势</strong>
                                  <span style={{ fontSize: 10, color: "#9ca3af", marginLeft: 8 }}>
                                    月频 IC 均值 + 6 个月 EWMA 趋势线
                                  </span>
                                  <MonthlyIcPanel factorName={fname} apiBase={API} />
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
        dataDate={activeSnapshot || undefined}
        onClose={() => setManualDialogOpen(false)}
        onCreated={() => {
          loadData();
          loadSourceTasks();
          loadIndSummary();
        }}
      />

      {cleanupOpen && (
        <div
          onClick={() => !cleanupExecuting && setCleanupOpen(false)}
          style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.5)", zIndex: 9999, display: "flex", alignItems: "center", justifyContent: "center", padding: 20 }}
        >
          <div
            onClick={(e) => e.stopPropagation()}
            style={{ background: "#fff", borderRadius: 12, padding: 20, maxWidth: 1400, width: "100%", maxHeight: "92vh", display: "flex", flexDirection: "column", boxShadow: "0 20px 60px rgba(0,0,0,0.3)" }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12, borderBottom: "1px solid #e5e7eb", paddingBottom: 12 }}>
              <h2 style={{ margin: 0, fontSize: 18, color: "#111827" }}>🧹 因子清洗</h2>
              <button
                onClick={() => !cleanupExecuting && setCleanupOpen(false)}
                disabled={cleanupExecuting}
                style={{ background: "transparent", border: "none", fontSize: 22, cursor: cleanupExecuting ? "not-allowed" : "pointer", color: "#6b7280" }}
              >
                ×
              </button>
            </div>

            {/* 步骤 1: 规则选择 + 执行 dry-run */}
            {!cleanupResult && !cleanupExecuteResult && (
              <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                <div style={{ background: "#fff7ed", padding: 12, borderRadius: 8, fontSize: 12, color: "#9a3412", lineHeight: 1.7 }}>
                  <div style={{ fontWeight: 700, marginBottom: 4 }}>清洗规则（后端产品化服务）</div>
                  <div>• <strong>near_identical</strong> — 簇内冗余: cluster_role=&apos;member&apos; (complete-linkage 阈值 0.999)</div>
                  <div>• <strong>pure_noise_v2</strong> — 纯噪声: grade=D + |ic|&lt;0.003 + |rank_ic|&lt;0.003 + pos∈[0.45,0.55] + |rank_icir|&lt;0.1</div>
                  <div>• <strong>reverse_redundant</strong> — 反向重复: corr ≤ -0.999, 留正 IC / |IC| 大者</div>
                </div>

                <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                  {(["near_identical", "pure_noise_v2", "reverse_redundant"] as const).map(rule => (
                    <label key={rule} style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13, cursor: "pointer" }}>
                      <input
                        type="checkbox"
                        checked={cleanupRules[rule]}
                        onChange={(e) => setCleanupRules(prev => ({ ...prev, [rule]: e.target.checked }))}
                      />
                      <code style={{ background: "#f3f4f6", padding: "2px 6px", borderRadius: 4 }}>{rule}</code>
                    </label>
                  ))}
                </div>

                <div style={{ display: "flex", gap: 8, justifyContent: "flex-end", marginTop: 8 }}>
                  <button
                    onClick={() => setCleanupOpen(false)}
                    style={{ padding: "8px 16px", fontSize: 13, borderRadius: 6, border: "1px solid #d1d5db", background: "#fff", color: "#374151", cursor: "pointer" }}
                  >
                    取消
                  </button>
                  <button
                    onClick={async () => {
                      const rules = Object.entries(cleanupRules).filter(([_, v]) => v).map(([k]) => k);
                      if (rules.length === 0) { alert("至少选一条规则"); return; }
                      setCleanupLoading(true);
                      try {
                        const res = await fetch(`${API}/quantevolver/factors/cleanup/preview`, {
                          method: "POST",
                          headers: { "Content-Type": "application/json" },
                          body: JSON.stringify({ rules }),
                        });
                        const data = await res.json();
                        if (!res.ok || !data.ok) throw new Error(data.detail || "预览失败");
                        setCleanupResult({ summary: data.summary, candidates: data.candidates, reverse_pairs: data.reverse_pairs });
                        setCleanupSelected(new Set(data.candidates.map((c: any) => c.id)));
                      } catch (e: any) {
                        alert(`预览失败: ${e?.message || "未知错误"}`);
                      } finally {
                        setCleanupLoading(false);
                      }
                    }}
                    disabled={cleanupLoading}
                    style={{ padding: "8px 16px", fontSize: 13, borderRadius: 6, border: "none", background: "#ea580c", color: "#fff", fontWeight: 700, cursor: cleanupLoading ? "wait" : "pointer", opacity: cleanupLoading ? 0.6 : 1 }}
                  >
                    {cleanupLoading ? "扫描中..." : "🔍 Dry-run 预览"}
                  </button>
                </div>
              </div>
            )}

            {/* 步骤 2: 候选清单 */}
            {cleanupResult && !cleanupExecuteResult && (
              <div style={{ display: "flex", flexDirection: "column", gap: 12, overflow: "hidden", flex: 1 }}>
                <div style={{ background: "#fef3c7", padding: 12, borderRadius: 8, fontSize: 12, color: "#92400e", display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 8 }}>
                  <div><strong>启用因子</strong>: {cleanupResult.summary.total_enabled}</div>
                  <div><strong>候选 disable</strong>: <span style={{ fontSize: 14, color: "#dc2626" }}>{cleanupResult.summary.total_candidates}</span></div>
                  <div><strong>清洗后</strong>: {cleanupResult.summary.after_cleanup}</div>
                  <div><strong>已勾选</strong>: {cleanupSelected.size}</div>
                  {Object.entries(cleanupResult.summary.by_rule).map(([k, v]) => (
                    <div key={k} style={{ fontSize: 11 }}>
                      <code>{k}</code>: {v}
                    </div>
                  ))}
                </div>

                <div style={{ flex: 1, overflow: "auto", border: "1px solid #e5e7eb", borderRadius: 6 }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                    <thead style={{ position: "sticky", top: 0, background: "#f9fafb", zIndex: 1 }}>
                      <tr>
                        <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>
                          <input
                            type="checkbox"
                            checked={cleanupSelected.size === cleanupResult.candidates.length}
                            onChange={(e) => {
                              if (e.target.checked) setCleanupSelected(new Set(cleanupResult.candidates.map(c => c.id)));
                              else setCleanupSelected(new Set());
                            }}
                          />
                        </th>
                        <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>规则</th>
                        <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>因子名</th>
                        <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>来源</th>
                        <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>评级</th>
                        <th style={{ padding: 6, textAlign: "right", borderBottom: "1px solid #e5e7eb" }}>ic</th>
                        <th style={{ padding: 6, textAlign: "right", borderBottom: "1px solid #e5e7eb" }}>rank_ic</th>
                        <th style={{ padding: 6, textAlign: "right", borderBottom: "1px solid #e5e7eb" }}>icir</th>
                        <th style={{ padding: 6, textAlign: "right", borderBottom: "1px solid #e5e7eb" }}>pos%</th>
                        <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>cluster</th>
                        <th style={{ padding: 6, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>详情</th>
                      </tr>
                    </thead>
                    <tbody>
                      {cleanupResult.candidates.map(c => {
                        const checked = cleanupSelected.has(c.id);
                        const ruleColor = c.cleanup_rule === "reverse_redundant" ? "#dc2626"
                                        : c.cleanup_rule === "near_identical" ? "#7c3aed"
                                        : "#ea580c";
                        return (
                          <tr key={c.id} style={{ background: checked ? "#fff" : "#f9fafb", opacity: checked ? 1 : 0.5 }}>
                            <td style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}>
                              <input
                                type="checkbox"
                                checked={checked}
                                onChange={(e) => {
                                  const next = new Set(cleanupSelected);
                                  if (e.target.checked) next.add(c.id); else next.delete(c.id);
                                  setCleanupSelected(next);
                                }}
                              />
                            </td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}>
                              <span style={{ fontSize: 10, padding: "2px 6px", borderRadius: 4, background: ruleColor, color: "#fff", fontWeight: 700 }}>
                                {c.cleanup_rule}
                              </span>
                            </td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f3f4f6", fontFamily: "monospace" }}>{c.factor_name}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f3f4f6", color: "#6b7280" }}>{c.source}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}>{c.official_grade || "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f3f4f6", textAlign: "right", fontFamily: "monospace" }}>{c.ind_ic !== null ? Number(c.ind_ic).toFixed(4) : "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f3f4f6", textAlign: "right", fontFamily: "monospace" }}>{c.ind_rank_ic !== null ? Number(c.ind_rank_ic).toFixed(4) : "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f3f4f6", textAlign: "right", fontFamily: "monospace" }}>{c.ind_icir !== null ? Number(c.ind_icir).toFixed(3) : "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f3f4f6", textAlign: "right", fontFamily: "monospace" }}>{c.ic_positive_ratio !== null ? (Number(c.ic_positive_ratio) * 100).toFixed(1) : "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f3f4f6", color: "#6b7280" }}>{c.cluster_id ? `#${c.cluster_id}/${c.cluster_role}` : "-"}</td>
                            <td style={{ padding: 6, borderBottom: "1px solid #f3f4f6", color: "#6b7280", fontSize: 10 }}>{c.cleanup_detail}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>

                <div style={{ display: "flex", gap: 8, justifyContent: "flex-end", paddingTop: 8, borderTop: "1px solid #e5e7eb" }}>
                  <button
                    onClick={() => { setCleanupResult(null); setCleanupSelected(new Set()); }}
                    disabled={cleanupExecuting}
                    style={{ padding: "8px 16px", fontSize: 13, borderRadius: 6, border: "1px solid #d1d5db", background: "#fff", color: "#374151", cursor: cleanupExecuting ? "not-allowed" : "pointer" }}
                  >
                    ← 重新预览
                  </button>
                  <button
                    onClick={async () => {
                      if (cleanupSelected.size === 0) { alert("请勾选至少一个因子"); return; }
                      if (!confirm(`将禁用 ${cleanupSelected.size} 个因子, 不可在此页面撤销 (需用 batch_id 回滚). 确认?`)) return;
                      setCleanupExecuting(true);
                      try {
                        const ids = Array.from(cleanupSelected);
                        const reasons: Record<string, string> = {};
                        for (const c of cleanupResult.candidates) {
                          if (cleanupSelected.has(c.id)) reasons[String(c.id)] = c.cleanup_reason;
                        }
                        const res = await fetch(`${API}/quantevolver/factors/cleanup/execute`, {
                          method: "POST",
                          headers: { "Content-Type": "application/json" },
                          body: JSON.stringify({ factor_ids: ids, reasons }),
                        });
                        const data = await res.json();
                        if (!res.ok) throw new Error(data.detail || "执行失败");
                        setCleanupExecuteResult(data);
                        loadData();
                      } catch (e: any) {
                        alert(`执行失败: ${e?.message || "未知错误"}`);
                      } finally {
                        setCleanupExecuting(false);
                      }
                    }}
                    disabled={cleanupExecuting || cleanupSelected.size === 0}
                    style={{ padding: "8px 16px", fontSize: 13, borderRadius: 6, border: "none", background: "#dc2626", color: "#fff", fontWeight: 700, cursor: cleanupExecuting || cleanupSelected.size === 0 ? "not-allowed" : "pointer", opacity: cleanupExecuting || cleanupSelected.size === 0 ? 0.5 : 1 }}
                  >
                    {cleanupExecuting ? "执行中..." : `⚠ 正式禁用 (${cleanupSelected.size})`}
                  </button>
                </div>
              </div>
            )}

            {/* 步骤 3: 执行结果 */}
            {cleanupExecuteResult && (
              <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                <div style={{ background: cleanupExecuteResult.ok ? "#dcfce7" : "#fee2e2", padding: 16, borderRadius: 8, fontSize: 13, color: cleanupExecuteResult.ok ? "#15803d" : "#b91c1c" }}>
                  <div style={{ fontSize: 16, fontWeight: 700, marginBottom: 8 }}>
                    {cleanupExecuteResult.ok ? "✓ 执行成功" : "✗ 执行失败"}
                  </div>
                  <div>批次 ID: <code>{cleanupExecuteResult.batch_id}</code></div>
                  <div>已禁用: <strong>{cleanupExecuteResult.disabled_count}</strong> 个因子</div>
                  <div style={{ marginTop: 6 }}>分布:</div>
                  {Object.entries(cleanupExecuteResult.by_reason).map(([r, n]) => (
                    <div key={r} style={{ marginLeft: 12 }}>• <code>{r}</code>: {n}</div>
                  ))}
                  {cleanupExecuteResult.errors.length > 0 && (
                    <div style={{ marginTop: 8, color: "#b91c1c" }}>
                      错误: {cleanupExecuteResult.errors.join("; ")}
                    </div>
                  )}
                </div>

                <div style={{ background: "#f3f4f6", padding: 12, borderRadius: 8, fontSize: 11, fontFamily: "monospace", color: "#374151" }}>
                  <div style={{ fontSize: 12, fontWeight: 700, marginBottom: 4 }}>回滚 SQL (保留备用):</div>
                  {cleanupExecuteResult.rollback_sql}
                </div>

                <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
                  <button
                    onClick={() => { setCleanupOpen(false); setCleanupResult(null); setCleanupExecuteResult(null); setCleanupSelected(new Set()); }}
                    style={{ padding: "8px 16px", fontSize: 13, borderRadius: 6, border: "none", background: "#2563eb", color: "#fff", fontWeight: 700, cursor: "pointer" }}
                  >
                    完成
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
