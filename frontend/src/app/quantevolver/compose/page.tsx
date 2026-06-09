"use client";

import React, { Suspense, useEffect, useState, useCallback, useMemo } from "react";
import { useSearchParams } from "next/navigation";
import dynamic from "next/dynamic";
import FactorList from "../components/FactorList";
import ModelList from "../components/ModelList";
import MultiAlphaModeToggle from "../components/MultiAlphaModeToggle";
import MultiAlphaGroupEditor, { type MultiAlphaConfig } from "../components/MultiAlphaGroupEditor";
import MultiAlphaResults from "../components/MultiAlphaResults";
import MultiAlphaProgress from "../components/MultiAlphaProgress";
import { useExperimentSSE } from "../components/useExperimentSSE";
import LogTerminal from "../components/LogTerminal";
import MetricsSummary from "../components/MetricsSummary";
import SectorBlacklistPanel from "../components/SectorBlacklistPanel";
import ParamSchemaForm from "../evolution/components/ParamSchemaForm";

const IcSeriesChart = dynamic(() => import("../components/charts/IcSeriesChart"), { ssr: false });
const LossCurveChart = dynamic(() => import("../components/charts/LossCurveChart"), { ssr: false });
const ReturnCurveChart = dynamic(() => import("../components/charts/ReturnCurveChart"), { ssr: false });

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";
const MULTI_ALPHA_DISTRIBUTED_ENABLED =
  process.env.NEXT_PUBLIC_MULTI_ALPHA_DISTRIBUTED_ENABLED === "1";
const QE_DEFAULT_SIGNAL_END = "2026-04-28";
const QE_DEFAULT_BACKTEST_END = "2026-04-27";
const DEFAULT_QE_DATA_SPLIT = {
  train_start: "2018-08-01",
  train_end: "2022-12-31",
  valid_start: "2023-01-01",
  valid_end: "2024-06-30",
  test_start: "2024-07-01",
  test_end: QE_DEFAULT_SIGNAL_END,
  backtest_end: QE_DEFAULT_BACKTEST_END,
};
const DEFAULT_QE_RANDOM_SEED = 20260522;
const QE_RANDOM_SEED_MAX = 4294967295;

function isValidQeRandomSeed(value: number) {
  return Number.isInteger(value) && value >= 0 && value <= QE_RANDOM_SEED_MAX;
}

function parseQeRandomSeedInput(value: string, fallback = DEFAULT_QE_RANDOM_SEED) {
  const parsed = Number(value);
  return isValidQeRandomSeed(parsed) ? parsed : fallback;
}

function deriveBacktestEnd(testEnd?: string) {
  if (!testEnd) return QE_DEFAULT_BACKTEST_END;
  return testEnd >= QE_DEFAULT_SIGNAL_END ? QE_DEFAULT_BACKTEST_END : testEnd;
}

function withSafeBacktestEnd(split?: Record<string, any>) {
  const next = { ...DEFAULT_QE_DATA_SPLIT, ...(split || {}) };
  if (!next.backtest_end) {
    next.backtest_end = deriveBacktestEnd(next.test_end);
  }
  return next;
}

const DATA_SOURCE_MAP: Record<string, string> = {
  daily_pv: "日线行情", daily_basic: "每日基本面", moneyflow: "个股资金流向", cyq_perf: "筹码分布", bak_basic: "股票历史信息", multi: "多数据源",
};
const FACTOR_TYPE_MAP: Record<string, string> = {
  CrossSection: "截面因子", TimeSeries: "时序因子",
};
const GRADE_ORDER: Record<string, number> = { S: 0, A: 1, B: 2, C: 3, D: 4 };

type Factor = { factor_name: string; source: string; ic?: number | null; sharpe?: number | null; is_sota_factor?: boolean;
  category?: string; official_grade?: string | null; official_score?: number | null; description?: string; ind_annual_return?: number | null; factor_type?: string; data_source?: string };
type Model = { model_id: string; model_name: string; model_type?: string; ic?: number; annualized_return?: number; is_sota?: boolean; display_name?: string; description?: string };
type Strategy = { strategy_id: string; display_name: string; strategy_type?: string; portfolio_config?: any; description?: string; market?: string; catalog_source?: string; };

type EvalResult = {
  ok: boolean;
  risks?: { level: string; type: string; message: string }[];
  suggestions?: string[];
  overall_score?: number;
  llm_commentary?: string;
};

type ConfigResult = {
  ok: boolean;
  experiment_id?: string;
  experiment_dir?: string;
  wsl_command?: string;
  operation?: string;
  editable?: boolean;
  startable?: boolean;
  error?: string;
  // 多Alpha 特有字段
  alpha_mode?: string;
  execution_mode?: string;
  total_groups?: number;
  meta_method?: string;
  group_configs?: Array<{ group_name: string; node_id: string; model_id: string; factor_count: number; reuse_mode?: string }>;
  node_distribution?: Record<string, string[]>;
  is_distributed?: boolean;
};

/* ━━ 类型标签颜色 ━━ */
const TYPE_COLORS: Record<string, { bg: string; text: string; label: string }> = {
  daily: { bg: "#dbeafe", text: "#2563eb", label: "日频" },
  intraday: { bg: "#fef3c7", text: "#d97706", label: "日内" },
};

function ComposePageContent() {
  const searchParams = useSearchParams();
  const editExperimentId = searchParams.get("edit_experiment_id") || "";
  const isEditPendingExperiment = Boolean(editExperimentId);
  const [currentStep, setCurrentStep] = useState(1);
  const STEPS = ["因子选择", "模型选择", "策略选择", "组合配置与评估", "生成执行与下发"];

  /* ━━ 公共样式常量（与 evolution 页面统一） ━━ */
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

  const btnPrimary: React.CSSProperties = {
    padding: "8px 20px",
    backgroundColor: "#2563eb",
    color: "#fff",
    border: "none",
    borderRadius: "6px",
    fontSize: "13px",
    fontWeight: 600,
    cursor: "pointer",
    display: "inline-flex",
    alignItems: "center",
    gap: "6px",
    boxShadow: "0 2px 4px rgba(37, 99, 235, 0.2)",
    transition: "background-color 0.2s",
  };

  const btnSecondary: React.CSSProperties = {
    padding: "8px 20px",
    backgroundColor: "#f1f5f9",
    color: "#475569",
    border: "1px solid #e2e8f0",
    borderRadius: "6px",
    fontSize: "13px",
    fontWeight: 600,
    cursor: "pointer",
    display: "inline-flex",
    alignItems: "center",
    gap: "6px",
    transition: "background-color 0.2s",
  };

  const inputStyle: React.CSSProperties = {
    width: "100%",
    padding: "8px 12px",
    borderRadius: "6px",
    border: "1px solid #cbd5e1",
    fontSize: "14px",
    boxSizing: "border-box",
    outline: "none",
  };

  const labelStyle: React.CSSProperties = {
    display: "block",
    fontSize: "13px",
    fontWeight: 600,
    color: "#475569",
    marginBottom: "6px",
  };

  /* ── AI智能生成 ── */
  const [userRequirement, setUserRequirement] = useState("");
  const [aiLoading, setAiLoading] = useState(false);
  const [aiResult, setAiResult] = useState<any>(null);

  /* ── 数据集 ── */
  const [factors, setFactors] = useState<Factor[]>([]);
  const [models, setModels] = useState<Model[]>([]);
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [loading, setLoading] = useState(true);

  /* ── 选择状态 ── */
  const [selectedFactors, setSelectedFactors] = useState<Set<string>>(new Set());
  const [selectedModel, setSelectedModel] = useState<string>("");
  const [selectedStrategy, setSelectedStrategy] = useState<string>("");

  /* ── 因子列表控制 ── */
  const [factorPage, setFactorPage] = useState(1);
  const factorPageSize = 20;
  const [factorTypeFilter, setFactorTypeFilter] = useState<string>("");
  const [dataSourceFilter, setDataSourceFilter] = useState<string>("");
  const [factorSourceTab, setFactorSourceTab] = useState<string>("all");
  const [showAlphaFactors, setShowAlphaFactors] = useState(false);
  const [factorSortKey, setFactorSortKey] = useState<string>("grade");
  const [factorSortDir, setFactorSortDir] = useState<"asc"|"desc"|null>("asc");

  /* ── 配置参数 ── */
  const [topk, setTopk] = useState(50);
  const [nDrop, setNDrop] = useState(5);
  const [holdThresh, setHoldThresh] = useState(1);
  const [disableAlphaBaseline, setDisableAlphaBaseline] = useState(false);
  const [quickTrain, setQuickTrain] = useState(false);
  const [randomSeed, setRandomSeed] = useState(DEFAULT_QE_RANDOM_SEED);
  const [labelType, setLabelType] = useState<"close" | "open" | "vwap">("close");
  const [labelHorizon, setLabelHorizon] = useState<1 | 3 | 5 | 10 | 20>(1);
  const [blacklistEnabled, setBlacklistEnabled] = useState(false);
  const [stockPoolPath, setStockPoolPath] = useState<string | null>(null);
  const [blacklistSnapshot, setBlacklistSnapshot] = useState<any | null>(null);
  const [dataSplit, setDataSplit] = useState(() => ({ ...DEFAULT_QE_DATA_SPLIT }));
  const [dispatchMode, setDispatchMode] = useState<"independent" | "evolution">("independent");
  const [evolutionLoops, setEvolutionLoops] = useState(5);
  const [evolutionObjective, setEvolutionObjective] = useState("");

  /* ── Multi-Alpha 模式 ── */
  const [alphaMode, setAlphaMode] = useState<"single" | "multi">("single");
  const [multiAlphaConfig, setMultiAlphaConfig] = useState<MultiAlphaConfig | null>(null);

  const handleAlphaModeChange = useCallback((mode: "single" | "multi") => {
    setAlphaMode(mode);
    if (mode === "multi") {
      setSelectedFactors(new Set());
      setSelectedModel("");
    } else {
      setMultiAlphaConfig(null);
    }
    setCorrAnalyzed(false);
    setCorrPairs([]);
  }, []);

  /* ── HMM 板块轮动 ── */
  const [enableSectorHmm, setEnableSectorHmm] = useState(false);
  const [hmmConfigs, setHmmConfigs] = useState<any[]>([]);
  const [hmmSelectedConfigId, setHmmSelectedConfigId] = useState("");
  const [hmmSnapshots, setHmmSnapshots] = useState<any[]>([]);
  const [hmmModelVersionId, setHmmModelVersionId] = useState("");
  const [hmmSignalPreset, setHmmSignalPreset] = useState<string>("preset_A");

  /* ── 回测频率 ── */
  const [backtestFreq, setBacktestFreq] = useState<"1min" | "day">("1min");

  /* ── 执行算法 ── */
  const [executionAlgoCatalog, setExecutionAlgoCatalog] = useState<any[]>([]);
  const qeExecutionAlgoCatalog = useMemo(() => executionAlgoCatalog.filter((a: any) => a.qe_supported !== false), [executionAlgoCatalog]);
  const [executionAlgo, setExecutionAlgo] = useState<string>("");
  const [executionAlgoParams, setExecutionAlgoParams] = useState<Record<string, any>>({});
  const selectedExecutionAlgoInfo = useMemo(
    () => executionAlgo ? executionAlgoCatalog.find((a: any) => a.algo_code === executionAlgo) : null,
    [executionAlgo, executionAlgoCatalog],
  );
  const [computeNodes, setComputeNodes] = useState<any[]>([]);
  const [executionNodeId, setExecutionNodeId] = useState<string>("");
  const [filterSuspendedOnSignal, setFilterSuspendedOnSignal] = useState(false);
  const [suspendFilterStrict, setSuspendFilterStrict] = useState(true);

  /* ── 尾盘涨停未成交资金处理 ── */
  const [unfilledHandler, setUnfilledHandler] = useState<string>(""); // "" | "TAIL_BOOST" | "TAIL_SUBSTITUTE"
  const [unfilledBackupDepth, setUnfilledBackupDepth] = useState<number>(15);

  /* ── 从已有实验继承 ── */
  type PriorExperiment = { experiment_id: string; alpha_mode?: string; factor_names?: string[]; model_id?: string; strategy_id?: string; data_split?: any; custom_params?: any; multi_alpha_config?: any; created_at?: string; result_metrics?: any; ic?: number | null };
  const [priorExperiments, setPriorExperiments] = useState<PriorExperiment[]>([]);
  const [priorExpId, setPriorExpId] = useState<string>("");
  const [inheritLoading, setInheritLoading] = useState(false);
  const [inheritExpanded, setInheritExpanded] = useState(false);

  /* ── RDAgent Task 导入 ── */
  type SourceTask = { task_id: string; sota_factor_count: number; sota_model_count: number; best_ic: number | null; best_sharpe: number | null; best_annualized_return: number | null; worst_max_drawdown: number | null; total_loops: number; has_sota: boolean };
  const [sourceTasks, setSourceTasks] = useState<SourceTask[]>([]);
  const [sourceTaskId, setSourceTaskId] = useState<string>("");
  const [importLoading, setImportLoading] = useState(false);
  const [importExpanded, setImportExpanded] = useState(false);

  /* ── 因子相关性 & 独立IC ── */
  const [corrPairs, setCorrPairs] = useState<Array<{factor_a: string; factor_b: string; correlation: number}>>([]);
  const [corrLoading, setCorrLoading] = useState(false);
  const [corrAnalyzed, setCorrAnalyzed] = useState(false);
  const [classificationMap, setClassificationMap] = useState<Record<string, {official_grade?: string | null; official_score?: number | null; category?: string}>>({});

  /* ── 结果状态 ── */
  const [evalResult, setEvalResult] = useState<EvalResult | null>(null);
  const [configResult, setConfigResult] = useState<ConfigResult | null>(null);
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [logsVisible, setLogsVisible] = useState(false); // 日志面板默认关闭
  const [editLoadError, setEditLoadError] = useState<string | null>(null);
  const [editConfigLoaded, setEditConfigLoaded] = useState(false);

  /* ── 单次实验执行（共享 Hook） ── */
  const sse = useExperimentSSE({ pollAfterDisconnect: true });

  /* ── 加载数据 ── */
  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [fRes, mRes, sRes, cRes] = await Promise.all([
        fetch(`${API}/quantevolver/factors?limit=1000&exclude_source=qlib_alpha158,alpha158,alpha360&sort_field=ind_ic&sort_order=desc`).then(r => r.json()),
        fetch(`${API}/quantevolver/models?limit=100`).then(r => r.json()),
        fetch(`${API}/quantevolver/strategies?limit=50`).then(r => r.json()),
        fetch(`${API}/quantevolver/factor-analyst/classifications?limit=1000&active_only=false`).then(r => r.json()).catch(() => ({ items: [] })),
      ]);
      const classMap: Record<string, any> = {};
      (cRes.items || []).forEach((c: any) => { classMap[c.factor_name] = c; });
      setClassificationMap(classMap);
      
      const enrichedFactors = (fRes.items || []).map((f: any) => {
        const name = f.factor_name ?? f.name;
        const cls = classMap[name];
        return { 
          factor_name: name,
          source: f.source,
          ic: f.ind_ic ?? null,
          sharpe: f.ind_sharpe ?? null,
          is_sota_factor: f.is_sota_factor,
          factor_type: f.factor_type,
          data_source: f.data_source,
          category: f.category ?? cls?.category,
          official_grade: f.official_grade ?? cls?.official_grade ?? null,
          official_score: f.official_score ?? cls?.official_score ?? null,
          description: f.description_cn || f.cl_description,
          ind_annual_return: f.ind_annual_return ?? null,
        };
      });
      setFactors(enrichedFactors);

      const dbModels = mRes.items || [];
      const hasLGB = dbModels.some((m: Model) => m.model_name === "LGBModel" || m.model_type === "LGB");
      const allModels = hasLGB ? dbModels : [
        { model_id: "__builtin_lgbmodel__", model_name: "LGBModel", model_type: "LGB", display_name: "LGBModel (QLib内置)", is_sota: false },
        ...dbModels,
      ];
      setModels(allModels);
      setStrategies(sRes.items || []);
    } catch (e) { console.error("加载数据失败", e); }
    setLoading(false);
  }, []);

  useEffect(() => { loadData(); }, [loadData]);

  const applySingleExperimentConfig = useCallback((exp: any) => {
    setAlphaMode("single");
    setMultiAlphaConfig(null);
    const factorNames: string[] = Array.isArray(exp.factor_names) ? exp.factor_names : [];
    const factorSources = exp.factor_sources || exp.custom_params?.qe_factor_sources || {};
    const keys = new Set<string>();
    for (const name of factorNames) {
      const source = factorSources[name] || factors.find((f: Factor) => f.factor_name === name)?.source;
      keys.add(source ? `${name}||${source}` : name);
    }
    setSelectedFactors(keys);
    setSelectedModel(exp.model_id || "");
    setSelectedStrategy(exp.strategy_id || "");
    if (exp.data_split) setDataSplit(withSafeBacktestEnd(exp.data_split));
    const cp = exp.custom_params || {};
    if (cp.topk) setTopk(cp.topk);
    if (cp.n_drop) setNDrop(cp.n_drop);
    if (cp.hold_thresh) setHoldThresh(cp.hold_thresh);
    if (cp.random_seed !== undefined) {
      setRandomSeed(parseQeRandomSeedInput(String(cp.random_seed), DEFAULT_QE_RANDOM_SEED));
    }
    setDisableAlphaBaseline(!!cp.disable_alpha158);
    setQuickTrain(!!cp.quick_train);
    setLabelType((["close", "open", "vwap"].includes(cp.label_type) ? cp.label_type : "close") as "close" | "open" | "vwap");
    setLabelHorizon(([1, 3, 5, 10, 20].includes(Number(cp.label_horizon || 1)) ? Number(cp.label_horizon || 1) : 1) as 1 | 3 | 5 | 10 | 20);
    setExecutionAlgo(cp.execution_algo || "");
    setExecutionAlgoParams(cp.execution_algo_params || {});
    setBacktestFreq(cp.execution_algo === "CLOSE_PRICE" || cp.backtest_freq === "day" ? "day" : "1min");
    setFilterSuspendedOnSignal(!!(cp.filter_suspended_on_signal || cp.exclude_suspended));
    setSuspendFilterStrict(cp.suspend_filter_strict !== false);
    setBlacklistEnabled(!!cp.stock_pool);
    setStockPoolPath(cp.stock_pool || null);
    setBlacklistSnapshot(cp.sector_blacklist_snapshot || null);
    setEnableSectorHmm(!!cp.enable_sector_hmm);
    setHmmModelVersionId(cp.hmm_model_version_id || "");
    setHmmSignalPreset(cp.hmm_signal_preset || "preset_A");
    setUnfilledHandler(cp.unfilled_handler || "");
    setUnfilledBackupDepth(Number(cp.unfilled_backup_depth || 15));
    setConfigResult({
      ok: true,
      experiment_id: exp.experiment_id,
      operation: "edit_pending_config",
      editable: exp.editable,
      startable: exp.startable,
    });
    setCurrentStep(5);
  }, [factors]);

  useEffect(() => {
    if (!isEditPendingExperiment || editConfigLoaded || loading) return;
    let cancelled = false;
    async function loadEditableExperiment() {
      try {
        const res = await fetch(`${API}/quantevolver/experiments/${editExperimentId}/editable-config`);
        const data = await res.json();
        if (!res.ok || data.ok === false) {
          throw new Error(data.detail || data.error || `HTTP ${res.status}`);
        }
        const editable = data.data || data.experiment || data;
        if (!editable.editable) {
          throw new Error(editable.start_reason || "该实验已经启动，不能全量编辑");
        }
        if (!cancelled) {
          applySingleExperimentConfig(editable);
          setEditConfigLoaded(true);
          setEditLoadError(null);
        }
      } catch (err: any) {
        if (!cancelled) {
          setEditLoadError(err?.message || "加载待启动实验配置失败");
          setEditConfigLoaded(true);
        }
      }
    }
    void loadEditableExperiment();
    return () => { cancelled = true; };
  }, [applySingleExperimentConfig, editConfigLoaded, editExperimentId, isEditPendingExperiment, loading]);

  /* ── 加载 RDAgent Task 列表 ── */
  useEffect(() => {
    fetch(`${API}/quantevolver/evolution/source-tasks`)
      .then(r => r.json())
      .then(d => {
        if (d.status === "success") setSourceTasks(d.data || []);
        else console.error("加载 source tasks 失败:", d);
      })
      .catch((e) => {
        console.error("加载 source tasks 失败", e);
      });
  }, []);

  /* ── 加载执行算法目录 ── */
  useEffect(() => {
    fetch(`${API}/quantevolver/execution-algorithms`)
      .then(r => r.json())
      .then(d => { if (d.ok) setExecutionAlgoCatalog(d.items || []); })
      .catch((e) => { console.error("加载执行算法目录失败", e); });
    fetch(`${API}/dispatch/nodes`)
      .then(r => r.json())
      .then(d => { if (Array.isArray(d)) setComputeNodes(d); else if (d?.data) setComputeNodes(d.data); })
      .catch((e) => { console.error("load QE execution nodes failed", e); });
  }, []);

  /* ── HMM 配置/快照加载 ── */
  const fetchHmmConfigs = useCallback(async () => {
    try {
      const res = await fetch(`${API}/hmm-training/configs?model_type=sector_hmm`);
      if (res.ok) {
        const d = await res.json();
        setHmmConfigs(Array.isArray(d) ? d : []);
      }
    } catch (e) {
      console.error("加载 HMM 配置列表失败", e);
      setHmmConfigs([]);
    }
  }, []);

  useEffect(() => {
    if (enableSectorHmm && hmmConfigs.length === 0) fetchHmmConfigs();
  }, [enableSectorHmm, fetchHmmConfigs, hmmConfigs.length]);

  const fetchHmmSnapshots = useCallback(async (configId: string) => {
    if (!configId) { setHmmSnapshots([]); return; }
    try {
      const res = await fetch(`${API}/hmm-training/configs/${configId}/snapshots`);
      if (res.ok) {
        const data = await res.json();
        setHmmSnapshots((Array.isArray(data) ? data : []).filter((s: any) => s.status === "completed"));
      } else {
        console.error("加载 HMM 快照列表失败: HTTP", res.status, res.statusText);
        setHmmSnapshots([]);
      }
    } catch (e) {
      console.error("加载 HMM 快照列表失败", e);
      setHmmSnapshots([]);
    }
  }, []);

  /* ── 从已有实验继承配置 ── */
  async function inheritFromExperiment() {
    if (!priorExpId) return;
    setInheritLoading(true);
    try {
      const res = await fetch(`${API}/quantevolver/experiments/${priorExpId}?detail=full`);
      const d = await res.json();
      if (!d.ok) { alert("获取实验详情失败"); setInheritLoading(false); return; }
      const exp = d.experiment || d;
      if (exp.alpha_mode === "multi") {
        if (!exp.multi_alpha_config) {
          alert("该多Alpha实验缺少分组配置数据 (multi_alpha_config 为空)，无法继承");
          setInheritLoading(false);
          return;
        }
        setAlphaMode("multi");
        setMultiAlphaConfig(exp.multi_alpha_config);
        setSelectedFactors(new Set());
        setSelectedModel("");
      } else {
        setAlphaMode("single");
        setMultiAlphaConfig(null);
        if (exp.factor_names?.length) {
          const keys = new Set<string>();
          for (const name of exp.factor_names) {
            const match = factors.find((f: Factor) => f.factor_name === name);
            keys.add(match ? `${match.factor_name}||${match.source}` : name);
          }
          setSelectedFactors(keys);
        }
        if (exp.model_id) setSelectedModel(exp.model_id);
      }
      if (exp.strategy_id) setSelectedStrategy(exp.strategy_id);
      if (exp.data_split) setDataSplit(withSafeBacktestEnd(exp.data_split));
      if (exp.custom_params) {
        if (exp.custom_params.topk) setTopk(exp.custom_params.topk);
        if (exp.custom_params.n_drop) setNDrop(exp.custom_params.n_drop);
        if (exp.custom_params.hold_thresh) setHoldThresh(exp.custom_params.hold_thresh);
        if (exp.custom_params.random_seed !== undefined) {
          setRandomSeed(parseQeRandomSeedInput(String(exp.custom_params.random_seed), DEFAULT_QE_RANDOM_SEED));
        }
        if ([1, 3, 5, 10, 20].includes(Number(exp.custom_params.label_horizon || 1))) {
          setLabelHorizon(Number(exp.custom_params.label_horizon || 1) as 1 | 3 | 5 | 10 | 20);
        }
        if (exp.custom_params.execution_algo) {
          setExecutionAlgo(exp.custom_params.execution_algo);
          setBacktestFreq(exp.custom_params.execution_algo === "CLOSE_PRICE" ? "day" : "1min");
        } else if (exp.custom_params.backtest_freq === "day") {
          setExecutionAlgo("CLOSE_PRICE");
          setExecutionAlgoParams({});
          setBacktestFreq("day");
        }
        if (exp.custom_params.execution_algo_params) setExecutionAlgoParams(exp.custom_params.execution_algo_params);
        setFilterSuspendedOnSignal(!!(exp.custom_params.filter_suspended_on_signal || exp.custom_params.exclude_suspended));
        setSuspendFilterStrict(exp.custom_params.suspend_filter_strict !== false);
      }
      setCurrentStep(1);
      setCorrAnalyzed(false);
      setCorrPairs([]);
    } catch (e: any) { alert("继承失败: " + (e?.message || "")); }
    setInheritLoading(false);
  }

  /* ── 从 RDAgent Task 导入配置 ── */
  async function importFromTask() {
    if (!sourceTaskId) return;
    setImportLoading(true);
    try {
      const res = await fetch(`${API}/quantevolver/evolution/source-tasks/${sourceTaskId}/preview`);
      const d = await res.json();
      if (d.status !== "success" || !d.data) { alert("获取 Task 详情失败"); setImportLoading(false); return; }
      const preview = d.data;
      // 预填充因子
      if (preview.sota_factors?.length) {
        const keys = new Set<string>();
        for (const f of preview.sota_factors) {
          const match = factors.find((ff: Factor) => ff.factor_name === f.factor_name);
          keys.add(match ? `${match.factor_name}||${match.source}` : f.factor_name);
        }
        setSelectedFactors(keys);
      }
      // 预填充模型
      if (preview.sota_models?.length) {
        setSelectedModel(preview.sota_models[0].model_id);
      }
      setCurrentStep(1);
    } catch (e: any) { alert("导入失败: " + (e?.message || "")); }
    setImportLoading(false);
  }

  /* ── AI 智能生成 ── */
  async function aiGenerate() {
    if (!userRequirement.trim()) { alert("请输入实验组合设计目标"); return; }
    setAiLoading(true); setAiResult(null); setConfigResult(null); setEvalResult(null);
    // 重置执行状态
    sse.reset();
    try {
      const res = await fetch(`${API}/quantevolver/experiment/smart-select`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_requirement: userRequirement.trim(), use_llm: true, max_factors: 30 }),
      });
      const data = await res.json();
      setAiResult(data);
      if (data.ok && data.combination) {
        const keys = data.combination.factor_names.map((name: string) => {
          const f = factors.find((f: any) => f.factor_name === name);
          return f ? `${f.factor_name}||${f.source}` : name;
        });
        setSelectedFactors(new Set(keys));
        setSelectedModel(data.combination.model_id || "");
        setSelectedStrategy(data.combination.strategy_id || "TopkDropoutStrategy");
        if (data.combination.strategy_params) {
          setTopk(data.combination.strategy_params.topk || 50);
          setNDrop(data.combination.strategy_params.n_drop || 5);
        }
        setCurrentStep(4);
      } else {
        alert("智能生成失败: " + (data.error || "未知错误"));
      }
    } catch (e: any) {
      alert("智能生成失败: " + (e?.message || ""));
    }
    setAiLoading(false);
  }

  /* ── 因子筛选排序逻辑 ── */
  function handleFactorSort(field: string) {
    if (factorSortKey !== field) { setFactorSortKey(field); setFactorSortDir("desc"); }
    else if (factorSortDir === "desc") { setFactorSortDir("asc"); }
    else if (factorSortDir === "asc") { setFactorSortKey(""); setFactorSortDir(null); }
    else { setFactorSortDir("desc"); }
    setFactorPage(1);
  }

  const allFilteredFactors = useMemo(() => {
    return factors.filter(f => {
      if (factorSourceTab === "sota") return f.source === "rdagent_task_sync";
      if (factorSourceTab === "alpha158") return f.source === "alpha158";
      if (factorSourceTab === "alpha360") return f.source === "alpha360";
      if (!showAlphaFactors && (f.source === "alpha158" || f.source === "alpha360")) return false;
      if (factorTypeFilter && f.factor_type !== factorTypeFilter) return false;
      if (dataSourceFilter && f.data_source !== dataSourceFilter) return false;
      return true;
    }).sort((a, b) => {
      if (!factorSortKey || !factorSortDir) return 0;
      const dir = factorSortDir === "asc" ? 1 : -1;
      if (factorSortKey === "grade") return dir * ((GRADE_ORDER[a.official_grade || "D"] ?? 5) - (GRADE_ORDER[b.official_grade || "D"] ?? 5));
      if (factorSortKey === "ic") return dir * ((b.ic ?? -999) - (a.ic ?? -999));
      if (factorSortKey === "sharpe") return dir * ((b.sharpe ?? -999) - (a.sharpe ?? -999));
      if (factorSortKey === "name") return dir * a.factor_name.localeCompare(b.factor_name);
      return 0;
    });
  }, [factors, factorSourceTab, showAlphaFactors, factorTypeFilter, dataSourceFilter, factorSortKey, factorSortDir]);

  const totalFactorPages = Math.max(1, Math.ceil(allFilteredFactors.length / factorPageSize));
  const pagedFactors = allFilteredFactors.slice((factorPage - 1) * factorPageSize, factorPage * factorPageSize);

  /* ── 因子相关性检查 ── */
  async function checkCorrelations() {
    if (selectedFactors.size < 2) return;
    setCorrLoading(true);
    try {
      const names = Array.from(selectedFactors).map(k => k.split("||")[0]);
      const res = await fetch(`${API}/quantevolver/evolution/correlations/subset`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ factor_names: names, threshold: 0.5 }),
      });
      if (!res.ok) { setCorrLoading(false); return; }
      const data = await res.json();
      setCorrPairs(data.pairs || []);
      setCorrAnalyzed(true);
    } catch (e) { console.error("Correlation check failed:", e); }
    setCorrLoading(false);
  }

  /* ── 已选因子的独立IC汇总 ── */
  const selectedFactorMetrics = useMemo(() => {
    const factorMap = new Map(factors.map(fac => [fac.factor_name, fac]));
    const names = Array.from(selectedFactors).map(k => k.split("||")[0]);
    return names.map(name => {
      const cls = classificationMap[name];
      const fac = factorMap.get(name);
      return {
        name,
        ic: fac?.ic ?? null,
        sharpe: fac?.sharpe ?? null,
        ann_ret: fac?.ind_annual_return ?? null,
        official_grade: cls?.official_grade ?? fac?.official_grade ?? null,
        category: cls?.category ?? fac?.category ?? null,
      };
    }).sort((a, b) => (Math.abs(b.ic ?? 0)) - (Math.abs(a.ic ?? 0)));
  }, [selectedFactors, classificationMap, factors]);

  /* ── 操作动作 ── */
  function validateRuntimeSelections() {
    if (executionAlgo && executionAlgoCatalog.length > 0 && !selectedExecutionAlgoInfo) {
      alert(`Execution algorithm ${executionAlgo} is not present in the backend catalog; refusing to submit inconsistent UI config.`);
      return false;
    }
    if (executionAlgo && selectedExecutionAlgoInfo?.qe_supported === false) {
      alert(`Execution algorithm ${executionAlgo} is not wired into QE/Qlib; refusing to silently fall back.`);
      return false;
    }
    if (blacklistEnabled && !stockPoolPath) {
      alert("行业黑名单已启用，但股票池尚未生成成功。请等待生成完成或手动刷新成功后再继续。");
      return false;
    }
    if (blacklistEnabled && !blacklistSnapshot) {
      alert("行业黑名单已启用，但黑名单快照尚未生成。请手动刷新股票池成功后再继续。");
      return false;
    }
    if (enableSectorHmm && !hmmModelVersionId) {
      alert("行业 HMM 已启用，但尚未选择已完成的 HMM 快照。请选择快照后再继续。");
      return false;
    }
    if (!isValidQeRandomSeed(randomSeed)) {
      alert(`固定随机种子必须是 0 到 ${QE_RANDOM_SEED_MAX} 之间的整数。`);
      return false;
    }
    return true;
  }

  function buildRuntimeCustomParams() {
    return {
      topk,
      n_drop: nDrop,
      hold_thresh: holdThresh,
      disable_alpha158: disableAlphaBaseline,
      quick_train: quickTrain,
      random_seed: randomSeed,
      label_type: labelType,
      ...(labelHorizon !== 1 ? { label_horizon: labelHorizon } : {}),
      backtest_freq: backtestFreq,
      ...(executionAlgo ? { execution_algo: executionAlgo, execution_algo_params: executionAlgoParams } : {}),
      ...(filterSuspendedOnSignal ? { filter_suspended_on_signal: true, suspend_filter_strict: suspendFilterStrict } : {}),
      ...(blacklistEnabled && stockPoolPath ? {
        stock_pool: stockPoolPath,
        sector_blacklist_enabled: true,
        sector_blacklist_snapshot: blacklistSnapshot,
      } : {}),
      ...(enableSectorHmm && hmmModelVersionId ? { enable_sector_hmm: true, hmm_model_version_id: hmmModelVersionId, hmm_signal_preset: hmmSignalPreset } : {}),
    };
  }

  function buildUnfilledHandlerPayload() {
    if (executionAlgo === "CLOSE_PRICE" || !unfilledHandler) return {};
    return {
      unfilled_handler: unfilledHandler,
      unfilled_handler_params: unfilledHandler === "TAIL_SUBSTITUTE" ? { backup_depth: unfilledBackupDepth } : {},
    };
  }

  async function evaluateCombination() {
    if (alphaMode === "single" && selectedFactors.size === 0) { alert("请先选择因子"); return; }
    if (alphaMode === "multi" && !multiAlphaConfig) { alert("请先生成多Alpha分组配置"); return; }
    if (!validateRuntimeSelections()) return;
    setActionLoading("evaluate");
    try {
      // Multi-Alpha: 聚合各组因子
      const factorNames = alphaMode === "multi" && multiAlphaConfig
        ? [...new Set(multiAlphaConfig.alpha_groups.flatMap(g => g.factor_names))]
        : Array.from(selectedFactors).map(k => k.split("||")[0]);

      const res = await fetch(`${API}/quantevolver/experiment/evaluate-portfolio`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          factor_names: factorNames,
          model_id: alphaMode === "single" ? (selectedModel || undefined) : multiAlphaConfig?.alpha_groups[0]?.model_id,
          strategy_id: selectedStrategy || undefined,
          custom_params: buildRuntimeCustomParams(),
          ...(alphaMode === "multi" && multiAlphaConfig ? { alpha_mode: "multi", multi_alpha_config: multiAlphaConfig } : {}),
        }),
      });
      const data = await res.json();
      if (!res.ok) { alert("评估失败: " + (data?.detail || res.statusText)); setActionLoading(null); return; }
      setEvalResult(data);
    } catch (e: any) { alert("评估失败: " + (e?.message || "")); }
    setActionLoading(null);
  }

  async function generateConfig() {
    if (alphaMode === "single" && selectedFactors.size === 0) { alert("请先选择因子"); return; }
    if (alphaMode === "multi" && !multiAlphaConfig) { alert("请先生成多Alpha分组配置"); return; }
    if (!validateRuntimeSelections()) return;
    if (alphaMode === "multi" && multiAlphaConfig?.execution_mode === "distributed" && !MULTI_ALPHA_DISTRIBUTED_ENABLED) {
      alert("分布式 Multi-Alpha 尚未启用；当前阶段必须使用 WSL 单节点串行模式完成验证。");
      return;
    }
    setActionLoading("generate");
    // 重置上一次的执行状态
    sse.reset();
    try {
      // Multi-Alpha 模式: 因子由各组管理，聚合所有组的因子作为 factor_names
      const factorNames = alphaMode === "multi" && multiAlphaConfig
        ? [...new Set(multiAlphaConfig.alpha_groups.flatMap(g => g.factor_names))]
        : Array.from(selectedFactors).map(k => k.split("||")[0]);
      const factorSources = alphaMode === "single"
        ? Object.fromEntries(
            Array.from(selectedFactors)
              .map(k => k.split("||"))
              .filter(parts => parts[0] && parts[1])
              .map(([name, source]) => [name, source]),
          )
        : undefined;

      const usePendingExperimentEndpoint = alphaMode === "single" && dispatchMode === "independent";
      const endpoint = isEditPendingExperiment
        ? `${API}/quantevolver/experiments/${editExperimentId}/editable-config`
        : usePendingExperimentEndpoint
          ? `${API}/quantevolver/experiments/pending`
          : `${API}/quantevolver/config/generate`;
      const res = await fetch(endpoint, {
        method: isEditPendingExperiment ? "PUT" : "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          factor_names: factorNames,
          factor_sources: factorSources,
          model_id: alphaMode === "single" ? (selectedModel || undefined) : multiAlphaConfig?.alpha_groups[0]?.model_id,
          strategy_id: selectedStrategy || undefined,
          data_split: withSafeBacktestEnd(dataSplit), custom_params: buildRuntimeCustomParams(),
          ...buildUnfilledHandlerPayload(),
          dispatch_mode: dispatchMode,
          evolution_params: dispatchMode === "evolution" ? { loops: evolutionLoops, objective: evolutionObjective } : undefined,
          // Multi-Alpha 字段
          ...(alphaMode === "multi" && multiAlphaConfig ? {
            alpha_mode: "multi",
            multi_alpha_config: multiAlphaConfig,
          } : {}),
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        alert("生成失败: " + (data?.detail || res.statusText || "未知错误"));
        setActionLoading(null);
        return;
      }
      setConfigResult(data);
    } catch (e: any) { alert("生成失败: " + (e?.message || "")); }
    setActionLoading(null);
  }

  const getSortIndicator = (field: string) => factorSortKey === field ? (factorSortDir === "desc" ? " ▼" : factorSortDir === "asc" ? " ▲" : "") : "";
  const toggleFactor = (name: string) => setSelectedFactors(prev => {
    const n = new Set(prev); n.has(name) ? n.delete(name) : n.add(name);
    setCorrAnalyzed(false); setCorrPairs([]);
    return n;
  });

  const removeSelectedFactorByName = useCallback((factorName: string) => {
    const normalizedName = String(factorName || "").trim();
    if (!normalizedName) return;

    setSelectedFactors(prev => {
      const next = new Set(Array.from(prev).filter(k => k.split("||")[0] !== normalizedName));
      return next.size === prev.size ? prev : next;
    });

    if (selectedFactors.size <= 2) {
      setCorrAnalyzed(false);
      setCorrPairs([]);
    } else {
      setCorrPairs(prev => prev.filter(p => p.factor_a !== normalizedName && p.factor_b !== normalizedName));
    }
  }, [selectedFactors.size]);

  const renderCorrelationFactorCell = (factorName: string, tone: "danger" | "warning") => (
    <td style={{ padding: "3px 6px", fontFamily: "monospace", maxWidth: 170 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 6, minWidth: 0 }}>
        <span
          title={factorName}
          style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1, minWidth: 0 }}
        >
          {factorName}
        </span>
        <button
          type="button"
          onClick={() => removeSelectedFactorByName(factorName)}
          title={`从QE实验组合中移除 ${factorName}`}
          style={{
            border: tone === "danger" ? "1px solid #fca5a5" : "1px solid #fcd34d",
            backgroundColor: tone === "danger" ? "#fee2e2" : "#fef3c7",
            color: tone === "danger" ? "#b91c1c" : "#92400e",
            borderRadius: 4,
            padding: "1px 5px",
            fontSize: 10,
            fontWeight: 700,
            cursor: "pointer",
            flex: "0 0 auto",
          }}
        >
          移除
        </button>
      </div>
    </td>
  );

  return (
    <div style={{
      padding: "24px",
      boxSizing: "border-box",
      fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif",
      minHeight: "calc(100vh - 48px)",
      maxWidth: "1400px",
      margin: "0 auto",
    }}>
      {/* 顶部标题区 */}
      <div style={{ marginBottom: "24px" }}>
        <h1 style={{ margin: 0, fontSize: "24px", fontWeight: 700, color: "#ffffff", textShadow: "0 1px 3px rgba(0,0,0,0.15)" }}>{isEditPendingExperiment ? "编辑待启动 QE 实验" : "QE实验设计"}</h1>
        <p style={{ margin: "8px 0 0", fontSize: "14px", color: "rgba(255,255,255,0.75)" }}>{isEditPendingExperiment ? `正在编辑 ${editExperimentId}，保存后仍保持待启动，不会自动执行。` : "双轨驱动：支持AI智能生成配置与人工分步流程式选择，为您构建优质的因子组合与模型演进任务。"}</p>
        {editLoadError && (
          <div style={{ marginTop: 12, padding: "10px 12px", borderRadius: 8, background: "#fee2e2", color: "#991b1b", fontSize: 13, fontWeight: 600 }}>
            加载待启动实验失败：{editLoadError}
          </div>
        )}
      </div>
      
      {/* AI 智能实验设计区 */}
      <div style={{ ...cardStyle, marginBottom: "24px" }}>
        <div style={headerStyle}>
          <h2 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#7c3aed", display: "flex", alignItems: "center", gap: "8px" }}>
            ✨ AI 智能实验设计
          </h2>
        </div>
        <div style={{ padding: "20px" }}>
          <textarea
            value={userRequirement}
            onChange={e => setUserRequirement(e.target.value)}
            placeholder="请输入您的实验组合设计目标，例如：构建一个偏向于动量反转的日频量化组合，配合高频微观结构因子..."
            rows={3}
            style={{ ...inputStyle, resize: "none", padding: "12px 16px", fontSize: "14px", lineHeight: 1.6, borderRadius: "8px" }}
          />
          <div style={{ display: "flex", gap: "12px", marginTop: "16px", alignItems: "center", flexWrap: "wrap" }}>
            <button
              onClick={aiGenerate}
              disabled={aiLoading}
              style={{ ...btnPrimary, backgroundColor: "#7c3aed", padding: "10px 24px", fontSize: "14px", boxShadow: "0 2px 4px rgba(124, 58, 237, 0.25)", opacity: aiLoading ? 0.5 : 1 }}
            >
              {aiLoading ? "正在深度解析并生成..." : "智能生成配置"}
            </button>
            <div style={{ display: "flex", gap: "8px" }}>
              {["稳健低回撤", "资金流选股", "高夏普动量"].map(tag => (
                <button key={tag} onClick={() => setUserRequirement(tag + "组合，要求全面考虑风险控制")}
                  style={{ padding: "4px 12px", fontSize: "12px", color: "#64748b", border: "1px solid #e2e8f0", borderRadius: "16px", backgroundColor: "#fff", cursor: "pointer", transition: "all 0.2s" }}>
                  {tag}
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* 从已有实验继承配置 */}
      <div style={{ ...cardStyle, marginBottom: "24px" }}>
        <div
          onClick={() => {
            setInheritExpanded(!inheritExpanded);
            if (!inheritExpanded && priorExperiments.length === 0) {
              fetch(`${API}/quantevolver/experiments?limit=20&detail=summary`)
                .then(r => r.json())
                .then(d => { if (d.ok || d.items) setPriorExperiments(d.items || []); })
                .catch(() => {});
            }
          }}
          style={{ ...headerStyle, cursor: "pointer", display: "flex", justifyContent: "space-between", alignItems: "center" }}
        >
          <h2 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#059669", display: "flex", alignItems: "center", gap: "8px" }}>
            从已有实验继承配置
          </h2>
          <span style={{ color: "#94a3b8", fontSize: "12px" }}>{inheritExpanded ? "收起" : "展开"}</span>
        </div>
        {inheritExpanded && (
          <div style={{ padding: "20px" }}>
            <p style={{ margin: "0 0 12px", fontSize: "13px", color: "#64748b" }}>
              选择一个已有实验（支持单Alpha和多Alpha），一键继承其因子、模型、策略等完整配置。
            </p>
            <div style={{ display: "flex", gap: "12px", alignItems: "flex-end" }}>
              <div style={{ flex: 1 }}>
                <select
                  value={priorExpId}
                  onChange={e => setPriorExpId(e.target.value)}
                  style={{ width: "100%", padding: "10px 12px", fontSize: "13px", border: "1px solid #e2e8f0", borderRadius: "8px", backgroundColor: "#f8fafc", color: "#1e293b" }}
                >
                  <option value="">-- 选择已有实验 --</option>
                  {priorExperiments.map(exp => (
                    <option key={exp.experiment_id} value={exp.experiment_id}>
                      {exp.experiment_id} | {exp.alpha_mode === "multi" ? "多Alpha" : "单Alpha"} | IC:{exp.ic != null ? Number(exp.ic).toFixed(4) : "-"} | {exp.created_at ? new Date(exp.created_at).toLocaleDateString("zh-CN") : ""}
                    </option>
                  ))}
                </select>
              </div>
              <button
                onClick={inheritFromExperiment}
                disabled={!priorExpId || inheritLoading}
                style={{
                  ...btnPrimary,
                  backgroundColor: priorExpId ? "#059669" : "#94a3b8",
                  padding: "10px 24px",
                  fontSize: "14px",
                  opacity: !priorExpId || inheritLoading ? 0.5 : 1,
                  whiteSpace: "nowrap",
                }}
              >
                {inheritLoading ? "加载中..." : "继承配置"}
              </button>
            </div>
          </div>
        )}
      </div>

      {/* RDAgent Task 快速导入 */}
      {sourceTasks.length > 0 && (
        <div style={{ ...cardStyle, marginBottom: "24px" }}>
          <div
            onClick={() => setImportExpanded(!importExpanded)}
            style={{ ...headerStyle, cursor: "pointer", display: "flex", justifyContent: "space-between", alignItems: "center" }}
          >
            <h2 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#0891b2", display: "flex", alignItems: "center", gap: "8px" }}>
              RDAgent Task 快速导入
            </h2>
            <span style={{ color: "#94a3b8", fontSize: "12px" }}>{importExpanded ? "收起" : "展开"}</span>
          </div>
          {importExpanded && (
            <div style={{ padding: "20px" }}>
              <p style={{ margin: "0 0 12px", fontSize: "13px", color: "#64748b" }}>
                选择一个 RDAgent Task，一键导入其 SOTA 因子和模型作为起点，然后在后续步骤中自由调整。
              </p>
              <div style={{ display: "flex", gap: "12px", alignItems: "flex-end" }}>
                <div style={{ flex: 1 }}>
                  <select
                    value={sourceTaskId}
                    onChange={e => setSourceTaskId(e.target.value)}
                    style={{ width: "100%", padding: "10px 12px", fontSize: "13px", border: "1px solid #e2e8f0", borderRadius: "8px", backgroundColor: "#f8fafc", color: "#1e293b" }}
                  >
                    <option value="">-- 选择 RDAgent Task --</option>
                    {sourceTasks.map(t => (
                      <option key={t.task_id} value={t.task_id}>
                        {t.task_id} | 因子:{t.sota_factor_count} 模型:{t.sota_model_count} | IC:{t.best_ic != null ? t.best_ic.toFixed(4) : "-"} | 年化:{t.best_annualized_return != null ? (t.best_annualized_return * 100).toFixed(1) + "%" : "-"} | 回撤:{t.worst_max_drawdown != null ? (t.worst_max_drawdown * 100).toFixed(1) + "%" : "-"}
                      </option>
                    ))}
                  </select>
                </div>
                <button
                  onClick={importFromTask}
                  disabled={!sourceTaskId || importLoading}
                  style={{
                    ...btnPrimary,
                    backgroundColor: sourceTaskId ? "#0891b2" : "#94a3b8",
                    padding: "10px 24px",
                    fontSize: "14px",
                    opacity: !sourceTaskId || importLoading ? 0.5 : 1,
                    whiteSpace: "nowrap",
                  }}
                >
                  {importLoading ? "导入中..." : "导入配置"}
                </button>
              </div>
            </div>
          )}
        </div>
      )}

      {/* 流程导航 - 横向按钮样式 */}
      <div style={{ ...cardStyle, marginBottom: "24px" }}>
        <div style={{ display: "flex", width: "100%" }}>
          {STEPS.map((step, idx) => {
            const stepNum = idx + 1;
            const isActive = currentStep === stepNum;
            const isPassed = currentStep > stepNum;

            return (
              <button
                key={stepNum}
                data-testid={`qe-step-${stepNum}`}
                onClick={() => setCurrentStep(stepNum)}
                style={{
                  flex: 1,
                  padding: "16px 8px",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  gap: "10px",
                  border: "none",
                  borderRight: idx < STEPS.length - 1 ? "1px solid #f1f5f9" : "none",
                  backgroundColor: isActive ? "#eff6ff" : isPassed ? "#f8fafc" : "#ffffff",
                  cursor: "pointer",
                  transition: "all 0.2s ease",
                  borderBottom: isActive ? "3px solid #2563eb" : "3px solid transparent",
                }}
              >
                <div style={{
                  width: "32px",
                  height: "32px",
                  borderRadius: "50%",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  fontWeight: 700,
                  fontSize: "13px",
                  flexShrink: 0,
                  backgroundColor: isActive ? "#2563eb" : isPassed ? "#dbeafe" : "#f1f5f9",
                  color: isActive ? "#ffffff" : isPassed ? "#2563eb" : "#94a3b8",
                  boxShadow: isActive ? "0 2px 4px rgba(37, 99, 235, 0.3)" : "none",
                  transition: "all 0.2s ease",
                }}>
                  {isPassed ? "✓" : stepNum}
                </div>
                <span style={{
                  fontWeight: 600,
                  fontSize: "14px",
                  whiteSpace: "nowrap",
                  color: isActive ? "#1e293b" : isPassed ? "#475569" : "#94a3b8",
                }}>
                  {step}
                </span>
              </button>
            );
          })}
        </div>
      </div>

      {/* 下部：分步卡片区 */}
      <div style={{ ...cardStyle, minHeight: "500px" }}>

        {/* Step 1: 因子选择 / Multi-Alpha 分组 */}
        {currentStep === 1 && (
          <div style={{ padding: "24px" }}>
            {/* Alpha 模式切换 — 始终显示 */}
            <MultiAlphaModeToggle alphaMode={alphaMode} onModeChange={handleAlphaModeChange} />

            {alphaMode === "single" ? (
              <>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px" }}>
                  <h2 style={{ margin: 0, fontSize: "18px", fontWeight: 700, color: "#1e293b" }}>1. 因子选择 (Factor Selection)</h2>
                  <span style={{ backgroundColor: "#dbeafe", color: "#2563eb", padding: "4px 12px", borderRadius: "12px", fontSize: "13px", fontWeight: 600 }}>已选 {selectedFactors.size} 项</span>
                </div>

                <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", overflow: "hidden" }}>
                  <FactorList
                    mode="selection"
                    selectedFactors={selectedFactors}
                    onFactorSelect={(selected) => { setSelectedFactors(selected); setCorrAnalyzed(false); setCorrPairs([]); }}
                    cacheContext={{
                      experimentId: configResult?.experiment_id || priorExpId || null,
                      trainStart: dataSplit.train_start,
                      backtestEnd: dataSplit.backtest_end || dataSplit.test_end,
                    }}
                  />
                </div>
              </>
            ) : (
              <>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px" }}>
                  <h2 style={{ margin: 0, fontSize: "18px", fontWeight: 700, color: "#1e293b" }}>1. 多Alpha因子分组 (Multi-Alpha Groups)</h2>
                  {multiAlphaConfig && (
                    <span style={{ backgroundColor: "#f3e8ff", color: "#7c3aed", padding: "4px 12px", borderRadius: "12px", fontSize: "13px", fontWeight: 600 }}>
                      {multiAlphaConfig.alpha_groups.length} 组 / {multiAlphaConfig.alpha_groups.reduce((s, g) => s + g.factor_names.length, 0)} 因子
                    </span>
                  )}
                </div>

                <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", padding: "16px" }}>
                  <MultiAlphaGroupEditor
                    config={multiAlphaConfig}
                    onConfigChange={setMultiAlphaConfig}
                    apiBase={API}
                  />
                </div>
              </>
            )}

            <div style={{ display: "flex", justifyContent: "flex-end", marginTop: "24px" }}>
              <button
                data-testid="qe-step-next-factors"
                onClick={() => setCurrentStep(2)}
                disabled={alphaMode === "single" ? selectedFactors.size === 0 : !multiAlphaConfig}
                style={{
                  ...btnPrimary, padding: "10px 28px", fontSize: "14px",
                  opacity: (alphaMode === "single" ? selectedFactors.size === 0 : !multiAlphaConfig) ? 0.5 : 1,
                }}
              >
                下一步：{alphaMode === "single" ? "选择模型" : "模型汇总"}
              </button>
            </div>
          </div>
        )}

        {/* Step 2: 模型选择 / Multi-Alpha 模型汇总 */}
        {currentStep === 2 && (
          <div style={{ padding: "24px" }}>
            {alphaMode === "single" ? (
              <>
                <h2 style={{ margin: "0 0 16px", fontSize: "18px", fontWeight: 700, color: "#1e293b" }}>2. 模型选择 (Model Selection)</h2>
                <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", overflow: "hidden", marginBottom: "24px" }}>
                  <ModelList
                    mode="selection"
                    selectedModel={selectedModel}
                    onSelectModel={(modelId) => setSelectedModel(modelId)}
                  />
                </div>
              </>
            ) : (
              <>
                <h2 style={{ margin: "0 0 16px", fontSize: "18px", fontWeight: 700, color: "#1e293b" }}>2. 各组模型汇总 (Group Model Summary)</h2>
                {multiAlphaConfig ? (
                  <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", overflow: "hidden", marginBottom: "24px" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                      <thead>
                        <tr style={{ background: "#f9fafb", textAlign: "left" }}>
                          <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12 }}>组名</th>
                          <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12, width: 60, textAlign: "center" }}>因子数</th>
                          <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12 }}>模型</th>
                          <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12 }}>数据集</th>
                          <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12, width: 60, textAlign: "center" }}>计算</th>
                        </tr>
                      </thead>
                      <tbody>
                        {multiAlphaConfig.alpha_groups.map((g) => (
                          <tr key={g.group_name} style={{ borderTop: "1px solid #f3f4f6" }}>
                            <td style={{ padding: "10px 16px", fontWeight: 600, color: "#111827" }}>{g.group_name}</td>
                            <td style={{ padding: "10px 16px", textAlign: "center", fontFamily: "monospace" }}>{g.factor_names.length}</td>
                            <td style={{ padding: "10px 16px", fontFamily: "monospace", fontSize: 12, color: "#4b5563" }}>{g.model_id.replace(/__seed_|__/g, "")}</td>
                            <td style={{ padding: "10px 16px", fontSize: 12, color: "#6b7280" }}>{g.dataset_type}</td>
                            <td style={{ padding: "10px 16px", textAlign: "center" }}>
                              <span style={{
                                padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 600,
                                backgroundColor: g.compute_resource === "gpu" ? "#dcfce7" : "#dbeafe",
                                color: g.compute_resource === "gpu" ? "#166534" : "#1e40af",
                              }}>{g.compute_resource.toUpperCase()}</span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    <div style={{ padding: "12px 16px", backgroundColor: "#f8fafc", borderTop: "1px solid #e2e8f0", fontSize: 12, color: "#64748b" }}>
                      模型已在分组中配置（Step 1），如需调整请返回上一步。Meta-Model: {multiAlphaConfig.meta_model.method} | 执行模式: {multiAlphaConfig.execution_mode}
                    </div>
                  </div>
                ) : (
                  <div style={{ padding: "40px", textAlign: "center", color: "#94a3b8", fontSize: 14 }}>
                    请先在 Step 1 中生成分组配置
                  </div>
                )}
              </>
            )}

            <div style={{ display: "flex", justifyContent: "space-between", marginTop: "24px" }}>
              <button onClick={() => setCurrentStep(1)} style={btnSecondary}>上一步</button>
              <button data-testid="qe-step-next-model" onClick={() => setCurrentStep(3)} style={{ ...btnPrimary, padding: "10px 28px", fontSize: "14px" }}>下一步：选择策略</button>
            </div>
          </div>
        )}

        {/* Step 3: 策略选择 */}
        {currentStep === 3 && (
          <div style={{ padding: "24px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px" }}>
              <h2 style={{ margin: 0, fontSize: "18px", fontWeight: 700, color: "#1e293b" }}>选择交易策略与核心参数</h2>
              <span style={{ fontSize: "13px", color: "#64748b" }}>
                已选择策略：<strong style={{ color: "#2563eb" }}>{selectedStrategy || "未选择"}</strong>
              </span>
            </div>

            {/* 策略列表 */}
            <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", overflow: "hidden", marginBottom: "24px" }}>
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                  <thead>
                    <tr style={{ background: "#f9fafb", textAlign: "left" }}>
                      <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12, width: 60, textAlign: "center" }}>选择</th>
                      <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12 }}>策略名称</th>
                      <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12 }}>描述</th>
                      <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12, width: 70 }}>类型</th>
                      <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12, width: 70 }}>市场</th>
                      <th style={{ padding: "10px 16px", fontWeight: 500, color: "#6b7280", fontSize: 12, width: 80 }}>来源</th>
                    </tr>
                  </thead>
                  <tbody>
                    {strategies.length > 0 ? strategies.map(s => {
                      const tc = TYPE_COLORS[s.strategy_type || "daily"] || TYPE_COLORS.daily;
                      const isSelected = selectedStrategy === s.strategy_id;
                      return (
                        <tr key={s.strategy_id}
                          onClick={() => setSelectedStrategy(s.strategy_id)}
                          style={{ 
                            borderTop: "1px solid #f3f4f6", 
                            cursor: "pointer",
                            background: isSelected ? "#f5f3ff" : "transparent"
                          }}
                          onMouseEnter={e => { if(!isSelected) e.currentTarget.style.background = "#f9fafb" }}
                          onMouseLeave={e => { if(!isSelected) e.currentTarget.style.background = "transparent" }}
                        >
                          <td style={{ padding: "10px 16px", textAlign: "center" }}>
                            <input 
                              type="radio" 
                              name="strategy-selection"
                              checked={isSelected}
                              onChange={() => setSelectedStrategy(s.strategy_id)}
                              style={{ cursor: "pointer", width: 16, height: 16, accentColor: "#7c3aed" }}
                            />
                          </td>
                          <td style={{ padding: "10px 16px" }}>
                            <div style={{ fontWeight: 600, color: "#111827" }}>{s.display_name}</div>
                            <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 2 }}>{s.strategy_id}</div>
                          </td>
                          <td style={{ padding: "10px 16px", color: "#4b5563", fontSize: 12, maxWidth: 320, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={s.description}>
                            {s.description || "-"}
                          </td>
                          <td style={{ padding: "10px 16px" }}>
                            <span style={{ padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 600, background: tc.bg, color: tc.text }}>
                              {tc.label}
                            </span>
                          </td>
                          <td style={{ padding: "10px 16px", fontSize: 12, color: "#6b7280" }}>{s.market || "-"}</td>
                          <td style={{ padding: "10px 16px", fontSize: 12, color: "#6b7280" }}>{s.catalog_source || "-"}</td>
                        </tr>
                      );
                    }) : (
                      <tr>
                        <td colSpan={6} style={{ padding: "32px", textAlign: "center", color: "#9ca3af", fontSize: 14 }}>
                          尚未加载到策略数据
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>

            <div style={{ backgroundColor: "#f8fafc", borderRadius: "8px", padding: "20px", border: "1px solid #e2e8f0", marginBottom: "24px" }}>
              <h3 style={{ margin: "0 0 16px", fontSize: "13px", fontWeight: 700, color: "#1e293b" }}>策略核心参数调整</h3>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "20px", maxWidth: "620px" }}>
                <div>
                  <label style={labelStyle}>Top K (持仓数量)</label>
                  <input data-testid="qe-topk" type="number" value={topk} onChange={e => setTopk(Number(e.target.value))} style={inputStyle} />
                </div>
                <div>
                  <label style={labelStyle}>N Drop (每日替换)</label>
                  <input data-testid="qe-n-drop" type="number" value={nDrop} onChange={e => setNDrop(Number(e.target.value))} style={inputStyle} />
                </div>
                <div>
                  <label style={labelStyle}>Hold Thresh (最短持仓天数)</label>
                  <input data-testid="qe-hold-thresh" type="number" min={1} max={30} value={holdThresh} onChange={e => setHoldThresh(Math.max(1, Number(e.target.value) || 1))} style={inputStyle} />
                </div>
              </div>
              {labelHorizon !== holdThresh && (
                <div style={{ marginTop: "8px", fontSize: "12px", color: "#b45309" }}>
                  训练标签期限为 {labelHorizon}d，最短持仓为 {holdThresh}d；二者不一致可能影响 IC 到收益的转换，请确认这是有意设置。
                </div>
              )}
            </div>

            {/* ── 执行算法选择 ── */}
            <div style={{ backgroundColor: "#f8fafc", borderRadius: "8px", padding: "20px", border: "1px solid #e2e8f0", marginBottom: "24px" }}>
              <h3 style={{ margin: "0 0 16px", fontSize: "13px", fontWeight: 700, color: "#1e293b" }}>日内执行算法（决定分钟线/日线执行方式）</h3>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px", marginBottom: "12px" }}>
                <div>
                  <label style={{ display: "block", fontSize: "12px", fontWeight: 600, color: "#64748b", marginBottom: "4px" }}>执行算法</label>
                  <select
                    value={executionAlgo}
                    onChange={e => {
                      const code = e.target.value;
                      const info = executionAlgoCatalog.find((a: any) => a.algo_code === code);
                      setExecutionAlgo(code);
                      setExecutionAlgoParams(info?.default_config || {});
                      setBacktestFreq(code === "CLOSE_PRICE" ? "day" : "1min");
                      if (code === "CLOSE_PRICE") {
                        setUnfilledHandler("");
                        setUnfilledBackupDepth(15);
                      }
                    }}
                    style={{ width: "100%", padding: "7px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px", boxSizing: "border-box", backgroundColor: "white" }}
                  >
                    <option value="">默认（TailTWAP 分钟线执行）</option>
                    {executionAlgo && executionAlgoCatalog.find((a: any) => a.algo_code === executionAlgo)?.qe_supported === false && (
                      <option value={executionAlgo} disabled>{executionAlgo}（QE 未接入，禁止执行）</option>
                    )}
                    {qeExecutionAlgoCatalog.map((a: any) => (
                      <option key={a.algo_code} value={a.algo_code}>{a.algo_name || a.algo_code}</option>
                    ))}
                  </select>
                </div>
                <div style={{ display: "flex", alignItems: "flex-end" }}>
                  <div style={{ padding: "7px 12px", borderRadius: "6px", fontSize: "12px", fontWeight: 600,
                    backgroundColor: executionAlgo === "CLOSE_PRICE" ? "#fef3c7" : executionAlgo ? "#dbeafe" : "#e0e7ff",
                    color: executionAlgo === "CLOSE_PRICE" ? "#92400e" : executionAlgo ? "#1d4ed8" : "#4338ca",
                    border: "1px solid " + (executionAlgo === "CLOSE_PRICE" ? "#fcd34d" : executionAlgo ? "#93c5fd" : "#a5b4fc"),
                  }}>
                    {executionAlgo === "CLOSE_PRICE" ? "日线回测 (收盘价执行)" : executionAlgo ? "分钟线回测 (日内拆单)" : "分钟线回测 (TailTWAP)"}
                  </div>
                </div>
              </div>
              {executionAlgo && (() => {
                const algoInfo = executionAlgoCatalog.find((a: any) => a.algo_code === executionAlgo);
                const schema = algoInfo?.param_schema?.properties;
                if (!schema || Object.keys(schema).length === 0) return null;
                return (
                  <div style={{ marginTop: "8px" }}>
                    <div style={{ fontSize: "12px", fontWeight: 600, color: "#64748b", marginBottom: "6px" }}>算法参数（{algoInfo?.algo_name || executionAlgo}）</div>
                    <ParamSchemaForm
                      schema={schema}
                      values={executionAlgoParams}
                      onChange={(key, val) => setExecutionAlgoParams(prev => ({ ...prev, [key]: val }))}
                    />
                  </div>
                );
              })()}

              {/* ── 尾盘涨停未成交资金处理策略（仅分钟线回测有效） ── */}
              {executionAlgo !== "CLOSE_PRICE" && (
                <div style={{ marginTop: "12px", paddingTop: "12px", borderTop: "1px dashed #cbd5e1" }}>
                  <div style={{ fontSize: "12px", fontWeight: 600, color: "#64748b", marginBottom: "6px" }}>
                    尾盘涨停未成交资金处理（仅分钟线回测有效）
                  </div>
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px" }}>
                    <select
                      value={unfilledHandler}
                      onChange={e => setUnfilledHandler(e.target.value)}
                      style={{ width: "100%", padding: "7px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px", boxSizing: "border-box", backgroundColor: "white" }}
                    >
                      <option value="">不处理（资金闲置）</option>
                      <option value="TAIL_BOOST">TAIL_BOOST — 加仓已持有股票（14:50）</option>
                      <option value="TAIL_SUBSTITUTE">TAIL_SUBSTITUTE — 替补买入候选股（14:55）</option>
                    </select>
                    {unfilledHandler === "TAIL_SUBSTITUTE" && (
                      <input
                        type="number"
                        min={1}
                        max={100}
                        value={unfilledBackupDepth}
                        onChange={e => setUnfilledBackupDepth(Number(e.target.value) || 15)}
                        placeholder="候选深度（默认15）"
                        style={{ width: "100%", padding: "7px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px", boxSizing: "border-box" }}
                      />
                    )}
                  </div>
                  <div style={{ fontSize: "11px", color: "#94a3b8", marginTop: "6px" }}>
                    TAIL_BOOST 按持仓市值比例加仓已有持仓 / TAIL_SUBSTITUTE 等额买入排名 topk 之后的候选股
                  </div>
                </div>
              )}
            </div>

            <div style={{ display: "flex", justifyContent: "space-between", marginTop: "24px" }}>
              <button onClick={() => setCurrentStep(2)} style={btnSecondary}>上一步</button>
              <button data-testid="qe-step-next-strategy" onClick={() => setCurrentStep(4)} style={{ ...btnPrimary, padding: "10px 28px", fontSize: "14px" }}>下一步：组合配置与评估</button>
            </div>
          </div>
        )}

        {/* Step 4: 组合配置与AI评估 */}
        {currentStep === 4 && (
          <div style={{ padding: "24px" }}>
            <h2 style={{ margin: "0 0 20px", fontSize: "18px", fontWeight: 700, color: "#1e293b" }}>4. 组合配置预览与 AI 评估</h2>

            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "20px", marginBottom: "24px" }}>
              {/* 左侧预览 */}
              <div style={{ backgroundColor: "#f8fafc", borderRadius: "8px", padding: "20px", border: "1px solid #e2e8f0" }}>
                <h3 style={{ margin: "0 0 16px", fontSize: "13px", fontWeight: 700, color: "#1e293b", textTransform: "uppercase", letterSpacing: "0.05em" }}>当前配置清单</h3>

                {alphaMode === "multi" && multiAlphaConfig ? (
                  /* Multi-Alpha 配置预览 */
                  <div style={{ fontSize: "14px", display: "flex", flexDirection: "column", gap: "16px" }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                      <span style={{ color: "#64748b", fontWeight: 500 }}>Alpha 模式</span>
                      <span style={{ fontSize: "14px", fontWeight: 700, color: "#7c3aed" }}>多Alpha (Phase 3)</span>
                    </div>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                      <span style={{ color: "#64748b", fontWeight: 500 }}>分组数 / 总因子</span>
                      <span style={{ fontSize: "20px", fontWeight: 700, fontFamily: "monospace", color: "#7c3aed" }}>
                        {multiAlphaConfig.alpha_groups.length} 组 / {multiAlphaConfig.alpha_groups.reduce((s, g) => s + g.factor_names.length, 0)} 因子
                      </span>
                    </div>
                    {/* 各组摘要 */}
                    <div style={{ maxHeight: "140px", overflowY: "auto", display: "flex", flexDirection: "column", gap: "6px", paddingBottom: "12px", borderBottom: "1px solid #f1f5f9" }}>
                      {multiAlphaConfig.alpha_groups.map(g => (
                        <div key={g.group_name} style={{ display: "flex", justifyContent: "space-between", fontSize: 12, padding: "4px 0" }}>
                          <span style={{ fontWeight: 600, color: "#374151" }}>{g.group_name}</span>
                          <span style={{ color: "#64748b", fontFamily: "monospace" }}>
                            {g.factor_names.length}因子 | {g.model_id.replace(/__seed_|__/g, "")} | {g.compute_resource.toUpperCase()}
                          </span>
                        </div>
                      ))}
                    </div>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                      <span style={{ color: "#64748b", fontWeight: 500 }}>Meta-Model</span>
                      <span style={{ fontWeight: 600, color: "#1e293b" }}>{multiAlphaConfig.meta_model.method} | {multiAlphaConfig.meta_model.cv_strategy}</span>
                    </div>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                      <span style={{ color: "#64748b", fontWeight: 500 }}>执行模式</span>
                      <span style={{ fontWeight: 600, color: "#1e293b" }}>{multiAlphaConfig.execution_mode}</span>
                    </div>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                      <span style={{ color: "#64748b", fontWeight: 500 }}>选定策略</span>
                      <span style={{ fontWeight: 600, color: "#1e293b" }}>
                        {(() => {
                          if (!selectedStrategy) return "未选择";
                          const s = strategies.find(s => s.strategy_id === selectedStrategy);
                          return `${s?.display_name || selectedStrategy} (TopK=${topk}, n_drop=${nDrop}, hold=${holdThresh}d, label=${labelHorizon}d)`;
                        })()}
                      </span>
                    </div>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                      <span style={{ color: "#64748b", fontWeight: 500 }}>执行算法</span>
                      <span style={{ fontWeight: 600, color: "#1e293b" }}>
                        {executionAlgo === "CLOSE_PRICE" ? "收盘价执行 (日线)" : executionAlgo ? `${executionAlgoCatalog.find((a: any) => a.algo_code === executionAlgo)?.algo_name || executionAlgo} (分钟线)` : "默认 TailTWAP (分钟线)"}
                      </span>
                    </div>
                  </div>
                ) : (
                  /* Single-Alpha 配置预览（原有逻辑） */
                  <div style={{ fontSize: "14px", display: "flex", flexDirection: "column", gap: "16px" }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                      <span style={{ color: "#64748b", fontWeight: 500 }}>已选因子数量</span>
                      <span style={{ fontSize: "20px", fontWeight: 700, fontFamily: "monospace", color: "#059669" }}>{selectedFactors.size} 个</span>
                    </div>
                    {selectedFactors.size > 0 && (
                      <div style={{ display: "flex", flexWrap: "wrap", gap: "6px", paddingBottom: "12px", borderBottom: "1px solid #f1f5f9", maxHeight: "120px", overflowY: "auto" }}>
                        {Array.from(selectedFactors).map(k => {
                          const name = k.split("||")[0];
                          return (
                            <span key={k} style={{ padding: "2px 8px", backgroundColor: "#dbeafe", color: "#2563eb", fontSize: "11px", borderRadius: "4px", fontFamily: "monospace", border: "1px solid #bfdbfe" }}>
                              {name}
                            </span>
                          );
                        })}
                      </div>
                    )}
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                      <span style={{ color: "#64748b", fontWeight: 500 }}>选定模型</span>
                      <span style={{ fontWeight: 600, color: "#1e293b", textAlign: "right", maxWidth: "200px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={selectedModel}>
                        {(() => {
                          if (!selectedModel) return "未选择";
                          const m = models.find(m => m.model_id === selectedModel);
                          return m ? `${m.display_name || m.model_name} (${m.model_type || "未知类型"})` : selectedModel;
                        })()}
                      </span>
                    </div>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                      <span style={{ color: "#64748b", fontWeight: 500 }}>选定策略</span>
                      <span style={{ fontWeight: 600, color: "#1e293b" }}>
                        {(() => {
                          if (!selectedStrategy) return "未选择";
                          const s = strategies.find(s => s.strategy_id === selectedStrategy);
                          return `${s?.display_name || selectedStrategy} (TopK=${topk}, n_drop=${nDrop}, hold=${holdThresh}d, label=${labelHorizon}d)`;
                        })()}
                      </span>
                    </div>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", borderBottom: "1px solid #f1f5f9", paddingBottom: "8px" }}>
                      <span style={{ color: "#64748b", fontWeight: 500 }}>执行算法</span>
                      <span style={{ fontWeight: 600, color: "#1e293b" }}>
                        {executionAlgo === "CLOSE_PRICE" ? "收盘价执行 (日线)" : executionAlgo ? `${executionAlgoCatalog.find((a: any) => a.algo_code === executionAlgo)?.algo_name || executionAlgo} (分钟线)` : "默认 TailTWAP (分钟线)"}
                      </span>
                    </div>
                  </div>
                )}

                <div style={{ marginTop: "20px", paddingTop: "16px", borderTop: "1px solid #e2e8f0" }}>
                  <button onClick={evaluateCombination} disabled={actionLoading === "evaluate" || (alphaMode === "single" && selectedFactors.size === 0)}
                    style={{ ...btnPrimary, width: "100%", justifyContent: "center", padding: "10px 20px", fontSize: "14px", backgroundColor: "#3b82f6", opacity: (actionLoading === "evaluate" || (alphaMode === "single" && selectedFactors.size === 0)) ? 0.5 : 1 }}>
                    {actionLoading === "evaluate" ? "正在调用 LLM 进行深度评估..." : "AI 评估此组合的合理性"}
                  </button>
                </div>
              </div>

              {/* 右侧评估结果 */}
              <div style={{ backgroundColor: "#ffffff", borderRadius: "8px", padding: "20px", border: "1px solid #e2e8f0", boxShadow: "0 1px 3px rgba(0,0,0,0.05)", minHeight: "250px", display: "flex", flexDirection: "column" }}>
                {evalResult ? (
                  <div>
                    <div style={{ display: "flex", alignItems: "center", gap: "16px", marginBottom: "16px" }}>
                      <div style={{ width: "56px", height: "56px", borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center", fontSize: "20px", fontWeight: 700, backgroundColor: "#dbeafe", color: "#1d4ed8", border: "3px solid #93c5fd" }}>
                        {evalResult.overall_score?.toFixed(0)}
                      </div>
                      <div>
                        <h3 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#1e293b" }}>综合评估得分</h3>
                        <p style={{ margin: "4px 0 0", fontSize: "12px", color: "#64748b" }}>基于多维度因子互补性和策略匹配度</p>
                      </div>
                    </div>
                    {evalResult.llm_commentary && (
                      <div style={{ fontSize: "14px", color: "#334155", lineHeight: 1.6, marginBottom: "16px", whiteSpace: "pre-wrap", padding: "12px", backgroundColor: "#f8fafc", borderRadius: "6px", border: "1px solid #f1f5f9" }}>
                        {evalResult.llm_commentary}
                      </div>
                    )}
                    {evalResult.risks && evalResult.risks.length > 0 && (
                      <div style={{ marginBottom: "12px" }}>
                        <h4 style={{ margin: "0 0 8px", fontSize: "12px", fontWeight: 700, color: "#dc2626", textTransform: "uppercase", letterSpacing: "0.05em" }}>识别到的风险</h4>
                        {evalResult.risks.map((r, i) => (
                          <div key={i} style={{ fontSize: "12px", backgroundColor: "#fef2f2", color: "#b91c1c", padding: "8px 12px", borderRadius: "6px", marginBottom: "4px", border: "1px solid #fecaca" }}>{r.message}</div>
                        ))}
                      </div>
                    )}
                  </div>
                ) : (
                  <div style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", color: "#94a3b8" }}>
                    <div style={{ fontSize: "36px", marginBottom: "8px" }}>🤖</div>
                    <p style={{ margin: 0, fontSize: "14px" }}>点击左侧按钮，使用 AI 分析您的投资组合</p>
                  </div>
                )}
              </div>
            </div>

            {/* 因子独立IC汇总 + 相关性检查 */}
            {selectedFactors.size > 0 && (
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "20px", marginTop: "20px" }}>
                {/* 左: 因子独立IC汇总 */}
                <div style={{ backgroundColor: "#f8fafc", borderRadius: "8px", padding: "16px", border: "1px solid #e2e8f0" }}>
                  <h3 style={{ margin: "0 0 12px", fontSize: "13px", fontWeight: 700, color: "#1e293b", textTransform: "uppercase", letterSpacing: "0.05em" }}>
                    已选因子独立指标 ({selectedFactorMetrics.length})
                  </h3>
                  <div style={{ maxHeight: "280px", overflowY: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                      <thead>
                        <tr style={{ background: "#e2e8f0", position: "sticky", top: 0 }}>
                          <th style={{ padding: "6px 8px", textAlign: "left", fontWeight: 600, color: "#475569" }}>因子</th>
                          <th style={{ padding: "6px 8px", textAlign: "center", fontWeight: 600, color: "#475569", width: 50 }}>评级</th>
                          <th style={{ padding: "6px 8px", textAlign: "right", fontWeight: 600, color: "#475569", width: 70 }}>IC</th>
                          <th style={{ padding: "6px 8px", textAlign: "right", fontWeight: 600, color: "#475569", width: 70 }}>Sharpe</th>
                          <th style={{ padding: "6px 8px", textAlign: "right", fontWeight: 600, color: "#475569", width: 80 }}>年化</th>
                        </tr>
                      </thead>
                      <tbody>
                        {selectedFactorMetrics.map((f, i) => (
                          <tr key={f.name} style={{ borderTop: "1px solid #f1f5f9", backgroundColor: i % 2 === 0 ? "#fff" : "#f8fafc" }}>
                            <td style={{ padding: "5px 8px", fontFamily: "monospace", fontSize: 11, maxWidth: 160, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={f.name}>{f.name}</td>
                            <td style={{ padding: "5px 8px", textAlign: "center" }}>
                              {f.official_grade ? <span style={{ padding: "1px 6px", borderRadius: 3, fontSize: 10, fontWeight: 700, color: "#fff", backgroundColor: f.official_grade === "S" ? "#7c3aed" : f.official_grade === "A" ? "#2563eb" : f.official_grade === "B" ? "#10b981" : f.official_grade === "C" ? "#f59e0b" : "#ef4444" }}>{f.official_grade}</span> : "-"}
                            </td>
                            <td style={{ padding: "5px 8px", textAlign: "right", fontFamily: "monospace", color: (f.ic ?? 0) > 0.03 ? "#059669" : "#64748b" }}>{f.ic != null ? f.ic.toFixed(4) : "-"}</td>
                            <td style={{ padding: "5px 8px", textAlign: "right", fontFamily: "monospace", color: "#64748b" }}>{f.sharpe != null ? f.sharpe.toFixed(3) : "-"}</td>
                            <td style={{ padding: "5px 8px", textAlign: "right", fontFamily: "monospace", color: (f.ann_ret ?? 0) > 0 ? "#059669" : "#ef4444" }}>{f.ann_ret != null ? (f.ann_ret * 100).toFixed(1) + "%" : "-"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  {selectedFactorMetrics.length > 0 && (() => {
                    const ics = selectedFactorMetrics.filter(f => f.ic != null).map(f => f.ic!);
                    const avgIc = ics.length > 0 ? ics.reduce((a, b) => a + b, 0) / ics.length : null;
                    const noIcCount = selectedFactorMetrics.filter(f => f.ic == null).length;
                    return (
                      <div style={{ marginTop: "8px", paddingTop: "8px", borderTop: "1px solid #e2e8f0", fontSize: 11, color: "#64748b", display: "flex", gap: "16px" }}>
                        <span>平均 IC: <strong style={{ color: (avgIc ?? 0) > 0.03 ? "#059669" : "#64748b" }}>{avgIc != null ? avgIc.toFixed(4) : "N/A"}</strong></span>
                        {noIcCount > 0 && <span style={{ color: "#f59e0b" }}>{noIcCount} 个因子无 IC 数据</span>}
                      </div>
                    );
                  })()}
                </div>

                {/* 右: 因子相关性检查 */}
                <div style={{ backgroundColor: "#f8fafc", borderRadius: "8px", padding: "16px", border: "1px solid #e2e8f0" }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "12px" }}>
                    <h3 style={{ margin: 0, fontSize: "13px", fontWeight: 700, color: "#1e293b", textTransform: "uppercase", letterSpacing: "0.05em" }}>因子相关性检查</h3>
                    <button
                      onClick={() => { if (corrAnalyzed) { setCorrAnalyzed(false); setCorrPairs([]); } else { checkCorrelations(); } }}
                      disabled={corrLoading || selectedFactors.size < 2}
                      style={{
                        padding: "4px 12px", fontSize: "11px", fontWeight: 600, borderRadius: "4px", cursor: corrLoading || selectedFactors.size < 2 ? "not-allowed" : "pointer",
                        backgroundColor: corrAnalyzed ? "#fef3c7" : "#eff6ff", color: corrAnalyzed ? "#92400e" : "#2563eb",
                        border: corrAnalyzed ? "1px solid #fde68a" : "1px solid #bfdbfe",
                        opacity: corrLoading || selectedFactors.size < 2 ? 0.5 : 1,
                      }}
                    >
                      {corrLoading ? "分析中..." : corrAnalyzed ? "收起" : "检查相关性"}
                    </button>
                  </div>

                  {!corrAnalyzed ? (
                    <div style={{ color: "#94a3b8", fontSize: 13, textAlign: "center", padding: "40px 0" }}>
                      点击&quot;检查相关性&quot;查看已选因子间的相关性矩阵
                    </div>
                  ) : (
                    <div style={{ maxHeight: "280px", overflowY: "auto" }}>
                      {corrPairs.length === 0 ? (
                        <div style={{ color: "#059669", fontSize: 13, textAlign: "center", padding: "40px 0" }}>
                          已选因子间无高相关性对 (|corr| &lt; 0.5)，组合多样性良好
                        </div>
                      ) : (() => {
                        const highPairs = corrPairs.filter(p => Math.abs(p.correlation) > 0.7);
                        const medPairs = corrPairs.filter(p => Math.abs(p.correlation) > 0.5 && Math.abs(p.correlation) <= 0.7);
                        return (
                          <div>
                            {highPairs.length > 0 && (
                              <div style={{ marginBottom: "8px" }}>
                                <div style={{ fontSize: 11, fontWeight: 600, color: "#dc2626", marginBottom: "4px" }}>高相关 (|corr| &gt; 0.7) - {highPairs.length} 对</div>
                                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                                  <tbody>
                                    {highPairs.sort((a, b) => Math.abs(b.correlation) - Math.abs(a.correlation)).map((p, i) => (
                                      <tr key={i} style={{ backgroundColor: Math.abs(p.correlation) > 0.85 ? "#fecaca" : "#fed7aa", borderBottom: "1px solid #fde68a" }}>
                                        {renderCorrelationFactorCell(p.factor_a, "danger")}
                                        {renderCorrelationFactorCell(p.factor_b, "danger")}
                                        <td style={{ padding: "3px 6px", textAlign: "right", fontWeight: 600 }}>{p.correlation.toFixed(4)}</td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            )}
                            {medPairs.length > 0 && (
                              <div>
                                <div style={{ fontSize: 11, fontWeight: 600, color: "#d97706", marginBottom: "4px" }}>中等相关 (0.5 &lt; |corr| &le; 0.7) - {medPairs.length} 对</div>
                                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                                  <tbody>
                                    {medPairs.sort((a, b) => Math.abs(b.correlation) - Math.abs(a.correlation)).map((p, i) => (
                                      <tr key={i} style={{ backgroundColor: "#fef9c3", borderBottom: "1px solid #fde68a" }}>
                                        {renderCorrelationFactorCell(p.factor_a, "warning")}
                                        {renderCorrelationFactorCell(p.factor_b, "warning")}
                                        <td style={{ padding: "3px 6px", textAlign: "right", color: "#92400e" }}>{p.correlation.toFixed(4)}</td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            )}
                            <div style={{ marginTop: "8px", paddingTop: "8px", borderTop: "1px solid #e2e8f0", fontSize: 11, color: "#64748b" }}>
                              {highPairs.length > 0 && <span style={{ color: "#dc2626" }}>建议移除 {highPairs.length} 对高相关因子中的冗余项</span>}
                              {highPairs.length === 0 && <span style={{ color: "#059669" }}>无高相关因子对，组合较为健康</span>}
                            </div>
                          </div>
                        );
                      })()}
                    </div>
                  )}
                </div>
              </div>
            )}

            <div style={{ display: "flex", justifyContent: "space-between", marginTop: "24px", paddingTop: "20px", borderTop: "1px solid #f1f5f9" }}>
              <button onClick={() => setCurrentStep(3)} style={btnSecondary}>上一步</button>
              <button data-testid="qe-step-next-review" onClick={() => setCurrentStep(5)} style={{ ...btnPrimary, padding: "10px 28px", fontSize: "14px" }}>确认配置并进入下一步</button>
            </div>
          </div>
        )}

        {/* Step 5: 任务下发设置 */}
        {currentStep === 5 && (
          <div style={{ padding: "24px" }}>
            <h2 style={{ margin: "0 0 20px", fontSize: "18px", fontWeight: 700, color: "#1e293b" }}>5. 任务下发设置 (Task Dispatching)</h2>

            <div style={{ backgroundColor: "#f8fafc", borderRadius: "8px", padding: "20px", border: "1px solid #e2e8f0", marginBottom: "24px" }}>
              <h3 style={{ margin: "0 0 16px", fontSize: "13px", fontWeight: 700, color: "#1e293b", textTransform: "uppercase", letterSpacing: "0.05em" }}>时间区间与数据基线</h3>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "20px", marginBottom: "16px" }}>
                {[ { label: "训练集", sk: "train_start", ek: "train_end" }, { label: "验证集", sk: "valid_start", ek: "valid_end" }, { label: "测试/信号集", sk: "test_start", ek: "test_end" } ].map(seg => (
                  <div key={seg.label}>
                    <label style={labelStyle}>{seg.label}</label>
                    <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                      <input data-testid={`qe-date-${seg.sk}`} type="date" value={(dataSplit as any)[seg.sk]} onChange={e => setDataSplit(p => ({ ...p, [seg.sk]: e.target.value }))} style={{ ...inputStyle, padding: "6px 8px", fontSize: "12px" }} />
                      <span style={{ color: "#94a3b8" }}>-</span>
                      <input data-testid={`qe-date-${seg.ek}`} type="date" value={(dataSplit as any)[seg.ek]} onChange={e => setDataSplit(p => {
                        const next = { ...p, [seg.ek]: e.target.value };
                        if (seg.ek === "test_end") next.backtest_end = deriveBacktestEnd(e.target.value);
                        return next;
                      })} style={{ ...inputStyle, padding: "6px 8px", fontSize: "12px" }} />
                    </div>
                  </div>
                ))}
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 2fr", gap: "20px", alignItems: "end", marginTop: "8px" }}>
                <div>
                  <label style={labelStyle}>组合回测截止</label>
                  <input data-testid="qe-date-backtest-end" type="date" value={dataSplit.backtest_end || deriveBacktestEnd(dataSplit.test_end)} onChange={e => setDataSplit(p => ({ ...p, backtest_end: e.target.value }))} style={{ ...inputStyle, padding: "6px 8px", fontSize: "12px" }} />
                </div>
                <div style={{ fontSize: "12px", color: "#64748b", lineHeight: 1.6 }}>
                  默认回测到 {QE_DEFAULT_BACKTEST_END}；测试/信号数据保留到 {QE_DEFAULT_SIGNAL_END}，用于 Qlib 下一交易日价格和标签读取，避免最后一日日历越界。
                </div>
              </div>
              <div style={{ display: "flex", gap: "24px", marginTop: "16px", paddingTop: "16px", borderTop: "1px solid #e2e8f0" }}>
                <label style={{ display: "flex", alignItems: "center", gap: "8px", fontSize: "14px", color: "#475569", cursor: "pointer" }}>
                  <input type="checkbox" checked={disableAlphaBaseline} onChange={e => setDisableAlphaBaseline(e.target.checked)} style={{ width: "16px", height: "16px", accentColor: "#2563eb" }} />
                  禁用 Alpha158 基线因子
                </label>
                <label style={{ display: "flex", alignItems: "center", gap: "8px", fontSize: "14px", color: "#475569", cursor: "pointer" }}>
                  <input data-testid="qe-quick-train" type="checkbox" checked={quickTrain} onChange={e => setQuickTrain(e.target.checked)} style={{ width: "16px", height: "16px", accentColor: "#2563eb" }} />
                  启用快速训练模式 <span style={{ fontSize: "12px", color: "#d97706", marginLeft: "4px" }}>(训练时间缩短至20%)</span>
                </label>
              </div>
              <div style={{ marginTop: "16px", padding: "12px 14px", borderRadius: "8px", border: "1px solid #bfdbfe", backgroundColor: "#eff6ff" }}>
                <label style={{ ...labelStyle, color: "#1d4ed8" }}>固定随机种子（必填）</label>
                <div style={{ display: "grid", gridTemplateColumns: "220px 1fr", gap: "12px", alignItems: "center" }}>
                  <input
                    data-testid="qe-random-seed"
                    type="number"
                    min={0}
                    max={QE_RANDOM_SEED_MAX}
                    step={1}
                    value={randomSeed}
                    onChange={e => setRandomSeed(parseQeRandomSeedInput(e.target.value))}
                    style={{ ...inputStyle, fontFamily: "monospace", borderColor: isValidQeRandomSeed(randomSeed) ? "#93c5fd" : "#ef4444" }}
                  />
                  <div style={{ fontSize: "12px", color: "#475569", lineHeight: 1.6 }}>
                    该种子会写入 custom_params.random_seed、生成配置和归档元数据；训练型 QE 任务缺失种子会被后端拒绝。
                  </div>
                </div>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: "12px", marginTop: "12px" }}>
                <span style={{ fontSize: "14px", color: "#475569", fontWeight: 600, whiteSpace: "nowrap" }}>训练标签</span>
                {(["close", "open", "vwap"] as const).map(lt => (
                  <label key={lt} onClick={() => setLabelType(lt)} style={{
                    display: "flex", alignItems: "center", gap: "6px", padding: "6px 14px",
                    borderRadius: "6px", cursor: "pointer", fontSize: "13px", fontWeight: 500, transition: "all 0.2s",
                    border: labelType === lt ? "2px solid #3b82f6" : "2px solid #e2e8f0",
                    backgroundColor: labelType === lt ? "#eff6ff" : "#ffffff",
                    color: labelType === lt ? "#1d4ed8" : "#64748b",
                  }}>
                    <div style={{
                      width: "14px", height: "14px", borderRadius: "50%",
                      border: `2px solid ${labelType === lt ? "#3b82f6" : "#cbd5e1"}`,
                      display: "flex", alignItems: "center", justifyContent: "center",
                    }}>
                      {labelType === lt && <div style={{ width: "6px", height: "6px", backgroundColor: "#3b82f6", borderRadius: "50%" }} />}
                    </div>
                    {lt === "close" ? "Close-to-Close" : lt === "open" ? "Open-to-Open" : "VWAP-to-VWAP"}
                    {lt === "close" && <span style={{ fontSize: "11px", color: "#94a3b8" }}>(默认)</span>}
                    {lt === "open" && <span style={{ fontSize: "11px", color: "#d97706" }}>(可执行价)</span>}
                  </label>
                ))}
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: "12px", marginTop: "12px", flexWrap: "wrap" }}>
                <span style={{ fontSize: "14px", color: "#475569", fontWeight: 600, whiteSpace: "nowrap" }}>Label Horizon</span>
                {([1, 3, 5, 10, 20] as const).map(h => (
                  <button
                    data-testid={`qe-label-horizon-${h}`}
                    key={h}
                    type="button"
                    onClick={() => setLabelHorizon(h)}
                    style={{
                      padding: "6px 14px",
                      borderRadius: "6px",
                      border: labelHorizon === h ? "2px solid #0f766e" : "2px solid #e2e8f0",
                      backgroundColor: labelHorizon === h ? "#ccfbf1" : "#ffffff",
                      color: labelHorizon === h ? "#0f766e" : "#64748b",
                      fontSize: "13px",
                      fontWeight: 700,
                      cursor: "pointer",
                    }}
                  >
                    {h}d
                  </button>
                ))}
                <button
                  data-testid="qe-sync-hold-thresh"
                  type="button"
                  onClick={() => setHoldThresh(labelHorizon)}
                  style={{ padding: "6px 10px", borderRadius: "6px", border: "1px solid #14b8a6", background: "#f0fdfa", color: "#0f766e", fontSize: "12px", fontWeight: 700, cursor: "pointer" }}
                >
                  同步最短持仓={labelHorizon}d
                </button>
                <span style={{ fontSize: "12px", color: "#64748b" }}>
                  公式：T 日特征预测 T+1 到 T+{labelHorizon + 1} forward return；1d 保持旧逻辑。
                </span>
              </div>
            </div>

            <SectorBlacklistPanel
              enabled={blacklistEnabled}
              onEnabledChange={setBlacklistEnabled}
              onPoolPathChange={setStockPoolPath}
              onBlacklistSnapshotChange={setBlacklistSnapshot}
            />


            <div style={{ marginBottom: "24px", padding: "16px 20px", backgroundColor: "#f8fafc", borderRadius: "8px", border: "1px solid #e2e8f0" }}>
              <label style={{ display: "flex", alignItems: "center", gap: "8px", fontSize: "14px", color: "#475569", cursor: "pointer", fontWeight: 600 }}>
                <input data-testid="qe-filter-suspended" type="checkbox" checked={filterSuspendedOnSignal} onChange={e => setFilterSuspendedOnSignal(e.target.checked)} style={{ width: "16px", height: "16px", accentColor: "#0f766e" }} />
                启用 suspend_d 日频选股停牌过滤
              </label>
              <div style={{ fontSize: "12px", color: "#64748b", marginTop: "6px" }}>
                生成每日选股信号时从本地 artifact 过滤 market.suspend_d 已停牌股票，Qlib 回测过程中不逐日访问数据库。
              </div>
              {filterSuspendedOnSignal && (
                <label style={{ display: "flex", alignItems: "center", gap: "8px", fontSize: "13px", color: "#475569", cursor: "pointer", marginTop: "10px" }}>
                  <input data-testid="qe-suspend-filter-strict" type="checkbox" checked={suspendFilterStrict} onChange={e => setSuspendFilterStrict(e.target.checked)} style={{ width: "15px", height: "15px", accentColor: "#0f766e" }} />
                  严格校验 suspend_d 每个回测交易日的刷新审计；缺失时直接失败
                </label>
              )}
            </div>

            {/* ── HMM 行业板块轮动 ── */}
            <div style={{ marginBottom: "24px", padding: "16px 20px", backgroundColor: "#f8fafc", borderRadius: "8px", border: "1px solid #e2e8f0" }}>
              <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: enableSectorHmm ? "12px" : "0" }}>
                <input type="checkbox" id="compose-enable-hmm" checked={enableSectorHmm}
                  onChange={e => {
                    const v = e.target.checked;
                    setEnableSectorHmm(v);
                    if (!v) { setHmmSelectedConfigId(""); setHmmSnapshots([]); setHmmModelVersionId(""); }
                    if (v && hmmConfigs.length === 0) fetchHmmConfigs();
                  }}
                />
                <label htmlFor="compose-enable-hmm" style={{ fontSize: "13px", fontWeight: 600, color: "#475569" }}>
                  启用行业 HMM 热度调整（板块轮动）
                </label>
              </div>
              {enableSectorHmm && (
                <>
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px" }}>
                    <div>
                      <label style={{ display: "block", fontSize: "12px", fontWeight: 500, color: "#6b7280", marginBottom: "4px" }}>选择配置版本</label>
                      <select value={hmmSelectedConfigId}
                        onChange={e => { const cid = e.target.value; setHmmSelectedConfigId(cid); setHmmModelVersionId(""); fetchHmmSnapshots(cid); }}
                        style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px", boxSizing: "border-box", backgroundColor: "white" }}
                      >
                        <option value="">-- 选择配置 --</option>
                        {hmmConfigs.map((c: any) => <option key={c.config_id} value={c.config_id}>{c.display_name}</option>)}
                      </select>
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: "12px", fontWeight: 500, color: "#6b7280", marginBottom: "4px" }}>选择时间快照</label>
                      <select value={hmmModelVersionId}
                        onChange={e => setHmmModelVersionId(e.target.value)}
                        disabled={!hmmSelectedConfigId}
                        style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px", boxSizing: "border-box", backgroundColor: hmmSelectedConfigId ? "white" : "#f1f5f9" }}
                      >
                        <option value="">-- 选择快照 --</option>
                        {hmmSnapshots.map((s: any) => (
                          <option key={s.snapshot_id} value={s.snapshot_id}>
                            {new Date(s.trained_at).toLocaleString("zh-CN")} ({s.sector_count} 行业)
                          </option>
                        ))}
                      </select>
                      {hmmSelectedConfigId && hmmSnapshots.length === 0 && (
                        <div style={{ fontSize: "11px", color: "#d97706", marginTop: "4px" }}>该配置暂无已完成的快照</div>
                      )}
                    </div>
                  </div>
                  <div style={{ marginTop: "12px" }}>
                    <label style={{ display: "block", fontSize: "12px", fontWeight: 500, color: "#6b7280", marginBottom: "6px" }}>信号系数档位</label>
                    <div style={{ display: "flex", gap: "12px" }}>
                      <label style={{ display: "flex", alignItems: "center", gap: "6px", cursor: "pointer", fontSize: "13px", color: "#374151" }}>
                        <input type="radio" name="compose-hmm-preset" value="preset_A" checked={hmmSignalPreset === "preset_A"} onChange={() => setHmmSignalPreset("preset_A")} />
                        保守档（热态+5% / 冷态-4%）
                      </label>
                      <label style={{ display: "flex", alignItems: "center", gap: "6px", cursor: "pointer", fontSize: "13px", color: "#374151" }}>
                        <input type="radio" name="compose-hmm-preset" value="preset_B" checked={hmmSignalPreset === "preset_B"} onChange={() => setHmmSignalPreset("preset_B")} />
                        激进档（热态+10% / 冷态-8%）
                      </label>
                    </div>
                  </div>
                </>
              )}
            </div>

            {/* ── 回测频率（已在策略选择步骤配置）── */}
            <div style={{ marginBottom: "24px", padding: "16px 20px", backgroundColor: "#f8fafc", borderRadius: "8px", border: "1px solid #e2e8f0" }}>
              <div style={{ fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "8px" }}>回测执行频率</div>
              <div style={{ fontSize: "13px", color: "#334155" }}>
                {executionAlgo === "CLOSE_PRICE" ? "日线回测（收盘价执行）" : executionAlgo ? `分钟线回测（${executionAlgoCatalog.find((a: any) => a.algo_code === executionAlgo)?.algo_name || executionAlgo}）` : "分钟线回测（默认 TailTWAP 执行）"}
                <span style={{ fontSize: "11px", color: "#94a3b8", marginLeft: "8px" }}>← 在「策略选择」步骤中配置</span>
              </div>
            </div>

            <div style={{ marginBottom: "32px" }}>
              <h3 style={{ margin: "0 0 16px", fontSize: "13px", fontWeight: 700, color: "#1e293b", textTransform: "uppercase", letterSpacing: "0.05em" }}>选择任务分流模式</h3>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "16px" }}>
                <div onClick={() => setDispatchMode("independent")}
                  style={{
                    padding: "20px", borderRadius: "8px", cursor: "pointer", transition: "all 0.2s",
                    border: dispatchMode === "independent" ? "2px solid #3b82f6" : "2px solid #e2e8f0",
                    backgroundColor: dispatchMode === "independent" ? "#eff6ff" : "#ffffff",
                  }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
                    <h4 style={{ margin: 0, fontWeight: 700, color: "#1d4ed8", fontSize: "16px" }}>执行独立任务 (单次回测)</h4>
                    <div style={{ width: "20px", height: "20px", borderRadius: "50%", border: "2px solid #93c5fd", display: "flex", alignItems: "center", justifyContent: "center", backgroundColor: "#fff" }}>
                      {dispatchMode === "independent" && <div style={{ width: "10px", height: "10px", backgroundColor: "#3b82f6", borderRadius: "50%" }} />}
                    </div>
                  </div>
                  <p style={{ margin: 0, fontSize: "13px", color: "#475569", lineHeight: 1.5 }}>单次生成 QLib 配置并发送到 WSL 环境执行，不会触发自动循环演进。适合验证当前组合的效果。</p>
                </div>

                <div onClick={() => setDispatchMode("evolution")}
                  style={{
                    padding: "20px", borderRadius: "8px", cursor: "pointer", transition: "all 0.2s",
                    border: dispatchMode === "evolution" ? "2px solid #7c3aed" : "2px solid #e2e8f0",
                    backgroundColor: dispatchMode === "evolution" ? "#f5f3ff" : "#ffffff",
                  }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
                    <h4 style={{ margin: 0, fontWeight: 700, color: "#6d28d9", fontSize: "16px" }}>启动 QE 自动演进 (持续迭代)</h4>
                    <div style={{ width: "20px", height: "20px", borderRadius: "50%", border: "2px solid #c4b5fd", display: "flex", alignItems: "center", justifyContent: "center", backgroundColor: "#fff" }}>
                      {dispatchMode === "evolution" && <div style={{ width: "10px", height: "10px", backgroundColor: "#7c3aed", borderRadius: "50%" }} />}
                    </div>
                  </div>
                  <p style={{ margin: "0 0 12px", fontSize: "13px", color: "#475569", lineHeight: 1.5 }}>以此配置作为 Task 0 (初始环境)，交给 AIstock 调度大脑进行自动化、循环的因子与模型挖掘。</p>

                  {dispatchMode === "evolution" && (
                    <div style={{ display: "flex", flexDirection: "column", gap: "12px", marginTop: "16px", paddingTop: "16px", borderTop: "1px solid #ddd6fe" }} onClick={e => e.stopPropagation()}>
                      <div>
                        <label style={{ ...labelStyle, color: "#6d28d9" }}>演进总体目标描述</label>
                        <input type="text" value={evolutionObjective} onChange={e => setEvolutionObjective(e.target.value)} placeholder="例如：挖掘低相关性的新动量因子，提升多头收益" style={{ ...inputStyle, borderColor: "#c4b5fd", fontSize: "13px" }} />
                      </div>
                      <div>
                        <label style={{ ...labelStyle, color: "#6d28d9" }}>预设循环迭代次数 (Loops)</label>
                        <input type="number" value={evolutionLoops} onChange={e => setEvolutionLoops(Number(e.target.value))} style={{ ...inputStyle, width: "96px", borderColor: "#c4b5fd", fontSize: "13px" }} />
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>

            {configResult?.ok && (
              <div style={{ backgroundColor: configResult.alpha_mode === "multi" ? "#faf5ff" : "#dcfce7", border: `1px solid ${configResult.alpha_mode === "multi" ? "#d8b4fe" : "#86efac"}`, borderRadius: "8px", padding: "20px", marginBottom: "24px" }}>
                <h3 style={{ margin: "0 0 8px", fontWeight: 700, color: configResult.alpha_mode === "multi" ? "#7c3aed" : "#166534", fontSize: "15px" }}>
                  {configResult.alpha_mode === "multi" ? "多Alpha 实验已生成！" : "任务已成功生成！"}
                </h3>
                <p style={{ margin: "0 0 4px", fontSize: "13px", color: configResult.alpha_mode === "multi" ? "#6d28d9" : "#15803d" }}><strong>Experiment ID:</strong> {configResult.experiment_id}</p>
                {configResult.experiment_dir && <p style={{ margin: "0 0 4px", fontSize: "13px", color: configResult.alpha_mode === "multi" ? "#6d28d9" : "#15803d" }}><strong>工作目录:</strong> {configResult.experiment_dir}</p>}

                {/* 多Alpha 节点分配展示 */}
                {configResult.alpha_mode === "multi" && configResult.node_distribution && (
                  <div style={{ margin: "8px 0 12px", padding: "10px", background: "#f5f3ff", borderRadius: 6, fontSize: 12 }}>
                    <div style={{ fontWeight: 600, color: "#5b21b6", marginBottom: 6 }}>
                      {configResult.total_groups} 组 | {configResult.execution_mode} 模式 | Meta: {configResult.meta_method}
                      {configResult.is_distributed && <span style={{ marginLeft: 6, padding: "1px 6px", background: "#7c3aed", color: "#fff", borderRadius: 10, fontSize: 10, fontWeight: 700 }}>分布式</span>}
                    </div>
                    {Object.entries(configResult.node_distribution).map(([nodeId, groupNames]) => (
                      <div key={nodeId} style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 3 }}>
                        <span style={{ padding: "1px 6px", background: nodeId.includes("wsl") ? "#dbeafe" : "#fef3c7", borderRadius: 4, fontSize: 10, fontWeight: 600, color: nodeId.includes("wsl") ? "#1d4ed8" : "#92400e" }}>
                          {nodeId}
                        </span>
                        <span style={{ color: "#6b7280" }}>{(groupNames as string[]).join(", ")}</span>
                      </div>
                    ))}
                  </div>
                )}

                {configResult.wsl_command && (
                  <div style={{ backgroundColor: "#0f172a", borderRadius: "6px", padding: "12px", overflowX: "auto" }}>
                    <pre style={{ margin: 0, fontSize: "12px", color: configResult.alpha_mode === "multi" ? "#c4b5fd" : "#4ade80", fontFamily: "'Fira Code', Consolas, monospace", whiteSpace: "pre-wrap" }}>{configResult.wsl_command}</pre>
                  </div>
                )}
                <div style={{ marginTop: "12px", display: "flex", gap: "12px", flexWrap: "wrap" }}>
                  {configResult.wsl_command && (
                    <button onClick={() => { navigator.clipboard.writeText(configResult.wsl_command || ""); alert("已复制命令"); }}
                      style={{ ...btnSecondary, fontSize: "13px", borderColor: "#86efac", color: "#166534" }}>复制终端命令</button>
                  )}
                  {dispatchMode === "independent" && configResult.experiment_id && (
                    <>
                      <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
                        <span style={{ fontSize: 11, color: "#94a3b8", marginRight: 2 }}>引擎:</span>
                        <span style={{ padding: "3px 8px", fontSize: 10, borderRadius: 4, border: "1.5px solid #0ea5e9", background: "#f0f9ff", color: "#0284c7", fontWeight: 600 }}>统一执行层</span>
                      </div>
                      <select
                        data-testid="qe-execution-node"
                        value={executionNodeId}
                        onChange={e => setExecutionNodeId(e.target.value)}
                        disabled={sse.runStatus === "running" || sse.runStatus === "starting"}
                        style={{ padding: "7px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "12px", minWidth: 180 }}
                      >
                        <option value="">默认 WSL 节点</option>
                        {computeNodes.map((n: any) => (
                          <option key={n.node_id} value={n.node_id}>{n.display_name || n.node_id} ({n.status || "unknown"})</option>
                        ))}
                      </select>
                      <button
                        onClick={() => { if (configResult.experiment_id) sse.startRun(configResult.experiment_id, executionNodeId || undefined).catch((err) => { console.error("Failed to start experiment run:", err); alert("启动实验运行失败，请查看控制台日志"); }); }}
                        data-testid="qe-run-backtest"
                        disabled={sse.runStatus === "running" || sse.runStatus === "starting"}
                        style={{
                          ...btnPrimary,
                          backgroundColor: sse.runStatus === "running" || sse.runStatus === "starting" ? "#6b7280" : "#059669",
                          fontSize: "13px",
                          boxShadow: "0 2px 4px rgba(5, 150, 105, 0.2)",
                          opacity: sse.runStatus === "running" || sse.runStatus === "starting" ? 0.7 : 1,
                        }}
                      >
                        {sse.runStatus === "starting" ? "正在提交..." : sse.runStatus === "running" ? "执行中..." : "启动"}
                      </button>
                    </>
                  )}
                  {dispatchMode === "evolution" && (
                    <button onClick={() => window.open(`/quantevolver/evolution?base_experiment_id=${configResult?.experiment_id || ""}`, "_blank")}
                      style={{ ...btnPrimary, backgroundColor: "#10b981", fontSize: "13px", boxShadow: "0 2px 4px rgba(16, 185, 129, 0.2)" }}>前往演进监控大屏</button>
                  )}
                </div>
              </div>
            )}

            {/* 实时日志面板 */}
            {sse.runStatus && (
              <div style={{ marginBottom: "24px" }}>
                {/* 日志展开/收起按钮 */}
                <div
                  onClick={() => setLogsVisible(v => !v)}
                  style={{
                    display: "flex", justifyContent: "space-between", alignItems: "center",
                    padding: "8px 12px", borderRadius: logsVisible ? "8px 8px 0 0" : "8px",
                    background: "#1e293b", cursor: "pointer", userSelect: "none",
                  }}
                >
                  <span style={{ fontSize: 12, fontWeight: 600, color: "#94a3b8" }}>
                    {logsVisible ? "▼" : "▶"} 执行日志
                  </span>
                  <span style={{
                    fontSize: 11, padding: "2px 8px", borderRadius: 10, fontWeight: 600,
                    background: sse.runStatus === "completed" ? "#10b98120" : sse.runStatus === "failed" ? "#ef444420" : "#f59e0b20",
                    color: sse.runStatus === "completed" ? "#10b981" : sse.runStatus === "failed" ? "#ef4444" : "#f59e0b",
                  }}>
                    {sse.runStatus === "starting" ? "提交中" : sse.runStatus === "running" ? "执行中" : sse.runStatus === "completed" ? "已完成" : sse.runStatus === "failed" ? "失败" : sse.runStatus}
                  </span>
                </div>
                {logsVisible && (
                  <LogTerminal
                    logs={sse.runLogs}
                    logsEndRef={sse.logsEndRef}
                    maxHeight={300}
                    fontSize={12}
                    statusLabel={
                      sse.runStatus === "starting" ? "正在提交..." :
                      sse.runStatus === "running" ? "回测执行中" :
                      sse.runStatus === "completed" ? "执行完成" :
                      sse.runStatus === "failed" ? "执行失败" : sse.runStatus
                    }
                    statusColor={
                      sse.runStatus === "completed" ? "#10b981" :
                      sse.runStatus === "failed" ? "#ef4444" : "#f59e0b"
                    }
                    statusPulse={sse.runStatus === "running" || sse.runStatus === "starting"}
                  />
                )}

                {/* 多Alpha执行进度 */}
                {alphaMode === "multi" && configResult?.experiment_id && (sse.runStatus === "running" || sse.runStatus === "starting") && (
                  <div style={{ marginTop: 12 }}>
                    <MultiAlphaProgress experimentId={configResult.experiment_id} apiBase={API} />
                  </div>
                )}

                {/* 多Alpha完成后结果 */}
                {alphaMode === "multi" && configResult?.experiment_id && sse.runStatus === "completed" && (
                  <div style={{ marginTop: 12 }}>
                    <MultiAlphaResults experimentId={configResult.experiment_id} apiBase={API} />
                  </div>
                )}

                {/* 完成后展示核心指标 */}
                {sse.runStatus === "completed" && sse.runMetrics && (
                  <div style={{ marginTop: 12 }}>
                    <MetricsSummary metrics={sse.runMetrics} />
                  </div>
                )}

                {/* 完成后展示图表 */}
                {sse.runStatus === "completed" && sse.enhancedMetrics && (
                  <div style={{ marginTop: 16, display: "grid", gap: 16 }}>
                    {sse.enhancedMetrics.ic_series && (
                      <div style={{ background: "#fff", borderRadius: 8, padding: 16, border: "1px solid #e5e7eb" }}>
                        <h4 style={{ margin: "0 0 8px", fontSize: 14, fontWeight: 600, color: "#374151" }}>IC 时序诊断</h4>
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
                      <div style={{ background: "#fff", borderRadius: 8, padding: 16, border: "1px solid #e5e7eb" }}>
                        <h4 style={{ margin: "0 0 8px", fontSize: 14, fontWeight: 600, color: "#374151" }}>收益曲线</h4>
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
                      <div style={{ background: "#fff", borderRadius: 8, padding: 16, border: "1px solid #e5e7eb" }}>
                        <h4 style={{ margin: "0 0 8px", fontSize: 14, fontWeight: 600, color: "#374151" }}>训练过程</h4>
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

                {/* 实验完成后自动弹出演进创建建议 */}
                {sse.runStatus === "completed" && configResult?.experiment_id && dispatchMode === "independent" && alphaMode === "multi" && (
                  <div style={{ marginTop: 16, padding: 16, borderRadius: 8, backgroundColor: "#faf5ff", border: "1px solid #d8b4fe" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
                      <strong style={{ color: "#7c3aed", fontSize: 14 }}>多Alpha 实验已完成</strong>
                    </div>
                    <div style={{ display: "flex", gap: 8 }}>
                      <a
                        href={`/quantevolver/multi-alpha/diagnostics/${configResult.experiment_id}`}
                        style={{
                          display: "inline-block", padding: "8px 16px", backgroundColor: "#7c3aed", color: "#fff",
                          borderRadius: 6, fontSize: 13, fontWeight: 600, textDecoration: "none",
                          boxShadow: "0 2px 4px rgba(124, 58, 237, 0.2)",
                        }}
                      >
                        查看多Alpha诊断
                      </a>
                      <a
                        href={`/quantevolver/multi-alpha/evolve-wizard?source_exp=${configResult.experiment_id}`}
                        style={{
                          display: "inline-block", padding: "8px 16px", backgroundColor: "#fff", color: "#7c3aed",
                          borderRadius: 6, fontSize: 13, fontWeight: 600, textDecoration: "none",
                          border: "1px solid #8b5cf6",
                        }}
                      >
                        基于此实验演进
                      </a>
                    </div>
                  </div>
                )}
                {sse.runStatus === "completed" && configResult?.experiment_id && dispatchMode === "independent" && alphaMode !== "multi" && (
                  <div style={{ marginTop: 16, padding: 16, borderRadius: 8, backgroundColor: "#f0f9ff", border: "1px solid #bae6fd" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
                      <strong style={{ color: "#0369a1", fontSize: 14 }}>基线实验已完成，是否启动自动演进？</strong>
                    </div>
                    <p style={{ margin: "0 0 12px", fontSize: 13, color: "#475569" }}>
                      当前实验可作为演进起点，AI 将自动迭代优化因子组合和模型参数。
                    </p>
                    <a
                      href={`/quantevolver/evolution?base_experiment_id=${configResult.experiment_id}`}
                      style={{
                        display: "inline-block", padding: "8px 16px", backgroundColor: "#6d28d9", color: "#fff",
                        borderRadius: 6, fontSize: 13, fontWeight: 600, textDecoration: "none",
                        boxShadow: "0 2px 4px rgba(109, 40, 217, 0.2)",
                      }}
                    >
                      前往创建演进任务 (预填实验ID)
                    </a>
                  </div>
                )}
              </div>
            )}

            <div style={{ display: "flex", justifyContent: "space-between", marginTop: "24px", paddingTop: "20px", borderTop: "1px solid #f1f5f9" }}>
              <button onClick={() => setCurrentStep(4)} style={btnSecondary}>上一步</button>
              <button data-testid="qe-generate-config" onClick={generateConfig} disabled={actionLoading === "generate" || (alphaMode === "single" ? selectedFactors.size === 0 : !multiAlphaConfig)}
                style={{
                  ...btnPrimary,
                  padding: "12px 32px",
                  fontSize: "15px",
                  fontWeight: 700,
                  backgroundColor: isEditPendingExperiment ? "#059669" : alphaMode === "multi" ? "#7c3aed" : "#1e293b",
                  boxShadow: alphaMode === "multi" ? "0 4px 12px rgba(124, 58, 237, 0.25)" : "0 4px 12px rgba(0, 0, 0, 0.15)",
                  opacity: (actionLoading === "generate" || (alphaMode === "single" ? selectedFactors.size === 0 : !multiAlphaConfig)) ? 0.5 : 1,
                }}>
                {actionLoading === "generate" ? (isEditPendingExperiment ? "正在保存..." : "正在执行生成中...") : isEditPendingExperiment ? "保存待启动配置" : alphaMode === "multi" ? (dispatchMode === "independent" ? "执行多Alpha实验 (Multi-Alpha)" : "启动多Alpha演进 (Multi-Alpha Evolution)") : (dispatchMode === "independent" ? "创建待启动单次任务" : "启动 QE 自动演进 (Start Evolution)")}
              </button>
            </div>
          </div>
        )}
      </div>

      {/* 脉冲动画 CSS */}
      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.3; }
        }
      `}</style>
    </div>
  );
}

export default function ComposePage() {
  return (
    <Suspense fallback={<div style={{ padding: 24, color: "#475569" }}>Loading QE composer...</div>}>
      <ComposePageContent />
    </Suspense>
  );
}
