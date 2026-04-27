"use client";

import React, { useState, useEffect, useCallback, useRef, useMemo } from "react";
import {
  Play, Terminal, GitMerge, FileCode2,
  Activity, ArrowRight, DownloadCloud, CheckCircle2,
  AlertCircle, TrendingUp, BarChart3,
  Square, RotateCcw, Pause, XCircle, RefreshCw, Trash2, Copy
} from "lucide-react";
import dynamic from "next/dynamic";

import LogsPanel from "./components/LogsPanel";
import LogTerminal from "../components/LogTerminal";
import ParamSchemaForm from "./components/ParamSchemaForm";
import SectorBlacklistPanel from "../components/SectorBlacklistPanel";
import TopologyPanel from "./components/TopologyPanel";
import LoopDetailPanel, { getTaskStatusInfo } from "./components/LoopDetailPanel";
import type { Loop } from "./components/TopologyPanel";

const FactorList = dynamic(() => import("../components/FactorList"), { ssr: false });
const ModelList = dynamic(() => import("../components/ModelList"), { ssr: false });

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";
const TERMINAL_LOG_STATUSES = new Set([
  "completed",
  "failed",
  "cancelled",
  "canceled",
  "interrupted",
  "timeout",
  "paused",
  "stopped",
]);

// 模块级样式常量 — 避免每次渲染重新创建对象
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

interface Task {
  task_id: string;
  task_name: string;
  target_desc: string;
  max_loops: number;
  current_loop: number;
  status: string;
  base_experiment_id: string;
  source_type?: string;
  task_type?: string;  // 'evolution' | 'strategy_evo'
  created_at: string;
  updated_at: string;
  // Extended fields from SELECT *
  evolution_guidance?: string;
  evolution_mode?: string;
  strategy_id?: string;
  strategy_params?: Record<string, any>;
  execution_algo?: string;
  execution_algo_params?: Record<string, any>;
  unfilled_handler?: string;
  unfilled_handler_params?: Record<string, any>;
  label_type?: string;
  label_horizon?: number;
  stock_pool?: string;
  factor_blacklist?: string[];
}

export default function EvolutionDashboard() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [activeTaskId, setActiveTaskId] = useState<string | null>(null);
  const [loops, setLoops] = useState<Loop[]>([]);
  const [activeLoopIndex, setActiveLoopIndex] = useState<number | null>(null);

  const [showCreateTask, setShowCreateTask] = useState(false);
  const [newTask, setNewTask] = useState({
    task_name: "",
    target_desc: "",
    max_loops: 10,
    base_experiment_id: "",
    source_type: "qe_experiment" as "qe_experiment" | "rdagent_task_sota" | "evolution_fork" | "custom_evo",
    source_task_id: "",
    include_alpha_baseline: false,
    evolution_guidance: "",
    evolution_mode: "auto" as "auto" | "factor_only" | "model_only" | "joint",
    fork_from_task_id: "",
    fork_from_loop_index: -1,
    inherit_history: false,
    strategy_id: "",
    strategy_params: {} as Record<string, any>,
    execution_algo: "",
    execution_algo_params: {} as Record<string, any>,
    unfilled_handler: "",
    unfilled_handler_params: {} as Record<string, any>,
    enable_sector_hmm: false,
    hmm_model_version_id: "",
    hmm_signal_preset: "preset_A",
    node_id: "",
    label_horizon: 1 as 1 | 3 | 5 | 10 | 20,
    filter_suspended_on_signal: false,
    suspend_filter_strict: true,
  });
  const [isCreating, setIsCreating] = useState(false);

  // HMM 模型选择器状态
  const [hmmConfigs, setHmmConfigs] = useState<any[]>([]);
  const [hmmSnapshots, setHmmSnapshots] = useState<any[]>([]);
  const [hmmSelectedConfigId, setHmmSelectedConfigId] = useState("");
  const [blacklistEnabled, setBlacklistEnabled] = useState(false);
  const [stockPoolPath, setStockPoolPath] = useState<string | null>(null);
  const [sourceTasks, setSourceTasks] = useState<any[]>([]);
  const [forkSourceLoops, setForkSourceLoops] = useState<any[]>([]);
  const [sourceExperiments, setSourceExperiments] = useState<any[]>([]);
  const [sotaPreview, setSotaPreview] = useState<any>(null);
  const [strategyCatalog, setStrategyCatalog] = useState<any[]>([]);
  const [executionAlgoCatalog, setExecutionAlgoCatalog] = useState<any[]>([]);
  const qeExecutionAlgoCatalog = useMemo(() => executionAlgoCatalog.filter((a: any) => a.qe_supported !== false), [executionAlgoCatalog]);
  const renderQeExecutionAlgoOptions = (currentValue: string | undefined, emptyLabel: string) => (
    <>
      <option value="">{emptyLabel}</option>
      {currentValue && executionAlgoCatalog.find((a: any) => a.algo_code === currentValue)?.qe_supported === false && (
        <option value={currentValue} disabled>{currentValue} (QE unsupported)</option>
      )}
      {qeExecutionAlgoCatalog.map((a: any) => (
        <option key={a.algo_code} value={a.algo_code}>{a.algo_name || a.algo_code}</option>
      ))}
    </>
  );
  const ensureQeExecutionAlgoSupported = (algo: string | undefined, context: string) => {
    if (!algo) return true;
    const info = executionAlgoCatalog.find((a: any) => a.algo_code === algo);
    if (executionAlgoCatalog.length > 0 && !info) {
      alert(`${context}: execution algorithm ${algo} is not present in the backend catalog.`);
      return false;
    }
    if (info?.qe_supported === false) {
      alert(`${context}: execution algorithm ${algo} is not wired into QE/Qlib; refusing to silently fall back.`);
      return false;
    }
    return true;
  };
  const stripRuntimeStrategyFlags = (params?: Record<string, any>) => {
    const cleaned = { ...(params || {}) };
    delete cleaned.filter_suspended_on_signal;
    delete cleaned.exclude_suspended;
    delete cleaned.suspend_filter_strict;
    delete cleaned.suspend_filter_file;
    return cleaned;
  };
  const [computeNodes, setComputeNodes] = useState<any[]>([]);

  // 手动选择模型/因子相关状态
  type TaskAssetType = "factor" | "model" | "mixed" | "none";
  const [detectedTaskType, setDetectedTaskType] = useState<TaskAssetType>("none");
  const [showCustomizeFactors, setShowCustomizeFactors] = useState(false);
  const [showCustomizeModel, setShowCustomizeModel] = useState(false);
  const [selectedFactorsForEvo, setSelectedFactorsForEvo] = useState<Set<string>>(new Set());
  const [selectedModelForEvo, setSelectedModelForEvo] = useState<string>("");

  // 从因子库额外添加因子
  const [showFactorLibrary, setShowFactorLibrary] = useState(false);
  const [additionalFactorKeys, setAdditionalFactorKeys] = useState<Set<string>>(new Set());

  // ── 自定义演进 (custom_evo) 状态 ──
  type CustomEvoLoopConfig = {
    label: string;
    factor_keys: Set<string>;
    model_id: string;
    strategy_id: string;
    strategy_params: Record<string, any>;
    execution_algo: string;
    execution_algo_params: Record<string, any>;
    enable_sector_hmm: boolean;
    hmm_config_id: string;
    hmm_model_version_id: string;
    hmm_signal_preset: string;
    unfilled_handler: string;
    unfilled_handler_params: Record<string, any>;
    label_type: string;
    label_horizon: 1 | 3 | 5 | 10 | 20;
    data_split: Record<string, string> | null;
    blacklist_enabled: boolean;
    stock_pool: string;
    filter_suspended_on_signal: boolean;
    suspend_filter_strict: boolean;
    collapsed: boolean;
    // backtest-only 模式
    backtest_only: boolean;
    model_source_task_id: string;
    model_source_loop_index: number | null;
    _source_factor_keys: Set<string>; // 源模型的因子快照，用于变更检测
  };
  const makeDefaultCustomLoop = (): CustomEvoLoopConfig => ({
    label: "",
    factor_keys: new Set(),
    model_id: "",
    strategy_id: "",
    strategy_params: {},
    execution_algo: "",
    execution_algo_params: {},
    enable_sector_hmm: false,
    hmm_config_id: "",
    hmm_model_version_id: "",
    hmm_signal_preset: "preset_A",
    unfilled_handler: "",
    unfilled_handler_params: {},
    label_type: "",
    label_horizon: 1,
    data_split: null,
    blacklist_enabled: false,
    stock_pool: "",
    filter_suspended_on_signal: false,
    suspend_filter_strict: true,
    collapsed: false,
    backtest_only: false,
    model_source_task_id: "",
    model_source_loop_index: null,
    _source_factor_keys: new Set(),
  });
  const [customEvoLoops, setCustomEvoLoops] = useState<CustomEvoLoopConfig[]>([makeDefaultCustomLoop()]);
  const [customEvoExecutionMode, setCustomEvoExecutionMode] = useState<string>("serial");
  const [customEvoParallelism, setCustomEvoParallelism] = useState<number>(2);
  const [customEvoNodeId, setCustomEvoNodeId] = useState<string>("");
  const [customEvoInitSource, setCustomEvoInitSource] = useState<"manual" | "qe_experiment" | "evolution_loop" | "rdagent_task">("manual");
  const [customEvoFirstLoopReady, setCustomEvoFirstLoopReady] = useState(false);
  // 自定义演进 — 从来源加载配置的辅助状态
  const [customEvoSourceExpId, setCustomEvoSourceExpId] = useState("");
  const [customEvoSourceTaskId, setCustomEvoSourceTaskId] = useState("");
  const [customEvoSourceLoopIdx, setCustomEvoSourceLoopIdx] = useState(-1);
  const [customEvoForkLoops, setCustomEvoForkLoops] = useState<any[]>([]);
  const [customEvoHmmSnapshots, setCustomEvoHmmSnapshots] = useState<Record<number, any[]>>({});

  const updateCustomEvoLoop = (index: number, updates: Partial<CustomEvoLoopConfig>) => {
    setCustomEvoLoops(prev => {
      const next = [...prev];
      const cur = next[index];
      // 因子变更检测：如果修改了因子且当前是 backtest_only，自动关闭
      if (updates.factor_keys && cur.backtest_only && cur._source_factor_keys.size > 0) {
        const srcNames = new Set([...cur._source_factor_keys].map(k => k.split("||")[0]));
        const newNames = new Set([...updates.factor_keys].map(k => k.split("||")[0]));
        const changed = srcNames.size !== newNames.size || [...srcNames].some(n => !newNames.has(n));
        if (changed) {
          updates.backtest_only = false;
          setTimeout(() => alert("因子已变更，需要重新训练模型。已自动关闭 backtest-only 模式。"), 0);
        }
      }
      next[index] = { ...cur, ...updates };
      return next;
    });
  };
  const addCustomEvoLoop = () => {
    // 新 Loop 默认继承第一个 Loop 的全部配置
    const first = customEvoLoops[0];
    setCustomEvoLoops(prev => [...prev, {
      ...first,
      label: `Loop ${prev.length + 1}`,
      factor_keys: new Set(first.factor_keys),
      strategy_params: stripRuntimeStrategyFlags(first.strategy_params),
      execution_algo_params: { ...first.execution_algo_params },
      unfilled_handler_params: { ...first.unfilled_handler_params },
      data_split: first.data_split ? { ...first.data_split } : null,
      collapsed: true,
    }]);
  };
  const removeCustomEvoLoop = (index: number) => {
    if (index === 0) return; // 第一个不可删除
    setCustomEvoLoops(prev => prev.filter((_, i) => i !== index));
  };

  // 自定义演进 — 从来源加载配置到第一个 Loop
  // 根据因子名列表查询因子库，返回完整的 "name||source" key 集合
  const resolveFactorKeysFromNames = async (factorNames: string[]): Promise<{ keys: Set<string>; missing: string[] }> => {
    if (factorNames.length === 0) return { keys: new Set(), missing: [] };
    const res = await fetch(`${API}/quantevolver/factors?limit=2000`);
    if (!res.ok) throw new Error(`获取因子库失败 (HTTP ${res.status})`);
    const data = await res.json();
    const allFactors: any[] = data.items || data.data || [];
    // 按 factor_name 建索引（可能有同名多 source，取第一个）
    const nameToSource = new Map<string, string>();
    for (const f of allFactors) {
      if (f.factor_name && !nameToSource.has(f.factor_name)) {
        nameToSource.set(f.factor_name, f.source || "");
      }
    }
    const keys = new Set<string>();
    const missing: string[] = [];
    for (const name of factorNames) {
      const src = nameToSource.get(name);
      if (src !== undefined) {
        keys.add(`${name}||${src}`);
      } else {
        missing.push(name);
      }
    }
    return { keys, missing };
  };

  const loadCustomEvoFromExperiment = async (expId: string) => {
    try {
      const res = await fetch(`${API}/quantevolver/experiments/${expId}`);
      if (!res.ok) { alert(`加载实验失败 (HTTP ${res.status})`); return; }
      const data = await res.json();
      if (!data.ok) { alert(`加载实验失败: ${data.error || "未知错误"}`); return; }
      const exp = data.experiment;
      let factorNames: string[] = exp.factor_names || [];
      if (typeof factorNames === "string") { try { factorNames = JSON.parse(factorNames); } catch { factorNames = []; } }
      if (!Array.isArray(factorNames) || factorNames.length === 0) { alert("该实验没有因子数据"); return; }
      // 从因子库解析出真实的 source
      const { keys: factorKeys, missing } = await resolveFactorKeysFromNames(factorNames);
      if (missing.length > 0) {
        const missingList = missing.slice(0, 10).join("\n  ");
        const confirmMsg = `以下 ${missing.length} 个因子在因子库中未找到（可能已删除）：\n  ${missingList}${missing.length > 10 ? "\n  ..." : ""}\n\n已匹配 ${factorKeys.size}/${factorNames.length} 个因子。\n点击"确定"忽略缺失因子继续加载，点击"取消"放弃加载。`;
        if (!confirm(confirmMsg)) return;
      }
      if (factorKeys.size === 0) { alert("所有因子均未在因子库中找到，无法加载"); return; }
      // 记录 model_source 用于 backtest-only 模式
      const sourceTaskId = exp.qe_task_id || "";
      const sourceLoopIdx = exp.qe_loop_id ? parseInt((exp.qe_loop_id.match(/Loop(\d+)/) || [])[1] || "1") : 1;
      const expParams = typeof exp.custom_params === "string" ? JSON.parse(exp.custom_params || "{}") : (exp.custom_params || {});
      const expHorizon = ([1, 3, 5, 10, 20].includes(Number(expParams.label_horizon || 1)) ? Number(expParams.label_horizon || 1) : 1) as 1 | 3 | 5 | 10 | 20;
      updateCustomEvoLoop(0, {
        factor_keys: factorKeys,
        model_id: exp.model_id || "",
        strategy_id: exp.strategy_id || "",
        strategy_params: {},
        label_horizon: expHorizon,
        backtest_only: !!sourceTaskId,
        model_source_task_id: sourceTaskId,
        model_source_loop_index: sourceTaskId ? sourceLoopIdx : null,
        _source_factor_keys: new Set(factorKeys),
      });
      alert(`已加载实验配置：${factorKeys.size} 个因子，模型 ${exp.model_id || "无"}${sourceTaskId ? "（已启用 backtest-only）" : ""}`);
    } catch (e: any) {
      alert(`加载实验失败: ${e?.message || "网络错误"}`);
    }
  };
  const loadCustomEvoFromEvolutionLoop = async (taskId: string, loopIndex: number) => {
    try {
      const res = await fetch(`${API}/quantevolver/evolution/tasks/${taskId}`);
      if (!res.ok) { alert(`加载演进任务失败 (HTTP ${res.status})`); return; }
      const data = await res.json();
      const taskData = data.data || data;
      const loops = taskData.loops || [];
      const loop = loops.find((l: any) => l.loop_index === loopIndex);
      if (!loop || !loop.config_json) { alert(`Loop ${loopIndex} 没有配置数据`); return; }
      const config = typeof loop.config_json === "string" ? JSON.parse(loop.config_json) : loop.config_json;
      const configParams = config.model_params || {};
      const loopHorizon = ([1, 3, 5, 10, 20].includes(Number(configParams.label_horizon || config.label_horizon || 1))
        ? Number(configParams.label_horizon || config.label_horizon || 1)
        : 1) as 1 | 3 | 5 | 10 | 20;
      const factorList: string[] = config.factor_list || config.factor_names || [];
      if (factorList.length === 0) { alert(`Loop ${loopIndex} 没有因子数据`); return; }
      // 因子列表可能是纯名称或 "name||source"
      const hasSource = factorList.some((n: string) => n.includes("||"));
      let factorKeys: Set<string>;
      let missing: string[] = [];
      if (hasSource) {
        factorKeys = new Set(factorList.map((n: string) => n.includes("||") ? n : `${n}||`));
      } else {
        const resolved = await resolveFactorKeysFromNames(factorList);
        factorKeys = resolved.keys;
        missing = resolved.missing;
      }
      if (missing.length > 0) {
        const missingList = missing.slice(0, 10).join("\n  ");
        const confirmMsg = `以下 ${missing.length} 个因子在因子库中未找到（可能已删除）：\n  ${missingList}${missing.length > 10 ? "\n  ..." : ""}\n\n已匹配 ${factorKeys.size}/${factorList.length} 个因子。\n点击"确定"忽略缺失因子继续加载，点击"取消"放弃加载。`;
        if (!confirm(confirmMsg)) return;
      }
      if (factorKeys.size === 0) { alert("所有因子均未在因子库中找到，无法加载"); return; }
      updateCustomEvoLoop(0, {
        factor_keys: factorKeys,
        model_id: config.model_id || "",
        strategy_id: config.strategy_id || "",
        strategy_params: config.model_params || {},
        label_horizon: loopHorizon,
        backtest_only: true,
        model_source_task_id: taskId,
        model_source_loop_index: loopIndex,
        _source_factor_keys: new Set(factorKeys),
      });
      alert(`已加载 Loop ${loopIndex} 配置：${factorKeys.size} 个因子，模型 ${config.model_id || "无"}（已启用 backtest-only）`);
    } catch (e: any) {
      alert(`加载 Loop 配置失败: ${e?.message || "网络错误"}`);
    }
  };

  // 因子相关性分析
  const [corrPairs, setCorrPairs] = useState<Array<{factor_a: string; factor_b: string; correlation: number}>>([]);
  const [corrLoading, setCorrLoading] = useState(false);
  const [corrAnalyzed, setCorrAnalyzed] = useState(false);
  const [factorsToRemove, setFactorsToRemove] = useState<Set<string>>(new Set());

  // Stop/Resume 状态
  const [showResumeDialog, setShowResumeDialog] = useState<string | null>(null); // task_id or null
  const [additionalLoops, setAdditionalLoops] = useState(0);
  const [forceFullTrain, setForceFullTrain] = useState(false);

  // Fork 状态
  const [showForkDialog, setShowForkDialog] = useState<number | null>(null); // from_loop_index or null
  const [forkType, setForkType] = useState<"evolution" | "strategy_evo">("evolution"); // fork 类型
  const [forkForm, setForkForm] = useState({
    task_name: "",
    max_loops: 10,
    evolution_guidance: "",
    evolution_mode: "auto" as string,
    inherit_history: false,
    strategy_id: "",
    strategy_params: {} as Record<string, any>,
    execution_algo: "",
    execution_algo_params: {} as Record<string, any>,
    label_horizon: 1 as 1 | 3 | 5 | 10 | 20,
    filter_suspended_on_signal: false,
    suspend_filter_strict: true,
  });

  // 策略演进相关状态
  const [cloneFromTask, setCloneFromTask] = useState<Task | null>(null);
  const [factorsExpanded, setFactorsExpanded] = useState(false);
  const [strategyEvoLoops, setStrategyEvoLoops] = useState<any[]>([]);
  const [strategyEvoExecutionMode, setStrategyEvoExecutionMode] = useState<"serial" | "parallel">("serial");
  const [isForking, setIsForking] = useState(false);

  // Phase 3: 增强诊断状态
  const [enhancedMetrics, setEnhancedMetrics] = useState<any>(null);
  const [detailTab, setDetailTab] = useState<string>("overview");
  const [rightPanelView, setRightPanelView] = useState<"loop" | "trajectory">("loop");

  const [logs, setLogs] = useState<string[]>([
    "[System] 演进控制中心已启动...",
    "[System] 等待连接至 AIstock 演进调度引擎..."
  ]);
  const [logsCollapsed, setLogsCollapsed] = useState(false);

  // 后端日志面板
  const [backendLogsOpen, setBackendLogsOpen] = useState(false);
  const [backendLogs, setBackendLogs] = useState<string[]>([]);
  const [backendLogLevel, setBackendLogLevel] = useState("");
  const backendLogsEndRef = useRef<HTMLDivElement>(null);

  // 因子验证弹窗状态
  const [factorValidation, setFactorValidation] = useState<{
    taskId: string;
    validation: any;
  } | null>(null);
  const [resolving, setResolving] = useState(false);

  const eventSourceRef = useRef<EventSource | null>(null);
  const autoSelectLoopRef = useRef(true); // 是否需要自动选中 loop

  // ── 性能优化: SSE 日志节流 ──
  // 将高频 SSE 消息缓存到 ref，每 500ms 批量 flush 到 state
  const logBufferRef = useRef<string[]>([]);
  const flushTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const appendLogs = useCallback((newLines: string[]) => {
    logBufferRef.current.push(...newLines);
    if (!flushTimerRef.current) {
      flushTimerRef.current = setTimeout(() => {
        flushTimerRef.current = null;
        const buffered = logBufferRef.current.splice(0);
        if (buffered.length > 0) {
          setLogs(prev => [...prev, ...buffered].slice(-200));
        }
      }, 500);
    }
  }, []);

  // cleanup flush timer
  useEffect(() => {
    return () => {
      if (flushTimerRef.current) {
        clearTimeout(flushTimerRef.current);
        flushTimerRef.current = null;
      }
    };
  }, []);

  // 获取任务列表
  const fetchTasks = useCallback(async () => {
    try {
      const res = await fetch(`${API}/quantevolver/evolution/tasks`);
      const data = await res.json();
      if (data.status === "success" && data.data) {
        setTasks(data.data);
      }
    } catch (e) {
      console.error("Failed to fetch tasks:", e);
    }
  }, []);

  // 动态轮询: 有 running 任务 10s，否则 60s
  const pollIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const hasRunningTask = useMemo(() => tasks.some(t => t.status === "running"), [tasks]);

  useEffect(() => {
    fetchTasks();
  }, [fetchTasks]);

  useEffect(() => {
    if (pollIntervalRef.current) clearInterval(pollIntervalRef.current);
    const delay = hasRunningTask ? 10_000 : 60_000;
    pollIntervalRef.current = setInterval(fetchTasks, delay);
    return () => { if (pollIntervalRef.current) clearInterval(pollIntervalRef.current); };
  }, [hasRunningTask, fetchTasks]);

  useEffect(() => {
    fetch(`${API}/quantevolver/strategies?limit=100`)
      .then(r => r.json())
      .then(d => { if (d.ok) setStrategyCatalog(d.items || []); })
      .catch(() => {});
    fetch(`${API}/quantevolver/execution-algorithms`)
      .then(r => r.json())
      .then(d => { if (d.ok) setExecutionAlgoCatalog(d.items || []); })
      .catch(() => {});
    fetch(`${API}/dispatch/nodes`)
      .then(r => r.json())
      .then(d => { if (Array.isArray(d)) setComputeNodes(d); else if (d.data) setComputeNodes(d.data); })
      .catch(() => {});
  }, []);

  // Auto-fill from URL params (when navigating from compose page)
  useEffect(() => {
    if (typeof window !== "undefined") {
      const params = new URLSearchParams(window.location.search);
      const baseExpId = params.get("base_experiment_id");
      if (baseExpId) {
        setNewTask(prev => ({ ...prev, base_experiment_id: baseExpId, source_type: "qe_experiment" }));
        setShowCreateTask(true);
        fetchSourceExperiments();
        // Clean up URL
        window.history.replaceState({}, "", window.location.pathname);
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // 从 cloneFromTask 预填充新建任务弹窗
  useEffect(() => {
    if (showCreateTask && cloneFromTask) {
      const sp: Record<string, any> = cloneFromTask.strategy_params || {};
      setNewTask({
        task_name: cloneFromTask.task_name + "_副本",
        target_desc: cloneFromTask.target_desc || "",
        max_loops: cloneFromTask.max_loops,
        base_experiment_id: cloneFromTask.base_experiment_id || "",
        source_type: "qe_experiment",
        source_task_id: "",
        include_alpha_baseline: false,
        evolution_guidance: (cloneFromTask as any).evolution_guidance || "",
        evolution_mode: (cloneFromTask as any).evolution_mode || "auto",
        fork_from_task_id: "",
        fork_from_loop_index: -1,
        inherit_history: false,
        strategy_id: cloneFromTask.strategy_id || "",
        strategy_params: stripRuntimeStrategyFlags(cloneFromTask.strategy_params),
        execution_algo: (cloneFromTask as any).execution_algo || "",
        execution_algo_params: (cloneFromTask as any).execution_algo_params || {},
        unfilled_handler: (cloneFromTask as any).unfilled_handler || "",
        unfilled_handler_params: (cloneFromTask as any).unfilled_handler_params || {},
        enable_sector_hmm: !!sp.enable_sector_hmm,
        hmm_model_version_id: sp.hmm_model_version_id || "",
        hmm_signal_preset: sp.hmm_signal_preset || "preset_A",
        node_id: "",
        label_horizon: ([1, 3, 5, 10, 20].includes(Number((cloneFromTask as any).label_horizon || 1))
          ? Number((cloneFromTask as any).label_horizon || 1)
          : 1) as 1 | 3 | 5 | 10 | 20,
        filter_suspended_on_signal: !!sp.filter_suspended_on_signal,
        suspend_filter_strict: sp.suspend_filter_strict !== false,
      });
      if ((cloneFromTask as any).stock_pool) {
        setBlacklistEnabled(true);
        setStockPoolPath((cloneFromTask as any).stock_pool);
      }
      setCloneFromTask(null);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [showCreateTask, cloneFromTask]);

  // Auto-fill from URL params (when navigating from RDAgent Tasks page)
  useEffect(() => {
    if (typeof window !== "undefined") {
      const params = new URLSearchParams(window.location.search);
      const sourceTaskId = params.get("source_task_id");
      if (sourceTaskId) {
        setNewTask(prev => ({
          ...prev,
          source_type: "rdagent_task_sota",
          source_task_id: sourceTaskId,
        }));
        setShowCreateTask(true);
        fetchSourceTasks();
        fetchSotaPreview(sourceTaskId);
        window.history.replaceState({}, "", window.location.pathname);
      }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // 获取任务详情和 Loops
  const fetchTaskDetail = useCallback(async (taskId: string) => {
    try {
      const res = await fetch(`${API}/quantevolver/evolution/tasks/${taskId}`);
      const data = await res.json();
      if (data.status === "success" && data.data) {
        setLoops(data.data.loops || []);
        // 仅在需要自动选中时（首次加载/切换任务后）选中 loop
        if (autoSelectLoopRef.current && data.data.loops && data.data.loops.length > 0) {
          const sotaLoop = data.data.loops.find((l: Loop) => l.is_sota);
          if (sotaLoop) setActiveLoopIndex(sotaLoop.loop_index);
          else setActiveLoopIndex(data.data.loops[data.data.loops.length - 1].loop_index);
          autoSelectLoopRef.current = false;
        }
      }
    } catch (e) {
      console.error("Failed to fetch task detail:", e);
    }
  }, []); // 无状态依赖，避免无限循环

  // 监听 Task 选中切换（仅依赖 activeTaskId，避免无限循环）
  const activeTaskIdRef = useRef<string | null>(null);
  useEffect(() => {
    if (activeTaskId) {
      activeTaskIdRef.current = activeTaskId;
      autoSelectLoopRef.current = true; // 切换任务时允许自动选中 loop
      setActiveLoopIndex(null);
      fetchTaskDetail(activeTaskId);

      const selectedTask = tasks.find(t => t.task_id === activeTaskId);
      const selectedStatus = selectedTask?.status?.toLowerCase();

      // 连接 SSE 日志流
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }

      if (selectedStatus && TERMINAL_LOG_STATUSES.has(selectedStatus)) {
        let cancelled = false;
        appendLogs([`[System] 任务 ${activeTaskId} 已是终态(${selectedStatus})，只读取本地日志尾部，不打开实时日志流...`]);
        fetch(`${API}/quantevolver/evolution/tasks/${activeTaskId}/logs/tail?tail=200`)
          .then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
          .then(data => {
            if (cancelled) return;
            const payload = data?.data || data;
            const logLines = Array.isArray(payload?.logs) ? payload.logs : [];
            appendLogs(logLines.length ? logLines : ["[System] 未找到本地 evolution.log 尾部内容"]);
            appendLogs([`[System] 任务终态: ${payload?.task_status || selectedStatus}，未连接 RDAgent 实时日志流`]);
          })
          .catch(e => {
            if (!cancelled) appendLogs([`[Error] 读取本地日志尾部失败: ${e?.message || e}`]);
          });
        const detailInterval = setInterval(() => fetchTaskDetail(activeTaskId), 60000);
        return () => {
          cancelled = true;
          activeTaskIdRef.current = null;
          clearInterval(detailInterval);
        };
      }

      appendLogs([`[System] 已连接到任务 ${activeTaskId} 的实时日志流...`]);

      let reconnectCount = 0;
      const MAX_RECONNECT = 200;
      const RECONNECT_DELAY = 3000;
      const boundTaskId = activeTaskId; // 捕获当前 taskId，防止闭包串扰

      function createSSE(taskId: string) {
        const sse = new EventSource(`${API}/quantevolver/evolution/tasks/${taskId}/logs`);
        sse.onmessage = (event) => {
          try {
            const data = JSON.parse(event.data);
            const terminalLogEvent =
              data.status === "deleted" ||
              data.status === "missing" ||
              data.event === "task_deleted" ||
              data.event === "task_log_workspace_missing" ||
              data.event === "task_log_terminal";

            if (terminalLogEvent) {
              const logLines = data.logs ? (Array.isArray(data.logs) ? data.logs : [String(data.logs)]) : [];
              appendLogs(logLines.length ? logLines : ["[System] 日志流已关闭"]);
              sse.close();
              if (eventSourceRef.current === sse) eventSourceRef.current = null;
              if (activeTaskIdRef.current === boundTaskId) {
                activeTaskIdRef.current = null;
                setActiveTaskId(null);
                setLoops([]);
                fetchTasks();
              }
              return;
            }

            reconnectCount = 0; // 收到有效消息才重置重连计数

            // 处理日志消息
            if (data.logs) {
              const logLines = Array.isArray(data.logs) ? data.logs : [String(data.logs)];
              appendLogs(logLines);
            }

            // 检测状态变化事件 → 自动刷新任务和 Loop 列表
            if (data.status === "completed" || data.status === "failed" ||
                data.event === "loop_completed" || data.event === "task_completed" ||
                data.event === "loop_started") {
              fetchTasks();
              fetchTaskDetail(taskId);
              if (data.event === "loop_completed") {
                appendLogs([`[System] Loop ${data.loop_index ?? ""} 已完成，正在刷新数据...`]);
              }
              if (data.event === "task_completed" || data.status === "completed") {
                appendLogs(["[System] 演进任务已完成！"]);
              }
            }

            // 处理进度信息
            if (data.progress) {
              appendLogs([`[Progress] Loop ${data.progress.current_loop}/${data.progress.max_loops} - ${data.progress.phase || ""}`]);
            }
          } catch(e) {
            // 纯文本消息
            appendLogs([event.data]);
          }
        };

        sse.onerror = async () => {
          sse.close();
          if (activeTaskIdRef.current !== boundTaskId) return;
          try {
            const taskRes = await fetch(`${API}/quantevolver/evolution/tasks/${taskId}`);
            if (taskRes.status === 404 || taskRes.status === 204) {
              appendLogs([`[System] 任务 ${taskId} 已不存在，停止日志流重连`]);
              if (eventSourceRef.current === sse) eventSourceRef.current = null;
              activeTaskIdRef.current = null;
              setActiveTaskId(null);
              setLoops([]);
              fetchTasks();
              return;
            }
          } catch {
            // Network errors still use the bounded reconnect path below.
          }
          if (reconnectCount < MAX_RECONNECT && activeTaskIdRef.current === boundTaskId) {
            reconnectCount++;
            setTimeout(() => {
              // 双重检查：确保 task 没被切换
              if (activeTaskIdRef.current !== boundTaskId) return;
              const newSse = createSSE(taskId);
              eventSourceRef.current = newSse;
            }, RECONNECT_DELAY);
          } else {
            appendLogs(["[Error] 日志流重连次数已达上限，请点击任务列表重新选择该任务"]);
          }
        };

        return sse;
      }

      const sse = createSSE(activeTaskId);

      eventSourceRef.current = sse;

      // 定时刷新任务详情（更新 loop 列表），但不重建 SSE
      // 仅当选中任务为 running 时才定时刷新，否则依赖手动刷新
      const detailDelay = selectedTask?.status === "running" ? 15000 : 60000;
      const detailInterval = setInterval(() => fetchTaskDetail(activeTaskId), detailDelay);
      return () => {
        activeTaskIdRef.current = null; // 标记已清理，阻止旧 reconnect timer
        clearInterval(detailInterval);
        if (eventSourceRef.current) {
          eventSourceRef.current.close();
          eventSourceRef.current = null;
        }
      };
    }

    return () => {
      activeTaskIdRef.current = null;
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTaskId]);

  // 获取可用的 RDAgent source tasks
  const fetchSourceTasks = useCallback(async () => {
    try {
      const res = await fetch(`${API}/quantevolver/evolution/source-tasks`);
      const data = await res.json();
      if (data.status === "success") setSourceTasks(data.data || []);
    } catch (e) {
      console.error("Failed to fetch source tasks:", e);
    }
  }, []);

  // 获取已完成的 QE 实验列表
  const fetchSourceExperiments = useCallback(async () => {
    try {
      const res = await fetch(`${API}/quantevolver/evolution/source-experiments`);
      const data = await res.json();
      if (data.status === "success") setSourceExperiments(data.data || []);
    } catch (e) {
      console.error("Failed to fetch source experiments:", e);
    }
  }, []);

  // 预览 SOTA 资产
  const fetchSotaPreview = useCallback(async (taskId: string) => {
    if (!taskId) { setSotaPreview(null); setDetectedTaskType("none"); return; }
    try {
      const res = await fetch(`${API}/quantevolver/evolution/source-tasks/${taskId}/preview?include_alpha=${newTask.include_alpha_baseline}`);
      const data = await res.json();
      if (data.status === "success") {
        setSotaPreview(data.data);
        // 检测 Task 类型
        const hasFac = (data.data.total_sota_factors || 0) > 0;
        const hasMod = (data.data.total_sota_models || 0) > 0;
        if (hasFac && !hasMod) setDetectedTaskType("factor");
        else if (hasMod && !hasFac) setDetectedTaskType("model");
        else if (hasFac && hasMod) setDetectedTaskType("mixed");
        else setDetectedTaskType("none");
      }
    } catch (e) {
      console.error("Failed to fetch SOTA preview:", e);
    }
  }, [newTask.include_alpha_baseline]);

  // ── HMM 模型选择器数据获取 ──
  const fetchHmmConfigs = useCallback(async () => {
    try {
      const res = await fetch(`${API}/hmm-training/configs?model_type=sector_hmm`);
      if (res.ok) {
        const data = await res.json();
        setHmmConfigs(Array.isArray(data) ? data : []);
      }
    } catch (e) { console.error("Failed to fetch HMM configs:", e); }
  }, []);

  const fetchHmmSnapshots = useCallback(async (configId: string) => {
    if (!configId) { setHmmSnapshots([]); return; }
    try {
      const res = await fetch(`${API}/hmm-training/configs/${configId}/snapshots`);
      if (res.ok) {
        const data = await res.json();
        // Only show completed snapshots
        setHmmSnapshots((Array.isArray(data) ? data : []).filter((s: any) => s.status === "completed"));
      }
    } catch (e) { console.error("Failed to fetch HMM snapshots:", e); }
  }, []);

  const fetchHmmSnapshotsForLoop = useCallback(async (loopIdx: number, configId: string) => {
    if (!configId) { setCustomEvoHmmSnapshots(prev => ({ ...prev, [loopIdx]: [] })); return; }
    try {
      const res = await fetch(`${API}/hmm-training/configs/${configId}/snapshots`);
      if (res.ok) {
        const data = await res.json();
        setCustomEvoHmmSnapshots(prev => ({
          ...prev,
          [loopIdx]: (Array.isArray(data) ? data : []).filter((s: any) => s.status === "completed"),
        }));
      }
    } catch (e) { console.error("Failed to fetch HMM snapshots for loop:", e); }
  }, []);

  // 提交新建任务
  const handleCreateTask = async () => {
    // custom_evo 有自己的验证逻辑
    if (newTask.source_type === "custom_evo") {
      if (!newTask.task_name) { alert("请填写任务名称"); return; }
      // 直接跳到提交逻辑（验证在下方 custom_evo 分支中）
    } else {
    if (!newTask.task_name || !newTask.target_desc) {
      alert("请填写任务名称和目标描述");
      return;
    }
    if (newTask.source_type === "qe_experiment" && !newTask.base_experiment_id) {
      alert("请填写基础实验 ID");
      return;
    }
    if (newTask.source_type === "rdagent_task_sota" && !newTask.source_task_id) {
      alert("请选择 RDAgent Task");
      return;
    }
    if (newTask.source_type === "evolution_fork") {
      if (!newTask.fork_from_task_id) { alert("请选择源演进任务"); return; }
      if (newTask.fork_from_loop_index < 0) { alert("请选择从哪个 Loop 开始"); return; }
    }
    if (!newTask.evolution_guidance) {
      alert("请填写演进指引");
      return;
    }

    // RDAgent Task 模型/因子验证
    if (newTask.source_type === "rdagent_task_sota" && sotaPreview) {
      // 检查是否所有因子都被移除
      const totalSotaFactors = sotaPreview.sota_factors?.length || 0;
      if (totalSotaFactors > 0 && factorsToRemove.size >= totalSotaFactors && selectedFactorsForEvo.size === 0) {
        alert("至少保留一个因子");
        return;
      }
      // Factor Task: 必须选模型
      if (detectedTaskType === "factor" && !selectedModelForEvo) {
        alert("Factor Task 必须选择一个模型后才能创建任务");
        return;
      }
      // Model Task: 如无演进因子，则不能无因子提交
      if (detectedTaskType === "model" && selectedFactorsForEvo.size === 0) {
        const hasEvolvedFactors = (sotaPreview?.total_task_factors || 0) > 0;
        if (!hasEvolvedFactors) {
          alert("此 Task 没有 SOTA 因子，请至少选择一个因子后再创建任务");
          return;
        }
      }
      // Mixed Task: 不强制，但若开启了自定义面板且为空则提醒
      if (detectedTaskType === "mixed") {
        if (showCustomizeFactors && selectedFactorsForEvo.size === 0) {
          alert("已开启因子自定义但未选择任何因子，请选择因子或取消自定义");
          return;
        }
      }
    }
    } // end of non-custom_evo validation

    if (newTask.source_type !== "custom_evo" && !ensureQeExecutionAlgoSupported(newTask.execution_algo, "new task")) {
      return;
    }

    setIsCreating(true);
    try {
      // ── evolution_fork: 走独立的 fork API ──
      if (newTask.source_type === "evolution_fork") {
        const res = await fetch(`${API}/quantevolver/evolution/tasks/${newTask.fork_from_task_id}/fork`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            from_loop_index: newTask.fork_from_loop_index,
            task_name: newTask.task_name,
            max_loops: newTask.max_loops,
            evolution_guidance: newTask.evolution_guidance || undefined,
            evolution_mode: newTask.evolution_mode,
            inherit_history: newTask.inherit_history,
            strategy_id: newTask.strategy_id || undefined,
            strategy_params: Object.keys(newTask.strategy_params).length > 0 ? newTask.strategy_params : undefined,
            execution_algo: newTask.execution_algo || undefined,
            execution_algo_params: Object.keys(newTask.execution_algo_params).length > 0 ? newTask.execution_algo_params : undefined,
            filter_suspended_on_signal: !!newTask.filter_suspended_on_signal,
            suspend_filter_strict: newTask.suspend_filter_strict !== false,
            unfilled_handler: newTask.unfilled_handler || undefined,
            unfilled_handler_params:
              newTask.unfilled_handler && Object.keys(newTask.unfilled_handler_params || {}).length > 0
                ? newTask.unfilled_handler_params
                : undefined,
            additional_factor_keys: additionalFactorKeys.size > 0 ? Array.from(additionalFactorKeys) : undefined,
            label_horizon: newTask.label_horizon,
          }),
        });
        if (!res.ok) {
          const errText = await res.text();
          alert(`创建失败 (HTTP ${res.status}): ${errText}`);
          setIsCreating(false);
          return; // 不清空 forkSourceLoops，允许用户重试
        }
        const data = await res.json();
        if (data.status === "success") {
          alert(`已从 Loop ${newTask.fork_from_loop_index} 创建新演进任务！`);
          setShowCreateTask(false);
          setNewTask({ task_name: "", target_desc: "", max_loops: 10, base_experiment_id: "", source_type: "qe_experiment", source_task_id: "", include_alpha_baseline: false, evolution_guidance: "", evolution_mode: "auto", fork_from_task_id: "", fork_from_loop_index: -1, inherit_history: false, strategy_id: "", strategy_params: {}, execution_algo: "", execution_algo_params: {}, unfilled_handler: "", unfilled_handler_params: {}, enable_sector_hmm: false, hmm_model_version_id: "", hmm_signal_preset: "preset_A", node_id: "", label_horizon: 1, filter_suspended_on_signal: false, suspend_filter_strict: true });
          setForkSourceLoops([]);
          setShowFactorLibrary(false);
          setAdditionalFactorKeys(new Set());
          fetchTasks();
          setTimeout(() => setActiveTaskId(data.task_id), 500);
        } else {
          alert("创建失败: " + (data.detail || "未知错误"));
          // 不清空 forkSourceLoops，允许用户修改后重试
        }
        setIsCreating(false);
        return;
      }

      // ── custom_evo: 走独立的自定义演进 API ──
      if (newTask.source_type === "custom_evo") {
        // 验证
        for (let i = 0; i < customEvoLoops.length; i++) {
          const loop = customEvoLoops[i];
          if (loop.factor_keys.size === 0) { alert(`Loop ${i + 1} 必须选择至少一个因子`); setIsCreating(false); return; }
          if (!loop.model_id) { alert(`Loop ${i + 1} 必须选择一个模型`); setIsCreating(false); return; }
          if (!ensureQeExecutionAlgoSupported(loop.execution_algo, `custom loop ${i + 1}`)) { setIsCreating(false); return; }
        }
        const loopsPayload = customEvoLoops.map((loop, i) => ({
          label: loop.label || `Loop ${i + 1}`,
          loop_index: i + 1,
          factor_keys: Array.from(loop.factor_keys),
          model_id: loop.model_id,
          strategy_id: loop.strategy_id || undefined,
          strategy_params: Object.keys(loop.strategy_params).length > 0 ? loop.strategy_params : undefined,
          execution_algo: loop.execution_algo || undefined,
          execution_algo_params: Object.keys(loop.execution_algo_params).length > 0 ? loop.execution_algo_params : undefined,
          filter_suspended_on_signal: !!loop.filter_suspended_on_signal,
          suspend_filter_strict: loop.suspend_filter_strict !== false,
          enable_sector_hmm: loop.enable_sector_hmm,
          hmm_model_version_id: loop.enable_sector_hmm ? (loop.hmm_model_version_id || undefined) : undefined,
          hmm_signal_preset: loop.enable_sector_hmm ? (loop.hmm_signal_preset || undefined) : undefined,
          unfilled_handler: loop.unfilled_handler || undefined,
          unfilled_handler_params: loop.unfilled_handler && Object.keys(loop.unfilled_handler_params).length > 0 ? loop.unfilled_handler_params : undefined,
          stock_pool: loop.stock_pool || undefined,
          label_type: loop.label_type || undefined,
          label_horizon: loop.label_horizon,
          data_split: loop.data_split || undefined,
          backtest_only: loop.backtest_only,
          model_source_task_id: loop.backtest_only ? (loop.model_source_task_id || undefined) : undefined,
          model_source_loop_index: loop.backtest_only ? (loop.model_source_loop_index ?? undefined) : undefined,
        }));
        const execMode = customEvoExecutionMode === "parallel" ? `parallel_${customEvoParallelism}` : "serial";
        const res = await fetch(`${API}/quantevolver/evolution/custom-tasks`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            task_name: newTask.task_name,
            target_desc: newTask.target_desc || "自定义演进任务",
            loops: loopsPayload,
            execution_mode: execMode,
            node_id: customEvoNodeId || undefined,
            engine_mode: "unified",
          }),
        });
        if (!res.ok) {
          const errText = await res.text();
          alert(`创建失败 (HTTP ${res.status}): ${errText}`);
          setIsCreating(false);
          return;
        }
        const data = await res.json();
        if (data.status === "success") {
          alert(`自定义演进任务创建成功！共 ${data.total_loops} 个 Loop`);
          setShowCreateTask(false);
          setCustomEvoLoops([makeDefaultCustomLoop()]);
          setCustomEvoFirstLoopReady(false);
          fetchTasks();
          setTimeout(() => setActiveTaskId(data.task_id), 500);
        } else {
          alert("创建失败: " + (data.detail || "未知错误"));
        }
        setIsCreating(false);
        return;
      }

      // 构建 factor_keys：考虑因子表格 checkbox 移除
      let factorKeys: string[] | undefined;
      if (selectedFactorsForEvo.size > 0) {
        // 用户通过 FactorList 组件手动选了（Model/Mixed 自定义模式）
        factorKeys = Array.from(selectedFactorsForEvo)
          .filter(k => !factorsToRemove.has(k.split("||")[0]));
        if (factorKeys.length === 0) {
          alert("所有选中的因子都被移除了，至少保留一个因子");
          setIsCreating(false);
          return;
        }
      } else if (factorsToRemove.size > 0) {
        // 用了默认 SOTA 因子但移除了部分
        const checkedFactors = (sotaPreview?.sota_factors || [])
          .filter((f: any) => !factorsToRemove.has(f.factor_name));
        factorKeys = checkedFactors.map((f: any) => `${f.factor_name}||${f.source}`);
      }
      // factorsToRemove 为空且 selectedFactorsForEvo 为空时不传 selected_factor_keys，后端使用全部 SOTA 因子

      const submitData = {
        ...newTask,
        node_id: newTask.node_id || undefined,
        selected_model_id: selectedModelForEvo || undefined,
        selected_factor_keys: factorKeys,
        additional_factor_keys: additionalFactorKeys.size > 0 ? Array.from(additionalFactorKeys) : undefined,
        ...(blacklistEnabled && stockPoolPath ? { stock_pool: stockPoolPath } : {}),
        strategy_id: newTask.strategy_id || undefined,
        strategy_params: Object.keys(newTask.strategy_params).length > 0 ? newTask.strategy_params : undefined,
        execution_algo: newTask.execution_algo || undefined,
        execution_algo_params: Object.keys(newTask.execution_algo_params).length > 0 ? newTask.execution_algo_params : undefined,
        filter_suspended_on_signal: !!newTask.filter_suspended_on_signal,
        suspend_filter_strict: newTask.suspend_filter_strict !== false,
        unfilled_handler: newTask.unfilled_handler || undefined,
        unfilled_handler_params:
          newTask.unfilled_handler && Object.keys(newTask.unfilled_handler_params || {}).length > 0
            ? newTask.unfilled_handler_params
            : undefined,
        enable_sector_hmm: newTask.enable_sector_hmm || undefined,
        hmm_model_version_id: newTask.enable_sector_hmm ? (newTask.hmm_model_version_id || undefined) : undefined,
        hmm_signal_preset: newTask.enable_sector_hmm ? (newTask.hmm_signal_preset || undefined) : undefined,
        label_horizon: newTask.label_horizon,
      };
      const res = await fetch(`${API}/quantevolver/evolution/tasks`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(submitData)
      });
      if (!res.ok) {
        let errDetail: string;
        try {
          const errData = await res.json();
          errDetail = errData.detail || JSON.stringify(errData);
        } catch {
          errDetail = await res.text();
        }
        alert(`创建任务失败 (HTTP ${res.status}): ${errDetail}`);
        setIsCreating(false);
        return;
      }
      const data = await res.json();
      if (data.status === "success") {
        if (data.factor_validation) {
          // 有因子验证问题 — 弹出处理弹窗
          setFactorValidation({ taskId: data.task_id, validation: data.factor_validation });
          setShowCreateTask(false);
          setNewTask({ task_name: "", target_desc: "", max_loops: 10, base_experiment_id: "", source_type: "qe_experiment", source_task_id: "", include_alpha_baseline: false, evolution_guidance: "", evolution_mode: "auto", fork_from_task_id: "", fork_from_loop_index: -1, inherit_history: false, strategy_id: "", strategy_params: {}, execution_algo: "", execution_algo_params: {}, unfilled_handler: "", unfilled_handler_params: {}, enable_sector_hmm: false, hmm_model_version_id: "", hmm_signal_preset: "preset_A", node_id: "", label_horizon: 1, filter_suspended_on_signal: false, suspend_filter_strict: true });
          setSotaPreview(null);
          setDetectedTaskType("none");
          setSelectedFactorsForEvo(new Set());
          setSelectedModelForEvo("");
          setShowCustomizeFactors(false);
          setShowCustomizeModel(false);
          setShowFactorLibrary(false);
          setAdditionalFactorKeys(new Set());
          setCorrPairs([]); setCorrAnalyzed(false); setCorrLoading(false); setFactorsToRemove(new Set());
          fetchTasks();
          setActiveTaskId(data.task_id);
        } else {
          alert("演进任务创建成功并已在后台启动！");
          setShowCreateTask(false);
          setNewTask({ task_name: "", target_desc: "", max_loops: 10, base_experiment_id: "", source_type: "qe_experiment", source_task_id: "", include_alpha_baseline: false, evolution_guidance: "", evolution_mode: "auto", fork_from_task_id: "", fork_from_loop_index: -1, inherit_history: false, strategy_id: "", strategy_params: {}, execution_algo: "", execution_algo_params: {}, unfilled_handler: "", unfilled_handler_params: {}, enable_sector_hmm: false, hmm_model_version_id: "", hmm_signal_preset: "preset_A", node_id: "", label_horizon: 1, filter_suspended_on_signal: false, suspend_filter_strict: true });
          setSotaPreview(null);
          setDetectedTaskType("none");
          setSelectedFactorsForEvo(new Set());
          setSelectedModelForEvo("");
          setShowCustomizeFactors(false);
          setShowCustomizeModel(false);
          setShowFactorLibrary(false);
          setAdditionalFactorKeys(new Set());
          setCorrPairs([]); setCorrAnalyzed(false); setCorrLoading(false); setFactorsToRemove(new Set());
          fetchTasks();
          setActiveTaskId(data.task_id);
        }
      } else {
        alert("创建任务失败: " + (data.detail || "未知错误"));
      }
    } catch (e: any) {
      alert("创建任务失败: " + (e?.message || "网络错误"));
    } finally {
      setIsCreating(false);
    }
  };

  // 停止任务
  const handleStopTask = async (taskId: string, e: React.MouseEvent) => {
    e.stopPropagation();
    if (!confirm("确定要停止此演进任务吗？当前运行中的 Loop 会被终止，后续未运行的 Loop 也会被一并停止。")) return;
    try {
      const res = await fetch(`${API}/quantevolver/evolution/tasks/${taskId}/stop`, { method: "POST" });
      const data = await res.json();
      if (data.status === "success" || data.status === "warning") {
        const detail = data.detail || {};
        const killed = detail.loops_killed?.length ?? 0;
        const cancelled = detail.loops_cancelled?.length ?? 0;
        appendLogs([`[System] 任务 ${taskId} 已停止：终止/检查 ${killed} 个 Loop，标记停止 ${cancelled} 个未完成 Loop`]);
        if (data.status === "warning") appendLogs([`[Warning] ${data.message}`]);
        fetchTasks();
      } else {
        alert("停止失败: " + (data.detail || "未知错误"));
      }
    } catch (err: any) {
      alert("停止失败: " + (err?.message || "网络错误"));
    }
  };

  // 恢复任务
  const handleResumeTask = async () => {
    if (!showResumeDialog) return;
    const resumingTaskId = showResumeDialog;
    try {
      const res = await fetch(`${API}/quantevolver/evolution/tasks/${resumingTaskId}/resume`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ additional_loops: additionalLoops, force_full_train: forceFullTrain }),
      });
      const data = await res.json();
      if (data.status === "success") {
        appendLogs([`[System] 任务 ${resumingTaskId} 已恢复演进`]);
        setShowResumeDialog(null);
        setAdditionalLoops(0);
        setForceFullTrain(false);
        setActiveTaskId(data.task_id);
        // 延迟刷新：等待 background task 执行结果反映到 DB
        setTimeout(async () => {
          const freshRes = await fetch(`${API}/quantevolver/evolution/tasks`);
          const freshData = await freshRes.json();
          if (freshData.tasks) {
            const resumedTask = freshData.tasks.find((t: any) => t.task_id === resumingTaskId);
            if (resumedTask && resumedTask.status === "failed") {
              appendLogs([`[Error] 任务 ${resumingTaskId} 恢复后立即失败，请检查后端日志`]);
            }
            setTasks(freshData.tasks);
          }
        }, 3000);
      } else {
        alert("恢复失败: " + (data.detail || "未知错误"));
      }
    } catch (err: any) {
      alert("恢复失败: " + (err?.message || "网络错误"));
    }
  };

  // Fork: 从指定 Loop 分叉演进
  const handleForkFromLoop = (loopIndex: number) => {
    if (!activeTaskId) return;
    const task = tasks.find(t => t.task_id === activeTaskId) as any;
    const sourceLoop = loops.find(l => l.loop_index === loopIndex) as any;
    const cfg = sourceLoop?.config_json || {};
    const mp = cfg?.model_params || {};
    const sourceHorizon = ([1, 3, 5, 10, 20].includes(Number(mp.label_horizon || cfg.label_horizon || task?.label_horizon || 1))
      ? Number(mp.label_horizon || cfg.label_horizon || task?.label_horizon || 1)
      : 1) as 1 | 3 | 5 | 10 | 20;
    setForkForm({
      task_name: task ? `${task.task_name}_from_L${loopIndex}` : "",
      max_loops: 10,
      evolution_guidance: "",
      evolution_mode: "auto",
      inherit_history: false,
      strategy_id: task?.strategy_id || "",
      strategy_params: stripRuntimeStrategyFlags(task?.strategy_params),
      execution_algo: task?.execution_algo || "",
      execution_algo_params: task?.execution_algo_params || {},
      label_horizon: sourceHorizon,
      filter_suspended_on_signal: !!task?.strategy_params?.filter_suspended_on_signal,
      suspend_filter_strict: task?.strategy_params?.suspend_filter_strict !== false,
    });
    setShowForkDialog(loopIndex);
  };

  const handleForkCancel = () => {
    setShowForkDialog(null);
    setIsForking(false);
    setForkType("evolution");
    setForkForm({ task_name: "", max_loops: 10, evolution_guidance: "", evolution_mode: "auto", inherit_history: false, strategy_id: "", strategy_params: {}, execution_algo: "", execution_algo_params: {}, label_horizon: 1, filter_suspended_on_signal: false, suspend_filter_strict: true });
    setStrategyEvoLoops([]);
    setStrategyEvoExecutionMode("serial");
  };

  // 策略演进 Loop 配置管理
  const addStrategyEvoLoop = () => {
    const sourceLoop = loops.find(l => l.loop_index === showForkDialog);
    const newLoop = {
      loop_index: strategyEvoLoops.length + 1,
      label: `Loop ${strategyEvoLoops.length + 1}`,
      strategy_params: {
        topk: 50,
        n_drop: 5,
        hold_thresh: 2,
        risk_degree: 0.95,
      },
      strategy_id: "",
      execution_algo: "",
      execution_algo_params: {},
      filter_suspended_on_signal: false,
      suspend_filter_strict: true,
      enable_sector_hmm: false,
      hmm_model_version_id: "",
      hmm_signal_preset: "",
      sector_blacklist: [],
      stock_pool: "",
    };
    setStrategyEvoLoops([...strategyEvoLoops, newLoop]);
  };

  const removeStrategyEvoLoop = (index: number) => {
    const newLoops = strategyEvoLoops.filter((_, i) => i !== index);
    // 重新编号
    const renumbered = newLoops.map((loop, i) => ({ ...loop, loop_index: i + 1 }));
    setStrategyEvoLoops(renumbered);
  };

  const updateStrategyEvoLoop = (index: number, updates: any) => {
    const newLoops = [...strategyEvoLoops];
    newLoops[index] = { ...newLoops[index], ...updates };
    setStrategyEvoLoops(newLoops);
  };

  // 从源 Loop 配置复制
  const copyFromSourceLoop = () => {
    const sourceLoop = loops.find(l => l.loop_index === showForkDialog);
    if (!sourceLoop) return;

    const config = sourceLoop.config_json || {};
    const newLoop = {
      loop_index: strategyEvoLoops.length + 1,
      label: `Loop ${strategyEvoLoops.length + 1} (源配置)`,
      strategy_params: {
        topk: config.strategy_params?.topk || 50,
        n_drop: config.strategy_params?.n_drop || 5,
        hold_thresh: config.strategy_params?.hold_thresh || 2,
        risk_degree: config.strategy_params?.risk_degree || 0.95,
      },
      strategy_id: config.strategy_id || "",
      execution_algo: config.execution_algo || "",
      execution_algo_params: {},
      filter_suspended_on_signal: !!config.strategy_params?.filter_suspended_on_signal,
      suspend_filter_strict: config.strategy_params?.suspend_filter_strict !== false,
      enable_sector_hmm: false,
      hmm_model_version_id: "",
      hmm_signal_preset: "",
      sector_blacklist: [],
      stock_pool: "",
    };
    setStrategyEvoLoops([...strategyEvoLoops, newLoop]);
  };

  // 策略演进提交
  const handleStrategyEvoSubmit = async () => {
    if (!activeTaskId || showForkDialog === null) return;
    if (strategyEvoLoops.length === 0) {
      alert("请至少配置一个策略回测 Loop");
      return;
    }
    for (let i = 0; i < strategyEvoLoops.length; i++) {
      if (!ensureQeExecutionAlgoSupported(strategyEvoLoops[i]?.execution_algo, `strategy loop ${i + 1}`)) return;
    }

    setIsForking(true);
    try {
      const res = await fetch(`${API}/quantevolver/evolution/tasks/${activeTaskId}/strategy-fork`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          from_loop_index: showForkDialog,
          task_name: forkForm.task_name || undefined,
          loops: strategyEvoLoops,
          execution_mode: strategyEvoExecutionMode,
          inherit_history: forkForm.inherit_history,
        }),
      });
      const data = await res.json();
      if (data.status === "success") {
        appendLogs([`[System] 已从 Loop ${showForkDialog} 创建策略演进任务 ${data.task_id}`]);
        appendLogs([`[System] 共 ${data.total_loops} 个策略回测 Loop，执行方式: ${data.execution_mode}`]);
        handleForkCancel();
        fetchTasks();
        setTimeout(() => setActiveTaskId(data.task_id), 500);
      } else {
        alert("策略演进创建失败: " + (data.detail || "未知错误"));
      }
    } catch (err: any) {
      alert("策略演进创建失败: " + (err?.message || "网络错误"));
    } finally {
      setIsForking(false);
    }
  };

  const handleForkSubmit = async () => {
    if (!activeTaskId || showForkDialog === null) return;
    // 提交前二次校验 loop 状态
    const targetLoop = loops.find(l => l.loop_index === showForkDialog);
    if (!targetLoop || targetLoop.status !== "completed") {
      alert("该 Loop 状态已变更，请重新选择");
      handleForkCancel();
      return;
    }
    if (!ensureQeExecutionAlgoSupported(forkForm.execution_algo, "fork task")) return;
    setIsForking(true);
    try {
      const res = await fetch(`${API}/quantevolver/evolution/tasks/${activeTaskId}/fork`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          from_loop_index: showForkDialog,
          task_name: forkForm.task_name || undefined,
          max_loops: forkForm.max_loops,
          evolution_guidance: forkForm.evolution_guidance || undefined,
          evolution_mode: forkForm.evolution_mode,
          inherit_history: forkForm.inherit_history,
          strategy_id: forkForm.strategy_id || undefined,
          strategy_params: Object.keys(forkForm.strategy_params).length > 0 ? forkForm.strategy_params : undefined,
          execution_algo: forkForm.execution_algo || undefined,
          execution_algo_params: Object.keys(forkForm.execution_algo_params).length > 0 ? forkForm.execution_algo_params : undefined,
          filter_suspended_on_signal: !!forkForm.filter_suspended_on_signal,
          suspend_filter_strict: forkForm.suspend_filter_strict !== false,
          label_horizon: forkForm.label_horizon,
        }),
      });
      const data = await res.json();
      if (data.status === "success") {
        appendLogs([`[System] 已从 Loop ${showForkDialog} 分叉创建新任务 ${data.task_id}`]);
        handleForkCancel();
        fetchTasks();
        // 延迟切换到新 task，让 fetchTasks 有时间刷新列表
        setTimeout(() => setActiveTaskId(data.task_id), 500);
      } else {
        alert("分叉失败: " + (data.detail || "未知错误"));
      }
    } catch (err: any) {
      alert("分叉失败: " + (err?.message || "网络错误"));
    } finally {
      setIsForking(false);
    }
  };

  // ── 性能优化: useMemo 计算值 ──
  const activeLoopData = useMemo(
    () => loops.find(l => l.loop_index === activeLoopIndex),
    [loops, activeLoopIndex]
  );
  const prevLoopData = useMemo(
    () => activeLoopData ? loops.find(l => l.loop_index === activeLoopData.loop_index - 1) : undefined,
    [loops, activeLoopData]
  );

  // Phase 3: 当选中 Loop 变化时，拉取增强指标（仅 completed 状态才请求）
  const enhancedMetricsRef = useRef(enhancedMetrics);
  enhancedMetricsRef.current = enhancedMetrics;

  useEffect(() => {
    if (!activeLoopData?.loop_id || (activeLoopData.status !== "completed" && activeLoopData.status !== "failed")) {
      // 仅在当前有值时才 set null，避免无谓 re-render
      if (enhancedMetricsRef.current !== null) setEnhancedMetrics(null);
      return;
    }

    // 清空旧数据（仅在有值时触发渲染）
    if (enhancedMetricsRef.current !== null) setEnhancedMetrics(null);

    // AbortController 防止快速切换 Loop 时竞态
    const controller = new AbortController();
    const loopPathId = `Loop${activeLoopData.loop_index}`;
    fetch(`${API}/quantevolver/evolution/tasks/${activeTaskId}/loops/${loopPathId}/enhanced-metrics`, {
      signal: controller.signal,
    })
      .then(res => {
        if (!res.ok) throw new Error(`Enhanced metrics request failed: ${res.status} ${res.statusText}`);
        return res.json();
      })
      .then(json => {
        if (json?.status === "success" && json?.data) {
          setEnhancedMetrics(json.data);
        }
      })
      .catch((e) => {
        if (e.name !== "AbortError") {
          console.error(`[QE] Enhanced metrics error for ${loopPathId}:`, e.message);
        }
      });

    return () => controller.abort();
  }, [activeLoopData?.loop_id, activeLoopData?.status, activeLoopData?.loop_index, activeTaskId]);

  // 后端日志轮询（仅面板展开时运行）
  useEffect(() => {
    if (!backendLogsOpen) return;
    const fetchLogs = async () => {
      try {
        const res = await fetch(`${API}/quantevolver/evolution/system/logs?tail=150&level=${backendLogLevel}`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const d = await res.json();
        if (d.ok) setBackendLogs(d.lines);
      } catch (e) {
        console.error("[QE] Backend logs fetch error:", e);
      }
    };
    fetchLogs();
    const timer = setInterval(fetchLogs, 5000);
    return () => clearInterval(timer);
  }, [backendLogsOpen, backendLogLevel]);

  const configDiffLines = useMemo(() => {
    if (!activeLoopData?.config_json || !prevLoopData?.config_json) return [] as string[];
    const curr = activeLoopData.config_json;
    const prev = prevLoopData.config_json;
    const keys = Array.from(new Set<string>([...Object.keys(curr), ...Object.keys(prev)])).sort();
    const lines: string[] = [];
    for (const key of keys) {
      const before = JSON.stringify(prev[key]);
      const after = JSON.stringify(curr[key]);
      if (before !== after) {
        lines.push(`${key}: ${before} -> ${after}`);
      }
    }
    return lines;
  }, [activeLoopData, prevLoopData]);

  // 同步资产
  const handleSyncAssets = useCallback(async (loopIndex: number) => {
    if (!confirm("确定要将此 Loop 的模型资产同步到实盘环境吗？")) return;
    const loopPathId = `Loop${loopIndex}`;
    try {
      const res = await fetch(`${API}/quantevolver/evolution/tasks/${activeTaskId}/loops/${loopPathId}/sync_assets`, {
        method: "POST"
      });
      const data = await res.json();
      if (data.status === "success") {
        alert("资产同步成功！\n保存路径：" + data.local_path);
      } else {
        alert("资产同步失败: " + (data.detail || "未知错误"));
      }
    } catch (e: any) {
      alert("资产同步失败: " + (e?.message || "网络错误"));
    }
  }, [activeTaskId]);

  // 重试失败的 Loop
  const handleRetryLoop = async (taskId: string, loopIndex: number) => {
    if (!confirm("确定要重试 Loop " + loopIndex + " 的回测吗？（跳过训练）")) return;
    try {
      const res = await fetch(`${API}/quantevolver/evolution/tasks/${taskId}/loops/${loopIndex}/retry`, { method: "POST" });
      const data = await res.json();
      if (!res.ok) {
        alert("重试失败: " + (data.detail || "未知错误"));
        return;
      }
      appendLogs([`[INFO] Loop ${loopIndex} 重试已提交`]);
      fetchTasks();
      if (activeTaskId) fetchTaskDetail(activeTaskId);
    } catch (err: any) {
      alert("重试失败: " + (err?.message || "网络错误"));
    }
  };

  // 删除任务
  const handleDeleteTask = async (taskId: string, taskName: string, e: React.MouseEvent) => {
    e.stopPropagation();
    if (!confirm(`确定要删除演进任务「${taskName}」吗？\n\n将同时删除所有关联的 Loop 记录、SOTA 注册、子实验和因子/模型指标。\n此操作不可撤销！`)) return;
    try {
      if (activeTaskId === taskId) {
        activeTaskIdRef.current = null; // 阻止旧 SSE onerror 继续重连
        if (eventSourceRef.current) {
          eventSourceRef.current.close();
          eventSourceRef.current = null;
        }
      }
      const res = await fetch(`${API}/quantevolver/evolution/tasks/${taskId}`, { method: "DELETE" });
      const data = await res.json();
      if (data.status === "success") {
        appendLogs([`[System] 任务 ${taskName} 已删除`]);
        if (activeTaskId === taskId) { setActiveTaskId(null); setLoops([]); }
        fetchTasks();
      } else {
        alert("删除失败: " + (data.detail || "未知错误"));
      }
    } catch (err: any) {
      alert("删除失败: " + (err?.message || "网络错误"));
    }
  };

  // ── 性能优化: useCallback 稳定引用（传给 React.memo 子组件） ──
  const handleSelectLoop = useCallback((idx: number) => setActiveLoopIndex(idx), []);
  const handleToggleLogs = useCallback(() => setLogsCollapsed(prev => !prev), []);
  const handleSetDetailTab = useCallback((tab: string) => setDetailTab(tab), []);
  const handleSetRightPanelView = useCallback((v: "loop" | "trajectory") => {
    setRightPanelView(v);
    if (v === "loop") setDetailTab("overview");
  }, []);

  return (
    <div style={{
      display: "flex",
      flexDirection: "column",
      minHeight: "calc(100vh - 48px)",
      gap: "16px",
      padding: "24px",
      boxSizing: "border-box",
      fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif"
    }}>

      {/* ============ 区域A: 演进控制中心 (顶部全宽) ============ */}
      <div style={{ ...cardStyle }}>
        <div style={{ display: "flex", minHeight: 0 }}>
          {/* 左侧: 操作栏 */}
          <div style={{ flex: "0 0 180px", display: "flex", flexDirection: "column", gap: "12px", padding: "16px 20px", borderRight: "1px solid #f1f5f9", backgroundColor: "#f8fafc" }}>
            <h2 style={{ margin: 0, fontSize: "15px", fontWeight: 700, color: "#1e293b", display: "flex", alignItems: "center", gap: "8px" }}>
              <Activity color="#3b82f6" size={18} />
              演进控制中心
            </h2>
            <button
              onClick={() => { setShowCreateTask(true); fetchSourceExperiments(); fetchSourceTasks(); }}
              style={{
                padding: "8px 14px",
                backgroundColor: "#2563eb",
                color: "#fff",
                border: "none",
                borderRadius: "6px",
                fontSize: "13px",
                fontWeight: 600,
                cursor: "pointer",
                display: "flex",
                alignItems: "center",
                gap: "6px",
                boxShadow: "0 2px 4px rgba(37, 99, 235, 0.2)"
              }}>
              <Play size={14} />
              新建任务
            </button>
            <button
              onClick={() => { fetchTasks(); if (activeTaskId) fetchTaskDetail(activeTaskId); }}
              title={hasRunningTask ? "自动刷新: 每10秒" : "自动刷新: 每60秒（无活跃任务）"}
              style={{
                padding: "6px 14px",
                backgroundColor: "#fff",
                color: "#64748b",
                border: "1px solid #e2e8f0",
                borderRadius: "6px",
                fontSize: "12px",
                fontWeight: 600,
                cursor: "pointer",
                display: "flex",
                alignItems: "center",
                gap: "6px",
              }}>
              <RefreshCw size={12} />
              刷新
              <span style={{ fontSize: "10px", color: "#94a3b8", fontWeight: 400 }}>
                {hasRunningTask ? "10s" : "60s"}
              </span>
            </button>
          </div>

          {/* 右侧: 任务列表表格 */}
          <div style={{ flex: 1, overflow: "hidden", display: "flex", flexDirection: "column" }}>
            <div style={{ maxHeight: "660px", overflowY: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "13px" }}>
                <thead>
                  <tr style={{ backgroundColor: "#f8fafc", borderBottom: "1px solid #e2e8f0", position: "sticky", top: 0, zIndex: 1 }}>
                    <th style={{ padding: "10px 16px", textAlign: "left", fontWeight: 700, color: "#475569", fontSize: "12px", textTransform: "uppercase", letterSpacing: "0.05em" }}>任务名称</th>
                    <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "80px" }}>类型</th>
                    <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "80px" }}>演进模式</th>
                    <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "80px" }}>来源</th>
                    <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "80px" }}>状态</th>
                    <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "80px" }}>Loop</th>
                    <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "130px" }}>创建时间</th>
                    <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "120px" }}>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {tasks.length === 0 ? (
                    <tr>
                      <td colSpan={8} style={{ padding: "24px", textAlign: "center", color: "#94a3b8" }}>暂无演进任务</td>
                    </tr>
                  ) : tasks.map(task => {
                    const isActive = activeTaskId === task.task_id;
                    const statusInfo = getTaskStatusInfo(task.status);
                    const canStop = task.status === "running";
                    const canResume = ["stopped", "paused", "completed", "failed"].includes(task.status);
                    const canDelete = task.status !== "running";
                    const sourceType = task.source_type;
                    return (
                      <React.Fragment key={task.task_id}>
                      <tr
                        onClick={() => setActiveTaskId(task.task_id)}
                        style={{
                          cursor: "pointer",
                          backgroundColor: isActive ? "#eff6ff" : "#fff",
                          borderBottom: "1px solid #f1f5f9",
                          transition: "background-color 0.15s",
                        }}
                        onMouseEnter={e => { if (!isActive) (e.currentTarget as HTMLElement).style.backgroundColor = "#fafafa"; }}
                        onMouseLeave={e => { if (!isActive) (e.currentTarget as HTMLElement).style.backgroundColor = "#fff"; }}
                      >
                        <td style={{ padding: "8px 16px", fontWeight: isActive ? 700 : 500, color: "#0f172a", maxWidth: "280px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                          <div>{task.task_name}</div>
                          <div style={{ fontSize: "10px", color: "#94a3b8", fontFamily: "monospace", fontWeight: 400 }}>{task.task_id}</div>
                        </td>
                        <td style={{ padding: "10px 12px", textAlign: "center" }}>
                          <span style={{
                            fontSize: "10px", fontWeight: 700, padding: "2px 8px", borderRadius: "12px",
                            backgroundColor: task.task_type === "strategy_evo" ? "#fef3c7" : task.task_type === "custom_evo" ? "#e0e7ff" : "#f0fdf4",
                            color: task.task_type === "strategy_evo" ? "#b45309" : task.task_type === "custom_evo" ? "#4338ca" : "#15803d",
                          }}>
                            {task.task_type === "strategy_evo" ? "策略演进" : task.task_type === "custom_evo" ? "自定义" : "自动演进"}
                          </span>
                        </td>
                        <td style={{ padding: "10px 12px", textAlign: "center" }}>
                          <span style={{
                            fontSize: "10px", fontWeight: 700, padding: "2px 8px", borderRadius: "12px",
                            backgroundColor: task.evolution_mode === "factor_only" ? "#fef9c3" : task.evolution_mode === "model_only" ? "#dbeafe" : task.evolution_mode === "joint" ? "#f3e8ff" : "#ecfdf5",
                            color: task.evolution_mode === "factor_only" ? "#a16207" : task.evolution_mode === "model_only" ? "#1d4ed8" : task.evolution_mode === "joint" ? "#7c3aed" : "#047857",
                          }}>
                            {task.evolution_mode === "factor_only" ? "仅因子" : task.evolution_mode === "model_only" ? "仅模型" : task.evolution_mode === "joint" ? "联合" : "自动"}
                          </span>
                        </td>
                        <td style={{ padding: "10px 12px", textAlign: "center" }}>
                          <span style={{
                            fontSize: "10px", fontWeight: 700, padding: "2px 8px", borderRadius: "12px",
                            backgroundColor: sourceType === "fork" ? "#faf5ff" : sourceType === "rdagent_task_sota" ? "#ede9fe" : "#dbeafe",
                            color: sourceType === "fork" ? "#9333ea" : sourceType === "rdagent_task_sota" ? "#7c3aed" : "#2563eb",
                          }}>
                            {sourceType === "fork" ? "分叉" : sourceType === "rdagent_task_sota" ? "RDAgent" : "QE实验"}
                          </span>
                        </td>
                        <td style={{ padding: "10px 12px", textAlign: "center" }}>
                          <span style={{
                            fontSize: "11px", fontWeight: 600, padding: "2px 8px", borderRadius: "12px",
                            color: statusInfo.color, backgroundColor: statusInfo.bgColor, whiteSpace: "nowrap",
                            display: "inline-flex", alignItems: "center", gap: "4px",
                          }}>
                            {task.status === "running" && <span style={{ display: "inline-block", width: "6px", height: "6px", borderRadius: "50%", backgroundColor: "#22c55e", animation: "pulse 2s infinite" }} />}
                            {statusInfo.label}
                          </span>
                        </td>
                        <td style={{ padding: "10px 12px", textAlign: "center", fontFamily: "monospace", fontSize: "12px", color: "#475569" }}>
                          {task.current_loop}/{task.max_loops}
                        </td>
                        <td style={{ padding: "10px 12px", textAlign: "center", fontSize: "11px", color: "#64748b", whiteSpace: "nowrap" }}>
                          {task.created_at ? new Date(task.created_at).toLocaleString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" }) : "-"}
                        </td>
                        <td style={{ padding: "10px 12px", textAlign: "center" }}>
                          <div style={{ display: "flex", gap: "4px", justifyContent: "center" }}>
                            <button onClick={(e) => { e.stopPropagation(); window.open(`/quantevolver/evolution/${task.task_id}`, '_blank'); }}
                              title="独立详情页"
                              style={{ padding: "4px 8px", border: "1px solid #3b82f6", borderRadius: "4px", backgroundColor: "#eff6ff", color: "#3b82f6", fontSize: "11px", fontWeight: 600, cursor: "pointer", display: "flex", alignItems: "center", gap: "3px" }}>
                              详情
                            </button>
                            {canStop && (
                              <button onClick={(e) => handleStopTask(task.task_id, e)}
                                title="停止"
                                style={{ padding: "4px 8px", border: "1px solid #fca5a5", borderRadius: "4px", backgroundColor: "#fff", color: "#ef4444", fontSize: "11px", fontWeight: 600, cursor: "pointer", display: "flex", alignItems: "center", gap: "3px" }}>
                                <Square size={11} /> 停止
                              </button>
                            )}
                            {canResume && (
                              <button onClick={(e) => { e.stopPropagation(); setShowResumeDialog(task.task_id); setAdditionalLoops(0); setForceFullTrain(false); }}
                                title="恢复演进"
                                style={{ padding: "4px 8px", border: "1px solid #86efac", borderRadius: "4px", backgroundColor: "#fff", color: "#16a34a", fontSize: "11px", fontWeight: 600, cursor: "pointer", display: "flex", alignItems: "center", gap: "3px" }}>
                                <RotateCcw size={11} /> 恢复
                              </button>
                            )}
                            {canDelete && (
                              <button onClick={(e) => handleDeleteTask(task.task_id, task.task_name, e)}
                                title="删除任务"
                                style={{ padding: "4px 8px", border: "1px solid #fca5a5", borderRadius: "4px", backgroundColor: "#fff", color: "#ef4444", fontSize: "11px", fontWeight: 600, cursor: "pointer", display: "flex", alignItems: "center", gap: "3px" }}>
                                <Trash2 size={11} />
                              </button>
                            )}
                          </div>
                        </td>
                      </tr>
                      {isActive && (() => {
                        const sp: Record<string, any> = (task as any).strategy_params || {};
                        const hmmOn = !!sp.enable_sector_hmm;
                        const sName = task.strategy_id
                          ? (strategyCatalog.find((s: any) => s.strategy_id === task.strategy_id)?.display_name || task.strategy_id)
                          : "默认";
                        const eName = (task as any).execution_algo || "收盘价成交";
                        const firstLoop = loops.find((l: Loop) => l.loop_index === 1);
                        const cfg: any = firstLoop?.config_json || {};
                        const fList: string[] = cfg.factor_list || cfg.factor_names || [];
                        const mId: string = cfg.model_id || "";
                        const lblMap: Record<string, string> = { close: "收盘价", open: "开盘价", vwap: "VWAP" };
                        const modeMap: Record<string, string> = { auto: "自动", factor_only: "仅因子", model_only: "仅模型", joint: "联合" };
                        const CB = (l: string, v: string, c: string) => (
                          <span style={{ display: "inline-flex", alignItems: "center", gap: "4px", fontSize: "11px" }}>
                            <span style={{ color: "#94a3b8", fontWeight: 500 }}>{l}:</span>
                            <span style={{ fontSize: "10px", fontWeight: 700, padding: "1px 7px", borderRadius: "10px", backgroundColor: c + "18", color: c }}>{v}</span>
                          </span>
                        );
                        return (
                        <tr>
                          <td colSpan={3} style={{ padding: "0 16px 10px 16px", backgroundColor: "#eff6ff", borderBottom: "2px solid #bfdbfe" }}>
                            <div style={{ display: "flex", alignItems: "flex-start", gap: "12px", padding: "8px 12px", backgroundColor: "#f8faff", borderRadius: "8px", border: "1px solid #e0e7ff" }}>
                              <div style={{ flex: 1, minWidth: 0 }}>
                                <div style={{ display: "flex", flexWrap: "wrap", gap: "8px", alignItems: "center", marginBottom: "6px" }}>
                                  {CB("策略", sName, "#3b82f6")}
                                  {CB("执行", eName, "#8b5cf6")}
                                  {mId && CB("模型", mId.length > 16 ? mId.slice(0, 16) + "..." : mId, "#059669")}
                                  {CB("标签", lblMap[(task as any).label_type || "close"] || (task as any).label_type || "收盘价", "#d97706")}
                                  {CB("Horizon", `${(task as any).label_horizon || cfg?.label_horizon || cfg?.model_params?.label_horizon || 1}d`, "#0f766e")}
                                  {CB("演进", modeMap[(task as any).evolution_mode || "auto"] || (task as any).evolution_mode || "自动", "#6366f1")}
                                  {CB("HMM", hmmOn ? `启用 (${sp.hmm_signal_preset || "preset_A"})` : "未启用", hmmOn ? "#16a34a" : "#94a3b8")}
                                  {CB("行业黑名单", (task as any).stock_pool ? "已启用" : "未启用", (task as any).stock_pool ? "#16a34a" : "#94a3b8")}
                                </div>
                                <div style={{ marginBottom: "4px" }}>
                                  <span style={{ fontSize: "11px", color: "#64748b", fontWeight: 600, marginRight: "6px" }}>因子 ({fList.length}):</span>
                                  {fList.length === 0 && <span style={{ fontSize: "11px", color: "#94a3b8" }}>加载中...</span>}
                                  {fList.length > 0 && (
                                    <>
                                      {fList.slice(0, factorsExpanded ? undefined : 8).map((f: string, i: number) => (
                                        <span key={i} style={{ display: "inline-block", fontSize: "10px", padding: "1px 6px", borderRadius: "4px", backgroundColor: "#e0f2fe", color: "#0369a1", marginRight: "3px", marginBottom: "2px" }}>{f}</span>
                                      ))}
                                      {fList.length > 8 && (
                                        <button onClick={() => setFactorsExpanded(!factorsExpanded)}
                                          style={{ fontSize: "10px", color: "#3b82f6", background: "none", border: "none", cursor: "pointer", fontWeight: 600 }}>
                                          {factorsExpanded ? "收起" : `+${fList.length - 8} 更多`}
                                        </button>
                                      )}
                                    </>
                                  )}
                                </div>
                                {(task.target_desc || (task as any).evolution_guidance) && (
                                  <div style={{ display: "flex", gap: "16px", fontSize: "11px", color: "#475569" }}>
                                    {task.target_desc && (
                                      <div style={{ flex: 1, minWidth: 0 }}>
                                        <span style={{ fontWeight: 600, color: "#64748b" }}>目标: </span>
                                        <span title={task.target_desc}>{task.target_desc.length > 80 ? task.target_desc.slice(0, 80) + "..." : task.target_desc}</span>
                                      </div>
                                    )}
                                    {(task as any).evolution_guidance && (
                                      <div style={{ flex: 1, minWidth: 0 }}>
                                        <span style={{ fontWeight: 600, color: "#64748b" }}>指引: </span>
                                        <span title={(task as any).evolution_guidance}>{(task as any).evolution_guidance.length > 80 ? (task as any).evolution_guidance.slice(0, 80) + "..." : (task as any).evolution_guidance}</span>
                                      </div>
                                    )}
                                  </div>
                                )}
                                {(task as any).factor_blacklist && (task as any).factor_blacklist.length > 0 && (
                                  <div style={{ marginTop: "4px" }}>
                                    <span style={{ fontSize: "11px", color: "#ef4444", fontWeight: 600, marginRight: "6px" }}>因子黑名单:</span>
                                    {(task as any).factor_blacklist.map((f: string, i: number) => (
                                      <span key={i} style={{ display: "inline-block", fontSize: "10px", padding: "1px 6px", borderRadius: "4px", backgroundColor: "#fef2f2", color: "#dc2626", marginRight: "3px", textDecoration: "line-through" }}>{f}</span>
                                    ))}
                                  </div>
                                )}
                              </div>
                            </div>
                          </td>
                          <td colSpan={5} style={{ padding: "8px 12px 10px 12px", backgroundColor: "#eff6ff", borderBottom: "2px solid #bfdbfe", verticalAlign: "top" }}>
                            <button
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setCloneFromTask(task);
                                  setShowCreateTask(true);
                                  fetchSourceExperiments();
                                  fetchSourceTasks();
                                }}
                                title="基于此配置新建演进任务"
                                style={{
                                  flexShrink: 0, display: "flex", alignItems: "center", gap: "4px",
                                  padding: "6px 12px", borderRadius: "6px", border: "1px solid #3b82f6",
                                  backgroundColor: "#eff6ff", color: "#2563eb", fontSize: "11px",
                                  fontWeight: 600, cursor: "pointer", whiteSpace: "nowrap",
                                }}
                              >
                                <Copy size={12} /> 基于此配置新建
                              </button>
                          </td>
                        </tr>
                        );
                      })()}
                      </React.Fragment>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>

      {/* ============ 实时日志 (任务列表下方, 可折叠) — React.memo 子组件 ============ */}
      <LogsPanel
        logs={logs}
        collapsed={logsCollapsed}
        onToggle={handleToggleLogs}
      />

      {/* ============ 后端日志面板（折叠） ============ */}
      <div style={{ marginBottom: "8px", border: "1px solid #e2e8f0", borderRadius: "8px", overflow: "hidden" }}>
        <div
          onClick={() => setBackendLogsOpen(o => !o)}
          style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 14px", backgroundColor: "#f8fafc", cursor: "pointer", userSelect: "none" }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "8px", fontSize: "13px", fontWeight: 600, color: "#475569" }}>
            <span style={{ fontSize: "10px" }}>{backendLogsOpen ? "▼" : "▶"}</span>
            后端日志（Windows AIstock）
            <span style={{ fontSize: "11px", fontWeight: 400, color: "#94a3b8" }}>LLM分析 / API报错</span>
          </div>
          {backendLogsOpen && (
            <div style={{ display: "flex", gap: "6px" }} onClick={e => e.stopPropagation()}>
              {(["全部", "ERROR", "WARN", "INFO"] as const).map(lv => (
                <button key={lv}
                  onClick={() => setBackendLogLevel(lv === "全部" ? "" : lv)}
                  style={{ fontSize: "11px", padding: "2px 8px", borderRadius: "4px", border: "1px solid #cbd5e1", cursor: "pointer",
                    backgroundColor: (lv === "全部" ? "" : lv) === backendLogLevel ? "#1e293b" : "#fff",
                    color: (lv === "全部" ? "" : lv) === backendLogLevel ? "#fff" : "#475569",
                  }}>{lv}</button>
              ))}
            </div>
          )}
        </div>
        {backendLogsOpen && (
          <LogTerminal
            logs={backendLogs.length > 0 ? backendLogs : ["[System] 等待日志..."]}
            logsEndRef={backendLogsEndRef}
            maxHeight={280}
            fontSize={11}
          />
        )}
      </div>

      {/* ============ 区域B: 拓扑 + Loop 详情 (中间横向并列, flex:1) ============ */}
      <div style={{ display: "flex", flex: 1, gap: "16px", minHeight: "1200px" }}>

        {/* 演进拓扑 — React.memo 子组件 */}
        <TopologyPanel
          loops={loops}
          activeLoopIndex={activeLoopIndex}
          onSelectLoop={handleSelectLoop}
          onRetryLoop={handleRetryLoop}
          taskType={tasks.find(t => t.task_id === activeTaskId)?.task_type}
        />

        {/* Loop 详情看板 — React.memo 子组件 */}
        <LoopDetailPanel
          activeLoopData={activeLoopData}
          prevLoopData={prevLoopData}
          rightPanelView={rightPanelView}
          onSetRightPanelView={handleSetRightPanelView}
          detailTab={detailTab}
          onSetDetailTab={handleSetDetailTab}
          enhancedMetrics={enhancedMetrics}
          activeTaskId={activeTaskId}
          activeTask={tasks.find(t => t.task_id === activeTaskId)}
          configDiffLines={configDiffLines}
          onSyncAssets={handleSyncAssets}
          onForkFromLoop={handleForkFromLoop}
          taskType={tasks.find(t => t.task_id === activeTaskId)?.task_type}
          loops={loops}
          onLoopSelect={handleSelectLoop}
        />

      </div>

      {showCreateTask && (
        <div style={{
          position: "fixed", top: 0, left: 0, right: 0, bottom: 0,
          backgroundColor: "rgba(0,0,0,0.5)", zIndex: 100,
          display: "flex", alignItems: "center", justifyContent: "center",
        }}>
          <div style={{
            backgroundColor: "#fff", padding: "24px", borderRadius: "12px",
            width: "960px", maxHeight: "90vh", overflowY: "auto",
            boxShadow: "0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04)"
          }}>
            <h2 style={{ margin: "0 0 20px", fontSize: "18px", color: "#1e293b", display: "flex", alignItems: "center", gap: "8px" }}>
              <Play size={20} color="#3b82f6" />
              新建演进任务
            </h2>

            <div style={{ display: "flex", flexDirection: "column", gap: "16px" }}>
              <div>
                <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>任务名称</label>
                <input
                  type="text"
                  value={newTask.task_name}
                  onChange={e => setNewTask({...newTask, task_name: e.target.value})}
                  placeholder="例如: Alpha158-基于XGBoost的演进"
                  style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box" }}
                />
              </div>

              {/* 来源类型选择 */}
              <div>
                <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>来源类型</label>
                <div style={{ display: "flex", gap: "8px" }}>
                  {([
                    { value: "qe_experiment" as const, label: "QE 实验" },
                    { value: "rdagent_task_sota" as const, label: "RDAgent Task SOTA" },
                    { value: "evolution_fork" as const, label: "已有演进 Loop" },
                    { value: "custom_evo" as const, label: "自定义演进" },
                  ]).map(opt => (
                    <button key={opt.value}
                      onClick={() => {
                        setNewTask({ ...newTask, source_type: opt.value, base_experiment_id: "", source_task_id: "", fork_from_task_id: "", fork_from_loop_index: -1, inherit_history: false });
                        setSotaPreview(null);
                        setDetectedTaskType("none");
                        setSelectedFactorsForEvo(new Set());
                        setSelectedModelForEvo("");
                        setShowCustomizeFactors(false);
                        setShowCustomizeModel(false);
                        setShowFactorLibrary(false);
                        setAdditionalFactorKeys(new Set());
                        setCorrPairs([]);
                        setCorrAnalyzed(false);
                        setCorrLoading(false);
                        setFactorsToRemove(new Set());
                        setForkSourceLoops([]);
                        if (opt.value === "rdagent_task_sota") fetchSourceTasks();
                        if (opt.value === "qe_experiment") fetchSourceExperiments();
                        if (opt.value === "custom_evo") {
                          fetchSourceExperiments();
                          fetchSourceTasks();
                          setCustomEvoLoops([makeDefaultCustomLoop()]);
                          setCustomEvoFirstLoopReady(false);
                          setCustomEvoInitSource("manual");
                          setCustomEvoSourceExpId("");
                          setCustomEvoSourceTaskId("");
                          setCustomEvoSourceLoopIdx(-1);
                          setCustomEvoForkLoops([]);
                          setCustomEvoExecutionMode("serial");
                          setCustomEvoParallelism(2);
                          setCustomEvoNodeId("");
                        }
                      }}
                      style={{
                        flex: 1, padding: "8px 12px", borderRadius: "6px", fontSize: "13px", fontWeight: 600, cursor: "pointer",
                        border: newTask.source_type === opt.value ? "2px solid #3b82f6" : "1px solid #cbd5e1",
                        backgroundColor: newTask.source_type === opt.value ? "#eff6ff" : "#fff",
                        color: newTask.source_type === opt.value ? "#2563eb" : "#64748b",
                      }}
                    >{opt.label}</button>
                  ))}
                </div>
              </div>

              {newTask.source_type !== "custom_evo" && (<>{newTask.source_type === "qe_experiment" ? (
                <div>
                  <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>选择基础实验</label>
                  <select
                    value={newTask.base_experiment_id}
                    onChange={e => setNewTask({...newTask, base_experiment_id: e.target.value})}
                    style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box" }}
                  >
                    <option value="">-- 请选择已完成的实验 --</option>
                    {sourceExperiments.map(exp => (
                      <option key={exp.experiment_id} value={exp.experiment_id}>
                        {exp.experiment_name || exp.experiment_id.slice(0, 8)}
                        {" "}(IC: {exp.ic?.toFixed(4) ?? "N/A"}, 年化: {exp.annualized_return != null ? (exp.annualized_return * 100).toFixed(2) + "%" : "N/A"}, 因子: {exp.factor_count ?? 0}
                        {exp.is_sota ? ", SOTA" : ""})
                      </option>
                    ))}
                  </select>
                  {sourceExperiments.length === 0 && (
                    <div style={{ marginTop: "6px" }}>
                      <button
                        onClick={fetchSourceExperiments}
                        style={{ fontSize: "12px", color: "#3b82f6", background: "none", border: "none", cursor: "pointer", padding: 0, textDecoration: "underline" }}
                      >刷新实验列表</button>
                    </div>
                  )}
                  <div style={{ marginTop: "8px" }}>
                    <label style={{ display: "block", fontSize: "12px", color: "#94a3b8", marginBottom: "4px" }}>或手动输入实验 ID</label>
                    <input
                      type="text"
                      value={newTask.base_experiment_id}
                      onChange={e => setNewTask({...newTask, base_experiment_id: e.target.value})}
                      placeholder="输入 qe_experiments 的 experiment_id"
                      style={{ width: "100%", padding: "6px 10px", borderRadius: "6px", border: "1px solid #e2e8f0", fontSize: "13px", boxSizing: "border-box", fontFamily: "monospace", color: "#64748b" }}
                    />
                  </div>
                  {/* 已选实验预览 */}
                  {newTask.base_experiment_id && sourceExperiments.find(e => e.experiment_id === newTask.base_experiment_id) && (() => {
                    const sel = sourceExperiments.find(e => e.experiment_id === newTask.base_experiment_id)!;
                    return (
                      <div style={{ marginTop: "8px", padding: "10px", borderRadius: "6px", backgroundColor: "#eff6ff", border: "1px solid #bfdbfe", fontSize: "12px" }}>
                        <div style={{ fontWeight: 600, color: "#1e40af", marginBottom: "4px" }}>已选实验</div>
                        <div>名称: {sel.experiment_name || "-"}</div>
                        <div>IC: {sel.ic?.toFixed(4) ?? "N/A"} | ICIR: {sel.icir?.toFixed(4) ?? "N/A"} | Sharpe: {(sel.annualized_return && sel.max_drawdown) ? "有数据" : "N/A"}</div>
                        <div>因子数: {sel.factor_count ?? 0} | 模型: {sel.model_id || "N/A"}</div>
                        <div>完成时间: {sel.completed_at ? new Date(sel.completed_at).toLocaleString() : "N/A"}</div>
                      </div>
                    );
                  })()}
                </div>
              ) : (
                <>
                  <div>
                    <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>选择 RDAgent Task</label>
                    <select
                      value={newTask.source_task_id}
                      onChange={e => {
                        setNewTask({ ...newTask, source_task_id: e.target.value });
                        setSelectedFactorsForEvo(new Set());
                        setSelectedModelForEvo("");
                        setShowCustomizeFactors(false);
                        setShowCustomizeModel(false);
                        setShowFactorLibrary(false);
                        setAdditionalFactorKeys(new Set());
                        setCorrPairs([]);
                        setCorrAnalyzed(false);
                        setCorrLoading(false);
                        setFactorsToRemove(new Set());
                        fetchSotaPreview(e.target.value);
                      }}
                      style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box" }}
                    >
                      <option value="">-- 请选择 --</option>
                      {sourceTasks.map(t => (
                        <option key={t.task_id} value={t.task_id}>
                          {t.task_id.length > 20 ? t.task_id.slice(0, 20) + "..." : t.task_id}
                          {" "}(Loops: {t.total_loops}{t.has_sota ? `, SOTA模型: ${t.sota_model_count}, 因子: ${t.sota_factor_count}` : ", 无SOTA"}, IC: {t.best_ic?.toFixed(4) ?? "N/A"}, 年化: {t.best_annualized_return != null ? (t.best_annualized_return * 100).toFixed(2) + "%" : "N/A"}, 回撤: {t.worst_max_drawdown != null ? (t.worst_max_drawdown * 100).toFixed(2) + "%" : "N/A"})
                        </option>
                      ))}
                    </select>
                  </div>

                  {/* Task 类型标签 + SOTA 因子完整表格 */}
                  {sotaPreview && detectedTaskType !== "none" && (
                    <div style={{ borderRadius: "6px", backgroundColor: "#f8fafc", border: "1px solid #e2e8f0", fontSize: "12px", overflow: "hidden" }}>
                      {/* 头部：类型标签 + 统计 */}
                      <div style={{ padding: "10px 14px", display: "flex", alignItems: "center", gap: "8px", borderBottom: "1px solid #e2e8f0" }}>
                        <span style={{
                          display: "inline-block", padding: "2px 8px", borderRadius: "4px", fontWeight: 700, fontSize: "11px",
                          backgroundColor: detectedTaskType === "factor" ? "#dbeafe" : detectedTaskType === "model" ? "#d1fae5" : "#fef3c7",
                          color: detectedTaskType === "factor" ? "#2563eb" : detectedTaskType === "model" ? "#059669" : "#d97706",
                        }}>
                          {detectedTaskType === "factor" ? "Factor Task" : detectedTaskType === "model" ? "Model Task" : "Mixed Task"}
                        </span>
                        <span style={{ color: "#64748b" }}>
                          {sotaPreview.total_sota_factors} 个 SOTA 因子 | {sotaPreview.total_sota_models} 个 SOTA 模型
                        </span>
                      </div>

                      {/* SOTA 因子表格 */}
                      {sotaPreview.sota_factors?.length > 0 && (
                        <div style={{ padding: "8px 14px" }}>
                          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "11px" }}>
                            <thead>
                              <tr style={{ borderBottom: "1px solid #e2e8f0" }}>
                                <th style={{ textAlign: "left", padding: "4px 6px", color: "#64748b", fontWeight: 600, width: "28px" }}></th>
                                <th style={{ textAlign: "left", padding: "4px 6px", color: "#64748b", fontWeight: 600 }}>因子名称</th>
                                <th style={{ textAlign: "right", padding: "4px 6px", color: "#64748b", fontWeight: 600 }}>IC</th>
                                <th style={{ textAlign: "right", padding: "4px 6px", color: "#64748b", fontWeight: 600 }}>ICIR</th>
                                <th style={{ textAlign: "right", padding: "4px 6px", color: "#64748b", fontWeight: 600 }}>Sharpe</th>
                                <th style={{ textAlign: "right", padding: "4px 6px", color: "#64748b", fontWeight: 600 }}>年化收益</th>
                                <th style={{ textAlign: "right", padding: "4px 6px", color: "#64748b", fontWeight: 600 }}>最大回撤</th>
                              </tr>
                            </thead>
                            <tbody>
                              {sotaPreview.sota_factors.map((f: any) => (
                                <tr key={f.factor_name} style={{
                                  borderBottom: "1px solid #f1f5f9",
                                  opacity: factorsToRemove.has(f.factor_name) ? 0.4 : 1,
                                }}>
                                  <td style={{ padding: "4px 6px" }}>
                                    <input
                                      type="checkbox"
                                      checked={!factorsToRemove.has(f.factor_name)}
                                      onChange={() => {
                                        setFactorsToRemove(prev => {
                                          const next = new Set(prev);
                                          if (next.has(f.factor_name)) next.delete(f.factor_name);
                                          else next.add(f.factor_name);
                                          return next;
                                        });
                                      }}
                                      style={{ cursor: "pointer" }}
                                    />
                                  </td>
                                  <td style={{ padding: "4px 6px", color: "#1e293b", fontFamily: "monospace", fontSize: "11px" }}>{f.factor_name}</td>
                                  <td style={{ textAlign: "right", padding: "4px 6px", color: "#475569" }}>{f.ic_mean?.toFixed(4) ?? "N/A"}</td>
                                  <td style={{ textAlign: "right", padding: "4px 6px", color: "#475569" }}>{f.icir?.toFixed(2) ?? "N/A"}</td>
                                  <td style={{ textAlign: "right", padding: "4px 6px", color: "#475569" }}>{f.top_excess_sharpe?.toFixed(2) ?? "N/A"}</td>
                                  <td style={{ textAlign: "right", padding: "4px 6px", color: "#475569" }}>
                                    {f.top_excess_annual_return != null ? `${(f.top_excess_annual_return * 100).toFixed(1)}%` : "N/A"}
                                  </td>
                                  <td style={{ textAlign: "right", padding: "4px 6px", color: "#475569" }}>
                                    {f.top_max_drawdown != null ? `${(f.top_max_drawdown * 100).toFixed(1)}%` : "N/A"}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>

                          {/* 底部：已选计数 + 相关性分析按钮 */}
                          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: "8px", paddingTop: "6px", borderTop: "1px solid #e2e8f0" }}>
                            <span style={{ color: "#64748b", fontSize: "11px" }}>
                              已选 {Math.max(0, (sotaPreview.sota_factors?.length || 0) - factorsToRemove.size)} / {sotaPreview.sota_factors?.length || 0} 个因子
                            </span>
                            <button
                              onClick={async () => {
                                if (corrAnalyzed) {
                                  setCorrAnalyzed(false);
                                  return;
                                }
                                setCorrLoading(true);
                                try {
                                  const res = await fetch(`${API}/quantevolver/evolution/source-tasks/${newTask.source_task_id}/preview?include_correlations=true`);
                                  const data = await res.json();
                                  if (data.status === "success" && data.data.correlation_pairs) {
                                    setCorrPairs(data.data.correlation_pairs);
                                  }
                                  setCorrAnalyzed(true);
                                } catch (e) {
                                  console.error("Failed to fetch correlations:", e);
                                } finally {
                                  setCorrLoading(false);
                                }
                              }}
                              disabled={corrLoading || (sotaPreview.sota_factors?.length || 0) < 2}
                              style={{
                                padding: "4px 12px", fontSize: "11px", fontWeight: 600, borderRadius: "4px", cursor: corrLoading || (sotaPreview.sota_factors?.length || 0) < 2 ? "not-allowed" : "pointer",
                                backgroundColor: corrAnalyzed ? "#fef3c7" : "#eff6ff", color: corrAnalyzed ? "#92400e" : "#2563eb",
                                border: corrAnalyzed ? "1px solid #fde68a" : "1px solid #bfdbfe",
                                opacity: corrLoading || (sotaPreview.sota_factors?.length || 0) < 2 ? 0.5 : 1,
                              }}
                            >
                              {corrLoading ? "分析中..." : corrAnalyzed ? "收起相关性" : "相关性分析"}
                            </button>
                          </div>
                        </div>
                      )}

                      {/* 相关性分析面板 */}
                      {corrAnalyzed && (
                        <div style={{ padding: "10px 14px", borderTop: "1px solid #e2e8f0", backgroundColor: "#fffbeb" }}>
                          <div style={{ fontWeight: 600, color: "#92400e", marginBottom: "8px", fontSize: "12px" }}>因子相关性分析</div>
                          {corrPairs.length === 0 ? (
                            <div style={{ color: "#78350f", fontSize: "11px" }}>暂无已计算的相关性数据</div>
                          ) : (() => {
                            const highPairs = corrPairs.filter(p => Math.abs(p.correlation) > 0.7);
                            const lowPairs = corrPairs.filter(p => Math.abs(p.correlation) <= 0.7);
                            // 建造因子指标 map
                            const factorMap: Record<string, any> = {};
                            (sotaPreview.sota_factors || []).forEach((f: any) => { factorMap[f.factor_name] = f; });

                            // 生成建议移除列表
                            const suggestRemovals = new Set<string>();
                            highPairs.forEach(p => {
                              const a = factorMap[p.factor_a];
                              const b = factorMap[p.factor_b];
                              if (!a || !b) return;
                              const aIc = a.ic_mean ?? 0;
                              const bIc = b.ic_mean ?? 0;
                              const aSh = a.top_excess_sharpe ?? 0;
                              const bSh = b.top_excess_sharpe ?? 0;
                              if (aIc < bIc) suggestRemovals.add(p.factor_a);
                              else if (aIc > bIc) suggestRemovals.add(p.factor_b);
                              else if (aSh < bSh) suggestRemovals.add(p.factor_a);
                              else suggestRemovals.add(p.factor_b);
                            });

                            // 检查建议是否已全部应用
                            const allApplied = suggestRemovals.size > 0 && Array.from(suggestRemovals).every(n => factorsToRemove.has(n));

                            return (
                              <>
                                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "11px" }}>
                                  <thead>
                                    <tr style={{ borderBottom: "1px solid #fde68a" }}>
                                      <th style={{ textAlign: "left", padding: "3px 6px", color: "#78350f", fontWeight: 600 }}>因子 A</th>
                                      <th style={{ textAlign: "left", padding: "3px 6px", color: "#78350f", fontWeight: 600 }}>因子 B</th>
                                      <th style={{ textAlign: "right", padding: "3px 6px", color: "#78350f", fontWeight: 600 }}>相关系数</th>
                                      <th style={{ textAlign: "left", padding: "3px 6px", color: "#78350f", fontWeight: 600 }}>建议</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {highPairs.map((p, i) => {
                                      const a = factorMap[p.factor_a];
                                      const b = factorMap[p.factor_b];
                                      let suggestion = "";
                                      let suggestName = "";
                                      if (a && b) {
                                        const aIc = a.ic_mean ?? 0;
                                        const bIc = b.ic_mean ?? 0;
                                        const aSh = a.top_excess_sharpe ?? 0;
                                        const bSh = b.top_excess_sharpe ?? 0;
                                        if (aIc < bIc) { suggestion = `建议移除 ${p.factor_a} (IC 较低)`; suggestName = p.factor_a; }
                                        else if (aIc > bIc) { suggestion = `建议移除 ${p.factor_b} (IC 较低)`; suggestName = p.factor_b; }
                                        else if (aSh < bSh) { suggestion = `建议移除 ${p.factor_a} (Sharpe 较低)`; suggestName = p.factor_a; }
                                        else { suggestion = `建议移除 ${p.factor_b} (Sharpe 较低)`; suggestName = p.factor_b; }
                                      }
                                      const alreadyRemoved = suggestName && factorsToRemove.has(suggestName);
                                      return (
                                        <tr key={i} style={{ backgroundColor: Math.abs(p.correlation) > 0.85 ? "#fecaca" : "#fed7aa", borderBottom: "1px solid #fde68a" }}>
                                          <td style={{ padding: "3px 6px", fontFamily: "monospace" }}>{p.factor_a}</td>
                                          <td style={{ padding: "3px 6px", fontFamily: "monospace" }}>{p.factor_b}</td>
                                          <td style={{ textAlign: "right", padding: "3px 6px", fontWeight: 600 }}>{p.correlation.toFixed(4)}</td>
                                          <td style={{ padding: "3px 6px" }}>
                                            {suggestion && (
                                              alreadyRemoved ? (
                                                <span style={{ color: "#16a34a", fontSize: "10px" }}>已移除 {suggestName}</span>
                                              ) : (
                                                <span
                                                  onClick={() => {
                                                    if (suggestName) setFactorsToRemove(prev => new Set(prev).add(suggestName));
                                                  }}
                                                  style={{ color: "#dc2626", cursor: "pointer", textDecoration: "underline" }}
                                                >{suggestion}</span>
                                              )
                                            )}
                                          </td>
                                        </tr>
                                      );
                                    })}
                                    {lowPairs.length > 0 && (
                                      <tr>
                                        <td colSpan={4} style={{ padding: "4px 6px", color: "#94a3b8", fontSize: "10px", borderBottom: "1px solid #e2e8f0" }}>
                                          ───── 以下 |corr| ≤ 0.7 ─────
                                        </td>
                                      </tr>
                                    )}
                                    {lowPairs.map((p, i) => (
                                      <tr key={`low-${i}`} style={{ borderBottom: "1px solid #f1f5f9" }}>
                                        <td style={{ padding: "3px 6px", fontFamily: "monospace", color: "#64748b" }}>{p.factor_a}</td>
                                        <td style={{ padding: "3px 6px", fontFamily: "monospace", color: "#64748b" }}>{p.factor_b}</td>
                                        <td style={{ textAlign: "right", padding: "3px 6px", color: "#64748b" }}>{p.correlation.toFixed(4)}</td>
                                        <td style={{ padding: "3px 6px" }}></td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>

                                {/* 自动建议摘要 */}
                                <div style={{ marginTop: "8px", padding: "6px 8px", backgroundColor: "#fef9c3", borderRadius: "4px", fontSize: "11px" }}>
                                  <span style={{ color: "#78350f" }}>
                                    汇总: {sotaPreview.sota_factors?.length || 0} 个因子共 {corrPairs.length} 对组合，其中 {highPairs.length} 对相关性 &gt; 0.7
                                  </span>
                                  {suggestRemovals.size > 0 && (
                                    <div style={{ marginTop: "4px", display: "flex", alignItems: "center", gap: "8px" }}>
                                      {allApplied ? (
                                        <span style={{ color: "#16a34a", fontWeight: 600 }}>
                                          已移除: {Array.from(suggestRemovals).join(", ")}
                                        </span>
                                      ) : (
                                        <>
                                          <span style={{ color: "#dc2626" }}>
                                            建议移除: {Array.from(suggestRemovals).join(", ")}
                                          </span>
                                          <button
                                            onClick={() => {
                                              setFactorsToRemove(prev => {
                                                const next = new Set(prev);
                                                suggestRemovals.forEach(n => next.add(n));
                                                return next;
                                              });
                                            }}
                                            style={{
                                              padding: "2px 8px", fontSize: "10px", fontWeight: 600, borderRadius: "3px", cursor: "pointer",
                                              backgroundColor: "#fee2e2", color: "#dc2626", border: "1px solid #fecaca",
                                            }}
                                          >一键应用建议</button>
                                        </>
                                      )}
                                    </div>
                                  )}
                                </div>
                              </>
                            );
                          })()}
                        </div>
                      )}

                      {/* SOTA 模型摘要 */}
                      {sotaPreview.sota_models?.length > 0 && (
                        <div style={{ padding: "6px 14px 10px", borderTop: "1px solid #e2e8f0" }}>
                          {sotaPreview.sota_models.slice(0, 3).map((m: any) => (
                            <div key={m.model_id} style={{ color: "#475569", paddingLeft: "8px", fontSize: "11px" }}>
                              模型: {m.model_name || m.model_id} (IC: {m.ic?.toFixed(4) ?? "N/A"})
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  )}

                  {/* Mixed Task: 默认配置摘要 + 可选自定义按钮 */}
                  {detectedTaskType === "mixed" && sotaPreview && (
                    <div style={{ padding: "10px 14px", borderRadius: "6px", backgroundColor: "#fffbeb", border: "1px solid #fde68a", fontSize: "12px" }}>
                      <div style={{ fontWeight: 600, color: "#92400e", marginBottom: "6px" }}>默认配置（可直接提交）</div>
                      <div style={{ color: "#78350f" }}>
                        因子: 使用 {sotaPreview.total_sota_factors} 个 SOTA 因子
                        <button
                          onClick={() => {
                            if (!showCustomizeFactors) {
                              // 开启自定义时，预选 SOTA 因子
                              const keys = new Set<string>(
                                (sotaPreview.sota_factors || []).map((f: any) => `${f.factor_name}||${f.source}`)
                              );
                              setSelectedFactorsForEvo(keys);
                            } else {
                              setSelectedFactorsForEvo(new Set());
                            }
                            setShowCustomizeFactors(!showCustomizeFactors);
                          }}
                          style={{ marginLeft: "8px", fontSize: "11px", color: "#2563eb", background: "none", border: "none", cursor: "pointer", textDecoration: "underline" }}
                        >{showCustomizeFactors ? "取消自定义" : "自定义因子"}</button>
                      </div>
                      <div style={{ color: "#78350f", marginTop: "2px" }}>
                        模型: {sotaPreview.sota_models?.[0]?.model_name || sotaPreview.sota_models?.[0]?.model_id || "N/A"}
                        {sotaPreview.sota_models?.[0]?.ic != null && ` (IC: ${sotaPreview.sota_models[0].ic.toFixed(4)})`}
                        <button
                          onClick={() => {
                            if (!showCustomizeModel) {
                              setSelectedModelForEvo(sotaPreview.sota_models?.[0]?.model_id || "");
                            } else {
                              setSelectedModelForEvo("");
                            }
                            setShowCustomizeModel(!showCustomizeModel);
                          }}
                          style={{ marginLeft: "8px", fontSize: "11px", color: "#2563eb", background: "none", border: "none", cursor: "pointer", textDecoration: "underline" }}
                        >{showCustomizeModel ? "取消更换" : "更换模型"}</button>
                      </div>
                    </div>
                  )}

                  {/* Alpha 补充选项 - Factor Task 或 Mixed(未自定义因子时) */}
                  {(detectedTaskType === "factor" || (detectedTaskType === "mixed" && !showCustomizeFactors)) && sotaPreview && (
                    <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                      <input
                        type="checkbox"
                        id="include_alpha"
                        checked={newTask.include_alpha_baseline}
                        onChange={e => setNewTask({ ...newTask, include_alpha_baseline: e.target.checked })}
                      />
                      <label htmlFor="include_alpha" style={{ fontSize: "13px", color: "#475569" }}>补充 Alpha158/360 基准因子</label>
                    </div>
                  )}

                  {/* 选择面板: 模型 — Factor Task(必选) 或 Mixed(可选自定义) */}
                  {(detectedTaskType === "factor" || (detectedTaskType === "mixed" && showCustomizeModel)) && (
                    <div style={{
                      border: detectedTaskType === "factor" ? "2px solid #3b82f6" : "2px solid #94a3b8", borderRadius: "8px", overflow: "hidden",
                    }}>
                      <div style={{ padding: "8px 12px", backgroundColor: detectedTaskType === "factor" ? "#eff6ff" : "#f8fafc", borderBottom: "1px solid #e2e8f0", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                        <span style={{ fontSize: "13px", fontWeight: 700, color: detectedTaskType === "factor" ? "#1e40af" : "#475569" }}>
                          {detectedTaskType === "factor" ? "选择模型" : "更换模型"} {detectedTaskType === "factor" && <span style={{ color: "#ef4444" }}>*</span>}
                        </span>
                        {selectedModelForEvo && (
                          <span style={{ fontSize: "12px", color: "#2563eb", fontWeight: 600 }}>
                            已选: {selectedModelForEvo.length > 30 ? selectedModelForEvo.slice(0, 30) + "..." : selectedModelForEvo}
                          </span>
                        )}
                      </div>
                      <div style={{ maxHeight: "400px", overflow: "auto" }}>
                        <ModelList
                          mode="selection"
                          selectedModel={selectedModelForEvo || null}
                          onSelectModel={(modelId: string) => setSelectedModelForEvo(modelId)}
                        />
                      </div>
                      {detectedTaskType === "factor" && !selectedModelForEvo && (
                        <div style={{ padding: "8px 12px", backgroundColor: "#fef2f2", borderTop: "1px solid #fecaca", fontSize: "12px", color: "#dc2626" }}>
                          请从上方列表中选择一个模型
                        </div>
                      )}
                    </div>
                  )}

                  {/* 选择面板: 因子 — Model Task(必选) 或 Mixed(可选自定义) */}
                  {(detectedTaskType === "model" || (detectedTaskType === "mixed" && showCustomizeFactors)) && (
                    <div style={{
                      border: detectedTaskType === "model" ? "2px solid #10b981" : "2px solid #94a3b8", borderRadius: "8px", overflow: "hidden",
                    }}>
                      <div style={{ padding: "8px 12px", backgroundColor: detectedTaskType === "model" ? "#ecfdf5" : "#f8fafc", borderBottom: "1px solid #e2e8f0", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                        <span style={{ fontSize: "13px", fontWeight: 700, color: detectedTaskType === "model" ? "#065f46" : "#475569" }}>
                          {detectedTaskType === "model" ? "选择因子" : "自定义因子"} {detectedTaskType === "model" && !(sotaPreview?.total_task_factors > 0) && <span style={{ color: "#ef4444" }}>*</span>}
                        </span>
                        {selectedFactorsForEvo.size > 0 && (
                          <span style={{ fontSize: "12px", color: "#059669", fontWeight: 600 }}>
                            已选 {selectedFactorsForEvo.size} 个因子
                          </span>
                        )}
                      </div>
                      <div style={{ maxHeight: "400px", overflow: "auto" }}>
                        <FactorList
                          mode="selection"
                          selectedFactors={selectedFactorsForEvo}
                          onFactorSelect={(selected: Set<string>) => setSelectedFactorsForEvo(selected)}
                          cacheContext={{
                            experimentId: newTask.base_experiment_id || null,
                            trainStart: null,
                            backtestEnd: null,
                          }}
                        />
                      </div>
                      {detectedTaskType === "model" && selectedFactorsForEvo.size === 0 && (
                        (sotaPreview?.total_task_factors || 0) > 0 ? (
                          <div style={{ padding: "8px 12px", backgroundColor: "#f0fdf4", borderTop: "1px solid #bbf7d0", fontSize: "12px", color: "#166534" }}>
                            此 Task 已有 {sotaPreview.total_task_factors} 个演进因子，可直接创建任务（也可手动选择因子覆盖）
                          </div>
                        ) : (
                          <div style={{ padding: "8px 12px", backgroundColor: "#fef2f2", borderTop: "1px solid #fecaca", fontSize: "12px", color: "#dc2626" }}>
                            请从上方列表中至少选择一个因子
                          </div>
                        )
                      )}
                    </div>
                  )}
                </>
              )}

              {/* 已有演进 Loop (Fork) */}
              {newTask.source_type === "evolution_fork" && (
                <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
                  <div>
                    <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>选择源演进任务</label>
                    <select
                      value={newTask.fork_from_task_id}
                      onChange={async (e) => {
                        const tid = e.target.value;
                        setNewTask({ ...newTask, fork_from_task_id: tid, fork_from_loop_index: -1 });
                        setForkSourceLoops([]);
                        if (tid) {
                          try {
                            const res = await fetch(`${API}/quantevolver/evolution/tasks/${tid}`);
                            if (!res.ok) throw new Error(`HTTP ${res.status}`);
                            const data = await res.json();
                            const taskData = data?.data || data;
                            if (taskData?.loops) {
                              const completed = taskData.loops.filter((l: any) => l.status === "completed");
                              setForkSourceLoops(completed);
                            } else {
                              alert("该任务没有已完成的 Loop 数据");
                            }
                          } catch (e: any) {
                            console.error("Failed to fetch task loops:", e);
                            alert(`加载源任务 Loop 列表失败: ${e?.message || "未知错误"}`);
                          }
                        }
                      }}
                      style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box" }}
                    >
                      <option value="">-- 请选择已有的演进任务 --</option>
                      {tasks.map(t => (
                        <option key={t.task_id} value={t.task_id}>
                          {t.task_name} ({t.task_id.slice(0, 20)}) — Loop {t.current_loop}/{t.max_loops} [{t.status}]
                        </option>
                      ))}
                    </select>
                  </div>
                  {forkSourceLoops.length > 0 && (
                    <div>
                      <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>选择从哪个 Loop 开始</label>
                      <select
                        value={newTask.fork_from_loop_index}
                        onChange={e => setNewTask({ ...newTask, fork_from_loop_index: parseInt(e.target.value) })}
                        style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box" }}
                      >
                        <option value={-1}>-- 请选择已完成的 Loop --</option>
                        {forkSourceLoops.map((l: any) => {
                          const raw = l.metrics_json || {};
                          const m = typeof raw === "string" ? (() => { try { return JSON.parse(raw); } catch { return {}; } })() : raw;
                          const ic = typeof m === "object" && m !== null ? m.IC : null;
                          return (
                            <option key={l.loop_index} value={l.loop_index}>
                              Loop {l.loop_index} — {l.action_type || "initial"}
                              {ic != null ? ` (IC: ${Number(ic).toFixed(4)})` : ""}
                              {l.is_sota ? " ★SOTA" : ""}
                            </option>
                          );
                        })}
                      </select>
                    </div>
                  )}
                  <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                    <input type="checkbox" id="create-fork-inherit" checked={newTask.inherit_history}
                      onChange={e => setNewTask({ ...newTask, inherit_history: e.target.checked })}
                    />
                    <label htmlFor="create-fork-inherit" style={{ fontSize: "13px", color: "#475569", cursor: "pointer" }}>
                      继承截止到该 Loop 的演进历史（Agent 可参考过去经验避免重复探索）
                    </label>
                  </div>
                </div>
              )}

              <div>
                <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>演进目标描述 (给 Agent 的指引)</label>
                <textarea
                  value={newTask.target_desc}
                  onChange={e => setNewTask({...newTask, target_desc: e.target.value})}
                  placeholder="例如: 尝试提升 ICIR，降低多头波动率，重点探索树模型的深度参数..."
                  rows={3}
                  style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box", resize: "vertical" }}
                />
              </div>

              {/* ── 自定义添加因子（从因子库） ── */}
              <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", overflow: "hidden", backgroundColor: "#f8fafc" }}>
                <button
                  type="button"
                  onClick={() => setShowFactorLibrary(!showFactorLibrary)}
                  style={{
                    width: "100%", padding: "12px 16px", display: "flex", justifyContent: "space-between", alignItems: "center",
                    background: "none", border: "none", cursor: "pointer", fontSize: "13px", fontWeight: 700, color: "#334155",
                  }}
                >
                  <span>
                    自定义添加因子（从因子库）
                    {additionalFactorKeys.size > 0 && (
                      <span style={{ marginLeft: "8px", fontSize: "12px", color: "#059669", fontWeight: 600 }}>
                        已添加 {additionalFactorKeys.size} 个因子
                      </span>
                    )}
                  </span>
                  <span style={{ fontSize: "12px", color: "#94a3b8" }}>{showFactorLibrary ? "▲" : "▼"}</span>
                </button>
                {showFactorLibrary && (
                  <div style={{ borderTop: "1px solid #e2e8f0" }}>
                    <div style={{ padding: "8px 16px", backgroundColor: "#eff6ff", fontSize: "12px", color: "#1e40af" }}>
                      从因子库中选择额外因子，这些因子将与来源默认因子合并
                    </div>
                    <div style={{ maxHeight: "400px", overflow: "auto" }}>
                      <FactorList
                        mode="selection"
                        selectedFactors={additionalFactorKeys}
                        onFactorSelect={(selected: Set<string>) => setAdditionalFactorKeys(selected)}
                        cacheContext={{
                          experimentId: newTask.base_experiment_id || null,
                          trainStart: null,
                          backtestEnd: null,
                        }}
                      />
                    </div>
                  </div>
                )}
              </div>

              {/* 交易策略 & 执行算法选择 */}
              <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", padding: "16px", backgroundColor: "#f8fafc" }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: "#334155", marginBottom: "12px" }}>交易策略 & 执行算法（可选）</div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px", marginBottom: "12px" }}>
                  <div>
                    <label style={{ display: "block", fontSize: "12px", fontWeight: 600, color: "#64748b", marginBottom: "4px" }}>交易策略</label>
                    <select
                      value={newTask.strategy_id}
                      onChange={e => setNewTask({ ...newTask, strategy_id: e.target.value, strategy_params: {} })}
                      style={{ width: "100%", padding: "7px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px", boxSizing: "border-box", backgroundColor: "white" }}
                    >
                      <option value="">默认（继承基础实验）</option>
                      {strategyCatalog.map(s => (
                        <option key={s.strategy_id} value={s.strategy_id}>{s.display_name || s.strategy_id}</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label style={{ display: "block", fontSize: "12px", fontWeight: 600, color: "#64748b", marginBottom: "4px" }}>日内执行算法</label>
                    <select
                      value={newTask.execution_algo}
                      onChange={e => {
                        const algoCode = e.target.value;
                        const algoInfo = executionAlgoCatalog.find((a: any) => a.algo_code === algoCode);
                        const defaults = algoInfo?.default_config || {};
                        setNewTask({ ...newTask, execution_algo: algoCode, execution_algo_params: defaults });
                      }}
                      style={{ width: "100%", padding: "7px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px", boxSizing: "border-box", backgroundColor: "white" }}
                    >
                      {renderQeExecutionAlgoOptions(newTask.execution_algo, "默认（TWAP）")}
                    </select>
                  </div>
                </div>
                {/* 策略参数图形化编辑 */}
                {!newTask.strategy_id ? (
                  <div style={{ marginTop: "8px" }}>
                    <div style={{ fontSize: "12px", fontWeight: 600, color: "#64748b", marginBottom: "6px" }}>策略参数（默认 TopkDropoutStrategy）</div>
                    <ParamSchemaForm
                      schema={{
                        topk: { type: "integer", default: 50, minimum: 1, maximum: 200, description: "持仓股票数" },
                        n_drop: { type: "integer", default: 5, minimum: 0, maximum: 50, description: "每期替换数" },
                        hold_thresh: { type: "integer", default: 2, minimum: 1, maximum: 30, description: "持有期（天）" },
                        risk_degree: { type: "number", default: 0.95, minimum: 0.1, maximum: 1.0, description: "仓位比例" },
                        method_buy: { type: "string", default: "top", enum: ["top", "random"], description: "买入方式" },
                        method_sell: { type: "string", default: "bottom", enum: ["bottom", "random"], description: "卖出方式" },
                        only_tradable: { type: "boolean", default: true, description: "仅可交易标的" },
                        forbid_all_trade_at_limit: { type: "boolean", default: false, description: "涨跌停禁止交易" },
                        initial_cash: { type: "integer", default: 100000000, minimum: 100000, description: "初始资金（元）" },
                      }}
                      values={newTask.strategy_params}
                      onChange={(key, val) => setNewTask(prev => ({ ...prev, strategy_params: { ...prev.strategy_params, [key]: val } }))}
                    />
                  </div>
                ) : (
                  <div style={{ marginTop: "8px" }}>
                    <div style={{ padding: "8px 12px", backgroundColor: "#f1f5f9", borderRadius: "6px", fontSize: "12px", color: "#64748b", marginBottom: "8px" }}>
                      自定义策略参数请在策略管理页面的 portfolio_config 中配置
                    </div>
                    <ParamSchemaForm
                      schema={{
                        initial_cash: { type: "integer", default: 100000000, minimum: 100000, description: "初始资金（元）" },
                      }}
                      values={newTask.strategy_params}
                      onChange={(key, val) => setNewTask(prev => ({ ...prev, strategy_params: { ...prev.strategy_params, [key]: val } }))}
                    />
                  </div>
                )}

                {/* 执行算法参数图形化编辑 */}
                {newTask.execution_algo && (() => {
                  const algoInfo = executionAlgoCatalog.find((a: any) => a.algo_code === newTask.execution_algo);
                  const schema = algoInfo?.param_schema?.properties;
                  if (!schema || Object.keys(schema).length === 0) return null;
                  return (
                    <div style={{ marginTop: "8px" }}>
                      <div style={{ fontSize: "12px", fontWeight: 600, color: "#64748b", marginBottom: "6px" }}>执行算法参数（{algoInfo?.algo_name || newTask.execution_algo}）</div>
                      <ParamSchemaForm
                        schema={schema}
                        values={newTask.execution_algo_params}
                        onChange={(key, val) => setNewTask(prev => ({ ...prev, execution_algo_params: { ...prev.execution_algo_params, [key]: val } }))}
                      />
                    </div>
                  );
                })()}

                {/* 尾盘涨停未成交资金处理（仅分钟线回测有效） */}
                {newTask.execution_algo !== "CLOSE_PRICE" && (
                  <div style={{ marginTop: "12px", paddingTop: "12px", borderTop: "1px dashed #cbd5e1" }}>
                    <div style={{ fontSize: "12px", fontWeight: 600, color: "#64748b", marginBottom: "6px" }}>
                      尾盘涨停未成交资金处理（仅分钟线回测有效）
                    </div>
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px" }}>
                      <select
                        value={newTask.unfilled_handler || ""}
                        onChange={e => {
                          const handler = e.target.value;
                          setNewTask({
                            ...newTask,
                            unfilled_handler: handler,
                            unfilled_handler_params: handler === "TAIL_SUBSTITUTE" ? { backup_depth: 15 } : {},
                          });
                        }}
                        style={{ width: "100%", padding: "7px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px", boxSizing: "border-box", backgroundColor: "white" }}
                      >
                        <option value="">不处理（资金闲置）</option>
                        <option value="TAIL_BOOST">TAIL_BOOST — 加仓已持有股票（14:50）</option>
                        <option value="TAIL_SUBSTITUTE">TAIL_SUBSTITUTE — 替补买入候选股（14:55）</option>
                      </select>
                      {newTask.unfilled_handler === "TAIL_SUBSTITUTE" && (
                        <input
                          type="number"
                          min={1}
                          max={100}
                          value={newTask.unfilled_handler_params?.backup_depth ?? 15}
                          onChange={e => setNewTask(prev => ({ ...prev, unfilled_handler_params: { ...prev.unfilled_handler_params, backup_depth: Number(e.target.value) || 15 } }))}
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

              <div style={{ border: "1px solid #bae6fd", borderRadius: "8px", padding: "12px", backgroundColor: "#f0f9ff" }}>
                <label style={{ display: "flex", alignItems: "center", gap: "8px", fontSize: "13px", fontWeight: 700, color: "#0369a1" }}>
                  <input
                    type="checkbox"
                    checked={newTask.filter_suspended_on_signal}
                    onChange={e => setNewTask(prev => ({ ...prev, filter_suspended_on_signal: e.target.checked }))}
                  />
                  启用 suspend_d 日频选股停牌过滤
                </label>
                {newTask.filter_suspended_on_signal && (
                  <label style={{ display: "flex", alignItems: "center", gap: "8px", marginTop: "8px", fontSize: "12px", color: "#075985" }}>
                    <input
                      type="checkbox"
                      checked={newTask.suspend_filter_strict}
                      onChange={e => setNewTask(prev => ({ ...prev, suspend_filter_strict: e.target.checked }))}
                    />
                    严格要求 suspend_d 每个回测交易日审计成功
                  </label>
                )}
              </div>

              <div style={{ border: "1px solid #ccfbf1", borderRadius: "8px", padding: "12px", backgroundColor: "#f0fdfa" }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: "#0f766e", marginBottom: "8px" }}>
                  训练标签期限（Label Horizon）
                </div>
                <div style={{ display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
                  {([1, 3, 5, 10, 20] as const).map(h => (
                    <button
                      key={h}
                      type="button"
                      onClick={() => setNewTask(prev => ({ ...prev, label_horizon: h }))}
                      style={{
                        padding: "6px 14px",
                        borderRadius: "6px",
                        border: newTask.label_horizon === h ? "2px solid #0f766e" : "1px solid #99f6e4",
                        backgroundColor: newTask.label_horizon === h ? "#ccfbf1" : "#ffffff",
                        color: newTask.label_horizon === h ? "#0f766e" : "#64748b",
                        fontSize: "13px",
                        fontWeight: 700,
                        cursor: "pointer",
                      }}
                    >
                      {h}d
                    </button>
                  ))}
                  <span style={{ fontSize: "12px", color: "#64748b" }}>
                    自动演进创建后固定该期限，Agent 不允许静默修改。
                  </span>
                </div>
              </div>

              {/* 初始资金 */}
              <div>
                <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>
                  初始资金（元）
                  <span style={{ color: "#94a3b8", fontWeight: 400 }}>（默认 1 亿）</span>
                </label>
                <input
                  type="number"
                  min={100000}
                  step={10000000}
                  value={newTask.strategy_params.initial_cash ?? ""}
                  placeholder="100000000"
                  onChange={e => {
                    const v = parseInt(e.target.value);
                    setNewTask(prev => ({
                      ...prev,
                      strategy_params: {
                        ...prev.strategy_params,
                        initial_cash: isNaN(v) ? undefined : v,
                      }
                    }));
                  }}
                  style={{ width: "240px", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box" }}
                />
              </div>

              <div>
                <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>
                  演进指引 <span style={{ color: "#ef4444" }}>*</span>
                </label>
                <textarea
                  value={newTask.evolution_guidance}
                  onChange={e => setNewTask({ ...newTask, evolution_guidance: e.target.value })}
                  placeholder="描述你希望演进往什么方向探索。例如：优先尝试动量+波动率因子组合，避免过度依赖单一类别..."
                  rows={3}
                  style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box", resize: "vertical" }}
                />
              </div>

              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "16px" }}>
                <div>
                  <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>演进模式</label>
                  <select
                    value={newTask.evolution_mode}
                    onChange={e => setNewTask({ ...newTask, evolution_mode: e.target.value as any })}
                    style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box", backgroundColor: "white" }}
                  >
                    <option value="auto">Auto (自动决策)</option>
                    <option value="factor_only">仅因子调整</option>
                    <option value="model_only">仅模型调整</option>
                    <option value="joint">联合调整</option>
                  </select>
                </div>

                <div>
                  <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>最大演进轮次 (Max Loops)</label>
                <input
                  type="number"
                  aria-label="最大演进轮次"
                  min={1} max={50}
                  value={newTask.max_loops}
                  onChange={e => setNewTask({...newTask, max_loops: parseInt(e.target.value) || 10})}
                  style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box" }}
                />
              </div>

                <div>
                  <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>执行节点</label>
                  <select
                    value={newTask.node_id}
                    onChange={e => setNewTask({ ...newTask, node_id: e.target.value })}
                    style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box", backgroundColor: "white" }}
                  >
                    <option value="">本地节点 (默认)</option>
                    {computeNodes.filter(n => n.node_id !== "wsl2-5080").map(n => (
                      <option key={n.node_id} value={n.node_id}>{n.display_name || n.node_id} ({n.status || "unknown"})</option>
                    ))}
                  </select>
                </div>
            </div>

            <SectorBlacklistPanel
              enabled={blacklistEnabled}
              onEnabledChange={setBlacklistEnabled}
              onPoolPathChange={setStockPoolPath}
            />

            {/* ── HMM 模型选择器 ── */}
            <div style={{ padding: "12px 16px", backgroundColor: "#f8fafc", borderRadius: "8px", border: "1px solid #e2e8f0" }}>
              <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: newTask.enable_sector_hmm ? "12px" : "0" }}>
                <input
                  type="checkbox"
                  id="enable-sector-hmm"
                  checked={newTask.enable_sector_hmm}
                  onChange={e => {
                    const enabled = e.target.checked;
                    setNewTask(prev => ({ ...prev, enable_sector_hmm: enabled, hmm_model_version_id: "" }));
                    setHmmSelectedConfigId("");
                    setHmmSnapshots([]);
                    if (enabled) fetchHmmConfigs();
                  }}
                />
                <label htmlFor="enable-sector-hmm" style={{ fontSize: "13px", fontWeight: 600, color: "#475569" }}>
                  启用行业 HMM 热度调整
                </label>
              </div>
              {newTask.enable_sector_hmm && (
                <>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px" }}>
                  <div>
                    <label style={{ display: "block", fontSize: "12px", fontWeight: 500, color: "#6b7280", marginBottom: "4px" }}>选择配置版本</label>
                    <select
                      value={hmmSelectedConfigId}
                      onChange={e => {
                        const cid = e.target.value;
                        setHmmSelectedConfigId(cid);
                        setNewTask(prev => ({ ...prev, hmm_model_version_id: "" }));
                        fetchHmmSnapshots(cid);
                      }}
                      style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px", boxSizing: "border-box", backgroundColor: "white" }}
                    >
                      <option value="">-- 选择配置 --</option>
                      {hmmConfigs.map((c: any) => (
                        <option key={c.config_id} value={c.config_id}>{c.display_name}</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label style={{ display: "block", fontSize: "12px", fontWeight: 500, color: "#6b7280", marginBottom: "4px" }}>选择时间快照</label>
                    <select
                      value={newTask.hmm_model_version_id}
                      onChange={e => setNewTask(prev => ({ ...prev, hmm_model_version_id: e.target.value }))}
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
                {/* 信号系数档位选择 */}
                <div style={{ marginTop: "12px" }}>
                  <label style={{ display: "block", fontSize: "12px", fontWeight: 500, color: "#6b7280", marginBottom: "6px" }}>信号系数档位</label>
                  <div style={{ display: "flex", gap: "12px" }}>
                    <label style={{ display: "flex", alignItems: "center", gap: "6px", cursor: "pointer", fontSize: "13px", color: "#374151" }}>
                      <input type="radio" name="hmm_preset" value="preset_A" checked={newTask.hmm_signal_preset === "preset_A"} onChange={() => setNewTask(prev => ({ ...prev, hmm_signal_preset: "preset_A" }))} />
                      保守档（热态+5% / 冷态-4%）
                    </label>
                    <label style={{ display: "flex", alignItems: "center", gap: "6px", cursor: "pointer", fontSize: "13px", color: "#374151" }}>
                      <input type="radio" name="hmm_preset" value="preset_B" checked={newTask.hmm_signal_preset === "preset_B"} onChange={() => setNewTask(prev => ({ ...prev, hmm_signal_preset: "preset_B" }))} />
                      激进档（热态+10% / 冷态-8%）
                    </label>
                  </div>
                </div>
                </>
              )}
            </div>
            </>)}

            {/* ═══════════════════════════════════════════════════════ */}
            {/* 自定义演进 (custom_evo) 专属 UI                        */}
            {/* ═══════════════════════════════════════════════════════ */}
            {newTask.source_type === "custom_evo" && (
            <div style={{ display: "flex", flexDirection: "column", gap: "16px" }}>
              {/* 任务描述 */}
              <div>
                <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>任务描述（可选）</label>
                <input type="text" value={newTask.target_desc} onChange={e => setNewTask({...newTask, target_desc: e.target.value})} placeholder="自定义演进任务描述" style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box" }} />
              </div>

              {/* 初始配置来源（快速填充） */}
              <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", padding: "12px", backgroundColor: "#f8fafc" }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: "#334155", marginBottom: "8px" }}>初始配置来源（可选，用于快速填充第一个 Loop）</div>
                <div style={{ display: "flex", gap: "6px", marginBottom: "8px" }}>
                  {([
                    { value: "manual" as const, label: "手工配置" },
                    { value: "qe_experiment" as const, label: "从 QE 实验" },
                    { value: "evolution_loop" as const, label: "从演进 Loop" },
                    { value: "rdagent_task" as const, label: "从 RDAgent Task" },
                  ] as const).map(opt => (
                    <button key={opt.value} onClick={() => {
                      setCustomEvoInitSource(opt.value);
                      if (opt.value === "qe_experiment") fetchSourceExperiments();
                      if (opt.value === "rdagent_task") fetchSourceTasks();
                    }} style={{
                      flex: 1, padding: "6px 8px", borderRadius: "6px", fontSize: "12px", fontWeight: 600, cursor: "pointer",
                      border: customEvoInitSource === opt.value ? "2px solid #8b5cf6" : "1px solid #cbd5e1",
                      backgroundColor: customEvoInitSource === opt.value ? "#f5f3ff" : "#fff",
                      color: customEvoInitSource === opt.value ? "#7c3aed" : "#64748b",
                    }}>{opt.label}</button>
                  ))}
                </div>
                {customEvoInitSource === "qe_experiment" && (
                  <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
                    <select value={customEvoSourceExpId} onChange={e => setCustomEvoSourceExpId(e.target.value)} style={{ flex: 1, padding: "6px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px" }}>
                      <option value="">-- 选择实验 --</option>
                      {sourceExperiments.map(exp => (<option key={exp.experiment_id} value={exp.experiment_id}>{exp.experiment_name || exp.experiment_id.slice(0,12)} (IC:{exp.ic?.toFixed(4) ?? "N/A"}, 年化:{exp.annualized_return != null ? (exp.annualized_return * 100).toFixed(2) + "%" : "N/A"})</option>))}
                    </select>
                    <button onClick={() => { if (customEvoSourceExpId) loadCustomEvoFromExperiment(customEvoSourceExpId); }} disabled={!customEvoSourceExpId} style={{ padding: "6px 12px", backgroundColor: customEvoSourceExpId ? "#8b5cf6" : "#e2e8f0", color: "#fff", border: "none", borderRadius: "6px", fontSize: "12px", cursor: customEvoSourceExpId ? "pointer" : "not-allowed" }}>加载</button>
                  </div>
                )}
                {customEvoInitSource === "evolution_loop" && (
                  <div style={{ display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
                    <select value={customEvoSourceTaskId} onChange={async e => {
                      const tid = e.target.value;
                      setCustomEvoSourceTaskId(tid);
                      setCustomEvoSourceLoopIdx(-1);
                      if (tid) {
                        try {
                          const r = await fetch(`${API}/quantevolver/evolution/tasks/${tid}`);
                          if (r.ok) { const d = await r.json(); setCustomEvoForkLoops((d.data || d).loops?.filter((l: any) => l.status === "completed") || []); }
                          else { alert(`加载任务 Loop 列表失败 (HTTP ${r.status})`); setCustomEvoForkLoops([]); }
                        } catch (e: any) { alert(`加载失败: ${e?.message || "网络错误"}`); setCustomEvoForkLoops([]); }
                      } else { setCustomEvoForkLoops([]); }
                    }} style={{ flex: 1, padding: "6px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px" }}>
                      <option value="">-- 选择演进任务 --</option>
                      {tasks.filter(t => t.status === "completed" || t.current_loop > 0).map(t => (<option key={t.task_id} value={t.task_id}>{t.task_name || t.task_id.slice(0,12)} (L{t.current_loop}/{t.max_loops})</option>))}
                    </select>
                    <select value={customEvoSourceLoopIdx} onChange={e => setCustomEvoSourceLoopIdx(parseInt(e.target.value))} style={{ width: "120px", padding: "6px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px" }}>
                      <option value={-1}>-- Loop --</option>
                      {customEvoForkLoops.map((l: any) => (<option key={l.loop_index} value={l.loop_index}>Loop {l.loop_index} {l.is_sota ? "(SOTA)" : ""}</option>))}
                    </select>
                    <button onClick={() => { if (customEvoSourceTaskId && customEvoSourceLoopIdx >= 0) loadCustomEvoFromEvolutionLoop(customEvoSourceTaskId, customEvoSourceLoopIdx); }} disabled={!customEvoSourceTaskId || customEvoSourceLoopIdx < 0} style={{ padding: "6px 12px", backgroundColor: (customEvoSourceTaskId && customEvoSourceLoopIdx >= 0) ? "#8b5cf6" : "#e2e8f0", color: "#fff", border: "none", borderRadius: "6px", fontSize: "12px", cursor: (customEvoSourceTaskId && customEvoSourceLoopIdx >= 0) ? "pointer" : "not-allowed" }}>加载</button>
                  </div>
                )}
                {customEvoInitSource === "rdagent_task" && (
                  <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
                    <select value={customEvoSourceTaskId} onChange={e => setCustomEvoSourceTaskId(e.target.value)} style={{ flex: 1, padding: "6px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px" }}>
                      <option value="">-- 选择 Task --</option>
                      {sourceTasks.map(t => (<option key={t.task_id} value={t.task_id}>{t.task_id.slice(0,12)} ({t.total_sota_factors}F/{t.total_sota_models}M)</option>))}
                    </select>
                    <button onClick={async () => {
                      if (!customEvoSourceTaskId) return;
                      try {
                        const r = await fetch(`${API}/quantevolver/source-tasks/${customEvoSourceTaskId}/sota-assets`);
                        if (!r.ok) { alert(`加载 SOTA 资产失败 (HTTP ${r.status})`); return; }
                        const d = await r.json();
                        const factors = d.sota_factors || [];
                        const models = d.sota_models || [];
                        if (factors.length === 0 && models.length === 0) { alert("该 Task 没有 SOTA 资产"); return; }
                        const fk = new Set<string>(factors.map((f: any) => `${f.factor_name}||${f.source}`));
                        updateCustomEvoLoop(0, { factor_keys: fk, model_id: models[0]?.model_id || "" });
                        alert(`已加载: ${factors.length} 个因子，${models.length} 个模型`);
                      } catch (e: any) { alert(`加载失败: ${e?.message || "网络错误"}`); }
                    }} disabled={!customEvoSourceTaskId} style={{ padding: "6px 12px", backgroundColor: customEvoSourceTaskId ? "#8b5cf6" : "#e2e8f0", color: "#fff", border: "none", borderRadius: "6px", fontSize: "12px", cursor: customEvoSourceTaskId ? "pointer" : "not-allowed" }}>加载</button>
                  </div>
                )}
              </div>

              {/* ── Loop 配置区 ── */}
              {customEvoLoops.map((loop, loopIdx) => (
                <div key={loopIdx} style={{ border: loopIdx === 0 ? "2px solid #8b5cf6" : "1px solid #e5e7eb", borderRadius: "10px", overflow: "hidden", backgroundColor: "#fff" }}>
                  {/* Loop 头部 */}
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "10px 16px", backgroundColor: loopIdx === 0 ? "#f5f3ff" : "#f8fafc", borderBottom: "1px solid #e5e7eb", cursor: loopIdx > 0 ? "pointer" : "default" }}
                    onClick={() => { if (loopIdx > 0) updateCustomEvoLoop(loopIdx, { collapsed: !loop.collapsed }); }}>
                    <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                      <span style={{ fontSize: "14px", fontWeight: 700, color: loopIdx === 0 ? "#7c3aed" : "#334155" }}>
                        {loopIdx === 0 ? "Loop 1（基准配置）" : `Loop ${loopIdx + 1}`}
                      </span>
                      <input type="text" value={loop.label} onChange={e => { e.stopPropagation(); updateCustomEvoLoop(loopIdx, { label: e.target.value }); }} onClick={e => e.stopPropagation()} placeholder="标签（可选）" style={{ padding: "3px 8px", borderRadius: "4px", border: "1px solid #cbd5e1", fontSize: "12px", width: "160px" }} />
                      {loopIdx > 0 && <span style={{ fontSize: "11px", color: "#94a3b8" }}>
                        因子:{loop.factor_keys.size} | 模型:{loop.model_id ? loop.model_id.slice(0,12) : "未选"}
                      </span>}
                    </div>
                    <div style={{ display: "flex", gap: "6px", alignItems: "center" }}>
                      {loopIdx > 0 && <span style={{ fontSize: "12px", color: "#94a3b8" }}>{loop.collapsed ? "▼" : "▲"}</span>}
                      {loopIdx > 0 && <button onClick={e => { e.stopPropagation(); removeCustomEvoLoop(loopIdx); }} style={{ padding: "3px 8px", backgroundColor: "#fee2e2", color: "#dc2626", border: "none", borderRadius: "4px", fontSize: "11px", cursor: "pointer" }}>删除</button>}
                    </div>
                  </div>

                  {/* Loop 内容（可折叠） */}
                  {!(loopIdx > 0 && loop.collapsed) && (
                  <div style={{ padding: "16px", display: "flex", flexDirection: "column", gap: "12px" }}>
                    {/* backtest-only 模式开关 */}
                    {loop.model_source_task_id && (
                      <div style={{ padding: "10px 14px", backgroundColor: loop.backtest_only ? "#eff6ff" : "#f8fafc", borderRadius: "8px", border: `1px solid ${loop.backtest_only ? "#93c5fd" : "#e2e8f0"}` }}>
                        <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                          <input type="checkbox" checked={loop.backtest_only} onChange={e => {
                            if (e.target.checked && loop._source_factor_keys.size > 0) {
                              const srcNames = new Set([...loop._source_factor_keys].map(k => k.split("||")[0]));
                              const curNames = new Set([...loop.factor_keys].map(k => k.split("||")[0]));
                              const changed = srcNames.size !== curNames.size || [...srcNames].some(n => !curNames.has(n));
                              if (changed) { alert("因子已变更，无法启用 backtest-only 模式。请先恢复原因子配置。"); return; }
                            }
                            updateCustomEvoLoop(loopIdx, { backtest_only: e.target.checked });
                          }} />
                          <label style={{ fontSize: "12px", fontWeight: 600, color: loop.backtest_only ? "#1e40af" : "#64748b" }}>
                            Backtest-only（跳过训练，复用已有模型）
                          </label>
                        </div>
                        <div style={{ fontSize: "11px", color: "#94a3b8", marginTop: "4px", marginLeft: "24px" }}>
                          复用模型自 {loop.model_source_task_id}/Loop{loop.model_source_loop_index ?? "?"}
                          {loop.backtest_only && " — 因子不可变更"}
                        </div>
                      </div>
                    )}
                    {/* 因子选择 */}
                    <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", overflow: "hidden" }}>
                      <div style={{ padding: "8px 12px", backgroundColor: "#f0fdf4", fontSize: "12px", fontWeight: 600, color: "#166534", display: "flex", justifyContent: "space-between" }}>
                        <span>因子选择（已选 {loop.factor_keys.size} 个）</span>
                      </div>
                      <div style={{ maxHeight: "250px", overflow: "auto" }}>
                        <FactorList mode="selection" selectedFactors={loop.factor_keys} onFactorSelect={(selected: Set<string>) => updateCustomEvoLoop(loopIdx, { factor_keys: selected })} cacheContext={{ experimentId: customEvoSourceExpId || null, trainStart: loop.data_split?.train_start || null, backtestEnd: loop.data_split?.backtest_end || loop.data_split?.test_end || null }} />
                      </div>
                    </div>

                    {/* 模型选择 */}
                    <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", overflow: "hidden" }}>
                      <div style={{ padding: "8px 12px", backgroundColor: "#eff6ff", fontSize: "12px", fontWeight: 600, color: "#1e40af" }}>
                        模型选择{loop.model_id ? ` — 已选: ${loop.model_id}` : "（必选）"}
                      </div>
                      <div style={{ maxHeight: "200px", overflow: "auto" }}>
                        <ModelList mode="selection" selectedModel={loop.model_id} onSelectModel={(modelId: string) => updateCustomEvoLoop(loopIdx, { model_id: modelId })} />
                      </div>
                    </div>

                    <div style={{ border: "1px solid #ccfbf1", borderRadius: "8px", padding: "10px", backgroundColor: loop.backtest_only ? "#f8fafc" : "#f0fdfa" }}>
                      <div style={{ fontSize: "12px", fontWeight: 700, color: "#0f766e", marginBottom: "6px" }}>
                        训练标签期限（Label Horizon）
                        {loop.backtest_only && <span style={{ color: "#64748b", fontWeight: 500 }}> - backtest-only 锁定源模型</span>}
                      </div>
                      <div style={{ display: "flex", gap: "6px", flexWrap: "wrap", alignItems: "center" }}>
                        {([1, 3, 5, 10, 20] as const).map(h => (
                          <button
                            key={h}
                            type="button"
                            disabled={loop.backtest_only}
                            onClick={() => updateCustomEvoLoop(loopIdx, { label_horizon: h })}
                            style={{
                              padding: "5px 10px",
                              borderRadius: "5px",
                              border: loop.label_horizon === h ? "2px solid #0f766e" : "1px solid #99f6e4",
                              backgroundColor: loop.label_horizon === h ? "#ccfbf1" : "#ffffff",
                              color: loop.label_horizon === h ? "#0f766e" : "#64748b",
                              fontSize: "12px",
                              fontWeight: 700,
                              cursor: loop.backtest_only ? "not-allowed" : "pointer",
                              opacity: loop.backtest_only && loop.label_horizon !== h ? 0.45 : 1,
                            }}
                          >
                            {h}d
                          </button>
                        ))}
                        <span style={{ fontSize: "11px", color: "#64748b" }}>
                          选择 5d/10d/20d 时建议同步调整 hold_thresh，避免高 IC 无法转化为收益。
                        </span>
                      </div>
                    </div>

                    {/* 策略 + 执行算法 */}
                    <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", padding: "12px", backgroundColor: "#f8fafc" }}>
                      <div style={{ fontSize: "12px", fontWeight: 700, color: "#334155", marginBottom: "8px" }}>策略 & 执行算法</div>
                      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "8px", marginBottom: "8px" }}>
                        <div>
                          <label style={{ display: "block", fontSize: "11px", color: "#64748b", marginBottom: "2px" }}>交易策略</label>
                          <select value={loop.strategy_id} onChange={e => updateCustomEvoLoop(loopIdx, { strategy_id: e.target.value, strategy_params: {} })} style={{ width: "100%", padding: "5px 8px", borderRadius: "4px", border: "1px solid #cbd5e1", fontSize: "12px" }}>
                            <option value="">默认 TopkDropoutStrategy</option>
                            {strategyCatalog.map(s => (<option key={s.strategy_id} value={s.strategy_id}>{s.display_name || s.strategy_id}</option>))}
                          </select>
                        </div>
                        <div>
                          <label style={{ display: "block", fontSize: "11px", color: "#64748b", marginBottom: "2px" }}>执行算法</label>
                          <select value={loop.execution_algo} onChange={e => {
                            const code = e.target.value;
                            const info = executionAlgoCatalog.find((a: any) => a.algo_code === code);
                            updateCustomEvoLoop(loopIdx, { execution_algo: code, execution_algo_params: info?.default_config || {} });
                          }} style={{ width: "100%", padding: "5px 8px", borderRadius: "4px", border: "1px solid #cbd5e1", fontSize: "12px" }}>
                            {renderQeExecutionAlgoOptions(loop.execution_algo, "默认 TWAP")}
                          </select>
                        </div>
                      </div>
                      {/* 策略参数 */}
                      <ParamSchemaForm
                        schema={{
                          topk: { type: "integer", default: 50, minimum: 1, maximum: 200, description: "持仓股票数" },
                          n_drop: { type: "integer", default: 5, minimum: 0, maximum: 50, description: "每期替换数" },
                          hold_thresh: { type: "integer", default: 2, minimum: 1, maximum: 30, description: "持有期（天）" },
                          risk_degree: { type: "number", default: 0.95, minimum: 0.1, maximum: 1.0, description: "仓位比例" },
                        }}
                        values={loop.strategy_params}
                        onChange={(key: string, val: any) => updateCustomEvoLoop(loopIdx, { strategy_params: { ...loop.strategy_params, [key]: val } })}
                      />
                      {/* 尾盘处理 */}
                      {loop.execution_algo !== "CLOSE_PRICE" && (
                        <div style={{ marginTop: "8px", display: "grid", gridTemplateColumns: "1fr 1fr", gap: "8px" }}>
                          <div>
                            <label style={{ display: "block", fontSize: "11px", color: "#64748b", marginBottom: "2px" }}>尾盘处理</label>
                            <select value={loop.unfilled_handler} onChange={e => updateCustomEvoLoop(loopIdx, { unfilled_handler: e.target.value, unfilled_handler_params: e.target.value === "TAIL_SUBSTITUTE" ? { backup_depth: 15 } : {} })} style={{ width: "100%", padding: "5px 8px", borderRadius: "4px", border: "1px solid #cbd5e1", fontSize: "12px" }}>
                              <option value="">不处理</option>
                              <option value="TAIL_BOOST">TAIL_BOOST</option>
                              <option value="TAIL_SUBSTITUTE">TAIL_SUBSTITUTE</option>
                            </select>
                          </div>
                          {loop.unfilled_handler === "TAIL_SUBSTITUTE" && (
                            <div>
                              <label style={{ display: "block", fontSize: "11px", color: "#64748b", marginBottom: "2px" }}>候选深度</label>
                              <input type="number" min={1} max={100} value={loop.unfilled_handler_params?.backup_depth ?? 15} onChange={e => updateCustomEvoLoop(loopIdx, { unfilled_handler_params: { ...loop.unfilled_handler_params, backup_depth: Number(e.target.value) || 15 } })} style={{ width: "100%", padding: "5px 8px", borderRadius: "4px", border: "1px solid #cbd5e1", fontSize: "12px" }} />
                            </div>
                          )}
                        </div>
                      )}
                      <div style={{ marginTop: "8px", padding: "8px", borderRadius: "6px", backgroundColor: "#f0f9ff", border: "1px solid #bae6fd" }}>
                        <label style={{ display: "flex", alignItems: "center", gap: "6px", fontSize: "12px", color: "#0369a1" }}>
                          <input
                            type="checkbox"
                            checked={loop.filter_suspended_on_signal}
                            onChange={e => updateCustomEvoLoop(loopIdx, { filter_suspended_on_signal: e.target.checked })}
                          />
                          suspend_d 停牌过滤
                        </label>
                        {loop.filter_suspended_on_signal && (
                          <label style={{ display: "flex", alignItems: "center", gap: "6px", marginTop: "6px", fontSize: "11px", color: "#075985" }}>
                            <input
                              type="checkbox"
                              checked={loop.suspend_filter_strict}
                              onChange={e => updateCustomEvoLoop(loopIdx, { suspend_filter_strict: e.target.checked })}
                            />
                            严格审计
                          </label>
                        )}
                      </div>
                      {/* 初始资金 */}
                      <div style={{ marginTop: "8px" }}>
                        <label style={{ display: "block", fontSize: "11px", color: "#64748b", marginBottom: "2px" }}>初始资金（元）</label>
                        <input type="number" min={100000} step={10000000} value={loop.strategy_params.initial_cash ?? ""} placeholder="100000000" onChange={e => { const v = parseInt(e.target.value); updateCustomEvoLoop(loopIdx, { strategy_params: { ...loop.strategy_params, initial_cash: isNaN(v) ? undefined : v } }); }} style={{ width: "160px", padding: "5px 8px", borderRadius: "4px", border: "1px solid #cbd5e1", fontSize: "12px" }} />
                        <span style={{ fontSize: "11px", color: "#94a3b8", marginLeft: "6px" }}>默认 1 亿</span>
                      </div>
                      {/* HMM */}
                      <div style={{ marginTop: "8px" }}>
                        <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                          <input type="checkbox" checked={loop.enable_sector_hmm} onChange={e => {
                            const enabled = e.target.checked;
                            updateCustomEvoLoop(loopIdx, { enable_sector_hmm: enabled, hmm_config_id: "", hmm_model_version_id: "" });
                            if (enabled) fetchHmmConfigs();
                          }} />
                          <label style={{ fontSize: "12px", color: "#64748b" }}>启用 HMM 行业热度</label>
                        </div>
                        {loop.enable_sector_hmm && (
                          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "8px", marginTop: "6px" }}>
                            <div>
                              <label style={{ display: "block", fontSize: "11px", color: "#6b7280", marginBottom: "2px" }}>配置版本</label>
                              <select value={loop.hmm_config_id} onChange={e => {
                                const cid = e.target.value;
                                updateCustomEvoLoop(loopIdx, { hmm_config_id: cid, hmm_model_version_id: "" });
                                fetchHmmSnapshotsForLoop(loopIdx, cid);
                              }} style={{ width: "100%", padding: "4px 6px", borderRadius: "4px", border: "1px solid #cbd5e1", fontSize: "11px" }}>
                                <option value="">-- 选择配置 --</option>
                                {hmmConfigs.map((c: any) => (
                                  <option key={c.config_id} value={c.config_id}>{c.display_name}</option>
                                ))}
                              </select>
                            </div>
                            <div>
                              <label style={{ display: "block", fontSize: "11px", color: "#6b7280", marginBottom: "2px" }}>时间快照</label>
                              <select value={loop.hmm_model_version_id} onChange={e => updateCustomEvoLoop(loopIdx, { hmm_model_version_id: e.target.value })}
                                disabled={!loop.hmm_config_id}
                                style={{ width: "100%", padding: "4px 6px", borderRadius: "4px", border: "1px solid #cbd5e1", fontSize: "11px", backgroundColor: loop.hmm_config_id ? "white" : "#f1f5f9" }}>
                                <option value="">-- 选择快照 --</option>
                                {(customEvoHmmSnapshots[loopIdx] || []).map((s: any) => (
                                  <option key={s.snapshot_id} value={s.snapshot_id}>
                                    {new Date(s.trained_at).toLocaleString("zh-CN")} ({s.sector_count} 行业)
                                  </option>
                                ))}
                              </select>
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                    {/* 行业黑名单 */}
                    <SectorBlacklistPanel
                      enabled={loop.blacklist_enabled}
                      onEnabledChange={enabled => updateCustomEvoLoop(loopIdx, { blacklist_enabled: enabled, stock_pool: enabled ? loop.stock_pool : "" })}
                      onPoolPathChange={path => updateCustomEvoLoop(loopIdx, { stock_pool: path || "" })}
                    />
                  </div>
                  )}
                </div>
              ))}

              {/* 添加 Loop + 执行设置 */}
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <button onClick={addCustomEvoLoop} style={{ padding: "8px 16px", backgroundColor: "#8b5cf6", color: "#fff", border: "none", borderRadius: "6px", fontSize: "13px", fontWeight: 600, cursor: "pointer" }}>
                  + 添加 Loop（继承 Loop 1 配置）
                </button>
                <span style={{ fontSize: "12px", color: "#94a3b8" }}>共 {customEvoLoops.length} 个 Loop</span>
              </div>

              {/* 执行方式 + 节点 */}
              <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", padding: "12px", backgroundColor: "#f8fafc" }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: "#334155", marginBottom: "8px" }}>执行设置</div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "12px" }}>
                  <div>
                    <label style={{ display: "block", fontSize: "12px", fontWeight: 600, color: "#64748b", marginBottom: "4px" }}>执行方式</label>
                    <div style={{ display: "flex", gap: "6px" }}>
                      <button onClick={() => setCustomEvoExecutionMode("serial")} style={{ flex: 1, padding: "6px", borderRadius: "6px", fontSize: "12px", fontWeight: 600, cursor: "pointer", border: customEvoExecutionMode === "serial" ? "2px solid #8b5cf6" : "1px solid #cbd5e1", backgroundColor: customEvoExecutionMode === "serial" ? "#f5f3ff" : "#fff", color: customEvoExecutionMode === "serial" ? "#7c3aed" : "#64748b" }}>串行</button>
                      <button onClick={() => setCustomEvoExecutionMode("parallel")} style={{ flex: 1, padding: "6px", borderRadius: "6px", fontSize: "12px", fontWeight: 600, cursor: "pointer", border: customEvoExecutionMode === "parallel" ? "2px solid #8b5cf6" : "1px solid #cbd5e1", backgroundColor: customEvoExecutionMode === "parallel" ? "#f5f3ff" : "#fff", color: customEvoExecutionMode === "parallel" ? "#7c3aed" : "#64748b" }}>并行</button>
                    </div>
                  </div>
                  {customEvoExecutionMode === "parallel" && (
                    <div>
                      <label style={{ display: "block", fontSize: "12px", fontWeight: 600, color: "#64748b", marginBottom: "4px" }}>并行度</label>
                      <select value={customEvoParallelism} onChange={e => setCustomEvoParallelism(parseInt(e.target.value))} style={{ width: "100%", padding: "6px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px" }}>
                        <option value={2}>2</option>
                        <option value={4}>4</option>
                        <option value={6}>6</option>
                        <option value={8}>8</option>
                      </select>
                    </div>
                  )}
                  <div>
                    <label style={{ display: "block", fontSize: "12px", fontWeight: 600, color: "#64748b", marginBottom: "4px" }}>执行节点</label>
                    <select value={customEvoNodeId} onChange={e => setCustomEvoNodeId(e.target.value)} style={{ width: "100%", padding: "6px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px" }}>
                      <option value="">本地节点 (默认)</option>
                      {computeNodes.filter(n => n.node_id !== "wsl2-5080").map(n => (
                        <option key={n.node_id} value={n.node_id}>{n.display_name || n.node_id} ({n.status || "unknown"})</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label style={{ display: "block", fontSize: "12px", fontWeight: 600, color: "#64748b", marginBottom: "4px" }}>引擎模式</label>
                    <div style={{ padding: "6px 10px", borderRadius: "6px", border: "1px solid #0ea5e9", backgroundColor: "#f0f9ff", fontSize: "12px", fontWeight: 600, color: "#0284c7", textAlign: "center" }}>统一引擎</div>
                  </div>
                </div>
              </div>
            </div>
            )}

            <div style={{ display: "flex", justifyContent: "flex-end", gap: "12px", marginTop: "24px", paddingTop: "16px", borderTop: "1px solid #f1f5f9" }}>
              <button
                onClick={() => setShowCreateTask(false)}
                style={{ padding: "8px 16px", backgroundColor: "#f1f5f9", color: "#475569", border: "none", borderRadius: "6px", fontSize: "14px", fontWeight: 600, cursor: "pointer" }}
              >
                取消
              </button>
              <button
                onClick={handleCreateTask}
                disabled={isCreating}
                style={{ padding: "8px 16px", backgroundColor: "#2563eb", color: "#fff", border: "none", borderRadius: "6px", fontSize: "14px", fontWeight: 600, cursor: isCreating ? "not-allowed" : "pointer", opacity: isCreating ? 0.7 : 1 }}
              >
                {isCreating ? "创建中..." : (newTask.source_type === "custom_evo" ? `创建自定义演进 (${customEvoLoops.length} Loops)` : "创建并启动演进")}
              </button>
            </div>
          </div>
        </div>
      </div>
      )}

      {/* Resume 弹窗 */}
      {showResumeDialog && (
        <div style={{
          position: "fixed", top: 0, left: 0, right: 0, bottom: 0,
          backgroundColor: "rgba(0,0,0,0.5)", zIndex: 100,
          display: "flex", alignItems: "center", justifyContent: "center",
        }}>
          <div style={{
            backgroundColor: "#fff", padding: "24px", borderRadius: "12px",
            width: "380px", boxShadow: "0 20px 25px -5px rgba(0, 0, 0, 0.1)"
          }}>
            <h2 style={{ margin: "0 0 16px", fontSize: "18px", color: "#1e293b", display: "flex", alignItems: "center", gap: "8px" }}>
              <RotateCcw size={20} color="#16a34a" />
              恢复演进任务
            </h2>
            <p style={{ margin: "0 0 16px", fontSize: "13px", color: "#64748b" }}>
              任务将从上次中断的 Loop 继续。可选增加额外演进轮数。
            </p>
            <div style={{ marginBottom: "16px" }}>
              <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>
                额外增加轮数（0 = 使用原 max_loops）
              </label>
              <input type="number" min={0} max={50} value={additionalLoops}
                onChange={e => setAdditionalLoops(parseInt(e.target.value) || 0)}
                style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box" }}
              />
            </div>
            <div style={{ marginBottom: "16px", padding: "10px 12px", backgroundColor: forceFullTrain ? "#fef3c7" : "#f8fafc", borderRadius: "6px", border: `1px solid ${forceFullTrain ? "#f59e0b" : "#e2e8f0"}` }}>
              <label style={{ display: "flex", alignItems: "center", gap: "8px", cursor: "pointer", fontSize: "13px", color: "#475569" }}>
                <input type="checkbox" checked={forceFullTrain}
                  onChange={e => setForceFullTrain(e.target.checked)}
                  style={{ width: "16px", height: "16px", accentColor: "#f59e0b" }}
                />
                <span style={{ fontWeight: 600 }}>强制完整训练</span>
              </label>
              <p style={{ margin: "4px 0 0 24px", fontSize: "12px", color: "#94a3b8" }}>
                忽略 backtest_only 配置，所有 Loop 重新训练+回测（适用于源模型不可用的场景）
              </p>
            </div>
            <div style={{ display: "flex", justifyContent: "flex-end", gap: "12px" }}>
              <button onClick={() => setShowResumeDialog(null)}
                style={{ padding: "8px 16px", backgroundColor: "#f1f5f9", color: "#475569", border: "none", borderRadius: "6px", fontSize: "14px", fontWeight: 600, cursor: "pointer" }}>
                取消
              </button>
              <button onClick={handleResumeTask}
                style={{ padding: "8px 16px", backgroundColor: "#16a34a", color: "#fff", border: "none", borderRadius: "6px", fontSize: "14px", fontWeight: 600, cursor: "pointer" }}>
                恢复演进
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Fork 弹窗 */}
      {showForkDialog !== null && (
        <div style={{
          position: "fixed", top: 0, left: 0, right: 0, bottom: 0,
          backgroundColor: "rgba(0,0,0,0.5)", zIndex: 100,
          display: "flex", alignItems: "center", justifyContent: "center",
        }}>
          <div style={{
            backgroundColor: "#fff", padding: "24px", borderRadius: "12px",
            width: forkType === "strategy_evo" ? "640px" : "520px",
            maxHeight: "85vh", overflowY: "auto", boxShadow: "0 20px 25px -5px rgba(0, 0, 0, 0.1)"
          }}>
            {/* Fork 类型选择 */}
            <div style={{ display: "flex", gap: "8px", marginBottom: "16px", borderBottom: "1px solid #e5e7eb", paddingBottom: "16px" }}>
              <button
                onClick={() => setForkType("evolution")}
                style={{
                  flex: 1,
                  padding: "10px 16px",
                  borderRadius: "8px",
                  border: forkType === "evolution" ? "2px solid #8b5cf6" : "1px solid #cbd5e1",
                  backgroundColor: forkType === "evolution" ? "#f5f3ff" : "#fff",
                  fontWeight: forkType === "evolution" ? 600 : 400,
                  fontSize: "14px",
                  cursor: "pointer",
                  transition: "all 0.2s",
                  color: forkType === "evolution" ? "#7c3aed" : "#64748b",
                }}>
                普通演进
              </button>
              <button
                onClick={() => {
                  setForkType("strategy_evo");
                  // 初始化一个默认 Loop
                  if (strategyEvoLoops.length === 0) {
                    addStrategyEvoLoop();
                  }
                }}
                style={{
                  flex: 1,
                  padding: "10px 16px",
                  borderRadius: "8px",
                  border: forkType === "strategy_evo" ? "2px solid #f59e0b" : "1px solid #cbd5e1",
                  backgroundColor: forkType === "strategy_evo" ? "#fffbeb" : "#fff",
                  fontWeight: forkType === "strategy_evo" ? 600 : 400,
                  fontSize: "14px",
                  cursor: "pointer",
                  transition: "all 0.2s",
                  color: forkType === "strategy_evo" ? "#d97706" : "#64748b",
                }}>
                策略演进（跳过训练）
              </button>
            </div>

            {forkType === "evolution" ? (
              <>
                {/* 原有普通演进内容 */}
                <h2 style={{ margin: "0 0 16px", fontSize: "18px", color: "#1e293b", display: "flex", alignItems: "center", gap: "8px" }}>
                  <GitMerge size={20} color="#8b5cf6" />
                  以 Loop {showForkDialog} 为基础演进
                </h2>
                <p style={{ margin: "0 0 16px", fontSize: "13px", color: "#64748b" }}>
                  将使用该 Loop 的因子+模型配置创建全新演进任务，Loop 1 为初始回测建立基线。
                </p>
            <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
              <div>
                <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "4px" }}>
                  任务名称
                </label>
                <input type="text" value={forkForm.task_name}
                  onChange={e => setForkForm(f => ({ ...f, task_name: e.target.value }))}
                  style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box" }}
                />
              </div>
              <div style={{ display: "flex", gap: "12px" }}>
                <div style={{ flex: 1 }}>
                  <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "4px" }}>
                    最大轮数
                  </label>
                  <input type="number" min={1} max={50} value={forkForm.max_loops}
                    onChange={e => setForkForm(f => ({ ...f, max_loops: parseInt(e.target.value) || 10 }))}
                    style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box" }}
                  />
                </div>
                <div style={{ flex: 1 }}>
                  <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "4px" }}>
                    演进模式
                  </label>
                  <select value={forkForm.evolution_mode}
                    onChange={e => setForkForm(f => ({ ...f, evolution_mode: e.target.value }))}
                    style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box", backgroundColor: "#fff" }}>
                    <option value="auto">自动</option>
                    <option value="factor_only">仅因子</option>
                    <option value="model_only">仅模型</option>
                    <option value="joint">联合</option>
                  </select>
                </div>
              </div>
              <div style={{ padding: "10px 12px", borderRadius: "8px", border: "1px solid #ccfbf1", backgroundColor: "#f0fdfa" }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: "#0f766e", marginBottom: "6px" }}>
                  训练标签期限（普通 fork 会重训，可覆盖源 Loop）
                </div>
                <div style={{ display: "flex", gap: "6px", flexWrap: "wrap" }}>
                  {([1, 3, 5, 10, 20] as const).map(h => (
                    <button
                      key={h}
                      type="button"
                      onClick={() => setForkForm(f => ({ ...f, label_horizon: h }))}
                      style={{
                        padding: "5px 10px",
                        borderRadius: "5px",
                        border: forkForm.label_horizon === h ? "2px solid #0f766e" : "1px solid #99f6e4",
                        backgroundColor: forkForm.label_horizon === h ? "#ccfbf1" : "#ffffff",
                        color: forkForm.label_horizon === h ? "#0f766e" : "#64748b",
                        fontSize: "12px",
                        fontWeight: 700,
                        cursor: "pointer",
                      }}
                    >
                      {h}d
                    </button>
                  ))}
                </div>
              </div>
              <div>
                <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "4px" }}>
                  演进指引（可选）
                </label>
                <textarea value={forkForm.evolution_guidance}
                  onChange={e => setForkForm(f => ({ ...f, evolution_guidance: e.target.value }))}
                  rows={2}
                  style={{ width: "100%", padding: "8px 12px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "14px", boxSizing: "border-box", resize: "vertical" }}
                  placeholder="例如: 尝试更激进的因子替换策略..."
                />
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                <input type="checkbox" id="fork-inherit" checked={forkForm.inherit_history}
                  onChange={e => setForkForm(f => ({ ...f, inherit_history: e.target.checked }))}
                />
                <label htmlFor="fork-inherit" style={{ fontSize: "13px", color: "#475569", cursor: "pointer" }}>
                  继承截止到 Loop {showForkDialog} 的演进历史（Agent 可参考过去经验）
                </label>
              </div>
              {/* 策略 & 执行算法 */}
              <div style={{ border: "1px solid #e2e8f0", borderRadius: "8px", padding: "12px", backgroundColor: "#f8fafc" }}>
                <div style={{ fontSize: "12px", fontWeight: 700, color: "#334155", marginBottom: "10px" }}>交易策略 & 执行算法（可选，留空=继承源任务）</div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px", marginBottom: "10px" }}>
                  <div>
                    <label style={{ display: "block", fontSize: "12px", fontWeight: 600, color: "#64748b", marginBottom: "4px" }}>交易策略</label>
                    <select value={forkForm.strategy_id}
                      onChange={e => setForkForm(f => ({ ...f, strategy_id: e.target.value, strategy_params: {} }))}
                      style={{ width: "100%", padding: "6px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px", boxSizing: "border-box", backgroundColor: "white" }}>
                      <option value="">继承源任务</option>
                      {strategyCatalog.map(s => (
                        <option key={s.strategy_id} value={s.strategy_id}>{s.display_name || s.strategy_id}</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label style={{ display: "block", fontSize: "12px", fontWeight: 600, color: "#64748b", marginBottom: "4px" }}>日内执行算法</label>
                    <select value={forkForm.execution_algo}
                      onChange={e => {
                        const algoCode = e.target.value;
                        const algoInfo = executionAlgoCatalog.find((a: any) => a.algo_code === algoCode);
                        setForkForm(f => ({ ...f, execution_algo: algoCode, execution_algo_params: algoInfo?.default_config || {} }));
                      }}
                      style={{ width: "100%", padding: "6px 10px", borderRadius: "6px", border: "1px solid #cbd5e1", fontSize: "13px", boxSizing: "border-box", backgroundColor: "white" }}>
                      {renderQeExecutionAlgoOptions(forkForm.execution_algo, "继承源任务")}
                    </select>
                  </div>
                </div>
                <div style={{ padding: "8px 10px", borderRadius: "6px", backgroundColor: "#f0f9ff", border: "1px solid #bae6fd", marginBottom: "10px" }}>
                  <label style={{ display: "flex", alignItems: "center", gap: "8px", fontSize: "12px", color: "#0369a1", fontWeight: 600 }}>
                    <input
                      type="checkbox"
                      checked={forkForm.filter_suspended_on_signal}
                      onChange={e => setForkForm(f => ({ ...f, filter_suspended_on_signal: e.target.checked }))}
                    />
                    启用 suspend_d 日频选股停牌过滤
                  </label>
                  {forkForm.filter_suspended_on_signal && (
                    <label style={{ display: "flex", alignItems: "center", gap: "8px", marginTop: "6px", fontSize: "12px", color: "#075985" }}>
                      <input
                        type="checkbox"
                        checked={forkForm.suspend_filter_strict}
                        onChange={e => setForkForm(f => ({ ...f, suspend_filter_strict: e.target.checked }))}
                      />
                      严格要求 suspend_d 审计成功
                    </label>
                  )}
                </div>
                {/* 策略参数（含 initial_cash） */}
                {!forkForm.strategy_id ? (
                  <ParamSchemaForm
                    schema={{
                      topk: { type: "integer", default: 50, minimum: 1, maximum: 200, description: "持仓股票数" },
                      n_drop: { type: "integer", default: 5, minimum: 0, maximum: 50, description: "每期替换数" },
                      hold_thresh: { type: "integer", default: 2, minimum: 1, maximum: 30, description: "持有期（天）" },
                      risk_degree: { type: "number", default: 0.95, minimum: 0.1, maximum: 1.0, description: "仓位比例" },
                      initial_cash: { type: "integer", default: 100000000, minimum: 100000, description: "初始资金（元）" },
                    }}
                    values={forkForm.strategy_params}
                    onChange={(key, val) => setForkForm(f => ({ ...f, strategy_params: { ...f.strategy_params, [key]: val } }))}
                  />
                ) : (
                  <div style={{ marginTop: "8px" }}>
                    <div style={{ padding: "8px 12px", backgroundColor: "#f1f5f9", borderRadius: "6px", fontSize: "12px", color: "#64748b", marginBottom: "8px" }}>
                      自定义策略参数请在策略管理页面的 portfolio_config 中配置
                    </div>
                    <ParamSchemaForm
                      schema={{
                        initial_cash: { type: "integer", default: 100000000, minimum: 100000, description: "初始资金（元）" },
                      }}
                      values={forkForm.strategy_params}
                      onChange={(key, val) => setForkForm(f => ({ ...f, strategy_params: { ...f.strategy_params, [key]: val } }))}
                    />
                  </div>
                )}
              </div>
            </div>
            </>
            ) : (
              <>
                {/* 策略演进配置表单 */}
                <h2 style={{ margin: "0 0 16px", fontSize: "18px", color: "#1e293b", display: "flex", alignItems: "center", gap: "8px" }}>
                  <Activity size={20} color="#f59e0b" />
                  策略演进（跳过训练）
                </h2>
                <p style={{ margin: "0 0 16px", fontSize: "13px", color: "#64748b" }}>
                  复用 Loop {showForkDialog} 的已训练模型，仅修改策略参数进行批量回测。
                </p>

                {/* 模型来源信息 */}
                <div style={{ padding: "12px", backgroundColor: "#f8fafc", borderRadius: "8px", marginBottom: "16px", border: "1px solid #e5e7eb" }}>
                  <div style={{ fontSize: "12px", color: "#64748b", marginBottom: "4px" }}>
                    <strong>模型来源：</strong> Loop {showForkDialog}
                  </div>
                  {(() => {
                    const sourceLoop = loops.find(l => l.loop_index === showForkDialog);
                    if (!sourceLoop) return null;
                    const config = sourceLoop.config_json || {};
                    const sourceHorizon = config?.model_params?.label_horizon || config?.label_horizon || forkForm.label_horizon || 1;
                    return (
                      <div style={{ fontSize: "12px", color: "#475569" }}>
                        模型 ID: {config.model_id || "N/A"} | 因子数: {config.factor_names?.length || config.factor_list?.length || 0} | Label Horizon: {sourceHorizon}d（锁定）
                      </div>
                    );
                  })()}
                </div>

                {/* 执行方式 */}
                <div style={{ marginBottom: "16px" }}>
                  <label style={{ display: "block", fontSize: "13px", fontWeight: 600, color: "#475569", marginBottom: "6px" }}>
                    执行方式
                  </label>
                  <div style={{ display: "flex", gap: "12px" }}>
                    <button
                      onClick={() => setStrategyEvoExecutionMode("serial")}
                      style={{
                        flex: 1,
                        padding: "10px",
                        borderRadius: "8px",
                        border: strategyEvoExecutionMode === "serial" ? "2px solid #8b5cf6" : "1px solid #cbd5e1",
                        backgroundColor: strategyEvoExecutionMode === "serial" ? "#f5f3ff" : "#fff",
                        fontWeight: strategyEvoExecutionMode === "serial" ? 600 : 400,
                        cursor: "pointer",
                      }}
                    >
                      串行（逐个执行）
                    </button>
                    <button
                      onClick={() => setStrategyEvoExecutionMode("parallel")}
                      style={{
                        flex: 1,
                        padding: "10px",
                        borderRadius: "8px",
                        border: strategyEvoExecutionMode === "parallel" ? "2px solid #8b5cf6" : "1px solid #cbd5e1",
                        backgroundColor: strategyEvoExecutionMode === "parallel" ? "#f5f3ff" : "#fff",
                        fontWeight: strategyEvoExecutionMode === "parallel" ? 600 : 400,
                        cursor: "pointer",
                      }}
                    >
                      并行（同时执行）
                    </button>
                  </div>
                </div>

                {/* Loop 配置列表 */}
                <div style={{ marginBottom: "16px" }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
                    <label style={{ fontSize: "13px", fontWeight: 600, color: "#475569" }}>
                      策略回测 Loop 配置
                    </label>
                    <div style={{ display: "flex", gap: "8px" }}>
                      <button
                        onClick={copyFromSourceLoop}
                        style={{
                          padding: "6px 12px",
                          backgroundColor: "#e0e7ff",
                          color: "#4338ca",
                          border: "none",
                          borderRadius: "6px",
                          fontSize: "12px",
                          fontWeight: 500,
                          cursor: "pointer",
                        }}
                      >
                        从源配置复制
                      </button>
                      <button
                        onClick={addStrategyEvoLoop}
                        style={{
                          padding: "6px 12px",
                          backgroundColor: "#8b5cf6",
                          color: "#fff",
                          border: "none",
                          borderRadius: "6px",
                          fontSize: "12px",
                          fontWeight: 500,
                          cursor: "pointer",
                        }}
                      >
                        + 添加 Loop
                      </button>
                    </div>
                  </div>

                  {strategyEvoLoops.map((loop, index) => (
                    <div
                      key={index}
                      style={{
                        border: "1px solid #e5e7eb",
                        borderRadius: "8px",
                        padding: "12px",
                        marginBottom: "12px",
                        backgroundColor: "#fff",
                      }}
                    >
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "12px" }}>
                        <input
                          type="text"
                          value={loop.label || ""}
                          onChange={e => updateStrategyEvoLoop(index, { label: e.target.value })}
                          placeholder={`Loop ${loop.loop_index}`}
                          style={{
                            flex: 1,
                            padding: "6px 10px",
                            borderRadius: "6px",
                            border: "1px solid #cbd5e1",
                            fontSize: "14px",
                            marginRight: "12px",
                          }}
                        />
                        {strategyEvoLoops.length > 1 && (
                          <button
                            onClick={() => removeStrategyEvoLoop(index)}
                            style={{
                              padding: "4px 8px",
                              backgroundColor: "#fee2e2",
                              color: "#dc2626",
                              border: "none",
                              borderRadius: "4px",
                              fontSize: "12px",
                              cursor: "pointer",
                            }}
                          >
                            删除
                          </button>
                        )}
                      </div>

                      {/* 策略参数 */}
                      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: "8px", marginBottom: "8px" }}>
                        <div>
                          <label style={{ display: "block", fontSize: "11px", color: "#64748b", marginBottom: "2px" }}>topk</label>
                          <input
                            type="number"
                            value={loop.strategy_params?.topk || 50}
                            onChange={e => updateStrategyEvoLoop(index, { strategy_params: { ...loop.strategy_params, topk: parseInt(e.target.value) || 50 } })}
                            style={{ width: "100%", padding: "4px 6px", borderRadius: "4px", border: "1px solid #cbd5e1", fontSize: "12px" }}
                          />
                        </div>
                        <div>
                          <label style={{ display: "block", fontSize: "11px", color: "#64748b", marginBottom: "2px" }}>n_drop</label>
                          <input
                            type="number"
                            value={loop.strategy_params?.n_drop || 5}
                            onChange={e => updateStrategyEvoLoop(index, { strategy_params: { ...loop.strategy_params, n_drop: parseInt(e.target.value) || 5 } })}
                            style={{ width: "100%", padding: "4px 6px", borderRadius: "4px", border: "1px solid #cbd5e1", fontSize: "12px" }}
                          />
                        </div>
                        <div>
                          <label style={{ display: "block", fontSize: "11px", color: "#64748b", marginBottom: "2px" }}>hold_thresh</label>
                          <input
                            type="number"
                            value={loop.strategy_params?.hold_thresh || 2}
                            onChange={e => updateStrategyEvoLoop(index, { strategy_params: { ...loop.strategy_params, hold_thresh: parseInt(e.target.value) || 2 } })}
                            style={{ width: "100%", padding: "4px 6px", borderRadius: "4px", border: "1px solid #cbd5e1", fontSize: "12px" }}
                          />
                        </div>
                        <div>
                          <label style={{ display: "block", fontSize: "11px", color: "#64748b", marginBottom: "2px" }}>risk_degree</label>
                          <input
                            type="number"
                            step="0.05"
                            min="0.1"
                            max="1.0"
                            value={loop.strategy_params?.risk_degree || 0.95}
                            onChange={e => updateStrategyEvoLoop(index, { strategy_params: { ...loop.strategy_params, risk_degree: parseFloat(e.target.value) || 0.95 } })}
                            style={{ width: "100%", padding: "4px 6px", borderRadius: "4px", border: "1px solid #cbd5e1", fontSize: "12px" }}
                          />
                        </div>
                      </div>

                      {/* 执行算法和 HMM */}
                      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "8px", marginBottom: "8px" }}>
                        <div>
                          <label style={{ display: "block", fontSize: "11px", color: "#64748b", marginBottom: "2px" }}>执行算法</label>
                          <select
                            value={loop.execution_algo || ""}
                            onChange={e => updateStrategyEvoLoop(index, { execution_algo: e.target.value })}
                            style={{ width: "100%", padding: "4px 6px", borderRadius: "4px", border: "1px solid #cbd5e1", fontSize: "12px" }}
                          >
                            {renderQeExecutionAlgoOptions(loop.execution_algo, "继承源任务")}
                          </select>
                        </div>
                        <div style={{ display: "flex", alignItems: "center", paddingTop: "16px" }}>
                          <input
                            type="checkbox"
                            id={`hmm-${index}`}
                            checked={loop.enable_sector_hmm || false}
                            onChange={e => updateStrategyEvoLoop(index, { enable_sector_hmm: e.target.checked })}
                            style={{ marginRight: "6px" }}
                          />
                          <label htmlFor={`hmm-${index}`} style={{ fontSize: "12px", color: "#64748b", cursor: "pointer" }}>
                            启用 HMM 行业热度调整
                          </label>
                        </div>
                      </div>

                      <div style={{ padding: "8px", borderRadius: "6px", backgroundColor: "#f0f9ff", border: "1px solid #bae6fd", marginBottom: "8px" }}>
                        <label style={{ display: "flex", alignItems: "center", gap: "6px", fontSize: "12px", color: "#0369a1" }}>
                          <input
                            type="checkbox"
                            checked={loop.filter_suspended_on_signal || false}
                            onChange={e => updateStrategyEvoLoop(index, { filter_suspended_on_signal: e.target.checked })}
                          />
                          suspend_d 停牌过滤
                        </label>
                        {loop.filter_suspended_on_signal && (
                          <label style={{ display: "flex", alignItems: "center", gap: "6px", marginTop: "6px", fontSize: "11px", color: "#075985" }}>
                            <input
                              type="checkbox"
                              checked={loop.suspend_filter_strict !== false}
                              onChange={e => updateStrategyEvoLoop(index, { suspend_filter_strict: e.target.checked })}
                            />
                            严格审计
                          </label>
                        )}
                      </div>

                      {loop.enable_sector_hmm && (
                        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "8px", padding: "8px", backgroundColor: "#fffbeb", borderRadius: "6px", marginBottom: "8px" }}>
                          <div>
                            <label style={{ display: "block", fontSize: "11px", color: "#92400e", marginBottom: "2px" }}>HMM 模型版本</label>
                            <select
                              value={loop.hmm_model_version_id || ""}
                              onChange={e => updateStrategyEvoLoop(index, { hmm_model_version_id: e.target.value })}
                              style={{ width: "100%", padding: "4px 6px", borderRadius: "4px", border: "1px solid #fcd34d", fontSize: "12px" }}
                            >
                              <option value="">选择模型...</option>
                              {hmmSnapshots.map(s => (
                                <option key={s.snapshot_id} value={s.snapshot_id}>{s.snapshot_id}</option>
                              ))}
                            </select>
                          </div>
                          <div>
                            <label style={{ display: "block", fontSize: "11px", color: "#92400e", marginBottom: "2px" }}>信号预设</label>
                            <select
                              value={loop.hmm_signal_preset || ""}
                              onChange={e => updateStrategyEvoLoop(index, { hmm_signal_preset: e.target.value })}
                              style={{ width: "100%", padding: "4px 6px", borderRadius: "4px", border: "1px solid #fcd34d", fontSize: "12px" }}
                            >
                              <option value="">选择预设...</option>
                              <option value="preset_A">预设 A (保守, 最高+5%)</option>
                              <option value="preset_B">预设 B (激进, 最高+10%)</option>
                            </select>
                          </div>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </>
            )}

            <div style={{ display: "flex", justifyContent: "flex-end", gap: "12px", marginTop: "20px" }}>
              <button onClick={handleForkCancel}
                style={{ padding: "8px 16px", backgroundColor: "#f1f5f9", color: "#475569", border: "none", borderRadius: "6px", fontSize: "14px", fontWeight: 600, cursor: "pointer" }}>
                取消
              </button>
              <button
                onClick={forkType === "strategy_evo" ? handleStrategyEvoSubmit : handleForkSubmit}
                disabled={isForking}
                style={{ padding: "8px 16px", backgroundColor: forkType === "strategy_evo" ? "#f59e0b" : "#8b5cf6", color: "#fff", border: "none", borderRadius: "6px", fontSize: "14px", fontWeight: 600, cursor: "pointer", opacity: isForking ? 0.6 : 1 }}
              >
                {isForking ? "创建中..." : (forkType === "strategy_evo" ? `创建策略演进任务 (${strategyEvoLoops.length} Loops)` : "创建分叉任务")}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* 因子验证弹窗 */}
      {factorValidation && (
        <div style={{ position: "fixed", inset: 0, backgroundColor: "rgba(0,0,0,0.5)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 9999 }}>
          <div style={{ backgroundColor: "#fff", borderRadius: "12px", padding: "24px", maxWidth: "520px", width: "90%", boxShadow: "0 20px 60px rgba(0,0,0,0.2)" }}>
            <h2 style={{ margin: "0 0 12px", fontSize: "18px", fontWeight: 700, color: "#d97706" }}>
              因子可用性问题
            </h2>
            <p style={{ margin: "0 0 12px", fontSize: "13px", color: "#64748b" }}>
              任务已创建，但发现以下因子存在问题：
            </p>

            {factorValidation.validation.deleted_factors?.length > 0 && (
              <div style={{ padding: "8px 12px", background: "#fef2f2", borderRadius: 6, marginBottom: 8, fontSize: 12, color: "#991b1b" }}>
                <strong>已删除因子：</strong> {factorValidation.validation.deleted_factors.join(", ")}
              </div>
            )}
            {factorValidation.validation.unavailable_factors?.length > 0 && (
              <div style={{ padding: "8px 12px", background: "#fffbeb", borderRadius: 6, marginBottom: 8, fontSize: 12, color: "#92400e" }}>
                <strong>不可用因子：</strong> {factorValidation.validation.unavailable_factors.join(", ")}
              </div>
            )}
            {factorValidation.validation.valid_factors?.length > 0 && (
              <div style={{ padding: "8px 12px", background: "#f0fdf4", borderRadius: 6, marginBottom: 12, fontSize: 12, color: "#166534" }}>
                <strong>可用因子 ({factorValidation.validation.valid_factors.length})：</strong> {factorValidation.validation.valid_factors.join(", ")}
              </div>
            )}

            <div style={{ display: "flex", justifyContent: "flex-end", gap: "8px", marginTop: 16 }}>
              <button
                disabled={resolving}
                onClick={async () => {
                  // 取消：删除任务
                  try {
                    await fetch(`${API}/quantevolver/evolution/tasks/${factorValidation.taskId}/stop`, { method: "POST" });
                  } catch {}
                  setFactorValidation(null);
                  fetchTasks();
                }}
                style={{ padding: "8px 16px", backgroundColor: "#f1f5f9", color: "#475569", border: "none", borderRadius: "6px", fontSize: "13px", fontWeight: 600, cursor: "pointer" }}>
                取消任务
              </button>
              <button
                disabled={resolving || !factorValidation.validation.valid_factors?.length}
                onClick={async () => {
                  setResolving(true);
                  try {
                    const res = await fetch(`${API}/quantevolver/evolution/tasks/${factorValidation.taskId}/resolve-factors`, {
                      method: "POST",
                      headers: { "Content-Type": "application/json" },
                      body: JSON.stringify({ action: "remove" }),
                    });
                    const data = await res.json();
                    if (data.status === "success") {
                      alert(`已移除问题因子，${data.new_factor_count} 个因子继续演进`);
                      setFactorValidation(null);
                      fetchTasks();
                    } else {
                      alert("处理失败: " + (data.detail || "未知错误"));
                    }
                  } catch (e: any) {
                    alert("处理失败: " + (e?.message || "网络错误"));
                  } finally {
                    setResolving(false);
                  }
                }}
                style={{
                  padding: "8px 16px", backgroundColor: "#16a34a", color: "#fff", border: "none",
                  borderRadius: "6px", fontSize: "13px", fontWeight: 600, cursor: "pointer",
                  opacity: resolving ? 0.6 : 1,
                }}>
                {resolving ? "处理中..." : "移除问题因子继续"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
