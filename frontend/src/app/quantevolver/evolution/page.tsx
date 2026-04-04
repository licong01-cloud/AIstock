"use client";

import React, { useState, useEffect, useCallback, useRef, useMemo } from "react";
import {
  Play, Terminal, GitMerge, FileCode2,
  Activity, ArrowRight, DownloadCloud, CheckCircle2,
  AlertCircle, TrendingUp, BarChart3,
  Square, RotateCcw, Pause, XCircle, RefreshCw, Trash2
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
    source_type: "qe_experiment" as "qe_experiment" | "rdagent_task_sota" | "evolution_fork",
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
    enable_sector_hmm: false,
    hmm_model_version_id: "",
    hmm_signal_preset: "preset_A",
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

  // 因子相关性分析
  const [corrPairs, setCorrPairs] = useState<Array<{factor_a: string; factor_b: string; correlation: number}>>([]);
  const [corrLoading, setCorrLoading] = useState(false);
  const [corrAnalyzed, setCorrAnalyzed] = useState(false);
  const [factorsToRemove, setFactorsToRemove] = useState<Set<string>>(new Set());

  // Stop/Resume 状态
  const [showResumeDialog, setShowResumeDialog] = useState<string | null>(null); // task_id or null
  const [additionalLoops, setAdditionalLoops] = useState(0);

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
  });

  // 策略演进相关状态
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

      // 连接 SSE 日志流
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }

      appendLogs([`[System] 已连接到任务 ${activeTaskId} 的实时日志流...`]);

      let reconnectCount = 0;
      const MAX_RECONNECT = 200;
      const RECONNECT_DELAY = 3000;
      const boundTaskId = activeTaskId; // 捕获当前 taskId，防止闭包串扰

      function createSSE(taskId: string) {
        const sse = new EventSource(`${API}/quantevolver/evolution/tasks/${taskId}/logs`);
        sse.onmessage = (event) => {
          reconnectCount = 0; // 收到消息重置重连计数
          try {
            const data = JSON.parse(event.data);

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

        sse.onerror = () => {
          sse.close();
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
      const selectedTask = tasks.find(t => t.task_id === activeTaskId);
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

  // 提交新建任务
  const handleCreateTask = async () => {
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
            additional_factor_keys: additionalFactorKeys.size > 0 ? Array.from(additionalFactorKeys) : undefined,
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
          setNewTask({ task_name: "", target_desc: "", max_loops: 10, base_experiment_id: "", source_type: "qe_experiment", source_task_id: "", include_alpha_baseline: false, evolution_guidance: "", evolution_mode: "auto", fork_from_task_id: "", fork_from_loop_index: -1, inherit_history: false, strategy_id: "", strategy_params: {}, execution_algo: "", execution_algo_params: {}, enable_sector_hmm: false, hmm_model_version_id: "", hmm_signal_preset: "preset_A" });
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
        selected_model_id: selectedModelForEvo || undefined,
        selected_factor_keys: factorKeys,
        additional_factor_keys: additionalFactorKeys.size > 0 ? Array.from(additionalFactorKeys) : undefined,
        ...(blacklistEnabled && stockPoolPath ? { stock_pool: stockPoolPath } : {}),
        strategy_id: newTask.strategy_id || undefined,
        strategy_params: Object.keys(newTask.strategy_params).length > 0 ? newTask.strategy_params : undefined,
        execution_algo: newTask.execution_algo || undefined,
        execution_algo_params: Object.keys(newTask.execution_algo_params).length > 0 ? newTask.execution_algo_params : undefined,
        enable_sector_hmm: newTask.enable_sector_hmm || undefined,
        hmm_model_version_id: newTask.enable_sector_hmm ? (newTask.hmm_model_version_id || undefined) : undefined,
        hmm_signal_preset: newTask.enable_sector_hmm ? (newTask.hmm_signal_preset || undefined) : undefined,
      };
      const res = await fetch(`${API}/quantevolver/evolution/tasks`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(submitData)
      });
      const data = await res.json();
      if (data.status === "success") {
        if (data.factor_validation) {
          // 有因子验证问题 — 弹出处理弹窗
          setFactorValidation({ taskId: data.task_id, validation: data.factor_validation });
          setShowCreateTask(false);
          setNewTask({ task_name: "", target_desc: "", max_loops: 10, base_experiment_id: "", source_type: "qe_experiment", source_task_id: "", include_alpha_baseline: false, evolution_guidance: "", evolution_mode: "auto", fork_from_task_id: "", fork_from_loop_index: -1, inherit_history: false, strategy_id: "", strategy_params: {}, execution_algo: "", execution_algo_params: {}, enable_sector_hmm: false, hmm_model_version_id: "", hmm_signal_preset: "preset_A" });
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
          setNewTask({ task_name: "", target_desc: "", max_loops: 10, base_experiment_id: "", source_type: "qe_experiment", source_task_id: "", include_alpha_baseline: false, evolution_guidance: "", evolution_mode: "auto", fork_from_task_id: "", fork_from_loop_index: -1, inherit_history: false, strategy_id: "", strategy_params: {}, execution_algo: "", execution_algo_params: {}, enable_sector_hmm: false, hmm_model_version_id: "", hmm_signal_preset: "preset_A" });
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
    if (!confirm("确定要停止此演进任务吗？")) return;
    try {
      const res = await fetch(`${API}/quantevolver/evolution/tasks/${taskId}/stop`, { method: "POST" });
      const data = await res.json();
      if (data.status === "success") {
        appendLogs([`[System] 任务 ${taskId} 已停止`]);
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
        body: JSON.stringify({ additional_loops: additionalLoops }),
      });
      const data = await res.json();
      if (data.status === "success") {
        appendLogs([`[System] 任务 ${resumingTaskId} 已恢复演进`]);
        setShowResumeDialog(null);
        setAdditionalLoops(0);
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
    setForkForm({
      task_name: task ? `${task.task_name}_from_L${loopIndex}` : "",
      max_loops: 10,
      evolution_guidance: "",
      evolution_mode: "auto",
      inherit_history: false,
      strategy_id: task?.strategy_id || "",
      strategy_params: task?.strategy_params || {},
      execution_algo: task?.execution_algo || "",
      execution_algo_params: task?.execution_algo_params || {},
    });
    setShowForkDialog(loopIndex);
  };

  const handleForkCancel = () => {
    setShowForkDialog(null);
    setIsForking(false);
    setForkType("evolution");
    setForkForm({ task_name: "", max_loops: 10, evolution_guidance: "", evolution_mode: "auto", inherit_history: false, strategy_id: "", strategy_params: {}, execution_algo: "", execution_algo_params: {} });
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
            <div style={{ maxHeight: "220px", overflowY: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "13px" }}>
                <thead>
                  <tr style={{ backgroundColor: "#f8fafc", borderBottom: "1px solid #e2e8f0", position: "sticky", top: 0, zIndex: 1 }}>
                    <th style={{ padding: "10px 16px", textAlign: "left", fontWeight: 700, color: "#475569", fontSize: "12px", textTransform: "uppercase", letterSpacing: "0.05em" }}>任务名称</th>
                    <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "80px" }}>来源</th>
                    <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "80px" }}>状态</th>
                    <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "80px" }}>Loop</th>
                    <th style={{ padding: "10px 12px", textAlign: "center", fontWeight: 700, color: "#475569", fontSize: "12px", width: "120px" }}>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {tasks.length === 0 ? (
                    <tr>
                      <td colSpan={5} style={{ padding: "24px", textAlign: "center", color: "#94a3b8" }}>暂无演进任务</td>
                    </tr>
                  ) : tasks.map(task => {
                    const isActive = activeTaskId === task.task_id;
                    const statusInfo = getTaskStatusInfo(task.status);
                    const canStop = task.status === "running";
                    const canResume = ["stopped", "paused", "completed", "failed"].includes(task.status);
                    const canDelete = task.status !== "running";
                    const sourceType = task.source_type;
                    return (
                      <tr
                        key={task.task_id}
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
                        <td style={{ padding: "10px 12px", textAlign: "center" }}>
                          <div style={{ display: "flex", gap: "4px", justifyContent: "center" }}>
                            {canStop && (
                              <button onClick={(e) => handleStopTask(task.task_id, e)}
                                title="停止"
                                style={{ padding: "4px 8px", border: "1px solid #fca5a5", borderRadius: "4px", backgroundColor: "#fff", color: "#ef4444", fontSize: "11px", fontWeight: 600, cursor: "pointer", display: "flex", alignItems: "center", gap: "3px" }}>
                                <Square size={11} /> 停止
                              </button>
                            )}
                            {canResume && (
                              <button onClick={(e) => { e.stopPropagation(); setShowResumeDialog(task.task_id); setAdditionalLoops(0); }}
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
          configDiffLines={configDiffLines}
          onSyncAssets={handleSyncAssets}
          onForkFromLoop={handleForkFromLoop}
          taskType={tasks.find(t => t.task_id === activeTaskId)?.task_type}
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

              {newTask.source_type === "qe_experiment" ? (
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
                        {" "}(IC: {exp.ic?.toFixed(4) ?? "N/A"}, 因子: {exp.factor_count ?? 0}
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
                      <option value="">默认（TWAP）</option>
                      {executionAlgoCatalog.map(a => (
                        <option key={a.algo_code} value={a.algo_code}>{a.algo_name || a.algo_code}</option>
                      ))}
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

              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "16px" }}>
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
                {isCreating ? "创建中..." : "创建并启动演进"}
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
                      <option value="">继承源任务</option>
                      {executionAlgoCatalog.map(a => (
                        <option key={a.algo_code} value={a.algo_code}>{a.algo_name || a.algo_code}</option>
                      ))}
                    </select>
                  </div>
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
                    return (
                      <div style={{ fontSize: "12px", color: "#475569" }}>
                        模型 ID: {config.model_id || "N/A"} | 因子数: {config.factor_names?.length || 0}
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
                            <option value="">继承源任务</option>
                            {executionAlgoCatalog.map(a => (
                              <option key={a.algo_code} value={a.algo_code}>{a.algo_name || a.algo_code}</option>
                            ))}
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
