"use client";

import React, { useEffect, useState, useCallback, useRef } from "react";
import { QEReadCache } from "../../components/read-cache";
import { useActiveRefresh, isExecutingTask } from "../../components/use-active-refresh";
import { ArrowLeft, ChevronDown, ChevronRight, Star } from "lucide-react";
import dynamic from "next/dynamic";
import { AllStocksTable } from "../../components/AllStocksTable";
import { FactorAnalysisPanel } from "../../components/FactorAnalysisPanel";
import { StrategyConfigCard } from "../../components/StrategyConfigCard";
import { PaperV2ApiError, strategyPackageApi } from "@/lib/paper-v2/api";
import type { JsonObject } from "@/lib/paper-v2/types";
import { qeArchiveApi, type ArchiveSourceItemStatus, type ArchiveSourceStatus, type BackfillReport } from "@/lib/qe-archive/api";

const IcSeriesChart = dynamic(() => import("../../components/charts/IcSeriesChart"), { ssr: false });
const ReturnCurveChart = dynamic(() => import("../../components/charts/ReturnCurveChart"), { ssr: false });
const LossCurveChart = dynamic(() => import("../../components/charts/LossCurveChart"), { ssr: false });

const API = process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:8001/api/v1";
const QE_ARCHIVE_WRITE_CONFIRM = "QE_ARCHIVE_WRITE";

function MetricBadge({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <span style={{ display: "inline-flex", flexDirection: "column", alignItems: "center", padding: "4px 12px", backgroundColor: "#f8fafc", borderRadius: 6, border: "1px solid #e2e8f0", minWidth: 80 }}>
      <span style={{ fontSize: 10, color: "#94a3b8", fontWeight: 600 }}>{label}</span>
      <span style={{ fontSize: 14, fontWeight: 700, color: color ?? "#0f172a", fontFamily: "monospace" }}>{value}</span>
    </span>
  );
}

function MetricCard({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={{ textAlign: "center", padding: 16, backgroundColor: "#f8fafc", borderRadius: 8, border: "1px solid #e2e8f0" }}>
      <div style={{ fontSize: 11, color: "#64748b", fontWeight: 600, textTransform: "uppercase" }}>{label}</div>
      <div style={{ fontSize: 22, fontWeight: 700, color: color ?? "#0f172a", fontFamily: "monospace", marginTop: 4 }}>{value}</div>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div style={{ backgroundColor: "#fff", borderRadius: 8, border: "1px solid #e2e8f0", padding: 20 }}>
      <h3 style={{ margin: "0 0 16px", fontSize: 13, fontWeight: 700, color: "#334155", textTransform: "uppercase", letterSpacing: "0.05em" }}>{title}</h3>
      {children}
    </div>
  );
}

const ACTION_COLORS: Record<string, string> = {
  initial: "#6366f1",
  factor_adjust: "#f59e0b",
  param_tune: "#3b82f6",
  model_switch: "#8b5cf6",
  factor_model_joint: "#ec4899",
};

function apiErrorMessage(error: unknown): string {
  if (error instanceof PaperV2ApiError) return error.message;
  if (error instanceof Error) return error.message;
  return String(error || "unknown error");
}

function getLoopPathId(loop: Loop): string {
  if (loop.loop_id) {
    const raw = String(loop.loop_id);
    const match = raw.match(/(?:^|_)Loop(\d+)$/i);
    return match ? `Loop${match[1]}` : raw;
  }
  return `Loop${loop.loop_index}`;
}

function getArchiveLoopId(taskId: string, loop: Loop): string {
  if (loop.loop_id) return String(loop.loop_id);
  return `${taskId}_Loop${loop.loop_index}`;
}

function archiveStatusLabel(status?: string): string {
  switch (status) {
    case "archived": return "已入仓";
    case "fully_archived": return "全部入仓";
    case "partially_archived": return "部分入仓";
    case "recommended": return "推荐入仓";
    case "eligible": return "可入仓";
    case "not_recommended": return "不建议";
    case "manual_only": return "人工判断";
    case "skipped": return "已跳过";
    case "not_archived":
    default:
      return "未入仓";
  }
}

function archiveStatusStyle(status?: string): React.CSSProperties {
  const palette: Record<string, { bg: string; fg: string; border: string }> = {
    archived: { bg: "#ecfdf5", fg: "#047857", border: "#a7f3d0" },
    fully_archived: { bg: "#ecfdf5", fg: "#047857", border: "#a7f3d0" },
    partially_archived: { bg: "#fffbeb", fg: "#b45309", border: "#fde68a" },
    recommended: { bg: "#eff6ff", fg: "#1d4ed8", border: "#bfdbfe" },
    eligible: { bg: "#eff6ff", fg: "#1d4ed8", border: "#bfdbfe" },
    not_recommended: { bg: "#f8fafc", fg: "#64748b", border: "#cbd5e1" },
    manual_only: { bg: "#f5f3ff", fg: "#6d28d9", border: "#ddd6fe" },
    skipped: { bg: "#f8fafc", fg: "#64748b", border: "#cbd5e1" },
    not_archived: { bg: "#fef2f2", fg: "#b91c1c", border: "#fecaca" },
  };
  const colors = palette[status || "not_archived"] || palette.not_archived;
  return {
    padding: "2px 8px",
    borderRadius: 999,
    fontSize: 10,
    fontWeight: 700,
    backgroundColor: colors.bg,
    color: colors.fg,
    border: `1px solid ${colors.border}`,
    whiteSpace: "nowrap",
  };
}

function ArchiveBadge({ status }: { status?: ArchiveSourceItemStatus }) {
  const archiveStatus = status?.archive_status || "not_archived";
  return <span title={status?.run_ids?.join(", ") || status?.reason || ""} style={archiveStatusStyle(archiveStatus)}>{archiveStatusLabel(archiveStatus)}</span>;
}

function summarizeBackfillReport(report: BackfillReport): string {
  const rows = report.results || [];
  const candidate = report.candidate_count ?? rows.length;
  const willArchive = rows.filter((item) => item.will_archive !== false && !item.skipped_reason && !item.error).length;
  const written = report.ingested_count ?? rows.filter((item) => item.run_id && !item.dry_run && !item.error).length;
  const skipped = report.skipped_count ?? rows.filter((item) => item.skipped_reason || item.will_archive === false).length;
  const failed = report.failed_count ?? rows.filter((item) => item.error).length;
  return `\u5019\u9009 ${candidate} \u6761\uff0c\u53ef\u5165\u4ed3 ${willArchive} \u6761\uff0c\u5df2\u5199\u5165 ${written} \u6761\uff0c\u8df3\u8fc7 ${skipped} \u6761\uff0c\u5931\u8d25 ${failed} \u6761`;
}

interface Loop {
  loop_id: string;
  loop_index: number;
  status: string;
  is_sota: boolean;
  action_type?: string;
  config_json?: any;
  agent_analysis?: any;
  config_summary?: any;
  metrics_summary?: any;
  metrics_json?: any;
  experiment_id?: string | null;
  archive_run_id?: string | null;
}

export default function EvolutionDetailPage({ params }: { params: { taskId: string } }) {
  const taskId = params.taskId;
  const reads = useRef(new QEReadCache());
  const currentTask = useRef(taskId);
  currentTask.current = taskId;
  const expandedRef = useRef(new Set<number>());
  const loopVersions = useRef<Record<string, string>>({});
  useEffect(() => () => { reads.current.clear(); }, [taskId]);

  const [task, setTask] = useState<any>(null);
  const [loops, setLoops] = useState<Loop[]>([]);
  const [expandedLoops, setExpandedLoops] = useState<Set<number>>(new Set());
  const [enhancedCache, setEnhancedCache] = useState<Record<string, any>>({});
  const [loadingLoops, setLoadingLoops] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [candidateActionId, setCandidateActionId] = useState<string | null>(null);
  const [candidateMessage, setCandidateMessage] = useState<{ loopId: string; msg: string; ok: boolean } | null>(null);
  const [enhancedErrors, setEnhancedErrors] = useState<Record<string, string>>({});
  const [archiveStatus, setArchiveStatus] = useState<ArchiveSourceStatus | null>(null);
  const [archiveStatusLoading, setArchiveStatusLoading] = useState(false);
  const [selectedLoopIndices, setSelectedLoopIndices] = useState<Set<number>>(new Set());
  const [archiveActionLoading, setArchiveActionLoading] = useState<"preview" | "execute" | null>(null);
  const [archiveMessage, setArchiveMessage] = useState<{ msg: string; ok: boolean } | null>(null);

  const loadArchiveStatus = useCallback(async (nextLoops: Loop[]) => {
    if (!taskId) return;
    const loopIds = nextLoops.map((loop) => getArchiveLoopId(taskId, loop)).filter(Boolean);
    setArchiveStatusLoading(true);
    try {
      const nextStatus = await qeArchiveApi.sourceStatus({
        task_ids: [taskId],
        loop_ids: loopIds,
        include_recommendation: true,
      });
      setArchiveStatus(nextStatus);
    } catch (exc) {
      setArchiveMessage({ ok: false, msg: `数仓状态读取失败: ${apiErrorMessage(exc)}` });
    } finally {
      setArchiveStatusLoading(false);
    }
  }, [taskId]);

  const fetchEnhancedDirect = useCallback((loopId: string) => {
    const index = Number(loopId.match(/Loop(\d+)$/i)?.[1]);
    if (!Number.isInteger(index)) { setEnhancedErrors(previous => ({ ...previous, [loopId]: "无效 Loop 标识" })); return; }
    const version = loopVersions.current[loopId] || "";
    setLoadingLoops(previous => new Set(previous).add(loopId));
    const base = `${API}/quantevolver/evolution/tasks/${taskId}/loops/${index}`;
    Promise.all(["config", "metrics", "analysis"].map(kind =>
      reads.current.read(`${base}/${kind}`, `${base}/${kind}:${version}`)
    )).then(([config, metrics, analysis]) => {
      if (currentTask.current !== taskId || loopVersions.current[loopId] !== version || !expandedRef.current.has(index)) return;
      const fullMetrics = metrics.data?.metrics_json || {};
      setLoops(previous => previous.map(loop => loop.loop_id === loopId ? {
        ...loop, config_json: config.data?.config_json, metrics_json: fullMetrics,
        agent_analysis: analysis.data?.agent_analysis,
      } : loop));
      setEnhancedCache(previous => {
        const next = { ...previous, [loopId]: fullMetrics.enhanced_metrics || {} };
        // Only a bounded number of detailed loops stays resident in this page.
        const keys = Object.keys(next);
        keys.slice(0, Math.max(0, keys.length - 8)).forEach(key => delete next[key]);
        return next;
      });
      setEnhancedErrors(previous => { const next = { ...previous }; delete next[loopId]; return next; });
    }).catch(reason => {
      if (currentTask.current === taskId && reason.name !== "AbortError") {
        setEnhancedErrors(previous => ({ ...previous, [loopId]: String(reason.message || reason) }));
      }
    }).finally(() => {
      if (currentTask.current === taskId) setLoadingLoops(previous => { const next = new Set(previous); next.delete(loopId); return next; });
    });
  }, [taskId]);

  const loadSummary = useCallback(async (initial = false) => {
    const url = `${API}/quantevolver/evolution/tasks/${taskId}/loops/comparison`;
    const result = await reads.current.read(url, url, false);
    if (currentTask.current !== taskId) return;
    if (result.status !== "success" || !result.data) throw new Error("任务摘要响应无效");
    const nextLoops = (result.data.loops || []).map((loop: any) => ({
      ...loop, config_json: loop.config_summary || {}, metrics_json: loop.metrics_summary || {},
    }));
    nextLoops.forEach((loop: any) => {
      const version = `${loop.status}:${loop.updated_at || ""}:${loop.experiment_id || ""}`;
      if (loopVersions.current[loop.loop_id] !== version) {
        setEnhancedCache(previous => { const next = { ...previous }; delete next[loop.loop_id]; return next; });
      }
      loopVersions.current[loop.loop_id] = version;
    });
    setTask(result.data.task || result.data);
    setLoops(nextLoops);
    if (initial || !isExecutingTask(result.data.task || result.data)) await loadArchiveStatus(nextLoops);
    setError(null);
    // Preserve operator expansion; do not automatically mount a SOTA's heavy charts.
    for (const index of expandedRef.current) {
      const loop = nextLoops.find((item: Loop) => item.loop_index === index);
      if (loop) fetchEnhancedDirect(loop.loop_id);
    }
  }, [taskId, loadArchiveStatus, fetchEnhancedDirect]);

  const summaryRead = useRef(loadSummary);
  summaryRead.current = loadSummary;
  useEffect(() => {
    setLoading(true); setTask(null); setLoops([]); setExpandedLoops(new Set());
    setEnhancedCache({}); setEnhancedErrors({}); setSelectedLoopIndices(new Set());
    loopVersions.current = {};
    expandedRef.current = new Set();
    let cancelled = false;
    summaryRead.current(true).catch(reason => { if (!cancelled) setError(String(reason)); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; reads.current.clear(); };
  }, [taskId]);

  useActiveRefresh(async () => {
    try { await summaryRead.current(); }
    catch (reason) { setError(String(reason)); throw reason; }
  }, isExecutingTask(task), taskId);

  const fetchEnhanced = useCallback((loopId: string) => {
    if (enhancedCache[loopId] || loadingLoops.has(loopId)) return;
    fetchEnhancedDirect(loopId);
  }, [enhancedCache, loadingLoops, fetchEnhancedDirect]);

  const addLoopToCandidatePackages = async (loop: Loop, event: React.MouseEvent) => {
    event.stopPropagation();
    const loopPathId = getLoopPathId(loop);
    setCandidateActionId(loop.loop_id);
    setCandidateMessage(null);
    try {
      const base = `${API}/quantevolver/evolution/tasks/${taskId}/loops/${loop.loop_index}`;
      // Explicit promotion loads the full snapshot even if the panel was never expanded.
      const [payload, metricPayload, analysisPayload] = await Promise.all([
        reads.current.read(`${base}/config`, `${base}/config`, false),
        reads.current.read(`${base}/metrics`, `${base}/metrics`, false),
        reads.current.read(`${base}/analysis`, `${base}/analysis`, false),
      ]);
      const config = payload.data?.config_json || {};
      const metrics = metricPayload.data?.metrics_json || {};
      const enhanced = metrics.enhanced_metrics;
      const completeLoop = { ...loop, config_json: config, metrics_json: metrics, agent_analysis: analysisPayload.data?.agent_analysis };
      const factorList = Array.isArray(config.factor_list) ? config.factor_list : [];
      const candidate = await strategyPackageApi.createCandidateFromQELoop({
        qe_task_id: taskId,
        qe_loop_id: loopPathId,
        experiment_id: loop.experiment_id ?? null,
        created_by: "quantevolver_evolution_task_detail",
        display_name: `${task.task_name ?? taskId} / ${loopPathId}`,
        archive_run_id: loop.archive_run_id ?? null,
        snapshot_config: {
          source_ui: "quantevolver_evolution_task_detail",
          qe_task_id: taskId,
          qe_loop_id: loopPathId,
          loop_index: loop.loop_index,
          loop_data: completeLoop,
          loop_config: config,
          enhanced_summary: enhanced?.summary ?? null,
        } as JsonObject,
        factor_manifest: {
          factor_list: factorList,
          factor_count: factorList.length,
        },
        model_manifest: {
          model_id: config.model_id ?? null,
          model_config: config.model_config ?? null,
          training_config: config.training_config ?? null,
          missing_reproducibility_items: config.seed == null ? ["seed"] : [],
        },
        strategy_manifest: {
          strategy_id: config.strategy_id ?? null,
          action_type: loop.action_type ?? null,
          daily_strategy_config: config.daily_strategy_config ?? null,
          minute_execution_config: config.minute_execution_config ?? null,
          tail_handling_config: config.tail_handling_config ?? null,
          platform_runtime_boundary: "HMM/ST/PIT/event signals are Paper v2 platform capabilities, not package assets",
        },
        metric_snapshot: {
          ...metrics,
          enhanced_summary: enhanced?.summary ?? null,
        } as JsonObject,
        artifact_refs: {
          enhanced_metrics_available: Boolean(enhanced),
          enhanced_metrics_endpoint: `/quantevolver/evolution/tasks/${taskId}/loops/${loopPathId}/enhanced-metrics`,
          evolution_task_route: `/quantevolver/evolution/${taskId}`,
        },
        completeness: {
          candidate_snapshot_created: true,
          strategy_package_manifest_available: Boolean(config.strategy_package_manifest),
          missing_items: config.strategy_package_manifest ? [] : ["strategy_package_manifest"],
        },
        eligibility: {
          candidate_only: true,
          can_enter_selection_or_paper_after_package_validation: true,
          live_approval_reserved: false,
        },
        audit_context: {
          manual_action: true,
          ui_route: `/quantevolver/evolution/${taskId}`,
          design_doc: "docs/architecture/paper_v2_qe_candidate_strategy_warehouse_design_20260512.md",
          created_at: new Date().toISOString(),
        },
        manual_action: true,
      });
      setCandidateMessage({ loopId: loop.loop_id, ok: true, msg: `已加入候选策略包: ${candidate.candidate_id}` });
    } catch (e) {
      setCandidateMessage({ loopId: loop.loop_id, ok: false, msg: `加入候选策略包失败: ${apiErrorMessage(e)}` });
    } finally {
      setCandidateActionId(null);
    }
  };

  const archiveStatusForLoop = useCallback((loop: Loop): ArchiveSourceItemStatus | undefined => {
    const loopId = getArchiveLoopId(taskId, loop);
    return archiveStatus?.loops?.[loopId];
  }, [archiveStatus, taskId]);

  const canSelectArchiveLoop = useCallback((loop: Loop): boolean => {
    const status = archiveStatusForLoop(loop)?.archive_status || "not_archived";
    return loop.status === "completed" && status !== "archived";
  }, [archiveStatusForLoop]);

  const toggleArchiveSelection = (loop: Loop, checked?: boolean) => {
    if (!canSelectArchiveLoop(loop)) return;
    setSelectedLoopIndices(prev => {
      const next = new Set(prev);
      const shouldSelect = checked ?? !next.has(loop.loop_index);
      if (shouldSelect) next.add(loop.loop_index);
      else next.delete(loop.loop_index);
      return next;
    });
  };

  const selectedArchiveIndices = () => Array.from(selectedLoopIndices)
    .filter(index => loops.some(loop => loop.loop_index === index && canSelectArchiveLoop(loop)))
    .sort((a, b) => a - b);

  const previewArchiveSelection = async (indices?: number[]) => {
    const loopIndices = indices && indices.length ? indices : selectedArchiveIndices();
    if (!loopIndices.length) {
      setArchiveMessage({ ok: false, msg: "请先选择至少一个已完成且未入仓的 Loop" });
      return;
    }
    setArchiveActionLoading("preview");
    setArchiveMessage(null);
    try {
      const report = await qeArchiveApi.previewSelection({
        source: "loop",
        task_id: taskId,
        loop_indices: loopIndices,
        status: "completed",
        include_archived: false,
        validate_after_write: true,
      });
      setArchiveMessage({ ok: true, msg: `预览完成：${summarizeBackfillReport(report)}` });
      void loadArchiveStatus(loops);
    } catch (exc) {
      setArchiveMessage({ ok: false, msg: `预览失败: ${apiErrorMessage(exc)}` });
    } finally {
      setArchiveActionLoading(null);
    }
  };

  const executeArchiveSelection = async (indices?: number[]) => {
    const loopIndices = indices && indices.length ? indices : selectedArchiveIndices();
    if (!loopIndices.length) {
      setArchiveMessage({ ok: false, msg: "请先选择至少一个已完成且未入仓的 Loop" });
      return;
    }
    const ok = window.confirm(`确认将 ${loopIndices.length} 个 Loop 写入 QE Archive 数仓？此操作会写数据库，但不会删除原始 QE 实验。`);
    if (!ok) return;
    setArchiveActionLoading("execute");
    setArchiveMessage(null);
    try {
      const report = await qeArchiveApi.executeSelection({
        source: "loop",
        task_id: taskId,
        loop_indices: loopIndices,
        status: "completed",
        include_archived: false,
        validate_after_write: true,
        confirm_write: QE_ARCHIVE_WRITE_CONFIRM,
      });
      setArchiveMessage({ ok: true, msg: `入仓完成：${summarizeBackfillReport(report)}` });
      setSelectedLoopIndices(new Set());
      void loadArchiveStatus(loops);
    } catch (exc) {
      setArchiveMessage({ ok: false, msg: `入仓失败: ${apiErrorMessage(exc)}` });
    } finally {
      setArchiveActionLoading(null);
    }
  };

  const toggleLoop = (loopIndex: number, loopId: string) => {
    const next = new Set(expandedRef.current);
    if (next.has(loopIndex)) {
      next.delete(loopIndex);
      setLoops(previous => previous.map(loop => loop.loop_id === loopId ? {
        ...loop, config_json: loop.config_summary || {}, metrics_json: loop.metrics_summary || {}, agent_analysis: undefined,
      } : loop));
      setEnhancedCache(previous => { const rest = { ...previous }; delete rest[loopId]; return rest; });
    } else {
      next.add(loopIndex);
    }
    expandedRef.current = next;
    setExpandedLoops(next);
    if (next.has(loopIndex)) fetchEnhanced(loopId);
  };

  if (loading) {
    return <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: "100vh", backgroundColor: "#f1f5f9" }}><div style={{ fontSize: 14, color: "#64748b" }}>加载中...</div></div>;
  }
  if (!task) {
    return <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: "100vh", backgroundColor: "#f1f5f9" }}><div style={{ fontSize: 14, color: "#ef4444" }}>{error ?? "任务数据加载失败"}</div></div>;
  }

  const fmtPct = (v: number | null | undefined) => v != null ? (v * 100).toFixed(2) + "%" : "-";
  const fmtNum = (v: number | null | undefined, d = 4) => v != null ? v.toFixed(d) : "-";

  const sotaCount = loops.filter(l => l.is_sota).length;
  const completedCount = loops.filter(l => l.status === "completed").length;
  const taskArchiveStatus = archiveStatus?.tasks?.[taskId];
  const archivedLoopCount = taskArchiveStatus?.archived_loop_count ?? loops.filter(loop => archiveStatusForLoop(loop)?.archive_status === "archived").length;
  const pendingArchiveCount = taskArchiveStatus?.pending_loop_count ?? loops.filter(canSelectArchiveLoop).length;
  const recommendedArchiveCount = taskArchiveStatus?.recommended_loop_count ?? loops.filter(loop => archiveStatusForLoop(loop)?.recommended).length;
  const selectedArchiveCount = selectedArchiveIndices().length;

  return (
    <div style={{ minHeight: "100vh", backgroundColor: "#f1f5f9", padding: "24px 32px" }}>
      {error && <div role="alert">{error}（保留上次成功读取的结果）</div>}
      <button onClick={() => { reads.current.clear(); void summaryRead.current(true).catch(reason => setError(String(reason))); }}>刷新详情</button>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 24 }}>
        <button onClick={() => window.close()} style={{ display: "flex", alignItems: "center", gap: 6, padding: "6px 12px", borderRadius: 6, border: "1px solid #e2e8f0", backgroundColor: "#fff", cursor: "pointer", fontSize: 13, color: "#475569" }}>
          <ArrowLeft size={14} /> 关闭
        </button>
        <h1 style={{ margin: 0, fontSize: 18, fontWeight: 700, color: "#0f172a" }}>
          {task.task_name ?? task.task_id}
        </h1>
        <span style={{ padding: "2px 10px", borderRadius: 12, fontSize: 11, fontWeight: 600, backgroundColor: task.status === "completed" ? "#eff6ff" : task.status === "running" ? "#f0fdf4" : "#fef2f2", color: task.status === "completed" ? "#3b82f6" : task.status === "running" ? "#22c55e" : "#ef4444" }}>
          {task.status}
        </span>
        <span style={{ fontSize: 12, color: "#64748b" }}>共 {loops.length} Loops | 完成 {completedCount} | SOTA {sotaCount}</span>
        <span style={{ fontSize: 12, color: "#64748b" }}>数仓 已入仓 {archivedLoopCount} | 推荐 {recommendedArchiveCount} | 待入仓 {pendingArchiveCount}</span>
        {taskArchiveStatus && <span style={archiveStatusStyle(taskArchiveStatus.archive_status)}>{archiveStatusLabel(taskArchiveStatus.archive_status)}</span>}
      </div>

      <div style={{ maxWidth: 1200, marginBottom: 12, background: "#fff", border: "1px solid #e2e8f0", borderRadius: 8, padding: 12, display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
          <strong style={{ fontSize: 13, color: "#0f172a" }}>QE Archive 手动入仓</strong>
          <span style={{ fontSize: 12, color: "#64748b" }}>选择已完成且未入仓的 Loop，先预览再确认写入。不会自动入仓，也不会删除原始实验。</span>
          {archiveStatusLoading && <span style={{ fontSize: 12, color: "#2563eb" }}>状态刷新中...</span>}
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
          <button
            type="button"
            onClick={() => {
              const selectable = loops.filter(loop => canSelectArchiveLoop(loop) && archiveStatusForLoop(loop)?.recommended).map(loop => loop.loop_index);
              setSelectedLoopIndices(new Set(selectable));
            }}
            disabled={!recommendedArchiveCount || Boolean(archiveActionLoading)}
            style={{ padding: "5px 10px", fontSize: 11, borderRadius: 6, border: "1px solid #bfdbfe", background: "#eff6ff", color: "#1d4ed8", cursor: recommendedArchiveCount ? "pointer" : "not-allowed", fontWeight: 700 }}
          >
            选择推荐 loop
          </button>
          <button
            type="button"
            onClick={() => {
              const selectable = loops.filter(canSelectArchiveLoop).map(loop => loop.loop_index);
              setSelectedLoopIndices(new Set(selectable));
            }}
            disabled={!pendingArchiveCount || Boolean(archiveActionLoading)}
            style={{ padding: "5px 10px", fontSize: 11, borderRadius: 6, border: "1px solid #cbd5e1", background: "#f8fafc", color: "#334155", cursor: pendingArchiveCount ? "pointer" : "not-allowed", fontWeight: 700 }}
          >
            选择待入仓
          </button>
          <button
            type="button"
            onClick={() => setSelectedLoopIndices(new Set())}
            disabled={!selectedArchiveCount || Boolean(archiveActionLoading)}
            style={{ padding: "5px 10px", fontSize: 11, borderRadius: 6, border: "1px solid #cbd5e1", background: "#fff", color: "#334155", cursor: selectedArchiveCount ? "pointer" : "not-allowed" }}
          >
            清空选择
          </button>
          <button
            type="button"
            onClick={() => void previewArchiveSelection()}
            disabled={!selectedArchiveCount || Boolean(archiveActionLoading)}
            style={{ padding: "5px 10px", fontSize: 11, borderRadius: 6, border: "1px solid #2563eb", background: "#eff6ff", color: "#1d4ed8", cursor: selectedArchiveCount ? "pointer" : "not-allowed", fontWeight: 700 }}
          >
            {archiveActionLoading === "preview" ? "预览中..." : `预览入仓(${selectedArchiveCount})`}
          </button>
          <button
            type="button"
            onClick={() => void executeArchiveSelection()}
            disabled={!selectedArchiveCount || Boolean(archiveActionLoading)}
            style={{ padding: "5px 10px", fontSize: 11, borderRadius: 6, border: "1px solid #059669", background: "#ecfdf5", color: "#047857", cursor: selectedArchiveCount ? "pointer" : "not-allowed", fontWeight: 700 }}
          >
            {archiveActionLoading === "execute" ? "写入中..." : `确认入仓(${selectedArchiveCount})`}
          </button>
          <button
            type="button"
            onClick={() => void loadArchiveStatus(loops)}
            disabled={archiveStatusLoading}
            style={{ padding: "5px 10px", fontSize: 11, borderRadius: 6, border: "1px solid #cbd5e1", background: "#fff", color: "#334155", cursor: archiveStatusLoading ? "not-allowed" : "pointer" }}
          >
            刷新数仓状态
          </button>
        </div>
        {archiveMessage && (
          <div style={{ width: "100%", padding: "8px 10px", borderRadius: 6, fontSize: 12, color: archiveMessage.ok ? "#166534" : "#991b1b", backgroundColor: archiveMessage.ok ? "#f0fdf4" : "#fef2f2", border: `1px solid ${archiveMessage.ok ? "#bbf7d0" : "#fecaca"}` }}>
            {archiveMessage.msg}
          </div>
        )}
      </div>

      {/* Loop 手风琴 */}
      <div style={{ display: "flex", flexDirection: "column", gap: 8, maxWidth: 1200 }}>
        {loops.map(loop => {
          const isExpanded = expandedLoops.has(loop.loop_index);
          const m = { ...(loop.metrics_json || {}), ...(loop.metrics_summary || {}) };
          const actionType = loop.action_type ?? m.action_type ?? "initial";
          const actionColor = ACTION_COLORS[actionType] ?? "#64748b";
          const loopArchiveStatus = archiveStatusForLoop(loop);
          const loopSelectable = canSelectArchiveLoop(loop);
          const loopSelected = selectedLoopIndices.has(loop.loop_index) && loopSelectable;
          const em = enhancedCache[loop.loop_id];
          const isLoading = loadingLoops.has(loop.loop_id);
          const td = em?.trade_diagnostics ?? {};

          return (
            <div key={loop.loop_id} style={{ borderRadius: 8, border: `1px solid ${loop.is_sota ? "#f59e0b" : "#e2e8f0"}`, backgroundColor: "#fff", overflow: "hidden", boxShadow: loop.is_sota ? "0 0 0 1px #f59e0b" : undefined }}>
              {/* 收起状态 — 概要行 */}
              <div
                onClick={() => toggleLoop(loop.loop_index, loop.loop_id)}
                style={{ display: "flex", alignItems: "center", gap: 12, padding: "12px 16px", cursor: "pointer", userSelect: "none" }}
              >
                {isExpanded ? <ChevronDown size={16} color="#64748b" /> : <ChevronRight size={16} color="#64748b" />}
                <input
                  aria-label={`选择 Loop ${loop.loop_index} 入仓`}
                  type="checkbox"
                  checked={loopSelected}
                  disabled={!loopSelectable || Boolean(archiveActionLoading)}
                  onClick={e => e.stopPropagation()}
                  onChange={e => toggleArchiveSelection(loop, e.target.checked)}
                  style={{ width: 14, height: 14, accentColor: "#059669", cursor: loopSelectable ? "pointer" : "not-allowed" }}
                />
                <span style={{ fontWeight: 700, fontSize: 13, color: "#0f172a", minWidth: 60 }}>Loop {loop.loop_index}</span>
                <span style={{ padding: "2px 8px", borderRadius: 4, fontSize: 10, fontWeight: 600, backgroundColor: actionColor + "18", color: actionColor }}>{actionType}</span>
                {loop.is_sota && <Star size={14} color="#f59e0b" fill="#f59e0b" />}
                <span style={{ padding: "2px 8px", borderRadius: 4, fontSize: 10, fontWeight: 600, backgroundColor: loop.status === "completed" ? "#f0fdf4" : loop.status === "failed" ? "#fef2f2" : "#fffbeb", color: loop.status === "completed" ? "#22c55e" : loop.status === "failed" ? "#ef4444" : "#f59e0b" }}>{loop.status}</span>
                <ArchiveBadge status={loopArchiveStatus} />
                {loop.status === "completed" && (
                  <button
                    data-testid="qe-task-loop-add-candidate"
                    onClick={(e) => addLoopToCandidatePackages(loop, e)}
                    disabled={candidateActionId === loop.loop_id}
                    style={{ padding: "4px 10px", fontSize: 11, cursor: candidateActionId === loop.loop_id ? "not-allowed" : "pointer", borderRadius: 4, border: "1px solid #2563eb", background: "#eff6ff", color: "#1d4ed8", fontWeight: 700 }}
                  >
                    {candidateActionId === loop.loop_id ? "加入中..." : "加入候选策略包"}
                  </button>
                )}
                {loopSelectable && (
                  <>
                    <button
                      data-testid="qe-task-loop-archive-preview"
                      onClick={(e) => { e.stopPropagation(); void previewArchiveSelection([loop.loop_index]); }}
                      disabled={Boolean(archiveActionLoading)}
                      style={{ padding: "4px 10px", fontSize: 11, cursor: archiveActionLoading ? "not-allowed" : "pointer", borderRadius: 4, border: "1px solid #2563eb", background: "#eff6ff", color: "#1d4ed8", fontWeight: 700 }}
                    >
                      预览入仓
                    </button>
                    <button
                      data-testid="qe-task-loop-archive-execute"
                      onClick={(e) => { e.stopPropagation(); void executeArchiveSelection([loop.loop_index]); }}
                      disabled={Boolean(archiveActionLoading)}
                      style={{ padding: "4px 10px", fontSize: 11, cursor: archiveActionLoading ? "not-allowed" : "pointer", borderRadius: 4, border: "1px solid #059669", background: "#ecfdf5", color: "#047857", fontWeight: 700 }}
                    >
                      入仓
                    </button>
                  </>
                )}
                <div style={{ flex: 1 }} />
                <MetricBadge label="IC" value={fmtNum(m.ic)} />
                <MetricBadge label="Sharpe" value={fmtNum(m.sharpe, 2)} />
                <MetricBadge label="CAGR" value={fmtPct(m.cagr)} color={(m.cagr ?? 0) >= 0 ? "#e53935" : "#22a35a"} />
                <MetricBadge label="超额年化" value={fmtPct(m.annualized_return)} />
                <MetricBadge label="IR" value={fmtNum(m.information_ratio, 2)} />
                <MetricBadge label="MDD" value={fmtPct(m.max_drawdown)} color="#22a35a" />
              </div>

              {/* 展开状态 — 完整详情 */}
              {isExpanded && (
                <div style={{ borderTop: "1px solid #e2e8f0", padding: 20, display: "flex", flexDirection: "column", gap: 16 }}>
                  {isLoading && <div style={{ textAlign: "center", padding: 40, color: "#94a3b8" }}>加载增强指标中...</div>}

                  {!isLoading && enhancedErrors[loop.loop_id] && (
                    <div style={{ padding: "10px 16px", backgroundColor: "#fef2f2", border: "1px solid #ef4444", borderRadius: 8, fontSize: 12, color: "#991b1b" }}>
                      增强指标加载失败: {enhancedErrors[loop.loop_id]}
                    </div>
                  )}

                  {candidateMessage?.loopId === loop.loop_id && (
                    <div style={{ padding: "10px 16px", backgroundColor: candidateMessage.ok ? "#f0fdf4" : "#fef2f2", border: `1px solid ${candidateMessage.ok ? "#22c55e" : "#ef4444"}`, borderRadius: 8, fontSize: 12, color: candidateMessage.ok ? "#166534" : "#991b1b" }}>
                      {candidateMessage.msg}
                    </div>
                  )}

                  {!isLoading && (
                    <>
                      {/* 策略与执行配置 */}
                      {loop.config_json && (
                        <StrategyConfigCard source={{ loopConfig: loop.config_json }} />
                      )}

                      {/* 核心指标 */}
                      <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 12 }}>
                        <MetricCard label="IC" value={fmtNum(m.ic)} />
                        <MetricCard label="ICIR" value={fmtNum(m.icir)} />
                        <MetricCard label="Rank IC" value={fmtNum(m.rank_ic)} />
                        <MetricCard label="CAGR" value={fmtPct(m.cagr)} color={(m.cagr ?? 0) >= 0 ? "#e53935" : "#22a35a"} />
                        <MetricCard label="最大回撤" value={fmtPct(m.max_drawdown)} color="#22a35a" />
                      </div>

                      {/* 交易效率 */}
                      {td.avg_turnover != null && (
                        <Section title="交易效率">
                          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16 }}>
                            <MetricCard label="平均换手率" value={td.avg_turnover?.toFixed(3) ?? "-"} />
                            <MetricCard label="成本侵蚀(年化)" value={td.cost_drag_annualized != null ? (td.cost_drag_annualized * 100).toFixed(1) + "%" : "-"} />
                            <MetricCard label="日均交易笔数" value={td.daily_trade_count_avg?.toFixed(1) ?? "-"} />
                          </div>
                        </Section>
                      )}

                      {/* 全部持仓股票（支持排序） */}
                      {em?.all_stocks && em.all_stocks.length > 0 ? (
                        <AllStocksTable stocks={em.all_stocks} stockTrades={em?.stock_trades} />
                      ) : (em?.top_stocks || em?.bottom_stocks) ? (
                        <AllStocksTable
                          stocks={[...(em?.top_stocks ?? []), ...(em?.bottom_stocks ?? [])]}
                          stockTrades={em?.stock_trades}
                        />
                      ) : null}

                      {/* 因子贡献度 */}
                      {(em?.factor_analysis?.feature_importance || em?.feature_importance) && (
                        <FactorAnalysisPanel featureImportance={em.factor_analysis?.feature_importance ?? em.feature_importance} />
                      )}

                      {/* IC 诊断 */}
                      {(em?.ic_diagnostics || (em?.dates && em?.ic_series)) && (
                        <Section title="IC 诊断">
                          <IcSeriesChart {...(em.ic_diagnostics || em)} />
                        </Section>
                      )}

                      {/* 收益曲线 */}
                      {(em?.return_curves || (em?.dates && (em?.cumulative_excess_no_cost || em?.cumulative_excess_with_cost))) && (
                        <Section title="收益曲线">
                          <ReturnCurveChart {...(em.return_curves || em)} />
                        </Section>
                      )}

                      {/* 训练过程 */}
                      {(em?.training_diagnostics || em?.train_loss_curve) && (
                        <Section title="训练过程">
                          <LossCurveChart {...(em.training_diagnostics || em)} />
                        </Section>
                      )}

                      {/* Agent 诊断 */}
                      {loop.agent_analysis && (
                        <Section title="Agent 诊断">
                          <pre style={{ fontSize: 11, color: "#475569", whiteSpace: "pre-wrap", wordBreak: "break-word", maxHeight: 300, overflow: "auto", backgroundColor: "#f8fafc", padding: 12, borderRadius: 6 }}>
                            {typeof loop.agent_analysis === "string" ? loop.agent_analysis : JSON.stringify(loop.agent_analysis, null, 2)}
                          </pre>
                        </Section>
                      )}
                    </>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
