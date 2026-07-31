
"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { formatCompact, formatNumber, shortHash } from "@/lib/paper-v2/format";
import {
  API_BASE,
  type ArchiveJob,
  type ArchiveSummary,
  type BackfillCandidate,
  type BackfillCandidateLoop,
  type BackfillReport,
  type OutboxEvent,
  type PromotionCandidateItem,
  type RunLeaderboardItem,
  type RunOperatorOption,
  type RunQuality,
  type TopKQualityItem,
  type WorkerRunReport,
  qeArchiveApi,
} from "@/lib/qe-archive/api";
import BusinessSearchSelect, { type BusinessSearchOption } from "./BusinessSearchSelect";
import LongTrendComparisonPanel from "./LongTrendComparisonPanel";

const WRITE_CONFIRM_TEXT = "QE_ARCHIVE_WRITE";
const WORKER_CONFIRM_TEXT = "QE_ARCHIVE_WORKER_RUN";
const QUALITY_GATE = {
  min_metrics: 60,
  min_curves: 3000,
  min_factors: 1,
  require_account_summary: true,
};

function n(value: unknown): number {
  const parsed = Number(value || 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatDateTime(value: unknown): string {
  const text = String(value || "");
  return text ? text.replace("T", " ").slice(0, 19) : "-";
}

function formatPct(value: unknown, digits = 2): string {
  if (value === null || value === undefined || value === "") return "-";
  const parsed = Number(value);
  return Number.isFinite(parsed) ? `${(parsed * 100).toFixed(digits)}%` : "-";
}

function formatSignedPct(value: unknown, digits = 2): string {
  if (value === null || value === undefined || value === "") return "-";
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "-";
  const prefix = parsed > 0 ? "+" : "";
  return `${prefix}${(parsed * 100).toFixed(digits)}%`;
}

function topkStatusLabel(status?: string | null): string {
  if (status === "ok") return "数据完整";
  if (status === "missing_label") return "缺少标签制品";
  if (status === "missing_pred") return "缺少预测制品";
  return "历史证据缺失";
}

function topkSourceLabel(source?: string | null): string {
  return source ? "预测与标签制品已关联" : "未形成可关联的预测/标签证据";
}

function topkMissingReason(status?: string | null): string {
  if (status === "missing_pred") return "该历史实验未保存可用于排名的预测制品。";
  if (status === "missing_label") return "该历史实验未保存可与预测按交易日关联的标签制品。";
  return "该历史实验缺少可按交易日关联的预测与标签证据；已有 IC/RankIC 指标不等于具备 Top-K 明细。";
}

function gateText(pass?: boolean | null): string {
  if (pass === true) return "PASSED";
  if (pass === false) return "FAILED";
  return "NO_DATA";
}

function statusCounts(counts?: Record<string, number>): { status: string; count: number }[] {
  return Object.entries(counts || {}).map(([status, count]) => ({ status, count }));
}

function candidateTypeLabel(candidate: BackfillCandidate): string {
  if (candidate.candidate_type === "evolution_task") return "演进任务";
  return "单次实验";
}

function candidatePrimaryId(candidate: BackfillCandidate): string {
  return String(candidate.task_id || candidate.experiment_id || "-");
}

function archiveStatusLabel(status?: string): string {
  switch (status) {
    case "archived": return "已入仓";
    case "fully_archived": return "全部入仓";
    case "partially_archived": return "部分入仓";
    case "recommended": return "推荐入仓";
    case "eligible": return "可入仓";
    case "manual_only": return "人工判断";
    case "not_recommended": return "不建议";
    case "skipped": return "已跳过";
    case "not_archived":
    default:
      return "未入仓";
  }
}

function archiveStatusStyle(status?: string) {
  const palette: Record<string, { bg: string; fg: string; border: string }> = {
    archived: { bg: "#ecfdf5", fg: "#047857", border: "#a7f3d0" },
    fully_archived: { bg: "#ecfdf5", fg: "#047857", border: "#a7f3d0" },
    partially_archived: { bg: "#fffbeb", fg: "#b45309", border: "#fde68a" },
    recommended: { bg: "#eff6ff", fg: "#1d4ed8", border: "#bfdbfe" },
    eligible: { bg: "#eff6ff", fg: "#1d4ed8", border: "#bfdbfe" },
    manual_only: { bg: "#f5f3ff", fg: "#6d28d9", border: "#ddd6fe" },
    skipped: { bg: "#f8fafc", fg: "#64748b", border: "#cbd5e1" },
    not_recommended: { bg: "#f8fafc", fg: "#64748b", border: "#cbd5e1" },
    not_archived: { bg: "#fef2f2", fg: "#b91c1c", border: "#fecaca" },
  };
  const colors = palette[status || "not_archived"] || palette.not_archived;
  return {
    display: "inline-flex",
    padding: "2px 8px",
    borderRadius: 999,
    fontSize: 11,
    fontWeight: 700,
    backgroundColor: colors.bg,
    color: colors.fg,
    border: `1px solid ${colors.border}`,
    whiteSpace: "nowrap" as const,
  };
}

function ArchiveStatusPill({ status }: { status?: string }) {
  return <span style={archiveStatusStyle(status)}>{archiveStatusLabel(status)}</span>;
}

function loopMetric(loop: BackfillCandidateLoop, keys: string[]): unknown {
  for (const key of keys) {
    const value = (loop as unknown as Record<string, unknown>)[key];
    if (value !== undefined && value !== null && value !== "") return value;
  }
  return null;
}

function runTypeLabel(value?: string | null): string {
  if (value === "evolution_loop") return "演进 Loop";
  if (value === "single_experiment" || value === "qe_experiment") return "单次实验";
  return "归档实验";
}

function runListChoice(run: RunOperatorOption): BusinessSearchOption {
  const loop = run.loop_index ? ` Loop${run.loop_index}` : "";
  const date = formatDateTime(run.completed_at || run.archived_at).slice(0, 10);
  return {
    value: run.value,
    label: run.task_name || `${runTypeLabel(run.run_type)}${loop}`,
    description: `${date}｜${run.model_type || "模型未知"}｜标签 ${run.label_horizon ?? "-"}D｜因子 ${run.factor_count ?? "-"}`,
  };
}

function StatusCountStrip({ counts, empty }: { counts?: Record<string, number>; empty: string }) {
  const rows = statusCounts(counts);
  if (!rows.length) return <span className="pv2-muted">{empty}</span>;
  return (
    <div className="pv2-chip-row">
      {rows.map((row) => (
        <span className="pv2-chip" key={row.status}>
          {row.status}: {row.count}
        </span>
      ))}
    </div>
  );
}

function ReportSummary({ report }: { report: BackfillReport | null }) {
  if (!report) return <div className="pv2-help">Select candidates, preview them, then confirm before writing to the warehouse.</div>;
  const rows = report.results || [];
  const willArchive = rows.filter((row) => row.will_archive !== false && !row.skipped_reason && !row.error).length;
  const written = report.ingested_count ?? rows.filter((row) => row.run_id && !row.dry_run && !row.error).length;
  const skipped = report.skipped_count ?? rows.filter((row) => row.skipped_reason || row.will_archive === false).length;
  const failed = report.failed_count ?? rows.filter((row) => row.error).length;
  return (
    <div className="pv2-readable-list">
      <div className="pv2-grid pv2-grid-4">
        <MetricCard label={"\u5904\u7406\u6570"} value={formatCompact(report.processed_count || 0, 0)} hint={report.backfill_run_id ? `audit ${shortHash(report.backfill_run_id)}` : "legacy/no audit"} tone={report.write_enabled ? "success" : "info"} />
        <MetricCard label={report.dry_run ? "\u53ef\u5165\u4ed3" : "\u5df2\u5199\u5165"} value={formatCompact(report.dry_run ? willArchive : written, 0)} hint={report.write_enabled ? "\u5df2\u5199\u5165\u6570\u4ed3" : "dry-run"} tone={report.write_enabled ? "success" : "neutral"} />
        <MetricCard label={"\u8df3\u8fc7"} value={formatCompact(skipped, 0)} hint={"\u672a\u7b26\u5408\u5165\u4ed3\u6761\u4ef6\uff0c\u4e0d\u7b49\u4e8e\u5931\u8d25"} tone="warning" />
        <MetricCard label={"\u5931\u8d25"} value={formatCompact(failed, 0)} hint={failed ? "\u9700\u8981\u5904\u7406\u7684\u5199\u5165\u9519\u8bef" : "\u65e0\u5199\u5165\u9519\u8bef"} tone={failed ? "danger" : "neutral"} />
      </div>
      <PaperTable
        rows={rows.slice(0, 40)}
        empty="暂无补录结果"
        columns={[
          { key: "run", header: "归档 Run", render: (row) => <span className="pv2-mono">{shortHash(row.run_id)}</span> },
          { key: "source", header: "来源", render: (row) => <><div>{row.event_type || "-"}</div><div className="pv2-muted pv2-mono">{row.source_sub_id || row.source_id || "-"}</div></> },
          { key: "stats", header: "写入统计", render: (row) => <span>{formatStats(row.stats)}</span> },
          { key: "quality", header: "质量", render: (row) => row.quality ? <><StatusBadge status={row.quality.passed ? "PASSED" : "FAILED"} /><div className="pv2-muted">指标 {row.quality.metric_count || 0} / 曲线 {row.quality.curve_count || 0} / 因子 {row.quality.factor_count_rows || 0}</div></> : <span className="pv2-muted">dry-run</span> },
        ]}
      />
    </div>
  );
}

function formatStats(stats: unknown): string {
  if (!stats || typeof stats !== "object") return "-";
  const record = stats as Record<string, unknown>;
  const parts = [
    ["metrics", record.metrics_written ?? record.metric_count],
    ["curves", record.curves_written ?? record.curve_count],
    ["factors", record.factors_written ?? record.factor_count],
    ["symbols", record.symbol_summaries_written ?? record.symbol_summary_count],
    ["trades", record.trades_written ?? record.trade_count],
    ["events", record.execution_events_written ?? record.execution_event_count],
    ["审计原文", record.raw_payloads_written ?? record.raw_payload_count],
  ]
    .filter(([, value]) => value !== undefined && value !== null)
    .map(([key, value]) => `${key} ${value}`);
  return parts.length ? parts.join(" / ") : "无可写统计";
}

function QualityPanel({ quality }: { quality: RunQuality | null }) {
  if (!quality) return <div className="pv2-help">从上方按任务名称、模型、类型或日期搜索并选择归档实验后，可核对配置、指标、曲线和因子是否完整。</div>;
  const checks = [
    { label: "配置完整", value: quality.config_capture_complete ? "是" : "否", tone: quality.config_capture_complete ? "success" as const : "warning" as const },
    { label: "可复现等级", value: quality.reproducibility_level || "-", tone: quality.reproducibility_level === "full" ? "success" as const : "warning" as const },
    { label: "指标行", value: formatCompact(quality.metric_count || 0, 0), tone: "info" as const },
    { label: "曲线行", value: formatCompact(quality.curve_count || 0, 0), tone: "info" as const },
  ];
  const rowCounts = [
    { name: "来源记录", value: quality.source_count },
    { name: "数据上下文", value: quality.data_context_count },
    { name: "账户摘要", value: quality.account_summary_count },
    { name: "因子参与记录", value: quality.factor_count_rows },
    { name: "股票汇总", value: quality.symbol_summary_count },
    { name: "交易明细", value: quality.trade_count },
    { name: "执行事件", value: quality.execution_event_count },
    { name: "审计原文记录", value: quality.raw_payload_count },
    { name: "artifact manifest", value: quality.artifact_count },
    { name: "优先级分数", value: quality.priority_score_count },
  ];
  return (
    <div className="pv2-readable-list">
      <div className="pv2-grid pv2-grid-4">
        {checks.map((item) => <MetricCard key={item.label} label={item.label} value={item.value} tone={item.tone} />)}
      </div>
      <div className="pv2-readable-panel">
        <div className="pv2-readable-table">
          <div className="pv2-readable-row"><div className="pv2-readable-key">所选归档实验</div><div className="pv2-readable-value">质量记录已加载</div></div>
          <div className="pv2-readable-row"><div className="pv2-readable-key">类型/状态</div><div className="pv2-readable-value">{quality.run_type || "-"} / <StatusBadge status={quality.status} /></div></div>
          <div className="pv2-readable-row"><div className="pv2-readable-key">研究有效性</div><div className="pv2-readable-value">{quality.research_valid ? "有效" : `无效：${quality.invalid_reason || "未说明"}`}</div></div>
          <div className="pv2-readable-row"><div className="pv2-readable-key">频率/标签</div><div className="pv2-readable-value">{quality.freq || "-"} / horizon {quality.label_horizon ?? "-"}</div></div>
          <div className="pv2-readable-row"><div className="pv2-readable-key">归档时间</div><div className="pv2-readable-value">{formatDateTime(quality.archived_at)}</div></div>
        </div>
      </div>
      <PaperTable
        rows={rowCounts}
        empty="暂无质量计数"
        columns={[
          { key: "name", header: "数据项", render: (row) => row.name },
          { key: "value", header: "数量", render: (row) => formatCompact(row.value || 0, 0) },
        ]}
      />
    </div>
  );
}

function TopKQualityCards({ rows }: { rows: TopKQualityItem[] }) {
  const current = rows.find((row) => row.topk_quality_status === "ok") || rows[0];
  const completeCount = rows.filter((row) => row.topk_quality_status === "ok").length;
  const missingCount = rows.length - completeCount;
  if (!current) {
    return (
      <div className="pv2-help">
        当前查询没有 Top-K 质量记录。新归档实验会在预测与标签制品可关联后自动形成质量证据；历史实验需先恢复原始制品才能重算。
      </div>
    );
  }
  return (
    <div className="pv2-readable-list">
      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="topk_return@20" value={formatSignedPct(current.topk_return_20)} hint={`@50 ${formatSignedPct(current.topk_return_50)}`} tone={current.topk_quality_status === "ok" ? "success" : "warning"} />
        <MetricCard label="Hit Rate@20" value={formatPct(current.topk_hit_rate_20)} hint={`@50 ${formatPct(current.topk_hit_rate_50)}`} tone="info" />
        <MetricCard label="@20-@50 Decay" value={formatSignedPct(current.topk_decay)} hint="rank<=20 均值 - rank<=50 均值" tone="neutral" />
        <MetricCard label="Within RankIC" value={formatNumber(current.within_portfolio_rankic, 4)} hint={current.topk_rank_direction || "rank 1=best"} tone="neutral" />
      </div>
      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="Dispersion@20" value={formatPct(current.topk_dispersion_20)} hint="Top20 realized return std" />
        <MetricCard label="覆盖交易日" value={formatCompact(current.topk_date_count || 0, 0)} hint={`joined ${formatCompact(current.topk_joined_observation_count || 0, 0)}`} />
        <MetricCard label="Top-K 状态" value={topkStatusLabel(current.topk_quality_status)} hint={topkSourceLabel(current.topk_source)} tone={current.topk_quality_status === "ok" ? "success" : "warning"} />
        <MetricCard label="当前批次覆盖" value={`${completeCount} 完整 / ${missingCount} 缺失`} hint="缺失不会伪装成成功，也不会用 SQL 推导替代原始证据" tone={missingCount ? "warning" : "success"} />
      </div>
      {current.topk_quality_status !== "ok" ? <div className="pv2-help" data-topk-reason="historical_artifact_missing" style={{ color: "#b45309" }}>缺失原因：{topkMissingReason(current.topk_quality_status)} 下一步：恢复该实验的预测与标签制品后执行受控重算。</div> : null}
    </div>
  );
}

export default function QEArchivePage() {
  const [summary, setSummary] = useState<ArchiveSummary | null>(null);
  const [outbox, setOutbox] = useState<OutboxEvent[]>([]);
  const [jobs, setJobs] = useState<ArchiveJob[]>([]);
  const [archivedRuns, setArchivedRuns] = useState<RunOperatorOption[]>([]);
  const [runSearch, setRunSearch] = useState("");
  const [runTypeFilter, setRunTypeFilter] = useState("");
  const [runCompletedFrom, setRunCompletedFrom] = useState("");
  const [runCompletedTo, setRunCompletedTo] = useState("");
  const [candidates, setCandidates] = useState<BackfillCandidate[]>([]);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [expandedCandidateIds, setExpandedCandidateIds] = useState<Set<string>>(new Set());
  const [selectedLoopIds, setSelectedLoopIds] = useState<Set<string>>(new Set());
  const [candidatePage, setCandidatePage] = useState(1);
  const [candidatePageSize, setCandidatePageSize] = useState(20);
  const [candidateHasMore, setCandidateHasMore] = useState(false);
  const [candidateStatus, setCandidateStatus] = useState("completed");
  const [includeArchived, setIncludeArchived] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [writeConfirm, setWriteConfirm] = useState("");
  const [backfillReport, setBackfillReport] = useState<BackfillReport | null>(null);
  const [backfillBusy, setBackfillBusy] = useState(false);
  const [workerConfirm, setWorkerConfirm] = useState("");
  const [workerLimit, setWorkerLimit] = useState(10);
  const [workerReport, setWorkerReport] = useState<WorkerRunReport | null>(null);
  const [workerBusy, setWorkerBusy] = useState(false);
  const [qualityRunId, setQualityRunId] = useState("");
  const [quality, setQuality] = useState<RunQuality | null>(null);
  const [leaderboard, setLeaderboard] = useState<RunLeaderboardItem[]>([]);
  const [promotionCandidates, setPromotionCandidates] = useState<PromotionCandidateItem[]>([]);
  const [topkRows, setTopkRows] = useState<TopKQualityItem[]>([]);
  const [qualityBusy, setQualityBusy] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [nextSummary, nextOutbox, nextJobs, nextCandidates, nextLeaderboard, nextPromotionCandidates, nextTopkRows] = await Promise.all([
        qeArchiveApi.health(),
        qeArchiveApi.outbox(30),
        qeArchiveApi.jobs(30),
        qeArchiveApi.backfillCandidates({ page: candidatePage, page_size: candidatePageSize, status: candidateStatus, include_archived: includeArchived }),
        qeArchiveApi.runLeaderboard({ limit: 20, order_by: "calmar" }),
        qeArchiveApi.promotionCandidates({ limit: 20, order_by: "calmar" }),
        qeArchiveApi.topkQuality({ limit: 20, k: 20 }),
      ]);
      setSummary(nextSummary);
      setOutbox(nextOutbox);
      setJobs(nextJobs);
      setCandidates(nextCandidates.candidates || []);
      setLeaderboard(nextLeaderboard || []);
      setPromotionCandidates(nextPromotionCandidates || []);
      setTopkRows(nextTopkRows || []);
      setCandidateHasMore(Boolean(nextCandidates.has_more));
      setSelectedIds((previous) => new Set([...previous].filter((id) => (nextCandidates.candidates || []).some((item) => item.candidate_id === id))));
      const visibleLoopIds = new Set((nextCandidates.candidates || []).flatMap((item) => (item.loops || []).map((loop) => String(loop.loop_id || ""))));
      setSelectedLoopIds((previous) => new Set([...previous].filter((id) => visibleLoopIds.has(id))));
      setExpandedCandidateIds((previous) => new Set([...previous].filter((id) => (nextCandidates.candidates || []).some((item) => item.candidate_id === id))));
    } catch (err) {
      setError(err);
    } finally {
      setLoading(false);
    }
  }, [candidatePage, candidatePageSize, candidateStatus, includeArchived]);

  const loadArchivedRuns = useCallback(async () => {
    setQualityBusy(true);
    setError(null);
    try {
      const response = await qeArchiveApi.runOperatorOptions({
        search: runSearch || undefined,
        run_type: runTypeFilter || undefined,
        completed_from: runCompletedFrom || undefined,
        completed_to: runCompletedTo || undefined,
        limit: 30,
      });
      setArchivedRuns(response.items || []);
      if (response.items?.length === 1) setQualityRunId((previous) => previous || response.items[0].value);
    } catch (err) {
      setArchivedRuns([]);
      setError(err);
    } finally {
      setQualityBusy(false);
    }
  }, [runCompletedFrom, runCompletedTo, runSearch, runTypeFilter]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    const timer = window.setTimeout(() => void loadArchivedRuns(), 250);
    return () => window.clearTimeout(timer);
  }, [loadArchivedRuns]);

  const validRuns = n(summary?.research_valid_counts?.true);
  const invalidRuns = n(summary?.research_valid_counts?.false);
  const pendingOutbox = n(summary?.pending_outbox_count);

  const latestRows = useMemo(() => outbox.slice(0, 12), [outbox]);
  const latestJobs = useMemo(() => jobs.slice(0, 12), [jobs]);
  const qualityRunOptions = archivedRuns.map(runListChoice);
  const selectedCandidates = useMemo(() => candidates.filter((item) => selectedIds.has(item.candidate_id)), [candidates, selectedIds]);
  const selectedTaskIds = selectedCandidates.filter((item) => item.candidate_type === "evolution_task" && item.task_id).map((item) => String(item.task_id));
  const selectedExperimentIds = selectedCandidates.filter((item) => item.candidate_type === "single_experiment" && item.experiment_id).map((item) => String(item.experiment_id));
  const selectedLoopList = useMemo(
    () => candidates
      .flatMap((candidate) => candidate.loops || [])
      .filter((loop) => loop.loop_id && selectedLoopIds.has(String(loop.loop_id)) && !selectedTaskIds.includes(String(loop.task_id || ""))),
    [candidates, selectedLoopIds, selectedTaskIds],
  );
  const selectedLoopIdList = selectedLoopList.map((loop) => String(loop.loop_id));
  const selectedRunCount = selectedCandidates.reduce((sum, item) => sum + n(item.pending_run_count), 0) + selectedLoopIdList.length;
  const selectionCount = selectedCandidates.length + selectedLoopIdList.length;
  const writeDisabledReason = backfillBusy
    ? "补录处理中"
    : selectionCount === 0
      ? "请先选择待入库候选"
      : writeConfirm !== WRITE_CONFIRM_TEXT
        ? `请先填入确认文本 ${WRITE_CONFIRM_TEXT}`
        : "";

  function toggleCandidate(candidateId: string) {
    setSelectedIds((previous) => {
      const next = new Set(previous);
      if (next.has(candidateId)) next.delete(candidateId);
      else next.add(candidateId);
      return next;
    });
  }

  function toggleCandidateExpanded(candidateId: string) {
    setExpandedCandidateIds((previous) => {
      const next = new Set(previous);
      if (next.has(candidateId)) next.delete(candidateId);
      else next.add(candidateId);
      return next;
    });
  }

  function selectableLoop(loop: BackfillCandidateLoop): boolean {
    return Boolean(loop.loop_id && loop.eligible && loop.archive_status !== "archived");
  }

  function toggleLoop(loop: BackfillCandidateLoop, checked?: boolean) {
    if (!loop.loop_id || !selectableLoop(loop)) return;
    const loopId = String(loop.loop_id);
    setSelectedLoopIds((previous) => {
      const next = new Set(previous);
      const shouldSelect = checked ?? !next.has(loopId);
      if (shouldSelect) next.add(loopId);
      else next.delete(loopId);
      return next;
    });
  }

  function selectCandidateLoops(candidate: BackfillCandidate, mode: "recommended" | "eligible") {
    const loopIds = (candidate.loops || [])
      .filter((loop) => selectableLoop(loop) && (mode === "eligible" || loop.recommended))
      .map((loop) => String(loop.loop_id));
    setSelectedLoopIds((previous) => new Set([...previous, ...loopIds]));
    setExpandedCandidateIds((previous) => new Set(previous).add(candidate.candidate_id));
  }

  function selectPendingCandidates() {
    setSelectedIds(new Set(candidates.filter((item) => n(item.pending_run_count) > 0).map((item) => item.candidate_id)));
  }

  async function runBackfill(write: boolean) {
    if (!selectionCount) {
      setError(new Error("请先在候选列表中选择需要写入数仓的 QE 实验或任务。"));
      return;
    }
    setBackfillBusy(true);
    setError(null);
    try {
      const payload = {
        source: "all" as const,
        task_ids: selectedTaskIds,
        experiment_ids: selectedExperimentIds,
        loop_ids: selectedLoopIdList,
        status: candidateStatus,
        include_archived: false,
        validate_after_write: true,
        ...QUALITY_GATE,
      };
      const report = write
        ? await qeArchiveApi.executeSelection({ ...payload, confirm_write: writeConfirm })
        : await qeArchiveApi.previewSelection(payload);
      setBackfillReport(report);
      if (write) await load();
    } catch (err) {
      setError(err);
    } finally {
      setBackfillBusy(false);
    }
  }

  function renderCandidateLoopControls(row: BackfillCandidate) {
    const loops = row.loops || [];
    const expanded = expandedCandidateIds.has(row.candidate_id);
    if (row.candidate_type !== "evolution_task") {
      return (
        <>
          <div>{formatNumber(row.archived_run_count || 0, 0)} / {formatNumber(row.selected_run_count || 0, 0)}</div>
          <div className="pv2-muted">单实验，待入库 {formatNumber(row.pending_run_count || 0, 0)}</div>
        </>
      );
    }
    return (
      <div style={{ display: "grid", gap: 8 }}>
        <div>
          <div>{formatNumber(row.archived_run_count || 0, 0)} / {formatNumber(row.selected_run_count || 0, 0)}</div>
          <div className="pv2-muted">
            总 loop {formatNumber(row.loop_count || 0, 0)}，待入库 {formatNumber(row.pending_run_count || 0, 0)}，推荐 {formatNumber(row.recommended_run_count || 0, 0)}
          </div>
        </div>
        <div className="pv2-row-actions">
          <button className="pv2-button-ghost" type="button" onClick={() => toggleCandidateExpanded(row.candidate_id)}>
            {expanded ? "收起 loop" : "展开 loop"}
          </button>
          <button className="pv2-button-ghost" type="button" onClick={() => selectCandidateLoops(row, "recommended")} disabled={!loops.some((loop) => selectableLoop(loop) && loop.recommended)}>
            选推荐 loop
          </button>
          <button className="pv2-button-ghost" type="button" onClick={() => selectCandidateLoops(row, "eligible")} disabled={!loops.some(selectableLoop)}>
            选全部有效 loop
          </button>
        </div>
        {expanded && (
          <div style={{ display: "grid", gap: 6, padding: 8, border: "1px solid #e2e8f0", borderRadius: 8, background: "#f8fafc" }}>
            {loops.length ? loops.map((loop) => {
              const loopId = String(loop.loop_id || "");
              const selectable = selectableLoop(loop);
              const selected = selectable && selectedLoopIds.has(loopId);
              const icValue = loopMetric(loop, ["IC", "ic"]);
              const retValue = loopMetric(loop, ["annualized_return"]);
              return (
                <label key={loopId || `${row.task_id}-${loop.loop_index}`} style={{ display: "grid", gridTemplateColumns: "18px 58px 1fr auto", gap: 8, alignItems: "center", fontSize: 12 }}>
                  <input
                    type="checkbox"
                    checked={selected}
                    disabled={!selectable}
                    onChange={(event) => toggleLoop(loop, event.target.checked)}
                    aria-label={`选择 ${row.task_id} Loop ${loop.loop_index} 入仓`}
                  />
                  <span className="pv2-mono">Loop {loop.loop_index ?? "-"}</span>
                  <span className="pv2-muted">
                    {loop.action_type || "-"} / IC {icValue == null ? "-" : Number(icValue).toFixed(4)} / 年化 {retValue == null ? "-" : `${(Number(retValue) * 100).toFixed(2)}%`}
                  </span>
                  <ArchiveStatusPill status={loop.archive_status} />
                </label>
              );
            }) : <div className="pv2-muted">暂无 loop 明细</div>}
          </div>
        )}
      </div>
    );
  }

  async function runWorkerOnce() {
    setWorkerBusy(true);
    setError(null);
    try {
      const report = await qeArchiveApi.runWorkerOnce({ limit: workerLimit, confirm_run: workerConfirm });
      setWorkerReport(report);
      await load();
    } catch (err) {
      setError(err);
    } finally {
      setWorkerBusy(false);
    }
  }

  async function lookupQuality() {
    const runId = qualityRunId.trim();
    if (!runId) return;
    setQualityBusy(true);
    setError(null);
    try {
      const [nextQuality, nextTopkRows] = await Promise.all([
        qeArchiveApi.quality(runId),
        qeArchiveApi.topkQuality({ run_id: runId, k: 20, limit: 5 }),
      ]);
      setQuality(nextQuality);
      setTopkRows(nextTopkRows);
    } catch (err) {
      setError(err);
    } finally {
      setQualityBusy(false);
    }
  }

  return (
    <main className="pv2-shell">
      <section className="pv2-hero">
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">QE Archive Warehouse</div>
            <h1>QE 实验实时数仓</h1>
            <p>
              从数据库列出尚未完整入库的 QE 实验或演进任务，选中后可将其配置、指标、曲线、因子和审计原文记录写入数仓。
              <span className="pv2-mono"> {API_BASE}/qe-archive </span> API；不会重启或影响生产 8001。
            </p>
          </div>
          <div className="pv2-row-actions">
            <button className="pv2-button-primary" onClick={() => void load()} disabled={loading} type="button">
              {loading ? "刷新中" : "刷新候选"}
            </button>
          </div>
        </div>
      </section>

      <ErrorPanel error={error} title="QE Archive 操作失败" />

      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="归档 Run" value={formatCompact(summary?.run_count || 0, 0)} hint={`有效 ${formatCompact(validRuns, 0)} / 无效 ${formatCompact(invalidRuns, 0)}`} tone="info" />
        <MetricCard label="待处理 Outbox" value={formatCompact(pendingOutbox, 0)} hint="loop/experiment 完成后进入 outbox" tone={pendingOutbox > 0 ? "warning" : "success"} />
        <MetricCard label="Top-K 证据覆盖" value={formatCompact(topkRows.filter((row) => row.topk_quality_status === "ok").length, 0)} hint={`当前 ${topkRows.length} 条中，缺失 ${topkRows.filter((row) => row.topk_quality_status !== "ok").length} 条`} tone="info" />
        <MetricCard label="最近归档" value={formatDateTime(summary?.latest_archived_at).slice(0, 10)} hint={formatDateTime(summary?.latest_archived_at)} />
      </div>

      <SectionCard title="CAGR/MDD 主榜" eyebrow="default order: Calmar; IC moved to diagnostics">
        <PaperTable
          rows={leaderboard}
          empty="暂无 leaderboard 数据"
          columns={[
            { key: "run", header: "归档实验", render: (row, index) => <><div>{row.task_id ? `演进 Loop ${row.loop_index ?? "-"}` : row.experiment_id ? "单次实验" : `归档实验 ${index + 1}`}</div><div className="pv2-muted">完成于 {formatDateTime(row.completed_at).slice(0, 10)}</div></> },
            { key: "model", header: "模型/标签", render: (row) => <><div>{row.model_type || "-"}</div><div className="pv2-muted">horizon {row.label_horizon ?? "-"} / 因子 {row.factor_count ?? "-"}</div></> },
            { key: "return", header: "CAGR / MDD / Calmar", render: (row) => <><div>{formatPct(row.cagr)} / {formatPct(row.max_drawdown)}</div><div className="pv2-muted">Calmar {formatNumber(row.calmar, 2)}</div></> },
            { key: "topk", header: "Top-K @20", render: (row) => <><div>return {formatSignedPct(row.topk_return_20)} / hit {formatPct(row.topk_hit_rate_20)}</div><div className="pv2-muted">@50 {formatSignedPct(row.topk_return_50)} / decay {formatSignedPct(row.topk_decay)}</div></> },
            { key: "quality", header: "Top-K 质量", render: (row) => <><span style={archiveStatusStyle(row.topk_quality_status === "ok" ? "archived" : "manual_only")}>{topkStatusLabel(row.topk_quality_status)}</span><div className="pv2-muted">覆盖 {formatCompact(row.topk_date_count || 0, 0)} 个交易日 / 关联 {formatCompact(row.topk_joined_observation_count || 0, 0)} 条</div></> },
            { key: "diagnostics", header: "IC 诊断", render: (row) => (
              <details>
                <summary className="pv2-muted">展开 IC/RankIC</summary>
                <div className="pv2-muted">IC {formatNumber(row.ic, 4)} / ICIR {formatNumber(row.icir, 3)}</div>
                <div className="pv2-muted">RankIC {formatNumber(row.rank_ic, 4)} / RankICIR {formatNumber(row.rank_icir, 3)}</div>
                <div className="pv2-muted">IR {formatNumber(row.information_ratio, 3)} / Sharpe {formatNumber(row.sharpe, 3)}</div>
              </details>
            ) },
          ]}
        />
      </SectionCard>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="Top-K 质量卡" eyebrow="@20 vs @50 / hit / decay / rankic / dispersion">
          <TopKQualityCards rows={topkRows} />
        </SectionCard>
        <SectionCard title="晋升候选新门" eyebrow="hard: CAGR/MDD/CV; soft: Top-K present only">
          <PaperTable
            rows={promotionCandidates}
            empty="暂无晋升候选"
            columns={[
              { key: "config", header: "配置", render: (row, index) => <><div>候选配置 {index + 1}｜{row.model_type || "模型未知"}</div><div className="pv2-muted">训练标签 {row.label_horizon ?? "-"}D / {formatCompact(row.distinct_seed_count || 0, 0)} 个随机种子</div></> },
              { key: "tier0", header: "硬门实际值", render: (row) => <><div>CAGR {formatPct(row.cagr_mean)} ≥ {formatPct(row.cagr_gate_threshold)}</div><div>MDD {formatPct(row.max_drawdown_mean)} ≥ {formatPct(row.max_drawdown_gate_threshold)}</div><div>CV {formatPct(row.cagr_cv)} &lt; {formatPct(row.cagr_cv_gate_threshold)}</div></> },
              { key: "gate", header: "门状态", render: (row) => <><StatusBadge status={gateText(row.passes_gate)} /><div className="pv2-muted">CAGR <StatusBadge status={gateText(row.cagr_gate_passes)} /> MDD <StatusBadge status={gateText(row.max_drawdown_gate_passes)} /> CV <StatusBadge status={gateText(row.cagr_cv_gate_passes)} /></div></> },
              { key: "topk", header: "Top-K 软门", render: (row) => <><div>return@20 {formatSignedPct(row.topk_return_20_mean)} / hit {formatPct(row.topk_hit_rate_20_mean)}</div><div className="pv2-muted">{row.topk_soft_gate_status === "present" ? "证据已具备" : "等待预测与标签证据"} / 样本 {formatCompact(row.topk_return_20_sample_count || 0, 0)}</div></> },
              { key: "diagnostics", header: "诊断", render: (row) => <><div>Calmar {formatNumber(row.calmar ?? row.calmar_mean, 2)}</div><div className="pv2-muted">ICIR {formatNumber(row.icir_mean, 3)} / RankICIR {formatNumber(row.rank_icir_mean, 3)}</div></> },
            ]}
          />
        </SectionCard>
      </div>

      <SectionCard title="历史补录候选列表" eyebrow="select experiments / archive all loops">
        <div className="pv2-form-grid">
          <label className="pv2-field">
            <span>候选状态</span>
            <select className="pv2-select" value={candidateStatus} onChange={(event) => { setCandidateStatus(event.target.value); setCandidatePage(1); }}>
              <option value="completed">仅 completed</option>
              <option value="terminal">终态 completed/failed/interrupted/cancelled</option>
              <option value="all">全部状态</option>
            </select>
          </label>
          <label className="pv2-field">
            <span>显示上限</span>
            <input className="pv2-input" type="number" min={1} max={500} value={candidatePageSize} onChange={(event) => { setCandidatePageSize(Math.max(1, Math.min(500, Number(event.target.value) || 20))); setCandidatePage(1); }} />
          </label>
          <label className="pv2-field">
            <span>已入库项</span>
            <select className="pv2-select" value={includeArchived ? "yes" : "no"} onChange={(event) => { setIncludeArchived(event.target.value === "yes"); setCandidatePage(1); }}>
              <option value="no">隐藏已完整入库</option>
              <option value="yes">显示已完整入库</option>
            </select>
          </label>
        </div>
        <div className="pv2-row-actions" style={{ marginTop: 12, marginBottom: 12 }}>
          <button className="pv2-button" type="button" onClick={selectPendingCandidates}>选择全部待入库</button>
          <button className="pv2-button-ghost" type="button" onClick={() => { setSelectedIds(new Set()); setSelectedLoopIds(new Set()); }}>清空选择</button>
          <span className="pv2-help">已选择 {selectedCandidates.length} 个候选、{selectedLoopIdList.length} 个精确 loop，预计写入 {selectedRunCount} 个实验；历史 Top-K 只有在预测与标签制品仍可恢复时才能重算。</span>
        </div>
        <div className="pv2-row-actions" style={{ marginTop: 8, marginBottom: 12 }}>
          <button className="pv2-button-ghost" type="button" aria-label="previous candidate page" onClick={() => setCandidatePage((page) => Math.max(1, page - 1))} disabled={candidatePage <= 1 || loading}>上一页</button>
          <span className="pv2-help" aria-label="candidate pagination status">第 {candidatePage} 页，每页 {candidatePageSize} 条，本页 {candidates.length} 条{candidateHasMore ? "，还有下一页" : "，已到末页"}</span>
          <button className="pv2-button-ghost" type="button" aria-label="next candidate page" onClick={() => setCandidatePage((page) => page + 1)} disabled={!candidateHasMore || loading}>下一页</button>
        </div>
        <PaperTable
          rows={candidates}
          empty="暂无可补录的 QE 实验"
          columns={[
            { key: "select", header: "选择", render: (row) => <input type="checkbox" checked={selectedIds.has(row.candidate_id)} disabled={n(row.pending_run_count) === 0} onChange={() => toggleCandidate(row.candidate_id)} aria-label={`选择 ${row.display_name || row.candidate_id}`} /> },
            { key: "type", header: "实验类型", render: (row) => <><div>{candidateTypeLabel(row)}</div><div className="pv2-muted">{row.experiment_type || "-"}</div></> },
            { key: "name", header: "实验说明", render: (row) => <><div>{row.display_name || "-"}</div><div className="pv2-muted pv2-mono">{shortHash(candidatePrimaryId(row))}</div><div className="pv2-muted">{row.description || "-"}</div></> },
            { key: "loops", header: "Loop / 入库", render: (row) => renderCandidateLoopControls(row) },
            { key: "status", header: "状态", render: (row) => <><StatusBadge status={row.status} /><div style={{ marginTop: 4 }}><ArchiveStatusPill status={row.is_fully_archived ? "fully_archived" : n(row.recommended_run_count) > 0 ? "recommended" : n(row.pending_run_count) > 0 ? "eligible" : "not_archived"} /></div></> },
            { key: "meta", header: "模型/因子", render: (row) => <><div>{row.model_id || row.model_catalog_id || "-"}</div><div className="pv2-muted">horizon {row.label_horizon ?? "-"} / 因子 {row.factor_count ?? "-"}</div></> },
            { key: "time", header: "执行时间", render: (row) => <><div>开始 {formatDateTime(row.started_at || row.created_at)}</div><div className="pv2-muted">结束 {formatDateTime(row.completed_at || row.updated_at)}</div></> },
          ]}
        />
        <div className="pv2-readable-panel" style={{ marginTop: 12 }}>
          <div className="pv2-readable-table">
            <div className="pv2-readable-row"><div className="pv2-readable-key">全部数据录入</div><div className="pv2-readable-value">写入配置、参数、因子列表、账户摘要、指标、曲线和审计原文记录；演进任务会按 loop 展开逐个入库。</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">质量阈值说明</div><div className="pv2-readable-value">最少 {QUALITY_GATE.min_metrics} 个指标、{QUALITY_GATE.min_curves} 条曲线、{QUALITY_GATE.min_factors} 条因子记录是写入后的完整性校验，不是采集范围开关。</div></div>
          </div>
        </div>
        <div className="pv2-form-grid" style={{ marginTop: 12 }}>
          <label className="pv2-field">
            <span>写入确认</span>
            <input className="pv2-input" value={writeConfirm} onChange={(event) => setWriteConfirm(event.target.value)} placeholder={WRITE_CONFIRM_TEXT} />
          </label>
          <div className="pv2-field"><span>&nbsp;</span><button className="pv2-button-ghost" type="button" aria-label="fill archive write confirm" onClick={() => setWriteConfirm(WRITE_CONFIRM_TEXT)}>填入确认文本</button></div>
          <div className="pv2-field"><span>&nbsp;</span><button className="pv2-button" type="button" onClick={() => void runBackfill(false)} disabled={backfillBusy || selectionCount === 0}>dry-run 预览选中项</button></div>
          <div className="pv2-field"><span>&nbsp;</span><button className="pv2-button-danger" type="button" aria-label="write selected candidates to archive" onClick={() => void runBackfill(true)} disabled={Boolean(writeDisabledReason)}>写入数仓</button></div>
        </div>
        {writeDisabledReason ? <div className="pv2-help" aria-label="archive write disabled reason">{writeDisabledReason}</div> : <div className="pv2-help">已满足正式入库条件，点击“写入数仓”会执行 confirmed write。</div>}
        <div style={{ marginTop: 16 }}><ReportSummary report={backfillReport} /></div>
      </SectionCard>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="实时入库队列" eyebrow="outbox / job status">
          <div className="pv2-readable-list">
            <div><div className="pv2-label">Outbox 状态</div><StatusCountStrip counts={summary?.outbox_status_counts} empty="暂无 outbox 事件" /></div>
            <div><div className="pv2-label">Worker Job 状态</div><StatusCountStrip counts={summary?.archive_job_status_counts} empty="暂无 worker job" /></div>
          </div>
        </SectionCard>

        <SectionCard title="手动处理队列" eyebrow="safe one-shot worker">
          <div className="pv2-form-grid">
            <label className="pv2-field"><span>处理条数</span><input className="pv2-input" type="number" min={1} max={100} value={workerLimit} onChange={(event) => setWorkerLimit(Number(event.target.value))} /></label>
            <label className="pv2-field"><span>确认文本</span><input className="pv2-input" value={workerConfirm} onChange={(event) => setWorkerConfirm(event.target.value)} placeholder={WORKER_CONFIRM_TEXT} /></label>
            <div className="pv2-field"><span>&nbsp;</span><button className="pv2-button-primary" type="button" onClick={() => void runWorkerOnce()} disabled={workerBusy || workerConfirm !== WORKER_CONFIRM_TEXT}>{workerBusy ? "处理中" : "处理一次 Outbox"}</button></div>
          </div>
          {workerReport ? (
            <div className="pv2-grid pv2-grid-3" style={{ marginTop: 12 }}>
              <MetricCard label="Claimed" value={formatCompact(workerReport.claimed || 0, 0)} />
              <MetricCard label="Completed" value={formatCompact(workerReport.completed || 0, 0)} tone="success" />
              <MetricCard label="Failed" value={formatCompact(workerReport.failed || 0, 0)} tone={workerReport.failed ? "danger" : "neutral"} />
            </div>
          ) : <div className="pv2-help">Worker 不会常驻运行；每次处理都需要确认文本。</div>}
        </SectionCard>
      </div>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="实验质量核对" eyebrow="配置 / 指标 / 曲线 / 因子">
          <div className="pv2-form-grid" style={{ marginBottom: 12 }}>
            <BusinessSearchSelect testId="qe-archive-quality-run" label="归档实验" value={qualityRunId} options={qualityRunOptions} search={runSearch} onSearchChange={(value) => { setRunSearch(value); setQualityRunId(""); setQuality(null); }} onValueChange={(value) => { setQualityRunId(value); setQuality(null); }} searchPlaceholder="按任务名称、模型或完成日期搜索" emptyLabel="请选择归档实验" loading={qualityBusy} required />
            <label className="pv2-field"><span>实验类型</span><select className="pv2-select" value={runTypeFilter} onChange={(event) => { setRunTypeFilter(event.target.value); setQualityRunId(""); setQuality(null); }}><option value="">全部类型</option><option value="evolution_loop">演进 Loop</option><option value="single_experiment">单次实验</option><option value="qe_experiment">QE 实验</option></select></label>
            <label className="pv2-field"><span>完成日期起</span><input className="pv2-input" type="date" value={runCompletedFrom} onChange={(event) => { setRunCompletedFrom(event.target.value); setQualityRunId(""); setQuality(null); }} /></label>
            <label className="pv2-field"><span>完成日期止</span><input className="pv2-input" type="date" value={runCompletedTo} onChange={(event) => { setRunCompletedTo(event.target.value); setQualityRunId(""); setQuality(null); }} /></label>
            <div className="pv2-field"><span>&nbsp;</span><button className="pv2-button" type="button" aria-label="check run quality" onClick={() => void lookupQuality()} disabled={qualityBusy || !qualityRunId.trim()}>{qualityBusy ? "查询中" : "查询质量"}</button></div>
            <div className="pv2-field"><span>&nbsp;</span><button className="pv2-button-ghost" type="button" aria-label="refresh run list" onClick={() => void loadArchivedRuns()} disabled={qualityBusy}>{qualityBusy ? "刷新中" : "刷新候选"}</button></div>
          </div>
          <QualityPanel quality={quality} />
        </SectionCard>

        <SectionCard title="最近 Outbox" eyebrow="realtime capture queue">
          <PaperTable
            rows={latestRows}
            empty="暂无 outbox 事件"
            columns={[
              { key: "event", header: "事件", render: (row) => <><StatusBadge status={row.status} /><div>{row.event_type}</div><div className="pv2-muted pv2-mono">{shortHash(row.event_id)}</div></> },
              { key: "source", header: "来源", render: (row) => <><div className="pv2-mono">{shortHash(row.source_id)}</div><div className="pv2-muted pv2-mono">{shortHash(row.source_sub_id)}</div></> },
              { key: "retry", header: "重试", render: (row) => formatNumber(row.retry_count || 0, 0) },
              { key: "time", header: "创建时间", render: (row) => formatDateTime(row.created_at) },
            ]}
          />
        </SectionCard>
      </div>

      <SectionCard title="最近 Worker Jobs" eyebrow="archive write lifecycle">
        <PaperTable
          rows={latestJobs}
          empty="暂无 worker job"
          columns={[
            { key: "job", header: "Job", render: (row) => <><StatusBadge status={row.status} /><div className="pv2-mono">{shortHash(row.job_id)}</div></> },
            { key: "type", header: "类型", render: (row) => row.job_type },
            { key: "run", header: "Run", render: (row) => <span className="pv2-mono">{shortHash(row.run_id)}</span> },
            { key: "retry", header: "重试", render: (row) => formatNumber(row.retry_count || 0, 0) },
            { key: "time", header: "完成时间", render: (row) => formatDateTime(row.completed_at || row.updated_at) },
            { key: "error", header: "错误", render: (row) => row.error_message ? <span style={{ color: "#b91c1c" }}>{row.error_message}</span> : <span className="pv2-muted">-</span> },
          ]}
        />
      </SectionCard>

      <LongTrendComparisonPanel />
    </main>
  );
}
