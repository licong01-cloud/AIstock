
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
  type ArchivedRunListItem,
  type ArchiveSummary,
  type BackfillCandidate,
  type BackfillReport,
  type OutboxEvent,
  type RunQuality,
  type WorkerRunReport,
  qeArchiveApi,
} from "@/lib/qe-archive/api";

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

function runListLabel(run: ArchivedRunListItem): string {
  const source = run.loop_id || run.experiment_id || run.task_id || run.logical_experiment_id || run.run_id;
  const loop = run.loop_index ? ` Loop${run.loop_index}` : "";
  return `${shortHash(run.run_id)} | ${run.run_type || "-"} | ${source || "-"}${loop}`;
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
  if (!report) return <div className="pv2-help">请选择候选实验并先执行 dry-run 预览，确认后再写入数仓。</div>;
  const rows = report.results || [];
  return (
    <div className="pv2-readable-list">
      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="处理数量" value={formatCompact(report.processed_count || 0, 0)} hint={report.dry_run ? "dry-run 预览" : "已写入数仓"} tone={report.write_enabled ? "success" : "info"} />
        <MetricCard label="来源" value={report.source || "-"} hint={`状态筛选 ${report.status || "completed"}`} />
        <MetricCard label="模式" value={report.write_enabled ? "写入" : "预览"} hint={report.write_enabled ? "已触发持久化" : "未写数据库"} tone={report.write_enabled ? "warning" : "neutral"} />
        <MetricCard label="涉及 run" value={formatCompact(rows.length, 0)} hint="展开 loop/experiment 后的数量" />
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
    ["raw", record.raw_payloads_written ?? record.raw_payload_count],
  ]
    .filter(([, value]) => value !== undefined && value !== null)
    .map(([key, value]) => `${key} ${value}`);
  return parts.length ? parts.join(" / ") : "无可写统计";
}

function QualityPanel({ quality }: { quality: RunQuality | null }) {
  if (!quality) return <div className="pv2-help">输入 run_id 后查询该实验是否已完整保存配置、指标、曲线、因子和 raw payload。</div>;
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
    { name: "原始 payload", value: quality.raw_payload_count },
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
          <div className="pv2-readable-row"><div className="pv2-readable-key">Run</div><div className="pv2-readable-value pv2-mono">{quality.run_id}</div></div>
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

export default function QEArchivePage() {
  const [summary, setSummary] = useState<ArchiveSummary | null>(null);
  const [outbox, setOutbox] = useState<OutboxEvent[]>([]);
  const [jobs, setJobs] = useState<ArchiveJob[]>([]);
  const [archivedRuns, setArchivedRuns] = useState<ArchivedRunListItem[]>([]);
  const [candidates, setCandidates] = useState<BackfillCandidate[]>([]);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [candidateLimit, setCandidateLimit] = useState(100);
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
  const [qualityBusy, setQualityBusy] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [nextSummary, nextOutbox, nextJobs, nextRuns, nextCandidates] = await Promise.all([
        qeArchiveApi.health(),
        qeArchiveApi.outbox(30),
        qeArchiveApi.jobs(30),
        qeArchiveApi.runs({ limit: 100 }),
        qeArchiveApi.backfillCandidates({ limit: candidateLimit, status: candidateStatus, include_archived: includeArchived }),
      ]);
      setSummary(nextSummary);
      setOutbox(nextOutbox);
      setJobs(nextJobs);
      setArchivedRuns(nextRuns);
      setCandidates(nextCandidates.candidates || []);
      setSelectedIds((previous) => new Set([...previous].filter((id) => (nextCandidates.candidates || []).some((item) => item.candidate_id === id))));
    } catch (err) {
      setError(err);
    } finally {
      setLoading(false);
    }
  }, [candidateLimit, candidateStatus, includeArchived]);

  useEffect(() => {
    void load();
  }, [load]);

  const validRuns = n(summary?.research_valid_counts?.true);
  const invalidRuns = n(summary?.research_valid_counts?.false);
  const completedJobs = n(summary?.archive_job_status_counts?.completed);
  const failedJobs = n(summary?.archive_job_status_counts?.failed);
  const pendingOutbox = n(summary?.pending_outbox_count);

  const latestRows = useMemo(() => outbox.slice(0, 12), [outbox]);
  const latestJobs = useMemo(() => jobs.slice(0, 12), [jobs]);
  const selectedCandidates = useMemo(() => candidates.filter((item) => selectedIds.has(item.candidate_id)), [candidates, selectedIds]);
  const selectedTaskIds = selectedCandidates.filter((item) => item.candidate_type === "evolution_task" && item.task_id).map((item) => String(item.task_id));
  const selectedExperimentIds = selectedCandidates.filter((item) => item.candidate_type === "single_experiment" && item.experiment_id).map((item) => String(item.experiment_id));
  const selectedRunCount = selectedCandidates.reduce((sum, item) => sum + n(item.pending_run_count), 0);
  const pendingCandidateCount = candidates.filter((item) => n(item.pending_run_count) > 0).length;

  function toggleCandidate(candidateId: string) {
    setSelectedIds((previous) => {
      const next = new Set(previous);
      if (next.has(candidateId)) next.delete(candidateId);
      else next.add(candidateId);
      return next;
    });
  }

  function selectPendingCandidates() {
    setSelectedIds(new Set(candidates.filter((item) => n(item.pending_run_count) > 0).map((item) => item.candidate_id)));
  }

  async function runBackfill(write: boolean) {
    if (!selectedCandidates.length) {
      setError(new Error("请先在候选列表中选择需要写入数仓的 QE 实验或任务。"));
      return;
    }
    setBackfillBusy(true);
    setError(null);
    try {
      const report = await qeArchiveApi.backfill({
        source: "all",
        task_ids: selectedTaskIds,
        experiment_ids: selectedExperimentIds,
        status: candidateStatus,
        include_archived: false,
        write,
        confirm_write: write ? writeConfirm : "",
        validate_after_write: true,
        ...QUALITY_GATE,
      });
      setBackfillReport(report);
      if (write) await load();
    } catch (err) {
      setError(err);
    } finally {
      setBackfillBusy(false);
    }
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
      setQuality(await qeArchiveApi.quality(runId));
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
              从数据库列出尚未完整入库的 QE 实验或演进任务，选中后可将其所有可解析配置、指标、曲线、因子和原始 payload 写入数仓。
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
        <MetricCard label="待补录候选" value={formatCompact(pendingCandidateCount, 0)} hint={`当前 ${formatCompact(candidates.length, 0)} 条`} tone={pendingCandidateCount > 0 ? "warning" : "success"} />
        <MetricCard label="最近归档" value={formatDateTime(summary?.latest_archived_at).slice(0, 10)} hint={formatDateTime(summary?.latest_archived_at)} />
      </div>

      <SectionCard title="历史补录候选列表" eyebrow="select experiments / archive all loops">
        <div className="pv2-form-grid">
          <label className="pv2-field">
            <span>候选状态</span>
            <select className="pv2-select" value={candidateStatus} onChange={(event) => setCandidateStatus(event.target.value)}>
              <option value="completed">仅 completed</option>
              <option value="terminal">终态 completed/failed/interrupted/cancelled</option>
              <option value="all">全部状态</option>
            </select>
          </label>
          <label className="pv2-field">
            <span>显示上限</span>
            <input className="pv2-input" type="number" min={1} max={500} value={candidateLimit} onChange={(event) => setCandidateLimit(Number(event.target.value))} />
          </label>
          <label className="pv2-field">
            <span>已入库项</span>
            <select className="pv2-select" value={includeArchived ? "yes" : "no"} onChange={(event) => setIncludeArchived(event.target.value === "yes")}>
              <option value="no">隐藏已完整入库</option>
              <option value="yes">显示已完整入库</option>
            </select>
          </label>
        </div>
        <div className="pv2-row-actions" style={{ marginTop: 12, marginBottom: 12 }}>
          <button className="pv2-button" type="button" onClick={selectPendingCandidates}>选择全部待入库</button>
          <button className="pv2-button-ghost" type="button" onClick={() => setSelectedIds(new Set())}>清空选择</button>
          <span className="pv2-help">已选择 {selectedCandidates.length} 个候选，预计展开 {selectedRunCount} 个 run；演进任务会自动包含其全部符合状态的 loop。</span>
        </div>
        <PaperTable
          rows={candidates}
          empty="暂无可补录的 QE 实验"
          columns={[
            { key: "select", header: "选择", render: (row) => <input type="checkbox" checked={selectedIds.has(row.candidate_id)} disabled={n(row.pending_run_count) === 0} onChange={() => toggleCandidate(row.candidate_id)} aria-label={`选择 ${row.display_name || row.candidate_id}`} /> },
            { key: "type", header: "实验类型", render: (row) => <><div>{candidateTypeLabel(row)}</div><div className="pv2-muted">{row.experiment_type || "-"}</div></> },
            { key: "name", header: "实验说明", render: (row) => <><div>{row.display_name || "-"}</div><div className="pv2-muted pv2-mono">{shortHash(candidatePrimaryId(row))}</div><div className="pv2-muted">{row.description || "-"}</div></> },
            { key: "loops", header: "Loop / 入库", render: (row) => <><div>{formatNumber(row.archived_run_count || 0, 0)} / {formatNumber(row.selected_run_count || 0, 0)}</div><div className="pv2-muted">总 loop {formatNumber(row.loop_count || 0, 0)}，待入库 {formatNumber(row.pending_run_count || 0, 0)}</div></> },
            { key: "status", header: "状态", render: (row) => <><StatusBadge status={row.status} /><div className="pv2-muted">{row.is_fully_archived ? "已完整入库" : "待入库"}</div></> },
            { key: "meta", header: "模型/因子", render: (row) => <><div>{row.model_id || row.model_catalog_id || "-"}</div><div className="pv2-muted">horizon {row.label_horizon ?? "-"} / 因子 {row.factor_count ?? "-"}</div></> },
            { key: "time", header: "执行时间", render: (row) => <><div>开始 {formatDateTime(row.started_at || row.created_at)}</div><div className="pv2-muted">结束 {formatDateTime(row.completed_at || row.updated_at)}</div></> },
          ]}
        />
        <div className="pv2-readable-panel" style={{ marginTop: 12 }}>
          <div className="pv2-readable-table">
            <div className="pv2-readable-row"><div className="pv2-readable-key">全部数据录入</div><div className="pv2-readable-value">写入配置、参数、因子列表、账户摘要、指标、曲线、原始 payload；演进任务会按 loop 展开逐个入库。</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">质量阈值说明</div><div className="pv2-readable-value">最少 {QUALITY_GATE.min_metrics} 个指标、{QUALITY_GATE.min_curves} 条曲线、{QUALITY_GATE.min_factors} 条因子记录是写入后的完整性校验，不是采集范围开关。</div></div>
          </div>
        </div>
        <div className="pv2-form-grid" style={{ marginTop: 12 }}>
          <label className="pv2-field">
            <span>写入确认</span>
            <input className="pv2-input" value={writeConfirm} onChange={(event) => setWriteConfirm(event.target.value)} placeholder={WRITE_CONFIRM_TEXT} />
          </label>
          <div className="pv2-field"><span>&nbsp;</span><button className="pv2-button" type="button" onClick={() => void runBackfill(false)} disabled={backfillBusy || selectedCandidates.length === 0}>dry-run 预览选中项</button></div>
          <div className="pv2-field"><span>&nbsp;</span><button className="pv2-button-danger" type="button" onClick={() => void runBackfill(true)} disabled={backfillBusy || selectedCandidates.length === 0 || writeConfirm !== WRITE_CONFIRM_TEXT}>写入数仓</button></div>
        </div>
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
        <SectionCard title="Run 质量核对" eyebrow="config / metrics / curves / factors">
          <div className="pv2-form-grid" style={{ marginBottom: 12 }}>
            <label className="pv2-field">
              <span>最近入库 Run</span>
              <select className="pv2-select" value={qualityRunId} onChange={(event) => setQualityRunId(event.target.value)} aria-label="Select archived run for quality">
                <option value="">选择已入库 Run</option>
                {archivedRuns.map((run) => (
                  <option key={run.run_id} value={run.run_id}>{runListLabel(run)}</option>
                ))}
              </select>
            </label>
            <label className="pv2-field">
              <span>Run ID</span>
              <input className="pv2-input" value={qualityRunId} onChange={(event) => setQualityRunId(event.target.value)} placeholder="qear_run_..." />
            </label>
            <div className="pv2-field"><span>&nbsp;</span><button className="pv2-button" type="button" onClick={() => void lookupQuality()} disabled={qualityBusy || !qualityRunId.trim()}>{qualityBusy ? "查询中" : "查询质量"}</button></div>
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
    </main>
  );
}
