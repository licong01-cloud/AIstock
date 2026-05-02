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
  type BackfillReport,
  type OutboxEvent,
  type RunQuality,
  type WorkerRunReport,
  qeArchiveApi,
} from "@/lib/qe-archive/api";

const WRITE_CONFIRM_TEXT = "QE_ARCHIVE_WRITE";
const WORKER_CONFIRM_TEXT = "QE_ARCHIVE_WORKER_RUN";

type BackfillSource = "experiment" | "loop" | "all";

function splitIds(value: string): string[] {
  return value
    .split(/[\n,;\s]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

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
  if (!report) return <div className="pv2-help">尚未执行补录预览或写入。</div>;
  const rows = report.results || [];
  return (
    <div className="pv2-readable-list">
      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="处理对象" value={formatCompact(report.processed_count || 0, 0)} hint={report.dry_run ? "dry-run 仅预览" : "已确认写入"} tone={report.write_enabled ? "success" : "info"} />
        <MetricCard label="来源" value={report.source || "-"} hint={`状态过滤 ${report.status || "completed"}`} />
        <MetricCard label="模式" value={report.write_enabled ? "写入" : "预览"} hint={report.write_enabled ? "已要求写确认" : "不会改数据库"} tone={report.write_enabled ? "warning" : "neutral"} />
        <MetricCard label="返回 run" value={formatCompact(rows.length, 0)} hint="每个 loop/experiment 一行" />
      </div>
      <PaperTable
        rows={rows.slice(0, 20)}
        empty="暂无补录结果"
        columns={[
          { key: "run", header: "归档 Run", render: (row) => <span className="pv2-mono">{shortHash(row.run_id)}</span> },
          { key: "source", header: "来源", render: (row) => <><div>{row.event_type || "-"}</div><div className="pv2-muted pv2-mono">{row.source_sub_id || row.source_id || "-"}</div></> },
          { key: "stats", header: "采集概况", render: (row) => <span>{formatStats(row.stats)}</span> },
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
    ["raw", record.raw_payloads_written ?? record.raw_payload_count],
  ]
    .filter(([, value]) => value !== undefined && value !== null)
    .map(([key, value]) => `${key} ${value}`);
  return parts.length ? parts.join(" / ") : "已生成归档负载";
}

function QualityPanel({ quality }: { quality: RunQuality | null }) {
  if (!quality) return <div className="pv2-help">输入 run_id 后可核对配置、可复现性、指标、曲线、因子与原始 payload 行数。</div>;
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
    { name: "原始 payload", value: quality.raw_payload_count },
    { name: "artifact manifest", value: quality.artifact_count },
    { name: "优先级评分", value: quality.priority_score_count },
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
          { key: "name", header: "检查项", render: (row) => row.name },
          { key: "value", header: "行数", render: (row) => formatCompact(row.value || 0, 0) },
        ]}
      />
    </div>
  );
}

export default function QEArchivePage() {
  const [summary, setSummary] = useState<ArchiveSummary | null>(null);
  const [outbox, setOutbox] = useState<OutboxEvent[]>([]);
  const [jobs, setJobs] = useState<ArchiveJob[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [source, setSource] = useState<BackfillSource>("loop");
  const [loopIds, setLoopIds] = useState("");
  const [experimentIds, setExperimentIds] = useState("");
  const [taskId, setTaskId] = useState("");
  const [loopIndex, setLoopIndex] = useState("");
  const [limit, setLimit] = useState(20);
  const [minMetrics, setMinMetrics] = useState(60);
  const [minCurves, setMinCurves] = useState(3000);
  const [minFactors, setMinFactors] = useState(1);
  const [requireAccount, setRequireAccount] = useState(true);
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
      const [nextSummary, nextOutbox, nextJobs] = await Promise.all([
        qeArchiveApi.health(),
        qeArchiveApi.outbox(30),
        qeArchiveApi.jobs(30),
      ]);
      setSummary(nextSummary);
      setOutbox(nextOutbox);
      setJobs(nextJobs);
    } catch (err) {
      setError(err);
    } finally {
      setLoading(false);
    }
  }, []);

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

  async function runBackfill(write: boolean) {
    setBackfillBusy(true);
    setError(null);
    try {
      const report = await qeArchiveApi.backfill({
        source,
        loop_ids: splitIds(loopIds),
        experiment_ids: splitIds(experimentIds),
        task_id: taskId.trim() || null,
        loop_index: loopIndex.trim() ? Number(loopIndex) : null,
        status: "completed",
        limit,
        write,
        confirm_write: write ? writeConfirm : "",
        validate_after_write: true,
        min_metrics: minMetrics,
        min_curves: minCurves,
        min_factors: minFactors,
        require_account_summary: requireAccount,
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
              面向每个实验和每个 loop 的配置、因子列表、回测指标、曲线、账户摘要与可复现性核对。当前页面只通过
              <span className="pv2-mono"> {API_BASE}/qe-archive </span> API 操作，不启动生产 8001 之外的新服务。
            </p>
          </div>
          <div className="pv2-row-actions">
            <button className="pv2-button-primary" onClick={() => void load()} disabled={loading} type="button">
              {loading ? "刷新中" : "刷新数仓状态"}
            </button>
          </div>
        </div>
      </section>

      <ErrorPanel error={error} title="QE Archive 操作失败" />

      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="归档 Run" value={formatCompact(summary?.run_count || 0, 0)} hint={`有效 ${formatCompact(validRuns, 0)} / 无效 ${formatCompact(invalidRuns, 0)}`} tone="info" />
        <MetricCard label="待处理 Outbox" value={formatCompact(pendingOutbox, 0)} hint="loop/experiment 完成后先进入 outbox" tone={pendingOutbox > 0 ? "warning" : "success"} />
        <MetricCard label="Worker 完成" value={formatCompact(completedJobs, 0)} hint={`失败 ${formatCompact(failedJobs, 0)}`} tone={failedJobs > 0 ? "danger" : "success"} />
        <MetricCard label="最新归档" value={formatDateTime(summary?.latest_archived_at).slice(0, 10)} hint={formatDateTime(summary?.latest_archived_at)} />
      </div>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="实时入库队列" eyebrow="outbox / job status">
          <div className="pv2-readable-list">
            <div>
              <div className="pv2-label">Outbox 状态</div>
              <StatusCountStrip counts={summary?.outbox_status_counts} empty="暂无 outbox 事件" />
            </div>
            <div>
              <div className="pv2-label">Worker Job 状态</div>
              <StatusCountStrip counts={summary?.archive_job_status_counts} empty="暂无 worker job" />
            </div>
          </div>
        </SectionCard>

        <SectionCard title="手动处理队列" eyebrow="safe one-shot worker">
          <div className="pv2-form-grid">
            <label className="pv2-field">
              <span>处理条数</span>
              <input className="pv2-input" type="number" min={1} max={100} value={workerLimit} onChange={(event) => setWorkerLimit(Number(event.target.value))} />
            </label>
            <label className="pv2-field">
              <span>确认文本</span>
              <input className="pv2-input" value={workerConfirm} onChange={(event) => setWorkerConfirm(event.target.value)} placeholder={WORKER_CONFIRM_TEXT} />
            </label>
            <div className="pv2-field">
              <span>&nbsp;</span>
              <button className="pv2-button-primary" type="button" onClick={() => void runWorkerOnce()} disabled={workerBusy || workerConfirm !== WORKER_CONFIRM_TEXT}>
                {workerBusy ? "处理中" : "处理一次 Outbox"}
              </button>
            </div>
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

      <SectionCard title="历史补录 API" eyebrow="dry-run first / confirmed write">
        <div className="pv2-form-grid">
          <label className="pv2-field">
            <span>补录来源</span>
            <select className="pv2-select" value={source} onChange={(event) => setSource(event.target.value as BackfillSource)}>
              <option value="loop">QE Loop</option>
              <option value="experiment">单次实验</option>
              <option value="all">实验 + Loop</option>
            </select>
          </label>
          <label className="pv2-field">
            <span>自动扫描上限</span>
            <input className="pv2-input" type="number" min={1} max={500} value={limit} onChange={(event) => setLimit(Number(event.target.value))} />
          </label>
          <label className="pv2-field">
            <span>写入确认</span>
            <input className="pv2-input" value={writeConfirm} onChange={(event) => setWriteConfirm(event.target.value)} placeholder={WRITE_CONFIRM_TEXT} />
          </label>
        </div>
        <div className="pv2-form-grid" style={{ marginTop: 12 }}>
          <label className="pv2-field">
            <span>Loop IDs</span>
            <textarea className="pv2-textarea" value={loopIds} onChange={(event) => setLoopIds(event.target.value)} placeholder="每行一个 loop_id，可留空按 completed 自动扫描" />
          </label>
          <label className="pv2-field">
            <span>Experiment IDs</span>
            <textarea className="pv2-textarea" value={experimentIds} onChange={(event) => setExperimentIds(event.target.value)} placeholder="每行一个 experiment_id" />
          </label>
          <div className="pv2-readable-list">
            <label className="pv2-field">
              <span>Task ID + Loop Index</span>
              <input className="pv2-input" value={taskId} onChange={(event) => setTaskId(event.target.value)} placeholder="可选 task_id" />
              <input className="pv2-input" value={loopIndex} onChange={(event) => setLoopIndex(event.target.value)} placeholder="可选 loop_index" />
            </label>
            <div className="pv2-grid pv2-grid-3">
              <label className="pv2-field"><span>最少指标</span><input className="pv2-input" type="number" value={minMetrics} onChange={(event) => setMinMetrics(Number(event.target.value))} /></label>
              <label className="pv2-field"><span>最少曲线</span><input className="pv2-input" type="number" value={minCurves} onChange={(event) => setMinCurves(Number(event.target.value))} /></label>
              <label className="pv2-field"><span>最少因子</span><input className="pv2-input" type="number" value={minFactors} onChange={(event) => setMinFactors(Number(event.target.value))} /></label>
            </div>
            <label className="pv2-chip" style={{ width: "fit-content" }}>
              <input type="checkbox" checked={requireAccount} onChange={(event) => setRequireAccount(event.target.checked)} /> 要求账户摘要
            </label>
          </div>
        </div>
        <div className="pv2-row-actions" style={{ marginTop: 12 }}>
          <button className="pv2-button" type="button" onClick={() => void runBackfill(false)} disabled={backfillBusy}>先 dry-run 预览</button>
          <button className="pv2-button-danger" type="button" onClick={() => void runBackfill(true)} disabled={backfillBusy || writeConfirm !== WRITE_CONFIRM_TEXT}>确认写入数仓</button>
        </div>
        <div style={{ marginTop: 16 }}><ReportSummary report={backfillReport} /></div>
      </SectionCard>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="Run 质量核对" eyebrow="config / metrics / curves / factors">
          <div className="pv2-row-actions" style={{ marginBottom: 12 }}>
            <input className="pv2-input" value={qualityRunId} onChange={(event) => setQualityRunId(event.target.value)} placeholder="qear_run_..." />
            <button className="pv2-button" type="button" onClick={() => void lookupQuality()} disabled={qualityBusy || !qualityRunId.trim()}>{qualityBusy ? "查询中" : "查询质量"}</button>
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
