"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";

import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import JsonPanel from "@/components/paper-v2/JsonPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { formatCompact, formatNumber, shortHash } from "@/lib/paper-v2/format";
import { qeArchiveApi, type ArchiveSourceStatus } from "@/lib/qe-archive/api";
import {
  type JsonObject,
  type ResearchArtifactRef,
  type ResearchBackfillRun,
  type ResearchBacktestRecord,
  type ResearchComparison,
  type ResearchExperimentDetail,
  type ResearchExternalRunLink,
  type ResearchPipelineEvent,
  type ResearchStageAttempt,
  type ResearchStagePlan,
  researchPipelineApi,
} from "@/lib/research-pipeline/api";

function display(value: unknown): string {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "boolean") return value ? "yes" : "no";
  if (typeof value === "number") return Number.isFinite(value) ? formatNumber(value, Number.isInteger(value) ? 0 : 4) : "-";
  if (Array.isArray(value)) return value.length ? value.map((item) => display(item)).join(" / ") : "-";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function formatDateTime(value: unknown): string {
  const text = String(value || "");
  return text ? text.replace("T", " ").slice(0, 19) : "-";
}

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function metricText(value: unknown, digits = 4): string {
  if (value === null || value === undefined || value === "") return "-";
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return formatNumber(n, digits);
}

function compactText(value: unknown, size = 9): string {
  return shortHash(value, size);
}

function stageAttemptMap(attempts: ResearchStageAttempt[]): Map<string, ResearchStageAttempt[]> {
  const byStage = new Map<string, ResearchStageAttempt[]>();
  for (const attempt of attempts) {
    const stage = attempt.stage_name || "unknown";
    const rows = byStage.get(stage) || [];
    rows.push(attempt);
    byStage.set(stage, rows);
  }
  for (const rows of byStage.values()) rows.sort((a, b) => (a.attempt_no || 0) - (b.attempt_no || 0));
  return byStage;
}

function latestAttempt(attempts: ResearchStageAttempt[]): ResearchStageAttempt | undefined {
  return [...attempts].sort((a, b) => (b.attempt_no || 0) - (a.attempt_no || 0))[0];
}

function countsBy<T>(rows: T[], keyer: (row: T) => string | undefined | null): Record<string, number> {
  return rows.reduce<Record<string, number>>((acc, row) => {
    const key = keyer(row) || "unknown";
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});
}

function CountChips({ counts }: { counts: Record<string, number> }) {
  const entries = Object.entries(counts);
  if (!entries.length) return <span className="pv2-muted">No counts</span>;
  return (
    <div className="pv2-chip-row">
      {entries.map(([key, count]) => <span className="pv2-chip" key={key}>{key}: {count}</span>)}
    </div>
  );
}

function KeyValuePanel({ rows }: { rows: Array<[string, unknown]> }) {
  return (
    <div className="pv2-readable-panel">
      <div className="pv2-readable-table">
        {rows.map(([key, value]) => (
          <div className="pv2-readable-row" key={key}>
            <div className="pv2-readable-key">{key}</div>
            <div className="pv2-readable-value">{display(value)}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function metricFrom(record: ResearchBacktestRecord, key: "ann" | "mdd" | "ir" | "ic" | "rank_ic" | "sharpe" | "turnover"): string {
  const direct = record[key];
  if (direct !== undefined && direct !== null) return metricText(direct);
  const metrics = isObject(record.metrics_json) ? record.metrics_json : {};
  return metricText(metrics[key]);
}

function sourceLabel(record: ResearchBacktestRecord): string {
  const loop = record.source_loop_index !== null && record.source_loop_index !== undefined ? ` loop ${record.source_loop_index}` : "";
  return `${record.source_type || "-"} / ${compactText(record.source_task_id)}${loop}`;
}

function archiveStatusLabel(status?: string): string {
  switch (status) {
    case "archived": return "已入仓";
    case "fully_archived": return "全部入仓";
    case "partially_archived": return "部分入仓";
    case "not_archived":
    default:
      return "未入仓";
  }
}

function StageTimeline({ stages, attempts }: { stages: ResearchStagePlan[]; attempts: ResearchStageAttempt[] }) {
  const attemptsByStage = useMemo(() => stageAttemptMap(attempts), [attempts]);
  return (
    <div className="pv2-readable-list">
      {stages.length ? stages.map((stage) => {
        const stageAttempts = attemptsByStage.get(stage.stage_name) || [];
        const latest = latestAttempt(stageAttempts);
        return (
          <div className="pv2-readable-item" key={stage.stage_id}>
            <div className="pv2-card-head">
              <div>
                <strong>{stage.stage_order}. {stage.stage_name}</strong><br />
                <span className="pv2-muted">latest attempt {stage.latest_attempt_no ?? latest?.attempt_no ?? "-"} / configured {formatDateTime(stage.updated_at)}</span>
              </div>
              <StatusBadge status={stage.status} />
            </div>
            <div className="pv2-chip-row" style={{ marginTop: 10 }}>
              <span className="pv2-chip">attempts {stageAttempts.length}</span>
              <span className="pv2-chip">stage_id {compactText(stage.stage_id)}</span>
              {latest?.completed_at ? <span className="pv2-chip">latest completed {formatDateTime(latest.completed_at)}</span> : null}
            </div>
            <details className="pv2-readable-item" style={{ marginTop: 10 }}>
              <summary>Show stage config and attempts</summary>
              <JsonPanel value={{ planned_config_json: stage.planned_config_json || {}, attempts: stageAttempts }} />
            </details>
          </div>
        );
      }) : <p className="pv2-muted">No stage plan.</p>}
    </div>
  );
}

function ArtifactTable({ rows }: { rows: ResearchArtifactRef[] }) {
  return (
    <PaperTable
      rows={rows}
      empty="No artifact refs."
      columns={[
        { key: "artifact", header: "Artifact", render: (row) => <><span className="pv2-mono">{compactText(row.artifact_ref_id)}</span><br /><span className="pv2-muted">{row.domain_type} / {compactText(row.domain_id)}</span></> },
        { key: "status", header: "Status", render: (row) => <StatusBadge status={row.status || "unknown"} /> },
        { key: "uri", header: "URI / SHA", render: (row) => <><span title={row.artifact_uri || ""}>{row.artifact_uri || "-"}</span><br /><span className="pv2-muted pv2-mono">{compactText(row.artifact_sha256)}</span></> },
        { key: "attempt", header: "Stage Attempt", render: (row) => <span className="pv2-mono">{compactText(row.stage_attempt_id)}</span> },
        { key: "time", header: "Time", render: (row) => <><div>{formatDateTime(row.updated_at)}</div><span className="pv2-muted">created {formatDateTime(row.created_at)}</span></> },
      ]}
    />
  );
}

function ExternalLinksTable({ rows }: { rows: ResearchExternalRunLink[] }) {
  return (
    <PaperTable
      rows={rows}
      empty="No external run links."
      columns={[
        { key: "run", header: "External Run", render: (row) => <><StatusBadge status={row.status || "linked"} /><br /><span>{row.run_type}</span><br /><span className="pv2-muted pv2-mono">{compactText(row.external_id)}</span></> },
        { key: "link", header: "Link", render: (row) => row.external_url ? <a href={row.external_url}>{row.external_url}</a> : <span className="pv2-muted">No URL</span> },
        { key: "attempt", header: "Stage Attempt", render: (row) => <span className="pv2-mono">{compactText(row.stage_attempt_id)}</span> },
        { key: "meta", header: "Metadata", render: (row) => <details className="pv2-readable-item"><summary>metadata</summary><JsonPanel value={row.metadata_json || {}} /></details> },
      ]}
    />
  );
}

function ComparisonTable({ rows }: { rows: ResearchComparison[] }) {
  return (
    <PaperTable
      rows={rows}
      empty="No comparison records."
      columns={[
        { key: "cmp", header: "Comparison", render: (row) => <><StatusBadge status={row.verdict} /><br /><span className="pv2-muted pv2-mono">{compactText(row.comparison_id)}</span></> },
        { key: "metrics", header: "Metrics", render: (row) => <><span>ann {metricText(row.metrics_json?.ann)}</span><br /><span className="pv2-muted">mdd {metricText(row.metrics_json?.mdd)} / ir {metricText(row.metrics_json?.ir)}</span></> },
        { key: "reason", header: "Reason", render: (row) => row.reason_md || <span className="pv2-muted">-</span> },
        { key: "time", header: "Time", render: (row) => <><div>{formatDateTime(row.created_at)}</div><span className="pv2-muted">{row.created_by || "-"}</span></> },
      ]}
    />
  );
}

function EventTable({ rows }: { rows: ResearchPipelineEvent[] }) {
  return (
    <PaperTable
      rows={rows.slice(0, 80)}
      empty="No pipeline events."
      columns={[
        { key: "event", header: "Event", render: (row) => <><StatusBadge status={row.severity || "info"} /><br /><span>{row.event_type}</span><br /><span className="pv2-muted pv2-mono">{compactText(row.event_id)}</span></> },
        { key: "message", header: "Message", render: (row) => <><div>{row.message}</div><span className="pv2-muted">{row.created_by || "-"}</span></> },
        { key: "attempt", header: "Attempt", render: (row) => <span className="pv2-mono">{compactText(row.stage_attempt_id)}</span> },
        { key: "time", header: "Time", render: (row) => formatDateTime(row.created_at) },
      ]}
    />
  );
}

export default function ResearchPipelineExperimentPage() {
  const params = useParams<{ experimentId: string }>();
  const experimentId = Array.isArray(params?.experimentId) ? params.experimentId[0] : params?.experimentId;

  const [experiment, setExperiment] = useState<ResearchExperimentDetail | null>(null);
  const [artifactRefs, setArtifactRefs] = useState<ResearchArtifactRef[]>([]);
  const [backtestRecords, setBacktestRecords] = useState<ResearchBacktestRecord[]>([]);
  const [backfillRuns, setBackfillRuns] = useState<ResearchBackfillRun[]>([]);
  const [dedupStatus, setDedupStatus] = useState("");
  const [representative, setRepresentative] = useState("");
  const [sourceTaskId, setSourceTaskId] = useState("");
  const [archiveStatus, setArchiveStatus] = useState<ArchiveSourceStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    if (!experimentId) return;
    setLoading(true);
    setError(null);
    try {
      const representativeFilter = representative === "true" ? true : representative === "false" ? false : undefined;
      const [nextExperiment, nextArtifacts, nextBacktests, nextBackfills] = await Promise.all([
        researchPipelineApi.experiment(experimentId),
        researchPipelineApi.artifactRefs(experimentId, { limit: 200 }),
        researchPipelineApi.backtestRecords(experimentId, {
          research_domain: "hmm",
          dedup_status: dedupStatus || undefined,
          qe_archive_representative: representativeFilter,
          source_task_id: sourceTaskId.trim() || undefined,
          limit: 200,
        }),
        researchPipelineApi.backfillRuns(experimentId, 100),
      ]);
      setExperiment(nextExperiment);
      setArtifactRefs(nextArtifacts);
      setBacktestRecords(nextBacktests);
      setBackfillRuns(nextBackfills);
      const taskIds = Array.from(new Set(nextBacktests.map((row) => row.source_task_id).filter((value): value is string => Boolean(value))));
      const loopIds = Array.from(new Set(nextBacktests.map((row) => row.source_loop_id).filter((value): value is string => Boolean(value))));
      const experimentIds = Array.from(new Set(nextBacktests.map((row) => row.source_experiment_id).filter((value): value is string => Boolean(value))));
      if (taskIds.length || loopIds.length || experimentIds.length) {
        const nextArchiveStatus = await qeArchiveApi.sourceStatus({
          task_ids: taskIds,
          loop_ids: loopIds,
          experiment_ids: experimentIds,
          include_recommendation: true,
        });
        setArchiveStatus(nextArchiveStatus);
      } else {
        setArchiveStatus(null);
      }
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [dedupStatus, experimentId, representative, sourceTaskId]);

  useEffect(() => {
    void load();
  }, [load]);

  const stages = experiment?.stages || [];
  const attempts = experiment?.attempts || [];
  const externalLinks = experiment?.external_run_links || [];
  const comparisons = experiment?.comparisons || [];
  const events = experiment?.events || [];
  const chronologicalBacktests = useMemo(
    () => [...backtestRecords].sort((a, b) => String(a.source_created_at || a.created_at || "").localeCompare(String(b.source_created_at || b.created_at || ""))),
    [backtestRecords],
  );
  const representativeCount = backtestRecords.filter((row) => row.qe_archive_representative).length;
  const eligibleCount = backtestRecords.filter((row) => row.qe_archive_eligible).length;
  const archivedBacktestCount = backtestRecords.filter((row) => archiveStatus?.loops?.[row.source_loop_id]?.archive_status === "archived").length;
  const failedStages = stages.filter((stage) => ["failed", "timeout", "cancelled"].includes(String(stage.status))).length;
  const latestBackfill = backfillRuns[0];

  return (
    <main className="pv2-shell">
      <section className="pv2-hero">
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">Research Pipeline detail / read-only</div>
            <h1>{experiment?.title || experimentId || "Research Experiment"}</h1>
            <p>
              Inspect stages, retry attempts, artifact refs, HMM backtest records, and backfill runs.
              This page does not expose write or execute actions; QE Archive status is read-only for 手动入仓决策。
            </p>
            <div className="pv2-chip-row">
              <span className="pv2-chip pv2-mono">{experimentId}</span>
              <span className="pv2-chip">{experiment?.pipeline_type || "-"}</span>
              <span className="pv2-chip">created_by {experiment?.created_by || "-"}</span>
            </div>
          </div>
          <div className="pv2-row-actions">
            <Link className="pv2-button-ghost" href="/research-pipeline">Back to list</Link>
            <button className="pv2-button-primary" type="button" onClick={() => void load()} disabled={loading}>{loading ? "Refreshing..." : "Refresh"}</button>
          </div>
        </div>
      </section>

      <ErrorPanel error={error} title="Research Pipeline detail read failed" />

      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="Experiment Status" value={experiment?.status || "unknown"} hint={experiment?.blocked_reason || `updated ${formatDateTime(experiment?.updated_at)}`} tone={experiment?.status === "validated" ? "success" : experiment?.status === "blocked" || experiment?.status === "stage_failed" ? "danger" : "info"} />
        <MetricCard label="Stages" value={formatCompact(stages.length, 0)} hint={failedStages ? `${failedStages} failed/timeout/cancelled` : `${attempts.length} attempts`} tone={failedStages ? "danger" : "success"} />
        <MetricCard label="HMM Backtests" value={formatCompact(backtestRecords.length, 0)} hint={`eligible ${eligibleCount} / representative ${representativeCount} / archived ${archivedBacktestCount}`} tone={backtestRecords.length ? "info" : "warning"} />
        <MetricCard label="Backfill Runs" value={formatCompact(backfillRuns.length, 0)} hint={latestBackfill ? `${latestBackfill.status} ${formatDateTime(latestBackfill.created_at)}` : "No backfill history"} tone={latestBackfill?.status === "failed" ? "danger" : backfillRuns.length ? "success" : "neutral"} />
      </div>

      <SectionCard title="Experiment Metadata" eyebrow="criteria / baseline / lifecycle">
        {experiment ? (
          <div className="pv2-grid pv2-grid-2">
            <KeyValuePanel rows={[
              ["experiment_id", experiment.experiment_id],
              ["pipeline_type", experiment.pipeline_type],
              ["status", experiment.status],
              ["issue_url", experiment.issue_url],
              ["blocked_reason", experiment.blocked_reason],
              ["validated_at", formatDateTime(experiment.validated_at)],
              ["created_at", formatDateTime(experiment.created_at)],
              ["updated_at", formatDateTime(experiment.updated_at)],
            ]} />
            <JsonPanel value={{ criteria_json: experiment.criteria_json || {}, baseline_ref_json: experiment.baseline_ref_json || {}, metadata_json: experiment.metadata_json || {} }} />
          </div>
        ) : <p className="pv2-muted">Loading experiment detail.</p>}
      </SectionCard>

      <SectionCard title="Stages And Attempts" eyebrow="planned stages / immutable retry history">
        <StageTimeline stages={stages} attempts={attempts} />
      </SectionCard>

      <SectionCard title="HMM Backtest Records Timeline" eyebrow="research_domain=hmm / chronological records">
        <div className="pv2-form-grid pv2-filter-card">
          <label className="pv2-field" htmlFor="rp-bt-dedup">
            <span>Dedup</span>
            <select className="pv2-select" id="rp-bt-dedup" value={dedupStatus} onChange={(event) => setDedupStatus(event.target.value)}>
              <option value="">All</option>
              <option value="primary">primary</option>
              <option value="duplicate_same_config">duplicate_same_config</option>
              <option value="hmm_variant">hmm_variant</option>
              <option value="excluded">excluded</option>
            </select>
          </label>
          <label className="pv2-field" htmlFor="rp-bt-representative">
            <span>QE Archive representative</span>
            <select className="pv2-select" id="rp-bt-representative" value={representative} onChange={(event) => setRepresentative(event.target.value)}>
              <option value="">All</option>
              <option value="true">true</option>
              <option value="false">false</option>
            </select>
          </label>
          <label className="pv2-field" htmlFor="rp-bt-task">
            <span>source_task_id</span>
            <input className="pv2-input" id="rp-bt-task" value={sourceTaskId} onChange={(event) => setSourceTaskId(event.target.value)} placeholder="exact task id" />
          </label>
        </div>
        <div className="pv2-grid pv2-grid-2" style={{ marginTop: 12, marginBottom: 12 }}>
          <div className="pv2-notice pv2-notice-info">
            <div className="pv2-notice-title">Dedup distribution</div>
            <div className="pv2-notice-body"><CountChips counts={countsBy(backtestRecords, (row) => row.dedup_status)} /></div>
          </div>
          <div className="pv2-notice pv2-notice-info">
            <div className="pv2-notice-title">Source distribution</div>
            <div className="pv2-notice-body"><CountChips counts={countsBy(backtestRecords, (row) => row.source_type)} /></div>
          </div>
        </div>
        <PaperTable
          rows={chronologicalBacktests}
          empty="No HMM backtest timeline records."
          columns={[
            { key: "time", header: "Timeline", render: (row) => <><div>{formatDateTime(row.source_created_at || row.created_at)}</div><span className="pv2-muted pv2-mono">{compactText(row.record_id)}</span></> },
            { key: "source", header: "Source", render: (row) => <><div>{sourceLabel(row)}</div><span className="pv2-muted pv2-mono">loop {compactText(row.source_loop_id)}</span></> },
            { key: "dedup", header: "Dedup / Archive", render: (row) => <><StatusBadge status={row.dedup_status || "unknown"} /><br /><span className="pv2-muted">eligible {display(row.qe_archive_eligible)} / repr {display(row.qe_archive_representative)}</span>{row.rejection_reason ? <><br /><span className="pv2-muted">{row.rejection_reason}</span></> : null}</> },
            { key: "archive_status", header: "QE Archive", render: (row) => {
              const loopStatus = archiveStatus?.loops?.[row.source_loop_id];
              const taskStatus = archiveStatus?.tasks?.[row.source_task_id];
              const status = loopStatus?.archive_status || "not_archived";
              return <><StatusBadge status={archiveStatusLabel(status)} /><br /><span className="pv2-muted">task {archiveStatusLabel(taskStatus?.archive_status)}</span><br /><span className="pv2-muted pv2-mono">{(loopStatus?.run_ids || []).map((item) => compactText(item)).join(" / ") || "-"}</span></>;
            } },
            { key: "metrics", header: "Core Metrics", render: (row) => <><div>ann {metricFrom(row, "ann")} / mdd {metricFrom(row, "mdd")}</div><span className="pv2-muted">ir {metricFrom(row, "ir")} / ic {metricFrom(row, "ic")} / rank_ic {metricFrom(row, "rank_ic")}</span></> },
            { key: "hmm", header: "HMM / Config", render: (row) => <><div className="pv2-mono">hmm {compactText(row.hmm_config_sig)}</div><span className="pv2-muted pv2-mono">non-hmm {compactText(row.non_hmm_config_sig)}</span><br /><span className="pv2-muted pv2-mono">archive {compactText(row.archive_family_sig)}</span></> },
            { key: "detail", header: "Detail", render: (row) => <details className="pv2-readable-item"><summary>payload / summaries</summary><JsonPanel value={{ metrics_json: row.metrics_json || {}, hmm_config_summary_json: row.hmm_config_summary_json || {}, config_summary_json: row.config_summary_json || {}, source_payload_json: row.source_payload_json || {} }} /></details> },
          ]}
        />
      </SectionCard>

      <SectionCard title="Backfill Runs" eyebrow="preview / execute audit records">
        <PaperTable
          rows={backfillRuns}
          empty="No backfill runs."
          columns={[
            { key: "run", header: "Run", render: (row) => <><StatusBadge status={row.status} /><br /><span className="pv2-muted pv2-mono">{compactText(row.backfill_run_id)}</span><br /><span className="pv2-muted">{row.backfill_type || "hmm_backtest_timeline"}</span></> },
            { key: "mode", header: "Mode", render: (row) => <><div>dry_run {display(row.dry_run)}</div><span className="pv2-muted">created_by {row.created_by || "-"}</span></> },
            { key: "counts", header: "Counts", render: (row) => <JsonPanel value={row.counts_json || {}} /> },
            { key: "time", header: "Time", render: (row) => <><div>created {formatDateTime(row.created_at)}</div><span className="pv2-muted">started {formatDateTime(row.started_at)} / completed {formatDateTime(row.completed_at)}</span></> },
            { key: "scope", header: "Scope / Error", render: (row) => <details className="pv2-readable-item"><summary>{row.error_message || "scope"}</summary><JsonPanel value={{ source_scope_json: row.source_scope_json || {}, source_fingerprint_json: row.source_fingerprint_json || {}, error_message: row.error_message }} /></details> },
          ]}
        />
      </SectionCard>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="Artifact Refs" eyebrow="candidate / validated references">
          <ArtifactTable rows={artifactRefs.length ? artifactRefs : (experiment?.artifact_refs || [])} />
        </SectionCard>
        <SectionCard title="External Run Links" eyebrow="QE / validation / HMM references">
          <ExternalLinksTable rows={externalLinks} />
        </SectionCard>
      </div>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="Comparisons" eyebrow="baseline versus candidate">
          <ComparisonTable rows={comparisons} />
        </SectionCard>
        <SectionCard title="Pipeline Events" eyebrow="latest audit messages">
          <EventTable rows={events} />
        </SectionCard>
      </div>
    </main>
  );
}
