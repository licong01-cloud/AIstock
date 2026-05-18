"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import JsonPanel from "@/components/paper-v2/JsonPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { formatCompact, shortHash } from "@/lib/paper-v2/format";
import {
  API_BASE,
  type JsonObject,
  type ResearchExperimentSummary,
  type ResearchPipelineHealth,
  type ResearchPipelineTypeConfig,
  researchPipelineApi,
} from "@/lib/research-pipeline/api";

const PAGE_SIZE = 30;

function display(value: unknown): string {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "boolean") return value ? "yes" : "no";
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

function countsByStatus(rows: ResearchExperimentSummary[]): Record<string, number> {
  return rows.reduce<Record<string, number>>((acc, row) => {
    const status = String(row.status || "unknown");
    acc[status] = (acc[status] || 0) + 1;
    return acc;
  }, {});
}

function latestDate(rows: ResearchExperimentSummary[]): string {
  const first = [...rows].sort((a, b) => String(b.updated_at || b.created_at || "").localeCompare(String(a.updated_at || a.created_at || "")))[0];
  return formatDateTime(first?.updated_at || first?.created_at);
}

function pipelineDisplayName(pipelineTypes: Record<string, ResearchPipelineTypeConfig>, pipelineType: string): string {
  return String(pipelineTypes[pipelineType]?.display_name || pipelineType || "-");
}

function metadataValue(row: ResearchExperimentSummary, keys: string[]): string {
  const metadata = isObject(row.metadata_json) ? row.metadata_json : {};
  const baseline = isObject(row.baseline_ref_json) ? row.baseline_ref_json : {};
  const criteria = isObject(row.criteria_json) ? row.criteria_json : {};
  for (const key of keys) {
    const value = metadata[key] ?? baseline[key] ?? criteria[key];
    if (value !== undefined && value !== null && value !== "") return display(value);
  }
  return "-";
}

function StatusCounts({ counts }: { counts: Record<string, number> }) {
  const entries = Object.entries(counts);
  if (!entries.length) return <span className="pv2-muted">No status counts</span>;
  return (
    <div className="pv2-chip-row">
      {entries.map(([status, count]) => (
        <span className="pv2-chip" key={status}>{status}: {count}</span>
      ))}
    </div>
  );
}

export default function ResearchPipelinePage() {
  const [experiments, setExperiments] = useState<ResearchExperimentSummary[]>([]);
  const [pipelineTypes, setPipelineTypes] = useState<Record<string, ResearchPipelineTypeConfig>>({});
  const [health, setHealth] = useState<ResearchPipelineHealth | null>(null);
  const [statusFilter, setStatusFilter] = useState("");
  const [pipelineFilter, setPipelineFilter] = useState("hmm_research");
  const [search, setSearch] = useState("");
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [nextHealth, nextTypes, nextRows] = await Promise.all([
        researchPipelineApi.health(),
        researchPipelineApi.pipelineTypes(),
        researchPipelineApi.experiments({
          status: statusFilter || undefined,
          pipeline_type: pipelineFilter || undefined,
          search: search.trim() || undefined,
          limit: PAGE_SIZE + 1,
          offset,
        }),
      ]);
      setHealth(nextHealth);
      setPipelineTypes(nextTypes);
      setHasMore(nextRows.length > PAGE_SIZE);
      setExperiments(nextRows.slice(0, PAGE_SIZE));
    } catch (exc) {
      setError(exc);
      setExperiments([]);
      setHasMore(false);
    } finally {
      setLoading(false);
    }
  }, [offset, pipelineFilter, search, statusFilter]);

  useEffect(() => {
    void load();
  }, [load]);

  const statusCounts = useMemo(() => countsByStatus(experiments), [experiments]);
  const hmmCount = experiments.filter((item) => item.pipeline_type === "hmm_research").length;
  const runningCount = experiments.filter((item) => ["draft", "running"].includes(String(item.status))).length;
  const issueLinkedCount = experiments.filter((item) => item.issue_url).length;

  function resetAndSet(setter: (value: string) => void, value: string) {
    setter(value);
    setOffset(0);
  }

  return (
    <main className="pv2-shell">
      <section className="pv2-hero">
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">Research Pipeline / read-only console</div>
            <h1>Research Pipeline Status And HMM Backtest History</h1>
            <p>
              Inspect research experiments, stage attempts, artifact references, and HMM backtest/backfill history.
              This page is read-only and only calls GET endpoints under <span className="pv2-mono">{API_BASE}/research-pipeline</span>.
            </p>
          </div>
          <div className="pv2-row-actions">
            <button className="pv2-button-primary" type="button" onClick={() => void load()} disabled={loading}>{loading ? "Refreshing..." : "Refresh"}</button>
          </div>
        </div>
      </section>

      <ErrorPanel error={error} title="Research Pipeline read failed" />

      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="Current Page" value={formatCompact(experiments.length, 0)} hint={hasMore ? `offset ${offset}, more rows available` : `offset ${offset}`} tone="info" />
        <MetricCard label="HMM Research" value={formatCompact(hmmCount, 0)} hint="HMM timeline candidates" tone={hmmCount ? "success" : "warning"} />
        <MetricCard label="Open Work" value={formatCompact(runningCount, 0)} hint="draft + running" tone={runningCount ? "warning" : "success"} />
        <MetricCard label="Latest Update" value={latestDate(experiments).slice(0, 10)} hint={latestDate(experiments)} />
      </div>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="Read Boundary" eyebrow="no write actions">
          <div className="pv2-readable-panel">
            <div className="pv2-readable-table">
              <div className="pv2-readable-row"><div className="pv2-readable-key">API health</div><div className="pv2-readable-value"><StatusBadge status={health?.status || "unknown"} /> {health?.service || "research-pipeline"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">Visible pipelines</div><div className="pv2-readable-value">{Object.keys(pipelineTypes).length ? Object.keys(pipelineTypes).join(" / ") : "-"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">Blocked actions</div><div className="pv2-readable-value">No create, run, retry, backfill execute, promote, or reject actions are exposed.</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">Issue links</div><div className="pv2-readable-value">{issueLinkedCount} rows on this page already have an issue_url.</div></div>
            </div>
          </div>
        </SectionCard>
        <SectionCard title="Status Distribution" eyebrow="current page only">
          <StatusCounts counts={statusCounts} />
        </SectionCard>
      </div>

      <SectionCard title="Experiments" eyebrow="filters / history entry">
        <div className="pv2-form-grid pv2-filter-card">
          <label className="pv2-field" htmlFor="rp-status-filter">
            <span>Status</span>
            <select className="pv2-select" id="rp-status-filter" value={statusFilter} onChange={(event) => resetAndSet(setStatusFilter, event.target.value)}>
              <option value="">All statuses</option>
              <option value="draft">draft</option>
              <option value="running">running</option>
              <option value="stage_failed">stage_failed</option>
              <option value="validated">validated</option>
              <option value="blocked">blocked</option>
              <option value="promotion_requested">promotion_requested</option>
              <option value="rejected">rejected</option>
            </select>
          </label>
          <label className="pv2-field" htmlFor="rp-type-filter">
            <span>Pipeline type</span>
            <select className="pv2-select" id="rp-type-filter" value={pipelineFilter} onChange={(event) => resetAndSet(setPipelineFilter, event.target.value)}>
              <option value="">All types</option>
              {Object.entries(pipelineTypes).map(([key, config]) => (
                <option key={key} value={key}>{config.display_name || key}</option>
              ))}
            </select>
          </label>
          <label className="pv2-field" htmlFor="rp-search-filter">
            <span>Search</span>
            <input className="pv2-input" id="rp-search-filter" value={search} onChange={(event) => resetAndSet(setSearch, event.target.value)} placeholder="experiment id / title / description" />
          </label>
        </div>
        <div className="pv2-row-actions" style={{ marginTop: 12, marginBottom: 12 }}>
          <button className="pv2-button-ghost" type="button" disabled={offset <= 0 || loading} onClick={() => setOffset((value) => Math.max(0, value - PAGE_SIZE))}>Previous</button>
          <span className="pv2-help">offset {offset} / page size {PAGE_SIZE}</span>
          <button className="pv2-button-ghost" type="button" disabled={!hasMore || loading} onClick={() => setOffset((value) => value + PAGE_SIZE)}>Next</button>
        </div>
        <PaperTable
          rows={experiments}
          empty="No Research Pipeline experiments."
          columns={[
            {
              key: "experiment",
              header: "Experiment",
              render: (row) => (
                <>
                  <Link className="pv2-link-button" href={`/research-pipeline/${encodeURIComponent(row.experiment_id)}`}>{row.title || row.experiment_id}</Link>
                  <br />
                  <span className="pv2-muted pv2-mono">{shortHash(row.experiment_id)}</span>
                  {row.description ? <><br /><span className="pv2-muted">{row.description}</span></> : null}
                </>
              ),
            },
            { key: "type", header: "Type", render: (row) => <><div>{pipelineDisplayName(pipelineTypes, row.pipeline_type)}</div><span className="pv2-muted pv2-mono">{row.pipeline_type}</span></> },
            { key: "status", header: "Status", render: (row) => <><StatusBadge status={row.status} />{row.blocked_reason ? <><br /><span className="pv2-muted">{row.blocked_reason}</span></> : null}</> },
            { key: "baseline", header: "Baseline / Window", render: (row) => <><div>{metadataValue(row, ["baseline", "baseline_name", "baseline_ref", "strategy_version"])}</div><span className="pv2-muted">{metadataValue(row, ["window", "as_of", "start_date", "end_date"])}</span></> },
            { key: "issue", header: "Issue / Validation", render: (row) => <><div>{row.issue_url ? <a href={row.issue_url}>{row.issue_url}</a> : <span className="pv2-muted">No issue link</span>}</div><span className="pv2-muted">validated {formatDateTime(row.validated_at)}</span></> },
            { key: "time", header: "Time", render: (row) => <><div>updated {formatDateTime(row.updated_at)}</div><span className="pv2-muted">created {formatDateTime(row.created_at)} / {row.created_by || "-"}</span></> },
          ]}
        />
      </SectionCard>

      <SectionCard title="Pipeline Type Config" eyebrow="backend advertised stages / criteria">
        {Object.keys(pipelineTypes).length ? <JsonPanel value={pipelineTypes} /> : <p className="pv2-muted">No pipeline-types response.</p>}
      </SectionCard>
    </main>
  );
}
