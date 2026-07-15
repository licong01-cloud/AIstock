"use client";

import Link from "next/link";

import { BlockerCard, normalizeBlockerCard } from "@/components/research-assistant/BlockerCard";
import { DetailDrawer, EmptyState, StatusPill, display } from "@/components/research-assistant/AssistantShared";
import { EvidenceCard, normalizeEvidenceRef } from "@/components/research-assistant/EvidenceCard";
import type {
  AssistantAgentRun,
  AssistantBlockerCard,
  AssistantEvidenceCard,
  AssistantTraceEvent,
  JsonObject,
} from "@/lib/research-assistant/api";

function asObject(value: unknown): JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? (value as JsonObject) : {};
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function runResult(run: AssistantAgentRun): JsonObject {
  return asObject(run.result_json);
}

export function traceLinkFor(run: AssistantAgentRun): string {
  return run.trace_id ? `/research-assistant/trace?trace_id=${encodeURIComponent(run.trace_id)}` : "";
}

export function summarizeWorkerTrace(run: AssistantAgentRun, traces: AssistantTraceEvent[]): Array<[string, unknown]> {
  const matched = traces.filter((trace) => trace.trace_id === run.trace_id || trace.task_id === run.parent_task_id);
  const result = runResult(run);
  return [
    ["agent_run_id", run.agent_run_id],
    ["role", run.role || run.agent_key],
    ["status", run.status],
    ["trace_id", run.trace_id],
    ["context_pack_id", asObject(run.input_json).context_pack_id || result.context_pack_id],
    ["evidence_refs", asArray(result.evidence_refs).length],
    ["trace_events", matched.length],
  ];
}

function evidenceCardsForRun(run: AssistantAgentRun): AssistantEvidenceCard[] {
  const result = runResult(run);
  const explicit = asArray(result.evidence_cards).filter((item): item is AssistantEvidenceCard => typeof item === "object" && item !== null) as AssistantEvidenceCard[];
  if (explicit.length) return explicit;
  const refs = asArray(result.evidence_refs).map(normalizeEvidenceRef);
  if (!refs.length) return [];
  return [{
    card_id: `agent-run-evidence-${run.agent_run_id}`,
    title: `${run.agent_key || run.role || "agent"} evidence`,
    summary: String(result.summary || "Agent run returned evidence references."),
    evidence_refs: refs,
    status: "supported",
  }];
}

function blockerCardsForRun(run: AssistantAgentRun): AssistantBlockerCard[] {
  const result = runResult(run);
  const cards = asArray(result.blocker_cards).map(normalizeBlockerCard).filter((item): item is AssistantBlockerCard => Boolean(item));
  if (cards.length) return cards;
  const status = String(run.status || "");
  if (["blocked", "approval_required", "failed", "high_risk_pending"].includes(status)) {
    return [{
      blocker_id: `agent-run-blocker-${run.agent_run_id}`,
      status: status === "failed" ? "blocked" : status,
      reason: String(result.blocked_reason || result.error || `${run.agent_key || run.agent_run_id} did not complete successfully`),
      next_step: String(result.next_step || "Review worker trace and keep the action gated."),
      provenance: asObject(result.provenance),
      as_of: typeof result.as_of === "string" ? result.as_of : undefined,
    }];
  }
  return [];
}

function RunCard({ run, traces }: { run: AssistantAgentRun; traces: AssistantTraceEvent[] }) {
  const result = runResult(run);
  const evidenceCards = evidenceCardsForRun(run);
  const blockerCards = blockerCardsForRun(run);
  const link = traceLinkFor(run);
  return (
    <article className="ra-agent-run-card" data-testid="ra-agent-run-card">
      <div className="ra-card-headline">
        <div>
          <span className="ra-chat-eyebrow">{run.role || "agent"}</span>
          <h3>{run.agent_key || run.agent_run_id}</h3>
        </div>
        <StatusPill status={run.status || "unknown"} />
      </div>
      <div className="ra-evidence-ref-grid">
        <span>parent_task_id</span><strong>{display(run.parent_task_id)}</strong>
        <span>model_profile_id</span><strong>{display(run.model_profile_id)}</strong>
        <span>trace_id</span><strong>{display(run.trace_id)}</strong>
        <span>reduce_summary</span><strong>{display(result.reduce_summary || result.summary)}</strong>
      </div>
      {link ? <Link className="ra-chat-admin-link" href={link}>查看 worker 轨迹</Link> : <p className="ra-muted">Trace link unavailable because this run has no trace_id.</p>}
      <div className="ra-phase7-card-grid">
        {evidenceCards.map((card) => <EvidenceCard card={card} key={card.card_id} />)}
        {blockerCards.map((card) => <BlockerCard card={card} key={card.blocker_id} />)}
      </div>
      <DetailDrawer title="worker process detail" data={{ run, trace_summary: summarizeWorkerTrace(run, traces) }} />
    </article>
  );
}

export function AgentTeamsRunView({ runs, traceEvents }: { runs: AssistantAgentRun[]; traceEvents: AssistantTraceEvent[] }) {
  if (!runs.length) {
    return <EmptyState title="暂无 Agent Teams 运行记录" hint="The API returned no assistant_agent_runs rows for this filter." />;
  }
  const orchestrators = runs.filter((run) => String(run.role || run.agent_key || "").toLowerCase().includes("orchestrator"));
  const workers = runs.filter((run) => !orchestrators.includes(run));
  const reduceRows = runs.flatMap((run) => {
    const result = runResult(run);
    return result.reduce_summary ? [[run.agent_run_id, result.reduce_summary] as [string, unknown]] : [];
  });
  return (
    <section className="ra-phase7-panel" data-testid="ra-agent-teams-view">
      <div className="ra-card-headline">
        <span className="ra-chat-eyebrow">Phase 7 Agent Teams</span>
        <StatusPill status={workers.length >= 2 ? "ready" : "needs_more_worker_evidence"}>{workers.length} workers</StatusPill>
      </div>
      <div className="ra-agent-team-summary">
        <p>orchestrator: {orchestrators.map((run) => run.agent_key || run.agent_run_id).join(" / ") || "not returned"}</p>
        <p>worker status: {workers.map((run) => `${run.agent_key || run.agent_run_id}:${run.status || "unknown"}`).join(" / ") || "not returned"}</p>
        <p>reduce: {reduceRows.map(([id, summary]) => `${id} -> ${display(summary)}`).join(" / ") || "not returned"}</p>
      </div>
      <div className="ra-agent-run-grid">
        {runs.map((run) => <RunCard run={run} traces={traceEvents} key={run.agent_run_id} />)}
      </div>
    </section>
  );
}
