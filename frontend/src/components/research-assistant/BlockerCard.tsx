"use client";

import { DiagnosticLogBlock, StatusPill, display } from "@/components/research-assistant/AssistantShared";
import type { AssistantBlockerCard, JsonObject } from "@/lib/research-assistant/api";

function blockerTone(status: string): string {
  if (status === "approval_required" || status === "high_risk_pending") return "warning";
  if (status === "blocked" || status === "failed") return "danger";
  return "neutral";
}

function provenanceDisplay(provenance?: JsonObject): string {
  if (!provenance || !Object.keys(provenance).length) return "-";
  const source = provenance.source || provenance.source_ref;
  return source ? display(source) : Object.entries(provenance).map(([key, value]) => `${key}: ${display(value)}`).join(", ");
}

export function normalizeBlockerCard(value: unknown): AssistantBlockerCard | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const row = value as JsonObject;
  const status = String(row.status || "");
  const reason = String(row.reason || row.blocked_reason || "");
  const nextStep = String(row.next_step || row.operator_action || "");
  if (!status || !reason || !nextStep) return null;
  return {
    ...(row as AssistantBlockerCard),
    blocker_id: String(row.blocker_id || row.id || `blocker-${status}`),
    status,
    reason,
    next_step: nextStep,
  };
}

export function BlockerCard({ card }: { card: AssistantBlockerCard }) {
  const status = String(card.status || "blocked");
  return (
    <article className={`ra-blocker-card ra-blocker-card-${blockerTone(status)}`} data-testid="ra-blocker-card">
      <div className="ra-card-headline">
        <span className="ra-chat-eyebrow">Blocker Card</span>
        <StatusPill status={status}>{status}</StatusPill>
      </div>
      <h3>{card.reason}</h3>
      <div className="ra-evidence-ref-grid">
        <span>next_step</span><strong>{card.next_step}</strong>
        <span>as_of</span><strong>{display(card.as_of)}</strong>
        <span>provenance</span><strong>{provenanceDisplay(card.provenance)}</strong>
      </div>
      <details className="ra-diagnostic-details" data-testid="ra-blocker-log">
        <summary>Developer details / Diagnostic log</summary>
        <DiagnosticLogBlock
          title="Blocker diagnostic log"
          data={card}
          testId="ra-blocker-log-detail"
        />
      </details>
    </article>
  );
}
