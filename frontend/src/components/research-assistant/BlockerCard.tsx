"use client";

import { DetailDrawer, StatusPill, display } from "@/components/research-assistant/AssistantShared";
import type { AssistantBlockerCard, JsonObject } from "@/lib/research-assistant/api";

function blockerTone(status: string): string {
  if (status === "approval_required" || status === "high_risk_pending") return "warning";
  if (status === "blocked" || status === "failed") return "danger";
  return "neutral";
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
        <span>provenance</span><strong>{card.provenance ? Object.keys(card.provenance).join(", ") : "-"}</strong>
      </div>
      <DetailDrawer title="blocker detail" data={card} />
    </article>
  );
}
