"use client";

import { DiagnosticLogBlock, StatusPill, display } from "@/components/research-assistant/AssistantShared";
import type { AssistantEvidenceCard, AssistantEvidenceRef, JsonObject } from "@/lib/research-assistant/api";

const REQUIRED_EVIDENCE_FIELDS = ["source", "provenance", "as_of"] as const;

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function normalizeEvidenceRef(value: unknown): AssistantEvidenceRef {
  if (typeof value === "string") return { source_ref: value };
  if (isObject(value)) return value as AssistantEvidenceRef;
  return {};
}

export function evidenceRefMissing(ref: AssistantEvidenceRef): string[] {
  return REQUIRED_EVIDENCE_FIELDS.filter((field) => {
    if (field === "provenance") return !isObject(ref.provenance) || Object.keys(ref.provenance).length === 0;
    return !String(ref[field] || "").trim();
  });
}

export function evidenceCompleteness(card: AssistantEvidenceCard): { ok: boolean; missing: string[] } {
  const refs = Array.isArray(card.evidence_refs) ? card.evidence_refs.map(normalizeEvidenceRef) : [];
  if (!refs.length) return { ok: false, missing: ["evidence_refs"] };
  const missing = [...new Set(refs.flatMap(evidenceRefMissing))];
  return { ok: missing.length === 0, missing };
}

export function EvidenceCard({ card }: { card: AssistantEvidenceCard }) {
  const refs = Array.isArray(card.evidence_refs) ? card.evidence_refs.map(normalizeEvidenceRef) : [];
  const completeness = evidenceCompleteness({ ...card, evidence_refs: refs });
  const effectiveStatus = completeness.ok && card.status === "supported" ? "supported" : card.status === "blocked" ? "blocked" : "insufficient";

  return (
    <article className={`ra-evidence-card ra-evidence-card-${effectiveStatus}`} data-testid="ra-evidence-card">
      <div className="ra-card-headline">
        <span className="ra-chat-eyebrow">Evidence Card</span>
        <StatusPill status={effectiveStatus}>{effectiveStatus === "supported" ? "supported" : effectiveStatus === "blocked" ? "blocked" : "evidence_insufficient"}</StatusPill>
      </div>
      <h3>{card.title || card.card_id}</h3>
      <p>{card.summary || "Evidence summary is unavailable because the backend did not provide a summary."}</p>
      {!completeness.ok ? (
        <div className="ra-evidence-gap" data-testid="ra-evidence-gap">
          Missing required evidence fields: {completeness.missing.join(", ")}. The UI does not create a default as_of or source.
        </div>
      ) : null}
      <div className="ra-evidence-ref-list">
        {refs.map((ref, index) => {
          const missing = evidenceRefMissing(ref);
          return (
            <div className="ra-evidence-ref" data-testid="ra-evidence-ref" key={`${ref.source || ref.source_ref || "evidence"}-${index}`}>
              <div className="ra-evidence-ref-grid">
                <span>source</span><strong>{display(ref.source || ref.source_ref)}</strong>
                <span>as_of</span><strong>{display(ref.as_of)}</strong>
                <span>provenance</span><strong>{isObject(ref.provenance) ? Object.keys(ref.provenance).join(", ") : "-"}</strong>
              </div>
              {missing.length ? <p className="ra-muted">insufficient: {missing.join(", ")}</p> : null}
              <DiagnosticLogBlock
                title="Evidence provenance log"
                data={ref}
                testId="ra-evidence-log"
              />
            </div>
          );
        })}
      </div>
    </article>
  );
}
