
"use client";

import { useCallback, useEffect, useState } from "react";

import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantIssueCandidate, type AssistantPage } from "@/lib/research-assistant/api";

const DRAFT_NOTICE = "\u975e\u6743\u5a01\u5bf9\u8bdd\u8349\u7a3f/\u89e3\u91ca\u7f13\u5b58\uff0c\u5f85 Phase 2 \u9000\u573a";
const OFFICIAL_WORKFLOW_REQUIRED = "\u6b63\u5f0f\u63d0\u4ea4\u5fc5\u987b\u8d70 AIstock issue workflow / Validation MCP";
const STANDARD_WORKFLOW_HINT = `RA only displays the Validation candidate fact source. ${OFFICIAL_WORKFLOW_REQUIRED}.`;

function text(value: unknown, fallback = "-"): string {
  const raw = String(value ?? "").trim();
  return raw || fallback;
}

function sourceRefs(row: AssistantIssueCandidate): string {
  const refs = row.source_refs || row.evidence_refs || [];
  return refs.length ? refs.slice(0, 2).join(" / ") : text(row.source_ref);
}

export default function ResearchAssistantIssueCandidatesPage() {
  const [page, setPage] = useState<AssistantPage<AssistantIssueCandidate> | null>(null);
  const [status, setStatus] = useState("");
  const [error, setError] = useState<unknown>(null);
  const [notice, setNotice] = useState<string>(STANDARD_WORKFLOW_HINT);

  const issues = page?.items || [];
  const degraded = page?.data_state === "degraded";

  const load = useCallback(async () => {
    setError(null);
    try {
      setPage(await researchAssistantApi.issueCandidates({ status: status || undefined, limit: 100 }));
    } catch (exc) {
      setError(exc);
    }
  }, [status]);

  useEffect(() => {
    void load();
  }, [load]);

  function showWorkflowHint(candidateId: string) {
    setNotice(`Candidate ${candidateId} GitHub sync is block-only in RA. Use report_bug / mcp_github_issue_create / mcp_github_issue_sync_bug.`);
  }

  return (
    <main>
      <ApiErrorBox error={error} />
      <SectionCard title="Validation Candidate Issue Assistant View" eyebrow="source_of_truth: Validation issue candidates">
        <div className="ra-empty" style={{ marginBottom: 12 }}>
          <strong>{degraded ? "Fact source unavailable: explicit degraded state" : "Formal candidates come from Validation / Nightly / issue workflow"}</strong>
          <p>{degraded ? `${(page?.reason_codes || []).join(", ") || "validation_issue_fact_source_unavailable"}: ${(page?.warnings || []).join(" / ")}` : STANDARD_WORKFLOW_HINT}</p>
          <p>{DRAFT_NOTICE}; assistant_issue_candidates cannot substitute for Validation facts.</p>
        </div>
        <div className="pv2-row-actions" style={{ marginBottom: 12 }}>
          <label className="pv2-field" htmlFor="ra-issue-status">
            <span>Validation status filter</span>
            <input className="pv2-input" id="ra-issue-status" value={status} onChange={(event) => setStatus(event.target.value)} placeholder="leave blank, or new/promoted/closed" />
          </label>
          <button className="pv2-button-ghost" type="button" onClick={() => void load()}>Refresh fact source</button>
        </div>
        {notice ? <div className="ra-empty" style={{ marginBottom: 12 }}><strong>Standard workflow</strong><p>{notice}</p></div> : null}
        <PaperTable
          rows={issues}
          empty={degraded ? "Validation candidate fact source is unavailable; RA draft tables cannot substitute for facts." : "No Validation issue candidates."}
          columns={[
            { key: "title", header: "Candidate", render: (row) => <><span className="ra-title">{row.title}</span><br /><span className="pv2-muted">{text(row.problem_statement || row.actual || row.summary)}</span></> },
            { key: "severity", header: "Severity", render: (row) => <StatusBadge status={row.severity} /> },
            { key: "status", header: "Status / Gate", render: (row) => <><StatusBadge status={row.status} /><br /><span className="pv2-muted">quality={text(row.quality_gate_state)} / ready={String(row.issue_payload_ready ?? "-")}</span></> },
            { key: "source", header: "Fact Source", render: (row) => <><span>{text(row.source_type)}</span><br /><span className="pv2-muted">{text(row.source_plan_key)}</span></> },
            { key: "module", header: "Module", render: (row) => row.module_id || row.module || "-" },
            { key: "refs", header: "Evidence", render: (row) => <span className="pv2-muted">{sourceRefs(row)}</span> },
            { key: "detail", header: "Detail", render: (row) => <DetailDrawer title="Validation candidate payload" data={row} /> },
            { key: "action", header: "Action", render: (row) => <button className="pv2-button-ghost" type="button" onClick={() => showWorkflowHint(row.candidate_id)}>Use standard workflow</button> },
          ]}
        />
        {!issues.length && !degraded ? <EmptyState title="Validation candidate queue is empty" hint="This is the source-of-truth empty state; assistant_issue_candidates remains a non-authoritative draft/cache only." /> : null}
      </SectionCard>
    </main>
  );
}
