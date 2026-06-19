
"use client";

import { useCallback, useEffect, useState } from "react";

import JsonPanel from "@/components/paper-v2/JsonPanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantIssueCandidate, type AssistantValidationDiscoverySummary, type JsonObject } from "@/lib/research-assistant/api";

const DRAFT_NOTICE = "\u8349\u7a3f\u8868\u5df2\u9000\u573a\uff0c\u6b63\u5f0f\u4e8b\u5b9e\u6e90=Validation/Nightly/issue workflow";

function text(value: unknown, fallback = "-"): string {
  const raw = String(value ?? "").trim();
  return raw || fallback;
}

export default function ResearchAssistantStreamsPage() {
  const [summary, setSummary] = useState<AssistantValidationDiscoverySummary | null>(null);
  const [issues, setIssues] = useState<AssistantIssueCandidate[]>([]);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const next = await researchAssistantApi.validationDiscoverySummary();
      setSummary(next);
      setIssues(next.candidate_issues_needing_review || []);
    } catch (exc) {
      setError(exc);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const reports = (summary?.latest_reports || []) as JsonObject[];
  const degraded = summary?.data_state === "degraded";

  return (
    <main>
      <ApiErrorBox error={error} />
      <SectionCard title="Pipeline Discovery Assistant View" eyebrow="derived_from_validation_candidates">
        <div className="ra-empty" style={{ marginBottom: 12 }}>
          <strong>{degraded ? "Validation fact source unavailable: explicit degraded state" : "Discovery stream is derived from Validation/Nightly candidates"}</strong>
          <p>{summary?.discovery_manifest_api_note || "No RA-owned discovery report fact source is presented here; this view consumes Validation candidate fields."}</p>
          <p>{DRAFT_NOTICE}; RA retired assistant_validation_discovery_reports and cannot substitute drafts for Validation/Nightly facts.</p>
          {degraded ? <p>reason={text((summary?.reason_codes || []).join(", "))}; warning={text((summary?.warnings || []).join(" / "))}</p> : null}
        </div>
        <JsonPanel value={{ source_of_truth: summary?.source_of_truth, discovery_report_mode: summary?.discovery_report_mode, candidate_summary: summary?.candidate_summary || {}, data_state: summary?.data_state }} />
      </SectionCard>
      <SectionCard title="Derived Discovery Entries" eyebrow="Validation candidate fields: source_type / source_plan_key / active_discovery_reason">
        <PaperTable
          rows={reports}
          empty={degraded ? "Validation fact source unavailable; RA discovery drafts cannot substitute for facts." : "No derived discovery entries from Validation candidates."}
          columns={[
            { key: "title", header: "Title", render: (row) => <><span className="ra-title">{String(row.title || "-")}</span><br /><span className="pv2-muted">candidate={String(row.candidate_id || "-")}</span></> },
            { key: "status", header: "Status", render: (row) => <StatusBadge status={row.status} /> },
            { key: "source", header: "Source", render: (row) => <><span>{String(row.source_type || "-")}</span><br /><span className="pv2-muted">{String(row.source_plan_key || "-")}</span></> },
            { key: "reason", header: "Discovery Reason", render: (row) => String(row.active_discovery_reason || "-") },
            { key: "detail", header: "Detail", render: (row) => <DetailDrawer title="derived discovery payload" data={row} /> },
          ]}
        />
        {!reports.length && !degraded ? <EmptyState title="No derived discovery entries" hint="This is a real Validation candidate empty state; RA discovery draft storage is retired and cannot be used as a fallback." /> : null}
      </SectionCard>
      <SectionCard title="Validation Candidate Flow" eyebrow="strict workflow before GitHub / BUG JSON">
        <PaperTable
          rows={issues}
          empty={degraded ? "Validation candidate fact source unavailable; RA drafts cannot substitute for facts." : "No Validation candidates in this view."}
          columns={[
            { key: "title", header: "Candidate", render: (row) => <><span>{row.title || "-"}</span><br /><span className="pv2-muted">{text(row.source_refs?.[0] || row.source_ref)}</span></> },
            { key: "severity", header: "Severity", render: (row) => <StatusBadge status={row.severity} /> },
            { key: "status", header: "Status", render: (row) => <StatusBadge status={row.status} /> },
            { key: "source", header: "Fact Source", render: (row) => text(row.source_type) },
            { key: "detail", header: "Detail", render: (row) => <DetailDrawer title="candidate evidence" data={row} /> },
          ]}
        />
      </SectionCard>
    </main>
  );
}
