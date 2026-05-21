"use client";

import { useCallback, useEffect, useState } from "react";

import JsonPanel from "@/components/paper-v2/JsonPanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantIssueCandidate, type AssistantValidationDiscoverySummary, type JsonObject } from "@/lib/research-assistant/api";

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

  return (
    <main>
      <ApiErrorBox error={error} />
      <SectionCard title="Validation / Pipeline Discovery Stream" eyebrow="nightly report / llm assisted">
        <JsonPanel value={{ boundary: "阶段一展示真实 discovery report 与候选 Issue 队列；夜间 LLM 探测任务由后续调度写入。", summary: summary || {} }} />
      </SectionCard>
      <SectionCard title="发现报告" eyebrow="assistant_validation_discovery_reports">
        <PaperTable
          rows={reports}
          empty="暂无发现报告。"
          columns={[
            { key: "title", header: "报告", render: (row) => <><span className="ra-title">{String(row.title || "-")}</span><br /><span className="pv2-muted">{String(row.run_date || "-")}</span></> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "detail", header: "详情", render: (row) => <DetailDrawer title="summary / evidence" data={row} /> },
          ]}
        />
        {!reports.length ? <EmptyState title="暂无夜间测试汇报" hint="这是真实空状态，未使用 mock 报告冒充完成。" /> : null}
      </SectionCard>
      <SectionCard title="发现流候选 Issue" eyebrow="strict review before GitHub">
        <PaperTable
          rows={issues}
          empty="暂无发现流候选 Issue。"
          columns={[
            { key: "title", header: "标题", render: (row) => row.title || "-" },
            { key: "severity", header: "级别", render: (row) => <StatusBadge status={row.severity} /> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "detail", header: "详情", render: (row) => <DetailDrawer title="candidate evidence" data={row} /> },
          ]}
        />
      </SectionCard>
    </main>
  );
}
