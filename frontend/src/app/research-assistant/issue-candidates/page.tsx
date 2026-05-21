"use client";

import { useCallback, useEffect, useState } from "react";

import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantIssueCandidate } from "@/lib/research-assistant/api";

export default function ResearchAssistantIssueCandidatesPage() {
  const [issues, setIssues] = useState<AssistantIssueCandidate[]>([]);
  const [status, setStatus] = useState("needs_review");
  const [error, setError] = useState<unknown>(null);
  const [actionError, setActionError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      setIssues((await researchAssistantApi.issueCandidates({ status, limit: 100 })).items);
    } catch (exc) {
      setError(exc);
    }
  }, [status]);

  useEffect(() => {
    void load();
  }, [load]);

  async function dryRunSync(candidateId: string) {
    setActionError(null);
    try {
      await researchAssistantApi.githubSyncIssueCandidate(candidateId, { mode: "dry_run", requested_by: "ui" });
      await load();
    } catch (exc) {
      setActionError(exc);
    }
  }

  return (
    <main>
      <ApiErrorBox error={error} />
      <ApiErrorBox error={actionError} title="GitHub 同步预检查失败" />
      <SectionCard title="候选 Issue 队列" eyebrow="review before GitHub sync">
        <div className="pv2-row-actions" style={{ marginBottom: 12 }}>
          <label className="pv2-field" htmlFor="ra-issue-status"><span>状态过滤</span><input className="pv2-input" id="ra-issue-status" value={status} onChange={(event) => setStatus(event.target.value)} placeholder="needs_review" /></label>
          <button className="pv2-button-ghost" type="button" onClick={() => void load()}>刷新</button>
        </div>
        <PaperTable
          rows={issues}
          empty="暂无候选 Issue。"
          columns={[
            { key: "title", header: "标题", render: (row) => <><span className="ra-title">{row.title}</span><br /><span className="pv2-muted">{row.problem_statement}</span></> },
            { key: "severity", header: "级别", render: (row) => <StatusBadge status={row.severity} /> },
            { key: "status", header: "状态", render: (row) => <><StatusBadge status={row.status} /><br /><span className="pv2-muted">{row.github_sync_status || "not_requested"}</span></> },
            { key: "module", header: "模块", render: (row) => row.module || "-" },
            { key: "detail", header: "详情", render: (row) => <DetailDrawer title="candidate payload" data={row} /> },
            { key: "action", header: "操作", render: (row) => <button className="pv2-button-ghost" type="button" onClick={() => void dryRunSync(row.candidate_id)}>GitHub dry-run</button> },
          ]}
        />
        {!issues.length ? <EmptyState title="候选 Issue 队列为空" hint="LLM 或人工探测发现的问题会先进入此队列，审核通过后才允许同步 GitHub。" /> : null}
      </SectionCard>
    </main>
  );
}
