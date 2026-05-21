"use client";

import Link from "next/link";
import { useCallback, useEffect, useState } from "react";

import JsonPanel from "@/components/paper-v2/JsonPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState, StatusChips, formatDateTime } from "@/components/research-assistant/AssistantShared";
import {
  API_BASE,
  type AssistantHealth,
  type AssistantIssueCandidate,
  type AssistantOverview,
  type AssistantTask,
  researchAssistantApi,
} from "@/lib/research-assistant/api";
import { formatCompact } from "@/lib/paper-v2/format";

export default function ResearchAssistantHomePage() {
  const [health, setHealth] = useState<AssistantHealth | null>(null);
  const [overview, setOverview] = useState<AssistantOverview | null>(null);
  const [tasks, setTasks] = useState<AssistantTask[]>([]);
  const [issues, setIssues] = useState<AssistantIssueCandidate[]>([]);
  const [notifications, setNotifications] = useState<unknown>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [nextHealth, nextOverview, nextTasks, nextIssues, nextNotifications] = await Promise.all([
        researchAssistantApi.health(),
        researchAssistantApi.overview(),
        researchAssistantApi.tasks({ limit: 8 }),
        researchAssistantApi.issueCandidates({ limit: 8 }),
        researchAssistantApi.notificationSummary(),
      ]);
      setHealth(nextHealth);
      setOverview(nextOverview);
      setTasks(nextTasks.items);
      setIssues(nextIssues.items);
      setNotifications(nextNotifications);
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  return (
    <main>
      <ApiErrorBox error={error} />
      <section className="pv2-card">
        <div className="pv2-card-head">
          <div>
            <div className="pv2-eyebrow">API / real data</div>
            <h2>阶段一总览</h2>
            <p className="pv2-muted">读取真实 API：<span className="pv2-mono">{API_BASE}/research-assistant</span>。若后端 schema 未部署，页面会显示错误，不会使用 mock 冒充成功。</p>
          </div>
          <div className="ra-top-actions">
            <button className="pv2-button-primary" type="button" onClick={() => void load()} disabled={loading}>{loading ? "刷新中..." : "刷新"}</button>
            <Link className="pv2-button-ghost" href="/research-assistant/approvals">待审批</Link>
          </div>
        </div>
        <div className="pv2-chip-row">
          <span className="pv2-chip">健康状态 <StatusBadge status={health?.status || "unknown"} /></span>
          <span className="pv2-chip">Repository <StatusBadge status={String(health?.repository?.status || "unknown")} /></span>
          <span className="pv2-chip">Phase {String(health?.phase || "phase1")}</span>
        </div>
        {health ? <DetailDrawer title="查看运行边界和仓储健康详情" data={health} /> : null}
      </section>

      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="运行中任务" value={formatCompact(overview?.running_tasks || 0, 0)} hint="Task Ledger running" tone={(overview?.running_tasks || 0) ? "warning" : "success"} />
        <MetricCard label="待审批" value={formatCompact(overview?.pending_approvals || 0, 0)} hint="高风险 MCP/Skill/记忆审批" tone={(overview?.pending_approvals || 0) ? "warning" : "success"} />
        <MetricCard label="候选 Issue" value={formatCompact(overview?.candidate_issues || 0, 0)} hint="只进入队列，不自动入 GitHub" tone={(overview?.candidate_issues || 0) ? "warning" : "info"} />
        <MetricCard label="已批准记忆" value={formatCompact(overview?.approved_memories || 0, 0)} hint="Memory Ledger approved" tone="info" />
      </div>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="状态分布" eyebrow="ledger counts">
          <h3 className="pv2-subsection-head">任务</h3>
          <StatusChips counts={overview?.task_status} />
          <h3 className="pv2-subsection-head">审批</h3>
          <StatusChips counts={overview?.approval_status} />
          <h3 className="pv2-subsection-head">候选 Issue</h3>
          <StatusChips counts={overview?.issue_candidate_status} />
          <h3 className="pv2-subsection-head">通知摘要</h3>
          <JsonPanel value={notifications || {}} />
        </SectionCard>
        <SectionCard title="阶段一红线" eyebrow="non-negotiable">
          <div className="pv2-readable-panel">
            <div className="pv2-readable-table">
              <div className="pv2-readable-row"><div className="pv2-readable-key">执行方式</div><div className="pv2-readable-value">只通过 MCP/API；不控制鼠标键盘。</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">长期记忆</div><div className="pv2-readable-value">Memory Ledger 是事实源；RAG/向量仅做后续辅助召回。</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">Issue</div><div className="pv2-readable-value">候选队列先审核；正式 Issue 必须 GitHub 同步。</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">高风险操作</div><div className="pv2-readable-value">必须 preflight + approval + trace，不允许静默兜底。</div></div>
            </div>
          </div>
        </SectionCard>
      </div>

      <SectionCard title="最近任务" eyebrow="task ledger">
        <PaperTable
          rows={tasks}
          empty="暂无任务。可以在“任务”页面创建真实 Task Ledger 记录。"
          columns={[
            { key: "title", header: "任务", render: (row) => <><Link className="pv2-link-button" href={`/research-assistant/tasks?task=${encodeURIComponent(row.task_id)}`}>{row.title}</Link><br /><span className="pv2-muted pv2-mono">{row.task_id}</span></> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "risk", header: "风险", render: (row) => <StatusBadge status={row.risk_level} /> },
            { key: "time", header: "更新时间", render: (row) => formatDateTime(row.updated_at || row.created_at) },
          ]}
        />
        {!tasks.length ? <EmptyState title="Task Ledger 为空" hint="空状态来自真实 API；没有用静态假数据填充。" /> : null}
      </SectionCard>

      <SectionCard title="候选 Issue" eyebrow="review queue">
        <PaperTable
          rows={issues}
          empty="暂无候选 Issue。"
          columns={[
            { key: "title", header: "标题", render: (row) => <><span className="ra-title">{row.title}</span><br /><span className="pv2-muted">{row.problem_statement}</span></> },
            { key: "severity", header: "级别", render: (row) => <StatusBadge status={row.severity} /> },
            { key: "status", header: "状态", render: (row) => <><StatusBadge status={row.status} /><br /><span className="pv2-muted">{row.github_sync_status || "not_requested"}</span></> },
            { key: "module", header: "模块", render: (row) => row.module || "-" },
          ]}
        />
      </SectionCard>
    </main>
  );
}
