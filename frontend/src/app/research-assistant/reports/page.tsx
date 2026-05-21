"use client";

import { useCallback, useEffect, useState } from "react";

import JsonPanel from "@/components/paper-v2/JsonPanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState, formatDateTime } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantReport } from "@/lib/research-assistant/api";

export default function ResearchAssistantReportsPage() {
  const [reports, setReports] = useState<AssistantReport[]>([]);
  const [notifications, setNotifications] = useState<unknown[]>([]);
  const [agenda, setAgenda] = useState<unknown[]>([]);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const [reportPage, notificationPage, agendaPage] = await Promise.all([researchAssistantApi.reports(), researchAssistantApi.notifications(), researchAssistantApi.agenda()]);
      setReports(reportPage.items);
      setNotifications(notificationPage.items);
      setAgenda(agendaPage.items);
    } catch (exc) {
      setError(exc);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  return (
    <main>
      <ApiErrorBox error={error} />
      <SectionCard title="晨报与研究报告" eyebrow="source / evidence / jump">
        <PaperTable
          rows={reports}
          empty="暂无报告。"
          columns={[
            { key: "title", header: "报告", render: (row) => <><span className="ra-title">{row.title}</span><br /><span className="pv2-muted">{row.body_md}</span></> },
            { key: "type", header: "类型", render: (row) => row.report_type || "-" },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "time", header: "时间", render: (row) => formatDateTime(row.created_at) },
            { key: "detail", header: "详情", render: (row) => <DetailDrawer title="summary / evidence" data={row} /> },
          ]}
        />
        {!reports.length ? <EmptyState title="报告列表为空" hint="夜间测试汇报和 LLM 探测报告将在这里展示真实记录。" /> : null}
      </SectionCard>
      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="Web 内通知" eyebrow="assistant_notifications">
          <JsonPanel value={notifications} />
        </SectionCard>
        <SectionCard title="今日事项 / personal namespace" eyebrow="agenda">
          <JsonPanel value={agenda} />
        </SectionCard>
      </div>
    </main>
  );
}
