"use client";

import { useCallback, useEffect, useState } from "react";

import JsonPanel from "@/components/paper-v2/JsonPanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState, formatDateTime } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantNotification } from "@/lib/research-assistant/api";

export default function ResearchAssistantNotificationsPage() {
  const [notifications, setNotifications] = useState<AssistantNotification[]>([]);
  const [summary, setSummary] = useState<unknown>(null);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const [notificationPage, nextSummary] = await Promise.all([researchAssistantApi.notifications(), researchAssistantApi.notificationSummary()]);
      setNotifications(notificationPage.items);
      setSummary(nextSummary);
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
      <SectionCard title="通知中心" eyebrow="web notifications">
        <DetailDrawer title="通知 summary" data={summary || {}} />
        <PaperTable
          rows={notifications}
          empty="暂无通知。"
          columns={[
            { key: "title", header: "通知", render: (row) => <><span className="ra-title">{row.title}</span><br /><span className="pv2-muted">{row.message}</span></> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "risk", header: "风险", render: (row) => <StatusBadge status={row.risk_level} /> },
            { key: "time", header: "时间", render: (row) => formatDateTime(row.created_at) },
            { key: "detail", header: "详情", render: (row) => <DetailDrawer title="metadata" data={row} /> },
          ]}
        />
        {!notifications.length ? <EmptyState title="通知列表为空" hint="这是 notifications 独立路由的真实 API 空状态，不再 re-export 报告页面。" /> : null}
      </SectionCard>
      <SectionCard title="Raw Summary" eyebrow="debug contract">
        <JsonPanel value={summary || {}} />
      </SectionCard>
    </main>
  );
}
