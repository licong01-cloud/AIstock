"use client";

import { useCallback, useEffect, useState } from "react";

import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState, formatDateTime } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantExternalEvent, type AssistantExternalSession } from "@/lib/research-assistant/api";

export function ExternalAgentsSection() {
  const [sessions, setSessions] = useState<AssistantExternalSession[]>([]);
  const [events, setEvents] = useState<AssistantExternalEvent[]>([]);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const [sessionPage, eventPage] = await Promise.all([researchAssistantApi.externalSessions({ limit: 100 }), researchAssistantApi.externalEvents({ limit: 100 })]);
      setSessions(sessionPage.items);
      setEvents(eventPage.items);
    } catch (exc) {
      setError(exc);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  return (
    <>
      <ApiErrorBox error={error} />
      <SectionCard title="External Agent Connector" eyebrow="real sessions / events">
        <p className="pv2-muted">外部 Agent 只能通过后端登记的 session/event/API 边界进入；页面读取真实列表接口，空列表代表尚未接入，不用静态 JSON 冒充完成。</p>
        <PaperTable
          rows={sessions}
          empty="暂无外部 Agent session；后端接入 Codex/Claude 后会写入真实记录。"
          columns={[
            { key: "agent", header: "Agent", render: (row) => <><span className="ra-title">{row.agent_name || row.agent_type || row.session_id}</span><br /><span className="pv2-muted pv2-mono">{row.session_id}</span></> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "time", header: "创建时间", render: (row) => formatDateTime(row.created_at) },
            { key: "detail", header: "详情", render: (row) => <DetailDrawer title="auth / metadata" data={row} /> },
          ]}
        />
        {!sessions.length ? <EmptyState title="外部 Agent session 为空" hint="阶段边界来自真实 API 空状态；不会显示硬编码假 session。" /> : null}
      </SectionCard>
      <SectionCard title="External Agent Events" eyebrow="audit trail">
        <PaperTable
          rows={events}
          empty="暂无外部 Agent event。"
          columns={[
            { key: "event", header: "事件", render: (row) => <><span className="ra-title">{row.event_type || row.external_event_id}</span><br /><span className="pv2-muted pv2-mono">{row.session_id || "-"}</span></> },
            { key: "risk", header: "风险", render: (row) => <StatusBadge status={row.risk_level} /> },
            { key: "time", header: "时间", render: (row) => formatDateTime(row.created_at) },
            { key: "detail", header: "详情", render: (row) => <DetailDrawer title="payload / evidence" data={row} /> },
          ]}
        />
      </SectionCard>
    </>
  );
}
