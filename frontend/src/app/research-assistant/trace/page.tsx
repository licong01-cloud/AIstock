"use client";

import { useCallback, useEffect, useState } from "react";

import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState, formatDateTime } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantOverview, type AssistantTraceEvent } from "@/lib/research-assistant/api";

export default function ResearchAssistantTracePage() {
  const [overview, setOverview] = useState<AssistantOverview | null>(null);
  const [events, setEvents] = useState<AssistantTraceEvent[]>([]);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const [nextOverview, eventPage] = await Promise.all([researchAssistantApi.overview(), researchAssistantApi.traceEvents({ limit: 100 })]);
      setOverview(nextOverview);
      setEvents(eventPage.items);
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
      <SectionCard title="Trace 与成本" eyebrow="assistant_trace_events">
        <div className="pv2-chip-row">
          {Object.entries(overview?.trace_status || {}).map(([status, count]) => <span className="pv2-chip" key={status}><StatusBadge status={status} /> {count}</span>)}
          {!Object.keys(overview?.trace_status || {}).length ? <span className="pv2-chip">暂无 trace 计数</span> : null}
        </div>
        <p className="pv2-muted">Trace 页面读取真实 trace event 列表与 overview 计数；真实模型调用成本由后续执行器写入，不以静态说明冒充。</p>
      </SectionCard>
      <SectionCard title="Trace Events" eyebrow="LLM / MCP / Skill">
        <PaperTable
          rows={events}
          empty="暂无 Trace Event。"
          columns={[
            { key: "event", header: "事件", render: (row) => <><span className="ra-title">{row.event_type || row.trace_id}</span><br /><span className="pv2-muted pv2-mono">{row.component || "-"}</span></> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "cost", header: "成本/耗时", render: (row) => <><span>{row.duration_ms ? `${row.duration_ms} ms` : "-"}</span><DetailDrawer title="cost_json" data={row.cost_json || {}} /></> },
            { key: "time", header: "时间", render: (row) => formatDateTime(row.created_at) },
            { key: "detail", header: "详情", render: (row) => <DetailDrawer title="payload" data={row} /> },
          ]}
        />
        {!events.length ? <EmptyState title="Trace Event 为空" hint="这是来自真实 API 的空状态；后端执行器写入后才展示记录。" /> : null}
      </SectionCard>
    </main>
  );
}
