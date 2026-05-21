"use client";

import { useCallback, useEffect, useState } from "react";

import JsonPanel from "@/components/paper-v2/JsonPanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState, formatDateTime } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantTask, type AssistantTaskEvent } from "@/lib/research-assistant/api";

export default function ResearchAssistantTasksPage() {
  const [tasks, setTasks] = useState<AssistantTask[]>([]);
  const [selected, setSelected] = useState<{ task: AssistantTask; events: AssistantTaskEvent[] } | null>(null);
  const [title, setTitle] = useState("HMM 演进任务状态跟踪");
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const page = await researchAssistantApi.tasks({ limit: 100 });
      setTasks(page.items);
      if (!selected && page.items[0]) setSelected(await researchAssistantApi.task(page.items[0].task_id));
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [selected]);

  useEffect(() => {
    void load();
  }, [load]);

  async function createTask() {
    const task = await researchAssistantApi.createTask({ title, task_type: "research_stream", risk_level: "medium", input_json: { source: "ui" } });
    setTitle("");
    await load();
    setSelected(await researchAssistantApi.task(task.task_id));
  }

  async function addTriageEvent(taskId: string) {
    await researchAssistantApi.addTaskEvent(taskId, { event_type: "triage_required", severity: "warning", message: "人工标记需要 triage", payload_json: { source: "ui" } });
    setSelected(await researchAssistantApi.task(taskId));
    await load();
  }

  return (
    <main>
      <ApiErrorBox error={error} />
      <SectionCard title="Task Ledger" eyebrow="idempotency / event replay" action={<button className="pv2-button-primary" type="button" onClick={() => void load()} disabled={loading}>刷新</button>}>
        <div className="pv2-form-grid pv2-filter-card">
          <label className="pv2-field" htmlFor="ra-task-title"><span>新任务标题</span><input className="pv2-input" id="ra-task-title" value={title} onChange={(event) => setTitle(event.target.value)} /></label>
          <div className="pv2-field"><span className="pv2-label">写入边界</span><button className="pv2-button-primary" type="button" onClick={() => void createTask()} disabled={!title.trim()}>创建真实任务</button></div>
        </div>
        <PaperTable
          rows={tasks}
          empty="暂无任务。"
          columns={[
            { key: "title", header: "任务", render: (row) => <button className="pv2-link-button" type="button" onClick={() => void researchAssistantApi.task(row.task_id).then(setSelected)}>{row.title}<br /><span className="pv2-muted pv2-mono">{row.task_id}</span></button> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "risk", header: "风险", render: (row) => <StatusBadge status={row.risk_level} /> },
            { key: "time", header: "时间", render: (row) => formatDateTime(row.updated_at || row.created_at) },
            { key: "action", header: "操作", render: (row) => <button className="pv2-button-ghost" type="button" onClick={() => void addTriageEvent(row.task_id)}>标记 triage</button> },
          ]}
        />
      </SectionCard>
      <SectionCard title="事件流回放" eyebrow="agent task event stream">
        {selected ? (
          <>
            <JsonPanel value={selected.task} />
            <div className="ra-timeline" style={{ marginTop: 12 }}>
              {selected.events.map((event) => (
                <div className="ra-timeline-item" key={event.event_id}>
                  <div className="ra-timeline-meta"><StatusBadge status={event.event_type} /><StatusBadge status={event.severity} /><span className="pv2-muted">{formatDateTime(event.created_at)}</span></div>
                  <div>{event.message}</div>
                  <DetailDrawer title="事件 payload / evidence" data={event} />
                </div>
              ))}
            </div>
          </>
        ) : <EmptyState title="请选择任务" hint="点击任务列表中的标题查看可回放事件流。" />}
      </SectionCard>
    </main>
  );
}
