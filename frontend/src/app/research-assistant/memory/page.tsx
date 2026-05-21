"use client";

import { useCallback, useEffect, useState } from "react";

import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState, formatDateTime } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantContextPack, type AssistantMemory } from "@/lib/research-assistant/api";

const MEMORY_CONFIRM = "APPROVE_RESEARCH_ASSISTANT_ACTION";

export default function ResearchAssistantMemoryPage() {
  const [memories, setMemories] = useState<AssistantMemory[]>([]);
  const [packs, setPacks] = useState<AssistantContextPack[]>([]);
  const [error, setError] = useState<unknown>(null);
  const [actionError, setActionError] = useState<unknown>(null);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [confirmations, setConfirmations] = useState<Record<string, string>>({});
  const [draft, setDraft] = useState({ subject_key: "aistock.assistant.memory", title: "长期记忆候选", content_text: "记录一条可审核、可追溯的事实。", source_ref: "manual-ui" });

  const load = useCallback(async () => {
    setError(null);
    try {
      const [memoryPage, packPage] = await Promise.all([researchAssistantApi.memories({ limit: 100 }), researchAssistantApi.contextPacks({ limit: 50 })]);
      setMemories(memoryPage.items);
      setPacks(packPage.items);
    } catch (exc) {
      setError(exc);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  async function createMemory() {
    setActionError(null);
    try {
      await researchAssistantApi.createMemory({ memory_type: "core", namespace: "aistock", risk_level: "medium", evidence_refs: [draft.source_ref], ...draft });
      await load();
    } catch (exc) {
      setActionError(exc);
    }
  }

  async function approve(memory: AssistantMemory) {
    setBusyId(memory.memory_id);
    setActionError(null);
    try {
      const approval = await researchAssistantApi.createApproval({
        approval_type: "memory.approve",
        risk_level: "high",
        plan_digest: `memory-${memory.memory_id}`,
        summary: `approve ${memory.subject_key || memory.memory_id}`,
        required_confirmation_text: MEMORY_CONFIRM,
        created_by: "ui",
      });
      await researchAssistantApi.updateMemoryStatus(memory.memory_id, { status: "approved", approved_by: "ui", approval_id: approval.approval_id, confirmation_text: confirmations[memory.memory_id] || "" });
      await load();
    } catch (exc) {
      setActionError(exc);
    } finally {
      setBusyId(null);
    }
  }

  async function buildPack() {
    setActionError(null);
    try {
      await researchAssistantApi.buildContextPack({ namespace: "aistock", token_budget: 16000 });
      await load();
    } catch (exc) {
      setActionError(exc);
    }
  }

  return (
    <main>
      <ApiErrorBox error={error} />
      <ApiErrorBox error={actionError} title="记忆操作失败" />
      <SectionCard title="Memory Ledger" eyebrow="source of truth">
        <div className="pv2-form-grid pv2-filter-card">
          <label className="pv2-field" htmlFor="ra-memory-title"><span>标题</span><input className="pv2-input" id="ra-memory-title" value={draft.title} onChange={(event) => setDraft({ ...draft, title: event.target.value })} /></label>
          <label className="pv2-field" htmlFor="ra-memory-subject"><span>Subject Key</span><input className="pv2-input" id="ra-memory-subject" value={draft.subject_key} onChange={(event) => setDraft({ ...draft, subject_key: event.target.value })} /></label>
          <label className="pv2-field" htmlFor="ra-memory-source"><span>来源</span><input className="pv2-input" id="ra-memory-source" value={draft.source_ref} onChange={(event) => setDraft({ ...draft, source_ref: event.target.value })} /></label>
          <label className="pv2-field" htmlFor="ra-memory-text"><span>内容</span><input className="pv2-input" id="ra-memory-text" value={draft.content_text} onChange={(event) => setDraft({ ...draft, content_text: event.target.value })} /></label>
        </div>
        <div className="pv2-row-actions" style={{ marginTop: 12 }}>
          <button className="pv2-button-primary" type="button" onClick={() => void createMemory()}>创建记忆候选</button>
          <button className="pv2-button-ghost" type="button" onClick={() => void buildPack()}>构建 Context Pack</button>
        </div>
        <PaperTable
          rows={memories}
          empty="暂无长期记忆。"
          columns={[
            { key: "title", header: "标题", render: (row) => <><span className="ra-title">{row.title}</span><br /><span className="pv2-muted">{row.subject_key}</span></> },
            { key: "type", header: "类型", render: (row) => row.memory_type || "-" },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.approval_status} /> },
            { key: "source", header: "来源", render: (row) => row.source_ref || "manual" },
            { key: "time", header: "更新时间", render: (row) => formatDateTime(row.updated_at) },
            { key: "action", header: "操作", render: (row) => {
              const canApprove = row.approval_status === "draft";
              const typed = confirmations[row.memory_id] || "";
              return <div className="ra-panel-list">
                {canApprove ? <label className="pv2-field" htmlFor={`ra-memory-confirm-${row.memory_id}`}><span>确认文本：<span className="pv2-mono">{MEMORY_CONFIRM}</span></span><input className="pv2-input" id={`ra-memory-confirm-${row.memory_id}`} value={typed} onChange={(event) => setConfirmations({ ...confirmations, [row.memory_id]: event.target.value })} /></label> : null}
                <button className="pv2-button-ghost" type="button" disabled={!canApprove || typed !== MEMORY_CONFIRM || busyId === row.memory_id} onClick={() => void approve(row)}>{canApprove ? "批准" : "已处理"}</button>
                <DetailDrawer title="内容与证据" data={row} />
              </div>;
            } },
          ]}
        />
        {!memories.length ? <EmptyState title="Memory Ledger 为空" /> : null}
      </SectionCard>
      <SectionCard title="Context Packs" eyebrow="non-RAG deterministic bundle">
        <PaperTable
          rows={packs}
          empty="暂无 Context Pack。"
          columns={[
            { key: "id", header: "Pack", render: (row) => <><span className="pv2-mono">{row.context_pack_id}</span><br /><span className="pv2-muted">{row.pack_summary}</span></> },
            { key: "budget", header: "Token Budget", render: (row) => row.token_budget || "-" },
            { key: "checksum", header: "Checksum", render: (row) => <span className="pv2-mono">{String(row.checksum || "-").slice(0, 16)}</span> },
            { key: "detail", header: "详情", render: (row) => <DetailDrawer title="pack_json/source refs" data={row} /> },
          ]}
        />
      </SectionCard>
    </main>
  );
}
