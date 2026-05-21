"use client";

import { useCallback, useEffect, useState } from "react";

import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState, formatDateTime } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantApproval } from "@/lib/research-assistant/api";

export default function ResearchAssistantApprovalsPage() {
  const [approvals, setApprovals] = useState<AssistantApproval[]>([]);
  const [summary, setSummary] = useState("审批高风险 MCP 调用");
  const [confirmation, setConfirmation] = useState("APPROVE_RESEARCH_ASSISTANT_ACTION");
  const [typedConfirmations, setTypedConfirmations] = useState<Record<string, string>>({});
  const [error, setError] = useState<unknown>(null);
  const [actionError, setActionError] = useState<unknown>(null);
  const [busyId, setBusyId] = useState<string | null>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      setApprovals((await researchAssistantApi.approvals({ limit: 100 })).items);
    } catch (exc) {
      setError(exc);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  async function createApproval() {
    setActionError(null);
    try {
      await researchAssistantApi.createApproval({
        approval_type: "manual.high_risk",
        risk_level: "high",
        plan_digest: `manual-${Date.now()}`,
        summary,
        required_confirmation_text: confirmation,
        created_by: "ui",
      });
      await load();
    } catch (exc) {
      setActionError(exc);
    }
  }

  async function approve(row: AssistantApproval) {
    setBusyId(row.approval_id);
    setActionError(null);
    try {
      await researchAssistantApi.approve(row.approval_id, typedConfirmations[row.approval_id] || "");
      await load();
    } catch (exc) {
      setActionError(exc);
    } finally {
      setBusyId(null);
    }
  }

  async function reject(approvalId: string) {
    setBusyId(approvalId);
    setActionError(null);
    try {
      await researchAssistantApi.reject(approvalId);
      await load();
    } catch (exc) {
      setActionError(exc);
    } finally {
      setBusyId(null);
    }
  }

  return (
    <main>
      <ApiErrorBox error={error} />
      <ApiErrorBox error={actionError} title="审批操作失败" />
      <SectionCard title="审批中心" eyebrow="risk / digest / replay">
        <div className="pv2-form-grid pv2-filter-card">
          <label className="pv2-field" htmlFor="ra-approval-summary"><span>审批摘要</span><input className="pv2-input" id="ra-approval-summary" value={summary} onChange={(event) => setSummary(event.target.value)} /></label>
          <label className="pv2-field" htmlFor="ra-approval-confirm"><span>确认文本</span><input className="pv2-input" id="ra-approval-confirm" value={confirmation} onChange={(event) => setConfirmation(event.target.value)} /></label>
          <div className="pv2-field"><span className="pv2-label">操作</span><button className="pv2-button-primary" type="button" onClick={() => void createApproval()}>创建审批请求</button></div>
        </div>
        <PaperTable
          rows={approvals}
          empty="暂无审批请求。"
          columns={[
            { key: "summary", header: "摘要", render: (row) => <><span className="ra-title">{row.summary}</span><br /><span className="pv2-muted pv2-mono">{row.plan_digest}</span></> },
            { key: "risk", header: "风险", render: (row) => <StatusBadge status={row.risk_level} /> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "time", header: "创建时间", render: (row) => formatDateTime(row.created_at) },
            { key: "action", header: "操作", render: (row) => {
              const isPending = row.status === "pending";
              const requiredText = String(row.required_confirmation_text || "");
              const typed = typedConfirmations[row.approval_id] || "";
              const canApprove = isPending && requiredText.length > 0 && typed === requiredText && busyId !== row.approval_id;
              return (
                <div className="ra-panel-list">
                  {isPending ? (
                    <label className="pv2-field" htmlFor={`ra-approve-confirm-${row.approval_id}`}>
                      <span>输入确认文本：<span className="pv2-mono">{requiredText}</span></span>
                      <input className="pv2-input" id={`ra-approve-confirm-${row.approval_id}`} value={typed} onChange={(event) => setTypedConfirmations({ ...typedConfirmations, [row.approval_id]: event.target.value })} />
                    </label>
                  ) : <span className="pv2-muted">已完成审批或拒绝。</span>}
                  <div className="pv2-chip-row">
                    <button className="pv2-button-primary" type="button" disabled={!canApprove} onClick={() => void approve(row)}>批准</button>
                    <button className="pv2-button-danger" type="button" disabled={!isPending || busyId === row.approval_id} onClick={() => void reject(row.approval_id)}>拒绝</button>
                    <DetailDrawer title="审批详情" data={row} />
                  </div>
                </div>
              );
            } },
          ]}
        />
        {!approvals.length ? <EmptyState title="暂无待处理审批" /> : null}
      </SectionCard>
    </main>
  );
}
