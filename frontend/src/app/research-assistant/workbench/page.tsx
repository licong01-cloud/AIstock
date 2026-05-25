
"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { ApiErrorBox, DetailDrawer, EmptyState, asObject, display, formatDateTime } from "@/components/research-assistant/AssistantShared";
import {
  researchAssistantApi,
  type AssistantActionProposal,
  type AssistantActionProposalResult,
  type AssistantCapability,
  type AssistantMcpTool,
  type AssistantTask,
  type JsonObject,
} from "@/lib/research-assistant/api";

const DEFAULT_QE_DRAFT_PAYLOAD = `{
  "template_kind": "custom_evo",
  "title": "QE 10 loop draft",
  "config_json": {
    "loops": [
      { "factor_keys": ["alpha001"], "model_id": "lightgbm" }
    ],
    "stock_pool": "fixed_pit_pool",
    "backtest_window": { "start": "2023-01-01", "end": "2024-12-31" }
  }
}`;

const LEGACY_DRY_RUN_PAYLOAD = `{
  "title": "候选 Issue",
  "problem_statement": "用于验证 dry-run 边界"
}`;

type ExecutionStep = "propose" | "confirm" | "preflight" | "approve" | "execute";
type ActionEvents = { task_events?: JsonObject[]; mcp_tool_events?: JsonObject[]; trace_events?: JsonObject[] };

function parseJsonObject(text: string): JsonObject | null {
  try {
    const parsed = JSON.parse(text) as unknown;
    return asObject(parsed);
  } catch {
    return null;
  }
}

function summarizePreflight(preflight: unknown) {
  const data = asObject(preflight);
  const traceEvent = asObject(data.trace_event);
  const deepLinks = Array.isArray(data.deep_links) ? data.deep_links : Array.isArray(data.deep_link_refs) ? data.deep_link_refs : [];
  return {
    passed: Boolean(data.passed),
    approvalRequired: Boolean(data.approval_required ?? data.requires_approval),
    missingConfirmations: Array.isArray(data.missing_confirmations) ? data.missing_confirmations : [],
    failedChecks: Array.isArray(data.failed_checks) ? data.failed_checks : [],
    toolEvent: traceEvent.event_type || data.event_type || data.status || "preflight_result",
    deepLinks,
  };
}

function confirmationText(capability: AssistantCapability | null, fallback = "") {
  const options = Array.isArray(capability?.required_confirmations) ? capability.required_confirmations.filter((item): item is string => typeof item === "string") : [];
  return fallback || options[0] || "";
}

function firstResultCard(result: AssistantActionProposalResult | null): JsonObject | null {
  if (!result) return null;
  const cards = Array.isArray(result.human_cards) ? result.human_cards : [];
  if (cards[0] && typeof cards[0] === "object") return cards[0] as JsonObject;
  const eventCard = asObject(asObject(result.tool_event).result_card_json);
  return Object.keys(eventCard).length ? eventCard : null;
}

function disabledReason(step: ExecutionStep, params: {
  capability: AssistantCapability | null;
  taskId: string;
  payload: JsonObject | null;
  proposal: AssistantActionProposal | null;
  confirmation: string;
  approvalConfirmation: string;
  busy: boolean;
}) {
  const requiredConfirm = confirmationText(params.capability);
  if (params.busy) return "操作正在执行";
  if (step === "propose") {
    if (!params.capability) return "请选择 capability";
    if (!params.taskId) return "请选择任务账本";
    if (!params.payload) return "payload 必须是 JSON object";
    return "";
  }
  if (!params.proposal) return "请选择 Action Proposal";
  if (step === "confirm") {
    if (params.proposal.status !== "proposed" && params.proposal.status !== "preflight_failed") return `当前 ${display(params.proposal.status)} 不可确认`;
    if (requiredConfirm && params.confirmation !== requiredConfirm) return `请输入确认文本 ${requiredConfirm}`;
    return "";
  }
  if (step === "preflight") {
    if (params.proposal.status !== "confirmed" && params.proposal.status !== "approval_required" && params.proposal.status !== "approved" && params.proposal.status !== "preflight_failed") return "请先确认 Action Proposal";
    return "";
  }
  if (step === "approve") {
    if (params.proposal.status !== "approval_required" && params.proposal.status !== "approved") return "仅 approval_required 状态需要审批";
    if (requiredConfirm && params.approvalConfirmation !== requiredConfirm) return `请输入审批确认文本 ${requiredConfirm}`;
    return "";
  }
  if (params.proposal.status !== "preflight_passed" && params.proposal.status !== "approved") return "请先通过 preflight；如需要请完成 approval";
  return "";
}

function HumanResultCard({ result }: { result: AssistantActionProposalResult | null }) {
  const card = firstResultCard(result);
  const error = asObject(result?.error);
  if (!result) return <EmptyState title="等待 Action Proposal" hint="执行结果会以卡片展示；raw JSON 仅在调试抽屉中查看" />;
  if (error.code) {
    return (
      <div className="pv2-error-panel ra-action-result-card" role="alert">
        <strong>{display(error.code)}</strong>
        <p>{display(error.human_reason)}</p>
        <p className="pv2-muted">下一步：{display(error.next_step)}</p>
        <p className="pv2-muted">审计链接：<span className="pv2-mono">{display(error.audit_link)}</span></p>
      </div>
    );
  }
  return (
    <div className="ra-action-result-card">
      <span className="pv2-eyebrow">执行结果</span>
      <h3>{display(card?.title || result.status)}</h3>
      <p>{display(card?.summary || "暂无摘要")}</p>
      <div className="pv2-chip-row">
        <span className="pv2-chip">executed: {display(result.executed)}</span>
        {card?.template_id ? <span className="pv2-chip">template: {display(card.template_id)}</span> : null}
        {result.trace_id ? <span className="pv2-chip">trace: {display(result.trace_id)}</span> : null}
      </div>
      {card?.next_step ? <p className="pv2-muted">下一步：{display(card.next_step)}</p> : null}
    </div>
  );
}

function summarizeActionEvents(events: ActionEvents | null) {
  if (!events) return [] as Array<[string, unknown]>;
  return [
    ["Task events", events.task_events?.length || 0],
    ["MCP events", events.mcp_tool_events?.length || 0],
    ["Trace events", events.trace_events?.length || 0],
  ] as Array<[string, unknown]>;
}

export default function ResearchAssistantWorkbenchPage() {
  const [capabilities, setCapabilities] = useState<AssistantCapability[]>([]);
  const [actions, setActions] = useState<AssistantActionProposal[]>([]);
  const [tools, setTools] = useState<AssistantMcpTool[]>([]);
  const [tasks, setTasks] = useState<AssistantTask[]>([]);
  const [selectedCapabilityKey, setSelectedCapabilityKey] = useState("qe.create_experiment_draft");
  const [selectedActionId, setSelectedActionId] = useState("");
  const [selectedTool, setSelectedTool] = useState<AssistantMcpTool | null>(null);
  const [selectedTaskId, setSelectedTaskId] = useState("");
  const [payloadText, setPayloadText] = useState(DEFAULT_QE_DRAFT_PAYLOAD);
  const [legacyPayloadText, setLegacyPayloadText] = useState(LEGACY_DRY_RUN_PAYLOAD);
  const [proposalTitle, setProposalTitle] = useState("生成 QE template 草案");
  const [proposalSummary, setProposalSummary] = useState("只生成草案，不触发 materialize/run；确认后进入 Action Proposal、preflight 和审批流程");
  const [confirmation, setConfirmation] = useState("CONFIRM_QE_DRAFT");
  const [approvalConfirmation, setApprovalConfirmation] = useState("CONFIRM_QE_DRAFT");
  const [preflight, setPreflight] = useState<unknown>(null);
  const [dryRunResult, setDryRunResult] = useState<unknown>(null);
  const [executeResult, setExecuteResult] = useState<AssistantActionProposalResult | null>(null);
  const [actionEvents, setActionEvents] = useState<ActionEvents | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [actionError, setActionError] = useState<unknown>(null);
  const [legacyError, setLegacyError] = useState<unknown>(null);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState<ExecutionStep | null>(null);
  const [legacyBusy, setLegacyBusy] = useState<"preflight" | "dry_run" | null>(null);

  const parsedPayload = useMemo(() => parseJsonObject(payloadText), [payloadText]);
  const legacyParsedPayload = useMemo(() => parseJsonObject(legacyPayloadText), [legacyPayloadText]);
  const selectedCapability = useMemo(() => capabilities.find((item) => item.capability_key === selectedCapabilityKey) || null, [capabilities, selectedCapabilityKey]);
  const selectedAction = useMemo(() => actions.find((item) => item.action_proposal_id === selectedActionId) || null, [actions, selectedActionId]);
  const preflightSummary = preflight ? summarizePreflight(preflight) : null;
  const proposalType = selectedCapability?.capability_type === "workflow_pack" ? "workflow_pack" : selectedCapability?.capability_type === "skill" ? "skill" : "mcp_tool";
  const disabled = (step: ExecutionStep) => disabledReason(step, { capability: selectedCapability, taskId: selectedTaskId, payload: parsedPayload, proposal: selectedAction, confirmation, approvalConfirmation, busy: busy !== null });

  const refreshActions = useCallback(async (preferredId?: string) => {
    const page = await researchAssistantApi.actionProposals({ limit: 100 });
    setActions(page.items);
    if (preferredId) {
      setSelectedActionId(preferredId);
    } else {
      setSelectedActionId((current) => current || page.items[0]?.action_proposal_id || "");
    }
  }, []);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [capPage, actionPage, toolPage, taskPage] = await Promise.all([
        researchAssistantApi.capabilities({ status: "approved", limit: 200 }),
        researchAssistantApi.actionProposals({ limit: 100 }),
        researchAssistantApi.mcpTools({ limit: 200 }),
        researchAssistantApi.tasks({ limit: 100 }),
      ]);
      setCapabilities(capPage.items);
      setActions(actionPage.items);
      setTools(toolPage.items);
      setTasks(taskPage.items);
      setSelectedCapabilityKey((current) => capPage.items.some((item) => item.capability_key === current) ? current : capPage.items[0]?.capability_key || "");
      setSelectedActionId((current) => current || actionPage.items[0]?.action_proposal_id || "");
      setSelectedTool((current) => current || toolPage.items[0] || null);
      setSelectedTaskId((current) => current || taskPage.items[0]?.task_id || "");
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    const text = confirmationText(selectedCapability);
    if (text) {
      setConfirmation(text);
      setApprovalConfirmation(text);
    }
  }, [selectedCapability]);

  async function runAction(step: ExecutionStep) {
    if (disabled(step)) return;
    setBusy(step);
    setActionError(null);
    try {
      if (step === "propose") {
        const created = await researchAssistantApi.createActionProposal({
          task_id: selectedTaskId,
          capability_key: selectedCapabilityKey,
          proposal_type: proposalType,
          title: proposalTitle,
          summary: proposalSummary,
          input_json: parsedPayload || {},
        });
        setExecuteResult(null);
        setPreflight(null);
        setActionEvents(null);
        await refreshActions(created.action_proposal_id);
      }
      if (step === "confirm" && selectedAction) {
        const updated = await researchAssistantApi.confirmActionProposal(selectedAction.action_proposal_id, { confirmation_text: confirmation });
        await refreshActions(updated.action_proposal_id);
      }
      if (step === "preflight" && selectedAction) {
        const result = await researchAssistantApi.preflightActionProposal(selectedAction.action_proposal_id, {});
        setPreflight(result.preflight || result);
        const updated = asObject(result.proposal) as AssistantActionProposal;
        await refreshActions(updated.action_proposal_id || selectedAction.action_proposal_id);
      }
      if (step === "approve" && selectedAction) {
        const result = await researchAssistantApi.approveActionProposal(selectedAction.action_proposal_id, { confirmation_text: approvalConfirmation });
        const updated = asObject(result.proposal) as AssistantActionProposal;
        await refreshActions(updated.action_proposal_id || selectedAction.action_proposal_id);
      }
      if (step === "execute" && selectedAction) {
        const result = await researchAssistantApi.executeActionProposal(selectedAction.action_proposal_id, {});
        setExecuteResult(result);
        await refreshActions(result.proposal?.action_proposal_id || selectedAction.action_proposal_id);
        setActionEvents(await researchAssistantApi.actionProposalEvents(result.proposal?.action_proposal_id || selectedAction.action_proposal_id) as ActionEvents);
      }
    } catch (exc) {
      setActionError(exc);
    } finally {
      setBusy(null);
    }
  }

  async function runLegacyPreflight() {
    if (!selectedTool || !legacyParsedPayload) return;
    setLegacyBusy("preflight");
    setLegacyError(null);
    setDryRunResult(null);
    try {
      const result = await researchAssistantApi.preflightMcpTool({
        task_id: selectedTaskId || undefined,
        server_key: selectedTool.server_key,
        tool_name: selectedTool.tool_name,
        payload_json: legacyParsedPayload,
        idempotency_key: `${selectedTool.tool_name}-ui-preflight`,
      });
      setDryRunResult(result);
    } catch (exc) {
      setLegacyError(exc);
    } finally {
      setLegacyBusy(null);
    }
  }

  async function runLegacyDryRun() {
    if (!selectedTool || !legacyParsedPayload) return;
    setLegacyBusy("dry_run");
    setLegacyError(null);
    setDryRunResult(null);
    try {
      const result = await researchAssistantApi.dryRunExecuteTool({
        task_id: selectedTaskId || undefined,
        server_key: selectedTool.server_key,
        tool_name: selectedTool.tool_name,
        payload_json: legacyParsedPayload,
        idempotency_key: `${selectedTool.tool_name}-ui-dry-run-execute`,
        deep_link: `/research-assistant/workbench?server_key=${selectedTool.server_key}&tool=${selectedTool.tool_name}`,
      });
      setDryRunResult(result);
    } catch (exc) {
      setLegacyError(exc);
    } finally {
      setLegacyBusy(null);
    }
  }

  return (
    <main>
      <ApiErrorBox error={error} />
      <ApiErrorBox error={actionError} title="Action Proposal 操作失败" />
      <div className="ra-two-column">
        <SectionCard title="Action Proposal 执行控制台" eyebrow="proposal / confirm / preflight / approval / execute">
          <label className="pv2-field" htmlFor="ra-capability-select">
            <span>选择 capability</span>
            <select className="pv2-select" id="ra-capability-select" value={selectedCapabilityKey} onChange={(event) => setSelectedCapabilityKey(event.target.value)}>
              {capabilities.map((capability) => <option key={capability.capability_id} value={capability.capability_key}>{capability.title || capability.capability_key}</option>)}
            </select>
          </label>
          <div className="pv2-chip-row" style={{ marginTop: 10 }}>
            <StatusBadge status={selectedCapability?.risk_level} />
            <StatusBadge status={selectedCapability?.side_effect_level} />
            <span className="pv2-chip">{display(selectedCapability?.capability_key)}</span>
          </div>
          <label className="pv2-field" htmlFor="ra-task-select" style={{ marginTop: 12 }}>
            <span>任务账本</span>
            <select className="pv2-select" id="ra-task-select" value={selectedTaskId} onChange={(event) => setSelectedTaskId(event.target.value)}>
              <option value="">请选择任务</option>
              {tasks.map((task) => <option key={task.task_id} value={task.task_id}>{task.title}</option>)}
            </select>
          </label>
          <label className="pv2-field" htmlFor="ra-proposal-title" style={{ marginTop: 12 }}><span>Proposal 标题</span><input className="pv2-input" id="ra-proposal-title" value={proposalTitle} onChange={(event) => setProposalTitle(event.target.value)} /></label>
          <label className="pv2-field" htmlFor="ra-proposal-summary" style={{ marginTop: 12 }}><span>Proposal 摘要</span><input className="pv2-input" id="ra-proposal-summary" value={proposalSummary} onChange={(event) => setProposalSummary(event.target.value)} /></label>
          <label className="pv2-field" htmlFor="ra-action-payload" style={{ marginTop: 12 }}>
            <span>输入 JSON</span>
            <textarea className="pv2-textarea" id="ra-action-payload" value={payloadText} onChange={(event) => setPayloadText(event.target.value)} />
          </label>
          {!parsedPayload ? <span className="pv2-error-meta">JSON 无效，无法创建 Proposal</span> : null}
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button-primary" type="button" onClick={() => void runAction("propose")} disabled={Boolean(disabled("propose"))}>{busy === "propose" ? "创建中..." : "创建 Proposal"}</button>
            <button className="pv2-button-ghost" type="button" onClick={() => void load()} disabled={loading}>{loading ? "加载中..." : "刷新"}</button>
          </div>
          {disabled("propose") ? <p className="pv2-help">{disabled("propose")}</p> : <p className="pv2-help">创建 Proposal 后仍不会直接调用 MCP。</p>}
          {selectedCapability ? <DetailDrawer title="capability schema / gates" data={selectedCapability} /> : <EmptyState title="无 capability" />}
        </SectionCard>

        <SectionCard title="执行状态" eyebrow="human-readable state">
          <label className="pv2-field" htmlFor="ra-action-select">
            <span>选择 Action Proposal</span>
            <select className="pv2-select" id="ra-action-select" value={selectedActionId} onChange={(event) => { setSelectedActionId(event.target.value); setExecuteResult(null); setActionEvents(null); }}>
              <option value="">请选择 Proposal</option>
              {actions.map((action) => <option key={action.action_proposal_id} value={action.action_proposal_id}>{action.title} / {action.status}</option>)}
            </select>
          </label>
          {selectedAction ? (
            <div className="pv2-readable-panel" style={{ marginTop: 12 }}>
              <div className="pv2-readable-table">
                <div className="pv2-readable-row"><div className="pv2-readable-key">状态</div><div className="pv2-readable-value"><StatusBadge status={selectedAction.status} /></div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">Capability</div><div className="pv2-readable-value"><span className="pv2-mono">{display(selectedAction.capability_key)}</span></div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">Plan digest</div><div className="pv2-readable-value"><span className="pv2-mono">{display(selectedAction.plan_digest)}</span></div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">更新时间</div><div className="pv2-readable-value">{formatDateTime(selectedAction.updated_at)}</div></div>
              </div>
            </div>
          ) : <EmptyState title="尚未选择 Proposal" />}
          <label className="pv2-field" htmlFor="ra-confirm-text" style={{ marginTop: 12 }}>
            <span>确认文本</span>
            <input className="pv2-input" id="ra-confirm-text" value={confirmation} onChange={(event) => setConfirmation(event.target.value)} />
          </label>
          <label className="pv2-field" htmlFor="ra-approval-text" style={{ marginTop: 12 }}>
            <span>审批确认文本</span>
            <input className="pv2-input" id="ra-approval-text" value={approvalConfirmation} onChange={(event) => setApprovalConfirmation(event.target.value)} />
          </label>
          <div className="ra-gate-grid" style={{ marginTop: 12 }}>
            {(["confirm", "preflight", "approve", "execute"] as ExecutionStep[]).map((step) => {
              const reason = disabled(step);
              const labels: Record<ExecutionStep, string> = { propose: "创建", confirm: "确认", preflight: "Preflight", approve: "审批", execute: "执行" };
              return (
                <div className="ra-gate-card" key={step}>
                  <strong>{labels[step]}</strong>
                  <p>{reason || "可以执行下一步"}</p>
                  <button className={step === "execute" ? "pv2-button-primary" : "pv2-button-ghost"} type="button" disabled={Boolean(reason)} onClick={() => void runAction(step)}>{busy === step ? "执行中..." : labels[step]}</button>
                </div>
              );
            })}
          </div>
        </SectionCard>
      </div>

      <div className="ra-two-column">
        <SectionCard title="Preflight / Result" eyebrow="cards first, json debug drawer second">
          {preflightSummary ? (
            <div className="pv2-readable-panel">
              <div className="pv2-readable-table">
                <div className="pv2-readable-row"><div className="pv2-readable-key">Passed</div><div className="pv2-readable-value"><StatusBadge status={preflightSummary.passed ? "passed" : "blocked"} /></div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">Approval</div><div className="pv2-readable-value">{preflightSummary.approvalRequired ? "需要审批" : "无需审批"}</div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">Failed checks</div><div className="pv2-readable-value">{preflightSummary.failedChecks.length ? preflightSummary.failedChecks.map(display).join(" / ") : "-"}</div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">Tool event</div><div className="pv2-readable-value"><span className="pv2-mono">{display(preflightSummary.toolEvent)}</span></div></div>
              </div>
            </div>
          ) : <EmptyState title="等待 Action preflight" hint="preflight 结果会先以卡片展示" />}
          <HumanResultCard result={executeResult} />
          {summarizeActionEvents(actionEvents).length ? (
            <div className="pv2-chip-row" style={{ marginTop: 12 }}>
              {summarizeActionEvents(actionEvents).map(([key, value]) => <span className="pv2-chip" key={key}>{key}: {display(value)}</span>)}
            </div>
          ) : null}
          {preflight ? <DetailDrawer title="调试 preflight payload" data={preflight} /> : null}
          {executeResult ? <DetailDrawer title="调试 execute payload" data={executeResult} /> : null}
          {actionEvents ? <DetailDrawer title="审计事件 payload" data={actionEvents} /> : null}
        </SectionCard>

        <SectionCard title="旧版 dry-run 兼容" eyebrow="no real execution">
          <ApiErrorBox error={legacyError} title="旧版 dry-run 失败" />
          <label className="pv2-field" htmlFor="ra-tool-select">
            <span>选择 MCP 工具</span>
            <select className="pv2-select" id="ra-tool-select" value={selectedTool?.tool_id || ""} onChange={(event) => setSelectedTool(tools.find((tool) => tool.tool_id === event.target.value) || null)}>
              {tools.map((tool) => <option key={tool.tool_id} value={tool.tool_id}>{tool.server_key} / {tool.tool_name}</option>)}
            </select>
          </label>
          <label className="pv2-field" htmlFor="ra-legacy-payload" style={{ marginTop: 12 }}>
            <span>Dry-run JSON</span>
            <textarea className="pv2-textarea" id="ra-legacy-payload" value={legacyPayloadText} onChange={(event) => setLegacyPayloadText(event.target.value)} />
          </label>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button-ghost" type="button" onClick={() => void runLegacyPreflight()} disabled={!selectedTool || !legacyParsedPayload || legacyBusy !== null}>{legacyBusy === "preflight" ? "preflight 中..." : "执行 preflight"}</button>
            <button className="pv2-button-ghost" type="button" onClick={() => void runLegacyDryRun()} disabled={!selectedTool || !legacyParsedPayload || legacyBusy !== null}>{legacyBusy === "dry_run" ? "dry-run 中..." : "执行 dry-run"}</button>
          </div>
          {!legacyParsedPayload ? <span className="pv2-error-meta">JSON 无效，无法执行</span> : null}
          {dryRunResult ? <DetailDrawer title="dry-run / preflight debug payload" data={dryRunResult} /> : <EmptyState title="等待 dry-run" hint="仅用于验证旧 execute gateway 兼容边界" />}
        </SectionCard>
      </div>

      <SectionCard title="Capability 与 Proposal 目录" eyebrow="real catalog">
        <PaperTable
          rows={capabilities}
          empty="暂无 capability；请先执行 catalog seed 或 capability sync。"
          columns={[
            { key: "capability", header: "能力", render: (row) => <><span className="ra-title">{row.title || row.capability_key}</span><br /><span className="pv2-muted pv2-mono">{row.capability_key}</span></> },
            { key: "risk", header: "风险", render: (row) => <StatusBadge status={row.risk_level} /> },
            { key: "effect", header: "副作用", render: (row) => <StatusBadge status={row.side_effect_level} /> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
          ]}
        />
      </SectionCard>
    </main>
  );
}
