
"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { AgentTeamsRunView } from "@/components/research-assistant/AgentTeamsRunView";
import { ApiErrorBox, DetailDrawer, DiagnosticLogBlock, EmptyState, asObject, display, formatDateTime } from "@/components/research-assistant/AssistantShared";
import {
  LOCAL_DATA_MANAGEMENT_CAPABILITY,
  LOCAL_DATA_MANAGEMENT_PHASES,
  isLocalDataManagementTool,
  localDataRiskLabel,
  localDataToolPhase,
  localDataToolTitle,
  researchAssistantApi,
  type AssistantActionProposal,
  type AssistantActionProposalResult,
  type AssistantAgentRun,
  type AssistantCapability,
  type AssistantMcpTool,
  type AssistantTask,
  type AssistantTraceEvent,
  type JsonObject,
} from "@/lib/research-assistant/api";
import uiCopy from "@/lib/research-assistant/ui-copy";

const workbenchCopy = uiCopy.workbench;
const DEFAULT_QE_DRAFT_PAYLOAD = JSON.stringify(workbenchCopy.defaultQeDraftPayload, null, 2);
const LEGACY_DRY_RUN_PAYLOAD = JSON.stringify(workbenchCopy.legacyDryRunPayload, null, 2);

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
  const copy = workbenchCopy.disabledReasons;
  if (params.busy) return copy.busy;
  if (step === "propose") {
    if (!params.capability) return copy.selectCapability;
    if (!params.taskId) return copy.selectTask;
    if (!params.payload) return copy.payloadObject;
    return "";
  }
  if (!params.proposal) return copy.selectProposal;
  if (step === "confirm") {
    if (params.proposal.status !== "proposed" && params.proposal.status !== "preflight_failed") return `${copy.notConfirmablePrefix} ${display(params.proposal.status)} ${copy.notConfirmableSuffix}`;
    if (requiredConfirm && params.confirmation !== requiredConfirm) return `${copy.enterConfirmationPrefix} ${requiredConfirm}`;
    return "";
  }
  if (step === "preflight") {
    if (params.proposal.status !== "confirmed" && params.proposal.status !== "approval_required" && params.proposal.status !== "approved" && params.proposal.status !== "preflight_failed") return copy.confirmFirst;
    return "";
  }
  if (step === "approve") {
    if (params.proposal.status !== "approval_required" && params.proposal.status !== "approved") return copy.approvalOnly;
    if (requiredConfirm && params.approvalConfirmation !== requiredConfirm) return `${copy.enterApprovalConfirmationPrefix} ${requiredConfirm}`;
    return "";
  }
  if (params.proposal.status !== "preflight_passed" && params.proposal.status !== "approved") return copy.preflightFirst;
  return "";
}

function HumanResultCard({ result }: { result: AssistantActionProposalResult | null }) {
  const card = firstResultCard(result);
  const error = asObject(result?.error);
  if (!result) return <EmptyState title={workbenchCopy.result.emptyTitle} hint={workbenchCopy.result.emptyHint} />;
  if (error.code) {
    return (
      <div className="pv2-error-panel ra-action-result-card" role="alert">
        <strong>{display(error.code)}</strong>
        <p>{display(error.human_reason)}</p>
        <p className="pv2-muted">{workbenchCopy.result.nextStepPrefix}{display(error.next_step)}</p>
        <p className="pv2-muted">{workbenchCopy.result.auditLinkPrefix}<span className="pv2-mono">{display(error.audit_link)}</span></p>
        <DiagnosticLogBlock title="Action error log" data={error} testId="ra-action-error-log" />
      </div>
    );
  }
  return (
    <div className="ra-action-result-card">
      <span className="pv2-eyebrow">{workbenchCopy.result.eyebrow}</span>
      <h3>{display(card?.title || result.status)}</h3>
      <p>{display(card?.summary || workbenchCopy.result.emptySummary)}</p>
      <div className="pv2-chip-row">
        <span className="pv2-chip">executed: {display(result.executed)}</span>
        {card?.template_id ? <span className="pv2-chip">template: {display(card.template_id)}</span> : null}
        {result.trace_id ? <span className="pv2-chip">trace: {display(result.trace_id)}</span> : null}
      </div>
      {card?.next_step ? <p className="pv2-muted">{workbenchCopy.result.nextStepPrefix}{display(card.next_step)}</p> : null}
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
  const [agentRuns, setAgentRuns] = useState<AssistantAgentRun[]>([]);
  const [traceEvents, setTraceEvents] = useState<AssistantTraceEvent[]>([]);
  const [selectedCapabilityKey, setSelectedCapabilityKey] = useState("qe.create_experiment_draft");
  const [selectedActionId, setSelectedActionId] = useState("");
  const [selectedTool, setSelectedTool] = useState<AssistantMcpTool | null>(null);
  const [selectedTaskId, setSelectedTaskId] = useState("");
  const [payloadText, setPayloadText] = useState(DEFAULT_QE_DRAFT_PAYLOAD);
  const [legacyPayloadText, setLegacyPayloadText] = useState(LEGACY_DRY_RUN_PAYLOAD);
  const [proposalTitle, setProposalTitle] = useState<string>(workbenchCopy.defaultProposalTitle);
  const [proposalSummary, setProposalSummary] = useState<string>(workbenchCopy.defaultProposalSummary);
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
  const localDataTools = useMemo(() => tools.filter(isLocalDataManagementTool), [tools]);
  const selectedToolPhase = selectedTool ? localDataToolPhase(selectedTool.tool_name) : undefined;
  const selectedIsLocalData = selectedTool ? isLocalDataManagementTool(selectedTool) : false;
  const localDataPhaseRows = LOCAL_DATA_MANAGEMENT_PHASES.map((phase) => ({
    ...phase,
    status: selectedToolPhase?.key === phase.key ? "current" : phase.requiresConfirmation ? "locked" : localDataTools.length ? "idle" : "locked",
  }));

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
      const [capPage, actionPage, toolPage, taskPage, runsPage, tracesPage] = await Promise.all([
        researchAssistantApi.capabilities({ status: "approved", limit: 200 }),
        researchAssistantApi.actionProposals({ limit: 100 }),
        researchAssistantApi.mcpTools({ limit: 200 }),
        researchAssistantApi.tasks({ limit: 100 }),
        researchAssistantApi.agentRuns({ limit: 100 }),
        researchAssistantApi.traceEvents({ limit: 100 }),
      ]);
      setCapabilities(capPage.items);
      setActions(actionPage.items);
      setTools(toolPage.items);
      setTasks(taskPage.items);
      setAgentRuns(runsPage.items);
      setTraceEvents(tracesPage.items);
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
      <AgentTeamsRunView runs={agentRuns} traceEvents={traceEvents} />
      <SectionCard title="本地数据 MCP 工作台" eyebrow="local_data_management / check-plan-confirm">
        <div className="pv2-readable-panel" data-testid="ra-local-data-workbench-card">
          <div className="pv2-readable-table">
            <div className="pv2-readable-row"><div className="pv2-readable-key">能力</div><div className="pv2-readable-value">{LOCAL_DATA_MANAGEMENT_CAPABILITY.displayName}（{LOCAL_DATA_MANAGEMENT_CAPABILITY.capabilityKey}）</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">Gateway module</div><div className="pv2-readable-value">{LOCAL_DATA_MANAGEMENT_CAPABILITY.gatewayModule}</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">当前目录</div><div className="pv2-readable-value">{localDataTools.length ? `已读取 ${localDataTools.length} 个本地数据工具` : "尚未读取到 local_data 工具，不使用静态假工具冒充可执行能力"}</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">选中工具</div><div className="pv2-readable-value">{selectedTool ? `${selectedTool.server_key}/${selectedTool.tool_name}` : "未选择工具"}</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">所属能力</div><div className="pv2-readable-value">{selectedIsLocalData ? "local_data_management" : "当前选中工具不是本地数据工具"}</div></div>
          </div>
        </div>
        <div className="pv2-readable-list" style={{ marginTop: 12 }}>
          {localDataPhaseRows.map((phase) => (
            <div className="pv2-readable-item" key={phase.key}>
              <strong>{phase.title}</strong> <StatusBadge status={phase.status === "idle" ? "pending" : phase.status} />
              <p className="pv2-muted">{phase.description}</p>
              <span className="pv2-chip">{localDataRiskLabel(phase.riskLevel)}</span>
              {phase.requiresConfirmation ? <span className="pv2-chip">需要确认</span> : <span className="pv2-chip">无需确认</span>}
            </div>
          ))}
        </div>
      </SectionCard>

      <ApiErrorBox error={actionError} title={workbenchCopy.sections.actionErrorTitle} />
      <div className="ra-two-column">
        <SectionCard title={workbenchCopy.sections.consoleTitle} eyebrow={workbenchCopy.sections.consoleEyebrow}>
          <label className="pv2-field" htmlFor="ra-capability-select">
            <span>{workbenchCopy.sections.capabilityLabel}</span>
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
            <span>{workbenchCopy.sections.taskLedger}</span>
            <select className="pv2-select" id="ra-task-select" value={selectedTaskId} onChange={(event) => setSelectedTaskId(event.target.value)}>
              <option value="">{workbenchCopy.sections.selectTaskOption}</option>
              {tasks.map((task) => <option key={task.task_id} value={task.task_id}>{task.title}</option>)}
            </select>
          </label>
          <label className="pv2-field" htmlFor="ra-proposal-title" style={{ marginTop: 12 }}><span>{workbenchCopy.sections.proposalTitle}</span><input className="pv2-input" id="ra-proposal-title" value={proposalTitle} onChange={(event) => setProposalTitle(event.target.value)} /></label>
          <label className="pv2-field" htmlFor="ra-proposal-summary" style={{ marginTop: 12 }}><span>{workbenchCopy.sections.proposalSummary}</span><input className="pv2-input" id="ra-proposal-summary" value={proposalSummary} onChange={(event) => setProposalSummary(event.target.value)} /></label>
          <label className="pv2-field" htmlFor="ra-action-payload" style={{ marginTop: 12 }}>
            <span>{workbenchCopy.sections.inputJson}</span>
            <textarea className="pv2-textarea" id="ra-action-payload" value={payloadText} onChange={(event) => setPayloadText(event.target.value)} />
          </label>
          {!parsedPayload ? <span className="pv2-error-meta">{workbenchCopy.sections.invalidProposalJson}</span> : null}
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button-primary" type="button" onClick={() => void runAction("propose")} disabled={Boolean(disabled("propose"))}>{busy === "propose" ? workbenchCopy.sections.creating : workbenchCopy.sections.createProposal}</button>
            <button className="pv2-button-ghost" type="button" onClick={() => void load()} disabled={loading}>{loading ? workbenchCopy.sections.loading : workbenchCopy.sections.refresh}</button>
          </div>
          {disabled("propose") ? <p className="pv2-help">{disabled("propose")}</p> : <p className="pv2-help">{workbenchCopy.sections.postCreateHelp}</p>}
          {selectedCapability ? <DetailDrawer title={workbenchCopy.sections.capabilityDrawer} data={selectedCapability} /> : <EmptyState title={workbenchCopy.sections.noCapability} />}
        </SectionCard>

        <SectionCard title={workbenchCopy.sections.executionStatusTitle} eyebrow={workbenchCopy.sections.executionStatusEyebrow}>
          <label className="pv2-field" htmlFor="ra-action-select">
            <span>{workbenchCopy.sections.selectActionProposal}</span>
            <select className="pv2-select" id="ra-action-select" value={selectedActionId} onChange={(event) => { setSelectedActionId(event.target.value); setExecuteResult(null); setActionEvents(null); }}>
              <option value="">{workbenchCopy.sections.selectProposalOption}</option>
              {actions.map((action) => <option key={action.action_proposal_id} value={action.action_proposal_id}>{action.title} / {action.status}</option>)}
            </select>
          </label>
          {selectedAction ? (
            <div className="pv2-readable-panel" style={{ marginTop: 12 }}>
              <div className="pv2-readable-table">
                <div className="pv2-readable-row"><div className="pv2-readable-key">{workbenchCopy.sections.status}</div><div className="pv2-readable-value"><StatusBadge status={selectedAction.status} /></div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">Capability</div><div className="pv2-readable-value"><span className="pv2-mono">{display(selectedAction.capability_key)}</span></div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">Plan digest</div><div className="pv2-readable-value"><span className="pv2-mono">{display(selectedAction.plan_digest)}</span></div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">{workbenchCopy.sections.updatedAt}</div><div className="pv2-readable-value">{formatDateTime(selectedAction.updated_at)}</div></div>
              </div>
            </div>
          ) : <EmptyState title={workbenchCopy.sections.noProposalSelected} />}
          <label className="pv2-field" htmlFor="ra-confirm-text" style={{ marginTop: 12 }}>
            <span>{workbenchCopy.sections.confirmationText}</span>
            <input className="pv2-input" id="ra-confirm-text" value={confirmation} onChange={(event) => setConfirmation(event.target.value)} />
          </label>
          <label className="pv2-field" htmlFor="ra-approval-text" style={{ marginTop: 12 }}>
            <span>{workbenchCopy.sections.approvalConfirmationText}</span>
            <input className="pv2-input" id="ra-approval-text" value={approvalConfirmation} onChange={(event) => setApprovalConfirmation(event.target.value)} />
          </label>
          <div className="ra-gate-grid" style={{ marginTop: 12 }}>
            {(["confirm", "preflight", "approve", "execute"] as ExecutionStep[]).map((step) => {
              const reason = disabled(step);
              const labels: Record<ExecutionStep, string> = workbenchCopy.gateLabels;
              return (
                <div className="ra-gate-card" key={step}>
                  <strong>{labels[step]}</strong>
                  <p>{reason || workbenchCopy.sections.canRunNext}</p>
                  <button className={step === "execute" ? "pv2-button-primary" : "pv2-button-ghost"} type="button" disabled={Boolean(reason)} onClick={() => void runAction(step)}>{busy === step ? workbenchCopy.sections.loading : labels[step]}</button>
                </div>
              );
            })}
          </div>
        </SectionCard>
      </div>

      <div className="ra-two-column">
        <SectionCard title={workbenchCopy.sections.preflightResultTitle} eyebrow={workbenchCopy.sections.preflightResultEyebrow}>
          {preflightSummary ? (
            <div className="pv2-readable-panel">
              <div className="pv2-readable-table">
                <div className="pv2-readable-row"><div className="pv2-readable-key">Passed</div><div className="pv2-readable-value"><StatusBadge status={preflightSummary.passed ? "passed" : "blocked"} /></div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">{workbenchCopy.sections.approval}</div><div className="pv2-readable-value">{preflightSummary.approvalRequired ? workbenchCopy.sections.approvalRequired : workbenchCopy.sections.approvalNotRequired}</div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">Failed checks</div><div className="pv2-readable-value">{preflightSummary.failedChecks.length ? preflightSummary.failedChecks.map(display).join(" / ") : "-"}</div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">Tool event</div><div className="pv2-readable-value"><span className="pv2-mono">{display(preflightSummary.toolEvent)}</span></div></div>
              </div>
            </div>
          ) : <EmptyState title={workbenchCopy.sections.waitingPreflightTitle} hint={workbenchCopy.sections.waitingPreflightHint} />}
          <HumanResultCard result={executeResult} />
          {summarizeActionEvents(actionEvents).length ? (
            <div className="pv2-chip-row" style={{ marginTop: 12 }}>
              {summarizeActionEvents(actionEvents).map(([key, value]) => <span className="pv2-chip" key={key}>{key}: {display(value)}</span>)}
            </div>
          ) : null}
          {preflight ? <DiagnosticLogBlock title={workbenchCopy.sections.debugPreflightPayload} data={preflight} testId="ra-workbench-preflight-log" /> : null}
          {executeResult ? <DiagnosticLogBlock title={workbenchCopy.sections.debugExecutePayload} data={executeResult} testId="ra-workbench-execute-log" /> : null}
          {actionEvents ? <DiagnosticLogBlock title={workbenchCopy.sections.auditEventPayload} data={actionEvents} testId="ra-workbench-audit-log" /> : null}
        </SectionCard>

        <SectionCard title={workbenchCopy.sections.legacyTitle} eyebrow={workbenchCopy.sections.legacyEyebrow}>
          <ApiErrorBox error={legacyError} title={workbenchCopy.sections.legacyErrorTitle} />
          <label className="pv2-field" htmlFor="ra-tool-select">
            <span>{workbenchCopy.sections.selectMcpTool}</span>
            <select className="pv2-select" id="ra-tool-select" value={selectedTool?.tool_id || ""} onChange={(event) => setSelectedTool(tools.find((tool) => tool.tool_id === event.target.value) || null)}>
              {tools.map((tool) => <option key={tool.tool_id} value={tool.tool_id}>{tool.server_key} / {tool.tool_name}</option>)}
            </select>
          </label>
          <label className="pv2-field" htmlFor="ra-legacy-payload" style={{ marginTop: 12 }}>
            <span>Dry-run JSON</span>
            <textarea className="pv2-textarea" id="ra-legacy-payload" value={legacyPayloadText} onChange={(event) => setLegacyPayloadText(event.target.value)} />
          </label>
          <div className="pv2-row-actions" style={{ marginTop: 12 }}>
            <button className="pv2-button-ghost" type="button" onClick={() => void runLegacyPreflight()} disabled={!selectedTool || !legacyParsedPayload || legacyBusy !== null}>{legacyBusy === "preflight" ? workbenchCopy.sections.preflightRunning : workbenchCopy.sections.executePreflight}</button>
            <button className="pv2-button-ghost" type="button" onClick={() => void runLegacyDryRun()} disabled={!selectedTool || !legacyParsedPayload || legacyBusy !== null}>{legacyBusy === "dry_run" ? workbenchCopy.sections.dryRunRunning : workbenchCopy.sections.executeDryRun}</button>
          </div>
          {!legacyParsedPayload ? <span className="pv2-error-meta">{workbenchCopy.sections.invalidExecutionJson}</span> : null}
          {dryRunResult ? <DiagnosticLogBlock title="dry-run / preflight debug payload" data={dryRunResult} testId="ra-workbench-dry-run-log" /> : <EmptyState title={workbenchCopy.sections.waitingDryRunTitle} hint={workbenchCopy.sections.waitingDryRunHint} />}
        </SectionCard>
      </div>

      <SectionCard title="本地数据工具目录" eyebrow="real catalog / readable cards">
        {localDataTools.length ? (
          <div className="pv2-readable-list" data-testid="ra-local-data-tool-cards">
            {localDataTools.map((tool) => {
              const phase = localDataToolPhase(tool.tool_name);
              return (
                <div className="pv2-readable-item" key={tool.tool_id}>
                  <strong>{localDataToolTitle(tool)}</strong>
                  <p className="pv2-muted">{phase?.description || tool.description || "本地数据管理工具，具体入参和 trace 保留在审计详情中。"}</p>
                  <span className="pv2-chip">{phase?.title || "未分配阶段"}</span>
                  <span className="pv2-chip">{localDataRiskLabel(tool.risk_level || phase?.riskLevel)}</span>
                  <span className="pv2-chip">{tool.requires_approval || phase?.requiresConfirmation ? "需要确认" : "无需确认"}</span>
                </div>
              );
            })}
          </div>
        ) : (
          <EmptyState title="尚未读取到 local_data 工具" hint="请等待后端 Capability Registry / MCP Catalog 写入真实目录；页面不会用静态假工具冒充可执行能力。" />
        )}
      </SectionCard>

      <SectionCard title={workbenchCopy.sections.catalogTitle} eyebrow={workbenchCopy.sections.catalogEyebrow}>
        <PaperTable
          rows={capabilities}
          empty={workbenchCopy.sections.emptyCapabilities}
          columns={[
            { key: "capability", header: workbenchCopy.sections.capabilityColumn, render: (row) => <><span className="ra-title">{row.title || row.capability_key}</span><br /><span className="pv2-muted pv2-mono">{row.capability_key}</span></> },
            { key: "risk", header: workbenchCopy.sections.riskColumn, render: (row) => <StatusBadge status={row.risk_level} /> },
            { key: "effect", header: workbenchCopy.sections.effectColumn, render: (row) => <StatusBadge status={row.side_effect_level} /> },
            { key: "status", header: workbenchCopy.sections.statusColumn, render: (row) => <StatusBadge status={row.status} /> },
          ]}
        />
      </SectionCard>
    </main>
  );
}
