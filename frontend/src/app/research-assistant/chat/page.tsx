"use client";

import {
  AssistantRuntimeProvider,
  ComposerPrimitive,
  MessagePartPrimitive,
  MessagePrimitive,
  ThreadPrimitive,
  useLocalRuntime,
  useMessage,
  type ChatModelAdapter,
  type ChatModelRunOptions,
  type ThreadMessageLike,
} from "@assistant-ui/react";
import Link from "next/link";
import { useCallback, useMemo, useState } from "react";

import {
  LOCAL_DATA_MANAGEMENT_CAPABILITY,
  LOCAL_DATA_MANAGEMENT_PHASES,
  ResearchAssistantApiError,
  localDataRiskLabel,
  researchAssistantApi,
  type AssistantCatalogReadiness,
  type AssistantChatTurnResult,
  type LocalDataPhase,
  type LocalDataPhaseKey,
} from "@/lib/research-assistant/api";
import uiCopy from "@/lib/research-assistant/ui-copy";

type RailStep = { label: string; status: string };
type PlanCard = { title?: string; steps?: string[] };
type ClarificationCard = { title?: string; questions?: string[] };
type Proposal = { title?: string; risk?: string; approval_required?: boolean; status?: string };
type LocalDataPhaseCard = { key?: string; phase?: string; title?: string; label?: string; status?: string; description?: string };
type McpRouteDecision = {
  domain?: string;
  server_key?: string | null;
  tool_name?: string | null;
  reason?: string;
  policy?: string;
  side_effect?: string;
  summary_first?: boolean;
  preflight_required?: boolean;
  confirmation_required?: boolean;
};
type ContextHealth = {
  status?: string;
  utilization_ratio?: number;
  compact_summary_count?: number;
  key_fact_count?: number;
  show_badge?: boolean;
};

type ChatCards = {
  dialogue_mode?: string;
  mode_decision?: Record<string, unknown>;
  plan_card?: PlanCard;
  clarification_card?: ClarificationCard;
  action_proposals?: Proposal[];
  status_rail?: RailStep[];
  capability_summary?: Record<string, unknown>;
  local_data_management?: Record<string, unknown>;
  local_data_card?: Record<string, unknown>;
  local_data_phases?: LocalDataPhaseCard[];
  mcp_route_decision?: McpRouteDecision;
  safety?: Record<string, unknown>;
  context_health?: ContextHealth;
  ui_display?: {
    show_plan_card?: boolean;
    show_clarification_card?: boolean;
    show_context_health_badge?: boolean;
    details_default_collapsed?: boolean;
  };
};

type CatalogNotReadyDetail = {
  code?: string;
  message?: string;
  operator_action?: string | null;
  readiness?: AssistantCatalogReadiness;
};

const chatCopy = uiCopy.chat;
const initialSteps: RailStep[] = chatCopy.initialSteps.map((step) => ({ ...step }));
const thinkingSteps: RailStep[] = chatCopy.thinkingSteps.map((step) => ({ ...step }));

const welcomeMessages: ThreadMessageLike[] = [
  {
    role: "assistant",
    content: chatCopy.welcomeMessages.map((text) => ({ type: "text", text })),
  },
];

function textFromOptions(options: ChatModelRunOptions): string {
  const last = [...options.messages].reverse().find((message) => message.role === "user");
  if (!last) return "";
  return last.content
    .map((part) => (part.type === "text" ? part.text : ""))
    .join("\n")
    .trim();
}

function asCards(value: unknown): ChatCards {
  if (!value || typeof value !== "object") return {};
  return value as ChatCards;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

function routeDecision(cards: ChatCards): McpRouteDecision | null {
  const route = asRecord(cards.mcp_route_decision);
  if (!route?.server_key || !route?.tool_name) return null;
  return route as McpRouteDecision;
}

function stripAssistantToolChoiceMarkup(text: string, route: McpRouteDecision | null): string {
  if (!text.toLowerCase().includes("<assistant_tool_choice")) return text;
  const cleaned = text.replace(/<assistant_tool_choice\b[^>]*>[\s\S]*?<\/assistant_tool_choice>/gi, "").trim();
  const prefix = route
    ? `我已完成 MCP route decision：${route.domain || "mcp"} -> ${route.server_key}/${route.tool_name}。确认前只展示 preflight、计划和 summary-first 结果，不直接执行写操作。`
    : "我已识别到需要工具选择，会先展示 route decision、preflight 和确认边界，不直接执行写操作。";
  return [prefix, cleaned].filter(Boolean).join("\n\n");
}

function catalogNotReadyDetail(error: unknown): CatalogNotReadyDetail | null {
  if (!(error instanceof ResearchAssistantApiError)) return null;
  const raw = asRecord(error.raw);
  const detail = asRecord(raw?.detail);
  if (detail?.code !== "research_assistant_catalog_not_ready") return null;
  return detail as CatalogNotReadyDetail;
}

function catalogSetupReply(detail: CatalogNotReadyDetail): string {
  const readiness = detail.readiness;
  const missing = readiness?.checks
    ?.filter((check) => !check.ready)
    .map((check) => `${check.label} ${chatCopy.catalogCard.checkCurrent} ${check.present}/${check.expected_min}`)
    .join("；");
  return [
    chatCopy.catalogSetupReply.notReady,
    missing ? `${chatCopy.catalogSetupReply.missingPrefix}${missing}。` : chatCopy.catalogSetupReply.missingFallback,
    chatCopy.catalogSetupReply.nextStep,
  ].join("\n");
}

function statusText(status: string): string {
  return chatCopy.statusText[status as keyof typeof chatCopy.statusText] || chatCopy.statusText.default;
}

function proposalStatusText(status?: string): string {
  if (!status) return chatCopy.proposalStatusText.default;
  return chatCopy.proposalStatusText[status as keyof typeof chatCopy.proposalStatusText] || status;
}


function phaseRecordStatus(records: LocalDataPhaseCard[], phase: LocalDataPhase): string | null {
  const matched = records.find((record) => {
    const key = String(record.key || record.phase || "").toLowerCase();
    const title = String(record.title || record.label || "");
    return key === phase.key || key === phase.shortTitle || title.includes(phase.shortTitle) || title.includes(phase.title);
  });
  return matched?.status || null;
}

function localDataPhaseRows(cards: ChatCards, hasLatest: boolean): Array<LocalDataPhase & { status: string }> {
  const localDataCard = asRecord(cards.local_data_management) || asRecord(cards.local_data_card) || {};
  const explicitRecords = [
    ...(Array.isArray(cards.local_data_phases) ? cards.local_data_phases : []),
    ...(Array.isArray(localDataCard.phases) ? (localDataCard.phases as LocalDataPhaseCard[]) : []),
    ...(Array.isArray(localDataCard.stage_statuses) ? (localDataCard.stage_statuses as LocalDataPhaseCard[]) : []),
  ];
  const fallback: Record<LocalDataPhaseKey, string> = hasLatest
    ? { check: "done", plan: "done", confirm: "current", execute: "locked", review: "locked" }
    : { check: "idle", plan: "idle", confirm: "locked", execute: "locked", review: "locked" };

  return LOCAL_DATA_MANAGEMENT_PHASES.map((phase) => ({
    ...phase,
    status: phaseRecordStatus(explicitRecords, phase) || fallback[phase.key],
  }));
}

function hasLocalDataContext(cards: ChatCards): boolean {
  const capability = cards.capability_summary || {};
  const promptBranches = Array.isArray(capability.prompt_branches) ? capability.prompt_branches : [];
  return Boolean(
    cards.local_data_management ||
      cards.local_data_card ||
      cards.local_data_phases?.length ||
      capability.local_data_management ||
      String(capability.mcp || "").includes("aistock-local-data") ||
      promptBranches.some((item) => String(item).includes("local_data")),
  );
}

function localDataCapabilityText(capability: Record<string, unknown>): string {
  return String(
    capability.local_data_management ||
      capability.local_data ||
      `${LOCAL_DATA_MANAGEMENT_CAPABILITY.displayName} 按检查、计划、确认、执行、复查闭环处理，确认前不会启动数据任务。`,
  );
}

function shouldShowSideDetails(latest: AssistantChatTurnResult | null, cards: ChatCards): boolean {
  if (!latest) return true;
  const showPlan = cards.ui_display?.show_plan_card !== false;
  const showClarify = cards.ui_display?.show_clarification_card !== false;
  const hasPlan = showPlan && Boolean(cards.plan_card?.title || cards.plan_card?.steps?.length);
  const hasClarification = showClarify && Boolean(cards.clarification_card?.questions?.length);
  const hasProposal = Boolean(cards.action_proposals?.length);
  return hasPlan || hasClarification || hasProposal || Boolean(routeDecision(cards)) || hasLocalDataContext(cards);
}

function createAdapter(
  onTurn: (result: AssistantChatTurnResult) => void,
  onStage: (steps: RailStep[]) => void,
  onCatalogIssue: (detail: CatalogNotReadyDetail | null) => void,
  conversationId: string | null,
  setConversationId: (id: string) => void,
): ChatModelAdapter {
  return {
    async run(options) {
      const message = textFromOptions(options);
      if (!message) {
        return { content: [{ type: "text", text: chatCopy.emptyInputReply }] };
      }
      onStage(thinkingSteps);
      let result: AssistantChatTurnResult;
      const payload: Record<string, unknown> = { message, phase: "planning", risk_level: "medium", allow_execute: false };
      if (conversationId) payload.conversation_id = conversationId;
      try {
        result = await researchAssistantApi.chatTurn(payload);
      } catch (error) {
        const detail = catalogNotReadyDetail(error);
        if (detail) {
          onCatalogIssue(detail);
          onStage(initialSteps);
          return { content: [{ type: "text", text: catalogSetupReply(detail) }] };
        }
        throw error;
      }
      onCatalogIssue(null);
      const newConversationId = (result.conversation as Record<string, unknown> | null)?.conversation_id as string | undefined;
      if (newConversationId && !conversationId) setConversationId(newConversationId);
      onTurn(result);
      const cards = asCards(result.cards || result.assistant_message?.content_json?.cards);
      if (cards.status_rail?.length) onStage(cards.status_rail);
      const reply = stripAssistantToolChoiceMarkup(result.assistant_message?.content_text || chatCopy.fallbackReply, routeDecision(cards));
      return { content: [{ type: "text", text: reply }] };
    },
  };
}

function TaskProgressRail({ steps, latest }: { steps: RailStep[]; latest: AssistantChatTurnResult | null }) {
  const cards = asCards(latest?.cards || latest?.assistant_message?.content_json?.cards);
  const capability = cards.capability_summary || {};
  const contextHealth = cards.context_health;
  const showLocalData = hasLocalDataContext(cards);
  const localDataRows = localDataPhaseRows(cards, Boolean(latest));
  return (
    <aside className="ra-chat-rail" aria-label={chatCopy.rail.ariaLabel}>
      <div className="ra-chat-rail-head">
        <span className="ra-chat-eyebrow">{chatCopy.rail.eyebrow}</span>
        <h2>{chatCopy.rail.title}</h2>
        <p>{chatCopy.rail.body}</p>
      </div>
      <ol className="ra-chat-steps">
        {steps.map((step) => (
          <li className={`ra-chat-step ra-chat-step-${step.status}`} key={step.label}>
            <span className="ra-chat-step-dot" aria-hidden="true" />
            <span className="ra-chat-step-body">
              <strong>{step.label}</strong>
              <small>{statusText(step.status)}</small>
            </span>
          </li>
        ))}
      </ol>
      <div className="ra-chat-capability">
        <span className="ra-chat-eyebrow">{chatCopy.rail.capabilityEyebrow}</span>
        <p>{String(capability.mcp || chatCopy.rail.defaultMcp)}</p>
        <p>{String(capability.skill || chatCopy.rail.defaultSkill)}</p>
        <p>{String(capability.model || chatCopy.rail.defaultModel)}</p>
      </div>
      {showLocalData ? (
        <div className="ra-chat-capability" data-testid="ra-local-data-phase-card" style={{ marginTop: 12 }}>
          <span className="ra-chat-eyebrow">local_data_management</span>
          <strong>{LOCAL_DATA_MANAGEMENT_CAPABILITY.displayName}闭环</strong>
          <p>{localDataCapabilityText(capability)}</p>
          {localDataRows.map((phase) => (
            <p key={phase.key}>
              {phase.shortTitle}：{statusText(phase.status)} · {localDataRiskLabel(phase.riskLevel)}
            </p>
          ))}
        </div>
      ) : null}
      {contextHealth?.show_badge ? (
        <div className="ra-chat-capability">
          <span className="ra-chat-eyebrow">{chatCopy.rail.contextEyebrow}</span>
          <p>
            {chatCopy.rail.contextStatusPrefix}{contextHealth.status || "healthy"}；{chatCopy.rail.contextWindowUsage}{Math.round((contextHealth.utilization_ratio || 0) * 100)}%。
          </p>
          <p>
            {chatCopy.rail.contextSummaryPrefix} {contextHealth.compact_summary_count || 0} {chatCopy.rail.contextSummaryUnit}，{chatCopy.rail.contextKeyFactPrefix} {contextHealth.key_fact_count || 0} {chatCopy.rail.contextKeyFactSuffix}
          </p>
        </div>
      ) : null}
      <Link className="ra-chat-admin-link" href="/research-assistant/admin">{chatCopy.rail.adminLink}</Link>
    </aside>
  );
}

function AssistantMessageText() {
  return <MessagePartPrimitive.Text component="p" className="ra-chat-message-text" smooth={false} />;
}

function ChatMessage() {
  const role = useMessage((state) => state.role);
  return (
    <MessagePrimitive.Root className={`ra-chat-message ra-chat-message-${role}`}>
      <div className="ra-chat-avatar" aria-hidden="true">{role === "user" ? chatCopy.avatar.user : chatCopy.avatar.assistant}</div>
      <div className="ra-chat-bubble">
        <MessagePrimitive.Parts components={{ Text: AssistantMessageText }} />
      </div>
    </MessagePrimitive.Root>
  );
}

function PlanSummary({ latest }: { latest: AssistantChatTurnResult | null }) {
  const cards = asCards(latest?.cards || latest?.assistant_message?.content_json?.cards);
  if (!shouldShowSideDetails(latest, cards)) return null;
  const plan = cards.ui_display?.show_plan_card === false ? undefined : cards.plan_card;
  const clarify = cards.ui_display?.show_clarification_card === false ? undefined : cards.clarification_card;
  const proposals = cards.action_proposals || [];
  const route = routeDecision(cards);
  const showLocalData = hasLocalDataContext(cards);
  const localDataRows = localDataPhaseRows(cards, Boolean(latest));
  if (!latest) {
    return (
      <section className="ra-chat-card ra-chat-card-welcome">
        <span className="ra-chat-eyebrow">{chatCopy.planSummary.welcomeEyebrow}</span>
        <h2>{chatCopy.planSummary.welcomeTitle}</h2>
        <p>{chatCopy.planSummary.welcomeExample}</p>
        <div className="ra-chat-confirm-card">
          <strong>也可以直接说：检查本地数据同步情况并生成修复计划</strong>
          <p>助理会按“检查、计划、确认、执行、复查”展示中文卡片，确认前不启动同步任务或修复任务。</p>
        </div>
      </section>
    );
  }
  return (
    <section className="ra-chat-card" data-testid="ra-chat-plan-card">
      <span className="ra-chat-eyebrow">{chatCopy.planSummary.detailEyebrow}</span>
      <h2>{plan?.title || chatCopy.planSummary.detailTitle}</h2>
      {plan?.steps?.length ? (
        <ul>
          {plan.steps.map((step) => <li key={step}>{step}</li>)}
        </ul>
      ) : null}
      {clarify?.questions?.length ? (
        <div className="ra-chat-confirm-card" data-testid="ra-chat-confirm-card">
          <strong>{clarify.title || chatCopy.planSummary.clarificationTitle}</strong>
          {clarify.questions.map((question) => <p key={question}>{question}</p>)}
        </div>
      ) : null}
      {proposals.length ? (
        <div className="ra-chat-proposals">
          {proposals.map((proposal) => (
            <span className="ra-chat-proposal" key={`${proposal.title}-${proposal.status}`}>
              {proposal.title}{chatCopy.planSummary.proposalSeparator}{proposalStatusText(proposal.status)}
            </span>
          ))}
        </div>
      ) : null}
      {route ? (
        <div className="ra-chat-confirm-card" data-testid="ra-mcp-route-card">
          <strong>MCP route decision</strong>
          <p>{route.domain || "mcp"} -> {route.server_key}/{route.tool_name}</p>
          <p>{route.summary_first ? "summary-first：列表只展示概要，详情按需展开。" : "按工具返回结果展示。"}</p>
          <p>{route.confirmation_required ? "需要确认和审批后才可执行。" : route.preflight_required ? "先执行 preflight/计划，不直接写入。" : "只读查询，不执行写操作。"}</p>
          {route.reason ? <p>{route.reason}</p> : null}
        </div>
      ) : null}
      {showLocalData ? (
        <div className="ra-chat-confirm-card" data-testid="ra-local-data-plan-card">
          <strong>本地数据管理执行阶段</strong>
          {localDataRows.map((phase) => (
            <p key={phase.key}>
              {phase.title}：{statusText(phase.status)}。{phase.description}
            </p>
          ))}
        </div>
      ) : null}
    </section>
  );
}

function CatalogSetupCard({
  detail,
  initializing,
  initMessage,
  onInitialize,
}: {
  detail: CatalogNotReadyDetail;
  initializing: boolean;
  initMessage: string | null;
  onInitialize: () => void;
}) {
  const readiness = detail.readiness;
  const ready = readiness?.ready === true;
  const missingChecks = readiness?.checks?.filter((check) => !check.ready) || [];
  return (
    <section className="ra-chat-card ra-chat-card-setup" data-testid="ra-chat-catalog-setup">
      <span className="ra-chat-eyebrow">{ready ? chatCopy.catalogCard.readyEyebrow : chatCopy.catalogCard.notReadyEyebrow}</span>
      <h2>{ready ? chatCopy.catalogCard.readyTitle : chatCopy.catalogCard.notReadyTitle}</h2>
      <p>{ready ? chatCopy.catalogCard.readyBody : chatCopy.catalogCard.notReadyBody}</p>
      {!ready && missingChecks.length ? (
        <ul>
          {missingChecks.map((check) => (
            <li key={check.catalog}>
              {check.label}：{chatCopy.catalogCard.checkCurrent} {check.present} / {chatCopy.catalogCard.checkExpected} {check.expected_min}
            </li>
          ))}
        </ul>
      ) : null}
      {!ready ? (
        <button className="ra-chat-setup-button" type="button" onClick={onInitialize} disabled={initializing}>
          {initializing ? chatCopy.catalogCard.initializing : chatCopy.catalogCard.initialize}
        </button>
      ) : null}
      {initMessage ? <p className="ra-chat-setup-result">{initMessage}</p> : null}
    </section>
  );
}

function AssistantThread() {
  return (
    <ThreadPrimitive.Root className="ra-chat-thread-root">
      <ThreadPrimitive.Viewport className="ra-chat-viewport">
        <ThreadPrimitive.Messages components={{ Message: ChatMessage }} />
        <ThreadPrimitive.ViewportFooter />
      </ThreadPrimitive.Viewport>
      <ComposerPrimitive.Root className="ra-chat-composer">
        <ComposerPrimitive.Input
          className="ra-chat-input"
          placeholder={chatCopy.composer.placeholder}
          submitMode="enter"
          rows={2}
        />
        <ComposerPrimitive.Send className="ra-chat-send">{chatCopy.composer.send}</ComposerPrimitive.Send>
      </ComposerPrimitive.Root>
    </ThreadPrimitive.Root>
  );
}

export default function ResearchAssistantChatPage() {
  const [latest, setLatest] = useState<AssistantChatTurnResult | null>(null);
  const [steps, setSteps] = useState<RailStep[]>(initialSteps);
  const [catalogIssue, setCatalogIssue] = useState<CatalogNotReadyDetail | null>(null);
  const [initializingCatalogs, setInitializingCatalogs] = useState(false);
  const [catalogInitMessage, setCatalogInitMessage] = useState<string | null>(null);
  const [conversationId, setConversationId] = useState<string | null>(null);

  const newConversation = useCallback(() => {
    setConversationId(null);
    setLatest(null);
    setSteps(initialSteps);
    setCatalogIssue(null);
    setCatalogInitMessage(null);
  }, []);

  const initializeCatalogs = useCallback(async () => {
    setInitializingCatalogs(true);
    setCatalogInitMessage(null);
    try {
      await researchAssistantApi.seedCatalogs();
      const readiness = await researchAssistantApi.catalogReadiness();
      if (readiness.ready) {
        setCatalogIssue({ code: "research_assistant_catalog_ready", readiness });
        setCatalogInitMessage(chatCopy.catalogCard.initDone);
      } else {
        setCatalogIssue({ code: "research_assistant_catalog_not_ready", readiness });
        setCatalogInitMessage(chatCopy.catalogCard.initIncomplete);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : chatCopy.catalogCard.initFailed;
      setCatalogInitMessage(message);
    } finally {
      setInitializingCatalogs(false);
    }
  }, []);

  const adapter = useMemo(() => createAdapter(setLatest, setSteps, setCatalogIssue, conversationId, setConversationId), [conversationId]);
  const runtime = useLocalRuntime(adapter, { initialMessages: welcomeMessages });

  return (
    <main className="ra-chat-shell" data-testid="ra-chat-main">
      <TaskProgressRail steps={steps} latest={latest} />
      <section className="ra-chat-main-panel">
        <div className="ra-chat-hero">
          <span className="ra-chat-eyebrow">{chatCopy.hero.eyebrow}</span>
          <h1>{chatCopy.hero.title}</h1>
          <p>{chatCopy.hero.body}</p>
          {conversationId ? (
            <button className="ra-chat-new-session-button" type="button" onClick={newConversation}>
              {chatCopy.hero.newConversation}
            </button>
          ) : null}
        </div>
        <AssistantRuntimeProvider runtime={runtime}>
          <AssistantThread />
        </AssistantRuntimeProvider>
      </section>
      {catalogIssue ? (
        <CatalogSetupCard
          detail={catalogIssue}
          initializing={initializingCatalogs}
          initMessage={catalogInitMessage}
          onInitialize={initializeCatalogs}
        />
      ) : (
        <PlanSummary latest={latest} />
      )}
    </main>
  );
}
