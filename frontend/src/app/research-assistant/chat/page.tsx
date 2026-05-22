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
  ResearchAssistantApiError,
  researchAssistantApi,
  type AssistantCatalogReadiness,
  type AssistantChatTurnResult,
} from "@/lib/research-assistant/api";

type RailStep = { label: string; status: string };
type PlanCard = { title?: string; steps?: string[] };
type ClarificationCard = { title?: string; questions?: string[] };
type Proposal = { title?: string; risk?: string; approval_required?: boolean; status?: string };

type ChatCards = {
  plan_card?: PlanCard;
  clarification_card?: ClarificationCard;
  action_proposals?: Proposal[];
  status_rail?: RailStep[];
  capability_summary?: Record<string, unknown>;
  safety?: Record<string, unknown>;
};

type CatalogNotReadyDetail = {
  code?: string;
  message?: string;
  operator_action?: string | null;
  readiness?: AssistantCatalogReadiness;
};

const initialSteps: RailStep[] = [
  { label: "接收需求", status: "idle" },
  { label: "选择提示词", status: "idle" },
  { label: "构建上下文", status: "idle" },
  { label: "等待确认", status: "idle" },
  { label: "MCP 预检查", status: "locked" },
  { label: "执行", status: "locked" },
  { label: "写入记忆", status: "locked" },
];

const thinkingSteps: RailStep[] = [
  { label: "接收需求", status: "current" },
  { label: "选择提示词", status: "idle" },
  { label: "构建上下文", status: "idle" },
  { label: "等待确认", status: "idle" },
  { label: "MCP 预检查", status: "locked" },
  { label: "执行", status: "locked" },
  { label: "写入记忆", status: "locked" },
];

const welcomeMessages: ThreadMessageLike[] = [
  {
    role: "assistant",
    content: [
      {
        type: "text",
        text: "你好，我是 AIstock 研究助理。你可以直接描述研究或实验目标，例如创建 QE 实验、分析 HMM 演进、复盘因子研发或整理今天需要关注的事项。我会先理解并确认，不会在确认前执行高风险 MCP。",
      },
    ],
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
    .map((check) => `${check.label} 当前 ${check.present}/${check.expected_min}`)
    .join("；");
  return [
    "助理目录尚未初始化完整，所以我还不能安全地理解和执行这次对话。",
    missing ? `缺少的目录：${missing}。` : "缺少 Prompt Tree、MCP、Skill 或模型路由目录。",
    "请点击右侧初始化按钮；初始化完成后，再重新发送你的研究或实验目标。",
  ].join("\n");
}

function statusText(status: string): string {
  if (status === "done") return "已完成";
  if (status === "current") return "进行中";
  if (status === "locked") return "未解锁";
  if (status === "failed") return "失败";
  return "等待";
}

function proposalStatusText(status?: string): string {
  if (status === "waiting_confirmation") return "等待确认";
  if (status === "draft_only") return "仅生成草稿";
  if (status === "ready") return "可继续讨论";
  return status || "待处理";
}

function cardText(result: AssistantChatTurnResult): string {
  const cards = asCards(result.cards || result.assistant_message?.content_json?.cards);
  const parts: string[] = [];
  const plan = cards.plan_card;
  if (plan?.steps?.length) {
    parts.push(`\n计划：${plan.title || "下一步计划"}`);
    parts.push(...plan.steps.map((step, index) => `${index + 1}. ${step}`));
  }
  const clarify = cards.clarification_card;
  if (clarify?.questions?.length) {
    parts.push(`\n需要确认：${clarify.title || "关键问题"}`);
    parts.push(...clarify.questions.map((question, index) => `${index + 1}. ${question}`));
  }
  const proposals = cards.action_proposals || [];
  if (proposals.length) {
    parts.push("\n可选动作：");
    parts.push(...proposals.map((proposal) => `- ${proposal.title || "待命名动作"}；风险：${proposal.risk || "未标注"}；状态：${proposalStatusText(proposal.status)}`));
  }
  parts.push("\n安全边界：本轮只完成理解、计划和确认，不会执行 QE materialize/run 或其他高风险 MCP。确认后才进入预检查和执行。");
  return parts.join("\n");
}

function createAdapter(
  onTurn: (result: AssistantChatTurnResult) => void,
  onStage: (steps: RailStep[]) => void,
  onCatalogIssue: (detail: CatalogNotReadyDetail | null) => void,
): ChatModelAdapter {
  return {
    async run(options) {
      const message = textFromOptions(options);
      if (!message) {
        return { content: [{ type: "text", text: "请直接告诉我你要完成的研究或实验目标，我会先理解并向你确认。" }] };
      }
      onStage(thinkingSteps);
      let result: AssistantChatTurnResult;
      try {
        result = await researchAssistantApi.chatTurn({ message, phase: "planning", risk_level: "medium", allow_execute: false });
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
      onTurn(result);
      const cards = asCards(result.cards || result.assistant_message?.content_json?.cards);
      if (cards.status_rail?.length) onStage(cards.status_rail);
      const reply = `${result.assistant_message?.content_text || "我已理解你的需求，先生成计划并等待确认。"}\n${cardText(result)}`;
      return { content: [{ type: "text", text: reply }] };
    },
  };
}

function TaskProgressRail({ steps, latest }: { steps: RailStep[]; latest: AssistantChatTurnResult | null }) {
  const cards = asCards(latest?.cards || latest?.assistant_message?.content_json?.cards);
  const capability = cards.capability_summary || {};
  return (
    <aside className="ra-chat-rail" aria-label="任务状态轨道">
      <div className="ra-chat-rail-head">
        <span className="ra-chat-eyebrow">实时状态</span>
        <h2>助理正在做什么</h2>
        <p>这里只展示人类可读进度；后台 ID、payload 与 Trace 留在审计页面。</p>
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
        <span className="ra-chat-eyebrow">能力选择</span>
        <p>{String(capability.mcp || "将按需选择 Research Assistant、QE、Validation 等 MCP。")}</p>
        <p>{String(capability.skill || "将按需选择本地 Skill Catalog，不要求用户记住工具名。")}</p>
        <p>{String(capability.model || "主模型负责理解、确认和调度。")}</p>
      </div>
      <Link className="ra-chat-admin-link" href="/research-assistant/admin">打开后台管理 / 审计</Link>
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
      <div className="ra-chat-avatar" aria-hidden="true">{role === "user" ? "我" : "AI"}</div>
      <div className="ra-chat-bubble">
        <MessagePrimitive.Parts components={{ Text: AssistantMessageText }} />
      </div>
    </MessagePrimitive.Root>
  );
}

function PlanSummary({ latest }: { latest: AssistantChatTurnResult | null }) {
  const cards = asCards(latest?.cards || latest?.assistant_message?.content_json?.cards);
  const plan = cards.plan_card;
  const clarify = cards.clarification_card;
  const proposals = cards.action_proposals || [];
  if (!latest) {
    return (
      <section className="ra-chat-card ra-chat-card-welcome">
        <span className="ra-chat-eyebrow">下一步</span>
        <h2>直接输入你的目标</h2>
        <p>示例：帮我创建一个 QE 10 loop 实验，先不要执行。</p>
      </section>
    );
  }
  return (
    <section className="ra-chat-card" data-testid="ra-chat-plan-card">
      <span className="ra-chat-eyebrow">计划卡</span>
      <h2>{plan?.title || "本轮计划"}</h2>
      <ul>
        {(plan?.steps || []).map((step) => <li key={step}>{step}</li>)}
      </ul>
      {clarify?.questions?.length ? (
        <div className="ra-chat-confirm-card" data-testid="ra-chat-confirm-card">
          <strong>{clarify.title || "需要你确认"}</strong>
          {clarify.questions.map((question) => <p key={question}>{question}</p>)}
        </div>
      ) : null}
      {proposals.length ? (
        <div className="ra-chat-proposals">
          {proposals.map((proposal) => (
            <span className="ra-chat-proposal" key={`${proposal.title}-${proposal.status}`}>{proposal.title} · {proposalStatusText(proposal.status)}</span>
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
      <span className="ra-chat-eyebrow">{ready ? "初始化完成" : "需要初始化"}</span>
      <h2>{ready ? "助理目录已准备好" : "助理目录尚未准备好"}</h2>
      <p>{ready ? "Prompt Tree、MCP、Skill 和模型路由目录已经可用。请重新发送你的研究或实验目标。" : "Prompt Tree、MCP、Skill 和模型路由目录必须先写入数据库。完成后，助理才能按设计方案选择提示词分支、模型和工具。"}</p>
      {!ready && missingChecks.length ? (
        <ul>
          {missingChecks.map((check) => (
            <li key={check.catalog}>
              {check.label}：当前 {check.present} / 至少 {check.expected_min}
            </li>
          ))}
        </ul>
      ) : null}
      {!ready ? (
        <button className="ra-chat-setup-button" type="button" onClick={onInitialize} disabled={initializing}>
          {initializing ? "正在初始化目录..." : "初始化助理目录"}
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
          placeholder="直接描述你的研究目标，例如：帮我创建一个 QE 10 loop 实验，先不要执行。"
          submitMode="enter"
          rows={2}
        />
        <ComposerPrimitive.Send className="ra-chat-send">发送</ComposerPrimitive.Send>
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

  const initializeCatalogs = useCallback(async () => {
    setInitializingCatalogs(true);
    setCatalogInitMessage(null);
    try {
      await researchAssistantApi.seedCatalogs();
      const readiness = await researchAssistantApi.catalogReadiness();
      if (readiness.ready) {
        setCatalogIssue({ code: "research_assistant_catalog_ready", readiness });
        setCatalogInitMessage("目录初始化完成。请重新发送你的研究或实验目标。");
      } else {
        setCatalogIssue({ code: "research_assistant_catalog_not_ready", readiness });
        setCatalogInitMessage("目录仍未完整，请查看缺少项后再次初始化。");
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "目录初始化失败，请查看后台日志。";
      setCatalogInitMessage(message);
    } finally {
      setInitializingCatalogs(false);
    }
  }, []);

  const adapter = useMemo(() => createAdapter(setLatest, setSteps, setCatalogIssue), []);
  const runtime = useLocalRuntime(adapter, { initialMessages: welcomeMessages });

  return (
    <main className="ra-chat-shell" data-testid="ra-chat-main">
      <TaskProgressRail steps={steps} latest={latest} />
      <section className="ra-chat-main-panel">
        <div className="ra-chat-hero">
          <span className="ra-chat-eyebrow">AIstock Research Assistant</span>
          <h1>像 Codex 一样对话，由 MCP 安全执行</h1>
          <p>助理会先理解、复述、追问和生成计划卡；确认前不会执行高风险工具。</p>
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
