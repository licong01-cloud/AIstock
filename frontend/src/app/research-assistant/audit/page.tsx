import Link from "next/link";

import { AgentRunsSection } from "./AgentRunsSection";
import { ExternalAgentsSection } from "./ExternalAgentsSection";
import { TasksSection } from "./TasksSection";
import { TraceSection } from "./TraceSection";

type AuditTabKey = "tasks" | "trace" | "agent-runs" | "external-agents";

const AUDIT_TABS: Array<{ key: AuditTabKey; label: string; href: string }> = [
  { key: "tasks", label: "任务", href: "/research-assistant/audit?tab=tasks" },
  { key: "trace", label: "Trace", href: "/research-assistant/audit?tab=trace" },
  { key: "agent-runs", label: "Agent运行", href: "/research-assistant/audit?tab=agent-runs" },
  { key: "external-agents", label: "外部Agent", href: "/research-assistant/audit?tab=external-agents" },
];

function normalizeTab(value: string | string[] | undefined): AuditTabKey {
  const raw = Array.isArray(value) ? value[0] : value;
  if (raw === "trace" || raw === "agent-runs" || raw === "external-agents") return raw;
  return "tasks";
}

function renderAuditSection(tab: AuditTabKey) {
  if (tab === "trace") return <TraceSection />;
  if (tab === "agent-runs") return <AgentRunsSection />;
  if (tab === "external-agents") return <ExternalAgentsSection />;
  return <TasksSection />;
}

export default function ResearchAssistantAuditPage({
  searchParams,
}: {
  searchParams?: { tab?: string | string[] };
}) {
  const activeTab = normalizeTab(searchParams?.tab);
  return (
    <main>
      <section className="ra-mcp-section" aria-labelledby="ra-audit-title">
        <div className="ra-mcp-section-head">
          <div>
            <span className="ra-mcp-eyebrow">audit</span>
            <h2 id="ra-audit-title">研究助理审计</h2>
            <p>统一承载任务账本、Trace、Agent 运行与外部 Agent 审计视图；旧审计路由会重定向到对应标签页，功能只合并不删除。</p>
          </div>
        </div>
        <nav className="ra-tabs" aria-label="研究助理审计标签">
          {AUDIT_TABS.map((tab) => (
            <Link className={`ra-tab ${activeTab === tab.key ? "ra-tab-active" : ""}`} href={tab.href} key={tab.key}>
              {tab.label}
            </Link>
          ))}
        </nav>
      </section>
      {renderAuditSection(activeTab)}
    </main>
  );
}
