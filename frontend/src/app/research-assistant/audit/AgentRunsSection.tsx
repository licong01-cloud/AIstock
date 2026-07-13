"use client";

import { useCallback, useEffect, useState } from "react";

import SectionCard from "@/components/paper-v2/SectionCard";
import { AgentTeamsRunView } from "@/components/research-assistant/AgentTeamsRunView";
import { ApiErrorBox } from "@/components/research-assistant/AssistantShared";
import { researchAssistantApi, type AssistantAgentRun, type AssistantTraceEvent } from "@/lib/research-assistant/api";

export function AgentRunsSection() {
  const [agentRuns, setAgentRuns] = useState<AssistantAgentRun[]>([]);
  const [traceEvents, setTraceEvents] = useState<AssistantTraceEvent[]>([]);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const [runsPage, tracesPage] = await Promise.all([
        researchAssistantApi.agentRuns({ limit: 100 }),
        researchAssistantApi.traceEvents({ limit: 100 }),
      ]);
      setAgentRuns(runsPage.items);
      setTraceEvents(tracesPage.items);
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
      <SectionCard title="Agent 运行审计" eyebrow="assistant_agent_runs / worker traces">
        <p className="pv2-muted">Agent 运行页读取真实 assistant_agent_runs 与 trace event；用于保留 orchestrator、worker、reduce、证据和审批阻断链路，不隐藏失败。</p>
      </SectionCard>
      <AgentTeamsRunView runs={agentRuns} traceEvents={traceEvents} />
    </>
  );
}
