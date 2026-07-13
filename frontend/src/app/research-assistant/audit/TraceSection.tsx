"use client";

import { useCallback, useEffect, useState } from "react";

import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { AgentTeamsRunView } from "@/components/research-assistant/AgentTeamsRunView";
import { ApiErrorBox, DetailDrawer, EmptyState, formatDateTime } from "@/components/research-assistant/AssistantShared";
import {
  researchAssistantApi,
  type AssistantAgentRun,
  type AssistantLlmUsageSummary,
  type AssistantLlmUsageTotals,
  type AssistantOverview,
  type AssistantTraceEvent,
} from "@/lib/research-assistant/api";

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function formatNumber(value: unknown): string {
  if (typeof value === "number" && Number.isFinite(value)) return value.toLocaleString("zh-CN");
  if (typeof value === "string" && value.trim()) {
    const number = Number(value);
    return Number.isFinite(number) ? number.toLocaleString("zh-CN") : value;
  }
  return "-";
}

function formatUsd(value: unknown): string {
  if (value === null || value === undefined || value === "") return "不可用";
  const number = Number(value);
  if (!Number.isFinite(number)) return String(value);
  if (number === 0) return "$0";
  return `$${number.toFixed(number < 0.01 ? 6 : 4)}`;
}

function usageStatusLabel(summary?: AssistantLlmUsageTotals): string {
  const usageStatus = String(summary?.usage_status || "unavailable");
  const costStatus = String(summary?.cost_status || "unavailable");
  return `usage=${usageStatus} / cost=${costStatus}`;
}

function eventUsageSummary(row: AssistantTraceEvent): AssistantLlmUsageTotals {
  const costJson = asRecord(row.cost_json);
  return asRecord(costJson.usage_summary) as AssistantLlmUsageTotals;
}

function usageSummaryLine(summary?: AssistantLlmUsageTotals): string {
  if (!summary) return "Token/Cost: 尚无 ledger 汇总";
  const calls = formatNumber(summary.call_count);
  const totalTokens = formatNumber(summary.total_tokens);
  const cost = formatUsd(summary.total_cost_usd);
  return `${calls} calls · ${totalTokens} tokens · ${cost}`;
}

export function TraceSection() {
  const [overview, setOverview] = useState<AssistantOverview | null>(null);
  const [events, setEvents] = useState<AssistantTraceEvent[]>([]);
  const [agentRuns, setAgentRuns] = useState<AssistantAgentRun[]>([]);
  const [usageSummary, setUsageSummary] = useState<AssistantLlmUsageSummary | null>(null);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setError(null);
    try {
      const [nextOverview, eventPage, runsPage, nextUsageSummary] = await Promise.all([
        researchAssistantApi.overview(),
        researchAssistantApi.traceEvents({ limit: 100 }),
        researchAssistantApi.agentRuns({ limit: 100 }),
        researchAssistantApi.llmUsageSummary({ limit: 100 }),
      ]);
      setOverview(nextOverview);
      setEvents(eventPage.items);
      setAgentRuns(runsPage.items);
      setUsageSummary(nextUsageSummary);
    } catch (exc) {
      setError(exc);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const summary = usageSummary?.summary;

  return (
    <>
      <ApiErrorBox error={error} />
      <AgentTeamsRunView runs={agentRuns} traceEvents={events} />
      <SectionCard title="Trace 与成本" eyebrow="assistant_trace_events / assistant_llm_usage_events">
        <div className="pv2-chip-row">
          {Object.entries(overview?.trace_status || {}).map(([status, count]) => <span className="pv2-chip" key={status}><StatusBadge status={status} /> {count}</span>)}
          {!Object.keys(overview?.trace_status || {}).length ? <span className="pv2-chip">暂无 trace 计数</span> : null}
        </div>
        <div className="pv2-chip-row" style={{ marginTop: 10 }}>
          <span className="pv2-chip">LLM 调用 {formatNumber(summary?.call_count)}</span>
          <span className="pv2-chip">总 tokens {formatNumber(summary?.total_tokens)}</span>
          <span className="pv2-chip">输入 {formatNumber(summary?.prompt_tokens)} / 输出 {formatNumber(summary?.completion_tokens)}</span>
          <span className="pv2-chip">成本 {formatUsd(summary?.total_cost_usd)}</span>
          <span className="pv2-chip">{usageStatusLabel(summary)}</span>
        </div>
        <p className="pv2-muted">
          Token 与成本事实源为 <span className="pv2-mono">assistant_llm_usage_events</span>；trace 的 <span className="pv2-mono">cost_json</span> 仅是汇总缓存。估算或不可用会显式标注，不保存 prompt 全文。
        </p>
        {usageSummary ? <DetailDrawer title="LLM usage ledger summary" data={usageSummary} /> : null}
      </SectionCard>
      <SectionCard title="Trace Events" eyebrow="LLM / MCP / Skill">
        <PaperTable
          rows={events}
          empty="暂无 Trace Event。"
          columns={[
            { key: "event", header: "事件", render: (row) => <><span className="ra-title">{row.event_type || row.trace_id}</span><br /><span className="pv2-muted pv2-mono">{row.component || "-"}</span></> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            {
              key: "cost",
              header: "成本/耗时",
              render: (row) => {
                const rowSummary = eventUsageSummary(row);
                return (
                  <>
                    <span>{usageSummaryLine(rowSummary)}</span>
                    <br />
                    <span className="pv2-muted">{row.duration_ms ? `${row.duration_ms} ms` : "-"} · {usageStatusLabel(rowSummary)}</span>
                    <br />
                    <DetailDrawer title="cost_json" data={row.cost_json || {}} />
                  </>
                );
              },
            },
            { key: "time", header: "时间", render: (row) => formatDateTime(row.created_at) },
            { key: "detail", header: "详情", render: (row) => <DetailDrawer title="payload" data={row} /> },
          ]}
        />
        {!events.length ? <EmptyState title="Trace Event 为空" hint="这是来自真实 API 的空状态；后端执行器写入后才展示记录。" /> : null}
      </SectionCard>
    </>
  );
}
