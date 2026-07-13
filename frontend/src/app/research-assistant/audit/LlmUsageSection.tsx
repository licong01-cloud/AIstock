"use client";

import dynamic from "next/dynamic";
import { useCallback, useEffect, useMemo, useState } from "react";

import { ApiErrorBox, EmptyState } from "@/components/research-assistant/AssistantShared";
import { formatUsageNumber, formatUsageUsd, usageStatusText } from "@/components/research-assistant/llm-usage-format";
import {
  researchAssistantApi,
  type AssistantLlmUsageReport,
  type AssistantLlmUsageTimeBucket,
} from "@/lib/research-assistant/api";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false }) as any;
const TIMEZONE = "Asia/Shanghai";

function dateInput(daysAgo: number): string {
  const date = new Date();
  date.setDate(date.getDate() - daysAgo);
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
}

function dateRangeHours(dateFrom: string, dateTo: string): number {
  const start = new Date(`${dateFrom}T00:00:00+08:00`).getTime();
  const end = new Date(`${dateTo}T23:59:59+08:00`).getTime();
  return Math.max(0, (end - start) / 3_600_000);
}

function autoGranularity(dateFrom: string, dateTo: string): "hour" | "day" {
  return dateRangeHours(dateFrom, dateTo) <= 48 ? "hour" : "day";
}

function asCostNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function bucketLabel(bucket: AssistantLlmUsageTimeBucket): string {
  return String(bucket.bucket_start || "").replace("T", " ").slice(0, 16);
}

function sumByBucket(buckets: AssistantLlmUsageTimeBucket[], field: "total_tokens" | "total_cost_usd"): Array<{ x: string; y: number | null }> {
  const values = new Map<string, number>();
  const costUnavailable = new Set<string>();
  for (const bucket of buckets) {
    const x = bucketLabel(bucket);
    if (field === "total_cost_usd") {
      const cost = asCostNumber(bucket.total_cost_usd);
      if (cost === null) {
        costUnavailable.add(x);
        if (!values.has(x)) values.set(x, 0);
      } else {
        values.set(x, (values.get(x) || 0) + cost);
      }
    } else {
      values.set(x, (values.get(x) || 0) + Number(bucket.total_tokens || 0));
    }
  }
  return [...values.entries()].sort(([left], [right]) => left.localeCompare(right)).map(([x, y]) => ({
    x,
    y: field === "total_cost_usd" && costUnavailable.has(x) && y === 0 ? null : y,
  }));
}

function statusChartData(report: AssistantLlmUsageReport | null) {
  const usage = report?.status_breakdown?.usage || {};
  const cost = report?.status_breakdown?.cost || {};
  const statuses = ["recorded", "estimated", "unavailable", "failed"];
  return [
    { x: statuses, y: statuses.map((status) => usage[status] || 0), type: "bar", name: "usage" },
    { x: statuses, y: statuses.map((status) => cost[status] || 0), type: "bar", name: "cost" },
  ];
}

export function LlmUsageSection() {
  const [dateFrom, setDateFrom] = useState(dateInput(6));
  const [dateTo, setDateTo] = useState(dateInput(0));
  const [granularity, setGranularity] = useState<"hour" | "day">(autoGranularity(dateInput(6), dateInput(0)));
  const [model, setModel] = useState("");
  const [provider, setProvider] = useState("");
  const [report, setReport] = useState<AssistantLlmUsageReport | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const next = await researchAssistantApi.llmUsageReport({
        date_from: `${dateFrom}T00:00:00+08:00`,
        date_to: `${dateTo}T23:59:59+08:00`,
        granularity,
        timezone: TIMEZONE,
        model: model.trim() || undefined,
        provider: provider.trim() || undefined,
        limit_models: 8,
      });
      setReport(next);
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [dateFrom, dateTo, granularity, model, provider]);

  useEffect(() => {
    void load();
  }, [load]);

  const summary = report?.summary;
  const buckets = useMemo(() => report?.time_series || [], [report]);
  const modelOptions = useMemo(() => [...new Set((report?.model_breakdown || []).map((item) => String(item.model || "")).filter(Boolean))], [report]);
  const providerOptions = useMemo(() => [...new Set((report?.model_breakdown || []).map((item) => String(item.provider || "")).filter(Boolean))], [report]);
  const models = useMemo(() => [...new Set(buckets.map((bucket) => String(bucket.model || "unknown")))], [buckets]);
  const tokenSeries = models.map((name) => ({
    x: buckets.filter((bucket) => String(bucket.model || "unknown") === name).map(bucketLabel),
    y: buckets.filter((bucket) => String(bucket.model || "unknown") === name).map((bucket) => Number(bucket.total_tokens || 0)),
    customdata: buckets.filter((bucket) => String(bucket.model || "unknown") === name).map((bucket) => String(bucket.provider || "unknown")),
    type: "bar",
    name,
    hovertemplate: "%{x}<br>%{y} tokens<br>provider=%{customdata}<extra>%{fullData.name}</extra>",
  }));
  const costPoints = sumByBucket(buckets, "total_cost_usd");
  const costUnavailable = (summary?.unavailable_cost_event_count || 0) + (summary?.failed_cost_event_count || 0);
  const topModels = report?.model_breakdown || [];
  const hasData = buckets.length > 0 || topModels.length > 0 || Number(summary?.call_count || 0) > 0;

  function updateDateRange(nextFrom: string, nextTo: string) {
    setDateFrom(nextFrom);
    setDateTo(nextTo);
    setGranularity(autoGranularity(nextFrom, nextTo));
  }

  function applyPreset(days: 7 | 30) {
    const nextFrom = dateInput(days - 1);
    const nextTo = dateInput(0);
    updateDateRange(nextFrom, nextTo);
  }

  return (
    <section className="ra-usage-section" data-testid="ra-llm-usage-section">
      <ApiErrorBox error={error} />
      <div className="ra-usage-filter-card">
        <div>
          <span className="ra-chat-eyebrow">assistant_llm_usage_events</span>
          <h2>LLM 消耗报表</h2>
          <p>默认最近 7 天；可切换最近 30 天。图表按模型分组，provider 作为过滤与 tooltip 维度；timezone={TIMEZONE}。</p>
        </div>
        <div className="ra-usage-filter-grid">
          <label>
            <span>开始日期</span>
            <input type="date" value={dateFrom} onChange={(event) => updateDateRange(event.target.value, dateTo)} />
          </label>
          <label>
            <span>结束日期</span>
            <input type="date" value={dateTo} onChange={(event) => updateDateRange(dateFrom, event.target.value)} />
          </label>
          <label>
            <span>模型</span>
            <input list="ra-llm-model-options" value={model} onChange={(event) => setModel(event.target.value)} placeholder="全部模型" />
            <datalist id="ra-llm-model-options">{modelOptions.map((item) => <option value={item} key={item} />)}</datalist>
          </label>
          <label>
            <span>Provider</span>
            <input list="ra-llm-provider-options" value={provider} onChange={(event) => setProvider(event.target.value)} placeholder="全部 provider" />
            <datalist id="ra-llm-provider-options">{providerOptions.map((item) => <option value={item} key={item} />)}</datalist>
          </label>
        </div>
        <div className="ra-usage-actions">
          <button type="button" className="ra-secondary-button" onClick={() => applyPreset(7)}>最近 7 天</button>
          <button type="button" className="ra-secondary-button" onClick={() => applyPreset(30)}>最近 30 天</button>
          <button type="button" className={granularity === "hour" ? "ra-primary-button" : "ra-secondary-button"} onClick={() => setGranularity("hour")}>按小时</button>
          <button type="button" className={granularity === "day" ? "ra-primary-button" : "ra-secondary-button"} onClick={() => setGranularity("day")}>按天</button>
          <button type="button" className="ra-primary-button" onClick={() => void load()} disabled={loading}>{loading ? "刷新中" : "刷新"}</button>
        </div>
      </div>

      <div className="ra-usage-kpi-grid" data-testid="ra-llm-usage-kpis">
        <div className="ra-usage-kpi"><span>总 tokens</span><strong>{formatUsageNumber(summary?.total_tokens)}</strong><small>{usageStatusText(summary)}</small></div>
        <div className="ra-usage-kpi"><span>输入 / 输出</span><strong>{formatUsageNumber(summary?.prompt_tokens)} / {formatUsageNumber(summary?.completion_tokens)}</strong><small>prompt / completion</small></div>
        <div className="ra-usage-kpi"><span>成本 USD</span><strong>{formatUsageUsd(summary?.total_cost_usd, summary?.cost_reason_code || summary?.reason_code)}</strong><small>{costUnavailable ? `${costUnavailable} 条成本不可用或失败` : "ledger 已记录"}</small></div>
        <div className="ra-usage-kpi"><span>调用次数</span><strong>{formatUsageNumber(summary?.call_count)}</strong><small>估算 {formatUsageNumber(summary?.estimated_usage_event_count)} / 不可用 {formatUsageNumber(summary?.unavailable_usage_event_count)}</small></div>
      </div>

      {!hasData && !error ? <EmptyState title="所选范围暂无 LLM ledger 记录" hint="图表只消费 assistant_llm_usage_events；不会用 trace cost_json 伪造报表。" /> : null}

      {hasData ? (
        <div className="ra-usage-chart-grid" data-testid="ra-llm-usage-charts">
          <div className="ra-usage-chart-card" data-testid="ra-llm-token-chart">
            <h3>Token 趋势（按模型）</h3>
            <Plot data={tokenSeries} layout={{ barmode: "stack", autosize: true, height: 320, margin: { t: 18, r: 18, b: 70, l: 58 }, paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)" }} useResizeHandler style={{ width: "100%" }} config={{ displayModeBar: false }} />
          </div>
          <div className="ra-usage-chart-card" data-testid="ra-llm-cost-chart">
            <h3>成本趋势</h3>
            <Plot data={[{ x: costPoints.map((point) => point.x), y: costPoints.map((point) => point.y), type: "scatter", mode: "lines+markers", name: "cost_usd" }]} layout={{ autosize: true, height: 320, margin: { t: 18, r: 18, b: 70, l: 58 }, paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", annotations: costUnavailable ? [{ text: "部分成本不可用，未按 $0 渲染", xref: "paper", yref: "paper", x: 0, y: 1.08, showarrow: false }] : [] }} useResizeHandler style={{ width: "100%" }} config={{ displayModeBar: false }} />
          </div>
          <div className="ra-usage-chart-card" data-testid="ra-llm-top-model-chart">
            <h3>Top 模型 token 消耗</h3>
            <Plot data={[{ x: topModels.map((item) => Number(item.total_tokens || 0)), y: topModels.map((item) => `${item.model || "unknown"} (${item.provider || "unknown"})`), type: "bar", orientation: "h", name: "tokens" }]} layout={{ autosize: true, height: 340, margin: { t: 18, r: 18, b: 50, l: 190 }, paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)" }} useResizeHandler style={{ width: "100%" }} config={{ displayModeBar: false }} />
          </div>
          <div className="ra-usage-chart-card" data-testid="ra-llm-status-chart">
            <h3>Usage / Cost 状态分布</h3>
            <Plot data={statusChartData(report)} layout={{ barmode: "group", autosize: true, height: 340, margin: { t: 18, r: 18, b: 60, l: 58 }, paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)" }} useResizeHandler style={{ width: "100%" }} config={{ displayModeBar: false }} />
          </div>
        </div>
      ) : null}
    </section>
  );
}
