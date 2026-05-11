"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import {
  API_BASE,
  REGIME_LABELS,
  REGIME_VALUES,
  SOURCE_METHODS,
  type Regime,
  type RegimeDistributionResponse,
  type RegimeLabel,
  type RegimeMethodsResponse,
  marketRegimeApi,
} from "@/lib/market-regime/api";

function shortDate(value?: string | null): string {
  if (!value) return "-";
  return value.slice(0, 10);
}

function regimeTone(regime: string): "success" | "danger" | "warning" | "info" | "neutral" {
  switch (regime) {
    case "bull":
      return "success";
    case "bear":
      return "danger";
    case "high_vol":
      return "warning";
    case "low_vol":
      return "info";
    case "oscillation":
      return "neutral";
    default:
      return "neutral";
  }
}

export default function MarketRegimePage() {
  const [sourceMethod, setSourceMethod] = useState<string>("simple_quadrant");
  const [methods, setMethods] = useState<RegimeMethodsResponse | null>(null);
  const [timeline, setTimeline] = useState<RegimeLabel[]>([]);
  const [distribution, setDistribution] = useState<RegimeDistributionResponse | null>(null);
  const [current, setCurrent] = useState<RegimeLabel | null>(null);
  const [startDate, setStartDate] = useState<string>("");
  const [endDate, setEndDate] = useState<string>("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = {
        source_method: sourceMethod,
        start_date: startDate || undefined,
        end_date: endDate || undefined,
      };
      const [methodsRes, timelineRes, distRes, currentRes] = await Promise.all([
        marketRegimeApi.methods(),
        marketRegimeApi.timeline({ ...params, limit: 2500 }),
        marketRegimeApi.distribution(params),
        marketRegimeApi.current(sourceMethod),
      ]);
      setMethods(methodsRes);
      setTimeline(timelineRes.items);
      setDistribution(distRes);
      setCurrent(currentRes);
    } catch (err) {
      setError(err);
    } finally {
      setLoading(false);
    }
  }, [sourceMethod, startDate, endDate]);

  useEffect(() => {
    void load();
  }, [load]);

  const distributionRows = useMemo(() => {
    if (!distribution) return [];
    return distribution.items.map((item) => ({
      ...item,
      label: REGIME_LABELS[item.regime as Regime] ?? item.regime,
      pctText: `${(item.pct * 100).toFixed(1)}%`,
      width: Math.max(2, Math.round(item.pct * 100)),
    }));
  }, [distribution]);

  const latestRows = useMemo(() => timeline.slice(-30).reverse(), [timeline]);

  return (
    <main className="pv2-shell">
      <section className="pv2-hero">
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">Market Regime</div>
            <h1>市场状态分类</h1>
            <p>
              展示 <span className="pv2-mono">market.regime_label</span> 的时间线与分布，并支持切换 source_method
              (<span className="pv2-mono">simple_quadrant / hmm_viterbi / bbq / ensemble</span>)。当前 dev DB 仅 simple_quadrant 有合成数据。
              <span className="pv2-mono"> {API_BASE}/market/regime-label </span> API。
            </p>
          </div>
          <div className="pv2-row-actions">
            <button
              className="pv2-button-primary"
              type="button"
              onClick={() => void load()}
              disabled={loading}
              data-testid="refresh-regime"
            >
              {loading ? "刷新中" : "刷新"}
            </button>
          </div>
        </div>
      </section>

      <ErrorPanel error={error} title="Market Regime API 调用失败" />

      <div className="pv2-grid pv2-grid-4">
        <MetricCard
          label="当前 source_method"
          value={sourceMethod}
          hint={methods ? `已有数据 ${methods.available.length}/${methods.supported.length}` : "-"}
        />
        <MetricCard
          label="时间线条目"
          value={String(timeline.length)}
          hint={
            timeline.length
              ? `${shortDate(timeline[0].trade_date)} → ${shortDate(timeline[timeline.length - 1].trade_date)}`
              : "无数据"
          }
        />
        <MetricCard
          label="分布总数"
          value={String(distribution?.total ?? 0)}
          hint={distribution ? `${distribution.items.length} 个 regime` : "-"}
        />
        <MetricCard
          label="当前 regime"
          value={current ? REGIME_LABELS[current.regime] ?? current.regime : "-"}
          hint={current ? `as of ${shortDate(current.trade_date)}` : "无最新标签"}
          tone={current ? regimeTone(current.regime) : "neutral"}
        />
      </div>

      <SectionCard title="Source Method" eyebrow="switch classification method">
        <div className="pv2-form-grid">
          <label className="pv2-field">
            <span>分类方法</span>
            <select
              className="pv2-select"
              value={sourceMethod}
              onChange={(event) => setSourceMethod(event.target.value)}
              data-testid="source-method"
            >
              {SOURCE_METHODS.map((method) => {
                const hasData = methods?.available.includes(method);
                return (
                  <option key={method} value={method}>
                    {method}
                    {hasData ? "" : " (无数据)"}
                  </option>
                );
              })}
            </select>
          </label>
          <label className="pv2-field">
            <span>起始日期</span>
            <input
              className="pv2-input"
              type="date"
              value={startDate}
              onChange={(event) => setStartDate(event.target.value)}
              data-testid="start-date"
            />
          </label>
          <label className="pv2-field">
            <span>结束日期</span>
            <input
              className="pv2-input"
              type="date"
              value={endDate}
              onChange={(event) => setEndDate(event.target.value)}
              data-testid="end-date"
            />
          </label>
        </div>
      </SectionCard>

      <SectionCard title="Regime 分布" eyebrow="bull / bear / oscillation / high_vol / low_vol">
        {!distribution || distribution.total === 0 ? (
          <div className="pv2-help" data-testid="distribution-empty">所选 source_method 区间内暂无数据。</div>
        ) : (
          <div className="pv2-readable-list" data-testid="distribution-rows">
            {distributionRows.map((row) => (
              <div key={row.regime} className="pv2-readable-row" data-testid={`dist-${row.regime}`}>
                <div className="pv2-readable-key">{row.label}</div>
                <div className="pv2-readable-value">
                  <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    <div
                      style={{
                        background: regimeTone(row.regime) === "success" ? "#16a34a" : regimeTone(row.regime) === "danger" ? "#dc2626" : regimeTone(row.regime) === "warning" ? "#f59e0b" : regimeTone(row.regime) === "info" ? "#3b82f6" : "#94a3b8",
                        width: `${row.width}%`,
                        height: 14,
                        borderRadius: 4,
                        minWidth: 8,
                      }}
                    />
                    <span className="pv2-mono">{row.pctText}</span>
                    <span className="pv2-muted">({row.count})</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </SectionCard>

      <SectionCard title="近期时间线" eyebrow="latest 30 trade dates">
        <PaperTable
          rows={latestRows}
          empty="所选 source_method 区间内暂无数据"
          columns={[
            {
              key: "date",
              header: "交易日",
              render: (row) => <span className="pv2-mono">{shortDate(row.trade_date)}</span>,
            },
            {
              key: "regime",
              header: "Regime",
              render: (row) => (
                <span data-testid={`timeline-regime-${row.trade_date}`}>
                  <StatusBadge status={REGIME_LABELS[row.regime] ?? row.regime} />
                </span>
              ),
            },
            {
              key: "confidence",
              header: "Confidence",
              render: (row) => (row.regime_confidence == null ? "-" : row.regime_confidence.toFixed(3)),
            },
            {
              key: "signal",
              header: "信号摘要",
              render: (row) => (
                <span className="pv2-muted pv2-mono">
                  {row.source_signal_json
                    ? Object.entries(row.source_signal_json)
                        .slice(0, 3)
                        .map(([k, v]) => `${k}=${typeof v === "number" ? Number(v).toFixed(3) : String(v)}`)
                        .join(" / ")
                    : "-"}
                </span>
              ),
            },
            {
              key: "labeled",
              header: "标注时间",
              render: (row) => shortDate(row.labeled_at),
            },
          ]}
        />
      </SectionCard>
    </main>
  );
}
