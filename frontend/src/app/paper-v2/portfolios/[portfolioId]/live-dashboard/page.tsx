"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";
import CopyChip from "@/components/paper-v2/CopyChip";
import ErrorListCard from "@/components/paper-v2/ErrorListCard";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { paperV2Api } from "@/lib/paper-v2/api";
import { asText, dataSourceLabel, formatCompact, formatNumber, formatPercent, shortHash } from "@/lib/paper-v2/format";
import type { JsonObject, PaperLiveDashboard } from "@/lib/paper-v2/types";

function asArray(value: unknown): JsonObject[] {
  return Array.isArray(value) ? value.filter((item): item is JsonObject => typeof item === "object" && item !== null && !Array.isArray(item)) : [];
}

function obj(value: unknown): JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value) ? value as JsonObject : {};
}

function num(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function dt(value: unknown): string {
  if (!value) return "-";
  return String(value).slice(0, 19).replace("T", " ");
}

function sideLabel(value: unknown): string {
  const raw = String(value || "");
  if (raw === "BUY") return "买入";
  if (raw === "SELL") return "卖出";
  return raw || "-";
}

function packageName(dashboard: PaperLiveDashboard | null): string {
  return String(dashboard?.package?.package_name || dashboard?.portfolio?.package_id || "-");
}

function latestSnapshot(dashboard: PaperLiveDashboard | null): JsonObject {
  const intraday = asArray(dashboard?.intraday_nav?.snapshots).at(-1);
  const daily = (dashboard?.daily_snapshots || [])[0];
  return intraday || daily || {};
}

function navBars(dashboard: PaperLiveDashboard | null) {
  const rows = asArray(dashboard?.intraday_nav?.snapshots);
  const values = rows.map((item) => num(item.nav)).filter((value) => value > 0);
  const min = values.length ? Math.min(...values) : 0;
  const max = values.length ? Math.max(...values) : 0;
  return rows.map((item) => {
    const nav = num(item.nav);
    const pct = max > min ? ((nav - min) / (max - min)) * 100 : 50;
    return { time: dt(item.snapshot_time), nav, pct: Math.max(8, pct) };
  });
}

export default function PaperV2LiveDashboardPage() {
  const params = useParams<{ portfolioId: string }>();
  const portfolioId = String(params.portfolioId || "");
  const [dashboard, setDashboard] = useState<PaperLiveDashboard | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setDashboard(await paperV2Api.liveDashboard(portfolioId, { event_limit: 800 }));
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [portfolioId]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => {
    if (!autoRefresh) return;
    const timer = window.setInterval(() => { load(); }, 10_000);
    return () => window.clearInterval(timer);
  }, [autoRefresh, load]);

  const signal = obj(dashboard?.daily_signal);
  const target = obj(dashboard?.target_rebalance);
  const minute = obj(dashboard?.minute_execution);
  const minuteSummary = obj(minute.summary);
  const freshness = obj(dashboard?.data_freshness);
  const scheduler = obj(dashboard?.scheduler);
  const snapshot = latestSnapshot(dashboard);
  const initialCash = num(dashboard?.portfolio?.initial_cash);
  const nav = num(snapshot.nav || dashboard?.portfolio?.initial_cash);
  const returnRate = initialCash ? nav / initialCash - 1 : null;
  const candidates = asArray(signal.top_candidates);
  const targets = asArray(target.targets);
  const intents = asArray(target.order_intents);
  const timeline = asArray(minute.timeline);
  const warnings = dashboard?.warnings || [];
  const positions = dashboard?.positions || [];
  const errors = dashboard?.errors || [];
  const bars = useMemo(() => navBars(dashboard), [dashboard]);

  return (
    <main data-testid="paper-live-dashboard">
      <div className="pv2-detail-nav">
        <Link href="/paper-v2/running">运行监控</Link>
        <Link href={`/paper-v2/portfolios/${portfolioId}`}>组合统计</Link>
        <Link href={`/paper-v2/portfolios/${portfolioId}/run-console`}>运行控制台</Link>
        <Link href={`/paper-v2/portfolios/${portfolioId}/ledger`}>账本</Link>
        <Link href={`/paper-v2/portfolios/${portfolioId}/performance`}>绩效</Link>
      </div>
      <ErrorPanel error={error} title="实时运行详情加载失败" />

      <SectionCard
        title={`${packageName(dashboard)} 实时模拟盘详情`}
        eyebrow={dashboard?.active_session ? `session ${shortHash(dashboard.active_session.session_id)}` : "只读观察"}
        action={
          <div className="pv2-row-actions">
            <label className="pv2-chip"><input type="checkbox" checked={autoRefresh} onChange={(event) => setAutoRefresh(event.target.checked)} /> 自动刷新</label>
            <button className="pv2-button" data-testid="live-dashboard-refresh" onClick={load} disabled={loading} type="button">刷新</button>
          </div>
        }
      >
        <div className="pv2-grid pv2-grid-4">
          <MetricCard label="会话状态" value={dashboard?.active_session?.status || "无会话"} hint={dashboard?.active_session?.mode || "-"} tone={errors.length ? "danger" : "success"} />
          <MetricCard label="最新处理分钟" value={dt(freshness.last_processed_bar_time)} hint={`可用 ${dt(freshness.latest_available_bar_time)}`} tone={num(freshness.lag_minutes) <= 1 ? "success" : "warning"} />
          <MetricCard label="当前净值" value={formatCompact(nav)} hint={`收益 ${formatPercent(returnRate)}`} tone={(returnRate || 0) >= 0 ? "success" : "danger"} />
          <MetricCard label="订单/成交/未成交" value={`${formatNumber(minuteSummary.order_count, 0)} / ${formatNumber(minuteSummary.fill_count, 0)} / ${formatNumber(minuteSummary.no_fill_count, 0)}`} hint={`调度器 ${scheduler.running ? "运行中" : "未运行"}`} tone={errors.length ? "danger" : "info"} />
        </div>
        <div className="pv2-chip-row" style={{ marginTop: 14 }}>
          <span className="pv2-chip">数据源: {dataSourceLabel(dashboard?.current_run?.data_source)}</span>
          <span className="pv2-chip">交易日: {dashboard?.current_run?.trade_date || "-"}</span>
          <CopyChip label={`portfolio_id ${shortHash(portfolioId, 6)}`} value={portfolioId} title={`完整 portfolio_id：${portfolioId}`} />
          <CopyChip label={`package_id ${shortHash(dashboard?.portfolio?.package_id, 6)}`} value={dashboard?.portfolio?.package_id} title={dashboard?.portfolio?.package_id ? `完整 package_id：${dashboard.portfolio.package_id}` : ""} />
          <CopyChip label={`manifest ${shortHash(dashboard?.portfolio?.manifest_sha256, 6)}`} value={dashboard?.portfolio?.manifest_sha256} title={dashboard?.portfolio?.manifest_sha256 ? `完整 manifest_sha256：${dashboard.portfolio.manifest_sha256}` : ""} />
        </div>
      </SectionCard>

      {warnings.length ? (
        <NoticePanel title="运行观察提示" tone="warning">
          {warnings.map((item, index) => <span key={index}>{asText(item.message)}{index < warnings.length - 1 ? "；" : ""}</span>)}
        </NoticePanel>
      ) : null}

      <div className="pv2-grid pv2-grid-main">
        <SectionCard title="今日信号" eyebrow={`cutoff ${asText(signal.cutoff_date)}`} action={<StatusBadge status={String(signal.status || "MISSING")} />}>
          {String(signal.status || "") !== "AVAILABLE" ? (
            <NoticePanel title="信号不可用" tone="warning">{asText(signal.missing_reason)}</NoticePanel>
          ) : (
            <>
              <div className="pv2-chip-row" style={{ marginBottom: 12 }}>
                <span className="pv2-chip">目标交易日 {asText(signal.trade_date)}</span>
                <span className="pv2-chip">打分日 {asText(signal.score_trade_date)}</span>
                <span className="pv2-chip">参考价日 {asText(signal.reference_price_trade_date)}</span>
                <span className="pv2-chip">候选数 {formatNumber(signal.candidate_count, 0)}</span>
              </div>
              <PaperTable
                rows={candidates.slice(0, 20)}
                empty="没有可展示的 Top 候选。"
                columns={[
                  { key: "rank", header: "排名", render: (row) => formatNumber(row.rank, 0) },
                  { key: "symbol", header: "股票", render: (row) => asText(row.symbol) },
                  { key: "score", header: "分数", render: (row) => formatNumber(row.score, 6) },
                  { key: "price", header: "参考价", render: (row) => formatNumber(row.reference_price, 3) },
                  { key: "weight", header: "目标权重", render: (row) => formatPercent(row.target_weight) },
                  { key: "reason", header: "来源", render: (row) => asText(row.reason) },
                ]}
              />
            </>
          )}
        </SectionCard>

        <SectionCard title="目标仓位与调仓意图" eyebrow={`${targets.length} 个目标 / ${intents.length} 个订单意图`} action={<StatusBadge status={String(target.status || "MISSING")} />}>
          <PaperTable
            rows={targets.slice(0, 20)}
            empty={String(target.status || "") === "MISSING" ? asText(target.missing_reason) : "暂无目标仓位。"}
            columns={[
              { key: "symbol", header: "股票", render: (row) => asText(row.symbol) },
              { key: "rank", header: "排名", render: (row) => formatNumber(row.rank, 0) },
              { key: "qty", header: "目标股数", render: (row) => formatNumber(row.target_quantity, 0) },
              { key: "weight", header: "目标权重", render: (row) => formatPercent(row.target_weight) },
            ]}
          />
          <PaperTable
            rows={intents.slice(0, 20)}
            empty="暂无调仓意图。"
            columns={[
              { key: "symbol", header: "股票", render: (row) => asText(row.symbol) },
              { key: "side", header: "方向", render: (row) => sideLabel(row.side) },
              { key: "qty", header: "订单股数", render: (row) => formatNumber(row.quantity, 0) },
              { key: "intent", header: "意图", render: (row) => <span className="pv2-mono">{shortHash(row.intent_id)}</span> },
            ]}
          />
        </SectionCard>
      </div>

      <SectionCard title="分钟执行时间轴" eyebrow="每分钟成交 / 未成交原因" action={<StatusBadge status={String(minute.status || "MISSING")} />}>
        <PaperTable
          rows={timeline.slice(0, 80)}
          empty={String(minute.status || "") === "MISSING" ? asText(minute.missing_reason) : "暂无分钟执行事件。"}
          columns={[
            { key: "time", header: "时间", render: (row) => dt(row.event_time) },
            { key: "symbol", header: "股票", render: (row) => asText(row.symbol) },
            { key: "side", header: "方向", render: (row) => sideLabel(row.side) },
            { key: "order", header: "订单量", render: (row) => formatNumber(row.order_quantity, 0) },
            { key: "fill", header: "本次成交", render: (row) => formatNumber(row.fill_quantity, 0) },
            { key: "price", header: "成交价", render: (row) => formatNumber(row.fill_price, 4) },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={String(row.event_type || "-")} /> },
            { key: "reason", header: "原因", render: (row) => asText(row.reason_label) },
          ]}
        />
      </SectionCard>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="实时资产曲线" eyebrow={`${bars.length} 个分钟快照`}>
          {bars.length ? (
            <div className="pv2-sparkline">
              {bars.map((item, index) => <div className="pv2-spark-bar" key={`${item.time}-${index}`} style={{ height: `${item.pct}%` }} title={`${item.time}: ${formatNumber(item.nav, 2)}`} />)}
            </div>
          ) : (
            <NoticePanel title="分钟资产快照缺失" tone="warning">{asText(dashboard?.intraday_nav?.missing_reason)}</NoticePanel>
          )}
          <div className="pv2-grid pv2-grid-3" style={{ marginTop: 14 }}>
            <MetricCard label="现金" value={formatNumber(snapshot.cash, 2)} />
            <MetricCard label="持仓市值" value={formatNumber(snapshot.market_value, 2)} />
            <MetricCard label="净值" value={formatNumber(snapshot.nav || nav, 2)} />
          </div>
        </SectionCard>

        <SectionCard title="当前持仓" eyebrow={`${positions.length} 只持仓`}>
          <PaperTable
            rows={positions.slice(0, 30)}
            empty="暂无当前持仓。"
            columns={[
              { key: "symbol", header: "股票", render: (row) => asText(row.symbol) },
              { key: "qty", header: "数量", render: (row) => formatNumber(row.quantity, 0) },
              { key: "cost", header: "成本", render: (row) => formatNumber(row.avg_cost, 4) },
              { key: "price", header: "最新价", render: (row) => formatNumber(row.market_price, 4) },
              { key: "value", header: "市值", render: (row) => formatNumber(row.market_value, 2) },
            ]}
          />
        </SectionCard>
      </div>

      <SectionCard title="错误与数据质量" eyebrow="fail-fast 结果必须显式展示">
        <ErrorListCard rows={errors.slice(0, 20)} empty="暂无持久化错误。" />
      </SectionCard>
    </main>
  );
}
