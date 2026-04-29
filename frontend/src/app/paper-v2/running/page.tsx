"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { paperV2Api } from "@/lib/paper-v2/api";
import { formatCompact, formatNumber, formatPercent, shortHash } from "@/lib/paper-v2/format";
import type { JsonObject, PaperPortfolio, PaperRun, PaperSession } from "@/lib/paper-v2/types";

type RunningPortfolioSummary = {
  portfolio: PaperPortfolio;
  latestRun?: PaperRun;
  latestSession?: PaperSession;
  orders: JsonObject[];
  fills: JsonObject[];
  positions: JsonObject[];
  snapshots: JsonObject[];
  errors: JsonObject[];
  performance?: JsonObject | null;
};

function n(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function latestSnapshot(row: RunningPortfolioSummary): JsonObject | undefined {
  return row.snapshots[0];
}

function latestPositionDate(row: RunningPortfolioSummary): string {
  return String(row.positions[0]?.trade_date || "-");
}

function latestPositions(row: RunningPortfolioSummary): JsonObject[] {
  const date = latestPositionDate(row);
  if (date === "-") return [];
  return row.positions.filter((item) => String(item.trade_date || "") === date);
}

function totalReturn(row: RunningPortfolioSummary): number | null {
  const snapshot = latestSnapshot(row);
  const nav = n(snapshot?.nav);
  const initial = n(row.portfolio.initial_cash);
  if (!nav || !initial) return null;
  return nav / initial - 1;
}

function packageName(portfolio: PaperPortfolio): string {
  return String(portfolio.frozen_manifest?.package_name || portfolio.package_id || "-");
}

function packageSource(portfolio: PaperPortfolio): string {
  return String(portfolio.frozen_manifest?.source_id || portfolio.frozen_manifest?.run_id || portfolio.package_id || "-");
}

function navSeries(row: RunningPortfolioSummary) {
  const ordered = [...row.snapshots].reverse();
  const values = ordered.map((item) => n(item.nav)).filter((value) => Number.isFinite(value) && value > 0);
  const min = Math.min(...values);
  const max = Math.max(...values);
  return ordered.map((item) => {
    const nav = n(item.nav);
    const pct = max > min ? ((nav - min) / (max - min)) * 100 : 50;
    return { trade_date: String(item.trade_date || "-"), nav, pct: Math.max(8, pct) };
  });
}

export default function PaperV2RunningPage() {
  const [rows, setRows] = useState<RunningPortfolioSummary[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const portfolios = await paperV2Api.listPortfolios(300);
      const active = portfolios.filter((item) => ["READY", "RUNNING", "PAUSED"].includes(String(item.status || "").toUpperCase()));
      const summaries = await Promise.all(active.map(async (portfolio) => {
        const [runs, sessions, orders, fills, positions, snapshots, errors] = await Promise.all([
          paperV2Api.runs(portfolio.portfolio_id),
          paperV2Api.listSessions(portfolio.portfolio_id),
          paperV2Api.orders(portfolio.portfolio_id),
          paperV2Api.fills(portfolio.portfolio_id),
          paperV2Api.positions(portfolio.portfolio_id),
          paperV2Api.snapshots(portfolio.portfolio_id),
          paperV2Api.errors(portfolio.portfolio_id),
        ]);
        const performance = snapshots.length ? await paperV2Api.performanceOrNull(portfolio.portfolio_id) : null;
        return { portfolio, latestRun: runs[0], latestSession: sessions[0], orders, fills, positions, snapshots, errors, performance };
      }));
      setRows(summaries);
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const totals = useMemo(() => {
    const nav = rows.reduce((sum, row) => sum + n(latestSnapshot(row)?.nav || row.portfolio.initial_cash), 0);
    const initial = rows.reduce((sum, row) => sum + n(row.portfolio.initial_cash), 0);
    const errors = rows.reduce((sum, row) => sum + row.errors.length, 0);
    const fills = rows.reduce((sum, row) => sum + row.fills.length, 0);
    return { nav, initial, pnl: nav - initial, returnRate: initial ? nav / initial - 1 : null, errors, fills };
  }, [rows]);

  return (
    <main>
      <ErrorPanel error={error} title="正在运行模拟盘汇总加载失败" />
      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="活跃模拟盘" value={rows.length} hint="READY / RUNNING / PAUSED" tone="info" />
        <MetricCard label="当前总净值" value={formatCompact(totals.nav)} hint={`初始资金 ${formatCompact(totals.initial)}`} tone="success" />
        <MetricCard label="累计收益" value={formatNumber(totals.pnl, 2)} hint={formatPercent(totals.returnRate)} tone={totals.pnl >= 0 ? "success" : "danger"} />
        <MetricCard label="阻断错误" value={totals.errors} hint={`${totals.fills} 条成交记录`} tone={totals.errors ? "danger" : "success"} />
      </div>

      <NoticePanel title="模拟盘运行总览" tone="info">
        本页只读取 Paper Trading v2 已持久化账本、会话、订单、成交、持仓、快照和绩效；不会触发交易、回放、重置或调度动作。
      </NoticePanel>

      <SectionCard title="正在运行模拟盘列表" eyebrow={loading ? "加载中" : "点击组合查看完整统计"} action={<button className="pv2-button" onClick={load} disabled={loading} type="button">刷新</button>}>
        <PaperTable
          rows={rows}
          empty="暂无 READY / RUNNING / PAUSED 的 Paper v2 模拟盘组合。"
          columns={[
            { key: "name", header: "模拟盘", render: (row) => <><Link href={`/paper-v2/portfolios/${row.portfolio.portfolio_id}/live-dashboard`}>{row.portfolio.portfolio_name}</Link><br /><span className="pv2-muted pv2-mono">{shortHash(row.portfolio.portfolio_id)}</span></> },
            { key: "package", header: "策略包", render: (row) => <><Link href={`/paper-v2/portfolios/${row.portfolio.portfolio_id}/live-dashboard`}>{packageName(row.portfolio)}</Link><br /><span className="pv2-muted">{packageSource(row.portfolio)}</span></> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.portfolio.status} /> },
            { key: "latest", header: "最近运行/会话", render: (row) => <>{row.latestRun ? `${row.latestRun.trade_date} / ${row.latestRun.status}` : "未运行"}<br /><span className="pv2-muted">{row.latestSession ? `${row.latestSession.mode} / ${row.latestSession.status}` : "无会话"}</span></> },
            { key: "nav", header: "净值", render: (row) => formatNumber(latestSnapshot(row)?.nav || row.portfolio.initial_cash, 2) },
            { key: "ret", header: "累计收益", render: (row) => formatPercent(totalReturn(row)) },
            { key: "cash", header: "现金 / 市值", render: (row) => <>{formatNumber(latestSnapshot(row)?.cash, 2)}<br /><span className="pv2-muted">{formatNumber(latestSnapshot(row)?.market_value, 2)}</span></> },
            { key: "counts", header: "订单/成交/持仓", render: (row) => `${row.orders.length} / ${row.fills.length} / ${latestPositions(row).length}` },
            { key: "errors", header: "错误", render: (row) => row.errors.length ? <StatusBadge status="FAILED" /> : <StatusBadge status="PASSED" /> },
            { key: "actions", header: "操作", render: (row) => <div className="pv2-row-actions"><Link className="pv2-link-button" href={`/paper-v2/portfolios/${row.portfolio.portfolio_id}/live-dashboard`}>实时详情</Link><Link className="pv2-link-button" href={`/paper-v2/portfolios/${row.portfolio.portfolio_id}`}>统计</Link><Link className="pv2-link-button" href={`/paper-v2/portfolios/${row.portfolio.portfolio_id}/ledger`}>交易</Link><Link className="pv2-link-button" href={`/paper-v2/portfolios/${row.portfolio.portfolio_id}/performance`}>收益</Link></div> },
          ]}
        />
      </SectionCard>

      <div className="pv2-grid pv2-grid-2">
        {rows.map((row) => (
          <SectionCard key={row.portfolio.portfolio_id} title={row.portfolio.portfolio_name} eyebrow={`策略包 ${packageName(row.portfolio)} / 启动日 ${row.portfolio.start_date}`} action={<Link className="pv2-button" href={`/paper-v2/portfolios/${row.portfolio.portfolio_id}/live-dashboard`}>打开实时详情</Link>}>
            <div className="pv2-grid pv2-grid-4">
              <MetricCard label="净值" value={formatNumber(latestSnapshot(row)?.nav || row.portfolio.initial_cash, 2)} />
              <MetricCard label="收益率" value={formatPercent(totalReturn(row))} tone={(totalReturn(row) || 0) >= 0 ? "success" : "danger"} />
              <MetricCard label="持仓数" value={latestPositions(row).length} />
              <MetricCard label="错误" value={row.errors.length} tone={row.errors.length ? "danger" : "success"} />
            </div>
            <div className="pv2-card" style={{ marginTop: 12 }}>
              <div className="pv2-eyebrow">净值曲线</div>
              <div className="pv2-sparkline">
                {navSeries(row).map((item, index) => <div className="pv2-spark-bar" key={`${row.portfolio.portfolio_id}-${item.trade_date}-${index}`} style={{ height: `${item.pct}%` }} title={`${item.trade_date}: ${formatNumber(item.nav, 2)}`} />)}
              </div>
            </div>
            <PaperTable
              rows={latestPositions(row).slice(0, 8)}
              empty="暂无当前持仓。"
              columns={[
                { key: "symbol", header: "股票", render: (item) => String(item.symbol || "-") },
                { key: "qty", header: "数量", render: (item) => formatNumber(item.quantity, 0) },
                { key: "cost", header: "成本", render: (item) => formatNumber(item.avg_cost, 4) },
                { key: "value", header: "市值", render: (item) => formatNumber(item.market_value, 2) },
              ]}
            />
          </SectionCard>
        ))}
      </div>
    </main>
  );
}
