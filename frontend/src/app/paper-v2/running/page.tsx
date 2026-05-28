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
import {
  latestSnapshot,
  n,
  packageName,
  packageSource,
  parseRunningSummaryItem,
  RUNNING_SEARCH_FIELD_OPTIONS,
  RUNNING_SORT_OPTIONS,
  RUNNING_STATUS_OPTIONS,
  runningScenario,
  statusFilterToStatuses,
  totalReturn,
  type RunningPortfolioSummary,
} from "@/lib/paper-v2/running-summary";
import type { JsonObject, RunningSummaryPagination, RunningSummarySortBy, RunningSummarySortDir } from "@/lib/paper-v2/types";

function cashParam(value: string): number | null {
  if (!value.trim()) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function latestPositions(row: RunningPortfolioSummary): JsonObject[] {
  return row.latestPositions;
}

function navSeries(row: RunningPortfolioSummary) {
  const ordered = [...row.recentSnapshots].reverse();
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
  const [pagination, setPagination] = useState<RunningSummaryPagination | null>(null);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [statusFilter, setStatusFilter] = useState("ACTIVE");
  const [sortBy, setSortBy] = useState<RunningSummarySortBy>("latest_run_time");
  const [sortDir, setSortDir] = useState<RunningSummarySortDir>("desc");
  const [searchField, setSearchField] = useState("all");
  const [search, setSearch] = useState("");
  const [minCash, setMinCash] = useState("");
  const [maxCash, setMaxCash] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const pageData = await paperV2Api.runningSummaryPage({
        page,
        pageSize,
        snapshotLimit: 30,
        positionLimit: 8,
        statuses: statusFilterToStatuses(statusFilter),
        sortBy,
        sortDir,
        search,
        searchFields: [searchField],
        minInitialCash: cashParam(minCash),
        maxInitialCash: cashParam(maxCash),
      });
      setRows(pageData.summaries.map(parseRunningSummaryItem));
      setPagination(pageData.pagination);
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [maxCash, minCash, page, pageSize, search, searchField, sortBy, sortDir, statusFilter]);

  useEffect(() => { load(); }, [load]);

  const totals = useMemo(() => {
    const nav = rows.reduce((sum, row) => sum + n(latestSnapshot(row)?.nav || row.portfolio.initial_cash), 0);
    const initial = rows.reduce((sum, row) => sum + n(row.portfolio.initial_cash), 0);
    const errors = rows.reduce((sum, row) => sum + row.counts.errors, 0);
    const fills = rows.reduce((sum, row) => sum + row.counts.fills, 0);
    return { nav, initial, pnl: nav - initial, returnRate: initial ? nav / initial - 1 : null, errors, fills };
  }, [rows]);

  const totalPages = Math.max(1, pagination?.total_pages || 1);
  const pageStart = pagination?.total ? (page - 1) * pageSize + 1 : 0;
  const pageEnd = pagination?.total ? Math.min(page * pageSize, pagination.total) : 0;

  function resetPage(next: () => void) {
    setPage(1);
    next();
  }

  return (
    <main>
      <ErrorPanel error={error} title="运行/追赶模拟盘汇总加载失败" />
      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="运行/暂停模拟盘" value={pagination?.total ?? rows.length} hint="不包含 READY 未就绪模拟盘" tone="info" />
        <MetricCard label="本页总净值" value={formatCompact(totals.nav)} hint={`初始资金 ${formatCompact(totals.initial)}`} tone="success" />
        <MetricCard label="本页累计收益" value={formatNumber(totals.pnl, 2)} hint={formatPercent(totals.returnRate)} tone={totals.pnl >= 0 ? "success" : "danger"} />
        <MetricCard label="本页阻断错误" value={totals.errors} hint={`${totals.fills} 条成交记录`} tone={totals.errors ? "danger" : "success"} />
      </div>

      <NoticePanel title="运行状态语义" tone="info">
        READY 显示为未就绪/未运行，不再归入正在运行；本页默认只展示 RUNNING / PAUSED。运行任务分为仅历史追赶、历史追赶后自动实时、完全实时三种场景，盘中也允许恢复、启动或切换，实际下单仍由运行时和 broker fail-fast 校验。
      </NoticePanel>

      <SectionCard title="运行/追赶模拟盘列表" eyebrow={loading ? "加载中" : `${pageStart}-${pageEnd} / ${pagination?.total || 0} 个模拟盘`} action={<button className="pv2-button" onClick={load} disabled={loading} type="button">刷新</button>}>
        <div className="pv2-card pv2-filter-card">
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>状态筛选</label><select className="pv2-select" value={statusFilter} onChange={(event) => resetPage(() => setStatusFilter(event.target.value))}>{RUNNING_STATUS_OPTIONS.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}</select></div>
            <div className="pv2-field"><label>排序字段</label><select className="pv2-select" value={sortBy} onChange={(event) => resetPage(() => setSortBy(event.target.value as RunningSummarySortBy))}>{RUNNING_SORT_OPTIONS.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}</select></div>
            <div className="pv2-field"><label>排序方向</label><select className="pv2-select" value={sortDir} onChange={(event) => resetPage(() => setSortDir(event.target.value as RunningSummarySortDir))}><option value="desc">降序</option><option value="asc">升序</option></select></div>
            <div className="pv2-field"><label>筛选字段</label><select className="pv2-select" value={searchField} onChange={(event) => resetPage(() => setSearchField(event.target.value))}>{RUNNING_SEARCH_FIELD_OPTIONS.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}</select></div>
            <div className="pv2-field"><label>字段关键字</label><input className="pv2-input" value={search} placeholder="支持模拟盘ID、策略包、状态、数据源等" onChange={(event) => resetPage(() => setSearch(event.target.value))} /></div>
            <div className="pv2-field"><label>每页数量</label><select className="pv2-select" value={pageSize} onChange={(event) => resetPage(() => setPageSize(Number(event.target.value)))}><option value={20}>20</option><option value={30}>30</option><option value={50}>50</option></select></div>
            <div className="pv2-field"><label>初始资金下限</label><input className="pv2-input" type="number" min={0} value={minCash} onChange={(event) => resetPage(() => setMinCash(event.target.value))} /></div>
            <div className="pv2-field"><label>初始资金上限</label><input className="pv2-input" type="number" min={0} value={maxCash} onChange={(event) => resetPage(() => setMaxCash(event.target.value))} /></div>
          </div>
        </div>
        <PaperTable
          rows={rows}
          empty="暂无符合条件的 RUNNING / PAUSED Paper v2 模拟盘；READY 组合请手动选择未就绪状态筛选。"
          columns={[
            { key: "name", header: "模拟盘", render: (row) => <><Link href={`/paper-v2/portfolios/${row.portfolio.portfolio_id}/live-dashboard`}>{row.portfolio.portfolio_name}</Link><br /><span className="pv2-muted pv2-mono">{shortHash(row.portfolio.portfolio_id)}</span></> },
            { key: "package", header: "策略包", render: (row) => <><Link href={`/paper-v2/portfolios/${row.portfolio.portfolio_id}/live-dashboard`}>{packageName(row.portfolio)}</Link><br /><span className="pv2-muted">{packageSource(row.portfolio)}</span></> },
            { key: "status", header: "模拟盘状态", render: (row) => <StatusBadge status={row.portfolio.status} /> },
            { key: "scenario", header: "运行场景", render: (row) => { const scenario = runningScenario(row); return <>{scenario.label}<br /><span className="pv2-muted">{scenario.hint}</span></>; } },
            { key: "latest", header: "最近运行/会话", render: (row) => <>{row.latestRun ? <>{row.latestRun.trade_date} / <StatusBadge status={row.latestRun.status} /></> : "未运行"}<br /><span className="pv2-muted">{row.latestSession ? <><StatusBadge status={row.latestSession.mode} /> / <StatusBadge status={row.latestSession.status} /></> : "无会话"}</span></> },
            { key: "nav", header: "净值", render: (row) => formatNumber(latestSnapshot(row)?.nav || row.portfolio.initial_cash, 2) },
            { key: "ret", header: "累计收益", render: (row) => formatPercent(totalReturn(row)) },
            { key: "cash", header: "现金 / 市值", render: (row) => <>{formatNumber(latestSnapshot(row)?.cash, 2)}<br /><span className="pv2-muted">{formatNumber(latestSnapshot(row)?.market_value, 2)}</span></> },
            { key: "counts", header: "订单/成交/持仓", render: (row) => `${row.counts.orders} / ${row.counts.fills} / ${row.counts.positions}` },
            { key: "errors", header: "错误", render: (row) => row.counts.errors ? <StatusBadge status="FAILED" /> : <StatusBadge status="PASSED" /> },
            { key: "actions", header: "操作", render: (row) => <div className="pv2-row-actions"><Link className="pv2-link-button" href={`/paper-v2/portfolios/${row.portfolio.portfolio_id}/run-console`}>运行控制</Link><Link className="pv2-link-button" href={`/paper-v2/portfolios/${row.portfolio.portfolio_id}/live-dashboard`}>实时详情</Link><Link className="pv2-link-button" href={`/paper-v2/portfolios/${row.portfolio.portfolio_id}/ledger`}>交易</Link><Link className="pv2-link-button" href={`/paper-v2/portfolios/${row.portfolio.portfolio_id}/performance`}>收益</Link></div> },
          ]}
        />
        <div className="pv2-pagination">
          <button className="pv2-button-ghost" disabled={page <= 1 || loading} onClick={() => setPage((current) => Math.max(1, current - 1))} type="button">上一页</button>
          <span className="pv2-muted">第 {page} / {totalPages} 页，每页最多 50 个</span>
          <button className="pv2-button-ghost" disabled={page >= totalPages || loading} onClick={() => setPage((current) => current + 1)} type="button">下一页</button>
        </div>
      </SectionCard>

      <div className="pv2-grid pv2-grid-2">
        {rows.map((row) => (
          <SectionCard key={row.portfolio.portfolio_id} title={row.portfolio.portfolio_name} eyebrow={`策略包 ${packageName(row.portfolio)} / 启动日 ${row.portfolio.start_date}`} action={<Link className="pv2-button" href={`/paper-v2/portfolios/${row.portfolio.portfolio_id}/live-dashboard`}>打开实时详情</Link>}>
            <div className="pv2-grid pv2-grid-4">
              <MetricCard label="净值" value={formatNumber(latestSnapshot(row)?.nav || row.portfolio.initial_cash, 2)} />
              <MetricCard label="收益率" value={formatPercent(totalReturn(row))} tone={(totalReturn(row) || 0) >= 0 ? "success" : "danger"} />
              <MetricCard label="持仓数" value={latestPositions(row).length} />
              <MetricCard label="错误" value={row.counts.errors} tone={row.counts.errors ? "danger" : "success"} />
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
