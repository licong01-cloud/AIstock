"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import WorkflowStepper from "@/components/paper-v2/WorkflowStepper";
import { paperV2Api, selectionCenterApi, strategyPackageApi } from "@/lib/paper-v2/api";
import { dataSourceLabel, formatCompact, packageDisplayLabel, paperV2WorkflowSteps, shortHash } from "@/lib/paper-v2/format";
import {
  latestSnapshot,
  parseRunningSummaryItem,
  RUNNING_SEARCH_FIELD_OPTIONS,
  RUNNING_SORT_OPTIONS,
  RUNNING_STATUS_OPTIONS,
  statusFilterToStatuses,
  type RunningPortfolioSummary,
} from "@/lib/paper-v2/running-summary";
import type { RunningSummaryPagination, RunningSummarySortBy, RunningSummarySortDir, SelectablePackage, StrategyPackage, TradingDayStatus } from "@/lib/paper-v2/types";

function cashParam(value: string): number | null {
  if (!value.trim()) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function latestRunLabel(row: RunningPortfolioSummary): string {
  if (!row.latestRun) return "尚未运行";
  const time = row.latestRun.started_at || row.latestRun.completed_at || row.latestRun.trade_date;
  return `${row.latestRun.trade_date} / ${row.latestRun.status}${time ? ` / ${time}` : ""}`;
}

export default function PaperV2OverviewPage() {
  const [packages, setPackages] = useState<StrategyPackage[]>([]);
  const [selectable, setSelectable] = useState<SelectablePackage[]>([]);
  const [rows, setRows] = useState<RunningPortfolioSummary[]>([]);
  const [tradingDayStatus, setTradingDayStatus] = useState<TradingDayStatus | null>(null);
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
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [pkgRows, selectableRows, runningPage, tradingStatus] = await Promise.all([
        strategyPackageApi.list(undefined, 200),
        selectionCenterApi.selectablePackages(300),
        paperV2Api.runningSummaryPage({
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
        }),
        paperV2Api.tradingDayStatus(),
      ]);
      setPackages(pkgRows);
      setSelectable(selectableRows);
      setRows(runningPage.summaries.map(parseRunningSummaryItem));
      setPagination(runningPage.pagination);
      setTradingDayStatus(tradingStatus);
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [maxCash, minCash, page, pageSize, search, searchField, sortBy, sortDir, statusFilter]);

  useEffect(() => { load(); }, [load]);

  const readyPackages = packages.filter((item) => String(item.package_status || "").toUpperCase() !== "RETIRED").length;
  const activeTotal = pagination?.total ?? rows.length;
  const blockingErrors = rows.reduce((total, item) => total + item.counts.errors, 0);
  const latestRuns = rows.filter((item) => item.latestRun).length;
  const staleModels = useMemo(() => selectable.filter((item) => String(item.model_state?.staleness_status || "").includes("STALE")).length, [selectable]);
  const totalPages = Math.max(1, pagination?.total_pages || 1);
  const pageStart = pagination?.total ? (page - 1) * pageSize + 1 : 0;
  const pageEnd = pagination?.total ? Math.min(page * pageSize, pagination.total) : 0;

  function resetPage(next: () => void) {
    setPage(1);
    next();
  }

  const workflowSteps = paperV2WorkflowSteps({
    hasPackages: packages.length > 0,
    hasSelectionEnabledPackage: readyPackages > 0,
    hasPaperEnabledPackage: readyPackages > 0,
    hasSelectionRun: selectable.some((item) => item.latest_selection_run),
    hasPortfolio: rows.length > 0 || (pagination?.total ?? 0) > 0,
    hasReadyRun: latestRuns > 0,
  });

  return (
    <main>
      <WorkflowStepper steps={workflowSteps} />
      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="可用策略包" value={readyPackages} hint={`共 ${packages.length} 个策略包`} tone="success" />
        <MetricCard label="可选策略包" value={selectable.length} hint={`${staleModels} 个模型过期提醒`} tone={staleModels ? "warning" : "success"} />
        <MetricCard label="运行/暂停模拟盘" value={activeTotal} hint="RUNNING / PAUSED，READY 显示为未就绪" tone="info" />
        <MetricCard label="本页阻断错误" value={blockingErrors} hint="当前页 fail-fast 问题" tone={blockingErrors ? "danger" : "success"} />
      </div>

      <ErrorPanel error={error} title="总览加载失败" />

      <SectionCard title="官方交易日状态" eyebrow="market.trading_calendar / 文件缓存">
        <div className="pv2-grid pv2-grid-4">
          <MetricCard label="今日状态" value={tradingDayStatus?.is_trading_day ? "交易日" : "非交易日"} hint={tradingDayStatus?.as_of_date || "-"} tone={tradingDayStatus?.is_trading_day ? "success" : "warning"} />
          <MetricCard label="最近结束交易日" value={tradingDayStatus?.latest_completed_trading_day || "-"} hint="统一交易日服务返回" />
          <MetricCard label="最快下个交易日" value={tradingDayStatus?.next_trading_day || "-"} hint="禁止本地周末推断兜底" />
          <MetricCard label="缓存行数" value={String(tradingDayStatus?.cache?.calendar_row_count ?? "-")} hint={String(tradingDayStatus?.cache?.refresh_reason || "file cache")} tone="info" />
        </div>
        {tradingDayStatus?.warnings?.length ? (
          <NoticePanel title="交易日表覆盖范围警告" tone="warning">
            {tradingDayStatus.warnings.map((warning, index) => (
              <div key={`${String(warning.code || "warning")}-${index}`}>{String(warning.message || warning.code || "交易日表需要更新")}</div>
            ))}
          </NoticePanel>
        ) : null}
      </SectionCard>

      <SectionCard title="流程看板" eyebrow="v2 正确流程" action={<Link className="pv2-button" href="/paper-v2/selection">运行选股</Link>}>
        <div className="pv2-grid pv2-grid-4">
          <MetricCard label="1. 资产合格策略包" value={readyPackages} hint="资产合格即可进入选股/模拟盘" />
          <MetricCard label="2. 选股可执行" value={selectable.length} hint="策略包选股中心" />
          <MetricCard label="3. 模拟盘已就绪" value={activeTotal} hint="后端分页统计" />
          <MetricCard label="4. 本页运行记录" value={latestRuns} hint="最近运行/回放" />
        </div>
      </SectionCard>

      <SectionCard title="运行/追赶模拟盘监控" eyebrow="资金 / 持仓 / 交易 / 收益" action={<Link className="pv2-button-primary" href="/paper-v2/running">打开运行监控</Link>}>
        <NoticePanel title="已改为单接口聚合" tone="info">
          总览不再按组合扇出 runs/errors/snapshots 请求；当前表格由 /paper-v2/running-summary 一次性返回，并在数据库侧分页、排序和筛选。
        </NoticePanel>
        <PaperTable
          rows={rows.slice(0, 6)}
          empty="暂无正在运行或待运行的模拟盘。"
          columns={[
            { key: "name", header: "模拟盘", render: ({ portfolio }) => <Link href={`/paper-v2/portfolios/${portfolio.portfolio_id}`}>{portfolio.portfolio_name}</Link> },
            { key: "status", header: "状态", render: ({ portfolio }) => <StatusBadge status={portfolio.status} /> },
            { key: "nav", header: "最新净值", render: (row) => formatCompact(latestSnapshot(row)?.nav || row.portfolio.initial_cash) },
            { key: "cash", header: "现金", render: (row) => formatCompact(latestSnapshot(row)?.cash) },
            { key: "run", header: "最近运行", render: (row) => latestRunLabel(row) },
            { key: "action", header: "统计", render: ({ portfolio }) => <Link className="pv2-link-button" href={`/paper-v2/portfolios/${portfolio.portfolio_id}`}>查看完整统计</Link> },
          ]}
        />
      </SectionCard>

      <SectionCard title="运行/暂停模拟盘" eyebrow={loading ? "加载中" : `${pageStart}-${pageEnd} / ${pagination?.total || 0} 个模拟盘`} action={<button className="pv2-button" onClick={load} disabled={loading} type="button">刷新</button>}>
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
          empty="暂无符合条件的 Paper v2 活跃模拟盘。"
          columns={[
            { key: "name", header: "模拟盘", render: ({ portfolio }) => <Link href={`/paper-v2/portfolios/${portfolio.portfolio_id}`}>{portfolio.portfolio_name}</Link> },
            { key: "status", header: "状态", render: ({ portfolio }) => <StatusBadge status={portfolio.status} /> },
            { key: "package", header: "策略包", render: ({ portfolio }) => {
              const pkg = packages.find((item) => item.package_id === portfolio.package_id);
              const label = pkg ? packageDisplayLabel(pkg) : shortHash(portfolio.package_id, 7);
              return <span title={String(portfolio.package_id)}>{label}</span>;
            } },
            { key: "cash", header: "初始资金", render: ({ portfolio }) => formatCompact(portfolio.initial_cash) },
            { key: "source", header: "数据源", render: ({ portfolio }) => dataSourceLabel(portfolio.data_source) },
            { key: "run", header: "最近运行", render: (row) => row.latestRun ? <><StatusBadge status={row.latestRun.status} /> <span className="pv2-muted">{row.latestRun.trade_date}</span></> : <span className="pv2-muted">尚未运行</span> },
            { key: "errors", header: "错误", render: ({ counts }) => counts.errors ? <StatusBadge status="FAILED" /> : <StatusBadge status="PASSED" /> },
            { key: "action", header: "操作", render: ({ portfolio }) => <Link className="pv2-link-button" href={`/paper-v2/portfolios/${portfolio.portfolio_id}/run-console`}>运行控制台</Link> },
          ]}
        />
        <div className="pv2-pagination">
          <button className="pv2-button-ghost" disabled={page <= 1 || loading} onClick={() => setPage((current) => Math.max(1, current - 1))} type="button">上一页</button>
          <span className="pv2-muted">第 {page} / {totalPages} 页，每页最多 50 个</span>
          <button className="pv2-button-ghost" disabled={page >= totalPages || loading} onClick={() => setPage((current) => current + 1)} type="button">下一页</button>
        </div>
      </SectionCard>
    </main>
  );
}
