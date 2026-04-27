"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { paperV2Api, selectionCenterApi, strategyPackageApi } from "@/lib/paper-v2/api";
import { formatCompact, shortHash } from "@/lib/paper-v2/format";
import type { JsonObject, PaperPortfolio, PaperRun, SelectablePackage, StrategyPackage } from "@/lib/paper-v2/types";

type PortfolioSummary = {
  portfolio: PaperPortfolio;
  latestRun?: PaperRun;
  errorCount: number;
  latestSnapshot?: JsonObject;
};

export default function PaperV2OverviewPage() {
  const [packages, setPackages] = useState<StrategyPackage[]>([]);
  const [selectable, setSelectable] = useState<SelectablePackage[]>([]);
  const [summaries, setSummaries] = useState<PortfolioSummary[]>([]);
  const [detailLoadErrors, setDetailLoadErrors] = useState<JsonObject[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<unknown>(null);

  useEffect(() => {
    let alive = true;
    async function load() {
      setLoading(true);
      setError(null);
      try {
        const [pkgRows, selectableRows, portfolioRows] = await Promise.all([
          strategyPackageApi.list(undefined, 200),
          selectionCenterApi.selectablePackages(300),
          paperV2Api.listPortfolios(200),
        ]);
        const detailErrors: JsonObject[] = [];
        const portfolioSummaries = await Promise.all(portfolioRows.map(async (portfolio) => {
          const [runs, errors, snapshots] = await Promise.all([
            paperV2Api.runs(portfolio.portfolio_id).catch((exc) => { detailErrors.push({ portfolio_id: portfolio.portfolio_id, endpoint: "runs", message: String(exc) }); return []; }),
            paperV2Api.errors(portfolio.portfolio_id).catch((exc) => { detailErrors.push({ portfolio_id: portfolio.portfolio_id, endpoint: "errors", message: String(exc) }); return []; }),
            paperV2Api.snapshots(portfolio.portfolio_id).catch((exc) => { detailErrors.push({ portfolio_id: portfolio.portfolio_id, endpoint: "daily-snapshots", message: String(exc) }); return []; }),
          ]);
          return { portfolio, latestRun: runs[0], errorCount: errors.length, latestSnapshot: snapshots[0] };
        }));
        if (alive) {
          setPackages(pkgRows);
          setSelectable(selectableRows);
          setSummaries(portfolioSummaries);
          setDetailLoadErrors(detailErrors);
        }
      } catch (exc) {
        if (alive) setError(exc);
      } finally {
        if (alive) setLoading(false);
      }
    }
    load();
    return () => { alive = false; };
  }, []);

  const readyPackages = packages.filter((item) => ["SELECTION_ENABLED", "PAPER_ENABLED", "PAPER_RUNNING", "PAPER_PASSED"].includes(item.package_status)).length;
  const activePortfolios = summaries.filter(({ portfolio }) => ["READY", "RUNNING", "PAUSED"].includes(portfolio.status)).length;
  const blockingErrors = summaries.reduce((total, item) => total + item.errorCount, 0);
  const latestRuns = summaries.filter((item) => item.latestRun).length;
  const staleModels = useMemo(() => selectable.filter((item) => String(item.model_state?.staleness_status || "").includes("STALE")).length, [selectable]);

  return (
    <main>
      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="可用策略包" value={readyPackages} hint={`共 ${packages.length} 个策略包`} tone="success" />
        <MetricCard label="可选策略包" value={selectable.length} hint={`${staleModels} 个模型过期提醒`} tone={staleModels ? "warning" : "success"} />
        <MetricCard label="运行中组合" value={activePortfolios} hint={`共 ${summaries.length} 个组合`} tone="info" />
        <MetricCard label="阻断错误" value={blockingErrors} hint="可见 fail-fast 问题" tone={blockingErrors ? "danger" : "success"} />
      </div>

      <ErrorPanel error={error} title="总览加载失败" />
      {detailLoadErrors.length ? (
        <NoticePanel title="部分组合详情加载失败" tone="warning" context={detailLoadErrors}>
          总览加载期间部分组合详情接口失败。页面不会把失败当成空成功；请打开受影响组合或查看上下文。
        </NoticePanel>
      ) : null}

      <SectionCard title="流程看板" eyebrow="v2 正确流程" action={<Link className="pv2-button" href="/paper-v2/selection">运行选股</Link>}>
        <div className="pv2-grid pv2-grid-4">
          <MetricCard label="1. 策略包已启用" value={readyPackages} hint="可进入选股/模拟盘" />
          <MetricCard label="2. 选股可执行" value={selectable.length} hint="策略包选股中心" />
          <MetricCard label="3. 组合已就绪" value={activePortfolios} hint="已冻结组合合约" />
          <MetricCard label="4. 运行已记录" value={latestRuns} hint="就绪检查/单日运行/回放" />
        </div>
      </SectionCard>

      <SectionCard title="正在运行模拟盘监控" eyebrow="资金 / 持仓 / 交易 / 收益" action={<Link className="pv2-button-primary" href="/paper-v2/running">打开运行监控</Link>}>
        <NoticePanel title="独立运行视图" tone="info">
          运行监控页汇总 READY / RUNNING / PAUSED 的 Paper v2 组合，并展示启动以来的订单、成交、当前资金、持仓、净值和收益曲线入口。
        </NoticePanel>
        <PaperTable
          rows={summaries.filter(({ portfolio }) => ["READY", "RUNNING", "PAUSED"].includes(portfolio.status)).slice(0, 6)}
          empty="暂无正在运行或待运行的模拟盘组合。"
          columns={[
            { key: "name", header: "模拟盘", render: ({ portfolio }) => <Link href={`/paper-v2/portfolios/${portfolio.portfolio_id}`}>{portfolio.portfolio_name}</Link> },
            { key: "status", header: "状态", render: ({ portfolio }) => <StatusBadge status={portfolio.status} /> },
            { key: "nav", header: "最新净值", render: ({ latestSnapshot, portfolio }) => formatCompact(latestSnapshot?.nav || portfolio.initial_cash) },
            { key: "cash", header: "现金", render: ({ latestSnapshot }) => formatCompact(latestSnapshot?.cash) },
            { key: "run", header: "最近运行", render: ({ latestRun }) => latestRun ? `${latestRun.trade_date} / ${latestRun.status}` : "尚未运行" },
            { key: "action", header: "统计", render: ({ portfolio }) => <Link className="pv2-link-button" href={`/paper-v2/portfolios/${portfolio.portfolio_id}`}>查看完整统计</Link> },
          ]}
        />
      </SectionCard>

      <SectionCard title="活跃模拟组合" eyebrow={loading ? "加载中" : "模拟盘 v2"} action={<Link className="pv2-button" href="/paper-v2/portfolios">打开中心</Link>}>
        <PaperTable
          rows={summaries}
          empty="暂无模拟盘 v2 组合。请从 StrategyPackage 或单策略包选股运行创建。"
          columns={[
            { key: "name", header: "组合", render: ({ portfolio }) => <Link href={`/paper-v2/portfolios/${portfolio.portfolio_id}`}>{portfolio.portfolio_name}</Link> },
            { key: "status", header: "状态", render: ({ portfolio }) => <StatusBadge status={portfolio.status} /> },
            { key: "package", header: "策略包", render: ({ portfolio }) => <span className="pv2-mono">{shortHash(portfolio.package_id, 6)}</span> },
            { key: "cash", header: "初始资金", render: ({ portfolio }) => formatCompact(portfolio.initial_cash) },
            { key: "run", header: "最近运行", render: ({ latestRun }) => latestRun ? <><StatusBadge status={latestRun.status} /> <span className="pv2-muted">{latestRun.trade_date}</span></> : <span className="pv2-muted">尚未运行</span> },
            { key: "errors", header: "错误", render: ({ errorCount }) => errorCount ? <StatusBadge status="FAILED" /> : <StatusBadge status="PASSED" /> },
            { key: "action", header: "操作", render: ({ portfolio }) => <Link className="pv2-link-button" href={`/paper-v2/portfolios/${portfolio.portfolio_id}/run-console`}>运行控制台</Link> },
          ]}
        />
      </SectionCard>
    </main>
  );
}
