"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import JsonPanel from "@/components/paper-v2/JsonPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import { paperV2Api } from "@/lib/paper-v2/api";
import { formatNumber, formatPercent, shortHash } from "@/lib/paper-v2/format";
import type { JsonObject, PaperPortfolio } from "@/lib/paper-v2/types";

function text(row: JsonObject, key: string): string {
  const value = row[key];
  if (value === null || value === undefined || value === "") return "-";
  return String(value);
}

export default function PaperV2PerformancePage() {
  const params = useParams<{ portfolioId: string }>();
  const portfolioId = String(params.portfolioId || "");
  const [portfolio, setPortfolio] = useState<PaperPortfolio | null>(null);
  const [report, setReport] = useState<JsonObject | null>(null);
  const [snapshots, setSnapshots] = useState<JsonObject[]>([]);
  const [error, setError] = useState<unknown>(null);
  const [reportError, setReportError] = useState<unknown>(null);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    setReportError(null);
    try {
      const [portfolioRow, snapshotRows] = await Promise.all([
        paperV2Api.getPortfolio(portfolioId),
        paperV2Api.snapshots(portfolioId),
      ]);
      setPortfolio(portfolioRow);
      setSnapshots(snapshotRows);
      if (snapshotRows.length) {
        try {
          setReport(await paperV2Api.performance(portfolioId));
        } catch (exc) {
          setReport(null);
          setReportError(exc);
        }
      } else {
        setReport(null);
        setReportError(null);
      }
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [portfolioId]);

  useEffect(() => { load(); }, [load]);

  const dailyReturns = Array.isArray(report?.daily_returns) ? report.daily_returns as JsonObject[] : [];
  const navs = useMemo(() => {
    const values = dailyReturns.map((row) => Number(row.nav)).filter((value) => Number.isFinite(value));
    const min = Math.min(...values);
    const max = Math.max(...values);
    return dailyReturns.map((row) => {
      const nav = Number(row.nav);
      const pct = Number.isFinite(nav) && Number.isFinite(min) && Number.isFinite(max) && max > min ? ((nav - min) / (max - min)) * 100 : 50;
      return { trade_date: row.trade_date, nav, pct };
    });
  }, [dailyReturns]);

  const insufficient = Array.isArray(report?.insufficient_data_reasons) ? report.insufficient_data_reasons as string[] : [];

  return (
    <main>
      <div className="pv2-detail-nav">
        <Link href={`/paper-v2/portfolios/${portfolioId}`}>详情</Link>
        <Link href={`/paper-v2/portfolios/${portfolioId}/live-dashboard`}>实时详情</Link>
        <Link href={`/paper-v2/portfolios/${portfolioId}/run-console`}>运行控制台</Link>
        <Link href={`/paper-v2/portfolios/${portfolioId}/ledger`}>账本</Link>
      </div>
      <ErrorPanel error={error} title="绩效加载失败" />
      <ErrorPanel error={reportError} title="绩效报告不可用" />

      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="总收益率" value={formatPercent(report?.total_return)} tone={Number(report?.total_return || 0) >= 0 ? "success" : "danger"} />
        <MetricCard label="年化收益" value={formatPercent(report?.annualized_return)} />
        <MetricCard label="夏普比率" value={formatNumber(report?.sharpe, 2)} />
        <MetricCard label="最大回撤" value={formatPercent(report?.max_drawdown)} tone={Number(report?.max_drawdown || 0) < 0 ? "warning" : "success"} />
      </div>

      <SectionCard title="绩效报告" eyebrow={loading ? "加载中" : portfolio?.portfolio_name || shortHash(portfolioId)} action={<button className="pv2-button" onClick={load} type="button">刷新</button>}>
        {report ? (
          <>
            <div className="pv2-grid pv2-grid-4">
              <MetricCard label="最终净值" value={formatNumber(report.final_nav, 2)} />
              <MetricCard label="年化波动率" value={formatPercent(report.annualized_volatility)} />
              <MetricCard label="平均日收益" value={formatPercent(report.avg_daily_return)} />
              <MetricCard label="盈利天数占比" value={formatPercent(report.win_day_ratio)} />
            </div>
            {insufficient.length ? (
              <NoticePanel title="数据不足说明" tone="warning">
                {insufficient.join("; ")}
              </NoticePanel>
            ) : null}
            <div className="pv2-card" style={{ marginTop: 14 }}>
              <div className="pv2-eyebrow">净值曲线</div>
              <div className="pv2-sparkline">
                {navs.map((item, index) => (
                  <div className="pv2-spark-bar" key={`${item.trade_date}-${index}`} style={{ height: `${Math.max(8, item.pct)}%` }} title={`${item.trade_date}: ${formatNumber(item.nav, 2)}`} />
                ))}
              </div>
            </div>
          </>
        ) : (
          <NoticePanel title="暂无绩效报告" tone="warning">
            绩效报告至少需要一个已持久化的日快照。请先执行单日运行或历史回放。
          </NoticePanel>
        )}
      </SectionCard>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="日收益" eyebrow="报告输出">
          <PaperTable
            rows={dailyReturns}
            empty="暂无日收益记录。"
            columns={[
              { key: "date", header: "日期", render: (row) => text(row, "trade_date") },
              { key: "nav", header: "净值", render: (row) => formatNumber(row.nav, 2) },
              { key: "return", header: "日收益", render: (row) => formatPercent(row.daily_return) },
            ]}
          />
        </SectionCard>

        <SectionCard title="持久化快照" eyebrow="来源记录">
          <PaperTable
            rows={snapshots}
            empty="暂无持久化日快照。"
            columns={[
              { key: "date", header: "日期", render: (row) => text(row, "trade_date") },
              { key: "cash", header: "现金", render: (row) => formatNumber(row.cash, 2) },
              { key: "market", header: "市值", render: (row) => formatNumber(row.market_value, 2) },
              { key: "nav", header: "净值", render: (row) => formatNumber(row.nav, 2) },
              { key: "positions", header: "持仓数", render: (row) => formatNumber(row.position_count, 0) },
            ]}
          />
        </SectionCard>
      </div>

      {report ? (
        <SectionCard title="原始报告 ??" eyebrow="调试追踪">
          <JsonPanel value={report} />
        </SectionCard>
      ) : null}
    </main>
  );
}
