"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import JsonPanel from "@/components/paper-v2/JsonPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { paperV2Api } from "@/lib/paper-v2/api";
import { formatCompact, shortHash } from "@/lib/paper-v2/format";
import type { Activation, JsonObject, PaperPortfolio, PaperRun } from "@/lib/paper-v2/types";

function asDate(value: unknown): string {
  if (!value) return "-";
  return String(value).slice(0, 19).replace("T", " ");
}

export default function PaperV2PortfolioDetailPage() {
  const params = useParams<{ portfolioId: string }>();
  const portfolioId = String(params.portfolioId || "");
  const [portfolio, setPortfolio] = useState<PaperPortfolio | null>(null);
  const [runs, setRuns] = useState<PaperRun[]>([]);
  const [errors, setErrors] = useState<JsonObject[]>([]);
  const [snapshots, setSnapshots] = useState<JsonObject[]>([]);
  const [activations, setActivations] = useState<Activation[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [portfolioRow, runRows, errorRows, snapshotRows, activationRows] = await Promise.all([
        paperV2Api.getPortfolio(portfolioId),
        paperV2Api.runs(portfolioId),
        paperV2Api.errors(portfolioId),
        paperV2Api.snapshots(portfolioId),
        paperV2Api.activations(portfolioId),
      ]);
      setPortfolio(portfolioRow);
      setRuns(runRows);
      setErrors(errorRows);
      setSnapshots(snapshotRows);
      setActivations(activationRows);
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [portfolioId]);

  useEffect(() => { load(); }, [load]);

  async function lifecycle(action: "pause" | "resume" | "complete" | "retire") {
    setError(null);
    try {
      await paperV2Api.lifecycle(portfolioId, action);
      await load();
    } catch (exc) {
      setError(exc);
    }
  }

  const latestSnapshot = snapshots[0];
  const latestRun = runs[0];

  return (
    <main>
      <div className="pv2-detail-nav">
        <Link href="/paper-v2/portfolios">组合中心</Link>
        <Link href={`/paper-v2/portfolios/${portfolioId}/run-console`}>运行控制台</Link>
        <Link href={`/paper-v2/portfolios/${portfolioId}/ledger`}>账本</Link>
        <Link href={`/paper-v2/portfolios/${portfolioId}/performance`}>绩效</Link>
      </div>
      <ErrorPanel error={error} title="组合详情加载失败" />

      {portfolio ? (
        <>
          <div className="pv2-grid pv2-grid-4">
            <MetricCard label="状态" value={portfolio.status} tone={portfolio.status === "READY" ? "success" : "warning"} />
            <MetricCard label="初始资金" value={formatCompact(portfolio.initial_cash)} />
            <MetricCard label="运行次数" value={runs.length} hint={latestRun ? `最近 ${latestRun.trade_date}` : "尚未运行"} tone="info" />
            <MetricCard label="错误" value={errors.length} tone={errors.length ? "danger" : "success"} />
          </div>

          <SectionCard
            title={portfolio.portfolio_name}
            eyebrow={loading ? "加载中" : "已冻结模拟盘 v2 组合"}
            action={
              <div className="pv2-row-actions">
                <button className="pv2-button" onClick={() => lifecycle(portfolio.status === "PAUSED" ? "resume" : "pause")} type="button">
                  {portfolio.status === "PAUSED" ? "恢复" : "暂停"}
                </button>
                <button className="pv2-button" onClick={() => lifecycle("complete")} type="button">完成</button>
                <button className="pv2-button-danger" onClick={() => lifecycle("retire")} type="button">退役</button>
              </div>
            }
          >
            <div className="pv2-chip-row">
              <span className="pv2-chip">portfolio_id: {shortHash(portfolio.portfolio_id)}</span>
              <span className="pv2-chip">package_id: {shortHash(portfolio.package_id)}</span>
              <span className="pv2-chip">manifest: {shortHash(portfolio.manifest_sha256)}</span>
              <span className="pv2-chip">data: {portfolio.data_source}</span>
              <span className="pv2-chip">start: {portfolio.start_date}</span>
            </div>
            <NoticePanel title="冻结合约" tone="info">
              package_id、manifest_sha256、initial_cash、start_date、data_source、fee_policy、risk_policy 和默认 execution_policy 都是不可变的组合创建事实。每日执行策略变更必须记录为带日期的激活记录。
            </NoticePanel>
          </SectionCard>

          <div className="pv2-grid pv2-grid-main">
            <SectionCard title="冻结策略" eyebrow="Manifest 锁定">
              <h3>执行策略</h3>
              <JsonPanel value={portfolio.execution_policy || {}} />
              <h3>费用策略</h3>
              <JsonPanel value={portfolio.fee_policy || {}} />
              <h3>风控策略</h3>
              <JsonPanel value={portfolio.risk_policy || {}} />
            </SectionCard>

            <SectionCard title="操作入口" eyebrow="下一步">
              <div className="pv2-grid pv2-grid-2">
                <Link className="pv2-button" href={`/paper-v2/portfolios/${portfolioId}/run-console`}>就绪检查 / 单日运行 / 回放</Link>
                <Link className="pv2-button" href={`/paper-v2/portfolios/${portfolioId}/ledger`}>订单 / 成交 / 持仓</Link>
                <Link className="pv2-button" href={`/paper-v2/portfolios/${portfolioId}/performance`}>绩效报告</Link>
                <Link className="pv2-button-ghost" href="/paper-v2/model-hmm">模型与 HMM 维护</Link>
              </div>
              <h3>最新快照</h3>
              {latestSnapshot ? <JsonPanel value={latestSnapshot} /> : <div className="pv2-muted">暂无日快照。</div>}
            </SectionCard>
          </div>

          <div className="pv2-grid pv2-grid-2">
            <SectionCard title="执行策略激活" eyebrow="按交易日">
              <PaperTable
                rows={activations}
                empty="暂无按日期激活的执行策略，将使用组合默认策略。"
                columns={[
                  { key: "date", header: "交易日期", render: (row) => row.trade_date },
                  { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
                  { key: "policy", header: "策略", render: (row) => row.policy_name || shortHash(row.policy_id) },
                  { key: "hash", header: "Hash", render: (row) => <span className="pv2-mono">{shortHash(row.policy_sha256)}</span> },
                  { key: "at", header: "激活时间", render: (row) => asDate(row.activated_at) },
                ]}
              />
            </SectionCard>

            <SectionCard title="最近运行与错误" eyebrow="可追溯">
              <PaperTable
                rows={runs.slice(0, 8)}
                empty="暂无运行。"
                columns={[
                  { key: "date", header: "日期", render: (row) => row.trade_date },
                  { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
                  { key: "run", header: "运行", render: (row) => <span className="pv2-mono">{shortHash(row.run_id)}</span> },
                ]}
              />
              <PaperTable
                rows={errors.slice(0, 6)}
                empty="暂无持久化错误。"
                columns={[
                  { key: "stage", header: "阶段", render: (row) => String(row.stage || row.error_stage || "-") },
                  { key: "code", header: "代码", render: (row) => <StatusBadge status={String(row.error_code || "error")} /> },
                  { key: "message", header: "消息", render: (row) => String(row.message || row.error_message || "-") },
                ]}
              />
            </SectionCard>
          </div>
        </>
      ) : !error ? (
        <SectionCard title="正在加载组合" eyebrow="模拟盘 v2">
          <div className="pv2-muted">正在加载 {portfolioId}...</div>
        </SectionCard>
      ) : null}
    </main>
  );
}
