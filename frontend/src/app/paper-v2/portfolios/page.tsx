"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import JsonPanel from "@/components/paper-v2/JsonPanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { paperV2Api, strategyPackageApi } from "@/lib/paper-v2/api";
import { formatCompact, shortHash, todayIso } from "@/lib/paper-v2/format";
import type { DataSource, ExecutionPolicy, PaperPortfolio, StrategyPackage } from "@/lib/paper-v2/types";

export default function PaperV2PortfoliosPage() {
  const [portfolios, setPortfolios] = useState<PaperPortfolio[]>([]);
  const [packages, setPackages] = useState<StrategyPackage[]>([]);
  const [policies, setPolicies] = useState<ExecutionPolicy[]>([]);
  const [packageId, setPackageId] = useState("");
  const [name, setName] = useState("模拟盘 v2 组合");
  const [initialCash, setInitialCash] = useState(1000000);
  const [startDate, setStartDate] = useState(todayIso());
  const [dataSource, setDataSource] = useState<DataSource>("DB_HISTORICAL");
  const [policyId, setPolicyId] = useState("");
  const [created, setCreated] = useState<PaperPortfolio | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(false);

  const selectedPackage = useMemo(() => packages.find((item) => item.package_id === packageId), [packages, packageId]);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [portfolioRows, packageRows] = await Promise.all([
        paperV2Api.listPortfolios(300),
        strategyPackageApi.list(undefined, 300),
      ]);
      setPortfolios(portfolioRows);
      setPackages(packageRows);
      const initialPackage = typeof window !== "undefined" ? new URLSearchParams(window.location.search).get("package_id") : null;
      if (!packageId) setPackageId(initialPackage || packageRows[0]?.package_id || "");
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [packageId]);

  useEffect(() => { load(); }, [load]);

  useEffect(() => {
    if (!packageId) return;
    strategyPackageApi.executionPolicies(packageId).then((rows) => {
      setPolicies(rows);
      const paperReady = rows.find((item) => item.paper_enabled);
      setPolicyId(paperReady?.policy_id || rows[0]?.policy_id || "");
    }).catch((exc) => {
      setPolicies([]);
      setError(exc);
    });
  }, [packageId]);

  async function createPortfolio() {
    setError(null);
    setCreated(null);
    try {
      if (!packageId) throw new Error("请先选择 StrategyPackage。");
      const portfolio = await paperV2Api.createPortfolio({
        package_id: packageId,
        portfolio_name: name,
        initial_cash: initialCash,
        start_date: startDate,
        data_source: dataSource,
        execution_policy: policyId ? { validated_execution_policy_id: policyId } : undefined,
      });
      setCreated(portfolio);
      await load();
    } catch (exc) {
      setError(exc);
    }
  }

  async function lifecycle(portfolioId: string, action: "pause" | "resume" | "complete" | "retire") {
    setError(null);
    try {
      await paperV2Api.lifecycle(portfolioId, action);
      await load();
    } catch (exc) {
      setError(exc);
    }
  }

  return (
    <main>
      <ErrorPanel error={error} title="组合操作失败" />
      <div className="pv2-grid pv2-grid-main">
        <SectionCard title="创建模拟盘 v2 组合" eyebrow="冻结合约向导">
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>StrategyPackage</label><select className="pv2-select" value={packageId} onChange={(event) => setPackageId(event.target.value)}>{packages.map((item) => <option value={item.package_id} key={item.package_id}>{item.package_name} / {item.package_status}</option>)}</select></div>
            <div className="pv2-field"><label>组合名称</label><input className="pv2-input" value={name} onChange={(event) => setName(event.target.value)} /></div>
            <div className="pv2-field"><label>初始资金</label><input className="pv2-input" type="number" min={1} value={initialCash} onChange={(event) => setInitialCash(Number(event.target.value))} /></div>
            <div className="pv2-field"><label>开始日期</label><input className="pv2-input" type="date" value={startDate} onChange={(event) => setStartDate(event.target.value)} /></div>
            <div className="pv2-field"><label>数据源</label><select className="pv2-select" value={dataSource} onChange={(event) => setDataSource(event.target.value as DataSource)}><option value="DB_HISTORICAL">DB_HISTORICAL</option><option value="TDX_REALTIME">TDX_REALTIME</option></select></div>
            <div className="pv2-field"><label>已验证执行策略</label><select className="pv2-select" value={policyId} onChange={(event) => setPolicyId(event.target.value)}><option value="">Manifest 默认策略（校验通过时）</option>{policies.map((item) => <option value={item.policy_id} key={item.policy_id}>{item.policy_name || item.policy_id} / {item.algo_code} / {item.paper_enabled ? "paper" : "disabled"}</option>)}</select></div>
          </div>
          <div className="pv2-card" style={{ marginTop: 14 }}>
            <div className="pv2-eyebrow">创建前复核</div>
            <div className="pv2-chip-row">
              <span className="pv2-chip">package: {selectedPackage?.package_name || "-"}</span>
              <span className="pv2-chip">manifest: {shortHash(selectedPackage?.manifest_sha256)}</span>
              <span className="pv2-chip">data: {dataSource}</span>
              <span className="pv2-chip">cash: {formatCompact(initialCash)}</span>
            </div>
          </div>
          <button className="pv2-button-primary" onClick={createPortfolio} type="button">创建冻结组合</button>
          {created ? <JsonPanel value={{ created_portfolio_id: created.portfolio_id, package_id: created.package_id, manifest_sha256: created.manifest_sha256 }} /> : null}
        </SectionCard>

        <SectionCard title="组合生命周期规则" eyebrow="护栏">
          <ul>
            <li>组合会冻结 package_id、manifest hash、初始资金、开始日期、数据源、费用、风控和执行策略。</li>
            <li>单日运行要求组合处于 READY 状态，并应先通过就绪检查。</li>
            <li>执行策略必须经过回测验证；不接受模拟盘独有的原始算法配置。</li>
            <li>重置回放必须在运行控制台输入明确确认文本。</li>
          </ul>
        </SectionCard>
      </div>

      <SectionCard title="模拟盘 v2 组合" eyebrow={loading ? "加载中" : `${portfolios.length} 个组合`} action={<button className="pv2-button" onClick={load} type="button">刷新</button>}>
        <PaperTable
          rows={portfolios}
          empty="暂无模拟盘 v2 组合。"
          columns={[
            { key: "name", header: "名称", render: (row) => <Link href={`/paper-v2/portfolios/${row.portfolio_id}`}>{row.portfolio_name}</Link> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "package", header: "策略包", render: (row) => <span className="pv2-mono">{shortHash(row.package_id, 7)}</span> },
            { key: "cash", header: "初始资金", render: (row) => formatCompact(row.initial_cash) },
            { key: "source", header: "数据源", render: (row) => row.data_source },
            { key: "start", header: "开始", render: (row) => row.start_date },
            { key: "actions", header: "操作", render: (row) => <div className="pv2-row-actions"><Link className="pv2-link-button" href={`/paper-v2/portfolios/${row.portfolio_id}/run-console`}>运行</Link><button className="pv2-link-button" onClick={() => lifecycle(row.portfolio_id, row.status === "PAUSED" ? "resume" : "pause")} type="button">{row.status === "PAUSED" ? "恢复" : "暂停"}</button><button className="pv2-link-button" onClick={() => lifecycle(row.portfolio_id, "retire")} type="button">退役</button></div> },
          ]}
        />
      </SectionCard>
    </main>
  );
}
