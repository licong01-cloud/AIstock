"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { paperV2Api, qmtApi, strategyPackageApi, type QmtStatus } from "@/lib/paper-v2/api";
import { asText, dataSourceLabel, formatCompact } from "@/lib/paper-v2/format";
import type { ExecutionPolicy, JsonObject, PaperPortfolio, StrategyPackage } from "@/lib/paper-v2/types";

function numberValue(row: JsonObject | null | undefined, key: string): number | null {
  const raw = row?.[key];
  const value = typeof raw === "number" ? raw : Number(raw);
  return Number.isFinite(value) ? value : null;
}

function textValue(row: JsonObject | null | undefined, key: string): string {
  const raw = row?.[key];
  if (raw === null || raw === undefined || raw === "") return "-";
  return String(raw);
}

function fmt(value: unknown, digits = 2): string {
  if (value === null || value === undefined || value === "") return "-";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "-";
  return n.toLocaleString("zh-CN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function miniqmtPortfolios(rows: PaperPortfolio[]): PaperPortfolio[] {
  return rows.filter((row) => row.broker_backend === "minqmt_sim" || row.data_source === "MINIQMT_REALTIME");
}

function todayIso(): string {
  const now = new Date();
  return new Date(now.getTime() - now.getTimezoneOffset() * 60_000).toISOString().slice(0, 10);
}

function packageAssetEligible(pkg: StrategyPackage): boolean {
  const eligibility = pkg.asset_eligibility as JsonObject | undefined;
  if (typeof eligibility?.eligible === "boolean") return eligibility.eligible;
  return String(pkg.package_status || "").toUpperCase() !== "RETIRED";
}

export default function PaperV2MiniQMTSimPage() {
  const [status, setStatus] = useState<QmtStatus | null>(null);
  const [account, setAccount] = useState<JsonObject | null>(null);
  const [positions, setPositions] = useState<JsonObject[]>([]);
  const [orders, setOrders] = useState<JsonObject[]>([]);
  const [trades, setTrades] = useState<JsonObject[]>([]);
  const [portfolios, setPortfolios] = useState<PaperPortfolio[]>([]);
  const [packages, setPackages] = useState<StrategyPackage[]>([]);
  const [policies, setPolicies] = useState<ExecutionPolicy[]>([]);
  const [packageId, setPackageId] = useState("");
  const [policyId, setPolicyId] = useState("");
  const [portfolioName, setPortfolioName] = useState(`MiniQMT-${todayIso()}`);
  const [initialCash, setInitialCash] = useState(100000);
  const [loading, setLoading] = useState(false);
  const [connecting, setConnecting] = useState(false);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<unknown>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [nextStatus, portfolioRows, packageRows] = await Promise.all([
        qmtApi.status(),
        paperV2Api.listPortfolios(300),
        strategyPackageApi.list(undefined, 300),
      ]);
      setStatus(nextStatus);
      setPortfolios(portfolioRows);
      setPackages(packageRows);
      if (nextStatus.connected) {
        const [nextAccount, nextPositions, nextOrders, nextTrades] = await Promise.all([
          qmtApi.account(),
          qmtApi.positions(),
          qmtApi.orders(false),
          qmtApi.trades(),
        ]);
        setAccount(nextAccount);
        setPositions(nextPositions);
        setOrders(nextOrders);
        setTrades(nextTrades);
      } else {
        setAccount(null);
        setPositions([]);
        setOrders([]);
        setTrades([]);
      }
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const connected = Boolean(status?.connected);
  const simMode = String(status?.mode || "").toUpperCase() === "SIM";
  const miniPortfolios = useMemo(() => miniqmtPortfolios(portfolios), [portfolios]);
  const activeMiniPortfolios = miniPortfolios.filter((row) => ["READY", "RUNNING", "PAUSED"].includes(row.status));
  const eligiblePackages = useMemo(
    () => packages.filter(packageAssetEligible),
    [packages],
  );
  const provider = status?.provider || "-";

  useEffect(() => {
    if (!packageId && eligiblePackages.length) {
      setPackageId(eligiblePackages[0].package_id);
      setPortfolioName(`MiniQMT-${todayIso()}-${eligiblePackages[0].package_name || eligiblePackages[0].package_id}`);
    }
  }, [eligiblePackages, packageId]);

  useEffect(() => {
    if (!packageId) {
      setPolicies([]);
      setPolicyId("");
      return;
    }
    let alive = true;
    strategyPackageApi.executionPolicies(packageId).then((rows) => {
      if (!alive) return;
      setPolicies(rows);
      setPolicyId((current) => {
        const currentPolicy = rows.find((item) => item.policy_id === current);
        if (currentPolicy) return current;
        return rows[0]?.policy_id || "";
      });
    }).catch((exc) => {
      if (!alive) return;
      setPolicies([]);
      setPolicyId("");
      setError(exc);
    });
    return () => { alive = false; };
  }, [packageId]);

  async function connect() {
    setConnecting(true);
    setError(null);
    try {
      await qmtApi.connect();
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setConnecting(false);
    }
  }

  async function createExclusivePortfolio() {
    setCreating(true);
    setError(null);
    try {
      if (!packageId) throw new Error("请先选择资产合格的策略包。");
      await paperV2Api.createPortfolio({
        package_id: packageId,
        portfolio_name: portfolioName.trim() || `MiniQMT-${todayIso()}`,
        initial_cash: initialCash,
        start_date: todayIso(),
        data_source: "MINIQMT_REALTIME",
        broker_backend: "minqmt_sim",
        execution_policy: policyId ? { validated_execution_policy_id: policyId } : undefined,
      });
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setCreating(false);
    }
  }

  return (
    <main>
      <ErrorPanel error={error} title="MiniQMT 模拟盘检查失败" />
      <div className="pv2-grid pv2-grid-4">
        <div className="pv2-metric pv2-metric-info"><div className="pv2-metric-label">连接状态</div><div className="pv2-metric-value">{connected ? "已连接" : "未连接"}</div><div className="pv2-metric-hint">{provider} / {status?.client_class || "-"}</div></div>
        <div className="pv2-metric"><div className="pv2-metric-label">账号模式</div><div className="pv2-metric-value">{status?.mode || "-"}</div><div className="pv2-metric-hint">MVP 仅允许 exclusive_account</div></div>
        <div className="pv2-metric pv2-metric-success"><div className="pv2-metric-label">账号总资产</div><div className="pv2-metric-value">{fmt(numberValue(account, "total_asset"))}</div><div className="pv2-metric-hint">仅来自 MiniQMT account query</div></div>
        <div className="pv2-metric pv2-metric-warning"><div className="pv2-metric-label">MiniQMT 组合</div><div className="pv2-metric-value">{activeMiniPortfolios.length}/{miniPortfolios.length}</div><div className="pv2-metric-hint">不与 LocalSim 组合混用</div></div>
      </div>

      <NoticePanel title="交易权威边界" tone="warning">
        AIstock 只生成买卖方向、代码、数量和提交时间；MiniQMT 是唯一委托、拒单、成交、资金和持仓权威。本页面不会用 TDX、DB、tick 或 LocalSim 补成交，也不会展示每策略真实资金池。
      </NoticePanel>

      <SectionCard title="创建 MiniQMT 独占账号组合" eyebrow="exclusive account only" action={<button className="pv2-button-primary" onClick={createExclusivePortfolio} disabled={creating || !connected || !simMode || !packageId} type="button">{creating ? "创建中..." : "创建组合"}</button>}>
        <div className="pv2-form-grid">
          <div className="pv2-field"><label>策略包</label><select className="pv2-select" value={packageId} onChange={(event) => setPackageId(event.target.value)}>{eligiblePackages.map((item) => <option value={item.package_id} key={item.package_id}>{item.package_name} / {item.package_status}</option>)}</select></div>
          <div className="pv2-field"><label>Validated execution policy</label><select className="pv2-select" value={policyId} onChange={(event) => setPolicyId(event.target.value)}><option value="">平台默认：使用 manifest 默认执行策略</option>{policies.map((item) => <option value={item.policy_id} key={item.policy_id}>{item.policy_name || item.policy_id} / {item.algo_code || "-"}</option>)}</select></div>
          <div className="pv2-field"><label>组合名称</label><input className="pv2-input" value={portfolioName} onChange={(event) => setPortfolioName(event.target.value)} /></div>
          <div className="pv2-field"><label>本地兼容资金字段</label><input className="pv2-input" type="number" min={1} value={initialCash} onChange={(event) => setInitialCash(Number(event.target.value))} /></div>
          <div className="pv2-field"><label>Broker / 数据通道</label><input className="pv2-input" value="minqmt_sim / MINIQMT_REALTIME" readOnly /></div>
        </div>
        <NoticePanel title="资金口径" tone="info">
          这里的 initial_cash 仅用于兼容 Paper v2 组合 schema，不代表 MiniQMT 已分配独立资金；实际资金、持仓和成交必须以 MiniQMT 账号查询为准。
        </NoticePanel>
      </SectionCard>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="MiniQMT 连接与账户" eyebrow="broker authority" action={<div className="pv2-row-actions"><button className="pv2-button" onClick={load} disabled={loading} type="button">刷新</button><button className="pv2-button-primary" onClick={connect} disabled={connecting || connected} type="button">{connecting ? "连接中..." : "连接 MiniQMT"}</button></div>}>
          <div className="pv2-readable-panel">
            <div className="pv2-readable-table">
              <div className="pv2-readable-row"><div className="pv2-readable-key">连接</div><div className="pv2-readable-value"><StatusBadge status={connected ? "CONNECTED" : "DISCONNECTED"} /></div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">SIM 检查</div><div className="pv2-readable-value"><StatusBadge status={simMode ? "SIM" : "CHECK_REQUIRED"} /> {simMode ? "当前为模拟账号模式" : "请确认不是实盘账号"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">账号</div><div className="pv2-readable-value pv2-mono">{status?.account_id || "-"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">session_id</div><div className="pv2-readable-value pv2-mono">{status?.session_id ?? "-"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">可用资金</div><div className="pv2-readable-value">{fmt(numberValue(account, "available_cash"))}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">市值/冻结</div><div className="pv2-readable-value">{fmt(numberValue(account, "market_value"))} / {fmt(numberValue(account, "frozen_cash"))}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">错误</div><div className="pv2-readable-value">{status?.last_error || "-"}</div></div>
            </div>
          </div>
        </SectionCard>

        <SectionCard title="MiniQMT 组合清单" eyebrow="exclusive account">
          <PaperTable
            rows={miniPortfolios}
            empty="暂无 broker_backend=minqmt_sim 的 Paper v2 组合。"
            columns={[
              { key: "name", header: "名称", render: (row) => row.portfolio_name },
              { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
              { key: "source", header: "通道", render: (row) => dataSourceLabel(row.data_source) },
              { key: "cash", header: "本地字段", render: (row) => <span title="仅兼容旧 schema，不代表 MiniQMT 真实分配资金">{formatCompact(row.initial_cash)}</span> },
              { key: "actions", header: "操作", render: (row) => <Link className="pv2-button" href={`/paper-v2/portfolios/${row.portfolio_id}/run-console`}>运行控制台</Link> },
            ]}
          />
        </SectionCard>
      </div>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="MiniQMT 持仓" eyebrow="query_stock_positions">
          <PaperTable
            rows={positions}
            empty={connected ? "MiniQMT 当前无持仓。" : "未连接 MiniQMT。"}
            columns={[
              { key: "symbol", header: "股票", render: (row) => <span>{textValue(row, "stock_code")} {textValue(row, "stock_name") !== "-" ? asText(row.stock_name) : ""}</span> },
              { key: "qty", header: "数量", render: (row) => fmt(numberValue(row, "quantity"), 0) },
              { key: "sell", header: "可卖", render: (row) => fmt(numberValue(row, "can_sell"), 0) },
              { key: "price", header: "现价/成本", render: (row) => `${fmt(numberValue(row, "current_price"))} / ${fmt(numberValue(row, "cost_price"))}` },
              { key: "mv", header: "市值", render: (row) => fmt(numberValue(row, "market_value")) },
            ]}
          />
        </SectionCard>

        <SectionCard title="当日委托" eyebrow="query_stock_orders">
          <PaperTable
            rows={orders}
            empty={connected ? "MiniQMT 当前无委托。" : "未连接 MiniQMT。"}
            columns={[
              { key: "time", header: "时间", render: (row) => textValue(row, "order_time") },
              { key: "symbol", header: "股票", render: (row) => textValue(row, "stock_code") },
              { key: "side", header: "方向", render: (row) => textValue(row, "order_type_name") },
              { key: "qty", header: "委托/成交", render: (row) => `${fmt(numberValue(row, "order_volume"), 0)} / ${fmt(numberValue(row, "traded_volume"), 0)}` },
              { key: "status", header: "状态", render: (row) => <span>{textValue(row, "order_status")} {textValue(row, "status_msg")}</span> },
              { key: "strategy", header: "归因", render: (row) => <span className="pv2-mono">{textValue(row, "strategy_name")}</span> },
            ]}
          />
        </SectionCard>
      </div>

      <SectionCard title="当日成交" eyebrow="query_stock_trades">
        <PaperTable
          rows={trades}
          empty={connected ? "MiniQMT 当前无成交。" : "未连接 MiniQMT。"}
          columns={[
            { key: "time", header: "时间", render: (row) => textValue(row, "traded_time") },
            { key: "symbol", header: "股票", render: (row) => textValue(row, "stock_code") },
            { key: "side", header: "方向", render: (row) => textValue(row, "order_type_name") },
            { key: "qty", header: "数量", render: (row) => fmt(numberValue(row, "traded_volume"), 0) },
            { key: "price", header: "价格", render: (row) => fmt(numberValue(row, "traded_price")) },
            { key: "amount", header: "金额", render: (row) => fmt(numberValue(row, "traded_amount")) },
            { key: "strategy", header: "归因", render: (row) => <span className="pv2-mono">{textValue(row, "strategy_name")}</span> },
          ]}
        />
      </SectionCard>
    </main>
  );
}
