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
import type { ExecutionPolicy, JsonObject, PaperAutoRunSummary, PaperPortfolio, PaperSchedulerBootstrapStatus, StrategyPackage } from "@/lib/paper-v2/types";

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


type SortDirection = "asc" | "desc";
type SortState = { key: string; direction: SortDirection } | null;

function firstTextValue(row: JsonObject | null | undefined, keys: string[]): string {
  for (const key of keys) {
    const raw = row?.[key];
    if (raw !== null && raw !== undefined && raw !== "") return String(raw);
  }
  return "-";
}

function firstNumberValue(row: JsonObject | null | undefined, keys: string[]): number | null {
  for (const key of keys) {
    const raw = row?.[key];
    if (raw === null || raw === undefined || raw === "") continue;
    const value = typeof raw === "number" ? raw : Number(raw);
    if (Number.isFinite(value)) return value;
  }
  return null;
}

function firstPositiveNumberValue(row: JsonObject | null | undefined, keys: string[]): number | null {
  for (const key of keys) {
    const value = firstNumberValue(row, [key]);
    if (value !== null && value > 0) return value;
  }
  return firstNumberValue(row, keys);
}

function positionCode(row: JsonObject): string {
  return firstTextValue(row, ["stock_code", "symbol", "ts_code", "code"]);
}

function positionName(row: JsonObject): string {
  return firstTextValue(row, ["stock_name", "instrument_name", "name", "security_name"]);
}

function positionQuantity(row: JsonObject): number | null {
  return firstNumberValue(row, ["quantity", "volume", "current_amount"]);
}

function positionCostPrice(row: JsonObject): number | null {
  return firstPositiveNumberValue(row, ["cost_price", "avg_cost", "avg_price", "open_price"]);
}

function positionCurrentPrice(row: JsonObject): number | null {
  return firstPositiveNumberValue(row, ["current_price", "last_price", "market_price"]);
}

function positionMarketValue(row: JsonObject): number | null {
  const direct = firstNumberValue(row, ["market_value", "marketValue"]);
  if (direct !== null && direct !== 0) return direct;
  const quantity = positionQuantity(row);
  const price = positionCurrentPrice(row);
  if (quantity !== null && quantity > 0 && price !== null && price > 0) return quantity * price;
  return direct;
}

function positionMarketValueSource(row: JsonObject): string {
  const direct = firstNumberValue(row, ["market_value", "marketValue"]);
  const quantity = positionQuantity(row);
  const price = positionCurrentPrice(row);
  if (direct !== null && direct !== 0) return "MiniQMT market_value";
  if (quantity !== null && quantity > 0 && price !== null && price > 0) return "估算：数量 * 当前价";
  return "MiniQMT market_value 为空或为 0";
}

function tradeCode(row: JsonObject): string {
  return firstTextValue(row, ["stock_code", "symbol", "ts_code", "code"]);
}

function tradeName(row: JsonObject): string {
  return firstTextValue(row, ["stock_name", "instrument_name", "name", "security_name"]);
}

function tradeTime(row: JsonObject): string {
  return firstTextValue(row, ["traded_time", "trade_time", "created_at", "updated_at"]);
}

function tradeDate(row: JsonObject): string {
  const explicit = firstTextValue(row, ["trade_date", "trading_day", "date"]);
  if (explicit !== "-") return explicit.slice(0, 10);
  const created = firstTextValue(row, ["created_at", "updated_at"]);
  return created !== "-" && /^\d{4}-\d{2}-\d{2}/.test(created) ? created.slice(0, 10) : "";
}

function tradeAmount(row: JsonObject): number | null {
  const direct = firstNumberValue(row, ["traded_amount", "amount", "trade_amount"]);
  if (direct !== null) return direct;
  const price = firstNumberValue(row, ["traded_price", "price"]);
  const volume = firstNumberValue(row, ["traded_volume", "quantity", "volume"]);
  return price !== null && volume !== null ? price * volume : null;
}

function positionSortValue(row: JsonObject, key: string): string | number | null {
  if (key === "code") return positionCode(row);
  if (key === "name") return positionName(row);
  if (key === "quantity") return positionQuantity(row);
  if (key === "can_sell") return firstNumberValue(row, ["can_sell", "can_use_volume", "available_quantity"]);
  if (key === "open_price") return firstNumberValue(row, ["open_price"]);
  if (key === "cost_price") return positionCostPrice(row);
  if (key === "current_price") return positionCurrentPrice(row);
  if (key === "market_value") return positionMarketValue(row);
  if (key === "position_profit") return firstNumberValue(row, ["position_profit", "unrealized_pnl", "profit"]);
  if (key === "float_profit") return firstNumberValue(row, ["float_profit", "day_profit"]);
  if (key === "profit_rate") return firstNumberValue(row, ["profit_rate"]);
  return firstTextValue(row, [key]);
}

function tradeSortValue(row: JsonObject, key: string): string | number | null {
  if (key === "time") return tradeTime(row);
  if (key === "code") return tradeCode(row);
  if (key === "name") return tradeName(row);
  if (key === "side") return firstTextValue(row, ["order_type_name", "side", "direction"]);
  if (key === "quantity") return firstNumberValue(row, ["traded_volume", "quantity", "volume"]);
  if (key === "price") return firstNumberValue(row, ["traded_price", "price"]);
  if (key === "amount") return tradeAmount(row);
  if (key === "strategy") return firstTextValue(row, ["strategy_name", "strategy", "order_remark"]);
  if (key === "order_id") return firstTextValue(row, ["order_id", "order_sysid", "traded_id"]);
  return firstTextValue(row, [key]);
}

function nextSortState(current: SortState, key: string): SortState {
  if (!current || current.key !== key) return { key, direction: "asc" };
  if (current.direction === "asc") return { key, direction: "desc" };
  return null;
}

function compareSortValues(left: string | number | null, right: string | number | null): number {
  const leftMissing = left === null || left === "" || left === "-";
  const rightMissing = right === null || right === "" || right === "-";
  if (leftMissing && rightMissing) return 0;
  if (leftMissing) return 1;
  if (rightMissing) return -1;
  if (typeof left === "number" && typeof right === "number") return left - right;
  return String(left).localeCompare(String(right), "zh-CN", { numeric: true, sensitivity: "base" });
}

function sortRows(rows: JsonObject[], sort: SortState, valueGetter: (row: JsonObject, key: string) => string | number | null): JsonObject[] {
  if (!sort) return rows;
  const direction = sort.direction === "asc" ? 1 : -1;
  return [...rows].sort((left, right) => direction * compareSortValues(valueGetter(left, sort.key), valueGetter(right, sort.key)));
}

function SortHeader({ label, sortKey, sort, onSort, testId }: { label: string; sortKey: string; sort: SortState; onSort: (key: string) => void; testId: string }) {
  const active = sort?.key === sortKey;
  const suffix = active ? (sort.direction === "asc" ? " ↑" : " ↓") : "";
  const title = active && sort.direction === "desc" ? "点击清空排序" : active ? "点击切换降序" : "点击升序";
  return <button className="pv2-link-button" data-testid={testId} onClick={() => onSort(sortKey)} title={title} type="button">{label}{suffix}</button>;
}

function packageAssetEligible(pkg: StrategyPackage): boolean {
  const eligibility = pkg.asset_eligibility as JsonObject | undefined;
  if (typeof eligibility?.eligible === "boolean") return eligibility.eligible;
  return String(pkg.package_status || "").toUpperCase() !== "RETIRED";
}

export default function PaperV2MiniQMTSimPage() {
  const [status, setStatus] = useState<QmtStatus | null>(null);
  const [bootstrapStatus, setBootstrapStatus] = useState<PaperSchedulerBootstrapStatus | null>(null);
  const [account, setAccount] = useState<JsonObject | null>(null);
  const [positions, setPositions] = useState<JsonObject[]>([]);
  const [orders, setOrders] = useState<JsonObject[]>([]);
  const [trades, setTrades] = useState<JsonObject[]>([]);
  const [portfolios, setPortfolios] = useState<PaperPortfolio[]>([]);
  const [autoRunByPortfolio, setAutoRunByPortfolio] = useState<Record<string, PaperAutoRunSummary>>({});
  const [packages, setPackages] = useState<StrategyPackage[]>([]);
  const [policies, setPolicies] = useState<ExecutionPolicy[]>([]);
  const [packageId, setPackageId] = useState("");
  const [policyId, setPolicyId] = useState("");
  const [portfolioName, setPortfolioName] = useState(`MiniQMT-${todayIso()}`);
  const [initialCash, setInitialCash] = useState(100000);
  const [brokerAccountId, setBrokerAccountId] = useState("");
  const [topK, setTopK] = useState(10);
  const [loading, setLoading] = useState(false);
  const [connecting, setConnecting] = useState(false);
  const [creating, setCreating] = useState(false);
  const [recovering, setRecovering] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [positionSort, setPositionSort] = useState<SortState>(null);
  const [tradeSort, setTradeSort] = useState<SortState>(null);
  const [tradesExpanded, setTradesExpanded] = useState(false);
  const [tradePage, setTradePage] = useState(1);

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
      setBrokerAccountId((current) => current || String(nextStatus.account_id || ""));
      setPortfolios(portfolioRows);
      setPackages(packageRows);
      setBootstrapStatus(await paperV2Api.schedulerBootstrapStatus());
      const miniRows = miniqmtPortfolios(portfolioRows).slice(0, 50);
      const statusRows = await Promise.all(
        miniRows.map(async (row) => {
          try {
            return [row.portfolio_id, await paperV2Api.autoRunStatus(row.portfolio_id)] as const;
        } catch {
          return [row.portfolio_id, undefined] as const;
        }
      }),
      );
      const nextAutoRunByPortfolio: Record<string, PaperAutoRunSummary> = {};
      for (const [portfolioId, value] of statusRows) {
        if (value) nextAutoRunByPortfolio[portfolioId] = value;
      }
      setAutoRunByPortfolio(nextAutoRunByPortfolio);
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
  const sortedPositions = useMemo(() => sortRows(positions, positionSort, positionSortValue), [positions, positionSort]);
  const currentTradeDate = useMemo(() => trades.map(tradeDate).find(Boolean) || "", [trades]);
  const currentTrades = useMemo(() => currentTradeDate ? trades.filter((row) => tradeDate(row) === currentTradeDate) : trades, [currentTradeDate, trades]);
  const sortedTrades = useMemo(() => sortRows(currentTrades, tradeSort, tradeSortValue), [currentTrades, tradeSort]);
  const tradePageSize = 10;
  const tradeTotalPages = Math.max(1, Math.ceil(sortedTrades.length / tradePageSize));
  const tradePageSafe = Math.min(tradePage, tradeTotalPages);
  const visibleTrades = sortedTrades.slice((tradePageSafe - 1) * tradePageSize, tradePageSafe * tradePageSize);

  function togglePositionSort(key: string) {
    setPositionSort((current) => nextSortState(current, key));
  }

  function toggleTradeSort(key: string) {
    setTradeSort((current) => nextSortState(current, key));
    setTradePage(1);
  }

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
      if (!brokerAccountId.trim()) throw new Error("请先填写 MiniQMT SIM 账号。");
      await paperV2Api.createMiniQMTAutoRunPortfolio({
        package_id: packageId,
        portfolio_name: portfolioName.trim() || `MiniQMT-${todayIso()}`,
        initial_cash: initialCash,
        start_date: todayIso(),
        broker_account_id: brokerAccountId.trim(),
        top_k: topK,
        hmm: { enabled: false, auto_compute: true, manual_snapshot_required: false },
        industry_blacklist: [],
        execution_policy: policyId ? { validated_execution_policy_id: policyId } : undefined,
        created_by: "paper_v2_minqmt_ui",
        create_session: true,
      });
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setCreating(false);
    }
  }

  async function recoverAutoRun() {
    setRecovering(true);
    setError(null);
    try {
      await paperV2Api.recoverAutoRun({ limit: 50 });
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setRecovering(false);
    }
  }

  async function toggleAutoRun(row: PaperPortfolio) {
    setError(null);
    try {
      if (row.auto_run_enabled) {
        await paperV2Api.disableAutoRun(row.portfolio_id, { updated_by: "paper_v2_minqmt_ui" });
      } else {
        if (!brokerAccountId.trim()) throw new Error("启用自动运行前需要填写 MiniQMT SIM 账号。");
        await paperV2Api.enableAutoRun(row.portfolio_id, {
          broker_account_id: brokerAccountId.trim(),
          updated_by: "paper_v2_minqmt_ui",
          create_session: true,
        });
      }
      await load();
    } catch (exc) {
      setError(exc);
    }
  }

  return (
    <main>
      <ErrorPanel error={error} title="MiniQMT 模拟盘检查失败" />
      <div className="pv2-grid pv2-grid-4">
        <div className="pv2-metric pv2-metric-info"><div className="pv2-metric-label">连接状态</div><div className="pv2-metric-value">{connected ? "已连接" : "未连接"}</div><div className="pv2-metric-hint">{provider} / {status?.client_class || "-"}</div></div>
        <div className="pv2-metric"><div className="pv2-metric-label">账号模式</div><div className="pv2-metric-value">{status?.mode || "-"}</div><div className="pv2-metric-hint">MVP 仅允许 exclusive_account</div></div>
        <div className="pv2-metric pv2-metric-success"><div className="pv2-metric-label">账号总资产</div><div className="pv2-metric-value">{fmt(numberValue(account, "total_asset"))}</div><div className="pv2-metric-hint">仅来自 MiniQMT account query</div></div>
        <div className="pv2-metric pv2-metric-warning"><div className="pv2-metric-label">自动调度</div><div className="pv2-metric-value">{bootstrapStatus?.scheduler?.running ? "运行中" : "未运行"}</div><div className="pv2-metric-hint">自动组合 {activeMiniPortfolios.filter((row) => row.auto_run_enabled).length}/{miniPortfolios.length}</div></div>
      </div>

      <NoticePanel title="交易权威边界" tone="warning">
        AIstock 只生成买卖方向、代码、数量和提交时间；MiniQMT 是唯一委托、拒单、成交、资金和持仓权威。本页面不会用 TDX、DB、tick 或 LocalSim 补成交，也不会展示每策略真实资金池。
      </NoticePanel>

      <SectionCard title="创建 MiniQMT 自动运行组合" eyebrow="exclusive account auto-run" action={<button className="pv2-button-primary" onClick={createExclusivePortfolio} disabled={creating || !packageId || !brokerAccountId.trim()} type="button">{creating ? "创建中..." : "创建并启用自动运行"}</button>}>
        <div className="pv2-form-grid">
          <div className="pv2-field"><label>策略包</label><select className="pv2-select" value={packageId} onChange={(event) => setPackageId(event.target.value)}>{eligiblePackages.map((item) => <option value={item.package_id} key={item.package_id}>{item.package_name} / {item.package_status}</option>)}</select></div>
          <div className="pv2-field"><label>Validated execution policy</label><select className="pv2-select" value={policyId} onChange={(event) => setPolicyId(event.target.value)}><option value="">平台默认：使用 manifest 默认执行策略</option>{policies.map((item) => <option value={item.policy_id} key={item.policy_id}>{item.policy_name || item.policy_id} / {item.algo_code || "-"}</option>)}</select></div>
          <div className="pv2-field"><label>组合名称</label><input className="pv2-input" value={portfolioName} onChange={(event) => setPortfolioName(event.target.value)} /></div>
          <div className="pv2-field"><label>本地兼容资金字段</label><input className="pv2-input" type="number" min={1} value={initialCash} onChange={(event) => setInitialCash(Number(event.target.value))} /></div>
          <div className="pv2-field"><label>MiniQMT SIM 账号</label><input className="pv2-input" value={brokerAccountId} onChange={(event) => setBrokerAccountId(event.target.value)} placeholder="读取 MINIQMT_ACCOUNT_ID 或手工填写" /></div>
          <div className="pv2-field"><label>TopK</label><input className="pv2-input" type="number" min={1} max={100} value={topK} onChange={(event) => setTopK(Number(event.target.value))} /></div>
          <div className="pv2-field"><label>Broker / 数据通道</label><input className="pv2-input" value="minqmt_sim / MINIQMT_REALTIME" readOnly /></div>
          <div className="pv2-field"><label>HMM 日内参数</label><input className="pv2-input" value="默认关闭；启用后平台每日自动 compute/cache" readOnly /></div>
        </div>
        <NoticePanel title="资金口径" tone="info">
          这里的 initial_cash 仅用于兼容 Paper v2 组合 schema，不代表 MiniQMT 已分配独立资金；实际资金、持仓和成交必须以 MiniQMT 账号查询为准。
        </NoticePanel>
        {(!connected || !simMode) ? (
          <NoticePanel title="MiniQMT 运行时状态" tone="warning">
            当前连接或 SIM 检查未通过，但创建组合不再被 broker 状态阻断；后续提交委托或执行 Tick 时会按 MiniQMT 实时状态 fail-fast。
          </NoticePanel>
        ) : null}
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

        <SectionCard title="自动运行调度" eyebrow="restart recovery" action={<button className="pv2-button" onClick={recoverAutoRun} disabled={recovering} type="button">{recovering ? "恢复中..." : "恢复缺失 session"}</button>}>
          <div className="pv2-readable-panel">
            <div className="pv2-readable-table">
              <div className="pv2-readable-row"><div className="pv2-readable-key">进程调度器</div><div className="pv2-readable-value"><StatusBadge status={bootstrapStatus?.scheduler?.running ? "RUNNING" : "STOPPED"} /> interval={bootstrapStatus?.scheduler?.interval_seconds ?? "-"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">重启自启动</div><div className="pv2-readable-value">{bootstrapStatus?.scheduler_autostart_env ? "已配置" : "未配置"} <span className="pv2-mono">{bootstrapStatus?.scheduler_env_raw || "-"}</span></div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">Auto-run</div><div className="pv2-readable-value">{String((bootstrapStatus?.auto_run as JsonObject | undefined)?.env_enabled ?? "-")} / missing-session={String((bootstrapStatus?.auto_run as JsonObject | undefined)?.bootstrap_missing_session ?? "-")}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">最近 tick</div><div className="pv2-readable-value">{bootstrapStatus?.scheduler?.last_run_at || "-"}</div></div>
            </div>
          </div>
        </SectionCard>
      </div>

      <SectionCard title="MiniQMT 组合清单" eyebrow="exclusive account auto-run">
        <NoticePanel title="本地字段说明" tone="info">
          <span data-testid="miniqmt-local-fields-help">“本地字段”来自 AIstock Paper v2 本地 portfolio schema，用于兼容创建流程、列表排序和 auto-run 控制；它不代表 MiniQMT 账户已分配的真实资金。真实资金、持仓、成本、市值和成交以 MiniQMT broker query 为准。</span>
        </NoticePanel>
        <PaperTable
          rows={miniPortfolios}
          empty="暂无 broker_backend=minqmt_sim 的 Paper v2 组合。"
          columns={[
            { key: "name", header: "组合", render: (row) => <span>{row.portfolio_name}<br /><span className="pv2-muted pv2-mono">{row.portfolio_id}</span></span> },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
            { key: "auto", header: "自动运行", render: (row) => <StatusBadge status={row.auto_run_enabled ? "ENABLED" : "DISABLED"} /> },
            { key: "source", header: "通道", render: (row) => dataSourceLabel(row.data_source) },
            { key: "plan", header: "下次计划", render: (row) => autoRunByPortfolio[row.portfolio_id]?.next_plan || "-" },
            { key: "cash", header: "本地字段", render: (row) => <span title="portfolio.initial_cash is local schema metadata; broker cash uses MiniQMT account query">{formatCompact(row.initial_cash)}<br /><span className="pv2-muted">initial_cash / schema</span></span> },
            { key: "actions", header: "操作", render: (row) => <div className="pv2-row-actions"><Link className="pv2-button" href={`/paper-v2/portfolios/${row.portfolio_id}/run-console`}>运行控制台</Link><button className="pv2-button" type="button" onClick={() => toggleAutoRun(row)}>{row.auto_run_enabled ? "停用自动运行" : "启用自动运行"}</button></div> },
          ]}
        />
      </SectionCard>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="MiniQMT 持仓" eyebrow={`query_stock_positions / ${sortedPositions.length} 条`}>
          <NoticePanel title="持仓成本和市值口径" tone="info">
            成本优先显示 MiniQMT 返回的 cost_price/avg_price/avg_cost；若这些字段为 0，再显示 open_price 作为券商可读成本口径。市值优先显示 MiniQMT market_value；若 broker 未返回市值但有数量和现价，表格会明示“估算”。数量为 0 表示 MiniQMT 当前查询返回零剩余持仓，可能是今日已卖出/清仓后保留的券商行，不应单独等同于今日清仓；需结合当日成交和委托确认。
          </NoticePanel>
          <PaperTable
            rows={sortedPositions}
            empty={connected ? "MiniQMT 当前无持仓。" : "未连接 MiniQMT。"}
            columns={[
              { key: "code", header: <SortHeader label="股票代码" sortKey="code" sort={positionSort} onSort={togglePositionSort} testId="miniqmt-position-sort-code" />, render: (row) => <span className="pv2-mono">{positionCode(row)}</span> },
              { key: "name", header: <SortHeader label="股票名称" sortKey="name" sort={positionSort} onSort={togglePositionSort} testId="miniqmt-position-sort-name" />, render: (row) => positionName(row) },
              { key: "qty", header: <SortHeader label="数量" sortKey="quantity" sort={positionSort} onSort={togglePositionSort} testId="miniqmt-position-sort-quantity" />, render: (row) => { const quantity = positionQuantity(row); return <span>{fmt(quantity, 0)}{quantity === 0 ? <span className="pv2-muted"> 可能已清仓</span> : null}</span>; } },
              { key: "sell", header: <SortHeader label="可卖" sortKey="can_sell" sort={positionSort} onSort={togglePositionSort} testId="miniqmt-position-sort-can-sell" />, render: (row) => fmt(firstNumberValue(row, ["can_sell", "can_use_volume", "available_quantity"]), 0) },
              { key: "cost", header: <SortHeader label="成本" sortKey="cost_price" sort={positionSort} onSort={togglePositionSort} testId="miniqmt-position-sort-cost" />, render: (row) => fmt(positionCostPrice(row), 4) },
              { key: "price", header: <SortHeader label="现价" sortKey="current_price" sort={positionSort} onSort={togglePositionSort} testId="miniqmt-position-sort-price" />, render: (row) => fmt(positionCurrentPrice(row), 4) },
              { key: "mv", header: <SortHeader label="市值" sortKey="market_value" sort={positionSort} onSort={togglePositionSort} testId="miniqmt-position-sort-market-value" />, render: (row) => { const source = positionMarketValueSource(row); return <span title={source}>{fmt(positionMarketValue(row))}{source.includes("估算") ? <span className="pv2-muted"> 估算</span> : null}</span>; } },
              { key: "profit", header: <SortHeader label="持仓盈亏" sortKey="position_profit" sort={positionSort} onSort={togglePositionSort} testId="miniqmt-position-sort-profit" />, render: (row) => fmt(firstNumberValue(row, ["position_profit", "unrealized_pnl", "profit"])) },
              { key: "dayProfit", header: <SortHeader label="当日盈亏" sortKey="float_profit" sort={positionSort} onSort={togglePositionSort} testId="miniqmt-position-sort-day-profit" />, render: (row) => fmt(firstNumberValue(row, ["float_profit", "day_profit"])) },
              { key: "rate", header: <SortHeader label="盈亏率" sortKey="profit_rate" sort={positionSort} onSort={togglePositionSort} testId="miniqmt-position-sort-profit-rate" />, render: (row) => fmt(firstNumberValue(row, ["profit_rate"]) ?? null, 4) },
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

      <SectionCard
        title="当日成交"
        eyebrow={`query_stock_trades / ${currentTrades.length} 条${currentTradeDate ? ` / ${currentTradeDate}` : ""}`}
        action={<button className="pv2-button" data-testid="miniqmt-trades-toggle" onClick={() => setTradesExpanded((current) => !current)} type="button">{tradesExpanded ? "收起当日成交" : `展开当日成交 (${currentTrades.length})`}</button>}
      >
        <NoticePanel title="成交表格默认收起" tone="info">
          MiniQMT query_stock_trades 一般返回当日成交。为避免首屏过长，表格默认隐藏；展开后可按表头在所有显示字段上升序/降序/清空排序，并使用翻页查看全部成交。
        </NoticePanel>
        {tradesExpanded ? (
          <>
            <div data-testid="miniqmt-trades-table">
              <PaperTable
                rows={visibleTrades}
                empty={connected ? "MiniQMT 当前无成交。" : "未连接 MiniQMT。"}
                columns={[
                  { key: "time", header: <SortHeader label="时间" sortKey="time" sort={tradeSort} onSort={toggleTradeSort} testId="miniqmt-trade-sort-time" />, render: (row) => tradeTime(row) },
                  { key: "code", header: <SortHeader label="股票代码" sortKey="code" sort={tradeSort} onSort={toggleTradeSort} testId="miniqmt-trade-sort-code" />, render: (row) => <span className="pv2-mono">{tradeCode(row)}</span> },
                  { key: "name", header: <SortHeader label="股票名称" sortKey="name" sort={tradeSort} onSort={toggleTradeSort} testId="miniqmt-trade-sort-name" />, render: (row) => tradeName(row) },
                  { key: "side", header: <SortHeader label="方向" sortKey="side" sort={tradeSort} onSort={toggleTradeSort} testId="miniqmt-trade-sort-side" />, render: (row) => firstTextValue(row, ["order_type_name", "side", "direction"]) },
                  { key: "qty", header: <SortHeader label="数量" sortKey="quantity" sort={tradeSort} onSort={toggleTradeSort} testId="miniqmt-trade-sort-quantity" />, render: (row) => fmt(firstNumberValue(row, ["traded_volume", "quantity", "volume"]), 0) },
                  { key: "price", header: <SortHeader label="价格" sortKey="price" sort={tradeSort} onSort={toggleTradeSort} testId="miniqmt-trade-sort-price" />, render: (row) => fmt(firstNumberValue(row, ["traded_price", "price"]), 4) },
                  { key: "amount", header: <SortHeader label="金额" sortKey="amount" sort={tradeSort} onSort={toggleTradeSort} testId="miniqmt-trade-sort-amount" />, render: (row) => fmt(tradeAmount(row)) },
                  { key: "strategy", header: <SortHeader label="策略" sortKey="strategy" sort={tradeSort} onSort={toggleTradeSort} testId="miniqmt-trade-sort-strategy" />, render: (row) => <span className="pv2-mono">{firstTextValue(row, ["strategy_name", "strategy", "order_remark"])}</span> },
                  { key: "order", header: <SortHeader label="委托/成交ID" sortKey="order_id" sort={tradeSort} onSort={toggleTradeSort} testId="miniqmt-trade-sort-order" />, render: (row) => <span className="pv2-mono">{firstTextValue(row, ["order_id", "order_sysid", "traded_id"])}</span> },
                ]}
              />
            </div>
            <div className="pv2-row-actions" style={{ marginTop: 12, justifyContent: "flex-end" }}>
              <button className="pv2-button-ghost" data-testid="miniqmt-trades-prev" disabled={tradePageSafe <= 1 || loading} onClick={() => setTradePage((current) => Math.max(1, current - 1))} type="button">上一页</button>
              <span className="pv2-muted">第 {tradePageSafe} / {tradeTotalPages} 页</span>
              <button className="pv2-button-ghost" data-testid="miniqmt-trades-next" disabled={tradePageSafe >= tradeTotalPages || loading} onClick={() => setTradePage((current) => current + 1)} type="button">下一页</button>
            </div>
          </>
        ) : null}
      </SectionCard>
    </main>
  );
}
