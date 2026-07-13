"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { paperV2Api, qmtApi, simulationRuntimeApi, strategyPackageApi, type QmtStatus } from "@/lib/paper-v2/api";
import { asText, dataSourceLabel, formatCompact } from "@/lib/paper-v2/format";
import type {
  ExecutionPolicy,
  JsonObject,
  PaperAutoRunSummary,
  PaperPortfolio,
  PaperSchedulerBootstrapStatus,
  SimulationRuntimeOperatorCommandRequest,
  SimulationRuntimeRunSummary,
  StrategyPackage,
} from "@/lib/paper-v2/types";

type OperatorCommandType = SimulationRuntimeOperatorCommandRequest["command_type"];

type OperatorCommandOption = {
  value: OperatorCommandType;
  label: string;
  summary: string;
  impact: string;
  requiresStrategySlot?: boolean;
  requiresAlphaSignalBook?: boolean;
};

const OPERATOR_COMMAND_OPTIONS: OperatorCommandOption[] = [
  {
    value: "CANCEL_ALL_OPEN_ORDERS",
    label: "撤销全部未成交委托",
    summary: "通过 MiniQMTExecutionRuntime 撤销当前账号组仍处于活跃状态的子单。",
    impact: "不会新增买卖单；可能改变当前挂单状态。",
  },
  {
    value: "FLATTEN_ALL_POSITIONS",
    label: "清空全部持仓",
    summary: "对 MiniQMT 模拟账号内可卖持仓生成受控清仓操作。",
    impact: "会影响整个 MiniQMT 模拟账号组，请仅在确认需要退出全部仓位时使用。",
  },
  {
    value: "FLATTEN_STRATEGY_SLOT",
    label: "清空当前策略槽持仓",
    summary: "仅对选中运行记录对应的策略槽执行清仓。",
    impact: "需要可追溯的 strategy_slot_id；不会处理其他策略槽。",
    requiresStrategySlot: true,
  },
  {
    value: "RESET_STRATEGY_SLOT",
    label: "重置当前策略槽",
    summary: "终结选中策略槽的运行时状态并撤销其活跃子单。",
    impact: "用于人工恢复策略槽；不会修改策略包源码或模型。",
    requiresStrategySlot: true,
  },
  {
    value: "REPLACE_ALPHA_SIGNAL_BOOK",
    label: "更换 Alpha 信号簿",
    summary: "把选中策略槽切换到一份可追溯的 Alpha 信号簿。",
    impact: "仅替换信号簿引用，不直接提交委托；必须从页面发现的信号簿候选中选择。",
    requiresStrategySlot: true,
    requiresAlphaSignalBook: true,
  },
];

const OPERATOR_COMMAND_BY_VALUE = new Map(OPERATOR_COMMAND_OPTIONS.map((item) => [item.value, item]));

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

function rawText(value: unknown): string {
  if (value === null || value === undefined || value === "") return "";
  return String(value);
}

function objectValue(value: unknown): JsonObject | null {
  return value && typeof value === "object" && !Array.isArray(value) ? value as JsonObject : null;
}

function itemValue(row: JsonObject | null | undefined, key: string): unknown {
  return row ? row[key] : undefined;
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

function portfolioAccountGroupId(row: PaperPortfolio | null | undefined): string {
  const autoRunConfig = objectValue(row?.auto_run_config);
  const autoRunSummary = objectValue(itemValue(row, "auto_run"));
  const binding = objectValue(itemValue(autoRunSummary, "binding"));
  const values = [
    autoRunConfig?.account_group_id,
    autoRunConfig?.broker_account_id,
    binding?.account_group_id,
    binding?.broker_account_id,
    row?.portfolio_id,
  ];
  for (const value of values) {
    const text = rawText(value).trim();
    if (text) return text;
  }
  return "";
}

function portfolioStrategySlotId(row: PaperPortfolio | null | undefined): string {
  const autoRunConfig = objectValue(row?.auto_run_config);
  const autoRunSummary = objectValue(itemValue(row, "auto_run"));
  const binding = objectValue(itemValue(autoRunSummary, "binding"));
  const values = [
    autoRunConfig?.strategy_slot_id,
    binding?.strategy_slot_id,
    row?.portfolio_id,
  ];
  for (const value of values) {
    const text = rawText(value).trim();
    if (text) return text;
  }
  return "";
}

function runtimeIdFromRun(row: SimulationRuntimeRunSummary | null | undefined): string | undefined {
  const evidence = runtimeEvidenceFromRun(row);
  const runtimeId = rawText(itemValue(evidence, "runtime_id")).trim();
  return runtimeId || undefined;
}

function runtimeEvidenceFromRun(row: SimulationRuntimeRunSummary | null | undefined): JsonObject | null {
  const batch = objectValue(itemValue(row?.broker_context, "qmt_batch_result"));
  return objectValue(itemValue(batch, "runtime_evidence"));
}

function runtimeHashFromRun(row: SimulationRuntimeRunSummary | null | undefined): string {
  return rawText(row?.execution_plan_hash || row?.binding_hash || row?.release_hash).trim();
}

function alphaSignalBookCandidates(rows: SimulationRuntimeRunSummary[]): string[] {
  const ids = new Set<string>();
  for (const row of rows) {
    for (const source of [
      row,
      row.broker_context,
      row.reconciliation_context,
      row.strategy_performance,
      runtimeEvidenceFromRun(row),
    ]) {
      const direct = rawText(itemValue(objectValue(source), "alpha_signal_book_id")).trim();
      if (direct) ids.add(direct);
    }
  }
  return Array.from(ids).sort();
}

function runDisplayValue(row: SimulationRuntimeRunSummary | null | undefined, key: string): string {
  const display = objectValue(itemValue(row, "display"));
  const value = rawText(itemValue(display, key)).trim();
  return value || "-";
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
  const [runtimeRuns, setRuntimeRuns] = useState<SimulationRuntimeRunSummary[]>([]);
  const [autoRunByPortfolio, setAutoRunByPortfolio] = useState<Record<string, PaperAutoRunSummary>>({});
  const [packages, setPackages] = useState<StrategyPackage[]>([]);
  const [policies, setPolicies] = useState<ExecutionPolicy[]>([]);
  const [selectedPortfolioId, setSelectedPortfolioId] = useState("");
  const [selectedRuntimeRunId, setSelectedRuntimeRunId] = useState("");
  const [operatorCommand, setOperatorCommand] = useState<OperatorCommandType>("CANCEL_ALL_OPEN_ORDERS");
  const [operatorReason, setOperatorReason] = useState("");
  const [operatorAlphaSignalBookId, setOperatorAlphaSignalBookId] = useState("");
  const [operatorSubmitting, setOperatorSubmitting] = useState(false);
  const [operatorConfirming, setOperatorConfirming] = useState(false);
  const [operatorResult, setOperatorResult] = useState<JsonObject | null>(null);
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
      const [nextStatus, portfolioPage, packageRows] = await Promise.all([
        qmtApi.status(),
        paperV2Api.listPortfoliosPage({ page: 1, pageSize: 50, brokerBackend: "minqmt_sim" }),
        strategyPackageApi.listSummary(undefined, 100),
      ]);
      const portfolioRows = portfolioPage.portfolios;
      setStatus(nextStatus);
      setBrokerAccountId((current) => current || String(nextStatus.account_id || ""));
      setPortfolios(portfolioRows);
      setPackages(packageRows);
      setBootstrapStatus(await paperV2Api.schedulerBootstrapStatus());
      const runsPayload = await simulationRuntimeApi.listRuns({
        brokerBackend: "minqmt_sim",
        limit: 50,
      });
      setRuntimeRuns(runsPayload.runs);
      if (nextStatus.connected) {
        const [nextAccount, nextPositions, nextOrders] = await Promise.all([
          qmtApi.account(),
          qmtApi.positions(),
          qmtApi.orders(false),
        ]);
        setAccount(nextAccount);
        setPositions(nextPositions);
        setOrders(nextOrders);
        setTrades([]);
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

  const connected = Boolean(status?.connected);
  const simMode = String(status?.mode || "").toUpperCase() === "SIM";

  useEffect(() => { load(); }, [load]);

  useEffect(() => {
    let alive = true;
    const miniRows = miniqmtPortfolios(portfolios).slice(0, 50);
    if (!miniRows.length) {
      setAutoRunByPortfolio({});
      return () => { alive = false; };
    }
    Promise.all(
      miniRows.map(async (row) => {
        try {
          return [row.portfolio_id, await paperV2Api.autoRunStatus(row.portfolio_id)] as const;
        } catch {
          return [row.portfolio_id, undefined] as const;
        }
      }),
    ).then((statusRows) => {
      if (!alive) return;
      const nextAutoRunByPortfolio: Record<string, PaperAutoRunSummary> = {};
      for (const [portfolioId, value] of statusRows) {
        if (value) nextAutoRunByPortfolio[portfolioId] = value;
      }
      setAutoRunByPortfolio(nextAutoRunByPortfolio);
    }).catch((exc) => {
      if (alive) setError(exc);
    });
    return () => { alive = false; };
  }, [portfolios]);

  useEffect(() => {
    if (!connected || !tradesExpanded || trades.length) return;
    let alive = true;
    qmtApi.trades().then((nextTrades) => {
      if (alive) setTrades(nextTrades);
    }).catch((exc) => {
      if (alive) setError(exc);
    });
    return () => { alive = false; };
  }, [connected, trades.length, tradesExpanded]);

  const miniPortfolios = useMemo(() => miniqmtPortfolios(portfolios), [portfolios]);
  const activeMiniPortfolios = miniPortfolios.filter((row) => ["READY", "RUNNING", "PAUSED"].includes(row.status));
  const eligiblePackages = useMemo(
    () => packages.filter(packageAssetEligible),
    [packages],
  );
  const provider = status?.provider || "-";
  const sortedPositions = useMemo(() => sortRows(positions, positionSort, positionSortValue), [positions, positionSort]);
  const selectedPortfolio = useMemo(
    () => miniPortfolios.find((row) => row.portfolio_id === selectedPortfolioId) || miniPortfolios[0] || null,
    [miniPortfolios, selectedPortfolioId],
  );
  const selectedRuntimeRun = useMemo(
    () => runtimeRuns.find((row) => row.run_id === selectedRuntimeRunId) || runtimeRuns[0] || null,
    [runtimeRuns, selectedRuntimeRunId],
  );
  const selectedCommandOption = OPERATOR_COMMAND_BY_VALUE.get(operatorCommand) || OPERATOR_COMMAND_OPTIONS[0];
  const alphaBookOptions = useMemo(() => alphaSignalBookCandidates(runtimeRuns), [runtimeRuns]);
  const operatorAccountGroupId = rawText(selectedRuntimeRun?.account_group_id).trim() || portfolioAccountGroupId(selectedPortfolio);
  const operatorStrategySlotId = rawText(selectedRuntimeRun?.strategy_slot_id).trim() || portfolioStrategySlotId(selectedPortfolio);
  const operatorRuntimeConfigHash = runtimeHashFromRun(selectedRuntimeRun);
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

  function requestOperatorCommand() {
    setError(null);
    setOperatorResult(null);
    if (operatorDisabledReason) {
      setError(new Error(operatorDisabledReason));
      return;
    }
    setOperatorConfirming(true);
  }

  async function executeConfirmedOperatorCommand() {
    if (operatorDisabledReason || !selectedRuntimeRun) return;
    setOperatorSubmitting(true);
    setError(null);
    setOperatorResult(null);
    try {
      const result = await simulationRuntimeApi.executeMiniQmtOperatorCommand({
        command_type: operatorCommand,
        account_group_id: operatorAccountGroupId,
        strategy_slot_id: operatorStrategySlotId || undefined,
        alpha_signal_book_id: selectedCommandOption.requiresAlphaSignalBook ? operatorAlphaSignalBookId : undefined,
        trade_date: selectedRuntimeRun.trade_date || todayIso(),
        runtime_config_hash: operatorRuntimeConfigHash,
        runtime_id: runtimeIdFromRun(selectedRuntimeRun),
        reason: operatorReason.trim(),
        confirm_text: `EXECUTE ${operatorCommand}`,
        requested_by: "paper-v2-miniqmt-sim-ui",
        payload: {
          selected_portfolio_id: selectedPortfolio?.portfolio_id,
          selected_run_id: selectedRuntimeRun.run_id,
          source: "miniqmt_sim_controlled_ops_card",
        },
      });
      setOperatorResult(result as unknown as JsonObject);
      setOperatorConfirming(false);
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setOperatorSubmitting(false);
    }
  }

  const operatorDisabledReason = !selectedRuntimeRun
    ? "请先选择一条 MiniQMT 模拟盘运行证据。"
    : !operatorReason.trim()
      ? "请填写运维原因。"
      : !operatorAccountGroupId
        ? "缺少 account_group_id，无法审计命令归属。"
        : selectedCommandOption.requiresStrategySlot && !operatorStrategySlotId
          ? "当前命令需要 strategy_slot_id，请选择带策略槽的运行记录。"
          : !operatorRuntimeConfigHash
            ? "缺少运行配置 hash，请选择已有 MiniQMT runtime run。"
            : selectedCommandOption.requiresAlphaSignalBook && !operatorAlphaSignalBookId
              ? "更换 Alpha 信号簿前必须先选择页面发现的信号簿。"
              : "";

  return (
    <main>
      <ErrorPanel error={error} title="MiniQMT 模拟盘检查失败" />
      <div className="pv2-grid pv2-grid-4">
        <div className="pv2-metric pv2-metric-info"><div className="pv2-metric-label">连接状态</div><div className="pv2-metric-value">{connected ? "已连接" : "未连接"}</div><div className="pv2-metric-hint">{provider} / {status?.client_class || "-"}</div></div>
        <div className="pv2-metric"><div className="pv2-metric-label">账号模式</div><div className="pv2-metric-value">{status?.mode || "-"}</div><div className="pv2-metric-hint">account_group_slots 多策略槽位</div></div>
        <div className="pv2-metric pv2-metric-success"><div className="pv2-metric-label">账号总资产</div><div className="pv2-metric-value">{fmt(numberValue(account, "total_asset"))}</div><div className="pv2-metric-hint">仅来自 MiniQMT account query</div></div>
        <div className="pv2-metric pv2-metric-warning"><div className="pv2-metric-label">自动调度</div><div className="pv2-metric-value">{bootstrapStatus?.scheduler?.running ? "运行中" : "未运行"}</div><div className="pv2-metric-hint">自动组合 {activeMiniPortfolios.filter((row) => row.auto_run_enabled).length}/{miniPortfolios.length}</div></div>
      </div>

      <NoticePanel title="交易权威边界" tone="warning">
        AIstock 只生成买卖方向、代码、数量和提交时间；MiniQMT 是唯一委托、拒单、成交、资金和持仓权威。本页面不会用 TDX、DB、tick 或 LocalSim 补成交，也不会展示每策略真实资金池。
      </NoticePanel>

      <SectionCard title="MiniQMT 模拟盘受控操作" eyebrow="运行时命令 / 二次确认">
        <NoticePanel title="仅限 MiniQMT 模拟盘" tone="warning">
          撤单、清仓、重置策略槽和更换 Alpha 信号簿都通过 MiniQMTExecutionRuntime 审计执行；页面不要求手工输入确认文本，点击提交后会弹出二次确认，再由前端生成后端所需的受控确认字段。
        </NoticePanel>
        <div className="pv2-form-grid" data-testid="miniqmt-operator-command-panel">
          <div className="pv2-field">
            <label>目标 MiniQMT 模拟盘</label>
            <select className="pv2-select" data-testid="miniqmt-operator-portfolio" value={selectedPortfolio?.portfolio_id || ""} onChange={(event) => setSelectedPortfolioId(event.target.value)}>
              {miniPortfolios.length ? miniPortfolios.map((item) => <option key={item.portfolio_id} value={item.portfolio_id}>{item.portfolio_name} / {item.status}</option>) : <option value="">暂无 MiniQMT 模拟盘</option>}
            </select>
          </div>
          <div className="pv2-field">
            <label>运行证据</label>
            <select className="pv2-select" data-testid="miniqmt-operator-runtime-run" value={selectedRuntimeRun?.run_id || ""} onChange={(event) => setSelectedRuntimeRunId(event.target.value)}>
              {runtimeRuns.length ? runtimeRuns.map((item) => (
                <option key={item.run_id} value={item.run_id}>
                  {item.trade_date} / {runDisplayValue(item, "account_slot_label")} / {item.status}
                </option>
              )) : <option value="">暂无 MiniQMT runtime run</option>}
            </select>
          </div>
          <div className="pv2-field">
            <label>受控命令</label>
            <select
              className="pv2-select"
              data-testid="miniqmt-operator-command-type"
              value={operatorCommand}
              onChange={(event) => {
                setOperatorCommand(event.target.value as OperatorCommandType);
                setOperatorAlphaSignalBookId("");
                setOperatorResult(null);
              }}
            >
              {OPERATOR_COMMAND_OPTIONS.map((item) => <option key={item.value} value={item.value}>{item.label}</option>)}
            </select>
          </div>
          <div className="pv2-field">
            <label>运维原因</label>
            <input className="pv2-input" data-testid="miniqmt-operator-reason" value={operatorReason} onChange={(event) => setOperatorReason(event.target.value)} placeholder="必填：说明为什么需要人工操作" />
          </div>
          {selectedCommandOption.requiresAlphaSignalBook ? (
            <div className="pv2-field">
              <label>选择 Alpha 信号簿</label>
              <select className="pv2-select" data-testid="miniqmt-operator-alpha-book" value={operatorAlphaSignalBookId} onChange={(event) => setOperatorAlphaSignalBookId(event.target.value)}>
                <option value="">请选择可追溯信号簿</option>
                {alphaBookOptions.map((item) => <option key={item} value={item}>{item}</option>)}
              </select>
              <span className="pv2-help">仅更换 Alpha 信号簿时需要；候选来自 MiniQMT runtime run 证据，不再要求手工输入复杂 ID。</span>
            </div>
          ) : null}
        </div>
        <div className="pv2-readable-panel">
          <div className="pv2-readable-table">
            <div className="pv2-readable-row"><div className="pv2-readable-key">命令说明</div><div className="pv2-readable-value">{selectedCommandOption.summary}</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">影响范围</div><div className="pv2-readable-value">{selectedCommandOption.impact}</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">账号组 / 策略槽</div><div className="pv2-readable-value pv2-mono">{operatorAccountGroupId || "-"} / {operatorStrategySlotId || "-"}</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">运行证据</div><div className="pv2-readable-value pv2-mono" data-testid="miniqmt-operator-selected-run">{selectedRuntimeRun?.run_id || "请选择 MiniQMT runtime run"}</div></div>
            <div className="pv2-readable-row"><div className="pv2-readable-key">最近结果</div><div className="pv2-readable-value" data-testid="miniqmt-operator-result">{asText(itemValue(objectValue(operatorResult?.result), "status") || itemValue(operatorResult, "ok"))}</div></div>
          </div>
        </div>
        {operatorDisabledReason ? <div className="pv2-help" data-testid="miniqmt-operator-disabled-reason">{operatorDisabledReason}</div> : null}
        <button className="pv2-button-danger" data-testid="miniqmt-operator-submit" type="button" onClick={requestOperatorCommand} disabled={Boolean(operatorDisabledReason) || operatorSubmitting}>
          {operatorSubmitting ? "执行中..." : "提交受控操作"}
        </button>
      </SectionCard>

      {operatorConfirming ? (
        <div className="pv2-modal-backdrop" role="presentation">
          <div className="pv2-modal-card" role="dialog" aria-modal="true" aria-labelledby="miniqmt-operator-confirm-title" data-testid="miniqmt-operator-confirm-dialog">
            <div className="pv2-eyebrow">MiniQMTExecutionRuntime</div>
            <h2 id="miniqmt-operator-confirm-title">确认执行：{selectedCommandOption.label}</h2>
            <p>{selectedCommandOption.summary}</p>
            <div className="pv2-readable-panel">
              <div className="pv2-readable-table">
                <div className="pv2-readable-row"><div className="pv2-readable-key">目标模拟盘</div><div className="pv2-readable-value">{selectedPortfolio?.portfolio_name || "-"}</div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">账号组 / 策略槽</div><div className="pv2-readable-value pv2-mono">{operatorAccountGroupId || "-"} / {operatorStrategySlotId || "-"}</div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">运行证据</div><div className="pv2-readable-value pv2-mono">{selectedRuntimeRun?.run_id || "-"}</div></div>
                <div className="pv2-readable-row"><div className="pv2-readable-key">原因</div><div className="pv2-readable-value">{operatorReason}</div></div>
                {selectedCommandOption.requiresAlphaSignalBook ? <div className="pv2-readable-row"><div className="pv2-readable-key">Alpha 信号簿</div><div className="pv2-readable-value pv2-mono">{operatorAlphaSignalBookId || "-"}</div></div> : null}
              </div>
            </div>
            <NoticePanel title="二次确认" tone="warning">
              点击“确认执行”后才会向后端提交受控命令；后端仍保留破坏性命令确认门和审计记录。
            </NoticePanel>
            <div className="pv2-row-actions" style={{ justifyContent: "flex-end" }}>
              <button className="pv2-button-ghost" type="button" onClick={() => setOperatorConfirming(false)} disabled={operatorSubmitting}>取消</button>
              <button className="pv2-button-danger" data-testid="miniqmt-operator-confirm-submit" type="button" onClick={executeConfirmedOperatorCommand} disabled={operatorSubmitting || Boolean(operatorDisabledReason)}>
                {operatorSubmitting ? "执行中..." : "确认执行"}
              </button>
            </div>
          </div>
        </div>
      ) : null}

      <SectionCard title="创建 MiniQMT 自动运行组合" eyebrow="account group slot auto-run" action={<button className="pv2-button-primary" onClick={createExclusivePortfolio} disabled={creating || !packageId || !brokerAccountId.trim()} type="button">{creating ? "创建中..." : "创建并启用自动运行"}</button>}>
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

      <SectionCard title="MiniQMT 组合清单" eyebrow="account group slots">
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
