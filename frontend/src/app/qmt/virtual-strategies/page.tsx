"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import JsonPanel from "@/components/paper-v2/JsonPanel";
import NoticePanel from "@/components/paper-v2/NoticePanel";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { qmtApi, type QmtStatus } from "@/lib/paper-v2/api";
import { asText, formatCompact, formatNumber, shortHash } from "@/lib/paper-v2/format";
import { qmtStrategyLedgerApi, type QmtVirtualStrategySummary } from "@/lib/qmt-strategy-ledger/api";
import type { JsonObject } from "@/lib/paper-v2/types";

function todayIso(): string {
  const now = new Date();
  return new Date(now.getTime() - now.getTimezoneOffset() * 60_000).toISOString().slice(0, 10);
}

function textValue(row: JsonObject | null | undefined, key: string): string {
  const raw = row?.[key];
  if (raw === null || raw === undefined || raw === "") return "-";
  return String(raw);
}

function arrayValue(value: unknown): JsonObject[] {
  return Array.isArray(value) ? value.filter((item): item is JsonObject => Boolean(item) && typeof item === "object" && !Array.isArray(item)) : [];
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.map((item) => String(item)) : [];
}

function objectValue(value: unknown): JsonObject | null {
  return value && typeof value === "object" && !Array.isArray(value) ? value as JsonObject : null;
}

function metricTone(count: number, warning = 1): string {
  return count >= warning ? "pv2-metric-warning" : "pv2-metric-success";
}

export default function QmtVirtualStrategiesPage() {
  const [accountId, setAccountId] = useState("");
  const [tradeDate, setTradeDate] = useState(todayIso());
  const [summary, setSummary] = useState<QmtVirtualStrategySummary | null>(null);
  const [qmtStatus, setQmtStatus] = useState<QmtStatus | null>(null);
  const [syncResult, setSyncResult] = useState<JsonObject | null>(null);
  const [reconcileResult, setReconcileResult] = useState<JsonObject | null>(null);
  const [bindingResult, setBindingResult] = useState<JsonObject | null>(null);
  const [previewResult, setPreviewResult] = useState<JsonObject | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(false);
  const [action, setAction] = useState<string | null>(null);

  const [strategyId, setStrategyId] = useState("");
  const [packageId, setPackageId] = useState("");
  const [selectionRunId, setSelectionRunId] = useState("");
  const [targetWeight, setTargetWeight] = useState("0.02");
  const [topK, setTopK] = useState("20");
  const [bindingId, setBindingId] = useState("");
  const [defaultTargetWeight, setDefaultTargetWeight] = useState("0.02");
  const [previewTopK, setPreviewTopK] = useState("20");
  const [orderRemarkPrefix, setOrderRemarkPrefix] = useState("qmtpkg");

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [nextStatus, nextSummary] = await Promise.all([
        qmtApi.status().catch((exc) => ({ enabled: false, connected: false, account_id: null, last_error: exc instanceof Error ? exc.message : String(exc) } as QmtStatus)),
        qmtStrategyLedgerApi.summary({ account_id: accountId.trim() || undefined, trade_date: tradeDate }),
      ]);
      setQmtStatus(nextStatus);
      setSummary(nextSummary);
      if (!accountId.trim() && nextStatus.account_id) setAccountId(String(nextStatus.account_id));
      const strategies = arrayValue(nextSummary.strategies);
      if (!strategyId && strategies.length) setStrategyId(textValue(strategies[0], "strategy_id"));
      const firstBinding = strategies.map((row) => row.active_binding).find(Boolean) as JsonObject | undefined;
      if (!bindingId && firstBinding?.binding_id) setBindingId(String(firstBinding.binding_id));
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [accountId, bindingId, strategyId, tradeDate]);

  useEffect(() => { load(); }, [load]);

  const strategies = useMemo(() => arrayValue(summary?.strategies), [summary]);
  const overlapSymbols = useMemo(() => stringArray(summary?.overlap_symbols), [summary]);
  const selectedStrategy = strategies.find((row) => textValue(row, "strategy_id") === strategyId) || strategies[0] || null;
  const selectedPositions = arrayValue(selectedStrategy?.positions);
  const activeBinding = (selectedStrategy?.active_binding && typeof selectedStrategy.active_binding === "object") ? selectedStrategy.active_binding as JsonObject : null;
  const brokerConnected = Boolean(qmtStatus?.connected);
  const currentAccountId = accountId.trim() || qmtStatus?.account_id || "";

  async function runAction<T extends JsonObject>(label: string, fn: () => Promise<T>, after?: (value: T) => void) {
    setAction(label);
    setError(null);
    try {
      const result = await fn();
      after?.(result);
      await load();
    } catch (exc) {
      setError(exc);
    } finally {
      setAction(null);
    }
  }

  async function syncSnapshot() {
    if (!currentAccountId) throw new Error("请先填写 account_id，或确认 MiniQMT 状态接口返回账号。 ");
    await runAction("同步中", () => qmtStrategyLedgerApi.syncSnapshot({ account_id: currentAccountId, trade_date: tradeDate }), setSyncResult);
  }

  async function reconcile() {
    if (!currentAccountId) throw new Error("请先填写 account_id，或确认 MiniQMT 状态接口返回账号。 ");
    await runAction("对账中", () => qmtStrategyLedgerApi.reconcile({ account_id: currentAccountId, trade_date: tradeDate }), setReconcileResult);
  }

  async function bindPackage() {
    await runAction(
      "绑定中",
      () => qmtStrategyLedgerApi.bindPackage({
        strategy_id: strategyId,
        package_id: packageId.trim(),
        selection_run_id: selectionRunId.trim(),
        trade_date: tradeDate,
        target_weight: targetWeight.trim() || undefined,
        top_k: topK.trim() ? Number(topK) : undefined,
        runtime_config: { source: "qmt_virtual_strategy_ui" },
      }),
      (result) => {
        setBindingResult(result);
        const binding = objectValue(result.binding);
        if (binding?.binding_id) setBindingId(String(binding.binding_id));
      },
    );
  }

  async function previewOrders() {
    const id = bindingId.trim() || textValue(activeBinding, "binding_id");
    if (!id || id === "-") throw new Error("请先选择已有 binding_id，或完成策略包绑定。 ");
    await runAction(
      "预检中",
      () => qmtStrategyLedgerApi.previewOrdersFromBinding(id, {
        default_target_weight: defaultTargetWeight.trim() || undefined,
        top_k: previewTopK.trim() ? Number(previewTopK) : undefined,
        price_type: 5,
        buy_price_slippage_bps: 0,
        sell_price_slippage_bps: 0,
        order_remark_prefix: orderRemarkPrefix.trim() || "qmtpkg",
        mode: "SIM",
      }),
      setPreviewResult,
    );
  }

  const reconcileReport = objectValue(reconcileResult?.report);
  const reconcileIssues = arrayValue(reconcileReport?.issues);
  const strategyLotQuantities = reconcileReport?.strategy_lot_quantities as Record<string, Record<string, number>> | undefined;
  const brokerQuantities = reconcileReport?.broker_quantities as Record<string, number> | undefined;
  const orderBuild = objectValue(previewResult?.order_build);
  const builtRequests = arrayValue(orderBuild?.requests);
  const preflights = arrayValue(previewResult?.preflights);

  return (
    <main>
      <section className="pv2-hero" style={{ marginBottom: 16 }}>
        <div className="pv2-hero-top">
          <div>
            <div className="pv2-kicker">MiniQMT multi-strategy ledger</div>
            <h1>MiniQMT 虚拟策略分仓看板</h1>
            <p>
              这里把 MiniQMT 原生账户视角与 AIstock 虚拟策略账户视角并排展示。MiniQMT 仍是委托、成交、资金和合并持仓权威；AIstock 只负责 strategy_name/order_remark 归因、分仓账本和默认关闭的下单预检。
            </p>
          </div>
          <div className="pv2-chip-row">
            <span className="pv2-chip">Broker 权威</span>
            <span className="pv2-chip">策略账本只读同步</span>
            <span className="pv2-chip">真实提交默认关闭</span>
          </div>
        </div>
      </section>

      <ErrorPanel error={error} title="MiniQMT 虚拟策略操作失败" />
      <NoticePanel title="安全边界" tone="warning">
        本页只调用 sync、reconciliation、package binding 和订单 preview 接口；不会调用 /orders、/orders/batch 或 /orders/cancel，不会提交或撤销真实 MiniQMT 委托。当前旧 monitor 若展示策略收益，仍属于 broker 观察口径，非严格策略级收益权威。
      </NoticePanel>

      <div className="pv2-grid pv2-grid-4">
        <div className="pv2-metric pv2-metric-info"><div className="pv2-metric-label">MiniQMT 连接</div><div className="pv2-metric-value">{brokerConnected ? "已连接" : "未连接"}</div><div className="pv2-metric-hint">{qmtStatus?.provider || "-"} / {qmtStatus?.mode || "-"}</div></div>
        <div className="pv2-metric"><div className="pv2-metric-label">虚拟策略</div><div className="pv2-metric-value">{formatNumber(summary?.strategy_count || strategies.length, 0)}</div><div className="pv2-metric-hint">AIstock 分仓账户，不是券商子账户</div></div>
        <div className={`pv2-metric ${metricTone(overlapSymbols.length)}`}><div className="pv2-metric-label">同股多策略</div><div className="pv2-metric-value">{formatNumber(overlapSymbols.length, 0)}</div><div className="pv2-metric-hint">{overlapSymbols.length ? overlapSymbols.join(", ") : "暂无重叠持仓"}</div></div>
        <div className={`pv2-metric ${metricTone(Number(summary?.unattributed_orders || 0) + Number(summary?.unattributed_trades || 0))}`}><div className="pv2-metric-label">未归因回报</div><div className="pv2-metric-value">{formatNumber(Number(summary?.unattributed_orders || 0) + Number(summary?.unattributed_trades || 0), 0)}</div><div className="pv2-metric-hint">空策略名、重复 remark 或未知成交</div></div>
      </div>

      <SectionCard title="操作上下文" eyebrow="account and trade date" action={<button className="pv2-button" onClick={load} disabled={loading} type="button">{loading ? "刷新中..." : "刷新看板"}</button>}>
        <div className="pv2-form-grid">
          <div className="pv2-field"><label>MiniQMT account_id</label><input className="pv2-input" value={accountId} onChange={(event) => setAccountId(event.target.value)} placeholder="例如 62266303" /></div>
          <div className="pv2-field"><label>trade_date</label><input className="pv2-input" type="date" value={tradeDate} onChange={(event) => setTradeDate(event.target.value)} /></div>
          <div className="pv2-field"><label>当前账号</label><input className="pv2-input" value={currentAccountId || "未识别"} readOnly /></div>
        </div>
        <div className="pv2-row-actions" style={{ marginTop: 14 }}>
          <button className="pv2-button-primary" onClick={syncSnapshot} disabled={Boolean(action)} type="button">{action === "同步中" ? "同步中..." : "只读同步 MiniQMT 快照"}</button>
          <button className="pv2-button" onClick={reconcile} disabled={Boolean(action)} type="button">{action === "对账中" ? "对账中..." : "执行分仓对账"}</button>
          <button className="pv2-button-ghost" disabled type="button" title="Phase 6 看板不暴露真实提交入口">真实下单入口默认关闭</button>
        </div>
      </SectionCard>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="MiniQMT 原生账户视角" eyebrow="broker authority">
          <div className="pv2-readable-panel">
            <div className="pv2-readable-table">
              <div className="pv2-readable-row"><div className="pv2-readable-key">连接状态</div><div className="pv2-readable-value"><StatusBadge status={brokerConnected ? "CONNECTED" : "DISCONNECTED"} /></div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">账号 / 模式</div><div className="pv2-readable-value pv2-mono">{qmtStatus?.account_id || "-"} / {qmtStatus?.mode || "-"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">provider</div><div className="pv2-readable-value">{qmtStatus?.provider || "-"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">session_id</div><div className="pv2-readable-value pv2-mono">{qmtStatus?.session_id ?? "-"}</div></div>
              <div className="pv2-readable-row"><div className="pv2-readable-key">last_error</div><div className="pv2-readable-value">{qmtStatus?.last_error || "-"}</div></div>
            </div>
          </div>
          <NoticePanel title="观察口径" tone="info">MiniQMT 原生账户只能看到合并资金、合并持仓、合并订单和合并成交；同一股票被多个策略持有时，需要 AIstock 虚拟策略账本做归因。</NoticePanel>
        </SectionCard>

        <SectionCard title="AIstock 虚拟策略账户视角" eyebrow="strategy ledger">
          <PaperTable
            rows={strategies}
            empty="暂无 qmt_strategy.virtual_account 记录。"
            columns={[
              { key: "strategy", header: "策略", render: (row) => <button className="pv2-link-button" onClick={() => setStrategyId(textValue(row, "strategy_id"))} type="button">{textValue(row, "strategy_name")}</button> },
              { key: "status", header: "状态", render: (row) => <StatusBadge status={row.status} /> },
              { key: "cash", header: "现金/冻结", render: (row) => `${formatCompact(row.cash)} / ${formatCompact(row.frozen_cash)}` },
              { key: "pnl", header: "已/未实现收益", render: (row) => `${formatCompact(row.realized_pnl)} / ${formatCompact(row.unrealized_pnl)}` },
              { key: "binding", header: "策略包绑定", render: (row) => {
                const binding = row.active_binding as JsonObject | null | undefined;
                return binding ? <span className="pv2-mono">{shortHash(binding.package_id, 7)} / {shortHash(binding.selection_run_id, 7)}</span> : <span className="pv2-muted">未绑定</span>;
              } },
            ]}
          />
        </SectionCard>
      </div>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="策略持仓 lot 汇总" eyebrow={selectedStrategy ? textValue(selectedStrategy, "strategy_name") : "no strategy"}>
          <PaperTable
            rows={selectedPositions}
            empty="当前策略暂无可归因 lot。"
            columns={[
              { key: "symbol", header: "股票", render: (row) => <span className="pv2-mono">{textValue(row, "symbol")}</span> },
              { key: "qty", header: "数量/可卖", render: (row) => `${formatNumber(row.remaining_quantity, 0)} / ${formatNumber(row.available_quantity, 0)}` },
              { key: "cost", header: "成本/均价", render: (row) => `${formatCompact(row.cost_amount)} / ${formatNumber(row.avg_cost, 4)}` },
              { key: "lots", header: "lot", render: (row) => formatNumber(row.lot_count, 0) },
            ]}
          />
        </SectionCard>

        <SectionCard title="同股多策略归因" eyebrow="overlap symbols">
          <PaperTable
            rows={overlapSymbols.map((symbol) => ({ symbol }))}
            empty="暂无多个策略共同持有的股票。"
            columns={[
              { key: "symbol", header: "股票", render: (row) => <span className="pv2-mono">{textValue(row, "symbol")}</span> },
              { key: "strategies", header: "涉及策略", render: (row) => strategies.filter((strategy) => arrayValue(strategy.positions).some((pos) => textValue(pos, "symbol") === textValue(row, "symbol"))).map((strategy) => textValue(strategy, "strategy_name")).join(" / ") || "-" },
              { key: "quantity", header: "策略合计", render: (row) => formatNumber(strategies.reduce((sum, strategy) => sum + arrayValue(strategy.positions).filter((pos) => textValue(pos, "symbol") === textValue(row, "symbol")).reduce((inner, pos) => inner + Number(pos.remaining_quantity || 0), 0), 0), 0) },
            ]}
          />
        </SectionCard>
      </div>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="绑定 StrategyPackage" eyebrow="package binding" action={<button className="pv2-button-primary" onClick={bindPackage} disabled={Boolean(action) || !strategyId || !packageId.trim() || !selectionRunId.trim()} type="button">{action === "绑定中" ? "绑定中..." : "创建绑定"}</button>}>
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>strategy_id</label><select className="pv2-select" value={strategyId} onChange={(event) => setStrategyId(event.target.value)}>{strategies.map((row) => <option value={textValue(row, "strategy_id")} key={textValue(row, "strategy_id")}>{textValue(row, "strategy_name")}</option>)}</select></div>
            <div className="pv2-field"><label>package_id</label><input className="pv2-input" value={packageId} onChange={(event) => setPackageId(event.target.value)} placeholder="策略包 ID" /></div>
            <div className="pv2-field"><label>selection_run_id</label><input className="pv2-input" value={selectionRunId} onChange={(event) => setSelectionRunId(event.target.value)} placeholder="Selection Run ID" /></div>
            <div className="pv2-field"><label>target_weight</label><input className="pv2-input" value={targetWeight} onChange={(event) => setTargetWeight(event.target.value)} /></div>
            <div className="pv2-field"><label>top_k</label><input className="pv2-input" value={topK} onChange={(event) => setTopK(event.target.value)} /></div>
            <div className="pv2-field"><label>active binding</label><input className="pv2-input" value={textValue(activeBinding, "binding_id")} readOnly /></div>
          </div>
          {bindingResult ? <JsonPanel value={bindingResult} /> : null}
        </SectionCard>

        <SectionCard title="策略包订单预检" eyebrow="preview only" action={<button className="pv2-button-primary" onClick={previewOrders} disabled={Boolean(action)} type="button">{action === "预检中" ? "预检中..." : "生成订单预检"}</button>}>
          <div className="pv2-form-grid">
            <div className="pv2-field"><label>binding_id</label><input className="pv2-input" value={bindingId} onChange={(event) => setBindingId(event.target.value)} placeholder="绑定 ID" /></div>
            <div className="pv2-field"><label>default_target_weight</label><input className="pv2-input" value={defaultTargetWeight} onChange={(event) => setDefaultTargetWeight(event.target.value)} /></div>
            <div className="pv2-field"><label>top_k</label><input className="pv2-input" value={previewTopK} onChange={(event) => setPreviewTopK(event.target.value)} /></div>
            <div className="pv2-field"><label>order_remark_prefix</label><input className="pv2-input" value={orderRemarkPrefix} onChange={(event) => setOrderRemarkPrefix(event.target.value)} /></div>
            <div className="pv2-field"><label>mode</label><input className="pv2-input" value="SIM preview only" readOnly /></div>
          </div>
          <NoticePanel title="不会真实提交" tone="info">本预检只创建内存中的订单请求展示和 preflight 结果，真实 submit/cancel API 仍受后端环境变量双重保护，且本页不调用这些端点。</NoticePanel>
        </SectionCard>
      </div>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="对账异常" eyebrow="reconciliation issues">
          <PaperTable
            rows={reconcileIssues}
            empty="尚未执行对账，或对账未发现异常。"
            columns={[
              { key: "type", header: "类型", render: (row) => <StatusBadge status={row.issue_type} /> },
              { key: "severity", header: "级别", render: (row) => <StatusBadge status={row.severity} /> },
              { key: "symbol", header: "股票", render: (row) => textValue(row, "symbol") },
              { key: "message", header: "说明", render: (row) => asText(row.message) },
            ]}
          />
        </SectionCard>

        <SectionCard title="Broker 合并数量 vs 策略 lot" eyebrow="position reconciliation">
          <PaperTable
            rows={Object.keys({ ...(brokerQuantities || {}), ...(strategyLotQuantities ? Object.values(strategyLotQuantities).reduce<Record<string, number>>((acc, row) => { Object.keys(row).forEach((symbol) => { acc[symbol] = (acc[symbol] || 0) + Number(row[symbol] || 0); }); return acc; }, {}) : {}) }).sort().map((symbol) => ({ symbol }))}
            empty="尚未执行对账。"
            columns={[
              { key: "symbol", header: "股票", render: (row) => <span className="pv2-mono">{textValue(row, "symbol")}</span> },
              { key: "broker", header: "MiniQMT 数量", render: (row) => formatNumber(brokerQuantities?.[textValue(row, "symbol")] || 0, 0) },
              { key: "strategy", header: "策略 lot 合计", render: (row) => formatNumber(strategyLotQuantities ? Object.values(strategyLotQuantities).reduce((sum, item) => sum + Number(item[textValue(row, "symbol")] || 0), 0) : 0, 0) },
              { key: "detail", header: "策略分布", render: (row) => strategyLotQuantities ? Object.entries(strategyLotQuantities).filter(([, item]) => Number(item[textValue(row, "symbol")] || 0) > 0).map(([name, item]) => `${name}:${formatNumber(item[textValue(row, "symbol")], 0)}`).join(" / ") || "-" : "-" },
            ]}
          />
        </SectionCard>
      </div>

      <SectionCard title="预检订单结果" eyebrow="managed order requests">
        <PaperTable
          rows={builtRequests}
          empty="尚未生成订单预检。"
          columns={[
            { key: "symbol", header: "股票", render: (row) => <span className="pv2-mono">{textValue(row, "symbol")}</span> },
            { key: "side", header: "方向", render: (row) => <StatusBadge status={row.side} /> },
            { key: "qty", header: "数量", render: (row) => formatNumber(row.quantity, 0) },
            { key: "price", header: "价格", render: (row) => formatNumber(row.price, 4) },
            { key: "remark", header: "order_remark", render: (row) => <span className="pv2-mono">{textValue(row, "order_remark")}</span> },
            { key: "preflight", header: "预检", render: (_row, index) => <StatusBadge status={preflights[index]?.allowed ? "PASSED" : "FAILED"} /> },
          ]}
        />
        {syncResult ? <NoticePanel title="最近同步结果" tone="success" context={syncResult}>MiniQMT orders/trades/positions 已按只读方式同步到策略账本边界。</NoticePanel> : null}
        {previewResult ? <JsonPanel value={{ skipped: (orderBuild?.skipped as unknown[]) || [], preflights }} /> : null}
      </SectionCard>
    </main>
  );
}
