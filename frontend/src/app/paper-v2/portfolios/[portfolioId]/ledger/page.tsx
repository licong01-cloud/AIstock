"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import ErrorPanel from "@/components/paper-v2/ErrorPanel";
import JsonPanel from "@/components/paper-v2/JsonPanel";
import MetricCard from "@/components/paper-v2/MetricCard";
import PaperTable from "@/components/paper-v2/PaperTable";
import SectionCard from "@/components/paper-v2/SectionCard";
import StatusBadge from "@/components/paper-v2/StatusBadge";
import { paperV2Api } from "@/lib/paper-v2/api";
import { formatNumber, shortHash } from "@/lib/paper-v2/format";
import type { JsonObject, PaperPortfolio } from "@/lib/paper-v2/types";

function text(row: JsonObject, key: string): string {
  const value = row[key];
  if (value === null || value === undefined || value === "") return "-";
  return String(value);
}

function compactJson(row: JsonObject, key: string): string {
  const value = row[key];
  if (!value || typeof value !== "object") return "-";
  return Object.keys(value as Record<string, unknown>).join(", ") || "{}";
}

export default function PaperV2LedgerPage() {
  const params = useParams<{ portfolioId: string }>();
  const portfolioId = String(params.portfolioId || "");
  const [portfolio, setPortfolio] = useState<PaperPortfolio | null>(null);
  const [orders, setOrders] = useState<JsonObject[]>([]);
  const [fills, setFills] = useState<JsonObject[]>([]);
  const [cash, setCash] = useState<JsonObject[]>([]);
  const [positions, setPositions] = useState<JsonObject[]>([]);
  const [snapshots, setSnapshots] = useState<JsonObject[]>([]);
  const [events, setEvents] = useState<JsonObject[]>([]);
  const [errors, setErrors] = useState<JsonObject[]>([]);
  const [selectedRow, setSelectedRow] = useState<JsonObject | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [portfolioRow, orderRows, fillRows, cashRows, positionRows, snapshotRows, eventRows, errorRows] = await Promise.all([
        paperV2Api.getPortfolio(portfolioId),
        paperV2Api.orders(portfolioId),
        paperV2Api.fills(portfolioId),
        paperV2Api.cashLedger(portfolioId),
        paperV2Api.positions(portfolioId),
        paperV2Api.snapshots(portfolioId),
        paperV2Api.runEvents(portfolioId),
        paperV2Api.errors(portfolioId),
      ]);
      setPortfolio(portfolioRow);
      setOrders(orderRows);
      setFills(fillRows);
      setCash(cashRows);
      setPositions(positionRows);
      setSnapshots(snapshotRows);
      setEvents(eventRows);
      setErrors(errorRows);
    } catch (exc) {
      setError(exc);
    } finally {
      setLoading(false);
    }
  }, [portfolioId]);

  useEffect(() => { load(); }, [load]);

  return (
    <main>
      <div className="pv2-detail-nav">
        <Link href={`/paper-v2/portfolios/${portfolioId}`}>详情</Link>
        <Link href={`/paper-v2/portfolios/${portfolioId}/run-console`}>运行控制台</Link>
        <Link href={`/paper-v2/portfolios/${portfolioId}/performance`}>绩效</Link>
      </div>
      <ErrorPanel error={error} title="账本加载失败" />

      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="订单" value={orders.length} hint={portfolio?.portfolio_name} />
        <MetricCard label="成交" value={fills.length} />
        <MetricCard label="现金流水" value={cash.length} tone="info" />
        <MetricCard label="快照" value={snapshots.length} tone={snapshots.length ? "success" : "warning"} />
      </div>

      <SectionCard title="订单" eyebrow={loading ? "加载中" : `portfolio ${shortHash(portfolioId)}`} action={<button className="pv2-button" onClick={load} type="button">刷新</button>}>
        <PaperTable
          rows={orders}
          empty="暂无订单。请先执行就绪检查和单日运行。"
          columns={[
            { key: "date", header: "创建时间", render: (row) => text(row, "created_at").slice(0, 19).replace("T", " ") },
            { key: "symbol", header: "股票代码", render: (row) => text(row, "symbol") },
            { key: "side", header: "方向", render: (row) => <StatusBadge status={text(row, "side")} /> },
            { key: "qty", header: "数量", render: (row) => formatNumber(row.quantity, 0) },
            { key: "filled", header: "已成交", render: (row) => formatNumber(row.filled_quantity, 0) },
            { key: "price", header: "均价", render: (row) => formatNumber(row.avg_fill_price, 4) },
            { key: "status", header: "状态", render: (row) => <StatusBadge status={text(row, "status")} /> },
            { key: "trace", header: "追踪", render: (row) => <button className="pv2-link-button" type="button" onClick={() => setSelectedRow(row)}>JSON</button> },
          ]}
        />
      </SectionCard>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="成交" eyebrow="成交驱动账本">
          <PaperTable
            rows={fills}
            empty="暂无成交。"
            columns={[
              { key: "time", header: "成交时间", render: (row) => text(row, "trade_time").slice(0, 19).replace("T", " ") },
              { key: "symbol", header: "股票代码", render: (row) => text(row, "symbol") },
              { key: "side", header: "方向", render: (row) => <StatusBadge status={text(row, "side")} /> },
              { key: "qty", header: "数量", render: (row) => formatNumber(row.quantity, 0) },
              { key: "price", header: "价格", render: (row) => formatNumber(row.price, 4) },
              { key: "reason", header: "原因", render: (row) => text(row, "reason") },
            ]}
          />
        </SectionCard>

        <SectionCard title="现金流水" eyebrow="资金追踪">
          <PaperTable
            rows={cash}
            empty="暂无现金流水。"
            columns={[
              { key: "date", header: "日期", render: (row) => text(row, "trade_date") },
              { key: "symbol", header: "股票代码", render: (row) => text(row, "symbol") },
              { key: "side", header: "方向", render: (row) => <StatusBadge status={text(row, "side")} /> },
              { key: "notional", header: "成交金额", render: (row) => formatNumber(row.notional, 2) },
              { key: "fee", header: "费用", render: (row) => formatNumber(row.fee, 2) },
              { key: "delta", header: "现金变化", render: (row) => formatNumber(row.cash_delta, 2) },
              { key: "after", header: "交易后现金", render: (row) => formatNumber(row.cash_after, 2) },
            ]}
          />
        </SectionCard>
      </div>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="持仓" eyebrow="日终持仓">
          <PaperTable
            rows={positions}
            empty="暂无持仓。"
            columns={[
              { key: "date", header: "日期", render: (row) => text(row, "trade_date") },
              { key: "symbol", header: "股票代码", render: (row) => text(row, "symbol") },
              { key: "qty", header: "数量", render: (row) => formatNumber(row.quantity, 0) },
              { key: "avail", header: "可用数量", render: (row) => formatNumber(row.available_quantity, 0) },
              { key: "cost", header: "平均成本", render: (row) => formatNumber(row.avg_cost, 4) },
              { key: "value", header: "市值", render: (row) => formatNumber(row.market_value, 2) },
            ]}
          />
        </SectionCard>

        <SectionCard title="日快照" eyebrow="净值与现金">
          <PaperTable
            rows={snapshots}
            empty="暂无日快照。"
            columns={[
              { key: "date", header: "日期", render: (row) => text(row, "trade_date") },
              { key: "cash", header: "现金", render: (row) => formatNumber(row.cash, 2) },
              { key: "mv", header: "市值", render: (row) => formatNumber(row.market_value, 2) },
              { key: "nav", header: "净值", render: (row) => formatNumber(row.nav, 2) },
              { key: "positions", header: "持仓数", render: (row) => formatNumber(row.position_count, 0) },
            ]}
          />
        </SectionCard>
      </div>

      <div className="pv2-grid pv2-grid-2">
        <SectionCard title="运行事件" eyebrow="执行追踪">
          <PaperTable
            rows={events}
            empty="暂无运行事件。"
            columns={[
              { key: "seq", header: "序号", render: (row) => text(row, "event_seq") },
              { key: "type", header: "类型", render: (row) => text(row, "event_type") },
              { key: "msg", header: "消息", render: (row) => text(row, "message") },
              { key: "ctx", header: "上下文", render: (row) => compactJson(row, "context") },
            ]}
          />
        </SectionCard>

        <SectionCard title="错误" eyebrow="Fail-fast 持久化">
          <PaperTable
            rows={errors}
            empty="暂无错误。"
            columns={[
              { key: "time", header: "创建时间", render: (row) => text(row, "created_at").slice(0, 19).replace("T", " ") },
              { key: "code", header: "代码", render: (row) => <StatusBadge status={text(row, "error_code")} /> },
              { key: "message", header: "消息", render: (row) => text(row, "message") },
              { key: "context", header: "上下文", render: (row) => compactJson(row, "context") },
            ]}
          />
        </SectionCard>
      </div>

      {selectedRow ? (
        <SectionCard title="所选记录 JSON" eyebrow="追踪详情" action={<button className="pv2-button-ghost" onClick={() => setSelectedRow(null)} type="button">关闭</button>}>
          <JsonPanel value={selectedRow} />
        </SectionCard>
      ) : null}
    </main>
  );
}
