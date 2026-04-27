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
import { formatNumber, shortHash, statusLabel } from "@/lib/paper-v2/format";
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

function OrderTraceDetail({ row, onClose }: { row: JsonObject; onClose: () => void }) {
  return (
    <div className="pv2-trace-panel" id="ledger-order-trace-detail" data-testid="ledger-order-trace-detail">
      <div className="pv2-card-head">
        <div>
          <div className="pv2-eyebrow">ORDER TRACE</div>
          <h3>订单追踪详情</h3>
          <p className="pv2-help">
            这里展示的是后端持久化的订单原始审计数据，用于核对策略来源、订单状态、成交数量和执行上下文；不是“失败”标记。
          </p>
        </div>
        <button className="pv2-button-ghost" data-testid="ledger-trace-close" onClick={onClose} type="button">
          关闭
        </button>
      </div>
      <div className="pv2-grid pv2-grid-4">
        <MetricCard label="股票" value={text(row, "symbol")} />
        <MetricCard label="方向" value={statusLabel(text(row, "side"))} />
        <MetricCard
          label="状态"
          value={statusLabel(text(row, "status"))}
          tone={String(row.status || "").toUpperCase() === "FILLED" ? "success" : "warning"}
        />
        <MetricCard label="成交数量" value={formatNumber(row.filled_quantity, 0)} />
      </div>
      <JsonPanel value={row} />
    </div>
  );
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

      <SectionCard
        title="订单"
        eyebrow={loading ? "加载中" : `portfolio ${shortHash(portfolioId)}`}
        action={<button className="pv2-button" data-testid="ledger-refresh" onClick={load} type="button">刷新</button>}
      >
        <div className="pv2-help" style={{ marginBottom: 10 }}>
          订单状态会按业务含义中文展示：FILLED 表示“已全部成交”，不是失败；最后一列是审计追踪入口，点击“查看追踪”会在订单表下方展开该订单的策略来源、成交数量和执行上下文。
        </div>
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
            {
              key: "trace",
              header: "订单追踪",
              render: (row, index) => (
                <button
                  aria-controls="ledger-order-trace-detail"
                  aria-expanded={selectedRow === row}
                  className="pv2-order-trace-button"
                  data-testid={`ledger-order-trace-${index}`}
                  title="查看该订单的后端原始审计数据与执行上下文"
                  type="button"
                  onClick={() => setSelectedRow(row)}
                >
                  {selectedRow === row ? "已打开" : "查看追踪"}
                </button>
              ),
            },
          ]}
        />
        {selectedRow ? <OrderTraceDetail row={selectedRow} onClose={() => setSelectedRow(null)} /> : null}
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

    </main>
  );
}
