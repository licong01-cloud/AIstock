"use client";

import React, { useState } from "react";
import { StockTradeDetail, type Trade } from "./StockTradeDetail";

export interface StockEntry {
  code: string;
  profit: number;
  profit_pct: number;
  avg_cost?: number;
  last_price?: number;
  holding_days: number;
  first_date: string;
  last_date: string;
}

interface TopStocksTableProps {
  title: string;
  titleColor: string;
  stocks: StockEntry[];
  stockTrades?: Record<string, Trade[]>;
  /** 盈利表用红色，亏损表用绿色 */
  variant: "profit" | "loss";
}

export function TopStocksTable({ title, titleColor, stocks, stockTrades, variant }: TopStocksTableProps) {
  const [expandedStock, setExpandedStock] = useState<string | null>(null);

  if (!stocks || stocks.length === 0) return null;

  const profitColor = variant === "profit" ? "#e53935" : "#22a35a";
  const expandBg = variant === "profit" ? "#fff7f7" : "#f0fff4";

  return (
    <div style={{ backgroundColor: "#fff", borderRadius: 8, border: "1px solid #e2e8f0", padding: 20 }}>
      <h3 style={{ margin: "0 0 16px", fontSize: 13, fontWeight: 700, color: titleColor, textTransform: "uppercase", letterSpacing: "0.05em" }}>
        {title}
      </h3>
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: "monospace" }}>
          <thead>
            <tr style={{ borderBottom: "2px solid #e2e8f0", textAlign: "left" }}>
              <th style={{ padding: "8px 6px", color: "#64748b", fontWeight: 600 }}>#</th>
              <th style={{ padding: "8px 6px", color: "#64748b", fontWeight: 600 }}>股票代码</th>
              <th style={{ padding: "8px 6px", color: "#64748b", fontWeight: 600, textAlign: "right" }}>
                {variant === "profit" ? "累计盈亏" : "累计亏损"}
              </th>
              <th style={{ padding: "8px 6px", color: "#64748b", fontWeight: 600, textAlign: "right" }}>
                {variant === "profit" ? "收益率" : "亏损率"}
              </th>
              <th style={{ padding: "8px 6px", color: "#64748b", fontWeight: 600, textAlign: "right" }}>均价成本</th>
              <th style={{ padding: "8px 6px", color: "#64748b", fontWeight: 600, textAlign: "right" }}>最新价</th>
              <th style={{ padding: "8px 6px", color: "#64748b", fontWeight: 600, textAlign: "right" }}>持仓天数</th>
              <th style={{ padding: "8px 6px", color: "#64748b", fontWeight: 600 }}>首次持仓</th>
              <th style={{ padding: "8px 6px", color: "#64748b", fontWeight: 600 }}>最后持仓</th>
            </tr>
          </thead>
          <tbody>
            {stocks.map((s, i) => (
              <React.Fragment key={s.code}>
                <tr
                  style={{ borderBottom: "1px solid #f1f5f9", cursor: "pointer", backgroundColor: expandedStock === s.code ? expandBg : undefined }}
                  onClick={() => setExpandedStock(expandedStock === s.code ? null : s.code)}
                >
                  <td style={{ padding: "6px", color: "#94a3b8" }}>{i + 1}</td>
                  <td style={{ padding: "6px", fontWeight: 600, color: "#0f172a" }}>
                    {s.code} <span style={{ color: "#94a3b8", fontSize: 10 }}>{expandedStock === s.code ? "▲" : "▼"}</span>
                  </td>
                  <td style={{ padding: "6px", textAlign: "right", color: profitColor, fontWeight: 600 }}>
                    {variant === "profit" ? "+" : ""}
                    {typeof s.profit === "number" ? s.profit.toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : "-"}
                  </td>
                  <td style={{ padding: "6px", textAlign: "right", color: profitColor }}>
                    {typeof s.profit_pct === "number" ? (s.profit_pct * 100).toFixed(2) + "%" : "-"}
                  </td>
                  <td style={{ padding: "6px", textAlign: "right", color: "#334155" }}>{s.avg_cost?.toFixed(2) ?? "-"}</td>
                  <td style={{ padding: "6px", textAlign: "right", color: "#334155" }}>{s.last_price?.toFixed(2) ?? "-"}</td>
                  <td style={{ padding: "6px", textAlign: "right", color: "#334155" }}>{s.holding_days}</td>
                  <td style={{ padding: "6px", color: "#64748b" }}>{s.first_date}</td>
                  <td style={{ padding: "6px", color: "#64748b" }}>{s.last_date}</td>
                </tr>
                {expandedStock === s.code && (
                  <tr>
                    <td colSpan={9} style={{ padding: "0 0 8px 24px", backgroundColor: "#fafafa" }}>
                      <StockTradeDetail trades={stockTrades?.[s.code]} />
                    </td>
                  </tr>
                )}
              </React.Fragment>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
