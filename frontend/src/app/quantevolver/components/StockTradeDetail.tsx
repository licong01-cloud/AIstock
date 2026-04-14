"use client";

import React from "react";

export interface Trade {
  date: string;
  type: string;
  price: number;
  amount: number;
  pnl: number | null;
}

export function StockTradeDetail({ trades }: { trades?: Trade[] }) {
  if (!trades || trades.length === 0)
    return <div style={{ padding: "12px", color: "#94a3b8", fontSize: 12 }}>暂无交易记录</div>;
  return (
    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, fontFamily: "monospace", marginTop: 6 }}>
      <thead>
        <tr style={{ borderBottom: "1px solid #e2e8f0" }}>
          <th style={{ padding: "4px 8px", color: "#64748b", textAlign: "left" }}>日期</th>
          <th style={{ padding: "4px 8px", color: "#64748b", textAlign: "left" }}>操作</th>
          <th style={{ padding: "4px 8px", color: "#64748b", textAlign: "right" }}>价格</th>
          <th style={{ padding: "4px 8px", color: "#64748b", textAlign: "right" }}>金额</th>
          <th style={{ padding: "4px 8px", color: "#64748b", textAlign: "right" }}>本次盈亏</th>
        </tr>
      </thead>
      <tbody>
        {trades.map((t, i) => {
          const price = typeof t.price === "number" && isFinite(t.price) ? t.price : null;
          const amount = typeof t.amount === "number" && isFinite(t.amount) ? t.amount : null;
          const pnl = typeof t.pnl === "number" && isFinite(t.pnl) ? t.pnl : null;
          return (
          <tr key={i} style={{ borderBottom: "1px solid #f8fafc" }}>
            <td style={{ padding: "4px 8px", color: "#475569" }}>{t.date ?? "-"}</td>
            <td style={{ padding: "4px 8px", color: t.type === "buy" ? "#e53935" : "#22a35a", fontWeight: 600 }}>
              {t.type === "buy" ? "买入" : "卖出"}
            </td>
            <td style={{ padding: "4px 8px", textAlign: "right", color: "#334155" }}>{price != null ? price.toFixed(4) : "-"}</td>
            <td style={{ padding: "4px 8px", textAlign: "right", color: "#334155" }}>
              {amount != null ? amount.toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : "-"}
            </td>
            <td
              style={{
                padding: "4px 8px",
                textAlign: "right",
                color: pnl == null ? "#94a3b8" : pnl >= 0 ? "#e53935" : "#22a35a",
                fontWeight: 600,
              }}
            >
              {pnl == null ? "-" : (pnl >= 0 ? "+" : "") + pnl.toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
            </td>
          </tr>
          );
        })}
      </tbody>
    </table>
  );
}
