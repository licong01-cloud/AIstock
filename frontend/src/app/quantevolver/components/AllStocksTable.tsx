"use client";

import React, { useMemo, useState } from "react";
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

type SortKey = "code" | "profit" | "profit_pct" | "avg_cost" | "last_price" | "holding_days" | "first_date" | "last_date";
type SortDir = "asc" | "desc";

interface AllStocksTableProps {
  stocks: StockEntry[];
  stockTrades?: Record<string, Trade[]>;
}

const PAGE_SIZE = 50;

export function AllStocksTable({ stocks, stockTrades }: AllStocksTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>("first_date");
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [expandedStock, setExpandedStock] = useState<string | null>(null);
  const [page, setPage] = useState(0);

  const sorted = useMemo(() => {
    const arr = [...stocks];
    arr.sort((a, b) => {
      let va: any = a[sortKey];
      let vb: any = b[sortKey];
      if (va == null) va = sortDir === "asc" ? Infinity : -Infinity;
      if (vb == null) vb = sortDir === "asc" ? Infinity : -Infinity;
      if (typeof va === "string") {
        const cmp = va.localeCompare(vb);
        return sortDir === "asc" ? cmp : -cmp;
      }
      return sortDir === "asc" ? va - vb : vb - va;
    });
    return arr;
  }, [stocks, sortKey, sortDir]);

  const totalPages = Math.ceil(sorted.length / PAGE_SIZE);
  const paged = sorted.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  if (!stocks || stocks.length === 0) return null;

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(d => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir(key === "first_date" || key === "code" ? "asc" : "desc");
    }
    setPage(0);
  };

  const arrow = (key: SortKey) => sortKey === key ? (sortDir === "asc" ? " \u2191" : " \u2193") : "";

  const thStyle = (key: SortKey, align: "left" | "right" = "left"): React.CSSProperties => ({
    padding: "8px 6px",
    color: sortKey === key ? "#1e40af" : "#64748b",
    fontWeight: 600,
    cursor: "pointer",
    textAlign: align,
    userSelect: "none",
    whiteSpace: "nowrap",
  });

  // 统计摘要
  const totalProfit = stocks.reduce((s, x) => s + x.profit, 0);
  const nWin = stocks.filter(s => s.profit > 0).length;
  const nLoss = stocks.filter(s => s.profit < 0).length;

  return (
    <div style={{ backgroundColor: "#fff", borderRadius: 8, border: "1px solid #e2e8f0", padding: 20 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <h3 style={{ margin: 0, fontSize: 13, fontWeight: 700, color: "#334155", textTransform: "uppercase", letterSpacing: "0.05em" }}>
          全部持仓股票 ({stocks.length})
        </h3>
        <div style={{ fontSize: 11, color: "#64748b" }}>
          <span style={{ color: "#e53935" }}>盈利 {nWin}</span>
          {" / "}
          <span style={{ color: "#22a35a" }}>亏损 {nLoss}</span>
          {" / "}
          <span>累计 {totalProfit >= 0 ? "+" : ""}{(totalProfit / 1e4).toFixed(1)}万</span>
        </div>
      </div>

      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, fontFamily: "monospace" }}>
          <thead>
            <tr style={{ borderBottom: "2px solid #e2e8f0", textAlign: "left" }}>
              <th style={{ padding: "8px 6px", color: "#64748b", fontWeight: 600, width: 32 }}>#</th>
              <th style={thStyle("code")} onClick={() => handleSort("code")}>股票代码{arrow("code")}</th>
              <th style={thStyle("profit", "right")} onClick={() => handleSort("profit")}>累计盈亏{arrow("profit")}</th>
              <th style={thStyle("profit_pct", "right")} onClick={() => handleSort("profit_pct")}>收益率{arrow("profit_pct")}</th>
              <th style={thStyle("avg_cost", "right")} onClick={() => handleSort("avg_cost")}>均价成本{arrow("avg_cost")}</th>
              <th style={thStyle("last_price", "right")} onClick={() => handleSort("last_price")}>最新价{arrow("last_price")}</th>
              <th style={thStyle("holding_days", "right")} onClick={() => handleSort("holding_days")}>持仓天数{arrow("holding_days")}</th>
              <th style={thStyle("first_date")} onClick={() => handleSort("first_date")}>首次持仓{arrow("first_date")}</th>
              <th style={thStyle("last_date")} onClick={() => handleSort("last_date")}>最后持仓{arrow("last_date")}</th>
            </tr>
          </thead>
          <tbody>
            {paged.map((s, i) => {
              const profitColor = s.profit >= 0 ? "#e53935" : "#22a35a";
              const globalIdx = page * PAGE_SIZE + i + 1;
              return (
                <React.Fragment key={s.code}>
                  <tr
                    style={{
                      borderBottom: "1px solid #f1f5f9",
                      cursor: "pointer",
                      backgroundColor: expandedStock === s.code ? "#f8fafc" : undefined,
                    }}
                    onClick={() => setExpandedStock(expandedStock === s.code ? null : s.code)}
                  >
                    <td style={{ padding: "6px", color: "#94a3b8" }}>{globalIdx}</td>
                    <td style={{ padding: "6px", fontWeight: 600, color: "#0f172a" }}>
                      {s.code} <span style={{ color: "#94a3b8", fontSize: 10 }}>{expandedStock === s.code ? "\u25B2" : "\u25BC"}</span>
                    </td>
                    <td style={{ padding: "6px", textAlign: "right", color: profitColor, fontWeight: 600 }}>
                      {s.profit >= 0 ? "+" : ""}
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
              );
            })}
          </tbody>
        </table>
      </div>

      {/* 分页 */}
      {totalPages > 1 && (
        <div style={{ display: "flex", justifyContent: "center", alignItems: "center", gap: 8, marginTop: 12 }}>
          <button
            disabled={page === 0}
            onClick={() => setPage(p => p - 1)}
            style={{ padding: "4px 10px", borderRadius: 4, border: "1px solid #e2e8f0", backgroundColor: page === 0 ? "#f1f5f9" : "#fff", cursor: page === 0 ? "default" : "pointer", fontSize: 12, color: "#475569" }}
          >
            上一页
          </button>
          <span style={{ fontSize: 12, color: "#64748b" }}>
            {page + 1} / {totalPages}
          </span>
          <button
            disabled={page >= totalPages - 1}
            onClick={() => setPage(p => p + 1)}
            style={{ padding: "4px 10px", borderRadius: 4, border: "1px solid #e2e8f0", backgroundColor: page >= totalPages - 1 ? "#f1f5f9" : "#fff", cursor: page >= totalPages - 1 ? "default" : "pointer", fontSize: 12, color: "#475569" }}
          >
            下一页
          </button>
        </div>
      )}
    </div>
  );
}
