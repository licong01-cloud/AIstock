"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import dynamic from "next/dynamic";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false }) as any;

const API = "http://127.0.0.1:8001/api/v1";

interface Portfolio {
  id: number;
  portfolio_name: string;
  status: string;
  signal_source: string;
  signal_source_id: string;
}

interface Snapshot {
  trade_date: string;
  total_value: number;
  cash: number;
  stock_value: number;
  daily_return: number;
  cumulative_return: number;
  max_drawdown: number;
  position_count: number;
  buy_count: number;
  sell_count: number;
}

interface Position {
  symbol: string;
  symbol_name: string;
  quantity: number;
  avg_cost: number;
  close_price: number;
  market_value: number;
  weight: number;
  unrealized_pnl: number;
  unrealized_pnl_pct: number;
  holding_days: number;
}

interface Trade {
  trade_date: string;
  symbol: string;
  symbol_name: string;
  side: string;
  quantity: number;
  price: number;
  amount: number;
  commission: number;
  stamp_tax: number;
  transfer_fee: number;
  slippage_cost: number;
  total_cost: number;
  realized_pnl: number | null;
}

interface Signal {
  symbol: string;
  side: string;
  target_quantity: number;
  score: number;
  trade_date: string;
  status: string;
}

/** 安全数字格式化 — PostgreSQL numeric 序列化为 JSON string 时不会崩溃 */
const fmt = (v: unknown, digits: number): string =>
  v != null && v !== "" ? Number(v).toFixed(digits) : "";

export default function MonitorPage() {
  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [showAllStatus, setShowAllStatus] = useState(false);
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [positions, setPositions] = useState<Position[]>([]);
  const [trades, setTrades] = useState<Trade[]>([]);
  const [tradeOffset, setTradeOffset] = useState(0);
  const [hasMoreTrades, setHasMoreTrades] = useState(false);
  const [pendingSignals, setPendingSignals] = useState<Signal[]>([]);
  const [allSignals, setAllSignals] = useState<Signal[]>([]);
  const [tab, setTab] = useState<"overview" | "positions" | "trades" | "signals" | "stock-pnl">("overview");
  const [stockPnl, setStockPnl] = useState<any[]>([]);
  const [expandedStock, setExpandedStock] = useState<string | null>(null);
  const [stockTradeHistory, setStockTradeHistory] = useState<any[]>([]);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const autoRefreshRef = useRef<NodeJS.Timeout | null>(null);

  // 历史持仓日期
  const [posDate, setPosDate] = useState("");
  const [histPositions, setHistPositions] = useState<Position[] | null>(null);
  const [posSort, setPosSort] = useState<{ key: string; dir: "asc" | "desc" }>({ key: "pnl", dir: "desc" });

  useEffect(() => {
    fetch(`${API}/paper-trading/portfolios`)
      .then((r) => r.json())
      .then((d) => {
        setPortfolios(d);
        const running = d.filter((p: Portfolio) => p.status === "running");
        if (running.length > 0 && !selectedId) setSelectedId(running[0].id);
        else if (d.length > 0 && !selectedId) setSelectedId(d[0].id);
      })
      .catch(() => {});
  }, []);

  const filteredPortfolios = showAllStatus
    ? portfolios
    : portfolios.filter((p) => ["running", "catching_up", "caught_up"].includes(p.status));

  const loadData = useCallback(async () => {
    if (!selectedId) return;
    try {
      const LIMIT = 50;
      const [snap, pos, tr, sig, pending, pnl] = await Promise.all([
        fetch(`${API}/paper-trading/portfolios/${selectedId}/snapshots`).then((r) => r.json()),
        fetch(`${API}/paper-trading/portfolios/${selectedId}/positions`).then((r) => r.json()),
        fetch(`${API}/paper-trading/portfolios/${selectedId}/trades?limit=${LIMIT}&offset=0`).then((r) => r.json()),
        fetch(`${API}/paper-trading/portfolios/${selectedId}/signals?limit=200`).then((r) => r.json()),
        fetch(`${API}/paper-trading/portfolios/${selectedId}/signals/pending`).then((r) => r.json()),
        fetch(`${API}/paper-trading/portfolios/${selectedId}/stock-pnl`).then((r) => r.json()),
      ]);
      setSnapshots(snap);
      setPositions(pos);
      setTrades(tr);
      setTradeOffset(0);
      setHasMoreTrades(tr.length >= LIMIT);
      setAllSignals(sig);
      setPendingSignals(pending);
      setStockPnl(pnl);
      setHistPositions(null);
      setPosDate("");
    } catch (e) {
      console.error(e);
    }
  }, [selectedId]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  // 自动刷新
  useEffect(() => {
    if (autoRefresh) {
      autoRefreshRef.current = setInterval(loadData, 30000);
    }
    return () => {
      if (autoRefreshRef.current) clearInterval(autoRefreshRef.current);
    };
  }, [autoRefresh, loadData]);

  const loadMoreTrades = async () => {
    if (!selectedId) return;
    const newOffset = tradeOffset + 50;
    const resp = await fetch(`${API}/paper-trading/portfolios/${selectedId}/trades?limit=50&offset=${newOffset}`);
    const more = await resp.json();
    setTrades((prev) => [...prev, ...more]);
    setTradeOffset(newOffset);
    setHasMoreTrades(more.length >= 50);
  };

  const loadHistPositions = async () => {
    if (!selectedId || !posDate) return;
    const resp = await fetch(`${API}/paper-trading/portfolios/${selectedId}/positions/${posDate}`);
    const data = await resp.json();
    setHistPositions(data);
  };

  useEffect(() => {
    if (posDate) loadHistPositions();
  }, [posDate, selectedId]);

  const loadStockDetail = async (symbol: string) => {
    if (expandedStock === symbol) {
      setExpandedStock(null);
      return;
    }
    if (!selectedId) return;
    const resp = await fetch(`${API}/paper-trading/portfolios/${selectedId}/stock-pnl/${symbol}`);
    const data = await resp.json();
    setStockTradeHistory(data);
    setExpandedStock(symbol);
  };

  const runOnce = async () => {
    if (!selectedId) return;
    if (!confirm("确认手动触发一次策略执行？将生成新的交易信号。")) return;
    try {
      const resp = await fetch(`${API}/paper-trading/portfolios/${selectedId}/run-once`, { method: "POST" });
      if (!resp.ok) throw new Error(await resp.text());
      const data = await resp.json();
      alert(`执行完成: 生成信号 ${data.signals_generated} 个`);
      loadData();
    } catch (e: any) {
      alert("执行失败: " + (e.message || e));
    }
  };

  const latest = snapshots.length > 0 ? snapshots[snapshots.length - 1] : null;
  const displayPositions = histPositions !== null ? histPositions : positions;
  const selectedPortfolio = portfolios.find(p => p.id === selectedId);

  const handlePosSort = (key: string) => {
    setPosSort(prev => ({ key, dir: prev.key === key && prev.dir === "desc" ? "asc" : "desc" }));
  };

  const sortedPositions = [...displayPositions].sort((a, b) => {
    const { key, dir } = posSort;
    let aVal: any = a[key as keyof typeof a];
    let bVal: any = b[key as keyof typeof b];
    if (aVal == null) aVal = 0;
    if (bVal == null) bVal = 0;
    return dir === "desc" ? (bVal > aVal ? 1 : -1) : (aVal > bVal ? 1 : -1);
  });

  // 追赶中时定期刷新 portfolios 列表，确保状态变更后 UI 及时更新
  useEffect(() => {
    if (selectedPortfolio?.status !== "catching_up") return;
    const t = setInterval(() => {
      fetch(`${API}/paper-trading/portfolios`)
        .then(r => r.json())
        .then(d => setPortfolios(d))
        .catch(() => {});
    }, 6000);
    return () => clearInterval(t);
  }, [selectedPortfolio?.status]);

  return (
    <div>
      {/* 组合选择 */}
      <div style={{ display: "flex", gap: 12, marginBottom: 16, alignItems: "center", flexWrap: "wrap" }}>
        <select
          value={selectedId || ""}
          onChange={(e) => setSelectedId(Number(e.target.value))}
          style={{ padding: "6px 12px", border: "1px solid #d1d5db", borderRadius: 6, fontSize: 13 }}
        >
          {filteredPortfolios.map((p) => (
            <option key={p.id} value={p.id}>{p.portfolio_name} (#{p.id}) [{p.status}]</option>
          ))}
        </select>
        <label style={{ fontSize: 12, display: "flex", alignItems: "center", gap: 4 }}>
          <input type="checkbox" checked={showAllStatus} onChange={(e) => setShowAllStatus(e.target.checked)} />
          显示全部状态
        </label>
        <button onClick={loadData} style={btnSmStyle}>刷新</button>
        <button onClick={runOnce} style={btnSmStyle}>手动触发</button>
        <label style={{ fontSize: 12, display: "flex", alignItems: "center", gap: 4, marginLeft: "auto" }}>
          <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} />
          自动刷新 (30s)
        </label>
      </div>

      {/* 追赶中: 显示进度替代指标卡片 */}
      {selectedPortfolio?.status === "catching_up" && (
        <CatchupProgressBar portfolioId={selectedId!} />
      )}

      {/* 摘要卡片 */}
      {latest && selectedPortfolio?.status !== "catching_up" && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(6, 1fr)", gap: 12, marginBottom: 16 }}>
          <MetricCard label="总资产" value={`¥${(Number(latest.total_value) / 10000).toFixed(2)}万`} />
          <MetricCard label="现金" value={`¥${(Number(latest.cash) / 10000).toFixed(2)}万`} />
          <MetricCard label="累计收益" value={`${(Number(latest.cumulative_return) * 100).toFixed(2)}%`}
            color={Number(latest.cumulative_return) >= 0 ? "#dc2626" : "#16a34a"} />
          <MetricCard label="当日收益" value={`${(Number(latest.daily_return) * 100).toFixed(2)}%`}
            color={Number(latest.daily_return) >= 0 ? "#dc2626" : "#16a34a"} />
          <MetricCard label="最大回撤" value={`${(Number(latest.max_drawdown) * 100).toFixed(2)}%`} color="#dc2626" />
          <MetricCard label="持仓数" value={String(latest.position_count || 0)} />
        </div>
      )}

      {/* Tab 导航 */}
      <div style={{ display: "flex", gap: 4, borderBottom: "1px solid #e5e7eb", marginBottom: 16 }}>
        {(["overview", "positions", "trades", "signals", "stock-pnl"] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            style={{
              padding: "8px 16px", fontSize: 13, border: "none", background: "transparent",
              borderBottom: tab === t ? "2px solid #2563eb" : "2px solid transparent",
              color: tab === t ? "#60a5fa" : "#ffffff", fontWeight: tab === t ? 600 : 400,
              cursor: "pointer", marginBottom: -1,
            }}
          >
            {{ overview: "净值曲线", positions: "当前持仓", trades: "交易记录", signals: "信号记录", "stock-pnl": "个股盈亏" }[t]}
          </button>
        ))}
      </div>

      {/* 净值曲线 */}
      {tab === "overview" && snapshots.length > 0 && (
        <div style={cardStyle}>
          <Plot
            data={[
              {
                x: snapshots.map((s) => s.trade_date),
                y: snapshots.map((s) => Number(s.cumulative_return) * 100),
                type: "scatter", mode: "lines", name: "累计收益(%)",
                line: { color: "#2563eb", width: 2 },
              },
            ]}
            layout={{
              height: 350, margin: { t: 30, b: 40, l: 50, r: 20 },
              xaxis: { title: "" }, yaxis: { title: "累计收益(%)" },
              paper_bgcolor: "transparent", plot_bgcolor: "transparent",
            }}
            config={{ displayModeBar: false }}
            style={{ width: "100%" }}
          />
        </div>
      )}

      {/* 当前持仓 + 历史持仓 */}
      {tab === "positions" && (
        <div>
          {/* 历史持仓日期选择 */}
          <div style={{ display: "flex", gap: 8, marginBottom: 12, alignItems: "center" }}>
            <input type="date" value={posDate} onChange={(e) => setPosDate(e.target.value)}
              style={{ padding: "4px 10px", border: "1px solid #d1d5db", borderRadius: 6, fontSize: 13 }} />
            {histPositions !== null && (
              <button onClick={() => { setHistPositions(null); setPosDate(""); }} style={{ ...btnSmStyle, background: "#6b7280" }}>返回最新</button>
            )}
            {histPositions !== null && <span style={{ fontSize: 12, color: "#6b7280" }}>当前查看: {posDate}</span>}
          </div>
          <div style={{ ...cardStyle, padding: 0, overflow: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
              <thead>
                <tr style={{ background: "#f9fafb" }}>
                  <th style={thStyle}>代码</th>
                  <th style={thStyle}>名称</th>
                  <th style={{ ...rthStyle, cursor: "pointer" }} onClick={() => handlePosSort("quantity")}>
                    数量 {posSort.key === "quantity" && (posSort.dir === "desc" ? "↓" : "↑")}
                  </th>
                  <th style={{ ...rthStyle, cursor: "pointer" }} onClick={() => handlePosSort("avg_cost")}>
                    成本 {posSort.key === "avg_cost" && (posSort.dir === "desc" ? "↓" : "↑")}
                  </th>
                  <th style={rthStyle}>现价</th>
                  <th style={{ ...rthStyle, cursor: "pointer" }} onClick={() => handlePosSort("market_value")}>
                    市值 {posSort.key === "market_value" && (posSort.dir === "desc" ? "↓" : "↑")}
                  </th>
                  <th style={{ ...rthStyle, cursor: "pointer" }} onClick={() => handlePosSort("weight")}>
                    权重 {posSort.key === "weight" && (posSort.dir === "desc" ? "↓" : "↑")}
                  </th>
                  <th style={{ ...rthStyle, cursor: "pointer" }} onClick={() => handlePosSort("pnl")}>
                    浮动盈亏 {posSort.key === "pnl" && (posSort.dir === "desc" ? "↓" : "↑")}
                  </th>
                  <th style={{ ...rthStyle, cursor: "pointer" }} onClick={() => handlePosSort("pnl_pct")}>
                    盈亏率 {posSort.key === "pnl_pct" && (posSort.dir === "desc" ? "↓" : "↑")}
                  </th>
                  <th style={{ ...rthStyle, cursor: "pointer" }} onClick={() => handlePosSort("hold_days")}>
                    持有天数 {posSort.key === "hold_days" && (posSort.dir === "desc" ? "↓" : "↑")}
                  </th>
                </tr>
              </thead>
              <tbody>
                {sortedPositions.map((p) => (
                  <tr key={p.symbol}>
                    <td style={tdStyle}><code>{p.symbol}</code></td>
                    <td style={tdStyle}>{p.symbol_name || "-"}</td>
                    <td style={rtdStyle}>{p.quantity}</td>
                    <td style={rtdStyle}>{fmt(p.avg_cost, 2)}</td>
                    <td style={rtdStyle}>{fmt(p.close_price, 2)}</td>
                    <td style={rtdStyle}>{(Number(p.market_value) / 10000).toFixed(2)}万</td>
                    <td style={rtdStyle}>{p.weight ? (Number(p.weight) * 100).toFixed(1) + "%" : "-"}</td>
                    <td style={{ ...rtdStyle, color: Number(p.unrealized_pnl ?? 0) >= 0 ? "#dc2626" : "#16a34a" }}>
                      {fmt(p.unrealized_pnl, 0)}
                    </td>
                    <td style={{ ...rtdStyle, color: Number(p.unrealized_pnl_pct ?? 0) >= 0 ? "#dc2626" : "#16a34a" }}>
                      {p.unrealized_pnl_pct ? (Number(p.unrealized_pnl_pct) * 100).toFixed(2) + "%" : "-"}
                    </td>
                    <td style={rtdStyle}>{p.holding_days ?? "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {displayPositions.length === 0 && <div style={{ textAlign: "center", padding: 30, color: "#9ca3af" }}>暂无持仓</div>}
          </div>
        </div>
      )}

      {/* 交易记录 + 分页 */}
      {tab === "trades" && (
        <div style={{ ...cardStyle, padding: 0, overflow: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ background: "#f9fafb" }}>
                <th style={thStyle}>日期</th>
                <th style={thStyle}>代码</th>
                <th style={thStyle}>名称</th>
                <th style={thStyle}>方向</th>
                <th style={rthStyle}>数量</th>
                <th style={rthStyle}>价格</th>
                <th style={rthStyle}>金额</th>
                <th style={rthStyle}>佣金</th>
                <th style={rthStyle}>印花税</th>
                <th style={rthStyle}>过户费</th>
                <th style={rthStyle}>滑点</th>
                <th style={rthStyle}>总费用</th>
                <th style={rthStyle}>实现盈亏</th>
              </tr>
            </thead>
            <tbody>
              {trades.map((t, i) => (
                <tr key={i} style={{ background: i % 2 === 0 ? "#fff" : "#f9fafb" }}>
                  <td style={tdStyle}>{t.trade_date}</td>
                  <td style={tdStyle}><code>{t.symbol}</code></td>
                  <td style={tdStyle}>{t.symbol_name || "-"}</td>
                  <td style={{ ...tdStyle, color: t.side === "BUY" ? "#dc2626" : "#16a34a", fontWeight: 600 }}>
                    {t.side === "BUY" ? "买入" : "卖出"}
                  </td>
                  <td style={rtdStyle}>{t.quantity}</td>
                  <td style={rtdStyle}>{fmt(t.price, 2)}</td>
                  <td style={rtdStyle}>{(Number(t.amount) / 10000).toFixed(2)}万</td>
                  <td style={rtdStyle}>{fmt(t.commission, 2)}</td>
                  <td style={rtdStyle}>{fmt(t.stamp_tax, 2)}</td>
                  <td style={rtdStyle}>{fmt(t.transfer_fee, 2)}</td>
                  <td style={rtdStyle}>{fmt(t.slippage_cost, 2)}</td>
                  <td style={rtdStyle}>{fmt(t.total_cost, 2)}</td>
                  <td style={{ ...rtdStyle, color: Number(t.realized_pnl ?? 0) >= 0 ? "#dc2626" : "#16a34a", fontWeight: 500 }}>
                    {t.realized_pnl != null ? Number(t.realized_pnl).toFixed(0) : "-"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {trades.length === 0 && <div style={{ textAlign: "center", padding: 30, color: "#9ca3af" }}>暂无交易记录</div>}
          {hasMoreTrades && (
            <div style={{ textAlign: "center", padding: 12 }}>
              <button onClick={loadMoreTrades} style={btnSmStyle}>加载更多</button>
            </div>
          )}
        </div>
      )}

      {/* 信号记录 (含全量+待执行) */}
      {tab === "signals" && (
        <div>
          <h4 style={{ fontSize: 14, fontWeight: 600, margin: "0 0 8px" }}>待执行信号 ({pendingSignals.length})</h4>
          <div style={{ ...cardStyle, padding: 0, overflow: "auto", marginBottom: 16 }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
              <thead>
                <tr style={{ background: "#f9fafb" }}>
                  <th style={thStyle}>交易日</th>
                  <th style={thStyle}>代码</th>
                  <th style={thStyle}>方向</th>
                  <th style={rthStyle}>目标数量</th>
                  <th style={rthStyle}>评分</th>
                  <th style={thStyle}>状态</th>
                </tr>
              </thead>
              <tbody>
                {pendingSignals.map((s, i) => (
                  <tr key={i}>
                    <td style={tdStyle}>{s.trade_date}</td>
                    <td style={tdStyle}><code>{s.symbol}</code></td>
                    <td style={{ ...tdStyle, color: s.side === "BUY" ? "#dc2626" : "#16a34a", fontWeight: 600 }}>
                      {s.side === "BUY" ? "买入" : "卖出"}
                    </td>
                    <td style={rtdStyle}>{s.target_quantity}</td>
                    <td style={rtdStyle}>{s.score != null ? Number(s.score).toFixed(4) : ""}</td>
                    <td style={tdStyle}><span style={{ fontSize: 11, padding: "1px 6px", borderRadius: 3, background: "#dbeafe" }}>{s.status}</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
            {pendingSignals.length === 0 && <div style={{ textAlign: "center", padding: 20, color: "#9ca3af" }}>无待执行信号</div>}
          </div>

          <h4 style={{ fontSize: 14, fontWeight: 600, margin: "0 0 8px" }}>历史信号 (最近 200 条)</h4>
          <div style={{ ...cardStyle, padding: 0, overflow: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
              <thead>
                <tr style={{ background: "#f9fafb" }}>
                  <th style={thStyle}>交易日</th>
                  <th style={thStyle}>代码</th>
                  <th style={thStyle}>方向</th>
                  <th style={rthStyle}>目标数量</th>
                  <th style={rthStyle}>评分</th>
                  <th style={thStyle}>状态</th>
                </tr>
              </thead>
              <tbody>
                {allSignals.map((s, i) => (
                  <tr key={i} style={{ background: i % 2 === 0 ? "#fff" : "#f9fafb" }}>
                    <td style={tdStyle}>{s.trade_date}</td>
                    <td style={tdStyle}><code>{s.symbol}</code></td>
                    <td style={{ ...tdStyle, color: s.side === "BUY" ? "#dc2626" : "#16a34a", fontWeight: 600 }}>
                      {s.side === "BUY" ? "买入" : "卖出"}
                    </td>
                    <td style={rtdStyle}>{s.target_quantity}</td>
                    <td style={rtdStyle}>{s.score != null ? Number(s.score).toFixed(4) : ""}</td>
                    <td style={tdStyle}>
                      <span style={{
                        fontSize: 11, padding: "1px 6px", borderRadius: 3,
                        background: s.status === "executed" ? "#dcfce7" : s.status === "pending" ? "#dbeafe" : s.status === "skipped" ? "#fef3c7" : "#f3f4f6",
                      }}>{s.status}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {allSignals.length === 0 && <div style={{ textAlign: "center", padding: 20, color: "#9ca3af" }}>暂无信号记录</div>}
          </div>
        </div>
      )}

      {/* 个股盈亏 + 展开明细 */}
      {tab === "stock-pnl" && (
        <div style={{ ...cardStyle, padding: 0, overflow: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
            <thead>
              <tr style={{ background: "#f9fafb" }}>
                <th style={thStyle}>代码</th>
                <th style={thStyle}>名称</th>
                <th style={rthStyle}>买入次数</th>
                <th style={rthStyle}>卖出次数</th>
                <th style={rthStyle}>已实现盈亏</th>
                <th style={rthStyle}>浮动盈亏</th>
                <th style={rthStyle}>总盈亏</th>
                <th style={rthStyle}>胜率</th>
                <th style={rthStyle}>平均持有天数</th>
                <th style={thStyle}>持有中</th>
                <th style={thStyle}>明细</th>
              </tr>
            </thead>
            <tbody>
              {stockPnl.map((s: any) => (
                <>
                  <tr key={s.symbol}>
                    <td style={tdStyle}><code>{s.symbol}</code></td>
                    <td style={tdStyle}>{s.symbol_name || "-"}</td>
                    <td style={rtdStyle}>{s.buy_count}</td>
                    <td style={rtdStyle}>{s.sell_count}</td>
                    <td style={{ ...rtdStyle, color: Number(s.total_realized_pnl ?? 0) >= 0 ? "#dc2626" : "#16a34a" }}>
                      {fmt(s.total_realized_pnl, 0)}
                    </td>
                    <td style={{ ...rtdStyle, color: Number(s.total_unrealized_pnl ?? 0) >= 0 ? "#dc2626" : "#16a34a" }}>
                      {fmt(s.total_unrealized_pnl, 0)}
                    </td>
                    <td style={{ ...rtdStyle, fontWeight: 600, color: Number(s.total_pnl ?? 0) >= 0 ? "#dc2626" : "#16a34a" }}>
                      {fmt(s.total_pnl, 0)}
                    </td>
                    <td style={rtdStyle}>
                      {Number(s.win_count) + Number(s.loss_count) > 0 ? ((Number(s.win_count) / (Number(s.win_count) + Number(s.loss_count))) * 100).toFixed(0) + "%" : "-"}
                    </td>
                    <td style={rtdStyle}>{s.avg_holding_days != null ? Number(s.avg_holding_days).toFixed(1) : "-"}</td>
                    <td style={tdStyle}>{s.is_holding ? "持有" : ""}</td>
                    <td style={tdStyle}>
                      <button onClick={() => loadStockDetail(s.symbol)} style={{ fontSize: 11, color: "#2563eb", background: "none", border: "none", cursor: "pointer", textDecoration: "underline" }}>
                        {expandedStock === s.symbol ? "收起" : "展开"}
                      </button>
                    </td>
                  </tr>
                  {expandedStock === s.symbol && stockTradeHistory.length > 0 && (
                    <tr key={`${s.symbol}-detail`}>
                      <td colSpan={11} style={{ padding: 0, background: "#f9fafb" }}>
                        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, margin: "8px 16px", maxWidth: "calc(100% - 32px)" }}>
                          <thead>
                            <tr>
                              <th style={{ ...thStyle, fontSize: 11 }}>日期</th>
                              <th style={{ ...thStyle, fontSize: 11 }}>方向</th>
                              <th style={{ ...rthStyle, fontSize: 11 }}>数量</th>
                              <th style={{ ...rthStyle, fontSize: 11 }}>价格</th>
                              <th style={{ ...rthStyle, fontSize: 11 }}>金额</th>
                              <th style={{ ...rthStyle, fontSize: 11 }}>总费用</th>
                              <th style={{ ...rthStyle, fontSize: 11 }}>实现盈亏</th>
                            </tr>
                          </thead>
                          <tbody>
                            {stockTradeHistory.map((t: any, idx: number) => (
                              <tr key={idx}>
                                <td style={{ ...tdStyle, fontSize: 11 }}>{t.trade_date}</td>
                                <td style={{ ...tdStyle, fontSize: 11, color: t.side === "BUY" ? "#dc2626" : "#16a34a", fontWeight: 600 }}>
                                  {t.side === "BUY" ? "买入" : "卖出"}
                                </td>
                                <td style={{ ...rtdStyle, fontSize: 11 }}>{t.quantity}</td>
                                <td style={{ ...rtdStyle, fontSize: 11 }}>{fmt(t.price, 2)}</td>
                                <td style={{ ...rtdStyle, fontSize: 11 }}>{fmt(t.amount, 0)}</td>
                                <td style={{ ...rtdStyle, fontSize: 11 }}>{fmt(t.total_cost, 2)}</td>
                                <td style={{ ...rtdStyle, fontSize: 11, color: Number(t.realized_pnl ?? 0) >= 0 ? "#dc2626" : "#16a34a" }}>
                                  {t.realized_pnl != null ? Number(t.realized_pnl).toFixed(0) : "-"}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </td>
                    </tr>
                  )}
                </>
              ))}
            </tbody>
          </table>
          {stockPnl.length === 0 && <div style={{ textAlign: "center", padding: 30, color: "#9ca3af" }}>暂无个股盈亏数据</div>}
        </div>
      )}

      {portfolios.length === 0 && (
        <div style={{ textAlign: "center", color: "#9ca3af", padding: 40 }}>无模拟盘，请先在配置页创建</div>
      )}
    </div>
  );
}

function CatchupProgressBar({ portfolioId }: { portfolioId: number }) {
  const [prog, setProg] = useState<any>(null);
  useEffect(() => {
    const load = () => fetch(`${API}/paper-trading/portfolios/${portfolioId}/catchup-progress`)
      .then(r => r.json()).then(setProg).catch(() => {});
    load();
    const t = setInterval(load, 5000);
    return () => clearInterval(t);
  }, [portfolioId]);

  if (!prog || prog.status !== "catching_up") return null;
  const hasError = !!prog.error;
  return (
    <div style={{ background: hasError ? "#fef2f2" : "#ecfdf5", borderRadius: 10, padding: 20, marginBottom: 16 }}>
      <div style={{ fontSize: 15, fontWeight: 600, color: hasError ? "#dc2626" : "#059669", marginBottom: 12 }}>
        {hasError ? "历史追赶失败" : "历史追赶进行中"}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16, marginBottom: 12 }}>
        <MetricCard label="进度" value={`${prog.completed_days}/${prog.total_days} 天`} />
        <MetricCard label="当前回放日期" value={prog.current_date || "-"} />
        <MetricCard label="完成百分比" value={`${fmt(prog.pct, 1)}%`} />
      </div>
      <div style={{ width: "100%", height: 10, background: hasError ? "#fecaca" : "#d1fae5", borderRadius: 5 }}>
        <div style={{
          width: `${prog.pct ?? 0}%`, height: "100%",
          background: hasError ? "#ef4444" : "linear-gradient(90deg, #059669, #10b981)",
          borderRadius: 5, transition: "width 0.5s ease",
        }} />
      </div>
      {hasError && (
        <div style={{ marginTop: 8, padding: 8, background: "#fee2e2", borderRadius: 6, color: "#991b1b", fontSize: 12 }}>
          {prog.error_time && <div style={{ fontSize: 10, color: "#7f1d1d", marginBottom: 4 }}>失败时间: {prog.error_time}</div>}
          {prog.error}
        </div>
      )}
    </div>
  );
}

function MetricCard({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div style={{
      background: "#fff", borderRadius: 12, padding: "12px 16px",
      boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
    }}>
      <div style={{ fontSize: 11, color: "#9ca3af", marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 18, fontWeight: 700, color: color || "#111827" }}>{value}</div>
    </div>
  );
}

const cardStyle: React.CSSProperties = { background: "#fff", borderRadius: 12, padding: 20, marginBottom: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" };
const btnSmStyle: React.CSSProperties = { padding: "6px 14px", background: "#2563eb", color: "#fff", border: "none", borderRadius: 6, fontSize: 13, cursor: "pointer" };
const thStyle: React.CSSProperties = { padding: "8px 12px", textAlign: "left", fontSize: 12, fontWeight: 600, borderBottom: "1px solid #e5e7eb" };
const rthStyle: React.CSSProperties = { ...thStyle, textAlign: "right" as const };
const tdStyle: React.CSSProperties = { padding: "7px 12px", borderBottom: "1px solid #f3f4f6" };
const rtdStyle: React.CSSProperties = { ...tdStyle, textAlign: "right" as const };
