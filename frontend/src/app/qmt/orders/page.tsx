"use client";

import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface QmtOrder {
  order_id: string;
  order_sysid: string;
  stock_code: string;
  stock_name: string;
  order_time: string;
  order_type: number;
  order_type_name: string;
  order_volume: number;
  price_type: number;
  price: number;
  traded_volume: number;
  traded_price: number;
  order_status: number;
  status_msg: string;
  strategy_name: string;
  order_remark: string;
  secu_account: string;
}

interface QmtTrade {
  trade_id?: string;
  stock_code: string;
  stock_name: string;
  trade_time: string;
  trade_volume: number;
  trade_price: number;
  order_id?: string;
  order_sysid?: string;
  strategy_name?: string;
}

export default function QmtOrdersPage() {
  const [orders, setOrders] = useState<QmtOrder[]>([]);
  const [trades, setTrades] = useState<QmtTrade[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<"orders" | "trades">("orders");

  const [refreshIntervalSec, setRefreshIntervalSec] = useState<number>(10);

  async function loadData() {
    setLoading(true);
    setError(null);
    try {
      if (activeTab === "orders") {
        const res = await fetch(`${API_BASE}/qmt/orders`);
        if (!res.ok) throw new Error(`委托请求失败: ${res.status}`);
        const data: QmtOrder[] = await res.json();
        setOrders(data || []);
      } else {
        const res = await fetch(`${API_BASE}/qmt/trades`);
        if (!res.ok) throw new Error(`成交请求失败: ${res.status}`);
        const data: QmtTrade[] = await res.json();
        setTrades(data || []);
      }
    } catch (e: any) {
      setError(e?.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadData();
  }, [activeTab]);

  // 自动刷新，仅在当前标签页激活时生效
  useEffect(() => {
    if (!refreshIntervalSec || refreshIntervalSec <= 0) return;

    const timer = setInterval(() => {
      loadData();
    }, refreshIntervalSec * 1000);

    return () => clearInterval(timer);
  }, [activeTab, refreshIntervalSec]);

  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "linear-gradient(135deg, #0f172a 0%, #1d4ed8 60%, #22c55e 100%)",
          borderRadius: 16,
          padding: 20,
          color: "#fff",
          marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>QMT 当日委托 / 成交</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          查看当前 QMT 账户的当日委托与成交情况，便于核对策略执行结果
        </p>
      </section>

      <div
        style={{
          display: "flex",
          gap: 8,
          marginBottom: 16,
        }}
      >
        <button
          type="button"
          onClick={() => setActiveTab("orders")}
          style={{
            padding: "8px 16px",
            borderRadius: 999,
            border: "none",
            cursor: "pointer",
            fontWeight: 600,
            backgroundColor:
              activeTab === "orders" ? "#4f46e5" : "rgba(15,23,42,0.06)",
            color: activeTab === "orders" ? "#fff" : "#111827",
          }}
        >
          当日委托
        </button>
        <button
          type="button"
          onClick={() => setActiveTab("trades")}
          style={{
            padding: "8px 16px",
            borderRadius: 999,
            border: "none",
            cursor: "pointer",
            fontWeight: 600,
            backgroundColor:
              activeTab === "trades" ? "#4f46e5" : "rgba(15,23,42,0.06)",
            color: activeTab === "trades" ? "#fff" : "#111827",
          }}
        >
          当日成交
        </button>
      </div>

      {error && (
        <div
          style={{
            padding: 12,
            background: "#fee",
            border: "1px solid #fcc",
            borderRadius: 8,
            marginBottom: 16,
            color: "#c00",
          }}
        >
          {error}
        </div>
      )}

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 20,
          boxShadow: "0 1px 3px rgba(0,0,0,0.1)",
        }}
      >
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: 16,
          }}
        >
          <h2 style={{ margin: 0 }}>
            {activeTab === "orders" ? "当日委托" : "当日成交"}
          </h2>
          <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13 }}>
            <span>自动刷新间隔：</span>
            <select
              value={refreshIntervalSec}
              onChange={(e) => setRefreshIntervalSec(Number(e.target.value))}
              aria-label="QMT 委托成交自动刷新间隔"
              style={{ padding: "4px 8px", borderRadius: 4, border: "1px solid #ddd" }}
            >
              <option value={5}>5 秒</option>
              <option value={10}>10 秒</option>
              <option value={30}>30 秒</option>
              <option value={60}>1 分钟</option>
            </select>
            <button
              type="button"
              onClick={loadData}
              style={{
                padding: "4px 10px",
                borderRadius: 4,
                border: "1px solid #ddd",
                background: "#fff",
                cursor: "pointer",
              }}
            >
              手动刷新
            </button>
          </div>
        </div>

        {loading ? (
          <div>加载中...</div>
        ) : activeTab === "orders" ? (
          orders.length === 0 ? (
            <div style={{ padding: 40, textAlign: "center", color: "#666" }}>
              暂无当日委托
            </div>
          ) : (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ background: "#f5f5f5" }}>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>时间</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>股票</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>方向</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>数量</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>价格</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>成交数量/价</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>状态</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>订单ID</th>
                    <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>备注</th>
                  </tr>
                </thead>
                <tbody>
                  {orders.map((o) => (
                    <tr key={o.order_id} style={{ borderBottom: "1px solid #eee" }}>
                      <td style={{ padding: 12 }}>{o.order_time}</td>
                      <td style={{ padding: 12 }}>
                        {o.stock_code} {o.stock_name}
                      </td>
                      <td style={{ padding: 12 }}>{o.order_type_name}</td>
                      <td style={{ padding: 12 }}>{o.order_volume}</td>
                      <td style={{ padding: 12 }}>{o.price?.toFixed(3) ?? "-"}</td>
                      <td style={{ padding: 12 }}>
                        {o.traded_volume > 0
                          ? `${o.traded_volume} / ${o.traded_price?.toFixed(3) ?? "-"}`
                          : "-"}
                      </td>
                      <td style={{ padding: 12 }}>{o.status_msg}</td>
                      <td style={{ padding: 12 }}>{o.order_id}</td>
                      <td style={{ padding: 12 }}>{o.order_remark || "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )
        ) : trades.length === 0 ? (
          <div style={{ padding: 40, textAlign: "center", color: "#666" }}>
            暂无当日成交
          </div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ background: "#f5f5f5" }}>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>时间</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>股票</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>成交数量</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>成交价格</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>订单ID</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #ddd" }}>策略</th>
                </tr>
              </thead>
              <tbody>
                {trades.map((t, idx) => (
                  <tr key={t.trade_id || `${t.stock_code}-${t.trade_time}-${idx}`} style={{ borderBottom: "1px solid #eee" }}>
                    <td style={{ padding: 12 }}>{t.trade_time}</td>
                    <td style={{ padding: 12 }}>
                      {t.stock_code} {t.stock_name}
                    </td>
                    <td style={{ padding: 12 }}>{t.trade_volume}</td>
                    <td style={{ padding: 12 }}>{t.trade_price?.toFixed(3) ?? "-"}</td>
                    <td style={{ padding: 12 }}>{t.order_id || "-"}</td>
                    <td style={{ padding: 12 }}>{t.strategy_name || "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </main>
  );
}
