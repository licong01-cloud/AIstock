"use client";

import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface MonitorTask {
  id: number;
  task_name: string;
  stock_code: string;
  stock_name?: string;
  enabled: boolean;
  check_interval: number;
  auto_trade: boolean;
  position_size_pct: number;
  has_position: boolean;
  position_cost?: number;
  position_quantity?: number;
}

export default function SmartMonitorPage() {
  const [activeTab, setActiveTab] = useState<
    "realtime" | "tasks" | "positions" | "history" | "settings"
  >("realtime");

  return (
    <main style={{ padding: 24 }}>
      <section style={{ marginBottom: 16 }}>
        <h1 style={{ margin: 0, fontSize: 22 }}>🤖 AI盯盘 - 决策交易系统</h1>
        <p style={{ marginTop: 4, fontSize: 13, color: "#666" }}>
          本页是对旧版 Streamlit 智能盯盘模块的完整迁移入口，后端通过
          /api/v1/smart-monitor 提供服务。
        </p>
      </section>

      <section style={{ marginBottom: 12 }}>
        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: 6,
            borderBottom: "1px solid #e5e7eb",
            paddingBottom: 4,
            marginBottom: 8,
          }}
        >
          {[
            { key: "realtime", label: "📊 实时分析" },
            { key: "tasks", label: "🎯 监控任务" },
            { key: "positions", label: "📈 持仓管理" },
            { key: "history", label: "📜 历史记录" },
            { key: "settings", label: "⚙️ 系统设置" },
          ].map((tab) => (
            <button
              key={tab.key}
              type="button"
              onClick={() => setActiveTab(tab.key as any)}
              style={{
                padding: "6px 10px",
                borderRadius: 999,
                border: "none",
                background:
                  activeTab === tab.key ? "#0f766e" : "transparent",
                color: activeTab === tab.key ? "#fff" : "#374151",
                fontSize: 13,
                cursor: "pointer",
              }}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </section>

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 14,
          boxShadow: "0 2px 10px rgba(0,0,0,0.04)",
          fontSize: 13,
        }}
      >
        {activeTab === "realtime" && <RealtimeTab />}
        {activeTab === "tasks" && <TasksTab />}
        {activeTab === "positions" && <PositionsTab />}
        {activeTab === "history" && <HistoryTab />}
        {activeTab === "settings" && <SettingsTab />}
      </section>
    </main>
  );
}

function RealtimeTab() {
  const [stockCode, setStockCode] = useState("");
  const [autoTrade, setAutoTrade] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<any | null>(null);

  const handleAnalyze = async () => {
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const body = { stock_code: stockCode.trim(), auto_trade: autoTrade };
      const res = await fetch(
        `${API_BASE.replace(/\/$/, "")}/smart-monitor/analyze`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
      );
      const data = await res.json();
      if (!data?.success) {
        setError(data?.error || "分析失败");
      }
      setResult(data);
    } catch (e: any) {
      setError(e?.message || "分析请求失败");
    } finally {
      setLoading(false);
    }
  };

  const decision = result?.decision;
  const market = result?.market_data;

  return (
    <div>
      <h2 style={{ marginTop: 0, fontSize: 16 }}>📊 实时分析</h2>
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <input
          type="text"
          value={stockCode}
          onChange={(e) => setStockCode(e.target.value)}
          placeholder="例如: 600519"
          style={{
            flex: 1,
            padding: "6px 8px",
            borderRadius: 8,
            border: "1px solid #d4d4d4",
            fontSize: 13,
          }}
        />
        <label style={{ fontSize: 12, display: "flex", alignItems: "center", gap: 4 }}>
          <input
            type="checkbox"
            checked={autoTrade}
            onChange={(e) => setAutoTrade(e.target.checked)}
          />
          自动交易
        </label>
        <button
          type="button"
          onClick={handleAnalyze}
          disabled={loading || !stockCode.trim()}
          style={{
            padding: "6px 10px",
            borderRadius: 8,
            border: "none",
            background: loading ? "#9ca3af" : "#0f766e",
            color: "#fff",
            cursor: loading ? "default" : "pointer",
            fontSize: 13,
          }}
        >
          {loading ? "分析中..." : "开始分析"}
        </button>
      </div>

      {error && (
        <p style={{ color: "#dc2626", fontSize: 12, marginTop: 8 }}>{error}</p>
      )}

      {result && (
        <div style={{ marginTop: 12, display: "flex", flexDirection: "column", gap: 8 }}>
          <div
            style={{
              padding: 10,
              borderRadius: 10,
              background: "#f9fafb",
              border: "1px solid #e5e7eb",
            }}
          >
            <div style={{ fontWeight: 600, marginBottom: 4 }}>
              {result.stock_code} {result.stock_name || ""}
            </div>
            {decision && (
              <div style={{ fontSize: 13 }}>
                决策: {decision.action} · 信心 {decision.confidence}% · 风险等级
                {" "}
                {decision.risk_level ?? "N/A"}
              </div>
            )}
            {result.session_info && (
              <div style={{ fontSize: 12, color: "#6b7280", marginTop: 2 }}>
                当前时段: {result.session_info.session} ·
                {" "}
                {result.session_info.recommendation}
              </div>
            )}
          </div>

          {market && (
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
                gap: 8,
              }}
            >
              <div
                style={{
                  padding: 8,
                  borderRadius: 8,
                  background: "#ecfeff",
                  border: "1px solid #bae6fd",
                  fontSize: 12,
                }}
              >
                <div>当前价: ¥{market.current_price?.toFixed?.(2) ?? market.current_price}</div>
                <div>涨跌幅: {market.change_pct?.toFixed?.(2) ?? market.change_pct}%</div>
              </div>
              <div
                style={{
                  padding: 8,
                  borderRadius: 8,
                  background: "#fefce8",
                  border: "1px solid #facc15",
                  fontSize: 12,
                }}
              >
                <div>成交量: {market.volume}</div>
                <div>换手率: {market.turnover_rate?.toFixed?.(2) ?? market.turnover_rate}%</div>
              </div>
              <div
                style={{
                  padding: 8,
                  borderRadius: 8,
                  background: "#f5f3ff",
                  border: "1px solid #ddd6fe",
                  fontSize: 12,
                }}
              >
                <div>MA5: {market.ma5}</div>
                <div>MA20: {market.ma20}</div>
                <div>MA60: {market.ma60}</div>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function TasksTab() {
  const [tasks, setTasks] = useState<MonitorTask[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      setLoading(true);
      setError(null);
      try {
        const res = await fetch(
          `${API_BASE.replace(/\/$/, "")}/smart-monitor/tasks?enabled_only=false`,
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data: MonitorTask[] = await res.json();
        setTasks(data);
      } catch (e: any) {
        setError(e?.message || "加载监控任务失败");
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  return (
    <div>
      <h2 style={{ marginTop: 0, fontSize: 16 }}>🎯 监控任务</h2>
      {loading && <p>加载中...</p>}
      {error && (
        <p style={{ color: "#dc2626", fontSize: 12 }}>{error}</p>
      )}
      {!loading && !error && tasks.length === 0 && (
        <p style={{ fontSize: 13, color: "#666" }}>当前暂无监控任务。</p>
      )}
      {tasks.length > 0 && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {tasks.map((t) => (
            <div
              key={t.id}
              style={{
                border: "1px solid #e5e7eb",
                borderRadius: 10,
                padding: 10,
                background: "#f9fafb",
              }}
            >
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <div>
                  <div style={{ fontWeight: 600 }}>
                    {t.task_name} ({t.stock_code})
                  </div>
                  <div
                    style={{ fontSize: 12, color: "#6b7280", marginTop: 2 }}
                  >
                    间隔 {t.check_interval} 秒 ·
                    {" "}
                    {t.auto_trade ? "🤖 自动交易" : "👀 仅监控"}
                  </div>
                </div>
                <div style={{ fontSize: 12, textAlign: "right" }}>
                  <div>
                    {t.enabled ? (
                      <span style={{ color: "#16a34a" }}>✅ 已启用</span>
                    ) : (
                      <span style={{ color: "#6b7280" }}>⏸️ 已禁用</span>
                    )}
                  </div>
                  {t.has_position && t.position_quantity && t.position_cost && (
                    <div style={{ color: "#0f766e", marginTop: 2 }}>
                      持仓 {t.position_quantity} 股 @ {t.position_cost} 元
                    </div>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function PositionsTab() {
  const [positions, setPositions] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      setLoading(true);
      setError(null);
      try {
        const res = await fetch(
          `${API_BASE.replace(/\/$/, "")}/smart-monitor/positions`,
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data: any[] = await res.json();
        setPositions(data);
      } catch (e: any) {
        setError(e?.message || "加载持仓监控失败");
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  return (
    <div>
      <h2 style={{ marginTop: 0, fontSize: 16 }}>📈 持仓管理</h2>
      {loading && <p>加载中...</p>}
      {error && (
        <p style={{ color: "#dc2626", fontSize: 12 }}>{error}</p>
      )}
      {!loading && !error && positions.length === 0 && (
        <p style={{ fontSize: 13, color: "#666" }}>当前无持仓监控记录。</p>
      )}
      {positions.length > 0 && (
        <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ background: "#f3f4f6" }}>
              <th style={{ padding: 6, textAlign: "left" }}>代码</th>
              <th style={{ padding: 6, textAlign: "left" }}>名称</th>
              <th style={{ padding: 6, textAlign: "right" }}>持仓</th>
              <th style={{ padding: 6, textAlign: "right" }}>成本价</th>
              <th style={{ padding: 6, textAlign: "right" }}>现价</th>
              <th style={{ padding: 6, textAlign: "right" }}>盈亏%</th>
            </tr>
          </thead>
          <tbody>
            {positions.map((p) => (
              <tr key={p.id}>
                <td style={{ padding: 6 }}>{p.stock_code}</td>
                <td style={{ padding: 6 }}>{p.stock_name}</td>
                <td style={{ padding: 6, textAlign: "right" }}>{p.quantity}</td>
                <td style={{ padding: 6, textAlign: "right" }}>{p.cost_price}</td>
                <td style={{ padding: 6, textAlign: "right" }}>{p.current_price}</td>
                <td style={{ padding: 6, textAlign: "right" }}>
                  {p.profit_loss_pct?.toFixed?.(2) ?? p.profit_loss_pct}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

function HistoryTab() {
  const [decisions, setDecisions] = useState<any[]>([]);
  const [trades, setTrades] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      setLoading(true);
      setError(null);
      try {
        const [decRes, trdRes] = await Promise.all([
          fetch(`${API_BASE.replace(/\/$/, "")}/smart-monitor/decisions`),
          fetch(`${API_BASE.replace(/\/$/, "")}/smart-monitor/trades`),
        ]);
        if (!decRes.ok) throw new Error(`决策历史请求失败 ${decRes.status}`);
        if (!trdRes.ok) throw new Error(`交易记录请求失败 ${trdRes.status}`);
        const decData: any[] = await decRes.json();
        const trdData: any[] = await trdRes.json();
        setDecisions(decData);
        setTrades(trdData);
      } catch (e: any) {
        setError(e?.message || "加载历史记录失败");
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  return (
    <div>
      <h2 style={{ marginTop: 0, fontSize: 16 }}>📜 历史记录</h2>
      {loading && <p>加载中...</p>}
      {error && (
        <p style={{ color: "#dc2626", fontSize: 12 }}>{error}</p>
      )}
      {!loading && !error && decisions.length === 0 && trades.length === 0 && (
        <p style={{ fontSize: 13, color: "#666" }}>暂无历史记录。</p>
      )}
      {decisions.length > 0 && (
        <div style={{ marginBottom: 16 }}>
          <h3 style={{ fontSize: 14 }}>🤖 AI决策历史</h3>
          <ul style={{ paddingLeft: 16, margin: 0 }}>
            {decisions.map((d) => (
              <li key={d.id} style={{ marginBottom: 4 }}>
                {d.decision_time} · {d.stock_code} {d.stock_name} · {d.action} ·
                信心 {d.confidence}%
              </li>
            ))}
          </ul>
        </div>
      )}
      {trades.length > 0 && (
        <div>
          <h3 style={{ fontSize: 14 }}>💱 交易记录</h3>
          <ul style={{ paddingLeft: 16, margin: 0 }}>
            {trades.map((t) => (
              <li key={t.id} style={{ marginBottom: 4 }}>
                {t.trade_time} · {t.stock_code} {t.stock_name} · {t.trade_type} ·
                数量 {t.quantity} · 价格 {t.price}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

function SettingsTab() {
  return (
    <div>
      <h2 style={{ marginTop: 0, fontSize: 16 }}>⚙️ 系统设置</h2>
      <p style={{ fontSize: 13, color: "#666" }}>
        智能盯盘的环境配置（DeepSeek、MiniQMT、通知方式等）沿用主系统的
        
        <code>.env</code> 与 <a href="/config">/config</a> 页面。后续可在此处补充只读概览和快捷操作。
      </p>
    </div>
  );
}
