"use client";

import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface QMTStatus {
  enabled: boolean;
  connected: boolean;
  mode: string;
  account_id: string | null;
  provider: string;
  userdata_path: string | null;
  session_id: number | null;
  last_error: string | null;
}

interface AccountInfo {
  provider: string;
  connected: boolean;
  mode: string;
  account_id: string;
  available_cash: number;
  total_asset: number;
  market_value: number;
  frozen_cash: number;
  fetch_balance: number;
}

export interface Position {
  stock_code: string;
  stock_name: string;
  quantity: number;
  can_sell: number;
  open_price: number;
  cost_price: number;
  current_price: number;
  prev_close: number;
  market_value: number;
  // 持仓总盈亏金额（相对成本价的累计浮盈/浮亏），对应 xtquant 的 position_profit
  position_profit: number;
  // 当日盈亏金额（相对昨收的日内盈亏），对应 xtquant 的 float_profit
  float_profit: number;
  // 持仓总盈亏比例（position_profit / 成本市值），对应 xtquant 的 profit_rate
  profit_rate: number;
  secu_account: string;
}

interface Order {
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

interface Trade {
  traded_id: string;
  stock_code: string;
  stock_name: string;
  order_type: number;
  order_type_name: string;
  traded_time: string;
  traded_price: number;
  traded_volume: number;
  traded_amount: number;
  order_id: string;
  order_sysid: string;
  commission: number;
  strategy_name: string;
  order_remark: string;
  secu_account: string;
}

function formatNumber(v: number | null | undefined, digits = 2) {
  if (v === null || v === undefined) return "-";
  const n = typeof v === "number" ? v : Number(v);
  if (Number.isNaN(n)) return "-";
  return n.toFixed(digits);
}

function formatDateTime(s?: string | null) {
  if (!s) return "-";
  try {
    // 处理格式如 "20241220103000" 的时间字符串
    if (s.length === 14 && /^\d+$/.test(s)) {
      const year = s.substring(0, 4);
      const month = s.substring(4, 6);
      const day = s.substring(6, 8);
      const hour = s.substring(8, 10);
      const minute = s.substring(10, 12);
      const second = s.substring(12, 14);
      return `${year}-${month}-${day} ${hour}:${minute}:${second}`;
    }
    const d = new Date(s);
    if (Number.isNaN(d.getTime())) return s;
    return `${d.getMonth() + 1}-${d.getDate().toString().padStart(2, "0")} ${d
      .getHours()
      .toString()
      .padStart(2, "0")}:${d.getMinutes().toString().padStart(2, "0")}`;
  } catch {
    return s;
  }
}

interface MonitorSummary {
  timestamp: string;
  account: {
    total_asset: number;
    market_value: number;
    available_cash: number;
    total_position_profit: number;
    total_daily_profit: number;
  };
  positions: Array<Record<string, any>>;
  alerts: Array<Record<string, any>>;
}

interface MonitorGlobalConfig {
  max_total_drawdown: number;
  max_daily_loss: number;
  max_position_weight: number;
  min_available_cash_ratio: number;
}

interface MonitorConfigResponse {
  global: MonitorGlobalConfig;
  per_symbol: Record<string, any>;
}

export default function QMTPositionsPage() {
  const [activeTab, setActiveTab] = useState<
    "positions" | "orders" | "trades" | "account" | "trade" | "ipo" | "bank" | "monitor"
  >("positions");

  const [status, setStatus] = useState<QMTStatus | null>(null);
  const [account, setAccount] = useState<AccountInfo | null>(null);
  const [positions, setPositions] = useState<Position[]>([]);
  const [orders, setOrders] = useState<Order[]>([]);
  const [trades, setTrades] = useState<Trade[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [connecting, setConnecting] = useState(false);
  const [autoRefreshEnabled, setAutoRefreshEnabled] = useState(false); // 默认关闭自动刷新
  const [refreshInterval, setRefreshInterval] = useState(5); // 默认5秒

  // 交易相关状态
  const [showOrderForm, setShowOrderForm] = useState(false);
  const [orderStockCode, setOrderStockCode] = useState("");
  const [orderType, setOrderType] = useState<23 | 24>(23); // 买入
  const [orderVolume, setOrderVolume] = useState(100);
  const [priceType, setPriceType] = useState("FIX_PRICE"); // 限价
  const [orderPrice, setOrderPrice] = useState(0);
  const [orderStrategyName, setOrderStrategyName] = useState("");
  const [orderRemark, setOrderRemark] = useState("");
  const [placingOrder, setPlacingOrder] = useState(false);
  const [cancelingOrderId, setCancelingOrderId] = useState<string | null>(null);

  // 新股申购相关状态
  const [ipoLimit, setIpoLimit] = useState<Record<string, any> | null>(null);
  const [ipoList, setIpoList] = useState<Array<Record<string, any>>>([]);
  const [loadingIpo, setLoadingIpo] = useState(false);

  // 银证转账相关状态
  const [bankInfo, setBankInfo] = useState<Array<Record<string, any>>>([]);
  const [transferDirection, setTransferDirection] = useState<"in" | "out">("in");
  const [transferBankNo, setTransferBankNo] = useState("");
  const [transferBankAccount, setTransferBankAccount] = useState("");
  const [transferBankPwd, setTransferBankPwd] = useState("");
  const [transferAmount, setTransferAmount] = useState(0);
  const [transferring, setTransferring] = useState(false);

  // QMT 账户级监控摘要
  const [monitorSummary, setMonitorSummary] = useState<MonitorSummary | null>(null);
  // QMT 监控阈值配置（本地默认 + 后端覆盖）
  const [monitorGlobal, setMonitorGlobal] = useState<MonitorGlobalConfig>({
    max_total_drawdown: -10000,
    max_daily_loss: -3000,
    max_position_weight: 0.3,
    min_available_cash_ratio: 0.05,
  });
  const [monitorPerSymbolJson, setMonitorPerSymbolJson] = useState("{}");

  async function loadStatus() {
    try {
      const res = await fetch(`${API_BASE}/qmt/status`);
      if (!res.ok) throw new Error(`获取状态失败: ${res.status}`);
      const data: QMTStatus = await res.json();
      setStatus(data);
      return data;
    } catch (e: any) {
      setError(e?.message || "获取状态失败");
      return null;
    }
  }

  async function handleConnect() {
    setConnecting(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/qmt/connect`, {
        method: "POST",
      });
      const data = await res.json();
      if (!res.ok || !data.success) {
        throw new Error(data.message || `连接失败: ${res.status}`);
      }
      await loadStatus();
      await loadData();
    } catch (e: any) {
      setError(e?.message || "连接失败");
    } finally {
      setConnecting(false);
    }
  }

  async function loadData(skipStatusCheck = false) {
    // 如果未跳过状态检查，则检查连接状态
    if (!skipStatusCheck) {
      const currentStatus = await loadStatus();
      if (!currentStatus?.connected) return;
    } else if (!status?.connected) {
      return;
    }

    setLoading(true);
    setError(null);
    try {
      // 加载账户信息
      const accountRes = await fetch(`${API_BASE}/qmt/account`);
      if (accountRes.ok) {
        const accountData: AccountInfo = await accountRes.json();
        setAccount(accountData);
      }

      // 加载持仓
      const positionsRes = await fetch(`${API_BASE}/qmt/positions`);
      if (positionsRes.ok) {
        const positionsData: Position[] = await positionsRes.json();
        setPositions(positionsData || []);
      }

      // 加载委托
      const ordersRes = await fetch(`${API_BASE}/qmt/orders`);
      if (ordersRes.ok) {
        const ordersData: Order[] = await ordersRes.json();
        setOrders(ordersData || []);
      }

      // 加载成交
      const tradesRes = await fetch(`${API_BASE}/qmt/trades`);
      if (tradesRes.ok) {
        const tradesData: Trade[] = await tradesRes.json();
        setTrades(tradesData || []);
      }

      // 加载账户监控摘要（仅供监控页使用）
      try {
        const monitorRes = await fetch(`${API_BASE}/qmt/monitor/summary`);
        if (monitorRes.ok) {
          const monitorData: MonitorSummary = await monitorRes.json();
          setMonitorSummary(monitorData);
        } else {
          setMonitorSummary(null);
        }
      } catch {
        setMonitorSummary(null);
      }
    } catch (e: any) {
      setError(e?.message || "加载数据失败");
    } finally {
      setLoading(false);
    }
  }

  // 页面加载时自动连接
  useEffect(() => {
    let mounted = true;

    async function init() {
      const s = await loadStatus();
      if (!mounted) return;

      // 如果未连接且已启用，则自动连接
      if (s && !s.connected && s.enabled) {
        setConnecting(true);
        try {
          const res = await fetch(`${API_BASE}/qmt/connect`, {
            method: "POST",
          });
          const data = await res.json();
          if (res.ok && data.success) {
            await loadStatus();
            await loadData();
          } else {
            setError(data.message || "自动连接失败");
          }
        } catch (e: any) {
          setError(e?.message || "自动连接失败");
        } finally {
          setConnecting(false);
        }
      } else if (s?.connected) {
        // 如果已连接，直接加载数据
        await loadData();
      }
    }

    init();

    return () => {
      mounted = false;
    };
  }, []);

  // 切换标签页时加载数据
  useEffect(() => {
    if (status?.connected) {
      loadData();
    }
  }, [activeTab]);

  // 自动刷新功能
  useEffect(() => {
    if (!autoRefreshEnabled || !status?.connected) return;

    const intervalId = setInterval(() => {
      // 直接刷新数据，不重复检查状态（状态由定时器触发时已保证连接）
      loadData(true);
    }, refreshInterval * 1000);

    return () => {
      clearInterval(intervalId);
    };
  }, [autoRefreshEnabled, refreshInterval, status?.connected]);

  const positionStats = {
    total: positions.length,
    totalMarketValue: positions.reduce((sum, p) => sum + p.market_value, 0),
    // 总持仓盈亏（相对成本价的累计浮盈/浮亏），对应 xtquant position_profit
    totalPositionProfit: positions.reduce((sum, p) => sum + p.position_profit, 0),
    // 当日盈亏总和（相对昨收的日内盈亏），对应 xtquant float_profit
    totalDailyProfit: positions.reduce((sum, p) => sum + p.float_profit, 0),
  };

  function requestTradePassword(actionLabel: string): string | null {
    const pwd = window.prompt(`执行${actionLabel}需要输入交易密码`, "");
    if (pwd === null) return null;
    const trimmed = pwd.trim();
    if (!trimmed) {
      setError("交易密码不能为空");
      return null;
    }
    return trimmed;
  }

  // 下单函数
  async function handlePlaceOrder() {
    if (!orderStockCode.trim()) {
      setError("请输入股票代码");
      return;
    }
    if (orderVolume <= 0) {
      setError("委托数量必须大于0");
      return;
    }
    if (priceType === "FIX_PRICE" && orderPrice <= 0) {
      setError("限价单必须填写价格");
      return;
    }

    const tradePassword = requestTradePassword("下单");
    if (!tradePassword) {
      return;
    }

    setPlacingOrder(true);
    setError(null);
    try {
      // 转换股票代码格式：600000 -> 600000.SH, 000001 -> 000001.SZ
      let stockCode = orderStockCode.trim().toUpperCase();
      if (!stockCode.includes(".")) {
        if (stockCode.startsWith("6") || stockCode.startsWith("9")) {
          stockCode = `${stockCode}.SH`;
        } else {
          stockCode = `${stockCode}.SZ`;
        }
      }

      // 价格类型映射（根据 xtconstant.py）
      const priceTypeMap: Record<string, number> = {
        FIX_PRICE: 11, // 限价
        LATEST_PRICE: 5, // 最新价
        MARKET_PEER_PRICE_FIRST: 44, // 对手方最优
        MARKET_MINE_PRICE_FIRST: 45, // 本方最优
      };

      const res = await fetch(`${API_BASE}/qmt/order`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          stock_code: stockCode,
          order_type: orderType,
          order_volume: orderVolume,
          price_type: priceTypeMap[priceType] || 11,
          price: priceType === "FIX_PRICE" ? orderPrice : 0,
          strategy_name: orderStrategyName.trim(),
          order_remark: orderRemark.trim(),
          trade_password: tradePassword,
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || data?.message || `下单失败: ${res.status}`);
      }
      if (data.success) {
        alert(`下单成功！订单编号：${data.order_id}`);
        setShowOrderForm(false);
        setOrderStockCode("");
        setOrderVolume(100);
        setOrderPrice(0);
        setOrderStrategyName("");
        setOrderRemark("");
        // 刷新数据
        await loadData();
      } else {
        setError(`下单失败：${data.message}`);
      }
    } catch (e: any) {
      setError(e?.message || "下单失败");
    } finally {
      setPlacingOrder(false);
    }
  }

  // 撤单函数
  async function handleCancelOrder(orderId: string) {
    if (!confirm("确认撤单吗？")) return;

    const tradePassword = requestTradePassword("撤单");
    if (!tradePassword) {
      return;
    }

    setCancelingOrderId(orderId);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/qmt/cancel`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ order_id: orderId, trade_password: tradePassword }),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || data?.message || `撤单失败: ${res.status}`);
      }
      if (data.success) {
        alert("撤单成功！");
        await loadData();
      } else {
        setError(`撤单失败：${data.message}`);
      }
    } catch (e: any) {
      setError(e?.message || "撤单失败");
    } finally {
      setCancelingOrderId(null);
    }
  }

  // 加载新股申购额度
  async function loadIpoLimit() {
    setLoadingIpo(true);
    try {
      const res = await fetch(`${API_BASE}/qmt/ipo/limit`);
      if (res.ok) {
        const data = await res.json();
        setIpoLimit(data);
      }
    } catch (e: any) {
      setError(e?.message || "加载新股申购额度失败");
    } finally {
      setLoadingIpo(false);
    }
  }

  // 加载新股列表
  async function loadIpoList() {
    setLoadingIpo(true);
    try {
      const res = await fetch(`${API_BASE}/qmt/ipo/list`);
      if (res.ok) {
        const data = await res.json();
        setIpoList(data || []);
      }
    } catch (e: any) {
      setError(e?.message || "加载新股列表失败");
    } finally {
      setLoadingIpo(false);
    }
  }

  // 加载银行信息
  async function loadBankInfo() {
    try {
      const res = await fetch(`${API_BASE}/qmt/bank/info`);
      if (res.ok) {
        const data = await res.json();
        setBankInfo(data || []);
        if (data && data.length > 0) {
          setTransferBankNo(data[0].bank_no || "");
        }
      }
    } catch (e: any) {
      setError(e?.message || "加载银行信息失败");
    }
  }

  // 银证转账
  async function handleBankTransfer() {
    if (!transferBankNo || !transferBankAccount || !transferBankPwd) {
      setError("请填写完整的银行信息");
      return;
    }
    if (transferAmount <= 0) {
      setError("转账金额必须大于0");
      return;
    }

    const tradePassword = requestTradePassword("银证转账");
    if (!tradePassword) {
      return;
    }

    setTransferring(true);
    setError(null);
    try {
      const endpoint =
        transferDirection === "in" ? "/qmt/bank/transfer-in" : "/qmt/bank/transfer-out";
      const res = await fetch(`${API_BASE}${endpoint}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          bank_no: transferBankNo,
          bank_account: transferBankAccount,
          bank_pwd: transferBankPwd,
          amount: transferAmount,
          trade_password: tradePassword,
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || data?.message || `转账失败: ${res.status}`);
      }
      if (data.success) {
        alert(`转账成功！${data.message}`);
        setTransferAmount(0);
        await loadData(); // 刷新账户信息
      } else {
        setError(`转账失败：${data.message}`);
      }
    } catch (e: any) {
      setError(e?.message || "转账失败");
    } finally {
      setTransferring(false);
    }
  }

  // 切换标签页时加载对应数据
  useEffect(() => {
    if (activeTab === "ipo" && status?.connected) {
      loadIpoLimit();
      loadIpoList();
    }
    if (activeTab === "bank" && status?.connected) {
      loadBankInfo();
    }
  }, [activeTab, status?.connected]);

  // 仅在进入监控标签时加载监控配置
  useEffect(() => {
    async function loadMonitorConfig() {
      try {
        const res = await fetch(`${API_BASE}/qmt/monitor/config`);
        if (res.ok) {
          const data: MonitorConfigResponse = await res.json();
          if (data.global) {
            setMonitorGlobal(data.global);
          }
          setMonitorPerSymbolJson(JSON.stringify(data.per_symbol || {}, null, 2));
        }
      } catch {
        // 忽略，保持本地默认
      }
    }

    if (activeTab === "monitor" && status?.connected) {
      loadMonitorConfig();
    }
  }, [activeTab, status?.connected]);

  async function handleSaveMonitorConfig() {
    try {
      let perSymbolParsed: Record<string, any> = {};
      if (monitorPerSymbolJson.trim()) {
        perSymbolParsed = JSON.parse(monitorPerSymbolJson);
      }

      const payload = {
        global: monitorGlobal,
        per_symbol: perSymbolParsed,
      };

      const res = await fetch(`${API_BASE}/qmt/monitor/config`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const txt = await res.text();
        alert(`保存监控配置失败: ${res.status} ${txt}`);
        return;
      }
      alert("监控阈值已保存");
    } catch (e: any) {
      alert(e?.message || "保存监控配置失败，请检查 per_symbol JSON 格式");
    }
  }

  return (
    <main style={{ padding: 24 }}>
      <section style={{ marginBottom: 16 }}>
        <h1 style={{ margin: 0, fontSize: 22 }}>💼 QMT 持仓管理</h1>
        <p style={{ marginTop: 4, fontSize: 13, color: "#666" }}>
          查看 QMT 模拟盘的资金、持仓、委托和成交信息
        </p>
      </section>

      {/* 连接状态和操作 */}
      <section style={{ marginBottom: 16 }}>
        <div
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 14,
            boxShadow: "0 2px 8px rgba(0,0,0,0.06)",
          }}
        >
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
            }}
          >
            <div>
              <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 4 }}>
                连接状态
              </div>
              <div style={{ fontSize: 13, color: "#666" }}>
                {status ? (
                  <>
                    {status.connected ? (
                      <span style={{ color: "#16a34a" }}>✅ 已连接</span>
                    ) : (
                      <span style={{ color: "#dc2626" }}>❌ 未连接</span>
                    )}
                    {status.account_id && (
                      <> | 账户: {status.account_id} | 模式: {status.mode}</>
                    )}
                    {status.provider && <> | 提供者: {status.provider}</>}
                  </>
                ) : (
                  "加载中..."
                )}
              </div>
            </div>
            <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
              {/* 自动刷新控制 */}
              {status?.connected && (
                <div
                  style={{
                    display: "flex",
                    gap: 8,
                    alignItems: "center",
                    fontSize: 13,
                    padding: "4px 12px",
                    background: "#f3f4f6",
                    borderRadius: 8,
                  }}
                >
                  <label
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 6,
                      cursor: "pointer",
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={autoRefreshEnabled}
                      onChange={(e) => setAutoRefreshEnabled(e.target.checked)}
                    />
                    <span>自动刷新</span>
                  </label>
                  {autoRefreshEnabled && (
                    <select
                      value={refreshInterval}
                      onChange={(e) =>
                        setRefreshInterval(Number(e.target.value))
                      }
                      style={{
                        padding: "2px 6px",
                        borderRadius: 4,
                        border: "1px solid #d4d4d4",
                        fontSize: 12,
                        background: "#fff",
                      }}
                    >
                      <option value={5}>5秒</option>
                      <option value={10}>10秒</option>
                      <option value={30}>30秒</option>
                      <option value={60}>60秒</option>
                    </select>
                  )}
                </div>
              )}
              {status && !status.connected && (
                <button
                  type="button"
                  disabled={connecting}
                  onClick={handleConnect}
                  style={{
                    padding: "6px 16px",
                    borderRadius: 8,
                    border: "none",
                    background:
                      "linear-gradient(135deg, #10b981 0%, #22c55e 100%)",
                    color: "#fff",
                    cursor: connecting ? "default" : "pointer",
                    fontWeight: 600,
                  }}
                >
                  {connecting ? "连接中..." : "连接 QMT"}
                </button>
              )}
              {status?.connected && (
                <button
                  type="button"
                  onClick={() => loadData()}
                  disabled={loading}
                  style={{
                    padding: "6px 16px",
                    borderRadius: 8,
                    border: "1px solid #d4d4d4",
                    background: "#fff",
                    cursor: loading ? "default" : "pointer",
                  }}
                >
                  {loading ? "刷新中..." : "🔄 手动刷新"}
                </button>
              )}
            </div>
          </div>
        </div>
      </section>

      {error && (
        <p style={{ color: "#b00020", fontSize: 13, marginBottom: 8 }}>
          错误：{error}
        </p>
      )}

      {/* 顶部标签切换 */}
      <section style={{ marginBottom: 12 }}>
        <div style={{ display: "flex", gap: 12, fontSize: 13 }}>
          <button
            type="button"
            onClick={() => setActiveTab("account")}
            style={{
              padding: "6px 12px",
              borderRadius: 999,
              border: "1px solid #ccc",
              background: activeTab === "account" ? "#eef2ff" : "#fff",
            }}
          >
            💰 账户资金
          </button>
          <button
            type="button"
            onClick={() => setActiveTab("positions")}
            style={{
              padding: "6px 12px",
              borderRadius: 999,
              border: "1px solid #ccc",
              background: activeTab === "positions" ? "#eef2ff" : "#fff",
            }}
          >
            📊 持仓明细 ({positionStats.total})
          </button>
          <button
            type="button"
            onClick={() => setActiveTab("orders")}
            style={{
              padding: "6px 12px",
              borderRadius: 999,
              border: "1px solid #ccc",
              background: activeTab === "orders" ? "#eef2ff" : "#fff",
            }}
          >
            📝 委托 ({orders.length})
          </button>
          <button
            type="button"
            onClick={() => setActiveTab("trades")}
            style={{
              padding: "6px 12px",
              borderRadius: 999,
              border: "1px solid #ccc",
              background: activeTab === "trades" ? "#eef2ff" : "#fff",
            }}
          >
            ✅ 成交 ({trades.length})
          </button>
          <button
            type="button"
            onClick={() => setActiveTab("trade")}
            style={{
              padding: "6px 12px",
              borderRadius: 999,
              border: "1px solid #ccc",
              background: activeTab === "trade" ? "#eef2ff" : "#fff",
            }}
          >
            📝 交易下单
          </button>
          <button
            type="button"
            onClick={() => setActiveTab("ipo")}
            style={{
              padding: "6px 12px",
              borderRadius: 999,
              border: "1px solid #ccc",
              background: activeTab === "ipo" ? "#eef2ff" : "#fff",
            }}
          >
            🎯 新股申购
          </button>
          <button
            type="button"
            onClick={() => setActiveTab("bank")}
            style={{
              padding: "6px 12px",
              borderRadius: 999,
              border: "1px solid #ccc",
              background: activeTab === "bank" ? "#eef2ff" : "#fff",
            }}
          >
            💳 银证转账
          </button>
          <button
            type="button"
            onClick={() => setActiveTab("monitor")}
            style={{
              padding: "6px 12px",
              borderRadius: 999,
              border: "1px solid #ccc",
              background: activeTab === "monitor" ? "#eef2ff" : "#fff",
            }}
          >
            🚨 账户监控
          </button>
        </div>
      </section>

      {/* 账户监控 */}
      {activeTab === "monitor" && (
        <section>
          {loading && <p style={{ fontSize: 13, color: "#666" }}>加载中...</p>}

          {!loading && !monitorSummary && (
            <div style={{ padding: 40, textAlign: "center", color: "#666" }}>
              暂无监控数据，请确认 QMT 已连接且有持仓。
            </div>
          )}

          {!loading && monitorSummary && (
            <>
              {/* 顶部账户监控摘要卡片 */}
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
                  gap: 12,
                  marginBottom: 16,
                }}
              >
                <div
                  style={{
                    background: "#fff",
                    borderRadius: 10,
                    padding: 16,
                    boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                  }}
                >
                  <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 8 }}>总资产</div>
                  <div style={{ fontSize: 24, fontWeight: 600 }}>
                    ¥{formatNumber(monitorSummary.account.total_asset)}
                  </div>
                </div>
                <div
                  style={{
                    background: "#fff",
                    borderRadius: 10,
                    padding: 16,
                    boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                  }}
                >
                  <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 8 }}>持仓市值</div>
                  <div style={{ fontSize: 24, fontWeight: 600 }}>
                    ¥{formatNumber(monitorSummary.account.market_value)}
                  </div>
                </div>
                <div
                  style={{
                    background: "#fff",
                    borderRadius: 10,
                    padding: 16,
                    boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                  }}
                >
                  <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 8 }}>总持仓盈亏</div>
                  <div
                    style={{
                      fontSize: 24,
                      fontWeight: 600,
                      color:
                        monitorSummary.account.total_position_profit > 0
                          ? "#dc2626"
                          : monitorSummary.account.total_position_profit < 0
                          ? "#16a34a"
                          : "#374151",
                    }}
                  >
                    ¥{formatNumber(monitorSummary.account.total_position_profit)}
                  </div>
                </div>
                <div
                  style={{
                    background: "#fff",
                    borderRadius: 10,
                    padding: 16,
                    boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                  }}
                >
                  <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 8 }}>当日盈亏</div>
                  <div
                    style={{
                      fontSize: 24,
                      fontWeight: 600,
                      color:
                        monitorSummary.account.total_daily_profit > 0
                          ? "#dc2626"
                          : monitorSummary.account.total_daily_profit < 0
                          ? "#16a34a"
                          : "#374151",
                    }}
                  >
                    ¥{formatNumber(monitorSummary.account.total_daily_profit)}
                  </div>
                </div>
              </div>

              {/* 告警列表 */}
              <div
                style={{
                  marginBottom: 16,
                  background: "#fff7ed",
                  borderRadius: 10,
                  padding: 12,
                  border: "1px solid #fed7aa",
                }}
              >
                <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>告警列表</div>
                {monitorSummary.alerts.length === 0 ? (
                  <div style={{ fontSize: 13, color: "#4b5563" }}>当前无告警。</div>
                ) : (
                  <ul style={{ margin: 0, paddingLeft: 20, fontSize: 13, color: "#4b5563" }}>
                    {monitorSummary.alerts.map((a, idx) => (
                      <li key={idx}>
                        [{a.level}] {a.message}
                      </li>
                    ))}
                  </ul>
                )}
              </div>

              {/* 持仓监控明细 */}
              <div style={{ marginBottom: 24 }}>
                <h3 style={{ marginTop: 0, marginBottom: 8 }}>持仓监控明细</h3>
                {monitorSummary.positions.length === 0 ? (
                  <div style={{ padding: 12, color: "#6b7280" }}>暂无持仓。</div>
                ) : (
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse" }}>
                      <thead>
                        <tr style={{ background: "#f5f5f5" }}>
                          <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #ddd" }}>代码</th>
                          <th style={{ padding: 8, textAlign: "left", borderBottom: "2px solid #ddd" }}>名称</th>
                          <th style={{ padding: 8, textAlign: "right", borderBottom: "2px solid #ddd" }}>数量</th>
                          <th style={{ padding: 8, textAlign: "right", borderBottom: "2px solid #ddd" }}>市值</th>
                          <th style={{ padding: 8, textAlign: "right", borderBottom: "2px solid #ddd" }}>持仓盈亏</th>
                          <th style={{ padding: 8, textAlign: "right", borderBottom: "2px solid #ddd" }}>当日盈亏</th>
                          <th style={{ padding: 8, textAlign: "right", borderBottom: "2px solid #ddd" }}>资产占比</th>
                        </tr>
                      </thead>
                      <tbody>
                        {monitorSummary.positions.map((p: any, idx: number) => {
                          const posPnl = Number(p.position_profit || 0);
                          const dailyPnl = Number(p.float_profit || 0);
                          const mv = Number(p.market_value || 0);
                          const weightAsset = Number(p.weight_asset || 0);
                          return (
                            <tr key={idx} style={{ borderBottom: "1px solid #eee" }}>
                              <td style={{ padding: 8 }}>{p.stock_code}</td>
                              <td style={{ padding: 8 }}>{p.stock_name}</td>
                              <td style={{ padding: 8, textAlign: "right" }}>{p.quantity}</td>
                              <td style={{ padding: 8, textAlign: "right" }}>{mv.toFixed(2)}</td>
                              <td
                                style={{
                                  padding: 8,
                                  textAlign: "right",
                                  color: posPnl > 0 ? "#dc2626" : posPnl < 0 ? "#16a34a" : "#374151",
                                }}
                              >
                                {posPnl.toFixed(2)}
                              </td>
                              <td
                                style={{
                                  padding: 8,
                                  textAlign: "right",
                                  color: dailyPnl > 0 ? "#dc2626" : dailyPnl < 0 ? "#16a34a" : "#374151",
                                }}
                              >
                                {dailyPnl.toFixed(2)}
                              </td>
                              <td style={{ padding: 8, textAlign: "right" }}>{(weightAsset * 100).toFixed(2)}%</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>

              {/* 监控阈值配置 */}
              <div
                style={{
                  background: "#fff",
                  borderRadius: 12,
                  padding: 16,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.05)",
                }}
              >
                <h3 style={{ marginTop: 0, marginBottom: 12 }}>监控阈值配置</h3>
                <div style={{ display: "grid", gridTemplateColumns: "2fr 3fr", gap: 16, fontSize: 13 }}>
                  <div>
                    <h4 style={{ marginTop: 0, marginBottom: 8 }}>全局阈值</h4>
                    <div style={{ display: "grid", gap: 8 }}>
                      <label style={{ display: "block" }}>
                        <span>总持仓回撤下限 (max_total_drawdown)</span>
                        <input
                          type="number"
                          value={monitorGlobal?.max_total_drawdown || 0}
                          onChange={(e) =>
                            setMonitorGlobal({
                              ...monitorGlobal,
                              max_total_drawdown: Number(e.target.value || 0),
                            })
                          }
                          style={{
                            width: "100%",
                            marginTop: 4,
                            padding: "4px 8px",
                            borderRadius: 6,
                            border: "1px solid #d4d4d4",
                          }}
                        />
                      </label>
                      <label style={{ display: "block" }}>
                        <span>当日亏损下限 (max_daily_loss)</span>
                        <input
                          type="number"
                          value={monitorGlobal?.max_daily_loss || 0}
                          onChange={(e) =>
                            setMonitorGlobal({
                              ...monitorGlobal,
                              max_daily_loss: Number(e.target.value || 0),
                            })
                          }
                          style={{
                            width: "100%",
                            marginTop: 4,
                            padding: "4px 8px",
                            borderRadius: 6,
                            border: "1px solid #d4d4d4",
                          }}
                        />
                      </label>
                      <label style={{ display: "block" }}>
                        <span>单票最大资产占比 (max_position_weight)</span>
                        <input
                          type="number"
                          value={monitorGlobal?.max_position_weight || 0}
                          step={0.01}
                          onChange={(e) =>
                            setMonitorGlobal({
                              ...monitorGlobal,
                              max_position_weight: Number(e.target.value || 0),
                            })
                          }
                          style={{
                            width: "100%",
                            marginTop: 4,
                            padding: "4px 8px",
                            borderRadius: 6,
                            border: "1px solid #d4d4d4",
                          }}
                        />
                      </label>
                      <label style={{ display: "block" }}>
                        <span>最小可用资金比例 (min_available_cash_ratio)</span>
                        <input
                          type="number"
                          value={monitorGlobal?.min_available_cash_ratio || 0}
                          step={0.01}
                          onChange={(e) =>
                            setMonitorGlobal({
                              ...monitorGlobal,
                              min_available_cash_ratio: Number(e.target.value || 0),
                            })
                          }
                          style={{
                            width: "100%",
                            marginTop: 4,
                            padding: "4px 8px",
                            borderRadius: 6,
                            border: "1px solid #d4d4d4",
                          }}
                        />
                      </label>
                    </div>
                  </div>
                  <div>
                    <h4 style={{ marginTop: 0, marginBottom: 8 }}>按股票代码阈值 per_symbol (JSON)</h4>
                    <textarea
                      value={monitorPerSymbolJson}
                      onChange={(e) => setMonitorPerSymbolJson(e.target.value)}
                      rows={12}
                      style={{
                        width: "100%",
                        fontFamily: "monospace",
                        fontSize: 12,
                        padding: 8,
                        borderRadius: 6,
                        border: "1px solid #d4d4d4",
                        resize: "vertical",
                      }}
                    />
                    <p style={{ fontSize: 12, color: "#6b7280", marginTop: 4 }}>
                      示例: <code>{'{"600000.SH":{"max_daily_loss":-500,"max_position_loss":-2000}}'}</code>
                    </p>
                  </div>
                </div>

                <div style={{ marginTop: 12, textAlign: "right" }}>
                  <button
                    type="button"
                    onClick={handleSaveMonitorConfig}
                    style={{
                      padding: "6px 16px",
                      borderRadius: 8,
                      border: "none",
                      background: "#4f46e5",
                      color: "#fff",
                      cursor: "pointer",
                      fontWeight: 600,
                    }}
                  >
                    保存监控阈值
                  </button>
                </div>
              </div>
            </>
          )}
        </section>
      )}

      {/* 账户资金 */}
      {activeTab === "account" && (
        <section>
          {loading && <p style={{ fontSize: 13, color: "#666" }}>加载中...</p>}
          {account && (
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
                gap: 12,
                marginBottom: 16,
              }}
            >
              <div
                style={{
                  background: "#fff",
                  borderRadius: 10,
                  padding: 16,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                }}
              >
                <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 8 }}>
                  可用资金
                </div>
                <div style={{ fontSize: 24, fontWeight: 600 }}>
                  ¥{formatNumber(account.available_cash)}
                </div>
              </div>
              <div
                style={{
                  background: "#fff",
                  borderRadius: 10,
                  padding: 16,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                }}
              >
                <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 8 }}>
                  总资产
                </div>
                <div style={{ fontSize: 24, fontWeight: 600 }}>
                  ¥{formatNumber(account.total_asset)}
                </div>
              </div>
              <div
                style={{
                  background: "#fff",
                  borderRadius: 10,
                  padding: 16,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                }}
              >
                <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 8 }}>
                  持仓市值
                </div>
                <div style={{ fontSize: 24, fontWeight: 600 }}>
                  ¥{formatNumber(account.market_value)}
                </div>
              </div>
              <div
                style={{
                  background: "#fff",
                  borderRadius: 10,
                  padding: 16,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                }}
              >
                <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 8 }}>
                  总持仓盈亏
                </div>
                <div
                  style={{
                    fontSize: 24,
                    fontWeight: 600,
                    color:
                      positionStats.totalPositionProfit > 0
                        ? "#dc2626"
                        : positionStats.totalPositionProfit < 0
                        ? "#16a34a"
                        : "#374151",
                  }}
                >
                  ¥{formatNumber(positionStats.totalPositionProfit)}
                </div>
              </div>
              <div
                style={{
                  background: "#fff",
                  borderRadius: 10,
                  padding: 16,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                }}
              >
                <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 8 }}>
                  当日盈亏
                </div>
                <div
                  style={{
                    fontSize: 24,
                    fontWeight: 600,
                    color:
                      positionStats.totalDailyProfit > 0
                        ? "#dc2626"
                        : positionStats.totalDailyProfit < 0
                        ? "#16a34a"
                        : "#374151",
                  }}
                >
                  ¥{formatNumber(positionStats.totalDailyProfit)}
                </div>
              </div>
              <div
                style={{
                  background: "#fff",
                  borderRadius: 10,
                  padding: 16,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                }}
              >
                <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 8 }}>
                  冻结资金
                </div>
                <div style={{ fontSize: 24, fontWeight: 600 }}>
                  ¥{formatNumber(account.frozen_cash)}
                </div>
              </div>
              <div
                style={{
                  background: "#fff",
                  borderRadius: 10,
                  padding: 16,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                }}
              >
                <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 8 }}>
                  可取资金
                </div>
                <div style={{ fontSize: 24, fontWeight: 600 }}>
                  ¥{formatNumber(account.fetch_balance)}
                </div>
              </div>
              <div
                style={{
                  background: "#fff",
                  borderRadius: 10,
                  padding: 16,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                }}
              >
                <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 8 }}>
                  账户模式
                </div>
                <div style={{ fontSize: 18, fontWeight: 600 }}>
                  {account.mode}
                </div>
              </div>
            </div>
          )}
          {!account && !loading && status?.connected && (
            <p style={{ fontSize: 13, color: "#666" }}>暂无账户信息</p>
          )}
        </section>
      )}

      {/* 持仓明细 */}
      {activeTab === "positions" && (
        <section>
          {loading && <p style={{ fontSize: 13, color: "#666" }}>加载中...</p>}
          {!loading && positionStats.total > 0 && (
            <>
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
                  gap: 12,
                  marginBottom: 16,
                }}
              >
                <div
                  style={{
                    background: "#fff",
                    borderRadius: 10,
                    padding: 12,
                    boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                  }}
                >
                  <div style={{ fontSize: 13, color: "#6b7280" }}>持仓数量</div>
                  <div style={{ marginTop: 4, fontSize: 18 }}>
                    {positionStats.total}
                  </div>
                </div>
                <div
                  style={{
                    background: "#fff",
                    borderRadius: 10,
                    padding: 12,
                    boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                  }}
                >
                  <div style={{ fontSize: 13, color: "#6b7280" }}>持仓市值</div>
                  <div style={{ marginTop: 4, fontSize: 18 }}>
                    ¥{formatNumber(positionStats.totalMarketValue)}
                  </div>
                </div>
                <div
                  style={{
                    background: "#fff",
                    borderRadius: 10,
                    padding: 12,
                    boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                  }}
                >
                  <div style={{ fontSize: 13, color: "#6b7280" }}>总持仓盈亏</div>
                  <div
                    style={{
                      marginTop: 4,
                      fontSize: 18,
                      color:
                        positionStats.totalPositionProfit > 0
                          ? "#dc2626"
                          : positionStats.totalPositionProfit < 0
                          ? "#16a34a"
                          : "#374151",
                    }}
                  >
                    ¥{formatNumber(positionStats.totalPositionProfit)}
                  </div>
                </div>
                <div
                  style={{
                    background: "#fff",
                    borderRadius: 10,
                    padding: 12,
                    boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                  }}
                >
                  <div style={{ fontSize: 13, color: "#6b7280" }}>当日盈亏</div>
                  <div
                    style={{
                      marginTop: 4,
                      fontSize: 18,
                      color:
                        positionStats.totalDailyProfit > 0
                          ? "#dc2626"
                          : positionStats.totalDailyProfit < 0
                          ? "#16a34a"
                          : "#374151",
                    }}
                  >
                    ¥{formatNumber(positionStats.totalDailyProfit)}
                  </div>
                </div>
              </div>

              <div
                style={{
                  background: "#fff",
                  borderRadius: 12,
                  padding: 14,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.05)",
                  overflowX: "auto",
                }}
              >
                <table
                  style={{
                    width: "100%",
                    borderCollapse: "collapse",
                    fontSize: 12,
                  }}
                >
                  <thead>
                    <tr>
                      <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #e5e7eb" }}>
                        代码/名称
                      </th>
                      <th style={{ textAlign: "left", padding: 8, borderBottom: "1px solid #e5e7eb" }}>
                        股东账户
                      </th>
                      <th style={{ textAlign: "right", padding: 8, borderBottom: "1px solid #e5e7eb" }}>
                        持仓数量
                      </th>
                      <th style={{ textAlign: "right", padding: 8, borderBottom: "1px solid #e5e7eb" }}>
                        可卖数量
                      </th>
                      <th style={{ textAlign: "right", padding: 8, borderBottom: "1px solid #e5e7eb" }}>
                        成本价
                      </th>
                      <th style={{ textAlign: "right", padding: 8, borderBottom: "1px solid #e5e7eb" }}>
                        现价
                      </th>
                      <th style={{ textAlign: "right", padding: 8, borderBottom: "1px solid #e5e7eb" }}>
                        市值
                      </th>
                      <th style={{ textAlign: "right", padding: 8, borderBottom: "1px solid #e5e7eb" }}>
                        持仓总成本
                      </th>
                      <th style={{ textAlign: "right", padding: 8, borderBottom: "1px solid #e5e7eb" }}>
                        持仓盈亏(¥)
                      </th>
                      <th style={{ textAlign: "right", padding: 8, borderBottom: "1px solid #e5e7eb" }}>
                        持仓盈亏(%)
                      </th>
                      <th style={{ textAlign: "right", padding: 8, borderBottom: "1px solid #e5e7eb" }}>
                        当日盈亏(¥)
                      </th>
                      <th style={{ textAlign: "right", padding: 8, borderBottom: "1px solid #e5e7eb" }}>
                        当日盈亏(%)
                      </th>
                      <th style={{ textAlign: "right", padding: 8, borderBottom: "1px solid #e5e7eb" }}>
                        资产占比 / 市值占比
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {positions.map((pos, idx) => {
                      const totalCost = pos.cost_price * pos.quantity;
                      const dailyPct =
                        totalCost > 0 ? (pos.float_profit / totalCost) * 100 : 0;
                      const assetTotal = account?.total_asset || 0;
                      const mvTotal = positionStats.totalMarketValue || 0;
                      const assetPct = assetTotal > 0 ? (pos.market_value / assetTotal) * 100 : 0;
                      const mvPct = mvTotal > 0 ? (pos.market_value / mvTotal) * 100 : 0;
                      return (
                        <tr key={idx}>
                          <td style={{ padding: 8, borderBottom: "1px solid #f3f4f6" }}>
                            <div style={{ fontWeight: 600 }}>
                              {pos.stock_code} {pos.stock_name}
                            </div>
                          </td>
                          <td style={{ padding: 8, borderBottom: "1px solid #f3f4f6" }}>
                            {pos.secu_account}
                          </td>
                          <td
                            style={{
                              padding: 8,
                              borderBottom: "1px solid #f3f4f6",
                              textAlign: "right",
                            }}
                          >
                            {pos.quantity}
                          </td>
                          <td
                            style={{
                              padding: 8,
                              borderBottom: "1px solid #f3f4f6",
                              textAlign: "right",
                            }}
                          >
                            {pos.can_sell}
                          </td>
                          <td
                            style={{
                              padding: 8,
                              borderBottom: "1px solid #f3f4f6",
                              textAlign: "right",
                            }}
                          >
                            ¥{formatNumber(pos.cost_price, 3)}
                          </td>
                          <td
                            style={{
                              padding: 8,
                              borderBottom: "1px solid #f3f4f6",
                              textAlign: "right",
                            }}
                          >
                            ¥{formatNumber(pos.current_price, 3)}
                          </td>
                          <td
                            style={{
                              padding: 8,
                              borderBottom: "1px solid #f3f4f6",
                              textAlign: "right",
                            }}
                          >
                            ¥{formatNumber(pos.market_value)}
                          </td>
                          <td
                            style={{
                              padding: 8,
                              borderBottom: "1px solid #f3f4f6",
                              textAlign: "right",
                            }}
                          >
                            ¥{formatNumber(totalCost)}
                          </td>
                          <td
                            style={{
                              padding: 8,
                              borderBottom: "1px solid #f3f4f6",
                              textAlign: "right",
                              color:
                                pos.position_profit > 0
                                  ? "#dc2626"
                                  : pos.position_profit < 0
                                  ? "#16a34a"
                                  : "#374151",
                            }}
                          >
                            ¥{formatNumber(pos.position_profit)}
                          </td>
                          <td
                            style={{
                              padding: 8,
                              borderBottom: "1px solid #f3f4f6",
                              textAlign: "right",
                              color:
                                pos.position_profit > 0
                                  ? "#dc2626"
                                  : pos.position_profit < 0
                                  ? "#16a34a"
                                  : "#374151",
                            }}
                          >
                            {formatNumber(pos.profit_rate * 100, 2)}%
                          </td>
                          <td
                            style={{
                              padding: 8,
                              borderBottom: "1px solid #f3f4f6",
                              textAlign: "right",
                              color:
                                pos.float_profit > 0
                                  ? "#dc2626"
                                  : pos.float_profit < 0
                                  ? "#16a34a"
                                  : "#374151",
                            }}
                          >
                            ¥{formatNumber(pos.float_profit)}
                          </td>
                          <td
                            style={{
                              padding: 8,
                              borderBottom: "1px solid #f3f4f6",
                              textAlign: "right",
                              color:
                                pos.float_profit > 0
                                  ? "#dc2626"
                                  : pos.float_profit < 0
                                  ? "#16a34a"
                                  : "#374151",
                            }}
                          >
                            {formatNumber(dailyPct, 2)}%
                          </td>
                          <td
                            style={{
                              padding: 8,
                              borderBottom: "1px solid #f3f4f6",
                              textAlign: "right",
                            }}
                          >
                            {formatNumber(assetPct, 2)}% / {formatNumber(mvPct, 2)}%
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </>
          )}
          {!loading && positionStats.total === 0 && (
            <p style={{ fontSize: 13, color: "#666" }}>暂无持仓</p>
          )}
        </section>
      )}

      {/* 委托 */}
      {activeTab === "orders" && (
        <section>
          {loading && <p style={{ fontSize: 13, color: "#666" }}>加载中...</p>}
          {!loading && orders.length > 0 && (
            <div
              style={{
                background: "#fff",
                borderRadius: 12,
                padding: 14,
                boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
                overflowX: "auto",
              }}
            >
              <table
                style={{
                  width: "100%",
                  borderCollapse: "collapse",
                  fontSize: 12,
                }}
              >
                <thead>
                  <tr>
                    <th
                      style={{
                        textAlign: "left",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      股票代码
                    </th>
                    <th
                      style={{
                        textAlign: "left",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      委托时间
                    </th>
                    <th
                      style={{
                        textAlign: "center",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      方向
                    </th>
                    <th
                      style={{
                        textAlign: "right",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      委托价
                    </th>
                    <th
                      style={{
                        textAlign: "right",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      委托量
                    </th>
                    <th
                      style={{
                        textAlign: "right",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      成交价
                    </th>
                    <th
                      style={{
                        textAlign: "right",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      成交量
                    </th>
                    <th
                      style={{
                        textAlign: "left",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      状态
                    </th>
                    <th
                      style={{
                        textAlign: "center",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      操作
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {orders.map((order, idx) => (
                    <tr key={idx}>
                      <td style={{ padding: 8, borderBottom: "1px solid #f3f4f6" }}>
                        {order.stock_code} {order.stock_name}
                      </td>
                      <td style={{ padding: 8, borderBottom: "1px solid #f3f4f6" }}>
                        {formatDateTime(order.order_time)}
                      </td>
                      <td
                        style={{
                          padding: 8,
                          borderBottom: "1px solid #f3f4f6",
                          textAlign: "center",
                          color: order.order_type === 23 ? "#dc2626" : "#16a34a",
                        }}
                      >
                        {order.order_type_name}
                      </td>
                      <td
                        style={{
                          padding: 8,
                          borderBottom: "1px solid #f3f4f6",
                          textAlign: "right",
                        }}
                      >
                        ¥{formatNumber(order.price, 3)}
                      </td>
                      <td
                        style={{
                          padding: 8,
                          borderBottom: "1px solid #f3f4f6",
                          textAlign: "right",
                        }}
                      >
                        {order.order_volume}
                      </td>
                      <td
                        style={{
                          padding: 8,
                          borderBottom: "1px solid #f3f4f6",
                          textAlign: "right",
                        }}
                      >
                        {order.traded_price > 0
                          ? `¥${formatNumber(order.traded_price, 3)}`
                          : "-"}
                      </td>
                      <td
                        style={{
                          padding: 8,
                          borderBottom: "1px solid #f3f4f6",
                          textAlign: "right",
                        }}
                      >
                        {order.traded_volume > 0 ? order.traded_volume : "-"}
                      </td>
                      <td style={{ padding: 8, borderBottom: "1px solid #f3f4f6" }}>
                        {order.status_msg || `状态: ${order.order_status}`}
                      </td>
                      <td
                        style={{
                          padding: 8,
                          borderBottom: "1px solid #f3f4f6",
                          textAlign: "center",
                        }}
                      >
                        {/* 可撤委托显示撤单按钮 */}
                        {order.order_status === 50 || order.order_status === 55 ? (
                          <button
                            type="button"
                            onClick={() => handleCancelOrder(order.order_id)}
                            disabled={cancelingOrderId === order.order_id}
                            style={{
                              padding: "4px 8px",
                              borderRadius: 4,
                              border: "1px solid #fecaca",
                              background: "#fef2f2",
                              color: "#b91c1c",
                              cursor: cancelingOrderId === order.order_id ? "default" : "pointer",
                              fontSize: 12,
                            }}
                          >
                            {cancelingOrderId === order.order_id ? "撤单中..." : "撤单"}
                          </button>
                        ) : (
                          "-"
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {!loading && orders.length === 0 && (
            <p style={{ fontSize: 13, color: "#666" }}>暂无委托记录</p>
          )}
        </section>
      )}

      {/* 成交 */}
      {activeTab === "trades" && (
        <section>
          {loading && <p style={{ fontSize: 13, color: "#666" }}>加载中...</p>}
          {!loading && trades.length > 0 && (
            <div
              style={{
                background: "#fff",
                borderRadius: 12,
                padding: 14,
                boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
                overflowX: "auto",
              }}
            >
              <table
                style={{
                  width: "100%",
                  borderCollapse: "collapse",
                  fontSize: 12,
                }}
              >
                <thead>
                  <tr>
                    <th
                      style={{
                        textAlign: "left",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      股票代码
                    </th>
                    <th
                      style={{
                        textAlign: "left",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      成交时间
                    </th>
                    <th
                      style={{
                        textAlign: "center",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      方向
                    </th>
                    <th
                      style={{
                        textAlign: "right",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      成交价
                    </th>
                    <th
                      style={{
                        textAlign: "right",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      成交量
                    </th>
                    <th
                      style={{
                        textAlign: "right",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      成交金额
                    </th>
                    <th
                      style={{
                        textAlign: "right",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      手续费
                    </th>
                    <th
                      style={{
                        textAlign: "left",
                        padding: 8,
                        borderBottom: "1px solid #e5e7eb",
                      }}
                    >
                      委托编号
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {trades.map((trade, idx) => (
                    <tr key={idx}>
                      <td style={{ padding: 8, borderBottom: "1px solid #f3f4f6" }}>
                        {trade.stock_code} {trade.stock_name}
                      </td>
                      <td style={{ padding: 8, borderBottom: "1px solid #f3f4f6" }}>
                        {formatDateTime(trade.traded_time)}
                      </td>
                      <td
                        style={{
                          padding: 8,
                          borderBottom: "1px solid #f3f4f6",
                          textAlign: "center",
                          color: trade.order_type === 23 ? "#dc2626" : "#16a34a",
                        }}
                      >
                        {trade.order_type_name}
                      </td>
                      <td
                        style={{
                          padding: 8,
                          borderBottom: "1px solid #f3f4f6",
                          textAlign: "right",
                        }}
                      >
                        ¥{formatNumber(trade.traded_price, 3)}
                      </td>
                      <td
                        style={{
                          padding: 8,
                          borderBottom: "1px solid #f3f4f6",
                          textAlign: "right",
                        }}
                      >
                        {trade.traded_volume}
                      </td>
                      <td
                        style={{
                          padding: 8,
                          borderBottom: "1px solid #f3f4f6",
                          textAlign: "right",
                        }}
                      >
                        ¥{formatNumber(trade.traded_amount)}
                      </td>
                      <td
                        style={{
                          padding: 8,
                          borderBottom: "1px solid #f3f4f6",
                          textAlign: "right",
                        }}
                      >
                        ¥{formatNumber(trade.commission)}
                      </td>
                      <td style={{ padding: 8, borderBottom: "1px solid #f3f4f6" }}>
                        {trade.order_id}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {!loading && trades.length === 0 && (
            <p style={{ fontSize: 13, color: "#666" }}>暂无成交记录</p>
          )}
        </section>
      )}

      {/* 交易下单 */}
      {activeTab === "trade" && (
        <section>
          <div
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 20,
              boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
            }}
          >
            <h2 style={{ marginTop: 0, fontSize: 18, marginBottom: 16 }}>
              📝 股票下单
            </h2>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
                gap: 16,
                fontSize: 13,
              }}
            >
              <div>
                <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                  股票代码 *
                </label>
                <input
                  type="text"
                  value={orderStockCode}
                  onChange={(e) => setOrderStockCode(e.target.value)}
                  placeholder="例如: 600000 或 000001"
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                    fontSize: 14,
                  }}
                />
              </div>
              <div>
                <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                  买卖方向 *
                </label>
                <select
                  value={orderType}
                  onChange={(e) => setOrderType(Number(e.target.value) as 23 | 24)}
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                    fontSize: 14,
                  }}
                >
                  <option value={23}>买入</option>
                  <option value={24}>卖出</option>
                </select>
              </div>
              <div>
                <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                  委托数量（股）*
                </label>
                <input
                  type="number"
                  value={orderVolume}
                  onChange={(e) => setOrderVolume(Number(e.target.value) || 0)}
                  min={100}
                  step={100}
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                    fontSize: 14,
                  }}
                />
              </div>
              <div>
                <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                  报价类型 *
                </label>
                <select
                  value={priceType}
                  onChange={(e) => setPriceType(e.target.value)}
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                    fontSize: 14,
                  }}
                >
                  <option value="FIX_PRICE">限价</option>
                  <option value="LATEST_PRICE">最新价</option>
                  <option value="MARKET_PEER_PRICE_FIRST">对手方最优</option>
                  <option value="MARKET_MINE_PRICE_FIRST">本方最优</option>
                </select>
              </div>
              {priceType === "FIX_PRICE" && (
                <div>
                  <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                    委托价格 *
                  </label>
                  <input
                    type="number"
                    value={orderPrice}
                    onChange={(e) => setOrderPrice(Number(e.target.value) || 0)}
                    step={0.01}
                    min={0}
                    style={{
                      width: "100%",
                      padding: "8px 12px",
                      borderRadius: 8,
                      border: "1px solid #ddd",
                      fontSize: 14,
                    }}
                  />
                </div>
              )}
              <div>
                <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                  策略名称
                </label>
                <input
                  type="text"
                  value={orderStrategyName}
                  onChange={(e) => setOrderStrategyName(e.target.value)}
                  placeholder="可选"
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                    fontSize: 14,
                  }}
                />
              </div>
              <div style={{ gridColumn: "1 / -1" }}>
                <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                  委托备注
                </label>
                <textarea
                  value={orderRemark}
                  onChange={(e) => setOrderRemark(e.target.value)}
                  placeholder="可选"
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                    fontSize: 14,
                    minHeight: 60,
                  }}
                />
              </div>
            </div>
            <div style={{ marginTop: 16, display: "flex", gap: 12 }}>
              <button
                type="button"
                onClick={handlePlaceOrder}
                disabled={placingOrder || !status?.connected}
                style={{
                  padding: "10px 24px",
                  borderRadius: 8,
                  border: "none",
                  background:
                    orderType === 23
                      ? "linear-gradient(135deg, #dc2626 0%, #ef4444 100%)"
                      : "linear-gradient(135deg, #16a34a 0%, #22c55e 100%)",
                  color: "#fff",
                  cursor: placingOrder || !status?.connected ? "default" : "pointer",
                  fontWeight: 600,
                  fontSize: 14,
                }}
              >
                {placingOrder
                  ? "下单中..."
                  : orderType === 23
                    ? "买入"
                    : "卖出"}
              </button>
              <button
                type="button"
                onClick={() => {
                  setOrderStockCode("");
                  setOrderVolume(100);
                  setOrderPrice(0);
                  setOrderStrategyName("");
                  setOrderRemark("");
                }}
                style={{
                  padding: "10px 24px",
                  borderRadius: 8,
                  border: "1px solid #ddd",
                  background: "#fff",
                  cursor: "pointer",
                  fontSize: 14,
                }}
              >
                清空
              </button>
            </div>
          </div>
        </section>
      )}

      {/* 新股申购 */}
      {activeTab === "ipo" && (
        <section>
          <div
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 20,
              boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
              marginBottom: 16,
            }}
          >
            <h2 style={{ marginTop: 0, fontSize: 18, marginBottom: 16 }}>
              🎯 新股申购额度
            </h2>
            {loadingIpo && <p style={{ fontSize: 13, color: "#666" }}>加载中...</p>}
            {!loadingIpo && ipoLimit && Object.keys(ipoLimit).length > 0 && (
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
                  gap: 12,
                }}
              >
                <div
                  style={{
                    background: "#f3f4f6",
                    borderRadius: 8,
                    padding: 12,
                  }}
                >
                  <div style={{ fontSize: 13, color: "#6b7280" }}>申购额度</div>
                  <div style={{ marginTop: 4, fontSize: 18, fontWeight: 600 }}>
                    ¥{formatNumber(ipoLimit.purchase_limit)}
                  </div>
                </div>
              </div>
            )}
            {!loadingIpo && (!ipoLimit || Object.keys(ipoLimit).length === 0) && (
              <p style={{ fontSize: 13, color: "#666" }}>暂无申购额度信息</p>
            )}
          </div>

          <div
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 20,
              boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
            }}
          >
            <h2 style={{ marginTop: 0, fontSize: 18, marginBottom: 16 }}>
              📋 可申购新股列表
            </h2>
            {loadingIpo && <p style={{ fontSize: 13, color: "#666" }}>加载中...</p>}
            {!loadingIpo && ipoList.length > 0 && (
              <div
                style={{
                  overflowX: "auto",
                }}
              >
                <table
                  style={{
                    width: "100%",
                    borderCollapse: "collapse",
                    fontSize: 12,
                  }}
                >
                  <thead>
                    <tr>
                      <th
                        style={{
                          textAlign: "left",
                          padding: 8,
                          borderBottom: "1px solid #e5e7eb",
                        }}
                      >
                        股票代码
                      </th>
                      <th
                        style={{
                          textAlign: "left",
                          padding: 8,
                          borderBottom: "1px solid #e5e7eb",
                        }}
                      >
                        股票名称
                      </th>
                      <th
                        style={{
                          textAlign: "right",
                          padding: 8,
                          borderBottom: "1px solid #e5e7eb",
                        }}
                      >
                        发行价
                      </th>
                      <th
                        style={{
                          textAlign: "right",
                          padding: 8,
                          borderBottom: "1px solid #e5e7eb",
                        }}
                      >
                        申购上限
                      </th>
                      <th
                        style={{
                          textAlign: "left",
                          padding: 8,
                          borderBottom: "1px solid #e5e7eb",
                        }}
                      >
                        申购日期
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {ipoList.map((ipo, idx) => (
                      <tr key={idx}>
                        <td style={{ padding: 8, borderBottom: "1px solid #f3f4f6" }}>
                          {ipo.stock_code}
                        </td>
                        <td style={{ padding: 8, borderBottom: "1px solid #f3f4f6" }}>
                          {ipo.stock_name}
                        </td>
                        <td
                          style={{
                            padding: 8,
                            borderBottom: "1px solid #f3f4f6",
                            textAlign: "right",
                          }}
                        >
                          ¥{formatNumber(ipo.issue_price)}
                        </td>
                        <td
                          style={{
                            padding: 8,
                            borderBottom: "1px solid #f3f4f6",
                            textAlign: "right",
                          }}
                        >
                          {ipo.purchase_limit > 0
                            ? formatNumber(ipo.purchase_limit)
                            : "-"}
                        </td>
                        <td style={{ padding: 8, borderBottom: "1px solid #f3f4f6" }}>
                          {ipo.purchase_date || "-"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            {!loadingIpo && ipoList.length === 0 && (
              <p style={{ fontSize: 13, color: "#666" }}>暂无可申购新股</p>
            )}
          </div>
        </section>
      )}

      {/* 银证转账 */}
      {activeTab === "bank" && (
        <section>
          <div
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 20,
              boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
            }}
          >
            <h2 style={{ marginTop: 0, fontSize: 18, marginBottom: 16 }}>
              💳 银证转账
            </h2>
            <p style={{ fontSize: 13, color: "#666", marginBottom: 16 }}>
              银行与证券账户之间的资金划转
            </p>

            {/* 银行信息 */}
            {bankInfo.length > 0 && (
              <div style={{ marginBottom: 16 }}>
                <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 8 }}>
                  已绑定银行
                </div>
                {bankInfo.map((bank, idx) => (
                  <div
                    key={idx}
                    style={{
                      padding: 12,
                      background: "#f3f4f6",
                      borderRadius: 8,
                      marginBottom: 8,
                      fontSize: 13,
                    }}
                  >
                    <div>
                      {bank.bank_name} ({bank.bank_no})
                    </div>
                    <div style={{ color: "#6b7280", marginTop: 4 }}>
                      账号: {bank.bank_account}
                    </div>
                  </div>
                ))}
              </div>
            )}

            {/* 转账表单 */}
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
                gap: 16,
                fontSize: 13,
              }}
            >
              <div>
                <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                  转账方向 *
                </label>
                <select
                  value={transferDirection}
                  onChange={(e) => setTransferDirection(e.target.value as "in" | "out")}
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                    fontSize: 14,
                  }}
                >
                  <option value="in">银行转证券</option>
                  <option value="out">证券转银行</option>
                </select>
              </div>
              {bankInfo.length > 0 && (
                <div>
                  <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                    银行编号 *
                  </label>
                  <select
                    value={transferBankNo}
                    onChange={(e) => setTransferBankNo(e.target.value)}
                    style={{
                      width: "100%",
                      padding: "8px 12px",
                      borderRadius: 8,
                      border: "1px solid #ddd",
                      fontSize: 14,
                    }}
                  >
                    {bankInfo.map((bank, idx) => (
                      <option key={idx} value={bank.bank_no}>
                        {bank.bank_name} ({bank.bank_no})
                      </option>
                    ))}
                  </select>
                </div>
              )}
              {bankInfo.length === 0 && (
                <div>
                  <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                    银行编号 *
                  </label>
                  <input
                    type="text"
                    value={transferBankNo}
                    onChange={(e) => setTransferBankNo(e.target.value)}
                    placeholder="请输入银行编号"
                    style={{
                      width: "100%",
                      padding: "8px 12px",
                      borderRadius: 8,
                      border: "1px solid #ddd",
                      fontSize: 14,
                    }}
                  />
                </div>
              )}
              <div>
                <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                  银行账号 *
                </label>
                <input
                  type="text"
                  value={transferBankAccount}
                  onChange={(e) => setTransferBankAccount(e.target.value)}
                  placeholder="请输入银行账号"
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                    fontSize: 14,
                  }}
                />
              </div>
              <div>
                <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                  银行密码 *
                </label>
                <input
                  type="password"
                  value={transferBankPwd}
                  onChange={(e) => setTransferBankPwd(e.target.value)}
                  placeholder="请输入银行密码"
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                    fontSize: 14,
                  }}
                />
              </div>
              <div>
                <label style={{ display: "block", marginBottom: 4, fontWeight: 600 }}>
                  转账金额 *
                </label>
                <input
                  type="number"
                  value={transferAmount}
                  onChange={(e) => setTransferAmount(Number(e.target.value) || 0)}
                  step={0.01}
                  min={0}
                  placeholder="请输入转账金额"
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                    fontSize: 14,
                  }}
                />
              </div>
            </div>
            <div style={{ marginTop: 16 }}>
              <button
                type="button"
                onClick={handleBankTransfer}
                disabled={transferring || !status?.connected}
                style={{
                  padding: "10px 24px",
                  borderRadius: 8,
                  border: "none",
                  background: "linear-gradient(135deg, #6366f1 0%, #4f46e5 100%)",
                  color: "#fff",
                  cursor: transferring || !status?.connected ? "default" : "pointer",
                  fontWeight: 600,
                  fontSize: 14,
                }}
              >
                {transferring
                  ? "转账中..."
                  : transferDirection === "in"
                    ? "银行转证券"
                    : "证券转银行"}
              </button>
            </div>
          </div>
        </section>
      )}

      {!status?.connected && (
        <section>
          <div
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 24,
              boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
              textAlign: "center",
            }}
          >
            <p style={{ fontSize: 14, color: "#666" }}>
              请先连接 QMT 以查看持仓和交易信息
            </p>
          </div>
        </section>
      )}
    </main>
  );
}

