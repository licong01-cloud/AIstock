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

interface Position {
  stock_code: string;
  stock_name: string;
  quantity: number;
  can_sell: number;
  open_price: number;
  cost_price: number;
  current_price: number;
  market_value: number;
  float_profit: number;
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

export default function QMTPositionsPage() {
  const [activeTab, setActiveTab] = useState<
    "positions" | "orders" | "trades" | "account" | "trade" | "ipo" | "bank"
  >("positions");

  const [status, setStatus] = useState<QMTStatus | null>(null);
  const [account, setAccount] = useState<AccountInfo | null>(null);
  const [positions, setPositions] = useState<Position[]>([]);
  const [orders, setOrders] = useState<Order[]>([]);
  const [trades, setTrades] = useState<Trade[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [connecting, setConnecting] = useState(false);
  const [autoRefreshEnabled, setAutoRefreshEnabled] = useState(true);
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
    totalFloatProfit: positions.reduce((sum, p) => sum + p.float_profit, 0),
  };

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
        }),
      });
      const data = await res.json();
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

    setCancelingOrderId(orderId);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/qmt/cancel`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ order_id: orderId }),
      });
      const data = await res.json();
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
        }),
      });
      const data = await res.json();
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

      {/* 标签页 */}
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
        </div>
      </section>

      {/* 账户资金 */}
      {activeTab === "account" && (
        <section>
          {loading && <p style={{ fontSize: 13, color: "#666" }}>加载中...</p>}
          {account && (
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
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
                  gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
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
                  <div style={{ fontSize: 13, color: "#6b7280" }}>浮动盈亏</div>
                  <div
                    style={{
                      marginTop: 4,
                      fontSize: 18,
                      color:
                        positionStats.totalFloatProfit >= 0 ? "#16a34a" : "#dc2626",
                    }}
                  >
                    ¥{formatNumber(positionStats.totalFloatProfit)}
                  </div>
                </div>
              </div>

              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {positions.map((pos, idx) => (
                  <div
                    key={idx}
                    style={{
                      background: "#fff",
                      borderRadius: 12,
                      padding: 14,
                      boxShadow: "0 2px 6px rgba(0,0,0,0.05)",
                    }}
                  >
                    <div
                      style={{
                        display: "grid",
                        gridTemplateColumns: "2fr 1fr 1fr 1fr 1fr 1fr",
                        gap: 12,
                        fontSize: 13,
                      }}
                    >
                      <div>
                        <div style={{ fontWeight: 600 }}>
                          {pos.stock_code} {pos.stock_name}
                        </div>
                        <div style={{ marginTop: 2, color: "#6b7280", fontSize: 12 }}>
                          股东账户: {pos.secu_account}
                        </div>
                      </div>
                      <div>
                        <div style={{ color: "#6b7280" }}>持仓数量</div>
                        <div style={{ marginTop: 2 }}>{pos.quantity} 股</div>
                        <div style={{ marginTop: 2, fontSize: 12, color: "#6b7280" }}>
                          可卖: {pos.can_sell} 股
                        </div>
                      </div>
                      <div>
                        <div style={{ color: "#6b7280" }}>成本价</div>
                        <div style={{ marginTop: 2 }}>
                          ¥{formatNumber(pos.cost_price, 3)}
                        </div>
                      </div>
                      <div>
                        <div style={{ color: "#6b7280" }}>当前价</div>
                        <div style={{ marginTop: 2 }}>
                          ¥{formatNumber(pos.current_price, 3)}
                        </div>
                      </div>
                      <div>
                        <div style={{ color: "#6b7280" }}>市值</div>
                        <div style={{ marginTop: 2 }}>
                          ¥{formatNumber(pos.market_value)}
                        </div>
                      </div>
                      <div>
                        <div style={{ color: "#6b7280" }}>浮动盈亏</div>
                        <div
                          style={{
                            marginTop: 2,
                            color: pos.float_profit >= 0 ? "#16a34a" : "#dc2626",
                            fontWeight: 600,
                          }}
                        >
                          ¥{formatNumber(pos.float_profit)} (
                          {formatNumber(pos.profit_rate * 100, 2)}%)
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
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

