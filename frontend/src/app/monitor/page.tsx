"use client";

import { useEffect, useMemo, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface MonitorStock {
  id: number;
  symbol: string;
  name: string;
  rating: string;
  entry_range?: { min?: number; max?: number } | null;
  take_profit?: number | null;
  stop_loss?: number | null;
  current_price?: number | null | string;
  last_checked?: string | null;
  check_interval: number;
  notification_enabled: boolean;
  quant_enabled?: boolean;
  quant_config?: Record<string, any> | null;
}

interface MonitorSummary {
  total_stocks: number;
  stocks_needing_update: number;
  pending_notifications: number;
  active_monitoring: boolean;
}

interface MonitorNotification {
  id: number;
  stock_id?: number | null;
  symbol: string;
  name: string;
  type: string;
  message: string;
  triggered_at?: string | null;
  sent?: boolean;
}

interface EmailConfigStatus {
  enabled: boolean;
  smtp_server: string;
  smtp_port: number;
  email_from: string;
  email_to: string;
  configured: boolean;
}

interface SchedulerStatus {
  scheduler_running: boolean;
  scheduler_enabled: boolean;
  is_trading_day: boolean;
  is_trading_time: boolean;
  market: string;
  next_trading_time: string;
  monitor_service_running: boolean;
  auto_stop: boolean;
}

interface SchedulerConfigPayload {
  enabled: boolean;
  market: string;
  trading_days: number[];
  auto_stop: boolean;
  pre_market_minutes: number;
  post_market_minutes: number;
}

interface MiniQmtStatus {
  enabled: boolean;
  connected: boolean;
  ready?: boolean;
  account_id?: string | null;
}

function formatDateTime(tt?: string | null) {
  if (!tt) return "-";
  try {
    const d = new Date(tt);
    if (Number.isNaN(d.getTime())) return tt;
    return `${d.getMonth() + 1}-${d
      .getDate()
      .toString()
      .padStart(2, "0")} ${d
      .getHours()
      .toString()
      .padStart(2, "0")}:${d
      .getMinutes()
      .toString()
      .padStart(2, "0")}`;
  } catch {
    return tt;
  }
}

export default function MonitorPage() {
  const [summary, setSummary] = useState<MonitorSummary | null>(null);
  const [loadingSummary, setLoadingSummary] = useState(false);

  const [stocks, setStocks] = useState<MonitorStock[]>([]);
  const [loadingStocks, setLoadingStocks] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [searchTerm, setSearchTerm] = useState("");
  const [ratingFilter, setRatingFilter] = useState<string>("全部");

  const [showAddPanel, setShowAddPanel] = useState(false);

  const [addSymbol, setAddSymbol] = useState("");
  const [addName, setAddName] = useState("");
  const [addEntryMin, setAddEntryMin] = useState(0);
  const [addEntryMax, setAddEntryMax] = useState(0);
  const [addTakeProfit, setAddTakeProfit] = useState(0);
  const [addStopLoss, setAddStopLoss] = useState(0);
  const [addCheckInterval, setAddCheckInterval] = useState(30);
  const [addNotificationEnabled, setAddNotificationEnabled] = useState(true);
  const [addRating, setAddRating] = useState("买入");
  const [addQuantEnabled, setAddQuantEnabled] = useState(false);
  const [addQuantMaxPos, setAddQuantMaxPos] = useState(0.2);
  const [addQuantAutoSL, setAddQuantAutoSL] = useState(true);
  const [addQuantAutoTP, setAddQuantAutoTP] = useState(true);
  const [addingStock, setAddingStock] = useState(false);

  const [editingId, setEditingId] = useState<number | null>(null);
  const [editEntryMin, setEditEntryMin] = useState(0);
  const [editEntryMax, setEditEntryMax] = useState(0);
  const [editTakeProfit, setEditTakeProfit] = useState(0);
  const [editStopLoss, setEditStopLoss] = useState(0);
  const [editCheckInterval, setEditCheckInterval] = useState(30);
  const [editRating, setEditRating] = useState("买入");
  const [editNotificationEnabled, setEditNotificationEnabled] = useState(true);
  const [editQuantEnabled, setEditQuantEnabled] = useState(false);
  const [editQuantMaxPos, setEditQuantMaxPos] = useState(0.2);
  const [editQuantAutoSL, setEditQuantAutoSL] = useState(true);
  const [editQuantAutoTP, setEditQuantAutoTP] = useState(true);
  const [savingEdit, setSavingEdit] = useState(false);

  const [notifications, setNotifications] = useState<MonitorNotification[]>([]);
  const [emailConfig, setEmailConfig] = useState<EmailConfigStatus | null>(null);
  const [loadingEmailConfig, setLoadingEmailConfig] = useState(false);
  const [sendingTestEmail, setSendingTestEmail] = useState(false);

  const [scheduler, setScheduler] = useState<SchedulerStatus | null>(null);
  const [loadingScheduler, setLoadingScheduler] = useState(false);
  const [savingScheduler, setSavingScheduler] = useState(false);
  const [schedMarket, setSchedMarket] = useState("CN");
  const [schedTradingDays, setSchedTradingDays] = useState<number[]>([1, 2, 3, 4, 5]);
  const [schedEnabled, setSchedEnabled] = useState(false);
  const [schedAutoStop, setSchedAutoStop] = useState(true);
  const [schedPreMinutes, setSchedPreMinutes] = useState(5);
  const [schedPostMinutes, setSchedPostMinutes] = useState(5);

  const [miniQmt, setMiniQmt] = useState<MiniQmtStatus | null>(null);
  const [loadingMiniQmt, setLoadingMiniQmt] = useState(false);
  const [operatingMiniQmt, setOperatingMiniQmt] = useState(false);

  const filteredStocks = useMemo(() => {
    let list = stocks;
    if (searchTerm.trim()) {
      const q = searchTerm.trim().toLowerCase();
      list = list.filter(
        (s) =>
          s.symbol.toLowerCase().includes(q) ||
          (s.name || "").toLowerCase().includes(q),
      );
    }
    if (ratingFilter !== "全部") {
      list = list.filter((s) => s.rating === ratingFilter);
    }
    return list;
  }, [stocks, searchTerm, ratingFilter]);

  const quantStocks = useMemo(
    () => stocks.filter((s) => s.quant_enabled),
    [stocks],
  );

  async function loadSummary() {
    setLoadingSummary(true);
    try {
      const res = await fetch(`${API_BASE}/monitor/summary`);
      if (res.ok) {
        const data: MonitorSummary = await res.json();
        setSummary(data);
      }
    } catch {
      // ignore
    } finally {
      setLoadingSummary(false);
    }
  }

  async function loadStocks() {
    setLoadingStocks(true);
    try {
      const res = await fetch(`${API_BASE}/monitor/stocks`);
      if (!res.ok) throw new Error(`监测列表请求失败: ${res.status}`);
      const data: MonitorStock[] = await res.json();
      setStocks(data || []);
    } catch (e: any) {
      setError(e?.message || "加载监测列表失败");
      setStocks([]);
    } finally {
      setLoadingStocks(false);
    }
  }

  async function loadNotifications() {
    try {
      const res = await fetch(
        `${API_BASE}/monitor/notifications/recent?limit=10`,
      );
      if (res.ok) {
        const data: MonitorNotification[] = await res.json();
        setNotifications(data || []);
      }
    } catch {
      // ignore
    }
  }

  async function loadEmailConfig() {
    setLoadingEmailConfig(true);
    try {
      const res = await fetch(
        `${API_BASE}/monitor/notifications/email-config-status`,
      );
      if (res.ok) {
        const data: EmailConfigStatus = await res.json();
        setEmailConfig(data);
      }
    } catch {
      // ignore
    } finally {
      setLoadingEmailConfig(false);
    }
  }

  async function loadScheduler() {
    setLoadingScheduler(true);
    try {
      const res = await fetch(`${API_BASE}/monitor/scheduler/status`);
      if (res.ok) {
        const data: SchedulerStatus = await res.json();
        setScheduler(data);
        setSchedEnabled(data.scheduler_enabled);
        setSchedAutoStop(data.auto_stop);
        setSchedMarket(data.market || "CN");
        // 默认交易日：如果服务端没有显式给出，用 1-5
        setSchedTradingDays((prev) => prev.length ? prev : [1, 2, 3, 4, 5]);
      }
    } catch {
      // ignore
    } finally {
      setLoadingScheduler(false);
    }
  }

  async function loadMiniQmt() {
    setLoadingMiniQmt(true);
    try {
      const res = await fetch(`${API_BASE}/monitor/miniqmt/status`);
      if (res.ok) {
        const data: MiniQmtStatus = await res.json();
        setMiniQmt(data);
      }
    } catch {
      // ignore
    } finally {
      setLoadingMiniQmt(false);
    }
  }

  useEffect(() => {
    loadSummary();
    loadStocks();
    loadNotifications();
    loadEmailConfig();
    loadScheduler();
    loadMiniQmt();
  }, []);

  async function handleServiceStart() {
    try {
      await fetch(`${API_BASE}/monitor/service/start`, { method: "POST" });
      await loadSummary();
      await loadScheduler();
    } catch {
      // ignore
    }
  }

  async function handleServiceStop() {
    try {
      await fetch(`${API_BASE}/monitor/service/stop`, { method: "POST" });
      await loadSummary();
      await loadScheduler();
    } catch {
      // ignore
    }
  }

  async function handleManualUpdateAll() {
    try {
      const res = await fetch(
        `${API_BASE}/monitor/service/manual-update-all`,
        { method: "POST" },
      );
      if (!res.ok) throw new Error(String(res.status));
      await loadStocks();
      await loadSummary();
    } catch (e: any) {
      setError(e?.message || "手动更新失败");
    }
  }

  async function handleAddStock() {
    const min = Number(addEntryMin);
    const max = Number(addEntryMax);
    if (!addSymbol.trim() || !(min > 0 && max > 0 && max > min)) {
      setError("请填写完整的股票代码和有效的进场区间");
      return;
    }
    setAddingStock(true);
    setError(null);
    try {
      const payload: any = {
        symbol: addSymbol.trim(),
        name: addName.trim() || null,
        rating: addRating,
        entry_min: min,
        entry_max: max,
        take_profit: addTakeProfit > 0 ? Number(addTakeProfit) : null,
        stop_loss: addStopLoss > 0 ? Number(addStopLoss) : null,
        check_interval: Number(addCheckInterval) || 30,
        notification_enabled: addNotificationEnabled,
        quant_enabled: addQuantEnabled,
        quant_config: undefined,
      };
      if (addQuantEnabled) {
        payload.quant_config = {
          max_position_pct: addQuantMaxPos,
          auto_stop_loss: addQuantAutoSL,
          auto_take_profit: addQuantAutoTP,
          min_trade_amount: 5000,
        };
      }
      const res = await fetch(`${API_BASE}/monitor/stocks`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) throw new Error(`添加失败: ${res.status}`);
      setAddSymbol("");
      setAddName("");
      setAddEntryMin(0);
      setAddEntryMax(0);
      setAddTakeProfit(0);
      setAddStopLoss(0);
      setAddCheckInterval(30);
      setAddNotificationEnabled(true);
      setAddRating("买入");
      setAddQuantEnabled(false);
      await loadStocks();
      await loadSummary();
    } catch (e: any) {
      setError(e?.message || "添加监测失败");
    } finally {
      setAddingStock(false);
    }
  }

  function openEdit(stock: MonitorStock) {
    const entry = stock.entry_range || {};
    setEditingId(stock.id);
    setEditEntryMin(Number(entry.min || 0));
    setEditEntryMax(Number(entry.max || 0));
    setEditTakeProfit(Number(stock.take_profit || 0));
    setEditStopLoss(Number(stock.stop_loss || 0));
    setEditCheckInterval(Number(stock.check_interval || 30));
    setEditRating(stock.rating || "买入");
    setEditNotificationEnabled(!!stock.notification_enabled);
    setEditQuantEnabled(!!stock.quant_enabled);
    const qc = stock.quant_config || {};
    setEditQuantMaxPos(Number((qc as any).max_position_pct ?? 0.2));
    setEditQuantAutoSL(Boolean((qc as any).auto_stop_loss ?? true));
    setEditQuantAutoTP(Boolean((qc as any).auto_take_profit ?? true));
  }

  async function handleSaveEdit() {
    if (editingId == null) return;
    const min = Number(editEntryMin);
    const max = Number(editEntryMax);
    if (!(min > 0 && max > 0 && max > min)) {
      setError("请输入有效的进场区间");
      return;
    }
    setSavingEdit(true);
    setError(null);
    try {
      const payload: any = {
        rating: editRating,
        entry_min: min,
        entry_max: max,
        take_profit: editTakeProfit > 0 ? Number(editTakeProfit) : null,
        stop_loss: editStopLoss > 0 ? Number(editStopLoss) : null,
        check_interval: Number(editCheckInterval) || 30,
        notification_enabled: editNotificationEnabled,
        quant_enabled: editQuantEnabled,
        quant_config: undefined,
      };
      if (editQuantEnabled) {
        payload.quant_config = {
          max_position_pct: editQuantMaxPos,
          auto_stop_loss: editQuantAutoSL,
          auto_take_profit: editQuantAutoTP,
          min_trade_amount: 5000,
        };
      }
      const res = await fetch(`${API_BASE}/monitor/stocks/${editingId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) throw new Error(`保存失败: ${res.status}`);
      setEditingId(null);
      await loadStocks();
    } catch (e: any) {
      setError(e?.message || "保存修改失败");
    } finally {
      setSavingEdit(false);
    }
  }

  async function handleDeleteStock(id: number) {
    const yes =
      typeof window === "undefined" ||
      window.confirm("确认删除该监测项？此操作不可恢复。");
    if (!yes) return;
    try {
      const res = await fetch(`${API_BASE}/monitor/stocks/${id}`, {
        method: "DELETE",
      });
      if (!res.ok) throw new Error(`删除失败: ${res.status}`);
      await loadStocks();
      await loadSummary();
    } catch (e: any) {
      setError(e?.message || "删除监测失败");
    }
  }

  async function handleManualUpdate(id: number) {
    try {
      const res = await fetch(
        `${API_BASE}/monitor/stocks/${id}/manual-update`,
        { method: "POST" },
      );
      if (!res.ok) throw new Error(`更新失败: ${res.status}`);
      await loadStocks();
      await loadSummary();
    } catch (e: any) {
      setError(e?.message || "更新失败");
    }
  }

  async function handleToggleNotification(id: number, enabled: boolean) {
    try {
      const res = await fetch(
        `${API_BASE}/monitor/stocks/${id}/notification`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ enabled }),
        },
      );
      if (!res.ok) throw new Error(`切换通知失败: ${res.status}`);
      await loadStocks();
    } catch (e: any) {
      setError(e?.message || "切换通知失败");
    }
  }

  async function handleMarkAllNotificationsSent() {
    try {
      await fetch(`${API_BASE}/monitor/notifications/mark-all-sent`, {
        method: "POST",
      });
      await loadNotifications();
    } catch {
      // ignore
    }
  }

  async function handleClearNotifications() {
    const yes =
      typeof window === "undefined" ||
      window.confirm("确认清空所有通知记录？");
    if (!yes) return;
    try {
      await fetch(`${API_BASE}/monitor/notifications/clear`, {
        method: "POST",
      });
      await loadNotifications();
    } catch {
      // ignore
    }
  }

  async function handleSendTestEmail() {
    setSendingTestEmail(true);
    try {
      const res = await fetch(
        `${API_BASE}/monitor/notifications/send-test-email`,
        { method: "POST" },
      );
      const data = await res.json();
      if (!res.ok || !data.success) {
        const msg = data?.message || `测试邮件发送失败: ${res.status}`;
        setError(String(msg));
      }
    } catch (e: any) {
      setError(e?.message || "测试邮件发送失败");
    } finally {
      setSendingTestEmail(false);
    }
  }

  async function handleSaveScheduler() {
    setSavingScheduler(true);
    try {
      const payload: SchedulerConfigPayload = {
        enabled: schedEnabled,
        market: schedMarket,
        trading_days: schedTradingDays,
        auto_stop: schedAutoStop,
        pre_market_minutes: schedPreMinutes,
        post_market_minutes: schedPostMinutes,
      };
      const res = await fetch(`${API_BASE}/monitor/scheduler/config`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) throw new Error(`保存调度配置失败: ${res.status}`);
      await loadScheduler();
    } catch (e: any) {
      setError(e?.message || "保存调度配置失败");
    } finally {
      setSavingScheduler(false);
    }
  }

  async function handleStartScheduler() {
    try {
      await fetch(`${API_BASE}/monitor/scheduler/start`, { method: "POST" });
      await loadScheduler();
    } catch {
      // ignore
    }
  }

  async function handleStopScheduler() {
    try {
      await fetch(`${API_BASE}/monitor/scheduler/stop`, { method: "POST" });
      await loadScheduler();
    } catch {
      // ignore
    }
  }

  async function handleMiniQmtConnect() {
    setOperatingMiniQmt(true);
    try {
      await fetch(`${API_BASE}/monitor/miniqmt/connect`, { method: "POST" });
      await loadMiniQmt();
    } catch {
      // ignore
    } finally {
      setOperatingMiniQmt(false);
    }
  }

  async function handleMiniQmtDisconnect() {
    setOperatingMiniQmt(true);
    try {
      await fetch(`${API_BASE}/monitor/miniqmt/disconnect`, { method: "POST" });
      await loadMiniQmt();
    } catch {
      // ignore
    } finally {
      setOperatingMiniQmt(false);
    }
  }

  const tradingDayOptions = [
    { value: 1, label: "周一" },
    { value: 2, label: "周二" },
    { value: 3, label: "周三" },
    { value: 4, label: "周四" },
    { value: 5, label: "周五" },
    { value: 6, label: "周六" },
    { value: 7, label: "周日" },
  ];

  function toggleTradingDay(day: number) {
    setSchedTradingDays((prev) => {
      const set = new Set(prev);
      if (set.has(day)) set.delete(day);
      else set.add(day);
      return Array.from(set).sort((a, b) => a - b);
    });
  }

  return (
    <main style={{ padding: 24 }}>
      <section style={{ marginBottom: 16 }}>
        <h1 style={{ margin: 0, fontSize: 22 }}>📊 股票监测中心</h1>
        <p style={{ marginTop: 4, fontSize: 13, color: "#666" }}>
          完整复刻旧版监测管理：监测服务控制、监测列表、关键价位、通知与定时调度。
        </p>
      </section>

      {/* 顶部状态与服务控制 */}
      <section
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(6, minmax(0, 1fr))",
          gap: 12,
          marginBottom: 16,
          fontSize: 13,
        }}
      >
        <div
          style={{
            background: "#fff",
            borderRadius: 10,
            padding: 10,
            boxShadow: "0 2px 8px rgba(0,0,0,0.06)",
          }}
        >
          <div style={{ fontWeight: 600 }}>监测服务</div>
          <div style={{ marginTop: 6 }}>
            {summary?.active_monitoring ? (
              <span style={{ color: "#16a34a" }}>🟢 运行中</span>
            ) : (
              <span style={{ color: "#dc2626" }}>🔴 已停止</span>
            )}
          </div>
        </div>

        <div
          style={{
            background: "#fff",
            borderRadius: 10,
            padding: 10,
            boxShadow: "0 2px 8px rgba(0,0,0,0.06)",
          }}
        >
          <div style={{ fontWeight: 600 }}>监测股票</div>
          <div style={{ marginTop: 6, fontSize: 18 }}>
            {summary?.total_stocks ?? "-"}
          </div>
        </div>

        <div
          style={{
            background: "#fff",
            borderRadius: 10,
            padding: 10,
            boxShadow: "0 2px 8px rgba(0,0,0,0.06)",
          }}
        >
          <div style={{ fontWeight: 600 }}>待处理通知</div>
          <div style={{ marginTop: 6, fontSize: 18 }}>
            {summary?.pending_notifications ?? "-"}
          </div>
        </div>

        <div
          style={{
            background: "#fff",
            borderRadius: 10,
            padding: 10,
            boxShadow: "0 2px 8px rgba(0,0,0,0.06)",
          }}
        >
          <div style={{ fontWeight: 600 }}>MiniQMT</div>
          <div style={{ marginTop: 6 }}>
            {miniQmt?.enabled ? (
              <span style={{ color: miniQmt.connected ? "#16a34a" : "#ea580c" }}>
                {miniQmt.connected ? "✅ 已连接" : "⏸️ 未连接"}
              </span>
            ) : (
              <span style={{ color: "#999" }}>未启用</span>
            )}
          </div>
          {miniQmt?.account_id && (
            <div style={{ marginTop: 4, fontSize: 12, color: "#555" }}>
              账户: {miniQmt.account_id}
            </div>
          )}
        </div>

        <div
          style={{
            display: "flex",
            flexDirection: "column",
            gap: 6,
            justifyContent: "center",
          }}
        >
          <button
            type="button"
            onClick={handleServiceStart}
            style={{
              padding: "6px 10px",
              borderRadius: 8,
              border: "none",
              background: "#16a34a",
              color: "#fff",
              cursor: "pointer",
              fontSize: 13,
            }}
          >
            ▶️ 启动监测
          </button>
          <button
            type="button"
            onClick={handleServiceStop}
            style={{
              padding: "6px 10px",
              borderRadius: 8,
              border: "none",
              background: "#ef4444",
              color: "#fff",
              cursor: "pointer",
              fontSize: 13,
            }}
          >
            ⏹️ 停止监测
          </button>
        </div>

        <div
          style={{
            display: "flex",
            flexDirection: "column",
            gap: 6,
            justifyContent: "center",
          }}
        >
          <button
            type="button"
            onClick={handleManualUpdateAll}
            style={{
              padding: "6px 10px",
              borderRadius: 8,
              border: "none",
              background: "#0ea5e9",
              color: "#fff",
              cursor: "pointer",
              fontSize: 13,
            }}
          >
            🔄 手动更新需要更新的股票
          </button>
          <button
            type="button"
            onClick={() => {
              loadSummary();
              loadStocks();
              loadNotifications();
            }}
            style={{
              padding: "6px 10px",
              borderRadius: 8,
              border: "1px solid #d4d4d4",
              background: "#fff",
              color: "#333",
              cursor: "pointer",
              fontSize: 13,
            }}
          >
            🔁 刷新状态
          </button>
        </div>
      </section>

      {/* 添加监测折叠区 */}
      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 14,
          boxShadow: "0 2px 10px rgba(0,0,0,0.06)",
          marginBottom: 18,
        }}
      >
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            cursor: "pointer",
          }}
          onClick={() => setShowAddPanel((v) => !v)}
        >
          <h2 style={{ margin: 0, fontSize: 18 }}>➕ 添加股票监测</h2>
          <span style={{ fontSize: 13, color: "#555" }}>
            {showAddPanel ? "收起" : "展开"}
          </span>
        </div>
        {showAddPanel && (
          <div style={{ marginTop: 12 }}>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "1.2fr 1.3fr",
                gap: 16,
              }}
            >
              <div>
                <h3 style={{ fontSize: 14, marginTop: 0 }}>📈 股票信息</h3>
                <div style={{ marginBottom: 8 }}>
                  <label style={{ fontSize: 13 }}>股票代码</label>
                  <input
                    value={addSymbol}
                    onChange={(e) => setAddSymbol(e.target.value)}
                    placeholder="例如: AAPL, 000001"
                    style={{
                      marginTop: 4,
                      width: "100%",
                      padding: "6px 8px",
                      borderRadius: 8,
                      border: "1px solid #ddd",
                    }}
                  />
                </div>
                <div style={{ marginBottom: 8 }}>
                  <label style={{ fontSize: 13 }}>股票名称（可选）</label>
                  <input
                    value={addName}
                    onChange={(e) => setAddName(e.target.value)}
                    placeholder="例如: 苹果公司"
                    style={{
                      marginTop: 4,
                      width: "100%",
                      padding: "6px 8px",
                      borderRadius: 8,
                      border: "1px solid #ddd",
                    }}
                  />
                </div>
              </div>

              <div>
                <h3 style={{ fontSize: 14, marginTop: 0 }}>⚙️ 监测设置</h3>
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
                    gap: 8,
                    marginBottom: 8,
                  }}
                >
                  <div>
                    <label style={{ fontSize: 13 }}>进场区间最低价</label>
                    <input
                      type="number"
                      value={addEntryMin}
                      onChange={(e) => setAddEntryMin(Number(e.target.value))}
                      style={{
                        marginTop: 4,
                        width: "100%",
                        padding: "6px 8px",
                        borderRadius: 8,
                        border: "1px solid #ddd",
                      }}
                      step="0.01"
                    />
                  </div>
                  <div>
                    <label style={{ fontSize: 13 }}>进场区间最高价</label>
                    <input
                      type="number"
                      value={addEntryMax}
                      onChange={(e) => setAddEntryMax(Number(e.target.value))}
                      style={{
                        marginTop: 4,
                        width: "100%",
                        padding: "6px 8px",
                        borderRadius: 8,
                        border: "1px solid #ddd",
                      }}
                      step="0.01"
                    />
                  </div>
                  <div>
                    <label style={{ fontSize: 13 }}>止盈价位（可选）</label>
                    <input
                      type="number"
                      value={addTakeProfit}
                      onChange={(e) =>
                        setAddTakeProfit(Number(e.target.value))
                      }
                      style={{
                        marginTop: 4,
                        width: "100%",
                        padding: "6px 8px",
                        borderRadius: 8,
                        border: "1px solid #ddd",
                      }}
                      step="0.01"
                    />
                  </div>
                  <div>
                    <label style={{ fontSize: 13 }}>止损价位（可选）</label>
                    <input
                      type="number"
                      value={addStopLoss}
                      onChange={(e) => setAddStopLoss(Number(e.target.value))}
                      style={{
                        marginTop: 4,
                        width: "100%",
                        padding: "6px 8px",
                        borderRadius: 8,
                        border: "1px solid #ddd",
                      }}
                      step="0.01"
                    />
                  </div>
                </div>

                <div style={{ marginBottom: 8 }}>
                  <label style={{ fontSize: 13 }}>监测间隔（分钟）</label>
                  <input
                    type="number"
                    value={addCheckInterval}
                    onChange={(e) =>
                      setAddCheckInterval(Number(e.target.value) || 30)
                    }
                    min={5}
                    max={120}
                    style={{
                      marginTop: 4,
                      width: "100%",
                      padding: "6px 8px",
                      borderRadius: 8,
                      border: "1px solid #ddd",
                    }}
                    title="监测该股票的价格检查间隔（分钟）"
                  />
                </div>

                <div
                  style={{
                    display: "flex",
                    flexWrap: "wrap",
                    gap: 10,
                    alignItems: "center",
                    marginBottom: 8,
                  }}
                >
                  <label style={{ fontSize: 13 }}>
                    <input
                      type="checkbox"
                      checked={addNotificationEnabled}
                      onChange={(e) =>
                        setAddNotificationEnabled(e.target.checked)
                      }
                    />
                    &nbsp;启用通知
                  </label>
                  <label style={{ fontSize: 13 }}>
                    投资评级：
                    <select
                      value={addRating}
                      onChange={(e) => setAddRating(e.target.value)}
                      style={{ marginLeft: 4 }}
                      title="选择新增监测股票的投资评级"
                    >
                      <option value="买入">买入</option>
                      <option value="持有">持有</option>
                      <option value="卖出">卖出</option>
                    </select>
                  </label>
                </div>

                <div style={{ marginTop: 4 }}>
                  <div style={{ fontSize: 13, fontWeight: 600 }}>
                    🤖 量化交易（MiniQMT）
                  </div>
                  <label style={{ fontSize: 13 }}>
                    <input
                      type="checkbox"
                      checked={addQuantEnabled}
                      onChange={(e) => setAddQuantEnabled(e.target.checked)}
                    />
                    &nbsp;启用量化自动交易
                  </label>
                  {addQuantEnabled && (
                    <div
                      style={{
                        marginTop: 6,
                        display: "grid",
                        gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
                        gap: 8,
                        fontSize: 12,
                      }}
                    >
                      <div>
                        <label>最大仓位比例</label>
                        <input
                          type="number"
                          step="0.05"
                          min={0.05}
                          max={0.5}
                          value={addQuantMaxPos}
                          onChange={(e) =>
                            setAddQuantMaxPos(Number(e.target.value))
                          }
                          style={{
                            marginTop: 2,
                            width: "100%",
                            padding: "4px 6px",
                            borderRadius: 8,
                            border: "1px solid #ddd",
                          }}
                        />
                      </div>
                      <div>
                        <label>
                          <input
                            type="checkbox"
                            checked={addQuantAutoSL}
                            onChange={(e) =>
                              setAddQuantAutoSL(e.target.checked)
                            }
                          />
                          &nbsp;自动止损
                        </label>
                      </div>
                      <div>
                        <label>
                          <input
                            type="checkbox"
                            checked={addQuantAutoTP}
                            onChange={(e) =>
                              setAddQuantAutoTP(e.target.checked)
                            }
                          />
                          &nbsp;自动止盈
                        </label>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>

            <div style={{ marginTop: 12 }}>
              <button
                type="button"
                disabled={addingStock}
                onClick={handleAddStock}
                style={{
                  padding: "8px 16px",
                  borderRadius: 10,
                  border: "none",
                  background:
                    "linear-gradient(135deg, #10b981 0%, #22c55e 100%)",
                  color: "#fff",
                  cursor: addingStock ? "default" : "pointer",
                  fontWeight: 600,
                }}
              >
                {addingStock ? "添加中..." : "✅ 添加监测"}
              </button>
            </div>
          </div>
        )}
      </section>

      {/* 监测股票列表 */}
      <section style={{ marginBottom: 20 }}>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: 8,
          }}
        >
          <h2 style={{ margin: 0, fontSize: 18 }}>📋 监测股票列表</h2>
          <div style={{ display: "flex", gap: 8 }}>
            <input
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              placeholder="🔍 搜索代码或名称"
              style={{
                padding: "6px 8px",
                borderRadius: 999,
                border: "1px solid #ddd",
                fontSize: 13,
                minWidth: 200,
              }}
            />
            <select
              value={ratingFilter}
              onChange={(e) => setRatingFilter(e.target.value)}
              style={{ padding: "6px 8px", borderRadius: 999, fontSize: 13 }}
            >
              <option value="全部">全部评级</option>
              <option value="买入">买入</option>
              <option value="持有">持有</option>
              <option value="卖出">卖出</option>
            </select>
          </div>
        </div>

        {loadingStocks && (
          <p style={{ fontSize: 13, color: "#666" }}>正在加载监测列表...</p>
        )}
        {error && (
          <p style={{ fontSize: 13, color: "#b00020" }}>错误：{error}</p>
        )}
        {!loadingStocks && filteredStocks.length === 0 && (
          <p style={{ fontSize: 13, color: "#666" }}>
            📭 暂无监测股票，请先在上方添加股票监测。
          </p>
        )}

        {filteredStocks.length > 0 && (
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))",
              gap: 12,
            }}
          >
            {filteredStocks.map((s) => {
              const entry = s.entry_range || {};
              const ratingColor: Record<string, string> = {
                买入: "🟢",
                持有: "🟡",
                卖出: "🔴",
              };
              return (
                <div
                  key={s.id}
                  style={{
                    borderRadius: 12,
                    padding: 12,
                    background: "#f9fafb",
                    boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                  }}
                >
                  <div
                    style={{
                      display: "flex",
                      justifyContent: "space-between",
                      alignItems: "center",
                      marginBottom: 6,
                    }}
                  >
                    <div>
                      <div style={{ fontWeight: 600 }}>
                        {s.symbol} - {s.name}
                      </div>
                      <div style={{ fontSize: 12, marginTop: 2 }}>
                        评级: {ratingColor[s.rating] || "⚪"} {s.rating}
                      </div>
                    </div>
                    <div style={{ textAlign: "right" }}>
                      <div style={{ fontSize: 12, color: "#666" }}>当前价格</div>
                      <div style={{ fontSize: 16, marginTop: 2 }}>
                        {s.current_price && s.current_price !== "N/A"
                          ? `¥${Number(s.current_price).toFixed(2)}`
                          : "等待更新"}
                      </div>
                    </div>
                  </div>

                  <div style={{ fontSize: 12, marginTop: 4 }}>
                    <div style={{ fontWeight: 600 }}>🎯 关键位置</div>
                    <div
                      style={{
                        display: "grid",
                        gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
                        gap: 6,
                        marginTop: 4,
                      }}
                    >
                      <div>
                        <div>进场区间</div>
                        <div style={{ marginTop: 2 }}>
                          {entry.min && entry.max
                            ? `¥${entry.min} - ¥${entry.max}`
                            : "未设置"}
                        </div>
                      </div>
                      <div>
                        <div>止盈位</div>
                        <div style={{ marginTop: 2 }}>
                          {s.take_profit ? `¥${s.take_profit}` : "未设置"}
                        </div>
                      </div>
                      <div>
                        <div>止损位</div>
                        <div style={{ marginTop: 2 }}>
                          {s.stop_loss ? `¥${s.stop_loss}` : "未设置"}
                        </div>
                      </div>
                    </div>
                  </div>

                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
                      gap: 6,
                      marginTop: 6,
                      fontSize: 12,
                      color: "#555",
                    }}
                  >
                    <div>间隔: {s.check_interval} 分钟</div>
                    <div>
                      最后检查:
                      {s.last_checked
                        ? ` ${formatDateTime(s.last_checked)}`
                        : " 从未"}
                    </div>
                    <div>
                      通知: {s.notification_enabled ? "🟢 启用" : "🔴 禁用"}
                      {s.quant_enabled && <span> ｜ 🤖 量化启用</span>}
                    </div>
                  </div>

                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
                      gap: 6,
                      marginTop: 8,
                      fontSize: 12,
                    }}
                  >
                    <button
                      type="button"
                      onClick={() => handleManualUpdate(s.id)}
                      style={{
                        padding: "4px 6px",
                        borderRadius: 8,
                        border: "1px solid #d4d4d4",
                        background: "#fff",
                        cursor: "pointer",
                      }}
                    >
                      🔄 更新
                    </button>
                    <button
                      type="button"
                      onClick={() => openEdit(s)}
                      style={{
                        padding: "4px 6px",
                        borderRadius: 8,
                        border: "1px solid #d4d4d4",
                        background: "#fff",
                        cursor: "pointer",
                      }}
                    >
                      ✏️ 编辑
                    </button>
                    <button
                      type="button"
                      onClick={() =>
                        handleToggleNotification(s.id, !s.notification_enabled)
                      }
                      style={{
                        padding: "4px 6px",
                        borderRadius: 8,
                        border: "1px solid #d4d4d4",
                        background: "#fff",
                        cursor: "pointer",
                      }}
                    >
                      {s.notification_enabled ? "🔕 禁用" : "🔔 启用"}
                    </button>
                    <button
                      type="button"
                      onClick={() => handleDeleteStock(s.id)}
                      style={{
                        padding: "4px 6px",
                        borderRadius: 8,
                        border: "1px solid #fecaca",
                        background: "#fef2f2",
                        color: "#b91c1c",
                        cursor: "pointer",
                      }}
                    >
                      🗑️ 删除
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </section>

      {/* 编辑对话框（简单内联） */}
      {editingId != null && (
        <section
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 14,
            boxShadow: "0 2px 10px rgba(0,0,0,0.08)",
            marginBottom: 18,
          }}
        >
          <h2 style={{ marginTop: 0, fontSize: 16 }}>✏️ 编辑监测配置</h2>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "1.2fr 1.3fr",
              gap: 16,
            }}
          >
            <div>
              <h3 style={{ fontSize: 14, marginTop: 0 }}>🎯 关键位置</h3>
              <div style={{ marginBottom: 6 }}>
                <label style={{ fontSize: 13 }}>进场区间最低价</label>
                <input
                  type="number"
                  value={editEntryMin}
                  onChange={(e) => setEditEntryMin(Number(e.target.value))}
                  style={{
                    marginTop: 4,
                    width: "100%",
                    padding: "6px 8px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                  }}
                  step="0.01"
                />
              </div>
              <div style={{ marginBottom: 6 }}>
                <label style={{ fontSize: 13 }}>进场区间最高价</label>
                <input
                  type="number"
                  value={editEntryMax}
                  onChange={(e) => setEditEntryMax(Number(e.target.value))}
                  style={{
                    marginTop: 4,
                    width: "100%",
                    padding: "6px 8px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                  }}
                  step="0.01"
                />
              </div>
              <div style={{ marginBottom: 6 }}>
                <label style={{ fontSize: 13 }}>止盈价位</label>
                <input
                  type="number"
                  value={editTakeProfit}
                  onChange={(e) => setEditTakeProfit(Number(e.target.value))}
                  style={{
                    marginTop: 4,
                    width: "100%",
                    padding: "6px 8px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                  }}
                  step="0.01"
                />
              </div>
              <div style={{ marginBottom: 6 }}>
                <label style={{ fontSize: 13 }}>止损价位</label>
                <input
                  type="number"
                  value={editStopLoss}
                  onChange={(e) => setEditStopLoss(Number(e.target.value))}
                  style={{
                    marginTop: 4,
                    width: "100%",
                    padding: "6px 8px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                  }}
                  step="0.01"
                />
              </div>
            </div>

            <div>
              <h3 style={{ fontSize: 14, marginTop: 0 }}>⚙️ 监测设置</h3>
              <div style={{ marginBottom: 6 }}>
                <label style={{ fontSize: 13 }}>监测间隔（分钟）</label>
                <input
                  type="number"
                  value={editCheckInterval}
                  onChange={(e) =>
                    setEditCheckInterval(Number(e.target.value) || 30)
                  }
                  min={5}
                  max={120}
                  style={{
                    marginTop: 4,
                    width: "100%",
                    padding: "6px 8px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                  }}
                />
              </div>
              <div
                style={{
                  display: "flex",
                  flexWrap: "wrap",
                  gap: 10,
                  alignItems: "center",
                  marginBottom: 6,
                }}
              >
                <label style={{ fontSize: 13 }}>
                  投资评级：
                  <select
                    value={editRating}
                    onChange={(e) => setEditRating(e.target.value)}
                    style={{ marginLeft: 4 }}
                  >
                    <option value="买入">买入</option>
                    <option value="持有">持有</option>
                    <option value="卖出">卖出</option>
                  </select>
                </label>
                <label style={{ fontSize: 13 }}>
                  <input
                    type="checkbox"
                    checked={editNotificationEnabled}
                    onChange={(e) =>
                      setEditNotificationEnabled(e.target.checked)
                    }
                  />
                  &nbsp;启用通知
                </label>
              </div>

              <div>
                <div style={{ fontSize: 13, fontWeight: 600 }}>
                  🤖 量化交易
                </div>
                <label style={{ fontSize: 13 }}>
                  <input
                    type="checkbox"
                    checked={editQuantEnabled}
                    onChange={(e) => setEditQuantEnabled(e.target.checked)}
                  />
                  &nbsp;启用量化自动交易
                </label>
                {editQuantEnabled && (
                  <div
                    style={{
                      marginTop: 6,
                      display: "grid",
                      gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
                      gap: 8,
                      fontSize: 12,
                    }}
                  >
                    <div>
                      <label>最大仓位比例</label>
                      <input
                        type="number"
                        step="0.05"
                        min={0.05}
                        max={0.5}
                        value={editQuantMaxPos}
                        onChange={(e) =>
                          setEditQuantMaxPos(Number(e.target.value))
                        }
                        style={{
                          marginTop: 2,
                          width: "100%",
                          padding: "4px 6px",
                          borderRadius: 8,
                          border: "1px solid #ddd",
                        }}
                      />
                    </div>
                    <div>
                      <label>
                        <input
                          type="checkbox"
                          checked={editQuantAutoSL}
                          onChange={(e) =>
                            setEditQuantAutoSL(e.target.checked)
                          }
                        />
                        &nbsp;自动止损
                      </label>
                    </div>
                    <div>
                      <label>
                        <input
                          type="checkbox"
                          checked={editQuantAutoTP}
                          onChange={(e) =>
                            setEditQuantAutoTP(e.target.checked)
                          }
                        />
                        &nbsp;自动止盈
                      </label>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>

          <div
            style={{
              marginTop: 10,
              display: "flex",
              gap: 8,
              justifyContent: "flex-end",
            }}
          >
            <button
              type="button"
              onClick={() => setEditingId(null)}
              style={{
                padding: "6px 12px",
                borderRadius: 8,
                border: "1px solid #d4d4d4",
                background: "#fff",
                cursor: "pointer",
              }}
            >
              取消
            </button>
            <button
              type="button"
              disabled={savingEdit}
              onClick={handleSaveEdit}
              style={{
                padding: "6px 12px",
                borderRadius: 8,
                border: "none",
                background:
                  "linear-gradient(135deg, #6366f1 0%, #4f46e5 100%)",
                color: "#fff",
                cursor: savingEdit ? "default" : "pointer",
                fontWeight: 600,
              }}
            >
              {savingEdit ? "保存中..." : "✅ 保存修改"}
            </button>
          </div>
        </section>
      )}

      {/* 通知管理 & MiniQMT 状态 */}
      <section
        style={{
          display: "grid",
          gridTemplateColumns: "minmax(0, 1.4fr) minmax(0, 1.6fr)",
          gap: 16,
          marginBottom: 20,
        }}
      >
        <div
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 14,
            boxShadow: "0 2px 10px rgba(0,0,0,0.06)",
          }}
        >
          <h2 style={{ marginTop: 0, fontSize: 16 }}>📧 邮件通知设置</h2>
          {loadingEmailConfig && (
            <p style={{ fontSize: 13, color: "#666" }}>正在加载配置...</p>
          )}
          {emailConfig && (
            <>
              <p style={{ fontSize: 13 }}>
                当前状态：
                {emailConfig.configured ? (
                  <span style={{ color: "#16a34a" }}>已完成配置</span>
                ) : (
                  <span style={{ color: "#b45309" }}>未配置或不完整</span>
                )}
              </p>
              <div
                style={{
                  background: "#f9fafb",
                  borderRadius: 8,
                  padding: 8,
                  fontSize: 12,
                }}
              >
                <div>SMTP服务器: {emailConfig.smtp_server}</div>
                <div>SMTP端口: {emailConfig.smtp_port}</div>
                <div>发送邮箱: {emailConfig.email_from}</div>
                <div>接收邮箱: {emailConfig.email_to}</div>
                <div>
                  启用状态: {emailConfig.enabled ? "是" : "否"}
                </div>
              </div>
              <div style={{ marginTop: 8 }}>
                <button
                  type="button"
                  disabled={!emailConfig.configured || sendingTestEmail}
                  onClick={handleSendTestEmail}
                  style={{
                    padding: "6px 12px",
                    borderRadius: 8,
                    border: "none",
                    background: emailConfig.configured
                      ? "#0ea5e9"
                      : "#e5e7eb",
                    color: emailConfig.configured ? "#fff" : "#9ca3af",
                    cursor:
                      emailConfig.configured && !sendingTestEmail
                        ? "pointer"
                        : "default",
                    fontSize: 13,
                  }}
                >
                  {sendingTestEmail ? "发送中..." : "📧 发送测试邮件"}
                </button>
              </div>
              <p
                style={{
                  marginTop: 8,
                  fontSize: 12,
                  color: "#6b7280",
                  whiteSpace: "pre-wrap",
                }}
              >
                {"在 .env 中配置 EMAIL_ENABLED/SMTP_SERVER/EMAIL_FROM/EMAIL_PASSWORD/EMAIL_TO 等参数。\n建议使用授权码而非邮箱登录密码。"}
              </p>
            </>
          )}
        </div>

        <div
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 14,
            boxShadow: "0 2px 10px rgba(0,0,0,0.06)",
          }}
        >
          <h2 style={{ marginTop: 0, fontSize: 16 }}>📱 通知历史</h2>
          {notifications.length === 0 ? (
            <p style={{ fontSize: 13, color: "#666" }}>📭 暂无通知</p>
          ) : (
            <>
              <div
                style={{
                  maxHeight: 260,
                  overflowY: "auto",
                  paddingRight: 4,
                }}
              >
                {notifications.map((n) => {
                  const iconMap: Record<string, string> = {
                    entry: "🟢",
                    take_profit: "🟡",
                    stop_loss: "🔴",
                    quant_trade: "🤖",
                  };
                  const icon = iconMap[n.type] || "🔵";
                  return (
                    <div
                      key={n.id}
                      style={{
                        borderRadius: 8,
                        padding: 8,
                        background: "#f9fafb",
                        marginBottom: 6,
                        fontSize: 12,
                      }}
                    >
                      <div>
                        {icon} <strong>{n.symbol}</strong> - {n.message}
                      </div>
                      <div
                        style={{
                          marginTop: 2,
                          display: "flex",
                          justifyContent: "space-between",
                          color: "#6b7280",
                        }}
                      >
                        <span>{formatDateTime(n.triggered_at)}</span>
                        <span>
                          {n.sent ? "✅ 已发送" : "⏳ 待发送"}
                        </span>
                      </div>
                    </div>
                  );
                })}
              </div>
              <div
                style={{
                  marginTop: 8,
                  display: "flex",
                  gap: 8,
                }}
              >
                <button
                  type="button"
                  onClick={handleMarkAllNotificationsSent}
                  style={{
                    padding: "6px 10px",
                    borderRadius: 8,
                    border: "none",
                    background: "#22c55e",
                    color: "#fff",
                    cursor: "pointer",
                    fontSize: 13,
                  }}
                >
                  ✅ 标记已读
                </button>
                <button
                  type="button"
                  onClick={handleClearNotifications}
                  style={{
                    padding: "6px 10px",
                    borderRadius: 8,
                    border: "1px solid #fecaca",
                    background: "#fef2f2",
                    color: "#b91c1c",
                    cursor: "pointer",
                    fontSize: 13,
                  }}
                >
                  🗑️ 清空通知
                </button>
              </div>
            </>
          )}
        </div>
      </section>

      {/* MiniQMT 状态与量化统计 + 定时调度配置 */}
      <section
        style={{
          display: "grid",
          gridTemplateColumns: "minmax(0, 1.2fr) minmax(0, 1.8fr)",
          gap: 16,
          marginBottom: 40,
        }}
      >
        <div
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 14,
            boxShadow: "0 2px 10px rgba(0,0,0,0.06)",
          }}
        >
          <h2 style={{ marginTop: 0, fontSize: 16 }}>🤖 MiniQMT 量化交易</h2>
          {loadingMiniQmt && (
            <p style={{ fontSize: 13, color: "#666" }}>正在读取状态...</p>
          )}
          {miniQmt && (
            <>
              <div style={{ fontSize: 13, marginBottom: 6 }}>
                组件启用：
                {miniQmt.enabled ? (
                  <span style={{ color: "#16a34a" }}>已启用</span>
                ) : (
                  <span style={{ color: "#b45309" }}>未启用</span>
                )}
              </div>
              <div style={{ fontSize: 13, marginBottom: 6 }}>
                连接状态：
                {miniQmt.connected ? (
                  <span style={{ color: "#16a34a" }}>✅ 已连接</span>
                ) : (
                  <span style={{ color: "#6b7280" }}>⏸️ 未连接</span>
                )}
              </div>
              <div style={{ fontSize: 13, marginBottom: 6 }}>
                账户ID：{miniQmt.account_id || "未配置"}
              </div>
              <div style={{ marginTop: 8, display: "flex", gap: 8 }}>
                {miniQmt.enabled && !miniQmt.connected && (
                  <button
                    type="button"
                    disabled={operatingMiniQmt}
                    onClick={handleMiniQmtConnect}
                    style={{
                      padding: "6px 10px",
                      borderRadius: 8,
                      border: "none",
                      background: "#22c55e",
                      color: "#fff",
                      cursor: operatingMiniQmt ? "default" : "pointer",
                      fontSize: 13,
                    }}
                  >
                    🔗 连接 MiniQMT
                  </button>
                )}
                {miniQmt.connected && (
                  <button
                    type="button"
                    disabled={operatingMiniQmt}
                    onClick={handleMiniQmtDisconnect}
                    style={{
                      padding: "6px 10px",
                      borderRadius: 8,
                      border: "1px solid #d4d4d4",
                      background: "#fff",
                      color: "#374151",
                      cursor: operatingMiniQmt ? "default" : "pointer",
                      fontSize: 13,
                    }}
                  >
                    🔌 断开连接
                  </button>
                )}
              </div>
            </>
          )}

          <div style={{ marginTop: 12 }}>
            <h3 style={{ fontSize: 14, marginTop: 0 }}>📈 量化统计</h3>
            <p style={{ fontSize: 13 }}>
              启用量化的股票：{quantStocks.length}/{stocks.length}
            </p>
            {quantStocks.length > 0 ? (
              <ul
                style={{
                  margin: 0,
                  paddingLeft: 16,
                  fontSize: 12,
                  maxHeight: 160,
                  overflowY: "auto",
                }}
              >
                {quantStocks.map((s) => (
                  <li key={s.id}>
                    🤖 {s.symbol} - {s.name}
                  </li>
                ))}
              </ul>
            ) : (
              <p style={{ fontSize: 12, color: "#6b7280" }}>
                暂无启用量化交易的股票。
              </p>
            )}
            <p
              style={{
                marginTop: 8,
                fontSize: 12,
                color: "#6b7280",
                whiteSpace: "pre-wrap",
              }}
            >
              {"在 config.py 中配置 MINIQMT_CONFIG，并确保 MiniQMT 客户端已安装并登录。"}
            </p>
          </div>
        </div>

        <div
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 14,
            boxShadow: "0 2px 10px rgba(0,0,0,0.06)",
          }}
        >
          <h2 style={{ marginTop: 0, fontSize: 16 }}>⏰ 定时自动启动/关闭</h2>
          {loadingScheduler && (
            <p style={{ fontSize: 13, color: "#666" }}>正在加载调度状态...</p>
          )}
          {scheduler && (
            <>
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
                  gap: 8,
                  fontSize: 13,
                  marginBottom: 10,
                }}
              >
                <div>
                  定时：
                  {scheduler.scheduler_enabled ? (
                    <span style={{ color: "#16a34a" }}>已启用</span>
                  ) : (
                    <span style={{ color: "#6b7280" }}>未启用</span>
                  )}
                </div>
                <div>
                  调度器：
                  {scheduler.scheduler_running ? (
                    <span style={{ color: "#16a34a" }}>运行中</span>
                  ) : (
                    <span style={{ color: "#6b7280" }}>未运行</span>
                  )}
                </div>
                <div>
                  交易日：
                  {scheduler.is_trading_day ? "是" : "否"}
                </div>
                <div>
                  当前时间：
                  {scheduler.is_trading_time ? "交易时间内" : "非交易时间"}
                </div>
              </div>

              <div style={{ marginBottom: 8 }}>
                <label style={{ fontSize: 13 }}>市场</label>
                <select
                  value={schedMarket}
                  onChange={(e) => setSchedMarket(e.target.value)}
                  style={{
                    marginLeft: 8,
                    padding: "4px 8px",
                    borderRadius: 8,
                  }}
                >
                  <option value="CN">中国A股</option>
                  <option value="US">美股</option>
                  <option value="HK">港股</option>
                </select>
              </div>

              <div style={{ marginBottom: 8, fontSize: 13 }}>
                <div style={{ marginBottom: 4 }}>交易日</div>
                <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                  {tradingDayOptions.map((d) => (
                    <label
                      key={d.value}
                      style={{
                        padding: "4px 8px",
                        borderRadius: 999,
                        border: schedTradingDays.includes(d.value)
                          ? "1px solid #4f46e5"
                          : "1px solid #d4d4d4",
                        background: schedTradingDays.includes(d.value)
                          ? "#eef2ff"
                          : "#fff",
                        cursor: "pointer",
                      }}
                    >
                      <input
                        type="checkbox"
                        checked={schedTradingDays.includes(d.value)}
                        onChange={() => toggleTradingDay(d.value)}
                        style={{ marginRight: 4 }}
                      />
                      {d.label}
                    </label>
                  ))}
                </div>
              </div>

              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
                  gap: 8,
                  fontSize: 13,
                  marginBottom: 8,
                }}
              >
                <label>
                  <input
                    type="checkbox"
                    checked={schedEnabled}
                    onChange={(e) => setSchedEnabled(e.target.checked)}
                  />
                  &nbsp;启用定时调度
                </label>
                <label>
                  <input
                    type="checkbox"
                    checked={schedAutoStop}
                    onChange={(e) => setSchedAutoStop(e.target.checked)}
                  />
                  &nbsp;收盘后自动停止
                </label>
                <div>
                  <span>提前启动(分钟)：</span>
                  <input
                    type="number"
                    value={schedPreMinutes}
                    onChange={(e) =>
                      setSchedPreMinutes(Number(e.target.value) || 0)
                    }
                    min={0}
                    max={30}
                    style={{
                      width: 60,
                      marginLeft: 4,
                      padding: "2px 4px",
                      borderRadius: 6,
                      border: "1px solid #ddd",
                    }}
                  />
                </div>
                <div>
                  <span>延后停止(分钟)：</span>
                  <input
                    type="number"
                    value={schedPostMinutes}
                    onChange={(e) =>
                      setSchedPostMinutes(Number(e.target.value) || 0)
                    }
                    min={0}
                    max={30}
                    style={{
                      width: 60,
                      marginLeft: 4,
                      padding: "2px 4px",
                      borderRadius: 6,
                      border: "1px solid #ddd",
                    }}
                  />
                </div>
              </div>

              <p
                style={{
                  marginTop: 4,
                  fontSize: 12,
                  color: "#6b7280",
                  whiteSpace: "pre-wrap",
                }}
              >
                {"启用定时调度后，系统将在交易时间自动启动/停止监测服务，不影响手动操作。"}
              </p>

              <div
                style={{
                  marginTop: 8,
                  display: "flex",
                  gap: 8,
                  flexWrap: "wrap",
                }}
              >
                <button
                  type="button"
                  disabled={savingScheduler}
                  onClick={handleSaveScheduler}
                  style={{
                    padding: "6px 12px",
                    borderRadius: 8,
                    border: "none",
                    background:
                      "linear-gradient(135deg, #6366f1 0%, #4f46e5 100%)",
                    color: "#fff",
                    cursor: savingScheduler ? "default" : "pointer",
                    fontSize: 13,
                  }}
                >
                  {savingScheduler ? "保存中..." : "💾 保存设置"}
                </button>
                {scheduler.scheduler_running ? (
                  <button
                    type="button"
                    onClick={handleStopScheduler}
                    style={{
                      padding: "6px 12px",
                      borderRadius: 8,
                      border: "1px solid #d4d4d4",
                      background: "#fff",
                      fontSize: 13,
                      cursor: "pointer",
                    }}
                  >
                    ⏹️ 停止调度器
                  </button>
                ) : (
                  <button
                    type="button"
                    disabled={!schedEnabled}
                    onClick={handleStartScheduler}
                    style={{
                      padding: "6px 12px",
                      borderRadius: 8,
                      border: "1px solid #d4d4d4",
                      background: schedEnabled ? "#fff" : "#f9fafb",
                      fontSize: 13,
                      cursor: schedEnabled ? "pointer" : "default",
                    }}
                  >
                    ▶️ 启动调度器
                  </button>
                )}
              </div>
            </>
          )}
        </div>
      </section>
    </main>
  );
}
