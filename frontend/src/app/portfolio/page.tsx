"use client";

import { useEffect, useMemo, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface PortfolioStock {
  id: number;
  code: string;
  name: string;
  cost_price?: number | null;
  quantity?: number | null;
  note?: string | null;
  auto_monitor: boolean;
  created_at?: string | null;
  updated_at?: string | null;
}

interface BatchFinalDecision {
  rating?: string;
  confidence_level?: number | string;
  target_price?: string | number | null;
  entry_range?: string | null;
  take_profit?: string | number | null;
  stop_loss?: string | number | null;
  advice?: string;
  summary?: string;
}

interface BatchStockInfo {
  name?: string;
  current_price?: number | null;
}

interface BatchSingleResult {
  success: boolean;
  error?: string;
  final_decision?: BatchFinalDecision;
  stock_info?: BatchStockInfo;
}

interface BatchResultItem {
  code: string;
  name?: string;
  result: BatchSingleResult;
}

interface BatchFailedItem {
  code: string;
  name?: string;
  error: string;
}

interface BatchAnalyzeResponse {
  success: boolean;
  mode: "sequential" | "parallel" | string;
  total: number;
  succeeded: number;
  failed: number;
  elapsed_time: number;
  results: BatchResultItem[];
  failed_stocks: BatchFailedItem[];
  saved_count?: number;
  sync_result?: {
    added: number;
    updated: number;
    failed: number;
    total: number;
  } | null;
}

interface SchedulerStatus {
  is_running: boolean;
  schedule_time: string;
  analysis_mode: "sequential" | "parallel" | string;
  auto_monitor_sync: boolean;
  notification_enabled: boolean;
  last_run_time?: string | null;
  next_run_time?: string | null;
  portfolio_count: number;
  schedule_times?: string[];
}

interface HistoryRecord {
  id: number;
  portfolio_stock_id: number;
  analysis_time: string;
  rating: string;
  confidence: number;
  current_price?: number | null;
  target_price?: number | null;
  entry_min?: number | null;
  entry_max?: number | null;
  take_profit?: number | null;
  stop_loss?: number | null;
  summary?: string | null;
  code?: string;
  name?: string;
}

function formatNumber(
  v: number | string | null | undefined,
  digits = 2,
) {
  if (v === null || v === undefined) return "-";
  const n = typeof v === "number" ? v : Number(v);
  if (Number.isNaN(n)) return "-";
  return n.toFixed(digits);
}

function formatDateTime(s?: string | null) {
  if (!s) return "-";
  try {
    const d = new Date(s);
    if (Number.isNaN(d.getTime())) return s;
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
    return s;
  }
}

export default function PortfolioPage() {
  const [activeTab, setActiveTab] = useState<
    "stocks" | "batch" | "scheduler" | "history"
  >("stocks");

  const [stocks, setStocks] = useState<PortfolioStock[]>([]);
  const [loadingStocks, setLoadingStocks] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [showAddForm, setShowAddForm] = useState(false);
  const [addCode, setAddCode] = useState("");
  const [addName, setAddName] = useState("");
  const [addCost, setAddCost] = useState<number | "">("");
  const [addQty, setAddQty] = useState<number | "">("");
  const [addNote, setAddNote] = useState("");
  const [addAutoMonitor, setAddAutoMonitor] = useState(true);
  const [adding, setAdding] = useState(false);

  const [editingId, setEditingId] = useState<number | null>(null);
  const [editCost, setEditCost] = useState<number | "">("");
  const [editQty, setEditQty] = useState<number | "">("");
  const [editNote, setEditNote] = useState("");
  const [editAutoMonitor, setEditAutoMonitor] = useState(true);
  const [savingEdit, setSavingEdit] = useState(false);

  const [batchMode, setBatchMode] = useState<"sequential" | "parallel">(
    "sequential",
  );
  const [batchMaxWorkers, setBatchMaxWorkers] = useState(3);
  const [batchAutoSync, setBatchAutoSync] = useState(true);
  const [batchSendNotification, setBatchSendNotification] = useState(true);
  const [batchRunning, setBatchRunning] = useState(false);
  const [batchResult, setBatchResult] = useState<BatchAnalyzeResponse | null>(
    null,
  );

  const [scheduler, setScheduler] = useState<SchedulerStatus | null>(null);
  const [loadingScheduler, setLoadingScheduler] = useState(false);
  const [savingScheduler, setSavingScheduler] = useState(false);
  const [schedTimes, setSchedTimes] = useState<string[]>(["09:30"]);
  const [schedMode, setSchedMode] = useState<"sequential" | "parallel">(
    "sequential",
  );
  const [schedMaxWorkers, setSchedMaxWorkers] = useState(3);
  const [schedAutoSync, setSchedAutoSync] = useState(true);
  const [schedSendNotification, setSchedSendNotification] = useState(true);
  const [schedNewTime, setSchedNewTime] = useState("15:05");

  const [historyStocksLoaded, setHistoryStocksLoaded] = useState(false);
  const [historyTabKey, setHistoryTabKey] = useState<string>("全部");
  const [historyRecords, setHistoryRecords] = useState<HistoryRecord[]>([]);
  const [loadingHistory, setLoadingHistory] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);

  const stats = useMemo(() => {
    const total = stocks.length;
    const autoCount = stocks.filter((s) => s.auto_monitor).length;
    let totalCost = 0;
    for (const s of stocks) {
      const cost = typeof s.cost_price === "number" ? s.cost_price : 0;
      const qty = typeof s.quantity === "number" ? s.quantity : 0;
      if (cost && qty) totalCost += cost * qty;
    }
    return { total, autoCount, totalCost };
  }, [stocks]);

  async function loadStocks() {
    setLoadingStocks(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/portfolio/stocks`);
      if (!res.ok) throw new Error(`加载持仓失败: ${res.status}`);
      const data: PortfolioStock[] = await res.json();
      setStocks(data || []);
    } catch (e: any) {
      setError(e?.message || "加载持仓失败");
      setStocks([]);
    } finally {
      setLoadingStocks(false);
    }
  }

  async function handleAddStock() {
    if (!addCode.trim()) {
      setError("请输入股票代码");
      return;
    }
    setAdding(true);
    setError(null);
    try {
      const payload: any = {
        code: addCode.trim().toUpperCase(),
        name: addName.trim() || null,
        cost_price:
          typeof addCost === "number" && addCost > 0 ? Number(addCost) : null,
        quantity:
          typeof addQty === "number" && addQty > 0 ? Number(addQty) : null,
        note: addNote.trim() || null,
        auto_monitor: addAutoMonitor,
      };
      const res = await fetch(`${API_BASE}/portfolio/stocks`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || `添加失败: ${res.status}`);
      }
      setAddCode("");
      setAddName("");
      setAddCost("");
      setAddQty("");
      setAddNote("");
      setAddAutoMonitor(true);
      await loadStocks();
    } catch (e: any) {
      setError(e?.message || "添加持仓失败");
    } finally {
      setAdding(false);
    }
  }

  function openEdit(stock: PortfolioStock) {
    setEditingId(stock.id);
    setEditCost(
      typeof stock.cost_price === "number" ? stock.cost_price : "",
    );
    setEditQty(typeof stock.quantity === "number" ? stock.quantity : "");
    setEditNote(stock.note || "");
    setEditAutoMonitor(!!stock.auto_monitor);
  }

  async function handleSaveEdit() {
    if (editingId == null) return;
    setSavingEdit(true);
    setError(null);
    try {
      const payload: any = {};
      if (editCost !== "") payload.cost_price = Number(editCost) || 0;
      if (editQty !== "") payload.quantity = Number(editQty) || 0;
      payload.note = editNote;
      payload.auto_monitor = editAutoMonitor;
      const res = await fetch(`${API_BASE}/portfolio/stocks/${editingId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || `更新失败: ${res.status}`);
      }
      setEditingId(null);
      await loadStocks();
    } catch (e: any) {
      setError(e?.message || "更新持仓失败");
    } finally {
      setSavingEdit(false);
    }
  }

  async function handleDeleteStock(id: number, code: string) {
    const ok =
      typeof window === "undefined" ||
      window.confirm(`确认删除持仓 ${code} 吗？此操作不可恢复。`);
    if (!ok) return;
    try {
      const res = await fetch(`${API_BASE}/portfolio/stocks/${id}`, {
        method: "DELETE",
      });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || `删除失败: ${res.status}`);
      }
      await loadStocks();
    } catch (e: any) {
      setError(e?.message || "删除持仓失败");
    }
  }

  async function handleBatchAnalyze() {
    setBatchRunning(true);
    setError(null);
    setBatchResult(null);
    try {
      const payload = {
        mode: batchMode,
        max_workers: batchMode === "parallel" ? batchMaxWorkers : 1,
        auto_sync_monitor: batchAutoSync,
        send_notification: batchSendNotification,
      };
      const res = await fetch(`${API_BASE}/portfolio/batch-analyze`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data: BatchAnalyzeResponse = await res.json();
      if (!res.ok || !data.success) {
        const msg = (data as any)?.error || `批量分析失败: ${res.status}`;
        throw new Error(String(msg));
      }
      setBatchResult(data);
    } catch (e: any) {
      setError(e?.message || "批量分析失败");
    } finally {
      setBatchRunning(false);
    }
  }

  async function loadScheduler() {
    setLoadingScheduler(true);
    try {
      const res = await fetch(`${API_BASE}/portfolio/scheduler/status`);
      if (!res.ok) throw new Error(`读取调度器状态失败: ${res.status}`);
      const data: SchedulerStatus = await res.json();
      setScheduler(data);
      setSchedTimes(data.schedule_times && data.schedule_times.length
        ? data.schedule_times
        : [data.schedule_time || "09:30"]);
      setSchedMode(
        (data.analysis_mode as "sequential" | "parallel") || "sequential",
      );
      setSchedAutoSync(!!data.auto_monitor_sync);
      setSchedSendNotification(!!data.notification_enabled);
      setSchedMaxWorkers(3);
    } catch (e: any) {
      setError(e?.message || "加载调度器状态失败");
    } finally {
      setLoadingScheduler(false);
    }
  }

  async function saveSchedulerConfig(nextTimes: string[] = schedTimes) {
    setSavingScheduler(true);
    setError(null);
    try {
      const payload = {
        schedule_times: nextTimes,
        analysis_mode: schedMode,
        max_workers: schedMode === "parallel" ? schedMaxWorkers : 1,
        auto_sync_monitor: schedAutoSync,
        send_notification: schedSendNotification,
      };
      const res = await fetch(`${API_BASE}/portfolio/scheduler/config`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) throw new Error(`保存配置失败: ${res.status}`);
      const data: SchedulerStatus = await res.json();
      setScheduler(data);
      setSchedTimes(
        data.schedule_times && data.schedule_times.length
          ? data.schedule_times
          : [data.schedule_time || "09:30"],
      );
    } catch (e: any) {
      setError(e?.message || "保存调度配置失败");
    } finally {
      setSavingScheduler(false);
    }
  }

  async function handleAddTime() {
    if (!schedNewTime) return;
    if (schedTimes.includes(schedNewTime)) return;
    const next = [...schedTimes, schedNewTime].sort();
    setSchedTimes(next);
    await saveSchedulerConfig(next);
  }

  async function handleRemoveTime(time: string) {
    if (schedTimes.length <= 1) {
      setError("至少保留一个定时时间");
      return;
    }
    const next = schedTimes.filter((t) => t !== time);
    setSchedTimes(next);
    await saveSchedulerConfig(next);
  }

  async function handleStartScheduler() {
    setSavingScheduler(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/portfolio/scheduler/start`, {
        method: "POST",
      });
      const data: SchedulerStatus = await res.json();
      if (!res.ok) {
        throw new Error((data as any)?.detail || `启动失败: ${res.status}`);
      }
      setScheduler(data);
      setSchedTimes(
        data.schedule_times && data.schedule_times.length
          ? data.schedule_times
          : [data.schedule_time || "09:30"],
      );
    } catch (e: any) {
      setError(e?.message || "启动调度器失败");
    } finally {
      setSavingScheduler(false);
    }
  }

  async function handleStopScheduler() {
    setSavingScheduler(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/portfolio/scheduler/stop`, {
        method: "POST",
      });
      const data: SchedulerStatus = await res.json();
      if (!res.ok) {
        throw new Error((data as any)?.detail || `停止失败: ${res.status}`);
      }
      setScheduler(data);
    } catch (e: any) {
      setError(e?.message || "停止调度器失败");
    } finally {
      setSavingScheduler(false);
    }
  }

  async function handleRunOnce() {
    setSavingScheduler(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/portfolio/scheduler/run-once`, {
        method: "POST",
      });
      const data: SchedulerStatus = await res.json();
      if (!res.ok) {
        throw new Error((data as any)?.detail || `执行失败: ${res.status}`);
      }
      setScheduler(data);
    } catch (e: any) {
      setError(e?.message || "立即执行失败");
    } finally {
      setSavingScheduler(false);
    }
  }

  async function loadHistoryRecords(tabKey: string) {
    setLoadingHistory(true);
    setHistoryError(null);
    try {
      const allStocks = stocks;
      if (!allStocks.length) {
        setHistoryRecords([]);
        return;
      }
      if (tabKey === "全部") {
        const results: HistoryRecord[] = [];
        await Promise.all(
          allStocks.map(async (s) => {
            try {
              const res = await fetch(
                `${API_BASE}/portfolio/analysis/history?stock_id=${s.id}&limit=5`,
              );
              if (!res.ok) return;
              const data: HistoryRecord[] = await res.json();
              for (const r of data) {
                r.code = s.code;
                r.name = s.name;
              }
              results.push(...data);
            } catch {
              // ignore single failure
            }
          }),
        );
        results.sort(
          (a, b) =>
            new Date(b.analysis_time).getTime() -
            new Date(a.analysis_time).getTime(),
        );
        setHistoryRecords(results.slice(0, 20));
      } else {
        const s = allStocks.find((x) => x.code === tabKey);
        if (!s) {
          setHistoryRecords([]);
          return;
        }
        const res = await fetch(
          `${API_BASE}/portfolio/analysis/history?stock_id=${s.id}&limit=20`,
        );
        if (!res.ok) throw new Error(`加载历史失败: ${res.status}`);
        const data: HistoryRecord[] = await res.json();
        for (const r of data) {
          r.code = s.code;
          r.name = s.name;
        }
        setHistoryRecords(data);
      }
    } catch (e: any) {
      setHistoryError(e?.message || "加载分析历史失败");
      setHistoryRecords([]);
    } finally {
      setLoadingHistory(false);
    }
  }

  async function handleChangeHistoryTab(key: string) {
    setHistoryTabKey(key);
    await loadHistoryRecords(key);
  }

  useEffect(() => {
    loadStocks();
  }, []);

  useEffect(() => {
    if (activeTab === "scheduler" && !scheduler && !loadingScheduler) {
      loadScheduler();
    }
    if (activeTab === "history" && !historyStocksLoaded) {
      setHistoryStocksLoaded(true);
      if (stocks.length) {
        loadHistoryRecords("全部");
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab, stocks]);

  return (
    <main style={{ padding: 24 }}>
      <section style={{ marginBottom: 16 }}>
        <h1 style={{ margin: 0, fontSize: 22 }}>📊 持仓定时分析</h1>
        <p style={{ marginTop: 4, fontSize: 13, color: "#666" }}>
          复刻旧版持仓管理模块：持仓股票管理、批量分析、定时任务与分析历史。
        </p>
      </section>

      <section style={{ marginBottom: 12 }}>
        <div style={{ display: "flex", gap: 12, fontSize: 13 }}>
          <button
            type="button"
            onClick={() => setActiveTab("stocks")}
            style={{
              padding: "6px 12px",
              borderRadius: 999,
              border: "1px solid #ccc",
              background: activeTab === "stocks" ? "#eef2ff" : "#fff",
            }}
          >
            📝 持仓管理
          </button>
          <button
            type="button"
            onClick={() => setActiveTab("batch")}
            style={{
              padding: "6px 12px",
              borderRadius: 999,
              border: "1px solid #ccc",
              background: activeTab === "batch" ? "#eef2ff" : "#fff",
            }}
          >
            🔄 批量分析
          </button>
          <button
            type="button"
            onClick={() => setActiveTab("scheduler")}
            style={{
              padding: "6px 12px",
              borderRadius: 999,
              border: "1px solid #ccc",
              background: activeTab === "scheduler" ? "#eef2ff" : "#fff",
            }}
          >
            ⏰ 定时任务
          </button>
          <button
            type="button"
            onClick={() => setActiveTab("history")}
            style={{
              padding: "6px 12px",
              borderRadius: 999,
              border: "1px solid #ccc",
              background: activeTab === "history" ? "#eef2ff" : "#fff",
            }}
          >
            📈 分析历史
          </button>
        </div>
      </section>

      {error && (
        <p style={{ color: "#b00020", fontSize: 13, marginBottom: 8 }}>
          错误：{error}
        </p>
      )}

      {activeTab === "stocks" && (
        <section>
          <div
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 14,
              boxShadow: "0 2px 8px rgba(0,0,0,0.06)",
              marginBottom: 16,
            }}
          >
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
              }}
            >
              <h2 style={{ margin: 0, fontSize: 18 }}>📝 持仓股票管理</h2>
              <button
                type="button"
                onClick={() => setShowAddForm((v) => !v)}
                style={{
                  padding: "4px 10px",
                  borderRadius: 999,
                  border: "1px solid #ccc",
                  background: "#fafafa",
                  fontSize: 12,
                }}
              >
                {showAddForm ? "收起添加表单" : "➕ 添加持仓股票"}
              </button>
            </div>

            {showAddForm && (
              <div style={{ marginTop: 12 }}>
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
                    gap: 12,
                    fontSize: 13,
                  }}
                >
                  <div>
                    <label>
                      股票代码*
                      <input
                        value={addCode}
                        onChange={(e) => setAddCode(e.target.value)}
                        placeholder="例如: 600519 或 000001"
                        style={{
                          marginTop: 4,
                          width: "100%",
                          padding: "6px 8px",
                          borderRadius: 8,
                          border: "1px solid #ddd",
                        }}
                      />
                    </label>
                    <label style={{ marginTop: 8, display: "block" }}>
                      股票名称
                      <input
                        value={addName}
                        onChange={(e) => setAddName(e.target.value)}
                        placeholder="例如: 贵州茅台"
                        style={{
                          marginTop: 4,
                          width: "100%",
                          padding: "6px 8px",
                          borderRadius: 8,
                          border: "1px solid #ddd",
                        }}
                      />
                    </label>
                  </div>
                  <div>
                    <label style={{ display: "block" }}>
                      成本价
                      <input
                        type="number"
                        value={addCost === "" ? "" : addCost}
                        onChange={(e) =>
                          setAddCost(
                            e.target.value === ""
                              ? ""
                              : Number(e.target.value),
                          )
                        }
                        step="0.001"
                        style={{
                          marginTop: 4,
                          width: "100%",
                          padding: "6px 8px",
                          borderRadius: 8,
                          border: "1px solid #ddd",
                        }}
                      />
                    </label>
                    <label style={{ marginTop: 8, display: "block" }}>
                      持仓数量
                      <input
                        type="number"
                        value={addQty === "" ? "" : addQty}
                        onChange={(e) =>
                          setAddQty(
                            e.target.value === ""
                              ? ""
                              : Number(e.target.value),
                          )
                        }
                        step="100"
                        style={{
                          marginTop: 4,
                          width: "100%",
                          padding: "6px 8px",
                          borderRadius: 8,
                          border: "1px solid #ddd",
                        }}
                      />
                    </label>
                  </div>
                </div>

                <label style={{ marginTop: 8, display: "block", fontSize: 13 }}>
                  备注
                  <textarea
                    value={addNote}
                    onChange={(e) => setAddNote(e.target.value)}
                    placeholder="记录买入理由等信息"
                    style={{
                      marginTop: 4,
                      width: "100%",
                      padding: 8,
                      borderRadius: 8,
                      border: "1px solid #ddd",
                      minHeight: 64,
                    }}
                  />
                </label>

                <label
                  style={{
                    marginTop: 8,
                    display: "flex",
                    alignItems: "center",
                    gap: 6,
                    fontSize: 13,
                  }}
                >
                  <input
                    type="checkbox"
                    checked={addAutoMonitor}
                    onChange={(e) => setAddAutoMonitor(e.target.checked)}
                  />
                  分析后自动同步到监测
                </label>

                <div style={{ marginTop: 10 }}>
                  <button
                    type="button"
                    disabled={adding}
                    onClick={handleAddStock}
                    style={{
                      padding: "8px 16px",
                      borderRadius: 10,
                      border: "none",
                      background:
                        "linear-gradient(135deg, #10b981 0%, #22c55e 100%)",
                      color: "#fff",
                      cursor: adding ? "default" : "pointer",
                      fontWeight: 600,
                    }}
                  >
                    {adding ? "添加中..." : "➕ 添加股票"}
                  </button>
                </div>
              </div>
            )}
          </div>

          <section style={{ marginBottom: 12 }}>
            <div style={{ display: "flex", gap: 12 }}>
              <div
                style={{
                  flex: 1,
                  background: "#fff",
                  borderRadius: 10,
                  padding: 10,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                  fontSize: 13,
                }}
              >
                <div style={{ color: "#6b7280" }}>持仓股票数</div>
                <div style={{ marginTop: 4, fontSize: 18 }}>{stats.total}</div>
              </div>
              <div
                style={{
                  flex: 1,
                  background: "#fff",
                  borderRadius: 10,
                  padding: 10,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                  fontSize: 13,
                }}
              >
                <div style={{ color: "#6b7280" }}>启用自动监测</div>
                <div style={{ marginTop: 4, fontSize: 18 }}>{stats.autoCount}</div>
              </div>
              <div
                style={{
                  flex: 1,
                  background: "#fff",
                  borderRadius: 10,
                  padding: 10,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.06)",
                  fontSize: 13,
                }}
              >
                <div style={{ color: "#6b7280" }}>总持仓成本</div>
                <div style={{ marginTop: 4, fontSize: 18 }}>
                  ¥{stats.totalCost.toLocaleString(undefined, {
                    maximumFractionDigits: 2,
                  })}
                </div>
              </div>
            </div>
          </section>
          {loadingStocks && (
            <p style={{ fontSize: 13, color: "#666" }}>正在加载持仓...</p>
          )}

          {!loadingStocks && stocks.length === 0 && (
            <p style={{ fontSize: 13, color: "#666" }}>
              暂无持仓股票，请先添加股票代码开始管理。
            </p>
          )}

          <section style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {stocks.map((s) => (
              <div
                key={s.id}
                style={{
                  background: "#fff",
                  borderRadius: 12,
                  padding: 12,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.05)",
                }}
              >
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "3fr 2fr 2fr 2fr",
                    gap: 8,
                    alignItems: "center",
                    fontSize: 13,
                  }}
                >
                  <div>
                    <div style={{ fontWeight: 600 }}>
                      {s.code} {s.name}
                    </div>
                    {s.note && (
                      <div style={{ marginTop: 2, color: "#6b7280" }}>
                        备注: {s.note}
                      </div>
                    )}
                  </div>
                  <div>
                    {s.cost_price && s.quantity ? (
                      <>
                        <div>成本: ¥{formatNumber(s.cost_price, 3)}</div>
                        <div style={{ marginTop: 2 }}>
                          数量: {s.quantity} 股
                        </div>
                      </>
                    ) : (
                      <div style={{ color: "#6b7280" }}>未设置持仓</div>
                    )}
                  </div>
                  <div>
                    {s.auto_monitor ? (
                      <span style={{ color: "#16a34a" }}>🔔 自动监测</span>
                    ) : (
                      <span style={{ color: "#6b7280" }}>🔕 不监测</span>
                    )}
                  </div>
                  <div>
                    <div style={{ display: "flex", gap: 8 }}>
                      <button
                        type="button"
                        onClick={() => openEdit(s)}
                        style={{
                          padding: "4px 8px",
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
                        onClick={() => handleDeleteStock(s.id, s.code)}
                        style={{
                          padding: "4px 8px",
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
                </div>

                {editingId === s.id && (
                  <div
                    style={{
                      marginTop: 10,
                      paddingTop: 10,
                      borderTop: "1px dashed #e5e7eb",
                      fontSize: 13,
                    }}
                  >
                    <div
                      style={{
                        display: "grid",
                        gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
                        gap: 10,
                      }}
                    >
                      <div>
                        <label>
                          成本价
                          <input
                            type="number"
                            value={editCost === "" ? "" : editCost}
                            onChange={(e) =>
                              setEditCost(
                                e.target.value === ""
                                  ? ""
                                  : Number(e.target.value),
                              )
                            }
                            step="0.001"
                            style={{
                              marginTop: 4,
                              width: "100%",
                              padding: "6px 8px",
                              borderRadius: 8,
                              border: "1px solid #ddd",
                            }}
                          />
                        </label>
                        <label
                          style={{
                            marginTop: 8,
                            display: "flex",
                            alignItems: "center",
                            gap: 6,
                          }}
                        >
                          <input
                            type="checkbox"
                            checked={editAutoMonitor}
                            onChange={(e) =>
                              setEditAutoMonitor(e.target.checked)
                            }
                          />
                          自动同步到监测
                        </label>
                      </div>
                      <div>
                        <label>
                          备注
                          <textarea
                            value={editNote}
                            onChange={(e) => setEditNote(e.target.value)}
                            style={{
                              marginTop: 4,
                              width: "100%",
                              padding: 8,
                              borderRadius: 8,
                              border: "1px solid #ddd",
                              minHeight: 64,
                            }}
                          />
                        </label>
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
                        {savingEdit ? "保存中..." : "保存"}
                      </button>
                    </div>
                  </div>
                )}
              </div>
            ))}
          </section>
        </section>
      )}

      {activeTab === "batch" && (
        <section>
          <h2 style={{ fontSize: 18, marginTop: 0 }}>🔄 批量分析持仓股票</h2>
          {stocks.length === 0 ? (
            <p style={{ fontSize: 13, color: "#666" }}>
              暂无持仓股票，请先在“持仓管理”中添加。
            </p>
          ) : (
            <>
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
                  gap: 12,
                  marginBottom: 12,
                  fontSize: 13,
                }}
              >
                <div
                  style={{
                    background: "#fff",
                    borderRadius: 10,
                    padding: 10,
                    boxShadow: "0 2px 6px rgba(0,0,0,0.05)",
                  }}
                >
                  <div>持仓股票数</div>
                  <div style={{ marginTop: 4, fontSize: 18 }}>{stats.total}</div>
                </div>
                <div
                  style={{
                    background: "#fff",
                    borderRadius: 10,
                    padding: 10,
                    boxShadow: "0 2px 6px rgba(0,0,0,0.05)",
                  }}
                >
                  <div>分析模式</div>
                  <select
                    value={batchMode}
                    onChange={(e) =>
                      setBatchMode(e.target.value as "sequential" | "parallel")
                    }
                    style={{ marginTop: 4, width: "100%" }}
                  >
                    <option value="sequential">顺序分析</option>
                    <option value="parallel">并行分析</option>
                  </select>
                </div>
                <div
                  style={{
                    background: "#fff",
                    borderRadius: 10,
                    padding: 10,
                    boxShadow: "0 2px 6px rgba(0,0,0,0.05)",
                  }}
                >
                  <div>并行线程数</div>
                  <input
                    type="number"
                    min={2}
                    max={10}
                    value={batchMaxWorkers}
                    disabled={batchMode === "sequential"}
                    onChange={(e) =>
                      setBatchMaxWorkers(Number(e.target.value) || 3)
                    }
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

              <div
                style={{
                  display: "flex",
                  gap: 12,
                  alignItems: "center",
                  fontSize: 13,
                  marginBottom: 12,
                }}
              >
                <label>
                  <input
                    type="checkbox"
                    checked={batchAutoSync}
                    onChange={(e) => setBatchAutoSync(e.target.checked)}
                  />
                  &nbsp;自动同步到监测
                </label>
                <label>
                  <input
                    type="checkbox"
                    checked={batchSendNotification}
                    onChange={(e) =>
                      setBatchSendNotification(e.target.checked)
                    }
                  />
                  &nbsp;发送完成通知
                </label>
                <button
                  type="button"
                  disabled={batchRunning}
                  onClick={handleBatchAnalyze}
                  style={{
                    padding: "6px 12px",
                    borderRadius: 999,
                    border: "none",
                    background:
                      "linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%)",
                    color: "#fff",
                    cursor: batchRunning ? "default" : "pointer",
                    fontWeight: 600,
                  }}
                >
                  {batchRunning ? "正在分析..." : "🚀 立即开始分析"}
                </button>
              </div>

              {batchResult && (
                <div
                  style={{
                    marginTop: 8,
                    background: "#fff",
                    borderRadius: 12,
                    padding: 12,
                    boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
                    fontSize: 13,
                  }}
                >
                  <h3 style={{ marginTop: 0, fontSize: 15 }}>分析结果概览</h3>
                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
                      gap: 8,
                    }}
                  >
                    <div>
                      <div>总计</div>
                      <div style={{ marginTop: 2, fontSize: 18 }}>
                        {batchResult.total}
                      </div>
                    </div>
                    <div>
                      <div>成功</div>
                      <div style={{ marginTop: 2, fontSize: 18 }}>
                        {batchResult.succeeded}
                      </div>
                    </div>
                    <div>
                      <div>失败</div>
                      <div style={{ marginTop: 2, fontSize: 18 }}>
                        {batchResult.failed}
                      </div>
                    </div>
                    <div>
                      <div>耗时</div>
                      <div style={{ marginTop: 2, fontSize: 18 }}>
                        {batchResult.elapsed_time.toFixed(1)} 秒
                      </div>
                    </div>
                  </div>
                  {typeof batchResult.saved_count === "number" && (
                    <p style={{ marginTop: 8 }}>
                      💾 已保存 {batchResult.saved_count} 条分析记录到数据库
                    </p>
                  )}
                  {batchResult.sync_result && (
                    <p style={{ marginTop: 4 }}>
                      📊 监测同步: 新增 {batchResult.sync_result.added} 只, 更新
                      {batchResult.sync_result.updated} 只
                    </p>
                  )}
                </div>
              )}
            </>
          )}
        </section>
      )}

      {activeTab === "scheduler" && (
        <section>
          <h2 style={{ fontSize: 18, marginTop: 0 }}>⏰ 持仓定时任务</h2>
          <p style={{ fontSize: 13, color: "#666", marginBottom: 8 }}>
            配置每天固定时点自动执行持仓批量分析，并可选择是否自动同步到监测、发送通知。
          </p>

          {loadingScheduler && (
            <p style={{ fontSize: 13, color: "#666" }}>正在读取调度器状态...</p>
          )}

          {scheduler && (
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
                gap: 12,
                marginBottom: 12,
                fontSize: 13,
              }}
            >
              <div
                style={{
                  background: "#fff",
                  borderRadius: 10,
                  padding: 10,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.05)",
                }}
              >
                <div>当前状态</div>
                <div style={{ marginTop: 4, fontSize: 18 }}>
                  {scheduler.is_running ? "运行中" : "已停止"}
                </div>
              </div>
              <div
                style={{
                  background: "#fff",
                  borderRadius: 10,
                  padding: 10,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.05)",
                }}
              >
                <div>最近一次执行</div>
                <div style={{ marginTop: 4 }}>
                  {formatDateTime(scheduler.last_run_time)}
                </div>
              </div>
              <div
                style={{
                  background: "#fff",
                  borderRadius: 10,
                  padding: 10,
                  boxShadow: "0 2px 6px rgba(0,0,0,0.05)",
                }}
              >
                <div>下次执行时间</div>
                <div style={{ marginTop: 4 }}>
                  {formatDateTime(scheduler.next_run_time)}
                </div>
              </div>
            </div>
          )}

          <div
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 12,
              boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
              fontSize: 13,
            }}
          >
            <h3 style={{ marginTop: 0, fontSize: 15 }}>调度配置</h3>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
                gap: 12,
              }}
            >
              <div>
                <div style={{ marginBottom: 4 }}>分析模式</div>
                <select
                  value={schedMode}
                  onChange={(e) =>
                    setSchedMode(e.target.value as "sequential" | "parallel")
                  }
                  style={{ width: "100%" }}
                >
                  <option value="sequential">顺序分析</option>
                  <option value="parallel">并行分析</option>
                </select>
              </div>
              <div>
                <div style={{ marginBottom: 4 }}>并行线程数</div>
                <input
                  type="number"
                  min={2}
                  max={10}
                  disabled={schedMode === "sequential"}
                  value={schedMaxWorkers}
                  onChange={(e) =>
                    setSchedMaxWorkers(Number(e.target.value) || 3)
                  }
                  style={{
                    width: "100%",
                    padding: "6px 8px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                  }}
                />
              </div>
            </div>

            <div
              style={{
                marginTop: 10,
                display: "flex",
                flexWrap: "wrap",
                gap: 12,
                alignItems: "center",
              }}
            >
              <label>
                <input
                  type="checkbox"
                  checked={schedAutoSync}
                  onChange={(e) => setSchedAutoSync(e.target.checked)}
                />
                &nbsp;分析后自动同步到监测
              </label>
              <label>
                <input
                  type="checkbox"
                  checked={schedSendNotification}
                  onChange={(e) => setSchedSendNotification(e.target.checked)}
                />
                &nbsp;发送分析完成通知
              </label>
            </div>

            <div style={{ marginTop: 10 }}>
              <div style={{ fontWeight: 500, marginBottom: 4 }}>每天执行时间</div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
                {schedTimes.map((t) => (
                  <span
                    key={t}
                    style={{
                      padding: "4px 8px",
                      borderRadius: 999,
                      border: "1px solid #ddd",
                      fontSize: 12,
                    }}
                  >
                    {t}
                    <button
                      type="button"
                      onClick={() => handleRemoveTime(t)}
                      style={{
                        marginLeft: 6,
                        border: "none",
                        background: "transparent",
                        cursor: "pointer",
                      }}
                    >
                      ×
                    </button>
                  </span>
                ))}
              </div>
              <div style={{ marginTop: 6, display: "flex", gap: 8 }}>
                <input
                  type="time"
                  value={schedNewTime}
                  onChange={(e) => setSchedNewTime(e.target.value)}
                  style={{
                    padding: "4px 8px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                  }}
                />
                <button
                  type="button"
                  disabled={savingScheduler}
                  onClick={handleAddTime}
                  style={{
                    padding: "4px 10px",
                    borderRadius: 8,
                    border: "1px solid #d4d4d4",
                    background: "#fff",
                    cursor: savingScheduler ? "default" : "pointer",
                  }}
                >
                  添加时间
                </button>
              </div>
            </div>

            <div
              style={{
                marginTop: 12,
                display: "flex",
                gap: 10,
                flexWrap: "wrap",
              }}
            >
              <button
                type="button"
                disabled={savingScheduler}
                onClick={() => saveSchedulerConfig()}
                style={{
                  padding: "6px 12px",
                  borderRadius: 8,
                  border: "none",
                  background:
                    "linear-gradient(135deg, #6366f1 0%, #4f46e5 100%)",
                  color: "#fff",
                  cursor: savingScheduler ? "default" : "pointer",
                }}
              >
                保存配置
              </button>
              <button
                type="button"
                disabled={savingScheduler}
                onClick={handleStartScheduler}
                style={{
                  padding: "6px 12px",
                  borderRadius: 8,
                  border: "1px solid #22c55e",
                  background: "#dcfce7",
                  color: "#166534",
                  cursor: savingScheduler ? "default" : "pointer",
                }}
              >
                启动定时任务
              </button>
              <button
                type="button"
                disabled={savingScheduler}
                onClick={handleStopScheduler}
                style={{
                  padding: "6px 12px",
                  borderRadius: 8,
                  border: "1px solid #fecaca",
                  background: "#fef2f2",
                  color: "#b91c1c",
                  cursor: savingScheduler ? "default" : "pointer",
                }}
              >
                停止定时任务
              </button>
              <button
                type="button"
                disabled={savingScheduler}
                onClick={handleRunOnce}
                style={{
                  padding: "6px 12px",
                  borderRadius: 8,
                  border: "1px solid #d4d4d4",
                  background: "#fff",
                  cursor: savingScheduler ? "default" : "pointer",
                }}
              >
                立即执行一次
              </button>
            </div>
          </div>
        </section>
      )}

      {activeTab === "history" && (
        <section>
          <h2 style={{ fontSize: 18, marginTop: 0 }}>📈 持仓分析历史</h2>
          <p style={{ fontSize: 13, color: "#666", marginBottom: 8 }}>
            查看最近的持仓分析记录，可以按单只股票查看详细历史，也可以查看全部最新分析概览。
          </p>

          <div
            style={{
              display: "flex",
              gap: 8,
              flexWrap: "wrap",
              marginBottom: 8,
              fontSize: 13,
            }}
          >
            <button
              type="button"
              onClick={() => handleChangeHistoryTab("全部")}
              style={{
                padding: "4px 10px",
                borderRadius: 999,
                border: "1px solid #d4d4d4",
                background: historyTabKey === "全部" ? "#e5e7eb" : "#fff",
              }}
            >
              全部
            </button>
            {stocks.map((s) => (
              <button
                key={s.code}
                type="button"
                onClick={() => handleChangeHistoryTab(s.code)}
                style={{
                  padding: "4px 10px",
                  borderRadius: 999,
                  border: "1px solid #d4d4d4",
                  background:
                    historyTabKey === s.code ? "#e5e7eb" : "#fff",
                }}
              >
                {s.code} {s.name}
              </button>
            ))}
          </div>

          {loadingHistory && (
            <p style={{ fontSize: 13, color: "#666" }}>正在加载分析历史...</p>
          )}
          {historyError && (
            <p style={{ fontSize: 13, color: "#b00020" }}>错误：{historyError}</p>
          )}

          {!loadingHistory && historyRecords.length === 0 && (
            <p style={{ fontSize: 13, color: "#666" }}>暂无历史记录。</p>
          )}

          {historyRecords.length > 0 && (
            <table
              style={{
                width: "100%",
                borderCollapse: "collapse",
                fontSize: 12,
                marginTop: 4,
              }}
            >
              <thead>
                <tr>
                  <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #e5e7eb" }}>
                    股票
                  </th>
                  <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #e5e7eb" }}>
                    时间
                  </th>
                  <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #e5e7eb" }}>
                    评级
                  </th>
                  <th style={{ textAlign: "right", padding: 6, borderBottom: "1px solid #e5e7eb" }}>
                    置信度
                  </th>
                  <th style={{ textAlign: "right", padding: 6, borderBottom: "1px solid #e5e7eb" }}>
                    当前价
                  </th>
                  <th style={{ textAlign: "right", padding: 6, borderBottom: "1px solid #e5e7eb" }}>
                    目标价
                  </th>
                  <th style={{ textAlign: "left", padding: 6, borderBottom: "1px solid #e5e7eb" }}>
                    摘要
                  </th>
                </tr>
              </thead>
              <tbody>
                {historyRecords.map((r) => (
                  <tr key={r.id}>
                    <td style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}>
                      {r.code} {r.name}
                    </td>
                    <td style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}>
                      {formatDateTime(r.analysis_time)}
                    </td>
                    <td style={{ padding: 6, borderBottom: "1px solid #f3f4f6" }}>
                      {r.rating}
                    </td>
                    <td
                      style={{
                        padding: 6,
                        borderBottom: "1px solid #f3f4f6",
                        textAlign: "right",
                      }}
                    >
                      {formatNumber(r.confidence, 1)}%
                    </td>
                    <td
                      style={{
                        padding: 6,
                        borderBottom: "1px solid #f3f4f6",
                        textAlign: "right",
                      }}
                    >
                      {formatNumber(r.current_price, 2)}
                    </td>
                    <td
                      style={{
                        padding: 6,
                        borderBottom: "1px solid #f3f4f6",
                        textAlign: "right",
                      }}
                    >
                      {formatNumber(r.target_price, 2)}
                    </td>
                    <td
                      style={{
                        padding: 6,
                        borderBottom: "1px solid #f3f4f6",
                        maxWidth: 260,
                      }}
                    >
                      {r.summary}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>
      )}

      </main>
  );
}
