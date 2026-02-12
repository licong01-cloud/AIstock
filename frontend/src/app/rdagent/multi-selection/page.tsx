"use client";

import { useEffect, useRef, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface StrategyCatalogItem {
  strategy_id: string;
  display_name?: string | null;
  scenario: string | null;
  step_name: string | null;
  action: string | null;
  workspace_example?: {
    task_run_id: string | null;
    loop_id: number | null;
    workspace_id: string | null;
  } | null;
  model_config?: any;
}

interface InferenceResult {
  symbol: string;
  score: number;
  rank: number;
  strategy_id: string;
  name?: string | null;
  price?: number | null;
  pct_change?: number | null;
  quote_source?: string | null;
  quote_time?: string | null;
}

interface LoopSelectionMeta {
  as_of_date?: string | null;
  top_k?: number | null;
  inference_meta?: {
    asset_bundle_id?: string | null;
    freq?: string | null;
    universe_size?: number | null;
    universe_preview?: string[] | null;
    use_config_universe?: boolean | null;
    source?: string | null;
    data_dates?: {
      kline_daily_raw?: string | null;
      daily_basic?: string | null;
      moneyflow_ts?: string | null;
    } | null;
  } | null;
}

interface LoopSelectionItem {
  task_run_id: string;
  loop_id: number;
  strategy_id?: string | null;
  display_name?: string | null;
}

interface Category {
  id: number;
  name: string;
}

export default function MultiSelectionPage() {
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [selectedLoops, setSelectedLoops] = useState<LoopSelectionItem[]>([]);
  const [strategies, setStrategies] = useState<StrategyCatalogItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [inferenceLoading, setInferenceLoading] = useState<Record<string, boolean>>({});
  const [results, setResults] = useState<Record<string, InferenceResult[]>>({});
  const [loopMetas, setLoopMetas] = useState<Record<string, LoopSelectionMeta>>({});
  const [selectedStocks, setSelectedStocks] = useState<Record<string, string[]>>({}); // strategy_id -> symbol[]

  const [loopLogs, setLoopLogs] = useState<Record<string, string[]>>({});
  const [loopLogOpen, setLoopLogOpen] = useState<Record<string, boolean>>({});
  const loopEventSourcesRef = useRef<Record<string, EventSource | null>>({});
  
  // Watchlist Modal
  const [showWatchlistModal, setShowWatchlistModal] = useState(false);
  const [categories, setCategories] = useState<Category[]>([]);
  const [selectedCategoryId, setSelectedCategoryId] = useState<string>("");
  const [newCategoryName, setNewCategoryName] = useState("");
  const [currentAddingStocks, setCurrentAddingStocks] = useState<{symbol: string, strategy_id: string}[]>([]);

  useEffect(() => {
    loadSelectedIds();
    loadSelectedLoops();
    loadCategories();
  }, []);

  useEffect(() => {
    if (selectedIds.length > 0) {
      loadStrategies();
    } else {
      setLoading(false);
    }
  }, [selectedIds]);

  useEffect(() => {
    if (selectedIds.length === 0 && selectedLoops.length > 0) {
      setLoading(false);
    }
  }, [selectedLoops, selectedIds]);

  function loadSelectedIds() {
    const stored = localStorage.getItem("aistock_inference_strategies");
    if (stored) {
      try {
        const arr = JSON.parse(stored);
        if (Array.isArray(arr)) {
          setSelectedIds(arr.map((x) => String(x)).filter((x) => x));
        } else {
          setSelectedIds([]);
        }
      } catch (e) {
        console.error("加载选定策略 ID 失败", e);
      }
    }
  }

  function loadSelectedLoops() {
    const stored = localStorage.getItem("aistock_inference_loops");
    if (stored) {
      try {
        const arr = JSON.parse(stored);
        if (Array.isArray(arr)) {
          const normalized: LoopSelectionItem[] = arr
            .filter((x) => x && x.task_run_id && x.loop_id != null)
            .map((x) => ({
              task_run_id: String(x.task_run_id),
              loop_id: Number(x.loop_id),
              strategy_id: x.strategy_id != null ? String(x.strategy_id) : null,
              display_name: x.display_name != null ? String(x.display_name) : null,
            }));
          setSelectedLoops(normalized);
          return;
        }
      } catch (e) {
        console.error("加载选定 loop 失败", e);
      }
    }
    setSelectedLoops([]);
  }

  async function loadStrategies() {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/rdagent/catalogs/strategies?limit=500`);
      const data = await res.json();
      const allItems: StrategyCatalogItem[] = data.items || [];
      const filtered = allItems.filter(item => selectedIds.includes(item.strategy_id));
      setStrategies(filtered);
    } catch (e) {
      console.error("加载策略详情失败", e);
    } finally {
      setLoading(false);
    }
  }

  async function loadCategories() {
    try {
      const res = await fetch(`${API_BASE}/watchlist/categories`);
      if (res.ok) {
        const data = await res.json();
        setCategories(data);
      }
    } catch (e) {
      console.error("加载分类失败", e);
    }
  }

  function removeFromSelection(id: string) {
    const next = selectedIds.filter(sid => sid !== id);
    setSelectedIds(next);
    localStorage.setItem("aistock_inference_strategies", JSON.stringify(next));
  }

  function removeLoopFromSelection(taskRunId: string, loopId: number) {
    const key = "aistock_inference_loops";
    const next = selectedLoops.filter(
      (x) => !(x.task_run_id === taskRunId && x.loop_id === loopId),
    );
    setSelectedLoops(next);
    localStorage.setItem(key, JSON.stringify(next));
  }

  function loopKey(item: LoopSelectionItem) {
    return `loop:${item.task_run_id}:${item.loop_id}`;
  }

  function noteForSourceId(sourceId: string) {
    if (sourceId.startsWith("loop:")) {
      const parts = sourceId.split(":");
      if (parts.length >= 3) {
        return `来自 loop ${parts[1]}/${parts[2]}`;
      }
      return `来自 loop ${sourceId}`;
    }
    return `来自策略 ${sourceId}`;
  }

  function noteForSourceIdWithDisplay(sourceId: string) {
    if (sourceId.startsWith("loop:")) {
      const parts = sourceId.split(":");
      if (parts.length >= 3) {
        const tr = parts[1];
        const lid = Number(parts[2]);
        const found = selectedLoops.find(
          (x) => x.task_run_id === tr && Number(x.loop_id) === lid,
        );
        if (found) {
          return `来自 loop ${found.display_name || `${found.task_run_id}/${found.loop_id}`}`;
        }
        return `来自 loop ${tr}/${parts[2]}`;
      }
    }
    return noteForSourceId(sourceId);
  }

  async function runInference(strategyId: string) {
    setInferenceLoading(prev => ({ ...prev, [strategyId]: true }));
    try {
      const res = await fetch(`${API_BASE}/rdagent/strategies/${strategyId}/inference`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ version_tag: "v1" }),
      });
      if (!res.ok) throw new Error("推理失败");
      
      const vRes = await fetch(`${API_BASE}/rdagent/strategies/${strategyId}/versions`);
      const vData = await vRes.json();
      if (vData.items?.length > 0) {
        const latestVersionId = vData.items[0].strategy_version_id;
        const today = new Date().toISOString().split("T")[0];
        const sRes = await fetch(`${API_BASE}/rdagent/signals/by_date?strategy_version_id=${latestVersionId}&trade_date=${today}&k=50`);
        const sData = await sRes.json();
        const rows = (sData.rows || []).map((r: any) => ({ ...r, strategy_id: strategyId }));
        setResults(prev => ({ ...prev, [strategyId]: rows }));
      }
    } catch (e) {
      console.error(`推理失败: ${strategyId}`, e);
      alert(`${strategyId} 推理失败: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setInferenceLoading(prev => ({ ...prev, [strategyId]: false }));
    }
  }

  async function runLoopSelection(item: LoopSelectionItem) {
    const key = loopKey(item);
    setInferenceLoading((prev) => ({ ...prev, [key]: true }));

    try {
      const old = loopEventSourcesRef.current[key];
      if (old) {
        old.close();
        loopEventSourcesRef.current[key] = null;
      }
    } catch {}

    setLoopLogs((prev) => ({ ...prev, [key]: [] }));

    try {
      const url = `${API_BASE}/rdagent/loops/${encodeURIComponent(item.task_run_id)}/${item.loop_id}/selection/stream?top_k=50`;
      const es = new EventSource(url);
      loopEventSourcesRef.current[key] = es;

      const pushLog = (line: string) => {
        setLoopLogs((prev) => {
          const cur = prev[key] || [];
          const next = cur.length > 2000 ? cur.slice(cur.length - 1500) : cur;
          return { ...prev, [key]: [...next, line] };
        });
      };

      es.addEventListener("start", (ev: any) => {
        try {
          pushLog(`SSE start: ${ev?.data || ""}`);
        } catch {}
      });

      es.addEventListener("log", (ev: any) => {
        pushLog(String(ev?.data || ""));
      });

      es.addEventListener("error", (ev: any) => {
        try {
          const data = String(ev?.data || "");
          pushLog(`SSE error: ${data}`);
          let detail = "选股失败";
          try {
            const obj = JSON.parse(data);
            detail = obj?.detail || detail;
          } catch {
            if (data) detail = data;
          }
          alert(`loop ${item.task_run_id}/${item.loop_id} 选股失败: ${detail}`);
        } catch {
          alert(`loop ${item.task_run_id}/${item.loop_id} 选股失败`);
        } finally {
          try {
            es.close();
          } catch {}
          loopEventSourcesRef.current[key] = null;
          setInferenceLoading((prev) => ({ ...prev, [key]: false }));
        }
      });

      es.addEventListener("result", (ev: any) => {
        try {
          const obj = JSON.parse(String(ev?.data || "{}"));
          const data = obj;
          const payload = data?.ok ? data : data;
          const asOf = payload?.as_of || payload?.as_of_date || null;

          setLoopMetas((prev) => ({
            ...prev,
            [key]: {
              as_of_date: asOf,
              top_k: payload?.top_k ?? null,
              inference_meta: payload?.inference_meta ?? null,
            },
          }));

          const items = payload?.items || payload?.rows || [];
          const rows = (items || []).map((r: any) => ({
            symbol: r.symbol,
            score: r.score,
            rank: r.rank,
            strategy_id: key,
            name: r.name,
            price: r.price,
            pct_change: r.pct_change,
            quote_source: r.quote_source,
            quote_time: r.quote_time,
          }));
          setResults((prev) => ({ ...prev, [key]: rows }));
          pushLog("SSE result received");
        } catch (e) {
          pushLog(`SSE result parse failed: ${e instanceof Error ? e.message : String(e)}`);
        } finally {
          try {
            es.close();
          } catch {}
          loopEventSourcesRef.current[key] = null;
          setInferenceLoading((prev) => ({ ...prev, [key]: false }));
        }
      });

      es.addEventListener("ping", () => {});
    } catch (e) {
      console.error(`loop 选股失败: ${item.task_run_id}/${item.loop_id}`, e);
      alert(
        `loop ${item.task_run_id}/${item.loop_id} 选股失败: ${e instanceof Error ? e.message : String(e)}`,
      );
      setInferenceLoading((prev) => ({ ...prev, [key]: false }));
    }
  }

  async function runAllInference() {
    strategies.forEach((s) => runInference(s.strategy_id));
    selectedLoops.forEach((lp) => runLoopSelection(lp));
  }

  function toggleStockSelection(strategyId: string, symbol: string) {
    setSelectedStocks(prev => {
      const current = prev[strategyId] || [];
      const next = current.includes(symbol)
        ? current.filter(s => s !== symbol)
        : [...current, symbol];
      return { ...prev, [strategyId]: next };
    });
  }

  function toggleAllStocks(strategyId: string) {
    const strategyResults = results[strategyId] || [];
    const current = selectedStocks[strategyId] || [];
    const next = current.length === strategyResults.length
      ? []
      : strategyResults.map(r => r.symbol);
    setSelectedStocks(prev => ({ ...prev, [strategyId]: next }));
  }

  function handleBatchAddToWatchlist() {
    const allToAdd: {symbol: string, strategy_id: string}[] = [];
    Object.entries(selectedStocks).forEach(([sid, symbols]) => {
      symbols.forEach(symbol => {
        allToAdd.push({ symbol, strategy_id: sid });
      });
    });

    if (allToAdd.length === 0) {
      alert("请先选择要加入自选的股票");
      return;
    }

    setCurrentAddingStocks(allToAdd);
    setShowWatchlistModal(true);
  }

  async function executeAddToWatchlist() {
    let targetCatId: number | null = null;

    try {
      if (newCategoryName.trim()) {
        const res = await fetch(`${API_BASE}/watchlist/categories`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name: newCategoryName.trim() }),
        });
        const data = await res.json();
        targetCatId = data.id;
        await loadCategories();
      } else if (selectedCategoryId) {
        targetCatId = parseInt(selectedCategoryId);
      }

      if (!targetCatId) {
        alert("请选择或新建一个分类");
        return;
      }

      let successCount = 0;
      for (const item of currentAddingStocks) {
        const res = await fetch(`${API_BASE}/watchlist/items/add`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            code: item.symbol,
            category_id: targetCatId,
            note: noteForSourceIdWithDisplay(item.strategy_id),
          }),
        });
        if (res.ok) successCount++;
      }

      alert(`成功添加 ${successCount} 只股票到自选股池`);
      setShowWatchlistModal(false);
      setSelectedStocks({});
      setNewCategoryName("");
      setSelectedCategoryId("");
    } catch (e) {
      console.error("添加到自选失败", e);
      alert("添加失败，请重试");
    }
  }

  if (loading) {
    return <div style={{ padding: 40, textAlign: "center", color: "#6b7280" }}>正在加载选定策略...</div>;
  }

  return (
    <main style={{ padding: 24, maxWidth: 1400, margin: "0 auto" }}>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <button 
            onClick={() => window.location.href = '/rdagent/strategies-catalog'}
            style={{ 
              padding: 8, background: "none", border: "none", cursor: "pointer", 
              borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center" 
            }}
            title="返回"
          >
            <span style={{ fontSize: 24 }}>←</span>
          </button>
          <div>
            <h1 style={{ margin: 0, fontSize: 24, fontWeight: "bold", color: "#111827" }}>多策略选股中心</h1>
            <p style={{ margin: "4px 0 0", fontSize: 14, color: "#6b7280" }}>对比不同实验版本的选股结果，一键跟踪实战表现</p>
          </div>
        </div>
        <div style={{ display: "flex", gap: 12 }}>
          <button
            onClick={runAllInference}
            style={{
              padding: "10px 20px", background: "#4f46e5", color: "#fff", border: "none",
              borderRadius: 8, cursor: "pointer", fontWeight: 500, boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
            }}
          >
            ⚡ 执行全量选股
          </button>
          <button
            onClick={handleBatchAddToWatchlist}
            style={{
              padding: "10px 20px", background: "#10b981", color: "#fff", border: "none",
              borderRadius: 8, cursor: "pointer", fontWeight: 500, boxShadow: "0 1px 2px rgba(0,0,0,0.05)"
            }}
          >
            ⭐ 一键加入自选
          </button>
        </div>
      </div>

      {selectedIds.length === 0 && selectedLoops.length === 0 ? (
        <div style={{ 
          padding: "80px 20px", background: "#fff", border: "2px dashed #e5e7eb", 
          borderRadius: 16, textAlign: "center" 
        }}>
          <div style={{ fontSize: 48, marginBottom: 16, opacity: 0.3 }}>📊</div>
          <h3 style={{ fontSize: 18, fontWeight: 500, color: "#111827", margin: 0 }}>尚未选择任何策略</h3>
          <p style={{ color: "#6b7280", marginTop: 8 }}>前往策略目录，点击“添加到选股”按钮，即可在此处进行批量推理与对比。</p>
          <button 
            onClick={() => window.location.href = '/rdagent/strategies-catalog'}
            style={{ 
              marginTop: 24, padding: "10px 24px", background: "#111827", color: "#fff", 
              border: "none", borderRadius: 999, cursor: "pointer" 
            }}
          >
            去策略目录看看
          </button>
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
          {selectedLoops.map((lp) => {
            const k = loopKey(lp);
            const title = lp.display_name || "loop";
            const sub = `${lp.task_run_id}/${lp.loop_id}`;
            const meta = loopMetas[k];
            const inferMeta = meta?.inference_meta;
            const dataDates = inferMeta?.data_dates;
            const logs = loopLogs[k] || [];
            const logOpen = !!loopLogOpen[k];
            return (
              <div key={k} style={{ 
                background: "#fff", border: "1px solid #e5e7eb", borderRadius: 12, 
                boxShadow: "0 1px 3px rgba(0,0,0,0.05)", overflow: "hidden", display: "flex", flexDirection: "column" 
              }}>
                <div style={{ 
                  padding: 16, borderBottom: "1px solid #f3f4f6", display: "flex", 
                  alignItems: "center", justifyContent: "space-between", background: "#f9fafb" 
                }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                    <div style={{ 
                      width: 40, height: 40, background: "#dcfce7", borderRadius: 8, 
                      display: "flex", alignItems: "center", justifyContent: "center", fontSize: 20 
                    }}>🧪</div>
                    <div>
                      <h3 style={{ margin: 0, fontSize: 16, fontWeight: "bold", color: "#111827" }}>{title}</h3>
                      <div style={{ fontSize: 12, color: "#6b7280", marginTop: 2 }}>{sub}</div>
                      {lp.strategy_id && (
                        <div style={{ fontSize: 12, color: "#6b7280", marginTop: 2 }}>
                          strategy_id: {lp.strategy_id}
                        </div>
                      )}
                    </div>
                  </div>
                  <div style={{ display: "flex", gap: 8 }}>
                    <button 
                      onClick={() => runLoopSelection(lp)}
                      disabled={inferenceLoading[k]}
                      style={{ 
                        padding: 8, color: "#6b7280", border: "none", background: "none", cursor: "pointer",
                        borderRadius: 6
                      }}
                      title="执行选股"
                    >
                      {inferenceLoading[k] ? "⌛" : "▶️"}
                    </button>
                    <button
                      onClick={() => setLoopLogOpen((prev) => ({ ...prev, [k]: !logOpen }))}
                      style={{
                        padding: 8,
                        color: logOpen ? "#111827" : "#6b7280",
                        border: "none",
                        background: "none",
                        cursor: "pointer",
                        borderRadius: 6,
                      }}
                      title="展开/收起选股日志"
                    >
                      {logOpen ? "🧾" : "📄"}
                    </button>
                    <button 
                      onClick={() => removeLoopFromSelection(lp.task_run_id, lp.loop_id)}
                      style={{ 
                        padding: 8, color: "#ef4444", border: "none", background: "none", cursor: "pointer",
                        borderRadius: 6
                      }}
                      title="移除"
                    >
                      🗑️
                    </button>
                  </div>
                </div>
                <div style={{ padding: 12, borderBottom: "1px solid #f3f4f6", background: "#ffffff" }}>
                  <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 6 }}>本次选股数据元信息</div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 12, fontSize: 12, color: "#111827" }}>
                    <div>as_of: <span style={{ fontFamily: "monospace" }}>{meta?.as_of_date || "-"}</span></div>
                    <div>freq: <span style={{ fontFamily: "monospace" }}>{inferMeta?.freq || "-"}</span></div>
                    <div>bundle: <span style={{ fontFamily: "monospace" }}>{inferMeta?.asset_bundle_id || "-"}</span></div>
                    <div>universe_size: <span style={{ fontFamily: "monospace" }}>{inferMeta?.universe_size != null ? String(inferMeta.universe_size) : "-"}</span></div>
                    <div>use_config_universe: <span style={{ fontFamily: "monospace" }}>{inferMeta?.use_config_universe != null ? String(inferMeta.use_config_universe) : "-"}</span></div>
                    <div>kline_latest: <span style={{ fontFamily: "monospace" }}>{dataDates?.kline_daily_raw || "-"}</span></div>
                    <div>daily_basic_latest: <span style={{ fontFamily: "monospace" }}>{dataDates?.daily_basic || "-"}</span></div>
                    <div>moneyflow_latest: <span style={{ fontFamily: "monospace" }}>{dataDates?.moneyflow_ts || "-"}</span></div>
                  </div>
                  {inferMeta?.universe_preview && inferMeta.universe_preview.length > 0 && (
                    <div style={{ marginTop: 6, fontSize: 12, color: "#6b7280" }}>
                      universe_preview: <span style={{ fontFamily: "monospace" }}>{inferMeta.universe_preview.join(", ")}</span>
                    </div>
                  )}
                </div>

                {logOpen && (
                  <div style={{ padding: 12, borderBottom: "1px solid #f3f4f6", background: "#0b1020" }}>
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                      <div style={{ fontSize: 12, color: "#cbd5e1" }}>选股日志（实时）</div>
                      <button
                        onClick={() => setLoopLogs((prev) => ({ ...prev, [k]: [] }))}
                        style={{
                          padding: "4px 8px",
                          fontSize: 12,
                          color: "#e2e8f0",
                          border: "1px solid rgba(148,163,184,0.35)",
                          background: "rgba(15,23,42,0.6)",
                          borderRadius: 6,
                          cursor: "pointer",
                        }}
                      >
                        清空
                      </button>
                    </div>
                    <div style={{ marginTop: 8, maxHeight: 220, overflow: "auto", fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace", fontSize: 12, lineHeight: 1.4, color: "#e2e8f0" }}>
                      {logs.length === 0 ? (
                        <div style={{ color: "#94a3b8" }}>暂无日志。点击上方 ▶️ 执行选股后会实时输出。</div>
                      ) : (
                        logs.map((ln, idx) => (
                          <div key={idx} style={{ whiteSpace: "pre-wrap", wordBreak: "break-word" }}>
                            {ln}
                          </div>
                        ))
                      )}
                    </div>
                  </div>
                )}
                <div style={{ flex: 1, minHeight: 300, position: "relative" }}>
                  {inferenceLoading[k] && (
                    <div style={{ 
                      position: "absolute", inset: 0, background: "rgba(255,255,255,0.8)", 
                      zIndex: 10, display: "flex", alignItems: "center", justifyContent: "center" 
                    }}>
                      <div style={{ textAlign: "center" }}>
                        <div style={{ 
                          width: 32, height: 32, border: "4px solid #10b981", borderTopColor: "transparent", 
                          borderRadius: "50%", margin: "0 auto 12px", animation: "spin 1s linear infinite" 
                        }}></div>
                        <span style={{ fontSize: 14, fontWeight: 500, color: "#4b5563" }}>正在计算打分...</span>
                      </div>
                    </div>
                  )}
                  {!results[k] ? (
                    <div style={{ padding: "60px 20px", textAlign: "center", color: "#9ca3af" }}>
                      <div style={{ fontSize: 40, marginBottom: 8, opacity: 0.2 }}>🔍</div>
                      <span style={{ fontSize: 14 }}>暂无数据，点击上方执行按钮开始选股</span>
                    </div>
                  ) : (
                    <div style={{ overflow: "auto", maxHeight: 400 }}>
                      <table style={{ width: "100%", borderCollapse: "collapse", textAlign: "left" }}>
                        <thead style={{ position: "sticky", top: 0, background: "#fff", zIndex: 5 }}>
                          <tr style={{ borderBottom: "1px solid #f3f4f6" }}>
                            <th style={{ padding: 12, width: 40 }}>
                              <input 
                                type="checkbox" 
                                aria-label="全选/取消全选"
                                title="全选/取消全选"
                                checked={(selectedStocks[k] || []).length === (results[k] || []).length && (results[k] || []).length > 0}
                                onChange={() => toggleAllStocks(k)}
                              />
                            </th>
                            <th style={{ padding: 12, fontSize: 12, fontWeight: 600, color: "#6b7280" }}>排名</th>
                            <th style={{ padding: 12, fontSize: 12, fontWeight: 600, color: "#6b7280" }}>代码</th>
                            <th style={{ padding: 12, fontSize: 12, fontWeight: 600, color: "#6b7280" }}>名称</th>
                            <th style={{ padding: 12, fontSize: 12, fontWeight: 600, color: "#6b7280", textAlign: "right" }}>最新价</th>
                            <th style={{ padding: 12, fontSize: 12, fontWeight: 600, color: "#6b7280", textAlign: "right" }}>涨跌幅%</th>
                            <th style={{ padding: 12, fontSize: 12, fontWeight: 600, color: "#6b7280", textAlign: "right" }}>预测分数</th>
                          </tr>
                        </thead>
                        <tbody>
                          {results[k].map((r) => {
                            const isSelected = (selectedStocks[k] || []).includes(r.symbol);
                            return (
                              <tr 
                                key={r.symbol} 
                                style={{ 
                                  borderBottom: "1px solid #f9fafb", cursor: "pointer",
                                  background: isSelected ? "#f0fdf4" : "transparent"
                                }}
                                onClick={() => toggleStockSelection(k, r.symbol)}
                              >
                                <td style={{ padding: 12 }} onClick={(e) => e.stopPropagation()}>
                                  <input 
                                    type="checkbox" 
                                    aria-label={`选择 ${r.symbol}`}
                                    title={`选择 ${r.symbol}`}
                                    checked={isSelected}
                                    onChange={() => toggleStockSelection(k, r.symbol)}
                                  />
                                </td>
                                <td style={{ padding: 12 }}>
                                  <span style={{ 
                                    display: "inline-flex", alignItems: "center", justifyContent: "center", 
                                    width: 24, height: 24, borderRadius: "50%", fontSize: 11, fontWeight: "bold",
                                    background: r.rank <= 3 ? "#fef3c7" : "#f3f4f6",
                                    color: r.rank <= 3 ? "#92400e" : "#4b5563"
                                  }}>
                                    {r.rank}
                                  </span>
                                </td>
                                <td style={{ padding: 12, fontSize: 14, fontWeight: 500, color: "#111827", fontFamily: "monospace" }}>{r.symbol}</td>
                                <td style={{ padding: 12, fontSize: 12, color: "#374151" }}>{r.name || "-"}</td>
                                <td style={{ padding: 12, textAlign: "right", fontSize: 12, color: "#111827" }}>{r.price != null ? r.price.toFixed(2) : "-"}</td>
                                <td style={{ padding: 12, textAlign: "right", fontSize: 12, color: (r.pct_change ?? 0) >= 0 ? "#dc2626" : "#2563eb" }}>
                                  {r.pct_change != null ? r.pct_change.toFixed(2) : "-"}
                                </td>
                                <td style={{ padding: 12, textAlign: "right", fontSize: 14, fontWeight: 600, color: "#059669" }}>
                                  {r.score.toFixed(4)}
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              </div>
            );
          })}
          {strategies.map((s) => (
            <div key={s.strategy_id} style={{ 
              background: "#fff", border: "1px solid #e5e7eb", borderRadius: 12, 
              boxShadow: "0 1px 3px rgba(0,0,0,0.05)", overflow: "hidden", display: "flex", flexDirection: "column" 
            }}>
              {/* Card Header */}
              <div style={{ 
                padding: 16, borderBottom: "1px solid #f3f4f6", display: "flex", 
                alignItems: "center", justifyContent: "space-between", background: "#f9fafb" 
              }}>
                <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                  <div style={{ 
                    width: 40, height: 40, background: "#e0e7ff", borderRadius: 8, 
                    display: "flex", alignItems: "center", justifyContent: "center", fontSize: 20 
                  }}>📈</div>
                  <div>
                    <h3 style={{ margin: 0, fontSize: 16, fontWeight: "bold", color: "#111827" }}>{s.display_name || s.strategy_id}</h3>
                    <div style={{ fontSize: 12, color: "#6b7280", marginTop: 2 }}>
                      {s.strategy_id} • {s.scenario} • Loop {s.workspace_example?.loop_id}
                    </div>
                  </div>
                </div>
                <div style={{ display: "flex", gap: 8 }}>
                  <button 
                    onClick={() => runInference(s.strategy_id)}
                    disabled={inferenceLoading[s.strategy_id]}
                    style={{ 
                      padding: 8, color: "#6b7280", border: "none", background: "none", cursor: "pointer",
                      borderRadius: 6, transition: "background 0.2s"
                    }}
                    title="执行推理"
                  >
                    {inferenceLoading[s.strategy_id] ? "⌛" : "▶️"}
                  </button>
                  <button 
                    onClick={() => removeFromSelection(s.strategy_id)}
                    style={{ 
                      padding: 8, color: "#ef4444", border: "none", background: "none", cursor: "pointer",
                      borderRadius: 6
                    }}
                    title="移除"
                  >
                    🗑️
                  </button>
                </div>
              </div>

              {/* Results Area */}
              <div style={{ flex: 1, minHeight: 300, position: "relative" }}>
                {inferenceLoading[s.strategy_id] && (
                  <div style={{ 
                    position: "absolute", inset: 0, background: "rgba(255,255,255,0.8)", 
                    zIndex: 10, display: "flex", alignItems: "center", justifyContent: "center" 
                  }}>
                    <div style={{ textAlign: "center" }}>
                      <div style={{ 
                        width: 32, height: 32, border: "4px solid #4f46e5", borderTopColor: "transparent", 
                        borderRadius: "50%", margin: "0 auto 12px", animation: "spin 1s linear infinite" 
                      }}></div>
                      <span style={{ fontSize: 14, fontWeight: 500, color: "#4b5563" }}>正在计算打分...</span>
                    </div>
                  </div>
                )}

                {!results[s.strategy_id] ? (
                  <div style={{ padding: "60px 20px", textAlign: "center", color: "#9ca3af" }}>
                    <div style={{ fontSize: 40, marginBottom: 8, opacity: 0.2 }}>🔍</div>
                    <span style={{ fontSize: 14 }}>暂无数据，点击上方执行按钮开始选股</span>
                  </div>
                ) : (
                  <div style={{ overflow: "auto", maxHeight: 400 }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", textAlign: "left" }}>
                      <thead style={{ position: "sticky", top: 0, background: "#fff", zIndex: 5 }}>
                        <tr style={{ borderBottom: "1px solid #f3f4f6" }}>
                          <th style={{ padding: 12, width: 40 }}>
                            <input 
                              type="checkbox" 
                              aria-label="全选/取消全选"
                              title="全选/取消全选"
                              checked={(selectedStocks[s.strategy_id] || []).length === (results[s.strategy_id] || []).length && (results[s.strategy_id] || []).length > 0}
                              onChange={() => toggleAllStocks(s.strategy_id)}
                            />
                          </th>
                          <th style={{ padding: 12, fontSize: 12, fontWeight: 600, color: "#6b7280" }}>排名</th>
                          <th style={{ padding: 12, fontSize: 12, fontWeight: 600, color: "#6b7280" }}>代码</th>
                          <th style={{ padding: 12, fontSize: 12, fontWeight: 600, color: "#6b7280", textAlign: "right" }}>预测分数</th>
                        </tr>
                      </thead>
                      <tbody>
                        {results[s.strategy_id].map((r) => {
                          const isSelected = (selectedStocks[s.strategy_id] || []).includes(r.symbol);
                          return (
                            <tr 
                              key={r.symbol} 
                              style={{ 
                                borderBottom: "1px solid #f9fafb", cursor: "pointer",
                                background: isSelected ? "#f5f3ff" : "transparent"
                              }}
                              onClick={() => toggleStockSelection(s.strategy_id, r.symbol)}
                            >
                              <td style={{ padding: 12 }} onClick={(e) => e.stopPropagation()}>
                                <input 
                                  type="checkbox" 
                                  aria-label={`选择 ${r.symbol}`}
                                  title={`选择 ${r.symbol}`}
                                  checked={isSelected}
                                  onChange={() => toggleStockSelection(s.strategy_id, r.symbol)}
                                />
                              </td>
                              <td style={{ padding: 12 }}>
                                <span style={{ 
                                  display: "inline-flex", alignItems: "center", justifyContent: "center", 
                                  width: 24, height: 24, borderRadius: "50%", fontSize: 11, fontWeight: "bold",
                                  background: r.rank <= 3 ? "#fef3c7" : "#f3f4f6",
                                  color: r.rank <= 3 ? "#92400e" : "#4b5563"
                                }}>
                                  {r.rank}
                                </span>
                              </td>
                              <td style={{ padding: 12, fontSize: 14, fontWeight: 500, color: "#111827", fontFamily: "monospace" }}>{r.symbol}</td>
                              <td style={{ padding: 12, textAlign: "right", fontSize: 14, fontWeight: 600, color: "#059669" }}>
                                {r.score.toFixed(4)}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Watchlist Modal */}
      {showWatchlistModal && (
        <div style={{ 
          position: "fixed", inset: 0, zIndex: 100, background: "rgba(17,24,39,0.6)", 
          backdropFilter: "blur(4px)", display: "flex", alignItems: "center", justifyContent: "center", padding: 16 
        }}>
          <div style={{ background: "#fff", borderRadius: 16, width: "100%", maxWidth: 440, boxShadow: "0 25px 50px -12px rgba(0,0,0,0.25)" }}>
            <div style={{ padding: 24, borderBottom: "1px solid #f3f4f6" }}>
              <h3 style={{ margin: 0, fontSize: 20, fontWeight: "bold", color: "#111827", display: "flex", alignItems: "center", gap: 8 }}>
                <span style={{ fontSize: 24 }}>⭐</span> 加入自选股池
              </h3>
              <p style={{ margin: "8px 0 0", fontSize: 14, color: "#6b7280" }}>准备将 {currentAddingStocks.length} 只股票批量归类并开启绩效追踪</p>
            </div>
            
            <div style={{ padding: 24, display: "flex", flexDirection: "column", gap: 20 }}>
              <div>
                <label style={{ display: "block", fontSize: 14, fontWeight: 600, color: "#374151", marginBottom: 8 }}>选择现有分类</label>
                <select 
                  style={{ 
                    width: "100%", padding: "10px 12px", border: "1px solid #d1d5db", borderRadius: 8,
                    outline: "none", fontSize: 14
                  }}
                  aria-label="选择现有分类"
                  title="选择现有分类"
                  value={selectedCategoryId}
                  onChange={(e) => {
                    setSelectedCategoryId(e.target.value);
                    if (e.target.value) setNewCategoryName("");
                  }}
                >
                  <option value="">-- 请选择分类 --</option>
                  {categories.map(c => (
                    <option key={c.id} value={c.id}>{c.name}</option>
                  ))}
                </select>
              </div>

              <div style={{ position: "relative", textAlign: "center" }}>
                <div style={{ position: "absolute", top: "50%", left: 0, right: 0, borderTop: "1px solid #f3f4f6" }}></div>
                <span style={{ position: "relative", background: "#fff", padding: "0 12px", fontSize: 12, color: "#9ca3af" }}>或者</span>
              </div>

              <div>
                <label style={{ display: "block", fontSize: 14, fontWeight: 600, color: "#374151", marginBottom: 8 }}>新建分类名称</label>
                <input 
                  type="text"
                  placeholder="例如：RD-Agent潜力股、量化优选..."
                  style={{ 
                    width: "100%", padding: "10px 12px", border: "1px solid #d1d5db", borderRadius: 8,
                    outline: "none", fontSize: 14
                  }}
                  aria-label="新建分类名称"
                  title="新建分类名称"
                  value={newCategoryName}
                  onChange={(e) => {
                    setNewCategoryName(e.target.value);
                    if (e.target.value) setSelectedCategoryId("");
                  }}
                />
              </div>

              <div style={{ background: "#ecfdf5", padding: 16, borderRadius: 12, display: "flex", gap: 12 }}>
                <div style={{ fontSize: 20 }}>⚙️</div>
                <p style={{ margin: 0, fontSize: 12, color: "#065f46", lineHeight: 1.5 }}>
                  系统将自动记录此时的<strong>最新价格</strong>作为加入价格，并自动为您计算该策略选股后的累计涨幅。
                </p>
              </div>
            </div>

            <div style={{ padding: 24, background: "#f9fafb", borderBottomLeftRadius: 16, borderBottomRightRadius: 16, display: "flex", gap: 12 }}>
              <button 
                onClick={() => setShowWatchlistModal(false)}
                style={{ 
                  flex: 1, padding: "10px", background: "#fff", border: "1px solid #d1d5db", 
                  borderRadius: 8, color: "#374151", fontWeight: 500, cursor: "pointer" 
                }}
              >
                取消
              </button>
              <button 
                onClick={executeAddToWatchlist}
                style={{ 
                  flex: 1, padding: "10px", background: "#10b981", color: "#fff", 
                  border: "none", borderRadius: 8, fontWeight: 500, cursor: "pointer" 
                }}
              >
                确认添加
              </button>
            </div>
          </div>
        </div>
      )}

      <style jsx global>{`
        @keyframes spin {
          from { transform: rotate(0deg); }
          to { transform: rotate(360deg); }
        }
      `}</style>
    </main>
  );
}
