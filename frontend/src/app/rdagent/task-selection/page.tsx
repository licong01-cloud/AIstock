"use client";

import { useEffect, useMemo, useRef, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type SelectionRow = {
  symbol: string;
  score: number;
  rank?: number;
  name?: string;
  price?: number;
  pct_change?: number;
  quote_source?: string;
  quote_time?: string;
};

type WatchlistCategory = {
  id: number;
  name: string;
  description?: string | null;
};

export default function RDAgentTaskSelectionPage() {
  const [taskId, setTaskId] = useState<string>("");
  const [tradeDate, setTradeDate] = useState<string>(() => new Date().toISOString().split('T')[0]);
  const [cutoffDate, setCutoffDate] = useState<string>(() => new Date().toISOString().split('T')[0]);
  const [topK, setTopK] = useState<number>(50);

  const [running, setRunning] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);
  const [result, setResult] = useState<any>(null);
  const [rows, setRows] = useState<SelectionRow[]>([]);

  const [categories, setCategories] = useState<WatchlistCategory[]>([]);
  const [targetCategoryId, setTargetCategoryId] = useState<number | "">("");
  const [selectedSymbols, setSelectedSymbols] = useState<Record<string, boolean>>({});
  const [adding, setAdding] = useState(false);

  const esRef = useRef<EventSource | null>(null);

  const queryTaskId = useMemo(() => {
    try {
      const u = new URL(window.location.href);
      return (u.searchParams.get("task_id") || "").trim();
    } catch {
      return "";
    }
  }, []);

  useEffect(() => {
    if (queryTaskId) setTaskId(queryTaskId);
  }, [queryTaskId]);

  const selectedCount = useMemo(() => {
    return Object.values(selectedSymbols || {}).filter(Boolean).length;
  }, [selectedSymbols]);

  const allSelected = useMemo(() => {
    const keys = Object.keys(selectedSymbols || {});
    if (!keys.length) return false;
    return keys.every((k) => selectedSymbols[k]);
  }, [selectedSymbols]);

  const anyRows = rows && rows.length > 0;

  useEffect(() => {
    (async () => {
      try {
        const res = await fetch(`${API_BASE}/watchlist/categories`);
        if (!res.ok) return;
        const data = (await res.json()) as WatchlistCategory[];
        if (Array.isArray(data)) {
          setCategories(data);
          setTargetCategoryId((prev) => {
            if (prev !== "") return prev;
            return data.length > 0 ? data[0].id : "";
          });
        }
      } catch {
        // ignore
      }
    })();
  }, []);

  function toggleAll(checked: boolean) {
    const next: Record<string, boolean> = {};
    for (const k of Object.keys(selectedSymbols || {})) {
      next[k] = checked;
    }
    setSelectedSymbols(next);
  }

  function toggleOne(symbol: string, checked: boolean) {
    setSelectedSymbols((prev) => ({
      ...(prev || {}),
      [symbol]: checked,
    }));
  }

  async function addSelectedToWatchlist() {
    const tid = taskId.trim();
    if (!tid) {
      alert("请输入 task_id");
      return;
    }
    if (!anyRows) {
      alert("请先完成一次选股");
      return;
    }
    if (targetCategoryId === "") {
      alert("请先选择自选分类");
      return;
    }
    const picked = rows.filter((r) => selectedSymbols[String(r.symbol)]);
    if (!picked.length) {
      alert("请先勾选要加入自选池的股票");
      return;
    }

    setAdding(true);
    try {
      const payload = {
        items: picked.map((r) => ({
          code: r.symbol,
          rank: r.rank,
          name: r.name,
          task_id: tid,
        })),
        category_id: targetCategoryId,
        on_conflict: "ignore",
        entry_source: "rdagent_task",
      };
      const res = await fetch(`${API_BASE}/watchlist/items/bulk-add-from-selection`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || !data?.ok) {
        const msg = data?.detail || data?.error || "加入自选池失败";
        alert(String(msg));
        return;
      }
      const errs = Array.isArray(data?.errors) ? data.errors : [];
      if (errs.length) {
        alert(`加入完成：成功/跳过=${data.added}/${data.skipped}，失败=${errs.length}（见控制台）`);
        // eslint-disable-next-line no-console
        console.log("bulk-add-from-selection errors", errs);
      } else {
        alert(`加入完成：成功/跳过=${data.added}/${data.skipped}`);
      }
    } catch (e) {
      alert(`加入自选池失败: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      setAdding(false);
    }
  }

  function resetState() {
    setLogs([]);
    setResult(null);
    setRows([]);
    setSelectedSymbols({});
  }

  function stopStream() {
    try {
      if (esRef.current) esRef.current.close();
    } catch {}
    esRef.current = null;
    setRunning(false);
  }

  async function runSelectionStream() {
    const tid = taskId.trim();
    if (!tid) {
      alert("请输入 task_id");
      return;
    }

    resetState();
    setRunning(true);

    try {
      stopStream();
    } catch {}

    const params = new URLSearchParams();
    if (tradeDate) params.set("trade_date", tradeDate);
    if (cutoffDate) params.set("cutoff_date", cutoffDate);
    params.set("top_k", String(topK || 50));

    const url = `${API_BASE}/rdagent/tasks/${encodeURIComponent(tid)}/selection/stream?${params.toString()}`;
    const es = new EventSource(url);
    esRef.current = es;

    const pushLog = (line: string) => {
      setLogs((prev) => {
        const cur = prev || [];
        const next = cur.length > 2500 ? cur.slice(cur.length - 1800) : cur;
        return [...next, line];
      });
    };

    es.addEventListener("start", (ev: any) => {
      try {
        pushLog(`SSE start: ${String(ev?.data || "")}`);
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
        alert(`Task ${tid} 选股失败: ${detail}`);
      } catch {
        alert(`Task ${tid} 选股失败`);
      } finally {
        stopStream();
      }
    });

    es.addEventListener("result", (ev: any) => {
      try {
        const obj = JSON.parse(String(ev?.data || "{}"));
        setResult(obj);
        const items = obj?.items || obj?.rows || [];
        const mapped: SelectionRow[] = (items || []).map((r: any) => ({
          symbol: r.symbol,
          score: r.score,
          rank: r.rank,
          name: r.name,
          price: r.price,
          pct_change: r.pct_change,
          quote_source: r.quote_source,
          quote_time: r.quote_time,
        }));
        setRows(mapped);
        const nextSelected: Record<string, boolean> = {};
        for (const r of mapped) {
          if (r?.symbol) nextSelected[String(r.symbol)] = false;
        }
        setSelectedSymbols(nextSelected);
        pushLog("SSE result received");
      } catch (e) {
        pushLog(`SSE result parse failed: ${e instanceof Error ? e.message : String(e)}`);
      } finally {
        stopStream();
      }
    });

    es.addEventListener("ping", () => {});
  }

  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "linear-gradient(135deg, #f97316 0%, #ef4444 100%)",
          borderRadius: 16,
          padding: 20,
          color: "#fff",
          marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>Task 一键选股</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          基于本地 task manifest 推导 loop 并复用 AIstock 的 build_loop_selection（SSE 实时日志）
        </p>
      </section>

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 16,
          marginBottom: 16,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
        }}
      >
        <div style={{ display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center" }}>
          <label style={{ fontSize: 12 }}>
            task_id:
            <input
              value={taskId}
              onChange={(e) => setTaskId(e.target.value)}
              placeholder="如 2025-12-13_06-25-45-651001"
              style={{ marginLeft: 6, padding: 4, fontSize: 12, minWidth: 320 }}
            />
          </label>

          <label style={{ fontSize: 12 }}>
            自选分类:
            <select
              value={targetCategoryId}
              onChange={(e) => {
                const v = e.target.value;
                setTargetCategoryId(v ? Number(v) : "");
              }}
              style={{ marginLeft: 6, padding: 4, fontSize: 12, width: 220 }}
            >
              {(categories || []).map((c) => (
                <option key={c.id} value={c.id}>
                  {c.name}
                </option>
              ))}
            </select>
          </label>

          <label style={{ fontSize: 12 }}>
            trade_date:
            <input
              value={tradeDate}
              onChange={(e) => setTradeDate(e.target.value)}
              type="date"
              style={{ marginLeft: 6, padding: 4, fontSize: 12, width: 150 }}
            />
          </label>

          <label style={{ fontSize: 12 }}>
            cutoff_date:
            <input
              value={cutoffDate}
              onChange={(e) => setCutoffDate(e.target.value)}
              type="date"
              style={{ marginLeft: 6, padding: 4, fontSize: 12, width: 150 }}
            />
          </label>

          <label style={{ fontSize: 12 }}>
            top_k:
            <input
              value={String(topK)}
              onChange={(e) => setTopK(Number(e.target.value || 50))}
              type="number"
              min={1}
              max={500}
              style={{ marginLeft: 6, padding: 4, fontSize: 12, width: 90 }}
            />
          </label>

          <button
            onClick={runSelectionStream}
            disabled={running}
            style={{
              padding: "6px 10px",
              fontSize: 12,
              cursor: running ? "not-allowed" : "pointer",
              background: running ? "#e5e7eb" : "#111827",
              color: running ? "#6b7280" : "#fff",
              borderRadius: 8,
              border: "none",
            }}
          >
            {running ? "选股中..." : "开始选股（SSE）"}
          </button>

          <button
            onClick={stopStream}
            disabled={!running}
            style={{ padding: "6px 10px", fontSize: 12, cursor: !running ? "not-allowed" : "pointer" }}
          >
            停止
          </button>

          <button
            onClick={() => window.open(`/rdagent/tasks/${encodeURIComponent(taskId.trim())}`, "_blank")}
            disabled={!taskId.trim()}
            style={{ padding: "6px 10px", fontSize: 12, cursor: !taskId.trim() ? "not-allowed" : "pointer" }}
          >
            打开 Task 详情
          </button>

          <button
            onClick={addSelectedToWatchlist}
            disabled={adding || !anyRows || selectedCount === 0}
            style={
              {
                padding: "6px 10px",
                fontSize: 12,
                cursor: adding || !anyRows || selectedCount === 0 ? "not-allowed" : "pointer",
                background: adding || !anyRows || selectedCount === 0 ? "#e5e7eb" : "#10b981",
                color: adding || !anyRows || selectedCount === 0 ? "#6b7280" : "#fff",
                borderRadius: 8,
                border: "none",
              } as any
            }
          >
            {adding ? "加入中..." : `加入自选池（已选 ${selectedCount}）`}
          </button>
        </div>
      </section>

      <section
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: 16,
          marginBottom: 16,
        }}
      >
        <div
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 16,
            boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
          }}
        >
          <div style={{ fontWeight: 600, marginBottom: 8 }}>实时日志（SSE）</div>
          <div style={{ height: 360, overflow: "auto", fontSize: 12, fontFamily: "monospace", whiteSpace: "pre" }}>
            {(logs || []).join("\n")}
          </div>
        </div>

        <div
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 16,
            boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
          }}
        >
          <div style={{ fontWeight: 600, marginBottom: 8 }}>响应（原始）</div>
          <div style={{ height: 360, overflow: "auto", fontSize: 12, fontFamily: "monospace", whiteSpace: "pre" }}>
            {JSON.stringify(result, null, 2)}
          </div>
        </div>
      </section>

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 16,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
        }}
      >
        <div style={{ fontWeight: 600, marginBottom: 8 }}>TopK 结果</div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>
                <th style={{ padding: 8, width: 32 }}>
                  <input
                    type="checkbox"
                    checked={allSelected}
                    onChange={(e) => toggleAll(e.target.checked)}
                    disabled={!anyRows}
                  />
                </th>
                <th style={{ padding: 8, width: 56, whiteSpace: "nowrap" }}>rank</th>
                <th style={{ padding: 8, width: 92, whiteSpace: "nowrap" }}>symbol</th>
                <th style={{ padding: 8, width: 120 }}>name</th>
                <th style={{ padding: 8, width: 110 }}>score</th>
                <th style={{ padding: 8, width: 80 }}>price</th>
                <th style={{ padding: 8, width: 90 }}>pct_change</th>
                <th style={{ padding: 8, width: 160 }}>quote_time</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={`${r.symbol}:${r.rank ?? ""}`} style={{ borderBottom: "1px solid #f3f4f6" }}>
                  <td style={{ padding: 8 }}>
                    <input
                      type="checkbox"
                      checked={!!selectedSymbols[String(r.symbol)]}
                      onChange={(e) => toggleOne(String(r.symbol), e.target.checked)}
                    />
                  </td>
                  <td style={{ padding: 8, width: 56, whiteSpace: "nowrap" }}>{r.rank ?? "-"}</td>
                  <td style={{ padding: 8, width: 92, fontFamily: "monospace", whiteSpace: "nowrap" }}>{r.symbol}</td>
                  <td style={{ padding: 8 }}>{r.name || "-"}</td>
                  <td style={{ padding: 8 }}>{typeof r.score === "number" ? r.score.toFixed(6) : String(r.score)}</td>
                  <td style={{ padding: 8 }}>{r.price ?? "-"}</td>
                  <td style={{ padding: 8 }}>{r.pct_change ?? "-"}</td>
                  <td style={{ padding: 8 }}>{r.quote_time || "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}
