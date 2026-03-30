"use client";

import { useState, useEffect, useCallback, useRef } from "react";

const API = "http://127.0.0.1:8001/api/v1";

interface SelectionItem {
  symbol: string;
  name: string | null;
  price: number | null;
  pct_change: number | null;
  score: number | null;
  rank: number | null;
  quote_source: string;
  quote_time: string | null;
}

interface SelectionResult {
  items: SelectionItem[];
  inference_meta?: Record<string, any>;
  as_of?: string;
}

/* ---------- 来源下拉选项 ---------- */
interface SourceOption {
  id: string;
  label: string;
}

interface RdagentTaskWithMetrics {
  task_id: string;
  sota_factors: number;
  sota_models: number;
  best_ic: number | null;
  best_ann_return: number | null;
  best_max_drawdown: number | null;
}

export default function SelectionPage() {
  const [signalSource, setSignalSource] = useState<string>("rdagent_task");
  const [sourceId, setSourceId] = useState("");
  const [manualInput, setManualInput] = useState(false);   // 手动输入模式
  const [loopId, setLoopId] = useState<string>("");
  const [topK, setTopK] = useState(50);
  const [tradeDate, setTradeDate] = useState(new Date().toISOString().slice(0, 10));

  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<SelectionResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [addingWatchlist, setAddingWatchlist] = useState(false);

  /* ---------- 因子改造预检查 ---------- */
  const [preflight, setPreflight] = useState<{
    ready: boolean;
    message: string;
    not_transformed: string[];
    missing: string[];
    total_dynamic: number;
    transformed: string[];
  } | null>(null);
  const [preflightLoading, setPreflightLoading] = useState(false);

  /* ---------- 来源列表 ---------- */
  const [rdagentTasks, setRdagentTasks] = useState<RdagentTaskWithMetrics[]>([]);
  const [qeExperiments, setQeExperiments] = useState<SourceOption[]>([]);
  const [qeEvolutions, setQeEvolutions] = useState<SourceOption[]>([]);
  const [loadingOptions, setLoadingOptions] = useState(false);

  /* ---------- Task 卡片面板 ---------- */
  const [taskDropdownOpen, setTaskDropdownOpen] = useState(false);
  const taskPanelRef = useRef<HTMLDivElement>(null);

  /* ---------- 日期提示 ---------- */
  const [dateInfo, setDateInfo] = useState<{ actual_trade_date: string; next_trade_date: string | null } | null>(null);
  const [dateInfoLoading, setDateInfoLoading] = useState(false);

  // 加载 RDAgent task 列表 (with metrics) + QE 列表
  useEffect(() => {
    setLoadingOptions(true);
    Promise.all([
      fetch(`${API}/rdagent/tasks/local-with-metrics?limit=200&sort_by=best_ic&sort_order=desc`)
        .then((r) => r.json())
        .then((d) => {
          const items: any[] = d.items || d || [];
          setRdagentTasks(
            items.map((t: any) => ({
              task_id: t.task_id || "",
              sota_factors: t.sota_factors ?? 0,
              sota_models: t.sota_models ?? 0,
              best_ic: t.best_ic ?? null,
              best_ann_return: t.best_ann_return ?? null,
              best_max_drawdown: t.best_max_drawdown ?? null,
            }))
          );
        })
        .catch(() => {}),
      fetch(`${API}/quantevolver/experiments?limit=200`)
        .then((r) => r.json())
        .then((d) => {
          const items: any[] = d.items || d || [];
          setQeExperiments(
            items.map((e: any) => ({
              id: e.experiment_id || "",
              label: e.experiment_name
                ? `${e.experiment_id} — ${e.experiment_name}`
                : e.experiment_id || "unknown",
            }))
          );
        })
        .catch(() => {}),
      fetch(`${API}/quantevolver/evolution/tasks`)
        .then((r) => r.json())
        .then((d) => {
          const items: any[] = d.data || d.items || d || [];
          setQeEvolutions(
            items.map((t: any) => ({
              id: t.task_id || "",
              label: t.task_name
                ? `${t.task_id} — ${t.task_name}`
                : t.task_id || "unknown",
            }))
          );
        })
        .catch(() => {}),
    ]).finally(() => setLoadingOptions(false));
  }, []);

  // 切换来源时重置已选 ID
  useEffect(() => {
    setSourceId("");
    setLoopId("");
    setManualInput(false);
    setPreflight(null);
    setTaskDropdownOpen(false);
  }, [signalSource]);

  // sourceId 变化时自动检查因子改造状态
  useEffect(() => {
    if (!sourceId) {
      setPreflight(null);
      return;
    }
    setPreflightLoading(true);
    setPreflight(null);
    fetch(`${API}/paper-trading/selection/preflight`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        signal_source: signalSource,
        signal_source_id: sourceId,
        signal_loop_id: signalSource === "rdagent_task" && loopId ? parseInt(loopId) : undefined,
      }),
    })
      .then((r) => r.json())
      .then((d) => setPreflight(d))
      .catch(() => {})
      .finally(() => setPreflightLoading(false));
  }, [signalSource, sourceId, loopId]);

  // 日期变化时查询下一交易日
  useEffect(() => {
    if (!tradeDate) {
      setDateInfo(null);
      return;
    }
    setDateInfoLoading(true);
    setDateInfo(null);
    fetch(`${API}/paper-trading/next-trade-date?trade_date=${tradeDate}`)
      .then((r) => r.json())
      .then((d) => setDateInfo({ actual_trade_date: d.actual_trade_date, next_trade_date: d.next_trade_date }))
      .catch(() => setDateInfo(null))
      .finally(() => setDateInfoLoading(false));
  }, [tradeDate]);

  // 点击外部关闭 task 面板
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (taskPanelRef.current && !taskPanelRef.current.contains(e.target as Node)) {
        setTaskDropdownOpen(false);
      }
    }
    if (taskDropdownOpen) {
      document.addEventListener("mousedown", handleClick);
      return () => document.removeEventListener("mousedown", handleClick);
    }
  }, [taskDropdownOpen]);

  /* QE 来源用原来的 SourceOption 列表 */
  const qeOptions =
    signalSource === "qe_experiment" ? qeExperiments : qeEvolutions;

  /* 选中的 task 对象 */
  const selectedTask = rdagentTasks.find((t) => t.task_id === sourceId);

  /* ---------- 选股执行 ---------- */
  const runSelection = useCallback(async () => {
    // 因子未改造时阻止选股
    if (preflight && !preflight.ready) {
      setError(preflight.message);
      return;
    }
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const body: any = {
        signal_source: signalSource,
        signal_source_id: sourceId,
        top_k: topK,
        trade_date: tradeDate,
      };
      if (signalSource === "rdagent_task" && loopId) {
        body.signal_loop_id = parseInt(loopId);
      }
      const resp = await fetch(`${API}/paper-trading/selection`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!resp.ok) {
        const txt = await resp.text();
        throw new Error(`${resp.status}: ${txt}`);
      }
      const data = await resp.json();
      setResult(data);
    } catch (e: any) {
      setError(e.message || String(e));
    } finally {
      setLoading(false);
    }
  }, [signalSource, sourceId, loopId, topK, tradeDate, preflight]);

  const toggleSelect = (symbol: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(symbol)) next.delete(symbol);
      else next.add(symbol);
      return next;
    });
  };

  const toggleAll = () => {
    if (!result) return;
    if (selected.size === result.items.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(result.items.map((i) => i.symbol)));
    }
  };

  const addToWatchlist = useCallback(async () => {
    if (selected.size === 0 || !result) return;
    setAddingWatchlist(true);
    try {
      const selectedItems = result.items.filter(i => selected.has(i.symbol));
      const resp = await fetch(`${API}/paper-trading/selection/add-watchlist`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          items: selectedItems.map(i => ({
            code: i.symbol,
            rank: i.rank,
            name: i.name,
            price: i.price,
          })),
          signal_source: signalSource,
          signal_source_id: sourceId,
          signal_loop_id: signalSource === "rdagent_task" && loopId ? parseInt(loopId) : undefined,
          entry_price_date: dateInfo?.next_trade_date || undefined,
        }),
      });
      if (!resp.ok) throw new Error(await resp.text());
      const data = await resp.json();
      const priceNote = dateInfo?.next_trade_date ? `（以 ${dateInfo.next_trade_date} 收盘价入场）` : "";
      alert(`已添加 ${data.added} 只到自选股${priceNote}`);
    } catch (e: any) {
      alert("添加失败: " + (e.message || e));
    } finally {
      setAddingWatchlist(false);
    }
  }, [selected, result, signalSource, sourceId, loopId, dateInfo]);

  // CSV 导出
  const downloadCsv = useCallback(() => {
    if (!result || result.items.length === 0) return;
    const headers = ["排名", "代码", "名称", "评分", "价格", "涨跌幅"];
    const lines: string[] = [headers.join(",")];
    for (const item of result.items) {
      const pct = item.pct_change != null ? item.pct_change.toFixed(2) + "%" : "";
      const row = [
        item.rank ?? "",
        item.symbol,
        (item.name || "").replace(/,/g, "，"),
        item.score != null ? item.score.toFixed(6) : "",
        item.price != null ? item.price.toFixed(2) : "",
        pct,
      ];
      lines.push(row.join(","));
    }
    const blob = new Blob(["\ufeff" + lines.join("\n")], {
      type: "text/csv;charset=utf-8;",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `selection_${signalSource}_${sourceId}_${tradeDate}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }, [result, signalSource, sourceId, tradeDate]);

  // 行业统计
  const industryStat = result
    ? result.items.reduce<Record<string, number>>((acc, it) => {
        const ind = (it as any).industry || "未知";
        acc[ind] = (acc[ind] || 0) + 1;
        return acc;
      }, {})
    : {};

  const meta = result?.inference_meta || {};

  /* 格式化指标 */
  const fmtIc = (v: number | null) => (v != null ? v.toFixed(4) : "-");
  const fmtPct = (v: number | null) => (v != null ? v.toFixed(1) + "%" : "-");
  const fmtDd = (v: number | null) => (v != null ? v.toFixed(1) + "%" : "-");

  /* 日期是否被调整 */
  const dateAdjusted = dateInfo && dateInfo.actual_trade_date !== tradeDate;

  return (
    <div>
      {/* 配置区域 */}
      <div style={cardStyle}>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 16, alignItems: "end" }}>
          {/* 信号来源 */}
          <div>
            <label style={labelStyle}>信号来源</label>
            <select
              value={signalSource}
              onChange={(e) => setSignalSource(e.target.value)}
              style={inputStyle}
            >
              <option value="rdagent_task">RDAgent Task</option>
              <option value="qe_experiment">QE 单次实验</option>
              <option value="qe_evolution">QE 演进 SOTA</option>
            </select>
          </div>

          {/* 来源 ID：Task 卡片选择器 / QE 下拉框 / 手工输入 */}
          <div>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <label style={{ ...labelStyle, marginBottom: 0 }}>
                {signalSource === "rdagent_task" ? "Task Run ID" : signalSource === "qe_experiment" ? "Experiment ID" : "Evolution Task ID"}
              </label>
              <button
                onClick={() => { setManualInput(!manualInput); setSourceId(""); setTaskDropdownOpen(false); }}
                style={{ fontSize: 11, color: "#2563eb", background: "none", border: "none", cursor: "pointer", padding: 0 }}
              >
                {manualInput ? "切换下拉" : "手工输入"}
              </button>
            </div>
            <div style={{ marginTop: 4 }}>
              {manualInput ? (
                <input
                  value={sourceId}
                  onChange={(e) => setSourceId(e.target.value)}
                  placeholder={signalSource === "rdagent_task" ? "task_xxx" : "exp_xxx"}
                  style={inputStyle}
                />
              ) : signalSource === "rdagent_task" ? (
                /* ── 自定义 Task 卡片选择器 ── */
                <div ref={taskPanelRef} style={{ position: "relative" }}>
                  <div
                    onClick={() => setTaskDropdownOpen(!taskDropdownOpen)}
                    style={{
                      ...inputStyle,
                      cursor: "pointer",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                      background: "#fff",
                      minHeight: 32,
                    }}
                  >
                    {loadingOptions ? (
                      <span style={{ color: "#9ca3af" }}>加载中...</span>
                    ) : selectedTask ? (
                      <span style={{ fontSize: 12, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {selectedTask.task_id.length > 20 ? selectedTask.task_id.slice(0, 20) + "…" : selectedTask.task_id}
                        <span style={{ color: "#6b7280", marginLeft: 6 }}>
                          IC:{fmtIc(selectedTask.best_ic)} 年化:{fmtPct(selectedTask.best_ann_return)}
                        </span>
                      </span>
                    ) : rdagentTasks.length === 0 ? (
                      <span style={{ color: "#9ca3af" }}>暂无可用任务</span>
                    ) : (
                      <span style={{ color: "#9ca3af" }}>-- 请选择 --</span>
                    )}
                    <span style={{ fontSize: 10, color: "#9ca3af", marginLeft: 4 }}>{taskDropdownOpen ? "▲" : "▼"}</span>
                  </div>

                  {taskDropdownOpen && (
                    <div style={{
                      position: "absolute",
                      top: "100%",
                      left: 0,
                      right: 0,
                      zIndex: 50,
                      background: "#fff",
                      border: "1px solid #d1d5db",
                      borderRadius: 8,
                      marginTop: 4,
                      maxHeight: 320,
                      overflowY: "auto",
                      boxShadow: "0 4px 12px rgba(0,0,0,0.12)",
                    }}>
                      {rdagentTasks.length === 0 ? (
                        <div style={{ padding: 12, color: "#9ca3af", fontSize: 13, textAlign: "center" }}>暂无可用任务</div>
                      ) : (
                        rdagentTasks.map((t) => {
                          const isSelected = t.task_id === sourceId;
                          return (
                            <div
                              key={t.task_id}
                              onClick={() => { setSourceId(t.task_id); setTaskDropdownOpen(false); }}
                              style={{
                                padding: "8px 12px",
                                cursor: "pointer",
                                background: isSelected ? "#eff6ff" : "transparent",
                                borderLeft: isSelected ? "3px solid #2563eb" : "3px solid transparent",
                                borderBottom: "1px solid #f3f4f6",
                                transition: "background 0.15s",
                              }}
                              onMouseEnter={(e) => { if (!isSelected) (e.currentTarget.style.background = "#f9fafb"); }}
                              onMouseLeave={(e) => { if (!isSelected) (e.currentTarget.style.background = "transparent"); }}
                            >
                              <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                                <span style={{
                                  width: 8, height: 8, borderRadius: "50%",
                                  background: isSelected ? "#2563eb" : "#d1d5db",
                                  flexShrink: 0,
                                }} />
                                <span style={{
                                  fontSize: 12, fontFamily: "monospace", fontWeight: isSelected ? 600 : 400,
                                  color: isSelected ? "#1d4ed8" : "#111",
                                  overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                                }}>
                                  {t.task_id}
                                </span>
                              </div>
                              <div style={{ display: "flex", gap: 10, marginTop: 4, marginLeft: 14, fontSize: 11, color: "#6b7280" }}>
                                <span>IC:<b style={{ color: t.best_ic != null && t.best_ic > 0.05 ? "#16a34a" : "#111" }}>{fmtIc(t.best_ic)}</b></span>
                                <span>年化:<b style={{ color: t.best_ann_return != null && t.best_ann_return > 80 ? "#16a34a" : "#111" }}>{fmtPct(t.best_ann_return)}</b></span>
                                <span>回撤:<b style={{ color: "#111" }}>{fmtDd(t.best_max_drawdown)}</b></span>
                                <span style={{ color: "#9ca3af" }}>F:{t.sota_factors} M:{t.sota_models}</span>
                              </div>
                            </div>
                          );
                        })
                      )}
                    </div>
                  )}
                </div>
              ) : (
                /* ── QE 来源保持原有 select ── */
                <select
                  value={sourceId}
                  onChange={(e) => setSourceId(e.target.value)}
                  style={inputStyle}
                  disabled={loadingOptions}
                >
                  <option value="">
                    {loadingOptions ? "加载中..." : qeOptions.length === 0 ? "暂无可用任务" : "-- 请选择 --"}
                  </option>
                  {qeOptions.map((opt) => (
                    <option key={opt.id} value={opt.id}>{opt.label}</option>
                  ))}
                </select>
              )}
            </div>
          </div>

          {/* Loop ID (仅 rdagent_task) */}
          {signalSource === "rdagent_task" && (
            <div>
              <label style={labelStyle}>Loop ID</label>
              <input
                value={loopId}
                onChange={(e) => setLoopId(e.target.value)}
                placeholder="留空=最新SOTA"
                style={inputStyle}
                type="number"
              />
            </div>
          )}

          {/* 日期 */}
          <div>
            <label style={labelStyle}>日期</label>
            <input
              value={tradeDate}
              onChange={(e) => setTradeDate(e.target.value)}
              type="date"
              style={inputStyle}
            />
            {/* 日期提示 */}
            {dateInfoLoading && (
              <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 4 }}>查询交易日...</div>
            )}
            {dateInfo && !dateInfoLoading && (
              <div style={{ marginTop: 4 }}>
                {dateAdjusted && (
                  <div style={{ fontSize: 11, color: "#d97706", background: "#fffbeb", padding: "2px 6px", borderRadius: 4, marginBottom: 2 }}>
                    非交易日，已自动调整至 {dateInfo.actual_trade_date}
                  </div>
                )}
                {dateInfo.next_trade_date ? (
                  <div style={{ fontSize: 11, color: "#6b7280" }}>
                    以 {dateInfo.actual_trade_date} 收盘数据，选出 <b style={{ color: "#2563eb" }}>{dateInfo.next_trade_date}</b>（下一交易日）的股票
                  </div>
                ) : (
                  <div style={{ fontSize: 11, color: "#dc2626" }}>
                    无可用交易日数据
                  </div>
                )}
              </div>
            )}
          </div>
        </div>

        <div style={{ display: "flex", gap: 12, marginTop: 16, alignItems: "center" }}>
          <div>
            <label style={labelStyle}>Top K</label>
            <input
              value={topK}
              onChange={(e) => setTopK(Number(e.target.value))}
              type="number"
              style={{ ...inputStyle, width: 80 }}
              min={10}
              max={200}
            />
          </div>
          <button
            onClick={runSelection}
            disabled={loading || !sourceId || preflightLoading || (preflight != null && !preflight.ready)}
            style={{
              ...btnStyle,
              opacity: loading || !sourceId || preflightLoading || (preflight != null && !preflight.ready) ? 0.5 : 1,
              marginTop: 20,
            }}
          >
            {loading ? "选股中..." : preflightLoading ? "检查中..." : "执行选股"}
          </button>
          {result && selected.size > 0 && (
            <button
              onClick={addToWatchlist}
              disabled={addingWatchlist}
              style={{ ...btnSecondaryStyle, marginTop: 20 }}
            >
              {addingWatchlist ? "添加中..." : `加入自选 (已选 ${selected.size})`}
            </button>
          )}
          {result && result.items.length > 0 && (
            <button
              onClick={downloadCsv}
              style={{ ...btnSecondaryStyle, marginTop: 20 }}
            >
              导出 CSV
            </button>
          )}
        </div>
      </div>

      {/* 因子改造状态提示 */}
      {preflightLoading && sourceId && (
        <div style={{ padding: 12, margin: "12px 0", background: "#f0f9ff", borderRadius: 6, color: "#1e40af", fontSize: 13 }}>
          正在检查因子改造状态...
        </div>
      )}
      {preflight && !preflight.ready && (
        <div style={{ padding: 12, margin: "12px 0", background: "#fffbeb", border: "1px solid #f59e0b", borderRadius: 6 }}>
          <div style={{ fontWeight: 600, color: "#b45309", marginBottom: 6 }}>
            因子改造未完成，无法使用 DB 数据源选股
          </div>
          <div style={{ fontSize: 13, color: "#92400e" }}>{preflight.message}</div>
          {preflight.missing.length > 0 && (
            <div style={{ fontSize: 12, color: "#92400e", marginTop: 4 }}>
              未录入: {preflight.missing.join(", ")}
            </div>
          )}
          {preflight.not_transformed.length > 0 && (
            <div style={{ fontSize: 12, color: "#92400e", marginTop: 4 }}>
              未改造: {preflight.not_transformed.join(", ")}
            </div>
          )}
          <div style={{ fontSize: 12, color: "#6b7280", marginTop: 6 }}>
            请先到因子管理页面完成因子改造（transformation_status → SUCCESS）
          </div>
        </div>
      )}
      {preflight && preflight.ready && (
        <div style={{ padding: 8, margin: "12px 0", background: "#f0fdf4", borderRadius: 6, color: "#166534", fontSize: 13 }}>
          {preflight.message}
        </div>
      )}

      {error && (
        <div style={{ color: "#dc2626", padding: 12, margin: "12px 0", background: "#fef2f2", borderRadius: 6 }}>
          {error}
        </div>
      )}

      {/* 结果 */}
      {result && (
        <>
          {/* 元信息 */}
          <div style={{ ...cardStyle, display: "flex", gap: 24, flexWrap: "wrap", fontSize: 13, color: "#6b7280" }}>
            <span>共 <b style={{ color: "#111" }}>{result.items.length}</b> 只</span>
            <span>推理日期: <b>{result.as_of || meta.as_of || tradeDate}</b></span>
            {meta.universe_size && <span>股票池: {meta.universe_size}</span>}
            {meta.data_dates?.kline_daily_raw && <span>K线日期: {meta.data_dates.kline_daily_raw}</span>}
            {Object.keys(industryStat).length > 0 && (
              <span>
                行业: {Object.entries(industryStat).sort((a, b) => b[1] - a[1]).slice(0, 6).map(([k, v]) => `${k}(${v})`).join(" ")}
              </span>
            )}
          </div>

          {/* 表格 */}
          <div style={{ ...cardStyle, padding: 0, overflow: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
              <thead>
                <tr style={{ background: "linear-gradient(135deg, #1e40af, #3b82f6)", color: "#fff" }}>
                  <th style={thStyle}>
                    <input type="checkbox" onChange={toggleAll} checked={selected.size === result.items.length && result.items.length > 0} />
                  </th>
                  <th style={thStyle}>排名</th>
                  <th style={thStyle}>代码</th>
                  <th style={thStyle}>名称</th>
                  <th style={{ ...thStyle, textAlign: "right" }}>评分</th>
                  <th style={{ ...thStyle, textAlign: "right" }}>价格</th>
                  <th style={{ ...thStyle, textAlign: "right" }}>涨跌幅</th>
                </tr>
              </thead>
              <tbody>
                {result.items.map((item, i) => (
                  <tr
                    key={item.symbol}
                    style={{
                      background: i % 2 === 0 ? "#fff" : "#f9fafb",
                      cursor: "pointer",
                    }}
                    onClick={() => toggleSelect(item.symbol)}
                  >
                    <td style={tdStyle}>
                      <input type="checkbox" checked={selected.has(item.symbol)} readOnly />
                    </td>
                    <td style={tdStyle}>{item.rank ?? i + 1}</td>
                    <td style={{ ...tdStyle, fontFamily: "monospace" }}>{item.symbol}</td>
                    <td style={tdStyle}>{item.name || "-"}</td>
                    <td style={{ ...tdStyle, textAlign: "right", fontWeight: 600 }}>
                      {item.score != null ? item.score.toFixed(4) : "-"}
                    </td>
                    <td style={{ ...tdStyle, textAlign: "right" }}>
                      {item.price != null ? item.price.toFixed(2) : "-"}
                    </td>
                    <td
                      style={{
                        ...tdStyle,
                        textAlign: "right",
                        color: (item.pct_change ?? 0) >= 0 ? "#dc2626" : "#16a34a",
                        fontWeight: 500,
                      }}
                    >
                      {item.pct_change != null
                        ? `${item.pct_change >= 0 ? "+" : ""}${item.pct_change.toFixed(2)}%`
                        : "-"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}

// ── 样式常量 ──

const cardStyle: React.CSSProperties = {
  background: "#fff",
  borderRadius: 12,
  padding: 20,
  marginBottom: 16,
  boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
};

const labelStyle: React.CSSProperties = {
  display: "block",
  fontSize: 12,
  fontWeight: 500,
  color: "#6b7280",
  marginBottom: 4,
};

const inputStyle: React.CSSProperties = {
  width: "100%",
  padding: "6px 10px",
  border: "1px solid #d1d5db",
  borderRadius: 6,
  fontSize: 13,
  outline: "none",
};

const btnStyle: React.CSSProperties = {
  padding: "8px 20px",
  background: "linear-gradient(135deg, #2563eb, #1d4ed8)",
  color: "#fff",
  border: "none",
  borderRadius: 6,
  fontSize: 13,
  fontWeight: 600,
  cursor: "pointer",
};

const btnSecondaryStyle: React.CSSProperties = {
  padding: "8px 20px",
  background: "#fff",
  color: "#2563eb",
  border: "1px solid #2563eb",
  borderRadius: 6,
  fontSize: 13,
  fontWeight: 600,
  cursor: "pointer",
};

const thStyle: React.CSSProperties = {
  padding: "10px 12px",
  textAlign: "left",
  fontSize: 12,
  fontWeight: 600,
};

const tdStyle: React.CSSProperties = {
  padding: "8px 12px",
  borderBottom: "1px solid #f3f4f6",
};
