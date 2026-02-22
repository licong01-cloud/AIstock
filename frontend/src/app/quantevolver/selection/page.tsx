"use client";

import { useEffect, useState, useCallback, useMemo } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type Experiment = {
  experiment_id: string;
  experiment_name: string;
  status: string;
  factor_names?: string[];
  model_id?: string;
  strategy_id?: string;
  workspace_path?: string;
  result_metrics?: any;
  created_at?: string;
  updated_at?: string;
};

type SelectionRow = {
  symbol: string;
  name?: string;
  score: number;
  rank?: number;
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

const KEY_METRICS = [
  { key: "IC", label: "IC", fmt: (v: number) => v.toFixed(4), good: (v: number) => v > 0.03 },
  { key: "ICIR", label: "ICIR", fmt: (v: number) => v.toFixed(4), good: (v: number) => v > 0.3 },
  { key: "Rank IC", label: "Rank IC", fmt: (v: number) => v.toFixed(4), good: (v: number) => v > 0.05 },
  { key: "annualized_return", label: "年化收益", fmt: (v: number) => (v * 100).toFixed(2) + "%", good: (v: number) => v > 0.1 },
  { key: "max_drawdown", label: "最大回撤", fmt: (v: number) => (v * 100).toFixed(2) + "%", good: (v: number) => v > -0.2 },
  { key: "sharpe", label: "Sharpe", fmt: (v: number) => v.toFixed(3), good: (v: number) => v > 1.0 },
];

const MODEL_NAMES: Record<string, string> = {
  LGBModel: "LightGBM",
  linear: "线性模型",
  XGBModel: "XGBoost",
  CatBoostModel: "CatBoost",
  DNNModel: "深度神经网络",
  TabNetModel: "TabNet",
};

function getMetrics(exp: Experiment): Record<string, any> {
  if (!exp.result_metrics) return {};
  if (typeof exp.result_metrics === "string") {
    try { return JSON.parse(exp.result_metrics); } catch { return {}; }
  }
  return exp.result_metrics;
}

export default function QESelectionPage() {
  const [experiments, setExperiments] = useState<Experiment[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedExpId, setSelectedExpId] = useState<string>("");
  const [sortKey, setSortKey] = useState<string>("sharpe");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  // 选股参数
  const [tradeDate, setTradeDate] = useState<string>(() => new Date().toISOString().split("T")[0]);
  const [topK, setTopK] = useState<number>(50);
  const [running, setRunning] = useState(false);
  const [selectionResult, setSelectionResult] = useState<SelectionRow[]>([]);
  const [selectionError, setSelectionError] = useState<string>("");
  const [selectionLogs, setSelectionLogs] = useState<string[]>([]);

  // 自选股
  const [categories, setCategories] = useState<WatchlistCategory[]>([]);
  const [targetCategoryId, setTargetCategoryId] = useState<number | "">("");
  const [selectedSymbols, setSelectedSymbols] = useState<Record<string, boolean>>({});
  const [adding, setAdding] = useState(false);

  const [toast, setToast] = useState<{ msg: string; ok: boolean } | null>(null);
  const showToast = useCallback((msg: string, ok: boolean) => {
    setToast({ msg, ok });
    setTimeout(() => setToast(null), 3500);
  }, []);

  // 加载已完成的实验列表
  async function loadExperiments() {
    setLoading(true);
    try {
      const res = await fetch(`${API}/quantevolver/experiments?limit=100`);
      const data = await res.json();
      const items: Experiment[] = data.items || [];
      // 只保留有回测结果的实验
      setExperiments(items.filter(e => {
        const m = getMetrics(e);
        return Object.keys(m).length > 0;
      }));
    } catch (e: any) {
      showToast("加载实验列表失败: " + (e?.message || ""), false);
    }
    setLoading(false);
  }

  // 加载自选股分类
  async function loadCategories() {
    try {
      const res = await fetch(`${API}/watchlist/categories`);
      if (res.ok) {
        const data = await res.json();
        if (Array.isArray(data)) {
          setCategories(data);
          if (data.length > 0) setTargetCategoryId(data[0].id);
        }
      }
    } catch {}
  }

  useEffect(() => {
    loadExperiments();
    loadCategories();
  }, []);

  // 排序后的实验列表
  const sortedExperiments = useMemo(() => {
    return [...experiments].sort((a, b) => {
      const ma = getMetrics(a);
      const mb = getMetrics(b);
      const va = ma[sortKey] ?? (sortDir === "desc" ? -Infinity : Infinity);
      const vb = mb[sortKey] ?? (sortDir === "desc" ? -Infinity : Infinity);
      const na = typeof va === "number" ? va : parseFloat(va) || 0;
      const nb = typeof vb === "number" ? vb : parseFloat(vb) || 0;
      return sortDir === "desc" ? nb - na : na - nb;
    });
  }, [experiments, sortKey, sortDir]);

  const selectedExp = experiments.find(e => e.experiment_id === selectedExpId);

  // 触发选股
  async function runSelection() {
    if (!selectedExpId) {
      showToast("请先选择一个实验", false);
      return;
    }
    setRunning(true);
    setSelectionResult([]);
    setSelectionError("");
    setSelectionLogs(["开始基于QE实验选股..."]);
    try {
      const res = await fetch(`${API}/quantevolver/experiments/${selectedExpId}/selection`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ trade_date: tradeDate, top_k: topK }),
      });
      const data = await res.json();
      if (data.ok) {
        setSelectionResult(data.rows || []);
        setSelectionLogs(prev => [...prev, `选股完成，共 ${(data.rows || []).length} 只股票`]);
        // 初始化全选
        const sel: Record<string, boolean> = {};
        (data.rows || []).forEach((r: SelectionRow) => { sel[r.symbol] = true; });
        setSelectedSymbols(sel);
        showToast(`选股完成，共 ${(data.rows || []).length} 只`, true);
      } else {
        setSelectionError(data.error || data.detail || "选股失败");
        setSelectionLogs(prev => [...prev, `选股失败: ${data.error || data.detail || "未知错误"}`]);
      }
    } catch (e: any) {
      setSelectionError(e?.message || "请求失败");
      setSelectionLogs(prev => [...prev, `请求失败: ${e?.message || ""}`]);
    }
    setRunning(false);
  }

  // 添加到自选股
  async function addToWatchlist() {
    const symbols = Object.entries(selectedSymbols).filter(([, v]) => v).map(([k]) => k);
    if (!symbols.length) { showToast("请先选择股票", false); return; }
    if (!targetCategoryId) { showToast("请选择自选股分类", false); return; }
    setAdding(true);
    try {
      const res = await fetch(`${API}/watchlist/batch-add`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ category_id: targetCategoryId, symbols }),
      });
      const data = await res.json();
      if (data.ok || res.ok) {
        showToast(`已添加 ${symbols.length} 只股票到自选`, true);
      } else {
        showToast("添加失败: " + (data.error || data.detail || ""), false);
      }
    } catch (e: any) {
      showToast("添加失败: " + (e?.message || ""), false);
    }
    setAdding(false);
  }

  const selectedCount = Object.values(selectedSymbols).filter(Boolean).length;

  return (
    <main className="p-6">
      {/* Toast */}
      {toast && (
        <div className={`fixed top-4 right-4 z-50 px-4 py-3 rounded-lg shadow-lg text-sm font-medium ${toast.ok ? "bg-green-600 text-white" : "bg-red-600 text-white"}`}>
          {toast.msg}
        </div>
      )}

      {/* Banner */}
      <section className="rounded-2xl p-5 text-white mb-5"
        style={{ background: "linear-gradient(135deg, #7c3aed 0%, #2563eb 100%)" }}>
        <h1 className="text-2xl font-bold m-0">QE实验选股</h1>
        <p className="mt-2 text-sm opacity-90">基于Quant Evolver回测实验结果，选择最佳因子组合进行选股推理</p>
      </section>

      <div className="grid gap-5" style={{ gridTemplateColumns: "1fr 380px" }}>
        {/* 左侧：实验排行 */}
        <div>
          {/* 实验排行榜 */}
          <section className="bg-white rounded-xl p-4 shadow-sm mb-4">
            <div className="flex justify-between items-center mb-3">
              <h2 className="text-base font-semibold m-0">实验排行榜</h2>
              <div className="flex items-center gap-2">
                <span className="text-xs text-gray-400">排序：</span>
                <select value={sortKey} onChange={e => setSortKey(e.target.value)}
                  className="px-2 py-1 text-[11px] border border-gray-300 rounded" title="排序指标">
                  <option value="sharpe">Sharpe</option>
                  <option value="annualized_return">年化收益</option>
                  <option value="IC">IC</option>
                  <option value="ICIR">ICIR</option>
                  <option value="Rank IC">Rank IC</option>
                  <option value="max_drawdown">最大回撤</option>
                  <option value="information_ratio">IR</option>
                </select>
                <button onClick={() => setSortDir(d => d === "asc" ? "desc" : "asc")}
                  className="px-1.5 py-1 text-[11px] border border-gray-300 rounded bg-white cursor-pointer">
                  {sortDir === "asc" ? "↑升序" : "↓降序"}
                </button>
                <button onClick={loadExperiments} disabled={loading}
                  className="px-2 py-1 text-[11px] border border-gray-300 rounded bg-white cursor-pointer disabled:opacity-50">
                  {loading ? "..." : "刷新"}
                </button>
              </div>
            </div>

            {sortedExperiments.length === 0 ? (
              <div className="text-center py-10 text-gray-400 text-sm">
                {loading ? "加载中..." : "暂无已完成的实验。请先在实验历史页面同步结果。"}
              </div>
            ) : (
              <div className="space-y-2 max-h-[520px] overflow-y-auto">
                {sortedExperiments.map((exp, idx) => {
                  const metrics = getMetrics(exp);
                  const isSelected = selectedExpId === exp.experiment_id;
                  return (
                    <div key={exp.experiment_id}
                      onClick={() => setSelectedExpId(exp.experiment_id)}
                      className={`p-3 rounded-lg border-2 cursor-pointer transition-all ${isSelected ? "border-purple-500 bg-purple-50" : "border-transparent bg-gray-50 hover:bg-gray-100"}`}>
                      <div className="flex items-center gap-2">
                        <span className={`w-6 h-6 rounded-full flex items-center justify-center text-[10px] font-bold shrink-0 ${idx === 0 ? "bg-amber-400 text-white" : idx === 1 ? "bg-gray-300 text-white" : idx === 2 ? "bg-amber-600 text-white" : "bg-gray-200 text-gray-500"}`}>
                          {idx + 1}
                        </span>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2">
                            <span className="font-semibold text-xs truncate">{exp.experiment_name}</span>
                            {exp.model_id && (
                              <span className="px-1.5 py-0.5 rounded text-[9px] font-semibold bg-blue-100 text-blue-700 shrink-0">
                                {MODEL_NAMES[exp.model_id] || exp.model_id}
                              </span>
                            )}
                            <span className="px-1.5 py-0.5 rounded text-[9px] font-semibold bg-violet-100 text-violet-700 shrink-0">
                              {(exp.factor_names || []).length} 因子
                            </span>
                          </div>
                        </div>
                        {isSelected && <span className="text-purple-600 text-xs font-bold shrink-0">✓ 已选</span>}
                      </div>
                      {/* 关键指标 */}
                      <div className="flex gap-1.5 mt-2 flex-wrap">
                        {KEY_METRICS.map(km => {
                          const val = metrics[km.key];
                          if (val == null) return null;
                          const numVal = typeof val === "number" ? val : parseFloat(val);
                          if (isNaN(numVal)) return null;
                          const isGood = km.good(numVal);
                          return (
                            <div key={km.key}
                              className={`text-center px-1.5 py-0.5 rounded border ${isGood ? "bg-green-50 border-green-200" : "bg-red-50 border-red-200"}`}
                              style={{ minWidth: 48 }}>
                              <div className={`text-[10px] font-bold ${isGood ? "text-green-600" : "text-red-600"}`}>
                                {km.fmt(numVal)}
                              </div>
                              <div className="text-[8px] text-gray-400">{km.label}</div>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </section>

          {/* 选股结果 */}
          {selectionResult.length > 0 && (
            <section className="bg-white rounded-xl p-4 shadow-sm">
              <div className="flex justify-between items-center mb-3">
                <h2 className="text-base font-semibold m-0">
                  选股结果
                  <span className="ml-2 text-sm font-normal text-gray-500">{selectionResult.length} 只</span>
                </h2>
                <div className="flex items-center gap-2">
                  <label className="flex items-center gap-1 text-[11px] cursor-pointer">
                    <input type="checkbox"
                      checked={selectedCount === selectionResult.length && selectionResult.length > 0}
                      onChange={e => {
                        const next: Record<string, boolean> = {};
                        selectionResult.forEach(r => { next[r.symbol] = e.target.checked; });
                        setSelectedSymbols(next);
                      }} />
                    全选
                  </label>
                  <span className="text-[11px] text-gray-400">已选 {selectedCount}</span>
                </div>
              </div>
              <div className="overflow-x-auto">
                <table className="w-full text-[11px]" style={{ borderCollapse: "collapse" }}>
                  <thead>
                    <tr className="border-b-2 border-gray-200 text-left">
                      <th className="px-2 py-1.5 w-8"></th>
                      <th className="px-2 py-1.5 font-semibold">排名</th>
                      <th className="px-2 py-1.5 font-semibold">代码</th>
                      <th className="px-2 py-1.5 font-semibold">名称</th>
                      <th className="px-2 py-1.5 font-semibold">评分</th>
                      <th className="px-2 py-1.5 font-semibold">现价</th>
                      <th className="px-2 py-1.5 font-semibold">涨跌幅</th>
                    </tr>
                  </thead>
                  <tbody>
                    {selectionResult.map((r, i) => (
                      <tr key={r.symbol} className="border-b border-gray-100 hover:bg-gray-50">
                        <td className="px-2 py-1">
                          <input type="checkbox" checked={!!selectedSymbols[r.symbol]}
                            onChange={e => setSelectedSymbols(prev => ({ ...prev, [r.symbol]: e.target.checked }))} />
                        </td>
                        <td className="px-2 py-1 text-gray-500">{r.rank ?? i + 1}</td>
                        <td className="px-2 py-1 font-mono font-semibold">{r.symbol}</td>
                        <td className="px-2 py-1">{r.name || "-"}</td>
                        <td className="px-2 py-1 font-semibold text-purple-700">{r.score?.toFixed(4) ?? "-"}</td>
                        <td className="px-2 py-1">{r.price != null ? r.price.toFixed(2) : "-"}</td>
                        <td className={`px-2 py-1 font-semibold ${(r.pct_change ?? 0) >= 0 ? "text-red-600" : "text-green-600"}`}>
                          {r.pct_change != null ? (r.pct_change >= 0 ? "+" : "") + r.pct_change.toFixed(2) + "%" : "-"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}

          {/* 选股错误 */}
          {selectionError && (
            <section className="bg-white rounded-xl p-4 shadow-sm mt-4">
              <div className="text-red-600 text-sm">{selectionError}</div>
            </section>
          )}
        </div>

        {/* 右侧：选股操作面板 */}
        <div>
          {/* 选中实验信息 */}
          <section className="bg-white rounded-xl p-4 shadow-sm mb-4">
            <h2 className="text-base font-semibold mb-3">选股配置</h2>
            {selectedExp ? (
              <div className="space-y-3">
                <div className="p-3 bg-purple-50 rounded-lg border border-purple-200">
                  <div className="font-semibold text-sm text-purple-800">{selectedExp.experiment_name}</div>
                  <div className="text-[11px] text-gray-500 mt-1">
                    {(selectedExp.factor_names || []).length} 个因子 · {MODEL_NAMES[selectedExp.model_id || ""] || selectedExp.model_id || "默认模型"}
                  </div>
                </div>

                <div>
                  <label className="text-xs font-semibold text-gray-600 block mb-1">推理日期</label>
                  <input type="date" value={tradeDate} onChange={e => setTradeDate(e.target.value)}
                    className="w-full px-2 py-1.5 text-xs border border-gray-300 rounded" />
                </div>

                <div>
                  <label className="text-xs font-semibold text-gray-600 block mb-1">选股数量 (Top K)</label>
                  <input type="number" value={topK} onChange={e => setTopK(Number(e.target.value))}
                    min={1} max={500}
                    className="w-full px-2 py-1.5 text-xs border border-gray-300 rounded" />
                </div>

                <button onClick={runSelection} disabled={running}
                  className="w-full py-2.5 bg-purple-600 text-white font-semibold rounded-lg border-none cursor-pointer text-sm disabled:opacity-50 hover:bg-purple-700">
                  {running ? "选股中..." : "开始选股"}
                </button>
              </div>
            ) : (
              <div className="text-center py-8 text-gray-400 text-sm">
                请在左侧排行榜中选择一个实验
              </div>
            )}
          </section>

          {/* 添加到自选股 */}
          {selectionResult.length > 0 && (
            <section className="bg-white rounded-xl p-4 shadow-sm mb-4">
              <h2 className="text-base font-semibold mb-3">添加到自选</h2>
              <div className="space-y-2">
                <select value={targetCategoryId} onChange={e => setTargetCategoryId(Number(e.target.value) || "")}
                  className="w-full px-2 py-1.5 text-xs border border-gray-300 rounded" title="自选股分类">
                  <option value="">选择分类</option>
                  {categories.map(c => (
                    <option key={c.id} value={c.id}>{c.name}</option>
                  ))}
                </select>
                <button onClick={addToWatchlist} disabled={adding || selectedCount === 0}
                  className="w-full py-2 bg-blue-600 text-white font-semibold rounded-lg border-none cursor-pointer text-sm disabled:opacity-50 hover:bg-blue-700">
                  {adding ? "添加中..." : `添加 ${selectedCount} 只到自选`}
                </button>
              </div>
            </section>
          )}

          {/* 选股日志 */}
          {selectionLogs.length > 0 && (
            <section className="bg-white rounded-xl p-4 shadow-sm">
              <h2 className="text-xs font-semibold text-gray-600 mb-2">执行日志</h2>
              <div className="bg-slate-800 text-slate-300 p-3 rounded-lg text-[10px] font-mono max-h-48 overflow-auto">
                {selectionLogs.map((log, i) => (
                  <div key={i} className="leading-relaxed">{log}</div>
                ))}
              </div>
            </section>
          )}
        </div>
      </div>
    </main>
  );
}
