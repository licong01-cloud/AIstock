"use client";

import { useEffect, useMemo, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface MainForceRecommendation {
  rank: number;
  symbol: string;
  name: string;
  reasons: string[];
  highlights?: string | null;
  risks?: string | null;
  position?: string | null;
  investment_period?: string | null;
  stock_data: Record<string, any>;
}

interface MainForceAnalyzeResponse {
  success: boolean;
  error?: string | null;
  total_stocks: number;
  filtered_stocks: number;
  params: Record<string, any>;
  final_recommendations: MainForceRecommendation[];
  fund_flow_analysis?: string | null;
  industry_analysis?: string | null;
  fundamental_analysis?: string | null;
  candidates: Record<string, any>[];
  report_markdown?: string | null;
  report_html?: string | null;
}

interface BatchHistorySummary {
  total_records: number;
  total_stocks_analyzed: number;
  total_success: number;
  total_failed: number;
  average_time: number;
  success_rate: number;
}

interface BatchHistoryRecord {
  id: number;
  analysis_date: string;
  batch_count: number;
  analysis_mode: string;
  success_count: number;
  failed_count: number;
  total_time: number;
  created_at: string;
  results: Record<string, any>[];
}

type DateOption = "最近3个月" | "最近6个月" | "最近1年" | "自定义日期";
type MarketOption = "全部" | "A股+科创板" | "北交所";
type ActiveTab = "analysis" | "history";

type ModelKey = string;

const MODEL_OPTIONS: { key: ModelKey; label: string }[] = [
  { key: "deepseek-chat", label: "DeepSeek Chat (默认)" },
  { key: "deepseek-reasoner", label: "DeepSeek Reasoner (推理增强)" },
  { key: "qwen-plus", label: "qwen-plus (阿里百炼)" },
  { key: "qwen-plus-latest", label: "qwen-plus-latest (阿里百炼)" },
  { key: "qwen-flash", label: "qwen-flash (阿里百炼)" },
  { key: "qwen-turbo", label: "qwen-turbo (阿里百炼)" },
  { key: "qwen3-max", label: "qwen-max (阿里百炼)" },
  { key: "qwen-long", label: "qwen-long (阿里百炼)" },
  {
    key: "deepseek-ai/DeepSeek-R1-0528-Qwen3-8B",
    label: "DeepSeek-R1 免费(硅基流动)",
  },
  { key: "Qwen/Qwen2.5-7B-Instruct", label: "Qwen 免费(硅基流动)" },
  {
    key: "Pro/deepseek-ai/DeepSeek-V3.1-Terminus",
    label: "DeepSeek-V3.1-Terminus (硅基流动)",
  },
  { key: "deepseek-ai/DeepSeek-R1", label: "DeepSeek-R1 (硅基流动)" },
  {
    key: "Qwen/Qwen3-235B-A22B-Thinking-2507",
    label: "Qwen3-235B (硅基流动)",
  },
  { key: "zai-org/GLM-4.6", label: "智谱(硅基流动)" },
  { key: "moonshotai/Kimi-K2-Instruct-0905", label: "Kimi (硅基流动)" },
  { key: "Ring-1T", label: "蚂蚁百灵 (硅基流动)" },
  { key: "step3", label: "阶跃星辰(硅基流动)" },
];

function formatCustomDateLabel(value: string): string | null {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return `${d.getFullYear()}年${d.getMonth() + 1}月${d.getDate()}日`;
}

function buildCsv(rows: Record<string, any>[]): string {
  if (!rows.length) return "";
  const allKeys = new Set<string>();
  rows.forEach((r) => {
    Object.keys(r).forEach((k) => allKeys.add(k));
  });
  const header = Array.from(allKeys);
  const escape = (v: any) => {
    if (v === null || v === undefined) return "";
    const s = String(v).replace(/"/g, '""');
    if (/[",\n]/.test(s)) return `"${s}"`;
    return s;
  };
  const lines = [header.map(escape).join(",")];
  rows.forEach((r) => {
    lines.push(header.map((k) => escape(r[k])).join(","));
  });
  return lines.join("\n");
}

function triggerDownload(content: string, filename: string, mime: string) {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

export default function MainForcePage() {
  const [activeTab, setActiveTab] = useState<ActiveTab>("analysis");

  const [dateOption, setDateOption] = useState<DateOption>("最近3个月");
  const [customStartDate, setCustomStartDate] = useState<string>("");
  const [finalN, setFinalN] = useState<number>(5);
  const [marketOption, setMarketOption] = useState<MarketOption>("全部");
  const [maxChange, setMaxChange] = useState<number>(30);
  const [minCap, setMinCap] = useState<number>(50);
  const [maxCap, setMaxCap] = useState<number>(5000);
  const [model, setModel] = useState<ModelKey>("deepseek-chat");

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<MainForceAnalyzeResponse | null>(null);

  const [watchlistCategories, setWatchlistCategories] = useState<
    { id: number; name: string }[]
  >([]);
  const [watchlistSelCatName, setWatchlistSelCatName] = useState<string>("");
  const [watchlistMoveIfExists, setWatchlistMoveIfExists] = useState(false);
  const [watchlistSelectedCodes, setWatchlistSelectedCodes] = useState<string[]>(
    [],
  );
  const [watchlistNewCatName, setWatchlistNewCatName] = useState<string>("");
  const [watchlistMessage, setWatchlistMessage] = useState<string | null>(null);
  const [watchlistError, setWatchlistError] = useState<string | null>(null);

  const [historySummary, setHistorySummary] = useState<BatchHistorySummary | null>(
    null,
  );
  const [historyRecords, setHistoryRecords] = useState<BatchHistoryRecord[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [deletingId, setDeletingId] = useState<number | null>(null);

  const [candidateSortKey, setCandidateSortKey] = useState<string | null>(null);
  const [candidateSortAsc, setCandidateSortAsc] = useState<boolean>(true);

  const [lastDateLabel, setLastDateLabel] = useState<string>("");

  const marketCode = useMemo(() => {
    if (marketOption === "A股+科创板") return "asr";
    if (marketOption === "北交所") return "bse";
    return "all";
  }, [marketOption]);

  const watchlistCategoryNames = useMemo(
    () => watchlistCategories.map((c) => c.name),
    [watchlistCategories],
  );

  const candidateCodeOptions = useMemo(() => {
    if (!result?.candidates?.length) return [] as string[];
    const set = new Set<string>();
    for (const row of result.candidates) {
      const raw =
        (row["股票代码"] as string) ||
        (row["code"] as string) ||
        (row["symbol"] as string) ||
        "";
      if (!raw) continue;
      const s = String(raw).trim();
      if (!s) continue;
      const clean = s.includes(".") ? s.split(".")[0] : s;
      set.add(clean);
    }
    return Array.from(set);
  }, [result]);

  async function handleAnalyze() {
    setLoading(true);
    setError(null);
    setResult(null);

    let start_date: string | null = null;
    let days_ago: number | null = null;
    let uiDateLabel = "";

    if (dateOption === "自定义日期") {
      const label = formatCustomDateLabel(customStartDate);
      if (!label) {
        setLoading(false);
        setError("请选择有效的开始日期");
        return;
      }
      start_date = label;
      uiDateLabel = label;
    } else {
      if (dateOption === "最近3个月") days_ago = 90;
      else if (dateOption === "最近6个月") days_ago = 180;
      else if (dateOption === "最近1年") days_ago = 365;
      uiDateLabel = dateOption;
    }

    setLastDateLabel(uiDateLabel);

    const payload = {
      start_date,
      days_ago,
      final_n: finalN,
      max_range_change: maxChange,
      min_market_cap: minCap,
      max_market_cap: maxCap,
      market: marketCode,
      model,
    };

    try {
      const res = await fetch(`${API_BASE}/main-force/analyze`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        throw new Error(`请求失败: ${res.status}`);
      }
      const data: MainForceAnalyzeResponse = await res.json();
      if (!data.success) {
        setError(data.error || "分析失败");
      }
      setResult(data);
    } catch (e: any) {
      setError(e?.message || "未知错误");
    } finally {
      setLoading(false);
    }
  }

  function handleDownloadCandidatesCsv() {
    if (!result?.candidates?.length) return;
    const csv = buildCsv(result.candidates);
    const blob = new Blob([csv], {
      type: "text/csv;charset=utf-8;",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `main_force_candidates_${new Date()
      .toISOString()
      .slice(0, 10)}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  function handlePrefillBatchFromCandidates(count: number) {
    if (!result?.candidates?.length) return;
    const codes: string[] = [];
    for (const row of result.candidates.slice(0, count)) {
      const code =
        (row["股票代码"] as string) ||
        (row["code"] as string) ||
        (row["symbol"] as string) ||
        "";
      if (code) {
        const clean = String(code).split(".")[0];
        codes.push(clean);
      }
    }
    if (!codes.length) return;
    try {
      if (typeof window !== "undefined") {
        window.localStorage.setItem(
          "analysis_prefill_batch_codes",
          codes.join("\n"),
        );
        window.alert(
          `已将前 ${codes.length} 只股票代码写入批量分析预填，请前往“股票分析”页面使用批量分析。`,
        );
      }
    } catch {
      // ignore
    }
  }

  async function loadHistory() {
    setHistoryLoading(true);
    setHistoryError(null);
    try {
      const [sRes, hRes] = await Promise.all([
        fetch(`${API_BASE}/main-force/batch/history/summary`),
        fetch(`${API_BASE}/main-force/batch/history?limit=50`),
      ]);
      if (!sRes.ok) throw new Error(`统计请求失败: ${sRes.status}`);
      if (!hRes.ok) throw new Error(`列表请求失败: ${hRes.status}`);
      const summary: BatchHistorySummary = await sRes.json();
      const list = await hRes.json();
      const items: BatchHistoryRecord[] = list.items || [];
      setHistorySummary(summary);
      setHistoryRecords(items);
    } catch (e: any) {
      setHistoryError(e?.message || "加载历史记录失败");
      setHistorySummary(null);
      setHistoryRecords([]);
    } finally {
      setHistoryLoading(false);
    }
  }

  async function loadWatchlistCategories() {
    try {
      const res = await fetch(`${API_BASE}/watchlist/categories`);
      if (!res.ok) return;
      const data = await res.json();
      const items = Array.isArray(data) ? data : [];
      setWatchlistCategories(
        items.map((it: any) => ({
          id: Number(it.id),
          name: String(it.name),
        })),
      );
    } catch {
      // ignore
    }
  }

  useEffect(() => {
    if (activeTab === "history") {
      void loadHistory();
    }
  }, [activeTab]);

  useEffect(() => {
    void loadWatchlistCategories();
  }, []);

  async function handleDeleteRecord(id: number) {
    if (typeof window !== "undefined") {
      const ok = window.confirm("确认删除该历史记录？此操作不可恢复。");
      if (!ok) return;
    }
    setDeletingId(id);
    try {
      const res = await fetch(`${API_BASE}/main-force/batch/history/${id}`, {
        method: "DELETE",
      });
      if (!res.ok) throw new Error(`删除失败: ${res.status}`);
      const data = await res.json();
      if (!data.success) throw new Error("删除未成功");
      setHistoryRecords((prev) => prev.filter((r) => r.id !== id));
    } catch (e: any) {
      setHistoryError(e?.message || "删除记录失败");
    } finally {
      setDeletingId(null);
    }
  }

  useEffect(() => {
    if (!result?.candidates?.length) {
      setWatchlistSelectedCodes([]);
      return;
    }
    const rows = result.candidates;
    const codes: string[] = [];
    for (const row of rows) {
      const raw =
        (row["股票代码"] as string) ||
        (row["code"] as string) ||
        (row["symbol"] as string) ||
        "";
      if (!raw) continue;
      const s = String(raw).trim();
      if (!s) continue;
      const clean = s.includes(".") ? s.split(".")[0] : s;
      codes.push(clean);
      if (codes.length >= 10) break;
    }
    setWatchlistSelectedCodes(codes);
  }, [result]);

  async function handleCreateWatchlistCategory() {
    const n = watchlistNewCatName.trim();
    if (!n) return;
    setWatchlistError(null);
    setWatchlistMessage(null);
    try {
      const res = await fetch(`${API_BASE}/watchlist/categories`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: n, description: null }),
      });
      if (!res.ok) throw new Error(`创建分类失败: ${res.status}`);
      const data = await res.json();
      const cid = Number(data.id);
      await loadWatchlistCategories();
      const nextName = n;
      setWatchlistSelCatName(nextName);
      setWatchlistNewCatName("");
      setWatchlistMessage(`已创建分类：${nextName} (ID=${cid})`);
    } catch (e: any) {
      setWatchlistError(e?.message || "创建分类失败");
    }
  }

  async function handleAddCandidatesToWatchlist() {
    setWatchlistError(null);
    setWatchlistMessage(null);
    if (!watchlistSelectedCodes.length) {
      setWatchlistError("请选择至少一只股票代码");
      return;
    }
    const codes = Array.from(new Set(watchlistSelectedCodes)).filter(Boolean);
    if (!codes.length) {
      setWatchlistError("代码列表为空");
      return;
    }
    try {
      let targetName = watchlistSelCatName.trim() || "默认";
      let target = watchlistCategories.find((c) => c.name === targetName) || null;
      if (!target) {
        const resCat = await fetch(`${API_BASE}/watchlist/categories`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name: targetName, description: null }),
        });
        if (!resCat.ok) throw new Error(`创建分类失败: ${resCat.status}`);
        const data = await resCat.json();
        const cid = Number(data.id);
        await loadWatchlistCategories();
        target = { id: cid, name: targetName };
        targetName = target.name;
        setWatchlistSelCatName(targetName);
      }
      if (!target) throw new Error("无法确定目标分类");
      const res = await fetch(`${API_BASE}/watchlist/items/bulk-add`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          codes,
          category_id: target.id,
          on_conflict: watchlistMoveIfExists ? "move" : "ignore",
        }),
      });
      if (!res.ok) throw new Error(`批量加入自选失败: ${res.status}`);
      const data = await res.json();
      const added = data.added ?? 0;
      const skipped = data.skipped ?? 0;
      const moved = data.moved ?? 0;
      setWatchlistMessage(
        `添加完成：新增 ${added}，跳过 ${skipped}，移动 ${moved}`,
      );
    } catch (e: any) {
      setWatchlistError(e?.message || "批量加入自选失败");
    }
  }

  const hasResult = !!result && result.success;
  const recommendations = result?.final_recommendations || [];
  const candidates = result?.candidates || [];

  const candidateKeys = useMemo(
    () => (candidates.length ? Object.keys(candidates[0]) : []),
    [candidates],
  );

  const sortedCandidates = useMemo(() => {
    if (!candidates.length) return [] as Record<string, any>[];
    if (!candidateSortKey) return candidates;
    const key = candidateSortKey;
    const asc = candidateSortAsc;
    const copy = [...candidates];
    copy.sort((a, b) => {
      const va = a[key];
      const vb = b[key];
      if (va == null && vb == null) return 0;
      if (va == null) return asc ? -1 : 1;
      if (vb == null) return asc ? 1 : -1;
      const na = Number(va);
      const nb = Number(vb);
      const aIsNum = !Number.isNaN(na);
      const bIsNum = !Number.isNaN(nb);
      let cmp = 0;
      if (aIsNum && bIsNum) {
        cmp = na === nb ? 0 : na < nb ? -1 : 1;
      } else {
        const sa = String(va);
        const sb = String(vb);
        cmp = sa.localeCompare(sb, "zh-Hans-CN");
      }
      return asc ? cmp : -cmp;
    });
    return copy;
  }, [candidates, candidateSortKey, candidateSortAsc]);

  function handleCandidateSort(key: string) {
    setCandidateSortKey((prevKey) => {
      if (prevKey === key) {
        setCandidateSortAsc((prevAsc) => !prevAsc);
        return prevKey;
      }
      setCandidateSortAsc(true);
      return key;
    });
  }

  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
          borderRadius: 16,
          padding: 20,
          color: "#fff",
          marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>
          🎯 主力选股 - 智能筛选优质标的
        </h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          复用旧版主力选股全流程：主力资金 → 智能筛选 → 多智能体分析 → 综合决策。主力选股是通过对市场数据进行分析和筛选，找出最有投资价值的股票。通过本页面，您可以进行主力选股分析，并查看分析结果。
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
          onClick={() => setActiveTab("analysis")}
          style={{
            padding: "8px 16px",
            borderRadius: 999,
            border: "none",
            cursor: "pointer",
            fontWeight: 600,
            backgroundColor:
              activeTab === "analysis" ? "#4f46e5" : "rgba(15,23,42,0.06)",
            color: activeTab === "analysis" ? "#fff" : "#111827",
          }}
        >
          📊 主力选股分析
        </button>
        <button
          type="button"
          onClick={() => setActiveTab("history")}
          style={{
            padding: "8px 16px",
            borderRadius: 999,
            border: "none",
            cursor: "pointer",
            fontWeight: 600,
            backgroundColor:
              activeTab === "history" ? "#4f46e5" : "rgba(15,23,42,0.06)",
            color: activeTab === "history" ? "#fff" : "#111827",
          }}
        >
          📚 批量分析历史
        </button>
      </div>

      {activeTab === "analysis" && (
        <section
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 16,
            boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
            marginBottom: 16,
          }}
        >
          <h2 style={{ marginTop: 0, fontSize: 18 }}>参数设置</h2>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
              gap: 16,
              marginBottom: 12,
            }}
          >
            <div>
              <label style={{ fontWeight: 600, fontSize: 13 }}>时间区间</label>
              <select
                value={dateOption}
                onChange={(e) =>
                  setDateOption(e.target.value as DateOption)
                }
                style={{
                  marginTop: 4,
                  width: "100%",
                  padding: "6px 8px",
                  borderRadius: 8,
                  border: "1px solid #ddd",
                }}
              >
                <option value="最近3个月">最近3个月</option>
                <option value="最近6个月">最近6个月</option>
                <option value="最近1年">最近1年</option>
                <option value="自定义日期">自定义日期</option>
              </select>
              {dateOption === "自定义日期" && (
                <input
                  type="date"
                  value={customStartDate}
                  onChange={(e) => setCustomStartDate(e.target.value)}
                  style={{
                    marginTop: 6,
                    width: "100%",
                    padding: "6px 8px",
                    borderRadius: 8,
                    border: "1px solid #ddd",
                  }}
                />
              )}
            </div>
            <div>
              <label style={{ fontWeight: 600, fontSize: 13 }}>
                最终精选数量
              </label>
              <input
                type="number"
                min={3}
                max={10}
                value={finalN}
                onChange={(e) =>
                  setFinalN(Math.max(3, Math.min(10, Number(e.target.value) || 3)))
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
            <div>
              <label style={{ fontWeight: 600, fontSize: 13 }}>市场</label>
              <div style={{ marginTop: 4, display: "flex", gap: 8, flexWrap: "wrap" }}>
                {(["全部", "A股+科创板", "北交所"] as MarketOption[]).map(
                  (opt) => (
                    <button
                      key={opt}
                      type="button"
                      onClick={() => setMarketOption(opt)}
                      style={{
                        padding: "4px 10px",
                        borderRadius: 999,
                        border: "1px solid #ddd",
                        backgroundColor:
                          marketOption === opt
                            ? "#4f46e5"
                            : "rgba(249,250,251,1)",
                        color: marketOption === opt ? "#fff" : "#111827",
                        fontSize: 12,
                        cursor: "pointer",
                      }}
                    >
                      {opt}
                    </button>
                  ),
                )}
              </div>
              {marketOption === "北交所" && (
                <p style={{ marginTop: 4, fontSize: 12, color: "#4b5563" }}>
                  📌 当前选择：仅分析北交所股票（8/4 开头代码）。
                </p>
              )}
            </div>
          </div>

          <details style={{ marginBottom: 12 }}>
            <summary
              style={{
                cursor: "pointer",
                fontWeight: 600,
                fontSize: 13,
              }}
            >
              ⚙️ 高级筛选参数
            </summary>
            <div
              style={{
                marginTop: 8,
                display: "grid",
                gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
                gap: 16,
              }}
            >
              <div>
                <label style={{ fontWeight: 600, fontSize: 13 }}>
                  最大涨跌幅 (%)
                </label>
                <input
                  type="number"
                  min={5}
                  max={200}
                  step={5}
                  value={maxChange}
                  onChange={(e) =>
                    setMaxChange(
                      Math.max(5, Math.min(200, Number(e.target.value) || 30)),
                    )
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
              <div>
                <label style={{ fontWeight: 600, fontSize: 13 }}>
                  最小市值 (亿)
                </label>
                <input
                  type="number"
                  min={10}
                  max={500}
                  step={10}
                  value={minCap}
                  onChange={(e) =>
                    setMinCap(
                      Math.max(10, Math.min(500, Number(e.target.value) || 50)),
                    )
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
              <div>
                <label style={{ fontWeight: 600, fontSize: 13 }}>
                  最大市值 (亿)
                </label>
                <input
                  type="number"
                  min={50}
                  max={50000}
                  step={100}
                  value={maxCap}
                  onChange={(e) =>
                    setMaxCap(
                      Math.max(
                        50,
                        Math.min(50000, Number(e.target.value) || 5000),
                      ),
                    )
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
          </details>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "minmax(0, 2fr) minmax(0, 1fr)",
              gap: 16,
              alignItems: "flex-end",
            }}
          >
            <div>
              <label style={{ fontWeight: 600, fontSize: 13 }}>AI 模型</label>
              <select
                value={model}
                onChange={(e) => setModel(e.target.value)}
                style={{
                  marginTop: 4,
                  width: "100%",
                  padding: "6px 8px",
                  borderRadius: 8,
                  border: "1px solid #ddd",
                }}
              >
                {MODEL_OPTIONS.map((m) => (
                  <option key={m.key} value={m.key}>
                    {m.label}
                  </option>
                ))}
              </select>
            </div>
            <div
              style={{
                display: "flex",
                gap: 8,
                justifyContent: "flex-end",
              }}
            >
              <button
                type="button"
                onClick={handleAnalyze}
                disabled={loading}
                style={{
                  padding: "8px 16px",
                  borderRadius: 8,
                  border: "none",
                  background:
                    "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
                  color: "#fff",
                  fontWeight: 600,
                  cursor: loading ? "default" : "pointer",
                }}
              >
                {loading ? "分析中..." : "🚀 开始主力选股"}
              </button>
              <button
                type="button"
                onClick={() => {
                  setResult(null);
                  setError(null);
                }}
                style={{
                  padding: "8px 16px",
                  borderRadius: 8,
                  border: "1px solid #e5e7eb",
                  background: "#fff",
                  color: "#111827",
                  fontWeight: 500,
                  cursor: "pointer",
                }}
              >
                🔄 清除结果
              </button>
            </div>
          </div>

          {error && (
            <p style={{ marginTop: 8, color: "#b91c1c", fontSize: 13 }}>
              错误：{error}
            </p>
          )}
        </section>
      )}

      {activeTab === "analysis" && hasResult && (
        <>
          <section
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 16,
              boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
              marginBottom: 16,
            }}
          >
            <h2 style={{ marginTop: 0, fontSize: 18 }}>整体统计</h2>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
                gap: 16,
              }}
            >
              <div className="metric-card">
                <div style={{ fontWeight: 600, fontSize: 13 }}>获取股票数</div>
                <div style={{ marginTop: 4, fontSize: 20 }}>
                  {result?.total_stocks ?? 0}
                </div>
              </div>
              <div className="metric-card">
                <div style={{ fontWeight: 600, fontSize: 13 }}>筛选后</div>
                <div style={{ marginTop: 4, fontSize: 20 }}>
                  {result?.filtered_stocks ?? 0}
                </div>
              </div>
              <div className="metric-card">
                <div style={{ fontWeight: 600, fontSize: 13 }}>最终推荐</div>
                <div style={{ marginTop: 4, fontSize: 20 }}>
                  {recommendations.length}
                </div>
              </div>
            </div>
          </section>

          <section
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 16,
              boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
              marginBottom: 16,
            }}
          >
            <h2 style={{ marginTop: 0, fontSize: 18 }}>🤖 分析师整体报告</h2>
            <div style={{ display: "flex", gap: 8, marginBottom: 8 }}>
              <details style={{ flex: 1 }}>
                <summary style={{ cursor: "pointer", fontWeight: 600 }}>
                  💰 资金流向分析
                </summary>
                <pre
                  style={{
                    whiteSpace: "pre-wrap",
                    marginTop: 8,
                    fontSize: 13,
                    background: "#f9fafb",
                    padding: 8,
                    borderRadius: 8,
                  }}
                >
                  {result?.fund_flow_analysis || "暂无资金流向分析"}
                </pre>
              </details>
              <details style={{ flex: 1 }}>
                <summary style={{ cursor: "pointer", fontWeight: 600 }}>
                  📊 行业板块分析
                </summary>
                <pre
                  style={{
                    whiteSpace: "pre-wrap",
                    marginTop: 8,
                    fontSize: 13,
                    background: "#f9fafb",
                    padding: 8,
                    borderRadius: 8,
                  }}
                >
                  {result?.industry_analysis || "暂无行业板块分析"}
                </pre>
              </details>
              <details style={{ flex: 1 }}>
                <summary style={{ cursor: "pointer", fontWeight: 600 }}>
                  📈 财务基本面分析
                </summary>
                <pre
                  style={{
                    whiteSpace: "pre-wrap",
                    marginTop: 8,
                    fontSize: 13,
                    background: "#f9fafb",
                    padding: 8,
                    borderRadius: 8,
                  }}
                >
                  {result?.fundamental_analysis || "暂无基本面分析"}
                </pre>
              </details>
            </div>
          </section>

          {recommendations.length > 0 && (
            <section
              style={{
                background: "#fff",
                borderRadius: 12,
                padding: 16,
                boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
                marginBottom: 16,
              }}
            >
              <h2 style={{ marginTop: 0, fontSize: 18 }}>⭐ 精选推荐</h2>
              {recommendations.map((rec) => (
                <details
                  key={rec.rank}
                  style={{ marginBottom: 8 }}
                  open={rec.rank <= 3}
                >
                  <summary
                    style={{
                      cursor: "pointer",
                      fontWeight: 600,
                      fontSize: 14,
                    }}
                  >
                    【第{rec.rank}名】{rec.symbol} - {rec.name}
                  </summary>
                  <div style={{ marginTop: 8 }}>
                    <div
                      style={{
                        display: "grid",
                        gridTemplateColumns: "minmax(0, 1.5fr) minmax(0, 1fr)",
                        gap: 16,
                      }}
                    >
                      <div>
                        <h4 style={{ margin: "4px 0" }}>📌 推荐理由</h4>
                        <ul style={{ paddingLeft: 20, marginTop: 4 }}>
                          {(rec.reasons || []).map((r, idx) => (
                            <li key={idx} style={{ fontSize: 13 }}>
                              {r}
                            </li>
                          ))}
                        </ul>
                        <h4 style={{ margin: "8px 0 4px" }}>💡 投资亮点</h4>
                        <p style={{ fontSize: 13 }}>
                          {rec.highlights || "N/A"}
                        </p>
                        <h4 style={{ margin: "8px 0 4px" }}>⚠️ 风险提示</h4>
                        <p style={{ fontSize: 13 }}>
                          {rec.risks || "N/A"}
                        </p>
                      </div>
                      <div>
                        <h4 style={{ margin: "4px 0" }}>📊 投资建议</h4>
                        <p style={{ fontSize: 13 }}>
                          建议仓位：{rec.position || "N/A"}
                        </p>
                        <p style={{ fontSize: 13 }}>
                          投资周期：{rec.investment_period || "N/A"}
                        </p>
                        <h4 style={{ margin: "8px 0 4px" }}>📊 股票详细数据</h4>
                        <div
                          style={{
                            maxHeight: 220,
                            overflow: "auto",
                            background: "#f9fafb",
                            padding: 8,
                            borderRadius: 8,
                            fontSize: 12,
                          }}
                        >
                          <table
                            style={{
                              width: "100%",
                              borderCollapse: "collapse",
                            }}
                          >
                            <tbody>
                              {Object.entries(rec.stock_data || {}).map(
                                ([k, v]) => (
                                  <tr key={k}>
                                    <td
                                      style={{
                                        borderBottom:
                                          "1px solid rgba(209, 213, 219, 0.8)",
                                        padding: "2px 4px",
                                        whiteSpace: "nowrap",
                                      }}
                                    >
                                      {k}
                                    </td>
                                    <td
                                      style={{
                                        borderBottom:
                                          "1px solid rgba(209, 213, 219, 0.8)",
                                        padding: "2px 4px",
                                      }}
                                    >
                                      {String(v)}
                                    </td>
                                  </tr>
                                ),
                              )}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    </div>
                  </div>
                </details>
              ))}
            </section>
          )}

          <section
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 16,
              boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
              marginBottom: 16,
              maxWidth: 1200,
              marginLeft: "auto",
              marginRight: "auto",
            }}
          >
            <h2 style={{ marginTop: 0, fontSize: 18 }}>📋 候选股票列表</h2>
            <p style={{ fontSize: 13, color: "#4b5563", marginTop: 0 }}>
              时间区间：{lastDateLabel || dateOption}；共 {candidates.length} 只候选股票。
              下面表格直接来自旧版主力选股的 raw_stocks DataFrame。
            </p>
            <div style={{ marginBottom: 8 }}>
              <button
                type="button"
                onClick={handleDownloadCandidatesCsv}
                disabled={!candidates.length}
                style={{
                  padding: "6px 12px",
                  borderRadius: 8,
                  border: "none",
                  background:
                    "linear-gradient(135deg, #0ea5e9 0%, #6366f1 100%)",
                  color: "#fff",
                  fontSize: 13,
                  cursor: candidates.length ? "pointer" : "default",
                }}
              >
                📥 下载候选列表 CSV
              </button>
            </div>
            <div
              style={{
                maxHeight: 400,
                width: "100%",
                overflowX: "auto",
                overflowY: "auto",
                border: "1px solid #e5e7eb",
                borderRadius: 8,
              }}
            >
              {candidates.length === 0 ? (
                <p style={{ padding: 8, fontSize: 13 }}>暂无候选数据。</p>
              ) : (
                <table
                  style={{
                    minWidth: "max-content",
                    borderCollapse: "collapse",
                    fontSize: 12,
                  }}
                >
                  <thead>
                    <tr>
                      {candidateKeys.map((key) => (
                        <th
                          key={key}
                          style={{
                            position: "sticky",
                            top: 0,
                            background: "#f9fafb",
                            borderBottom: "1px solid #e5e7eb",
                            padding: "4px 6px",
                            textAlign: "left",
                            whiteSpace: "nowrap",
                            cursor: "pointer",
                            userSelect: "none",
                          }}
                          onClick={() => handleCandidateSort(key)}
                        >
                          {key}
                          {candidateSortKey === key && (
                            <span>
                              {candidateSortAsc ? " ▲" : " ▼"}
                            </span>
                          )}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {sortedCandidates.map((row, idx) => (
                      <tr key={idx}>
                        {candidateKeys.map((key) => (
                          <td
                            key={key}
                            style={{
                              borderBottom: "1px solid #f3f4f6",
                              padding: "2px 6px",
                              whiteSpace: "nowrap",
                            }}
                          >
                            {row[key] as any}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            {candidates.length > 0 && (
              <details
                style={{ marginTop: 12 }}
              >
                <summary
                  style={{
                    cursor: "pointer",
                    fontWeight: 600,
                    fontSize: 14,
                  }}
                >
                  ➕ 添加到自选股票池
                </summary>
                <div style={{ marginTop: 8, fontSize: 13 }}>
                  <p style={{ marginTop: 0, marginBottom: 8, color: "#4b5563" }}>
                    将候选股票代码批量加入自选股票池，分类管理逻辑与旧版主力选股保持一致。
                  </p>
                  <div style={{ marginBottom: 8 }}>
                    <label style={{ fontWeight: 600 }}>目标分类</label>
                    <div
                      style={{
                        marginTop: 4,
                        display: "flex",
                        gap: 8,
                        alignItems: "center",
                      }}
                    >
                      <select
                        value={watchlistSelCatName}
                        onChange={(e) => setWatchlistSelCatName(e.target.value)}
                        style={{
                          flex: 1,
                          padding: "6px 8px",
                          borderRadius: 8,
                          border: "1px solid #ddd",
                        }}
                      >
                        <option value="">默认</option>
                        {watchlistCategoryNames.map((name) => (
                          <option key={name} value={name}>
                            {name}
                          </option>
                        ))}
                      </select>
                    </div>
                    <label
                      style={{
                        display: "flex",
                        alignItems: "center",
                        gap: 4,
                        marginTop: 6,
                        fontSize: 12,
                      }}
                    >
                      <input
                        type="checkbox"
                        checked={watchlistMoveIfExists}
                        onChange={(e) =>
                          setWatchlistMoveIfExists(e.target.checked)
                        }
                      />
                      <span>存在则移动（如股票已在其他分类中则移动到当前分类）</span>
                    </label>
                  </div>
                  <div>
                    <label style={{ fontWeight: 600 }}>候选股票代码</label>
                    <select
                      multiple
                      value={watchlistSelectedCodes}
                      onChange={(e) => {
                        const opts = Array.from(e.target.selectedOptions).map(
                          (o) => o.value,
                        );
                        setWatchlistSelectedCodes(opts);
                      }}
                      size={Math.min(10, Math.max(4, candidateCodeOptions.length))}
                      style={{
                        marginTop: 4,
                        width: "100%",
                        padding: 4,
                        borderRadius: 8,
                        border: "1px solid #ddd",
                      }}
                    >
                      {candidateCodeOptions.map((code) => (
                        <option key={code} value={code}>
                          {code}
                        </option>
                      ))}
                    </select>
                    <p style={{ marginTop: 4, fontSize: 12, color: "#6b7280" }}>
                      默认选中前 10 只，如需精确控制可在上方多选列表中调整。
                    </p>
                  </div>
                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: "minmax(0, 2.2fr) minmax(0, 1fr)",
                      gap: 12,
                      alignItems: "flex-end",
                    }}
                  >
                    <div>
                      <label style={{ fontWeight: 600 }}>新建分类（可选）</label>
                      <input
                        type="text"
                        value={watchlistNewCatName}
                        onChange={(e) => setWatchlistNewCatName(e.target.value)}
                        placeholder="输入新分类名后点击创建"
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
                        gap: 8,
                        justifyContent: "flex-end",
                      }}
                    >
                      <button
                        type="button"
                        onClick={handleCreateWatchlistCategory}
                        disabled={!watchlistNewCatName.trim()}
                        style={{
                          padding: "6px 12px",
                          borderRadius: 8,
                          border: "1px solid #e5e7eb",
                          background: "#fff",
                          fontSize: 13,
                          cursor: watchlistNewCatName.trim()
                            ? "pointer"
                            : "default",
                        }}
                      >
                        创建分类
                      </button>
                      <button
                        type="button"
                        onClick={handleAddCandidatesToWatchlist}
                        disabled={!watchlistSelectedCodes.length}
                        style={{
                          padding: "6px 14px",
                          borderRadius: 8,
                          border: "none",
                          background:
                            "linear-gradient(135deg, #22c55e 0%, #16a34a 100%)",
                          color: "#fff",
                          fontSize: 13,
                          cursor: watchlistSelectedCodes.length
                            ? "pointer"
                            : "default",
                        }}
                      >
                        添加到自选
                      </button>
                    </div>
                  </div>
                  {watchlistError && (
                    <p
                      style={{
                        marginTop: 6,
                        fontSize: 12,
                        color: "#b91c1c",
                      }}
                    >
                      错误：{watchlistError}
                    </p>
                  )}
                  {watchlistMessage && (
                    <p
                      style={{
                        marginTop: 4,
                        fontSize: 12,
                        color: "#15803d",
                      }}
                    >
                      {watchlistMessage}
                    </p>
                  )}
                </div>
              </details>
            )}

            {candidates.length > 0 && (
              <div
                style={{
                  marginTop: 12,
                  paddingTop: 8,
                  borderTop: "1px solid #e5e7eb",
                }}
              >
                <h3 style={{ margin: "0 0 4px", fontSize: 15 }}>
                  🚀 批量深度分析（与“股票分析”页面联动）
                </h3>
                <p style={{ fontSize: 12, color: "#4b5563", marginTop: 0 }}>
                  将主力资金 TOP 候选股票的代码写入
                  “股票分析”页的批量分析预填缓存，保持与旧版“主力选股 → 批量分析”
                  的联动语义一致。
                </p>
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  {[10, 20, 30, 50].map((n) => (
                    <button
                      key={n}
                      type="button"
                      onClick={() => handlePrefillBatchFromCandidates(n)}
                      style={{
                        padding: "6px 10px",
                        borderRadius: 999,
                        border: "1px solid #e5e7eb",
                        background: "#fff",
                        fontSize: 12,
                        cursor: "pointer",
                      }}
                    >
                      预填前 {n} 只股票代码
                    </button>
                  ))}
                </div>
              </div>
            )}
          </section>
          {(result?.report_markdown || result?.report_html) && (
            <section
              style={{
                background: "#fff",
                borderRadius: 12,
                padding: 16,
                boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
                marginBottom: 16,
              }}
            >
              <h2 style={{ marginTop: 0, fontSize: 18 }}>📥 下载分析报告</h2>
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
                  gap: 16,
                  alignItems: "flex-start",
                }}
              >
                <div>
                  <h3 style={{ margin: "0 0 4px", fontSize: 15 }}>Markdown 格式</h3>
                  <p style={{ fontSize: 12, color: "#4b5563", marginTop: 0 }}>
                    适合编辑和进一步处理，内容与旧版 Markdown 报告保持一致。
                  </p>
                  <button
                    type="button"
                    onClick={() => {
                      if (!result?.report_markdown) return;
                      const ts = new Date().toISOString().replace(/[:T]/g, "_").slice(0, 15);
                      triggerDownload(
                        result.report_markdown,
                        `主力选股分析报告_${ts}.md`,
                        "text/markdown;charset=utf-8;",
                      );
                    }}
                    disabled={!result?.report_markdown}
                    style={{
                      padding: "6px 14px",
                      borderRadius: 8,
                      border: "none",
                      background:
                        "linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%)",
                      color: "#fff",
                      fontSize: 13,
                      cursor: result?.report_markdown ? "pointer" : "default",
                    }}
                  >
                    📄 下载 Markdown 报告
                  </button>
                </div>
                <div>
                  <h3 style={{ margin: "0 0 4px", fontSize: 15 }}>HTML 格式</h3>
                  <p style={{ fontSize: 12, color: "#4b5563", marginTop: 0 }}>
                    可直接在浏览器中打开查看，排版与旧版 HTML 报告一致。
                  </p>
                  <button
                    type="button"
                    onClick={() => {
                      if (!result?.report_html) return;
                      const ts = new Date().toISOString().replace(/[:T]/g, "_").slice(0, 15);
                      triggerDownload(
                        result.report_html,
                        `主力选股分析报告_${ts}.html`,
                        "text/html;charset=utf-8;",
                      );
                    }}
                    disabled={!result?.report_html}
                    style={{
                      padding: "6px 14px",
                      borderRadius: 8,
                      border: "none",
                      background:
                        "linear-gradient(135deg, #0ea5e9 0%, #6366f1 100%)",
                      color: "#fff",
                      fontSize: 13,
                      cursor: result?.report_html ? "pointer" : "default",
                    }}
                  >
                    🌐 下载 HTML 报告
                  </button>
                </div>
              </div>
            </section>
          )}
        </>
      )}

      {activeTab === "history" && (
        <section
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 16,
            boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
          }}
        >
          <h2 style={{ marginTop: 0, fontSize: 18 }}>📚 主力选股批量分析历史记录</h2>
          {historyLoading && (
            <p style={{ fontSize: 13 }}>正在加载历史记录...</p>
          )}
          {historyError && (
            <p style={{ fontSize: 13, color: "#b91c1c" }}>
              错误：{historyError}
            </p>
          )}
          {historySummary && (
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(5, minmax(0, 1fr))",
                gap: 12,
                marginBottom: 12,
              }}
            >
              <div className="metric-card">
                <div style={{ fontWeight: 600, fontSize: 13 }}>总记录数</div>
                <div style={{ marginTop: 4, fontSize: 18 }}>
                  {historySummary.total_records}
                </div>
              </div>
              <div className="metric-card">
                <div style={{ fontWeight: 600, fontSize: 13 }}>
                  分析股票总数
                </div>
                <div style={{ marginTop: 4, fontSize: 18 }}>
                  {historySummary.total_stocks_analyzed}
                </div>
              </div>
              <div className="metric-card">
                <div style={{ fontWeight: 600, fontSize: 13 }}>成功分析</div>
                <div style={{ marginTop: 4, fontSize: 18 }}>
                  {historySummary.total_success}
                </div>
              </div>
              <div className="metric-card">
                <div style={{ fontWeight: 600, fontSize: 13 }}>成功率</div>
                <div style={{ marginTop: 4, fontSize: 18 }}>
                  {historySummary.success_rate}%
                </div>
              </div>
              <div className="metric-card">
                <div style={{ fontWeight: 600, fontSize: 13 }}>平均耗时</div>
                <div style={{ marginTop: 4, fontSize: 18 }}>
                  {historySummary.average_time.toFixed(1)} 秒
                </div>
              </div>
            </div>
          )}

          <div style={{ marginTop: 8 }}>
            {historyRecords.length === 0 && !historyLoading ? (
              <p style={{ fontSize: 13 }}>暂无批量分析历史记录。</p>
            ) : (
              historyRecords.map((rec, idx) => {
                const successRate =
                  rec.batch_count > 0
                    ? (rec.success_count / rec.batch_count) * 100
                    : 0;
                const avgTime =
                  rec.batch_count > 0
                    ? rec.total_time / rec.batch_count
                    : 0;
                return (
                  <details
                    key={rec.id}
                    style={{ marginBottom: 8 }}
                    open={idx === 0}
                  >
                    <summary
                      style={{
                        cursor: "pointer",
                        fontWeight: 600,
                        fontSize: 14,
                      }}
                    >
                      🔍 {rec.analysis_date} | 共 {rec.batch_count} 只 | 成功
                      {rec.success_count} 只 | {rec.analysis_mode} | 耗时
                      {(rec.total_time / 60).toFixed(1)} 分钟
                    </summary>
                    <div style={{ marginTop: 8 }}>
                      <div
                        style={{
                          display: "grid",
                          gridTemplateColumns:
                            "repeat(4, minmax(0, 1fr))",
                          gap: 8,
                          marginBottom: 8,
                          fontSize: 13,
                        }}
                      >
                        <div>
                          <div>分析时间</div>
                          <div>{rec.analysis_date}</div>
                        </div>
                        <div>
                          <div>分析模式</div>
                          <div>{rec.analysis_mode}</div>
                        </div>
                        <div>
                          <div>成功率</div>
                          <div>{successRate.toFixed(1)}%</div>
                        </div>
                        <div>
                          <div>平均耗时</div>
                          <div>{avgTime.toFixed(1)} 秒</div>
                        </div>
                      </div>

                      <div style={{ marginBottom: 8 }}>
                        <strong>成功分析的股票：</strong>
                        {" "}
                        {
                          rec.results.filter((r) => r.success).length
                        }{" "}
                        只
                      </div>

                      <div
                        style={{
                          maxHeight: 260,
                          overflow: "auto",
                          border: "1px solid #e5e7eb",
                          borderRadius: 8,
                          marginBottom: 8,
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
                                  position: "sticky",
                                  top: 0,
                                  background: "#f9fafb",
                                  borderBottom: "1px solid #e5e7eb",
                                  padding: "4px 6px",
                                  textAlign: "left",
                                }}
                              >
                                股票代码
                              </th>
                              <th
                                style={{
                                  position: "sticky",
                                  top: 0,
                                  background: "#f9fafb",
                                  borderBottom: "1px solid #e5e7eb",
                                  padding: "4px 6px",
                                  textAlign: "left",
                                }}
                              >
                                股票名称
                              </th>
                              <th
                                style={{
                                  position: "sticky",
                                  top: 0,
                                  background: "#f9fafb",
                                  borderBottom: "1px solid #e5e7eb",
                                  padding: "4px 6px",
                                  textAlign: "left",
                                }}
                              >
                                评级
                              </th>
                              <th
                                style={{
                                  position: "sticky",
                                  top: 0,
                                  background: "#f9fafb",
                                  borderBottom: "1px solid #e5e7eb",
                                  padding: "4px 6px",
                                  textAlign: "left",
                                }}
                              >
                                信心度
                              </th>
                            </tr>
                          </thead>
                          <tbody>
                            {rec.results
                              .filter((r) => r.success)
                              .map((r, i) => {
                                const stockInfo = (r.stock_info || {}) as any;
                                const finalDecision = (r.final_decision || {}) as any;
                                const name =
                                  stockInfo.name ||
                                  stockInfo["股票名称"] ||
                                  "";
                                const rating =
                                  finalDecision.rating ||
                                  finalDecision.investment_rating ||
                                  "";
                                const confidence =
                                  finalDecision.confidence_level ?? "";
                                return (
                                  <tr key={i}>
                                    <td
                                      style={{
                                        borderBottom:
                                          "1px solid #f3f4f6",
                                        padding: "2px 6px",
                                      }}
                                    >
                                      {r.symbol}
                                    </td>
                                    <td
                                      style={{
                                        borderBottom:
                                          "1px solid #f3f4f6",
                                        padding: "2px 6px",
                                      }}
                                    >
                                      {name}
                                    </td>
                                    <td
                                      style={{
                                        borderBottom:
                                          "1px solid #f3f4f6",
                                        padding: "2px 6px",
                                      }}
                                    >
                                      {rating}
                                    </td>
                                    <td
                                      style={{
                                        borderBottom:
                                          "1px solid #f3f4f6",
                                        padding: "2px 6px",
                                      }}
                                    >
                                      {confidence}
                                    </td>
                                  </tr>
                                );
                              })}
                          </tbody>
                        </table>
                      </div>

                      <div style={{ marginBottom: 8 }}>
                        <strong>失败分析的股票：</strong>
                        {" "}
                        {
                          rec.results.filter((r) => !r.success).length
                        }{" "}
                        只
                      </div>

                      <div
                        style={{
                          maxHeight: 260,
                          overflow: "auto",
                          border: "1px solid #e5e7eb",
                          borderRadius: 8,
                          marginBottom: 8,
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
                                  position: "sticky",
                                  top: 0,
                                  background: "#f9fafb",
                                  borderBottom: "1px solid #e5e7eb",
                                  padding: "4px 6px",
                                  textAlign: "left",
                                }}
                              >
                                股票代码
                              </th>
                              <th
                                style={{
                                  position: "sticky",
                                  top: 0,
                                  background: "#f9fafb",
                                  borderBottom: "1px solid #e5e7eb",
                                  padding: "4px 6px",
                                  textAlign: "left",
                                }}
                              >
                                股票名称
                              </th>
                              <th
                                style={{
                                  position: "sticky",
                                  top: 0,
                                  background: "#f9fafb",
                                  borderBottom: "1px solid #e5e7eb",
                                  padding: "4px 6px",
                                  textAlign: "left",
                                }}
                              >
                                失败原因
                              </th>
                            </tr>
                          </thead>
                          <tbody>
                            {rec.results
                              .filter((r) => !r.success)
                              .map((r, i) => {
                                const stockInfo = (r.stock_info || {}) as any;
                                const name =
                                  stockInfo.name ||
                                  stockInfo["股票名称"] ||
                                  "";
                                const reason = r.reason || "";
                                return (
                                  <tr key={i}>
                                    <td
                                      style={{
                                        borderBottom:
                                          "1px solid #f3f4f6",
                                        padding: "2px 6px",
                                      }}
                                    >
                                      {r.symbol}
                                    </td>
                                    <td
                                      style={{
                                        borderBottom:
                                          "1px solid #f3f4f6",
                                        padding: "2px 6px",
                                      }}
                                    >
                                      {name}
                                    </td>
                                    <td
                                      style={{
                                        borderBottom:
                                          "1px solid #f3f4f6",
                                        padding: "2px 6px",
                                      }}
                                    >
                                      {reason}
                                    </td>
                                  </tr>
                                );
                              })}
                          </tbody>
                        </table>
                      </div>

                      <div style={{ display: "flex", gap: 8 }}>
                        <button
                          type="button"
                          onClick={() => handleDeleteRecord(rec.id)}
                          disabled={deletingId === rec.id}
                          style={{
                            padding: "6px 12px",
                            borderRadius: 8,
                            border: "1px solid #fecaca",
                            background: "#fef2f2",
                            color: "#b91c1c",
                            fontSize: 12,
                            cursor:
                              deletingId === rec.id ? "default" : "pointer",
                          }}
                        >
                          {deletingId === rec.id ? "正在删除..." : "🗑️ 删除此记录"}
                        </button>
                      </div>
                    </div>
                  </details>
                );
              })
            )}
          </div>
        </section>
      )}
    </main>
  );
}
