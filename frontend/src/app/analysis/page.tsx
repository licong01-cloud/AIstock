"use client";

import { useEffect, useState, useMemo } from "react";
import dynamic from "next/dynamic";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false }) as any;

interface AgentOpinion {
  name: string;
  summary: string;
  score?: number | null;
}

interface StockAnalysisResponse {
  ts_code: string;
  conclusion: string;
  agents?: AgentOpinion[];
  agents_raw?: Record<string, any> | null;
  discussion?: string | null;
  final_decision?: Record<string, any> | null;
  data_fetch_diagnostics?: Record<string, any> | null;
  technical_indicators?: Record<string, any> | null;
  record_id?: number | null;
  saved_to_db?: boolean | null;
}

interface StockKlineSeries {
  dates: string[];
  open: (number | null)[];
  high: (number | null)[];
  low: (number | null)[];
  close: (number | null)[];
}

interface StockQuote {
  symbol: string;
  name: string;
  current_price?: number | null;
  change_percent?: number | null;
   open_price?: number | null;
   high_price?: number | null;
   low_price?: number | null;
   pre_close?: number | null;
   volume?: number | null;
   amount?: number | null;
   quote_source?: string | null;
   quote_timestamp?: string | null;
   week52_high?: number | null;
   week52_low?: number | null;
}

interface StockContextResponse {
  ts_code: string;
  name: string;
  quote: StockQuote | null;
  kline: StockKlineSeries | null;
}

interface HistoryRecord {
  id: number;
  symbol: string;
  stock_name: string;
  analysis_date?: string | null;
  rating?: string | null;
}

interface HistoryListResponse {
  total: number;
  items: HistoryRecord[];
}

interface BatchStockAnalysisItemResult {
  ts_code: string;
  success: boolean;
  error?: string | null;
  analysis?: StockAnalysisResponse | null;
}

interface BatchStockAnalysisResponse {
  total: number;
  success_count: number;
  failed_count: number;
  results: BatchStockAnalysisItemResult[];
}

type EnabledAnalysts = Record<string, boolean>;

const DEFAULT_ENABLED_ANALYSTS: EnabledAnalysts = {
  technical: true,
  fundamental: true,
  fund_flow: true,
  risk: true,
  sentiment: true,
  news: true,
  research: false,
  announcement: false,
  chip: false,
};

function normalizeMarkdownText(text: string | null | undefined): string {
  if (!text) return "";
  return text
    .split(/\r?\n/)
    .map((line) => {
      let l = line;
      if (/^```/.test(l.trim())) return "";
      l = l.replace(/^#{1,6}\s+/, "");
      l = l.replace(/^[-*]\s+/, "• ");
      return l;
    })
    .join("\n");
}

function extractSummaryFromJsonLike(
  conclusion: string | null | undefined,
): string | null {
  if (!conclusion) return null;
  let text = String(conclusion).trim();
  if (!text) return null;

  if (text.startsWith("```") || text.startsWith("```json")) {
    text = text.replace(/^```[a-zA-Z0-9]*\s*/u, "");
    if (text.endsWith("```")) {
      text = text.slice(0, -3);
    }
    text = text.trim();
  }

  try {
    const obj = JSON.parse(text) as any;
    if (obj && typeof obj.summary === "string" && obj.summary.trim()) {
      return obj.summary.trim();
    }
  } catch {
    // ignore
  }
  return null;
}

function getMainConclusionText(result: StockAnalysisResponse): string {
  const fd = (result.final_decision || {}) as any;
  if (fd && typeof fd.summary === "string" && fd.summary.trim().length > 0) {
    return normalizeMarkdownText(fd.summary);
  }

  const fromJson = extractSummaryFromJsonLike(result.conclusion);
  if (fromJson) {
    const norm = normalizeMarkdownText(fromJson);
    if (norm.trim() && norm.trim() !== "{}") return norm;
  }

  const fallback = normalizeMarkdownText(result.conclusion || "");
  const trimmed = fallback.trim();
  if (!trimmed || trimmed === "{}") {
    return "暂无核心结论，请参考下方分析师团队报告。";
  }
  return fallback;
}

function formatDateTime(value?: string | null): string {
  if (!value) return "-";
  let s = String(value).trim();
  if (!s) return "-";
  s = s.replace("T", " ");
  const dotIndex = s.indexOf(".");
  if (dotIndex >= 0) {
    s = s.slice(0, dotIndex);
  }
  s = s.replace(/Z$/, "");
  s = s.replace(/[+-]\d{2}:?\d{2}$/, "");
  s = s.trim();
  if (s.length >= 19) return s.slice(0, 19);
  if (s.length >= 10) return s.slice(0, 10);
  return s;
}

export default function AnalysisPage() {
  const [tsCode, setTsCode] = useState("000001");
  const [enabledAnalysts, setEnabledAnalysts] =
    useState<EnabledAnalysts>(DEFAULT_ENABLED_ANALYSTS);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<StockAnalysisResponse | null>(null);
  const [context, setContext] = useState<StockContextResponse | null>(null);
  const [activeAgentIndex, setActiveAgentIndex] = useState(0);
  const [analysisProgress, setAnalysisProgress] = useState<number | null>(null);
  const [analysisStatus, setAnalysisStatus] = useState("");

  const [batchCodesText, setBatchCodesText] = useState("");
  const [batchMode, setBatchMode] = useState<"sequential" | "parallel">(
    "sequential",
  );
  const [batchLoading, setBatchLoading] = useState(false);
  const [batchError, setBatchError] = useState<string | null>(null);
  const [batchResult, setBatchResult] =
    useState<BatchStockAnalysisResponse | null>(null);

  const [historyItems, setHistoryItems] = useState<HistoryRecord[]>([]);
  const [historyTotal, setHistoryTotal] = useState(0);
  const [historyPage, setHistoryPage] = useState(1);
  const [historyPageSize] = useState(20);
  const [historyQuery, setHistoryQuery] = useState("");
  const [historyRating, setHistoryRating] = useState("");
  const [historyStartDate, setHistoryStartDate] = useState("");
  const [historyEndDate, setHistoryEndDate] = useState("");
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);

  const [historyDetailRecordId, setHistoryDetailRecordId] =
    useState<number | null>(null);
  const [historyDetailLoading, setHistoryDetailLoading] = useState(false);
  const [historyDetailError, setHistoryDetailError] = useState<string | null>(
    null,
  );
  const [historyDetailResult, setHistoryDetailResult] =
    useState<StockAnalysisResponse | null>(null);
  const [historyDetailQuote, setHistoryDetailQuote] =
    useState<StockQuote | null>(null);
  const [historyActiveAgentIndex, setHistoryActiveAgentIndex] = useState(0);
  const [historyMonitorIds, setHistoryMonitorIds] = useState<number[]>([]);
  const [historyDeletingIds, setHistoryDeletingIds] = useState<number[]>([]);

  const batchCodes = useMemo(
    () =>
      batchCodesText
        .split(/[\n,]+/)
        .map((s) => s.trim())
        .filter(Boolean),
    [batchCodesText],
  );

  function toggleAnalyst(key: keyof EnabledAnalysts) {
    setEnabledAnalysts((prev) => ({ ...prev, [key]: !prev[key] }));
  }

  function handleUseFirstBatchCode() {
    if (!batchCodes.length) {
      setError(
        "当前没有可用的批量预填代码，请先在云选股等页面勾选股票并触发批量分析预填。",
      );
      return;
    }
    const first = batchCodes[0];
    setTsCode(first);
    setError(null);
  }

  async function handleBatchAnalyze() {
    const codes = batchCodes;
    if (!codes.length) {
      setBatchError(
        "当前没有可用的批量代码，请先在上方文本框或其他页面预填股票代码。",
      );
      return;
    }

    setBatchLoading(true);
    setBatchError(null);
    setBatchResult(null);
    try {
      const payload = {
        ts_codes: codes,
        enabled_analysts: enabledAnalysts,
        batch_mode: batchMode,
      };
      const res = await fetch(`${API_BASE}/analysis/stock/batch`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        throw new Error(`批量分析请求失败: ${res.status}`);
      }
      const data: BatchStockAnalysisResponse = await res.json();
      setBatchResult(data);
    } catch (e: any) {
      setBatchError(e?.message || "批量分析时发生未知错误");
    } finally {
      setBatchLoading(false);
    }
  }

  function handleBatchClearCache() {
    setBatchCodesText("");
    try {
      if (typeof window !== "undefined") {
        window.localStorage.removeItem("analysis_prefill_batch_codes");
      }
    } catch {
      // ignore
    }
  }

  function handleBatchClearResult() {
    setBatchResult(null);
    setBatchError(null);
  }

  async function handleAnalyze() {
    const code = tsCode.trim();
    if (!code) {
      setError("请先输入股票代码（6位数字，例如 000001）");
      return;
    }
    if (!/^\d{6}$/.test(code)) {
      setError("股票代码格式错误，仅支持6位数字，例如 000001");
      return;
    }

    setLoading(true);
    setError(null);
    setResult(null);
    setContext(null);
    setAnalysisProgress(0);
    setAnalysisStatus("准备开始分析...");

    try {
      const payload = {
        ts_code: code,
        enabled_analysts: enabledAnalysts,
      };

      setAnalysisStatus("正在获取行情与基础数据...");
      setAnalysisProgress(10);
      try {
        const ctxRes = await fetch(`${API_BASE}/analysis/stock/context`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ts_code: code }),
        });
        if (ctxRes.ok) {
          const ctx: StockContextResponse = await ctxRes.json();
          setContext(ctx);
        }
      } catch {
        // ignore context error, 不影响后续分析
      }

      setAnalysisStatus("正在获取多维度数据并运行多智能体分析...");
      setAnalysisProgress(30);

      const anaRes = await fetch(`${API_BASE}/analysis/stock`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!anaRes.ok) {
        throw new Error(`请求失败: ${anaRes.status}`);
      }
      const data: StockAnalysisResponse = await anaRes.json();
      setResult(data);
      setActiveAgentIndex(0);
      setAnalysisProgress(100);
      setAnalysisStatus("分析完成");

      setHistoryPage(1);
      setHistoryQuery(code);
      loadHistory(1, code);
    } catch (e: any) {
      setError(e?.message || "未知错误");
      setAnalysisStatus("分析失败");
    } finally {
      setLoading(false);
    }
  }

  async function loadHistory(pageOverride?: number, queryOverride?: string) {
    const pageToLoad = pageOverride ?? historyPage ?? 1;
    const q =
      queryOverride !== undefined
        ? queryOverride
        : historyQuery !== undefined
        ? historyQuery
        : "";

    setHistoryLoading(true);
    setHistoryError(null);

    try {
      const params = new URLSearchParams();
      params.set("page", String(pageToLoad));
      params.set("page_size", String(historyPageSize));
      if (q && q.trim()) {
        params.set("q", q.trim());
      }
      if (historyRating && historyRating.trim()) {
        params.set("rating", historyRating.trim());
      }
      if (historyStartDate) {
        params.set("start_date", historyStartDate);
      }
      if (historyEndDate) {
        params.set("end_date", historyEndDate);
      }

      const res = await fetch(
        `${API_BASE}/analysis/history?${params.toString()}`,
      );
      if (!res.ok) {
        throw new Error(`历史记录请求失败: ${res.status}`);
      }
      const data: HistoryListResponse = await res.json();
      setHistoryItems(data.items || []);
      setHistoryTotal(data.total ?? data.items?.length ?? 0);
      setHistoryPage(pageToLoad);
    } catch (e: any) {
      setHistoryError(e?.message || "加载历史记录时发生未知错误");
      setHistoryItems([]);
      setHistoryTotal(0);
    } finally {
      setHistoryLoading(false);
    }
  }

  useEffect(() => {
    try {
      if (typeof window !== "undefined") {
        const cached = window.localStorage.getItem(
          "analysis_prefill_batch_codes",
        );
        if (cached && cached.trim()) {
          setBatchCodesText(cached);
        }
      }
    } catch {
      // ignore
    }

    try {
      if (typeof window !== "undefined") {
        const single = window.localStorage.getItem(
          "analysis_prefill_single_code",
        );
        if (single && single.trim()) {
          setTsCode(single.trim());
          window.localStorage.removeItem("analysis_prefill_single_code");
        }
        const historyQ = window.localStorage.getItem(
          "analysis_prefill_history_q",
        );
        if (historyQ && historyQ.trim()) {
          const q = historyQ.trim();
          setHistoryQuery(q);
          loadHistory(1, q);
          window.localStorage.removeItem("analysis_prefill_history_q");
          const el = document.getElementById("analysis-history-section");
          if (el) {
            el.scrollIntoView({ behavior: "smooth", block: "start" });
          }
          return;
        }
      }
    } catch {
      // ignore
    }

    loadHistory(1, "");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function handleHistorySearch() {
    loadHistory(1);
  }

  function handleHistoryReset() {
    setHistoryQuery("");
    setHistoryRating("");
    setHistoryStartDate("");
    setHistoryEndDate("");
    loadHistory(1, "");
  }

  function changeHistoryPage(nextPage: number) {
    if (nextPage < 1) return;
    const totalPages = Math.max(
      1,
      Math.ceil((historyTotal || 0) / historyPageSize || 1),
    );
    const clamped = Math.min(totalPages, nextPage);
    loadHistory(clamped);
  }

  async function handleHistoryViewDetail(recordId: number, symbol: string) {
    if (historyDetailRecordId === recordId && historyDetailResult) {
      setHistoryDetailRecordId(null);
      setHistoryDetailResult(null);
      setHistoryDetailQuote(null);
      setHistoryDetailError(null);
      return;
    }

    setHistoryDetailRecordId(recordId);
    setHistoryActiveAgentIndex(0);
    setHistoryDetailLoading(true);
    setHistoryDetailError(null);
    setHistoryDetailResult(null);
    setHistoryDetailQuote(null);

    try {
      const detailRes = await fetch(
        `${API_BASE}/analysis/history/${recordId}`,
      );
      if (!detailRes.ok) {
        throw new Error(`历史详情请求失败: ${detailRes.status}`);
      }
      const detail: StockAnalysisResponse = await detailRes.json();
      setHistoryDetailResult(detail);

      try {
        const qSym = detail.ts_code || symbol;
        if (qSym) {
          const quoteRes = await fetch(
            `${API_BASE}/analysis/stock/quote/${qSym}`,
          );
          if (quoteRes.ok) {
            const quote: StockQuote = await quoteRes.json();
            setHistoryDetailQuote(quote);
          }
        }
      } catch {
        // ignore realtime quote error
      }
    } catch (e: any) {
      setHistoryDetailError(e?.message || "历史详情请求失败");
    } finally {
      setHistoryDetailLoading(false);
    }
  }

  function handleDownloadPdf(recordId?: number | null) {
    if (!recordId) return;
    window.open(`${API_BASE}/analysis/stock/report/pdf/${recordId}`, "_blank");
  }

  function handleDownloadMarkdown(resultObj: StockAnalysisResponse) {
    const blob = new Blob([resultObj.conclusion], {
      type: "text/markdown;charset=utf-8",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${resultObj.ts_code}_analysis_${new Date().toISOString().slice(0, 10)}.md`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
  }

  async function handleHistoryDelete(id: number) {
    if (!confirm("确定要删除这条历史记录吗？")) return;
    setHistoryDeletingIds((prev) => [...prev, id]);
    try {
      const res = await fetch(`${API_BASE}/analysis/history/${id}`, {
        method: "DELETE",
      });
      if (!res.ok) {
        throw new Error("删除失败");
      }
      setHistoryItems((prev) => prev.filter((item) => item.id !== id));
      setHistoryTotal((t) => Math.max(0, t - 1));
    } catch (e: any) {
      alert(e?.message || "删除时发生错误");
    } finally {
      setHistoryDeletingIds((prev) => prev.filter((i) => i !== id));
    }
  }

  async function handleHistoryAddToMonitor(id: number) {
    setHistoryMonitorIds((prev) => [...prev, id]);
    try {
      // 这里假设有一个加入监测的API，当前仅做前端模拟演示
      await new Promise((resolve) => setTimeout(resolve, 500));
      alert("已加入智能监测列表（模拟）");
    } catch {
      alert("加入监测失败");
    } finally {
      setHistoryMonitorIds((prev) => prev.filter((i) => i !== id));
    }
  }

  return (
    <div style={{ padding: 24 }}>
      <h1 style={{ fontSize: 22, marginBottom: 16 }}>📊 智能股票分析（简化版）</h1>

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 16,
          boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
          marginBottom: 24,
        }}
      >
        <h2 style={{ marginTop: 0, fontSize: 18 }}>🔍 基本参数</h2>
        <div style={{ marginBottom: 12 }}>
          <label style={{ fontWeight: 600, fontSize: 14 }}>
            股票代码（6位数字）
          </label>
          <div style={{ marginTop: 4, display: "flex", gap: 8 }}>
            <input
              value={tsCode}
              onChange={(e) => setTsCode(e.target.value)}
              placeholder="例如: 000001"
              style={{
                flex: 1,
                borderRadius: 8,
                border: "1px solid #ddd",
                padding: "8px 10px",
              }}
            />
            <button
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
              {loading ? "分析中..." : "🚀 开始分析"}
            </button>
          </div>
        </div>

        {error && (
          <p style={{ color: "#b00020", marginTop: 8 }}>错误：{error}</p>
        )}

        <div
          style={{
            marginTop: 12,
            paddingTop: 10,
            borderTop: "1px solid #eee",
            fontSize: 12,
          }}
        >
          <h3 style={{ fontSize: 14, margin: "0 0 4px" }}>👨‍💼 选择分析师团队</h3>
          <p style={{ margin: "0 0 6px", color: "#555" }}>
            通过勾选下方分析师，可以控制本次分析与批量分析时参与协作的分析模块。
          </p>
          <div
            style={{
              display: "flex",
              flexWrap: "wrap",
              gap: 8,
              marginTop: 4,
            }}
          >
            <label
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 4,
                padding: "4px 8px",
                borderRadius: 999,
                background: "#f3f4f6",
                cursor: "pointer",
              }}
            >
              <input
                type="checkbox"
                checked={enabledAnalysts.technical}
                onChange={() => toggleAnalyst("technical")}
              />
              <span>📊 技术分析师</span>
            </label>
            <label
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 4,
                padding: "4px 8px",
                borderRadius: 999,
                background: "#f3f4f6",
                cursor: "pointer",
              }}
            >
              <input
                type="checkbox"
                checked={enabledAnalysts.fundamental}
                onChange={() => toggleAnalyst("fundamental")}
              />
              <span>💼 基本面分析师</span>
            </label>
            <label
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 4,
                padding: "4px 8px",
                borderRadius: 999,
                background: "#f3f4f6",
                cursor: "pointer",
              }}
            >
              <input
                type="checkbox"
                checked={enabledAnalysts.fund_flow}
                onChange={() => toggleAnalyst("fund_flow")}
              />
              <span>💰 资金面分析师</span>
            </label>
            <label
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 4,
                padding: "4px 8px",
                borderRadius: 999,
                background: "#f3f4f6",
                cursor: "pointer",
              }}
            >
              <input
                type="checkbox"
                checked={enabledAnalysts.risk}
                onChange={() => toggleAnalyst("risk")}
              />
              <span>⚠️ 风险管理师</span>
            </label>
            <label
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 4,
                padding: "4px 8px",
                borderRadius: 999,
                background: "#f3f4f6",
                cursor: "pointer",
              }}
            >
              <input
                type="checkbox"
                checked={enabledAnalysts.sentiment}
                onChange={() => toggleAnalyst("sentiment")}
              />
              <span>📈 情绪分析师</span>
            </label>
            <label
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 4,
                padding: "4px 8px",
                borderRadius: 999,
                background: "#f3f4f6",
                cursor: "pointer",
              }}
            >
              <input
                type="checkbox"
                checked={enabledAnalysts.news}
                onChange={() => toggleAnalyst("news")}
              />
              <span>📰 新闻分析师</span>
            </label>
            <label
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 4,
                padding: "4px 8px",
                borderRadius: 999,
                background: "#f3f4f6",
                cursor: "pointer",
              }}
            >
              <input
                type="checkbox"
                checked={enabledAnalysts.research}
                onChange={() => toggleAnalyst("research")}
              />
              <span>📑 研报分析师</span>
            </label>
            <label
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 4,
                padding: "4px 8px",
                borderRadius: 999,
                background: "#f3f4f6",
                cursor: "pointer",
              }}
            >
              <input
                type="checkbox"
                checked={enabledAnalysts.announcement}
                onChange={() => toggleAnalyst("announcement")}
              />
              <span>📢 公告分析师</span>
            </label>
            <label
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 4,
                padding: "4px 8px",
                borderRadius: 999,
                background: "#f3f4f6",
                cursor: "pointer",
              }}
            >
              <input
                type="checkbox"
                checked={enabledAnalysts.chip}
                onChange={() => toggleAnalyst("chip")}
              />
              <span>🎯 筹码分析师</span>
            </label>
          </div>
        </div>

        {/* 批量股票分析 */}
        <div
          style={{
            marginTop: 16,
            paddingTop: 10,
            borderTop: "1px solid #eee",
            fontSize: 12,
          }}
        >
          <h3 style={{ fontSize: 14, margin: "0 0 4px" }}>📦 批量股票分析</h3>
          <p style={{ margin: "0 0 6px", color: "#555" }}>
            请输入多个股票代码（每行一个或用逗号分隔）。也支持从云选股、主力选股等页面预填，当前已加载 {batchCodes.length} 个代码。
          </p>
          <textarea
            value={batchCodesText}
            onChange={(e) => setBatchCodesText(e.target.value)}
            rows={4}
            style={{
              width: "100%",
              resize: "vertical",
              borderRadius: 8,
              border: "1px solid #ddd",
              padding: "6px 8px",
              fontFamily: "monospace",
            }}
          />
          <div
            style={{
              marginTop: 6,
              display: "flex",
              gap: 8,
              flexWrap: "wrap",
            }}
          >
            <button
              type="button"
              onClick={handleUseFirstBatchCode}
              style={{
                padding: "6px 10px",
                borderRadius: 8,
                border: "none",
                background: "#4b5563",
                color: "#fff",
                fontSize: 12,
                cursor: "pointer",
              }}
            >
              将首个代码填入上方输入框
            </button>
            <button
              type="button"
              onClick={handleBatchClearCache}
              style={{
                padding: "6px 10px",
                borderRadius: 8,
                border: "1px solid #d1d5db",
                background: "#f9fafb",
                fontSize: 12,
                cursor: "pointer",
              }}
            >
              🔄 清除缓存
            </button>
            <button
              type="button"
              onClick={handleBatchClearResult}
              style={{
                padding: "6px 10px",
                borderRadius: 8,
                border: "1px solid #f97373",
                background: "#fef2f2",
                fontSize: 12,
                cursor: "pointer",
              }}
            >
              🗑️ 清除结果
            </button>
            <span style={{ alignSelf: "center", color: "#777" }}>
              （每行一个代码，或使用逗号分隔多个代码）
            </span>
          </div>
          <div
            style={{
              marginTop: 10,
              display: "flex",
              flexWrap: "wrap",
              gap: 8,
              alignItems: "center",
            }}
          >
            <span style={{ fontSize: 12, color: "#555" }}>批量模式：</span>
            <select
              value={batchMode}
              onChange={(e) =>
                setBatchMode(
                  e.target.value === "parallel" ? "parallel" : "sequential",
                )
              }
              style={{ fontSize: 12, padding: "3px 6px" }}
            >
              <option value="sequential">顺序分析</option>
              <option value="parallel">多线程并行</option>
            </select>
            <button
              type="button"
              onClick={handleBatchAnalyze}
              disabled={batchLoading}
              style={{
                padding: "6px 10px",
                borderRadius: 8,
                border: "none",
                background:
                  "linear-gradient(135deg, #0ea5e9 0%, #6366f1 100%)",
                color: "#fff",
                fontSize: 12,
                cursor: batchLoading ? "default" : "pointer",
              }}
            >
              {batchLoading ? "批量分析中..." : "📊 开始批量分析"}
            </button>
          </div>
          {batchError && (
            <p style={{ marginTop: 6, fontSize: 12, color: "#b00020" }}>
              批量分析错误：{batchError}
            </p>
          )}
          {batchResult && (
            <p style={{ marginTop: 6, fontSize: 12, color: "#555" }}>
              已完成批量分析：共 {batchResult.total} 只，
              成功 {batchResult.success_count} 只，
              失败 {batchResult.failed_count} 只。
            </p>
          )}
          {batchResult && batchResult.results && (
            <div style={{ marginTop: 12 }}>
              <details>
                <summary
                  style={{
                    cursor: "pointer",
                    fontSize: 13,
                    color: "#4b5563",
                    outline: "none",
                  }}
                >
                  查看详细列表 ({batchResult.results.length})
                </summary>
                <div
                  style={{
                    marginTop: 8,
                    display: "flex",
                    flexDirection: "column",
                    gap: 6,
                  }}
                >
                  {batchResult.results.map((item, idx) => (
                    <div
                      key={idx}
                      style={{
                        padding: 8,
                        background: item.success ? "#f0fdf4" : "#fef2f2",
                        border: item.success
                          ? "1px solid #bbf7d0"
                          : "1px solid #fecaca",
                        borderRadius: 6,
                        fontSize: 12,
                      }}
                    >
                      <div
                        style={{
                          fontWeight: 600,
                          display: "flex",
                          justifyContent: "space-between",
                        }}
                      >
                        <span>{item.ts_code}</span>
                        <span
                          style={{
                            color: item.success ? "#16a34a" : "#dc2626",
                          }}
                        >
                          {item.success ? "成功" : "失败"}
                        </span>
                      </div>
                      {item.error && (
                        <div style={{ color: "#b91c1c", marginTop: 2 }}>
                          {item.error}
                        </div>
                      )}
                      {item.analysis && (
                        <div style={{ color: "#374151", marginTop: 4 }}>
                          {item.analysis.conclusion.slice(0, 60)}...
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </details>
            </div>
          )}
        </div>

        {analysisProgress !== null && (
          <div
            style={{
              marginTop: 12,
            }}
          >
            <div
              style={{
                fontSize: 12,
                marginBottom: 4,
              }}
            >
              {analysisStatus || "正在分析..."}
            </div>
            <div
              style={{
                width: "100%",
                height: 6,
                borderRadius: 999,
                background: "#e5e7eb",
                overflow: "hidden",
              }}
            >
              <div
                style={{
                  width: `${Math.min(100, Math.max(0, analysisProgress))}%`,
                  height: "100%",
                  transition: "width 0.2s ease-out",
                  background:
                    "linear-gradient(90deg, #22c55e 0%, #16a34a 40%, #0ea5e9 100%)",
                }}
              />
            </div>
          </div>
        )}

        {context && context.kline && context.kline.dates.length > 0 && (
          <div style={{ marginTop: 16 }}>
            <h3 style={{ margin: "0 0 4px" }}>K线图</h3>
            <Plot
              data={[
                {
                  x: context.kline.dates,
                  open: context.kline.open,
                  high: context.kline.high,
                  low: context.kline.low,
                  close: context.kline.close,
                  type: "candlestick",
                  increasing: { line: { color: "#b91c1c" } },
                  decreasing: { line: { color: "#15803d" } },
                  name: "K线",
                },
              ]}
              layout={{
                margin: { l: 40, r: 10, t: 10, b: 30 },
                height: 320,
                xaxis: { title: "日期", showgrid: false },
                yaxis: { title: "价格", showgrid: true },
                showlegend: false,
              }}
              style={{ width: "100%", height: "100%" }}
              config={{ displayModeBar: false, responsive: true }}
            />
          </div>
        )}

        {result && (
          <div
            style={{
              marginTop: 16,
              paddingTop: 12,
              borderTop: "1px solid #eee",
              fontSize: 14,
            }}
          >
            {context && context.quote && (
              <section
                style={{
                  marginBottom: 16,
                  background: "#fff",
                  borderRadius: 12,
                  padding: 16,
                  boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
                }}
              >
                <h3 style={{ marginTop: 0, fontSize: 16 }}>
                  📊 实时行情概览
                </h3>
                <div
                  style={{
                    marginTop: 8,
                    display: "grid",
                    gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
                    gap: 12,
                    fontSize: 13,
                  }}
                >
                  {(() => {
                    const q = context.quote as StockQuote;
                    const formatNumber = (v: any, digits: number) => {
                      if (typeof v === "number" && Number.isFinite(v)) {
                        return v.toFixed(digits);
                      }
                      return v ?? "--";
                    };
                    const formatPercent = (v: any) => {
                      if (typeof v === "number" && Number.isFinite(v)) {
                        const sign = v > 0 ? "+" : "";
                        return `${sign}${v.toFixed(2)}%`;
                      }
                      return v ?? "--";
                    };

                    return (
                      <>
                        <div
                          style={{
                            borderRadius: 8,
                            border: "1px solid #e5e7eb",
                            padding: 10,
                            background: "#f9fafb",
                          }}
                        >
                          <div
                            style={{
                              fontSize: 12,
                              color: "#6b7280",
                              marginBottom: 4,
                            }}
                          >
                            当前价格 / 涨跌幅
                          </div>
                          <div
                            style={{
                              display: "flex",
                              justifyContent: "space-between",
                              alignItems: "baseline",
                            }}
                          >
                            <div
                              style={{
                                fontSize: 20,
                                fontWeight: 700,
                                color: "#111827",
                              }}
                            >
                              {q.current_price != null
                                ? q.current_price.toFixed(2)
                                : "--"}
                            </div>
                            <div
                              style={{
                                fontSize: 12,
                                color:
                                  typeof q.change_percent === "number" &&
                                  q.change_percent > 0
                                    ? "#b91c1c"
                                    : typeof q.change_percent === "number" &&
                                        q.change_percent < 0
                                      ? "#15803d"
                                      : "#374151",
                              }}
                            >
                              {formatPercent(q.change_percent)}
                            </div>
                          </div>
                        </div>

                        <div
                          style={{
                            borderRadius: 8,
                            border: "1px solid #e5e7eb",
                            padding: 10,
                            background: "#f9fafb",
                          }}
                        >
                          <div
                            style={{
                              fontSize: 12,
                              color: "#6b7280",
                              marginBottom: 4,
                            }}
                          >
                            成交量 / 成交额
                          </div>
                          <div style={{ fontSize: 13, lineHeight: 1.6 }}>
                            <div>
                              量：
                              {typeof q.volume === "number"
                                ? q.volume.toFixed(0)
                                : q.volume ?? "--"}
                            </div>
                            <div>
                              额：
                              {typeof q.amount === "number"
                                ? q.amount.toFixed(0)
                                : q.amount ?? "--"}
                            </div>
                          </div>
                        </div>

                        <div
                          style={{
                            borderRadius: 8,
                            border: "1px solid #e5e7eb",
                            padding: 10,
                            background: "#f9fafb",
                          }}
                        >
                          <div
                            style={{
                              fontSize: 12,
                              color: "#6b7280",
                              marginBottom: 4,
                            }}
                          >
                            52周高 / 52周低
                          </div>
                          <div style={{ fontSize: 13, lineHeight: 1.6 }}>
                            <div>
                              高：
                              {formatNumber(q.week52_high, 2)}
                            </div>
                            <div>
                              低：
                              {formatNumber(q.week52_low, 2)}
                            </div>
                          </div>
                        </div>

                        <div
                          style={{
                            borderRadius: 8,
                            border: "1px solid #e5e7eb",
                            padding: 10,
                            background: "#f9fafb",
                          }}
                        >
                          <div
                            style={{
                              fontSize: 12,
                              color: "#6b7280",
                              marginBottom: 4,
                            }}
                          >
                            当日开盘 / 最高 / 最低
                          </div>
                          <div style={{ fontSize: 13, lineHeight: 1.6 }}>
                            <div>
                              开：
                              {formatNumber(q.open_price, 2)}
                            </div>
                            <div>
                              高：
                              {formatNumber(q.high_price, 2)}
                            </div>
                            <div>
                              低：
                              {formatNumber(q.low_price, 2)}
                            </div>
                          </div>
                        </div>
                      </>
                    );
                  })()}
                </div>
              </section>
            )}

            {result.data_fetch_diagnostics && (
              <section
                style={{
                  marginBottom: 16,
                  background: "#fff",
                  borderRadius: 12,
                  padding: 16,
                  boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
                }}
              >
                <h3 style={{ marginTop: 0, fontSize: 16 }}>
                  🔍 数据获取诊断
                </h3>
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
                    gap: 12,
                    fontSize: 13,
                  }}
                >
                  {Object.entries(result.data_fetch_diagnostics).map(
                    ([key, value]) => {
                      const v = value as any;
                      const status = v?.status || "unknown";
                      const hasError = status === "error";
                      const hasData = v?.has_data;
                      let label = key;
                      if (key === "stock_info") label = "基础信息";
                      if (key === "stock_data") label = "历史行情";
                      if (key === "technical_indicators") label = "技术指标";
                      if (key === "financial_data") label = "财务数据";
                      if (key === "fund_flow_data") label = "资金流数据";
                      if (key === "risk_data") label = "风险数据";
                      if (key === "sentiment_data") label = "市场情绪数据";
                      if (key === "news_data") label = "新闻数据";
                      if (key === "research_data") label = "研报数据";
                      if (key === "announcement_data") label = "公告数据";
                      if (key === "chip_data") label = "筹码数据";

                      const color = hasError
                        ? "#b00020"
                        : status === "success"
                          ? "#2e7d32"
                          : "#555";

                      let statusText = "未知";
                      if (status === "success") statusText = "获取成功";
                      else if (status === "error") statusText = "获取失败";
                      else if (status === "skipped")
                        statusText = "已跳过（分析师未启用）";
                      else if (status === "not_implemented")
                        statusText = "未接入统一获取";

                      return (
                        <div
                          key={key}
                          style={{
                            borderRadius: 8,
                            border: "1px solid #eee",
                            padding: 10,
                            background: "#fafafa",
                          }}
                        >
                          <div
                            style={{
                              fontWeight: 600,
                              marginBottom: 4,
                            }}
                          >
                            {label}
                          </div>
                          <div style={{ color, marginBottom: 4 }}>
                            状态：
                            {statusText}
                          </div>
                          {typeof hasData === "boolean" && (
                            <div
                              style={{
                                marginBottom: 4,
                              }}
                            >
                              是否有数据：
                              {hasData ? "是" : "否"}
                            </div>
                          )}
                          {v?.period && (
                            <div
                              style={{
                                marginBottom: 4,
                              }}
                            >
                              period：
                              {String(v.period)}
                            </div>
                          )}
                          {v?.reason && (
                            <div
                              style={{
                                marginBottom: 4,
                                fontSize: 12,
                                color: "#777",
                              }}
                            >
                              原因：
                              {v.reason === "analyst_disabled"
                                ? "分析师未启用"
                                : v.reason === "data_fetch_not_wired"
                                  ? "尚未接入统一数据获取"
                                  : "未显式传入启用配置"}
                            </div>
                          )}
                          {hasError && v?.error && (
                            <details
                              style={{
                                marginTop: 4,
                              }}
                            >
                              <summary
                                style={{
                                  cursor: "pointer",
                                  fontSize: 12,
                                }}
                              >
                                查看错误详情
                              </summary>
                              <pre
                                style={{
                                  whiteSpace: "pre-wrap",
                                  marginTop: 4,
                                  fontSize: 11,
                                  background: "#fff",
                                  padding: 6,
                                  borderRadius: 4,
                                }}
                              >
                                {String(v.error)}
                              </pre>
                            </details>
                          )}
                        </div>
                      );
                    },
                  )}
                </div>
              </section>
            )}

            {(() => {
              const ti = result.technical_indicators as any | null;
              if (!ti || typeof ti !== "object" || Array.isArray(ti)) return null;
              if (Object.keys(ti).length === 0) return null;

              const getVal = (v: any, digits: number) => {
                if (typeof v === "number" && Number.isFinite(v)) {
                  return v.toFixed(digits);
                }
                return v ?? "--";
              };

              const rsi = getVal(ti.rsi, 2);
              const ma20 = getVal(ti.ma20, 2);
              const volumeRatio = getVal(ti.volume_ratio, 2);
              const macd = getVal(ti.macd, 4);

              return (
                <section
                  style={{
                    marginBottom: 16,
                    background: "#fff",
                    borderRadius: 12,
                    padding: 16,
                    boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
                  }}
                >
                  <h3 style={{ marginTop: 0, fontSize: 16 }}>
                    📈 关键技术指标
                  </h3>
                  <div
                    style={{
                      marginTop: 8,
                      display: "grid",
                      gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
                      gap: 12,
                      fontSize: 13,
                    }}
                  >
                    <div
                      style={{
                        borderRadius: 8,
                        border: "1px solid #e5e7eb",
                        padding: 10,
                        background: "#f9fafb",
                      }}
                    >
                      <div
                        style={{
                          fontSize: 12,
                          color: "#6b7280",
                          marginBottom: 4,
                        }}
                      >
                        RSI
                      </div>
                      <div
                        style={{
                          fontSize: 18,
                          fontWeight: 700,
                          color: "#111827",
                        }}
                      >
                        {rsi}
                      </div>
                    </div>

                    <div
                      style={{
                        borderRadius: 8,
                        border: "1px solid #e5e7eb",
                        padding: 10,
                        background: "#f9fafb",
                      }}
                    >
                      <div
                        style={{
                          fontSize: 12,
                          color: "#6b7280",
                          marginBottom: 4,
                        }}
                      >
                        MA20
                      </div>
                      <div
                        style={{
                          fontSize: 18,
                          fontWeight: 700,
                          color: "#111827",
                        }}
                      >
                        {ma20}
                      </div>
                    </div>

                    <div
                      style={{
                        borderRadius: 8,
                        border: "1px solid #e5e7eb",
                        padding: 10,
                        background: "#f9fafb",
                      }}
                    >
                      <div
                        style={{
                          fontSize: 12,
                          color: "#6b7280",
                          marginBottom: 4,
                        }}
                      >
                        量比
                      </div>
                      <div
                        style={{
                          fontSize: 18,
                          fontWeight: 700,
                          color: "#111827",
                        }}
                      >
                        {volumeRatio}
                      </div>
                    </div>

                    <div
                      style={{
                        borderRadius: 8,
                        border: "1px solid #e5e7eb",
                        padding: 10,
                        background: "#f9fafb",
                      }}
                    >
                      <div
                        style={{
                          fontSize: 12,
                          color: "#6b7280",
                          marginBottom: 4,
                        }}
                      >
                        MACD
                      </div>
                      <div
                        style={{
                          fontSize: 18,
                          fontWeight: 700,
                          color: "#111827",
                        }}
                      >
                        {macd}
                      </div>
                    </div>
                  </div>
                </section>
              );
            })()}

            <section
              style={{
                marginBottom: 16,
              }}
            >
              <div
                style={{
                  background:
                    "linear-gradient(135deg, #e8f5e9 0%, #c8e6c9 100%)",
                  borderRadius: 12,
                  padding: 16,
                  border: "2px solid #4caf50",
                  boxShadow: "0 4px 18px rgba(76,175,80,0.25)",
                }}
              >
                <h3 style={{ marginTop: 0, fontSize: 16 }}>
                  📋 最终投资决策 - {result.ts_code}
                </h3>
                <div
                  style={{
                    marginTop: 6,
                    padding: 10,
                    borderRadius: 8,
                    background: "rgba(255,255,255,0.9)",
                    fontSize: 14,
                    lineHeight: 1.6,
                    whiteSpace: "pre-line",
                  }}
                >
                  {getMainConclusionText(result)}
                </div>

                {result.final_decision && (
                  <div
                    style={{
                      marginTop: 8,
                      fontSize: 13,
                      lineHeight: 1.6,
                    }}
                  >
                    {(() => {
                      const fd = (result.final_decision || {}) as any;
                      return (
                        <>
                          {fd?.rating && (
                            <p style={{ margin: "0 0 4px" }}>
                              <strong>投资评级：</strong>
                              {String(fd.rating)}
                            </p>
                          )}
                          {fd?.time_horizon && (
                            <p style={{ margin: "0 0 6px" }}>
                              <strong>建议持有周期：</strong>
                              {String(fd.time_horizon)}
                            </p>
                          )}
                          {Array.isArray(fd?.key_reasons) &&
                            fd.key_reasons.length > 0 && (
                              <div style={{ marginTop: 4 }}>
                                <div
                                  style={{
                                    fontWeight: 600,
                                    marginBottom: 2,
                                  }}
                                >
                                  核心理由：
                                </div>
                                <ul
                                  style={{
                                    margin: 0,
                                    paddingLeft: 20,
                                  }}
                                >
                                  {fd.key_reasons.map(
                                    (item: any, idx2: number) => (
                                      <li key={idx2}>
                                        {normalizeMarkdownText(
                                          typeof item === "string"
                                            ? item
                                            : String(item),
                                        )}
                                      </li>
                                    ),
                                  )}
                                </ul>
                              </div>
                            )}
                          {Array.isArray(fd?.risk_points) &&
                            fd.risk_points.length > 0 && (
                              <div style={{ marginTop: 6 }}>
                                <div
                                  style={{
                                    fontWeight: 600,
                                    marginBottom: 2,
                                  }}
                                >
                                  风险提示：
                                </div>
                                <ul
                                  style={{
                                    margin: 0,
                                    paddingLeft: 20,
                                  }}
                                >
                                  {fd.risk_points.map(
                                    (item: any, idx2: number) => (
                                      <li key={idx2}>
                                        {normalizeMarkdownText(
                                          typeof item === "string"
                                            ? item
                                            : String(item),
                                        )}
                                      </li>
                                    ),
                                  )}
                                </ul>
                              </div>
                            )}
                        </>
                      );
                    })()}
                  </div>
                )}
              </div>
            </section>

            {result.agents && result.agents.length > 0 && (
              <section
                style={{
                  marginBottom: 16,
                  background: "#fff",
                  borderRadius: 12,
                  padding: 16,
                  boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
                }}
              >
                <h3 style={{ marginTop: 0, fontSize: 16 }}>
                  🤖 分析师团队报告
                </h3>
                <div
                  style={{
                    marginTop: 8,
                    display: "flex",
                    flexWrap: "wrap",
                    gap: 8,
                  }}
                >
                  {result.agents.map((agent, idx2) => {
                    const active = idx2 === activeAgentIndex;
                    return (
                      <button
                        key={idx2}
                        type="button"
                        onClick={() => setActiveAgentIndex(idx2)}
                        style={{
                          padding: "6px 12px",
                          borderRadius: 999,
                          border: active
                            ? "1px solid #4f46e5"
                            : "1px solid #d1d5db",
                          background: active
                            ? "linear-gradient(135deg, #4f46e5 0%, #6366f1 100%)"
                            : "#f9fafb",
                          color: active ? "#f9fafb" : "#111827",
                          fontSize: 12,
                          cursor: "pointer",
                        }}
                      >
                        {agent.name}
                      </button>
                    );
                  })}
                </div>

                {result.agents[activeAgentIndex] && (
                  <div
                    style={{
                      marginTop: 12,
                      padding: 12,
                      borderRadius: 10,
                      background:
                        "linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)",
                    }}
                  >
                    <h4
                      style={{
                        marginTop: 0,
                        marginBottom: 6,
                        fontSize: 15,
                      }}
                    >
                      {result.agents[activeAgentIndex].name}
                    </h4>
                    <div
                      style={{
                        padding: 10,
                        borderRadius: 8,
                        background: "rgba(255,255,255,0.9)",
                        fontSize: 14,
                        lineHeight: 1.6,
                        whiteSpace: "pre-line",
                      }}
                    >
                      {normalizeMarkdownText(
                        result.agents[activeAgentIndex].summary,
                      )}
                    </div>
                  </div>
                )}
              </section>
            )}

            {result.discussion && (
              <section
                style={{
                  marginBottom: 16,
                  background: "#fff",
                  borderRadius: 12,
                  padding: 16,
                  boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
                }}
              >
                <h3 style={{ marginTop: 0, fontSize: 16 }}>
                  🤝 团队讨论纪要
                </h3>
                <div
                  style={{
                    marginTop: 6,
                    padding: 10,
                    borderRadius: 8,
                    background: "#f9fafb",
                    fontSize: 14,
                    lineHeight: 1.6,
                    whiteSpace: "pre-line",
                  }}
                >
                  {normalizeMarkdownText(result.discussion)}
                </div>
              </section>
            )}

            <div style={{ marginTop: 16, display: "flex", gap: 12 }}>
              <button
                onClick={() => handleDownloadPdf(result.record_id)}
                style={{
                  padding: "8px 16px",
                  borderRadius: 8,
                  border: "1px solid #d1d5db",
                  background: "#fff",
                  cursor: "pointer",
                  fontSize: 13,
                }}
              >
                📄 导出 PDF 报告
              </button>
              <button
                onClick={() => handleDownloadMarkdown(result)}
                style={{
                  padding: "8px 16px",
                  borderRadius: 8,
                  border: "1px solid #d1d5db",
                  background: "#fff",
                  cursor: "pointer",
                  fontSize: 13,
                }}
              >
                ⬇️ 导出 Markdown
              </button>
            </div>
          </div>
        )}

        {/* K线图 已在上方 context 区域中统一展示 */}
      </section>

      <section
        id="analysis-history-section"
        style={{
          marginTop: 16,
          background: "#fff",
          borderRadius: 12,
          padding: 16,
          boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
        }}
      >
        <h2 style={{ marginTop: 0, fontSize: 18 }}>📚 历史分析记录</h2>

        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: 8,
            marginBottom: 12,
            fontSize: 13,
          }}
        >
          <input
            value={historyQuery}
            onChange={(e) => setHistoryQuery(e.target.value)}
            placeholder="输入股票代码或名称，按回车或点击搜索"
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                handleHistorySearch();
              }
            }}
            style={{
              flex: 1,
              minWidth: 180,
              borderRadius: 8,
              border: "1px solid #ddd",
              padding: "6px 8px",
            }}
          />
          <select
            value={historyRating}
            onChange={(e) => setHistoryRating(e.target.value)}
            style={{
              minWidth: 120,
              borderRadius: 8,
              border: "1px solid #ddd",
              padding: "6px 8px",
              background: "#fff",
            }}
          >
            <option value="">评级不限</option>
            <option value="买入">买入/强烈推荐</option>
            <option value="增持">增持</option>
            <option value="中性">中性/持有</option>
            <option value="减持">减持</option>
            <option value="卖出">卖出/回避</option>
          </select>
          <input
            type="date"
            value={historyStartDate}
            onChange={(e) => setHistoryStartDate(e.target.value)}
            style={{
              minWidth: 130,
              borderRadius: 8,
              border: "1px solid #ddd",
              padding: "6px 8px",
            }}
          />
          <span style={{ alignSelf: "center" }}>至</span>
          <input
            type="date"
            value={historyEndDate}
            onChange={(e) => setHistoryEndDate(e.target.value)}
            style={{
              minWidth: 130,
              borderRadius: 8,
              border: "1px solid #ddd",
              padding: "6px 8px",
            }}
          />
          <button
            type="button"
            onClick={handleHistorySearch}
            style={{
              padding: "6px 12px",
              borderRadius: 8,
              border: "none",
              background:
                "linear-gradient(135deg, #0ea5e9 0%, #6366f1 100%)",
              color: "#fff",
              fontSize: 12,
              cursor: "pointer",
            }}
          >
            🔍 搜索
          </button>
          <button
            type="button"
            onClick={handleHistoryReset}
            style={{
              padding: "6px 12px",
              borderRadius: 8,
              border: "1px solid #ccc",
              background: "#fafafa",
              fontSize: 12,
              cursor: "pointer",
            }}
          >
            重置
          </button>
        </div>

        {historyError && (
          <p style={{ color: "#b00020", fontSize: 13, marginBottom: 8 }}>
            历史记录错误：
            {historyError}
          </p>
        )}

        {historyLoading && (
          <p style={{ fontSize: 13, color: "#555" }}>正在加载历史记录...</p>
        )}

        {!historyLoading && historyItems.length === 0 && (
          <p style={{ fontSize: 13, color: "#777" }}>
            暂无历史分析记录，可先在上方完成一次分析后再查看，或通过搜索加载已有记录。
          </p>
        )}

        {historyItems.length > 0 && (
          <>
            <p style={{ fontSize: 13, color: "#555", marginBottom: 8 }}>
              共 {historyTotal} 条记录，每页 {historyPageSize} 条。
            </p>
            <ul style={{ paddingLeft: 16 }}>
              {historyItems.map((item) => (
                <li key={item.id} style={{ marginBottom: 4, fontSize: 13 }}>
                  {item.stock_name} ({item.symbol}) -{" "}
                  {item.analysis_date
                    ? formatDateTime(item.analysis_date)
                    : "未知时间"}
                  {" "}- 评级：{item.rating || "未知"}
                  <button
                    type="button"
                    onClick={() =>
                      handleHistoryViewDetail(item.id, item.symbol)
                    }
                    style={{
                      marginLeft: 8,
                      padding: "2px 8px",
                      borderRadius: 999,
                      border: "1px solid #4b5563",
                      background:
                        historyDetailRecordId === item.id
                          ? "#e5e7eb"
                          : "#111827",
                      color:
                        historyDetailRecordId === item.id
                          ? "#111827"
                          : "#f9fafb",
                      fontSize: 12,
                      cursor: "pointer",
                    }}
                  >
                    {historyDetailRecordId === item.id ? "收起详情" : "详情"}
                  </button>
                  <button
                    type="button"
                    onClick={() => handleDownloadPdf(item.id)}
                    style={{
                      marginLeft: 6,
                      padding: "2px 8px",
                      borderRadius: 999,
                      border: "1px solid #d1d5db",
                      background: "#fff",
                      fontSize: 12,
                      cursor: "pointer",
                    }}
                  >
                    📄 PDF
                  </button>
                  <button
                    type="button"
                    onClick={() => handleHistoryAddToMonitor(item.id)}
                    disabled={historyMonitorIds.includes(item.id)}
                    style={{
                      marginLeft: 6,
                      padding: "2px 8px",
                      borderRadius: 999,
                      border: "1px solid #60a5fa",
                      background: "#eff6ff",
                      color: "#1d4ed8",
                      fontSize: 12,
                      cursor: historyMonitorIds.includes(item.id)
                        ? "default"
                        : "pointer",
                      opacity: historyMonitorIds.includes(item.id) ? 0.6 : 1,
                    }}
                  >
                    {historyMonitorIds.includes(item.id) ? "已加入" : "➕ 监测"}
                  </button>
                  <button
                    type="button"
                    onClick={() => handleHistoryDelete(item.id)}
                    disabled={historyDeletingIds.includes(item.id)}
                    style={{
                      marginLeft: 6,
                      padding: "2px 8px",
                      borderRadius: 999,
                      border: "1px solid #fca5a5",
                      background: "#fef2f2",
                      color: "#b91c1c",
                      fontSize: 12,
                      cursor: historyDeletingIds.includes(item.id)
                        ? "default"
                        : "pointer",
                      opacity: historyDeletingIds.includes(item.id) ? 0.6 : 1,
                    }}
                  >
                    🗑️ 删除
                  </button>
                </li>
              ))}
            </ul>
            <div
              style={{
                marginTop: 8,
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                fontSize: 13,
              }}
            >
              <button
                type="button"
                onClick={() => changeHistoryPage(historyPage - 1)}
                disabled={historyPage <= 1}
                style={{
                  padding: "4px 10px",
                  borderRadius: 999,
                  border: "1px solid #ccc",
                  background: "#fafafa",
                  fontSize: 12,
                  cursor: historyPage <= 1 ? "default" : "pointer",
                }}
              >
                ⬅️ 上一页
              </button>
              <span style={{ color: "#555" }}>
                第 {historyPage} 页 / 共
                {" "}
                {Math.max(
                  1,
                  Math.ceil((historyTotal || 0) / historyPageSize || 1),
                )}
                {" "}
                页
              </span>
              <button
                type="button"
                onClick={() => changeHistoryPage(historyPage + 1)}
                disabled={
                  historyPage >=
                  Math.max(
                    1,
                    Math.ceil((historyTotal || 0) / historyPageSize || 1),
                  )
                }
                style={{
                  padding: "4px 10px",
                  borderRadius: 999,
                  border: "1px solid #ccc",
                  background: "#fafafa",
                  fontSize: 12,
                  cursor:
                    historyPage >=
                    Math.max(
                      1,
                      Math.ceil(
                        (historyTotal || 0) / historyPageSize || 1,
                      ),
                    )
                      ? "default"
                      : "pointer",
                }}
              >
                下一页 ➡️
              </button>
            </div>
          </>
        )}

        {historyDetailRecordId && (
          <div style={{ marginTop: 12, fontSize: 13 }}>
            {historyDetailLoading && (
              <p style={{ margin: 0, color: "#555" }}>
                正在加载完整历史报告...
              </p>
            )}
            {!historyDetailLoading && historyDetailError && (
              <p style={{ margin: 0, color: "#b00020" }}>
                加载详情失败：
                {historyDetailError}
              </p>
            )}
            {!historyDetailLoading &&
              !historyDetailError &&
              historyDetailResult && (
                <div style={{ padding: 12, background: "#f9fafb", borderRadius: 8 }}>
                  <h3 style={{ fontSize: 16, margin: "8px 0 12px" }}>
                    📋 历史记录详情 - {historyDetailResult.ts_code}
                  </h3>

                  {historyDetailQuote && (
                    <div
                      style={{
                        marginBottom: 12,
                        padding: 12,
                        background: "#fff",
                        borderRadius: 8,
                        boxShadow: "0 2px 8px rgba(0,0,0,0.05)",
                        display: "flex",
                        justifyContent: "space-between",
                        alignItems: "center",
                      }}
                    >
                      <div>
                        <div style={{ fontWeight: 600, fontSize: 15 }}>
                          {historyDetailQuote.name} ({historyDetailQuote.symbol})
                        </div>
                      </div>
                      <div style={{ textAlign: "right" }}>
                        <div
                          style={{
                            fontSize: 20,
                            fontWeight: 700,
                            color: "#d32f2f",
                          }}
                        >
                          ¥
                          {historyDetailQuote.current_price != null
                            ? historyDetailQuote.current_price.toFixed(2)
                            : "--"}
                        </div>
                        <div style={{ fontSize: 12, color: "#d32f2f" }}>
                          {historyDetailQuote.change_percent != null
                            ? `${historyDetailQuote.change_percent > 0 ? "+" : ""}${historyDetailQuote.change_percent.toFixed(2)}%`
                            : "--"}
                        </div>
                      </div>
                    </div>
                  )}

                  <div
                    style={{
                      background: "#fff",
                      borderRadius: 8,
                      padding: 16,
                      marginBottom: 12,
                      borderLeft: "4px solid #4caf50",
                    }}
                  >
                    <div
                      style={{
                        fontWeight: 600,
                        fontSize: 14,
                        marginBottom: 8,
                      }}
                    >
                      核心结论
                    </div>
                    <div
                      style={{
                        fontSize: 14,
                        lineHeight: 1.6,
                        whiteSpace: "pre-line",
                      }}
                    >
                      {getMainConclusionText(historyDetailResult)}
                    </div>
                  </div>

                  {historyDetailResult.agents &&
                    historyDetailResult.agents.length > 0 && (
                      <div
                        style={{
                          background: "#fff",
                          borderRadius: 8,
                          padding: 16,
                          marginBottom: 12,
                        }}
                      >
                        <div
                          style={{
                            fontWeight: 600,
                            marginBottom: 8,
                          }}
                        >
                          分析师团队报告
                        </div>
                        <div
                          style={{
                            marginTop: 4,
                            display: "flex",
                            flexWrap: "wrap",
                            gap: 8,
                          }}
                        >
                          {historyDetailResult.agents.map((ag, agIdx) => {
                            const active = agIdx === historyActiveAgentIndex;
                            return (
                              <button
                                key={agIdx}
                                type="button"
                                onClick={() => setHistoryActiveAgentIndex(agIdx)}
                                style={{
                                  padding: "4px 10px",
                                  borderRadius: 999,
                                  border: active
                                    ? "1px solid #4f46e5"
                                    : "1px solid #d1d5db",
                                  background: active
                                    ? "linear-gradient(135deg, #4f46e5 0%, #6366f1 100%)"
                                    : "#f9fafb",
                                  color: active ? "#f9fafb" : "#111827",
                                  fontSize: 12,
                                  cursor: "pointer",
                                }}
                              >
                                {ag.name}
                              </button>
                            );
                          })}
                        </div>

                        {historyDetailResult.agents[historyActiveAgentIndex] && (
                          <div
                            style={{
                              marginTop: 12,
                              padding: 12,
                              borderRadius: 10,
                              background:
                                "linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)",
                            }}
                          >
                            <h4
                              style={{
                                marginTop: 0,
                                marginBottom: 6,
                                fontSize: 14,
                              }}
                            >
                              {historyDetailResult.agents[historyActiveAgentIndex]
                                .name}
                            </h4>
                            <div
                              style={{
                                padding: 10,
                                borderRadius: 8,
                                background: "rgba(255,255,255,0.9)",
                                fontSize: 13,
                                lineHeight: 1.6,
                                whiteSpace: "pre-line",
                              }}
                            >
                              {normalizeMarkdownText(
                                historyDetailResult.agents[
                                  historyActiveAgentIndex
                                ].summary,
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    )}

                  {historyDetailResult.discussion && (
                    <div
                      style={{
                        background: "#fff",
                        borderRadius: 8,
                        padding: 16,
                        marginBottom: 12,
                      }}
                    >
                      <div
                        style={{
                          fontWeight: 600,
                          marginBottom: 8,
                        }}
                      >
                        团队讨论纪要
                      </div>
                      <div
                        style={{
                          fontSize: 13,
                          lineHeight: 1.6,
                          whiteSpace: "pre-line",
                        }}
                      >
                        {normalizeMarkdownText(historyDetailResult.discussion)}
                      </div>
                    </div>
                  )}

                  {historyDetailResult.data_fetch_diagnostics && (
                    <div
                      style={{
                        background: "#fff",
                        borderRadius: 8,
                        padding: 16,
                        marginBottom: 12,
                      }}
                    >
                      <div
                        style={{
                          fontWeight: 600,
                          marginBottom: 8,
                        }}
                      >
                        数据获取诊断
                      </div>
                      <div
                        style={{
                          display: "grid",
                          gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
                          gap: 12,
                          fontSize: 13,
                        }}
                      >
                        {Object.entries(
                          historyDetailResult.data_fetch_diagnostics,
                        ).map(([key, value]) => {
                          const v = value as any;
                          const status = v?.status || "unknown";
                          const hasError = status === "error";
                          const hasData = v?.has_data;
                          let label = key;
                          if (key === "stock_info") label = "基础信息";
                          if (key === "stock_data") label = "历史行情";
                          if (key === "technical_indicators") label = "技术指标";
                          if (key === "financial_data") label = "财务数据";
                          if (key === "fund_flow_data") label = "资金流数据";
                          if (key === "risk_data") label = "风险数据";
                          if (key === "sentiment_data") label = "市场情绪数据";
                          if (key === "news_data") label = "新闻数据";
                          if (key === "research_data") label = "研报数据";
                          if (key === "announcement_data") label = "公告数据";
                          if (key === "chip_data") label = "筹码数据";

                          const color = hasError
                            ? "#b00020"
                            : status === "success"
                              ? "#2e7d32"
                              : "#555";

                          let statusText = "未知";
                          if (status === "success") statusText = "获取成功";
                          else if (status === "error") statusText = "获取失败";
                          else if (status === "skipped")
                            statusText = "已跳过（分析师未启用）";
                          else if (status === "not_implemented")
                            statusText = "未接入统一获取";

                          return (
                            <div
                              key={key}
                              style={{
                                borderRadius: 8,
                                border: "1px solid #eee",
                                padding: 10,
                                background: "#fafafa",
                              }}
                            >
                              <div
                                style={{
                                  fontWeight: 600,
                                  marginBottom: 4,
                                }}
                              >
                                {label}
                              </div>
                              <div style={{ color, marginBottom: 4 }}>
                                状态：
                                {statusText}
                              </div>
                              {typeof hasData === "boolean" && (
                                <div
                                  style={{
                                    marginBottom: 4,
                                  }}
                                >
                                  是否有数据：
                                  {hasData ? "是" : "否"}
                                </div>
                              )}
                              {v?.period && (
                                <div
                                  style={{
                                    marginBottom: 4,
                                  }}
                                >
                                  period：
                                  {String(v.period)}
                                </div>
                              )}
                              {v?.reason && (
                                <div
                                  style={{
                                    marginBottom: 4,
                                    fontSize: 12,
                                    color: "#777",
                                  }}
                                >
                                  原因：
                                  {v.reason === "analyst_disabled"
                                    ? "分析师未启用"
                                    : v.reason === "data_fetch_not_wired"
                                      ? "尚未接入统一数据获取"
                                      : "未显式传入启用配置"}
                                </div>
                              )}
                              {hasError && v?.error && (
                                <details
                                  style={{
                                    marginTop: 4,
                                  }}
                                >
                                  <summary
                                    style={{
                                      cursor: "pointer",
                                      fontSize: 12,
                                    }}
                                  >
                                    查看错误详情
                                  </summary>
                                  <pre
                                    style={{
                                      whiteSpace: "pre-wrap",
                                      marginTop: 4,
                                      fontSize: 11,
                                      background: "#fff",
                                      padding: 6,
                                      borderRadius: 4,
                                    }}
                                  >
                                    {String(v.error)}
                                  </pre>
                                </details>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  )}

                </div>
              )}
          </div>
        )}
      </section>
    </div>
  );
}
