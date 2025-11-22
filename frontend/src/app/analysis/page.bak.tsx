"use client";

import { useEffect, useMemo, useState } from "react";
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
  agents: AgentOpinion[];
  conclusion: string;
  agents_raw?: Record<string, any> | null;
  discussion?: string | null;
  final_decision?: Record<string, any> | null;
  data_fetch_diagnostics?: Record<string, any> | null;
  record_id?: number | null;
  saved_to_db?: boolean | null;
}

interface StockKlineSeries {
  dates: string[];
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
}

interface StockContextResponse {
  ts_code: string;
  name: string;
  quote: StockQuote | null;
  kline: StockKlineSeries | null;
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

interface HistoryRecord {
  id: number;
  symbol: string;
  stock_name: string;
  analysis_date?: string | null;
  period?: string | null;
  rating?: string | null;
  created_at?: string | null;
}

interface HistoryListResponse {
  total: number;
  items: HistoryRecord[];
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
      // 去掉 Markdown 代码块标记
      if (/^```/.test(l.trim())) return "";
      // 去掉 Markdown 标题符号
      l = l.replace(/^#{1,6}\s+/, "");
      // 将无序列表符号统一为中文项目符号
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

  // 去掉 ```json / ``` 包裹
  if (text.startsWith("```")) {
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
    // 不是合法 JSON 时忽略
  }
  return null;
}

function getMainConclusionText(result: StockAnalysisResponse): string {
  const fd = (result.final_decision || {}) as any;
  if (fd && typeof fd.summary === "string" && fd.summary.trim().length > 0) {
    return normalizeMarkdownText(fd.summary);
  }

  const fromJson = extractSummaryFromJsonLike(result.conclusion);
  if (fromJson) return normalizeMarkdownText(fromJson);

  return normalizeMarkdownText(result.conclusion || "");
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
  if (s.length >= 19) return s.slice(0, 19); // YYYY-MM-DD HH:MM:SS
  if (s.length >= 10) return s.slice(0, 10); // YYYY-MM-DD
  return s;
}

function formatDate(value?: string | null): string {
  const dt = formatDateTime(value);
  if (dt === "-") return "-";
  if (dt.length >= 10) return dt.slice(0, 10);
  return dt;
}

export default function AnalysisPage() {
  const [tsCode, setTsCode] = useState("000001");
  const [enabledAnalysts, setEnabledAnalysts] = useState<EnabledAnalysts>(
    DEFAULT_ENABLED_ANALYSTS,
  );
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<StockAnalysisResponse | null>(null);
  const [context, setContext] = useState<StockContextResponse | null>(null);

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
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [historyRating, setHistoryRating] = useState("");
  const [historyStartDate, setHistoryStartDate] = useState("");
  const [historyEndDate, setHistoryEndDate] = useState("");
  const [historyDeletingIds, setHistoryDeletingIds] = useState<number[]>([]);
  const [historyMonitorIds, setHistoryMonitorIds] = useState<number[]>([]);
  const [historyDetailRecordId, setHistoryDetailRecordId] = useState<number | null>(
    null,
  );
  const [historyDetailLoading, setHistoryDetailLoading] = useState(false);
  const [historyDetailError, setHistoryDetailError] = useState<string | null>(
    null,
  );
  const [historyDetailText, setHistoryDetailText] = useState("");
  const [historyDetailResult, setHistoryDetailResult] =
    useState<StockAnalysisResponse | null>(null);
  const [historyDetailContext, setHistoryDetailContext] =
    useState<StockContextResponse | null>(null);
  const [activeAgentIndex, setActiveAgentIndex] = useState(0);

  const batchCodes = useMemo(
    () =>
      batchCodesText
        .split(/[\n,]+/)
        .map((s) => s.trim())
        .filter(Boolean),
    [batchCodesText],
  );

  useEffect(() => {
    try {
      if (typeof window === "undefined") return;
      const cached = window.localStorage.getItem(
        "analysis_prefill_batch_codes",
      );
      if (cached && cached.trim()) {
        setBatchCodesText(cached);
      }
    } catch {
      // 本地存储不可用时忽略
    }
  }, []);

  useEffect(() => {
    if (typeof window !== "undefined") {
      try {
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
      } catch {
      }
    }
    loadHistory(1, "");
    // 初次加载页面时显示最近的历史记录
  }, []);

  const activeAnalystLabels = useMemo(() => {
    const mapping: Record<string, string> = {
      technical: "📊 技术分析师",
      fundamental: "💼 基本面分析师",
      fund_flow: "💰 资金面分析师",
      risk: "⚠️ 风险管理师",
      sentiment: "📈 市场情绪分析师",
      news: "📰 新闻分析师",
      research: "📑 机构研报分析师",
      announcement: "📢 公告分析师",
      chip: "🎯 筹码分析师",
    };
    return Object.entries(enabledAnalysts)
      .filter(([, v]) => v)
      .map(([k]) => mapping[k]);
  }, [enabledAnalysts]);

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
    try {
      const payload = {
        // 旧数据访问层支持纯6位代码，内部会自动转换为 ts_code
        ts_code: code,
        enabled_analysts: enabledAnalysts,
      };

      const [ctxRes, anaRes] = await Promise.all([
        fetch(`${API_BASE}/analysis/stock/context`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ts_code: code }),
        }),
        fetch(`${API_BASE}/analysis/stock`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        }),
      ]);

      if (ctxRes.ok) {
        const ctx: StockContextResponse = await ctxRes.json();
        setContext(ctx);
      } else {
        setContext(null);
      }

      if (!anaRes.ok) {
        throw new Error(`请求失败: ${anaRes.status}`);
      }
      const data: StockAnalysisResponse = await anaRes.json();
      setResult(data);
      setActiveAgentIndex(0);

      // 分析成功后，自动按当前代码刷新历史记录列表
      setHistoryPage(1);
      setHistoryQuery(code);
      loadHistory(1, code);
    } catch (e: any) {
      setError(e?.message || "未知错误");
    } finally {
      setLoading(false);
    }
  }

  function toggleAnalyst(key: keyof EnabledAnalysts) {
    setEnabledAnalysts((prev) => ({ ...prev, [key]: !prev[key] }));
  }

  function handleUseFirstBatchCode() {
    if (!batchCodes.length) {
      setError("当前没有可用的批量预填代码，请先在云选股等页面勾选股票并触发批量分析预填。");
      return;
    }
    const first = batchCodes[0];
    setTsCode(first);
    setError(null);
  }

  async function handleDownloadPdf() {
    if (!result?.record_id) {
      setError("当前分析结果尚未成功保存，暂无法导出 PDF 报告，请先重新分析一次。");
      return;
    }
    const url = `${API_BASE}/analysis/stock/report/pdf/${result.record_id}`;
    try {
      if (typeof window !== "undefined") {
        window.open(url, "_blank");
      }
    } catch (e: any) {
      setError(e?.message || "打开 PDF 下载链接失败");
    }
  }

  async function handleDownloadMarkdown() {
    if (!result?.record_id) {
      setError("当前分析结果尚未成功保存，暂无法导出 Markdown 报告，请先重新分析一次。");
      return;
    }
    const url = `${API_BASE}/analysis/stock/report/markdown/${result.record_id}`;
    try {
      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(`Markdown 报告请求失败: ${res.status}`);
      }
      const text = await res.text();
      const blob = new Blob([text], {
        type: "text/markdown;charset=utf-8;",
      });
      const a = document.createElement("a");
      const symbol = result.ts_code || "unknown";
      const ts = new Date()
        .toISOString()
        .replace(/[-:T]/g, "")
        .slice(0, 14);
      a.href = URL.createObjectURL(blob);
      a.download = `股票分析报告_${symbol}_${ts}.md`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(a.href);
    } catch (e: any) {
      setError(e?.message || "下载 Markdown 报告失败");
    }
  }

  async function handleBatchAnalyze() {
    const codes = batchCodes;
    if (!codes.length) {
      setBatchError("当前没有可用的批量代码，请先在上方文本框或其他页面预填股票代码。");
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
    }
  }

  function handleBatchClearResult() {
    setBatchResult(null);
    setBatchError(null);
  }

  async function loadHistory(
    pageOverride?: number,
    queryOverride?: string,
  ): Promise<void> {
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

  function handleHistorySearch() {
    // 使用当前输入的关键字重新加载第一页
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

  async function handleHistoryDownloadPdf(recordId: number) {
    const url = `${API_BASE}/analysis/stock/report/pdf/${recordId}`;
    try {
      if (typeof window !== "undefined") {
        window.open(url, "_blank");
      }
    } catch (e: any) {
      setHistoryError(e?.message || "打开历史记录 PDF 下载链接失败");
    }
  }

  async function handleHistoryDelete(recordId: number) {
    if (typeof window !== "undefined") {
      const ok = window.confirm("确认删除该历史分析记录？此操作不可恢复。");
      if (!ok) return;
    }

    setHistoryDeletingIds((prev) => [...prev, recordId]);
    try {
      const res = await fetch(
        `${API_BASE}/analysis/history/${recordId}`,
        {
          method: "DELETE",
        },
      );
      if (!res.ok) {
        let msg = "删除历史记录失败";
        try {
          const data = await res.json();
          if (data?.detail) msg = String(data.detail);
        } catch {
          // ignore
        }
        throw new Error(msg);
      }
      // 删除成功后，重新加载当前页
      await loadHistory(historyPage);
    } catch (e: any) {
      setHistoryError(e?.message || "删除历史记录时发生未知错误");
    } finally {
      setHistoryDeletingIds((prev) => prev.filter((id) => id !== recordId));
    }
  }

  async function handleHistoryAddToMonitor(recordId: number) {
    if (typeof window !== "undefined") {
      const ok = window.confirm(
        "确认将该历史记录一键加入监测？系统将根据该记录的最终投资决策自动提取进场区间/止盈/止损等参数。",
      );
      if (!ok) return;
    }

    setHistoryMonitorIds((prev) => [...prev, recordId]);
    try {
      const res = await fetch(
        `${API_BASE}/analysis/history/${recordId}/monitor_quick_add`,
        {
          method: "POST",
        },
      );
      if (!res.ok) {
        let msg = "加入监测失败";
        try {
          const data = await res.json();

  async function handleHistoryViewDetail(recordId: number, symbol: string) {
    if (historyDetailRecordId === recordId && historyDetailResult) {
      setHistoryDetailRecordId(null);
      return;
    }
    setHistoryDetailRecordId(recordId);
    setHistoryDetailLoading(true);
    setHistoryDetailError(null);
    setHistoryDetailText("");
    setHistoryDetailResult(null);
    setHistoryDetailContext(null);
    try {
      const detailRes = await fetch(`${API_BASE}/analysis/history/${recordId}`);
      if (!detailRes.ok) {
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
    </section>

    {/* 历史分析记录（简化版） */}
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
                {item.stock_name} ({item.symbol}) - {" "}
                {item.analysis_date
                  ? formatDateTime(item.analysis_date)
                  : "未知时间"}
                {" "}- 评级：{item.rating || "未知"}
                <button
                  type="button"
                  onClick={() => handleHistoryViewDetail(item.id, item.symbol)}
                  style={{
                    marginLeft: 8,
                    padding: "2px 8px",
                    borderRadius: 999,
                    border: "1px solid #4b5563",
                    background:
                      historyDetailRecordId === item.id ? "#e5e7eb" : "#111827",
                    color:
                      historyDetailRecordId === item.id ? "#111827" : "#f9fafb",
                    fontSize: 12,
                    cursor: "pointer",
                  }}
                >
                  {historyDetailRecordId === item.id ? "收起详情" : "详情"}
                </button>
              </li>
            ))}
          </ul>
        </>
      )}

      {historyDetailRecordId && (
        <div style={{ marginTop: 12, fontSize: 13 }}>
          {historyDetailLoading && (
            <p style={{ margin: 0, color: "#555" }}>正在加载完整历史报告...</p>
          )}
          {!historyDetailLoading && historyDetailError && (
            <p style={{ margin: 0, color: "#b00020" }}>
              加载详情失败：
              {historyDetailError}
            </p>
          )}
          {!historyDetailLoading && !historyDetailError && historyDetailResult && (
            <div>
              <h3 style={{ fontSize: 16, margin: "8px 0 4px" }}>
                历史记录详情 - {historyDetailResult.ts_code}
              </h3>
              <p style={{ whiteSpace: "pre-line" }}>
                {normalizeMarkdownText(historyDetailResult.conclusion)}
              </p>
              {historyDetailContext && (
                <p style={{ marginTop: 4 }}>
                  实时行情：
                  {historyDetailContext.quote?.current_price != null
                    ? `¥${historyDetailContext.quote.current_price.toFixed(2)}`
                    : "-"}
                </p>
              )}
            </div>
          )}
        </div>
      )}
    </section>
  </div>
);
                            "linear-gradient(135deg, #e74c3c 0%, #f97316 100%)",
                          color: "#fff",
                          fontSize: 12,
                          cursor: "pointer",
                        }}
                      >
                        📄 为该历史记录导出 PDF 报告
                      </button>
                      <button
                        type="button"
                        onClick={() => handleHistoryAddToMonitor(item.id)}
                        disabled={historyMonitorIds.includes(item.id)}
                        style={{
                          padding: "6px 12px",
                          borderRadius: 999,
                          border: "1px solid #0ea5e9",
                          background: historyMonitorIds.includes(item.id)
                            ? "#e0f2fe"
                            : "#f0f9ff",
                          color: "#0369a1",
                          fontSize: 12,
                          cursor: historyMonitorIds.includes(item.id)
                            ? "default"
                            : "pointer",
                        }}
                      >
                        ➕ 加入监测
                      </button>
                      <button
                        type="button"
                        onClick={() => handleHistoryDelete(item.id)}
                        disabled={historyDeletingIds.includes(item.id)}
                        style={{
                          padding: "6px 12px",
                          borderRadius: 999,
                          border: "1px solid #e11d48",
                          background: historyDeletingIds.includes(item.id)
                            ? "#fee2e2"
                            : "#fef2f2",
                          color: "#b91c1c",
                          fontSize: 12,
                          cursor: historyDeletingIds.includes(item.id)
                            ? "default"
                            : "pointer",
                        }}
                      >
                        🗑️ 删除记录
                      </button>
                    </div>
                    {historyDetailRecordId === item.id && (
                      <div style={{ marginTop: 8, fontSize: 12 }}>
                        {historyDetailLoading ? (
                          <p style={{ margin: 0, color: "#555" }}>
                            正在加载完整历史报告...
                          </p>
                        ) : historyDetailError ? (
                          <p style={{ margin: 0, color: "#b00020" }}>
                            加载详情失败：
                            {historyDetailError}
                          </p>
                        ) : historyDetailResult ? (
                          <div>
                            {historyDetailContext && (
                              <section
                                style={{
                                  background: "#fff",
                                  borderRadius: 12,
                                  padding: 16,
                                  boxShadow:
                                    "0 4px 16px rgba(0,0,0,0.08)",
                                  marginBottom: 16,
                                }}
                              >
                                <h3
                                  style={{ marginTop: 0, fontSize: 16 }}
                                >
                                  📊 实时行情与价格走势
                                </h3>
                                <div
                                  style={{
                                    display: "grid",
                                    gridTemplateColumns:
                                      "repeat(4, minmax(0, 1fr))",
                                    gap: 12,
                                    marginBottom: 12,
                                    fontSize: 13,
                                  }}
                                >
                                  <div className="metric-card">
                                    <div
                                      style={{ fontWeight: 600 }}
                                    >
                                      名称 / 代码
                                    </div>
                                    <div style={{ marginTop: 4 }}>
                                      {historyDetailContext.name ||
                                        historyDetailContext.quote?.name ||
                                        "-"}
                                    </div>
                                    <div
                                      style={{
                                        marginTop: 2,
                                        opacity: 0.7,
                                      }}
                                    >
                                      {historyDetailContext.quote
                                        ?.symbol ||
                                        historyDetailContext.ts_code}
                                    </div>
                                  </div>
                                  <div className="metric-card">
                                    <div
                                      style={{ fontWeight: 600 }}
                                    >
                                      现价
                                    </div>
                                    <div
                                      style={{
                                        marginTop: 4,
                                        fontSize: 16,
                                      }}
                                    >
                                      {historyDetailContext.quote
                                        ?.current_price != null
                                        ? `¥${historyDetailContext.quote.current_price.toFixed(2)}`
                                        : "-"}
                                    </div>
                                  </div>
                                  <div className="metric-card">
                                    <div
                                      style={{ fontWeight: 600 }}
                                    >
                                      涨跌幅
                                    </div>
                                    <div
                                      style={{
                                        marginTop: 4,
                                        fontSize: 16,
                                        color:
                                          (historyDetailContext.quote
                                            ?.change_percent ?? 0) > 0
                                            ? "#e53935"
                                            : (historyDetailContext.quote
                                                  ?.change_percent ?? 0) < 0
                                              ? "#1e88e5"
                                              : "#333",
                                      }}
                                    >
                                      {historyDetailContext.quote
                                        ?.change_percent != null
                                        ? `${historyDetailContext.quote.change_percent.toFixed(2)}%`
                                        : "-"}
                                    </div>
                                  </div>
                                  <div className="metric-card">
                                    <div
                                      style={{ fontWeight: 600 }}
                                    >
                                      成交额 / 成交量
                                    </div>
                                    <div style={{ marginTop: 4 }}>
                                      {historyDetailContext.quote
                                        ?.amount != null
                                        ? `${(historyDetailContext.quote.amount / 1e8).toFixed(2)} 亿`
                                        : "-"}
                                    </div>
                                    <div
                                      style={{
                                        marginTop: 2,
                                        opacity: 0.7,
                                      }}
                                    >
                                      {historyDetailContext.quote
                                        ?.volume != null
                                        ? `${(historyDetailContext.quote.volume / 1e4).toFixed(0)} 万手`
                                        : ""}
                                    </div>
                                  </div>
                                </div>

                                {historyDetailContext.kline &&
                                  historyDetailContext.kline.dates
                                    .length > 0 && (
                                    <div style={{ marginTop: 8 }}>
                                      <Plot
                                        data={[
                                          {
                                            x: historyDetailContext
                                              .kline.dates,
                                            y: historyDetailContext
                                              .kline.close,
                                            type: "scatter",
                                            mode: "lines",
                                            line: {
                                              color: "#4a67e8",
                                              width: 2,
                                            },
                                            name: "收盘价",
                                          },
                                        ]}
                                        layout={{
                                          margin: {
                                            l: 40,
                                            r: 10,
                                            t: 10,
                                            b: 30,
                                          },
                                          height: 260,
                                          xaxis: {
                                            title: "日期",
                                            showgrid: false,
                                          },
                                          yaxis: {
                                            title: "价格",
                                            showgrid: true,
                                          },
                                          showlegend: false,
                                        }}
                                        style={{
                                          width: "100%",
                                          height: "100%",
                                        }}
                                        config={{
                                          displayModeBar: false,
                                          responsive: true,
                                        }}
                                      />
                                    </div>
                                  )}
                              </section>
                            )}

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
                                  boxShadow:
                                    "0 4px 18px rgba(76,175,80,0.25)",
                                }}
                              >
                                <h3
                                  style={{ marginTop: 0, fontSize: 16 }}
                                >
                                  📋 最终投资决策 -
                                  {" "}
                                  {historyDetailResult.ts_code}
                                </h3>
                                <div
                                  style={{
                                    marginTop: 6,
                                    padding: 10,
                                    borderRadius: 8,
                                    background:
                                      "rgba(255,255,255,0.9)",
                                    fontSize: 14,
                                    lineHeight: 1.6,
                                    whiteSpace: "pre-line",
                                  }}
                                >
                                  {getMainConclusionText(
                                    historyDetailResult,
                                  )}
                                </div>

                                {historyDetailResult.final_decision && (
                                  <div
                                    style={{
                                      marginTop: 8,
                                      fontSize: 13,
                                      lineHeight: 1.6,
                                    }}
                                  >
                                    {(() => {
                                      const fd =
                                        (historyDetailResult
                                          .final_decision ||
                                          {}) as any;
                                      return (
                                        <>
                                          {fd?.rating && (
                                            <p
                                              style={{
                                                margin: "0 0 4px",
                                              }}
                                            >
                                              <strong>
                                                投资评级：
                                              </strong>
                                              {String(fd.rating)}
                                            </p>
                                          )}
                                          {fd?.time_horizon && (
                                            <p
                                              style={{
                                                margin: "0 0 6px",
                                              }}
                                            >
                                              <strong>
                                                建议持有周期：
                                              </strong>
                                              {String(
                                                fd.time_horizon,
                                              )}
                                            </p>
                                          )}
                                          {Array.isArray(
                                            fd?.key_reasons,
                                          ) &&
                                            fd.key_reasons.length > 0 && (
                                              <div
                                                style={{
                                                  marginTop: 4,
                                                }}
                                              >
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
                                                    (
                                                      item: any,
                                                      idx2: number,
                                                    ) => (
                                                      <li
                                                        key={idx2}
                                                      >
                                                        {normalizeMarkdownText(
                                                          typeof item ===
                                                              "string"
                                                            ? item
                                                            : String(
                                                                item,
                                                              ),
                                                        )}
                                                      </li>
                                                    ),
                                                  )}
                                                </ul>
                                              </div>
                                            )}
                                          {Array.isArray(
                                            fd?.risk_points,
                                          ) &&
                                            fd.risk_points.length > 0 && (
                                              <div
                                                style={{
                                                  marginTop: 6,
                                                }}
                                              >
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
                                                    (
                                                      item: any,
                                                      idx2: number,
                                                    ) => (
                                                      <li
                                                        key={idx2}
                                                      >
                                                        {normalizeMarkdownText(
                                                          typeof item ===
                                                              "string"
                                                            ? item
                                                            : String(
                                                                item,
                                                              ),
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

                            {historyDetailResult.agents &&
                              historyDetailResult.agents.length > 0 && (
                                <section
                                  style={{
                                    marginBottom: 16,
                                    background: "#fff",
                                    borderRadius: 12,
                                    padding: 16,
                                    boxShadow:
                                      "0 4px 16px rgba(0,0,0,0.08)",
                                  }}
                                >
                                  <h3
                                    style={{
                                      marginTop: 0,
                                      fontSize: 16,
                                    }}
                                  >
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
                                    {historyDetailResult.agents.map(
                                      (agent, idx2) => {
                                        const active =
                                          idx2 === activeAgentIndex;
                                        return (
                                          <button
                                            key={idx2}
                                            type="button"
                                            onClick={() =>
                                              setActiveAgentIndex(
                                                idx2,
                                              )
                                            }
                                            style={{
                                              padding: "6px 12px",
                                              borderRadius: 999,
                                              border: active
                                                ? "1px solid #4f46e5"
                                                : "1px solid #d1d5db",
                                              background: active
                                                ? "linear-gradient(135deg, #4f46e5 0%, #6366f1 100%)"
                                                : "#f9fafb",
                                              color: active
                                                ? "#f9fafb"
                                                : "#111827",
                                              fontSize: 12,
                                              cursor: "pointer",
                                            }}
                                          >
                                            {agent.name}
                                          </button>
                                        );
                                      },
                                    )}
                                  </div>

                                  {historyDetailResult.agents[
                                    activeAgentIndex
                                  ] && (
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
                                        {
                                          historyDetailResult.agents[
                                            activeAgentIndex
                                          ].name
                                        }
                                      </h4>
                                      <div
                                        style={{
                                          padding: 10,
                                          borderRadius: 8,
                                          background:
                                            "rgba(255,255,255,0.9)",
                                          fontSize: 14,
                                          lineHeight: 1.6,
                                          whiteSpace: "pre-line",
                                        }}
                                      >
                                        {normalizeMarkdownText(
                                          historyDetailResult.agents[
                                            activeAgentIndex
                                          ].summary,
                                        )}
                                      </div>
                                    </div>
                                  )}
                                </section>
                              )}

                            {historyDetailResult
                              .data_fetch_diagnostics && (
                              <section
                                style={{
                                  marginTop: 16,
                                  background: "#fff",
                                  borderRadius: 12,
                                  padding: 16,
                                  boxShadow:
                                    "0 4px 16px rgba(0,0,0,0.08)",
                                }}
                              >
                                <h3
                                  style={{ marginTop: 0, fontSize: 16 }}
                                >
                                  🔍 数据获取诊断
                                </h3>
                                <div
                                  style={{
                                    display: "grid",
                                    gridTemplateColumns:
                                      "repeat(3, minmax(0, 1fr))",
                                    gap: 12,
                                    fontSize: 13,
                                  }}
                                >
                                  {Object.entries(
                                    historyDetailResult
                                      .data_fetch_diagnostics,
                                  ).map(([key, value]) => {
                                    const v = value as any;
                                    const status = v?.status || "unknown";
                                    const hasError = status === "error";
                                    const hasData = v?.has_data;
                                    let label = key;
                                    if (key === "stock_info")
                                      label = "基础信息";
                                    if (key === "stock_data")
                                      label = "历史行情";
                                    if (key === "technical_indicators")
                                      label = "技术指标";
                                    if (key === "financial_data")
                                      label = "财务数据";
                                    if (key === "fund_flow_data")
                                      label = "资金流数据";
                                    if (key === "risk_data")
                                      label = "风险数据";
                                    if (key === "sentiment_data")
                                      label = "市场情绪数据";
                                    if (key === "news_data")
                                      label = "新闻数据";
                                    if (key === "research_data")
                                      label = "研报数据";
                                    if (key === "announcement_data")
                                      label = "公告数据";
                                    if (key === "chip_data")
                                      label = "筹码数据";

                                    const color = hasError
                                      ? "#b00020"
                                      : status === "success"
                                        ? "#2e7d32"
                                        : "#555";

                                    let statusText = "未知";
                                    if (status === "success")
                                      statusText = "获取成功";
                                    else if (status === "error")
                                      statusText = "获取失败";
                                    else if (status === "skipped")
                                      statusText = "已跳过（分析师未启用）";
                                    else if (
                                      status === "not_implemented"
                                    )
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
                                        <div
                                          style={{ color, marginBottom: 4 }}
                                        >
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
                                            {v.reason ===
                                            "analyst_disabled"
                                              ? "分析师未启用"
                                              : v.reason ===
                                                  "data_fetch_not_wired"
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
                                                whiteSpace:
                                                  "pre-wrap",
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
                              </section>
                            )}
                          </div>
                        ) : (
                          <p style={{ margin: 0, color: "#777" }}>
                            未找到该历史记录的详细结果。
                          </p>
                        )}
                      </div>
                    )}
                  </div>
                </details>
              ))}
            </div>

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
      </section>
    </div>
  );
}
