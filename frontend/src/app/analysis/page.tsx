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

  const [historyDetailRecordId, setHistoryDetailRecordId] =
    useState<number | null>(null);
  const [historyDetailLoading, setHistoryDetailLoading] = useState(false);
  const [historyDetailError, setHistoryDetailError] = useState<string | null>(
    null,
  );
  const [historyDetailResult, setHistoryDetailResult] =
    useState<StockAnalysisResponse | null>(null);
  const [historyDetailContext, setHistoryDetailContext] =
    useState<StockContextResponse | null>(null);

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

    try {
      const payload = { ts_code: code };
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
      }

      if (!anaRes.ok) {
        throw new Error(`请求失败: ${anaRes.status}`);
      }
      const data: StockAnalysisResponse = await anaRes.json();
      setResult(data);

      setHistoryPage(1);
      setHistoryQuery(code);
      loadHistory(1, code);
    } catch (e: any) {
      setError(e?.message || "未知错误");
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

    loadHistory(1, "");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function handleHistorySearch() {
    loadHistory(1);
  }

  function handleHistoryReset() {
    setHistoryQuery("");
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
      setHistoryDetailContext(null);
      setHistoryDetailError(null);
      return;
    }

    setHistoryDetailRecordId(recordId);
    setHistoryDetailLoading(true);
    setHistoryDetailError(null);
    setHistoryDetailResult(null);
    setHistoryDetailContext(null);

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
        const ctxRes = await fetch(`${API_BASE}/analysis/stock/context`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ts_code: detail.ts_code || symbol }),
        });
        if (ctxRes.ok) {
          const ctx: StockContextResponse = await ctxRes.json();
          setHistoryDetailContext(ctx);
        }
      } catch {
        // ignore context error
      }
    } catch (e: any) {
      setHistoryDetailError(e?.message || "历史详情请求失败");
    } finally {
      setHistoryDetailLoading(false);
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
        </div>

        {result && (
          <div
            style={{
              marginTop: 16,
              paddingTop: 12,
              borderTop: "1px solid #eee",
              fontSize: 14,
            }}
          >
            <h3 style={{ margin: "0 0 6px" }}>分析结果 - {result.ts_code}</h3>
            <p style={{ whiteSpace: "pre-line" }}>
              {normalizeMarkdownText(result.conclusion)}
            </p>
          </div>
        )}

        {context && context.kline && context.kline.dates.length > 0 && (
          <div style={{ marginTop: 16 }}>
            <h3 style={{ margin: "0 0 4px" }}>价格走势</h3>
            <Plot
              data={[
                {
                  x: context.kline.dates,
                  y: context.kline.close,
                  type: "scatter",
                  mode: "lines",
                  line: { color: "#4a67e8", width: 2 },
                  name: "收盘价",
                },
              ]}
              layout={{
                margin: { l: 40, r: 10, t: 10, b: 30 },
                height: 260,
                xaxis: { title: "日期", showgrid: false },
                yaxis: { title: "价格", showgrid: true },
                showlegend: false,
              }}
              style={{ width: "100%", height: "100%" }}
              config={{ displayModeBar: false, responsive: true }}
            />
          </div>
        )}
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
}
