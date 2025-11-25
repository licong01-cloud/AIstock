"use client";

import { useEffect, useMemo, useState } from "react";
import dynamic from "next/dynamic";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false }) as any;

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

interface TrendPredictionScenario {
  direction: "up" | "down" | "flat";
  magnitude_min_pct: number;
  magnitude_max_pct: number;
  probability: number;
  label: string;
  narrative: string;
}

interface TrendPredictionHorizon {
  horizon: "1d" | "1w" | "1m" | "long";
  scenarios: TrendPredictionScenario[];
  base_expectation_pct?: number | null;
}

interface TrendAnalystResult {
  name: string;
  role: string;
  raw_text: string;
  conclusion_json: Record<string, any>;
  created_at: string;
}

interface PredictionStep {
  step: number;
  analyst_key: string;
  analyst_name: string;
  description: string;
  horizons: TrendPredictionHorizon[];
  created_at: string;
}

interface StockTrendAnalysisResponse {
  ts_code: string;
  analysis_date: string;
  mode: "realtime" | "backtest";
  horizons: TrendPredictionHorizon[];
  analysts: TrendAnalystResult[];
  risk_report?: TrendAnalystResult | null;
  prediction_evolution: PredictionStep[];
  record_id?: number | null;
  data_fetch_diagnostics?: Record<string, any> | null;
  technical_indicators?: Record<string, any> | null;
  rating?: string | null;
}

interface TrendHistoryRecord {
  id: number;
  symbol: string;
  stock_name: string;
  analysis_date?: string | null;
  mode?: string | null;
  rating?: string | null;
  created_at?: string | null;
}

interface TrendHistoryListResponse {
  total: number;
  items: TrendHistoryRecord[];
}

type EnabledAnalysts = Record<string, boolean>;

const DEFAULT_ENABLED_ANALYSTS: EnabledAnalysts = {
  technical: true,
  fundamental: true,
  risk: true,
  sentiment: false,
  news: false,
  research: false,
  announcement: false,
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

export default function TrendAnalysisPage() {
  const [tsCode, setTsCode] = useState("000001");
  const [enabledAnalysts, setEnabledAnalysts] =
    useState<EnabledAnalysts>(DEFAULT_ENABLED_ANALYSTS);
  const [trendResult, setTrendResult] =
    useState<StockTrendAnalysisResponse | null>(null);
  const [trendLoading, setTrendLoading] = useState(false);
  const [trendError, setTrendError] = useState<string | null>(null);
  const [trendContext, setTrendContext] =
    useState<StockContextResponse | null>(null);
  const [activeAnalystIndex, setActiveAnalystIndex] = useState(0);
  const [trendProgress, setTrendProgress] = useState<number | null>(null);
  const [trendStatus, setTrendStatus] = useState("");

  const [historyItems, setHistoryItems] = useState<TrendHistoryRecord[]>([]);
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
    useState<StockTrendAnalysisResponse | null>(null);
  const [historyDetailQuote, setHistoryDetailQuote] =
    useState<StockQuote | null>(null);
  const [historyActiveAnalystIndex, setHistoryActiveAnalystIndex] =
    useState(0);

  const allAnalystsSelected = useMemo(
    () => Object.values(enabledAnalysts).every((v) => v),
    [enabledAnalysts],
  );

  function toggleAnalyst(key: keyof EnabledAnalysts) {
    setEnabledAnalysts((prev) => ({ ...prev, [key]: !prev[key] }));
  }

  function handleToggleAllAnalysts() {
    setEnabledAnalysts((prev) => {
      if (allAnalystsSelected) {
        return { ...DEFAULT_ENABLED_ANALYSTS };
      }
      const next: EnabledAnalysts = {};
      Object.keys(prev).forEach((key) => {
        next[key] = true;
      });
      return next;
    });
  }

  async function handleAnalyzeTrend() {
    const code = tsCode.trim();
    if (!code) {
      setTrendError("请先输入股票代码（6位数字，例如 000001）");
      return;
    }
    if (!/^\d{6}$/.test(code)) {
      setTrendError("股票代码格式错误，仅支持6位数字，例如 000001");
      return;
    }

    setTrendLoading(true);
    setTrendError(null);
    setTrendResult(null);
    setTrendContext(null);
    setActiveAnalystIndex(0);
    setTrendProgress(0);
    setTrendStatus("准备开始趋势分析...");
    try {
      setTrendStatus("正在获取行情与基础数据...");
      setTrendProgress(10);
      try {
        const ctxRes = await fetch(`${API_BASE}/analysis/stock/context`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ts_code: code }),
        });
        if (ctxRes.ok) {
          const ctx: StockContextResponse = await ctxRes.json();
          setTrendContext(ctx);
        }
      } catch {
        // ignore context error, 不影响趋势分析
      }

      const payload = {
        ts_code: code,
        enabled_analysts: enabledAnalysts,
        mode: "realtime" as const,
      };
      setTrendStatus("正在获取多维度数据并运行趋势分析...");
      setTrendProgress(30);
      const res = await fetch(`${API_BASE}/analysis/stock/trend`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        throw new Error(`趋势分析请求失败: ${res.status}`);
      }
      const data: StockTrendAnalysisResponse = await res.json();
      setTrendResult(data);
      setTrendProgress(100);
      setTrendStatus("趋势分析完成");
    } catch (e: any) {
      setTrendError(e?.message || "趋势分析时发生未知错误");
      setTrendStatus("趋势分析失败");
    } finally {
      setTrendLoading(false);
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
        `${API_BASE}/analysis/trend/history?${params.toString()}`,
      );
      if (!res.ok) {
        throw new Error(`趋势历史记录请求失败: ${res.status}`);
      }
      const data: TrendHistoryListResponse = await res.json();
      setHistoryItems(data.items || []);
      setHistoryTotal(data.total ?? data.items?.length ?? 0);
      setHistoryPage(pageToLoad);
    } catch (e: any) {
      setHistoryError(e?.message || "加载趋势历史记录时发生未知错误");
      setHistoryItems([]);
      setHistoryTotal(0);
    } finally {
      setHistoryLoading(false);
    }
  }

  useEffect(() => {
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
    setHistoryActiveAnalystIndex(0);
    setHistoryDetailLoading(true);
    setHistoryDetailError(null);
    setHistoryDetailResult(null);
    setHistoryDetailQuote(null);

    try {
      const detailRes = await fetch(
        `${API_BASE}/analysis/trend/history/${recordId}`,
      );
      if (!detailRes.ok) {
        throw new Error(`趋势历史详情请求失败: ${detailRes.status}`);
      }
      const detail: StockTrendAnalysisResponse = await detailRes.json();
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
      setHistoryDetailError(e?.message || "趋势历史详情请求失败");
    } finally {
      setHistoryDetailLoading(false);
    }
  }

  function handleDownloadTrendPdf(recordId?: number | null) {
    if (!recordId) return;
    window.open(
      `${API_BASE}/analysis/stock/trend/report/pdf/${recordId}`,
      "_blank",
    );
  }

  function handleDownloadTrendMarkdown(recordId?: number | null) {
    if (!recordId) return;
    window.open(
      `${API_BASE}/analysis/stock/trend/report/markdown/${recordId}`,
      "_blank",
    );
  }

  return (
    <div style={{ padding: 24 }}>
      <h1 style={{ fontSize: 22, marginBottom: 16 }}>📈 股票趋势分析</h1>

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
              type="button"
              onClick={handleAnalyzeTrend}
              disabled={trendLoading}
              style={{
                padding: "8px 16px",
                borderRadius: 8,
                border: "none",
                background:
                  "linear-gradient(135deg, #0ea5e9 0%, #6366f1 100%)",
                color: "#fff",
                fontWeight: 600,
                cursor: trendLoading ? "default" : "pointer",
              }}
            >
              {trendLoading ? "趋势分析中..." : "📈 开始趋势分析"}
            </button>
          </div>
        </div>

        {trendError && (
          <p style={{ color: "#b00020", marginTop: 4 }}>错误：{trendError}</p>
        )}

        {trendProgress !== null && (
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
              {trendStatus || "正在进行趋势分析..."}
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
                  width: `${Math.min(100, Math.max(0, trendProgress))}%`,
                  height: "100%",
                  transition: "width 0.2s ease-out",
                  background:
                    "linear-gradient(90deg, #22c55e 0%, #16a34a 40%, #0ea5e9 100%)",
                }}
              />
            </div>
          </div>
        )}

        <div
          style={{
            marginTop: 12,
            paddingTop: 10,
            borderTop: "1px solid #eee",
            fontSize: 12,
          }}
        >
          <h3 style={{ fontSize: 14, margin: "0 0 4px" }}>👨‍💼 选择趋势分析师团队</h3>
          <p style={{ margin: "0 0 6px", color: "#555" }}>
            通过勾选下方分析师，可以控制趋势分析管线中参与修正的维度。
          </p>
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              margin: "4px 0 8px",
              gap: 8,
              flexWrap: "wrap",
            }}
          >
            <span style={{ fontSize: 12, color: "#6b7280" }}>
              默认启用：技术 / 基本面 / 资金面 / 风险管理师；其余维度可按需开启。
            </span>
            <button
              type="button"
              onClick={handleToggleAllAnalysts}
              style={{
                padding: "4px 10px",
                borderRadius: 999,
                border: "1px solid #d1d5db",
                background: allAnalystsSelected ? "#eef2ff" : "#f9fafb",
                fontSize: 12,
                cursor: "pointer",
                whiteSpace: "nowrap",
              }}
            >
              {allAnalystsSelected ? "恢复默认分析师" : "一键全选分析师"}
            </button>
          </div>
          <div
            style={{
              display: "flex",
              flexWrap: "wrap",
              gap: 8,
              marginTop: 4,
            }}
          >
            {(
              [
                ["technical", "📊 技术资金分析师"],
                ["fundamental", "💼 基本面分析师"],
                ["risk", "⚠️ 风险管理师"],
                ["sentiment", "📈 情绪分析师"],
                ["news", "📰 新闻分析师"],
                ["research", "📑 研报分析师"],
                ["announcement", "📢 公告分析师"],
              ] as [keyof EnabledAnalysts, string][]
            ).map(([key, label]) => (
              <label
                key={key}
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
                  checked={enabledAnalysts[key]}
                  onChange={() => toggleAnalyst(key)}
                />
                <span>{label}</span>
              </label>
            ))}
          </div>
        </div>
      </section>

      {trendContext && trendContext.kline &&
        trendContext.kline.dates.length > 0 && (
          <div style={{ marginTop: 16 }}>
            <h3 style={{ margin: "0 0 4px" }}>K线图</h3>
            <Plot
              data={[
                {
                  x: trendContext.kline.dates,
                  open: trendContext.kline.open,
                  high: trendContext.kline.high,
                  low: trendContext.kline.low,
                  close: trendContext.kline.close,
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

      {trendResult && (
        <section
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 16,
            boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
            fontSize: 13,
          }}
        >
          <h2 style={{ marginTop: 0, fontSize: 18 }}>📉 多周期趋势预测结果</h2>
          <p style={{ margin: "4px 0 8px", color: "#6b7280", fontSize: 12 }}>
            股票：
            {trendResult.ts_code} ，分析日期：
            {trendResult.analysis_date}，模式：
            {trendResult.mode === "realtime" ? "实时" : "回测"}
          </p>

          {trendContext && trendContext.quote && (
            <section
              style={{
                marginBottom: 16,
                background: "#fff",
                borderRadius: 12,
                padding: 16,
                boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
              }}
            >
              <h3 style={{ marginTop: 0, fontSize: 16 }}>📊 实时行情概览</h3>
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
                  const q = trendContext.quote as StockQuote;
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

          {(() => {
            const ti = trendResult.technical_indicators as any | null;
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
                <h3 style={{ marginTop: 0, fontSize: 16 }}>📈 关键技术指标</h3>
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
                      RSI 相对强弱指数
                    </div>
                    <div style={{ fontSize: 16, fontWeight: 600 }}>{rsi}</div>
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
                      MA20 - 20日均线
                    </div>
                    <div style={{ fontSize: 16, fontWeight: 600 }}>{ma20}</div>
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
                      量比（Volume Ratio）
                    </div>
                    <div style={{ fontSize: 16, fontWeight: 600 }}>
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
                      MACD 指标
                    </div>
                    <div style={{ fontSize: 16, fontWeight: 600 }}>{macd}</div>
                  </div>
                </div>
              </section>
            );
          })()}

          {trendResult.data_fetch_diagnostics && (
            <section
              style={{
                marginBottom: 16,
                background: "#fff",
                borderRadius: 12,
                padding: 16,
                boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
              }}
            >
              <h3 style={{ marginTop: 0, fontSize: 16 }}>🧪 数据获取诊断</h3>
              <div
                style={{
                  marginTop: 8,
                  display: "grid",
                  gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
                  gap: 12,
                  fontSize: 13,
                }}
              >
                {Object.entries(trendResult.data_fetch_diagnostics).map(
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
                        <div
                          style={{
                            fontSize: 12,
                            color: color,
                            marginBottom: 2,
                          }}
                        >
                          {statusText}
                          {typeof hasData === "boolean" &&
                            ` · ${hasData ? "有数据" : "无数据"}`}
                        </div>
                        {v?.error && (
                          <div
                            style={{
                              marginTop: 2,
                              fontSize: 12,
                              color: "#b00020",
                              wordBreak: "break-all",
                            }}
                          >
                            错误：
                            {String(v.error)}
                          </div>
                        )}
                      </div>
                    );
                  },
                )}
              </div>
            </section>
          )}

          {trendResult.horizons && trendResult.horizons.length > 0 && (
            <div
              style={{
                marginTop: 4,
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
                gap: 12,
              }}
            >
              {trendResult.horizons.map((h) => {
                const labelMap: Record<string, string> = {
                  "1d": "1天",
                  "1w": "1周",
                  "1m": "1个月",
                  long: "长线",
                };
                const horizonLabel = labelMap[h.horizon] || h.horizon;
                return (
                  <div
                    key={h.horizon}
                    style={{
                      padding: 10,
                      borderRadius: 10,
                      border: "1px solid #e5e7eb",
                      background: "#f9fafb",
                    }}
                  >
                    <div
                      style={{
                        fontWeight: 600,
                        fontSize: 13,
                        marginBottom: 4,
                      }}
                    >
                      {horizonLabel}
                      {typeof h.base_expectation_pct === "number" && (
                        <span
                          style={{
                            marginLeft: 4,
                            fontWeight: 400,
                            color: "#6b7280",
                          }}
                        >
                          （期望涨跌：
                          {h.base_expectation_pct >= 0 ? "+" : ""}
                          {h.base_expectation_pct.toFixed(2)}%
                          ）
                        </span>
                      )}
                    </div>
                    <ul
                      style={{
                        listStyle: "none",
                        paddingLeft: 0,
                        margin: 0,
                      }}
                    >
                      {h.scenarios.map((s, idx) => (
                        // eslint-disable-next-line react/no-array-index-key
                        <li
                          key={idx}
                          style={{ marginBottom: 4, lineHeight: 1.5 }}
                        >
                          <div>
                            <strong>{s.label}</strong>{" "}
                            <span style={{ color: "#6b7280" }}>
                              ({(s.probability * 100).toFixed(1)}%)
                            </span>
                          </div>
                          <div
                            style={{
                              fontSize: 11,
                              color: "#4b5563",
                            }}
                          >
                            {s.narrative}
                          </div>
                        </li>
                      ))}
                    </ul>
                  </div>
                );
              })}
            </div>
          )}

          {trendResult.prediction_evolution &&
            trendResult.prediction_evolution.length > 0 && (
              (() => {
                const steps = trendResult.prediction_evolution;
                const horizonKeys: ("1d" | "1w" | "1m" | "long")[] = [
                  "1d",
                  "1w",
                  "1m",
                  "long",
                ];
                const labelMap: Record<string, string> = {
                  "1d": "1天",
                  "1w": "1周",
                  "1m": "1个月",
                  long: "长线",
                };
                const xLabels = steps.map(
                  (s) => `第${s.step}步-${s.analyst_name}`,
                );

                const traces = horizonKeys.map((hk) => {
                  const ys = steps.map((s) => {
                    const h = s.horizons.find((hh) => hh.horizon === hk);
                    return typeof h?.base_expectation_pct === "number"
                      ? h.base_expectation_pct
                      : null;
                  });
                  return {
                    x: xLabels,
                    y: ys,
                    mode: "lines+markers",
                    name: labelMap[hk] || hk,
                  };
                });

                return (
                  <div style={{ marginTop: 16 }}>
                    <h3 style={{ marginTop: 0, fontSize: 16 }}>
                      📈 各周期期望涨跌随分析步骤的变化
                    </h3>
                    <Plot
                      data={traces}
                      layout={{
                        margin: { l: 40, r: 10, t: 30, b: 50 },
                        height: 320,
                        xaxis: { title: "分析步骤", showgrid: false },
                        yaxis: { title: "期望涨跌幅(%)", showgrid: true },
                        showlegend: true,
                        legend: { orientation: "h" },
                      }}
                      style={{ width: "100%", height: "100%" }}
                      config={{ displayModeBar: false, responsive: true }}
                    />
                  </div>
                );
              })()
            )}

          {trendResult.horizons && trendResult.horizons.length > 0 && (
            <div style={{ marginTop: 16 }}>
              <h3 style={{ marginTop: 0, fontSize: 16 }}>📊 最终多周期期望涨跌</h3>
              <Plot
                data={[
                  {
                    x: trendResult.horizons.map((h) => {
                      const m: Record<string, string> = {
                        "1d": "1天",
                        "1w": "1周",
                        "1m": "1个月",
                        long: "长线",
                      };
                      return m[h.horizon] || h.horizon;
                    }),
                    y: trendResult.horizons.map((h) =>
                      typeof h.base_expectation_pct === "number"
                        ? h.base_expectation_pct
                        : 0,
                    ),
                    type: "bar",
                    marker: { color: "#6366f1" },
                  },
                ]}
                layout={{
                  margin: { l: 40, r: 10, t: 30, b: 40 },
                  height: 260,
                  xaxis: { title: "时间跨度", showgrid: false },
                  yaxis: { title: "期望涨跌幅(%)", showgrid: true },
                  showlegend: false,
                }}
                style={{ width: "100%", height: "100%" }}
                config={{ displayModeBar: false, responsive: true }}
              />
            </div>
          )}

          {trendResult.analysts && trendResult.analysts.length > 0 && (
            <div
              style={{
                marginTop: 16,
                fontSize: 12,
                color: "#4b5563",
              }}
            >
              <div style={{ marginBottom: 4, fontWeight: 600 }}>
                参与的趋势分析师（点击查看各自的修正与理由）：
              </div>
              <div
                style={{
                  marginTop: 6,
                  display: "flex",
                  flexWrap: "wrap",
                  gap: 8,
                }}
              >
                {trendResult.analysts.map((a, idx) => {
                  const active = idx === activeAnalystIndex;
                  return (
                    // eslint-disable-next-line react/no-array-index-key
                    <button
                      key={idx}
                      type="button"
                      onClick={() => setActiveAnalystIndex(idx)}
                      style={{
                        display: "inline-flex",
                        flexDirection: "column",
                        alignItems: "flex-start",
                        gap: 2,
                        padding: "6px 10px",
                        borderRadius: 999,
                        border: active
                          ? "1px solid #4f46e5"
                          : "1px solid #d1d5db",
                        background: active
                          ? "linear-gradient(135deg, #4f46e5 0%, #6366f1 100%)"
                          : "#f9fafb",
                        color: active ? "#f9fafb" : "#111827",
                        cursor: "pointer",
                        fontSize: 12,
                      }}
                    >
                      <span style={{ fontWeight: 600 }}>{a.name}</span>
                      <span
                        style={{
                          color: active ? "#e5e7eb" : "#6b7280",
                          fontSize: 11,
                        }}
                      >
                        {a.role}
                      </span>
                      <span
                        style={{
                          color: active ? "#e5e7eb" : "#9ca3af",
                          fontSize: 11,
                        }}
                      >
                        {formatDateTime(a.created_at)}
                      </span>
                    </button>
                  );
                })}
              </div>

              {trendResult.analysts[activeAnalystIndex] && (
                <div
                  style={{
                    marginTop: 12,
                    padding: 12,
                    borderRadius: 10,
                    background:
                      "linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)",
                  }}
                >
                  {(() => {
                    const a = trendResult.analysts[activeAnalystIndex];
                    const cj = (a.conclusion_json || {}) as any;
                    const factors = cj.factors || {};

                    const analystKey =
                      (cj.analyst_key as string | undefined) || "";
                    const steps = trendResult.prediction_evolution || [];
                    const currentStep =
                      steps.find(
                        (s) =>
                          s.analyst_key === analystKey ||
                          s.analyst_name === a.name,
                      ) || steps.find((s) => s.step === 0);
                    const prevStep =
                      currentStep && currentStep.step > 0
                        ? steps.find((s) => s.step === currentStep.step - 1)
                        : undefined;

                    const horizonsNow = currentStep?.horizons || [];
                    const horizonsPrev = prevStep?.horizons || [];

                    const horizonKeys: ("1d" | "1w" | "1m" | "long")[] = [
                      "1d",
                      "1w",
                      "1m",
                      "long",
                    ];
                    const labelMap: Record<string, string> = {
                      "1d": "1天",
                      "1w": "1周",
                      "1m": "1个月",
                      long: "长线",
                    };

                    const formatPct = (v: number | null | undefined) => {
                      if (typeof v === "number" && Number.isFinite(v)) {
                        const sign = v > 0 ? "+" : "";
                        return `${sign}${v.toFixed(2)}%`;
                      }
                      return "--";
                    };

                    return (
                      <>
                        <h4
                          style={{
                            marginTop: 0,
                            marginBottom: 6,
                            fontSize: 14,
                          }}
                        >
                          {a.name}
                        </h4>
                        {currentStep && (
                          <div
                            style={{
                              marginTop: 10,
                              padding: 10,
                              borderRadius: 8,
                              background: "#f9fafb",
                            }}
                          >
                            <div
                              style={{
                                fontWeight: 600,
                                marginBottom: 6,
                              }}
                            >
                              本轮对多周期期望涨跌的修正：
                            </div>
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
                                      textAlign: "left",
                                      padding: "4px 6px",
                                      borderBottom: "1px solid #e5e7eb",
                                    }}
                                  >
                                    周期
                                  </th>
                                  <th
                                    style={{
                                      textAlign: "right",
                                      padding: "4px 6px",
                                      borderBottom: "1px solid #e5e7eb",
                                    }}
                                  >
                                    本轮期望涨跌
                                  </th>
                                  <th
                                    style={{
                                      textAlign: "right",
                                      padding: "4px 6px",
                                      borderBottom: "1px solid #e5e7eb",
                                    }}
                                  >
                                    上一轮期望涨跌
                                  </th>
                                  <th
                                    style={{
                                      textAlign: "right",
                                      padding: "4px 6px",
                                      borderBottom: "1px solid #e5e7eb",
                                    }}
                                  >
                                    调整幅度
                                  </th>
                                </tr>
                              </thead>
                              <tbody>
                                {horizonKeys.map((hk) => {
                                  const cur = horizonsNow.find(
                                    (h) => h.horizon === hk,
                                  );
                                  const prev = horizonsPrev.find(
                                    (h) => h.horizon === hk,
                                  );
                                  const curVal =
                                    typeof cur?.base_expectation_pct ===
                                    "number"
                                      ? cur.base_expectation_pct
                                      : null;
                                  const prevVal =
                                    typeof prev?.base_expectation_pct ===
                                    "number"
                                      ? prev.base_expectation_pct
                                      : null;
                                  const delta =
                                    curVal != null && prevVal != null
                                      ? curVal - prevVal
                                      : null;

                                  return (
                                    <tr key={hk}>
                                      <td
                                        style={{
                                          padding: "4px 6px",
                                          borderBottom:
                                            "1px solid #f3f4f6",
                                        }}
                                      >
                                        {labelMap[hk] || hk}
                                      </td>
                                      <td
                                        style={{
                                          padding: "4px 6px",
                                          textAlign: "right",
                                          borderBottom:
                                            "1px solid #f3f4f6",
                                        }}
                                      >
                                        {formatPct(curVal)}
                                      </td>
                                      <td
                                        style={{
                                          padding: "4px 6px",
                                          textAlign: "right",
                                          borderBottom:
                                            "1px solid #f3f4f6",
                                        }}
                                      >
                                        {formatPct(prevVal)}
                                      </td>
                                      <td
                                        style={{
                                          padding: "4px 6px",
                                          textAlign: "right",
                                          borderBottom:
                                            "1px solid #f3f4f6",
                                        }}
                                      >
                                        {formatPct(delta)}
                                      </td>
                                    </tr>
                                  );
                                })}
                              </tbody>
                            </table>
                          </div>
                        )}

                        {factors && Object.keys(factors).length > 0 && (
                          <div
                            style={{
                              marginTop: 8,
                              fontSize: 12,
                              color: "#374151",
                              background: "rgba(255,255,255,0.9)",
                              borderRadius: 8,
                              padding: 8,
                            }}
                          >
                            <div
                              style={{
                                fontWeight: 600,
                                marginBottom: 4,
                              }}
                            >
                              主要打分因子（仅对技术资金分析师等适用）：
                            </div>
                            <div
                              style={{
                                display: "grid",
                                gridTemplateColumns:
                                  "repeat(auto-fit, minmax(140px, 1fr))",
                                gap: 4,
                              }}
                            >
                              {"change_pct" in factors && (
                                <div>近期涨跌幅：{formatPct(factors.change_pct)}</div>
                              )}
                              {"rsi" in factors && (
                                <div>
                                  RSI：
                                  {typeof factors.rsi === "number"
                                    ? factors.rsi.toFixed(2)
                                    : String(factors.rsi ?? "--")}
                                </div>
                              )}
                              {"volume_ratio" in factors && (
                                <div>
                                  量比：
                                  {typeof factors.volume_ratio === "number"
                                    ? factors.volume_ratio.toFixed(2)
                                    : String(factors.volume_ratio ?? "--")}
                                </div>
                              )}
                              {"score" in factors && (
                                <div>
                                  综合评分：
                                  {typeof factors.score === "number"
                                    ? factors.score.toFixed(2)
                                    : String(factors.score ?? "--")}
                                </div>
                              )}
                            </div>
                          </div>
                        )}

                        <div
                          style={{
                            marginTop: 10,
                            padding: 10,
                            borderRadius: 8,
                            background: "rgba(255,255,255,0.9)",
                            fontSize: 13,
                            lineHeight: 1.6,
                            whiteSpace: "pre-line",
                          }}
                        >
                          {normalizeMarkdownText(a.raw_text)}
                        </div>
                      </>
                    );
                  })()}
                </div>
              )}
            </div>
          )}

          {trendResult.risk_report && (
            <div
              style={{
                marginTop: 12,
                padding: 10,
                borderRadius: 10,
                border: "1px dashed #f97316",
                background: "#fff7ed",
                fontSize: 12,
                whiteSpace: "pre-line",
              }}
            >
              <div
                style={{
                  fontWeight: 600,
                  marginBottom: 4,
                }}
              >
                ⚠️ 风险管理师补充说明
              </div>
              {normalizeMarkdownText(trendResult.risk_report.raw_text)}
            </div>
          )}

          {trendResult.prediction_evolution &&
            trendResult.prediction_evolution.length > 1 && (
              <div
                style={{
                  marginTop: 12,
                  fontSize: 12,
                }}
              >
                <details>
                  <summary
                    style={{
                      cursor: "pointer",
                      color: "#4b5563",
                      outline: "none",
                    }}
                  >
                    查看预测演进过程（
                    {trendResult.prediction_evolution.length}
                    步）
                  </summary>
                  <ol
                    style={{
                      marginTop: 6,
                      paddingLeft: 20,
                    }}
                  >
                    {trendResult.prediction_evolution.map((step) => (
                      <li key={step.step} style={{ marginBottom: 4 }}>
                        <strong>
                          第 {step.step} 步：
                          {step.analyst_name}
                        </strong>{" "}
                        -
                        {" "}
                        {step.description}
                      </li>
                    ))}
                  </ol>
                </details>
              </div>
            )}

          <div style={{ marginTop: 16, display: "flex", gap: 12 }}>
            <button
              type="button"
              onClick={() => handleDownloadTrendPdf(trendResult.record_id)}
              style={{
                padding: "8px 16px",
                borderRadius: 8,
                border: "1px solid #d1d5db",
                background: "#fff",
                cursor: "pointer",
                fontSize: 13,
              }}
            >
              📄 导出趋势 PDF 报告
            </button>
            <button
              type="button"
              onClick={() => handleDownloadTrendMarkdown(trendResult.record_id)}
              style={{
                padding: "8px 16px",
                borderRadius: 8,
                border: "1px solid #d1d5db",
                background: "#fff",
                cursor: "pointer",
                fontSize: 13,
              }}
            >
              ⬇️ 导出趋势 Markdown
            </button>
          </div>
        </section>
      )}

      <section
        id="trend-history-section"
        style={{
          marginTop: 16,
          background: "#fff",
          borderRadius: 12,
          padding: 16,
          boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
          fontSize: 13,
        }}
      >
        <h2 style={{ marginTop: 0, fontSize: 18 }}>📚 趋势分析历史记录</h2>

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
              minWidth: 140,
              borderRadius: 8,
              border: "1px solid #ddd",
              padding: "6px 8px",
              background: "#fff",
            }}
          >
            <option value="">趋势评级不限</option>
            <option value="强烈买入">强烈买入</option>
            <option value="买入">买入</option>
            <option value="增持">增持</option>
            <option value="中性/持有">中性/持有</option>
            <option value="减持">减持</option>
            <option value="卖出/回避">卖出/回避</option>
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
            趋势历史错误：
            {historyError}
          </p>
        )}

        {historyLoading && (
          <p style={{ fontSize: 13, color: "#555" }}>正在加载趋势历史记录...</p>
        )}

        {!historyLoading && historyItems.length === 0 && (
          <p style={{ fontSize: 13, color: "#777" }}>
            暂无趋势分析历史记录，可先在上方完成一次趋势分析后再查看，或通过搜索加载已有记录。
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
                  {item.stock_name} ({item.symbol}) -
                  {" "}
                  {item.analysis_date
                    ? formatDateTime(item.analysis_date)
                    : "未知时间"}
                  {" "}- 模式：
                  {item.mode === "backtest" ? "回测" : "实时"}
                  {" "}- 趋势评级：
                  {item.rating || "未知"}
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
                    onClick={() => handleDownloadTrendPdf(item.id)}
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
                正在加载完整历史趋势报告...
              </p>
            )}
            {!historyDetailLoading && historyDetailError && (
              <p style={{ margin: 0, color: "#b00020" }}>
                加载趋势详情失败：
                {historyDetailError}
              </p>
            )}
            {!historyDetailLoading &&
              !historyDetailError &&
              historyDetailResult && (
                <div
                  style={{
                    padding: 12,
                    background: "#f9fafb",
                    borderRadius: 8,
                    marginTop: 8,
                  }}
                >
                  <h3 style={{ fontSize: 16, margin: "8px 0 12px" }}>
                    📋 趋势历史详情 - {historyDetailResult.ts_code}
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
                            ? `${
                                historyDetailQuote.change_percent > 0 ? "+" : ""
                              }${historyDetailQuote.change_percent.toFixed(2)}%`
                            : "--"}
                        </div>
                      </div>
                    </div>
                  )}

                  <div
                    style={{
                      background: "#fff",
                      borderRadius: 8,
                      padding: 12,
                      marginBottom: 8,
                    }}
                  >
                    <div style={{ marginBottom: 4 }}>
                      分析日期：{historyDetailResult.analysis_date}，模式：
                      {historyDetailResult.mode === "realtime" ? "实时" : "回测"}
                    </div>
                    <div>
                      趋势评级：{historyDetailResult.rating || "未知"}
                    </div>
                  </div>

                  {historyDetailResult.horizons &&
                    historyDetailResult.horizons.length > 0 && (
                      <div
                        style={{
                          background: "#fff",
                          borderRadius: 8,
                          padding: 12,
                          marginBottom: 8,
                        }}
                      >
                        <h4
                          style={{
                            marginTop: 0,
                            fontSize: 14,
                            marginBottom: 6,
                          }}
                        >
                          多周期预期概览
                        </h4>
                        <ul
                          style={{
                            listStyle: "none",
                            paddingLeft: 0,
                            margin: 0,
                          }}
                        >
                          {historyDetailResult.horizons.map((h) => {
                            const labelMap: Record<string, string> = {
                              "1d": "1天",
                              "1w": "1周",
                              "1m": "1个月",
                              long: "长线",
                            };
                            const label = labelMap[h.horizon] || h.horizon;
                            return (
                              <li
                                key={h.horizon}
                                style={{ marginBottom: 4, fontSize: 13 }}
                              >
                                {label}：
                                {typeof h.base_expectation_pct === "number"
                                  ? `${
                                      h.base_expectation_pct >= 0 ? "+" : ""
                                    }${h.base_expectation_pct.toFixed(2)}%`
                                  : "不明确"}
                              </li>
                            );
                          })}
                        </ul>
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
