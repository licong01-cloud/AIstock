"use client";

import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type ModelKey = string;
type ActiveTab = "analysis" | "history";

interface CacheMeta {
  from_cache?: boolean;
  cache_warning?: string;
  data_timestamp?: string;
}

interface SectorLongShortItem {
  sector: string;
  direction?: string;
  reason?: string;
  confidence?: number;
  risk?: string;
}

interface SectorRotationItem {
  sector: string;
  stage?: string;
  logic?: string;
  time_window?: string;
  advice?: string;
}

interface SectorHeatItem {
  sector: string;
  score?: number;
  trend?: string;
  sustainability?: string;
}

interface SectorPredictions {
  prediction_text?: string;
  long_short?: {
    bullish?: SectorLongShortItem[];
    bearish?: SectorLongShortItem[];
    neutral?: SectorLongShortItem[];
  };
  rotation?: {
    current_strong?: SectorRotationItem[];
    potential?: SectorRotationItem[];
    declining?: SectorRotationItem[];
  };
  heat?: {
    hottest?: SectorHeatItem[];
    heating?: SectorHeatItem[];
    cooling?: SectorHeatItem[];
  };
  summary?: {
    market_view?: string;
    key_opportunity?: string;
    major_risk?: string;
    strategy?: string;
  };
  confidence_score?: number;
  risk_level?: string;
  investment_horizon?: string;
  market_outlook?: string;
}

interface AgentsAnalysisMap {
  [key: string]: {
    agent_name?: string;
    agent_role?: string;
    focus_areas?: string[];
    analysis?: string;
    timestamp?: string;
  };
}

interface SavedReportSummary {
  id?: number;
  created_at?: string;
  data_date_range?: string;
  summary?: string;
  confidence_score?: number;
  risk_level?: string;
  market_outlook?: string;
}

interface DataSummary {
  market_overview?: any;
  sector_count?: number;
  concept_count?: number;
}

interface SectorStrategyResult {
  success: boolean;
  error?: string | null;
  timestamp?: string;
  final_predictions?: SectorPredictions;
  agents_analysis?: AgentsAnalysisMap;
  comprehensive_report?: string;
  cache_meta?: CacheMeta;
  saved_report?: SavedReportSummary;
  report_id?: number;
  data_summary?: DataSummary;
}

interface SectorStrategyHistoryItem {
  id: number;
  created_at: string;
  data_date_range?: string | null;
  summary?: string | null;
  confidence_score?: number | null;
  risk_level?: string | null;
  market_outlook?: string | null;
}

interface SectorStrategyHistoryListResponse {
  items: SectorStrategyHistoryItem[];
}

function formatPercent(value: number | undefined | null, digits = 2): string {
  if (value == null || Number.isNaN(value)) return "-";
  return `${value.toFixed(digits)}%`;
}

export default function SectorStrategyPage() {
  const [activeTab, setActiveTab] = useState<ActiveTab>("analysis");
  const [model, setModel] = useState<ModelKey>("deepseek-chat");

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<SectorStrategyResult | null>(null);
  const [fromHistory, setFromHistory] = useState(false);

  const [historyItems, setHistoryItems] = useState<SectorStrategyHistoryItem[]>(
    [],
  );
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [historyDeletingId, setHistoryDeletingId] = useState<number | null>(
    null,
  );

  async function handleAnalyze() {
    setLoading(true);
    setError(null);
    setResult(null);
    setFromHistory(false);

    try {
      const res = await fetch(`${API_BASE}/sector-strategy/analyze`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ model }),
      });
      if (!res.ok) {
        throw new Error(`请求失败: ${res.status}`);
      }
      const data = (await res.json()) as SectorStrategyResult;
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

  async function loadHistory() {
    setHistoryLoading(true);
    setHistoryError(null);
    try {
      const res = await fetch(`${API_BASE}/sector-strategy/history?limit=20`);
      if (!res.ok) {
        throw new Error(`历史报告请求失败: ${res.status}`);
      }
      const data: SectorStrategyHistoryListResponse = await res.json();
      setHistoryItems(data.items || []);
    } catch (e: any) {
      setHistoryError(e?.message || "加载历史报告失败");
      setHistoryItems([]);
    } finally {
      setHistoryLoading(false);
    }
  }

  useEffect(() => {
    if (activeTab === "history") {
      void loadHistory();
    }
  }, [activeTab]);

  async function handleLoadHistoryToView(id: number) {
    setHistoryError(null);
    try {
      const res = await fetch(`${API_BASE}/sector-strategy/history/${id}`);
      if (!res.ok) {
        throw new Error(`获取报告详情失败: ${res.status}`);
      }
      const data = await res.json();
      const report = data.report as any;
      const parsed =
        (report?.analysis_content_parsed as SectorStrategyResult | undefined) ||
        null;
      if (!parsed) {
        throw new Error("报告内容缺失或格式不正确");
      }
      setResult(parsed);
      setFromHistory(true);
      setActiveTab("analysis");
    } catch (e: any) {
      setHistoryError(e?.message || "加载报告详情失败");
    }
  }

  async function handleDeleteHistory(id: number) {
    if (typeof window !== "undefined") {
      const ok = window.confirm("确认删除该历史报告？此操作不可恢复。");
      if (!ok) return;
    }
    setHistoryDeletingId(id);
    try {
      const res = await fetch(`${API_BASE}/sector-strategy/history/${id}`, {
        method: "DELETE",
      });
      if (!res.ok) {
        let msg = "删除失败";
        try {
          const data = await res.json();
          if (data?.detail) msg = String(data.detail);
        } catch {
          // ignore
        }
        throw new Error(msg);
      }
      await loadHistory();
    } catch (e: any) {
      setHistoryError(e?.message || "删除历史报告失败");
    } finally {
      setHistoryDeletingId(null);
    }
  }

  const predictions: SectorPredictions | undefined =
    result?.final_predictions || undefined;
  const bullish = predictions?.long_short?.bullish ?? [];
  const bearish = predictions?.long_short?.bearish ?? [];
  const rotation = predictions?.rotation;
  const heat = predictions?.heat;
  const summary = predictions?.summary;

  const marketOverview = result?.data_summary?.market_overview ?? {};
  const shIndex = marketOverview?.sh_index;
  const szIndex = marketOverview?.sz_index;
  const cybIndex = marketOverview?.cyb_index;

  const hasResult = !!result && result.success;

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
          🎯 智策板块策略分析
        </h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          复用旧版 SectorStrategy 数据采集与多智能体引擎：宏观·板块·资金·情绪四位分析师协同给出板块多空、轮动和热度预测。
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
          📊 智策分析
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
          📋 历史报告
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
                <option value="deepseek-chat">DeepSeek Chat (默认)</option>
                <option value="deepseek-reasoner">
                  DeepSeek Reasoner (推理增强)
                </option>
              </select>
              <p style={{ marginTop: 6, fontSize: 12, color: "#6b7280" }}>
                建议使用 DeepSeek Chat 或 Reasoner，保持与旧版智策分析结果口径一致。
              </p>
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
                {loading ? "分析中..." : "🚀 开始智策分析"}
              </button>
              <button
                type="button"
                onClick={() => {
                  setResult(null);
                  setError(null);
                  setFromHistory(false);
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

          {result?.cache_meta &&
            (result.cache_meta.from_cache || result.cache_meta.cache_warning) && (
              <p style={{ marginTop: 8, color: "#92400e", fontSize: 12 }}>
                ⚠️ {result.cache_meta.cache_warning || "当前分析基于缓存数据，可能不是最新信息"}
              </p>
            )}
        </section>
      )}

      {activeTab === "analysis" && hasResult && (
        <>
          {result?.data_summary && (
            <section
              style={{
                background: "#fff",
                borderRadius: 12,
                padding: 16,
                boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
                marginBottom: 16,
              }}
            >
              <h2 style={{ marginTop: 0, fontSize: 18 }}>📊 市场数据概览</h2>
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
                  gap: 12,
                  fontSize: 13,
                }}
              >
                {shIndex && (
                  <div className="metric-card">
                    <div style={{ fontWeight: 600 }}>上证指数</div>
                    <div style={{ marginTop: 4, fontSize: 16 }}>
                      {shIndex.close?.toFixed?.(2) ?? "-"}
                    </div>
                    <div
                      style={{
                        marginTop: 2,
                        color:
                          (shIndex.change_pct ?? 0) > 0
                            ? "#e53935"
                            : (shIndex.change_pct ?? 0) < 0
                              ? "#1e88e5"
                              : "#374151",
                      }}
                    >
                      {formatPercent(shIndex.change_pct)}
                    </div>
                  </div>
                )}
                {szIndex && (
                  <div className="metric-card">
                    <div style={{ fontWeight: 600 }}>深证成指</div>
                    <div style={{ marginTop: 4, fontSize: 16 }}>
                      {szIndex.close?.toFixed?.(2) ?? "-"}
                    </div>
                    <div
                      style={{
                        marginTop: 2,
                        color:
                          (szIndex.change_pct ?? 0) > 0
                            ? "#e53935"
                            : (szIndex.change_pct ?? 0) < 0
                              ? "#1e88e5"
                              : "#374151",
                      }}
                    >
                      {formatPercent(szIndex.change_pct)}
                    </div>
                  </div>
                )}
                {cybIndex && (
                  <div className="metric-card">
                    <div style={{ fontWeight: 600 }}>创业板指</div>
                    <div style={{ marginTop: 4, fontSize: 16 }}>
                      {cybIndex.close?.toFixed?.(2) ?? "-"}
                    </div>
                    <div
                      style={{
                        marginTop: 2,
                        color:
                          (cybIndex.change_pct ?? 0) > 0
                            ? "#e53935"
                            : (cybIndex.change_pct ?? 0) < 0
                              ? "#1e88e5"
                              : "#374151",
                      }}
                    >
                      {formatPercent(cybIndex.change_pct)}
                    </div>
                  </div>
                )}
                <div className="metric-card">
                  <div style={{ fontWeight: 600 }}>行业板块数量</div>
                  <div style={{ marginTop: 4, fontSize: 16 }}>
                    {result.data_summary.sector_count ?? "-"}
                  </div>
                </div>
                <div className="metric-card">
                  <div style={{ fontWeight: 600 }}>概念板块数量</div>
                  <div style={{ marginTop: 4, fontSize: 16 }}>
                    {result.data_summary.concept_count ?? "-"}
                  </div>
                </div>
                {typeof marketOverview.total_stocks === "number" && (
                  <div className="metric-card">
                    <div style={{ fontWeight: 600 }}>上涨家数占比</div>
                    <div style={{ marginTop: 4, fontSize: 16 }}>
                      {marketOverview.up_count ?? "-"}/{
                        marketOverview.total_stocks ?? "-"
                      }
                    </div>
                    <div style={{ marginTop: 2, color: "#10b981" }}>
                      {formatPercent(marketOverview.up_ratio)}
                    </div>
                  </div>
                )}
              </div>
            </section>
          )}

          <section
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 16,
              boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
              marginBottom: 16,
            }}
          >
            <h2 style={{ marginTop: 0, fontSize: 18 }}>📝 报告摘要</h2>
            {fromHistory && (
              <p style={{ fontSize: 12, color: "#6b7280" }}>
                当前展示为历史报告内容，可再次点击“开始智策分析”获取最新市场数据分析。
              </p>
            )}
            {result?.saved_report ? (
              <>
                <p style={{ fontSize: 13, color: "#374151" }}>
                  {result.saved_report.summary || "智策板块分析报告"}
                </p>
                <p style={{ fontSize: 12, color: "#6b7280" }}>
                  生成时间: {result.saved_report.created_at || "-"} ｜ 数据区间:{" "}
                  {result.saved_report.data_date_range || "-"}
                </p>
              </>
            ) : (
              <p style={{ fontSize: 13, color: "#374151" }}>
                智策板块分析报告，包含板块多空、轮动、热度和整体策略判断。
              </p>
            )}
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
            <h2 style={{ marginTop: 0, fontSize: 18 }}>🎯 核心预测</h2>
            {predictions?.prediction_text ? (
              <p style={{ whiteSpace: "pre-wrap", fontSize: 13 }}>
                {predictions.prediction_text}
              </p>
            ) : (
              <>
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
                    gap: 16,
                    marginBottom: 16,
                    fontSize: 13,
                  }}
                >
                  <div>
                    <h3 style={{ marginTop: 0, fontSize: 15 }}>🟢 看多板块</h3>
                    {bullish.length ? (
                      bullish.map((item, idx) => (
                        <div
                          key={`bull-${idx}`}
                          style={{
                            marginBottom: 8,
                            padding: 8,
                            borderRadius: 8,
                            background: "#ecfdf3",
                          }}
                        >
                          <div style={{ fontWeight: 600 }}>
                            {idx + 1}. {item.sector} ({
                              item.confidence ?? 0
                            }
                            /10)
                          </div>
                          <div>理由：{item.reason || "-"}</div>
                          <div>风险：{item.risk || "-"}</div>
                        </div>
                      ))
                    ) : (
                      <p>暂无看多板块</p>
                    )}
                  </div>
                  <div>
                    <h3 style={{ marginTop: 0, fontSize: 15 }}>🔴 看空板块</h3>
                    {bearish.length ? (
                      bearish.map((item, idx) => (
                        <div
                          key={`bear-${idx}`}
                          style={{
                            marginBottom: 8,
                            padding: 8,
                            borderRadius: 8,
                            background: "#fef2f2",
                          }}
                        >
                          <div style={{ fontWeight: 600 }}>
                            {idx + 1}. {item.sector} ({
                              item.confidence ?? 0
                            }
                            /10)
                          </div>
                          <div>理由：{item.reason || "-"}</div>
                          <div>风险：{item.risk || "-"}</div>
                        </div>
                      ))
                    ) : (
                      <p>暂无看空板块</p>
                    )}
                  </div>
                </div>

                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
                    gap: 16,
                    marginBottom: 16,
                    fontSize: 13,
                  }}
                >
                  <div>
                    <h3 style={{ marginTop: 0, fontSize: 15 }}>💪 当前强势</h3>
                    {rotation?.current_strong?.length ? (
                      rotation.current_strong.map((item, idx) => (
                        <div key={`cs-${idx}`} style={{ marginBottom: 8 }}>
                          <div style={{ fontWeight: 600 }}>{item.sector}</div>
                          <div>时间窗口：{item.time_window || "-"}</div>
                          <div>逻辑：{item.logic || "-"}</div>
                          <div>建议：{item.advice || "-"}</div>
                        </div>
                      ))
                    ) : (
                      <p>暂无数据</p>
                    )}
                  </div>
                  <div>
                    <h3 style={{ marginTop: 0, fontSize: 15 }}>🌱 潜力接力</h3>
                    {rotation?.potential?.length ? (
                      rotation.potential.map((item, idx) => (
                        <div key={`pt-${idx}`} style={{ marginBottom: 8 }}>
                          <div style={{ fontWeight: 600 }}>{item.sector}</div>
                          <div>时间窗口：{item.time_window || "-"}</div>
                          <div>逻辑：{item.logic || "-"}</div>
                          <div>建议：{item.advice || "-"}</div>
                        </div>
                      ))
                    ) : (
                      <p>暂无数据</p>
                    )}
                  </div>
                  <div>
                    <h3 style={{ marginTop: 0, fontSize: 15 }}>📉 衰退板块</h3>
                    {rotation?.declining?.length ? (
                      rotation.declining.map((item, idx) => (
                        <div key={`dc-${idx}`} style={{ marginBottom: 8 }}>
                          <div style={{ fontWeight: 600 }}>{item.sector}</div>
                          <div>时间窗口：{item.time_window || "-"}</div>
                          <div>逻辑：{item.logic || "-"}</div>
                          <div>建议：{item.advice || "-"}</div>
                        </div>
                      ))
                    ) : (
                      <p>暂无数据</p>
                    )}
                  </div>
                </div>

                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
                    gap: 16,
                    marginBottom: 16,
                    fontSize: 13,
                  }}
                >
                  <div>
                    <h3 style={{ marginTop: 0, fontSize: 15 }}>🔥 最热板块</h3>
                    {heat?.hottest?.length ? (
                      heat.hottest.slice(0, 5).map((item, idx) => (
                        <div key={`hot-${idx}`} style={{ marginBottom: 8 }}>
                          <div style={{ fontWeight: 600 }}>
                            {idx + 1}. {item.sector}
                          </div>
                          <div>热度：{item.score ?? 0}</div>
                          <div>趋势：{item.trend || "-"}</div>
                          <div>持续性：{item.sustainability || "-"}</div>
                        </div>
                      ))
                    ) : (
                      <p>暂无数据</p>
                    )}
                  </div>
                  <div>
                    <h3 style={{ marginTop: 0, fontSize: 15 }}>📈 升温板块</h3>
                    {heat?.heating?.length ? (
                      heat.heating.slice(0, 5).map((item, idx) => (
                        <div key={`heat-${idx}`} style={{ marginBottom: 8 }}>
                          <div style={{ fontWeight: 600 }}>{item.sector}</div>
                          <div>热度：{item.score ?? 0}</div>
                        </div>
                      ))
                    ) : (
                      <p>暂无数据</p>
                    )}
                  </div>
                  <div>
                    <h3 style={{ marginTop: 0, fontSize: 15 }}>📉 降温板块</h3>
                    {heat?.cooling?.length ? (
                      heat.cooling.slice(0, 5).map((item, idx) => (
                        <div key={`cool-${idx}`} style={{ marginBottom: 8 }}>
                          <div style={{ fontWeight: 600 }}>{item.sector}</div>
                          <div>热度：{item.score ?? 0}</div>
                        </div>
                      ))
                    ) : (
                      <p>暂无数据</p>
                    )}
                  </div>
                </div>

                {summary && (
                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
                      gap: 16,
                      fontSize: 13,
                    }}
                  >
                    <div>
                      <h3 style={{ marginTop: 0, fontSize: 15 }}>💡 市场观点</h3>
                      <p>{summary.market_view || "-"}</p>
                      <h3 style={{ marginTop: 8, fontSize: 15 }}>🎯 核心机会</h3>
                      <p>{summary.key_opportunity || "-"}</p>
                    </div>
                    <div>
                      <h3 style={{ marginTop: 0, fontSize: 15 }}>⚠️ 主要风险</h3>
                      <p>{summary.major_risk || "-"}</p>
                      <h3 style={{ marginTop: 8, fontSize: 15 }}>📋 整体策略</h3>
                      <p>{summary.strategy || "-"}</p>
                    </div>
                  </div>
                )}
              </>
            )}
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
            <h2 style={{ marginTop: 0, fontSize: 18 }}>🤖 AI 智能体分析报告</h2>
            {!result?.agents_analysis ||
            Object.keys(result.agents_analysis).length === 0 ? (
              <p style={{ fontSize: 13 }}>暂无智能体分析数据。</p>
            ) : (
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "repeat(2, minmax(0, 1fr))",
                  gap: 16,
                }}
              >
                {Object.entries(result.agents_analysis).map(
                  ([key, agent]) => (
                    <div
                      key={key}
                      style={{
                        padding: 12,
                        borderRadius: 12,
                        background: "#f9fafb",
                        fontSize: 13,
                      }}
                    >
                      <div style={{ fontWeight: 600, marginBottom: 4 }}>
                        {agent.agent_name || "未知分析师"}
                      </div>
                      <div style={{ marginBottom: 4 }}>
                        职责：{agent.agent_role || "-"}
                      </div>
                      {agent.focus_areas && agent.focus_areas.length > 0 && (
                        <div style={{ marginBottom: 4 }}>
                          关注领域：{agent.focus_areas.join(", ")}
                        </div>
                      )}
                      <div style={{ marginBottom: 4 }}>
                        分析时间：{agent.timestamp || "-"}
                      </div>
                      <div
                        style={{
                          marginTop: 8,
                          paddingTop: 8,
                          borderTop: "1px solid #e5e7eb",
                          whiteSpace: "pre-wrap",
                        }}
                      >
                        {agent.analysis || "暂无分析"}
                      </div>
                    </div>
                  ),
                )}
              </div>
            )}
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
            <h2 style={{ marginTop: 0, fontSize: 18 }}>📊 综合研判</h2>
            {result?.comprehensive_report ? (
              <p style={{ whiteSpace: "pre-wrap", fontSize: 13 }}>
                {result.comprehensive_report}
              </p>
            ) : (
              <p style={{ fontSize: 13 }}>暂无综合研判数据。</p>
            )}
          </section>

          <section
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 16,
              boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
            }}
          >
            <details>
              <summary
                style={{
                  cursor: "pointer",
                  fontWeight: 600,
                  fontSize: 13,
                }}
              >
                🔍 查看原始 JSON 结构
              </summary>
              <pre
                style={{
                  marginTop: 8,
                  maxHeight: 480,
                  overflow: "auto",
                  fontSize: 12,
                  background: "#f9fafb",
                  padding: 12,
                  borderRadius: 8,
                }}
              >
{JSON.stringify(result, null, 2)}
              </pre>
            </details>
          </section>

          <section
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 16,
              boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
              marginTop: 16,
            }}
          >
            <h2 style={{ marginTop: 0, fontSize: 18 }}>📄 导出报告</h2>
            <p style={{ fontSize: 13, color: "#374151" }}>
              将当前智策分析结果导出为 PDF 或 Markdown 文件，便于保存与分享。
            </p>
            <div
              style={{
                display: "flex",
                gap: 8,
                marginTop: 8,
                flexWrap: "wrap",
              }}
            >
              <button
                type="button"
                disabled={!hasResult}
                onClick={async () => {
                  if (!result || !result.success) return;
                  try {
                    const res = await fetch(
                      `${API_BASE}/sector-strategy/export/pdf`,
                      {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ result }),
                      },
                    );
                    if (!res.ok) {
                      throw new Error(`PDF 导出失败: ${res.status}`);
                    }
                    const blob = await res.blob();
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement("a");
                    a.href = url;
                    const ts =
                      result.timestamp?.replace(/[: ]/g, "_") || "report";
                    a.download = `sector_strategy_${ts}.pdf`;
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                    URL.revokeObjectURL(url);
                  } catch (e) {
                    console.error(e);
                  }
                }}
                style={{
                  padding: "8px 16px",
                  borderRadius: 8,
                  border: "none",
                  background: hasResult ? "#4f46e5" : "#e5e7eb",
                  color: hasResult ? "#fff" : "#9ca3af",
                  fontWeight: 600,
                  cursor: hasResult ? "pointer" : "default",
                }}
              >
                📄 导出 PDF
              </button>
              <button
                type="button"
                disabled={!hasResult}
                onClick={async () => {
                  if (!result || !result.success) return;
                  try {
                    const res = await fetch(
                      `${API_BASE}/sector-strategy/export/markdown`,
                      {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ result }),
                      },
                    );
                    if (!res.ok) {
                      throw new Error(`Markdown 导出失败: ${res.status}`);
                    }
                    const blob = await res.blob();
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement("a");
                    a.href = url;
                    const ts =
                      result.timestamp?.replace(/[: ]/g, "_") || "report";
                    a.download = `sector_strategy_${ts}.md`;
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                    URL.revokeObjectURL(url);
                  } catch (e) {
                    console.error(e);
                  }
                }}
                style={{
                  padding: "8px 16px",
                  borderRadius: 8,
                  border: "1px solid #e5e7eb",
                  background: "#fff",
                  color: hasResult ? "#111827" : "#9ca3af",
                  fontWeight: 500,
                  cursor: hasResult ? "pointer" : "default",
                }}
              >
                📝 导出 Markdown
              </button>
            </div>
          </section>
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
          <h2 style={{ marginTop: 0, fontSize: 18 }}>📋 智策历史报告</h2>
          <p style={{ fontSize: 13, color: "#374151" }}>
            查看和管理历史智策分析报告，可一键加载到分析视图或删除。
          </p>
          {historyError && (
            <p style={{ marginTop: 8, color: "#b91c1c", fontSize: 13 }}>
              错误：{historyError}
            </p>
          )}
          {historyLoading ? (
            <p style={{ marginTop: 8, fontSize: 13 }}>加载中...</p>
          ) : historyItems.length === 0 ? (
            <p style={{ marginTop: 8, fontSize: 13 }}>暂无历史报告。</p>
          ) : (
            <div style={{ marginTop: 8 }}>
              {historyItems.map((item) => (
                <div
                  key={item.id}
                  style={{
                    padding: 12,
                    borderRadius: 12,
                    border: "1px solid #e5e7eb",
                    marginBottom: 12,
                    fontSize: 13,
                  }}
                >
                  <div style={{ fontWeight: 600 }}>
                    📊 报告 #{item.id}
                  </div>
                  <div
                    style={{ marginTop: 2, color: "#6b7280", fontSize: 12 }}
                  >
                    生成时间: {item.created_at} ｜ 数据区间:{" "}
                    {item.data_date_range || "-"}
                  </div>
                  <div
                    style={{
                      display: "grid",
                      gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
                      gap: 8,
                      marginTop: 8,
                    }}
                  >
                    <div>
                      <div style={{ fontSize: 12, color: "#6b7280" }}>
                        置信度
                      </div>
                      <div style={{ fontSize: 14 }}>
                        {item.confidence_score != null
                          ? formatPercent(item.confidence_score, 1)
                          : "-"}
                      </div>
                    </div>
                    <div>
                      <div style={{ fontSize: 12, color: "#6b7280" }}>
                        风险等级
                      </div>
                      <div style={{ fontSize: 14 }}>
                        {item.risk_level || "-"}
                      </div>
                    </div>
                    <div>
                      <div style={{ fontSize: 12, color: "#6b7280" }}>
                        市场展望
                      </div>
                      <div style={{ fontSize: 14 }}>
                        {item.market_outlook || "-"}
                      </div>
                    </div>
                  </div>
                  <div
                    style={{
                      display: "flex",
                      gap: 8,
                      marginTop: 8,
                      justifyContent: "flex-end",
                    }}
                  >
                    <button
                      type="button"
                      onClick={() => handleLoadHistoryToView(item.id)}
                      style={{
                        padding: "6px 12px",
                        borderRadius: 8,
                        border: "1px solid #e5e7eb",
                        background: "#fff",
                        cursor: "pointer",
                      }}
                    >
                      📥 加载到分析视图
                    </button>
                    <button
                      type="button"
                      onClick={() => handleDeleteHistory(item.id)}
                      disabled={historyDeletingId === item.id}
                      style={{
                        padding: "6px 12px",
                        borderRadius: 8,
                        border: "1px solid #fee2e2",
                        background: "#fef2f2",
                        color: "#b91c1c",
                        cursor:
                          historyDeletingId === item.id ? "default" : "pointer",
                      }}
                    >
                      {historyDeletingId === item.id ? "删除中..." : "🗑️ 删除"}
                    </button>
                  </div>
                  {item.summary && (
                    <div
                      style={{
                        marginTop: 8,
                        fontSize: 12,
                        color: "#4b5563",
                      }}
                    >
                      摘要：{item.summary}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </section>
      )}
    </main>
  );
}
