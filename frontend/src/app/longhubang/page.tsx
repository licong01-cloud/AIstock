"use client";

import React from "react";
import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type AnalysisMode = "date" | "recent_days";

type LonghubangTab = "analysis" | "history" | "stats";

interface LonghubangSummary {
  total_records?: number;
  total_stocks?: number;
  total_youzi?: number;
  total_buy_amount?: number;
  total_sell_amount?: number;
  total_net_inflow?: number;
}

interface LonghubangDataInfo {
  total_records?: number;
  total_stocks?: number;
  total_youzi?: number;
  summary?: LonghubangSummary & {
    top_youzi?: Record<string, number>;
    top_stocks?: {
      code: string;
      name: string;
      net_inflow: number;
    }[];
    hot_concepts?: Record<string, number>;
  };
}

interface LonghubangAgentAnalysisItem {
  agent_name?: string;
  agent_role?: string;
  analysis?: string;
  focus_areas?: string[];
  timestamp?: string;
}

interface LonghubangAgentsAnalysis {
  [key: string]: LonghubangAgentAnalysisItem;
}

interface LonghubangRecommendedStock {
  rank?: number;
  code?: string;
  name?: string;
  net_inflow?: number;
  reason?: string;
  confidence?: string;
  hold_period?: string;
  buy_price?: string;
  target_price?: string;
  stop_loss?: string;
}

interface LonghubangScoringRow {
  排名?: number;
  股票名称?: string;
  股票代码?: string;
  综合评分?: number;
  资金含金量?: number;
  净买入额?: number;
  卖出压力?: number;
  机构共振?: number;
  加分项?: number;
  顶级游资?: number;
  买方数?: number;
  机构参与?: string;
  净流入?: number;
}

interface LonghubangFinalReport {
  title?: string;
  timestamp?: string;
  summary?: string;
  data_overview?: {
    total_records?: number;
    total_stocks?: number;
    total_youzi?: number;
    total_net_inflow?: number;
  };
  recommended_stocks_count?: number;
  agents_count?: number;
}

interface LonghubangAnalyzeResponse {
  success: boolean;
  error?: string | null;
  timestamp?: string | null;
  data_info?: LonghubangDataInfo | null;
  agents_analysis?: LonghubangAgentsAnalysis | null;
  final_report?: LonghubangFinalReport | null;
  scoring_ranking?: LonghubangScoringRow[] | null;
  recommended_stocks?: LonghubangRecommendedStock[] | null;
  report_id?: number | null;
}

interface HistoryItem {
  id: number;
  analysis_date: string;
  data_date_range?: string | null;
  summary?: string | null;
  created_at?: string | null;
}

interface HistoryListResponse {
  items: HistoryItem[];
}

interface HistoryDetailResponse {
  report: any;
}

interface LonghubangStatsResponse {
  stats: {
    total_records?: number;
    total_stocks?: number;
    total_youzi?: number;
    total_reports?: number;
    date_range?: {
      start?: string | null;
      end?: string | null;
    };
    [key: string]: any;
  };
}

function formatNumber(value?: number | null, digits = 0): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return value.toLocaleString("zh-CN", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatDateTime(value?: string | null): string {
  if (!value) return "-";
  let s = String(value).trim();
  if (!s) return "-";
  s = s.replace("T", " ");
  // 去掉毫秒部分
  const dotIndex = s.indexOf(".");
  if (dotIndex >= 0) {
    s = s.slice(0, dotIndex);
  }
  // 去掉尾部时区偏移，如 +08:00 / -08:00 / Z
  s = s.replace(/Z$/, "");
  s = s.replace(/[+-]\d{2}:?\d{2}$/, "");
  s = s.trim();
  if (s.length >= 19) {
    return s.slice(0, 19); // YYYY-MM-DD HH:MM:SS
  }
  if (s.length >= 10) {
    return s.slice(0, 10); // YYYY-MM-DD
  }
  return s;
}

function formatDate(value?: string | null): string {
  const dt = formatDateTime(value);
  if (dt === "-") return "-";
  if (dt.length >= 10) return dt.slice(0, 10);
  return dt;
}

export default function LonghubangPage() {
  const [activeTab, setActiveTab] = useState<LonghubangTab>("analysis");

  const [analysisMode, setAnalysisMode] = useState<AnalysisMode>("recent_days");
  const [selectedDate, setSelectedDate] = useState<string>("");
  const [recentDays, setRecentDays] = useState<number>(1);
  const [model, setModel] = useState<string>("deepseek-chat");

  const [analyzing, setAnalyzing] = useState(false);
  const [result, setResult] = useState<LonghubangAnalyzeResponse | null>(null);
  const [analysisError, setAnalysisError] = useState<string | null>(null);

  const [historyLoading, setHistoryLoading] = useState(false);
  const [history, setHistory] = useState<HistoryItem[]>([]);
  const [historyError, setHistoryError] = useState<string | null>(null);

  const [detailLoading, setDetailLoading] = useState(false);
  const [selectedReport, setSelectedReport] = useState<HistoryDetailResponse | null>(
    null,
  );

  const [statsLoading, setStatsLoading] = useState(false);
  const [stats, setStats] = useState<LonghubangStatsResponse | null>(null);
  const [statsError, setStatsError] = useState<string | null>(null);

  useEffect(() => {
    if (!selectedDate) {
      const d = new Date();
      d.setDate(d.getDate() - 1);
      const yyyy = d.getFullYear();
      const mm = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      setSelectedDate(`${yyyy}-${mm}-${dd}`);
    }
  }, [selectedDate]);

  useEffect(() => {
    if (activeTab === "history" && history.length === 0 && !historyLoading) {
      void fetchHistory();
    }
    if (activeTab === "stats" && !stats && !statsLoading) {
      void fetchStats();
    }
  }, [activeTab]);

  const hasResult = !!result && result.success;

  async function runAnalysis() {
    setAnalyzing(true);
    setAnalysisError(null);
    try {
      const payload: any = {
        mode: analysisMode,
        model,
      };
      if (analysisMode === "date") {
        payload.date = selectedDate;
      } else {
        payload.days = recentDays;
      }

      const res = await fetch(`${API_BASE}/longhubang/analyze`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`分析请求失败: ${res.status} ${text}`);
      }
      const data = (await res.json()) as LonghubangAnalyzeResponse;
      setResult(data);
    } catch (e: any) {
      console.error(e);
      setAnalysisError(e?.message || String(e));
    } finally {
      setAnalyzing(false);
    }
  }

  async function fetchHistory() {
    setHistoryLoading(true);
    setHistoryError(null);
    try {
      const res = await fetch(`${API_BASE}/longhubang/history?limit=50`);
      if (!res.ok) {
        throw new Error(`历史列表获取失败: ${res.status}`);
      }
      const data = (await res.json()) as HistoryListResponse;
      setHistory(data.items || []);
    } catch (e: any) {
      console.error(e);
      setHistoryError(e?.message || String(e));
    } finally {
      setHistoryLoading(false);
    }
  }

  async function fetchHistoryDetail(id: number) {
    setDetailLoading(true);
    try {
      const res = await fetch(`${API_BASE}/longhubang/history/${id}`);
      if (!res.ok) {
        throw new Error(`历史报告加载失败: ${res.status}`);
      }
      const data = (await res.json()) as HistoryDetailResponse;
      setSelectedReport(data);
    } catch (e) {
      console.error(e);
    } finally {
      setDetailLoading(false);
    }
  }

  async function deleteHistoryItem(id: number) {
    if (!window.confirm(`确认删除报告 #${id}？此操作不可撤销`)) return;
    try {
      const res = await fetch(`${API_BASE}/longhubang/history/${id}`, {
        method: "DELETE",
      });
      if (!res.ok) {
        throw new Error(`删除失败: ${res.status}`);
      }
      setHistory((prev) => prev.filter((h) => h.id !== id));
      if (selectedReport?.report?.id === id) {
        setSelectedReport(null);
      }
    } catch (e) {
      console.error(e);
      alert("删除失败，请查看控制台日志");
    }
  }

  async function fetchStats() {
    setStatsLoading(true);
    setStatsError(null);
    try {
      const res = await fetch(`${API_BASE}/longhubang/stats`);
      if (!res.ok) {
        throw new Error(`统计数据获取失败: ${res.status}`);
      }
      const data = (await res.json()) as LonghubangStatsResponse;
      setStats(data);
    } catch (e: any) {
      console.error(e);
      setStatsError(e?.message || String(e));
    } finally {
      setStatsLoading(false);
    }
  }

  function buildResultFromHistoryReport(report: any): LonghubangAnalyzeResponse {
    const parsed = (report?.analysis_content_parsed || {}) as any;

    return {
      success: true,
      error: null,
      timestamp:
        (parsed.timestamp as string | undefined) ||
        (report?.analysis_date as string | undefined) ||
        null,
      data_info: (parsed.data_info as LonghubangDataInfo | undefined) || {},
      agents_analysis:
        (parsed.agents_analysis as LonghubangAgentsAnalysis | undefined) || {},
      final_report:
        (parsed.final_report as LonghubangFinalReport | undefined) || {
          summary: (report?.summary as string | undefined) || "",
        },
      scoring_ranking:
        (parsed.scoring_ranking as LonghubangScoringRow[] | undefined) || [],
      recommended_stocks:
        (report?.recommended_stocks as LonghubangRecommendedStock[] | undefined) ||
        [],
      report_id: (report?.id as number | undefined) ?? null,
    };
  }

  function renderAnalysisResult(r: LonghubangAnalyzeResponse) {
    const info = r.data_info || {};
    const summary = (info.summary || {}) as LonghubangSummary & {
      hot_concepts?: Record<string, number>;
    };
    const recommended = r.recommended_stocks || [];
    const scoring = r.scoring_ranking || [];

    return (
      <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        <section
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 16,
            boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
          }}
        >
          <h2 style={{ marginTop: 0, fontSize: 18 }}>📊 数据概况</h2>
          <p style={{ fontSize: 13, color: "#4b5563" }}>
            基于 StockAPI 龙虎榜数据的多维度统计，用于支持后续 AI 分析。
          </p>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(120px, 1fr))",
              gap: 12,
              marginTop: 12,
            }}
          >
            <MetricCard
              label="龙虎榜记录"
              value={formatNumber(info.total_records ?? summary.total_records, 0)}
            />
            <MetricCard
              label="涉及股票"
              value={formatNumber(info.total_stocks ?? summary.total_stocks, 0)}
            />
            <MetricCard
              label="涉及游资"
              value={formatNumber(info.total_youzi ?? summary.total_youzi, 0)}
            />
            <MetricCard
              label="净流入金额"
              value={`${formatNumber(summary.total_net_inflow, 0)} 元`}
            />
          </div>
        </section>

        {recommended.length > 0 && (
          <section
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 16,
              boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
            }}
          >
            <h2 style={{ marginTop: 0, fontSize: 18 }}>🎯 AI 推荐股票</h2>
            <p style={{ fontSize: 13, color: "#4b5563" }}>
              基于 5 位 AI 分析师的综合分析，系统筛选出的潜力股票清单。
            </p>
            <div style={{ overflowX: "auto", marginTop: 8 }}>
              <table
                style={{
                  width: "100%",
                  borderCollapse: "collapse",
                  fontSize: 13,
                }}
              >
                <thead>
                  <tr style={{ background: "#f3f4f6" }}>
                    <th style={thStyle}>排名</th>
                    <th style={thStyle}>代码</th>
                    <th style={thStyle}>名称</th>
                    <th style={thStyle}>净流入</th>
                    <th style={thStyle}>确定性</th>
                    <th style={thStyle}>持有周期</th>
                    <th style={thStyle}>推荐理由</th>
                  </tr>
                </thead>
                <tbody>
                  {recommended.map((s, idx) => (
                    <tr
                      key={`${s.code}-${idx}`}
                      style={{
                        borderTop: "1px solid #e5e7eb",
                        background: idx % 2 === 0 ? "#ffffff" : "#f9fafb",
                      }}
                    >
                      <td style={tdStyle}>{s.rank ?? idx + 1}</td>
                      <td style={tdStyle}>{s.code}</td>
                      <td style={tdStyle}>{s.name}</td>
                      <td style={tdStyle}>
                        {s.net_inflow !== undefined
                          ? `${formatNumber(s.net_inflow, 0)} 元`
                          : "-"}
                      </td>
                      <td style={tdStyle}>{s.confidence || "-"}</td>
                      <td style={tdStyle}>{s.hold_period || "-"}</td>
                      <td style={{ ...tdStyle, maxWidth: 320 }}>
                        {s.reason || "-"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}

        {r.agents_analysis && Object.keys(r.agents_analysis).length > 0 && (
          <section
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 16,
              boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
            }}
          >
            <h2 style={{ marginTop: 0, fontSize: 18 }}>🤖 AI 分析师团队报告</h2>
            <p style={{ fontSize: 13, color: "#4b5563" }}>
              包含游资行为、个股潜力、题材追踪、风险控制、首席策略师等多维度解读。
            </p>
            <div style={{ marginTop: 8, display: "flex", flexDirection: "column", gap: 8 }}>
              {Object.entries(r.agents_analysis || {}).map(([key, value]) => {
                const v = value as LonghubangAgentAnalysisItem;
                return (
                  <details key={key} style={{ borderRadius: 8, border: "1px solid #e5e7eb", padding: 8 }}>
                    <summary
                      style={{
                        cursor: "pointer",
                        fontWeight: 600,
                        fontSize: 14,
                      }}
                    >
                      {v.agent_name || key}
                      {v.agent_role ? ` · ${v.agent_role}` : ""}
                    </summary>
                    <div style={{ marginTop: 8, fontSize: 13, whiteSpace: "pre-wrap" }}>
                      {v.analysis || "暂无分析"}
                    </div>
                    {v.timestamp && (
                      <div style={{ marginTop: 4, fontSize: 12, color: "#6b7280" }}>
                        分析时间：{formatDateTime(v.timestamp)}
                      </div>
                    )}
                  </details>
                );
              })}
            </div>
          </section>
        )}

        {scoring.length > 0 && (
          <section
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 16,
              boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
            }}
          >
            <h2 style={{ marginTop: 0, fontSize: 18 }}>🏆 AI 智能评分排名（TOP10）</h2>
            <p style={{ fontSize: 13, color: "#4b5563" }}>
              基于资金含金量、净买入额、卖出压力、机构共振、加分项等指标综合评分。
            </p>
            <div style={{ overflowX: "auto", marginTop: 8 }}>
              <table
                style={{
                  width: "100%",
                  borderCollapse: "collapse",
                  fontSize: 12,
                }}
              >
                <thead>
                  <tr style={{ background: "#f3f4f6" }}>
                    <th style={thStyle}>排名</th>
                    <th style={thStyle}>股票名称</th>
                    <th style={thStyle}>代码</th>
                    <th style={thStyle}>综合评分</th>
                    <th style={thStyle}>资金含金量</th>
                    <th style={thStyle}>净买入额评分</th>
                    <th style={thStyle}>卖出压力</th>
                    <th style={thStyle}>机构共振</th>
                    <th style={thStyle}>加分项</th>
                    <th style={thStyle}>顶级游资</th>
                    <th style={thStyle}>买方数</th>
                    <th style={thStyle}>机构参与</th>
                    <th style={thStyle}>净流入(元)</th>
                  </tr>
                </thead>
                <tbody>
                  {scoring.slice(0, 10).map((row, idx) => (
                    <tr
                      key={`${row.股票代码}-${idx}`}
                      style={{
                        borderTop: "1px solid #e5e7eb",
                        background: idx % 2 === 0 ? "#ffffff" : "#f9fafb",
                      }}
                    >
                      <td style={tdStyle}>{row.排名 ?? idx + 1}</td>
                      <td style={tdStyle}>{row.股票名称}</td>
                      <td style={tdStyle}>{row.股票代码}</td>
                      <td style={tdStyle}>{row.综合评分}</td>
                      <td style={tdStyle}>{row.资金含金量}</td>
                      <td style={tdStyle}>{row.净买入额}</td>
                      <td style={tdStyle}>{row.卖出压力}</td>
                      <td style={tdStyle}>{row.机构共振}</td>
                      <td style={tdStyle}>{row.加分项}</td>
                      <td style={tdStyle}>{row.顶级游资}</td>
                      <td style={tdStyle}>{row.买方数}</td>
                      <td style={tdStyle}>{row.机构参与}</td>
                      <td style={tdStyle}>{formatNumber(row.净流入, 2)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}

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
{JSON.stringify(r, null, 2)}
            </pre>
          </details>
        </section>

        {result && result.success && (
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
              将当前龙虎榜分析结果导出为 PDF 或 Markdown 文件，便于保存与分享。
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
                onClick={async () => {
                  if (!result || !result.success) return;
                  try {
                    const res = await fetch(
                      `${API_BASE}/longhubang/export/pdf`,
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
                    a.download = `longhubang_${ts}.pdf`;
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
                  background: "#4f46e5",
                  color: "#fff",
                  fontWeight: 600,
                  cursor: "pointer",
                }}
              >
                📄 导出 PDF
              </button>
              <button
                type="button"
                onClick={async () => {
                  if (!result || !result.success) return;
                  try {
                    const res = await fetch(
                      `${API_BASE}/longhubang/export/markdown`,
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
                    a.download = `longhubang_${ts}.md`;
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
                  color: "#111827",
                  fontWeight: 500,
                  cursor: "pointer",
                }}
              >
                📝 导出 Markdown
              </button>
            </div>
          </section>
        )}
      </div>
    );
  }

  function renderHistorySection() {
    return (
      <div style={{ display: "flex", gap: 16, alignItems: "flex-start" }}>
        <section
          style={{
            flex: 1,
            background: "#fff",
            borderRadius: 12,
            padding: 16,
            boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
          }}
        >
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              marginBottom: 8,
            }}
          >
            <h2 style={{ margin: 0, fontSize: 18 }}>📚 历史分析报告</h2>
            <button
              type="button"
              onClick={() => void fetchHistory()}
              disabled={historyLoading}
              style={{
                padding: "6px 12px",
                borderRadius: 999,
                border: "1px solid #e5e7eb",
                background: "#fff",
                fontSize: 12,
                cursor: "pointer",
              }}
            >
              🔄 刷新
            </button>
          </div>
          {historyError && (
            <div style={{ color: "#b91c1c", fontSize: 13 }}>{historyError}</div>
          )}
          {historyLoading && <div style={{ fontSize: 13 }}>加载中...</div>}
          {!historyLoading && history.length === 0 && (
            <div style={{ fontSize: 13, color: "#6b7280" }}>暂无历史报告</div>
          )}
          <div style={{ marginTop: 8, display: "flex", flexDirection: "column", gap: 8 }}>
            {history.map((item) => (
              <div
                key={item.id}
                style={{
                  borderRadius: 10,
                  border: "1px solid #e5e7eb",
                  padding: 10,
                  cursor: "pointer",
                  background:
                    selectedReport?.report?.id === item.id ? "#eef2ff" : "#fff",
                }}
                onClick={() => void fetchHistoryDetail(item.id)}
              >
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    gap: 8,
                  }}
                >
                  <div>
                    <div style={{ fontWeight: 600, fontSize: 14 }}>
                      报告 #{item.id}
                    </div>
                    <div style={{ fontSize: 12, color: "#6b7280" }}>
                      分析时间：{formatDateTime(item.analysis_date)}
                    </div>
                    {item.data_date_range && (
                      <div style={{ fontSize: 12, color: "#6b7280" }}>
                        数据范围：{item.data_date_range}
                      </div>
                    )}
                    {item.summary && (
                      <div
                        style={{
                          marginTop: 4,
                          fontSize: 12,
                          color: "#374151",
                          maxHeight: 40,
                          overflow: "hidden",
                        }}
                      >
                        {item.summary}
                      </div>
                    )}
                  </div>
                  <button
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      void deleteHistoryItem(item.id);
                    }}
                    style={{
                      padding: "4px 10px",
                      borderRadius: 999,
                      border: "1px solid #fecaca",
                      background: "#fef2f2",
                      color: "#b91c1c",
                      fontSize: 12,
                      cursor: "pointer",
                    }}
                  >
                    🗑 删除
                  </button>
                </div>
              </div>
            ))}
          </div>
        </section>

        <section
          style={{
            flex: 1.3,
            background: "#fff",
            borderRadius: 12,
            padding: 16,
            boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
            minHeight: 200,
          }}
        >
          <h2 style={{ marginTop: 0, fontSize: 18 }}>📄 报告详情</h2>
          {detailLoading && <div style={{ fontSize: 13 }}>加载中...</div>}
          {!detailLoading && !selectedReport && (
            <div style={{ fontSize: 13, color: "#6b7280" }}>
              请选择左侧一条历史报告查看详情
            </div>
          )}
          {!detailLoading && selectedReport && (
            <div
              style={{
                marginTop: 8,
                display: "flex",
                flexDirection: "column",
                gap: 12,
              }}
            >
              {selectedReport.report?.summary && (
                <div
                  style={{
                    padding: 12,
                    borderRadius: 10,
                    background: "#eff6ff",
                    fontSize: 13,
                    color: "#1d4ed8",
                  }}
                >
                  {selectedReport.report.summary}
                </div>
              )}
              {selectedReport.report?.analysis_content_parsed && (
                <div
                  style={{
                    display: "flex",
                    gap: 8,
                    flexWrap: "wrap",
                  }}
                >
                  <button
                    type="button"
                    onClick={() => {
                      const loaded = buildResultFromHistoryReport(
                        selectedReport.report,
                      );
                      setResult(loaded);
                      setActiveTab("analysis");
                    }}
                    style={{
                      padding: "6px 12px",
                      borderRadius: 999,
                      border: "none",
                      background: "#4f46e5",
                      color: "#fff",
                      fontSize: 12,
                      fontWeight: 600,
                      cursor: "pointer",
                    }}
                  >
                    📋 加载到分析标签
                  </button>
                  <details
                    style={{
                      flexBasis: "100%",
                      marginTop: 4,
                    }}
                  >
                    <summary
                      style={{
                        cursor: "pointer",
                        fontWeight: 600,
                        fontSize: 13,
                      }}
                    >
                      📊 使用分析视图查看本报告
                    </summary>
                    <div style={{ marginTop: 8 }}>
                      {renderAnalysisResult(
                        buildResultFromHistoryReport(selectedReport.report),
                      )}
                    </div>
                  </details>
                </div>
              )}
              <details>
                <summary
                  style={{
                    cursor: "pointer",
                    fontWeight: 600,
                    fontSize: 13,
                  }}
                >
                  🔍 查看原始 JSON
                </summary>
                <pre
                  style={{
                    marginTop: 8,
                    maxHeight: 420,
                    overflow: "auto",
                    fontSize: 12,
                    background: "#f9fafb",
                    padding: 12,
                    borderRadius: 8,
                  }}
                >
{JSON.stringify(selectedReport.report, null, 2)}
                </pre>
              </details>
            </div>
          )}
        </section>
      </div>
    );
  }

  function renderStatsSection() {
    if (statsLoading) {
      return <div style={{ fontSize: 13 }}>统计数据加载中...</div>;
    }
    if (statsError) {
      return <div style={{ color: "#b91c1c", fontSize: 13 }}>{statsError}</div>;
    }
    if (!stats) {
      return (
        <div style={{ fontSize: 13, color: "#6b7280" }}>
          暂无统计数据，请稍后重试。
        </div>
      );
    }
    const s = stats.stats || {};
    const range = s.date_range || {};
    const topYouzi = (s.top_youzi || []) as any[];
    const topStocks = (s.top_stocks || []) as any[];

    return (
      <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        <section
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 16,
            boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
          }}
        >
          <h2 style={{ marginTop: 0, fontSize: 18 }}>📈 基础统计</h2>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
              gap: 12,
              marginTop: 12,
            }}
          >
            <MetricCard
              label="总龙虎榜记录"
              value={formatNumber(s.total_records, 0)}
            />
            <MetricCard label="股票总数" value={formatNumber(s.total_stocks, 0)} />
            <MetricCard label="游资总数" value={formatNumber(s.total_youzi, 0)} />
            <MetricCard
              label="分析报告数"
              value={formatNumber(s.total_reports, 0)}
            />
          </div>
          <div style={{ marginTop: 12, fontSize: 13, color: "#4b5563" }}>
            数据日期范围：{formatDate(range.start)} 至 {formatDate(range.end)}
          </div>
        </section>

        {topYouzi.length > 0 && (
          <section
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 16,
              boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
            }}
          >
            <h2 style={{ marginTop: 0, fontSize: 18 }}>🏆 近30天活跃游资排名</h2>
            <p style={{ fontSize: 13, color: "#4b5563" }}>
              基于龙虎榜记录统计游资上榜次数与净流入金额。
            </p>
            <div style={{ overflowX: "auto", marginTop: 8 }}>
              <table
                style={{
                  width: "100%",
                  borderCollapse: "collapse",
                  fontSize: 12,
                }}
              >
                <thead>
                  <tr style={{ background: "#f3f4f6" }}>
                    <th style={thStyle}>游资名称</th>
                    <th style={thStyle}>交易次数</th>
                    <th style={thStyle}>总净流入(元)</th>
                  </tr>
                </thead>
                <tbody>
                  {topYouzi.map((row, idx) => (
                    <tr
                      key={`${row.youzi_name || ""}-${idx}`}
                      style={{
                        borderTop: "1px solid #e5e7eb",
                        background: idx % 2 === 0 ? "#ffffff" : "#f9fafb",
                      }}
                    >
                      <td style={tdStyle}>{row.youzi_name}</td>
                      <td style={tdStyle}>{row.trade_count}</td>
                      <td style={tdStyle}>
                        {formatNumber(
                          typeof row.total_net_inflow === "number"
                            ? row.total_net_inflow
                            : Number(row.total_net_inflow) || 0,
                          0,
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}

        {topStocks.length > 0 && (
          <section
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 16,
              boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
            }}
          >
            <h2 style={{ marginTop: 0, fontSize: 18 }}>📈 近30天热门股票排名</h2>
            <p style={{ fontSize: 13, color: "#4b5563" }}>
              统计上榜频率较高且净流入靠前的股票。
            </p>
            <div style={{ overflowX: "auto", marginTop: 8 }}>
              <table
                style={{
                  width: "100%",
                  borderCollapse: "collapse",
                  fontSize: 12,
                }}
              >
                <thead>
                  <tr style={{ background: "#f3f4f6" }}>
                    <th style={thStyle}>股票代码</th>
                    <th style={thStyle}>股票名称</th>
                    <th style={thStyle}>游资数量</th>
                    <th style={thStyle}>总净流入(元)</th>
                  </tr>
                </thead>
                <tbody>
                  {topStocks.map((row, idx) => (
                    <tr
                      key={`${row.stock_code || ""}-${idx}`}
                      style={{
                        borderTop: "1px solid #e5e7eb",
                        background: idx % 2 === 0 ? "#ffffff" : "#f9fafb",
                      }}
                    >
                      <td style={tdStyle}>{row.stock_code}</td>
                      <td style={tdStyle}>{row.stock_name}</td>
                      <td style={tdStyle}>{row.youzi_count}</td>
                      <td style={tdStyle}>
                        {formatNumber(
                          typeof row.total_net_inflow === "number"
                            ? row.total_net_inflow
                            : Number(row.total_net_inflow) || 0,
                          0,
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}

        <section
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 16,
            boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
          }}
        >
          <h2 style={{ marginTop: 0, fontSize: 18 }}>🧾 原始统计 JSON</h2>
          <pre
            style={{
              marginTop: 8,
              maxHeight: 420,
              overflow: "auto",
              fontSize: 12,
              background: "#f9fafb",
              padding: 12,
              borderRadius: 8,
            }}
          >
{JSON.stringify(s, null, 2)}
          </pre>
        </section>
      </div>
    );
  }

  return (
    <main
      style={{
        padding: 24,
        maxWidth: 1200,
        margin: "0 auto",
        background: "#f3f4f6",
        minHeight: "100vh",
      }}
    >
      <header style={{ marginBottom: 16 }}>
        <h1 style={{ margin: 0, fontSize: 24 }}>
          🎯 智瞰龙虎 - AI 驱动的龙虎榜分析
        </h1>
        <p style={{ marginTop: 4, fontSize: 13, color: "#4b5563" }}>
          Multi-Agent Dragon Tiger Analysis | 游资 · 个股 · 题材 · 风险 多维分析
        </p>
      </header>

      <section
        style={{
          background: "#e5e7eb",
          borderRadius: 999,
          padding: 4,
          display: "inline-flex",
          marginBottom: 16,
        }}
      >
        <TabPill
          active={activeTab === "analysis"}
          onClick={() => setActiveTab("analysis")}
        >
          📊 龙虎榜分析
        </TabPill>
        <TabPill
          active={activeTab === "history"}
          onClick={() => setActiveTab("history")}
        >
          📚 历史报告
        </TabPill>
        <TabPill
          active={activeTab === "stats"}
          onClick={() => setActiveTab("stats")}
        >
          📈 数据统计
        </TabPill>
      </section>

      {activeTab === "analysis" && (
        <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
          <section
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 16,
              boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
            }}
          >
            <h2 style={{ marginTop: 0, fontSize: 18 }}>⚙️ 分析参数</h2>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
                gap: 12,
                marginTop: 8,
              }}
            >
              <div>
                <label
                  style={{ display: "block", fontSize: 13, marginBottom: 4 }}
                >
                  分析模式
                </label>
                <select
                  value={analysisMode}
                  onChange={(e) =>
                    setAnalysisMode(e.target.value as AnalysisMode)
                  }
                  style={{
                    width: "100%",
                    padding: "6px 8px",
                    borderRadius: 8,
                    border: "1px solid #d1d5db",
                    fontSize: 13,
                  }}
                  aria-label="分析模式"
                >
                  <option value="date">指定日期</option>
                  <option value="recent_days">最近 N 天</option>
                </select>
              </div>

              {analysisMode === "date" ? (
                <div>
                  <label
                    style={{ display: "block", fontSize: 13, marginBottom: 4 }}
                  >
                    分析日期
                  </label>
                  <input
                    type="date"
                    value={selectedDate}
                    onChange={(e) => setSelectedDate(e.target.value)}
                    style={{
                      width: "100%",
                      padding: "6px 8px",
                      borderRadius: 8,
                      border: "1px solid #d1d5db",
                      fontSize: 13,
                    }}
                  />
                </div>
              ) : (
                <div>
                  <label
                    style={{ display: "block", fontSize: 13, marginBottom: 4 }}
                  >
                    最近天数
                  </label>
                  <input
                    type="number"
                    min={1}
                    max={10}
                    value={recentDays}
                    onChange={(e) =>
                      setRecentDays(Math.max(1, Number(e.target.value) || 1))
                    }
                    style={{
                      width: "100%",
                      padding: "6px 8px",
                      borderRadius: 8,
                      border: "1px solid #d1d5db",
                      fontSize: 13,
                    }}
                  />
                </div>
              )}

              <div>
                <label
                  style={{ display: "block", fontSize: 13, marginBottom: 4 }}
                >
                  AI 模型
                </label>
                <select
                  value={model}
                  onChange={(e) => setModel(e.target.value)}
                  style={{
                    width: "100%",
                    padding: "6px 8px",
                    borderRadius: 8,
                    border: "1px solid #d1d5db",
                    fontSize: 13,
                  }}
                  aria-label="AI 模型"
                >
                  <option value="deepseek-chat">DeepSeek-Chat</option>
                  <option value="deepseek-reasoner">DeepSeek-Reasoner</option>
                </select>
              </div>
            </div>

            <div
              style={{
                marginTop: 12,
                display: "flex",
                gap: 8,
                flexWrap: "wrap",
              }}
            >
              <button
                type="button"
                onClick={() => void runAnalysis()}
                disabled={analyzing}
                style={{
                  padding: "8px 16px",
                  borderRadius: 999,
                  border: "none",
                  background: "#4f46e5",
                  color: "#fff",
                  fontWeight: 600,
                  cursor: "pointer",
                }}
              >
                {analyzing ? "分析中..." : "🚀 开始分析"}
              </button>
              <button
                type="button"
                onClick={() => {
                  setResult(null);
                  setAnalysisError(null);
                }}
                style={{
                  padding: "8px 16px",
                  borderRadius: 999,
                  border: "1px solid #e5e7eb",
                  background: "#fff",
                  fontWeight: 500,
                  cursor: "pointer",
                }}
              >
                🔄 清除结果
              </button>
              {analysisError && (
                <span style={{ color: "#b91c1c", fontSize: 13 }}>
                  {analysisError}
                </span>
              )}
            </div>
          </section>

          {hasResult && result && renderAnalysisResult(result)}
        </div>
      )}

      {activeTab === "history" && renderHistorySection()}

      {activeTab === "stats" && renderStatsSection()}
    </main>
  );
}

function TabPill(props: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={props.onClick}
      style={{
        border: "none",
        background: props.active ? "#ffffff" : "transparent",
        color: props.active ? "#111827" : "#4b5563",
        borderRadius: 999,
        padding: "6px 14px",
        fontSize: 13,
        fontWeight: props.active ? 600 : 500,
        cursor: "pointer",
      }}
    >
      {props.children}
    </button>
  );
}

function MetricCard(props: { label: string; value: string }) {
  return (
    <div
      style={{
        padding: 10,
        borderRadius: 10,
        background: "#f9fafb",
        border: "1px solid #e5e7eb",
      }}
    >
      <div style={{ fontSize: 12, color: "#6b7280" }}>{props.label}</div>
      <div style={{ fontSize: 18, fontWeight: 600, marginTop: 2 }}>
        {props.value}
      </div>
    </div>
  );
}

const thStyle: React.CSSProperties = {
  padding: "6px 8px",
  textAlign: "left",
  borderBottom: "1px solid #e5e7eb",
  whiteSpace: "nowrap",
};

const tdStyle: React.CSSProperties = {
  padding: "6px 8px",
  textAlign: "left",
  borderBottom: "1px solid #e5e7eb",
  verticalAlign: "top",
};
