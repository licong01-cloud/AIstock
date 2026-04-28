"use client";

import React, { useState, useMemo } from "react";
import dynamic from "next/dynamic";
import { Light as SyntaxHighlighter } from "react-syntax-highlighter";
import python from "react-syntax-highlighter/dist/esm/languages/hljs/python";
import atomOneLight from "react-syntax-highlighter/dist/esm/styles/hljs/atom-one-light";

SyntaxHighlighter.registerLanguage("python", python);

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false }) as any;

const BASE = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface FactorCatalogMetrics {
  factor_name: string;
  source: string;
  ic: number | null;
  sharpe: number | null;
  annualized_return: number | null;
  max_drawdown: number | null;
  is_sota_factor: boolean | null;
  is_available: boolean | null;
}

interface FactorIndependentMetrics {
  ic_mean: number | null;
  rank_ic_mean: number | null;
  icir: number | null;
  rank_icir: number | null;
  ic_positive_ratio: number | null;
  top_sharpe: number | null;
  top_max_drawdown: number | null;
  top_annual_return: number | null;
  top_excess_annual_return: number | null;
  coverage: number | null;
  turnover: number | null;
  group_return_monotonicity: number | null;
  ic_std: number | null;
  rank_ic_std: number | null;
}

interface FactorClassification {
  official_grade: string | null;
  official_score: number | null;
  official_rule_version: string | null;
  category: string | null;
  llm_analysis: string | null;
  description: string | null;
  grade_reason: string | null;
  factor_dimension: string | null;
  factor_source: string | null;
}

interface FactorFullMetrics {
  catalog: FactorCatalogMetrics | null;
  independent: FactorIndependentMetrics | null;
  classification: FactorClassification | null;
  source_code?: string | null;
}

export interface PairData {
  factor_a: string;
  factor_b: string;
  correlation?: number;
  db_result?: {
    correlation: number;
    method: string;
    computed_at: string;
    data_period: string;
  } | null;
  daily_analysis?: {
    correlation_ewma: number;
    effective_days: number;
    avg_stocks_per_day: number;
    daily_correlations: [string, number][];
  } | null;
  factor_metrics?: Record<string, FactorFullMetrics>;
}

export interface RelatedFactor {
  factor: string;
  correlation: number;
}

interface Props {
  factorA: string;
  factorB: string;
  factorNames: string[];
  pairData: PairData | null;
  relatedFactors: RelatedFactor[];
  loading: boolean;
  onChangePair: (fa: string, fb: string) => void;
  onDeleteFactor?: (factorName: string, source: string) => Promise<void>;
  onSetUnavailable?: (factorName: string, source: string) => Promise<void>;
  onGoToDedup?: (factorName: string) => void;
}

function corrColor(corr: number): string {
  if (corr >= 0.7) return "#dc2626";
  if (corr >= 0.5) return "#ea580c";
  if (corr <= -0.7) return "#2563eb";
  if (corr <= -0.5) return "#3b82f6";
  return "#374151";
}

function judgeWinner(
  a: number | null | undefined,
  b: number | null | undefined,
  higherBetter = true
): "A" | "B" | "tie" | "na" {
  if (a == null && b == null) return "na";
  if (a == null) return "B";
  if (b == null) return "A";
  if (Math.abs(a - b) < 1e-8) return "tie";
  return higherBetter ? (a > b ? "A" : "B") : a < b ? "A" : "B";
}

const gradeRank: Record<string, number> = { S: 5, A: 4, B: 3, C: 2, D: 1 };
function judgeGrade(a: string | null | undefined, b: string | null | undefined): "A" | "B" | "tie" | "na" {
  if (!a && !b) return "na";
  if (!a) return "B";
  if (!b) return "A";
  const ra = gradeRank[a] ?? 0;
  const rb = gradeRank[b] ?? 0;
  if (ra === rb) return "tie";
  return ra > rb ? "A" : "B";
}

function fmt(v: number | null | undefined, decimals = 4, pct = false): string {
  if (v == null) return "-";
  if (pct) return `${(v * 100).toFixed(decimals > 2 ? decimals - 2 : 1)}%`;
  return v.toFixed(decimals);
}

function WinnerBadge({ winner, nameA, nameB }: { winner: "A" | "B" | "tie" | "na"; nameA: string; nameB: string }) {
  if (winner === "na") return <span style={{ color: "#9ca3af" }}>-</span>;
  if (winner === "tie") return <span style={{ color: "#9ca3af" }}>平</span>;
  const label = winner === "A" ? nameA.slice(0, 12) : nameB.slice(0, 12);
  const color = winner === "A" ? "#059669" : "#7c3aed";
  return <span style={{ color, fontWeight: 600 }}>{label.length > 10 ? label.slice(0, 10) + "…" : label} ✓</span>;
}

export default function PairDetail({
  factorA,
  factorB,
  factorNames,
  pairData,
  relatedFactors,
  loading,
  onChangePair,
  onDeleteFactor,
  onSetUnavailable,
  onGoToDedup,
}: Props) {
  const da = pairData?.daily_analysis;
  const corr = da?.correlation_ewma ?? pairData?.db_result?.correlation ?? null;
  const fm = pairData?.factor_metrics;
  const mA = fm?.[factorA];
  const mB = fm?.[factorB];

  const [confirmAction, setConfirmAction] = useState<{
    type: "delete" | "unavailable";
    factorName: string;
    source: string;
  } | null>(null);
  const [actionLoading, setActionLoading] = useState(false);
  const [codeExpanded, setCodeExpanded] = useState(false);
  const [aiAnalysis, setAiAnalysis] = useState<string | null>(null);
  const [aiLoading, setAiLoading] = useState(false);
  const [aiModel, setAiModel] = useState<string | null>(null);

  const codeA = mA?.source_code;
  const codeB = mB?.source_code;
  const hasCode = !!(codeA || codeB);

  // 综合结论
  const comparison = useMemo(() => {
    if (!mA || !mB) return null;
    type Row = { label: string; va: number | null | undefined; vb: number | null | undefined; higherBetter: boolean; pct?: boolean };
    const rows: Row[] = [];
    // Independent
    rows.push({ label: "IC均值", va: mA.independent?.ic_mean, vb: mB.independent?.ic_mean, higherBetter: true });
    rows.push({ label: "Rank IC", va: mA.independent?.rank_ic_mean, vb: mB.independent?.rank_ic_mean, higherBetter: true });
    rows.push({ label: "ICIR", va: mA.independent?.icir, vb: mB.independent?.icir, higherBetter: true });
    rows.push({ label: "Rank ICIR", va: mA.independent?.rank_icir, vb: mB.independent?.rank_icir, higherBetter: true });
    rows.push({ label: "多头Sharpe", va: mA.independent?.top_sharpe, vb: mB.independent?.top_sharpe, higherBetter: true });
    rows.push({ label: "多头回撤", va: mA.independent?.top_max_drawdown, vb: mB.independent?.top_max_drawdown, higherBetter: true, pct: true });
    rows.push({ label: "多头收益", va: mA.independent?.top_annual_return, vb: mB.independent?.top_annual_return, higherBetter: true, pct: true });
    rows.push({ label: "覆盖率", va: mA.independent?.coverage, vb: mB.independent?.coverage, higherBetter: true, pct: true });
    rows.push({ label: "换手率", va: mA.independent?.turnover, vb: mB.independent?.turnover, higherBetter: false, pct: true });
    rows.push({ label: "IC胜率", va: mA.independent?.ic_positive_ratio, vb: mB.independent?.ic_positive_ratio, higherBetter: true, pct: true });
    rows.push({ label: "单调性", va: mA.independent?.group_return_monotonicity, vb: mB.independent?.group_return_monotonicity, higherBetter: true });

    let aWins = 0, bWins = 0, comparable = 0;
    for (const r of rows) {
      const w = judgeWinner(r.va, r.vb, r.higherBetter);
      if (w === "A") { aWins++; comparable++; }
      else if (w === "B") { bWins++; comparable++; }
      else if (w === "tie") comparable++;
    }
    const recommended = aWins >= bWins ? "A" : "B";
    const loser = recommended === "A" ? "B" : "A";
    const loserName = loser === "A" ? factorA : factorB;
    const loserM = loser === "A" ? mA : mB;
    const loserSource = loserM.catalog?.source ?? loserM.classification?.factor_source ?? "rdagent_task_sync";

    return { rows, aWins, bWins, comparable, recommended, loser, loserName, loserSource };
  }, [mA, mB, factorA, factorB]);

  const handleAction = async (type: "delete" | "unavailable") => {
    if (!confirmAction) return;
    setActionLoading(true);
    try {
      if (type === "delete" && onDeleteFactor) {
        await onDeleteFactor(confirmAction.factorName, confirmAction.source);
      } else if (type === "unavailable" && onSetUnavailable) {
        await onSetUnavailable(confirmAction.factorName, confirmAction.source);
      }
    } finally {
      setActionLoading(false);
      setConfirmAction(null);
    }
  };

  const handleAIAnalyze = async () => {
    setAiLoading(true);
    setAiAnalysis(null);
    setAiModel(null);
    try {
      const resp = await fetch(`${BASE}/quantevolver/evolution/correlations/pair/analyze`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ factor_a: factorA, factor_b: factorB }),
      });
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({ detail: resp.statusText }));
        throw new Error(err.detail || `HTTP ${resp.status}`);
      }
      const data = await resp.json();
      setAiAnalysis(data.analysis);
      setAiModel(data.model || null);
    } catch (e: any) {
      setAiAnalysis(`分析失败: ${e.message || e}`);
    } finally {
      setAiLoading(false);
    }
  };

  return (
    <div>
      {/* 因子选择器 */}
      <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 20, flexWrap: "wrap" }}>
        <label style={{ fontSize: 13, color: "#6b7280" }}>因子 A:</label>
        <select
          value={factorA}
          onChange={(e) => onChangePair(e.target.value, factorB)}
          style={{ padding: "6px 10px", border: "1px solid #e5e7eb", borderRadius: 8, fontSize: 12, fontFamily: "monospace", outline: "none", maxWidth: 240 }}
        >
          <option value="">-- 选择因子 --</option>
          {factorNames.map((n) => <option key={n} value={n}>{n}</option>)}
        </select>
        <label style={{ fontSize: 13, color: "#6b7280" }}>因子 B:</label>
        <select
          value={factorB}
          onChange={(e) => onChangePair(factorA, e.target.value)}
          style={{ padding: "6px 10px", border: "1px solid #e5e7eb", borderRadius: 8, fontSize: 12, fontFamily: "monospace", outline: "none", maxWidth: 240 }}
        >
          <option value="">-- 选择因子 --</option>
          {factorNames.map((n) => <option key={n} value={n}>{n}</option>)}
        </select>
      </div>

      {/* 加载中 / 未选择 */}
      {loading ? (
        <div style={{ color: "#9ca3af", textAlign: "center", padding: 48 }}>加载中...</div>
      ) : !factorA || !factorB ? (
        <div style={{ color: "#9ca3af", textAlign: "center", padding: 48 }}>请选择两个因子进行对比分析</div>
      ) : !pairData ? (
        <div style={{ color: "#9ca3af", textAlign: "center", padding: 48 }}>暂无该因子对的相关性数据</div>
      ) : (
        <>
          {/* 概要卡片组 */}
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(160px, 1fr))", gap: 16, marginBottom: 24 }}>
            <MetricCard label="EWMA 相关系数" value={corr != null ? corr.toFixed(4) : "-"} color={corr != null ? corrColor(corr) : "#374151"} large />
            <MetricCard label="有效天数" value={da?.effective_days != null ? String(da.effective_days) : "-"} />
            <MetricCard label="平均股票数/天" value={da?.avg_stocks_per_day != null ? da.avg_stocks_per_day.toFixed(0) : "-"} />
            <MetricCard label="计算方法" value={pairData.db_result?.method ?? "spearman_ewma"} />
            <MetricCard label="数据区间" value={pairData.db_result?.data_period ?? "-"} />
          </div>

          {/* 日频相关性折线图 */}
          {da?.daily_correlations && da.daily_correlations.length > 0 && (
            <div style={{ background: "#fff", borderRadius: 12, padding: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)", marginBottom: 24 }}>
              <h3 style={{ margin: "0 0 12px", fontSize: 14, fontWeight: 600, color: "#374151" }}>日频 Spearman 截面相关性</h3>
              <Plot
                data={[{
                  x: da.daily_correlations.map((d) => d[0]),
                  y: da.daily_correlations.map((d) => d[1]),
                  type: "scatter", mode: "lines", name: "Daily Corr",
                  line: { color: "#7c3aed", width: 1.5 },
                }]}
                layout={{
                  height: 320, margin: { t: 20, r: 20, b: 50, l: 50 },
                  xaxis: { title: "", tickangle: -30 },
                  yaxis: { title: "Correlation", range: [-1, 1] },
                  shapes: [hLine(0.7, "#dc2626", "dot"), hLine(-0.7, "#dc2626", "dot"), hLine(0.5, "#d1d5db", "dash"), hLine(-0.5, "#d1d5db", "dash"), hLine(0, "#9ca3af", "solid")],
                  font: { size: 11 }, showlegend: false,
                }}
                config={{ responsive: true, displayModeBar: false }}
                style={{ width: "100%" }}
              />
            </div>
          )}

          {/* 因子指标对比表 */}
          {comparison && (
            <div style={{ background: "#fff", borderRadius: 12, padding: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)", marginBottom: 24 }}>
              <h3 style={{ margin: "0 0 16px", fontSize: 14, fontWeight: 600, color: "#374151" }}>因子指标对比</h3>

              {/* 独立指标 */}
              <ComparisonSection title="独立指标（权威来源）" rows={comparison.rows} factorA={factorA} factorB={factorB} />

              {/* LLM分析 */}
              {(mA?.classification || mB?.classification) && (
                <div style={{ marginTop: 16 }}>
                  <div style={{ fontSize: 13, fontWeight: 600, color: "#374151", marginBottom: 8, cursor: "pointer" }}>
                    LLM分析
                  </div>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                    <thead>
                      <tr style={{ borderBottom: "2px solid #e5e7eb", color: "#6b7280" }}>
                        <th style={thStyle}>指标</th>
                        <th style={thStyle}>{factorA.slice(0, 16)}</th>
                        <th style={thStyle}>{factorB.slice(0, 16)}</th>
                        <th style={thStyle}>优胜</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr style={trStyle}>
                        <td style={tdStyle}>评级</td>
                        <td style={tdCenterStyle}>{mA?.classification?.official_grade ?? "-"}</td>
                        <td style={tdCenterStyle}>{mB?.classification?.official_grade ?? "-"}</td>
                        <td style={tdCenterStyle}><WinnerBadge winner={judgeGrade(mA?.classification?.official_grade, mB?.classification?.official_grade)} nameA={factorA} nameB={factorB} /></td>
                      </tr>
                      <tr style={trStyle}>
                        <td style={tdStyle}>分类</td>
                        <td style={tdCenterStyle}>{mA?.classification?.category ?? "-"}</td>
                        <td style={tdCenterStyle}>{mB?.classification?.category ?? "-"}</td>
                        <td style={tdCenterStyle}><span style={{ color: "#9ca3af" }}>-</span></td>
                      </tr>
                      <tr style={trStyle}>
                        <td style={tdStyle}>描述</td>
                        <td style={{ ...tdCenterStyle, fontSize: 11, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{mA?.classification?.description ?? "-"}</td>
                        <td style={{ ...tdCenterStyle, fontSize: 11, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{mB?.classification?.description ?? "-"}</td>
                        <td style={tdCenterStyle}><span style={{ color: "#9ca3af" }}>-</span></td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              )}

              {/* 因子源代码对比 */}
              {hasCode && (
                <div style={{ marginTop: 16 }}>
                  <div
                    onClick={() => setCodeExpanded(!codeExpanded)}
                    style={{ fontSize: 13, fontWeight: 600, color: "#374151", marginBottom: 8, cursor: "pointer", userSelect: "none", display: "flex", alignItems: "center", gap: 6 }}
                  >
                    <span style={{ fontSize: 10, color: "#9ca3af", transition: "transform 0.2s", transform: codeExpanded ? "rotate(90deg)" : "rotate(0deg)" }}>&#9654;</span>
                    因子源代码对比
                    <span style={{ fontSize: 11, color: "#9ca3af", fontWeight: 400 }}>（点击{codeExpanded ? "收起" : "展开"}）</span>
                  </div>
                  {codeExpanded && (
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                      <div>
                        <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 4, fontWeight: 600 }}>{factorA.slice(0, 24)}</div>
                        {codeA ? (
                          <SyntaxHighlighter
                            language="python"
                            style={atomOneLight}
                            customStyle={{ fontSize: 11, borderRadius: 8, maxHeight: 400, overflow: "auto", border: "1px solid #e5e7eb" }}
                            showLineNumbers
                          >
                            {codeA}
                          </SyntaxHighlighter>
                        ) : (
                          <div style={{ padding: 16, color: "#9ca3af", fontSize: 12, background: "#f9fafb", borderRadius: 8, border: "1px solid #e5e7eb" }}>源代码不可用</div>
                        )}
                      </div>
                      <div>
                        <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 4, fontWeight: 600 }}>{factorB.slice(0, 24)}</div>
                        {codeB ? (
                          <SyntaxHighlighter
                            language="python"
                            style={atomOneLight}
                            customStyle={{ fontSize: 11, borderRadius: 8, maxHeight: 400, overflow: "auto", border: "1px solid #e5e7eb" }}
                            showLineNumbers
                          >
                            {codeB}
                          </SyntaxHighlighter>
                        ) : (
                          <div style={{ padding: 16, color: "#9ca3af", fontSize: 12, background: "#f9fafb", borderRadius: 8, border: "1px solid #e5e7eb" }}>源代码不可用</div>
                        )}
                      </div>
                    </div>
                  )}
                </div>
              )}

              {/* AI 相关性分析 */}
              <div style={{ marginTop: 16 }}>
                <div style={{ fontSize: 13, fontWeight: 600, color: "#374151", marginBottom: 8, display: "flex", alignItems: "center", gap: 10 }}>
                  AI 相关性分析
                  <button
                    onClick={handleAIAnalyze}
                    disabled={aiLoading}
                    style={{
                      padding: "4px 14px", fontSize: 12, fontWeight: 600, borderRadius: 6, cursor: aiLoading ? "not-allowed" : "pointer", border: "none",
                      background: aiLoading ? "#e5e7eb" : "linear-gradient(135deg, #7c3aed, #6366f1)", color: aiLoading ? "#9ca3af" : "#fff",
                    }}
                  >
                    {aiLoading ? "分析中..." : "AI 分析"}
                  </button>
                  {aiModel && <span style={{ fontSize: 11, color: "#9ca3af" }}>模型: {aiModel}</span>}
                </div>
                {aiLoading && (
                  <div style={{ padding: 24, textAlign: "center", color: "#9ca3af", fontSize: 13 }}>
                    正在调用 LLM 分析因子相关性，请稍候...
                  </div>
                )}
                {aiAnalysis && !aiLoading && (
                  <div style={{ padding: 14, background: "#faf5ff", borderRadius: 8, border: "1px solid #e9d5ff", fontSize: 13, lineHeight: 1.7, whiteSpace: "pre-wrap", color: "#374151" }}>
                    {aiAnalysis}
                  </div>
                )}
              </div>

              {/* 综合结论 */}
              <div style={{ marginTop: 16, padding: 12, background: "#f0fdf4", borderRadius: 8, border: "1px solid #bbf7d0" }}>
                <div style={{ fontSize: 13, fontWeight: 600, color: "#166534", marginBottom: 4 }}>综合结论</div>
                <div style={{ fontSize: 12, color: "#166534" }}>
                  {comparison.recommended === "A" ? factorA : factorB} 在 {Math.max(comparison.aWins, comparison.bWins)}/{comparison.comparable} 项可比指标中胜出。
                  {corr != null && Math.abs(corr) >= 0.5
                    ? `建议：保留 ${comparison.recommended === "A" ? factorA : factorB}，处理 ${comparison.loserName}。`
                    : "当前相关性较低，两个因子可独立保留。"
                  }
                </div>
              </div>

              {/* 操作按钮 — 仅高相关性时显示 */}
              {(onDeleteFactor || onSetUnavailable) && corr != null && Math.abs(corr) >= 0.5 && (
                <div style={{ marginTop: 16, padding: 12, background: "#f9fafb", borderRadius: 8, border: "1px solid #e5e7eb" }}>
                  <div style={{ fontSize: 13, fontWeight: 600, color: "#374151", marginBottom: 8 }}>操作建议</div>
                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center", marginBottom: 8 }}>
                    <button
                      onClick={() => {/* no-op: 保留两个因子 */}}
                      style={{ padding: "6px 14px", fontSize: 12, background: "#f3f4f6", color: "#374151", border: "1px solid #d1d5db", borderRadius: 6, cursor: "pointer" }}
                    >
                      保留两个因子
                    </button>
                  </div>
                  <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 6 }}>
                    针对 {comparison.loserName}：
                  </div>
                  <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                    {onDeleteFactor && (
                      <button
                        onClick={() => setConfirmAction({ type: "delete", factorName: comparison.loserName, source: comparison.loserSource })}
                        style={{ padding: "6px 14px", fontSize: 12, background: "#fef2f2", color: "#dc2626", border: "1px solid #fecaca", borderRadius: 6, cursor: "pointer" }}
                      >
                        彻底删除 {comparison.loserName.slice(0, 16)}
                      </button>
                    )}
                    {onSetUnavailable && (
                      <button
                        onClick={() => setConfirmAction({ type: "unavailable", factorName: comparison.loserName, source: comparison.loserSource })}
                        style={{ padding: "6px 14px", fontSize: 12, background: "#fffbeb", color: "#d97706", border: "1px solid #fde68a", borderRadius: 6, cursor: "pointer" }}
                      >
                        设为不可用
                      </button>
                    )}
                  </div>
                </div>
              )}
            </div>
          )}

          {/* 确认弹窗 */}
          {confirmAction && (
            <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.4)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 9999 }}>
              <div style={{ background: "#fff", borderRadius: 12, padding: 24, maxWidth: 420, width: "90%", boxShadow: "0 20px 60px rgba(0,0,0,0.2)" }}>
                <h3 style={{ margin: "0 0 12px", fontSize: 16, fontWeight: 600, color: confirmAction.type === "delete" ? "#dc2626" : "#d97706" }}>
                  {confirmAction.type === "delete" ? "确认删除因子" : "确认标记不可用"}
                </h3>
                <div style={{
                  padding: 12, borderRadius: 8, marginBottom: 16, fontSize: 13,
                  background: confirmAction.type === "delete" ? "#fef2f2" : "#fffbeb",
                  border: `1px solid ${confirmAction.type === "delete" ? "#fecaca" : "#fde68a"}`,
                  color: confirmAction.type === "delete" ? "#991b1b" : "#92400e",
                }}>
                  {confirmAction.type === "delete"
                    ? "此操作将级联删除该因子的所有关联数据（指标、相关性、实验记录等），不可撤销！"
                    : "标记为不可用后，该因子不参与 SOTA 保护和新实验，但可随时恢复。"
                  }
                </div>
                <div style={{ fontSize: 13, color: "#374151", marginBottom: 16 }}>
                  因子：<strong style={{ fontFamily: "monospace" }}>{confirmAction.factorName}</strong>
                  <br />来源：{confirmAction.source}
                </div>
                <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
                  <button
                    onClick={() => setConfirmAction(null)}
                    disabled={actionLoading}
                    style={{ padding: "8px 16px", fontSize: 13, background: "#f3f4f6", color: "#374151", border: "1px solid #d1d5db", borderRadius: 8, cursor: "pointer" }}
                  >
                    取消
                  </button>
                  <button
                    onClick={() => handleAction(confirmAction.type)}
                    disabled={actionLoading}
                    style={{
                      padding: "8px 16px", fontSize: 13, borderRadius: 8, cursor: "pointer", border: "none",
                      background: confirmAction.type === "delete" ? "#dc2626" : "#d97706",
                      color: "#fff", opacity: actionLoading ? 0.6 : 1,
                    }}
                  >
                    {actionLoading ? "处理中..." : "确认"}
                  </button>
                </div>
              </div>
            </div>
          )}

          {/* 关联因子 Top10 */}
          {relatedFactors.length > 0 && (
            <div style={{ background: "#fff", borderRadius: 12, padding: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
                <h3 style={{ margin: 0, fontSize: 14, fontWeight: 600, color: "#374151" }}>
                  {factorA} 的高相关因子 Top {relatedFactors.length}
                </h3>
                {onGoToDedup && (
                  <button
                    onClick={() => onGoToDedup(factorA)}
                    style={{
                      padding: "4px 12px",
                      fontSize: 12,
                      fontWeight: 500,
                      background: "#f3f4f6",
                      color: "#7c3aed",
                      border: "none",
                      borderRadius: 6,
                      cursor: "pointer",
                    }}
                  >
                    批量管理 &rarr;
                  </button>
                )}
              </div>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                <thead>
                  <tr style={{ borderBottom: "2px solid #e5e7eb", fontSize: 12, color: "#6b7280" }}>
                    <th style={{ padding: "8px 12px", textAlign: "left" }}>因子</th>
                    <th style={{ padding: "8px 12px", textAlign: "center" }}>相关系数</th>
                    <th style={{ padding: "8px 12px", textAlign: "center" }}>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {relatedFactors.map((r) => (
                    <tr key={r.factor} style={{ borderBottom: "1px solid #f3f4f6" }}>
                      <td style={{ padding: "6px 12px", fontFamily: "monospace", fontSize: 12 }}>{r.factor}</td>
                      <td style={{ padding: "6px 12px", textAlign: "center", fontWeight: 600, fontFamily: "monospace", color: corrColor(r.correlation ?? 0) }}>
                        {r.correlation != null ? r.correlation.toFixed(4) : "-"}
                      </td>
                      <td style={{ padding: "6px 12px", textAlign: "center" }}>
                        <button
                          onClick={() => onChangePair(factorA, r.factor)}
                          style={{ padding: "3px 10px", fontSize: 11, background: "#f3f4f6", color: "#4b5563", border: "none", borderRadius: 6, cursor: "pointer" }}
                        >
                          对比
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  );
}

// ── 对比表 Section 子组件 ──

const thStyle: React.CSSProperties = { padding: "6px 8px", textAlign: "center", fontSize: 12 };
const tdStyle: React.CSSProperties = { padding: "5px 8px", fontSize: 12 };
const tdCenterStyle: React.CSSProperties = { ...tdStyle, textAlign: "center", fontFamily: "monospace" };
const trStyle: React.CSSProperties = { borderBottom: "1px solid #f3f4f6" };

function ComparisonSection({ title, rows, factorA, factorB }: {
  title: string;
  rows: { label: string; va: number | null | undefined; vb: number | null | undefined; higherBetter: boolean; pct?: boolean }[];
  factorA: string;
  factorB: string;
}) {
  return (
    <div style={{ marginBottom: 12 }}>
      <div style={{ fontSize: 13, fontWeight: 600, color: "#374151", marginBottom: 6 }}>{title}</div>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
        <thead>
          <tr style={{ borderBottom: "2px solid #e5e7eb", color: "#6b7280" }}>
            <th style={thStyle}>指标</th>
            <th style={thStyle}>{factorA.slice(0, 16)}</th>
            <th style={thStyle}>{factorB.slice(0, 16)}</th>
            <th style={thStyle}>优胜</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => {
            const winner = judgeWinner(r.va, r.vb, r.higherBetter);
            return (
              <tr key={r.label} style={trStyle}>
                <td style={tdStyle}>{r.label}</td>
                <td style={{ ...tdCenterStyle, fontWeight: winner === "A" ? 700 : 400, color: winner === "A" ? "#059669" : undefined }}>
                  {fmt(r.va, 4, r.pct)}
                </td>
                <td style={{ ...tdCenterStyle, fontWeight: winner === "B" ? 700 : 400, color: winner === "B" ? "#7c3aed" : undefined }}>
                  {fmt(r.vb, 4, r.pct)}
                </td>
                <td style={tdCenterStyle}>
                  <WinnerBadge winner={winner} nameA={factorA} nameB={factorB} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function MetricCard({ label, value, color, large }: { label: string; value: string; color?: string; large?: boolean }) {
  return (
    <div style={{ background: "#f9fafb", borderRadius: 10, padding: "14px 16px", textAlign: "center" }}>
      <div style={{ fontSize: large ? 28 : 18, fontWeight: 700, fontFamily: "monospace", color: color ?? "#374151" }}>
        {value}
      </div>
      <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 4 }}>{label}</div>
    </div>
  );
}

function hLine(y: number, color: string, dash: "solid" | "dash" | "dot"): any {
  return { type: "line", xref: "paper", x0: 0, x1: 1, y0: y, y1: y, line: { color, width: 1, dash } };
}
