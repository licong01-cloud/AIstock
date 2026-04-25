"use client";

import React, { useState, useCallback } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";
const BASE = `${API}/quantevolver`;

type Candidate = {
  factor_catalog_id: number;
  factor_name: string;
  source: string;
  deletion_reason: "exact_twin" | "pure_noise" | "fuzzy_twin";
  // twin-specific
  twin_kept?: string;
  twin_kept_id?: number;
  twin_corr?: number;
  kept_is_immune?: boolean;
  // metrics preview
  rank_ic_mean?: number | null;
  rank_ic_1d?: number | null;
  rank_ic_5d?: number | null;
  rank_ic_10d?: number | null;
  rank_ic_20d?: number | null;
  rank_icir_annualized?: number | null;
  top_excess_sharpe?: number | null;
  coverage?: number | null;
  monthly_ic_trend_slope?: number | null;
  ic_sign_consistency_12m?: number | null;
  ic_oos_is_ratio?: number | null;
  v2_grade?: string | null;
  v2_score?: number | null;
};

type AnalyzeResult = {
  ok: boolean;
  thresholds: Record<string, number>;
  total_factors: number;
  immune_count: number;
  exact_twins: Candidate[];
  pure_noise: Candidate[];
  fuzzy_twins: Candidate[];
  total_candidates: number;
  remaining_keep: number;
};

type Bucket = "exact_twins" | "pure_noise" | "fuzzy_twins";

const BUCKET_LABELS: Record<Bucket, string> = {
  exact_twins: "精确孪生 (|corr| ≥ 0.999)",
  pure_noise: "纯噪声 (全指标低于阈值)",
  fuzzy_twins: "模糊孪生 (0.98 ≤ |corr| < 0.999)",
};

function fmt(v: number | null | undefined, digits = 3): string {
  if (v === null || v === undefined) return "-";
  if (typeof v !== "number" || !isFinite(v)) return "-";
  return v.toFixed(digits);
}

export default function FactorDeletionPage() {
  const [result, setResult] = useState<AnalyzeResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [executing, setExecuting] = useState(false);
  const [activeTab, setActiveTab] = useState<Bucket>("exact_twins");
  const [selected, setSelected] = useState<Set<string>>(new Set()); // key = `${source}:${factor_name}`
  const [toast, setToast] = useState<{ text: string; type: "success" | "error" } | null>(null);

  const showToast = (text: string, type: "success" | "error" = "success") => {
    setToast({ text, type });
    setTimeout(() => setToast(null), 4500);
  };

  const key = (c: Candidate) => `${c.source}:${c.factor_name}`;

  const handleAnalyze = useCallback(async () => {
    setLoading(true);
    setResult(null);
    setSelected(new Set());
    try {
      const res = await fetch(`${BASE}/deletion/analyze`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      });
      if (!res.ok) {
        const t = await res.text();
        showToast(`分析失败: ${t}`, "error");
        return;
      }
      const data: AnalyzeResult = await res.json();
      setResult(data);
      showToast(
        `分析完成：${data.total_candidates} 个候选（${data.exact_twins.length} 精确孪生 / ${data.pure_noise.length} 纯噪声 / ${data.fuzzy_twins.length} 模糊孪生）`
      );
    } catch (e: any) {
      showToast(`分析异常: ${e.message}`, "error");
    } finally {
      setLoading(false);
    }
  }, []);

  const toggle = (c: Candidate) => {
    const k = key(c);
    const s = new Set(selected);
    if (s.has(k)) s.delete(k);
    else s.add(k);
    setSelected(s);
  };

  const toggleAllInBucket = (bucket: Bucket) => {
    if (!result) return;
    const rows = result[bucket];
    const keys = rows.map(key);
    const allSelected = keys.every((k) => selected.has(k));
    const s = new Set(selected);
    if (allSelected) keys.forEach((k) => s.delete(k));
    else keys.forEach((k) => s.add(k));
    setSelected(s);
  };

  const handleExecute = useCallback(async () => {
    if (!result || selected.size === 0) return;
    if (!confirm(`确认删除 ${selected.size} 个因子？删除不可恢复，所有关联表数据将被级联清理。`)) return;
    setExecuting(true);
    try {
      const all = [...result.exact_twins, ...result.pure_noise, ...result.fuzzy_twins];
      const picked = all.filter((c) => selected.has(key(c)));
      const factors = picked.map((c) => ({ factor_name: c.factor_name, source: c.source }));

      const res = await fetch(`${BASE}/factors/batch-action`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "delete", factors }),
      });
      const data = await res.json();
      const succeeded = data.succeeded?.length ?? 0;
      const failed = data.failed?.length ?? 0;
      showToast(`删除完成: 成功 ${succeeded}，失败 ${failed}`, failed === 0 ? "success" : "error");
      // 自动重新分析
      await handleAnalyze();
    } catch (e: any) {
      showToast(`删除异常: ${e.message}`, "error");
    } finally {
      setExecuting(false);
    }
  }, [result, selected, handleAnalyze]);

  const rows: Candidate[] = result ? result[activeTab] : [];
  const allInBucketSelected = rows.length > 0 && rows.every((c) => selected.has(key(c)));

  return (
    <div style={{ maxWidth: 1400, margin: "0 auto", padding: 20 }}>
      {toast && (
        <div
          style={{
            position: "fixed",
            top: 24,
            right: 24,
            zIndex: 9999,
            padding: "10px 20px",
            borderRadius: 8,
            fontSize: 13,
            fontWeight: 500,
            color: "#fff",
            background: toast.type === "success" ? "#10b981" : "#ef4444",
            boxShadow: "0 4px 12px rgba(0,0,0,0.15)",
            maxWidth: 600,
          }}
        >
          {toast.text}
        </div>
      )}

      <h1 style={{ fontSize: 22, fontWeight: 700, marginBottom: 8 }}>因子删除候选分析</h1>
      <p style={{ color: "#6b7280", fontSize: 13, marginBottom: 20 }}>
        基于 v2 评级 + 多周期 rankIC + 月度趋势 + 相关性，识别可删除的精确孪生 / 纯噪声 / 模糊孪生。
        <br />
        5 条免疫规则保护任一周期强预测 / 后发先至 / OOS 稳定 / 高 Sharpe / 人工手写因子。
      </p>

      {/* 顶部操作区 */}
      <div
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 16,
          marginBottom: 16,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
          display: "flex",
          gap: 12,
          alignItems: "center",
          flexWrap: "wrap",
        }}
      >
        <button
          onClick={handleAnalyze}
          disabled={loading || executing}
          style={{
            padding: "10px 22px",
            borderRadius: 8,
            fontSize: 14,
            fontWeight: 600,
            border: "none",
            cursor: loading || executing ? "not-allowed" : "pointer",
            background: loading ? "#9ca3af" : "#7c3aed",
            color: "#fff",
          }}
        >
          {loading ? "分析中..." : "🔍 开始分析"}
        </button>

        {result && (
          <>
            <div style={{ fontSize: 13, color: "#374151" }}>
              <b>总因子:</b> {result.total_factors} &nbsp;
              <b style={{ color: "#10b981" }}>免疫保留:</b> {result.immune_count} &nbsp;
              <b style={{ color: "#ef4444" }}>候选删除:</b> {result.total_candidates} &nbsp;
              <b style={{ color: "#2563eb" }}>最终保留:</b> {result.remaining_keep}
            </div>
            <div style={{ flex: 1 }} />
            <button
              onClick={handleExecute}
              disabled={selected.size === 0 || executing}
              style={{
                padding: "10px 22px",
                borderRadius: 8,
                fontSize: 14,
                fontWeight: 600,
                border: "none",
                cursor: selected.size === 0 || executing ? "not-allowed" : "pointer",
                background: selected.size === 0 ? "#d1d5db" : executing ? "#9ca3af" : "#ef4444",
                color: "#fff",
              }}
            >
              {executing ? "删除中..." : `🗑️ 删除选中 (${selected.size})`}
            </button>
          </>
        )}
      </div>

      {/* Tab 切换 */}
      {result && (
        <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
          {(Object.keys(BUCKET_LABELS) as Bucket[]).map((b) => (
            <button
              key={b}
              onClick={() => setActiveTab(b)}
              style={{
                padding: "8px 18px",
                borderRadius: 8,
                fontSize: 13,
                fontWeight: 600,
                border: "none",
                cursor: "pointer",
                background: activeTab === b ? "#7c3aed" : "#f3f4f6",
                color: activeTab === b ? "#fff" : "#6b7280",
              }}
            >
              {BUCKET_LABELS[b]} ({result[b].length})
            </button>
          ))}
        </div>
      )}

      {/* 候选表格 */}
      {result && (
        <div
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 12,
            boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
            overflowX: "auto",
          }}
        >
          {rows.length === 0 ? (
            <div style={{ color: "#9ca3af", textAlign: "center", padding: 48 }}>
              此分类下暂无候选因子
            </div>
          ) : (
            <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
              <thead style={{ background: "#f9fafb" }}>
                <tr>
                  <th style={th}>
                    <input
                      type="checkbox"
                      checked={allInBucketSelected}
                      onChange={() => toggleAllInBucket(activeTab)}
                    />
                  </th>
                  <th style={th}>因子名</th>
                  <th style={th}>来源</th>
                  {activeTab !== "pure_noise" && (
                    <>
                      <th style={th}>保留的孪生</th>
                      <th style={th}>|corr|</th>
                      <th style={th}>对方免疫</th>
                    </>
                  )}
                  <th style={th}>rank_ic_mean</th>
                  <th style={th}>1d</th>
                  <th style={th}>5d</th>
                  <th style={th}>10d</th>
                  <th style={th}>20d</th>
                  <th style={th}>ICIR年化</th>
                  <th style={th}>Sharpe</th>
                  <th style={th}>月趋势</th>
                  <th style={th}>符号一致</th>
                  <th style={th}>OOS/IS</th>
                  <th style={th}>覆盖</th>
                  <th style={th}>v2评分</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((c) => {
                  const isSelected = selected.has(key(c));
                  return (
                    <tr
                      key={key(c)}
                      style={{
                        borderTop: "1px solid #f3f4f6",
                        background: isSelected ? "#fef2f2" : undefined,
                      }}
                    >
                      <td style={td}>
                        <input type="checkbox" checked={isSelected} onChange={() => toggle(c)} />
                      </td>
                      <td style={{ ...td, fontFamily: "monospace" }}>{c.factor_name}</td>
                      <td style={td}>{c.source}</td>
                      {activeTab !== "pure_noise" && (
                        <>
                          <td style={{ ...td, fontFamily: "monospace", color: "#059669" }}>
                            {c.twin_kept || "-"}
                          </td>
                          <td style={td}>{fmt(c.twin_corr, 4)}</td>
                          <td style={td}>
                            {c.kept_is_immune ? (
                              <span style={{ color: "#059669", fontWeight: 600 }}>✓</span>
                            ) : (
                              ""
                            )}
                          </td>
                        </>
                      )}
                      <td style={td}>{fmt(c.rank_ic_mean, 4)}</td>
                      <td style={td}>{fmt(c.rank_ic_1d, 4)}</td>
                      <td style={td}>{fmt(c.rank_ic_5d, 4)}</td>
                      <td style={td}>{fmt(c.rank_ic_10d, 4)}</td>
                      <td style={td}>{fmt(c.rank_ic_20d, 4)}</td>
                      <td style={td}>{fmt(c.rank_icir_annualized, 3)}</td>
                      <td style={td}>{fmt(c.top_excess_sharpe, 2)}</td>
                      <td style={td}>{fmt(c.monthly_ic_trend_slope, 5)}</td>
                      <td style={td}>{fmt(c.ic_sign_consistency_12m, 2)}</td>
                      <td style={td}>{fmt(c.ic_oos_is_ratio, 2)}</td>
                      <td style={td}>{fmt(c.coverage, 2)}</td>
                      <td style={td}>
                        {c.v2_grade ? (
                          <span>
                            <b>{c.v2_grade}</b> {fmt(c.v2_score, 1)}
                          </span>
                        ) : (
                          "-"
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      )}

      {!result && !loading && (
        <div
          style={{
            background: "#fff",
            borderRadius: 12,
            padding: 64,
            textAlign: "center",
            color: "#9ca3af",
            fontSize: 14,
            boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
          }}
        >
          点击「🔍 开始分析」开始识别删除候选因子
        </div>
      )}
    </div>
  );
}

const th: React.CSSProperties = {
  padding: "10px 8px",
  textAlign: "left",
  fontSize: 11,
  fontWeight: 600,
  color: "#6b7280",
  textTransform: "uppercase",
  borderBottom: "1px solid #e5e7eb",
  whiteSpace: "nowrap",
};
const td: React.CSSProperties = {
  padding: "8px",
  whiteSpace: "nowrap",
};
