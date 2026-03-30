"use client";

import React, { useEffect, useState, useCallback } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

type ExecutionAlgo = {
  id: number;
  algo_code: string;
  algo_name: string;
  algo_type: string;
  description?: string;
  source?: string;
  source_code?: string;
  default_config?: Record<string, any>;
  param_schema?: Record<string, any>;
  supported_freqs?: string[];
  min_bars?: number;
  category?: string;
  category_reason?: string;
  grade?: string;
  grade_score?: number;
  analysis_profile?: any;
  llm_analysis_at?: string;
  is_enabled?: boolean;
  sort_order?: number;
  created_at?: string;
  updated_at?: string;
};

const CATEGORY_COLORS: Record<string, { bg: string; text: string }> = {
  SCHEDULE:   { bg: "#dbeafe", text: "#1e40af" },
  ADAPTIVE:   { bg: "#ede9fe", text: "#6d28d9" },
  PASSIVE:    { bg: "#d1fae5", text: "#065f46" },
  AGGRESSIVE: { bg: "#fee2e2", text: "#991b1b" },
  HYBRID:     { bg: "#fef3c7", text: "#92400e" },
};

const CATEGORY_NAMES: Record<string, string> = {
  SCHEDULE:   "定时调度",
  ADAPTIVE:   "自适应",
  PASSIVE:    "被动跟随",
  AGGRESSIVE: "激进执行",
  HYBRID:     "混合",
};

const GRADE_COLORS: Record<string, string> = {
  S: "#d97706",
  A: "#059669",
  B: "#2563eb",
  C: "#9ca3af",
  D: "#ef4444",
};

const SCORE_DIMS: { key: string; label: string }[] = [
  { key: "execution_quality", label: "执行质量" },
  { key: "adaptiveness", label: "自适应" },
  { key: "data_feasibility", label: "数据可行" },
  { key: "complexity_benefit", label: "复杂收益" },
  { key: "a_share_suitability", label: "A股适用" },
  { key: "robustness", label: "鲁棒性" },
];

export default function ExecutionAlgoList() {
  const [algos, setAlgos] = useState<ExecutionAlgo[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedCode, setExpandedCode] = useState<string | null>(null);
  const [categoryFilter, setCategoryFilter] = useState("");
  const [gradeFilter, setGradeFilter] = useState("");

  // 批量分析
  const [batchLoading, setBatchLoading] = useState(false);
  const [batchProgress, setBatchProgress] = useState<{
    current: number; total: number; algo_name?: string;
  } | null>(null);
  const [batchResult, setBatchResult] = useState<{
    total?: number; analyzed?: number; errors?: string[];
  } | null>(null);

  // 单个分析中
  const [analyzingCode, setAnalyzingCode] = useState<string | null>(null);

  const loadAlgos = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (categoryFilter) params.set("category", categoryFilter);
      if (gradeFilter) params.set("grade", gradeFilter);
      const res = await fetch(`${API}/quantevolver/execution-algorithms?${params.toString()}`);
      const data = await res.json();
      setAlgos(data.items || []);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    }
    setLoading(false);
  }, [categoryFilter, gradeFilter]);

  useEffect(() => { loadAlgos(); }, [loadAlgos]);

  // ---- 单个分析 ----
  async function analyzeOne(algoCode: string) {
    setAnalyzingCode(algoCode);
    try {
      const res = await fetch(
        `${API}/quantevolver/execution-algorithms/${encodeURIComponent(algoCode)}/analyze`,
        { method: "POST" },
      );
      if (!res.ok) {
        const data = await res.json();
        alert("分析失败: " + (data.detail || res.statusText));
      } else {
        await loadAlgos();
      }
    } catch (e: any) {
      alert("分析失败: " + (e?.message || "网络错误"));
    }
    setAnalyzingCode(null);
  }

  // ---- 批量分析（SSE）----
  async function batchAnalyze() {
    if (!confirm("确定要批量分析所有执行算法吗？")) return;
    setBatchLoading(true);
    setBatchProgress(null);
    setBatchResult(null);

    try {
      const res = await fetch(`${API}/quantevolver/execution-algorithms/batch-analyze`);
      const reader = res.body?.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (reader) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";

        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          try {
            const event = JSON.parse(line.slice(6));
            if (event.type === "progress") {
              setBatchProgress({ current: event.current, total: event.total, algo_name: event.algo_name });
            } else if (event.type === "done") {
              setBatchResult({ total: event.total, analyzed: event.analyzed, errors: event.errors });
              loadAlgos();
            }
          } catch {}
        }
      }
    } catch (e: any) {
      alert("批量分析失败: " + (e?.message || ""));
    }

    setBatchLoading(false);
    setBatchProgress(null);
  }

  // ---- 启用/禁用切换 ----
  async function toggleEnabled(algo: ExecutionAlgo) {
    try {
      const res = await fetch(
        `${API}/quantevolver/execution-algorithms/${encodeURIComponent(algo.algo_code)}`,
        {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ is_enabled: !algo.is_enabled }),
        },
      );
      if (res.ok) loadAlgos();
    } catch {}
  }

  const filtered = algos;

  const thStyle: React.CSSProperties = {
    padding: "8px 10px", fontWeight: 600, color: "#4b5563", background: "#f9fafb",
    whiteSpace: "nowrap", userSelect: "none",
  };
  const tdStyle: React.CSSProperties = { padding: "8px 10px" };

  return (
    <div style={{ padding: 24 }}>
      {/* 标题横幅 */}
      <section
        style={{
          background: "linear-gradient(135deg, #8b5cf6 0%, #6366f1 100%)",
          borderRadius: 16, padding: 20, color: "#fff", marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>执行算法库</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          管理日内执行算法，查看LLM分析的分类、评级、适用场景和A股注意事项
        </p>
      </section>

      {/* 筛选栏 */}
      <section style={{ background: "#fff", borderRadius: 12, padding: 16, marginBottom: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
        <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
          <select
            value={categoryFilter}
            onChange={e => setCategoryFilter(e.target.value)}
            style={{ padding: "6px 10px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db" }}
          >
            <option value="">全部分类</option>
            {Object.entries(CATEGORY_NAMES).map(([k, v]) => (
              <option key={k} value={k}>{v}</option>
            ))}
          </select>

          <select
            value={gradeFilter}
            onChange={e => setGradeFilter(e.target.value)}
            style={{ padding: "6px 10px", fontSize: 12, borderRadius: 6, border: "1px solid #d1d5db" }}
          >
            <option value="">全部评级</option>
            {["S", "A", "B", "C", "D"].map(g => (
              <option key={g} value={g}>{g}</option>
            ))}
          </select>

          <button onClick={loadAlgos} disabled={loading}
            style={{ padding: "6px 12px", fontSize: 12, cursor: "pointer", borderRadius: 6, border: "1px solid #d1d5db", background: "#fff" }}>
            {loading ? "加载中..." : "刷新"}
          </button>

          <button
            onClick={batchAnalyze}
            disabled={batchLoading}
            style={{
              padding: "6px 14px", fontSize: 12,
              cursor: batchLoading ? "not-allowed" : "pointer",
              borderRadius: 6, border: "none", background: "#6366f1", color: "#fff", fontWeight: 600,
              opacity: batchLoading ? 0.5 : 1,
            }}
          >
            {batchLoading ? "分析中..." : "批量LLM分析"}
          </button>

          <span style={{ fontSize: 12, color: "#9ca3af" }}>
            共 {filtered.length} 个算法
          </span>
        </div>

        {/* 批量分析进度条 */}
        {batchProgress && (
          <div style={{ marginTop: 8, padding: 10, borderRadius: 6, fontSize: 12, background: "#ede9fe", color: "#5b21b6" }}>
            <div style={{ marginBottom: 4 }}>
              <strong>批量分析进行中：</strong> {batchProgress.current} / {batchProgress.total}
              {batchProgress.algo_name && <span style={{ marginLeft: 8, color: "#7c3aed" }}>当前: {batchProgress.algo_name}</span>}
            </div>
            <div style={{ width: "100%", height: 8, background: "#e5e7eb", borderRadius: 4, overflow: "hidden" }}>
              <div style={{ width: `${(batchProgress.current / batchProgress.total) * 100}%`, height: "100%", background: "#6366f1", transition: "width 0.3s" }} />
            </div>
          </div>
        )}

        {/* 批量分析结果 */}
        {batchResult && (
          <div style={{
            marginTop: 8, padding: 10, borderRadius: 6, fontSize: 12,
            background: (batchResult.errors?.length || 0) > 0 ? "#fef3c7" : "#d1fae5",
            color: (batchResult.errors?.length || 0) > 0 ? "#92400e" : "#065f46",
          }}>
            <strong>批量分析完成：</strong>
            共 {batchResult.total} 个算法，成功分析 {batchResult.analyzed} 个
            {(batchResult.errors?.length || 0) > 0 && (
              <span>，{batchResult.errors!.length} 个失败</span>
            )}
          </div>
        )}

        {error && <div style={{ marginTop: 8, padding: 8, background: "#fee2e2", borderRadius: 6, fontSize: 12 }}>{error}</div>}
      </section>

      {/* 表格 */}
      <section style={{ background: "#fff", borderRadius: 12, boxShadow: "0 1px 3px rgba(0,0,0,0.08)", overflow: "hidden" }}>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>
                <th style={thStyle}>算法名称</th>
                <th style={thStyle}>代码</th>
                <th style={thStyle}>分类</th>
                <th style={thStyle}>评级</th>
                <th style={thStyle}>综合分</th>
                <th style={thStyle}>来源</th>
                <th style={thStyle}>支持频率</th>
                <th style={thStyle}>状态</th>
                <th style={thStyle}>操作</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map(algo => {
                const isExpanded = expandedCode === algo.algo_code;
                const catColor = CATEGORY_COLORS[algo.category || ""] || { bg: "#f3f4f6", text: "#6b7280" };

                return (
                  <React.Fragment key={algo.algo_code}>
                    {/* 主行 */}
                    <tr style={{ borderBottom: isExpanded ? "none" : "1px solid #f3f4f6" }}>
                      <td style={{ ...tdStyle, fontWeight: 600 }}>
                        {algo.algo_name}
                      </td>
                      <td style={{ ...tdStyle, fontFamily: "monospace", fontSize: 11, color: "#6b7280" }}>
                        {algo.algo_code}
                      </td>
                      <td style={tdStyle}>
                        {algo.category ? (
                          <span style={{
                            fontSize: 10, padding: "2px 8px", borderRadius: 4,
                            background: catColor.bg, color: catColor.text,
                            fontWeight: 600, whiteSpace: "nowrap",
                          }}>
                            {CATEGORY_NAMES[algo.category] || algo.category}
                          </span>
                        ) : (
                          <span style={{ color: "#d1d5db", fontSize: 11 }}>未分析</span>
                        )}
                      </td>
                      <td style={tdStyle}>
                        {algo.grade ? (
                          <span style={{
                            padding: "2px 8px", borderRadius: 4, fontSize: 11, fontWeight: 700,
                            color: "#fff",
                            background: GRADE_COLORS[algo.grade] || "#9ca3af",
                          }}>
                            {algo.grade}
                          </span>
                        ) : (
                          <span style={{ color: "#d1d5db", fontSize: 11 }}>-</span>
                        )}
                      </td>
                      <td style={{
                        ...tdStyle, fontFamily: "monospace", fontSize: 11, fontWeight: 600,
                        color: algo.grade_score != null ? "#374151" : "#d1d5db",
                      }}>
                        {algo.grade_score != null ? algo.grade_score : "-"}
                      </td>
                      <td style={tdStyle}>
                        <span style={{
                          fontSize: 10, padding: "2px 6px", borderRadius: 4,
                          background: algo.source === "qlib" ? "#ede9fe" : algo.source === "custom" ? "#dbeafe" : "#f3f4f6",
                          color: algo.source === "qlib" ? "#7c3aed" : algo.source === "custom" ? "#2563eb" : "#6b7280",
                        }}>
                          {algo.source || "builtin"}
                        </span>
                      </td>
                      <td style={{ ...tdStyle, fontSize: 10, color: "#6b7280" }}>
                        {(algo.supported_freqs || []).join(", ")}
                      </td>
                      <td style={tdStyle}>
                        <span
                          onClick={() => toggleEnabled(algo)}
                          style={{
                            fontSize: 10, padding: "2px 8px", borderRadius: 4, cursor: "pointer",
                            background: algo.is_enabled ? "#d1fae5" : "#fee2e2",
                            color: algo.is_enabled ? "#059669" : "#dc2626",
                            fontWeight: 600,
                          }}
                        >
                          {algo.is_enabled ? "启用" : "禁用"}
                        </span>
                      </td>
                      <td style={tdStyle}>
                        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                          <span
                            onClick={() => setExpandedCode(isExpanded ? null : algo.algo_code)}
                            style={{ color: "#6366f1", cursor: "pointer", fontSize: 11, borderBottom: "1px dashed #6366f1", userSelect: "none" }}
                          >
                            {isExpanded ? "收起" : "展开"}
                          </span>
                          <span
                            onClick={() => analyzingCode !== algo.algo_code && analyzeOne(algo.algo_code)}
                            style={{
                              color: analyzingCode === algo.algo_code ? "#9ca3af" : "#2563eb",
                              cursor: analyzingCode === algo.algo_code ? "wait" : "pointer",
                              fontSize: 11, borderBottom: "1px dashed", userSelect: "none",
                            }}
                          >
                            {analyzingCode === algo.algo_code ? "分析中..." : "分析"}
                          </span>
                        </div>
                      </td>
                    </tr>

                    {/* 展开详情行 */}
                    {isExpanded && (
                      <tr style={{ background: "#faf5ff" }}>
                        <td colSpan={9} style={{ padding: 0 }}>
                          <AlgoDetail algo={algo} />
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>

        {!loading && filtered.length === 0 && (
          <div style={{ textAlign: "center", padding: 60, color: "#9ca3af" }}>
            暂无执行算法数据。请先运行数据库初始化。
          </div>
        )}
      </section>
    </div>
  );
}

// ---- 展开详情子组件 ----
function AlgoDetail({ algo }: { algo: ExecutionAlgo }) {
  const profile = algo.analysis_profile || {};
  const scores = profile.scores || {};

  return (
    <div style={{
      borderLeft: "3px solid #6366f1",
      padding: "16px 20px",
      fontSize: 12, lineHeight: 1.7,
    }}>
      {/* 算法描述 */}
      {algo.description && (
        <div style={{ marginBottom: 12 }}>
          <strong style={{ color: "#374151" }}>算法描述</strong>
          <div style={{ marginTop: 4, color: "#6b7280", whiteSpace: "pre-wrap", background: "#f9fafb", padding: "8px 12px", borderRadius: 6 }}>
            {algo.description}
          </div>
        </div>
      )}

      {/* 核心公式 */}
      {algo.source_code && (
        <div style={{ marginBottom: 12, borderTop: "1px dashed #e5e7eb", paddingTop: 12 }}>
          <strong style={{ color: "#374151" }}>核心公式/伪代码</strong>
          <pre style={{
            marginTop: 4, padding: 10, borderRadius: 6, fontSize: 11,
            background: "#1e293b", color: "#e2e8f0", overflow: "auto",
            maxHeight: 200, lineHeight: 1.4,
          }}>
            {algo.source_code}
          </pre>
        </div>
      )}

      {/* 默认参数 */}
      {algo.default_config && Object.keys(algo.default_config).length > 0 && (
        <div style={{ marginBottom: 12, borderTop: "1px dashed #e5e7eb", paddingTop: 12 }}>
          <strong style={{ color: "#374151" }}>默认参数</strong>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 6 }}>
            {Object.entries(algo.default_config).map(([k, v]) => (
              <span key={k} style={{
                fontSize: 10, padding: "3px 8px", borderRadius: 4,
                background: "#f3f4f6", color: "#374151", fontFamily: "monospace",
              }}>
                {k}={JSON.stringify(v)}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* 分类理由 */}
      {algo.category_reason && (
        <div style={{ marginBottom: 12, borderTop: "1px dashed #e5e7eb", paddingTop: 12 }}>
          <strong style={{ color: "#374151" }}>分类理由</strong>
          <div style={{ marginTop: 4, color: "#6b7280" }}>{algo.category_reason}</div>
        </div>
      )}

      {/* 6 维评分 */}
      {Object.keys(scores).length > 0 && (
        <div style={{ marginBottom: 12, borderTop: "1px dashed #e5e7eb", paddingTop: 12 }}>
          <strong style={{ color: "#374151" }}>6 维评分</strong>
          <div style={{
            display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(120px, 1fr))",
            gap: 8, marginTop: 8,
          }}>
            {SCORE_DIMS.map(dim => {
              const val = scores[dim.key];
              return (
                <div key={dim.key} style={{
                  textAlign: "center", padding: "8px 6px",
                  background: "#f0fdf4", borderRadius: 6, border: "1px solid #e5e7eb",
                }}>
                  <div style={{ fontSize: 16, fontWeight: 700, color: scoreColor(val) }}>
                    {val != null ? val : "-"}
                  </div>
                  <div style={{ fontSize: 10, color: "#9ca3af", marginTop: 2 }}>{dim.label}</div>
                  {val != null && (
                    <div style={{
                      marginTop: 4, height: 4, background: "#e5e7eb", borderRadius: 2, overflow: "hidden",
                    }}>
                      <div style={{
                        width: `${val}%`, height: "100%",
                        background: scoreColor(val),
                        transition: "width 0.3s",
                      }} />
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* 适用场景 / 优势 / 劣势 */}
      {(profile.applicable_scenarios?.length > 0 || profile.advantages?.length > 0 || profile.disadvantages?.length > 0) && (
        <div style={{ marginBottom: 12, borderTop: "1px dashed #e5e7eb", paddingTop: 12 }}>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12 }}>
            {profile.applicable_scenarios?.length > 0 && (
              <div>
                <strong style={{ color: "#374151", fontSize: 11 }}>适用场景</strong>
                <ul style={{ margin: "4px 0 0 16px", padding: 0, color: "#6b7280", fontSize: 11 }}>
                  {profile.applicable_scenarios.map((s: string, i: number) => <li key={i}>{s}</li>)}
                </ul>
              </div>
            )}
            {profile.advantages?.length > 0 && (
              <div>
                <strong style={{ color: "#059669", fontSize: 11 }}>优势</strong>
                <ul style={{ margin: "4px 0 0 16px", padding: 0, color: "#6b7280", fontSize: 11 }}>
                  {profile.advantages.map((s: string, i: number) => <li key={i}>{s}</li>)}
                </ul>
              </div>
            )}
            {profile.disadvantages?.length > 0 && (
              <div>
                <strong style={{ color: "#dc2626", fontSize: 11 }}>劣势</strong>
                <ul style={{ margin: "4px 0 0 16px", padding: 0, color: "#6b7280", fontSize: 11 }}>
                  {profile.disadvantages.map((s: string, i: number) => <li key={i}>{s}</li>)}
                </ul>
              </div>
            )}
          </div>
        </div>
      )}

      {/* best_for / avoid_for */}
      {(profile.best_for?.length > 0 || profile.avoid_for?.length > 0) && (
        <div style={{ marginBottom: 12, borderTop: "1px dashed #e5e7eb", paddingTop: 12 }}>
          <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
            {profile.best_for?.length > 0 && (
              <div>
                <strong style={{ color: "#059669", fontSize: 11 }}>最适合</strong>
                <div style={{ display: "flex", gap: 4, flexWrap: "wrap", marginTop: 4 }}>
                  {profile.best_for.map((s: string, i: number) => (
                    <span key={i} style={{ fontSize: 10, padding: "2px 6px", borderRadius: 4, background: "#d1fae5", color: "#065f46" }}>{s}</span>
                  ))}
                </div>
              </div>
            )}
            {profile.avoid_for?.length > 0 && (
              <div>
                <strong style={{ color: "#dc2626", fontSize: 11 }}>应避免</strong>
                <div style={{ display: "flex", gap: 4, flexWrap: "wrap", marginTop: 4 }}>
                  {profile.avoid_for.map((s: string, i: number) => (
                    <span key={i} style={{ fontSize: 10, padding: "2px 6px", borderRadius: 4, background: "#fee2e2", color: "#991b1b" }}>{s}</span>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* A股注意事项 */}
      {profile.a_share_notes && (
        <div style={{ marginBottom: 12, borderTop: "1px dashed #e5e7eb", paddingTop: 12 }}>
          <strong style={{ color: "#374151" }}>A股特殊注意事项</strong>
          <div style={{
            marginTop: 4, color: "#92400e", background: "#fffbeb", padding: "8px 12px", borderRadius: 6, fontSize: 11,
          }}>
            {profile.a_share_notes}
          </div>
        </div>
      )}

      {/* 使用建议 */}
      {profile.usage_guidance && (
        <div style={{ marginBottom: 12, borderTop: "1px dashed #e5e7eb", paddingTop: 12 }}>
          <strong style={{ color: "#374151" }}>使用建议</strong>
          <div style={{ marginTop: 4, color: "#6b7280", fontSize: 11 }}>{profile.usage_guidance}</div>
        </div>
      )}

      {/* 完整 JSON */}
      {algo.analysis_profile && typeof algo.analysis_profile === "object" && Object.keys(algo.analysis_profile).length > 0 && (
        <details style={{ marginBottom: 12, borderTop: "1px dashed #e5e7eb", paddingTop: 12 }}>
          <summary style={{ fontSize: 11, color: "#6366f1", cursor: "pointer" }}>
            查看完整分析 JSON
          </summary>
          <pre style={{
            marginTop: 4, padding: 10, borderRadius: 6, fontSize: 10,
            background: "#1e293b", color: "#e2e8f0", overflow: "auto",
            maxHeight: 300, lineHeight: 1.4,
          }}>
            {JSON.stringify(algo.analysis_profile, null, 2)}
          </pre>
        </details>
      )}

      {/* 元信息 */}
      <div style={{ borderTop: "1px dashed #e5e7eb", paddingTop: 12, fontSize: 11, color: "#9ca3af" }}>
        <div>最低Bar数: {algo.min_bars ?? "-"} | 排序: {algo.sort_order ?? 0}</div>
        {algo.llm_analysis_at && <div>最近分析: {algo.llm_analysis_at}</div>}
        {algo.created_at && <div>创建时间: {algo.created_at}</div>}
      </div>

      {/* 收起按钮 */}
      <div style={{ marginTop: 12, textAlign: "right" }}>
        <button
          onClick={() => { /* parent handles via expandedCode */ }}
          style={{
            padding: "4px 16px", fontSize: 11, borderRadius: 4,
            border: "1px solid #d1d5db", background: "#fff", cursor: "pointer", color: "#6b7280",
          }}
        >
          收起
        </button>
      </div>
    </div>
  );
}

function scoreColor(val: number | undefined): string {
  if (val == null) return "#9ca3af";
  if (val >= 80) return "#059669";
  if (val >= 60) return "#2563eb";
  if (val >= 40) return "#d97706";
  return "#ef4444";
}
