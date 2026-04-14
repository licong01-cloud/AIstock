"use client";

import React, { useState } from "react";

interface FactorEntry {
  name: string;
  gain: number;
  gain_pct: number;
  split: number;
  method?: string;  // "lightgbm_gain" | "pytorch_correlation"
}

interface FactorDetailData {
  name: string;
  expression?: string;
  description_cn?: string;
  source_task_id?: string;
  source_loop_tag?: string;
  factor_type?: string;
  data_source?: string;
  // Classification (from qe_factor_classification)
  category?: string;
  grade?: string;
  llm_analysis?: string;
  classification_description?: string;
  factor_dimension?: string;
  factor_profile?: any;
}

interface FactorAnalysisPanelProps {
  featureImportance: FactorEntry[];
}

const API = process.env.NEXT_PUBLIC_API_BASE ?? "http://127.0.0.1:8001/api/v1";

// Alpha158 ��子名集合（用于区分自定义 vs Alpha158）
const ALPHA158_NAMES = new Set([
  "KMID","KLEN","KMID2","KUP","KUP2","KLOW","KLOW2","KSFT","KSFT2",
  "OPEN0","HIGH0","LOW0","VWAP0","ROC5","ROC10","ROC20","ROC30","ROC60",
  "MA5","MA10","STD5","STD10","STD20","STD30","STD60",
  "CORR5","CORR10","CORR20","CORR30","CORR60",
  "CORD5","CORD10","CORD20","CORD30","CORD60",
  "RESI5","RESI10","RESI20","RESI30","RESI60",
  "VSTD5","VSTD10","VSTD20","VSTD30","VSTD60",
  "WVMA5","WVMA10","WVMA20","WVMA30","WVMA60",
  "RSQR5","RSQR10","RSQR20","RSQR30","RSQR60",
]);

const GRADE_COLORS: Record<string, { bg: string; text: string }> = {
  S: { bg: "#fef3c7", text: "#92400e" },
  A: { bg: "#dcfce7", text: "#166534" },
  B: { bg: "#dbeafe", text: "#1e40af" },
  C: { bg: "#f1f5f9", text: "#475569" },
  D: { bg: "#fef2f2", text: "#991b1b" },
};

function MetricCell({ label, value, pct, digits = 4 }: { label: string; value: any; pct?: boolean; digits?: number }) {
  const formatted = value != null && isFinite(value)
    ? (pct ? (value * 100).toFixed(2) + "%" : Number(value).toFixed(digits))
    : "-";
  return (
    <div style={{ textAlign: "center", padding: 8, backgroundColor: "#f8fafc", borderRadius: 6, border: "1px solid #e2e8f0" }}>
      <div style={{ fontSize: 10, color: "#94a3b8" }}>{label}</div>
      <div style={{ fontSize: 14, fontWeight: 700, fontFamily: "monospace", color: "#0f172a" }}>{formatted}</div>
    </div>
  );
}

function FactorDetailModal({ factorName, onClose }: { factorName: string; onClose: () => void }) {
  const [detail, setDetail] = useState<FactorDetailData | null>(null);
  const [indMetrics, setIndMetrics] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  React.useEffect(() => {
    setLoading(true);
    Promise.all([
      fetch(`${API}/rdagent/catalogs/factors/${encodeURIComponent(factorName)}`).then(r => r.ok ? r.json() : null).catch(() => null),
      fetch(`${API}/quantevolver/factors/${encodeURIComponent(factorName)}/independent-metrics`).then(r => r.ok ? r.json() : null).catch(() => null),
    ]).then(([detailRes, metricsRes]) => {
      // catalog API 直接返回 {...fields}（无 data wrapper）
      if (detailRes?.name) setDetail(detailRes);
      else if (detailRes?.data?.name) setDetail(detailRes.data);
      else setError("因子库中未找到该因子");

      // independent-metrics API 返回 {ok, metrics: [...], ...}
      // 取第一条（最新计算结果）
      if (metricsRes?.metrics?.length > 0) {
        setIndMetrics(metricsRes.metrics[0]);
      } else if (metricsRes?.data?.metrics?.length > 0) {
        setIndMetrics(metricsRes.data.metrics[0]);
      }
    }).finally(() => setLoading(false));
  }, [factorName]);

  const gc = detail?.grade ? (GRADE_COLORS[detail.grade] ?? GRADE_COLORS.C) : null;

  return (
    <div style={{ position: "fixed", inset: 0, zIndex: 9999, display: "flex", justifyContent: "center", alignItems: "center" }} onClick={onClose}>
      <div style={{ position: "absolute", inset: 0, backgroundColor: "rgba(0,0,0,0.4)" }} />
      <div style={{ position: "relative", backgroundColor: "#fff", borderRadius: 12, padding: 24, maxWidth: 780, width: "92%", maxHeight: "85vh", overflow: "auto", boxShadow: "0 20px 60px rgba(0,0,0,0.3)" }} onClick={e => e.stopPropagation()}>
        {/* Header */}
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <h3 style={{ margin: 0, fontSize: 16, fontWeight: 700, color: "#0f172a" }}>{factorName}</h3>
            {gc && detail?.grade && (
              <span style={{ padding: "2px 10px", borderRadius: 12, fontSize: 11, fontWeight: 700, backgroundColor: gc.bg, color: gc.text }}>
                {detail.grade}
              </span>
            )}
            {detail?.category && (
              <span style={{ padding: "2px 10px", borderRadius: 4, fontSize: 11, fontWeight: 600, backgroundColor: "#ede9fe", color: "#6d28d9" }}>
                {detail.category}
              </span>
            )}
          </div>
          <button onClick={onClose} style={{ border: "none", background: "none", fontSize: 20, cursor: "pointer", color: "#94a3b8" }}>x</button>
        </div>

        {loading && <div style={{ textAlign: "center", padding: 40, color: "#94a3b8" }}>加载中...</div>}
        {error && <div style={{ padding: 16, color: "#94a3b8", textAlign: "center" }}>{error}</div>}

        {detail && (
          <div style={{ display: "flex", flexDirection: "column", gap: 14, fontSize: 12 }}>
            {/* 因子分类标签 */}
            <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
              {detail.factor_type && (
                <span style={{ padding: "2px 8px", borderRadius: 4, fontSize: 11, backgroundColor: "#f0f9ff", color: "#0369a1", border: "1px solid #bae6fd" }}>
                  {detail.factor_type === "CrossSection" ? "截面因子" : detail.factor_type === "TimeSeries" ? "时序因子" : detail.factor_type}
                </span>
              )}
              {detail.data_source && (
                <span style={{ padding: "2px 8px", borderRadius: 4, fontSize: 11, backgroundColor: "#fefce8", color: "#854d0e", border: "1px solid #fef08a" }}>
                  {detail.data_source}
                </span>
              )}
              {detail.factor_dimension && (
                <span style={{ padding: "2px 8px", borderRadius: 4, fontSize: 11, backgroundColor: "#f0fdf4", color: "#166534", border: "1px solid #bbf7d0" }}>
                  {detail.factor_dimension}
                </span>
              )}
              {detail.source_task_id && (
                <span style={{ padding: "2px 8px", borderRadius: 4, fontSize: 11, backgroundColor: "#f1f5f9", color: "#64748b" }}>
                  来源: {detail.source_task_id}{detail.source_loop_tag ? ` / ${detail.source_loop_tag}` : ""}
                </span>
              )}
            </div>

            {/* LLM 分析描述 */}
            {detail.llm_analysis && (
              <div>
                <div style={{ fontWeight: 600, color: "#7c3aed", marginBottom: 4 }}>LLM 分析</div>
                <div style={{ color: "#334155", lineHeight: 1.6, backgroundColor: "#faf5ff", padding: 12, borderRadius: 6, border: "1px solid #e9d5ff", whiteSpace: "pre-wrap" }}>
                  {detail.llm_analysis}
                </div>
              </div>
            )}

            {/* 因子说明 (classification_description 或 description_cn) */}
            {(detail.classification_description || detail.description_cn) && (
              <div>
                <div style={{ fontWeight: 600, color: "#64748b", marginBottom: 4 }}>因子说明</div>
                <div style={{ color: "#334155", lineHeight: 1.5 }}>{detail.classification_description || detail.description_cn}</div>
              </div>
            )}

            {/* 因子表达式 */}
            {detail.expression && (
              <div>
                <div style={{ fontWeight: 600, color: "#64748b", marginBottom: 4 }}>因子表达式</div>
                <pre style={{ backgroundColor: "#fffbeb", padding: 10, borderRadius: 6, fontSize: 11, fontFamily: "monospace", whiteSpace: "pre-wrap", wordBreak: "break-all", border: "1px solid #fde68a", margin: 0 }}>{detail.expression}</pre>
              </div>
            )}
          </div>
        )}

        {/* 独立因子指标 */}
        {indMetrics && (
          <div style={{ marginTop: 16, display: "flex", flexDirection: "column", gap: 12 }}>
            {/* 核心 IC 指标 */}
            <div>
              <div style={{ fontWeight: 600, color: "#64748b", marginBottom: 8, fontSize: 12 }}>独立因子指标</div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 8 }}>
                <MetricCell label="IC" value={indMetrics.ic_mean} />
                <MetricCell label="Rank IC" value={indMetrics.rank_ic_mean} />
                <MetricCell label="ICIR" value={indMetrics.icir} />
                <MetricCell label="Rank ICIR" value={indMetrics.rank_icir} />
                <MetricCell label="IC 胜率" value={indMetrics.ic_positive_ratio} pct />
              </div>
            </div>

            {/* 多持有期 Rank IC */}
            {(indMetrics.rank_ic_1d != null || indMetrics.rank_ic_5d != null) && (
              <div>
                <div style={{ fontWeight: 600, color: "#64748b", marginBottom: 8, fontSize: 12 }}>多持有期 Rank IC</div>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 8 }}>
                  <MetricCell label="1D" value={indMetrics.rank_ic_1d} />
                  <MetricCell label="5D" value={indMetrics.rank_ic_5d} />
                  <MetricCell label="10D" value={indMetrics.rank_ic_10d} />
                  <MetricCell label="20D" value={indMetrics.rank_ic_20d} />
                </div>
              </div>
            )}

            {/* 组合表现 */}
            <div>
              <div style={{ fontWeight: 600, color: "#64748b", marginBottom: 8, fontSize: 12 }}>多头组合表现</div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 8 }}>
                <MetricCell label="年化收益" value={indMetrics.top_annual_return} pct />
                <MetricCell label="超额年化" value={indMetrics.top_excess_annual_return} pct />
                <MetricCell label="夏普" value={indMetrics.top_sharpe} digits={2} />
                <MetricCell label="最大回撤" value={indMetrics.top_max_drawdown} pct />
                <MetricCell label="单调性" value={indMetrics.group_return_monotonicity} digits={3} />
              </div>
            </div>

            {/* 其他指标 */}
            <div>
              <div style={{ fontWeight: 600, color: "#64748b", marginBottom: 8, fontSize: 12 }}>其他特征</div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 8 }}>
                <MetricCell label="换手率" value={indMetrics.turnover} digits={3} />
                <MetricCell label="IC 半衰期" value={indMetrics.ic_decay_half_life} digits={1} />
                <MetricCell label="覆盖率" value={indMetrics.coverage} pct />
                <MetricCell label="交易日数" value={indMetrics.n_trading_days} digits={0} />
              </div>
            </div>

            {/* 评估窗口 & 日期范围 */}
            <div style={{ fontSize: 10, color: "#94a3b8", display: "flex", gap: 16 }}>
              {indMetrics.eval_window && <span>窗口: {indMetrics.eval_window}</span>}
              {indMetrics.data_start && <span>数据: {indMetrics.data_start} ~ {indMetrics.data_end}</span>}
              {indMetrics.calc_engine && <span>引擎: {indMetrics.calc_engine}</span>}
            </div>
          </div>
        )}

        {/* 无数据兜底 */}
        {!loading && !indMetrics && !error && (
          <div style={{ marginTop: 16, padding: 20, textAlign: "center", color: "#94a3b8", fontSize: 12, backgroundColor: "#f8fafc", borderRadius: 6 }}>
            暂无独立因子指标数据（因子可能尚未完成指标计算）
          </div>
        )}
      </div>
    </div>
  );
}

export function FactorAnalysisPanel({ featureImportance }: FactorAnalysisPanelProps) {
  const [showAll, setShowAll] = useState(false);
  const [selectedFactor, setSelectedFactor] = useState<string | null>(null);

  if (!featureImportance || featureImportance.length === 0) return null;

  const maxGainPct = featureImportance[0]?.gain_pct ?? 1;
  const displayed = showAll ? featureImportance : featureImportance.slice(0, 20);
  const method = featureImportance[0]?.method || "lightgbm_gain";
  const methodLabel =
    method === "pytorch_correlation"
      ? "基于因子与预测相关性 (PyTorch)"
      : "基于 LightGBM Gain";
  const hasSplitCount = method === "lightgbm_gain";

  return (
    <div style={{ backgroundColor: "#fff", borderRadius: 8, border: "1px solid #e2e8f0", padding: 20 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
        <h3 style={{ margin: 0, fontSize: 13, fontWeight: 700, color: "#334155", textTransform: "uppercase", letterSpacing: "0.05em" }}>
          因子贡献度 (Feature Importance)
        </h3>
        <span style={{ fontSize: 11, color: "#94a3b8" }}>
          共 {featureImportance.length} 个因子 · {methodLabel}
        </span>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
        {displayed.map((f, i) => {
          const barWidth = Math.max((f.gain_pct / maxGainPct) * 100, 1);
          const isAlpha158 = ALPHA158_NAMES.has(f.name);
          const barColor = isAlpha158 ? "#94a3b8" : "#3b82f6";

          return (
            <div key={f.name} style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 11, fontFamily: "monospace" }}>
              <span style={{ width: 24, textAlign: "right", color: "#94a3b8", flexShrink: 0 }}>{i + 1}</span>
              <span
                style={{ width: 280, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", color: isAlpha158 ? "#94a3b8" : "#2563eb", fontWeight: i < 5 ? 600 : 400, flexShrink: 0, cursor: isAlpha158 ? "default" : "pointer", textDecoration: isAlpha158 ? "none" : "underline", textDecorationColor: "#93c5fd" }}
                title={f.name}
                onClick={() => { if (!isAlpha158) setSelectedFactor(f.name); }}
              >
                {f.name}
              </span>
              <div style={{ flex: 1, height: 16, backgroundColor: "#f1f5f9", borderRadius: 3, overflow: "hidden" }}>
                <div style={{ width: `${barWidth}%`, height: "100%", backgroundColor: barColor, borderRadius: 3, transition: "width 0.3s" }} />
              </div>
              <span style={{ width: 55, textAlign: "right", color: "#334155", fontWeight: 600, flexShrink: 0 }}>
                {f.gain_pct.toFixed(1)}%
              </span>
              {hasSplitCount && (
                <span style={{ width: 50, textAlign: "right", color: "#94a3b8", flexShrink: 0 }}>
                  {f.split}次
                </span>
              )}
            </div>
          );
        })}
      </div>

      {featureImportance.length > 20 && (
        <button
          onClick={() => setShowAll(!showAll)}
          style={{ marginTop: 12, padding: "6px 16px", fontSize: 11, cursor: "pointer", borderRadius: 4, border: "1px solid #e2e8f0", backgroundColor: "#f8fafc", color: "#475569" }}
        >
          {showAll ? "收起" : `展开全部 ${featureImportance.length} 个因子`}
        </button>
      )}

      <div style={{ marginTop: 12, fontSize: 10, color: "#94a3b8", display: "flex", gap: 16, flexWrap: "wrap" }}>
        <span><span style={{ display: "inline-block", width: 10, height: 10, backgroundColor: "#3b82f6", borderRadius: 2, marginRight: 4 }} />自定义因子 (点击查看详情)</span>
        <span><span style={{ display: "inline-block", width: 10, height: 10, backgroundColor: "#94a3b8", borderRadius: 2, marginRight: 4 }} />Alpha158 因子</span>
        {method === "pytorch_correlation" ? (
          <span>Gain%: 该因子与模型预测输出的|相关性|占比 (PyTorch 模型权重代理)</span>
        ) : (
          <span>Gain%: 该因子在模型决策树中的信息增益占比</span>
        )}
      </div>

      {selectedFactor && (
        <FactorDetailModal factorName={selectedFactor} onClose={() => setSelectedFactor(null)} />
      )}
    </div>
  );
}
