"use client";

import React, { useEffect, useState, useMemo } from "react";
import { useParams } from "next/navigation";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

// ── Types ──────────────────────────────────────────────────────────

interface GroupMetrics {
  group_name: string;
  factor_count: number;
  model_id: string;
  dataset_type: string;
  compute_resource: string;
  group_ic: number | null;
  group_icir: number | null;
  group_sharpe: number | null;
  meta_weight: number | null;
  status: string;
  assigned_node_id: string | null;
  factor_names: string[];
}

interface Bottleneck {
  rule_id: string;
  severity: string;
  message: string;
  affected_groups: string[];
  recommendation: string;
  action_type: string;
  action_params: Record<string, any>;
}

interface OptimizationGuidance {
  rule_id: string;
  severity: string;
  message: string;
  recommendation: string;
  action_type: string;
  affected_groups: string[];
  evidence?: Record<string, any>;
  source_fields?: string[];
}

interface GroupDiagnostic {
  group_name: string;
  contribution_to_combined_ic?: number | null;
  data_available?: Record<string, boolean>;
  ic_diagnostics?: Record<string, any>;
  training_diagnostics?: Record<string, any>;
  prediction_diagnostics?: Record<string, any>;
  feature_importance_top?: Array<Record<string, any>>;
}

interface DiagnosticsData {
  experiment_id: string;
  groups: GroupMetrics[];
  meta_method: string | null;
  execution_mode: string | null;
  correlations: Record<string, number>;
  bottlenecks: Bottleneck[];
  recommendations: Bottleneck[];
  combined_ic: number | null;
  combined_vs_groups?: Record<string, any>;
  portfolio_diagnostics?: Record<string, any>;
  diversification?: Record<string, any>;
  group_diagnostics?: GroupDiagnostic[];
  data_availability?: Record<string, any>;
  ic_quality?: Record<string, any>;
  optimization_guidance?: OptimizationGuidance[];
}

// ── Styles ─────────────────────────────────────────────────────────

const cardStyle: React.CSSProperties = {
  backgroundColor: "#ffffff",
  borderRadius: "12px",
  boxShadow: "0 4px 12px rgba(0, 0, 0, 0.05)",
  border: "1px solid rgba(255, 255, 255, 0.2)",
  overflow: "hidden",
};

const headerStyle: React.CSSProperties = {
  padding: "16px 20px",
  borderBottom: "1px solid #f1f5f9",
  backgroundColor: "#f8fafc",
};

function fmtNum(value: any, digits = 4) {
  return typeof value === "number" && Number.isFinite(value) ? value.toFixed(digits) : "-";
}

function fmtPct(value: any, digits = 1) {
  return typeof value === "number" && Number.isFinite(value) ? `${(value * 100).toFixed(digits)}%` : "-";
}

function severityStyle(severity: string): React.CSSProperties {
  if (severity === "high") return { backgroundColor: "#fef2f2", borderColor: "#fecaca", color: "#991b1b" };
  if (severity === "medium") return { backgroundColor: "#fffbeb", borderColor: "#fed7aa", color: "#92400e" };
  return { backgroundColor: "#f0fdf4", borderColor: "#bbf7d0", color: "#166534" };
}

// ── Component ──────────────────────────────────────────────────────

export default function MultiAlphaDiagnosticsPage() {
  const params = useParams();
  const expId = params.expId as string;

  const [data, setData] = useState<DiagnosticsData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!expId) return;
    setLoading(true);
    fetch(`${API}/quantevolver/multi-alpha/${expId}/diagnostics`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}: ${r.statusText}`);
        return r.json();
      })
      .then((d) => {
        if (d.ok && d.diagnostics) setData(d.diagnostics);
        else setError(d.error || d.detail || "诊断数据格式异常");
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [expId]);

  // Parse correlation pairs for heatmap
  const corrPairs = useMemo(() => {
    if (!data?.correlations) return [];
    return Object.entries(data.correlations).map(([key, val]) => {
      const [a, b] = key.split("|");
      return { group_a: a, group_b: b, correlation: val };
    });
  }, [data]);

  const uniqueGroupNames = useMemo(
    () => data?.groups.map((g) => g.group_name) || [],
    [data]
  );

  const groupDiagByName = useMemo(() => {
    const m = new Map<string, GroupDiagnostic>();
    (data?.group_diagnostics || []).forEach((g) => {
      if (g.group_name) m.set(g.group_name, g);
    });
    return m;
  }, [data]);

  const availability = data?.data_availability || {};
  const portfolio = data?.portfolio_diagnostics || {};
  const diversification = data?.diversification || {};
  const icQualityEntries = Object.entries(data?.ic_quality || {});
  const optimizationGuidance = data?.optimization_guidance || [];

  if (loading)
    return (
      <div style={{ padding: 40, textAlign: "center", color: "#94a3b8" }}>
        加载诊断数据中...
      </div>
    );
  if (error)
    return (
      <div style={{ padding: 40, textAlign: "center", color: "#ef4444" }}>
        {error}
      </div>
    );
  if (!data) return null;

  return (
    <div
      style={{
        padding: "24px",
        maxWidth: "1400px",
        margin: "0 auto",
        fontFamily:
          "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif",
      }}
    >
      {/* 标题 */}
      <div style={{ marginBottom: "24px" }}>
        <h1
          style={{
            margin: 0,
            fontSize: "24px",
            fontWeight: 700,
            color: "#ffffff",
            textShadow: "0 1px 3px rgba(0,0,0,0.15)",
          }}
        >
          Multi-Alpha 诊断面板
        </h1>
        <p
          style={{
            margin: "8px 0 0",
            fontSize: "14px",
            color: "rgba(255,255,255,0.75)",
          }}
        >
          实验 {expId} | Meta: {data.meta_method || "-"} | 执行:{" "}
          {data.execution_mode || "-"} | Combined IC:{" "}
          {data.combined_ic != null ? data.combined_ic.toFixed(4) : "-"}
        </p>
      </div>

      {/* 0. 统一分析覆盖率 + 组合回测诊断 */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: "24px",
          marginBottom: "24px",
        }}
      >
        <div style={cardStyle}>
          <div style={headerStyle}>
            <h2 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#1e293b" }}>
              统一分析数据覆盖率
            </h2>
          </div>
          <div style={{ padding: "20px", display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px" }}>
            {[
              ["组合增强指标", availability.combined_enhanced_metrics ? "已入库" : "缺失"],
              ["组合收益曲线", availability.combined_return_curves ? "已入库" : "缺失"],
              ["组合交易诊断", availability.combined_trade_diagnostics ? "已入库" : "缺失"],
              ["组合预测诊断", availability.combined_prediction_diagnostics ? "已入库" : "缺失"],
              ["组级增强指标", `${availability.groups_with_enhanced_metrics ?? 0}/${availability.groups_total ?? uniqueGroupNames.length}`],
              ["组级训练诊断", availability.combined_training_diagnostics ? "组合有训练数据" : "以组级数据为准"],
            ].map(([label, value]) => (
              <div key={label} style={{ border: "1px solid #e2e8f0", borderRadius: 8, padding: "10px" }}>
                <div style={{ fontSize: 12, color: "#64748b" }}>{label}</div>
                <div style={{ marginTop: 4, fontSize: 14, fontWeight: 700, color: String(value).includes("缺失") ? "#dc2626" : "#0f172a" }}>
                  {value}
                </div>
              </div>
            ))}
            {Array.isArray(availability.missing_group_enhanced_metrics) &&
              availability.missing_group_enhanced_metrics.length > 0 && (
                <div
                  style={{
                    gridColumn: "1 / -1",
                    padding: "10px 12px",
                    borderRadius: 8,
                    border: "1px solid #fecaca",
                    backgroundColor: "#fef2f2",
                    color: "#991b1b",
                    fontSize: 12,
                  }}
                >
                  缺少组级增强指标：{availability.missing_group_enhanced_metrics.join(", ")}。这些组不会生成训练/预测级优化建议，需先修复产物采集。
                </div>
              )}
            {icQualityEntries.length > 0 && (
              <div
                style={{
                  gridColumn: "1 / -1",
                  padding: "10px 12px",
                  borderRadius: 8,
                  border: "1px solid #bfdbfe",
                  backgroundColor: "#eff6ff",
                  color: "#1e3a8a",
                  fontSize: 12,
                }}
              >
                <div style={{ fontWeight: 700, marginBottom: 6 }}>
                  IC 质量诊断
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 80px 80px 80px", gap: 6 }}>
                  {icQualityEntries.map(([name, quality]) => (
                    <React.Fragment key={name}>
                      <span style={{ fontFamily: "monospace" }}>{name}</span>
                      <span>days: {quality?.total_days ?? "-"}</span>
                      <span>valid: {quality?.valid_days ?? "-"}</span>
                      <span>skipped: {quality?.skipped_days ?? "-"}</span>
                    </React.Fragment>
                  ))}
                </div>
                <div style={{ marginTop: 6, color: "#475569" }}>
                  仅当标签/预测为空值或常量时才剔除单日 IC；有效交易日覆盖不足时后端仍会直接失败。
                </div>
              </div>
            )}
          </div>
        </div>

        <div style={cardStyle}>
          <div style={headerStyle}>
            <h2 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#1e293b" }}>
              组合回测诊断
            </h2>
          </div>
          <div style={{ padding: "20px", display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px" }}>
            {[
              ["年化收益(含成本)", fmtPct(portfolio.annualized_return_with_cost, 2)],
              ["年化收益(无成本)", fmtPct(portfolio.annualized_return_no_cost, 2)],
              ["成本拖累", fmtPct(portfolio.cost_drag_annualized, 2)],
              ["最大回撤(含成本)", fmtPct(portfolio.max_drawdown_with_cost, 2)],
              ["年化换手", fmtNum(portfolio.annualized_turnover, 2)],
              ["Top30稳定性", fmtPct(portfolio.top30_stability, 1)],
              ["有效组数", fmtNum(diversification.effective_group_count, 2)],
              ["最大组间相关", fmtNum(diversification.max_abs_correlation, 4)],
            ].map(([label, value]) => (
              <div key={label} style={{ border: "1px solid #e2e8f0", borderRadius: 8, padding: "10px" }}>
                <div style={{ fontSize: 12, color: "#64748b" }}>{label}</div>
                <div style={{ marginTop: 4, fontSize: 14, fontWeight: 700, color: "#0f172a", fontFamily: "monospace" }}>
                  {value}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* 1. Group Performance Matrix */}
      <div style={{ ...cardStyle, marginBottom: "24px" }}>
        <div style={headerStyle}>
          <h2
            style={{
              margin: 0,
              fontSize: "16px",
              fontWeight: 700,
              color: "#1e293b",
            }}
          >
            Group Performance Matrix
          </h2>
        </div>
        <div style={{ overflowX: "auto" }}>
          <table
            style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}
          >
            <thead>
              <tr style={{ background: "#f9fafb", textAlign: "left" }}>
                <th
                  style={{
                    padding: "10px 16px",
                    fontWeight: 500,
                    color: "#6b7280",
                    fontSize: 12,
                  }}
                >
                  组名
                </th>
                <th
                  style={{
                    padding: "10px 16px",
                    fontWeight: 500,
                    color: "#6b7280",
                    fontSize: 12,
                    textAlign: "center",
                    width: 60,
                  }}
                >
                  因子
                </th>
                <th
                  style={{
                    padding: "10px 16px",
                    fontWeight: 500,
                    color: "#6b7280",
                    fontSize: 12,
                  }}
                >
                  模型
                </th>
                <th
                  style={{
                    padding: "10px 16px",
                    fontWeight: 500,
                    color: "#6b7280",
                    fontSize: 12,
                    textAlign: "right",
                    width: 70,
                  }}
                >
                  IC
                </th>
                <th
                  style={{
                    padding: "10px 16px",
                    fontWeight: 500,
                    color: "#6b7280",
                    fontSize: 12,
                    textAlign: "right",
                    width: 70,
                  }}
                >
                  ICIR
                </th>
                <th
                  style={{
                    padding: "10px 16px",
                    fontWeight: 500,
                    color: "#6b7280",
                    fontSize: 12,
                    textAlign: "right",
                    width: 70,
                  }}
                >
                  Sharpe
                </th>
                <th
                  style={{
                    padding: "10px 16px",
                    fontWeight: 500,
                    color: "#6b7280",
                    fontSize: 12,
                    textAlign: "right",
                    width: 80,
                  }}
                >
                  Meta权重
                </th>
                <th
                  style={{
                    padding: "10px 16px",
                    fontWeight: 500,
                    color: "#6b7280",
                    fontSize: 12,
                    textAlign: "center",
                    width: 60,
                  }}
                >
                  状态
                </th>
              </tr>
            </thead>
            <tbody>
              {data.groups.map((g) => {
                const isZeroWeight =
                  g.meta_weight != null && g.meta_weight < 0.05;
                const isLowIC =
                  g.group_ic != null && Math.abs(g.group_ic) < 0.01;
                const rowBg = isZeroWeight
                  ? "#fef2f2"
                  : isLowIC
                    ? "#fffbeb"
                    : "transparent";

                return (
                  <tr
                    key={g.group_name}
                    style={{
                      borderTop: "1px solid #f3f4f6",
                      backgroundColor: rowBg,
                    }}
                  >
                    <td
                      style={{
                        padding: "10px 16px",
                        fontWeight: 600,
                        color: "#111827",
                      }}
                    >
                      {isZeroWeight && (
                        <span
                          style={{
                            color: "#dc2626",
                            marginRight: 4,
                            fontSize: 14,
                          }}
                        >
                          !
                        </span>
                      )}
                      {g.group_name}
                    </td>
                    <td
                      style={{
                        padding: "10px 16px",
                        textAlign: "center",
                        fontFamily: "monospace",
                      }}
                    >
                      {g.factor_count}
                    </td>
                    <td
                      style={{
                        padding: "10px 16px",
                        fontFamily: "monospace",
                        fontSize: 12,
                        color: "#4b5563",
                      }}
                    >
                      {g.model_id.replace(/__seed_|__/g, "")}
                    </td>
                    <td
                      style={{
                        padding: "10px 16px",
                        textAlign: "right",
                        fontFamily: "monospace",
                        color:
                          g.group_ic != null && Math.abs(g.group_ic) > 0.03
                            ? "#059669"
                            : "#64748b",
                      }}
                    >
                      {g.group_ic != null ? g.group_ic.toFixed(4) : "-"}
                    </td>
                    <td
                      style={{
                        padding: "10px 16px",
                        textAlign: "right",
                        fontFamily: "monospace",
                        color: "#64748b",
                      }}
                    >
                      {g.group_icir != null ? g.group_icir.toFixed(3) : "-"}
                    </td>
                    <td
                      style={{
                        padding: "10px 16px",
                        textAlign: "right",
                        fontFamily: "monospace",
                        color: "#64748b",
                      }}
                    >
                      {g.group_sharpe != null ? g.group_sharpe.toFixed(3) : "-"}
                    </td>
                    <td
                      style={{
                        padding: "10px 16px",
                        textAlign: "right",
                        fontFamily: "monospace",
                        fontWeight: 600,
                        color: isZeroWeight ? "#dc2626" : "#7c3aed",
                      }}
                    >
                      {g.meta_weight != null
                        ? (g.meta_weight * 100).toFixed(1) + "%"
                        : "-"}
                    </td>
                    <td style={{ padding: "10px 16px", textAlign: "center" }}>
                      <span
                        style={{
                          padding: "2px 8px",
                          borderRadius: 4,
                          fontSize: 11,
                          fontWeight: 600,
                          backgroundColor:
                            g.status === "completed"
                              ? "#dcfce7"
                              : g.status === "failed"
                                ? "#fecaca"
                                : "#fef3c7",
                          color:
                            g.status === "completed"
                              ? "#166534"
                              : g.status === "failed"
                                ? "#991b1b"
                                : "#92400e",
                        }}
                      >
                        {g.status}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* 2 + 3: Meta 权重 + 相关性 */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: "24px",
          marginBottom: "24px",
        }}
      >
        {/* Meta权重柱状图 (简单 CSS bar) */}
        <div style={cardStyle}>
          <div style={headerStyle}>
            <h2
              style={{
                margin: 0,
                fontSize: "16px",
                fontWeight: 700,
                color: "#1e293b",
              }}
            >
              Meta-Model 权重分布
            </h2>
          </div>
          <div style={{ padding: "20px" }}>
            {data.groups
              .filter((g) => g.meta_weight != null)
              .sort((a, b) => (b.meta_weight || 0) - (a.meta_weight || 0))
              .map((g) => (
                <div
                  key={g.group_name}
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: "8px",
                    marginBottom: "8px",
                  }}
                >
                  <span
                    style={{
                      width: "120px",
                      fontSize: 12,
                      fontWeight: 600,
                      color: "#374151",
                      textAlign: "right",
                      flexShrink: 0,
                    }}
                  >
                    {g.group_name}
                  </span>
                  <div
                    style={{
                      flex: 1,
                      height: "20px",
                      backgroundColor: "#f1f5f9",
                      borderRadius: "4px",
                      overflow: "hidden",
                    }}
                  >
                    <div
                      style={{
                        height: "100%",
                        width: `${Math.max(1, (g.meta_weight || 0) * 100)}%`,
                        backgroundColor:
                          (g.meta_weight || 0) < 0.05
                            ? "#fca5a5"
                            : (g.meta_weight || 0) > 0.4
                              ? "#a78bfa"
                              : "#93c5fd",
                        borderRadius: "4px",
                        transition: "width 0.3s",
                      }}
                    />
                  </div>
                  <span
                    style={{
                      width: "50px",
                      fontSize: 12,
                      fontFamily: "monospace",
                      color: "#64748b",
                      flexShrink: 0,
                    }}
                  >
                    {((g.meta_weight || 0) * 100).toFixed(1)}%
                  </span>
                </div>
              ))}
          </div>
        </div>

        {/* 组间相关性 */}
        <div style={cardStyle}>
          <div style={headerStyle}>
            <h2
              style={{
                margin: 0,
                fontSize: "16px",
                fontWeight: 700,
                color: "#1e293b",
              }}
            >
              组间预测相关性
            </h2>
          </div>
          <div style={{ padding: "20px" }}>
            {corrPairs.length > 0 ? (
              <table
                style={{
                  width: "100%",
                  borderCollapse: "collapse",
                  fontSize: 12,
                }}
              >
                <thead>
                  <tr style={{ background: "#f9fafb" }}>
                    <th
                      style={{
                        padding: "6px 10px",
                        textAlign: "left",
                        fontWeight: 500,
                        color: "#6b7280",
                      }}
                    >
                      组 A
                    </th>
                    <th
                      style={{
                        padding: "6px 10px",
                        textAlign: "left",
                        fontWeight: 500,
                        color: "#6b7280",
                      }}
                    >
                      组 B
                    </th>
                    <th
                      style={{
                        padding: "6px 10px",
                        textAlign: "right",
                        fontWeight: 500,
                        color: "#6b7280",
                      }}
                    >
                      相关性
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {corrPairs
                    .sort(
                      (a, b) =>
                        Math.abs(b.correlation) - Math.abs(a.correlation)
                    )
                    .map((p, i) => (
                      <tr
                        key={i}
                        style={{
                          borderTop: "1px solid #f3f4f6",
                          backgroundColor:
                            Math.abs(p.correlation) > 0.7
                              ? "#fef2f2"
                              : Math.abs(p.correlation) > 0.5
                                ? "#fffbeb"
                                : "transparent",
                        }}
                      >
                        <td
                          style={{
                            padding: "6px 10px",
                            fontFamily: "monospace",
                          }}
                        >
                          {p.group_a}
                        </td>
                        <td
                          style={{
                            padding: "6px 10px",
                            fontFamily: "monospace",
                          }}
                        >
                          {p.group_b}
                        </td>
                        <td
                          style={{
                            padding: "6px 10px",
                            textAlign: "right",
                            fontFamily: "monospace",
                            fontWeight: 600,
                            color:
                              Math.abs(p.correlation) > 0.7
                                ? "#dc2626"
                                : Math.abs(p.correlation) > 0.5
                                  ? "#d97706"
                                  : "#64748b",
                          }}
                        >
                          {p.correlation.toFixed(4)}
                        </td>
                      </tr>
                    ))}
                </tbody>
              </table>
            ) : (
              <div
                style={{
                  padding: "30px",
                  textAlign: "center",
                  color: "#94a3b8",
                  fontSize: 13,
                }}
              >
                暂无组间相关性数据 (实验完成后自动计算)
              </div>
            )}
          </div>
        </div>
      </div>

      {/* 4. 单组深度诊断 */}
      <div style={{ ...cardStyle, marginBottom: "24px" }}>
        <div style={headerStyle}>
          <h2
            style={{
              margin: 0,
              fontSize: "16px",
              fontWeight: 700,
              color: "#1e293b",
            }}
          >
            单组训练 / 预测 / 因子诊断
          </h2>
        </div>
        <div
          style={{
            padding: "20px",
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: "16px",
          }}
        >
          {data.groups.map((g) => {
            const diag = groupDiagByName.get(g.group_name);
            const train = diag?.training_diagnostics || {};
            const pred = diag?.prediction_diagnostics || {};
            const icDiag = diag?.ic_diagnostics || {};
            const topFactors = diag?.feature_importance_top || [];
            const dataAvailable = diag?.data_available || {};

            return (
              <div
                key={g.group_name}
                style={{
                  border: "1px solid #e2e8f0",
                  borderRadius: "8px",
                  padding: "12px",
                }}
              >
                <div
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                    marginBottom: "8px",
                  }}
                >
                  <span style={{ fontWeight: 600, fontSize: 14, color: "#1e293b" }}>
                    {g.group_name}
                  </span>
                  <span style={{ fontSize: 12, color: "#64748b", fontFamily: "monospace" }}>
                    {g.factor_count} 因子 | IC: {g.group_ic != null ? g.group_ic.toFixed(4) : "-"}
                  </span>
                </div>

                {!dataAvailable.enhanced_metrics && (
                  <div
                    style={{
                      marginBottom: 10,
                      padding: "8px",
                      borderRadius: 6,
                      backgroundColor: "#fef2f2",
                      color: "#991b1b",
                      fontSize: 12,
                    }}
                  >
                    该组缺少增强指标，训练/预测/因子诊断不会被静默推断。
                  </div>
                )}

                <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 8, marginBottom: 10 }}>
                  <MetricChip label="贡献IC" value={fmtNum(diag?.contribution_to_combined_ic, 5)} />
                  <MetricChip label="IC正率" value={fmtPct(icDiag.ic_positive_ratio, 1)} />
                  <MetricChip label="Rank换手" value={fmtPct(pred.pred_rank_turnover, 1)} />
                  <MetricChip label="Top30稳定" value={fmtPct(pred.top30_stability, 1)} />
                  <MetricChip label="Train Loss" value={fmtNum(train.final_train_loss, 5)} />
                  <MetricChip label="Valid Loss" value={fmtNum(train.final_val_loss, 5)} />
                  <MetricChip label="过拟合比" value={fmtNum(train.overfit_ratio, 3)} />
                  <MetricChip label="泛化Gap" value={fmtNum(train.generalization_gap, 5)} />
                  <MetricChip label="Loss点数" value={`${train.train_loss_points ?? 0}/${train.val_loss_points ?? 0}`} />
                </div>

                <div style={{ marginBottom: 8, fontSize: 12, fontWeight: 600, color: "#475569" }}>
                  Top 因子重要性
                </div>
                <div style={{ display: "flex", flexWrap: "wrap", gap: "4px", maxHeight: "80px", overflowY: "auto" }}>
                  {topFactors.length > 0 ? (
                    topFactors.map((f, idx) => (
                      <span
                        key={`${g.group_name}-${f.name || idx}`}
                        style={{
                          padding: "1px 6px",
                          backgroundColor: "#ecfeff",
                          color: "#155e75",
                          fontSize: 10,
                          borderRadius: "3px",
                          fontFamily: "monospace",
                        }}
                      >
                        {f.name || f.factor || `factor_${idx + 1}`}
                        {typeof f.gain_pct === "number" ? ` ${f.gain_pct.toFixed(1)}%` : ""}
                      </span>
                    ))
                  ) : (
                    g.factor_names.map((f) => (
                      <span
                        key={f}
                        style={{
                          padding: "1px 6px",
                          backgroundColor: "#f1f5f9",
                          color: "#475569",
                          fontSize: 10,
                          borderRadius: "3px",
                          fontFamily: "monospace",
                        }}
                      >
                        {f}
                      </span>
                    ))
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* 5. 多Alpha优化指导 */}
      {optimizationGuidance.length > 0 && (
        <div style={{ ...cardStyle, marginBottom: "24px" }}>
          <div style={headerStyle}>
            <h2 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#0f766e" }}>
              基于回测与训练数据的优化指导 ({optimizationGuidance.length})
            </h2>
          </div>
          <div style={{ padding: "20px", display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px" }}>
            {optimizationGuidance.map((item, i) => (
              <div
                key={`${item.rule_id}-${i}`}
                style={{
                  padding: "12px 16px",
                  borderRadius: "8px",
                  border: "1px solid",
                  ...severityStyle(item.severity),
                }}
              >
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
                  <span style={{ fontSize: 10, fontWeight: 700, textTransform: "uppercase" }}>
                    {item.severity}
                  </span>
                  <span style={{ fontSize: 12, fontWeight: 700 }}>{item.rule_id}</span>
                </div>
                <p style={{ margin: "0 0 6px", fontSize: 13, color: "#334155" }}>{item.message}</p>
                <p style={{ margin: "0 0 8px", fontSize: 12, color: "#047857", fontWeight: 600 }}>
                  {item.recommendation}
                </p>
                {item.affected_groups?.length > 0 && (
                  <div style={{ marginBottom: 6, fontSize: 11, color: "#475569" }}>
                    影响组：{item.affected_groups.join(", ")}
                  </div>
                )}
                {item.evidence && (
                  <pre
                    style={{
                      margin: 0,
                      padding: "8px",
                      borderRadius: 6,
                      backgroundColor: "rgba(255,255,255,0.65)",
                      color: "#334155",
                      fontSize: 10,
                      overflowX: "auto",
                    }}
                  >
                    {JSON.stringify(item.evidence, null, 2)}
                  </pre>
                )}
                <a
                  href={`/quantevolver/multi-alpha/evolve-wizard?source_exp=${expId}&action=${item.action_type}&target=${(item.affected_groups || []).join(",")}`}
                  style={{
                    display: "inline-flex",
                    marginTop: 8,
                    padding: "5px 10px",
                    borderRadius: 6,
                    backgroundColor: "#ffffff",
                    border: "1px solid #cbd5e1",
                    color: "#0f766e",
                    fontSize: 12,
                    fontWeight: 700,
                    textDecoration: "none",
                  }}
                >
                  进入演进向导
                </a>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 5. 瓶颈识别 */}
      {data.bottlenecks.length > 0 && (
        <div style={{ ...cardStyle, marginBottom: "24px" }}>
          <div style={headerStyle}>
            <h2
              style={{
                margin: 0,
                fontSize: "16px",
                fontWeight: 700,
                color: "#dc2626",
              }}
            >
              瓶颈识别 ({data.bottlenecks.length})
            </h2>
          </div>
          <div
            style={{
              padding: "20px",
              display: "flex",
              flexDirection: "column",
              gap: "12px",
            }}
          >
            {data.bottlenecks.map((b, i) => (
              <div
                key={i}
                style={{
                  padding: "12px 16px",
                  borderRadius: "8px",
                  backgroundColor:
                    b.severity === "high"
                      ? "#fef2f2"
                      : b.severity === "medium"
                        ? "#fffbeb"
                        : "#f0fdf4",
                  border: `1px solid ${b.severity === "high" ? "#fecaca" : b.severity === "medium" ? "#fed7aa" : "#bbf7d0"}`,
                }}
              >
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: "8px",
                    marginBottom: "4px",
                  }}
                >
                  <span
                    style={{
                      fontSize: 10,
                      fontWeight: 700,
                      padding: "1px 6px",
                      borderRadius: 3,
                      backgroundColor:
                        b.severity === "high"
                          ? "#dc2626"
                          : b.severity === "medium"
                            ? "#d97706"
                            : "#16a34a",
                      color: "#fff",
                      textTransform: "uppercase",
                    }}
                  >
                    {b.severity}
                  </span>
                  <span style={{ fontSize: 12, fontWeight: 600, color: "#374151" }}>
                    {b.rule_id}
                  </span>
                </div>
                <p style={{ margin: "0 0 4px", fontSize: 13, color: "#4b5563" }}>
                  {b.message}
                </p>
                <p
                  style={{
                    margin: 0,
                    fontSize: 12,
                    color: "#059669",
                    fontWeight: 500,
                  }}
                >
                  {b.recommendation}
                </p>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 6. 演进建议 */}
      {data.recommendations.length > 0 && (
        <div style={cardStyle}>
          <div style={headerStyle}>
            <h2
              style={{
                margin: 0,
                fontSize: "16px",
                fontWeight: 700,
                color: "#7c3aed",
              }}
            >
              演进建议
            </h2>
          </div>
          <div
            style={{
              padding: "20px",
              display: "flex",
              flexWrap: "wrap",
              gap: "12px",
            }}
          >
            {data.recommendations.map((r, i) => (
              <a
                key={i}
                href={`/quantevolver/multi-alpha/evolve-wizard?source_exp=${expId}&action=${r.action_type}&target=${r.affected_groups.join(",")}`}
                style={{
                  display: "inline-flex",
                  alignItems: "center",
                  gap: "6px",
                  padding: "8px 16px",
                  borderRadius: "8px",
                  backgroundColor: "#f5f3ff",
                  border: "1px solid #ddd6fe",
                  color: "#6d28d9",
                  fontSize: 13,
                  fontWeight: 600,
                  textDecoration: "none",
                  transition: "all 0.2s",
                }}
              >
                {r.action_type === "remove_group"
                  ? "删除"
                  : r.action_type === "merge_groups"
                    ? "合并"
                    : r.action_type === "switch_model"
                      ? "换模型"
                      : r.action_type === "tune_meta"
                        ? "调Meta"
                        : "优化"}{" "}
                {r.affected_groups.join("+")}
              </a>
            ))}
          </div>
        </div>
      )}

      {/* 无瓶颈时显示健康状态 */}
      {data.bottlenecks.length === 0 && optimizationGuidance.length === 0 && (
        <div
          style={{
            ...cardStyle,
            padding: "40px",
            textAlign: "center",
          }}
        >
          <div style={{ fontSize: 36, marginBottom: 8 }}>OK</div>
          <p style={{ margin: 0, fontSize: 14, color: "#059669", fontWeight: 600 }}>
            Multi-Alpha 组合状态健康，未发现明显瓶颈
          </p>
        </div>
      )}
    </div>
  );
}

function MetricChip({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div style={{ border: "1px solid #e2e8f0", borderRadius: 6, padding: "6px 8px", backgroundColor: "#f8fafc" }}>
      <div style={{ fontSize: 10, color: "#64748b" }}>{label}</div>
      <div style={{ marginTop: 2, fontSize: 12, fontWeight: 700, color: "#0f172a", fontFamily: "monospace" }}>
        {value}
      </div>
    </div>
  );
}
