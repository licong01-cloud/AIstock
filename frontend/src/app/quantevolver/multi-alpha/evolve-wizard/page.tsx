"use client";

import React, { useEffect, useState, useCallback, useMemo } from "react";
import { useSearchParams } from "next/navigation";
import MultiAlphaGroupEditor, {
  type MultiAlphaConfig,
  type AlphaGroupConfig,
} from "../../components/MultiAlphaGroupEditor";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

// ── Types ──────────────────────────────────────────────────────────

interface ExperimentSummary {
  experiment_id: string;
  alpha_mode?: string;
  multi_alpha_config?: MultiAlphaConfig;
  created_at?: string;
  result_metrics?: any;
  ic?: number | null;
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

const btnPrimary: React.CSSProperties = {
  padding: "10px 24px",
  backgroundColor: "#7c3aed",
  color: "#fff",
  border: "none",
  borderRadius: "6px",
  fontSize: "14px",
  fontWeight: 600,
  cursor: "pointer",
  boxShadow: "0 2px 4px rgba(124, 58, 237, 0.25)",
};

const btnSecondary: React.CSSProperties = {
  padding: "10px 24px",
  backgroundColor: "#f1f5f9",
  color: "#475569",
  border: "1px solid #e2e8f0",
  borderRadius: "6px",
  fontSize: "14px",
  fontWeight: 600,
  cursor: "pointer",
};

// ── Component ──────────────────────────────────────────────────────

export default function EvolveWizardPage() {
  const searchParams = useSearchParams();
  const initialSourceExp = searchParams.get("source_exp") || "";

  const [step, setStep] = useState(1);

  // Step 1: Source experiment
  const [experiments, setExperiments] = useState<ExperimentSummary[]>([]);
  const [selectedExpId, setSelectedExpId] = useState(initialSourceExp);
  const [sourceConfig, setSourceConfig] = useState<MultiAlphaConfig | null>(null);
  const [loadingExps, setLoadingExps] = useState(true);

  // Step 2: Diagnostics
  const [diagnostics, setDiagnostics] = useState<any>(null);
  const [loadingDiag, setLoadingDiag] = useState(false);

  // Step 3: Config changes
  const [newConfig, setNewConfig] = useState<MultiAlphaConfig | null>(null);

  // Step 4: Submit
  const [submitting, setSubmitting] = useState(false);
  const [submitResult, setSubmitResult] = useState<any>(null);

  // ── Load experiment list ──────────────────────────────────────────

  useEffect(() => {
    setLoadingExps(true);
    fetch(`${API}/quantevolver/experiments?limit=50&alpha_mode=multi`)
      .then((r) => r.json())
      .then((d) => {
        if (d.ok || d.items) setExperiments(d.items || []);
      })
      .catch(() => {})
      .finally(() => setLoadingExps(false));
  }, []);

  // ── Load source experiment config ─────────────────────────────────

  const loadSourceConfig = useCallback(async () => {
    if (!selectedExpId) return;
    try {
      const res = await fetch(`${API}/quantevolver/experiments/${selectedExpId}`);
      const d = await res.json();
      if (d.ok && d.experiment?.multi_alpha_config) {
        setSourceConfig(d.experiment.multi_alpha_config);
        // Initialize newConfig as a deep copy
        setNewConfig(JSON.parse(JSON.stringify(d.experiment.multi_alpha_config)));
      }
    } catch (e) {
      console.error("Failed to load source config:", e);
    }
  }, [selectedExpId]);

  // ── Load diagnostics ──────────────────────────────────────────────

  const loadDiagnostics = useCallback(async () => {
    if (!selectedExpId) return;
    setLoadingDiag(true);
    try {
      const res = await fetch(
        `${API}/quantevolver/multi-alpha/${selectedExpId}/diagnostics`
      );
      const d = await res.json();
      if (d.ok) setDiagnostics(d.diagnostics);
    } catch (e) {
      console.error("Failed to load diagnostics:", e);
    }
    setLoadingDiag(false);
  }, [selectedExpId]);

  // ── Submit new experiment ─────────────────────────────────────────

  const handleSubmit = useCallback(async () => {
    if (!newConfig) return;
    setSubmitting(true);
    try {
      // Aggregate factor names from all groups
      const allFactors = [...new Set(newConfig.alpha_groups.flatMap((g) => g.factor_names || []))];

      const res = await fetch(`${API}/quantevolver/config/generate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          factor_names: allFactors,
          model_id: newConfig.alpha_groups[0]?.model_id || undefined,
          alpha_mode: "multi",
          multi_alpha_config: newConfig,
          dispatch_mode: "independent",
          // 血统追踪：关联到源实验
          parent_multi_alpha_id: selectedExpId || undefined,
        }),
      });
      const data = await res.json();
      setSubmitResult(data);
    } catch (e: any) {
      setSubmitResult({ ok: false, error: e.message });
    }
    setSubmitting(false);
  }, [newConfig]);

  // ── Diff computation ──────────────────────────────────────────────

  const configDiff = useMemo(() => {
    if (!sourceConfig || !newConfig) return [];
    const diffs: string[] = [];

    // Compare groups
    const srcNames = new Set(sourceConfig.alpha_groups.map((g) => g.group_name));
    const newNames = new Set(newConfig.alpha_groups.map((g) => g.group_name));

    for (const name of newNames) {
      if (!srcNames.has(name)) diffs.push(`+ 新增组: ${name}`);
    }
    for (const name of srcNames) {
      if (!newNames.has(name)) diffs.push(`- 删除组: ${name}`);
    }

    for (const ng of newConfig.alpha_groups) {
      const sg = sourceConfig.alpha_groups.find((g) => g.group_name === ng.group_name);
      if (!sg) continue;

      if (ng.model_id !== sg.model_id) {
        diffs.push(
          `~ ${ng.group_name}: 模型 ${sg.model_id.replace(/__seed_|__/g, "")} -> ${ng.model_id.replace(/__seed_|__/g, "")}`
        );
      }
      if (ng.factor_names.length !== sg.factor_names.length) {
        diffs.push(
          `~ ${ng.group_name}: 因子数 ${sg.factor_names.length} -> ${ng.factor_names.length}`
        );
      }
      if (ng.reuse_mode && ng.reuse_mode !== "retrain") {
        diffs.push(
          `  ${ng.group_name}: ${ng.reuse_mode} (复用源: ${ng.model_source_experiment_id || "未指定"})`
        );
      }
    }

    // Meta-model changes
    if (sourceConfig.meta_model.method !== newConfig.meta_model.method) {
      diffs.push(
        `~ Meta: ${sourceConfig.meta_model.method} -> ${newConfig.meta_model.method}`
      );
    }

    return diffs;
  }, [sourceConfig, newConfig]);

  // ── Step titles ───────────────────────────────────────────────────

  const STEPS = [
    "选择源实验",
    "诊断回顾",
    "配置变更",
    "确认提交",
  ];

  return (
    <div
      style={{
        padding: "24px",
        maxWidth: "1200px",
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
          Multi-Alpha 引导式演进
        </h1>
        <p
          style={{
            margin: "8px 0 0",
            fontSize: "14px",
            color: "rgba(255,255,255,0.75)",
          }}
        >
          基于已有实验的诊断结果，引导优化多Alpha配置
        </p>
      </div>

      {/* Step 导航 */}
      <div
        style={{
          ...cardStyle,
          marginBottom: "24px",
          display: "flex",
        }}
      >
        {STEPS.map((s, idx) => {
          const sNum = idx + 1;
          const isActive = step === sNum;
          const isPassed = step > sNum;
          return (
            <button
              key={sNum}
              onClick={() => sNum < step && setStep(sNum)}
              style={{
                flex: 1,
                padding: "14px 8px",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                gap: "8px",
                border: "none",
                borderRight:
                  idx < STEPS.length - 1 ? "1px solid #f1f5f9" : "none",
                backgroundColor: isActive
                  ? "#f5f3ff"
                  : isPassed
                    ? "#f8fafc"
                    : "#ffffff",
                cursor: sNum <= step ? "pointer" : "default",
                borderBottom: isActive
                  ? "3px solid #7c3aed"
                  : "3px solid transparent",
              }}
            >
              <div
                style={{
                  width: "28px",
                  height: "28px",
                  borderRadius: "50%",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  fontWeight: 700,
                  fontSize: "12px",
                  backgroundColor: isActive
                    ? "#7c3aed"
                    : isPassed
                      ? "#ddd6fe"
                      : "#f1f5f9",
                  color: isActive
                    ? "#ffffff"
                    : isPassed
                      ? "#7c3aed"
                      : "#94a3b8",
                }}
              >
                {isPassed ? "OK" : sNum}
              </div>
              <span
                style={{
                  fontWeight: 600,
                  fontSize: "13px",
                  color: isActive
                    ? "#1e293b"
                    : isPassed
                      ? "#475569"
                      : "#94a3b8",
                }}
              >
                {s}
              </span>
            </button>
          );
        })}
      </div>

      {/* Step 1: 选择源实验 */}
      {step === 1 && (
        <div style={{ ...cardStyle, padding: "24px" }}>
          <h2
            style={{
              margin: "0 0 16px",
              fontSize: "18px",
              fontWeight: 700,
              color: "#1e293b",
            }}
          >
            选择源实验
          </h2>
          {loadingExps ? (
            <div style={{ padding: 20, color: "#94a3b8" }}>加载中...</div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
              {experiments.map((exp) => (
                <div
                  key={exp.experiment_id}
                  onClick={() => setSelectedExpId(exp.experiment_id)}
                  style={{
                    padding: "12px 16px",
                    border:
                      selectedExpId === exp.experiment_id
                        ? "2px solid #7c3aed"
                        : "1px solid #e2e8f0",
                    borderRadius: "8px",
                    cursor: "pointer",
                    backgroundColor:
                      selectedExpId === exp.experiment_id
                        ? "#f5f3ff"
                        : "#fff",
                  }}
                >
                  <div
                    style={{
                      display: "flex",
                      justifyContent: "space-between",
                      alignItems: "center",
                    }}
                  >
                    <span
                      style={{
                        fontWeight: 600,
                        fontSize: 14,
                        color: "#1e293b",
                      }}
                    >
                      {exp.experiment_id}
                    </span>
                    <span
                      style={{
                        fontSize: 12,
                        color: "#64748b",
                        fontFamily: "monospace",
                      }}
                    >
                      IC:{" "}
                      {exp.ic != null
                        ? Number(exp.ic).toFixed(4)
                        : "-"}{" "}
                      |{" "}
                      {exp.created_at
                        ? new Date(exp.created_at).toLocaleDateString("zh-CN")
                        : ""}
                    </span>
                  </div>
                  {exp.multi_alpha_config && (
                    <div
                      style={{
                        fontSize: 12,
                        color: "#64748b",
                        marginTop: 4,
                      }}
                    >
                      {exp.multi_alpha_config.alpha_groups?.length || "?"} 组 |
                      Meta: {exp.multi_alpha_config.meta_model?.method || "-"} |
                      {exp.multi_alpha_config.execution_mode || "-"}
                    </div>
                  )}
                </div>
              ))}
              {experiments.length === 0 && (
                <div style={{ padding: 30, textAlign: "center", color: "#94a3b8" }}>
                  暂无已完成的多Alpha实验
                </div>
              )}
            </div>
          )}
          <div
            style={{
              display: "flex",
              justifyContent: "flex-end",
              marginTop: "24px",
            }}
          >
            <button
              onClick={() => {
                loadSourceConfig();
                loadDiagnostics();
                setStep(2);
              }}
              disabled={!selectedExpId}
              style={{
                ...btnPrimary,
                opacity: selectedExpId ? 1 : 0.5,
              }}
            >
              下一步：诊断回顾
            </button>
          </div>
        </div>
      )}

      {/* Step 2: 诊断回顾 */}
      {step === 2 && (
        <div style={{ ...cardStyle, padding: "24px" }}>
          <h2
            style={{
              margin: "0 0 16px",
              fontSize: "18px",
              fontWeight: 700,
              color: "#1e293b",
            }}
          >
            诊断回顾 — {selectedExpId}
          </h2>

          {loadingDiag ? (
            <div style={{ padding: 20, color: "#94a3b8" }}>加载诊断中...</div>
          ) : diagnostics ? (
            <div>
              {/* Compact bottleneck list */}
              {diagnostics.bottlenecks?.length > 0 ? (
                <div
                  style={{
                    display: "flex",
                    flexDirection: "column",
                    gap: "8px",
                    marginBottom: "20px",
                  }}
                >
                  {diagnostics.bottlenecks.map(
                    (b: Bottleneck, i: number) => (
                      <div
                        key={i}
                        style={{
                          padding: "10px 14px",
                          borderRadius: "6px",
                          backgroundColor:
                            b.severity === "high"
                              ? "#fef2f2"
                              : b.severity === "medium"
                                ? "#fffbeb"
                                : "#f0fdf4",
                          border: `1px solid ${b.severity === "high" ? "#fecaca" : b.severity === "medium" ? "#fed7aa" : "#bbf7d0"}`,
                          fontSize: 13,
                        }}
                      >
                        <strong
                          style={{
                            color:
                              b.severity === "high"
                                ? "#dc2626"
                                : b.severity === "medium"
                                  ? "#d97706"
                                  : "#16a34a",
                          }}
                        >
                          [{b.severity.toUpperCase()}]
                        </strong>{" "}
                        {b.message}
                        <div
                          style={{
                            marginTop: 4,
                            fontSize: 12,
                            color: "#059669",
                          }}
                        >
                          {b.recommendation}
                        </div>
                      </div>
                    )
                  )}
                </div>
              ) : (
                <div
                  style={{
                    padding: "20px",
                    textAlign: "center",
                    color: "#059669",
                    fontSize: 14,
                    marginBottom: "20px",
                  }}
                >
                  组合状态健康，未发现明显瓶颈
                </div>
              )}

              {/* Compact group summary */}
              <div
                style={{
                  borderTop: "1px solid #e2e8f0",
                  paddingTop: "16px",
                }}
              >
                <div
                  style={{
                    fontSize: 13,
                    fontWeight: 600,
                    color: "#475569",
                    marginBottom: 8,
                  }}
                >
                  组性能概览
                </div>
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))",
                    gap: "8px",
                  }}
                >
                  {(diagnostics.groups || []).map((g: any) => (
                    <div
                      key={g.group_name}
                      style={{
                        padding: "8px 12px",
                        border: "1px solid #e2e8f0",
                        borderRadius: "6px",
                        fontSize: 12,
                      }}
                    >
                      <div style={{ fontWeight: 600, color: "#1e293b" }}>
                        {g.group_name}
                      </div>
                      <div style={{ color: "#64748b", fontFamily: "monospace" }}>
                        IC:{" "}
                        {g.group_ic != null ? g.group_ic.toFixed(4) : "-"} |
                        Weight:{" "}
                        {g.meta_weight != null
                          ? (g.meta_weight * 100).toFixed(1) + "%"
                          : "-"}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          ) : (
            <div style={{ padding: 20, color: "#94a3b8" }}>
              诊断数据不可用（实验可能尚未完成）
            </div>
          )}

          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              marginTop: "24px",
            }}
          >
            <button onClick={() => setStep(1)} style={btnSecondary}>
              上一步
            </button>
            <button onClick={() => setStep(3)} style={btnPrimary}>
              下一步：配置变更
            </button>
          </div>
        </div>
      )}

      {/* Step 3: 配置变更 */}
      {step === 3 && (
        <div style={{ ...cardStyle, padding: "24px" }}>
          <h2
            style={{
              margin: "0 0 16px",
              fontSize: "18px",
              fontWeight: 700,
              color: "#1e293b",
            }}
          >
            配置变更
          </h2>

          {newConfig ? (
            <div>
              <div
                style={{
                  border: "1px solid #e2e8f0",
                  borderRadius: "8px",
                  padding: "16px",
                  marginBottom: "20px",
                }}
              >
                <MultiAlphaGroupEditor
                  config={newConfig}
                  onConfigChange={setNewConfig}
                  apiBase={API}
                />
              </div>

              {/* Diff 预览 */}
              {configDiff.length > 0 && (
                <div
                  style={{
                    backgroundColor: "#1e293b",
                    borderRadius: "8px",
                    padding: "16px",
                    marginBottom: "16px",
                  }}
                >
                  <div
                    style={{
                      fontSize: 12,
                      fontWeight: 600,
                      color: "#94a3b8",
                      marginBottom: 8,
                    }}
                  >
                    与源实验的差异
                  </div>
                  <pre
                    style={{
                      margin: 0,
                      fontSize: 12,
                      fontFamily: "'Fira Code', Consolas, monospace",
                      color: "#e2e8f0",
                      lineHeight: 1.6,
                    }}
                  >
                    {configDiff.join("\n")}
                  </pre>
                </div>
              )}
              {configDiff.length === 0 && (
                <div
                  style={{
                    padding: "12px 16px",
                    backgroundColor: "#fffbeb",
                    border: "1px solid #fed7aa",
                    borderRadius: "6px",
                    fontSize: 13,
                    color: "#92400e",
                    marginBottom: "16px",
                  }}
                >
                  当前配置与源实验相同，请做出修改后再提交
                </div>
              )}
            </div>
          ) : (
            <div style={{ padding: 20, color: "#94a3b8" }}>
              请返回 Step 1 选择源实验
            </div>
          )}

          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              marginTop: "24px",
            }}
          >
            <button onClick={() => setStep(2)} style={btnSecondary}>
              上一步
            </button>
            <button
              onClick={() => setStep(4)}
              disabled={configDiff.length === 0}
              style={{
                ...btnPrimary,
                opacity: configDiff.length === 0 ? 0.5 : 1,
              }}
            >
              下一步：确认提交
            </button>
          </div>
        </div>
      )}

      {/* Step 4: 确认提交 */}
      {step === 4 && (
        <div style={{ ...cardStyle, padding: "24px" }}>
          <h2
            style={{
              margin: "0 0 16px",
              fontSize: "18px",
              fontWeight: 700,
              color: "#1e293b",
            }}
          >
            确认提交
          </h2>

          {submitResult ? (
            <div
              style={{
                backgroundColor: submitResult.ok ? "#dcfce7" : "#fef2f2",
                border: `1px solid ${submitResult.ok ? "#86efac" : "#fecaca"}`,
                borderRadius: "8px",
                padding: "20px",
                marginBottom: "24px",
              }}
            >
              {submitResult.ok ? (
                <>
                  <h3
                    style={{
                      margin: "0 0 8px",
                      fontWeight: 700,
                      color: "#166534",
                    }}
                  >
                    演进实验已创建
                  </h3>
                  <p style={{ margin: 0, fontSize: 13, color: "#15803d" }}>
                    <strong>Experiment ID:</strong>{" "}
                    {submitResult.parent_experiment_id || submitResult.experiment_id}
                  </p>
                  <div
                    style={{
                      marginTop: 12,
                      display: "flex",
                      gap: "12px",
                    }}
                  >
                    <a
                      href={`/quantevolver/multi-alpha/diagnostics/${submitResult.parent_experiment_id || submitResult.experiment_id}`}
                      style={{
                        ...btnPrimary,
                        backgroundColor: "#059669",
                        textDecoration: "none",
                        display: "inline-block",
                      }}
                    >
                      查看诊断
                    </a>
                  </div>
                </>
              ) : (
                <p style={{ margin: 0, fontSize: 13, color: "#dc2626" }}>
                  提交失败: {submitResult.error || submitResult.detail}
                </p>
              )}
            </div>
          ) : (
            <>
              {/* Summary */}
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "1fr 1fr",
                  gap: "20px",
                  marginBottom: "20px",
                }}
              >
                <div
                  style={{
                    backgroundColor: "#f8fafc",
                    borderRadius: "8px",
                    padding: "16px",
                    border: "1px solid #e2e8f0",
                  }}
                >
                  <h3
                    style={{
                      margin: "0 0 12px",
                      fontSize: 13,
                      fontWeight: 700,
                      color: "#64748b",
                      textTransform: "uppercase",
                    }}
                  >
                    源实验
                  </h3>
                  <div style={{ fontSize: 14, fontWeight: 600, color: "#1e293b" }}>
                    {selectedExpId}
                  </div>
                  {sourceConfig && (
                    <div style={{ fontSize: 12, color: "#64748b", marginTop: 4 }}>
                      {sourceConfig.alpha_groups.length} 组 |{" "}
                      {sourceConfig.alpha_groups.reduce(
                        (s, g) => s + g.factor_names.length,
                        0
                      )}{" "}
                      因子 | Meta: {sourceConfig.meta_model.method}
                    </div>
                  )}
                </div>
                <div
                  style={{
                    backgroundColor: "#f5f3ff",
                    borderRadius: "8px",
                    padding: "16px",
                    border: "1px solid #ddd6fe",
                  }}
                >
                  <h3
                    style={{
                      margin: "0 0 12px",
                      fontSize: 13,
                      fontWeight: 700,
                      color: "#7c3aed",
                      textTransform: "uppercase",
                    }}
                  >
                    新配置
                  </h3>
                  {newConfig && (
                    <>
                      <div
                        style={{
                          fontSize: 14,
                          fontWeight: 600,
                          color: "#1e293b",
                        }}
                      >
                        {newConfig.alpha_groups.length} 组 |{" "}
                        {newConfig.alpha_groups.reduce(
                          (s, g) => s + g.factor_names.length,
                          0
                        )}{" "}
                        因子
                      </div>
                      <div style={{ fontSize: 12, color: "#64748b", marginTop: 4 }}>
                        Meta: {newConfig.meta_model.method} |{" "}
                        {newConfig.execution_mode}
                      </div>
                      <div style={{ fontSize: 12, color: "#64748b", marginTop: 2 }}>
                        需重训组:{" "}
                        {
                          newConfig.alpha_groups.filter(
                            (g) => !g.reuse_mode || g.reuse_mode === "retrain"
                          ).length
                        }{" "}
                        / 复用组:{" "}
                        {
                          newConfig.alpha_groups.filter(
                            (g) =>
                              g.reuse_mode &&
                              g.reuse_mode !== "retrain"
                          ).length
                        }
                      </div>
                    </>
                  )}
                </div>
              </div>

              {/* Diff */}
              <div
                style={{
                  backgroundColor: "#1e293b",
                  borderRadius: "8px",
                  padding: "16px",
                  marginBottom: "24px",
                }}
              >
                <div
                  style={{
                    fontSize: 12,
                    fontWeight: 600,
                    color: "#94a3b8",
                    marginBottom: 8,
                  }}
                >
                  变更摘要 ({configDiff.length} 项)
                </div>
                <pre
                  style={{
                    margin: 0,
                    fontSize: 12,
                    fontFamily: "'Fira Code', Consolas, monospace",
                    color: "#e2e8f0",
                    lineHeight: 1.6,
                  }}
                >
                  {configDiff.join("\n") || "(无变更)"}
                </pre>
              </div>
            </>
          )}

          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              marginTop: "24px",
            }}
          >
            <button
              onClick={() => setStep(3)}
              style={btnSecondary}
              disabled={!!submitResult}
            >
              上一步
            </button>
            {!submitResult && (
              <button
                onClick={handleSubmit}
                disabled={submitting}
                style={{
                  ...btnPrimary,
                  padding: "12px 32px",
                  fontSize: "15px",
                  opacity: submitting ? 0.5 : 1,
                }}
              >
                {submitting ? "提交中..." : "创建演进实验"}
              </button>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
