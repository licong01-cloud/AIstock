"use client";

import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8001/api/v1";

interface RDStrategy {
  strategy_id: string;
  strategy_name: string;
  strategy_kind: string;
  output_mode: string;
  enabled: boolean;
  created_at: string | null;
  updated_at: string | null;
  source_strategy_key: string;
}

interface RDStrategyVersion {
  strategy_version_id: string;
  version_tag: string;
  artifact_root_path: string;
  import_status: string;
  created_at: string | null;
}

interface BestLoopSummary {
  strategy_id: string;
  task_run_id: string;
  loop_id: number;
  status: string | null;
  has_result: boolean | null;
  metrics: Record<string, any> | null;
  decision: string | null;
  summary_execution: string | null;
  summary_value_feedback: string | null;
  summary_shape_feedback: string | null;
}

function formatDateTime(value: string | null) {
  if (!value) return "-";
  try {
    const d = new Date(value);
    return d.toLocaleString("zh-CN");
  } catch {
    return value;
  }
}

function openStrategyCatalog(strategyId: string) {
  const url = `/rdagent/strategies-catalog?strategy_id=${encodeURIComponent(
    strategyId,
  )}`;
  window.open(url, "_blank");
}

export default function RDagentStrategiesPage() {
  const [strategies, setStrategies] = useState<RDStrategy[]>([]);
  const [versions, setVersions] = useState<Record<string, RDStrategyVersion[]>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [resultLoading, setResultLoading] = useState(false);
  const [activeResultStrategyId, setActiveResultStrategyId] = useState<string | null>(null);
  const [results, setResults] = useState<Record<string, any>>({});
  const [fullCurveStrategyId, setFullCurveStrategyId] = useState<string | null>(null);
  const [bestLoops, setBestLoops] = useState<Record<string, BestLoopSummary>>({});

  useEffect(() => {
    loadStrategies();
    loadBestLoops();
  }, []);

  async function loadStrategies() {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}/rdagent/strategies`);
      if (!res.ok) throw new Error(`加载策略失败: ${res.status}`);
      const data = await res.json();
      setStrategies(data.items || []);
    } catch (e: any) {
      setError(e?.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }

  async function loadBestLoops() {
    try {
      const res = await fetch(`${API_BASE}/rdagent/catalogs/strategy-loop-best`);
      if (!res.ok) throw new Error(`加载最佳 loop 摘要失败: ${res.status}`);
      const data = await res.json();
      const map: Record<string, BestLoopSummary> = {};
      (data.items || []).forEach((item: BestLoopSummary) => {
        if (item.strategy_id) {
          map[item.strategy_id] = item;
        }
      });
      setBestLoops(map);
    } catch (e) {
      // 聚合失败不阻塞主页面，仅在控制台打印
      console.error(e);
    }
  }

  async function loadStrategyResult(strategyId: string) {
    if (results[strategyId]) {
      // toggle
      setActiveResultStrategyId((prev) => (prev === strategyId ? null : strategyId));
      return;
    }
    setResultLoading(true);
    try {
      const res = await fetch(`${API_BASE}/rdagent/strategies/${strategyId}/result`);
      if (!res.ok) throw new Error(`加载回测结果失败: ${res.status}`);
      const data = await res.json();
      setResults((prev) => ({ ...prev, [strategyId]: data }));
      setActiveResultStrategyId(strategyId);
    } catch (e: any) {
      console.error(e);
      alert(e?.message || "加载回测结果失败");
    } finally {
      setResultLoading(false);
    }
  }

  async function loadVersions(strategyId: string) {
    if (versions[strategyId]) return;
    try {
      const res = await fetch(`${API_BASE}/rdagent/strategies/${strategyId}/versions`);
      if (!res.ok) throw new Error(`加载版本失败: ${res.status}`);
      const data = await res.json();
      setVersions((prev) => ({ ...prev, [strategyId]: data.items || [] }));
    } catch (e) {
      console.error(e);
    }
  }

  return (
    <main style={{ padding: 24 }}>
      <section
        style={{
          background: "linear-gradient(135deg, #0ea5e9 0%, #6366f1 100%)",
          borderRadius: 16,
          padding: 20,
          color: "#fff",
          marginBottom: 16,
        }}
      >
        <h1 style={{ margin: 0, fontSize: 24 }}>RD-Agent 策略管理</h1>
        <p style={{ marginTop: 8, opacity: 0.9, fontSize: 13 }}>
          浏览已从 RD-Agent 导入到 AIstock 的策略及版本（仅 RDagent 视角）
        </p>
      </section>

      {error && (
        <div
          style={{
            padding: 12,
            background: "#fee2e2",
            border: "1px solid #fecaca",
            borderRadius: 8,
            marginBottom: 16,
            color: "#b91c1c",
          }}
        >
          {error}
        </div>
      )}

      <section
        style={{
          background: "#fff",
          borderRadius: 12,
          padding: 20,
          boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
        }}
      >
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: 16,
          }}
        >
          <h2 style={{ margin: 0 }}>已导入 RD-Agent 策略</h2>
        </div>

        {loading ? (
          <div>加载中...</div>
        ) : strategies.length === 0 ? (
          <div style={{ padding: 40, textAlign: "center", color: "#6b7280" }}>
            暂无已导入的 RD-Agent 策略。
          </div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ background: "#f9fafb" }}>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>策略ID</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>名称</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>概览</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>RD-Agent 实验最佳性能</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>状态</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>创建时间</th>
                  <th style={{ padding: 12, textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>版本 / 回测 / Catalog 联动</th>
                </tr>
              </thead>
              <tbody>
                {strategies.map((s) => (
                  <tr key={s.strategy_id} style={{ borderBottom: "1px solid #e5e7eb" }}>
                    <td style={{ padding: 12, fontSize: 11 }}>{s.strategy_id}</td>
                    <td style={{ padding: 12 }}>
                      <div style={{ fontWeight: 600 }}>{s.strategy_name}</div>
                      <div style={{ marginTop: 4, fontSize: 12, color: "#4b5563" }}>
                        {s.strategy_kind === "portfolio" ? "组合策略" : "单标的策略"}
                        {" · "}
                        {s.output_mode === "target_weight" ? "目标权重" : "TopK 排名"}
                      </div>
                      <div style={{ marginTop: 4, fontSize: 11, color: "#6b7280" }}>
                        来源：RD-Agent（{s.source_strategy_key}）
                      </div>
                    </td>
                    <td style={{ padding: 12, minWidth: 220 }}>
                      {(() => {
                        const best = bestLoops[s.strategy_id];
                        if (!best || !best.metrics) {
                          return (
                            <div style={{ fontSize: 11, color: "#9ca3af" }}>
                              暂无 loop 指标摘要
                            </div>
                          );
                        }
                        const m = best.metrics;
                        const pick = (keys: string[]) => {
                          for (const k of keys) {
                            if (m[k] !== undefined && m[k] !== null) return m[k];
                          }
                          return undefined;
                        };
                        const ann = pick(["ann_ret", "annual_return", "annualized_return"]);
                        const mdd = pick(["mdd", "max_drawdown"]);
                        const ic = pick(["ic_mean", "ic", "IC"]);

                        // Fallback logic for Unnamed: 0 / 0 format
                        let displayAnn = ann;
                        if (displayAnn === undefined && m["Unnamed: 0"]?.includes("annualized_return")) {
                          displayAnn = m["0"];
                        }
                        let displayMdd = mdd;
                        if (displayMdd === undefined && m["Unnamed: 0"]?.includes("max_drawdown")) {
                          displayMdd = m["0"];
                        }
                        let displayIc = ic;
                        if (displayIc === undefined && m["Unnamed: 0"]?.includes("ic_mean")) {
                          displayIc = m["0"];
                        }

                        const fmt = (v: any) => {
                          if (v === undefined || v === null) return "-";
                          const num = Number(v);
                          return Number.isFinite(num) ? num.toFixed(4) : String(v).slice(0, 12);
                        };
                        return (
                          <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                            <div
                              style={{
                                padding: "4px 6px",
                                borderRadius: 6,
                                background: "#ecfeff",
                                fontSize: 11,
                              }}
                            >
                              <div style={{ color: "#64748b" }}>年化收益</div>
                              <div style={{ fontWeight: 600 }}>{fmt(displayAnn)}</div>
                            </div>
                            <div
                              style={{
                                padding: "4px 6px",
                                borderRadius: 6,
                                background: "#fef9c3",
                                fontSize: 11,
                              }}
                            >
                              <div style={{ color: "#64748b" }}>最大回撤</div>
                              <div style={{ fontWeight: 600 }}>{fmt(displayMdd)}</div>
                            </div>
                            <div
                              style={{
                                padding: "4px 6px",
                                borderRadius: 6,
                                background: "#f5f3ff",
                                fontSize: 11,
                              }}
                            >
                              <div style={{ color: "#64748b" }}>IC(mean)</div>
                              <div style={{ fontWeight: 600 }}>{fmt(displayIc)}</div>
                            </div>
                          </div>
                        );
                      })()}
                    </td>
                    <td style={{ padding: 12 }}>
                      <span
                        style={{
                          padding: "4px 8px",
                          borderRadius: 999,
                          backgroundColor: s.enabled ? "#dcfce7" : "#fee2e2",
                          color: s.enabled ? "#166534" : "#991b1b",
                          fontSize: 12,
                        }}
                      >
                        {s.enabled ? "启用" : "禁用"}
                      </span>
                    </td>
                    <td style={{ padding: 12 }}>{formatDateTime(s.created_at)}</td>
                    <td style={{ padding: 12, minWidth: 320 }}>
                      <button
                        type="button"
                        onClick={() => loadVersions(s.strategy_id)}
                        style={{
                          padding: "4px 8px",
                          borderRadius: 6,
                          border: "1px solid #e5e7eb",
                          background: "#f9fafb",
                          fontSize: 12,
                          cursor: "pointer",
                        }}
                      >
                        展开版本
                      </button>
                      <button
                        type="button"
                        onClick={() => loadStrategyResult(s.strategy_id)}
                        style={{
                          padding: "4px 8px",
                          borderRadius: 6,
                          border: "1px solid #e5e7eb",
                          background: "#eef2ff",
                          fontSize: 12,
                          cursor: "pointer",
                          marginLeft: 8,
                        }}
                        disabled={resultLoading && activeResultStrategyId === s.strategy_id}
                      >
                        {activeResultStrategyId === s.strategy_id ? "隐藏结果" : "查看结果"}
                      </button>
                      <button
                        type="button"
                        onClick={() => openStrategyCatalog(s.strategy_id)}
                        style={{
                          padding: "4px 8px",
                          borderRadius: 6,
                          border: "1px solid #e5e7eb",
                          background: "#f9fafb",
                          fontSize: 12,
                          cursor: "pointer",
                          marginLeft: 8,
                        }}
                      >
                        策略目录
                      </button>
                      {versions[s.strategy_id] && versions[s.strategy_id].length > 0 && (
                        <ul style={{ marginTop: 8, paddingLeft: 16, fontSize: 12 }}>
                          {versions[s.strategy_id].map((v) => (
                            <li key={v.strategy_version_id} style={{ marginBottom: 4 }}>
                              <span style={{ fontWeight: 600 }}>{v.version_tag}</span>
                              <span style={{ marginLeft: 8, color: "#6b7280" }}>
                                {formatDateTime(v.created_at)}
                              </span>
                            </li>
                          ))}
                        </ul>
                      )}
                      {activeResultStrategyId === s.strategy_id && results[s.strategy_id] && (
                        <div
                          style={{
                            marginTop: 12,
                            padding: 14,
                            borderRadius: 10,
                            border: "1px solid #e5e7eb",
                            background: "#f9fafb",
                            maxWidth: 900,
                          }}
                        >
                          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
                            <div style={{ fontSize: 12, fontWeight: 600 }}>回测结果概览</div>
                            <div style={{ fontSize: 11, color: "#6b7280" }}>
                              workspace: {results[s.strategy_id]?.workspace_path || "-"}
                            </div>
                          </div>
                          <div style={{ fontSize: 11, color: "#6b7280", marginBottom: 6 }}>
                            {(() => {
                              const curve = (results[s.strategy_id]?.equity_curve || []) as any[];
                              if (!curve.length) return "";
                              const first = curve[0];
                              const last = curve[curve.length - 1];
                              const startLabel = first.date || first.index || 0;
                              const endLabel = last.date || last.index || curve.length - 1;
                              return `回测区间：${startLabel} ~ ${endLabel}（${curve.length} 点位）`;
                            })()}
                          </div>
                          <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginBottom: 10 }}>
                            {(() => {
                              const m = results[s.strategy_id]?.metrics || {};
                              const cards: { label: string; key: string }[] = [
                                { label: "年化收益", key: "ann_ret" },
                                { label: "年化收益", key: "annual_return" },
                                { label: "最大回撤", key: "max_drawdown" },
                                { label: "最大回撤", key: "mdd" },
                                { label: "Sharpe", key: "sharpe" },
                                { label: "信息比率", key: "information_ratio" },
                                { label: "IC(mean)", key: "ic_mean" },
                              ];
                              return cards
                                .filter((c) => m[c.key] !== undefined)
                                .map((c) => (
                                  <div
                                    key={c.key}
                                    style={{
                                      padding: "6px 8px",
                                      borderRadius: 6,
                                      background: "#eef2ff",
                                      fontSize: 11,
                                      minWidth: 110,
                                    }}
                                  >
                                    <div style={{ color: "#6b7280" }}>{c.label}</div>
                                    <div style={{ fontWeight: 600 }}>{String(m[c.key]).slice(0, 12)}</div>
                                  </div>
                                ));
                            })()}
                          </div>
                          <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 4 }}>净值曲线（采样展示，最多 80 行）</div>
                          <div
                            style={{
                              maxHeight: 140,
                              overflow: "auto",
                              borderRadius: 6,
                              border: "1px solid #e5e7eb",
                              background: "#ffffff",
                            }}
                          >
                            <table style={{ width: "100%", borderCollapse: "collapse" }}>
                              <thead>
                                <tr style={{ background: "#f9fafb" }}>
                                  <th style={{ padding: 4, textAlign: "left", borderBottom: "1px solid #e5e7eb" }}>
                                    日期/索引
                                  </th>
                                  <th style={{ padding: 4, textAlign: "right", borderBottom: "1px solid #e5e7eb" }}>NAV</th>
                                </tr>
                              </thead>
                              <tbody>
                                {(results[s.strategy_id]?.equity_curve || []).slice(0, 80).map((p: any, idx: number) => (
                                  <tr key={idx} style={{ borderBottom: "1px solid #f3f4f6" }}>
                                    <td style={{ padding: 4, fontSize: 11 }}>
                                      {p.date || p.index || idx}
                                    </td>
                                    <td style={{ padding: 4, fontSize: 11, textAlign: "right" }}>
                                      {p.nav !== undefined ? p.nav.toFixed ? p.nav.toFixed(4) : String(p.nav) : "-"}
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                          <div style={{ marginTop: 8, textAlign: "right" }}>
                            <button
                              type="button"
                              onClick={() => setFullCurveStrategyId(s.strategy_id)}
                              style={{
                                padding: "4px 8px",
                                borderRadius: 6,
                                border: "1px solid #c7d2fe",
                                background: "#e0e7ff",
                                fontSize: 11,
                                cursor: "pointer",
                              }}
                            >
                              查看完整曲线图
                            </button>
                          </div>
                        </div>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
      {fullCurveStrategyId && results[fullCurveStrategyId] && (
        <div
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(0,0,0,0.55)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            zIndex: 1000,
          }}
        >
          <div
            style={{
              background: "#fff",
              borderRadius: 12,
              padding: 20,
              width: "96%",
              maxWidth: 1400,
              height: "80vh",
              overflow: "auto",
              boxShadow: "0 10px 40px rgba(0,0,0,0.25)",
            }}
          >
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                marginBottom: 12,
              }}
            >
              <div>
                <div style={{ fontSize: 14, fontWeight: 600 }}>净值曲线（完整回测周期）</div>
                <div style={{ fontSize: 11, color: "#6b7280", marginTop: 2 }}>
                  strategy_id: {fullCurveStrategyId}
                </div>
              </div>
              <button
                type="button"
                onClick={() => setFullCurveStrategyId(null)}
                style={{
                  padding: "6px 12px",
                  borderRadius: 6,
                  border: "1px solid #e5e7eb",
                  background: "#f9fafb",
                  cursor: "pointer",
                  fontSize: 13,
                }}
              >
                关闭
              </button>
            </div>
            {(() => {
              const curve = (results[fullCurveStrategyId]?.equity_curve || []) as any[];
              if (!curve.length) {
                return <div style={{ fontSize: 12, color: "#6b7280" }}>暂无净值数据。</div>;
              }
              const navs = curve.map((p) => Number(p.nav));
              const minNav = Math.min(...navs);
              const maxNav = Math.max(...navs);
              const span = maxNav - minNav || 1;
              const innerXMin = 10;
              const innerXMax = 110;
              const innerYMin = 5;
              const innerYMax = 95;
              const innerYSpan = innerYMax - innerYMin;
              const points = curve
                .map((p, idx) => {
                  const t = curve.length > 1 ? idx / (curve.length - 1) : 0;
                  const x = innerXMin + t * (innerXMax - innerXMin);
                  const rel = (Number(p.nav) - minNav) / span;
                  const y = innerYMax - rel * innerYSpan;
                  return `${x},${y}`;
                })
                .join(" ");
              const first = curve[0];
              const last = curve[curve.length - 1];
              const startLabel = first.date || first.index || 0;
              const endLabel = last.date || last.index || curve.length - 1;
              const xTickIdxRaw = [0, 0.25, 0.5, 0.75, 1].map((r) =>
                Math.min(curve.length - 1, Math.max(0, Math.round(r * (curve.length - 1)))),
              );
              const xTickIdx = Array.from(new Set(xTickIdxRaw));
              return (
                <div>
                  <div style={{ fontSize: 12, color: "#4b5563", marginBottom: 6 }}>
                    区间：{startLabel} ~ {endLabel}（{curve.length} 点位），NAV 范围：{minNav.toFixed(4)} ~ {maxNav.toFixed(4)}
                  </div>
                  <div
                    style={{
                      borderRadius: 8,
                      border: "1px solid #e5e7eb",
                      background: "#f9fafb",
                      padding: 10,
                      height: "calc(100% - 32px)",
                      minHeight: 260,
                    }}
                  >
                    <svg viewBox="0 0 120 110" preserveAspectRatio="none" style={{ width: "100%", height: "100%" }}>
                      <defs>
                        <linearGradient id="nav-line" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="0%" stopColor="#4f46e5" />
                          <stop offset="100%" stopColor="#22c55e" />
                        </linearGradient>
                      </defs>
                      <rect x="0" y="0" width="120" height="110" fill="#f9fafb" />
                      {/* y 轴 */}
                      <line x1="10" y1="5" x2="10" y2="95" stroke="#d1d5db" strokeWidth="0.5" />
                      {/* x 轴 */}
                      <line x1="10" y1="95" x2="110" y2="95" stroke="#d1d5db" strokeWidth="0.5" />
                      {/* y 轴刻度和标签（5 段） */}
                      {Array.from({ length: 5 }).map((_, i) => {
                        const t = i / 4;
                        const y = innerYMax - t * innerYSpan;
                        const nav = minNav + t * span;
                        return (
                          <g key={i}>
                            <line x1="10" y1={y} x2="110" y2={y} stroke="#e5e7eb" strokeWidth="0.3" />
                            <text
                              x="8"
                              y={y + 1.5}
                              fontSize="3"
                              textAnchor="end"
                              fill="#6b7280"
                            >
                              {nav.toFixed(2)}
                            </text>
                          </g>
                        );
                      })}
                      {/* x 轴刻度和标签：起点/中点/终点 */}
                      {xTickIdx.map((idx, i) => {
                        const t = curve.length > 1 ? idx / (curve.length - 1) : 0;
                        const x = innerXMin + t * (innerXMax - innerXMin);
                        const pt = curve[idx];
                        const labelSrc = pt.date || pt.index || idx;
                        const label = typeof labelSrc === "string" ? labelSrc.slice(0, 10) : String(labelSrc);
                        return (
                          <g key={i}>
                            <line x1={x} y1="95" x2={x} y2="97" stroke="#9ca3af" strokeWidth="0.5" />
                            <text
                              x={x}
                              y="103"
                              fontSize="3"
                              textAnchor="middle"
                              fill="#6b7280"
                            >
                              {label}
                            </text>
                          </g>
                        );
                      })}
                      {/* 轴标签 */}
                      <text x="2" y="8" fontSize="3" fill="#6b7280" writingMode="tb-rl">
                        净值（NAV）
                      </text>
                      <text x="60" y="108" fontSize="3" textAnchor="middle" fill="#6b7280">
                        时间 / 序号
                      </text>
                      <polyline
                        fill="none"
                        stroke="url(#nav-line)"
                        strokeWidth={1.2}
                        points={points}
                      />
                    </svg>
                  </div>
                </div>
              );
            })()}
          </div>
        </div>
      )}
    </main>
  );
}
