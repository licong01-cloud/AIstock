"use client";

import React from "react";

const panelCardStyle: React.CSSProperties = {
  backgroundColor: "#ffffff",
  border: "1px solid #e2e8f0",
  borderRadius: 12,
  boxShadow: "0 1px 3px rgba(15, 23, 42, 0.06)",
  overflow: "hidden",
};

const sectionHeaderStyle: React.CSSProperties = {
  padding: "14px 18px",
  borderBottom: "1px solid #e2e8f0",
  backgroundColor: "#f8fafc",
};

const thStyle: React.CSSProperties = {
  padding: "9px 10px",
  textAlign: "left",
  color: "#475569",
  fontSize: 12,
  fontWeight: 800,
  borderBottom: "1px solid #e2e8f0",
  backgroundColor: "#f8fafc",
};

const tdStyle: React.CSSProperties = {
  padding: "9px 10px",
  borderBottom: "1px solid #f1f5f9",
  color: "#334155",
  fontSize: 12,
  verticalAlign: "top",
};

type MetricMap = Record<string, unknown>;

export type CombineDiagnosticsLoop = {
  loop_id?: string;
  run_id?: string;
  loop_index: number;
  status: string;
  is_sota?: boolean;
  config_json?: Record<string, any>;
  metrics_json?: MetricMap;
  weights_json?: Record<string, any>;
  per_window_weights_json?: any[];
  loo?: Array<Record<string, any>>;
  oos_start?: string | null;
  oos_end?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

type CombineDiagnosticsTask = {
  task_id?: string;
  task_name?: string;
  status?: string;
  current_loop?: number;
  max_loops?: number;
  roster_hash?: string;
  normalize_method?: string;
  walk_forward_signature?: string;
  updated_at?: string;
};

type FusionDiagnosticRow = {
  symbol: string;
  rank: number;
  score: number;
  fusion_score?: number;
  source_package_ids?: string[];
  support_count?: number;
  rank_dispersion?: number;
  package_presence?: Record<string, string>;
};

type FusionDiagnostics = {
  ok?: boolean;
  run_id: string;
  mode: string;
  package_ids: string[];
  fusion_method?: string;
  fusion_policy_sha256?: string;
  diagnostics: FusionDiagnosticRow[];
};

type LegDiagnosticRow = {
  legId: string;
  seedCount: number | null;
  avgWeight: number | null;
  latestWeight: number | null;
  minWeight: number | null;
  maxWeight: number | null;
  marginalCagrAvg: number | null;
  marginalSharpeAvg: number | null;
  marginalCalmarAvg: number | null;
  negativeLooCount: number;
  missingWeightCount: number;
  missingLooCount: number;
};

type Bottleneck = {
  severity: "high" | "medium" | "info";
  title: string;
  message: string;
  evidence: string;
};

type Gap = {
  field: string;
  reason: string;
  impact: string;
};

type Props = {
  apiBase: string;
  taskKey: string;
  task: CombineDiagnosticsTask | null;
  loops: CombineDiagnosticsLoop[];
  selectedScheme: string;
  selectionRunId?: string | null;
};

function asNumber(value: unknown): number | null {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function avg(values: Array<number | null>): number | null {
  const finite = values.filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  if (finite.length === 0) return null;
  return finite.reduce((sum, value) => sum + value, 0) / finite.length;
}

function fmtNum(value: unknown, digits = 3): string {
  const parsed = asNumber(value);
  return parsed == null ? "-" : parsed.toFixed(digits);
}

function fmtPct(value: unknown, digits = 2): string {
  const parsed = asNumber(value);
  return parsed == null ? "-" : `${(parsed * 100).toFixed(digits)}%`;
}

function shortText(value: unknown, max = 42): string {
  const text = String(value ?? "-");
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

function statusColor(status: string): string {
  if (status === "completed" || status === "succeeded") return "#047857";
  if (status === "running") return "#0369a1";
  if (status === "failed" || status === "partial_failed") return "#b91c1c";
  return "#64748b";
}

function severityStyle(severity: Bottleneck["severity"]): React.CSSProperties {
  if (severity === "high") return { borderColor: "#fecaca", backgroundColor: "#fef2f2", color: "#991b1b" };
  if (severity === "medium") return { borderColor: "#fed7aa", backgroundColor: "#fffbeb", color: "#92400e" };
  return { borderColor: "#bfdbfe", backgroundColor: "#eff6ff", color: "#1e3a8a" };
}

function extractRoster(loops: CombineDiagnosticsLoop[]): Array<Record<string, any>> {
  for (const loop of loops) {
    const roster = loop.config_json?.roster;
    if (Array.isArray(roster) && roster.length > 0) return roster;
  }
  return [];
}

function legIdOf(item: Record<string, any>, index: number): string {
  return String(item.leg_id || item.id || item.name || `leg_${index + 1}`);
}

function extractWeights(loop: CombineDiagnosticsLoop): Record<string, unknown> {
  const direct = loop.weights_json || loop.config_json?.weights_json || {};
  if (direct.leg_weights && typeof direct.leg_weights === "object") return direct.leg_weights;
  if (direct.weights && typeof direct.weights === "object") return direct.weights;
  return direct;
}

function buildLegRows(loops: CombineDiagnosticsLoop[]): LegDiagnosticRow[] {
  const roster = extractRoster(loops);
  return roster.map((leg, index) => {
    const legId = legIdOf(leg, index);
    const seedRunIds = Array.isArray(leg.seed_run_ids) ? leg.seed_run_ids : null;
    const weights = loops.map((loop) => asNumber(extractWeights(loop)[legId]));
    const latestWeight = [...weights].reverse().find((value) => value != null) ?? null;
    const looByLoop = loops.map((loop) => (Array.isArray(loop.loo) ? loop.loo : []).find((row) => row.dropped_leg_id === legId) || null);
    const cagrValues = looByLoop.map((row) => asNumber(row?.marginal_cagr));
    const sharpeValues = looByLoop.map((row) => asNumber(row?.marginal_sharpe));
    const calmarValues = looByLoop.map((row) => asNumber(row?.marginal_calmar));
    const negativeLooCount = looByLoop.filter((row) => {
      if (!row) return false;
      return [row.marginal_cagr, row.marginal_sharpe, row.marginal_calmar].some((value) => {
        const parsed = asNumber(value);
        return parsed != null && parsed <= 0;
      });
    }).length;

    return {
      legId,
      seedCount: seedRunIds ? seedRunIds.length : null,
      avgWeight: avg(weights),
      latestWeight,
      minWeight: weights.some((value) => value != null) ? Math.min(...weights.filter((value): value is number => value != null)) : null,
      maxWeight: weights.some((value) => value != null) ? Math.max(...weights.filter((value): value is number => value != null)) : null,
      marginalCagrAvg: avg(cagrValues),
      marginalSharpeAvg: avg(sharpeValues),
      marginalCalmarAvg: avg(calmarValues),
      negativeLooCount,
      missingWeightCount: weights.filter((value) => value == null).length,
      missingLooCount: looByLoop.filter((row) => !row).length,
    };
  });
}

function buildBottlenecks(loops: CombineDiagnosticsLoop[], legRows: LegDiagnosticRow[]): Bottleneck[] {
  const items: Bottleneck[] = [];
  const failedLoops = loops.filter((loop) => loop.status === "failed" || loop.status === "partial_failed");
  if (failedLoops.length > 0) {
    items.push({
      severity: "high",
      title: "存在失败回测窗口",
      message: `${failedLoops.length}/${loops.length} 个 combine run 未成功，诊断仅基于已落库字段渲染。`,
      evidence: failedLoops.map((loop) => loop.run_id || loop.loop_id || `配置${loop.loop_index}`).join(", "),
    });
  }

  for (const leg of legRows) {
    if (leg.avgWeight != null && leg.avgWeight <= 0.05) {
      items.push({
        severity: "medium",
        title: "腿权重接近 0",
        message: `${leg.legId} 在 ${legRows.length} 腿组合中的平均权重 <= 5%。`,
        evidence: `avg_weight=${fmtNum(leg.avgWeight, 4)}, latest_weight=${fmtNum(leg.latestWeight, 4)}`,
      });
    }
    if ((leg.marginalCagrAvg != null && leg.marginalCagrAvg <= 0) || (leg.marginalSharpeAvg != null && leg.marginalSharpeAvg <= 0)) {
      items.push({
        severity: "medium",
        title: "LOO 边际贡献偏负",
        message: `${leg.legId} 的 leave-one-out 边际贡献含负值，可能拖累组合。`,
        evidence: `avg_marginal_cagr=${fmtPct(leg.marginalCagrAvg)}, avg_marginal_sharpe=${fmtNum(leg.marginalSharpeAvg, 3)}, negative_rows=${leg.negativeLooCount}`,
      });
    }
    if (leg.missingWeightCount > 0 || leg.missingLooCount > 0) {
      items.push({
        severity: "info",
        title: "腿级诊断字段不完整",
        message: `${leg.legId} 缺少部分 scheme/LOO 行；页面显示 '-'，不补默认值。`,
        evidence: `missing_weight_rows=${leg.missingWeightCount}, missing_loo_rows=${leg.missingLooCount}`,
      });
    }
  }

  if (items.length === 0) {
    items.push({
      severity: "info",
      title: "未触发内置瓶颈规则",
      message: "当前 combine _scheme_result/_loo 字段未显示失败窗口、低权重或负边际贡献。",
      evidence: "这是可用字段上的规则结果，不代表训练/预测/相关性字段已完整。",
    });
  }
  return items;
}

function buildGaps(hasFusionDiagnostics: boolean, selectionRunId: string): Gap[] {
  return [
    {
      field: "组间相关 / pairwise correlation matrix",
      reason: "combine task payload 暂未暴露腿间预测相关矩阵；本页不从旧 alpha_mode=multi diagnostics 推断。",
      impact: "相关性热力图保留为缺口说明；需要正交性时继续使用独立 orthogonality 工具。",
    },
    {
      field: "训练过程 / prediction diagnostics",
      reason: "combine _scheme_result/_loo 是组合回测结果表，不包含每腿训练 loss、预测分布、feature importance。",
      impact: "本页仅渲染真实的 scheme 指标、权重、LOO 和 run 证据，不伪造训练/预测诊断。",
    },
    {
      field: "Selection Center fusion diagnostics binding",
      reason: hasFusionDiagnostics
        ? "已通过显式 selection_run_id 读取 /selection-center/runs/{id}/fusion-diagnostics。"
        : (selectionRunId ? "fusion-diagnostics 读取失败或尚未返回可用数据。" : "当前 combine task payload 未绑定 selection_center run_id。"),
      impact: hasFusionDiagnostics ? "可查看真实融合股票诊断。" : "如需要股票级融合证据，请粘贴 Selection Center run_id 后读取。",
    },
    {
      field: "腿来源精确溯源",
      reason: "当前前端只消费 combine task 已返回的 roster/seed_run_ids；若后端未物化 factor/model/source loop，本页不补齐。",
      impact: "腿来源细节缺失时展示 seed 数和 leg_id，不把缺失来源显示为已解析。",
    },
  ];
}

function endpointErrorMessage(payload: any, fallback: string): string {
  const detail = payload?.detail || payload?.error || payload?.message;
  if (!detail) return fallback;
  if (typeof detail === "string") return detail;
  if (typeof detail === "object") {
    const reason = detail.reason_code ? `reason_code=${detail.reason_code}` : "";
    const message = detail.message || detail.detail || fallback;
    return [reason, message].filter(Boolean).join(": ");
  }
  return fallback;
}

export default function CombineDiagnosticsPanel({
  apiBase,
  taskKey,
  task,
  loops,
  selectedScheme,
  selectionRunId,
}: Props) {
  const [fusionRunId, setFusionRunId] = React.useState(selectionRunId || "");
  const [fusionData, setFusionData] = React.useState<FusionDiagnostics | null>(null);
  const [fusionError, setFusionError] = React.useState<string | null>(null);
  const [fusionLoading, setFusionLoading] = React.useState(false);

  const legRows = React.useMemo(() => buildLegRows(loops), [loops]);
  const bottlenecks = React.useMemo(() => buildBottlenecks(loops, legRows), [loops, legRows]);
  const dataGaps = React.useMemo(() => buildGaps(Boolean(fusionData), fusionRunId.trim()), [fusionData, fusionRunId]);
  const runIds = React.useMemo(() => loops.map((loop) => loop.run_id).filter(Boolean) as string[], [loops]);
  const completedLoops = loops.filter((loop) => loop.status === "completed").length;
  const bestLoop = loops.find((loop) => loop.is_sota);
  const perWindowPoints = loops.reduce((sum, loop) => sum + (Array.isArray(loop.per_window_weights_json) ? loop.per_window_weights_json.length : 0), 0);

  const fetchFusionDiagnostics = React.useCallback(async (runId: string) => {
    setFusionLoading(true);
    setFusionError(null);
    setFusionData(null);
    try {
      const response = await fetch(`${apiBase}/selection-center/runs/${encodeURIComponent(runId)}/fusion-diagnostics`);
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload.ok !== true) {
        throw new Error(endpointErrorMessage(payload, `HTTP ${response.status}`));
      }
      setFusionData(payload as FusionDiagnostics);
    } catch (error) {
      setFusionError(error instanceof Error ? error.message : String(error));
    } finally {
      setFusionLoading(false);
    }
  }, [apiBase]);

  const loadFusionDiagnostics = React.useCallback(async () => {
    const runId = fusionRunId.trim();
    if (!runId) {
      setFusionError("请先输入 Selection Center run_id；不会用 combine task_key 伪造。");
      return;
    }
    await fetchFusionDiagnostics(runId);
  }, [fetchFusionDiagnostics, fusionRunId]);

  React.useEffect(() => {
    const runId = selectionRunId?.trim();
    if (!runId) return;
    setFusionRunId(runId);
    void fetchFusionDiagnostics(runId);
  }, [fetchFusionDiagnostics, selectionRunId]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <div style={panelCardStyle}>
        <div style={sectionHeaderStyle}>
          <h2 style={{ margin: 0, fontSize: 16, color: "#0f172a" }}>Combine Diagnostics</h2>
          <p style={{ margin: "6px 0 0", fontSize: 12, color: "#64748b" }}>
            数据源：combine task + strategy_pkg.multi_alpha_combine_backtest_scheme_result/_loo；旧 alpha_mode=multi diagnostics 已下线。
          </p>
        </div>
        <div style={{ padding: 18, display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))", gap: 12 }}>
          {[
            ["Task", task?.task_id || taskKey],
            ["Scheme", selectedScheme || "-"],
            ["状态", task?.status || "-"],
            ["完成窗口", `${completedLoops}/${loops.length}`],
            ["最佳 run", bestLoop?.run_id || "-"],
            ["Roster hash", task?.roster_hash || "-"],
            ["Normalize", task?.normalize_method || "-"],
            ["滚动权重点", String(perWindowPoints)],
          ].map(([label, value]) => (
            <div key={label} style={{ border: "1px solid #e2e8f0", borderRadius: 8, padding: "10px 12px", backgroundColor: "#f8fafc", minWidth: 0 }}>
              <div style={{ fontSize: 11, color: "#64748b", fontWeight: 700 }}>{label}</div>
              <div title={String(value)} style={{ marginTop: 4, fontSize: 13, color: "#0f172a", fontFamily: "monospace", fontWeight: 800, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {shortText(value, 32)}
              </div>
            </div>
          ))}
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "minmax(0, 1.2fr) minmax(320px, 0.8fr)", gap: 16 }}>
        <div style={panelCardStyle}>
          <div style={sectionHeaderStyle}>
            <h3 style={{ margin: 0, fontSize: 14, color: "#0f766e" }}>权重 / Meta-weight 等价视图</h3>
          </div>
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th style={thStyle}>leg_id</th>
                  <th style={{ ...thStyle, textAlign: "right" }}>seed</th>
                  <th style={{ ...thStyle, textAlign: "right" }}>最新权重</th>
                  <th style={{ ...thStyle, textAlign: "right" }}>平均权重</th>
                  <th style={{ ...thStyle, textAlign: "right" }}>LOO ΔCAGR</th>
                  <th style={{ ...thStyle, textAlign: "right" }}>LOO ΔSharpe</th>
                  <th style={{ ...thStyle, textAlign: "right" }}>负贡献行</th>
                </tr>
              </thead>
              <tbody>
                {legRows.map((row) => (
                  <tr key={row.legId}>
                    <td style={{ ...tdStyle, fontFamily: "monospace", fontWeight: 800 }}>{row.legId}</td>
                    <td style={{ ...tdStyle, textAlign: "right" }}>{row.seedCount ?? "-"}</td>
                    <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace" }}>{fmtNum(row.latestWeight, 4)}</td>
                    <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace" }}>{fmtNum(row.avgWeight, 4)}</td>
                    <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace", color: row.marginalCagrAvg != null && row.marginalCagrAvg <= 0 ? "#b91c1c" : "#334155" }}>{fmtPct(row.marginalCagrAvg)}</td>
                    <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace", color: row.marginalSharpeAvg != null && row.marginalSharpeAvg <= 0 ? "#b91c1c" : "#334155" }}>{fmtNum(row.marginalSharpeAvg, 3)}</td>
                    <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace" }}>{row.negativeLooCount}</td>
                  </tr>
                ))}
                {legRows.length === 0 && (
                  <tr><td colSpan={7} style={{ ...tdStyle, textAlign: "center", color: "#94a3b8" }}>combine payload 未提供 roster 腿数据。</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div style={panelCardStyle}>
          <div style={sectionHeaderStyle}>
            <h3 style={{ margin: 0, fontSize: 14, color: "#1d4ed8" }}>组间相关 / 正交性缺口</h3>
          </div>
          <div style={{ padding: 16, color: "#475569", fontSize: 13, lineHeight: 1.7 }}>
            <p style={{ margin: "0 0 10px" }}>
              当前 combine task 只暴露 run、scheme、weights、LOO；没有腿间预测相关矩阵。本页不会把 LOO 或权重伪装成相关性。
            </p>
            <p style={{ margin: "0 0 10px" }}>
              如需真实正交性，请继续使用独立入口 <a href="/quantevolver/multi-alpha/orthogonality" style={{ color: "#2563eb", fontWeight: 800 }}>多Alpha 正交性</a>，输入可比较的 prediction-store run_id。
            </p>
            <div style={{ border: "1px solid #bfdbfe", borderRadius: 8, backgroundColor: "#eff6ff", padding: 10, fontSize: 12, color: "#1e3a8a" }}>
              当前 combine run_id: {runIds.length > 0 ? runIds.map((id) => shortText(id, 18)).join(", ") : "-"}
            </div>
          </div>
        </div>
      </div>

      <div style={panelCardStyle}>
        <div style={sectionHeaderStyle}>
          <h3 style={{ margin: 0, fontSize: 14, color: "#334155" }}>回测执行证据</h3>
        </div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr>
                <th style={thStyle}>配置</th>
                <th style={thStyle}>run_id</th>
                <th style={thStyle}>窗口</th>
                <th style={thStyle}>状态</th>
                <th style={{ ...thStyle, textAlign: "right" }}>CAGR</th>
                <th style={{ ...thStyle, textAlign: "right" }}>Sharpe</th>
                <th style={{ ...thStyle, textAlign: "right" }}>MaxDD</th>
                <th style={{ ...thStyle, textAlign: "right" }}>Turnover</th>
              </tr>
            </thead>
            <tbody>
              {loops.map((loop) => (
                <tr key={loop.loop_id || loop.run_id || loop.loop_index}>
                  <td style={{ ...tdStyle, fontFamily: "monospace", fontWeight: 800 }}>配置 {loop.loop_index}</td>
                  <td style={{ ...tdStyle, fontFamily: "monospace" }}>{loop.run_id || "-"}</td>
                  <td style={{ ...tdStyle }}>{loop.oos_start || "-"} ~ {loop.oos_end || "-"}</td>
                  <td style={{ ...tdStyle, color: statusColor(loop.status), fontWeight: 800 }}>{loop.status}</td>
                  <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace" }}>{fmtPct(loop.metrics_json?.cagr ?? loop.metrics_json?.annualized_return)}</td>
                  <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace" }}>{fmtNum(loop.metrics_json?.sharpe, 3)}</td>
                  <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace" }}>{fmtPct(loop.metrics_json?.max_drawdown)}</td>
                  <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace" }}>{fmtNum(loop.metrics_json?.turnover, 3)}</td>
                </tr>
              ))}
              {loops.length === 0 && (
                <tr><td colSpan={8} style={{ ...tdStyle, textAlign: "center", color: "#94a3b8" }}>暂无 combine run 证据。</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) minmax(0, 1fr)", gap: 16 }}>
        <div style={panelCardStyle}>
          <div style={sectionHeaderStyle}>
            <h3 style={{ margin: 0, fontSize: 14, color: "#b45309" }}>瓶颈识别</h3>
          </div>
          <div style={{ padding: 16, display: "flex", flexDirection: "column", gap: 10 }}>
            {bottlenecks.map((item, index) => (
              <div key={`${item.title}-${index}`} style={{ border: "1px solid", borderRadius: 8, padding: 12, ...severityStyle(item.severity) }}>
                <div style={{ fontSize: 12, fontWeight: 900, marginBottom: 4 }}>{item.severity.toUpperCase()} · {item.title}</div>
                <div style={{ fontSize: 13, color: "#334155", marginBottom: 6 }}>{item.message}</div>
                <div style={{ fontSize: 11, fontFamily: "monospace", color: "inherit" }}>{item.evidence}</div>
              </div>
            ))}
          </div>
        </div>

        <div style={panelCardStyle}>
          <div style={sectionHeaderStyle}>
            <h3 style={{ margin: 0, fontSize: 14, color: "#047857" }}>优化建议</h3>
          </div>
          <div style={{ padding: 16, color: "#334155", fontSize: 13, lineHeight: 1.7 }}>
            <ol style={{ margin: 0, paddingLeft: 18 }}>
              <li>优先查看 LOO ΔCAGR / ΔSharpe 为负的腿，比较移除前后的组合指标。</li>
              <li>切换详情页 scheme 下拉框，确认低权重或负贡献是否只出现在特定 weighting_scheme。</li>
              <li>存在失败窗口时先读 run_id 对应后端 reason/log；本页不把失败窗口算作成功。</li>
              <li>若需要股票级融合证据，输入 Selection Center run_id 读取真实 fusion-diagnostics。</li>
            </ol>
          </div>
        </div>
      </div>

      <div style={panelCardStyle}>
        <div style={sectionHeaderStyle}>
          <h3 style={{ margin: 0, fontSize: 14, color: "#4f46e5" }}>Selection Center fusion diagnostics（可选真实数据）</h3>
        </div>
        <div style={{ padding: 16 }}>
          <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
            <input
              value={fusionRunId}
              onChange={(event) => setFusionRunId(event.target.value)}
              placeholder="粘贴 selection-center run_id；未绑定时留空"
              style={{ flex: 1, padding: "8px 10px", border: "1px solid #cbd5e1", borderRadius: 6, fontSize: 13 }}
            />
            <button
              onClick={() => void loadFusionDiagnostics()}
              disabled={fusionLoading}
              style={{ padding: "8px 12px", border: "1px solid #4f46e5", borderRadius: 6, backgroundColor: "#eef2ff", color: "#4338ca", fontWeight: 800, cursor: fusionLoading ? "not-allowed" : "pointer" }}
            >
              {fusionLoading ? "读取中..." : "读取融合诊断"}
            </button>
          </div>
          {fusionError && (
            <div style={{ padding: 10, border: "1px solid #fecaca", borderRadius: 8, backgroundColor: "#fef2f2", color: "#991b1b", fontSize: 12, marginBottom: 12 }}>
              {fusionError}
            </div>
          )}
          {fusionData ? (
            <div style={{ overflowX: "auto" }}>
              <div style={{ marginBottom: 8, fontSize: 12, color: "#64748b" }}>
                run={fusionData.run_id} · mode={fusionData.mode} · packages={fusionData.package_ids?.length || 0} · method={fusionData.fusion_method || "-"}
              </div>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr>
                    <th style={thStyle}>symbol</th>
                    <th style={{ ...thStyle, textAlign: "right" }}>rank</th>
                    <th style={{ ...thStyle, textAlign: "right" }}>fusion_score</th>
                    <th style={{ ...thStyle, textAlign: "right" }}>support</th>
                    <th style={{ ...thStyle, textAlign: "right" }}>dispersion</th>
                    <th style={thStyle}>source packages</th>
                  </tr>
                </thead>
                <tbody>
                  {(fusionData.diagnostics || []).slice(0, 20).map((row) => (
                    <tr key={`${row.symbol}-${row.rank}`}>
                      <td style={{ ...tdStyle, fontFamily: "monospace", fontWeight: 800 }}>{row.symbol}</td>
                      <td style={{ ...tdStyle, textAlign: "right" }}>{row.rank}</td>
                      <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace" }}>{fmtNum(row.fusion_score ?? row.score, 5)}</td>
                      <td style={{ ...tdStyle, textAlign: "right" }}>{row.support_count ?? "-"}</td>
                      <td style={{ ...tdStyle, textAlign: "right", fontFamily: "monospace" }}>{fmtNum(row.rank_dispersion, 3)}</td>
                      <td style={{ ...tdStyle, fontFamily: "monospace" }}>{(row.source_package_ids || []).join(", ") || "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div style={{ color: "#64748b", fontSize: 12, lineHeight: 1.6 }}>
              combine-backtest task 当前没有 selection_run_id 绑定字段；仅在输入 run_id 后读取 Selection Center 真实接口，不使用占位数据。
            </div>
          )}
        </div>
      </div>

      <div style={panelCardStyle}>
        <div style={sectionHeaderStyle}>
          <h3 style={{ margin: 0, fontSize: 14, color: "#b91c1c" }}>字段缺口（显式列出，不伪造）</h3>
        </div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr>
                <th style={thStyle}>字段</th>
                <th style={thStyle}>缺口原因</th>
                <th style={thStyle}>影响</th>
              </tr>
            </thead>
            <tbody>
              {dataGaps.map((gap) => (
                <tr key={gap.field}>
                  <td style={{ ...tdStyle, fontWeight: 800 }}>{gap.field}</td>
                  <td style={tdStyle}>{gap.reason}</td>
                  <td style={tdStyle}>{gap.impact}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
