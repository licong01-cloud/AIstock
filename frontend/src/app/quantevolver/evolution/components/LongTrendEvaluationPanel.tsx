"use client";

import React from "react";
import { AlertTriangle, RefreshCw, TrendingUp } from "lucide-react";
import {
  QEArchiveApiError,
  qeArchiveApi,
  type JsonObject,
  type LongTrendEvaluation,
  type LongTrendEvaluationDetail,
  type LongTrendInputPreview,
  type LongTrendMetric,
} from "@/lib/qe-archive/api";

const HORIZONS = [20, 40, 60, 120, 180] as const;
const BARRIERS = [0.3, 0.5, 0.7] as const;
const FAMILIES = [
  "signal_path",
  "position_episode",
  "portfolio_result",
  "order_fill",
  "execution_cause",
  "sector_regime",
] as const;

type Props = {
  taskId: string;
  loopIndex: number;
  loopStatus?: string;
};

function asObject(value: unknown): JsonObject {
  return value && typeof value === "object" && !Array.isArray(value) ? value as JsonObject : {};
}

function asNumber(value: unknown): number | null {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function text(value: unknown, fallback = "-"): string {
  if (value === null || value === undefined || value === "") return fallback;
  return String(value);
}

function decimal(value: unknown, digits = 4): string {
  const parsed = asNumber(value);
  return parsed === null ? "-" : parsed.toFixed(digits);
}

function percent(value: unknown, digits = 2): string {
  const parsed = asNumber(value);
  return parsed === null ? "-" : `${(parsed * 100).toFixed(digits)}%`;
}

function integer(value: unknown): string {
  const parsed = asNumber(value);
  return parsed === null ? "-" : Math.round(parsed).toLocaleString("zh-CN");
}

function statusValue(value: unknown): string {
  if (typeof value === "string") return value;
  const record = asObject(value);
  return text(record.status ?? record.state ?? record.reason_code);
}

function compactValue(value: unknown): string {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value !== "object") return text(value);
  if (Array.isArray(value)) {
    const scalars = value.filter((item) => typeof item !== "object").slice(0, 6).map((item) => text(item));
    return scalars.length > 0 ? scalars.join("、") : `${value.length} 项`;
  }
  const record = asObject(value);
  const direct = record.status ?? record.state ?? record.reason_code ?? record.message ?? record.action;
  if (direct !== undefined && direct !== null && direct !== "") return text(direct);
  const pairs = Object.entries(record)
    .filter(([, item]) => typeof item !== "object" && item !== null && item !== "")
    .slice(0, 6)
    .map(([key, item]) => `${key}=${text(item)}`);
  return pairs.length > 0 ? pairs.join(" · ") : `${Object.keys(record).length} 项`;
}

function statusCounts(value: unknown): string {
  const entries = Object.entries(asObject(value));
  return entries.length > 0 ? entries.map(([key, count]) => `${key} ${integer(count)}`).join(" · ") : "-";
}

function dataActionLines(value: unknown): string[] {
  if (value === null || value === undefined || value === "") return [];
  if (Array.isArray(value)) return value.flatMap((item) => dataActionLines(item)).slice(0, 20);
  if (typeof value !== "object") return [text(value)];
  const record = asObject(value);
  const action = record.action ?? record.message ?? record.reason_code;
  if (action) return [compactValue(record)];
  return Object.entries(record).flatMap(([key, item]) => {
    if (item === null || item === undefined || item === "") return [];
    if (typeof item === "object") return dataActionLines(item).map((line) => `${key}: ${line}`);
    return [`${key}: ${text(item)}`];
  }).slice(0, 20);
}

function evidenceLines(value: unknown): string[] {
  return dataActionLines(value).filter((line) => line !== "-").slice(0, 12);
}

function FamilyEvidenceCard({ family, value }: { family: string; value: unknown }) {
  const record = asObject(value);
  const reasons = evidenceLines(record.reason_codes ?? record.reason_code ?? record.message);
  const coverage = evidenceLines(record.coverage);
  const limitations = evidenceLines(record.limitations);
  const missingInputs = evidenceLines(record.missing_inputs);
  const actions = evidenceLines(record.data_actions);
  const sections = [
    ["reason", reasons],
    ["coverage", coverage],
    ["limitations", limitations],
    ["missing inputs", missingInputs],
    ["data actions", actions],
  ] as const;
  return (
    <div data-testid={`qe-long-trend-family-${family}`} style={{ borderBottom: "1px solid #e2e8f0", padding: "8px 0" }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8, fontSize: 12 }}>
        <span style={{ fontFamily: "monospace" }}>{family}</span>
        <strong>{statusValue(value)}</strong>
      </div>
      {sections.map(([label, lines]) => lines.length > 0 && (
        <div key={label} style={{ marginTop: 4, fontSize: 11, color: "#64748b" }}>
          <strong>{label}：</strong>{lines.join("；")}
        </div>
      ))}
    </div>
  );
}

function errorMessage(error: unknown): string {
  if (error instanceof QEArchiveApiError) return error.message;
  if (error instanceof Error) return error.message;
  return text(error, "未知错误");
}

function dimension(metric: LongTrendMetric): JsonObject {
  return asObject(metric.dimension_json);
}

function metricJson(metric?: LongTrendMetric): JsonObject {
  return asObject(metric?.value_json);
}

function findMetric(
  metrics: LongTrendMetric[],
  metricKey: string,
  payload: { horizon?: number; barrier?: number; k?: number; slice?: string } = {},
): LongTrendMetric | undefined {
  const candidates = metrics.filter((metric) => {
    if (metric.metric_key !== metricKey) return false;
    const dim = dimension(metric);
    if (payload.horizon !== undefined && metric.horizon !== payload.horizon) return false;
    if (payload.barrier !== undefined && asNumber(dim.barrier) !== payload.barrier) return false;
    if (payload.k !== undefined && asNumber(dim.k) !== payload.k) return false;
    if (payload.slice !== undefined && text(dim.slice, "") !== payload.slice) return false;
    return true;
  });
  return candidates[0];
}

const panelStyle: React.CSSProperties = {
  background: "#fff",
  border: "1px solid #dbeafe",
  borderRadius: 10,
  padding: 18,
  boxShadow: "0 1px 3px rgba(15, 23, 42, 0.05)",
};

const badgeStyle: React.CSSProperties = {
  display: "inline-flex",
  alignItems: "center",
  padding: "3px 8px",
  borderRadius: 999,
  background: "#eff6ff",
  color: "#1d4ed8",
  border: "1px solid #bfdbfe",
  fontSize: 11,
  fontWeight: 700,
};

export default function LongTrendEvaluationPanel({ taskId, loopIndex, loopStatus }: Props) {
  const [evaluations, setEvaluations] = React.useState<LongTrendEvaluation[]>([]);
  const [detail, setDetail] = React.useState<LongTrendEvaluationDetail | null>(null);
  const [selectedId, setSelectedId] = React.useState("");
  const [outcomeSnapshotId, setOutcomeSnapshotId] = React.useState("");
  const [inputPreview, setInputPreview] = React.useState<LongTrendInputPreview | null>(null);
  const [sectorHorizon, setSectorHorizon] = React.useState<(typeof HORIZONS)[number]>(60);
  const [loading, setLoading] = React.useState(false);
  const [submitting, setSubmitting] = React.useState(false);
  const [previewing, setPreviewing] = React.useState(false);
  const [message, setMessage] = React.useState<{ ok: boolean; text: string } | null>(null);
  const requestSeq = React.useRef(0);

  const refresh = React.useCallback(async (preferredEvaluationId?: string, preserveMessage = false) => {
    const requestId = ++requestSeq.current;
    setLoading(true);
    if (!preserveMessage) setMessage(null);
    try {
      const rows = await qeArchiveApi.listLongTrendEvaluations(taskId, loopIndex);
      if (requestId !== requestSeq.current) return;
      setEvaluations(rows);
      const existingSelected = preferredEvaluationId || selectedId;
      const nextId = rows.some((row) => row.evaluation_id === existingSelected)
        ? existingSelected
        : rows[0]?.evaluation_id || "";
      setSelectedId(nextId);
      const latestSnapshot = rows.find((row) => row.evaluation_id === nextId)?.outcome_dataset_snapshot_id
        || rows[0]?.outcome_dataset_snapshot_id;
      if (latestSnapshot) setOutcomeSnapshotId(latestSnapshot);
      if (!nextId) {
        setDetail(null);
        return;
      }
      const loaded = await qeArchiveApi.allLongTrendEvaluationDetail(nextId);
      if (requestId === requestSeq.current) setDetail(loaded);
    } catch (error) {
      if (requestId === requestSeq.current) {
        setDetail(null);
        setMessage({ ok: false, text: errorMessage(error) });
      }
    } finally {
      if (requestId === requestSeq.current) setLoading(false);
    }
  }, [loopIndex, selectedId, taskId]);

  React.useEffect(() => {
    setEvaluations([]);
    setDetail(null);
    setSelectedId("");
    setOutcomeSnapshotId("");
    setInputPreview(null);
    void refresh();
    return () => { requestSeq.current += 1; };
    // selectedId must not retrigger DB recovery; the selector calls refresh explicitly.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [taskId, loopIndex]);

  const createOrUpdate = React.useCallback(async () => {
    const snapshot = outcomeSnapshotId.trim();
    if (!snapshot) {
      setMessage({ ok: false, text: "请输入已注册的 outcome dataset snapshot id" });
      return;
    }
    setSubmitting(true);
    setMessage(null);
    try {
      const result = await qeArchiveApi.createOrUpdateLongTrendEvaluation({
        task_id: taskId,
        loop_index: loopIndex,
        profile_id: "qe_long_trend_v1",
        outcome_dataset_snapshot_id: snapshot,
      });
      setMessage({
        ok: true,
        text: `评价请求已返回 ${result.evaluation_id}；状态 ${text(result.status)}，节点提交 ${result.ready_for_node === false ? "未发生/无需" : "按后端权威状态处理"}`,
      });
      await refresh(result.evaluation_id, true);
    } catch (error) {
      setMessage({ ok: false, text: errorMessage(error) });
    } finally {
      setSubmitting(false);
    }
  }, [loopIndex, outcomeSnapshotId, refresh, taskId]);

  const previewInputs = React.useCallback(async () => {
    const snapshot = outcomeSnapshotId.trim();
    if (!snapshot) {
      setMessage({ ok: false, text: "请输入已注册的 outcome dataset snapshot id" });
      return;
    }
    setPreviewing(true);
    setMessage(null);
    try {
      const result = await qeArchiveApi.longTrendInputPreview({
        task_id: taskId,
        loop_index: loopIndex,
        profile_id: "qe_long_trend_v1",
        outcome_dataset_snapshot_id: snapshot,
      });
      setInputPreview(result);
    } catch (error) {
      setInputPreview(null);
      setMessage({ ok: false, text: errorMessage(error) });
    } finally {
      setPreviewing(false);
    }
  }, [loopIndex, outcomeSnapshotId, taskId]);

  const evaluation = detail?.evaluation;
  const metrics = detail?.metrics || [];
  const familyStatus = evaluation?.family_status_json || {};
  const platformStatus = asObject(evaluation?.platform_delivery_status_json);
  const episode = metricJson(findMetric(metrics, "episode_capture_summary"));
  const execution = metricJson(findMetric(metrics, "entry_execution_summary"));
  const concentration = metricJson(findMetric(metrics, "top50_sector_concentration"));
  const actionLines = dataActionLines(evaluation?.data_action_plan_json);
  const sectorRows = metrics
    .filter((metric) => metric.metric_key === "sector_signal_path" && metric.horizon === sectorHorizon)
    .filter((metric) => text(dimension(metric).slice, "") === "all_oos" && asNumber(dimension(metric).k) === 50)
    .sort((left, right) => (asNumber(metricJson(right).sample_count) || 0) - (asNumber(metricJson(left).sample_count) || 0))
    .slice(0, 10);
  const canSubmit = loopStatus === "completed" && !submitting && outcomeSnapshotId.trim().length > 0;
  const canPreview = loopStatus === "completed" && !previewing && outcomeSnapshotId.trim().length > 0;

  return (
    <div data-testid="qe-long-trend-panel" style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <div style={panelStyle}>
        <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 16, flexWrap: "wrap" }}>
          <div>
            <h3 style={{ margin: 0, display: "flex", alignItems: "center", gap: 8, color: "#1e3a8a", fontSize: 16 }}>
              <TrendingUp size={18} /> 长期趋势评价
            </h3>
            <p style={{ margin: "6px 0 0", color: "#64748b", fontSize: 12, lineHeight: 1.6 }}>
              固定 profile：qe_long_trend_v1。页面从数据库恢复状态；同 identity 幂等，不提供 force、路径或节点覆盖。
            </p>
          </div>
          <button
            type="button"
            data-testid="qe-long-trend-refresh"
            onClick={() => void refresh()}
            disabled={loading}
            style={{ padding: "7px 11px", borderRadius: 7, border: "1px solid #cbd5e1", background: "#fff", color: "#334155", cursor: loading ? "wait" : "pointer", display: "flex", alignItems: "center", gap: 6 }}
          >
            <RefreshCw size={14} /> {loading ? "恢复中..." : "刷新 DB 状态"}
          </button>
        </div>

        <div style={{ marginTop: 14, display: "grid", gridTemplateColumns: "minmax(260px, 1fr) auto auto", gap: 10 }}>
          <input
            data-testid="qe-long-trend-outcome-snapshot"
            value={outcomeSnapshotId}
            onChange={(event) => {
              setOutcomeSnapshotId(event.target.value);
              setInputPreview(null);
            }}
            placeholder="已注册 outcome_dataset_snapshot_id"
            aria-label="长期趋势 outcome dataset snapshot id"
            style={{ minWidth: 0, padding: "9px 11px", borderRadius: 7, border: "1px solid #cbd5e1", fontFamily: "monospace", fontSize: 12 }}
          />
          <button
            type="button"
            data-testid="qe-long-trend-preview"
            onClick={() => void previewInputs()}
            disabled={!canPreview}
            style={{ padding: "9px 14px", borderRadius: 7, border: "1px solid #2563eb", background: canPreview ? "#eff6ff" : "#f1f5f9", color: canPreview ? "#1d4ed8" : "#94a3b8", fontWeight: 700, cursor: canPreview ? "pointer" : "not-allowed" }}
          >
            {previewing ? "预检中..." : "只读输入预检"}
          </button>
          <button
            type="button"
            data-testid="qe-long-trend-create"
            onClick={() => void createOrUpdate()}
            disabled={!canSubmit}
            style={{ padding: "9px 14px", borderRadius: 7, border: "none", background: canSubmit ? "#2563eb" : "#cbd5e1", color: "#fff", fontWeight: 700, cursor: canSubmit ? "pointer" : "not-allowed" }}
          >
            {submitting ? "提交中..." : "生成/更新长期趋势评价"}
          </button>
        </div>
        {loopStatus !== "completed" && (
          <div style={{ marginTop: 8, fontSize: 12, color: "#92400e" }}>Loop 尚未完成；只恢复已有评价，创建操作保持禁用。</div>
        )}
        {message && (
          <div data-testid="qe-long-trend-message" style={{ marginTop: 10, padding: "9px 11px", borderRadius: 7, border: `1px solid ${message.ok ? "#86efac" : "#fecaca"}`, background: message.ok ? "#f0fdf4" : "#fef2f2", color: message.ok ? "#166534" : "#991b1b", fontSize: 12, wordBreak: "break-word" }}>
            {message.text}
          </div>
        )}
        {inputPreview && (
          <div data-testid="qe-long-trend-input-preview" style={{ marginTop: 12, padding: 12, borderRadius: 8, border: "1px solid #bfdbfe", background: "#f8fafc" }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap", fontSize: 12 }}>
              <strong style={{ color: "#1e3a8a" }}>历史输入可用性（只读）</strong>
              <span data-testid="qe-long-trend-preview-readiness" style={badgeStyle}>
                技术提交就绪：{inputPreview.ready_for_node ? "是" : "否"}
              </span>
            </div>
            <div style={{ marginTop: 6, color: "#64748b", fontSize: 11 }}>
              此状态仅说明节点输入是否可解析，不是科研许可或方向门禁；缺失输入不会隐藏“生成/更新”入口。
            </div>
            <div style={{ marginTop: 10, display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 7 }}>
              {[...inputPreview.dataset_inputs, ...inputPreview.artifact_inputs].map((item) => (
                <div key={`${item.category}:${item.input_name}`} data-testid={`qe-long-trend-preview-${item.input_name}`} style={{ padding: "7px 9px", borderRadius: 6, border: `1px solid ${item.available ? "#bbf7d0" : "#fed7aa"}`, background: item.available ? "#f0fdf4" : "#fff7ed", fontSize: 11 }}>
                  <strong>{item.input_name}</strong>：{item.available ? "可用" : "缺失/不可解析"}
                  {!item.available && <div style={{ marginTop: 3, fontFamily: "monospace", wordBreak: "break-all" }}>{text(item.reason_code)}</div>}
                </div>
              ))}
            </div>
            {inputPreview.data_action_plan.length > 0 && (
              <div data-testid="qe-long-trend-preview-actions" style={{ marginTop: 10, color: "#92400e", fontSize: 11 }}>
                {inputPreview.data_action_plan.map((action, index) => (
                  <div key={`${text(action.action)}:${index}`}>{text(action.action)} · {text(action.input_name)} · {text(action.reason_code)}</div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>

      {evaluations.length > 1 && (
        <div style={panelStyle}>
          <label style={{ fontSize: 12, fontWeight: 700, color: "#475569" }}>
            评价版本
            <select
              data-testid="qe-long-trend-evaluation-select"
              value={selectedId}
              onChange={(event) => void refresh(event.target.value)}
              style={{ display: "block", width: "100%", marginTop: 6, padding: 8, borderRadius: 7, border: "1px solid #cbd5e1", fontFamily: "monospace" }}
            >
              {evaluations.map((row) => <option key={row.evaluation_id} value={row.evaluation_id}>{row.evaluation_id} · {text(row.status)} · {text(row.outcome_dataset_snapshot_id)}</option>)}
            </select>
          </label>
        </div>
      )}

      {!loading && !evaluation && !message && (
        <div style={{ ...panelStyle, color: "#64748b", textAlign: "center", padding: 32 }}>
          当前 Loop 尚无长期趋势评价。输入已注册 outcome snapshot 后使用唯一创建入口。
        </div>
      )}

      {evaluation && (
        <>
          <div style={panelStyle}>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
              <span data-testid="qe-long-trend-evaluation-status" style={badgeStyle}>task {text(evaluation.status)}</span>
              <span style={badgeStyle}>profile {text(evaluation.profile_id)}</span>
              <span style={badgeStyle}>metrics {integer(platformStatus.db_metric_count ?? metrics.length)}</span>
              <span style={badgeStyle}>artifacts {integer(platformStatus.db_artifact_count ?? detail?.artifacts.length)}</span>
            </div>
            <div style={{ marginTop: 12, display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 8, fontSize: 12 }}>
              <div><strong>evaluation</strong><div style={{ fontFamily: "monospace", wordBreak: "break-all" }}>{evaluation.evaluation_id}</div></div>
              <div><strong>run</strong><div style={{ fontFamily: "monospace", wordBreak: "break-all" }}>{text(evaluation.run_id)}</div></div>
              <div><strong>feature snapshot</strong><div style={{ fontFamily: "monospace", wordBreak: "break-all" }}>{text(evaluation.feature_dataset_snapshot_id)}</div></div>
              <div><strong>outcome snapshot</strong><div style={{ fontFamily: "monospace", wordBreak: "break-all" }}>{text(evaluation.outcome_dataset_snapshot_id)}</div></div>
              <div><strong>evaluation as-of</strong><div data-testid="qe-long-trend-evaluation-asof">{text(evaluation.evaluation_asof)}</div></div>
              <div><strong>更新时间</strong><div>{text(evaluation.updated_at)}</div></div>
              <div><strong>reason code</strong><div>{text(evaluation.reason_code)}</div></div>
            </div>
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 16 }}>
            <div style={panelStyle}>
              <h4 style={{ margin: "0 0 10px", color: "#334155" }}>指标族状态</h4>
              <div style={{ display: "grid", gap: 7 }}>
                {FAMILIES.map((family) => <FamilyEvidenceCard key={family} family={family} value={familyStatus[family]} />)}
              </div>
            </div>
            <div style={panelStyle}>
              <h4 style={{ margin: "0 0 10px", color: "#334155" }}>平台交付状态</h4>
              <div style={{ display: "grid", gap: 7 }}>
                {Object.entries(platformStatus).map(([key, value]) => <div key={key} style={{ display: "flex", justifyContent: "space-between", gap: 8, fontSize: 12 }}><span style={{ fontFamily: "monospace" }}>{key}</span><strong>{compactValue(value)}</strong></div>)}
              </div>
            </div>
          </div>

          <div data-testid="qe-long-trend-coverage-censoring" style={panelStyle}>
            <h4 style={{ margin: "0 0 10px", color: "#334155" }}>成熟度、执行覆盖与删失说明</h4>
            <div style={{ display: "grid", gap: 6, fontSize: 12 }}>
              {HORIZONS.map((horizon) => {
                const maturity = metricJson(findMetric(metrics, "maturity", { horizon, slice: "all_oos" }));
                return <div key={horizon}><strong>{horizon}D：</strong><span style={{ fontFamily: "monospace" }}>{statusCounts(maturity)}</span></div>;
              })}
              <div><strong>order_fill coverage：</strong>{evidenceLines(asObject(familyStatus.order_fill).coverage).join("；") || "-"}</div>
              <div><strong>execution_cause coverage：</strong>{evidenceLines(asObject(familyStatus.execution_cause).coverage).join("；") || "-"}</div>
            </div>
            <p style={{ margin: "10px 0 0", color: "#64748b", fontSize: 11, lineHeight: 1.6 }}>
              仅 matured 样本进入固定期限收益、RankIC 与 barrier 分母；right_censored、path_incomplete、invalid_entry、instrument_exit_unresolved 等状态保持独立计数，不补 0、不伪装为未命中或亏损。
            </p>
          </div>

          <div style={panelStyle}>
            <h4 style={{ margin: "0 0 10px", color: "#334155" }}>20–180D 路径与成熟度（all_oos / Top50）</h4>
            <div style={{ overflowX: "auto" }}>
              <table data-testid="qe-long-trend-horizon-table" style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                <thead><tr style={{ background: "#eff6ff" }}>{["Horizon", "RankIC", "平均收益", "Path MFE", "Path MAE", "成熟", "右删失"].map((label) => <th key={label} style={{ padding: 8, textAlign: label === "Horizon" ? "left" : "right" }}>{label}</th>)}</tr></thead>
                <tbody>{HORIZONS.map((horizon) => {
                  const rank = findMetric(metrics, "rank_ic", { horizon, slice: "all_oos" });
                  const returns = metricJson(findMetric(metrics, "topk_return_distribution", { horizon, slice: "all_oos", k: 50 }));
                  const maturity = metricJson(findMetric(metrics, "maturity", { horizon, slice: "all_oos" }));
                  return <tr key={horizon} style={{ borderBottom: "1px solid #e2e8f0" }}>
                    <td style={{ padding: 8, fontWeight: 800 }}>{horizon}D</td>
                    <td style={{ padding: 8, textAlign: "right", fontFamily: "monospace" }}>{decimal(rank?.value_num)}</td>
                    <td style={{ padding: 8, textAlign: "right", fontFamily: "monospace" }}>{percent(asObject(returns.return).mean)}</td>
                    <td style={{ padding: 8, textAlign: "right", fontFamily: "monospace" }}>{percent(asObject(returns.path_mfe).mean)}</td>
                    <td style={{ padding: 8, textAlign: "right", fontFamily: "monospace" }}>{percent(asObject(returns.path_mae).mean)}</td>
                    <td style={{ padding: 8, textAlign: "right", fontFamily: "monospace" }}>{integer(maturity.matured)}</td>
                    <td style={{ padding: 8, textAlign: "right", fontFamily: "monospace" }}>{integer(maturity.right_censored)}</td>
                  </tr>;
                })}</tbody>
              </table>
            </div>
          </div>

          <div style={panelStyle}>
            <h4 style={{ margin: "0 0 10px", color: "#334155" }}>30% / 50% / 70% Barrier（all_oos / Top50）</h4>
            <div style={{ overflowX: "auto" }}><table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
              <thead><tr style={{ background: "#f5f3ff" }}>{["Horizon", "Barrier", "Precision", "Recall", "AUCPR", "命中中位天数"].map((label) => <th key={label} style={{ padding: 8, textAlign: label === "Horizon" || label === "Barrier" ? "left" : "right" }}>{label}</th>)}</tr></thead>
              <tbody>{HORIZONS.flatMap((horizon) => BARRIERS.map((barrier) => {
                const value = metricJson(findMetric(metrics, "barrier_capture", { horizon, barrier, slice: "all_oos", k: 50 }));
                return <tr key={`${horizon}-${barrier}`} style={{ borderBottom: "1px solid #e2e8f0" }}><td style={{ padding: 8 }}>{horizon}D</td><td style={{ padding: 8 }}>{Math.round(barrier * 100)}%</td><td style={{ padding: 8, textAlign: "right" }}>{percent(value.precision_at_k)}</td><td style={{ padding: 8, textAlign: "right" }}>{percent(value.recall_at_k)}</td><td style={{ padding: 8, textAlign: "right" }}>{decimal(value.aucpr)}</td><td style={{ padding: 8, textAlign: "right" }}>{decimal(asObject(value.time_to_hit).p50, 1)}</td></tr>;
              }))}</tbody>
            </table></div>
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 16 }}>
            <div style={panelStyle}>
              <h4 style={{ margin: "0 0 10px", color: "#334155" }}>Episode Capture</h4>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 8, fontSize: 12 }}>
                <div>episode 数<strong style={{ display: "block" }}>{integer(episode.episode_count)}</strong></div>
                <div>capture 中位数<strong style={{ display: "block" }}>{percent(asObject(episode.episode_capture_ratio).p50)}</strong></div>
                <div>extended capture 中位数<strong style={{ display: "block" }}>{percent(asObject(episode.extended_capture_ratio).p50)}</strong></div>
                <div>false early-exit<strong style={{ display: "block" }}>{percent(episode.false_early_exit_ratio)}</strong></div>
              </div>
            </div>
            <div style={panelStyle}>
              <h4 style={{ margin: "0 0 10px", color: "#334155" }}>入场 / 退出证据</h4>
              <div style={{ fontSize: 12, lineHeight: 1.8 }}>
                <div>入场状态：<span style={{ fontFamily: "monospace" }}>{statusCounts(execution.entry_status_counts)}</span></div>
                <div>退出状态：<span style={{ fontFamily: "monospace" }}>{statusCounts(execution.exit_status_counts)}</span></div>
                <div>入场证据质量：<span style={{ fontFamily: "monospace" }}>{statusCounts(execution.entry_evidence_level_counts)}</span></div>
                <div>退出证据质量：<span style={{ fontFamily: "monospace" }}>{statusCounts(execution.exit_evidence_level_counts)}</span></div>
                <div>入场阻断原因：<span style={{ fontFamily: "monospace" }}>{statusCounts(execution.entry_block_reason_counts)}</span></div>
                <div>退出阻断原因：<span style={{ fontFamily: "monospace" }}>{statusCounts(execution.exit_block_reason_counts)}</span></div>
                <div>entry delay p50：{decimal(asObject(execution.entry_delay_days).p50, 1)} 天</div>
                <div>exit delay p50：{decimal(asObject(execution.exit_delay_days).p50, 1)} 天</div>
                <div>入场阻断损失：{percent(asObject(execution.missed_mfe_due_to_entry_block).mean)}</div>
                <div>退出额外回撤：{percent(asObject(execution.blocked_exit_extra_drawdown).mean)}</div>
                <div>退出额外持仓 p50：{decimal(asObject(execution.blocked_exit_extra_holding_days).p50, 1)} 天</div>
              </div>
            </div>
          </div>

          <div style={panelStyle}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 12 }}>
              <h4 style={{ margin: 0, color: "#334155" }}>板块集中与 L2 Top10</h4>
              <select value={sectorHorizon} onChange={(event) => setSectorHorizon(Number(event.target.value) as (typeof HORIZONS)[number])} style={{ padding: 6, borderRadius: 6, border: "1px solid #cbd5e1" }}>{HORIZONS.map((horizon) => <option key={horizon} value={horizon}>{horizon}D</option>)}</select>
            </div>
            <div style={{ marginTop: 10, display: "flex", gap: 14, flexWrap: "wrap", fontSize: 12 }}><span>HHI {decimal(concentration.daily_hhi_mean)}</span><span>Top1 share {percent(concentration.daily_top1_sector_share_mean)}</span><span>mapped {percent(concentration.mapped_rate)}</span></div>
            <div style={{ marginTop: 10, overflowX: "auto" }}><table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}><thead><tr style={{ background: "#f0fdf4" }}><th style={{ padding: 7, textAlign: "left" }}>L2 code</th><th style={{ padding: 7, textAlign: "right" }}>样本</th><th style={{ padding: 7, textAlign: "right" }}>平均收益</th><th style={{ padding: 7, textAlign: "right" }}>MFE</th><th style={{ padding: 7, textAlign: "right" }}>MAE</th></tr></thead><tbody>{sectorRows.map((metric) => { const value = metricJson(metric); return <tr key={metric.dimension_key} style={{ borderBottom: "1px solid #e2e8f0" }}><td style={{ padding: 7, fontFamily: "monospace" }}>{text(metric.sector_code)}</td><td style={{ padding: 7, textAlign: "right" }}>{integer(value.sample_count)}</td><td style={{ padding: 7, textAlign: "right" }}>{percent(asObject(value.return).mean)}</td><td style={{ padding: 7, textAlign: "right" }}>{percent(asObject(value.path_mfe).mean)}</td><td style={{ padding: 7, textAlign: "right" }}>{percent(asObject(value.path_mae).mean)}</td></tr>; })}</tbody></table></div>
          </div>

          <div style={panelStyle}>
            <h4 style={{ margin: "0 0 10px", color: "#334155" }}>制品与数据行动</h4>
            <div style={{ display: "grid", gap: 6, fontSize: 12 }}>{detail?.artifacts.map((artifact) => <div key={artifact.artifact_type} style={{ display: "grid", gridTemplateColumns: "180px 1fr auto", gap: 8, borderBottom: "1px solid #e2e8f0", padding: "6px 0" }}><strong>{artifact.artifact_type}</strong><span style={{ fontFamily: "monospace", wordBreak: "break-all" }}>{artifact.artifact_uri}</span><span>{integer(artifact.row_count)}</span></div>)}</div>
            {actionLines.length > 0 && <div style={{ marginTop: 12, padding: 10, background: "#fffbeb", border: "1px solid #fde68a", borderRadius: 7, fontSize: 11 }}><strong>数据行动计划</strong><ul style={{ margin: "6px 0 0", paddingLeft: 18 }}>{actionLines.map((line, index) => <li key={`${index}-${line}`} style={{ marginBottom: 3, wordBreak: "break-word" }}>{line}</li>)}</ul></div>}
          </div>
        </>
      )}

      {loading && <div style={{ ...panelStyle, textAlign: "center", color: "#64748b" }}>正在按有界分页从数据库恢复评价状态与指标…</div>}
      {evaluation?.reason_code && <div style={{ ...panelStyle, display: "flex", gap: 8, color: "#92400e", background: "#fffbeb" }}><AlertTriangle size={17} /><span>{evaluation.reason_code}</span></div>}
    </div>
  );
}
