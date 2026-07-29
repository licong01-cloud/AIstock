"use client";

import React from "react";
import { BarChart3, Search } from "lucide-react";
import {
  QEArchiveApiError,
  qeArchiveApi,
  type JsonObject,
  type LongTrendQualityItem,
} from "@/lib/qe-archive/api";

const HORIZONS = [20, 40, 60, 120, 180] as const;
const METRICS = [
  ["rank_ic", "RankIC"],
  ["topk_return_distribution", "Top50 收益/MFE/MAE"],
  ["barrier_capture", "Barrier 捕获"],
  ["maturity", "成熟度"],
  ["sector_signal_path", "L2 板块路径"],
] as const;

function asObject(value: unknown): JsonObject {
  return value && typeof value === "object" && !Array.isArray(value) ? value as JsonObject : {};
}

function number(value: unknown): number | null {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function text(value: unknown, fallback = "-"): string {
  return value === null || value === undefined || value === "" ? fallback : String(value);
}

function decimal(value: unknown, digits = 4): string {
  const parsed = number(value);
  return parsed === null ? "-" : parsed.toFixed(digits);
}

function percent(value: unknown, digits = 2): string {
  const parsed = number(value);
  return parsed === null ? "-" : `${(parsed * 100).toFixed(digits)}%`;
}

function shortHash(value: unknown): string {
  const raw = text(value);
  return raw.length > 18 ? `${raw.slice(0, 10)}…${raw.slice(-6)}` : raw;
}

function errorMessage(error: unknown): string {
  if (error instanceof QEArchiveApiError) return error.message;
  if (error instanceof Error) return error.message;
  return text(error, "未知错误");
}

function dimension(row: LongTrendQualityItem): JsonObject {
  return asObject(row.dimension_json);
}

function selectComparisonRows(
  items: LongTrendQualityItem[],
  metricKey: string,
  barrier: number,
  horizon: number,
): LongTrendQualityItem[] {
  const selected = items.filter((row) => {
    if (row.metric_key !== metricKey) return false;
    if (row.horizon !== horizon) return false;
    const dim = dimension(row);
    if (text(dim.slice, "") !== "all_oos") return false;
    if (metricKey === "topk_return_distribution" && number(dim.k) !== 50) return false;
    if (metricKey === "barrier_capture" && (number(dim.k) !== 50 || number(dim.barrier) !== barrier)) return false;
    return true;
  });
  const byEvaluation = new Map<string, LongTrendQualityItem>();
  for (const row of selected) if (!byEvaluation.has(row.evaluation_id)) byEvaluation.set(row.evaluation_id, row);
  return [...byEvaluation.values()].sort((left, right) => {
    const asof = text(right.evaluation_asof, "").localeCompare(text(left.evaluation_asof, ""));
    return asof || left.evaluation_id.localeCompare(right.evaluation_id);
  });
}

function metricDisplay(row: LongTrendQualityItem): string {
  const value = asObject(row.value_json);
  switch (row.metric_key) {
    case "rank_ic": return decimal(row.value_num);
    case "topk_return_distribution":
      return `收益 ${percent(asObject(value.return).mean)} / MFE ${percent(asObject(value.path_mfe).mean)} / MAE ${percent(asObject(value.path_mae).mean)}`;
    case "barrier_capture":
      return `P ${percent(value.precision_at_k)} / R ${percent(value.recall_at_k)} / T50 ${decimal(asObject(value.time_to_hit).p50, 1)}d`;
    case "maturity":
      return `成熟 ${text(value.matured)} / 右删失 ${text(value.right_censored)}`;
    case "sector_signal_path":
      return `收益 ${percent(asObject(value.return).mean)} / 样本 ${text(value.sample_count)}`;
    default: return row.value_text || decimal(row.value_num);
  }
}

const fieldStyle: React.CSSProperties = {
  width: "100%",
  padding: "8px 10px",
  border: "1px solid #cbd5e1",
  borderRadius: 7,
  background: "#fff",
  boxSizing: "border-box",
  fontSize: 12,
};

export default function LongTrendComparisonPanel() {
  const [taskId, setTaskId] = React.useState("");
  const [snapshotId, setSnapshotId] = React.useState("");
  const [horizon, setHorizon] = React.useState<(typeof HORIZONS)[number]>(60);
  const [metricKey, setMetricKey] = React.useState<(typeof METRICS)[number][0]>("rank_ic");
  const [barrier, setBarrier] = React.useState(0.5);
  const [sectorCode, setSectorCode] = React.useState("");
  const [familyStatus, setFamilyStatus] = React.useState("");
  const [rows, setRows] = React.useState<LongTrendQualityItem[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState("");
  const [queried, setQueried] = React.useState(false);

  const runQuery = React.useCallback(async () => {
    const snapshot = snapshotId.trim();
    if (!snapshot) {
      setError("必须指定同一 outcome snapshot；页面不会混排不同 vintage。");
      return;
    }
    if (metricKey === "sector_signal_path" && !sectorCode.trim()) {
      setError("板块路径比较必须指定 L2 sector code，避免无界板块展开。");
      return;
    }
    setLoading(true);
    setError("");
    setQueried(true);
    try {
      const items = await qeArchiveApi.allLongTrendQuality({
        task_id: taskId.trim() || undefined,
        outcome_dataset_snapshot_id: snapshot,
        metric_key: metricKey,
        sector_code: sectorCode.trim() || undefined,
        family_status: familyStatus as "COMPUTED" | "COMPUTED_WITH_LIMITATIONS" | "NOT_COMPUTABLE" | "NOT_VERIFIABLE" || undefined,
      });
      setRows(selectComparisonRows(items, metricKey, barrier, horizon));
    } catch (queryError) {
      setRows([]);
      setError(errorMessage(queryError));
    } finally {
      setLoading(false);
    }
  }, [barrier, familyStatus, horizon, metricKey, sectorCode, snapshotId, taskId]);

  return (
    <section data-testid="qe-long-trend-archive-comparison" style={{ marginTop: 24, background: "#fff", border: "1px solid #dbeafe", borderRadius: 12, padding: 18, boxShadow: "0 1px 3px rgba(15,23,42,0.05)" }}>
      <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 14, flexWrap: "wrap" }}>
        <div>
          <h2 style={{ margin: 0, display: "flex", alignItems: "center", gap: 8, fontSize: 17, color: "#1e3a8a" }}><BarChart3 size={18} />长期趋势同 Vintage 对比</h2>
          <p style={{ margin: "6px 0 0", color: "#64748b", fontSize: 12 }}>只读查询；outcome snapshot 必填，不同 vintage 不会被混排。单页 ≤100，客户端总量上限 5,000，超限显式报错。</p>
        </div>
        <button data-testid="qe-long-trend-archive-query" type="button" onClick={() => void runQuery()} disabled={loading} style={{ padding: "8px 13px", border: "none", borderRadius: 7, background: "#2563eb", color: "#fff", fontWeight: 700, cursor: loading ? "wait" : "pointer", display: "flex", alignItems: "center", gap: 6 }}><Search size={14} />{loading ? "查询中..." : "执行只读对比"}</button>
      </div>

      <div style={{ marginTop: 14, display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 10 }}>
        <label style={{ fontSize: 11, color: "#475569", fontWeight: 700 }}>Task ID（可选）<input data-testid="qe-long-trend-archive-task" value={taskId} onChange={(event) => setTaskId(event.target.value)} style={fieldStyle} /></label>
        <label style={{ fontSize: 11, color: "#475569", fontWeight: 700 }}>Outcome snapshot（必填）<input data-testid="qe-long-trend-archive-snapshot" value={snapshotId} onChange={(event) => setSnapshotId(event.target.value)} style={{ ...fieldStyle, fontFamily: "monospace" }} /></label>
        <label style={{ fontSize: 11, color: "#475569", fontWeight: 700 }}>指标<select data-testid="qe-long-trend-archive-metric" value={metricKey} onChange={(event) => setMetricKey(event.target.value as (typeof METRICS)[number][0])} style={fieldStyle}>{METRICS.map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
        <label style={{ fontSize: 11, color: "#475569", fontWeight: 700 }}>评价期限<select data-testid="qe-long-trend-archive-horizon" value={horizon} onChange={(event) => setHorizon(Number(event.target.value) as (typeof HORIZONS)[number])} style={fieldStyle}>{HORIZONS.map((value) => <option key={value} value={value}>{value}D</option>)}</select></label>
        <label style={{ fontSize: 11, color: "#475569", fontWeight: 700 }}>Family evidence quality<select value={familyStatus} onChange={(event) => setFamilyStatus(event.target.value)} style={fieldStyle}><option value="">全部</option><option value="COMPUTED">COMPUTED</option><option value="COMPUTED_WITH_LIMITATIONS">COMPUTED_WITH_LIMITATIONS</option><option value="NOT_COMPUTABLE">NOT_COMPUTABLE</option><option value="NOT_VERIFIABLE">NOT_VERIFIABLE</option></select></label>
        {metricKey === "barrier_capture" ? <label style={{ fontSize: 11, color: "#475569", fontWeight: 700 }}>Barrier<select value={barrier} onChange={(event) => setBarrier(Number(event.target.value))} style={fieldStyle}><option value={0.3}>30%</option><option value={0.5}>50%</option><option value={0.7}>70%</option></select></label> : <label style={{ fontSize: 11, color: "#475569", fontWeight: 700 }}>L2 sector code{metricKey === "sector_signal_path" ? "（必填）" : "（可选）"}<input value={sectorCode} onChange={(event) => setSectorCode(event.target.value)} style={{ ...fieldStyle, fontFamily: "monospace" }} /></label>}
      </div>

      {error && <div data-testid="qe-long-trend-archive-error" style={{ marginTop: 12, padding: 10, borderRadius: 7, border: "1px solid #fecaca", background: "#fef2f2", color: "#991b1b", fontSize: 12 }}>{error}</div>}

      {queried && !loading && !error && (
        <div style={{ marginTop: 14, overflowX: "auto" }}>
          <table data-testid="qe-long-trend-archive-table" style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead><tr style={{ background: "#eff6ff" }}>{["Run", "Model", "Seed", "Factor set", "训练标签", "Evaluation as-of", "状态", "指标值", "质量"].map((label) => <th key={label} style={{ padding: 8, textAlign: "left", whiteSpace: "nowrap" }}>{label}</th>)}</tr></thead>
            <tbody>{rows.map((row) => <tr key={row.evaluation_id} style={{ borderBottom: "1px solid #e2e8f0" }}>
              <td style={{ padding: 8, fontFamily: "monospace" }} title={text(row.run_id)}>{shortHash(row.run_id)}</td>
              <td style={{ padding: 8 }}>{text(row.model_type)}</td>
              <td style={{ padding: 8, fontFamily: "monospace" }}>{text(row.random_seed)}</td>
              <td style={{ padding: 8, fontFamily: "monospace" }} title={text(row.factor_set_hash)}>{shortHash(row.factor_set_hash)} ({text(row.factor_count)})</td>
              <td style={{ padding: 8 }}>{text(row.label_horizon)}D</td>
              <td style={{ padding: 8 }}>{text(row.evaluation_asof)}</td>
              <td style={{ padding: 8 }}>{text(row.evaluation_status)}</td>
              <td style={{ padding: 8, whiteSpace: "nowrap" }}>{metricDisplay(row)}</td>
              <td style={{ padding: 8 }}>{text(row.quality_flag)}</td>
            </tr>)}</tbody>
          </table>
          {rows.length === 0 && <div style={{ padding: 24, textAlign: "center", color: "#64748b" }}>当前同 vintage/filter 下没有可比较的 canonical metric。</div>}
        </div>
      )}
    </section>
  );
}
