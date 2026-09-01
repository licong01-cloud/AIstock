"use client";

import React from "react";
import { BarChart3, Search } from "lucide-react";
import {
  QEArchiveApiError,
  qeArchiveApi,
  type JsonObject,
  type LongTrendOperatorOptions,
  type LongTrendQualityItem,
  type LongTrendSectorOption,
  type LongTrendSnapshotOption,
  type LongTrendTaskOption,
} from "@/lib/qe-archive/api";
import BusinessSearchSelect, { type BusinessSearchOption } from "./BusinessSearchSelect";

const HORIZONS = [20, 40, 60, 120, 180] as const;
const METRICS = [
  ["rank_ic", "RankIC"],
  ["topk_return_distribution", "Top50 收益/MFE/MAE"],
  ["barrier_capture", "Barrier 捕获"],
  ["maturity", "成熟度"],
  ["sector_signal_path", "L2 板块路径"],
] as const;
const ENTRY_EXECUTION_STATUSES = [
  "filled_t1",
  "partial_fill_t1",
  "delayed_fill",
  "never_filled",
  "not_attempted_by_strategy",
  "not_verifiable",
] as const;
const EXIT_EXECUTION_STATUSES = [
  "filled_on_exit_signal_day",
  "delayed_exit",
  "never_exited",
  "not_attempted_by_strategy",
  "not_verifiable",
] as const;
const ENTRY_EVIDENCE_LEVELS = [
  "none",
  "ambiguous_trade_match",
  "reconciled_trade",
  "indicator_and_trade_reconciled",
  "qlib_indicator_object",
  "explicit_order_intent",
  "position_transition_only",
] as const;
const EXIT_EVIDENCE_LEVELS = [
  "none",
  "ambiguous_trade_match",
  "exit_signal_only",
  "reconciled_trade",
  "position_transition",
  "qlib_indicator_object",
  "indicator_and_exit_reconciled",
  "explicit_order_intent",
  "position_transition_only",
] as const;

const FAMILY_STATUS_LABELS: Record<string, string> = {
  COMPUTED: "证据完整",
  COMPUTED_WITH_LIMITATIONS: "证据有限",
  NOT_COMPUTABLE: "无法计算",
  NOT_VERIFIABLE: "无法核验",
};
const EXECUTION_STATUS_LABELS: Record<string, string> = {
  filled_t1: "次日完成成交",
  partial_fill_t1: "次日部分成交",
  delayed_fill: "延迟成交",
  never_filled: "未成交",
  filled_on_exit_signal_day: "退出信号日成交",
  delayed_exit: "延迟退出",
  never_exited: "未退出",
  not_attempted_by_strategy: "策略未尝试",
  not_verifiable: "无法核验",
};
const EVIDENCE_LEVEL_LABELS: Record<string, string> = {
  none: "无证据",
  ambiguous_trade_match: "成交匹配不明确",
  reconciled_trade: "成交已核对",
  indicator_and_trade_reconciled: "指标与成交已核对",
  qlib_indicator_object: "Qlib 指标对象",
  explicit_order_intent: "明确委托意图",
  position_transition_only: "仅持仓变化",
  exit_signal_only: "仅退出信号",
  position_transition: "持仓变化已确认",
  indicator_and_exit_reconciled: "指标与退出已核对",
};
const EVALUATION_STATUS_LABELS: Record<string, string> = {
  succeeded: "完成",
  partial: "部分完成",
  failed: "失败",
  cancelled: "已取消",
  published: "已发布",
};
const QUALITY_LABELS: Record<string, string> = {
  ok: "证据完整",
  computed_with_limitations: "证据有限",
  insufficient_maturity: "样本未成熟",
  not_computable: "无法计算",
  not_verifiable: "无法核验",
  censored_only: "仅右删失样本",
};

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

function taskChoice(option: LongTrendTaskOption): BusinessSearchOption {
  const models = (option.model_types || []).join("/") || "模型未知";
  return {
    value: option.value,
    label: option.task_name || "未命名演进任务",
    description: `${text(option.latest_evaluation_asof, "日期未知")}｜${models}｜${option.evaluation_count} 次评价`,
  };
}

function snapshotChoice(option: LongTrendSnapshotOption): BusinessSearchOption {
  const taskNames = (option.task_names || []).slice(0, 2).join("、") || "任务未知";
  return {
    value: option.value,
    label: `${text(option.latest_evaluation_asof, "日期未知")} 截止的结果快照`,
    description: `${taskNames}｜${option.evaluation_count} 次评价`,
  };
}

function sectorChoice(option: LongTrendSectorOption): BusinessSearchOption {
  return {
    value: option.value,
    label: option.sector_name || "未命名二级行业",
    description: `${text(option.latest_evaluation_asof, "日期未知")}｜${option.evaluation_count} 次评价`,
  };
}

export default function LongTrendComparisonPanel() {
  const [taskId, setTaskId] = React.useState("");
  const [snapshotId, setSnapshotId] = React.useState("");
  const [taskSearch, setTaskSearch] = React.useState("");
  const [snapshotSearch, setSnapshotSearch] = React.useState("");
  const [sectorSearch, setSectorSearch] = React.useState("");
  const [taskOptions, setTaskOptions] = React.useState<LongTrendTaskOption[]>([]);
  const [snapshotOptions, setSnapshotOptions] = React.useState<LongTrendSnapshotOption[]>([]);
  const [sectorOptions, setSectorOptions] = React.useState<LongTrendSectorOption[]>([]);
  const [optionsBusyCount, setOptionsBusyCount] = React.useState(0);
  const [horizon, setHorizon] = React.useState<(typeof HORIZONS)[number]>(60);
  const [metricKey, setMetricKey] = React.useState<(typeof METRICS)[number][0]>("rank_ic");
  const [barrier, setBarrier] = React.useState(0.5);
  const [sectorCode, setSectorCode] = React.useState("");
  const [familyStatus, setFamilyStatus] = React.useState("");
  const [entryExecutionStatus, setEntryExecutionStatus] = React.useState<"" | (typeof ENTRY_EXECUTION_STATUSES)[number]>("");
  const [exitExecutionStatus, setExitExecutionStatus] = React.useState<"" | (typeof EXIT_EXECUTION_STATUSES)[number]>("");
  const [entryEvidenceLevel, setEntryEvidenceLevel] = React.useState<"" | (typeof ENTRY_EVIDENCE_LEVELS)[number]>("");
  const [exitEvidenceLevel, setExitEvidenceLevel] = React.useState<"" | (typeof EXIT_EVIDENCE_LEVELS)[number]>("");
  const [rows, setRows] = React.useState<LongTrendQualityItem[]>([]);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState("");
  const [queried, setQueried] = React.useState(false);

  React.useEffect(() => {
    setRows([]);
    setQueried(false);
  }, [barrier, entryEvidenceLevel, entryExecutionStatus, exitEvidenceLevel, exitExecutionStatus, familyStatus, horizon, metricKey, sectorCode, snapshotId, taskId]);

  const loadOptions = React.useCallback(async (payload: Parameters<typeof qeArchiveApi.longTrendOperatorOptions>[0]) => {
    setOptionsBusyCount((count) => count + 1);
    try {
      return await qeArchiveApi.longTrendOperatorOptions({ ...payload, limit: 30 });
    } finally {
      setOptionsBusyCount((count) => Math.max(0, count - 1));
    }
  }, []);
  const optionsLoading = optionsBusyCount > 0;

  React.useEffect(() => {
    let active = true;
    const timer = window.setTimeout(() => {
      void loadOptions({ search: taskSearch || undefined }).then((options: LongTrendOperatorOptions) => {
        if (!active) return;
        setTaskOptions(options.tasks || []);
        if (!taskId && options.tasks?.length === 1) setTaskId(options.tasks[0].value);
      }).catch((optionError) => active && setError(errorMessage(optionError)));
    }, 250);
    return () => { active = false; window.clearTimeout(timer); };
  }, [loadOptions, taskId, taskSearch]);

  React.useEffect(() => {
    let active = true;
    const timer = window.setTimeout(() => {
      void loadOptions({ search: snapshotSearch || undefined, task_id: taskId || undefined }).then((options) => {
        if (!active) return;
        setSnapshotOptions(options.snapshots || []);
        if (!snapshotId && options.snapshots?.length === 1) setSnapshotId(options.snapshots[0].value);
      }).catch((optionError) => active && setError(errorMessage(optionError)));
    }, 250);
    return () => { active = false; window.clearTimeout(timer); };
  }, [loadOptions, snapshotId, snapshotSearch, taskId]);

  React.useEffect(() => {
    let active = true;
    const timer = window.setTimeout(() => {
      void loadOptions({ search: sectorSearch || undefined, task_id: taskId || undefined, outcome_dataset_snapshot_id: snapshotId || undefined }).then((options) => {
        if (!active) return;
        setSectorOptions(options.sectors || []);
        if (!sectorCode && options.sectors?.length === 1) setSectorCode(options.sectors[0].value);
      }).catch((optionError) => active && setError(errorMessage(optionError)));
    }, 250);
    return () => { active = false; window.clearTimeout(timer); };
  }, [loadOptions, sectorCode, sectorSearch, snapshotId, taskId]);

  const runQuery = React.useCallback(async () => {
    const snapshot = snapshotId.trim();
    if (!snapshot) {
      setError("必须指定同一 outcome snapshot；页面不会混排不同 vintage。");
      return;
    }
    if (metricKey === "sector_signal_path" && !sectorCode.trim()) {
      setError("板块路径比较必须先从二级行业候选中选择一个板块，避免无界展开。");
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
        horizon,
        sector_code: metricKey === "sector_signal_path" ? sectorCode.trim() : undefined,
        family_status: familyStatus as "COMPUTED" | "COMPUTED_WITH_LIMITATIONS" | "NOT_COMPUTABLE" | "NOT_VERIFIABLE" || undefined,
        entry_execution_status: entryExecutionStatus || undefined,
        exit_execution_status: exitExecutionStatus || undefined,
        entry_execution_evidence_level: entryEvidenceLevel || undefined,
        exit_execution_evidence_level: exitEvidenceLevel || undefined,
      });
      setRows(selectComparisonRows(items, metricKey, barrier, horizon));
    } catch (queryError) {
      setRows([]);
      setError(errorMessage(queryError));
    } finally {
      setLoading(false);
    }
  }, [barrier, entryEvidenceLevel, entryExecutionStatus, exitEvidenceLevel, exitExecutionStatus, familyStatus, horizon, metricKey, sectorCode, snapshotId, taskId]);

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
        <BusinessSearchSelect testId="qe-long-trend-archive-task" label="演进任务" value={taskId} options={taskOptions.map(taskChoice)} search={taskSearch} onSearchChange={(value) => { setTaskSearch(value); setTaskId(""); setSnapshotId(""); setSectorCode(""); }} onValueChange={(value) => { setTaskId(value); setSnapshotId(""); setSectorCode(""); }} searchPlaceholder="按任务名称、模型或评价日期搜索" emptyLabel="全部演进任务" loading={optionsLoading} />
        <BusinessSearchSelect testId="qe-long-trend-archive-snapshot" label="结果快照" value={snapshotId} options={snapshotOptions.map(snapshotChoice)} search={snapshotSearch} onSearchChange={(value) => { setSnapshotSearch(value); setSnapshotId(""); setSectorCode(""); }} onValueChange={(value) => { setSnapshotId(value); setSectorCode(""); }} searchPlaceholder="按截止日期或任务名称搜索" emptyLabel="请选择同一结果快照" loading={optionsLoading} required />
        <label style={{ fontSize: 11, color: "#475569", fontWeight: 700 }}>指标<select data-testid="qe-long-trend-archive-metric" value={metricKey} onChange={(event) => setMetricKey(event.target.value as (typeof METRICS)[number][0])} style={fieldStyle}>{METRICS.map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
        <label style={{ fontSize: 11, color: "#475569", fontWeight: 700 }}>评价期限<select data-testid="qe-long-trend-archive-horizon" value={horizon} onChange={(event) => setHorizon(Number(event.target.value) as (typeof HORIZONS)[number])} style={fieldStyle}>{HORIZONS.map((value) => <option key={value} value={value}>{value}D</option>)}</select></label>
        <label style={{ fontSize: 11, color: "#475569", fontWeight: 700 }}>指标族证据质量<select value={familyStatus} onChange={(event) => setFamilyStatus(event.target.value)} style={fieldStyle}><option value="">全部</option>{Object.entries(FAMILY_STATUS_LABELS).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
        <label style={{ fontSize: 11, color: "#475569", fontWeight: 700 }}>入场执行状态<select data-testid="qe-long-trend-archive-entry-status" value={entryExecutionStatus} onChange={(event) => setEntryExecutionStatus(event.target.value as "" | (typeof ENTRY_EXECUTION_STATUSES)[number])} style={fieldStyle}><option value="">全部</option>{ENTRY_EXECUTION_STATUSES.map((value) => <option key={value} value={value}>{EXECUTION_STATUS_LABELS[value]}</option>)}</select></label>
        <label style={{ fontSize: 11, color: "#475569", fontWeight: 700 }}>退出执行状态<select data-testid="qe-long-trend-archive-exit-status" value={exitExecutionStatus} onChange={(event) => setExitExecutionStatus(event.target.value as "" | (typeof EXIT_EXECUTION_STATUSES)[number])} style={fieldStyle}><option value="">全部</option>{EXIT_EXECUTION_STATUSES.map((value) => <option key={value} value={value}>{EXECUTION_STATUS_LABELS[value]}</option>)}</select></label>
        <label style={{ fontSize: 11, color: "#475569", fontWeight: 700 }}>入场证据质量<select data-testid="qe-long-trend-archive-entry-evidence" value={entryEvidenceLevel} onChange={(event) => setEntryEvidenceLevel(event.target.value as "" | (typeof ENTRY_EVIDENCE_LEVELS)[number])} style={fieldStyle}><option value="">全部</option>{ENTRY_EVIDENCE_LEVELS.map((value) => <option key={value} value={value}>{EVIDENCE_LEVEL_LABELS[value]}</option>)}</select></label>
        <label style={{ fontSize: 11, color: "#475569", fontWeight: 700 }}>退出证据质量<select data-testid="qe-long-trend-archive-exit-evidence" value={exitEvidenceLevel} onChange={(event) => setExitEvidenceLevel(event.target.value as "" | (typeof EXIT_EVIDENCE_LEVELS)[number])} style={fieldStyle}><option value="">全部</option>{EXIT_EVIDENCE_LEVELS.map((value) => <option key={value} value={value}>{EVIDENCE_LEVEL_LABELS[value]}</option>)}</select></label>
        {metricKey === "barrier_capture" ? <label style={{ fontSize: 11, color: "#475569", fontWeight: 700 }}>Barrier<select value={barrier} onChange={(event) => setBarrier(Number(event.target.value))} style={fieldStyle}><option value={0.3}>30%</option><option value={0.5}>50%</option><option value={0.7}>70%</option></select></label> : metricKey === "sector_signal_path" ? <BusinessSearchSelect testId="qe-long-trend-archive-sector" label="二级行业" value={sectorCode} options={sectorOptions.map(sectorChoice)} search={sectorSearch} onSearchChange={(value) => { setSectorSearch(value); setSectorCode(""); }} onValueChange={setSectorCode} searchPlaceholder="按行业名称或评价日期搜索" emptyLabel="请选择二级行业" loading={optionsLoading} required /> : <div style={{ fontSize: 11, color: "#64748b", alignSelf: "end", padding: 8 }}>二级行业筛选仅在板块路径指标启用。</div>}
      </div>

      {error && <div data-testid="qe-long-trend-archive-error" style={{ marginTop: 12, padding: 10, borderRadius: 7, border: "1px solid #fecaca", background: "#fef2f2", color: "#991b1b", fontSize: 12 }}>{error}</div>}

      {queried && !loading && !error && (
        <div style={{ marginTop: 14, overflowX: "auto" }}>
          <table data-testid="qe-long-trend-archive-table" style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead><tr style={{ background: "#eff6ff" }}>{["模型", "随机种子", "因子数量", "训练标签", "评价截止日", "评价状态", "指标值", "证据质量"].map((label) => <th key={label} style={{ padding: 8, textAlign: "left", whiteSpace: "nowrap" }}>{label}</th>)}</tr></thead>
            <tbody>{rows.map((row) => <tr key={row.evaluation_id} style={{ borderBottom: "1px solid #e2e8f0" }}>
              <td style={{ padding: 8 }}>{text(row.model_type)}</td>
              <td style={{ padding: 8 }}>{text(row.random_seed)}</td>
              <td style={{ padding: 8 }}>{text(row.factor_count)}</td>
              <td style={{ padding: 8 }}>{text(row.label_horizon)}D</td>
              <td style={{ padding: 8 }}>{text(row.evaluation_asof)}</td>
              <td style={{ padding: 8 }}>{EVALUATION_STATUS_LABELS[text(row.evaluation_status, "")] || "状态未知"}</td>
              <td style={{ padding: 8, whiteSpace: "nowrap" }}>{metricDisplay(row)}</td>
              <td style={{ padding: 8 }}>{QUALITY_LABELS[text(row.quality_flag, "")] || "证据状态未知"}</td>
            </tr>)}</tbody>
          </table>
          {rows.length === 0 && <div style={{ padding: 24, textAlign: "center", color: "#64748b" }}>当前结果快照与筛选条件下没有可比较的指标。</div>}
        </div>
      )}
    </section>
  );
}
