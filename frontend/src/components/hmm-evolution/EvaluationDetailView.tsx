"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ArrowLeft, RefreshCw } from "lucide-react";
import { getEvaluation, HMMApiError } from "@/lib/hmm-evolution/api";
import type { EvaluationDetail } from "@/lib/hmm-research/contracts";
import { TERMINAL_EVALUATION_STATUSES } from "@/lib/hmm-research/contracts";
import EvidencePanel from "@/components/hmm-research/EvidencePanel";
import HMMResearchShell from "@/components/hmm-research/HMMResearchShell";
import StatusBadge from "@/components/hmm-research/StatusBadge";
import VisibleErrorState from "@/components/hmm-research/VisibleErrorState";
import DailyMetricChart from "@/components/hmm-evolution/DailyMetricChart";
import styles from "@/components/hmm-research/hmm-research.module.css";

const POLL_FAST_MS = 3_000;
const POLL_SLOW_MS = 10_000;
const POLL_BACKOFF_AFTER_MS = 60_000;
const POLL_TIMEOUT_MS = 15 * 60_000;

type DailySummary = {
  date: string;
  replacement_count: number;
  daily_net_label: number | null;
  daily_net_db_10d: number | null;
  calculation_status: "no_adjustment" | "computed" | "incomplete_evidence";
  missing_return_evidence_count: number;
};

type IncompleteReturnEvidence = {
  date: string;
  symbol: string;
  replacement_type: "entered_by_hmm" | "dropped_by_hmm";
  evidence_type: "label_return" | "market_return";
  horizon_trading_days: number;
  required_start_date: string;
  required_label_date: string | null;
  reason: string;
};

export default function EvaluationDetailView({ evalId }: { evalId: string }) {
  const [evaluation, setEvaluation] = useState<EvaluationDetail | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(true);
  const [stale, setStale] = useState(false);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<string | null>(null);
  const pollStartedAt = useRef<number | null>(null);

  const load = useCallback(async (background = false) => {
    setError(null);
    if (!background) setLoading(true);
    try {
      setEvaluation(await getEvaluation(evalId));
      setStale(false);
      setLastUpdatedAt(new Date().toISOString());
    } catch (nextError) {
      setError(nextError);
      setStale(background);
    } finally {
      if (!background) setLoading(false);
    }
  }, [evalId]);

  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    if (!evaluation || TERMINAL_EVALUATION_STATUSES.has(evaluation.status)) {
      pollStartedAt.current = null;
      return;
    }
    if (pollStartedAt.current === null) pollStartedAt.current = Date.now();
    const elapsed = Date.now() - pollStartedAt.current;
    if (elapsed >= POLL_TIMEOUT_MS) {
      setStale(true);
      setError(new HMMApiError({
        error_code: "HMM_EVOLUTION_CLIENT_TIMEOUT",
        reason_code: "hmm_evolution_client_polling_timeout",
        message: "评估自动轮询已达到 15 分钟上限，页面保留最后一次数据并停止自动请求。",
        context: { retry_condition: "核对自动评估 worker 服务与 durable state 后手动刷新。" },
      }, 504));
      return;
    }
    const delay = elapsed >= POLL_BACKOFF_AFTER_MS ? POLL_SLOW_MS : POLL_FAST_MS;
    const timer = setTimeout(() => void load(true), delay);
    return () => clearTimeout(timer);
  }, [evaluation, load]);

  const dailySummaryState = useMemo(
    () => validateDailySummary(evaluation?.metrics_json, evaluation?.status),
    [evaluation],
  );
  const incompleteEvidenceState = useMemo(
    () => validateIncompleteReturnEvidence(evaluation?.metrics_json),
    [evaluation],
  );

  return (
    <HMMResearchShell>
      <main className={styles.page}>
        <div className={styles.detailHeader}>
          <div>
            <Link href="/hmm-evolution" className={styles.backLink}><ArrowLeft size={13} /> 返回演进实验室</Link>
            <div className={styles.eyebrow}>Evaluation Evidence</div>
            <h1 className={styles.title}>评估 {evalId}</h1>
            <p className={styles.heroCopy}>结构化展示输入身份、动态标签 horizon、数据水位、结果与失败原因，不直接输出原始 payload。</p>
          </div>
          <div className={styles.panelActions}>{evaluation ? <StatusBadge status={evaluation.status} /> : null}<button type="button" className={styles.button} onClick={() => void load(false)}><RefreshCw size={14} />刷新</button></div>
        </div>
        {loading ? <div className={styles.loadingState}>正在加载评估证据；失败会显式终止。</div> : null}
        {error ? <VisibleErrorState error={error} onRetry={() => void load(false)} /> : null}
        {stale && evaluation ? <div className={`${styles.notice} ${styles.noticeWarning}`}>当前显示最后一次成功数据（{formatDateTime(lastUpdatedAt)}），手动刷新成功前不得视为最新状态。</div> : null}
        {evaluation ? (
          <div className={styles.stack}>
            <section className={styles.metricsGrid}>
              <Metric label={`净标签收益 · ${evaluation.label_horizon_days} 交易日`} value={formatPercent(evaluation.net_label_return)} note={`${evaluation.label_comparable_day_count} 个可比日`} className={styles.metricGreen} />
              <Metric label="Net DB 10D" value={formatPercent(evaluation.net_db_10d)} note={`${evaluation.db_comparable_day_count} 个可比日`} className={styles.metricBlue} />
              <Metric label="正值日比例" value={formatPercent(evaluation.positive_net_label_day_ratio)} note={`改变 TopK ${evaluation.changed_day_count} 日`} className={styles.metricAmber} />
              <Metric label="覆盖率" value={formatPercent(evaluation.primary_coverage_ratio)} note={`证据质量：${evaluation.evidence_quality || "未生成"}`} className={styles.metricSlate} />
            </section>

            {evaluation.evidence_quality === "degraded" ? <div className={`${styles.notice} ${styles.noticeWarning}`}><strong>证据降级</strong><span>{warningSummary(evaluation.warnings_json)}</span></div> : null}
            {evaluation.result_validity === "known_invalid" ? <div className={`${styles.notice} ${styles.noticeWarning}`}><strong>历史结果仅供审计</strong><span>该评估使用旧版整数除法行情收益，Net DB 10D 与推荐分数无效；新任务不会复用此结果。</span></div> : null}
            {["failed", "timed_out", "cancelled"].includes(evaluation.status) ? <VisibleErrorState error={evaluationError(evaluation)} title="评估未成功完成" /> : null}

            <section className={styles.detailGrid}>
              <div className={styles.panel}><div className={styles.panelHeader}><h2 className={styles.panelTitle}>输入身份</h2></div><div className={styles.panelBody}><EvidencePanel sections={[{ title: "Frozen identity", rows: [
                { label: "候选", value: evaluation.candidate_display_name },
                { label: "Candidate ID", value: evaluation.candidate_id },
                { label: "Base loop", value: evaluation.base_loop_ref },
                { label: "评估窗口", value: `${evaluation.window_start} → ${evaluation.window_end}` },
                { label: "Label horizon", value: `${evaluation.label_horizon_days} 个交易日` },
                { label: "TopK", value: String(evaluation.topk) },
              ] }]} /></div></div>
              <div className={styles.panel}><div className={styles.panelHeader}><h2 className={styles.panelTitle}>数据水位</h2></div><div className={styles.panelBody}><EvidencePanel sections={[{ title: "As-of and coverage", rows: [
                { label: "Resolved as-of", value: evaluation.as_of_date },
                { label: "交易日数", value: String(evaluation.trading_days_count) },
                { label: "替换记录", value: String(evaluation.replacement_count) },
                { label: "Universe", value: evaluation.universe_id },
                { label: "Evidence quality", value: evaluation.evidence_quality || "未生成" },
                { label: "Warnings", value: String(evaluation.warnings_json.length) },
              ] }]} /></div></div>
              <div className={styles.panel}><div className={styles.panelHeader}><h2 className={styles.panelTitle}>计算版本</h2></div><div className={styles.panelBody}><EvidencePanel sections={[{ title: "Replay identity", rows: [
                { label: "Evaluator", value: evaluation.evaluator_version },
                { label: "Spec SHA", value: shortHash(evaluation.evaluation_spec_hash) },
                { label: "Source SHA", value: shortHash(evaluation.source_manifest_hash) },
                { label: "Candidate SHA", value: shortHash(evaluation.candidate_manifest_hash) },
                { label: "Input SHA", value: shortHash(evaluation.input_hash) },
                { label: "Result SHA", value: evaluation.result_hash ? shortHash(evaluation.result_hash) : "未生成" },
                { label: "结果有效性", value: evaluation.result_validity === "valid" ? "有效" : "历史无效（只读）" },
              ] }]} /></div></div>
              <div className={styles.panel}><div className={styles.panelHeader}><h2 className={styles.panelTitle}>执行状态</h2></div><div className={styles.panelBody}><EvidencePanel sections={[{ title: "Durable state", rows: [
                { label: "状态", value: evaluation.status },
                { label: "Generation", value: String(evaluation.run_generation) },
                { label: "排队时间", value: formatDateTime(evaluation.queued_at) },
                { label: "开始时间", value: formatDateTime(evaluation.started_at) },
                { label: "最近心跳", value: formatDateTime(evaluation.heartbeat_at) },
                { label: "完成时间", value: formatDateTime(evaluation.completed_at) },
              ] }]} /></div></div>
            </section>

            <section className={styles.panel}>
              <div className={styles.panelHeader}><div><h2 className={styles.panelTitle}>逐日替换摘要</h2><div className={styles.panelSubtitle}>只展示业务字段；replacement samples 与原始 payload 不在主视图直显</div></div></div>
              <div className={styles.panelBody}>
                {dailySummaryState.error ? <VisibleErrorState error={dailySummaryState.error} title="逐日结果契约错误" /> : null}
                {!dailySummaryState.error ? <DailyMetricChart rows={dailySummaryState.rows} horizon={evaluation.label_horizon_days} /> : null}
              </div>
              {!dailySummaryState.error ? <div className={styles.panelBodyTable}><DailySummaryTable rows={dailySummaryState.rows} horizon={evaluation.label_horizon_days} /></div> : null}
            </section>
            {incompleteEvidenceState.error ? <VisibleErrorState error={incompleteEvidenceState.error} title="缺失收益证据契约错误" /> : null}
            {!incompleteEvidenceState.error && incompleteEvidenceState.rows.length > 0 ? (
              <section className={styles.panel}>
                <div className={styles.panelHeader}><div><h2 className={styles.panelTitle}>缺失收益证据</h2><div className={styles.panelSubtitle}>逐只股票列出未计算原因；不会用剩余股票的局部均值冒充完整日收益。</div></div></div>
                <div className={styles.panelBodyTable}><IncompleteEvidenceTable rows={incompleteEvidenceState.rows} /></div>
              </section>
            ) : null}
          </div>
        ) : null}
      </main>
    </HMMResearchShell>
  );
}

function DailySummaryTable({ rows, horizon }: { rows: DailySummary[]; horizon: number }) {
  if (rows.length === 0) return <div className={styles.emptyState}>尚无逐日结果；空集合不会显示为成功收益。</div>;
  return <table className={styles.table}><thead><tr><th>交易日</th><th>替换数量</th><th>状态</th><th>净标签收益 · {horizon}D</th><th>Net DB 10D</th></tr></thead><tbody>{rows.map((row) => <tr key={row.date}><td>{row.date}</td><td>{row.replacement_count}</td><td>{dailyStatusLabel(row)}</td><td>{formatDailyMetric(row, row.daily_net_label)}</td><td>{formatDailyMetric(row, row.daily_net_db_10d)}</td></tr>)}</tbody></table>;
}

function IncompleteEvidenceTable({ rows }: { rows: IncompleteReturnEvidence[] }) {
  return <table className={styles.table}><thead><tr><th>交易日</th><th>股票</th><th>替换方向</th><th>证据</th><th>所需日期</th><th>未计算原因</th></tr></thead><tbody>{rows.map((row) => <tr key={`${row.date}-${row.symbol}-${row.evidence_type}`}><td>{row.date}</td><td>{row.symbol}</td><td>{row.replacement_type === "entered_by_hmm" ? "调入" : "调出"}</td><td>{row.evidence_type === "market_return" ? `行情 ${row.horizon_trading_days}D` : `标签 ${row.horizon_trading_days}D`}</td><td>{row.required_start_date} → {row.required_label_date || "尚未形成"}</td><td>{missingReasonLabel(row.reason)}</td></tr>)}</tbody></table>;
}

function validateDailySummary(
  metrics: Record<string, unknown> | null | undefined,
  status: string | undefined,
): { rows: DailySummary[]; error: HMMApiError | null } {
  const rows = metrics?.daily_summary;
  if (rows === undefined && status !== "succeeded") {
    return { rows: [], error: null };
  }
  if (!Array.isArray(rows)) {
    return { rows: [], error: dailySummaryContractError("metrics_json.daily_summary 必须是数组") };
  }
  const parsed: DailySummary[] = [];
  for (const [index, row] of rows.entries()) {
    if (!row || typeof row !== "object") {
      return { rows: [], error: dailySummaryContractError(`第 ${index + 1} 行不是对象`) };
    }
    const value = row as Record<string, unknown>;
    if (typeof value.date !== "string" || typeof value.replacement_count !== "number") {
      return { rows: [], error: dailySummaryContractError(`第 ${index + 1} 行缺少 date 或 replacement_count`) };
    }
    if (
      (value.daily_net_label !== null && typeof value.daily_net_label !== "number")
      || (value.daily_net_db_10d !== null && typeof value.daily_net_db_10d !== "number")
    ) {
      return { rows: [], error: dailySummaryContractError(`第 ${index + 1} 行收益字段类型错误`) };
    }
    parsed.push({
      date: value.date,
      replacement_count: value.replacement_count,
      daily_net_label: typeof value.daily_net_label === "number" ? value.daily_net_label : null,
      daily_net_db_10d: typeof value.daily_net_db_10d === "number" ? value.daily_net_db_10d : null,
      calculation_status: parseDailyCalculationStatus(value),
      missing_return_evidence_count: typeof value.missing_return_evidence_count === "number"
        ? value.missing_return_evidence_count
        : 0,
    });
  }
  return { rows: parsed, error: null };
}

function parseDailyCalculationStatus(value: Record<string, unknown>): DailySummary["calculation_status"] {
  if (["no_adjustment", "computed", "incomplete_evidence"].includes(String(value.calculation_status))) {
    return value.calculation_status as DailySummary["calculation_status"];
  }
  if (value.replacement_count === 0) return "no_adjustment";
  if (value.daily_net_label === null || value.daily_net_db_10d === null) return "incomplete_evidence";
  return "computed";
}

function validateIncompleteReturnEvidence(
  metrics: Record<string, unknown> | null | undefined,
): { rows: IncompleteReturnEvidence[]; error: HMMApiError | null } {
  const evidence = metrics?.incomplete_return_evidence;
  if (evidence === undefined) return { rows: [], error: null };
  if (!Array.isArray(evidence)) {
    return { rows: [], error: dailySummaryContractError("metrics_json.incomplete_return_evidence 必须是数组") };
  }
  const rows: IncompleteReturnEvidence[] = [];
  for (const [index, item] of evidence.entries()) {
    if (!item || typeof item !== "object") {
      return { rows: [], error: dailySummaryContractError(`缺失证据第 ${index + 1} 行不是对象`) };
    }
    const value = item as Record<string, unknown>;
    if (
      typeof value.date !== "string"
      || typeof value.symbol !== "string"
      || !["entered_by_hmm", "dropped_by_hmm"].includes(String(value.replacement_type))
      || !["label_return", "market_return"].includes(String(value.evidence_type))
      || typeof value.horizon_trading_days !== "number"
      || typeof value.required_start_date !== "string"
      || (value.required_label_date !== null && typeof value.required_label_date !== "string")
      || typeof value.reason !== "string"
    ) {
      return { rows: [], error: dailySummaryContractError(`缺失证据第 ${index + 1} 行字段不完整`) };
    }
    rows.push(value as IncompleteReturnEvidence);
  }
  return { rows, error: null };
}

function dailySummaryContractError(message: string): HMMApiError {
  return new HMMApiError({
    error_code: "HMM_EVOLUTION_CLIENT_CONTRACT_ERROR",
    reason_code: "hmm_evolution_client_invalid_daily_summary",
    message,
    context: { retry_condition: "修复 evaluator/API daily_summary 契约后重新加载。" },
  }, 502);
}

function warningSummary(warnings: Array<Record<string, unknown>>): string {
  if (warnings.length === 0) return "证据标记为 degraded，但未返回 warning；请核对 evaluator。";
  return warnings.map((warning) => typeof warning.message === "string" ? warning.message : typeof warning.code === "string" ? warning.code : "未命名 warning").join("；");
}

function evaluationError(evaluation: EvaluationDetail) {
  return new HMMApiError(
    {
      error_code: evaluation.error_code || "HMM_EVOLUTION_ERROR",
      reason_code: evaluation.reason_code || "hmm_evolution_evaluation_not_succeeded",
      message: evaluation.error_message || `评估终止于 ${evaluation.status}`,
      context: evaluation.error_context || {},
    },
    409,
  );
}

function Metric({ label, value, note, className }: { label: string; value: string; note: string; className: string }) {
  return <article className={`${styles.metricCard} ${className}`}><div className={styles.metricLabel}>{label}</div><div className={styles.metricValue}>{value}</div><div className={styles.metricNote}>{note}</div></article>;
}

function dailyStatusLabel(row: DailySummary): string {
  if (row.calculation_status === "no_adjustment") return "当日无调整";
  if (row.calculation_status === "incomplete_evidence") return `证据缺失 ${row.missing_return_evidence_count} 项`;
  return "已完整计算";
}

function formatDailyMetric(row: DailySummary, value: number | null): string {
  if (value !== null) return formatPercent(value);
  if (row.calculation_status === "no_adjustment") return "当日无调整";
  if (row.calculation_status === "incomplete_evidence") return "证据缺失";
  return "未生成";
}

function missingReasonLabel(reason: string): string {
  const labels: Record<string, string> = {
    forward_horizon_not_completed: "前瞻交易日尚未完成",
    start_price_missing: "起始日有效收盘价缺失",
    horizon_price_missing: "目标交易日有效收盘价缺失（常见于停牌）",
    label_artifact_return_missing: "QE 标签产物缺少该股票收益",
    market_return_missing_without_repository_evidence: "行情收益缺失且仓库未返回原因证据",
  };
  return labels[reason] || reason;
}

function formatPercent(value: number | null): string { return value === null ? "未计算" : `${value >= 0 ? "+" : ""}${(value * 100).toFixed(2)}%`; }
function formatDateTime(value: string | null): string { return value ? new Intl.DateTimeFormat("zh-CN", { dateStyle: "medium", timeStyle: "short" }).format(new Date(value)) : "未记录"; }
function shortHash(value: string): string { return value.length <= 18 ? value : `${value.slice(0, 9)}…${value.slice(-7)}`; }
