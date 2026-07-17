"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import { ArrowLeft, RefreshCw } from "lucide-react";
import { getEvaluation, HMMApiError } from "@/lib/hmm-evolution/api";
import type { EvaluationDetail } from "@/lib/hmm-research/contracts";
import { TERMINAL_EVALUATION_STATUSES } from "@/lib/hmm-research/contracts";
import EvidencePanel from "@/components/hmm-research/EvidencePanel";
import HMMResearchShell from "@/components/hmm-research/HMMResearchShell";
import StatusBadge from "@/components/hmm-research/StatusBadge";
import VisibleErrorState from "@/components/hmm-research/VisibleErrorState";
import styles from "@/components/hmm-research/hmm-research.module.css";

type DailySummary = {
  date: string;
  replacement_count: number;
  daily_net_label: number | null;
  daily_net_db_10d: number | null;
};

export default function EvaluationDetailView({ evalId }: { evalId: string }) {
  const [evaluation, setEvaluation] = useState<EvaluationDetail | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setError(null);
    setLoading(true);
    try {
      setEvaluation(await getEvaluation(evalId));
    } catch (nextError) {
      setError(nextError);
    } finally {
      setLoading(false);
    }
  }, [evalId]);

  useEffect(() => { void load(); }, [load]);
  useEffect(() => {
    if (!evaluation || TERMINAL_EVALUATION_STATUSES.has(evaluation.status)) return;
    const timer = setTimeout(() => void load(), 3_000);
    return () => clearTimeout(timer);
  }, [evaluation, load]);

  const dailySummary = useMemo(() => extractDailySummary(evaluation?.metrics_json), [evaluation]);

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
          <div className={styles.panelActions}>{evaluation ? <StatusBadge status={evaluation.status} /> : null}<button type="button" className={styles.button} onClick={() => void load()}><RefreshCw size={14} />刷新</button></div>
        </div>
        {loading ? <div className={styles.loadingState}>正在加载评估证据；失败会显式终止。</div> : null}
        {error ? <VisibleErrorState error={error} onRetry={() => void load()} /> : null}
        {evaluation ? (
          <div className={styles.stack}>
            <section className={styles.metricsGrid}>
              <Metric label={`净标签收益 · ${evaluation.label_horizon_days} 交易日`} value={formatPercent(evaluation.net_label_return)} note={`${evaluation.label_comparable_day_count} 个可比日`} className={styles.metricGreen} />
              <Metric label="Net DB 10D" value={formatPercent(evaluation.net_db_10d)} note={`${evaluation.db_comparable_day_count} 个可比日`} className={styles.metricBlue} />
              <Metric label="正值日比例" value={formatPercent(evaluation.positive_net_label_day_ratio)} note={`改变 TopK ${evaluation.changed_day_count} 日`} className={styles.metricAmber} />
              <Metric label="覆盖率" value={formatPercent(evaluation.primary_coverage_ratio)} note={`证据质量：${evaluation.evidence_quality || "未生成"}`} className={styles.metricSlate} />
            </section>

            {evaluation.evidence_quality === "degraded" ? <div className={`${styles.notice} ${styles.noticeWarning}`}><strong>证据降级</strong><span>{warningSummary(evaluation.warnings_json)}</span></div> : null}
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
              <div className={styles.panelBodyTable}><DailySummaryTable rows={dailySummary} horizon={evaluation.label_horizon_days} /></div>
            </section>
          </div>
        ) : null}
      </main>
    </HMMResearchShell>
  );
}

function DailySummaryTable({ rows, horizon }: { rows: DailySummary[]; horizon: number }) {
  if (rows.length === 0) return <div className={styles.emptyState}>尚无逐日结果；空集合不会显示为成功收益。</div>;
  return <table className={styles.table}><thead><tr><th>交易日</th><th>替换数量</th><th>净标签收益 · {horizon}D</th><th>Net DB 10D</th></tr></thead><tbody>{rows.map((row) => <tr key={row.date}><td>{row.date}</td><td>{row.replacement_count}</td><td>{formatPercent(row.daily_net_label)}</td><td>{formatPercent(row.daily_net_db_10d)}</td></tr>)}</tbody></table>;
}

function extractDailySummary(metrics: Record<string, unknown> | null | undefined): DailySummary[] {
  const rows = metrics?.daily_summary;
  if (!Array.isArray(rows)) return [];
  return rows.flatMap((row) => {
    if (!row || typeof row !== "object") return [];
    const value = row as Record<string, unknown>;
    if (typeof value.date !== "string" || typeof value.replacement_count !== "number") return [];
    return [{
      date: value.date,
      replacement_count: value.replacement_count,
      daily_net_label: typeof value.daily_net_label === "number" ? value.daily_net_label : null,
      daily_net_db_10d: typeof value.daily_net_db_10d === "number" ? value.daily_net_db_10d : null,
    }];
  });
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

function formatPercent(value: number | null): string { return value === null ? "未计算" : `${value >= 0 ? "+" : ""}${(value * 100).toFixed(2)}%`; }
function formatDateTime(value: string | null): string { return value ? new Intl.DateTimeFormat("zh-CN", { dateStyle: "medium", timeStyle: "short" }).format(new Date(value)) : "未记录"; }
function shortHash(value: string): string { return value.length <= 18 ? value : `${value.slice(0, 9)}…${value.slice(-7)}`; }
