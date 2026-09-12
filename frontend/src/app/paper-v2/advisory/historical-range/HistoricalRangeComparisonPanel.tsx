"use client";

import { useEffect, useMemo, useState } from "react";

import type { HistoricalRangeComparison, HistoricalRangeRecord } from "@/lib/api/advisory";

type Props = {
  runs: HistoricalRangeRecord[];
  comparison: HistoricalRangeComparison | null;
  comparing: boolean;
  onCompare: (baselineRangeRunId: string, candidateRangeRunId: string) => Promise<HistoricalRangeComparison | null>;
};

function identityLabel(run: HistoricalRangeRecord): string {
  return `${String(run.research_program_id || "-")} · ${String(run.package_id || "-")}@${String(run.package_version || "-")}`;
}

function sideValue(side: HistoricalRangeComparison["metrics"][number]["baseline"]): string {
  return side.status === "AVAILABLE"
    ? String(side.value)
    : `${side.status}: ${side.reason_code || "UNAVAILABLE"}`;
}

function supportValue(value: unknown): string {
  return typeof value === "number" && Number.isFinite(value) ? String(value) : "0";
}

export function HistoricalRangeComparisonPanel({ runs, comparison, comparing, onCompare }: Props) {
  const [baselineId, setBaselineId] = useState("");
  const [candidateId, setCandidateId] = useState("");

  useEffect(() => {
    const ids = runs.map((run) => String(run.range_run_id));
    setBaselineId((current) => ids.includes(current) ? current : (ids[0] || ""));
    setCandidateId((current) => ids.includes(current) && current !== (ids[0] || "")
      ? current
      : (ids.find((id) => id !== (ids[0] || "")) || ""));
  }, [runs]);

  const displayed = comparison
    && comparison.baseline.range_run_id === baselineId
    && comparison.candidate.range_run_id === candidateId
    ? comparison
    : null;
  const keyMetrics = useMemo(() => displayed?.metrics.filter((metric) =>
    (metric.metric_key.includes("RETURN_NET_ABSOLUTE") || metric.metric_key.includes("RETURN_NET_EXCESS"))
      && [":mean_return", ":win_rate", ":max_drawdown", ":turnover"]
        .some((suffix) => metric.metric_key.endsWith(suffix))
  ) || [], [displayed]);
  const canCompare = runs.length >= 2 && baselineId && candidateId && baselineId !== candidateId && !comparing;
  const omittedBaseline = displayed
    ? displayed.omitted_diagnostics.baseline.available_daily_recall
      + displayed.omitted_diagnostics.baseline.unavailable_daily_recall
    : 0;
  const omittedCandidate = displayed
    ? displayed.omitted_diagnostics.candidate.available_daily_recall
      + displayed.omitted_diagnostics.candidate.unavailable_daily_recall
    : 0;

  if (runs.length < 2) {
    return <div className="ahr-state">同一批次至少需要两个 Program run，才可进行基线与候选对比。</div>;
  }

  return (
    <section className="ahr-comparison" data-testid="historical-range-comparison">
      <div className="ahr-section-head">
        <div><span className="ahr-eyebrow">SAME-BATCH COMPARISON</span><h3>基线与候选业务对比</h3></div>
        {displayed ? <span className={`ahr-status ahr-status-${displayed.comparability.status.toLowerCase().replaceAll("_", "-")}`}>{displayed.comparability.status}</span> : null}
      </div>
      <div className="ahr-comparison-controls">
        <label>基线 Program
          <select className="pv2-select" aria-label="基线 Program" value={baselineId} onChange={(event) => setBaselineId(event.target.value)}>
            {runs.map((run) => <option key={String(run.range_run_id)} value={String(run.range_run_id)}>{identityLabel(run)}</option>)}
          </select>
        </label>
        <label>候选 Program
          <select className="pv2-select" aria-label="候选 Program" value={candidateId} onChange={(event) => setCandidateId(event.target.value)}>
            {runs.map((run) => <option key={String(run.range_run_id)} value={String(run.range_run_id)} disabled={String(run.range_run_id) === baselineId}>{identityLabel(run)}</option>)}
          </select>
        </label>
        <button className="pv2-button" type="button" disabled={!canCompare} onClick={() => void onCompare(baselineId, candidateId)}>
          {comparing ? "读取中…" : "比较已有结果"}
        </button>
      </div>
      {!displayed ? <p className="ahr-warning">选择同批次的两个不同 Program 后读取紧凑 Summary；本操作不重跑回放，也不生成新证据。</p> : null}
      {displayed ? <>
        <div className="ahr-comparison-identities">
          <div><small>基线</small><strong>{String(displayed.baseline.package_id)}@{String(displayed.baseline.package_version)}</strong><code>{String(displayed.baseline.range_run_id)} / {String(displayed.baseline.summary_id || "NO_SUMMARY")}</code></div>
          <div><small>候选</small><strong>{String(displayed.candidate.package_id)}@{String(displayed.candidate.package_version)}</strong><code>{String(displayed.candidate.range_run_id)} / {String(displayed.candidate.summary_id || "NO_SUMMARY")}</code></div>
        </div>
        {displayed.comparability.blockers.length ? <div className="ahr-domain-state is-failed">阻断：{displayed.comparability.blockers.join(" / ")}</div> : null}
        {displayed.comparability.warnings.length ? <div className="ahr-domain-state is-waiting">提示：{displayed.comparability.warnings.join(" / ")}</div> : null}
        <div className="ahr-metrics">
          <div><small>基线成功日</small><strong>{supportValue(displayed.day_support.baseline.successful_day_count)}</strong></div>
          <div><small>候选成功日</small><strong>{supportValue(displayed.day_support.candidate.successful_day_count)}</strong></div>
          <div><small>基线无合格荐股日</small><strong>{supportValue(displayed.day_support.baseline.valid_no_candidate_day_count)}</strong></div>
          <div><small>候选无合格荐股日</small><strong>{supportValue(displayed.day_support.candidate.valid_no_candidate_day_count)}</strong></div>
          <div><small>差值口径</small><strong>候选 − 基线</strong></div>
        </div>
        {keyMetrics.length ? <div className="ahr-summary-metrics" data-testid="historical-range-key-metrics">
          {keyMetrics.map((metric) => <span key={`${metric.metric_key}:${metric.group_key || ""}`}>{metric.metric_key}: {metric.delta ?? "不可比"}</span>)}
        </div> : null}
        <p className="ahr-warning" data-testid="historical-range-omitted-diagnostics">
          逐日 recall 诊断不属于业务聚合指标，交互响应已省略：基线 {omittedBaseline} 项，候选 {omittedCandidate} 项；
          省略数量已审计，原始 Summary 仍可从既有明细接口读取。
        </p>
        <details open className="ahr-comparison-details">
          <summary>完整业务聚合指标（不筛除不利或 unavailable 项）</summary>
          <div className="ahr-table-wrap"><table className="ahr-table"><thead><tr><th>指标</th><th>基线</th><th>候选</th><th>候选 − 基线</th></tr></thead><tbody>
            {displayed.metrics.map((metric) => <tr key={`${metric.metric_key}:${metric.group_key || ""}`}><td><code>{metric.metric_key}</code>{metric.group_key ? <small>{metric.group_key}</small> : null}</td><td>{sideValue(metric.baseline)}</td><td>{sideValue(metric.candidate)}</td><td>{metric.delta ?? "-"}</td></tr>)}
            {!displayed.metrics.length ? <tr><td colSpan={4}><div className="ahr-state">双方最新 Summary 均未报告可展示指标。</div></td></tr> : null}
          </tbody></table></div>
        </details>
        <p className="ahr-warning">差值仅用于历史业务验证，不代表统计显著、模型胜出或可激活结论。</p>
      </> : null}
    </section>
  );
}
