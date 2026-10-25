"use client";

import { useEffect, useMemo, useState } from "react";
import { getRiskL2, getRiskL2Overview, RiskL2Detail, RiskL2Overview } from "@/lib/hmm-risk/api";

function errorText(error: unknown) {
  return error instanceof Error ? error.message : "HMM 风险读取失败";
}

function HACSummary({ values }: { values: Record<string, unknown> }) {
  if (typeof values.status === "string") return <p>{values.status}：{String(values.reason ?? "原因未提供")}</p>;
  return <ul>{Object.entries(values).map(([name, raw]) => {
    const value = raw as { status?: string; interval_95?: number[]; n_dates?: number; reason?: string };
    return <li key={name}>{name}：{value.status}；{value.interval_95 ? `95% 区间 [${value.interval_95.join(", ")}]；日期数 ${value.n_dates}` : `不可计算原因：${value.reason}`}</li>;
  })}</ul>;
}

export default function RiskL2Panel({ initialRunId = "" }: { initialRunId?: string }) {
  const [inputRun, setInputRun] = useState(initialRunId);
  const [runId, setRunId] = useState(initialRunId);
  const [overview, setOverview] = useState<RiskL2Overview | null>(null);
  const [detail, setDetail] = useState<RiskL2Detail | null>(null);
  const [tradeDate, setTradeDate] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [top, setTop] = useState("10");
  const [bottom, setBottom] = useState("10");

  useEffect(() => {
    let current = true;
    setOverview(null); setDetail(null); setTradeDate(""); setError(null); setLoading(false);
    if (!runId) return;
    if (!/^[0-9a-f]{64}$/.test(runId)) {
      setError("必须显式提供 64 位 run identity；不自动选择 latest"); return;
    }
    setLoading(true);
    getRiskL2Overview(runId).then((value) => {
      if (!current) return;
      const historical = value.validation_basis === "HISTORICAL_CAUSAL_FIXED_TRAIN_DEVELOPMENT";
      const inferred = value.validation_basis === "CAUSAL_DATE_INFERENCE_NO_OUTCOMES";
      if (value.run_id !== runId || (!historical && !inferred)
        || (historical && (value.dates.length !== 424 || !value.compact_summary.overall || !value.compact_summary.hac))
        || (inferred && (value.dates.length === 0 || value.effect_status !== "NOT_EVALUATED_FOR_INFERENCE_DATES"
          || value.dates.some((day) => !/^\d{4}-\d{2}-\d{2}$/.test(day))
          || value.compact_summary.model_identity?.model_sha256 !== value.model_hash || !value.input_identity))
        || !value.dates.includes(value.trade_date) || new Set(value.dates).size !== value.dates.length) {
        throw new Error("hmm_risk_risk_l2_readback_failed：版本或历史日期身份不一致");
      }
      setOverview(value); setTradeDate(value.trade_date);
    }).catch((e: unknown) => {
      if (current) { setError(errorText(e)); setLoading(false); }
    });
    return () => { current = false; };
  }, [runId]);

  useEffect(() => {
    let current = true;
    setDetail(null);
    if (!overview || !tradeDate || overview.run_id !== runId) return;
    setError(null); setLoading(true);
    getRiskL2(tradeDate, runId).then((value) => {
      if (!current) return;
      if (value.run_id !== runId || value.trade_date !== tradeDate || value.model_hash !== overview.model_hash
        || value.input_hash !== overview.input_hash || value.rows.length !== 131
        || value.mapping_hash !== overview.mapping_hash || value.as_of_date >= tradeDate
        || value.validation_basis !== overview.validation_basis || value.acceptance_hash !== overview.acceptance_hash
        || (value.validation_basis === "HISTORICAL_CAUSAL_FIXED_TRAIN_DEVELOPMENT"
          && (!value.compact_summary.overall || !value.compact_summary.hac))
        || (value.validation_basis === "CAUSAL_DATE_INFERENCE_NO_OUTCOMES"
          && (value.effect_status !== "NOT_EVALUATED_FOR_INFERENCE_DATES"
            || value.rows.some((r) => r.outcome_status !== "OUTCOME_NOT_EVALUATED"
              || r.event !== null || r.realized_drawdown !== null || r.realized_return !== null)
            || value.compact_summary.model_identity?.model_sha256 !== value.model_hash || !value.input_identity))
        || value.forward_confirmation !== "NOT_STARTED" || value.advisory_status !== "NOT_AVAILABLE"
        || new Set(value.rows.map((r) => r.sector_code)).size !== 131
        || value.rows.some((r) => r.run_id !== runId || r.trade_date !== tradeDate || r.as_of_date !== value.as_of_date)
        || value.day_summary.warning_count !== value.rows.filter((r) => r.warning === true).length) {
        throw new Error("hmm_risk_risk_l2_readback_failed：日期/模型/131 行业身份不一致");
      }
      setDetail(value);
    }).catch((e: unknown) => { if (current) setError(errorText(e)); })
      .finally(() => { if (current) setLoading(false); });
    return () => { current = false; };
  }, [overview, runId, tradeDate]);

  const limitValid = /^\d+$/.test(top) && /^\d+$/.test(bottom)
    && Number(top) + Number(bottom) >= 1 && Number(top) + Number(bottom) <= 30;
  const shown = useMemo(() => {
    if (!detail || !limitValid) return [];
    const ranked = detail.rows.filter((r) => r.availability === "available" && r.probability !== null)
      .sort((a, b) => b.probability! - a.probability! || a.sector_code.localeCompare(b.sector_code));
    const chosen = [...ranked.slice(0, Number(top)), ...(Number(bottom) ? ranked.slice(-Number(bottom)).reverse() : [])];
    return Array.from(new Map(chosen.map((r) => [r.sector_code, r])).values());
  }, [detail, top, bottom, limitValid]);
  const shownWarnings = shown.filter((r) => r.warning === true).length;
  const unavailable = detail?.rows.filter((r) => r.availability === "unavailable") ?? [];
  const download = () => {
    if (!detail) return;
    const url = URL.createObjectURL(new Blob([JSON.stringify(detail)], { type: "application/json" }));
    const link = document.createElement("a"); link.href = url;
    link.download = `risk-l2-${runId}-${tradeDate}.json`; link.click(); URL.revokeObjectURL(url);
  };

  return (
    <section aria-label="申万二级行业风险研究" className="space-y-4 rounded-lg border border-border bg-card p-4 text-card-foreground">
      <h1 className="text-xl font-semibold">L2 风险研究与指定日期推断</h1>
      <p>研究输出，未完成前瞻确认；不是今日预警、买卖建议或已验证的收益增益。</p>
      <p>目标：未来 10 交易日绝对路径回撤 ≤ -8%；固定报警概率阈值 0.20。分类模型输出未经独立概率校准，低概率不代表安全。</p>
      <form onSubmit={(e) => { e.preventDefault(); setRunId(inputRun.trim()); }} className="flex flex-wrap gap-2">
        <label>明确 run identity <input aria-label="run identity" value={inputRun} onChange={(e) => setInputRun(e.target.value)} className="w-80 rounded border border-input bg-background p-2" /></label>
        <button type="submit" className="rounded border border-border px-3">读取指定版本</button>
      </form>
      {!runId && <p>请提供明确的封存 run；不自动搜索最新模型。</p>}
      {overview && <label>历史决策日期 <select aria-label="历史决策日期" value={tradeDate} onChange={(e) => setTradeDate(e.target.value)} className="rounded border border-input bg-background p-2">
        {overview.dates.map((d) => <option key={d} value={d}>{d}</option>)}
      </select></label>}
      {loading && <p role="status">正在读取指定历史数据…</p>}
      {error && <p role="alert">{error}</p>}
      {detail && detail.run_id === runId && detail.trade_date === tradeDate && !error && <>
        <p>历史日期：{detail.trade_date}；严格 as-of：{detail.as_of_date}；{detail.validation_basis}（非 OOF/非 untouched）</p>
        <dl className="break-all text-sm">
          <dt>模型</dt><dd>{detail.model_version} / {detail.model_hash}</dd>
          <dt>run / 输入 / mapping</dt><dd>{detail.run_id} / {detail.input_hash} / {detail.mapping_hash}</dd>
          <dt>状态</dt><dd>{detail.risk_l2_capability_status}；surface={detail.research_surface_status}；effect={detail.effect_status}；power={detail.forward_power_status}；forward={detail.forward_confirmation}；advisory={detail.advisory_status}</dd>
        </dl>
        <p>完整目录 131；可用 {detail.day_summary.available_count}；不可用 {detail.day_summary.unavailable_count}；未知报警 {detail.day_summary.unknown_warning_count}。</p>
        <p data-testid="warning-summary">全部报警 {detail.day_summary.warning_count}；当前显示报警 {shownWarnings}；隐藏报警 {detail.day_summary.warning_count - shownWarnings}。</p>
        {detail.validation_basis === "CAUSAL_DATE_INFERENCE_NO_OUTCOMES" ? <details open>
          <summary>指定日期风险推断：本次未评价效果</summary>
          <p>本次未读取标签或未来收益，未应用交易策略；不能由历史模型效果推导本日期有效，更不代表实时行情已接通。</p>
          <p className="break-all">模型训练输入：{detail.compact_summary.model_identity!.training_input_sha256}；训练 PIT：{detail.compact_summary.model_identity!.training_pit_bundle_sha256}</p>
          <p className="break-all">本次数据：{detail.input_identity!.release_identity.frozen_release_generation}；cutoff={detail.input_identity!.release_identity.cutoff}；manifest={detail.input_identity!.release_identity.dataset_manifest_sha256}</p>
          <p>本次可用比例={detail.compact_summary.prediction_coverage}；完整131目录；事后收益/事件均未评价，不填0或沿用原标签。</p>
        </details> : <details open>
          <summary>历史 development 效果与误报/漏报代价（不是交易 PnL）</summary>
          <p>precision={detail.compact_summary.overall!.precision}；base rate={detail.compact_summary.overall!.base_rate}；lift={detail.compact_summary.overall!.precision_lift}；recall={detail.compact_summary.overall!.recall}；合法 M={detail.compact_summary.overall!.M}；Brier={detail.compact_summary.overall!.brier}</p>
          <p>已知报警 FP={detail.compact_summary.overall!.FP} / {detail.compact_summary.overall!.known_warnings}；漏报 FN={detail.compact_summary.overall!.FN}；错误报警未来收益均值={detail.compact_summary.overall!.false_alert_return_mean}；漏报回撤均值={detail.compact_summary.overall!.missed_drawdown_mean}。不等于可避免亏损。</p>
          <p>总体 coverage={detail.compact_summary.prediction_coverage}；成熟日期有效占比={detail.compact_summary.valid_mature_day_share}；标签成熟/合法 NA：{Object.entries(detail.compact_summary.outcome_status_counts).map(([k, n]) => `${k}=${n}`).join("；")}</p>
          <div aria-label="原HAC区间及原因"><HACSummary values={detail.compact_summary.hac!} /></div>
        </details>}
        <div className="flex flex-wrap gap-3">
          <label>最高概率数量 <input aria-label="最高概率数量" type="number" min="0" max="30" value={top} onChange={(e) => setTop(e.target.value)} className="w-20 rounded border border-input bg-background p-1" /></label>
          <label>最低概率数量 <input aria-label="最低概率数量" type="number" min="0" max="30" value={bottom} onChange={(e) => setBottom(e.target.value)} className="w-20 rounded border border-input bg-background p-1" /></label>
          <button onClick={download} className="rounded border border-border px-3">下载完整 131 行业结果</button>
        </div>
        {!limitValid && <p role="alert">前后数量须为非负整数，合计 1～30；不会截断后台人口或报警。</p>}
        <div role="region" aria-label="L2风险概率展示" className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          {shown.map((row) => <article key={row.sector_code} className="rounded border border-border p-3">
            <code>{row.sector_code}</code><p>名称 authority：{row.name_authority}</p>
            <p>风险概率 {row.probability?.toFixed(4)}；{row.warning ? "报警" : "未报警（非安全承诺）"}</p>
            <p>事后评价：{row.outcome_status}；event={row.event === null ? "NA" : row.event}；回撤={row.realized_drawdown === null ? "NA" : row.realized_drawdown}</p>
          </article>)}
        </div>
        {detail.day_summary.available_count === 0 && <p>该日无可用概率，请查看以下原始不可用原因。</p>}
        <details open={unavailable.length > 0}><summary>不可用行业与原始原因（{unavailable.length}）</summary>
          <ul>{unavailable.map((r) => <li key={r.sector_code}>{r.sector_code}：{r.reason_code}；概率/报警未知，不纳入低风险榜</li>)}</ul>
        </details>
      </>}
    </section>
  );
}
