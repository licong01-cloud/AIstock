"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  getRotationL2,
  getRotationL2Overview,
  HMMRiskApiError,
  type RotationL2Overview,
  type RotationL2Row,
} from "@/lib/hmm-risk/api";
import styles from "./rotation-l1.module.css";

const CONFIGURED_RUN_ID = process.env.NEXT_PUBLIC_HMM_ROTATION_L2_RUN_ID?.trim() || "";
const FROZEN_HMM_VERSION = "hmm_risk_l2_postcalibration_effect_v1";
const SUPERVISED_VERSION = "hmm_risk_rotation_l2_moneyflow_supervised_v1";
const PRICE_SUPERVISED_VERSION = "hmm_risk_rotation_l2_moneyflow_price_supervised_v1";

function formatNumber(value: number | null, digits = 4): string {
  return value === null ? "—" : value.toFixed(digits);
}

function stateLabel(row: RotationL2Row): string {
  if (row.availability === "unavailable") return "不可用";
  if (row.forecast_state === "trending") return "相对走强";
  if (row.forecast_state === "fading") return "相对走弱";
  return "中性";
}

export default function RotationL2Dashboard() {
  const [overview, setOverview] = useState<RotationL2Overview | null>(null);
  const [rows, setRows] = useState<RotationL2Row[]>([]);
  const [runId, setRunId] = useState(CONFIGURED_RUN_ID);
  const [requestedDate, setRequestedDate] = useState("");
  const [detailDate, setDetailDate] = useState("");
  const [detailAsOf, setDetailAsOf] = useState("");
  const [topCount, setTopCount] = useState(10);
  const [bottomCount, setBottomCount] = useState(10);
  const [countError, setCountError] = useState("");
  const [error, setError] = useState<{ message: string; reason: string } | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (CONFIGURED_RUN_ID) return;
    setRunId(new URLSearchParams(window.location.search).get("run_id")?.trim() || "");
  }, []);

  useEffect(() => {
    if (!runId) return;
    let active = true;
    async function load() {
      setLoading(true);
      setError(null);
      setRows([]);
      try {
        const nextOverview = await getRotationL2Overview(runId);
        if ([nextOverview.model_hash, nextOverview.input_hash, nextOverview.mapping_hash,
          nextOverview.quote_authority_hash].some((value) => typeof value !== "string" || !/^[a-f0-9]{64}$/.test(value))) {
          throw new HMMRiskApiError("L2 run 缺少正式身份，不接受空值间相等。", "hmm_risk_rotation_l2_ui_identity_invalid", 500);
        }
        const dates = nextOverview.available_trade_dates;
        if (nextOverview.run_id !== runId || !Array.isArray(dates) || dates.length === 0
          || dates.some((value) => typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value))
          || new Set(dates).size !== dates.length || dates.join() !== [...dates].sort().join()
          || dates[dates.length - 1] !== nextOverview.trade_date) {
          throw new HMMRiskApiError("L2 历史日期目录与明确 run 不一致。", "hmm_risk_rotation_l2_ui_date_catalog_invalid", 500);
        }
        const tradeDate = requestedDate || nextOverview.trade_date;
        if (!dates.includes(tradeDate)) {
          throw new HMMRiskApiError("请求日期不属于该 run 的历史目录，不回退邻日。", "hmm_risk_rotation_l2_ui_date_catalog_invalid", 500);
        }
        const detail = await getRotationL2(tradeDate, runId);
        if (!active) return;
        if (!Array.isArray(detail.rows) || detail.rows.length !== 131
          || detail.rows.some((row) => !row || typeof row.sector_code !== "string")
          || new Set(detail.rows.map((row) => row.sector_code)).size !== 131) {
          throw new HMMRiskApiError(
            "L2 行业目录不是完整的 131 项，拒绝展示不完整排名。",
            "hmm_risk_rotation_l2_ui_denominator_invalid",
            500,
          );
        }
        const asOf = detail.rows[0].as_of_date;
        if (detail.run_id !== runId || detail.trade_date !== tradeDate || !asOf || asOf >= tradeDate
          || (tradeDate === nextOverview.trade_date && asOf !== nextOverview.as_of_date)
          || detail.rows.some((row) => row.run_id !== runId || row.trade_date !== tradeDate
            || row.as_of_date !== asOf || row.model_hash !== nextOverview.model_hash
            || row.input_hash !== nextOverview.input_hash || row.mapping_hash !== nextOverview.mapping_hash
            || row.quote_authority_hash !== nextOverview.quote_authority_hash)) {
          throw new HMMRiskApiError("L2 日期明细与 run/模型/输入身份不一致。", "hmm_risk_rotation_l2_ui_identity_invalid", 500);
        }
        const hmm = nextOverview.model_version === FROZEN_HMM_VERSION;
        const supervised = [SUPERVISED_VERSION, PRICE_SUPERVISED_VERSION].includes(nextOverview.model_version || "");
        const states = new Set(["trending", "neutral", "fading"]);
        if ((nextOverview.model_version !== undefined && !hmm && !supervised)
          || (supervised && (nextOverview.validation_basis !== "HISTORICAL_CAUSAL_FIXED_TRAIN_DEVELOPMENT"
            || nextOverview.training_end !== "2025-03-31" || nextOverview.training_outcome_end !== "2025-04-15"
            || nextOverview.selection_basis !== "RETROSPECTIVE_DEVELOPMENT_SELECTED"))
          || (hmm && (nextOverview.validation_basis !== "POST_CALIBRATION_RETROSPECTIVE_DEVELOPMENT"
            || nextOverview.run_id !== runId))
          || detail.rows.some((row) => hmm
            ? row.model_version !== FROZEN_HMM_VERSION || row.run_id !== runId
              || row.trade_date !== tradeDate || row.as_of_date !== asOf || (row.availability === "available"
              && (row.semantic_state !== row.forecast_state || !states.has(row.semantic_state || "")
                || !states.has(row.daily_rank_group || "") || row.rotation_score === null || !Number.isFinite(row.rotation_score)
                || row.rotation_score < -0.5 || row.rotation_score > 0.5))
            : supervised ? row.model_version !== nextOverview.model_version
            : row.model_version !== undefined || nextOverview.validation_basis !== "HISTORICAL_CAUSAL_REPLAY_ZERO_FIT")) {
          throw new HMMRiskApiError("L2 模型版本与状态/排名投影不一致，拒绝混用。", "hmm_risk_rotation_l2_ui_version_invalid", 500);
        }
        if (detail.rows.some((row) => row.availability === "available"
          ? row.rotation_score === null || !Number.isFinite(row.rotation_score) || row.rotation_score < -0.5
            || row.rotation_score > 0.5 || !states.has(row.forecast_state || "")
          : row.availability !== "unavailable" || row.rotation_score !== null || row.forecast_state !== null || !row.reason_code)) {
          throw new HMMRiskApiError("L2 分数或不可用状态不合法，不生成默认排名。", "hmm_risk_rotation_l2_ui_prediction_invalid", 500);
        }
        if (nextOverview.model_version === PRICE_SUPERVISED_VERSION && detail.rows.some((row) => {
          const c = row.feature_contributions;
          if (row.availability === "unavailable") return c !== null;
          if (!c || !/^[0-9a-f]{64}$/.test(c.model_parameter_sha256)) return true;
          const terms = [c.intercept, c.moneyflow_level_linear_term, c.moneyflow_delta_linear_term,
            c.relative_momentum_linear_term, c.relative_downside_linear_term];
          return terms.some((value) => typeof value !== "number" || !Number.isFinite(value))
            || !Number.isFinite(c.raw_prediction) || c.average_rank_score !== row.rotation_score
            || c.daily_rank_group !== row.forecast_state
            || Math.abs(c.raw_prediction - terms.reduce<number>((sum, value) => sum + (value as number), 0)) > 1e-12 + 1e-10 * Math.abs(c.raw_prediction);
        })) {
          throw new HMMRiskApiError("四特征解释与原始预测不一致。", "hmm_risk_rotation_l2_ui_explanation_invalid", 500);
        }
        setOverview(nextOverview);
        setDetailDate(tradeDate);
        setDetailAsOf(asOf);
        setRows(detail.rows);
      } catch (caught) {
        if (!active) return;
        const apiError = caught instanceof HMMRiskApiError ? caught : null;
        setError({
          message: caught instanceof Error ? caught.message : "HMM L2 数据读取失败",
          reason: apiError?.reasonCode || "hmm_risk_client_request_failed",
        });
      } finally {
        if (active) setLoading(false);
      }
    }
    void load();
    return () => {
      active = false;
    };
  }, [runId, requestedDate]);

  const selection = useMemo(() => {
    const available = rows
      .filter((row) => row.availability === "available" && row.rotation_score !== null)
      .sort((left, right) => {
        const scoreOrder = (right.rotation_score ?? -Infinity) - (left.rotation_score ?? -Infinity);
        return scoreOrder || left.sector_code.localeCompare(right.sector_code);
      });
    const requestedCount = topCount + bottomCount;
    const actualTopCount = Math.min(topCount, available.length);
    const actualBottomCount = Math.min(bottomCount, available.length - actualTopCount);
    const bottomStart = available.length - actualBottomCount;
    const topBoundaryTie =
      actualTopCount > 0 && actualTopCount < available.length
        ? available[actualTopCount - 1].rotation_score === available[actualTopCount].rotation_score
        : false;
    const bottomBoundaryTie =
      actualBottomCount > 0 && bottomStart > actualTopCount
        ? available[bottomStart].rotation_score === available[bottomStart - 1].rotation_score
        : false;
    return {
      top: available.slice(0, actualTopCount),
      bottom: actualBottomCount === 0 ? [] : available.slice(bottomStart).reverse(),
      unavailableCount: rows.length - available.length,
      boundaryTie: topBoundaryTie || bottomBoundaryTie,
      insufficientAvailable: available.length < requestedCount,
      availableCount: available.length,
      actualCount: actualTopCount + actualBottomCount,
    };
  }, [rows, topCount, bottomCount]);

  function updateCounts(kind: "top" | "bottom", raw: string) {
    const value = Number(raw);
    if (!Number.isInteger(value) || value < 0 || value > 30) {
      setCountError("数量必须是 0 到 30 的整数。");
      return;
    }
    const nextTop = kind === "top" ? value : topCount;
    const nextBottom = kind === "bottom" ? value : bottomCount;
    if (nextTop + nextBottom < 1 || nextTop + nextBottom > 30) {
      setCountError("前列与后列合计必须在 1 到 30 之间。");
      return;
    }
    setCountError("");
    if (kind === "top") setTopCount(value);
    else setBottomCount(value);
  }

  const cards = [...selection.top, ...selection.bottom];

  return (
    <main className={styles.root}>
      <header className={styles.header}>
        <div>
          <p className={styles.eyebrow}>HMM EVOLUTION · L2 PRIMARY</p>
          <h1>申万二级行业轮动研究预测</h1>
          <p className={styles.subtitle}>
            展示因果历史回放产生的相对强弱分数；不是收益保证、交易信号或 advisory 能力。
          </p>
        </div>
        <Link href="/hmm-evolution" className={styles.link}>查看演进实验</Link>
      </header>

      {!runId && (
        <section className={styles.warning} role="status">
          NOT_CONFIGURED：未显式配置 NEXT_PUBLIC_HMM_ROTATION_L2_RUN_ID，页面不会猜测最新模型。
        </section>
      )}
      {loading && <section className={styles.notice}>正在读取真实 L2 prediction repository…</section>}
      {overview && (
        <section className={styles.notice} aria-label="L2 历史日期选择">
          <label>历史预测日 <select aria-label="历史预测日" value={requestedDate || overview.trade_date}
            onChange={(event) => {
              setRows([]);
              setLoading(true);
              setError(null);
              setRequestedDate(event.target.value);
            }}>
            {overview.available_trade_dates.map((value) => <option key={value} value={value}>{value}</option>)}
          </select></label>
          <span> 同一 run 的 {overview.available_trade_dates.length} 个存储日期；不重算模型、不回退日期。</span>
        </section>
      )}
      {error && (
        <section className={styles.error} role="alert">
          <strong>预测暂不可用</strong><span>{error.message}</span><code>{error.reason}</code>
        </section>
      )}
      {overview && !error && !loading && (
        <>
          <section className={styles.statusGrid} aria-label="L2 轮动能力状态">
            <article><span>研究表面</span><strong>{overview.research_surface_status}</strong></article>
            <article><span>轮动能力</span><strong>{overview.rotation_l2_capability_status}</strong></article>
            <article><span>开发效果</span><strong>{overview.effect_status}</strong></article>
            <article><span>Advisory</span><strong>{overview.advisory_status}</strong></article>
          </section>
          <section className={styles.evidence}>
            <div><span>预测日 / as-of</span><strong>{detailDate} / {detailAsOf}</strong></div>
            <div><span>真实覆盖</span><strong>{selection.availableCount} / {rows.length}</strong></div>
            <div><span>Development Rank IC</span><strong>{formatNumber(overview.metrics.overall.mean_daily_rank_ic)}</strong></div>
            <div><span>95% HAC 区间</span><strong>[{formatNumber(overview.metrics.hac.lower)}, {formatNumber(overview.metrics.hac.upper)}]</strong></div>
          </section>
          <section className={styles.warning}>
            forward_confirmation={overview.forward_confirmation}；tail_accessed={String(overview.tail_accessed)}。禁止接入 Selection、Paper、QMT 或自动调仓。
          </section>
          <section className={styles.notice} aria-label="排名数量配置">
            <label>前列 <input aria-label="前列数量" type="number" min={0} max={30} value={topCount} onChange={(event) => updateCounts("top", event.target.value)} /></label>{" "}
            <label>后列 <input aria-label="后列数量" type="number" min={0} max={30} value={bottomCount} onChange={(event) => updateCounts("bottom", event.target.value)} /></label>{" "}
            <span>合计 {topCount + bottomCount} / 30；不可用目录 {selection.unavailableCount}</span>
            {countError && <strong role="alert">{countError}</strong>}
            {selection.insufficientAvailable && (
              <strong role="alert">
                可用行业仅 {selection.availableCount} 项，少于请求的 {topCount + bottomCount} 项；实际展示 {selection.actualCount} 项且不重复。
              </strong>
            )}
            {selection.boundaryTie && <strong>边界存在同分；代码仅用于稳定显示次序，不代表严格优胜。</strong>}
          </section>
          <section className={styles.heatmap} aria-label="申万二级行业轮动排名">
            {cards.map((row) => (
              <article key={`${row.sector_code}-${row.forecast_state}`} className={`${styles.card} ${styles[row.model_version === FROZEN_HMM_VERSION ? row.daily_rank_group || "unavailable" : row.forecast_state || "unavailable"]}`}>
                <div>
                  <strong>{row.sector_name}</strong>
                  {row.sector_name !== row.sector_code && <code>{row.sector_code}</code>}
                </div>
                {row.model_version === FROZEN_HMM_VERSION ? (
                  <>
                    <span>行业内状态：{row.semantic_state === "trending" ? "走强" : row.semantic_state === "fading" ? "走弱" : "中性"}</span>
                    <span>当日相对排名：{row.daily_rank_group === "trending" ? "前列" : row.daily_rank_group === "fading" ? "后列" : "中段"}</span>
                  </>
                ) : <span>{stateLabel(row)}</span>}
                <b>{formatNumber(row.rotation_score, 5)}</b>
                {row.model_version === PRICE_SUPERVISED_VERSION && row.feature_contributions && <>
                  <span>原始预测 {formatNumber(row.feature_contributions.raw_prediction, 5)}；截距 {formatNumber(row.feature_contributions.intercept, 5)}</span>
                  <span>资金流水平项 {formatNumber(row.feature_contributions.moneyflow_level_linear_term, 5)}；变化项 {formatNumber(row.feature_contributions.moneyflow_delta_linear_term, 5)}</span>
                  <span>相对动量项 {formatNumber(row.feature_contributions.relative_momentum_linear_term ?? null, 5)}；下行半偏差项 {formatNumber(row.feature_contributions.relative_downside_linear_term ?? null, 5)}</span>
                </>}
              </article>
            ))}
          </section>
          <footer className={styles.lineage}>
            <span>run {overview.run_id}</span><span>model {overview.model_hash}</span>
            <span>input {overview.input_hash}</span><span>mapping {overview.mapping_hash}</span>
            <span>quote {overview.quote_authority_hash}</span><span>validation {overview.validation_basis}</span>
            {[SUPERVISED_VERSION, PRICE_SUPERVISED_VERSION].includes(overview.model_version || "") && <span>训练 decision 截止 {overview.training_end}；训练标签截止 {overview.training_outcome_end}。固定模型历史样本外回放；研发已查看历史，不是 untouched / 前瞻确认。</span>}
          </footer>
        </>
      )}
      <section aria-label="L1 历史风险独立能力">
        <p className={styles.subtitle}>以下为既有 L1 风险能力，不属于 L2 轮动分数，也不构成 L2 风险占位。</p>
      </section>
    </main>
  );
}
