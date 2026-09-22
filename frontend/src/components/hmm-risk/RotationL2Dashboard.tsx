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
      try {
        const nextOverview = await getRotationL2Overview(runId);
        const detail = await getRotationL2(nextOverview.trade_date, runId);
        if (!active) return;
        if (detail.rows.length !== 131 || new Set(detail.rows.map((row) => row.sector_code)).size !== 131) {
          throw new HMMRiskApiError(
            "L2 行业目录不是完整的 131 项，拒绝展示不完整排名。",
            "hmm_risk_rotation_l2_ui_denominator_invalid",
            500,
          );
        }
        setOverview(nextOverview);
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
  }, [runId]);

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
      {error && (
        <section className={styles.error} role="alert">
          <strong>预测暂不可用</strong><span>{error.message}</span><code>{error.reason}</code>
        </section>
      )}
      {overview && !error && (
        <>
          <section className={styles.statusGrid} aria-label="L2 轮动能力状态">
            <article><span>研究表面</span><strong>{overview.research_surface_status}</strong></article>
            <article><span>轮动能力</span><strong>{overview.rotation_l2_capability_status}</strong></article>
            <article><span>开发效果</span><strong>{overview.effect_status}</strong></article>
            <article><span>Advisory</span><strong>{overview.advisory_status}</strong></article>
          </section>
          <section className={styles.evidence}>
            <div><span>预测日 / as-of</span><strong>{overview.trade_date} / {overview.as_of_date}</strong></div>
            <div><span>真实覆盖</span><strong>{overview.available_count} / {overview.sector_count}</strong></div>
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
              <article key={`${row.sector_code}-${row.forecast_state}`} className={`${styles.card} ${styles[row.forecast_state || "unavailable"]}`}>
                <div>
                  <strong>{row.sector_name}</strong>
                  {row.sector_name !== row.sector_code && <code>{row.sector_code}</code>}
                </div>
                <span>{stateLabel(row)}</span><b>{formatNumber(row.rotation_score, 5)}</b>
              </article>
            ))}
          </section>
          <footer className={styles.lineage}>
            <span>run {overview.run_id}</span><span>model {overview.model_hash}</span>
            <span>input {overview.input_hash}</span><span>mapping {overview.mapping_hash}</span>
            <span>quote {overview.quote_authority_hash}</span><span>validation {overview.validation_basis}</span>
          </footer>
        </>
      )}
    </main>
  );
}
