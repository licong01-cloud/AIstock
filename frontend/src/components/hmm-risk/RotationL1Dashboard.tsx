"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  getRotationL1,
  getRotationOverview,
  HMMRiskApiError,
  type RotationL1Row,
  type RotationOverview,
} from "@/lib/hmm-risk/api";
import styles from "./rotation-l1.module.css";

function number(value: number | null, digits = 4): string {
  return value === null ? "—" : value.toFixed(digits);
}

function stateLabel(row: RotationL1Row): string {
  if (row.availability === "unavailable") return "不可用";
  return row.forecast_state === "trending" ? "走强" : row.forecast_state === "fading" ? "走弱" : "中性";
}

export default function RotationL1Dashboard() {
  const [overview, setOverview] = useState<RotationOverview | null>(null);
  const [rows, setRows] = useState<RotationL1Row[]>([]);
  const [error, setError] = useState<{ message: string; reason: string } | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let active = true;
    async function load() {
      try {
        const nextOverview = await getRotationOverview();
        const detail = await getRotationL1(nextOverview.trade_date, nextOverview.model_hash);
        if (!active) return;
        if (detail.rows.length !== 31) {
          throw new HMMRiskApiError(
            "L1 板块分母不是 31，拒绝展示不完整热力图。",
            "hmm_risk_rotation_ui_denominator_invalid",
            500,
          );
        }
        setOverview(nextOverview);
        setRows(detail.rows);
      } catch (caught) {
        if (!active) return;
        const apiError = caught instanceof HMMRiskApiError ? caught : null;
        setError({
          message: caught instanceof Error ? caught.message : "HMM Risk 数据读取失败",
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
  }, []);

  const sorted = useMemo(
    () => [...rows].sort((left, right) => (right.rotation_score ?? -Infinity) - (left.rotation_score ?? -Infinity)),
    [rows],
  );

  return (
    <main className={styles.root}>
      <header className={styles.header}>
        <div>
          <p className={styles.eyebrow}>HMM EVOLUTION · G2-A</p>
          <h1>L1 板块轮动预测</h1>
          <p className={styles.subtitle}>研究展示与正式 advisory 状态严格分离；rotation score 不是概率或置信度。</p>
        </div>
        <Link href="/hmm-evolution" className={styles.link}>演进实验室</Link>
      </header>

      {loading && <section className={styles.notice}>正在读取真实 prediction repository…</section>}
      {error && (
        <section className={styles.error} role="alert">
          <strong>预测暂不可用</strong>
          <span>{error.message}</span>
          <code>{error.reason}</code>
        </section>
      )}
      {overview && !error && (
        <>
          <section className={styles.statusGrid} aria-label="HMM 风险能力状态">
            <article><span>研究表面</span><strong>{overview.research_surface_status}</strong></article>
            <article><span>轮动能力</span><strong>{overview.rotation_l1_capability_status}</strong></article>
            <article><span>前瞻确认</span><strong>{overview.forward_confirmation}</strong></article>
            <article><span>Advisory</span><strong>{overview.advisory_status}</strong></article>
          </section>
          <section className={styles.evidence}>
            <div><span>预测日 / as-of</span><strong>{overview.trade_date} / {overview.as_of_date}</strong></div>
            <div><span>真实覆盖</span><strong>{overview.available_count} / {overview.sector_count}</strong></div>
            <div><span>Development OOF Rank IC</span><strong>{number(overview.development_oof_rank_ic)}</strong></div>
            <div><span>95% HAC 区间</span><strong>[{number(overview.development_oof_rank_ic_hac_lower)}, {number(overview.development_oof_rank_ic_hac_upper)}]</strong></div>
          </section>
          {overview.rotation_l1_capability_status === "NOT_AVAILABLE" &&
            overview.development_oof_rank_ic !== null &&
            overview.development_oof_rank_ic < overview.binding_mbe_rank_ic && (
              <section className={styles.warning}>
                BELOW_BINDING_MBE：development OOF Rank IC 低于 {overview.binding_mbe_rank_ic.toFixed(4)}，当前页面不得声明轮动预测能力。
              </section>
            )}
          {overview.forward_confirmation !== "PASSED" && (
            <section className={styles.warning}>
              本结果尚未完成前瞻确认（{overview.forward_confirmation}），不得用于 Selection、Paper、QMT 或自动调仓。
            </section>
          )}
          <section className={styles.heatmap} aria-label="31 个申万一级板块轮动热力图">
            {sorted.map((row) => (
              <article key={row.sector_code} className={`${styles.card} ${styles[row.forecast_state || "unavailable"]}`}>
                <div><strong>{row.sector_name}</strong><code>{row.sector_code}</code></div>
                <span>{stateLabel(row)}</span>
                <b>{number(row.rotation_score, 5)}</b>
                {row.reason_code && <small>{row.reason_code}</small>}
              </article>
            ))}
          </section>
          <footer className={styles.lineage}>
            <span>model {overview.model_hash}</span>
            <span>input {overview.input_hash}</span>
            <span>mapping {overview.mapping_snapshot_hash}</span>
            <span>validation {overview.validation_basis}</span>
          </footer>
        </>
      )}
    </main>
  );
}
