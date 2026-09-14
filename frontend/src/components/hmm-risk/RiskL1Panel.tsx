"use client";

import { useEffect, useMemo, useState } from "react";
import {
  getRiskL1,
  getRiskL1Overview,
  HMMRiskApiError,
  type RiskL1Overview,
  type RiskL1Row,
} from "@/lib/hmm-risk/api";
import styles from "./rotation-l1.module.css";

function formatted(value: number | null, digits = 4): string {
  return value === null ? "—" : value.toFixed(digits);
}

export default function RiskL1Panel() {
  const [overview, setOverview] = useState<RiskL1Overview | null>(null);
  const [rows, setRows] = useState<RiskL1Row[]>([]);
  const [error, setError] = useState<{ message: string; reason: string } | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let active = true;
    async function load() {
      try {
        const next = await getRiskL1Overview();
        const detail = await getRiskL1(next.trade_date, next.model_hash);
        if (!active) return;
        if (detail.rows.length !== 31) {
          throw new HMMRiskApiError(
            "L1 风险分母不是 31，拒绝展示不完整结果。",
            "hmm_risk_risk_l1_ui_denominator_invalid",
            500,
          );
        }
        setOverview(next);
        setRows(detail.rows);
      } catch (caught) {
        if (!active) return;
        const apiError = caught instanceof HMMRiskApiError ? caught : null;
        setError({
          message: caught instanceof Error ? caught.message : "HMM L1 风险数据读取失败",
          reason: apiError?.reasonCode || "hmm_risk_risk_l1_client_request_failed",
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
    () => [...rows].sort((left, right) => (right.risk_score ?? -Infinity) - (left.risk_score ?? -Infinity)),
    [rows],
  );

  return (
    <section aria-label="申万一级板块风险预警">
      <h2>L1 风险预警</h2>
      <p className={styles.subtitle}>
        风险分数仅用于研究排序，不是校准概率；未完成前瞻确认，不得接入 Selection、Paper、QMT 或自动调仓。
      </p>
      {loading && <div className={styles.notice}>正在读取真实 Risk_L1 prediction repository…</div>}
      {error && (
        <div className={styles.error} role="alert">
          <strong>风险预警暂不可用</strong><span>{error.message}</span><code>{error.reason}</code>
        </div>
      )}
      {overview && !error && (
        <>
          <div className={styles.statusGrid}>
            <article><span>研究表面</span><strong>{overview.risk_l1_research_surface_status}</strong></article>
            <article><span>风险能力</span><strong>{overview.risk_l1_capability_status}</strong></article>
            <article><span>前瞻确认</span><strong>{overview.forward_confirmation}</strong></article>
            <article><span>高风险预警</span><strong>{overview.high_warning_count}</strong></article>
          </div>
          <div className={styles.evidence}>
            <div><span>预测日 / as-of</span><strong>{overview.trade_date} / {overview.as_of_date}</strong></div>
            <div><span>真实覆盖</span><strong>{overview.available_count} / {overview.sector_count}</strong></div>
            <div><span>Development precision lift</span><strong>{formatted(overview.development_precision_lift)}</strong></div>
            <div><span>Development recall</span><strong>{formatted(overview.development_recall)}</strong></div>
          </div>
          <section className={styles.heatmap} aria-label="31 个申万一级板块风险热力图">
            {sorted.map((row) => (
              <article key={row.sector_code} className={`${styles.card} ${styles[row.risk_level || "unavailable"]}`}>
                <div><strong>{row.sector_name}</strong><code>{row.sector_code}</code></div>
                <span>{row.availability === "unavailable" ? "不可用" : row.risk_level}</span>
                <b>{formatted(row.risk_score, 5)}</b>
                {row.predicted_warning && <small>HIGH RISK WARNING</small>}
                {row.reason_code && <small>{row.reason_code}</small>}
              </article>
            ))}
          </section>
          <div className={styles.lineage}>
            <span>model {overview.model_hash}</span><span>input {overview.input_hash}</span>
            <span>mapping {overview.mapping_snapshot_hash}</span><span>validation {overview.validation_basis}</span>
          </div>
        </>
      )}
    </section>
  );
}
