-- =============================================================================
-- QE Archive Top-K Rubric Views (2026-06-16)
-- =============================================================================
-- Purpose:
--   Re-point QE analytics from global IC/RankIC as the promotion gate to
--   CAGR/MDD/Calmar plus forward-only prediction-rank Top-K diagnostics.
--
-- Notes:
--   - Additive only: no table rewrites and no historical Top-K backfill.
--   - Top-K metrics are expected in qe_archive.run_metric after the backtest
--     extractor writes enhanced_metrics.prediction_diagnostics.topk_*.
--   - IC/RankIC/ICIR remain exposed as diagnostic columns.
-- =============================================================================

CREATE OR REPLACE VIEW qe_archive.v_topk_quality AS
SELECT
    r.run_id,
    r.task_id,
    r.loop_index,
    r.experiment_id,
    r.model_type,
    r.factor_set_hash,
    r.label_horizon,
    r.completed_at,
    MAX(m.value_num) FILTER (WHERE m.metric_key IN ('topk_return_20', 'topk_return@20', 'topk_return_at_20')) AS topk_return_20,
    MAX(m.value_num) FILTER (WHERE m.metric_key IN ('topk_return_50', 'topk_return@50', 'topk_return_at_50')) AS topk_return_50,
    MAX(m.value_num) FILTER (WHERE m.metric_key IN ('topk_hit_rate_20', 'topk_hit_rate@20', 'topk_hit_rate_at_20')) AS topk_hit_rate_20,
    MAX(m.value_num) FILTER (WHERE m.metric_key IN ('topk_hit_rate_50', 'topk_hit_rate@50', 'topk_hit_rate_at_50')) AS topk_hit_rate_50,
    MAX(m.value_num) FILTER (WHERE m.metric_key = 'topk_decay') AS topk_decay,
    MAX(m.value_num) FILTER (WHERE m.metric_key = 'within_portfolio_rankic') AS within_portfolio_rankic,
    MAX(m.value_num) FILTER (WHERE m.metric_key IN ('topk_dispersion_20', 'topk_dispersion@20')) AS topk_dispersion_20,
    MAX(m.value_num) FILTER (WHERE m.metric_key IN ('topk_dispersion_50', 'topk_dispersion@50')) AS topk_dispersion_50,
    MAX(m.value_num) FILTER (WHERE m.metric_key = 'topk_date_count') AS topk_date_count,
    MAX(m.value_num) FILTER (WHERE m.metric_key = 'topk_joined_observation_count') AS topk_joined_observation_count,
    MAX(m.value_num) FILTER (WHERE m.metric_key = 'topk_pred_observation_count') AS topk_pred_observation_count,
    MAX(m.value_num) FILTER (WHERE m.metric_key = 'topk_label_observation_count') AS topk_label_observation_count,
    MAX(m.value_num) FILTER (WHERE m.metric_key = 'topk_rankic_date_count') AS topk_rankic_date_count,
    MAX(m.value_num) FILTER (WHERE m.metric_key = 'topk_observation_count_20') AS topk_observation_count_20,
    MAX(m.value_num) FILTER (WHERE m.metric_key = 'topk_observation_count_50') AS topk_observation_count_50,
    MAX(m.value_text) FILTER (WHERE m.metric_key = 'topk_quality_status') AS topk_quality_status,
    MAX(m.value_text) FILTER (WHERE m.metric_key = 'topk_source') AS topk_source,
    MAX(m.value_text) FILTER (WHERE m.metric_key = 'topk_error') AS topk_error,
    MAX(m.value_text) FILTER (WHERE m.metric_key = 'topk_label_source') AS topk_label_source,
    MAX(m.value_text) FILTER (WHERE m.metric_key = 'topk_rank_direction') AS topk_rank_direction,
    BOOL_OR(m.metric_key LIKE 'topk_%' OR m.metric_key = 'within_portfolio_rankic') AS has_topk_metrics,
    MAX(m.quality_flag) FILTER (WHERE m.metric_key LIKE 'topk_%' OR m.metric_key = 'within_portfolio_rankic') AS topk_metric_quality_flag
FROM qe_archive.run r
LEFT JOIN qe_archive.run_metric m
       ON m.run_id = r.run_id
      AND (
          m.metric_key LIKE 'topk_%'
          OR m.metric_key IN (
              'topk_return@20', 'topk_return@50',
              'topk_return_at_20', 'topk_return_at_50',
              'topk_hit_rate@20', 'topk_hit_rate@50',
              'topk_hit_rate_at_20', 'topk_hit_rate_at_50',
              'within_portfolio_rankic'
          )
      )
WHERE r.research_valid = TRUE
  AND COALESCE(r.is_latest_attempt, TRUE) = TRUE
  AND r.status IN ('completed', 'archived', 'partial_archived')
GROUP BY r.run_id, r.task_id, r.loop_index, r.experiment_id, r.model_type, r.factor_set_hash, r.label_horizon, r.completed_at;

COMMENT ON VIEW qe_archive.v_topk_quality IS
'Run-level forward-only prediction-rank Top-K quality metrics pivoted from run_metric.';

CREATE OR REPLACE VIEW qe_archive.v_run_leaderboard AS
WITH m AS (
    SELECT
        run_id,
        MAX(value_num) FILTER (WHERE metric_key IN ('ic', 'IC'))                  AS ic,
        MAX(value_num) FILTER (WHERE metric_key IN ('icir', 'ICIR'))             AS icir,
        MAX(value_num) FILTER (WHERE metric_key IN ('rank_ic', 'rankic', 'RankIC'))     AS rank_ic,
        MAX(value_num) FILTER (WHERE metric_key IN ('rank_icir', 'rankicir', 'RankICIR')) AS rank_icir,
        MAX(value_num) FILTER (WHERE metric_key IN ('information_ratio', 'ir', 'IR'))    AS information_ratio
    FROM qe_archive.run_metric
    GROUP BY run_id
)
SELECT
    r.run_id,
    r.task_id,
    r.loop_index,
    r.experiment_id,
    r.node_id,
    r.model_type,
    r.model_family,
    r.factor_set_hash,
    r.factor_count,
    r.label_horizon,
    r.freq,
    m.ic,
    m.icir,
    m.rank_ic,
    m.rank_icir,
    a.cagr,
    a.total_return,
    a.sharpe,
    m.information_ratio,
    a.max_drawdown,
    CASE WHEN a.max_drawdown IS NOT NULL AND a.max_drawdown <> 0
         THEN a.cagr / abs(a.max_drawdown) END AS calmar,
    a.annualized_volatility,
    a.avg_cash_ratio,
    a.n_trading_days,
    rm.random_seed,
    rm.reproducibility_level,
    rm.verification_status,
    r.score_total,
    r.completed_at,
    tq.topk_return_20,
    tq.topk_return_50,
    tq.topk_hit_rate_20,
    tq.topk_hit_rate_50,
    tq.topk_decay,
    tq.within_portfolio_rankic,
    tq.topk_dispersion_20,
    tq.topk_dispersion_50,
    tq.topk_quality_status,
    tq.topk_source,
    tq.topk_date_count,
    tq.topk_joined_observation_count
FROM qe_archive.run r
LEFT JOIN qe_archive.run_account_summary a       ON a.run_id = r.run_id
LEFT JOIN m                                       ON m.run_id = r.run_id
LEFT JOIN qe_archive.run_reproducibility_manifest rm ON rm.run_id = r.run_id
LEFT JOIN qe_archive.v_topk_quality tq            ON tq.run_id = r.run_id
WHERE r.research_valid = TRUE
  AND COALESCE(r.is_latest_attempt, TRUE) = TRUE
  AND r.status IN ('completed', 'archived', 'partial_archived');

COMMENT ON VIEW qe_archive.v_run_leaderboard IS
'Run-level CAGR/MDD/Calmar leaderboard with IC diagnostics and forward-only Top-K quality metrics.';

CREATE OR REPLACE VIEW qe_archive.v_seed_robustness AS
WITH cfg AS (
    SELECT
        lb.*,
        rc.runtime_flags->>'undertrain_mode'                                  AS undertrain_mode,
        COALESCE(rc.strategy_config->>'topk', rc.runtime_flags->>'topk')      AS topk
    FROM qe_archive.v_run_leaderboard lb
    LEFT JOIN qe_archive.run_config rc ON rc.run_id = lb.run_id
)
SELECT
    factor_set_hash,
    model_type,
    label_horizon,
    COALESCE(undertrain_mode, 'normal')  AS undertrain_mode,
    topk,
    count(*)                              AS run_count,
    count(DISTINCT random_seed)           AS distinct_seed_count,
    array_agg(DISTINCT random_seed)       AS random_seeds,
    avg(cagr)                             AS cagr_mean,
    stddev_samp(cagr)                     AS cagr_std,
    CASE WHEN avg(cagr) IS NOT NULL AND avg(cagr) <> 0
         THEN stddev_samp(cagr) / abs(avg(cagr)) END AS cagr_cv,
    min(cagr)                             AS cagr_worst,
    max(cagr)                             AS cagr_best,
    avg(sharpe)                           AS sharpe_mean,
    avg(information_ratio)                AS ir_mean,
    min(information_ratio)                AS ir_worst,
    avg(max_drawdown)                     AS max_drawdown_mean,
    avg(icir)                             AS icir_mean,
    stddev_samp(icir)                     AS icir_std,
    avg(rank_icir)                        AS rank_icir_mean,
    (CASE WHEN avg(cagr) IS NOT NULL AND avg(cagr) <> 0
          AND stddev_samp(cagr) / abs(avg(cagr)) < 0.25 THEN TRUE ELSE FALSE END) AS is_return_stable,
    max(completed_at)                     AS latest_completed_at,
    CASE WHEN avg(max_drawdown) IS NOT NULL AND avg(max_drawdown) <> 0
         THEN avg(cagr) / abs(avg(max_drawdown)) END AS calmar,
    CASE WHEN avg(max_drawdown) IS NOT NULL AND avg(max_drawdown) <> 0
         THEN avg(cagr) / abs(avg(max_drawdown)) END AS calmar_mean,
    avg(topk_return_20)                   AS topk_return_20_mean,
    stddev_samp(topk_return_20)           AS topk_return_20_std,
    CASE WHEN avg(topk_return_20) IS NOT NULL AND avg(topk_return_20) <> 0
         THEN stddev_samp(topk_return_20) / abs(avg(topk_return_20)) END AS topk_return_20_cv,
    count(topk_return_20)                 AS topk_return_20_sample_count,
    avg(topk_return_50)                   AS topk_return_50_mean,
    avg(topk_hit_rate_20)                 AS topk_hit_rate_20_mean,
    avg(topk_hit_rate_50)                 AS topk_hit_rate_50_mean,
    avg(topk_decay)                       AS topk_decay_mean,
    avg(within_portfolio_rankic)          AS within_portfolio_rankic_mean,
    avg(topk_dispersion_20)               AS topk_dispersion_20_mean,
    avg(topk_dispersion_50)               AS topk_dispersion_50_mean,
    count(*) FILTER (WHERE topk_quality_status IS NOT NULL) AS topk_metric_run_count,
    count(*) FILTER (WHERE topk_quality_status = 'ok')      AS topk_ok_run_count
FROM cfg
GROUP BY factor_set_hash, model_type, label_horizon, COALESCE(undertrain_mode, 'normal'), topk;

COMMENT ON VIEW qe_archive.v_seed_robustness IS
'Seed robustness by config fingerprint: return/risk stability plus nullable forward-only Top-K aggregates.';

CREATE OR REPLACE VIEW qe_archive.v_overfit_flags AS
WITH cfg AS (
    SELECT
        lb.*,
        COALESCE(rc.runtime_flags->>'undertrain_mode', 'normal') AS undertrain_mode,
        COALESCE(rc.strategy_config->>'topk', rc.runtime_flags->>'topk') AS topk
    FROM qe_archive.v_run_leaderboard lb
    LEFT JOIN qe_archive.run_config rc ON rc.run_id = lb.run_id
)
SELECT
    lb.run_id,
    lb.task_id,
    lb.loop_index,
    lb.model_type,
    lb.label_horizon,
    lb.random_seed,
    lb.cagr,
    lb.information_ratio,
    lb.icir,
    mc.training_failed,
    mc.convergence_ratio,
    mc.overfit_ratio,
    -- IC/ICIR are diagnostic only under the Top-K rubric; this flag is no
    -- longer part of is_suspicious or promotion gating.
    (lb.information_ratio >= 2.0 AND COALESCE(lb.icir, 0) < 0.5) AS flag_return_without_signal,
    (COALESCE(mc.training_failed, FALSE) = TRUE AND lb.cagr >= 0.5) AS flag_undertrained_highret,
    (sr.cagr_mean IS NOT NULL AND sr.cagr_std IS NOT NULL
       AND lb.cagr > sr.cagr_mean + 2 * sr.cagr_std) AS flag_seed_outlier,
    ((COALESCE(mc.training_failed, FALSE) = TRUE AND lb.cagr >= 0.5)
       OR (sr.cagr_mean IS NOT NULL AND sr.cagr_std IS NOT NULL
           AND lb.cagr > sr.cagr_mean + 2 * sr.cagr_std)) AS is_suspicious
FROM cfg lb
LEFT JOIN qe_archive.run r        ON r.run_id = lb.run_id
LEFT JOIN aistock_model_catalog mc ON mc.id = r.model_catalog_id
LEFT JOIN qe_archive.v_seed_robustness sr
       ON sr.factor_set_hash = lb.factor_set_hash
      AND sr.model_type      = lb.model_type
      AND sr.label_horizon   = lb.label_horizon
      AND sr.undertrain_mode = lb.undertrain_mode
      AND sr.topk IS NOT DISTINCT FROM lb.topk;

COMMENT ON VIEW qe_archive.v_overfit_flags IS
'Overfit and seed-outlier flags: IC diagnostics are exposed but not used in the Top-K promotion gate.';

CREATE OR REPLACE VIEW qe_archive.v_promotion_candidates AS
SELECT
    sr.factor_set_hash,
    sr.model_type,
    sr.label_horizon,
    sr.undertrain_mode,
    sr.topk,
    sr.run_count,
    sr.distinct_seed_count,
    sr.random_seeds,
    sr.cagr_mean,
    sr.cagr_std,
    sr.cagr_cv,
    sr.cagr_worst,
    sr.cagr_best,
    sr.sharpe_mean,
    sr.ir_mean,
    sr.ir_worst,
    sr.max_drawdown_mean,
    sr.icir_mean,
    sr.icir_std,
    sr.rank_icir_mean,
    sr.is_return_stable,
    sr.latest_completed_at,
    (
        sr.cagr_mean >= 0.60
        AND sr.max_drawdown_mean >= -0.20
        AND sr.cagr_cv < 0.15
    ) AS passes_gate,
    sr.calmar_mean,
    sr.calmar_mean AS calmar,
    (sr.cagr_mean >= 0.60) AS cagr_gate_passes,
    (sr.max_drawdown_mean >= -0.20) AS max_drawdown_gate_passes,
    (sr.cagr_cv < 0.15) AS cagr_cv_gate_passes,
    TRUE AS overfit_gate_passes,
    0.60::DOUBLE PRECISION AS cagr_gate_threshold,
    (-0.20)::DOUBLE PRECISION AS max_drawdown_gate_threshold,
    0.15::DOUBLE PRECISION AS cagr_cv_gate_threshold,
    sr.topk_return_20_mean,
    sr.topk_return_20_std,
    sr.topk_return_20_cv,
    sr.topk_return_20_sample_count,
    sr.topk_return_50_mean,
    sr.topk_hit_rate_20_mean,
    sr.topk_hit_rate_50_mean,
    sr.topk_decay_mean,
    sr.within_portfolio_rankic_mean,
    sr.topk_dispersion_20_mean,
    sr.topk_dispersion_50_mean,
    sr.topk_metric_run_count,
    sr.topk_ok_run_count,
    (sr.topk_return_20_mean IS NOT NULL) AS topk_return_20_present,
    -- TODO(strategy session): calibrate topk_return_20 threshold and promote
    -- this soft gate into the hard promotion gate after enough new runs exist.
    CASE
        WHEN sr.topk_return_20_mean IS NULL THEN 'missing_forward_only'
        ELSE 'present_pending_calibration'
    END AS topk_soft_gate_status
FROM qe_archive.v_seed_robustness sr
WHERE sr.distinct_seed_count >= 5
  AND NOT EXISTS (
      SELECT 1
      FROM qe_archive.v_overfit_flags f
      JOIN qe_archive.v_run_leaderboard lb2 ON lb2.run_id = f.run_id
      LEFT JOIN qe_archive.run_config rc2 ON rc2.run_id = lb2.run_id
      WHERE lb2.factor_set_hash = sr.factor_set_hash
        AND lb2.model_type      = sr.model_type
        AND lb2.label_horizon   = sr.label_horizon
        AND COALESCE(rc2.runtime_flags->>'undertrain_mode', 'normal') = sr.undertrain_mode
        AND COALESCE(rc2.strategy_config->>'topk', rc2.runtime_flags->>'topk') IS NOT DISTINCT FROM sr.topk
        AND f.is_suspicious = TRUE
  );

COMMENT ON VIEW qe_archive.v_promotion_candidates IS
'Promotion candidate configurations: hard CAGR/MDD/CAGR-CV gate, nullable Top-K soft gate status, and no suspicious runs.';

-- =============================================================================
-- END. This migration intentionally does not create a historical Top-K backfill.
-- =============================================================================
