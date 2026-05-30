-- =============================================================================
-- QE Archive Analytics Views v1 (2026-05-29, applied 2026-05-30)
-- =============================================================================
-- Design docs:
--   docs/methodology/qe/QE_DataWarehouse_Analytics_Design_v1_20260529.md
--   docs/methodology/qe/QE_Evolution_Methodology_v1_20260529.md
--
-- Purpose:
--   Upgrade qe_archive from historical experiment records into a queryable
--   analytics layer for run ranking, seed robustness, factor stability,
--   factor performance, model hyperparameter/seed analysis, overfit flags,
--   promotion candidates, and evolution lineage.
--
-- Production DDL gate:
--   Authorized by the user and applied to production on 2026-05-30 for BUG-170.
--   The DDL is read-only CREATE OR REPLACE VIEW and does not mutate data rows.
--
-- Apply precheck summary:
--   - Production metric keys include ic, icir, rank_ic, rank_icir,
--     information_ratio, cagr, sharpe and max_drawdown.
--   - qe_archive.metric_taxonomy may be empty; run_metric key counts are the
--     effective source of truth for this migration.
--   - run_config.runtime_flags.topk exists in production samples; strategy_config
--     topk can be empty, so views coalesce both paths.
--   - run_config.model_params is populated for current model/hyperparameter rows.
--
-- Payload contract:
--   These views intentionally expose compact, summary-first fields. API/MCP
--   consumers must keep default limits small and avoid returning full configs,
--   raw payloads, matrices, logs, model weights, or unbounded row sets.
-- =============================================================================

CREATE OR REPLACE VIEW qe_archive.v_run_leaderboard AS
WITH m AS (
    -- Pivot key run_metric signal metrics.
    -- Validated against production run_metric keys on 2026-05-30.
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
    -- Signal axis.
    m.ic,
    m.icir,
    m.rank_ic,
    m.rank_icir,
    -- Return / portfolio axis.
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
    -- Reproducibility.
    rm.random_seed,
    rm.reproducibility_level,
    rm.verification_status,
    r.score_total,
    r.completed_at
FROM qe_archive.run r
LEFT JOIN qe_archive.run_account_summary a       ON a.run_id = r.run_id
LEFT JOIN m                                       ON m.run_id = r.run_id
LEFT JOIN qe_archive.run_reproducibility_manifest rm ON rm.run_id = r.run_id
WHERE r.research_valid = TRUE
  AND COALESCE(r.is_latest_attempt, TRUE) = TRUE
  AND r.status IN ('completed', 'archived', 'partial_archived');

COMMENT ON VIEW qe_archive.v_run_leaderboard IS
'Run-level dual-axis leaderboard: signal metrics plus return/risk metrics, seed, and config fingerprint.';

-- -----------------------------------------------------------------------------
-- V2: v_seed_robustness - seed robustness.
-- Grain: factor_set_hash x model_type x label_horizon x undertrain_mode x topk.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW qe_archive.v_seed_robustness AS
WITH cfg AS (
    SELECT
        lb.*,
        -- Validated JSONB paths: runtime_flags.topk exists; strategy_config.topk may be empty.
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
    -- Return-axis stability.
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
    -- Signal-axis stability.
    avg(icir)                             AS icir_mean,
    stddev_samp(icir)                     AS icir_std,
    avg(rank_icir)                        AS rank_icir_mean,
    -- Stability rule: CV < 0.25 is considered return-stable.
    (CASE WHEN avg(cagr) IS NOT NULL AND avg(cagr) <> 0
          AND stddev_samp(cagr) / abs(avg(cagr)) < 0.25 THEN TRUE ELSE FALSE END) AS is_return_stable,
    max(completed_at)                     AS latest_completed_at
FROM cfg
GROUP BY factor_set_hash, model_type, label_horizon, COALESCE(undertrain_mode, 'normal'), topk;

COMMENT ON VIEW qe_archive.v_seed_robustness IS
'Seed robustness by config fingerprint: multi-seed mean, std, CV, worst/best and stability flags.';

-- -----------------------------------------------------------------------------
-- V3: v_factor_importance_stability - factor attribution stability.
-- Grain: factor_name x method.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW qe_archive.v_factor_importance_stability AS
SELECT
    fi.factor_name,
    fi.method,
    count(*)                                   AS run_count,
    count(DISTINCT rm.random_seed)             AS distinct_seed_count,
    array_agg(DISTINCT rm.random_seed)
        FILTER (WHERE rm.random_seed IS NOT NULL) AS random_seeds,
    avg(fi.normalized_value)                   AS avg_normalized_value,
    stddev_samp(fi.normalized_value)           AS std_normalized_value,
    min(fi.normalized_value)                   AS min_normalized_value,
    max(fi.normalized_value)                   AS max_normalized_value,
    avg(fi.weight_pct)                         AS avg_weight_pct,
    avg(fi.rank_in_run)                        AS avg_rank,
    min(fi.rank_in_run)                        AS best_rank,
    -- Instability rule: std_normalized_value > 0.35.
    (CASE WHEN stddev_samp(fi.normalized_value) > 0.35 THEN TRUE ELSE FALSE END) AS is_unstable,
    max(fi.normalized_value) - min(fi.normalized_value) AS importance_range,
    max(r.completed_at)                        AS latest_completed_at
FROM qe_archive.run_factor_importance fi
JOIN qe_archive.run r ON r.run_id = fi.run_id AND r.research_valid = TRUE
LEFT JOIN qe_archive.run_reproducibility_manifest rm ON rm.run_id = fi.run_id
GROUP BY fi.factor_name, fi.method;

COMMENT ON VIEW qe_archive.v_factor_importance_stability IS
'Factor importance stability across runs and seeds: ranks, normalized-value dispersion and unstable flag.';

-- -----------------------------------------------------------------------------
-- V4: v_factor_performance - factor performance footprint.
-- Grain: factor_name.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW qe_archive.v_factor_performance AS
SELECT
    rf.factor_name,
    bool_or(rf.is_alpha158)              AS is_alpha158,
    count(DISTINCT rf.run_id)            AS run_count,
    max(lb.cagr)                         AS best_cagr,
    avg(lb.cagr)                         AS avg_cagr,
    max(lb.sharpe)                       AS best_sharpe,
    avg(lb.sharpe)                       AS avg_sharpe,
    max(lb.icir)                         AS best_icir,
    avg(lb.icir)                         AS avg_icir,
    max(lb.completed_at)                 AS latest_used_at
FROM qe_archive.run_factor rf
JOIN qe_archive.v_run_leaderboard lb ON lb.run_id = rf.run_id
GROUP BY rf.factor_name;

COMMENT ON VIEW qe_archive.v_factor_performance IS
'Factor performance footprint: best/average return and signal metrics plus usage frequency.';

-- -----------------------------------------------------------------------------
-- V5: v_model_hyperparam_seed_perf - model hyperparameter by seed performance.
-- Grain: model_type x hyperparam_hash x seed.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW qe_archive.v_model_hyperparam_seed_perf AS
SELECT
    lb.model_type,
    lb.model_family,
    -- model_params is used as the compact hyperparameter fingerprint input.
    rc.model_params,
    md5(COALESCE(rc.model_params::text, ''))  AS hyperparam_hash,
    lb.label_horizon,
    lb.random_seed,
    mt.objective_name,
    mt.objective_value,
    lb.ic,
    lb.icir,
    lb.cagr,
    lb.sharpe,
    lb.information_ratio,
    lb.max_drawdown,
    lb.run_id,
    lb.task_id,
    lb.loop_index,
    lb.completed_at
FROM qe_archive.v_run_leaderboard lb
LEFT JOIN qe_archive.run_config rc       ON rc.run_id = lb.run_id
LEFT JOIN qe_archive.run_model_trial mt  ON mt.run_id = lb.run_id;

COMMENT ON VIEW qe_archive.v_model_hyperparam_seed_perf IS
'Model hyperparameter by seed performance: compact dual-axis metrics with hyperparam hash.';

-- -----------------------------------------------------------------------------
-- V6: v_overfit_flags - overfit and variance-tail red flags.
-- Grain: run_id.
-- -----------------------------------------------------------------------------
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
    -- Flag 1: strong return axis but weak signal axis.
    (lb.information_ratio >= 2.0 AND COALESCE(lb.icir, 0) < 0.5)      AS flag_return_without_signal,
    -- Flag 2: training failure with unusually high return.
    (COALESCE(mc.training_failed, FALSE) = TRUE AND lb.cagr >= 0.5)   AS flag_undertrained_highret,
    -- Flag 3: single seed exceeds same-config mean by more than two standard deviations.
    (sr.cagr_mean IS NOT NULL AND sr.cagr_std IS NOT NULL
       AND lb.cagr > sr.cagr_mean + 2 * sr.cagr_std)                  AS flag_seed_outlier,
    -- Combined suspicious flag.
    (( lb.information_ratio >= 2.0 AND COALESCE(lb.icir, 0) < 0.5)
       OR (COALESCE(mc.training_failed, FALSE) = TRUE AND lb.cagr >= 0.5)
       OR (sr.cagr_mean IS NOT NULL AND sr.cagr_std IS NOT NULL
           AND lb.cagr > sr.cagr_mean + 2 * sr.cagr_std))             AS is_suspicious
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
'Overfit and seed-outlier flags: return-without-signal, undertrained-high-return and seed outlier indicators.';

-- -----------------------------------------------------------------------------
-- V7: v_promotion_candidates - promotion candidate leaderboard.
-- Grain: config fingerprint.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW qe_archive.v_promotion_candidates AS
SELECT
    sr.*,
    -- Dual-axis gate: signal and return-risk thresholds plus stable returns.
    (sr.icir_mean >= 0.5 AND sr.ir_mean >= 1.5 AND sr.is_return_stable) AS passes_gate
FROM qe_archive.v_seed_robustness sr
WHERE sr.distinct_seed_count >= 5          -- Multi-seed requirement.
  AND sr.is_return_stable = TRUE
  AND NOT EXISTS (
      -- Exclude configurations with suspicious runs.
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
'Promotion candidate configurations: dual-axis gates, multi-seed stability, and no suspicious runs.';

-- -----------------------------------------------------------------------------
-- V8: v_evolution_lineage - evolution lineage.
-- Grain: task x loop x experiment x run.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW qe_archive.v_evolution_lineage AS
SELECT
    lb.task_id,
    lb.loop_index,
    lb.experiment_id,
    lb.run_id,
    lb.model_type,
    lb.label_horizon,
    lb.factor_count,
    lb.ic,
    lb.icir,
    lb.cagr,
    lb.sharpe,
    lb.information_ratio,
    lb.max_drawdown,
    lb.random_seed,
    lb.completed_at
FROM qe_archive.v_run_leaderboard lb
ORDER BY lb.task_id, lb.loop_index;

COMMENT ON VIEW qe_archive.v_evolution_lineage IS
'Evolution lineage: task, loop, experiment and run chain with compact dual-axis metrics.';

-- =============================================================================
-- END. After apply, verify every view with SELECT count(*).
-- =============================================================================
