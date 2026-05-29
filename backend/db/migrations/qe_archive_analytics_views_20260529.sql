-- =============================================================================
-- QE Archive Analytics Views v1  (2026-05-29)
-- =============================================================================
-- 配套设计文档: docs/methodology/qe/QE_DataWarehouse_Analytics_Design_v1_20260529.md
-- 方法论:       docs/methodology/qe/QE_Evolution_Methodology_v1_20260529.md
--
-- 目的: 把 qe_archive 从"实验历史记录"升级为可直接产出结论的数据仓库分析层。
--       8 个视图覆盖: 双轴运行榜 / seed鲁棒性 / 因子稳定性 / 因子表现 /
--       模型超参×seed性能 / 过拟合红旗 / 晋升候选 / 演进血缘。
--
-- 门禁: 本文件仅提交, 未在生产库执行 (production_ddl_pending)。
-- ⚠️ APPLY 前必校验:
--   (1) 下方 IC/ICIR/RankIC/RankICIR/IR 的 metric_key 字符串需对照
--       qe_archive.metric_taxonomy 确认 (见每处 -- VALIDATE 注释)。
--   (2) run_config.strategy_config / runtime_flags 的 JSONB 路径需确认。
--   (3) apply 后对每个视图执行 SELECT count(*) 抽验可查。
-- 全部为 CREATE OR REPLACE VIEW, 幂等可重复执行。
-- =============================================================================

-- -----------------------------------------------------------------------------
-- V1: v_run_leaderboard  —— 双轴运行榜 (一行看齐 信号轴 + 收益轴 + seed + 指纹)
-- 粒度: run_id (仅 research_valid 且 latest attempt)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW qe_archive.v_run_leaderboard AS
WITH m AS (
    -- 透视 run_metric 的关键标量指标。
    -- VALIDATE: 下列 metric_key 需对照 qe_archive.metric_taxonomy 确认真实命名。
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
    -- 信号轴
    m.ic,
    m.icir,
    m.rank_ic,
    m.rank_icir,
    -- 收益/组合轴
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
    -- 复现性
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
'双轴运行榜: 每个有效 run 的信号轴(IC/ICIR/RankIC/RankICIR)+收益轴(CAGR/Sharpe/IR/MaxDD/Calmar)+seed+配置指纹。方法论考核 Part 6.1。';

-- -----------------------------------------------------------------------------
-- V2: v_seed_robustness  —— seed 鲁棒性 (核心: 挤掉偶然冠军, 给诚实生产预期)
-- 粒度: 配置指纹 (factor_set_hash × model_type × label_horizon × undertrain × topk)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW qe_archive.v_seed_robustness AS
WITH cfg AS (
    SELECT
        lb.*,
        -- VALIDATE: JSONB 路径需确认 (run_config.runtime_flags / strategy_config)
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
    -- 收益轴稳定性
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
    -- 信号轴稳定性
    avg(icir)                             AS icir_mean,
    stddev_samp(icir)                     AS icir_std,
    avg(rank_icir)                        AS rank_icir_mean,
    -- 稳定性判据 (方法论原则 1: cv<0.25 收益稳定)
    (CASE WHEN avg(cagr) IS NOT NULL AND avg(cagr) <> 0
          AND stddev_samp(cagr) / abs(avg(cagr)) < 0.25 THEN TRUE ELSE FALSE END) AS is_return_stable,
    max(completed_at)                     AS latest_completed_at
FROM cfg
GROUP BY factor_set_hash, model_type, label_horizon, COALESCE(undertrain_mode, 'normal'), topk;

COMMENT ON VIEW qe_archive.v_seed_robustness IS
'seed 鲁棒性: 按配置指纹聚合多 seed 的 mean/std/cv/worst。把偶然冠军挤掉, 给出诚实生产预期。方法论 Part 6.2 + 原则 1/2。';

-- -----------------------------------------------------------------------------
-- V3: v_factor_importance_stability  —— 因子归因稳定性 (固化现有 MCP 聚合为 SQL)
-- 粒度: factor_name × method
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
    -- 不稳定标记 (方法论 Part 6.3 / 因子筛选 Step4: std_normalized>0.35 视为不稳定)
    (CASE WHEN stddev_samp(fi.normalized_value) > 0.35 THEN TRUE ELSE FALSE END) AS is_unstable,
    max(fi.normalized_value) - min(fi.normalized_value) AS importance_range,
    max(r.completed_at)                        AS latest_completed_at
FROM qe_archive.run_factor_importance fi
JOIN qe_archive.run r ON r.run_id = fi.run_id AND r.research_valid = TRUE
LEFT JOIN qe_archive.run_reproducibility_manifest rm ON rm.run_id = fi.run_id
GROUP BY fi.factor_name, fi.method;

COMMENT ON VIEW qe_archive.v_factor_importance_stability IS
'因子跨 seed 重要性稳定性: avg_rank/best_rank/std_normalized/distinct_seed_count。区分稳定贡献者 vs seed 噪声。方法论 Part 6.3 + 因子筛选 Step4。';

-- -----------------------------------------------------------------------------
-- V4: v_factor_performance  —— 因子表现足迹
-- 粒度: factor_name
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
'因子表现足迹: 某因子参与过的有效 run 的最佳/平均双轴 + 使用频次。因子筛选 Step2/3 取数。';

-- -----------------------------------------------------------------------------
-- V5: v_model_hyperparam_seed_perf  —— 模型超参×SEED 性能 (替代 500 端点 D2)
-- 粒度: model_type × 超参指纹 × seed
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW qe_archive.v_model_hyperparam_seed_perf AS
SELECT
    lb.model_type,
    lb.model_family,
    -- VALIDATE: model_params JSONB 形态需确认; 此处取整体作为超参指纹
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
'模型 超参×seed 性能: 每模型类型下各超参档位×seed 的双轴表现。回答"超参和SEED配置性能分析"; 替代故障的 query_model_trials 端点(D2)。';

-- -----------------------------------------------------------------------------
-- V6: v_overfit_flags  —— 过拟合 / 方差尾部红旗 (防 abbc/L16 陷阱)
-- 粒度: run_id
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW qe_archive.v_overfit_flags AS
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
    -- 红旗 1: 收益轴爆表但信号轴平庸 (IR 高 / ICIR 平)
    (lb.information_ratio >= 2.0 AND COALESCE(lb.icir, 0) < 0.5)      AS flag_return_without_signal,
    -- 红旗 2: 欠训练却高收益
    (COALESCE(mc.training_failed, FALSE) = TRUE AND lb.cagr >= 0.5)   AS flag_undertrained_highret,
    -- 红旗 3: 单 seed 远超同指纹集成均值 (> mean + 2*std)
    (sr.cagr_mean IS NOT NULL AND sr.cagr_std IS NOT NULL
       AND lb.cagr > sr.cagr_mean + 2 * sr.cagr_std)                  AS flag_seed_outlier,
    -- 综合 suspicious
    (( lb.information_ratio >= 2.0 AND COALESCE(lb.icir, 0) < 0.5)
       OR (COALESCE(mc.training_failed, FALSE) = TRUE AND lb.cagr >= 0.5)
       OR (sr.cagr_mean IS NOT NULL AND sr.cagr_std IS NOT NULL
           AND lb.cagr > sr.cagr_mean + 2 * sr.cagr_std))             AS is_suspicious
FROM qe_archive.v_run_leaderboard lb
LEFT JOIN qe_archive.run r        ON r.run_id = lb.run_id
LEFT JOIN aistock_model_catalog mc ON mc.id = r.model_catalog_id
LEFT JOIN qe_archive.v_seed_robustness sr
       ON sr.factor_set_hash = lb.factor_set_hash
      AND sr.model_type      = lb.model_type
      AND sr.label_horizon   = lb.label_horizon;

COMMENT ON VIEW qe_archive.v_overfit_flags IS
'过拟合/方差尾部红旗: 收益无信号 / 欠训练高收益 / 单seed离群 → suspicious。方法论 Part 6.4 自动哨兵。';

-- -----------------------------------------------------------------------------
-- V7: v_promotion_candidates  —— 晋升候选榜
-- 粒度: 配置指纹
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW qe_archive.v_promotion_candidates AS
SELECT
    sr.*,
    -- 双轴过线 (方法论 Part 7 探索层门: IC≥0.06 等价用 icir_mean 近似; IR≥1.5)
    (sr.icir_mean >= 0.5 AND sr.ir_mean >= 1.5 AND sr.is_return_stable) AS passes_gate
FROM qe_archive.v_seed_robustness sr
WHERE sr.distinct_seed_count >= 5          -- 必须经多 seed (Route C)
  AND sr.is_return_stable = TRUE
  AND NOT EXISTS (
      -- 排除含 suspicious run 的指纹
      SELECT 1
      FROM qe_archive.v_overfit_flags f
      JOIN qe_archive.v_run_leaderboard lb2 ON lb2.run_id = f.run_id
      WHERE lb2.factor_set_hash = sr.factor_set_hash
        AND lb2.model_type      = sr.model_type
        AND lb2.label_horizon   = sr.label_horizon
        AND f.is_suspicious = TRUE
  );

COMMENT ON VIEW qe_archive.v_promotion_candidates IS
'晋升候选: 双轴过线 + seed稳定(N>=5,cv<0.25) + 无 suspicious 红旗的配置。方法论 Part 7 验证层闸门。';

-- -----------------------------------------------------------------------------
-- V8: v_evolution_lineage  —— 演进血缘
-- 粒度: task × loop × experiment × run
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
'演进血缘: task→loop→experiment→run 链 + 每轮双轴变化。实验复盘/选基线(方法论 Part 8)。';

-- =============================================================================
-- END. apply 后建议: SELECT count(*) FROM qe_archive.v_run_leaderboard; (逐个抽验)
-- =============================================================================
