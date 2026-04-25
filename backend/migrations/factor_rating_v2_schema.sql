-- ============================================================================
-- Factor Rating v2.0 Schema Migration (T2 + T3 + T4 + T5)
-- Date: 2026-04-22
-- Scope:
--   T2: aistock_factor_catalog          -> +5 columns (rehab / disable metadata)
--   T3: qe_factor_classification        -> +14 columns (direction / horizon / mechanism / cluster)
--   T4: aistock_factor_metrics          -> +3 columns (direction / best_horizon / advantage)
--   T5: aistock_factor_monthly_ic       -> +3 columns (sign_consistency / trend_slope / oos_is_ratio)
-- Safety:
--   - 全部 ADD COLUMN IF NOT EXISTS, nullable/默认NULL, 幂等
--   - 不改主键/不改约束/不触发回填
-- Post-migration backfill (单独执行, 见文件末尾):
--   UPDATE aistock_factor_metrics SET direction = SIGN(rank_ic_mean)
--   WHERE direction IS NULL AND rank_ic_mean IS NOT NULL;
-- ============================================================================

BEGIN;

-- ─────────────────────────────────────────────────────────────────────────────
-- T2: aistock_factor_catalog — Rehab 元数据 + 禁用原因追溯
-- ─────────────────────────────────────────────────────────────────────────────

ALTER TABLE aistock_factor_catalog
    ADD COLUMN IF NOT EXISTS disable_reason    TEXT,
    ADD COLUMN IF NOT EXISTS disable_batch_id  TEXT,
    ADD COLUMN IF NOT EXISTS disable_at        TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS rehab_candidate   BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS last_rehab_at     TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_catalog_disable_batch
    ON aistock_factor_catalog(disable_batch_id)
    WHERE disable_batch_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_catalog_rehab_candidate
    ON aistock_factor_catalog(rehab_candidate)
    WHERE rehab_candidate = TRUE;

COMMENT ON COLUMN aistock_factor_catalog.disable_reason IS
    '禁用原因: pure_noise / data_source_deprecated / dedup_duplicate / rating_bug_v1 / manual 等';
COMMENT ON COLUMN aistock_factor_catalog.disable_batch_id IS
    'Rehab/禁用批次 ID, 用于批量回滚';
COMMENT ON COLUMN aistock_factor_catalog.disable_at IS
    '禁用时间戳';
COMMENT ON COLUMN aistock_factor_catalog.rehab_candidate IS
    '由 Phase 3 聚类代表选拔后自动设置: 禁用但在簇内被选为代表 -> TRUE -> 触发 rehab';
COMMENT ON COLUMN aistock_factor_catalog.last_rehab_at IS
    '最后一次 rehab 时间戳';

-- ─────────────────────────────────────────────────────────────────────────────
-- T3: qe_factor_classification — 方向 / Horizon / 机制 / 聚类元数据
-- ─────────────────────────────────────────────────────────────────────────────

ALTER TABLE qe_factor_classification
    -- 方向
    ADD COLUMN IF NOT EXISTS direction                 SMALLINT,
    -- Horizon argmax 产出
    ADD COLUMN IF NOT EXISTS best_horizon              SMALLINT,
    ADD COLUMN IF NOT EXISTS best_horizon_advantage    DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS horizon_class             TEXT,
    -- LLM 产出
    ADD COLUMN IF NOT EXISTS signal_mechanism          TEXT,
    -- 一阶派生指标 (最新快照, 时间序列在 aistock_factor_monthly_ic)
    ADD COLUMN IF NOT EXISTS sector_exposure_corr      DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS ic_sign_consistency_12m   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS ic_oos_is_ratio           DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS monthly_ic_trend_slope    DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS cross_horizon_consistency DOUBLE PRECISION,
    -- 聚类产出 (T10 写入)
    ADD COLUMN IF NOT EXISTS cluster_id                BIGINT,
    ADD COLUMN IF NOT EXISTS cluster_role              TEXT,
    ADD COLUMN IF NOT EXISTS cluster_size              SMALLINT,
    ADD COLUMN IF NOT EXISTS intra_cluster_max_corr    DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS representative_score      DOUBLE PRECISION;

COMMENT ON COLUMN qe_factor_classification.direction IS
    '因子方向: +1 正向(高分预期高收益) / -1 反向 / 0 中性 / NULL 未判定。评级引擎 v2.0 依赖此字段';
COMMENT ON COLUMN qe_factor_classification.best_horizon IS
    'Horizon argmax: 取 abs(rank_ic_{1,5,10,20d}) 最大的 horizon (1/5/10/20)';
COMMENT ON COLUMN qe_factor_classification.best_horizon_advantage IS
    'max_abs(rank_ic_horizons) - second_abs(rank_ic_horizons) — horizon 领先幅度';
COMMENT ON COLUMN qe_factor_classification.horizon_class IS
    '持仓周期: short (best_horizon<=5) / medium (5<best_horizon<=10) / long (best_horizon>10)';
COMMENT ON COLUMN qe_factor_classification.signal_mechanism IS
    '信号机制 7 类: reversal / momentum / crowding / liquidity_premium / value_premium / quality / microstructure';
COMMENT ON COLUMN qe_factor_classification.sector_exposure_corr IS
    '与申万一级行业平均收益的 |corr|, <0.5 视为低行业暴露';
COMMENT ON COLUMN qe_factor_classification.ic_sign_consistency_12m IS
    '近 12 月月度 IC 方向一致性 (最新快照, 取自 aistock_factor_monthly_ic.sign_consistency_12m)';
COMMENT ON COLUMN qe_factor_classification.ic_oos_is_ratio IS
    '样本外/样本内 IC 比值 (最新快照, 取自 aistock_factor_monthly_ic.oos_is_ratio)';
COMMENT ON COLUMN qe_factor_classification.monthly_ic_trend_slope IS
    '近 12 月 IC Theil-Sen slope (最新快照)';
COMMENT ON COLUMN qe_factor_classification.cross_horizon_consistency IS
    '跨 horizon 方向一致性 (1d/5d/10d/20d IC 符号比例)';
COMMENT ON COLUMN qe_factor_classification.cluster_role IS
    'representative = 簇内得分最高(multi_alpha_fitness +4) / synthesized / anchor / member / isolated';

CREATE INDEX IF NOT EXISTS idx_classification_cluster_id
    ON qe_factor_classification(cluster_id)
    WHERE cluster_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_classification_signal_mechanism
    ON qe_factor_classification(signal_mechanism)
    WHERE signal_mechanism IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_classification_horizon_class
    ON qe_factor_classification(horizon_class)
    WHERE horizon_class IS NOT NULL;

-- ─────────────────────────────────────────────────────────────────────────────
-- T4: aistock_factor_metrics — 方向 + Horizon argmax 产出
-- ─────────────────────────────────────────────────────────────────────────────
-- 月度聚合字段 (sign_consistency / oos_is_ratio / trend_slope) 不放在 metrics 表,
-- 统一存入 aistock_factor_monthly_ic (T5)

ALTER TABLE aistock_factor_metrics
    ADD COLUMN IF NOT EXISTS direction                 SMALLINT,
    ADD COLUMN IF NOT EXISTS best_horizon              SMALLINT,
    ADD COLUMN IF NOT EXISTS best_horizon_advantage    DOUBLE PRECISION;

COMMENT ON COLUMN aistock_factor_metrics.direction IS
    '因子方向, 由 sign(rank_ic_mean) 决定(或人工覆盖)。评级引擎 v2.0 读此字段';
COMMENT ON COLUMN aistock_factor_metrics.best_horizon IS
    'argmax over rank_ic_{1,5,10,20d}: 取 abs 值最大的那个 horizon';
COMMENT ON COLUMN aistock_factor_metrics.best_horizon_advantage IS
    'max_abs(rank_ic_horizons) - second_abs(rank_ic_horizons) — horizon 领先幅度';

-- ─────────────────────────────────────────────────────────────────────────────
-- T5: aistock_factor_monthly_ic — 月度一阶聚合 (Q2.4 定案承载表)
-- ─────────────────────────────────────────────────────────────────────────────

ALTER TABLE aistock_factor_monthly_ic
    ADD COLUMN IF NOT EXISTS sign_consistency_12m    DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS trend_slope_12m         DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS oos_is_ratio            DOUBLE PRECISION;

COMMENT ON COLUMN aistock_factor_monthly_ic.sign_consistency_12m IS
    '近 12 个 month_end 的 ic_mean 符号一致性 = mean(sign(ic_mean[-12:]) == sign(mean(ic_mean[-12:])))';
COMMENT ON COLUMN aistock_factor_monthly_ic.trend_slope_12m IS
    '近 12 月 ic_mean 的 Theil-Sen slope; 正=变强, 负=衰退';
COMMENT ON COLUMN aistock_factor_monthly_ic.oos_is_ratio IS
    '近 6 月 mean(ic_mean) / 前 6 月 mean(ic_mean) — 样本外/样本内 IC 稳定性比';

COMMIT;

-- ============================================================================
-- POST-MIGRATION BACKFILL (单独执行, 不放在 transaction 内)
-- ============================================================================
-- direction 字段回填: 所有 rank_ic_mean 非空的行, 按符号填充
-- 安全性: 仅改 direction IS NULL 的行, 不覆盖已有值
--
-- UPDATE aistock_factor_metrics
-- SET direction = CASE
--     WHEN rank_ic_mean > 0 THEN 1::SMALLINT
--     WHEN rank_ic_mean < 0 THEN -1::SMALLINT
--     ELSE 0::SMALLINT
-- END
-- WHERE direction IS NULL AND rank_ic_mean IS NOT NULL;
--
-- 验证:
-- SELECT
--     COUNT(*) FILTER (WHERE direction = 1)  AS positive_cnt,
--     COUNT(*) FILTER (WHERE direction = -1) AS negative_cnt,
--     COUNT(*) FILTER (WHERE direction = 0)  AS neutral_cnt,
--     COUNT(*) FILTER (WHERE direction IS NULL) AS null_cnt
-- FROM aistock_factor_metrics;
