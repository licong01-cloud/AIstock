-- v2 分类字段覆盖度检查
-- 用法: psql ... -f scripts/_check_v2_coverage.sql
-- 跑完 batch-analyze 后执行此脚本验证每个 v2 字段是否真的写入了

\echo '=== 总体覆盖度 (已分析过的因子) ==='
SELECT
  COUNT(*)                                                           AS total_analyzed,
  -- Phase 0 字段 (老版本已实现)
  COUNT(*) FILTER (WHERE category IS NOT NULL)                       AS has_category,
  COUNT(*) FILTER (WHERE factor_dimension IS NOT NULL)               AS has_dimension,
  COUNT(*) FILTER (WHERE data_source_group IS NOT NULL)              AS has_ds_group,
  COUNT(*) FILTER (WHERE linearity IS NOT NULL)                      AS has_linearity,
  COUNT(*) FILTER (WHERE update_freq IS NOT NULL)                    AS has_update_freq,
  COUNT(*) FILTER (WHERE holding_period_class IS NOT NULL)           AS has_hp_class,
  -- Phase 1 新实现字段 (ts_info_density + cross_horizon_consistency)
  COUNT(*) FILTER (WHERE ts_info_density IS NOT NULL)                AS has_ts_density,
  COUNT(*) FILTER (WHERE cross_horizon_consistency IS NOT NULL)      AS has_cross_horizon,
  -- 依赖 LLM v2 的字段
  COUNT(*) FILTER (WHERE direction IS NOT NULL)                      AS has_direction,
  COUNT(*) FILTER (WHERE signal_mechanism IS NOT NULL)               AS has_signal_mech,
  COUNT(*) FILTER (WHERE sector_exposure_corr IS NOT NULL)           AS has_sector_corr,
  -- IC 时序派生字段
  COUNT(*) FILTER (WHERE ic_sign_consistency_12m IS NOT NULL)        AS has_ic_sign_12m,
  COUNT(*) FILTER (WHERE ic_oos_is_ratio IS NOT NULL)                AS has_ic_oos_ratio,
  COUNT(*) FILTER (WHERE monthly_ic_trend_slope IS NOT NULL)         AS has_monthly_slope,
  -- Best horizon 字段
  COUNT(*) FILTER (WHERE best_horizon IS NOT NULL)                   AS has_best_horizon,
  COUNT(*) FILTER (WHERE best_horizon_advantage IS NOT NULL)         AS has_best_hz_adv,
  COUNT(*) FILTER (WHERE horizon_class IS NOT NULL)                  AS has_horizon_class,
  -- Phase 2 字段 (仍未实现, 预期全部为 0)
  COUNT(*) FILTER (WHERE cluster_id IS NOT NULL)                     AS has_cluster_id,
  COUNT(*) FILTER (WHERE cluster_role IS NOT NULL)                   AS has_cluster_role,
  COUNT(*) FILTER (WHERE cluster_size IS NOT NULL)                   AS has_cluster_size,
  COUNT(*) FILTER (WHERE intra_cluster_max_corr IS NOT NULL)         AS has_intra_corr,
  COUNT(*) FILTER (WHERE representative_score IS NOT NULL)           AS has_rep_score
FROM qe_factor_classification
WHERE analyzed_at IS NOT NULL;

\echo ''
\echo '=== 最近 10 个分析因子的 v2 字段明细 ==='
SELECT
  factor_name,
  ts_info_density,
  cross_horizon_consistency,
  direction,
  signal_mechanism,
  best_horizon,
  horizon_class,
  cluster_id,
  analyzed_at
FROM qe_factor_classification
WHERE analyzed_at IS NOT NULL
ORDER BY analyzed_at DESC NULLS LAST
LIMIT 10;

\echo ''
\echo '=== 预期值参考 ==='
\echo 'Phase 1 修复后 (重启 + 重新 batch-analyze):'
\echo '  has_ts_density / has_cross_horizon 应接近 has_category (100% 覆盖)'
\echo '  ts_density 依赖 parquet 缓存: 无缓存的因子 ts_density=NULL 属正常'
\echo 'Phase 2 未实现: has_cluster_* / has_rep_score 仍为 0 (需修相关性 dispatch bug)'
