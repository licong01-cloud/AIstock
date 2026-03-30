-- ============================================================
-- A1: 已入库 Loop 指标映射 SQL 补齐
-- 将 QLib 长键名映射为前端期望的短键名
-- 执行方式: psql -f backfill_loop_metrics_mapping.sql
-- ============================================================

BEGIN;

-- 先查看受影响行数（预检）
SELECT COUNT(*) AS affected_rows
FROM qe_evolution_loops
WHERE status = 'completed'
  AND metrics_json IS NOT NULL
  AND (
    (metrics_json ? '1day.excess_return_with_cost.information_ratio' AND NOT metrics_json ? 'sharpe')
    OR (metrics_json ? '1day.excess_return_without_cost.annualized_return' AND NOT metrics_json ? 'annualized_return')
    OR (metrics_json ? 'Rank IC' AND NOT metrics_json ? 'Rank_IC')
  );

-- 执行映射补写
UPDATE qe_evolution_loops
SET metrics_json = metrics_json
    || CASE WHEN metrics_json ? 'Rank IC' AND NOT metrics_json ? 'Rank_IC'
            THEN jsonb_build_object('Rank_IC', metrics_json->'Rank IC') ELSE '{}'::jsonb END
    || CASE WHEN metrics_json ? '1day.excess_return_with_cost.information_ratio' AND NOT metrics_json ? 'sharpe'
            THEN jsonb_build_object('sharpe', metrics_json->'1day.excess_return_with_cost.information_ratio') ELSE '{}'::jsonb END
    || CASE WHEN metrics_json ? '1day.excess_return_without_cost.annualized_return' AND NOT metrics_json ? 'annualized_return'
            THEN jsonb_build_object('annualized_return', metrics_json->'1day.excess_return_without_cost.annualized_return') ELSE '{}'::jsonb END
    || CASE WHEN metrics_json ? '1day.excess_return_with_cost.annualized_return' AND NOT metrics_json ? 'annualized_return_with_cost'
            THEN jsonb_build_object('annualized_return_with_cost', metrics_json->'1day.excess_return_with_cost.annualized_return') ELSE '{}'::jsonb END
    || CASE WHEN metrics_json ? '1day.excess_return_with_cost.max_drawdown' AND NOT metrics_json ? 'max_drawdown'
            THEN jsonb_build_object('max_drawdown', metrics_json->'1day.excess_return_with_cost.max_drawdown') ELSE '{}'::jsonb END
    || CASE WHEN metrics_json ? '1day.excess_return_without_cost.information_ratio' AND NOT metrics_json ? 'sharpe_no_cost'
            THEN jsonb_build_object('sharpe_no_cost', metrics_json->'1day.excess_return_without_cost.information_ratio') ELSE '{}'::jsonb END
    || CASE WHEN metrics_json ? '1day.excess_return_without_cost.max_drawdown' AND NOT metrics_json ? 'max_drawdown_no_cost'
            THEN jsonb_build_object('max_drawdown_no_cost', metrics_json->'1day.excess_return_without_cost.max_drawdown') ELSE '{}'::jsonb END
    || CASE WHEN metrics_json ? '1day.excess_return_with_cost.mean' AND NOT metrics_json ? 'daily_return'
            THEN jsonb_build_object('daily_return', metrics_json->'1day.excess_return_with_cost.mean') ELSE '{}'::jsonb END
    || CASE WHEN metrics_json ? '1day.excess_return_without_cost.mean' AND NOT metrics_json ? 'daily_return_no_cost'
            THEN jsonb_build_object('daily_return_no_cost', metrics_json->'1day.excess_return_without_cost.mean') ELSE '{}'::jsonb END
WHERE status = 'completed' AND metrics_json IS NOT NULL;

-- 验证补写结果
SELECT loop_id,
       metrics_json->>'sharpe' AS sharpe,
       metrics_json->>'annualized_return' AS annualized_return,
       metrics_json->>'Rank_IC' AS rank_ic,
       metrics_json->>'max_drawdown' AS max_drawdown
FROM qe_evolution_loops
WHERE status = 'completed' AND metrics_json IS NOT NULL
ORDER BY created_at DESC
LIMIT 10;

COMMIT;
