-- =====================================================
-- AIstock RD-Agent Catalog 表结构改进脚本
-- 目标：为因子、策略、模型表添加最佳 Loop 关联字段
-- =====================================================

-- 1. 为 aistock_factor_catalog 添加最佳 Loop 关联字段
ALTER TABLE aistock_factor_catalog
ADD COLUMN IF NOT EXISTS best_loop_task_run_id TEXT,
ADD COLUMN IF NOT EXISTS best_loop_id INTEGER,
ADD COLUMN IF NOT EXISTS best_max_drawdown DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS best_ic DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS best_ic_ir DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS best_win_rate DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS total_loop_count INTEGER DEFAULT 0;

-- 添加索引
CREATE INDEX IF NOT EXISTS idx_factor_best_loop ON aistock_factor_catalog (best_loop_task_run_id, best_loop_id);

-- 2. 为 aistock_strategy_catalog 添加最佳 Loop 关联字段和性能指标
ALTER TABLE aistock_strategy_catalog
ADD COLUMN IF NOT EXISTS best_loop_task_run_id TEXT,
ADD COLUMN IF NOT EXISTS best_loop_id INTEGER,
ADD COLUMN IF NOT EXISTS best_annualized_return DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS best_max_drawdown DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS best_sharpe DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS best_ic DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS best_ic_ir DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS best_win_rate DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS best_performance_summary TEXT,
ADD COLUMN IF NOT EXISTS total_loop_count INTEGER DEFAULT 0;

-- 添加索引
CREATE INDEX IF NOT EXISTS idx_strategy_best_loop ON aistock_strategy_catalog (best_loop_task_run_id, best_loop_id);

-- 3. 为 aistock_model_catalog 添加性能指标字段
ALTER TABLE aistock_model_catalog
ADD COLUMN IF NOT EXISTS annualized_return DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS max_drawdown DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS sharpe DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS ic DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS ic_ir DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS win_rate DOUBLE PRECISION;

-- 4. 创建因子-Loop 性能关联表（支持多对多关系）
CREATE TABLE IF NOT EXISTS aistock_factor_loop_performance (
    id BIGSERIAL PRIMARY KEY,
    factor_name TEXT NOT NULL,
    source TEXT NOT NULL,
    task_run_id TEXT NOT NULL,
    loop_id INTEGER NOT NULL,
    annualized_return DOUBLE PRECISION,
    max_drawdown DOUBLE PRECISION,
    sharpe DOUBLE PRECISION,
    ic DOUBLE PRECISION,
    ic_ir DOUBLE PRECISION,
    win_rate DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (factor_name, source) REFERENCES aistock_factor_catalog (factor_name, source) ON DELETE CASCADE,
    UNIQUE (factor_name, source, task_run_id, loop_id)
);

-- 添加索引
CREATE INDEX IF NOT EXISTS idx_factor_loop_perf_factor ON aistock_factor_loop_performance (factor_name, source);
CREATE INDEX IF NOT EXISTS idx_factor_loop_perf_loop ON aistock_factor_loop_performance (task_run_id, loop_id);
CREATE INDEX IF NOT EXISTS idx_factor_loop_perf_sharpe ON aistock_factor_loop_performance (sharpe DESC);

-- 5. 创建策略-Loop 性能关联表（支持一对多关系）
CREATE TABLE IF NOT EXISTS aistock_strategy_loop_performance (
    id BIGSERIAL PRIMARY KEY,
    strategy_id TEXT NOT NULL,
    task_run_id TEXT NOT NULL,
    loop_id INTEGER NOT NULL,
    annualized_return DOUBLE PRECISION,
    max_drawdown DOUBLE PRECISION,
    sharpe DOUBLE PRECISION,
    ic DOUBLE PRECISION,
    ic_ir DOUBLE PRECISION,
    win_rate DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (strategy_id) REFERENCES aistock_strategy_catalog (strategy_id) ON DELETE CASCADE,
    UNIQUE (strategy_id, task_run_id, loop_id)
);

-- 添加索引
CREATE INDEX IF NOT EXISTS idx_strategy_loop_perf_strategy ON aistock_strategy_loop_performance (strategy_id);
CREATE INDEX IF NOT EXISTS idx_strategy_loop_perf_loop ON aistock_strategy_loop_performance (task_run_id, loop_id);
CREATE INDEX IF NOT EXISTS idx_strategy_loop_perf_sharpe ON aistock_strategy_loop_performance (sharpe DESC);

-- =====================================================
-- 数据迁移脚本：从 loop_catalog 填充关联表和聚合字段
-- =====================================================

-- 6. 填充因子-Loop 性能关联表
INSERT INTO aistock_factor_loop_performance (
    factor_name, source, task_run_id, loop_id,
    annualized_return, max_drawdown, sharpe, ic, ic_ir, win_rate
)
SELECT 
    unnest(factor_names) as factor_name,
    'rdagent' as source,
    task_run_id,
    loop_id,
    annualized_return,
    max_drawdown,
    sharpe,
    ic,
    ic_ir,
    win_rate
FROM aistock_loop_catalog
WHERE factor_names IS NOT NULL
  AND array_length(factor_names, 1) > 0
ON CONFLICT (factor_name, source, task_run_id, loop_id) DO UPDATE SET
    annualized_return = EXCLUDED.annualized_return,
    max_drawdown = EXCLUDED.max_drawdown,
    sharpe = EXCLUDED.sharpe,
    ic = EXCLUDED.ic,
    ic_ir = EXCLUDED.ic_ir,
    win_rate = EXCLUDED.win_rate,
    updated_at = NOW();

-- 7. 填充策略-Loop 性能关联表
INSERT INTO aistock_strategy_loop_performance (
    strategy_id, task_run_id, loop_id,
    annualized_return, max_drawdown, sharpe, ic, ic_ir, win_rate
)
SELECT 
    strategy_id,
    task_run_id,
    loop_id,
    annualized_return,
    max_drawdown,
    sharpe,
    ic,
    ic_ir,
    win_rate
FROM aistock_loop_catalog
WHERE strategy_id IS NOT NULL
ON CONFLICT (strategy_id, task_run_id, loop_id) DO UPDATE SET
    annualized_return = EXCLUDED.annualized_return,
    max_drawdown = EXCLUDED.max_drawdown,
    sharpe = EXCLUDED.sharpe,
    ic = EXCLUDED.ic,
    ic_ir = EXCLUDED.ic_ir,
    win_rate = EXCLUDED.win_rate,
    updated_at = NOW();

-- 8. 更新 aistock_factor_catalog 的聚合字段（最佳 Loop）
UPDATE aistock_factor_catalog f
SET 
    total_loop_count = sub.count,
    best_loop_task_run_id = sub.task_run_id,
    best_loop_id = sub.loop_id,
    best_max_drawdown = sub.max_drawdown,
    best_ic = sub.ic,
    best_ic_ir = sub.ic_ir,
    best_win_rate = sub.win_rate
FROM (
    SELECT 
        flp.factor_name,
        flp.source,
        COUNT(*) as count,
        flp.task_run_id,
        flp.loop_id,
        flp.max_drawdown,
        flp.ic,
        flp.ic_ir,
        flp.win_rate,
        ROW_NUMBER() OVER (PARTITION BY flp.factor_name, flp.source ORDER BY flp.sharpe DESC) as rn
    FROM aistock_factor_loop_performance flp
    WHERE flp.sharpe IS NOT NULL
    GROUP BY flp.factor_name, flp.source, flp.task_run_id, flp.loop_id,
             flp.max_drawdown, flp.ic, flp.ic_ir, flp.win_rate
) sub
WHERE sub.factor_name = f.factor_name 
  AND sub.source = f.source 
  AND sub.rn = 1;

-- 9. 更新 aistock_strategy_catalog 的聚合字段（最佳 Loop）
UPDATE aistock_strategy_catalog s
SET 
    total_loop_count = sub.count,
    best_loop_task_run_id = sub.task_run_id,
    best_loop_id = sub.loop_id,
    best_annualized_return = sub.annualized_return,
    best_max_drawdown = sub.max_drawdown,
    best_sharpe = sub.sharpe,
    best_ic = sub.ic,
    best_ic_ir = sub.ic_ir,
    best_win_rate = sub.win_rate,
    best_performance_summary = 
        'Sharpe: ' || COALESCE(sub.sharpe::TEXT, 'N/A') || 
        ', Ann.Ret: ' || COALESCE(ROUND((sub.annualized_return * 100)::NUMERIC, 2)::TEXT, 'N/A') || '%' ||
        ', MaxDD: ' || COALESCE(ROUND((sub.max_drawdown * 100)::NUMERIC, 2)::TEXT, 'N/A') || '%' ||
        ', IC: ' || COALESCE(ROUND(sub.ic::NUMERIC, 4)::TEXT, 'N/A')
FROM (
    SELECT 
        slp.strategy_id,
        COUNT(*) as count,
        slp.task_run_id,
        slp.loop_id,
        slp.annualized_return,
        slp.max_drawdown,
        slp.sharpe,
        slp.ic,
        slp.ic_ir,
        slp.win_rate,
        ROW_NUMBER() OVER (PARTITION BY slp.strategy_id ORDER BY slp.sharpe DESC) as rn
    FROM aistock_strategy_loop_performance slp
    WHERE slp.sharpe IS NOT NULL
    GROUP BY slp.strategy_id, slp.task_run_id, slp.loop_id,
             slp.annualized_return, slp.max_drawdown, slp.sharpe,
             slp.ic, slp.ic_ir, slp.win_rate
) sub
WHERE sub.strategy_id = s.strategy_id 
  AND sub.rn = 1;

-- 10. 更新 aistock_model_catalog 的性能字段
UPDATE aistock_model_catalog m
SET 
    annualized_return = l.annualized_return,
    max_drawdown = l.max_drawdown,
    sharpe = l.sharpe,
    ic = l.ic,
    ic_ir = l.ic_ir,
    win_rate = l.win_rate
FROM aistock_loop_catalog l
WHERE l.task_run_id = m.task_run_id 
  AND l.loop_id = m.loop_id;

-- =====================================================
-- 验证脚本
-- =====================================================

-- 验证因子表的最佳 Loop 关联
SELECT 
    'Factor Catalog' as table_name,
    COUNT(*) as total_factors,
    COUNT(best_loop_id) as factors_with_best_loop,
    ROUND(COUNT(best_loop_id)::NUMERIC / COUNT(*) * 100, 2) as coverage_percentage
FROM aistock_factor_catalog;

-- 验证策略表的最佳 Loop 关联
SELECT 
    'Strategy Catalog' as table_name,
    COUNT(*) as total_strategies,
    COUNT(best_loop_id) as strategies_with_best_loop,
    ROUND(COUNT(best_loop_id)::NUMERIC / COUNT(*) * 100, 2) as coverage_percentage
FROM aistock_strategy_catalog;

-- 验证模型表的性能字段
SELECT 
    'Model Catalog' as table_name,
    COUNT(*) as total_models,
    COUNT(sharpe) as models_with_performance,
    ROUND(COUNT(sharpe)::NUMERIC / COUNT(*) * 100, 2) as coverage_percentage
FROM aistock_model_catalog;

-- 验证因子-Loop 关联表
SELECT 
    'Factor-Loop Performance' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT factor_name) as unique_factors,
    COUNT(DISTINCT task_run_id || '-' || loop_id) as unique_loops
FROM aistock_factor_loop_performance;

-- 验证策略-Loop 关联表
SELECT 
    'Strategy-Loop Performance' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT strategy_id) as unique_strategies,
    COUNT(DISTINCT task_run_id || '-' || loop_id) as unique_loops
FROM aistock_strategy_loop_performance;
