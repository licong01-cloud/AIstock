-- =====================================================
-- 最小化迁移脚本：仅添加最佳 Loop 关联字段
-- 性能数据仅存储在 aistock_loop_catalog 表中
-- =====================================================

-- 1. 清理 aistock_factor_catalog 表中的性能字段（如果存在）
DO $$
BEGIN
    -- 删除性能字段
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_factor_catalog' 
        AND table_schema = 'public'
        AND column_name = 'best_max_drawdown'
    ) THEN
        ALTER TABLE aistock_factor_catalog DROP COLUMN best_max_drawdown;
        RAISE NOTICE 'Dropped best_max_drawdown from aistock_factor_catalog';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_factor_catalog' 
        AND table_schema = 'public'
        AND column_name = 'best_ic'
    ) THEN
        ALTER TABLE aistock_factor_catalog DROP COLUMN best_ic;
        RAISE NOTICE 'Dropped best_ic from aistock_factor_catalog';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_factor_catalog' 
        AND table_schema = 'public'
        AND column_name = 'best_ic_ir'
    ) THEN
        ALTER TABLE aistock_factor_catalog DROP COLUMN best_ic_ir;
        RAISE NOTICE 'Dropped best_ic_ir from aistock_factor_catalog';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_factor_catalog' 
        AND table_schema = 'public'
        AND column_name = 'best_win_rate'
    ) THEN
        ALTER TABLE aistock_factor_catalog DROP COLUMN best_win_rate;
        RAISE NOTICE 'Dropped best_win_rate from aistock_factor_catalog';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_factor_catalog' 
        AND table_schema = 'public'
        AND column_name = 'total_loop_count'
    ) THEN
        ALTER TABLE aistock_factor_catalog DROP COLUMN total_loop_count;
        RAISE NOTICE 'Dropped total_loop_count from aistock_factor_catalog';
    END IF;
END $$;

-- 添加最佳 Loop 关联字段
ALTER TABLE aistock_factor_catalog
ADD COLUMN IF NOT EXISTS best_loop_task_run_id TEXT,
ADD COLUMN IF NOT EXISTS best_loop_id INTEGER;

CREATE INDEX IF NOT EXISTS idx_factor_best_loop ON aistock_factor_catalog (best_loop_task_run_id, best_loop_id);

-- 2. 清理 aistock_strategy_catalog 表中的性能字段（如果存在）
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_strategy_catalog' 
        AND table_schema = 'public'
        AND column_name = 'best_annualized_return'
    ) THEN
        ALTER TABLE aistock_strategy_catalog DROP COLUMN best_annualized_return;
        RAISE NOTICE 'Dropped best_annualized_return from aistock_strategy_catalog';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_strategy_catalog' 
        AND table_schema = 'public'
        AND column_name = 'best_max_drawdown'
    ) THEN
        ALTER TABLE aistock_strategy_catalog DROP COLUMN best_max_drawdown;
        RAISE NOTICE 'Dropped best_max_drawdown from aistock_strategy_catalog';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_strategy_catalog' 
        AND table_schema = 'public'
        AND column_name = 'best_sharpe'
    ) THEN
        ALTER TABLE aistock_strategy_catalog DROP COLUMN best_sharpe;
        RAISE NOTICE 'Dropped best_sharpe from aistock_strategy_catalog';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_strategy_catalog' 
        AND table_schema = 'public'
        AND column_name = 'best_ic'
    ) THEN
        ALTER TABLE aistock_strategy_catalog DROP COLUMN best_ic;
        RAISE NOTICE 'Dropped best_ic from aistock_strategy_catalog';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_strategy_catalog' 
        AND table_schema = 'public'
        AND column_name = 'best_ic_ir'
    ) THEN
        ALTER TABLE aistock_strategy_catalog DROP COLUMN best_ic_ir;
        RAISE NOTICE 'Dropped best_ic_ir from aistock_strategy_catalog';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_strategy_catalog' 
        AND table_schema = 'public'
        AND column_name = 'best_win_rate'
    ) THEN
        ALTER TABLE aistock_strategy_catalog DROP COLUMN best_win_rate;
        RAISE NOTICE 'Dropped best_win_rate from aistock_strategy_catalog';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_strategy_catalog' 
        AND table_schema = 'public'
        AND column_name = 'best_performance_summary'
    ) THEN
        ALTER TABLE aistock_strategy_catalog DROP COLUMN best_performance_summary;
        RAISE NOTICE 'Dropped best_performance_summary from aistock_strategy_catalog';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_strategy_catalog' 
        AND table_schema = 'public'
        AND column_name = 'total_loop_count'
    ) THEN
        ALTER TABLE aistock_strategy_catalog DROP COLUMN total_loop_count;
        RAISE NOTICE 'Dropped total_loop_count from aistock_strategy_catalog';
    END IF;
END $$;

-- 添加最佳 Loop 关联字段
ALTER TABLE aistock_strategy_catalog
ADD COLUMN IF NOT EXISTS best_loop_task_run_id TEXT,
ADD COLUMN IF NOT EXISTS best_loop_id INTEGER;

CREATE INDEX IF NOT EXISTS idx_strategy_best_loop ON aistock_strategy_catalog (best_loop_task_run_id, best_loop_id);

-- 3. 清理 aistock_model_catalog 表中的性能字段（如果存在）
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_model_catalog' 
        AND table_schema = 'public'
        AND column_name = 'annualized_return'
    ) THEN
        ALTER TABLE aistock_model_catalog DROP COLUMN annualized_return;
        RAISE NOTICE 'Dropped annualized_return from aistock_model_catalog';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_model_catalog' 
        AND table_schema = 'public'
        AND column_name = 'max_drawdown'
    ) THEN
        ALTER TABLE aistock_model_catalog DROP COLUMN max_drawdown;
        RAISE NOTICE 'Dropped max_drawdown from aistock_model_catalog';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_model_catalog' 
        AND table_schema = 'public'
        AND column_name = 'sharpe'
    ) THEN
        ALTER TABLE aistock_model_catalog DROP COLUMN sharpe;
        RAISE NOTICE 'Dropped sharpe from aistock_model_catalog';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_model_catalog' 
        AND table_schema = 'public'
        AND column_name = 'ic'
    ) THEN
        ALTER TABLE aistock_model_catalog DROP COLUMN ic;
        RAISE NOTICE 'Dropped ic from aistock_model_catalog';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_model_catalog' 
        AND table_schema = 'public'
        AND column_name = 'ic_ir'
    ) THEN
        ALTER TABLE aistock_model_catalog DROP COLUMN ic_ir;
        RAISE NOTICE 'Dropped ic_ir from aistock_model_catalog';
    END IF;
    
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'aistock_model_catalog' 
        AND table_schema = 'public'
        AND column_name = 'win_rate'
    ) THEN
        ALTER TABLE aistock_model_catalog DROP COLUMN win_rate;
        RAISE NOTICE 'Dropped win_rate from aistock_model_catalog';
    END IF;
END $$;

-- 模型表已有 task_run_id 和 loop_id 字段，无需添加

-- 4. 删除性能关联表（如果存在）
DROP TABLE IF EXISTS aistock_factor_loop_performance CASCADE;
DROP TABLE IF EXISTS aistock_strategy_loop_performance CASCADE;

-- 5. 填充因子的最佳 Loop 关联
UPDATE aistock_factor_catalog f
SET 
    best_loop_task_run_id = fl.task_run_id,
    best_loop_id = fl.loop_id
FROM (
    SELECT 
        unnest(factor_names) as factor_name,
        task_run_id,
        loop_id,
        sharpe,
        ROW_NUMBER() OVER (PARTITION BY unnest(factor_names) ORDER BY sharpe DESC NULLS LAST) as rn
    FROM aistock_loop_catalog
    WHERE factor_names IS NOT NULL
      AND array_length(factor_names, 1) > 0
) fl
WHERE fl.factor_name = f.factor_name 
  AND fl.source = 'rdagent'
  AND fl.rn = 1;

-- 6. 填充策略的最佳 Loop 关联
UPDATE aistock_strategy_catalog s
SET 
    best_loop_task_run_id = sl.task_run_id,
    best_loop_id = sl.loop_id
FROM (
    SELECT 
        strategy_id,
        task_run_id,
        loop_id,
        sharpe,
        ROW_NUMBER() OVER (PARTITION BY strategy_id ORDER BY sharpe DESC NULLS LAST) as rn
    FROM aistock_loop_catalog
    WHERE strategy_id IS NOT NULL
) sl
WHERE sl.strategy_id = s.strategy_id 
  AND sl.rn = 1;

-- =====================================================
-- 验证查询
-- =====================================================

-- 验证因子表
SELECT 
    'Factor Catalog' as table_name,
    COUNT(*) as total,
    COUNT(best_loop_id) as with_best_loop,
    ROUND(COUNT(best_loop_id)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) as coverage_percentage
FROM aistock_factor_catalog;

-- 验证策略表
SELECT 
    'Strategy Catalog' as table_name,
    COUNT(*) as total,
    COUNT(best_loop_id) as with_best_loop,
    ROUND(COUNT(best_loop_id)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) as coverage_percentage
FROM aistock_strategy_catalog;

-- 验证模型表
SELECT 
    'Model Catalog' as table_name,
    COUNT(*) as total,
    COUNT(task_run_id) as with_loop,
    ROUND(COUNT(task_run_id)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) as coverage_percentage
FROM aistock_model_catalog;

-- 示例查询：获取因子的最佳性能
SELECT 
    f.factor_name,
    f.best_loop_task_run_id,
    f.best_loop_id,
    l.annualized_return,
    l.max_drawdown,
    l.sharpe,
    l.ic,
    l.ic_ir,
    l.win_rate
FROM aistock_factor_catalog f
JOIN aistock_loop_catalog l 
    ON f.best_loop_task_run_id = l.task_run_id 
    AND f.best_loop_id = l.loop_id
WHERE f.best_loop_id IS NOT NULL
LIMIT 5;

-- 示例查询：获取策略的最佳性能
SELECT 
    s.strategy_id,
    s.best_loop_task_run_id,
    s.best_loop_id,
    l.annualized_return,
    l.max_drawdown,
    l.sharpe,
    l.ic,
    l.ic_ir,
    l.win_rate
FROM aistock_strategy_catalog s
JOIN aistock_loop_catalog l 
    ON s.best_loop_task_run_id = l.task_run_id 
    AND s.best_loop_id = l.loop_id
WHERE s.best_loop_id IS NOT NULL
LIMIT 5;

-- 示例查询：获取模型的性能
SELECT 
    m.model_id,
    m.task_run_id,
    m.loop_id,
    l.annualized_return,
    l.max_drawdown,
    l.sharpe,
    l.ic,
    l.ic_ir,
    l.win_rate
FROM aistock_model_catalog m
JOIN aistock_loop_catalog l 
    ON m.task_run_id = l.task_run_id 
    AND m.loop_id = l.loop_id
WHERE m.task_run_id IS NOT NULL
LIMIT 5;
