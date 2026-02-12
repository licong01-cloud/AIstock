-- ============================================================
-- AIstock Task同步系统数据库表结构
-- 基于《模型权重文件定位方案_v2.md》
-- ============================================================

-- 1. Factor Catalog表：存储所有SOTA因子（含去重）
CREATE TABLE IF NOT EXISTS rdagent.rdagent_factor_catalog (
    factor_id SERIAL PRIMARY KEY,
    factor_name TEXT NOT NULL,
    factor_code TEXT,
    factor_formulation TEXT,
    factor_description TEXT,
    
    -- 性能指标（保留最佳版本）
    best_ic DOUBLE PRECISION,
    best_icir DOUBLE PRECISION,
    best_annual_return DOUBLE PRECISION,
    best_max_drawdown DOUBLE PRECISION,
    best_sharpe_ratio DOUBLE PRECISION,
    
    -- 来源信息
    source_task_id TEXT NOT NULL,
    source_loop_id INTEGER,
    source_factor_index INTEGER,
    
    -- 文件资产路径
    factor_code_path TEXT,
    
    -- 元数据
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- 唯一约束：因子名称唯一
    CONSTRAINT uk_factor_name UNIQUE (factor_name)
);

CREATE INDEX IF NOT EXISTS idx_factor_catalog_task ON rdagent.rdagent_factor_catalog(source_task_id);
CREATE INDEX IF NOT EXISTS idx_factor_catalog_ic ON rdagent.rdagent_factor_catalog(best_ic DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_factor_catalog_created ON rdagent.rdagent_factor_catalog(created_at DESC);

COMMENT ON TABLE rdagent.rdagent_factor_catalog IS 'SOTA因子目录表，存储所有去重后的SOTA因子';
COMMENT ON COLUMN rdagent.rdagent_factor_catalog.factor_name IS '因子名称（唯一）';
COMMENT ON COLUMN rdagent.rdagent_factor_catalog.best_ic IS '最佳IC值（去重时保留）';
COMMENT ON COLUMN rdagent.rdagent_factor_catalog.source_task_id IS '来源Task ID';

-- 2. LOOP Catalog表：存储所有LOOP详情
CREATE TABLE IF NOT EXISTS rdagent.rdagent_loop_catalog (
    loop_catalog_id SERIAL PRIMARY KEY,
    task_id TEXT NOT NULL,
    loop_id INTEGER NOT NULL,
    
    -- LOOP基本信息
    exp_type TEXT,
    hypothesis TEXT,
    reason TEXT,
    
    -- 性能指标
    valid_score DOUBLE PRECISION,
    annual_return DOUBLE PRECISION,
    max_drawdown DOUBLE PRECISION,
    information_ratio DOUBLE PRECISION,
    sharpe_ratio DOUBLE PRECISION,
    
    -- SOTA标记
    is_sota BOOLEAN DEFAULT FALSE,
    
    -- 因子信息（如果是FactorTask）
    factor_name TEXT,
    factor_formulation TEXT,
    
    -- 元数据
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- 唯一约束：task_id + loop_id唯一
    CONSTRAINT uk_task_loop UNIQUE (task_id, loop_id)
);

CREATE INDEX IF NOT EXISTS idx_loop_catalog_task ON rdagent.rdagent_loop_catalog(task_id);
CREATE INDEX IF NOT EXISTS idx_loop_catalog_sota ON rdagent.rdagent_loop_catalog(is_sota) WHERE is_sota = TRUE;
CREATE INDEX IF NOT EXISTS idx_loop_catalog_ic ON rdagent.rdagent_loop_catalog(valid_score DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_loop_catalog_created ON rdagent.rdagent_loop_catalog(created_at DESC);

COMMENT ON TABLE rdagent.rdagent_loop_catalog IS 'LOOP目录表，存储所有Task的LOOP详情';
COMMENT ON COLUMN rdagent.rdagent_loop_catalog.is_sota IS '是否为SOTA LOOP（基于last_sota_factor_index判断）';
COMMENT ON COLUMN rdagent.rdagent_loop_catalog.valid_score IS 'IC值（验证集）';

-- 3. 更新aistock_task_catalog表，添加SOTA和LOOP统计字段
ALTER TABLE aistock_task_catalog 
ADD COLUMN IF NOT EXISTS sota_factors_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS total_loops_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS has_model_weight BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS has_factor_order BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS dir_exists BOOLEAN DEFAULT TRUE;

COMMENT ON COLUMN aistock_task_catalog.sota_factors_count IS 'SOTA因子数量';
COMMENT ON COLUMN aistock_task_catalog.total_loops_count IS 'LOOP总数';
COMMENT ON COLUMN aistock_task_catalog.has_model_weight IS '是否有模型权重文件';
COMMENT ON COLUMN aistock_task_catalog.has_factor_order IS '是否有factor_order.json';
COMMENT ON COLUMN aistock_task_catalog.dir_exists IS 'log目录是否存在';

-- 4. 创建Alpha基线因子表
CREATE TABLE IF NOT EXISTS rdagent.rdagent_alpha_baseline_factors (
    baseline_id SERIAL PRIMARY KEY,
    task_id TEXT NOT NULL,
    factor_name TEXT NOT NULL,
    factor_index INTEGER,
    
    -- 来源信息
    source_type TEXT, -- 'FilterCol.col_list' or 'model_meta.json'
    
    -- 元数据
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- 唯一约束
    CONSTRAINT uk_task_baseline_factor UNIQUE (task_id, factor_name)
);

CREATE INDEX IF NOT EXISTS idx_baseline_task ON rdagent.rdagent_alpha_baseline_factors(task_id);

COMMENT ON TABLE rdagent.rdagent_alpha_baseline_factors IS 'Alpha基线因子表，存储与SOTA因子一起回测的Alpha158因子';

-- 5. 创建因子入口序列表
CREATE TABLE IF NOT EXISTS rdagent.rdagent_factor_order (
    order_id SERIAL PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE,
    
    -- 因子顺序（JSON数组）
    factor_order JSONB NOT NULL,
    
    -- 统计信息
    total_factors INTEGER,
    alpha158_count INTEGER,
    dynamic_factors_count INTEGER,
    
    -- 来源信息
    source_file TEXT, -- 'combined_factors_df.parquet'
    generated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- 元数据
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_factor_order_task ON rdagent.rdagent_factor_order(task_id);

COMMENT ON TABLE rdagent.rdagent_factor_order IS '因子入口序列表，存储训练时使用的因子顺序';
COMMENT ON COLUMN rdagent.rdagent_factor_order.factor_order IS '因子顺序JSON数组，确保与训练时一致';
