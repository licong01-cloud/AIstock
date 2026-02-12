-- RD-Agent 备选TASK和LOOP表创建脚本
-- 创建时间: 2026-01-30

-- 创建schema（如果不存在）
CREATE SCHEMA IF NOT EXISTS rdagent;

-- 1. 创建备选TASK表
CREATE TABLE IF NOT EXISTS rdagent.rdagent_candidate_tasks (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(100) UNIQUE NOT NULL,
    log_dir TEXT NOT NULL,
    
    -- SOTA因子信息
    has_sota BOOLEAN DEFAULT NULL,
    sota_factors_count INTEGER DEFAULT 0,
    sota_checked_at TIMESTAMP WITH TIME ZONE,
    
    -- 任务基本信息
    hist_len INTEGER DEFAULT 0,
    task_status VARCHAR(50),
    
    -- 同步状态
    is_synced BOOLEAN DEFAULT FALSE,
    sync_status VARCHAR(50),
    synced_at TIMESTAMP WITH TIME ZONE,
    
    -- 文件状态
    dir_exists BOOLEAN DEFAULT TRUE,
    dir_checked_at TIMESTAMP WITH TIME ZONE,
    
    -- 时间戳
    discovered_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_rdagent_candidate_tasks_has_sota 
    ON rdagent.rdagent_candidate_tasks(has_sota);
CREATE INDEX IF NOT EXISTS idx_rdagent_candidate_tasks_is_synced 
    ON rdagent.rdagent_candidate_tasks(is_synced);
CREATE INDEX IF NOT EXISTS idx_rdagent_candidate_tasks_dir_exists 
    ON rdagent.rdagent_candidate_tasks(dir_exists);
CREATE INDEX IF NOT EXISTS idx_rdagent_candidate_tasks_discovered_at 
    ON rdagent.rdagent_candidate_tasks(discovered_at DESC);

-- 2. 创建备选LOOP表
CREATE TABLE IF NOT EXISTS rdagent.rdagent_candidate_loops (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(100) NOT NULL,
    loop_id INTEGER NOT NULL,
    
    -- 实验信息
    exp_type VARCHAR(100),
    hypothesis TEXT,
    reason TEXT,
    
    -- 性能指标
    valid_score DOUBLE PRECISION,
    test_score DOUBLE PRECISION,
    annualized_return DOUBLE PRECISION,
    max_drawdown DOUBLE PRECISION,
    information_ratio DOUBLE PRECISION,
    
    -- SOTA标记
    is_sota BOOLEAN DEFAULT FALSE,
    feedback TEXT,
    
    -- 时间戳
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束
    CONSTRAINT rdagent_candidate_loops_task_loop_key UNIQUE (task_id, loop_id),
    
    -- 外键约束
    CONSTRAINT fk_rdagent_candidate_loops_task 
        FOREIGN KEY (task_id) 
        REFERENCES rdagent.rdagent_candidate_tasks(task_id) 
        ON DELETE CASCADE
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_rdagent_candidate_loops_task_id 
    ON rdagent.rdagent_candidate_loops(task_id);
CREATE INDEX IF NOT EXISTS idx_rdagent_candidate_loops_is_sota 
    ON rdagent.rdagent_candidate_loops(is_sota);
CREATE INDEX IF NOT EXISTS idx_rdagent_candidate_loops_valid_score 
    ON rdagent.rdagent_candidate_loops(valid_score DESC);

-- 添加注释
COMMENT ON TABLE rdagent.rdagent_candidate_tasks IS 'RD-Agent备选任务表，缓存从log目录扫描的TASK信息';
COMMENT ON TABLE rdagent.rdagent_candidate_loops IS 'RD-Agent备选LOOP表，缓存TASK中的LOOP详情';

COMMENT ON COLUMN rdagent.rdagent_candidate_tasks.has_sota IS '是否有SOTA因子';
COMMENT ON COLUMN rdagent.rdagent_candidate_tasks.dir_exists IS '目录是否存在';
COMMENT ON COLUMN rdagent.rdagent_candidate_loops.valid_score IS 'IC值（验证集）';
COMMENT ON COLUMN rdagent.rdagent_candidate_loops.annualized_return IS '年化收益';
COMMENT ON COLUMN rdagent.rdagent_candidate_loops.max_drawdown IS '最大回撤';
