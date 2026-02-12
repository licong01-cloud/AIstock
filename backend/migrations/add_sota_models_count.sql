-- 添加sota_models_count字段到rdagent_candidate_tasks表
-- 创建时间: 2026-02-01

-- 添加sota_models_count字段
ALTER TABLE rdagent.rdagent_candidate_tasks 
ADD COLUMN IF NOT EXISTS sota_models_count INTEGER DEFAULT 0;

-- 添加注释
COMMENT ON COLUMN rdagent.rdagent_candidate_tasks.sota_models_count IS 'SOTA模型LOOP数量';

-- 创建索引（可选，如果需要按模型数量查询）
CREATE INDEX IF NOT EXISTS idx_rdagent_candidate_tasks_sota_models 
    ON rdagent.rdagent_candidate_tasks(sota_models_count);
