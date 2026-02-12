-- V2对齐信息字段迁移
-- 为rdagent_candidate_tasks表新增V2对齐预览相关字段
-- 创建时间: 2026-02-09

-- Alpha基线因子数
ALTER TABLE rdagent.rdagent_candidate_tasks
    ADD COLUMN IF NOT EXISTS alpha_factors_count INTEGER DEFAULT 0;

-- 模型特征数
ALTER TABLE rdagent.rdagent_candidate_tasks
    ADD COLUMN IF NOT EXISTS model_feature_count INTEGER DEFAULT NULL;

-- V2对齐状态
ALTER TABLE rdagent.rdagent_candidate_tasks
    ADD COLUMN IF NOT EXISTS is_aligned BOOLEAN DEFAULT NULL;

-- V2对齐检查时间
ALTER TABLE rdagent.rdagent_candidate_tasks
    ADD COLUMN IF NOT EXISTS v2_checked_at TIMESTAMP WITH TIME ZONE DEFAULT NULL;

-- SOTA因子名列表(JSON数组)
ALTER TABLE rdagent.rdagent_candidate_tasks
    ADD COLUMN IF NOT EXISTS sota_factors_list JSONB DEFAULT NULL;

-- Alpha因子名列表(JSON数组)
ALTER TABLE rdagent.rdagent_candidate_tasks
    ADD COLUMN IF NOT EXISTS alpha_factors_list JSONB DEFAULT NULL;

-- LOOP总数(从V2 API获取的hist_len)
-- hist_len字段已存在，无需新增

-- 添加注释
COMMENT ON COLUMN rdagent.rdagent_candidate_tasks.alpha_factors_count IS 'Alpha基线因子数(从V2对齐API获取)';
COMMENT ON COLUMN rdagent.rdagent_candidate_tasks.model_feature_count IS '模型特征数(从V2对齐API获取)';
COMMENT ON COLUMN rdagent.rdagent_candidate_tasks.is_aligned IS 'V2对齐状态: Alpha+SOTA==模型特征数';
COMMENT ON COLUMN rdagent.rdagent_candidate_tasks.v2_checked_at IS 'V2对齐信息最后检查时间';
COMMENT ON COLUMN rdagent.rdagent_candidate_tasks.sota_factors_list IS 'SOTA因子名列表(JSON数组)';
COMMENT ON COLUMN rdagent.rdagent_candidate_tasks.alpha_factors_list IS 'Alpha基线因子名列表(JSON数组)';
