-- ============================================
-- LLM配置系统升级 - 数据库迁移脚本
-- 功能：支持RDAgent和AIstock独立配置、模型同步、服务商区分
-- 创建时间：2026-02-19
-- ============================================

-- ============================================
-- Part 1: 修改服务商表
-- ============================================

-- 添加新字段
ALTER TABLE aistock_llm_providers 
ADD COLUMN IF NOT EXISTS provider_type VARCHAR(50) DEFAULT 'official';

ALTER TABLE aistock_llm_providers 
ADD COLUMN IF NOT EXISTS use_proxy BOOLEAN DEFAULT FALSE;

ALTER TABLE aistock_llm_providers 
ADD COLUMN IF NOT EXISTS proxy_model_prefix VARCHAR(100);

ALTER TABLE aistock_llm_providers 
ADD COLUMN IF NOT EXISTS supports_vision BOOLEAN DEFAULT FALSE;

-- 修改litellm_prefix字段，去掉末尾斜杠（统一格式）
UPDATE aistock_llm_providers SET litellm_prefix = 'deepseek' WHERE provider_name = 'deepseek' AND litellm_prefix LIKE 'deepseek/%';
UPDATE aistock_llm_providers SET litellm_prefix = 'openai' WHERE provider_name = 'siliconflow' AND litellm_prefix LIKE 'openai/%';
UPDATE aistock_llm_providers SET litellm_prefix = 'dashscope' WHERE provider_name = 'dashscope' AND litellm_prefix LIKE 'dashscope/%';
UPDATE aistock_llm_providers SET litellm_prefix = 'anthropic' WHERE provider_name = 'claude' AND litellm_prefix LIKE 'anthropic/%';

-- 更新provider_type
UPDATE aistock_llm_providers SET provider_type = 'official' WHERE provider_name IN ('deepseek', 'claude');
UPDATE aistock_llm_providers SET provider_type = 'platform' WHERE provider_name = 'dashscope';
UPDATE aistock_llm_providers SET provider_type = 'agent' WHERE provider_name = 'siliconflow';

-- 添加注释
COMMENT ON COLUMN aistock_llm_providers.provider_type IS '服务商类型: official=官方, agent=代理商, platform=平台, proxy=代理服务';
COMMENT ON COLUMN aistock_llm_providers.use_proxy IS '是否需要通过LiteLLM Proxy访问';
COMMENT ON COLUMN aistock_llm_providers.proxy_model_prefix IS 'Proxy模型前缀，用于生成模型别名';

-- ============================================
-- Part 2: 修改模型表
-- ============================================

-- 添加新字段
ALTER TABLE aistock_llm_models 
ADD COLUMN IF NOT EXISTS proxy_model_alias VARCHAR(200);

ALTER TABLE aistock_llm_models 
ADD COLUMN IF NOT EXISTS input_price DECIMAL(10, 4);

ALTER TABLE aistock_llm_models 
ADD COLUMN IF NOT EXISTS output_price DECIMAL(10, 4);

ALTER TABLE aistock_llm_models 
ADD COLUMN IF NOT EXISTS is_synced BOOLEAN DEFAULT FALSE;

-- 添加唯一约束（服务商+模型名）
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'aistock_llm_models_provider_model_uniq'
    ) THEN
        ALTER TABLE aistock_llm_models ADD CONSTRAINT aistock_llm_models_provider_model_uniq UNIQUE(provider_id, model_name);
    END IF;
END $$;

-- 修改model_type约束，添加vision类型
ALTER TABLE aistock_llm_models DROP CONSTRAINT IF EXISTS chk_model_type;
ALTER TABLE aistock_llm_models ADD CONSTRAINT chk_model_type CHECK (model_type IN ('chat', 'embedding', 'reasoner', 'vision'));

-- 添加注释
COMMENT ON COLUMN aistock_llm_models.proxy_model_alias IS 'Proxy模型别名，用于litellm_proxy/{alias}格式';
COMMENT ON COLUMN aistock_llm_models.is_synced IS '是否从服务商API自动同步';

-- ============================================
-- Part 3: 阶段映射表（RDAgent专用）
-- ============================================

-- 检查原表是否存在，如果存在则重命名，否则创建新表
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'aistock_llm_stage_mapping') THEN
        -- 重命名表
        ALTER TABLE aistock_llm_stage_mapping RENAME TO aistock_llm_rdagent_stages;
    ELSIF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'aistock_llm_rdagent_stages') THEN
        -- 创建新表
        CREATE TABLE aistock_llm_rdagent_stages (
            id BIGSERIAL PRIMARY KEY,
            stage_name VARCHAR(50) NOT NULL UNIQUE,
            model_id BIGINT REFERENCES aistock_llm_models(id) ON DELETE SET NULL,
            temperature DECIMAL(3, 2),
            max_tokens INT,
            stage_display_name VARCHAR(100),
            description TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            CONSTRAINT chk_stage_name CHECK (stage_name IN ('direct_exp_gen', 'coding', 'feedback', 'default', 'embedding', 'hypothesis', 'summary'))
        );
    END IF;
END $$;

-- 添加新字段
ALTER TABLE aistock_llm_rdagent_stages 
ADD COLUMN IF NOT EXISTS stage_display_name VARCHAR(100);

ALTER TABLE aistock_llm_rdagent_stages 
ADD COLUMN IF NOT EXISTS description TEXT;

ALTER TABLE aistock_llm_rdagent_stages 
ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;

-- 更新阶段显示名称
UPDATE aistock_llm_rdagent_stages SET stage_display_name = '直接实验生成', description = '直接生成实验代码' WHERE stage_name = 'direct_exp_gen';
UPDATE aistock_llm_rdagent_stages SET stage_display_name = '编码阶段', description = '因子代码生成' WHERE stage_name = 'coding';
UPDATE aistock_llm_rdagent_stages SET stage_display_name = '反馈阶段', description = '实验结果分析' WHERE stage_name = 'feedback';
UPDATE aistock_llm_rdagent_stages SET stage_display_name = '默认阶段', description = '默认模型' WHERE stage_name = 'default';
UPDATE aistock_llm_rdagent_stages SET stage_display_name = '嵌入阶段', description = '文本嵌入' WHERE stage_name = 'embedding';

-- 添加新阶段
INSERT INTO aistock_llm_rdagent_stages (stage_name, stage_display_name, description, temperature, max_tokens, is_active) VALUES
('hypothesis', '假设阶段', '假设生成', 0.8, 4000, TRUE),
('summary', '总结阶段', '总结生成', 0.7, 4000, TRUE)
ON CONFLICT (stage_name) DO NOTHING;

-- 添加注释
COMMENT ON TABLE aistock_llm_rdagent_stages IS 'RDAgent阶段-模型映射表';

-- ============================================
-- Part 4: 创建AIstock Agent配置表
-- ============================================

CREATE TABLE IF NOT EXISTS aistock_llm_aistock_agents (
    id BIGSERIAL PRIMARY KEY,
    agent_key VARCHAR(100) NOT NULL UNIQUE,
    agent_name VARCHAR(200) NOT NULL,
    agent_type VARCHAR(50),
    model_id BIGINT REFERENCES aistock_llm_models(id) ON DELETE SET NULL,
    temperature DECIMAL(3, 2) DEFAULT 0.7,
    max_tokens INT DEFAULT 4000,
    system_prompt TEXT,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_aistock_agents_key ON aistock_llm_aistock_agents(agent_key);
CREATE INDEX IF NOT EXISTS idx_aistock_agents_active ON aistock_llm_aistock_agents(is_active);
CREATE INDEX IF NOT EXISTS idx_aistock_agents_model ON aistock_llm_aistock_agents(model_id);

-- 预置AIstock Agent
INSERT INTO aistock_llm_aistock_agents (agent_key, agent_name, agent_type, description) VALUES
('factor_analyzer', '因子分析器', 'analysis', '因子数据分析'),
('strategy_generator', '策略生成器', 'generation', '策略代码生成'),
('data_processor', '数据处理器', 'processing', '数据处理任务'),
('report_generator', '报告生成器', 'generation', '分析报告生成'),
('risk_analyzer', '风险分析器', 'analysis', '风险评估分析'),
('market_sentiment', '市场情绪分析', 'analysis', '市场情绪监控')
ON CONFLICT (agent_key) DO NOTHING;

-- 添加注释
COMMENT ON TABLE aistock_llm_aistock_agents IS 'AIstock Agent配置表';
COMMENT ON COLUMN aistock_llm_aistock_agents.agent_key IS 'Agent唯一标识';
COMMENT ON COLUMN aistock_llm_aistock_agents.agent_type IS 'Agent类型: analysis/generation/processing';

-- ============================================
-- Part 5: 创建模型类型视图
-- ============================================

CREATE OR REPLACE VIEW v_llm_models_by_type AS
SELECT 
    m.id,
    m.model_name,
    m.display_name,
    m.full_model_id,
    m.model_type,
    m.model_category,
    m.context_window,
    m.input_price,
    m.output_price,
    p.provider_name,
    p.display_name AS provider_display_name,
    p.litellm_prefix,
    p.use_proxy,
    m.proxy_model_alias,
    m.is_synced,
    m.is_active
FROM aistock_llm_models m
JOIN aistock_llm_providers p ON m.provider_id = p.id
WHERE m.is_active = TRUE AND p.is_active = TRUE
ORDER BY p.display_name, m.model_type, m.display_name;

COMMENT ON VIEW v_llm_models_by_type IS '模型类型分类视图，用于按类型筛选模型';

-- ============================================
-- Part 6: 创建服务商模型统计视图
-- ============================================

CREATE OR REPLACE VIEW v_provider_model_stats AS
SELECT 
    p.id AS provider_id,
    p.provider_name,
    p.display_name,
    p.litellm_prefix,
    p.provider_type,
    p.use_proxy,
    COUNT(m.id) AS total_models,
    COUNT(CASE WHEN m.model_type = 'chat' THEN 1 END) AS chat_models,
    COUNT(CASE WHEN m.model_type = 'embedding' THEN 1 END) AS embedding_models,
    COUNT(CASE WHEN m.model_type = 'reasoner' THEN 1 END) AS reasoner_models,
    COUNT(CASE WHEN m.model_type = 'vision' THEN 1 END) AS vision_models,
    COUNT(CASE WHEN m.is_synced THEN 1 END) AS synced_models
FROM aistock_llm_providers p
LEFT JOIN aistock_llm_models m ON p.id = m.provider_id AND m.is_active = TRUE
WHERE p.is_active = TRUE
GROUP BY p.id, p.provider_name, p.display_name, p.litellm_prefix, p.provider_type, p.use_proxy
ORDER BY p.display_name;

COMMENT ON VIEW v_provider_model_stats IS '服务商模型统计视图';

-- ============================================
-- Part 7: 创建配置预览视图
-- ============================================

-- RDAgent配置预览视图
CREATE OR REPLACE VIEW v_rdagent_config_preview AS
SELECT 
    s.stage_name,
    s.stage_display_name,
    s.temperature,
    s.max_tokens,
    m.model_name,
    m.display_name AS model_display_name,
    m.full_model_id,
    p.provider_name,
    p.display_name AS provider_display_name,
    p.litellm_prefix,
    ac.api_base,
    ac.env_api_key_name,
    ac.env_api_base_name
FROM aistock_llm_rdagent_stages s
LEFT JOIN aistock_llm_models m ON s.model_id = m.id
LEFT JOIN aistock_llm_providers p ON m.provider_id = p.id
LEFT JOIN aistock_llm_api_configs ac ON m.api_config_id = ac.id
WHERE s.is_active = TRUE
ORDER BY s.stage_name;

COMMENT ON VIEW v_rdagent_config_preview IS 'RDAgent配置预览视图';

-- AIstock Agent配置预览视图
CREATE OR REPLACE VIEW v_aistock_agent_config_preview AS
SELECT 
    a.agent_key,
    a.agent_name,
    a.agent_type,
    a.temperature,
    a.max_tokens,
    a.system_prompt,
    m.model_name,
    m.display_name AS model_display_name,
    m.full_model_id,
    p.provider_name,
    p.display_name AS provider_display_name,
    p.litellm_prefix,
    ac.api_base,
    ac.env_api_key_name,
    ac.env_api_base_name
FROM aistock_llm_aistock_agents a
LEFT JOIN aistock_llm_models m ON a.model_id = m.id
LEFT JOIN aistock_llm_providers p ON m.provider_id = p.id
LEFT JOIN aistock_llm_api_configs ac ON m.api_config_id = ac.id OR (ac.provider_id = p.id AND ac.config_purpose = 'default')
WHERE a.is_active = TRUE
ORDER BY a.agent_key;

COMMENT ON VIEW v_aistock_agent_config_preview IS 'AIstock Agent配置预览视图';

-- ============================================
-- Part 8: 更新变更记录表约束
-- ============================================

-- 更新变更记录表的阶段约束
ALTER TABLE aistock_llm_stage_change_log DROP CONSTRAINT IF EXISTS chk_stage_name_log;
ALTER TABLE aistock_llm_stage_change_log ADD CONSTRAINT chk_stage_name_log CHECK (stage_name IN ('direct_exp_gen', 'coding', 'feedback', 'default', 'embedding', 'hypothesis', 'summary'));

-- ============================================
-- 完成
-- ============================================

-- 输出完成信息
DO $$
BEGIN
    RAISE NOTICE 'LLM配置系统升级完成';
    RAISE NOTICE '- 服务商表新增: provider_type, use_proxy, proxy_model_prefix, supports_vision';
    RAISE NOTICE '- 模型表新增: proxy_model_alias, input_price, output_price, is_synced';
    RAISE NOTICE '- 阶段映射表重命名为: aistock_llm_rdagent_stages';
    RAISE NOTICE '- 新建AIstock Agent配置表: aistock_llm_aistock_agents';
    RAISE NOTICE '- 新建视图: v_llm_models_by_type, v_provider_model_stats, v_rdagent_config_preview, v_aistock_agent_config_preview';
END $$;
