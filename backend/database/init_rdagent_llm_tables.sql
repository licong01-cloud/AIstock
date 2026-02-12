-- RD-Agent LLM Configuration Management Tables
-- AIstock侧数据库表结构
-- 用于存储LLM配置、变更记录等信息

-- 1. LLM服务商配置表
CREATE TABLE IF NOT EXISTS aistock_llm_providers (
    id BIGSERIAL PRIMARY KEY,
    provider_name VARCHAR(100) NOT NULL UNIQUE,
    display_name VARCHAR(200) NOT NULL,
    api_base_url VARCHAR(500) NOT NULL,
    litellm_prefix VARCHAR(50) NOT NULL,
    api_key VARCHAR(500),
    supports_chat BOOLEAN DEFAULT true,
    supports_embedding BOOLEAN DEFAULT false,
    supports_reasoner BOOLEAN DEFAULT false,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. LLM模型配置表
CREATE TABLE IF NOT EXISTS aistock_llm_models (
    id BIGSERIAL PRIMARY KEY,
    provider_id BIGINT NOT NULL REFERENCES aistock_llm_providers(id) ON DELETE CASCADE,
    model_name VARCHAR(200) NOT NULL,
    display_name VARCHAR(200) NOT NULL,
    full_model_id VARCHAR(300) NOT NULL UNIQUE,
    model_type VARCHAR(50) NOT NULL,
    model_category VARCHAR(50),
    description TEXT,
    is_verified BOOLEAN DEFAULT false,
    last_verified_at TIMESTAMP,
    verification_error TEXT,
    max_tokens INT,
    context_window INT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_model_type CHECK (model_type IN ('chat', 'embedding', 'reasoner')),
    CONSTRAINT chk_model_category CHECK (model_category IN ('对话/Coding', '推理模型', '嵌入式模型')),
    CONSTRAINT chk_description_length CHECK (char_length(description) <= 100)
);

-- 3. 阶段-模型映射表
CREATE TABLE IF NOT EXISTS aistock_llm_stage_mapping (
    id BIGSERIAL PRIMARY KEY,
    stage_name VARCHAR(50) NOT NULL UNIQUE,
    model_id BIGINT REFERENCES aistock_llm_models(id) ON DELETE SET NULL,
    temperature DECIMAL(3, 2),
    max_tokens INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_stage_name CHECK (stage_name IN ('direct_exp_gen', 'coding', 'feedback', 'default', 'embedding'))
);

-- 4. 阶段模型变更记录表
CREATE TABLE IF NOT EXISTS aistock_llm_stage_change_log (
    id BIGSERIAL PRIMARY KEY,
    stage_name VARCHAR(50) NOT NULL,
    old_model_id BIGINT REFERENCES aistock_llm_models(id) ON DELETE SET NULL,
    new_model_id BIGINT REFERENCES aistock_llm_models(id) ON DELETE SET NULL,
    old_model_display_name VARCHAR(200),
    new_model_display_name VARCHAR(200),
    old_full_model_id VARCHAR(300),
    new_full_model_id VARCHAR(300),
    change_reason VARCHAR(200),
    is_rollback BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_stage_name_log CHECK (stage_name IN ('direct_exp_gen', 'coding', 'feedback', 'default', 'embedding'))
);

-- 5. .env备份历史表
CREATE TABLE IF NOT EXISTS aistock_env_backup_history (
    id BIGSERIAL PRIMARY KEY,
    backup_filename VARCHAR(500) NOT NULL,
    backup_path VARCHAR(1000) NOT NULL,
    backup_size BIGINT,
    backup_reason VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_llm_models_provider ON aistock_llm_models(provider_id);
CREATE INDEX IF NOT EXISTS idx_llm_models_type ON aistock_llm_models(model_type);
CREATE INDEX IF NOT EXISTS idx_llm_models_active ON aistock_llm_models(is_active);
CREATE INDEX IF NOT EXISTS idx_stage_mapping_stage ON aistock_llm_stage_mapping(stage_name);
CREATE INDEX IF NOT EXISTS idx_stage_change_log_stage ON aistock_llm_stage_change_log(stage_name);
CREATE INDEX IF NOT EXISTS idx_stage_change_log_created ON aistock_llm_stage_change_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_env_backup_created ON aistock_env_backup_history(created_at DESC);

-- 初始化默认服务商数据（从当前.env配置迁移）
INSERT INTO aistock_llm_providers (provider_name, display_name, api_base_url, litellm_prefix, supports_chat, supports_embedding, supports_reasoner) VALUES
('deepseek', 'DeepSeek', 'https://api.deepseek.com', 'deepseek/', true, false, true),
('siliconflow', '硅基流动', 'https://api.siliconflow.cn/v1', 'openai/', true, true, false),
('dashscope', '阿里云百炼', 'https://dashscope.aliyuncs.com/compatible-mode/v1', 'dashscope/openai/', true, true, false),
('claude', 'Claude (Anthropic)', 'https://api.anthropic.com', 'anthropic/', true, false, true)
ON CONFLICT (provider_name) DO NOTHING;

-- 初始化默认阶段映射
INSERT INTO aistock_llm_stage_mapping (stage_name, temperature, max_tokens) VALUES
('direct_exp_gen', 0.8, 6000),
('coding', 0.7, 4000),
('feedback', 0.5, 5000),
('default', 0.7, 4000),
('embedding', NULL, NULL)
ON CONFLICT (stage_name) DO NOTHING;

-- 添加注释
COMMENT ON TABLE aistock_llm_providers IS 'LLM服务商配置表';
COMMENT ON TABLE aistock_llm_models IS 'LLM模型配置表';
COMMENT ON TABLE aistock_llm_stage_mapping IS 'RD-Agent阶段-模型映射表';
COMMENT ON TABLE aistock_llm_stage_change_log IS '阶段模型变更记录表';
COMMENT ON TABLE aistock_env_backup_history IS '.env文件备份历史表';
