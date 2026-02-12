-- ============================================
-- RDAgent UI配置功能增强 - 数据库迁移脚本
-- 功能：新增API配置管理表，支持多服务商API配置
-- 创建时间：2026-02-05
-- ============================================

-- 1. 创建API配置表
CREATE TABLE IF NOT EXISTS aistock_llm_api_configs (
    id SERIAL PRIMARY KEY,
    provider_id INTEGER NOT NULL REFERENCES aistock_llm_providers(id) ON DELETE CASCADE,
    
    -- API配置信息
    api_base VARCHAR(500) NOT NULL,
    api_key VARCHAR(500) NOT NULL,  -- 建议后续加密存储
    
    -- 环境变量映射（用于生成.env文件）
    env_api_base_name VARCHAR(100) NOT NULL,  -- 如: OPENAI_API_BASE, DEEPSEEK_API_BASE
    env_api_key_name VARCHAR(100) NOT NULL,   -- 如: OPENAI_API_KEY, DEEPSEEK_API_KEY
    
    -- 配置用途（支持一个服务商多套配置）
    config_purpose VARCHAR(50) DEFAULT 'default',  -- default, chat, embedding, reasoner
    
    -- 配置优先级（同一服务商多套配置时使用）
    priority INTEGER DEFAULT 0,
    
    -- 配置说明
    description TEXT,
    
    -- 状态管理
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束：同一服务商+用途只能有一套激活的配置
    UNIQUE(provider_id, config_purpose, is_active)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_api_configs_provider ON aistock_llm_api_configs(provider_id);
CREATE INDEX IF NOT EXISTS idx_api_configs_active ON aistock_llm_api_configs(is_active);
CREATE INDEX IF NOT EXISTS idx_api_configs_purpose ON aistock_llm_api_configs(config_purpose);

-- 2. 修改服务商表：添加默认环境变量前缀
ALTER TABLE aistock_llm_providers 
ADD COLUMN IF NOT EXISTS default_env_prefix VARCHAR(50);

-- 更新现有服务商的环境变量前缀
UPDATE aistock_llm_providers SET default_env_prefix = 'openai' WHERE provider_name = 'openai';
UPDATE aistock_llm_providers SET default_env_prefix = 'deepseek' WHERE provider_name = 'deepseek';
UPDATE aistock_llm_providers SET default_env_prefix = 'dashscope' WHERE provider_name = 'dashscope';
UPDATE aistock_llm_providers SET default_env_prefix = 'anthropic' WHERE provider_name = 'anthropic';

-- 3. 修改模型表：添加API配置关联
ALTER TABLE aistock_llm_models 
ADD COLUMN IF NOT EXISTS api_config_id INTEGER REFERENCES aistock_llm_api_configs(id) ON DELETE SET NULL;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_models_api_config ON aistock_llm_models(api_config_id);

-- 4. 添加注释
COMMENT ON TABLE aistock_llm_api_configs IS 'LLM服务商API配置表，支持多服务商、多用途的API配置管理';
COMMENT ON COLUMN aistock_llm_api_configs.env_api_base_name IS '环境变量名称（API Base），如：OPENAI_API_BASE';
COMMENT ON COLUMN aistock_llm_api_configs.env_api_key_name IS '环境变量名称（API Key），如：OPENAI_API_KEY';
COMMENT ON COLUMN aistock_llm_api_configs.config_purpose IS '配置用途：default/chat/embedding/reasoner';
COMMENT ON COLUMN aistock_llm_providers.default_env_prefix IS '默认环境变量前缀，用于自动生成环境变量名';
COMMENT ON COLUMN aistock_llm_models.api_config_id IS '关联的API配置ID，用于确定模型使用哪套API配置';
