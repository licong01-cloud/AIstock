# LLM模型配置完整设计方案

## 一、问题汇总与解决方案

### 1.1 所有场景分析

| 场景 | 需求 | 前缀组合 | 是否支持 | 解决方案 |
|------|------|----------|----------|----------|
| A | 硅基流动GLM(coding) + 阿里云百炼GLM(假设) | `openai/` + `dashscope/` | ✅ 支持 | 前缀不同，环境变量不同 |
| B | 硅基流动GLM + 硅基流动DeepSeek | `openai/` + `openai/` | ✅ 支持 | 同一服务商，共用环境变量 |
| C | 阿里云百炼GLM + 阿里云百炼Qwen | `dashscope/` + `dashscope/` | ✅ 支持 | 同一服务商，共用环境变量 |
| D | 多个Anthropic代理商 | `anthropic/` + `anthropic/` | ❌ 冲突 | 需特殊处理 |
| E | DeepSeek官方 + 硅基流动DeepSeek | `deepseek/` + `openai/` | ✅ 支持 | 前缀不同，环境变量不同 |
| F | 智谱AI官方GLM + 阿里云百炼GLM | `zai/` + `dashscope/` | ✅ 支持 | 前缀不同，环境变量不同 |

---

## 二、Anthropic多代理商配置方案

### 2.1 问题分析

LiteLLM的`anthropic/`前缀固定读取`ANTHROPIC_API_KEY`和`ANTHROPIC_API_BASE`。

**冲突场景**：
```
代理商A: anthropic/claude-sonnet-4-5 → 读取ANTHROPIC_API_KEY (代理商A的key)
代理商B: anthropic/claude-sonnet-4-5 → 也读取ANTHROPIC_API_KEY (但需要代理商B的key)
```

### 2.2 解决方案

#### 方案A：使用OpenAI兼容API（推荐）

很多Anthropic代理商提供OpenAI兼容API，可以使用`openai/`前缀：

```bash
# 代理商A（使用OpenAI兼容API）
OPENAI_API_BASE=https://agent-a.com/v1
OPENAI_API_KEY=sk-agent-a-key

# 模型调用
openai/claude-sonnet-4-5
```

**限制**：同一时间只能有一个服务商使用`openai/`前缀。

#### 方案B：使用LiteLLM Proxy

```yaml
# config.yaml
model_list:
  - model_name: claude-agent-a
    litellm_params:
      model: anthropic/claude-sonnet-4-5
      api_base: https://agent-a.com/v1
      api_key: os.environ/AGENT_A_API_KEY

  - model_name: claude-agent-b
    litellm_params:
      model: anthropic/claude-sonnet-4-5
      api_base: https://agent-b.com/v1
      api_key: os.environ/AGENT_B_API_KEY
```

```bash
# 使用
LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "litellm_proxy/claude-agent-a"},
  "feedback": {"model": "litellm_proxy/claude-agent-b"}
}'
```

#### 方案C：数据库层面区分（推荐）

在数据库中为每个代理商创建独立的服务商记录，使用不同的环境变量名：

| 服务商 | litellm_prefix | 环境变量 |
|--------|----------------|----------|
| Anthropic官方 | anthropic | ANTHROPIC_API_KEY |
| Anthropic代理商A | openai | AGENT_A_API_KEY (写入OPENAI_API_KEY) |
| Anthropic代理商B | openai | AGENT_B_API_KEY (需Proxy或切换) |

---

## 三、硅基流动GLM + 阿里云百炼GLM 配置方案

### 3.1 配置示例

**需求**：
- coding阶段：硅基流动的GLM-4-9B
- 假设(hypothesis)阶段：阿里云百炼的GLM-4-Plus

**配置方式**：

```bash
# .env
# 硅基流动
OPENAI_API_BASE=https://api.siliconflow.cn/v1
OPENAI_API_KEY=sk-siliconflow-xxx

# 阿里云百炼
DASHSCOPE_API_BASE=https://dashscope.aliyuncs.com/compatible-mode/v1
DASHSCOPE_API_KEY=sk-dashscope-yyy

# 阶段映射
LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "openai/THUDM/glm-4-9b-0414", "temperature": "0.7"},
  "hypothesis": {"model": "dashscope/glm-4-plus", "temperature": "0.8"},
  "feedback": {"model": "dashscope/qwen-turbo", "temperature": "0.5"},
  "default": {"model": "deepseek/deepseek-chat", "temperature": "0.7"}
}'
```

### 3.2 数据库配置

```sql
-- 服务商
INSERT INTO aistock_llm_providers (provider_name, display_name, litellm_prefix, api_base_url) VALUES
('siliconflow', '硅基流动', 'openai', 'https://api.siliconflow.cn/v1'),
('dashscope', '阿里云百炼', 'dashscope', 'https://dashscope.aliyuncs.com/compatible-mode/v1');

-- 模型（同一模型名可由多个服务商提供）
INSERT INTO aistock_llm_models (provider_id, model_name, display_name, full_model_id) VALUES
-- 硅基流动的GLM
((SELECT id FROM aistock_llm_providers WHERE provider_name='siliconflow'), 
 'THUDM/glm-4-9b-0414', 'GLM-4-9B (硅基流动)', 'openai/THUDM/glm-4-9b-0414'),
-- 阿里云百炼的GLM
((SELECT id FROM aistock_llm_providers WHERE provider_name='dashscope'), 
 'glm-4-plus', 'GLM-4-Plus (阿里云百炼)', 'dashscope/glm-4-plus');

-- 阶段映射
INSERT INTO aistock_llm_stage_mappings (stage_name, model_id, temperature) VALUES
('coding', (SELECT id FROM aistock_llm_models WHERE full_model_id='openai/THUDM/glm-4-9b-0414'), 0.7),
('hypothesis', (SELECT id FROM aistock_llm_models WHERE full_model_id='dashscope/glm-4-plus'), 0.8);
```

### 3.3 调用流程

```
coding阶段:
  full_model_id = "openai/THUDM/glm-4-9b-0414"
      ↓
  LiteLLM解析前缀 "openai"
      ↓
  读取 OPENAI_API_KEY + OPENAI_API_BASE
      ↓
  请求发送到 https://api.siliconflow.cn/v1

hypothesis阶段:
  full_model_id = "dashscope/glm-4-plus"
      ↓
  LiteLLM解析前缀 "dashscope"
      ↓
  读取 DASHSCOPE_API_KEY + DASHSCOPE_API_BASE
      ↓
  请求发送到 https://dashscope.aliyuncs.com/...
```

---

## 四、完整数据库设计方案

### 4.1 表结构

```sql
-- 服务商表
CREATE TABLE aistock_llm_providers (
    id BIGSERIAL PRIMARY KEY,
    provider_name VARCHAR(100) NOT NULL UNIQUE,  -- 服务商标识
    display_name VARCHAR(200) NOT NULL,          -- 显示名称
    provider_type VARCHAR(50),                    -- 类型：official/agent/proxy
    litellm_prefix VARCHAR(50) NOT NULL,         -- LiteLLM前缀
    api_base_url VARCHAR(500),                    -- 默认API Base
    default_env_prefix VARCHAR(50),               -- 默认环境变量前缀
    supports_chat BOOLEAN DEFAULT FALSE,
    supports_embedding BOOLEAN DEFAULT FALSE,
    supports_reasoner BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE
);

-- 模型表
CREATE TABLE aistock_llm_models (
    id BIGSERIAL PRIMARY KEY,
    provider_id BIGINT NOT NULL REFERENCES aistock_llm_providers(id),
    model_name VARCHAR(200) NOT NULL,             -- 服务商侧模型名
    display_name VARCHAR(200) NOT NULL,           -- 显示名称（含服务商）
    full_model_id VARCHAR(300) NOT NULL UNIQUE,   -- LiteLLM完整ID
    model_type VARCHAR(50),                        -- chat/embedding/reasoner
    model_category VARCHAR(100),                   -- 分类
    api_config_id BIGINT REFERENCES aistock_llm_api_configs(id),
    is_active BOOLEAN DEFAULT TRUE
);

-- API配置表
CREATE TABLE aistock_llm_api_configs (
    id SERIAL PRIMARY KEY,
    provider_id INTEGER NOT NULL REFERENCES aistock_llm_providers(id),
    api_base VARCHAR(500) NOT NULL,
    api_key VARCHAR(500) NOT NULL,
    env_api_base_name VARCHAR(100),               -- 环境变量名（API Base）
    env_api_key_name VARCHAR(100),                -- 环境变量名（API Key）
    config_purpose VARCHAR(50) DEFAULT 'default', -- default/chat/embedding/reasoner
    is_active BOOLEAN DEFAULT TRUE
);

-- 阶段映射表
CREATE TABLE aistock_llm_stage_mappings (
    id BIGSERIAL PRIMARY KEY,
    stage_name VARCHAR(50) NOT NULL UNIQUE,
    model_id BIGINT REFERENCES aistock_llm_models(id),
    temperature DECIMAL(3, 2),
    max_tokens INT,
    is_active BOOLEAN DEFAULT TRUE
);
```

### 4.2 完整示例数据

```sql
-- ============================================
-- 服务商数据
-- ============================================
INSERT INTO aistock_llm_providers 
(provider_name, display_name, provider_type, litellm_prefix, api_base_url, default_env_prefix, supports_chat, supports_embedding, supports_reasoner) VALUES
-- 官方服务商
('deepseek', 'DeepSeek官方', 'official', 'deepseek', 'https://api.deepseek.com', 'DEEPSEEK', true, false, true),
('anthropic', 'Anthropic官方', 'official', 'anthropic', 'https://api.anthropic.com', 'ANTHROPIC', true, false, true),
('zhipu', '智谱AI官方', 'official', 'zai', 'https://open.bigmodel.cn/api/paas/v4', 'ZAI', true, false, false),

-- 平台服务商（有专属前缀）
('dashscope', '阿里云百炼', 'platform', 'dashscope', 'https://dashscope.aliyuncs.com/compatible-mode/v1', 'DASHSCOPE', true, true, false),

-- OpenAI兼容服务商（共用openai前缀）
('siliconflow', '硅基流动', 'agent', 'openai', 'https://api.siliconflow.cn/v1', 'OPENAI', true, true, false),
('openai', 'OpenAI官方', 'official', 'openai', 'https://api.openai.com/v1', 'OPENAI', true, true, false),

-- Anthropic代理商（使用openai兼容API）
('anthropic_agent_a', 'Claude代理商A', 'agent', 'openai', 'https://agent-a.com/v1', 'OPENAI', true, false, true);

-- ============================================
-- API配置数据
-- ============================================
INSERT INTO aistock_llm_api_configs 
(provider_id, api_base, api_key, env_api_base_name, env_api_key_name, config_purpose) VALUES
-- DeepSeek官方
((SELECT id FROM aistock_llm_providers WHERE provider_name='deepseek'), 
 'https://api.deepseek.com', 'sk-deepseek-xxx', 'DEEPSEEK_API_BASE', 'DEEPSEEK_API_KEY', 'default'),
-- 阿里云百炼
((SELECT id FROM aistock_llm_providers WHERE provider_name='dashscope'), 
 'https://dashscope.aliyuncs.com/compatible-mode/v1', 'sk-dashscope-xxx', 'DASHSCOPE_API_BASE', 'DASHSCOPE_API_KEY', 'default'),
-- 智谱AI官方
((SELECT id FROM aistock_llm_providers WHERE provider_name='zhipu'), 
 'https://open.bigmodel.cn/api/paas/v4', 'sk-zhipu-xxx', 'ZAI_API_BASE', 'ZAI_API_KEY', 'default'),
-- 硅基流动
((SELECT id FROM aistock_llm_providers WHERE provider_name='siliconflow'), 
 'https://api.siliconflow.cn/v1', 'sk-siliconflow-xxx', 'OPENAI_API_BASE', 'OPENAI_API_KEY', 'default'),
-- Anthropic官方
((SELECT id FROM aistock_llm_providers WHERE provider_name='anthropic'), 
 'https://api.anthropic.com', 'sk-anthropic-xxx', 'ANTHROPIC_API_BASE', 'ANTHROPIC_API_KEY', 'default');

-- ============================================
-- 模型数据（同一模型可由多个服务商提供）
-- ============================================
INSERT INTO aistock_llm_models 
(provider_id, model_name, display_name, full_model_id, model_type, api_config_id) VALUES
-- DeepSeek官方的模型
((SELECT id FROM aistock_llm_providers WHERE provider_name='deepseek'), 
 'deepseek-chat', 'DeepSeek Chat', 'deepseek/deepseek-chat', 'chat', 
 (SELECT id FROM aistock_llm_api_configs WHERE provider_id=(SELECT id FROM aistock_llm_providers WHERE provider_name='deepseek'))),

-- 硅基流动的模型
((SELECT id FROM aistock_llm_providers WHERE provider_name='siliconflow'), 
 'THUDM/glm-4-9b-0414', 'GLM-4-9B (硅基流动)', 'openai/THUDM/glm-4-9b-0414', 'chat',
 (SELECT id FROM aistock_llm_api_configs WHERE provider_id=(SELECT id FROM aistock_llm_providers WHERE provider_name='siliconflow'))),
((SELECT id FROM aistock_llm_providers WHERE provider_name='siliconflow'), 
 'deepseek-ai/DeepSeek-V3', 'DeepSeek-V3 (硅基流动)', 'openai/deepseek-ai/DeepSeek-V3', 'chat',
 (SELECT id FROM aistock_llm_api_configs WHERE provider_id=(SELECT id FROM aistock_llm_providers WHERE provider_name='siliconflow'))),

-- 阿里云百炼的模型
((SELECT id FROM aistock_llm_providers WHERE provider_name='dashscope'), 
 'qwen-turbo', 'Qwen Turbo (阿里云百炼)', 'dashscope/qwen-turbo', 'chat',
 (SELECT id FROM aistock_llm_api_configs WHERE provider_id=(SELECT id FROM aistock_llm_providers WHERE provider_name='dashscope'))),
((SELECT id FROM aistock_llm_providers WHERE provider_name='dashscope'), 
 'glm-4-plus', 'GLM-4-Plus (阿里云百炼)', 'dashscope/glm-4-plus', 'chat',
 (SELECT id FROM aistock_llm_api_configs WHERE provider_id=(SELECT id FROM aistock_llm_providers WHERE provider_name='dashscope'))),
((SELECT id FROM aistock_llm_providers WHERE provider_name='dashscope'), 
 'deepseek-v3', 'DeepSeek-V3 (阿里云百炼)', 'dashscope/deepseek-v3', 'chat',
 (SELECT id FROM aistock_llm_api_configs WHERE provider_id=(SELECT id FROM aistock_llm_providers WHERE provider_name='dashscope'))),

-- 智谱AI官方的模型
((SELECT id FROM aistock_llm_providers WHERE provider_name='zhipu'), 
 'glm-4.7', 'GLM-4.7 (智谱AI官方)', 'zai/glm-4.7', 'chat',
 (SELECT id FROM aistock_llm_api_configs WHERE provider_id=(SELECT id FROM aistock_llm_providers WHERE provider_name='zhipu'))),

-- Anthropic官方的模型
((SELECT id FROM aistock_llm_providers WHERE provider_name='anthropic'), 
 'claude-sonnet-4-5-20250929', 'Claude Sonnet 4.5', 'anthropic/claude-sonnet-4-5-20250929', 'chat',
 (SELECT id FROM aistock_llm_api_configs WHERE provider_id=(SELECT id FROM aistock_llm_providers WHERE provider_name='anthropic')));

-- ============================================
-- 阶段映射数据
-- ============================================
INSERT INTO aistock_llm_stage_mappings (stage_name, model_id, temperature, max_tokens) VALUES
('coding', (SELECT id FROM aistock_llm_models WHERE full_model_id='openai/THUDM/glm-4-9b-0414'), 0.7, 5000),
('hypothesis', (SELECT id FROM aistock_llm_models WHERE full_model_id='dashscope/glm-4-plus'), 0.8, 6000),
('feedback', (SELECT id FROM aistock_llm_models WHERE full_model_id='dashscope/qwen-turbo'), 0.5, 5000),
('default', (SELECT id FROM aistock_llm_models WHERE full_model_id='deepseek/deepseek-chat'), 0.7, 4000);
```

---

## 五、环境变量生成规则

### 5.1 规则定义

```python
# AIstock生成.env的逻辑（伪代码）

def generate_env_content(stage_mappings):
    env_vars = {}
    model_map = {}
    
    for mapping in stage_mappings:
        model = get_model(mapping.model_id)
        provider = get_provider(model.provider_id)
        api_config = get_api_config(model.api_config_id)
        
        # 1. 添加阶段-模型映射
        model_map[mapping.stage_name] = {
            "model": model.full_model_id,
            "temperature": str(mapping.temperature),
            "max_tokens": str(mapping.max_tokens)
        }
        
        # 2. 添加API凭证环境变量
        # 根据litellm_prefix确定环境变量名
        prefix = provider.litellm_prefix.upper()
        
        # 特殊处理：openai前缀的服务商都写入OPENAI_API_KEY
        if provider.litellm_prefix == 'openai':
            env_vars['OPENAI_API_BASE'] = api_config.api_base
            env_vars['OPENAI_API_KEY'] = api_config.api_key
        else:
            # 使用服务商专属环境变量
            env_key = f"{prefix}_API_KEY"
            env_base = f"{prefix}_API_BASE"
            env_vars[env_key] = api_config.api_key
            env_vars[env_base] = api_config.api_base
    
    # 3. 生成LITELLM_CHAT_MODEL_MAP
    env_vars['LITELLM_CHAT_MODEL_MAP'] = json.dumps(model_map, ensure_ascii=False)
    
    return env_vars
```

### 5.2 冲突检测规则

```python
def detect_conflicts(stage_mappings):
    openai_providers = set()
    
    for mapping in stage_mappings:
        model = get_model(mapping.model_id)
        provider = get_provider(model.provider_id)
        
        if provider.litellm_prefix == 'openai':
            openai_providers.add(provider.provider_name)
    
    if len(openai_providers) > 1:
        return {
            "has_conflict": True,
            "message": f"多个服务商使用openai前缀: {openai_providers}，将产生冲突",
            "suggestion": "建议使用LiteLLM Proxy或只选择一个openai前缀的服务商"
        }
    
    return {"has_conflict": False}
```

---

## 六、支持矩阵总结

### 6.1 完全支持的场景

| 场景 | 配置 | 说明 |
|------|------|------|
| 不同服务商的同一模型 | 硅基流动GLM + 阿里云百炼GLM | 前缀不同，完全支持 |
| 不同服务商的不同模型 | 硅基流动GLM + DeepSeek官方 | 前缀不同，完全支持 |
| 同一服务商的不同模型 | 阿里云百炼Qwen + 阿里云百炼GLM | 共用环境变量，支持 |
| 官方 + 第三方代理 | DeepSeek官方 + 硅基流动DeepSeek | 前缀不同，完全支持 |

### 6.2 需要特殊处理的场景

| 场景 | 问题 | 解决方案 |
|------|------|----------|
| 多个OpenAI兼容服务商 | 共用OPENAI_API_KEY | 使用LiteLLM Proxy |
| 多个Anthropic代理商 | 共用ANTHROPIC_API_KEY | 使用LiteLLM Proxy或OpenAI兼容API |

### 6.3 环境变量映射表

| litellm_prefix | 环境变量KEY | 环境变量BASE | 可共存数量 |
|-----------------|-------------|--------------|------------|
| `openai` | OPENAI_API_KEY | OPENAI_API_BASE | 1个（冲突风险）|
| `deepseek` | DEEPSEEK_API_KEY | DEEPSEEK_API_BASE | 无限 |
| `dashscope` | DASHSCOPE_API_KEY | DASHSCOPE_API_BASE | 无限 |
| `zai` | ZAI_API_KEY | ZAI_API_BASE | 无限 |
| `anthropic` | ANTHROPIC_API_KEY | ANTHROPIC_API_BASE | 1个（冲突风险）|

---

## 七、最终配置示例

### 7.1 场景：硅基流动GLM(coding) + 阿里云百炼GLM(hypothesis) + DeepSeek官方(default)

**生成的.env**：

```bash
# 硅基流动（openai前缀）
OPENAI_API_BASE=https://api.siliconflow.cn/v1
OPENAI_API_KEY=sk-siliconflow-xxx

# 阿里云百炼（dashscope前缀）
DASHSCOPE_API_BASE=https://dashscope.aliyuncs.com/compatible-mode/v1
DASHSCOPE_API_KEY=sk-dashscope-yyy

# DeepSeek官方（deepseek前缀）
DEEPSEEK_API_BASE=https://api.deepseek.com
DEEPSEEK_API_KEY=sk-deepseek-zzz

# 阶段映射
LITELLM_CHAT_MODEL_MAP='{"coding":{"model":"openai/THUDM/glm-4-9b-0414","temperature":"0.7","max_tokens":"5000"},"hypothesis":{"model":"dashscope/glm-4-plus","temperature":"0.8","max_tokens":"6000"},"default":{"model":"deepseek/deepseek-chat","temperature":"0.7","max_tokens":"4000"}}'
```

**调用流程**：

```
coding:     openai/THUDM/glm-4-9b-0414 → OPENAI_API_KEY → 硅基流动
hypothesis: dashscope/glm-4-plus       → DASHSCOPE_API_KEY → 阿里云百炼
default:    deepseek/deepseek-chat     → DEEPSEEK_API_KEY → DeepSeek官方
```

---

## 八、实施建议

### 8.1 数据库数据修正

1. **修正服务商表**：确保`litellm_prefix`与LiteLLM官方一致
2. **修正模型表**：确保`full_model_id`格式正确
3. **修正API配置表**：确保环境变量名与LiteLLM映射一致

### 8.2 前端配置页面

1. **模型选择**：显示"服务商 + 模型名"组合
2. **冲突检测**：检测多个openai/anthropic前缀的服务商
3. **配置预览**：显示将要生成的.env配置

### 8.3 不需要修改代码

- RDAgent：完全依赖LiteLLM内部机制
- LiteLLM：已内置支持所有服务商
- AIstock：只需修正数据库配置数据
