# 多服务商提供相同模型的配置方案

## 一、问题场景

| 场景 | 服务商 | 提供的模型 | LiteLLM前缀 |
|------|--------|------------|-------------|
| A | 硅基流动 | GLM, DeepSeek, Qwen等 | `openai/` |
| B | 阿里云百炼 | GLM, DeepSeek, Qwen等 | `dashscope/` |
| C | DeepSeek官方 | DeepSeek系列 | `deepseek/` |
| D | 智谱AI官方 | GLM系列 | `zai/` |

**核心问题**：同一模型（如GLM-4.7）可由多个服务商提供，如何配置才能共存？

---

## 二、LiteLLM前缀机制分析

### 2.1 前缀决定服务商，不是模型名

**关键理解**：LiteLLM通过前缀识别服务商，不是通过模型名。

```
模型字符串格式: <服务商前缀>/<模型名>

dashscope/glm-4.7  → 阿里云百炼的GLM-4.7
zai/glm-4.7        → 智谱AI官方的GLM-4.7
openai/glm-4.7     → OpenAI兼容API的GLM-4.7（如硅基流动）
```

### 2.2 环境变量由前缀决定

| 前缀 | 环境变量KEY | 环境变量BASE | 服务商 |
|------|-------------|--------------|--------|
| `dashscope/` | DASHSCOPE_API_KEY | DASHSCOPE_API_BASE | 阿里云百炼 |
| `zai/` | ZAI_API_KEY | ZAI_API_BASE | 智谱AI官方 |
| `deepseek/` | DEEPSEEK_API_KEY | DEEPSEEK_API_BASE | DeepSeek官方 |
| `openai/` | OPENAI_API_KEY | OPENAI_API_BASE | OpenAI兼容服务商 |

---

## 三、配置方案

### 3.1 场景：使用硅基流动的GLM和DeepSeek

**硅基流动特点**：
- 提供OpenAI兼容API
- LiteLLM无专属前缀
- 必须使用`openai/`前缀

**配置方式**：

```bash
# .env
OPENAI_API_KEY=sk-siliconflow-key
OPENAI_API_BASE=https://api.siliconflow.cn/v1

# LITELLM_CHAT_MODEL_MAP
LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "openai/THUDM/glm-4-9b-0414"},
  "feedback": {"model": "openai/deepseek-ai/DeepSeek-V3"}
}'
```

**数据库配置**：

| 服务商 | provider_name | litellm_prefix | api_base_url |
|--------|---------------|----------------|--------------|
| 硅基流动 | siliconflow | openai | https://api.siliconflow.cn/v1 |

| 模型 | model_name | full_model_id | provider |
|------|------------|---------------|----------|
| GLM-4-9B | THUDM/glm-4-9b-0414 | openai/THUDM/glm-4-9b-0414 | siliconflow |
| DeepSeek-V3 | deepseek-ai/DeepSeek-V3 | openai/deepseek-ai/DeepSeek-V3 | siliconflow |

### 3.2 场景：使用阿里云百炼的GLM和DeepSeek

**阿里云百炼特点**：
- LiteLLM有专属前缀`dashscope/`
- 可与OpenAI兼容服务商共存

**配置方式**：

```bash
# .env
DASHSCOPE_API_KEY=sk-dashscope-key
DASHSCOPE_API_BASE=https://dashscope.aliyuncs.com/compatible-mode/v1

# LITELLM_CHAT_MODEL_MAP
LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "dashscope/glm-4-plus"},
  "feedback": {"model": "dashscope/deepseek-v3"}
}'
```

**数据库配置**：

| 服务商 | provider_name | litellm_prefix | api_base_url |
|--------|---------------|----------------|--------------|
| 阿里云百炼 | dashscope | dashscope | https://dashscope.aliyuncs.com/compatible-mode/v1 |

| 模型 | model_name | full_model_id | provider |
|------|------------|---------------|----------|
| GLM-4-Plus | glm-4-plus | dashscope/glm-4-plus | dashscope |
| DeepSeek-V3 | deepseek-v3 | dashscope/deepseek-v3 | dashscope |

### 3.3 场景：混合使用多个服务商

**需求**：coding阶段用硅基流动的GLM，feedback阶段用阿里云百炼的Qwen

**配置方式**：

```bash
# .env - 两个服务商的环境变量共存
OPENAI_API_KEY=sk-siliconflow-key
OPENAI_API_BASE=https://api.siliconflow.cn/v1

DASHSCOPE_API_KEY=sk-dashscope-key
DASHSCOPE_API_BASE=https://dashscope.aliyuncs.com/compatible-mode/v1

# LITELLM_CHAT_MODEL_MAP
LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "openai/THUDM/glm-4-9b-0414"},
  "feedback": {"model": "dashscope/qwen-turbo"}
}'
```

**关键**：`openai/`和`dashscope/`使用不同的环境变量，**不会冲突**。

---

## 四、组合支持矩阵

### 4.1 可共存的组合

| 组合 | 前缀1 | 前缀2 | 是否冲突 | 说明 |
|------|-------|-------|----------|------|
| 硅基流动 + 阿里云百炼 | `openai/` | `dashscope/` | ✅ 不冲突 | 环境变量不同 |
| 硅基流动 + DeepSeek官方 | `openai/` | `deepseek/` | ✅ 不冲突 | 环境变量不同 |
| 硅基流动 + 智谱AI官方 | `openai/` | `zai/` | ✅ 不冲突 | 环境变量不同 |
| 阿里云百炼 + DeepSeek官方 | `dashscope/` | `deepseek/` | ✅ 不冲突 | 环境变量不同 |
| 阿里云百炼 + 智谱AI官方 | `dashscope/` | `zai/` | ✅ 不冲突 | 环境变量不同 |

### 4.2 会冲突的组合

| 组合 | 前缀1 | 前缀2 | 是否冲突 | 说明 |
|------|-------|-------|----------|------|
| 硅基流动 + 另一个OpenAI兼容服务商 | `openai/` | `openai/` | ❌ 冲突 | 共用OPENAI_API_KEY |
| 硅基流动 + OpenAI官方 | `openai/` | `openai/` | ❌ 冲突 | 共用OPENAI_API_KEY |

---

## 五、数据库设计建议

### 5.1 服务商表设计

```sql
-- 关键字段
provider_name VARCHAR(100)    -- 服务商标识
litellm_prefix VARCHAR(50)    -- LiteLLM前缀
api_base_url VARCHAR(500)     -- API基础URL
default_env_prefix VARCHAR(50) -- 默认环境变量前缀
```

**示例数据**：

| provider_name | litellm_prefix | api_base_url | default_env_prefix |
|---------------|----------------|--------------|-------------------|
| siliconflow | openai | https://api.siliconflow.cn/v1 | OPENAI |
| dashscope | dashscope | https://dashscope.aliyuncs.com/... | DASHSCOPE |
| deepseek | deepseek | https://api.deepseek.com | DEEPSEEK |
| zhipu | zai | https://open.bigmodel.cn/api/... | ZAI |

### 5.2 模型表设计

```sql
-- 关键字段
provider_id BIGINT           -- 所属服务商
model_name VARCHAR(200)      -- 模型名（服务商侧）
full_model_id VARCHAR(300)   -- LiteLLM完整ID
```

**示例数据**：

| provider_id | model_name | full_model_id |
|-------------|------------|---------------|
| siliconflow | THUDM/glm-4-9b-0414 | openai/THUDM/glm-4-9b-0414 |
| dashscope | glm-4-plus | dashscope/glm-4-plus |
| zhipu | glm-4.7 | zai/glm-4.7 |
| siliconflow | deepseek-ai/DeepSeek-V3 | openai/deepseek-ai/DeepSeek-V3 |
| dashscope | deepseek-v3 | dashscope/deepseek-v3 |
| deepseek | deepseek-chat | deepseek/deepseek-chat |

### 5.3 关键规则

1. **同一模型可由多个服务商提供**：每个服务商一条记录
2. **full_model_id由服务商前缀+模型名组成**
3. **用户选择模型时，实际选择的是"服务商+模型"组合**

---

## 六、完整配置示例

### 6.1 需求：coding用硅基流动GLM，feedback用阿里云百炼Qwen，default用DeepSeek官方

**数据库配置**：

```sql
-- 服务商
INSERT INTO aistock_llm_providers (provider_name, litellm_prefix, api_base_url, default_env_prefix) VALUES
('siliconflow', 'openai', 'https://api.siliconflow.cn/v1', 'OPENAI'),
('dashscope', 'dashscope', 'https://dashscope.aliyuncs.com/compatible-mode/v1', 'DASHSCOPE'),
('deepseek', 'deepseek', 'https://api.deepseek.com', 'DEEPSEEK');

-- 模型
INSERT INTO aistock_llm_models (provider_id, model_name, full_model_id) VALUES
((SELECT id FROM aistock_llm_providers WHERE provider_name='siliconflow'), 'THUDM/glm-4-9b-0414', 'openai/THUDM/glm-4-9b-0414'),
((SELECT id FROM aistock_llm_providers WHERE provider_name='dashscope'), 'qwen-turbo', 'dashscope/qwen-turbo'),
((SELECT id FROM aistock_llm_providers WHERE provider_name='deepseek'), 'deepseek-chat', 'deepseek/deepseek-chat');

-- API配置
INSERT INTO aistock_llm_api_configs (provider_id, api_base, api_key, env_api_base_name, env_api_key_name) VALUES
((SELECT id FROM aistock_llm_providers WHERE provider_name='siliconflow'), 'https://api.siliconflow.cn/v1', 'sk-sf-xxx', 'OPENAI_API_BASE', 'OPENAI_API_KEY'),
((SELECT id FROM aistock_llm_providers WHERE provider_name='dashscope'), 'https://dashscope.aliyuncs.com/compatible-mode/v1', 'sk-ds-yyy', 'DASHSCOPE_API_BASE', 'DASHSCOPE_API_KEY'),
((SELECT id FROM aistock_llm_providers WHERE provider_name='deepseek'), 'https://api.deepseek.com', 'sk-dk-zzz', 'DEEPSEEK_API_BASE', 'DEEPSEEK_API_KEY');

-- 阶段映射
INSERT INTO aistock_llm_stage_mappings (stage_name, model_id) VALUES
('coding', (SELECT id FROM aistock_llm_models WHERE full_model_id='openai/THUDM/glm-4-9b-0414')),
('feedback', (SELECT id FROM aistock_llm_models WHERE full_model_id='dashscope/qwen-turbo')),
('default', (SELECT id FROM aistock_llm_models WHERE full_model_id='deepseek/deepseek-chat'));
```

**生成的.env**：

```bash
OPENAI_API_BASE=https://api.siliconflow.cn/v1
OPENAI_API_KEY=sk-sf-xxx

DASHSCOPE_API_BASE=https://dashscope.aliyuncs.com/compatible-mode/v1
DASHSCOPE_API_KEY=sk-ds-yyy

DEEPSEEK_API_BASE=https://api.deepseek.com
DEEPSEEK_API_KEY=sk-dk-zzz

LITELLM_CHAT_MODEL_MAP='{"coding":{"model":"openai/THUDM/glm-4-9b-0414"},"feedback":{"model":"dashscope/qwen-turbo"},"default":{"model":"deepseek/deepseek-chat"}}'
```

---

## 七、总结

### 7.1 核心结论

| 问题 | 答案 |
|------|------|
| 硅基流动的GLM/DeepSeek如何配置？ | 使用`openai/`前缀，设置OPENAI_API_BASE为硅基流动地址 |
| 阿里云百炼的GLM/DeepSeek如何配置？ | 使用`dashscope/`前缀，设置DASHSCOPE_API_KEY |
| 这几种组合能共存吗？ | **可以**，只要前缀不同就不会冲突 |

### 7.2 共存规则

```
✅ 可共存：
  openai/ (硅基流动) + dashscope/ (阿里云百炼) + deepseek/ (DeepSeek官方) + zai/ (智谱AI)

❌ 不可共存：
  openai/ (硅基流动) + openai/ (另一个OpenAI兼容服务商)
```

### 7.3 数据库设计原则

1. **服务商 = API提供方**，不是模型开发者
2. **同一模型可由多个服务商提供**
3. **full_model_id = litellm_prefix + model_name**
4. **环境变量名由litellm_prefix决定**
