# LLM模型配置架构改造方案

## 一、核心问题解答

### 1.1 是否还需要LiteLLM Proxy？

**答案：对于当前需求，不需要**

| 场景 | 是否需要Proxy | 说明 |
|------|--------------|------|
| 使用LiteLLM已支持的服务商专属前缀 | **不需要** | 如`dashscope/`、`zai/`、`deepseek/` |
| 使用多个OpenAI兼容服务商且无专属前缀 | **需要** | 如多个第三方代理都只能用`openai/`前缀 |
| 需要负载均衡、成本追踪 | **需要** | 企业级功能 |
| 简单的多服务商并存 | **不需要** | 使用专属前缀即可 |

### 1.2 OpenAI兼容模型的环境变量限制

**答案：只有使用`openai/`前缀的模型才有此限制**

| 前缀类型 | 环境变量 | 限制 |
|----------|----------|------|
| `openai/` | OPENAI_API_KEY, OPENAI_API_BASE | **所有OpenAI兼容服务商共用** |
| `dashscope/` | DASHSCOPE_API_KEY | 阿里云百炼专属 |
| `zai/` | ZAI_API_KEY | 智谱AI专属 |
| `deepseek/` | DEEPSEEK_API_KEY | DeepSeek专属 |
| `anthropic/` | ANTHROPIC_API_KEY | Claude专属 |

**关键发现**：LiteLLM已为阿里云百炼、智谱AI等中国服务商提供专属前缀，**不存在环境变量冲突问题**。

### 1.3 是否支持不同阶段使用不同服务商的不同OpenAI兼容模型？

**答案：支持，有两种方式**

**方式A：使用服务商专属前缀（推荐）**

```bash
LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "dashscope/qwen-turbo"},      # 阿里云百炼
  "feedback": {"model": "zai/glm-4.7"},             # 智谱AI
  "default": {"model": "deepseek/deepseek-chat"}    # DeepSeek官方
}'
```

**方式B：同一服务商的不同模型**

```bash
LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "dashscope/qwen-max"},        # 阿里云百炼 - Qwen Max
  "feedback": {"model": "dashscope/qwen-turbo"},    # 阿里云百炼 - Qwen Turbo
  "default": {"model": "dashscope/qwen-plus"}       # 阿里云百炼 - Qwen Plus
}'
```

---

## 二、当前数据库结构分析

### 2.1 现有表结构

```
aistock_llm_providers
├── id, provider_name, display_name
├── api_base_url (单一URL)
├── litellm_prefix (如 openai, deepseek)
└── supports_chat, supports_embedding, supports_reasoner

aistock_llm_models
├── id, provider_id, model_name, display_name
├── full_model_id (如 openai/glm-4.7)
├── model_type, model_category
└── api_config_id (关联API配置)

aistock_llm_api_configs
├── id, provider_id
├── api_base, api_key
├── env_api_base_name (如 OPENAI_API_BASE)
└── env_api_key_name (如 OPENAI_API_KEY)

aistock_llm_stage_mappings
├── stage_name, model_id
└── temperature, max_tokens
```

### 2.2 当前问题

| 问题 | 表现 |
|------|------|
| **服务商前缀错误** | 硅基流动、阿里云百炼的`litellm_prefix=openai`，导致环境变量冲突 |
| **模型归属错误** | GLM-4.7归属"openai"服务商，实际应归属智谱AI或阿里云百炼 |
| **环境变量命名混乱** | 所有OpenAI兼容模型都写入`OPENAI_API_BASE` |

---

## 三、改造方案（不修改代码）

### 3.1 核心原则

1. **服务商 = 实际提供API的厂商**
2. **litellm_prefix = LiteLLM官方定义的前缀**
3. **每个服务商使用专属环境变量**

### 3.2 服务商分类

| 类型 | 服务商 | litellm_prefix | 环境变量前缀 | 说明 |
|------|--------|----------------|--------------|------|
| **专属前缀** | DeepSeek | `deepseek` | DEEPSEEK_ | 官方专属 |
| **专属前缀** | 阿里云百炼 | `dashscope` | DASHSCOPE_ | 官方专属 |
| **专属前缀** | 智谱AI | `zai` | ZAI_ | 官方专属 |
| **专属前缀** | Anthropic | `anthropic` | ANTHROPIC_ | 官方专属 |
| **OpenAI兼容** | 硅基流动 | `openai` | SILICONFLOW_* | 需特殊处理 |
| **OpenAI兼容** | 第三方代理 | `openai` | CUSTOM_* | 需特殊处理 |

### 3.3 数据库数据改造方案

#### 3.3.1 服务商表数据修正

```sql
-- 修正服务商配置
UPDATE aistock_llm_providers SET 
  litellm_prefix = 'dashscope',
  default_env_prefix = 'DASHSCOPE'
WHERE provider_name = 'dashscope';

UPDATE aistock_llm_providers SET 
  litellm_prefix = 'zai',
  default_env_prefix = 'ZAI'
WHERE provider_name = 'zhipu';

-- 新增智谱AI服务商（如果不存在）
INSERT INTO aistock_llm_providers (provider_name, display_name, litellm_prefix, default_env_prefix, supports_chat)
VALUES ('zhipu', '智谱AI', 'zai', 'ZAI', true)
ON CONFLICT (provider_name) DO UPDATE SET 
  litellm_prefix = 'zai',
  default_env_prefix = 'ZAI';
```

#### 3.3.2 模型表数据修正

```sql
-- 将GLM-4.7归属修正为智谱AI
UPDATE aistock_llm_models SET 
  provider_id = (SELECT id FROM aistock_llm_providers WHERE provider_name = 'zhipu'),
  full_model_id = 'zai/glm-4.7'
WHERE model_name = 'glm-4.7';

-- 将阿里云百炼的Qwen模型使用dashscope前缀
UPDATE aistock_llm_models SET 
  full_model_id = 'dashscope/' || model_name
WHERE provider_id = (SELECT id FROM aistock_llm_providers WHERE provider_name = 'dashscope');
```

#### 3.3.3 API配置表数据修正

```sql
-- 智谱AI的API配置
UPDATE aistock_llm_api_configs SET 
  env_api_base_name = 'ZAI_API_BASE',
  env_api_key_name = 'ZAI_API_KEY'
WHERE provider_id = (SELECT id FROM aistock_llm_providers WHERE provider_name = 'zhipu');

-- 阿里云百炼的API配置
UPDATE aistock_llm_api_configs SET 
  env_api_base_name = 'DASHSCOPE_API_BASE',
  env_api_key_name = 'DASHSCOPE_API_KEY'
WHERE provider_id = (SELECT id FROM aistock_llm_providers WHERE provider_name = 'dashscope');
```

---

## 四、环境变量映射规则

### 4.1 LiteLLM官方前缀-环境变量映射

| litellm_prefix | 环境变量KEY | 环境变量BASE |
|-----------------|-------------|--------------|
| `openai` | OPENAI_API_KEY | OPENAI_API_BASE |
| `deepseek` | DEEPSEEK_API_KEY | DEEPSEEK_API_BASE |
| `dashscope` | DASHSCOPE_API_KEY | DASHSCOPE_API_BASE |
| `zai` | ZAI_API_KEY | ZAI_API_BASE |
| `anthropic` | ANTHROPIC_API_KEY | ANTHROPIC_API_BASE |
| `litellm_proxy` | LITELLM_PROXY_API_KEY | LITELLM_PROXY_API_BASE |

### 4.2 AIstock数据库字段与LiteLLM映射

```
数据库字段:
  providers.litellm_prefix = "dashscope"
  api_configs.env_api_key_name = "DASHSCOPE_API_KEY"
  api_configs.env_api_base_name = "DASHSCOPE_API_BASE"

生成.env:
  DASHSCOPE_API_KEY = <api_key值>
  DASHSCOPE_API_BASE = <api_base值>

模型调用:
  full_model_id = "dashscope/qwen-turbo"
  LiteLLM自动读取 DASHSCOPE_API_KEY
```

---

## 五、完整配置示例

### 5.1 数据库配置

| 服务商 | provider_name | litellm_prefix | default_env_prefix |
|--------|---------------|----------------|-------------------|
| DeepSeek | deepseek | deepseek | DEEPSEEK |
| 阿里云百炼 | dashscope | dashscope | DASHSCOPE |
| 智谱AI | zhipu | zai | ZAI |
| Anthropic | anthropic | anthropic | ANTHROPIC |
| 硅基流动 | siliconflow | openai | SILICONFLOW |

| 模型 | model_name | full_model_id | provider |
|------|------------|---------------|----------|
| DeepSeek Chat | deepseek-chat | deepseek/deepseek-chat | deepseek |
| Qwen Turbo | qwen-turbo | dashscope/qwen-turbo | dashscope |
| GLM-4.7 | glm-4.7 | zai/glm-4.7 | zhipu |
| Claude Sonnet | claude-sonnet-4-5 | anthropic/claude-sonnet-4-5 | anthropic |

### 5.2 生成的.env配置

```bash
# DeepSeek官方
DEEPSEEK_API_KEY=sk-xxx
DEEPSEEK_API_BASE=https://api.deepseek.com

# 阿里云百炼
DASHSCOPE_API_KEY=sk-yyy
DASHSCOPE_API_BASE=https://dashscope.aliyuncs.com/compatible-mode/v1

# 智谱AI
ZAI_API_KEY=sk-zzz
ZAI_API_BASE=https://open.bigmodel.cn/api/paas/v4

# Claude (Anthropic)
ANTHROPIC_API_KEY=sk-www
ANTHROPIC_API_BASE=https://api.anthropic.com

# 阶段-模型映射
LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "dashscope/qwen-turbo", "temperature": "0.7", "max_tokens": "5000"},
  "feedback": {"model": "zai/glm-4.7", "temperature": "0.5", "max_tokens": "5000"},
  "default": {"model": "deepseek/deepseek-chat", "temperature": "0.7", "max_tokens": "4000"}
}'

LITELLM_EMBEDDING_MODEL=dashscope/text-embedding-v3
```

---

## 六、硅基流动等OpenAI兼容服务商的特殊处理

### 6.1 问题

硅基流动等第三方服务商只有OpenAI兼容API，LiteLLM没有为其提供专属前缀。

### 6.2 解决方案

**方案A：使用`openai/`前缀 + 自定义环境变量名**

```sql
-- 数据库配置
INSERT INTO aistock_llm_providers (provider_name, display_name, litellm_prefix, default_env_prefix)
VALUES ('siliconflow', '硅基流动', 'openai', 'SILICONFLOW');

INSERT INTO aistock_llm_api_configs (provider_id, api_base, api_key, env_api_base_name, env_api_key_name)
VALUES (
  (SELECT id FROM aistock_llm_providers WHERE provider_name = 'siliconflow'),
  'https://api.siliconflow.cn/v1',
  'sk-xxx',
  'SILICONFLOW_API_BASE',
  'SILICONFLOW_API_KEY'
);
```

**问题**：LiteLLM的`openai/`前缀固定读取`OPENAI_API_KEY`，不会读取`SILICONFLOW_API_KEY`。

**解决方案**：在AIstock更新.env时，同时写入`OPENAI_API_BASE`和`OPENAI_API_KEY`：

```bash
# 当选择硅基流动的模型时，AIstock写入：
OPENAI_API_BASE=https://api.siliconflow.cn/v1
OPENAI_API_KEY=sk-xxx

# 模型调用
full_model_id = "openai/THUDM/glm-4-9b-0414"
```

**限制**：同一时间只能有一个服务商使用`openai/`前缀。

### 6.3 方案B：使用LiteLLM Proxy（推荐用于多OpenAI兼容服务商）

如果需要同时使用多个OpenAI兼容服务商，部署LiteLLM Proxy：

```yaml
# config.yaml
model_list:
  - model_name: glm-siliconflow
    litellm_params:
      model: openai/THUDM/glm-4-9b-0414
      api_base: https://api.siliconflow.cn/v1
      api_key: os.environ/SILICONFLOW_API_KEY

  - model_name: glm-bailian
    litellm_params:
      model: openai/glm-4
      api_base: https://dashscope.aliyuncs.com/compatible-mode/v1
      api_key: os.environ/DASHSCOPE_API_KEY
```

---

## 七、AIstock配置更新逻辑

### 7.1 环境变量写入规则

```python
# 伪代码逻辑（不修改代码，仅说明规则）

def build_env_updates(stage_mappings):
    env_updates = {}
    
    for mapping in stage_mappings:
        model = get_model_info(mapping.model_id)
        provider = get_provider_info(model.provider_id)
        api_config = get_api_config(model.api_config_id)
        
        # 根据litellm_prefix确定环境变量名
        if provider.litellm_prefix in ['deepseek', 'dashscope', 'zai', 'anthropic']:
            # 使用LiteLLM官方映射
            env_key = f"{provider.litellm_prefix.upper()}_API_KEY"
            env_base = f"{provider.litellm_prefix.upper()}_API_BASE"
        else:
            # 使用数据库中配置的自定义环境变量名
            env_key = api_config.env_api_key_name
            env_base = api_config.env_api_base_name
        
        env_updates[env_key] = api_config.api_key
        env_updates[env_base] = api_config.api_base
    
    return env_updates
```

### 7.2 冲突检测规则

在AIstock前端配置页面添加冲突检测：

1. 检查是否有多个服务商使用`openai/`前缀
2. 如果有，提示用户只能选择一个
3. 或建议使用LiteLLM Proxy

---

## 八、总结

### 8.1 核心结论

| 问题 | 答案 |
|------|------|
| 是否需要LiteLLM Proxy？ | **大部分场景不需要**，只有多OpenAI兼容服务商并存时需要 |
| OpenAI兼容模型环境变量限制？ | **只有`openai/`前缀有限制**，专属前缀无限制 |
| 不同阶段使用不同服务商模型？ | **完全支持**，使用专属前缀即可 |

### 8.2 改造要点

1. **修正服务商的`litellm_prefix`**：使用LiteLLM官方定义的前缀
2. **修正模型的`full_model_id`**：使用正确的前缀格式
3. **修正API配置的环境变量名**：与LiteLLM官方映射一致
4. **前端添加冲突检测**：防止多个服务商使用`openai/`前缀

### 8.3 推荐配置策略

| 服务商 | 推荐前缀 | 环境变量 |
|--------|----------|----------|
| DeepSeek | `deepseek/` | DEEPSEEK_API_KEY |
| 阿里云百炼 | `dashscope/` | DASHSCOPE_API_KEY |
| 智谱AI | `zai/` | ZAI_API_KEY |
| Anthropic | `anthropic/` | ANTHROPIC_API_KEY |
| 硅基流动 | `openai/` | OPENAI_API_KEY（需独占）|

**无需修改任何代码，只需修正数据库配置数据即可实现多服务商并存**。
