# LiteLLM Proxy引入方案

## 一、Proxy架构概述

### 1.1 引入Proxy前后的架构对比

**引入前（当前架构）**：

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  AIstock    │────▶│  RDAgent    │────▶│  LiteLLM    │
│  数据库配置  │     │  .env文件   │     │  SDK        │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
                    ┌──────────────────────────┼──────────────────────────┐
                    │                          │                          │
                    ▼                          ▼                          ▼
              ┌──────────┐              ┌──────────┐              ┌──────────┐
              │ DeepSeek │              │ 阿里云   │              │ 智谱AI   │
              │ 官方API  │              │ 百炼API  │              │ 官方API  │
              └──────────┘              └──────────┘              └──────────┘
```

**引入后（Proxy架构）**：

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  AIstock    │────▶│  RDAgent    │────▶│  LiteLLM    │────▶│ LiteLLM     │
│  数据库配置  │     │  .env文件   │     │  SDK        │     │ Proxy服务   │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
                                                                   │
                    ┌──────────────────────────────────────────────┼──────────────────────────┐
                    │                                              │                          │
                    ▼                                              ▼                          ▼
              ┌──────────┐                                  ┌──────────┐              ┌──────────┐
              │ DeepSeek │                                  │ 阿里云   │              │ 智谱AI   │
              │ 官方API  │                                  │ 百炼API  │              │ 官方API  │
              └──────────┘                                  └──────────┘              └──────────┘
```

### 1.2 Proxy的核心作用

| 功能 | 说明 |
|------|------|
| **统一API网关** | 所有模型通过一个OpenAI兼容API访问 |
| **模型别名映射** | 将用户友好的模型名映射到实际服务商 |
| **多服务商共存** | 解决多个OpenAI兼容服务商/Anthropic代理商的冲突问题 |
| **负载均衡** | 多个部署之间的负载均衡 |
| **成本追踪** | 统一追踪所有模型的调用成本 |

---

## 二、部署LiteLLM Proxy

### 2.1 安装

```bash
pip install 'litellm[proxy]'
```

### 2.2 配置文件

**文件路径**：`F:/Dev/litellm-proxy/config.yaml`

```yaml
model_list:
  # ============================================
  # DeepSeek官方
  # ============================================
  - model_name: deepseek-chat
    litellm_params:
      model: deepseek/deepseek-chat
      api_key: os.environ/DEEPSEEK_API_KEY

  # ============================================
  # 阿里云百炼
  # ============================================
  - model_name: qwen-turbo
    litellm_params:
      model: dashscope/qwen-turbo
      api_key: os.environ/DASHSCOPE_API_KEY

  - model_name: glm-4-plus-bailian
    litellm_params:
      model: dashscope/glm-4-plus
      api_key: os.environ/DASHSCOPE_API_KEY

  # ============================================
  # 智谱AI官方
  # ============================================
  - model_name: glm-4.7-zhipu
    litellm_params:
      model: zai/glm-4.7
      api_key: os.environ/ZAI_API_KEY

  # ============================================
  # 硅基流动（OpenAI兼容）
  # ============================================
  - model_name: glm-4-9b-siliconflow
    litellm_params:
      model: openai/THUDM/glm-4-9b-0414
      api_base: https://api.siliconflow.cn/v1
      api_key: os.environ/SILICONFLOW_API_KEY

  - model_name: deepseek-v3-siliconflow
    litellm_params:
      model: openai/deepseek-ai/DeepSeek-V3
      api_base: https://api.siliconflow.cn/v1
      api_key: os.environ/SILICONFLOW_API_KEY

  # ============================================
  # Anthropic代理商A
  # ============================================
  - model_name: claude-agent-a
    litellm_params:
      model: anthropic/claude-sonnet-4-5-20250929
      api_base: https://agent-a.com/v1
      api_key: os.environ/AGENT_A_API_KEY

  # ============================================
  # Anthropic代理商B
  # ============================================
  - model_name: claude-agent-b
    litellm_params:
      model: anthropic/claude-sonnet-4-5-20250929
      api_base: https://agent-b.com/v1
      api_key: os.environ/AGENT_B_API_KEY

# ============================================
# 通用设置
# ============================================
general_settings:
  master_key: sk-litellm-proxy-master-key  # Proxy的访问密钥

# ============================================
# 环境变量（可选，也可在.env中设置）
# ============================================
environment_variables:
  DEEPSEEK_API_KEY: sk-deepseek-xxx
  DASHSCOPE_API_KEY: sk-dashscope-xxx
  ZAI_API_KEY: sk-zhipu-xxx
  SILICONFLOW_API_KEY: sk-siliconflow-xxx
  AGENT_A_API_KEY: sk-agent-a-xxx
  AGENT_B_API_KEY: sk-agent-b-xxx
```

### 2.3 启动Proxy

```bash
# 方式1：命令行启动
litellm --config config.yaml --port 4000

# 方式2：Docker启动
docker run -d \
  --name litellm-proxy \
  -p 4000:4000 \
  -v $(pwd)/config.yaml:/app/config.yaml \
  ghcr.io/berriai/litellm:main-latest \
  --config /app/config.yaml --port 4000
```

---

## 三、RDAgent配置改动

### 3.1 .env文件改动

**改动前**：
```bash
# 直接配置各服务商的API凭证
DEEPSEEK_API_KEY=sk-xxx
DASHSCOPE_API_KEY=sk-yyy
OPENAI_API_KEY=sk-zzz

LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "openai/THUDM/glm-4-9b-0414"},
  "feedback": {"model": "dashscope/qwen-turbo"}
}'
```

**改动后**：
```bash
# 只需配置Proxy的访问凭证
LITELLM_PROXY_API_KEY=sk-litellm-proxy-master-key
LITELLM_PROXY_API_BASE=http://localhost:4000

# 模型名使用Proxy中定义的别名
LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "litellm_proxy/glm-4-9b-siliconflow"},
  "feedback": {"model": "litellm_proxy/qwen-turbo"},
  "hypothesis": {"model": "litellm_proxy/glm-4-plus-bailian"}
}'
```

### 3.2 RDAgent代码分析

**文件**：`rdagent/oai/backend/pydantic_ai.py`

```python
PROVIDER_TO_ENV_MAP = {
    "openai": "OPENAI",
    "azure_ai": "AZURE_AI",
    "azure": "AZURE",
    "litellm_proxy": "LITELLM_PROXY",  # 已内置支持
}
```

**结论**：RDAgent已内置支持`litellm_proxy`前缀，**无需修改RDAgent代码**。

### 3.3 调用流程

```
RDAgent调用:
  model = "litellm_proxy/glm-4-9b-siliconflow"
      ↓
  LiteLLM SDK解析前缀 "litellm_proxy"
      ↓
  读取 LITELLM_PROXY_API_KEY + LITELLM_PROXY_API_BASE
      ↓
  请求发送到 http://localhost:4000/chat/completions
      ↓
  LiteLLM Proxy根据模型名 "glm-4-9b-siliconflow" 查找配置
      ↓
  转发到 https://api.siliconflow.cn/v1/chat/completions
```

---

## 四、AIstock配置更新逻辑改动

### 4.1 数据库改动

**新增字段**：

```sql
-- 服务商表新增字段
ALTER TABLE aistock_llm_providers ADD COLUMN use_proxy BOOLEAN DEFAULT FALSE;
ALTER TABLE aistock_llm_providers ADD COLUMN proxy_model_name VARCHAR(200);

-- 模型表新增字段
ALTER TABLE aistock_llm_models ADD COLUMN proxy_model_name VARCHAR(200);
```

**示例数据**：

| provider_name | use_proxy | proxy_model_name | 说明 |
|---------------|-----------|------------------|------|
| deepseek | false | NULL | 不使用Proxy |
| siliconflow | true | glm-4-9b-siliconflow | 使用Proxy |
| anthropic_agent_a | true | claude-agent-a | 使用Proxy |

### 4.2 配置更新逻辑改动

**文件**：`AIstock/backend/routers/rdagent_llm_config.py`

**改动逻辑**：

```python
# 伪代码 - 配置更新逻辑

def build_env_updates(stage_mappings, use_proxy_mode=False):
    env_updates = {}
    model_map = {}
    
    if use_proxy_mode:
        # Proxy模式：只配置Proxy的访问凭证
        env_updates['LITELLM_PROXY_API_KEY'] = get_proxy_master_key()
        env_updates['LITELLM_PROXY_API_BASE'] = get_proxy_base_url()
        
        for mapping in stage_mappings:
            model = get_model(mapping.model_id)
            
            # 使用Proxy中定义的模型别名
            if model.proxy_model_name:
                full_model_id = f"litellm_proxy/{model.proxy_model_name}"
            else:
                full_model_id = model.full_model_id
            
            model_map[mapping.stage_name] = {
                "model": full_model_id,
                "temperature": str(mapping.temperature),
                "max_tokens": str(mapping.max_tokens)
            }
    else:
        # 直连模式：配置各服务商的API凭证
        for mapping in stage_mappings:
            model = get_model(mapping.model_id)
            provider = get_provider(model.provider_id)
            api_config = get_api_config(model.api_config_id)
            
            model_map[mapping.stage_name] = {
                "model": model.full_model_id,
                ...
            }
            
            # 添加API凭证
            env_updates[api_config.env_api_key_name] = api_config.api_key
            env_updates[api_config.env_api_base_name] = api_config.api_base
    
    env_updates['LITELLM_CHAT_MODEL_MAP'] = json.dumps(model_map)
    return env_updates
```

### 4.3 前端改动

**配置页面新增选项**：

1. **模式选择**：
   - 直连模式（默认）
   - Proxy模式

2. **Proxy配置**：
   - Proxy地址
   - Proxy密钥

3. **模型选择**：
   - 直连模式：显示服务商+模型名
   - Proxy模式：显示Proxy中定义的模型别名

---

## 五、完整改动清单

### 5.1 需要改动的文件

| 文件 | 改动内容 | 是否必须 |
|------|----------|----------|
| **LiteLLM Proxy配置** | 新建config.yaml | Proxy模式必须 |
| **RDAgent .env** | 配置LITELLM_PROXY_* | Proxy模式必须 |
| **AIstock数据库** | 新增use_proxy等字段 | 可选（用于切换模式）|
| **AIstock后端** | 配置更新逻辑改动 | 可选（用于自动切换）|
| **AIstock前端** | 配置页面新增选项 | 可选（用于UI切换）|

### 5.2 不需要改动的文件

| 文件 | 原因 |
|------|------|
| RDAgent核心代码 | 已内置支持litellm_proxy前缀 |
| LiteLLM库 | Proxy是LiteLLM自带功能 |

---

## 六、两种模式对比

### 6.1 直连模式（当前）

**优点**：
- 无需额外部署服务
- 配置简单直接

**缺点**：
- 多个OpenAI兼容服务商会冲突
- 多个Anthropic代理商会冲突

**适用场景**：
- 使用的服务商都有专属前缀（dashscope、zai、deepseek等）
- 只有一个OpenAI兼容服务商

### 6.2 Proxy模式

**优点**：
- 支持任意数量的服务商共存
- 统一管理所有API凭证
- 支持负载均衡、成本追踪

**缺点**：
- 需要额外部署Proxy服务
- 增加一层调用链路

**适用场景**：
- 需要同时使用多个OpenAI兼容服务商
- 需要同时使用多个Anthropic代理商
- 需要企业级功能（成本追踪、负载均衡）

---

## 七、实施建议

### 7.1 优先使用直连模式

对于大多数场景，使用服务商专属前缀即可满足需求：

| 服务商 | 前缀 | 直连模式支持 |
|--------|------|--------------|
| DeepSeek官方 | deepseek/ | ✅ |
| 阿里云百炼 | dashscope/ | ✅ |
| 智谱AI官方 | zai/ | ✅ |
| Anthropic官方 | anthropic/ | ✅ |

### 7.2 仅在必要时使用Proxy

**需要Proxy的场景**：
1. 同时使用硅基流动 + 另一个OpenAI兼容服务商
2. 同时使用多个Anthropic代理商
3. 需要负载均衡或成本追踪

### 7.3 混合模式（推荐）

可以同时支持直连和Proxy：

```bash
# 直连的服务商
DEEPSEEK_API_KEY=sk-xxx
DASHSCOPE_API_KEY=sk-yyy

# Proxy的服务商
LITELLM_PROXY_API_KEY=sk-proxy-key
LITELLM_PROXY_API_BASE=http://localhost:4000

# 混合使用
LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "litellm_proxy/glm-4-9b-siliconflow"},
  "feedback": {"model": "dashscope/qwen-turbo"},
  "default": {"model": "deepseek/deepseek-chat"}
}'
```

---

## 八、总结

### 8.1 改动清单

| 改动项 | 是否必须 | 说明 |
|--------|----------|------|
| 部署LiteLLM Proxy | Proxy模式必须 | 新建config.yaml并启动服务 |
| RDAgent .env配置 | Proxy模式必须 | 配置LITELLM_PROXY_API_KEY/BASE |
| AIstock数据库改动 | 可选 | 支持模式切换 |
| AIstock后端改动 | 可选 | 支持自动配置生成 |
| AIstock前端改动 | 可选 | 支持UI模式切换 |
| RDAgent代码改动 | **不需要** | 已内置支持 |
| LiteLLM代码改动 | **不需要** | Proxy是自带功能 |

### 8.2 推荐方案

1. **短期**：使用直连模式 + 服务商专属前缀
2. **中期**：在AIstock中添加Proxy模式支持（可选）
3. **长期**：根据实际需求决定是否部署Proxy
