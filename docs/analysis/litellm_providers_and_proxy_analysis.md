# LiteLLM支持的服务商及Proxy配置分析

## 一、LiteLLM支持的服务商列表

### 1.1 官方文档确认的服务商

根据LiteLLM官方文档（https://docs.litellm.ai/docs/providers），**LiteLLM支持100+服务商**，远不止3种前缀。

### 1.2 与中国用户相关的服务商

| 服务商 | 前缀 | 环境变量 | 官方文档 |
|--------|------|----------|----------|
| **阿里云百炼 (Qwen)** | `dashscope/` | DASHSCOPE_API_KEY | https://docs.litellm.ai/docs/providers/dashscope |
| **智谱AI (GLM)** | `zai/` | ZAI_API_KEY | https://docs.litellm.ai/docs/providers/zai |
| **DeepSeek** | `deepseek/` | DEEPSEEK_API_KEY | https://docs.litellm.ai/docs/providers/deepseek |
| **硅基流动** | `openai/` (兼容模式) | OPENAI_API_KEY + 自定义api_base | OpenAI兼容端点 |

### 1.3 其他主要服务商

| 服务商 | 前缀 | 说明 |
|--------|------|------|
| OpenAI | `openai/` | 官方OpenAI |
| Azure OpenAI | `azure/` | Azure部署 |
| Anthropic (Claude) | `anthropic/` | Claude官方 |
| Google (Gemini) | `gemini/` | Google AI Studio |
| AWS Bedrock | `bedrock/` | AWS托管服务 |
| Vertex AI | `vertex_ai/` | GCP托管服务 |
| Moonshot | `moonshot/` | 月之暗面 |
| Minimax | `minimax/` | Minimax |
| X.AI (Grok) | `xai/` | Elon Musk的xAI |

### 1.4 关键发现

**LiteLLM已经内置支持阿里云百炼和智谱AI的专属前缀**：

```python
# 阿里云百炼 - 使用 dashscope/ 前缀
response = completion(
    model="dashscope/qwen-turbo",
    messages=[{"role": "user", "content": "hello"}]
)
# 自动读取 DASHSCOPE_API_KEY

# 智谱AI - 使用 zai/ 前缀
response = completion(
    model="zai/glm-4.7",
    messages=[{"role": "user", "content": "hello"}]
)
# 自动读取 ZAI_API_KEY
```

---

## 二、LiteLLM Proxy功能详解

### 2.1 核心功能

| 功能 | 说明 |
|------|------|
| **统一API网关** | 所有模型通过OpenAI兼容API访问 |
| **多服务商路由** | 在配置文件中定义模型映射 |
| **负载均衡** | 支持多部署之间的负载均衡 |
| **成本追踪** | 统一追踪所有模型的调用成本 |
| **用户/团队管理** | API密钥管理、预算控制 |
| **速率限制** | RPM/TPM限制 |

### 2.2 配置示例

**config.yaml**:

```yaml
model_list:
  # 阿里云百炼 - Qwen模型
  - model_name: qwen-turbo-bailian
    litellm_params:
      model: dashscope/qwen-turbo
      api_key: os.environ/DASHSCOPE_API_KEY

  # 智谱AI - GLM模型
  - model_name: glm-4.7-zhipu
    litellm_params:
      model: zai/glm-4.7
      api_key: os.environ/ZAI_API_KEY

  # 硅基流动 - 通过OpenAI兼容模式
  - model_name: glm-siliconflow
    litellm_params:
      model: openai/THUDM/glm-4-9b-0414
      api_base: https://api.siliconflow.cn/v1
      api_key: os.environ/SILICONFLOW_API_KEY

  # DeepSeek官方
  - model_name: deepseek-official
    litellm_params:
      model: deepseek/deepseek-chat
      api_key: os.environ/DEEPSEEK_API_KEY

general_settings:
  master_key: sk-your-master-key
```

### 2.3 启动Proxy

```bash
# 安装
pip install 'litellm[proxy]'

# 启动
litellm --config config.yaml --port 4000
```

### 2.4 使用Proxy

```python
# 所有请求通过统一的API
response = completion(
    model="litellm_proxy/qwen-turbo-bailian",  # 使用config中定义的model_name
    messages=[{"role": "user", "content": "hello"}],
    api_base="http://localhost:4000",
    api_key="sk-your-master-key"
)
```

---

## 三、是否需要修改RDAgent代码

### 3.1 主LLM调用路径

**文件**: `rdagent/oai/backend/litellm.py`

```python
response = completion(
    messages=messages,
    stream=LITELLM_SETTINGS.chat_stream,
    max_retries=0,
    **complete_kwargs,  # 包含 model="dashscope/qwen-turbo" 或 "zai/glm-4.7"
    **kwargs,
)
```

**结论**：
- **不需要修改RDAgent代码**
- LiteLLM的`completion()`函数自动根据模型前缀查找对应环境变量
- 只需在.env中设置正确的环境变量

### 3.2 Pydantic-AI适配器

**文件**: `rdagent/oai/backend/pydantic_ai.py`

```python
PROVIDER_TO_ENV_MAP = {
    "openai": "OPENAI",
    "azure_ai": "AZURE_AI",
    "azure": "AZURE",
    "litellm_proxy": "LITELLM_PROXY",
}
```

**这个映射表只用于pydantic_ai适配器**，不影响主LLM调用。

如果使用pydantic_ai适配器且需要支持其他服务商，需要扩展此映射表。但**主LLM调用不需要修改**。

---

## 四、RDAgent不同阶段使用不同服务商模型

### 4.1 方案A：使用服务商专属前缀（推荐，无需修改代码）

**配置示例**：

```bash
# .env 文件
DASHSCOPE_API_KEY=sk-xxx        # 阿里云百炼
ZAI_API_KEY=sk-yyy              # 智谱AI
DEEPSEEK_API_KEY=sk-zzz         # DeepSeek官方

# LITELLM_CHAT_MODEL_MAP
LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "dashscope/qwen-turbo", "temperature": "0.7"},
  "feedback": {"model": "zai/glm-4.7", "temperature": "0.5"},
  "default": {"model": "deepseek/deepseek-chat", "temperature": "0.7"}
}'
```

**调用流程**：

```
coding阶段: dashscope/qwen-turbo
    ↓
LiteLLM解析前缀 "dashscope"
    ↓
读取 DASHSCOPE_API_KEY
    ↓
请求发送到阿里云百炼

feedback阶段: zai/glm-4.7
    ↓
LiteLLM解析前缀 "zai"
    ↓
读取 ZAI_API_KEY
    ↓
请求发送到智谱AI
```

**优点**：
- 不需要修改任何代码
- 每个服务商有独立的环境变量
- 不会产生冲突

### 4.2 方案B：使用LiteLLM Proxy（更灵活）

**配置步骤**：

1. 创建 `config.yaml`:

```yaml
model_list:
  - model_name: qwen-coding
    litellm_params:
      model: dashscope/qwen-turbo
      api_key: os.environ/DASHSCOPE_API_KEY

  - model_name: glm-feedback
    litellm_params:
      model: zai/glm-4.7
      api_key: os.environ/ZAI_API_KEY

  - model_name: deepseek-default
    litellm_params:
      model: deepseek/deepseek-chat
      api_key: os.environ/DEEPSEEK_API_KEY
```

2. 启动Proxy:

```bash
litellm --config config.yaml --port 4000
```

3. 配置RDAgent:

```bash
LITELLM_PROXY_API_KEY=sk-master-key
LITELLM_PROXY_API_BASE=http://localhost:4000

LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "litellm_proxy/qwen-coding"},
  "feedback": {"model": "litellm_proxy/glm-feedback"},
  "default": {"model": "litellm_proxy/deepseek-default"}
}'
```

**优点**：
- 集中管理所有模型配置
- 支持负载均衡
- 成本追踪

---

## 五、总结

### 问题回答

| 问题 | 答案 |
|------|------|
| LiteLLM Proxy可实现哪些功能？ | 统一API网关、多服务商路由、负载均衡、成本追踪、用户管理 |
| 需要修改RDAgent代码吗？ | **不需要**，主LLM调用完全依赖LiteLLM内部机制 |
| 需要修改LiteLLM代码吗？ | **不需要**，LiteLLM已内置支持100+服务商 |
| LiteLLM只支持3种前缀吗？ | **不是**，支持100+服务商，包括dashscope/、zai/、deepseek/等专属前缀 |
| 不同阶段能用不同服务商吗？ | **可以**，使用服务商专属前缀即可，无需修改代码 |

### 推荐方案

**使用服务商专属前缀**：

| 服务商 | 前缀 | 环境变量 |
|--------|------|----------|
| 阿里云百炼 | `dashscope/` | DASHSCOPE_API_KEY |
| 智谱AI | `zai/` | ZAI_API_KEY |
| DeepSeek | `deepseek/` | DEEPSEEK_API_KEY |
| 硅基流动 | `openai/` + api_base | 需配置OPENAI_API_BASE |

**示例配置**：

```bash
# coding阶段用阿里云百炼
# feedback阶段用智谱AI
# default阶段用DeepSeek官方

LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "dashscope/qwen-turbo"},
  "feedback": {"model": "zai/glm-4.7"},
  "default": {"model": "deepseek/deepseek-chat"}
}'
```

**无需修改任何代码，只需配置环境变量即可实现多服务商并存**。
