# RDAgent LiteLLM 环境变量读取机制分析

## 一、核心结论

**不需要修改LiteLLM代码**。LiteLLM库本身已内置了根据模型前缀自动查找对应环境变量的机制。

---

## 二、RDAgent调用LLM的代码分析

### 2.1 Chat Completion调用

**文件**: `rdagent/oai/backend/litellm.py` 第201-207行

```python
response = completion(
    messages=messages,
    stream=LITELLM_SETTINGS.chat_stream,
    max_retries=0,
    **complete_kwargs,  # 只包含 model, temperature, max_tokens, reasoning_effort
    **kwargs,
)
```

**关键发现**：
- **没有显式传递 `api_key` 或 `api_base` 参数**
- 完全依赖 LiteLLM 库内部的环境变量查找机制

### 2.2 Embedding调用

**文件**: `rdagent/oai/backend/litellm.py` 第91-124行

```python
embedding_params = {
    "model": model_name,
    "input": input_content_list,
}

# Add api_base if EMBEDDING_API_BASE is set
embedding_api_base = os.getenv("EMBEDDING_API_BASE")
if embedding_api_base:
    embedding_params["api_base"] = embedding_api_base

# Add api_key if EMBEDDING_API_KEY is set
embedding_api_key = os.getenv("EMBEDDING_API_KEY")
if embedding_api_key:
    embedding_params["api_key"] = embedding_api_key

response = embedding(**embedding_params)
```

**关键发现**：
- Embedding调用**显式读取**了`EMBEDDING_API_BASE`和`EMBEDDING_API_KEY`
- 这是RDAgent为Embedding做的特殊处理，而非LiteLLM的默认行为

### 2.3 Pydantic-AI适配器

**文件**: `rdagent/oai/backend/pydantic_ai.py` 第19-51行

```python
PROVIDER_TO_ENV_MAP = {
    "openai": "OPENAI",
    "azure_ai": "AZURE_AI",
    "azure": "AZURE",
    "litellm_proxy": "LITELLM_PROXY",
}

# ...

_, custom_llm_provider, _, _ = get_llm_provider(selected_model)
prefix = PROVIDER_TO_ENV_MAP[custom_llm_provider]
api_key = os.getenv(f"{prefix}_API_KEY", None)
api_base = os.getenv(f"{prefix}_API_BASE", None)
```

**关键发现**：
- 这个映射表**不完整**，缺少`deepseek`、`anthropic`等
- 但这只用于`pydantic_ai`适配器，不用于主LLM调用

---

## 三、LiteLLM内部环境变量查找机制

### 3.1 LiteLLM如何确定使用哪个环境变量

**LiteLLM库内部逻辑**（基于其官方文档和源码）：

```
模型字符串格式: <provider>/<model_name>
                    ↓
LiteLLM解析provider部分
                    ↓
查找对应的环境变量:
  - openai/*     → OPENAI_API_KEY, OPENAI_API_BASE
  - deepseek/*   → DEEPSEEK_API_KEY, DEEPSEEK_API_BASE
  - anthropic/*  → ANTHROPIC_API_KEY, ANTHROPIC_API_BASE
  - azure/*      → AZURE_API_KEY, AZURE_API_BASE
```

### 3.2 验证：当前.env配置

```bash
# 当前.env中的配置
OPENAI_API_BASE = https://da...  # 实际指向阿里云百炼
OPENAI_API_KEY = sk-482af8b...   # 阿里云百炼的Key

# LITELLM_CHAT_MODEL_MAP
coding: {"model": "openai/glm-4.7", ...}
```

**调用链**：
```
1. RDAgent调用 completion(model="openai/glm-4.7", ...)
2. LiteLLM解析前缀 "openai"
3. LiteLLM查找环境变量 OPENAI_API_KEY + OPENAI_API_BASE
4. OPENAI_API_BASE = https://dashscope.aliyuncs.com/...
5. 请求发送到阿里云百炼API
```

---

## 四、回答用户问题

### 问题1：是否需要修改LiteLLM代码？

**答案：不需要**

LiteLLM库已经内置了根据模型前缀自动查找环境变量的机制。RDAgent的`completion()`调用没有显式传递api_key/api_base，完全依赖LiteLLM的内部逻辑。

### 问题2：RDAgent的env文件怎样获取API key？

**答案**：

| 调用类型 | 环境变量获取方式 |
|----------|------------------|
| Chat Completion | LiteLLM根据模型前缀自动查找（如`openai/`→`OPENAI_API_KEY`）|
| Embedding | RDAgent代码显式读取`EMBEDDING_API_KEY`和`EMBEDDING_API_BASE`|

### 问题3：是否只有使用OpenAI兼容模型就只能读OPENAI_API_BASE和OPENAI_API_KEY？

**答案：是的，但有条件**

当模型使用`openai/`前缀时：
- LiteLLM**固定**查找`OPENAI_API_KEY`和`OPENAI_API_BASE`
- 无法让`openai/glm-4.7`读取`DASHSCOPE_API_KEY`

**解决方案**：
1. 将`OPENAI_API_BASE`设置为实际服务商的地址
2. 或使用其他前缀（如`deepseek/`、`anthropic/`）

### 问题4：更换服务商后是否必须修改OPENAI_API_BASE和OPENAI_API_KEY？

**答案：是的，如果使用`openai/`前缀**

**示例**：

| 目标服务商 | 模型写法 | 需要设置的环境变量 |
|------------|----------|-------------------|
| 硅基流动 | `openai/glm-4.7` | `OPENAI_API_BASE=https://api.siliconflow.cn/v1` |
| 阿里云百炼 | `openai/glm-4.7` | `OPENAI_API_BASE=https://dashscope.aliyuncs.com/...` |
| DeepSeek官方 | `deepseek/deepseek-chat` | `DEEPSEEK_API_BASE=https://api.deepseek.com` |

---

## 五、当前架构的限制

### 5.1 核心限制

**LiteLLM的前缀-环境变量映射是固定的**：

| 前缀 | 环境变量前缀 | 说明 |
|------|-------------|------|
| openai | OPENAI_ | 所有OpenAI兼容API共用 |
| deepseek | DEEPSEEK_ | DeepSeek官方专用 |
| anthropic | ANTHROPIC_ | Claude官方专用 |

### 5.2 问题场景

**无法同时使用多个OpenAI兼容服务商**：

```
期望配置:
  - openai/glm-4.7 → 阿里云百炼
  - openai/deepseek-chat → 硅基流动

实际行为:
  - 两者都读取同一个 OPENAI_API_BASE
  - 无法区分
```

### 5.3 解决方案（不修改LiteLLM代码）

#### 方案A：切换环境变量（当前做法）

每次更换服务商时，修改`OPENAI_API_BASE`和`OPENAI_API_KEY`。

**优点**：简单，无需改代码
**缺点**：无法同时使用多个服务商

#### 方案B：使用LiteLLM Proxy

部署LiteLLM Proxy服务，在配置文件中定义模型映射：

```yaml
model_list:
  - model_name: glm-4.7-bailian
    litellm_params:
      model: openai/glm-4.7
      api_base: https://dashscope.aliyuncs.com/compatible-mode/v1
      api_key: os.environ/DASHSCOPE_API_KEY

  - model_name: glm-4.7-siliconflow
    litellm_params:
      model: openai/glm-4.7
      api_base: https://api.siliconflow.cn/v1
      api_key: os.environ/SILICONFLOW_API_KEY
```

然后使用`litellm_proxy/glm-4.7-bailian`作为模型名。

**优点**：支持多服务商并存
**缺点**：需要额外部署服务

#### 方案C：为每个服务商设置专属环境变量

在RDAgent代码中扩展环境变量读取逻辑（需修改RDAgent代码，但不修改LiteLLM）：

```python
# 假设的扩展逻辑
if model.startswith("openai/"):
    # 优先读取服务商专属环境变量
    api_base = os.getenv("SILICONFLOW_API_BASE") or os.getenv("DASHSCOPE_API_BASE") or os.getenv("OPENAI_API_BASE")
```

**优点**：无需部署额外服务
**缺点**：需要修改RDAgent代码

---

## 六、总结

### 关键机制

```
┌─────────────────────────────────────────────────────────────┐
│                    RDAgent调用流程                           │
│  completion(model="openai/glm-4.7", messages=[...])         │
│                    ↓                                        │
│  LiteLLM解析: provider="openai"                             │
│                    ↓                                        │
│  LiteLLM查找: OPENAI_API_KEY + OPENAI_API_BASE              │
│                    ↓                                        │
│  发送请求到 OPENAI_API_BASE 指定的地址                       │
└─────────────────────────────────────────────────────────────┘
```

### 核心答案

| 问题 | 答案 |
|------|------|
| 需要修改LiteLLM代码吗？ | **不需要** |
| 环境变量如何获取？ | LiteLLM根据模型前缀自动查找 |
| `openai/`前缀只能读`OPENAI_*`吗？ | **是的**，这是LiteLLM的固定映射 |
| 更换服务商必须改`OPENAI_API_BASE`吗？ | **是的**，如果使用`openai/`前缀 |

### 建议

1. **短期**：继续使用当前方式，切换服务商时修改`OPENAI_API_BASE`
2. **中期**：为不同服务商配置专属环境变量（需扩展RDAgent代码）
3. **长期**：部署LiteLLM Proxy实现多服务商并存
