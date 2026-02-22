# LLM服务商与模型配置问题分析

## 一、当前配置现状

### 1.1 数据库中的服务商配置

| ID | provider_name | display_name | api_base_url | litellm_prefix | 问题 |
|----|---------------|--------------|--------------|----------------|------|
| 1 | deepseek | DeepSeek | https://api.deepseek.com | deepseek | ✅ 正确 |
| 2 | openai | OPENAI | None | openai | ⚠️ 被其他服务商占用 |
| 3 | siliconflow | 硅基流动 | https://api.siliconflow.cn/v1 | **openai** | ❌ 前缀错误 |
| 4 | dashscope | 阿里云百炼 | https://dashscope.aliyuncs.com/... | **openai** | ❌ 前缀错误 |
| 5 | anthropic | Claude | https://api.anthropic.com | anthropic | ✅ 正确 |
| 6 | litellm_proxy | LiteLLM Proxy | None | litellm_proxy | ⚠️ 概念模糊 |

### 1.2 数据库中的模型配置

| 模型 | full_model_id | 归属服务商 | 实际API来源 | 问题 |
|------|---------------|------------|-------------|------|
| deepseek-chat | deepseek/deepseek-chat | deepseek | DeepSeek官方 | ✅ 正确 |
| claude-sonnet-4-5 | claude-sonnet-4-5-... | anthropic | 第三方代理 | ⚠️ API非官方 |
| BAAI/bge-m3 | **openai**/BAAI/bge-m3 | openai | 硅基流动 | ❌ 归属错误 |
| glm-4.7 | **openai**/glm-4.7 | openai | 阿里云百炼 | ❌ 归属错误 |
| Qwen3-Embedding | litellm_proxy/Qwen/... | litellm_proxy | 未知 | ⚠️ 概念模糊 |

### 1.3 RDAgent .env 环境变量

```bash
# 实际生效的API配置
OPENAI_API_BASE = https://da...  # 阿里云百炼地址！
OPENAI_API_KEY = sk-482af8b...   # 阿里云百炼的Key

DEEPSEEK_API_BASE = https://ap...
DEEPSEEK_API_KEY = sk-952e19d...

LITELLM_PROXY_API_BASE = https://ap...
EMBEDDING_API_BASE = https://ap...
```

---

## 二、核心问题分析

### 2.1 服务商定义混乱

**问题本质**：混淆了"真正的模型服务商"和"LiteLLM前缀类型"

| 实体类型 | 定义 | 示例 |
|----------|------|------|
| **模型服务商** | 提供模型API服务的厂商 | 硅基流动、阿里云百炼、DeepSeek |
| **LiteLLM前缀** | LiteLLM识别API凭证的方式 | openai、deepseek、anthropic |

**当前错误**：
- 硅基流动被当作独立服务商，但`litellm_prefix=openai`
- 阿里云百炼被当作独立服务商，但`litellm_prefix=openai`
- 导致：使用硅基流动的模型时，LiteLLM去读`OPENAI_API_KEY`而非`SILICONFLOW_API_KEY`

### 2.2 模型归属错误

**GLM-4.7案例**：
```
数据库配置:
  模型名: glm-4.7
  full_model_id: openai/glm-4.7
  归属服务商: openai
  API Base: https://dashscope.aliyuncs.com/...  (阿里云百炼)

实际调用链:
  LiteLLM看到 "openai/glm-4.7"
  → 使用 OPENAI_API_KEY + OPENAI_API_BASE
  → 发送到阿里云百炼的API
```

**问题**：GLM是智谱AI的模型，通过阿里云百炼调用，却被归属到"openai"服务商。

### 2.3 LiteLLM前缀机制导致归一化

**LiteLLM的前缀映射逻辑**：

```
full_model_id = "openai/glm-4.7"
                ↓
LiteLLM解析前缀 = "openai"
                ↓
查找环境变量:
  - OPENAI_API_KEY
  - OPENAI_API_BASE
                ↓
发送请求到 OPENAI_API_BASE 指定的地址
```

**结果**：所有使用`openai/`前缀的模型，最终都通过`OPENAI_API_BASE`指向的地址调用。

当前`OPENAI_API_BASE`指向阿里云百炼，所以：
- `openai/glm-4.7` → 阿里云百炼
- `openai/BAAI/bge-m3` → 如果设置正确应该去硅基流动，但实际会去阿里云百炼

### 2.4 环境变量冲突

当前.env中：
- `OPENAI_API_BASE` = 阿里云百炼地址
- `EMBEDDING_API_BASE` = 硅基流动地址
- `LITELLM_PROXY_API_BASE` = 硅基流动地址

**问题**：同一个`OPENAI_API_BASE`被多个服务商共用，无法区分。

---

## 三、LiteLLM的正确理解

### 3.1 LiteLLM的设计意图

LiteLLM通过**前缀**来识别使用哪套API凭证：

| 前缀 | 环境变量 | 用途 |
|------|----------|------|
| openai | OPENAI_API_KEY, OPENAI_API_BASE | OpenAI官方或兼容API |
| deepseek | DEEPSEEK_API_KEY, DEEPSEEK_API_BASE | DeepSeek官方 |
| anthropic | ANTHROPIC_API_KEY, ANTHROPIC_API_BASE | Claude官方 |
| openai | OPENAI_API_KEY, OPENAI_API_BASE | **兼容OpenAI格式的第三方** |

### 3.2 兼容OpenAI格式的服务商

以下服务商提供OpenAI兼容API，可以使用`openai/`前缀：
- 硅基流动
- 阿里云百炼
- 智谱AI (GLM)
- Moonshot
- 各种第三方代理

**关键**：这些服务商需要各自独立的`OPENAI_API_BASE`指向各自的地址。

### 3.3 LiteLLM的限制

LiteLLM**不支持**同时配置多个`openai/`前缀指向不同服务商。

**解决方案**：
1. 使用自定义前缀（需要LiteLLM配置文件）
2. 使用LiteLLM Proxy统一代理
3. 切换`OPENAI_API_BASE`（当前做法，但容易冲突）

---

## 四、建议的清晰架构

### 4.1 重新定义概念层级

```
┌─────────────────────────────────────────────────────────────┐
│                    模型服务商 (Provider)                     │
│  定义：提供LLM API服务的厂商                                  │
│  示例：DeepSeek、硅基流动、阿里云百炼、智谱AI、Anthropic      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    模型 (Model)                             │
│  定义：具体的LLM模型实例                                      │
│  示例：deepseek-chat、glm-4、gpt-4o、claude-sonnet           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    API端点 (API Endpoint)                    │
│  定义：调用模型的具体API地址和凭证                            │
│  一个服务商可以有多个API端点（如官方API、代理API）            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    LiteLLM前缀 (Prefix)                      │
│  定义：LiteLLM识别API凭证的方式                              │
│  注意：这是技术实现细节，不是业务概念                         │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 建议的数据库表结构调整

**服务商表 (aistock_llm_providers)**：

| 字段 | 说明 | 示例 |
|------|------|------|
| provider_name | 服务商标识 | siliconflow |
| display_name | 显示名称 | 硅基流动 |
| provider_type | 服务商类型 | `official` / `aggregator` / `proxy` |
| official_site | 官网地址 | https://siliconflow.cn |
| supported_models | 支持的模型列表 | ["deepseek-chat", "glm-4", ...] |

**API端点表 (aistock_llm_api_endpoints)** - 新增：

| 字段 | 说明 | 示例 |
|------|------|------|
| endpoint_name | 端点标识 | siliconflow_main |
| provider_id | 所属服务商 | 3 (硅基流动) |
| api_base | API地址 | https://api.siliconflow.cn/v1 |
| api_key | API密钥 | sk-xxx |
| litellm_prefix | LiteLLM前缀 | openai |
| env_api_key_name | 环境变量名 | SILICONFLOW_API_KEY |
| env_api_base_name | 环境变量名 | SILICONFLOW_API_BASE |
| is_default | 是否默认 | true |

**模型表 (aistock_llm_models)** - 调整：

| 字段 | 说明 | 示例 |
|------|------|------|
| model_name | 模型名称 | glm-4.7 |
| original_provider | 原始开发商 | zhipu (智谱AI) |
| available_endpoints | 可用API端点 | [1, 2, 3] |
| selected_endpoint | 当前选择的端点 | 2 (阿里云百炼) |
| full_model_id | LiteLLM完整ID | openai/glm-4.7 |

### 4.3 建议的配置管理逻辑

**原则**：
1. **一个服务商可以有多个API端点**
2. **一个模型可以通过多个服务商的API调用**
3. **LiteLLM前缀由API端点决定，不由服务商决定**

**示例配置**：

```
服务商: 硅基流动
  └─ API端点1: 硅基流动主API
       ├─ api_base: https://api.siliconflow.cn/v1
       ├─ litellm_prefix: openai
       └─ env_vars: SILICONFLOW_API_KEY, SILICONFLOW_API_BASE

模型: glm-4.7
  ├─ 原始开发商: 智谱AI
  ├─ 可通过以下端点调用:
  │   ├─ 硅基流动API (openai/glm-4.7 → SILICONFLOW_API_BASE)
  │   ├─ 阿里云百炼API (openai/glm-4.7 → DASHSCOPE_API_BASE)
  │   └─ 智谱AI官方API (zhipu/glm-4.7 → ZHIPU_API_KEY)
  └─ 当前选择: 阿里云百炼API
```

### 4.4 环境变量命名规范建议

**统一命名规则**：`<PROVIDER>_<TYPE>`

```bash
# DeepSeek官方
DEEPSEEK_API_KEY=sk-xxx
DEEPSEEK_API_BASE=https://api.deepseek.com

# 硅基流动
SILICONFLOW_API_KEY=sk-xxx
SILICONFLOW_API_BASE=https://api.siliconflow.cn/v1

# 阿里云百炼
DASHSCOPE_API_KEY=sk-xxx
DASHSCOPE_API_BASE=https://dashscope.aliyuncs.com/compatible-mode/v1

# 智谱AI官方
ZHIPU_API_KEY=sk-xxx
ZHIPU_API_BASE=https://open.bigmodel.cn/api/paas/v4

# Anthropic官方
ANTHROPIC_API_KEY=sk-xxx
ANTHROPIC_API_BASE=https://api.anthropic.com
```

**LiteLLM配置**：使用`litellm`配置文件或动态设置映射关系。

---

## 五、当前问题的解决方案

### 5.1 短期方案（不改动表结构）

**调整服务商定义**：

| provider_name | 调整建议 |
|---------------|----------|
| openai | 重命名为 `openai_official`，仅用于真正的OpenAI |
| siliconflow | 保持，但明确其为"聚合服务商" |
| dashscope | 保持，明确其为"阿里云百炼" |

**调整模型归属**：

| 模型 | 当前归属 | 建议归属 |
|------|----------|----------|
| glm-4.7 | openai | dashscope（通过阿里云百炼调用）|
| BAAI/bge-m3 | openai | siliconflow（通过硅基流动调用）|

**环境变量分离**：

```bash
# 为每个服务商设置独立的环境变量
SILICONFLOW_API_KEY=sk-xxx
SILICONFLOW_API_BASE=https://api.siliconflow.cn/v1

DASHSCOPE_API_KEY=sk-xxx
DASHSCOPE_API_BASE=https://dashscope.aliyuncs.com/compatible-mode/v1
```

### 5.2 中期方案（调整表结构）

1. 新增 `aistock_llm_api_endpoints` 表
2. 模型表增加 `original_provider` 和 `available_endpoints` 字段
3. 前端UI支持"选择API端点"而非"选择服务商"

### 5.3 长期方案（使用LiteLLM Proxy）

部署LiteLLM Proxy服务，统一管理所有模型映射：

```yaml
# litellm_config.yaml
model_list:
  - model_name: glm-4.7
    litellm_params:
      model: openai/glm-4.7
      api_base: https://dashscope.aliyuncs.com/compatible-mode/v1
      api_key: os.environ/DASHSCOPE_API_KEY

  - model_name: deepseek-chat
    litellm_params:
      model: deepseek/deepseek-chat
      api_key: os.environ/DEEPSEEK_API_KEY
```

所有调用统一走LiteLLM Proxy，无需管理复杂的前缀映射。

---

## 六、总结

### 问题根源

1. **概念混淆**：将"LiteLLM前缀"当作"服务商"来管理
2. **归属错误**：模型归属的服务商与实际API来源不一致
3. **环境变量冲突**：多个服务商共用`OPENAI_API_BASE`

### 核心理解

**LiteLLM的`openai/`前缀是一个"协议类型"，不是"服务商标识"**。

所有兼容OpenAI API格式的服务商都可以使用`openai/`前缀，但需要各自独立的API凭证配置。

### 建议方向

1. **分离概念**：服务商 ≠ LiteLLM前缀
2. **引入API端点**：一个服务商可有多个API端点
3. **模型多源**：一个模型可通过多个端点调用
4. **统一代理**：长期使用LiteLLM Proxy简化管理
