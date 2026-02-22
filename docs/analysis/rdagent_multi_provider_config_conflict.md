# RDAgent多服务商模型配置冲突分析

## 一、当前配置更新机制

### 1.1 AIstock配置更新流程

**文件**: `AIstock/backend/routers/rdagent_llm_config.py` 第809-880行

```python
# 4. 获取所有模型信息，用于构建API更新
cursor.execute("""
    SELECT sm.stage_name, m.full_model_id, sm.temperature, sm.max_tokens,
           ac.api_base, ac.api_key, ac.env_api_base_name, ac.env_api_key_name
    FROM aistock_llm_stage_mappings sm
    LEFT JOIN aistock_llm_models m ON sm.model_id = m.id
    LEFT JOIN aistock_llm_api_configs ac ON m.api_config_id = ac.id
    WHERE sm.is_active = true
""")

stage_map = {}
api_credentials = {}

for row in cursor.fetchall():
    # ... 构建stage_map ...
    
    # 收集API凭证
    if api_base and env_api_base_name:
        api_credentials[env_api_base_name] = api_base  # 如: OPENAI_API_BASE = https://...
    if api_key and env_api_key_name:
        api_credentials[env_api_key_name] = api_key    # 如: OPENAI_API_KEY = sk-xxx

# 7. 调用RD-Agent API更新配置
result = await client.update_config(
    stage_mappings=api_stage_mappings,
    api_credentials=api_credentials,  # 传递所有API凭证
    ...
)
```

### 1.2 RDAgent接收配置更新

**文件**: `RD-Agent-main/rdagent/app/llm_config/env_manager.py` 第287-291行

```python
# Add API credentials if provided
if api_credentials:
    for key, value in api_credentials.items():
        if value is not None:
            updates[key] = value  # 直接写入对应的环境变量
```

### 1.3 结论：**是的，程序会替换环境变量**

AIstock根据每个模型配置的`env_api_base_name`和`env_api_key_name`，将对应的API凭证写入RDAgent的.env文件。

---

## 二、多服务商OpenAI兼容模型配置问题

### 2.1 场景：两个阶段使用不同服务商的OpenAI兼容模型

**假设配置**：

| 阶段 | 模型 | 实际服务商 | 数据库中的env配置 |
|------|------|------------|-------------------|
| coding | openai/glm-4.7 | 阿里云百炼 | OPENAI_API_BASE, OPENAI_API_KEY |
| feedback | openai/deepseek-chat | 硅基流动 | OPENAI_API_BASE, OPENAI_API_KEY |

### 2.2 问题：环境变量冲突

**AIstock发送的api_credentials**：

```python
api_credentials = {
    "OPENAI_API_BASE": "https://dashscope.aliyuncs.com/...",  # coding阶段的配置
    "OPENAI_API_KEY": "sk-xxx",                               # coding阶段的配置
}
```

**结果**：
- 最后写入的值会覆盖之前的值
- **两个阶段都会使用同一个OPENAI_API_BASE**
- 如果coding阶段最后写入，feedback阶段也会使用阿里云百炼的API

### 2.3 冲突验证

查看当前.env配置：

```bash
LITELLM_CHAT_MODEL_MAP={
    "coding": {"model": "openai/glm-4.7", ...},
    "feedback": {"model": "deepseek/deepseek-chat", ...}  # 注意：这里用的是deepseek/前缀
}

OPENAI_API_BASE = https://da...  # 阿里云百炼
DEEPSEEK_API_BASE = https://ap... # DeepSeek官方
```

**当前配置避免了冲突**：
- coding使用`openai/glm-4.7` → 读取`OPENAI_API_BASE`（阿里云百炼）
- feedback使用`deepseek/deepseek-chat` → 读取`DEEPSEEK_API_BASE`（DeepSeek官方）

### 2.4 如果两个阶段都用`openai/`前缀会怎样？

**冲突场景**：

```bash
# 假设这样配置
LITELLM_CHAT_MODEL_MAP={
    "coding": {"model": "openai/glm-4.7", ...},      # 想用阿里云百炼
    "feedback": {"model": "openai/deepseek-chat", ...} # 想用硅基流动
}

# 但环境变量只能有一个
OPENAI_API_BASE = https://???  # 只能设置一个地址
OPENAI_API_KEY = sk-???        # 只能设置一个Key
```

**结论**：**会产生冲突，两个阶段实际会使用同一个服务商**

---

## 三、RDAgent兼容的模型类型

### 3.1 RDAgent完全依赖LiteLLM

**证据**：`rdagent/oai/backend/litellm.py`

RDAgent没有自己的模型兼容列表，完全通过LiteLLM的`completion()`和`embedding()`函数调用模型。

### 3.2 LiteLLM支持的前缀类型

| 前缀 | 环境变量 | 说明 |
|------|----------|------|
| `openai/` | OPENAI_API_KEY, OPENAI_API_BASE | OpenAI官方及所有兼容API |
| `deepseek/` | DEEPSEEK_API_KEY, DEEPSEEK_API_BASE | DeepSeek官方 |
| `anthropic/` | ANTHROPIC_API_KEY, ANTHROPIC_API_BASE | Claude官方 |
| `azure/` | AZURE_API_KEY, AZURE_API_BASE | Azure OpenAI |
| `litellm_proxy/` | LITELLM_PROXY_API_KEY, LITELLM_PROXY_API_BASE | LiteLLM Proxy |

### 3.3 关键限制

**LiteLLM的前缀-环境变量映射是固定的，不能自定义**：

- 无法让`openai/model-a`读取`SERVICE_A_API_KEY`
- 无法让`openai/model-b`读取`SERVICE_B_API_KEY`
- 所有`openai/`前缀的模型都读取同一个`OPENAI_API_KEY`

---

## 四、解决方案分析

### 4.1 当前做法：使用不同前缀区分服务商

**示例**：

| 服务商 | 模型写法 | 环境变量 |
|--------|----------|----------|
| DeepSeek官方 | `deepseek/deepseek-chat` | DEEPSEEK_API_KEY |
| 阿里云百炼 | `openai/glm-4.7` | OPENAI_API_KEY |
| 硅基流动 | `openai/xxx` | OPENAI_API_KEY（冲突）|

**限制**：只能有一个服务商使用`openai/`前缀

### 4.2 方案A：为每个OpenAI兼容服务商设置专属前缀（不可行）

**问题**：LiteLLM不支持自定义前缀

### 4.3 方案B：使用LiteLLM Proxy（推荐）

部署LiteLLM Proxy，在配置文件中定义模型映射：

```yaml
model_list:
  - model_name: glm-bailian
    litellm_params:
      model: openai/glm-4.7
      api_base: https://dashscope.aliyuncs.com/compatible-mode/v1
      api_key: os.environ/DASHSCOPE_API_KEY

  - model_name: deepseek-siliconflow
    litellm_params:
      model: openai/deepseek-chat
      api_base: https://api.siliconflow.cn/v1
      api_key: os.environ/SILICONFLOW_API_KEY
```

**使用方式**：

```bash
LITELLM_CHAT_MODEL_MAP={
    "coding": {"model": "litellm_proxy/glm-bailian", ...},
    "feedback": {"model": "litellm_proxy/deepseek-siliconflow", ...}
}
```

### 4.4 方案C：AIstock侧限制配置（当前可行）

**规则**：
1. 只允许一个服务商使用`openai/`前缀
2. 其他服务商使用其专属前缀（如`deepseek/`、`anthropic/`）
3. 在前端配置页面添加冲突检测和提示

---

## 五、总结

### 问题回答

| 问题 | 答案 |
|------|------|
| 程序是否替换OPENAI_API_KEY等环境变量？ | **是的**，根据模型配置的env_api_key_name写入对应环境变量 |
| 两个不同服务商的OpenAI兼容模型会冲突吗？ | **会冲突**，如果都用`openai/`前缀，只能使用同一个环境变量 |
| RDAgent兼容哪些模型？ | **完全依赖LiteLLM**，无自己的兼容列表 |

### 核心限制

```
┌─────────────────────────────────────────────────────────────┐
│  LiteLLM前缀-环境变量映射是固定的                            │
│                                                              │
│  openai/*  →  OPENAI_API_KEY + OPENAI_API_BASE              │
│  （所有OpenAI兼容服务商共用，只能配置一个）                   │
│                                                              │
│  deepseek/*  →  DEEPSEEK_API_KEY + DEEPSEEK_API_BASE        │
│  anthropic/*  →  ANTHROPIC_API_KEY + ANTHROPIC_API_BASE     │
└─────────────────────────────────────────────────────────────┘
```

### 建议措施

1. **AIstock前端添加冲突检测**：当用户尝试配置多个`openai/`前缀模型时提示冲突
2. **使用专属前缀**：DeepSeek用`deepseek/`，Claude用`anthropic/`
3. **长期方案**：部署LiteLLM Proxy实现多服务商并存
