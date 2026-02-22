# LLM模型配置共享方案分析

## 一、当前架构分析

### 1. AIstock侧数据库存储

AIstock已建立完整的LLM配置数据库表结构：

| 表名 | 用途 | 关键字段 |
|------|------|----------|
| `aistock_llm_providers` | 服务商配置 | provider_name, display_name, api_base_url, litellm_prefix |
| `aistock_llm_models` | 模型配置 | provider_id, model_name, full_model_id, model_type, api_config_id |
| `aistock_llm_api_configs` | API凭证 | api_base, api_key, env_api_base_name, env_api_key_name |
| `aistock_llm_stage_mappings` | RDAgent阶段映射 | stage_name, model_id, temperature, max_tokens |
| `aistock_llm_config_change_log` | 变更历史 | stage_name, old_model_id, new_model_id |

**数据流**：
```
前端配置 → AIstock数据库存储 → API调用RDAgent → 更新RDAgent .env文件
```

### 2. RDAgent侧配置

RDAgent依赖`.env`文件中的环境变量：
- `CHAT_MODEL` - 默认聊天模型
- `LITELLM_CHAT_MODEL_MAP` - 阶段-模型映射JSON
- `LITELLM_EMBEDDING_MODEL` - Embedding模型
- 各服务商的 `*_API_KEY` 和 `*_API_BASE`

**限制**：RDAgent代码使用`os.getenv()`读取配置，必须从环境变量获取。

### 3. QE提示词配置页面现状

当前QE Agent模型选择的数据来源（`GET /quantevolver/llm-models`）：

```python
# 从RDAgent .env读取
rdagent_env_path = Path(rdagent_root) / ".env"
env_vals = dotenv_values(str(rdagent_env_path))

# 解析 CHAT_MODEL 和 LITELLM_CHAT_MODEL_MAP
# 硬编码添加常用模型作为备选
common_models = ["deepseek/deepseek-chat", "openai/gpt-4o-mini", ...]
```

**问题**：QE页面无法使用AIstock数据库中已配置的模型服务商和模型列表。

---

## 二、两套系统的差异

| 维度 | RDAgent配置 | AIstock数据库配置 |
|------|-------------|-------------------|
| 存储位置 | .env文件 | PostgreSQL数据库 |
| 管理方式 | 手动编辑或API更新 | 前端UI + API管理 |
| 模型来源 | 环境变量解析 | 数据库CRUD |
| 适用范围 | 仅RDAgent使用 | 可扩展到AIstock自身 |
| 灵活性 | 受代码限制 | 完全自由 |

---

## 三、方案建议

### 推荐方案：统一使用AIstock数据库作为主数据源

**理由**：

1. **已有完整基础设施**
   - 数据库表结构完备
   - 前端管理页面已实现
   - API接口已就绪

2. **RDAgent侧保持不变**
   - RDAgent继续使用.env文件（代码限制）
   - 通过API同步机制将数据库配置写入.env

3. **AIstock侧自由扩展**
   - QE Agent模型选择直接从数据库读取
   - 不受RDAgent代码限制

### 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                     AIstock 数据库                          │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────┐    │
│  │ providers   │  │   models    │  │   api_configs    │    │
│  └─────────────┘  └─────────────┘  └──────────────────┘    │
└─────────────────────────────────────────────────────────────┘
           │                    │
           ▼                    ▼
┌──────────────────┐   ┌──────────────────────┐
│  RDAgent配置页面  │   │  QE提示词配置页面     │
│  /config/rdagent │   │  /quantevolver/prompts│
│  - 阶段模型映射   │   │  - Agent模型选择      │
│  - 同步到.env    │   │  - 直接读数据库       │
└──────────────────┘   └──────────────────────┘
           │
           ▼ (通过RDAgent API)
┌──────────────────┐
│  RDAgent .env    │
│  (运行时配置)    │
└──────────────────┘
```

---

## 四、具体实现建议

### 1. 新增API：获取数据库中的模型列表

**端点**：`GET /quantevolver/llm-models-from-db`

**返回**：从`aistock_llm_models`表读取所有活跃模型，格式化为前端下拉框可用格式

```json
{
  "ok": true,
  "models": [
    {
      "id": "deepseek/deepseek-chat",
      "name": "DeepSeek Chat",
      "provider": "deepseek",
      "type": "chat",
      "has_api_config": true
    }
  ]
}
```

### 2. 修改QE提示词页面模型下拉框数据源

将前端模型下拉框从读取RDAgent .env改为读取AIstock数据库API。

### 3. 保持RDAgent同步机制不变

- `/config/rdagent-llm` 页面继续管理RDAgent阶段映射
- 更新时通过API写入RDAgent .env文件
- 数据库作为配置的"主副本"

### 4. 可选：添加模型来源标识

在模型列表中标识模型来源：
- `source: "rdagent_sync"` - 从RDAgent同步
- `source: "aistock_managed"` - AIstock自行管理

---

## 五、数据库 vs AIstock .env 对比

| 对比项 | 数据库方案 | AIstock .env方案 |
|--------|-----------|-----------------|
| 查询效率 | 高（索引优化） | 低（文件解析） |
| 并发安全 | 事务保护 | 文件锁问题 |
| 历史追溯 | 变更日志表 | 无 |
| 扩展性 | 字段可扩展 | 格式受限 |
| 多服务共享 | 天然支持 | 需文件同步 |
| 备份恢复 | 数据库备份 | 文件备份 |

**结论**：数据库方案明显优于.env方案。

---

## 六、总结

### 现状
- 模型服务商和模型配置**已存储在AIstock数据库**
- QE提示词页面却从RDAgent .env读取模型列表
- 存在两套独立的模型管理入口

### 建议
1. **统一数据源**：所有模型配置以AIstock数据库为主
2. **RDAgent同步**：通过API将数据库配置同步到RDAgent .env
3. **QE直接读库**：QE Agent模型选择直接从数据库API获取
4. **保持兼容**：保留RDAgent配置页面，作为同步入口

### 实现优先级
1. 高：新增数据库模型列表API，供QE页面使用
2. 中：前端模型下拉框切换数据源
3. 低：添加模型来源标识和过滤功能
