# LLM模型配置系统完整设计方案（补充完善版）

## 一、设计目标

### 1.1 核心目标

| 目标 | 说明 |
|------|------|
| **统一数据库** | RDAgent和AIstock共用一套模型服务商和模型数据库 |
| **独立配置** | RDAgent和AIstock可互不干扰地独立配置模型使用 |
| **模型同步** | 添加服务商后可自动获取模型列表，按类型筛选批量导入 |
| **服务商区分** | 模型表存储服务商信息，区分不同服务商提供的同一模型 |
| **先服务商后模型** | 配置时先选择服务商，再选择该服务商的模型 |

### 1.2 系统架构

```
┌─────────────────────────────────────────────────────────────────┐
│                     统一数据库层                                 │
├─────────────────────────────────────────────────────────────────┤
│  aistock_llm_providers     │ 服务商表                           │
│  aistock_llm_models        │ 模型表（关联服务商）                │
│  aistock_llm_api_configs   │ API配置表                          │
│  aistock_llm_rdagent_stages│ RDAgent阶段映射表                   │
│  aistock_llm_aistock_agents│ AIstock Agent配置表                 │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌─────────────────────────┐     ┌─────────────────────────┐
│      RDAgent配置        │     │     AIstock配置         │
├─────────────────────────┤     ├─────────────────────────┤
│ coding → 服务商A-模型X   │     │ agent1 → 服务商B-模型Y  │
│ feedback → 服务商B-模型Y │     │ agent2 → 服务商A-模型Z  │
│ default → 服务商C-模型Z  │     │ agent3 → 服务商C-模型W  │
└─────────────────────────┘     └─────────────────────────┘
              │                               │
              └───────────────┬───────────────┘
                              ▼
                    ┌─────────────────┐
                    │   LiteLLM调用    │
                    └─────────────────┘
```

---

## 二、数据库设计

### 2.1 服务商表（aistock_llm_providers）

```sql
CREATE TABLE IF NOT EXISTS aistock_llm_providers (
    id BIGSERIAL PRIMARY KEY,
    provider_name VARCHAR(100) NOT NULL UNIQUE,    -- 服务商标识
    display_name VARCHAR(200) NOT NULL,             -- 显示名称
    provider_type VARCHAR(50) DEFAULT 'official',   -- 类型: official/agent/proxy
    litellm_prefix VARCHAR(50) NOT NULL,            -- LiteLLM前缀
    api_base_url VARCHAR(500),                       -- 默认API Base
    default_env_prefix VARCHAR(50),                  -- 默认环境变量前缀
    use_proxy BOOLEAN DEFAULT FALSE,                 -- 是否使用Proxy
    proxy_model_prefix VARCHAR(100),                 -- Proxy模型前缀
    supports_chat BOOLEAN DEFAULT FALSE,             -- 支持对话
    supports_embedding BOOLEAN DEFAULT FALSE,        -- 支持嵌入
    supports_reasoner BOOLEAN DEFAULT FALSE,         -- 支持推理
    supports_vision BOOLEAN DEFAULT FALSE,           -- 支持视觉
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE aistock_llm_providers IS 'LLM服务商表';
COMMENT ON COLUMN aistock_llm_providers.provider_type IS 'official=官方, agent=代理商, proxy=代理服务';
COMMENT ON COLUMN aistock_llm_providers.use_proxy IS '是否需要通过LiteLLM Proxy访问';
```

### 2.2 模型表（aistock_llm_models）

```sql
CREATE TABLE IF NOT EXISTS aistock_llm_models (
    id BIGSERIAL PRIMARY KEY,
    provider_id BIGINT NOT NULL REFERENCES aistock_llm_providers(id) ON DELETE CASCADE,
    model_name VARCHAR(200) NOT NULL,               -- 服务商侧模型名
    display_name VARCHAR(200) NOT NULL,              -- 显示名称
    full_model_id VARCHAR(300) NOT NULL UNIQUE,      -- LiteLLM完整ID
    model_type VARCHAR(50) NOT NULL,                 -- 模型类型
    model_category VARCHAR(100),                     -- 模型分类
    proxy_model_alias VARCHAR(200),                  -- Proxy模型别名
    context_window INT,                              -- 上下文窗口
    max_output_tokens INT,                           -- 最大输出token
    input_price DECIMAL(10, 4),                      -- 输入价格($/1M tokens)
    output_price DECIMAL(10, 4),                     -- 输出价格($/1M tokens)
    description TEXT,
    api_config_id BIGINT REFERENCES aistock_llm_api_configs(id),
    is_synced BOOLEAN DEFAULT FALSE,                  -- 是否从API同步
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(provider_id, model_name)
);

COMMENT ON TABLE aistock_llm_models IS 'LLM模型表';
COMMENT ON COLUMN aistock_llm_models.model_type IS 'chat=对话, embedding=嵌入, reasoner=推理, vision=视觉';
COMMENT ON COLUMN aistock_llm_models.is_synced IS '是否从服务商API自动同步';
```

### 2.3 API配置表（aistock_llm_api_configs）

```sql
CREATE TABLE IF NOT EXISTS aistock_llm_api_configs (
    id BIGSERIAL PRIMARY KEY,
    provider_id BIGINT NOT NULL REFERENCES aistock_llm_providers(id) ON DELETE CASCADE,
    config_name VARCHAR(100) DEFAULT 'default',      -- 配置名称
    api_base VARCHAR(500) NOT NULL,                  -- API Base URL
    api_key VARCHAR(500) NOT NULL,                   -- API Key
    env_api_base_name VARCHAR(100),                  -- 环境变量名(API Base)
    env_api_key_name VARCHAR(100),                   -- 环境变量名(API Key)
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2.4 RDAgent阶段映射表（aistock_llm_rdagent_stages）

```sql
CREATE TABLE IF NOT EXISTS aistock_llm_rdagent_stages (
    id BIGSERIAL PRIMARY KEY,
    stage_name VARCHAR(50) NOT NULL UNIQUE,          -- 阶段名称
    stage_display_name VARCHAR(100),                 -- 阶段显示名称
    model_id BIGINT REFERENCES aistock_llm_models(id),
    temperature DECIMAL(3, 2) DEFAULT 0.7,
    max_tokens INT DEFAULT 4000,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 预置RDAgent阶段
INSERT INTO aistock_llm_rdagent_stages (stage_name, stage_display_name, description) VALUES
('coding', '编码阶段', '因子代码生成'),
('feedback', '反馈阶段', '实验结果分析'),
('hypothesis', '假设阶段', '假设生成'),
('summary', '总结阶段', '总结生成'),
('default', '默认阶段', '默认模型')
ON CONFLICT (stage_name) DO NOTHING;
```

### 2.5 AIstock Agent配置表（aistock_llm_aistock_agents）

```sql
CREATE TABLE IF NOT EXISTS aistock_llm_aistock_agents (
    id BIGSERIAL PRIMARY KEY,
    agent_key VARCHAR(100) NOT NULL UNIQUE,          -- Agent标识
    agent_name VARCHAR(200) NOT NULL,                -- Agent名称
    agent_type VARCHAR(50),                          -- Agent类型
    model_id BIGINT REFERENCES aistock_llm_models(id),
    temperature DECIMAL(3, 2) DEFAULT 0.7,
    max_tokens INT DEFAULT 4000,
    system_prompt TEXT,                              -- 系统提示词
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 预置AIstock Agent
INSERT INTO aistock_llm_aistock_agents (agent_key, agent_name, agent_type, description) VALUES
('factor_analyzer', '因子分析器', 'analysis', '因子数据分析'),
('strategy_generator', '策略生成器', 'generation', '策略代码生成'),
('data_processor', '数据处理器', 'processing', '数据处理任务'),
('report_generator', '报告生成器', 'generation', '分析报告生成')
ON CONFLICT (agent_key) DO NOTHING;
```

---

## 三、模型类型分类

### 3.1 模型类型定义

| model_type | 说明 | 示例 |
|------------|------|------|
| `chat` | 对话模型 | GPT-4, Qwen, GLM, DeepSeek |
| `embedding` | 嵌入模型 | text-embedding-3, bge-large |
| `reasoner` | 推理模型 | o1, DeepSeek-Reasoner |
| `vision` | 视觉模型 | GPT-4V, Qwen-VL |

### 3.2 模型分类定义

| model_category | 说明 |
|----------------|------|
| `general` | 通用模型 |
| `code` | 代码专用 |
| `math` | 数学专用 |
| `financial` | 金融专用 |
| `scientific` | 科研专用 |

---

## 四、模型同步功能设计

### 4.1 同步流程

```
┌─────────────────────────────────────────────────────────────────┐
│                     模型同步流程                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. 用户选择服务商                                               │
│     └─ 从aistock_llm_providers表获取已配置的服务商               │
│                                                                 │
│  2. 用户选择模型类型筛选                                          │
│     └─ chat / embedding / reasoner / vision / all              │
│                                                                 │
│  3. 点击"获取模型列表"按钮                                        │
│     └─ 调用服务商API: GET {api_base}/models                      │
│     └─ 解析返回的模型列表                                         │
│     └─ 根据模型类型筛选                                           │
│                                                                 │
│  4. 显示模型列表（带复选框）                                       │
│     └─ 显示: 模型名、类型、上下文窗口等                           │
│     └─ 每个模型有复选框                                           │
│                                                                 │
│  5. 用户勾选要导入的模型                                          │
│     └─ 支持全选/反选                                              │
│                                                                 │
│  6. 点击"导入选中模型"按钮                                        │
│     └─ 批量插入到aistock_llm_models表                            │
│     └─ 自动关联服务商ID                                           │
│     └─ 自动生成full_model_id                                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 API设计

**获取服务商模型列表**：

```
POST /api/v1/llm/providers/{provider_id}/models/fetch
Request:
{
  "model_type": "chat",  // 可选: chat/embedding/reasoner/vision/all
  "api_base": "https://api.siliconflow.cn/v1",  // 可选，覆盖默认
  "api_key": "sk-xxx"    // 可选，覆盖默认
}

Response:
{
  "success": true,
  "models": [
    {
      "model_name": "Qwen/Qwen2.5-72B-Instruct",
      "model_type": "chat",
      "context_window": 32768,
      "input_price": 0.35,
      "output_price": 0.4
    },
    {
      "model_name": "THUDM/glm-4-9b-0414",
      "model_type": "chat",
      "context_window": 131072
    }
  ],
  "total": 2,
  "filtered": 2
}
```

**批量导入模型**：

```
POST /api/v1/llm/models/batch-import
Request:
{
  "provider_id": 1,
  "models": [
    {
      "model_name": "Qwen/Qwen2.5-72B-Instruct",
      "display_name": "Qwen2.5-72B (硅基流动)",
      "model_type": "chat",
      "context_window": 32768
    },
    {
      "model_name": "THUDM/glm-4-9b-0414",
      "display_name": "GLM-4-9B (硅基流动)",
      "model_type": "chat",
      "context_window": 131072
    }
  ]
}

Response:
{
  "success": true,
  "imported_count": 2,
  "skipped_count": 0,  // 已存在的模型
  "imported_models": [
    {
      "id": 101,
      "model_name": "Qwen/Qwen2.5-72B-Instruct",
      "full_model_id": "openai/Qwen/Qwen2.5-72B-Instruct"
    }
  ]
}
```

---

## 五、模型选择UI设计

### 5.1 选择流程

```
┌─────────────────────────────────────────────────────────────────┐
│                     模型选择流程                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Step 1: 选择服务商                                              │
│     ┌─────────────────────────────────────────┐                │
│     │ 服务商: [下拉选择]                        │                │
│     │   - 硅基流动                             │                │
│     │   - 阿里云百炼                           │                │
│     │   - DeepSeek官方                         │                │
│     │   - 智谱AI官方                           │                │
│     └─────────────────────────────────────────┘                │
│                                                                 │
│  Step 2: 选择模型（只显示所选服务商的模型）                       │
│     ┌─────────────────────────────────────────┐                │
│     │ 模型: [下拉选择]                          │                │
│     │   - GLM-4-9B (硅基流动)                  │                │
│     │   - Qwen2.5-72B (硅基流动)               │                │
│     │   - DeepSeek-V3 (硅基流动)               │                │
│     └─────────────────────────────────────────┘                │
│                                                                 │
│  Step 3: 配置参数                                                │
│     ┌─────────────────────────────────────────┐                │
│     │ Temperature: [滑块 0.0-1.0]              │                │
│     │ Max Tokens: [输入框]                     │                │
│     └─────────────────────────────────────────┘                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 显示格式

**服务商选择下拉框**：
```
显示格式: {display_name}
示例:
  - 硅基流动
  - 阿里云百炼
  - DeepSeek官方
```

**模型选择下拉框**：
```
显示格式: {model_display_name} ({provider_name})
示例（选择硅基流动后）:
  - GLM-4-9B (硅基流动)
  - Qwen2.5-72B (硅基流动)
  - DeepSeek-V3 (硅基流动)

示例（选择阿里云百炼后）:
  - Qwen-Turbo (阿里云百炼)
  - GLM-4-Plus (阿里云百炼)
  - DeepSeek-V3 (阿里云百炼)
```

---

## 六、配置独立性设计

### 6.1 RDAgent配置

**配置表**：`aistock_llm_rdagent_stages`

**配置流程**：
1. 用户在RDAgent配置页面
2. 为每个阶段选择服务商 → 选择模型
3. 保存到`aistock_llm_rdagent_stages`表
4. 调用RDAgent API更新.env文件

**生成的.env内容**：
```bash
# 根据选择的模型自动生成
LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "openai/THUDM/glm-4-9b-0414", "temperature": "0.7"},
  "feedback": {"model": "dashscope/qwen-turbo", "temperature": "0.5"},
  "default": {"model": "deepseek/deepseek-chat", "temperature": "0.7"}
}'

# 根据模型对应的服务商自动添加API凭证
OPENAI_API_KEY=sk-siliconflow
OPENAI_API_BASE=https://api.siliconflow.cn/v1
DASHSCOPE_API_KEY=sk-dashscope
DASHSCOPE_API_BASE=https://dashscope.aliyuncs.com/...
DEEPSEEK_API_KEY=sk-deepseek
DEEPSEEK_API_BASE=https://api.deepseek.com
```

### 6.2 AIstock Agent配置

**配置表**：`aistock_llm_aistock_agents`

**配置流程**：
1. 用户在AIstock Agent配置页面
2. 为每个Agent选择服务商 → 选择模型
3. 保存到`aistock_llm_aistock_agents`表
4. AIstock运行时从数据库读取配置

**运行时调用**：
```python
# AIstock运行时获取Agent配置
def get_agent_llm_config(agent_key: str) -> dict:
    config = db.query("""
        SELECT m.full_model_id, a.temperature, a.max_tokens,
               ac.api_key, ac.api_base
        FROM aistock_llm_aistock_agents a
        JOIN aistock_llm_models m ON a.model_id = m.id
        JOIN aistock_llm_providers p ON m.provider_id = p.id
        LEFT JOIN aistock_llm_api_configs ac ON p.id = ac.provider_id
        WHERE a.agent_key = %s AND a.is_active = true
    """, (agent_key,))
    
    return {
        "model": config["full_model_id"],
        "temperature": config["temperature"],
        "api_key": config["api_key"],
        "api_base": config["api_base"]
    }
```

### 6.3 独立性保证

| 方面 | RDAgent | AIstock | 独立性 |
|------|---------|---------|--------|
| 配置表 | aistock_llm_rdagent_stages | aistock_llm_aistock_agents | ✅ 独立 |
| 配置页面 | /rdagent/llm-config | /aistock/agent-config | ✅ 独立 |
| 生效方式 | 更新RDAgent .env | AIstock运行时读取 | ✅ 独立 |
| 模型选择 | 先服务商后模型 | 先服务商后模型 | ✅ 一致 |

---

## 七、前端页面设计

### 7.1 服务商管理页面

**路径**：`/llm/providers`

**功能**：
- 服务商列表（增删改查）
- 每个服务商显示：名称、类型、LiteLLM前缀、支持的模型类型
- 操作按钮：编辑、删除、同步模型

### 7.2 模型管理页面

**路径**：`/llm/models`

**功能**：
- 模型列表（可按服务商、类型筛选）
- 每个模型显示：名称、服务商、类型、上下文窗口
- 操作按钮：编辑、删除
- 批量导入功能

**模型同步弹窗**：
```
┌─────────────────────────────────────────────────────────────────┐
│  从服务商同步模型                                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  服务商: [硅基流动 ▼]                                           │
│                                                                 │
│  模型类型: ○ 全部  ● 对话  ○ 嵌入  ○ 推理  ○ 视觉              │
│                                                                 │
│  [获取模型列表]                                                  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │ □ 模型名称                    类型    上下文   价格       │ │
│  ├───────────────────────────────────────────────────────────┤ │
│  │ ☑ Qwen/Qwen2.5-72B-Instruct  chat    32K     $0.35/$0.4  │ │
│  │ ☑ THUDM/glm-4-9b-0414        chat    128K    -          │ │
│  │ ☐ BAAI/bge-large-zh-v1.5     embedding -       -        │ │
│  │ ☑ deepseek-ai/DeepSeek-V3    chat    64K     $0.1/$0.2  │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  [全选] [反选]                              [导入选中模型]       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 7.3 RDAgent配置页面

**路径**：`/rdagent/llm-config`

**功能**：
- 显示RDAgent各阶段
- 每个阶段配置：服务商选择 → 模型选择 → 参数配置
- 预览将要生成的.env内容
- 应用配置按钮

### 7.4 AIstock Agent配置页面

**路径**：`/aistock/agent-config`

**功能**：
- 显示AIstock各Agent
- 每个Agent配置：服务商选择 → 模型选择 → 参数配置
- 系统提示词编辑
- 保存配置按钮

---

## 八、开发任务清单

### 8.1 数据库改造

| 任务 | 文件 | 说明 |
|------|------|------|
| 1. 创建迁移脚本 | `migrations/add_llm_tables.sql` | 创建所有表 |
| 2. 添加字段 | `migrations/alter_llm_tables.sql` | 添加新字段 |
| 3. 预置数据 | `migrations/seed_llm_data.sql` | 预置阶段和Agent |

### 8.2 后端API开发

| 任务 | 文件 | 说明 |
|------|------|------|
| 1. 服务商API | `routers/llm_providers.py` | CRUD + 模型同步 |
| 2. 模型API | `routers/llm_models.py` | CRUD + 批量导入 |
| 3. RDAgent配置API | `routers/llm_rdagent_config.py` | 阶段配置 |
| 4. AIstock Agent API | `routers/llm_aistock_agents.py` | Agent配置 |
| 5. 模型同步服务 | `services/llm_model_sync.py` | 从服务商API获取模型 |

### 8.3 前端开发

| 任务 | 文件 | 说明 |
|------|------|------|
| 1. 服务商管理页 | `app/llm/providers/page.tsx` | 服务商CRUD |
| 2. 模型管理页 | `app/llm/models/page.tsx` | 模型CRUD + 同步 |
| 3. RDAgent配置页 | `app/rdagent/llm-config/page.tsx` | 阶段配置 |
| 4. AIstock Agent页 | `app/aistock/agent-config/page.tsx` | Agent配置 |
| 5. 公共组件 | `components/llm/ModelSelector.tsx` | 服务商-模型选择器 |

---

## 九、验证方案

### 9.1 数据库验证

```sql
-- 验证表结构
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' AND table_name LIKE 'aistock_llm%';

-- 验证预置数据
SELECT * FROM aistock_llm_rdagent_stages;
SELECT * FROM aistock_llm_aistock_agents;
```

### 9.2 API验证

```bash
# 获取服务商列表
curl http://localhost:8001/api/v1/llm/providers

# 同步模型
curl -X POST http://localhost:8001/api/v1/llm/providers/1/models/fetch \
  -H "Content-Type: application/json" \
  -d '{"model_type": "chat"}'

# 批量导入
curl -X POST http://localhost:8001/api/v1/llm/models/batch-import \
  -H "Content-Type: application/json" \
  -d '{"provider_id": 1, "models": [...]}'

# 配置RDAgent阶段
curl -X PUT http://localhost:8001/api/v1/llm/rdagent/stages/coding \
  -H "Content-Type: application/json" \
  -d '{"provider_id": 1, "model_id": 101, "temperature": 0.7}'
```

### 9.3 功能验证

| 验证项 | 验证方法 | 预期结果 |
|--------|----------|----------|
| 服务商添加 | UI添加服务商 | 数据库有记录 |
| 模型同步 | 点击获取模型列表 | 显示服务商模型 |
| 批量导入 | 勾选模型并导入 | 模型表有记录 |
| RDAgent配置 | 选择服务商-模型 | 配置保存成功 |
| AIstock配置 | 选择服务商-模型 | 配置保存成功 |
| 独立性 | 两边配置不同模型 | 互不影响 |
