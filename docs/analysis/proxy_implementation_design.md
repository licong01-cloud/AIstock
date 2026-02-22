# LiteLLM Proxy完整设计与开发测试方案

## 一、Proxy性能影响分析

### 1.1 性能开销评估

| 环节 | 直连模式 | Proxy模式 | 开销 |
|------|----------|-----------|------|
| 网络跳数 | 客户端 → 服务商API | 客户端 → Proxy → 服务商API | +1跳 |
| 延迟增加 | ~50-200ms | ~55-210ms | +5-10ms |
| 内存占用 | 本地SDK | 本地SDK + Proxy服务 | +50-200MB |
| CPU占用 | 可忽略 | Proxy路由处理 | +1-5% |

### 1.2 性能影响结论

**影响程度**：轻微，可接受

- **延迟增加**：约5-10ms（本地Proxy）或50-100ms（远程Proxy）
- **吞吐量**：Proxy支持连接池和并发优化，可能反而提升吞吐
- **可靠性**：Proxy可作为熔断器，提升整体稳定性

### 1.3 性能优化建议

| 优化项 | 说明 |
|--------|------|
| 本地部署 | Proxy与RDAgent同机部署，减少网络延迟 |
| 连接池 | Proxy复用HTTP连接，减少握手开销 |
| 缓存 | Proxy支持响应缓存，减少重复请求 |

---

## 二、Proxy配置限制分析

### 2.1 使用Proxy后的配置能力

| 配置场景 | 直连模式 | Proxy模式 | 结论 |
|----------|----------|-----------|------|
| 不同阶段使用不同服务商 | ✅ 支持（专属前缀）| ✅ 支持 | 无差异 |
| 不同阶段使用同一服务商不同模型 | ✅ 支持 | ✅ 支持 | 无差异 |
| 多个OpenAI兼容服务商 | ❌ 冲突 | ✅ 支持 | **Proxy更优** |
| 多个Anthropic代理商 | ❌ 冲突 | ✅ 支持 | **Proxy更优** |
| 动态切换服务商 | 需更新.env | 只需改模型名 | **Proxy更灵活** |

### 2.2 Proxy配置限制

| 限制项 | 说明 | 解决方案 |
|--------|------|----------|
| 需要维护config.yaml | 模型变更需更新配置 | AIstock自动生成config.yaml |
| Proxy服务需持续运行 | 单点故障风险 | Docker重启策略、高可用部署 |
| 模型名需预定义 | 不能随意使用模型名 | 数据库管理模型别名 |

### 2.3 结论

**Proxy模式对未来模型配置完全没有限制**，反而更灵活：
- 支持任意数量的服务商共存
- 支持动态切换模型（只需改模型名）
- 支持负载均衡、重试、熔断等高级功能

---

## 三、自动获取服务商模型列表

### 3.1 LiteLLM Proxy的模型列表API

```bash
# 获取Proxy支持的模型列表
curl http://localhost:4000/v1/models \
  -H "Authorization: Bearer sk-proxy-master-key"

# 响应示例
{
  "data": [
    {"id": "deepseek-chat", "object": "model"},
    {"id": "qwen-turbo", "object": "model"},
    {"id": "glm-4-9b-siliconflow", "object": "model"}
  ]
}
```

### 3.2 各服务商的模型列表API

| 服务商 | API端点 | 说明 |
|--------|---------|------|
| OpenAI | GET /v1/models | 返回所有可用模型 |
| DeepSeek | GET /v1/models | 返回所有可用模型 |
| 阿里云百炼 | GET /v1/models | 返回所有可用模型 |
| 智谱AI | GET /v1/models | 返回所有可用模型 |
| 硅基流动 | GET /v1/models | 返回所有可用模型 |

### 3.3 设计方案

**AIstock新增功能**：

```python
# 伪代码 - 自动获取服务商模型列表

async def fetch_provider_models(provider_id: int) -> list[dict]:
    """从服务商API获取模型列表"""
    provider = get_provider(provider_id)
    api_config = get_api_config(provider_id)
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{api_config.api_base}/models",
            headers={"Authorization": f"Bearer {api_config.api_key}"}
        )
        
    models = []
    for model in response.json().get("data", []):
        models.append({
            "model_name": model["id"],
            "provider_id": provider_id,
            "full_model_id": f"{provider.litellm_prefix}/{model['id']}"
        })
    
    return models
```

**前端功能**：
- 选择服务商后，点击"同步模型列表"按钮
- 自动调用服务商API获取模型列表
- 将模型保存到数据库

---

## 四、AIstock Agent配置影响评估

### 4.1 当前架构分析

**当前配置流程**：

```
┌─────────────────────────────────────────────────────────────────┐
│                        AIstock配置流程                           │
├─────────────────────────────────────────────────────────────────┤
│  1. 用户在UI中选择阶段-模型映射                                   │
│  2. 后端从数据库读取模型信息和API配置                             │
│  3. 后端构建stage_mappings和api_credentials                     │
│  4. 调用RDAgent API更新配置                                      │
│  5. RDAgent更新.env文件                                          │
│     - LITELLM_CHAT_MODEL_MAP                                     │
│     - 各服务商的API_KEY和API_BASE                                 │
└─────────────────────────────────────────────────────────────────┘
```

**当前限制**：
- RDAgent只有一个.env文件
- 所有阶段共享同一组环境变量
- 多个OpenAI兼容服务商会冲突

### 4.2 Proxy模式架构

**Proxy模式配置流程**：

```
┌─────────────────────────────────────────────────────────────────┐
│                     Proxy模式配置流程                            │
├─────────────────────────────────────────────────────────────────┤
│  1. 用户在UI中选择阶段-模型映射                                   │
│  2. 后端从数据库读取模型信息                                      │
│  3. 后端构建stage_mappings（使用Proxy模型别名）                   │
│  4. 调用RDAgent API更新配置                                      │
│  5. RDAgent更新.env文件                                          │
│     - LITELLM_CHAT_MODEL_MAP（使用litellm_proxy/前缀）           │
│     - LITELLM_PROXY_API_KEY                                      │
│     - LITELLM_PROXY_API_BASE                                     │
└─────────────────────────────────────────────────────────────────┘
```

### 4.3 对AIstock Agent配置的影响

| 影响项 | 当前模式 | Proxy模式 | 差异 |
|--------|----------|-----------|------|
| 配置UI | 选择模型 | 选择模型（相同）| 无差异 |
| 数据库 | 存储模型信息 | 存储模型信息+Proxy别名 | 新增字段 |
| 后端逻辑 | 构建api_credentials | 只构建stage_mappings | 简化 |
| RDAgent调用 | 传递多个环境变量 | 只传递Proxy凭证 | 简化 |
| 冲突处理 | 需检测openai冲突 | 无需检测 | 简化 |

**结论**：Proxy模式对AIstock Agent配置的影响是**简化**，而非复杂化。

---

## 五、Env配置方案设计

### 5.1 方案对比

#### 方案A：按Agent配置Env（当前）

```bash
# 每次更新时，只写入当前使用的模型对应的环境变量
LITELLM_CHAT_MODEL_MAP='{"coding":{"model":"openai/glm-4-9b"},"feedback":{"model":"dashscope/qwen"}}'
OPENAI_API_KEY=sk-xxx
OPENAI_API_BASE=https://api.siliconflow.cn/v1
DASHSCOPE_API_KEY=sk-yyy
DASHSCOPE_API_BASE=https://dashscope.aliyuncs.com/...
```

**优点**：
- 只配置使用的模型，env文件简洁

**缺点**：
- 切换模型需更新env
- 多个OpenAI兼容服务商冲突

#### 方案B：全量配置Env

```bash
# 所有服务商的所有模型都配置在env中
OPENAI_API_KEY=sk-xxx
OPENAI_API_BASE=https://api.siliconflow.cn/v1
DASHSCOPE_API_KEY=sk-yyy
DASHSCOPE_API_BASE=...
DEEPSEEK_API_KEY=sk-zzz
DEEPSEEK_API_BASE=...
ZAI_API_KEY=sk-www
ZAI_API_BASE=...
ANTHROPIC_API_KEY=sk-vvv
ANTHROPIC_API_BASE=...

LITELLM_CHAT_MODEL_MAP='{"coding":{"model":"openai/glm-4-9b"},"feedback":{"model":"dashscope/qwen"}}'
```

**优点**：
- 切换模型只需改LITELLM_CHAT_MODEL_MAP
- 无需频繁更新env

**缺点**：
- 多个OpenAI兼容服务商仍冲突
- env文件较长

#### 方案C：Proxy模式（推荐）

```bash
# 只需配置Proxy凭证
LITELLM_PROXY_API_KEY=sk-proxy-master-key
LITELLM_PROXY_API_BASE=http://localhost:4000

# 模型映射使用Proxy别名
LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "litellm_proxy/glm-4-9b-siliconflow"},
  "feedback": {"model": "litellm_proxy/qwen-turbo"},
  "hypothesis": {"model": "litellm_proxy/glm-4-plus-bailian"}
}'
```

**优点**：
- 无冲突限制
- 切换模型只需改模型名
- 支持任意数量服务商

**缺点**：
- 需要维护Proxy服务

### 5.2 推荐方案

**混合模式**：直连 + Proxy并存

```bash
# 直连的服务商（有专属前缀）
DEEPSEEK_API_KEY=sk-xxx
DASHSCOPE_API_KEY=sk-yyy
ZAI_API_KEY=sk-zzz

# Proxy的服务商（OpenAI兼容/多代理商）
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

## 六、完整设计方案

### 6.1 数据库设计

```sql
-- ============================================
-- 服务商表（新增字段）
-- ============================================
ALTER TABLE aistock_llm_providers ADD COLUMN use_proxy BOOLEAN DEFAULT FALSE;
ALTER TABLE aistock_llm_providers ADD COLUMN proxy_model_prefix VARCHAR(100);

-- ============================================
-- 模型表（新增字段）
-- ============================================
ALTER TABLE aistock_llm_models ADD COLUMN proxy_model_alias VARCHAR(200);
-- Proxy模型别名，用于litellm_proxy/{alias}格式

-- ============================================
-- Agent表（新增）
-- ============================================
CREATE TABLE aistock_llm_agents (
    id BIGSERIAL PRIMARY KEY,
    agent_name VARCHAR(100) NOT NULL UNIQUE,
    display_name VARCHAR(200) NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Agent-阶段映射表（新增）
-- ============================================
CREATE TABLE aistock_llm_agent_stage_mappings (
    id BIGSERIAL PRIMARY KEY,
    agent_id BIGINT NOT NULL REFERENCES aistock_llm_agents(id) ON DELETE CASCADE,
    stage_name VARCHAR(50) NOT NULL,
    model_id BIGINT REFERENCES aistock_llm_models(id),
    temperature DECIMAL(3, 2),
    max_tokens INT,
    is_active BOOLEAN DEFAULT TRUE,
    UNIQUE(agent_id, stage_name)
);
```

### 6.2 数据示例

```sql
-- 服务商
INSERT INTO aistock_llm_providers 
(provider_name, display_name, litellm_prefix, use_proxy, proxy_model_prefix) VALUES
('siliconflow', '硅基流动', 'openai', TRUE, 'sf'),
('dashscope', '阿里云百炼', 'dashscope', FALSE, NULL),
('deepseek', 'DeepSeek官方', 'deepseek', FALSE, NULL),
('zhipu', '智谱AI官方', 'zai', FALSE, NULL);

-- 模型
INSERT INTO aistock_llm_models 
(provider_id, model_name, display_name, full_model_id, proxy_model_alias) VALUES
-- 硅基流动（使用Proxy）
((SELECT id FROM aistock_llm_providers WHERE provider_name='siliconflow'),
 'THUDM/glm-4-9b-0414', 'GLM-4-9B (硅基流动)', 
 'openai/THUDM/glm-4-9b-0414', 'sf-glm-4-9b'),
-- 阿里云百炼（直连）
((SELECT id FROM aistock_llm_providers WHERE provider_name='dashscope'),
 'qwen-turbo', 'Qwen Turbo (阿里云百炼)',
 'dashscope/qwen-turbo', NULL);

-- Agent
INSERT INTO aistock_llm_agents (agent_name, display_name) VALUES
('quant_coder', '量化因子编码器'),
('quant_feedback', '量化反馈分析'),
('quant_hypothesis', '量化假设生成');

-- Agent-阶段映射
INSERT INTO aistock_llm_agent_stage_mappings 
(agent_id, stage_name, model_id, temperature) VALUES
((SELECT id FROM aistock_llm_agents WHERE agent_name='quant_coder'), 
 'coding', 
 (SELECT id FROM aistock_llm_models WHERE proxy_model_alias='sf-glm-4-9b'), 
 0.7);
```

### 6.3 Proxy配置自动生成

**AIstock后端新增功能**：

```python
def generate_proxy_config_yaml() -> str:
    """根据数据库配置生成Proxy的config.yaml"""
    providers = get_all_providers(use_proxy=True)
    
    model_list = []
    for provider in providers:
        models = get_provider_models(provider.id)
        for model in models:
            model_list.append({
                "model_name": model.proxy_model_alias,
                "litellm_params": {
                    "model": model.full_model_id,
                    "api_base": provider.api_base_url,
                    "api_key": f"os.environ/{provider.provider_name.upper()}_API_KEY"
                }
            })
    
    config = {
        "model_list": model_list,
        "general_settings": {
            "master_key": "os.environ/LITELLM_PROXY_MASTER_KEY"
        }
    }
    
    return yaml.dump(config)
```

### 6.4 配置更新流程

```
┌─────────────────────────────────────────────────────────────────┐
│                    配置更新完整流程                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. 用户在UI中选择Agent和阶段-模型映射                           │
│     └─ 前端显示：服务商名 + 模型名（如"硅基流动 - GLM-4-9B"）    │
│                                                                 │
│  2. 后端保存到aistock_llm_agent_stage_mappings表                │
│                                                                 │
│  3. 后端判断模型是否使用Proxy                                    │
│     ├─ use_proxy=TRUE: 使用litellm_proxy/{alias}格式            │
│     └─ use_proxy=FALSE: 使用full_model_id                        │
│                                                                 │
│  4. 构建LITELLM_CHAT_MODEL_MAP                                  │
│     {                                                           │
│       "coding": {"model": "litellm_proxy/sf-glm-4-9b"},         │
│       "feedback": {"model": "dashscope/qwen-turbo"}             │
│     }                                                           │
│                                                                 │
│  5. 构建环境变量                                                 │
│     ├─ 直连服务商：{PREFIX}_API_KEY, {PREFIX}_API_BASE          │
│     └─ Proxy：LITELLM_PROXY_API_KEY, LITELLM_PROXY_API_BASE      │
│                                                                 │
│  6. 调用RDAgent API更新.env                                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 七、开发任务清单

### 7.1 后端开发任务

| 任务 | 文件 | 说明 |
|------|------|------|
| 1. 数据库迁移 | 新建迁移脚本 | 新增use_proxy、proxy_model_alias等字段 |
| 2. 服务商API | `routers/llm_providers.py` | 新增同步模型列表接口 |
| 3. 模型管理API | `routers/llm_models.py` | 支持Proxy模型别名管理 |
| 4. Agent管理API | `routers/llm_agents.py` | 新增Agent和阶段映射管理 |
| 5. Proxy配置生成 | `services/proxy_config.py` | 自动生成config.yaml |
| 6. 配置更新逻辑 | `routers/rdagent_llm_config.py` | 支持Proxy模式 |

### 7.2 前端开发任务

| 任务 | 文件 | 说明 |
|------|------|------|
| 1. 服务商管理页 | `app/llm/providers/page.tsx` | 新增同步模型列表按钮 |
| 2. 模型管理页 | `app/llm/models/page.tsx` | 支持Proxy模型别名编辑 |
| 3. Agent配置页 | `app/llm/agents/page.tsx` | 新增Agent和阶段映射配置 |
| 4. 配置预览 | 各配置页 | 显示将要生成的.env内容 |

### 7.3 Proxy部署任务

| 任务 | 说明 |
|------|------|
| 1. 创建配置目录 | `F:/Dev/litellm-proxy/` |
| 2. 编写config.yaml | 根据数据库配置生成 |
| 3. 编写启动脚本 | `start_proxy.sh` 或 Docker Compose |
| 4. 配置自动重启 | systemd或Docker重启策略 |

---

## 八、验证方案

### 8.1 RDAgent模型测试命令验证

**测试脚本**：`debug_tools/test_llm_config.py`

```python
"""
LLM配置验证脚本
验证不同阶段使用不同模型的配置是否正确
"""
import os
import sys
sys.path.insert(0, 'F:/Dev/RD-Agent-main')

from litellm import completion

def test_direct_mode():
    """测试直连模式"""
    # 设置环境变量
    os.environ['DASHSCOPE_API_KEY'] = 'sk-xxx'
    os.environ['DEEPSEEK_API_KEY'] = 'sk-yyy'
    
    # 测试阿里云百炼
    response = completion(
        model="dashscope/qwen-turbo",
        messages=[{"role": "user", "content": "你好，请回复1"}],
        max_tokens=10
    )
    print(f"阿里云百炼响应: {response.choices[0].message.content}")
    
    # 测试DeepSeek
    response = completion(
        model="deepseek/deepseek-chat",
        messages=[{"role": "user", "content": "你好，请回复2"}],
        max_tokens=10
    )
    print(f"DeepSeek响应: {response.choices[0].message.content}")

def test_proxy_mode():
    """测试Proxy模式"""
    os.environ['LITELLM_PROXY_API_KEY'] = 'sk-proxy-master-key'
    os.environ['LITELLM_PROXY_API_BASE'] = 'http://localhost:4000'
    
    # 测试Proxy模型
    response = completion(
        model="litellm_proxy/sf-glm-4-9b",
        messages=[{"role": "user", "content": "你好，请回复3"}],
        max_tokens=10
    )
    print(f"Proxy响应: {response.choices[0].message.content}")

def test_mixed_mode():
    """测试混合模式"""
    # 直连服务商
    os.environ['DASHSCOPE_API_KEY'] = 'sk-xxx'
    # Proxy
    os.environ['LITELLM_PROXY_API_KEY'] = 'sk-proxy-key'
    os.environ['LITELLM_PROXY_API_BASE'] = 'http://localhost:4000'
    
    # 测试直连
    response = completion(
        model="dashscope/qwen-turbo",
        messages=[{"role": "user", "content": "测试直连"}],
        max_tokens=10
    )
    print(f"直连响应: {response.choices[0].message.content}")
    
    # 测试Proxy
    response = completion(
        model="litellm_proxy/sf-glm-4-9b",
        messages=[{"role": "user", "content": "测试Proxy"}],
        max_tokens=10
    )
    print(f"Proxy响应: {response.choices[0].message.content}")

if __name__ == "__main__":
    print("=== 测试直连模式 ===")
    test_direct_mode()
    
    print("\n=== 测试Proxy模式 ===")
    test_proxy_mode()
    
    print("\n=== 测试混合模式 ===")
    test_mixed_mode()
```

### 8.2 RDAgent运行期间模型调用验证

**验证脚本**：`debug_tools/test_rdagent_llm_call.py`

```python
"""
验证RDAgent运行期间的模型调用
不执行真正的RDAgent任务，只验证模型配置是否正确加载
"""
import os
import sys
sys.path.insert(0, 'F:/Dev/RD-Agent-main')

from rdagent.oai.llm_conf import LLM_SETTINGS

def test_stage_model_loading():
    """测试阶段模型加载"""
    from rdagent.oai.backend.litellm import LiteLLMAPIBackend
    
    # 设置环境变量
    os.environ['LITELLM_CHAT_MODEL_MAP'] = '''{
        "coding": {"model": "litellm_proxy/sf-glm-4-9b", "temperature": "0.7"},
        "feedback": {"model": "dashscope/qwen-turbo", "temperature": "0.5"},
        "default": {"model": "deepseek/deepseek-chat", "temperature": "0.7"}
    }'''
    os.environ['LITELLM_PROXY_API_KEY'] = 'sk-proxy-key'
    os.environ['LITELLM_PROXY_API_BASE'] = 'http://localhost:4000'
    os.environ['DASHSCOPE_API_KEY'] = 'sk-dashscope'
    os.environ['DEEPSEEK_API_KEY'] = 'sk-deepseek'
    
    # 测试各阶段模型
    backend = LiteLLMAPIBackend()
    
    # 获取各阶段的模型配置
    for stage in ['coding', 'feedback', 'default']:
        kwargs = backend.get_complete_kwargs(stage=stage)
        print(f"{stage}阶段模型: {kwargs.get('model')}")
        print(f"{stage}阶段temperature: {kwargs.get('temperature')}")
        
if __name__ == "__main__":
    test_stage_model_loading()
```

### 8.3 AIstock UI配置验证

**验证步骤**：

1. **服务商配置验证**
   - 在UI中添加服务商
   - 点击"同步模型列表"
   - 验证模型是否正确保存到数据库

2. **Agent配置验证**
   - 创建Agent
   - 为Agent配置不同阶段的模型
   - 验证数据库中agent_stage_mappings表

3. **配置更新验证**
   - 点击"应用配置"
   - 验证RDAgent的.env文件更新
   - 验证LITELLM_CHAT_MODEL_MAP内容

4. **多Agent验证**
   - 创建多个Agent
   - 每个Agent使用不同服务商的模型
   - 验证无冲突

### 8.4 验证检查清单

| 验证项 | 验证方法 | 预期结果 |
|--------|----------|----------|
| 直连模式 | test_llm_config.py | 各服务商模型正常响应 |
| Proxy模式 | test_llm_config.py | Proxy模型正常响应 |
| 混合模式 | test_llm_config.py | 直连和Proxy都正常 |
| 阶段模型加载 | test_rdagent_llm_call.py | 各阶段加载正确模型 |
| UI配置保存 | 数据库查询 | 数据正确保存 |
| UI配置更新 | .env文件检查 | 内容正确更新 |
| 多Agent共存 | 创建多个Agent配置 | 无冲突 |

---

## 九、最佳设计方案总结

### 9.1 推荐架构

```
┌─────────────────────────────────────────────────────────────────┐
│                       推荐架构                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  数据库层                                                        │
│  ├─ aistock_llm_providers: 服务商（use_proxy标记）               │
│  ├─ aistock_llm_models: 模型（proxy_model_alias）               │
│  ├─ aistock_llm_agents: Agent定义                               │
│  └─ aistock_llm_agent_stage_mappings: Agent阶段映射             │
│                                                                 │
│  配置层                                                          │
│  ├─ Proxy config.yaml: 由AIstock根据数据库自动生成               │
│  └─ RDAgent .env: 由AIstock根据Agent配置更新                     │
│                                                                 │
│  运行层                                                          │
│  ├─ LiteLLM Proxy: 处理OpenAI兼容服务商                          │
│  └─ RDAgent: 通过litellm_proxy/前缀调用Proxy模型                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 9.2 Env配置策略

**推荐：混合模式**

```bash
# 直连服务商（有专属前缀，无冲突）
DEEPSEEK_API_KEY=sk-xxx
DASHSCOPE_API_KEY=sk-yyy
ZAI_API_KEY=sk-zzz

# Proxy服务商（OpenAI兼容/多代理商）
LITELLM_PROXY_API_KEY=sk-proxy-key
LITELLM_PROXY_API_BASE=http://localhost:4000

# 阶段映射
LITELLM_CHAT_MODEL_MAP='{
  "coding": {"model": "litellm_proxy/sf-glm-4-9b"},
  "feedback": {"model": "dashscope/qwen-turbo"},
  "default": {"model": "deepseek/deepseek-chat"}
}'
```

### 9.3 核心优势

| 优势 | 说明 |
|------|------|
| **无冲突限制** | 任意数量服务商共存 |
| **灵活切换** | 只需改模型名，无需改环境变量 |
| **统一管理** | 数据库管理所有配置 |
| **自动同步** | 可从服务商API同步模型列表 |
| **向后兼容** | 直连模式继续可用 |

---

## 十、实施计划

### 10.1 阶段划分

| 阶段 | 任务 | 工期 |
|------|------|------|
| **Phase 1** | 数据库改造 + 后端API | 2天 |
| **Phase 2** | 前端UI改造 | 2天 |
| **Phase 3** | Proxy部署 + 配置生成 | 1天 |
| **Phase 4** | 验证测试 | 1天 |

### 10.2 依赖关系

```
Phase 1 (数据库+后端)
    ↓
Phase 2 (前端UI) ← 依赖后端API
    ↓
Phase 3 (Proxy部署) ← 依赖数据库配置
    ↓
Phase 4 (验证测试) ← 依赖全部完成
```

### 10.3 风险与缓解

| 风险 | 缓解措施 |
|------|----------|
| Proxy服务中断 | Docker自动重启、健康检查 |
| 模型API变更 | 定期同步模型列表 |
| 配置冲突 | 前端添加冲突检测提示 |
