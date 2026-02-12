# RD-Agent SOTA 因子去重设计方案

## 1. 现状分析

### 1.1 当前问题

在 RD-Agent 因子研发过程中，存在以下核心问题：

| 问题 | 说明 | 影响 |
|------|------|------|
| **重复因子研发** | LLM 可能生成与已有因子相同或相似的因子 | 浪费计算资源，降低研发效率 |
| **缺乏全局去重机制** | 每次任务独立运行，无法感知已有因子库 | 因子库冗余，难以管理 |
| **语义相似因子无法识别** | 只能检测精确重复，无法识别逻辑相似的因子 | 如 `momentum_20d` 和 `momentum_19d` 视为不同因子 |
| **因子列表增长导致性能下降** | 因子数量增加，注入到 Prompt 的内容增多，可能超出 token 限制 | LLM 判断能力下降，响应速度变慢 |

### 1.2 技术约束

根据用户约束，方案必须满足：

- **不修改 RD-Agent 核心代码**：只能通过配置文件和提示词实现
- **不修改 RD-Agent 目录**：所有改动在 AIstock 项目内
- **不使用 Streamlit**：必须使用 AIstock Next.js 前端和 FastAPI 后端
- **AIstock 后端端口 8001**：所有 API 请求使用 `http://localhost:8001`

## 2. 需求分析

### 2.1 核心需求

| 需求 | 优先级 | 说明 |
|------|--------|------|
| **全局因子注册** | 高 | 建立全局因子注册表，记录所有已研发因子 |
| **去重机制** | 高 | 避免重复研发相同或相似的因子 |
| **语义相似度检测** | 中 | 识别逻辑相似的因子（如 `momentum_20d` 和 `momentum_19d`） |
| **因子列表优化** | 中 | 解决因子列表增长导致的 token 消耗和性能问题 |
| **易于维护** | 中 | 新因子产出后可追加写入，无需手动维护 |

### 2.2 非功能需求

| 需求 | 说明 |
|------|------|
| **零侵入 RD-Agent** | 所有去重逻辑在 AIstock 后端实现 |
| **高效计算** | 去重算法时间复杂度低，适合大规模因子库 |
| **可扩展性** | 易于添加新的特征维度和去重规则 |
| **可解释性** | 算法清晰，易于理解和维护 |

## 3. 技术方案对比

### 3.1 方案1：精确匹配（名称 + 表达式）

#### 原理
通过比对因子名称和表达式是否完全相同来判断重复。

#### 优点
- 实现简单
- 计算快速

#### 缺点
- 无法检测语义相似的因子
- 表达式格式敏感

#### 适用场景
- 精确去重，不考虑语义相似

### 3.2 方案2：特征哈希（Feature Hashing）

#### 原理
将因子特征（字段、操作、时间窗口等）通过哈希函数转换为数值 ID，通过哈希值判断重复。

#### 优点
- 成熟技术，广泛应用于机器学习
- 高效计算，O(1) 时间复杂度
- 易于实现

#### 缺点
- 无法检测语义相似的因子
- 哈希碰撞可能导致误判

#### 适用场景
- 精确去重，大规模因子库

#### 参考资源
- [Feature Hashing - Wikipedia](https://en.wikipedia.org/wiki/Feature_hashing)
- [Feature Hashing for Large Scale Multitask Learning](https://alex.smola.org/papers/2009/Weinbergeretal09.pdf)

### 3.3 方案3：向量编码 + 相似度计算

#### 原理
将因子特征编码为向量，计算向量相似度（如余弦相似度），通过相似度阈值判断重复。

#### 优点
- 可以检测语义相似的因子
- 相似度可量化

#### 缺点
- 需要设计编码算法
- 计算复杂度高
- 需要确定相似度阈值

#### 适用场景
- 语义去重，检测逻辑相似的因子

### 3.4 方案4：特征指纹 + 相似度计算（推荐）

#### 原理
将因子特征编码为指纹，通过指纹相似度判断重复。

#### 优点
- 可以检测语义相似的因子
- 计算相对简单
- 易于理解和维护

#### 缺点
- 需要设计指纹算法
- 哈希碰撞可能导致误判

#### 适用场景
- 平衡精确度和计算复杂度

## 4. 推荐方案：Feature Hashing + 相似度计算

### 4.1 核心思路

基于在线搜索结果，推荐使用 **Feature Hashing + 相似度计算** 的组合方案：

1. **Feature Hashing**：将因子特征转换为哈希 ID，用于快速精确匹配
2. **相似度计算**：计算因子间的语义相似度，用于检测逻辑相似的因子
3. **组合判断**：先通过哈希 ID 快速筛选，再通过相似度计算精确判断

### 4.2 技术实现

#### 4.2.1 因子特征向量化

```python
import numpy as np
from typing import Dict

def factor_feature_vector(factor: Dict) -> np.ndarray:
    """将因子特征编码为向量"""
    features = []
    
    # 1. 输入字段（One-Hot 编码）
    input_features = factor.get("input_features", [])
    field_set = {"close", "volume", "open", "high", "low"}  # 预定义字段集合
    for field in field_set:
        features.append(1 if field in input_features else 0)
    
    # 2. 数学变换（One-Hot 编码）
    transformations = factor.get("transformations", [])
    transform_set = {"ratio", "shift", "std", "log"}  # 预定义变换集合
    for transform in transform_set:
        features.append(1 if transform in transformations else 0)
    
    # 3. 时间窗口（归一化）
    time_window = factor.get("time_window", 0)
    features.append(min(time_window / 100, 1.0))  # 归一化到 [0, 1]
    
    # 4. 标签（One-Hot 编码）
    tags = factor.get("tags", [])
    tag_set = {"momentum", "volatility", "trend", "volume"}  # 预定义标签集合
    for tag in tag_set:
        features.append(1 if tag in tags else 0)
    
    return np.array(features)
```

#### 4.2.2 因子哈希 ID 计算

```python
import hashlib

def factor_hash_id(factor: Dict) -> str:
    """计算因子哈希 ID（基于 Feature Hashing）"""
    # 特征提取
    features = []
    features.extend(factor.get("input_features", []))
    features.extend(factor.get("transformations", []))
    features.append(f"window_{factor.get('time_window', 0)}")
    features.extend(factor.get("tags", []))
    
    # 特征哈希
    feature_str = "|".join(sorted(features))
    hash_value = hashlib.md5(feature_str.encode()).hexdigest()
    
    return f"factor_{hash_value}"
```

#### 4.2.3 因子相似度计算

```python
from sklearn.metrics.pairwise import cosine_similarity

def factor_similarity(f1: Dict, f2: Dict) -> float:
    """计算因子相似度（基于余弦相似度）"""
    # 特征向量化
    v1 = factor_feature_vector(f1)
    v2 = factor_feature_vector(f2)
    
    # 余弦相似度
    similarity = cosine_similarity([v1], [v2])[0][0]
    
    return similarity
```

#### 4.2.4 重复判断

```python
DUPLICATE_THRESHOLD = 0.8  # 相似度 >= 0.8 视为重复

def is_duplicate(factor: Dict, registry: list[Dict]) -> tuple:
    """检查因子是否重复"""
    for existing in registry:
        # 1. 精确匹配（名称和表达式）
        if (factor["name"] == existing["name"] or 
            factor["expression"] == existing["expression"]):
            return True, existing, "exact"
        
        # 2. 相似度匹配
        similarity = factor_similarity(factor, existing)
        if similarity >= DUPLICATE_THRESHOLD:
            return True, existing, f"semantic_{similarity:.2f}"
    
    return False, None, None
```

### 4.3 相似度阈值确定

#### 实验方法

1. **收集测试数据**：
   - 已知重复的因子对（如 `momentum_20d` 和 `momentum_19d`）
   - 已知不重复的因子对（如 `momentum_20d` 和 `volatility_10d`）

2. **计算相似度分布**：
   - 重复因子的相似度分布
   - 不重复因子的相似度分布

3. **确定阈值**：
   - 选择一个阈值，使得重复因子的相似度 >= 阈值
   - 不重复因子的相似度 < 阈值

4. **验证**：
   - 使用测试数据验证阈值的有效性
   - 调整阈值以优化准确率

#### 经验值

| 相似度范围 | 判断 |
|-----------|------|
| >= 0.9 | 高度重复，视为相同因子 |
| 0.7 - 0.9 | 可能重复，需要人工确认 |
| < 0.7 | 不重复 |

**推荐阈值**：0.8（平衡精确度和召回率）

### 4.4 方案优势

| 优势 | 说明 |
|------|------|
| **成熟技术** | Feature Hashing 是广泛应用的成熟技术 |
| **高效计算** | O(1) 哈希计算 + O(n) 相似度计算 |
| **可扩展** | 易于添加新的特征维度 |
| **可解释** | 算法清晰，易于理解和维护 |
| **零侵入 RD-Agent** | 所有去重逻辑在 AIstock 后端实现 |

## 5. 实施落地方案

### 5.1 存储格式

#### 5.1.1 文件位置

```
app_tpl/qlib/v0/sota_factors_registry.json
```

#### 5.1.2 数据结构

```json
{
  "version": "v0",
  "updated_at": "2026-01-22T12:00:00",
  "factors": [
    {
      "name": "momentum_20d",
      "expression": "Ref($close, 20) / $close - 1",
      "logic_description": "计算过去20天的价格变化率，衡量价格动量趋势",
      "input_features": ["$close"],
      "time_window": 20,
      "transformations": ["ratio", "shift"],
      "tags": ["momentum", "price"],
      "description": "20日动量因子",
      "source_task_id": "task-001",
      "created_at": "2026-01-10T10:00:00"
    },
    {
      "name": "volatility_10d",
      "expression": "Std($close, 10)",
      "logic_description": "计算过去10天价格的标准差，衡量价格波动程度",
      "input_features": ["$close"],
      "time_window": 10,
      "transformations": ["std"],
      "tags": ["volatility", "price"],
      "description": "10日波动率因子",
      "source_task_id": "task-002",
      "created_at": "2026-01-12T14:30:00"
    }
  ],
  "rules": [
    "必须生成与列表中 name/expression/logic_description 不同的因子",
    "使用相同 input_features/time_window/transformations 的因子视为重复",
    "重复视为失败，需重新设计"
  ]
}
```

#### 5.1.3 字段说明

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `version` | string | 是 | 注册表版本号，与模板版本对应 |
| `updated_at` | string | 是 | 最后更新时间（ISO 8601） |
| `factors` | array | 是 | 因子列表 |
| `factors[].name` | string | 是 | 因子名称 |
| `factors[].expression` | string | 是 | 因子表达式 |
| `factors[].logic_description` | string | 是 | 因子逻辑描述 |
| `factors[].input_features` | array | 是 | 使用的输入特征 |
| `factors[].time_window` | number | 否 | 时间窗口 |
| `factors[].transformations` | array | 否 | 数学变换 |
| `factors[].tags` | array | 否 | 因子标签 |
| `factors[].description` | string | 否 | 因子简要描述 |
| `factors[].source_task_id` | string | 否 | 产出该因子的任务 ID |
| `factors[].created_at` | string | 否 | 因子创建时间 |
| `rules` | array | 否 | 去重规则 |

### 5.2 校验机制

#### 5.2.1 校验接口（AIstock 后端）

在 `backend/routers/rdagent_factors.py` 新增校验接口：

```python
from pydantic import BaseModel
from typing import List, Optional

class FactorDedupRequest(BaseModel):
    name: str
    expression: str
    logic_description: Optional[str] = None
    input_features: Optional[List[str]] = None
    time_window: Optional[int] = None
    transformations: Optional[List[str]] = None
    tags: Optional[List[str]] = None

class FactorDedupResponse(BaseModel):
    duplicate: bool
    type: Optional[str] = None  # "exact", "semantic"
    existing: Optional[dict] = None
    similarity: Optional[float] = None

@router.post("/dedup", response_model=FactorDedupResponse)
def check_factor_duplication(req: FactorDedupRequest) -> FactorDedupResponse:
    """检查因子是否重复"""
    registry_path = Path("app_tpl/qlib/v0/sota_factors_registry.json")
    registry = json.loads(registry_path.read_text())

    factor = {
        "name": req.name,
        "expression": req.expression,
        "logic_description": req.logic_description,
        "input_features": req.input_features,
        "time_window": req.time_window,
        "transformations": req.transformations,
        "tags": req.tags
    }

    is_dup, existing, dup_type = is_duplicate(factor, registry["factors"])
    if is_dup:
        similarity = factor_similarity(factor, existing)
        return FactorDedupResponse(
            duplicate=True,
            type=dup_type,
            existing=existing,
            similarity=similarity
        )

    return FactorDedupResponse(duplicate=False)
```

#### 5.2.2 自动入库接口（可选）

```python
@router.post("/register")
def register_factor(req: FactorRegisterRequest) -> Dict[str, Any]:
    """注册新因子到注册表"""
    registry_path = Path("app_tpl/qlib/v0/sota_factors_registry.json")
    registry = json.loads(registry_path.read_text())

    # 检查是否重复
    is_dup, _, _ = is_duplicate(req.dict(), registry["factors"])
    if is_dup:
        raise HTTPException(status_code=409, detail="因子已存在")

    # 追加新因子
    new_factor = {
        "name": req.name,
        "expression": req.expression,
        "logic_description": req.logic_description,
        "input_features": req.input_features,
        "time_window": req.time_window,
        "transformations": req.transformations,
        "tags": req.tags,
        "description": req.description,
        "source_task_id": req.source_task_id,
        "created_at": datetime.now().isoformat()
    }
    registry["factors"].append(new_factor)
    registry["updated_at"] = datetime.now().isoformat()

    # 写入文件
    registry_path.write_text(json.dumps(registry, indent=2, ensure_ascii=False))

    return {"ok": True, "factor": new_factor}
```

### 5.3 因子列表优化方案

#### 5.3.1 问题分析

| 影响因素 | 说明 |
|----------|------|
| **Token 消耗** | 因子列表越大，注入到 Prompt 的内容越多，可能超出 token 限制 |
| **LLM 判断** | 过多因子信息可能分散 LLM 注意力，降低去重效果 |
| **性能下降** | 每次生成都需要读取完整列表，影响响应速度 |

#### 5.3.2 优化方案

**组合方案**：分层存储 + 智能筛选 + 摘要模式

1. **分层存储**：标记高/中/低优先级因子
2. **智能筛选**：根据生成上下文注入相关因子（最多 50 个）
3. **摘要模式**：对于低优先级因子，只注入摘要信息
4. **定期清理**：移除低质量或过时因子

#### 5.3.3 智能筛选实现

```python
def get_factors_for_llm(generation_context: dict) -> list:
    """根据生成上下文获取相关因子"""
    registry = load_sota_registry()

    # 1. 获取生成上下文（如 LLM 倾向生成的因子类型）
    preferred_tags = generation_context.get("preferred_tags", [])

    # 2. 筛选相关因子
    related_factors = [
        f for f in registry["factors"]
        if any(tag in f.get("tags", []) for tag in preferred_tags)
    ]

    # 3. 限制数量（如最多 50 个）
    return related_factors[:50]
```

### 5.4 实施步骤

#### 阶段 1：创建注册表

1. 在 `app_tpl/qlib/v0/` 下创建 `sota_factors_registry.json`
2. 初始化为空或填入已有因子（如有）

#### 阶段 2：实现去重算法

1. 在 AIstock 后端实现 `factor_feature_vector`、`factor_hash_id`、`factor_similarity` 函数
2. 实现 `is_duplicate` 函数
3. 实现 `/dedup` 接口

#### 阶段 3：集成到任务流程

1. 在任务编排中调用 `/dedup` 接口
2. 根据校验结果决定是否重新生成或继续实现

#### 阶段 4：测试验证

1. 运行因子研发任务，观察去重效果
2. 检查生成结果是否与注册表重复
3. 验证新因子入库流程

## 6. 限制与注意事项

### 6.1 技术限制

| 限制 | 说明 | 应对措施 |
|------|------|----------|
| **RD-Agent 执行模式** | RD-Agent 的 `develop` 方法是同步阻塞执行，无法中途暂停 | 通过 Prompt 约束 + 后置校验实现 |
| **Prompt 遵守度** | 依赖 LLM 严格遵守提示词，可能偶尔忽略 | 后置校验作为保险 |
| **表达式标准化** | 需要确保表达式格式统一 | 标准化表达式格式 |

### 6.2 注意事项

- 注册表路径需在 `app_tpl/qlib/v0/` 内，确保 RD-Agent 可访问
- 提示词约束需明确且简洁，避免 LLM 忽略
- 相似度阈值需要通过实验确定，推荐初始值为 0.8
- 定期清理低质量或过时因子，控制因子列表大小

### 6.3 外部 API 调用是否中断任务？

**不会中断 RD-Agent 任务，不需要修改 RD-Agent 代码**

| 方面 | 说明 |
|------|------|
| **校验时机** | 在 RD-Agent 完成生成后，AIstock 后端接管校验 |
| **校验位置** | 校验逻辑在 AIstock 后端，不在 RD-Agent 内部 |
| **RD-Agent 角色** | 只负责生成因子假设和实现，不负责校验 |
| **中断情况** | 如果发现重复，AIstock 调用 RD-Agent 重新生成（新任务） |

**结论**：
- **不会中断**：RD-Agent 完成生成后，AIstock 后端接管校验，RD-Agent 任务已完成
- **无需修改 RD-Agent**：校验逻辑完全在 AIstock 后端，RD-Agent 只需支持"重新生成"（如果已有则无需修改）

## 7. 扩展方向

### 7.1 语义去重（未来）

- 使用向量数据库存储因子描述
- 计算语义相似度，检测逻辑相似的因子
- 提示词中增加"避免与已有因子逻辑相似"的约束

### 7.2 因子分类（未来）

- 在注册表中增加 `category` 字段（如 `momentum`、`volatility`）
- 提示词中约束"避免在同一类别中重复"

### 7.3 自动入库（未来）

- 在任务完成时自动提取因子信息
- 调用 `/rdagent/factors/register` 接口入库
- 无需手动维护注册表

## 8. 总结

### 8.1 方案总结

通过 **Feature Hashing + 相似度计算** 的组合方案，可以在不修改 RD-Agent 核心代码的前提下，实现已有因子的全局去重：

1. **存储格式**：JSON 文件存储因子注册表
2. **去重算法**：Feature Hashing 用于精确匹配，相似度计算用于语义去重
3. **校验机制**：AIstock 后端实现 `/dedup` 接口，RD-Agent 零侵入
4. **列表优化**：智能筛选 + 摘要模式，控制 token 消耗

### 8.2 核心优势

| 优势 | 说明 |
|------|------|
| **成熟技术** | Feature Hashing 是广泛应用的成熟技术 |
| **高效计算** | O(1) 哈希计算 + O(n) 相似度计算 |
| **可扩展** | 易于添加新的特征维度 |
| **可解释** | 算法清晰，易于理解和维护 |
| **零侵入 RD-Agent** | 所有去重逻辑在 AIstock 后端实现 |

### 8.3 实施建议

1. **优先实现**：创建注册表 + 实现去重算法 + 集成到任务流程
2. **逐步优化**：根据测试结果调整相似度阈值
3. **长期维护**：定期清理低质量因子，控制因子列表大小

---

**文档版本**: v1.0  
**创建时间**: 2026-01-22  
**适用模板**: app_tpl/qlib/v0  
**RD-Agent 兼容性**: 无需修改核心代码  
**技术参考**: Feature Hashing (Wikipedia), 基于相似度的因子研究 (BigQuant)

### 3.1 文件位置

```
app_tpl/qlib/v0/sota_factors_registry.json
```

### 3.2 数据结构

```json
{
  "version": "v0",
  "updated_at": "2026-01-22T12:00:00",
  "factors": [
    {
      "name": "momentum_20d",
      "expression": "Ref($close, 20) / $close - 1",
      "description": "20日动量因子",
      "source_task_id": "task-001",
      "created_at": "2026-01-10T10:00:00"
    },
    {
      "name": "volatility_10d",
      "expression": "Std($close, 10)",
      "description": "10日波动率因子",
      "source_task_id": "task-002",
      "created_at": "2026-01-12T14:30:00"
    }
  ],
  "rules": [
    "必须生成与列表中 name/expression 不同的因子",
    "重复视为失败，需重新设计"
  ]
}
```

### 3.3 字段说明

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `version` | string | 是 | 注册表版本号，与模板版本对应 |
| `updated_at` | string | 是 | 最后更新时间（ISO 8601） |
| `factors` | array | 是 | 因子列表 |
| `factors[].name` | string | 是 | 因子名称（用于名称比对） |
| `factors[].expression` | string | 是 | 因子表达式（用于表达式比对） |
| `factors[].logic_description` | string | 是 | 因子逻辑描述（用于语义比对） |
| `factors[].input_features` | array | 是 | 使用的输入特征（如 `["$close", "$volume"]`） |
| `factors[].time_window` | number | 否 | 时间窗口（如 20 日） |
| `factors[].transformations` | array | 否 | 数学变换（如 `["ratio", "shift", "log"]`） |
| `factors[].tags` | array | 否 | 因子标签（如 `["momentum", "price"]`） |
| `factors[].description` | string | 否 | 因子简要描述 |
| `factors[].source_task_id` | string | 否 | 产出该因子的任务 ID |
| `factors[].created_at` | string | 否 | 因子创建时间 |
| `rules` | array | 否 | 去重规则（注入到 Prompt） |

### 3.4 增强后的数据结构示例

```json
{
  "version": "v0",
  "updated_at": "2026-01-22T12:00:00",
  "factors": [
    {
      "name": "momentum_20d",
      "expression": "Ref($close, 20) / $close - 1",
      "logic_description": "计算过去20天的价格变化率，衡量价格动量趋势",
      "input_features": ["$close"],
      "time_window": 20,
      "transformations": ["ratio", "shift"],
      "tags": ["momentum", "price"],
      "description": "20日动量因子",
      "source_task_id": "task-001",
      "created_at": "2026-01-10T10:00:00"
    },
    {
      "name": "volatility_10d",
      "expression": "Std($close, 10)",
      "logic_description": "计算过去10天价格的标准差，衡量价格波动程度",
      "input_features": ["$close"],
      "time_window": 10,
      "transformations": ["std"],
      "tags": ["volatility", "price"],
      "description": "10日波动率因子",
      "source_task_id": "task-002",
      "created_at": "2026-01-12T14:30:00"
    }
  ],
  "rules": [
    "必须生成与列表中 name/expression/logic_description 不同的因子",
    "使用相同 input_features/time_window/transformations 的因子视为重复",
    "重复视为失败，需重新设计"
  ]
}
```

### 3.5 为什么需要增强字段？

| 原始字段缺陷 | 示例 | 后果 |
|--------------|------|------|
| 缺少**逻辑语义描述** | 只知道 `momentum_20d` 是"20日动量" | LLM 可能生成语义相同但写法不同的因子 |
| 缺少**输入特征** | 不知道使用了 `$close`、`$volume` 等哪些字段 | LLM 可能生成使用相同特征但名称不同的因子 |
| 缺少**时间窗口** | 不知道是 20 日还是 19 日 | LLM 可能生成 `momentum_19d` 视为不同因子 |
| 缺少**数学变换** | 不知道是否经过对数、标准化 | LLM 可能生成 `log(momentum_20d)` 视为不同因子 |

## 4. 注入方式

### 4.1 Prompt 模板引用

在 `prompts_core_constraints.yaml` 或相关提示词模板中，使用 Jinja 语法引用注册表：

```yaml
sota_factor_dedup:
  path: "{{ sota_factors_registry_path }}"
  rules:
    - "生成前必须读取 {{ sota_factors_registry_path }}，比对已有因子"
    - "若生成因子与库中 name/expression 重复，视为无效输出"
    - "重复时需重新设计因子逻辑"
```

RD-Agent 的 `T()` 机制会自动解析文件并注入内容。

### 4.2 假设阶段注入（推荐）

在 **因子假设生成阶段** 的 Prompt 中加入：

```yaml
factor_hypothesis_generation:
  constraints:
    - "必须先读取 {{ sota_factors_registry_path }}"
    - "生成前检查因子名称和表达式是否重复"
    - "重复则跳过或重新设计"
```

### 4.3 研发阶段注入（可选）

在 **因子实现阶段** 的 Prompt 中加入：

```yaml
factor_development:
  constraints:
    - "实现前再次读取 {{ sota_factors_registry_path }}"
    - "确认最终因子不重复"
```

## 5. 校验机制

### 5.1 后置校验方案（不修改 rdagent 代码）

#### 5.1.1 核心思路

**校验逻辑在 AIstock 后端，rdagent 只负责生成**

- rdagent 无需感知校验存在，只执行生成任务
- AIstock 后端在任务编排层插入校验逻辑
- 校验通过 HTTP 调用，rdagent 代码零侵入

#### 5.1.2 校验服务部署（AIstock 后端）

在 `backend/routers/rdagent_factors.py` 新增校验接口：

```python
from pydantic import BaseModel
from typing import List, Optional

class FactorDedupRequest(BaseModel):
    name: str
    expression: str
    logic_description: Optional[str] = None
    input_features: Optional[List[str]] = None
    time_window: Optional[int] = None
    transformations: Optional[List[str]] = None

class FactorDedupResponse(BaseModel):
    duplicate: bool
    type: Optional[str] = None  # "name", "expression", "semantic"
    existing: Optional[dict] = None

@router.post("/dedup", response_model=FactorDedupResponse)
def check_factor_duplication(req: FactorDedupRequest) -> FactorDedupResponse:
    """检查因子是否重复"""
    registry_path = Path("app_tpl/qlib/v0/sota_factors_registry.json")
    registry = json.loads(registry_path.read_text())

    for factor in registry["factors"]:
        # 1. 名称比对
        if factor["name"] == req.name:
            return FactorDedupResponse(
                duplicate=True,
                type="name",
                existing=factor
            )

        # 2. 表达式比对
        if factor["expression"] == req.expression:
            return FactorDedupResponse(
                duplicate=True,
                type="expression",
                existing=factor
            )

        # 3. 输入特征 + 时间窗口 + 变换比对（增强去重）
        if (req.input_features and req.time_window and req.transformations):
            if (set(factor.get("input_features", [])) == set(req.input_features) and
                factor.get("time_window") == req.time_window and
                set(factor.get("transformations", [])) == set(req.transformations)):
                return FactorDedupResponse(
                    duplicate=True,
                    type="semantic",
                    existing=factor
                )

    return FactorDedupResponse(duplicate=False)
```

#### 5.1.3 校验时机（任务编排层）

在 AIstock 后端调用 RD-Agent 的任务编排中插入校验：

```python
# backend/services/rdagent_worker.py
async def run_rdagent_task(task_id: str):
    # 1. 调用 RD-Agent 生成因子假设
    hypothesis = await rdagent.generate_hypothesis(task_id)

    # 2. 后置校验（假设阶段）
    duplicate_found = False
    for factor in hypothesis["factors"]:
        result = await check_factor_duplication(
            name=factor["name"],
            expression=factor["expression"],
            logic_description=factor.get("logic_description"),
            input_features=factor.get("input_features"),
            time_window=factor.get("time_window"),
            transformations=factor.get("transformations")
        )
        if result["duplicate"]:
            duplicate_found = True
            logger.warning(f"因子重复: {factor['name']}，类型: {result['type']}")
            # 标记重复，触发重新生成
            hypothesis = await rdagent.regenerate_hypothesis(task_id)
            break

    if duplicate_found:
        # 重新生成后再次校验
        return await run_rdagent_task(task_id)

    # 3. 继续因子实现
    implementation = await rdagent.implement_factors(task_id, hypothesis)

    # 4. 后置校验（实现阶段）
    for factor in implementation["factors"]:
        result = await check_factor_duplication(
            name=factor["name"],
            expression=factor["expression"],
            logic_description=factor.get("logic_description"),
            input_features=factor.get("input_features"),
            time_window=factor.get("time_window"),
            transformations=factor.get("transformations")
        )
        if result["duplicate"]:
            # 标记失败，记录到数据库
            await mark_task_failed(task_id, reason="duplicate_factor")
            logger.error(f"因子实现后重复: {factor['name']}")
            return {"status": "failed", "reason": "duplicate_factor"}

    return {"status": "success", "factors": implementation["factors"]}
```

#### 5.1.4 不修改 rdagent 代码的关键

| 原则 | 实现 |
|------|------|
| **校验逻辑在 AIstock 后端** | rdagent 只负责生成，不负责校验 |
| **校验通过 HTTP 调用** | rdagent 无需感知校验存在 |
| **任务编排在 AIstock 层** | rdagent 只执行子任务，不控制流程 |
| **零侵入 rdagent 代码** | 所有校验逻辑在外部服务 |

### 5.2 校验流程图

```
┌─────────────────┐
│ AIstock 后端    │
│ 调用 RD-Agent  │
└────────┬────────┘
         │
         v
┌─────────────────┐
│ RD-Agent        │
│ 生成因子假设    │
└────────┬────────┘
         │
         v
┌─────────────────┐
│ AIstock 后端    │
│ 调用 /dedup    │
└────────┬────────┘
         │
         v
┌─────────────────┐
│ 校验服务        │
│ 比对注册表      │
└────────┬────────┘
         │
         v
    ┌────┴────┐
    │ 重复?   │
    └────┬────┘
         │
    ┌────┴────┐
    │ 是 → 重新生成 │
    │ 否 → 继续实现 │
    └────┬────┘
         │
         v
┌─────────────────┐
│ RD-Agent        │
│ 实现因子        │
└────────┬────────┘
         │
         v
┌─────────────────┐
│ AIstock 后端    │
│ 再次调用 /dedup │
└────────┬────────┘
         │
         v
    ┌────┴────┐
    │ 重复?   │
    └────┬────┘
         │
    ┌────┴────┐
    │ 是 → 标记失败 │
    │ 否 → 任务成功 │
    └──────────┘
```

## 6. 维护流程

### 6.1 新因子入库

当新因子研发成功后：

1. 从任务结果中提取 `name`、`expression`、`logic_description`、`input_features`、`time_window`、`transformations`
2. 追加到 `sota_factors_registry.json`
3. 更新 `updated_at` 时间戳
4. 可选：在 AIstock 后端提供 `/rdagent/factors/register` 接口自动入库

#### 6.1.1 自动入库接口（可选）

```python
@router.post("/register")
def register_factor(req: FactorRegisterRequest) -> Dict[str, Any]:
    """注册新因子到注册表"""
    registry_path = Path("app_tpl/qlib/v0/sota_factors_registry.json")
    registry = json.loads(registry_path.read_text())

    # 检查是否重复
    for factor in registry["factors"]:
        if factor["name"] == req.name or factor["expression"] == req.expression:
            raise HTTPException(status_code=409, detail="因子已存在")

    # 追加新因子
    new_factor = {
        "name": req.name,
        "expression": req.expression,
        "logic_description": req.logic_description,
        "input_features": req.input_features,
        "time_window": req.time_window,
        "transformations": req.transformations,
        "tags": req.tags,
        "description": req.description,
        "source_task_id": req.source_task_id,
        "created_at": datetime.now().isoformat()
    }
    registry["factors"].append(new_factor)
    registry["updated_at"] = datetime.now().isoformat()

    # 写入文件
    registry_path.write_text(json.dumps(registry, indent=2, ensure_ascii=False))

    return {"ok": True, "factor": new_factor}
```

### 6.2 版本管理

- 每次模板发布（v0 → v1）时，复制注册表到新版本
- 新版本可独立维护自己的因子库
- v0 作为基线，不可删除

## 7. 实施步骤

### 阶段 1：创建注册表

1. 在 `app_tpl/qlib/v0/` 下创建 `sota_factors_registry.json`
2. 初始化为空或填入已有因子（如有）

### 阶段 2：修改 Prompt 模板

1. 在 `prompts_core_constraints.yaml` 中加入去重约束
2. 在 `prompts.yaml` 中引用注册表路径
3. 确保假设阶段的 Prompt 包含去重规则

### 阶段 3：实现校验接口（可选）

1. 在 AIstock 后端新增 `/rdagent/factors/dedup` 接口
2. 实现比对逻辑
3. 集成到任务流程中

### 阶段 4：测试验证

1. 运行因子研发任务，观察 LLM 是否遵守去重规则
2. 检查生成结果是否与注册表重复
3. 验证新因子入库流程

## 8. 限制与注意事项

### 8.1 限制

- **语义相似性**：仅比对 `name` 和 `expression`，无法检测语义相似的因子（如 `momentum_20d` vs `momentum_19d`）
- **Prompt 遵守度**：依赖 LLM 严格遵守提示词，可能偶尔忽略
- **表达式标准化**：需要确保表达式格式统一（如 `Ref($close, 20)` vs `Ref($close, 20.0)`）

### 8.2 注意事项

- 注册表路径需在 `app_tpl/qlib/v0/` 内，确保 RD-Agent 可访问
- 提示词约束需明确且简洁，避免 LLM 忽略
- 后置校验可作为保险，但不应依赖它完全避免重复

### 8.3 外部 API 调用是否中断任务？

**不会中断 rdagent 任务，不需要修改 rdagent 代码**

#### 执行流程

```
┌─────────────────┐
│ AIstock 后端    │
│ 调用 rdagent    │
└────────┬────────┘
         │
         v
┌─────────────────┐
│ rdagent        │
│ 生成因子假设    │  ← rdagent 正常执行
└────────┬────────┘
         │
         v
┌─────────────────┐
│ AIstock 后端    │
│ 调用 /dedup    │  ← 校验在这里，不影响 rdagent
└────────┬────────┘
         │
         v
    ┌────┴────┐
    │ 重复?   │
    └────┬────┘
         │
    ┌────┴────┐
    │ 是 → 调用 rdagent 重新生成（新任务） │  ← 新的 rdagent 任务
    │ 否 → 继续实现 │
    └──────────┘
```

#### 关键点

| 方面 | 说明 |
|------|------|
| **校验时机** | 在 rdagent 完成生成后，AIstock 后端接管校验 |
| **校验位置** | 校验逻辑在 AIstock 后端，不在 rdagent 内部 |
| **rdagent 角色** | 只负责生成因子假设和实现，不负责校验 |
| **中断情况** | 如果发现重复，AIstock 调用 rdagent 重新生成（新任务） |

**结论**：
- **不会中断**：rdagent 完成生成后，AIstock 后端接管校验，rdagent 任务已完成
- **无需修改 rdagent**：校验逻辑完全在 AIstock 后端，rdagent 只需支持"重新生成"（如果已有则无需修改）

### 8.4 因子列表增长对 LLM 的影响

**会受到影响，需要优化方案**

#### 影响分析

| 影响因素 | 说明 |
|----------|------|
| **Token 消耗** | 因子列表越大，注入到 Prompt 的内容越多，可能超出 token 限制 |
| **LLM 判断** | 过多因子信息可能分散 LLM 注意力，降低去重效果 |
| **性能下降** | 每次生成都需要读取完整列表，影响响应速度 |

#### 估算示例

假设每个因子平均 200 字符，1000 个因子约 200KB，约 50K tokens（中文），可能超出模型上下文限制。

#### 优化方案

##### 方案1：分层存储 + 智能筛选

```json
{
  "version": "v0",
  "updated_at": "2026-01-22T12:00:00",
  "factors": [
    {
      "name": "momentum_20d",
      "expression": "Ref($close, 20) / $close - 1",
      "logic_description": "计算过去20天的价格变化率",
      "input_features": ["$close"],
      "time_window": 20,
      "transformations": ["ratio", "shift"],
      "tags": ["momentum", "price"],
      "priority": "high",  // 新增：优先级
      "created_at": "2026-01-10T10:00:00"
    }
  ],
  "summary": {
    "total_count": 1000,
    "high_priority_count": 100,
    "categories": {
      "momentum": 200,
      "volatility": 150,
      "trend": 300,
      "volume": 350
    }
  }
}
```

**注入策略**：
- 只注入 **高优先级因子**（如最近 100 个）
- 根据 LLM 生成方向注入相关类别因子
- 如果 LLM 生成动量因子，只注入 `tags` 包含 `momentum` 的因子

##### 方案2：分页加载 + 按需注入

```python
def get_factors_for_llm(generation_context: dict) -> list:
    """根据生成上下文获取相关因子"""
    registry = load_sota_registry()

    # 1. 获取生成上下文（如 LLM 倾向生成的因子类型）
    preferred_tags = generation_context.get("preferred_tags", [])

    # 2. 筛选相关因子
    related_factors = [
        f for f in registry["factors"]
        if any(tag in f.get("tags", []) for tag in preferred_tags)
    ]

    # 3. 限制数量（如最多 50 个）
    return related_factors[:50]
```

##### 方案3：向量检索 + 语义去重（未来扩展）

使用向量数据库存储因子描述，计算语义相似度，只注入与当前生成方向相关的因子。

##### 方案4：摘要模式

```json
{
  "factors_summary": [
    {
      "category": "momentum",
      "count": 200,
      "examples": ["momentum_20d", "momentum_10d"],
      "warning": "已存在 200 个动量因子，避免重复"
    }
  ]
}
```

**注入摘要而非完整列表**，减少 token 消耗。

#### 推荐方案

**组合方案**：分层存储 + 智能筛选 + 摘要模式

1. **分层存储**：标记高/中/低优先级因子
2. **智能筛选**：根据生成上下文注入相关因子（最多 50 个）
3. **摘要模式**：对于低优先级因子，只注入摘要信息
4. **定期清理**：移除低质量或过时因子

## 9. 扩展方向

### 9.1 语义去重（未来）

- 使用向量数据库存储因子表达式
- 计算语义相似度，检测逻辑相似的因子
- 提示词中增加"避免与已有因子逻辑相似"的约束

### 9.2 因子分类（未来）

- 在注册表中增加 `category` 字段（如 `momentum`、`volatility`）
- 提示词中约束"避免在同一类别中重复"

### 9.3 自动入库（未来）

- 在任务完成时自动提取因子信息
- 调用 `/rdagent/factors/register` 接口入库
- 无需手动维护注册表

## 10. 总结

通过 **JSON 注册表 + Prompt 约束 + 后置校验** 的组合方案，可以在不修改 RD-Agent 核心代码的前提下，实现已有因子的全局去重。假设阶段让 LLM 读取注册表是关键，可避免重复设计；后置校验作为保险，确保最终因子不重复。

---

**文档版本**: v0.1  
**创建时间**: 2026-01-22  
**适用模板**: app_tpl/qlib/v0  
**RD-Agent 兼容性**: 无需修改核心代码
