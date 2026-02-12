"""
RD-Agent因子命名区分方案设计
==============================

## 1. 问题背景

### 当前问题
1. **同名因子冲突**: 多个workspace可能使用相同的因子名称（如"MA", "RSI"）
2. **Alpha158重复**: 每个model_loop都包含完整的158个Alpha158因子
3. **表达式不同但名称相同**: 例如MA(5)和MA(10)都叫"MA"
4. **跨实验重复**: 不同task_run_id中可能产生相同的因子

### 影响范围
- factor_registry表: 因子注册信息
- aistock_factor_catalog表: AIstock因子目录
- factor_meta.json: Workspace中的因子元数据
- 资产包: 因子文件的命名

## 2. 需求分析

### 功能需求
1. **唯一性**: 每个因子必须有唯一的标识
2. **可读性**: 因子名称应该易于理解
3. **可追溯**: 能够追溯到原始实验和workspace
4. **去重**: 相同表达式的因子应该被视为同一个
5. **兼容性**: 与现有系统兼容，不破坏现有数据

### 非功能需求
1. **性能**: 查询和匹配应该高效
2. **可扩展**: 支持未来新增的因子类型
3. **可维护**: 逻辑清晰，易于理解和维护

## 3. 方案设计

### 方案1: 命名空间前缀方案

#### 设计思路
在因子名称前添加命名空间前缀，包含实验信息

#### 命名格式
```
{task_run_id_short}_{loop_id}_{workspace_id_short}_{factor_name}
```

#### 示例
```
20251229_1_b3caf6_MA
20251229_1_b3caf6_RSI
20251229_2_e8b969_MA
```

#### 优点
- 简单直观，易于理解
- 包含完整的追溯信息
- 保证唯一性

#### 缺点
- 名称过长，影响可读性
- task_run_id可能很长
- 不支持表达式去重

#### 适用场景
- 需要完整追溯信息的场景
- 因子数量较少的场景

---

### 方案2: 因子指纹去重方案（推荐）

#### 设计思路
使用因子表达式计算MD5指纹，相同表达式的因子自动合并

#### 核心概念
1. **因子指纹**: 因子表达式的MD5哈希值
2. **因子名称**: 保持原有名称不变
3. **因子ID**: 使用指纹作为唯一标识
4. **最佳性能**: 记录每个因子的最佳性能

#### 数据结构
```python
{
    "factor_id": "a1b2c3d4e5f6...",  # MD5指纹
    "factor_name": "MA",            # 原始名称
    "expression": "MA(close, 5)",   # 表达式
    "source": "rdagent_generated",  # 来源
    "best_performance": {
        "ann_ret": 0.15,
        "max_dd": -0.08,
        "ic": 0.05
    },
    "occurrences": [
        {
            "task_run_id": "2025-12-29_05-17-56-204326",
            "loop_id": 1,
            "workspace_id": "b3caf6168516403580ea6ad430c1e31c",
            "performance": {"ann_ret": 0.12, "max_dd": -0.10, "ic": 0.04}
        },
        {
            "task_run_id": "2025-12-29_05-17-56-204326",
            "loop_id": 2,
            "workspace_id": "e8b9693e0a3b5047b2ac0cf5439730fc",
            "performance": {"ann_ret": 0.15, "max_dd": -0.08, "ic": 0.05}
        }
    ]
}
```

#### 指纹计算逻辑
```python
import hashlib

def calculate_factor_fingerprint(expression: str) -> str:
    """
    计算因子表达式的指纹
    
    Args:
        expression: 因子表达式，如 "MA(close, 5)"
    
    Returns:
        MD5哈希值，如 "a1b2c3d4e5f6..."
    """
    # 标准化表达式（去除空格，统一大小写）
    normalized = expression.strip().lower().replace(" ", "")
    
    # 计算MD5
    md5_hash = hashlib.md5(normalized.encode('utf-8')).hexdigest()
    
    return md5_hash
```

#### 数据库表结构
```sql
CREATE TABLE factor_registry (
    factor_id TEXT PRIMARY KEY,           -- MD5指纹
    factor_name TEXT NOT NULL,            -- 原始名称
    expression TEXT NOT NULL,             -- 表达式
    source TEXT,                          -- 来源
    best_performance_json TEXT,           -- 最佳性能
    occurrence_count INTEGER DEFAULT 1,   -- 出现次数
    created_at_utc TEXT,
    updated_at_utc TEXT
);

CREATE TABLE factor_occurrences (
    occurrence_id TEXT PRIMARY KEY,
    factor_id TEXT,                       -- 关联到factor_registry
    task_run_id TEXT,
    loop_id INTEGER,
    workspace_id TEXT,
    performance_json TEXT,
    created_at_utc TEXT,
    FOREIGN KEY (factor_id) REFERENCES factor_registry(factor_id)
);
```

#### 去重逻辑
```python
def upsert_factor_with_dedup(
    factor_name: str,
    expression: str,
    performance: dict,
    task_run_id: str,
    loop_id: int,
    workspace_id: str
):
    """
    插入或更新因子，自动去重
    
    1. 计算因子指纹
    2. 查询是否已存在
    3. 如果存在，更新最佳性能和出现次数
    4. 如果不存在，创建新记录
    5. 记录本次出现
    """
    factor_id = calculate_factor_fingerprint(expression)
    
    # 查询现有记录
    existing = query_factor_by_id(factor_id)
    
    if existing:
        # 更新最佳性能
        if is_better_performance(performance, existing.best_performance):
            update_best_performance(factor_id, performance)
        
        # 增加出现次数
        increment_occurrence_count(factor_id)
    else:
        # 创建新记录
        create_factor(
            factor_id=factor_id,
            factor_name=factor_name,
            expression=expression,
            best_performance=performance
        )
    
    # 记录本次出现
    create_occurrence(
        factor_id=factor_id,
        task_run_id=task_run_id,
        loop_id=loop_id,
        workspace_id=workspace_id,
        performance=performance
    )
```

#### 优点
- 自动去重，相同表达式视为同一个因子
- 保留原始因子名称，可读性好
- 记录最佳性能，便于选择
- 支持追溯所有出现位置
- 查询性能好（通过factor_id索引）

#### 缺点
- 需要额外的occurrences表
- 表达式必须准确，否则无法去重
- 相同名称但不同表达式的因子会被视为不同因子

#### 适用场景
- 因子数量较多的场景
- 需要自动去重的场景
- 需要最佳性能的场景

---

### 方案3: 层级命名方案

#### 设计思路
使用层级结构，将因子名称和workspace信息组合

#### 命名格式
```
{factor_name}@{workspace_id_short}
```

#### 示例
```
MA@b3caf6
MA@e8b969
RSI@b3caf6
```

#### 优点
- 简洁明了
- 包含workspace信息
- 长度适中

#### 缺点
- 不支持表达式去重
- workspace_id_short可能不唯一
- 无法追溯到task_run_id和loop_id

#### 适用场景
- 需要简洁命名的场景
- workspace数量较少的场景

---

### 方案4: 混合方案（最终推荐）

#### 设计思路
结合方案2（指纹去重）和方案3（层级命名）

#### 核心设计
1. **唯一标识**: 使用MD5指纹作为factor_id
2. **显示名称**: 使用`{factor_name}@{workspace_id_short}`格式
3. **去重逻辑**: 相同表达式自动合并
4. **最佳性能**: 记录并更新最佳性能
5. **完整追溯**: 通过occurrences表记录所有出现

#### 数据结构
```python
{
    "factor_id": "a1b2c3d4e5f6...",  # MD5指纹（唯一标识）
    "display_name": "MA@b3caf6",     # 显示名称
    "factor_name": "MA",            # 原始名称
    "expression": "MA(close, 5)",   # 表达式
    "source": "rdagent_generated",  # 来源
    "best_performance": {
        "ann_ret": 0.15,
        "max_dd": -0.08,
        "ic": 0.05,
        "task_run_id": "2025-12-29_05-17-56-204326",
        "loop_id": 2,
        "workspace_id": "e8b9693e0a3b5047b2ac0cf5439730fc"
    },
    "occurrence_count": 2
}
```

#### 显示名称生成逻辑
```python
def generate_display_name(factor_name: str, workspace_id: str) -> str:
    """
    生成显示名称
    
    格式: {factor_name}@{workspace_id_short}
    
    Args:
        factor_name: 因子名称，如 "MA"
        workspace_id: workspace_id，如 "b3caf6168516403580ea6ad430c1e31c"
    
    Returns:
        显示名称，如 "MA@b3caf6"
    """
    # 取workspace_id的前6位
    workspace_short = workspace_id[:6] if len(workspace_id) >= 6 else workspace_id
    
    return f"{factor_name}@{workspace_short}"
```

#### 数据库表结构
```sql
CREATE TABLE factor_registry (
    factor_id TEXT PRIMARY KEY,           -- MD5指纹
    display_name TEXT NOT NULL,           -- 显示名称
    factor_name TEXT NOT NULL,            -- 原始名称
    expression TEXT NOT NULL,             -- 表达式
    source TEXT,                          -- 来源
    best_performance_json TEXT,           -- 最佳性能（包含来源信息）
    occurrence_count INTEGER DEFAULT 1,   -- 出现次数
    created_at_utc TEXT,
    updated_at_utc TEXT
);

CREATE INDEX idx_factor_display_name ON factor_registry(display_name);
CREATE INDEX idx_factor_name ON factor_registry(factor_name);
```

#### 查询示例
```sql
-- 按显示名称查询
SELECT * FROM factor_registry WHERE display_name = 'MA@b3caf6';

-- 按原始名称查询（返回所有变体）
SELECT * FROM factor_registry WHERE factor_name = 'MA';

-- 查询最佳性能的因子
SELECT * FROM factor_registry 
ORDER BY json_extract(best_performance_json, '$.ann_ret') DESC;
```

#### 优点
- 结合了所有方案的优点
- 自动去重，避免重复
- 显示名称简洁明了
- 支持完整追溯
- 查询性能好

#### 缺点
- 实现复杂度较高
- 需要额外的表和索引

#### 适用场景
- 生产环境推荐使用
- 因子数量较多的场景
- 需要自动去重和最佳性能的场景

## 4. 实施建议

### 阶段1: 数据迁移
1. 备份现有数据
2. 计算所有现有因子的指纹
3. 合并重复因子
4. 生成显示名称
5. 更新数据库

### 阶段2: 代码修改
1. 修改`upsert_factor_registry`函数
2. 添加指纹计算逻辑
3. 添加显示名称生成逻辑
4. 修改查询逻辑
5. 更新API接口

### 阶段3: 测试验证
1. 单元测试
2. 集成测试
3. 性能测试
4. 数据一致性验证

### 阶段4: 上线部署
1. 灰度发布
2. 监控日志
3. 性能监控
4. 问题修复

## 5. 总结

### 推荐方案
**方案4: 混合方案**

### 核心特性
1. 使用MD5指纹作为唯一标识
2. 使用`{factor_name}@{workspace_id_short}`作为显示名称
3. 自动去重相同表达式的因子
4. 记录并更新最佳性能
5. 支持完整追溯

### 预期效果
- 解决同名因子冲突问题
- 自动去重，减少冗余
- 保留最佳性能，便于选择
- 显示名称简洁明了
- 查询性能优异
"""

print(__doc__)
