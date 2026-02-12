# Task同步逻辑严格分析报告

## 用户要求（4个严格要求）

1. **必须获取所有SOTA因子和alpha基线因子用于在AIstock侧执行选股**
2. **必须获取最后一个SOTA因子加入时，训练和回测使用的模型权重数据**
3. **不能使用模型LOOP中的权重数据，目前task选股不考虑模型LOOP的SOTA模型**
4. **必须确保SOTA因子和alpha基线因子的总数与模型权重的输入数量完全一致，且必须确保每个因子对应模型权重文件中的顺序准确，不能猜测，不能推断，必须程序代码确保准确，如果数量不一致或顺序不准确，必须报错，不能使用任何兜底方案，包括填充0等操作！**

---

## 当前代码问题分析

### ❌ 问题1: 使用了推断和猜测

**位置:** `rdagent_task_sync_service.py:434-481`

```python
# 方法1: 从FilterCol.col_list提取
filter_col = meta_json.get("FilterCol", {})
col_list = filter_col.get("col_list", [])
if col_list:
    alpha_baseline_factors = col_list
    alpha_source = "model_meta.json/FilterCol.col_list"

# 方法2: 从dataset_conf.kwargs.handler.kwargs.data_loader.kwargs.alpha158_config提取
if not alpha_baseline_factors:
    # ... 尝试其他位置

# 方法3: 从feature字段提取（可能包含所有特征）
if not alpha_baseline_factors:
    feature_list = meta_json.get("feature", [])
    if feature_list:
        # 过滤出Alpha158因子（通常是大写字母开头的短名称）
        alpha_baseline_factors = [
            f for f in feature_list 
            if f.isupper() or f in DEFAULT_ALPHA158_BASELINE  # ❌ 这是猜测！
        ]

# 如果未能从model_meta.json提取，使用默认的Alpha158基线因子
if not alpha_baseline_factors:
    alpha_baseline_factors = DEFAULT_ALPHA158_BASELINE  # ❌ 这是兜底方案！
    alpha_source = "default (RD-Agent conf_combined_factors_sota_model.yaml)"
```

**违反要求:** 
- 使用了多个"如果失败则尝试其他方法"的兜底逻辑
- 使用了默认值作为兜底方案
- 使用了 `f.isupper()` 这种启发式规则来"猜测"哪些是Alpha158因子

---

### ❌ 问题2: factor_order.json中只记录文件路径，不记录因子名称

**位置:** `rdagent_task_sync_service.py:620-629`

```python
factor_order_data = {
    "version": "v1",
    "task_id": tid,
    "generated_at_utc": _utc_now_iso(),
    "alpha158_factors": alpha_baseline_factors,
    "dynamic_factors": [f["path"] for f in all_factors],  # ❌ 只记录文件路径！
    "total_factors": len(alpha_baseline_factors) + len(all_factors),  # ❌ 错误计算！
    "alpha158_count": len(alpha_baseline_factors),
    "dynamic_count": len(all_factors),  # ❌ 这是文件数量，不是因子数量！
}
```

**违反要求:**
- `dynamic_factors` 只记录了文件路径（如 `"factor.py"`），没有记录实际的因子名称
- `total_factors` 计算错误：一个文件可能包含多个因子
- 无法确保因子顺序与模型训练时一致

---

### ❌ 问题3: 没有验证因子数量与模型权重的匹配

**位置:** `rdagent_task_sync_service.py:322-690`

同步逻辑中**完全没有**加载模型权重并验证特征数量的代码！

---

### ❌ 问题4: inference_engine.py中存在推断逻辑

**位置:** `inference_engine.py:497-544`

```python
def _infer_expected_features(self, task_dir: Path, manifest: Dict[str, Any]) -> List[str]:
    """严格推断预期特征列表：从 factor_order.json 获取 Alpha158 基线因子 + 动态因子"""
    # ... 尝试多个位置
    
    # 回退：从 model_meta.json 获取  # ❌ 兜底方案！
    if not alpha158_factors:
        # ... 尝试其他位置
    
    return []  # ❌ 失败时返回空列表，而不是报错！
```

**违反要求:**
- 函数名就叫 `_infer_expected_features`，明确表示这是"推断"
- 有多个兜底方案
- 失败时返回空列表而不是报错

---

### ❌ 问题5: 因子计算时存在类型转换兜底方案

**位置:** `rdagent_task_sync_service.py:532-557`

```python
# 添加类型检查和转换代码
fix_code = """
# 确保series是Series而不是DataFrame
if isinstance(series, pd.DataFrame):
    if series.shape[1] == 1:
        series = series.iloc[:, 0]  # ❌ 自动转换，这是兜底方案！
    else:
        # 如果是多列DataFrame，取第一列
        series = series.iloc[:, 0]  # ❌ 更糟糕的兜底方案！
"""
```

**违反要求:**
- 自动将DataFrame转换为Series
- 多列DataFrame时自动取第一列，这是猜测！

---

### ❌ 问题6: inference_engine.py中的特征组合逻辑有问题

**位置:** `inference_engine.py:752-803`

```python
# 7.2 添加 SOTA 动态因子（按 df_factors 中的列顺序）
for col_name in df_factors.columns:
    col_data = df_factors[col_name]
    # ...
    final_cols_data[col_name] = col_data
```

**问题:**
- 没有验证SOTA因子的顺序是否与训练时一致
- 依赖 `df_factors.columns` 的顺序，但这个顺序可能不正确

---

## 正确的实现方案

### 方案1: 从RD-Agent获取完整的特征列表和顺序

RD-Agent的 `sota_factor_anchor` API应该返回：
1. 模型权重文件
2. 模型期望的特征数量
3. **完整的特征列表（包括名称和顺序）**

如果API不提供，需要：
1. 加载模型权重文件
2. 从模型中提取 `feature_name_` 或 `feature_names_` 属性
3. 严格按照这个顺序组织因子

### 方案2: 从model_meta.json严格提取

model_meta.json中应该包含完整的特征配置，包括：
1. Alpha158基线因子列表（从 `infer_processors` 中的 `FilterCol` 提取）
2. SOTA动态因子列表（需要从训练配置中提取）

如果提取失败，**必须报错**，不能使用默认值。

### 方案3: 验证逻辑

同步完成后，必须：
1. 加载模型权重文件
2. 获取模型期望的特征数量和名称
3. 对比 factor_order.json 中的配置
4. 如果不匹配，**删除所有同步的文件并报错**

---

## 需要修复的代码文件

1. `backend/services/rdagent_task_sync_service.py` - 同步逻辑
2. `backend/inference_engine.py` - 推理逻辑
3. 需要创建新的验证模块来严格验证特征匹配

---

## 修复原则

1. **禁止使用任何默认值**
2. **禁止使用任何兜底方案**
3. **禁止使用任何推断或猜测**
4. **所有失败情况必须立即报错**
5. **必须从权威来源（模型权重文件）获取特征信息**
6. **必须严格验证特征数量和顺序**
