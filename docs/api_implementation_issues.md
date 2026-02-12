# AIstock API实现问题分析报告

## 执行日期
2026-01-31

## 问题描述

根据用户反馈和RD-Agent文档分析，发现AIstock后端API在获取SOTA因子数据时存在严重问题，**未能正确实现文档中描述的数据获取方案**。

## 用户反馈的关键信息

用户明确指出task `2025-12-26_06-19-42-126375` 在UI中可以看到**多个Loop最终结论显示Decision: True**，例如：

```
Hypothesis Feedback🔍
Observations: 本次实验实现了两个因子：主力资金净流入强度5日滚动（mf_main_net_amt_ratio_5d）和倒数市盈率20日波动率（value_pe_inv_vol_20d）。
从回测结果看，当前组合的年化收益率（0.820059）相比SOTA（0.679081）有明显提升，IC值也从0.033873提高到0.039253...
Decision: True
```

**这证明：**
1. 一个task中有多个SOTA因子（Decision: True）
2. 一个Loop中可能产生多个因子
3. 当前API实现**未能获取所有SOTA因子**

## 文档要求 vs 当前实现

### 文档要求（模型权重文件定位方案_v2.md）

**关键机制：**

1. **因子实验累积机制**（第2.1.1节）
```python
exp.based_experiments = [QlibFactorExperiment(sub_tasks=[])] + [
    t[0] for t in trace.hist if t[1] and isinstance(t[0], FactorExperiment)
]
```

**说明：**
- 每个因子实验的`based_experiments`包含**所有之前被接受的因子实验**
- `t[1]`表示`feedback.decision`，只有`decision=True`的因子才会被包含
- 最后一个SOTA因子的`based_experiments`包含所有SOTA因子

2. **正确的数据获取方式**（第3.1节）
```python
步骤1: 找到最后一个被接受的因子实验
  遍历: reversed(session.trace.hist)
  条件: feedback.decision=True AND isinstance(exp, FactorExperiment)

步骤2: 从based_experiments提取所有SOTA因子
  来源: last_sota_factor_exp.based_experiments
  提取: 因子名称、表达式、代码
```

### 当前AIstock API实现

**文件：** `f:\Dev\AIstock\backend\services\rdagent_task_sync_service.py`

**问题1：只查找最后一个SOTA因子**

```python
def _find_last_sota_factor_experiment_from_session(session_obj: Any) -> Tuple[Optional[JsonDict], Optional[str]]:
    hist = getattr(session_obj.trace, "hist", [])
    for i in range(len(hist) - 1, -1, -1):
        exp, feedback = hist[i]
        if feedback and getattr(feedback, "decision", False):
            exp_type = type(exp).__name__
            if "Factor" in exp_type:
                # 只返回最后一个SOTA因子的信息
                return {
                    "last_sota_factor_index": i,
                    "exp_type": exp_type,
                    "workspace_path": ws_path,
                    "file_dict_keys": list(file_dict.keys()),
                }, None
    return None, "no_sota_factor_experiment_found"
```

**问题：**
- ❌ 只找到最后一个SOTA因子
- ❌ 没有遍历`trace.hist`获取所有SOTA因子
- ❌ 没有统计SOTA因子总数

**问题2：未从based_experiments提取所有因子**

```python
def _extract_factor_sources_from_session(session_obj: Any, factor_exp_index: int) -> Tuple[List[JsonDict], Optional[str]]:
    hist = session_obj.trace.hist
    exp, _ = hist[factor_exp_index]
    out = []
    
    # 基础因子 (based_experiments)
    if hasattr(exp, "based_experiments"):
        for i, b_exp in enumerate(exp.based_experiments):
            if hasattr(b_exp, "sub_tasks") and b_exp.sub_tasks:
                # 提取因子信息
                out.append({
                    "name": b_exp.sub_tasks[0].factor_name,
                    "formulation": b_exp.sub_tasks[0].factor_formulation,
                    "filename": f"based_factor_{i}.py",
                    "code": code
                })
```

**问题：**
- ✓ 正确从`based_experiments`提取因子
- ❌ 但依赖于先找到正确的`factor_exp_index`
- ❌ 如果`_find_last_sota_factor_experiment_from_session`找错了，这里也会错

## 根本问题分析

### 问题1：概念理解错误

**错误理解：**
- 认为只需要"最后一个SOTA因子"
- 认为一个task只有一个SOTA因子

**正确理解：**
- 一个task可以有**多个SOTA因子**（多个Loop，每个Loop可能Decision: True）
- 需要获取**所有SOTA因子**的信息
- 最后一个SOTA因子的`based_experiments`包含所有之前的SOTA因子

### 问题2：实现方式错误

**错误方式：**
```python
# 只遍历一次trace.hist，找到第一个Decision=True就返回
for i in range(len(hist) - 1, -1, -1):
    if feedback and getattr(feedback, "decision", False):
        return {...}, None  # 立即返回
```

**正确方式（根据文档）：**
```python
# 方式1：遍历所有trace.hist，统计所有SOTA因子
all_sota_factors = []
for i, (exp, feedback) in enumerate(hist):
    if feedback and getattr(feedback, "decision", False):
        if "Factor" in type(exp).__name__:
            all_sota_factors.append({
                'index': i,
                'exp': exp,
                'factor_name': exp.sub_tasks[0].factor_name
            })

# 方式2：从最后一个SOTA因子的based_experiments提取
last_sota_exp = all_sota_factors[-1]['exp']
all_factors_from_based = []
for b_exp in last_sota_exp.based_experiments:
    if hasattr(b_exp, 'sub_tasks') and b_exp.sub_tasks:
        all_factors_from_based.append(b_exp.sub_tasks[0].factor_name)
```

### 问题3：与UI数据不一致

**UI显示：**
- 多个Loop显示Decision: True
- 每个Loop有详细的Hypothesis Feedback
- 显示年化收益率、IC值等指标

**API返回：**
- 只返回最后一个SOTA因子的信息
- 缺少其他SOTA因子的数据
- 无法验证是否所有SOTA因子都被正确获取

## 影响分析

### 1. 数据完整性问题

| 项目 | 预期 | 实际 | 影响 |
|------|------|------|------|
| SOTA因子数量 | 所有Decision=True的因子 | 只有最后一个 | ❌ 数据不完整 |
| 因子源代码 | 所有SOTA因子的代码 | 只有最后一个的based_experiments | ⚠ 可能完整（如果based_experiments正确） |
| 因子元数据 | 所有SOTA因子的名称、表达式 | 只有最后一个的based_experiments | ⚠ 可能完整 |

### 2. 验证准确性问题

**无法验证：**
- ❌ 是否所有SOTA因子都被获取
- ❌ SOTA因子总数是否正确
- ❌ 是否与UI显示的数据一致

### 3. 同步准确性问题

**当前验证结果：**
- task `2025-12-26_06-19-42-126375`：API返回0个SOTA因子
- task `2025-12-30_10-24-18-730664`：API返回数据，但无法确认是否完整

**根本原因：**
- API实现未按文档要求遍历`trace.hist`
- 依赖`all_sota_factors.json`（可能不完整或不存在）
- 未正确实现session数据读取

## 正确的实现方案

### 方案1：遍历trace.hist（推荐）

```python
def get_all_sota_factors_from_session(session_obj: Any) -> Dict:
    """从session的trace.hist中获取所有SOTA因子"""
    hist = getattr(session_obj.trace, "hist", [])
    
    all_sota_factors = []
    for i, (exp, feedback) in enumerate(hist):
        # 检查是否为被接受的因子实验
        if feedback and getattr(feedback, "decision", False):
            exp_type = type(exp).__name__
            if "Factor" in exp_type:
                # 提取因子信息
                factor_info = {
                    'index': i,
                    'exp_type': exp_type,
                    'factor_name': None,
                    'formulation': None,
                    'workspace_path': None,
                    'file_dict_keys': []
                }
                
                # 提取因子名称和表达式
                if hasattr(exp, 'sub_tasks') and exp.sub_tasks:
                    factor_info['factor_name'] = exp.sub_tasks[0].factor_name
                    factor_info['formulation'] = exp.sub_tasks[0].factor_formulation
                
                # 提取workspace路径
                if hasattr(exp, "experiment_workspace") and exp.experiment_workspace:
                    factor_info['workspace_path'] = exp.experiment_workspace.workspace_path
                
                # 提取file_dict keys
                if hasattr(exp, "sub_workspace_list") and exp.sub_workspace_list:
                    file_dict = getattr(exp.sub_workspace_list[0], "file_dict", {})
                    factor_info['file_dict_keys'] = list(file_dict.keys())
                
                all_sota_factors.append(factor_info)
    
    return {
        'total_experiments': len(hist),
        'sota_factors_count': len(all_sota_factors),
        'sota_factors': all_sota_factors,
        'last_sota_factor_index': all_sota_factors[-1]['index'] if all_sota_factors else -1
    }
```

### 方案2：从based_experiments提取（辅助验证）

```python
def get_all_factors_from_based_experiments(session_obj: Any, last_sota_index: int) -> List[Dict]:
    """从最后一个SOTA因子的based_experiments中提取所有因子"""
    hist = session_obj.trace.hist
    if last_sota_index < 0 or last_sota_index >= len(hist):
        return []
    
    exp, _ = hist[last_sota_index]
    all_factors = []
    
    # 从based_experiments提取
    if hasattr(exp, 'based_experiments'):
        for i, b_exp in enumerate(exp.based_experiments):
            if hasattr(b_exp, 'sub_tasks') and b_exp.sub_tasks:
                all_factors.append({
                    'index': i,
                    'factor_name': b_exp.sub_tasks[0].factor_name,
                    'formulation': b_exp.sub_tasks[0].factor_formulation,
                    'source': 'based_experiments'
                })
    
    # 添加当前因子
    if hasattr(exp, 'sub_tasks') and exp.sub_tasks:
        all_factors.append({
            'index': len(all_factors),
            'factor_name': exp.sub_tasks[0].factor_name,
            'formulation': exp.sub_tasks[0].factor_formulation,
            'source': 'current'
        })
    
    return all_factors
```

## 修复建议

### 1. 立即修复

**修改文件：** `f:\Dev\AIstock\backend\services\rdagent_task_sync_service.py`

**修改内容：**
1. 重写`_find_last_sota_factor_experiment_from_session()`
   - 改为`_get_all_sota_factors_from_session()`
   - 遍历完整的`trace.hist`
   - 返回所有SOTA因子的信息

2. 添加验证函数
   - 对比`trace.hist`遍历结果与`based_experiments`提取结果
   - 确保数据一致性

3. 更新API响应
   - 返回SOTA因子总数
   - 返回所有SOTA因子列表
   - 提供详细的验证信息

### 2. 测试验证

**验证步骤：**
1. 使用task `2025-12-26_06-19-42-126375`测试
2. 对比API返回的SOTA因子数量与UI显示
3. 验证所有因子的源代码都能获取
4. 确认与文档描述的机制一致

### 3. 文档更新

**更新内容：**
1. 明确说明一个task可以有多个SOTA因子
2. 说明一个Loop中可能有多个Decision: True的因子
3. 更新API文档，说明返回所有SOTA因子

## 结论

**当前问题：**
- ❌ AIstock API实现**未按文档要求**获取SOTA因子数据
- ❌ 只查找最后一个SOTA因子，**遗漏其他SOTA因子**
- ❌ 无法验证数据完整性和准确性

**根本原因：**
- 概念理解错误：认为只需要"最后一个"
- 实现方式错误：未遍历完整的`trace.hist`
- 缺少验证机制：无法确认数据是否完整

**修复优先级：**
- 🔴 **P0 - 立即修复**：重写SOTA因子获取逻辑
- 🔴 **P0 - 立即验证**：使用真实task数据验证
- 🟡 **P1 - 尽快完成**：添加数据一致性验证
- 🟡 **P1 - 尽快完成**：更新文档和API说明

**预期效果：**
- ✓ 正确获取所有SOTA因子（与UI显示一致）
- ✓ 数据完整性可验证
- ✓ 符合RD-Agent文档描述的机制
